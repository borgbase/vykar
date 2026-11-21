//! Cooperative cancellation shared by the engine, the storage backends and
//! the frontends.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::error::{Result, VykarError};

/// How often a waiter re-reads a flag that is raised without
/// [`CancelToken::cancel`], i.e. by a signal handler storing into the static
/// behind [`CancelToken::from_static`].
const STATIC_FLAG_POLL: Duration = Duration::from_millis(20);

/// A cancellation request shared by clones. Once cancelled it stays cancelled.
#[derive(Clone)]
pub struct CancelToken {
    inner: Arc<Inner>,
}

struct Inner {
    flag: Flag,
    /// Also the mutex of `wake`.
    state: Mutex<State>,
    wake: Condvar,
}

#[derive(Default)]
struct State {
    /// Wakers of pending [`Cancelled`] futures, by future id.
    waiters: Vec<(u64, Waker)>,
    next_id: u64,
    /// A watcher thread polls a static flag for the async waiters.
    watcher_running: bool,
}

enum Flag {
    Owned(AtomicBool),
    /// Raised by a signal handler, which may only store into an atomic.
    Static(&'static AtomicBool),
}

impl Inner {
    fn new(flag: Flag) -> Arc<Self> {
        Arc::new(Self {
            flag,
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
        })
    }

    fn raised(&self) -> bool {
        match &self.flag {
            Flag::Owned(f) => f.load(Ordering::Acquire),
            Flag::Static(f) => f.load(Ordering::Acquire),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wake every blocking and async waiter. The caller has raised the flag.
    fn wake_all(&self, mut state: MutexGuard<'_, State>) {
        let wakers = std::mem::take(&mut state.waiters);
        self.wake.notify_all();
        drop(state);
        for (_, waker) in wakers {
            waker.wake();
        }
    }
}

impl CancelToken {
    pub fn new() -> Self {
        Self {
            inner: Inner::new(Flag::Owned(AtomicBool::new(false))),
        }
    }

    /// A token over a process-wide flag that an async-signal handler sets.
    ///
    /// Polls see the store at once; blocking and async waiters within
    /// `STATIC_FLAG_POLL` (20 ms). Async waiters are served by a watcher
    /// thread that runs only while a [`Cancelled`] future is pending; if that
    /// thread cannot be spawned, async waiters are woken only by
    /// [`CancelToken::cancel`].
    pub fn from_static(flag: &'static AtomicBool) -> Self {
        Self {
            inner: Inner::new(Flag::Static(flag)),
        }
    }

    /// Raise the flag and wake every blocking and async waiter.
    pub fn cancel(&self) {
        match &self.inner.flag {
            Flag::Owned(f) => f.store(true, Ordering::Release),
            Flag::Static(f) => f.store(true, Ordering::Release),
        }
        // Waiters re-check the flag under this lock, so no wake-up is lost.
        let state = self.inner.lock();
        self.inner.wake_all(state);
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.raised()
    }

    /// `Err(VykarError::Interrupted)` once cancelled.
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(VykarError::Interrupted)
        } else {
            Ok(())
        }
    }

    /// Block for up to `timeout` or until cancelled; `true` when cancelled.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = self.inner.lock();
        loop {
            if self.inner.raised() {
                return true;
            }
            let mut slice = match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return false;
                    }
                    deadline - now
                }
                None => STATIC_FLAG_POLL,
            };
            if matches!(self.inner.flag, Flag::Static(_)) {
                slice = slice.min(STATIC_FLAG_POLL);
            }
            state = self
                .inner
                .wake
                .wait_timeout(state, slice)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Resolves once the token is cancelled.
    pub fn cancelled(&self) -> Cancelled<'_> {
        Cancelled {
            token: self,
            id: None,
        }
    }

    #[cfg(test)]
    fn watcher_running(&self) -> bool {
        self.inner.lock().watcher_running
    }

    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.inner.lock().waiters.len()
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CancelToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancelToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Polls a static flag on behalf of the pending async waiters. Exits once the
/// flag is raised, no waiter is left or the token is gone.
fn watch_static(inner: &Weak<Inner>) {
    loop {
        std::thread::sleep(STATIC_FLAG_POLL);
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let mut state = inner.lock();
        if inner.raised() {
            state.watcher_running = false;
            inner.wake_all(state);
            return;
        }
        if state.waiters.is_empty() {
            state.watcher_running = false;
            return;
        }
    }
}

/// Future returned by [`CancelToken::cancelled`].
#[must_use = "futures do nothing unless polled"]
pub struct Cancelled<'a> {
    token: &'a CancelToken,
    /// Set once this future has registered a waker.
    id: Option<u64>,
}

impl Future for Cancelled<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let inner = &this.token.inner;
        if inner.raised() {
            return Poll::Ready(());
        }
        let mut state = inner.lock();
        if inner.raised() {
            return Poll::Ready(());
        }
        let registered = this
            .id
            .and_then(|id| state.waiters.iter_mut().find(|(i, _)| *i == id));
        match registered {
            Some((_, waker)) => {
                if !waker.will_wake(cx.waker()) {
                    waker.clone_from(cx.waker());
                }
            }
            None => {
                let id = state.next_id;
                state.next_id += 1;
                state.waiters.push((id, cx.waker().clone()));
                this.id = Some(id);
            }
        }
        if matches!(inner.flag, Flag::Static(_)) && !state.watcher_running {
            let weak = Arc::downgrade(inner);
            let spawned = std::thread::Builder::new()
                .name("cancel-watch".into())
                .spawn(move || watch_static(&weak));
            // Without a watcher the waker stays registered for `cancel`.
            state.watcher_running = spawned.is_ok();
        }
        Poll::Pending
    }
}

impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.token.inner.lock().waiters.retain(|(i, _)| *i != id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::task::Wake;
    use std::thread::{self, Thread};

    const LONG: Duration = Duration::from_secs(60);
    const BOUND: Duration = Duration::from_secs(5);

    /// Unparks the test thread and counts its wake-ups.
    struct ThreadWaker {
        thread: Thread,
        wakes: AtomicUsize,
    }

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }
    }

    fn thread_waker() -> (Arc<ThreadWaker>, Waker) {
        let w = Arc::new(ThreadWaker {
            thread: thread::current(),
            wakes: AtomicUsize::new(0),
        });
        (w.clone(), Waker::from(w))
    }

    fn wait_until(bound: Duration, cond: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + bound;
        while Instant::now() < deadline {
            if cond() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        cond()
    }

    #[test]
    fn new_token_is_not_cancelled() {
        let token = CancelToken::new();
        assert!(!token.is_cancelled());
        assert!(token.check().is_ok());
    }

    #[test]
    fn cancel_is_seen_by_clones() {
        let token = CancelToken::new();
        let clone = token.clone();
        token.cancel();
        assert!(clone.is_cancelled());
        assert!(matches!(clone.check(), Err(VykarError::Interrupted)));
    }

    #[test]
    fn wait_timeout_times_out() {
        let token = CancelToken::new();
        let start = Instant::now();
        assert!(!token.wait_timeout(Duration::from_millis(50)));
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn wait_timeout_returns_at_once_when_already_cancelled() {
        let token = CancelToken::new();
        token.cancel();
        let start = Instant::now();
        assert!(token.wait_timeout(LONG));
        assert!(start.elapsed() < BOUND);
    }

    #[test]
    fn wait_timeout_wakes_on_cancel() {
        let token = CancelToken::new();
        let canceller = token.clone();
        let start = Instant::now();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            canceller.cancel();
        });
        assert!(token.wait_timeout(LONG));
        assert!(start.elapsed() < BOUND);
        handle.join().unwrap();
    }

    #[test]
    fn static_flag_store_is_seen_by_poll_and_wait() {
        static FLAG: AtomicBool = AtomicBool::new(false);
        let token = CancelToken::from_static(&FLAG);
        assert!(!token.is_cancelled());
        let start = Instant::now();
        let handle = thread::spawn(|| {
            thread::sleep(Duration::from_millis(50));
            FLAG.store(true, Ordering::SeqCst);
        });
        assert!(token.wait_timeout(LONG));
        assert!(start.elapsed() < BOUND);
        assert!(token.is_cancelled());
        handle.join().unwrap();
    }

    #[test]
    fn cancel_on_static_token_raises_the_static() {
        static FLAG: AtomicBool = AtomicBool::new(false);
        CancelToken::from_static(&FLAG).cancel();
        assert!(FLAG.load(Ordering::SeqCst));
    }

    #[test]
    fn cancelled_future_resolves_after_cancel() {
        let token = CancelToken::new();
        let (counter, waker) = thread_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = token.cancelled();
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());

        let canceller = token.clone();
        let handle = thread::spawn(move || canceller.cancel());
        handle.join().unwrap();
        assert_eq!(counter.wakes.load(Ordering::SeqCst), 1);
        assert!(Pin::new(&mut fut).poll(&mut cx).is_ready());

        let mut done = token.cancelled();
        assert!(Pin::new(&mut done).poll(&mut cx).is_ready());
    }

    #[test]
    fn repoll_keeps_one_waiter() {
        let token = CancelToken::new();
        let (_, waker) = thread_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = token.cancelled();
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
        assert_eq!(token.waiter_count(), 1);
    }

    #[test]
    fn dropped_pending_future_deregisters() {
        let token = CancelToken::new();
        let (_, waker) = thread_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = token.cancelled();
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
        assert_eq!(token.waiter_count(), 1);
        drop(fut);
        assert_eq!(token.waiter_count(), 0);
    }

    #[test]
    fn async_waiter_on_static_token_sees_a_plain_store() {
        static FLAG: AtomicBool = AtomicBool::new(false);
        let token = CancelToken::from_static(&FLAG);
        let handle = thread::spawn(|| {
            thread::sleep(Duration::from_millis(50));
            FLAG.store(true, Ordering::SeqCst);
        });
        let (counter, waker) = thread_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = token.cancelled();
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
        // The watcher must wake the waiter; a later poll alone would also
        // see the raised flag and prove nothing.
        assert!(wait_until(BOUND, || counter.wakes.load(Ordering::SeqCst) >= 1));
        assert!(Pin::new(&mut fut).poll(&mut cx).is_ready());
        handle.join().unwrap();
        assert!(wait_until(BOUND, || !token.watcher_running()));
    }

    #[test]
    fn watcher_exits_after_pending_future_is_dropped() {
        static FLAG: AtomicBool = AtomicBool::new(false);
        let token = CancelToken::from_static(&FLAG);
        let (_, waker) = thread_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = token.cancelled();
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
        assert!(token.watcher_running());
        drop(fut);
        assert!(wait_until(BOUND, || !token.watcher_running()));
        assert!(!FLAG.load(Ordering::SeqCst));
    }
}
