//! Clock and host identity of one client, carried by value.
//!
//! Production uses [`HostEnv::system`]; the protocol simulator gives each
//! logical client its own. This file is the only place in the session and
//! maintenance code that reads the system clock or the platform identity.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, Utc};

/// Time source of the session and maintenance code.
pub trait Clock: Send + Sync {
    /// Wall-clock time. Session ages, the legacy lock key's and the marker
    /// checks use it, because it keeps running while the machine is
    /// suspended.
    fn now_utc(&self) -> DateTime<Utc>;
    /// Monotonic time for deadlines and instrumentation.
    fn monotonic(&self) -> Instant;
    /// Block the calling thread for `d` on this clock.
    fn sleep(&self, d: Duration);
    /// Block until `signal` is set or `timeout` has passed on this clock.
    /// Returns whether the signal was set.
    fn wait_timeout(&self, signal: &StopSignal, timeout: Duration) -> bool;
    /// Monotonic time since `start`, saturating at zero.
    fn elapsed(&self, start: Instant) -> Duration {
        self.monotonic().saturating_duration_since(start)
    }
    /// Time since this machine booted, suspend included, or None where the
    /// platform does not say. Compared with an entry's age to tell an
    /// entry left by an earlier boot from one written by a cloned machine.
    fn uptime(&self) -> Option<Duration>;
}

/// The system clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_utc(&self) -> DateTime<Utc> {
        // From<SystemTime> handles a clock before the epoch without panicking.
        DateTime::<Utc>::from(SystemTime::now())
    }

    fn monotonic(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, d: Duration) {
        std::thread::sleep(d);
    }

    fn wait_timeout(&self, signal: &StopSignal, timeout: Duration) -> bool {
        signal.wait_real(timeout)
    }

    fn uptime(&self) -> Option<Duration> {
        crate::platform::uptime()
    }
}

/// Called after [`StopSignal::signal`]; see [`StopSignal::add_waker`].
pub type Waker = Arc<dyn Fn() + Send + Sync>;

/// A one-shot stop flag a thread can block on.
pub struct StopSignal {
    mutex: Mutex<bool>,
    condvar: Condvar,
    wakers: Mutex<Vec<Waker>>,
}

impl StopSignal {
    /// An unset signal.
    pub fn new() -> Self {
        Self {
            mutex: Mutex::new(false),
            condvar: Condvar::new(),
            wakers: Mutex::new(Vec::new()),
        }
    }

    /// Set the flag and wake every waiter, then call every waker.
    pub fn signal(&self) {
        *self.mutex.lock().expect("StopSignal mutex not poisoned") = true;
        self.condvar.notify_all();
        // The flag's mutex is released: a waker may take a clock's lock,
        // under which that clock reads the flag.
        // The list is copied out so a waker runs without the wakers lock:
        // one that panics or registers another waker cannot wedge the signal.
        let wakers: Vec<Waker> = self
            .wakers
            .lock()
            .expect("StopSignal wakers not poisoned")
            .clone();
        for waker in &wakers {
            waker();
        }
    }

    /// Call `waker` on every later [`Self::signal`], once per distinct
    /// `Arc`. For a clock whose [`Clock::wait_timeout`] blocks on its own
    /// condition variable instead of this signal's.
    pub fn add_waker(&self, waker: &Waker) {
        let mut wakers = self.wakers.lock().expect("StopSignal wakers not poisoned");
        if !wakers.iter().any(|w| Arc::ptr_eq(w, waker)) {
            wakers.push(Arc::clone(waker));
        }
    }

    /// Whether [`Self::signal`] has been called.
    pub fn is_signalled(&self) -> bool {
        *self.mutex.lock().expect("StopSignal mutex not poisoned")
    }

    /// Block up to `timeout` of real time. Returns true, at once, if
    /// signalled.
    pub fn wait_real(&self, timeout: Duration) -> bool {
        let guard = self.mutex.lock().expect("StopSignal mutex not poisoned");
        let (guard, _) = self
            .condvar
            .wait_timeout_while(guard, timeout, |stopped| !*stopped)
            .expect("StopSignal condvar wait not poisoned");
        *guard
    }
}

impl Default for StopSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// Who a client is, as written into session markers (and the v2/v3
/// upgrade's legacy lock key).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostIdentity {
    /// Hostname, with its domain if the system reports one.
    pub hostname: String,
    /// Process ID.
    pub pid: u32,
    /// Boot ID, where the OS exposes one.
    pub boot_id: Option<String>,
    /// Random ID of this installation (`platform::installation_id`),
    /// shared by every vykar process of one user on one machine.
    pub installation_id: Option<String>,
    /// Inode of the PID namespace the PID belongs to.
    pub pid_ns: Option<u64>,
}

impl HostIdentity {
    /// This process: the system hostname, the process ID, the boot ID,
    /// the installation ID and the PID namespace. The hostname is read each
    /// call; the other platform values once per process.
    pub fn system() -> Self {
        Self {
            hostname: crate::platform::hostname(),
            pid: std::process::id(),
            boot_id: crate::platform::boot_id(),
            installation_id: crate::platform::installation_id(),
            pid_ns: crate::platform::pid_namespace(),
        }
    }

    /// [`Self::hostname`] with the domain stripped.
    pub fn short_hostname(&self) -> String {
        crate::platform::strip_hostname_domain(&self.hostname)
    }
}

/// Whether `pid` is alive on the machine of the asking client `local`.
pub type PidAliveFn = dyn Fn(&HostIdentity, u32) -> bool + Send + Sync;

/// Clock and identity of one client.
#[derive(Clone)]
pub struct HostEnv {
    /// Time source.
    pub clock: Arc<dyn Clock>,
    /// Identity written into markers.
    pub host: HostIdentity,
    /// Same-host process liveness.
    pub pid_alive: Arc<PidAliveFn>,
    /// Whether this run has warned that locks or markers were aged by
    /// their timestamps ([`Self::first_timestamp_fallback`]). Shared by the clones of one
    /// run's environment; [`Self::for_new_run`] gives a fresh one.
    pub(crate) timestamp_fallback_warned: Arc<AtomicBool>,
}

impl HostEnv {
    /// The system clock and this process's identity, read now. The hostname
    /// is read each call, so a long-lived process sees a rename at its next
    /// operation; boot ID, installation ID and PID namespace once per
    /// process.
    pub fn system() -> Self {
        Self {
            clock: Arc::new(SystemClock),
            host: HostIdentity::system(),
            pid_alive: Arc::new(|_, pid| crate::platform::is_pid_alive(pid)),
            timestamp_fallback_warned: Arc::default(),
        }
    }

    /// This environment for a new run (one opened repository): the same
    /// clock, identity and liveness check, with its once-per-run warnings
    /// not yet given.
    #[must_use]
    pub fn for_new_run(&self) -> Self {
        Self {
            timestamp_fallback_warned: Arc::default(),
            ..self.clone()
        }
    }

    /// Whether the timestamp-fallback warning is still to be given in this
    /// run; true once, false after.
    pub(crate) fn first_timestamp_fallback(&self) -> bool {
        !self.timestamp_fallback_warned.swap(true, Ordering::Relaxed)
    }

    /// Replace the clock.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Replace the identity.
    pub fn with_host(mut self, host: HostIdentity) -> Self {
        self.host = host;
        self
    }

    /// Replace the liveness check.
    pub fn with_pid_alive(mut self, f: Arc<PidAliveFn>) -> Self {
        self.pid_alive = f;
        self
    }

    /// Wall-clock time on this client's clock.
    pub fn now_utc(&self) -> DateTime<Utc> {
        self.clock.now_utc()
    }

    /// Whether `pid` is alive on this client's machine.
    pub fn is_pid_alive(&self, pid: u32) -> bool {
        (self.pid_alive)(&self.host, pid)
    }
}

impl Default for HostEnv {
    fn default() -> Self {
        Self::system()
    }
}

impl fmt::Debug for HostEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostEnv")
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_fallback_flag_is_shared_by_clones_and_reset_per_run() {
        let env = HostEnv::system();
        assert!(env.first_timestamp_fallback());
        assert!(!env.clone().first_timestamp_fallback());
        let next = env.for_new_run();
        assert!(next.first_timestamp_fallback());
        assert!(!next.first_timestamp_fallback());
        assert!(!env.first_timestamp_fallback());
    }

    #[test]
    fn system_clock_now_utc_matches_chrono() {
        let diff = (SystemClock.now_utc() - Utc::now())
            .num_milliseconds()
            .abs();
        assert!(diff < 1000, "diff {diff} ms");
    }

    #[test]
    fn stop_signal_wait_real() {
        let s = StopSignal::new();
        assert!(!s.is_signalled());
        assert!(!s.wait_real(Duration::from_millis(10)));
        s.signal();
        assert!(s.is_signalled());
        let start = Instant::now();
        assert!(s.wait_real(Duration::from_secs(10)));
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn system_env_reads_platform_identity() {
        let env = HostEnv::system();
        assert_eq!(env.host.hostname, crate::platform::hostname());
        assert_eq!(env.host.pid, std::process::id());
        assert_eq!(env.host.boot_id, crate::platform::boot_id());
        assert_eq!(env.host.installation_id, crate::platform::installation_id());
        assert!(env.host.installation_id.is_some());
        assert_eq!(env.host.pid_ns, crate::platform::pid_namespace());
        #[cfg(target_os = "linux")]
        {
            assert!(env.host.pid_ns.is_some());
            assert!(SystemClock.uptime().is_some_and(|u| u > Duration::ZERO));
        }
    }

    #[test]
    fn short_hostname_strips_domain() {
        let host = HostIdentity {
            hostname: "box.example.org".into(),
            pid: 1,
            boot_id: None,
            installation_id: None,
            pid_ns: None,
        };
        assert_eq!(host.short_hostname(), "box");
    }
}
