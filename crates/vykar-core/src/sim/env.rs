//! Virtual time and the host model of the simulator.
//!
//! - [`SimTime`]: one virtual time shared by every client of a simulation,
//!   linked to the store clock so object mtimes and client time never
//!   diverge. It keeps a table of the threads blocked on it.
//! - [`SimClock`]: one client's view of it, with a wall-clock skew. It is
//!   the [`Clock`] of that client's [`HostEnv`](crate::repo::HostEnv).
//! - [`SimHosts`]: each client's identity and the PID-liveness model.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use super::fault::ClientId;
use super::log::thread_label;
use super::store::SimStore;
use crate::repo::host_env::{Clock, HostIdentity, StopSignal, Waker};

/// How [`SimClock::sleep`] behaves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeMode {
    /// `sleep(d)` advances the shared time by `d` and returns at once.
    /// Correct while at most one client is runnable: with two runnable
    /// clients one client's backoff would age the other's markers. The
    /// runner keeps that rule for spawned runs too (every other run in
    /// flight is held at a gate), so its scripted scenarios use Auto.
    Auto,
    /// Sleepers block until someone calls [`SimTime::advance`]. For a
    /// scenario with two clients that sleep at once.
    Manual,
}

/// One thread blocked on virtual time.
#[derive(Clone, Debug)]
pub struct Waiter {
    /// Virtual time since start at which it wakes.
    pub deadline: Duration,
    /// Thread name, or its id.
    pub thread: String,
}

struct TimeState {
    offset: Duration,
    wall_jump: chrono::Duration,
    mode: TimeMode,
    waiters: BTreeMap<u64, Waiter>,
    next_waiter: u64,
}

impl TimeState {
    /// Waiters whose deadline is still ahead. One whose deadline has passed
    /// has been woken and is about to unregister.
    fn pending(&self) -> impl Iterator<Item = &Waiter> {
        let offset = self.offset;
        self.waiters.values().filter(move |w| w.deadline > offset)
    }

    fn register(&mut self, deadline: Duration) -> u64 {
        let id = self.next_waiter;
        self.next_waiter += 1;
        self.waiters.insert(
            id,
            Waiter {
                deadline,
                thread: thread_label(),
            },
        );
        id
    }
}

/// Virtual time shared by every client of one simulation.
///
/// [`TimeMode::Auto`] holds only while one client at a time is runnable
/// (see its doc).
/// [`SimClock::wait_timeout`] never advances time in either mode.
pub struct SimTime {
    state: Mutex<TimeState>,
    cv: Condvar,
    base: Instant,
    start_utc: DateTime<Utc>,
    /// Advanced together with the virtual time.
    store: Option<SimStore>,
    /// Wakes the waiters of [`SimClock::wait_timeout`] on a stop signal.
    waker: Waker,
}

impl SimTime {
    /// Time starting at `start_utc`, linked to no store.
    pub fn new(start_utc: DateTime<Utc>, mode: TimeMode) -> Arc<Self> {
        Self::build(start_utc, mode, None)
    }

    /// Time starting at the store's clock. Every advance also advances the
    /// store clock, so the two never diverge.
    pub fn for_store(store: &SimStore, mode: TimeMode) -> Arc<Self> {
        Self::build(store.now(), mode, Some(store.clone()))
    }

    fn build(start_utc: DateTime<Utc>, mode: TimeMode, store: Option<SimStore>) -> Arc<Self> {
        Arc::new_cyclic(|weak: &Weak<Self>| {
            let weak = weak.clone();
            let waker: Waker = Arc::new(move || {
                if let Some(time) = weak.upgrade() {
                    // Taken so the notify cannot fall between a waiter's
                    // check of the signal and its wait.
                    let _st = time.lock();
                    time.cv.notify_all();
                }
            });
            Self {
                state: Mutex::new(TimeState {
                    offset: Duration::ZERO,
                    wall_jump: chrono::Duration::zero(),
                    mode,
                    waiters: BTreeMap::new(),
                    next_waiter: 0,
                }),
                cv: Condvar::new(),
                base: Instant::now(),
                start_utc,
                store,
                waker,
            }
        })
    }

    fn lock(&self) -> MutexGuard<'_, TimeState> {
        self.state.lock().expect("sim time poisoned")
    }

    /// Virtual time since start.
    pub fn elapsed(&self) -> Duration {
        self.lock().offset
    }

    /// Start time plus elapsed time plus wall jumps (no client skew).
    pub fn now_utc(&self) -> DateTime<Utc> {
        let st = self.lock();
        self.utc_at(&st)
    }

    fn utc_at(&self, st: &TimeState) -> DateTime<Utc> {
        let offset = chrono::Duration::from_std(st.offset).expect("sim time out of range");
        self.start_utc + offset + st.wall_jump
    }

    /// Move time forward, with the store clock; wakes every waiter.
    pub fn advance(&self, by: Duration) {
        let mut st = self.lock();
        self.advance_locked(&mut st, by);
    }

    fn advance_locked(&self, st: &mut TimeState, by: Duration) {
        st.offset += by;
        // The store never calls into SimTime, so holding our lock is safe.
        if let Some(store) = &self.store {
            store.advance_clock(by);
        }
        self.cv.notify_all();
    }

    /// Advance to the earliest pending waiter's deadline, if any; returns
    /// the step.
    pub fn advance_to_next_deadline(&self) -> Option<Duration> {
        let mut st = self.lock();
        let next = st.pending().map(|w| w.deadline).min()?;
        let step = next - st.offset;
        self.advance_locked(&mut st, step);
        Some(step)
    }

    /// Step the wall clock only (an NTP step); monotonic time and the store
    /// clock are unaffected.
    pub fn jump_wall(&self, by: chrono::Duration) {
        let mut st = self.lock();
        st.wall_jump += by;
        self.cv.notify_all();
    }

    pub fn set_mode(&self, mode: TimeMode) {
        self.lock().mode = mode;
    }

    /// Every registered waiter, including one whose deadline has passed and
    /// that has not yet woken.
    pub fn waiters(&self) -> Vec<Waiter> {
        self.lock().waiters.values().cloned().collect()
    }

    /// Block (real time, at most `timeout`) until exactly `n` threads wait
    /// with a deadline still ahead. A waiter whose deadline an advance has
    /// passed is not counted, so `advance(d); wait_for_waiters(1, ..)`
    /// returns only once the woken thread has done its work and blocked
    /// again. Returns false on timeout.
    pub fn wait_for_waiters(&self, n: usize, timeout: Duration) -> bool {
        let st = self.lock();
        let (_st, res) = self
            .cv
            .wait_timeout_while(st, timeout, |st| st.pending().count() != n)
            .expect("sim time poisoned");
        !res.timed_out()
    }

    /// A client's view, with a fixed wall-clock skew, on a machine booted
    /// [`DEFAULT_UPTIME_AT_START`] before the start.
    pub fn clock(self: &Arc<Self>, skew: chrono::Duration) -> SimClock {
        SimClock {
            time: Arc::clone(self),
            skew,
            booted_at: -default_uptime_at_start(),
        }
    }
}

/// One client's clock: the shared [`SimTime`] plus a wall-clock skew and
/// the virtual time at which the client's machine booted.
#[derive(Clone)]
pub struct SimClock {
    time: Arc<SimTime>,
    skew: chrono::Duration,
    booted_at: chrono::Duration,
}

impl SimClock {
    /// This clock on a machine that booted at `at`, virtual time since the
    /// start (negative: before the start).
    pub fn booted_at(mut self, at: chrono::Duration) -> Self {
        self.booted_at = at;
        self
    }
}

impl Clock for SimClock {
    fn now_utc(&self) -> DateTime<Utc> {
        self.time.now_utc() + self.skew
    }

    fn monotonic(&self) -> Instant {
        self.time.base + self.time.elapsed()
    }

    fn sleep(&self, d: Duration) {
        if d.is_zero() {
            return;
        }
        let t = &self.time;
        let mut st = t.lock();
        if st.mode == TimeMode::Auto {
            t.advance_locked(&mut st, d);
            return;
        }
        let deadline = st.offset + d;
        let id = st.register(deadline);
        t.cv.notify_all();
        let mut st =
            t.cv.wait_while(st, |st| st.offset < deadline)
                .expect("sim time poisoned");
        st.waiters.remove(&id);
        t.cv.notify_all();
    }

    fn wait_timeout(&self, signal: &StopSignal, d: Duration) -> bool {
        let t = &self.time;
        // Before taking our lock: `signal` calls its wakers outside its own
        // lock, and a waker takes ours.
        signal.add_waker(&t.waker);
        let mut st = t.lock();
        let deadline = st.offset + d;
        let id = st.register(deadline);
        t.cv.notify_all();
        let signalled = loop {
            if signal.is_signalled() {
                break true;
            }
            if st.offset >= deadline {
                break false;
            }
            st = t.cv.wait(st).expect("sim time poisoned");
        };
        st.waiters.remove(&id);
        t.cv.notify_all();
        signalled
    }

    /// Virtual monotonic time since boot: skew and wall jumps leave it
    /// alone, as NTP steps leave `CLOCK_BOOTTIME` alone.
    fn uptime(&self) -> Option<Duration> {
        let elapsed =
            chrono::Duration::from_std(self.time.elapsed()).expect("sim time out of range");
        Some(
            (elapsed - self.booted_at)
                .to_std()
                .unwrap_or(Duration::ZERO),
        )
    }
}

/// Inode of Linux's initial PID namespace (`PROC_PID_INIT_INO`): every
/// simulated process not in a container.
pub const INIT_PID_NS: u64 = 0xEFFF_FFFC;

/// Uptime of a simulated machine at the start of the simulation, unless
/// it boots later.
pub const DEFAULT_UPTIME_AT_START: Duration = Duration::from_secs(30 * 24 * 3600);

fn default_uptime_at_start() -> chrono::Duration {
    chrono::Duration::from_std(DEFAULT_UPTIME_AT_START).expect("constant in range")
}

/// Where a simulated client runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimMachine {
    pub hostname: String,
    pub boot_id: Option<String>,
    pub installation_id: Option<String>,
    pub pid_ns: Option<u64>,
    /// Virtual time since the start at which this boot began; negative
    /// for a machine already running at the start.
    pub booted_at: chrono::Duration,
}

impl SimMachine {
    /// `hostname` and `boot_id`, installation `install-<hostname>`, the
    /// initial PID namespace, booted [`DEFAULT_UPTIME_AT_START`] before
    /// the start.
    pub fn new(hostname: &str, boot_id: Option<&str>) -> Self {
        Self {
            hostname: hostname.to_string(),
            boot_id: boot_id.map(str::to_string),
            installation_id: Some(format!("install-{hostname}")),
            pid_ns: Some(INIT_PID_NS),
            booted_at: -default_uptime_at_start(),
        }
    }

    /// Replace the installation ID.
    pub fn installation(mut self, id: Option<&str>) -> Self {
        self.installation_id = id.map(str::to_string);
        self
    }

    /// Replace the PID namespace.
    pub fn pid_ns(mut self, ns: Option<u64>) -> Self {
        self.pid_ns = ns;
        self
    }

    /// Replace the boot time.
    pub fn booted_at(mut self, at: chrono::Duration) -> Self {
        self.booted_at = at;
        self
    }
}

struct HostSlot {
    host: HostIdentity,
    booted_at: chrono::Duration,
    running: bool,
}

struct HostsState {
    slots: BTreeMap<ClientId, HostSlot>,
    next_pid: u32,
}

/// Per-client identity and the PID-liveness model.
///
/// A machine is the pair (hostname, boot ID), with an installation ID, a
/// PID namespace and a boot time ([`SimMachine`]). Each run of a client is
/// a new process with a fresh PID, alive only while the run lasts and the
/// client has not crashed.
#[derive(Clone)]
pub struct SimHosts {
    inner: Arc<Mutex<HostsState>>,
    store: SimStore,
}

impl SimHosts {
    pub fn new(store: SimStore) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HostsState {
                slots: BTreeMap::new(),
                next_pid: 1000,
            })),
            store,
        }
    }

    fn lock(&self) -> MutexGuard<'_, HostsState> {
        self.inner.lock().expect("sim hosts poisoned")
    }

    /// Register `c` on `machine`, with a fresh PID.
    pub fn register(&self, c: ClientId, machine: &SimMachine) {
        let mut st = self.lock();
        let pid = st.next_pid;
        st.next_pid += 1;
        st.slots.insert(
            c,
            HostSlot {
                host: HostIdentity {
                    hostname: machine.hostname.clone(),
                    pid,
                    boot_id: machine.boot_id.clone(),
                    installation_id: machine.installation_id.clone(),
                    pid_ns: machine.pid_ns,
                },
                booted_at: machine.booted_at,
                running: false,
            },
        );
    }

    /// The identity of `c`'s current or last run.
    pub fn host(&self, c: ClientId) -> HostIdentity {
        self.lock()
            .slots
            .get(&c)
            .expect("unknown sim client")
            .host
            .clone()
    }

    /// When `c`'s machine booted, virtual time since the start.
    pub fn booted_at(&self, c: ClientId) -> chrono::Duration {
        self.lock()
            .slots
            .get(&c)
            .expect("unknown sim client")
            .booted_at
    }

    /// Start a new process for `c`: a fresh PID, running. Returns the PID.
    pub fn start_run(&self, c: ClientId) -> u32 {
        let mut st = self.lock();
        let pid = st.next_pid;
        st.next_pid += 1;
        let slot = st.slots.get_mut(&c).expect("unknown sim client");
        slot.host.pid = pid;
        slot.running = true;
        pid
    }

    pub fn set_running(&self, c: ClientId, running: bool) {
        self.lock()
            .slots
            .get_mut(&c)
            .expect("unknown sim client")
            .running = running;
    }

    /// A PID is alive on `local`'s machine iff some client on the same
    /// machine (same hostname and same boot ID) and in the same PID
    /// namespace has that PID, is running, and has not crashed. A PID on
    /// another machine or in another namespace is never alive from here, as
    /// `kill(pid, 0)` cannot see it.
    pub fn pid_alive(&self, local: &HostIdentity, pid: u32) -> bool {
        let st = self.lock();
        st.slots.iter().any(|(c, slot)| {
            slot.host.hostname == local.hostname
                && slot.host.boot_id == local.boot_id
                && slot.host.pid_ns == local.pid_ns
                && slot.host.pid == pid
                && slot.running
                && !self.store.is_crashed(*c)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::SimConfig;

    fn start() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    const WAIT: Duration = Duration::from_secs(5);

    #[test]
    fn auto_sleep_advances_at_once() {
        let time = SimTime::new(start(), TimeMode::Auto);
        let clock = time.clock(chrono::Duration::zero());
        let real = Instant::now();
        clock.sleep(Duration::from_secs(5));
        assert_eq!(time.elapsed(), Duration::from_secs(5));
        assert!(real.elapsed() < WAIT);
        assert_eq!(clock.now_utc(), start() + chrono::Duration::seconds(5));
    }

    #[test]
    fn auto_advance_moves_store_clock() {
        let store = SimStore::new(SimConfig::default());
        let before = store.now();
        let time = SimTime::for_store(&store, TimeMode::Auto);
        time.clock(chrono::Duration::zero())
            .sleep(Duration::from_secs(7));
        time.advance(Duration::from_secs(3));
        assert_eq!(store.now(), before + chrono::Duration::seconds(10));
        assert_eq!(time.now_utc(), store.now());
    }

    #[test]
    fn manual_sleep_blocks_until_advanced() {
        let time = SimTime::new(start(), TimeMode::Manual);
        let clock = time.clock(chrono::Duration::zero());
        let h = std::thread::spawn(move || clock.sleep(Duration::from_secs(5)));
        assert!(time.wait_for_waiters(1, WAIT));
        time.advance(Duration::from_secs(4));
        assert!(time.wait_for_waiters(1, WAIT));
        assert!(!h.is_finished());
        time.advance(Duration::from_secs(1));
        h.join().unwrap();
        assert!(time.waiters().is_empty());
    }

    #[test]
    fn wait_timeout_returns_on_signal() {
        let time = SimTime::new(start(), TimeMode::Auto);
        let clock = time.clock(chrono::Duration::zero());
        let signal = Arc::new(StopSignal::new());
        let s = Arc::clone(&signal);
        let h = std::thread::spawn(move || clock.wait_timeout(&s, Duration::from_secs(30)));
        assert!(time.wait_for_waiters(1, WAIT));
        let real = Instant::now();
        signal.signal();
        assert!(h.join().unwrap());
        assert!(real.elapsed() < WAIT);
        assert_eq!(time.elapsed(), Duration::ZERO, "wait_timeout advanced time");
    }

    #[test]
    fn wait_timeout_returns_false_past_deadline() {
        let time = SimTime::new(start(), TimeMode::Auto);
        let clock = time.clock(chrono::Duration::zero());
        let h = std::thread::spawn(move || {
            clock.wait_timeout(&StopSignal::new(), Duration::from_secs(30))
        });
        assert!(time.wait_for_waiters(1, WAIT));
        assert_eq!(time.elapsed(), Duration::ZERO);
        time.advance(Duration::from_secs(31));
        assert!(!h.join().unwrap());
    }

    #[test]
    fn skew_and_wall_jump_leave_monotonic_alone() {
        let time = SimTime::new(start(), TimeMode::Auto);
        let plain = time.clock(chrono::Duration::zero());
        let skewed = time.clock(chrono::Duration::minutes(5));
        assert_eq!(
            skewed.now_utc() - plain.now_utc(),
            chrono::Duration::minutes(5)
        );
        assert_eq!(skewed.monotonic(), plain.monotonic());
        let mono = plain.monotonic();
        time.jump_wall(chrono::Duration::hours(-1));
        assert_eq!(plain.now_utc(), start() - chrono::Duration::hours(1));
        assert_eq!(
            skewed.now_utc(),
            start() - chrono::Duration::hours(1) + chrono::Duration::minutes(5)
        );
        assert_eq!(plain.monotonic(), mono);
    }

    #[test]
    fn advance_to_next_deadline_steps_to_earliest_waiter() {
        let time = SimTime::new(start(), TimeMode::Manual);
        assert_eq!(time.advance_to_next_deadline(), None);
        let handles: Vec<_> = [7u64, 3]
            .into_iter()
            .map(|s| {
                let clock = time.clock(chrono::Duration::zero());
                std::thread::spawn(move || clock.sleep(Duration::from_secs(s)))
            })
            .collect();
        assert!(time.wait_for_waiters(2, WAIT));
        assert_eq!(
            time.advance_to_next_deadline(),
            Some(Duration::from_secs(3))
        );
        assert!(time.wait_for_waiters(1, WAIT));
        assert_eq!(
            time.advance_to_next_deadline(),
            Some(Duration::from_secs(4))
        );
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(time.elapsed(), Duration::from_secs(7));
    }

    #[test]
    fn pid_alive_models_machines_and_runs() {
        let store = SimStore::new(SimConfig::default());
        let hosts = SimHosts::new(store.clone());
        let (a, b, c) = (
            store.register_client("a"),
            store.register_client("b"),
            store.register_client("c"),
        );
        let d = store.register_client("d");
        hosts.register(a, &SimMachine::new("hostx", Some("boot1")));
        hosts.register(b, &SimMachine::new("hostx", Some("boot2")));
        hosts.register(c, &SimMachine::new("hosty", Some("boot1")));
        hosts.register(
            d,
            &SimMachine::new("hostx", Some("boot1")).pid_ns(Some(INIT_PID_NS + 1)),
        );
        let pid = hosts.start_run(a);
        let same = HostIdentity {
            hostname: "hostx".into(),
            pid: 1,
            boot_id: Some("boot1".into()),
            installation_id: Some("install-hostx".into()),
            pid_ns: Some(INIT_PID_NS),
        };
        assert!(hosts.pid_alive(&same, pid));
        assert!(!hosts.pid_alive(&hosts.host(b), pid), "other boot ID");
        assert!(!hosts.pid_alive(&hosts.host(c), pid), "other hostname");
        assert!(!hosts.pid_alive(&hosts.host(d), pid), "other PID namespace");
        hosts.set_running(a, false);
        assert!(!hosts.pid_alive(&same, pid), "finished run");
        let pid2 = hosts.start_run(a);
        assert_ne!(pid, pid2, "each run is a new process");
        assert!(!hosts.pid_alive(&same, pid));
        assert!(hosts.pid_alive(&same, pid2));
        store.crash(a);
        assert!(!hosts.pid_alive(&same, pid2), "crashed client");
    }

    #[test]
    fn sim_clock_uptime_follows_virtual_time() {
        let time = SimTime::new(start(), TimeMode::Auto);
        let clock = time.clock(chrono::Duration::zero());
        assert_eq!(clock.uptime(), Some(DEFAULT_UPTIME_AT_START));
        time.advance(Duration::from_secs(60));
        assert_eq!(
            clock.uptime(),
            Some(DEFAULT_UPTIME_AT_START + Duration::from_secs(60))
        );
        let fresh = time
            .clock(chrono::Duration::zero())
            .booted_at(chrono::Duration::seconds(60));
        assert_eq!(fresh.uptime(), Some(Duration::ZERO));
        time.advance(Duration::from_secs(30));
        assert_eq!(fresh.uptime(), Some(Duration::from_secs(30)));
        let skewed = time
            .clock(chrono::Duration::hours(3))
            .booted_at(chrono::Duration::seconds(60));
        assert_eq!(skewed.uptime(), fresh.uptime());
        time.jump_wall(chrono::Duration::hours(-2));
        assert_eq!(fresh.uptime(), Some(Duration::from_secs(30)));
        // Not yet booted: clamped at zero.
        let later = time
            .clock(chrono::Duration::zero())
            .booted_at(chrono::Duration::hours(1));
        assert_eq!(later.uptime(), Some(Duration::ZERO));
    }
}
