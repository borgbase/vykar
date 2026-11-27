use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use crossbeam_channel::{Receiver, Sender};
use vykar_core::app::scheduler::{CycleId, Scheduler, Work};
use vykar_core::app::service::RepoSelection;
use vykar_core::config::ScheduleConfig;
use vykar_types::error::VykarError;

use crate::messages::{AppCommand, UiEvent, log_entry_now};

// Bounds post-wake latency: after system sleep or a wall-clock jump the loop
// re-evaluates wall-clock state within this many seconds regardless of how
// far in the future `next_run` sits.
const MAX_WAIT: Duration = Duration::from_secs(60);

/// Time to wait before re-checking the scheduler state. Returns `ZERO` if
/// `next_run` has already passed, otherwise the remaining duration clamped
/// to [`MAX_WAIT`].
pub(crate) fn capped_wait(next_run: SystemTime, now: SystemTime) -> Duration {
    next_run
        .duration_since(now)
        .unwrap_or(Duration::ZERO)
        .min(MAX_WAIT)
}

/// Log every `(repo name, error)` of a cadence that could not be computed.
/// Only that repo loses its slot; the others stay on schedule.
pub(crate) fn send_cadence_errors(ui_tx: &Sender<UiEvent>, errors: Vec<(String, VykarError)>) {
    for (repo, e) in errors {
        let _ = ui_tx.send(log_entry_now(format!(
            "[{repo}] scheduler error: {e}. This repository will not run automatically \
             until the config is fixed and reloaded."
        )));
    }
}

/// Scheduler state shared by the scheduler thread and the worker: the core
/// [`Scheduler`] state machine (the same one the daemon drives) plus the
/// repository names its indices stand for.
#[derive(Debug, Default)]
pub(crate) struct SchedulerState {
    /// Repository names as produced by `ResolvedRepo::label_or_url` — the selector
    /// `AppCommand::RunBackupRepos` filters on. Index-aligned with the
    /// scheduler's schedules.
    names: Vec<String>,
    scheduler: Scheduler,
}

impl SchedulerState {
    /// Replace the per-repo entries and compute each one's first slot from `now`.
    ///
    /// Returns `(repo name, error)` for every repo whose cadence could not be
    /// computed, for the caller to report; core never prints. Those repos get
    /// no slot and never fire; the rest keep their schedule. One malformed
    /// `every:` must not silently stop every other repo's backups.
    ///
    /// `honor_on_startup` is true only at worker startup: a reload must not
    /// re-trigger a repo's `on_startup` run.
    pub(crate) fn set_repos(
        &mut self,
        entries: Vec<(String, ScheduleConfig)>,
        now: SystemTime,
        honor_on_startup: bool,
    ) -> Vec<(String, VykarError)> {
        let (names, schedules): (Vec<String>, Vec<ScheduleConfig>) = entries.into_iter().unzip();
        let errors = self.scheduler.load(schedules, now, honor_on_startup);
        self.names = names;
        self.named(errors)
    }

    pub(crate) fn set_paused(&mut self, paused: bool) {
        self.scheduler.set_paused(paused);
    }

    /// Names whose slot has arrived (startup log only; hands nothing out).
    pub(crate) fn due_names(&self, now: SystemTime) -> Vec<String> {
        self.scheduler
            .due(now)
            .into_iter()
            .filter_map(|idx| self.names.get(idx).cloned())
            .collect()
    }

    /// The slot of the repo at `idx`, if it has one.
    #[cfg(test)]
    pub(crate) fn next_run(&self, idx: usize) -> Option<SystemTime> {
        self.scheduler.next_run(idx)
    }

    /// The scheduler's decision at `now`, with the order's repositories as
    /// names, and when to poll again.
    pub(crate) fn poll(
        &mut self,
        now: SystemTime,
    ) -> (Option<(CycleId, Vec<String>)>, Option<SystemTime>) {
        let decision = self.scheduler.poll(now);
        let order = decision.run.map(|order| {
            let names = match order.selection {
                RepoSelection::All => self.names.clone(),
                RepoSelection::Indices(v) => v
                    .into_iter()
                    .filter_map(|idx| self.names.get(idx).cloned())
                    .collect(),
                RepoSelection::Explicit(idx) => self.names.get(idx).cloned().into_iter().collect(),
            };
            (order.id, names)
        });
        (order, decision.wake_at)
    }

    /// Record work the worker started itself (not handed out by `poll`).
    pub(crate) fn started(&mut self, work: Work) -> CycleId {
        self.scheduler.started(work)
    }

    /// End running work at `now`; returns the cadences that could not be
    /// recomputed, by repo name.
    pub(crate) fn finished(&mut self, id: CycleId, now: SystemTime) -> Vec<(String, VykarError)> {
        let done = self.scheduler.finished(id, now);
        self.named(done.errors)
    }

    fn named(&self, errors: Vec<(usize, VykarError)>) -> Vec<(String, VykarError)> {
        errors
            .into_iter()
            .map(|(idx, e)| (self.names.get(idx).cloned().unwrap_or_default(), e))
            .collect()
    }
}

/// Run the scheduler thread: poll the state, send each order the scheduler
/// hands out to the worker, and otherwise wait for the next slot or a notify
/// (config change, work finished). The worker ends every order through its
/// `OpGuard`, which notifies this thread afterwards.
pub(crate) fn spawn_scheduler(
    app_tx: Sender<AppCommand>,
    scheduler: Arc<Mutex<SchedulerState>>,
    notify_rx: Receiver<()>,
) {
    thread::spawn(move || {
        loop {
            let (order, wake_at) = match scheduler.lock() {
                Ok(mut s) => s.poll(SystemTime::now()),
                Err(_) => break,
            };
            if let Some((order, repo_names)) = order {
                if repo_names.is_empty() {
                    // Cannot happen (names and slots are replaced together),
                    // but an order nobody runs would hold the scheduler forever.
                    if let Ok(mut s) = scheduler.lock() {
                        s.finished(order, SystemTime::now());
                    }
                    continue;
                }
                if app_tx
                    .send(AppCommand::RunBackupRepos { repo_names, order })
                    .is_err()
                {
                    break;
                }
                continue;
            }
            // Paused, running or idle without a slot: wait for a notify.
            // Otherwise wait for the slot, capped so a suspend or a wall-clock
            // jump is noticed promptly (GitHub #110).
            let alive = match wake_at {
                Some(t) => crossbeam_channel::select! {
                    recv(notify_rx) -> r => r.is_ok(),
                    default(capped_wait(t, SystemTime::now())) => true,
                },
                None => notify_rx.recv().is_ok(),
            };
            if !alive {
                break;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::AppCommand;

    #[test]
    fn capped_wait_now_equals_next_is_zero() {
        let now = SystemTime::now();
        assert_eq!(capped_wait(now, now), Duration::ZERO);
    }

    #[test]
    fn capped_wait_past_next_run_is_zero() {
        let now = SystemTime::now();
        let past = now - Duration::from_secs(3600);
        assert_eq!(capped_wait(past, now), Duration::ZERO);
    }

    #[test]
    fn capped_wait_under_cap_returns_exact_remaining() {
        let now = SystemTime::now();
        let future = now + Duration::from_secs(10);
        assert_eq!(capped_wait(future, now), Duration::from_secs(10));
    }

    #[test]
    fn capped_wait_over_cap_is_clamped() {
        let now = SystemTime::now();
        let far_future = now + Duration::from_secs(7200);
        assert_eq!(capped_wait(far_future, now), MAX_WAIT);
    }

    fn sched(every: &str, enabled: bool) -> ScheduleConfig {
        ScheduleConfig {
            enabled,
            every: Some(every.to_string()),
            cron: None,
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        }
    }

    fn entries(repos: &[(&str, &str, bool)]) -> Vec<(String, ScheduleConfig)> {
        repos
            .iter()
            .map(|(name, every, on)| ((*name).to_string(), sched(every, *on)))
            .collect()
    }

    /// A scheduler state with slots computed from `now`.
    fn state_at(
        now: SystemTime,
        paused: bool,
        repos: &[(&str, &str, bool)],
    ) -> Arc<Mutex<SchedulerState>> {
        let mut state = SchedulerState::default();
        state.set_paused(paused);
        let errors = state.set_repos(entries(repos), now, false);
        assert!(errors.is_empty(), "unexpected cadence errors: {errors:?}");
        Arc::new(Mutex::new(state))
    }

    /// A state whose `1h` slots lie an hour in the past, as after a suspend.
    fn state_overdue(repos: &[(&str, &str, bool)]) -> Arc<Mutex<SchedulerState>> {
        state_at(
            SystemTime::now() - Duration::from_secs(2 * 3600),
            false,
            repos,
        )
    }

    /// Assert the command is a scheduled run of exactly `expected` repos and
    /// return its order id.
    fn assert_runs(cmd: &AppCommand, expected: &[&str]) -> CycleId {
        let AppCommand::RunBackupRepos { repo_names, order } = cmd else {
            unreachable!("expected RunBackupRepos, got {cmd:?}");
        };
        assert_eq!(repo_names, expected, "unexpected due set");
        *order
    }

    /// Spawn a scheduler over `state`, returning the notify sender and the
    /// command receiver.
    fn spawn(state: &Arc<Mutex<SchedulerState>>) -> (Sender<()>, Receiver<AppCommand>) {
        let (notify_tx, notify_rx) = crossbeam_channel::bounded::<()>(1);
        let (app_tx, app_rx) = crossbeam_channel::unbounded::<AppCommand>();
        spawn_scheduler(app_tx, state.clone(), notify_rx);
        (notify_tx, app_rx)
    }

    #[test]
    fn fires_when_next_run_arrives() {
        let state = state_at(SystemTime::now(), false, &[("repo-a", "1s", true)]);
        let (_notify_tx, app_rx) = spawn(&state);

        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    /// Regression test for GitHub #110: after system sleep, `SystemTime` has
    /// advanced past `next_run` even though a monotonic clock would not have.
    /// The scheduler must fire promptly rather than wait a full interval.
    #[test]
    fn fires_when_next_run_is_in_the_past() {
        let state = state_overdue(&[("repo-a", "1h", true)]);
        let (_notify_tx, app_rx) = spawn(&state);

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    #[test]
    fn fires_only_for_the_due_repo() {
        let state = state_at(
            SystemTime::now(),
            false,
            &[("hourly", "1h", true), ("nas", "1s", true)],
        );
        let (_notify_tx, app_rx) = spawn(&state);

        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["nas"]);

        // The hourly repo keeps its distant slot.
        let s = state.lock().unwrap();
        assert!(s.next_run(0).unwrap() > SystemTime::now() + Duration::from_secs(3000));
    }

    #[test]
    fn disabled_repo_is_never_due() {
        let state = state_at(
            SystemTime::now(),
            false,
            &[("off", "1s", false), ("on", "1s", true)],
        );
        assert!(state.lock().unwrap().next_run(0).is_none());
        let (_notify_tx, app_rx) = spawn(&state);

        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["on"]);
    }

    #[test]
    fn blocks_while_work_runs() {
        // Record the work *before* spawning, so the overdue slot cannot fire
        // first.
        let state = state_overdue(&[("repo-a", "1h", true)]);
        let id = state.lock().unwrap().started(Work::Other);
        let (notify_tx, app_rx) = spawn(&state);

        thread::sleep(Duration::from_millis(100));
        assert!(
            app_rx.try_recv().is_err(),
            "should not fire while work runs"
        );

        assert!(
            state
                .lock()
                .unwrap()
                .finished(id, SystemTime::now())
                .is_empty()
        );
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    #[test]
    fn order_is_not_handed_out_twice() {
        let state = state_overdue(&[("repo-a", "1h", true)]);
        let (notify_tx, app_rx) = spawn(&state);

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let order = assert_runs(&cmd, &["repo-a"]);
        // A notify while the order is out must not hand it out again.
        let _ = notify_tx.try_send(());
        assert!(app_rx.recv_timeout(Duration::from_millis(200)).is_err());

        // Finishing it moves the slot an interval on: still nothing to run.
        state.lock().unwrap().finished(order, SystemTime::now());
        let _ = notify_tx.try_send(());
        assert!(app_rx.recv_timeout(Duration::from_millis(200)).is_err());
        assert!(state.lock().unwrap().next_run(0).unwrap() > SystemTime::now());
    }

    #[test]
    fn config_reload_wakes_scheduler() {
        // Start paused, so nothing can fire however close the slot is.
        let state = state_at(SystemTime::now(), true, &[("repo-a", "1s", true)]);
        let (notify_tx, app_rx) = spawn(&state);

        thread::sleep(Duration::from_millis(100));
        assert!(app_rx.try_recv().is_err());

        // Simulate config reload: unpause and recompute the slots.
        {
            let mut s = state.lock().unwrap();
            s.set_paused(false);
            s.set_repos(entries(&[("repo-a", "1s", true)]), SystemTime::now(), false);
        }
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    /// One malformed cadence must not take the whole scheduler down with it:
    /// the broken repo loses its slot, every other repo keeps firing.
    #[test]
    fn a_broken_cadence_does_not_stop_the_other_repos() {
        let mut state = SchedulerState::default();
        let mut broken = sched("1h", true);
        broken.every = None;
        broken.cron = Some("not a cron".into());

        let errors = state.set_repos(
            vec![
                ("broken".to_string(), broken),
                ("good".to_string(), sched("1s", true)),
            ],
            SystemTime::now(),
            false,
        );

        let (failed_repo, _) = errors.first().expect("the broken cadence must be reported");
        assert_eq!(errors.len(), 1);
        assert_eq!(failed_repo, "broken", "the error must name the repo");
        assert!(
            !state.scheduler.is_paused(),
            "one bad cadence must not pause the scheduler"
        );
        assert!(state.next_run(0).is_none());
        assert!(state.next_run(1).is_some());

        let state = Arc::new(Mutex::new(state));
        let (_notify_tx, app_rx) = spawn(&state);
        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["good"]);
    }

    #[test]
    fn disable_blocks_indefinitely() {
        let state = state_at(SystemTime::now(), false, &[("repo-a", "1s", false)]);
        let (notify_tx, app_rx) = spawn(&state);

        // Disabled — nothing should fire.
        thread::sleep(Duration::from_millis(100));
        assert!(app_rx.try_recv().is_err());

        state.lock().unwrap().set_repos(
            entries(&[("repo-a", "1s", true)]),
            SystemTime::now(),
            false,
        );
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }
}
