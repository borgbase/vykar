use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use crossbeam_channel::{Receiver, Sender};
use vykar_core::app::scheduler::SchedulePlan;
use vykar_core::config::ScheduleConfig;
use vykar_types::error::VykarError;

use crate::messages::{log_entry_now, AppCommand, UiEvent};

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

/// Scheduler state shared with the worker. Pause is deliberately global (it is
/// the user-facing tray/menu toggle); the per-repository cadence lives in the
/// core [`SchedulePlan`], which both the daemon and this thread drive.
#[derive(Debug, Default)]
pub(crate) struct SchedulerState {
    pub enabled: bool,
    pub paused: bool,
    /// Repository names as produced by `ResolvedRepo::label_or_url` — the selector
    /// `AppCommand::RunBackupRepos` filters on. Index-aligned with `schedules`
    /// and `plan`.
    names: Vec<String>,
    schedules: Vec<ScheduleConfig>,
    plan: SchedulePlan,
}

impl SchedulerState {
    /// Replace the per-repo entries and compute each one's first slot.
    ///
    /// Returns `(repo name, error)` for every repo whose cadence could not be
    /// computed, for the caller to report; core never prints. Those repos get
    /// no slot and simply never fire — the rest keep their schedule, matching
    /// [`SchedulePlan`]'s contract and the daemon's behavior. One malformed
    /// `every:` must not silently stop every other repo's backups.
    ///
    /// `on_startup` is deliberately not honored here — the worker issues those
    /// runs itself at startup, so letting the plan fire them too would
    /// double-trigger.
    pub(crate) fn set_repos(
        &mut self,
        entries: Vec<(String, ScheduleConfig)>,
    ) -> Vec<(String, VykarError)> {
        let (names, schedules): (Vec<String>, Vec<ScheduleConfig>) = entries.into_iter().unzip();
        let refs: Vec<&ScheduleConfig> = schedules.iter().collect();
        let (plan, errors) = SchedulePlan::new(&refs, false);
        let reported = errors
            .into_iter()
            .map(|(idx, e)| (names.get(idx).cloned().unwrap_or_default(), e))
            .collect();
        self.names = names;
        self.schedules = schedules;
        self.plan = plan;
        reported
    }

    /// Names of the repositories whose slot has arrived, each moved on to its
    /// next slot (`now + interval` — drift, not the missed fixed slot).
    ///
    /// The second element is `(repo name, error)` for any repo that could not be
    /// rescheduled. Such a repo loses its slot; the others are unaffected.
    fn take_due(&mut self, now: SystemTime) -> (Vec<String>, Vec<(String, VykarError)>) {
        let mut names = Vec::new();
        let mut errors = Vec::new();
        for idx in self.plan.due(now) {
            // `plan`, `names` and `schedules` are replaced together by
            // `set_repos`, so a due index always resolves in both.
            let (Some(name), Some(schedule)) = (self.names.get(idx), self.schedules.get(idx))
            else {
                continue;
            };
            names.push(name.clone());
            if let Some(e) = self.plan.reschedule(idx, schedule) {
                errors.push((name.clone(), e));
            }
        }
        (names, errors)
    }
}

pub(crate) fn spawn_scheduler(
    app_tx: Sender<AppCommand>,
    ui_tx: Sender<UiEvent>,
    scheduler: Arc<Mutex<SchedulerState>>,
    backup_running: Arc<AtomicBool>,
    notify_rx: Receiver<()>,
) {
    thread::spawn(move || loop {
        let timeout = {
            let state = match scheduler.lock() {
                Ok(s) => s,
                Err(_) => break,
            };

            if !state.enabled || state.paused {
                // Disabled or paused — block until notified of state change.
                drop(state);
                if notify_rx.recv().is_err() {
                    break;
                }
                continue;
            }

            match state.plan.next_wake() {
                Some(next) => {
                    let wait = capped_wait(next, SystemTime::now());
                    if wait.is_zero() {
                        if backup_running.load(Ordering::SeqCst) {
                            // Backup is running and a slot has passed — block
                            // until woken by backup completion to avoid hot-spin.
                            drop(state);
                            if notify_rx.recv().is_err() {
                                break;
                            }
                            continue;
                        }
                        // Time to run — will be handled below after select.
                        Duration::ZERO
                    } else {
                        wait
                    }
                }
                None => {
                    // Nothing scheduled — block until notified.
                    drop(state);
                    if notify_rx.recv().is_err() {
                        break;
                    }
                    continue;
                }
            }
        };

        // Wait for timeout or notification (state change / backup completion).
        if !timeout.is_zero() {
            crossbeam_channel::select! {
                recv(notify_rx) -> res => {
                    if res.is_err() {
                        break;
                    }
                    // State changed — re-evaluate from the top.
                    continue;
                }
                default(timeout) => {
                    // Timeout reached — fall through to check if we should run.
                }
            }
        }

        // Re-check state under lock (may have changed during wait).
        let mut due_names: Vec<String> = Vec::new();
        {
            let mut state = match scheduler.lock() {
                Ok(s) => s,
                Err(_) => break,
            };

            if !state.enabled || state.paused {
                continue;
            }

            if !backup_running.load(Ordering::SeqCst) {
                let (names, errors) = state.take_due(SystemTime::now());
                due_names = names;
                // Only the offending repo loses its slot; the rest stay on
                // schedule. Pausing globally here would let one bad cadence
                // stop every backup silently.
                for (repo, e) in errors {
                    let _ = ui_tx.send(log_entry_now(format!(
                        "[{repo}] scheduler error: {e}. This repository will not run \
                         automatically until the config is fixed and reloaded."
                    )));
                }
            }
        }

        if !due_names.is_empty()
            && app_tx
                .send(AppCommand::RunBackupRepos {
                    repo_names: due_names,
                })
                .is_err()
        {
            break;
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

    /// A scheduler state with slots already computed by the core plan.
    fn state_with(
        enabled: bool,
        paused: bool,
        repos: &[(&str, &str, bool)],
    ) -> Arc<Mutex<SchedulerState>> {
        let mut state = SchedulerState {
            enabled,
            paused,
            ..SchedulerState::default()
        };
        let errors = state.set_repos(
            repos
                .iter()
                .map(|(name, every, on)| ((*name).to_string(), sched(every, *on)))
                .collect(),
        );
        assert!(errors.is_empty(), "unexpected cadence errors: {errors:?}");
        Arc::new(Mutex::new(state))
    }

    /// Sleep until every `1s` slot built by [`state_with`] lies in the past.
    /// Reproduces the post-sleep condition of GitHub #110 without reaching into
    /// the plan's internals.
    fn wait_past_slot() {
        thread::sleep(Duration::from_millis(1100));
    }

    /// Assert the command is a scheduled run of exactly `expected` repos.
    fn assert_runs(cmd: &AppCommand, expected: &[&str]) {
        let AppCommand::RunBackupRepos { repo_names } = cmd else {
            unreachable!("expected RunBackupRepos, got {cmd:?}");
        };
        assert_eq!(repo_names, expected, "unexpected due set");
    }

    /// Spawn a scheduler over `state`, returning the notify sender, the running
    /// flag, and the command receiver.
    fn spawn(
        state: &Arc<Mutex<SchedulerState>>,
        backup_running: Arc<AtomicBool>,
    ) -> (Sender<()>, Receiver<AppCommand>) {
        let (notify_tx, notify_rx) = crossbeam_channel::bounded::<()>(1);
        let (app_tx, app_rx) = crossbeam_channel::unbounded::<AppCommand>();
        let (ui_tx, _ui_rx) = crossbeam_channel::unbounded::<UiEvent>();
        spawn_scheduler(app_tx, ui_tx, state.clone(), backup_running, notify_rx);
        (notify_tx, app_rx)
    }

    #[test]
    fn fires_when_next_run_arrives() {
        let state = state_with(true, false, &[("repo-a", "1s", true)]);
        let (_notify_tx, app_rx) = spawn(&state, Arc::new(AtomicBool::new(false)));

        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    /// Regression test for GitHub #110: after system sleep, `SystemTime` has
    /// advanced past `next_run` even though a monotonic clock would not have.
    /// The scheduler must fire promptly rather than wait a full interval.
    #[test]
    fn fires_when_next_run_is_in_the_past() {
        let state = state_with(true, false, &[("repo-a", "1h", true)]);
        // Force the slot into the past the way a suspend/resume would.
        {
            let mut s = state.lock().unwrap();
            s.set_repos(vec![("repo-a".to_string(), sched("1s", true))]);
        }
        wait_past_slot();
        let (_notify_tx, app_rx) = spawn(&state, Arc::new(AtomicBool::new(false)));

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    #[test]
    fn fires_only_for_the_due_repo() {
        let state = state_with(true, false, &[("hourly", "1h", true), ("nas", "1s", true)]);
        let (_notify_tx, app_rx) = spawn(&state, Arc::new(AtomicBool::new(false)));

        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["nas"]);

        // The hourly repo keeps its distant slot.
        let s = state.lock().unwrap();
        assert!(s.plan.next_run(0).unwrap() > SystemTime::now() + Duration::from_secs(3000));
    }

    #[test]
    fn disabled_repo_is_never_due() {
        let state = state_with(true, false, &[("off", "1s", false), ("on", "1s", true)]);
        assert!(state.lock().unwrap().plan.next_run(0).is_none());
        let (_notify_tx, app_rx) = spawn(&state, Arc::new(AtomicBool::new(false)));

        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["on"]);
    }

    #[test]
    fn blocks_while_backup_running() {
        // Set backup_running *before* spawning the scheduler to avoid a race
        // where the scheduler sees next_run in the past and fires before we
        // can set the flag.
        let state = state_with(true, false, &[("repo-a", "1s", true)]);
        wait_past_slot();
        let backup_running = Arc::new(AtomicBool::new(true));
        let (notify_tx, app_rx) = spawn(&state, backup_running.clone());

        // Give scheduler a moment to enter its loop and block on notify_rx.
        thread::sleep(Duration::from_millis(100));
        assert!(
            app_rx.try_recv().is_err(),
            "should not fire while backup running"
        );

        // Finish backup and notify.
        backup_running.store(false, Ordering::SeqCst);
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    #[test]
    fn config_reload_wakes_scheduler() {
        // Start paused, so nothing can fire however close the slot is.
        let state = state_with(true, true, &[("repo-a", "1s", true)]);
        let (notify_tx, app_rx) = spawn(&state, Arc::new(AtomicBool::new(false)));

        thread::sleep(Duration::from_millis(100));
        assert!(app_rx.try_recv().is_err());

        // Simulate config reload: unpause and recompute the slots.
        {
            let mut s = state.lock().unwrap();
            s.paused = false;
            s.set_repos(vec![("repo-a".to_string(), sched("1s", true))]);
        }
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    /// One malformed cadence must not take the whole scheduler down with it:
    /// the broken repo loses its slot, every other repo keeps firing.
    #[test]
    fn a_broken_cadence_does_not_stop_the_other_repos() {
        let mut state = SchedulerState {
            enabled: true,
            paused: false,
            ..SchedulerState::default()
        };
        let mut broken = sched("1h", true);
        broken.every = None;
        broken.cron = Some("not a cron".into());

        let errors = state.set_repos(vec![
            ("broken".to_string(), broken),
            ("good".to_string(), sched("1s", true)),
        ]);

        let (failed_repo, _) = errors.first().expect("the broken cadence must be reported");
        assert_eq!(errors.len(), 1);
        assert_eq!(failed_repo, "broken", "the error must name the repo");
        assert!(
            !state.paused,
            "one bad cadence must not pause the scheduler"
        );
        assert!(state.plan.next_run(0).is_none());
        assert!(state.plan.next_run(1).is_some());

        let state = Arc::new(Mutex::new(state));
        let (_notify_tx, app_rx) = spawn(&state, Arc::new(AtomicBool::new(false)));
        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["good"]);
    }

    #[test]
    fn disable_blocks_indefinitely() {
        let state = state_with(false, false, &[("repo-a", "1s", true)]);
        let (notify_tx, app_rx) = spawn(&state, Arc::new(AtomicBool::new(false)));

        // Disabled — nothing should fire.
        thread::sleep(Duration::from_millis(100));
        assert!(app_rx.try_recv().is_err());

        {
            let mut s = state.lock().unwrap();
            s.enabled = true;
            s.set_repos(vec![("repo-a".to_string(), sched("1s", true))]);
        }
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }
}
