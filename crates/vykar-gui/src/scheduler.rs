use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use crossbeam_channel::{Receiver, Sender};
use vykar_core::config::ScheduleConfig;

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

/// One repository's cadence and its next scheduled run. Repositories can be on
/// different schedules, so each keeps its own `next_run`.
#[derive(Debug, Clone)]
pub(crate) struct RepoSchedule {
    /// Repository name as produced by `format_repo_name` — the same selector
    /// `AppCommand::RunBackupRepos` filters on.
    pub name: String,
    pub schedule: ScheduleConfig,
    /// `None` when the repo's schedule is disabled or its cadence is unusable.
    pub next_run: Option<SystemTime>,
}

/// Scheduler state shared with the worker. Pause is deliberately global (it is
/// the user-facing tray/menu toggle); cadence is per repository.
#[derive(Debug, Default)]
pub(crate) struct SchedulerState {
    pub enabled: bool,
    pub paused: bool,
    pub repos: Vec<RepoSchedule>,
}

impl SchedulerState {
    /// Earliest upcoming run across all repositories.
    fn earliest(&self) -> Option<SystemTime> {
        self.repos.iter().filter_map(|r| r.next_run).min()
    }

    /// Names of the repositories whose slot has arrived, each moved on to its
    /// next slot (`now + interval` — drift, not the missed fixed slot). The
    /// second element is the first rescheduling error, if any.
    fn take_due(
        &mut self,
        now: SystemTime,
    ) -> (Vec<String>, Option<vykar_types::error::VykarError>) {
        let mut names = Vec::new();
        let mut error = None;
        for repo in &mut self.repos {
            if !repo.schedule.enabled {
                repo.next_run = None;
                continue;
            }
            match repo.next_run {
                Some(slot) if slot <= now => {}
                _ => continue,
            }
            names.push(repo.name.clone());
            match vykar_core::app::scheduler::next_run_delay(&repo.schedule) {
                Ok(delay) => repo.next_run = Some(now + delay),
                Err(e) => {
                    repo.next_run = None;
                    error.get_or_insert(e);
                }
            }
        }
        (names, error)
    }

    /// Give every enabled repository without a slot its next one.
    fn fill_missing_next_runs(
        &mut self,
    ) -> std::result::Result<(), vykar_types::error::VykarError> {
        let now = SystemTime::now();
        for repo in &mut self.repos {
            if !repo.schedule.enabled {
                repo.next_run = None;
                continue;
            }
            if repo.next_run.is_none() {
                repo.next_run =
                    Some(now + vykar_core::app::scheduler::next_run_delay(&repo.schedule)?);
            }
        }
        Ok(())
    }
}

/// Terse schedule summary for the Overview metric card: the interval string,
/// the cron expression, or "Off" when disabled/paused.
pub(crate) fn schedule_brief(schedule: &ScheduleConfig, paused: bool) -> String {
    if !schedule.enabled || paused {
        return "Off".to_string();
    }
    if let Some(ref cron) = schedule.cron {
        return cron.clone();
    }
    schedule.every.clone().unwrap_or_else(|| "24h".to_string())
}

/// Identity used to decide whether all repos share one cadence. Compares the
/// parsed interval rather than the raw string, so `60m` and `1h` count as the
/// same schedule instead of a spurious "per-repo".
fn schedule_key(schedule: &ScheduleConfig) -> (bool, Option<String>, Option<u64>) {
    (
        schedule.enabled,
        schedule.cron.clone(),
        schedule.every_duration().ok().map(|d| d.as_secs()),
    )
}

/// Overview summary across all repositories: the shared cadence when they
/// agree, `"per-repo"` when they differ, `"Off"` when paused or repo-less.
pub(crate) fn repos_schedule_brief(schedules: &[&ScheduleConfig], paused: bool) -> String {
    if paused {
        return "Off".to_string();
    }
    let mut iter = schedules.iter();
    let Some(first) = iter.next() else {
        return "Off".to_string();
    };
    let first_key = schedule_key(first);
    if iter.any(|s| schedule_key(s) != first_key) {
        return "per-repo".to_string();
    }
    schedule_brief(first, paused)
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
            let mut state = match scheduler.lock() {
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

            if let Err(e) = state.fill_missing_next_runs() {
                state.paused = true;
                let _ = ui_tx.send(log_entry_now(format!(
                    "Scheduler error: {e}. Scheduling paused — reload config to resume."
                )));
                continue;
            }

            match state.earliest() {
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
                let (names, error) = state.take_due(SystemTime::now());
                due_names = names;
                if let Some(e) = error {
                    state.paused = true;
                    let _ = ui_tx.send(log_entry_now(format!(
                        "Scheduler error: {e}. Scheduling paused — reload config to resume."
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

    fn repo_entry(name: &str, every: &str, next_run: Option<SystemTime>) -> RepoSchedule {
        RepoSchedule {
            name: name.to_string(),
            schedule: ScheduleConfig {
                enabled: true,
                every: Some(every.to_string()),
                cron: None,
                on_startup: false,
                jitter_seconds: 0,
                passphrase_prompt_timeout_seconds: 300,
            },
            next_run,
        }
    }

    /// Assert the command is a scheduled run of exactly `expected` repos.
    fn assert_runs(cmd: &AppCommand, expected: &[&str]) {
        let AppCommand::RunBackupRepos { repo_names } = cmd else {
            unreachable!("expected RunBackupRepos, got {cmd:?}");
        };
        assert_eq!(repo_names, expected, "unexpected due set");
    }

    /// Helper: set up scheduler infrastructure for tests.
    fn setup(
        enabled: bool,
        paused: bool,
        next_run: Option<SystemTime>,
    ) -> (
        Arc<Mutex<SchedulerState>>,
        Arc<AtomicBool>,
        Sender<()>,
        Receiver<AppCommand>,
    ) {
        let state = Arc::new(Mutex::new(SchedulerState {
            enabled,
            paused,
            repos: vec![repo_entry("repo-a", "1s", next_run)],
        }));
        let backup_running = Arc::new(AtomicBool::new(false));
        let (notify_tx, notify_rx) = crossbeam_channel::bounded::<()>(1);
        let (app_tx, app_rx) = crossbeam_channel::unbounded::<AppCommand>();
        let (ui_tx, _ui_rx) = crossbeam_channel::unbounded::<UiEvent>();

        spawn_scheduler(
            app_tx,
            ui_tx,
            state.clone(),
            backup_running.clone(),
            notify_rx,
        );

        (state, backup_running, notify_tx, app_rx)
    }

    #[test]
    fn fires_when_next_run_arrives() {
        let (_state, _running, _notify_tx, app_rx) = setup(
            true,
            false,
            Some(SystemTime::now() + Duration::from_millis(30)),
        );

        // Should fire within a reasonable time.
        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    /// Regression test for GitHub #110: after system sleep, `SystemTime` has
    /// advanced past `next_run` even though a monotonic clock would not have.
    /// The scheduler must fire promptly rather than wait a full interval.
    #[test]
    fn fires_when_next_run_is_in_the_past() {
        let (_state, _running, _notify_tx, app_rx) = setup(
            true,
            false,
            Some(SystemTime::now() - Duration::from_secs(10)),
        );

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    #[test]
    fn fires_only_for_the_due_repo() {
        let state = Arc::new(Mutex::new(SchedulerState {
            enabled: true,
            paused: false,
            repos: vec![
                repo_entry(
                    "hourly",
                    "1h",
                    Some(SystemTime::now() + Duration::from_secs(3600)),
                ),
                repo_entry(
                    "nas",
                    "1s",
                    Some(SystemTime::now() + Duration::from_millis(30)),
                ),
            ],
        }));
        let backup_running = Arc::new(AtomicBool::new(false));
        let (_notify_tx, notify_rx) = crossbeam_channel::bounded::<()>(1);
        let (app_tx, app_rx) = crossbeam_channel::unbounded::<AppCommand>();
        let (ui_tx, _ui_rx) = crossbeam_channel::unbounded::<UiEvent>();

        spawn_scheduler(app_tx, ui_tx, state.clone(), backup_running, notify_rx);

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_runs(&cmd, &["nas"]);

        // The hourly repo keeps its distant slot.
        let s = state.lock().unwrap();
        assert!(
            s.repos.first().unwrap().next_run.unwrap()
                > SystemTime::now() + Duration::from_secs(3000)
        );
    }

    #[test]
    fn disabled_repo_is_never_due() {
        let mut off = repo_entry(
            "off",
            "1s",
            Some(SystemTime::now() - Duration::from_secs(1)),
        );
        off.schedule.enabled = false;
        let state = Arc::new(Mutex::new(SchedulerState {
            enabled: true,
            paused: false,
            repos: vec![
                off,
                repo_entry(
                    "on",
                    "1s",
                    Some(SystemTime::now() + Duration::from_millis(30)),
                ),
            ],
        }));
        let backup_running = Arc::new(AtomicBool::new(false));
        let (_notify_tx, notify_rx) = crossbeam_channel::bounded::<()>(1);
        let (app_tx, app_rx) = crossbeam_channel::unbounded::<AppCommand>();
        let (ui_tx, _ui_rx) = crossbeam_channel::unbounded::<UiEvent>();

        spawn_scheduler(app_tx, ui_tx, state.clone(), backup_running, notify_rx);

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_runs(&cmd, &["on"]);
    }

    #[test]
    fn blocks_while_backup_running() {
        // Set backup_running *before* spawning the scheduler to avoid a race
        // where the scheduler sees next_run in the past and fires before we
        // can set the flag.
        let state = Arc::new(Mutex::new(SchedulerState {
            enabled: true,
            paused: false,
            repos: vec![repo_entry(
                "repo-a",
                "1s",
                Some(SystemTime::now() - Duration::from_millis(10)),
            )],
        }));
        let backup_running = Arc::new(AtomicBool::new(true));
        let (notify_tx, notify_rx) = crossbeam_channel::bounded::<()>(1);
        let (app_tx, app_rx) = crossbeam_channel::unbounded::<AppCommand>();
        let (ui_tx, _ui_rx) = crossbeam_channel::unbounded::<UiEvent>();

        spawn_scheduler(
            app_tx,
            ui_tx,
            state.clone(),
            backup_running.clone(),
            notify_rx,
        );

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
        // Start paused with no next_run.
        let (state, _running, notify_tx, app_rx) = setup(true, true, None);

        // Nothing should fire while paused.
        thread::sleep(Duration::from_millis(100));
        assert!(app_rx.try_recv().is_err());

        // Simulate config reload: unpause and set imminent next_run.
        {
            let mut s = state.lock().unwrap();
            s.paused = false;
            s.repos.first_mut().unwrap().next_run =
                Some(SystemTime::now() + Duration::from_millis(20));
        }
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    #[test]
    fn disable_blocks_indefinitely() {
        let (state, _running, notify_tx, app_rx) = setup(false, false, None);

        // Disabled — nothing should fire.
        thread::sleep(Duration::from_millis(100));
        assert!(app_rx.try_recv().is_err());

        // Enable and set next_run.
        {
            let mut s = state.lock().unwrap();
            s.enabled = true;
            s.repos.first_mut().unwrap().next_run =
                Some(SystemTime::now() + Duration::from_millis(20));
        }
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_runs(&cmd, &["repo-a"]);
    }

    #[test]
    fn brief_is_per_repo_when_cadences_differ() {
        let hourly = repo_entry("a", "1h", None).schedule;
        let sixty_min = repo_entry("b", "60m", None).schedule;
        let daily = repo_entry("c", "1d", None).schedule;

        // `60m` and `1h` are the same schedule, not a mixed one.
        assert_eq!(repos_schedule_brief(&[&hourly, &sixty_min], false), "1h");
        assert_eq!(repos_schedule_brief(&[&hourly, &daily], false), "per-repo");
        assert_eq!(repos_schedule_brief(&[&hourly], true), "Off");
        assert_eq!(repos_schedule_brief(&[], false), "Off");
    }
}
