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

#[derive(Debug)]
pub(crate) struct SchedulerState {
    pub enabled: bool,
    pub paused: bool,
    pub every: Duration,
    pub cron: Option<String>,
    pub jitter_seconds: u64,
    pub next_run: Option<SystemTime>,
}

impl Default for SchedulerState {
    fn default() -> Self {
        Self {
            enabled: false,
            paused: false,
            every: Duration::from_secs(24 * 60 * 60),
            cron: None,
            jitter_seconds: 0,
            next_run: None,
        }
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

            if state.next_run.is_none() {
                match compute_scheduler_delay(&state) {
                    Ok(delay) => state.next_run = Some(SystemTime::now() + delay),
                    Err(e) => {
                        state.paused = true;
                        state.next_run = None;
                        let _ = ui_tx.send(log_entry_now(format!(
                            "Scheduler error: {e}. Scheduling paused — reload config to resume."
                        )));
                        continue;
                    }
                }
            }

            match state.next_run {
                Some(next) => {
                    let wait = capped_wait(next, SystemTime::now());
                    if wait.is_zero() {
                        if backup_running.load(Ordering::SeqCst) {
                            // Backup is running and next_run has passed — block
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
                    // No next_run — block until notified.
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
        let mut should_run = false;
        {
            let mut state = match scheduler.lock() {
                Ok(s) => s,
                Err(_) => break,
            };

            if !state.enabled || state.paused {
                continue;
            }

            if let Some(next) = state.next_run {
                if capped_wait(next, SystemTime::now()).is_zero()
                    && !backup_running.load(Ordering::SeqCst)
                {
                    should_run = true;
                    match compute_scheduler_delay(&state) {
                        Ok(delay) => state.next_run = Some(SystemTime::now() + delay),
                        Err(e) => {
                            state.paused = true;
                            state.next_run = None;
                            let _ = ui_tx.send(log_entry_now(format!(
                                "Scheduler error: {e}. Scheduling paused — reload config to resume."
                            )));
                        }
                    }
                }
            }
        }

        if should_run
            && app_tx
                .send(AppCommand::RunBackupAll { scheduled: true })
                .is_err()
        {
            break;
        }
    });
}

pub(crate) fn compute_scheduler_delay(
    state: &SchedulerState,
) -> std::result::Result<Duration, vykar_types::error::VykarError> {
    if let Some(ref cron_expr) = state.cron {
        let tmp = ScheduleConfig {
            enabled: true,
            every: None,
            cron: Some(cron_expr.clone()),
            on_startup: false,
            jitter_seconds: state.jitter_seconds,
            passphrase_prompt_timeout_seconds: 300,
        };
        vykar_core::app::scheduler::next_run_delay(&tmp)
    } else {
        Ok(state.every + vykar_core::app::scheduler::random_jitter(state.jitter_seconds))
    }
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
            every: Duration::from_millis(50),
            cron: None,
            jitter_seconds: 0,
            next_run,
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
        assert!(matches!(cmd, AppCommand::RunBackupAll { scheduled: true }));
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
        assert!(matches!(cmd, AppCommand::RunBackupAll { scheduled: true }));
    }

    #[test]
    fn blocks_while_backup_running() {
        // Set backup_running *before* spawning the scheduler to avoid a race
        // where the scheduler sees next_run in the past and fires before we
        // can set the flag.
        let state = Arc::new(Mutex::new(SchedulerState {
            enabled: true,
            paused: false,
            every: Duration::from_millis(50),
            cron: None,
            jitter_seconds: 0,
            next_run: Some(SystemTime::now() - Duration::from_millis(10)),
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
        assert!(matches!(cmd, AppCommand::RunBackupAll { scheduled: true }));
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
            s.next_run = Some(SystemTime::now() + Duration::from_millis(20));
        }
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(cmd, AppCommand::RunBackupAll { scheduled: true }));
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
            s.next_run = Some(SystemTime::now() + Duration::from_millis(20));
        }
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(cmd, AppCommand::RunBackupAll { scheduled: true }));
    }
}
