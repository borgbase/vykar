use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use vykar_core::config::ScheduleConfig;

use crate::messages::{log_entry_now, AppCommand, UiEvent};

#[derive(Debug)]
pub(crate) struct SchedulerState {
    pub enabled: bool,
    pub paused: bool,
    pub every: Duration,
    pub cron: Option<String>,
    pub jitter_seconds: u64,
    pub next_run: Option<Instant>,
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

pub(crate) fn schedule_description(schedule: &ScheduleConfig, paused: bool) -> String {
    let timing = if let Some(ref cron) = schedule.cron {
        format!("cron={cron}")
    } else {
        format!("every={}", schedule.every.as_deref().unwrap_or("24h"))
    };
    format!(
        "enabled={}, {timing}, on_startup={}, jitter_seconds={}, paused={}",
        schedule.enabled, schedule.on_startup, schedule.jitter_seconds, paused,
    )
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
                    Ok(delay) => state.next_run = Some(Instant::now() + delay),
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
                    let now = Instant::now();
                    if now >= next {
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
                        next - now
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
                if Instant::now() >= next && !backup_running.load(Ordering::SeqCst) {
                    should_run = true;
                    match compute_scheduler_delay(&state) {
                        Ok(delay) => state.next_run = Some(Instant::now() + delay),
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

    /// Helper: set up scheduler infrastructure for tests.
    fn setup(
        enabled: bool,
        paused: bool,
        next_run: Option<Instant>,
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
            Some(Instant::now() + Duration::from_millis(30)),
        );

        // Should fire within a reasonable time.
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
            next_run: Some(Instant::now() - Duration::from_millis(10)),
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
            s.next_run = Some(Instant::now() + Duration::from_millis(20));
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
            s.next_run = Some(Instant::now() + Duration::from_millis(20));
        }
        let _ = notify_tx.try_send(());

        let cmd = app_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(cmd, AppCommand::RunBackupAll { scheduled: true }));
    }
}
