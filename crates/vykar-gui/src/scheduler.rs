use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Local;
use crossbeam_channel::Sender;
use vykar_core::config::ScheduleConfig;

use crate::messages::{AppCommand, UiEvent};

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
) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(1));

        let mut should_run = false;

        {
            let mut state = match scheduler.lock() {
                Ok(s) => s,
                Err(_) => break,
            };

            if !state.enabled || state.paused {
                continue;
            }

            if state.next_run.is_none() {
                match compute_scheduler_delay(&state) {
                    Ok(delay) => state.next_run = Some(Instant::now() + delay),
                    Err(e) => {
                        state.paused = true;
                        state.next_run = None;
                        let _ = ui_tx.send(UiEvent::LogEntry {
                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                            message: format!(
                                "Scheduler error: {e}. Scheduling paused — reload config to resume."
                            ),
                        });
                        continue;
                    }
                }
            }

            if let Some(next) = state.next_run {
                if Instant::now() >= next && !backup_running.load(Ordering::SeqCst) {
                    should_run = true;
                    match compute_scheduler_delay(&state) {
                        Ok(delay) => state.next_run = Some(Instant::now() + delay),
                        Err(e) => {
                            state.paused = true;
                            state.next_run = None;
                            let _ = ui_tx.send(UiEvent::LogEntry {
                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                message: format!(
                                    "Scheduler error: {e}. Scheduling paused — reload config to resume."
                                ),
                            });
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
