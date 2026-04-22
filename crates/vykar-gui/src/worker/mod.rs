use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use crossbeam_channel::{Receiver, Sender};
use vykar_core::app;

use crate::messages::{AppCommand, UiEvent};
use crate::repo_helpers::send_log;
use crate::scheduler;
use crate::view_models::send_structured_data;

mod actions;
mod backup;
mod config_cmds;
mod repo_info;
mod shared;

pub(super) struct WorkerContext {
    pub(super) passphrases: HashMap<String, zeroize::Zeroizing<String>>,
    pub(super) config_display_path: PathBuf,
    pub(super) runtime: app::RuntimeConfig,

    pub(super) app_tx: Sender<AppCommand>,
    pub(super) ui_tx: Sender<UiEvent>,
    pub(super) sched_notify_tx: Sender<()>,

    pub(super) scheduler: Arc<Mutex<scheduler::SchedulerState>>,
    pub(super) backup_running: Arc<AtomicBool>,
    pub(super) cancel_requested: Arc<AtomicBool>,

    pub(super) scheduler_lock_held: bool,
    pub(super) schedule_paused: bool,
}

fn startup(ctx: &mut WorkerContext) {
    let schedule = ctx.runtime.schedule();
    let schedule_delay = vykar_core::app::scheduler::next_run_delay(&schedule)
        .unwrap_or_else(|_| Duration::from_secs(24 * 60 * 60));

    if let Ok(mut state) = ctx.scheduler.lock() {
        state.enabled = schedule.enabled && ctx.scheduler_lock_held;
        state.paused = !ctx.scheduler_lock_held;
        state.every = schedule
            .every_duration()
            .unwrap_or(Duration::from_secs(24 * 60 * 60));
        state.cron = schedule.cron.clone();
        state.jitter_seconds = schedule.jitter_seconds;
        state.next_run = Some(SystemTime::now() + schedule_delay);
    }
    let _ = ctx.sched_notify_tx.try_send(());

    let schedule_brief = if ctx.scheduler_lock_held {
        scheduler::schedule_brief(&schedule, false)
    } else {
        "Off".to_string()
    };
    let _ = ctx.ui_tx.send(UiEvent::ConfigInfo {
        path: ctx.config_display_path.display().to_string(),
        schedule_brief,
    });

    send_structured_data(&ctx.ui_tx, &ctx.runtime.repos);

    if let Ok(text) = std::fs::read_to_string(&ctx.config_display_path) {
        let _ = ctx.ui_tx.send(UiEvent::ConfigText(text));
    }

    let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);

    if ctx.scheduler_lock_held && schedule.enabled && schedule.on_startup {
        send_log(
            &ctx.ui_tx,
            "Scheduled on-startup backup requested by configuration.",
        );
        let _ = ctx
            .app_tx
            .send(AppCommand::RunBackupAll { scheduled: true });
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_worker(
    app_tx: Sender<AppCommand>,
    cmd_rx: Receiver<AppCommand>,
    ui_tx: Sender<UiEvent>,
    scheduler: Arc<Mutex<scheduler::SchedulerState>>,
    backup_running: Arc<AtomicBool>,
    cancel_requested: Arc<AtomicBool>,
    runtime: app::RuntimeConfig,
    scheduler_lock_held: bool,
    sched_notify_tx: Sender<()>,
) {
    let config_display_path = dunce::canonicalize(runtime.source.path())
        .unwrap_or_else(|_| runtime.source.path().to_path_buf());

    let mut ctx = WorkerContext {
        passphrases: HashMap::new(),
        config_display_path,
        runtime,
        app_tx,
        ui_tx,
        sched_notify_tx,
        scheduler,
        backup_running,
        cancel_requested,
        scheduler_lock_held,
        schedule_paused: !scheduler_lock_held,
    };

    startup(&mut ctx);

    while let Ok(cmd) = cmd_rx.recv() {
        match cmd {
            AppCommand::RunBackupAll { scheduled } => {
                backup::handle_backup_all(&mut ctx, scheduled)
            }
            AppCommand::RunBackupRepo { repo_name } => {
                backup::handle_backup_repo(&mut ctx, repo_name)
            }
            AppCommand::RunBackupSource { source_label } => {
                backup::handle_backup_source(&mut ctx, source_label)
            }
            AppCommand::FetchAllRepoInfo => repo_info::handle_fetch_all_repo_info(&mut ctx),
            AppCommand::RefreshSnapshots { repo_selector } => {
                repo_info::handle_refresh_snapshots(&mut ctx, repo_selector)
            }
            AppCommand::FetchSnapshotContents {
                repo_name,
                snapshot_name,
            } => repo_info::handle_fetch_snapshot_contents(&mut ctx, repo_name, snapshot_name),
            AppCommand::RestoreSelected {
                repo_name,
                snapshot,
                dest,
                paths,
            } => actions::handle_restore_selected(&mut ctx, repo_name, snapshot, dest, paths),
            AppCommand::DeleteSnapshot {
                repo_name,
                snapshot_name,
            } => actions::handle_delete_snapshot(&mut ctx, repo_name, snapshot_name),
            AppCommand::PruneRepo { repo_name } => actions::handle_prune_repo(&mut ctx, repo_name),
            AppCommand::FindFiles {
                repo_name,
                name_pattern,
            } => actions::handle_find_files(&mut ctx, repo_name, name_pattern),
            AppCommand::OpenConfigFile => config_cmds::handle_open_config_file(&ctx),
            AppCommand::ReloadConfig => config_cmds::handle_reload_config(&mut ctx),
            AppCommand::SwitchConfig => config_cmds::handle_switch_config(&mut ctx),
            AppCommand::SaveAndApplyConfig { yaml_text } => {
                config_cmds::handle_save_and_apply_config(&mut ctx, yaml_text)
            }
        }
    }
}
