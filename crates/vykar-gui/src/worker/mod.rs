use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crossbeam_channel::{Receiver, Sender};
use vykar_core::app;

use crate::messages::{AppCommand, RepoInfoData, UiEvent};
use crate::repo_helpers::{format_repo_name, send_log};
use crate::scheduler;
use crate::view_models::send_structured_data;

mod actions;
mod backup;
mod config_cmds;
mod mount;
mod repo_info;
mod shared;

pub(super) struct WorkerContext {
    pub(super) passphrases: HashMap<String, zeroize::Zeroizing<String>>,
    /// Last-known per-repo card data, keyed by repository URL. Lets a
    /// single-repo `FetchRepoInfo` re-emit a full, in-order `RepoModelData`.
    pub(super) repo_info: HashMap<String, RepoInfoData>,
    pub(super) config_display_path: PathBuf,
    pub(super) runtime: app::RuntimeConfig,

    pub(super) app_tx: Sender<AppCommand>,
    pub(super) ui_tx: Sender<UiEvent>,
    pub(super) sched_notify_tx: Sender<()>,

    pub(super) scheduler: Arc<Mutex<scheduler::SchedulerState>>,
    pub(super) backup_running: Arc<AtomicBool>,
    /// Set while any operation (backup *or* UI read) is running. Drives the
    /// tray "Cancel" item, mirroring the window Cancel button.
    pub(super) operation_running: Arc<AtomicBool>,
    pub(super) cancel_requested: Arc<AtomicBool>,

    pub(super) scheduler_lock_held: bool,
    pub(super) schedule_paused: bool,

    pub(super) mount: Option<mount::MountHandle>,
}

fn startup(ctx: &mut WorkerContext) {
    let schedule_brief = rebuild_scheduler_state(ctx);
    let _ = ctx.ui_tx.send(UiEvent::ConfigInfo {
        path: ctx.config_display_path.display().to_string(),
        schedule_brief,
    });

    send_structured_data(&ctx.ui_tx, &ctx.runtime.repos);

    match std::fs::read_to_string(&ctx.config_display_path) {
        Ok(text) => {
            let _ = ctx.ui_tx.send(UiEvent::ConfigText(text));
        }
        Err(e) => {
            send_log(
                &ctx.ui_tx,
                format!(
                    "Could not read config file for editor ({}): {e}",
                    ctx.config_display_path.display()
                ),
            );
        }
    }

    let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);

    // `on_startup` is per repository: only the repos that ask for it run now.
    if ctx.scheduler_lock_held {
        let on_startup: Vec<String> = ctx
            .runtime
            .repos
            .iter()
            .filter(|r| r.config.schedule.enabled && r.config.schedule.on_startup)
            .map(format_repo_name)
            .collect();
        if !on_startup.is_empty() {
            send_log(
                &ctx.ui_tx,
                format!(
                    "Scheduled on-startup backup requested by configuration for: {}",
                    on_startup.join(", ")
                ),
            );
            let _ = ctx.app_tx.send(AppCommand::RunBackupRepos {
                repo_names: on_startup,
            });
        }
    }
}

/// Rebuild the scheduler's per-repo entries from `ctx.runtime.repos` and return
/// the Overview schedule summary. Shared by startup and config reload so the
/// two cannot drift apart.
pub(super) fn rebuild_scheduler_state(ctx: &mut WorkerContext) -> String {
    let schedules: Vec<&vykar_core::config::ScheduleConfig> = ctx
        .runtime
        .repos
        .iter()
        .map(|r| &r.config.schedule)
        .collect();
    let paused = ctx.schedule_paused || !ctx.scheduler_lock_held;

    if let Ok(mut state) = ctx.scheduler.lock() {
        state.enabled =
            ctx.scheduler_lock_held && ctx.runtime.repos.iter().any(|r| r.config.schedule.enabled);
        state.paused = paused;
        // `next_run: None` — the scheduler thread computes each repo's first
        // slot, so a bad cadence surfaces through its pause-and-log path.
        state.repos = ctx
            .runtime
            .repos
            .iter()
            .map(|r| scheduler::RepoSchedule {
                name: format_repo_name(r),
                schedule: r.config.schedule.clone(),
                next_run: None,
            })
            .collect();
    }
    let _ = ctx.sched_notify_tx.try_send(());

    if ctx.scheduler_lock_held {
        scheduler::repos_schedule_brief(&schedules, paused)
    } else {
        "Off".to_string()
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_worker(
    app_tx: Sender<AppCommand>,
    cmd_rx: Receiver<AppCommand>,
    ui_tx: Sender<UiEvent>,
    scheduler: Arc<Mutex<scheduler::SchedulerState>>,
    backup_running: Arc<AtomicBool>,
    operation_running: Arc<AtomicBool>,
    cancel_requested: Arc<AtomicBool>,
    runtime: app::RuntimeConfig,
    scheduler_lock_held: bool,
    sched_notify_tx: Sender<()>,
) {
    let config_display_path = dunce::canonicalize(runtime.source.path())
        .unwrap_or_else(|_| runtime.source.path().to_path_buf());

    let mut ctx = WorkerContext {
        passphrases: HashMap::new(),
        repo_info: HashMap::new(),
        config_display_path,
        runtime,
        app_tx,
        ui_tx,
        sched_notify_tx,
        scheduler,
        backup_running,
        operation_running,
        cancel_requested,
        scheduler_lock_held,
        schedule_paused: !scheduler_lock_held,
        mount: None,
    };

    startup(&mut ctx);

    while let Ok(cmd) = cmd_rx.recv() {
        match cmd {
            AppCommand::RunBackupAll { scheduled } => {
                backup::handle_backup_all(&mut ctx, scheduled)
            }
            AppCommand::RunBackupRepos { repo_names } => {
                backup::handle_backup_repos(&mut ctx, repo_names)
            }
            AppCommand::RunBackupRepo { repo_name } => {
                backup::handle_backup_repo(&mut ctx, repo_name)
            }
            AppCommand::RunBackupSource { source_label } => {
                backup::handle_backup_source(&mut ctx, source_label)
            }
            AppCommand::FetchAllRepoInfo => repo_info::handle_fetch_all_repo_info(&mut ctx),
            AppCommand::FetchRepoInfo { repo_name } => {
                repo_info::handle_fetch_repo_info(&mut ctx, repo_name)
            }
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
            AppCommand::DiffSnapshots {
                repo_name,
                snapshot_a,
                snapshot_b,
            } => actions::handle_diff_snapshots(&mut ctx, repo_name, snapshot_a, snapshot_b),
            AppCommand::DeleteSnapshots {
                repo_name,
                snapshot_names,
            } => actions::handle_delete_snapshots(&mut ctx, repo_name, snapshot_names),
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
            AppCommand::ClearRepoLocks { repo_name } => {
                config_cmds::handle_clear_repo_locks(&mut ctx, repo_name)
            }
            AppCommand::ClearRepoSessions { repo_name } => {
                config_cmds::handle_clear_repo_sessions(&mut ctx, repo_name)
            }
            AppCommand::StartMount {
                repo_name,
                snapshot_name,
            } => mount::handle_start_mount(&mut ctx, repo_name, snapshot_name),
            AppCommand::StopMount => mount::handle_stop_mount(&mut ctx),
        }
    }

    // On worker shutdown, stop any active mount so we don't leak a listener.
    mount::handle_stop_mount(&mut ctx);
}
