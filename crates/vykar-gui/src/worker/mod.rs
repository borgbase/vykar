use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crossbeam_channel::{Receiver, Sender};
use vykar_core::app;

use crate::cancel::CancelSlot;
use crate::messages::{AppCommand, RepoInfoData, UiEvent};
use crate::repo_helpers::send_log;
use crate::scheduler;
use crate::view_models::send_structured_data;

mod actions;
mod backup;
mod config_cmds;
mod mount;
mod repo_info;
mod shared;
#[cfg(test)]
mod test_support;

pub(super) struct WorkerContext {
    /// Passphrase policy and the backup cycle; caches accepted passphrases.
    pub(super) service: app::AppService,
    /// Last-known per-repo card data, keyed by repository URL. Lets a
    /// single-repo `FetchRepoInfo` re-emit a full, in-order `RepoModelData`.
    pub(super) repo_info: HashMap<String, RepoInfoData>,
    pub(super) config_display_path: PathBuf,
    pub(super) runtime: app::RuntimeConfig,

    pub(super) app_tx: Sender<AppCommand>,
    pub(super) ui_tx: Sender<UiEvent>,
    pub(super) sched_notify_tx: Sender<()>,

    pub(super) scheduler: Arc<Mutex<scheduler::SchedulerState>>,
    /// Set while any operation (backup *or* UI read) is running. Drives the
    /// tray "Cancel" item, mirroring the window Cancel button.
    pub(super) operation_running: Arc<AtomicBool>,
    pub(super) cancel_requested: CancelSlot,

    pub(super) scheduler_lock_held: bool,
    pub(super) schedule_paused: bool,

    pub(super) mount: Option<mount::MountHandle>,
}

fn startup(ctx: &mut WorkerContext) {
    // Queued before the scheduler state is loaded: the scheduler thread may
    // enqueue the on-startup order as soon as it is, and the info fetch runs first.
    let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);

    let schedule_brief = rebuild_scheduler_state(ctx, true);
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

    // `on_startup` is per repository and lives in the scheduler: the repos
    // that ask for it are due now, and the scheduler thread hands them out.
    if ctx.scheduler_lock_held {
        let on_startup = ctx
            .scheduler
            .lock()
            .map(|s| s.due_names(SystemTime::now()))
            .unwrap_or_default();
        if !on_startup.is_empty() {
            send_log(
                &ctx.ui_tx,
                format!(
                    "Scheduled on-startup backup requested by configuration for: {}",
                    on_startup.join(", ")
                ),
            );
        }
    }
}

/// Rebuild the scheduler's per-repo entries from `ctx.runtime.repos` and return
/// the Overview schedule summary. Shared by startup and config reload so the
/// two cannot drift apart. `honor_on_startup` is true only at startup: a
/// reload must not re-trigger a repo's `on_startup` run.
pub(super) fn rebuild_scheduler_state(ctx: &mut WorkerContext, honor_on_startup: bool) -> String {
    let schedules: Vec<&vykar_core::config::ScheduleConfig> = ctx
        .runtime
        .repos
        .iter()
        .map(|r| &r.config.schedule)
        .collect();
    let paused = ctx.schedule_paused || !ctx.scheduler_lock_held;

    let mut cadence_errors = Vec::new();
    if let Ok(mut state) = ctx.scheduler.lock() {
        state.set_paused(paused);
        cadence_errors = state.set_repos(
            ctx.runtime
                .repos
                .iter()
                .map(|r| (r.label_or_url().to_string(), r.config.schedule.clone()))
                .collect(),
            SystemTime::now(),
            honor_on_startup,
        );
    }
    // Per-repo, never a global pause: an unschedulable cadence drops that one
    // repo from the timer and leaves the others running, as in the daemon.
    scheduler::send_cadence_errors(&ctx.ui_tx, cadence_errors);
    let _ = ctx.sched_notify_tx.try_send(());

    if ctx.scheduler_lock_held {
        app::scheduler::repos_schedule_brief(&schedules, paused)
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
    operation_running: Arc<AtomicBool>,
    cancel_requested: CancelSlot,
    runtime: app::RuntimeConfig,
    scheduler_lock_held: bool,
    sched_notify_tx: Sender<()>,
) {
    let config_display_path = dunce::canonicalize(runtime.source.path())
        .unwrap_or_else(|_| runtime.source.path().to_path_buf());

    let mut ctx = WorkerContext {
        service: app::AppService::new(),
        repo_info: HashMap::new(),
        config_display_path,
        runtime,
        app_tx,
        ui_tx,
        sched_notify_tx,
        scheduler,
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
            AppCommand::RunBackupRepos { repo_names, order } => {
                backup::handle_backup_repos(&mut ctx, repo_names, order)
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

#[cfg(test)]
mod tests {
    use super::test_support::{drain, harness};
    use super::*;

    const ON_STARTUP_LOG: &str = "Scheduled on-startup backup requested by configuration for: nas";

    fn on_startup_harness() -> test_support::Harness {
        harness(|tmp| {
            format!(
                "repositories:\n  - url: '{}'\n    label: nas\n\
                 encryption:\n  mode: aes256gcm\n  passphrase: gui-test\n\
                 schedule:\n  enabled: true\n  every: 1h\n  on_startup: true\n\
                 sources:\n  - path: '{}'\n",
                tmp.join("repo").display(),
                tmp.display(),
            )
        })
    }

    /// The on-startup run is the scheduler's: startup queues no backup itself,
    /// the info fetch is queued first, and the repo is due in the scheduler.
    #[test]
    fn startup_leaves_the_on_startup_run_to_the_scheduler() {
        let mut h = on_startup_harness();
        startup(&mut h.ctx);

        let cmds: Vec<AppCommand> = h.app_rx.try_iter().collect();
        assert!(
            matches!(cmds.as_slice(), [AppCommand::FetchAllRepoInfo]),
            "unexpected commands: {cmds:?}"
        );
        let (logs, _) = drain(&h.ui_rx);
        assert_eq!(
            logs.iter().filter(|m| m.as_str() == ON_STARTUP_LOG).count(),
            1,
            "logs: {logs:?}"
        );
        let due = h.ctx.scheduler.lock().unwrap().due_names(SystemTime::now());
        assert_eq!(due, ["nas"]);
    }

    /// Applying a config (reload, switch, save) never honours `on_startup`.
    #[test]
    fn apply_config_does_not_make_the_on_startup_repo_due() {
        let mut h = on_startup_harness();
        let path = h.tmp.path().join("config.yaml");
        assert!(config_cmds::apply_config(&mut h.ctx, path, false));

        let due = h.ctx.scheduler.lock().unwrap().due_names(SystemTime::now());
        assert!(due.is_empty(), "due after apply: {due:?}");
        let (logs, _) = drain(&h.ui_rx);
        assert!(!logs.iter().any(|m| m == ON_STARTUP_LOG), "logs: {logs:?}");
    }
}
