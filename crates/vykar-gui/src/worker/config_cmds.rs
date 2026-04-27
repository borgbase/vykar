use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use crate::config_helpers;
use crate::messages::{AppCommand, UiEvent};
use crate::repo_helpers::send_log;
use crate::scheduler;
use crate::view_models::send_structured_data;
use vykar_core::{repo::lock, storage};
use vykar_types::error::Result;

use super::shared::select_repo_or_log;
use super::WorkerContext;

pub(super) fn handle_open_config_file(ctx: &WorkerContext) {
    let path = ctx.runtime.source.path().display().to_string();
    send_log(&ctx.ui_tx, format!("Opening config file: {path}"));
    let _ = std::process::Command::new("open").arg(&path).spawn();
}

pub(super) fn handle_reload_config(ctx: &mut WorkerContext) {
    let config_path = dunce::canonicalize(ctx.runtime.source.path())
        .unwrap_or_else(|_| ctx.runtime.source.path().to_path_buf());
    apply_config(ctx, config_path, false);
}

pub(super) fn handle_switch_config(ctx: &mut WorkerContext) {
    let picked = tinyfiledialogs::open_file_dialog(
        "Open vykar config",
        "",
        Some((&["*.yaml", "*.yml"], "YAML files")),
    );
    if let Some(path_str) = picked {
        apply_config(ctx, PathBuf::from(path_str), true);
    }
}

pub(super) fn handle_save_and_apply_config(ctx: &mut WorkerContext, yaml_text: String) {
    let config_path = ctx.config_display_path.clone();
    let tmp_path = config_path.with_extension("yaml.tmp");
    if let Err(e) = config_helpers::write_tmp_secure(&tmp_path, yaml_text.as_bytes()) {
        let _ = ctx
            .ui_tx
            .send(UiEvent::ConfigSaveError(format!("Write failed: {e}")));
        return;
    }

    if let Err(msg) = config_helpers::validate_config(&tmp_path) {
        let _ = std::fs::remove_file(&tmp_path);
        let _ = ctx.ui_tx.send(UiEvent::ConfigSaveError(msg));
        return;
    }

    if let Err(e) = std::fs::rename(&tmp_path, &config_path) {
        let _ = std::fs::remove_file(&tmp_path);
        let _ = ctx
            .ui_tx
            .send(UiEvent::ConfigSaveError(format!("Rename failed: {e}")));
        return;
    }

    // apply_config re-runs validate_config internally, which is
    // redundant but harmless — it keeps the function self-contained.
    if apply_config(ctx, config_path, false) {
        send_log(&ctx.ui_tx, "Configuration saved and applied.");
    } else {
        let _ = ctx.ui_tx.send(UiEvent::ConfigSaveError(
            "Config saved to disk but failed to apply. Check log for details.".into(),
        ));
    }
}

pub(super) fn handle_clear_repo_locks(ctx: &mut WorkerContext, repo_name: String) {
    handle_repo_recovery_action(ctx, repo_name, "lock", "lock marker", lock::break_lock);
}

pub(super) fn handle_clear_repo_sessions(ctx: &mut WorkerContext, repo_name: String) {
    handle_repo_recovery_action(
        ctx,
        repo_name,
        "sessions",
        "session file",
        lock::clear_all_sessions,
    );
}

fn handle_repo_recovery_action<F>(
    ctx: &mut WorkerContext,
    repo_name: String,
    action_label: &str,
    item_label: &str,
    action: F,
) where
    F: FnOnce(&dyn vykar_core::storage::StorageBackend) -> Result<usize>,
{
    let repo = match select_repo_or_log(ctx, &ctx.runtime.repos, &repo_name) {
        Some(r) => r,
        None => return,
    };
    let backend =
        match storage::backend_from_config(&repo.config.repository, repo.config.limits.connections)
        {
            Ok(b) => b,
            Err(e) => {
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] cannot open storage: {e}"),
                );
                return;
            }
        };

    match action(backend.as_ref()) {
        Ok(removed) => {
            send_log(
                &ctx.ui_tx,
                format!("[{repo_name}] cleared {removed} {item_label}(s)."),
            );
            let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
            let _ = ctx.app_tx.send(AppCommand::RefreshSnapshots {
                repo_selector: repo_name,
            });
        }
        Err(e) => {
            send_log(
                &ctx.ui_tx,
                format!("[{repo_name}] clear {action_label} failed: {e}"),
            );
        }
    }
}

/// Apply a (possibly new) config file: load, validate, update runtime state, and notify the UI.
/// When `update_source` is true the runtime source path is switched to `config_path`.
/// Returns `true` on success, `false` on failure.
pub(super) fn apply_config(
    ctx: &mut WorkerContext,
    config_path: PathBuf,
    update_source: bool,
) -> bool {
    let repos = match config_helpers::validate_config(&config_path) {
        Ok(v) => v,
        Err(msg) => {
            send_log(&ctx.ui_tx, format!("{msg} Keeping previous config."));
            return false;
        }
    };
    let schedule = repos[0].config.schedule.clone();

    if update_source {
        use vykar_core::config::ConfigSource;
        ctx.runtime.source = ConfigSource::SearchOrder {
            path: config_path.clone(),
            level: "user",
        };
    }
    ctx.runtime.repos = repos;
    ctx.passphrases.clear();

    if let Ok(mut state) = ctx.scheduler.lock() {
        state.enabled = schedule.enabled && ctx.scheduler_lock_held;
        state.paused = ctx.schedule_paused || !ctx.scheduler_lock_held;
        state.every = schedule
            .every_duration()
            .unwrap_or(Duration::from_secs(24 * 60 * 60));
        state.cron = schedule.cron.clone();
        state.jitter_seconds = schedule.jitter_seconds;
        let delay = vykar_core::app::scheduler::next_run_delay(&schedule)
            .unwrap_or(Duration::from_secs(24 * 60 * 60));
        state.next_run = Some(SystemTime::now() + delay);
    }
    let _ = ctx.sched_notify_tx.try_send(());

    let canonical = dunce::canonicalize(&config_path).unwrap_or_else(|_| config_path.clone());
    ctx.config_display_path = canonical.clone();

    let schedule_brief = if ctx.scheduler_lock_held {
        scheduler::schedule_brief(&schedule, ctx.schedule_paused)
    } else {
        "Off".to_string()
    };
    let _ = ctx.ui_tx.send(UiEvent::ConfigInfo {
        path: canonical.display().to_string(),
        schedule_brief,
    });
    send_structured_data(&ctx.ui_tx, &ctx.runtime.repos);
    let _ = ctx
        .app_tx
        .send(crate::messages::AppCommand::FetchAllRepoInfo);
    send_log(&ctx.ui_tx, "Configuration reloaded.");

    match std::fs::read_to_string(&canonical) {
        Ok(text) => {
            let _ = ctx.ui_tx.send(UiEvent::ConfigText(text));
        }
        Err(e) => {
            send_log(
                &ctx.ui_tx,
                format!("Could not read config file for editor: {e}"),
            );
        }
    }

    true
}
