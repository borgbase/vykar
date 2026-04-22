use chrono::{DateTime, Local};
use vykar_core::app::operations;
use vykar_core::commands;
use vykar_core::commands::find::{FileStatus, FindFilter, FindScope};

use crate::messages::{AppCommand, FindResultRow, UiEvent};
use crate::repo_helpers::{find_repo_for_snapshot, get_or_resolve_passphrase, send_log};
use vykar_common::display::format_bytes;

use super::shared::{begin_ui_operation, end_ui_operation, select_repo_or_log};
use super::WorkerContext;

pub(super) fn handle_restore_selected(
    ctx: &mut WorkerContext,
    repo_name: String,
    snapshot: String,
    dest: String,
    paths: Vec<String>,
) {
    begin_ui_operation(ctx, "Restoring selected items...");

    match find_repo_for_snapshot(
        &ctx.runtime.repos,
        &repo_name,
        &snapshot,
        &mut ctx.passphrases,
    ) {
        Ok((repo, passphrase)) => {
            let path_set: std::collections::HashSet<String> = paths.into_iter().collect();
            match operations::restore_selected(
                &repo.config,
                passphrase.as_deref().map(|s| s.as_str()),
                &snapshot,
                &dest,
                &path_set,
            ) {
                Ok(stats) => {
                    send_log(
                        &ctx.ui_tx,
                        format!(
                            "Restored selected items from {} -> {} (files={}, dirs={}, symlinks={}, bytes={})",
                            snapshot,
                            dest,
                            stats.files,
                            stats.dirs,
                            stats.symlinks,
                            format_bytes(stats.total_bytes),
                        ),
                    );
                    let _ = ctx.ui_tx.send(UiEvent::RestoreFinished {
                        success: true,
                        message: format!(
                            "files={}, dirs={}, symlinks={}, bytes={}",
                            stats.files,
                            stats.dirs,
                            stats.symlinks,
                            format_bytes(stats.total_bytes),
                        ),
                    });
                }
                Err(e) => {
                    send_log(&ctx.ui_tx, format!("Restore failed: {e}"));
                    let _ = ctx.ui_tx.send(UiEvent::RestoreFinished {
                        success: false,
                        message: format!("{e}"),
                    });
                }
            }
        }
        Err(e) => {
            send_log(&ctx.ui_tx, format!("Failed to resolve snapshot: {e}"));
            let _ = ctx.ui_tx.send(UiEvent::RestoreFinished {
                success: false,
                message: format!("{e}"),
            });
        }
    }

    end_ui_operation(ctx);
}

pub(super) fn handle_delete_snapshot(
    ctx: &mut WorkerContext,
    repo_name: String,
    snapshot_name: String,
) {
    let confirmed = tinyfiledialogs::message_box_yes_no(
        "Delete Snapshot",
        &format!("Are you sure you want to delete snapshot {snapshot_name} from {repo_name}?"),
        tinyfiledialogs::MessageBoxIcon::Question,
        tinyfiledialogs::YesNo::No,
    );

    if confirmed == tinyfiledialogs::YesNo::No {
        send_log(&ctx.ui_tx, "Snapshot deletion cancelled.");
        return;
    }

    begin_ui_operation(ctx, "Deleting snapshot...");

    let repo = match select_repo_or_log(ctx, &ctx.runtime.repos, &repo_name) {
        Some(r) => r,
        None => {
            end_ui_operation(ctx);
            return;
        }
    };

    let passphrase = match get_or_resolve_passphrase(repo, &mut ctx.passphrases) {
        Ok(p) => p,
        Err(e) => {
            send_log(&ctx.ui_tx, format!("[{repo_name}] passphrase error: {e}"));
            end_ui_operation(ctx);
            return;
        }
    };

    match operations::delete_snapshot(
        &repo.config,
        passphrase.as_deref().map(|s| s.as_str()),
        &snapshot_name,
    ) {
        Ok(stats) => {
            send_log(
                &ctx.ui_tx,
                format!(
                    "[{repo_name}] Deleted snapshot '{}': {} chunks freed, {} reclaimed",
                    stats.snapshot_name,
                    stats.chunks_deleted,
                    format_bytes(stats.space_freed),
                ),
            );
            let _ = ctx.app_tx.send(AppCommand::RefreshSnapshots {
                repo_selector: repo_name,
            });
            let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
        }
        Err(e) => {
            send_log(&ctx.ui_tx, format!("[{repo_name}] delete failed: {e}"));
        }
    }
    end_ui_operation(ctx);
}

pub(super) fn handle_prune_repo(ctx: &mut WorkerContext, repo_name: String) {
    begin_ui_operation(ctx, "Pruning snapshots...");

    let repo = match select_repo_or_log(ctx, &ctx.runtime.repos, &repo_name) {
        Some(r) => r,
        None => {
            end_ui_operation(ctx);
            return;
        }
    };

    let passphrase = match get_or_resolve_passphrase(repo, &mut ctx.passphrases) {
        Ok(p) => p,
        Err(e) => {
            send_log(&ctx.ui_tx, format!("[{repo_name}] passphrase error: {e}"));
            end_ui_operation(ctx);
            return;
        }
    };

    match commands::prune::run(
        &repo.config,
        passphrase.as_deref().map(|s| s.as_str()),
        false,
        false,
        &repo.sources,
        &[],
        Some(&ctx.cancel_requested),
    ) {
        Ok((stats, _)) => {
            send_log(
                &ctx.ui_tx,
                format!(
                    "[{repo_name}] Pruned {} snapshots (kept {}), freed {} chunks ({})",
                    stats.pruned,
                    stats.kept,
                    stats.chunks_deleted,
                    format_bytes(stats.space_freed),
                ),
            );
            let _ = ctx.app_tx.send(AppCommand::RefreshSnapshots {
                repo_selector: repo_name,
            });
            let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
        }
        Err(e) => {
            send_log(&ctx.ui_tx, format!("[{repo_name}] prune failed: {e}"));
        }
    }

    end_ui_operation(ctx);
}

pub(super) fn handle_find_files(ctx: &mut WorkerContext, repo_name: String, name_pattern: String) {
    begin_ui_operation(ctx, "Searching files...");

    let repo = match select_repo_or_log(ctx, &ctx.runtime.repos, &repo_name) {
        Some(r) => r,
        None => {
            end_ui_operation(ctx);
            return;
        }
    };

    let passphrase = match get_or_resolve_passphrase(repo, &mut ctx.passphrases) {
        Ok(p) => p,
        Err(e) => {
            send_log(&ctx.ui_tx, format!("[{repo_name}] passphrase error: {e}"));
            end_ui_operation(ctx);
            return;
        }
    };

    let filter = match FindFilter::build(None, Some(&name_pattern), None, None, None, None, None) {
        Ok(f) => f,
        Err(e) => {
            send_log(&ctx.ui_tx, format!("Invalid name pattern: {e}"));
            end_ui_operation(ctx);
            return;
        }
    };

    let scope = FindScope {
        source_label: None,
        last_n: None,
    };

    match vykar_core::commands::find::run(
        &repo.config,
        passphrase.as_deref().map(|s| s.as_str()),
        &scope,
        &filter,
    ) {
        Ok(timelines) => {
            let mut rows = Vec::new();
            for tl in &timelines {
                for ah in &tl.hits {
                    let ts: DateTime<Local> = ah.hit.snapshot_time.with_timezone(&Local);
                    rows.push(FindResultRow {
                        path: tl.path.clone(),
                        snapshot: ah.hit.snapshot_name.clone(),
                        date: ts.format("%Y-%m-%d %H:%M:%S").to_string(),
                        size: format_bytes(ah.hit.size),
                        status: match ah.status {
                            FileStatus::Added => "Added".to_string(),
                            FileStatus::Modified => "Modified".to_string(),
                            FileStatus::Unchanged => "Unchanged".to_string(),
                        },
                    });
                }
            }
            send_log(
                &ctx.ui_tx,
                format!(
                    "[{repo_name}] Find '{}': {} paths, {} total hits",
                    name_pattern,
                    timelines.len(),
                    rows.len(),
                ),
            );
            let _ = ctx.ui_tx.send(UiEvent::FindResultsData { rows });
        }
        Err(e) => {
            send_log(&ctx.ui_tx, format!("[{repo_name}] find failed: {e}"));
        }
    }

    end_ui_operation(ctx);
}
