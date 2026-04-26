use std::collections::BTreeMap;

use chrono::{DateTime, Local, Utc};
use vykar_core::app::operations;
use vykar_core::commands;
use vykar_core::commands::delete::DeleteResult;
use vykar_core::commands::find::{FileStatus, FindFilter, FindScope};

use crate::messages::{AppCommand, FindResultRow, FindSnapshotGroup, UiEvent};
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
                    for w in &stats.warnings {
                        send_log(&ctx.ui_tx, format!("warning: {w}"));
                    }
                    if stats.warnings_suppressed > 0 {
                        send_log(
                            &ctx.ui_tx,
                            format!(
                                "warning: {} additional metadata warnings suppressed",
                                stats.warnings_suppressed
                            ),
                        );
                    }
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

pub(super) fn handle_delete_snapshots(
    ctx: &mut WorkerContext,
    repo_name: String,
    snapshot_names: Vec<String>,
) {
    if snapshot_names.is_empty() {
        return;
    }

    let prompt = if snapshot_names.len() == 1 {
        format!(
            "Are you sure you want to delete snapshot {} from {repo_name}?",
            snapshot_names[0]
        )
    } else {
        format!(
            "Are you sure you want to delete {} snapshots from {repo_name}?\n\nThis is a batch \
             operation: either all selected snapshots are deleted, or none.",
            snapshot_names.len()
        )
    };
    let confirmed = tinyfiledialogs::message_box_yes_no(
        "Delete Snapshots",
        &prompt,
        tinyfiledialogs::MessageBoxIcon::Question,
        tinyfiledialogs::YesNo::No,
    );

    if confirmed == tinyfiledialogs::YesNo::No {
        send_log(&ctx.ui_tx, "Snapshot deletion cancelled.");
        return;
    }

    let status = if snapshot_names.len() == 1 {
        "Deleting snapshot..."
    } else {
        "Deleting snapshots..."
    };
    begin_ui_operation(ctx, status);

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

    // Single batch call: validates all names up front and runs under one
    // maintenance lock (see `commands::delete::run`). Avoids per-row partial
    // failures and per-row maintenance-lock contention.
    let names: Vec<&str> = snapshot_names.iter().map(String::as_str).collect();
    let result: vykar_types::error::Result<DeleteResult> = commands::delete::run(
        &repo.config,
        passphrase.as_deref().map(|s| s.as_str()),
        &names,
        false,
        Some(&ctx.cancel_requested),
    );

    match result {
        Ok(result) => {
            let mut total_chunks = 0u64;
            let mut total_freed = 0u64;
            for stats in &result.stats {
                total_chunks += stats.chunks_deleted;
                total_freed += stats.space_freed;
            }
            if result.stats.len() == 1 {
                let s = &result.stats[0];
                send_log(
                    &ctx.ui_tx,
                    format!(
                        "[{repo_name}] Deleted snapshot '{}': {} chunks freed, {} reclaimed",
                        s.snapshot_name,
                        s.chunks_deleted,
                        format_bytes(s.space_freed),
                    ),
                );
            } else {
                send_log(
                    &ctx.ui_tx,
                    format!(
                        "[{repo_name}] Deleted {} snapshots: {} chunks freed, {} reclaimed",
                        result.stats.len(),
                        total_chunks,
                        format_bytes(total_freed),
                    ),
                );
            }
            // Surface any snapshot whose Phase 3 cleanup failed: it is missing
            // from `stats` but did delete from storage, so users still see it
            // disappear from the table.
            let reported: std::collections::HashSet<&str> = result
                .stats
                .iter()
                .map(|s| s.snapshot_name.as_str())
                .collect();
            for name in &snapshot_names {
                if !reported.contains(name.as_str()) {
                    send_log(
                        &ctx.ui_tx,
                        format!(
                            "[{repo_name}] Deleted snapshot '{name}' \
                             (post-commit cleanup stats unavailable; see warnings)"
                        ),
                    );
                }
            }
            for w in &result.warnings {
                send_log(&ctx.ui_tx, format!("[{repo_name}] warning: {w}"));
            }
            let _ = ctx.app_tx.send(AppCommand::RefreshSnapshots {
                repo_selector: repo_name,
            });
            let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
        }
        Err(e) => {
            // Pre-mutation validation failure (e.g. SnapshotNotFound) leaves
            // the repo untouched — single error covers the whole batch.
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
            for w in &stats.warnings {
                send_log(&ctx.ui_tx, format!("[{repo_name}] warning: {w}"));
            }
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

fn format_mtime_nanos(mtime_nanos: i64) -> String {
    let secs = mtime_nanos.div_euclid(1_000_000_000);
    let nsecs = mtime_nanos.rem_euclid(1_000_000_000) as u32;
    match DateTime::<Utc>::from_timestamp(secs, nsecs) {
        Some(dt) => dt
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string(),
        None => String::new(),
    }
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
            let mut by_snap: BTreeMap<(DateTime<Utc>, String), Vec<FindResultRow>> =
                BTreeMap::new();
            let mut total_hits: usize = 0;
            for tl in &timelines {
                for ah in &tl.hits {
                    by_snap
                        .entry((ah.hit.snapshot_time, ah.hit.snapshot_name.clone()))
                        .or_default()
                        .push(FindResultRow {
                            path: tl.path.clone(),
                            mtime: format_mtime_nanos(ah.hit.mtime),
                            size: format_bytes(ah.hit.size),
                            status: match ah.status {
                                FileStatus::Added => "Added".to_string(),
                                FileStatus::Modified => "Modified".to_string(),
                                FileStatus::Unchanged => "Unchanged".to_string(),
                            },
                        });
                    total_hits += 1;
                }
            }
            // Newest snapshot first.
            let groups: Vec<FindSnapshotGroup> = by_snap
                .into_iter()
                .rev()
                .map(|((ts, id), rows)| {
                    let local: DateTime<Local> = ts.with_timezone(&Local);
                    FindSnapshotGroup {
                        snapshot_id: id,
                        snapshot_time: local.format("%Y-%m-%d %H:%M:%S").to_string(),
                        rows,
                    }
                })
                .collect();
            send_log(
                &ctx.ui_tx,
                format!(
                    "[{repo_name}] Find '{}': {} paths, {} total hits, {} snapshots",
                    name_pattern,
                    timelines.len(),
                    total_hits,
                    groups.len(),
                ),
            );
            let _ = ctx.ui_tx.send(UiEvent::FindResultsData { groups });
        }
        Err(e) => {
            send_log(&ctx.ui_tx, format!("[{repo_name}] find failed: {e}"));
        }
    }

    end_ui_operation(ctx);
}
