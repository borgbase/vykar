use std::collections::BTreeMap;

use chrono::{DateTime, Local, Utc};
use vykar_core::app::operations;
use vykar_core::commands;
use vykar_core::commands::find::{FileStatus, FindFilter, FindScope};

use crate::messages::{AppCommand, DiffResultRow, FindResultRow, FindSnapshotGroup, UiEvent};
use crate::repo_helpers::{find_repo_for_snapshot, send_log, with_passphrase_retry, PassphraseRun};
use vykar_common::display::format_bytes;

use super::shared::{select_repo_or_log, OpGuard};
use super::WorkerContext;

pub(super) fn handle_restore_selected(
    ctx: &mut WorkerContext,
    repo_name: String,
    snapshot: String,
    dest: String,
    paths: Vec<String>,
) {
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        "Restoring selected items...",
    );

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
                    guard.fail(format!("Restore failed: {e}"));
                    let _ = ctx.ui_tx.send(UiEvent::RestoreFinished {
                        success: false,
                        message: format!("{e}"),
                    });
                }
            }
        }
        Err(e) => {
            guard.fail(format!("Failed to resolve snapshot: {e}"));
            let _ = ctx.ui_tx.send(UiEvent::RestoreFinished {
                success: false,
                message: format!("{e}"),
            });
        }
    }
}

pub(super) fn handle_delete_snapshots(
    ctx: &mut WorkerContext,
    repo_name: String,
    snapshot_names: Vec<String>,
) {
    if snapshot_names.is_empty() {
        return;
    }

    let prompt = if let [only] = snapshot_names.as_slice() {
        format!("Are you sure you want to delete snapshot {only} from {repo_name}?")
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
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        status,
    );

    let repo = match select_repo_or_log(ctx, &ctx.runtime.repos, &repo_name) {
        Some(r) => r,
        None => {
            guard.fail(format!("No repository matching '{repo_name}'."));
            return;
        }
    };

    // Single batch call: validates all names up front and runs under one
    // maintenance lock (see `commands::delete::run`). Avoids per-row partial
    // failures and per-row maintenance-lock contention.
    let names: Vec<&str> = snapshot_names.iter().map(String::as_str).collect();
    let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
        commands::delete::run(
            &repo.config,
            pass,
            &names,
            false,
            Some(&ctx.cancel_requested),
        )
    });

    match outcome {
        Ok(PassphraseRun::Ran(result)) => {
            let mut total_chunks = 0u64;
            let mut total_freed = 0u64;
            for stats in &result.stats {
                total_chunks += stats.chunks_deleted;
                total_freed += stats.space_freed;
            }
            if let [s] = result.stats.as_slice() {
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
        Ok(PassphraseRun::Canceled) => {
            send_log(
                &ctx.ui_tx,
                format!("[{repo_name}] passphrase prompt canceled; skipping."),
            );
        }
        Err(e) => {
            // Pre-mutation validation failure (e.g. SnapshotNotFound) leaves
            // the repo untouched — single error covers the whole batch.
            guard.fail(format!("[{repo_name}] delete failed: {e}"));
        }
    }
}

fn send_diff_error(
    ctx: &WorkerContext,
    repo_name: String,
    snapshot_a: String,
    snapshot_b: String,
    message: String,
) {
    let _ = ctx.ui_tx.send(UiEvent::DiffResultsData {
        repo_name,
        snapshot_a,
        snapshot_b,
        base_snapshot: String::new(),
        target_snapshot: String::new(),
        rows: Vec::new(),
        error: Some(message),
    });
}

pub(super) fn handle_diff_snapshots(
    ctx: &mut WorkerContext,
    repo_name: String,
    snapshot_a: String,
    snapshot_b: String,
) {
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        "Diffing snapshots...",
    );

    let repo = match select_repo_or_log(ctx, &ctx.runtime.repos, &repo_name) {
        Some(r) => r,
        None => {
            guard.fail(format!("[{repo_name}] repository not found"));
            send_diff_error(
                ctx,
                repo_name,
                snapshot_a,
                snapshot_b,
                "repository not found".to_string(),
            );
            return;
        }
    };

    let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
        operations::diff_snapshots(&repo.config, pass, &snapshot_a, &snapshot_b)
    });

    match outcome {
        Ok(PassphraseRun::Ran(result)) => {
            let rows: Vec<DiffResultRow> = result
                .entries
                .iter()
                .map(|entry| DiffResultRow {
                    change: entry.change,
                    path: entry.path.clone(),
                    old_size_bytes: entry.old_size,
                    new_size_bytes: entry.new_size,
                    delta_bytes: entry.size_delta,
                })
                .collect();
            send_log(
                &ctx.ui_tx,
                format!(
                    "[{repo_name}] Diff {} -> {}: {} file changes",
                    result.base_snapshot,
                    result.target_snapshot,
                    rows.len(),
                ),
            );
            let _ = ctx.ui_tx.send(UiEvent::DiffResultsData {
                repo_name,
                snapshot_a,
                snapshot_b,
                base_snapshot: result.base_snapshot,
                target_snapshot: result.target_snapshot,
                rows,
                error: None,
            });
        }
        Ok(PassphraseRun::Canceled) => {
            send_log(
                &ctx.ui_tx,
                format!("[{repo_name}] passphrase prompt canceled; skipping."),
            );
            send_diff_error(
                ctx,
                repo_name,
                snapshot_a,
                snapshot_b,
                "passphrase required".to_string(),
            );
        }
        Err(e) => {
            guard.fail(format!("[{repo_name}] diff failed: {e}"));
            send_diff_error(ctx, repo_name, snapshot_a, snapshot_b, e.to_string());
        }
    }
}

pub(super) fn handle_prune_repo(ctx: &mut WorkerContext, repo_name: String) {
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        "Pruning snapshots...",
    );

    let repo = match select_repo_or_log(ctx, &ctx.runtime.repos, &repo_name) {
        Some(r) => r,
        None => {
            guard.fail(format!("No repository matching '{repo_name}'."));
            return;
        }
    };

    let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
        commands::prune::run(
            &repo.config,
            pass,
            false,
            false,
            &repo.sources,
            &[],
            Some(&ctx.cancel_requested),
        )
    });

    match outcome {
        Ok(PassphraseRun::Ran((stats, _))) => {
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
        Ok(PassphraseRun::Canceled) => {
            send_log(
                &ctx.ui_tx,
                format!("[{repo_name}] passphrase prompt canceled; skipping."),
            );
        }
        Err(e) => {
            guard.fail(format!("[{repo_name}] prune failed: {e}"));
        }
    }
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
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        "Searching files...",
    );

    let repo = match select_repo_or_log(ctx, &ctx.runtime.repos, &repo_name) {
        Some(r) => r,
        None => {
            guard.fail(format!("No repository matching '{repo_name}'."));
            return;
        }
    };

    let filter = match FindFilter::build(None, None, Some(&name_pattern), None, None, None, None) {
        Ok(f) => f,
        Err(e) => {
            guard.fail(format!("Invalid name pattern: {e}"));
            return;
        }
    };

    let scope = FindScope {
        source_label: None,
        last_n: None,
    };

    let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
        vykar_core::commands::find::run(&repo.config, pass, &scope, &filter)
    });

    match outcome {
        Ok(PassphraseRun::Ran(timelines)) => {
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
        Ok(PassphraseRun::Canceled) => {
            send_log(
                &ctx.ui_tx,
                format!("[{repo_name}] passphrase prompt canceled; skipping."),
            );
        }
        Err(e) => {
            guard.fail(format!("[{repo_name}] find failed: {e}"));
        }
    }
}
