use std::collections::BTreeMap;

use chrono::{DateTime, Local, Utc};
use vykar_core::app::service::HookedCommand;
use vykar_core::commands;
use vykar_core::commands::find::{FileStatus, FindFilter, FindScope};

use crate::messages::{AppCommand, DiffResultRow, FindResultRow, FindSnapshotGroup, UiEvent};
use crate::repo_helpers::{PassphraseRun, find_repo_for_snapshot, send_log};
use vykar_common::display::format_bytes;
use vykar_types::error::VykarError;

use super::WorkerContext;
use super::shared::{OpGuard, with_hooked_repo_op, with_open_repo_op, with_repo_op};

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

    match find_repo_for_snapshot(&ctx.runtime.repos, &repo_name, &snapshot, &mut ctx.service) {
        Ok(PassphraseRun::Ran((repo, passphrase))) => {
            let path_set: std::collections::HashSet<String> = paths.into_iter().collect();
            match commands::restore::run_selected(
                &repo.config,
                passphrase.as_deref().map(|s| s.as_str()),
                &snapshot,
                &dest,
                &path_set,
                repo.config.xattrs.enabled,
                false, // verify_chunks
                // Taken after `OpGuard::ui`, which installs a fresh token at
                // operation start (see `with_repo_op`).
                &ctx.cancel_requested.current(),
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
                    let message = if matches!(e, VykarError::RestoreSourceUnreadable(_))
                        && !e.is_removed_during_read()
                    {
                        format!(
                            "{e}. To restore the intact files, run \
                             `vykar restore --continue-on-error` from the command line."
                        )
                    } else {
                        e.to_string()
                    };
                    guard.fail(format!("Restore failed: {message}"));
                    let _ = ctx.ui_tx.send(UiEvent::RestoreFinished {
                        success: false,
                        message,
                    });
                }
            }
        }
        Ok(PassphraseRun::Canceled) => {
            send_log(
                &ctx.ui_tx,
                format!("[{repo_name}] passphrase prompt canceled; restore skipped."),
            );
            let _ = ctx.ui_tx.send(UiEvent::RestoreFinished {
                success: false,
                message: "Passphrase required".to_string(),
            });
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

    // Single batch call: validates all names up front and runs in one
    // session (see `commands::delete::run_on_repo`). Avoids per-row partial
    // failures and a session registration per row. A pre-mutation
    // validation failure (e.g. SnapshotNotFound) leaves the repo untouched, so
    // one error covers the whole batch.
    let names: Vec<&str> = snapshot_names.iter().map(String::as_str).collect();
    let result = match with_open_repo_op(ctx, &repo_name, status, "delete", |_, r, cancel| {
        commands::delete::run_on_repo(r, &names, false, cancel)
    }) {
        Ok(r) => r,
        Err(_) => return,
    };

    if let [s] = result.stats.as_slice() {
        send_log(
            &ctx.ui_tx,
            format!(
                "[{repo_name}] Deleted snapshot '{}'; compact reclaims its space",
                s.snapshot_name,
            ),
        );
    } else {
        send_log(
            &ctx.ui_tx,
            format!(
                "[{repo_name}] Deleted {} snapshots; compact reclaims their space",
                result.stats.len(),
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
    // Diff is the one conventional handler that must also push a result event
    // on failure, so it consumes `with_repo_op`'s reason string.
    let result = match with_repo_op(
        ctx,
        &repo_name,
        "Diffing snapshots...",
        "diff",
        |repo, pass, _| commands::diff::run(&repo.config, pass, &snapshot_a, &snapshot_b),
    ) {
        Ok(r) => r,
        Err(reason) => {
            send_diff_error(ctx, repo_name, snapshot_a, snapshot_b, reason);
            return;
        }
    };

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

pub(super) fn handle_prune_repo(ctx: &mut WorkerContext, repo_name: String) {
    let Ok((stats, _)) = with_hooked_repo_op(
        ctx,
        &repo_name,
        "Pruning snapshots...",
        HookedCommand::Prune,
        |repo, r, cancel| {
            commands::prune::run_on_repo(
                r,
                &repo.config,
                &commands::prune::PruneRequest {
                    sources: &repo.sources,
                    ..Default::default()
                },
                cancel,
            )
        },
    ) else {
        return;
    };

    send_log(
        &ctx.ui_tx,
        format!(
            "[{repo_name}] Pruned {} snapshots (kept {}); compact reclaims their space",
            stats.pruned, stats.kept,
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
    // Built before the operation starts: a bad pattern is a user input error,
    // not a repository failure, so it never opens a repo.
    let filter = match FindFilter::build(None, None, Some(&name_pattern), None, None, None, None) {
        Ok(f) => f,
        Err(e) => {
            // No OpGuard is in scope yet, so surface the error in the status
            // bar explicitly — the log line alone is easy to miss.
            let msg = format!("Invalid name pattern: {e}");
            send_log(&ctx.ui_tx, msg.clone());
            let _ = ctx.ui_tx.send(UiEvent::ErrorStatus(msg));
            return;
        }
    };
    let scope = FindScope {
        source_label: None,
        last_n: None,
    };

    let Ok(timelines) = with_repo_op(
        ctx,
        &repo_name,
        "Searching files...",
        "find",
        |repo, pass, _| commands::find::run(&repo.config, pass, &scope, &filter),
    ) else {
        return;
    };

    let mut by_snap: BTreeMap<(DateTime<Utc>, String), Vec<FindResultRow>> = BTreeMap::new();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::test_support::{Harness, cmd_append, drain, harness};

    fn yaml_str(s: &str) -> String {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    }

    /// A worker over an initialized repo `nas` (AES-256-GCM, the test
    /// passphrase in its config) with one source and the given global hooks.
    fn prune_harness(hooks: &[(&str, String)]) -> Harness {
        let h = harness(|tmp| {
            let source = tmp.join("source");
            std::fs::create_dir_all(&source).unwrap();
            std::fs::write(source.join("a.txt"), b"a").unwrap();
            let hooks = hooks
                .iter()
                .map(|(k, v)| format!("  {k}: {}\n", yaml_str(v)))
                .collect::<Vec<_>>()
                .concat();
            format!(
                "repositories:\n  - url: {}\n    label: nas\n\
                 encryption:\n  mode: aes256gcm\n  passphrase: {}\n\
                 cache_dir: {}\n\
                 retention:\n  keep_last: 1\n\
                 hooks:\n{hooks}\
                 sources:\n  - path: {}\n    label: src\n",
                yaml_str(&tmp.join("repo").display().to_string()),
                yaml_str(vykar_core::testing::TEST_PASSPHRASE),
                yaml_str(&tmp.join("cache").display().to_string()),
                yaml_str(&source.display().to_string()),
            )
        });
        vykar_core::testing::with_cheap_kdf(|| {
            commands::init::run(
                &h.ctx.runtime.repos.first().unwrap().config,
                Some(vykar_core::testing::TEST_PASSPHRASE),
            )
        })
        .unwrap();
        h
    }

    #[test]
    fn prune_runs_prune_hooks() {
        let log = tempfile::tempdir().unwrap();
        let log = log.path().join("hooks.log");
        let mut h = prune_harness(&[
            ("before_prune", cmd_append(&log, "before_prune")),
            ("after_prune", cmd_append(&log, "after_prune")),
            ("finally_prune", cmd_append(&log, "finally_prune")),
        ]);

        handle_prune_repo(&mut h.ctx, "nas".into());
        let (logs, errors) = drain(&h.ui_rx);

        let lines: Vec<String> = std::fs::read_to_string(&log)
            .unwrap()
            .lines()
            .map(|l| l.trim().to_string())
            .collect();
        assert_eq!(lines, ["before_prune", "after_prune", "finally_prune"]);
        assert!(errors.is_empty(), "{errors:?}");
        assert!(logs.iter().any(|l| l.contains("Pruned")), "{logs:?}");
        assert!(h.tmp.path().join("cache").exists());
    }

    #[test]
    fn prune_hook_skip_is_logged_not_failed() {
        let mut h = prune_harness(&[("before_prune", "exit 75".to_string())]);

        handle_prune_repo(&mut h.ctx, "nas".into());
        let (logs, errors) = drain(&h.ui_rx);

        assert!(errors.is_empty(), "{errors:?}");
        assert!(
            logs.iter()
                .any(|l| l.contains("prune skipped") && l.contains("requested skip")),
            "{logs:?}"
        );
        assert!(!logs.iter().any(|l| l.contains("Pruned")), "{logs:?}");
    }

    #[test]
    fn prune_after_hook_failure_is_a_logged_warning() {
        let mut h = prune_harness(&[("after_prune", "exit 3".to_string())]);

        handle_prune_repo(&mut h.ctx, "nas".into());
        let (logs, errors) = drain(&h.ui_rx);

        assert!(errors.is_empty(), "{errors:?}");
        assert!(logs.iter().any(|l| l.contains("hook warning")), "{logs:?}");
    }

    #[test]
    fn prune_before_hook_failure_fails_once() {
        let mut h = prune_harness(&[("before_prune", "exit 3".to_string())]);

        handle_prune_repo(&mut h.ctx, "nas".into());
        let (_, errors) = drain(&h.ui_rx);

        assert!(
            matches!(errors.as_slice(), [e] if e.contains("prune failed")),
            "{errors:?}"
        );
    }
}
