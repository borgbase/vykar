use std::collections::HashMap;
use std::sync::atomic::Ordering;

use chrono::{DateTime, Local, Utc};
use slint::SharedString;
use vykar_core::app::operations;
use vykar_core::commands::info::InfoStats;
use vykar_core::commands::init;
use vykar_core::config::{self, EncryptionModeConfig};
use vykar_types::error::VykarError;

use crate::controllers;
use crate::messages::{RepoInfoData, SnapshotRowData, UiEvent};
use crate::repo_helpers::{
    find_repo_for_snapshot, format_repo_name, select_repos, send_log, with_passphrase_retry,
    PassphraseRun,
};
use crate::APP_TITLE;
use vykar_common::display::{format_bytes, format_count};

use super::shared::{select_repo_or_log, OpGuard};
use super::WorkerContext;

/// Placeholder shown for metric cells of a repo that failed to load.
const METRIC_PLACEHOLDER: &str = "\u{2014}"; // em dash

fn format_last_snapshot(t: Option<DateTime<Utc>>) -> String {
    let Some(t) = t else {
        return "N/A".to_string();
    };
    let secs = (Utc::now() - t).num_seconds();
    if secs < 0 {
        return t.with_timezone(&Local).format("%Y-%m-%d %H:%M").to_string();
    }
    if secs < 60 {
        "just now".to_string()
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86_400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86_400)
    }
}

fn ok_repo_info(repo_name: &str, url: &str, stats: &InfoStats) -> RepoInfoData {
    RepoInfoData {
        name: repo_name.into(),
        url: url.into(),
        snapshots: stats.snapshot_count.to_string().into(),
        last_snapshot: format_last_snapshot(stats.last_snapshot_time).into(),
        size: format_bytes(stats.unique_stored_size).into(),
        has_error: false,
        error: SharedString::default(),
    }
}

fn error_repo_info(repo_name: &str, url: &str, error: &str) -> RepoInfoData {
    RepoInfoData {
        name: repo_name.into(),
        url: url.into(),
        snapshots: METRIC_PLACEHOLDER.into(),
        last_snapshot: METRIC_PLACEHOLDER.into(),
        size: METRIC_PLACEHOLDER.into(),
        has_error: true,
        error: error.into(),
    }
}

/// Probe a single repository and return its card data plus an optional footer
/// failure reason. Never omits a repo: a failure (locked/unreachable/wrong
/// passphrase/declined init) yields a `has_error` card with placeholder metrics
/// so the repo stays in the sidebar. The second tuple element is `Some(reason)`
/// only for failures that should redden the footer — a canceled passphrase
/// prompt or a user-declined init is `None` (log-only), matching
/// `handle_refresh_snapshots` semantics.
///
/// Takes the disjoint context fields it needs (rather than `&mut WorkerContext`)
/// so callers can iterate `ctx.runtime.repos` while mutating `ctx.passphrases`.
fn fetch_one_repo_info(
    ui_tx: &crossbeam_channel::Sender<UiEvent>,
    passphrases: &mut HashMap<String, zeroize::Zeroizing<String>>,
    repo: &config::ResolvedRepo,
) -> (RepoInfoData, Option<String>) {
    let repo_name = format_repo_name(repo);
    let url = repo.config.repository.url.clone();

    // Stash the passphrase the probe resolved so an uninitialized-repo init can
    // reuse it (a configured passcommand is then not executed a second time).
    let mut probe_pass: Option<zeroize::Zeroizing<String>> = None;
    let outcome = with_passphrase_retry(repo, passphrases, 3, |pass| {
        probe_pass = pass.map(|s| zeroize::Zeroizing::new(s.to_string()));
        vykar_core::commands::info::run(&repo.config, pass)
    });

    match outcome {
        Ok(PassphraseRun::Ran(stats)) => (ok_repo_info(&repo_name, &url, &stats), None),
        Ok(PassphraseRun::Canceled) => {
            send_log(
                ui_tx,
                format!("[{repo_name}] passphrase prompt canceled; skipping."),
            );
            (
                error_repo_info(&repo_name, &url, "Passphrase required"),
                None,
            )
        }
        Err(VykarError::RepoNotFound(_)) => {
            init_repo_interactive(ui_tx, passphrases, repo, &repo_name, &url, probe_pass)
        }
        Err(VykarError::DecryptionFailed) => {
            send_log(ui_tx, format!("[{repo_name}] incorrect passphrase."));
            (
                error_repo_info(&repo_name, &url, "Incorrect passphrase"),
                Some("incorrect passphrase".to_string()),
            )
        }
        Err(e) => {
            send_log(ui_tx, format!("[{repo_name}] info failed: {e}"));
            (
                error_repo_info(&repo_name, &url, &format!("{e}")),
                Some(format!("{e}")),
            )
        }
    }
}

/// Offer to initialize an uninitialized repo, then re-probe it. Returns the
/// resulting card (success, declined, or failed) plus an optional footer failure
/// reason — `None` when the user declines/cancels, `Some` for real failures
/// (passphrase mismatch, init error, post-init probe error).
fn init_repo_interactive(
    ui_tx: &crossbeam_channel::Sender<UiEvent>,
    passphrases: &mut HashMap<String, zeroize::Zeroizing<String>>,
    repo: &config::ResolvedRepo,
    repo_name: &str,
    url: &str,
    probe_pass: Option<zeroize::Zeroizing<String>>,
) -> (RepoInfoData, Option<String>) {
    let confirmed = tinyfiledialogs::message_box_yes_no(
        &format!("{APP_TITLE} - Repository Not Initialized"),
        &format!(
            "Repository {repo_name} at {url} is not initialized.\n\
             Would you like to initialize it now?",
        ),
        tinyfiledialogs::MessageBoxIcon::Question,
        tinyfiledialogs::YesNo::Yes,
    );
    if confirmed == tinyfiledialogs::YesNo::No {
        send_log(
            ui_tx,
            format!("[{repo_name}] Repository initialization skipped."),
        );
        return (error_repo_info(repo_name, url, "Not initialized"), None);
    }

    // Resolve the init passphrase following the canonical rule:
    //   1. encryption: none → None
    //   2. Configured source (passphrase / passcommand) → reuse it
    //   3. Interactive prompt with enter + confirm (init needs confirmation to
    //      avoid locking the repo behind a typo).
    let has_configured_source =
        repo.config.encryption.passphrase.is_some() || repo.config.encryption.passcommand.is_some();
    let init_pass: Option<zeroize::Zeroizing<String>> = if repo.config.encryption.mode
        == EncryptionModeConfig::None
    {
        None
    } else if has_configured_source {
        // Reuse the passphrase the probe already resolved from the configured
        // source (passphrase / passcommand) rather than resolving it again — the
        // comment above and `fetch_one_repo_info` guarantee it is populated for
        // an encrypted, configured repo.
        probe_pass
    } else {
        let p1 = controllers::password_dialog::show_password_dialog(
            &format!("{APP_TITLE} - New Passphrase ({repo_name})"),
            "Enter new passphrase:",
        );
        match p1.filter(|v| !v.is_empty()) {
            None => {
                send_log(
                    ui_tx,
                    format!("[{repo_name}] Init cancelled (no passphrase)."),
                );
                return (error_repo_info(repo_name, url, "Not initialized"), None);
            }
            Some(p1_val) => {
                let p2 = controllers::password_dialog::show_password_dialog(
                    &format!("{APP_TITLE} - Confirm Passphrase ({repo_name})"),
                    "Confirm passphrase:",
                );
                match p2 {
                    Some(ref p2_val) if p2_val == &p1_val => Some(zeroize::Zeroizing::new(p1_val)),
                    _ => {
                        send_log(ui_tx, format!("[{repo_name}] Passphrases do not match."));
                        return (
                            error_repo_info(repo_name, url, "Passphrases did not match"),
                            Some("passphrases did not match".to_string()),
                        );
                    }
                }
            }
        }
    };

    let retry_pass = init_pass.clone();
    match init::run(&repo.config, init_pass.as_deref().map(|s| s.as_str())) {
        Ok(_) => {
            send_log(ui_tx, format!("[{repo_name}] Repository initialized."));
            if let Some(p) = init_pass {
                passphrases.insert(url.to_string(), p);
            }
        }
        Err(VykarError::RepoAlreadyExists(_)) => {
            send_log(
                ui_tx,
                format!("[{repo_name}] Repository was initialized concurrently."),
            );
        }
        Err(init_err) => {
            send_log(ui_tx, format!("[{repo_name}] init failed: {init_err}"));
            return (
                error_repo_info(repo_name, url, &format!("Init failed: {init_err}")),
                Some(format!("init failed: {init_err}")),
            );
        }
    }

    // Re-probe with the init passphrase to populate the card.
    match vykar_core::commands::info::run(&repo.config, retry_pass.as_deref().map(|s| s.as_str())) {
        Ok(stats) => (ok_repo_info(repo_name, url, &stats), None),
        Err(e) => {
            send_log(ui_tx, format!("[{repo_name}] info failed after init: {e}"));
            (
                error_repo_info(repo_name, url, &format!("{e}")),
                Some(format!("info failed after init: {e}")),
            )
        }
    }
}

/// Re-emit a full `RepoModelData` from the cached per-repo cards, in configured
/// order. Repos not yet probed are simply omitted (the sidebar keeps its
/// "Loading..." row until their first result arrives).
fn emit_repo_model(ctx: &WorkerContext) {
    let mut items = Vec::new();
    let mut labels = Vec::new();
    for repo in &ctx.runtime.repos {
        if let Some(data) = ctx.repo_info.get(&repo.config.repository.url) {
            labels.push(data.name.clone());
            items.push(data.clone());
        }
    }
    let _ = ctx.ui_tx.send(UiEvent::RepoModelData { items, labels });
}

pub(super) fn handle_fetch_all_repo_info(ctx: &mut WorkerContext) {
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        "Fetching repository info...",
    );

    // Pre-seed a placeholder card for every configured repo not already cached,
    // so `emit_repo_model` emits all repos even if the probe loop is cancelled
    // partway through — cancelled rows keep a Retry button instead of vanishing.
    for repo in &ctx.runtime.repos {
        let url = repo.config.repository.url.clone();
        ctx.repo_info
            .entry(url.clone())
            .or_insert_with(|| error_repo_info(&format_repo_name(repo), &url, "Not loaded"));
    }

    let total = ctx.runtime.repos.len();
    // Aggregate per-repo failures into a single `guard.fail` after the loop, so a
    // partial failure yields a persistent red footer rather than silently
    // returning to "Idle". Mirrors `handle_refresh_snapshots`.
    let mut failures: Vec<(String, String)> = Vec::new();
    for (i, repo) in ctx.runtime.repos.iter().enumerate() {
        if ctx.cancel_requested.load(Ordering::SeqCst) {
            send_log(&ctx.ui_tx, "Repository info fetch cancelled.");
            break;
        }
        let repo_name = format_repo_name(repo);
        let _ = ctx.ui_tx.send(UiEvent::Status(format!(
            "Loading repo info: [{}] ({}/{total})...",
            repo_name,
            i + 1
        )));
        let (data, failure) = fetch_one_repo_info(&ctx.ui_tx, &mut ctx.passphrases, repo);
        if let Some(reason) = failure {
            failures.push((repo_name, reason));
        }
        ctx.repo_info
            .insert(repo.config.repository.url.clone(), data);
    }

    if !failures.is_empty() {
        let detail = failures
            .iter()
            .map(|(name, reason)| format!("[{name}] {reason}"))
            .collect::<Vec<_>>()
            .join("; ");
        guard.fail(format!(
            "{} of {total} repositories failed to load: {detail}",
            failures.len()
        ));
    }

    emit_repo_model(ctx);
}

pub(super) fn handle_fetch_repo_info(ctx: &mut WorkerContext, repo_name: String) {
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        format!("Refreshing [{repo_name}]..."),
    );

    let repo = match select_repo_or_log(ctx, &ctx.runtime.repos, &repo_name) {
        Some(r) => r,
        None => return,
    };
    let url = repo.config.repository.url.clone();
    let (data, failure) = fetch_one_repo_info(&ctx.ui_tx, &mut ctx.passphrases, repo);
    if let Some(reason) = failure {
        guard.fail(format!("[{repo_name}] {reason}"));
    }
    ctx.repo_info.insert(url, data);
    emit_repo_model(ctx);
}

pub(super) fn handle_refresh_snapshots(ctx: &mut WorkerContext, repo_selector: String) {
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        "Loading snapshots...",
    );

    let repos_to_scan = match select_repos(&ctx.runtime.repos, &repo_selector) {
        Ok(repos) => repos,
        Err(e) => {
            guard.fail(format!("Failed to select repository: {e}"));
            return;
        }
    };

    let mut data = Vec::new();
    // Aggregate per-repo failures into a single `guard.fail` after the loop so a
    // partial failure yields "N of M refreshed; [X] failed: …" rather than a
    // misleading all-red status when only some repos failed.
    let total = repos_to_scan.len();
    let mut failures: Vec<(String, String)> = Vec::new();

    for repo in repos_to_scan {
        if ctx.cancel_requested.load(Ordering::SeqCst) {
            break;
        }
        let repo_name = format_repo_name(repo);
        let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
            operations::list_snapshots_with_stats(&repo.config, pass)
        });

        let mut snapshots = match outcome {
            Ok(PassphraseRun::Ran(snapshots)) => snapshots,
            Ok(PassphraseRun::Canceled) => {
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] passphrase prompt canceled; skipping."),
                );
                continue;
            }
            Err(e) => {
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] snapshot listing failed: {e}"),
                );
                failures.push((repo_name.clone(), format!("{e}")));
                continue;
            }
        };

        snapshots.sort_by_key(|(s, _)| s.time);
        for (s, stats) in snapshots {
            let ts: DateTime<Local> = s.time.with_timezone(&Local);
            let label = if s.source_label.is_empty() {
                "-".to_string()
            } else {
                s.source_label.clone()
            };
            let hostname = if s.hostname.is_empty() {
                "-".to_string()
            } else {
                s.hostname.clone()
            };
            let (files, size, added, nfiles, size_bytes, added_bytes) = match stats {
                Some(st) => (
                    format_count(st.nfiles),
                    format_bytes(st.original_size),
                    format_bytes(st.deduplicated_size),
                    Some(st.nfiles),
                    Some(st.original_size),
                    Some(st.deduplicated_size),
                ),
                None => (
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    None,
                    None,
                    None,
                ),
            };
            data.push(SnapshotRowData {
                id: s.name.clone().into(),
                hostname: hostname.into(),
                time_str: ts.format("%Y-%m-%d %H:%M").to_string().into(),
                label: label.into(),
                files: files.into(),
                size: size.into(),
                added: added.into(),
                nfiles,
                size_bytes,
                added_bytes,
                time_epoch: s.time.timestamp(),
                repo_name: repo_name.clone().into(),
            });
        }
    }

    if !failures.is_empty() {
        let detail = failures
            .iter()
            .map(|(name, reason)| format!("[{name}] {reason}"))
            .collect::<Vec<_>>()
            .join("; ");
        let refreshed = total.saturating_sub(failures.len());
        guard.fail(format!(
            "{refreshed} of {total} repositories refreshed; {} failed: {detail}",
            failures.len()
        ));
    }

    let _ = ctx.ui_tx.send(UiEvent::SnapshotTableData { data });
}

pub(super) fn handle_fetch_snapshot_contents(
    ctx: &mut WorkerContext,
    repo_name: String,
    snapshot_name: String,
) {
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        "Loading snapshot contents...",
    );

    match find_repo_for_snapshot(
        &ctx.runtime.repos,
        &repo_name,
        &snapshot_name,
        &mut ctx.passphrases,
    ) {
        Ok(PassphraseRun::Ran((repo, passphrase))) => {
            match operations::list_snapshot_items_with_source_paths(
                &repo.config,
                passphrase.as_deref().map(|s| s.as_str()),
                &snapshot_name,
            ) {
                Ok((items, source_paths)) => {
                    send_log(
                        &ctx.ui_tx,
                        format!(
                            "Loaded {} item(s) from snapshot {} in [{}]",
                            items.len(),
                            snapshot_name,
                            format_repo_name(repo)
                        ),
                    );

                    let _ = ctx.ui_tx.send(UiEvent::SnapshotContentsData {
                        repo_name: repo_name.clone(),
                        snapshot_name: snapshot_name.clone(),
                        items,
                        source_paths,
                    });
                }
                Err(e) => {
                    guard.fail(format!("Failed to load snapshot items: {e}"));
                }
            }
        }
        Ok(PassphraseRun::Canceled) => {
            send_log(
                &ctx.ui_tx,
                format!("[{repo_name}] passphrase prompt canceled; skipping."),
            );
        }
        Err(e) => {
            guard.fail(format!("Failed to resolve snapshot: {e}"));
        }
    }
}
