use chrono::{DateTime, Local, Utc};
use vykar_core::app::operations;
use vykar_core::commands::init;
use vykar_types::error::VykarError;

use crate::controllers;
use crate::messages::{RepoInfoData, SnapshotRowData, UiEvent};
use crate::repo_helpers::{
    find_repo_for_snapshot, format_repo_name, get_or_resolve_passphrase, select_repos, send_log,
};
use crate::APP_TITLE;
use vykar_common::display::{format_bytes, format_count};

use super::WorkerContext;

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

pub(super) fn handle_fetch_all_repo_info(ctx: &mut WorkerContext) {
    let _ = ctx
        .ui_tx
        .send(UiEvent::Status("Fetching repository info...".to_string()));

    let mut items = Vec::new();
    let mut labels = Vec::new();

    let total = ctx.runtime.repos.len();
    for (i, repo) in ctx.runtime.repos.iter().enumerate() {
        let repo_name = format_repo_name(repo);
        let _ = ctx.ui_tx.send(UiEvent::Status(format!(
            "Loading repo info: [{}] ({}/{total})...",
            repo_name,
            i + 1
        )));
        let url = repo.config.repository.url.clone();
        let passphrase = match get_or_resolve_passphrase(repo, &mut ctx.passphrases) {
            Ok(p) => p,
            Err(e) => {
                send_log(&ctx.ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                continue;
            }
        };

        match vykar_core::commands::info::run(
            &repo.config,
            passphrase.as_deref().map(|s| s.as_str()),
        ) {
            Ok(stats) => {
                items.push(RepoInfoData {
                    name: repo_name.clone(),
                    url,
                    snapshots: stats.snapshot_count.to_string(),
                    last_snapshot: format_last_snapshot(stats.last_snapshot_time),
                    size: format_bytes(stats.unique_stored_size),
                });
                labels.push(repo_name);
            }
            Err(e) => {
                if matches!(e, VykarError::RepoNotFound(_)) {
                    let confirmed = tinyfiledialogs::message_box_yes_no(
                        &format!("{APP_TITLE} - Repository Not Initialized"),
                        &format!(
                            "Repository {repo_name} at {url} is not initialized.\n\
                             Would you like to initialize it now?",
                        ),
                        tinyfiledialogs::MessageBoxIcon::Question,
                        tinyfiledialogs::YesNo::Yes,
                    );
                    if confirmed == tinyfiledialogs::YesNo::Yes {
                        // Resolve passphrase for init following the canonical rule:
                        // 1. encryption: none → None
                        // 2. Configured source (passphrase field / passcommand)
                        //    → reuse already-resolved value (no re-execution)
                        // 3. Interactive GUI prompt with enter + confirm
                        //
                        // We only reuse the outer `passphrase` when it provably
                        // came from a configured source. If it came from a single
                        // interactive password_box (no confirmation), we must NOT
                        // reuse it — init needs enter+confirm to avoid typos.
                        // Note: VYKAR_PASSPHRASE env var is not checked here
                        // because take_env_passphrase() removes it on first read,
                        // making the probe unreliable in a GUI context.
                        let has_configured_source = repo.config.encryption.passphrase.is_some()
                            || repo.config.encryption.passcommand.is_some();
                        let init_pass: Option<zeroize::Zeroizing<String>> = if repo
                            .config
                            .encryption
                            .mode
                            == vykar_core::config::EncryptionModeConfig::None
                        {
                            None
                        } else if has_configured_source && passphrase.is_some() {
                            passphrase.clone()
                        } else {
                            let title = format!("{APP_TITLE} - New Passphrase ({repo_name})");
                            let p1 = controllers::password_dialog::show_password_dialog(
                                &title,
                                "Enter new passphrase:",
                            );
                            match p1.filter(|v| !v.is_empty()) {
                                None => {
                                    send_log(
                                        &ctx.ui_tx,
                                        format!("[{repo_name}] Init cancelled (no passphrase)."),
                                    );
                                    continue;
                                }
                                Some(p1_val) => {
                                    let p2 = controllers::password_dialog::show_password_dialog(
                                        &format!("{APP_TITLE} - Confirm Passphrase ({repo_name})"),
                                        "Confirm passphrase:",
                                    );
                                    match p2 {
                                        Some(ref p2_val) if p2_val == &p1_val => {
                                            Some(zeroize::Zeroizing::new(p1_val))
                                        }
                                        _ => {
                                            send_log(
                                                &ctx.ui_tx,
                                                format!("[{repo_name}] Passphrases do not match."),
                                            );
                                            continue;
                                        }
                                    }
                                }
                            }
                        };

                        let retry_pass = init_pass.clone();
                        match init::run(&repo.config, init_pass.as_deref().map(|s| s.as_str())) {
                            Ok(_) => {
                                send_log(
                                    &ctx.ui_tx,
                                    format!("[{repo_name}] Repository initialized."),
                                );
                                if let Some(p) = init_pass {
                                    ctx.passphrases
                                        .insert(repo.config.repository.url.clone(), p);
                                }
                            }
                            Err(VykarError::RepoAlreadyExists(_)) => {
                                send_log(
                                    &ctx.ui_tx,
                                    format!(
                                        "[{repo_name}] Repository was initialized concurrently."
                                    ),
                                );
                            }
                            Err(init_err) => {
                                send_log(
                                    &ctx.ui_tx,
                                    format!("[{repo_name}] init failed: {init_err}"),
                                );
                                continue;
                            }
                        }

                        // Retry info with the init passphrase to populate the repo card
                        if let Ok(stats) = vykar_core::commands::info::run(
                            &repo.config,
                            retry_pass.as_deref().map(|s| s.as_str()),
                        ) {
                            items.push(RepoInfoData {
                                name: repo_name.clone(),
                                url: url.clone(),
                                snapshots: stats.snapshot_count.to_string(),
                                last_snapshot: format_last_snapshot(stats.last_snapshot_time),
                                size: format_bytes(stats.unique_stored_size),
                            });
                            labels.push(repo_name);
                        }
                    } else {
                        send_log(
                            &ctx.ui_tx,
                            format!("[{repo_name}] Repository initialization skipped."),
                        );
                    }
                } else {
                    send_log(&ctx.ui_tx, format!("[{repo_name}] info failed: {e}"));
                }
            }
        }
    }

    let _ = ctx.ui_tx.send(UiEvent::RepoModelData { items, labels });
    let _ = ctx.ui_tx.send(UiEvent::Status("Idle".to_string()));
}

pub(super) fn handle_refresh_snapshots(ctx: &mut WorkerContext, repo_selector: String) {
    let _ = ctx
        .ui_tx
        .send(UiEvent::Status("Loading snapshots...".to_string()));

    let repos_to_scan = match select_repos(&ctx.runtime.repos, &repo_selector) {
        Ok(repos) => repos,
        Err(e) => {
            send_log(&ctx.ui_tx, format!("Failed to select repository: {e}"));
            let _ = ctx.ui_tx.send(UiEvent::Status("Idle".to_string()));
            return;
        }
    };

    let mut data = Vec::new();

    for repo in repos_to_scan {
        let repo_name = format_repo_name(repo);
        let passphrase = match get_or_resolve_passphrase(repo, &mut ctx.passphrases) {
            Ok(pass) => pass,
            Err(e) => {
                send_log(&ctx.ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                continue;
            }
        };

        match operations::list_snapshots_with_stats(
            &repo.config,
            passphrase.as_deref().map(|s| s.as_str()),
        ) {
            Ok(mut snapshots) => {
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
                    let (files, size, nfiles, size_bytes) = match stats {
                        Some(st) => (
                            format_count(st.nfiles),
                            format_bytes(st.deduplicated_size),
                            Some(st.nfiles),
                            Some(st.deduplicated_size),
                        ),
                        None => ("-".to_string(), "-".to_string(), None, None),
                    };
                    data.push(SnapshotRowData {
                        id: s.name.clone(),
                        hostname,
                        time_str: ts.format("%Y-%m-%d %H:%M:%S").to_string(),
                        label,
                        files,
                        size,
                        nfiles,
                        size_bytes,
                        time_epoch: s.time.timestamp(),
                        repo_name: repo_name.clone(),
                    });
                }
            }
            Err(e) => {
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] snapshot listing failed: {e}"),
                );
            }
        }
    }

    let _ = ctx.ui_tx.send(UiEvent::SnapshotTableData { data });
    let _ = ctx.ui_tx.send(UiEvent::Status("Idle".to_string()));
}

pub(super) fn handle_fetch_snapshot_contents(
    ctx: &mut WorkerContext,
    repo_name: String,
    snapshot_name: String,
) {
    let _ = ctx
        .ui_tx
        .send(UiEvent::Status("Loading snapshot contents...".to_string()));

    match find_repo_for_snapshot(
        &ctx.runtime.repos,
        &repo_name,
        &snapshot_name,
        &mut ctx.passphrases,
    ) {
        Ok((repo, passphrase)) => {
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
                    send_log(&ctx.ui_tx, format!("Failed to load snapshot items: {e}"));
                }
            }
        }
        Err(e) => {
            send_log(&ctx.ui_tx, format!("Failed to resolve snapshot: {e}"));
        }
    }

    let _ = ctx.ui_tx.send(UiEvent::Status("Idle".to_string()));
}
