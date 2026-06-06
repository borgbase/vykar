use std::sync::atomic::Ordering;

use vykar_core::app::operations;
use vykar_core::commands::backup::BackupProgressEvent;
use vykar_core::config;

use crate::messages::{AppCommand, UiEvent};
use crate::progress::{format_check_status, format_step_outcome, BackupStatusTracker};
use crate::repo_helpers::{
    format_repo_name, get_or_resolve_passphrase, log_backup_report, send_log,
};

use super::shared::{begin_backup_operation, end_backup_operation, run_selection_with_progress};
use super::WorkerContext;

pub(super) fn handle_backup_all(ctx: &mut WorkerContext, scheduled: bool) {
    let status = if scheduled {
        "Running scheduled backup cycle..."
    } else {
        "Running backup cycle..."
    };
    begin_backup_operation(ctx, status);

    let mut any_snapshots_created = false;
    let total = ctx.runtime.repos.len();
    for (i, repo) in ctx.runtime.repos.iter().enumerate() {
        if ctx.cancel_requested.load(Ordering::SeqCst) {
            send_log(&ctx.ui_tx, "Backup cancelled by user.");
            break;
        }

        let repo_name = format_repo_name(repo);
        let _ = ctx.ui_tx.send(UiEvent::Status(format!(
            "[{}] ({}/{total})...",
            repo_name,
            i + 1
        )));

        let passphrase = match get_or_resolve_passphrase(repo, &mut ctx.passphrases) {
            Ok(pass) => pass,
            Err(e) => {
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] failed to resolve passphrase: {e}"),
                );
                continue;
            }
        };

        if repo.config.encryption.mode != config::EncryptionModeConfig::None && passphrase.is_none()
        {
            send_log(
                &ctx.ui_tx,
                format!("[{repo_name}] passphrase prompt canceled; skipping this repository"),
            );
            continue;
        }

        let mut tracker = BackupStatusTracker::new(repo_name.clone());
        let ui_tx_progress = ctx.ui_tx.clone();
        let rn = repo_name.clone();
        let result = operations::run_full_cycle_for_repo(
            repo,
            passphrase.as_deref().map(|s| s.as_str()),
            Some(&ctx.cancel_requested),
            false,
            &[],
            &mut |event| match &event {
                operations::CycleEvent::StepStarted(step) => {
                    let _ = ui_tx_progress.send(UiEvent::Status(format!(
                        "[{rn}] {}...",
                        step.command_name()
                    )));
                }
                operations::CycleEvent::Backup(BackupProgressEvent::Warning { message }) => {
                    send_log(&ui_tx_progress, format!("[{rn}] warning: {message}"));
                }
                operations::CycleEvent::Backup(evt) => {
                    if let Some(status) = tracker.format(evt) {
                        let _ = ui_tx_progress.send(UiEvent::Status(status));
                    }
                }
                operations::CycleEvent::Check(evt) => {
                    let _ = ui_tx_progress.send(UiEvent::Status(format_check_status(&rn, evt)));
                }
                operations::CycleEvent::HookWarning { warning, .. } => {
                    send_log(&ui_tx_progress, format!("[{rn}] hook warning: {warning}"));
                }
                operations::CycleEvent::StepWarning { step, message } => {
                    send_log(
                        &ui_tx_progress,
                        format!("[{rn}] {} warning: {message}", step.command_name()),
                    );
                }
                _ => {}
            },
        );

        if let Some(ref report) = result.backup_report {
            if !report.created.is_empty() {
                any_snapshots_created = true;
            }
            log_backup_report(&ctx.ui_tx, &repo_name, report);
        }
        for (step, outcome) in &result.steps {
            let msg = format_step_outcome(&repo_name, *step, outcome);
            if !msg.is_empty() {
                send_log(&ctx.ui_tx, msg);
            }
        }
    }

    if any_snapshots_created {
        let _ = ctx.ui_tx.send(UiEvent::TriggerSnapshotRefresh);
        let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
    }

    end_backup_operation(ctx);
}

pub(super) fn handle_backup_repo(ctx: &mut WorkerContext, repo_name: String) {
    let repo_name_sel = repo_name.trim().to_string();
    if repo_name_sel.is_empty() {
        send_log(&ctx.ui_tx, "Select a repository first.");
        return;
    }

    let repo = match config::select_repo(&ctx.runtime.repos, &repo_name_sel) {
        Some(r) => r,
        None => {
            send_log(
                &ctx.ui_tx,
                format!("No repository matching '{repo_name_sel}'."),
            );
            return;
        }
    };

    let rn = format_repo_name(repo);
    begin_backup_operation(ctx, format!("Running backup for [{rn}]..."));

    let passphrase = match get_or_resolve_passphrase(repo, &mut ctx.passphrases) {
        Ok(p) => p,
        Err(e) => {
            send_log(&ctx.ui_tx, format!("[{rn}] passphrase error: {e}"));
            end_backup_operation(ctx);
            return;
        }
    };

    if repo.config.encryption.mode != config::EncryptionModeConfig::None && passphrase.is_none() {
        send_log(
            &ctx.ui_tx,
            format!("[{rn}] passphrase prompt canceled; skipping."),
        );
        end_backup_operation(ctx);
        return;
    }

    match run_selection_with_progress(
        ctx,
        repo,
        &repo.sources,
        passphrase.as_deref().map(|s| s.as_str()),
    ) {
        Ok(report) => {
            if !report.created.is_empty() {
                let _ = ctx.app_tx.send(AppCommand::RefreshSnapshots {
                    repo_selector: rn.clone(),
                });
                let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
            }
            log_backup_report(&ctx.ui_tx, &rn, &report);
        }
        Err(e) => send_log(&ctx.ui_tx, format!("[{rn}] backup failed: {e}")),
    }

    end_backup_operation(ctx);
}

pub(super) fn handle_backup_source(ctx: &mut WorkerContext, source_label: String) {
    let source_label = source_label.trim().to_string();
    if source_label.is_empty() {
        send_log(&ctx.ui_tx, "Select a source first.");
        return;
    }

    begin_backup_operation(
        ctx,
        format!("Running backup for source '{source_label}'..."),
    );

    let mut any_backed_up = false;
    let total = ctx.runtime.repos.len();
    for (i, repo) in ctx.runtime.repos.iter().enumerate() {
        if ctx.cancel_requested.load(Ordering::SeqCst) {
            send_log(&ctx.ui_tx, "Backup cancelled by user.");
            break;
        }

        let matching_sources: Vec<config::SourceEntry> = repo
            .sources
            .iter()
            .filter(|s| s.label == source_label)
            .cloned()
            .collect();

        if matching_sources.is_empty() {
            continue;
        }

        let repo_name = format_repo_name(repo);
        let _ = ctx.ui_tx.send(UiEvent::Status(format!(
            "Backing up [{}] ({}/{total})...",
            repo_name,
            i + 1
        )));

        let passphrase = match get_or_resolve_passphrase(repo, &mut ctx.passphrases) {
            Ok(p) => p,
            Err(e) => {
                send_log(&ctx.ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                continue;
            }
        };

        if repo.config.encryption.mode != config::EncryptionModeConfig::None && passphrase.is_none()
        {
            send_log(
                &ctx.ui_tx,
                format!("[{repo_name}] passphrase prompt canceled; skipping."),
            );
            continue;
        }

        match run_selection_with_progress(
            ctx,
            repo,
            &matching_sources,
            passphrase.as_deref().map(|s| s.as_str()),
        ) {
            Ok(report) => {
                if !report.created.is_empty() {
                    any_backed_up = true;
                }
                log_backup_report(&ctx.ui_tx, &repo_name, &report);
            }
            Err(e) => {
                send_log(&ctx.ui_tx, format!("[{repo_name}] backup failed: {e}"));
            }
        }
    }

    if !any_backed_up {
        send_log(
            &ctx.ui_tx,
            format!("No repositories found with source '{source_label}'."),
        );
    } else {
        let _ = ctx.ui_tx.send(UiEvent::TriggerSnapshotRefresh);
        let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
    }

    end_backup_operation(ctx);
}
