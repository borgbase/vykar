use std::sync::atomic::Ordering;

use vykar_core::app::operations;
use vykar_core::commands::backup::BackupProgressEvent;
use vykar_core::config;
use vykar_types::error::VykarError;

use crate::messages::{AppCommand, UiEvent};
use crate::progress::{format_check_status, format_step_outcome, BackupStatusTracker};
use crate::repo_helpers::{
    format_repo_name, log_backup_report, send_log, with_passphrase_retry, PassphraseRun,
};

use super::shared::{run_selection_with_progress, OpGuard};
use super::WorkerContext;

pub(super) fn handle_backup_all(ctx: &mut WorkerContext, scheduled: bool) {
    let status = if scheduled {
        "Running scheduled backup cycle..."
    } else {
        "Running backup cycle..."
    };
    let mut guard = OpGuard::backup(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        &ctx.backup_running,
        &ctx.sched_notify_tx,
        status,
    );

    let mut any_snapshots_created = false;
    // Per-repo failures, aggregated into a single `guard.fail` after the loop so
    // a partial failure shows one red status naming the failed repos (rather than
    // a per-iteration flash that a later success would visually overwrite).
    let mut failures: Vec<(String, String)> = Vec::new();
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

        // Validate + cache the passphrase before the cycle (retry on a wrong
        // dialog entry; a wrong value is never cached). `run_full_cycle_for_repo`
        // buries decryption failures inside `steps` as a string, so it cannot
        // drive `with_passphrase_retry` directly — the `info::run` probe does.
        let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
            vykar_core::commands::info::run(&repo.config, pass)
        });
        let passphrase = match outcome {
            Ok(PassphraseRun::Ran(_)) => ctx.passphrases.get(&repo.config.repository.url).cloned(),
            Ok(PassphraseRun::Canceled) => {
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] passphrase prompt canceled; skipping this repository"),
                );
                continue;
            }
            Err(VykarError::RepoNotFound(_)) => {
                // An uninitialized repo cannot be backed up; surface it as a
                // failure rather than crashing the cycle below.
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] repository not initialized; skipping."),
                );
                failures.push((repo_name.clone(), "repository not initialized".to_string()));
                continue;
            }
            Err(e) => {
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] failed to open repository: {e}"),
                );
                failures.push((repo_name.clone(), format!("{e}")));
                continue;
            }
        };

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

        if result.has_failures() {
            let reason = result
                .steps
                .iter()
                .filter_map(|(step, o)| match o {
                    operations::StepOutcome::Failed(e) => {
                        Some(format!("{} failed: {e}", step.command_name()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(", ");
            failures.push((repo_name.clone(), reason));
        }
    }

    if !failures.is_empty() {
        let detail = failures
            .iter()
            .map(|(name, reason)| format!("[{name}] {reason}"))
            .collect::<Vec<_>>()
            .join("; ");
        guard.fail(format!(
            "Backup failed for {} of {total} repositories: {detail}",
            failures.len()
        ));
    }

    if any_snapshots_created {
        let _ = ctx.ui_tx.send(UiEvent::TriggerSnapshotRefresh);
        let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
    }
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
    let mut guard = OpGuard::backup(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        &ctx.backup_running,
        &ctx.sched_notify_tx,
        format!("Running backup for [{rn}]..."),
    );

    let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
        run_selection_with_progress(&ctx.ui_tx, &ctx.cancel_requested, repo, &repo.sources, pass)
    });

    match outcome {
        Ok(PassphraseRun::Ran(report)) => {
            if !report.created.is_empty() {
                let _ = ctx.app_tx.send(AppCommand::RefreshSnapshots {
                    repo_selector: rn.clone(),
                });
                let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
            }
            log_backup_report(&ctx.ui_tx, &rn, &report);
        }
        Ok(PassphraseRun::Canceled) => {
            send_log(
                &ctx.ui_tx,
                format!("[{rn}] passphrase prompt canceled; skipping."),
            );
        }
        Err(e) => {
            guard.fail(format!("[{rn}] backup failed: {e}"));
        }
    }
}

pub(super) fn handle_backup_source(ctx: &mut WorkerContext, source_label: String) {
    let source_label = source_label.trim().to_string();
    if source_label.is_empty() {
        send_log(&ctx.ui_tx, "Select a source first.");
        return;
    }

    let mut guard = OpGuard::backup(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        &ctx.backup_running,
        &ctx.sched_notify_tx,
        format!("Running backup for source '{source_label}'..."),
    );

    let mut any_backed_up = false;
    // Repos that actually carry the source (attempted), and per-repo failures
    // aggregated into a single `guard.fail` after the loop — mirrors
    // `handle_backup_all` so a partial failure shows one red status naming the
    // failed repos rather than a misleading all-red state.
    let mut attempted = 0usize;
    let mut failures: Vec<(String, String)> = Vec::new();
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
        attempted += 1;

        let repo_name = format_repo_name(repo);
        let _ = ctx.ui_tx.send(UiEvent::Status(format!(
            "Backing up [{}] ({}/{total})...",
            repo_name,
            i + 1
        )));

        let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
            run_selection_with_progress(
                &ctx.ui_tx,
                &ctx.cancel_requested,
                repo,
                &matching_sources,
                pass,
            )
        });

        match outcome {
            Ok(PassphraseRun::Ran(report)) => {
                if !report.created.is_empty() {
                    any_backed_up = true;
                }
                log_backup_report(&ctx.ui_tx, &repo_name, &report);
            }
            Ok(PassphraseRun::Canceled) => {
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] passphrase prompt canceled; skipping."),
                );
            }
            Err(e) => {
                send_log(&ctx.ui_tx, format!("[{repo_name}] backup failed: {e}"));
                failures.push((repo_name.clone(), format!("{e}")));
            }
        }
    }

    if !failures.is_empty() {
        let detail = failures
            .iter()
            .map(|(name, reason)| format!("[{name}] {reason}"))
            .collect::<Vec<_>>()
            .join("; ");
        guard.fail(format!(
            "Backup failed for {} of {attempted} repositories with source '{source_label}': {detail}",
            failures.len()
        ));
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
}
