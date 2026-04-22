use std::sync::atomic::Ordering;

use vykar_core::app::operations;
use vykar_core::config::{self, ResolvedRepo, SourceEntry};
use vykar_types::error::Result;

use crate::messages::UiEvent;
use crate::progress::BackupStatusTracker;
use crate::repo_helpers::{format_repo_name, send_log};

use super::WorkerContext;

pub(super) fn begin_backup_operation(ctx: &WorkerContext, status_msg: impl Into<String>) {
    ctx.cancel_requested.store(false, Ordering::SeqCst);
    ctx.backup_running.store(true, Ordering::SeqCst);
    let _ = ctx.ui_tx.send(UiEvent::OperationStarted);
    let _ = ctx.ui_tx.send(UiEvent::Status(status_msg.into()));
}

pub(super) fn end_backup_operation(ctx: &WorkerContext) {
    ctx.backup_running.store(false, Ordering::SeqCst);
    let _ = ctx.ui_tx.send(UiEvent::OperationFinished);
    let _ = ctx.ui_tx.send(UiEvent::Status("Idle".to_string()));
    let _ = ctx.sched_notify_tx.try_send(());
}

pub(super) fn begin_ui_operation(ctx: &WorkerContext, status_msg: impl Into<String>) {
    ctx.cancel_requested.store(false, Ordering::SeqCst);
    let _ = ctx.ui_tx.send(UiEvent::OperationStarted);
    let _ = ctx.ui_tx.send(UiEvent::Status(status_msg.into()));
}

pub(super) fn end_ui_operation(ctx: &WorkerContext) {
    let _ = ctx.ui_tx.send(UiEvent::OperationFinished);
    let _ = ctx.ui_tx.send(UiEvent::Status("Idle".to_string()));
}

pub(super) fn select_repo_or_log<'r>(
    ctx: &WorkerContext,
    repos: &'r [ResolvedRepo],
    name: &str,
) -> Option<&'r ResolvedRepo> {
    match config::select_repo(repos, name) {
        Some(r) => Some(r),
        None => {
            send_log(&ctx.ui_tx, format!("No repository matching '{name}'."));
            None
        }
    }
}

pub(super) fn run_selection_with_progress(
    ctx: &WorkerContext,
    repo: &ResolvedRepo,
    sources: &[SourceEntry],
    passphrase: Option<&str>,
) -> Result<operations::BackupRunReport> {
    let repo_name = format_repo_name(repo);
    let mut tracker = BackupStatusTracker::new(repo_name.clone());
    let ui_tx_progress = ctx.ui_tx.clone();
    operations::run_backup_selection(
        repo,
        sources,
        passphrase,
        Some(&ctx.cancel_requested),
        false,
        Some(&mut |evt| match evt {
            operations::BackupRunEvent::Backup(bpe) => {
                if let Some(status) = tracker.format(&bpe) {
                    let _ = ui_tx_progress.send(UiEvent::Status(status));
                }
            }
            operations::BackupRunEvent::HookWarning { warning, .. } => {
                send_log(
                    &ui_tx_progress,
                    format!("[{repo_name}] hook warning: {warning}"),
                );
            }
        }),
    )
}
