use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::Sender;
use vykar_core::app::operations;
use vykar_core::commands::backup::BackupProgressEvent;
use vykar_core::config::{self, ResolvedRepo, SourceEntry};
use vykar_types::error::Result;

use crate::messages::UiEvent;
use crate::progress::BackupStatusTracker;
use crate::repo_helpers::{send_log, with_passphrase_retry, PassphraseRun};

use super::WorkerContext;

/// RAII guard for a worker operation. Construction emits `OperationStarted` +
/// an initial `Status`; `Drop` emits `OperationFinished` + `Status("Idle")` and,
/// for backup operations, clears `backup_running` and pokes the scheduler.
///
/// The guard borrows only the individual channel/flag fields it needs (not the
/// whole [`WorkerContext`]), so handlers can still mutably borrow disjoint ctx
/// fields (e.g. `ctx.passphrases`) while a guard is live.
pub(super) struct OpGuard<'a> {
    ui_tx: &'a Sender<UiEvent>,
    /// Cleared on drop; set for every operation so the tray (and window) can
    /// tell whether *any* operation is in flight, not just backups.
    operation_running: &'a AtomicBool,
    /// `Some` for backup operations, cleared on drop; `None` for UI operations.
    backup_running: Option<&'a AtomicBool>,
    /// `Some` for backup operations, poked on drop so the scheduler recomputes.
    sched_notify_tx: Option<&'a Sender<()>>,
    /// Failure message recorded via [`OpGuard::fail`]. When set, `Drop` emits a
    /// persistent `ErrorStatus` instead of `Status("Idle")`.
    failed: Option<String>,
}

impl<'a> OpGuard<'a> {
    /// Begin a UI (read/maintenance) operation.
    pub(super) fn ui(
        ui_tx: &'a Sender<UiEvent>,
        cancel_requested: &AtomicBool,
        operation_running: &'a AtomicBool,
        status_msg: impl Into<String>,
    ) -> Self {
        cancel_requested.store(false, Ordering::SeqCst);
        operation_running.store(true, Ordering::SeqCst);
        let _ = ui_tx.send(UiEvent::OperationStarted);
        let _ = ui_tx.send(UiEvent::Status(status_msg.into()));
        Self {
            ui_tx,
            operation_running,
            backup_running: None,
            sched_notify_tx: None,
            failed: None,
        }
    }

    /// Begin a backup operation (also sets `backup_running` and, on drop, pokes
    /// the scheduler).
    pub(super) fn backup(
        ui_tx: &'a Sender<UiEvent>,
        cancel_requested: &AtomicBool,
        operation_running: &'a AtomicBool,
        backup_running: &'a AtomicBool,
        sched_notify_tx: &'a Sender<()>,
        status_msg: impl Into<String>,
    ) -> Self {
        cancel_requested.store(false, Ordering::SeqCst);
        operation_running.store(true, Ordering::SeqCst);
        backup_running.store(true, Ordering::SeqCst);
        let _ = ui_tx.send(UiEvent::OperationStarted);
        let _ = ui_tx.send(UiEvent::Status(status_msg.into()));
        Self {
            ui_tx,
            operation_running,
            backup_running: Some(backup_running),
            sched_notify_tx: Some(sched_notify_tx),
            failed: None,
        }
    }

    /// Record a failure message. Also appends the message to the log immediately
    /// (the guard holds the `UiEvent` sender), so call sites need not pair a
    /// separate `send_log`. On drop the status bar shows a persistent, clickable
    /// error state instead of returning to "Idle". The *last* `fail` call wins (a
    /// later success does not clear an earlier failure — call `fail` only on the
    /// terminal failure path of an operation).
    pub(super) fn fail(&mut self, msg: impl Into<String>) {
        let msg = msg.into();
        send_log(self.ui_tx, msg.clone());
        self.failed = Some(msg);
    }
}

impl Drop for OpGuard<'_> {
    fn drop(&mut self) {
        if let Some(running) = self.backup_running {
            running.store(false, Ordering::SeqCst);
        }
        self.operation_running.store(false, Ordering::SeqCst);
        let _ = self.ui_tx.send(UiEvent::OperationFinished);
        match self.failed.take() {
            Some(msg) => {
                let _ = self.ui_tx.send(UiEvent::ErrorStatus(msg));
            }
            None => {
                let _ = self.ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
        }
        if let Some(tx) = self.sched_notify_tx {
            let _ = tx.try_send(());
        }
    }
}

/// Select a repository, logging the miss. For use at sites that hold **no**
/// [`OpGuard`] — a guarded site must use [`select_repo_or_fail`] (or call
/// `config::select_repo` directly and `guard.fail` with its own message), since
/// `OpGuard::fail` already logs and would double-log the same failure.
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

/// Guard-aware variant: records the miss on the guard, which is the single
/// logger (and turns the status bar into a persistent error state).
pub(super) fn select_repo_or_fail<'r>(
    guard: &mut OpGuard<'_>,
    repos: &'r [ResolvedRepo],
    name: &str,
) -> Option<&'r ResolvedRepo> {
    match config::select_repo(repos, name) {
        Some(r) => Some(r),
        None => {
            guard.fail(format!("No repository matching '{name}'."));
            None
        }
    }
}

/// Logged when the user dismisses a passphrase prompt mid-operation. One
/// constant so the four conventional handlers cannot drift apart.
const PASSPHRASE_CANCELLED: &str = "passphrase prompt canceled; skipping.";

/// Guard + repo-select + passphrase-retry scaffold shared by the conventional
/// worker handlers (delete, diff, prune, find).
///
/// The guard lives for the duration of `run`, so a repo miss, a dismissed
/// prompt, and a failed run are all reported here — exactly once each. On
/// failure the `Err` carries a short user-facing reason for handlers that also
/// have to push a result event (diff); handlers with nothing to report simply
/// drop it.
///
/// `run` receives the cancel flag rather than reading it off the context: the
/// context is mutably borrowed here for the passphrase cache, so a closure
/// capturing `ctx` would not borrow-check.
///
/// Mount deliberately does **not** use this: it interleaves
/// `UiEvent::MountFailed` / `MountStopped` lifecycle events with every failure
/// arm and hands the validated passphrase to a spawned thread.
pub(super) fn with_repo_op<T>(
    ctx: &mut WorkerContext,
    repo_name: &str,
    status: impl Into<String>,
    label: &str,
    mut run: impl FnMut(&ResolvedRepo, Option<&str>, &AtomicBool) -> Result<T>,
) -> std::result::Result<T, String> {
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        status,
    );

    let Some(repo) = select_repo_or_fail(&mut guard, &ctx.runtime.repos, repo_name) else {
        return Err("repository not found".to_string());
    };

    let cancel = &ctx.cancel_requested;
    let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
        run(repo, pass, cancel)
    });

    match outcome {
        Ok(PassphraseRun::Ran(value)) => Ok(value),
        // A dismissed prompt is the user's choice, not a failure: log it and
        // leave the status bar clean.
        Ok(PassphraseRun::Canceled) => {
            send_log(&ctx.ui_tx, format!("[{repo_name}] {PASSPHRASE_CANCELLED}"));
            Err("passphrase required".to_string())
        }
        Err(e) => {
            guard.fail(format!("[{repo_name}] {label} failed: {e}"));
            Err(e.to_string())
        }
    }
}

pub(super) fn run_selection_with_progress(
    ui_tx: &Sender<UiEvent>,
    cancel_requested: &AtomicBool,
    repo: &ResolvedRepo,
    sources: &[SourceEntry],
    passphrase: Option<&str>,
) -> Result<operations::BackupRunReport> {
    let repo_name = repo.label_or_url().to_string();
    let mut tracker = BackupStatusTracker::new(repo_name.clone());
    let ui_tx_progress = ui_tx.clone();
    operations::run_backup_selection(
        repo,
        sources,
        passphrase,
        Some(cancel_requested),
        false,
        Some(&mut |evt| match evt {
            operations::BackupRunEvent::Backup(BackupProgressEvent::Warning { message }) => {
                send_log(&ui_tx_progress, format!("[{repo_name}] warning: {message}"));
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(rx: &crossbeam_channel::Receiver<UiEvent>) -> Vec<UiEvent> {
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            out.push(ev);
        }
        out
    }

    #[test]
    fn ui_guard_emits_started_then_finished_on_drop() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let cancel = AtomicBool::new(true);
        let op_running = AtomicBool::new(false);
        {
            let _g = OpGuard::ui(&tx, &cancel, &op_running, "Working...");
            // cancel flag reset, operation flag set at construction.
            assert!(!cancel.load(Ordering::SeqCst));
            assert!(op_running.load(Ordering::SeqCst));
            let started = drain(&rx);
            assert!(
                matches!(started.as_slice(), [UiEvent::OperationStarted, UiEvent::Status(s)] if s == "Working...")
            );
        }
        assert!(!op_running.load(Ordering::SeqCst));
        let finished = drain(&rx);
        assert!(
            matches!(finished.as_slice(), [UiEvent::OperationFinished, UiEvent::Status(s)] if s == "Idle")
        );
    }

    #[test]
    fn ui_guard_pairs_started_finished_on_early_return() {
        // Simulates an early return: guard is created then dropped immediately.
        let (tx, rx) = crossbeam_channel::unbounded();
        let cancel = AtomicBool::new(false);
        let op_running = AtomicBool::new(false);
        drop(OpGuard::ui(&tx, &cancel, &op_running, "Deleting..."));
        let events = drain(&rx);
        assert!(matches!(
            events.as_slice(),
            [
                UiEvent::OperationStarted,
                UiEvent::Status(_),
                UiEvent::OperationFinished,
                UiEvent::Status(_),
            ]
        ));
    }

    #[test]
    fn failed_guard_emits_error_status_instead_of_idle() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let cancel = AtomicBool::new(false);
        let op_running = AtomicBool::new(false);
        {
            let mut g = OpGuard::ui(&tx, &cancel, &op_running, "Restoring...");
            g.fail("boom");
        }
        let events = drain(&rx);
        assert!(matches!(
            events.last(),
            Some(UiEvent::ErrorStatus(s)) if s == "boom"
        ));
        assert!(!op_running.load(Ordering::SeqCst));
    }

    /// Regression: a guarded repo miss must produce exactly one log line.
    /// `select_repo_or_log` + `guard.fail` used to log the same failure twice.
    #[test]
    fn select_repo_or_fail_logs_the_miss_once() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let cancel = AtomicBool::new(false);
        let op_running = AtomicBool::new(false);
        {
            let mut g = OpGuard::ui(&tx, &cancel, &op_running, "Deleting...");
            assert!(select_repo_or_fail(&mut g, &[], "missing").is_none());
        }
        let events = drain(&rx);
        let logs: Vec<&String> = events
            .iter()
            .filter_map(|e| match e {
                UiEvent::LogEntry { message, .. } => Some(message),
                _ => None,
            })
            .collect();
        assert_eq!(logs.len(), 1, "expected a single log line, got {logs:?}");
        assert!(logs
            .first()
            .is_some_and(|m| m.contains("No repository matching 'missing'")));
        assert!(matches!(events.last(), Some(UiEvent::ErrorStatus(_))));
    }

    #[test]
    fn backup_guard_toggles_running_and_notifies_scheduler() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let (sched_tx, sched_rx) = crossbeam_channel::bounded(1);
        let cancel = AtomicBool::new(false);
        let op_running = AtomicBool::new(false);
        let running = AtomicBool::new(false);
        {
            let _g = OpGuard::backup(
                &tx,
                &cancel,
                &op_running,
                &running,
                &sched_tx,
                "Backing up...",
            );
            assert!(running.load(Ordering::SeqCst));
            assert!(op_running.load(Ordering::SeqCst));
        }
        assert!(!running.load(Ordering::SeqCst));
        assert!(!op_running.load(Ordering::SeqCst));
        // Scheduler was poked exactly once.
        assert!(sched_rx.try_recv().is_ok());
        let events = drain(&rx);
        assert!(matches!(events.first(), Some(UiEvent::OperationStarted)));
        assert!(matches!(events.last(), Some(UiEvent::Status(s)) if s == "Idle"));
    }
}
