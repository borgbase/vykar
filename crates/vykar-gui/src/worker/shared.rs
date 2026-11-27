use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use crossbeam_channel::Sender;
use vykar_core::app::SkipKind;
use vykar_core::app::scheduler::{CycleId, Work};
use vykar_core::app::service::{HookedCommand, RepoSelection};
use vykar_core::app::{AppService, OpsError, OpsResult, operations};
use vykar_core::commands::backup::BackupProgressEvent;
use vykar_core::config::{self, ResolvedRepo, SourceEntry};
use vykar_core::repo::control::{CancelToken, FnMutSink};
use vykar_core::repo::{OpenOptions, Repo};
use vykar_types::error::Result;

use crate::cancel::CancelSlot;
use crate::messages::UiEvent;
use crate::progress::BackupStatusTracker;
use crate::repo_helpers::{DialogPrompt, send_log};
use crate::scheduler::{SchedulerState, send_cadence_errors};

use super::WorkerContext;

/// How a backup operation counts in the scheduler.
#[derive(Debug, Clone, Copy)]
pub(super) enum BackupWork {
    /// An order the scheduler handed out; the guard ends it.
    Order(CycleId),
    /// A manual full cycle over every repository ("Back up all"): when it ends,
    /// only the slots it overran move.
    Cycle,
    /// A single-repo or single-source backup: holds scheduled cycles off,
    /// reschedules nothing.
    Other,
}

/// The scheduler work a backup guard ends on drop.
struct ScheduleHold<'a> {
    state: &'a Mutex<SchedulerState>,
    notify: &'a Sender<()>,
    id: CycleId,
}

/// RAII guard for a worker operation. Construction emits `OperationStarted` +
/// an initial `Status`; `Drop` emits `OperationFinished` + `Status("Idle")` and,
/// for backup operations, ends the work in the scheduler and pokes it.
///
/// The guard borrows only the individual channel/flag fields it needs (not the
/// whole [`WorkerContext`]), so handlers can still mutably borrow disjoint ctx
/// fields (e.g. `ctx.service`) while a guard is live.
pub(super) struct OpGuard<'a> {
    ui_tx: &'a Sender<UiEvent>,
    /// Cleared on drop; set for every operation so the tray (and window) can
    /// tell whether *any* operation is in flight, not just backups.
    operation_running: &'a AtomicBool,
    /// `Some` for backup operations: the scheduler work this guard ends on drop.
    /// `None` for UI operations.
    schedule: Option<ScheduleHold<'a>>,
    /// Failure message recorded via [`OpGuard::fail`]. When set, `Drop` emits a
    /// persistent `ErrorStatus` instead of `Status("Idle")`.
    failed: Option<String>,
}

impl<'a> OpGuard<'a> {
    /// Begin a UI (read/maintenance) operation.
    pub(super) fn ui(
        ui_tx: &'a Sender<UiEvent>,
        cancel_requested: &CancelSlot,
        operation_running: &'a AtomicBool,
        status_msg: impl Into<String>,
    ) -> Self {
        cancel_requested.renew();
        operation_running.store(true, Ordering::SeqCst);
        let _ = ui_tx.send(UiEvent::OperationStarted);
        let _ = ui_tx.send(UiEvent::Status(status_msg.into()));
        Self {
            ui_tx,
            operation_running,
            schedule: None,
            failed: None,
        }
    }

    /// Begin a backup operation. It holds scheduled cycles off until the guard
    /// drops; the drop ends the work in the scheduler (rescheduling what
    /// `work` covers) and then pokes the scheduler thread.
    pub(super) fn backup(
        ui_tx: &'a Sender<UiEvent>,
        cancel_requested: &CancelSlot,
        operation_running: &'a AtomicBool,
        scheduler: &'a Mutex<SchedulerState>,
        sched_notify_tx: &'a Sender<()>,
        work: BackupWork,
        status_msg: impl Into<String>,
    ) -> Self {
        cancel_requested.renew();
        operation_running.store(true, Ordering::SeqCst);
        let id = match work {
            BackupWork::Order(id) => Some(id),
            BackupWork::Cycle => scheduler
                .lock()
                .ok()
                .map(|mut s| s.started(Work::Cycle(RepoSelection::All))),
            BackupWork::Other => scheduler.lock().ok().map(|mut s| s.started(Work::Other)),
        };
        let _ = ui_tx.send(UiEvent::OperationStarted);
        let _ = ui_tx.send(UiEvent::Status(status_msg.into()));
        Self {
            ui_tx,
            operation_running,
            schedule: id.map(|id| ScheduleHold {
                state: scheduler,
                notify: sched_notify_tx,
                id,
            }),
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
        // End the work before the final notify: a scheduler thread blocked
        // because this work ran is woken only after it can see the work ended.
        if let Some(hold) = &self.schedule
            && let Ok(mut state) = hold.state.lock()
        {
            let errors = state.finished(hold.id, SystemTime::now());
            drop(state);
            send_cadence_errors(self.ui_tx, errors);
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
        if let Some(hold) = &self.schedule {
            let _ = hold.notify.try_send(());
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

/// Guard + repo-select + open scaffold shared by the conventional worker
/// handlers (delete, diff, find); prune uses [`with_hooked_repo_op`]. The
/// repository is opened with the passphrase (`OpenOptions::new()`, observing
/// the operation's cancel token), a refused passphrase re-prompts and only
/// the open runs again, and the passphrase is accepted right after the open;
/// `run` gets the handle. A decryption failure inside `run` is the
/// command's error, not a refused passphrase: it does not re-prompt.
///
/// The guard lives for the duration of `run`, so a repo miss, a dismissed
/// prompt, and a failed run are all reported here — exactly once each. On
/// failure the `Err` carries a short user-facing reason for handlers that also
/// have to push a result event (diff); handlers with nothing to report simply
/// drop it.
///
/// `run` receives the cancel token rather than reading it off the context: the
/// context is mutably borrowed here for the app service, so a closure
/// capturing `ctx` would not borrow-check.
///
/// Mount deliberately does **not** use this: it interleaves
/// `UiEvent::MountFailed` / `MountStopped` lifecycle events with every failure
/// arm and hands the opened repository (or, for one snapshot, the validated
/// passphrase) to a spawned thread.
pub(super) fn with_open_repo_op<T>(
    ctx: &mut WorkerContext,
    repo_name: &str,
    status: impl Into<String>,
    label: &str,
    mut run: impl FnMut(&ResolvedRepo, &mut Repo, &CancelToken) -> Result<T>,
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

    let cancel = ctx.cancel_requested.current();
    let mut prompt = DialogPrompt;
    let mut pass = ctx
        .service
        .passphrase(&repo.config, repo.label.as_deref(), &mut prompt);
    let outcome = pass
        .open::<OpsError>(OpenOptions::new(), &cancel)
        .and_then(|mut r| run(repo, &mut r, &cancel).map_err(OpsError::from));

    match outcome {
        Ok(value) => Ok(value),
        // A dismissed prompt is the user's choice, not a failure: log it and
        // leave the status bar clean.
        Err(OpsError::Skipped(r)) if r.kind == SkipKind::PassphrasePromptCanceled => {
            send_log(&ctx.ui_tx, format!("[{repo_name}] {PASSPHRASE_CANCELLED}"));
            Err("passphrase required".to_string())
        }
        Err(e) => {
            guard.fail(format!("[{repo_name}] {label} failed: {e}"));
            Err(e.to_string())
        }
    }
}

/// [`with_open_repo_op`] for a hookable command: the repo-level hooks for
/// `command` enclose the passphrase step, the open and `run`, so a prompt
/// appears only after the `before` hooks pass and a re-prompt does not re-run
/// them. The handle is closed before the `after` hooks run. A hook skip or a
/// dismissed prompt is logged and is not a failure; hook warnings are logged.
pub(super) fn with_hooked_repo_op<T>(
    ctx: &mut WorkerContext,
    repo_name: &str,
    status: impl Into<String>,
    command: HookedCommand,
    mut run: impl FnMut(&ResolvedRepo, &mut Repo, &CancelToken) -> Result<T>,
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

    let ui_tx = &ctx.ui_tx;
    let cancel = ctx.cancel_requested.current();
    let command_name = command.name();
    // A dismissed prompt is a skip, so `finally` hooks run but `after` hooks do not.
    let outcome = ctx.service.run_hooked(
        repo,
        command,
        false,
        &mut DialogPrompt,
        &mut |w| send_log(ui_tx, format!("[{repo_name}] hook warning: {w}")),
        |pass| {
            let mut r = pass.open::<OpsError>(OpenOptions::new(), &cancel)?;
            run(repo, &mut r, &cancel).map_err(OpsError::from)
        },
    );

    match outcome {
        Ok(value) => Ok(value),
        Err(OpsError::Skipped(r)) if r.kind == SkipKind::PassphrasePromptCanceled => {
            send_log(ui_tx, format!("[{repo_name}] {PASSPHRASE_CANCELLED}"));
            Err("passphrase required".to_string())
        }
        Err(OpsError::Skipped(reason)) => {
            send_log(
                ui_tx,
                format!("[{repo_name}] {command_name} skipped: {reason}"),
            );
            Err(reason.message)
        }
        Err(e) => {
            guard.fail(format!("[{repo_name}] {command_name} failed: {e}"));
            Err(e.to_string())
        }
    }
}

/// Backup of `sources` with progress in the status bar, through the app
/// service: the dialog comes after the repo `before` hooks.
pub(super) fn run_selection_with_progress(
    service: &mut AppService,
    ui_tx: &Sender<UiEvent>,
    cancel: &CancelToken,
    repo: &ResolvedRepo,
    sources: &[SourceEntry],
) -> OpsResult<operations::BackupRunReport> {
    let repo_name = repo.label_or_url().to_string();
    let mut tracker = BackupStatusTracker::new(repo_name.clone());
    let ui_tx_progress = ui_tx.clone();
    service.backup(
        repo,
        sources,
        &mut DialogPrompt,
        cancel,
        false,
        Some(&FnMutSink::new(
            |evt: operations::BackupRunEvent| match evt {
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
                operations::BackupRunEvent::SourceFailed { label, error } => {
                    send_log(
                        &ui_tx_progress,
                        format!("[{repo_name}] source {label} failed: {error}"),
                    );
                }
            },
        )),
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
        let cancel = CancelSlot::default();
        cancel.cancel();
        let op_running = AtomicBool::new(false);
        {
            let _g = OpGuard::ui(&tx, &cancel, &op_running, "Working...");
            // fresh cancel token installed, operation flag set at construction.
            assert!(!cancel.is_cancelled());
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
        let cancel = CancelSlot::default();
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
        let cancel = CancelSlot::default();
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
        let cancel = CancelSlot::default();
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
        assert!(
            logs.first()
                .is_some_and(|m| m.contains("No repository matching 'missing'"))
        );
        assert!(matches!(events.last(), Some(UiEvent::ErrorStatus(_))));
    }

    /// A state whose `1h` slot for "repo-a" lies an hour in the past.
    fn overdue_state() -> Mutex<SchedulerState> {
        let mut state = SchedulerState::default();
        let schedule = config::ScheduleConfig {
            enabled: true,
            every: Some("1h".into()),
            cron: None,
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        };
        let past = SystemTime::now() - std::time::Duration::from_secs(2 * 3600);
        assert!(
            state
                .set_repos(vec![("repo-a".into(), schedule)], past, false)
                .is_empty()
        );
        Mutex::new(state)
    }

    #[test]
    fn backup_guard_holds_and_releases_the_scheduler() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let (sched_tx, sched_rx) = crossbeam_channel::bounded(1);
        let cancel = CancelSlot::default();
        let op_running = AtomicBool::new(false);
        let state = overdue_state();
        {
            let _g = OpGuard::backup(
                &tx,
                &cancel,
                &op_running,
                &state,
                &sched_tx,
                BackupWork::Other,
                "Backing up...",
            );
            assert!(op_running.load(Ordering::SeqCst));
            let (order, _) = state.lock().unwrap().poll(SystemTime::now());
            assert!(
                order.is_none(),
                "nothing is handed out while the guard lives"
            );
        }
        assert!(!op_running.load(Ordering::SeqCst));
        // Scheduler was poked exactly once.
        assert!(sched_rx.try_recv().is_ok());
        assert!(sched_rx.try_recv().is_err());
        let events = drain(&rx);
        assert!(matches!(events.first(), Some(UiEvent::OperationStarted)));
        assert!(matches!(events.last(), Some(UiEvent::Status(s)) if s == "Idle"));

        // `Other` rescheduled nothing: the overdue slot is handed out now.
        let (order, _) = state.lock().unwrap().poll(SystemTime::now());
        let (_, names) = order.expect("the overdue slot fires after the guard drops");
        assert_eq!(names, ["repo-a"]);
    }

    #[test]
    fn backup_guard_for_an_order_reschedules_its_repos() {
        let (tx, _rx) = crossbeam_channel::unbounded();
        let (sched_tx, sched_rx) = crossbeam_channel::bounded(1);
        let cancel = CancelSlot::default();
        let op_running = AtomicBool::new(false);
        let state = overdue_state();
        let (order, _) = state.lock().unwrap().poll(SystemTime::now());
        let (id, _) = order.expect("the overdue slot is handed out");

        drop(OpGuard::backup(
            &tx,
            &cancel,
            &op_running,
            &state,
            &sched_tx,
            BackupWork::Order(id),
            "Running scheduled backup cycle...",
        ));
        assert!(sched_rx.try_recv().is_ok());

        let mut s = state.lock().unwrap();
        assert!(s.next_run(0).unwrap() > SystemTime::now());
        assert!(s.poll(SystemTime::now()).0.is_none());
    }
}
