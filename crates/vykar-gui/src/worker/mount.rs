use vykar_core::app::OpsError;
use vykar_core::app::passphrase::PASSPHRASE_PROMPT_CANCELED;
use vykar_core::commands::mount::{self, MountProgressEvent, MountRequest};
use vykar_core::config::VykarConfig;
use vykar_core::repo::Repo;
use vykar_core::repo::control::CancelToken;
use zeroize::Zeroizing;

use crate::messages::UiEvent;
use crate::repo_helpers::{DialogPrompt, PassphraseRun, find_repo_for_snapshot, send_log};

use super::WorkerContext;
use super::shared::OpGuard;

pub(crate) struct MountHandle {
    cancel: CancelToken,
    join: Option<std::thread::JoinHandle<()>>,
}

impl MountHandle {
    pub(super) fn stop(mut self) {
        self.cancel.cancel();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

pub(super) fn handle_start_mount(
    ctx: &mut WorkerContext,
    repo_name: String,
    snapshot_name: Option<String>,
) {
    if let Some(h) = ctx.mount.take() {
        h.stop();
        let _ = ctx.ui_tx.send(UiEvent::MountStopped);
    }

    // The guard covers only the synchronous startup (resolve repo/passphrase +
    // spawn); it emits `OperationStarted`/`Status` (clearing any stale red error)
    // and marks `operation_running` so the tray can cancel during startup. Once
    // the mount thread is spawned, its lifecycle is surfaced via the footer
    // banner (MountStarted/MountStopped/MountFailed), not the status bar.
    let mut guard = OpGuard::ui(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        "Starting mount...",
    );

    // The mount thread stops when this token is cancelled; the storage
    // observes it too.
    let mount_cancel = CancelToken::new();

    // Resolve the repository: opened here, or its config and passphrase.
    let source = match &snapshot_name {
        Some(snap) => {
            match find_repo_for_snapshot(&ctx.runtime.repos, &repo_name, snap, &mut ctx.service) {
                // The probe opened once; the mount thread opens again.
                Ok(PassphraseRun::Ran((repo, pass))) => MountSource::Config {
                    config: Box::new(repo.config.clone()),
                    passphrase: pass,
                },
                Ok(PassphraseRun::Canceled) => {
                    send_log(
                        &ctx.ui_tx,
                        format!("[{repo_name}] passphrase prompt canceled; mount skipped."),
                    );
                    // Reset the optimistic is_mount_active set by the UI on click.
                    let _ = ctx.ui_tx.send(UiEvent::MountStopped);
                    return;
                }
                Err(e) => {
                    guard.fail(format!("[{repo_name}] mount failed: {e}"));
                    let _ = ctx.ui_tx.send(UiEvent::MountFailed {
                        message: format!("{e}"),
                    });
                    return;
                }
            }
        }
        None => {
            // Plain `select_repo`: the guard is the single logger here
            // (`select_repo_or_log` would log a second, redundant line).
            let repo = match vykar_core::config::select_repo(&ctx.runtime.repos, &repo_name) {
                Some(r) => r,
                None => {
                    guard.fail(format!("no repository matching '{repo_name}'"));
                    let _ = ctx.ui_tx.send(UiEvent::MountFailed {
                        message: format!("no repository matching '{repo_name}'"),
                    });
                    return;
                }
            };
            // Open before spawning the mount thread so a wrong entry
            // re-prompts (and is never cached) instead of failing async. The
            // opened repository goes to the mount thread.
            let outcome = ctx
                .service
                .passphrase(&repo.config, repo.label.as_deref(), &mut DialogPrompt)
                .open::<OpsError>(mount::open_options(), &mount_cancel);
            match outcome {
                Ok(opened) => MountSource::Opened(Box::new(opened)),
                Err(OpsError::Skipped(r)) if r == PASSPHRASE_PROMPT_CANCELED => {
                    send_log(
                        &ctx.ui_tx,
                        format!("[{repo_name}] passphrase prompt canceled; mount skipped."),
                    );
                    // Reset the optimistic is_mount_active set by the UI on click.
                    let _ = ctx.ui_tx.send(UiEvent::MountStopped);
                    return;
                }
                Err(e) => {
                    guard.fail(format!("[{repo_name}] mount failed: {e}"));
                    let _ = ctx.ui_tx.send(UiEvent::MountFailed {
                        message: format!("{e}"),
                    });
                    return;
                }
            }
        }
    };

    let thread_cancel = mount_cancel.clone();
    let ui_tx = ctx.ui_tx.clone();
    let snap_opt = snapshot_name.clone();

    let join = std::thread::Builder::new()
        .name("vykar-gui-mount".into())
        .spawn(move || {
            let ui_tx_cb = ui_tx.clone();
            let on_event = |e: MountProgressEvent| match e {
                MountProgressEvent::Serving { address } => {
                    let _ = ui_tx_cb.send(UiEvent::MountStarted {
                        url: format!("http://{address}"),
                    });
                }
                // Mount lifecycle is surfaced via the footer banner (is_mount_active /
                // mount_url) so it doesn't clobber backup/restore progress in the
                // global status bar.
                MountProgressEvent::LoadingSnapshots => {}
                MountProgressEvent::ShuttingDown => {}
                MountProgressEvent::SnapshotLoaded { .. } => {}
            };
            let req = MountRequest {
                snapshot: snap_opt.as_deref(),
                address: "127.0.0.1:0",
                cache_size: 256,
                source_filter: &[],
            };
            let result = match source {
                MountSource::Opened(repo) => {
                    mount::run_on_repo(*repo, &req, Some(&on_event), &thread_cancel)
                }
                MountSource::Config { config, passphrase } => mount::run(
                    &config,
                    passphrase.as_deref().map(|s| s.as_str()),
                    &req,
                    Some(&on_event),
                    &thread_cancel,
                ),
            };
            match result {
                Ok(()) => {
                    let _ = ui_tx.send(UiEvent::MountStopped);
                }
                // Stopped while the thread was still opening or loading: the
                // reads observe the mount's token. A stop, not a failure.
                Err(vykar_types::error::VykarError::Interrupted)
                    if thread_cancel.is_cancelled() =>
                {
                    let _ = ui_tx.send(UiEvent::MountStopped);
                }
                Err(e) => {
                    let _ = ui_tx.send(UiEvent::MountFailed {
                        message: format!("{e}"),
                    });
                }
            }
        })
        .expect("failed to spawn mount thread");

    ctx.mount = Some(MountHandle {
        cancel: mount_cancel,
        join: Some(join),
    });
}

/// What the mount thread serves from.
enum MountSource {
    /// Opened on the worker with [`mount::open_options`].
    Opened(Box<Repo>),
    /// Opened by the mount thread.
    Config {
        config: Box<VykarConfig>,
        passphrase: Option<Zeroizing<String>>,
    },
}

pub(super) fn handle_stop_mount(ctx: &mut WorkerContext) {
    if let Some(h) = ctx.mount.take() {
        h.stop();
    }
}
