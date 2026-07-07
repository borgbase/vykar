use std::sync::Arc;

use vykar_core::commands::mount::{self, MountProgressEvent};

use crate::messages::UiEvent;
use crate::repo_helpers::{find_repo_for_snapshot, send_log, with_passphrase_retry, PassphraseRun};

use super::shared::{select_repo_or_log, OpGuard};
use super::WorkerContext;

pub(crate) struct MountHandle {
    shutdown: Arc<tokio::sync::Notify>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl MountHandle {
    pub(super) fn stop(mut self) {
        self.shutdown.notify_one();
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

    // Resolve repo config and passphrase.
    let (repo_cfg, pass) = match &snapshot_name {
        Some(snap) => {
            match find_repo_for_snapshot(&ctx.runtime.repos, &repo_name, snap, &mut ctx.passphrases)
            {
                Ok((repo, pass)) => (repo.config.clone(), pass),
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
            let repo = match select_repo_or_log(ctx, &ctx.runtime.repos, &repo_name) {
                Some(r) => r,
                None => {
                    guard.fail(format!("no repository matching '{repo_name}'"));
                    let _ = ctx.ui_tx.send(UiEvent::MountFailed {
                        message: format!("no repository matching '{repo_name}'"),
                    });
                    return;
                }
            };
            // Validate the passphrase before spawning the mount thread so a wrong
            // entry re-prompts (and is never cached) instead of failing async.
            let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
                vykar_core::commands::info::run(&repo.config, pass)
            });
            match outcome {
                Ok(PassphraseRun::Ran(_)) => {
                    // Passphrase (if any) is now validated and cached; read it
                    // back to hand to the mount thread. `None` for plaintext repos.
                    let pass = ctx.passphrases.get(&repo.config.repository.url).cloned();
                    (repo.config.clone(), pass)
                }
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
    };

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_thread = shutdown.clone();
    let ui_tx = ctx.ui_tx.clone();
    let snap_opt = snapshot_name.clone();

    let join = std::thread::Builder::new()
        .name("vykar-gui-mount".into())
        .spawn(move || {
            let ui_tx_cb = ui_tx.clone();
            let mut on_event = |e: MountProgressEvent| match e {
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
            let pass_ref: Option<&str> = pass.as_deref().map(|s| s.as_str());
            let result = mount::run_with_progress(
                &repo_cfg,
                pass_ref,
                snap_opt.as_deref(),
                "127.0.0.1:0",
                256,
                &[],
                Some(&mut on_event),
                Some(shutdown_thread),
            );
            match result {
                Ok(()) => {
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
        shutdown,
        join: Some(join),
    });
}

pub(super) fn handle_stop_mount(ctx: &mut WorkerContext) {
    if let Some(h) = ctx.mount.take() {
        h.stop();
    }
}
