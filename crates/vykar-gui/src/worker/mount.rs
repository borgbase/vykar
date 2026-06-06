use std::sync::Arc;

use vykar_core::commands::mount::{self, MountProgressEvent};

use crate::messages::UiEvent;
use crate::repo_helpers::{find_repo_for_snapshot, get_or_resolve_passphrase, send_log};

use super::shared::select_repo_or_log;
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

    // Resolve repo config and passphrase.
    let (repo_cfg, pass) = match &snapshot_name {
        Some(snap) => {
            match find_repo_for_snapshot(&ctx.runtime.repos, &repo_name, snap, &mut ctx.passphrases)
            {
                Ok((repo, pass)) => (repo.config.clone(), pass),
                Err(e) => {
                    send_log(&ctx.ui_tx, format!("[{repo_name}] mount failed: {e}"));
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
                    let _ = ctx.ui_tx.send(UiEvent::MountFailed {
                        message: format!("no repository matching '{repo_name}'"),
                    });
                    return;
                }
            };
            let pass = match get_or_resolve_passphrase(repo, &mut ctx.passphrases) {
                Ok(p) => p,
                Err(e) => {
                    send_log(&ctx.ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                    let _ = ctx.ui_tx.send(UiEvent::MountFailed {
                        message: format!("{e}"),
                    });
                    return;
                }
            };
            (repo.config.clone(), pass)
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
