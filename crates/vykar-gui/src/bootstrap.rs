//! Startup wiring extracted from `main::run`: window init, tray creation,
//! background threads, settings-tab and window-lifecycle callbacks, and the
//! tray menu-event loop. `run` orchestrates these; each function owns one
//! coherent slice of startup.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam_channel::{Receiver, Sender};
use slint::ComponentHandle;
use tray_icon::menu::{MenuEvent, MenuId};
use vykar_core::app;

use crate::messages::{log_entry_now, AppCommand, UiEvent};
use crate::repo_helpers::send_log;
use crate::{autostart, event_consumer, scheduler, state, ui_state, update_check, worker};
use crate::{AppData, MainWindow};

/// Create the main window and seed its initial (pre-worker) state.
pub(crate) fn init_main_window(
    gui_state: &state::GuiState,
    initial_config_path: String,
) -> Result<MainWindow, slint::PlatformError> {
    let ui = MainWindow::new()?;
    if let (Some(w), Some(h)) = (gui_state.window_width, gui_state.window_height) {
        ui.window().set_size(slint::LogicalSize::new(w, h));
    }
    if let Some(p) = gui_state.last_page {
        ui.set_current_page(state::page_from_i32(p));
    }
    // Selection is resolved by name once RepoModelData arrives. Holding the
    // name in UI-thread state avoids the stale-index bug where repo filtering
    // leaves the saved index pointing at the wrong row.
    ui_state::install(&ui, gui_state.last_repo_name.clone());
    ui.set_config_path("(loading...)".into());
    ui.set_editor_font_family(
        if cfg!(target_os = "macos") {
            "Menlo"
        } else if cfg!(target_os = "windows") {
            "Consolas"
        } else {
            "DejaVu Sans Mono"
        }
        .into(),
    );
    ui.set_status_text("Idle".into());
    ui.set_version_text(format!("v{}", env!("CARGO_PKG_VERSION")).into());

    // Seed the AppData global with the initial config path.
    ui.global::<AppData>()
        .set_active_config_path(initial_config_path.into());

    Ok(ui)
}

/// Menu IDs (and, off Linux, the live tray-icon handle) produced by
/// [`spawn_tray`]. The `_tray_icon` field keeps the icon alive for the process
/// lifetime on platforms where it lives on the main thread; on Linux the icon
/// is owned by the dedicated GTK thread instead.
pub(crate) struct TrayHandles {
    pub open_item_id: MenuId,
    pub run_now_item_id: MenuId,
    pub quit_item_id: MenuId,
    pub cancel_item_id: MenuId,
    #[cfg(not(target_os = "linux"))]
    _tray_icon: tray_icon::TrayIcon,
}

/// Build the tray icon and return its menu IDs.
///
/// On Linux, tray-icon requires a running GTK event loop for D-Bus
/// registration (AppIndicator) and menu signals; a dedicated thread owns the
/// tray icon and runs `gtk::main()` — event-driven, zero CPU when idle. On
/// other platforms the icon lives on the main thread (returned in
/// [`TrayHandles`] to stay alive).
pub(crate) fn spawn_tray() -> Result<TrayHandles, Box<dyn std::error::Error>> {
    #[cfg(target_os = "linux")]
    let handles = {
        let (ids_tx, ids_rx) =
            std::sync::mpsc::sync_channel::<Result<(MenuId, MenuId, MenuId, MenuId), String>>(1);
        thread::spawn(move || {
            if let Err(e) = gtk::init() {
                let _ = ids_tx.send(Err(format!("Failed to initialize GTK: {e}")));
                return;
            }
            match crate::tray::build_tray_icon() {
                Ok((_tray, open_id, run_now_id, quit_id, source_submenu, cancel_id)) => {
                    crate::tray_state::set_submenu(source_submenu);
                    let _ = ids_tx.send(Ok((open_id, run_now_id, quit_id, cancel_id)));
                    gtk::main();
                }
                Err(e) => {
                    let _ = ids_tx.send(Err(format!("failed to initialize tray icon: {e}")));
                }
            }
        });
        let (open_item_id, run_now_item_id, quit_item_id, cancel_item_id) = ids_rx
            .recv()
            .map_err(|_| "GTK thread exited unexpectedly")??;
        TrayHandles {
            open_item_id,
            run_now_item_id,
            quit_item_id,
            cancel_item_id,
        }
    };

    #[cfg(not(target_os = "linux"))]
    let handles = {
        let (
            tray_icon,
            open_item_id,
            run_now_item_id,
            quit_item_id,
            source_submenu,
            cancel_item_id,
        ) = crate::tray::build_tray_icon()
            .map_err(|e| format!("failed to initialize tray icon: {e}"))?;
        crate::tray_state::set_submenu(source_submenu);
        TrayHandles {
            open_item_id,
            run_now_item_id,
            quit_item_id,
            cancel_item_id,
            _tray_icon: tray_icon,
        }
    };

    Ok(handles)
}

/// Spawn the scheduler, the once-per-launch update check, and the worker.
/// Call only after all fallible UI/tray init has succeeded.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_background_threads(
    app_tx: Sender<AppCommand>,
    app_rx: Receiver<AppCommand>,
    ui_tx: Sender<UiEvent>,
    scheduler: Arc<Mutex<scheduler::SchedulerState>>,
    backup_running: Arc<AtomicBool>,
    operation_running: Arc<AtomicBool>,
    cancel_requested: Arc<AtomicBool>,
    runtime: app::RuntimeConfig,
    scheduler_lock_held: bool,
    sched_notify_tx: Sender<()>,
    sched_notify_rx: Receiver<()>,
) {
    scheduler::spawn_scheduler(
        app_tx.clone(),
        ui_tx.clone(),
        scheduler.clone(),
        backup_running.clone(),
        sched_notify_rx,
    );

    // Best-effort, once-per-launch update check. Detached from the worker so the
    // network call never delays startup commands.
    let ui_tx_update = ui_tx.clone();
    thread::spawn(move || {
        if let Some(info) = update_check::check(env!("CARGO_PKG_VERSION")) {
            let _ = ui_tx_update.send(UiEvent::UpdateAvailable {
                version: info.version,
                url: info.url,
            });
        }
    });

    thread::spawn(move || {
        worker::run_worker(
            app_tx,
            app_rx,
            ui_tx,
            scheduler,
            backup_running,
            operation_running,
            cancel_requested,
            runtime,
            scheduler_lock_held,
            sched_notify_tx,
        )
    });
}

/// Wire the Settings tab: detect autostart state, seed the toggles, and hook
/// their handlers. Returns the detected `autostart_on` state, which the caller
/// needs to decide whether to start hidden.
pub(crate) fn wire_settings_tab(
    ui: &MainWindow,
    ui_tx: &Sender<UiEvent>,
    start_in_background_pref: &Arc<AtomicBool>,
) -> bool {
    let autostart_on = match autostart::is_enabled() {
        Ok(v) => v,
        Err(e) => {
            let _ = ui_tx.send(log_entry_now(format!(
                "Could not detect autostart state: {e}"
            )));
            false
        }
    };
    ui.set_start_at_login(autostart_on);
    ui.set_start_in_background(start_in_background_pref.load(Ordering::Relaxed) || autostart_on);
    ui.set_start_in_background_enabled(!autostart_on);

    // "Start at login" toggle — register/remove OS autostart entry.
    ui.on_start_at_login_toggled({
        let ui_weak = ui.as_weak();
        let ui_tx = ui_tx.clone();
        let pref = start_in_background_pref.clone();
        move |checked| {
            let Some(ui) = ui_weak.upgrade() else { return };
            if let Err(e) = autostart::set_enabled(checked) {
                // Revert checkbox to previous state.
                ui.set_start_at_login(!checked);
                send_log(&ui_tx, format!("Autostart failed: {e}"));
                return;
            }
            if checked {
                // Force background on (display only), disable the checkbox.
                ui.set_start_in_background(true);
                ui.set_start_in_background_enabled(false);
                send_log(&ui_tx, "Autostart enabled.");
            } else {
                // Restore background checkbox to persisted preference.
                ui.set_start_in_background(pref.load(Ordering::Relaxed));
                ui.set_start_in_background_enabled(true);
                send_log(&ui_tx, "Autostart disabled.");
            }
        }
    });

    // "Start in background" toggle — only reachable when autostart is off.
    ui.on_start_in_background_toggled({
        let pref = start_in_background_pref.clone();
        let ui_weak = ui.as_weak();
        move |checked| {
            pref.store(checked, Ordering::Relaxed);
            // Capture live UI state so we never overwrite config_path / window
            // size with stale or default values.
            if let Some(s) = ui_weak
                .upgrade()
                .and_then(|ui| event_consumer::capture_gui_state(&ui, &pref))
            {
                state::save(&s);
                ui_state::set_last_gui_state(s);
            }
        }
    });

    autostart_on
}

/// Wire close-to-tray behavior: both the window-manager close and the in-app
/// Ctrl/Cmd-W path persist GUI state and hide (rather than quit) the window.
pub(crate) fn wire_window_lifecycle(ui: &MainWindow, start_in_background_pref: &Arc<AtomicBool>) {
    ui.window().on_close_requested({
        let ui_weak = ui.as_weak();
        let pref = start_in_background_pref.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                if let Some(s) = event_consumer::capture_gui_state(&ui, &pref) {
                    state::save(&s);
                    ui_state::set_last_gui_state(s);
                }
                ui.invoke_release_focus();
                let _ = ui.hide();
            }
            slint::CloseRequestResponse::HideWindow
        }
    });

    ui.on_close_window({
        let ui_weak = ui.as_weak();
        let pref = start_in_background_pref.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                if let Some(s) = event_consumer::capture_gui_state(&ui, &pref) {
                    state::save(&s);
                    ui_state::set_last_gui_state(s);
                }
                ui.invoke_release_focus();
                let _ = ui.hide();
            }
        }
    });
}

/// Spawn the tray menu-event loop. Runs on its own thread so the tray stays
/// responsive even while the worker is busy.
pub(crate) fn spawn_tray_event_loop(
    app_tx: Sender<AppCommand>,
    ui_tx: Sender<UiEvent>,
    cancel_requested: Arc<AtomicBool>,
    operation_running: Arc<AtomicBool>,
    tray: &TrayHandles,
    tray_source_rx: Receiver<Vec<(MenuId, String)>>,
) {
    let open_item_id = tray.open_item_id.clone();
    let run_now_item_id = tray.run_now_item_id.clone();
    let cancel_item_id = tray.cancel_item_id.clone();
    let quit_item_id = tray.quit_item_id.clone();

    thread::spawn(move || {
        let tx = app_tx;
        let cancel = cancel_requested;
        let log_tx = ui_tx;
        let menu_rx = MenuEvent::receiver();
        let mut tray_source_items: Vec<(MenuId, String)> = Vec::new();
        loop {
            crossbeam_channel::select! {
                recv(menu_rx) -> event => {
                    let Ok(event) = event else {
                        break;
                    };
                    if event.id == open_item_id {
                        // Bypass the worker queue so the tray stays responsive even
                        // while the worker is busy (e.g. initial FetchAllRepoInfo).
                        let _ = log_tx.send(UiEvent::ShowWindow);
                    } else if event.id == run_now_item_id {
                        let _ = tx.send(AppCommand::RunBackupAll { scheduled: false });
                    } else if event.id == cancel_item_id {
                        // Mirror the window Cancel button: active whenever *any*
                        // operation (backup or UI read) is running.
                        if !operation_running.load(Ordering::SeqCst) {
                            send_log(&log_tx, "No operation running.");
                            continue;
                        }
                        cancel.store(true, Ordering::SeqCst);
                        send_log(
                            &log_tx,
                            "Cancel requested; Vykar will stop when the current file, upload, or storage operation returns.",
                        );
                    } else if event.id == quit_item_id {
                        let _ = log_tx.send(UiEvent::Quit);
                        break;
                    } else if let Some((_, label)) =
                        tray_source_items.iter().find(|(id, _)| *id == event.id)
                    {
                        let _ = tx.send(AppCommand::RunBackupSource {
                            source_label: label.clone(),
                        });
                    }
                }
                recv(tray_source_rx) -> items => {
                    match items {
                        Ok(items) => tray_source_items = items,
                        Err(_) => break,
                    }
                }
            }
        }
    });
}
