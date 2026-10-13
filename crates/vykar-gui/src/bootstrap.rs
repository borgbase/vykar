//! Startup wiring extracted from `main::run`: window init, tray creation,
//! background threads, and settings-tab and window-lifecycle callbacks. `run`
//! orchestrates these; each function owns one coherent slice of startup.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam_channel::{Receiver, Sender};
use slint::ComponentHandle;
use vykar_core::app;

use crate::messages::{log_entry_now, AppCommand, UiEvent};
use crate::repo_helpers::send_log;
use crate::{autostart, event_consumer, scheduler, state, tray, ui_state, update_check, worker};
use crate::{AppData, MainWindow, VykarTray};

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

/// Wire the tray menu. Callbacks run on the Slint event loop and only send
/// messages or flip atomics, so the tray stays responsive while the worker is
/// busy.
fn wire_tray(
    tray: &VykarTray,
    app_tx: &Sender<AppCommand>,
    ui_tx: &Sender<UiEvent>,
    cancel_requested: &Arc<AtomicBool>,
    operation_running: &Arc<AtomicBool>,
) {
    tray.on_open_clicked({
        let ui_tx = ui_tx.clone();
        move || {
            let _ = ui_tx.send(UiEvent::ShowWindow);
        }
    });
    tray.on_run_all_clicked({
        let app_tx = app_tx.clone();
        move || {
            let _ = app_tx.send(AppCommand::RunBackupAll { scheduled: false });
        }
    });
    tray.on_run_source_clicked({
        let app_tx = app_tx.clone();
        move |label| {
            let _ = app_tx.send(AppCommand::RunBackupSource {
                source_label: label.to_string(),
            });
        }
    });
    tray.on_cancel_clicked({
        let ui_tx = ui_tx.clone();
        let cancel_requested = cancel_requested.clone();
        let operation_running = operation_running.clone();
        move || {
            // Mirror the window Cancel button: active whenever *any*
            // operation (backup or UI read) is running.
            if !operation_running.load(Ordering::SeqCst) {
                send_log(&ui_tx, "No operation running.");
                return;
            }
            cancel_requested.store(true, Ordering::SeqCst);
            send_log(
                &ui_tx,
                "Cancel requested; Vykar will stop when the current file, upload, or storage operation returns.",
            );
        }
    });
    tray.on_quit_clicked({
        let ui_tx = ui_tx.clone();
        move || {
            let _ = ui_tx.send(UiEvent::Quit);
        }
    });
}

/// Create the tray. Returns whether it is up now; on Linux it is created later
/// if no StatusNotifierItem host is running yet. Construction errors abort
/// startup, as before.
pub(crate) fn setup_tray(
    app_tx: &Sender<AppCommand>,
    ui_tx: &Sender<UiEvent>,
    cancel_requested: Arc<AtomicBool>,
    operation_running: Arc<AtomicBool>,
) -> Result<bool, Box<dyn std::error::Error>> {
    tray::set_factory({
        let app_tx = app_tx.clone();
        let ui_tx = ui_tx.clone();
        move || {
            let tray = tray::build_tray()?;
            wire_tray(
                &tray,
                &app_tx,
                &ui_tx,
                &cancel_requested,
                &operation_running,
            );
            Ok(tray)
        }
    });

    #[cfg(target_os = "linux")]
    if !tray::linux::watcher_present() {
        let ui_tx = ui_tx.clone();
        thread::spawn(move || {
            if let Err(e) = tray::linux::wait_for_watcher() {
                send_log(&ui_tx, format!("System tray unavailable: {e}"));
                return;
            }
            let _ = slint::invoke_from_event_loop(move || {
                if let Err(e) = tray::create() {
                    send_log(&ui_tx, format!("Failed to create tray icon: {e}"));
                }
            });
        });
        return Ok(false);
    }

    tray::create().map_err(|e| format!("failed to initialize tray icon: {e}"))?;

    #[cfg(target_os = "windows")]
    tray::windows::spawn_taskbar_listener(|| {
        let _ = slint::invoke_from_event_loop(|| {
            let _ = tray::create();
        });
    });

    Ok(true)
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
/// Ctrl/Cmd-W path persist GUI state and hide (rather than quit) the window,
/// or quit if no tray icon exists yet.
pub(crate) fn wire_window_lifecycle(
    ui: &MainWindow,
    ui_tx: &Sender<UiEvent>,
    start_in_background_pref: &Arc<AtomicBool>,
) {
    ui.window().on_close_requested({
        let ui_weak = ui.as_weak();
        let pref = start_in_background_pref.clone();
        let ui_tx = ui_tx.clone();
        move || {
            // Without a tray there would be no way back to a hidden window.
            if !tray::is_installed() {
                let _ = ui_tx.send(UiEvent::Quit);
                return slint::CloseRequestResponse::KeepWindowShown;
            }
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
        let ui_tx = ui_tx.clone();
        move || {
            if !tray::is_installed() {
                let _ = ui_tx.send(UiEvent::Quit);
                return;
            }
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
