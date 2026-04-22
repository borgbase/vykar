#![cfg_attr(
    all(target_os = "windows", not(debug_assertions)),
    windows_subsystem = "windows"
)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use slint::ComponentHandle;
use tray_icon::menu::MenuEvent;

mod autostart;
mod config_helpers;
mod controllers;
mod event_consumer;
mod file_tree;
mod messages;
mod progress;
mod repo_helpers;
mod scheduler;
mod state;
mod tray;
mod tray_state;
mod view_models;
mod worker;
use messages::{log_entry_now, AppCommand, SnapshotRowData, SourceInfoData, UiEvent};
use repo_helpers::send_log;

const APP_TITLE: &str = "Vykar Backup";

slint::include_modules!();

fn main() {
    if let Err(e) = run() {
        let msg = format!("{APP_TITLE} failed to start:\n\n{e}");
        tinyfiledialogs::message_box_ok(
            &format!("{APP_TITLE} \u{2014} Error"),
            &msg,
            tinyfiledialogs::MessageBoxIcon::Error,
        );
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let gui_state = state::load();
    let runtime = config_helpers::resolve_or_create_config(gui_state.config_path.as_deref())?;

    // Compute the initial active config path for the AppData global.
    let initial_config_path = dunce::canonicalize(runtime.source.path())
        .unwrap_or_else(|_| runtime.source.path().to_path_buf())
        .display()
        .to_string();

    // Persisted user preference for start-in-background (independent of autostart state).
    let start_in_background_pref = Arc::new(AtomicBool::new(
        gui_state.start_in_background.unwrap_or(false),
    ));

    // Last captured GUI state — updated on every window hide so we have a valid
    // snapshot even if the window is already destroyed when the process exits.
    let last_gui_state: Arc<Mutex<Option<state::GuiState>>> = Arc::new(Mutex::new(None));

    let (app_tx, app_rx) = crossbeam_channel::unbounded::<AppCommand>();
    let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();

    let scheduler = Arc::new(Mutex::new(scheduler::SchedulerState::default()));
    let backup_running = Arc::new(AtomicBool::new(false));
    let cancel_requested = Arc::new(AtomicBool::new(false));

    // Attempt to acquire the process-wide scheduler lock.
    // If another scheduler (daemon or GUI) holds it, disable automatic scheduling
    // but keep the GUI fully functional for manual operations.
    let scheduler_lock = vykar_core::app::scheduler::SchedulerLock::try_acquire();
    let scheduler_lock_held = scheduler_lock.is_some();
    // Keep the lock alive for the entire process lifetime.
    let _scheduler_lock = scheduler_lock;

    if !scheduler_lock_held {
        let _ = ui_tx.send(log_entry_now(
            "Scheduler disabled \u{2014} another vykar scheduler is already running (daemon or GUI).",
        ));
    }

    let (sched_notify_tx, sched_notify_rx) = crossbeam_channel::bounded::<()>(1);

    // ── Fallible UI / tray initialization (before spawning any threads) ──

    let ui = MainWindow::new()?;
    if let (Some(w), Some(h)) = (gui_state.window_width, gui_state.window_height) {
        ui.window().set_size(slint::LogicalSize::new(w, h));
    }
    if let Some(p) = gui_state.last_page {
        ui.set_current_page(state::page_from_i32(p));
    }
    // Selection is resolved by name once RepoModelData arrives — see
    // event_consumer. Holding the name here avoids the stale-index bug where
    // repo filtering (failed loads, reordering, renames) leaves the saved
    // index pointing at the wrong row.
    let pending_repo_name: Arc<Mutex<Option<String>>> =
        Arc::new(Mutex::new(gui_state.last_repo_name.clone()));
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

    // On Linux, tray-icon requires a running GTK event loop for D-Bus
    // registration (AppIndicator) and menu signals. Spawn a dedicated thread
    // that owns the tray icon and runs gtk::main() — fully event-driven,
    // zero CPU when idle. On other platforms the tray icon lives on the main
    // thread and no GTK integration is needed.
    #[cfg(target_os = "linux")]
    let (open_item_id, run_now_item_id, quit_item_id, cancel_item_id) = {
        use tray_icon::menu::MenuId;
        let (ids_tx, ids_rx) =
            std::sync::mpsc::sync_channel::<Result<(MenuId, MenuId, MenuId, MenuId), String>>(1);
        thread::spawn(move || {
            if let Err(e) = gtk::init() {
                let _ = ids_tx.send(Err(format!("Failed to initialize GTK: {e}")));
                return;
            }
            match tray::build_tray_icon() {
                Ok((_tray, open_id, run_now_id, quit_id, source_submenu, cancel_id)) => {
                    tray_state::set_submenu(source_submenu);
                    let _ = ids_tx.send(Ok((open_id, run_now_id, quit_id, cancel_id)));
                    gtk::main();
                }
                Err(e) => {
                    let _ = ids_tx.send(Err(format!("failed to initialize tray icon: {e}")));
                }
            }
        });
        ids_rx
            .recv()
            .map_err(|_| "GTK thread exited unexpectedly")??
    };

    #[cfg(not(target_os = "linux"))]
    let (_tray_icon, open_item_id, run_now_item_id, quit_item_id, source_submenu, cancel_item_id) =
        tray::build_tray_icon().map_err(|e| format!("failed to initialize tray icon: {e}"))?;

    #[cfg(not(target_os = "linux"))]
    tray_state::set_submenu(source_submenu);

    // ── Background threads (only after all fallible init succeeded) ──

    scheduler::spawn_scheduler(
        app_tx.clone(),
        ui_tx.clone(),
        scheduler.clone(),
        backup_running.clone(),
        sched_notify_rx,
    );

    let ui_tx_for_cancel = ui_tx.clone();

    thread::spawn({
        let app_tx = app_tx.clone();
        let scheduler = scheduler.clone();
        let backup_running = backup_running.clone();
        let cancel_requested = cancel_requested.clone();
        let sched_notify_tx = sched_notify_tx.clone();
        move || {
            worker::run_worker(
                app_tx,
                app_rx,
                ui_tx,
                scheduler,
                backup_running,
                cancel_requested,
                runtime,
                scheduler_lock_held,
                sched_notify_tx,
            )
        }
    });

    // snapshot_data stays as Arc<Mutex> — complex Rust struct used by sort_snapshot_table.
    let snapshot_data: Arc<Mutex<Vec<SnapshotRowData>>> = Arc::new(Mutex::new(Vec::new()));

    // Raw source data, used to rebuild the per-repo filtered source model
    // when the selected repo changes.
    let source_cache: Arc<Mutex<Vec<SourceInfoData>>> = Arc::new(Mutex::new(Vec::new()));

    // ── Event consumer ──

    let tray_source_items: Arc<Mutex<Vec<(tray_icon::menu::MenuId, String)>>> =
        Arc::new(Mutex::new(Vec::new()));

    event_consumer::spawn(
        ui_rx,
        ui.as_weak(),
        app_tx.clone(),
        snapshot_data.clone(),
        source_cache.clone(),
        last_gui_state.clone(),
        tray_source_items.clone(),
        start_in_background_pref.clone(),
        pending_repo_name.clone(),
    );

    // ── Callback wiring ──

    controllers::main_window::wire_callbacks(
        &ui,
        app_tx.clone(),
        ui_tx_for_cancel.clone(),
        snapshot_data,
        source_cache,
        cancel_requested.clone(),
    );

    // ── Settings tab initialization ──

    let autostart_on = match autostart::is_enabled() {
        Ok(v) => v,
        Err(e) => {
            let _ = ui_tx_for_cancel.send(messages::log_entry_now(format!(
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
        let ui_tx = ui_tx_for_cancel.clone();
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
        let last = last_gui_state.clone();
        let pending = pending_repo_name.clone();
        move |checked| {
            pref.store(checked, Ordering::Relaxed);
            // Capture live UI state so we never overwrite config_path / window
            // size with stale or default values.
            if let Some(s) = ui_weak
                .upgrade()
                .and_then(|ui| event_consumer::capture_gui_state(&ui, &pref, &pending))
            {
                state::save(&s);
                if let Ok(mut guard) = last.lock() {
                    *guard = Some(s);
                }
            }
        }
    });

    // ── Close-to-tray behavior ──

    ui.window().on_close_requested({
        let ui_weak = ui.as_weak();
        let last_gui_state = last_gui_state.clone();
        let pref = start_in_background_pref.clone();
        let pending = pending_repo_name.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                if let Some(s) = event_consumer::capture_gui_state(&ui, &pref, &pending) {
                    state::save(&s);
                    if let Ok(mut last) = last_gui_state.lock() {
                        *last = Some(s);
                    }
                }
                ui.invoke_release_focus();
                let _ = ui.hide();
            }
            slint::CloseRequestResponse::HideWindow
        }
    });

    ui.on_close_window({
        let ui_weak = ui.as_weak();
        let last_gui_state = last_gui_state.clone();
        let pref = start_in_background_pref.clone();
        let pending = pending_repo_name.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                if let Some(s) = event_consumer::capture_gui_state(&ui, &pref, &pending) {
                    state::save(&s);
                    if let Ok(mut last) = last_gui_state.lock() {
                        *last = Some(s);
                    }
                }
                ui.invoke_release_focus();
                let _ = ui.hide();
            }
        }
    });

    // ── Tray event handler ──

    {
        let tx = app_tx.clone();
        let tray_source_items = tray_source_items.clone();
        let cancel = cancel_requested.clone();
        let log_tx = ui_tx_for_cancel.clone();
        let backup_running = backup_running.clone();
        thread::spawn(move || {
            let menu_rx = MenuEvent::receiver();
            while let Ok(event) = menu_rx.recv() {
                if event.id == open_item_id {
                    // Bypass the worker queue so the tray stays responsive even
                    // while the worker is busy (e.g. initial FetchAllRepoInfo).
                    let _ = log_tx.send(UiEvent::ShowWindow);
                } else if event.id == run_now_item_id {
                    let _ = tx.send(AppCommand::RunBackupAll { scheduled: false });
                } else if event.id == cancel_item_id {
                    if !backup_running.load(Ordering::SeqCst) {
                        send_log(&log_tx, "No backup running.");
                        continue;
                    }
                    cancel.store(true, Ordering::SeqCst);
                    send_log(
                        &log_tx,
                        "Cancel requested; will stop after current step completes.",
                    );
                } else if event.id == quit_item_id {
                    let _ = log_tx.send(UiEvent::Quit);
                    break;
                } else if let Ok(items) = tray_source_items.lock() {
                    if let Some((_, label)) = items.iter().find(|(id, _)| *id == event.id) {
                        let _ = tx.send(AppCommand::RunBackupSource {
                            source_label: label.clone(),
                        });
                    }
                }
            }
        });
    }

    if !autostart::should_start_hidden(gui_state.start_in_background, autostart_on) {
        ui.show()?;
    }
    slint::run_event_loop_until_quit()?;

    // Persist GUI state. Eager saves (config change, window hide) cover most
    // paths; this final capture handles Cmd-Q on macOS where the event loop
    // exits without triggering on_close_requested.
    let final_state =
        event_consumer::capture_gui_state(&ui, &start_in_background_pref, &pending_repo_name)
            .or_else(|| last_gui_state.lock().ok().and_then(|g| g.clone()));
    if let Some(s) = final_state {
        state::save(&s);
    }

    Ok(())
}
