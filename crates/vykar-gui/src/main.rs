#![cfg_attr(
    all(target_os = "windows", not(debug_assertions)),
    windows_subsystem = "windows"
)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use slint::ComponentHandle;
use tray_icon::menu::MenuEvent;

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
use messages::{log_entry_now, AppCommand, SnapshotRowData, UiEvent};
use repo_helpers::send_log;

const APP_TITLE: &str = "Vykar Backup";

slint::include_modules!();

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tray-icon uses GTK widgets internally on Linux; GTK must be
    // initialised before any Menu / MenuItem is created.
    #[cfg(target_os = "linux")]
    gtk::init().expect("Failed to initialize GTK");

    let gui_state = state::load();
    let runtime = config_helpers::resolve_or_create_config(gui_state.config_path.as_deref())?;

    // Compute the initial active config path for the AppData global.
    let initial_config_path = dunce::canonicalize(runtime.source.path())
        .unwrap_or_else(|_| runtime.source.path().to_path_buf())
        .display()
        .to_string();

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

    let ui = MainWindow::new()?;
    if let (Some(w), Some(h)) = (gui_state.window_width, gui_state.window_height) {
        ui.window().set_size(slint::LogicalSize::new(w, h));
    }
    ui.set_config_path("(loading...)".into());
    ui.set_schedule_text("(loading...)".into());
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

    // Seed the AppData global with the initial config path.
    ui.global::<AppData>()
        .set_active_config_path(initial_config_path.into());

    // snapshot_data stays as Arc<Mutex> — complex Rust struct used by sort_snapshot_table.
    let snapshot_data: Arc<Mutex<Vec<SnapshotRowData>>> = Arc::new(Mutex::new(Vec::new()));

    // ── Event consumer ──

    let tray_source_items: Arc<Mutex<Vec<(tray_icon::menu::MenuId, String)>>> =
        Arc::new(Mutex::new(Vec::new()));

    event_consumer::spawn(
        ui_rx,
        ui.as_weak(),
        app_tx.clone(),
        snapshot_data.clone(),
        last_gui_state.clone(),
        tray_source_items.clone(),
    );

    // ── Callback wiring ──

    controllers::main_window::wire_callbacks(
        &ui,
        app_tx.clone(),
        ui_tx_for_cancel.clone(),
        cancel_requested.clone(),
        snapshot_data,
    );

    // ── Close-to-tray behavior ──

    ui.window().on_close_requested({
        let ui_weak = ui.as_weak();
        let last_gui_state = last_gui_state.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                if let Some(s) = event_consumer::capture_gui_state(&ui) {
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
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                if let Some(s) = event_consumer::capture_gui_state(&ui) {
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

    // ── Tray icon ──

    let (_tray_icon, open_item_id, run_now_item_id, quit_item_id, source_submenu, cancel_item_id) =
        tray::build_tray_icon().map_err(|e| format!("failed to initialize tray icon: {e}"))?;

    // Store the submenu in a thread-local so the event consumer can rebuild it
    // directly from invoke_from_event_loop (no polling timer needed).
    tray_state::set_submenu(source_submenu);

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
                    let _ = tx.send(AppCommand::ShowWindow);
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
                    let _ = tx.send(AppCommand::Quit);
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

    ui.show()?;
    slint::run_event_loop_until_quit()?;

    // Persist GUI state. Eager saves (config change, window hide) cover most
    // paths; this final capture handles Cmd-Q on macOS where the event loop
    // exits without triggering on_close_requested.
    let final_state = event_consumer::capture_gui_state(&ui)
        .or_else(|| last_gui_state.lock().ok().and_then(|g| g.clone()));
    if let Some(s) = final_state {
        state::save(&s);
    }

    Ok(())
}
