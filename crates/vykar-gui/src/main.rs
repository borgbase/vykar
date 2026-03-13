#![cfg_attr(
    all(target_os = "windows", not(debug_assertions)),
    windows_subsystem = "windows"
)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use chrono::Local;
use slint::ComponentHandle;
use tray_icon::menu::{MenuEvent, MenuId, MenuItem};

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
mod view_models;
mod worker;
use messages::{AppCommand, SnapshotRowData, UiEvent};
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
        let _ = ui_tx.send(UiEvent::LogEntry {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            message: "Scheduler disabled \u{2014} another vykar scheduler is already running (daemon or GUI).".to_string(),
        });
    }

    scheduler::spawn_scheduler(
        app_tx.clone(),
        ui_tx.clone(),
        scheduler.clone(),
        backup_running.clone(),
    );

    let ui_tx_for_cancel = ui_tx.clone();

    thread::spawn({
        let app_tx = app_tx.clone();
        let scheduler = scheduler.clone();
        let backup_running = backup_running.clone();
        let cancel_requested = cancel_requested.clone();
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

    let restore_win = RestoreWindow::new()?;
    let find_win = FindWindow::new()?;

    // snapshot_data stays as Arc<Mutex> — complex Rust struct used by sort_snapshot_table.
    let snapshot_data: Arc<Mutex<Vec<SnapshotRowData>>> = Arc::new(Mutex::new(Vec::new()));

    // ── Event consumer ──

    let tray_source_items: Arc<Mutex<Vec<(MenuId, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let (submenu_labels_tx, submenu_labels_rx) = crossbeam_channel::unbounded::<Vec<String>>();

    event_consumer::spawn(
        ui_rx,
        ui.as_weak(),
        restore_win.as_weak(),
        find_win.as_weak(),
        app_tx.clone(),
        snapshot_data.clone(),
        last_gui_state.clone(),
        submenu_labels_tx,
    );

    // ── Callback wiring ──

    controllers::main_window::wire_callbacks(
        &ui,
        &restore_win,
        &find_win,
        app_tx.clone(),
        ui_tx_for_cancel.clone(),
        cancel_requested.clone(),
        snapshot_data,
    );
    controllers::restore::wire_callbacks(&restore_win, app_tx.clone());
    controllers::find::wire_callbacks(&find_win, app_tx.clone());

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
                let _ = ui.hide();
            }
            slint::CloseRequestResponse::HideWindow
        }
    });

    // ── Periodic resize-save timer ──
    // Flush GUI state to disk when the window size changes so Cmd-Q (which
    // bypasses on_close_requested) doesn't lose the latest dimensions.
    let _resize_save_timer = {
        let ui_weak = ui.as_weak();
        let last_gui_state = last_gui_state.clone();
        let mut last_saved_size: Option<(u32, u32)> = None;
        let timer = slint::Timer::default();
        timer.start(
            slint::TimerMode::Repeated,
            Duration::from_secs(2),
            move || {
                let Some(ui) = ui_weak.upgrade() else {
                    return;
                };
                let sz = ui.window().size();
                let current = (sz.width, sz.height);
                if current.0 == 0 || current.1 == 0 {
                    return;
                }
                if last_saved_size == Some(current) {
                    return;
                }
                if let Some(s) = event_consumer::capture_gui_state(&ui) {
                    state::save(&s);
                    if let Ok(mut last) = last_gui_state.lock() {
                        *last = Some(s);
                    }
                    last_saved_size = Some(current);
                }
            },
        );
        timer
    };

    // ── Tray icon ──

    let (_tray_icon, open_item_id, run_now_item_id, quit_item_id, source_submenu, cancel_item) =
        tray::build_tray_icon().map_err(|e| format!("failed to initialize tray icon: {e}"))?;

    let cancel_item_id = cancel_item.id().clone();

    // Timer to keep tray menu state in sync with the app.
    // Submenu/MenuItem are !Send, so they must stay on the main thread; the event
    // consumer sends updated labels via a channel and this timer picks them up.
    // The timer also syncs the cancel item's enabled state with backup_running.
    let _tray_sync_timer = {
        let tray_source_items = tray_source_items.clone();
        let backup_running = backup_running.clone();
        let timer = slint::Timer::default();
        let mut was_running = false;
        timer.start(
            slint::TimerMode::Repeated,
            Duration::from_millis(200),
            move || {
                // Drain all pending submenu updates, keeping only the latest
                let mut latest = None;
                while let Ok(labels) = submenu_labels_rx.try_recv() {
                    latest = Some(labels);
                }
                if let Some(labels) = latest {
                    while source_submenu.remove_at(0).is_some() {}
                    let mut new_items = Vec::new();
                    for label in &labels {
                        let mi = MenuItem::new(label, true, None);
                        new_items.push((mi.id().clone(), label.clone()));
                        let _ = source_submenu.append(&mi);
                    }
                    if let Ok(mut tsi) = tray_source_items.lock() {
                        *tsi = new_items;
                    }
                }

                // Sync cancel item enabled state
                let running = backup_running.load(Ordering::SeqCst);
                if running != was_running {
                    cancel_item.set_enabled(running);
                    was_running = running;
                }
            },
        );
        timer
    };

    {
        let tx = app_tx.clone();
        let tray_source_items = tray_source_items.clone();
        let cancel = cancel_requested.clone();
        let log_tx = ui_tx_for_cancel.clone();
        thread::spawn(move || {
            let menu_rx = MenuEvent::receiver();
            while let Ok(event) = menu_rx.recv() {
                if event.id == open_item_id {
                    let _ = tx.send(AppCommand::ShowWindow);
                } else if event.id == run_now_item_id {
                    let _ = tx.send(AppCommand::RunBackupAll { scheduled: false });
                } else if event.id == cancel_item_id {
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

    // Persist GUI state. Eager saves (config change, resize timer, window hide)
    // cover most paths; this final capture handles Cmd-Q on macOS where the
    // event loop exits without triggering on_close_requested.
    let final_state = event_consumer::capture_gui_state(&ui)
        .or_else(|| last_gui_state.lock().ok().and_then(|g| g.clone()));
    if let Some(s) = final_state {
        state::save(&s);
    }

    Ok(())
}
