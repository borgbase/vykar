#![cfg_attr(
    all(target_os = "windows", not(debug_assertions)),
    windows_subsystem = "windows"
)]
// `slint!`-generated code expands `todo!()`/`unreachable!()` for unsupported
// embedding paths; the lints fire on macro-expanded source we cannot edit.
#![allow(clippy::todo, clippy::unimplemented, clippy::unreachable)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use slint::ComponentHandle;

mod autostart;
mod bootstrap;
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
mod ui_state;
mod update_check;
mod view_models;
mod worker;
use messages::{log_entry_now, AppCommand, UiEvent};

const APP_TITLE: &str = "Vykar Backup";

// slint::include_modules!() — generated code, not subject to our lints.
#[allow(
    warnings,
    clippy::all,
    clippy::pedantic,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod generated_ui {
    slint::include_modules!();
}
use generated_ui::*;

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

    let (app_tx, app_rx) = crossbeam_channel::unbounded::<AppCommand>();
    let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();

    let scheduler = Arc::new(Mutex::new(scheduler::SchedulerState::default()));
    let backup_running = Arc::new(AtomicBool::new(false));
    // Set while *any* worker operation runs (backup or UI read); drives the tray
    // "Cancel" item so it matches the window Cancel button.
    let operation_running = Arc::new(AtomicBool::new(false));
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

    let ui = bootstrap::init_main_window(&gui_state, initial_config_path)?;
    let tray = bootstrap::spawn_tray()?;

    // ── Background threads (only after all fallible init succeeded) ──

    bootstrap::spawn_background_threads(
        app_tx.clone(),
        app_rx,
        ui_tx.clone(),
        scheduler,
        backup_running.clone(),
        operation_running.clone(),
        cancel_requested.clone(),
        runtime,
        scheduler_lock_held,
        sched_notify_tx,
        sched_notify_rx,
    );

    // ── Event consumer ──

    let (tray_source_tx, tray_source_rx) =
        crossbeam_channel::unbounded::<Vec<(tray_icon::menu::MenuId, String)>>();

    event_consumer::spawn(
        ui_rx,
        ui.as_weak(),
        app_tx.clone(),
        tray_source_tx,
        start_in_background_pref.clone(),
    );

    // ── Callback wiring ──

    controllers::main_window::wire_callbacks(
        &ui,
        app_tx.clone(),
        ui_tx.clone(),
        cancel_requested.clone(),
    );

    // ── Settings tab + window lifecycle + tray event loop ──

    let autostart_on = bootstrap::wire_settings_tab(&ui, &ui_tx, &start_in_background_pref);
    bootstrap::wire_window_lifecycle(&ui, &start_in_background_pref);
    bootstrap::spawn_tray_event_loop(
        app_tx,
        ui_tx,
        cancel_requested,
        operation_running,
        &tray,
        tray_source_rx,
    );

    if !autostart::should_start_hidden(gui_state.start_in_background, autostart_on) {
        ui.show()?;
    }
    slint::run_event_loop_until_quit()?;

    // Persist GUI state. Eager saves (config change, window hide) cover most
    // paths; this final capture handles Cmd-Q on macOS where the event loop
    // exits without triggering on_close_requested.
    let final_state = event_consumer::capture_gui_state(&ui, &start_in_background_pref)
        .or_else(ui_state::last_gui_state);
    if let Some(s) = final_state {
        state::save(&s);
    }

    Ok(())
}
