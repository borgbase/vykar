use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crossbeam_channel::Receiver;
use slint::{ComponentHandle, Model, ModelRc, SharedString, StandardListViewItem, VecModel};
use tray_icon::menu::MenuId;

use crate::controllers;
use crate::messages::{AppCommand, SnapshotRowData, UiEvent};
use crate::state;
use crate::tray_state;
use crate::view_models::{to_string_model, to_table_model};
use crate::{AppData, MainWindow, RepoInfo, SourceInfo};

thread_local! {
    static LOG_MODEL: RefCell<Option<Rc<VecModel<ModelRc<StandardListViewItem>>>>> = const { RefCell::new(None) };
}

fn ensure_log_model(ui: &MainWindow) -> Rc<VecModel<ModelRc<StandardListViewItem>>> {
    LOG_MODEL.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            let model = Rc::new(VecModel::<ModelRc<StandardListViewItem>>::default());
            ui.set_log_rows(ModelRc::from(model.clone()));
            *borrow = Some(model);
        }
        borrow.as_ref().unwrap().clone()
    })
}

pub(crate) const MAX_LOG_ROWS: usize = 2_000;
pub(crate) const TRIM_TARGET: usize = 1_800;

/// Insert a 3-column log row (Date, Time, Event) at position 0 (newest first).
/// Trims the model to `TRIM_TARGET` rows when it exceeds `MAX_LOG_ROWS`.
pub(crate) fn prepend_log_entry(
    model: &Rc<VecModel<ModelRc<StandardListViewItem>>>,
    date: &str,
    timestamp: &str,
    message: &str,
) {
    let row: Vec<StandardListViewItem> = vec![
        StandardListViewItem::from(SharedString::from(date)),
        StandardListViewItem::from(SharedString::from(timestamp)),
        StandardListViewItem::from(SharedString::from(message)),
    ];
    model.insert(0, ModelRc::new(VecModel::from(row)));
    if model.row_count() > MAX_LOG_ROWS {
        // Keep the first TRIM_TARGET rows (newest) and drop the rest.
        let keep: Vec<_> = (0..TRIM_TARGET)
            .map(|i| model.row_data(i).unwrap())
            .collect();
        model.set_vec(keep);
    }
}

fn append_log_row(ui: &MainWindow, date: &str, timestamp: &str, message: &str) {
    let model = ensure_log_model(ui);
    prepend_log_entry(&model, date, timestamp, message);
}

pub(crate) fn capture_gui_state(ui: &MainWindow) -> Option<state::GuiState> {
    let win_size = ui.window().size();
    let scale = ui.window().scale_factor();
    if win_size.width == 0 || win_size.height == 0 {
        return None;
    }
    if !scale.is_finite() || scale <= 0.0 {
        return None;
    }
    let w = win_size.width as f32 / scale;
    let h = win_size.height as f32 / scale;
    if !w.is_finite() || !h.is_finite() || w <= 0.0 || h <= 0.0 {
        return None;
    }
    let config_path_str = ui.global::<AppData>().get_active_config_path().to_string();
    let config_path = if config_path_str.is_empty() {
        None
    } else {
        Some(config_path_str)
    };
    Some(state::GuiState {
        config_path,
        window_width: Some(w),
        window_height: Some(h),
    })
}

pub(crate) fn spawn(
    ui_rx: Receiver<UiEvent>,
    ui_weak: slint::Weak<MainWindow>,
    app_tx: crossbeam_channel::Sender<AppCommand>,
    snapshot_data: Arc<Mutex<Vec<SnapshotRowData>>>,
    last_gui_state: Arc<Mutex<Option<state::GuiState>>>,
    tray_source_items: Arc<Mutex<Vec<(MenuId, String)>>>,
) {
    std::thread::spawn(move || {
        while let Ok(event) = ui_rx.recv() {
            let ui_weak = ui_weak.clone();
            let snapshot_data = snapshot_data.clone();
            let app_tx = app_tx.clone();
            let last_gui_state = last_gui_state.clone();
            let tray_source_items = tray_source_items.clone();

            let _ = slint::invoke_from_event_loop(move || {
                let Some(ui) = ui_weak.upgrade() else {
                    return;
                };

                match event {
                    UiEvent::Status(status) => ui.set_status_text(status.into()),
                    UiEvent::LogEntry {
                        date,
                        timestamp,
                        message,
                    } => {
                        append_log_row(&ui, &date, &timestamp, &message);
                    }
                    UiEvent::ConfigInfo { path, schedule } => {
                        ui.global::<AppData>()
                            .set_active_config_path(path.clone().into());
                        ui.set_config_path(path.into());
                        ui.set_schedule_text(schedule.into());
                        // Eagerly persist so Cmd-Q keeps the config path.
                        if let Some(s) = capture_gui_state(&ui) {
                            state::save(&s);
                            if let Ok(mut last) = last_gui_state.lock() {
                                *last = Some(s);
                            }
                        }
                    }
                    UiEvent::RepoNames(names) => {
                        let first = names.first().cloned().unwrap_or_default();
                        ui.set_repo_names(to_string_model(names));
                        // Pre-select first repo in snapshots combo and auto-load
                        if ui.get_snapshots_repo_combo_value().is_empty() && !first.is_empty() {
                            ui.set_snapshots_repo_combo_value(first.clone().into());
                            let _ = app_tx.send(AppCommand::RefreshSnapshots {
                                repo_selector: first,
                            });
                        }
                    }
                    UiEvent::RepoModelData { items, labels } => {
                        ui.set_repo_loading(false);
                        ui.global::<AppData>()
                            .set_repo_labels(to_string_model(labels));
                        let model: Vec<RepoInfo> = items
                            .into_iter()
                            .map(|d| RepoInfo {
                                name: d.name.into(),
                                url: d.url.into(),
                                snapshots: d.snapshots.into(),
                                last_snapshot: d.last_snapshot.into(),
                                size: d.size.into(),
                            })
                            .collect();
                        ui.set_repo_model(ModelRc::new(VecModel::from(model)));
                    }
                    UiEvent::SourceModelData { items, labels } => {
                        // On Linux the tray submenu lives on the GTK thread;
                        // dispatch via idle_add_once to run there.
                        #[cfg(target_os = "linux")]
                        {
                            let tray_labels = labels.clone();
                            let tsi = tray_source_items.clone();
                            gtk::glib::idle_add_once(move || {
                                tray_state::rebuild_submenu(&tray_labels, &tsi);
                            });
                        }
                        #[cfg(not(target_os = "linux"))]
                        tray_state::rebuild_submenu(&labels, &tray_source_items);

                        ui.global::<AppData>()
                            .set_source_labels(to_string_model(labels));
                        let model: Vec<SourceInfo> = items
                            .into_iter()
                            .map(|d| SourceInfo {
                                label: d.label.into(),
                                paths: d.paths.into(),
                                excludes: d.excludes.into(),
                                target_repos: d.target_repos.into(),
                                expanded: false,
                                detail_paths: d.detail_paths.into(),
                                detail_excludes: d.detail_excludes.into(),
                                detail_exclude_if_present: d.detail_exclude_if_present.into(),
                                detail_flags: d.detail_flags.into(),
                                detail_hooks: d.detail_hooks.into(),
                                detail_retention: d.detail_retention.into(),
                                detail_command_dumps: d.detail_command_dumps.into(),
                            })
                            .collect();
                        ui.set_source_model(ModelRc::new(VecModel::from(model)));
                    }
                    UiEvent::SnapshotTableData { data } => {
                        let ids: Vec<String> = data.iter().map(|d| d.id.clone()).collect();
                        let rnames: Vec<String> =
                            data.iter().map(|d| d.repo_name.clone()).collect();
                        ui.global::<AppData>()
                            .set_snapshot_ids(to_string_model(ids));
                        ui.global::<AppData>()
                            .set_snapshot_repo_names(to_string_model(rnames));
                        let rows: Vec<Vec<String>> = data
                            .iter()
                            .map(|d| {
                                vec![
                                    d.id.clone(),
                                    d.hostname.clone(),
                                    d.time_str.clone(),
                                    d.source.clone(),
                                    d.label.clone(),
                                    d.files.clone(),
                                    d.size.clone(),
                                ]
                            })
                            .collect();
                        if let Ok(mut sd) = snapshot_data.lock() {
                            *sd = data;
                        }
                        ui.set_snapshot_rows(to_table_model(rows));
                    }
                    UiEvent::SnapshotContentsData {
                        repo_name,
                        snapshot_name,
                        items,
                    } => {
                        controllers::restore::with_window(|rw| {
                            controllers::restore::handle_snapshot_contents(
                                rw,
                                &repo_name,
                                &snapshot_name,
                                items,
                            );
                        });
                    }
                    UiEvent::RestoreFinished { success, message } => {
                        controllers::restore::with_window(|rw| {
                            controllers::restore::handle_restore_finished(rw, success, message);
                        });
                    }
                    UiEvent::FindResultsData { rows } => {
                        controllers::find::with_window(|fw| {
                            controllers::find::handle_results(fw, rows);
                        });
                    }
                    UiEvent::ConfigText(text) => {
                        ui.set_editor_baseline(text.clone().into());
                        ui.set_editor_text(text.into());
                        ui.set_editor_dirty(false);
                        ui.set_editor_status(SharedString::default());
                    }
                    UiEvent::ConfigSaveError(message) => {
                        ui.set_editor_status(message.into());
                    }
                    UiEvent::OperationStarted { cancellable } => {
                        ui.set_operation_busy(true);
                        ui.set_operation_cancellable(cancellable);
                    }
                    UiEvent::OperationFinished => {
                        ui.set_operation_busy(false);
                    }
                    UiEvent::Quit => {
                        if let Some(s) = capture_gui_state(&ui) {
                            if let Ok(mut last) = last_gui_state.lock() {
                                *last = Some(s);
                            }
                        }
                        let _ = slint::quit_event_loop();
                    }
                    UiEvent::ShowWindow => {
                        let _ = ui.show();
                        #[cfg(target_os = "macos")]
                        {
                            use objc2::MainThreadMarker;
                            use objc2_app_kit::NSApplication;
                            if let Some(mtm) = MainThreadMarker::new() {
                                NSApplication::sharedApplication(mtm).activate();
                            }
                        }
                    }
                    UiEvent::TriggerSnapshotRefresh => {
                        let sel = ui.get_snapshots_repo_combo_value().to_string();
                        let _ = app_tx.send(AppCommand::RefreshSnapshots { repo_selector: sel });
                    }
                }
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col_text(
        model: &Rc<VecModel<ModelRc<StandardListViewItem>>>,
        row: usize,
        col: usize,
    ) -> String {
        let row_model = model.row_data(row).unwrap();
        row_model.row_data(col).unwrap().text.to_string()
    }

    #[test]
    fn row_has_three_columns() {
        let model = Rc::new(VecModel::<ModelRc<StandardListViewItem>>::default());
        prepend_log_entry(&model, "Mar 15", "10:30:00", "test event");

        assert_eq!(model.row_count(), 1);
        assert_eq!(col_text(&model, 0, 0), "Mar 15");
        assert_eq!(col_text(&model, 0, 1), "10:30:00");
        assert_eq!(col_text(&model, 0, 2), "test event");
    }

    #[test]
    fn newest_entry_is_at_top() {
        let model = Rc::new(VecModel::<ModelRc<StandardListViewItem>>::default());
        prepend_log_entry(&model, "Mar 14", "09:00:00", "older");
        prepend_log_entry(&model, "Mar 15", "10:00:00", "newer");

        assert_eq!(model.row_count(), 2);
        assert_eq!(col_text(&model, 0, 2), "newer");
        assert_eq!(col_text(&model, 1, 2), "older");
    }

    #[test]
    fn trim_keeps_newest_rows() {
        let model = Rc::new(VecModel::<ModelRc<StandardListViewItem>>::default());
        for i in 0..=MAX_LOG_ROWS {
            prepend_log_entry(&model, "D", "T", &format!("msg-{i}"));
        }

        assert_eq!(model.row_count(), TRIM_TARGET);
        // Row 0 should be the most recently inserted entry.
        assert_eq!(col_text(&model, 0, 2), format!("msg-{MAX_LOG_ROWS}"));
        // Last kept row is the (MAX_LOG_ROWS+1 - TRIM_TARGET)'th newest.
        assert_eq!(
            col_text(&model, TRIM_TARGET - 1, 2),
            format!("msg-{}", MAX_LOG_ROWS + 1 - TRIM_TARGET)
        );
    }
}
