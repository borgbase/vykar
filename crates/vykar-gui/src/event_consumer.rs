use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crossbeam_channel::{Receiver, Sender};
use slint::{ComponentHandle, Model, ModelRc, SharedString, StandardListViewItem, VecModel};

use crate::controllers;
use crate::messages::{AppCommand, SnapshotRowData, UiEvent};
use crate::state;
use crate::view_models::{to_string_model, to_table_model};
use crate::{AppData, FindWindow, MainWindow, RepoInfo, RestoreWindow, SourceInfo};

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

const MAX_LOG_ROWS: usize = 10_000;
const TRIM_TARGET: usize = 9_000;

pub(crate) fn append_log_row(ui: &MainWindow, timestamp: &str, message: &str) {
    let model = ensure_log_model(ui);
    let row: Vec<StandardListViewItem> = vec![
        StandardListViewItem::from(SharedString::from(timestamp)),
        StandardListViewItem::from(SharedString::from(message)),
    ];
    model.push(ModelRc::new(VecModel::from(row)));
    if model.row_count() > MAX_LOG_ROWS {
        // Rebuild from the newest TRIM_TARGET rows in one shot to avoid
        // O(n)-per-row front-removal and repeated model-change notifications.
        let start = model.row_count() - TRIM_TARGET;
        let keep: Vec<_> = (start..model.row_count())
            .map(|i| model.row_data(i).unwrap())
            .collect();
        let fresh = Rc::new(VecModel::from(keep));
        ui.set_log_rows(ModelRc::from(fresh.clone()));
        LOG_MODEL.with(|cell| *cell.borrow_mut() = Some(fresh));
    }
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn(
    ui_rx: Receiver<UiEvent>,
    ui_weak: slint::Weak<MainWindow>,
    restore_weak: slint::Weak<RestoreWindow>,
    find_weak: slint::Weak<FindWindow>,
    app_tx: Sender<AppCommand>,
    snapshot_data: Arc<Mutex<Vec<SnapshotRowData>>>,
    last_gui_state: Arc<Mutex<Option<state::GuiState>>>,
    submenu_labels_tx: Sender<Vec<String>>,
) {
    std::thread::spawn(move || {
        while let Ok(event) = ui_rx.recv() {
            let ui_weak = ui_weak.clone();
            let restore_weak = restore_weak.clone();
            let find_weak = find_weak.clone();
            let snapshot_data = snapshot_data.clone();
            let app_tx = app_tx.clone();
            let last_gui_state = last_gui_state.clone();
            let submenu_labels_tx = submenu_labels_tx.clone();

            let _ = slint::invoke_from_event_loop(move || {
                let Some(ui) = ui_weak.upgrade() else {
                    return;
                };

                match event {
                    UiEvent::Status(status) => ui.set_status_text(status.into()),
                    UiEvent::LogEntry { timestamp, message } => {
                        append_log_row(&ui, &timestamp, &message);
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
                        // Signal the main thread to rebuild the tray submenu
                        let _ = submenu_labels_tx.send(labels.clone());

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
                    UiEvent::SnapshotContentsData { items } => {
                        if let Some(rw) = restore_weak.upgrade() {
                            controllers::restore::handle_snapshot_contents(&rw, items);
                        }
                    }
                    UiEvent::RestoreFinished { success, message } => {
                        if let Some(rw) = restore_weak.upgrade() {
                            controllers::restore::handle_restore_finished(&rw, success, message);
                        }
                    }
                    UiEvent::FindResultsData { rows } => {
                        if let Some(fw) = find_weak.upgrade() {
                            controllers::find::handle_results(&fw, rows);
                        }
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
