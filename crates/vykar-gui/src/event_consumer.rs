use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_channel::Receiver;
use slint::{ComponentHandle, Model, ModelRc, SharedString, StandardListViewItem, VecModel};
use tray_icon::menu::MenuId;

use crate::controllers;
use crate::messages::{AppCommand, SnapshotRowData, SourceInfoData, UiEvent};
use crate::state;
use crate::tray_state;
use crate::view_models::{
    build_repo_source_model, current_repo_name, to_string_model, to_table_model,
};
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

/// Clamp `idx` against `len`. Returns `0` for negative or out-of-range indices,
/// `idx` otherwise. `len == 0` always yields `0`.
pub(crate) fn clamp_repo_index(idx: i32, len: usize) -> i32 {
    if len == 0 || idx < 0 || (idx as usize) >= len {
        0
    } else {
        idx
    }
}

/// Resolve the selected repo index against the repo labels that actually
/// loaded. If `pending_name` is set, look it up in `labels` (0 on miss);
/// otherwise clamp `prev_idx` into range.
pub(crate) fn resolve_repo_index(
    labels: &[String],
    prev_idx: i32,
    pending_name: Option<&str>,
) -> i32 {
    if labels.is_empty() {
        return 0;
    }
    if let Some(name) = pending_name {
        return labels
            .iter()
            .position(|l| l == name)
            .map(|p| p as i32)
            .unwrap_or(0);
    }
    clamp_repo_index(prev_idx, labels.len())
}

pub(crate) fn capture_gui_state(
    ui: &MainWindow,
    start_in_background_pref: &AtomicBool,
    pending_repo_name: &Mutex<Option<String>>,
) -> Option<state::GuiState> {
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
    // Persist the selected repo by name, not index — survives config reordering
    // and filtered (failed-to-load) repos. Falls back to any still-pending name
    // if the model hasn't been populated yet.
    let last_repo_name = current_repo_name(ui).or_else(|| {
        pending_repo_name
            .lock()
            .ok()
            .and_then(|g| g.as_ref().cloned())
    });
    Some(state::GuiState {
        config_path,
        window_width: Some(w),
        window_height: Some(h),
        start_in_background: Some(start_in_background_pref.load(Ordering::Relaxed)),
        last_page: Some(state::page_to_i32(ui.get_current_page())),
        last_repo_name,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn(
    ui_rx: Receiver<UiEvent>,
    ui_weak: slint::Weak<MainWindow>,
    app_tx: crossbeam_channel::Sender<AppCommand>,
    snapshot_data: Arc<Mutex<Vec<SnapshotRowData>>>,
    source_cache: Arc<Mutex<Vec<SourceInfoData>>>,
    last_gui_state: Arc<Mutex<Option<state::GuiState>>>,
    tray_source_items: Arc<Mutex<Vec<(MenuId, String)>>>,
    start_in_background_pref: Arc<AtomicBool>,
    pending_repo_name: Arc<Mutex<Option<String>>>,
) {
    std::thread::spawn(move || {
        while let Ok(event) = ui_rx.recv() {
            let ui_weak = ui_weak.clone();
            let snapshot_data = snapshot_data.clone();
            let source_cache = source_cache.clone();
            let app_tx = app_tx.clone();
            let last_gui_state = last_gui_state.clone();
            let tray_source_items = tray_source_items.clone();
            let start_in_background_pref = start_in_background_pref.clone();
            let pending_repo_name = pending_repo_name.clone();

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
                    UiEvent::ConfigInfo {
                        path,
                        schedule_brief,
                    } => {
                        ui.global::<AppData>()
                            .set_active_config_path(path.clone().into());
                        ui.set_config_path(path.into());
                        ui.set_schedule_brief(schedule_brief.into());
                        // Eagerly persist so Cmd-Q keeps the config path.
                        if let Some(s) =
                            capture_gui_state(&ui, &start_in_background_pref, &pending_repo_name)
                        {
                            state::save(&s);
                            if let Ok(mut last) = last_gui_state.lock() {
                                *last = Some(s);
                            }
                        }
                    }
                    UiEvent::RepoNames(names) => {
                        // RepoNames is derived from the full configured repo list and
                        // arrives before RepoModelData (which is filtered to repos that
                        // loaded successfully). Clamp defensively so the index is never
                        // out-of-range against `names`; the authoritative resolution
                        // (by repo name) happens when RepoModelData arrives.
                        let clamped = clamp_repo_index(ui.get_current_repo_index(), names.len());
                        if clamped != ui.get_current_repo_index() {
                            ui.set_current_repo_index(clamped);
                        }
                    }
                    UiEvent::RepoModelData { items, labels } => {
                        ui.set_repo_loading(false);
                        ui.global::<AppData>()
                            .set_repo_labels(to_string_model(labels.clone()));

                        // Resolve the selected repo by name. On first load this consumes
                        // the persisted last_repo_name; afterward we just clamp the
                        // current index against the filtered labels.
                        let pending = pending_repo_name.lock().ok().and_then(|mut g| g.take());
                        let prev_idx = ui.get_current_repo_index();
                        let new_idx = resolve_repo_index(&labels, prev_idx, pending.as_deref());
                        if new_idx != prev_idx {
                            ui.set_current_repo_index(new_idx);
                        }

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

                        // Rebuild the per-repo source model now that the
                        // current repo's name is resolvable.
                        let current_repo = current_repo_name(&ui);
                        if let Ok(cache) = source_cache.lock() {
                            let repo_model =
                                build_repo_source_model(&cache, current_repo.as_deref());
                            ui.set_repo_source_model(ModelRc::new(VecModel::from(repo_model)));
                        }

                        // Trigger a snapshot refresh for the resolved repo.
                        if let Some(name) = current_repo {
                            let _ = app_tx.send(AppCommand::RefreshSnapshots {
                                repo_selector: name,
                            });
                        }
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

                        let current_repo = current_repo_name(&ui);
                        let repo_model = build_repo_source_model(&items, current_repo.as_deref());

                        let model: Vec<SourceInfo> = items
                            .iter()
                            .map(|d| SourceInfo {
                                label: d.label.clone().into(),
                                paths: d.paths.clone().into(),
                                excludes: d.excludes.clone().into(),
                                target_repos: d.target_repos.clone().into(),
                                expanded: false,
                                detail_paths: d.detail_paths.clone().into(),
                                detail_excludes: d.detail_excludes.clone().into(),
                                detail_exclude_if_present: d
                                    .detail_exclude_if_present
                                    .clone()
                                    .into(),
                                detail_flags: d.detail_flags.clone().into(),
                                detail_hooks: d.detail_hooks.clone().into(),
                                detail_retention: d.detail_retention.clone().into(),
                                detail_command_dumps: d.detail_command_dumps.clone().into(),
                            })
                            .collect();
                        ui.set_source_model(ModelRc::new(VecModel::from(model)));
                        ui.set_repo_source_model(ModelRc::new(VecModel::from(repo_model)));

                        if let Ok(mut cache) = source_cache.lock() {
                            *cache = items;
                        }
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
                                    d.label.clone(),
                                    d.files.clone(),
                                    d.size.clone(),
                                ]
                            })
                            .collect();
                        // Overview table: latest 3, columns ID / Time / Files / Size.
                        // Incoming data is sorted ascending by time, so take from the end
                        // to show newest first.
                        let recent_rows: Vec<Vec<String>> = data
                            .iter()
                            .rev()
                            .take(3)
                            .map(|d| {
                                vec![
                                    d.id.clone(),
                                    d.time_str.clone(),
                                    d.files.clone(),
                                    d.size.clone(),
                                ]
                            })
                            .collect();
                        if let Ok(mut sd) = snapshot_data.lock() {
                            *sd = data;
                        }
                        ui.set_snapshot_rows(to_table_model(rows));
                        ui.set_recent_snapshot_rows(to_table_model(recent_rows));
                    }
                    UiEvent::SnapshotContentsData {
                        repo_name,
                        snapshot_name,
                        items,
                        source_paths,
                    } => {
                        controllers::restore::with_window(|rw| {
                            controllers::restore::handle_snapshot_contents(
                                rw,
                                &repo_name,
                                &snapshot_name,
                                items,
                                &source_paths,
                            );
                        });
                    }
                    UiEvent::RestoreFinished { success, message } => {
                        controllers::restore::with_window(|rw| {
                            controllers::restore::handle_restore_finished(rw, success, message);
                        });
                    }
                    UiEvent::FindResultsData { rows } => {
                        let count = rows.len();
                        let table_rows: Vec<Vec<String>> = rows
                            .into_iter()
                            .map(|r| vec![r.snapshot, r.path, r.date, r.size, r.status])
                            .collect();
                        ui.set_find_result_rows(to_table_model(table_rows));
                        ui.set_find_status_text(format!("{count} results found.").into());
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
                    UiEvent::OperationStarted => {
                        ui.set_operation_busy(true);
                    }
                    UiEvent::OperationFinished => {
                        ui.set_operation_busy(false);
                    }
                    UiEvent::Quit => {
                        if let Some(s) =
                            capture_gui_state(&ui, &start_in_background_pref, &pending_repo_name)
                        {
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
                                let app = NSApplication::sharedApplication(mtm);
                                app.unhide(None);
                                for window in app.windows().iter() {
                                    if window.isMiniaturized() {
                                        window.deminiaturize(None);
                                    }
                                    window.makeKeyAndOrderFront(None);
                                }
                                app.activate();
                            }
                        }
                    }
                    UiEvent::TriggerSnapshotRefresh => {
                        let idx = ui.get_current_repo_index();
                        if idx >= 0 {
                            let labels = ui.global::<AppData>().get_repo_labels();
                            if let Some(name) = labels.row_data(idx as usize) {
                                let _ = app_tx.send(AppCommand::RefreshSnapshots {
                                    repo_selector: name.to_string(),
                                });
                            }
                        }
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

    fn labels(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn clamp_empty_returns_zero() {
        assert_eq!(clamp_repo_index(0, 0), 0);
        assert_eq!(clamp_repo_index(5, 0), 0);
        assert_eq!(clamp_repo_index(-1, 0), 0);
    }

    #[test]
    fn clamp_negative_and_out_of_range_fall_back_to_zero() {
        assert_eq!(clamp_repo_index(-1, 3), 0);
        assert_eq!(clamp_repo_index(3, 3), 0);
        assert_eq!(clamp_repo_index(100, 3), 0);
    }

    #[test]
    fn clamp_in_range_is_unchanged() {
        assert_eq!(clamp_repo_index(0, 3), 0);
        assert_eq!(clamp_repo_index(2, 3), 2);
    }

    #[test]
    fn resolve_uses_pending_name_when_present() {
        let ls = labels(&["alpha", "beta", "gamma"]);
        // Even if prev_idx would be valid, the pending name wins.
        assert_eq!(resolve_repo_index(&ls, 0, Some("gamma")), 2);
        assert_eq!(resolve_repo_index(&ls, 2, Some("alpha")), 0);
    }

    #[test]
    fn resolve_pending_name_missing_falls_back_to_zero() {
        // This is the scenario B2 guards against: persisted repo got filtered out.
        let ls = labels(&["alpha", "beta"]);
        assert_eq!(resolve_repo_index(&ls, 1, Some("removed-repo")), 0);
    }

    #[test]
    fn resolve_without_pending_clamps_prev_idx() {
        let ls = labels(&["alpha", "beta"]);
        // Out of range → 0, in range preserved.
        assert_eq!(resolve_repo_index(&ls, 5, None), 0);
        assert_eq!(resolve_repo_index(&ls, -1, None), 0);
        assert_eq!(resolve_repo_index(&ls, 1, None), 1);
    }

    #[test]
    fn resolve_empty_labels_yields_zero() {
        assert_eq!(resolve_repo_index(&[], 3, Some("anything")), 0);
        assert_eq!(resolve_repo_index(&[], 0, None), 0);
    }
}
