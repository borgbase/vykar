use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_channel::Receiver;
use slint::{ComponentHandle, Model, ModelRc, SharedString, StandardListViewItem, VecModel};

use crate::controllers;
use crate::messages::{AppCommand, UiEvent};
use crate::state;
use crate::tray_state;
use crate::ui_state;
use crate::view_models::to_find_groups_model;
use crate::{AppData, MainWindow};

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
        borrow.as_ref().expect("log model initialized").clone()
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
            .map(|i| model.row_data(i).expect("row index below trim target"))
            .collect();
        model.set_vec(keep);
    }
}

fn append_log_row(ui: &MainWindow, date: &str, timestamp: &str, message: &str) {
    let model = ensure_log_model(ui);
    prepend_log_entry(&model, date, timestamp, message);
}

/// Append a log row stamped with the current local time. Centralizes the
/// date/time formatting for UI-thread log rows (shares `messages::log_timestamp_now`
/// with the worker's `log_entry_now`).
fn append_log_now(ui: &MainWindow, message: &str) {
    let (date, timestamp) = crate::messages::log_timestamp_now();
    append_log_row(ui, &date, &timestamp, message);
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
    labels: &[impl AsRef<str>],
    prev_idx: i32,
    pending_name: Option<&str>,
) -> i32 {
    if labels.is_empty() {
        return 0;
    }
    if let Some(name) = pending_name {
        return labels
            .iter()
            .position(|label| label.as_ref() == name)
            .map(|p| p as i32)
            .unwrap_or(0);
    }
    clamp_repo_index(prev_idx, labels.len())
}

pub(crate) fn capture_gui_state(
    ui: &MainWindow,
    start_in_background_pref: &AtomicBool,
) -> Option<state::GuiState> {
    let win_size = ui.window().size();
    let scale = ui.window().scale_factor();
    if win_size.width == 0 || win_size.height == 0 {
        return None;
    }
    if !scale.is_finite() || scale <= 0.0 {
        return None;
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "window dimensions are small u32 values; f32 mantissa is sufficient"
    )]
    let w = win_size.width as f32 / scale;
    #[allow(
        clippy::cast_precision_loss,
        reason = "window dimensions are small u32 values; f32 mantissa is sufficient"
    )]
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
    let last_repo_name = ui_state::current_repo_name(ui).or_else(ui_state::pending_repo_name);
    Some(state::GuiState {
        config_path,
        window_width: Some(w),
        window_height: Some(h),
        start_in_background: Some(start_in_background_pref.load(Ordering::Relaxed)),
        last_page: Some(state::page_to_i32(ui.get_current_page())),
        last_repo_name,
    })
}

/// Borrowed context handed to the per-domain event handlers. Groups the
/// long-lived channels/handles so handlers stay 2-3 args instead of 6.
struct EventCtx<'a> {
    ui: &'a MainWindow,
    app_tx: &'a crossbeam_channel::Sender<AppCommand>,
    tray_source_tx: &'a crossbeam_channel::Sender<Vec<(tray_icon::menu::MenuId, String)>>,
    prefs: &'a AtomicBool,
}

fn apply_config_info(ctx: &EventCtx, path: String, schedule_brief: String) {
    ctx.ui
        .global::<AppData>()
        .set_active_config_path(path.clone().into());
    ctx.ui.set_config_path(path.into());
    ctx.ui.set_schedule_brief(schedule_brief.into());
    // Eagerly persist so Cmd-Q keeps the config path.
    if let Some(s) = capture_gui_state(ctx.ui, ctx.prefs) {
        state::save(&s);
        ui_state::set_last_gui_state(s);
    }
}

fn apply_repo_names(ctx: &EventCtx, names: Vec<SharedString>) {
    // RepoNames is derived from the full configured repo list and arrives
    // before RepoModelData (which is filtered to repos that loaded
    // successfully). Clamp defensively so the index is never out-of-range
    // against `names`; the authoritative resolution (by repo name) happens
    // when RepoModelData arrives.
    let clamped = clamp_repo_index(ctx.ui.get_current_repo_index(), names.len());
    if clamped != ctx.ui.get_current_repo_index() {
        ctx.ui.set_current_repo_index(clamped);
    }
}

fn apply_repo_model(
    ctx: &EventCtx,
    items: Vec<crate::messages::RepoInfoData>,
    labels: Vec<SharedString>,
) {
    ctx.ui.set_repo_loading(false);
    // Resolve the selected repo by name. On first load this consumes the
    // persisted last_repo_name; afterward we just clamp the current index
    // against the filtered labels.
    let pending = ui_state::take_pending_repo_name();
    let prev_idx = ctx.ui.get_current_repo_index();
    let new_idx = resolve_repo_index(&labels, prev_idx, pending.as_deref());
    ui_state::replace_repo_model(items, labels);
    if new_idx != prev_idx {
        ctx.ui.set_current_repo_index(new_idx);
    }

    // Rebuild the per-repo source model now that the current repo's name is
    // resolvable.
    let current_repo = ui_state::current_repo_name(ctx.ui);
    ui_state::refresh_repo_source_model(current_repo.as_deref());

    // Trigger a snapshot refresh for the resolved repo.
    if let Some(name) = current_repo {
        let _ = ctx.app_tx.send(AppCommand::RefreshSnapshots {
            repo_selector: name,
        });
    }
}

fn apply_source_model(
    ctx: &EventCtx,
    source_items: Vec<crate::messages::SourceInfoData>,
    labels: Vec<SharedString>,
) {
    // On Linux the tray submenu lives on the GTK thread; dispatch via
    // idle_add_once to run there.
    #[cfg(target_os = "linux")]
    {
        let tray_labels = labels.clone();
        let tray_source_tx = ctx.tray_source_tx.clone();
        gtk::glib::idle_add_once(move || {
            let items = tray_state::rebuild_submenu(&tray_labels);
            let _ = tray_source_tx.send(items);
        });
    }
    #[cfg(not(target_os = "linux"))]
    {
        let items = tray_state::rebuild_submenu(&labels);
        let _ = ctx.tray_source_tx.send(items);
    }

    let current_repo = ui_state::current_repo_name(ctx.ui);
    ui_state::replace_source_model(source_items, labels, current_repo.as_deref());
}

fn apply_find_results(ctx: &EventCtx, groups: Vec<crate::messages::FindSnapshotGroup>) {
    let total: usize = groups.iter().map(|g| g.rows.len()).sum();
    let snap_count = groups.len();
    ctx.ui.set_find_groups(to_find_groups_model(groups));
    ctx.ui.set_find_has_searched(true);
    ctx.ui
        .set_find_status_text(format!("{total} results across {snap_count} snapshots.").into());
}

fn handle_quit(ctx: &EventCtx) {
    if let Some(s) = capture_gui_state(ctx.ui, ctx.prefs) {
        ui_state::set_last_gui_state(s);
    }
    // Best-effort: stop any active mount so the listener is released cleanly.
    let _ = ctx.app_tx.send(AppCommand::StopMount);
    let _ = slint::quit_event_loop();
}

fn show_main_window(ctx: &EventCtx) {
    let _ = ctx.ui.show();
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

fn handle_mount_started(ctx: &EventCtx, url: String) {
    ctx.ui.set_is_mount_active(true);
    ctx.ui.set_mount_url(url.clone().into());
    if opener::open_browser(&url).is_err() {
        append_log_now(
            ctx.ui,
            &format!("Mount running at {url} — open it manually"),
        );
    }
}

fn handle_mount_failed(ctx: &EventCtx, message: String) {
    ctx.ui.set_is_mount_active(false);
    ctx.ui.set_mount_url("".into());
    append_log_now(ctx.ui, &format!("Mount failed: {message}"));
}

fn handle_update_available(ctx: &EventCtx, version: String, url: String) {
    ctx.ui.set_update_available(true);
    ctx.ui.set_update_url(url.into());
    append_log_now(ctx.ui, &format!("Update available: v{version}"));
}

fn trigger_snapshot_refresh(ctx: &EventCtx) {
    if let Some(name) = ui_state::current_repo_name(ctx.ui) {
        let _ = ctx.app_tx.send(AppCommand::RefreshSnapshots {
            repo_selector: name,
        });
    }
}

pub(crate) fn spawn(
    ui_rx: Receiver<UiEvent>,
    ui_weak: slint::Weak<MainWindow>,
    app_tx: crossbeam_channel::Sender<AppCommand>,
    tray_source_tx: crossbeam_channel::Sender<Vec<(tray_icon::menu::MenuId, String)>>,
    start_in_background_pref: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        while let Ok(event) = ui_rx.recv() {
            let ui_weak = ui_weak.clone();
            let app_tx = app_tx.clone();
            let tray_source_tx = tray_source_tx.clone();
            let start_in_background_pref = start_in_background_pref.clone();

            let _ = slint::invoke_from_event_loop(move || {
                let Some(ui) = ui_weak.upgrade() else {
                    return;
                };
                let ctx = EventCtx {
                    ui: &ui,
                    app_tx: &app_tx,
                    tray_source_tx: &tray_source_tx,
                    prefs: &start_in_background_pref,
                };

                match event {
                    UiEvent::Status(status) => {
                        // A fresh status clears any lingering error state.
                        ui.set_status_error(false);
                        ui.set_status_text(status.into());
                    }
                    UiEvent::ErrorStatus(message) => {
                        ui.set_status_error(true);
                        ui.set_status_text(message.into());
                    }
                    UiEvent::LogEntry {
                        date,
                        timestamp,
                        message,
                    } => append_log_row(&ui, &date, &timestamp, &message),
                    UiEvent::ConfigInfo {
                        path,
                        schedule_brief,
                    } => apply_config_info(&ctx, path, schedule_brief),
                    UiEvent::RepoNames(names) => apply_repo_names(&ctx, names),
                    UiEvent::RepoModelData { items, labels } => {
                        apply_repo_model(&ctx, items, labels)
                    }
                    UiEvent::SourceModelData { items, labels } => {
                        apply_source_model(&ctx, items, labels)
                    }
                    UiEvent::SnapshotTableData { data } => {
                        // Reverse once to newest-first; both the Snapshots table
                        // (default order) and the Overview "latest 3" consume this
                        // same canonical order. Column-header clicks on the
                        // Snapshots table reorder this list in place afterwards.
                        ui_state::replace_snapshot_data(&ui, data);
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
                    UiEvent::DiffResultsData {
                        repo_name,
                        snapshot_a,
                        snapshot_b,
                        base_snapshot,
                        target_snapshot,
                        rows,
                        error,
                    } => {
                        controllers::diff::with_window(|dw| {
                            controllers::diff::handle_diff_results(
                                dw,
                                controllers::diff::DiffResultsView {
                                    repo_name,
                                    snapshot_a,
                                    snapshot_b,
                                    base_snapshot,
                                    target_snapshot,
                                    rows,
                                    error,
                                },
                            );
                        });
                    }
                    UiEvent::FindResultsData { groups } => apply_find_results(&ctx, groups),
                    UiEvent::ConfigText(text) => {
                        ui.set_editor_baseline(text.clone().into());
                        ui.set_editor_text(text.into());
                        ui.set_editor_dirty(false);
                        ui.set_editor_status(SharedString::default());
                    }
                    UiEvent::ConfigSaveError(message) => ui.set_editor_status(message.into()),
                    UiEvent::OperationStarted => {
                        // Starting a new operation clears the previous error state.
                        ui.set_status_error(false);
                        ui.set_operation_busy(true);
                    }
                    UiEvent::OperationFinished => ui.set_operation_busy(false),
                    UiEvent::Quit => handle_quit(&ctx),
                    UiEvent::ShowWindow => show_main_window(&ctx),
                    UiEvent::MountStarted { url } => handle_mount_started(&ctx, url),
                    UiEvent::MountStopped => {
                        ui.set_is_mount_active(false);
                        ui.set_mount_url("".into());
                    }
                    UiEvent::MountFailed { message } => handle_mount_failed(&ctx, message),
                    UiEvent::UpdateAvailable { version, url } => {
                        handle_update_available(&ctx, version, url)
                    }
                    UiEvent::TriggerSnapshotRefresh => trigger_snapshot_refresh(&ctx),
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

    fn labels(names: &[&str]) -> Vec<SharedString> {
        names.iter().map(|s| (*s).into()).collect()
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
        // Failed-to-load repos now stay in the model (with an error card), so a
        // pending name only misses when the repo was truly removed from config.
        // In that case we fall back to the first repo.
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
        let empty: Vec<SharedString> = Vec::new();
        assert_eq!(resolve_repo_index(&empty, 3, Some("anything")), 0);
        assert_eq!(resolve_repo_index(&empty, 0, None), 0);
    }
}
