use std::cell::RefCell;

use slint::{ComponentHandle, ModelRc, StandardListViewItem, VecModel};
use vykar_common::display::format_bytes;
use vykar_core::commands::diff::DiffChangeKind;

use crate::messages::DiffResultRow;
use crate::view_models::to_table_model;
use crate::DiffWindow;

thread_local! {
    static DIFF_HANDLE: RefCell<Option<DiffWindow>> = const { RefCell::new(None) };
    static DIFF_ROWS: RefCell<Vec<DiffResultRow>> = const { RefCell::new(Vec::new()) };
}

pub(crate) struct DiffResultsView {
    pub repo_name: String,
    pub snapshot_a: String,
    pub snapshot_b: String,
    pub base_snapshot: String,
    pub target_snapshot: String,
    pub rows: Vec<DiffResultRow>,
    pub error: Option<String>,
}

pub(crate) fn ensure_window() -> Option<DiffWindow> {
    DIFF_HANDLE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if let Some(ref dw) = *borrow {
            return Some(dw.clone_strong());
        }
        let dw = DiffWindow::new().ok()?;
        wire_callbacks(&dw);
        let handle = dw.clone_strong();
        *borrow = Some(dw);
        Some(handle)
    })
}

pub(crate) fn with_window(f: impl FnOnce(&DiffWindow)) {
    DIFF_HANDLE.with(|cell| {
        if let Some(ref dw) = *cell.borrow() {
            f(dw);
        }
    });
}

pub(crate) fn prepare_loading(
    dw: &DiffWindow,
    repo_name: &str,
    snapshot_a: &str,
    snapshot_b: &str,
) {
    dw.set_repo_name(repo_name.into());
    dw.set_snapshot_a(snapshot_a.into());
    dw.set_snapshot_b(snapshot_b.into());
    dw.set_base_snapshot("".into());
    dw.set_target_snapshot("".into());
    dw.set_status_text("Loading diff...".into());
    dw.set_diff_rows(ModelRc::new(
        VecModel::<ModelRc<StandardListViewItem>>::default(),
    ));
    DIFF_ROWS.with(|cell| cell.borrow_mut().clear());
}

pub(crate) fn handle_diff_results(dw: &DiffWindow, result: DiffResultsView) {
    if dw.get_repo_name() != result.repo_name
        || dw.get_snapshot_a() != result.snapshot_a
        || dw.get_snapshot_b() != result.snapshot_b
    {
        return;
    }

    dw.set_base_snapshot(result.base_snapshot.into());
    dw.set_target_snapshot(result.target_snapshot.into());

    if let Some(error) = result.error {
        dw.set_diff_rows(ModelRc::new(
            VecModel::<ModelRc<StandardListViewItem>>::default(),
        ));
        dw.set_status_text(format!("Diff failed: {error}").into());
        DIFF_ROWS.with(|cell| cell.borrow_mut().clear());
        return;
    }

    let count = result.rows.len();
    set_rows(dw, result.rows);
    if count == 0 {
        dw.set_status_text("No file changes found.".into());
    } else {
        dw.set_status_text(format!("{count} file changes.").into());
    }
}

fn change_label(change: DiffChangeKind) -> &'static str {
    match change {
        DiffChangeKind::Added => "Added",
        DiffChangeKind::Removed => "Removed",
        DiffChangeKind::Modified => "Modified",
    }
}

fn format_optional_size(size: Option<u64>) -> String {
    size.map(format_bytes).unwrap_or_else(|| "-".to_string())
}

fn format_size_delta(delta: i64) -> String {
    match delta.cmp(&0) {
        std::cmp::Ordering::Less => format!("-{}", format_bytes(delta.unsigned_abs())),
        std::cmp::Ordering::Equal => format_bytes(0),
        std::cmp::Ordering::Greater => format!("+{}", format_bytes(delta as u64)),
    }
}

fn row_cells(row: &DiffResultRow) -> Vec<String> {
    vec![
        change_label(row.change).to_string(),
        row.path.clone(),
        format_optional_size(row.old_size_bytes),
        format_optional_size(row.new_size_bytes),
        format_size_delta(row.delta_bytes),
    ]
}

fn publish_rows(dw: &DiffWindow, rows: &[DiffResultRow]) {
    let table_rows = rows.iter().map(row_cells).collect();
    dw.set_diff_rows(to_table_model(table_rows));
}

fn set_rows(dw: &DiffWindow, rows: Vec<DiffResultRow>) {
    publish_rows(dw, &rows);
    DIFF_ROWS.with(|cell| {
        *cell.borrow_mut() = rows;
    });
}

fn sort_rows(dw: &DiffWindow, col_idx: i32, ascending: bool) {
    DIFF_ROWS.with(|cell| {
        let mut rows = cell.borrow_mut();
        match col_idx {
            0 => rows.sort_by_key(|row| row.change as u8),
            1 => rows.sort_by(|a, b| a.path.cmp(&b.path)),
            2 => rows.sort_by_key(|row| row.old_size_bytes),
            3 => rows.sort_by_key(|row| row.new_size_bytes),
            4 => rows.sort_by_key(|row| row.delta_bytes),
            _ => return,
        }
        if !ascending {
            rows.reverse();
        }
        publish_rows(dw, &rows);
    });
}

fn wire_callbacks(diff_win: &DiffWindow) {
    {
        let dw_weak = diff_win.as_weak();
        diff_win.on_close_clicked(move || {
            if let Some(dw) = dw_weak.upgrade() {
                let _ = dw.hide();
            }
            DIFF_ROWS.with(|cell| cell.borrow_mut().clear());
        });
    }

    {
        let dw_weak = diff_win.as_weak();
        diff_win.on_sort_ascending(move |col_idx| {
            if let Some(dw) = dw_weak.upgrade() {
                sort_rows(&dw, col_idx, true);
            }
        });
    }

    {
        let dw_weak = diff_win.as_weak();
        diff_win.on_sort_descending(move |col_idx| {
            if let Some(dw) = dw_weak.upgrade() {
                sort_rows(&dw, col_idx, false);
            }
        });
    }

    diff_win.window().on_close_requested(|| {
        DIFF_ROWS.with(|cell| cell.borrow_mut().clear());
        slint::CloseRequestResponse::HideWindow
    });
}
