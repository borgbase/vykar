use std::cell::RefCell;
use std::rc::Rc;

use slint::{ComponentHandle, Model, ModelRc, SharedString, StandardListViewItem, VecModel};

use crate::messages::{RepoInfoData, SnapshotRowData, SnapshotSelection, SourceInfoData};
use crate::state;
use crate::view_models::build_repo_source_model;
use crate::{AppData, MainWindow, RepoInfo, SourceInfo};

/// Glyph prefixed onto the ID cell when a snapshot row is selected.
const SELECTED_PREFIX: &str = "● ";

thread_local! {
    static UI_STATE: RefCell<Option<UiState>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SelectedSnapshot {
    pub repo_name: String,
    pub snapshot_id: String,
}

#[derive(Clone, Copy)]
pub(crate) enum SourceModelScope {
    All,
    Repo,
}

pub(crate) struct UiState {
    snapshot_data: Vec<SnapshotRowData>,
    snapshot_selection: SnapshotSelection,
    source_cache: Vec<SourceInfoData>,
    pending_repo_name: Option<String>,
    last_gui_state: Option<state::GuiState>,
    snapshot_rows: Rc<VecModel<ModelRc<StandardListViewItem>>>,
    recent_snapshot_rows: Rc<VecModel<ModelRc<StandardListViewItem>>>,
    repo_labels: Rc<VecModel<SharedString>>,
    source_labels: Rc<VecModel<SharedString>>,
    repo_model: Rc<VecModel<RepoInfo>>,
    source_model: Rc<VecModel<SourceInfo>>,
    repo_source_model: Rc<VecModel<SourceInfo>>,
}

impl UiState {
    fn new(pending_repo_name: Option<String>) -> Self {
        Self {
            snapshot_data: Vec::new(),
            snapshot_selection: SnapshotSelection::default(),
            source_cache: Vec::new(),
            pending_repo_name,
            last_gui_state: None,
            snapshot_rows: Rc::new(VecModel::default()),
            recent_snapshot_rows: Rc::new(VecModel::default()),
            repo_labels: Rc::new(VecModel::default()),
            source_labels: Rc::new(VecModel::default()),
            repo_model: Rc::new(VecModel::default()),
            source_model: Rc::new(VecModel::default()),
            repo_source_model: Rc::new(VecModel::default()),
        }
    }

    fn install_models(&self, ui: &MainWindow) {
        ui.set_snapshot_rows(ModelRc::from(self.snapshot_rows.clone()));
        ui.set_recent_snapshot_rows(ModelRc::from(self.recent_snapshot_rows.clone()));
        ui.set_repo_model(ModelRc::from(self.repo_model.clone()));
        ui.set_source_model(ModelRc::from(self.source_model.clone()));
        ui.set_repo_source_model(ModelRc::from(self.repo_source_model.clone()));
        ui.global::<AppData>()
            .set_repo_labels(ModelRc::from(self.repo_labels.clone()));
        ui.global::<AppData>()
            .set_source_labels(ModelRc::from(self.source_labels.clone()));
    }
}

pub(crate) fn install(ui: &MainWindow, pending_repo_name: Option<String>) {
    let state = UiState::new(pending_repo_name);
    state.install_models(ui);
    UI_STATE.with(|cell| {
        *cell.borrow_mut() = Some(state);
    });
}

fn with_state<R>(f: impl FnOnce(&UiState) -> R) -> R {
    UI_STATE.with(|cell| {
        let borrow = cell.borrow();
        let state = borrow
            .as_ref()
            .expect("vykar GUI UI state was not initialized");
        f(state)
    })
}

fn with_state_mut<R>(f: impl FnOnce(&mut UiState) -> R) -> R {
    UI_STATE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let state = borrow
            .as_mut()
            .expect("vykar GUI UI state was not initialized");
        f(state)
    })
}

pub(crate) fn pending_repo_name() -> Option<String> {
    with_state(|state| state.pending_repo_name.clone())
}

pub(crate) fn take_pending_repo_name() -> Option<String> {
    with_state_mut(|state| state.pending_repo_name.take())
}

pub(crate) fn set_last_gui_state(gui_state: state::GuiState) {
    with_state_mut(|state| {
        state.last_gui_state = Some(gui_state);
    });
}

pub(crate) fn last_gui_state() -> Option<state::GuiState> {
    with_state(|state| state.last_gui_state.clone())
}

pub(crate) fn repo_name_at(index: i32) -> Option<String> {
    if index < 0 {
        return None;
    }
    with_state(|state| {
        state
            .repo_labels
            .row_data(index as usize)
            .map(|label| label.to_string())
    })
}

pub(crate) fn current_repo_name(ui: &MainWindow) -> Option<String> {
    repo_name_at(ui.get_current_repo_index())
}

pub(crate) fn source_label_at(index: i32) -> Option<String> {
    if index < 0 {
        return None;
    }
    with_state(|state| {
        state
            .source_labels
            .row_data(index as usize)
            .map(|label| label.to_string())
    })
}

pub(crate) fn replace_repo_model(items: Vec<RepoInfoData>, labels: Vec<SharedString>) {
    with_state_mut(|state| {
        state.repo_labels.set_vec(labels);
        let repo_rows: Vec<RepoInfo> = items
            .into_iter()
            .map(|d| RepoInfo {
                name: d.name,
                url: d.url,
                snapshots: d.snapshots,
                last_snapshot: d.last_snapshot,
                size: d.size,
            })
            .collect();
        state.repo_model.set_vec(repo_rows);
    });
}

pub(crate) fn replace_source_model(
    items: Vec<SourceInfoData>,
    labels: Vec<SharedString>,
    current_repo: Option<&str>,
) {
    with_state_mut(|state| {
        state.source_labels.set_vec(labels);
        state.source_model.set_vec(build_source_model(&items));
        state
            .repo_source_model
            .set_vec(build_repo_source_model(&items, current_repo));
        state.source_cache = items;
    });
}

pub(crate) fn refresh_repo_source_model(current_repo: Option<&str>) {
    with_state(|state| {
        state
            .repo_source_model
            .set_vec(build_repo_source_model(&state.source_cache, current_repo));
    });
}

fn build_source_model(items: &[SourceInfoData]) -> Vec<SourceInfo> {
    items
        .iter()
        .map(|d| SourceInfo {
            label: d.label.clone(),
            paths: d.paths.clone(),
            excludes: d.excludes.clone(),
            target_repos: d.target_repos.clone(),
            expanded: false,
            detail_paths: d.detail_paths.clone(),
            detail_excludes: d.detail_excludes.clone(),
            detail_exclude_if_present: d.detail_exclude_if_present.clone(),
            detail_flags: d.detail_flags.clone(),
            detail_hooks: d.detail_hooks.clone(),
            detail_retention: d.detail_retention.clone(),
            detail_command_dumps: d.detail_command_dumps.clone(),
        })
        .collect()
}

pub(crate) fn toggle_source_expanded(scope: SourceModelScope, idx: i32) -> bool {
    with_state(|state| match scope {
        SourceModelScope::All => toggle_source_row_expanded(&state.source_model, idx),
        SourceModelScope::Repo => toggle_source_row_expanded(&state.repo_source_model, idx),
    })
}

pub(crate) fn toggle_source_row_expanded(model: &Rc<VecModel<SourceInfo>>, idx: i32) -> bool {
    if idx < 0 {
        return false;
    }
    let idx = idx as usize;
    let Some(mut item) = model.row_data(idx) else {
        return false;
    };
    item.expanded = !item.expanded;
    model.set_row_data(idx, item);
    true
}

pub(crate) fn replace_snapshot_data(ui: &MainWindow, mut data: Vec<SnapshotRowData>) {
    data.reverse();
    with_state_mut(|state| {
        state.snapshot_selection.reset(data.len());
        state.snapshot_data = data;
        publish_all_snapshot_rows(state);
        publish_recent_snapshot_rows(state);
        ui.set_snapshot_selected_count(state.snapshot_selection.count());
    });
}

pub(crate) fn sort_snapshots(ui: &MainWindow, col_idx: i32, ascending: bool) {
    with_state_mut(|state| {
        if !sort_snapshot_data(&mut state.snapshot_data, col_idx, ascending) {
            return;
        }
        state.snapshot_selection.reset(state.snapshot_data.len());
        publish_all_snapshot_rows(state);
        ui.set_snapshot_selected_count(state.snapshot_selection.count());
    });
}

pub(crate) fn click_snapshot_row(ui: &MainWindow, row: i32, toggle: bool, range: bool) {
    with_state_mut(|state| {
        let len = state.snapshot_data.len();
        let changed =
            apply_snapshot_selection(&mut state.snapshot_selection, len, row, toggle, range);
        for idx in changed {
            if let Some(data) = state.snapshot_data.get(idx) {
                let selected = state
                    .snapshot_selection
                    .selected
                    .get(idx)
                    .copied()
                    .unwrap_or(false);
                state
                    .snapshot_rows
                    .set_row_data(idx, snapshot_table_row(data, selected));
            }
        }
        ui.set_snapshot_selected_count(state.snapshot_selection.count());
    });
}

pub(crate) fn single_selected_snapshot() -> Option<SelectedSnapshot> {
    let snapshots = selected_snapshots(1);
    snapshots.and_then(|mut snapshots| snapshots.pop())
}

pub(crate) fn selected_snapshots(expected: usize) -> Option<Vec<SelectedSnapshot>> {
    with_state(|state| {
        let indices: Vec<usize> = state
            .snapshot_selection
            .selected
            .iter()
            .enumerate()
            .filter_map(|(idx, selected)| selected.then_some(idx))
            .collect();
        if indices.len() != expected {
            return None;
        }
        indices
            .into_iter()
            .map(|idx| {
                state.snapshot_data.get(idx).map(|row| SelectedSnapshot {
                    repo_name: row.repo_name.to_string(),
                    snapshot_id: row.id.to_string(),
                })
            })
            .collect()
    })
}

pub(crate) fn selected_snapshots_by_repo() -> std::collections::BTreeMap<String, Vec<String>> {
    with_state(|state| {
        let mut by_repo: std::collections::BTreeMap<String, Vec<String>> =
            std::collections::BTreeMap::new();
        for (idx, selected) in state.snapshot_selection.selected.iter().enumerate() {
            if !*selected {
                continue;
            }
            if let Some(row) = state.snapshot_data.get(idx) {
                by_repo
                    .entry(row.repo_name.to_string())
                    .or_default()
                    .push(row.id.to_string());
            }
        }
        by_repo
    })
}

fn publish_all_snapshot_rows(state: &UiState) {
    let rows: Vec<ModelRc<StandardListViewItem>> = state
        .snapshot_data
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let selected = state
                .snapshot_selection
                .selected
                .get(idx)
                .copied()
                .unwrap_or(false);
            snapshot_table_row(row, selected)
        })
        .collect();
    state.snapshot_rows.set_vec(rows);
}

fn publish_recent_snapshot_rows(state: &UiState) {
    let rows: Vec<ModelRc<StandardListViewItem>> = state
        .snapshot_data
        .iter()
        .take(3)
        .map(recent_snapshot_row)
        .collect();
    state.recent_snapshot_rows.set_vec(rows);
}

fn snapshot_table_row(row: &SnapshotRowData, selected: bool) -> ModelRc<StandardListViewItem> {
    let id = if selected {
        SharedString::from(format!("{SELECTED_PREFIX}{}", row.id))
    } else {
        row.id.clone()
    };
    standard_row([
        id,
        row.time_str.clone(),
        row.hostname.clone(),
        row.label.clone(),
        row.files.clone(),
        row.size.clone(),
    ])
}

fn recent_snapshot_row(row: &SnapshotRowData) -> ModelRc<StandardListViewItem> {
    standard_row([
        row.id.clone(),
        row.time_str.clone(),
        row.files.clone(),
        row.size.clone(),
    ])
}

fn standard_row<const N: usize>(cells: [SharedString; N]) -> ModelRc<StandardListViewItem> {
    let items: Vec<StandardListViewItem> =
        cells.into_iter().map(StandardListViewItem::from).collect();
    ModelRc::new(VecModel::from(items))
}

pub(crate) fn apply_snapshot_selection(
    selection: &mut SnapshotSelection,
    len: usize,
    row: i32,
    toggle: bool,
    range: bool,
) -> Vec<usize> {
    if row < 0 || (row as usize) >= len {
        return Vec::new();
    }
    let old_selected = if selection.selected.len() == len {
        selection.selected.clone()
    } else {
        selection.reset(len);
        vec![false; len]
    };

    let row = row as usize;
    if let (true, Some(anchor)) = (range, selection.anchor) {
        let (lo, hi) = if anchor <= row {
            (anchor, row)
        } else {
            (row, anchor)
        };
        for selected in selection.selected.iter_mut() {
            *selected = false;
        }
        if let Some(range_slice) = selection.selected.get_mut(lo..=hi) {
            for selected in range_slice {
                *selected = true;
            }
        }
    } else if toggle {
        if let Some(selected) = selection.selected.get_mut(row) {
            *selected = !*selected;
        }
        selection.anchor = Some(row);
    } else {
        for selected in selection.selected.iter_mut() {
            *selected = false;
        }
        if let Some(selected) = selection.selected.get_mut(row) {
            *selected = true;
        }
        selection.anchor = Some(row);
    }

    old_selected
        .into_iter()
        .zip(selection.selected.iter().copied())
        .enumerate()
        .filter_map(|(idx, (old, new))| (old != new).then_some(idx))
        .collect()
}

pub(crate) fn sort_snapshot_data(
    data: &mut [SnapshotRowData],
    col_idx: i32,
    ascending: bool,
) -> bool {
    // Columns: 0=ID, 1=Time, 2=Host, 3=Label, 4=Files, 5=Size
    match col_idx {
        0 => data.sort_by(|a, b| a.id.cmp(&b.id)),
        1 => data.sort_by_key(|row| row.time_epoch),
        2 => data.sort_by(|a, b| a.hostname.cmp(&b.hostname)),
        3 => data.sort_by(|a, b| a.label.cmp(&b.label)),
        4 => data.sort_by(|a, b| match (a.nfiles, b.nfiles) {
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }),
        5 => data.sort_by(|a, b| match (a.size_bytes, b.size_bytes) {
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }),
        _ => return false,
    }
    if !ascending {
        data.reverse();
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(
        id: &str,
        repo_name: &str,
        time_epoch: i64,
        nfiles: Option<u64>,
        size_bytes: Option<u64>,
    ) -> SnapshotRowData {
        SnapshotRowData {
            id: id.into(),
            hostname: format!("host-{id}").into(),
            time_str: format!("time-{time_epoch}").into(),
            label: format!("label-{id}").into(),
            files: nfiles
                .map(|n| n.to_string())
                .unwrap_or_else(|| "-".to_string())
                .into(),
            size: size_bytes
                .map(|n| n.to_string())
                .unwrap_or_else(|| "-".to_string())
                .into(),
            nfiles,
            size_bytes,
            time_epoch,
            repo_name: repo_name.into(),
        }
    }

    fn source_info(label: &str, expanded: bool) -> SourceInfo {
        SourceInfo {
            label: label.into(),
            paths: SharedString::default(),
            excludes: SharedString::default(),
            target_repos: SharedString::default(),
            expanded,
            detail_paths: SharedString::default(),
            detail_excludes: SharedString::default(),
            detail_exclude_if_present: SharedString::default(),
            detail_flags: SharedString::default(),
            detail_hooks: SharedString::default(),
            detail_retention: SharedString::default(),
            detail_command_dumps: SharedString::default(),
        }
    }

    #[test]
    fn selection_reports_only_changed_rows() {
        let mut selection = SnapshotSelection::default();
        selection.reset(4);

        assert_eq!(
            apply_snapshot_selection(&mut selection, 4, 1, false, false),
            vec![1]
        );
        assert_eq!(
            apply_snapshot_selection(&mut selection, 4, 3, true, false),
            vec![3]
        );
        assert_eq!(
            apply_snapshot_selection(&mut selection, 4, 2, false, true),
            vec![1, 2]
        );
        assert_eq!(selection.selected, vec![false, false, true, true]);
    }

    #[test]
    fn sorting_resets_selection_and_preserves_row_alignment() {
        let mut rows = vec![
            snapshot("b", "repo-b", 20, Some(2), Some(200)),
            snapshot("a", "repo-a", 10, Some(1), Some(100)),
        ];
        let mut selection = SnapshotSelection {
            selected: vec![true, false],
            anchor: Some(0),
        };

        assert!(sort_snapshot_data(&mut rows, 0, true));
        selection.reset(rows.len());

        assert_eq!(rows.len(), 2);
        let first = rows.first().expect("non-empty");
        let second = rows.get(1).expect("len == 2");
        assert_eq!(first.id, SharedString::from("a"));
        assert_eq!(first.repo_name, SharedString::from("repo-a"));
        assert_eq!(second.id, SharedString::from("b"));
        assert_eq!(second.repo_name, SharedString::from("repo-b"));
        assert_eq!(selection.selected, vec![false, false]);
        assert_eq!(selection.anchor, None);
    }

    #[test]
    fn source_expansion_toggles_one_row_in_place() {
        let model = Rc::new(VecModel::from(vec![
            source_info("one", false),
            source_info("two", false),
        ]));

        assert!(toggle_source_row_expanded(&model, 1));

        assert_eq!(model.row_count(), 2);
        assert!(!model.row_data(0).unwrap().expanded);
        assert!(model.row_data(1).unwrap().expanded);
    }
}
