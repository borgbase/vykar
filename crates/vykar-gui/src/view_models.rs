use std::sync::{Arc, Mutex};

use crossbeam_channel::Sender;
use slint::{ComponentHandle, Model, ModelRc, SharedString, StandardListViewItem, VecModel};
use vykar_core::config::ResolvedRepo;

use crate::messages::{
    FindSnapshotGroup, SnapshotRowData, SnapshotSelection, SourceInfoData, UiEvent,
};
use crate::repo_helpers::format_repo_name;
use crate::{AppData, FindSnapshotGroup as UiFindSnapshotGroup, MainWindow, SourceInfo};

pub(crate) fn to_table_model(rows: Vec<Vec<String>>) -> ModelRc<ModelRc<StandardListViewItem>> {
    let outer: Vec<ModelRc<StandardListViewItem>> = rows
        .into_iter()
        .map(|row| {
            let items: Vec<StandardListViewItem> = row
                .into_iter()
                .map(|cell| StandardListViewItem::from(SharedString::from(cell)))
                .collect();
            ModelRc::new(VecModel::from(items))
        })
        .collect();
    ModelRc::new(VecModel::from(outer))
}

pub(crate) fn to_find_groups_model(groups: Vec<FindSnapshotGroup>) -> ModelRc<UiFindSnapshotGroup> {
    let items: Vec<UiFindSnapshotGroup> = groups
        .into_iter()
        .map(|g| {
            let row_count = g.rows.len() as i32;
            let table_rows: Vec<Vec<String>> = g
                .rows
                .into_iter()
                .map(|r| vec![r.path, r.mtime, r.size, r.status])
                .collect();
            UiFindSnapshotGroup {
                snapshot_id: g.snapshot_id.into(),
                snapshot_time: g.snapshot_time.into(),
                row_count,
                rows: to_table_model(table_rows),
            }
        })
        .collect();
    ModelRc::new(VecModel::from(items))
}

pub(crate) fn to_string_model(items: Vec<String>) -> ModelRc<SharedString> {
    let shared: Vec<SharedString> = items.into_iter().map(SharedString::from).collect();
    ModelRc::new(VecModel::from(shared))
}

/// Glyph prefixed onto the ID cell when a snapshot row is selected.
const SELECTED_PREFIX: &str = "● ";

/// Build the snapshot table rows, prefixing the ID cell with a marker glyph
/// for selected rows. `selected` may be shorter than `data` (treated as
/// "not selected" for missing indices).
pub(crate) fn build_snapshot_table_rows(
    data: &[SnapshotRowData],
    selected: &[bool],
) -> Vec<Vec<String>> {
    data.iter()
        .enumerate()
        .map(|(i, d)| {
            let id = if selected.get(i).copied().unwrap_or(false) {
                format!("{SELECTED_PREFIX}{}", d.id)
            } else {
                d.id.clone()
            };
            vec![
                id,
                d.hostname.clone(),
                d.time_str.clone(),
                d.label.clone(),
                d.files.clone(),
                d.size.clone(),
            ]
        })
        .collect()
}

/// Push the snapshot table data to the UI: AppData id/repo lists, the rows
/// model (with selection prefix), and the selection count badge.
pub(crate) fn publish_snapshot_table(
    ui: &MainWindow,
    data: &[SnapshotRowData],
    selection: &SnapshotSelection,
) {
    let ids: Vec<String> = data.iter().map(|d| d.id.clone()).collect();
    let rnames: Vec<String> = data.iter().map(|d| d.repo_name.clone()).collect();
    ui.global::<AppData>()
        .set_snapshot_ids(to_string_model(ids));
    ui.global::<AppData>()
        .set_snapshot_repo_names(to_string_model(rnames));

    let rows = build_snapshot_table_rows(data, &selection.selected);
    ui.set_snapshot_rows(to_table_model(rows));
    ui.set_snapshot_selected_count(selection.count());
}

pub(crate) fn sort_snapshot_table(
    sd: &Arc<Mutex<Vec<SnapshotRowData>>>,
    sel: &Arc<Mutex<SnapshotSelection>>,
    ui_weak: &slint::Weak<MainWindow>,
    col_idx: i32,
    ascending: bool,
) {
    let Some(ui) = ui_weak.upgrade() else {
        return;
    };
    let Ok(mut data) = sd.lock() else {
        return;
    };

    // Columns: 0=ID, 1=Host, 2=Time, 3=Label, 4=Files, 5=Size
    match col_idx {
        0 => data.sort_by(|a, b| a.id.cmp(&b.id)),
        1 => data.sort_by(|a, b| a.hostname.cmp(&b.hostname)),
        2 => data.sort_by_key(|a| a.time_epoch),
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
        _ => return,
    }
    if !ascending {
        data.reverse();
    }

    if let Ok(mut selection) = sel.lock() {
        selection.reset(data.len());
        publish_snapshot_table(&ui, &data, &selection);
    }
}

pub(crate) fn collect_repo_names(repos: &[ResolvedRepo]) -> Vec<String> {
    repos.iter().map(format_repo_name).collect()
}

pub(crate) fn build_source_model_data(
    repos: &[ResolvedRepo],
) -> (Vec<SourceInfoData>, Vec<String>) {
    let mut seen = std::collections::HashSet::new();
    let mut items = Vec::new();
    let mut labels = Vec::new();

    for repo in repos {
        for source in &repo.sources {
            if !seen.insert(source.label.clone()) {
                continue;
            }
            let target = if source.repos.is_empty() {
                "(all)".to_string()
            } else {
                source.repos.join(", ")
            };
            let mut flags = Vec::new();
            if source.one_file_system {
                flags.push("one_file_system");
            }
            if source.git_ignore {
                flags.push("git_ignore");
            }
            if source.xattrs_enabled {
                flags.push("xattrs");
            }

            let mut hooks_lines = Vec::new();
            for (phase, cmds) in [
                ("before", &source.hooks.before),
                ("after", &source.hooks.after),
                ("failed", &source.hooks.failed),
                ("finally", &source.hooks.finally),
            ] {
                if !cmds.is_empty() {
                    hooks_lines.push(format!("{}: {}", phase, cmds.join("; ")));
                }
            }

            let mut retention_parts = Vec::new();
            if let Some(ref ret) = source.retention {
                if let Some(ref v) = ret.keep_within {
                    retention_parts.push(format!("keep_within: {v}"));
                }
                if let Some(v) = ret.keep_last {
                    retention_parts.push(format!("keep_last: {v}"));
                }
                if let Some(v) = ret.keep_hourly {
                    retention_parts.push(format!("keep_hourly: {v}"));
                }
                if let Some(v) = ret.keep_daily {
                    retention_parts.push(format!("keep_daily: {v}"));
                }
                if let Some(v) = ret.keep_weekly {
                    retention_parts.push(format!("keep_weekly: {v}"));
                }
                if let Some(v) = ret.keep_monthly {
                    retention_parts.push(format!("keep_monthly: {v}"));
                }
                if let Some(v) = ret.keep_yearly {
                    retention_parts.push(format!("keep_yearly: {v}"));
                }
            }

            items.push(SourceInfoData {
                label: source.label.clone(),
                paths: source.paths.join(", "),
                excludes: source.exclude.join(", "),
                target_repos: target,
                target_repo_names: source.repos.clone(),
                detail_paths: source.paths.join("\n"),
                detail_excludes: source.exclude.join("\n"),
                detail_exclude_if_present: source.exclude_if_present.join("\n"),
                detail_flags: flags.join(", "),
                detail_hooks: hooks_lines.join("\n"),
                detail_retention: retention_parts.join(", "),
                detail_command_dumps: source
                    .command_dumps
                    .iter()
                    .map(|d| format!("{}: {}", d.name, d.command))
                    .collect::<Vec<_>>()
                    .join("\n"),
            });
            labels.push(source.label.clone());
        }
    }

    (items, labels)
}

/// Look up the currently selected repo name from AppData.repo_labels.
pub(crate) fn current_repo_name(ui: &MainWindow) -> Option<String> {
    let idx = ui.get_current_repo_index();
    if idx < 0 {
        return None;
    }
    let labels = ui.global::<AppData>().get_repo_labels();
    labels.row_data(idx as usize).map(|s| s.to_string())
}

fn source_info_from_data(d: &SourceInfoData) -> SourceInfo {
    SourceInfo {
        label: d.label.clone().into(),
        paths: d.paths.clone().into(),
        excludes: d.excludes.clone().into(),
        target_repos: d.target_repos.clone().into(),
        expanded: false,
        detail_paths: d.detail_paths.clone().into(),
        detail_excludes: d.detail_excludes.clone().into(),
        detail_exclude_if_present: d.detail_exclude_if_present.clone().into(),
        detail_flags: d.detail_flags.clone().into(),
        detail_hooks: d.detail_hooks.clone().into(),
        detail_retention: d.detail_retention.clone().into(),
        detail_command_dumps: d.detail_command_dumps.clone().into(),
    }
}

/// Build the per-repo sources model: those targeting the given repo, or all repos.
pub(crate) fn build_repo_source_model(
    items: &[SourceInfoData],
    current_repo: Option<&str>,
) -> Vec<SourceInfo> {
    items
        .iter()
        .filter(|d| {
            d.target_repo_names.is_empty()
                || current_repo
                    .map(|r| d.target_repo_names.iter().any(|n| n == r))
                    .unwrap_or(false)
        })
        .map(source_info_from_data)
        .collect()
}

pub(crate) fn send_structured_data(ui_tx: &Sender<UiEvent>, repos: &[ResolvedRepo]) {
    let _ = ui_tx.send(UiEvent::RepoNames(collect_repo_names(repos)));

    let (items, labels) = build_source_model_data(repos);
    let _ = ui_tx.send(UiEvent::SourceModelData { items, labels });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(label: &str, target_repo_names: &[&str]) -> SourceInfoData {
        SourceInfoData {
            label: label.to_string(),
            paths: String::new(),
            excludes: String::new(),
            target_repos: target_repo_names.join(", "),
            target_repo_names: target_repo_names.iter().map(|s| (*s).to_string()).collect(),
            detail_paths: String::new(),
            detail_excludes: String::new(),
            detail_exclude_if_present: String::new(),
            detail_flags: String::new(),
            detail_hooks: String::new(),
            detail_retention: String::new(),
            detail_command_dumps: String::new(),
        }
    }

    fn labels_of(model: &[SourceInfo]) -> Vec<String> {
        model.iter().map(|s| s.label.to_string()).collect()
    }

    #[test]
    fn empty_target_list_means_all_repos() {
        // Sources without a targets list appear for every repo, and also when
        // no repo is selected.
        let items = vec![source("everywhere", &[])];
        assert_eq!(
            labels_of(&build_repo_source_model(&items, None)),
            ["everywhere"]
        );
        assert_eq!(
            labels_of(&build_repo_source_model(&items, Some("any-repo"))),
            ["everywhere"]
        );
    }

    #[test]
    fn target_list_filters_by_current_repo() {
        let items = vec![
            source("docs", &["main"]),
            source("photos", &["main", "offsite"]),
            source("vm", &["offsite"]),
        ];
        assert_eq!(
            labels_of(&build_repo_source_model(&items, Some("main"))),
            ["docs", "photos"]
        );
        assert_eq!(
            labels_of(&build_repo_source_model(&items, Some("offsite"))),
            ["photos", "vm"]
        );
    }

    #[test]
    fn unmatched_repo_hides_targeted_sources() {
        let items = vec![source("docs", &["main"]), source("any", &[])];
        // "any" is still visible (empty target = all); "docs" is filtered out.
        assert_eq!(
            labels_of(&build_repo_source_model(&items, Some("other"))),
            ["any"]
        );
    }

    #[test]
    fn no_selected_repo_shows_only_untargeted_sources() {
        let items = vec![source("docs", &["main"]), source("any", &[])];
        assert_eq!(labels_of(&build_repo_source_model(&items, None)), ["any"]);
    }
}
