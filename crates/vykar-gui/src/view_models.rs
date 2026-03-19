use std::sync::{Arc, Mutex};

use crossbeam_channel::Sender;
use slint::{ComponentHandle, ModelRc, SharedString, StandardListViewItem, VecModel};
use vykar_core::config::ResolvedRepo;

use crate::messages::{SnapshotRowData, SourceInfoData, UiEvent};
use crate::repo_helpers::format_repo_name;
use crate::{AppData, MainWindow};

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

pub(crate) fn to_string_model(items: Vec<String>) -> ModelRc<SharedString> {
    let shared: Vec<SharedString> = items.into_iter().map(SharedString::from).collect();
    ModelRc::new(VecModel::from(shared))
}

pub(crate) fn sort_snapshot_table(
    sd: &Arc<Mutex<Vec<SnapshotRowData>>>,
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

    // Columns: 0=ID, 1=Host, 2=Time, 3=Source, 4=Label, 5=Files, 6=Size
    match col_idx {
        0 => data.sort_by(|a, b| a.id.cmp(&b.id)),
        1 => data.sort_by(|a, b| a.hostname.cmp(&b.hostname)),
        2 => data.sort_by(|a, b| a.time_epoch.cmp(&b.time_epoch)),
        3 => data.sort_by(|a, b| a.source.cmp(&b.source)),
        4 => data.sort_by(|a, b| a.label.cmp(&b.label)),
        5 => data.sort_by(|a, b| match (a.nfiles, b.nfiles) {
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }),
        6 => data.sort_by(|a, b| match (a.size_bytes, b.size_bytes) {
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

    let ids: Vec<String> = data.iter().map(|d| d.id.clone()).collect();
    let rnames: Vec<String> = data.iter().map(|d| d.repo_name.clone()).collect();
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
    ui.set_snapshot_rows(to_table_model(rows));
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

pub(crate) fn send_structured_data(ui_tx: &Sender<UiEvent>, repos: &[ResolvedRepo]) {
    let _ = ui_tx.send(UiEvent::RepoNames(collect_repo_names(repos)));

    let (items, labels) = build_source_model_data(repos);
    let _ = ui_tx.send(UiEvent::SourceModelData { items, labels });
}
