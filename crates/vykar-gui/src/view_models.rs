use crossbeam_channel::Sender;
use slint::{ModelRc, SharedString, StandardListViewItem, VecModel};
use vykar_core::config::ResolvedRepo;

use crate::messages::{FindSnapshotGroup, SourceInfoData, UiEvent};
use crate::repo_helpers::format_repo_name;
use crate::{FindSnapshotGroup as UiFindSnapshotGroup, SourceInfo};

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

pub(crate) fn collect_repo_names(repos: &[ResolvedRepo]) -> Vec<SharedString> {
    repos
        .iter()
        .map(|repo| SharedString::from(format_repo_name(repo)))
        .collect()
}

pub(crate) fn build_source_model_data(
    repos: &[ResolvedRepo],
) -> (Vec<SourceInfoData>, Vec<SharedString>) {
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
                label: source.label.clone().into(),
                paths: source.paths.join(", ").into(),
                excludes: source.exclude.join(", ").into(),
                target_repos: target.into(),
                target_repo_names: source.repos.clone(),
                detail_paths: source.paths.join("\n").into(),
                detail_excludes: source.exclude.join("\n").into(),
                detail_exclude_if_present: source.exclude_if_present.join("\n").into(),
                detail_flags: flags.join(", ").into(),
                detail_hooks: hooks_lines.join("\n").into(),
                detail_retention: retention_parts.join(", ").into(),
                detail_command_dumps: source
                    .command_dumps
                    .iter()
                    .map(|d| format!("{}: {}", d.name, d.command))
                    .collect::<Vec<_>>()
                    .join("\n")
                    .into(),
            });
            labels.push(source.label.clone().into());
        }
    }

    (items, labels)
}

fn source_info_from_data(d: &SourceInfoData) -> SourceInfo {
    SourceInfo {
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
            label: label.into(),
            paths: SharedString::default(),
            excludes: SharedString::default(),
            target_repos: target_repo_names.join(", ").into(),
            target_repo_names: target_repo_names.iter().map(|s| (*s).to_string()).collect(),
            detail_paths: SharedString::default(),
            detail_excludes: SharedString::default(),
            detail_exclude_if_present: SharedString::default(),
            detail_flags: SharedString::default(),
            detail_hooks: SharedString::default(),
            detail_retention: SharedString::default(),
            detail_command_dumps: SharedString::default(),
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
