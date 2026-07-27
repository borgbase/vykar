//! Presentation-neutral projections of repository data.
//!
//! The CLI (`vykar list`, the daemon status page) and the GUI render the same
//! three things — the configured sources, a snapshot table, and a repo's
//! last-backup age — and used to each carry their own copy of the projection.
//! These producers own the shared shape and the formatting decisions; consumers
//! only join, wrap, or drop fields. Core still never prints: everything here is
//! returned, never written to a stream.

use chrono::{DateTime, Local, Utc};

use crate::config::ResolvedRepo;
use crate::repo::manifest::SnapshotEntry;
use crate::snapshot::SnapshotStats;
use vykar_common::display::{format_bytes, format_count};

/// Placeholder for a column with no value (unreadable stats, empty field).
const MISSING: &str = "-";

/// Timestamp format shared by every snapshot table.
const SNAPSHOT_TIME_FORMAT: &str = "%Y-%m-%d %H:%M";

/// One configured backup source, deduplicated by label across repositories.
///
/// Fields are kept as lists rather than pre-joined strings because the two
/// consumers join them differently: the CLI status page renders `folders` as a
/// list, the GUI renders both a one-line summary (`", "`) and a detail block
/// (`"\n"`).
#[derive(Debug, Clone)]
pub struct SourceSummary {
    pub label: String,
    pub paths: Vec<String>,
    pub excludes: Vec<String>,
    pub exclude_if_present: Vec<String>,
    /// Repositories this source targets; empty means "all".
    pub target_repo_names: Vec<String>,
    /// `target_repo_names` joined for display, or `"(all)"` when empty.
    pub target_repos: String,
    /// Enabled per-source flags (`one_file_system`, `git_ignore`, `xattrs`).
    pub options: Vec<String>,
    /// `"<phase>: <cmd>; <cmd>"` for each non-empty hook phase.
    pub hooks: Vec<String>,
    /// `"keep_<policy>: <value>"` for each configured retention policy.
    pub retention: Vec<String>,
    /// `"<name>: <command>"` for each command dump.
    pub command_dumps: Vec<String>,
}

/// Project every distinct source across `repos`, in configuration order.
///
/// Sources are deduplicated by label: the same source may target several
/// repositories, but it is one entry in the UI.
#[must_use]
pub fn collect_source_summaries(repos: &[ResolvedRepo]) -> Vec<SourceSummary> {
    let mut seen = std::collections::HashSet::new();
    let mut items = Vec::new();

    for repo in repos {
        for source in &repo.sources {
            if !seen.insert(source.label.clone()) {
                continue;
            }

            let mut options = Vec::new();
            if source.one_file_system {
                options.push("one_file_system".to_string());
            }
            if source.git_ignore {
                options.push("git_ignore".to_string());
            }
            if source.xattrs_enabled {
                options.push("xattrs".to_string());
            }

            let mut hooks = Vec::new();
            for (phase, cmds) in [
                ("before", &source.hooks.before),
                ("after", &source.hooks.after),
                ("failed", &source.hooks.failed),
                ("finally", &source.hooks.finally),
            ] {
                if !cmds.is_empty() {
                    hooks.push(format!("{}: {}", phase, cmds.join("; ")));
                }
            }

            let mut retention = Vec::new();
            if let Some(ref ret) = source.retention {
                if let Some(ref v) = ret.keep_within {
                    retention.push(format!("keep_within: {v}"));
                }
                for (name, value) in [
                    ("keep_last", ret.keep_last),
                    ("keep_hourly", ret.keep_hourly),
                    ("keep_daily", ret.keep_daily),
                    ("keep_weekly", ret.keep_weekly),
                    ("keep_monthly", ret.keep_monthly),
                    ("keep_yearly", ret.keep_yearly),
                ] {
                    if let Some(v) = value {
                        retention.push(format!("{name}: {v}"));
                    }
                }
            }

            items.push(SourceSummary {
                label: source.label.clone(),
                paths: source.paths.clone(),
                excludes: source.exclude.clone(),
                exclude_if_present: source.exclude_if_present.clone(),
                target_repos: if source.repos.is_empty() {
                    "(all)".to_string()
                } else {
                    source.repos.join(", ")
                },
                target_repo_names: source.repos.clone(),
                options,
                hooks,
                retention,
                command_dumps: source
                    .command_dumps
                    .iter()
                    .map(|d| format!("{}: {}", d.name, d.command))
                    .collect(),
            });
        }
    }

    items
}

/// One row of a snapshot table, formatted once for every front end.
///
/// The `Option<u64>` twins of the formatted size/count columns exist for the
/// GUI, which sorts on the raw values; they are `None` exactly when the
/// corresponding string is the `"-"` placeholder.
#[derive(Debug, Clone)]
pub struct SnapshotRowView {
    pub id: String,
    /// Local time, `"%Y-%m-%d %H:%M"`.
    pub time: String,
    pub time_epoch: i64,
    /// Hostname, or `"-"` when the snapshot did not record one.
    pub hostname: String,
    /// Source label, or `"-"`. See [`SnapshotRowView::effective_label`].
    pub label: String,
    /// `source_paths`, falling back to the source label, else empty.
    pub source_paths: Vec<String>,
    pub files: String,
    /// Original (pre-dedup) size — the "Size" column everywhere.
    pub size: String,
    /// Deduplicated size — the "Added" column.
    pub added: String,
    pub nfiles: Option<u64>,
    pub size_bytes: Option<u64>,
    pub added_bytes: Option<u64>,
}

impl SnapshotRowView {
    /// The label to show for a snapshot: `source_label`, falling back to the
    /// legacy `label` field.
    ///
    /// New snapshots only ever write `source_label`, but snapshots taken by
    /// older versions carry their label in `label` alone; without the fallback
    /// those rows render blank.
    #[must_use]
    pub fn effective_label(entry: &SnapshotEntry) -> &str {
        Self::effective_label_parts(&entry.source_label, &entry.label)
    }

    /// [`Self::effective_label`] for callers that hold the two label fields
    /// without a `SnapshotEntry` (e.g. `SnapshotMeta`), so every surface
    /// resolves labels by the same rule.
    #[must_use]
    pub fn effective_label_parts<'a>(source_label: &'a str, label: &'a str) -> &'a str {
        if source_label.is_empty() {
            label
        } else {
            source_label
        }
    }

    #[must_use]
    pub fn new(entry: &SnapshotEntry, stats: Option<&SnapshotStats>) -> Self {
        let dash_if_empty = |s: &str| {
            if s.is_empty() {
                MISSING.to_string()
            } else {
                s.to_string()
            }
        };

        let source_paths = if entry.source_paths.is_empty() {
            match Self::effective_label(entry) {
                "" => Vec::new(),
                label => vec![label.to_string()],
            }
        } else {
            entry.source_paths.clone()
        };

        Self {
            id: entry.name.clone(),
            time: entry
                .time
                .with_timezone(&Local)
                .format(SNAPSHOT_TIME_FORMAT)
                .to_string(),
            time_epoch: entry.time.timestamp(),
            hostname: dash_if_empty(&entry.hostname),
            label: dash_if_empty(Self::effective_label(entry)),
            source_paths,
            files: stats.map_or_else(|| MISSING.to_string(), |s| format_count(s.nfiles)),
            size: stats.map_or_else(|| MISSING.to_string(), |s| format_bytes(s.original_size)),
            added: stats.map_or_else(
                || MISSING.to_string(),
                |s| format_bytes(s.deduplicated_size),
            ),
            nfiles: stats.map(|s| s.nfiles),
            size_bytes: stats.map(|s| s.original_size),
            added_bytes: stats.map(|s| s.deduplicated_size),
        }
    }
}

/// Relative age of a repository's most recent snapshot, for a metric card.
///
/// A timestamp in the future (clock skew, or a snapshot taken on a host running
/// ahead) is shown as an absolute local time rather than a negative age.
#[must_use]
pub fn format_last_snapshot(t: Option<DateTime<Utc>>) -> String {
    let Some(t) = t else {
        return "N/A".to_string();
    };
    let secs = (Utc::now() - t).num_seconds();
    if secs < 0 {
        return t
            .with_timezone(&Local)
            .format(SNAPSHOT_TIME_FORMAT)
            .to_string();
    }
    if secs < 60 {
        "just now".to_string()
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86_400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86_400)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use vykar_types::snapshot_id::SnapshotId;

    fn entry(source_label: &str, legacy_label: &str, hostname: &str) -> SnapshotEntry {
        SnapshotEntry {
            name: "snap-1".into(),
            id: SnapshotId::from_bytes([7u8; 32]),
            time: Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap(),
            source_label: source_label.into(),
            label: legacy_label.into(),
            source_paths: Vec::new(),
            hostname: hostname.into(),
        }
    }

    fn stats() -> SnapshotStats {
        SnapshotStats {
            nfiles: 1234,
            original_size: 4096,
            compressed_size: 2048,
            deduplicated_size: 1024,
            errors: 0,
        }
    }

    #[test]
    fn legacy_label_is_used_when_source_label_is_empty() {
        // Snapshots written by older versions only set `label`; without the
        // fallback their rows render blank in every front end.
        assert_eq!(
            SnapshotRowView::new(&entry("", "old-style", "nas"), None).label,
            "old-style"
        );
        assert_eq!(
            SnapshotRowView::new(&entry("current", "old-style", "nas"), None).label,
            "current"
        );
    }

    #[test]
    fn empty_fields_become_the_placeholder() {
        let row = SnapshotRowView::new(&entry("", "", ""), None);
        assert_eq!(row.hostname, MISSING);
        assert_eq!(row.label, MISSING);
        assert_eq!(
            (row.files.as_str(), row.size.as_str(), row.added.as_str()),
            (MISSING, MISSING, MISSING)
        );
        assert_eq!(
            (row.nfiles, row.size_bytes, row.added_bytes),
            (None, None, None)
        );
        assert!(row.source_paths.is_empty());
    }

    #[test]
    fn size_is_original_and_added_is_deduplicated() {
        let s = stats();
        let row = SnapshotRowView::new(&entry("src", "", "nas"), Some(&s));
        assert_eq!(row.size, format_bytes(s.original_size));
        assert_eq!(row.added, format_bytes(s.deduplicated_size));
        assert_eq!(row.files, "1,234");
        assert_eq!(row.size_bytes, Some(s.original_size));
    }

    #[test]
    fn source_paths_fall_back_to_the_label() {
        let mut e = entry("photos", "", "nas");
        assert_eq!(
            SnapshotRowView::new(&e, None).source_paths,
            ["photos".to_string()]
        );
        e.source_paths = vec!["/srv/a".into(), "/srv/b".into()];
        assert_eq!(
            SnapshotRowView::new(&e, None).source_paths,
            ["/srv/a".to_string(), "/srv/b".to_string()]
        );
    }

    #[test]
    fn last_snapshot_age_ladder() {
        assert_eq!(format_last_snapshot(None), "N/A");
        let now = Utc::now();
        assert_eq!(format_last_snapshot(Some(now)), "just now");
        assert_eq!(
            format_last_snapshot(Some(now - chrono::Duration::minutes(5))),
            "5m ago"
        );
        assert_eq!(
            format_last_snapshot(Some(now - chrono::Duration::hours(3))),
            "3h ago"
        );
        assert_eq!(
            format_last_snapshot(Some(now - chrono::Duration::days(2))),
            "2d ago"
        );
        // A future timestamp shows as an absolute time, not a negative age.
        assert!(format_last_snapshot(Some(now + chrono::Duration::hours(1))).contains('-'));
    }
}
