//! Shared, read-only status snapshot served by the daemon's HTTP page.
//!
//! The daemon thread refreshes this struct after every backup cycle; the
//! HTTP thread renders it. All fields are formatted strings ready for HTML
//! or JSON; no repo I/O happens during request handling.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, Local};
use serde::Serialize;

use vykar_common::display::format_bytes;
use vykar_core::app::passphrase::configured_passphrase;
use vykar_core::app::scheduler::repos_schedule_brief;
use vykar_core::app::views::{self, format_last_snapshot};
use vykar_core::commands::list;
use vykar_core::config::ResolvedRepo;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RepoInfo {
    pub name: String,
    pub url: String,
    pub snapshots: String,
    pub last_snapshot: String,
    pub size: String,
    /// This repo's own next scheduled run — repos can have different cadences.
    /// Stamped by `set_repo_next_runs` and carried across row rebuilds by
    /// `carry_over_next_runs` (`refresh_repos` does not know the schedule).
    pub next_run: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SnapshotRow {
    pub id: String,
    pub time: String,
    pub hostname: String,
    pub label: String,
    pub files: String,
    pub size: String,
    pub repo_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SourceInfo {
    pub label: String,
    pub paths_summary: String,
    pub target_repos: String,
    pub folders: Vec<String>,
    pub exclusions: Vec<String>,
    pub exclude_if_present: Vec<String>,
    pub options: String,
    pub hooks: Vec<String>,
    pub retention: String,
    pub command_dumps: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct CycleSummary {
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub outcome: String,
    pub duration: Option<String>,
    pub had_error: bool,
    pub had_partial: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct ProcessInfo {
    pub hostname: String,
    pub pid: u32,
    pub version: String,
    pub uptime: String,
    pub next_run: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct DaemonStatus {
    pub process: ProcessInfo,
    pub schedule_brief: String,
    pub repos: Vec<RepoInfo>,
    pub recent_snapshots: Vec<SnapshotRow>,
    pub sources: Vec<SourceInfo>,
    pub last_cycle: CycleSummary,
}

/// Shared handle for the daemon's status snapshot.
pub(crate) type SharedStatus = Arc<RwLock<DaemonStatus>>;

pub(crate) fn new_shared() -> SharedStatus {
    Arc::new(RwLock::new(DaemonStatus::default()))
}

fn format_duration(d: Duration) -> String {
    let s = d.as_secs();
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m {}s", s / 60, s % 60)
    } else if s < 86_400 {
        format!("{}h {}m", s / 3600, (s % 3600) / 60)
    } else {
        format!("{}d {}h", s / 86_400, (s % 86_400) / 3600)
    }
}

fn format_next_run(next_run: Option<SystemTime>) -> String {
    match next_run {
        Some(t) => {
            let dt: DateTime<Local> = t.into();
            dt.format("%Y-%m-%d %H:%M:%S").to_string()
        }
        None => "Off".to_string(),
    }
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

const RECENT_SNAPSHOTS_LIMIT: usize = 10;

/// Initialize the static parts (process info, sources, schedule) once at
/// daemon startup. Per-repo data is populated by `refresh_repos`.
pub(crate) fn init(status: &SharedStatus, repos: &[ResolvedRepo], started_at: Instant) {
    let mut s = status.write().expect("status lock poisoned");
    s.process = ProcessInfo {
        hostname: hostname(),
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime: format_duration(started_at.elapsed()),
        next_run: None,
    };
    let schedules: Vec<&_> = repos.iter().map(|r| &r.config.schedule).collect();
    s.schedule_brief = repos_schedule_brief(&schedules, false);
    s.sources = collect_sources(repos);
}

/// Refresh process uptime and the earliest next-run hint (what the loop wakes at).
pub(crate) fn touch_process(
    status: &SharedStatus,
    started_at: Instant,
    next_run: Option<SystemTime>,
) {
    let mut s = status.write().expect("status lock poisoned");
    s.process.uptime = format_duration(started_at.elapsed());
    s.process.next_run = next_run.map(|t| {
        let dt: DateTime<Local> = t.into();
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    });
}

/// Stamp each repo row with its own next scheduled run, matched by name.
/// `refresh_repos` rebuilds the rows, so this is re-applied on every tick.
pub(crate) fn set_repo_next_runs(
    status: &SharedStatus,
    next_runs: &[(String, Option<SystemTime>)],
) {
    let mut s = status.write().expect("status lock poisoned");
    for row in &mut s.repos {
        if let Some((_, next)) = next_runs.iter().find(|(name, _)| *name == row.name) {
            row.next_run = format_next_run(*next);
        }
    }
}

fn collect_sources(repos: &[ResolvedRepo]) -> Vec<SourceInfo> {
    views::collect_source_summaries(repos)
        .into_iter()
        .map(|s| SourceInfo {
            paths_summary: s.paths.join(", "),
            label: s.label,
            target_repos: s.target_repos,
            folders: s.paths,
            exclusions: s.excludes,
            exclude_if_present: s.exclude_if_present,
            options: s.options.join(", "),
            hooks: s.hooks,
            retention: s.retention.join(", "),
            command_dumps: s.command_dumps,
        })
        .collect()
}

/// Carry each repo's next-run string across a row rebuild, matched by name.
///
/// `refresh_repos` builds fresh rows that do not know the schedule, so without
/// this the column would drop back to the placeholder until the daemon loop's
/// next 1s tick re-stamped it — briefly visible in `/api/status.json`.
fn carry_over_next_runs(new_rows: &mut [RepoInfo], previous: &[RepoInfo]) {
    for row in new_rows {
        if let Some(prev) = previous.iter().find(|p| p.name == row.name) {
            row.next_run.clone_from(&prev.next_run);
        }
    }
}

/// Re-read repo manifests and refresh the per-repo + recent-snapshots fields.
/// Errors are logged via tracing and that repo's row is skipped.
pub(crate) fn refresh_repos(status: &SharedStatus, repos: &[ResolvedRepo]) {
    let mut repo_rows: Vec<RepoInfo> = Vec::new();
    let mut all_snapshots: Vec<SnapshotRow> = Vec::new();

    for repo in repos {
        let name = repo.label_or_url().to_string();
        let url = repo.config.repository.url.clone();

        let pass = match configured_passphrase(&repo.config) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(repo = %name, error = %e, "status refresh: passphrase resolution failed");
                continue;
            }
        };
        let pass_ref = pass.as_deref().map(|s| s.as_str());

        match vykar_core::commands::info::run(&repo.config, pass_ref) {
            Ok(stats) => {
                repo_rows.push(RepoInfo {
                    name: name.clone(),
                    url,
                    snapshots: stats.snapshot_count.to_string(),
                    last_snapshot: format_last_snapshot(stats.last_snapshot_time),
                    size: format_bytes(stats.unique_stored_size),
                    // Carried over from the previous rows below; only a repo
                    // that has never been stamped keeps this placeholder.
                    next_run: "—".to_string(),
                });
            }
            Err(e) => {
                tracing::warn!(repo = %name, error = %e, "status refresh: info failed");
                continue;
            }
        }

        match list::list_snapshots_with_stats(&repo.config, pass_ref) {
            Ok(listing) => {
                if let Some(msg) =
                    vykar_core::repo::snapshot_cache::describe_skipped(&listing.hidden)
                {
                    tracing::warn!(repo = %name, "status refresh: {msg}");
                }
                let mut snapshots = listing.snapshots;
                snapshots.sort_by_key(|(s, _)| s.time);
                for (s, stats) in snapshots {
                    let row = views::SnapshotRowView::new(&s, stats.as_ref());
                    all_snapshots.push(SnapshotRow {
                        id: row.id,
                        time: row.time,
                        hostname: row.hostname,
                        label: row.label,
                        files: row.files,
                        size: row.size,
                        repo_name: name.clone(),
                    });
                }
            }
            Err(e) => {
                tracing::warn!(repo = %name, error = %e, "status refresh: snapshot listing failed");
            }
        }
    }

    // Most recent first across all repos.
    all_snapshots.sort_by(|a, b| b.time.cmp(&a.time));
    all_snapshots.truncate(RECENT_SNAPSHOTS_LIMIT);

    let mut s = status.write().expect("status lock poisoned");
    carry_over_next_runs(&mut repo_rows, &s.repos);
    s.repos = repo_rows;
    s.recent_snapshots = all_snapshots;
}

pub(crate) fn record_cycle_start(status: &SharedStatus) {
    let now: DateTime<Local> = Local::now();
    let mut s = status.write().expect("status lock poisoned");
    s.last_cycle = CycleSummary {
        started_at: Some(now.format("%Y-%m-%d %H:%M:%S").to_string()),
        finished_at: None,
        outcome: "running".to_string(),
        duration: None,
        had_error: false,
        had_partial: false,
    };
}

pub(crate) fn record_cycle_end(
    status: &SharedStatus,
    elapsed: Duration,
    had_error: bool,
    had_partial: bool,
) {
    let now: DateTime<Local> = Local::now();
    let outcome = if had_error {
        "errors"
    } else if had_partial {
        "partial"
    } else {
        "ok"
    };
    let mut s = status.write().expect("status lock poisoned");
    s.last_cycle.finished_at = Some(now.format("%Y-%m-%d %H:%M:%S").to_string());
    s.last_cycle.outcome = outcome.to_string();
    s.last_cycle.duration = Some(format_duration(elapsed));
    s.last_cycle.had_error = had_error;
    s.last_cycle.had_partial = had_partial;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: &str, next_run: &str) -> RepoInfo {
        RepoInfo {
            name: name.to_string(),
            url: format!("/backups/{name}"),
            snapshots: "0".to_string(),
            last_snapshot: "N/A".to_string(),
            size: "0 B".to_string(),
            next_run: next_run.to_string(),
        }
    }

    #[test]
    fn next_run_survives_a_row_rebuild() {
        let previous = vec![row("nas", "2026-07-25 13:00:00"), row("remote", "Off")];
        let mut rebuilt = vec![row("remote", "—"), row("nas", "—")];

        carry_over_next_runs(&mut rebuilt, &previous);

        assert_eq!(rebuilt[0].next_run, "Off");
        assert_eq!(rebuilt[1].next_run, "2026-07-25 13:00:00");
    }

    #[test]
    fn unknown_repo_keeps_the_placeholder() {
        // A repo added by a SIGHUP reload has no previous row; the daemon loop
        // stamps it within a tick.
        let previous = vec![row("nas", "2026-07-25 13:00:00")];
        let mut rebuilt = vec![row("fresh", "—")];

        carry_over_next_runs(&mut rebuilt, &previous);

        assert_eq!(rebuilt[0].next_run, "—");
    }

    #[test]
    fn next_run_formats_disabled_as_off() {
        assert_eq!(format_next_run(None), "Off");
        assert!(format_next_run(Some(SystemTime::now())).contains('-'));
    }
}
