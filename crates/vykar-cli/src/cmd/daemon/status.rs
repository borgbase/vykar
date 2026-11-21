//! Shared, read-only status snapshot served by the daemon's HTTP page.
//!
//! The daemon thread owns the process, schedule and cycle fields; the status
//! worker (`worker.rs`) owns the per-repo rows and recent snapshots and
//! publishes them here. The HTTP thread renders it. All fields are formatted
//! strings ready for HTML or JSON; no repo I/O happens during request handling.

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
use vykar_types::error::Result;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RepoInfo {
    pub name: String,
    pub url: String,
    pub snapshots: String,
    pub last_snapshot: String,
    pub size: String,
    /// This repo's own next scheduled run — repos can have different cadences.
    /// Stamped by `set_repo_next_runs` and carried across row rebuilds by
    /// `carry_over_next_runs` (the status worker does not know the schedule).
    pub next_run: String,
    /// Freshness of the row: `pending`, `ok`, `skipped` or `error`.
    pub state: String,
    /// When the row was last fetched successfully; "—" until then.
    pub refreshed_at: String,
}

impl RepoInfo {
    /// Row shown for a repo whose status has not been fetched yet.
    pub(crate) fn placeholder(repo: &ResolvedRepo) -> Self {
        Self {
            name: repo.label_or_url().to_string(),
            url: repo.config.repository.url.clone(),
            snapshots: "—".to_string(),
            last_snapshot: "—".to_string(),
            size: "—".to_string(),
            next_run: "—".to_string(),
            state: "pending".to_string(),
            refreshed_at: "—".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SnapshotRow {
    pub id: String,
    pub time: String,
    pub hostname: String,
    pub label: String,
    pub files: String,
    pub size: String,
    pub added: String,
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

pub(crate) const RECENT_SNAPSHOTS_LIMIT: usize = 10;

/// Initialize the static parts (process info, sources, schedule) at startup
/// and on reload, and seed one row per repo so `set_repo_next_runs` has rows
/// to stamp before the status worker publishes. Rows whose name and URL are
/// unchanged are kept; the worker owns their contents from then on.
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
    s.repos = seed_rows(repos, &s.repos);
}

fn seed_rows(repos: &[ResolvedRepo], previous: &[RepoInfo]) -> Vec<RepoInfo> {
    repos
        .iter()
        .map(|repo| {
            previous
                .iter()
                .find(|p| p.name == repo.label_or_url() && p.url == repo.config.repository.url)
                .cloned()
                .unwrap_or_else(|| RepoInfo::placeholder(repo))
        })
        .collect()
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
/// The status worker rebuilds the rows, so this is re-applied on every tick.
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
/// The status worker builds rows that do not know the schedule, so without
/// this the column would drop back to the placeholder until the daemon loop's
/// next 1s tick re-stamped it — briefly visible in `/api/status.json`.
fn carry_over_next_runs(new_rows: &mut [RepoInfo], previous: &[RepoInfo]) {
    for row in new_rows {
        if let Some(prev) = previous.iter().find(|p| p.name == row.name) {
            row.next_run.clone_from(&prev.next_run);
        }
    }
}

/// Read one repo's summary row and its newest `RECENT_SNAPSHOTS_LIMIT`
/// snapshots. The returned row has `state`/`refreshed_at` unset; the caller
/// stamps them.
pub(crate) fn fetch_repo(repo: &ResolvedRepo) -> Result<(RepoInfo, Vec<SnapshotRow>)> {
    let name = repo.label_or_url().to_string();
    let pass = configured_passphrase(&repo.config)?;
    let pass_ref = pass.as_deref().map(|s| s.as_str());

    let stats = vykar_core::commands::info::run(&repo.config, pass_ref)?;
    let info = RepoInfo {
        name: name.clone(),
        url: repo.config.repository.url.clone(),
        snapshots: stats.snapshot_count.to_string(),
        last_snapshot: format_last_snapshot(stats.last_snapshot_time),
        size: format_bytes(stats.unique_stored_size),
        next_run: "—".to_string(),
        state: String::new(),
        refreshed_at: String::new(),
    };

    let listing = list::list_snapshots_with_stats(&repo.config, pass_ref)?;
    if let Some(msg) = vykar_core::repo::snapshot_cache::describe_skipped(&listing.hidden) {
        tracing::warn!(repo = %name, "status refresh: {msg}");
    }
    let mut snapshots = listing.snapshots;
    snapshots.sort_by_key(|(s, _)| std::cmp::Reverse(s.time));
    snapshots.truncate(RECENT_SNAPSHOTS_LIMIT);
    let rows = snapshots
        .into_iter()
        .map(|(s, stats)| {
            let row = views::SnapshotRowView::new(&s, stats.as_ref());
            SnapshotRow {
                id: row.id,
                time: row.time,
                hostname: row.hostname,
                label: row.label,
                files: row.files,
                size: row.size,
                added: row.added,
                repo_name: name.clone(),
            }
        })
        .collect();
    Ok((info, rows))
}

/// Replace the per-repo rows and the recent-snapshot list. `snapshots` holds
/// each repo's own list; they are merged newest first and truncated.
pub(crate) fn publish(status: &SharedStatus, mut rows: Vec<RepoInfo>, snapshots: Vec<SnapshotRow>) {
    let mut recent = snapshots;
    recent.sort_by(|a, b| b.time.cmp(&a.time));
    recent.truncate(RECENT_SNAPSHOTS_LIMIT);

    let mut s = status.write().expect("status lock poisoned");
    carry_over_next_runs(&mut rows, &s.repos);
    s.repos = rows;
    s.recent_snapshots = recent;
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

/// `all_skipped` means every attempted repo was skipped by a hook or because its
/// repository was unavailable, so the cycle produced no backups and must not
/// read as `ok`.
fn cycle_outcome(had_error: bool, had_partial: bool, all_skipped: bool) -> &'static str {
    if had_error {
        "errors"
    } else if had_partial {
        "partial"
    } else if all_skipped {
        "skipped"
    } else {
        "ok"
    }
}

pub(crate) fn record_cycle_end(
    status: &SharedStatus,
    elapsed: Duration,
    had_error: bool,
    had_partial: bool,
    all_skipped: bool,
) {
    let now: DateTime<Local> = Local::now();
    let outcome = cycle_outcome(had_error, had_partial, all_skipped);
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
            state: "ok".to_string(),
            refreshed_at: "—".to_string(),
        }
    }

    fn snap(repo: &str, time: &str) -> SnapshotRow {
        SnapshotRow {
            id: format!("{repo}-{time}"),
            time: time.to_string(),
            hostname: "h".to_string(),
            label: "l".to_string(),
            files: "1".to_string(),
            size: "1 B".to_string(),
            added: "1 B".to_string(),
            repo_name: repo.to_string(),
        }
    }

    fn resolved(label: &str, url: &str) -> ResolvedRepo {
        let mut config = vykar_core::config::VykarConfig::default();
        config.repository.url = url.to_string();
        ResolvedRepo {
            label: Some(label.to_string()),
            config,
            global_hooks: Default::default(),
            repo_hooks: Default::default(),
            sources: Vec::new(),
        }
    }

    #[test]
    fn init_seeds_rows_so_next_run_stamps_land() {
        let status = new_shared();
        init(&status, &[resolved("a", "/a")], Instant::now());
        set_repo_next_runs(&status, &[("a".to_string(), None)]);

        let s = status.read().unwrap();
        assert_eq!(s.repos.len(), 1);
        assert_eq!(s.repos[0].state, "pending");
        assert_eq!(s.repos[0].next_run, "Off");
    }

    #[test]
    fn seed_rows_keeps_same_name_and_url_only() {
        let mut kept = row("a", "Off");
        kept.url = "/a".to_string();
        let mut moved = row("b", "Off");
        moved.url = "/b-old".to_string();
        let previous = vec![kept, moved, row("gone", "Off")];

        let rows = seed_rows(&[resolved("a", "/a"), resolved("b", "/b-new")], &previous);

        assert_eq!(rows.len(), 2);
        assert_eq!(
            (rows[0].state.as_str(), rows[0].next_run.as_str()),
            ("ok", "Off")
        );
        assert_eq!(
            (rows[1].state.as_str(), rows[1].url.as_str()),
            ("pending", "/b-new")
        );
    }

    #[test]
    fn publish_merges_snapshots_and_carries_next_run() {
        let status = new_shared();
        status.write().unwrap().repos = vec![row("a", "2026-07-25 13:00:00")];

        let mut snaps: Vec<SnapshotRow> = (0..8)
            .map(|i| snap("a", &format!("2026-01-0{} 00:00", i + 1)))
            .collect();
        snaps.extend((0..8).map(|i| snap("b", &format!("2026-02-0{} 00:00", i + 1))));

        publish(&status, vec![row("a", "—"), row("b", "—")], snaps);

        let s = status.read().unwrap();
        assert_eq!(s.repos[0].next_run, "2026-07-25 13:00:00");
        assert_eq!(s.repos[1].next_run, "—");
        assert_eq!(s.recent_snapshots.len(), RECENT_SNAPSHOTS_LIMIT);
        assert_eq!(s.recent_snapshots[0].time, "2026-02-08 00:00");
        assert!(s.recent_snapshots[..8].iter().all(|r| r.repo_name == "b"));
        assert_eq!(s.recent_snapshots[8].time, "2026-01-08 00:00");
    }

    #[test]
    fn cycle_outcome_precedence() {
        assert_eq!(cycle_outcome(true, true, true), "errors");
        assert_eq!(cycle_outcome(true, false, false), "errors");
        assert_eq!(cycle_outcome(false, true, true), "partial");
        assert_eq!(cycle_outcome(false, false, true), "skipped");
        assert_eq!(cycle_outcome(false, false, false), "ok");
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
