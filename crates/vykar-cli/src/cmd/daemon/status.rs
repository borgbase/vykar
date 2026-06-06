//! Shared, read-only status snapshot served by the daemon's HTTP page.
//!
//! The daemon thread refreshes this struct after every backup cycle; the
//! HTTP thread renders it. All fields are formatted strings ready for HTML
//! or JSON; no repo I/O happens during request handling.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, Local, Utc};
use serde::Serialize;

use vykar_common::display::{format_bytes, format_count};
use vykar_core::app::operations;
use vykar_core::app::passphrase::configured_passphrase;
use vykar_core::config::{ResolvedRepo, ScheduleConfig};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RepoInfo {
    pub name: String,
    pub url: String,
    pub snapshots: String,
    pub last_snapshot: String,
    pub size: String,
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

fn format_last_snapshot(t: Option<DateTime<Utc>>) -> String {
    let Some(t) = t else {
        return "N/A".to_string();
    };
    let secs = (Utc::now() - t).num_seconds();
    if secs < 0 {
        return t.with_timezone(&Local).format("%Y-%m-%d %H:%M").to_string();
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

fn schedule_brief(schedule: &ScheduleConfig) -> String {
    if !schedule.enabled {
        return "Off".to_string();
    }
    if let Some(ref cron) = schedule.cron {
        return cron.clone();
    }
    schedule.every.clone().unwrap_or_else(|| "24h".to_string())
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

const RECENT_SNAPSHOTS_LIMIT: usize = 10;

/// Initialize the static parts (process info, sources, schedule) once at
/// daemon startup. Per-repo data is populated by `refresh_repos`.
pub(crate) fn init(
    status: &SharedStatus,
    repos: &[ResolvedRepo],
    schedule: &ScheduleConfig,
    started_at: Instant,
) {
    let mut s = status.write().expect("status lock poisoned");
    s.process = ProcessInfo {
        hostname: hostname(),
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime: format_duration(started_at.elapsed()),
        next_run: None,
    };
    s.schedule_brief = schedule_brief(schedule);
    s.sources = collect_sources(repos);
}

/// Refresh process uptime and next-run hint.
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

fn collect_sources(repos: &[ResolvedRepo]) -> Vec<SourceInfo> {
    let mut seen = std::collections::HashSet::new();
    let mut items = Vec::new();
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

            let mut options = Vec::new();
            if source.one_file_system {
                options.push("one_file_system");
            }
            if source.git_ignore {
                options.push("git_ignore");
            }
            if source.xattrs_enabled {
                options.push("xattrs");
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

            items.push(SourceInfo {
                label: source.label.clone(),
                paths_summary: source.paths.join(", "),
                target_repos: target,
                folders: source.paths.clone(),
                exclusions: source.exclude.clone(),
                exclude_if_present: source.exclude_if_present.clone(),
                options: options.join(", "),
                hooks: hooks_lines,
                retention: retention_parts.join(", "),
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

/// Re-read repo manifests and refresh the per-repo + recent-snapshots fields.
/// Errors are logged via tracing and that repo's row is skipped.
pub(crate) fn refresh_repos(status: &SharedStatus, repos: &[ResolvedRepo]) {
    let mut repo_rows: Vec<RepoInfo> = Vec::new();
    let mut all_snapshots: Vec<SnapshotRow> = Vec::new();

    for repo in repos {
        let name = repo
            .label
            .as_deref()
            .unwrap_or(&repo.config.repository.url)
            .to_string();
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
                });
            }
            Err(e) => {
                tracing::warn!(repo = %name, error = %e, "status refresh: info failed");
                continue;
            }
        }

        match operations::list_snapshots_with_stats(&repo.config, pass_ref) {
            Ok(mut snapshots) => {
                snapshots.sort_by_key(|(s, _)| s.time);
                for (s, stats) in snapshots {
                    let ts: DateTime<Local> = s.time.with_timezone(&Local);
                    let label = if s.source_label.is_empty() {
                        "-".to_string()
                    } else {
                        s.source_label.clone()
                    };
                    let hostname = if s.hostname.is_empty() {
                        "-".to_string()
                    } else {
                        s.hostname.clone()
                    };
                    let (files, size) = match stats {
                        Some(st) => (format_count(st.nfiles), format_bytes(st.deduplicated_size)),
                        None => ("-".to_string(), "-".to_string()),
                    };
                    all_snapshots.push(SnapshotRow {
                        id: s.name.clone(),
                        time: ts.format("%Y-%m-%d %H:%M").to_string(),
                        hostname,
                        label,
                        files,
                        size,
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
