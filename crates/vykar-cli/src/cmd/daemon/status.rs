//! Shared, read-only status snapshot served by the daemon's HTTP page and
//! as `/api/status.json`.
//!
//! The daemon thread owns the process, schedule and cycle fields; the status
//! worker (`worker.rs`) owns the per-repo rows and recent snapshots and
//! publishes them here. The HTTP thread renders it. All fields are raw values
//! (schema 1 of the JSON documents, see `crate::json::SCHEMA`); the HTML
//! page formats them at render time. No repo I/O happens during request
//! handling.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, SubsecRound, Utc};
use serde::Serialize;

use vykar_core::app::operations::{CycleStep, StepOutcome};
use vykar_core::app::passphrase::{configured_passphrase, resolving_runs_command};
use vykar_core::app::scheduler::repos_schedule_brief;
use vykar_core::app::service::CycleReport;
use vykar_core::app::views::{self, SnapshotRecord};
use vykar_core::commands::list;
use vykar_core::config::ResolvedRepo;
use vykar_types::error::{Result, VykarError};
use zeroize::Zeroizing;

use crate::json::SCHEMA;

/// Freshness of a repository row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RepoState {
    Pending,
    Ok,
    Skipped,
    /// Serialized as `failed`, the page still shows `error` (`as_str`).
    #[serde(rename = "failed")]
    Error,
}

impl RepoState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ok => "ok",
            Self::Skipped => "skipped",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RepoInfo {
    /// `label_or_url`, the key rows are matched by.
    pub name: String,
    pub url: String,
    /// `None` until the row is fetched.
    pub snapshot_count: Option<usize>,
    pub last_snapshot_at: Option<DateTime<Utc>>,
    /// `None` until the row is fetched.
    pub unique_stored_size: Option<u64>,
    /// This repo's own next scheduled run (repos can have different
    /// cadences); `None` when it is not scheduled. Stamped by
    /// `set_repo_next_runs` and carried across row rebuilds by
    /// `carry_over_next_runs` (the status worker does not know the schedule).
    pub next_run_at: Option<DateTime<Utc>>,
    pub status: RepoState,
    /// When the row was last fetched successfully; `None` until then.
    pub refreshed_at: Option<DateTime<Utc>>,
}

impl RepoInfo {
    /// Row shown for a repo whose status has not been fetched yet.
    pub(crate) fn placeholder(repo: &ResolvedRepo) -> Self {
        Self {
            name: repo.label_or_url().to_string(),
            url: repo.config.repository.url.clone(),
            snapshot_count: None,
            last_snapshot_at: None,
            unique_stored_size: None,
            next_run_at: None,
            status: RepoState::Pending,
            refreshed_at: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SnapshotRow {
    pub repo_name: String,
    #[serde(flatten)]
    pub snapshot: SnapshotRecord,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SourceInfo {
    pub source_label: String,
    pub source_paths: Vec<String>,
    pub exclude: Vec<String>,
    pub exclude_if_present: Vec<String>,
    /// Empty: all repositories.
    pub target_repos: Vec<String>,
    pub options: Vec<String>,
    /// `"<phase>: <cmd>; <cmd>"` for each non-empty hook phase.
    pub hooks: Vec<String>,
    /// `"keep_<policy>: <value>"` for each configured retention policy.
    pub retention: Vec<String>,
    /// `"<name>: <command>"` for each command dump.
    pub command_dumps: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CycleOutcome {
    Running,
    Ok,
    Partial,
    /// Serialized as `failed`, the page still shows `errors` (`as_str`).
    #[serde(rename = "failed")]
    Errors,
    Skipped,
}

impl CycleOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Ok => "ok",
            Self::Partial => "partial",
            Self::Errors => "errors",
            Self::Skipped => "skipped",
        }
    }
}

/// One cycle step of a repository: `status` and `kind` as `--json` names
/// them, without the message. Step messages carry hook command lines (often
/// ping URLs with tokens) and engine error text, which the unauthenticated
/// endpoint does not expose; they stay in the daemon's log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StepEntry {
    pub step: CycleStep,
    /// "ok" | "partial" | "skipped" | "failed"
    pub status: String,
    /// The skip or error kind; `None` for ok and partial.
    pub kind: Option<String>,
}

impl StepEntry {
    /// Built from the outcome's own serialization, so the names are the ones
    /// `--json` prints.
    pub(crate) fn new(step: CycleStep, outcome: &StepOutcome) -> Self {
        let value = serde_json::to_value(outcome).unwrap_or_default();
        let field = |name: &str| value.get(name).and_then(|v| v.as_str()).map(str::to_owned);
        Self {
            step,
            status: field("status").unwrap_or_default(),
            kind: field("kind"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RepoCycleSummary {
    pub name: String,
    pub url: String,
    pub steps: Vec<StepEntry>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct CycleSummary {
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    /// `None` before the first cycle.
    pub status: Option<CycleOutcome>,
    pub duration_seconds: Option<u64>,
    pub had_error: bool,
    /// Only repositories without failures count.
    pub had_partial: bool,
    /// The repositories the last finished cycle reached, in order; kept while
    /// the next cycle runs.
    pub repos: Vec<RepoCycleSummary>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct ProcessInfo {
    pub hostname: String,
    pub pid: u32,
    pub version: String,
    /// Wall clock; set by `init`.
    pub started_at: Option<DateTime<Utc>>,
    pub uptime_seconds: u64,
    /// The earliest next run; `None` when nothing is scheduled.
    pub next_run_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DaemonStatus {
    /// `crate::json::SCHEMA`.
    pub schema: u32,
    pub process: ProcessInfo,
    pub schedule_brief: String,
    pub repos: Vec<RepoInfo>,
    pub recent_snapshots: Vec<SnapshotRow>,
    pub sources: Vec<SourceInfo>,
    pub last_cycle: CycleSummary,
}

impl Default for DaemonStatus {
    fn default() -> Self {
        Self {
            schema: SCHEMA,
            process: ProcessInfo::default(),
            schedule_brief: String::new(),
            repos: Vec::new(),
            recent_snapshots: Vec::new(),
            sources: Vec::new(),
            last_cycle: CycleSummary::default(),
        }
    }
}

/// Shared handle for the daemon's status snapshot.
pub(crate) type SharedStatus = Arc<RwLock<DaemonStatus>>;

pub(crate) fn new_shared() -> SharedStatus {
    Arc::new(RwLock::new(DaemonStatus::default()))
}

/// The daemon's own time stamps, in whole seconds so documents read stably.
pub(crate) fn now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(0)
}

fn utc(t: SystemTime) -> DateTime<Utc> {
    DateTime::<Utc>::from(t).trunc_subsecs(0)
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
    let elapsed = started_at.elapsed();
    let wall_start = chrono::Duration::from_std(elapsed)
        .ok()
        .and_then(|d| Utc::now().checked_sub_signed(d))
        .unwrap_or_else(Utc::now)
        .trunc_subsecs(0);
    let mut s = status.write().expect("status lock poisoned");
    // Kept across reloads: the process started once.
    let started = s.process.started_at.unwrap_or(wall_start);
    s.process = ProcessInfo {
        hostname: hostname(),
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        started_at: Some(started),
        uptime_seconds: elapsed.as_secs(),
        next_run_at: None,
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
    s.process.uptime_seconds = started_at.elapsed().as_secs();
    s.process.next_run_at = next_run.map(utc);
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
            row.next_run_at = next.map(utc);
        }
    }
}

fn collect_sources(repos: &[ResolvedRepo]) -> Vec<SourceInfo> {
    views::collect_source_summaries(repos)
        .into_iter()
        .map(|s| SourceInfo {
            source_label: s.label,
            source_paths: s.paths,
            exclude: s.excludes,
            exclude_if_present: s.exclude_if_present,
            target_repos: s.target_repo_names,
            options: s.options,
            hooks: s.hooks,
            retention: s.retention,
            command_dumps: s.command_dumps,
        })
        .collect()
}

/// Carry each repo's next run across a row rebuild, matched by name.
///
/// The status worker builds rows that do not know the schedule, so without
/// this the column would drop back to the placeholder until the daemon loop's
/// next 1s tick re-stamped it — briefly visible in `/api/status.json`.
fn carry_over_next_runs(new_rows: &mut [RepoInfo], previous: &[RepoInfo]) {
    for row in new_rows {
        if let Some(prev) = previous.iter().find(|p| p.name == row.name) {
            row.next_run_at = prev.next_run_at;
        }
    }
}

/// Read one repo's summary row and its newest `RECENT_SNAPSHOTS_LIMIT`
/// snapshots. The returned row has `status`/`refreshed_at` unset; the caller
/// stamps them.
/// `accepted` is the passphrase a cycle accepted; without one, a repo whose
/// passphrase comes from a passcommand is refused instead of running it.
pub(crate) fn fetch_repo(
    repo: &ResolvedRepo,
    accepted: Option<&str>,
) -> Result<(RepoInfo, Vec<SnapshotRow>)> {
    let name = repo.label_or_url().to_string();
    let pass = match accepted {
        Some(p) => Some(Zeroizing::new(p.to_string())),
        // The worker never gets here for a passcommand repo; refuse rather
        // than run a passcommand before the repo's hooks.
        None if resolving_runs_command(&repo.config) => {
            return Err(VykarError::Other(
                "no passphrase accepted by a cycle yet".into(),
            ));
        }
        None => configured_passphrase(&repo.config)?,
    };
    let pass_ref = pass.as_deref().map(|s| s.as_str());

    let stats = vykar_core::commands::info::run(&repo.config, pass_ref)?;
    let info = RepoInfo {
        snapshot_count: Some(stats.snapshot_count),
        last_snapshot_at: stats.last_snapshot_at,
        unique_stored_size: Some(stats.unique_stored_size),
        ..RepoInfo::placeholder(repo)
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
        .map(|(s, stats)| SnapshotRow {
            repo_name: name.clone(),
            snapshot: SnapshotRecord::new(&s, stats.as_ref()),
        })
        .collect();
    Ok((info, rows))
}

/// Replace the per-repo rows and the recent-snapshot list. `snapshots` holds
/// each repo's own list; they are merged newest first and truncated.
pub(crate) fn publish(status: &SharedStatus, mut rows: Vec<RepoInfo>, snapshots: Vec<SnapshotRow>) {
    let mut recent = snapshots;
    recent.sort_by_key(|r| std::cmp::Reverse(r.snapshot.created_at));
    recent.truncate(RECENT_SNAPSHOTS_LIMIT);

    let mut s = status.write().expect("status lock poisoned");
    carry_over_next_runs(&mut rows, &s.repos);
    s.repos = rows;
    s.recent_snapshots = recent;
}

/// A cycle started: the previous cycle's per-repository steps stay until
/// this one ends.
pub(crate) fn record_cycle_start(status: &SharedStatus) {
    let mut s = status.write().expect("status lock poisoned");
    let repos = std::mem::take(&mut s.last_cycle.repos);
    s.last_cycle = CycleSummary {
        started_at: Some(now()),
        finished_at: None,
        status: Some(CycleOutcome::Running),
        duration_seconds: None,
        had_error: false,
        had_partial: false,
        repos,
    };
}

/// The flags of a finished cycle, for the log lines and the page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CycleFlags {
    pub had_error: bool,
    pub had_partial: bool,
    pub all_skipped: bool,
}

/// `had_partial` counts only repositories that did not fail.
pub(crate) fn cycle_flags(report: &CycleReport) -> CycleFlags {
    CycleFlags {
        had_error: report.has_failures(),
        had_partial: report
            .repos
            .iter()
            .any(|r| !r.result.has_failures() && r.result.had_partial()),
        all_skipped: report.all_skipped(),
    }
}

/// `all_skipped` means every attempted repo was skipped by a hook or because its
/// repository was unavailable, so the cycle produced no backups and must not
/// read as `ok`.
fn cycle_outcome(flags: CycleFlags) -> CycleOutcome {
    if flags.had_error {
        CycleOutcome::Errors
    } else if flags.had_partial {
        CycleOutcome::Partial
    } else if flags.all_skipped {
        CycleOutcome::Skipped
    } else {
        CycleOutcome::Ok
    }
}

pub(crate) fn record_cycle_end(
    status: &SharedStatus,
    elapsed: Duration,
    report: &CycleReport,
    repos: &[ResolvedRepo],
) {
    let flags = cycle_flags(report);
    let summaries = report
        .repos
        .iter()
        .map(|r| {
            let (name, url) = repos.get(r.index).map_or_else(
                || ("?".to_string(), String::new()),
                |repo| {
                    (
                        repo.label_or_url().to_string(),
                        repo.config.repository.url.clone(),
                    )
                },
            );
            RepoCycleSummary {
                name,
                url,
                steps: r
                    .result
                    .steps
                    .iter()
                    .map(|(step, outcome)| StepEntry::new(*step, outcome))
                    .collect(),
            }
        })
        .collect();
    let mut s = status.write().expect("status lock poisoned");
    s.last_cycle.finished_at = Some(now());
    s.last_cycle.status = Some(cycle_outcome(flags));
    s.last_cycle.duration_seconds = Some(elapsed.as_secs());
    s.last_cycle.had_error = flags.had_error;
    s.last_cycle.had_partial = flags.had_partial;
    s.last_cycle.repos = summaries;
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use vykar_core::app::operations::FullCycleResult;
    use vykar_core::app::service::RepoCycleReport;
    use vykar_core::app::{SkipKind, SkipReason, StepError, StepErrorKind};
    use vykar_core::repo::manifest::SnapshotEntry;
    use vykar_types::snapshot_id::SnapshotId;

    use super::*;

    fn at(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 25, h, m, 0).unwrap()
    }

    fn row(name: &str, next_run: Option<DateTime<Utc>>) -> RepoInfo {
        RepoInfo {
            name: name.to_string(),
            url: format!("/backups/{name}"),
            snapshot_count: Some(0),
            last_snapshot_at: None,
            unique_stored_size: Some(0),
            next_run_at: next_run,
            status: RepoState::Ok,
            refreshed_at: None,
        }
    }

    fn record(name: &str, time: DateTime<Utc>) -> SnapshotRecord {
        let entry = SnapshotEntry {
            name: name.to_string(),
            id: SnapshotId::from_bytes([0; 32]),
            time,
            source_label: "l".to_string(),
            label: String::new(),
            source_paths: vec!["/p".to_string()],
            hostname: "h".to_string(),
        };
        SnapshotRecord::new(&entry, None)
    }

    fn snap(repo: &str, time: DateTime<Utc>) -> SnapshotRow {
        SnapshotRow {
            repo_name: repo.to_string(),
            snapshot: record(&format!("{repo}-{}", time.timestamp()), time),
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

    fn result(steps: Vec<(CycleStep, StepOutcome)>) -> FullCycleResult {
        FullCycleResult {
            backup_report: None,
            prune_stats: None,
            compact_stats: None,
            check_result: None,
            steps,
        }
    }

    fn report(results: Vec<Vec<(CycleStep, StepOutcome)>>) -> CycleReport {
        CycleReport {
            repos: results
                .into_iter()
                .enumerate()
                .map(|(index, steps)| RepoCycleReport {
                    index,
                    result: result(steps),
                })
                .collect(),
            not_reached: Vec::new(),
        }
    }

    fn failed(kind: StepErrorKind, message: &str) -> StepOutcome {
        StepOutcome::Failed(StepError::new(kind, message))
    }

    #[test]
    fn init_seeds_rows_so_next_run_stamps_land() {
        let status = new_shared();
        init(&status, &[resolved("a", "/a")], Instant::now());
        set_repo_next_runs(&status, &[("a".to_string(), None)]);

        let s = status.read().unwrap();
        assert_eq!(s.schema, 1);
        assert_eq!(s.repos.len(), 1);
        assert_eq!(s.repos[0].status, RepoState::Pending);
        assert_eq!(s.repos[0].next_run_at, None);
        assert!(s.process.started_at.is_some());
    }

    #[test]
    fn next_runs_are_stamped_in_whole_seconds() {
        let status = new_shared();
        init(&status, &[resolved("a", "/a")], Instant::now());
        let t = SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_500);
        set_repo_next_runs(&status, &[("a".to_string(), Some(t))]);
        touch_process(&status, Instant::now(), Some(t));

        let s = status.read().unwrap();
        let want = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        assert_eq!(s.repos[0].next_run_at, Some(want));
        assert_eq!(s.process.next_run_at, Some(want));
    }

    #[test]
    fn seed_rows_keeps_same_name_and_url_only() {
        let mut kept = row("a", None);
        kept.url = "/a".to_string();
        let mut moved = row("b", None);
        moved.url = "/b-old".to_string();
        let previous = vec![kept, moved, row("gone", None)];

        let rows = seed_rows(&[resolved("a", "/a"), resolved("b", "/b-new")], &previous);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].status, RepoState::Ok);
        assert_eq!(
            (rows[1].status, rows[1].url.as_str()),
            (RepoState::Pending, "/b-new")
        );
    }

    #[test]
    fn publish_merges_snapshots_and_carries_next_run() {
        let status = new_shared();
        status.write().unwrap().repos = vec![row("a", Some(at(13, 0)))];

        let day = |month: u32, d: u32| Utc.with_ymd_and_hms(2026, month, d, 0, 0, 0).unwrap();
        let mut snaps: Vec<SnapshotRow> = (1..=8).map(|d| snap("a", day(1, d))).collect();
        snaps.extend((1..=8).map(|d| snap("b", day(2, d))));

        publish(&status, vec![row("a", None), row("b", None)], snaps);

        let s = status.read().unwrap();
        assert_eq!(s.repos[0].next_run_at, Some(at(13, 0)));
        assert_eq!(s.repos[1].next_run_at, None);
        assert_eq!(s.recent_snapshots.len(), RECENT_SNAPSHOTS_LIMIT);
        assert_eq!(s.recent_snapshots[0].snapshot.created_at, day(2, 8));
        assert!(s.recent_snapshots[..8].iter().all(|r| r.repo_name == "b"));
        assert_eq!(s.recent_snapshots[8].snapshot.created_at, day(1, 8));
    }

    #[test]
    fn cycle_outcome_precedence() {
        let flags = |had_error, had_partial, all_skipped| CycleFlags {
            had_error,
            had_partial,
            all_skipped,
        };
        assert_eq!(cycle_outcome(flags(true, true, true)), CycleOutcome::Errors);
        assert_eq!(
            cycle_outcome(flags(true, false, false)),
            CycleOutcome::Errors
        );
        assert_eq!(
            cycle_outcome(flags(false, true, true)),
            CycleOutcome::Partial
        );
        assert_eq!(
            cycle_outcome(flags(false, false, true)),
            CycleOutcome::Skipped
        );
        assert_eq!(cycle_outcome(flags(false, false, false)), CycleOutcome::Ok);
    }

    /// status.json spells failure `failed`; the HTML page keeps its own words.
    #[test]
    fn state_names_in_json_and_on_the_page() {
        for (state, name, page) in [
            (RepoState::Pending, "pending", "pending"),
            (RepoState::Ok, "ok", "ok"),
            (RepoState::Skipped, "skipped", "skipped"),
            (RepoState::Error, "failed", "error"),
        ] {
            assert_eq!(
                serde_json::to_string(&state).unwrap(),
                format!("\"{name}\"")
            );
            assert_eq!(state.as_str(), page);
        }
        for (state, name, page) in [
            (CycleOutcome::Running, "running", "running"),
            (CycleOutcome::Ok, "ok", "ok"),
            (CycleOutcome::Partial, "partial", "partial"),
            (CycleOutcome::Errors, "failed", "errors"),
            (CycleOutcome::Skipped, "skipped", "skipped"),
        ] {
            assert_eq!(
                serde_json::to_string(&state).unwrap(),
                format!("\"{name}\"")
            );
            assert_eq!(state.as_str(), page);
        }
    }

    #[test]
    fn reload_keeps_the_process_start() {
        let status = new_shared();
        init(&status, &[resolved("a", "/a")], Instant::now());
        let first = at(1, 0);
        status.write().unwrap().process.started_at = Some(first);

        init(&status, &[resolved("a", "/a")], Instant::now());

        assert_eq!(status.read().unwrap().process.started_at, Some(first));
    }

    #[test]
    fn next_run_survives_a_row_rebuild() {
        let previous = vec![row("nas", Some(at(13, 0))), row("remote", None)];
        let mut rebuilt = vec![row("remote", Some(at(1, 0))), row("nas", None)];

        carry_over_next_runs(&mut rebuilt, &previous);

        assert_eq!(rebuilt[0].next_run_at, None);
        assert_eq!(rebuilt[1].next_run_at, Some(at(13, 0)));
    }

    #[test]
    fn unknown_repo_keeps_the_placeholder() {
        // A repo added by a SIGHUP reload has no previous row; the daemon loop
        // stamps it within a tick.
        let previous = vec![row("nas", Some(at(13, 0)))];
        let mut rebuilt = vec![row("fresh", None)];

        carry_over_next_runs(&mut rebuilt, &previous);

        assert_eq!(rebuilt[0].next_run_at, None);
    }

    #[test]
    fn record_cycle_end_keeps_the_partial_rule() {
        let repos = [resolved("a", "/a"), resolved("b", "/b")];
        let status = new_shared();
        // A failed repository's partial backup does not count.
        let mixed = report(vec![
            vec![
                (CycleStep::Backup, StepOutcome::Partial),
                (CycleStep::Prune, failed(StepErrorKind::Other, "x")),
            ],
            vec![(CycleStep::Backup, StepOutcome::Ok)],
        ]);
        record_cycle_end(&status, Duration::from_secs(5), &mixed, &repos);
        {
            let s = status.read().unwrap();
            assert!(s.last_cycle.had_error);
            assert!(!s.last_cycle.had_partial);
            assert_eq!(s.last_cycle.status, Some(CycleOutcome::Errors));
            assert_eq!(s.last_cycle.duration_seconds, Some(5));
            assert_eq!(s.last_cycle.repos.len(), 2);
            assert_eq!(s.last_cycle.repos[1].name, "b");
            assert_eq!(s.last_cycle.repos[1].url, "/b");
        }
        let partial = report(vec![vec![(CycleStep::Backup, StepOutcome::Partial)]]);
        record_cycle_end(&status, Duration::from_secs(1), &partial, &repos);
        let s = status.read().unwrap();
        assert!(!s.last_cycle.had_error);
        assert!(s.last_cycle.had_partial);
        assert_eq!(s.last_cycle.status, Some(CycleOutcome::Partial));
    }

    #[test]
    fn record_cycle_start_keeps_the_last_steps() {
        let repos = [resolved("a", "/a")];
        let status = new_shared();
        let done = report(vec![vec![(CycleStep::Backup, StepOutcome::Ok)]]);
        record_cycle_end(&status, Duration::from_secs(1), &done, &repos);

        record_cycle_start(&status);

        let s = status.read().unwrap();
        assert_eq!(s.last_cycle.status, Some(CycleOutcome::Running));
        assert_eq!(s.last_cycle.finished_at, None);
        assert_eq!(s.last_cycle.duration_seconds, None);
        assert_eq!(s.last_cycle.repos.len(), 1);
        assert_eq!(s.last_cycle.repos[0].steps[0].status, "ok");
    }

    /// The step entry's `status` and `kind` are the names `--json` prints
    /// for the same outcome.
    #[test]
    fn step_entries_agree_with_json_outcomes() {
        let cases = [
            (StepOutcome::Ok, "ok", None),
            (StepOutcome::Partial, "partial", None),
            (
                StepOutcome::Skipped(SkipReason::new(SkipKind::NoRetentionRules, "m")),
                "skipped",
                Some("no_retention_rules"),
            ),
            (
                failed(StepErrorKind::RepoNotFound, "m"),
                "failed",
                Some("repo_not_found"),
            ),
        ];
        for (outcome, status, kind) in cases {
            let json = serde_json::to_value(&outcome).unwrap();
            let entry = StepEntry::new(CycleStep::Prune, &outcome);
            assert_eq!(entry.status, status);
            assert_eq!(entry.kind.as_deref(), kind);
            assert_eq!(json["status"], status);
            assert_eq!(json.get("kind").and_then(|k| k.as_str()), kind);
            assert_eq!(
                serde_json::to_string(&entry).unwrap(),
                match kind {
                    Some(k) => format!(r#"{{"step":"prune","status":"{status}","kind":"{k}"}}"#),
                    None => format!(r#"{{"step":"prune","status":"{status}","kind":null}}"#),
                }
            );
        }
    }

    #[test]
    fn status_json_leaves_out_step_messages() {
        let secret = "curl -fsS https://hc-ping.com/SECRET-TOKEN/fail";
        let repos = [resolved("a", "/a")];
        let status = new_shared();
        let cycle = report(vec![vec![(
            CycleStep::Backup,
            failed(
                StepErrorKind::Hook,
                &format!("hook error: hook '{secret}' exited with 1"),
            ),
        )]]);
        record_cycle_end(&status, Duration::from_secs(1), &cycle, &repos);

        let json = serde_json::to_string(&*status.read().unwrap()).unwrap();
        assert!(!json.contains("SECRET-TOKEN"), "{json}");
        assert!(
            json.contains(r#"{"step":"backup","status":"failed","kind":"hook"}"#),
            "{json}"
        );
    }

    #[test]
    fn status_json_is_schema_1() {
        let s = DaemonStatus {
            schema: SCHEMA,
            process: ProcessInfo {
                hostname: "host1".into(),
                pid: 7,
                version: "1.2.3".into(),
                started_at: Some(at(9, 0)),
                uptime_seconds: 3900,
                next_run_at: Some(at(12, 0)),
            },
            schedule_brief: "1h".into(),
            repos: vec![
                RepoInfo {
                    name: "fresh".into(),
                    url: "/fresh".into(),
                    snapshot_count: None,
                    last_snapshot_at: None,
                    unique_stored_size: None,
                    next_run_at: None,
                    status: RepoState::Pending,
                    refreshed_at: None,
                },
                RepoInfo {
                    name: "nas".into(),
                    url: "/nas".into(),
                    snapshot_count: Some(3),
                    last_snapshot_at: Some(at(10, 0)),
                    unique_stored_size: Some(1024),
                    next_run_at: Some(at(12, 0)),
                    status: RepoState::Ok,
                    refreshed_at: Some(at(10, 5)),
                },
            ],
            recent_snapshots: vec![SnapshotRow {
                repo_name: "nas".into(),
                snapshot: record("a1b2c3d4", at(10, 0)),
            }],
            sources: vec![SourceInfo {
                source_label: "home".into(),
                source_paths: vec!["/home/u".into()],
                exclude: vec![".cache".into()],
                exclude_if_present: vec![],
                target_repos: vec![],
                options: vec!["git_ignore".into()],
                hooks: vec![],
                retention: vec!["keep_last: 7".into()],
                command_dumps: vec![],
            }],
            last_cycle: CycleSummary {
                started_at: Some(at(10, 0)),
                finished_at: Some(at(10, 3)),
                status: Some(CycleOutcome::Ok),
                duration_seconds: Some(180),
                had_error: false,
                had_partial: false,
                repos: vec![RepoCycleSummary {
                    name: "nas".into(),
                    url: "/nas".into(),
                    steps: vec![
                        StepEntry::new(CycleStep::Backup, &StepOutcome::Ok),
                        StepEntry::new(
                            CycleStep::Check,
                            &StepOutcome::Skipped(SkipReason::new(
                                SkipKind::CheckNotDue,
                                "check not due",
                            )),
                        ),
                    ],
                }],
            },
        };
        assert_eq!(
            serde_json::to_string(&s).unwrap(),
            concat!(
                r#"{"schema":1,"#,
                r#""process":{"hostname":"host1","pid":7,"version":"1.2.3","started_at":"2026-07-25T09:00:00Z","uptime_seconds":3900,"next_run_at":"2026-07-25T12:00:00Z"},"#,
                r#""schedule_brief":"1h","#,
                r#""repos":[{"name":"fresh","url":"/fresh","snapshot_count":null,"last_snapshot_at":null,"unique_stored_size":null,"next_run_at":null,"status":"pending","refreshed_at":null},"#,
                r#"{"name":"nas","url":"/nas","snapshot_count":3,"last_snapshot_at":"2026-07-25T10:00:00Z","unique_stored_size":1024,"next_run_at":"2026-07-25T12:00:00Z","status":"ok","refreshed_at":"2026-07-25T10:05:00Z"}],"#,
                r#""recent_snapshots":[{"repo_name":"nas","name":"a1b2c3d4","created_at":"2026-07-25T10:00:00Z","hostname":"h","source_label":"l","source_paths":["/p"],"stats":null}],"#,
                r#""sources":[{"source_label":"home","source_paths":["/home/u"],"exclude":[".cache"],"exclude_if_present":[],"target_repos":[],"options":["git_ignore"],"hooks":[],"retention":["keep_last: 7"],"command_dumps":[]}],"#,
                r#""last_cycle":{"started_at":"2026-07-25T10:00:00Z","finished_at":"2026-07-25T10:03:00Z","status":"ok","duration_seconds":180,"had_error":false,"had_partial":false,"#,
                r#""repos":[{"name":"nas","url":"/nas","steps":[{"step":"backup","status":"ok","kind":null},{"step":"check","status":"skipped","kind":"check_not_due"}]}]}}"#,
            )
        );
    }
}
