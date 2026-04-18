use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering};

use rand::RngCore;

use crate::commands;
use crate::compress::Compression;
use crate::config::{ResolvedRepo, SourceEntry, VykarConfig};
use crate::repo::manifest::SnapshotEntry;
use crate::snapshot::item::Item;
use vykar_types::error::{Result, VykarError};

#[derive(Debug, Clone)]
pub struct BackupSourceResult {
    pub source_label: String,
    pub snapshot_name: String,
    pub source_paths: Vec<String>,
    pub stats: crate::snapshot::SnapshotStats,
}

#[derive(Debug, Clone, Default)]
pub struct BackupRunReport {
    pub created: Vec<BackupSourceResult>,
}

#[derive(Debug, Clone)]
pub struct RestoreRequest {
    pub snapshot_name: String,
    pub destination: String,
    pub pattern: Option<String>,
}

// ── Hook-aware backup event types ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum HookScope {
    Repo,
    Source { label: String },
}

/// Events from a hook-aware backup run (backup-only path).
#[derive(Debug, Clone)]
pub enum BackupRunEvent {
    /// Pure backup engine event (files, stats, sources).
    Backup(commands::backup::BackupProgressEvent),
    /// A non-fatal hook failure (tracing::warn! already fired — this adds GUI visibility).
    HookWarning { scope: HookScope, warning: String },
}

// ── Full-cycle types ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleStep {
    Backup,
    Prune,
    Compact,
    Check,
}

impl CycleStep {
    pub fn command_name(&self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::Prune => "prune",
            Self::Compact => "compact",
            Self::Check => "check",
        }
    }
}

#[derive(Debug, Clone)]
pub enum StepOutcome {
    Ok,
    /// Backup completed but some sources had soft errors.
    Partial,
    Skipped(String),
    Failed(String),
}

impl StepOutcome {
    /// Ok and Partial are both "success" — after-hooks run, subsequent steps proceed.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Ok | Self::Partial)
    }

    pub fn error_msg(&self) -> Option<&str> {
        match self {
            Self::Failed(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CycleEvent {
    StepStarted(CycleStep),
    StepFinished(CycleStep, StepOutcome),
    Backup(commands::backup::BackupProgressEvent),
    Check(commands::check::CheckProgressEvent),
    /// Non-fatal hook failure (tracing::warn! already fired — this adds GUI visibility).
    HookWarning {
        step: CycleStep,
        scope: HookScope,
        warning: String,
    },
}

pub struct FullCycleResult {
    pub backup_report: Option<BackupRunReport>,
    pub prune_stats: Option<commands::prune::PruneStats>,
    pub compact_stats: Option<commands::compact::CompactStats>,
    pub check_result: Option<commands::check::CheckResult>,
    pub steps: Vec<(CycleStep, StepOutcome)>,
}

impl FullCycleResult {
    /// Any step has Failed outcome.
    pub fn has_failures(&self) -> bool {
        self.steps
            .iter()
            .any(|(_step, o)| matches!(o, StepOutcome::Failed(_)))
    }

    /// Backup step completed with Partial outcome.
    pub fn had_partial(&self) -> bool {
        self.steps
            .iter()
            .any(|(step, o)| matches!((step, o), (CycleStep::Backup, StepOutcome::Partial)))
    }
}

fn generate_snapshot_name() -> String {
    let mut buf = [0u8; 4];
    rand::rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

// ── Internal: source iteration + source hooks ─────────────────────────────

/// Run backup for each source, executing source-level hooks.
/// Emits BackupRunEvent for progress and hook lifecycle.
fn run_backup_sources(
    config: &VykarConfig,
    sources: &[SourceEntry],
    passphrase: Option<&str>,
    repo_label: Option<&str>,
    shutdown: Option<&AtomicBool>,
    verbose: bool,
    on_event: &mut Option<&mut dyn FnMut(BackupRunEvent)>,
) -> Result<BackupRunReport> {
    if sources.is_empty() {
        return Err(VykarError::Config(
            "no sources configured for this repository".into(),
        ));
    }

    let compression =
        Compression::from_algorithm(config.compression.algorithm, config.compression.zstd_level);

    let mut report = BackupRunReport::default();

    for source in sources {
        let snapshot_name = generate_snapshot_name();

        let run_backup = |on_event: &mut Option<&mut dyn FnMut(BackupRunEvent)>| -> Result<_> {
            let req = commands::backup::BackupRequest {
                snapshot_name: &snapshot_name,
                passphrase,
                source_paths: &source.paths,
                source_label: &source.label,
                exclude_patterns: &source.exclude,
                exclude_if_present: &source.exclude_if_present,
                one_file_system: source.one_file_system,
                git_ignore: source.git_ignore,
                xattrs_enabled: source.xattrs_enabled,
                compression,
                command_dumps: &source.command_dumps,
                verbose,
            };

            if let Some(ref mut cb) = on_event {
                let mut backup_cb = |bpe| {
                    cb(BackupRunEvent::Backup(bpe));
                };
                commands::backup::run_with_progress(config, req, Some(&mut backup_cb), shutdown)
            } else {
                commands::backup::run_with_progress(config, req, None, shutdown)
            }
        };

        let outcome = if source.hooks.has_any() {
            let mut ctx = crate::hooks::HookContext {
                command: "backup".to_string(),
                repository: config.repository.url.clone(),
                label: repo_label.map(|s| s.to_string()),
                error: None,
                source_label: Some(source.label.clone()),
                source_paths: Some(source.paths.clone()),
                warnings: Vec::new(),
            };
            let scope = HookScope::Source {
                label: source.label.clone(),
            };

            let result =
                crate::hooks::run_source_hooks(&source.hooks, &mut ctx, || run_backup(on_event));

            // Drain accumulated warnings into events (tracing already fired)
            drain_backup_warnings(&mut ctx, on_event, &scope);

            result?
        } else {
            run_backup(on_event)?
        };

        report.created.push(BackupSourceResult {
            source_label: source.label.clone(),
            snapshot_name,
            source_paths: source.paths.clone(),
            stats: outcome.stats,
        });
    }

    Ok(report)
}

/// Drain accumulated warnings from HookContext into BackupRunEvent.
fn drain_backup_warnings(
    ctx: &mut crate::hooks::HookContext,
    on_event: &mut Option<&mut dyn FnMut(BackupRunEvent)>,
    scope: &HookScope,
) {
    if let Some(ref mut cb) = on_event {
        for warning in ctx.warnings.drain(..) {
            cb(BackupRunEvent::HookWarning {
                scope: scope.clone(),
                warning,
            });
        }
    }
}

// ── Public hook-aware backup API ──────────────────────────────────────────

/// Run backup with full hook lifecycle (repo + source hooks).
/// Used by: CLI `vykar backup`, GUI RunBackupRepo, GUI RunBackupSource.
pub fn run_backup_selection(
    repo: &ResolvedRepo,
    sources: &[SourceEntry],
    passphrase: Option<&str>,
    shutdown: Option<&AtomicBool>,
    verbose: bool,
    on_event: Option<&mut dyn FnMut(BackupRunEvent)>,
) -> Result<BackupRunReport> {
    let config = &repo.config;
    let label = repo.label.as_deref();
    let has_hooks = !repo.global_hooks.is_empty() || !repo.repo_hooks.is_empty();
    let mut on_event = on_event;

    if !has_hooks {
        return run_backup_sources(
            config,
            sources,
            passphrase,
            label,
            shutdown,
            verbose,
            &mut on_event,
        );
    }

    let mut ctx = crate::hooks::HookContext {
        command: "backup".to_string(),
        repository: config.repository.url.clone(),
        label: repo.label.clone(),
        error: None,
        source_label: None,
        source_paths: None,
        warnings: Vec::new(),
    };

    // 1. Before hooks (fatal — run_before returns Err on failure)
    if let Err(e) = crate::hooks::run_before(&repo.global_hooks, &repo.repo_hooks, &mut ctx) {
        ctx.error = Some(e.to_string());
        crate::hooks::run_after_or_failed(&repo.global_hooks, &repo.repo_hooks, &mut ctx, false);
        crate::hooks::run_finally(&repo.global_hooks, &repo.repo_hooks, &mut ctx);
        drain_backup_warnings(&mut ctx, &mut on_event, &HookScope::Repo);
        return Err(e);
    }
    drain_backup_warnings(&mut ctx, &mut on_event, &HookScope::Repo);

    // 2. Backup (source hooks handled inside run_backup_sources)
    let result = run_backup_sources(
        config,
        sources,
        passphrase,
        label,
        shutdown,
        verbose,
        &mut on_event,
    );

    // 3. After/Failed + Finally
    let success = result.is_ok();
    if let Err(ref e) = result {
        if ctx.error.is_none() {
            ctx.error = Some(e.to_string());
        }
    }
    crate::hooks::run_after_or_failed(&repo.global_hooks, &repo.repo_hooks, &mut ctx, success);
    crate::hooks::run_finally(&repo.global_hooks, &repo.repo_hooks, &mut ctx);
    drain_backup_warnings(&mut ctx, &mut on_event, &HookScope::Repo);

    result
}

// ── Public hook wrapper for CLI standalone commands ───────────────────────

/// Wrap an action with repo-level hooks. For CLI standalone commands
/// (prune, compact, check) that aren't part of the full cycle.
pub fn run_command_with_hooks<F, T>(
    repo: &ResolvedRepo,
    command: &str,
    action: F,
) -> vykar_types::error::Result<T>
where
    F: FnOnce() -> vykar_types::error::Result<T>,
{
    let mut ctx = crate::hooks::HookContext {
        command: command.to_string(),
        repository: repo.config.repository.url.clone(),
        label: repo.label.clone(),
        error: None,
        source_label: None,
        source_paths: None,
        warnings: Vec::new(),
    };
    crate::hooks::run_with_hooks(&repo.global_hooks, &repo.repo_hooks, &mut ctx, action)
}

// ── Full-cycle with internalized hooks ───────────────────────────────────

/// Run the full backup cycle: backup → prune → compact → check.
/// Hooks are run internally — callers don't need to handle them.
pub fn run_full_cycle_for_repo(
    repo: &ResolvedRepo,
    passphrase: Option<&str>,
    shutdown: Option<&AtomicBool>,
    verbose: bool,
    source_filter: &[String],
    on_event: &mut dyn FnMut(CycleEvent),
) -> FullCycleResult {
    let config = &repo.config;
    let sources = &repo.sources;
    let shutting_down = |s: Option<&AtomicBool>| s.is_some_and(|f| f.load(Ordering::SeqCst));

    let mut steps: Vec<(CycleStep, StepOutcome)> = Vec::new();
    let mut backup_report: Option<BackupRunReport> = None;
    let mut prune_stats: Option<commands::prune::PruneStats> = None;
    let mut compact_stats: Option<commands::compact::CompactStats> = None;
    let mut check_result: Option<commands::check::CheckResult> = None;

    // 1. Backup
    if !shutting_down(shutdown) {
        match resolve_cycle_sources(sources, source_filter) {
            Err(e) => emit_failed(CycleStep::Backup, &e, on_event, &mut steps),
            Ok(effective_sources) => {
                backup_report = run_hooked_step(
                    CycleStep::Backup,
                    repo,
                    on_event,
                    &mut steps,
                    |evt| {
                        run_backup_sources(
                            config,
                            &effective_sources,
                            passphrase,
                            repo.label.as_deref(),
                            shutdown,
                            verbose,
                            &mut Some(&mut |bpe: BackupRunEvent| match bpe {
                                BackupRunEvent::Backup(e) => evt(CycleEvent::Backup(e)),
                                BackupRunEvent::HookWarning { scope, warning } => {
                                    evt(CycleEvent::HookWarning {
                                        step: CycleStep::Backup,
                                        scope,
                                        warning,
                                    });
                                }
                            }),
                        )
                    },
                    |report| {
                        if report.created.iter().any(|s| s.stats.errors > 0) {
                            StepOutcome::Partial
                        } else {
                            StepOutcome::Ok
                        }
                    },
                );
            }
        }
    }

    let backup_ok = steps
        .iter()
        .any(|(s, o)| matches!(s, CycleStep::Backup) && o.is_success());

    // 2. Prune
    if !shutting_down(shutdown) {
        let has_retention = config.retention.has_any_rule()
            || sources
                .iter()
                .any(|s| s.retention.as_ref().is_some_and(|r| r.has_any_rule()));

        if !has_retention {
            emit_skipped(CycleStep::Prune, "no retention rules", on_event, &mut steps);
        } else if !backup_ok {
            emit_skipped(CycleStep::Prune, "backup failed", on_event, &mut steps);
        } else {
            prune_stats = run_hooked_step(
                CycleStep::Prune,
                repo,
                on_event,
                &mut steps,
                |_evt| {
                    commands::prune::run(
                        config,
                        passphrase,
                        false,
                        false,
                        sources,
                        source_filter,
                        shutdown,
                    )
                    .map(|(stats, _)| stats)
                },
                |_| StepOutcome::Ok,
            );
        }
    }

    // 3. Compact
    if !shutting_down(shutdown) {
        if !backup_ok {
            emit_skipped(CycleStep::Compact, "backup failed", on_event, &mut steps);
        } else {
            compact_stats = run_hooked_step(
                CycleStep::Compact,
                repo,
                on_event,
                &mut steps,
                |_evt| {
                    commands::compact::run(
                        config,
                        passphrase,
                        config.compact.threshold,
                        None,
                        false,
                        shutdown,
                    )
                },
                |_| StepOutcome::Ok,
            );
        }
    }

    // 4. Check (metadata-only, with sampling support)
    if !shutting_down(shutdown) {
        let check_max_percent = config.check.max_percent;
        check_result = run_hooked_step(
            CycleStep::Check,
            repo,
            on_event,
            &mut steps,
            |evt| {
                commands::check::run_with_progress(
                    config,
                    passphrase,
                    false,
                    false,
                    Some(&mut |check_evt| evt(CycleEvent::Check(check_evt))),
                    check_max_percent,
                    true, // record_state: daemon/GUI updates the full_every timer
                )
            },
            |result| {
                if result.skipped {
                    StepOutcome::Skipped("check not due".into())
                } else if result.errors.is_empty() {
                    StepOutcome::Ok
                } else {
                    StepOutcome::Failed(format!("check found {} error(s)", result.errors.len()))
                }
            },
        );
    }

    FullCycleResult {
        backup_report,
        prune_stats,
        compact_stats,
        check_result,
        steps,
    }
}

/// Run a single cycle step with the full hook lifecycle.
/// Returns Some(value) on success/partial, None on failure or before-hook abort.
fn run_hooked_step<T>(
    step: CycleStep,
    repo: &ResolvedRepo,
    on_event: &mut dyn FnMut(CycleEvent),
    steps: &mut Vec<(CycleStep, StepOutcome)>,
    action: impl FnOnce(&mut dyn FnMut(CycleEvent)) -> Result<T>,
    classify: impl FnOnce(&T) -> StepOutcome,
) -> Option<T> {
    let has_hooks = !repo.global_hooks.is_empty() || !repo.repo_hooks.is_empty();
    let mut hook_ctx = has_hooks.then(|| crate::hooks::HookContext {
        command: step.command_name().to_string(),
        repository: repo.config.repository.url.clone(),
        label: repo.label.clone(),
        error: None,
        source_label: None,
        source_paths: None,
        warnings: Vec::new(),
    });

    on_event(CycleEvent::StepStarted(step));

    // Before hooks (fatal — abort step on failure)
    if let Some(ref mut ctx) = hook_ctx {
        if let Err(e) = crate::hooks::run_before(&repo.global_hooks, &repo.repo_hooks, ctx) {
            ctx.error = Some(e.to_string());
            crate::hooks::run_after_or_failed(&repo.global_hooks, &repo.repo_hooks, ctx, false);
            crate::hooks::run_finally(&repo.global_hooks, &repo.repo_hooks, ctx);
            drain_cycle_warnings(ctx, step, on_event);
            let outcome = StepOutcome::Failed(e.to_string());
            on_event(CycleEvent::StepFinished(step, outcome.clone()));
            steps.push((step, outcome));
            return None;
        }
        drain_cycle_warnings(ctx, step, on_event);
    }

    // Action
    let result = action(on_event);

    // Classify outcome + After/Failed/Finally hooks
    let (outcome, value) = match result {
        std::result::Result::Ok(val) => {
            let o = classify(&val);
            (o, Some(val))
        }
        Err(e) => (StepOutcome::Failed(e.to_string()), None),
    };

    if let Some(ref mut ctx) = hook_ctx {
        // Skipped is not a failure — treat it as success for the hook lifecycle
        // so that `after` hooks fire (not `failed`), and `finally` always runs.
        let success = outcome.is_success() || matches!(outcome, StepOutcome::Skipped(_));
        if let Some(msg) = outcome.error_msg() {
            if ctx.error.is_none() {
                ctx.error = Some(msg.to_string());
            }
        }
        crate::hooks::run_after_or_failed(&repo.global_hooks, &repo.repo_hooks, ctx, success);
        crate::hooks::run_finally(&repo.global_hooks, &repo.repo_hooks, ctx);
        drain_cycle_warnings(ctx, step, on_event);
    }

    on_event(CycleEvent::StepFinished(step, outcome.clone()));
    steps.push((step, outcome));
    value
}

/// Apply source_filter to the configured sources.
/// Returns the original slice when filter is empty, or a filtered owned vec.
fn resolve_cycle_sources<'a>(
    sources: &'a [SourceEntry],
    source_filter: &[String],
) -> std::result::Result<Cow<'a, [SourceEntry]>, String> {
    if source_filter.is_empty() {
        return Ok(Cow::Borrowed(sources));
    }
    crate::config::select_sources(sources, source_filter)
        .map(|selected| Cow::Owned(selected.into_iter().cloned().collect()))
}

fn emit_failed(
    step: CycleStep,
    error: &str,
    on_event: &mut dyn FnMut(CycleEvent),
    steps: &mut Vec<(CycleStep, StepOutcome)>,
) {
    let outcome = StepOutcome::Failed(error.into());
    on_event(CycleEvent::StepStarted(step));
    on_event(CycleEvent::StepFinished(step, outcome.clone()));
    steps.push((step, outcome));
}

fn emit_skipped(
    step: CycleStep,
    reason: &str,
    on_event: &mut dyn FnMut(CycleEvent),
    steps: &mut Vec<(CycleStep, StepOutcome)>,
) {
    let outcome = StepOutcome::Skipped(reason.into());
    on_event(CycleEvent::StepStarted(step));
    on_event(CycleEvent::StepFinished(step, outcome.clone()));
    steps.push((step, outcome));
}

fn drain_cycle_warnings(
    ctx: &mut crate::hooks::HookContext,
    step: CycleStep,
    on_event: &mut dyn FnMut(CycleEvent),
) {
    for warning in ctx.warnings.drain(..) {
        on_event(CycleEvent::HookWarning {
            step,
            scope: HookScope::Repo,
            warning,
        });
    }
}

// ── Remaining public APIs (unchanged) ────────────────────────────────────

pub fn list_snapshots(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<Vec<SnapshotEntry>> {
    commands::list::list_snapshots(config, passphrase)
}

pub fn list_snapshots_with_stats(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<Vec<(SnapshotEntry, Option<crate::snapshot::SnapshotStats>)>> {
    commands::list::list_snapshots_with_stats(config, passphrase)
}

pub fn list_snapshot_items(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<Vec<Item>> {
    commands::list::list_snapshot_items(config, passphrase, snapshot_name)
}

pub fn list_snapshot_items_with_source_paths(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<(Vec<Item>, Vec<String>)> {
    commands::list::list_snapshot_items_with_source_paths(config, passphrase, snapshot_name)
}

pub fn restore_snapshot(
    config: &VykarConfig,
    passphrase: Option<&str>,
    req: &RestoreRequest,
) -> Result<commands::restore::RestoreStats> {
    commands::restore::run(
        config,
        passphrase,
        &req.snapshot_name,
        &req.destination,
        req.pattern.as_deref(),
        config.xattrs.enabled,
    )
}

pub fn restore_selected(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    destination: &str,
    selected_paths: &std::collections::HashSet<String>,
) -> Result<commands::restore::RestoreStats> {
    commands::restore::run_selected(
        config,
        passphrase,
        snapshot_name,
        destination,
        selected_paths,
        config.xattrs.enabled,
    )
}

pub fn check_repo(
    config: &VykarConfig,
    passphrase: Option<&str>,
    verify_data: bool,
) -> Result<commands::check::CheckResult> {
    commands::check::run(config, passphrase, verify_data, false)
}

pub fn check_repo_with_progress(
    config: &VykarConfig,
    passphrase: Option<&str>,
    verify_data: bool,
    progress: &mut dyn FnMut(commands::check::CheckProgressEvent),
) -> Result<commands::check::CheckResult> {
    commands::check::run_with_progress(
        config,
        passphrase,
        verify_data,
        false,
        Some(progress),
        100,
        false,
    )
}

pub fn delete_snapshot(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<commands::delete::DeleteStats> {
    let mut results = commands::delete::run(config, passphrase, &[snapshot_name], false, None)?;
    Ok(results.remove(0))
}
