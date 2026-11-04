#![allow(
    clippy::doc_markdown,
    clippy::if_not_else,
    clippy::implicit_hasher,
    clippy::missing_errors_doc,
    clippy::redundant_closure_for_method_calls,
    clippy::too_many_lines
)]

use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering};

use rand::Rng;
use zeroize::Zeroizing;

use crate::commands;
use crate::compress::Compression;
use crate::config::{ResolvedRepo, SourceEntry, VykarConfig};
use vykar_types::error::{Result, VykarError};

#[derive(Debug, Clone)]
pub struct BackupSourceResult {
    pub source_label: String,
    pub snapshot_name: String,
    pub source_paths: Vec<String>,
    pub stats: crate::snapshot::SnapshotStats,
    /// Cloud-only (macOS `FileProvider`) files omitted from the snapshot. Run-
    /// local — not part of `stats`, and never counted as an error. See
    /// `BackupOutcome`.
    pub dataless_files_skipped: u64,
    /// Cloud-only directories that could not be listed; each omits a subtree.
    pub dataless_dirs_skipped: u64,
}

/// A source whose `before` hook asked for a skip.
#[derive(Debug, Clone)]
pub struct SkippedSource {
    pub label: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default)]
pub struct BackupRunReport {
    pub created: Vec<BackupSourceResult>,
    pub skipped: Vec<SkippedSource>,
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
    /// Non-fatal warning from a cycle step (e.g. prune post-commit refcount
    /// cleanup failed). `tracing::warn!` has already fired — this is for GUI
    /// visibility since the GUI does not subscribe to tracing.
    StepWarning {
        step: CycleStep,
        message: String,
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

    /// Why the backup step was skipped, if it was. Only a hook or a dismissed
    /// passphrase prompt does this, and the rest of the cycle is skipped with it.
    pub fn backup_skipped(&self) -> Option<&str> {
        self.steps.iter().find_map(|(step, o)| match (step, o) {
            (CycleStep::Backup, StepOutcome::Skipped(reason)) => Some(reason.as_str()),
            _ => None,
        })
    }
}

/// Resolves a cycle's passphrase on first use, at most once, so that nothing
/// (including a prompt or a connection) happens before the `before` hooks pass.
struct LazyPassphrase<'a> {
    resolve: &'a mut dyn FnMut() -> Result<Option<Zeroizing<String>>>,
    state: PassphraseState,
}

enum PassphraseState {
    Unresolved,
    Resolved(Option<Zeroizing<String>>),
    /// The error went to the step that asked first; `VykarError` is not `Clone`.
    Failed,
}

impl<'a> LazyPassphrase<'a> {
    fn new(resolve: &'a mut dyn FnMut() -> Result<Option<Zeroizing<String>>>) -> Self {
        Self {
            resolve,
            state: PassphraseState::Unresolved,
        }
    }

    fn failed(&self) -> bool {
        matches!(self.state, PassphraseState::Failed)
    }

    fn get(&mut self) -> Result<Option<&str>> {
        if matches!(self.state, PassphraseState::Unresolved) {
            match (self.resolve)() {
                Ok(pass) => self.state = PassphraseState::Resolved(pass),
                Err(e) => {
                    self.state = PassphraseState::Failed;
                    return Err(e);
                }
            }
        }
        match &self.state {
            PassphraseState::Resolved(pass) => Ok(pass.as_deref().map(String::as_str)),
            _ => Err(VykarError::Other("passphrase unavailable".into())),
        }
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

        let result = if source.hooks.has_any() {
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

            result
        } else {
            run_backup(on_event)
        };

        let outcome = match result {
            Ok(outcome) => outcome,
            Err(VykarError::Skipped(reason)) => {
                report.skipped.push(SkippedSource {
                    label: source.label.clone(),
                    reason,
                });
                continue;
            }
            Err(e) => return Err(e),
        };

        report.created.push(BackupSourceResult {
            source_label: source.label.clone(),
            snapshot_name,
            source_paths: source.paths.clone(),
            stats: outcome.stats,
            dataless_files_skipped: outcome.dataless_files_skipped,
            dataless_dirs_skipped: outcome.dataless_dirs_skipped,
        });
    }

    // Nothing ran, so to the repo-level lifecycle this is a skip, not a success.
    if report.created.is_empty() && !report.skipped.is_empty() {
        let reasons = report
            .skipped
            .iter()
            .map(|s| format!("source '{}': {}", s.label, s.reason))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(VykarError::Skipped(reasons));
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
    let mut on_event = on_event;
    let mut ctx = crate::hooks::HookContext {
        command: "backup".to_string(),
        repository: config.repository.url.clone(),
        label: repo.label.clone(),
        error: None,
        source_label: None,
        source_paths: None,
        warnings: Vec::new(),
    };

    // Source hooks run inside run_backup_sources.
    let result =
        crate::hooks::run_with_hooks(&repo.global_hooks, &repo.repo_hooks, &mut ctx, || {
            run_backup_sources(
                config,
                sources,
                passphrase,
                repo.label.as_deref(),
                shutdown,
                verbose,
                &mut on_event,
            )
        });
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
    run_full_cycle_with_passphrase_source(
        repo,
        &mut || Ok(passphrase.map(|p| Zeroizing::new(p.to_string()))),
        shutdown,
        verbose,
        source_filter,
        on_event,
    )
}

/// [`run_full_cycle_for_repo`] for callers whose passphrase step is expensive or
/// connects to the repository. `resolve` runs inside the first action that needs
/// it, after that step's `before` hooks pass, and at most once per cycle: a
/// failure is not retried. Returning `Err(VykarError::Skipped)` skips the step
/// without reporting an error.
pub fn run_full_cycle_with_passphrase_source(
    repo: &ResolvedRepo,
    resolve: &mut dyn FnMut() -> Result<Option<Zeroizing<String>>>,
    shutdown: Option<&AtomicBool>,
    verbose: bool,
    source_filter: &[String],
    on_event: &mut dyn FnMut(CycleEvent),
) -> FullCycleResult {
    let mut passphrase = LazyPassphrase::new(resolve);
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
            Err(e) => emit_terminal(
                CycleStep::Backup,
                StepOutcome::Failed(e),
                on_event,
                &mut steps,
            ),
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
                            passphrase.get()?,
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
    let backup_skipped = steps
        .iter()
        .any(|(s, o)| matches!((s, o), (CycleStep::Backup, StepOutcome::Skipped(_))));

    // A skipped backup must not connect at all: the skip usually means the
    // repository is unreachable, so the later steps would only produce errors.
    let backup_skip_outcome = || StepOutcome::Skipped("backup skipped".into());

    // 2. Prune
    if !shutting_down(shutdown) {
        let has_retention = config.retention.has_any_rule()
            || sources
                .iter()
                .any(|s| s.retention.as_ref().is_some_and(|r| r.has_any_rule()));

        if backup_skipped {
            emit_terminal(
                CycleStep::Prune,
                backup_skip_outcome(),
                on_event,
                &mut steps,
            );
        } else if !has_retention {
            emit_terminal(
                CycleStep::Prune,
                StepOutcome::Skipped("no retention rules".into()),
                on_event,
                &mut steps,
            );
        } else if !backup_ok {
            emit_terminal(
                CycleStep::Prune,
                StepOutcome::Skipped("backup failed".into()),
                on_event,
                &mut steps,
            );
        } else {
            prune_stats = run_hooked_step(
                CycleStep::Prune,
                repo,
                on_event,
                &mut steps,
                |evt| {
                    let stats = commands::prune::run(
                        config,
                        passphrase.get()?,
                        false,
                        false,
                        sources,
                        source_filter,
                        shutdown,
                    )
                    .map(|(stats, _)| stats)?;
                    // Surface post-commit warnings as cycle events so GUI/CLI
                    // consumers can log them as persistent entries. tracing
                    // already fired inside commands::prune::run.
                    for w in &stats.warnings {
                        evt(CycleEvent::StepWarning {
                            step: CycleStep::Prune,
                            message: w.clone(),
                        });
                    }
                    Ok(stats)
                },
                |_| StepOutcome::Ok,
            );
        }
    }

    // 3. Compact
    if !shutting_down(shutdown) {
        if backup_skipped {
            emit_terminal(
                CycleStep::Compact,
                backup_skip_outcome(),
                on_event,
                &mut steps,
            );
        } else if !backup_ok {
            emit_terminal(
                CycleStep::Compact,
                StepOutcome::Skipped("backup failed".into()),
                on_event,
                &mut steps,
            );
        } else {
            compact_stats = run_hooked_step(
                CycleStep::Compact,
                repo,
                on_event,
                &mut steps,
                |_evt| {
                    commands::compact::run(
                        config,
                        passphrase.get()?,
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
        if backup_skipped {
            emit_terminal(
                CycleStep::Check,
                backup_skip_outcome(),
                on_event,
                &mut steps,
            );
        } else if passphrase.failed() {
            // The backup step already reported why; don't repeat the same error.
            emit_terminal(
                CycleStep::Check,
                StepOutcome::Skipped("backup failed".into()),
                on_event,
                &mut steps,
            );
        } else {
            check_result = run_hooked_step(
                CycleStep::Check,
                repo,
                on_event,
                &mut steps,
                |evt| {
                    commands::check::run_with_progress(
                        config,
                        passphrase.get()?,
                        false,
                        false,
                        Some(&mut |check_evt| evt(CycleEvent::Check(check_evt))),
                        check_max_percent,
                        true, // record_state: daemon/GUI updates the full_every timer
                        shutdown,
                    )
                },
                |result| {
                    if result.skipped {
                        let reason = if check_max_percent == 0
                            && config.check.full_every_duration().is_none()
                        {
                            "check not configured"
                        } else {
                            "check not due"
                        };
                        StepOutcome::Skipped(reason.into())
                    } else if result.errors.is_empty() {
                        StepOutcome::Ok
                    } else {
                        StepOutcome::Failed(format!("check found {} error(s)", result.errors.len()))
                    }
                },
            );
        }
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
/// Returns Some(value) on success/partial, None on failure, skip or before-hook abort.
pub(crate) fn run_hooked_step<T>(
    step: CycleStep,
    repo: &ResolvedRepo,
    on_event: &mut dyn FnMut(CycleEvent),
    steps: &mut Vec<(CycleStep, StepOutcome)>,
    action: impl FnOnce(&mut dyn FnMut(CycleEvent)) -> Result<T>,
    classify: impl FnOnce(&T) -> StepOutcome,
) -> Option<T> {
    let mut ctx = crate::hooks::HookContext {
        command: step.command_name().to_string(),
        repository: repo.config.repository.url.clone(),
        label: repo.label.clone(),
        error: None,
        source_label: None,
        source_paths: None,
        warnings: Vec::new(),
    };

    on_event(CycleEvent::StepStarted(step));

    // A classified failure (e.g. check with errors) fires `failed` hooks. A step
    // that ran and classified itself Skipped (check not due) or Partial counts as
    // success and fires `after`; `Err(Skipped)` means it never ran and fires neither.
    let result = crate::hooks::run_lifecycle(
        crate::hooks::HookPhases::Repo {
            global: &repo.global_hooks,
            repo: &repo.repo_hooks,
        },
        &mut ctx,
        || action(on_event).map(|v| (classify(&v), v)),
        |r| match r {
            Err(VykarError::Skipped(_)) => crate::hooks::Verdict::Skipped,
            Err(e) => crate::hooks::Verdict::Failed(e.to_string()),
            Ok((o, _)) => match o.error_msg() {
                Some(msg) => crate::hooks::Verdict::Failed(msg.to_owned()),
                None => crate::hooks::Verdict::Success,
            },
        },
    );
    drain_cycle_warnings(&mut ctx, step, on_event);

    let (outcome, value) = match result {
        Err(VykarError::Skipped(reason)) | Ok(Err(VykarError::Skipped(reason))) => {
            (StepOutcome::Skipped(reason), None)
        }
        Err(e) | Ok(Err(e)) => (StepOutcome::Failed(e.to_string()), None),
        Ok(Ok((o, v))) => (o, Some(v)),
    };

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

/// Record a step that never ran its action: emit the started/finished pair and
/// push the terminal outcome.
fn emit_terminal(
    step: CycleStep,
    outcome: StepOutcome,
    on_event: &mut dyn FnMut(CycleEvent),
    steps: &mut Vec<(CycleStep, StepOutcome)>,
) {
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
