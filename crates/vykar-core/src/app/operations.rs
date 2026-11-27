#![allow(
    clippy::doc_markdown,
    clippy::if_not_else,
    clippy::implicit_hasher,
    clippy::missing_errors_doc,
    clippy::redundant_closure_for_method_calls,
    clippy::too_many_lines
)]

use std::borrow::Cow;

use rand::Rng;

pub use crate::app::error::{StepError, StepErrorKind};
use crate::app::passphrase::{NoPrompt, PassphraseCache, RepoPassphrase};
use crate::app::preflight::{self, UnavailableRepo};
use crate::app::{HookError, OpsError, OpsResult};
use crate::commands;
use crate::commands::check::{CheckProgressEvent, CheckRequest};
use crate::commands::compact::CompactRequest;
use crate::commands::prune::PruneRequest;
use crate::config::{ResolvedRepo, SourceEntry, VykarConfig};
use crate::repo::control::{CancelToken, EventSink};
use crate::repo::{HostEnv, OpenOptions, Repo};
use vykar_types::error::VykarError;

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

/// A source that failed on its own (e.g. its path is missing) while the run
/// went on to the next source.
#[derive(Debug, Clone)]
pub struct FailedSource {
    pub label: String,
    pub error: String,
}

/// A repository-level failure or interrupt that stopped the run after at
/// least one snapshot had been created.
#[derive(Debug, Clone)]
pub struct AbortedRun {
    pub label: String,
    pub error: String,
    pub interrupted: bool,
    /// Sources after `label` that were never started.
    pub not_attempted: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct BackupRunReport {
    pub created: Vec<BackupSourceResult>,
    pub skipped: Vec<SkippedSource>,
    pub failed: Vec<FailedSource>,
    pub aborted: Option<AbortedRun>,
}

impl BackupRunReport {
    /// `Some` when any source failed or the run was aborted; the run then
    /// counts as failed even though `created` holds snapshots.
    pub fn failure_summary(&self) -> Option<String> {
        let parts: Vec<String> = self
            .failed
            .iter()
            .map(|f| (&f.label, &f.error))
            .chain(self.aborted.iter().map(|a| (&a.label, &a.error)))
            .map(|(label, error)| format!("source '{label}': {error}"))
            .collect();
        (!parts.is_empty()).then(|| parts.join("; "))
    }
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
    /// A source failed and the run moved on (tracing::error! already fired).
    /// Emitted immediately, so it is visible even if the run later returns `Err`.
    SourceFailed { label: String, error: String },
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
    Failed(StepError),
}

impl StepOutcome {
    /// Ok and Partial are both "success" — after-hooks run, subsequent steps proceed.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Ok | Self::Partial)
    }

    pub fn error_msg(&self) -> Option<&str> {
        match self {
            Self::Failed(e) => Some(e.message.as_str()),
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
    /// Non-fatal warning from a cycle step (e.g. a post-commit cleanup of
    /// prune failed). `tracing::warn!` has already fired — this is for GUI
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
    /// The backup step was skipped because the local repository was missing
    /// after its `before` hooks; `backup_skipped()` holds the reason.
    pub repo_unavailable: bool,
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

    /// Why the backup step was skipped, if it was. Only a hook, a dismissed
    /// passphrase prompt or an unavailable local repository does this, and the
    /// rest of the cycle is skipped with it.
    pub fn backup_skipped(&self) -> Option<&str> {
        self.steps.iter().find_map(|(step, o)| match (step, o) {
            (CycleStep::Backup, StepOutcome::Skipped(reason)) => Some(reason.as_str()),
            _ => None,
        })
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
///
/// A source-local failure is recorded and the loop continues. A repository
/// failure or interrupt stops it: with no snapshot created yet the error is
/// returned unchanged, otherwise it is recorded in `aborted`. `Ok` is returned
/// only when at least one snapshot was created, i.e. the repository opened
/// with `passphrase`.
fn run_backup_sources(
    config: &VykarConfig,
    sources: &[SourceEntry],
    passphrase: Option<&str>,
    repo_label: Option<&str>,
    cancel: &CancelToken,
    verbose: bool,
    events: Option<&dyn EventSink<BackupRunEvent>>,
) -> OpsResult<BackupRunReport> {
    use commands::backup::FailureScope;

    if sources.is_empty() {
        return Err(VykarError::Config("no sources configured for this repository".into()).into());
    }

    let compression = config.compression.to_compression();

    let mut report = BackupRunReport::default();
    let mut first_failure: Option<OpsError> = None;

    for (idx, source) in sources.iter().enumerate() {
        let snapshot_name = generate_snapshot_name();

        let run_backup = || {
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

            let r = if let Some(sink) = events {
                let backup_events = |bpe| sink.emit(BackupRunEvent::Backup(bpe));
                commands::backup::run_scoped(
                    config,
                    req,
                    Some(&backup_events),
                    cancel,
                    None,
                    HostEnv::system(),
                )
            } else {
                commands::backup::run_scoped(config, req, None, cancel, None, HostEnv::system())
            };
            r.map_err(|(scope, e)| (scope, OpsError::from(e)))
        };

        let result = if cancel.is_cancelled() {
            Err((
                FailureScope::Repository,
                OpsError::from(VykarError::Interrupted),
            ))
        } else if source.hooks.has_any() {
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

            let outer = crate::hooks::run_lifecycle(
                crate::hooks::HookPhases::Source(&source.hooks),
                &mut ctx,
                run_backup,
                |r| match r {
                    Ok(_) => crate::hooks::Verdict::Success,
                    Err((_, OpsError::Skipped(_))) => crate::hooks::Verdict::Skipped,
                    Err((_, e)) => crate::hooks::Verdict::Failed(e.to_string()),
                },
            );

            // Drain accumulated warnings into events (tracing already fired)
            drain_backup_warnings(&mut ctx, events, &scope);

            // An outer error is a before-hook failure or skip: source-local.
            outer.unwrap_or_else(|e| Err((FailureScope::Source, e)))
        } else {
            run_backup()
        };

        let outcome = match result {
            Ok(outcome) => outcome,
            Err((_, OpsError::Skipped(reason))) => {
                report.skipped.push(SkippedSource {
                    label: source.label.clone(),
                    reason,
                });
                continue;
            }
            Err((FailureScope::Source, e)) => {
                let error = e.to_string();
                tracing::error!("backup of source '{}' failed: {error}", source.label);
                if let Some(sink) = events {
                    sink.emit(BackupRunEvent::SourceFailed {
                        label: source.label.clone(),
                        error: error.clone(),
                    });
                }
                report.failed.push(FailedSource {
                    label: source.label.clone(),
                    error,
                });
                first_failure.get_or_insert(e);
                continue;
            }
            Err((FailureScope::Repository, e)) => {
                // Nothing to keep: return the typed error so passphrase retry
                // and interrupt handling still recognise it.
                if report.created.is_empty() {
                    return Err(e);
                }
                let error = e.to_string();
                tracing::error!("backup stopped at source '{}': {error}", source.label);
                report.aborted = Some(AbortedRun {
                    label: source.label.clone(),
                    error,
                    interrupted: matches!(e, OpsError::Engine(VykarError::Interrupted)),
                    not_attempted: sources
                        .iter()
                        .skip(idx + 1)
                        .map(|s| s.label.clone())
                        .collect(),
                });
                return Ok(report);
            }
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

    if !report.created.is_empty() {
        return Ok(report);
    }

    // Nothing was created, so the repository may never have opened: never
    // return `Ok` here, or a caller could cache an unchecked passphrase.
    if let Some(first) = first_failure {
        if report.failed.len() == 1 && report.skipped.is_empty() {
            return Err(first);
        }
        let mut parts: Vec<String> = report
            .failed
            .iter()
            .map(|f| format!("source '{}': {}", f.label, f.error))
            .collect();
        parts.extend(
            report
                .skipped
                .iter()
                .map(|s| format!("source '{}' skipped: {}", s.label, s.reason)),
        );
        return Err(VykarError::Other(parts.join("; ")).into());
    }

    // Only skips: to the repo-level lifecycle this is a skip, not a success.
    let reasons = report
        .skipped
        .iter()
        .map(|s| format!("source '{}': {}", s.label, s.reason))
        .collect::<Vec<_>>()
        .join("; ");
    Err(OpsError::Skipped(reasons))
}

/// Drain accumulated warnings from HookContext into BackupRunEvent.
fn drain_backup_warnings(
    ctx: &mut crate::hooks::HookContext,
    events: Option<&dyn EventSink<BackupRunEvent>>,
    scope: &HookScope,
) {
    if let Some(sink) = events {
        for warning in ctx.warnings.drain(..) {
            sink.emit(BackupRunEvent::HookWarning {
                scope: scope.clone(),
                warning,
            });
        }
    }
}

// ── Public hook-aware backup API ──────────────────────────────────────────

/// Backup with the full hook lifecycle (repo + source hooks). The passphrase
/// is resolved inside the repo lifecycle, after its `before` hooks; a refused
/// value is retried without re-running them. Used by
/// [`crate::app::AppService::backup`].
pub(crate) fn run_backup_selection_with(
    repo: &ResolvedRepo,
    sources: &[SourceEntry],
    passphrase: &mut RepoPassphrase<'_>,
    cancel: &CancelToken,
    verbose: bool,
    events: Option<&dyn EventSink<BackupRunEvent>>,
) -> OpsResult<BackupRunReport> {
    let config = &repo.config;
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
    let outer = crate::hooks::run_lifecycle(
        crate::hooks::HookPhases::Repo {
            global: &repo.global_hooks,
            repo: &repo.repo_hooks,
        },
        &mut ctx,
        || {
            let r = passphrase.run(|pass| {
                run_backup_sources(
                    config,
                    sources,
                    pass,
                    repo.label.as_deref(),
                    cancel,
                    verbose,
                    events,
                )
            });
            // `Ok` means a snapshot was created, so the repository opened.
            if r.is_ok() {
                passphrase.accept();
            }
            r
        },
        |r| match r.as_ref().map(BackupRunReport::failure_summary) {
            Ok(Some(summary)) => crate::hooks::Verdict::Failed(summary),
            _ => crate::hooks::result_verdict(r),
        },
    );
    // Drain before flattening so before/failed/finally warnings always arrive.
    drain_backup_warnings(&mut ctx, events, &HookScope::Repo);
    outer.and_then(|r| r)
}

/// [`run_backup_selection_with`] over a fixed passphrase (tests).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn run_backup_selection(
    repo: &ResolvedRepo,
    sources: &[SourceEntry],
    passphrase: Option<&str>,
    cancel: &CancelToken,
    verbose: bool,
    events: Option<&dyn EventSink<BackupRunEvent>>,
) -> OpsResult<BackupRunReport> {
    let mut cache = PassphraseCache::default();
    let mut prompt = NoPrompt;
    let mut scope = RepoPassphrase::preset(&mut cache, &mut prompt, &repo.config, passphrase);
    run_backup_selection_with(repo, sources, &mut scope, cancel, verbose, events)
}

// ── Hook wrapper for standalone commands ──────────────────────────────────

/// Wrap an action with repo-level hooks, for the standalone commands (prune,
/// compact, check) that aren't part of the full cycle; frontends reach it
/// through [`crate::app::AppService::run_hooked`]. The action's error type is
/// the caller's (any [`HookError`]); hook failures and skips arrive
/// converted from [`OpsError`].
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn run_command_with_hooks<F, T, E>(
    repo: &ResolvedRepo,
    command: &str,
    action: F,
) -> Result<T, E>
where
    F: FnOnce() -> Result<T, E>,
    E: HookError,
{
    run_command_with_hook_warnings(repo, command, &mut |_| {}, action)
}

/// [`run_command_with_hooks`] that also hands each non-fatal hook warning
/// (an `after`/`failed`/`finally` failure, already traced) to `on_warning`,
/// for frontends that do not show tracing output. Errors as in
/// [`run_command_with_hooks`].
pub(crate) fn run_command_with_hook_warnings<F, T, E>(
    repo: &ResolvedRepo,
    command: &str,
    on_warning: &mut dyn FnMut(String),
    action: F,
) -> Result<T, E>
where
    F: FnOnce() -> Result<T, E>,
    E: HookError,
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
    let result = crate::hooks::run_lifecycle(
        crate::hooks::HookPhases::Repo {
            global: &repo.global_hooks,
            repo: &repo.repo_hooks,
        },
        &mut ctx,
        action,
        crate::hooks::error_verdict,
    )
    .map_err(E::from)
    .and_then(|r| r);
    for w in ctx.warnings.drain(..) {
        on_warning(w);
    }
    result
}

// ── Full-cycle with internalized hooks ───────────────────────────────────

/// Run the full backup cycle: backup → prune → compact → check.
/// Hooks are run internally — callers don't need to handle them.
/// With `UnavailableRepo::Skip`, a local repository whose `config` is missing
/// once the backup step's `before` hooks have run is skipped like a hook skip:
/// only `finally` hooks run and nothing is resolved or opened. Over a fixed
/// passphrase (tests); frontends use [`crate::app::AppService::run_cycle`].
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn run_full_cycle_for_repo(
    repo: &ResolvedRepo,
    passphrase: Option<&str>,
    cancel: &CancelToken,
    verbose: bool,
    source_filter: &[String],
    unavailable: UnavailableRepo,
    events: &dyn EventSink<CycleEvent>,
) -> FullCycleResult {
    let mut cache = PassphraseCache::default();
    let mut prompt = NoPrompt;
    let mut scope = RepoPassphrase::preset(&mut cache, &mut prompt, &repo.config, passphrase);
    run_full_cycle(
        repo,
        &mut scope,
        cancel,
        verbose,
        source_filter,
        unavailable,
        events,
    )
}

/// The full cycle over a passphrase scope. The passphrase is first resolved
/// inside the first step that needs one, after that step's `before` hooks
/// pass, and at most once per cycle; a resolve failure is not retried, and a
/// dismissed prompt skips the step without reporting an error. A value the
/// repository refuses is replaced by the scope (see [`RepoPassphrase`]) only
/// at an open, as long as no step has opened the repository with it: at the
/// backup's per-source opens and at the session's open. One session (one
/// open repository) serves prune, compact and check; a step whose action
/// fails drops it and the next step opens again.
/// With `UnavailableRepo::Skip`, a local repository whose `config` is missing
/// once the backup step's `before` hooks have run is skipped like a hook skip:
/// only `finally` hooks run and nothing is resolved or opened.
pub(crate) fn run_full_cycle(
    repo: &ResolvedRepo,
    passphrase: &mut RepoPassphrase<'_>,
    cancel: &CancelToken,
    verbose: bool,
    source_filter: &[String],
    unavailable: UnavailableRepo,
    events: &dyn EventSink<CycleEvent>,
) -> FullCycleResult {
    let config = &repo.config;
    let sources = &repo.sources;

    let mut steps: Vec<(CycleStep, StepOutcome)> = Vec::new();
    let mut backup_report: Option<BackupRunReport> = None;
    let mut prune_stats: Option<commands::prune::PruneStats> = None;
    let mut compact_stats: Option<commands::compact::CompactStats> = None;
    let mut check_result: Option<commands::check::CheckResult> = None;
    let mut repo_unavailable = false;
    // The cycle's open repository from prune onward: opened by the first
    // maintenance step that runs its action, after that step's `before`
    // hooks; reset and reused by the later steps; dropped after a step whose
    // action failed, and by the last step that uses it before its `after`
    // hooks run. One exception: when `before_check` fails or skips, check's
    // action never runs, so a session kept for it stays open through
    // `failed_check` and `finally_check`.
    let mut session: Option<Repo> = None;
    // Whether check opens (or reuses) the session, so compact keeps it.
    let check_needs_session =
        !commands::check::skips_without_open(config, config.check.max_percent);

    // 1. Backup
    if !cancel.is_cancelled() {
        match resolve_cycle_sources(sources, source_filter) {
            Err(e) => emit_terminal(
                CycleStep::Backup,
                StepOutcome::Failed(StepError::new(StepErrorKind::Config, e)),
                events,
                &mut steps,
            ),
            Ok(effective_sources) => {
                backup_report = run_hooked_step(
                    CycleStep::Backup,
                    repo,
                    events,
                    &mut steps,
                    |evt| {
                        // After the `before` hooks, so one can mount the repository;
                        // before the passphrase, so nothing is asked for one that
                        // is not there.
                        if unavailable == UnavailableRepo::Skip
                            && let Some(reason) = preflight::availability(repo).unavailable_reason()
                        {
                            repo_unavailable = true;
                            return Err(OpsError::Skipped(reason));
                        }
                        let report = passphrase.run(|pass| {
                            run_backup_sources(
                                config,
                                &effective_sources,
                                pass,
                                repo.label.as_deref(),
                                cancel,
                                verbose,
                                Some(&|bpe: BackupRunEvent| match bpe {
                                    BackupRunEvent::Backup(e) => evt.emit(CycleEvent::Backup(e)),
                                    BackupRunEvent::HookWarning { scope, warning } => {
                                        evt.emit(CycleEvent::HookWarning {
                                            step: CycleStep::Backup,
                                            scope,
                                            warning,
                                        });
                                    }
                                    BackupRunEvent::SourceFailed { label, error } => {
                                        evt.emit(CycleEvent::StepWarning {
                                            step: CycleStep::Backup,
                                            message: format!("source '{label}': {error}"),
                                        });
                                    }
                                }),
                            )
                        })?;
                        // `Ok` means a snapshot was created, so the repository opened.
                        passphrase.accept();
                        Ok(report)
                    },
                    |report| {
                        // Failed keeps the report, so created snapshots still
                        // print, while prune/compact skip as "backup failed".
                        if let Some(summary) = report.failure_summary() {
                            StepOutcome::Failed(StepError::new(StepErrorKind::Other, summary))
                        } else if report.created.iter().any(|s| s.stats.errors > 0) {
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
    if !cancel.is_cancelled() {
        let has_retention = config.retention.has_any_rule()
            || sources
                .iter()
                .any(|s| s.retention.as_ref().is_some_and(|r| r.has_any_rule()));

        if backup_skipped {
            emit_terminal(CycleStep::Prune, backup_skip_outcome(), events, &mut steps);
        } else if !has_retention {
            emit_terminal(
                CycleStep::Prune,
                StepOutcome::Skipped("no retention rules".into()),
                events,
                &mut steps,
            );
        } else if !backup_ok {
            emit_terminal(
                CycleStep::Prune,
                StepOutcome::Skipped("backup failed".into()),
                events,
                &mut steps,
            );
        } else {
            prune_stats = run_hooked_step(
                CycleStep::Prune,
                repo,
                events,
                &mut steps,
                |evt| {
                    let r = cycle_session(&mut session, passphrase, cancel).and_then(|repo| {
                        commands::prune::run_on_repo(
                            repo,
                            config,
                            &PruneRequest {
                                dry_run: false,
                                list: false,
                                sources,
                                source_filter,
                            },
                            cancel,
                        )
                        .map(|(stats, _)| stats)
                        .map_err(OpsError::from)
                    });
                    // Compact always follows a prune that ran, unless cancelled.
                    if r.is_err() || cancel.is_cancelled() {
                        session = None;
                    }
                    let stats = r?;
                    // Surface post-commit warnings as cycle events so GUI/CLI
                    // consumers can log them as persistent entries. tracing
                    // already fired inside commands::prune::run_on_repo.
                    for w in &stats.warnings {
                        evt.emit(CycleEvent::StepWarning {
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
    if !cancel.is_cancelled() {
        if backup_skipped {
            emit_terminal(
                CycleStep::Compact,
                backup_skip_outcome(),
                events,
                &mut steps,
            );
        } else if !backup_ok {
            emit_terminal(
                CycleStep::Compact,
                StepOutcome::Skipped("backup failed".into()),
                events,
                &mut steps,
            );
        } else {
            compact_stats = run_hooked_step(
                CycleStep::Compact,
                repo,
                events,
                &mut steps,
                |_evt| {
                    let r = cycle_session(&mut session, passphrase, cancel).and_then(|repo| {
                        // The config threshold is normalised at load.
                        match commands::compact::run_on_repo(
                            repo,
                            &CompactRequest::from_config(config),
                            cancel,
                        ) {
                            // Compact runs in every cycle; on an append-only server it
                            // belongs to the maintenance window, so the cycle skips it.
                            Err(VykarError::AppendOnlyServer { .. }) => Err(OpsError::Skipped(
                                "the server is append-only; compact runs in a maintenance window"
                                    .into(),
                            )),
                            other => other.map_err(OpsError::from),
                        }
                    });
                    // An append-only skip is an `Err` too: check opens again.
                    if r.is_err() || !check_needs_session || cancel.is_cancelled() {
                        session = None;
                    }
                    r
                },
                |_| StepOutcome::Ok,
            );
        }
    }

    // 4. Check (metadata-only, with sampling support)
    if !cancel.is_cancelled() {
        let check_max_percent = config.check.max_percent;
        if backup_skipped {
            emit_terminal(CycleStep::Check, backup_skip_outcome(), events, &mut steps);
        } else if passphrase.failed() {
            // The backup step already reported why; don't repeat the same error.
            emit_terminal(
                CycleStep::Check,
                StepOutcome::Skipped("backup failed".into()),
                events,
                &mut steps,
            );
        } else {
            check_result = run_hooked_step(
                CycleStep::Check,
                repo,
                events,
                &mut steps,
                |evt| {
                    // First, as the opener does: a cancel during the `before`
                    // hooks interrupts the step before the skip test and the open.
                    cancel.check().map_err(OpsError::from)?;
                    // A check that is not configured returns without opening.
                    if commands::check::skips_without_open(config, check_max_percent) {
                        session = None;
                        return Ok(commands::check::skipped_result());
                    }
                    cycle_session(&mut session, passphrase, cancel)?;
                    // The last step: taken, so no handle is open while its
                    // `after`, `failed` and `finally` hooks run.
                    let mut repo = session.take().expect("cycle_session opened it");
                    let sink = |e: CheckProgressEvent| evt.emit(CycleEvent::Check(e));
                    commands::check::run_on_repo(
                        &mut repo,
                        config,
                        &CheckRequest {
                            max_percent: check_max_percent,
                            // daemon/GUI updates the full_every timer
                            record_state: true,
                            ..CheckRequest::default()
                        },
                        Some(&sink),
                        cancel,
                    )
                    .map_err(OpsError::from)
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
                        StepOutcome::Failed(StepError::new(
                            StepErrorKind::Other,
                            format!("check found {} error(s)", result.errors.len()),
                        ))
                    }
                },
            );
        }
    }

    drop(session);
    FullCycleResult {
        backup_report,
        prune_stats,
        compact_stats,
        check_result,
        steps,
        repo_unavailable,
    }
}

/// The cycle's session for the next maintenance step: opened with
/// `passphrase` (accepted right after the open) when there is none, else
/// reset for the next command ([`Repo::reset_for_next_command`]). A failed
/// reset drops the session.
fn cycle_session<'s>(
    session: &'s mut Option<Repo>,
    passphrase: &mut RepoPassphrase<'_>,
    cancel: &CancelToken,
) -> OpsResult<&'s mut Repo> {
    if session.is_none() {
        *session = Some(passphrase.open::<OpsError>(OpenOptions::new(), cancel)?);
    } else if let Some(repo) = session.as_mut()
        && let Err(e) = repo.reset_for_next_command()
    {
        *session = None;
        return Err(OpsError::from(e));
    }
    Ok(session.as_mut().expect("opened or kept above"))
}

/// Run a single cycle step with the full hook lifecycle.
/// Returns Some(value) whenever the action returned `Ok` (even if `classify`
/// judged it Failed), None on error, skip or before-hook abort.
pub(crate) fn run_hooked_step<T>(
    step: CycleStep,
    repo: &ResolvedRepo,
    events: &dyn EventSink<CycleEvent>,
    steps: &mut Vec<(CycleStep, StepOutcome)>,
    action: impl FnOnce(&dyn EventSink<CycleEvent>) -> OpsResult<T>,
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

    events.emit(CycleEvent::StepStarted(step));

    // A classified failure (e.g. check with errors) fires `failed` hooks. A step
    // that ran and classified itself Skipped (check not due) or Partial counts as
    // success and fires `after`; `Err(Skipped)` means it never ran and fires neither.
    let result = crate::hooks::run_lifecycle(
        crate::hooks::HookPhases::Repo {
            global: &repo.global_hooks,
            repo: &repo.repo_hooks,
        },
        &mut ctx,
        || action(events).map(|v| (classify(&v), v)),
        |r| match r {
            Err(OpsError::Skipped(_)) => crate::hooks::Verdict::Skipped,
            Err(e) => crate::hooks::Verdict::Failed(e.to_string()),
            Ok((o, _)) => match o.error_msg() {
                Some(msg) => crate::hooks::Verdict::Failed(msg.to_owned()),
                None => crate::hooks::Verdict::Success,
            },
        },
    );
    drain_cycle_warnings(&mut ctx, step, events);

    let (outcome, value) = match result {
        Err(OpsError::Skipped(reason)) | Ok(Err(OpsError::Skipped(reason))) => {
            (StepOutcome::Skipped(reason), None)
        }
        Err(e) | Ok(Err(e)) => (StepOutcome::Failed(StepError::from(&e)), None),
        Ok(Ok((o, v))) => (o, Some(v)),
    };

    events.emit(CycleEvent::StepFinished(step, outcome.clone()));
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
    events: &dyn EventSink<CycleEvent>,
    steps: &mut Vec<(CycleStep, StepOutcome)>,
) {
    events.emit(CycleEvent::StepStarted(step));
    events.emit(CycleEvent::StepFinished(step, outcome.clone()));
    steps.push((step, outcome));
}

fn drain_cycle_warnings(
    ctx: &mut crate::hooks::HookContext,
    step: CycleStep,
    events: &dyn EventSink<CycleEvent>,
) {
    for warning in ctx.warnings.drain(..) {
        events.emit(CycleEvent::HookWarning {
            step,
            scope: HookScope::Repo,
            warning,
        });
    }
}
