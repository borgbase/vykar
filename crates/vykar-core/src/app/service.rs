//! The application service: passphrase policy, repository selection, the
//! multi-repository cycle and the hooked commands. Frontends supply a prompt,
//! a cancel token and an event callback, and render.

use zeroize::Zeroizing;

use crate::app::operations::{self, BackupRunEvent, BackupRunReport, CycleEvent, FullCycleResult};
use crate::app::passphrase::{PassphraseCache, PassphrasePrompt, RepoPassphrase};
use crate::app::preflight::UnavailableRepo;
use crate::app::{HookError, OpsResult};
use crate::config::{ResolvedRepo, SourceEntry, VykarConfig};
use crate::repo::control::CancelToken;

/// A command with repo-level hooks, other than backup (see [`AppService::backup`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookedCommand {
    Prune,
    Compact,
    Check,
}

impl HookedCommand {
    /// The hook command name (`before_<name>`, `VYKAR_COMMAND`).
    pub fn name(self) -> &'static str {
        match self {
            Self::Prune => "prune",
            Self::Compact => "compact",
            Self::Check => "check",
        }
    }
}

/// Which configured repositories a cycle covers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RepoSelection {
    /// Every repository, in configured order (bare `vykar`, SIGUSR1, GUI "Back up all").
    #[default]
    All,
    /// These indices into the repository list, in this order; out-of-range
    /// ones are ignored (scheduled cycles).
    Indices(Vec<usize>),
    /// One repository the user named (`-R`): an unavailable one fails
    /// instead of being skipped.
    Explicit(usize),
}

/// What a cycle runs.
#[derive(Debug, Clone, Default)]
pub struct CycleRequest {
    pub selection: RepoSelection,
    /// Source labels; empty runs every source.
    pub source_filter: Vec<String>,
    pub verbose: bool,
}

/// One repository's cycle.
pub struct RepoCycleReport {
    /// Index into the repository list the cycle was given.
    pub index: usize,
    pub result: FullCycleResult,
}

impl RepoCycleReport {
    /// The backup step was skipped and nothing failed: a hook skip, a
    /// dismissed prompt or an unavailable repository.
    pub fn skipped(&self) -> Option<&str> {
        if self.result.has_failures() {
            return None;
        }
        self.result.backup_skipped()
    }
}

/// The outcome of [`AppService::run_cycle`].
#[derive(Default)]
pub struct CycleReport {
    /// Repositories the cycle reached, in order.
    pub repos: Vec<RepoCycleReport>,
    /// Selected repositories not started because `cancel` was set.
    pub not_reached: Vec<usize>,
}

impl CycleReport {
    /// Any reached repository has a failed step.
    pub fn has_failures(&self) -> bool {
        self.repos.iter().any(|r| r.result.has_failures())
    }

    /// Any reached repository's backup was partial.
    pub fn had_partial(&self) -> bool {
        self.repos.iter().any(|r| r.result.had_partial())
    }

    /// At least one repository was reached and every one skipped.
    pub fn all_skipped(&self) -> bool {
        !self.repos.is_empty() && self.repos.iter().all(|r| r.skipped().is_some())
    }
}

/// What a cycle reports while it runs, on the calling thread.
pub enum AppEvent<'a> {
    RepoStarted {
        repo: &'a ResolvedRepo,
        /// Index into the repository list.
        index: usize,
        /// Position among the selected repositories, from 0.
        position: usize,
        /// Number of selected repositories.
        total: usize,
    },
    Cycle {
        repo: &'a ResolvedRepo,
        event: CycleEvent,
    },
    RepoFinished {
        repo: &'a ResolvedRepo,
        report: &'a RepoCycleReport,
    },
}

/// Passphrase policy and the commands that span hooks, the passphrase and
/// several repositories. Owns the cache of accepted passphrases.
#[derive(Default)]
pub struct AppService {
    cache: PassphraseCache,
    /// A cache hit starts its scope as accepted; see [`Self::one_shot`].
    one_shot: bool,
}

impl AppService {
    /// A service for a long-lived process (daemon, GUI): a cached value the
    /// repository refuses is evicted and resolved again.
    pub fn new() -> Self {
        Self::default()
    }

    /// A service for one CLI command. Its cache holds only values accepted
    /// earlier in the same run (a probe), so a cache hit starts its scope as
    /// accepted: a later decryption failure is damage, and the command does
    /// not run twice.
    pub fn one_shot() -> Self {
        Self {
            one_shot: true,
            ..Self::default()
        }
    }

    /// After a configuration reload: forget values of repositories that are
    /// gone or whose passphrase source changed.
    pub fn reload(&mut self, repos: &[ResolvedRepo]) {
        self.cache.retain_for(repos);
    }

    /// The passphrase scope for one unhooked command.
    pub fn passphrase<'a>(
        &'a mut self,
        config: &'a VykarConfig,
        label: Option<&str>,
        prompt: &'a mut dyn PassphrasePrompt,
    ) -> RepoPassphrase<'a> {
        RepoPassphrase::new(&mut self.cache, prompt, config, label).trusting_cache(self.one_shot)
    }

    /// Cache a value the caller validated another way (GUI: after `init`).
    pub fn remember(&mut self, config: &VykarConfig, value: Zeroizing<String>) {
        self.cache.insert(config, value);
    }

    /// Whether an accepted value for this repository is cached under its
    /// current passphrase source.
    pub fn is_cached(&self, config: &VykarConfig) -> bool {
        self.cache.contains(config)
    }

    /// Run `action` inside `command`'s repo-level hooks, or without hooks when
    /// `dry_run`. The passphrase scope is lazy, so a prompt comes after the
    /// `before` hooks and a re-prompt does not re-run them. Non-fatal hook
    /// warnings go to `on_warning` (already traced).
    pub fn run_hooked<T, E: HookError>(
        &mut self,
        repo: &ResolvedRepo,
        command: HookedCommand,
        dry_run: bool,
        prompt: &mut dyn PassphrasePrompt,
        on_warning: &mut dyn FnMut(String),
        action: impl FnOnce(&mut RepoPassphrase<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut pass = self.passphrase(&repo.config, repo.label.as_deref(), prompt);
        if dry_run {
            action(&mut pass)
        } else {
            operations::run_command_with_hook_warnings(repo, command.name(), on_warning, || {
                action(&mut pass)
            })
        }
    }

    /// Backup with repo and source hooks, the passphrase resolved after the
    /// repo `before` hooks.
    pub fn backup(
        &mut self,
        repo: &ResolvedRepo,
        sources: &[SourceEntry],
        prompt: &mut dyn PassphrasePrompt,
        cancel: &CancelToken,
        verbose: bool,
        on_event: Option<&mut dyn FnMut(BackupRunEvent)>,
    ) -> OpsResult<BackupRunReport> {
        let mut pass = self.passphrase(&repo.config, repo.label.as_deref(), prompt);
        operations::run_backup_selection_with(repo, sources, &mut pass, cancel, verbose, on_event)
    }

    /// The full cycle (backup → prune → compact → check) for the selected
    /// repositories. An unavailable local repository is skipped after its
    /// `before_backup` hooks, unless the selection is `Explicit`.
    pub fn run_cycle(
        &mut self,
        repos: &[ResolvedRepo],
        request: &CycleRequest,
        prompt: &mut dyn PassphrasePrompt,
        cancel: &CancelToken,
        on_event: &mut dyn FnMut(AppEvent<'_>),
    ) -> CycleReport {
        let (indices, unavailable): (Vec<usize>, _) = match &request.selection {
            RepoSelection::All => ((0..repos.len()).collect(), UnavailableRepo::Skip),
            RepoSelection::Indices(list) => (
                list.iter().copied().filter(|&i| i < repos.len()).collect(),
                UnavailableRepo::Skip,
            ),
            RepoSelection::Explicit(i) => (
                std::iter::once(*i).filter(|&i| i < repos.len()).collect(),
                UnavailableRepo::Attempt,
            ),
        };
        let total = indices.len();
        let mut report = CycleReport::default();
        for (position, &index) in indices.iter().enumerate() {
            if cancel.is_cancelled() {
                report
                    .not_reached
                    .extend(indices.iter().skip(position).copied());
                break;
            }
            let Some(repo) = repos.get(index) else {
                continue;
            };
            on_event(AppEvent::RepoStarted {
                repo,
                index,
                position,
                total,
            });
            let result = {
                let mut pass = self.passphrase(&repo.config, repo.label.as_deref(), &mut *prompt);
                operations::run_full_cycle(
                    repo,
                    &mut pass,
                    cancel,
                    request.verbose,
                    &request.source_filter,
                    unavailable,
                    &mut |event| on_event(AppEvent::Cycle { repo, event }),
                )
            };
            report.repos.push(RepoCycleReport { index, result });
            if let Some(finished) = report.repos.last() {
                on_event(AppEvent::RepoFinished {
                    repo,
                    report: finished,
                });
            }
        }
        report
    }
}
