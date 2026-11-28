#![allow(
    clippy::duration_suboptimal_units,
    clippy::missing_errors_doc,
    clippy::option_option,
    clippy::redundant_closure_for_method_calls
)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use zeroize::{Zeroize, Zeroizing};

use crate::app::{OpsError, PassphraseFailure, SkipKind, SkipReason};
use crate::config::{ResolvedRepo, VykarConfig};
use crate::platform::shell;
use vykar_types::error::{Result, VykarError};

/// Default timeout for passcommand execution (60 seconds).
const PASSCOMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// Context handed to an interactive passphrase prompt.
///
/// Deliberately carries no timeout: neither the CLI nor the GUI prompt can be
/// timed out (`schedule.passphrase_prompt_timeout_seconds` is accepted for
/// config compatibility but ignored).
#[derive(Debug, Clone)]
pub struct PromptContext {
    pub repository_label: Option<String>,
    pub repository_url: String,
}

pub fn configured_passphrase(config: &VykarConfig) -> Result<Option<Zeroizing<String>>> {
    if let Some(ref p) = config.encryption.passphrase {
        tracing::debug!(
            "using plaintext encryption.passphrase from config; prefer encryption.passcommand or VYKAR_PASSPHRASE"
        );
        return Ok(Some(Zeroizing::new(p.clone())));
    }

    if let Some(ref cmd) = config.encryption.passcommand {
        let mut command = shell::command_for_script(cmd);
        command.env_remove("VYKAR_PASSPHRASE");
        // A passcommand has no meaningful stdin — its *output* is the
        // passphrase. Inheriting the parent's fd 0 hands a GUI-launched child
        // whatever launchd left there; redirecting from /dev/null gives any
        // script that reads stdin a clean EOF instead of a blocking read.
        command.stdin(std::process::Stdio::null());
        let output = shell::run_command_with_timeout(&mut command, PASSCOMMAND_TIMEOUT)
            .map_err(VykarError::Io)?;

        if !output.status.success() {
            return Err(VykarError::Config(format!(
                "passcommand failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let mut raw = String::from_utf8(output.stdout)
            .map_err(|e| VykarError::Config(format!("passcommand output is not UTF-8: {e}")))?;
        let pass = Zeroizing::new(raw.trim().to_string());
        raw.zeroize();

        if pass.is_empty() {
            return Err(VykarError::Config(
                "passcommand returned an empty passphrase".into(),
            ));
        }

        return Ok(Some(pass));
    }

    if let Some(pass) = take_env_passphrase() {
        return Ok(Some(pass));
    }

    Ok(None)
}

/// Whether `config` names a non-interactive passphrase source, without
/// running `encryption.passcommand`. Only when neither `encryption.passphrase`
/// nor `encryption.passcommand` is set does it read `VYKAR_PASSPHRASE`, as
/// [`configured_passphrase`] does: into the process memo, removing it from
/// the environment, so the value stays available to later lookups.
pub fn has_configured_source(config: &VykarConfig) -> bool {
    config.encryption.passphrase.is_some()
        || config.encryption.passcommand.is_some()
        || take_env_passphrase().is_some()
}

/// Whether [`configured_passphrase`] would run `encryption.passcommand` for
/// `config` (an inline passphrase takes precedence over it).
pub fn resolving_runs_command(config: &VykarConfig) -> bool {
    config.encryption.passphrase.is_none() && config.encryption.passcommand.is_some()
}

/// Cache for `VYKAR_PASSPHRASE`: `None` = not yet read, `Some(v)` = already consumed.
static ENV_PASSPHRASE: Mutex<Option<Option<Zeroizing<String>>>> = Mutex::new(None);

/// Read `VYKAR_PASSPHRASE` from the process environment on first call,
/// remove it from the environment, and cache the value for subsequent calls.
fn take_env_passphrase() -> Option<Zeroizing<String>> {
    let mut cache = ENV_PASSPHRASE
        .lock()
        .expect("passphrase cache lock not poisoned");
    if let Some(ref cached) = *cache {
        return cached.clone();
    }
    let val = std::env::var("VYKAR_PASSPHRASE")
        .ok()
        .filter(|s| !s.is_empty());
    if val.is_some() {
        // SAFETY: `remove_var` is unsound while another thread reads or
        // writes the environment. Single-repo CLI commands and daemon startup
        // (`load_daemon_config`, through `has_configured_source`) get here
        // before spawning threads, and the cache above makes every later call
        // return early. Not guaranteed in the GUI (passphrases resolve on its
        // worker thread while the UI thread runs), in multi-repo CLI runs (an
        // earlier repo may already have started the storage runtime), or for a
        // daemon whose first env lookup comes after a SIGHUP reload.
        #[allow(unsafe_code)]
        unsafe {
            std::env::remove_var("VYKAR_PASSPHRASE");
        }
    }
    let result = val.map(Zeroizing::new);
    *cache = Some(result.clone());
    result
}

/// Reset the cached env passphrase. Only used by tests.
#[cfg(test)]
pub(crate) fn reset_env_passphrase_cache() {
    *ENV_PASSPHRASE
        .lock()
        .expect("passphrase cache lock not poisoned") = None;
}

pub fn resolve_passphrase<F>(
    config: &VykarConfig,
    label: Option<&str>,
    mut prompt: F,
) -> Result<Option<Zeroizing<String>>>
where
    F: FnMut(PromptContext) -> Result<Option<Zeroizing<String>>>,
{
    if let Some(pass) = configured_passphrase(config)? {
        return Ok(Some(pass));
    }

    prompt(PromptContext {
        repository_label: label.map(|s| s.to_string()),
        repository_url: config.repository.url.clone(),
    })
}

/// Which of the two `init` prompts the callback is being asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitPromptStage {
    /// "Enter new passphrase".
    Enter,
    /// "Confirm passphrase" — must match the [`InitPromptStage::Enter`] value.
    Confirm,
}

/// Outcome of resolving the passphrase for a repository that is about to be
/// initialized.
///
/// Cancellation is a distinct variant rather than an error: dismissing the
/// prompt means "don't create the repository", which no front end should
/// report as a failure.
#[derive(Debug)]
pub enum InitPassphrase {
    Provided(Zeroizing<String>),
    /// The user dismissed either prompt.
    Cancelled,
}

/// Resolve the passphrase to initialize a repository with.
///
/// Resolution order:
/// 1. `pre_resolved_configured` — a value the caller already obtained from a
///    **configured** source while probing the repository. Provenance matters:
///    only `encryption.passphrase` / `encryption.passcommand` qualify, never a
///    passphrase the user typed at the probe prompt. A probe typo reused here
///    would lock the new repository behind it, which is exactly what the
///    enter-and-confirm pair below exists to prevent.
/// 2. A configured source resolved now, including `VYKAR_PASSPHRASE`. Values
///    from these sources skip confirmation — there is nothing to mistype.
/// 3. Interactive enter-and-confirm.
///
/// The passphrase must be non-empty **whatever source it came from**, and the
/// two interactive entries must match; both are reported as typed errors
/// ([`VykarError::EmptyPassphrase`], [`VykarError::PassphraseMismatch`]) rather
/// than a generic string. Dismissing *either* prompt yields
/// [`InitPassphrase::Cancelled`].
///
/// The emptiness check covers the configured sources too, not just the prompt:
/// `encryption.passphrase: ""` deserializes to `Some("")` (the strict-string
/// deserializer rejects nulls, not empty strings), so without it a config typo
/// would silently create an encrypted repository whose key is derived from an
/// empty passphrase.
pub fn resolve_init_passphrase<F>(
    config: &VykarConfig,
    label: Option<&str>,
    pre_resolved_configured: Option<Zeroizing<String>>,
    mut prompt: F,
) -> Result<InitPassphrase>
where
    F: FnMut(InitPromptStage, &PromptContext) -> Result<Option<Zeroizing<String>>>,
{
    let provided = |pass: Zeroizing<String>| {
        if pass.is_empty() {
            Err(VykarError::EmptyPassphrase)
        } else {
            Ok(InitPassphrase::Provided(pass))
        }
    };

    if let Some(pass) = pre_resolved_configured {
        return provided(pass);
    }

    if let Some(pass) = configured_passphrase(config)? {
        return provided(pass);
    }

    let ctx = PromptContext {
        repository_label: label.map(|s| s.to_string()),
        repository_url: config.repository.url.clone(),
    };

    let Some(first) = prompt(InitPromptStage::Enter, &ctx)? else {
        return Ok(InitPassphrase::Cancelled);
    };
    // Checked before the confirm prompt so a blank first entry does not make
    // the user type it out twice to learn it was rejected.
    if first.is_empty() {
        return Err(VykarError::EmptyPassphrase);
    }

    let Some(second) = prompt(InitPromptStage::Confirm, &ctx)? else {
        return Ok(InitPassphrase::Cancelled);
    };
    if *first != *second {
        return Err(VykarError::PassphraseMismatch);
    }

    provided(first)
}

// ── Prompt, cache and the per-scope passphrase ─────────────────────────────

/// The skip reason of an operation whose passphrase prompt was dismissed.
pub const PASSPHRASE_PROMPT_CANCELED: &str = "passphrase prompt canceled";

/// What a prompt shows when the repository refused the previous entry.
pub const INCORRECT_PASSPHRASE: &str = "Incorrect passphrase. Please try again.";

/// Prompts per scope, the first included.
pub const MAX_PROMPTS: usize = 3;

/// A frontend's answer to a passphrase prompt.
#[derive(Debug)]
pub enum PromptReply {
    Value(Zeroizing<String>),
    /// Dismissed: the operation is skipped, not failed.
    Cancel,
    /// This frontend cannot ask now (the daemon; a terminal prompt asked to
    /// retry on a stdin that is not a terminal).
    Unavailable,
}

/// How a frontend asks for a passphrase. `retry_reason` is `Some` when the
/// repository refused the previous entry.
pub trait PassphrasePrompt {
    fn ask(&mut self, ctx: &PromptContext, retry_reason: Option<&str>) -> Result<PromptReply>;
}

/// Never asks: the daemon, and callers that rely on a configured source.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoPrompt;

impl PassphrasePrompt for NoPrompt {
    fn ask(&mut self, _: &PromptContext, _: Option<&str>) -> Result<PromptReply> {
        Ok(PromptReply::Unavailable)
    }
}

/// Passphrases a repository accepted, keyed by repository URL and remembered
/// together with the configured source they came under: a value cached
/// under another `encryption.passphrase` / `passcommand` is not returned.
#[derive(Default)]
pub struct PassphraseCache {
    entries: HashMap<String, CachedPassphrase>,
}

struct CachedPassphrase {
    value: Zeroizing<String>,
    source: SourceKey,
}

/// The configured passphrase source, for staleness only. Never printed.
#[derive(PartialEq, Eq)]
struct SourceKey {
    passphrase: Option<Zeroizing<String>>,
    passcommand: Option<String>,
}

impl SourceKey {
    fn of(config: &VykarConfig) -> Self {
        Self {
            passphrase: config.encryption.passphrase.clone().map(Zeroizing::new),
            passcommand: config.encryption.passcommand.clone(),
        }
    }
}

impl PassphraseCache {
    /// The cached value, if its source still matches; a stale entry is dropped.
    pub fn get(&mut self, config: &VykarConfig) -> Option<Zeroizing<String>> {
        let url = &config.repository.url;
        let entry = self.entries.get(url)?;
        if entry.source == SourceKey::of(config) {
            return Some(entry.value.clone());
        }
        self.entries.remove(url);
        None
    }

    pub fn insert(&mut self, config: &VykarConfig, value: Zeroizing<String>) {
        self.entries.insert(
            config.repository.url.clone(),
            CachedPassphrase {
                value,
                source: SourceKey::of(config),
            },
        );
    }

    pub fn remove(&mut self, config: &VykarConfig) {
        self.entries.remove(&config.repository.url);
    }

    /// Same staleness rule as `get`, without removing anything.
    pub fn contains(&self, config: &VykarConfig) -> bool {
        self.entries
            .get(&config.repository.url)
            .is_some_and(|e| e.source == SourceKey::of(config))
    }

    /// The cached value under the current source, without removing a stale entry.
    pub fn peek(&self, config: &VykarConfig) -> Option<Zeroizing<String>> {
        self.entries
            .get(&config.repository.url)
            .filter(|e| e.source == SourceKey::of(config))
            .map(|e| e.value.clone())
    }

    /// Keep only entries of a repository in `repos` whose source is unchanged.
    pub fn retain_for(&mut self, repos: &[ResolvedRepo]) {
        self.entries.retain(|url, entry| {
            repos.iter().any(|r| {
                r.config.repository.url == *url && SourceKey::of(&r.config) == entry.source
            })
        });
    }
}

/// One repository's passphrase for one command or cycle: resolved on first
/// use (cache, then configured source, then prompt), kept for the scope,
/// cached once the repository accepted it.
pub struct RepoPassphrase<'a> {
    cache: &'a mut PassphraseCache,
    prompt: &'a mut dyn PassphrasePrompt,
    config: &'a VykarConfig,
    ctx: PromptContext,
    state: PassState,
    /// An action succeeded with the current value: a later decryption
    /// failure is damage, not a refusal.
    accepted: bool,
    /// A cache hit counts as accepted from the start (one-shot processes,
    /// whose cached values were validated earlier in the same run).
    trust_cache: bool,
    prompts: usize,
}

enum PassState {
    Unresolved,
    Resolved {
        value: Option<Zeroizing<String>>,
        origin: Origin,
    },
    /// The error went to the caller that asked first.
    Failed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Origin {
    Cache,
    Configured,
    Prompt,
    Preset,
}

impl<'a> RepoPassphrase<'a> {
    pub fn new(
        cache: &'a mut PassphraseCache,
        prompt: &'a mut dyn PassphrasePrompt,
        config: &'a VykarConfig,
        label: Option<&str>,
    ) -> Self {
        Self {
            cache,
            prompt,
            config,
            ctx: PromptContext {
                repository_label: label.map(str::to_string),
                repository_url: config.repository.url.clone(),
            },
            state: PassState::Unresolved,
            accepted: false,
            trust_cache: false,
            prompts: 0,
        }
    }

    /// A scope that starts with `value` and never asks or caches (the
    /// fixed-passphrase wrappers used by tests). A refusal is final.
    pub(crate) fn preset(
        cache: &'a mut PassphraseCache,
        prompt: &'a mut dyn PassphrasePrompt,
        config: &'a VykarConfig,
        value: Option<&str>,
    ) -> Self {
        let mut scope = Self::new(cache, prompt, config, None);
        scope.state = PassState::Resolved {
            value: value.map(|v| Zeroizing::new(v.to_string())),
            origin: Origin::Preset,
        };
        scope
    }

    /// Make a cache hit start the scope as accepted (see
    /// [`crate::app::AppService::one_shot`]).
    pub(crate) fn trusting_cache(mut self, trust: bool) -> Self {
        self.trust_cache = trust;
        self
    }

    /// Run `action` with the passphrase and, on `Ok`, cache the value
    /// (unless it came from the cache or a preset). A value the repository
    /// refuses (`is_passphrase_failure`) before this scope's first `Ok` is
    /// replaced and `action` runs again.
    pub fn with<T, E>(
        &mut self,
        action: impl FnMut(Option<&str>) -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E>
    where
        E: From<OpsError> + PassphraseFailure,
    {
        let result = self.run(action);
        if result.is_ok() {
            self.accept();
        }
        result
    }

    /// Open this scope's repository with its passphrase and accept the value
    /// right after the open: resolved on first use, a value the repository
    /// refuses replaced and only the open run again (as in [`Self::with`]).
    /// From then on a decryption failure is the caller's error (damage), not a
    /// refusal. `cancel` is the token the storage backend's retry waits observe.
    pub fn open<E>(
        &mut self,
        opts: crate::repo::OpenOptions,
        cancel: &crate::repo::control::CancelToken,
    ) -> std::result::Result<crate::repo::Repo, E>
    where
        E: From<OpsError> + PassphraseFailure,
    {
        let config = self.config;
        let repo = self.run(|pass| {
            crate::commands::util::open_repo(config, pass, opts.clone(), cancel)
                .map_err(|e| E::from(OpsError::from(e)))
        })?;
        self.accept();
        Ok(repo)
    }

    /// The resolved value, if resolution succeeded (mount hands it to a thread).
    pub fn value(&self) -> Option<Zeroizing<String>> {
        match &self.state {
            PassState::Resolved { value, .. } => value.clone(),
            _ => None,
        }
    }

    /// [`Self::with`] without the accept (the cycle accepts per step).
    pub(crate) fn run<T, E>(
        &mut self,
        mut action: impl FnMut(Option<&str>) -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E>
    where
        E: From<OpsError> + PassphraseFailure,
    {
        if matches!(self.state, PassState::Unresolved) {
            self.resolve()?;
        }
        loop {
            let PassState::Resolved { value, origin } = &self.state else {
                return Err(E::from(OpsError::Engine(VykarError::Other(
                    "passphrase unavailable".into(),
                ))));
            };
            let origin = *origin;
            let failure = match action(value.as_deref().map(String::as_str)) {
                Ok(v) => return Ok(v),
                // Once accepted, a decryption failure is the action's own error.
                Err(e) if self.accepted || !e.is_passphrase_failure() => return Err(e),
                Err(e) => e,
            };
            match origin {
                Origin::Cache => {
                    // Only an accepted value is cached; self-heal if one is refused.
                    let refused = self.value();
                    self.cache.remove(self.config);
                    self.resolve_uncached()?;
                    // The configured source gives the refused value again, so the
                    // entry was not stale: running again would fail the same way.
                    if matches!(
                        self.state,
                        PassState::Resolved {
                            origin: Origin::Configured,
                            ..
                        }
                    ) && self.value().as_deref() == refused.as_deref()
                    {
                        return Err(failure);
                    }
                }
                Origin::Prompt if self.prompts < MAX_PROMPTS => {
                    match self.ask(Some(INCORRECT_PASSPHRASE)) {
                        Ok(PromptReply::Value(v)) => {
                            self.state = PassState::Resolved {
                                value: Some(v),
                                origin: Origin::Prompt,
                            };
                        }
                        Ok(PromptReply::Cancel) => {
                            self.state = PassState::Failed;
                            return Err(canceled());
                        }
                        // The value stays resolved, so later steps try it again.
                        Ok(PromptReply::Unavailable) => return Err(failure),
                        Err(e) => {
                            self.state = PassState::Failed;
                            return Err(E::from(OpsError::from(e)));
                        }
                    }
                }
                Origin::Prompt => {
                    self.state = PassState::Failed;
                    return Err(failure);
                }
                // Not retried; the value stays resolved, so later steps of a
                // cycle (check in particular) try it again.
                Origin::Configured | Origin::Preset => return Err(failure),
            }
        }
    }

    /// An action opened the repository with the current value: from now on a
    /// decryption failure is damage, and the value is cached (unless it came
    /// from the cache or a preset).
    pub(crate) fn accept(&mut self) {
        if let PassState::Resolved { value, origin } = &self.state {
            self.accepted = true;
            if let (Some(v), Origin::Configured | Origin::Prompt) = (value, origin) {
                self.cache.insert(self.config, v.clone());
            }
        }
    }

    /// Resolution failed or the prompt was dismissed or used up; the error
    /// went to the first caller.
    pub(crate) fn failed(&self) -> bool {
        matches!(self.state, PassState::Failed)
    }

    fn resolve<E: From<OpsError>>(&mut self) -> std::result::Result<(), E> {
        if let Some(value) = self.cache.get(self.config) {
            self.state = PassState::Resolved {
                value: Some(value),
                origin: Origin::Cache,
            };
            if self.trust_cache {
                self.accepted = true;
            }
            return Ok(());
        }
        self.resolve_uncached()
    }

    /// The configured source, else the prompt.
    fn resolve_uncached<E: From<OpsError>>(&mut self) -> std::result::Result<(), E> {
        let reply = match configured_passphrase(self.config) {
            Ok(Some(value)) => {
                self.state = PassState::Resolved {
                    value: Some(value),
                    origin: Origin::Configured,
                };
                return Ok(());
            }
            Ok(None) => self.ask(None),
            Err(e) => Err(e),
        };
        match reply {
            Ok(PromptReply::Value(value)) => {
                self.state = PassState::Resolved {
                    value: Some(value),
                    origin: Origin::Prompt,
                };
                Ok(())
            }
            Ok(PromptReply::Cancel) => {
                self.state = PassState::Failed;
                Err(canceled())
            }
            Ok(PromptReply::Unavailable) => {
                self.state = PassState::Failed;
                let name = self
                    .ctx
                    .repository_label
                    .as_deref()
                    .unwrap_or(&self.ctx.repository_url);
                Err(E::from(OpsError::Engine(VykarError::Config(format!(
                    "no passphrase for '{name}': configure encryption.passcommand, \
                     encryption.passphrase, or set VYKAR_PASSPHRASE"
                )))))
            }
            Err(e) => {
                self.state = PassState::Failed;
                Err(E::from(OpsError::from(e)))
            }
        }
    }

    /// Asks the frontend. Only an entry counts toward [`MAX_PROMPTS`]: a
    /// prompt that cannot ask must not use up the scope's tries.
    fn ask(&mut self, retry_reason: Option<&str>) -> Result<PromptReply> {
        let reply = self.prompt.ask(&self.ctx, retry_reason);
        if matches!(reply, Ok(PromptReply::Value(_))) {
            self.prompts += 1;
        }
        reply
    }
}

fn canceled<E: From<OpsError>>() -> E {
    E::from(OpsError::Skipped(SkipReason::new(
        SkipKind::PassphrasePromptCanceled,
        PASSPHRASE_PROMPT_CANCELED,
    )))
}

#[cfg(test)]
mod scope_tests {
    use std::cell::Cell;

    use super::*;
    use crate::tests::helpers::{Reply, ScriptedPrompt, make_test_config, prompt_only};

    fn config() -> VykarConfig {
        make_test_config(std::path::Path::new("/tmp/vykar-scope-test-repo"))
    }

    fn refused<T>() -> std::result::Result<T, OpsError> {
        Err(VykarError::DecryptionFailed.into())
    }

    /// Accepts only `right`.
    fn accepts_right(pass: Option<&str>) -> std::result::Result<&'static str, OpsError> {
        if pass == Some("right") {
            Ok("ok")
        } else {
            Err(VykarError::DecryptionFailed.into())
        }
    }

    fn cached(cache: &mut PassphraseCache, config: &VykarConfig) -> Option<String> {
        cache.get(config).map(|v| v.to_string())
    }

    #[test]
    fn cache_ignores_a_value_cached_under_another_source() {
        let mut a = config();
        a.encryption.passphrase = Some("a".into());
        let mut b = a.clone();
        b.encryption.passphrase = Some("b".into());
        let mut cache = PassphraseCache::default();
        cache.insert(&a, Zeroizing::new("a".into()));
        assert!(cache.contains(&a));
        assert!(!cache.contains(&b));
        assert!(cache.get(&b).is_none());
        assert!(!cache.contains(&a), "the stale entry is dropped");
    }

    #[test]
    fn peek_follows_the_source_without_evicting() {
        let a = config();
        let mut b = a.clone();
        b.encryption.passphrase = Some("b".into());
        let mut cache = PassphraseCache::default();
        cache.insert(&a, Zeroizing::new("a".into()));
        assert_eq!(cache.peek(&a).as_deref().map(String::as_str), Some("a"));
        assert!(cache.peek(&b).is_none());
        assert!(cache.contains(&a), "a stale peek does not evict");
    }

    #[test]
    fn retain_for_drops_removed_and_changed_repos() {
        use crate::tests::helpers::resolved_repo;
        let kept = config();
        let mut gone = config();
        gone.repository.url = "/tmp/vykar-scope-gone".into();
        let mut changed = config();
        changed.repository.url = "/tmp/vykar-scope-changed".into();
        let mut cache = PassphraseCache::default();
        for c in [&kept, &gone, &changed] {
            cache.insert(c, Zeroizing::new("p".into()));
        }
        let mut changed_now = changed.clone();
        changed_now.encryption.passcommand = Some("echo other".into());

        cache.retain_for(&[
            resolved_repo(kept.clone(), Vec::new()),
            resolved_repo(changed_now.clone(), Vec::new()),
        ]);

        assert!(cache.contains(&kept));
        assert!(!cache.contains(&gone));
        assert!(!cache.contains(&changed));
        assert!(!cache.contains(&changed_now));
    }

    #[test]
    fn wrong_then_right_prompt_caches_only_the_right_value() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        let mut prompt = ScriptedPrompt::new(&[Reply::Value("wrong"), Reply::Value("right")]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        assert_eq!(scope.with(accepts_right).unwrap(), "ok");
        drop(scope);
        assert_eq!(
            prompt.reasons,
            vec![None, Some(INCORRECT_PASSPHRASE.to_string())]
        );
        assert_eq!(cached(&mut cache, &config).as_deref(), Some("right"));
    }

    #[test]
    fn configured_wrong_passphrase_fails_without_asking() {
        let mut config = config();
        config.encryption.passphrase = Some("cfg".into());
        let mut cache = PassphraseCache::default();
        let mut prompt = ScriptedPrompt::new(&[]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        let runs = Cell::new(0);
        let err = scope
            .with(|p| {
                runs.set(runs.get() + 1);
                accepts_right(p)
            })
            .unwrap_err();
        assert!(matches!(
            err,
            OpsError::Engine(VykarError::DecryptionFailed)
        ));
        assert!(
            !scope.failed(),
            "a configured refusal does not end the scope"
        );
        drop(scope);
        assert_eq!(runs.get(), 1);
        assert_eq!(prompt.asks(), 0);
        assert!(!cache.contains(&config));
    }

    #[test]
    fn dismissed_prompt_is_a_skip_and_caches_nothing() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        let mut prompt = ScriptedPrompt::new(&[Reply::Cancel]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        let err = scope
            .with(|_| -> OpsResultUnit { unreachable!("the action must not run") })
            .unwrap_err();
        assert!(matches!(
            err,
            OpsError::Skipped(r)
                if r.kind == SkipKind::PassphrasePromptCanceled && r.message == PASSPHRASE_PROMPT_CANCELED
        ));
        assert!(scope.failed());
        drop(scope);
        assert!(!cache.contains(&config));
    }

    type OpsResultUnit = std::result::Result<(), OpsError>;

    #[test]
    fn cached_value_is_used_without_asking() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        cache.insert(&config, Zeroizing::new("right".into()));
        let mut prompt = ScriptedPrompt::new(&[]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        assert_eq!(scope.with(accepts_right).unwrap(), "ok");
        drop(scope);
        assert_eq!(prompt.asks(), 0);
    }

    #[test]
    fn poisoned_cache_is_evicted_and_asked_once() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        cache.insert(&config, Zeroizing::new("stale".into()));
        let mut prompt = ScriptedPrompt::new(&[Reply::Value("right")]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        assert_eq!(scope.with(accepts_right).unwrap(), "ok");
        drop(scope);
        assert_eq!(prompt.reasons, vec![None]);
        assert_eq!(cached(&mut cache, &config).as_deref(), Some("right"));
    }

    /// One-shot mode (A2): a cache hit is trusted, so a decryption failure is
    /// returned unchanged, the action runs once and the entry is kept.
    #[test]
    fn refused_cached_value_the_source_gives_again_is_not_run_twice() {
        let mut config = config();
        config.encryption.passphrase = Some("same".into());
        config.encryption.passcommand = None;
        let mut cache = PassphraseCache::default();
        cache.insert(&config, Zeroizing::new("same".into()));
        let mut prompt = ScriptedPrompt::new(&[]);
        let runs = Cell::new(0);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        let err = scope
            .with(|_| {
                runs.set(runs.get() + 1);
                refused::<()>()
            })
            .unwrap_err();
        assert!(err.is_passphrase_failure());
        assert!(!scope.failed(), "the value stays for later steps");
        drop(scope);
        assert_eq!(runs.get(), 1);
        assert_eq!(prompt.asks(), 0);
        assert!(!cache.contains(&config), "the refused entry is evicted");
    }

    #[test]
    fn trusted_cache_hit_is_not_evicted() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        cache.insert(&config, Zeroizing::new("right".into()));
        let mut prompt = ScriptedPrompt::new(&[]);
        let runs = Cell::new(0);
        let mut scope =
            RepoPassphrase::new(&mut cache, &mut prompt, &config, None).trusting_cache(true);
        let err = scope
            .with(|_| {
                runs.set(runs.get() + 1);
                refused::<()>()
            })
            .unwrap_err();
        assert!(matches!(
            err,
            OpsError::Engine(VykarError::DecryptionFailed)
        ));
        drop(scope);
        assert_eq!(runs.get(), 1);
        assert_eq!(prompt.asks(), 0);
        assert!(cache.contains(&config));

        // The default mode evicts it and resolves again.
        let mut prompt = ScriptedPrompt::new(&[Reply::Value("right")]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        let runs = Cell::new(0);
        let r = scope.with(|p| {
            runs.set(runs.get() + 1);
            if runs.get() == 1 {
                refused()
            } else {
                Ok(p.map(str::to_string))
            }
        });
        drop(scope);
        assert_eq!(r.unwrap().as_deref(), Some("right"));
        assert_eq!(runs.get(), 2);
        assert_eq!(prompt.asks(), 1);
    }

    #[test]
    fn three_wrong_prompts_return_the_refusal() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        let mut prompt =
            ScriptedPrompt::new(&[Reply::Value("a"), Reply::Value("b"), Reply::Value("c")]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        let err = scope.with(accepts_right).unwrap_err();
        assert!(matches!(
            err,
            OpsError::Engine(VykarError::DecryptionFailed)
        ));
        assert!(scope.failed());
        drop(scope);
        assert_eq!(prompt.asks(), MAX_PROMPTS);
        assert!(!cache.contains(&config));
    }

    #[test]
    fn a_skip_from_the_action_is_returned_without_retry_or_caching() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        let mut prompt = ScriptedPrompt::new(&[Reply::Value("a")]);
        let runs = Cell::new(0);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        let err = scope
            .with(|_| -> OpsResultUnit {
                runs.set(runs.get() + 1);
                Err(OpsError::Skipped(SkipReason::new(SkipKind::Hook, "hook")))
            })
            .unwrap_err();
        assert!(matches!(err, OpsError::Skipped(r) if r.message == "hook"));
        drop(scope);
        assert_eq!(runs.get(), 1);
        assert_eq!(prompt.asks(), 1);
        assert!(!cache.contains(&config));
    }

    #[test]
    fn an_engine_passphrase_failure_in_ops_error_retries() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        let mut prompt = ScriptedPrompt::new(&[Reply::Value("wrong"), Reply::Value("right")]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        let r = scope.with(accepts_right);
        drop(scope);
        assert_eq!(r.unwrap(), "ok");
        assert_eq!(prompt.asks(), 2);
        assert_eq!(cached(&mut cache, &config).as_deref(), Some("right"));
    }

    #[test]
    fn unavailable_retry_returns_the_refusal() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        let mut prompt = ScriptedPrompt::new(&[Reply::Value("wrong"), Reply::Unavailable]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        let err = scope.with(accepts_right).unwrap_err();
        assert!(matches!(
            err,
            OpsError::Engine(VykarError::DecryptionFailed)
        ));
        assert!(!scope.failed(), "the value stays for later steps (A1)");
        drop(scope);
        assert_eq!(prompt.asks(), 2);
        assert!(!cache.contains(&config));
    }

    #[test]
    fn unavailable_first_ask_is_a_config_error() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        let mut prompt = NoPrompt;
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, Some("nas"));
        let err = scope
            .with(|_| -> OpsResultUnit { unreachable!("the action must not run") })
            .unwrap_err();
        assert!(
            matches!(&err, OpsError::Engine(VykarError::Config(m)) if m.contains("passcommand") && m.contains("'nas'")),
            "{err}"
        );
        assert!(scope.failed());
    }

    #[test]
    fn value_is_kept_for_the_scope_without_success() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        let mut prompt = ScriptedPrompt::new(&[Reply::Value("v")]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        for _ in 0..2 {
            let err = scope
                .with(|_| -> OpsResultUnit { Err(VykarError::Interrupted.into()) })
                .unwrap_err();
            assert!(matches!(err, OpsError::Engine(VykarError::Interrupted)));
        }
        assert_eq!(scope.value().as_deref().map(String::as_str), Some("v"));
        drop(scope);
        assert_eq!(prompt.asks(), 1);
        assert!(!cache.contains(&config));
    }

    #[test]
    fn after_accept_a_decryption_failure_is_not_retried() {
        let mut config = config();
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        let mut prompt = ScriptedPrompt::new(&[Reply::Value("v")]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        scope.with(|_| Ok::<_, OpsError>(())).unwrap();
        let err = scope.with(|_| refused::<()>()).unwrap_err();
        assert!(matches!(
            err,
            OpsError::Engine(VykarError::DecryptionFailed)
        ));
        assert!(!scope.failed());
        drop(scope);
        assert_eq!(prompt.asks(), 1);
    }

    #[test]
    fn preset_is_never_cached_and_never_retried() {
        let config = config();
        let mut cache = PassphraseCache::default();
        let mut prompt = ScriptedPrompt::new(&[]);
        let mut scope = RepoPassphrase::preset(&mut cache, &mut prompt, &config, Some("p"));
        assert!(scope.with(|_| refused::<()>()).is_err());
        scope
            .with(|p| {
                assert_eq!(p, Some("p"));
                Ok::<_, OpsError>(())
            })
            .unwrap();
        drop(scope);
        assert_eq!(prompt.asks(), 0);
        assert!(!cache.contains(&config));
    }

    // ── Against a real repository ──────────────────────────────────────────
    //
    // Driven by a real `Repo::open`, because the failure these must catch is
    // a change in which variant that open returns; a hand-written
    // `Err(DecryptionFailed)` stub would keep passing throughout.

    fn open_with(
        repo_dir: &std::path::Path,
    ) -> impl FnMut(Option<&str>) -> std::result::Result<(), OpsError> + use<> {
        let repo_dir = repo_dir.to_path_buf();
        move |pass: Option<&str>| {
            let storage = Box::new(vykar_storage::local_backend::LocalBackend::new(
                repo_dir.to_str().expect("temp path is UTF-8"),
            )?);
            crate::repo::Repo::open(storage, pass, None, crate::repo::OpenOptions::new())
                .map(|_| ())
                .map_err(OpsError::from)
        }
    }

    /// An encrypted repository at `<tmp>/repo` whose passphrase is `pass`,
    /// and a prompt-only config for it.
    fn encrypted_repo(tmp: &tempfile::TempDir, pass: &str) -> (std::path::PathBuf, VykarConfig) {
        let repo_dir = tmp.path().join("repo");
        std::fs::create_dir_all(&repo_dir).expect("create repo dir");
        let config = make_test_config(&repo_dir);
        let storage = Box::new(
            vykar_storage::local_backend::LocalBackend::new(
                repo_dir.to_str().expect("temp path is UTF-8"),
            )
            .expect("local backend"),
        );
        crate::testing::with_cheap_kdf(|| {
            crate::repo::Repo::init(
                storage,
                crate::repo::EncryptionMode::Aes256Gcm,
                crate::repo::ChunkerParams::default(),
                Some(pass),
                None,
                None,
            )
        })
        .expect("init encrypted repo");
        (repo_dir, config)
    }

    #[test]
    fn a_real_wrong_passphrase_open_still_re_prompts() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (repo_dir, mut config) = encrypted_repo(&tmp, "right-passphrase");
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        let mut prompt = ScriptedPrompt::new(&[
            Reply::Value("wrong-passphrase"),
            Reply::Value("right-passphrase"),
        ]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        scope
            .with(open_with(&repo_dir))
            .expect("the second entry opens");
        drop(scope);
        assert_eq!(
            prompt.asks(),
            2,
            "a typo must trigger exactly one re-prompt"
        );
        assert_eq!(
            cached(&mut cache, &config).as_deref(),
            Some("right-passphrase")
        );
    }

    #[test]
    fn a_real_wrong_passphrase_open_evicts_a_stale_cached_entry() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (repo_dir, mut config) = encrypted_repo(&tmp, "right-passphrase");
        let _env = prompt_only(&mut config);
        let mut cache = PassphraseCache::default();
        cache.insert(&config, Zeroizing::new("stale-passphrase".into()));
        let mut prompt = ScriptedPrompt::new(&[Reply::Value("right-passphrase")]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        scope
            .with(open_with(&repo_dir))
            .expect("the fresh entry opens");
        drop(scope);
        assert_eq!(
            cached(&mut cache, &config).as_deref(),
            Some("right-passphrase"),
            "the stale entry must be evicted and replaced, not kept"
        );
    }

    /// A failure a wrong passphrase *cannot* explain must not burn retries.
    /// The two copies are corrupted **differently** on purpose: identical
    /// bytes take the matched-copies path, so a same-bytes fixture would never
    /// reach the divergent branch where the classification is easiest to get
    /// wrong.
    #[test]
    fn a_corrupt_key_is_not_treated_as_a_passphrase_problem() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (repo_dir, mut config) = encrypted_repo(&tmp, "right-passphrase");
        let _env = prompt_only(&mut config);
        let keys = repo_dir.join("keys");
        std::fs::write(keys.join("repokey"), b"not msgpack at all").expect("corrupt primary");
        let mut mangled = vykar_crypto::key::inspect_key_blob(
            &std::fs::read(keys.join("repokey.2")).expect("read secondary"),
        )
        .expect("the secondary starts out well-formed");
        mangled.nonce = vec![0u8; 8];
        std::fs::write(
            keys.join("repokey.2"),
            rmp_serde::to_vec(&mangled).expect("re-encode"),
        )
        .expect("corrupt secondary");

        let mut cache = PassphraseCache::default();
        let mut prompt =
            ScriptedPrompt::new(&[Reply::Value("right-passphrase"), Reply::Value("again")]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        let err = scope
            .with(open_with(&repo_dir))
            .expect_err("a corrupt key must fail the open");
        drop(scope);
        assert!(!err.is_passphrase_failure(), "{err}");
        assert_eq!(prompt.asks(), 1, "proven corruption must not re-prompt");
    }

    /// Argon2 parameters inside every ceiling but below Argon2's own
    /// `memory >= 8 * parallelism` floor cannot derive a key. That is
    /// corruption, and must not cost the operator a re-prompt.
    #[test]
    fn impossible_argon2_params_are_not_treated_as_a_passphrase_problem() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (repo_dir, mut config) = encrypted_repo(&tmp, "right-passphrase");
        let _env = prompt_only(&mut config);
        let keys = repo_dir.join("keys");
        let mut mangled = vykar_crypto::key::inspect_key_blob(
            &std::fs::read(keys.join("repokey")).expect("read primary"),
        )
        .expect("the primary starts out well-formed");
        mangled.kdf.memory_cost = 1;
        mangled.kdf.parallelism = 16;
        let bytes = rmp_serde::to_vec(&mangled).expect("re-encode");
        for name in ["repokey", "repokey.2"] {
            std::fs::write(keys.join(name), &bytes).expect("write mangled copy");
        }

        let mut cache = PassphraseCache::default();
        let mut prompt =
            ScriptedPrompt::new(&[Reply::Value("right-passphrase"), Reply::Value("again")]);
        let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &config, None);
        let err = scope
            .with(open_with(&repo_dir))
            .expect_err("an underivable key must fail the open");
        drop(scope);
        assert!(!err.is_passphrase_failure(), "{err}");
        assert_eq!(prompt.asks(), 1, "proven corruption must not re-prompt");
    }
}
