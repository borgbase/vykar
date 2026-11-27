//! The ops layer's error: an engine error, or an outcome only hooks and the
//! backup cycle produce.

use thiserror::Error;
use vykar_types::error::VykarError;

/// Errors of the ops layer: hooks, the backup cycle and the hooked command
/// wrappers. Either an engine error, or an outcome only this layer has.
#[derive(Debug, Error)]
pub enum OpsError {
    #[error(transparent)]
    Engine(#[from] VykarError),
    /// A hook could not run or exited non-zero.
    #[error("hook error: {0}")]
    Hook(String),
    /// The operation was deliberately not run, e.g. a `before` hook asked for
    /// a skip or a passphrase prompt was dismissed. Not a failure: callers
    /// report it without firing `failed` hooks or returning an error status.
    #[error("skipped: {0}")]
    Skipped(SkipReason),
}

/// Why an operation deliberately did not run. Not a failure: no `failed`
/// hooks, no error status. `kind` is for frontends and JSON; `message` is
/// the text shown to people.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SkipReason {
    pub kind: SkipKind,
    pub message: String,
}

/// Why a skip happened. The JSON names are part of schema 1: only add
/// variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipKind {
    /// A `before` hook exited with `HOOK_SKIP_EXIT_CODE`; also a backup whose
    /// every source was skipped by its source hooks.
    Hook,
    /// The passphrase prompt was dismissed.
    PassphrasePromptCanceled,
    /// A local repository whose `config` is missing (preflight).
    RepoUnavailable,
    /// Compact in a cycle on an append-only server.
    AppendOnlyServer,
    /// Prune in a cycle without a retention rule.
    NoRetentionRules,
    /// A cycle step after a failed backup.
    BackupFailed,
    /// A cycle step after a skipped backup.
    BackupSkipped,
    /// Check in a cycle that has no check configured.
    CheckNotConfigured,
    /// Check in a cycle before its interval has passed.
    CheckNotDue,
}

impl SkipReason {
    #[must_use]
    pub fn new(kind: SkipKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

pub type OpsResult<T> = std::result::Result<T, OpsError>;

impl OpsError {
    /// The engine error, if this is one.
    pub fn engine(&self) -> Option<&VykarError> {
        match self {
            Self::Engine(e) => Some(e),
            _ => None,
        }
    }

    /// [`VykarError::is_passphrase_failure`] of the engine error; false for
    /// hook errors and skips.
    pub fn is_passphrase_failure(&self) -> bool {
        self.engine().is_some_and(VykarError::is_passphrase_failure)
    }
}

/// What a failed cycle step was, for frontends that act on it and for JSON
/// output. `Passphrase` is the same predicate the cycle's retry uses. The
/// JSON names are part of schema 1: only add variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepErrorKind {
    /// A failure that a wrong passphrase explains ([`VykarError::is_passphrase_failure`]).
    /// The same error also means a damaged object, so after a step has
    /// opened the repository it is damage, not a refusal.
    Passphrase,
    /// [`VykarError::RepoNotFound`].
    RepoNotFound,
    /// [`VykarError::Interrupted`].
    Interrupted,
    /// A `before` hook failed or could not run ([`OpsError::Hook`]).
    Hook,
    /// [`VykarError::Config`], or a cycle input the configuration cannot
    /// satisfy (an unknown source filter).
    Config,
    /// Anything else, including a backup with failed sources and a check
    /// that found errors.
    Other,
}

/// A failed cycle step: its kind, and the text frontends show (the error's
/// `Display`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StepError {
    pub kind: StepErrorKind,
    pub message: String,
}

impl StepError {
    #[must_use]
    pub fn new(kind: StepErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl From<&VykarError> for StepError {
    fn from(e: &VykarError) -> Self {
        let kind = match e {
            // First: a refused passphrase can be any of several variants.
            e if e.is_passphrase_failure() => StepErrorKind::Passphrase,
            VykarError::RepoNotFound(_) => StepErrorKind::RepoNotFound,
            VykarError::Interrupted => StepErrorKind::Interrupted,
            VykarError::Config(_) => StepErrorKind::Config,
            _ => StepErrorKind::Other,
        };
        Self::new(kind, e.to_string())
    }
}

impl From<&OpsError> for StepError {
    fn from(e: &OpsError) -> Self {
        match e {
            // `Engine` is transparent: its text is the engine error's.
            OpsError::Engine(v) => Self::from(v),
            OpsError::Hook(_) => Self::new(StepErrorKind::Hook, e.to_string()),
            // A skip never reaches `Failed`: the cycle reports it as skipped.
            OpsError::Skipped(_) => Self::new(StepErrorKind::Other, e.to_string()),
        }
    }
}

impl std::fmt::Display for StepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// What the hook lifecycle needs of an action's error: its text (the
/// `failed` hooks' `VYKAR_ERROR`), and whether it is a deliberate skip, which
/// fires neither `after` nor `failed`. Lets a frontend run its own error
/// type through the hooks without flattening it.
pub trait HookError: std::fmt::Display + From<OpsError> {
    fn is_skip(&self) -> bool;
}

impl HookError for OpsError {
    fn is_skip(&self) -> bool {
        matches!(self, Self::Skipped(_))
    }
}

/// Whether a wrong passphrase could explain an error, for the passphrase
/// retry. Implemented by every error a passphrase-taking action returns.
pub trait PassphraseFailure {
    fn is_passphrase_failure(&self) -> bool;
}

impl PassphraseFailure for OpsError {
    fn is_passphrase_failure(&self) -> bool {
        OpsError::is_passphrase_failure(self)
    }
}

impl PassphraseFailure for VykarError {
    fn is_passphrase_failure(&self) -> bool {
        VykarError::is_passphrase_failure(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn texts_and_passphrase_failures() {
        assert_eq!(
            OpsError::Skipped(SkipReason::new(SkipKind::Hook, "r")).to_string(),
            "skipped: r"
        );
        assert_eq!(OpsError::Hook("h".into()).to_string(), "hook error: h");
        let e = OpsError::from(VykarError::DecryptionFailed);
        assert_eq!(e.to_string(), VykarError::DecryptionFailed.to_string());
        assert!(e.is_passphrase_failure());
        assert!(!OpsError::Skipped(SkipReason::new(SkipKind::Hook, "r")).is_passphrase_failure());
        assert!(!OpsError::Hook("h".into()).is_passphrase_failure());
        assert!(!OpsError::from(VykarError::Interrupted).is_passphrase_failure());
    }

    #[test]
    fn step_error_kinds_from_ops_errors() {
        let cases = [
            (
                OpsError::from(VykarError::DecryptionFailed),
                StepErrorKind::Passphrase,
            ),
            (
                OpsError::from(VykarError::RepositoryKeyUnwrapFailed(
                    "wrong passphrase".into(),
                )),
                StepErrorKind::Passphrase,
            ),
            (
                OpsError::from(VykarError::RepoNotFound("/r".into())),
                StepErrorKind::RepoNotFound,
            ),
            (
                OpsError::from(VykarError::Interrupted),
                StepErrorKind::Interrupted,
            ),
            (
                OpsError::from(VykarError::Config("bad".into())),
                StepErrorKind::Config,
            ),
            (OpsError::Hook("h".into()), StepErrorKind::Hook),
            (
                OpsError::from(VykarError::Other("x".into())),
                StepErrorKind::Other,
            ),
            (
                OpsError::Skipped(SkipReason::new(SkipKind::Hook, "s")),
                StepErrorKind::Other,
            ),
        ];
        for (e, kind) in cases {
            let step = StepError::from(&e);
            assert_eq!(step.kind, kind, "{e}");
            assert_eq!(step.message, e.to_string());
            assert_eq!(step.to_string(), step.message);
        }
    }

    #[test]
    fn skip_reasons_keep_their_text() {
        let reason = SkipReason::new(SkipKind::Hook, "r");
        assert_eq!(reason.to_string(), "r");
        let e = OpsError::Skipped(reason);
        assert_eq!(e.to_string(), "skipped: r");
        assert!(e.is_skip());
    }

    #[test]
    fn json_names_of_kinds() {
        use StepErrorKind as E;
        for kind in [
            E::Passphrase,
            E::RepoNotFound,
            E::Interrupted,
            E::Hook,
            E::Config,
            E::Other,
        ] {
            // Exhaustive: a new variant does not compile until it is pinned.
            let name = match kind {
                E::Passphrase => "passphrase",
                E::RepoNotFound => "repo_not_found",
                E::Interrupted => "interrupted",
                E::Hook => "hook",
                E::Config => "config",
                E::Other => "other",
            };
            assert_eq!(serde_json::to_string(&kind).unwrap(), format!("\"{name}\""));
        }
        use SkipKind as S;
        for kind in [
            S::Hook,
            S::PassphrasePromptCanceled,
            S::RepoUnavailable,
            S::AppendOnlyServer,
            S::NoRetentionRules,
            S::BackupFailed,
            S::BackupSkipped,
            S::CheckNotConfigured,
            S::CheckNotDue,
        ] {
            let name = match kind {
                S::Hook => "hook",
                S::PassphrasePromptCanceled => "passphrase_prompt_canceled",
                S::RepoUnavailable => "repo_unavailable",
                S::AppendOnlyServer => "append_only_server",
                S::NoRetentionRules => "no_retention_rules",
                S::BackupFailed => "backup_failed",
                S::BackupSkipped => "backup_skipped",
                S::CheckNotConfigured => "check_not_configured",
                S::CheckNotDue => "check_not_due",
            };
            assert_eq!(serde_json::to_string(&kind).unwrap(), format!("\"{name}\""));
        }
    }

    #[test]
    fn step_error_from_engine_error() {
        for v in [
            VykarError::DecryptionFailed,
            VykarError::RepositoryKeyUnwrapFailed("wrong passphrase".into()),
            VykarError::RepoNotFound("/r".into()),
            VykarError::Interrupted,
            VykarError::Config("bad".into()),
            VykarError::Other("x".into()),
        ] {
            let direct = StepError::from(&v);
            let via_ops = StepError::from(&OpsError::from(v));
            assert_eq!(direct, via_ops);
        }
    }

    #[test]
    fn ops_error_is_a_hook_error() {
        assert!(OpsError::Skipped(SkipReason::new(SkipKind::Hook, "s")).is_skip());
        assert!(!OpsError::Hook("h".into()).is_skip());
        assert!(!OpsError::from(VykarError::Interrupted).is_skip());
    }
}
