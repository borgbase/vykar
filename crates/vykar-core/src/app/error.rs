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
    Skipped(String),
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

/// What a failed cycle step was, for frontends that act on it and, later,
/// for JSON output. `Passphrase` is the same predicate the cycle's retry uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

impl From<&OpsError> for StepError {
    fn from(e: &OpsError) -> Self {
        let kind = match e {
            // First: a refused passphrase is also an engine error.
            e if e.is_passphrase_failure() => StepErrorKind::Passphrase,
            OpsError::Hook(_) => StepErrorKind::Hook,
            OpsError::Engine(VykarError::RepoNotFound(_)) => StepErrorKind::RepoNotFound,
            OpsError::Engine(VykarError::Interrupted) => StepErrorKind::Interrupted,
            OpsError::Engine(VykarError::Config(_)) => StepErrorKind::Config,
            // A skip never reaches `Failed`: the cycle reports it as skipped.
            OpsError::Engine(_) | OpsError::Skipped(_) => StepErrorKind::Other,
        };
        Self::new(kind, e.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn texts_and_passphrase_failures() {
        assert_eq!(OpsError::Skipped("r".into()).to_string(), "skipped: r");
        assert_eq!(OpsError::Hook("h".into()).to_string(), "hook error: h");
        let e = OpsError::from(VykarError::DecryptionFailed);
        assert_eq!(e.to_string(), VykarError::DecryptionFailed.to_string());
        assert!(e.is_passphrase_failure());
        assert!(!OpsError::Skipped("r".into()).is_passphrase_failure());
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
            (OpsError::Skipped("s".into()), StepErrorKind::Other),
        ];
        for (e, kind) in cases {
            let step = StepError::from(&e);
            assert_eq!(step.kind, kind, "{e}");
            assert_eq!(step.message, e.to_string());
            assert_eq!(step.to_string(), step.message);
        }
    }

    #[test]
    fn ops_error_is_a_hook_error() {
        assert!(OpsError::Skipped("s".into()).is_skip());
        assert!(!OpsError::Hook("h".into()).is_skip());
        assert!(!OpsError::from(VykarError::Interrupted).is_skip());
    }
}
