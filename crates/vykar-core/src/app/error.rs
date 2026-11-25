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
}
