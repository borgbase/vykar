use vykar_core::app::OpsError;
use vykar_types::error::VykarError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliError {
    #[error(transparent)]
    Vykar(#[from] VykarError),

    /// A hook error or a skip. An engine error from the ops layer is
    /// `Vykar` (see the `From<OpsError>` impl).
    #[error(transparent)]
    Ops(OpsError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    Msg(String),
}

/// An engine error stays `Vykar`, so matches on its variants keep working.
impl From<OpsError> for CliError {
    fn from(e: OpsError) -> Self {
        match e {
            OpsError::Engine(e) => Self::Vykar(e),
            e => Self::Ops(e),
        }
    }
}

impl From<String> for CliError {
    fn from(s: String) -> Self {
        Self::Msg(s)
    }
}

impl From<&str> for CliError {
    fn from(s: &str) -> Self {
        Self::Msg(s.into())
    }
}

pub(crate) type CliResult<T> = Result<T, CliError>;

/// A skip is the ops layer's; every other variant is a failure.
impl vykar_core::app::HookError for CliError {
    fn is_skip(&self) -> bool {
        matches!(self, Self::Ops(OpsError::Skipped(_)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ops_errors_keep_engine_errors_typed() {
        assert!(matches!(
            CliError::from(OpsError::Engine(VykarError::Interrupted)),
            CliError::Vykar(VykarError::Interrupted)
        ));
        assert!(matches!(
            CliError::from(OpsError::Skipped("r".into())),
            CliError::Ops(OpsError::Skipped(r)) if r == "r"
        ));
    }

    #[test]
    fn only_an_ops_skip_is_a_hook_skip() {
        use vykar_core::app::HookError;
        assert!(CliError::Ops(OpsError::Skipped("r".into())).is_skip());
        assert!(!CliError::Ops(OpsError::Hook("h".into())).is_skip());
        assert!(!CliError::Vykar(VykarError::Interrupted).is_skip());
        assert!(!CliError::Msg("m".into()).is_skip());
    }
}
