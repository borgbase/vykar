use vykar_types::error::VykarError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliError {
    #[error(transparent)]
    Vykar(#[from] VykarError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    Msg(String),
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
