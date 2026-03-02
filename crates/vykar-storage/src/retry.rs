use std::fmt;
use std::time::Duration;

use crate::RetryConfig;

/// Retry a closure on transient `ureq::Error`s with exponential backoff + jitter.
///
/// Used by S3 and REST backends which share the same HTTP error model.
#[allow(clippy::result_large_err)]
pub fn retry_http<T>(
    config: &RetryConfig,
    op_name: &str,
    backend_label: &str,
    f: impl Fn() -> std::result::Result<T, ureq::Error>,
) -> std::result::Result<T, ureq::Error> {
    let mut delay_ms = config.retry_delay_ms;
    let mut last_err = None;

    for attempt in 0..=config.max_retries {
        if attempt > 0 {
            let jitter = rand::random::<u64>() % delay_ms.max(1);
            std::thread::sleep(Duration::from_millis(delay_ms + jitter));
            delay_ms = (delay_ms * 2).min(config.retry_max_delay_ms);
        }
        match f() {
            Ok(val) => return Ok(val),
            Err(e) if is_retryable_http(&e) && attempt < config.max_retries => {
                tracing::warn!(
                    "{backend_label} {op_name}: transient error (attempt {}/{}), retrying: {e}",
                    attempt + 1,
                    config.max_retries,
                );
                last_err = Some(e);
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap())
}

/// Whether an HTTP error is transient and worth retrying.
pub fn is_retryable_http(err: &ureq::Error) -> bool {
    match err {
        ureq::Error::Transport(_) => true,
        ureq::Error::Status(code, _) => *code == 429 || *code >= 500,
    }
}

/// Unified error type for HTTP request + body read operations.
///
/// Keeps the retry module decoupled from `VykarError` — conversion to
/// application error types lives in each backend.
pub enum HttpRetryError {
    /// HTTP-level error (may be retryable: transport, 429, 5xx).
    Http(Box<ureq::Error>),
    /// Body read I/O error (may be retryable: connection reset, EOF, etc.).
    BodyIo(std::io::Error),
    /// Application error message (never retried).
    Permanent(String),
}

impl HttpRetryError {
    /// Wrap a `ureq::Error` (boxed to keep the enum small).
    pub fn http(e: ureq::Error) -> Self {
        HttpRetryError::Http(Box::new(e))
    }
}

impl fmt::Display for HttpRetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpRetryError::Http(e) => write!(f, "{e}"),
            HttpRetryError::BodyIo(e) => write!(f, "body read error: {e}"),
            HttpRetryError::Permanent(msg) => write!(f, "{msg}"),
        }
    }
}

/// Whether an I/O error is transient and worth retrying.
pub fn is_retryable_io(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
    )
}

fn is_retryable_http_body(err: &HttpRetryError) -> bool {
    match err {
        HttpRetryError::Http(e) => is_retryable_http(e.as_ref()),
        HttpRetryError::BodyIo(e) => is_retryable_io(e),
        HttpRetryError::Permanent(_) => false,
    }
}

/// Retry a closure that performs both an HTTP request and body read.
///
/// Same exponential-backoff-with-jitter loop as [`retry_http`] but operates
/// on [`HttpRetryError`] so that transient body-read I/O errors are also retried.
pub fn retry_http_body<T>(
    config: &RetryConfig,
    op_name: &str,
    backend_label: &str,
    f: impl Fn() -> std::result::Result<T, HttpRetryError>,
) -> std::result::Result<T, HttpRetryError> {
    let mut delay_ms = config.retry_delay_ms;
    let mut last_err = None;

    for attempt in 0..=config.max_retries {
        if attempt > 0 {
            let jitter = rand::random::<u64>() % delay_ms.max(1);
            std::thread::sleep(Duration::from_millis(delay_ms + jitter));
            delay_ms = (delay_ms * 2).min(config.retry_max_delay_ms);
        }
        match f() {
            Ok(val) => return Ok(val),
            Err(e) if is_retryable_http_body(&e) && attempt < config.max_retries => {
                tracing::warn!(
                    "{backend_label} {op_name}: transient error (attempt {}/{}), retrying: {e}",
                    attempt + 1,
                    config.max_retries,
                );
                last_err = Some(e);
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_io_errors() {
        let retryable_kinds = [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::Interrupted,
        ];
        for kind in retryable_kinds {
            let err = std::io::Error::new(kind, "test");
            assert!(is_retryable_io(&err), "{kind:?} should be retryable");
        }
    }

    #[test]
    fn non_retryable_io_errors() {
        let non_retryable_kinds = [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::InvalidData,
            std::io::ErrorKind::InvalidInput,
            std::io::ErrorKind::AlreadyExists,
        ];
        for kind in non_retryable_kinds {
            let err = std::io::Error::new(kind, "test");
            assert!(!is_retryable_io(&err), "{kind:?} should NOT be retryable");
        }
    }

    #[test]
    fn permanent_is_never_retryable() {
        let err = HttpRetryError::Permanent("bad data".into());
        assert!(!is_retryable_http_body(&err));
    }

    #[test]
    fn body_io_retryable_classification() {
        let retryable =
            HttpRetryError::BodyIo(std::io::Error::new(std::io::ErrorKind::ConnectionReset, ""));
        assert!(is_retryable_http_body(&retryable));

        let non_retryable =
            HttpRetryError::BodyIo(std::io::Error::new(std::io::ErrorKind::InvalidData, ""));
        assert!(!is_retryable_http_body(&non_retryable));
    }
}
