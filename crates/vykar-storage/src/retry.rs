use std::fmt;
use std::time::Duration;

use crate::RetryConfig;

/// Whether a ureq transport-level error is transient and worth retrying.
fn is_retryable_transport(err: &ureq::Error) -> bool {
    match err {
        ureq::Error::Timeout(_) | ureq::Error::ConnectionFailed | ureq::Error::HostNotFound => true,
        ureq::Error::Io(e) => is_retryable_io(e),
        _ => false,
    }
}

/// Retry a closure that makes an HTTP request, with exponential backoff + jitter.
///
/// Used by S3 and REST backends. The agent must be configured with
/// `http_status_as_error(false)` so that HTTP 4xx/5xx responses arrive as
/// `Ok(Response)`, allowing the `handle_response` closure to inspect the body
/// (important for S3 XML error diagnostics) and classify the status for retry.
///
/// The `f` closure performs the HTTP call, returning transport errors only.
/// The `handle_response` closure processes the HTTP response (including status
/// checks and body reads), returning application-level results or
/// retry-classified errors.
pub fn retry_http<T>(
    config: &RetryConfig,
    op_name: &str,
    backend_label: &str,
    f: impl Fn() -> std::result::Result<http::Response<ureq::Body>, ureq::Error>,
    handle_response: impl Fn(http::Response<ureq::Body>) -> std::result::Result<T, HttpRetryError>,
) -> std::result::Result<T, HttpRetryError> {
    let mut delay_ms = config.retry_delay_ms;
    let mut last_err = None;

    for attempt in 0..=config.max_retries {
        if attempt > 0 {
            let jitter = rand::random::<u64>() % delay_ms.max(1);
            std::thread::sleep(Duration::from_millis(delay_ms + jitter));
            delay_ms = (delay_ms * 2).min(config.retry_max_delay_ms);
        }

        let result = match f() {
            Ok(response) => handle_response(response),
            Err(ureq_err) => {
                if is_retryable_transport(&ureq_err) {
                    Err(HttpRetryError::Transport(ureq_err.to_string()))
                } else {
                    Err(HttpRetryError::Permanent(ureq_err.to_string()))
                }
            }
        };

        match result {
            Ok(val) => return Ok(val),
            Err(e) => {
                if e.is_retryable() && attempt < config.max_retries {
                    tracing::warn!(
                        "{backend_label} {op_name}: transient error (attempt {}/{}), retrying: {e}",
                        attempt + 1,
                        config.max_retries,
                    );
                    last_err = Some(e);
                } else {
                    return Err(e);
                }
            }
        }
    }
    Err(last_err.expect("retry loop recorded a retryable error"))
}

/// Classify an HTTP status code for retry purposes.
///
/// Returns `Ok(())` for success (< 400), `RetryableStatus` for 429/5xx,
/// `Permanent` for other 4xx.
pub fn classify_status(code: u16, message: String) -> std::result::Result<(), HttpRetryError> {
    if code < 400 {
        Ok(())
    } else if code == 429 || code >= 500 {
        Err(HttpRetryError::RetryableStatus { code, message })
    } else {
        Err(HttpRetryError::Permanent(message))
    }
}

/// Unified error type for HTTP request + body read operations.
///
/// Keeps the retry module decoupled from `VykarError` — conversion to
/// application error types lives in each backend.
pub enum HttpRetryError {
    /// Transport-level error (always retryable — non-retryable transport errors
    /// are converted to `Permanent` before reaching this variant).
    Transport(String),
    /// HTTP status that should be retried (429, 5xx).
    RetryableStatus {
        #[allow(dead_code)]
        code: u16,
        message: String,
    },
    /// Body read I/O error (may be retryable: connection reset, EOF, etc.).
    BodyIo(std::io::Error),
    /// Application error message (never retried).
    Permanent(String),
}

impl HttpRetryError {
    /// Whether this error is transient and worth retrying.
    pub fn is_retryable(&self) -> bool {
        match self {
            HttpRetryError::Transport(_) | HttpRetryError::RetryableStatus { .. } => true,
            HttpRetryError::BodyIo(e) => is_retryable_io(e),
            HttpRetryError::Permanent(_) => false,
        }
    }
}

impl fmt::Display for HttpRetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpRetryError::Transport(msg) => write!(f, "transport error: {msg}"),
            HttpRetryError::RetryableStatus { message, .. } => write!(f, "{message}"),
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
        assert!(!err.is_retryable());
    }

    #[test]
    fn body_io_retryable_classification() {
        let retryable =
            HttpRetryError::BodyIo(std::io::Error::new(std::io::ErrorKind::ConnectionReset, ""));
        assert!(retryable.is_retryable());

        let non_retryable =
            HttpRetryError::BodyIo(std::io::Error::new(std::io::ErrorKind::InvalidData, ""));
        assert!(!non_retryable.is_retryable());
    }

    #[test]
    fn transport_is_retryable() {
        let err = HttpRetryError::Transport("connection failed".into());
        assert!(err.is_retryable());
    }

    #[test]
    fn retryable_status_is_retryable() {
        let err = HttpRetryError::RetryableStatus {
            code: 429,
            message: "rate limited".into(),
        };
        assert!(err.is_retryable());

        let err = HttpRetryError::RetryableStatus {
            code: 503,
            message: "service unavailable".into(),
        };
        assert!(err.is_retryable());
    }

    #[test]
    fn classify_status_success() {
        assert!(classify_status(200, String::new()).is_ok());
        assert!(classify_status(204, String::new()).is_ok());
        assert!(classify_status(301, String::new()).is_ok());
    }

    #[test]
    fn classify_status_retryable() {
        assert!(classify_status(429, "rate limit".into())
            .unwrap_err()
            .is_retryable());
        assert!(classify_status(500, "internal".into())
            .unwrap_err()
            .is_retryable());
        assert!(classify_status(503, "unavail".into())
            .unwrap_err()
            .is_retryable());
    }

    #[test]
    fn classify_status_permanent() {
        assert!(!classify_status(400, "bad req".into())
            .unwrap_err()
            .is_retryable());
        assert!(!classify_status(403, "forbidden".into())
            .unwrap_err()
            .is_retryable());
        assert!(!classify_status(404, "not found".into())
            .unwrap_err()
            .is_retryable());
    }
}
