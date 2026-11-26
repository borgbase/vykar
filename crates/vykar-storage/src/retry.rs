use std::fmt;
use std::time::Duration;

use vykar_types::cancel::CancelToken;

use crate::RetryConfig;

/// Exponential backoff with jitter between the attempts of one operation.
/// The wait ends early once `cancel` is cancelled, and the caller then
/// starts no further attempt. Shared by `retry_http` and SFTP `retry_op`.
pub(crate) struct Backoff<'a> {
    /// Base of the next wait in ms. The wait is base + jitter, jitter in [0, base).
    next_ms: u64,
    max_ms: u64,
    cancel: &'a CancelToken,
}

impl<'a> Backoff<'a> {
    pub(crate) fn new(config: &RetryConfig, cancel: &'a CancelToken) -> Self {
        Self {
            next_ms: config.retry_delay_ms,
            max_ms: config.retry_max_delay_ms,
            cancel,
        }
    }

    /// The next wait, advancing the schedule: base = max(next, 1); the wait
    /// is base + rand % base ms; then next = min(2 * base, max(max_ms, 1)).
    fn next_delay(&mut self) -> Duration {
        let base = self.next_ms.max(1);
        let jitter = rand::random::<u64>() % base;
        self.next_ms = base.saturating_mul(2).min(self.max_ms.max(1));
        Duration::from_millis(base.saturating_add(jitter))
    }

    /// Wait before the next attempt. Err when the token is or becomes cancelled.
    pub(crate) fn wait(&mut self) -> std::result::Result<(), BackoffCancelled> {
        let d = self.next_delay();
        if self.cancel.wait_timeout(d) {
            Err(BackoffCancelled)
        } else {
            Ok(())
        }
    }
}

/// Returned by [`Backoff::wait`] when the operation was cancelled.
#[derive(Debug)]
pub(crate) struct BackoffCancelled;

/// Whether a ureq transport-level error is transient and worth retrying.
fn is_retryable_transport(err: &ureq::Error) -> bool {
    match err {
        ureq::Error::Timeout(_) | ureq::Error::ConnectionFailed | ureq::Error::HostNotFound => true,
        ureq::Error::Io(e) => is_retryable_io(e),
        _ => false,
    }
}

/// The underlying [`std::io::ErrorKind`] of a ureq transport error, if it was an
/// I/O error. Used by backends to give phase-specific diagnostics (e.g. a
/// connection closed mid-PUT). ureq does not expose which request phase produced
/// the I/O error — only its kind.
fn transport_io_kind(err: &ureq::Error) -> Option<std::io::ErrorKind> {
    match err {
        ureq::Error::Io(e) => Some(e.kind()),
        _ => None,
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
///
/// The first attempt always runs, even when `cancel` is already cancelled, so
/// cleanup after a cancel still reaches the server. A cancel during a retry
/// wait ends the wait at once and returns [`HttpRetryError::Interrupted`]
/// without a further attempt.
pub fn retry_http<T>(
    config: &RetryConfig,
    cancel: &CancelToken,
    op_name: &str,
    backend_label: &str,
    f: impl Fn() -> std::result::Result<http::Response<ureq::Body>, ureq::Error>,
    handle_response: impl Fn(http::Response<ureq::Body>) -> std::result::Result<T, HttpRetryError>,
) -> std::result::Result<T, HttpRetryError> {
    let mut backoff = Backoff::new(config, cancel);
    let mut last_err = None;

    for attempt in 0..=config.max_retries {
        if attempt > 0 && backoff.wait().is_err() {
            tracing::debug!("{backend_label} {op_name}: cancelled while waiting to retry");
            return Err(HttpRetryError::Interrupted);
        }

        let result = match f() {
            Ok(response) => handle_response(response),
            Err(ureq_err) => {
                if is_retryable_transport(&ureq_err) {
                    let kind = transport_io_kind(&ureq_err);
                    Err(HttpRetryError::Transport(ureq_err.to_string(), kind))
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
    /// are converted to `Permanent` before reaching this variant). The second
    /// field carries the underlying [`std::io::ErrorKind`] when the transport
    /// failure was an I/O error, so backends can add phase-specific hints.
    Transport(String, Option<std::io::ErrorKind>),
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
    /// The operation was cancelled during a retry wait; never retried.
    Interrupted,
}

impl HttpRetryError {
    /// The underlying [`std::io::ErrorKind`] for a transport-level I/O error.
    ///
    /// Returns `None` for non-transport errors and for transport errors that
    /// were not I/O errors (timeouts, connection-failed, host-not-found).
    /// Backends use this to add phase-specific diagnostics.
    pub fn transport_io_kind(&self) -> Option<std::io::ErrorKind> {
        match self {
            HttpRetryError::Transport(_, kind) => *kind,
            _ => None,
        }
    }

    /// Whether this error is transient and worth retrying.
    pub fn is_retryable(&self) -> bool {
        match self {
            HttpRetryError::Transport(..) | HttpRetryError::RetryableStatus { .. } => true,
            HttpRetryError::BodyIo(e) => is_retryable_io(e),
            HttpRetryError::Permanent(_) | HttpRetryError::Interrupted => false,
        }
    }
}

impl fmt::Display for HttpRetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpRetryError::Transport(msg, _) => write!(f, "transport error: {msg}"),
            HttpRetryError::RetryableStatus { message, .. } => write!(f, "{message}"),
            HttpRetryError::BodyIo(e) => write!(f, "body read error: {e}"),
            HttpRetryError::Permanent(msg) => write!(f, "{msg}"),
            HttpRetryError::Interrupted => write!(f, "interrupted"),
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
            // ureq 3.4.1 reports a peer reset that arrives mid-request-body on
            // a pooled connection as EINVAL rather than a connection-reset
            // kind. That is the #151 scenario — a transient S3 blip — and
            // treating it as permanent aborts the whole upload instead of
            // retrying the part. Broader than the other kinds here, but a
            // backup that fails on a recoverable network error is the worse
            // trade. `multipart_mid_part_reset_then_succeeds` covers it.
            | std::io::ErrorKind::InvalidInput
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
            // See the note in `is_retryable_io`: ureq surfaces a mid-body
            // peer reset on a pooled connection as EINVAL.
            std::io::ErrorKind::InvalidInput,
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
        let err = HttpRetryError::Transport("connection failed".into(), None);
        assert!(err.is_retryable());
    }

    #[test]
    fn retry_http_preserves_transport_io_kind() {
        // A ureq I/O transport error must reach the caller as a `Transport`
        // variant carrying the original `io::ErrorKind`, so backends can give
        // phase-specific diagnostics. Covers the f()-error mapping branch end
        // to end (not just a hand-built `Transport`).
        let config = RetryConfig {
            max_retries: 0,
            retry_delay_ms: 1,
            retry_max_delay_ms: 1,
        };
        let err = retry_http::<()>(
            &config,
            &CancelToken::new(),
            "PUT testkey",
            "S3",
            || {
                Err(ureq::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "broken pipe",
                )))
            },
            |_resp| Ok(()),
        )
        .unwrap_err();
        assert_eq!(
            err.transport_io_kind(),
            Some(std::io::ErrorKind::BrokenPipe)
        );
    }

    const LONG_RETRY: RetryConfig = RetryConfig {
        max_retries: 5,
        retry_delay_ms: 60_000,
        retry_max_delay_ms: 60_000,
    };

    /// Cancel `cancel` from another thread after `after`; the receiver gets
    /// the instant just before `cancel()`.
    fn cancel_later(
        cancel: &CancelToken,
        after: Duration,
    ) -> std::sync::mpsc::Receiver<std::time::Instant> {
        let (tx, rx) = std::sync::mpsc::channel();
        let cancel = cancel.clone();
        std::thread::spawn(move || {
            std::thread::sleep(after);
            let _ = tx.send(std::time::Instant::now());
            cancel.cancel();
        });
        rx
    }

    #[test]
    fn backoff_schedule_doubles_to_the_cap_with_jitter() {
        let config = RetryConfig {
            max_retries: 8,
            retry_delay_ms: 1500,
            retry_max_delay_ms: 60_000,
        };
        let token = CancelToken::new();
        let mut backoff = Backoff::new(&config, &token);
        for base in [1500, 3000, 6000, 12000, 24000, 48000, 60000, 60000] {
            let d = backoff.next_delay();
            assert!(
                d >= Duration::from_millis(base) && d < Duration::from_millis(2 * base),
                "{d:?} not in [{base}, {}) ms",
                2 * base
            );
        }
    }

    #[test]
    fn backoff_zero_config_waits_one_ms() {
        let config = RetryConfig {
            max_retries: 3,
            retry_delay_ms: 0,
            retry_max_delay_ms: 0,
        };
        let token = CancelToken::new();
        let mut backoff = Backoff::new(&config, &token);
        for _ in 0..4 {
            assert_eq!(backoff.next_delay(), Duration::from_millis(1));
        }
    }

    #[test]
    fn backoff_wait_ends_within_100ms_of_cancel() {
        let token = CancelToken::new();
        let mut backoff = Backoff::new(&LONG_RETRY, &token);
        let rx = cancel_later(&token, Duration::from_millis(50));
        assert!(backoff.wait().is_err());
        let latency = rx.recv().unwrap().elapsed();
        assert!(latency < Duration::from_millis(100), "{latency:?}");
    }

    #[test]
    fn retry_http_cancel_during_backoff_is_interrupted() {
        let token = CancelToken::new();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let rx = cancel_later(&token, Duration::from_millis(50));
        let err = retry_http::<()>(
            &LONG_RETRY,
            &token,
            "GET k",
            "TEST",
            || {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(ureq::Error::ConnectionFailed)
            },
            |_resp| Ok(()),
        )
        .unwrap_err();
        let latency = rx.recv().unwrap().elapsed();
        assert!(matches!(err, HttpRetryError::Interrupted), "{err}");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(latency < Duration::from_millis(100), "{latency:?}");
    }

    #[test]
    fn retry_http_first_attempt_runs_after_cancel() {
        let token = CancelToken::new();
        token.cancel();

        // A retryable failure: the first attempt runs, then no retry.
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let start = std::time::Instant::now();
        let err = retry_http::<()>(
            &LONG_RETRY,
            &token,
            "GET k",
            "TEST",
            || {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(ureq::Error::ConnectionFailed)
            },
            |_resp| Ok(()),
        )
        .unwrap_err();
        assert!(matches!(err, HttpRetryError::Interrupted), "{err}");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(start.elapsed() < Duration::from_millis(100));

        // A permanent failure keeps its own error.
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let err = retry_http::<()>(
            &LONG_RETRY,
            &token,
            "GET k",
            "TEST",
            || {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(ureq::Error::BadUri("bad".into()))
            },
            |_resp| Ok(()),
        )
        .unwrap_err();
        assert!(matches!(err, HttpRetryError::Permanent(_)), "{err}");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn interrupted_is_not_retryable() {
        assert!(!HttpRetryError::Interrupted.is_retryable());
        assert_eq!(HttpRetryError::Interrupted.to_string(), "interrupted");
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
        assert!(
            classify_status(429, "rate limit".into())
                .unwrap_err()
                .is_retryable()
        );
        assert!(
            classify_status(500, "internal".into())
                .unwrap_err()
                .is_retryable()
        );
        assert!(
            classify_status(503, "unavail".into())
                .unwrap_err()
                .is_retryable()
        );
    }

    #[test]
    fn classify_status_permanent() {
        assert!(
            !classify_status(400, "bad req".into())
                .unwrap_err()
                .is_retryable()
        );
        assert!(
            !classify_status(403, "forbidden".into())
                .unwrap_err()
                .is_retryable()
        );
        assert!(
            !classify_status(404, "not found".into())
                .unwrap_err()
                .is_retryable()
        );
    }
}
