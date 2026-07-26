use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Failures that prevent the server from starting.
///
/// Deliberately separate from [`ServerError`], which models per-request HTTP
/// responses (status mapping, body redaction) and has no meaning at startup.
#[derive(Debug)]
pub enum StartupError {
    /// The tokio runtime could not be built.
    Runtime(std::io::Error),
    /// `VYKAR_TOKEN` was unset or empty.
    MissingToken,
    /// The data directory could not be created.
    DataDir {
        path: String,
        source: std::io::Error,
    },
    /// The listen address could not be bound.
    Bind {
        addr: String,
        source: std::io::Error,
    },
    /// The server stopped with an error while accepting connections.
    Serve(std::io::Error),
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(e) => write!(f, "failed to build tokio runtime: {e}"),
            Self::MissingToken => write!(f, "VYKAR_TOKEN environment variable must be set"),
            Self::DataDir { path, source } => {
                write!(f, "cannot create data directory '{path}': {source}")
            }
            Self::Bind { addr, source } => write!(f, "cannot bind to {addr}: {source}"),
            Self::Serve(e) => write!(f, "server failed: {e}"),
        }
    }
}

impl std::error::Error for StartupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(e)
            | Self::DataDir { source: e, .. }
            | Self::Bind { source: e, .. }
            | Self::Serve(e) => Some(e),
            Self::MissingToken => None,
        }
    }
}

/// Server error type that maps to HTTP status codes.
#[derive(Debug)]
pub enum ServerError {
    NotFound(String),
    Forbidden(String),
    PayloadTooLarge(String),
    BadRequest(String),
    Conflict(String),
    RangeNotSatisfiable(String),
    Internal(String),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "not found: {msg}"),
            Self::Forbidden(msg) => write!(f, "forbidden: {msg}"),
            Self::PayloadTooLarge(msg) => write!(f, "payload too large: {msg}"),
            Self::BadRequest(msg) => write!(f, "bad request: {msg}"),
            Self::Conflict(msg) => write!(f, "conflict: {msg}"),
            Self::RangeNotSatisfiable(msg) => write!(f, "range not satisfiable: {msg}"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            Self::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            Self::PayloadTooLarge(msg) => (StatusCode::PAYLOAD_TOO_LARGE, msg.clone()),
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            Self::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            Self::RangeNotSatisfiable(msg) => (StatusCode::RANGE_NOT_SATISFIABLE, msg.clone()),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };
        // Log the full message server-side.
        tracing::error!(status = %status, error = %message);
        // Internal errors can carry server paths / io detail (e.g.
        // `From<io::Error>` and repack's path-bearing `format!` strings).
        // Return a generic body so nothing leaks to clients. Other variants are
        // server-composed and path-free, so their bodies are safe to return.
        let body = if matches!(self, Self::Internal(_)) {
            "internal server error".to_string()
        } else {
            message
        };
        (status, body).into_response()
    }
}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        match e.kind() {
            std::io::ErrorKind::NotFound => Self::NotFound(e.to_string()),
            _ => Self::Internal(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn internal_error_body_is_redacted() {
        let resp = ServerError::Internal("/var/lib/secret".into()).into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"internal server error");
    }

    #[tokio::test]
    async fn non_internal_error_body_preserved() {
        let resp = ServerError::BadRequest("bad key foo".into()).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"bad key foo");
    }

    #[test]
    fn range_not_satisfiable_maps_to_416() {
        let resp = ServerError::RangeNotSatisfiable("out of range".into()).into_response();
        assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }
}
