use std::io::Read;

use vykar_types::error::{Result, VykarError};

use crate::retry::HttpRetryError;

/// Build the `Range` header value for a `length`-byte read at `offset`.
///
/// `label` names the backend ("REST"/"S3") and only appears in the error.
///
/// # Errors
///
/// Returns an error when `offset + length` overflows `u64`.
pub fn range_header(label: &str, key: &str, offset: u64, length: u64) -> Result<String> {
    let end = offset
        .checked_add(length)
        .and_then(|n| n.checked_sub(1))
        .ok_or_else(|| {
            VykarError::Other(format!(
                "{label} GET_RANGE {key}: offset {offset} + length {length} overflows u64"
            ))
        })?;
    Ok(format!("bytes={offset}-{end}"))
}

/// Read the body of a range response, requiring exactly `length` bytes.
///
/// A short read is an error, not a silent truncation — it surfaces as
/// `BodyIo(UnexpectedEof)`, which the retry layer treats as transient, so a
/// connection dropped mid-body is retried rather than returning partial data.
///
/// # Errors
///
/// Returns `Permanent` when `length` exceeds the platform's `usize`, and
/// `BodyIo` on a read failure or a short body.
pub fn read_range_body(
    resp: &mut http::Response<ureq::Body>,
    label: &str,
    key: &str,
    offset: u64,
    length: u64,
) -> std::result::Result<Vec<u8>, HttpRetryError> {
    let cap = usize::try_from(length).map_err(|_| {
        HttpRetryError::Permanent(format!(
            "{label} GET_RANGE {key}: length {length} exceeds platform usize"
        ))
    })?;
    let mut buf = Vec::with_capacity(cap);
    resp.body_mut()
        .as_reader()
        .take(length)
        .read_to_end(&mut buf)
        .map_err(HttpRetryError::BodyIo)?;
    if buf.len() != cap {
        return Err(HttpRetryError::BodyIo(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "short read on {key} at offset {offset}: expected {length} bytes, got {}",
                buf.len()
            ),
        )));
    }
    Ok(buf)
}

/// Extract and parse the `Content-Length` header from HTTP response headers.
pub fn extract_content_length(headers: &http::HeaderMap, context: &str) -> Result<u64> {
    let header = headers.get(http::header::CONTENT_LENGTH).ok_or_else(|| {
        VykarError::Other(format!("{context}: response missing Content-Length header"))
    })?;
    let val = header
        .to_str()
        .map_err(|_| VykarError::Other(format!("{context}: non-ASCII Content-Length header")))?;
    val.parse::<u64>()
        .map_err(|_| VykarError::Other(format!("{context}: invalid Content-Length header: {val}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(name: &str, value: &str) -> http::HeaderMap {
        let mut map = http::HeaderMap::new();
        map.insert(
            http::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            http::header::HeaderValue::from_str(value).unwrap(),
        );
        map
    }

    #[test]
    fn range_header_formats_inclusive_end() {
        assert_eq!(range_header("REST", "k", 0, 100).unwrap(), "bytes=0-99");
        assert_eq!(range_header("S3", "k", 10, 1).unwrap(), "bytes=10-10");
    }

    #[test]
    fn range_header_rejects_overflow() {
        let err = range_header("REST", "k", u64::MAX, 2)
            .unwrap_err()
            .to_string();
        assert!(err.contains("overflows u64"), "got: {err}");
    }

    #[test]
    fn valid_content_length() {
        let headers = headers_with("content-length", "42");
        assert_eq!(extract_content_length(&headers, "test").unwrap(), 42);
    }

    #[test]
    fn zero_content_length() {
        let headers = headers_with("content-length", "0");
        assert_eq!(extract_content_length(&headers, "test").unwrap(), 0);
    }

    #[test]
    fn missing_content_length() {
        let headers = http::HeaderMap::new();
        let err = extract_content_length(&headers, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing Content-Length"), "got: {err}");
    }

    #[test]
    fn non_numeric_content_length() {
        let headers = headers_with("content-length", "garbage");
        let err = extract_content_length(&headers, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid Content-Length"), "got: {err}");
    }

    #[test]
    fn negative_content_length() {
        let headers = headers_with("content-length", "-1");
        let err = extract_content_length(&headers, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid Content-Length"), "got: {err}");
    }
}
