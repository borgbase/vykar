#![allow(
    clippy::duration_suboptimal_units,
    clippy::manual_let_else,
    clippy::missing_errors_doc,
    clippy::needless_pass_by_value
)]

use std::io::Read;
use std::time::Duration;

use base64::Engine;
use md5::{Digest, Md5};
use percent_encoding::percent_decode_str;
use rusty_s3::actions::{ListObjectsV2, S3Action};
use rusty_s3::{Bucket, Credentials, UrlStyle};

use crate::retry::HttpRetryError;
use crate::RetryConfig;
use vykar_types::error::{Result, VykarError};

use crate::StorageBackend;

/// Duration for presigned URL validity.
const PRESIGN_DURATION: Duration = Duration::from_secs(3600);

pub struct S3Backend {
    bucket: Bucket,
    credentials: Credentials,
    agent: ureq::Agent,
    retry: RetryConfig,
    /// Prefix (root path) prepended to all keys.
    root: String,
    /// When true, `delete()` overwrites with a zero-byte tombstone instead of
    /// issuing a real DELETE. For S3 Object Lock compatibility.
    soft_delete: bool,
}

#[allow(clippy::result_large_err)]
impl S3Backend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket_name: &str,
        region: &str,
        root: &str,
        endpoint: &str,
        access_key_id: &str,
        secret_access_key: &str,
        retry: RetryConfig,
        soft_delete: bool,
    ) -> Result<Self> {
        let base_url = endpoint.parse().map_err(|e| {
            VykarError::Config(format!("invalid S3 endpoint URL '{endpoint}': {e}"))
        })?;

        // Endpoint is always explicit in repository URL; use path-style addressing.
        let url_style = UrlStyle::Path;

        let bucket = Bucket::new(
            base_url,
            url_style,
            bucket_name.to_string(),
            region.to_string(),
        )
        .map_err(|e| VykarError::Config(format!("failed to create S3 bucket handle: {e}")))?;

        let credentials = Credentials::new(access_key_id, secret_access_key);

        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_connect(Some(Duration::from_secs(30)))
            .timeout_send_body(Some(Duration::from_secs(300)))
            .timeout_recv_body(Some(Duration::from_secs(300)))
            .build()
            .into();

        // Normalize root: strip leading/trailing slashes, ensure trailing slash if non-empty.
        let root = root.trim_matches('/').to_string();

        Ok(Self {
            bucket,
            credentials,
            agent,
            retry,
            root,
            soft_delete,
        })
    }

    /// Prepend the root prefix to a key.
    fn full_key(&self, key: &str) -> String {
        if self.root.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.root, key)
        }
    }

    /// Unified retry wrapper for HTTP calls with response handling.
    fn retry_call<T>(
        &self,
        op_name: &str,
        f: impl Fn() -> std::result::Result<http::Response<ureq::Body>, ureq::Error>,
        handle_response: impl Fn(http::Response<ureq::Body>) -> std::result::Result<T, HttpRetryError>,
    ) -> std::result::Result<T, HttpRetryError> {
        crate::retry::retry_http(&self.retry, op_name, "S3", f, handle_response)
    }
}

/// Check an HTTP response status for S3 operations, reading the error body for
/// diagnostics on 4xx/5xx responses.
///
/// Returns `Ok(())` for success status codes (< 400). For error statuses,
/// reads the S3 XML error body before classifying for retry.
fn s3_check_status(
    resp: &mut http::Response<ureq::Body>,
    op: &str,
    key: &str,
) -> std::result::Result<(), HttpRetryError> {
    let status = resp.status().as_u16();
    if status < 400 {
        return Ok(());
    }
    // Read error body for diagnostics — S3 returns XML with error details.
    let body = resp.body_mut().read_to_string().unwrap_or_default();
    let truncated;
    let display_body = if body.len() > 1024 {
        truncated = format!("{}...(truncated)", &body[..body.floor_char_boundary(1024)]);
        &truncated
    } else {
        &body
    };
    tracing::debug!("S3 {op} {key}: HTTP {status}: {display_body}");
    crate::retry::classify_status(status, format!("HTTP {status}: {display_body}"))
}

/// Convert an [`HttpRetryError`] into a `VykarError` for S3 operations.
fn s3_error(op: &str, key: &str, err: HttpRetryError) -> VykarError {
    VykarError::Other(format!("S3 {op} {key}: {err}"))
}

#[allow(clippy::result_large_err)]
impl StorageBackend for S3Backend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let full_key = self.full_key(key);
        let url = self
            .bucket
            .get_object(Some(&self.credentials), &full_key)
            .sign(PRESIGN_DURATION);

        let soft_delete = self.soft_delete;
        self.retry_call(
            &format!("GET {key}"),
            || self.agent.get(url.as_str()).call(),
            |mut resp| {
                let status = resp.status().as_u16();
                if status == 404 {
                    return Ok(None);
                }
                s3_check_status(&mut resp, "GET", key)?;
                let mut buf = Vec::new();
                resp.body_mut()
                    .as_reader()
                    .read_to_end(&mut buf)
                    .map_err(HttpRetryError::BodyIo)?;
                // Treat zero-byte objects as tombstones (soft-deleted).
                if soft_delete && buf.is_empty() {
                    return Ok(None);
                }
                Ok(Some(buf))
            },
        )
        .map_err(|e| s3_error("GET", key, e))
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.put_bytes(key, data)
    }

    fn delete(&self, key: &str) -> Result<()> {
        if self.soft_delete {
            // Overwrite with a zero-byte tombstone instead of deleting.
            // With S3 Object Lock + versioning, the previous version is
            // preserved for the configured retention period.
            return self.put_bytes(key, &[]);
        }
        let full_key = self.full_key(key);
        let url = self
            .bucket
            .delete_object(Some(&self.credentials), &full_key)
            .sign(PRESIGN_DURATION);

        self.retry_call(
            &format!("DELETE {key}"),
            || self.agent.delete(url.as_str()).call(),
            |mut resp| s3_check_status(&mut resp, "DELETE", key),
        )
        .map_err(|e| s3_error("DELETE", key, e))?;
        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let full_key = self.full_key(key);
        let url = self
            .bucket
            .head_object(Some(&self.credentials), &full_key)
            .sign(PRESIGN_DURATION);

        let soft_delete = self.soft_delete;
        self.retry_call(
            &format!("HEAD {key}"),
            || self.agent.head(url.as_str()).call(),
            |mut resp| {
                let status = resp.status().as_u16();
                if status == 404 {
                    return Ok(false);
                }
                s3_check_status(&mut resp, "HEAD", key)?;
                if soft_delete {
                    let len = crate::http_util::extract_content_length(
                        resp.headers(),
                        &format!("S3 HEAD {key}"),
                    )
                    .map_err(|e| HttpRetryError::Permanent(e.to_string()))?;
                    Ok(len > 0)
                } else {
                    Ok(true)
                }
            },
        )
        .map_err(|e| s3_error("HEAD", key, e))
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        let full_key = self.full_key(key);
        let url = self
            .bucket
            .head_object(Some(&self.credentials), &full_key)
            .sign(PRESIGN_DURATION);

        let soft_delete = self.soft_delete;
        self.retry_call(
            &format!("HEAD {key}"),
            || self.agent.head(url.as_str()).call(),
            |mut resp| {
                let status = resp.status().as_u16();
                if status == 404 {
                    return Ok(None);
                }
                s3_check_status(&mut resp, "HEAD", key)?;
                let len = crate::http_util::extract_content_length(
                    resp.headers(),
                    &format!("S3 HEAD {key}"),
                )
                .map_err(|e| HttpRetryError::Permanent(e.to_string()))?;
                // Treat zero-byte objects as tombstones (soft-deleted).
                if soft_delete && len == 0 {
                    return Ok(None);
                }
                Ok(Some(len))
            },
        )
        .map_err(|e| s3_error("HEAD", key, e))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_key(prefix);
        let root_prefix_len = if self.root.is_empty() {
            0
        } else {
            self.root.len() + 1 // +1 for the '/'
        };

        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut action = self.bucket.list_objects_v2(Some(&self.credentials));
            action.query_mut().insert("prefix", &full_prefix);
            if let Some(ref token) = continuation_token {
                action.query_mut().insert("continuation-token", token);
            }
            let url = action.sign(PRESIGN_DURATION);

            let parsed = self
                .retry_call(
                    &format!("LIST {prefix}"),
                    || self.agent.get(url.as_str()).call(),
                    |mut resp| {
                        s3_check_status(&mut resp, "LIST", prefix)?;
                        let mut body = Vec::new();
                        resp.body_mut()
                            .as_reader()
                            .read_to_end(&mut body)
                            .map_err(HttpRetryError::BodyIo)?;
                        ListObjectsV2::parse_response(&body).map_err(|e| {
                            HttpRetryError::Permanent(format!(
                                "S3 LIST {prefix}: failed to parse response: {e}"
                            ))
                        })
                    },
                )
                .map_err(|e| s3_error("LIST", prefix, e))?;

            for obj in &parsed.contents {
                // rusty_s3 sends encoding-type=url; some S3-compatible backends
                // (e.g. Garage) URL-encode keys in the response. Decode here —
                // for backends that don't encode, this is a no-op.
                let key = percent_decode_str(&obj.key)
                    .decode_utf8()
                    .map_err(|e| VykarError::Other(format!("S3 LIST: invalid UTF-8 in key: {e}")))?
                    .into_owned();
                // Skip directory markers
                if key.ends_with('/') {
                    continue;
                }
                // Skip zero-byte tombstones (soft-deleted objects).
                if self.soft_delete && obj.size == 0 {
                    continue;
                }
                // Strip root prefix to return relative keys
                if root_prefix_len > 0 && key.len() > root_prefix_len {
                    keys.push(key[root_prefix_len..].to_string());
                } else {
                    keys.push(key);
                }
            }

            match parsed.next_continuation_token {
                Some(token) => continuation_token = Some(token),
                None => break,
            }
        }

        Ok(keys)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        if length == 0 {
            return Err(VykarError::Other(format!(
                "S3 GET_RANGE {key}: zero-length read requested"
            )));
        }
        // Tombstone check: a zero-byte object cannot satisfy a range read.
        if self.soft_delete && self.size(key)?.is_none() {
            return Ok(None);
        }
        let full_key = self.full_key(key);
        let end = offset
            .checked_add(length)
            .and_then(|n| n.checked_sub(1))
            .ok_or_else(|| {
                VykarError::Other(format!(
                    "S3 GET_RANGE {key}: offset {offset} + length {length} overflows u64"
                ))
            })?;
        let range_header = format!("bytes={offset}-{end}");

        let mut action = self.bucket.get_object(Some(&self.credentials), &full_key);
        // SigV4 canonicalizes signed header names as lowercase.
        // Use lowercase here so the presigned SignedHeaders list is compliant.
        action.headers_mut().insert("range", &range_header);
        let url = action.sign(PRESIGN_DURATION);

        self.retry_call(
            &format!("GET_RANGE {key}"),
            || {
                self.agent
                    .get(url.as_str())
                    .header("range", &range_header)
                    .call()
            },
            |mut resp| {
                let status = resp.status().as_u16();
                if status == 404 {
                    return Ok(None);
                }
                if status >= 400 {
                    s3_check_status(&mut resp, "GET_RANGE", key)?;
                }
                if status == 200 {
                    return Err(HttpRetryError::Permanent(format!(
                        "S3 GET_RANGE {key}: server returned 200 instead of 206 (Range header ignored)"
                    )));
                }
                if status != 206 {
                    return Err(HttpRetryError::Permanent(format!(
                        "S3 GET_RANGE {key}: unexpected status {status}"
                    )));
                }
                let cap = match usize::try_from(length) {
                    Ok(c) => c,
                    Err(_) => {
                        return Err(HttpRetryError::Permanent(format!(
                            "S3 GET_RANGE {key}: length {length} exceeds platform usize"
                        )));
                    }
                };
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
                Ok(Some(buf))
            },
        )
        .map_err(|e| s3_error("GET_RANGE", key, e))
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let dir_key = if key.ends_with('/') {
            self.full_key(key)
        } else {
            self.full_key(&format!("{key}/"))
        };
        let content_type = "application/octet-stream";
        let content_md5 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(b""));

        let mut action = self.bucket.put_object(Some(&self.credentials), &dir_key);
        action.headers_mut().insert("content-type", content_type);
        action.headers_mut().insert("content-md5", &content_md5);
        let url = action.sign(PRESIGN_DURATION);

        self.retry_call(
            &format!("MKDIR {key}"),
            || {
                self.agent
                    .put(url.as_str())
                    .header("content-type", content_type)
                    .header("content-md5", &content_md5)
                    .send(&[] as &[u8])
            },
            |mut resp| s3_check_status(&mut resp, "MKDIR", key),
        )
        .map_err(|e| s3_error("MKDIR", key, e))?;
        Ok(())
    }
}

#[allow(clippy::result_large_err)]
impl S3Backend {
    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        let full_key = self.full_key(key);
        let content_type = "application/octet-stream";
        // Content-MD5 is required for S3 buckets with Object Lock enabled.
        let content_md5 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(data));

        let mut action = self.bucket.put_object(Some(&self.credentials), &full_key);
        // Sign content-type and content-md5 so the presigned URL covers the
        // headers the HTTP client sends with the body.
        action.headers_mut().insert("content-type", content_type);
        action.headers_mut().insert("content-md5", &content_md5);
        let url = action.sign(PRESIGN_DURATION);

        self.retry_call(
            &format!("PUT {key}"),
            || {
                self.agent
                    .put(url.as_str())
                    .header("content-type", content_type)
                    .header("content-md5", &content_md5)
                    .send(data)
            },
            |mut resp| s3_check_status(&mut resp, "PUT", key),
        )
        .map_err(|e| s3_error("PUT", key, e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RetryConfig;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};

    // ── Helpers ──────────────────────────────────────────────────────

    /// Read request headers and drain any body indicated by Content-Length.
    ///
    /// Draining is load-bearing on Windows: closing a socket with unread
    /// receive data triggers WSAECONNRESET (os error 10054) on the peer,
    /// which ureq surfaces as a transport error. Linux tolerates it.
    fn read_request(reader: &mut BufReader<TcpStream>) -> Vec<String> {
        let mut lines = Vec::new();
        let mut content_len = 0usize;
        let mut line = String::new();
        loop {
            line.clear();
            reader.read_line(&mut line).unwrap();
            if line.trim().is_empty() {
                break;
            }
            if let Some(val) = line.to_lowercase().strip_prefix("content-length:") {
                content_len = val.trim().parse().unwrap_or(0);
            }
            lines.push(line.trim().to_string());
        }
        if content_len > 0 {
            let mut body = vec![0u8; content_len];
            reader.read_exact(&mut body).unwrap();
        }
        lines
    }

    /// Single-response TCP mock server.
    fn mock_server(response: &str) -> (u16, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let response = response.to_string();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            read_request(&mut reader);
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
        });
        (port, handle)
    }

    /// Multi-response TCP mock server.
    fn mock_server_multi(responses: Vec<String>) -> (u16, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            for response in &responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                read_request(&mut reader);
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
                drop(stream);
            }
        });
        (port, handle)
    }

    /// Single-response mock that captures the full request (request line + headers).
    fn mock_server_capture(
        response: &str,
    ) -> (u16, Arc<Mutex<Vec<String>>>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let response = response.to_string();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_clone = Arc::clone(&captured);
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            *captured_clone.lock().unwrap() = read_request(&mut reader);
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
        });
        (port, captured, handle)
    }

    /// Multi-response mock that captures all requests.
    #[allow(clippy::type_complexity)]
    fn mock_server_capture_multi(
        responses: Vec<String>,
    ) -> (
        u16,
        Arc<Mutex<Vec<Vec<String>>>>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_clone = Arc::clone(&captured);
        let handle = std::thread::spawn(move || {
            for response in &responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let lines = read_request(&mut reader);
                captured_clone.lock().unwrap().push(lines);
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
                drop(stream);
            }
        });
        (port, captured, handle)
    }

    fn no_retry() -> RetryConfig {
        RetryConfig {
            max_retries: 0,
            ..Default::default()
        }
    }

    fn fast_retry() -> RetryConfig {
        RetryConfig {
            max_retries: 2,
            retry_delay_ms: 1,
            retry_max_delay_ms: 1,
        }
    }

    fn s3_backend(port: u16, retry: RetryConfig, soft_delete: bool) -> S3Backend {
        S3Backend::new(
            "test-bucket",
            "us-east-1",
            "",
            &format!("http://127.0.0.1:{port}"),
            "AKID",
            "SECRET",
            retry,
            soft_delete,
        )
        .unwrap()
    }

    fn s3_backend_rooted(port: u16, retry: RetryConfig, soft_delete: bool) -> S3Backend {
        S3Backend::new(
            "test-bucket",
            "us-east-1",
            "backups/vykar",
            &format!("http://127.0.0.1:{port}"),
            "AKID",
            "SECRET",
            retry,
            soft_delete,
        )
        .unwrap()
    }

    /// Generate ListBucketResult XML for rusty_s3 parser.
    fn list_xml(keys: &[(&str, u64)], next_token: Option<&str>) -> String {
        let mut xml = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix></Prefix>
  <KeyCount>"#,
        );
        xml.push_str(&keys.len().to_string());
        xml.push_str("</KeyCount>\n  <MaxKeys>1000</MaxKeys>\n  <IsTruncated>");
        xml.push_str(if next_token.is_some() {
            "true"
        } else {
            "false"
        });
        xml.push_str("</IsTruncated>\n");
        for (key, size) in keys {
            xml.push_str(&format!(
                "  <Contents>\n    <Key>{key}</Key>\n    \
                 <LastModified>2024-01-01T00:00:00.000Z</LastModified>\n    \
                 <ETag>\"abc\"</ETag>\n    \
                 <Size>{size}</Size>\n    \
                 <StorageClass>STANDARD</StorageClass>\n  </Contents>\n"
            ));
        }
        if let Some(token) = next_token {
            xml.push_str(&format!(
                "  <NextContinuationToken>{token}</NextContinuationToken>\n"
            ));
        }
        xml.push_str("  <EncodingType>url</EncodingType>\n</ListBucketResult>");
        xml
    }

    fn s3_error_xml(code: &str, message: &str) -> String {
        format!("<Error><Code>{code}</Code><Message>{message}</Message></Error>")
    }

    // ── 1. s3_check_status via GET ──────────────────────────────────

    #[test]
    fn get_200_succeeds() {
        let body = "hello";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let result = backend.get("testkey").unwrap();
        assert_eq!(result, Some(b"hello".to_vec()));
        handle.join().unwrap();
    }

    #[test]
    fn get_404_returns_none() {
        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), false);
        let result = backend.get("testkey").unwrap();
        assert_eq!(result, None);
        handle.join().unwrap();
    }

    #[test]
    fn get_429_retries_then_succeeds() {
        let body = "data";
        let responses = vec![
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string(),
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string(),
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ),
        ];
        let (port, handle) = mock_server_multi(responses);
        let backend = s3_backend(port, fast_retry(), false);
        let result = backend.get("testkey").unwrap();
        assert_eq!(result, Some(b"data".to_vec()));
        handle.join().unwrap();
    }

    #[test]
    fn get_500_retries_then_succeeds() {
        let body = "ok";
        let responses = vec![
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string(),
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ),
        ];
        let (port, handle) = mock_server_multi(responses);
        let backend = s3_backend(port, fast_retry(), false);
        let result = backend.get("testkey").unwrap();
        assert_eq!(result, Some(b"ok".to_vec()));
        handle.join().unwrap();
    }

    // ── 2. XML error body diagnostics ───────────────────────────────

    #[test]
    fn get_error_body_preserved() {
        let xml = s3_error_xml("AccessDenied", "Access Denied");
        let resp = format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
            xml.len()
        );
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let err = backend.get("testkey").unwrap_err().to_string();
        assert!(err.contains("AccessDenied"), "got: {err}");
        handle.join().unwrap();
    }

    #[test]
    fn get_error_body_truncated() {
        let big_body = "X".repeat(2000);
        let resp = format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{big_body}",
            big_body.len()
        );
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let err = backend.get("testkey").unwrap_err().to_string();
        assert!(err.contains("(truncated)"), "got: {err}");
        handle.join().unwrap();
    }

    // ── 3. GET_RANGE ────────────────────────────────────────────────

    #[test]
    fn get_range_404_returns_none() {
        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), false);
        let result = backend.get_range("testkey", 0, 10).unwrap();
        assert_eq!(result, None);
        handle.join().unwrap();
    }

    #[test]
    fn get_range_truncated_body_retries() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            // Request 1: truncated (declare 50 bytes, send 5, close)
            {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                }
                let headers = "HTTP/1.1 206 Partial Content\r\n\
                               Content-Length: 50\r\nConnection: close\r\n\r\n";
                stream.write_all(headers.as_bytes()).unwrap();
                stream.write_all(&[0xABu8; 5]).unwrap();
                stream.flush().unwrap();
                drop(stream);
            }
            // Request 2: complete
            {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                }
                let headers = "HTTP/1.1 206 Partial Content\r\n\
                               Content-Length: 50\r\nConnection: close\r\n\r\n";
                stream.write_all(headers.as_bytes()).unwrap();
                stream.write_all(&[0xABu8; 50]).unwrap();
                stream.flush().unwrap();
            }
        });
        let backend = s3_backend(port, fast_retry(), false);
        let result = backend.get_range("testkey", 0, 50).unwrap().unwrap();
        assert_eq!(result.len(), 50);
        handle.join().unwrap();
    }

    #[test]
    fn get_range_200_is_permanent_error() {
        let body = "full object";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let err = backend.get_range("testkey", 0, 10).unwrap_err().to_string();
        assert!(err.contains("200 instead of 206"), "got: {err}");
        handle.join().unwrap();
    }

    #[test]
    fn get_range_soft_delete_tombstone_returns_none() {
        // HEAD returns Content-Length: 0 → size() returns None → get_range short-circuits.
        // Use capture mock to assert exactly one request (HEAD) is made — no range GET.
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, captured, handle) = mock_server_capture(resp);
        let backend = s3_backend(port, no_retry(), true);
        let result = backend.get_range("testkey", 0, 10).unwrap();
        assert_eq!(result, None);
        let lines = captured.lock().unwrap();
        let request_line = &lines[0];
        assert!(
            request_line.starts_with("HEAD "),
            "expected HEAD request, got: {request_line}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn get_range_zero_length_errors() {
        let backend = s3_backend(1, no_retry(), false);
        let err = backend.get_range("testkey", 0, 0).unwrap_err().to_string();
        assert!(err.contains("zero-length read requested"), "got: {err}");
    }

    #[test]
    fn get_range_overflow_errors() {
        let backend = s3_backend(1, no_retry(), false);
        let err = backend
            .get_range("testkey", u64::MAX, 2)
            .unwrap_err()
            .to_string();
        assert!(err.contains("overflows u64"), "got: {err}");
    }

    // ── 4. HEAD: exists() and size() ────────────────────────────────

    #[test]
    fn exists_true_on_200() {
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 42\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), false);
        assert!(backend.exists("testkey").unwrap());
        handle.join().unwrap();
    }

    #[test]
    fn exists_false_on_404() {
        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), false);
        assert!(!backend.exists("testkey").unwrap());
        handle.join().unwrap();
    }

    #[test]
    fn exists_soft_delete_tombstone_false() {
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), true);
        assert!(!backend.exists("testkey").unwrap());
        handle.join().unwrap();
    }

    #[test]
    fn size_missing_content_length() {
        let resp = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), false);
        let err = backend.size("testkey").unwrap_err().to_string();
        assert!(err.contains("missing Content-Length"), "got: {err}");
        handle.join().unwrap();
    }

    #[test]
    fn size_rejects_invalid_content_length() {
        // ureq 3 rejects non-numeric Content-Length at the protocol level,
        // so this is a smoke test that the error surfaces through size().
        // Direct validation coverage lives in http_util::tests.
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: garbage\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), false);
        backend.size("testkey").unwrap_err();
        handle.join().unwrap();
    }

    #[test]
    fn size_200_returns_some() {
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 42\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), false);
        assert_eq!(backend.size("testkey").unwrap(), Some(42));
        handle.join().unwrap();
    }

    #[test]
    fn size_404_returns_none() {
        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), false);
        assert_eq!(backend.size("testkey").unwrap(), None);
        handle.join().unwrap();
    }

    #[test]
    fn size_soft_delete_zero_returns_none() {
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), true);
        let result = backend.size("testkey").unwrap();
        assert_eq!(result, None);
        handle.join().unwrap();
    }

    // ── 5. LIST with canned S3 XML ──────────────────────────────────

    #[test]
    fn list_single_page() {
        let xml = list_xml(&[("snapshots/abc", 100), ("snapshots/def", 200)], None);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
            xml.len()
        );
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/abc", "snapshots/def"]);
        handle.join().unwrap();
    }

    #[test]
    fn list_paginated() {
        let xml1 = list_xml(&[("snapshots/abc", 100)], Some("tok1"));
        let xml2 = list_xml(&[("snapshots/def", 200)], None);
        let responses = vec![
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml1}",
                xml1.len()
            ),
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml2}",
                xml2.len()
            ),
        ];
        let (port, captured, handle) = mock_server_capture_multi(responses);
        let backend = s3_backend(port, no_retry(), false);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/abc", "snapshots/def"]);

        let reqs = captured.lock().unwrap();
        assert_eq!(reqs.len(), 2);
        // Request 1: must include list-type=2, encoding-type=url, and exact prefix value
        let req1_line = &reqs[0][0];
        assert!(req1_line.contains("list-type=2"), "req1: {req1_line}");
        assert!(req1_line.contains("encoding-type=url"), "req1: {req1_line}");
        assert!(
            req1_line.contains("prefix=snapshots%2F") || req1_line.contains("prefix=snapshots/"),
            "expected prefix=snapshots/, req1: {req1_line}"
        );
        // Request 2: must include exact continuation-token value from page 1
        let req2_line = &reqs[1][0];
        assert!(
            req2_line.contains("continuation-token=tok1"),
            "expected continuation-token=tok1, req2: {req2_line}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn list_url_encoded_keys() {
        let xml = list_xml(&[("snapshots%2Fabc", 100)], None);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
            xml.len()
        );
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/abc"]);
        handle.join().unwrap();
    }

    #[test]
    fn list_skips_directory_markers() {
        let xml = list_xml(&[("snapshots/", 0), ("snapshots/abc", 100)], None);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
            xml.len()
        );
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/abc"]);
        handle.join().unwrap();
    }

    #[test]
    fn list_soft_delete_skips_zero_byte() {
        let xml = list_xml(&[("snapshots/tomb", 0), ("snapshots/live", 100)], None);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
            xml.len()
        );
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), true);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/live"]);
        handle.join().unwrap();
    }

    // ── 6. PUT / DELETE status ──────────────────────────────────────

    #[test]
    fn put_429_retries() {
        let responses = vec![
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string(),
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string(),
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
        ];
        let (port, handle) = mock_server_multi(responses);
        let backend = s3_backend(port, fast_retry(), false);
        backend.put("testkey", b"data").unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn put_403_fails_immediately() {
        let xml = s3_error_xml("AccessDenied", "Access Denied");
        let resp_str = format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
            xml.len()
        );
        let (port, handle) = mock_server(&resp_str);
        let backend = s3_backend(port, fast_retry(), false);
        let err = backend.put("testkey", b"data").unwrap_err().to_string();
        assert!(err.contains("AccessDenied"), "got: {err}");
        handle.join().unwrap();
    }

    #[test]
    fn delete_503_retries() {
        let responses = vec![
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string(),
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string(),
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
        ];
        let (port, handle) = mock_server_multi(responses);
        let backend = s3_backend(port, fast_retry(), false);
        backend.delete("testkey").unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn delete_error_body_in_diagnostics() {
        let xml = s3_error_xml("AccessDenied", "Access Denied");
        let resp = format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
            xml.len()
        );
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let err = backend.delete("testkey").unwrap_err().to_string();
        assert!(err.contains("AccessDenied"), "got: {err}");
        handle.join().unwrap();
    }

    // ── 7. Range header signing ─────────────────────────────────────

    #[test]
    fn get_range_signs_range_header() {
        let body = [0xABu8; 10];
        // Build raw response bytes since body is binary
        let header_bytes =
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 10\r\nConnection: close\r\n\r\n";
        let mut raw = Vec::from(&header_bytes[..]);
        raw.extend_from_slice(&body);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let captured_clone = Arc::clone(&captured);
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut lines = Vec::new();
            let mut line = String::new();
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line.trim().is_empty() {
                    break;
                }
                lines.push(line.trim().to_string());
            }
            *captured_clone.lock().unwrap() = lines;
            stream.write_all(&raw).unwrap();
            stream.flush().unwrap();
        });

        let backend = s3_backend(port, no_retry(), false);
        let _result = backend.get_range("testkey", 0, 10).unwrap();

        let lines = captured.lock().unwrap();
        // Check wire headers contain range header
        let has_range = lines.iter().any(|l| {
            let lower = l.to_lowercase();
            lower.starts_with("range:") && lower.contains("bytes=0-9")
        });
        assert!(has_range, "expected range header, got: {lines:?}");
        // Check URL query string contains range in the exact SignedHeaders value.
        // SigV4 lists signed headers semicolon-delimited, URL-encoded: host%3Brange
        let request_line = &lines[0];
        assert!(
            request_line.contains("X-Amz-SignedHeaders=host%3Brange"),
            "expected X-Amz-SignedHeaders=host%3Brange, got: {request_line}"
        );
        handle.join().unwrap();
    }

    // ── 8. Soft-delete behavior ─────────────────────────────────────

    #[test]
    fn get_soft_delete_zero_byte_returns_none() {
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, handle) = mock_server(resp);
        let backend = s3_backend(port, no_retry(), true);
        let result = backend.get("testkey").unwrap();
        assert_eq!(result, None);
        handle.join().unwrap();
    }

    #[test]
    fn delete_soft_delete_sends_put() {
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, captured, handle) = mock_server_capture(resp);
        let backend = s3_backend(port, no_retry(), true);
        backend.delete("testkey").unwrap();
        let lines = captured.lock().unwrap();
        let request_line = &lines[0];
        assert!(
            request_line.starts_with("PUT "),
            "expected PUT, got: {request_line}"
        );
        handle.join().unwrap();
    }

    // ── 9. Root prefix ──────────────────────────────────────────────

    #[test]
    fn get_with_root_sends_prefixed_path() {
        let body = "hello";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let (port, captured, handle) = mock_server_capture(&resp);
        let backend = s3_backend_rooted(port, no_retry(), false);
        let result = backend.get("testkey").unwrap();
        assert_eq!(result, Some(b"hello".to_vec()));
        let lines = captured.lock().unwrap();
        let request_line = &lines[0];
        assert!(
            request_line.contains("/test-bucket/backups/vykar/testkey"),
            "expected prefixed path, got: {request_line}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn list_with_root_strips_prefix() {
        let xml = list_xml(
            &[
                ("backups/vykar/snapshots/abc", 100),
                ("backups/vykar/snapshots/def", 200),
            ],
            None,
        );
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
            xml.len()
        );
        let (port, captured, handle) = mock_server_capture(&resp);
        let backend = s3_backend_rooted(port, no_retry(), false);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/abc", "snapshots/def"]);
        // Check that the request includes the exact prefixed prefix value
        let lines = captured.lock().unwrap();
        let request_line = &lines[0];
        assert!(
            request_line.contains("prefix=backups%2Fvykar%2Fsnapshots%2F")
                || request_line.contains("prefix=backups/vykar/snapshots/"),
            "expected prefix=backups/vykar/snapshots/, got: {request_line}"
        );
        handle.join().unwrap();
    }

    // ── 10. create_dir ──────────────────────────────────────────────

    #[test]
    fn create_dir_sends_put_with_trailing_slash_and_md5() {
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, captured, handle) = mock_server_capture(resp);
        let backend = s3_backend(port, no_retry(), false);
        backend.create_dir("data").unwrap();
        let lines = captured.lock().unwrap();
        // Request method is PUT
        let request_line = &lines[0];
        assert!(
            request_line.starts_with("PUT "),
            "expected PUT, got: {request_line}"
        );
        // Path contains trailing slash
        assert!(
            request_line.contains("/test-bucket/data/"),
            "expected trailing slash, got: {request_line}"
        );
        // Headers contain content-md5
        let has_md5 = lines.iter().any(|l| {
            let lower = l.to_lowercase();
            lower.starts_with("content-md5:") && lower.contains("1b2m2y8asgtpgamy7phcfg==")
        });
        assert!(has_md5, "expected content-md5 header, got: {lines:?}");
        handle.join().unwrap();
    }
}
