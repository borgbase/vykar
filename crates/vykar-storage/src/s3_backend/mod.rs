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

use crate::RetryConfig;
use crate::retry::HttpRetryError;
use vykar_types::cancel::CancelToken;
use vykar_types::error::{Result, VykarError};

use crate::conditional::Verifier;
use crate::{ConditionalWrite, ListedObject, S3ConditionalWrites, StorageBackend};

mod batch_delete;
pub(crate) mod conditional;
mod multipart;
pub(crate) mod probe;

/// Duration for presigned URL validity.
const PRESIGN_DURATION: Duration = Duration::from_secs(3600);

pub struct S3Backend {
    bucket: Bucket,
    credentials: Credentials,
    agent: ureq::Agent,
    retry: RetryConfig,
    /// See [`crate::StorageConfig::cancel`].
    cancel: CancelToken,
    /// Prefix (root path) prepended to all keys.
    root: String,
    /// When true, `delete()` overwrites with a zero-byte tombstone instead of
    /// issuing a real DELETE. For S3 Object Lock compatibility.
    soft_delete: bool,
    /// Backend-wide bound on concurrent multipart part PUTs.
    part_slots: multipart::PartSlots,
    /// Set by the first completed multipart upload. Until then `put_pack`
    /// keeps a pack's buffer through its completion, so a store that accepts
    /// parts but refuses completions still gets the second attempt.
    multipart_proven: std::sync::atomic::AtomicBool,
    /// Set once the endpoint answers DeleteObjects with NotImplemented; from
    /// then on batch_delete_keys reports UnsupportedBackend without a request.
    batch_delete_unsupported: std::sync::atomic::AtomicBool,
    request_width: usize,
    transfer_width: usize,
    /// `s3_conditional_writes`; see [`Self::with_conditional_writes`].
    conditional_mode: S3ConditionalWrites,
    /// The endpoint's host is an AWS one (what `auto` trusts).
    aws_endpoint: bool,
    /// Cache of the put-if-absent check.
    cond: Verifier,
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
        max_connections: Option<usize>,
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

        let part_concurrency =
            crate::http_util::part_concurrency(multipart::PART_CONCURRENCY_ENV, max_connections);
        // Keep every part stream pooled plus a few for control requests
        // (create/complete, locks, index), so a part never pays a fresh
        // connection (cold congestion window, TLS handshake).
        let pool = crate::http_idle_pool_size(max_connections).max(part_concurrency + 4);
        let agent = crate::http_util::build_agent(&crate::http_util::HttpTimeouts::default(), pool);

        // Normalize root: strip leading/trailing slashes, ensure trailing slash if non-empty.
        let root = root.trim_matches('/').to_string();

        Ok(Self {
            bucket,
            credentials,
            agent,
            retry,
            cancel: CancelToken::new(),
            root,
            soft_delete,
            part_slots: multipart::PartSlots::new(part_concurrency),
            multipart_proven: std::sync::atomic::AtomicBool::new(false),
            batch_delete_unsupported: std::sync::atomic::AtomicBool::new(false),
            request_width: crate::remote_request_width(max_connections),
            transfer_width: crate::transfer_width(max_connections),
            conditional_mode: S3ConditionalWrites::Auto,
            aws_endpoint: conditional::is_aws_endpoint(endpoint),
            cond: Verifier::new(),
        })
    }

    /// Whether `conditional()` offers put-if-absent: `Auto` (the default)
    /// only on AWS endpoints, `On` on any, `Off` never. The check on first
    /// use still runs in every mode that offers it.
    #[must_use]
    pub fn with_conditional_writes(mut self, mode: S3ConditionalWrites) -> Self {
        self.conditional_mode = mode;
        self
    }

    /// Observe `cancel` in retry waits; see [`crate::StorageConfig::cancel`].
    #[must_use]
    pub fn with_cancel(mut self, cancel: CancelToken) -> Self {
        self.cancel = cancel;
        self
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
        crate::retry::retry_http(&self.retry, &self.cancel, op_name, "S3", f, handle_response)
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
///
/// When a PUT exhausts its retries with a connection-close transport error
/// (broken pipe / connection reset / aborted), append a cautiously-worded hint
/// listing candidate causes. ureq does not expose which request phase produced
/// the I/O error, so the wording does not claim it was the body write — only
/// that the connection closed during the PUT. The cause is genuinely
/// undetermined (write-scoped credentials, storage cap/account restriction,
/// provider per-connection limit, or network middleware), so the hint must not
/// assert that credentials or quota are fine — successful reads do not rule out
/// a write-scoped permission or quota cause.
fn s3_error(op: &str, key: &str, err: HttpRetryError) -> VykarError {
    if matches!(err, HttpRetryError::Interrupted) {
        return VykarError::Interrupted;
    }
    let mut msg = format!("S3 {op} {key}: {err}");
    if op == "PUT"
        && matches!(
            err.transport_io_kind(),
            Some(
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
            )
        )
    {
        msg.push_str(
            " — The connection was closed during the PUT request. Possible causes: \
             bucket permissions or a read/write-scoped key; a storage cap or account \
             restriction; a provider per-connection/upload limit; or network middleware \
             (proxy/firewall/ISP/MTU) resetting the connection.",
        );
        // Pack uploads are the large, long-lived transfers most exposed to a
        // mid-transfer reset; other PUTs (locks, index, snapshots, sessions)
        // are small, so omit the upload-specific clause for them.
        if key.starts_with("packs/") {
            msg.push_str(" Large pack uploads are most exposed to mid-transfer resets.");
        }
    }
    VykarError::Other(msg)
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

    /// Like `put`, except that once a completion has succeeded on this
    /// backend, a multipart pack releases its buffer before the completion
    /// request (see `put_multipart_owned`). A key outside `packs/` takes the
    /// plain `put` path: only a content-addressed key reconciles by size.
    fn put_pack(&self, key: &str, data: crate::PackData) -> Result<()> {
        if key.starts_with("packs/")
            && data.len() > multipart::MULTIPART_THRESHOLD
            && self
                .multipart_proven
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return self.put_multipart_owned(&self.full_key(key), key, data);
        }
        self.put_bytes(key, data.as_slice())
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
        Ok(self
            .list_sized(prefix)?
            .into_iter()
            .map(|o| o.key)
            .collect())
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        self.list_objects(prefix, false)
    }

    fn list_meta(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        self.list_objects(prefix, true)
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
        let range_header = crate::http_util::range_header("S3", key, offset, length)?;

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
                crate::http_util::read_range_body(&mut resp, "S3", key, offset, length).map(Some)
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

    fn profile(&self) -> crate::BackendProfile {
        crate::BackendProfile {
            local: false,
            staged_put: false,
            request_width: self.request_width,
            transfer_width: self.transfer_width,
        }
    }

    fn batch_delete_keys(&self, keys: &[String], _scope: crate::BatchDeleteScope) -> Result<()> {
        self.delete_objects(keys)
    }

    fn conditional(&self) -> Option<&dyn ConditionalWrite> {
        (self.conditional_offered() && !self.cond.refused())
            .then_some(self as &dyn ConditionalWrite)
    }

    crate::unsupported_server_ops!(except[batch_delete_keys, conditional]);
}

#[allow(clippy::result_large_err)]
impl S3Backend {
    /// `list_sized` (`with_mtime` false) and `list_meta` (true).
    fn list_objects(&self, prefix: &str, with_mtime: bool) -> Result<Vec<ListedObject>> {
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
                        // rusty-s3 (instant-xml) parses from &str.
                        let body = std::str::from_utf8(&body).map_err(|e| {
                            HttpRetryError::Permanent(format!(
                                "S3 LIST {prefix}: response was not valid UTF-8: {e}"
                            ))
                        })?;
                        ListObjectsV2::parse_response(body).map_err(|e| {
                            HttpRetryError::Permanent(format!(
                                "S3 LIST {prefix}: failed to parse response: {e}"
                            ))
                        })
                    },
                )
                .map_err(|e| s3_error("LIST", prefix, e))?;

            for obj in &parsed.contents {
                // rusty_s3 sends encoding-type=url and decodes the keys itself when
                // the response echoes EncodingType. This covers backends (e.g.
                // Garage) that encode keys without echoing it; vykar's own keys
                // contain no `%`, so it is a no-op for already-decoded ones.
                let key = percent_decode_str(&obj.key)
                    .decode_utf8()
                    .map_err(|e| VykarError::Other(format!("S3 LIST: invalid UTF-8 in key: {e}")))?
                    .into_owned();
                // Skip directory markers
                if key.ends_with('/') {
                    continue;
                }
                // Skip zero-byte tombstones (soft-deleted objects).
                // rusty-s3 reports Size as optional; only a known zero is a tombstone,
                // so a server omitting Size can never hide a live key.
                if self.soft_delete && obj.size == Some(0) {
                    continue;
                }
                // Strip root prefix to return relative keys
                let key = if root_prefix_len > 0 && key.len() > root_prefix_len {
                    key[root_prefix_len..].to_string()
                } else {
                    key
                };
                keys.push(ListedObject {
                    key,
                    size: obj.size,
                    mtime: if with_mtime {
                        obj.last_modified
                            .as_deref()
                            .and_then(conditional::parse_s3_time)
                    } else {
                        None
                    },
                });
            }

            match parsed.next_continuation_token {
                Some(token) => continuation_token = Some(token),
                None => break,
            }
        }

        Ok(keys)
    }

    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        let full_key = self.full_key(key);
        // Large objects (packs, or any object over the threshold — e.g. a big
        // index) are uploaded via the S3 multipart API so a mid-transfer reset
        // costs one part instead of the whole object.
        if data.len() > multipart::MULTIPART_THRESHOLD {
            return self.put_multipart(&full_key, key, data);
        }
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

/// Shared backend/config constructors used by both the core and multipart test
/// modules (the multipart tests live in `multipart.rs`).
#[cfg(test)]
mod test_support {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    pub(super) fn no_retry() -> RetryConfig {
        RetryConfig {
            max_retries: 0,
            ..Default::default()
        }
    }

    pub(super) fn fast_retry() -> RetryConfig {
        RetryConfig {
            max_retries: 2,
            retry_delay_ms: 1,
            retry_max_delay_ms: 1,
        }
    }

    /// Backend for the scripted (strictly sequential) mock servers: one part in
    /// flight so requests arrive in a deterministic order.
    pub(super) fn s3_backend(port: u16, retry: RetryConfig, soft_delete: bool) -> S3Backend {
        sequential_parts(s3_backend_concurrent(port, retry, soft_delete))
    }

    /// Backend with the default (parallel) part concurrency.
    pub(super) fn s3_backend_concurrent(
        port: u16,
        retry: RetryConfig,
        soft_delete: bool,
    ) -> S3Backend {
        S3Backend::new(
            "test-bucket",
            "us-east-1",
            "",
            &format!("http://127.0.0.1:{port}"),
            "AKID",
            "SECRET",
            retry,
            soft_delete,
            None,
        )
        .unwrap()
    }

    pub(super) fn s3_backend_rooted(port: u16, retry: RetryConfig, soft_delete: bool) -> S3Backend {
        sequential_parts(
            S3Backend::new(
                "test-bucket",
                "us-east-1",
                "backups/vykar",
                &format!("http://127.0.0.1:{port}"),
                "AKID",
                "SECRET",
                retry,
                soft_delete,
                None,
            )
            .unwrap(),
        )
    }

    fn sequential_parts(mut backend: S3Backend) -> S3Backend {
        backend.part_slots = super::multipart::PartSlots::new(1);
        backend
    }

    pub(super) fn s3_error_xml(code: &str, message: &str) -> String {
        format!("<Error><Code>{code}</Code><Message>{message}</Message></Error>")
    }

    pub(super) fn reply(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// One parsed request: method, path without the query, whole request line,
    /// whether `if-none-match: *` was sent, and the body.
    pub(super) struct Req {
        pub(super) method: String,
        pub(super) path: String,
        pub(super) line: String,
        pub(super) if_none_match: bool,
        pub(super) body: Vec<u8>,
    }

    /// Serves requests until the test ends, answering each with `route`.
    pub(super) fn serve(route: impl Fn(&Req) -> (&'static str, String) + Send + 'static) -> u16 {
        serve_bytes(move |req| {
            let (status, body) = route(req);
            (status, body.into_bytes())
        })
    }

    /// [`serve`] with binary reply bodies. A HEAD reply carries the body's
    /// length as Content-Length and no body.
    pub(super) fn serve_bytes(
        route: impl Fn(&Req) -> (&'static str, Vec<u8>) + Send + 'static,
    ) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                let mut content_len = 0usize;
                let mut if_none_match = false;
                let mut line = String::new();
                loop {
                    line.clear();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line.trim().is_empty() {
                        break;
                    }
                    if request_line.is_empty() {
                        request_line = line.clone();
                        continue;
                    }
                    let lower = line.to_lowercase();
                    if let Some(v) = lower.strip_prefix("content-length:") {
                        content_len = v.trim().parse().unwrap_or(0);
                    }
                    if lower.replace(' ', "").trim_end() == "if-none-match:*" {
                        if_none_match = true;
                    }
                }
                let mut body = vec![0u8; content_len];
                let _ = std::io::Read::read_exact(&mut reader, &mut body);
                let mut parts = request_line.split_whitespace();
                let method = parts.next().unwrap_or_default().to_string();
                let target = parts.next().unwrap_or_default();
                let path = target.split('?').next().unwrap_or_default().to_string();
                let req = Req {
                    method,
                    path,
                    line: request_line.clone(),
                    if_none_match,
                    body,
                };
                let (status, reply_body) = route(&req);
                let head = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply_body.len()
                );
                let _ = stream.write_all(head.as_bytes());
                if req.method != "HEAD" {
                    let _ = stream.write_all(&reply_body);
                }
                let _ = stream.flush();
            }
        });
        port
    }

    /// An in-memory S3 for the conformance suite: PutObject (honouring
    /// `if-none-match: *` with 412), GetObject, HeadObject, DeleteObject and
    /// ListObjectsV2 with `<Size>` and `<LastModified>`. Connections are served
    /// one at a time, so concurrent creators are serialised (still one winner).
    pub(super) fn stateful_s3() -> u16 {
        use std::collections::BTreeMap;
        use std::sync::Mutex;
        let objects: Mutex<BTreeMap<String, Vec<u8>>> = Mutex::new(BTreeMap::new());
        serve_bytes(move |req| {
            let path = percent_encoding::percent_decode_str(&req.path)
                .decode_utf8_lossy()
                .into_owned();
            let key = path
                .strip_prefix("/test-bucket/")
                .unwrap_or_default()
                .to_string();
            let mut objects = objects.lock().unwrap();
            match req.method.as_str() {
                "PUT" => {
                    if req.if_none_match && objects.contains_key(&key) {
                        let xml = s3_error_xml("PreconditionFailed", "exists");
                        return ("412 Precondition Failed", xml.into_bytes());
                    }
                    objects.insert(key, req.body.clone());
                    ("200 OK", Vec::new())
                }
                "GET" if req.line.contains("list-type=2") => {
                    let target = req.line.split_whitespace().nth(1).unwrap_or_default();
                    let url = url::Url::parse(&format!("http://h{target}")).unwrap();
                    let prefix = url
                        .query_pairs()
                        .find(|(k, _)| k == "prefix")
                        .map(|(_, v)| v.into_owned())
                        .unwrap_or_default();
                    let mut xml = String::from(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                         <Name>test-bucket</Name><MaxKeys>1000</MaxKeys>\
                         <IsTruncated>false</IsTruncated>",
                    );
                    for (k, v) in objects.range(prefix.clone()..) {
                        if !k.starts_with(&prefix) {
                            break;
                        }
                        xml.push_str(&format!(
                            "<Contents><Key>{k}</Key><Size>{}</Size>\
                             <LastModified>2024-01-01T00:00:00.000Z</LastModified></Contents>",
                            v.len()
                        ));
                    }
                    xml.push_str("</ListBucketResult>");
                    ("200 OK", xml.into_bytes())
                }
                "GET" | "HEAD" => match objects.get(&key) {
                    Some(v) => ("200 OK", v.clone()),
                    None => ("404 Not Found", Vec::new()),
                },
                "DELETE" => {
                    objects.remove(&key);
                    ("204 No Content", Vec::new())
                }
                _ => ("400 Bad Request", Vec::new()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use crate::{ConditionalSupport, Created};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};

    // ── Helpers ──────────────────────────────────────────────────────

    /// Read request headers and drain any body indicated by Content-Length,
    /// returning both the header lines and the captured body bytes.
    ///
    /// Draining is load-bearing on Windows: closing a socket with unread
    /// receive data triggers WSAECONNRESET (os error 10054) on the peer,
    /// which ureq surfaces as a transport error. Linux tolerates it.
    pub(super) fn read_request(reader: &mut BufReader<TcpStream>) -> (Vec<String>, Vec<u8>) {
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
        let mut body = vec![0u8; content_len];
        if content_len > 0 {
            reader.read_exact(&mut body).unwrap();
        }
        (lines, body)
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
                let _ = read_request(&mut reader);
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
                drop(stream);
            }
        });
        (port, handle)
    }

    /// Single-response mock that captures the full request (request line + headers).
    pub(super) fn mock_server_capture(
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
            *captured_clone.lock().unwrap() = read_request(&mut reader).0;
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
                let (lines, _) = read_request(&mut reader);
                captured_clone.lock().unwrap().push(lines);
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
                drop(stream);
            }
        });
        (port, captured, handle)
    }

    /// Multi-response mock that captures each request's header lines and body.
    #[allow(clippy::type_complexity)]
    pub(super) fn mock_server_capture_bodies(
        responses: Vec<String>,
    ) -> (
        u16,
        Arc<Mutex<Vec<(Vec<String>, Vec<u8>)>>>,
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
                let request = read_request(&mut reader);
                captured_clone.lock().unwrap().push(request);
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
                drop(stream);
            }
        });
        (port, captured, handle)
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

    fn http_ok(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
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
    fn list_sized_single_page_reports_sizes() {
        let xml = list_xml(&[("snapshots/abc", 100), ("snapshots/def", 200)], None);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
            xml.len()
        );
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let listed = backend.list_sized("snapshots/").unwrap();
        assert_eq!(
            listed,
            vec![
                ListedObject {
                    key: "snapshots/abc".into(),
                    size: Some(100),
                    mtime: None,
                },
                ListedObject {
                    key: "snapshots/def".into(),
                    size: Some(200),
                    mtime: None,
                },
            ]
        );
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

    /// A soft-delete tombstone is a *known* zero size. rusty-s3 0.11 made Size
    /// optional, and a server omitting it must not hide a live key.
    #[test]
    fn list_soft_delete_keeps_key_with_missing_size() {
        let xml = list_xml(&[("snapshots/tomb", 0), ("snapshots/nosize", 100)], None)
            .replace("<Size>100</Size>", "");
        assert!(
            !xml.contains("<Size>100"),
            "fixture should drop Size: {xml}"
        );
        let resp = http_ok(&xml);
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), true);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/nosize"]);
        handle.join().unwrap();
    }

    /// AWS/MinIO form-encode: a literal `+` arrives as `%2B`, a space as `+`.
    /// Our own decode runs after rusty-s3's, so check the two don't compound.
    #[test]
    fn list_decodes_form_encoded_keys_once() {
        let xml = list_xml(&[("snapshots/a%2Bb", 100), ("snapshots/c+d", 100)], None);
        let resp = http_ok(&xml);
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/a+b", "snapshots/c d"]);
        handle.join().unwrap();
    }

    /// A root containing `+` or a space keeps its length through decoding, so
    /// stripping it by byte count still yields the relative key.
    #[test]
    fn list_with_root_containing_plus_strips_prefix() {
        let xml = list_xml(&[("back%2Bups/snapshots/abc", 100)], None);
        let resp = http_ok(&xml);
        let (port, handle) = mock_server(&resp);
        let backend = S3Backend::new(
            "test-bucket",
            "us-east-1",
            "back+ups",
            &format!("http://127.0.0.1:{port}"),
            "AKID",
            "SECRET",
            no_retry(),
            false,
            None,
        )
        .unwrap();
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/abc"]);
        handle.join().unwrap();
    }

    /// A truncated page may be empty; only IsTruncated=false ends the listing.
    #[test]
    fn list_follows_token_through_empty_page() {
        let xml1 = list_xml(&[("snapshots/abc", 100)], Some("tok1"));
        let xml2 = list_xml(&[], Some("tok2"));
        let xml3 = list_xml(&[("snapshots/def", 200)], None);
        let (port, captured, handle) =
            mock_server_capture_multi(vec![http_ok(&xml1), http_ok(&xml2), http_ok(&xml3)]);
        let backend = s3_backend(port, no_retry(), false);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/abc", "snapshots/def"]);
        assert_eq!(captured.lock().unwrap().len(), 3);
        handle.join().unwrap();
    }

    /// Stopping here would silently under-report keys, which is how a backup
    /// tool loses track of data. It has to be an error.
    #[test]
    fn list_truncated_without_token_is_an_error() {
        let xml = list_xml(&[("snapshots/abc", 100)], None).replace(
            "<IsTruncated>false</IsTruncated>",
            "<IsTruncated>true</IsTruncated>",
        );
        let resp = http_ok(&xml);
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let err = backend.list("snapshots/").unwrap_err().to_string();
        assert!(err.contains("failed to parse response"), "got: {err}");
        handle.join().unwrap();
    }

    /// A token alongside IsTruncated=false must not start another request: the
    /// mock accepts one connection, so a second page would fail the listing.
    #[test]
    fn list_complete_page_ignores_stray_token() {
        let xml = list_xml(&[("snapshots/abc", 100)], Some("stale")).replace(
            "<IsTruncated>true</IsTruncated>",
            "<IsTruncated>false</IsTruncated>",
        );
        let resp = http_ok(&xml);
        let (port, handle) = mock_server(&resp);
        let backend = s3_backend(port, no_retry(), false);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/abc"]);
        handle.join().unwrap();
    }

    /// Servers that omit IsTruncated still paginate on the token alone.
    #[test]
    fn list_follows_token_when_is_truncated_absent() {
        let xml1 = list_xml(&[("snapshots/abc", 100)], Some("tok1"))
            .replace("<IsTruncated>true</IsTruncated>\n", "");
        let xml2 = list_xml(&[("snapshots/def", 200)], None)
            .replace("<IsTruncated>false</IsTruncated>\n", "");
        assert!(!xml1.contains("IsTruncated") && !xml2.contains("IsTruncated"));
        let (port, handle) = mock_server_multi(vec![http_ok(&xml1), http_ok(&xml2)]);
        let backend = s3_backend(port, no_retry(), false);
        let keys = backend.list("snapshots/").unwrap();
        assert_eq!(keys, vec!["snapshots/abc", "snapshots/def"]);
        handle.join().unwrap();
    }

    /// The endpoint is always scheme://host[:port], so path-style URLs must stay
    /// `/<bucket>/<key>`, and the signed `host` must include a non-default port.
    #[test]
    fn signed_url_is_path_style_with_host_signed() {
        let backend = s3_backend(9000, no_retry(), false);
        let url = backend
            .bucket
            .get_object(Some(&backend.credentials), "packs/ab/cd")
            .sign(PRESIGN_DURATION);
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert_eq!(url.port(), Some(9000));
        assert_eq!(url.path(), "/test-bucket/packs/ab/cd");
        assert!(
            url.query()
                .unwrap_or_default()
                .contains("X-Amz-SignedHeaders=host"),
            "got: {url}"
        );
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

    // ── 11. Connection-close hint on exhausted PUT retries ──────────

    /// Build a transport error carrying the given I/O kind, as the retry layer
    /// produces it after exhausting retries on a connection-close PUT.
    fn transport(kind: std::io::ErrorKind) -> HttpRetryError {
        HttpRetryError::Transport(format!("io: {kind:?}"), Some(kind))
    }

    #[test]
    fn put_broken_pipe_appends_hint() {
        let err = transport(std::io::ErrorKind::BrokenPipe);
        let msg = s3_error("PUT", "index", err).to_string();
        assert!(
            msg.contains("connection was closed during the PUT request"),
            "expected hint, got: {msg}"
        );
        assert!(
            msg.contains("network middleware"),
            "expected candidate causes, got: {msg}"
        );
    }

    #[test]
    fn put_connection_reset_and_aborted_append_hint() {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
        ] {
            let msg = s3_error("PUT", "index", transport(kind)).to_string();
            assert!(
                msg.contains("connection was closed during the PUT request"),
                "{kind:?}: expected hint, got: {msg}"
            );
        }
    }

    #[test]
    fn put_pack_key_gets_upload_specific_clause() {
        let err = transport(std::io::ErrorKind::BrokenPipe);
        let msg = s3_error("PUT", "packs/ab/abcdef", err).to_string();
        assert!(
            msg.contains("Large pack uploads are most exposed"),
            "expected upload-specific clause, got: {msg}"
        );
    }

    #[test]
    fn put_non_pack_key_omits_upload_specific_clause() {
        let err = transport(std::io::ErrorKind::BrokenPipe);
        let msg = s3_error("PUT", "snapshots/abc", err).to_string();
        assert!(
            msg.contains("connection was closed during the PUT request"),
            "expected hint, got: {msg}"
        );
        assert!(
            !msg.contains("Large pack uploads"),
            "non-pack key should not get upload-specific clause, got: {msg}"
        );
    }

    #[test]
    fn put_non_close_transport_kind_gets_no_hint() {
        // A transport error whose kind is not a connection close must not get
        // the hint — it does not describe the failure. UnexpectedEof is a real
        // retryable transport kind (unlike a non-retryable kind, which the retry
        // layer maps to Permanent and never carries in Transport).
        let err = transport(std::io::ErrorKind::UnexpectedEof);
        let msg = s3_error("PUT", "packs/ab/abcdef", err).to_string();
        assert!(
            !msg.contains("connection was closed"),
            "non-close kind should not get hint, got: {msg}"
        );
    }

    #[test]
    fn non_put_op_gets_no_hint() {
        // The same broken-pipe transport error on a GET (or any non-PUT op)
        // must not get the PUT-specific hint.
        let err = transport(std::io::ErrorKind::BrokenPipe);
        let msg = s3_error("GET", "packs/ab/abcdef", err).to_string();
        assert!(
            !msg.contains("connection was closed"),
            "non-PUT op should not get hint, got: {msg}"
        );
    }

    #[test]
    fn s3_error_keeps_interrupted_typed() {
        assert!(matches!(
            s3_error("GET", "k", HttpRetryError::Interrupted),
            VykarError::Interrupted
        ));
    }

    #[test]
    fn s3_cancel_during_retry_wait_returns_interrupted() {
        let (port, handle) =
            mock_server("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
        let token = CancelToken::new();
        let backend = s3_backend(
            port,
            RetryConfig {
                max_retries: 5,
                retry_delay_ms: 60_000,
                retry_max_delay_ms: 60_000,
            },
            false,
        )
        .with_cancel(token.clone());
        let bound = Duration::from_secs(5);
        std::thread::scope(|s| {
            let get = s.spawn(|| backend.get("config"));
            let deadline = std::time::Instant::now() + bound;
            while !handle.is_finished() {
                assert!(std::time::Instant::now() < deadline, "request not answered");
                std::thread::sleep(Duration::from_millis(5));
            }
            let cancelled_at = std::time::Instant::now();
            token.cancel();
            let err = get.join().unwrap().unwrap_err();
            let latency = cancelled_at.elapsed();
            assert!(matches!(err, VykarError::Interrupted), "{err}");
            assert!(latency < bound, "{latency:?}");
        });
        handle.join().unwrap();
    }
    // ── Conditional writes and list_meta ─────────────────────────────

    fn conditional_backend(port: u16, retry: RetryConfig) -> S3Backend {
        s3_backend(port, retry, false).with_conditional_writes(S3ConditionalWrites::On)
    }

    #[test]
    fn conditional_put_signs_and_sends_if_none_match() {
        let (port, captured, handle) = mock_server_capture_bodies(vec![
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_string(),
        ]);
        let backend = conditional_backend(port, no_retry());
        assert_eq!(
            backend.put_if_absent_raw("locks/k", b"d").unwrap(),
            Created::Yes
        );
        handle.join().unwrap();
        let (lines, body) = captured.lock().unwrap()[0].clone();
        assert_eq!(body, b"d");
        assert!(
            lines
                .iter()
                .any(|l| l.to_lowercase().replace(' ', "") == "if-none-match:*"),
            "{lines:?}"
        );
        let request_line = &lines[0];
        let target = request_line.split_whitespace().nth(1).unwrap();
        let url = url::Url::parse(&format!("http://h{target}")).unwrap();
        let signed = url
            .query_pairs()
            .find(|(k, _)| k == "X-Amz-SignedHeaders")
            .map(|(_, v)| v.into_owned())
            .unwrap();
        assert!(signed.split(';').any(|h| h == "if-none-match"), "{signed}");
    }

    #[test]
    fn conditional_put_412_is_already_exists() {
        let port = serve(|_| {
            (
                "412 Precondition Failed",
                s3_error_xml("PreconditionFailed", "m"),
            )
        });
        let backend = conditional_backend(port, no_retry());
        assert_eq!(
            backend.put_if_absent_raw("locks/k", b"d").unwrap(),
            Created::AlreadyExists
        );
    }

    #[test]
    fn conditional_put_retries_409() {
        let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = n.clone();
        let port = serve(move |_| {
            if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                (
                    "409 Conflict",
                    s3_error_xml("ConditionalRequestConflict", "m"),
                )
            } else {
                ("200 OK", String::new())
            }
        });
        let backend = conditional_backend(port, fast_retry());
        assert_eq!(
            backend.put_if_absent_raw("locks/k", b"d").unwrap(),
            Created::Yes
        );
        assert_eq!(n.load(std::sync::atomic::Ordering::SeqCst), 2);

        let port = serve(|_| {
            (
                "409 Conflict",
                s3_error_xml("ConditionalRequestConflict", "m"),
            )
        });
        let backend = conditional_backend(port, no_retry());
        assert!(backend.put_if_absent_raw("locks/k", b"d").is_err());
    }

    #[test]
    fn conditional_refusals_make_verify_unsupported() {
        for (status, code) in [
            ("501 Not Implemented", "NotImplemented"),
            ("400 Bad Request", "InvalidRequest"),
        ] {
            let port = serve(move |req| {
                if req.method == "PUT" {
                    (status, s3_error_xml(code, "m"))
                } else {
                    ("204 No Content", String::new())
                }
            });
            let backend = conditional_backend(port, no_retry());
            let support = backend.conditional().unwrap().verify().unwrap();
            assert!(
                matches!(support, ConditionalSupport::Unsupported { .. }),
                "{status}: {support:?}"
            );
            assert!(backend.conditional().is_none());
        }
    }

    /// Garage 2.5.0: every PUT succeeds, the condition is ignored.
    #[test]
    fn a_store_that_ignores_the_condition_is_refused() {
        let stored = Arc::new(Mutex::new(Vec::new()));
        let s = stored.clone();
        let port = serve_bytes(move |req| match req.method.as_str() {
            "PUT" => {
                *s.lock().unwrap() = req.body.clone();
                ("200 OK", Vec::new())
            }
            "GET" => ("200 OK", s.lock().unwrap().clone()),
            _ => ("204 No Content", Vec::new()),
        });
        let backend = conditional_backend(port, no_retry());
        let ConditionalSupport::Unsupported { reason } =
            backend.conditional().unwrap().verify().unwrap()
        else {
            panic!("an ignored condition must be Unsupported");
        };
        assert!(reason.contains("ignores the condition"), "{reason}");
        assert!(backend.conditional().is_none());
        match backend.put_if_absent("locks/x", b"x") {
            Err(VykarError::UnsupportedBackend(m)) => {
                assert!(m.contains("s3_conditional_writes"), "{m}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn conditional_mode_gates_the_capability() {
        let local = |mode| s3_backend(1, no_retry(), false).with_conditional_writes(mode);
        assert!(local(S3ConditionalWrites::Off).conditional().is_none());
        assert!(local(S3ConditionalWrites::Auto).conditional().is_none());
        assert!(local(S3ConditionalWrites::On).conditional().is_some());
        let aws = S3Backend::new(
            "b",
            "eu-west-1",
            "",
            "https://s3.eu-west-1.amazonaws.com",
            "AKID",
            "SECRET",
            no_retry(),
            false,
            None,
        )
        .unwrap();
        assert!(aws.conditional().is_some(), "auto trusts AWS");
        assert!(
            aws.with_conditional_writes(S3ConditionalWrites::Off)
                .conditional()
                .is_none()
        );
    }

    #[test]
    fn over_cap_put_if_absent_sends_nothing() {
        let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = n.clone();
        let port = serve(move |_| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ("200 OK", String::new())
        });
        let backend = conditional_backend(port, no_retry());
        let big = vec![0u8; crate::MAX_PUT_IF_ABSENT_BYTES + 1];
        assert!(matches!(
            backend.put_if_absent("locks/k", &big),
            Err(VykarError::InvalidFormat(_))
        ));
        assert_eq!(n.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn list_meta_parses_last_modified() {
        let xml = |modified: &str| {
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <Name>test-bucket</Name><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>\
                 <Contents><Key>locks/a</Key><Size>3</Size>\
                 <LastModified>{modified}</LastModified></Contents></ListBucketResult>"
            )
        };
        let good = xml("2024-01-01T00:00:00.000Z");
        let port = serve(move |_| ("200 OK", good.clone()));
        let backend = s3_backend(port, no_retry(), false);
        let listed = backend.list_meta("locks/").unwrap();
        assert_eq!(
            listed,
            vec![ListedObject {
                key: "locks/a".into(),
                size: Some(3),
                mtime: Some(std::time::UNIX_EPOCH + Duration::from_secs(1_704_067_200)),
            }]
        );
        assert_eq!(backend.list_sized("locks/").unwrap()[0].mtime, None);

        let bad = xml("not a time");
        let port = serve(move |_| ("200 OK", bad.clone()));
        let backend = s3_backend(port, no_retry(), false);
        assert_eq!(backend.list_meta("locks/").unwrap()[0].mtime, None);
    }

    #[test]
    fn conformance_against_the_stateful_mock() {
        let backend = conditional_backend(stateful_s3(), no_retry());
        crate::conformance::run(&backend, &crate::conformance::ConformanceOptions::default())
            .unwrap();
    }

    /// Opt-in run against a real S3 service (MinIO, AWS). Returns at once when
    /// `VYKAR_TEST_S3_URL` or the keys are unset. With
    /// `VYKAR_TEST_S3_CONDITIONAL=auto` (AWS) it also asserts that `auto`
    /// recognises the endpoint.
    #[test]
    #[ignore = "set VYKAR_TEST_S3_URL etc. to run against a real S3 service"]
    fn conformance_real_s3() {
        let (Ok(url), Ok(key_id), Ok(secret)) = (
            std::env::var("VYKAR_TEST_S3_URL"),
            std::env::var("VYKAR_TEST_S3_ACCESS_KEY_ID"),
            std::env::var("VYKAR_TEST_S3_SECRET_ACCESS_KEY"),
        ) else {
            return;
        };
        let mode = match std::env::var("VYKAR_TEST_S3_CONDITIONAL").as_deref() {
            Ok("auto") => S3ConditionalWrites::Auto,
            _ => S3ConditionalWrites::On,
        };
        let cfg = crate::StorageConfig {
            allow_insecure_http: url.starts_with("s3+http://"),
            region: std::env::var("VYKAR_TEST_S3_REGION").ok(),
            access_key_id: Some(key_id),
            secret_access_key: Some(secret),
            sftp_key: None,
            sftp_agent: None,
            sftp_known_hosts: None,
            max_connections: None,
            sftp_timeout: None,
            access_token: None,
            retry: RetryConfig::default(),
            s3_soft_delete: false,
            s3_conditional_writes: mode,
            cancel: vykar_types::cancel::CancelToken::new(),
            url,
        };
        let backend = crate::backend_from_config(&cfg).unwrap();
        assert!(
            backend.conditional().is_some(),
            "{mode:?} offers put-if-absent"
        );
        crate::conformance::run(
            &*backend,
            &crate::conformance::ConformanceOptions::default(),
        )
        .unwrap();
    }
}
