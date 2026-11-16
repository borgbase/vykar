#![allow(
    clippy::duration_suboptimal_units,
    clippy::map_unwrap_or,
    clippy::manual_let_else,
    clippy::missing_errors_doc,
    clippy::redundant_closure_for_method_calls,
    clippy::used_underscore_binding
)]

mod parts;

use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use blake2::{Blake2b256, Digest};

use crate::retry::HttpRetryError;
use crate::RetryConfig;
use vykar_types::error::{Result, VykarError};

use crate::{
    ListedObject, RepackPlanRequest, RepackResultResponse, ServerCapabilities, StorageBackend,
    VerifyPacksPlanRequest, VerifyPacksResponse,
};
use vykar_types::hash::HashAlgorithm;

/// Read a short prefix of a response body for use in an error message.
///
/// Bounded and lossy on purpose: this is diagnostic text appended to an HTTP
/// status, not data. Returns an empty string when the body is unreadable or
/// blank so the caller's message degrades to the bare status.
fn read_body_snippet(resp: &mut http::Response<ureq::Body>) -> String {
    const MAX: u64 = 512;
    let mut buf = Vec::new();
    if resp
        .body_mut()
        .as_reader()
        .take(MAX)
        .read_to_end(&mut buf)
        .is_err()
        || buf.is_empty()
    {
        return String::new();
    }
    let text = String::from_utf8_lossy(&buf);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        String::new()
    } else {
        format!(": {trimmed}")
    }
}

/// Phase timeouts for [`RestBackend`]. `Default` uses the crate constants;
/// tests shrink them to exercise stalled servers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HttpTimeouts {
    pub resolve: Duration,
    pub send_request: Duration,
    /// Response-header wait for data operations (agent default).
    pub recv_response: Duration,
    /// Response-header wait for admin actions that run server-side work.
    pub admin: Duration,
    /// Response-header wait for server-side repack.
    pub repack: Duration,
}

impl Default for HttpTimeouts {
    fn default() -> Self {
        Self {
            resolve: crate::HTTP_RESOLVE_TIMEOUT,
            send_request: crate::HTTP_SEND_REQUEST_TIMEOUT,
            recv_response: crate::HTTP_RECV_RESPONSE_TIMEOUT,
            admin: crate::HTTP_ADMIN_RECV_RESPONSE_TIMEOUT,
            repack: crate::HTTP_REPACK_RECV_RESPONSE_TIMEOUT,
        }
    }
}

/// HTTP REST backend for remote repository access via vykar-server.
pub struct RestBackend {
    /// Base URL, e.g. `https://backup.example.com`
    base_url: String,
    agent: ureq::Agent,
    /// Pre-rendered `Authorization` header value (`Bearer <token>`), or `None`
    /// for an unauthenticated repository. Rendered once so the hot GET/PUT path
    /// does not allocate a header string per request.
    bearer: Option<String>,
    retry: RetryConfig,
    timeouts: HttpTimeouts,
    /// The repository's content-digest algorithm, bound once by
    /// `Repository::init`/`open` after the format is resolved.
    ///
    /// A `OnceLock` rather than a setter over a `Mutex`/atomic: backends are
    /// held as `Arc<dyn StorageBackend>`, and bind-once needs to be the
    /// natural implementation, not a convention. An atomic would prevent a
    /// data race without preventing uploads from disagreeing with the pack
    /// writer. Unset means BLAKE2b, which is what every repository used before
    /// format v3 and what a caller that never binds should get.
    content_hash: OnceLock<HashAlgorithm>,
    /// Set once the server has answered `?sizes` as an unknown request, so a
    /// process probes an old server at most once.
    sizes_unsupported: AtomicBool,
    /// Whether the server accepts pack uploads in parts, from `/health`.
    /// Probed on the first pack large enough to split (or recorded by an
    /// explicit [`StorageBackend::server_capabilities`] call), so a backup
    /// that uploads nothing pays no extra round trip.
    pack_parts: OnceLock<bool>,
    /// Set when the part path failed permanently; later packs use whole PUTs.
    parts_disabled: AtomicBool,
    /// Set by the first completed part upload. Until then a part upload
    /// keeps its whole-PUT fallback through the completion (see
    /// [`StorageBackend::put_owned`]).
    parts_proven: AtomicBool,
    /// Backend-wide budget of concurrent part PUTs.
    part_slots: crate::http_util::PartSlots,
    /// Target part size; packs at or below it are sent whole.
    part_size: usize,
}

impl RestBackend {
    pub fn new(
        base_url: &str,
        token: Option<&str>,
        retry: RetryConfig,
        max_connections: Option<usize>,
    ) -> Result<Self> {
        Self::with_timeouts(
            base_url,
            token,
            retry,
            max_connections,
            HttpTimeouts::default(),
        )
    }

    pub(crate) fn with_timeouts(
        base_url: &str,
        token: Option<&str>,
        retry: RetryConfig,
        max_connections: Option<usize>,
        timeouts: HttpTimeouts,
    ) -> Result<Self> {
        let part_concurrency = parts::part_concurrency(max_connections);
        // Part streams plus a few control connections stay pooled: ureq hands
        // out the oldest idle connection, and a part sent on a fresh one pays
        // a handshake and slow start.
        let pool = crate::http_idle_pool_size(max_connections).max(part_concurrency + 4);
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_resolve(Some(timeouts.resolve))
            .timeout_connect(Some(Duration::from_secs(30)))
            .timeout_send_request(Some(timeouts.send_request))
            .timeout_recv_response(Some(timeouts.recv_response))
            .timeout_send_body(Some(Duration::from_secs(5 * 60)))
            .timeout_recv_body(Some(Duration::from_secs(5 * 60)))
            .max_idle_connections_per_host(pool)
            .max_idle_connections(pool)
            .max_idle_age(crate::HTTP_IDLE_AGE)
            .tls_config(crate::http_tls_config())
            .build()
            .into();

        let base = base_url.trim_end_matches('/').to_string();

        Ok(Self {
            base_url: base,
            agent,
            bearer: token.map(|t| format!("Bearer {t}")),
            retry,
            timeouts,
            content_hash: OnceLock::new(),
            sizes_unsupported: AtomicBool::new(false),
            pack_parts: OnceLock::new(),
            parts_disabled: AtomicBool::new(false),
            parts_proven: AtomicBool::new(false),
            part_slots: crate::http_util::PartSlots::new(part_concurrency),
            part_size: parts::part_size(),
        })
    }

    fn url(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        format!("{}/{}", self.base_url, key)
    }

    /// Attach the `Authorization` header when the repository is authenticated.
    fn authed<B>(&self, req: ureq::RequestBuilder<B>) -> ureq::RequestBuilder<B> {
        match self.bearer.as_deref() {
            Some(bearer) => req.header("Authorization", bearer),
            None => req,
        }
    }

    /// Override the response-header wait for a request whose server-side work
    /// runs before the headers are sent.
    fn recv_response_within<B>(
        req: ureq::RequestBuilder<B>,
        timeout: Duration,
    ) -> ureq::RequestBuilder<B> {
        req.config().timeout_recv_response(Some(timeout)).build()
    }

    /// Unified retry wrapper for HTTP calls with response handling.
    fn retry_call<T>(
        &self,
        op_name: &str,
        f: impl Fn() -> std::result::Result<http::Response<ureq::Body>, ureq::Error>,
        handle_response: impl Fn(http::Response<ureq::Body>) -> std::result::Result<T, HttpRetryError>,
    ) -> std::result::Result<T, HttpRetryError> {
        self.retry_call_with(&self.retry, op_name, f, handle_response)
    }

    fn retry_call_with<T>(
        &self,
        retry: &RetryConfig,
        op_name: &str,
        f: impl Fn() -> std::result::Result<http::Response<ureq::Body>, ureq::Error>,
        handle_response: impl Fn(http::Response<ureq::Body>) -> std::result::Result<T, HttpRetryError>,
    ) -> std::result::Result<T, HttpRetryError> {
        crate::retry::retry_http(retry, op_name, "REST", f, handle_response)
    }

    /// POST `payload` as JSON and deserialize the JSON response body.
    ///
    /// Body reads go through a streaming reader rather than `read_json()`,
    /// whose 10 MB default limit a repack result can exceed.
    fn post_json<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        op_name: &str,
        url: &str,
        payload: &Req,
        recv_response: Duration,
        retry: &RetryConfig,
    ) -> Result<Resp> {
        let body = self
            .retry_call_with(
                retry,
                op_name,
                || {
                    let req = self.authed(self.agent.post(url));
                    Self::recv_response_within(req, recv_response).send_json(payload)
                },
                |mut resp| {
                    let status = resp.status().as_u16();
                    if status >= 400 {
                        // Include the server's own message. Without it a 400
                        // (e.g. "protocol version 2 not supported") reaches
                        // the user as a bare "HTTP 400".
                        let detail = read_body_snippet(&mut resp);
                        crate::retry::classify_status(
                            status,
                            format!("REST {op_name} failed: HTTP {status}{detail}"),
                        )?;
                    }
                    let mut buf = Vec::new();
                    resp.body_mut()
                        .as_reader()
                        .read_to_end(&mut buf)
                        .map_err(HttpRetryError::BodyIo)?;
                    Ok(buf)
                },
            )
            .map_err(|e| VykarError::Other(format!("REST {op_name}: {e}")))?;
        serde_json::from_slice(&body)
            .map_err(|e| VykarError::Other(format!("REST {op_name} parse: {e}")))
    }

    /// GET `/health` and parse the server's advertised capabilities.
    fn fetch_server_capabilities(&self) -> Result<ServerCapabilities> {
        let url = format!("{}/health", self.base_url);
        self.retry_call(
            "health",
            || self.authed(self.agent.get(&url)).call(),
            |mut resp| {
                let status = resp.status().as_u16();
                if status >= 400 {
                    crate::retry::classify_status(status, format!("REST health: HTTP {status}"))?;
                }
                let mut buf = Vec::new();
                resp.body_mut()
                    .as_reader()
                    .read_to_end(&mut buf)
                    .map_err(HttpRetryError::BodyIo)?;
                Ok(buf)
            },
        )
        .map_err(|e| VykarError::Other(format!("REST health: {e}")))
        .and_then(|body| {
            serde_json::from_slice(&body)
                .map_err(|e| VykarError::Other(format!("REST health parse: {e}")))
        })
    }

    /// POST `payload` as JSON, ignoring the response body.
    fn post_no_content<Req: serde::Serialize>(
        &self,
        op_name: &str,
        url: &str,
        payload: &Req,
        recv_response: Duration,
    ) -> Result<()> {
        self.retry_call(
            op_name,
            || {
                let req = self.authed(self.agent.post(url));
                Self::recv_response_within(req, recv_response).send_json(payload)
            },
            |resp| {
                let status = resp.status().as_u16();
                crate::retry::classify_status(
                    status,
                    format!("REST {op_name} failed: HTTP {status}"),
                )
            },
        )
        .map_err(|e| VykarError::Other(format!("REST {op_name}: {e}")))
    }

    /// Batch delete multiple keys in a single request.
    pub fn batch_delete(&self, keys: &[String], cleanup_dirs: bool) -> Result<()> {
        let url = if cleanup_dirs {
            format!("{}?batch-delete&cleanup-dirs", self.base_url)
        } else {
            format!("{}?batch-delete", self.base_url)
        };
        self.post_no_content("batch-delete", &url, &keys, self.timeouts.admin)
    }

    /// Send a verify-packs plan to the server for server-side pack verification.
    pub fn verify_packs(&self, plan: &VerifyPacksPlanRequest) -> Result<VerifyPacksResponse> {
        let url = format!("{}?verify-packs", self.base_url);
        self.post_json("verify-packs", &url, plan, self.timeouts.admin, &self.retry)
    }

    /// One `POST /<dir>?sizes` for keys that all lie below `dir`.
    ///
    /// `Ok(None)` when the server does not know the endpoint: a server that
    /// predates it answers 400 (its `POST` handler only knows `?mkdir`), and
    /// 404/405/501 cover a proxy that refuses the request.
    fn post_sizes(&self, dir: &str, keys: &[String]) -> Result<Option<Vec<Option<u64>>>> {
        let url = format!("{}?sizes", self.url(dir));
        let op_name = format!("SIZES {dir}/");
        let body = self
            .retry_call(
                &op_name,
                || {
                    let req = self.authed(self.agent.post(&url));
                    Self::recv_response_within(req, self.timeouts.admin).send_json(keys)
                },
                |mut resp| {
                    let status = resp.status().as_u16();
                    if matches!(status, 400 | 404 | 405 | 501) {
                        return Ok(None);
                    }
                    if status >= 400 {
                        let detail = read_body_snippet(&mut resp);
                        crate::retry::classify_status(
                            status,
                            format!("REST {op_name} failed: HTTP {status}{detail}"),
                        )?;
                    }
                    let mut buf = Vec::new();
                    resp.body_mut()
                        .as_reader()
                        .read_to_end(&mut buf)
                        .map_err(HttpRetryError::BodyIo)?;
                    Ok(Some(buf))
                },
            )
            .map_err(|e| VykarError::Other(format!("REST {op_name}: {e}")))?;
        let Some(body) = body else {
            return Ok(None);
        };
        let sizes: Vec<Option<u64>> = serde_json::from_slice(&body)
            .map_err(|e| VykarError::Other(format!("REST {op_name} parse: {e}")))?;
        if sizes.len() != keys.len() {
            return Err(VykarError::Other(format!(
                "REST {op_name}: server answered {} sizes for {} keys",
                sizes.len(),
                keys.len()
            )));
        }
        Ok(Some(sizes))
    }

    /// Send a repack plan to the server for server-side compaction.
    ///
    /// Not retried: a retry after a timeout could stall the caller for hours.
    /// The next compact run retries instead.
    pub fn repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        let url = format!("{}?repack", self.base_url);
        let retry = RetryConfig {
            max_retries: 0,
            ..self.retry
        };
        self.post_json("repack", &url, plan, self.timeouts.repack, &retry)
    }
}

impl RestBackend {
    /// Validate a `Content-Range: bytes {start}-{end}/{total}` header against
    /// the requested offset and length.
    fn validate_content_range(
        header: &str,
        expected_offset: u64,
        expected_length: u64,
        key: &str,
    ) -> Result<()> {
        // Expected format: "bytes {start}-{end}/{total}"
        let rest = header.strip_prefix("bytes ").ok_or_else(|| {
            VykarError::Other(format!(
                "REST GET_RANGE {key}: malformed Content-Range header: {header}"
            ))
        })?;
        let (range_part, _total) = rest.split_once('/').ok_or_else(|| {
            VykarError::Other(format!(
                "REST GET_RANGE {key}: malformed Content-Range header: {header}"
            ))
        })?;
        let (start_str, end_str) = range_part.split_once('-').ok_or_else(|| {
            VykarError::Other(format!(
                "REST GET_RANGE {key}: malformed Content-Range header: {header}"
            ))
        })?;
        let start: u64 = start_str.parse().map_err(|_| {
            VykarError::Other(format!(
                "REST GET_RANGE {key}: malformed Content-Range start: {header}"
            ))
        })?;
        let end: u64 = end_str.parse().map_err(|_| {
            VykarError::Other(format!(
                "REST GET_RANGE {key}: malformed Content-Range end: {header}"
            ))
        })?;
        let range_len = end
            .checked_sub(start)
            .and_then(|d| d.checked_add(1))
            .ok_or_else(|| {
                VykarError::Other(format!(
                    "REST GET_RANGE {key}: Content-Range overflow or end < start: {header}"
                ))
            })?;
        if start != expected_offset || range_len != expected_length {
            return Err(VykarError::Other(format!(
                "REST GET_RANGE {key}: Content-Range mismatch: got {header}, \
                 expected bytes {expected_offset}-{}/{}",
                expected_offset + expected_length - 1,
                _total
            )));
        }
        Ok(())
    }

    /// Extract the 64-char hex pack ID from a storage key like `packs/ab/<hex>`.
    /// Returns `None` for non-pack keys. Zero CPU cost — just a slice.
    fn try_extract_pack_hex(key: &str) -> Option<&str> {
        let rest = key.strip_prefix("packs/")?;
        // Skip the 2-char shard + '/'
        let hex = rest.get(3..)?;
        if hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            Some(hex)
        } else {
            None
        }
    }

    /// Compute unkeyed BLAKE2b-256 and return the 64-char hex string.
    /// Used for non-pack objects (manifest, index, snapshots, config).
    fn compute_blake2b_256_hex(data: &[u8]) -> String {
        let mut hasher = Blake2b256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    /// Whether packs go up in parts: the server advertised `pack_parts` and
    /// the path has not failed permanently. The first call probes `/health`;
    /// a failed probe counts as "not supported".
    fn pack_parts_enabled(&self) -> bool {
        if self.parts_disabled.load(Ordering::Relaxed) {
            return false;
        }
        *self.pack_parts.get_or_init(|| {
            self.fetch_server_capabilities()
                .map(|caps| caps.pack_parts)
                .unwrap_or_else(|e| {
                    tracing::debug!("REST: capability probe failed, uploading packs whole: {e}");
                    false
                })
        })
    }

    /// The content-digest algorithm this backend was bound to, defaulting to
    /// BLAKE2b when nothing bound it.
    fn content_hash(&self) -> HashAlgorithm {
        self.content_hash.get().copied().unwrap_or_default()
    }

    /// The digest header naming a pack's content hash.
    fn pack_digest_header(&self) -> &'static str {
        match self.content_hash() {
            HashAlgorithm::Blake2b => "X-Content-BLAKE2b",
            HashAlgorithm::Blake3 => "X-Content-BLAKE3",
        }
    }

    /// Whether a pack of `len` bytes goes up in parts.
    fn pack_parts_for(&self, len: usize) -> bool {
        len > self.part_size && self.pack_parts_enabled()
    }

    /// Log a part upload that will be retried as a whole PUT, and turn the
    /// part path off if the failure would recur.
    fn note_parts_failure(&self, key: &str, failure: &parts::PartsFailure) {
        if failure.permanent {
            self.parts_disabled.store(true, Ordering::Relaxed);
        }
        tracing::warn!(
            "REST PUT {key}: part upload failed ({}); sending the pack whole{}",
            failure.message,
            if failure.permanent {
                ", and every later one"
            } else {
                ""
            }
        );
    }

    /// Shared PUT implementation for borrowed data.
    ///
    /// Note what this does for pack keys: it never hashes them. A pack key
    /// *is* the pack ID, so the hex is lifted straight out of the key — which
    /// means that for packs only the header *name* changes with the
    /// algorithm, because the value is already the right digest. Non-pack
    /// objects are hashed here and stay BLAKE2b unconditionally.
    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        let (header, checksum) = match Self::try_extract_pack_hex(key) {
            Some(pack_hex) => {
                let header = self.pack_digest_header();
                if self.pack_parts_for(data.len()) {
                    match self.put_pack_in_parts(key, pack_hex, header, data) {
                        Ok(()) => return Ok(()),
                        Err(failure) => self.note_parts_failure(key, &failure),
                    }
                }
                (header, pack_hex.to_string())
            }
            None => ("X-Content-BLAKE2b", Self::compute_blake2b_256_hex(data)),
        };
        self.put_whole(key, header, &checksum, data)
    }

    /// One whole-object PUT carrying its content digest.
    fn put_whole(&self, key: &str, header: &str, checksum: &str, data: &[u8]) -> Result<()> {
        let url = self.url(key);
        self.retry_call(
            &format!("PUT {key}"),
            || {
                self.authed(self.agent.put(&url))
                    .header(header, checksum)
                    .send(data)
            },
            |resp| {
                let status = resp.status().as_u16();
                if status >= 400 {
                    crate::retry::classify_status(
                        status,
                        format!("REST PUT {key}: HTTP {status}"),
                    )?;
                }
                Ok(())
            },
        )
        .map_err(|e| VykarError::Other(format!("REST PUT {key}: {e}")))?;
        Ok(())
    }
}

impl RestBackend {
    /// GET a listing; `sized` adds `&sized` (see [`vykar_protocol::SizedKey`]).
    fn fetch_listing(&self, prefix: &str, sized: bool) -> Result<Vec<u8>> {
        let query = if sized { "list&sized" } else { "list" };
        let prefix = prefix.trim_start_matches('/');
        let url = if prefix.is_empty() {
            format!("{}?{query}", self.base_url)
        } else {
            format!("{}?{query}", self.url(prefix))
        };
        self.retry_call(
            &format!("LIST {prefix}"),
            || {
                let req = self.authed(self.agent.get(&url));
                Self::recv_response_within(req, self.timeouts.admin).call()
            },
            |mut resp| {
                let status = resp.status().as_u16();
                if status >= 400 {
                    crate::retry::classify_status(
                        status,
                        format!("REST LIST {prefix}: HTTP {status}"),
                    )?;
                }
                let mut buf = Vec::new();
                resp.body_mut()
                    .as_reader()
                    .read_to_end(&mut buf)
                    .map_err(HttpRetryError::BodyIo)?;
                Ok(buf)
            },
        )
        .map_err(|e| VykarError::Other(format!("REST LIST {prefix}: {e}")))
    }
}

impl StorageBackend for RestBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let url = self.url(key);
        self.retry_call(
            &format!("GET {key}"),
            || self.authed(self.agent.get(&url)).call(),
            |mut resp| {
                let status = resp.status().as_u16();
                if status == 404 {
                    return Ok(None);
                }
                if status >= 400 {
                    crate::retry::classify_status(
                        status,
                        format!("REST GET {key}: HTTP {status}"),
                    )?;
                }
                let mut buf = Vec::new();
                resp.body_mut()
                    .as_reader()
                    .read_to_end(&mut buf)
                    .map_err(HttpRetryError::BodyIo)?;
                Ok(Some(buf))
            },
        )
        .map_err(|e| VykarError::Other(format!("REST GET {key}: {e}")))
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.put_bytes(key, data)
    }

    /// Like `put`, except that a pack sent in parts releases its buffer once
    /// every part is acknowledged, before the completion request: the server
    /// then hashes, fsyncs and publishes the assembled file, which is the
    /// slow end of the upload, and nothing in it needs the bytes.
    ///
    /// The cost is the fallback: a completion that still fails after its
    /// in-place retries can no longer be retried as a whole PUT, so it is
    /// returned as the upload's error (the pack is not stored; the backup
    /// fails rather than claiming it). A failure during the part phase still
    /// falls back as `put` does, and until one completion has succeeded on
    /// this backend the buffer is kept through it, so a server or proxy that
    /// accepts parts but refuses completions still gets whole PUTs.
    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let Some(pack_hex) = Self::try_extract_pack_hex(key) else {
            return self.put_bytes(key, &data);
        };
        if !self.parts_proven.load(Ordering::Relaxed) || !self.pack_parts_for(data.len()) {
            return self.put_bytes(key, &data);
        }
        let header = self.pack_digest_header();
        match self.send_pack_parts(key, &data) {
            Ok(token) => {
                drop(data);
                self.complete_pack_parts(key, pack_hex, header, &token)
                    .map_err(|failure| {
                        if failure.permanent {
                            self.parts_disabled.store(true, Ordering::Relaxed);
                        }
                        VykarError::Other(format!(
                            "REST PUT {key}: {} (the pack buffer was already \
                             released, so it cannot be resent whole)",
                            failure.message
                        ))
                    })
            }
            Err(failure) => {
                self.note_parts_failure(key, &failure);
                self.put_whole(key, header, pack_hex, &data)
            }
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        let url = self.url(key);
        self.retry_call(
            &format!("DELETE {key}"),
            || self.authed(self.agent.delete(&url)).call(),
            |resp| {
                let status = resp.status().as_u16();
                if status == 404 {
                    return Ok(());
                }
                if status >= 400 {
                    crate::retry::classify_status(
                        status,
                        format!("REST DELETE {key}: HTTP {status}"),
                    )?;
                }
                Ok(())
            },
        )
        .map_err(|e| VykarError::Other(format!("REST DELETE {key}: {e}")))
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let url = self.url(key);
        self.retry_call(
            &format!("HEAD {key}"),
            || self.authed(self.agent.head(&url)).call(),
            |resp| {
                let status = resp.status().as_u16();
                if status == 404 {
                    return Ok(false);
                }
                if status >= 400 {
                    crate::retry::classify_status(
                        status,
                        format!("REST HEAD {key}: HTTP {status}"),
                    )?;
                }
                Ok(true)
            },
        )
        .map_err(|e| VykarError::Other(format!("REST HEAD {key}: {e}")))
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        let url = self.url(key);
        self.retry_call(
            &format!("HEAD {key}"),
            || self.authed(self.agent.head(&url)).call(),
            |resp| {
                let status = resp.status().as_u16();
                if status == 404 {
                    return Ok(None);
                }
                if status >= 400 {
                    crate::retry::classify_status(
                        status,
                        format!("REST HEAD {key}: HTTP {status}"),
                    )?;
                }
                let len = crate::http_util::extract_content_length(
                    resp.headers(),
                    &format!("REST HEAD {key}"),
                )
                .map_err(|e| HttpRetryError::Permanent(e.to_string()))?;
                Ok(Some(len))
            },
        )
        .map_err(|e| VykarError::Other(format!("REST HEAD {key}: {e}")))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        // Plain listing: sizes would roughly double large pack listings only
        // to be discarded.
        let body = self.fetch_listing(prefix, false)?;
        serde_json::from_slice(&body)
            .map_err(|e| VykarError::Other(format!("REST LIST parse: {e}")))
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        let body = self.fetch_listing(prefix, true)?;
        // A server that predates `sized` ignores it and returns bare keys.
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum Listing {
            Sized(Vec<vykar_protocol::SizedKey>),
            Plain(Vec<String>),
        }
        let listing: Listing = serde_json::from_slice(&body)
            .map_err(|e| VykarError::Other(format!("REST LIST parse: {e}")))?;
        Ok(match listing {
            Listing::Sized(keys) => keys
                .into_iter()
                .map(|k| ListedObject {
                    key: k.key,
                    size: Some(k.size),
                })
                .collect(),
            Listing::Plain(keys) => keys
                .into_iter()
                .map(|key| ListedObject { key, size: None })
                .collect(),
        })
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        if length == 0 {
            return Err(VykarError::Other(format!(
                "REST GET_RANGE {key}: zero-length read requested"
            )));
        }
        let url = self.url(key);
        let range_header = crate::http_util::range_header("REST", key, offset, length)?;
        self.retry_call(
            &format!("GET_RANGE {key}"),
            || {
                self.authed(self.agent.get(&url))
                    .header("Range", &range_header)
                    .call()
            },
            |mut resp| {
                let status = resp.status().as_u16();
                if status == 404 {
                    return Ok(None);
                }
                if status >= 400 {
                    crate::retry::classify_status(
                        status,
                        format!("REST GET_RANGE {key}: HTTP {status}"),
                    )?;
                }
                if status == 200 {
                    return Err(HttpRetryError::Permanent(format!(
                        "REST GET_RANGE {key}: server returned 200 instead of 206 (Range header ignored)"
                    )));
                }
                if status != 206 {
                    return Err(HttpRetryError::Permanent(format!(
                        "REST GET_RANGE {key}: unexpected status {status}"
                    )));
                }

                // Validate Content-Range header
                let content_range = resp
                    .headers()
                    .get("Content-Range")
                    .ok_or_else(|| {
                        HttpRetryError::Permanent(format!(
                            "REST GET_RANGE {key}: server returned 206 without Content-Range header"
                        ))
                    })?
                    .to_str()
                    .map_err(|_| {
                        HttpRetryError::Permanent(format!(
                            "REST GET_RANGE {key}: non-ASCII Content-Range header"
                        ))
                    })?
                    .to_string();

                if let Err(e) =
                    Self::validate_content_range(&content_range, offset, length, key)
                {
                    return Err(HttpRetryError::Permanent(e.to_string()));
                }

                crate::http_util::read_range_body(&mut resp, "REST", key, offset, length)
                    .map(Some)
            },
        )
        .map_err(|e| VykarError::Other(format!("REST GET_RANGE {key}: {e}")))
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let key = key.trim_start_matches('/');
        let url = format!("{}?mkdir", self.url(key));
        self.retry_call(
            &format!("MKDIR {key}"),
            || self.authed(self.agent.post(&url)).send(&[] as &[u8]),
            |resp| {
                let status = resp.status().as_u16();
                if status >= 400 {
                    crate::retry::classify_status(
                        status,
                        format!("REST MKDIR {key}: HTTP {status}"),
                    )?;
                }
                Ok(())
            },
        )
        .map_err(|e| VykarError::Other(format!("REST MKDIR {key}: {e}")))?;
        Ok(())
    }

    fn remove_stale_temp_files(&self, _older_than: std::time::Duration) -> Result<usize> {
        // The server sweeps its own temp files.
        Ok(0)
    }

    fn server_repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        self.repack(plan)
    }

    fn batch_delete_keys(&self, keys: &[String]) -> Result<()> {
        // Chunk below the server's per-request key cap (200k). cleanup-dirs runs
        // only on the final chunk, so empty directories are pruned once, after
        // every key has been removed.
        const CHUNK: usize = 100_000;
        if keys.len() <= CHUNK {
            return self.batch_delete(keys, true);
        }
        let mut chunks = keys.chunks(CHUNK).peekable();
        while let Some(chunk) = chunks.next() {
            let is_last = chunks.peek().is_none();
            self.batch_delete(chunk, is_last)?;
        }
        Ok(())
    }

    fn server_verify_packs(&self, plan: &VerifyPacksPlanRequest) -> Result<VerifyPacksResponse> {
        self.verify_packs(plan)
    }

    fn bind_content_hash(&self, algo: HashAlgorithm) -> Result<()> {
        match self.content_hash.set(algo) {
            Ok(()) => Ok(()),
            // Already bound. The same value is a harmless re-bind (open then
            // reopen); a different one means two repositories are sharing one
            // backend, which would upload packs under the wrong header.
            Err(_) if self.content_hash.get() == Some(&algo) => Ok(()),
            Err(_) => Err(VykarError::Other(format!(
                "REST backend already bound to content hash {}, cannot rebind to {}",
                self.content_hash().as_str(),
                algo.as_str()
            ))),
        }
    }

    fn server_capabilities(&self) -> Result<ServerCapabilities> {
        let caps = self.fetch_server_capabilities()?;
        let _ = self.pack_parts.set(caps.pack_parts);
        Ok(caps)
    }

    fn object_sizes(&self, keys: &[String]) -> Result<Vec<Option<u64>>> {
        let unsupported = || VykarError::UnsupportedBackend("batch object sizes API".into());
        if self.sizes_unsupported.load(Ordering::Relaxed) {
            return Err(unsupported());
        }
        // One request per run of keys sharing a top-level directory (packs
        // are all below `packs/`), split at the server's per-request cap.
        let mut sizes = Vec::with_capacity(keys.len());
        let mut rest = keys;
        while let Some(first) = rest.first() {
            let dir = first.split_once('/').map(|(dir, _)| dir).ok_or_else(|| {
                VykarError::Other(format!(
                    "REST object_sizes: key {first:?} is not inside a directory"
                ))
            })?;
            let prefix = format!("{dir}/");
            let run = rest
                .iter()
                .take(vykar_protocol::MAX_OBJECT_SIZES_KEYS)
                .take_while(|k| k.starts_with(&prefix))
                .count();
            let (batch_keys, tail) = rest.split_at(run);
            let Some(batch) = self.post_sizes(dir, batch_keys)? else {
                self.sizes_unsupported.store(true, Ordering::Relaxed);
                return Err(unsupported());
            };
            sizes.extend(batch);
            rest = tail;
        }
        Ok(sizes)
    }

    fn server_init(&self) -> Result<()> {
        let url = format!("{}?init", self.base_url);
        self.retry_call(
            "INIT",
            || {
                let req = self.authed(self.agent.post(&url));
                Self::recv_response_within(req, self.timeouts.admin).send(&[] as &[u8])
            },
            |resp| {
                let status = resp.status().as_u16();
                if status >= 400 {
                    crate::retry::classify_status(status, format!("REST INIT: HTTP {status}"))?;
                }
                Ok(())
            },
        )
        .map_err(|e| VykarError::Other(format!("REST INIT: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RetryConfig;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};

    #[test]
    fn validate_content_range_accepts_valid_header() {
        RestBackend::validate_content_range("bytes 0-99/1000", 0, 100, "test").unwrap();
    }

    #[test]
    fn validate_content_range_rejects_mismatched_start() {
        let err = RestBackend::validate_content_range("bytes 10-109/1000", 0, 100, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Content-Range mismatch"), "got: {err}");
    }

    #[test]
    fn validate_content_range_rejects_mismatched_length() {
        let err = RestBackend::validate_content_range("bytes 0-49/1000", 0, 100, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Content-Range mismatch"), "got: {err}");
    }

    #[test]
    fn validate_content_range_rejects_end_less_than_start() {
        let err = RestBackend::validate_content_range("bytes 10-5/1000", 10, 100, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("overflow or end < start"), "got: {err}");
    }

    #[test]
    fn validate_content_range_rejects_u64_max_end() {
        let header = format!("bytes 0-{}/99999", u64::MAX);
        let err = RestBackend::validate_content_range(&header, 0, 100, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("overflow or end < start"), "got: {err}");
    }

    #[test]
    fn validate_content_range_rejects_missing_bytes_prefix() {
        let err = RestBackend::validate_content_range("0-99/1000", 0, 100, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("malformed Content-Range"), "got: {err}");
    }

    // ── Content-hash binding and capability probe ──────────────────────

    fn json_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    #[test]
    fn unbound_backend_defaults_to_blake2b() {
        let backend = RestBackend::new("http://127.0.0.1:1", None, no_retry(), None).unwrap();
        assert_eq!(backend.content_hash(), HashAlgorithm::Blake2b);
    }

    #[test]
    fn binding_the_same_hash_twice_is_a_no_op() {
        let backend = RestBackend::new("http://127.0.0.1:1", None, no_retry(), None).unwrap();
        backend.bind_content_hash(HashAlgorithm::Blake3).unwrap();
        backend.bind_content_hash(HashAlgorithm::Blake3).unwrap();
        assert_eq!(backend.content_hash(), HashAlgorithm::Blake3);
    }

    /// A conflicting rebind means two repositories are sharing one backend,
    /// which would upload packs under the wrong header. It must not be
    /// silently accepted, and must not silently switch the algorithm either.
    #[test]
    fn conflicting_rebind_is_rejected_and_keeps_the_first_binding() {
        let backend = RestBackend::new("http://127.0.0.1:1", None, no_retry(), None).unwrap();
        backend.bind_content_hash(HashAlgorithm::Blake2b).unwrap();
        let err = backend
            .bind_content_hash(HashAlgorithm::Blake3)
            .unwrap_err()
            .to_string();
        assert!(err.contains("already bound"), "got: {err}");
        assert_eq!(backend.content_hash(), HashAlgorithm::Blake2b);
    }

    /// A pre-BLAKE3 server answers /health without `hashes`. The client must
    /// read that as blake2b-only rather than failing to parse the response.
    #[test]
    fn legacy_health_response_advertises_blake2b_only() {
        let (url, handle) = mock_server(&json_response(r#"{"status":"ok","version":"0.19.1"}"#));
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();

        let caps = backend.server_capabilities().unwrap();
        assert_eq!(caps.version, "0.19.1");
        assert_eq!(caps.protocol_version, 0);
        assert!(caps.supports_hash(HashAlgorithm::Blake2b));
        assert!(
            !caps.supports_hash(HashAlgorithm::Blake3),
            "a server that advertises no hashes must not be assumed to do BLAKE3"
        );
        handle.join().unwrap();
    }

    #[test]
    fn new_health_response_advertises_blake3() {
        let (url, handle) = mock_server(&json_response(
            r#"{"status":"ok","version":"0.20.0","protocol_version":2,"hashes":["blake2b","blake3"]}"#,
        ));
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();

        let caps = backend.server_capabilities().unwrap();
        assert_eq!(caps.protocol_version, 2);
        assert!(caps.supports_hash(HashAlgorithm::Blake3));
        handle.join().unwrap();
    }

    /// A *newer* server may advertise an algorithm this binary has never heard
    /// of. That must not fail deserialization of the whole response.
    #[test]
    fn unknown_advertised_hash_does_not_break_parsing() {
        let (url, handle) = mock_server(&json_response(
            r#"{"status":"ok","version":"9.9.9","protocol_version":7,"hashes":["blake2b","blake3","future9"]}"#,
        ));
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();

        let caps = backend.server_capabilities().unwrap();
        assert!(caps.supports_hash(HashAlgorithm::Blake3));
        handle.join().unwrap();
    }

    /// Pack uploads carry the header matching the bound algorithm, and the
    /// value is the pack key's own hex — `put_bytes` never rehashes a pack.
    #[test]
    fn pack_upload_header_follows_the_bound_hash() {
        for (algo, expected_header) in [
            (HashAlgorithm::Blake2b, "x-content-blake2b"),
            (HashAlgorithm::Blake3, "x-content-blake3"),
        ] {
            let hex = "ab".to_string() + &"cd".repeat(31);
            let key = format!("packs/ab/{hex}");
            let (url, request_line) =
                capture_request("HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n");
            let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();
            backend.bind_content_hash(algo).unwrap();

            backend.put(&key, b"pack bytes").unwrap();

            let request = request_line.join().unwrap().to_ascii_lowercase();
            assert!(
                request.contains(&format!("{expected_header}: {hex}")),
                "{algo}: expected `{expected_header}: {hex}` in request:\n{request}"
            );
            let other = match algo {
                HashAlgorithm::Blake2b => "x-content-blake3",
                HashAlgorithm::Blake3 => "x-content-blake2b",
            };
            assert!(
                !request.contains(other),
                "{algo}: both digest headers were sent, which the server rejects:\n{request}"
            );
        }
    }

    /// Non-pack objects are hashed by the client and stay BLAKE2b regardless
    /// of the repository format.
    #[test]
    fn non_pack_upload_stays_blake2b_under_a_blake3_binding() {
        let (url, request_line) =
            capture_request("HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n");
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();
        backend.bind_content_hash(HashAlgorithm::Blake3).unwrap();

        let data = b"repository config bytes";
        backend.put("config", data).unwrap();

        let request = request_line.join().unwrap().to_ascii_lowercase();
        assert!(
            request.contains(&format!(
                "x-content-blake2b: {}",
                RestBackend::compute_blake2b_256_hex(data)
            )),
            "got:\n{request}"
        );
        assert!(!request.contains("x-content-blake3"), "got:\n{request}");
    }

    /// A 400 must carry the server's own message. Without this the old-server
    /// rejection reaches the user as a bare "HTTP 400".
    #[test]
    fn post_json_4xx_includes_the_server_message() {
        let body = "protocol version 2 not supported; server supports <= 1";
        let (url, handle) = mock_server(&format!(
            "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        ));
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();

        let err = backend
            .repack(&RepackPlanRequest::new(Vec::new(), HashAlgorithm::Blake3))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("protocol version 2 not supported"),
            "the server's message must reach the user, got: {err}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn list_requests_plain_listing() {
        let (url, request) = capture_request(&json_response(r#"["config"]"#));
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();
        assert_eq!(backend.list("").unwrap(), vec!["config".to_string()]);
        let request = request.join().unwrap();
        assert!(request.starts_with("GET /?list "), "got:\n{request}");
    }

    #[test]
    fn list_sized_parses_sized_and_plain_listings() {
        let (url, request) = capture_request(&json_response(r#"[{"key":"packs/ab/x","size":42}]"#));
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();
        assert_eq!(
            backend.list_sized("packs").unwrap(),
            vec![ListedObject {
                key: "packs/ab/x".into(),
                size: Some(42)
            }]
        );
        let request = request.join().unwrap();
        assert!(
            request.starts_with("GET /packs?list&sized "),
            "got:\n{request}"
        );

        // An old server ignores `sized` and returns bare keys.
        let (url, request) = capture_request(&json_response(r#"["config","index"]"#));
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();
        assert_eq!(
            backend.list_sized("").unwrap(),
            vec![
                ListedObject {
                    key: "config".into(),
                    size: None
                },
                ListedObject {
                    key: "index".into(),
                    size: None
                },
            ]
        );
        let request = request.join().unwrap();
        assert!(request.starts_with("GET /?list&sized "), "got:\n{request}");
    }

    #[test]
    fn object_sizes_posts_one_request_per_directory() {
        let (url, request) = capture_request(&json_response("[7,null]"));
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();
        let keys = vec!["packs/ab/x".to_string(), "packs/cd/y".to_string()];
        assert_eq!(backend.object_sizes(&keys).unwrap(), vec![Some(7), None]);
        let request = request.join().unwrap();
        assert!(request.starts_with("POST /packs?sizes "), "got:\n{request}");
    }

    /// An old server answers the unknown `?sizes` POST with 400. That is
    /// "unsupported" (the caller falls back), and it is remembered, so the
    /// probe costs one request per process.
    #[test]
    fn object_sizes_on_an_old_server_is_unsupported_and_remembered() {
        let (url, server) = mock_server("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();
        let keys = vec!["packs/ab/x".to_string()];
        for _ in 0..2 {
            let err = backend.object_sizes(&keys).unwrap_err();
            assert!(matches!(err, VykarError::UnsupportedBackend(_)), "{err}");
        }
        server.join().unwrap();
    }

    #[test]
    fn object_sizes_rejects_a_short_answer() {
        let (url, server) = mock_server(&json_response("[7]"));
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();
        let keys = vec!["packs/ab/x".to_string(), "packs/ab/y".to_string()];
        let err = backend.object_sizes(&keys).unwrap_err().to_string();
        assert!(err.contains("1 sizes for 2 keys"), "{err}");
        server.join().unwrap();
    }

    // ── Pack part uploads ──────────────────────────────────────────────

    /// One request as a [`routing_server`] saw it.
    #[derive(Debug, Clone)]
    struct Seen {
        /// `METHOD /path?query`
        line: String,
        head: String,
        body: Vec<u8>,
    }

    type Routes = dyn Fn(&str) -> String + Send + Sync;

    /// A keep-alive HTTP server that answers each request with `route(line)`
    /// (line = `METHOD /path?query`) on a thread per connection, recording
    /// every request.
    fn routing_server(
        route: impl Fn(&str) -> String + Send + Sync + 'static,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<Seen>>>) {
        use std::sync::{Arc, Mutex};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let route: Arc<Routes> = Arc::new(route);
        let log = seen.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let log = log.clone();
                let route = route.clone();
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    loop {
                        let mut head = String::new();
                        let mut content_length = 0usize;
                        loop {
                            let mut line = String::new();
                            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                                return;
                            }
                            if let Some(v) =
                                line.to_ascii_lowercase().strip_prefix("content-length:")
                            {
                                content_length = v.trim().parse().unwrap_or(0);
                            }
                            let done = line.trim().is_empty();
                            head.push_str(&line);
                            if done {
                                break;
                            }
                        }
                        let mut body = vec![0u8; content_length];
                        if reader.read_exact(&mut body).is_err() {
                            return;
                        }
                        let line = head
                            .lines()
                            .next()
                            .unwrap_or("")
                            .rsplit_once(' ')
                            .map_or("", |(l, _)| l)
                            .to_string();
                        let response = route(&line);
                        log.lock().unwrap().push(Seen { line, head, body });
                        if stream.write_all(response.as_bytes()).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (url, seen)
    }

    fn empty(status: &str) -> String {
        format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n")
    }

    const HEALTH_PARTS: &str = r#"{"status":"ok","version":"0.21.0","protocol_version":2,"hashes":["blake2b","blake3"],"pack_parts":true}"#;
    const HEALTH_OLD: &str =
        r#"{"status":"ok","version":"0.20.0","protocol_version":2,"hashes":["blake2b","blake3"]}"#;

    /// Route: `/health` answers `health`; part PUTs and completions get
    /// `part` / `complete`; whole PUTs get `whole`.
    fn pack_routes(
        health: &'static str,
        part: &'static str,
        complete: &'static str,
        whole: &'static str,
    ) -> impl Fn(&str) -> String + Send + Sync + 'static {
        move |line| {
            if line.starts_with("GET /health") {
                json_response(health)
            } else if line.starts_with("PUT ") && line.contains("upload=") {
                empty(part)
            } else if line.starts_with("POST ") && line.contains("?complete") {
                empty(complete)
            } else {
                empty(whole)
            }
        }
    }

    fn test_pack(len: usize) -> (String, Vec<u8>) {
        let data: Vec<u8> = (0..len).map(|i| (i % 253) as u8).collect();
        let hex = vykar_types::pack_id::PackId::compute(&data, HashAlgorithm::Blake3).to_hex();
        (format!("packs/{}/{hex}", &hex[..2]), data)
    }

    fn parts_backend(url: &str, retry: RetryConfig) -> RestBackend {
        let mut backend = RestBackend::new(url, None, retry, None).unwrap();
        backend.part_size = 1000;
        backend.bind_content_hash(HashAlgorithm::Blake3).unwrap();
        backend
    }

    fn count(seen: &[Seen], pred: impl Fn(&Seen) -> bool) -> usize {
        seen.iter().filter(|s| pred(s)).count()
    }
    fn is_part(s: &Seen) -> bool {
        s.line.starts_with("PUT ") && s.line.contains("upload=")
    }
    fn is_whole(s: &Seen) -> bool {
        s.line.starts_with("PUT ") && !s.line.contains('?')
    }

    #[test]
    fn pack_goes_up_in_parts_when_the_server_advertises_them() {
        let (url, seen) = routing_server(pack_routes(
            HEALTH_PARTS,
            "204 No Content",
            "201 Created",
            "500 Internal Server Error",
        ));
        let backend = parts_backend(&url, no_retry());
        let (key, data) = test_pack(3500);
        backend.put(&key, &data).unwrap();

        let seen = seen.lock().unwrap().clone();
        assert_eq!(count(&seen, |s| s.line.starts_with("GET /health")), 1);
        assert_eq!(count(&seen, is_whole), 0, "{seen:#?}");
        // 4 near-equal parts, each body the matching slice of the pack.
        let mut parts: Vec<(usize, Vec<u8>)> = seen
            .iter()
            .filter(|s| is_part(s))
            .map(|s| {
                assert!(s.line.contains("&total=3500"), "{}", s.line);
                let offset = s.line.split("offset=").nth(1).unwrap();
                let offset: usize = offset.split('&').next().unwrap().parse().unwrap();
                (offset, s.body.clone())
            })
            .collect();
        parts.sort();
        assert_eq!(parts.len(), 4);
        let mut assembled = Vec::new();
        for (offset, body) in &parts {
            assert_eq!(*offset, assembled.len());
            assembled.extend_from_slice(body);
        }
        assert_eq!(assembled, data);
        // One token for the whole upload, and the completion carries the
        // pack's digest under the repository's header.
        let token = |line: &str| line.split("upload=").nth(1).unwrap()[..32].to_string();
        let tokens: std::collections::HashSet<String> = seen
            .iter()
            .filter(|s| s.line.contains("upload="))
            .map(|s| token(&s.line))
            .collect();
        assert_eq!(tokens.len(), 1);
        let completion = seen.iter().find(|s| s.line.contains("?complete")).unwrap();
        let hex = key.rsplit('/').next().unwrap();
        assert!(
            completion
                .head
                .to_ascii_lowercase()
                .contains(&format!("x-content-blake3: {hex}")),
            "{}",
            completion.head
        );
        assert_eq!(
            seen.last().unwrap().line,
            completion.line,
            "completion comes after every part"
        );
    }

    #[test]
    fn old_server_gets_whole_puts_and_is_probed_once() {
        let (url, seen) = routing_server(pack_routes(
            HEALTH_OLD,
            "500 Internal Server Error",
            "500 Internal Server Error",
            "201 Created",
        ));
        let backend = parts_backend(&url, no_retry());
        for len in [3000, 4000] {
            let (key, data) = test_pack(len);
            backend.put(&key, &data).unwrap();
        }
        let seen = seen.lock().unwrap().clone();
        assert_eq!(count(&seen, |s| s.line.starts_with("GET /health")), 1);
        assert_eq!(count(&seen, is_whole), 2);
        assert_eq!(count(&seen, |s| s.line.contains("upload=")), 0);
    }

    /// Small packs and non-pack objects never trigger the probe.
    #[test]
    fn small_objects_skip_the_probe() {
        let (url, seen) = routing_server(pack_routes(
            HEALTH_PARTS,
            "204 No Content",
            "201 Created",
            "201 Created",
        ));
        let backend = parts_backend(&url, no_retry());
        let (key, data) = test_pack(1000);
        backend.put(&key, &data).unwrap();
        backend.put("index", &[0u8; 5000]).unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2, "{seen:#?}");
        assert_eq!(count(&seen, is_whole), 2);
    }

    #[test]
    fn explicit_capability_call_is_reused() {
        let (url, seen) = routing_server(pack_routes(
            HEALTH_PARTS,
            "204 No Content",
            "201 Created",
            "201 Created",
        ));
        let backend = parts_backend(&url, no_retry());
        assert!(backend.server_capabilities().unwrap().pack_parts);
        let (key, data) = test_pack(3000);
        backend.put(&key, &data).unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(count(&seen, |s| s.line.starts_with("GET /health")), 1);
        assert_eq!(count(&seen, is_part), 3);
    }

    /// A refused part (4xx) falls back to a whole PUT and turns the part
    /// path off for the rest of the backend's life.
    #[test]
    fn refused_part_falls_back_and_disables_parts() {
        let (url, seen) = routing_server(pack_routes(
            HEALTH_PARTS,
            "400 Bad Request",
            "201 Created",
            "201 Created",
        ));
        let backend = parts_backend(&url, no_retry());
        let (key, data) = test_pack(3000);
        backend.put(&key, &data).unwrap();
        let (key2, data2) = test_pack(4000);
        backend.put(&key2, &data2).unwrap();

        let seen = seen.lock().unwrap().clone();
        assert_eq!(count(&seen, is_whole), 2);
        assert_eq!(count(&seen, |s| s.line.contains("?complete")), 0);
        let second_pack_parts = count(&seen, |s| is_part(s) && s.line.contains(&key2));
        assert_eq!(second_pack_parts, 0, "parts disabled after a refusal");
    }

    /// A transient failure (5xx after retries) falls back for that pack only.
    #[test]
    fn transient_part_failure_falls_back_for_that_pack_only() {
        let (url, seen) = routing_server(pack_routes(
            HEALTH_PARTS,
            "503 Service Unavailable",
            "201 Created",
            "201 Created",
        ));
        let backend = parts_backend(&url, no_retry());
        let (key, data) = test_pack(3000);
        backend.put(&key, &data).unwrap();
        let (key2, data2) = test_pack(4000);
        backend.put(&key2, &data2).unwrap();

        let seen = seen.lock().unwrap().clone();
        assert_eq!(count(&seen, is_whole), 2);
        assert!(count(&seen, |s| is_part(s) && s.line.contains(&key2)) > 0);
    }

    #[test]
    fn failed_completion_falls_back_to_a_whole_put() {
        let (url, seen) = routing_server(pack_routes(
            HEALTH_PARTS,
            "204 No Content",
            "404 Not Found",
            "201 Created",
        ));
        let backend = parts_backend(&url, no_retry());
        let (key, data) = test_pack(3000);
        backend.put(&key, &data).unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(count(&seen, is_part), 3);
        assert_eq!(count(&seen, is_whole), 1);
        assert!(is_whole(seen.last().unwrap()));
    }

    /// `put_owned` keeps the whole-PUT fallback through the completion until
    /// one completion has succeeded on the backend.
    #[test]
    fn owned_put_keeps_the_fallback_until_a_completion_succeeded() {
        let (url, seen) = routing_server(pack_routes(
            HEALTH_PARTS,
            "204 No Content",
            "404 Not Found",
            "201 Created",
        ));
        let backend = parts_backend(&url, no_retry());
        let (key, data) = test_pack(3000);
        backend.put_owned(&key, data).unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(count(&seen, is_part), 3);
        assert!(is_whole(seen.last().unwrap()), "{seen:#?}");
    }

    /// Once completions are known to work, `put_owned` releases the buffer
    /// before completing, so a failed completion is the upload's error and
    /// is never answered with a whole PUT.
    #[test]
    fn owned_put_after_a_completion_surfaces_a_failed_completion() {
        let complete_ok = std::sync::Arc::new(AtomicBool::new(true));
        let flag = std::sync::Arc::clone(&complete_ok);
        let (url, seen) = routing_server(move |line: &str| {
            if line.starts_with("GET /health") {
                json_response(HEALTH_PARTS)
            } else if line.starts_with("PUT ") && line.contains("upload=") {
                empty("204 No Content")
            } else if line.starts_with("POST ") && line.contains("?complete") {
                if flag.load(Ordering::Relaxed) {
                    empty("201 Created")
                } else {
                    empty("404 Not Found")
                }
            } else {
                empty("201 Created")
            }
        });
        let backend = parts_backend(&url, no_retry());
        let (key, data) = test_pack(3000);
        backend.put_owned(&key, data).unwrap();

        let (key2, data2) = test_pack(3500);
        backend.put_owned(&key2, data2).unwrap();
        complete_ok.store(false, Ordering::Relaxed);
        let (key3, data3) = test_pack(4000);
        let err = backend.put_owned(&key3, data3).unwrap_err().to_string();
        assert!(
            err.contains("404") && err.contains("released"),
            "got: {err}"
        );

        let seen = seen.lock().unwrap().clone();
        assert_eq!(count(&seen, is_whole), 0, "{seen:#?}");
        assert_eq!(count(&seen, |s| s.line.contains("?complete")), 3);
        assert_eq!(count(&seen, |s| is_part(s) && s.line.contains(&key3)), 4);
    }

    /// A part failure still falls back to a whole PUT from `put_owned`,
    /// with the buffer still held.
    #[test]
    fn owned_put_part_failure_falls_back() {
        let (url, seen) = routing_server(pack_routes(
            HEALTH_PARTS,
            "503 Service Unavailable",
            "201 Created",
            "201 Created",
        ));
        let backend = parts_backend(&url, no_retry());
        backend.parts_proven.store(true, Ordering::Relaxed);
        let (key, data) = test_pack(3500);
        backend.put_owned(&key, data).unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(count(&seen, is_whole), 1, "{seen:#?}");
        assert_eq!(count(&seen, |s| s.line.contains("?complete")), 0);
    }

    /// When the fallback PUT fails too, its error is what the caller sees.
    #[test]
    fn fallback_failure_is_surfaced() {
        let (url, _seen) = routing_server(pack_routes(
            HEALTH_PARTS,
            "413 Payload Too Large",
            "201 Created",
            "413 Payload Too Large",
        ));
        let backend = parts_backend(&url, no_retry());
        let (key, data) = test_pack(3000);
        let err = backend.put(&key, &data).unwrap_err().to_string();
        assert!(err.contains("413"), "got: {err}");
    }

    /// A failed probe is "not supported", never an upload error.
    #[test]
    fn failed_probe_means_whole_puts() {
        let (url, seen) = routing_server(|line: &str| {
            if line.starts_with("GET /health") {
                empty("500 Internal Server Error")
            } else {
                empty("201 Created")
            }
        });
        let backend = parts_backend(&url, no_retry());
        let (key, data) = test_pack(3000);
        backend.put(&key, &data).unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(count(&seen, is_whole), 1);
        assert_eq!(count(&seen, |s| s.line.contains("upload=")), 0);
    }

    /// Serve one canned response and hand back the full request text.
    fn capture_request(response: &str) -> (String, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");
        let response = response.to_string();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let request = read_request(&mut reader);
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
            request
        });
        (url, handle)
    }

    /// Read the request head and drain the body by Content-Length, returning
    /// the raw head. Draining matters: closing a socket with unread inbound
    /// bytes sends RST, and Windows then discards any response the client has
    /// not read yet (os errors 10053/10054).
    fn read_request(reader: &mut BufReader<TcpStream>) -> String {
        let mut request = String::new();
        let mut content_length = 0usize;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                content_length = v.trim().parse().unwrap_or(0);
            }
            let done = line.trim().is_empty();
            request.push_str(&line);
            if done {
                break;
            }
        }
        let mut body = vec![0u8; content_length];
        let _ = reader.read_exact(&mut body);
        request
    }

    /// Spin up a TCP listener that responds with a canned HTTP response to
    /// the first request, then return the listener's URL and a join handle.
    fn mock_server(response: &str) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");
        let response = response.to_string();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            read_request(&mut reader);
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
        });
        (url, handle)
    }

    /// Spin up a TCP listener that serves multiple sequential requests.
    /// Each entry in `responses` is served to one request in order.
    fn mock_server_multi(responses: Vec<String>) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");
        let handle = std::thread::spawn(move || {
            for response in &responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                read_request(&mut reader);
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
                // Drop stream to close connection (important for truncation tests)
                drop(stream);
            }
        });
        (url, handle)
    }

    /// A server that accepts connections and drains requests but never
    /// responds. Sockets stay open until the returned stop flag is set; the
    /// join handle yields the number of accepted connections.
    fn stalled_server() -> (
        String,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        std::thread::JoinHandle<usize>,
    ) {
        use std::sync::atomic::{AtomicBool, Ordering};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let handle = std::thread::spawn(move || {
            let mut held = Vec::new();
            while !stop_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        let mut reader = BufReader::new(stream.try_clone().unwrap());
                        read_request(&mut reader);
                        held.push(stream);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => panic!("accept failed: {e}"),
                }
            }
            held.len()
        });
        (url, stop, handle)
    }

    fn short_timeouts() -> HttpTimeouts {
        let t = Duration::from_millis(200);
        HttpTimeouts {
            resolve: t,
            send_request: t,
            recv_response: t,
            admin: t,
            repack: t,
        }
    }

    #[test]
    fn recv_response_timeout_unblocks_stalled_server() {
        let (url, stop, handle) = stalled_server();
        let backend =
            RestBackend::with_timeouts(&url, None, no_retry(), None, short_timeouts()).unwrap();

        let start = std::time::Instant::now();
        let result = backend.get("testkey");
        let elapsed = start.elapsed();

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        handle.join().unwrap();
        assert!(result.is_err(), "stalled GET must fail, got {result:?}");
        assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");
    }

    #[test]
    fn repack_is_not_retried_on_timeout() {
        let (url, stop, handle) = stalled_server();
        let backend =
            RestBackend::with_timeouts(&url, None, fast_retry(), None, short_timeouts()).unwrap();

        let plan = RepackPlanRequest::new(vec![], HashAlgorithm::Blake3);
        let result = backend.repack(&plan);

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let accepted = handle.join().unwrap();
        assert!(result.is_err(), "stalled repack must fail");
        assert_eq!(accepted, 1, "repack must not be retried");
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

    #[test]
    fn range_request_rejects_200_ok() {
        let body = "full object content";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let (url, handle) = mock_server(&resp);
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();

        let err = backend
            .get_range("testkey", 10, 50)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("200 instead of 206"),
            "expected 200-rejection error, got: {err}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn range_request_rejects_missing_content_range() {
        let body = [0u8; 50];
        let resp = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let (url, handle) = mock_server(&resp);
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();

        let err = backend
            .get_range("testkey", 10, 50)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("without Content-Range header"),
            "expected missing Content-Range error, got: {err}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn extract_pack_hex_returns_hex_for_pack_key() {
        let hex = "ab".to_string() + &"cd".repeat(31);
        let key = format!("packs/ab/{hex}");
        assert_eq!(RestBackend::try_extract_pack_hex(&key), Some(hex.as_str()));
    }

    #[test]
    fn extract_pack_hex_returns_none_for_non_pack_keys() {
        assert_eq!(RestBackend::try_extract_pack_hex("manifest"), None);
        assert_eq!(RestBackend::try_extract_pack_hex("index"), None);
        assert_eq!(RestBackend::try_extract_pack_hex("snapshots/abc123"), None);
        assert_eq!(RestBackend::try_extract_pack_hex("config"), None);
    }

    #[test]
    fn extract_pack_hex_returns_none_for_short_hex() {
        assert_eq!(RestBackend::try_extract_pack_hex("packs/ab/tooshort"), None);
    }

    #[test]
    fn compute_blake2b_matches_pack_id() {
        use vykar_types::hash::HashAlgorithm;
        use vykar_types::pack_id::PackId;
        let data = b"hello world test data for blake2b verification";
        let pack_id = PackId::compute(data, HashAlgorithm::Blake2b);
        let computed = RestBackend::compute_blake2b_256_hex(data);
        assert_eq!(computed, pack_id.to_hex());
    }

    #[test]
    fn range_request_rejects_mismatched_content_range() {
        let body = [0u8; 50];
        // Content-Range says bytes 0-49 but we requested offset=10
        let resp = format!(
            "HTTP/1.1 206 Partial Content\r\n\
             Content-Range: bytes 0-49/1000\r\n\
             Content-Length: {}\r\n\r\n",
            body.len()
        );
        let (url, handle) = mock_server(&resp);
        let backend = RestBackend::new(&url, None, no_retry(), None).unwrap();

        let err = backend
            .get_range("testkey", 10, 50)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Content-Range mismatch"),
            "expected Content-Range mismatch error, got: {err}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn get_retries_on_truncated_body() {
        let full_body = b"hello world, this is the full response body";
        // First response: declare Content-Length but send truncated data, then close
        let truncated_resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\ntruncated",
            full_body.len()
        );
        // Second response: complete
        let complete_resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {len}\r\n\r\n{body}",
            len = full_body.len(),
            body = std::str::from_utf8(full_body).unwrap(),
        );

        let (url, handle) = mock_server_multi(vec![truncated_resp, complete_resp]);
        let backend = RestBackend::new(&url, None, fast_retry(), None).unwrap();

        let result = backend.get("testkey").unwrap().unwrap();
        assert_eq!(result, full_body);
        handle.join().unwrap();
    }

    #[test]
    fn get_range_retries_on_truncated_body() {
        // Use a custom mock that sends raw bytes for binary body
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");
        let handle = std::thread::spawn(move || {
            // Request 1: truncated body (declare 50 bytes, send 5, close)
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
                     Content-Range: bytes 10-59/1000\r\n\
                     Content-Length: 50\r\n\r\n";
                stream.write_all(headers.as_bytes()).unwrap();
                stream.write_all(&[0xABu8; 5]).unwrap(); // only 5 of 50 bytes
                stream.flush().unwrap();
                drop(stream); // close → triggers I/O error on client
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
                     Content-Range: bytes 10-59/1000\r\n\
                     Content-Length: 50\r\n\r\n";
                stream.write_all(headers.as_bytes()).unwrap();
                stream.write_all(&[0xABu8; 50]).unwrap(); // full 50 bytes
                stream.flush().unwrap();
            }
        });

        let backend = RestBackend::new(&url, None, fast_retry(), None).unwrap();
        let result = backend.get_range("testkey", 10, 50).unwrap().unwrap();
        assert_eq!(result.len(), 50);
        assert!(result.iter().all(|&b| b == 0xAB));
        handle.join().unwrap();
    }

    #[test]
    fn get_range_permanent_errors_not_retried() {
        // 200-instead-of-206 is permanent — should fail immediately even with retries enabled
        let body = "full object content";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        // Only provide one response — if it retries it will hang/fail
        let (url, handle) = mock_server(&resp);
        let backend = RestBackend::new(&url, None, fast_retry(), None).unwrap();

        let err = backend
            .get_range("testkey", 10, 50)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("200 instead of 206"),
            "expected permanent error, got: {err}"
        );
        handle.join().unwrap();
    }
}
