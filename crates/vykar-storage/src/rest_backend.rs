#![allow(
    clippy::duration_suboptimal_units,
    clippy::map_unwrap_or,
    clippy::manual_let_else,
    clippy::missing_errors_doc,
    clippy::redundant_closure_for_method_calls,
    clippy::used_underscore_binding
)]

use std::io::Read;
use std::sync::OnceLock;
use std::time::Duration;

use blake2::{Blake2b256, Digest};

use crate::retry::HttpRetryError;
use crate::RetryConfig;
use vykar_types::error::{Result, VykarError};

use crate::{
    RepackPlanRequest, RepackResultResponse, ServerCapabilities, StorageBackend,
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
}

impl RestBackend {
    pub fn new(
        base_url: &str,
        token: Option<&str>,
        retry: RetryConfig,
        max_connections: Option<usize>,
    ) -> Result<Self> {
        let pool = crate::http_idle_pool_size(max_connections);
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_connect(Some(Duration::from_secs(30)))
            .timeout_send_body(Some(Duration::from_secs(5 * 60)))
            .timeout_recv_body(Some(Duration::from_secs(5 * 60)))
            .max_idle_connections_per_host(pool)
            .max_idle_connections(pool)
            .max_idle_age(crate::HTTP_IDLE_AGE)
            .build()
            .into();

        let base = base_url.trim_end_matches('/').to_string();

        Ok(Self {
            base_url: base,
            agent,
            bearer: token.map(|t| format!("Bearer {t}")),
            retry,
            content_hash: OnceLock::new(),
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

    /// Unified retry wrapper for HTTP calls with response handling.
    fn retry_call<T>(
        &self,
        op_name: &str,
        f: impl Fn() -> std::result::Result<http::Response<ureq::Body>, ureq::Error>,
        handle_response: impl Fn(http::Response<ureq::Body>) -> std::result::Result<T, HttpRetryError>,
    ) -> std::result::Result<T, HttpRetryError> {
        crate::retry::retry_http(&self.retry, op_name, "REST", f, handle_response)
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
    ) -> Result<Resp> {
        let body = self
            .retry_call(
                op_name,
                || self.authed(self.agent.post(url)).send_json(payload),
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
    ) -> Result<()> {
        self.retry_call(
            op_name,
            || self.authed(self.agent.post(url)).send_json(payload),
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
        self.post_no_content("batch-delete", &url, &keys)
    }

    /// Send a verify-packs plan to the server for server-side pack verification.
    pub fn verify_packs(&self, plan: &VerifyPacksPlanRequest) -> Result<VerifyPacksResponse> {
        let url = format!("{}?verify-packs", self.base_url);
        self.post_json("verify-packs", &url, plan)
    }

    /// Send a repack plan to the server for server-side compaction.
    pub fn repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        let url = format!("{}?repack", self.base_url);
        self.post_json("repack", &url, plan)
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

    /// The content-digest algorithm this backend was bound to, defaulting to
    /// BLAKE2b when nothing bound it.
    fn content_hash(&self) -> HashAlgorithm {
        self.content_hash.get().copied().unwrap_or_default()
    }

    /// Shared PUT implementation for both borrowed and owned data.
    ///
    /// Note what this does for pack keys: it never hashes them. A pack key
    /// *is* the pack ID, so the hex is lifted straight out of the key — which
    /// means that for packs only the header *name* changes with the
    /// algorithm, because the value is already the right digest. Non-pack
    /// objects are hashed here and stay BLAKE2b unconditionally.
    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        let url = self.url(key);
        let (header, checksum) = match Self::try_extract_pack_hex(key) {
            Some(pack_hex) => (
                match self.content_hash() {
                    HashAlgorithm::Blake2b => "X-Content-BLAKE2b",
                    HashAlgorithm::Blake3 => "X-Content-BLAKE3",
                },
                pack_hex.to_string(),
            ),
            None => ("X-Content-BLAKE2b", Self::compute_blake2b_256_hex(data)),
        };
        self.retry_call(
            &format!("PUT {key}"),
            || {
                self.authed(self.agent.put(&url))
                    .header(header, &checksum)
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
        let prefix = prefix.trim_start_matches('/');
        let url = if prefix.is_empty() {
            format!("{}?list", self.base_url)
        } else {
            format!("{}?list", self.url(prefix))
        };
        let body = self
            .retry_call(
                &format!("LIST {prefix}"),
                || self.authed(self.agent.get(&url)).call(),
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
            .map_err(|e| VykarError::Other(format!("REST LIST {prefix}: {e}")))?;
        let keys: Vec<String> = serde_json::from_slice(&body)
            .map_err(|e| VykarError::Other(format!("REST LIST parse: {e}")))?;
        Ok(keys)
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
        self.fetch_server_capabilities()
    }

    fn server_init(&self) -> Result<()> {
        let url = format!("{}?init", self.base_url);
        self.retry_call(
            "INIT",
            || self.authed(self.agent.post(&url)).send(&[] as &[u8]),
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
