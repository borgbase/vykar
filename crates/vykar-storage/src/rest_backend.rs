#![allow(
    clippy::duration_suboptimal_units,
    clippy::map_unwrap_or,
    clippy::manual_let_else,
    clippy::missing_errors_doc,
    clippy::redundant_closure_for_method_calls,
    clippy::used_underscore_binding
)]

use std::io::Read;
use std::time::Duration;

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use crate::retry::HttpRetryError;
use crate::RetryConfig;
use vykar_types::error::{Result, VykarError};

use crate::{
    RepackPlanRequest, RepackResultResponse, StorageBackend, VerifyPacksPlanRequest,
    VerifyPacksResponse,
};

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
                    crate::retry::classify_status(
                        status,
                        format!("REST {op_name} failed: HTTP {status}"),
                    )?;
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
        let mut hasher = Blake2bVar::new(32).expect("valid output size");
        hasher.update(data);
        let mut out = [0u8; 32];
        hasher.finalize_variable(&mut out).expect("correct length");
        hex::encode(out)
    }

    /// Shared PUT implementation for both borrowed and owned data.
    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        let url = self.url(key);
        let checksum = Self::try_extract_pack_hex(key)
            .map_or_else(|| Self::compute_blake2b_256_hex(data), str::to_string);
        self.retry_call(
            &format!("PUT {key}"),
            || {
                self.authed(self.agent.put(&url))
                    .header("X-Content-BLAKE2b", &checksum)
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
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

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

    /// Spin up a TCP listener that responds with a canned HTTP response to
    /// the first request, then return the listener's URL and a join handle.
    fn mock_server(response: &str) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");
        let response = response.to_string();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            // Consume the request
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line.trim().is_empty() {
                    break;
                }
            }
            // Send the canned response
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
                let mut line = String::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                }
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
        use vykar_types::pack_id::PackId;
        let data = b"hello world test data for blake2b verification";
        let pack_id = PackId::compute(data);
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
