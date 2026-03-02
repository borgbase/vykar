use std::io::Read;
use std::time::Duration;

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use crate::RetryConfig;
use vykar_types::error::{Result, VykarError};

use crate::{
    BackendLockInfo, RepackPlanRequest, RepackResultResponse, StorageBackend,
    VerifyPacksPlanRequest, VerifyPacksResponse,
};

/// HTTP REST backend for remote repository access via vykar-server.
pub struct RestBackend {
    /// Base URL, e.g. "https://backup.example.com"
    base_url: String,
    agent: ureq::Agent,
    token: Option<String>,
    retry: RetryConfig,
}

impl RestBackend {
    pub fn new(base_url: &str, token: Option<&str>, retry: RetryConfig) -> Result<Self> {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(30))
            .timeout_read(Duration::from_secs(300))
            .timeout_write(Duration::from_secs(300))
            .build();

        let base = base_url.trim_end_matches('/').to_string();

        Ok(Self {
            base_url: base,
            agent,
            token: token.map(|t| t.to_string()),
            retry,
        })
    }

    fn url(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        format!("{}/{}", self.base_url, key)
    }

    fn apply_auth(&self, req: ureq::Request) -> ureq::Request {
        if let Some(ref token) = self.token {
            req.set("Authorization", &format!("Bearer {token}"))
        } else {
            req
        }
    }

    /// Retry a closure on transient errors with exponential backoff + jitter.
    #[allow(clippy::result_large_err)]
    fn retry_call<T>(
        &self,
        op_name: &str,
        f: impl Fn() -> std::result::Result<T, ureq::Error>,
    ) -> std::result::Result<T, ureq::Error> {
        crate::retry::retry_http(&self.retry, op_name, "REST", f)
    }

    /// Retry a closure that performs both HTTP request and body read.
    fn retry_call_body<T>(
        &self,
        op_name: &str,
        f: impl Fn() -> std::result::Result<T, crate::retry::HttpRetryError>,
    ) -> std::result::Result<T, crate::retry::HttpRetryError> {
        crate::retry::retry_http_body(&self.retry, op_name, "REST", f)
    }

    /// Batch delete multiple keys in a single request.
    pub fn batch_delete(&self, keys: &[String], cleanup_dirs: bool) -> Result<()> {
        let url = if cleanup_dirs {
            format!("{}?batch-delete&cleanup-dirs", self.base_url)
        } else {
            format!("{}?batch-delete", self.base_url)
        };
        let payload = keys.to_vec();
        let resp = self
            .retry_call("batch-delete", || {
                let req = self.apply_auth(self.agent.post(&url));
                req.send_json(payload.clone())
            })
            .map_err(|e| VykarError::Other(format!("REST batch-delete: {e}")))?;
        if resp.status() >= 400 {
            return Err(VykarError::Other(format!(
                "REST batch-delete failed: HTTP {}",
                resp.status()
            )));
        }
        Ok(())
    }

    /// Get repository statistics from the server.
    pub fn stats(&self) -> Result<serde_json::Value> {
        let url = format!("{}?stats", self.base_url);
        let body = self
            .retry_call_body("stats", || {
                let req = self.apply_auth(self.agent.get(&url));
                let resp = req.call().map_err(crate::retry::HttpRetryError::http)?;
                let mut buf = Vec::new();
                resp.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(crate::retry::HttpRetryError::BodyIo)?;
                Ok(buf)
            })
            .map_err(|e| VykarError::Other(format!("REST stats: {e}")))?;
        let val: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| VykarError::Other(format!("REST stats parse: {e}")))?;
        Ok(val)
    }

    /// Acquire a lock on the server.
    pub fn acquire_lock(&self, id: &str, info: &BackendLockInfo) -> Result<()> {
        let url = format!("{}/locks/{}", self.base_url, id);
        let info = info.clone();
        match self.retry_call("lock-acquire", || {
            let req = self.apply_auth(self.agent.post(&url));
            req.send_json(info.clone())
        }) {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(409, _)) => Err(VykarError::Locked(id.to_string())),
            Err(e) => Err(VykarError::Other(format!("REST lock acquire: {e}"))),
        }
    }

    /// Release a lock on the server.
    pub fn release_lock(&self, id: &str) -> Result<()> {
        let url = format!("{}/locks/{}", self.base_url, id);
        match self.retry_call("lock-release", || {
            let req = self.apply_auth(self.agent.delete(&url));
            req.call()
        }) {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(404, _)) => Ok(()),
            Err(e) => Err(VykarError::Other(format!("REST lock release: {e}"))),
        }
    }

    /// Send a verify-packs plan to the server for server-side pack verification.
    pub fn verify_packs(&self, plan: &VerifyPacksPlanRequest) -> Result<VerifyPacksResponse> {
        let url = format!("{}?verify-packs", self.base_url);
        let plan = plan.clone();
        let resp = self
            .retry_call("verify-packs", || {
                let req = self.apply_auth(self.agent.post(&url));
                req.send_json(plan.clone())
            })
            .map_err(|e| VykarError::Other(format!("REST verify-packs: {e}")))?;
        if resp.status() >= 400 {
            return Err(VykarError::Other(format!(
                "REST verify-packs failed: HTTP {}",
                resp.status()
            )));
        }
        let val: VerifyPacksResponse = resp
            .into_json()
            .map_err(|e| VykarError::Other(format!("REST verify-packs parse: {e}")))?;
        Ok(val)
    }

    /// Send a repack plan to the server for server-side compaction.
    pub fn repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        let url = format!("{}?repack", self.base_url);
        let plan = plan.clone();
        let resp = self
            .retry_call("repack", || {
                let req = self.apply_auth(self.agent.post(&url));
                req.send_json(plan.clone())
            })
            .map_err(|e| VykarError::Other(format!("REST repack: {e}")))?;
        if resp.status() >= 400 {
            return Err(VykarError::Other(format!(
                "REST repack failed: HTTP {}",
                resp.status()
            )));
        }
        let val: RepackResultResponse = resp
            .into_json()
            .map_err(|e| VykarError::Other(format!("REST repack parse: {e}")))?;
        Ok(val)
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
            .map(|h| h.to_string())
            .unwrap_or_else(|| Self::compute_blake2b_256_hex(data));
        self.retry_call(&format!("PUT {key}"), || {
            let req = self
                .apply_auth(self.agent.put(&url))
                .set("X-Content-BLAKE2b", &checksum);
            req.send_bytes(data)
        })
        .map_err(|e| VykarError::Other(format!("REST PUT {key}: {e}")))?;
        Ok(())
    }
}

impl StorageBackend for RestBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        use crate::retry::HttpRetryError;
        let url = self.url(key);
        self.retry_call_body(&format!("GET {key}"), || {
            let req = self.apply_auth(self.agent.get(&url));
            match req.call() {
                Ok(resp) => {
                    let mut buf = Vec::new();
                    resp.into_reader()
                        .read_to_end(&mut buf)
                        .map_err(HttpRetryError::BodyIo)?;
                    Ok(Some(buf))
                }
                Err(ureq::Error::Status(404, _)) => Ok(None),
                Err(e) => Err(HttpRetryError::http(e)),
            }
        })
        .map_err(|e| VykarError::Other(format!("REST GET {key}: {e}")))
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.put_bytes(key, data)
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.put_bytes(key, &data)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let url = self.url(key);
        match self.retry_call(&format!("DELETE {key}"), || {
            let req = self.apply_auth(self.agent.delete(&url));
            req.call()
        }) {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(404, _)) => Ok(()),
            Err(e) => Err(VykarError::Other(format!("REST DELETE {key}: {e}"))),
        }
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let url = self.url(key);
        match self.retry_call(&format!("HEAD {key}"), || {
            let req = self.apply_auth(self.agent.head(&url));
            req.call()
        }) {
            Ok(_) => Ok(true),
            Err(ureq::Error::Status(404, _)) => Ok(false),
            Err(e) => Err(VykarError::Other(format!("REST HEAD {key}: {e}"))),
        }
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        let url = self.url(key);
        match self.retry_call(&format!("HEAD {key}"), || {
            let req = self.apply_auth(self.agent.head(&url));
            req.call()
        }) {
            Ok(resp) => {
                let len =
                    crate::http_util::extract_content_length(&resp, &format!("REST HEAD {key}"))?;
                Ok(Some(len))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(e) => Err(VykarError::Other(format!("REST HEAD {key}: {e}"))),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        use crate::retry::HttpRetryError;
        let prefix = prefix.trim_start_matches('/');
        let url = if prefix.is_empty() {
            format!("{}?list", self.base_url)
        } else {
            format!("{}?list", self.url(prefix))
        };
        let body = self
            .retry_call_body(&format!("LIST {prefix}"), || {
                let req = self.apply_auth(self.agent.get(&url));
                let resp = req.call().map_err(HttpRetryError::http)?;
                let mut buf = Vec::new();
                resp.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(HttpRetryError::BodyIo)?;
                Ok(buf)
            })
            .map_err(|e| VykarError::Other(format!("REST LIST {prefix}: {e}")))?;
        let keys: Vec<String> = serde_json::from_slice(&body)
            .map_err(|e| VykarError::Other(format!("REST LIST parse: {e}")))?;
        Ok(keys)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        use crate::retry::HttpRetryError;
        if length == 0 {
            return Err(VykarError::Other(format!(
                "REST GET_RANGE {key}: zero-length read requested"
            )));
        }
        let url = self.url(key);
        let end = offset
            .checked_add(length)
            .and_then(|n| n.checked_sub(1))
            .ok_or_else(|| {
                VykarError::Other(format!(
                    "REST GET_RANGE {key}: offset {offset} + length {length} overflows u64"
                ))
            })?;
        let range_header = format!("bytes={offset}-{end}");
        self.retry_call_body(&format!("GET_RANGE {key}"), || {
            let req = self
                .apply_auth(self.agent.get(&url))
                .set("Range", &range_header);
            match req.call() {
                Ok(resp) => {
                    let status = resp.status();
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
                    let content_range = match resp.header("Content-Range") {
                        Some(h) => h.to_string(),
                        None => {
                            return Err(HttpRetryError::Permanent(format!(
                                "REST GET_RANGE {key}: server returned 206 without Content-Range header"
                            )));
                        }
                    };
                    if let Err(e) = Self::validate_content_range(&content_range, offset, length, key) {
                        return Err(HttpRetryError::Permanent(e.to_string()));
                    }

                    let cap = match usize::try_from(length) {
                        Ok(c) => c,
                        Err(_) => {
                            return Err(HttpRetryError::Permanent(format!(
                                "REST GET_RANGE {key}: length {length} exceeds platform usize"
                            )));
                        }
                    };
                    let mut buf = Vec::with_capacity(cap);
                    resp.into_reader()
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
                }
                Err(ureq::Error::Status(404, _)) => Ok(None),
                Err(e) => Err(HttpRetryError::http(e)),
            }
        })
        .map_err(|e| VykarError::Other(format!("REST GET_RANGE {key}: {e}")))
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let key = key.trim_start_matches('/');
        let url = format!("{}?mkdir", self.url(key));
        self.retry_call(&format!("MKDIR {key}"), || {
            let req = self.apply_auth(self.agent.post(&url));
            req.call()
        })
        .map_err(|e| VykarError::Other(format!("REST MKDIR {key}: {e}")))?;
        Ok(())
    }

    fn acquire_advisory_lock(&self, lock_id: &str, info: &BackendLockInfo) -> Result<()> {
        self.acquire_lock(lock_id, info)
    }

    fn release_advisory_lock(&self, lock_id: &str) -> Result<()> {
        self.release_lock(lock_id)
    }

    fn server_repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        self.repack(plan)
    }

    fn batch_delete_keys(&self, keys: &[String]) -> Result<()> {
        self.batch_delete(keys, true)
    }

    fn server_verify_packs(&self, plan: &VerifyPacksPlanRequest) -> Result<VerifyPacksResponse> {
        self.verify_packs(plan)
    }

    fn server_init(&self) -> Result<()> {
        let url = format!("{}?init", self.base_url);
        self.retry_call("INIT", || {
            let req = self.apply_auth(self.agent.post(&url));
            req.call()
        })
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
        let backend = RestBackend::new(&url, None, no_retry()).unwrap();

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
        let backend = RestBackend::new(&url, None, no_retry()).unwrap();

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
        let backend = RestBackend::new(&url, None, no_retry()).unwrap();

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
        let backend = RestBackend::new(&url, None, fast_retry()).unwrap();

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

        let backend = RestBackend::new(&url, None, fast_retry()).unwrap();
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
        let backend = RestBackend::new(&url, None, fast_retry()).unwrap();

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
