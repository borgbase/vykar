use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use futures_util::TryStreamExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::error::ServerError;
use crate::state::AppState;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(serde::Deserialize, Default)]
pub struct ObjectQuery {
    pub list: Option<String>,
    pub mkdir: Option<String>,
}

/// GET /{*path} — if ?list present, list keys; otherwise read object.
/// Supports Range header for partial reads.
pub async fn get_or_list(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(query): Query<ObjectQuery>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    if query.list.is_some() {
        return list_keys(state, &key).await;
    }

    let file_path = state
        .file_path(&key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    // Check for Range header
    if let Some(range_header) = headers.get("Range").and_then(|v| v.to_str().ok()) {
        return handle_range_read(&file_path, range_header, &key).await;
    }

    stream_full_read(&file_path, &key).await
}

/// HEAD /{*path} — check existence, return Content-Length.
pub async fn head_object(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Response, ServerError> {
    let file_path = state
        .file_path(&key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    let meta = match tokio::fs::metadata(&file_path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(StatusCode::NOT_FOUND.into_response());
        }
        Err(e) => return Err(ServerError::from(e)),
    };

    Ok((
        StatusCode::OK,
        [("Content-Length", meta.len().to_string())],
        Body::empty(),
    )
        .into_response())
}

/// PUT /{*path} — write object. Enforces append-only and quota.
///
/// Streams the request body to a temp file to avoid buffering large uploads
/// in memory. Atomic rename on completion.
pub async fn put_object(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Result<Response, ServerError> {
    let file_path = state
        .file_path(&key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    let existing_meta = match tokio::fs::metadata(&file_path).await {
        Ok(meta) => Some(meta),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(ServerError::from(e)),
    };

    // Append-only: reject overwrites of pack files
    if state.inner.config.append_only && key.starts_with("packs/") && existing_meta.is_some() {
        return Err(ServerError::Forbidden(
            "append-only: cannot overwrite pack files".into(),
        ));
    }

    // Track old file size for quota accounting
    let old_size = existing_meta.as_ref().map_or(0, |m| m.len());

    // Quota pre-check using Content-Length if available
    let quota = state.quota_limit();
    if quota > 0 {
        if let Some(content_length) = headers
            .get("Content-Length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
        {
            let used = state.quota_used();
            if used.saturating_sub(old_size) + content_length > quota {
                return Err(ServerError::PayloadTooLarge(format!(
                    "quota exceeded: used {used}, limit {quota}, request {content_length}",
                )));
            }
        }
    }

    // Parse X-Content-BLAKE2b header
    let is_pack = key.starts_with("packs/");
    let expected_blake2b = match headers
        .get("X-Content-BLAKE2b")
        .and_then(|v| v.to_str().ok())
    {
        Some(hex_str) => {
            if hex_str.len() != 64 || !hex_str.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(ServerError::BadRequest(
                    "X-Content-BLAKE2b must be 64 hex characters".into(),
                ));
            }
            Some(hex_str.to_ascii_lowercase())
        }
        None if is_pack => {
            return Err(ServerError::BadRequest(
                "X-Content-BLAKE2b header required for pack uploads".into(),
            ));
        }
        None => None,
    };

    // Ensure parent directory exists
    if let Some(parent) = file_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(ServerError::from)?;
    }

    // Generate a unique temp file name
    let unique_id = TEMP_COUNTER.fetch_add(1, Relaxed);
    let file_name = file_path.file_name().unwrap_or_default().to_string_lossy();
    let temp_path = file_path.with_file_name(format!(".tmp.{file_name}.{unique_id}"));

    // Stream body to temp file. The write block scopes writer/reader so
    // file handles are closed before rename (required on Windows).
    let (data_len, hasher) = {
        let stream = body.into_data_stream();
        let stream = TryStreamExt::map_err(stream, std::io::Error::other);
        let mut reader = StreamReader::new(stream);

        let temp_file = tokio::fs::File::create(&temp_path)
            .await
            .map_err(ServerError::from)?;
        let mut writer = BufWriter::with_capacity(256 * 1024, temp_file);

        let mut hasher = expected_blake2b
            .as_ref()
            .map(|_| Blake2bVar::new(32).expect("valid output size"));

        let mut data_len: u64 = 0;
        let mut buf = vec![0u8; 256 * 1024]; // 256 KiB read buffer
        let write_result: Result<(), ServerError> = async {
            loop {
                let n = reader.read(&mut buf).await.map_err(ServerError::from)?;
                if n == 0 {
                    break;
                }

                data_len += n as u64;

                // Per-chunk quota enforcement
                if quota > 0 {
                    let used = state.quota_used();
                    if used.saturating_sub(old_size) + data_len > quota {
                        return Err(ServerError::PayloadTooLarge(format!(
                            "quota exceeded during upload: used {used}, limit {quota}, written {data_len}",
                        )));
                    }
                }

                if let Some(ref mut h) = hasher {
                    h.update(&buf[..n]);
                }

                writer.write_all(&buf[..n]).await.map_err(ServerError::from)?;
            }
            writer.flush().await.map_err(ServerError::from)?;
            writer.into_inner().sync_data().await.map_err(ServerError::from)?;
            Ok(())
        }
        .await;

        if let Err(e) = write_result {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(e);
        }

        (data_len, hasher)
    };

    // Validate Content-Length if it was present
    if let Some(content_length) = headers
        .get("Content-Length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
    {
        if data_len != content_length {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(ServerError::BadRequest(format!(
                "upload size mismatch: Content-Length {content_length}, received {data_len}"
            )));
        }
    }

    // Verify BLAKE2b checksum if header was present
    if let (Some(expected), Some(hasher)) = (&expected_blake2b, hasher) {
        let mut actual = [0u8; 32];
        hasher
            .finalize_variable(&mut actual)
            .expect("correct length");
        let actual_hex = hex::encode(actual);
        if actual_hex != *expected {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(ServerError::Conflict(format!(
                "BLAKE2b checksum mismatch: expected {expected}, got {actual_hex}"
            )));
        }
    }

    // Atomic rename temp → final path (file handle already closed)
    if let Err(e) = tokio::fs::rename(&temp_path, &file_path).await {
        let _ = tokio::fs::remove_file(&temp_path).await;
        return Err(ServerError::from(e));
    }

    // Update quota
    if data_len > old_size {
        state.add_quota_usage(data_len - old_size);
    } else {
        state.sub_quota_usage(old_size - data_len);
    }

    // Detect manifest write → record backup timestamp
    if key == "manifest" {
        state.record_backup();
    }

    let status = if old_size > 0 {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::CREATED
    };
    Ok(status.into_response())
}

/// DELETE /{*path} — delete object. Rejected in append-only mode.
pub async fn delete_object(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Response, ServerError> {
    if state.inner.config.append_only {
        return Err(ServerError::Forbidden(
            "append-only: delete not allowed".into(),
        ));
    }

    let file_path = state
        .file_path(&key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    let old_size = match tokio::fs::metadata(&file_path).await {
        Ok(meta) => meta.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(StatusCode::NOT_FOUND.into_response());
        }
        Err(e) => return Err(ServerError::from(e)),
    };

    match tokio::fs::remove_file(&file_path).await {
        Ok(()) => {
            state.sub_quota_usage(old_size);
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(StatusCode::NOT_FOUND.into_response())
        }
        Err(e) => Err(ServerError::from(e)),
    }
}

async fn stream_full_read(file_path: &std::path::Path, key: &str) -> Result<Response, ServerError> {
    let file = match tokio::fs::File::open(file_path).await {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ServerError::NotFound(key.to_string()));
        }
        Err(e) => return Err(ServerError::from(e)),
    };

    let file_len = file.metadata().await.map_err(ServerError::from)?.len();
    let body = Body::from_stream(ReaderStream::new(file));
    Ok((
        StatusCode::OK,
        [("Content-Length", file_len.to_string())],
        body,
    )
        .into_response())
}

/// POST /{*path}?mkdir — create directory.
pub async fn post_object(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(query): Query<ObjectQuery>,
) -> Result<Response, ServerError> {
    if query.mkdir.is_some() {
        let dir_path = state
            .file_path(&key)
            .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
        tokio::fs::create_dir_all(&dir_path)
            .await
            .map_err(ServerError::from)?;
        return Ok(StatusCode::CREATED.into_response());
    }

    Ok(StatusCode::BAD_REQUEST.into_response())
}

async fn list_keys(state: AppState, prefix: &str) -> Result<Response, ServerError> {
    let dir_path = state
        .file_path(prefix)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    let prefix_owned = prefix.to_string();
    let keys = tokio::task::spawn_blocking(move || list_files_recursive(&dir_path, &prefix_owned))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(axum::Json(keys).into_response())
}

fn list_files_recursive(dir: &std::path::Path, prefix: &str) -> Vec<String> {
    let mut keys = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let full_key = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", prefix.trim_end_matches('/'), name)
            };
            if path.is_dir() {
                keys.extend(list_files_recursive(&path, &full_key));
            } else {
                keys.push(full_key);
            }
        }
    }
    keys
}

/// PUT without Content-Length header (for concurrent test).
#[cfg(test)]
pub(crate) async fn authed_put_no_cl(
    router: axum::Router,
    path: &str,
    body: Vec<u8>,
) -> axum::response::Response {
    use tower::ServiceExt;
    let req = axum::http::Request::builder()
        .method("PUT")
        .uri(path)
        .header(
            "Authorization",
            format!("Bearer {}", super::test_helpers::TEST_TOKEN),
        )
        .body(Body::from(body))
        .unwrap();
    router.oneshot(req).await.unwrap()
}

async fn handle_range_read(
    file_path: &std::path::Path,
    range_header: &str,
    key: &str,
) -> Result<Response, ServerError> {
    // Parse "bytes=<start>-<end>"
    let range_str = range_header
        .strip_prefix("bytes=")
        .ok_or_else(|| ServerError::BadRequest("invalid Range header".into()))?;

    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() != 2 {
        return Err(ServerError::BadRequest("invalid Range header".into()));
    }

    let start: u64 = parts[0]
        .parse()
        .map_err(|_| ServerError::BadRequest("invalid range start".into()))?;
    let end: u64 = parts[1]
        .parse()
        .map_err(|_| ServerError::BadRequest("invalid range end".into()))?;
    if end < start {
        return Err(ServerError::BadRequest(
            "invalid Range header: end before start".into(),
        ));
    }

    let length = end
        .checked_sub(start)
        .and_then(|d| d.checked_add(1))
        .ok_or_else(|| ServerError::BadRequest("invalid Range header".into()))?;
    let mut file = match tokio::fs::File::open(file_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ServerError::NotFound(key.to_string()));
        }
        Err(e) => return Err(ServerError::from(e)),
    };
    let file_len = file.metadata().await.map_err(ServerError::from)?.len();

    if start >= file_len {
        return Err(ServerError::from(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "range start beyond file size",
        )));
    }

    file.seek(SeekFrom::Start(start))
        .await
        .map_err(ServerError::from)?;

    let to_read_u64 = length.min(file_len - start);
    let actual_end = start + to_read_u64 - 1;
    let body = Body::from_stream(ReaderStream::new(file.take(to_read_u64)));

    Ok((
        StatusCode::PARTIAL_CONTENT,
        [
            (
                "Content-Range",
                format!("bytes {start}-{actual_end}/{file_len}"),
            ),
            ("Content-Length", to_read_u64.to_string()),
        ],
        body,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::StatusCode;

    use super::super::test_helpers::*;

    const CONFIG_PATH: &str = "/config";

    #[tokio::test]
    async fn put_then_get_round_trip() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0xAB; 4096];

        let resp = authed_put(router.clone(), CONFIG_PATH, data.clone()).await;
        assert_status(&resp, StatusCode::CREATED);

        let resp = authed_get(router, CONFIG_PATH).await;
        assert_status(&resp, StatusCode::OK);
        assert_eq!(body_bytes(resp).await, data);
    }

    #[tokio::test]
    async fn put_overwrite_returns_no_content() {
        let (router, _state, _tmp) = setup_app(0);

        let resp = authed_put(router.clone(), CONFIG_PATH, vec![1; 100]).await;
        assert_status(&resp, StatusCode::CREATED);

        let resp = authed_put(router.clone(), CONFIG_PATH, vec![2; 200]).await;
        assert_status(&resp, StatusCode::NO_CONTENT);

        // Verify content was updated
        let resp = authed_get(router, CONFIG_PATH).await;
        assert_eq!(body_bytes(resp).await, vec![2u8; 200]);
    }

    #[tokio::test]
    async fn put_quota_rejected() {
        let (router, _state, _tmp) = setup_app(1024); // 1 KiB quota
        let data = vec![0xFF; 2048]; // 2 KiB body

        let resp = authed_put(router, CONFIG_PATH, data).await;
        assert_status(&resp, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn put_overwrite_net_quota() {
        let (router, _state, _tmp) = setup_app(10 * 1024); // 10 KiB

        // Upload 8 KiB — should succeed
        let resp = authed_put(router.clone(), CONFIG_PATH, vec![0xAA; 8 * 1024]).await;
        assert_status(&resp, StatusCode::CREATED);

        // Overwrite with 4 KiB — should succeed, net usage drops to 4 KiB
        let resp = authed_put(router.clone(), CONFIG_PATH, vec![0xBB; 4 * 1024]).await;
        assert_status(&resp, StatusCode::NO_CONTENT);

        // Upload another 6 KiB to a different key — total would be 10 KiB, should succeed
        let resp = authed_put(router.clone(), "/index", vec![0xCC; 6 * 1024]).await;
        assert_status(&resp, StatusCode::CREATED);

        // Verify first file has new content
        let resp = authed_get(router, CONFIG_PATH).await;
        assert_eq!(body_bytes(resp).await.len(), 4 * 1024);
    }

    #[tokio::test]
    async fn put_no_leftover_temp_files() {
        let (router, _state, tmp) = setup_app(0);

        let resp = authed_put(router, CONFIG_PATH, vec![0x42; 512]).await;
        assert_status(&resp, StatusCode::CREATED);

        // Check no .tmp.* files in the repo dir
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn put_quota_rejection_cleans_temp() {
        let (router, _state, tmp) = setup_app(256);

        // Upload that will be rejected by Content-Length pre-check
        let resp = authed_put(router, CONFIG_PATH, vec![0xFF; 512]).await;
        assert_status(&resp, StatusCode::PAYLOAD_TOO_LARGE);

        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn put_concurrent_same_key() {
        let (router, _state, tmp) = setup_app(0);
        let data_a = vec![0xAA; 1024];
        let data_b = vec![0xBB; 1024];

        let router_a = router.clone();
        let router_b = router.clone();
        let da = data_a.clone();
        let db = data_b.clone();

        let (ra, rb) = tokio::join!(
            super::authed_put_no_cl(router_a, CONFIG_PATH, da),
            super::authed_put_no_cl(router_b, CONFIG_PATH, db),
        );

        // Both should succeed (one creates, one overwrites — or both create
        // depending on race). Accept CREATED or NO_CONTENT for either.
        assert!(
            ra.status() == StatusCode::CREATED || ra.status() == StatusCode::NO_CONTENT,
            "unexpected status A: {}",
            ra.status()
        );
        assert!(
            rb.status() == StatusCode::CREATED || rb.status() == StatusCode::NO_CONTENT,
            "unexpected status B: {}",
            rb.status()
        );

        // File content should match one of the two payloads
        let resp = authed_get(router, CONFIG_PATH).await;
        let body = body_bytes(resp).await;
        assert!(
            body == data_a || body == data_b,
            "file content doesn't match either payload"
        );

        // No orphan temp files
        assert_no_temp_files(tmp.path());
    }

    /// Send an authenticated PUT with a Content-Length that differs from the body.
    async fn authed_put_with_cl(
        router: axum::Router,
        path: &str,
        body: Vec<u8>,
        content_length: u64,
    ) -> axum::response::Response {
        use tower::ServiceExt;
        let req = axum::http::Request::builder()
            .method("PUT")
            .uri(path)
            .header(
                "Authorization",
                format!("Bearer {}", super::super::test_helpers::TEST_TOKEN),
            )
            .header("Content-Length", content_length.to_string())
            .body(Body::from(body))
            .unwrap();
        router.oneshot(req).await.unwrap()
    }

    #[tokio::test]
    async fn put_object_rejects_content_length_mismatch() {
        let (router, _state, tmp) = setup_app(0);
        let body = vec![0xAB; 50];

        // Content-Length says 100, but body is only 50 bytes
        let resp = authed_put_with_cl(router.clone(), CONFIG_PATH, body, 100).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        let body_raw = body_bytes(resp).await;
        let body_text = String::from_utf8_lossy(&body_raw);
        assert!(
            body_text.contains("upload size mismatch"),
            "expected upload size mismatch error, got: {body_text}"
        );

        // Verify no temp files left behind
        assert_no_temp_files(tmp.path());

        // Verify the final object was not created
        let resp = authed_get(router, CONFIG_PATH).await;
        assert_status(&resp, StatusCode::NOT_FOUND);
    }

    /// Recursively check that no `.tmp.*` files exist under the given path.
    fn assert_no_temp_files(dir: &std::path::Path) {
        for path in walk_file_paths(dir) {
            let name = path.file_name().unwrap().to_string_lossy();
            assert!(
                !name.starts_with(".tmp."),
                "leftover temp file: {}",
                path.display()
            );
        }
    }

    fn walk_file_paths(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    out.extend(walk_file_paths(&path));
                } else {
                    out.push(path);
                }
            }
        }
        out
    }

    /// Compute BLAKE2b-256 hex of data (for test convenience).
    fn blake2b_hex(data: &[u8]) -> String {
        use blake2::digest::{Update, VariableOutput};
        use blake2::Blake2bVar;
        let mut hasher = Blake2bVar::new(32).unwrap();
        hasher.update(data);
        let mut out = [0u8; 32];
        hasher.finalize_variable(&mut out).unwrap();
        hex::encode(out)
    }

    /// Send an authenticated PUT with an X-Content-BLAKE2b header.
    async fn authed_put_with_blake2b(
        router: axum::Router,
        path: &str,
        body: Vec<u8>,
        checksum: &str,
    ) -> axum::response::Response {
        use tower::ServiceExt;
        let req = axum::http::Request::builder()
            .method("PUT")
            .uri(path)
            .header(
                "Authorization",
                format!("Bearer {}", super::super::test_helpers::TEST_TOKEN),
            )
            .header("Content-Length", body.len().to_string())
            .header("X-Content-BLAKE2b", checksum)
            .body(Body::from(body))
            .unwrap();
        router.oneshot(req).await.unwrap()
    }

    // -----------------------------------------------------------------------
    // BLAKE2b checksum verification tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn put_pack_with_valid_checksum_succeeds() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0xDE; 512];
        let checksum = blake2b_hex(&data);
        let pack_path = format!("/packs/{}/{}", &checksum[..2], checksum);

        let resp = authed_put_with_blake2b(router, &pack_path, data, &checksum).await;
        assert_status(&resp, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn put_pack_with_wrong_checksum_returns_409() {
        let (router, _state, tmp) = setup_app(0);
        let data = vec![0xDE; 512];
        let wrong_checksum = "a".repeat(64);
        let real_checksum = blake2b_hex(&data);
        let pack_path = format!("/packs/{}/{}", &real_checksum[..2], real_checksum);

        let resp = authed_put_with_blake2b(router.clone(), &pack_path, data, &wrong_checksum).await;
        assert_status(&resp, StatusCode::CONFLICT);
        let body_raw = body_bytes(resp).await;
        let body_text = String::from_utf8_lossy(&body_raw);
        assert!(
            body_text.contains("BLAKE2b checksum mismatch"),
            "got: {body_text}"
        );

        // Object should not exist
        let resp = authed_get(router, &pack_path).await;
        assert_status(&resp, StatusCode::NOT_FOUND);

        // No temp files
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn put_pack_without_checksum_returns_400() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0xDE; 512];
        let hex = blake2b_hex(&data);
        let pack_path = format!("/packs/{}/{}", &hex[..2], hex);

        // Use regular authed_put (no X-Content-BLAKE2b header)
        let resp = authed_put(router, &pack_path, data).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        let body_raw = body_bytes(resp).await;
        let body_text = String::from_utf8_lossy(&body_raw);
        assert!(
            body_text.contains("X-Content-BLAKE2b header required"),
            "got: {body_text}"
        );
    }

    #[tokio::test]
    async fn put_non_pack_without_checksum_succeeds() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0xAB; 128];

        // Regular PUT to config (no checksum header) should succeed
        let resp = authed_put(router, CONFIG_PATH, data).await;
        assert_status(&resp, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn put_with_malformed_checksum_returns_400() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0xAB; 128];

        // Too short
        let resp = authed_put_with_blake2b(router.clone(), CONFIG_PATH, data.clone(), "abcd").await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        let body_raw = body_bytes(resp).await;
        let body_text = String::from_utf8_lossy(&body_raw);
        assert!(body_text.contains("64 hex characters"), "got: {body_text}");

        // Right length but not hex
        let bad = "g".repeat(64);
        let resp = authed_put_with_blake2b(router, CONFIG_PATH, data, &bad).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn put_non_pack_with_valid_checksum_succeeds() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0xAB; 128];
        let checksum = blake2b_hex(&data);

        let resp = authed_put_with_blake2b(router, CONFIG_PATH, data, &checksum).await;
        assert_status(&resp, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn put_non_pack_with_wrong_checksum_returns_409() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0xAB; 128];
        let wrong = "b".repeat(64);

        let resp = authed_put_with_blake2b(router, CONFIG_PATH, data, &wrong).await;
        assert_status(&resp, StatusCode::CONFLICT);
    }
}
