use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::TryStreamExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::error::ServerError;
use crate::state::AppState;
use vykar_protocol::SizedKey;
use vykar_types::hash::{HashAlgorithm, Hasher256};

#[derive(serde::Deserialize, Default)]
pub struct ObjectQuery {
    pub list: Option<String>,
    /// With `list`: return [`vykar_protocol::SizedKey`] entries instead of bare keys.
    pub sized: Option<String>,
    pub mkdir: Option<String>,
    /// POST: report the sizes of a JSON list of keys (see `object_sizes`).
    pub sizes: Option<String>,
    /// Part upload token (`PUT ...?upload=<token>&offset=<n>&total=<n>`, and
    /// `POST ...?complete&upload=<token>`); see `super::parts`.
    pub upload: Option<String>,
    pub offset: Option<u64>,
    pub total: Option<u64>,
    pub complete: Option<String>,
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
        return list_keys(state, &key, query.sized.is_some()).await;
    }

    let file_path = state
        .file_path(&key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    // Check for Range header
    if let Some(range_header) = headers.get("Range").and_then(|v| v.to_str().ok()) {
        return handle_range_read(&file_path, range_header, &key).await;
    }

    if crate::index_etag::is_index_key(&key) {
        if let Some(resp) = index_not_modified(&state, &file_path, &headers) {
            return Ok(resp);
        }
    }

    stream_full_read(&file_path, &key).await
}

/// `304 Not Modified` for a `GET /index` whose `If-None-Match` names the digest
/// recorded by the last `PUT /index`, while the file that PUT produced is still
/// the one on disk. `None` means serve the object normally (also for any stat
/// error: the normal read reports it). Synchronous on purpose: the record's
/// lock is held across the stat.
fn index_not_modified(
    state: &AppState,
    file_path: &std::path::Path,
    headers: &HeaderMap,
) -> Option<Response> {
    let inm = headers
        .get(axum::http::header::IF_NONE_MATCH)?
        .to_str()
        .ok()?;
    let slot = state.lock_index_validator();
    let recorded = slot.as_ref()?;
    if !crate::index_etag::if_none_match_hits(inm, &recorded.digest_hex) {
        return None;
    }
    let meta = std::fs::metadata(file_path).ok()?;
    if crate::index_etag::FileFingerprint::of(&meta)? != recorded.fingerprint {
        return None;
    }
    let etag = format!("\"{}\"", recorded.digest_hex);
    drop(slot);
    Some((StatusCode::NOT_MODIFIED, [(axum::http::header::ETAG, etag)]).into_response())
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
///
/// Cancellation-safe: hyper drops this future when the client disconnects
/// mid-upload, so the temp file is owned by a `tempfile::TempPath` whose Drop
/// unlinks it. That is the *only* cleanup path — no per-site `remove_file`.
#[allow(clippy::too_many_lines)]
pub async fn put_object(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(query): Query<ObjectQuery>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Result<Response, ServerError> {
    if let Some(token) = query.upload.as_deref() {
        return super::parts::put_part(state, &key, token, &query, &headers, body).await;
    }

    let file_path = state
        .file_path(&key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
    let is_index = crate::index_etag::is_index_key(&key);

    let existing_meta = match tokio::fs::metadata(&file_path).await {
        Ok(meta) => Some(meta),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(ServerError::from(e)),
    };

    // Append-only: only index, index.gen, index.d/*, locks/*, sessions/* are
    // overwritable. Everything else (config, keys/*, snapshots/*, packs/*) is
    // immutable once written. An `index.d/` object is content-named, so an
    // overwrite is a retried or repeated PUT of the same state; the index
    // itself was always overwritable.
    if state.inner.config.append_only && existing_meta.is_some() {
        let is_mutable = key == "index"
            || key == "index.gen"
            || key == "manifest" // v1 compat — remove once v1 clients are retired
            || vykar_protocol::is_index_object_key(&key)
            || key.starts_with("locks/")
            || key.starts_with("sessions/");
        if !is_mutable {
            return Err(ServerError::Forbidden(
                "append-only: cannot overwrite immutable object".into(),
            ));
        }
    }

    // Track old file size for quota accounting.
    let old_size = existing_meta.as_ref().map_or(0, std::fs::Metadata::len);

    // Parse Content-Length once (reused for the reservation and the
    // post-upload size check).
    let content_length = headers
        .get("Content-Length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    // Reserve quota headroom up-front. Net accounting: reserve only the growth
    // over the object being overwritten. A no-Content-Length streaming upload
    // reserves 0 here and grows per-chunk below. The reservation's Drop releases
    // on every failure path (write error, CL/checksum mismatch, rename failure,
    // panic) — no per-site cleanup needed.
    let mut reservation = state
        .try_reserve_quota(content_length.map_or(0, |cl| cl.saturating_sub(old_size)))
        .map_err(|(used, limit)| {
            ServerError::PayloadTooLarge(format!(
                "quota exceeded: used {used}, limit {limit}, request {}",
                content_length.unwrap_or(0)
            ))
        })?;

    let expected_digest = parse_expected_digest(&headers, key.starts_with("packs/"))?;

    let parent = file_path
        .parent()
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
    ensure_parent_dir(&state, parent).await?;

    // `temp_path` is the RAII owner. It lives outside the write block below
    // so the file handle is dropped before the path is unlinked (Windows).
    let (temp_file, temp_path) =
        crate::cleanup::create_temp_in(parent).map_err(ServerError::from)?;

    // Stream body to temp file. The write block scopes writer/reader so
    // file handles are closed before rename (required on Windows).
    let (data_len, hasher) = {
        let stream = body.into_data_stream();
        let stream = TryStreamExt::map_err(stream, std::io::Error::other);
        let mut reader = StreamReader::new(stream);

        // A second handle (same open file description) for the writeback
        // kicks below, which run on the blocking pool while the body streams.
        let writeback_file = std::sync::Arc::new(temp_file.try_clone().map_err(ServerError::from)?);
        let mut since_writeback: u64 = 0;
        let mut writeback_task: Option<tokio::task::JoinHandle<()>> = None;

        let temp_file = tokio::fs::File::from_std(temp_file);
        let mut writer = BufWriter::with_capacity(256 * 1024, temp_file);

        let mut hasher = expected_digest
            .as_ref()
            .map(|(algo, _)| Hasher256::new(*algo));

        let mut data_len: u64 = 0;
        let mut buf = vec![0u8; 256 * 1024]; // 256 KiB read buffer
        let write_result: Result<(), ServerError> = async {
            loop {
                let n = reader.read(&mut buf).await.map_err(ServerError::from)?;
                if n == 0 {
                    break;
                }

                data_len += n as u64;

                // Grow the reservation to cover net growth as the body streams.
                // No-op once the up-front Content-Length reservation already
                // covers it (net accounting; overwrites shrink `needed`).
                let needed = data_len.saturating_sub(old_size);
                if needed > reservation.remaining() {
                    let extra = needed - reservation.remaining();
                    reservation.grow(extra).map_err(|(used, limit)| {
                        ServerError::PayloadTooLarge(format!(
                            "quota exceeded during upload: used {used}, limit {limit}, written {data_len}",
                        ))
                    })?;
                }

                let chunk = buf.get(..n).expect("n <= buf.len() (just-read bytes)");
                if let Some(ref mut h) = hasher {
                    h.update(chunk);
                }

                writer.write_all(chunk).await.map_err(ServerError::from)?;

                // Start writeback of what has been written so far, so the
                // final fdatasync does not have to flush the whole upload
                // while the client waits. At most one kick is in flight; a
                // skipped one is covered by the next, which spans the file.
                since_writeback += n as u64;
                if crate::writeback::SUPPORTED
                    && since_writeback >= crate::writeback::WRITEBACK_INTERVAL
                    && writeback_task
                        .as_ref()
                        .is_none_or(tokio::task::JoinHandle::is_finished)
                {
                    since_writeback = 0;
                    let file = std::sync::Arc::clone(&writeback_file);
                    writeback_task = Some(tokio::task::spawn_blocking(move || {
                        crate::writeback::start_writeback(&file);
                    }));
                }
            }
            // Wait for a still-running kick so no task outlives the upload
            // holding a handle to the temp file.
            if let Some(task) = writeback_task.take() {
                let _ = task.await;
            }
            writer.flush().await.map_err(ServerError::from)?;
            // fdatasync_file is blocking (and carries the Apple F_FULLFSYNC
            // fallback), so run it off the runtime the way fsync_dir_async does.
            let synced = writer.into_inner().into_std().await;
            tokio::task::spawn_blocking(move || vykar_common::fs::fdatasync_file(&synced))
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?
                .map_err(ServerError::from)?;
            Ok(())
        }
        .await;

        write_result?;

        (data_len, hasher)
    };

    // Validate Content-Length if it was present
    if let Some(content_length) = content_length {
        if data_len != content_length {
            return Err(ServerError::BadRequest(format!(
                "upload size mismatch: Content-Length {content_length}, received {data_len}"
            )));
        }
    }

    // Verify the content digest if a header was present.
    if let (Some((algo, expected)), Some(hasher)) = (&expected_digest, hasher) {
        let actual_hex = hasher.finalize_hex();
        if actual_hex != *expected {
            return Err(digest_mismatch(*algo, expected, &actual_hex));
        }
    }

    // Atomic rename temp → final path (file handle already closed). `persist`
    // consumes the owner, so nothing is unlinked afterwards; on failure the
    // temp is still dropped (and unlinked) with the error.
    if is_index {
        // Only a verified BLAKE3 can answer a conditional GET (it is the
        // digest clients cache the index under); any other PUT clears the
        // record. The lock keeps other `index` writes out
        // between the rename and the stat, so the fingerprint is this file's.
        let digest_hex = match &expected_digest {
            Some((HashAlgorithm::Blake3, hex)) => Some(hex.clone()),
            _ => None,
        };
        let mut slot = state.lock_index_validator();
        temp_path
            .persist(&file_path)
            .map_err(|e| ServerError::from(e.error))?;
        let fingerprint = std::fs::metadata(&file_path)
            .ok()
            .as_ref()
            .and_then(crate::index_etag::FileFingerprint::of);
        *slot = digest_hex
            .zip(fingerprint)
            .map(
                |(digest_hex, fingerprint)| crate::index_etag::IndexValidator {
                    digest_hex,
                    fingerprint,
                },
            );
    } else {
        temp_path
            .persist(&file_path)
            .map_err(|e| ServerError::from(e.error))?;
    }

    // Commit the reservation (moves reserved bytes into committed usage) right
    // after the rename, before the fsync below: the file is on disk either
    // way, and skipping the commit on fsync failure would undercount — a retry
    // of the same key sees old_size == data_len and commits nothing. When the
    // object shrank, drop the freed bytes from committed usage as well.
    reservation.commit();
    if data_len < old_size {
        state.sub_quota_usage(old_size - data_len);
    }

    // Fsync the parent directory so the rename survives power loss. The
    // snapshot blob is the client's commit point, so we must not ack until the
    // rename is durable — a 5xx here is correct; clients retry idempotent PUTs.
    fsync_dir_async(parent.to_path_buf())
        .await
        .map_err(ServerError::from)?;

    // Detect backup completion: v2 writes snapshots/<id>, v1 writes manifest.
    // Remove the manifest branch once v1 clients are retired.
    if (key.starts_with("snapshots/") && old_size == 0) || key == "manifest" {
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
    let is_temp = vykar_protocol::is_temp_file(&key);

    // Append-only forbids deleting committed data. Temp files are uncommitted
    // (interrupted PUTs), and locks/ + sessions/ are ephemeral coordination
    // state — deleting any of these cannot violate append-only. Index objects
    // are the same trust as overwriting `index`, which was always allowed.
    if state.inner.config.append_only
        && !is_temp
        && !vykar_protocol::is_index_object_key(&key)
        && !key.starts_with("locks/")
        && !key.starts_with("sessions/")
    {
        return Err(ServerError::Forbidden(
            "append-only: delete not allowed".into(),
        ));
    }

    // Temp names fail the strict key schema, so resolve them with the lenient
    // cleanup resolver. (This also fixes a latent bug: DELETE of a `.tmp.*` file
    // always returned 400 because `file_path` rejects temp names.)
    let file_path = if is_temp {
        state.file_path_for_cleanup(&key)
    } else {
        state.file_path(&key)
    }
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
            if crate::index_etag::is_index_key(&key) {
                *state.lock_index_validator() = None;
            }
            // Temp-file bytes were never committed to `quota_usage` (they live
            // in `quota_reserved` while the upload runs, released on failure),
            // so subtracting them would drift usage low — the unsafe direction.
            // Debris counted by a startup/rescan dir scan is over-counted until
            // the next rescan instead (safe direction).
            if !is_temp {
                state.sub_quota_usage(old_size);
            }
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(StatusCode::NOT_FOUND.into_response())
        }
        Err(e) => Err(ServerError::from(e)),
    }
}

/// Parse the content-digest header. Two named headers rather than one
/// generalized `X-Content-Digest: <algo>=<hex>`: the BLAKE2b variant has to be
/// kept for old-client compatibility either way, so generalizing would mean
/// two mechanisms for no gain, and the named form keeps the error message
/// below truthful.
///
///   BLAKE2b only                -> verify BLAKE2b
///   BLAKE3 only                 -> verify BLAKE3
///   both                        -> 400 (ambiguous)
///   neither, `is_pack`          -> 400
///   neither, non-pack key       -> skip verification
pub(crate) fn parse_expected_digest(
    headers: &HeaderMap,
    is_pack: bool,
) -> Result<Option<(HashAlgorithm, String)>, ServerError> {
    let parse_digest = |name: &str| -> Result<Option<String>, ServerError> {
        match headers.get(name).and_then(|v| v.to_str().ok()) {
            Some(hex_str) => {
                if hex_str.len() != 64 || !hex_str.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(ServerError::BadRequest(format!(
                        "{name} must be 64 hex characters"
                    )));
                }
                Ok(Some(hex_str.to_ascii_lowercase()))
            }
            None => Ok(None),
        }
    };
    match (
        parse_digest("X-Content-BLAKE2b")?,
        parse_digest("X-Content-BLAKE3")?,
    ) {
        (Some(_), Some(_)) => Err(ServerError::BadRequest(
            "X-Content-BLAKE2b and X-Content-BLAKE3 are mutually exclusive".into(),
        )),
        (Some(hex_str), None) => Ok(Some((HashAlgorithm::Blake2b, hex_str))),
        (None, Some(hex_str)) => Ok(Some((HashAlgorithm::Blake3, hex_str))),
        (None, None) if is_pack => Err(ServerError::BadRequest(
            "X-Content-BLAKE2b or X-Content-BLAKE3 header required for pack uploads".into(),
        )),
        (None, None) => Ok(None),
    }
}

/// The 409 for a content digest that does not match.
pub(crate) fn digest_mismatch(algo: HashAlgorithm, expected: &str, actual: &str) -> ServerError {
    let name = match algo {
        HashAlgorithm::Blake2b => "BLAKE2b",
        HashAlgorithm::Blake3 => "BLAKE3",
    };
    ServerError::Conflict(format!(
        "{name} checksum mismatch: expected {expected}, got {actual}"
    ))
}

/// Ensure `parent` exists. When it had to be created, fsync the new ancestor
/// chain up to `data_dir` so the directory entries survive power loss (rare
/// path — only when missing).
pub(crate) async fn ensure_parent_dir(
    state: &AppState,
    parent: &std::path::Path,
) -> Result<(), ServerError> {
    let parent_existed = tokio::fs::try_exists(parent).await.unwrap_or(false);
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(ServerError::from)?;
    if !parent_existed {
        let data_dir = state.inner.data_dir.as_path();
        let mut cursor = Some(parent);
        while let Some(dir) = cursor {
            fsync_dir_async(dir.to_path_buf())
                .await
                .map_err(ServerError::from)?;
            if dir == data_dir {
                break;
            }
            cursor = dir.parent();
        }
    }
    Ok(())
}

/// Fsync a directory off the async runtime (directory fsync is blocking).
pub(crate) async fn fsync_dir_async(dir: std::path::PathBuf) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || vykar_common::fs::fsync_dir(&dir))
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?
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
/// POST /{*path}?sizes — sizes of a JSON list of keys below `path`.
/// `POST /{*path}?complete&upload=<token>` — finish a part upload.
pub async fn post_object(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(query): Query<ObjectQuery>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ServerError> {
    // The raw body, not `Bytes`: object routes allow 512 MiB bodies, and a
    // `Bytes` extractor would buffer that much for every POST before the
    // handler runs. Only `?sizes` reads its body, under its own cap.
    if query.sizes.is_some() {
        let body = read_sizes_body(&headers, body).await?;
        return object_sizes(state, &key, &body).await;
    }
    if query.complete.is_some() {
        let token = query
            .upload
            .as_deref()
            .ok_or_else(|| ServerError::BadRequest("complete requires an upload token".into()))?;
        return super::parts::complete_upload(state, &key, token, &headers).await;
    }

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

/// Cap on a `POST ?sizes` body. Object routes accept 512 MiB bodies (pack
/// uploads); a key list at [`vykar_protocol::MAX_OBJECT_SIZES_KEYS`] is
/// ~8 MiB of JSON.
const MAX_SIZES_BODY_BYTES: usize = 32 * 1024 * 1024;

/// Most a `?sizes` body buffer reserves up front from `Content-Length`. The
/// header is only a claim: a client can declare the full cap and send nothing,
/// and there is no read timeout, so reserving the declared length would commit
/// up to the cap per stalled request. Beyond this the buffer grows as bytes
/// arrive; real bodies are ~20 KB, ~8 MiB at most.
const SIZES_BODY_INITIAL_CAPACITY: usize = 1024 * 1024;

/// Initial capacity of the `?sizes` body buffer for a declared length.
fn sizes_body_initial_capacity(content_length: Option<u64>) -> usize {
    content_length.map_or(0, |len| {
        usize::try_from(len)
            .unwrap_or(usize::MAX)
            .min(SIZES_BODY_INITIAL_CAPACITY)
    })
}

/// Read a `?sizes` request body, refusing (413) one larger than
/// [`MAX_SIZES_BODY_BYTES`] — up front from `Content-Length` when the client
/// sends one, else as soon as the streamed body crosses the cap — so at most
/// the cap is ever buffered.
async fn read_sizes_body(headers: &HeaderMap, body: Body) -> Result<Vec<u8>, ServerError> {
    let too_large = |len: u64| {
        ServerError::PayloadTooLarge(format!(
            "sizes request body is at least {len} bytes (limit {MAX_SIZES_BODY_BYTES})"
        ))
    };
    let content_length = headers
        .get("Content-Length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if let Some(len) = content_length {
        if len > MAX_SIZES_BODY_BYTES as u64 {
            return Err(too_large(len));
        }
    }
    let mut buf = Vec::with_capacity(sizes_body_initial_capacity(content_length));
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| ServerError::BadRequest(format!("reading sizes request: {e}")))?
    {
        let len = buf.len() + chunk.len();
        if len > MAX_SIZES_BODY_BYTES {
            return Err(too_large(len as u64));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// Answer `POST /<dir>?sizes`: the size of every requested key, `null` for
/// an absent one, in request order (see
/// [`vykar_protocol::MAX_OBJECT_SIZES_KEYS`]).
///
/// One request replaces a `HEAD` per key (or a listing per directory) for a
/// client that needs to know which of many objects exist, e.g. the packs a
/// backup is about to commit. Sizes follow symlinks and count only regular
/// files, matching what `list` reports. A stat error other than "not found"
/// fails the whole request: reporting such an object as absent would turn an
/// I/O problem into a false "missing" verdict.
async fn object_sizes(state: AppState, dir: &str, body: &[u8]) -> Result<Response, ServerError> {
    let keys: Vec<String> = serde_json::from_slice(body)
        .map_err(|e| ServerError::BadRequest(format!("invalid sizes request: {e}")))?;
    if keys.len() > vykar_protocol::MAX_OBJECT_SIZES_KEYS {
        return Err(ServerError::BadRequest(format!(
            "sizes request names {} keys (limit {})",
            keys.len(),
            vykar_protocol::MAX_OBJECT_SIZES_KEYS
        )));
    }
    let dir_prefix = format!("{}/", dir.trim_matches('/'));
    let mut paths = Vec::with_capacity(keys.len());
    for key in &keys {
        if !key.starts_with(&dir_prefix) || vykar_protocol::is_temp_file(key) {
            return Err(ServerError::BadRequest(format!(
                "sizes: key {key:?} is not an object below {dir_prefix}"
            )));
        }
        let path = state
            .file_path(key)
            .ok_or_else(|| ServerError::BadRequest(format!("sizes: invalid key {key:?}")))?;
        paths.push(path);
    }
    drop(keys);

    let sizes = tokio::task::spawn_blocking(move || {
        paths
            .iter()
            .map(|path| match std::fs::metadata(path) {
                Ok(meta) if meta.is_file() => Ok(Some(meta.len())),
                Ok(_) => Ok(None),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            })
            .collect::<std::io::Result<Vec<Option<u64>>>>()
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::from)?;

    Ok(axum::Json(sizes).into_response())
}

async fn list_keys(state: AppState, prefix: &str, sized: bool) -> Result<Response, ServerError> {
    let dir_path = state
        .file_path(prefix)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    let prefix_owned = prefix.to_string();
    let keys = tokio::task::spawn_blocking(move || list_files_recursive(&dir_path, &prefix_owned))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(list_response(keys, sized))
}

/// `sized` selects the `?list&sized` format; otherwise bare keys, as old
/// clients expect.
pub(crate) fn list_response(keys: Vec<SizedKey>, sized: bool) -> Response {
    if sized {
        axum::Json(keys).into_response()
    } else {
        axum::Json(keys.into_iter().map(|k| k.key).collect::<Vec<_>>()).into_response()
    }
}

/// Sizes follow symlinks, matching what HEAD reports for the same key.
fn list_files_recursive(dir: &std::path::Path, prefix: &str) -> Vec<SizedKey> {
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
            let Ok(meta) = std::fs::metadata(&path) else {
                continue;
            };
            if meta.is_dir() {
                keys.extend(list_files_recursive(&path, &full_key));
            } else if !vykar_protocol::is_temp_file(&full_key) {
                keys.push(SizedKey {
                    key: full_key,
                    size: meta.len(),
                });
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
    let (start, length) =
        vykar_protocol::parse_byte_range(range_header).map_err(ServerError::BadRequest)?;
    let mut file = match tokio::fs::File::open(file_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ServerError::NotFound(key.to_string()));
        }
        Err(e) => return Err(ServerError::from(e)),
    };
    let file_len = file.metadata().await.map_err(ServerError::from)?.len();

    if start >= file_len {
        // 416, not 500. We omit the `Content-Range: bytes */len` header: the
        // client's retry layer treats 4xx as permanent and doesn't parse it, so
        // an out-of-range read fails fast instead of retry-looping on a 500.
        return Err(ServerError::RangeNotSatisfiable(format!(
            "range start {start} beyond file size {file_len}"
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

    fn pack_key(byte: u8) -> String {
        format!("packs/{byte:02x}/{}", hex::encode([byte; 32]))
    }

    /// Store a pack file directly in the data dir (no checksum header needed).
    fn store_pack(tmp: &tempfile::TempDir, byte: u8, len: usize) -> String {
        let key = pack_key(byte);
        let path = tmp.path().join(&key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, vec![0u8; len]).unwrap();
        key
    }

    async fn post_sizes(router: axum::Router, keys: &[String]) -> axum::response::Response {
        authed_post(router, "/packs?sizes", serde_json::to_vec(keys).unwrap()).await
    }

    #[tokio::test]
    async fn sizes_reports_present_and_absent_in_order() {
        let (router, _state, tmp) = setup_app(0);
        let a = store_pack(&tmp, 0x0a, 10);
        let b = store_pack(&tmp, 0xb0, 7);
        let absent = pack_key(0xcc);
        let resp = post_sizes(router, &[b.clone(), absent, a.clone(), b]).await;
        assert_status(&resp, StatusCode::OK);
        let sizes: Vec<Option<u64>> = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(sizes, vec![Some(7), None, Some(10), Some(7)]);
    }

    #[tokio::test]
    async fn sizes_rejects_keys_outside_the_directory() {
        let (router, _state, _tmp) = setup_app(0);
        for bad in ["index", "snapshots/x", "packs/../config", "packsx/ab/cd"] {
            let resp = post_sizes(router.clone(), &[bad.to_string()]).await;
            assert_status(&resp, StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn sizes_rejects_oversized_and_malformed_requests() {
        let (router, _state, _tmp) = setup_app(0);
        let too_many = vec![pack_key(1); vykar_protocol::MAX_OBJECT_SIZES_KEYS + 1];
        let resp = post_sizes(router.clone(), &too_many).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        let resp = authed_post(router, "/packs?sizes", b"{not json".to_vec()).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
    }

    fn authed_request(method: &str, uri: &str) -> axum::http::request::Builder {
        axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("Authorization", format!("Bearer {TEST_TOKEN}"))
    }

    /// An over-cap `?sizes` body is refused with 413 whether the client
    /// declares its length or streams it, without buffering past the cap.
    #[tokio::test]
    async fn sizes_rejects_a_body_over_the_cap() {
        use futures_util::StreamExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tower::ServiceExt;
        let (router, _state, _tmp) = setup_app(0);
        let cap = super::MAX_SIZES_BODY_BYTES;

        // Declared too large: refused from the header. The body never ends,
        // so this would time out if the handler tried to read it.
        let req = authed_request("POST", "/packs?sizes")
            .header("Content-Length", (cap + 1).to_string())
            .body(Body::from_stream(futures_util::stream::pending::<
                Result<axum::body::Bytes, std::io::Error>,
            >()))
            .unwrap();
        let resp = tokio::time::timeout(Duration::from_secs(10), router.clone().oneshot(req))
            .await
            .expect("over-cap Content-Length must be refused without reading the body")
            .unwrap();
        assert_status(&resp, StatusCode::PAYLOAD_TOO_LARGE);

        // No Content-Length: refused once the stream crosses the cap. The
        // body is twice the cap; reading stops one chunk past it.
        const CHUNK: usize = 1024 * 1024;
        let pulled = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&pulled);
        let chunk = axum::body::Bytes::from(vec![b' '; CHUNK]);
        let stream = futures_util::stream::repeat_with(move || {
            counter.fetch_add(CHUNK, Ordering::Relaxed);
            Ok::<_, std::io::Error>(chunk.clone())
        })
        .take(2 * cap / CHUNK);
        let req = authed_request("POST", "/packs?sizes")
            .body(Body::from_stream(stream))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_status(&resp, StatusCode::PAYLOAD_TOO_LARGE);
        let pulled = pulled.load(Ordering::Relaxed);
        assert!(pulled <= cap + CHUNK, "read {pulled} bytes, cap {cap}");
    }

    /// A `?sizes` body of exactly the cap is accepted, with its length
    /// declared or streamed: `[]` padded with whitespace to the cap, sent in
    /// 1 MiB chunks so the test itself holds only one chunk.
    #[tokio::test]
    async fn sizes_accepts_a_body_at_the_cap() {
        use tower::ServiceExt;
        let (router, _state, _tmp) = setup_app(0);
        let cap = super::MAX_SIZES_BODY_BYTES;
        const CHUNK: usize = 1024 * 1024;
        let spaces = axum::body::Bytes::from(vec![b' '; CHUNK]);
        let body = || {
            let mut first = vec![b' '; CHUNK];
            first[..2].copy_from_slice(b"[]");
            let rest = std::iter::repeat_n(spaces.clone(), cap / CHUNK - 1);
            let chunks = std::iter::once(axum::body::Bytes::from(first))
                .chain(rest)
                .map(Ok::<_, std::io::Error>);
            Body::from_stream(futures_util::stream::iter(chunks))
        };

        for declared in [true, false] {
            let mut req = authed_request("POST", "/packs?sizes");
            if declared {
                req = req.header("Content-Length", cap.to_string());
            }
            let resp = router
                .clone()
                .oneshot(req.body(body()).unwrap())
                .await
                .unwrap();
            assert_status(&resp, StatusCode::OK);
            assert_eq!(body_bytes(resp).await, b"[]", "declared: {declared}");
        }
    }

    #[test]
    fn sizes_body_initial_capacity_is_bounded() {
        use super::{sizes_body_initial_capacity as cap_for, SIZES_BODY_INITIAL_CAPACITY};
        assert_eq!(cap_for(None), 0);
        assert_eq!(cap_for(Some(0)), 0);
        assert_eq!(cap_for(Some(20_000)), 20_000);
        let limit = SIZES_BODY_INITIAL_CAPACITY as u64;
        assert_eq!(cap_for(Some(limit)), SIZES_BODY_INITIAL_CAPACITY);
        assert_eq!(cap_for(Some(limit + 1)), SIZES_BODY_INITIAL_CAPACITY);
        let at_cap = super::MAX_SIZES_BODY_BYTES as u64;
        assert_eq!(cap_for(Some(at_cap)), SIZES_BODY_INITIAL_CAPACITY);
        assert_eq!(cap_for(Some(u64::MAX)), SIZES_BODY_INITIAL_CAPACITY);
    }

    /// `?mkdir` ignores a request body (clients send none, but one must not
    /// be buffered or break the request).
    #[tokio::test]
    async fn mkdir_creates_directory_with_or_without_a_body() {
        let (router, _state, tmp) = setup_app(0);
        let resp = authed_post(router.clone(), "/snapshots/a?mkdir", Vec::new()).await;
        assert_status(&resp, StatusCode::CREATED);
        assert!(tmp.path().join("snapshots/a").is_dir());
        let resp = authed_post(router, "/snapshots/b?mkdir", vec![0x41; 4096]).await;
        assert_status(&resp, StatusCode::CREATED);
        assert!(tmp.path().join("snapshots/b").is_dir());
    }

    /// A directory under a pack key is not an object.
    #[tokio::test]
    async fn sizes_reports_directories_as_absent() {
        let (router, _state, tmp) = setup_app(0);
        let key = pack_key(0x42);
        std::fs::create_dir_all(tmp.path().join(&key)).unwrap();
        let resp = post_sizes(router, &[key]).await;
        assert_status(&resp, StatusCode::OK);
        let sizes: Vec<Option<u64>> = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(sizes, vec![None]);
    }

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
    async fn put_concurrent_quota_race() {
        // Two concurrent 600-byte PUTs (distinct keys) against a 1000-byte
        // quota: exactly one succeeds, one is rejected, and committed usage
        // settles at 600 with no leftover reservation.
        let (router, state, _tmp) = setup_app(1000);

        let (ra, rb) = tokio::join!(
            authed_put(router.clone(), CONFIG_PATH, vec![0xAA; 600]),
            authed_put(router.clone(), "/index", vec![0xBB; 600]),
        );

        let statuses = [ra.status(), rb.status()];
        let created = statuses
            .iter()
            .filter(|s| **s == StatusCode::CREATED)
            .count();
        let rejected = statuses
            .iter()
            .filter(|s| **s == StatusCode::PAYLOAD_TOO_LARGE)
            .count();
        assert_eq!(created, 1, "exactly one PUT should succeed: {statuses:?}");
        assert_eq!(
            rejected, 1,
            "exactly one PUT should be rejected: {statuses:?}"
        );
        assert_eq!(state.quota_used(), 600);
        assert_eq!(state.quota_reserved(), 0);
    }

    #[tokio::test]
    async fn put_cl_mismatch_releases_reservation() {
        let (router, state, tmp) = setup_app(1000);

        // Reserve 800 via Content-Length, then send only 50 bytes -> mismatch.
        let resp = authed_put_with_cl(router.clone(), CONFIG_PATH, vec![0xAA; 50], 800).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        assert_eq!(state.quota_reserved(), 0, "failed PUT releases reservation");
        assert_no_temp_files(tmp.path());

        // A subsequent full-quota PUT now succeeds.
        let resp = authed_put(router.clone(), "/index", vec![0xBB; 1000]).await;
        assert_status(&resp, StatusCode::CREATED);
        assert_eq!(state.quota_reserved(), 0);
        assert_eq!(state.quota_used(), 1000);
    }

    #[tokio::test]
    async fn put_checksum_mismatch_releases_reservation() {
        let (router, state, tmp) = setup_app(1000);

        let data = vec![0xDE; 500];
        let wrong = "a".repeat(64);
        let real = blake2b_hex(&data);
        let pack_path = format!("/packs/{}/{}", &real[..2], real);

        let resp = authed_put_with_blake2b(router.clone(), &pack_path, data, &wrong).await;
        assert_status(&resp, StatusCode::CONFLICT);
        assert_eq!(
            state.quota_reserved(),
            0,
            "checksum failure releases reservation"
        );
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn put_no_cl_over_quota_releases_reservation() {
        let (router, state, tmp) = setup_app(1000);

        // No Content-Length header; body exceeds quota, rejected mid-stream.
        let resp = super::authed_put_no_cl(router.clone(), CONFIG_PATH, vec![0xAA; 2000]).await;
        assert_status(&resp, StatusCode::PAYLOAD_TOO_LARGE);
        assert_no_temp_files(tmp.path());
        assert_eq!(
            state.quota_reserved(),
            0,
            "mid-stream failure releases reservation"
        );
    }

    /// Start a PUT whose body never completes and drive it until the handler
    /// has created its temp file. Returns the parked handler future.
    ///
    /// Dropping the returned future is exactly what hyper does to a handler
    /// when the client disconnects mid-upload; this exercises the handler's
    /// cancellation safety, not the network path itself.
    async fn start_stalled_put(
        router: axum::Router,
        path: &str,
        data_dir: &std::path::Path,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        use futures_util::StreamExt;
        use std::time::{Duration, Instant};
        use tower::ServiceExt;

        let stream = futures_util::stream::once(async {
            Ok::<_, std::io::Error>(axum::body::Bytes::from(vec![0x41; 4096]))
        })
        .chain(futures_util::stream::pending());
        let req = axum::http::Request::builder()
            .method("PUT")
            .uri(path)
            .header(
                "Authorization",
                format!("Bearer {}", super::super::test_helpers::TEST_TOKEN),
            )
            .body(Body::from_stream(stream))
            .unwrap();
        let mut fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            Box::pin(async move {
                let _ = router.oneshot(req).await;
            });

        // Poll for the observable state rather than sleeping a fixed time.
        // This doubles as the positive control: it fails if the temp file
        // never appears.
        let deadline = Instant::now() + Duration::from_secs(5);
        while temp_file_count(data_dir) == 0 {
            assert!(
                Instant::now() < deadline,
                "handler never created its temp file"
            );
            let _ = tokio::time::timeout(Duration::from_millis(10), &mut fut).await;
        }
        fut
    }

    #[tokio::test]
    async fn put_cancelled_midstream_leaves_no_temp_file() {
        let (router, _state, tmp) = setup_app(0);

        let fut = start_stalled_put(router, CONFIG_PATH, tmp.path()).await;
        assert_eq!(temp_file_count(tmp.path()), 1);

        drop(fut); // the cancellation

        // TempPath::drop is sync, so no wait is needed.
        assert_no_temp_files(tmp.path());
        assert!(!tmp.path().join("config").exists(), "nothing committed");
    }

    #[tokio::test]
    async fn put_cancelled_midstream_releases_reservation() {
        let (router, state, tmp) = setup_app(1_000_000);

        let fut = start_stalled_put(router, CONFIG_PATH, tmp.path()).await;
        assert!(
            state.quota_reserved() > 0,
            "streaming upload has grown its reservation"
        );

        drop(fut);

        assert_eq!(
            state.quota_reserved(),
            0,
            "cancellation releases reservation"
        );
        assert_eq!(state.quota_used(), 0, "nothing committed");
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

    #[tokio::test]
    async fn range_beyond_file_returns_416() {
        use tower::ServiceExt;
        let (router, _state, _tmp) = setup_app(0);
        authed_put(router.clone(), CONFIG_PATH, vec![0xAB; 100]).await;

        let req = axum::http::Request::builder()
            .method("GET")
            .uri(CONFIG_PATH)
            .header(
                "Authorization",
                format!("Bearer {}", super::super::test_helpers::TEST_TOKEN),
            )
            .header("Range", "bytes=200-299")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_status(&resp, StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test]
    async fn list_omits_temp_files() {
        let (router, _state, tmp) = setup_app(0);
        std::fs::create_dir_all(tmp.path().join("packs/ab")).unwrap();
        std::fs::write(tmp.path().join("packs/ab/pack1"), b"p").unwrap();
        std::fs::write(tmp.path().join("packs/ab/.tmp.pack1.3"), b"x").unwrap();
        std::fs::write(tmp.path().join(".tmp.index.0"), b"x").unwrap();

        let resp = authed_get(router.clone(), "/packs?list").await;
        assert_status(&resp, StatusCode::OK);
        let keys: Vec<String> = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(keys, vec!["packs/ab/pack1"]);

        let resp = authed_get(router, "/?list").await;
        assert_status(&resp, StatusCode::OK);
        let keys: Vec<String> = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(keys, vec!["packs/ab/pack1"]);
    }

    /// One sized listing of `packs/` spans every shard (compact relies on it).
    #[tokio::test]
    async fn sized_listing_of_packs_spans_every_shard() {
        let (router, _state, tmp) = setup_app(0);
        let mut expected = Vec::new();
        for (shard, len) in [("00", 3u64), ("ab", 5), ("ff", 7)] {
            let key = format!("packs/{shard}/{}", shard.repeat(32));
            std::fs::write(tmp.path().join(&key), vec![0u8; len as usize]).unwrap();
            expected.push((key, len));
        }
        std::fs::write(tmp.path().join("packs/ab/.tmp.x.1"), b"x").unwrap();

        for uri in ["/packs/?list&sized", "/packs?list&sized"] {
            let resp = authed_get(router.clone(), uri).await;
            assert_status(&resp, StatusCode::OK);
            let listed: Vec<vykar_protocol::SizedKey> =
                serde_json::from_slice(&body_bytes(resp).await).unwrap();
            let mut listed: Vec<(String, u64)> =
                listed.into_iter().map(|k| (k.key, k.size)).collect();
            listed.sort();
            assert_eq!(listed, expected, "{uri}");
        }
    }

    /// Index objects are content-named: a repeated PUT and a DELETE are
    /// allowed in append-only mode, as overwriting `index` always was.
    #[tokio::test]
    async fn append_only_allows_index_object_overwrite_and_delete() {
        let (router, _state, tmp) = setup_app_append_only(0);
        let key = format!("index.d/{}", "5a".repeat(32));
        let uri = format!("/{key}");

        let resp = authed_put(router.clone(), &uri, b"obj".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);
        let resp = authed_put(router.clone(), &uri, b"obj".to_vec()).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        let resp = authed_delete(router.clone(), &uri).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert!(!tmp.path().join(&key).exists());

        for bad in ["/index.d/abc", "/index.d/x/y"] {
            let resp = authed_put(router.clone(), bad, b"obj".to_vec()).await;
            assert_status(&resp, StatusCode::BAD_REQUEST);
        }
        let upper = format!("/index.d/{}", "5A".repeat(32));
        let resp = authed_put(router, &upper, b"obj".to_vec()).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn append_only_delete_temp_file_succeeds() {
        let (router, _state, tmp) = setup_app_append_only(0);
        std::fs::write(tmp.path().join(".tmp.config.7"), b"partial").unwrap();

        let resp = authed_delete(router, "/.tmp.config.7").await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert!(!tmp.path().join(".tmp.config.7").exists());
    }

    #[tokio::test]
    async fn append_only_delete_committed_forbidden() {
        let (router, _state, tmp) = setup_app_append_only(0);
        std::fs::write(tmp.path().join("config"), b"cfg").unwrap();

        let resp = authed_delete(router, "/config").await;
        assert_status(&resp, StatusCode::FORBIDDEN);
        assert!(tmp.path().join("config").exists(), "config must survive");
    }

    #[tokio::test]
    async fn put_temp_named_key_rejected() {
        // A temp-looking name must not be committable: append-only DELETE
        // treats temp names as uncommitted debris, so a PUT-able temp-named
        // object would be deletable committed data.
        let (router, _state, tmp) = setup_app(0);
        let resp = authed_put(router.clone(), "/snapshots/.tmp.evil.1", b"data".to_vec()).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        // A trailing slash resolves to the same file and must not bypass the
        // rejection.
        let resp = authed_put(router, "/snapshots/.tmp.evil.1/", b"data".to_vec()).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        assert!(!tmp.path().join("snapshots/.tmp.evil.1").exists());
    }

    #[tokio::test]
    async fn append_only_delete_namespaced_temp_debris() {
        // Interrupted snapshot uploads leave `snapshots/.tmp.<id>.<n>` debris;
        // it must stay cleanable in append-only mode (PUT can never commit
        // such a name, so it is always uncommitted).
        let (router, _state, tmp) = setup_app_append_only(0);
        std::fs::write(tmp.path().join("snapshots").join(".tmp.abc123.5"), b"x").unwrap();

        let resp = authed_delete(router, "/snapshots/.tmp.abc123.5").await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert!(!tmp.path().join("snapshots").join(".tmp.abc123.5").exists());
    }

    #[tokio::test]
    async fn delete_temp_file_non_append_only() {
        // Latent-bug fix: DELETE of a `.tmp.*` file used to return 400 because
        // the strict key schema rejects temp names.
        let (router, _state, tmp) = setup_app(0);
        std::fs::write(tmp.path().join(".tmp.index.0"), b"partial").unwrap();

        let resp = authed_delete(router, "/.tmp.index.0").await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert!(!tmp.path().join(".tmp.index.0").exists());
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

    /// Compute BLAKE2b-256 hex of data (for test convenience).
    fn blake2b_hex(data: &[u8]) -> String {
        use blake2::{Blake2b256, Digest};
        let mut hasher = Blake2b256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    /// The BLAKE3 pack ID a client would send for `data`.
    ///
    /// Deliberately the *client's* `PackId::compute` rather than the server's
    /// own hasher: that makes these tests assert client/server agreement
    /// instead of restating the handler's implementation.
    fn blake3_hex(data: &[u8]) -> String {
        vykar_types::pack_id::PackId::compute(data, vykar_types::hash::HashAlgorithm::Blake3)
            .to_hex()
    }

    /// Send an authenticated PUT with an X-Content-BLAKE2b header.
    async fn authed_put_with_blake2b(
        router: axum::Router,
        path: &str,
        body: Vec<u8>,
        checksum: &str,
    ) -> axum::response::Response {
        authed_put_with_digests(router, path, body, &[("X-Content-BLAKE2b", checksum)]).await
    }

    /// Send an authenticated PUT with an arbitrary set of digest headers, so
    /// the "both present" and "BLAKE3 only" cases are expressible.
    async fn authed_put_with_digests(
        router: axum::Router,
        path: &str,
        body: Vec<u8>,
        digests: &[(&str, &str)],
    ) -> axum::response::Response {
        use tower::ServiceExt;
        let mut builder = axum::http::Request::builder()
            .method("PUT")
            .uri(path)
            .header(
                "Authorization",
                format!("Bearer {}", super::super::test_helpers::TEST_TOKEN),
            )
            .header("Content-Length", body.len().to_string());
        for (name, value) in digests {
            builder = builder.header(*name, *value);
        }
        let req = builder.body(Body::from(body)).unwrap();
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
            body_text.contains("X-Content-BLAKE2b or X-Content-BLAKE3 header required"),
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

    // -----------------------------------------------------------------------
    // BLAKE3 checksum verification tests (repository format v3)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn put_pack_with_valid_blake3_checksum_succeeds() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0xDE; 512];
        let checksum = blake3_hex(&data);
        let pack_path = format!("/packs/{}/{}", &checksum[..2], checksum);

        let resp =
            authed_put_with_digests(router, &pack_path, data, &[("X-Content-BLAKE3", &checksum)])
                .await;
        assert_status(&resp, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn put_pack_with_wrong_blake3_checksum_returns_409() {
        let (router, _state, tmp) = setup_app(0);
        let data = vec![0xDE; 512];
        let real = blake3_hex(&data);
        let wrong = "a".repeat(64);
        let pack_path = format!("/packs/{}/{}", &real[..2], real);

        let resp = authed_put_with_digests(
            router.clone(),
            &pack_path,
            data,
            &[("X-Content-BLAKE3", &wrong)],
        )
        .await;
        assert_status(&resp, StatusCode::CONFLICT);
        let body_text = String::from_utf8_lossy(&body_bytes(resp).await).into_owned();
        assert!(
            body_text.contains("BLAKE3 checksum mismatch"),
            "got: {body_text}"
        );

        let resp = authed_get(router, &pack_path).await;
        assert_status(&resp, StatusCode::NOT_FOUND);
        assert_no_temp_files(tmp.path());
    }

    /// A BLAKE2b digest must not be accepted under the BLAKE3 header name, and
    /// vice versa — otherwise a client/server algorithm mismatch would pass
    /// verification and store a pack under the wrong name.
    #[tokio::test]
    async fn digest_headers_are_not_interchangeable() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0x5A; 256];
        let b3 = blake3_hex(&data);
        let b2 = blake2b_hex(&data);
        let pack_path = format!("/packs/{}/{}", &b3[..2], b3);

        // BLAKE2b digest sent under the BLAKE3 header.
        let resp = authed_put_with_digests(
            router.clone(),
            &pack_path,
            data.clone(),
            &[("X-Content-BLAKE3", &b2)],
        )
        .await;
        assert_status(&resp, StatusCode::CONFLICT);

        // BLAKE3 digest sent under the BLAKE2b header.
        let resp =
            authed_put_with_digests(router, &pack_path, data, &[("X-Content-BLAKE2b", &b3)]).await;
        assert_status(&resp, StatusCode::CONFLICT);
    }

    /// Both headers at once is ambiguous — which one is authoritative? — so it
    /// is refused rather than resolved by precedence.
    #[tokio::test]
    async fn put_with_both_digest_headers_returns_400() {
        let (router, _state, tmp) = setup_app(0);
        let data = vec![0xDE; 512];
        let b2 = blake2b_hex(&data);
        let b3 = blake3_hex(&data);
        let pack_path = format!("/packs/{}/{}", &b3[..2], b3);

        let resp = authed_put_with_digests(
            router,
            &pack_path,
            data,
            &[("X-Content-BLAKE2b", &b2), ("X-Content-BLAKE3", &b3)],
        )
        .await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        let body_text = String::from_utf8_lossy(&body_bytes(resp).await).into_owned();
        assert!(body_text.contains("mutually exclusive"), "got: {body_text}");
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn put_with_malformed_blake3_checksum_returns_400() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0xAB; 128];

        let resp =
            authed_put_with_digests(router, CONFIG_PATH, data, &[("X-Content-BLAKE3", "abcd")])
                .await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        let body_text = String::from_utf8_lossy(&body_bytes(resp).await).into_owned();
        assert!(
            body_text.contains("X-Content-BLAKE3") && body_text.contains("64 hex characters"),
            "got: {body_text}"
        );
    }

    // -----------------------------------------------------------------------
    // Conditional GET of `index`
    // -----------------------------------------------------------------------

    const INDEX_PATH: &str = "/index";

    async fn authed_get_with_headers(
        router: axum::Router,
        path: &str,
        headers: &[(&str, &str)],
    ) -> axum::response::Response {
        use tower::ServiceExt;
        let mut builder = axum::http::Request::builder()
            .method("GET")
            .uri(path)
            .header(
                "Authorization",
                format!("Bearer {}", super::super::test_helpers::TEST_TOKEN),
            );
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        router
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn get_index_if_none_match(router: axum::Router, hex: &str) -> axum::response::Response {
        let inm = format!("\"{hex}\"");
        authed_get_with_headers(router, INDEX_PATH, &[("If-None-Match", &inm)]).await
    }

    async fn put_index(router: axum::Router, data: &[u8]) -> String {
        let hex = blake3_hex(data);
        let resp = authed_put_with_digests(
            router,
            INDEX_PATH,
            data.to_vec(),
            &[("X-Content-BLAKE3", &hex)],
        )
        .await;
        assert!(resp.status().is_success(), "PUT /index: {}", resp.status());
        hex
    }

    #[tokio::test]
    async fn index_get_with_matching_validator_is_304() {
        let (router, _state, _tmp) = setup_app(0);
        let hex = put_index(router.clone(), &[0x11; 300]).await;

        let resp = get_index_if_none_match(router, &hex).await;
        assert_status(&resp, StatusCode::NOT_MODIFIED);
        let etag = resp.headers().get("ETag").unwrap().to_str().unwrap();
        assert_eq!(etag, format!("\"{hex}\""));
        assert!(body_bytes(resp).await.is_empty());
    }

    #[tokio::test]
    async fn index_get_with_other_validator_is_200() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0x11; 300];
        put_index(router.clone(), &data).await;

        let resp = get_index_if_none_match(router, &"0".repeat(64)).await;
        assert_status(&resp, StatusCode::OK);
        assert_eq!(body_bytes(resp).await, data);
    }

    #[tokio::test]
    async fn index_without_record_is_200() {
        // A PUT without a digest leaves nothing to compare against.
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0x22; 300];
        let resp = authed_put(router.clone(), INDEX_PATH, data.clone()).await;
        assert_status(&resp, StatusCode::CREATED);
        let resp = get_index_if_none_match(router, &blake3_hex(&data)).await;
        assert_status(&resp, StatusCode::OK);

        // A file the server did not write (the restart case).
        let (router, _state, tmp) = setup_app(0);
        std::fs::write(tmp.path().join("index"), &data).unwrap();
        let resp = get_index_if_none_match(router, &blake3_hex(&data)).await;
        assert_status(&resp, StatusCode::OK);
        assert_eq!(body_bytes(resp).await, data);
    }

    #[tokio::test]
    async fn index_replaced_on_disk_is_200() {
        let (router, _state, tmp) = setup_app(0);
        let hex = put_index(router.clone(), &[0x33; 300]).await;

        let staged = tmp.path().join("staged-index");
        std::fs::write(&staged, vec![0x44; 300]).unwrap();
        // Same length, so off Unix only the mtime tells the files apart.
        std::fs::File::options()
            .write(true)
            .open(&staged)
            .unwrap()
            .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(2))
            .unwrap();
        std::fs::rename(&staged, tmp.path().join("index")).unwrap();

        let resp = get_index_if_none_match(router, &hex).await;
        assert_status(&resp, StatusCode::OK);
        assert_eq!(body_bytes(resp).await, vec![0x44; 300]);
    }

    #[tokio::test]
    async fn index_put_with_blake2b_clears_record() {
        // An older client's PUT: verified, but not a validator newer
        // clients could ever send.
        let (router, _state, _tmp) = setup_app(0);
        let first = put_index(router.clone(), &[0x55; 300]).await;

        let data = vec![0x66; 300];
        let resp = authed_put_with_blake2b(
            router.clone(),
            INDEX_PATH,
            data.clone(),
            &blake2b_hex(&data),
        )
        .await;
        assert_status(&resp, StatusCode::NO_CONTENT);

        for hex in [first, blake2b_hex(&data), blake3_hex(&data)] {
            let resp = get_index_if_none_match(router.clone(), &hex).await;
            assert_status(&resp, StatusCode::OK);
        }
    }

    /// A rejected PUT leaves the previous file, and so its record, in place.
    #[tokio::test]
    async fn index_put_digest_mismatch_keeps_the_previous_record() {
        let (router, _state, _tmp) = setup_app(0);
        let hex = put_index(router.clone(), &[0x77; 300]).await;

        let resp =
            authed_put_with_blake2b(router.clone(), INDEX_PATH, vec![0x88; 300], &"a".repeat(64))
                .await;
        assert_status(&resp, StatusCode::CONFLICT);

        let resp = get_index_if_none_match(router, &hex).await;
        assert_status(&resp, StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn if_none_match_on_other_keys_is_ignored() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0x99; 100];
        let hex = blake2b_hex(&data);
        let resp = authed_put_with_blake2b(router.clone(), CONFIG_PATH, data, &hex).await;
        assert_status(&resp, StatusCode::CREATED);

        let inm = format!("\"{hex}\"");
        let resp = authed_get_with_headers(router, CONFIG_PATH, &[("If-None-Match", &inm)]).await;
        assert_status(&resp, StatusCode::OK);
    }

    #[tokio::test]
    async fn range_read_of_index_ignores_if_none_match() {
        let (router, _state, _tmp) = setup_app(0);
        let hex = put_index(router.clone(), &[0xAA; 300]).await;

        let inm = format!("\"{hex}\"");
        let resp = authed_get_with_headers(
            router,
            INDEX_PATH,
            &[("If-None-Match", &inm), ("Range", "bytes=0-9")],
        )
        .await;
        assert_status(&resp, StatusCode::PARTIAL_CONTENT);
    }

    #[tokio::test]
    async fn deleted_index_is_404_not_304() {
        let (router, _state, _tmp) = setup_app(0);
        let hex = put_index(router.clone(), &[0xBB; 300]).await;
        let resp = authed_delete(router.clone(), INDEX_PATH).await;
        assert_status(&resp, StatusCode::NO_CONTENT);

        let resp = get_index_if_none_match(router, &hex).await;
        assert_status(&resp, StatusCode::NOT_FOUND);
    }

    /// Consistency check, not a race test: after two overlapping PUTs the
    /// record describes whichever file is left on disk, and only that one.
    #[tokio::test]
    async fn index_record_matches_the_file_left_on_disk() {
        let (router, _state, tmp) = setup_app(0);
        let a = vec![0xC1; 300];
        let b = vec![0xC2; 300];
        let (hex_a, hex_b) =
            tokio::join!(put_index(router.clone(), &a), put_index(router.clone(), &b));

        let on_disk = std::fs::read(tmp.path().join("index")).unwrap();
        let (kept, other) = if on_disk == a {
            (hex_a, hex_b)
        } else {
            assert_eq!(on_disk, b);
            (hex_b, hex_a)
        };
        let resp = get_index_if_none_match(router.clone(), &kept).await;
        assert_status(&resp, StatusCode::NOT_MODIFIED);
        let resp = get_index_if_none_match(router, &other).await;
        assert_status(&resp, StatusCode::OK);
    }

    /// The old-client path must keep working against a new server: this is the
    /// `old client + v2 repo + new server` row of the compatibility matrix.
    #[tokio::test]
    async fn old_style_blake2b_pack_upload_still_succeeds() {
        let (router, _state, _tmp) = setup_app(0);
        let data = vec![0x77; 1024];
        let checksum = blake2b_hex(&data);
        let pack_path = format!("/packs/{}/{}", &checksum[..2], checksum);

        let resp = authed_put_with_blake2b(router, &pack_path, data, &checksum).await;
        assert_status(&resp, StatusCode::CREATED);
    }
}
