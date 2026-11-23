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
    /// With `list&sized`: add `mtime_ms` to every entry.
    pub mtime: Option<String>,
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
        return list_keys(state, &key, query.sized.is_some(), query.mtime.is_some()).await;
    }

    let file_path = state
        .file_path(&key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    // Check for Range header
    if let Some(range_header) = headers.get("Range").and_then(|v| v.to_str().ok()) {
        return handle_range_read(&file_path, range_header, &key).await;
    }

    if crate::index_etag::is_index_key(&key)
        && let Some(resp) = index_not_modified(&state, &file_path, &headers)
    {
        return Ok(resp);
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
/// `If-None-Match: *` makes it a create-only PUT: 412 when the key exists,
/// decided atomically by a no-clobber rename, and checked before append-only
/// and quota (a create-only PUT never overwrites, so "exists" is the answer
/// in every mode). Any other `If-None-Match` value is a 400. 501 when the
/// data directory's filesystem cannot rename without replacing.
///
/// Two keys have rules of their own in every mode
/// ([`vykar_protocol::put_rule`]), checked after the body and any digest
/// header, and published with a no-clobber rename:
/// - `index.d/<id>`: 409 unless `<id>` is the BLAKE3 of the body. Onto an
///   existing object nothing is stored (the mtime is not renewed): 204.
/// - `index.h/<gen>`: write-once. Onto an existing head nothing is stored:
///   204 when the bytes are the stored ones, else 409.
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
    let conditional = match headers.get(axum::http::header::IF_NONE_MATCH) {
        None => false,
        Some(v) if v.to_str().is_ok_and(|s| s.trim() == "*") => true,
        Some(_) => {
            return Err(ServerError::BadRequest(
                "If-None-Match on PUT supports only *".into(),
            ));
        }
    };

    let file_path = state
        .file_path(&key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
    let is_index = crate::index_etag::is_index_key(&key);
    let rule = vykar_protocol::put_rule(&key);

    let existing_meta = match tokio::fs::metadata(&file_path).await {
        Ok(meta) => Some(meta),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(ServerError::from(e)),
    };

    // Before append-only, quota and the body: the fast path of create-only.
    // The no-clobber rename below decides a race with a concurrent creator.
    if conditional && existing_meta.is_some() {
        return Err(ServerError::PreconditionFailed("object exists".into()));
    }

    // Append-only: an existing object may be replaced only where the key-class
    // table allows it (`vykar_protocol::KeyClass`). Keys with a put rule are
    // never replaced in any mode; their outcome on an existing key is decided
    // after the body (identical bytes are acknowledged), so append-only adds
    // nothing for them.
    if state.inner.config.append_only
        && rule == vykar_protocol::PutRule::Plain
        && existing_meta.is_some()
        && !vykar_protocol::append_only_allows(&key, vykar_protocol::Verb::Overwrite)
    {
        return Err(ServerError::Forbidden(
            "append-only: cannot overwrite immutable object".into(),
        ));
    }

    // Track old file size for quota accounting. Keys with a put rule are never
    // replaced: the reservation covers the whole body and is committed only
    // when this request creates the object.
    let old_size = if rule == vykar_protocol::PutRule::Plain {
        existing_meta.as_ref().map_or(0, std::fs::Metadata::len)
    } else {
        0
    };

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
    // BLAKE3 of the body for the put rule, unless the header hasher already is one.
    let header_is_blake3 = matches!(expected_digest, Some((HashAlgorithm::Blake3, _)));
    let mut rule_hasher = (rule != vykar_protocol::PutRule::Plain && !header_is_blake3)
        .then(|| Hasher256::new(HashAlgorithm::Blake3));

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
    let (data_len, hasher, rule_hasher) = {
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
                if let Some(ref mut h) = rule_hasher {
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

        (data_len, hasher, rule_hasher)
    };

    // Validate Content-Length if it was present
    if let Some(content_length) = content_length
        && data_len != content_length
    {
        return Err(ServerError::BadRequest(format!(
            "upload size mismatch: Content-Length {content_length}, received {data_len}"
        )));
    }

    // Verify the content digest if a header was present.
    let mut header_hex = None;
    if let (Some((algo, expected)), Some(hasher)) = (&expected_digest, hasher) {
        let actual_hex = hasher.finalize_hex();
        if actual_hex != *expected {
            return Err(digest_mismatch(*algo, expected, &actual_hex));
        }
        header_hex = Some(actual_hex);
    }

    // The body's BLAKE3, for keys with a put rule.
    let body_blake3 = match rule {
        vykar_protocol::PutRule::Plain => None,
        _ if header_is_blake3 => header_hex,
        _ => rule_hasher.map(Hasher256::finalize_hex),
    };
    if rule == vykar_protocol::PutRule::NamedByBlake3 {
        let actual = body_blake3
            .as_deref()
            .expect("a BLAKE3 is computed for every key with a put rule");
        if vykar_protocol::index_object_name(&key) != Some(actual) {
            return Err(ServerError::Conflict(format!(
                "index object name mismatch: {key} is not the BLAKE3 of its body ({actual})"
            )));
        }
    }

    // Atomic rename temp → final path (file handle already closed). `persist`
    // consumes the owner, so nothing is unlinked afterwards; on failure the
    // temp is still dropped (and unlinked) with the error.
    if rule != vykar_protocol::PutRule::Plain {
        match publish_new(temp_path, &file_path, conditional)? {
            Published::Created => {}
            Published::Exists if conditional => {
                return Err(ServerError::PreconditionFailed("object exists".into()));
            }
            // Same name, so the same bytes: nothing to store. The reservation
            // is released by its Drop.
            Published::Exists if rule == vykar_protocol::PutRule::NamedByBlake3 => {
                return ack_committed(&file_path).await;
            }
            Published::Exists => {
                let stored = digest_of(&file_path, HashAlgorithm::Blake3).await?;
                if stored.is_some() && stored == body_blake3 {
                    return ack_committed(&file_path).await;
                }
                return Err(ServerError::Conflict(format!(
                    "index head {key} exists with other bytes; heads are write-once"
                )));
            }
        }
    } else if is_index {
        // Only a verified BLAKE3 can answer a conditional GET (it is the
        // digest clients cache the index under); any other PUT clears the
        // record. The lock keeps other `index` writes out
        // between the rename and the stat, so the fingerprint is this file's.
        let digest_hex = match &expected_digest {
            Some((HashAlgorithm::Blake3, hex)) => Some(hex.clone()),
            _ => None,
        };
        let mut slot = state.lock_index_validator();
        persist_temp(temp_path, &file_path, conditional)?;
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
        persist_temp(temp_path, &file_path, conditional)?;
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

/// Rename a finished upload into place: replacing, or for a create-only PUT
/// with a no-clobber rename. On error the temp file is dropped (unlinked) and
/// the caller's quota reservation released with it.
fn persist_temp(
    temp_path: tempfile::TempPath,
    file_path: &std::path::Path,
    conditional: bool,
) -> Result<(), ServerError> {
    if !conditional {
        return temp_path
            .persist(file_path)
            .map(drop)
            .map_err(|e| ServerError::from(e.error));
    }
    match temp_path.persist_noclobber(file_path) {
        Ok(()) => Ok(()),
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(ServerError::PreconditionFailed("object exists".into()))
        }
        Err(e) if vykar_common::fs::is_no_clobber_unsupported(&e.error) => {
            Err(ServerError::NotImplemented(
                "conditional PUT: the data directory's filesystem cannot create without \
                 replacing"
                    .into(),
            ))
        }
        Err(e) => Err(ServerError::from(e.error)),
    }
}

/// What publishing an upload under a never-replaced key did.
enum Published {
    /// This request created the object.
    Created,
    /// An object was already there; nothing was written.
    Exists,
}

/// Publish under a key that is never replaced (`PutRule::NamedByBlake3` or
/// `PutRule::WriteOnce`) with a no-clobber rename. On a filesystem that cannot
/// rename without replacing, a conditional PUT gets 501 as before; a plain PUT
/// falls back to "exists -> Exists, else a replacing rename", which a
/// concurrent creator of the same key can race: the later rename then replaces
/// the earlier object, and both requests are accounted as creations, so the
/// quota counts the key twice until usage is next recomputed. On every path
/// but `Created` the temp file is dropped (unlinked).
fn publish_new(
    temp_path: tempfile::TempPath,
    file_path: &std::path::Path,
    conditional: bool,
) -> Result<Published, ServerError> {
    match temp_path.persist_noclobber(file_path) {
        Ok(()) => Ok(Published::Created),
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(Published::Exists),
        Err(e) if vykar_common::fs::is_no_clobber_unsupported(&e.error) => {
            if conditional {
                return Err(ServerError::NotImplemented(
                    "conditional PUT: the data directory's filesystem cannot create without \
                     replacing"
                        .into(),
                ));
            }
            if file_path.exists() {
                return Ok(Published::Exists);
            }
            e.path
                .persist(file_path)
                .map_err(|e| ServerError::from(e.error))?;
            Ok(Published::Created)
        }
        Err(e) => Err(ServerError::from(e.error)),
    }
}

/// DELETE /{*path} — delete object. Append-only: per the key-class table.
pub async fn delete_object(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Response, ServerError> {
    let is_temp = vykar_protocol::is_temp_file(&key);

    // Append-only: only the classes the key-class table allows to be deleted
    // (`vykar_protocol::KeyClass`). Checked before path resolution.
    if state.inner.config.append_only
        && !vykar_protocol::append_only_allows(&key, vykar_protocol::Verb::Delete)
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

/// Acknowledge an object some request already committed under `file_path`.
/// Its contents were fdatasync'ed before their rename, but the rename is only
/// durable once the directory is fsync'ed — which that request may not have
/// done (it failed) or finished (it is still running). Sync it here, so no
/// `204` is sent before the object is durable under its final name.
pub(crate) async fn ack_committed(file_path: &std::path::Path) -> Result<Response, ServerError> {
    let parent = file_path
        .parent()
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
    fsync_dir_async(parent.to_path_buf())
        .await
        .map_err(ServerError::from)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Digest of the committed object at `path`, or `None` if there is none.
pub(crate) async fn digest_of(
    path: &std::path::Path,
    algo: HashAlgorithm,
) -> Result<Option<String>, ServerError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || match std::fs::File::open(&path) {
        Ok(file) => hash_reader(&file, algo).map(|(_, digest)| Some(digest)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::from)
}

pub(crate) fn hash_reader(
    mut reader: impl std::io::Read,
    algo: HashAlgorithm,
) -> std::io::Result<(u64, String)> {
    let mut hasher = Hasher256::new(algo);
    let mut buf = vec![0u8; 1024 * 1024];
    let mut len: u64 = 0;
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        hasher.update(buf.get(..n).expect("n <= buf.len()"));
        len += n as u64;
    }
    Ok((len, hasher.finalize_hex()))
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
    if let Some(len) = content_length
        && len > MAX_SIZES_BODY_BYTES as u64
    {
        return Err(too_large(len));
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

async fn list_keys(
    state: AppState,
    prefix: &str,
    sized: bool,
    mtime: bool,
) -> Result<Response, ServerError> {
    let dir_path = state
        .file_path(prefix)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    let prefix_owned = prefix.to_string();
    let keys =
        tokio::task::spawn_blocking(move || list_files_recursive(&dir_path, &prefix_owned, mtime))
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

/// A file's mtime in ms since the Unix epoch, for `?list&sized&mtime`;
/// `None` when the platform does not report one or it predates the epoch.
pub(crate) fn mtime_ms(meta: &std::fs::Metadata) -> Option<u64> {
    let since = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    u64::try_from(since.as_millis()).ok()
}

/// Sizes follow symlinks, matching what HEAD reports for the same key.
/// `mtime` fills [`SizedKey::mtime_ms`]; without it the field stays `None`
/// and is not serialized.
fn list_files_recursive(dir: &std::path::Path, prefix: &str, mtime: bool) -> Vec<SizedKey> {
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
                keys.extend(list_files_recursive(&path, &full_key, mtime));
            } else if !vykar_protocol::is_temp_file(&full_key) {
                keys.push(SizedKey {
                    key: full_key,
                    size: meta.len(),
                    mtime_ms: if mtime { mtime_ms(&meta) } else { None },
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
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
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
        use super::{SIZES_BODY_INITIAL_CAPACITY, sizes_body_initial_capacity as cap_for};
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

    /// One sample key per class, mapped by hand rather than with `key_class`.
    fn class_samples() -> Vec<(String, vykar_protocol::KeyClass)> {
        use vykar_protocol::KeyClass::*;
        let hex = "0123456789abcdef".repeat(4);
        vec![
            ("snapshots/abc".into(), WriteOnce),
            ("config".into(), WriteOnce),
            ("keys/k".into(), WriteOnce),
            ("pending_index/p".into(), WriteOnce),
            ("manifest".into(), LegacyMutable),
            (pack_key(0x7e), WriteOnce),
            ("index.h/7".into(), WriteOnce),
            (format!("packmaps/{hex}"), WriteOnce),
            (format!("index.d/{}", blake3_hex(b"one")), ContentAddressed),
            ("index".into(), LegacyMutable),
            ("index.gen".into(), LegacyMutable),
            ("locks/l.json".into(), Ephemeral),
            ("sessions/s.json".into(), Ephemeral),
            ("sessions/s.7.json".into(), Ephemeral),
            ("snapshots/.tmp.x.1".into(), Temp),
        ]
    }

    async fn put_with_blake3(
        router: axum::Router,
        key: &str,
        body: &[u8],
    ) -> axum::response::Response {
        let digest = blake3_hex(body);
        authed_put_with_digests(
            router,
            &format!("/{key}"),
            body.to_vec(),
            &[("X-Content-BLAKE3", &digest)],
        )
        .await
    }

    /// Create, overwrite, delete and batch-delete one sample key of every
    /// class. The expected outcome comes from `KeyClass::append_only_allows`.
    async fn run_class_matrix(append_only: bool) {
        use vykar_protocol::{KeyClass, Verb};
        for (key, class) in class_samples() {
            let (router, _state, tmp) = setup_app_with(0, append_only);
            let path = tmp.path().join(&key);
            let allows = |verb| !append_only || class.append_only_allows(verb);
            let write_direct = |bytes: &[u8]| {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, bytes).unwrap();
            };
            let ctx = format!("{key} ({class:?}, append_only={append_only})");

            // Create. Temp names are never committable, in any mode.
            let resp = put_with_blake3(router.clone(), &key, b"one").await;
            if class == KeyClass::Temp {
                assert_status(&resp, StatusCode::BAD_REQUEST);
                write_direct(b"one");
            } else {
                assert!(allows(Verb::Create), "{ctx}");
                assert_eq!(resp.status(), StatusCode::CREATED, "{ctx}");
            }

            // Overwrite. Keys with a put rule are never replaced, in any mode:
            // an index object is re-sent with its own bytes (acknowledged
            // without a write), a head with other bytes (refused).
            let rule = vykar_protocol::put_rule(&key);
            let second: &[u8] = if rule == vykar_protocol::PutRule::NamedByBlake3 {
                b"one"
            } else {
                b"two"
            };
            let resp = put_with_blake3(router.clone(), &key, second).await;
            let expected = if class == KeyClass::Temp {
                StatusCode::BAD_REQUEST
            } else if rule == vykar_protocol::PutRule::WriteOnce {
                StatusCode::CONFLICT
            } else if allows(Verb::Overwrite) {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::FORBIDDEN
            };
            assert_eq!(resp.status(), expected, "overwrite {ctx}");
            let stored = std::fs::read(&path).unwrap();
            let want: &[u8] = if expected == StatusCode::NO_CONTENT {
                second
            } else {
                b"one"
            };
            assert_eq!(stored, want, "overwrite {ctx}");

            // Delete.
            let resp = authed_delete(router.clone(), &format!("/{key}")).await;
            if allows(Verb::Delete) {
                assert_eq!(resp.status(), StatusCode::NO_CONTENT, "delete {ctx}");
                assert!(!path.exists(), "delete {ctx}");
            } else {
                assert_eq!(resp.status(), StatusCode::FORBIDDEN, "delete {ctx}");
                assert!(path.exists(), "delete {ctx}");
            }

            // Batch delete.
            if !path.exists() {
                write_direct(b"one");
            }
            let body = serde_json::to_vec(&[&key]).unwrap();
            let resp = authed_post(router, "/?batch-delete", body).await;
            if allows(Verb::Delete) {
                assert_eq!(resp.status(), StatusCode::NO_CONTENT, "batch {ctx}");
                assert!(!path.exists(), "batch {ctx}");
            } else {
                assert_eq!(resp.status(), StatusCode::FORBIDDEN, "batch {ctx}");
                assert!(path.exists(), "batch {ctx}");
            }
        }
    }

    #[tokio::test]
    async fn append_only_matrix_follows_the_key_class_table() {
        run_class_matrix(true).await;
    }

    #[tokio::test]
    async fn without_append_only_every_class_is_mutable() {
        run_class_matrix(false).await;
    }

    /// Slashes around a key resolve to the same file, so they must not change
    /// its class.
    #[tokio::test]
    async fn append_only_ignores_a_trailing_slash() {
        let (router, _state, tmp) = setup_app_append_only(0);
        std::fs::write(tmp.path().join("snapshots/abc"), b"s").unwrap();
        let resp = authed_put(router.clone(), "/snapshots/abc/", b"x".to_vec()).await;
        assert_status(&resp, StatusCode::FORBIDDEN);
        let resp = authed_delete(router, "/snapshots/abc/").await;
        assert_status(&resp, StatusCode::FORBIDDEN);
        assert_eq!(
            std::fs::read(tmp.path().join("snapshots/abc")).unwrap(),
            b"s"
        );
    }

    #[tokio::test]
    async fn v6_keys_round_trip() {
        let (router, _state, _tmp) = setup_app(0);
        let hex = "0123456789abcdef".repeat(4);
        let map = format!("packmaps/{hex}");
        for key in ["index.h/1", map.as_str()] {
            let resp =
                authed_put(router.clone(), &format!("/{key}"), key.as_bytes().to_vec()).await;
            assert_status(&resp, StatusCode::CREATED);
            let resp = authed_get(router.clone(), &format!("/{key}")).await;
            assert_status(&resp, StatusCode::OK);
            assert_eq!(body_bytes(resp).await, key.as_bytes());
        }
        for dir in ["index.h", "packmaps"] {
            let resp = authed_get(router.clone(), &format!("/{dir}?list")).await;
            assert_status(&resp, StatusCode::OK);
            let keys: Vec<String> = serde_json::from_slice(&body_bytes(resp).await).unwrap();
            assert_eq!(keys.len(), 1, "{dir}: {keys:?}");
            assert!(keys[0].starts_with(&format!("{dir}/")), "{dir}: {keys:?}");
        }
        let upper = format!("/packmaps/{}", hex.to_uppercase());
        for bad in ["/index.h/01", upper.as_str()] {
            let resp = authed_put(router.clone(), bad, b"x".to_vec()).await;
            assert_status(&resp, StatusCode::BAD_REQUEST);
        }
    }

    /// Index objects are content-named: a repeated PUT of the same bytes is
    /// acknowledged in append-only mode, a DELETE is refused.
    #[tokio::test]
    async fn append_only_index_object_reput_is_acked_and_delete_refused() {
        let (router, _state, tmp) = setup_app_append_only(0);
        let key = format!("index.d/{}", blake3_hex(b"obj"));
        let uri = format!("/{key}");

        let resp = authed_put(router.clone(), &uri, b"obj".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);
        let resp = authed_put(router.clone(), &uri, b"obj".to_vec()).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        let resp = authed_delete(router.clone(), &uri).await;
        assert_status(&resp, StatusCode::FORBIDDEN);
        assert_eq!(std::fs::read(tmp.path().join(&key)).unwrap(), b"obj");

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
    // ── Conditional PUT (`If-None-Match: *`) ──────────────────────────────

    async fn put_if_none_match(
        router: axum::Router,
        path: &str,
        value: &str,
        body: Vec<u8>,
    ) -> axum::response::Response {
        use tower::ServiceExt;
        let req = authed_request("PUT", path)
            .header("Content-Length", body.len().to_string())
            .header("If-None-Match", value)
            .body(Body::from(body))
            .unwrap();
        router.oneshot(req).await.unwrap()
    }

    #[tokio::test]
    async fn conditional_put_creates_then_refuses_with_412() {
        let (router, state, tmp) = setup_app(1_000_000);
        let resp = put_if_none_match(router.clone(), "/locks/a", "*", b"first".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);
        let used = state.quota_used();

        let resp = put_if_none_match(router.clone(), "/locks/a", "*", b"second".to_vec()).await;
        assert_status(&resp, StatusCode::PRECONDITION_FAILED);
        assert_eq!(std::fs::read(tmp.path().join("locks/a")).unwrap(), b"first");
        assert_eq!(state.quota_used(), used, "a 412 commits nothing");
        assert_eq!(state.quota_reserved(), 0);
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn conditional_put_rejects_other_if_none_match_values() {
        let (router, _state, tmp) = setup_app(0);
        let resp = put_if_none_match(router, "/locks/a", "\"abc\"", b"x".to_vec()).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        assert!(!tmp.path().join("locks/a").exists());
    }

    /// Create-only answers "exists" before the append-only refusal.
    #[tokio::test]
    async fn conditional_put_on_existing_write_once_key_is_412_in_append_only() {
        let (router, _state, tmp) = setup_app_append_only(0);
        let resp = authed_put(router.clone(), CONFIG_PATH, b"cfg".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);
        let resp = put_if_none_match(router.clone(), CONFIG_PATH, "*", b"other".to_vec()).await;
        assert_status(&resp, StatusCode::PRECONDITION_FAILED);
        assert_eq!(std::fs::read(tmp.path().join("config")).unwrap(), b"cfg");
        // A plain overwrite is still refused as before.
        let resp = authed_put(router, CONFIG_PATH, b"other".to_vec()).await;
        assert_status(&resp, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn conditional_put_of_index_creates_then_refuses() {
        let (router, _state, tmp) = setup_app(0);
        let resp = put_if_none_match(router.clone(), "/index", "*", b"i1".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);
        let resp = put_if_none_match(router, "/index", "*", b"i2".to_vec()).await;
        assert_status(&resp, StatusCode::PRECONDITION_FAILED);
        assert_eq!(std::fs::read(tmp.path().join("index")).unwrap(), b"i1");
    }

    /// The race path: the target appears after the existence check, while the
    /// body is still streaming. The no-clobber rename must decide it
    /// (`expected`: 412 for a conditional PUT), leave the other writer's
    /// bytes `winner`, and release the reservation.
    async fn conditional_put_loses_race(key: &str) {
        put_races_a_creator(key, true, b"winner", StatusCode::PRECONDITION_FAILED).await;
    }

    async fn put_races_a_creator(
        key: &str,
        conditional: bool,
        winner: &[u8],
        expected: StatusCode,
    ) {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;
        use tower::ServiceExt;

        let (router, state, tmp) = setup_app(1_000_000);
        let target = tmp.path().join(key);
        let dir = target.parent().unwrap().to_path_buf();
        let used = state.quota_used();

        let release = Arc::new(AtomicBool::new(false));
        let gate = Arc::clone(&release);
        let body = b"loser".to_vec();
        let stream = futures_util::stream::unfold(Some(body.clone()), move |chunk| {
            let gate = Arc::clone(&gate);
            async move {
                let chunk = chunk?;
                while !gate.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Some((
                    Ok::<_, std::io::Error>(axum::body::Bytes::from(chunk)),
                    None,
                ))
            }
        });
        let mut req = authed_request("PUT", &format!("/{key}"))
            .header("Content-Length", body.len().to_string());
        if conditional {
            req = req.header("If-None-Match", "*");
        }
        let req = req.body(Body::from_stream(stream)).unwrap();
        let handle = tokio::spawn(router.oneshot(req));

        // The temp file exists once the handler is past the existence check
        // and the quota reservation, and is waiting for the body.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while temp_file_count(&dir) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "no temp file appeared"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        std::fs::write(&target, winner).unwrap();
        release.store(true, Ordering::SeqCst);

        let resp = handle.await.unwrap().unwrap();
        assert_status(&resp, expected);
        assert_eq!(std::fs::read(&target).unwrap(), winner);
        assert_eq!(state.quota_used(), used, "a lost race commits nothing");
        assert_eq!(state.quota_reserved(), 0, "the reservation is released");
        assert_eq!(temp_file_count(&dir), 0);
    }

    #[tokio::test]
    async fn conditional_put_race_is_decided_by_the_rename() {
        conditional_put_loses_race("locks/raced").await;
    }

    #[tokio::test]
    async fn conditional_put_race_is_decided_by_the_rename_for_index() {
        conditional_put_loses_race("index").await;
    }

    /// A plain PUT of a head that another writer creates while the body
    /// streams: other bytes are refused (409), the same bytes acknowledged.
    #[tokio::test]
    async fn head_put_race_is_decided_by_the_rename() {
        put_races_a_creator("index.h/9", false, b"winner", StatusCode::CONFLICT).await;
        put_races_a_creator("index.h/9", false, b"loser", StatusCode::NO_CONTENT).await;
    }

    /// Move a file's mtime an hour back, so a rewrite would be visible.
    fn age_file(path: &std::path::Path) -> std::time::SystemTime {
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(old)
            .unwrap();
        std::fs::metadata(path).unwrap().modified().unwrap()
    }

    /// The identity of the file at `path`: its mtime and (unix) inode.
    fn file_identity(path: &std::path::Path) -> (std::time::SystemTime, u64) {
        let meta = std::fs::metadata(path).unwrap();
        #[cfg(unix)]
        let ino = std::os::unix::fs::MetadataExt::ino(&meta);
        #[cfg(not(unix))]
        let ino = 0;
        (meta.modified().unwrap(), ino)
    }

    /// Every PUT of `index.d/<id>` is refused unless `<id>` is the BLAKE3 of
    /// the body, whatever digest header (if any) the PUT carries.
    #[tokio::test]
    async fn index_object_put_checks_the_name() {
        for append_only in [false, true] {
            for header in [Some("X-Content-BLAKE3"), Some("X-Content-BLAKE2b"), None] {
                let ctx = format!("append_only={append_only} header={header:?}");
                let (router, state, tmp) = setup_app_with(1_000_000, append_only);
                let put = |key: String, body: &'static [u8]| {
                    let router = router.clone();
                    async move {
                        let digest = match header {
                            Some("X-Content-BLAKE3") => blake3_hex(body),
                            _ => blake2b_hex(body),
                        };
                        let digests: Vec<(&str, &str)> =
                            header.map(|h| (h, digest.as_str())).into_iter().collect();
                        authed_put_with_digests(router, &format!("/{key}"), body.to_vec(), &digests)
                            .await
                    }
                };

                let good = format!("index.d/{}", blake3_hex(b"named"));
                let resp = put(good.clone(), b"named").await;
                assert_eq!(resp.status(), StatusCode::CREATED, "{ctx}");
                assert_eq!(std::fs::read(tmp.path().join(&good)).unwrap(), b"named");
                let used = state.quota_used();

                let bad = format!("index.d/{}", blake3_hex(b"claimed"));
                let resp = put(bad.clone(), b"other").await;
                assert_eq!(resp.status(), StatusCode::CONFLICT, "{ctx}");
                assert!(!tmp.path().join(&bad).exists(), "{ctx}");
                assert_eq!(state.quota_used(), used, "{ctx}");
                assert_eq!(state.quota_reserved(), 0, "{ctx}");
                assert_no_temp_files(&tmp.path().join("index.d"));
            }
        }
    }

    /// A PUT onto an existing index object stores nothing: the same bytes are
    /// acknowledged (204) without renewing the file, other bytes under its
    /// name are refused (409) before the existing object is considered.
    #[tokio::test]
    async fn index_object_reput_keeps_the_file() {
        for append_only in [false, true] {
            let ctx = format!("append_only={append_only}");
            let (router, state, tmp) = setup_app_with(1_000_000, append_only);
            let key = format!("index.d/{}", blake3_hex(b"object"));
            let path = tmp.path().join(&key);
            let resp = put_with_blake3(router.clone(), &key, b"object").await;
            assert_eq!(resp.status(), StatusCode::CREATED, "{ctx}");
            age_file(&path);
            let before = file_identity(&path);
            let used = state.quota_used();

            let resp = put_with_blake3(router.clone(), &key, b"object").await;
            assert_eq!(resp.status(), StatusCode::NO_CONTENT, "{ctx}");
            assert_eq!(file_identity(&path), before, "{ctx}");
            assert_eq!(state.quota_used(), used, "{ctx}");
            assert_eq!(state.quota_reserved(), 0, "{ctx}");

            let resp = put_with_blake3(router.clone(), &key, b"impostor").await;
            assert_eq!(resp.status(), StatusCode::CONFLICT, "{ctx}");
            assert_eq!(std::fs::read(&path).unwrap(), b"object", "{ctx}");
            assert_eq!(file_identity(&path), before, "{ctx}");
            assert_eq!(state.quota_used(), used, "{ctx}");
            assert_eq!(state.quota_reserved(), 0, "{ctx}");
            assert_no_temp_files(&tmp.path().join("index.d"));
        }
    }

    /// `index.h/<gen>` is write-once in every mode: the same bytes again are
    /// acknowledged without a write, other bytes refused, and a create-only
    /// PUT answers "exists" either way.
    #[tokio::test]
    async fn head_is_write_once_in_every_mode() {
        for append_only in [false, true] {
            let ctx = format!("append_only={append_only}");
            let (router, state, tmp) = setup_app_with(1_000_000, append_only);
            let path = tmp.path().join("index.h/3");
            let resp = authed_put(router.clone(), "/index.h/3", b"head".to_vec()).await;
            assert_eq!(resp.status(), StatusCode::CREATED, "{ctx}");
            age_file(&path);
            let before = file_identity(&path);
            let used = state.quota_used();

            let resp = authed_put(router.clone(), "/index.h/3", b"head".to_vec()).await;
            assert_eq!(resp.status(), StatusCode::NO_CONTENT, "{ctx}");
            assert_eq!(file_identity(&path), before, "{ctx}");

            let resp = authed_put(router.clone(), "/index.h/3", b"other".to_vec()).await;
            assert_eq!(resp.status(), StatusCode::CONFLICT, "{ctx}");
            assert_eq!(std::fs::read(&path).unwrap(), b"head", "{ctx}");
            assert_eq!(file_identity(&path), before, "{ctx}");
            assert_eq!(state.quota_used(), used, "{ctx}");
            assert_eq!(state.quota_reserved(), 0, "{ctx}");

            let resp = put_if_none_match(router, "/index.h/3", "*", b"head".to_vec()).await;
            assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED, "{ctx}");
            assert_no_temp_files(&tmp.path().join("index.h"));
        }
    }

    /// Append-only refuses deletes of index heads, index objects and pack
    /// maps, alone and in a batch; temp debris under `index.d/` stays
    /// deletable.
    #[tokio::test]
    async fn append_only_refuses_index_deletes() {
        let (router, _state, tmp) = setup_app_append_only(0);
        let hex = "0123456789abcdef".repeat(4);
        let keys = [
            format!("index.d/{hex}"),
            "index.h/1".to_string(),
            format!("packmaps/{hex}"),
        ];
        for key in &keys {
            let path = tmp.path().join(key);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"x").unwrap();
        }
        for key in &keys {
            let resp = authed_delete(router.clone(), &format!("/{key}")).await;
            assert_eq!(resp.status(), StatusCode::FORBIDDEN, "{key}");
            let body = serde_json::to_vec(&[key]).unwrap();
            let resp = authed_post(router.clone(), "/?batch-delete", body).await;
            assert_eq!(resp.status(), StatusCode::FORBIDDEN, "batch {key}");
            assert!(tmp.path().join(key).exists(), "{key}");
        }
        std::fs::write(tmp.path().join("index.d/.tmp.x.1"), b"partial").unwrap();
        let resp = authed_delete(router, "/index.d/.tmp.x.1").await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert!(!tmp.path().join("index.d/.tmp.x.1").exists());
    }

    #[tokio::test]
    async fn list_mtime_is_added_only_on_request() {
        let (router, _state, _tmp) = setup_app(0);
        let resp = authed_put(router.clone(), "/locks/a", b"abc".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);

        let resp = authed_get(router.clone(), "/locks?list&sized").await;
        let raw = body_bytes(resp).await;
        let json: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(json, serde_json::json!([{"key": "locks/a", "size": 3}]));

        let resp = authed_get(router, "/locks?list&sized&mtime").await;
        let keys: Vec<vykar_protocol::SizedKey> =
            serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let now_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let [k] = keys.as_slice() else {
            panic!("one key expected: {keys:?}");
        };
        let mtime = k.mtime_ms.expect("mtime_ms requested");
        assert!(
            now_ms.abs_diff(mtime) < 60_000,
            "mtime {mtime}, now {now_ms}"
        );
    }
}
