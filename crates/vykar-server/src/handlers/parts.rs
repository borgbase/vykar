//! Pack uploads in parts, so one pack can travel over several connections.
//!
//! ```text
//! PUT  /packs/<xx>/<id>?upload=<token>&offset=<n>&total=<size>   (one per part, any order)
//! POST /packs/<xx>/<id>?complete&upload=<token>                  (X-Content-BLAKE3 / -BLAKE2b)
//! ```
//!
//! A part is a positioned write of `Content-Length` bytes at `offset` into the
//! upload's temp file; rewriting a part writes the same bytes again, so the
//! client's per-request retry applies unchanged. Completion keeps the
//! durability contract of a whole-object PUT: the assembled file is hashed,
//! size-checked and fdatasync'ed, renamed into place, and the directory is
//! fsync'ed before the response — no ack before the object is durable under
//! its final name. Retrying a completion whose response was lost succeeds:
//! the upload is gone, but the committed object carries the expected digest.
//! That retry, like an append-only completion that finds the identical pack
//! already stored, fsyncs the directory again before acking, since the
//! request that renamed the file may have failed or not finished its own.
//!
//! Only `packs/<xx>/<id>` keys accept parts. The temp file, quota
//! reservation and abandoned-upload expiry live in [`crate::uploads`].

use std::io::Read;
use std::path::{Path, PathBuf};

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::TryStreamExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tokio_util::io::StreamReader;

use super::objects::ObjectQuery;
use super::objects::{digest_mismatch, ensure_parent_dir, fsync_dir_async, parse_expected_digest};
use crate::error::ServerError;
use crate::state::AppState;
use crate::uploads::{Completion, PartUpload, Take};
use vykar_types::hash::{HashAlgorithm, Hasher256};

/// Resolve the final path of a pack key and the temp path of upload `token`.
fn resolve(state: &AppState, key: &str, token: &str) -> Result<(PathBuf, PathBuf), ServerError> {
    if !vykar_protocol::is_valid_pack_key(key) {
        return Err(ServerError::BadRequest(
            "part uploads are only accepted for pack keys".into(),
        ));
    }
    if !vykar_protocol::is_valid_upload_token(token) {
        return Err(ServerError::BadRequest("invalid upload token".into()));
    }
    let file_path = state
        .file_path(key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
    let (Some(parent), Some(name)) = (file_path.parent(), file_path.file_name()) else {
        return Err(ServerError::BadRequest("invalid path".into()));
    };
    let temp = parent.join(format!(
        "{}{}.{token}",
        vykar_protocol::TEMP_FILE_PREFIX,
        name.to_string_lossy()
    ));
    Ok((file_path, temp))
}

/// `PUT /packs/<xx>/<id>?upload=<token>&offset=<n>&total=<size>`.
pub(crate) async fn put_part(
    state: AppState,
    key: &str,
    token: &str,
    query: &ObjectQuery,
    headers: &HeaderMap,
    body: axum::body::Body,
) -> Result<Response, ServerError> {
    let (file_path, temp) = resolve(&state, key, token)?;
    let (Some(offset), Some(total)) = (query.offset, query.total) else {
        return Err(ServerError::BadRequest(
            "part upload requires offset and total".into(),
        ));
    };
    let len = headers
        .get("Content-Length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or_else(|| ServerError::BadRequest("part upload requires Content-Length".into()))?;
    if total == 0 || total > super::MAX_OBJECT_BODY_BYTES as u64 {
        return Err(ServerError::BadRequest(format!(
            "part upload total {total} out of range"
        )));
    }
    if len == 0 || offset.checked_add(len).is_none_or(|end| end > total) {
        return Err(ServerError::BadRequest(format!(
            "part {offset}+{len} outside total {total}"
        )));
    }
    // Refused up front so a doomed upload does not send its bytes; the
    // completion checks again.
    if state.inner.config.append_only && path_exists(&file_path).await? {
        return Err(ServerError::Forbidden(
            "append-only: cannot overwrite immutable object".into(),
        ));
    }
    let parent = file_path
        .parent()
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
    ensure_parent_dir(&state, parent).await?;

    let upload = state
        .inner
        .part_uploads
        .begin_or_join(&state, &temp, total)?;
    // The file handle is scoped inside `write_part`, so a part that runs to
    // completion has closed it before `upload` is dropped, and completion
    // only takes an upload no part holds. A part request cancelled mid-write
    // is the exception: tokio may still be running its last buffered write
    // (at most one 256 KiB chunk) on the blocking pool after `upload` is
    // dropped, so that write can land on the file a completion has taken.
    let written = write_part(&upload, offset, body).await;
    upload.touch();
    let written = written?;
    if written != len {
        return Err(ServerError::BadRequest(format!(
            "part size mismatch: Content-Length {len}, received {written}"
        )));
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn write_part(
    upload: &PartUpload,
    offset: u64,
    body: axum::body::Body,
) -> Result<u64, ServerError> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(upload.path())
        .await
        .map_err(ServerError::from)?;
    file.seek(SeekFrom::Start(offset))
        .await
        .map_err(ServerError::from)?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);
    let stream = TryStreamExt::map_err(body.into_data_stream(), std::io::Error::other);
    let mut reader = StreamReader::new(stream);
    let mut buf = vec![0u8; 256 * 1024];
    let mut written: u64 = 0;
    loop {
        let n = reader.read(&mut buf).await.map_err(ServerError::from)?;
        if n == 0 {
            break;
        }
        let chunk = buf.get(..n).expect("n <= buf.len() (just-read bytes)");
        writer.write_all(chunk).await.map_err(ServerError::from)?;
        written += n as u64;
    }
    writer.flush().await.map_err(ServerError::from)?;
    // No fsync per part: completion fdatasyncs the whole file once.
    Ok(written)
}

/// `POST /packs/<xx>/<id>?complete&upload=<token>`.
pub(crate) async fn complete_upload(
    state: AppState,
    key: &str,
    token: &str,
    headers: &HeaderMap,
) -> Result<Response, ServerError> {
    let (file_path, temp) = resolve(&state, key, token)?;
    let (algo, expected) = parse_expected_digest(headers, true)?
        .ok_or_else(|| ServerError::BadRequest("content digest required".into()))?;

    match state.inner.part_uploads.take(&temp) {
        Take::Owned(completion) => finish(&state, &file_path, completion, algo, &expected).await,
        // A part request is still writing (e.g. a client attempt that timed
        // out and is still draining). Retryable.
        Take::Busy => Err(ServerError::ServiceUnavailable(
            "upload has parts in flight".into(),
        )),
        // Another completion (a retry racing the first attempt) is still
        // running: verifying, publishing or syncing the directory. Retryable:
        // the marker lasts until that request has returned, so a retry after
        // it gets `Missing` below and acks from the stored digest, or gets
        // 404 if that completion failed or was cancelled.
        Take::Completing => Err(ServerError::ServiceUnavailable(
            "upload is being completed".into(),
        )),
        Take::Missing => {
            // A retried completion whose first response was lost: the upload
            // is gone because it was committed. Only the digest proves that.
            // The first completion has returned (its marker is gone) but may
            // have failed its directory fsync, and another request storing
            // the same pack may still be inside its own, so sync it before
            // acking: the file was fdatasync'ed before its rename, only the
            // rename can still be volatile.
            if digest_of(&file_path, algo).await?.as_deref() == Some(expected.as_str()) {
                return ack_committed(&file_path).await;
            }
            Err(ServerError::NotFound("unknown upload".into()))
        }
    }
}

/// Verify, make durable and publish an upload the caller owns exclusively.
/// `completion` lives until this returns, so its marker refuses every other
/// request for the temp path throughout. Every early return (and the future
/// being dropped at any await) drops it, which unlinks the file wherever it
/// is, releases the reservation and then clears the marker.
async fn finish(
    state: &AppState,
    file_path: &Path,
    mut completion: Completion<'_>,
    algo: HashAlgorithm,
    expected: &str,
) -> Result<Response, ServerError> {
    let total = completion.total();
    let parent = file_path
        .parent()
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    // Seal first, before any await: move the file away from the name part
    // requests open. No part request holds a handle on it (`take` proved
    // that), except for the last write of a cancelled part request that
    // tokio may still be finishing (see `put_part`). The sealed name is per
    // token, not per completion, which is safe only because the marker
    // outlives every use of it below: while `completion` exists no request
    // can create a file at the temp path, so no other completion can seal
    // one over this name between the hash (which opens it) and the publish
    // (which renames it). A client DELETE of the temp-named file, the
    // delete-repo purge or the aged-debris sweep can only unlink it, which
    // fails the open or the rename below; none can put other bytes there.
    let sealed = {
        let mut name = completion.temp_path().as_os_str().to_owned();
        name.push(".sealed");
        PathBuf::from(name)
    };
    completion.seal(&sealed).map_err(ServerError::from)?;

    let old_size = match tokio::fs::metadata(file_path).await {
        Ok(meta) => Some(meta.len()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(ServerError::from(e)),
    };
    if state.inner.config.append_only && old_size.is_some() {
        // The same pack already committed (e.g. by a concurrent backup):
        // nothing would be overwritten, so this is a success, not a refusal.
        // That request's directory fsync may not have finished yet.
        if digest_of(file_path, algo).await?.as_deref() == Some(expected) {
            return ack_committed(file_path).await;
        }
        return Err(ServerError::Forbidden(
            "append-only: cannot overwrite immutable object".into(),
        ));
    }

    // Hash and fdatasync off the runtime. The file was just written, so the
    // read is served from the page cache.
    let path = sealed.clone();
    let (len, actual) = tokio::task::spawn_blocking(move || -> std::io::Result<(u64, String)> {
        // Write access: Windows' FlushFileBuffers requires it.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        let (len, digest) = hash_reader(&file, algo)?;
        vykar_common::fs::fdatasync_file(&file)?;
        Ok((len, digest))
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::from)?;

    if len != total {
        return Err(ServerError::BadRequest(format!(
            "upload size mismatch: total {total}, assembled {len}"
        )));
    }
    if actual != expected {
        return Err(digest_mismatch(algo, expected, &actual));
    }

    // Same ordering and reasoning as `put_object`: commit right after the
    // rename, before the directory fsync.
    completion.publish(file_path).map_err(ServerError::from)?;
    if let Some(old) = old_size {
        state.sub_quota_usage(old);
    }
    fsync_dir_async(parent.to_path_buf())
        .await
        .map_err(ServerError::from)?;

    let status = if old_size.is_some() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::CREATED
    };
    Ok(status.into_response())
}

/// Acknowledge an object some request already committed under `file_path`.
/// Its contents were fdatasync'ed before their rename, but the rename is only
/// durable once the directory is fsync'ed — which that request may not have
/// done (it failed) or finished (it is still running). Sync it here, so no
/// `204` is sent before the object is durable under its final name.
async fn ack_committed(file_path: &Path) -> Result<Response, ServerError> {
    let parent = file_path
        .parent()
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
    fsync_dir_async(parent.to_path_buf())
        .await
        .map_err(ServerError::from)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn path_exists(path: &Path) -> Result<bool, ServerError> {
    match tokio::fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(ServerError::from(e)),
    }
}

/// Digest of the committed object at `path`, or `None` if there is none.
async fn digest_of(path: &Path, algo: HashAlgorithm) -> Result<Option<String>, ServerError> {
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

fn hash_reader(mut reader: impl Read, algo: HashAlgorithm) -> std::io::Result<(u64, String)> {
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

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::super::test_helpers::*;
    use vykar_types::hash::HashAlgorithm;
    use vykar_types::pack_id::PackId;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";
    const OTHER_TOKEN: &str = "fedcba9876543210fedcba9876543210";

    fn pack(len: usize) -> (Vec<u8>, String, String) {
        let data: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
        let id = PackId::compute(&data, HashAlgorithm::Blake3).to_hex();
        let path = format!("/packs/{}/{id}", &id[..2]);
        (data, id, path)
    }

    async fn send(
        router: axum::Router,
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> axum::response::Response {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("Authorization", format!("Bearer {TEST_TOKEN}"))
            .header("Content-Length", body.len().to_string());
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        router
            .oneshot(builder.body(Body::from(body)).unwrap())
            .await
            .unwrap()
    }

    async fn put_part(
        router: &axum::Router,
        path: &str,
        token: &str,
        data: &[u8],
        range: std::ops::Range<usize>,
    ) -> axum::response::Response {
        let uri = format!(
            "{path}?upload={token}&offset={}&total={}",
            range.start,
            data.len()
        );
        send(router.clone(), "PUT", &uri, &[], data[range].to_vec()).await
    }

    async fn complete(
        router: &axum::Router,
        path: &str,
        token: &str,
        header: &str,
        digest: &str,
    ) -> axum::response::Response {
        let uri = format!("{path}?complete&upload={token}");
        send(
            router.clone(),
            "POST",
            &uri,
            &[(header, digest)],
            Vec::new(),
        )
        .await
    }

    async fn upload_all(router: &axum::Router, path: &str, token: &str, data: &[u8]) {
        let n = data.len();
        for range in [0..n / 3, n / 3..2 * n / 3, 2 * n / 3..n] {
            assert_status(
                &put_part(router, path, token, data, range).await,
                StatusCode::NO_CONTENT,
            );
        }
    }

    #[tokio::test]
    async fn health_advertises_pack_parts() {
        let (router, _state, _tmp) = setup_app(0);
        let resp = send(router, "GET", "/health", &[], Vec::new()).await;
        assert_status(&resp, StatusCode::OK);
        let caps: vykar_protocol::ServerCapabilities =
            serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert!(caps.pack_parts);
        assert!(caps.segmented_index);
    }

    /// Parts in any order, one sent twice; the assembled object is exactly
    /// the pack, quota moves from reserved to used, nothing is left behind.
    #[tokio::test]
    async fn out_of_order_and_repeated_parts_assemble_the_pack() {
        let (router, state, tmp) = setup_app(1_000_000);
        let (data, id, path) = pack(30_000);

        for range in [20_000..30_000, 0..10_000, 10_000..20_000, 0..10_000] {
            assert_status(
                &put_part(&router, &path, TOKEN, &data, range).await,
                StatusCode::NO_CONTENT,
            );
        }
        assert_eq!(state.quota_reserved(), 30_000, "reservation spans requests");
        assert_eq!(temp_file_count(tmp.path()), 1);

        // The in-progress upload is invisible to listings.
        let resp = send(router.clone(), "GET", "/packs?list", &[], Vec::new()).await;
        let keys: Vec<String> = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert!(keys.is_empty(), "got {keys:?}");

        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::CREATED);

        let resp = send(router.clone(), "GET", &path, &[], Vec::new()).await;
        assert_eq!(body_bytes(resp).await, data);
        assert_no_temp_files(tmp.path());
        assert_eq!(state.quota_reserved(), 0);
        assert_eq!(state.quota_used(), 30_000);
        assert_eq!(state.inner.part_uploads.len(), 0);
    }

    #[tokio::test]
    async fn blake2b_digest_completes_a_v2_pack() {
        let (router, _state, tmp) = setup_app(0);
        let data = vec![0x5Au8; 5000];
        let id = PackId::compute(&data, HashAlgorithm::Blake2b).to_hex();
        let path = format!("/packs/{}/{id}", &id[..2]);
        upload_all(&router, &path, TOKEN, &data).await;
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE2b", &id).await;
        assert_status(&resp, StatusCode::CREATED);
        assert_no_temp_files(tmp.path());
    }

    /// A completion whose response was lost is retried; the retry must
    /// succeed without re-sending any part.
    #[tokio::test]
    async fn retried_completion_succeeds() {
        let (router, state, _tmp) = setup_app(1_000_000);
        let (data, id, path) = pack(9000);
        upload_all(&router, &path, TOKEN, &data).await;
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::CREATED);

        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert_eq!(state.quota_used(), 9000, "a retry commits nothing twice");
    }

    #[tokio::test]
    async fn digest_mismatch_discards_the_upload() {
        let (router, state, tmp) = setup_app(1_000_000);
        let (data, id, path) = pack(9000);
        upload_all(&router, &path, TOKEN, &data).await;

        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &"a".repeat(64)).await;
        assert_status(&resp, StatusCode::CONFLICT);
        let text = String::from_utf8(body_bytes(resp).await).unwrap();
        assert!(text.contains("BLAKE3 checksum mismatch"), "got: {text}");

        assert!(!tmp.path().join(&path[1..]).exists(), "nothing committed");
        assert_no_temp_files(tmp.path());
        assert_eq!(state.quota_reserved(), 0);
        assert_eq!(state.quota_used(), 0);
        // The upload is gone: completing again (even correctly) is a 404.
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::NOT_FOUND);
    }

    /// A pack whose id hashes a *different* BLAKE2b/BLAKE3 header value is
    /// still a mismatch: the header name selects the algorithm.
    #[tokio::test]
    async fn wrong_algorithm_header_is_a_mismatch() {
        let (router, _state, _tmp) = setup_app(0);
        let (data, id, path) = pack(4000);
        upload_all(&router, &path, TOKEN, &data).await;
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE2b", &id).await;
        assert_status(&resp, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn missing_part_is_a_size_mismatch() {
        let (router, state, tmp) = setup_app(1_000_000);
        let (data, id, path) = pack(9000);
        put_part(&router, &path, TOKEN, &data, 0..3000).await;
        put_part(&router, &path, TOKEN, &data, 3000..6000).await;

        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        let text = String::from_utf8(body_bytes(resp).await).unwrap();
        assert!(text.contains("size mismatch"), "got: {text}");
        assert_no_temp_files(tmp.path());
        assert_eq!(state.quota_reserved(), 0);
    }

    /// A gap in the middle keeps the length right but not the digest.
    #[tokio::test]
    async fn missing_middle_part_is_a_digest_mismatch() {
        let (router, _state, tmp) = setup_app(0);
        let (data, id, path) = pack(9000);
        put_part(&router, &path, TOKEN, &data, 0..3000).await;
        put_part(&router, &path, TOKEN, &data, 6000..9000).await;
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::CONFLICT);
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn unknown_token_is_not_found() {
        let (router, _state, _tmp) = setup_app(0);
        let (data, id, path) = pack(4000);
        upload_all(&router, &path, TOKEN, &data).await;
        let resp = complete(&router, &path, OTHER_TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::NOT_FOUND);
        // The real upload is untouched and still completes.
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn malformed_part_requests_are_rejected() {
        let (router, _state, tmp) = setup_app(0);
        let (data, _id, path) = pack(4000);
        let body = data[..1000].to_vec();
        for uri in [
            // Not a pack key.
            format!("/index?upload={TOKEN}&offset=0&total=4000"),
            // Malformed token.
            format!("{path}?upload=xyz&offset=0&total=4000"),
            format!("{path}?upload={}&offset=0&total=4000", TOKEN.to_uppercase()),
            // Missing offset / total.
            format!("{path}?upload={TOKEN}&total=4000"),
            format!("{path}?upload={TOKEN}&offset=0"),
            // Past the declared total.
            format!("{path}?upload={TOKEN}&offset=3500&total=4000"),
            format!("{path}?upload={TOKEN}&offset={}&total=4000", u64::MAX - 10),
            // Zero total.
            format!("{path}?upload={TOKEN}&offset=0&total=0"),
        ] {
            let resp = send(router.clone(), "PUT", &uri, &[], body.clone()).await;
            assert_status(&resp, StatusCode::BAD_REQUEST);
        }
        assert_no_temp_files(tmp.path());

        // A part disagreeing with the upload's total.
        put_part(&router, &path, TOKEN, &data, 0..1000).await;
        let uri = format!("{path}?upload={TOKEN}&offset=1000&total=5000");
        let resp = send(router.clone(), "PUT", &uri, &[], body).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);

        // Completion without a digest.
        let uri = format!("{path}?complete&upload={TOKEN}");
        let resp = send(router.clone(), "POST", &uri, &[], Vec::new()).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn append_only_refuses_parts_for_an_existing_pack() {
        let (router, _state, tmp) = setup_app_append_only(0);
        let (data, _id, path) = pack(4000);
        std::fs::write(tmp.path().join(&path[1..]), b"existing").unwrap();

        let resp = put_part(&router, &path, TOKEN, &data, 0..1000).await;
        assert_status(&resp, StatusCode::FORBIDDEN);
        assert_no_temp_files(tmp.path());
        assert_eq!(
            std::fs::read(tmp.path().join(&path[1..])).unwrap(),
            b"existing"
        );
    }

    /// The pack appears (another writer) between the parts and completion.
    #[tokio::test]
    async fn append_only_completion_never_overwrites() {
        let (router, _state, tmp) = setup_app_append_only(0);
        let (data, id, path) = pack(6000);
        let final_path = tmp.path().join(&path[1..]);

        // Different bytes at the key: refused, original kept.
        upload_all(&router, &path, TOKEN, &data).await;
        std::fs::write(&final_path, b"other").unwrap();
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::FORBIDDEN);
        assert_eq!(std::fs::read(&final_path).unwrap(), b"other");
        assert_no_temp_files(tmp.path());

        // The identical pack at the key: nothing to overwrite, so success.
        std::fs::remove_file(&final_path).unwrap();
        upload_all(&router, &path, OTHER_TOKEN, &data).await;
        std::fs::write(&final_path, &data).unwrap();
        let resp = complete(&router, &path, OTHER_TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn append_only_accepts_a_new_pack_in_parts() {
        let (router, _state, tmp) = setup_app_append_only(0);
        let (data, id, path) = pack(6000);
        upload_all(&router, &path, TOKEN, &data).await;
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::CREATED);
        // And a lost-response retry still succeeds in append-only mode.
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert_no_temp_files(tmp.path());
    }

    /// The whole declared total is reserved by the first part and held across
    /// requests, so a second upload cannot overrun the quota in the gaps.
    #[tokio::test]
    async fn quota_is_reserved_for_the_upload_and_released() {
        let (router, state, tmp) = setup_app(10_000);
        let (data, id, path) = pack(6000);
        let (other, _other_id, other_path) = pack(5000);

        // Larger than the quota outright.
        let (big, _big_id, big_path) = pack(20_000);
        let resp = put_part(&router, &big_path, TOKEN, &big, 0..1000).await;
        assert_status(&resp, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(state.quota_reserved(), 0);

        put_part(&router, &path, TOKEN, &data, 0..1000).await;
        assert_eq!(state.quota_reserved(), 6000);
        let resp = put_part(&router, &other_path, TOKEN, &other, 0..1000).await;
        assert_status(&resp, StatusCode::PAYLOAD_TOO_LARGE);

        // A failed completion releases the reservation...
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        assert_eq!(state.quota_reserved(), 0);
        // ...so the other upload now fits.
        upload_all(&router, &other_path, TOKEN, &other).await;
        assert_eq!(state.quota_reserved(), 5000);
        assert_no_temp_files_except_one(tmp.path());
    }

    fn assert_no_temp_files_except_one(dir: &std::path::Path) {
        assert_eq!(temp_file_count(dir), 1);
    }

    /// An upload nobody touches is expired: temp file unlinked, quota freed.
    #[tokio::test]
    async fn abandoned_upload_is_expired() {
        let (router, state, tmp) = setup_app(10_000);
        let (data, id, path) = pack(6000);
        put_part(&router, &path, TOKEN, &data, 0..1000).await;
        assert_eq!(temp_file_count(tmp.path()), 1);

        // Fresh uploads survive a normal expiry pass.
        assert_eq!(
            state
                .inner
                .part_uploads
                .expire(crate::uploads::PART_UPLOAD_IDLE_TTL),
            0
        );
        assert_eq!(temp_file_count(tmp.path()), 1);

        assert_eq!(
            state.inner.part_uploads.expire(std::time::Duration::ZERO),
            1
        );
        assert_no_temp_files(tmp.path());
        assert_eq!(state.quota_reserved(), 0);
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::NOT_FOUND);
    }

    /// The temp file of an upload the registry lost (server restart) is
    /// ordinary temp debris: hidden from listings and swept once aged.
    #[tokio::test]
    async fn orphaned_part_file_is_swept_as_debris() {
        let (router, state, tmp) = setup_app(0);
        let (data, _id, path) = pack(6000);
        put_part(&router, &path, TOKEN, &data, 0..1000).await;
        let temp = crate::handlers::test_helpers::find_temp_file(&state.inner.data_dir);
        // Forget the upload without unlinking, as a restart would.
        let leaked = state.inner.part_uploads.take(&temp);
        let crate::uploads::Take::Owned(completion) = leaked else {
            panic!("upload should be idle");
        };
        completion.forget_file();

        let f = std::fs::OpenOptions::new().write(true).open(&temp).unwrap();
        f.set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(25 * 3600))
            .unwrap();
        drop(f);
        let (_usage, swept) =
            crate::cleanup::scan_usage(tmp.path(), Some(crate::cleanup::TEMP_DEBRIS_MAX_AGE));
        assert_eq!(swept.files, 1);
        assert_no_temp_files(tmp.path());
    }

    /// Completion while a part request is still writing is refused with a
    /// retryable 503 and leaves the upload intact.
    #[tokio::test]
    async fn completion_waits_for_parts_in_flight() {
        use futures_util::StreamExt;
        let (router, state, _tmp) = setup_app(0);
        let (data, id, path) = pack(6000);
        upload_all(&router, &path, TOKEN, &data).await;

        // A part whose body never finishes.
        let stream = futures_util::stream::once(async {
            Ok::<_, std::io::Error>(axum::body::Bytes::from(vec![0u8; 100]))
        })
        .chain(futures_util::stream::pending());
        let req = Request::builder()
            .method("PUT")
            .uri(format!("{path}?upload={TOKEN}&offset=0&total=6000"))
            .header("Authorization", format!("Bearer {TEST_TOKEN}"))
            .header("Content-Length", "1000")
            .body(Body::from_stream(stream))
            .unwrap();
        let stalled_router = router.clone();
        let mut stalled = Box::pin(async move {
            let _ = stalled_router.oneshot(req).await;
        });
        let _ = tokio::time::timeout(std::time::Duration::from_millis(200), &mut stalled).await;

        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.inner.part_uploads.len(), 1);

        drop(stalled); // the client gives up on the stalled part
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::CREATED);
    }

    /// Between a completion taking the upload and sealing its file, neither a
    /// late part nor a racing completion may touch it: both get a retryable
    /// 503, and the part opens no file and registers no upload.
    #[tokio::test]
    async fn requests_during_completion_are_retryable_and_touch_nothing() {
        let (router, state, tmp) = setup_app(1_000_000);
        let (data, id, path) = pack(6000);
        upload_all(&router, &path, TOKEN, &data).await;
        let temp = crate::handlers::test_helpers::find_temp_file(&state.inner.data_dir);
        let before = std::fs::read(&temp).unwrap();

        // A completion that has taken the upload but not sealed it yet.
        let crate::uploads::Take::Owned(completion) = state.inner.part_uploads.take(&temp) else {
            panic!("upload should be idle");
        };

        let resp = put_part(&router, &path, TOKEN, &vec![0xEE; 6000], 0..3000).await;
        assert_status(&resp, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(std::fs::read(&temp).unwrap(), before, "file untouched");
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.inner.part_uploads.len(), 1, "only the marker");
        assert_eq!(state.quota_reserved(), 6000, "no second reservation");

        // The completion is dropped (client gone): everything is released
        // and the upload can be sent again.
        drop(completion);
        assert_no_temp_files(tmp.path());
        assert_eq!(state.quota_reserved(), 0);
        assert_eq!(state.inner.part_uploads.len(), 0);
        upload_all(&router, &path, TOKEN, &data).await;
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::CREATED);
    }

    /// After the seal, for as long as the completion runs (hash, fdatasync,
    /// publish, directory fsync), a late part and a retried completion still
    /// get a retryable 503 and touch nothing. Once it has ended, a retried
    /// completion gets 404 if it failed and 204 if it published.
    #[tokio::test]
    async fn requests_after_seal_are_retryable_until_the_completion_ends() {
        let (router, state, tmp) = setup_app(1_000_000);
        let (data, id, path) = pack(6000);
        let final_path = tmp.path().join(&path[1..]);
        let uploads = &state.inner.part_uploads;
        let garbage = vec![0xEE; 6000];

        for publish in [false, true] {
            upload_all(&router, &path, TOKEN, &data).await;
            let temp = crate::handlers::test_helpers::find_temp_file(&state.inner.data_dir);
            let sealed = std::path::PathBuf::from(format!("{}.sealed", temp.display()));

            // A completion that has sealed the file and is verifying it.
            let crate::uploads::Take::Owned(mut completion) = uploads.take(&temp) else {
                panic!("upload should be idle");
            };
            completion.seal(&sealed).unwrap();
            if publish {
                completion.publish(&final_path).unwrap();
            }
            for _ in 0..2 {
                let resp = put_part(&router, &path, TOKEN, &garbage, 0..3000).await;
                assert_status(&resp, StatusCode::SERVICE_UNAVAILABLE);
                let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
                assert_status(&resp, StatusCode::SERVICE_UNAVAILABLE);
            }
            assert!(!temp.exists(), "no fresh upload at the temp path");
            assert_eq!(uploads.len(), 1, "only the marker");
            assert_eq!(state.quota_reserved(), if publish { 0 } else { 6000 });

            // The completion ends: published, or failed / cancelled.
            drop(completion);
            assert_no_temp_files(tmp.path());
            assert_eq!(uploads.len(), 0);
            assert_eq!(state.quota_reserved(), 0);
            let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
            if publish {
                assert_status(&resp, StatusCode::NO_CONTENT);
                assert_eq!(std::fs::read(&final_path).unwrap(), data);
                assert_eq!(state.quota_used(), 6000);
            } else {
                assert_status(&resp, StatusCode::NOT_FOUND);
                assert!(!final_path.exists());
                assert_eq!(state.quota_used(), 0);
            }
        }

        // A stale part delivered after the completion ended starts a fresh
        // upload that nothing completes; it pins its reservation until idle
        // expiry, and never reaches the committed pack.
        let resp = put_part(&router, &path, TOKEN, &garbage, 0..3000).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert_eq!(state.quota_reserved(), 6000);
        assert_eq!(uploads.expire(std::time::Duration::ZERO), 1);
        assert_no_temp_files(tmp.path());
        assert_eq!(state.quota_reserved(), 0);
        assert_eq!(std::fs::read(&final_path).unwrap(), data);
    }

    /// Overwriting an existing pack outside append-only mode keeps net quota.
    #[tokio::test]
    async fn overwrite_keeps_net_quota() {
        let (router, state, tmp) = setup_app(1_000_000);
        let (data, id, path) = pack(6000);
        std::fs::write(tmp.path().join(&path[1..]), vec![0u8; 100]).unwrap();
        crate::state::rescan_usage(&state.inner);
        assert_eq!(state.quota_used(), 100);

        upload_all(&router, &path, TOKEN, &data).await;
        let resp = complete(&router, &path, TOKEN, "X-Content-BLAKE3", &id).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert_eq!(state.quota_used(), 6000);
        assert_eq!(state.quota_reserved(), 0);
    }
}
