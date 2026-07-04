//! S3 multipart upload.
//!
//! Objects larger than [`MULTIPART_THRESHOLD`] are uploaded with the S3 multipart
//! API (create → N parts → complete, abort on failure) so a mid-transfer reset
//! costs one 16 MiB part instead of restarting the whole object. This is layered
//! on the core [`S3Backend`](super::S3Backend) in the parent module, which it
//! reaches into for the shared `retry_call`, `s3_error`, `size`/`get`, and
//! `PRESIGN_DURATION`.

use std::io::Read;

use base64::Engine;
use md5::{Digest, Md5};
use rusty_s3::actions::{CreateMultipartUpload, S3Action};
use serde::Deserialize;

use super::{s3_error, S3Backend, PRESIGN_DURATION};
use crate::retry::HttpRetryError;
use crate::StorageBackend;
use vykar_types::error::{Result, VykarError};

/// Baseline multipart part size (16 MiB). Grown only for objects that would
/// otherwise need more than [`S3_MAX_PARTS`] parts (see [`derive_part_size`]).
const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
/// Objects strictly larger than this are uploaded with the S3 multipart API;
/// everything `<=` stays a single PUT. Equal to the part size so the smallest
/// multipart object is always at least two parts.
pub(super) const MULTIPART_THRESHOLD: usize = 16 * 1024 * 1024;
/// Whole-MPU restarts. One iteration runs create → parts → complete; a second
/// gives a transient/ambiguous failure one fresh `upload_id` to recover with.
const MULTIPART_MAX_ATTEMPTS: usize = 2;
/// S3/B2 hard limit on the number of parts per multipart upload.
const S3_MAX_PARTS: usize = 10_000;
/// S3 per-part maximum (5 GiB). A derived part larger than this means the object
/// is not representable as a multipart upload.
const S3_MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024;

/// Op labels for multipart actions (kept distinct from `PUT` so the
/// connection-close hint in [`s3_error`] applies only to the part data transfer).
const CREATE_MPU: &str = "CREATE_MPU";
const COMPLETE_MPU: &str = "COMPLETE_MPU";
const ABORT_MPU: &str = "ABORT_MPU";

#[allow(clippy::result_large_err)]
impl S3Backend {
    /// Upload `data` via the S3 multipart API with a bounded recovery loop.
    ///
    /// Each iteration runs one full multipart upload (create → parts → complete)
    /// that aborts its own `upload_id` on any internal failure. After a failed
    /// iteration it reconciles (if the outcome was ambiguous, the object may have
    /// committed anyway), then decides whether to restart with a fresh upload or
    /// give up — uniformly for content-addressed packs and mutable keys.
    pub(super) fn put_multipart(&self, full_key: &str, key: &str, data: &[u8]) -> Result<()> {
        let part_size = derive_part_size(data.len(), key)?;
        let mut last_err: Option<VykarError> = None;
        for _attempt in 0..MULTIPART_MAX_ATTEMPTS {
            match self.try_one_multipart(full_key, key, data, part_size) {
                Ok(()) => return Ok(()),
                Err(MultipartFailure {
                    kind,
                    err,
                    ambiguous,
                }) => {
                    // An ambiguous failure means the completion outcome was lost —
                    // the object may already be stored. Reconcile is INCONCLUSIVE,
                    // never fatal: a HEAD/GET that itself errors must not mask the
                    // upload error or skip the remaining attempt, so log and fall
                    // through rather than propagating it.
                    if ambiguous {
                        match self.reconcile(key, data) {
                            Ok(true) => return Ok(()),
                            Ok(false) => {}
                            Err(e) => tracing::warn!(
                                "S3 multipart {key}: reconcile inconclusive after \
                                 ambiguous completion: {e}"
                            ),
                        }
                    }
                    // A clean permanent failure (403, AccessDenied, missing ETag)
                    // cannot be fixed by a fresh upload; don't waste an attempt.
                    if kind == MultipartFailureKind::Permanent {
                        return Err(err);
                    }
                    last_err = Some(err);
                }
            }
        }
        Err(last_err.expect("multipart loop records an error on the last failed attempt"))
    }

    /// Proof-of-success primitive, called only when a completion outcome is
    /// ambiguous (the object may have committed). Takes the logical repo `key`,
    /// not `full_key` — [`size`](Self::size) and [`get`](Self::get) apply
    /// `full_key()` internally, so passing `full_key` would double-prefix in
    /// rooted repos.
    fn reconcile(&self, key: &str, data: &[u8]) -> Result<bool> {
        let expected = u64::try_from(data.len()).map_err(|e| {
            VykarError::Other(format!(
                "S3 multipart {key}: object size {} not representable as u64: {e}",
                data.len()
            ))
        })?;
        if key.starts_with("packs/") {
            // Content-addressed: the key IS the content id, so an object of the
            // right size at this key is necessarily this pack. HEAD suffices.
            Ok(self.size(key)? == Some(expected))
        } else {
            // Mutable key: size alone can't prove the bytes are ours (a prior
            // same-size object could exist). Only an exact byte compare does.
            Ok(self
                .get(key)?
                .is_some_and(|stored| stored.as_slice() == data))
        }
    }

    /// Run one full multipart upload. Aborts its own `upload_id` (best-effort) on
    /// any failure after creation. Create failures have no `upload_id` to abort.
    fn try_one_multipart(
        &self,
        full_key: &str,
        key: &str,
        data: &[u8],
        part_size: usize,
    ) -> std::result::Result<(), MultipartFailure> {
        let upload_id = self.create_mpu(full_key, key)?;
        match self.upload_parts_and_complete(full_key, key, data, part_size, &upload_id) {
            Ok(()) => Ok(()),
            Err(failure) => {
                self.abort_mpu(full_key, key, &upload_id);
                Err(failure)
            }
        }
    }

    /// `CreateMultipartUpload` (POST). Returns the `upload_id` on success.
    fn create_mpu(
        &self,
        full_key: &str,
        key: &str,
    ) -> std::result::Result<String, MultipartFailure> {
        let action = self
            .bucket
            .create_multipart_upload(Some(&self.credentials), full_key);
        let url = action.sign(PRESIGN_DURATION);

        let result = self.retry_call(
            &format!("{CREATE_MPU} {key}"),
            || self.agent.post(url.as_str()).send(&[] as &[u8]),
            |mut resp| {
                let status = resp.status().as_u16();
                if status < 400 {
                    let mut body = String::new();
                    resp.body_mut()
                        .as_reader()
                        .read_to_string(&mut body)
                        .map_err(HttpRetryError::BodyIo)?;
                    match CreateMultipartUpload::parse_response(&body) {
                        Ok(parsed) => Ok(CreateOutcome::Created(parsed.upload_id().to_owned())),
                        // 2xx whose body we can't read an upload id from: re-POSTing
                        // would orphan another MPU, so stop here (typed Ok) and let
                        // the outer loop restart once. Recorded as a residual
                        // unknown MPU (see §3.5 lifecycle note).
                        Err(_) => Ok(CreateOutcome::Unparseable),
                    }
                } else {
                    // Propagate a truncated error-body read as BodyIo so the retry
                    // layer handles it, rather than silently dropping the <Code>
                    // (which would misclassify e.g. a cut RequestTimeout as permanent).
                    let mut body = String::new();
                    resp.body_mut()
                        .as_reader()
                        .read_to_string(&mut body)
                        .map_err(HttpRetryError::BodyIo)?;
                    Err(classify_s3_error(
                        status,
                        parse_s3_error_code(&body).as_deref(),
                    ))
                }
            },
        );

        match result {
            Ok(CreateOutcome::Created(id)) => Ok(id),
            Ok(CreateOutcome::Unparseable) => Err(MultipartFailure {
                kind: MultipartFailureKind::Retryable,
                ambiguous: false,
                err: VykarError::Other(format!(
                    "S3 {CREATE_MPU} {key}: 2xx response with an unreadable upload id; \
                     a residual in-progress multipart upload may remain (set an \
                     AbortIncompleteMultipartUpload lifecycle rule to clean it up)"
                )),
            }),
            Err(e) => Err(MultipartFailure {
                kind: failure_kind(&e),
                ambiguous: false,
                err: s3_error(CREATE_MPU, key, e),
            }),
        }
    }

    /// Upload every part sequentially, then complete. Any part failure means the
    /// object was never committed (complete never ran) → not ambiguous.
    fn upload_parts_and_complete(
        &self,
        full_key: &str,
        key: &str,
        data: &[u8],
        part_size: usize,
        upload_id: &str,
    ) -> std::result::Result<(), MultipartFailure> {
        let mut etags: Vec<String> = Vec::new();
        for (i, chunk) in data.chunks(part_size).enumerate() {
            // part_size is derived to keep the count <= S3_MAX_PARTS (<= u16::MAX).
            let part_number = u16::try_from(i + 1).expect("part count bounded by S3_MAX_PARTS");
            match self.upload_part(full_key, key, part_number, upload_id, chunk)? {
                PartOutcome::Uploaded(etag) => etags.push(etag),
                // The MPU expired or was aborted out from under us. A fresh MPU can
                // recover, so restart at the outer loop.
                PartOutcome::NoSuchUpload => {
                    return Err(MultipartFailure {
                        kind: MultipartFailureKind::Retryable,
                        ambiguous: false,
                        err: VykarError::Other(format!(
                            "S3 PUT {key}: NoSuchUpload uploading part {part_number}; \
                             the multipart upload no longer exists"
                        )),
                    })
                }
            }
        }
        self.complete_mpu(full_key, key, &etags, upload_id)
    }

    /// `UploadPart` (PUT). Returns the part's `ETag` (quotes preserved) on success.
    fn upload_part(
        &self,
        full_key: &str,
        key: &str,
        part_number: u16,
        upload_id: &str,
        chunk: &[u8],
    ) -> std::result::Result<PartOutcome, MultipartFailure> {
        // Per-part Content-MD5 — integrity / Object-Lock parity with the single PUT.
        let content_md5 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(chunk));
        let mut action =
            self.bucket
                .upload_part(Some(&self.credentials), full_key, part_number, upload_id);
        action.headers_mut().insert("content-md5", &content_md5);
        let url = action.sign(PRESIGN_DURATION);

        let result = self.retry_call(
            &format!("PUT {key} part {part_number}"),
            || {
                self.agent
                    .put(url.as_str())
                    .header("content-md5", &content_md5)
                    .send(chunk)
            },
            |mut resp| {
                let status = resp.status().as_u16();
                if status < 400 {
                    let etag = resp
                        .headers()
                        .get("etag")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned)
                        .filter(|s| !s.is_empty());
                    return match etag {
                        Some(tag) => Ok(PartOutcome::Uploaded(tag)),
                        None => Err(HttpRetryError::Permanent(format!(
                            "part {part_number} response missing ETag header"
                        ))),
                    };
                }
                // Propagate a truncated error-body read as BodyIo (see create_mpu).
                let mut body = String::new();
                resp.body_mut()
                    .as_reader()
                    .read_to_string(&mut body)
                    .map_err(HttpRetryError::BodyIo)?;
                let code = parse_s3_error_code(&body);
                if code.as_deref() == Some("NoSuchUpload") {
                    return Ok(PartOutcome::NoSuchUpload);
                }
                Err(classify_s3_error(status, code.as_deref()))
            },
        );

        // Surface part failures as "PUT" so the connection-close hint + packs/
        // clause apply to the actual data transfer (the #151 failure).
        result.map_err(|e| MultipartFailure {
            kind: failure_kind(&e),
            ambiguous: false,
            err: s3_error("PUT", key, e),
        })
    }

    /// `CompleteMultipartUpload` (POST). Parses the response structurally — the
    /// 200-with-`<Error>` footgun and a lost-response mid-body cut both have to be
    /// distinguished from a real `CompleteMultipartUploadResult`.
    fn complete_mpu(
        &self,
        full_key: &str,
        key: &str,
        etags: &[String],
        upload_id: &str,
    ) -> std::result::Result<(), MultipartFailure> {
        let mut action = self.bucket.complete_multipart_upload(
            Some(&self.credentials),
            full_key,
            upload_id,
            etags.iter().map(String::as_str),
        );
        action
            .headers_mut()
            .insert("content-type", "application/xml");
        let url = action.sign(PRESIGN_DURATION); // &self — sign FIRST
        let body = action.body(); // consumes self — body SECOND

        // Ambiguity must survive inner retries: an earlier attempt can commit then
        // lose its response, while a later attempt returns a clean 5xx that on its
        // own looks unambiguous. Set on any lost-response signature (a body-read
        // error or a truncated/malformed 2xx body).
        let ambiguous = std::cell::Cell::new(false);

        let result = self.retry_call(
            &format!("{COMPLETE_MPU} {key}"),
            || {
                let sent = self
                    .agent
                    .post(url.as_str())
                    .header("content-type", "application/xml")
                    .send(body.as_bytes());
                if sent.is_err() {
                    // `.send()` spans the request write *and* the response-header
                    // read; ureq does not expose which phase failed. A failure after
                    // the server received the request may mean it committed, so mark
                    // ambiguous — a write-phase failure that never committed only
                    // costs one reconcile probe. (handle_response is not invoked on a
                    // transport error, so this is the only place to catch it.)
                    ambiguous.set(true);
                }
                sent
            },
            |mut resp| {
                let status = resp.status().as_u16();
                let mut body = String::new();
                if let Err(e) = resp.body_mut().as_reader().read_to_string(&mut body) {
                    // Response cut after the server received the request → the
                    // upload may have committed.
                    ambiguous.set(true);
                    return Err(HttpRetryError::BodyIo(e));
                }
                // An <Error> document — handles both the 200-with-error form and
                // genuine non-2xx errors.
                if let Some(code) = parse_s3_error_code(&body) {
                    if code == "NoSuchUpload" {
                        // A prior complete in this same retry_call may have committed
                        // before its response was lost. Must be Retryable so that, if
                        // reconcile proves the object absent, the outer loop can
                        // restart a fresh MPU.
                        return Ok(CompleteOutcome::Failed {
                            kind: MultipartFailureKind::Retryable,
                            ambiguous: true,
                            message: code,
                        });
                    }
                    if is_retryable_s3_code(&code) {
                        // Re-sign + re-POST the same body/upload_id (idempotent).
                        return Err(HttpRetryError::RetryableStatus {
                            code: status,
                            message: code,
                        });
                    }
                    return Ok(CompleteOutcome::Failed {
                        kind: MultipartFailureKind::Permanent,
                        ambiguous: false,
                        message: code,
                    });
                }
                if status < 400 {
                    // Real S3 success always carries the merged object ETag; require
                    // it so a well-formed-but-wrong 2xx body is not read as success.
                    if quick_xml::de::from_str::<CompleteResultDoc>(&body)
                        .is_ok_and(|doc| doc.etag.is_some())
                    {
                        return Ok(CompleteOutcome::Success);
                    }
                    // 200 then a mid-body cut: the lost-response signature.
                    ambiguous.set(true);
                    return Err(HttpRetryError::RetryableStatus {
                        code: status,
                        message: "malformed completion response body".to_string(),
                    });
                }
                // Non-2xx without a parseable <Code>: classify by status.
                let message = format!("HTTP {status}");
                Err(if status == 429 || status >= 500 {
                    HttpRetryError::RetryableStatus {
                        code: status,
                        message,
                    }
                } else {
                    HttpRetryError::Permanent(message)
                })
            },
        );

        match result {
            Ok(CompleteOutcome::Success) => Ok(()),
            Ok(CompleteOutcome::Failed {
                kind,
                ambiguous: amb,
                message,
            }) => Err(MultipartFailure {
                kind,
                ambiguous: amb || ambiguous.get(),
                err: VykarError::Other(format!("S3 {COMPLETE_MPU} {key}: {message}")),
            }),
            Err(e) => Err(MultipartFailure {
                kind: failure_kind(&e),
                ambiguous: ambiguous.get(),
                err: s3_error(COMPLETE_MPU, key, e),
            }),
        }
    }

    /// `AbortMultipartUpload` (DELETE), best-effort. A 404 / `NoSuchUpload` is
    /// expected success (already completed or never existed). Any other failure is
    /// swallowed with a warning and never replaces the original upload error.
    fn abort_mpu(&self, full_key: &str, key: &str, upload_id: &str) {
        let action =
            self.bucket
                .abort_multipart_upload(Some(&self.credentials), full_key, upload_id);
        let url = action.sign(PRESIGN_DURATION);

        let result = self.retry_call(
            &format!("{ABORT_MPU} {key}"),
            || self.agent.delete(url.as_str()).call(),
            |mut resp| {
                let status = resp.status().as_u16();
                if status == 404 || status < 400 {
                    return Ok(());
                }
                // Propagate a truncated error-body read as BodyIo (see create_mpu);
                // the abort is best-effort, so this just lets retry_call retry it.
                let mut body = String::new();
                resp.body_mut()
                    .as_reader()
                    .read_to_string(&mut body)
                    .map_err(HttpRetryError::BodyIo)?;
                let code = parse_s3_error_code(&body);
                if code.as_deref() == Some("NoSuchUpload") {
                    return Ok(());
                }
                Err(classify_s3_error(status, code.as_deref()))
            },
        );
        if let Err(e) = result {
            tracing::warn!(
                "S3 {ABORT_MPU} {key}: best-effort abort failed (upload may linger \
                 until an AbortIncompleteMultipartUpload lifecycle rule cleans it up): {e}"
            );
        }
    }
}

/// Disposition of a single multipart attempt's failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultipartFailureKind {
    /// A fresh MPU may recover (transient, ambiguous, or `NoSuchUpload`).
    Retryable,
    /// A clean permanent failure (403, `AccessDenied`, missing ETag); a restart
    /// cannot help.
    Permanent,
}

/// Outcome of one full multipart upload attempt that did not succeed.
struct MultipartFailure {
    kind: MultipartFailureKind,
    err: VykarError,
    /// The object may nonetheless have committed (lost-response /
    /// `NoSuchUpload`-after-complete). Triggers reconciliation.
    ambiguous: bool,
}

/// Value side of `create_mpu`'s `retry_call`.
enum CreateOutcome {
    /// `upload_id` from a parsed create response.
    Created(String),
    /// 2xx whose body could not be parsed for an upload id.
    Unparseable,
}

/// Value side of `upload_part`'s `retry_call`.
enum PartOutcome {
    /// The part's `ETag` (quotes preserved).
    Uploaded(String),
    /// The MPU no longer exists; restart at the outer loop.
    NoSuchUpload,
}

/// Value side of `complete_mpu`'s `retry_call`.
enum CompleteOutcome {
    Success,
    Failed {
        kind: MultipartFailureKind,
        ambiguous: bool,
        message: String,
    },
}

/// Minimal view of an S3 `<Error>` document — only the `<Code>` is needed.
#[derive(Deserialize)]
struct S3ErrorDoc {
    #[serde(rename = "Code")]
    code: String,
}

/// Minimal view of a `CompleteMultipartUploadResult` — the merged object `<ETag>`
/// is the marker of a genuine success body.
#[derive(Deserialize)]
struct CompleteResultDoc {
    #[serde(rename = "ETag")]
    etag: Option<String>,
}

/// Map an exhausted [`HttpRetryError`] to a restart disposition: a clean
/// `Permanent` cannot be helped by a fresh upload; everything else can.
fn failure_kind(err: &HttpRetryError) -> MultipartFailureKind {
    if matches!(err, HttpRetryError::Permanent(_)) {
        MultipartFailureKind::Permanent
    } else {
        MultipartFailureKind::Retryable
    }
}

/// Extract the `<Code>` from an S3 error document (non-2xx or the 2xx-embedded
/// form). Returns an owned `String` so it does not borrow the consumed body.
fn parse_s3_error_code(body: &str) -> Option<String> {
    quick_xml::de::from_str::<S3ErrorDoc>(body)
        .ok()
        .map(|e| e.code)
}

/// S3 error `<Code>`s that should be retried (re-signed + re-sent) rather than
/// treated as permanent. Extensible. `RequestTimeout` is a *retryable* HTTP 400.
fn is_retryable_s3_code(code: &str) -> bool {
    matches!(
        code,
        "InternalError" | "SlowDown" | "ServiceUnavailable" | "RequestTimeout"
    )
}

/// Classify an S3 error response (status + parsed `<Code>`) for retry. Reads the
/// retryable-code allowlist first, then falls back to status. Produces a bare
/// detail message (no `S3 <op> <key>:` prefix — [`s3_error`] adds that).
fn classify_s3_error(status: u16, code: Option<&str>) -> HttpRetryError {
    let message = match code {
        Some(c) => format!("HTTP {status}: {c}"),
        None => format!("HTTP {status}"),
    };
    if code.is_some_and(is_retryable_s3_code) || status == 429 || status >= 500 {
        HttpRetryError::RetryableStatus {
            code: status,
            message,
        }
    } else {
        HttpRetryError::Permanent(message)
    }
}

/// Derive the multipart part size for an object of `len` bytes.
///
/// Keeps the 16 MiB baseline for everything up to `16 MiB × S3_MAX_PARTS`
/// (156.25 GiB — all packs are well under this), grows it (rounded up to a MiB)
/// only when the object would otherwise need more than [`S3_MAX_PARTS`] parts,
/// and rejects objects whose derived part would exceed [`S3_MAX_PART_SIZE`]
/// (~48.8 TiB total) rather than emitting invalid oversized parts.
fn derive_part_size(len: usize, key: &str) -> Result<usize> {
    let rounded = len
        .div_ceil(S3_MAX_PARTS)
        .checked_next_multiple_of(1024 * 1024)
        .ok_or_else(|| {
            VykarError::Other(format!(
                "S3 multipart {key}: object size {len} too large for a multipart upload"
            ))
        })?;
    let part_size = MULTIPART_PART_SIZE.max(rounded);
    if part_size > S3_MAX_PART_SIZE {
        return Err(VykarError::Other(format!(
            "S3 multipart {key}: object size {len} exceeds the maximum representable \
             multipart upload ({S3_MAX_PARTS} parts × {S3_MAX_PART_SIZE} bytes)"
        )));
    }
    Ok(part_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3_backend::test_support::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    // ── 12. S3 multipart upload ─────────────────────────────────────

    /// Smallest object that takes the multipart path: 16 MiB + 1 → exactly two
    /// parts (one full 16 MiB part + a 1-byte tail).
    const MULTIPART_DATA_LEN: usize = MULTIPART_THRESHOLD + 1;

    /// A scripted reply for one request in a multipart sequence.
    #[derive(Clone)]
    enum MpReply {
        /// Read the full request (headers + Content-Length body), then send this
        /// response.
        Full(String),
        /// Read headers + only `n` request-body bytes, then RST the socket.
        /// Reproduces a broken pipe *while uploading a part* (the #151 failure),
        /// which the truncated-*response* idiom cannot.
        ResetAfterBody(usize),
        /// Read the full request, then close without sending any response, so the
        /// client's `.send()` itself errors. Models a lost response *after* the
        /// server received (and possibly committed) the request.
        NoResponse,
    }

    /// Scripted multipart mock: one reply per request, capturing each request's
    /// header lines and body bytes (the body bytes read so far for a reset).
    #[allow(clippy::type_complexity)]
    fn mock_server_script(
        replies: Vec<MpReply>,
    ) -> (
        u16,
        Arc<Mutex<Vec<(Vec<String>, Vec<u8>)>>>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let cc = Arc::clone(&captured);
        let handle = std::thread::spawn(move || {
            for reply in &replies {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                // Read headers manually so ResetAfterBody can stop mid-body.
                let mut lines = Vec::new();
                let mut content_len = 0usize;
                let mut line = String::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                    if let Some(v) = line.to_lowercase().strip_prefix("content-length:") {
                        content_len = v.trim().parse().unwrap_or(0);
                    }
                    lines.push(line.trim().to_string());
                }
                match reply {
                    MpReply::Full(response) => {
                        let mut body = vec![0u8; content_len];
                        if content_len > 0 {
                            reader.read_exact(&mut body).unwrap();
                        }
                        cc.lock().unwrap().push((lines, body));
                        stream.write_all(response.as_bytes()).unwrap();
                        stream.flush().unwrap();
                        drop(stream);
                    }
                    MpReply::ResetAfterBody(n) => {
                        let take = (*n).min(content_len);
                        let mut body = vec![0u8; take];
                        let _ = reader.read_exact(&mut body);
                        cc.lock().unwrap().push((lines, body));
                        // Close with the bulk of the request body still unread: the
                        // kernel sends RST, so the client's pending part-body write
                        // fails with a connection reset / broken pipe (the #151
                        // failure). Both fds (the original and the BufReader's clone)
                        // must drop for the socket to close.
                        drop(reader);
                        drop(stream);
                    }
                    MpReply::NoResponse => {
                        // Drain the full request, then close without responding → the
                        // client's `.send()` reads EOF instead of a status line and
                        // errors at the transport layer.
                        let mut body = vec![0u8; content_len];
                        if content_len > 0 {
                            reader.read_exact(&mut body).unwrap();
                        }
                        cc.lock().unwrap().push((lines, body));
                        drop(reader);
                        drop(stream);
                    }
                }
            }
        });
        (port, captured, handle)
    }

    fn mp_http_xml(status_line: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// 2xx `InitiateMultipartUploadResult` carrying `upload_id`. The
    /// `xmlns` namespace matches real S3 output; rusty-s3 0.10's instant-xml
    /// parser enforces it (quick-xml, used through 0.9, ignored namespaces).
    fn mp_create_ok(upload_id: &str) -> MpReply {
        MpReply::Full(mp_http_xml(
            "200 OK",
            &format!(
                "<?xml version=\"1.0\"?>\
                 <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <Bucket>test-bucket</Bucket><Key>k</Key>\
                 <UploadId>{upload_id}</UploadId></InitiateMultipartUploadResult>"
            ),
        ))
    }

    /// 2xx part response carrying an `ETag` header (quotes preserved).
    fn mp_part_ok(etag: &str) -> MpReply {
        MpReply::Full(format!(
            "HTTP/1.1 200 OK\r\nETag: {etag}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        ))
    }

    /// 2xx `CompleteMultipartUploadResult` (genuine success: carries an ETag).
    fn mp_complete_ok() -> MpReply {
        MpReply::Full(mp_http_xml(
            "200 OK",
            "<?xml version=\"1.0\"?><CompleteMultipartUploadResult>\
             <Location>http://x/k</Location><Bucket>test-bucket</Bucket>\
             <Key>k</Key><ETag>\"merged-etag\"</ETag></CompleteMultipartUploadResult>",
        ))
    }

    /// An S3 `<Error>` document at the given status (works for non-2xx and, with a
    /// 2xx status line, the 200-with-error footgun).
    fn mp_err(status_line: &str, code: &str) -> MpReply {
        MpReply::Full(mp_http_xml(status_line, &s3_error_xml(code, code)))
    }

    /// A bodiless status response (e.g. 204 abort, 503, 404 HEAD).
    fn mp_status(status_line: &str) -> MpReply {
        MpReply::Full(format!(
            "HTTP/1.1 {status_line}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        ))
    }

    /// A 200 that declares more body than it sends → the client's body read fails
    /// (lost-response signature: ambiguous completion).
    fn mp_truncated_2xx() -> MpReply {
        MpReply::Full(
            "HTTP/1.1 200 OK\r\nContent-Length: 500\r\nConnection: close\r\n\r\n<short>"
                .to_string(),
        )
    }

    /// A 200 HEAD response advertising `len` bytes (object present at that size).
    fn mp_head_len(len: usize) -> MpReply {
        MpReply::Full(format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n"
        ))
    }

    fn req_line(captured: &[(Vec<String>, Vec<u8>)], i: usize) -> &str {
        &captured[i].0[0]
    }

    /// Count `CreateMultipartUpload` requests (POST `?uploads=1`).
    fn mp_creates(captured: &[(Vec<String>, Vec<u8>)]) -> usize {
        captured
            .iter()
            .filter(|(lines, _)| lines[0].starts_with("POST ") && lines[0].contains("uploads=1"))
            .count()
    }

    #[test]
    fn multipart_threshold_exact_16mib_uses_single_put() {
        // Exactly the threshold stays a single PUT — no multipart create.
        let (port, captured, handle) = mock_server_script(vec![mp_status("200 OK")]);
        let backend = s3_backend(port, no_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_THRESHOLD])
            .unwrap();
        let cap = captured.lock().unwrap();
        assert_eq!(cap.len(), 1);
        assert!(
            req_line(&cap, 0).starts_with("PUT "),
            "got: {}",
            req_line(&cap, 0)
        );
        assert!(
            !req_line(&cap, 0).contains("uploads=1"),
            "should not be multipart: {}",
            req_line(&cap, 0)
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_threshold_plus_one_uses_multipart() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, no_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        let cap = captured.lock().unwrap();
        assert_eq!(mp_creates(&cap), 1);
        assert_eq!(cap.len(), 4, "create + 2 parts + complete");
        handle.join().unwrap();
    }

    #[test]
    fn derive_part_size_boundary_growth_and_rejection() {
        // 16 MiB × 10_000 = 156.25 GiB → still the 16 MiB baseline.
        let exact = MULTIPART_PART_SIZE * S3_MAX_PARTS;
        assert_eq!(derive_part_size(exact, "k").unwrap(), MULTIPART_PART_SIZE);
        // One byte above → grows so the count stays <= S3_MAX_PARTS, part >= 5 MiB.
        let ps = derive_part_size(exact + 1, "k").unwrap();
        assert!(
            ps > MULTIPART_PART_SIZE,
            "part must grow above the baseline"
        );
        assert!(
            (exact + 1).div_ceil(ps) <= S3_MAX_PARTS,
            "count must stay bounded"
        );
        assert!(
            ps >= 5 * 1024 * 1024,
            "each part must be >= the 5 MiB S3 minimum"
        );
        // Above S3_MAX_PARTS × S3_MAX_PART_SIZE (~48.8 TiB) → rejected, not oversized.
        let too_big = S3_MAX_PARTS
            .saturating_mul(S3_MAX_PART_SIZE)
            .saturating_add(1);
        let err = derive_part_size(too_big, "k").unwrap_err().to_string();
        assert!(
            err.contains("exceeds the maximum representable"),
            "got: {err}"
        );
    }

    #[test]
    fn multipart_happy_path_ordered_parts_and_etags() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"etag-1\""),
            mp_part_ok("\"etag-2\""),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, no_retry(), false);
        backend
            .put("packs/ab/obj", &vec![7u8; MULTIPART_DATA_LEN])
            .unwrap();
        let cap = captured.lock().unwrap();
        // Order: create → part 1 → part 2 → complete.
        assert!(req_line(&cap, 0).starts_with("POST ") && req_line(&cap, 0).contains("uploads=1"));
        assert!(
            req_line(&cap, 1).starts_with("PUT ") && req_line(&cap, 1).contains("partNumber=1")
        );
        assert!(
            req_line(&cap, 2).starts_with("PUT ") && req_line(&cap, 2).contains("partNumber=2")
        );
        assert!(
            req_line(&cap, 3).starts_with("POST ")
                && req_line(&cap, 3).contains("uploadId=")
                && !req_line(&cap, 3).contains("uploads=1")
        );
        // Completion XML carries the ETags in part order, quotes preserved.
        // rusty-s3 0.10 (instant-xml) XML-escapes the ETag's quotes to `&quot;`
        // (equally valid; S3 decodes it). quick-xml, used through 0.9, emitted
        // raw `"`.
        let body = String::from_utf8_lossy(&cap[3].1);
        let p1 = body
            .find("&quot;etag-1&quot;")
            .expect("etag-1 in completion body");
        let p2 = body
            .find("&quot;etag-2&quot;")
            .expect("etag-2 in completion body");
        assert!(p1 < p2, "ETags must be in part order: {body}");
        assert!(body.contains("<PartNumber>1</PartNumber>"));
        assert!(body.contains("<PartNumber>2</PartNumber>"));
        handle.join().unwrap();
    }

    #[test]
    fn multipart_mid_part_reset_then_succeeds() {
        // The #151 failure: socket reset mid part-body → the part is retried.
        let (port, _cap, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            MpReply::ResetAfterBody(4096),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, fast_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn multipart_permanent_part_failure_aborts_no_restart() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_err("403 Forbidden", "AccessDenied"),
            mp_status("204 No Content"),
        ]);
        let backend = s3_backend(port, fast_retry(), false);
        let err = backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap_err()
            .to_string();
        assert!(err.contains("AccessDenied"), "got: {err}");
        let cap = captured.lock().unwrap();
        assert_eq!(mp_creates(&cap), 1, "no restart on a permanent failure");
        assert_eq!(cap.len(), 3, "create + part + abort");
        assert!(
            req_line(&cap, 2).starts_with("DELETE "),
            "abort: {}",
            req_line(&cap, 2)
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_missing_part_etag_aborts() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_status("200 OK"), // part response with no ETag header
            mp_status("204 No Content"),
        ]);
        let backend = s3_backend(port, fast_retry(), false);
        let err = backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing ETag"), "got: {err}");
        let cap = captured.lock().unwrap();
        assert!(req_line(&cap, cap.len() - 1).starts_with("DELETE "));
        handle.join().unwrap();
    }

    #[test]
    fn multipart_part_no_such_upload_restarts() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_err("404 Not Found", "NoSuchUpload"),
            mp_status("204 No Content"), // abort u1
            mp_create_ok("u2"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, no_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        let cap = captured.lock().unwrap();
        assert_eq!(
            mp_creates(&cap),
            2,
            "NoSuchUpload on a part must restart the MPU"
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_request_timeout_on_create_is_retryable() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_err("400 Bad Request", "RequestTimeout"),
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, fast_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        let cap = captured.lock().unwrap();
        assert_eq!(
            mp_creates(&cap),
            2,
            "RequestTimeout (HTTP 400) on create is retryable"
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_request_timeout_on_part_is_retryable() {
        let (port, _cap, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_err("400 Bad Request", "RequestTimeout"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, fast_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn multipart_truncated_part_error_body_retries() {
        // A 4xx whose error body is cut mid-read must be retried as BodyIo, not
        // misclassified permanent from an empty <Code> (e.g. a cut RequestTimeout).
        let truncated_400 = MpReply::Full(
            "HTTP/1.1 400 Bad Request\r\nContent-Length: 500\r\nConnection: close\r\n\r\n<cut>"
                .to_string(),
        );
        let (port, _cap, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            truncated_400,
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, fast_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn multipart_malformed_create_restarts_once_then_fails() {
        // 2xx whose body has no UploadId → unparseable → one outer restart, then
        // failure (recorded as a residual unknown MPU; never "nothing created").
        let malformed = || {
            MpReply::Full(mp_http_xml(
                "200 OK",
                "<InitiateMultipartUploadResult><Bucket>b</Bucket></InitiateMultipartUploadResult>",
            ))
        };
        let (port, captured, handle) = mock_server_script(vec![malformed(), malformed()]);
        let backend = s3_backend(port, no_retry(), false);
        let err = backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap_err()
            .to_string();
        assert!(err.contains("unreadable upload id"), "got: {err}");
        let cap = captured.lock().unwrap();
        assert_eq!(mp_creates(&cap), 2, "exactly one outer restart");
        assert_eq!(cap.len(), 2, "no parts or abort issued");
        handle.join().unwrap();
    }

    #[test]
    fn multipart_embedded_transient_completion_errors_retry() {
        for code in ["SlowDown", "ServiceUnavailable", "RequestTimeout"] {
            let (port, _cap, handle) = mock_server_script(vec![
                mp_create_ok("u1"),
                mp_part_ok("\"e1\""),
                mp_part_ok("\"e2\""),
                mp_err("200 OK", code), // 200-with-error footgun, transient
                mp_complete_ok(),
            ]);
            let backend = s3_backend(port, fast_retry(), false);
            backend
                .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
                .unwrap_or_else(|e| panic!("{code} should retry to success: {e}"));
            handle.join().unwrap();
        }
    }

    #[test]
    fn multipart_truncated_completion_body_retries_then_succeeds() {
        // 200 that parses as neither result nor <Error> → retried; later success.
        let (port, _cap, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            MpReply::Full(mp_http_xml("200 OK", "<NotACompletionResult/>")),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, fast_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn multipart_lost_completion_packs_reconciles_via_head() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_truncated_2xx(),              // complete: body cut → ambiguous
            mp_status("204 No Content"),     // abort
            mp_head_len(MULTIPART_DATA_LEN), // reconcile HEAD: present, right size
        ]);
        let backend = s3_backend(port, no_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        let cap = captured.lock().unwrap();
        assert!(
            req_line(&cap, cap.len() - 1).starts_with("HEAD "),
            "reconcile via HEAD: {}",
            req_line(&cap, cap.len() - 1)
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_lost_completion_transport_error_reconciles() {
        // `.send()` itself errors (server reads the request, then closes with no
        // response): a committed request whose response was lost at the transport
        // layer, before any response body. The handler never runs, so ambiguity
        // must be set on the send-error path — otherwise reconciliation is skipped.
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            MpReply::NoResponse,             // complete: no response at all
            mp_status("204 No Content"),     // abort
            mp_head_len(MULTIPART_DATA_LEN), // reconcile HEAD: present
        ]);
        let backend = s3_backend(port, no_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        let cap = captured.lock().unwrap();
        assert!(
            req_line(&cap, cap.len() - 1).starts_with("HEAD "),
            "a lost completion response (transport error) must still reconcile: {}",
            req_line(&cap, cap.len() - 1)
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_complete_no_such_upload_reconciles() {
        // Both the 404 and the 200-with-<Error> forms of NoSuchUpload.
        for status in ["404 Not Found", "200 OK"] {
            let (port, captured, handle) = mock_server_script(vec![
                mp_create_ok("u1"),
                mp_part_ok("\"e1\""),
                mp_part_ok("\"e2\""),
                mp_err(status, "NoSuchUpload"),
                mp_status("204 No Content"),
                mp_head_len(MULTIPART_DATA_LEN),
            ]);
            let backend = s3_backend(port, no_retry(), false);
            backend
                .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
                .unwrap_or_else(|e| panic!("{status} NoSuchUpload should reconcile: {e}"));
            let cap = captured.lock().unwrap();
            assert!(req_line(&cap, cap.len() - 1).starts_with("HEAD "));
            handle.join().unwrap();
        }
    }

    #[test]
    fn multipart_rooted_reconcile_targets_single_prefixed_path() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_truncated_2xx(),
            mp_status("204 No Content"),
            mp_head_len(MULTIPART_DATA_LEN),
        ]);
        let backend = s3_backend_rooted(port, no_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        let cap = captured.lock().unwrap();
        let head = req_line(&cap, cap.len() - 1);
        assert!(head.starts_with("HEAD "), "got: {head}");
        assert!(
            head.contains("/test-bucket/backups/vykar/packs/ab/obj"),
            "reconcile must hit the single-prefixed path: {head}"
        );
        assert!(
            !head.contains("backups/vykar/backups/vykar"),
            "double-prefix bug: {head}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_pack_absent_after_ambiguous_restarts() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_truncated_2xx(),
            mp_status("204 No Content"),
            mp_status("404 Not Found"), // reconcile HEAD: absent
            mp_create_ok("u2"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, no_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        let cap = captured.lock().unwrap();
        assert_eq!(mp_creates(&cap), 2, "absent object → full restart");
        handle.join().unwrap();
    }

    #[test]
    fn multipart_mutable_key_lost_completion_byte_compare_equal() {
        let data = vec![b'A'; MULTIPART_DATA_LEN];
        let body = String::from_utf8(data.clone()).unwrap();
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_truncated_2xx(),
            mp_status("204 No Content"),
            MpReply::Full(mp_http_xml("200 OK", &body)), // reconcile GET: equal bytes
        ]);
        let backend = s3_backend(port, no_retry(), false);
        backend.put("index", &data).unwrap();
        let cap = captured.lock().unwrap();
        assert!(
            req_line(&cap, cap.len() - 1).starts_with("GET "),
            "mutable key reconciles via byte-compare GET: {}",
            req_line(&cap, cap.len() - 1)
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_mutable_key_different_bytes_not_reconciled() {
        let data = vec![b'A'; MULTIPART_DATA_LEN];
        let other = String::from_utf8(vec![b'B'; MULTIPART_DATA_LEN]).unwrap();
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_truncated_2xx(),
            mp_status("204 No Content"),
            MpReply::Full(mp_http_xml("200 OK", &other)), // reconcile GET: different bytes
            mp_create_ok("u2"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, no_retry(), false);
        backend.put("index", &data).unwrap();
        let cap = captured.lock().unwrap();
        assert_eq!(
            mp_creates(&cap),
            2,
            "different bytes must not reconcile; a size-only check would be wrong here"
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_reconcile_error_does_not_abort_loop() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_truncated_2xx(),
            mp_status("204 No Content"),
            mp_status("500 Internal Server Error"), // reconcile HEAD errors
            mp_create_ok("u2"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_complete_ok(),
        ]);
        let backend = s3_backend(port, no_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        let cap = captured.lock().unwrap();
        assert_eq!(
            mp_creates(&cap),
            2,
            "an inconclusive reconcile must not abort the loop"
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_embedded_permanent_completion_error_no_reconcile() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_err("200 OK", "AccessDenied"), // 200-with-error, permanent
            mp_status("204 No Content"),
        ]);
        let backend = s3_backend(port, no_retry(), false);
        let err = backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap_err()
            .to_string();
        assert!(err.contains("AccessDenied"), "got: {err}");
        let cap = captured.lock().unwrap();
        assert_eq!(cap.len(), 5, "create + 2 parts + complete + abort");
        assert!(
            cap.iter()
                .all(|(l, _)| !l[0].starts_with("HEAD ") && !l[0].starts_with("GET ")),
            "ambiguous == false must not reconcile"
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_inner_retry_ambiguity_survives_later_5xx() {
        // First complete attempt commits then its response is cut (cell set); later
        // attempts return 503 to exhaustion. The cell keeps ambiguity → reconcile.
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_truncated_2xx(),
            mp_status("503 Service Unavailable"),
            mp_status("503 Service Unavailable"),
            mp_status("204 No Content"),
            mp_head_len(MULTIPART_DATA_LEN),
        ]);
        let backend = s3_backend(port, fast_retry(), false);
        backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap();
        let cap = captured.lock().unwrap();
        assert!(
            req_line(&cap, cap.len() - 1).starts_with("HEAD "),
            "ambiguity must survive a later clean 503: {}",
            req_line(&cap, cap.len() - 1)
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_reconcile_absent_exhausted_returns_original_error() {
        let (port, _cap, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_truncated_2xx(),
            mp_status("204 No Content"),
            mp_status("404 Not Found"),
            mp_create_ok("u2"),
            mp_part_ok("\"e1\""),
            mp_part_ok("\"e2\""),
            mp_truncated_2xx(),
            mp_status("204 No Content"),
            mp_status("404 Not Found"),
        ]);
        let backend = s3_backend(port, no_retry(), false);
        let err = backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("COMPLETE_MPU"),
            "must surface the original completion error, not the reconcile result: {err}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn multipart_abort_no_such_upload_is_quiet() {
        let (port, captured, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_err("403 Forbidden", "AccessDenied"),
            mp_status("404 Not Found"), // abort: NoSuchUpload → quiet success
        ]);
        let backend = s3_backend(port, no_retry(), false);
        let err = backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("AccessDenied"),
            "original error preserved: {err}"
        );
        let cap = captured.lock().unwrap();
        assert!(req_line(&cap, cap.len() - 1).starts_with("DELETE "));
        handle.join().unwrap();
    }

    #[test]
    fn multipart_abort_failure_does_not_mask_original_error() {
        let (port, _cap, handle) = mock_server_script(vec![
            mp_create_ok("u1"),
            mp_err("403 Forbidden", "AccessDenied"),
            mp_status("500 Internal Server Error"), // abort retried (fast_retry)
            mp_status("500 Internal Server Error"),
            mp_status("500 Internal Server Error"),
        ]);
        let backend = s3_backend(port, fast_retry(), false);
        let err = backend
            .put("packs/ab/obj", &vec![0u8; MULTIPART_DATA_LEN])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("AccessDenied"),
            "an abort failure must not replace the original upload error: {err}"
        );
        handle.join().unwrap();
    }
}
