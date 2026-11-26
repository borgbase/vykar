//! Put-if-absent on S3: `If-None-Match: *` as a signed header on PutObject.
//! Shared with the step-0 probe (`probe.rs`), which sends the same request
//! once, without the retry layer.

use base64::Engine;
use md5::{Digest, Md5};
use rusty_s3::S3Action;
use vykar_types::error::{Result, VykarError};

use super::multipart::{classify_s3_error, parse_s3_error_code};
use super::services::ServiceStatus;
use super::{PRESIGN_DURATION, S3Backend, s3_error};
use crate::conditional::{after_verified, check_args};
use crate::retry::HttpRetryError;
use crate::{
    ConditionalSupport, ConditionalWrite, Created, S3ConditionalWrites, put_if_absent_unavailable,
};

pub(super) const OCTET_STREAM: &str = "application/octet-stream";

/// A signed PutObject, ready to send.
pub(super) struct PreparedPut {
    pub(super) url: String,
    pub(super) content_md5: String,
    pub(super) if_none_match: bool,
}

/// Sign a PutObject of `body` to `full_key`. `if_none_match` adds
/// `if-none-match: *` to the signed headers; the sender must send it too.
pub(super) fn prepare_put(
    b: &S3Backend,
    full_key: &str,
    body: &[u8],
    if_none_match: bool,
) -> PreparedPut {
    let content_md5 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(body));
    let mut action = b.bucket.put_object(Some(&b.credentials), full_key);
    action.headers_mut().insert("content-type", OCTET_STREAM);
    action.headers_mut().insert("content-md5", &content_md5);
    if if_none_match {
        action.headers_mut().insert("if-none-match", "*");
    }
    PreparedPut {
        url: action.sign(PRESIGN_DURATION).to_string(),
        content_md5,
        if_none_match,
    }
}

/// Whether an S3 answer to a conditional request means "this endpoint does
/// not implement the condition" rather than "it failed": 501, a
/// `NotImplemented` code, or 400 with `InvalidArgument`/`InvalidRequest`.
pub(crate) fn s3_refuses_mechanism(status: u16, code: Option<&str>) -> bool {
    status == 501
        || code == Some("NotImplemented")
        || (status == 400 && matches!(code, Some("InvalidArgument" | "InvalidRequest")))
}

/// How S3 answered a conditional PutObject.
enum Answer {
    Created,
    Exists,
    /// The endpoint does not implement the condition; the detail names why.
    Refused(String),
}

impl S3Backend {
    /// Why put-if-absent is refused on this endpoint without a request: its
    /// host belongs to a known-bad service. `None` otherwise.
    fn known_bad(&self) -> Option<String> {
        let s = self
            .service
            .filter(|s| s.status == ServiceStatus::KnownBad)?;
        Some(format!(
            "S3 endpoint is {} ({}), which lets two concurrent creates of one key both \
             succeed ({}); put-if-absent is refused there whatever s3_conditional_writes says",
            s.name, s.hosts, s.evidence
        ))
    }

    /// The check's answer on a known-bad host, cached as a failed check's
    /// would be, so `conditional()` turns `None`. No request is sent.
    fn refuse_known_bad(&self) -> Option<ConditionalSupport> {
        self.known_bad().map(|reason| self.cond.refuse(reason))
    }

    /// Whether `conditional()` offers put-if-absent, before any check.
    /// `On` stays true on a known-bad host, so the refusal surfaces with its
    /// reason (the check answers `Unsupported`).
    pub(super) fn conditional_offered(&self) -> bool {
        match self.conditional_mode {
            S3ConditionalWrites::On => true,
            S3ConditionalWrites::Auto => self.service.is_some_and(|s| s.status.offered_by_auto()),
            S3ConditionalWrites::Off => false,
        }
    }

    /// The unverified put-if-absent primitive: one signed PutObject with
    /// `If-None-Match: *`, retried like `put` (and on 409
    /// `ConditionalRequestConflict`). Never multipart: `check_args` caps the
    /// body at [`crate::MAX_PUT_IF_ABSENT_BYTES`].
    pub(super) fn put_if_absent_raw(&self, key: &str, data: &[u8]) -> Result<Created> {
        let full_key = self.full_key(key);
        let p = prepare_put(self, &full_key, data, true);
        let answer = self
            .retry_call(
                &format!("PUT_IF_ABSENT {key}"),
                || {
                    self.agent
                        .put(&p.url)
                        .header("content-type", OCTET_STREAM)
                        .header("content-md5", &p.content_md5)
                        .header("if-none-match", "*")
                        .send(data)
                },
                |mut resp| {
                    let status = resp.status().as_u16();
                    if (200..300).contains(&status) {
                        return Ok(Answer::Created);
                    }
                    if status == 412 {
                        return Ok(Answer::Exists);
                    }
                    if status == 409 {
                        return Err(HttpRetryError::RetryableStatus {
                            code: status,
                            message: "HTTP 409: concurrent conditional request".into(),
                        });
                    }
                    let body = resp.body_mut().read_to_string().unwrap_or_default();
                    let code = parse_s3_error_code(&body);
                    if s3_refuses_mechanism(status, code.as_deref()) {
                        return Ok(Answer::Refused(match code {
                            Some(c) => format!("{status} {c}"),
                            None => status.to_string(),
                        }));
                    }
                    if status < 400 {
                        return Err(HttpRetryError::Permanent(format!(
                            "unexpected HTTP {status}"
                        )));
                    }
                    tracing::debug!("S3 PUT_IF_ABSENT {key}: HTTP {status}: {body}");
                    Err(classify_s3_error(status, code.as_deref()))
                },
            )
            .map_err(|e| s3_error("PUT_IF_ABSENT", key, e))?;
        match answer {
            Answer::Created => Ok(Created::Yes),
            Answer::Exists => Ok(Created::AlreadyExists),
            Answer::Refused(detail) => Err(VykarError::UnsupportedBackend(format!(
                "S3 endpoint refuses If-None-Match (s3_conditional_writes): {detail}"
            ))),
        }
    }
}

/// The label names the setting, so an "unavailable" error tells the user
/// what to change.
const LABEL: &str = "S3 (s3_conditional_writes)";

impl ConditionalWrite for S3Backend {
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<Created> {
        check_args(key, data)?;
        if let Some(ConditionalSupport::Unsupported { reason }) = self.refuse_known_bad() {
            return Err(put_if_absent_unavailable(format_args!("{LABEL}: {reason}")));
        }
        self.cond
            .require(LABEL, self, &|k, d| self.put_if_absent_raw(k, d))?;
        after_verified(self.put_if_absent_raw(key, data))
    }

    fn verify(&self) -> Result<ConditionalSupport> {
        if let Some(answer) = self.refuse_known_bad() {
            return Ok(answer);
        }
        self.cond.verify(self, &|k, d| self.put_if_absent_raw(k, d))
    }

    fn verify_at_init(&self) -> Result<ConditionalSupport> {
        if let Some(answer) = self.refuse_known_bad() {
            return Ok(answer);
        }
        self.cond
            .verify_at_init(self, &|k, d| self.put_if_absent_raw(k, d))
    }
}

/// Parse an S3 `LastModified` (RFC 3339, e.g. `2024-01-01T00:00:00.000Z`).
/// A malformed value gives `None` ("not reported"), never an error.
pub(super) fn parse_s3_time(s: &str) -> Option<std::time::SystemTime> {
    match chrono::DateTime::parse_from_rfc3339(s) {
        Ok(t) => Some(std::time::SystemTime::from(t)),
        Err(e) => {
            tracing::debug!("S3 LIST: unparsable LastModified {s:?}: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusal_rule() {
        assert!(s3_refuses_mechanism(501, None));
        assert!(s3_refuses_mechanism(400, Some("InvalidRequest")));
        assert!(s3_refuses_mechanism(400, Some("InvalidArgument")));
        assert!(s3_refuses_mechanism(403, Some("NotImplemented")));
        assert!(!s3_refuses_mechanism(400, Some("BadDigest")));
        assert!(!s3_refuses_mechanism(403, Some("AccessDenied")));
        assert!(!s3_refuses_mechanism(412, None));
    }

    #[test]
    fn last_modified_parses() {
        assert_eq!(
            parse_s3_time("2024-01-01T00:00:00.000Z"),
            Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_704_067_200))
        );
        assert_eq!(parse_s3_time("yesterday"), None);
    }
}
