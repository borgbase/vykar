use std::collections::HashSet;
use std::io::Read;
use std::sync::atomic::Ordering;

use rusty_s3::S3Action;
use rusty_s3::actions::{DeleteObjectsResponse, ErrorObject, ObjectIdentifier};
use vykar_types::error::Result;

use super::multipart::{classify_s3_error, is_retryable_s3_code, parse_s3_error_code};
use super::{PRESIGN_DURATION, S3Backend, s3_error};
use crate::StorageBackend;
use crate::retry::HttpRetryError;

/// S3 refuses a DeleteObjects request naming more than 1000 keys.
pub(super) const DELETE_OBJECTS_MAX_KEYS: usize = 1000;

/// How many failed keys an error message names before summarising.
const REPORTED_FAILURES: usize = 10;

/// What one DeleteObjects request came to, when it did not fail.
#[derive(Debug, PartialEq, Eq)]
enum ChunkOutcome {
    /// Every named key is gone (or was absent).
    Deleted,
    /// The endpoint does not implement DeleteObjects.
    NotImplemented,
}

#[allow(clippy::result_large_err)]
impl S3Backend {
    /// [`crate::BatchDelete::delete_keys`] for S3: one quiet DeleteObjects
    /// request per 1000 distinct keys, in order, stopping at the first
    /// request that fails.
    ///
    /// Under `s3_soft_delete`, or once the endpoint answered NotImplemented
    /// (in this call or earlier), the remaining distinct keys are deleted one
    /// by one with [`StorageBackend::delete`] (tombstones under soft delete),
    /// stopping at the first error. A per-key error then surfaces as this
    /// call's `Err`; compact answers that by warning and deleting every key
    /// again, which costs extra DELETEs but reaches the same end state.
    pub(super) fn delete_objects(&self, keys: &[String]) -> Result<()> {
        let mut seen = HashSet::new();
        let unique: Vec<&str> = keys
            .iter()
            .map(String::as_str)
            .filter(|k| seen.insert(*k))
            .collect();
        if self.soft_delete || self.batch_delete_unsupported.load(Ordering::Relaxed) {
            return self.delete_one_by_one(&unique);
        }
        for (i, chunk) in unique.chunks(DELETE_OBJECTS_MAX_KEYS).enumerate() {
            match self.delete_objects_chunk(chunk)? {
                ChunkOutcome::Deleted => {}
                ChunkOutcome::NotImplemented => {
                    let rest = unique
                        .get(i * DELETE_OBJECTS_MAX_KEYS..)
                        .unwrap_or_default();
                    return self.delete_one_by_one(rest);
                }
            }
        }
        Ok(())
    }

    /// Per-key deletes, stopping at the first error.
    fn delete_one_by_one(&self, keys: &[&str]) -> Result<()> {
        for key in keys {
            StorageBackend::delete(self, key)?;
        }
        Ok(())
    }

    /// One DeleteObjects request for at most [`DELETE_OBJECTS_MAX_KEYS`] keys.
    /// `NotImplemented` also sets `batch_delete_unsupported`.
    fn delete_objects_chunk(&self, chunk: &[&str]) -> Result<ChunkOutcome> {
        let objects: Vec<ObjectIdentifier> = chunk
            .iter()
            .map(|k| ObjectIdentifier::new(self.full_key(k)))
            .collect();
        // body_with_md5 consumes the action and the MD5 must be a signed
        // header, so one action builds the body and a second one signs.
        let mut body_action = self
            .bucket
            .delete_objects(Some(&self.credentials), objects.iter());
        body_action.set_quiet(true);
        let (body, content_md5) = body_action.body_with_md5();
        let mut action = self
            .bucket
            .delete_objects(Some(&self.credentials), objects.iter());
        action
            .headers_mut()
            .insert("content-type", "application/xml");
        action
            .headers_mut()
            .insert("content-md5", content_md5.as_str());
        let url = action.sign(PRESIGN_DURATION);

        let label = format!("{} keys", chunk.len());
        let outcome = self
            .retry_call(
                &format!("DELETE_OBJECTS {label}"),
                || {
                    self.agent
                        .post(url.as_str())
                        .header("content-type", "application/xml")
                        .header("content-md5", &content_md5)
                        .send(body.as_bytes())
                },
                |mut resp| {
                    let status = resp.status().as_u16();
                    let mut text = String::new();
                    resp.body_mut()
                        .as_reader()
                        .read_to_string(&mut text)
                        .map_err(HttpRetryError::BodyIo)?;
                    handle_delete_objects_response(status, &text, &self.root, chunk.len())
                },
            )
            .map_err(|e| s3_error("DELETE_OBJECTS", &label, e))?;
        if outcome == ChunkOutcome::NotImplemented {
            self.batch_delete_unsupported.store(true, Ordering::Relaxed);
        }
        Ok(outcome)
    }
}

/// Classify one DeleteObjects response (status and whole body) for
/// `retry_http`. `root` is `S3Backend::root`, used to report keys relative.
fn handle_delete_objects_response(
    status: u16,
    body: &str,
    root: &str,
    requested: usize,
) -> std::result::Result<ChunkOutcome, HttpRetryError> {
    let code = parse_s3_error_code(body);
    // Checked before classification, which would retry a 501 as a 5xx.
    if status == 501 || code.as_deref() == Some("NotImplemented") {
        return Ok(ChunkOutcome::NotImplemented);
    }
    if !(200..300).contains(&status) {
        let display_body = if body.len() > 1024 {
            format!("{}...(truncated)", &body[..body.floor_char_boundary(1024)])
        } else {
            body.to_string()
        };
        tracing::debug!("S3 DELETE_OBJECTS: HTTP {status}: {display_body}");
        return Err(classify_s3_error(status, code.as_deref()));
    }
    if let Ok(mut parsed) = DeleteObjectsResponse::parse(body) {
        // An absent key is not an error. AWS omits it; some stores report it,
        // and a re-sent chunk names keys the first attempt already deleted.
        parsed.errors.retain(|e| e.code != "NoSuchKey");
        if parsed.errors.is_empty() {
            return Ok(ChunkOutcome::Deleted);
        }
        let message = describe_failures(&parsed.errors, root, requested);
        return if parsed.errors.iter().all(|e| is_retryable_s3_code(&e.code)) {
            Err(HttpRetryError::RetryableStatus {
                code: status,
                message,
            })
        } else {
            Err(HttpRetryError::Permanent(message))
        };
    }
    if let Some(c) = code {
        return Err(classify_s3_error(status, Some(&c)));
    }
    // Includes an empty body: success must be stated by a DeleteResult.
    Err(HttpRetryError::RetryableStatus {
        code: status,
        message: "malformed DeleteObjects response body".into(),
    })
}

/// "n of requested key(s) not deleted: k: Code (Message); ...; and m more".
fn describe_failures(errors: &[ErrorObject], root: &str, requested: usize) -> String {
    let prefix = format!("{root}/");
    let list = errors
        .iter()
        .take(REPORTED_FAILURES)
        .map(|e| {
            let key = if root.is_empty() {
                e.key.as_str()
            } else {
                e.key.strip_prefix(&prefix).unwrap_or(&e.key)
            };
            format!("{key}: {} ({})", e.code, e.message)
        })
        .collect::<Vec<_>>()
        .join("; ");
    let mut message = format!("{} of {requested} key(s) not deleted: {list}", errors.len());
    if errors.len() > REPORTED_FAILURES {
        message.push_str(&format!("; and {} more", errors.len() - REPORTED_FAILURES));
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3_backend::test_support::{
        fast_retry, no_retry, s3_backend, s3_backend_rooted, s3_error_xml,
    };
    use crate::s3_backend::tests::mock_server_capture_bodies;
    use crate::{BatchDelete, BatchDeleteScope, StorageBackend};
    use base64::Engine;
    use md5::{Digest, Md5};
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use vykar_types::error::VykarError;

    const NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

    fn delete_result(errors: &[(&str, &str)]) -> String {
        let mut xml =
            format!(r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="{NS}">"#);
        for (key, code) in errors {
            xml.push_str(&format!(
                "<Error><Key>{key}</Key><Code>{code}</Code><Message>msg {code}</Message></Error>"
            ));
        }
        xml.push_str("</DeleteResult>");
        xml
    }

    fn handle(status: u16, body: &str) -> std::result::Result<ChunkOutcome, HttpRetryError> {
        handle_delete_objects_response(status, body, "", 3)
    }

    #[test]
    fn response_with_no_errors_is_deleted() {
        assert!(matches!(
            handle(200, &delete_result(&[])),
            Ok(ChunkOutcome::Deleted)
        ));
        assert!(matches!(
            handle(200, &format!(r#"<DeleteResult xmlns="{NS}"/>"#)),
            Ok(ChunkOutcome::Deleted)
        ));
    }

    #[test]
    fn empty_or_non_2xx_success_body_is_not_deleted() {
        assert!(matches!(
            handle(200, ""),
            Err(HttpRetryError::RetryableStatus { code: 200, .. })
        ));
        assert!(matches!(
            handle(200, "  \n"),
            Err(HttpRetryError::RetryableStatus { .. })
        ));
        assert!(matches!(
            handle(304, &delete_result(&[])),
            Err(HttpRetryError::Permanent(_))
        ));
    }

    #[test]
    fn per_key_access_denied_is_permanent() {
        let body = delete_result(&[
            ("packs/aa/1", "AccessDenied"),
            ("packs/bb/2", "AccessDenied"),
        ]);
        match handle(200, &body) {
            Err(HttpRetryError::Permanent(msg)) => {
                assert!(msg.contains("2 of 3 key(s) not deleted"), "{msg}");
                assert!(msg.contains("packs/aa/1"), "{msg}");
                assert!(msg.contains("packs/bb/2"), "{msg}");
                assert!(msg.contains("AccessDenied"), "{msg}");
            }
            Err(e) => panic!("expected Permanent, got {e}"),
            Ok(o) => panic!("expected Permanent, got {o:?}"),
        }
    }

    #[test]
    fn per_key_no_such_key_is_not_a_failure() {
        let only_absent = delete_result(&[("a", "NoSuchKey"), ("b", "NoSuchKey")]);
        assert!(matches!(
            handle(200, &only_absent),
            Ok(ChunkOutcome::Deleted)
        ));

        let mixed = delete_result(&[("a", "NoSuchKey"), ("b", "AccessDenied")]);
        assert!(matches!(
            handle(200, &mixed),
            Err(HttpRetryError::Permanent(m)) if m.contains("b: AccessDenied") && !m.contains("NoSuchKey")
        ));
    }

    #[test]
    fn per_key_only_retryable_codes_retry() {
        let body = delete_result(&[("a", "InternalError"), ("b", "SlowDown")]);
        assert!(matches!(
            handle(200, &body),
            Err(HttpRetryError::RetryableStatus { .. })
        ));
        let body = delete_result(&[("a", "InternalError"), ("b", "AccessDenied")]);
        assert!(matches!(
            handle(200, &body),
            Err(HttpRetryError::Permanent(_))
        ));
    }

    #[test]
    fn not_implemented_is_recognised() {
        assert!(matches!(handle(501, ""), Ok(ChunkOutcome::NotImplemented)));
        assert!(matches!(
            handle(400, &s3_error_xml("NotImplemented", "nope")),
            Ok(ChunkOutcome::NotImplemented)
        ));
    }

    #[test]
    fn malformed_2xx_retries_and_embedded_error_classifies() {
        assert!(matches!(
            handle(200, "garbage"),
            Err(HttpRetryError::RetryableStatus { .. })
        ));
        assert!(matches!(
            handle(200, &s3_error_xml("InternalError", "try again")),
            Err(HttpRetryError::RetryableStatus { .. })
        ));
        assert!(matches!(
            handle(200, &s3_error_xml("AccessDenied", "no")),
            Err(HttpRetryError::Permanent(_))
        ));
    }

    #[test]
    fn describe_failures_strips_root_and_caps() {
        let errors: Vec<ErrorObject> = (0..12)
            .map(|i| ErrorObject {
                key: format!("backups/vykar/packs/{i:02}"),
                version_id: None,
                code: "AccessDenied".into(),
                message: "denied".into(),
            })
            .collect();
        let msg = describe_failures(&errors, "backups/vykar", 20);
        assert!(
            msg.starts_with("12 of 20 key(s) not deleted: packs/00: AccessDenied (denied)"),
            "{msg}"
        );
        assert!(!msg.contains("backups/vykar"), "{msg}");
        assert!(msg.contains("packs/09"), "{msg}");
        assert!(!msg.contains("packs/10"), "{msg}");
        assert!(msg.ends_with("; and 2 more"), "{msg}");
    }

    // ── Mock-server tests, through BatchDelete::delete_keys ──

    fn http_response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn clean() -> String {
        http_response("200 OK", &delete_result(&[]))
    }

    fn keys(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("k{i:04}")).collect()
    }

    fn batch_delete(backend: &S3Backend, keys: &[String]) -> Result<()> {
        BatchDelete::delete_keys(backend, keys, BatchDeleteScope::KeysOnly)
    }

    fn object_count(body: &[u8]) -> usize {
        String::from_utf8_lossy(body).matches("<Object>").count()
    }

    /// Serve the mock's remaining scripted responses so its thread ends; called
    /// after the assertions on how many requests the backend made.
    fn drain_mock(port: u16, remaining: usize, handle: std::thread::JoinHandle<()>) {
        for _ in 0..remaining {
            let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            stream
                .write_all(b"GET / HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
            let mut sink = Vec::new();
            std::io::Read::read_to_end(&mut stream, &mut sink).unwrap();
        }
        handle.join().unwrap();
    }

    #[test]
    fn batch_delete_partial_failure() {
        let body = delete_result(&[
            ("packs/aa/1", "AccessDenied"),
            ("packs/cc/3", "AccessDenied"),
        ]);
        let (port, captured, handle) =
            mock_server_capture_bodies(vec![http_response("200 OK", &body)]);
        let backend = s3_backend(port, no_retry(), false);
        let keys: Vec<String> = ["packs/aa/1", "packs/bb/2", "packs/cc/3"]
            .iter()
            .map(|k| (*k).to_string())
            .collect();
        let err = batch_delete(&backend, &keys).unwrap_err();
        handle.join().unwrap();
        match err {
            VykarError::Other(msg) => {
                assert!(msg.contains("2 of 3 key(s) not deleted"), "{msg}");
                assert!(msg.contains("packs/aa/1"), "{msg}");
                assert!(msg.contains("packs/cc/3"), "{msg}");
                assert!(!msg.contains("packs/bb/2"), "{msg}");
            }
            other => panic!("expected Other, got {other:?}"),
        }
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let request_line = &captured[0].0[0];
        assert!(
            request_line.starts_with("POST /test-bucket/?"),
            "{request_line}"
        );
        assert!(request_line.contains("delete"), "{request_line}");
    }

    #[test]
    fn batch_delete_splits_at_1000() {
        let (port, captured, handle) = mock_server_capture_bodies(vec![clean(), clean(), clean()]);
        let backend = s3_backend(port, no_retry(), false);
        batch_delete(&backend, &keys(2500)).unwrap();
        handle.join().unwrap();
        let captured = captured.lock().unwrap();
        let counts: Vec<usize> = captured.iter().map(|(_, b)| object_count(b)).collect();
        assert_eq!(counts, vec![1000, 1000, 500]);
        let firsts_lasts = [("k0000", "k0999"), ("k1000", "k1999"), ("k2000", "k2499")];
        for ((lines, body), (first, last)) in captured.iter().zip(firsts_lasts) {
            let text = String::from_utf8(body.clone()).unwrap();
            assert!(text.contains("<Quiet>true</Quiet>"), "{text}");
            let first_pos = text.find(&format!("<Key>{first}</Key>")).unwrap();
            let last_pos = text.find(&format!("<Key>{last}</Key>")).unwrap();
            assert_eq!(
                text[..first_pos].matches("<Key>").count(),
                0,
                "{first} is not first"
            );
            assert_eq!(
                text[last_pos..].matches("<Key>").count(),
                1,
                "{last} is not last"
            );

            let expected_md5 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(body));
            let md5_header = lines
                .iter()
                .find_map(|l| {
                    l.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("content-md5"))
                        .map(|(_, v)| v.trim().to_string())
                })
                .expect("content-md5 header");
            assert_eq!(md5_header, expected_md5);

            let request_line = &lines[0];
            let signed = request_line
                .split(['?', '&', ' '])
                .find_map(|p| p.strip_prefix("X-Amz-SignedHeaders="))
                .expect("X-Amz-SignedHeaders in query");
            assert!(signed.contains("content-md5"), "{signed}");
        }
    }

    #[test]
    fn batch_delete_retryable_key_error_resends_chunk() {
        let first = http_response("200 OK", &delete_result(&[("k0001", "InternalError")]));
        let (port, captured, handle) = mock_server_capture_bodies(vec![first, clean()]);
        let backend = s3_backend(port, fast_retry(), false);
        batch_delete(&backend, &keys(3)).unwrap();
        handle.join().unwrap();
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].1, captured[1].1);
    }

    fn deletes(captured: &[(Vec<String>, Vec<u8>)]) -> usize {
        captured
            .iter()
            .filter(|(head, _)| head.first().is_some_and(|l| l.starts_with("DELETE ")))
            .count()
    }

    fn posts(captured: &[(Vec<String>, Vec<u8>)]) -> usize {
        captured
            .iter()
            .filter(|(head, _)| head.first().is_some_and(|l| l.starts_with("POST ")))
            .count()
    }

    #[test]
    fn batch_delete_not_implemented_falls_back_to_per_key_deletes_and_is_sticky() {
        let not_impl = http_response("501 Not Implemented", "");
        let gone = || http_response("204 No Content", "");
        let mut script = vec![not_impl];
        script.extend((0..6).map(|_| gone()));
        let (port, captured, handle) = mock_server_capture_bodies(script);
        let backend = s3_backend(port, fast_retry(), false);
        assert!(backend.batch_delete().is_some());
        batch_delete(&backend, &keys(3)).unwrap();
        {
            let captured = captured.lock().unwrap();
            assert_eq!(posts(&captured), 1, "501 must not be retried");
            assert_eq!(deletes(&captured), 3);
        }
        assert!(
            backend.batch_delete().is_none(),
            "the refusal is remembered"
        );
        // A deleter already in hand deletes key by key without a POST.
        batch_delete(&backend, &keys(3)).unwrap();
        handle.join().unwrap();
        let captured = captured.lock().unwrap();
        assert_eq!(posts(&captured), 1, "flag must skip the request");
        assert_eq!(deletes(&captured), 6);
    }

    #[test]
    fn batch_delete_not_implemented_on_second_chunk_deletes_the_rest_per_key() {
        let not_impl = http_response("501 Not Implemented", "");
        let mut script = vec![clean(), not_impl];
        script.extend((0..500).map(|_| http_response("204 No Content", "")));
        let (port, captured, handle) = mock_server_capture_bodies(script);
        let backend = s3_backend(port, no_retry(), false);
        batch_delete(&backend, &keys(1500)).unwrap();
        handle.join().unwrap();
        let captured = captured.lock().unwrap();
        assert_eq!(posts(&captured), 2);
        assert_eq!(deletes(&captured), 500);
        assert!(backend.batch_delete().is_none());
    }

    #[test]
    fn batch_delete_stops_at_first_failed_chunk() {
        let denied = http_response("403 Forbidden", &s3_error_xml("AccessDenied", "no"));
        let (port, captured, handle) = mock_server_capture_bodies(vec![denied, clean()]);
        let backend = s3_backend(port, no_retry(), false);
        batch_delete(&backend, &keys(1500)).unwrap_err();
        assert_eq!(captured.lock().unwrap().len(), 1);
        drain_mock(port, 1, handle);
    }

    /// A listener nobody accepts on: after the call, a pending connection would
    /// show up as an accepted stream.
    fn idle_listener() -> (u16, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        (listener.local_addr().unwrap().port(), listener)
    }

    #[test]
    fn soft_delete_offers_no_batch_delete() {
        let (port, listener) = idle_listener();
        let backend = s3_backend(port, no_retry(), true);
        assert!(backend.batch_delete().is_none());
        assert!(listener.accept().is_err(), "no connection expected");
    }

    #[test]
    fn batch_delete_empty_is_noop() {
        let (port, listener) = idle_listener();
        let backend = s3_backend(port, no_retry(), false);
        for scope in [
            BatchDeleteScope::KeysOnly,
            BatchDeleteScope::WholeRepository,
        ] {
            backend
                .batch_delete()
                .expect("batch delete offered")
                .delete_keys(&[], scope)
                .unwrap();
        }
        assert!(listener.accept().is_err(), "no connection expected");
    }

    #[test]
    fn batch_delete_rooted_prefixes_keys() {
        let body = delete_result(&[("backups/vykar/packs/ab/x", "AccessDenied")]);
        let (port, captured, handle) =
            mock_server_capture_bodies(vec![http_response("200 OK", &body)]);
        let backend = s3_backend_rooted(port, no_retry(), false);
        let err = batch_delete(&backend, &["packs/ab/x".to_string()]).unwrap_err();
        handle.join().unwrap();
        let sent = String::from_utf8(captured.lock().unwrap()[0].1.clone()).unwrap();
        assert!(
            sent.contains("<Key>backups/vykar/packs/ab/x</Key>"),
            "{sent}"
        );
        let msg = err.to_string();
        assert!(msg.contains("packs/ab/x: AccessDenied"), "{msg}");
        assert!(!msg.contains("backups/vykar"), "{msg}");
    }

    #[test]
    fn batch_delete_dedups() {
        let (port, captured, handle) = mock_server_capture_bodies(vec![clean()]);
        let backend = s3_backend(port, no_retry(), false);
        let keys: Vec<String> = ["a", "a", "b"].iter().map(|k| (*k).to_string()).collect();
        batch_delete(&backend, &keys).unwrap();
        handle.join().unwrap();
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(object_count(&captured[0].1), 2);
    }

    #[test]
    fn batch_delete_403_is_permanent() {
        let denied = http_response("403 Forbidden", &s3_error_xml("AccessDenied", "no"));
        let (port, captured, handle) = mock_server_capture_bodies(vec![denied, clean()]);
        let backend = s3_backend(port, fast_retry(), false);
        let err = batch_delete(&backend, &keys(3)).unwrap_err();
        assert_eq!(captured.lock().unwrap().len(), 1);
        assert!(err.to_string().contains("AccessDenied"), "{err}");
        drain_mock(port, 1, handle);
    }
}
