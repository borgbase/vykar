//! S3 probe: `If-None-Match: *` as a signed header on PutObject and on
//! CompleteMultipartUpload. Conditional requests are sent once, without the
//! retry layer, so the first answer to each is what gets recorded: a retried
//! conditional request could report 412 against its own first attempt.

use std::io::Read;
use std::sync::Barrier;

use base64::Engine;
use md5::{Digest, Md5};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use rusty_s3::S3Action;
use rusty_s3::actions::CreateMultipartUpload;
use serde::Deserialize;
use vykar_types::error::{Result, VykarError};

use super::conditional::{OCTET_STREAM, PreparedPut, prepare_put};
use super::multipart::parse_s3_error_code;
use super::{PRESIGN_DURATION, S3Backend};
use crate::probe::{
    CREATE, CaseResult, Cleanup, LIST_AFTER_WRITE, Ledger, MPU_COMPLETE_RETRY, MPU_ON_EXISTING,
    OVERWRITE_CONTROL, Outcome, ProbeOptions, REPEAT_OTHER, REPEAT_SAME, RaceResult, Report,
    TOMBSTONE, is_created, is_exists, is_listed, listing_outcome, payload,
};
use crate::{ParsedUrl, RetryConfig, StorageBackend, StorageConfig};

const BODY_LEN: usize = 1024;
const MECHANISM: &str = "If-None-Match: * (signed)";

/// Probe `If-None-Match: *` on the S3 target in `cfg.url`. Credentials come from `cfg`.
/// Errors only before the first write.
pub fn run_s3(cfg: &StorageConfig, opts: &ProbeOptions) -> Result<Report> {
    let ParsedUrl::S3 {
        bucket,
        root,
        endpoint,
    } = crate::parse_repo_url(&cfg.url)?
    else {
        return Err(VykarError::Config(
            "probe s3: URL must be s3://endpoint/bucket[/root] or s3+http://...".into(),
        ));
    };
    crate::enforce_secure_http(&endpoint, cfg.allow_insecure_http, "url")?;
    let region = cfg.region.as_deref().unwrap_or("us-east-1");
    let key_id = cfg
        .access_key_id
        .as_deref()
        .ok_or_else(|| VykarError::Config("probe s3: no access key ID".into()))?;
    let secret = cfg
        .secret_access_key
        .as_deref()
        .ok_or_else(|| VykarError::Config("probe s3: no secret access key".into()))?;
    let scrub = Scrub { key_id, secret };
    let build = || {
        S3Backend::new(
            &bucket,
            region,
            &root,
            &endpoint,
            key_id,
            secret,
            RetryConfig::default(),
            // Soft delete hides zero-byte objects from `get` and `list`; the
            // tombstone case and the leftover listing must see them.
            false,
            cfg.max_connections,
        )
        .map_err(|e| VykarError::Config(scrub.text(&e.to_string())))
    };
    let backend = build()?;
    // The list-after-write case lists through its own agent and connections.
    let lister = build()?;
    let transport = if endpoint.to_ascii_lowercase().starts_with("http://") {
        "http"
    } else {
        "https"
    };
    let mut location = format!("{endpoint}/{bucket}");
    let root = root.trim_matches('/');
    if !root.is_empty() {
        location.push('/');
        location.push_str(root);
    }
    run_with_backend(&backend, &lister, &location, transport, opts)
}

/// Same, on an already built backend (tests use this with a mock endpoint).
/// `lister` is a second backend on the same bucket and root, for the
/// list-after-write case.
pub(crate) fn run_with_backend(
    b: &S3Backend,
    lister: &S3Backend,
    location: &str,
    transport: &str,
    opts: &ProbeOptions,
) -> Result<Report> {
    let scrub = Scrub {
        key_id: b.credentials.key(),
        secret: b.credentials.secret(),
    };
    let preflight = |e: VykarError| VykarError::Other(scrub.text(&e.to_string()));

    let mut ledger = Ledger::for_run(opts)?;
    let prefix = ledger.prefix().to_string();
    // Every name is allocated before the first write, so `?` stops only the pre-flight.
    let keys = Keys {
        k: ledger.key("k")?,
        mpu: ledger.key("mpu")?,
        tomb: ledger.key("tomb")?,
        race: (0..opts.rounds())
            .map(|r| ledger.key(&format!("race-{r}")))
            .collect::<Result<_>>()?,
        list: (0..opts.list_rounds())
            .map(|r| ledger.key(&format!("list-{r}")))
            .collect::<Result<_>>()?,
    };

    let existing = b.list(&format!("{prefix}/")).map_err(preflight)?;
    if !existing.is_empty() {
        return Err(VykarError::Other(format!(
            "probe prefix {prefix} already holds {} objects",
            existing.len()
        )));
    }
    if !opts.allow_repo_root && b.exists("config").map_err(preflight)? {
        return Err(VykarError::Config(
            "probe root holds a 'config' object (a repository?); pass --allow-repo-root to probe it"
                .into(),
        ));
    }

    let mut run = Run {
        b,
        lister,
        scrub,
        ledger,
        notes: Vec::new(),
        cleanup_notes: Vec::new(),
    };
    let cases = run.cases(&keys);
    let race = run.race(&keys.race, opts.writers());
    let cleanup = run.cleanup();
    Ok(Report::finish(
        opts,
        "s3",
        transport.to_string(),
        MECHANISM,
        location.to_string(),
        &run.ledger,
        cases,
        Some(race),
        run.notes,
        cleanup,
    ))
}

struct Keys {
    k: String,
    mpu: String,
    tomb: String,
    race: Vec<String>,
    list: Vec<String>,
}

/// The set rusty-s3 percent-encodes in query values such as `X-Amz-Credential`.
/// For ASCII it equals the SigV4 rule: everything but `A-Za-z0-9-_.~`.
const QUERY_VALUE: &AsciiSet = &CONTROLS
    .add(b':')
    .add(b'?')
    .add(b'#')
    .add(b'[')
    .add(b']')
    .add(b'@')
    .add(b'!')
    .add(b'$')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b';')
    .add(b'=')
    .add(b'"')
    .add(b' ')
    .add(b'<')
    .add(b'>')
    .add(b'%')
    .add(b'{')
    .add(b'}')
    .add(b'|')
    .add(b'\\')
    .add(b'^')
    .add(b'`')
    .add(b'/');

/// Removes the configured credentials, and the XML bodies that may echo them,
/// from every string the probe keeps.
struct Scrub<'a> {
    key_id: &'a str,
    secret: &'a str,
}

impl Scrub<'_> {
    fn text(&self, s: &str) -> String {
        let mut out = reduce_xml(s);
        for secret in [self.key_id, self.secret] {
            if secret.is_empty() {
                continue;
            }
            let encoded = utf8_percent_encode(secret, QUERY_VALUE).to_string();
            if encoded != secret {
                out = out.replace(&encoded, "<redacted>");
            }
            out = out.replace(secret, "<redacted>");
        }
        out
    }

    fn outcome(&self, o: Outcome) -> Outcome {
        match o {
            Outcome::AlreadyExists { detail } => Outcome::AlreadyExists {
                detail: self.text(&detail),
            },
            Outcome::Conflict { detail } => Outcome::Conflict {
                detail: self.text(&detail),
            },
            Outcome::Rejected { detail } => Outcome::Rejected {
                detail: self.text(&detail),
            },
            Outcome::Transport { detail } => Outcome::Transport {
                detail: self.text(&detail),
            },
            Outcome::Info { detail } => Outcome::Info {
                detail: self.text(&detail),
            },
            other => other,
        }
    }
}

/// Cut an embedded XML error body down to its `<Code>`.
fn reduce_xml(s: &str) -> String {
    let Some(start) = s.find('<') else {
        return s.to_string();
    };
    let head = s.get(..start).unwrap_or_default().trim_end();
    let code = s
        .find("<Code>")
        .and_then(|i| s.get(i + "<Code>".len()..))
        .and_then(|rest| rest.find("</Code>").and_then(|j| rest.get(..j)));
    match code {
        Some(code) => format!("{head} {code}"),
        None => format!("{head} <xml body omitted>"),
    }
}

/// Minimal view of a `CompleteMultipartUploadResult`.
#[derive(Deserialize)]
struct CompleteResultDoc {
    #[serde(rename = "ETag")]
    etag: Option<String>,
}

fn status_detail(status: u16, body: &str) -> (Option<String>, String) {
    let code = parse_s3_error_code(body);
    let detail = match &code {
        Some(c) => format!("{status} {c}"),
        None => status.to_string(),
    };
    (code, detail)
}

/// A failed step before the completion. Never "exists", whatever the status:
/// only the conditional completion can answer that.
fn stage_failure(stage: &str, status: u16, body: &str) -> Outcome {
    let (_, detail) = status_detail(status, body);
    Outcome::Rejected {
        detail: format!("{stage}: {detail}"),
    }
}

/// Map an HTTP status and body to an Outcome.
fn classify_put(status: u16, body: &str) -> Outcome {
    if (200..300).contains(&status) {
        return Outcome::Created;
    }
    let (_, detail) = status_detail(status, body);
    match status {
        412 => Outcome::AlreadyExists { detail },
        409 => Outcome::Conflict { detail },
        _ => Outcome::Rejected { detail },
    }
}

/// Like [`classify_put`], but a 2xx may carry an `<Error>` document, and a
/// success must carry the merged object's `<ETag>`.
fn classify_complete(status: u16, body: &str) -> Outcome {
    if !(200..300).contains(&status) {
        return classify_put(status, body);
    }
    if let (Some(code), detail) = status_detail(status, body) {
        return match code.as_str() {
            "PreconditionFailed" => Outcome::AlreadyExists { detail },
            "ConditionalRequestConflict" => Outcome::Conflict { detail },
            "NoSuchUpload" => Outcome::Rejected {
                detail: "NoSuchUpload".into(),
            },
            _ => Outcome::Rejected { detail },
        };
    }
    if quick_xml::de::from_str::<CompleteResultDoc>(body).is_ok_and(|d| d.etag.is_some()) {
        Outcome::Created
    } else {
        Outcome::Rejected {
            detail: "malformed completion body".into(),
        }
    }
}

fn read_body(resp: &mut http::Response<ureq::Body>) -> String {
    let mut body = String::new();
    let _ = resp.body_mut().as_reader().read_to_string(&mut body);
    body
}

fn send_put(b: &S3Backend, p: &PreparedPut, body: &[u8]) -> Outcome {
    let mut req = b
        .agent
        .put(&p.url)
        .header("content-type", OCTET_STREAM)
        .header("content-md5", &p.content_md5);
    if p.if_none_match {
        req = req.header("if-none-match", "*");
    }
    match req.send(body) {
        Ok(mut resp) => {
            let status = resp.status().as_u16();
            classify_put(status, &read_body(&mut resp))
        }
        Err(e) => Outcome::Transport {
            detail: e.to_string(),
        },
    }
}

/// One signed PutObject. `if_none_match` adds "if-none-match: *" to the signed
/// headers and to the request. No retry.
fn put_raw(b: &S3Backend, full_key: &str, body: &[u8], if_none_match: bool) -> Outcome {
    send_put(b, &prepare_put(b, full_key, body, if_none_match), body)
}

/// CreateMultipartUpload; records the upload id in the ledger before returning it.
fn create_mpu_raw(
    b: &S3Backend,
    ledger: &mut Ledger,
    full_key: &str,
) -> std::result::Result<String, Outcome> {
    let url = b
        .bucket
        .create_multipart_upload(Some(&b.credentials), full_key)
        .sign(PRESIGN_DURATION);
    match b.agent.post(url.as_str()).send(&[] as &[u8]) {
        Ok(mut resp) => {
            let status = resp.status().as_u16();
            let body = read_body(&mut resp);
            if !(200..300).contains(&status) {
                return Err(stage_failure("create upload", status, &body));
            }
            match CreateMultipartUpload::parse_response(&body) {
                Ok(parsed) => {
                    let id = parsed.upload_id().to_owned();
                    ledger.record_upload(full_key.to_string(), id.clone());
                    Ok(id)
                }
                Err(_) => Err(Outcome::Rejected {
                    detail: format!("create upload: {status} with an unreadable upload id"),
                }),
            }
        }
        Err(e) => Err(Outcome::Transport {
            detail: format!("create upload: {e}"),
        }),
    }
}

/// One UploadPart with part number 1; returns the ETag.
fn upload_part_raw(
    b: &S3Backend,
    full_key: &str,
    upload_id: &str,
    body: &[u8],
) -> std::result::Result<String, Outcome> {
    let content_md5 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(body));
    let mut action = b
        .bucket
        .upload_part(Some(&b.credentials), full_key, 1, upload_id);
    action.headers_mut().insert("content-md5", &content_md5);
    let url = action.sign(PRESIGN_DURATION);
    match b
        .agent
        .put(url.as_str())
        .header("content-md5", &content_md5)
        .send(body)
    {
        Ok(mut resp) => {
            let status = resp.status().as_u16();
            if !(200..300).contains(&status) {
                return Err(stage_failure("upload part", status, &read_body(&mut resp)));
            }
            resp.headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| Outcome::Rejected {
                    detail: "upload part: response without ETag".into(),
                })
        }
        Err(e) => Err(Outcome::Transport {
            detail: format!("upload part: {e}"),
        }),
    }
}

/// CompleteMultipartUpload of one part, with "if-none-match: *" signed and sent.
fn complete_mpu_raw(b: &S3Backend, full_key: &str, upload_id: &str, etag: &str) -> Outcome {
    let mut action = b.bucket.complete_multipart_upload(
        Some(&b.credentials),
        full_key,
        upload_id,
        std::iter::once(etag),
    );
    action
        .headers_mut()
        .insert("content-type", "application/xml");
    action.headers_mut().insert("if-none-match", "*");
    let url = action.sign(PRESIGN_DURATION);
    let body = action.body();
    match b
        .agent
        .post(url.as_str())
        .header("content-type", "application/xml")
        .header("if-none-match", "*")
        .send(body.as_bytes())
    {
        Ok(mut resp) => {
            let status = resp.status().as_u16();
            classify_complete(status, &read_body(&mut resp))
        }
        Err(e) => Outcome::Transport {
            detail: e.to_string(),
        },
    }
}

/// AbortMultipartUpload, best effort; 404/NoSuchUpload count as done.
fn abort_mpu_raw(
    b: &S3Backend,
    full_key: &str,
    upload_id: &str,
) -> std::result::Result<(), String> {
    let url = b
        .bucket
        .abort_multipart_upload(Some(&b.credentials), full_key, upload_id)
        .sign(PRESIGN_DURATION);
    match b.agent.delete(url.as_str()).call() {
        Ok(mut resp) => {
            let status = resp.status().as_u16();
            if status < 300 || status == 404 {
                return Ok(());
            }
            let (code, detail) = status_detail(status, &read_body(&mut resp));
            if code.as_deref() == Some("NoSuchUpload") {
                return Ok(());
            }
            Err(format!("abort upload on {full_key}: {detail}"))
        }
        Err(e) => Err(format!("abort upload on {full_key}: {e}")),
    }
}

struct Run<'a> {
    b: &'a S3Backend,
    lister: &'a S3Backend,
    scrub: Scrub<'a>,
    ledger: Ledger,
    notes: Vec<String>,
    cleanup_notes: Vec<String>,
}

impl Run<'_> {
    fn put(&self, key: &str, body: &[u8], if_none_match: bool) -> Outcome {
        self.scrub
            .outcome(put_raw(self.b, &self.b.full_key(key), body, if_none_match))
    }

    /// `Some(stored == want)`; `None` with a note if the object cannot be read.
    fn content(&mut self, key: &str, want: &[u8]) -> Option<bool> {
        match self.b.get(key) {
            Ok(Some(got)) => Some(got == want),
            Ok(None) => {
                self.notes.push(format!("content check: {key} is absent"));
                None
            }
            Err(e) => {
                self.notes.push(format!(
                    "content check: {}",
                    self.scrub.text(&e.to_string())
                ));
                None
            }
        }
    }

    /// Create, upload one part, complete. Returns the outcome and, if a
    /// completion was sent, the upload id and part ETag for a retry.
    fn mpu(&mut self, key: &str, body: &[u8]) -> (Outcome, Option<(String, String)>) {
        let full_key = self.b.full_key(key);
        let id = match create_mpu_raw(self.b, &mut self.ledger, &full_key) {
            Ok(id) => id,
            Err(o) => {
                if matches!(o, Outcome::Transport { .. })
                    || matches!(&o, Outcome::Rejected { detail } if detail.contains("upload id"))
                {
                    self.cleanup_notes.push(format!(
                        "CreateMultipartUpload on {full_key} got no usable reply; an upload \
                         with no recorded id may exist under the probe's key"
                    ));
                }
                return (self.scrub.outcome(o), None);
            }
        };
        let etag = match upload_part_raw(self.b, &full_key, &id, body) {
            Ok(etag) => etag,
            Err(o) => return (self.scrub.outcome(o), None),
        };
        let outcome = complete_mpu_raw(self.b, &full_key, &id, &etag);
        (self.scrub.outcome(outcome), Some((id, etag)))
    }

    fn cases(&mut self, keys: &Keys) -> Vec<CaseResult> {
        let prefix = self.ledger.prefix().to_string();
        let a = payload("A", &prefix, BODY_LEN);
        let bb = payload("B", &prefix, BODY_LEN);
        let c = payload("C", &prefix, BODY_LEN);
        let m = payload("M", &prefix, BODY_LEN);
        let m2 = payload("M2", &prefix, BODY_LEN);
        let mut cases = Vec::new();

        let created = self.put(&keys.k, &a, true);
        let content = if is_created(&created) {
            self.content(&keys.k, &a)
        } else {
            None
        };
        let create = CaseResult::expect(CREATE, "created", is_created, created, content, true);
        let create_ok = create.ok;
        cases.push(create);

        let mut control_ok = false;
        // Repeats are judged against the bytes of create, so they need it fully ok.
        if create_ok {
            for (name, data) in [(REPEAT_SAME, &a), (REPEAT_OTHER, &bb)] {
                let o = self.put(&keys.k, data, true);
                let content = self.content(&keys.k, &a);
                cases.push(CaseResult::expect(
                    name,
                    "412, bytes of create kept",
                    is_exists,
                    o,
                    content,
                    true,
                ));
            }
            let o = self.put(&keys.k, &c, false);
            let content = self.content(&keys.k, &c);
            let case = CaseResult::expect(
                OVERWRITE_CONTROL,
                "unconditional PUT overwrites",
                is_created,
                o,
                content,
                true,
            );
            control_ok = case.ok;
            cases.push(case);
        } else {
            for name in [REPEAT_SAME, REPEAT_OTHER, OVERWRITE_CONTROL] {
                cases.push(CaseResult::skipped(
                    name,
                    "after a successful create",
                    "create was not ok",
                ));
            }
        }

        let (o, retry) = self.mpu(&keys.mpu, &m);
        let content = if is_created(&o) {
            self.content(&keys.mpu, &m)
        } else {
            None
        };
        cases.push(CaseResult::expect(
            "mpu_absent",
            "created",
            is_created,
            o,
            content,
            true,
        ));

        match retry {
            Some((id, etag)) => {
                let full_key = self.b.full_key(&keys.mpu);
                let o = complete_mpu_raw(self.b, &full_key, &id, &etag);
                cases.push(CaseResult::info(
                    MPU_COMPLETE_RETRY,
                    self.scrub.outcome(o),
                    None,
                ));
            }
            None => cases.push(CaseResult::info(
                MPU_COMPLETE_RETRY,
                Outcome::Info {
                    detail: "skipped: no completion was sent".into(),
                },
                None,
            )),
        }

        if control_ok {
            let (o, sent) = self.mpu(&keys.k, &m2);
            let content = self.content(&keys.k, &c);
            cases.push(CaseResult::expect(
                MPU_ON_EXISTING,
                "412, bytes of overwrite_control kept",
                is_exists,
                o,
                content,
                true,
            ));
            // A refused completion leaves the upload open.
            if let Some((id, _)) = sent
                && let Err(e) = abort_mpu_raw(self.b, &self.b.full_key(&keys.k), &id)
            {
                self.cleanup_notes.push(self.scrub.text(&e));
            }
        } else {
            cases.push(CaseResult::skipped(
                MPU_ON_EXISTING,
                "412, bytes of overwrite_control kept",
                "overwrite_control did not succeed",
            ));
        }

        let setup = self.put(&keys.tomb, &[], false);
        if is_created(&setup) {
            let o = self.put(&keys.tomb, &a, true);
            let content = self.content(&keys.tomb, &[]);
            cases.push(CaseResult::expect(
                TOMBSTONE,
                "412 on a zero-byte object, still zero bytes",
                is_exists,
                o,
                content,
                true,
            ));
        } else {
            cases.push(CaseResult::skipped(
                TOMBSTONE,
                "412 on a zero-byte object, still zero bytes",
                &format!("writing the zero-byte object: {}", outcome_text(&setup)),
            ));
        }
        cases.push(self.list_after_write(&keys.list));
        cases
    }

    /// Plain PUTs through `b`, each followed by a LIST of the prefix through
    /// `lister`, which must show the key.
    fn list_after_write(&mut self, keys: &[String]) -> CaseResult {
        let result = |outcome| {
            CaseResult::expect(LIST_AFTER_WRITE, "listed", is_listed, outcome, None, false)
        };
        let dir = format!("{}/", self.ledger.prefix());
        let mut missed = 0;
        for key in keys {
            let o = self.put(key, b"l", false);
            if !is_created(&o) {
                return result(o);
            }
            match self.lister.list(&dir) {
                Ok(listed) => {
                    if !listed.contains(key) {
                        missed += 1;
                    }
                }
                Err(e) => {
                    return result(Outcome::Transport {
                        detail: self.scrub.text(&format!("list: {e}")),
                    });
                }
            }
        }
        result(listing_outcome(missed, keys.len()))
    }

    fn race(&mut self, keys: &[String], writers: usize) -> RaceResult {
        let prefix = self.ledger.prefix().to_string();
        let mut result = RaceResult::new(writers);
        for (r, key) in keys.iter().enumerate() {
            let full_key = self.b.full_key(key);
            let bodies: Vec<Vec<u8>> = (0..writers)
                .map(|i| payload(&format!("r{r}w{i}"), &prefix, BODY_LEN))
                .collect();
            let prepared: Vec<PreparedPut> = bodies
                .iter()
                .map(|body| prepare_put(self.b, &full_key, body, true))
                .collect();
            let barrier = Barrier::new(writers);
            let b = self.b;
            let outcomes: Vec<Outcome> = std::thread::scope(|s| {
                let handles: Vec<_> = prepared
                    .iter()
                    .zip(&bodies)
                    .map(|(p, body)| {
                        let barrier = &barrier;
                        s.spawn(move || {
                            barrier.wait();
                            send_put(b, p, body)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join().unwrap_or_else(|_| Outcome::Transport {
                            detail: "writer thread panicked".into(),
                        })
                    })
                    .collect()
            });
            let outcomes: Vec<Outcome> = outcomes
                .into_iter()
                .map(|o| self.scrub.outcome(o))
                .collect();
            if let Some(w) = result.add_round(&outcomes)
                && let Some(want) = bodies.get(w)
                && self.content(key, want) == Some(true)
            {
                result.content_matches_winner += 1;
            }
        }
        result
    }

    /// Abort every recorded upload, delete every recorded key, then list what
    /// is left under the prefix without deleting it.
    fn cleanup(&mut self) -> Cleanup {
        let mut out = Cleanup::default();
        for (full_key, id) in self.ledger.uploads() {
            if let Err(e) = abort_mpu_raw(self.b, full_key, id) {
                out.errors.push(self.scrub.text(&e));
            }
        }
        for key in self.ledger.keys() {
            match self.b.delete(key) {
                Ok(()) => out.removed += 1,
                Err(e) => out.errors.push(self.scrub.text(&e.to_string())),
            }
        }
        match self.b.list(&format!("{}/", self.ledger.prefix())) {
            Ok(mut left) => {
                left.sort();
                out.leftovers = left;
            }
            Err(e) => out.errors.push(self.scrub.text(&e.to_string())),
        }
        out.errors.append(&mut self.cleanup_notes);
        out
    }
}

fn outcome_text(o: &Outcome) -> String {
    match o {
        Outcome::Created => "created".into(),
        Outcome::NotAdvertised => "not advertised".into(),
        Outcome::AlreadyExists { detail }
        | Outcome::Conflict { detail }
        | Outcome::Rejected { detail }
        | Outcome::Transport { detail }
        | Outcome::Info { detail }
        | Outcome::Listed { detail }
        | Outcome::NotListed { detail } => detail.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::{Verdict, decide};
    use crate::s3_backend::test_support::{no_retry, reply, s3_backend, s3_error_xml, serve};
    use crate::s3_backend::tests::mock_server_capture;

    fn rejected_detail(o: &Outcome) -> &str {
        match o {
            Outcome::Rejected { detail } => detail,
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn classify_put_statuses() {
        assert_eq!(classify_put(200, ""), Outcome::Created);
        assert!(matches!(
            classify_put(412, &s3_error_xml("PreconditionFailed", "m")),
            Outcome::AlreadyExists { detail } if detail == "412 PreconditionFailed"
        ));
        assert!(matches!(
            classify_put(409, &s3_error_xml("ConditionalRequestConflict", "m")),
            Outcome::Conflict { .. }
        ));
        assert_eq!(
            rejected_detail(&classify_put(501, &s3_error_xml("NotImplemented", "m"))),
            "501 NotImplemented"
        );
        assert_eq!(
            rejected_detail(&classify_put(400, &s3_error_xml("InvalidArgument", "m"))),
            "400 InvalidArgument"
        );
        assert_eq!(
            rejected_detail(&classify_put(403, &s3_error_xml("AccessDenied", "m"))),
            "403 AccessDenied"
        );
    }

    #[test]
    fn classify_complete_error_in_200() {
        assert!(matches!(
            classify_complete(200, &s3_error_xml("PreconditionFailed", "m")),
            Outcome::AlreadyExists { .. }
        ));
        assert_eq!(
            rejected_detail(&classify_complete(200, &s3_error_xml("NoSuchUpload", "m"))),
            "NoSuchUpload"
        );
        assert_eq!(
            rejected_detail(&classify_complete(
                200,
                "<CompleteMultipartUploadResult><Key>k</Key></CompleteMultipartUploadResult>"
            )),
            "malformed completion body"
        );
        assert_eq!(
            classify_complete(
                200,
                "<CompleteMultipartUploadResult><ETag>\"e\"</ETag></CompleteMultipartUploadResult>"
            ),
            Outcome::Created
        );
        assert!(matches!(
            classify_complete(412, &s3_error_xml("PreconditionFailed", "m")),
            Outcome::AlreadyExists { .. }
        ));
    }

    #[test]
    fn unsupported_signature() {
        let case = |o: Outcome| {
            vec![CaseResult::expect(
                CREATE, "created", is_created, o, None, true,
            )]
        };
        let not_impl = classify_put(501, &s3_error_xml("NotImplemented", "m"));
        assert_eq!(decide(&case(not_impl), None).0, Verdict::Unsupported);
        let denied = classify_put(403, &s3_error_xml("AccessDenied", "m"));
        assert_eq!(decide(&case(denied), None).0, Verdict::Inconclusive);
    }

    fn signature_error_body(key_id: &str) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SignatureDoesNotMatch</Code>\
             <Message>The request signature we calculated does not match</Message>\
             <AWSAccessKeyId>{key_id}</AWSAccessKeyId><StringToSign>x</StringToSign></Error>"
        )
    }

    #[test]
    fn scrub_reduces_xml_and_removes_keys() {
        let scrub = Scrub {
            key_id: "AKIDUMMYPROBE",
            secret: "SECRETDUMMY",
        };
        let msg = format!(
            "S3 LIST p: HTTP 403: {} SECRETDUMMY",
            signature_error_body("AKIDUMMYPROBE")
        );
        let out = scrub.text(&msg);
        assert_eq!(out, "S3 LIST p: HTTP 403: SignatureDoesNotMatch");
        let url = scrub.text("transport error: http://h/b/k?X-Amz-Credential=AKIDUMMYPROBE%2F2026");
        assert!(!url.contains("AKIDUMMYPROBE"), "{url}");

        let scrub = Scrub {
            key_id: "AK+ID/DUMMY=",
            secret: "SECRETDUMMY",
        };
        let url =
            scrub.text("transport error: http://h/b/k?X-Amz-Credential=AK%2BID%2FDUMMY%3D%2F2026");
        assert_eq!(
            url,
            "transport error: http://h/b/k?X-Amz-Credential=<redacted>%2F2026"
        );
    }

    #[test]
    fn create_upload_412_is_not_exists() {
        let (port, _, handle) = mock_server_capture(&reply(
            "412 Precondition Failed",
            &s3_error_xml("PreconditionFailed", "m"),
        ));
        let backend = s3_backend(port, no_retry(), false);
        let mut ledger = Ledger::with_prefix("vykar-probe-t");
        let o = create_mpu_raw(&backend, &mut ledger, "k").unwrap_err();
        handle.join().unwrap();
        assert_eq!(rejected_detail(&o), "create upload: 412 PreconditionFailed");
        assert!(ledger.uploads().is_empty());
    }

    #[test]
    fn upload_part_412_is_not_exists() {
        let (port, _, handle) = mock_server_capture(&reply(
            "412 Precondition Failed",
            &s3_error_xml("PreconditionFailed", "m"),
        ));
        let backend = s3_backend(port, no_retry(), false);
        let o = upload_part_raw(&backend, "k", "upload-1", b"x").unwrap_err();
        handle.join().unwrap();
        assert_eq!(rejected_detail(&o), "upload part: 412 PreconditionFailed");
    }

    const EMPTY_LIST: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>test-bucket</Name><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>\
         <IsTruncated>false</IsTruncated></ListBucketResult>";

    /// An empty listing for LIST, 404 for HEAD and a `SignatureDoesNotMatch`
    /// naming `key_id` for everything else. `list_fails` makes LIST fail the same way.
    fn signature_mock(key_id: &str, list_fails: bool) -> u16 {
        let err = signature_error_body(key_id);
        serve(move |req| {
            let list = req.method == "GET" && req.line.contains("list-type=2");
            if list && !list_fails {
                ("200 OK", EMPTY_LIST.to_string())
            } else if req.method == "HEAD" {
                ("404 Not Found", String::new())
            } else {
                ("403 Forbidden", err.clone())
            }
        })
    }

    /// `create` gets 200 but the read-back differs: the repeats must not run,
    /// or their 200 would read as an ignored condition.
    #[test]
    fn repeats_skipped_when_create_bytes_differ() {
        let port = serve(|req| {
            let on_k = req.path.ends_with("/k");
            match req.method.as_str() {
                "GET" if req.line.contains("list-type=2") => ("200 OK", EMPTY_LIST.to_string()),
                "GET" if on_k => ("200 OK", "not the bytes of create".to_string()),
                "GET" | "HEAD" => ("404 Not Found", String::new()),
                // Every PUT on `k` succeeds, so a repeat would come back Created.
                "PUT" if on_k || !req.if_none_match => ("200 OK", String::new()),
                "PUT" => (
                    "412 Precondition Failed",
                    s3_error_xml("PreconditionFailed", "m"),
                ),
                "DELETE" => ("204 No Content", String::new()),
                _ => ("403 Forbidden", s3_error_xml("AccessDenied", "m")),
            }
        });
        let backend = s3_backend(port, no_retry(), false);
        let opts = ProbeOptions {
            race_writers: 2,
            race_rounds: 1,
            ..ProbeOptions::default()
        };
        let report =
            run_with_backend(&backend, &backend, "http://mock/test-bucket", "http", &opts).unwrap();
        let case = |name: &str| report.cases.iter().find(|c| c.name == name).unwrap();
        assert_eq!(case(CREATE).outcome, Outcome::Created);
        assert_eq!(case(CREATE).content_ok, Some(false));
        for name in [REPEAT_SAME, REPEAT_OTHER, OVERWRITE_CONTROL] {
            assert!(
                rejected_detail(&case(name).outcome).starts_with("skipped: "),
                "{:?}",
                case(name)
            );
        }
        assert_eq!(
            rejected_detail(&case("mpu_absent").outcome),
            "create upload: 403 AccessDenied"
        );
        assert_eq!(report.verdict, Verdict::Inconclusive, "{report:#?}");
    }

    #[test]
    fn signature_error_never_leaks_key_id() {
        // test_support backends use the key ID "AKID".
        let port = signature_mock("AKID", false);
        let backend = s3_backend(port, no_retry(), false);
        let opts = ProbeOptions {
            race_writers: 2,
            race_rounds: 1,
            ..ProbeOptions::default()
        };
        let report =
            run_with_backend(&backend, &backend, "http://mock/test-bucket", "http", &opts).unwrap();
        assert_eq!(report.verdict, Verdict::Inconclusive);
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("SignatureDoesNotMatch"), "{json}");
        assert!(!json.contains("AKID"), "{json}");
        assert!(!report.markdown_row().contains("AKID"));

        let port = signature_mock("AKID", true);
        let backend = s3_backend(port, no_retry(), false);
        let err = run_with_backend(&backend, &backend, "http://mock/test-bucket", "http", &opts)
            .unwrap_err()
            .to_string();
        assert!(err.contains("SignatureDoesNotMatch"), "{err}");
        assert!(!err.contains("AKID"), "{err}");
    }

    fn signed_headers(request_line: &str) -> String {
        let start = request_line
            .find("X-Amz-SignedHeaders=")
            .expect("presigned URL lists signed headers");
        let rest = &request_line[start + "X-Amz-SignedHeaders=".len()..];
        let end = rest.find(['&', ' ']).unwrap_or(rest.len());
        rest[..end].replace("%3B", ";").to_lowercase()
    }

    fn sends_if_none_match(lines: &[String]) -> bool {
        lines
            .iter()
            .skip(1)
            .any(|l| l.to_lowercase().replace(' ', "") == "if-none-match:*")
    }

    #[test]
    fn put_raw_signs_and_sends_if_none_match() {
        let ok = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (port, captured, handle) = mock_server_capture(ok);
        let backend = s3_backend(port, no_retry(), false);
        assert_eq!(put_raw(&backend, "k", b"x", true), Outcome::Created);
        handle.join().unwrap();
        let lines = captured.lock().unwrap().clone();
        assert!(
            signed_headers(&lines[0])
                .split(';')
                .any(|h| h == "if-none-match"),
            "{}",
            lines[0]
        );
        assert!(sends_if_none_match(&lines), "{lines:?}");

        let (port, captured, handle) = mock_server_capture(ok);
        let backend = s3_backend(port, no_retry(), false);
        assert_eq!(put_raw(&backend, "k", b"x", false), Outcome::Created);
        handle.join().unwrap();
        let lines = captured.lock().unwrap().clone();
        assert!(
            !signed_headers(&lines[0]).contains("if-none-match"),
            "{}",
            lines[0]
        );
        assert!(!sends_if_none_match(&lines), "{lines:?}");
    }

    #[test]
    fn complete_mpu_raw_signs_and_sends_if_none_match() {
        let body =
            "<CompleteMultipartUploadResult><ETag>\"e\"</ETag></CompleteMultipartUploadResult>";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let (port, captured, handle) = mock_server_capture(&resp);
        let backend = s3_backend(port, no_retry(), false);
        assert_eq!(
            complete_mpu_raw(&backend, "k", "upload-1", "\"etag\""),
            Outcome::Created
        );
        handle.join().unwrap();
        let lines = captured.lock().unwrap().clone();
        assert!(lines[0].starts_with("POST "), "{}", lines[0]);
        assert!(
            signed_headers(&lines[0])
                .split(';')
                .any(|h| h == "if-none-match"),
            "{}",
            lines[0]
        );
        assert!(sends_if_none_match(&lines), "{lines:?}");
    }
}
