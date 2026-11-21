//! Probe of put-if-absent on real storage targets.
//!
//! Each run creates one random prefix, tries the backend's create-if-absent
//! mechanism on keys under it, races concurrent creators, checks the stored
//! bytes and removes only what it recorded. Driven by the
//! `put_if_absent_probe` example; nothing in vykar calls it. Never prints.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};
use serde::Serialize;
use vykar_types::error::{Result, VykarError};

mod local;

pub use local::run_local;

pub use crate::s3_backend::probe::run_s3;
#[cfg(feature = "backend-sftp")]
pub use crate::sftp_backend::probe::{run_sftp, run_sftp_sessions};

/// Version of the probe's case list and report shape. Bump when a case changes.
pub const PROBE_VERSION: u32 = 1;
/// First component of every name the probe creates.
pub const PROBE_PREFIX: &str = "vykar-probe-";

/// Case names shared by the backends; [`decide`] matches on them.
pub(crate) const CREATE: &str = "create";
pub(crate) const REPEAT_SAME: &str = "repeat_same";
pub(crate) const REPEAT_OTHER: &str = "repeat_other";
pub(crate) const OVERWRITE_CONTROL: &str = "overwrite_control";
pub(crate) const MPU_ON_EXISTING: &str = "mpu_on_existing";
pub(crate) const MPU_COMPLETE_RETRY: &str = "mpu_complete_retry";
pub(crate) const TOMBSTONE: &str = "tombstone";

/// Cases whose outcome is recorded but never counted against the verdict.
const INFORMATIONAL: &[&str] = &[
    MPU_COMPLETE_RETRY,
    "advertised",
    "err_missing_source",
    "err_missing_dir",
    "plain_rename_over_existing",
    "fs_note",
];

/// Cases where a `Created` or changed bytes mean the condition was ignored.
const MUST_NOT_CREATE: &[&str] = &[REPEAT_SAME, REPEAT_OTHER, MPU_ON_EXISTING, TOMBSTONE];

/// Prefix of a `Rejected` detail that means the target refuses the mechanism
/// itself. Backends whose refusal is not an HTTP status add it.
pub(crate) const UNSUPPORTED_DETAIL: &str = "unsupported: ";

#[derive(Debug, Clone)]
pub struct ProbeOptions {
    /// Free text naming the target (service, version, host), copied into the report.
    pub label: String,
    /// Concurrent creators per race round (default 8, clamped to 2..=32).
    pub race_writers: usize,
    /// Race rounds, each on a fresh key (default 10, clamped to 1..=100).
    pub race_rounds: usize,
    /// Probe a root that already holds a `config` object (a repository). Default false.
    pub allow_repo_root: bool,
    /// SFTP only: the root path is relative to the login directory, resolved with
    /// `SSH_FXP_REALPATH` of "." (`SftpSession::canonicalize`). Default false.
    pub sftp_root_relative_to_home: bool,
    /// Name of the run's prefix, so a caller can record it before the first write.
    /// Must start with [`PROBE_PREFIX`] and contain no '/', '\\' or "..".
    /// `None` (the default) draws a fresh one with [`new_prefix`].
    pub prefix: Option<String>,
}

impl Default for ProbeOptions {
    fn default() -> Self {
        Self {
            label: String::new(),
            race_writers: 8,
            race_rounds: 10,
            allow_repo_root: false,
            sftp_root_relative_to_home: false,
            prefix: None,
        }
    }
}

impl ProbeOptions {
    /// `race_writers` clamped to 2..=32.
    pub fn writers(&self) -> usize {
        self.race_writers.clamp(2, 32)
    }

    /// `race_rounds` clamped to 1..=100.
    pub fn rounds(&self) -> usize {
        self.race_rounds.clamp(1, 100)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Outcome {
    /// This call created the object (200, link/rename succeeded).
    Created,
    /// Refused because the object exists: S3 412, local EEXIST, SFTP status with the
    /// target present afterwards. `detail` holds the raw status/code/errno.
    AlreadyExists { detail: String },
    /// S3 409 ConditionalRequestConflict.
    Conflict { detail: String },
    /// The server does not offer the mechanism (SFTP hardlink not advertised).
    NotAdvertised,
    /// Answered with an error that does not mean "exists".
    Rejected { detail: String },
    /// No answer: transport error or timeout.
    Transport { detail: String },
    /// Informational case; never counted against the verdict.
    Info { detail: String },
}

impl Outcome {
    pub(crate) fn short(&self) -> String {
        match self {
            Self::Created => "created".into(),
            Self::AlreadyExists { detail } => format!("exists ({detail})"),
            Self::Conflict { detail } => format!("conflict ({detail})"),
            Self::NotAdvertised => "not advertised".into(),
            Self::Rejected { detail } => format!("rejected ({detail})"),
            Self::Transport { detail } => format!("transport ({detail})"),
            Self::Info { detail } => detail.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CaseResult {
    pub name: &'static str,
    pub expected: &'static str,
    pub outcome: Outcome,
    /// Stored bytes compared after the case: `Some(true)` if they are what they must be.
    pub content_ok: Option<bool>,
    /// Whether the case met its expectation (always true for informational cases).
    pub ok: bool,
}

impl CaseResult {
    /// A case that expects `want` and, if `content_ok` is given, those bytes.
    pub(crate) fn expect(
        name: &'static str,
        expected: &'static str,
        want: fn(&Outcome) -> bool,
        outcome: Outcome,
        content_ok: Option<bool>,
        content_checked: bool,
    ) -> Self {
        let ok = want(&outcome) && (!content_checked || content_ok == Some(true));
        Self {
            name,
            expected,
            outcome,
            content_ok,
            ok,
        }
    }

    pub(crate) fn info(name: &'static str, outcome: Outcome, content_ok: Option<bool>) -> Self {
        Self {
            name,
            expected: "informational",
            outcome,
            content_ok,
            ok: true,
        }
    }

    /// A case not run because an earlier one left the state unknown.
    pub(crate) fn skipped(name: &'static str, expected: &'static str, why: &str) -> Self {
        Self {
            name,
            expected,
            outcome: Outcome::Rejected {
                detail: format!("skipped: {why}"),
            },
            content_ok: None,
            ok: false,
        }
    }
}

pub(crate) fn is_created(o: &Outcome) -> bool {
    matches!(o, Outcome::Created)
}

pub(crate) fn is_exists(o: &Outcome) -> bool {
    matches!(o, Outcome::AlreadyExists { .. })
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RaceResult {
    pub rounds: usize,
    pub writers: usize,
    pub rounds_one_winner: usize,
    pub rounds_no_winner: usize,
    pub max_winners: usize,
    pub already_exists: usize,
    pub conflicts: usize,
    pub errors: usize,
    /// Rounds where the stored bytes equal the single winner's bytes.
    pub content_matches_winner: usize,
    /// Distinct non-success details seen, for the report.
    pub details: Vec<String>,
}

impl RaceResult {
    pub(crate) fn new(writers: usize) -> Self {
        Self {
            writers,
            ..Self::default()
        }
    }

    /// Count one round's outcomes. Returns the index of the single winner, if
    /// exactly one writer created the object.
    pub(crate) fn add_round(&mut self, outcomes: &[Outcome]) -> Option<usize> {
        self.rounds += 1;
        let mut winners = Vec::new();
        for (i, o) in outcomes.iter().enumerate() {
            match o {
                Outcome::Created => winners.push(i),
                Outcome::AlreadyExists { .. } => self.already_exists += 1,
                Outcome::Conflict { detail } => {
                    self.conflicts += 1;
                    self.note(format!("conflict ({detail})"));
                }
                other => {
                    self.errors += 1;
                    self.note(other.short());
                }
            }
        }
        self.max_winners = self.max_winners.max(winners.len());
        match winners.as_slice() {
            [] => {
                self.rounds_no_winner += 1;
                None
            }
            [w] => {
                self.rounds_one_winner += 1;
                Some(*w)
            }
            _ => None,
        }
    }

    pub(crate) fn note(&mut self, detail: String) {
        if !self.details.contains(&detail) {
            self.details.push(detail);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Works,
    IgnoresCondition,
    Unsupported,
    Inconclusive,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Cleanup {
    pub removed: usize,
    /// Names still present under the prefix after cleanup. The probe never removes these.
    pub leftovers: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub probe_version: u32,
    pub label: String,
    /// "local" | "s3" | "sftp"
    pub backend: &'static str,
    /// `"fs" | "https" | "http" | "ssh" | "exec:<program>"`
    pub transport: String,
    pub mechanism: &'static str,
    /// Root URL or path, never credentials.
    pub location: String,
    pub prefix: String,
    pub started_unix: u64,
    pub cases: Vec<CaseResult>,
    pub race: Option<RaceResult>,
    pub verdict: Verdict,
    pub deviations: Vec<String>,
    pub cleanup: Cleanup,
}

const MARKDOWN_HEADER: &str = "| Target | Transport | Mechanism | create | repeat same | repeat other \
| race (rounds, one winner / rounds, max winners, 409s) \
| extra cases (MPU absent / MPU retry / MPU on existing / tombstone; or hardlink codes: \
missing source / missing dir / plain rename) | cleanup | verdict |\n\
|---|---|---|---|---|---|---|---|---|---|";

impl Report {
    /// Assemble a report and compute its verdict. `notes` are extra deviation
    /// lines from the run (content checks that failed to read, and the like).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finish(
        opts: &ProbeOptions,
        backend: &'static str,
        transport: String,
        mechanism: &'static str,
        location: String,
        ledger: &Ledger,
        cases: Vec<CaseResult>,
        race: Option<RaceResult>,
        notes: Vec<String>,
        cleanup: Cleanup,
    ) -> Self {
        let (verdict, mut deviations) = decide(&cases, race.as_ref());
        deviations.extend(notes);
        Self {
            probe_version: PROBE_VERSION,
            label: opts.label.clone(),
            backend,
            transport,
            mechanism,
            location,
            prefix: ledger.prefix().to_string(),
            started_unix: ledger.started_unix,
            cases,
            race,
            verdict,
            deviations,
            cleanup,
        }
    }

    /// Header and separator line of the Markdown result table.
    pub fn markdown_header() -> &'static str {
        MARKDOWN_HEADER
    }

    /// One row of that table. Cells: label, backend/transport, mechanism, create,
    /// repeat same, repeat other, race, backend-specific cases, cleanup, verdict.
    pub fn markdown_row(&self) -> String {
        let case = |name: &str| {
            self.cases
                .iter()
                .find(|c| c.name == name)
                .map(case_cell)
                .unwrap_or_else(|| "not run".into())
        };
        let race = match &self.race {
            Some(r) => format!(
                "{}/{} one winner; max winners {}; 409s {}; errors {}",
                r.rounds_one_winner, r.rounds, r.max_winners, r.conflicts, r.errors
            ),
            None => "not run".into(),
        };
        let extra = self
            .cases
            .iter()
            .filter(|c| ![CREATE, REPEAT_SAME, REPEAT_OTHER].contains(&c.name))
            .map(|c| format!("{}: {}", c.name, case_cell(c)))
            .collect::<Vec<_>>()
            .join("; ");
        let cleanup = if self.cleanup.leftovers.is_empty() && self.cleanup.errors.is_empty() {
            format!("clean ({} removed)", self.cleanup.removed)
        } else {
            format!(
                "{} removed, {} leftovers, {} errors",
                self.cleanup.removed,
                self.cleanup.leftovers.len(),
                self.cleanup.errors.len()
            )
        };
        let mut verdict = serde_json::to_value(self.verdict)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default();
        if !self.deviations.is_empty() {
            verdict.push_str(&format!(" ({})", self.deviations.join("; ")));
        }
        let cells = [
            self.label.clone(),
            format!("{}/{}", self.backend, self.transport),
            self.mechanism.to_string(),
            case(CREATE),
            case(REPEAT_SAME),
            case(REPEAT_OTHER),
            race,
            extra,
            cleanup,
            verdict,
        ];
        let cells: Vec<String> = cells.iter().map(|c| md_cell(c)).collect();
        format!("| {} |", cells.join(" | "))
    }

    /// 0 Works and clean; 1 IgnoresCondition; 2 Unsupported; 3 Inconclusive;
    /// 4 verdict Works but cleanup left names or errors.
    pub fn exit_code(&self) -> i32 {
        match self.verdict {
            Verdict::Works
                if self.cleanup.leftovers.is_empty() && self.cleanup.errors.is_empty() =>
            {
                0
            }
            Verdict::Works => 4,
            Verdict::IgnoresCondition => 1,
            Verdict::Unsupported => 2,
            Verdict::Inconclusive => 3,
        }
    }
}

fn case_cell(c: &CaseResult) -> String {
    let mut s = c.outcome.short();
    match c.content_ok {
        Some(false) => s.push_str(", bytes CHANGED"),
        Some(true) => s.push_str(", bytes ok"),
        None => {}
    }
    s
}

/// Escape a table cell: no pipes or line breaks.
fn md_cell(s: &str) -> String {
    s.replace('|', "\\|").replace(['\r', '\n'], " ")
}

/// Every name the probe may write or delete. Cleanup touches only what is recorded here.
pub(crate) struct Ledger {
    prefix: String,
    started_unix: u64,
    /// Root-relative `<prefix>/<name>`.
    keys: Vec<String>,
    /// Root-relative temp names, removed like keys.
    temps: Vec<String>,
    /// Directories the probe created or may have created, in creation order.
    dirs: Vec<String>,
    /// S3 (key, upload_id) of every CreateMultipartUpload that returned an id.
    uploads: Vec<(String, String)>,
}

/// A fresh prefix: `vykar-probe-<unix secs>-<32 hex of rand::random::<u128>()>`.
pub fn new_prefix() -> String {
    format!(
        "{PROBE_PREFIX}{}-{:032x}",
        unix_now(),
        rand::random::<u128>()
    )
}

fn is_safe_name(name: &str) -> bool {
    !name.is_empty() && !name.contains(['/', '\\']) && !name.contains("..")
}

impl Ledger {
    /// Ledger on `opts.prefix`, or on a fresh prefix if none is given. Errors if the
    /// given prefix does not start with [`PROBE_PREFIX`] or is not a safe name.
    pub(crate) fn for_run(opts: &ProbeOptions) -> Result<Self> {
        let prefix = match &opts.prefix {
            Some(p) if p.starts_with(PROBE_PREFIX) && is_safe_name(p) => p.clone(),
            Some(p) => {
                return Err(VykarError::Config(format!(
                    "probe: prefix '{p}' must start with '{PROBE_PREFIX}' and contain no '/', \
                     '\\' or '..'"
                )));
            }
            None => new_prefix(),
        };
        Ok(Self::build(prefix, unix_now()))
    }

    #[cfg(test)]
    pub(crate) fn with_prefix(prefix: &str) -> Self {
        Self::build(prefix.to_string(), unix_now())
    }

    fn build(prefix: String, started_unix: u64) -> Self {
        Self {
            prefix,
            started_unix,
            keys: Vec::new(),
            temps: Vec::new(),
            dirs: Vec::new(),
            uploads: Vec::new(),
        }
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    fn checked(&self, name: &str) -> Result<String> {
        if !is_safe_name(name) {
            return Err(VykarError::Other(format!(
                "probe: unsafe name '{name}' (must be non-empty, without '/', '\\' or '..')"
            )));
        }
        Ok(format!("{}/{name}", self.prefix))
    }

    /// Records and returns `<prefix>/<name>`. `name` must be non-empty and contain no
    /// '/', '\\' or "..": returns `VykarError::Other` otherwise.
    pub(crate) fn key(&mut self, name: &str) -> Result<String> {
        let key = self.checked(name)?;
        self.keys.push(key.clone());
        Ok(key)
    }

    /// Records and returns `<prefix>/<name>` as a temp file, with the same checks.
    pub(crate) fn record_temp(&mut self, name: &str) -> Result<String> {
        let key = self.checked(name)?;
        self.temps.push(key.clone());
        Ok(key)
    }

    /// Records `<prefix>/<dir>` as a directory and returns `<prefix>/<dir>/<name>`
    /// as a key. Both names get the checks of [`Ledger::key`].
    #[cfg_attr(not(feature = "backend-sftp"), allow(dead_code))]
    pub(crate) fn nested_key(&mut self, dir: &str, name: &str) -> Result<String> {
        let dir = self.checked(dir)?;
        self.checked(name)?;
        let key = format!("{dir}/{name}");
        self.dirs.push(dir);
        self.keys.push(key.clone());
        Ok(key)
    }

    pub(crate) fn record_dir(&mut self, dir: String) {
        self.dirs.push(dir);
    }

    pub(crate) fn record_upload(&mut self, key: String, upload_id: String) {
        self.uploads.push((key, upload_id));
    }

    pub(crate) fn keys(&self) -> &[String] {
        &self.keys
    }

    pub(crate) fn temps(&self) -> &[String] {
        &self.temps
    }

    pub(crate) fn dirs(&self) -> &[String] {
        &self.dirs
    }

    pub(crate) fn uploads(&self) -> &[(String, String)] {
        &self.uploads
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Contents of an S3 credentials file. Secrets are never formatted by `Debug`.
pub struct S3CredentialsFile {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: Option<String>,
    pub bucket: Option<String>,
    pub region: Option<String>,
}

impl std::fmt::Debug for S3CredentialsFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3CredentialsFile")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .finish()
    }
}

/// Parse `Name: value` lines. Names, case-insensitive after trimming:
/// `Access key ID` / `aws_access_key_id`, `Secret Access Key` / `aws_secret_access_key`,
/// `Endpoint`, `Bucket`, `Region Code` / `Region`. `=` is accepted in place of `:`.
/// Unknown names and blank lines are ignored; a missing key ID or secret is an error.
/// Error messages name the file and the missing field, never a value.
pub fn read_s3_credentials_file(path: &Path) -> Result<S3CredentialsFile> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        VykarError::Config(format!(
            "cannot read S3 credentials file {}: {}",
            path.display(),
            e.kind()
        ))
    })?;
    let mut access_key_id = None;
    let mut secret_access_key = None;
    let mut endpoint = None;
    let mut bucket = None;
    let mut region = None;
    for line in text.lines() {
        let Some(split) = line.find([':', '=']) else {
            continue;
        };
        let (name, value) = line.split_at(split);
        let value = value.get(1..).unwrap_or_default().trim();
        if value.is_empty() {
            continue;
        }
        let slot = match name.trim().to_ascii_lowercase().as_str() {
            "access key id" | "aws_access_key_id" => &mut access_key_id,
            "secret access key" | "aws_secret_access_key" => &mut secret_access_key,
            "endpoint" => &mut endpoint,
            "bucket" => &mut bucket,
            "region code" | "region" => &mut region,
            _ => continue,
        };
        *slot = Some(value.to_string());
    }
    let missing = |field: &str| {
        VykarError::Config(format!(
            "S3 credentials file {} has no '{field}' line",
            path.display()
        ))
    };
    Ok(S3CredentialsFile {
        access_key_id: access_key_id.ok_or_else(|| missing("Access key ID"))?,
        secret_access_key: secret_access_key.ok_or_else(|| missing("Secret Access Key"))?,
        endpoint,
        bucket,
        region,
    })
}

/// Whether a `create` rejection means the target refuses the mechanism:
/// an S3 `"<status> <Code>"` of 501 / `NotImplemented` or 400 with
/// `InvalidArgument`/`InvalidRequest`, or a detail a backend marked with
/// [`UNSUPPORTED_DETAIL`].
fn is_mechanism_refusal(detail: &str) -> bool {
    if detail.starts_with(UNSUPPORTED_DETAIL) {
        return true;
    }
    let mut parts = detail.split_whitespace();
    let status = parts.next().unwrap_or_default().parse().unwrap_or(0);
    crate::s3_backend::conditional::s3_refuses_mechanism(status, parts.next())
}

/// Verdict from the case list and race, plus one line per deviation.
pub(crate) fn decide(cases: &[CaseResult], race: Option<&RaceResult>) -> (Verdict, Vec<String>) {
    let mut deviations = Vec::new();
    let find = |name: &str| cases.iter().find(|c| c.name == name);

    let ignored_case = cases.iter().any(|c| {
        MUST_NOT_CREATE.contains(&c.name)
            && (matches!(c.outcome, Outcome::Created) || c.content_ok == Some(false))
    });
    let double_winner = race.is_some_and(|r| r.max_winners > 1);

    if let Some(c) = find(MPU_COMPLETE_RETRY)
        && matches!(c.outcome, Outcome::Created)
    {
        deviations.push(
            "mpu_complete_retry: a repeated CompleteMultipartUpload reported success again".into(),
        );
    }
    if let Some(r) = race
        && r.conflicts > 0
    {
        deviations.push(format!(
            "race: {} writers got 409 although nothing was deleted",
            r.conflicts
        ));
    }

    if ignored_case || double_winner {
        return (Verdict::IgnoresCondition, deviations);
    }

    if let Some(c) = find(CREATE) {
        let refused = match &c.outcome {
            Outcome::NotAdvertised => true,
            Outcome::Rejected { detail } => is_mechanism_refusal(detail),
            _ => false,
        };
        if refused {
            return (Verdict::Unsupported, deviations);
        }
    }

    let cases_ok = find(CREATE).is_some()
        && cases
            .iter()
            .filter(|c| !INFORMATIONAL.contains(&c.name))
            .all(|c| c.ok);
    let race_ok = race.is_some_and(|r| {
        r.writers >= 2
            && r.rounds > 0
            && r.rounds_one_winner == r.rounds
            && r.errors == 0
            && r.content_matches_winner == r.rounds
    });
    if cases_ok && race_ok {
        return (Verdict::Works, deviations);
    }
    match race {
        None => deviations.push("race not run".into()),
        Some(r) if r.writers < 2 => deviations.push("race needs at least 2 writers".into()),
        _ => {}
    }
    (Verdict::Inconclusive, deviations)
}

/// Distinct payloads: the 8 bytes `tag`, the 16-byte prefix hash, and `len` pattern bytes,
/// so every write in a run has different bytes from every other.
pub(crate) fn payload(tag: &str, prefix: &str, len: usize) -> Vec<u8> {
    let mut tag_bytes = [b' '; 8];
    for (dst, src) in tag_bytes.iter_mut().zip(tag.bytes()) {
        *dst = src;
    }
    let mut hasher = Md5::new();
    hasher.update(prefix.as_bytes());
    let prefix_hash = hasher.finalize();
    let mut seed = Md5::new();
    seed.update(tag_bytes);
    seed.update(prefix_hash);
    let seed = seed.finalize();

    let mut out = Vec::with_capacity(24 + len);
    out.extend_from_slice(&tag_bytes);
    out.extend_from_slice(&prefix_hash);
    out.extend(
        seed.iter()
            .cycle()
            .take(len)
            .enumerate()
            .map(|(i, b)| b ^ (i as u8)),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(
        name: &'static str,
        outcome: Outcome,
        content_ok: Option<bool>,
        ok: bool,
    ) -> CaseResult {
        CaseResult {
            name,
            expected: "",
            outcome,
            content_ok,
            ok,
        }
    }

    fn exists() -> Outcome {
        Outcome::AlreadyExists { detail: "x".into() }
    }

    fn good_cases() -> Vec<CaseResult> {
        vec![
            case(CREATE, Outcome::Created, Some(true), true),
            case(REPEAT_SAME, exists(), Some(true), true),
            case(REPEAT_OTHER, exists(), Some(true), true),
        ]
    }

    fn good_race() -> RaceResult {
        RaceResult {
            rounds: 3,
            writers: 4,
            rounds_one_winner: 3,
            max_winners: 1,
            already_exists: 9,
            content_matches_winner: 3,
            ..RaceResult::default()
        }
    }

    fn report(verdict: Verdict, cases: Vec<CaseResult>, cleanup: Cleanup) -> Report {
        Report {
            probe_version: PROBE_VERSION,
            label: "label".into(),
            backend: "sftp",
            transport: "ssh".into(),
            mechanism: "hardlink@openssh.com",
            location: "/x".into(),
            prefix: "p".into(),
            started_unix: 0,
            cases,
            race: Some(good_race()),
            verdict,
            deviations: Vec::new(),
            cleanup,
        }
    }

    #[test]
    fn ledger_rejects_unsafe_names() {
        let mut l = Ledger::with_prefix("vykar-probe-t");
        for bad in ["", "a/b", "..", "a\\b"] {
            assert!(l.key(bad).is_err(), "{bad:?} accepted");
            assert!(l.record_temp(bad).is_err(), "{bad:?} accepted as temp");
        }
        assert!(l.nested_key("d", "a/b").is_err());
        assert_eq!(l.key("k").unwrap(), "vykar-probe-t/k");
        assert_eq!(l.keys(), ["vykar-probe-t/k".to_string()]);
    }

    #[test]
    fn ledger_prefix_shape() {
        let a = Ledger::for_run(&ProbeOptions::default()).unwrap();
        let b = Ledger::for_run(&ProbeOptions::default()).unwrap();
        assert_ne!(a.prefix(), b.prefix());
        let rest = a.prefix().strip_prefix(PROBE_PREFIX).unwrap();
        let (secs, hex) = rest.split_once('-').unwrap();
        assert!(secs.parse::<u64>().unwrap() > 1_700_000_000);
        assert_eq!(hex.len(), 32);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn ledger_uses_given_prefix() {
        let given = new_prefix();
        let opts = ProbeOptions {
            prefix: Some(given.clone()),
            ..ProbeOptions::default()
        };
        assert_eq!(Ledger::for_run(&opts).unwrap().prefix(), given);
        for bad in [
            "other-1",
            "vykar-probe-a/b",
            "vykar-probe-..",
            "vykar-probe-a\\b",
        ] {
            let opts = ProbeOptions {
                prefix: Some(bad.into()),
                ..ProbeOptions::default()
            };
            assert!(Ledger::for_run(&opts).is_err(), "{bad:?} accepted");
        }
    }

    /// `create` succeeded but read back other bytes: the repeats are skipped,
    /// and the verdict does not blame the condition.
    #[test]
    fn decide_create_bytes_wrong_is_inconclusive() {
        let create = CaseResult::expect(
            CREATE,
            "created",
            is_created,
            Outcome::Created,
            Some(false),
            true,
        );
        assert!(!create.ok);
        let cases = vec![
            create,
            CaseResult::skipped(REPEAT_SAME, "", "create was not ok"),
            CaseResult::skipped(REPEAT_OTHER, "", "create was not ok"),
        ];
        assert_eq!(decide(&cases, Some(&good_race())).0, Verdict::Inconclusive);
    }

    #[test]
    fn decide_works() {
        let (v, d) = decide(&good_cases(), Some(&good_race()));
        assert_eq!(v, Verdict::Works, "{d:?}");
        assert!(d.is_empty());
    }

    #[test]
    fn decide_ignored_repeat() {
        let mut cases = good_cases();
        cases[2] = case(REPEAT_OTHER, Outcome::Created, Some(true), false);
        assert_eq!(
            decide(&cases, Some(&good_race())).0,
            Verdict::IgnoresCondition
        );
    }

    #[test]
    fn decide_ignored_content() {
        let mut cases = good_cases();
        cases[2] = case(REPEAT_OTHER, exists(), Some(false), false);
        assert_eq!(
            decide(&cases, Some(&good_race())).0,
            Verdict::IgnoresCondition
        );
    }

    #[test]
    fn decide_double_winner() {
        let race = RaceResult {
            max_winners: 2,
            rounds_one_winner: 2,
            ..good_race()
        };
        assert_eq!(
            decide(&good_cases(), Some(&race)).0,
            Verdict::IgnoresCondition
        );
    }

    #[test]
    fn decide_not_advertised() {
        let cases = vec![case(CREATE, Outcome::NotAdvertised, None, false)];
        assert_eq!(decide(&cases, None).0, Verdict::Unsupported);
    }

    #[test]
    fn decide_transport_error() {
        let mut cases = good_cases();
        cases[1] = case(
            REPEAT_SAME,
            Outcome::Transport {
                detail: "reset".into(),
            },
            None,
            false,
        );
        assert_eq!(decide(&cases, Some(&good_race())).0, Verdict::Inconclusive);
    }

    #[test]
    fn decide_single_writer_race() {
        let race = RaceResult {
            writers: 1,
            ..good_race()
        };
        assert_eq!(decide(&good_cases(), Some(&race)).0, Verdict::Inconclusive);
        assert_eq!(decide(&good_cases(), None).0, Verdict::Inconclusive);
    }

    #[test]
    fn decide_mpu_retry_created_is_deviation_only() {
        let mut cases = good_cases();
        cases.push(CaseResult::info(MPU_COMPLETE_RETRY, Outcome::Created, None));
        let (v, d) = decide(&cases, Some(&good_race()));
        assert_eq!(v, Verdict::Works);
        assert_eq!(d.len(), 1, "{d:?}");
    }

    #[test]
    fn decide_local_unsupported_marker() {
        let cases = vec![case(
            CREATE,
            Outcome::Rejected {
                detail: format!("{UNSUPPORTED_DETAIL}Unsupported (os error 45)"),
            },
            None,
            false,
        )];
        assert_eq!(decide(&cases, None).0, Verdict::Unsupported);
    }

    #[test]
    fn exit_codes() {
        let clean = Cleanup::default();
        assert_eq!(
            report(Verdict::Works, good_cases(), clean.clone()).exit_code(),
            0
        );
        assert_eq!(
            report(Verdict::IgnoresCondition, good_cases(), clean.clone()).exit_code(),
            1
        );
        assert_eq!(
            report(Verdict::Unsupported, good_cases(), clean.clone()).exit_code(),
            2
        );
        assert_eq!(
            report(Verdict::Inconclusive, good_cases(), clean).exit_code(),
            3
        );
        let dirty = Cleanup {
            leftovers: vec!["p/x".into()],
            ..Cleanup::default()
        };
        assert_eq!(report(Verdict::Works, good_cases(), dirty).exit_code(), 4);
    }

    fn cell_count(line: &str) -> usize {
        line.replace("\\|", "").matches('|').count()
    }

    #[test]
    fn markdown_row_has_header_column_count() {
        let mut cases = good_cases();
        cases.push(CaseResult::info(
            "err_missing_source",
            Outcome::Info {
                detail: "a | b\nc".into(),
            },
            None,
        ));
        let row = report(Verdict::Works, cases, Cleanup::default()).markdown_row();
        let header = Report::markdown_header().lines().next().unwrap();
        assert_eq!(cell_count(&row), cell_count(header), "{row}");
        assert!(!row.contains('\n'));
    }

    #[test]
    fn markdown_row_shows_advertised_extensions() {
        let mut cases = good_cases();
        cases.insert(
            0,
            CaseResult::info(
                "advertised",
                Outcome::Info {
                    detail: "hardlink@openssh.com: no, posix-rename@openssh.com: yes".into(),
                },
                None,
            ),
        );
        let row = report(Verdict::Unsupported, cases, Cleanup::default()).markdown_row();
        assert!(row.contains("hardlink@openssh.com: no"), "{row}");
    }

    #[test]
    fn credentials_file_owner_layout() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.txt");
        std::fs::write(
            &path,
            "Bucket: b\nEndpoint: https://e.example\n\nRegion Code: r1\n\
             Access key ID: AKDUMMY\nSecret Access Key: SKDUMMY\nOther: ignored\n",
        )
        .unwrap();
        let parsed = read_s3_credentials_file(&path).unwrap();
        assert_eq!(parsed.access_key_id, "AKDUMMY");
        assert_eq!(parsed.secret_access_key, "SKDUMMY");
        assert_eq!(parsed.endpoint.as_deref(), Some("https://e.example"));
        assert_eq!(parsed.bucket.as_deref(), Some("b"));
        assert_eq!(parsed.region.as_deref(), Some("r1"));
        let dbg = format!("{parsed:?}");
        assert!(
            !dbg.contains("AKDUMMY") && !dbg.contains("SKDUMMY"),
            "{dbg}"
        );
    }

    #[test]
    fn credentials_file_aws_names_and_equals() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds");
        std::fs::write(&path, "aws_access_key_id = x\naws_secret_access_key = y\n").unwrap();
        let parsed = read_s3_credentials_file(&path).unwrap();
        assert_eq!(parsed.access_key_id, "x");
        assert_eq!(parsed.secret_access_key, "y");
        assert_eq!(parsed.endpoint, None);
    }

    #[test]
    fn credentials_file_missing_secret() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds");
        std::fs::write(&path, "Access key ID: AKVALUE\n").unwrap();
        let err = read_s3_credentials_file(&path).unwrap_err().to_string();
        assert!(err.contains("Secret Access Key"), "{err}");
        assert!(!err.contains("AKVALUE"), "{err}");
    }

    #[test]
    fn payloads_differ_by_tag_and_prefix() {
        let a = payload("A", "p1", 64);
        assert_eq!(a.len(), 88);
        assert_ne!(a, payload("B", "p1", 64));
        assert_ne!(a, payload("A", "p2", 64));
        assert_eq!(a, payload("A", "p1", 64));
    }
}
