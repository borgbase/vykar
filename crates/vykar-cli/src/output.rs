//! Command results on stdout: human-readable text, or with `--json` one JSON
//! document per invocation.
//!
//! Commands never print results themselves. Each builds a [`Report`] and hands
//! it to [`Output::emit`], which renders it for humans at once or files it
//! under the current repository's `result` object for the final document.
//! The JSON is a public interface for scripts: it is built from dedicated view
//! types, never from repository or wire types, so a format change cannot
//! silently change it. Its layout is documented in
//! `docs/src/content/docs/json-output.md`.

use std::fmt::Display;
use std::io::{IsTerminal, Write};
use std::process::ExitCode;
use std::sync::atomic::Ordering;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Map, Value};

use vykar_core::config::ResolvedRepo;
use vykar_core::snapshot::SnapshotStats;
use vykar_core::snapshot::item::ItemType;

use crate::error::{CliError, CliResult};
use crate::{EXIT_ERROR, EXIT_INTERRUPTED, EXIT_PARTIAL, signal};

/// Version of the `--json` document layout. Raised only for a change that
/// breaks existing consumers; adding fields does not raise it.
pub(crate) const JSON_VERSION: u32 = 1;

pub(crate) trait Report {
    /// Key of this result in the repository's `result` object.
    const KEY: &'static str;

    fn json(&self) -> impl Serialize;

    fn print_human(&self);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Status {
    Ok,
    Partial,
    Skipped,
    Error,
    Interrupted,
}

#[derive(Serialize)]
struct RepoRecord {
    label: Option<String>,
    url: String,
    status: Status,
    skip_reason: Option<String>,
    error: Option<String>,
    result: Map<String, Value>,
}

#[derive(Serialize)]
struct Document<'a> {
    version: u32,
    command: &'a str,
    status: Status,
    error: Option<&'a str>,
    result: &'a Map<String, Value>,
    repositories: &'a [RepoRecord],
}

pub(crate) struct Output {
    json: bool,
    command: String,
    in_repo: bool,
    repos: Vec<RepoRecord>,
    /// Results of commands that run without a repository (`config`).
    result: Map<String, Value>,
    error: Option<String>,
}

impl Output {
    pub(crate) fn new(json: bool, command: String) -> Self {
        Self {
            json,
            command,
            in_repo: false,
            repos: Vec::new(),
            result: Map::new(),
            error: None,
        }
    }

    pub(crate) fn is_json(&self) -> bool {
        self.json
    }

    /// Whether the command may ask the user a question. A `--json` caller is
    /// a program, so it never is.
    pub(crate) fn interactive(&self) -> bool {
        !self.json && std::io::stdin().is_terminal()
    }

    pub(crate) fn emit<R: Report>(&mut self, report: &R) -> CliResult<()> {
        if !self.json {
            report.print_human();
            return Ok(());
        }
        let value = serde_json::to_value(report.json())?;
        let target = match self.repos.last_mut() {
            Some(record) if self.in_repo => &mut record.result,
            _ => &mut self.result,
        };
        target.insert(R::KEY.to_string(), value);
        Ok(())
    }

    pub(crate) fn begin_repo(&mut self, repo: &ResolvedRepo) {
        self.repos.push(RepoRecord {
            label: repo.label.clone(),
            url: repo.config.repository.url.clone(),
            status: Status::Ok,
            skip_reason: None,
            error: None,
            result: Map::new(),
        });
        self.in_repo = true;
    }

    /// Record why a hook skipped the current repository.
    pub(crate) fn skip_repo(&mut self, reason: &str) {
        if let Some(record) = self.repos.last_mut().filter(|_| self.in_repo) {
            record.status = Status::Skipped;
            record.skip_reason = Some(reason.to_string());
        }
    }

    pub(crate) fn end_repo(&mut self, result: &CliResult<bool>) {
        if let Some(record) = self.repos.last_mut().filter(|_| self.in_repo) {
            if signal::SHUTDOWN.load(Ordering::SeqCst) {
                record.status = Status::Interrupted;
            } else {
                match result {
                    Ok(true) => record.status = Status::Partial,
                    Ok(false) => {}
                    Err(e) => {
                        record.status = Status::Error;
                        record.error = Some(e.to_string());
                    }
                }
            }
        }
        self.in_repo = false;
    }

    /// Report an error that is not any one repository's, and return the
    /// matching exit code.
    pub(crate) fn fail(&mut self, message: impl Display) -> ExitCode {
        let message = message.to_string();
        eprintln!("Error: {message}");
        self.error = Some(message);
        ExitCode::from(EXIT_ERROR)
    }

    pub(crate) fn finish(self, code: ExitCode) -> ExitCode {
        if !self.json {
            return code;
        }
        let status = if code == ExitCode::SUCCESS {
            Status::Ok
        } else if code == ExitCode::from(EXIT_PARTIAL) {
            Status::Partial
        } else if code == ExitCode::from(EXIT_INTERRUPTED) {
            Status::Interrupted
        } else {
            Status::Error
        };
        let document = Document {
            version: JSON_VERSION,
            command: &self.command,
            status,
            error: self.error.as_deref(),
            result: &self.result,
            repositories: &self.repos,
        };
        match write_document(&document) {
            Ok(()) => code,
            Err(e) => {
                eprintln!("Error: could not write the JSON result: {e}");
                ExitCode::from(EXIT_ERROR)
            }
        }
    }
}

fn write_document(document: &Document<'_>) -> CliResult<()> {
    let stdout = std::io::stdout();
    let text = if stdout.is_terminal() {
        serde_json::to_string_pretty(document)?
    } else {
        serde_json::to_string(document)?
    };
    let mut lock = stdout.lock();
    writeln!(lock, "{text}")?;
    lock.flush().map_err(CliError::from)
}

#[derive(Serialize)]
pub(crate) struct SnapshotStatsJson {
    nfiles: u64,
    original_size: u64,
    compressed_size: u64,
    deduplicated_size: u64,
    errors: u64,
}

impl From<&SnapshotStats> for SnapshotStatsJson {
    fn from(stats: &SnapshotStats) -> Self {
        Self {
            nfiles: stats.nfiles,
            original_size: stats.original_size,
            compressed_size: stats.compressed_size,
            deduplicated_size: stats.deduplicated_size,
            errors: stats.errors,
        }
    }
}

pub(crate) fn item_type_name(entry_type: ItemType) -> &'static str {
    match entry_type {
        ItemType::RegularFile => "file",
        ItemType::Directory => "dir",
        ItemType::Symlink => "symlink",
    }
}

/// A nanosecond Unix timestamp as UTC, `None` when out of range.
pub(crate) fn utc_from_nanos(nanos: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(
        nanos.div_euclid(1_000_000_000),
        nanos.rem_euclid(1_000_000_000) as u32,
    )
}

/// `None` for an empty string: JSON reports a missing value as `null`.
pub(crate) fn non_empty(s: &str) -> Option<&str> {
    (!s.is_empty()).then_some(s)
}
