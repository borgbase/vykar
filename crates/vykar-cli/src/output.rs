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
use std::io::{BufWriter, IsTerminal, Write};
use std::process::ExitCode;
use std::sync::atomic::Ordering;

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use serde_json::value::RawValue;

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

impl Status {
    /// Classify a repository command's outcome. A shutdown signal outranks it.
    pub(crate) fn of(result: &CliResult<bool>) -> Self {
        if signal::SHUTDOWN.load(Ordering::SeqCst) {
            return Self::Interrupted;
        }
        match result {
            Ok(true) => Self::Partial,
            Ok(false) => Self::Ok,
            Err(_) => Self::Error,
        }
    }

    pub(crate) fn exit_code(self) -> ExitCode {
        match self {
            Self::Ok | Self::Skipped => ExitCode::SUCCESS,
            Self::Partial => ExitCode::from(EXIT_PARTIAL),
            Self::Error => ExitCode::from(EXIT_ERROR),
            Self::Interrupted => ExitCode::from(EXIT_INTERRUPTED),
        }
    }
}

/// Emitted results, already serialized, keyed by [`Report::KEY`] in emit order.
#[derive(Default)]
struct Results(Vec<(&'static str, Box<RawValue>)>);

impl Results {
    fn insert(&mut self, key: &'static str, value: Box<RawValue>) {
        match self.0.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => self.0.push((key, value)),
        }
    }
}

impl Serialize for Results {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.iter().map(|(k, v)| (k, v)))
    }
}

#[derive(Serialize)]
struct RepoRecord {
    label: Option<String>,
    url: String,
    status: Status,
    skip_reason: Option<String>,
    error: Option<String>,
    result: Results,
}

#[derive(Serialize)]
struct Document<'a> {
    version: u32,
    command: &'a str,
    status: Status,
    error: Option<&'a str>,
    result: &'a Results,
    repositories: &'a [RepoRecord],
}

pub(crate) struct Output {
    json: bool,
    command: String,
    in_repo: bool,
    repos: Vec<RepoRecord>,
    /// Results of commands that run without a repository (`config`).
    result: Results,
    error: Option<String>,
    diverted: Option<stdout_divert::Diverted>,
}

impl Output {
    pub(crate) fn new(json: bool, command: String) -> Self {
        Self {
            json,
            command,
            in_repo: false,
            repos: Vec::new(),
            result: Results::default(),
            error: None,
            diverted: None,
        }
    }

    /// With `--json`, point the process's stdout at stderr until [`finish`],
    /// so that hook commands and stray prints cannot corrupt the document.
    ///
    /// [`finish`]: Self::finish
    pub(crate) fn divert_stdout(&mut self) -> std::io::Result<()> {
        if self.json && self.diverted.is_none() {
            self.diverted = Some(stdout_divert::divert()?);
        }
        Ok(())
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
        let value = serde_json::value::to_raw_value(&report.json())?;
        let target = match self.repos.last_mut() {
            Some(record) if self.in_repo => &mut record.result,
            _ => &mut self.result,
        };
        target.insert(R::KEY, value);
        Ok(())
    }

    pub(crate) fn begin_repo(&mut self, repo: &ResolvedRepo) {
        self.repos.push(RepoRecord {
            label: repo.label.clone(),
            url: repo.config.repository.url.clone(),
            status: Status::Ok,
            skip_reason: None,
            error: None,
            result: Results::default(),
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

    /// Close the current repository's entry with the command's outcome, and
    /// return that outcome's [`Status`]. A skip recorded by [`Self::skip_repo`]
    /// stays when the command otherwise succeeded.
    pub(crate) fn end_repo(&mut self, result: &CliResult<bool>) -> Status {
        let status = Status::of(result);
        if let Some(record) = self.repos.last_mut().filter(|_| self.in_repo) {
            if status != Status::Ok {
                record.status = status;
            }
            if let (Status::Error, Err(e)) = (status, result) {
                record.error = Some(e.to_string());
            }
        }
        self.in_repo = false;
        status
    }

    /// Report an error that is not any one repository's, and return the
    /// matching exit code.
    pub(crate) fn fail(&mut self, message: impl Display) -> ExitCode {
        let message = message.to_string();
        eprintln!("Error: {message}");
        self.error = Some(message);
        ExitCode::from(EXIT_ERROR)
    }

    /// Record an error that is not any one repository's and was already printed.
    pub(crate) fn set_error(&mut self, message: String) {
        self.error = Some(message);
    }

    pub(crate) fn finish(mut self, code: ExitCode) -> ExitCode {
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
        match write_document(self.diverted.take(), &document) {
            Ok(()) => code,
            Err(e) => {
                eprintln!("Error: could not write the JSON result: {e}");
                ExitCode::from(EXIT_ERROR)
            }
        }
    }
}

/// Restore the original stdout and write the document to it.
fn write_document(
    diverted: Option<stdout_divert::Diverted>,
    document: &Document<'_>,
) -> CliResult<()> {
    let stdout = std::io::stdout();
    // Buffered stray output belongs to stderr, where stdout points until now.
    stdout.lock().flush()?;
    if let Some(diverted) = diverted {
        stdout_divert::restore(diverted)?;
    }
    let mut writer = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut writer, document)?;
    writeln!(writer)?;
    writer.flush().map_err(CliError::from)
}

/// Swaps the process-wide stdout (inherited by child processes) for stderr.
mod stdout_divert {
    // dup2 / SetStdHandle have no safe std wrapper; SAFETY documented per block.
    #![allow(unsafe_code)]

    use std::io;

    #[cfg(unix)]
    pub(crate) struct Diverted(std::os::fd::OwnedFd);

    #[cfg(unix)]
    pub(crate) fn divert() -> io::Result<Diverted> {
        use std::os::fd::AsFd;

        let saved = io::stdout().as_fd().try_clone_to_owned()?;
        // SAFETY: dup2 only operates on descriptor numbers; both are the
        // process's standard descriptors and failure is checked.
        if unsafe { libc::dup2(libc::STDERR_FILENO, libc::STDOUT_FILENO) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Diverted(saved))
    }

    #[cfg(unix)]
    pub(crate) fn restore(diverted: Diverted) -> io::Result<()> {
        use std::os::fd::AsRawFd;

        // SAFETY: `diverted.0` is an open descriptor owned by `diverted`, which
        // lives until after the call; failure is checked.
        if unsafe { libc::dup2(diverted.0.as_raw_fd(), libc::STDOUT_FILENO) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(windows)]
    pub(crate) struct Diverted(windows_sys::Win32::Foundation::HANDLE);

    #[cfg(windows)]
    pub(crate) fn divert() -> io::Result<Diverted> {
        use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
        use windows_sys::Win32::System::Console::{
            GetStdHandle, STD_ERROR_HANDLE, STD_OUTPUT_HANDLE, SetStdHandle,
        };

        // SAFETY: GetStdHandle takes a constant and has no Rust-side
        // preconditions; the returned handles are validated below.
        let (stdout, stderr) = unsafe {
            (
                GetStdHandle(STD_OUTPUT_HANDLE),
                GetStdHandle(STD_ERROR_HANDLE),
            )
        };
        if stdout == INVALID_HANDLE_VALUE || stderr == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `stderr` is the process's own standard error handle, so it
        // stays valid as its standard output; failure is checked.
        if unsafe { SetStdHandle(STD_OUTPUT_HANDLE, stderr) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Diverted(stdout))
    }

    #[cfg(windows)]
    pub(crate) fn restore(diverted: Diverted) -> io::Result<()> {
        use windows_sys::Win32::System::Console::{STD_OUTPUT_HANDLE, SetStdHandle};

        // SAFETY: `diverted.0` is the original standard output handle, which
        // nothing closed while it was replaced; failure is checked.
        if unsafe { SetStdHandle(STD_OUTPUT_HANDLE, diverted.0) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    pub(crate) struct Diverted;

    #[cfg(not(any(unix, windows)))]
    pub(crate) fn divert() -> io::Result<Diverted> {
        Ok(Diverted)
    }

    #[cfg(not(any(unix, windows)))]
    pub(crate) fn restore(_: Diverted) -> io::Result<()> {
        Ok(())
    }
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
