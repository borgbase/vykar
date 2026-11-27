use std::fmt;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, VykarError>;

/// Parsed fields of a session marker, present only when the marker JSON
/// could be decoded successfully.
#[derive(Debug, Clone)]
pub struct ActiveSessionDetails {
    pub hostname: String,
    pub pid: u32,
    /// Age of the session's `last_refresh` timestamp, pre-formatted
    /// (e.g. `"2h"`, `"3d 4h"`). Pre-formatting keeps this type free of
    /// chrono so it can live in `vykar-types`.
    pub age: String,
}

/// Summary of an active backup session blocking maintenance.
///
/// `details` is `None` when the marker's JSON could not be parsed — such
/// markers are preserved on storage while younger than the session
/// staleness by their mtime, so maintenance fails closed; after that they
/// are reaped like stale markers, and `vykar break-lock --sessions` removes
/// them at once.
#[derive(Debug, Clone)]
pub struct ActiveSessionInfo {
    pub id: String,
    pub details: Option<ActiveSessionDetails>,
}

/// List of active sessions blocking a maintenance command. Always non-empty
/// (an empty list should be represented by not returning `ActiveSessions`).
#[derive(Debug, Clone)]
pub struct ActiveSessionList(pub Vec<ActiveSessionInfo>);

impl ActiveSessionList {
    /// Returns true if any entry has an unparseable marker.
    pub fn has_malformed(&self) -> bool {
        self.0.iter().any(|s| s.details.is_none())
    }
}

impl fmt::Display for ActiveSessionList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{} active backup session(s) blocking maintenance:",
            self.0.len()
        )?;
        for s in &self.0 {
            match &s.details {
                Some(d) => writeln!(
                    f,
                    "  - {} (host={}, pid={}, last refresh {} ago)",
                    s.id, d.hostname, d.pid, d.age
                )?,
                None => writeln!(f, "  - {} (malformed marker, cannot parse)", s.id)?,
            }
        }
        write!(
            f,
            "Wait for in-progress backups to finish, or run `vykar break-lock --sessions` to force-clear."
        )
    }
}

/// Parsed fields of a marker or 0.24 lock, present only when it could be
/// read and decoded.
#[derive(Debug, Clone)]
pub struct LockHolderDetails {
    pub hostname: String,
    pub pid: u32,
    /// Age of the lock's acquisition timestamp, pre-formatted (e.g. `"12s"`,
    /// `"4m"`). Pre-formatting keeps this type free of chrono so it can live
    /// in `vykar-types`.
    pub age: String,
}

/// Identifies whatever holds the repository: a session marker
/// (`sessions/x-*.json`: an exclusive session; another `sessions/*.json`: a
/// committing backup or a compact), or a vykar 0.24 lock key
/// (`locks/*.json`) that a v2/v3 upgrade refuses beside.
///
/// `details` is `None` when the marker or lock could not be fetched or
/// parsed — only its storage key is known.
#[derive(Debug, Clone)]
pub struct LockHolder {
    pub key: String,
    pub details: Option<LockHolderDetails>,
}

impl fmt::Display for LockHolder {
    /// Renders on a single line: this is printed into the cycle summary table
    /// as `FAILED: {e}` (see `vykar-cli`'s `print_summary`), where multi-line
    /// output would break the alignment.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // An exclusive session's marker (`sessions/x-*.json`): `check
        // --repair`, `upgrade` or a pack-map write runs.
        if self.key.starts_with("sessions/x-") {
            return match &self.details {
                Some(d) => write!(
                    f,
                    "an exclusive maintenance run (`check --repair`, `upgrade` or a pack-map \
                     write) on {} (PID {}, started {} ago) holds the repository; nothing else \
                     writes to it until it ends. Wait for it to finish; if that process is gone, \
                     its marker stops blocking 45 minutes after its last refresh, or run \
                     `vykar break-lock --sessions` (it asks for confirmation and removes every \
                     session marker, those of running backups included).",
                    d.hostname, d.pid, d.age
                ),
                // Malformed, or gone between the listing and the read of it.
                None => write!(
                    f,
                    "an exclusive maintenance run's session marker is unreadable or was just \
                     removed (key: {}). Run the command again; if it persists and no \
                     `check --repair`, `upgrade` or pack-map write is running, wait until it is \
                     45 minutes old, or run `vykar break-lock --sessions` (it asks for \
                     confirmation and removes every session marker, those of running backups \
                     included).",
                    self.key
                ),
            };
        }
        // A session marker (`sessions/*.json`) holds the repository when a
        // backup is committing or a compact runs.
        if self.key.starts_with("sessions/") {
            return match &self.details {
                Some(d) => write!(
                    f,
                    "a backup committing or a compact running on {} (PID {}, started {} \
                     ago) holds the repository. Wait for it to finish, or run \
                     `vykar break-lock --sessions` if that process is gone.",
                    d.hostname, d.pid, d.age
                ),
                // Malformed, or gone between the listing and the read of it.
                None => write!(
                    f,
                    "a session marker that may belong to a committing backup is unreadable or \
                     was just removed (key: {}). Run the command again; if it persists and no \
                     backup is running, run `vykar break-lock --sessions` (an unreadable marker \
                     blocks exclusive runs and, on a single-writer repository, other head \
                     writers until it is 45 minutes old by its modification time, and for good \
                     where the storage reports none).",
                    self.key
                ),
            };
        }
        match &self.details {
            Some(d) => write!(
                f,
                "repository is locked by {} (PID {}), held for {}. \
                 Another machine may be running prune/compact, or the lock is stale. \
                 Wait for it to finish, or run `vykar break-lock` if that process is gone.",
                d.hostname, d.pid, d.age
            ),
            None => write!(
                f,
                "repository is locked by another process (lock: {}, holder details unreadable). \
                 Another machine may be running prune/compact, or the lock is stale. \
                 Wait for it to finish, or run `vykar break-lock` if that process is gone.",
                self.key
            ),
        }
    }
}

impl VykarError {
    /// Whether a wrong passphrase is still a live explanation for this error.
    ///
    /// Passphrase retry prompts and cached-passphrase eviction key off this,
    /// so **every** ambiguous authentication failure must be listed: a new
    /// variant left out here silently stops callers from re-prompting, and
    /// leaves a stale cached passphrase in place.
    pub fn is_passphrase_failure(&self) -> bool {
        matches!(
            self,
            Self::DecryptionFailed | Self::RepositoryKeyUnwrapFailed(_)
        )
    }

    /// Whether a failed write or delete looks like the backend *refusing* it
    /// on policy — append-only mode, a read-only mount, missing permissions —
    /// rather than a transient fault.
    ///
    /// Backends surface this only as a message, so this is a heuristic.
    /// Callers use it to decide whether to add a hint; the message shown
    /// must keep the backend's own reason, because a 403 is not proof of
    /// append-only mode.
    pub fn is_write_refusal(&self) -> bool {
        let msg = self.to_string().to_lowercase();
        msg.contains("append-only")
            || msg.contains("immutable")
            || msg.contains("read-only")
            || msg.contains("forbidden")
            || msg.contains("permission")
            || msg.contains("403")
    }
}

#[derive(Debug, Error)]
pub enum VykarError {
    #[error("repository not found at '{0}'")]
    RepoNotFound(String),

    #[error("repository already exists at '{0}'")]
    RepoAlreadyExists(String),

    #[error("decryption failed: wrong passphrase or corrupted data")]
    DecryptionFailed,

    /// The repository key could not be unwrapped, and the cause is genuinely
    /// ambiguous — wrong passphrase, or damage inside the authenticated
    /// payload, which AEAD cannot tell apart.
    ///
    /// Carries the full diagnosis (which copies were consulted, whether they
    /// corroborated each other) while staying a *typed* authentication
    /// failure, so passphrase re-prompting and cached-passphrase eviction keep
    /// working. Classify a failure here only when a wrong passphrase is still
    /// a live explanation; proven corruption belongs in another variant.
    #[error("{0}")]
    RepositoryKeyUnwrapFailed(String),

    #[error("passphrase must not be empty")]
    EmptyPassphrase,

    #[error("passphrases do not match")]
    PassphraseMismatch,

    #[error("key derivation error: {0}")]
    KeyDerivation(String),

    #[error("snapshot not found: '{0}'")]
    SnapshotNotFound(String),

    #[error("snapshot already exists: '{0}'")]
    SnapshotAlreadyExists(String),

    #[error("invalid repository format: {0}")]
    InvalidFormat(String),

    /// An `index.d/` object that the index head names is absent from
    /// storage, and a fresh read of the head still names it: the object is
    /// lost, not removed by a concurrent fold. `kind` is `"base"` or
    /// `"segment"`; `key` the storage key (`index.d/<64 hex>`).
    #[error(
        "index {kind} {key} is missing from storage although the index head names it; \
         `vykar check --repair` rebuilds the index from what can still be read and may \
         remove snapshots whose data it then cannot locate (run it with `--dry-run` \
         first to see which)"
    )]
    IndexObjectMissing { kind: &'static str, key: String },

    /// A lock-free index read kept finding objects missing and the head
    /// moved on every re-read: a concurrent writer, not damage.
    #[error(
        "index objects kept vanishing while reading the index, and the index head \
         changed on every re-read; another client is rewriting the index, retry later"
    )]
    IndexHeadUnstable,

    /// The index head moved since the index was loaded, or another writer
    /// created the generation this write tried. Maintenance writes treat it
    /// as "moved" (`repo::index_write::is_stale_index_error` in vykar-core).
    #[error("the index changed on storage after it was loaded; retry the operation")]
    IndexChanged,

    /// Another writer created the index head on every attempt of a write.
    /// `detail` says which write and how often. Maintenance does not match it.
    #[error("the index changed on storage after it was loaded: {detail}")]
    IndexHeadContention { detail: String },

    #[error("unknown object type tag: {0}")]
    UnknownObjectType(u8),

    #[error("unknown compression tag: {0}")]
    UnknownCompressionTag(u8),

    #[error("unsupported repository version: {0}")]
    UnsupportedVersion(u32),

    #[error(
        "snapshot was written by a newer vykar (format version {version}); \
         upgrade vykar to read it"
    )]
    UnsupportedSnapshotVersion { version: u32 },

    /// The snapshot's positional-array envelope does not match this build's.
    ///
    /// Only ever raised after the AEAD decrypt succeeded, which proves the
    /// bytes are intact and the key is right — so this is a version skew, never
    /// corruption, and must never be "repaired" by deleting the snapshot.
    #[error("snapshot metadata is incompatible with this build: {detail}")]
    IncompatibleSnapshotEnvelope { detail: String },

    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("unsupported backend: '{0}'")]
    UnsupportedBackend(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An I/O error with the path it happened at ([`IoContext::ctx`]).
    /// Classified like `Io` ([`VykarError::io_error`]).
    #[error("I/O error at {path}: {source}")]
    IoAt {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("{0}")]
    Locked(LockHolder),

    #[error("chunk not found in index: {0}")]
    ChunkNotInIndex(crate::chunk_id::ChunkId),

    /// A live snapshot names a tree chunk, or references a data chunk,
    /// that the index has no entry for (compact's mark, the unreferenced
    /// report). The index lost entries: `check --repair` re-indexes them
    /// from the packs when they still describe them.
    #[error(
        "snapshot '{snapshot}' {} chunk {chunk}, which is not in the index",
        live_chunk_role(*tree)
    )]
    LiveChunkNotInIndex {
        snapshot: String,
        chunk: crate::chunk_id::ChunkId,
        /// A tree chunk the snapshot names, else a data chunk it references.
        tree: bool,
    },

    /// A pack answered 404 to a read, and no newer head relocated its chunks.
    #[error("pack not found: {0}")]
    PackNotFound(crate::pack_id::PackId),

    /// A range read of a pack returned fewer bytes than asked.
    #[error(
        "short read from pack {pack_id} at offset {offset}: expected {expected} bytes, got {actual}"
    )]
    PackShortRead {
        pack_id: crate::pack_id::PackId,
        offset: u64,
        expected: u64,
        actual: u64,
    },

    /// A stored chunk failed to decode, with its location (restore's form,
    /// [`crate::chunk_decode::ChunkDecodeError::into_located`]). Boxed for
    /// `clippy::result_large_err`.
    #[error("{0}")]
    ChunkDecode(Box<crate::chunk_decode::ChunkDecodeError>),

    /// A restore aborted because repository data it needed could not be read
    /// or decoded. The inner error is the original failure. Nothing was moved
    /// into the destination; removing the staging tree is best-effort.
    #[error("{0}; no restored files were published")]
    RestoreSourceUnreadable(#[source] Box<VykarError>),

    #[error("decompression error: {0}")]
    Decompression(String),

    #[error("operation interrupted by signal")]
    Interrupted,

    #[error("active sessions prevent maintenance: {0}")]
    ActiveSessions(ActiveSessionList),

    /// `command` would delete repository objects, and the repository's
    /// vykar-server advertised append-only mode. Returned before the command
    /// changed anything. One line: it is printed into the cycle summary.
    #[error(
        "{command} deletes data, and the repository server is append-only (it refuses deletes); \
         nothing was changed. Run it from a trusted machine in a maintenance window, with the \
         server restarted without --append-only (see \"Maintenance on an append-only server\" \
         in the server setup guide)"
    )]
    AppendOnlyServer { command: &'static str },

    /// The repository is multi-writer (`multi_writer` in its config): its
    /// clients commit without a lock, which needs put-if-absent, and this
    /// client's storage does not offer it or its check concluded it
    /// unsupported. A write command returns it right after open, before it
    /// writes anything; a commit whose storage withdrew put-if-absent after
    /// the check passed returns it before its head create. `backend`: the
    /// repository URL, or "the storage backend". `reason`: the check's
    /// verdict.
    #[error(
        "this repository is multi-writer (its clients commit without a lock, which needs \
         put-if-absent), but {backend} does not offer put-if-absent to this client ({reason}), \
         so this client cannot run the command on it. If the storage supports conditional \
         writes, check the backend's settings (on S3: s3_conditional_writes). Otherwise run \
         `vykar upgrade --commit-mode single`: the repository is then single-writer, one \
         writer at a time on this storage (a backup waits while another one commits or a \
         compact runs, a compact refuses beside a committing backup), so backups no longer \
         commit side by side"
    )]
    MultiWriterUnsupported { backend: String, reason: String },

    /// A single-writer repository's head write (10-single-protocol step 7)
    /// found, right after its plain PUT, other bytes at the generation it
    /// wrote: another writer wrote the same generation at the same time
    /// (the best-effort single-writer check did not keep it out). This run
    /// writes nothing more: no snapshot, no garbage collection.
    #[error(
        "another writer wrote index generation {generation} at the same time as this run \
         (on a single-writer repository only one writer may commit at a time); this run \
         wrote nothing more. Run `vykar check`; if it reports chunks missing from the \
         index, run `vykar check --repair`"
    )]
    ConcurrentHeadWrite { generation: u64 },

    /// Compact did not run because the live snapshots could not be marked;
    /// nothing was changed. The text starts with vykar-core's
    /// `commands::compact::MARK_REFUSED`; the simulator matches the variant.
    /// The inner error is in the text and deliberately not a `source`, so a
    /// chain printer does not print it twice. The advice that ends the text
    /// depends on the cause ([`mark_refusal_advice`]).
    #[error(
        "compact refused: the live snapshots could not be marked, nothing was changed: {0}. {advice}",
        advice = mark_refusal_advice(.0)
    )]
    MarkRefused(Box<VykarError>),

    #[error("commit failed: referenced chunks were deleted since session started")]
    StaleChunksDuringCommit,

    /// A run writing without the lock found its session marker reaped, or
    /// its record of the marker broken (a gap, a miss, or a marker write
    /// that was too slow), before a write the marker guards:
    /// another client may have taken the session for ended. A backup checks
    /// between its commit mark and its snapshot write (the index head it
    /// wrote stays without its snapshot; a retry deduplicates against that
    /// head); a compact, prune or delete before each index head, each batch
    /// of deletes and each snapshot delete (what it wrote so far is
    /// unreferenced debris); an exclusive run (`check --repair`, `upgrade`,
    /// pack maps) before every write.
    #[error(
        "this run's session marker was reaped, lost, or not refreshed in time while it was writing \
         (a backup's commit, a maintenance run's index heads and deletes, an exclusive run's \
         writes); nothing more was written: run the command again (a backup reuses the data it \
         uploaded)"
    )]
    MarkerLostDuringCommit,

    /// An index write refused because its first object upload was longer
    /// ago than its objects' grace. `head_written`: the refusal came after
    /// the head create. The text starts with vykar-core's
    /// `repo::ages::SLOW_INDEX_WRITE_TEXT`; the simulator matches the variant.
    #[error(
        "the index write took longer than {} h {} min since its first object upload; retry{}",
        limit_mins / 60,
        limit_mins % 60,
        slow_index_write_suffix(*head_written)
    )]
    SlowIndexWrite { limit_mins: u64, head_written: bool },

    /// A reader planned a chunk read from a head that a concurrent prune and
    /// compact made obsolete: the pack answered 404 and the latest head has no
    /// entry for the chunk. Retrying reads the repository as it is now. Not
    /// damage by itself; `vykar check` tells.
    #[error(
        "chunk {chunk_id} was removed from the repository while it was being read \
         (its snapshot was deleted and compacted meanwhile); run the command again, \
         and run `vykar check` if the snapshot still exists"
    )]
    ChunkRemovedDuringRead { chunk_id: crate::chunk_id::ChunkId },

    /// The v2/v3 upgrade's legacy lock key lapsed.
    #[error("lock expired: {0}")]
    LockExpired(String),

    #[error("repository identity mismatch: {0}")]
    RepositoryMismatch(String),

    #[error("file changed during read: {path}{}", if *dataless { " (cloud-only file, hydration in progress)" } else { "" })]
    FileChangedDuringRead {
        path: String,
        /// macOS hint: the file was reported dataless on the post-read fstat,
        /// suggesting hydration is in progress. Surfaced in the message so
        /// the user can recognize the iCloud Drive / Dropbox / `OneDrive` case.
        ///
        /// Kept structural (rather than inlined into `path`) so consumers
        /// that round-trip, prefix-strip, or render `path` as a clickable
        /// link see a clean path; the `Display` impl does the formatting.
        dataless: bool,
    },

    #[error("{0}")]
    Other(String),
}

/// The verb of [`VykarError::LiveChunkNotInIndex`]'s text.
fn live_chunk_role(tree: bool) -> &'static str {
    if tree { "names tree" } else { "references" }
}

/// What a refused mark tells the user to do: `check --repair` when the
/// index lost entries of a live snapshot, else to look with `check`.
pub fn mark_refusal_advice(cause: &VykarError) -> &'static str {
    match cause {
        VykarError::LiveChunkNotInIndex { .. } => {
            "The index lost entries a live snapshot needs; run `vykar check --repair`, which \
             re-indexes them from the packs that hold them"
        }
        _ => "Run `vykar check` for details",
    }
}

/// The end of [`VykarError::SlowIndexWrite`]'s text.
fn slow_index_write_suffix(head_written: bool) -> &'static str {
    if head_written {
        ". The index head was written without its snapshot; the next run continues from it. \
         Run `vykar check` if the repository fails to open"
    } else {
        ""
    }
}

/// Attach the path an I/O error happened at. For new code; the blanket
/// `#[from] io::Error` stays for `?` without context.
pub trait IoContext<T> {
    fn ctx(self, path: impl AsRef<std::path::Path>) -> Result<T>;
}

impl<T> IoContext<T> for std::result::Result<T, std::io::Error> {
    fn ctx(self, path: impl AsRef<std::path::Path>) -> Result<T> {
        self.map_err(|source| VykarError::IoAt {
            path: path.as_ref().display().to_string(),
            source,
        })
    }
}

/// On Unix, returns `true` if the raw OS error is EIO (errno 5).
fn is_eio(e: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        e.raw_os_error() == Some(5)
    }
    #[cfg(not(unix))]
    {
        let _ = e;
        false
    }
}

/// macOS: `EDEADLK` (errno 11) from `read_dir`/`open` on a `FileProvider` inode
/// that would have to be materialized from a context that cannot drive
/// `fileproviderd` (e.g. a background launchd backup).
///
/// Scoped to macOS because errno 11 means something else everywhere else: on
/// Linux it is `EAGAIN`/`EWOULDBLOCK` (`EDEADLK` there is 35), and masking a
/// spurious would-block as a skip would hide real bugs.
///
/// Raw-errno match rather than a `libc` constant: `vykar-types` has no `libc`
/// dependency, matching the style of the internal `is_eio` helper.
pub fn is_fileprovider_deadlock(e: &std::io::Error) -> bool {
    #[cfg(target_os = "macos")]
    {
        e.raw_os_error() == Some(11)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = e;
        false
    }
}

/// Soft I/O errors that should yield a per-entry skip rather than abort the
/// backup. Used both at raw-`io::Error` call sites (`readlink`, `read_dir`,
/// `File::open`, `fstat`, `read_to_end`) and as the `VykarError::Io` arm of
/// [`VykarError::is_soft_file_error`].
///
/// macOS-specific case: `EDEADLK` raised by a `FileProvider` inode that cannot
/// be materialized from this process context — see [`is_fileprovider_deadlock`].
///
/// Windows-specific cases:
/// - **Synthetic `read_link` error from std** for unsupported reparse tags
///   (`OneDrive` Cloud Files, `AppExecLink`, dedup, HSM, WSL volume). std emits
///   `io::Error::new(ErrorKind::Uncategorized, "Unsupported reparse point type")`
///   with no `raw_os_error`; `Uncategorized` is unstable on stable Rust so we
///   match by message. If std rewords it, behavior reverts to today's "abort
///   backup" — safe failure mode.
/// - **`ERROR_CANT_ACCESS_FILE` and `ERROR_CLOUD_FILE_*` raw codes** that
///   surface from `File::open` / `read` / `fstat` on cloud placeholders when
///   the provider can't materialize content. List intentionally narrow;
///   verified against winerror.h, expand only on concrete user reports.
/// - **`ERROR_SHARING_VIOLATION` (32) and `ERROR_LOCK_VIOLATION` (33)** for
///   files locked by another process — pervasive in a user folder (NTUSER.DAT,
///   browser/Outlook DBs, app lock files):
///   - `32  ERROR_SHARING_VIOLATION` — file held open by another process
///   - `33  ERROR_LOCK_VIOLATION` — byte-range lock held (LockFileEx)
pub fn is_soft_backup_io_error(e: &std::io::Error) -> bool {
    if matches!(
        e.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound,
    ) || is_eio(e)
        || is_fileprovider_deadlock(e)
    {
        return true;
    }
    #[cfg(windows)]
    {
        // (a) synthetic readlink error — no raw_os_error, message-matched.
        if e.raw_os_error().is_none() && e.to_string().contains("Unsupported reparse point type") {
            return true;
        }
        // (b) cloud-file / can't-access and sharing/lock codes from winerror.h:
        //     1920 ERROR_CANT_ACCESS_FILE
        //     362  ERROR_CLOUD_FILE_PROVIDER_NOT_RUNNING
        //     395  ERROR_CLOUD_FILE_ACCESS_DENIED
        //     404  ERROR_CLOUD_FILE_PROVIDER_TERMINATED
        //     32   ERROR_SHARING_VIOLATION (file held open by another process)
        //     33   ERROR_LOCK_VIOLATION    (byte-range lock held, LockFileEx)
        if matches!(
            e.raw_os_error(),
            Some(1920) | Some(362) | Some(395) | Some(404) | Some(32) | Some(33)
        ) {
            return true;
        }
    }
    false
}

impl VykarError {
    /// Whether this is, or wraps (`RestoreSourceUnreadable`), a
    /// [`VykarError::ChunkRemovedDuringRead`]: a concurrent prune and compact
    /// removed what a reader planned, not damage.
    pub fn is_removed_during_read(&self) -> bool {
        match self {
            VykarError::ChunkRemovedDuringRead { .. } => true,
            VykarError::RestoreSourceUnreadable(inner) => inner.is_removed_during_read(),
            _ => false,
        }
    }

    /// Returns `true` for I/O errors that indicate a file was unreadable
    /// (permission denied, file vanished, EIO, or a Windows-specific
    /// unsupported-reparse / cloud-file / locked-file (sharing or lock
    /// violation) failure) **before** any data was committed. These are safe to
    /// skip for partial-backup support.
    ///
    /// Only for errors reading a backup's source files. Commit-path errors
    /// (storing a chunk, a pack upload, the index or snapshot writes) can carry
    /// the same I/O kinds and must stay fatal: never apply this
    /// classification to them. A write session whose pack upload failed
    /// refuses every further store and save regardless.
    pub fn is_soft_file_error(&self) -> bool {
        match self {
            VykarError::Io(e) | VykarError::IoAt { source: e, .. } => is_soft_backup_io_error(e),
            VykarError::FileChangedDuringRead { .. } => true,
            _ => false,
        }
    }

    /// The I/O error inside `Io` or `IoAt`.
    pub fn io_error(&self) -> Option<&std::io::Error> {
        match self {
            Self::Io(e) | Self::IoAt { source: e, .. } => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holder(key: &str, details: bool) -> String {
        LockHolder {
            key: key.to_string(),
            details: details.then(|| LockHolderDetails {
                hostname: "host-a".into(),
                pid: 42,
                age: "3m".into(),
            }),
        }
        .to_string()
    }

    /// An exclusive session's marker: one line, naming the commands, what
    /// clears it and that `break-lock --sessions` removes every marker.
    #[test]
    fn exclusive_session_holder_message() {
        for details in [true, false] {
            let msg = holder("sessions/x-0123456789abcdef0123456789abcdef.json", details);
            assert!(!msg.contains('\n'), "{msg}");
            assert!(msg.contains("exclusive maintenance run"), "{msg}");
            assert!(msg.contains("check --repair"), "{msg}");
            assert!(msg.contains("vykar break-lock --sessions"), "{msg}");
            assert!(msg.contains("45 minutes"), "{msg}");
            assert!(msg.contains("every session marker"), "{msg}");
        }
        let msg = holder("sessions/x-0123456789abcdef0123456789abcdef.json", true);
        assert!(msg.contains("host-a (PID 42, started 3m ago)"), "{msg}");
    }

    /// A shared session's marker keeps its text.
    #[test]
    fn shared_session_holder_message_is_unchanged() {
        let msg = holder("sessions/0123456789abcdef0123456789abcdef.json", true);
        assert!(
            msg.starts_with("a backup committing or a compact running on host-a"),
            "{msg}"
        );
        assert!(!msg.contains("exclusive"), "{msg}");
        let msg = holder("sessions/0123456789abcdef0123456789abcdef.json", false);
        assert!(
            msg.starts_with("a session marker that may belong to a committing backup"),
            "{msg}"
        );
    }

    /// One line (the cycle summary), naming the command and the way out; a
    /// write refusal like the storage errors it stands in for.
    #[test]
    fn append_only_server_message() {
        let e = VykarError::AppendOnlyServer {
            command: "vykar compact",
        };
        let msg = e.to_string();
        assert!(!msg.contains('\n'), "{msg}");
        assert!(msg.starts_with("vykar compact deletes data"), "{msg}");
        assert!(msg.contains("maintenance window"), "{msg}");
        assert!(e.is_write_refusal());
    }

    /// One line, naming `vykar check` for the case it was real damage.
    #[test]
    fn chunk_removed_during_read_message() {
        let e = VykarError::ChunkRemovedDuringRead {
            chunk_id: crate::chunk_id::ChunkId::from_bytes([7; 32]),
        };
        let msg = e.to_string();
        assert!(!msg.contains('\n'), "{msg}");
        assert!(
            msg.contains("removed from the repository while it was being read"),
            "{msg}"
        );
        assert!(msg.contains("`vykar check`"), "{msg}");
    }

    #[test]
    fn soft_io_error_permission_denied() {
        let e = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        assert!(is_soft_backup_io_error(&e));
    }

    #[test]
    fn soft_io_error_not_found() {
        let e = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        assert!(is_soft_backup_io_error(&e));
    }

    #[test]
    #[cfg(unix)]
    fn soft_io_error_eio() {
        let e = std::io::Error::from_raw_os_error(5);
        assert!(is_soft_backup_io_error(&e));
    }

    /// macOS: `EDEADLK` from a FileProvider inode that cannot be materialized
    /// in this process context (background launchd backup) must be soft so the
    /// walk skips the entry instead of aborting the whole backup (issue #183).
    #[test]
    #[cfg(target_os = "macos")]
    fn soft_io_error_fileprovider_deadlock_on_macos() {
        let e = std::io::Error::from_raw_os_error(11);
        assert!(is_fileprovider_deadlock(&e));
        assert!(is_soft_backup_io_error(&e));
    }

    /// Off macOS, errno 11 is never the FileProvider case — it is
    /// `EAGAIN`/`EWOULDBLOCK` on Linux and a genuine fcntl/flock deadlock on the
    /// BSDs — so it must stay hard; masking it would hide real bugs.
    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn deadlock_errno_is_not_soft_off_macos() {
        let e = std::io::Error::from_raw_os_error(11);
        assert!(!is_fileprovider_deadlock(&e));
        assert!(!is_soft_backup_io_error(&e));
    }

    #[test]
    fn non_soft_io_error() {
        let e = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        assert!(!is_soft_backup_io_error(&e));
    }

    #[test]
    #[cfg(windows)]
    fn soft_io_error_unsupported_reparse_point_message() {
        // std emits this synthetic error for unsupported reparse tags
        // (OneDrive Cloud Files, AppExecLink, dedup, HSM) with no raw_os_error.
        let e = std::io::Error::new(std::io::ErrorKind::Other, "Unsupported reparse point type");
        assert!(e.raw_os_error().is_none());
        assert!(is_soft_backup_io_error(&e));
    }

    #[test]
    #[cfg(windows)]
    fn soft_io_error_cant_access_file() {
        // ERROR_CANT_ACCESS_FILE — cloud placeholder couldn't be materialized.
        let e = std::io::Error::from_raw_os_error(1920);
        assert!(is_soft_backup_io_error(&e));
    }

    #[test]
    #[cfg(windows)]
    fn soft_io_error_cloud_file_provider_not_running() {
        let e = std::io::Error::from_raw_os_error(362);
        assert!(is_soft_backup_io_error(&e));
    }

    #[test]
    #[cfg(windows)]
    fn soft_io_error_cloud_file_access_denied() {
        let e = std::io::Error::from_raw_os_error(395);
        assert!(is_soft_backup_io_error(&e));
    }

    #[test]
    #[cfg(windows)]
    fn soft_io_error_cloud_file_provider_terminated() {
        let e = std::io::Error::from_raw_os_error(404);
        assert!(is_soft_backup_io_error(&e));
    }

    #[test]
    #[cfg(windows)]
    fn soft_io_error_sharing_violation() {
        // ERROR_SHARING_VIOLATION — file held open by another process.
        let e = std::io::Error::from_raw_os_error(32);
        assert!(is_soft_backup_io_error(&e));
    }

    #[test]
    #[cfg(windows)]
    fn soft_io_error_lock_violation() {
        // ERROR_LOCK_VIOLATION — byte-range lock held (LockFileEx).
        let e = std::io::Error::from_raw_os_error(33);
        assert!(is_soft_backup_io_error(&e));
    }

    #[test]
    #[cfg(windows)]
    fn non_soft_io_error_outside_windows_allowlist() {
        // 361 is just outside the codes we recognise; treat as hard.
        let e = std::io::Error::from_raw_os_error(361);
        assert!(!is_soft_backup_io_error(&e));
    }

    #[test]
    fn is_soft_file_error_delegates_to_io_classifier() {
        let err = VykarError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not found",
        ));
        assert!(err.is_soft_file_error());

        let err = VykarError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        assert!(!err.is_soft_file_error());
    }

    #[test]
    fn other_variant_is_not_soft() {
        let err = VykarError::Other("some error".to_string());
        assert!(!err.is_soft_file_error());
    }

    #[test]
    fn is_soft_file_error_file_changed_during_read() {
        let err = VykarError::FileChangedDuringRead {
            path: "/tmp/some/file".to_string(),
            dataless: false,
        };
        assert!(err.is_soft_file_error());
    }

    #[test]
    fn file_changed_during_read_appends_dataless_hint() {
        let err = VykarError::FileChangedDuringRead {
            path: "/tmp/icloud-doc".to_string(),
            dataless: true,
        };
        let msg = err.to_string();
        assert!(msg.contains("/tmp/icloud-doc"), "msg: {msg}");
        assert!(msg.contains("cloud-only file"), "msg: {msg}");
    }

    /// Each typed variant prints what the `Other` it replaced printed, on
    /// one line.
    #[test]
    fn typed_variants_keep_their_texts() {
        let p = crate::pack_id::PackId::from_bytes([0x07; 32]);
        let c = crate::chunk_id::ChunkId::from_bytes([0x09; 32]);
        let cases = [
            (VykarError::PackNotFound(p), format!("pack not found: {p}")),
            (
                VykarError::PackShortRead {
                    pack_id: p,
                    offset: 7,
                    expected: 100,
                    actual: 99,
                },
                format!("short read from pack {p} at offset 7: expected 100 bytes, got 99"),
            ),
            (
                VykarError::IndexChanged,
                "the index changed on storage after it was loaded; retry the operation".into(),
            ),
            (
                VykarError::IndexHeadContention {
                    detail: "index head 5 was created by another writer on each of 3 attempts; \
                             retry the backup"
                        .into(),
                },
                "the index changed on storage after it was loaded: index head 5 was created \
                 by another writer on each of 3 attempts; retry the backup"
                    .into(),
            ),
            (
                VykarError::MarkRefused(Box::new(VykarError::Other("x".into()))),
                "compact refused: the live snapshots could not be marked, nothing was changed: \
                 x. Run `vykar check` for details"
                    .into(),
            ),
            (
                VykarError::LiveChunkNotInIndex {
                    snapshot: "s".into(),
                    chunk: c,
                    tree: true,
                },
                format!("snapshot 's' names tree chunk {c}, which is not in the index"),
            ),
            (
                VykarError::LiveChunkNotInIndex {
                    snapshot: "s".into(),
                    chunk: c,
                    tree: false,
                },
                format!("snapshot 's' references chunk {c}, which is not in the index"),
            ),
            (
                VykarError::MarkRefused(Box::new(VykarError::LiveChunkNotInIndex {
                    snapshot: "s".into(),
                    chunk: c,
                    tree: true,
                })),
                format!(
                    "compact refused: the live snapshots could not be marked, nothing was \
                     changed: snapshot 's' names tree chunk {c}, which is not in the index. \
                     The index lost entries a live snapshot needs; run `vykar check --repair`, \
                     which re-indexes them from the packs that hold them"
                ),
            ),
            (
                VykarError::SlowIndexWrite {
                    limit_mins: 680,
                    head_written: false,
                },
                "the index write took longer than 11 h 20 min since its first object upload; \
                 retry"
                    .into(),
            ),
            (
                VykarError::SlowIndexWrite {
                    limit_mins: 680,
                    head_written: true,
                },
                "the index write took longer than 11 h 20 min since its first object upload; \
                 retry. The index head was written without its snapshot; the next run \
                 continues from it. Run `vykar check` if the repository fails to open"
                    .into(),
            ),
        ];
        for (e, want) in cases {
            let got = e.to_string();
            assert_eq!(got, want);
            assert!(!got.contains('\n'), "{got}");
        }
    }

    #[test]
    fn io_context_keeps_the_io_error() {
        let r: std::io::Result<()> = Err(std::io::Error::from_raw_os_error(5));
        let e = r.ctx("/x").unwrap_err();
        assert!(
            matches!(&e, VykarError::IoAt { path, .. } if path == "/x"),
            "{e:?}"
        );
        assert!(e.to_string().starts_with("I/O error at /x: "), "{e}");
        #[cfg(unix)]
        {
            assert_eq!(e.io_error().and_then(|io| io.raw_os_error()), Some(5));
            assert!(e.is_soft_file_error());
        }

        let r: std::io::Result<()> =
            Err(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        let e = r.ctx("/y").unwrap_err();
        assert!(e.io_error().is_some());
        assert!(!e.is_soft_file_error());
    }

    #[test]
    fn file_changed_during_read_omits_hint_when_not_dataless() {
        let err = VykarError::FileChangedDuringRead {
            path: "/tmp/regular".to_string(),
            dataless: false,
        };
        let msg = err.to_string();
        assert!(!msg.contains("cloud-only"), "msg: {msg}");
    }
}
