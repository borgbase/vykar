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
/// markers are preserved on storage so maintenance fails closed, and they
/// require operator intervention (`vykar break-lock --sessions`) to remove.
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

#[derive(Debug, Error)]
pub enum VykarError {
    #[error("storage I/O error: {0}")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("repository not found at '{0}'")]
    RepoNotFound(String),

    #[error("repository already exists at '{0}'")]
    RepoAlreadyExists(String),

    #[error("decryption failed: wrong passphrase or corrupted data")]
    DecryptionFailed,

    #[error("key derivation error: {0}")]
    KeyDerivation(String),

    #[error("snapshot not found: '{0}'")]
    SnapshotNotFound(String),

    #[error("snapshot already exists: '{0}'")]
    SnapshotAlreadyExists(String),

    #[error("invalid repository format: {0}")]
    InvalidFormat(String),

    #[error("unknown object type tag: {0}")]
    UnknownObjectType(u8),

    #[error("unknown compression tag: {0}")]
    UnknownCompressionTag(u8),

    #[error("unsupported repository version: {0}")]
    UnsupportedVersion(u32),

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

    #[error("repository is locked by another process (lock: {0})")]
    Locked(String),

    #[error("chunk not found in index: {0}")]
    ChunkNotInIndex(crate::chunk_id::ChunkId),

    #[error("decompression error: {0}")]
    Decompression(String),

    #[error("hook error: {0}")]
    Hook(String),

    #[error("operation interrupted by signal")]
    Interrupted,

    #[error("active sessions prevent maintenance: {0}")]
    ActiveSessions(ActiveSessionList),

    #[error("commit failed: referenced chunks were deleted since session started")]
    StaleChunksDuringCommit,

    #[error("lock expired: {0}")]
    LockExpired(String),

    #[error("repository identity mismatch: {0}")]
    RepositoryMismatch(String),

    #[error("file changed during read: {path}{}", if *dataless { " (cloud-only file, hydration in progress)" } else { "" })]
    FileChangedDuringRead {
        path: String,
        /// macOS hint: the file was reported dataless on the post-read fstat,
        /// suggesting hydration is in progress. Surfaced in the message so
        /// the user can recognize the iCloud Drive / Dropbox / OneDrive case.
        ///
        /// Kept structural (rather than inlined into `path`) so consumers
        /// that round-trip, prefix-strip, or render `path` as a clickable
        /// link see a clean path; the `Display` impl does the formatting.
        dataless: bool,
    },

    #[error("{0}")]
    Other(String),
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

impl VykarError {
    /// Returns `true` for I/O errors that indicate a file was unreadable
    /// (permission denied, file vanished, or EIO) **before** any data was
    /// committed. These are safe to skip for partial-backup support.
    pub fn is_soft_file_error(&self) -> bool {
        match self {
            VykarError::Io(e) => {
                matches!(
                    e.kind(),
                    std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
                ) || is_eio(e)
            }
            VykarError::FileChangedDuringRead { .. } => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_soft_file_error_permission_denied() {
        let err = VykarError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "permission denied",
        ));
        assert!(err.is_soft_file_error());
    }

    #[test]
    fn is_soft_file_error_not_found() {
        let err = VykarError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not found",
        ));
        assert!(err.is_soft_file_error());
    }

    #[test]
    #[cfg(unix)]
    fn is_soft_file_error_eio() {
        let err = VykarError::Io(std::io::Error::from_raw_os_error(5));
        assert!(err.is_soft_file_error());
    }

    #[test]
    fn is_soft_file_error_other_io_is_not_soft() {
        let err = VykarError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
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
