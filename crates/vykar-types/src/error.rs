use thiserror::Error;

pub type Result<T> = std::result::Result<T, VykarError>;

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

    #[error("active backup sessions prevent maintenance: {0:?}")]
    ActiveSessions(Vec<String>),

    #[error("commit failed: referenced chunks were deleted since session started")]
    StaleChunksDuringCommit,

    #[error("{0}")]
    Other(String),
}

impl VykarError {
    /// Returns `true` for I/O errors that indicate a file was unreadable
    /// (permission denied or file vanished) **before** any data was committed.
    /// These are safe to skip for partial-backup support.
    pub fn is_soft_file_error(&self) -> bool {
        matches!(self, VykarError::Io(e)
            if matches!(e.kind(),
                std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound))
    }
}
