pub mod file_cache;
pub mod format;
pub mod identity;
pub mod lock;
pub mod manifest;
pub mod pack;
pub mod snapshot_cache;
pub(crate) mod write_session;

mod chunks;
mod commit;
mod open;

#[cfg(test)]
pub(crate) use open::PLAINTEXT_CHUNK_ID_KEY_CONTEXT;
mod read;
mod session;

use std::collections::{HashMap as StdHashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use self::write_session::WriteSessionState;
use crate::config::{default_max_pack_size, default_min_pack_size, ChunkerConfig};
use crate::index::ChunkIndex;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;
use vykar_types::hash::HashAlgorithm;

use self::file_cache::FileCache;
use self::manifest::Manifest;

/// The repository layout version, parsed once from `RepoConfig::version`.
///
/// **The version implies the chunk/pack hash algorithm; there is no stored
/// `hash` field.** `RepoConfig` is serialized as a *positional* msgpack array,
/// and rmp-serde errors on an array longer than the struct
/// (`array had incorrect length`) — `#[serde(default)]` only rescues shorter
/// ones. An appended field would therefore make an old binary fail to decode
/// the config *before* it reached the version gate, replacing a clear
/// "unsupported repository version: 3" with a corruption-looking parse error.
/// `repo_config_is_exactly_seven_msgpack_elements` enforces the field count.
///
/// This is not really a conflation of two concepts: `version` answers "what
/// must a reader understand to read this repository?", and a reader computing
/// BLAKE2b chunk IDs *cannot* read a BLAKE3 repository — the storage keys and
/// the entire dedup index differ. Deferring an explicit `hash` field is also
/// free: whenever a future algorithm forces a v4 bump, the field can be
/// appended then, because a bumped version is already unreadable by old
/// binaries.
///
/// Only chunk IDs and pack IDs follow the format. Every other digest in the
/// workspace stays BLAKE2b for all versions — see the "Repository Versions"
/// subsection of `docs/src/architecture.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoFormat {
    /// Keyed/unkeyed BLAKE2b-256 chunk and pack IDs.
    V2,
    /// Keyed/unkeyed BLAKE3 chunk and pack IDs.
    V3,
}

impl RepoFormat {
    /// The format `vykar init` creates. Existing repositories keep theirs.
    pub const CURRENT: Self = Self::V3;

    /// The single version gate: an unknown version is rejected here and never
    /// reaches hashing code.
    ///
    /// Deliberately not a `2 => Blake2b, _ => Blake3` mapping — that would
    /// silently hand BLAKE3 semantics to a future v4.
    ///
    /// # Errors
    ///
    /// Returns [`vykar_types::error::VykarError::UnsupportedVersion`] for any
    /// other version.
    pub fn from_version(version: u32) -> Result<Self> {
        match version {
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            other => Err(vykar_types::error::VykarError::UnsupportedVersion(other)),
        }
    }

    /// The integer written to `RepoConfig::version`.
    pub fn version(self) -> u32 {
        match self {
            Self::V2 => 2,
            Self::V3 => 3,
        }
    }

    /// The algorithm this format uses for chunk IDs and pack IDs.
    pub fn chunk_hash(self) -> HashAlgorithm {
        match self {
            Self::V2 => HashAlgorithm::Blake2b,
            Self::V3 => HashAlgorithm::Blake3,
        }
    }
}

/// Persisted (unencrypted) at the `config` key.
///
/// **Exactly seven fields, deliberately.** See [`RepoFormat`] for why nothing
/// may be appended.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    pub version: u32,
    pub id: Vec<u8>, // 32 bytes
    pub chunker_params: ChunkerConfig,
    pub encryption: EncryptionMode,
    pub created: DateTime<Utc>,
    #[serde(default = "default_min_pack_size")]
    pub min_pack_size: u32,
    #[serde(default = "default_max_pack_size")]
    pub max_pack_size: u32,
}

/// Maximum total weight (bytes) of cached blobs in the blob cache.
const BLOB_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024; // 32 MiB

const INDEX_OBJECT_CONTEXT: &[u8] = b"index";

/// FIFO blob cache bounded by total weight in bytes.
/// Caches decrypted+decompressed chunks to avoid redundant storage reads.
struct BlobCache {
    entries: StdHashMap<ChunkId, Vec<u8>>,
    order: VecDeque<ChunkId>,
    current_bytes: usize,
    max_bytes: usize,
}

impl BlobCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: StdHashMap::new(),
            order: VecDeque::new(),
            current_bytes: 0,
            max_bytes,
        }
    }

    fn get(&self, id: &ChunkId) -> Option<&[u8]> {
        self.entries.get(id).map(Vec::as_slice)
    }

    fn insert(&mut self, id: ChunkId, data: Vec<u8>) {
        let data_len = data.len();
        // Don't cache items larger than the entire cache
        if data_len > self.max_bytes {
            return;
        }
        // Evict oldest entries until there's room
        while self.current_bytes + data_len > self.max_bytes {
            if let Some(evicted_id) = self.order.pop_front() {
                if let Some(evicted_data) = self.entries.remove(&evicted_id) {
                    self.current_bytes -= evicted_data.len();
                }
            } else {
                break;
            }
        }
        self.current_bytes += data_len;
        self.entries.insert(id, data);
        self.order.push_back(id);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionMode {
    None,
    Aes256Gcm,
    Chacha20Poly1305,
}

impl EncryptionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            EncryptionMode::None => "none",
            EncryptionMode::Aes256Gcm => "aes256gcm",
            EncryptionMode::Chacha20Poly1305 => "chacha20poly1305",
        }
    }
}

/// Options controlling which expensive subsystems are loaded when opening a
/// repository. Both default to `false` (skip). Only `backup` needs both.
#[derive(Clone, Debug, Default)]
pub struct OpenOptions {
    pub load_index: bool,
    pub load_file_cache: bool,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_index(mut self) -> Self {
        self.load_index = true;
        self
    }
    pub fn with_file_cache(mut self) -> Self {
        self.load_file_cache = true;
        self
    }
}

/// A handle to an opened repository.
pub struct Repository {
    pub storage: Arc<dyn StorageBackend>,
    pub crypto: Arc<dyn CryptoEngine>,
    manifest: Manifest,
    /// Snapshots present under `snapshots/` that could not be read into the
    /// manifest. Non-empty means `manifest` is an incomplete view of the
    /// repository — see [`Repository::skipped_snapshots`].
    skipped_snapshots: Vec<snapshot_cache::SkippedSnapshot>,
    chunk_index: ChunkIndex,
    pub config: RepoConfig,
    /// Resolved once from `config.version` at init/open. Every downstream
    /// caller asks the repository rather than re-inspecting the integer.
    format: RepoFormat,
    file_cache: FileCache,
    /// Weight-bounded cache for decrypted chunks (used during restore).
    blob_cache: BlobCache,
    /// Cache-validity token for the local dedup/restore caches.
    /// A random u64 rotated each time the index is saved.
    /// Stored inside the encrypted `IndexBlob` (source of truth) and
    /// advisory `index.gen` sidecar. Read from `index.gen` on open;
    /// verified against the remote `IndexBlob` on write paths.
    index_generation: u64,
    /// Whether the chunk index has been modified since last persist.
    index_dirty: bool,
    /// Whether the file cache has been modified since last persist.
    file_cache_dirty: bool,
    /// Whether to rebuild the local dedup cache at save time.
    rebuild_dedup_cache: bool,
    /// Override for the cache directory root (from config `cache_dir`).
    cache_dir_override: Option<PathBuf>,
    /// Write-path state: pack writers, upload queue, dedup, journal, sizing.
    /// `None` when no write session is active (read-only operations, compact, delete, prune).
    /// Activated by `begin_write_session()` before backup.
    write_session: Option<WriteSessionState>,
    /// Lock fence: called before persisting index/manifest to verify the lock is still valid.
    lock_fence: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
}

impl Repository {
    /// Mark the chunk index as needing persistence on the next `save_state()`.
    pub fn mark_index_dirty(&mut self) {
        self.index_dirty = true;
    }

    /// Mark the file cache as needing persistence on the next `save_state()`.
    pub fn mark_file_cache_dirty(&mut self) {
        self.file_cache_dirty = true;
    }

    /// Save the file cache to local disk if it has been modified.
    /// Returns `Ok(())` if no save was needed or the save succeeded.
    pub fn save_file_cache_if_dirty(&mut self) -> Result<()> {
        if !self.file_cache_dirty {
            return Ok(());
        }
        self.file_cache.save(
            &self.config.id,
            self.crypto.as_ref(),
            self.cache_dir_override.as_deref(),
        )?;
        self.file_cache_dirty = false;
        Ok(())
    }

    /// The repository layout format, resolved at open time.
    pub fn format(&self) -> RepoFormat {
        self.format
    }

    /// The content-digest algorithm this repository's chunk IDs and pack IDs
    /// use. Implied by the format version and fixed for the repository's life.
    pub fn content_hash(&self) -> HashAlgorithm {
        self.format.chunk_hash()
    }

    // ----- Accessors for private fields -----

    /// Current index generation (cache-validity token).
    pub fn index_generation(&self) -> u64 {
        self.index_generation
    }

    /// Read-only access to the manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Snapshots that exist under `snapshots/` but could not be read into the
    /// manifest — most commonly because they were written by a newer vykar.
    ///
    /// A non-empty result means [`Repository::manifest`] is an **incomplete**
    /// view of the repository. Callers that show a snapshot list to a user must
    /// surface this rather than rendering a silently truncated list.
    pub fn skipped_snapshots(&self) -> &[snapshot_cache::SkippedSnapshot] {
        &self.skipped_snapshots
    }

    /// Mutable access to the manifest (in-memory only, never persisted to storage).
    pub fn manifest_mut(&mut self) -> &mut Manifest {
        &mut self.manifest
    }

    /// Read-only access to the chunk index.
    pub fn chunk_index(&self) -> &ChunkIndex {
        &self.chunk_index
    }

    /// Mutable access to the chunk index. Automatically marks it dirty.
    pub fn chunk_index_mut(&mut self) -> &mut ChunkIndex {
        self.index_dirty = true;
        &mut self.chunk_index
    }

    /// Replace the chunk index with an empty one (frees memory).
    /// Does not mark dirty — intended for memory optimization (e.g. restore).
    pub fn clear_chunk_index(&mut self) {
        self.chunk_index = ChunkIndex::new();
    }

    /// Read-only access to the file cache.
    pub fn file_cache(&self) -> &FileCache {
        &self.file_cache
    }

    /// Mutable access to the file cache (for invalidation, section setup).
    pub fn file_cache_mut(&mut self) -> &mut FileCache {
        &mut self.file_cache
    }

    /// Temporarily take the file cache out of the repository.
    /// Does not mark dirty — use `restore_file_cache` to put it back,
    /// or `set_file_cache` to replace it (which marks dirty).
    pub fn take_file_cache(&mut self) -> FileCache {
        std::mem::take(&mut self.file_cache)
    }

    /// Put a previously-taken file cache back without marking dirty.
    pub fn restore_file_cache(&mut self, cache: FileCache) {
        self.file_cache = cache;
    }

    /// Replace the file cache and mark it dirty.
    pub fn set_file_cache(&mut self, cache: FileCache) {
        self.file_cache = cache;
        self.file_cache_dirty = true;
    }

    /// Install a lock fence that will be checked before persisting index/manifest.
    pub fn set_lock_fence(&mut self, fence: Arc<dyn Fn() -> Result<()> + Send + Sync>) {
        self.lock_fence = Some(fence);
    }

    /// Remove the lock fence.
    pub fn clear_lock_fence(&mut self) {
        self.lock_fence = None;
    }

    /// Check the lock fence if one is installed. No-op if no fence is set.
    pub(crate) fn check_lock_fence(&self) -> Result<()> {
        if let Some(ref fence) = self.lock_fence {
            fence()?;
        }
        Ok(())
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        if self.write_session.is_some() {
            warn!("Repository dropped with active write session");
        }
    }
}

#[cfg(test)]
mod format_tests {
    use super::*;
    use vykar_types::error::VykarError;

    /// The load-bearing constraint behind "no stored `hash` field".
    ///
    /// `RepoConfig` is serialized as a positional msgpack array. rmp-serde
    /// errors on an array *longer* than the struct — `#[serde(default)]` only
    /// rescues shorter ones — so an appended field would make an old binary
    /// fail to decode the config before it reached the version gate, turning a
    /// clear "unsupported repository version" into a parse error that looks
    /// like corruption.
    ///
    /// `0x97` is msgpack fixarray with 7 elements. If this fails because a
    /// field was added, the fix is not to update the constant: it is to bump
    /// the repository version instead, which is what makes appending safe.
    #[test]
    fn repo_config_is_exactly_seven_msgpack_elements() {
        let config = RepoConfig {
            version: RepoFormat::CURRENT.version(),
            id: vec![0u8; 32],
            chunker_params: ChunkerConfig::default(),
            encryption: EncryptionMode::None,
            created: chrono::Utc::now(),
            min_pack_size: default_min_pack_size(),
            max_pack_size: default_max_pack_size(),
        };
        let encoded = rmp_serde::to_vec(&config).expect("RepoConfig serializes");
        assert_eq!(
            encoded.first(),
            Some(&0x97),
            "RepoConfig must stay a 7-element msgpack array; \
             got a leading byte of {:#04x}",
            encoded.first().copied().unwrap_or(0)
        );
    }

    #[test]
    fn from_version_maps_known_versions() {
        assert_eq!(RepoFormat::from_version(2).unwrap(), RepoFormat::V2);
        assert_eq!(RepoFormat::from_version(3).unwrap(), RepoFormat::V3);
    }

    /// A future v4 must be refused, not silently handed v3 semantics.
    #[test]
    fn from_version_rejects_unknown_versions() {
        for version in [0u32, 1, 4, 5, u32::MAX] {
            match RepoFormat::from_version(version) {
                Err(VykarError::UnsupportedVersion(v)) => assert_eq!(v, version),
                other => panic!("version {version} should be unsupported, got {other:?}"),
            }
        }
    }

    #[test]
    fn version_round_trips() {
        for format in [RepoFormat::V2, RepoFormat::V3] {
            assert_eq!(RepoFormat::from_version(format.version()).unwrap(), format);
        }
    }

    #[test]
    fn format_selects_the_hash() {
        assert_eq!(RepoFormat::V2.chunk_hash(), HashAlgorithm::Blake2b);
        assert_eq!(RepoFormat::V3.chunk_hash(), HashAlgorithm::Blake3);
        assert_eq!(RepoFormat::CURRENT, RepoFormat::V3);
    }

    /// Pins the format-v3 plaintext chunk-ID key derivation. This value *is*
    /// the dedup identity of every unencrypted v3 repository, so changing it
    /// silently invalidates their indexes.
    #[test]
    fn plaintext_key_derivation_known_answer() {
        let derived = blake3::derive_key(PLAINTEXT_CHUNK_ID_KEY_CONTEXT, &[0x11u8; 32]);
        assert_eq!(
            hex::encode(derived),
            "947392f5ba41ab94ef39cc84b74bbb5a0c578bf7841b6b6cf76b17ad4a9e5b67"
        );
    }
}
