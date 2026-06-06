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

use self::file_cache::FileCache;
use self::manifest::Manifest;

/// Persisted (unencrypted) at the `config` key.
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
    chunk_index: ChunkIndex,
    pub config: RepoConfig,
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

    // ----- Accessors for private fields -----

    /// Current index generation (cache-validity token).
    pub fn index_generation(&self) -> u64 {
        self.index_generation
    }

    /// Read-only access to the manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
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
