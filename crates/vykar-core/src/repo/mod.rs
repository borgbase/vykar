pub mod file_cache;
pub mod format;
pub mod lock;
pub mod manifest;
pub mod pack;
pub(crate) mod write_session;

use std::collections::{HashMap as StdHashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use tracing::{debug, warn};

use self::write_session::WriteSessionState;
use crate::compress;
use crate::config::{
    default_max_pack_size, default_min_pack_size, ChunkerConfig, RepositoryConfig,
    DEFAULT_UPLOAD_CONCURRENCY,
};
use crate::index::dedup_cache::{self, TieredDedupIndex};
use crate::index::{ChunkIndex, DedupIndex, IndexDelta, PendingChunkEntry};
use vykar_crypto::key::{EncryptedKey, MasterKey};
use vykar_crypto::{self as crypto, CryptoEngine, PlaintextEngine};
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use self::file_cache::FileCache;
use self::format::{
    pack_object_streaming_with_context, pack_object_with_context,
    unpack_object_expect_with_context, ObjectType,
};
use self::manifest::Manifest;
use self::pack::{
    compute_data_pack_target, compute_tree_pack_target, read_blob_from_pack, PackType, PackWriter,
    SealedPack,
};

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

const MANIFEST_OBJECT_CONTEXT: &[u8] = b"manifest";
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
    /// Whether the manifest has been modified since last persist.
    manifest_dirty: bool,
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
}

impl Repository {
    /// Initialize a new repository.
    pub fn init(
        storage: Box<dyn StorageBackend>,
        encryption: EncryptionMode,
        chunker_params: ChunkerConfig,
        passphrase: Option<&str>,
        repo_config_opts: Option<&RepositoryConfig>,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let storage: Arc<dyn StorageBackend> = Arc::from(storage);

        // Check that the repo doesn't already exist
        if storage.exists("config")? {
            return Err(VykarError::RepoAlreadyExists("repository".into()));
        }

        // Generate repo ID
        let mut rng = rand::thread_rng();
        let mut repo_id = vec![0u8; 32];
        rng.fill_bytes(&mut repo_id);

        let min_pack_size = repo_config_opts
            .map(|c| c.min_pack_size)
            .unwrap_or_else(default_min_pack_size);
        let max_pack_size = repo_config_opts
            .map(|c| c.max_pack_size)
            .unwrap_or_else(default_max_pack_size);

        if max_pack_size > 512 * 1024 * 1024 {
            return Err(VykarError::Config(format!(
                "max_pack_size ({max_pack_size}) exceeds hard limit of 512 MiB"
            )));
        }

        let repo_config = RepoConfig {
            version: 1,
            id: repo_id,
            chunker_params: chunker_params.clone(),
            encryption: encryption.clone(),
            created: Utc::now(),
            min_pack_size,
            max_pack_size,
        };

        // Generate master key and crypto engine
        let (crypto, encrypted_key): (Arc<dyn CryptoEngine>, Option<EncryptedKey>) =
            match &encryption {
                EncryptionMode::None => {
                    let mut chunk_id_key = [0u8; 32];
                    rng.fill_bytes(&mut chunk_id_key);
                    (Arc::new(PlaintextEngine::new(&chunk_id_key)), None)
                }
                EncryptionMode::Aes256Gcm => {
                    let master_key = MasterKey::generate();
                    let pass = passphrase.ok_or_else(|| {
                        VykarError::Config("passphrase required for encrypted repository".into())
                    })?;
                    let enc_key = master_key.to_encrypted(pass)?;
                    let engine = crypto::aes_gcm::Aes256GcmEngine::new(
                        &master_key.encryption_key,
                        &master_key.chunk_id_key,
                    );
                    (Arc::new(engine), Some(enc_key))
                }
                EncryptionMode::Chacha20Poly1305 => {
                    let master_key = MasterKey::generate();
                    let pass = passphrase.ok_or_else(|| {
                        VykarError::Config("passphrase required for encrypted repository".into())
                    })?;
                    let enc_key = master_key.to_encrypted(pass)?;
                    let engine = crypto::chacha20_poly1305::ChaCha20Poly1305Engine::new(
                        &master_key.encryption_key,
                        &master_key.chunk_id_key,
                    );
                    (Arc::new(engine), Some(enc_key))
                }
            };

        // Try server-side init (creates keys/, snapshots/, locks/, packs/* in one request).
        // Placed after all validation and crypto setup so a failure above doesn't
        // leave a partially scaffolded repo on the server.
        let server_did_init = match storage.server_init() {
            Ok(()) => true,
            Err(VykarError::UnsupportedBackend(_)) => false,
            Err(err) => return Err(err),
        };

        // Store config (unencrypted)
        let config_data = rmp_serde::to_vec(&repo_config)?;
        storage.put("config", &config_data)?;

        // Store encrypted key if applicable
        if let Some(enc_key) = &encrypted_key {
            if !server_did_init {
                storage.create_dir("keys/")?;
            }
            let key_data = rmp_serde::to_vec(enc_key)?;
            storage.put("keys/repokey", &key_data)?;
        }

        // Store empty manifest
        let manifest = Manifest::new();
        let manifest_bytes = rmp_serde::to_vec(&manifest)?;
        let manifest_packed = pack_object_with_context(
            ObjectType::Manifest,
            MANIFEST_OBJECT_CONTEXT,
            &manifest_bytes,
            crypto.as_ref(),
        )?;
        storage.put("manifest", &manifest_packed)?;

        // Store empty chunk index (compressed with ZSTD)
        let chunk_index = ChunkIndex::new();
        let index_packed = pack_object_streaming_with_context(
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            64, // empty index is tiny
            crypto.as_ref(),
            |buf| {
                compress::compress_stream_zstd(buf, 3, |encoder| {
                    rmp_serde::encode::write(encoder, &chunk_index)?;
                    Ok(())
                })
            },
        )?;
        storage.put("index", &index_packed)?;

        // Create directory structure (skip if server already did it)
        if !server_did_init {
            storage.create_dir("snapshots/")?;
            storage.create_dir("locks/")?;
            for i in 0u8..=255 {
                storage.create_dir(&format!("packs/{:02x}/", i))?;
            }
        }

        Ok(Repository {
            storage,
            crypto,
            manifest,
            chunk_index,
            config: repo_config,
            file_cache: FileCache::new(),
            blob_cache: BlobCache::new(BLOB_CACHE_MAX_BYTES),
            manifest_dirty: false,
            index_dirty: false,
            file_cache_dirty: false,
            rebuild_dedup_cache: false,
            cache_dir_override: cache_dir,
            write_session: None,
        })
    }

    /// Open an existing repository.
    pub fn open(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let mut repo = Self::open_base(storage, passphrase, cache_dir)?;
        repo.load_chunk_index()?;
        Ok(repo)
    }

    /// Open a repository without loading the chunk index.
    /// Suitable for read-only operations (restore, list) that either don't need
    /// the index or will load it lazily via `load_chunk_index()`.
    pub fn open_without_index(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self> {
        Self::open_base(storage, passphrase, cache_dir)
    }

    /// Open a repository without loading the chunk index or file cache.
    /// Suitable for read-only operations (e.g. restore) that need neither.
    pub fn open_without_index_or_cache(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self> {
        Self::open_base_inner(storage, passphrase, cache_dir, true)
    }

    /// Shared open logic: reads config, builds crypto, loads manifest and file cache.
    /// Does NOT load the chunk index — callers either load it themselves or skip it.
    fn open_base(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self> {
        Self::open_base_inner(storage, passphrase, cache_dir, false)
    }

    fn open_base_inner(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
        skip_file_cache: bool,
    ) -> Result<Self> {
        let storage: Arc<dyn StorageBackend> = Arc::from(storage);

        // Read config
        let config_data = storage
            .get("config")?
            .ok_or_else(|| VykarError::RepoNotFound("config not found".into()))?;
        let repo_config: RepoConfig = rmp_serde::from_slice(&config_data)?;

        if repo_config.version != 1 {
            return Err(VykarError::UnsupportedVersion(repo_config.version));
        }

        if repo_config.max_pack_size > 512 * 1024 * 1024 {
            return Err(VykarError::Config(format!(
                "max_pack_size ({}) exceeds hard limit of 512 MiB",
                repo_config.max_pack_size
            )));
        }

        // Build crypto engine
        let crypto: Arc<dyn CryptoEngine> = match &repo_config.encryption {
            EncryptionMode::None => {
                let mut chunk_id_key = [0u8; 32];
                use blake2::digest::{Update, VariableOutput};
                use blake2::Blake2bVar;
                let mut hasher = Blake2bVar::new(32).unwrap();
                hasher.update(&repo_config.id);
                hasher.finalize_variable(&mut chunk_id_key).unwrap();
                Arc::new(PlaintextEngine::new(&chunk_id_key))
            }
            EncryptionMode::Aes256Gcm => {
                let key_data = storage
                    .get("keys/repokey")?
                    .ok_or_else(|| VykarError::InvalidFormat("missing keys/repokey".into()))?;
                let enc_key: EncryptedKey = rmp_serde::from_slice(&key_data)?;
                let pass = passphrase.ok_or_else(|| {
                    VykarError::Config("passphrase required for encrypted repository".into())
                })?;
                let master_key = MasterKey::from_encrypted(&enc_key, pass)?;
                let engine = crypto::aes_gcm::Aes256GcmEngine::new(
                    &master_key.encryption_key,
                    &master_key.chunk_id_key,
                );
                Arc::new(engine)
            }
            EncryptionMode::Chacha20Poly1305 => {
                let key_data = storage
                    .get("keys/repokey")?
                    .ok_or_else(|| VykarError::InvalidFormat("missing keys/repokey".into()))?;
                let enc_key: EncryptedKey = rmp_serde::from_slice(&key_data)?;
                let pass = passphrase.ok_or_else(|| {
                    VykarError::Config("passphrase required for encrypted repository".into())
                })?;
                let master_key = MasterKey::from_encrypted(&enc_key, pass)?;
                let engine = crypto::chacha20_poly1305::ChaCha20Poly1305Engine::new(
                    &master_key.encryption_key,
                    &master_key.chunk_id_key,
                );
                Arc::new(engine)
            }
        };

        // Read manifest
        let manifest_data = storage
            .get("manifest")?
            .ok_or_else(|| VykarError::InvalidFormat("missing manifest".into()))?;
        let manifest_bytes = unpack_object_expect_with_context(
            &manifest_data,
            ObjectType::Manifest,
            MANIFEST_OBJECT_CONTEXT,
            crypto.as_ref(),
        )?;
        let manifest: Manifest = rmp_serde::from_slice(&manifest_bytes)?;

        // Load file cache from local disk (not from the repo).
        let file_cache = if skip_file_cache {
            FileCache::new()
        } else {
            FileCache::load(&repo_config.id, crypto.as_ref(), cache_dir.as_deref())
        };

        Ok(Repository {
            storage,
            crypto,
            manifest,
            chunk_index: ChunkIndex::new(),
            config: repo_config,
            file_cache,
            blob_cache: BlobCache::new(BLOB_CACHE_MAX_BYTES),
            manifest_dirty: false,
            index_dirty: false,
            file_cache_dirty: false,
            rebuild_dedup_cache: false,
            cache_dir_override: cache_dir,
            write_session: None,
        })
    }

    /// Load the chunk index from storage on demand (using local blob cache).
    /// Can be called after `open_without_index()` to lazily load the index.
    /// Also recalculates the data pack writer target from the loaded index.
    pub fn load_chunk_index(&mut self) -> Result<()> {
        self.chunk_index = self.reload_full_index_cached()?;
        self.rebase_pack_target_from_index();
        Ok(())
    }

    /// Load the chunk index from storage, bypassing the local blob cache.
    /// Use this for operations like `check` that must verify what's actually
    /// in the remote repository.
    pub fn load_chunk_index_uncached(&mut self) -> Result<()> {
        self.chunk_index = self.reload_full_index()?;
        self.rebase_pack_target_from_index();
        Ok(())
    }

    /// Activate a write session for backup.
    ///
    /// Creates a fresh `WriteSessionState` with pack targets computed from the
    /// current chunk index and repo config. Must be called before any write-path
    /// methods (`store_chunk`, `flush_packs`, dedup modes, etc.).
    ///
    /// Returns an error if a session is already active (caller must `save_state()`
    /// or `flush_on_abort()` first).
    pub fn begin_write_session(&mut self) -> Result<()> {
        if self.write_session.is_some() {
            return Err(VykarError::Other("write session already active".into()));
        }
        let num_packs = self.chunk_index.count_distinct_packs();
        let data_target = compute_data_pack_target(
            num_packs,
            self.config.min_pack_size,
            self.config.max_pack_size,
        );
        let tree_target = compute_tree_pack_target(self.config.min_pack_size);
        let mut ws = WriteSessionState::new(data_target, tree_target, DEFAULT_UPLOAD_CONCURRENCY);
        ws.persisted_pack_count = num_packs;
        self.write_session = Some(ws);
        Ok(())
    }

    /// Set the session ID on the active write session (for per-session pending_index).
    pub fn set_write_session_id(&mut self, session_id: String) {
        if let Some(ws) = self.write_session.as_mut() {
            ws.session_id = session_id;
        }
    }

    /// Recompute pack-target state from `self.chunk_index`.
    ///
    /// Sets `persisted_pack_count` from the index, resets `session_packs_flushed`,
    /// and updates the data pack writer's target size. Called after any operation
    /// that brings `chunk_index` in sync with persisted storage (load or save).
    /// No-op when no write session is active.
    fn rebase_pack_target_from_index(&mut self) {
        let Some(ws) = self.write_session.as_mut() else {
            return;
        };
        // NOTE: count_distinct_packs() includes tree packs, which slightly
        // inflates the data pack target. Tree packs are a small fraction of
        // total packs (~1-2 per backup) so the effect is negligible (~2% via
        // sqrt scaling). A proper fix would require persisting pack type
        // metadata in the index or manifest.
        let num_packs = self.chunk_index.count_distinct_packs();
        ws.persisted_pack_count = num_packs;
        ws.session_packs_flushed = 0;
        let data_target = compute_data_pack_target(
            num_packs,
            self.config.min_pack_size,
            self.config.max_pack_size,
        );
        ws.data_pack_writer.set_target_size(data_target);
    }

    /// Mark the manifest as needing persistence on the next `save_state()`.
    pub fn mark_manifest_dirty(&mut self) {
        self.manifest_dirty = true;
    }

    /// Mark the chunk index as needing persistence on the next `save_state()`.
    pub fn mark_index_dirty(&mut self) {
        self.index_dirty = true;
    }

    /// Mark the file cache as needing persistence on the next `save_state()`.
    pub fn mark_file_cache_dirty(&mut self) {
        self.file_cache_dirty = true;
    }

    /// Try to open the mmap'd restore cache for this repository.
    /// Returns `None` if the cache is missing, stale, or corrupt.
    pub fn open_restore_cache(&self) -> Option<dedup_cache::MmapRestoreCache> {
        dedup_cache::MmapRestoreCache::open(
            &self.config.id,
            self.manifest.index_generation,
            self.cache_dir_override.as_deref(),
        )
    }

    // ----- Accessors for private fields -----

    /// Read-only access to the manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Mutable access to the manifest. Automatically marks it dirty.
    pub fn manifest_mut(&mut self) -> &mut Manifest {
        self.manifest_dirty = true;
        &mut self.manifest
    }

    /// Read-only access to the chunk index.
    pub fn chunk_index(&self) -> &ChunkIndex {
        &self.chunk_index
    }

    /// Current data pack target size in bytes (for testing).
    #[cfg(test)]
    pub(crate) fn data_pack_target(&self) -> usize {
        self.write_session
            .as_ref()
            .expect("no active write session")
            .data_pack_writer
            .target_size()
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

    /// Filter the chunk index to only retain entries for the given chunks.
    /// Does not mark dirty — this is a read-only memory optimization.
    pub fn retain_chunk_index(
        &mut self,
        needed: &std::collections::HashSet<vykar_types::chunk_id::ChunkId>,
    ) {
        self.chunk_index.retain_chunks(needed);
    }

    /// Read-only access to the file cache.
    pub fn file_cache(&self) -> &FileCache {
        &self.file_cache
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

    /// Apply backpressure to keep the number of in-flight uploads bounded.
    fn cap_pending_uploads(&mut self) -> Result<()> {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .cap_pending_uploads(&*self.storage, &*self.crypto)
    }

    /// Set the maximum number of in-flight background pack uploads.
    pub fn set_max_in_flight_uploads(&mut self, n: usize) {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .max_in_flight_uploads = n.max(1);
    }

    /// Replace the blob cache with a new one of the given capacity.
    pub fn set_blob_cache_max_bytes(&mut self, max_bytes: usize) {
        self.blob_cache = BlobCache::new(max_bytes);
    }

    /// Switch to dedup-only index mode to reduce memory during backup.
    ///
    /// Builds a lightweight `DedupIndex` (chunk_id → stored_size only) from the
    /// full `ChunkIndex`, then drops the full index to reclaim memory. All
    /// mutations are recorded in an `IndexDelta` and merged back at save time.
    ///
    /// For 10M chunks this reduces steady-state memory from ~800 MB to ~450 MB.
    pub fn enable_dedup_mode(&mut self) {
        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        if ws.dedup_index.is_some() {
            return; // already enabled
        }
        let dedup = DedupIndex::from_chunk_index(&self.chunk_index);
        // Drop the full index to reclaim memory
        self.chunk_index = ChunkIndex::new();
        ws.dedup_index = Some(dedup);
        ws.index_delta = Some(IndexDelta::new());
    }

    /// Switch to tiered dedup mode for minimal memory usage during backup.
    ///
    /// Tries to open a local mmap'd dedup cache validated against the manifest's
    /// `index_generation`. On success: builds an xor filter, drops the full
    /// `ChunkIndex`, and routes all lookups through the three-tier structure
    /// (~12 MB RSS for 10M chunks instead of ~680 MB).
    ///
    /// On failure (no cache, stale generation, corrupt file): falls back to the
    /// existing `DedupIndex` HashMap path.
    pub fn enable_tiered_dedup_mode(&mut self) {
        {
            let ws = self
                .write_session
                .as_ref()
                .expect("no active write session");
            if ws.tiered_dedup.is_some() || ws.dedup_index.is_some() {
                return; // already in a dedup mode
            }
        }

        self.rebuild_dedup_cache = true;
        let generation = self.manifest.index_generation;
        if let Some(mmap_cache) = dedup_cache::MmapDedupCache::open(
            &self.config.id,
            generation,
            self.cache_dir_override.as_deref(),
        ) {
            let tiered = TieredDedupIndex::new(mmap_cache);
            debug!(?tiered, "tiered dedup mode: using mmap cache");
            // Drop the full index to reclaim memory.
            self.chunk_index = ChunkIndex::new();
            let ws = self.write_session.as_mut().unwrap();
            ws.tiered_dedup = Some(tiered);
            ws.index_delta = Some(IndexDelta::new());
        } else {
            debug!("tiered dedup mode: no valid cache, falling back to DedupIndex");
            self.enable_dedup_mode();
        }
    }

    /// Return the pre-built xor filter from whichever dedup mode is active.
    /// Returns `None` when no dedup mode or no write session is active.
    pub fn dedup_filter(&self) -> Option<std::sync::Arc<xorf::Xor8>> {
        let ws = self.write_session.as_ref()?;
        if let Some(ref tiered) = ws.tiered_dedup {
            return tiered.xor_filter();
        }
        if let Some(ref dedup) = ws.dedup_index {
            return dedup.xor_filter();
        }
        None
    }

    /// Check if a chunk exists in the index (works in normal, dedup, and tiered modes).
    /// Falls through to chunk_index when no write session is active.
    pub fn chunk_exists(&self, id: &ChunkId) -> bool {
        if let Some(ref ws) = self.write_session {
            if let Some(ref tiered) = ws.tiered_dedup {
                return tiered.contains(id);
            }
            if let Some(ref dedup) = ws.dedup_index {
                return dedup.contains(id);
            }
        }
        self.chunk_index.contains(id)
    }

    /// Increment the refcount for a chunk (works in both normal and dedup modes).
    pub fn increment_chunk_ref(&mut self, id: &ChunkId) {
        if let Some(ref mut ws) = self.write_session {
            if let Some(ref mut delta) = ws.index_delta {
                delta.bump_refcount(id);
                return;
            }
        }
        self.chunk_index.increment_refcount(id);
        self.index_dirty = true;
    }

    /// Flush pending packs, wait for uploads, and apply the dedup delta from
    /// the active write session. Must only be called when `write_session` is `Some`.
    ///
    /// On success, the session's dedup structures and delta are consumed
    /// (moved into the chunk index or cache). On failure, the session remains
    /// active (not consumed) so the caller can invoke `flush_on_abort()` for
    /// best-effort journal persistence. Note that `tiered_dedup` and
    /// `index_delta` may already be taken out of the session at that point —
    /// `flush_on_abort` does not need them (it only seals packs, joins uploads,
    /// and writes the journal).
    /// Returns `true` when chunk_index hydration should be deferred to after
    /// persistence (reduces peak memory when incremental update or cache
    /// rebuild succeeds).
    fn apply_write_session(&mut self) -> Result<bool> {
        // Flush all pending packs and wait for uploads.
        self.flush_packs()?;

        // Drop tiered dedup index (releases mmap) before reloading full index.
        // Take delta and dedup_index out of the session for processing.
        let ws = self.write_session.as_mut().unwrap();
        ws.tiered_dedup.take();
        let delta = ws.index_delta.take();
        if delta.is_some() {
            ws.dedup_index = None;
        }

        let mut deferred_index_load = false;

        if let Some(delta) = delta {
            if !delta.is_empty() {
                // Non-empty delta: try incremental update first
                let fast_ok = self
                    .try_incremental_index_update(&delta)
                    .unwrap_or_else(|e| {
                        warn!("incremental index update failed: {e}");
                        false
                    });

                if fast_ok {
                    // Fast path succeeded — index uploaded, caches rebuilt,
                    // manifest.index_generation and manifest_dirty already set.
                    // Defer chunk_index hydration to reduce peak memory.
                    self.rebuild_dedup_cache = false;
                    deferred_index_load = true;
                } else {
                    // Slow path: full HashMap (first run, stale cache, error)
                    let mut full_index = self.reload_full_index()?;
                    delta.apply_to(&mut full_index);
                    self.chunk_index = full_index;
                    self.index_dirty = true;
                }
            } else if self.rebuild_dedup_cache {
                // Empty delta: index unchanged, but caches may need rebuilding.
                // Try to rebuild caches from full_index_cache if available.
                let mut rebuilt_from_cache = false;
                let cd = self.cache_dir_override.as_deref();
                if let Some(cache_path) = dedup_cache::full_index_cache_path(&self.config.id, cd) {
                    if dedup_cache::MmapFullIndexCache::open(
                        &self.config.id,
                        self.manifest.index_generation,
                        cd,
                    )
                    .is_some()
                    {
                        let gen = self.manifest.index_generation;
                        let id = &self.config.id;
                        let dedup_ok = dedup_cache::build_dedup_cache_from_full_cache(
                            &cache_path,
                            gen,
                            id,
                            cd,
                        )
                        .is_ok();
                        let restore_ok = dedup_cache::build_restore_cache_from_full_cache(
                            &cache_path,
                            gen,
                            id,
                            cd,
                        )
                        .is_ok();
                        if dedup_ok && restore_ok {
                            self.rebuild_dedup_cache = false;
                            rebuilt_from_cache = true;
                            // Defer chunk_index hydration to reduce peak memory.
                            deferred_index_load = true;
                        } else {
                            warn!(
                                "cache rebuild from full_index_cache partially failed, falling back"
                            );
                        }
                    }
                }
                if !rebuilt_from_cache {
                    // No valid full cache — must reload full index for slow-path cache rebuild.
                    self.chunk_index = self.reload_full_index()?;
                }
            } else {
                // Empty delta, no cache rebuild needed.
                // Still must restore chunk_index for postcondition (deferred).
                deferred_index_load = true;
            }
        }

        Ok(deferred_index_load)
    }

    /// Save the manifest and chunk index back to storage.
    /// When a write session is active: flushes pending packs, applies dedup delta.
    /// When no session is active: just persists dirty manifest/index/file_cache.
    /// Only writes components that have been marked dirty.
    pub fn save_state(&mut self) -> Result<()> {
        let deferred_index_load = if self.write_session.is_some() {
            self.apply_write_session()?
        } else {
            false
        };

        // When the index changes, rotate index_generation so the local dedup
        // cache is invalidated.  Must happen before the manifest write below.
        if self.index_dirty {
            self.manifest.index_generation = rand::thread_rng().next_u64();
            self.manifest_dirty = true;
        }

        // Index-first persistence: readers never see manifest referencing
        // missing data. Crash between index and manifest write leaves
        // harmless orphan entries in the index.
        if self.index_dirty {
            self.persist_index()?;
        }

        if self.manifest_dirty {
            self.persist_manifest()?;
        }

        // Rebuild the local dedup cache for next backup so tiered mode can
        // activate. Written on first backup (bootstrap) and every subsequent
        // backup that used tiered/dedup mode. Non-fatal on error.
        if self.rebuild_dedup_cache {
            let cd = self.cache_dir_override.as_deref();
            if let Err(e) = dedup_cache::build_dedup_cache(
                &self.chunk_index,
                self.manifest.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to rebuild dedup cache: {e}");
            }
            if let Err(e) = dedup_cache::build_restore_cache(
                &self.chunk_index,
                self.manifest.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to rebuild restore cache: {e}");
            }
            // Also build the full index cache for next incremental update.
            if let Err(e) = dedup_cache::build_full_index_cache(
                &self.chunk_index,
                self.manifest.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to build full index cache: {e}");
            }
            self.rebuild_dedup_cache = false;
        }

        // Save file cache before hydrating chunk_index to reduce peak memory.
        // Capture error instead of early-returning so we can hydrate first.
        let fc_result = if self.file_cache_dirty {
            match self.file_cache.save(
                &self.config.id,
                self.crypto.as_ref(),
                self.cache_dir_override.as_deref(),
            ) {
                Ok(()) => {
                    self.file_cache_dirty = false;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(())
        };

        // Always hydrate chunk_index — postcondition: self.chunk_index is valid
        // on all exit paths (success and error).
        if deferred_index_load {
            // Try local full_index_cache first (fast, no storage round-trip),
            // fall back to reloading from remote storage if cache is unavailable.
            self.chunk_index = dedup_cache::load_chunk_index_from_full_cache(
                &self.config.id,
                self.manifest.index_generation,
                self.cache_dir_override.as_deref(),
            )
            .or_else(|_| self.reload_full_index())?;
        }

        // Now propagate any file cache save error
        fc_result?;

        // Consume the write session on success — all entries are now in the
        // persisted index. The pending_index file itself is deleted later by
        // clear_pending_index() from the backup command.
        self.write_session = None;

        Ok(())
    }

    /// Commit a concurrent backup session. Called while holding the exclusive lock.
    ///
    /// 1. Flush packs and join uploads.
    /// 2. Take the delta from the write session.
    /// 3. Reload fresh manifest from storage.
    /// 4. Check snapshot name uniqueness against the fresh manifest.
    /// 5. Verify all new_entries pack_ids exist on storage.
    /// 6. Fast/slow path for index update based on generation match.
    /// 7. Persist index (first) then manifest (second).
    /// 8. Save file cache and consume write session.
    pub fn commit_concurrent_session(
        &mut self,
        snapshot_entry: manifest::SnapshotEntry,
        new_file_cache: file_cache::FileCache,
    ) -> Result<()> {
        // 1. Flush all pending packs and wait for uploads.
        self.flush_packs()?;

        // 2. Drop tiered dedup, take delta from write session.
        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        ws.tiered_dedup.take();
        let delta = ws.index_delta.take();
        if delta.is_some() {
            ws.dedup_index = None;
        }

        // 3. Record T0 generation and reload fresh manifest.
        let t0_generation = self.manifest.index_generation;
        self.reload_manifest()?;

        // 4. Check snapshot name uniqueness against fresh manifest.
        if self.manifest.find_snapshot(&snapshot_entry.name).is_some() {
            return Err(VykarError::SnapshotAlreadyExists(
                snapshot_entry.name.clone(),
            ));
        }

        if let Some(delta) = delta {
            if !delta.is_empty() {
                if t0_generation == self.manifest.index_generation {
                    // FAST PATH: No concurrent commits happened.
                    // Verify packs before applying (no reconciliation needed).
                    self.verify_delta_packs(&delta)?;

                    let fast_ok = self
                        .try_incremental_index_update(&delta)
                        .unwrap_or_else(|e| {
                            warn!("incremental index update failed during concurrent commit: {e}");
                            false
                        });

                    if fast_ok {
                        // Index uploaded, caches rebuilt, manifest.index_generation set.
                        self.rebuild_dedup_cache = false;
                    } else {
                        // Fall through to slow path.
                        let mut full_index = self.reload_full_index()?;
                        delta.apply_to(&mut full_index);
                        self.chunk_index = full_index;
                        self.index_dirty = true;
                        self.manifest.index_generation = rand::thread_rng().next_u64();
                        self.persist_index()?;
                    }
                } else {
                    // SLOW PATH: Another client committed since T0. Must reconcile.
                    // Reconcile first — some new_entries may become refcount bumps
                    // and no longer require their original packs.
                    let fresh_index = self.reload_full_index()?;
                    let reconciled = delta.reconcile(&fresh_index)?;
                    // Verify packs only for entries that remain after reconciliation.
                    self.verify_delta_packs(&reconciled)?;
                    let mut fresh_index = fresh_index;
                    reconciled.apply_to(&mut fresh_index);
                    self.chunk_index = fresh_index;
                    self.index_dirty = true;
                    self.manifest.index_generation = rand::thread_rng().next_u64();
                    self.persist_index()?;
                }
            }
        }

        // Add snapshot entry to manifest and persist.
        self.manifest.timestamp = Utc::now();
        self.manifest.snapshots.push(snapshot_entry);
        self.manifest_dirty = true;
        self.persist_manifest()?;

        // Save file cache.
        self.file_cache = new_file_cache;
        self.file_cache_dirty = true;
        if let Err(e) = self.file_cache.save(
            &self.config.id,
            self.crypto.as_ref(),
            self.cache_dir_override.as_deref(),
        ) {
            warn!("failed to save file cache after concurrent commit: {e}");
        } else {
            self.file_cache_dirty = false;
        }

        // Rebuild dedup caches if needed.
        if self.rebuild_dedup_cache {
            let cd = self.cache_dir_override.as_deref();
            if let Err(e) = dedup_cache::build_dedup_cache(
                &self.chunk_index,
                self.manifest.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to rebuild dedup cache: {e}");
            }
            if let Err(e) = dedup_cache::build_restore_cache(
                &self.chunk_index,
                self.manifest.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to rebuild restore cache: {e}");
            }
            if let Err(e) = dedup_cache::build_full_index_cache(
                &self.chunk_index,
                self.manifest.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to build full index cache: {e}");
            }
            self.rebuild_dedup_cache = false;
        }

        // Clean up recovered index journals from previous interrupted sessions.
        if let Some(ws) = self.write_session.as_mut() {
            ws.cleanup_recovered_indices(&*self.storage);
        }

        // Consume write session.
        self.write_session = None;

        Ok(())
    }

    /// Verify that all pack_ids referenced by new_entries in a delta actually
    /// exist on storage. Returns an error if any are missing.
    fn verify_delta_packs(&self, delta: &IndexDelta) -> Result<()> {
        let pack_ids: std::collections::HashSet<PackId> =
            delta.new_entries.iter().map(|e| e.pack_id).collect();
        for pack_id in &pack_ids {
            let key = pack_id.storage_key();
            if !self.storage.exists(&key)? {
                return Err(VykarError::Other(format!(
                    "commit failed: pack {} missing from storage (stale session or concurrent deletion)",
                    pack_id
                )));
            }
        }
        Ok(())
    }

    /// Serialize, encrypt, and write the chunk index to storage.
    fn persist_index(&mut self) -> Result<()> {
        let estimated_msgpack = self.chunk_index.len().saturating_mul(80);
        let estimated = 1 + zstd::zstd_safe::compress_bound(estimated_msgpack);
        let index_packed = pack_object_streaming_with_context(
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            estimated,
            self.crypto.as_ref(),
            |buf| {
                compress::compress_stream_zstd(buf, 3, |encoder| {
                    rmp_serde::encode::write(encoder, &self.chunk_index)?;
                    Ok(())
                })
            },
        )?;
        self.storage.put("index", &index_packed)?;
        self.index_dirty = false;
        Ok(())
    }

    /// Serialize, encrypt, and write the manifest to storage.
    fn persist_manifest(&mut self) -> Result<()> {
        let manifest_bytes = rmp_serde::to_vec(&self.manifest)?;
        let manifest_packed = pack_object_with_context(
            ObjectType::Manifest,
            MANIFEST_OBJECT_CONTEXT,
            &manifest_bytes,
            self.crypto.as_ref(),
        )?;
        self.storage.put("manifest", &manifest_packed)?;
        self.manifest_dirty = false;
        Ok(())
    }

    /// Reload the manifest from storage (for concurrent session commit).
    pub fn reload_manifest(&mut self) -> Result<()> {
        let manifest_data = self
            .storage
            .get("manifest")?
            .ok_or_else(|| VykarError::Other("manifest not found on reload".into()))?;
        let compressed = unpack_object_expect_with_context(
            &manifest_data,
            ObjectType::Manifest,
            MANIFEST_OBJECT_CONTEXT,
            self.crypto.as_ref(),
        )?;
        self.manifest = rmp_serde::from_slice(&compressed)?;
        self.manifest_dirty = false;
        Ok(())
    }

    /// Try to perform an incremental index update using the local full_index_cache.
    /// Returns `Ok(true)` on success (index uploaded, caches rebuilt, manifest updated).
    /// Returns `Ok(false)` if the cache is missing or stale (caller should use slow path).
    fn try_incremental_index_update(&mut self, delta: &IndexDelta) -> Result<bool> {
        let cd = self.cache_dir_override.as_deref();
        let cache_path = match dedup_cache::full_index_cache_path(&self.config.id, cd) {
            Some(p) => p,
            None => return Ok(false),
        };

        let old_cache = match dedup_cache::MmapFullIndexCache::open(
            &self.config.id,
            self.manifest.index_generation,
            cd,
        ) {
            Some(c) => c,
            None => return Ok(false),
        };

        debug!(
            old_entries = old_cache.entry_count(),
            new_entries = delta.new_entries.len(),
            refcount_bumps = delta.refcount_bumps.len(),
            "incremental index update: merging delta"
        );

        let new_gen = rand::thread_rng().next_u64();

        // Merge old cache + delta → new cache file
        dedup_cache::merge_full_index_cache(&old_cache, delta, new_gen, &cache_path)?;

        // Drop old mmap before we open the new one
        drop(old_cache);

        // Open the newly merged cache for serialization
        let new_cache = dedup_cache::MmapFullIndexCache::open_path(&cache_path, new_gen)
            .ok_or_else(|| {
                vykar_types::error::VykarError::Other(
                    "failed to open newly merged full index cache".into(),
                )
            })?;

        // Serialize from cache → encrypted packed object
        let packed =
            dedup_cache::serialize_full_cache_to_packed_object(&new_cache, self.crypto.as_ref())?;

        // Upload
        self.storage.put("index", &packed)?;

        // Free upload buffer
        drop(packed);

        // Rebuild dedup + restore caches from full cache (streaming)
        if let Err(e) = dedup_cache::build_dedup_cache_from_full_cache(
            &cache_path,
            new_gen,
            &self.config.id,
            cd,
        ) {
            warn!("failed to rebuild dedup cache from full cache: {e}");
        }
        if let Err(e) = dedup_cache::build_restore_cache_from_full_cache(
            &cache_path,
            new_gen,
            &self.config.id,
            cd,
        ) {
            warn!("failed to rebuild restore cache from full cache: {e}");
        }

        // Update manifest generation — must happen before return so
        // load_chunk_index_from_full_cache uses the correct generation.
        self.manifest.index_generation = new_gen;
        self.manifest_dirty = true;

        Ok(true)
    }

    /// Reload the full chunk index from storage (always downloads from remote).
    fn reload_full_index(&self) -> Result<ChunkIndex> {
        if let Some(index_data) = self.storage.get("index")? {
            Self::decode_index_blob(&index_data, self.crypto.as_ref())
        } else {
            Ok(ChunkIndex::new())
        }
    }

    /// Load the index blob, trying the local blob cache first.
    /// Falls back to remote download on cache miss.
    fn load_index_blob_cached(&self) -> Result<Option<Vec<u8>>> {
        let generation = self.manifest.index_generation;
        let cache_dir = self.cache_dir_override.as_deref();

        // Try local blob cache
        if let Some(blob) =
            dedup_cache::read_index_blob_cache(&self.config.id, generation, cache_dir)
        {
            debug!("index blob cache hit (generation {generation})");
            return Ok(Some(blob));
        }

        // Cache miss — download from remote
        let Some(blob) = self.storage.get("index")? else {
            return Ok(None);
        };

        // Save to local cache (non-fatal on error)
        if let Err(e) =
            dedup_cache::write_index_blob_cache(&blob, generation, &self.config.id, cache_dir)
        {
            debug!("failed to write index blob cache: {e}");
        }

        Ok(Some(blob))
    }

    /// Reload the full chunk index, trying the local blob cache first.
    /// Falls back to remote download if the cached blob is corrupt.
    fn reload_full_index_cached(&self) -> Result<ChunkIndex> {
        if let Some(index_data) = self.load_index_blob_cached()? {
            match Self::decode_index_blob(&index_data, self.crypto.as_ref()) {
                Ok(index) => return Ok(index),
                Err(e) => {
                    warn!("index blob cache corrupt, falling back to remote: {e}");
                    // Fall through to uncached remote download
                }
            }
        } else {
            return Ok(ChunkIndex::new());
        }

        // Cached blob was corrupt — download fresh and rewrite the cache
        let Some(blob) = self.storage.get("index")? else {
            return Ok(ChunkIndex::new());
        };
        let index = Self::decode_index_blob(&blob, self.crypto.as_ref())?;

        if let Err(e) = dedup_cache::write_index_blob_cache(
            &blob,
            self.manifest.index_generation,
            &self.config.id,
            self.cache_dir_override.as_deref(),
        ) {
            debug!("failed to rewrite index blob cache: {e}");
        }

        Ok(index)
    }

    /// Decrypt, decompress, and deserialize an index blob.
    fn decode_index_blob(index_data: &[u8], crypto: &dyn CryptoEngine) -> Result<ChunkIndex> {
        let compressed = unpack_object_expect_with_context(
            index_data,
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            crypto,
        )?;
        let index_bytes = compress::decompress_metadata(&compressed)?;
        Ok(rmp_serde::from_slice(&index_bytes)?)
    }

    /// Increment refcount if this chunk already exists in committed or pending state.
    /// Returns stored size when found. Works in normal, dedup, and tiered modes.
    /// Falls back to recovered chunks from a previous interrupted session.
    pub fn bump_ref_if_exists(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        // Check dedup modes (write session required).
        let in_dedup_mode = {
            let ws = self
                .write_session
                .as_mut()
                .expect("no active write session");
            if let Some(ref tiered) = ws.tiered_dedup {
                if let Some(stored_size) = tiered.get_stored_size(chunk_id) {
                    if let Some(ref mut delta) = ws.index_delta {
                        delta.bump_refcount(chunk_id);
                    }
                    return Some(stored_size);
                }
                true
            } else if let Some(ref dedup) = ws.dedup_index {
                if let Some(stored_size) = dedup.get_stored_size(chunk_id) {
                    if let Some(ref mut delta) = ws.index_delta {
                        delta.bump_refcount(chunk_id);
                    }
                    return Some(stored_size);
                }
                true
            } else {
                false
            }
        };

        // Normal mode: check chunk_index (only when no dedup mode is active).
        if !in_dedup_mode {
            if let Some(entry) = self.chunk_index.get(chunk_id) {
                let stored_size = entry.stored_size;
                self.chunk_index.increment_refcount(chunk_id);
                self.index_dirty = true;
                return Some(stored_size);
            }
        }

        // Check recovered chunks before pending pack writers.
        if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
            return Some(stored_size);
        }

        self.bump_ref_pending(chunk_id)
    }

    /// Prefilter said "probably exists" — tiered: skip xor, check session_new → mmap → pending.
    /// Non-tiered: falls through to bump_ref_if_exists.
    pub fn bump_ref_prefilter_hit(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        let is_tiered = {
            let ws = self
                .write_session
                .as_mut()
                .expect("no active write session");
            if let Some(ref tiered) = ws.tiered_dedup {
                if let Some(stored_size) = tiered.get_stored_size_skip_filter(chunk_id) {
                    if let Some(ref mut delta) = ws.index_delta {
                        delta.bump_refcount(chunk_id);
                    }
                    return Some(stored_size);
                }
                true
            } else {
                false
            }
        };
        if is_tiered {
            if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
                return Some(stored_size);
            }
            return self.bump_ref_pending(chunk_id);
        }
        self.bump_ref_if_exists(chunk_id)
    }

    /// Prefilter said "definitely doesn't exist" — tiered: session_new → pending only.
    /// Non-tiered: falls through to bump_ref_if_exists.
    pub fn bump_ref_prefilter_miss(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        let is_tiered = {
            let ws = self
                .write_session
                .as_mut()
                .expect("no active write session");
            if let Some(ref tiered) = ws.tiered_dedup {
                if let Some(stored_size) = tiered.session_new_stored_size(chunk_id) {
                    if let Some(ref mut delta) = ws.index_delta {
                        delta.bump_refcount(chunk_id);
                    }
                    return Some(stored_size);
                }
                true
            } else {
                false
            }
        };
        if is_tiered {
            if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
                return Some(stored_size);
            }
            return self.bump_ref_pending(chunk_id);
        }
        self.bump_ref_if_exists(chunk_id)
    }

    /// Check only pending pack writers (shared helper).
    fn bump_ref_pending(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .bump_ref_pending(chunk_id)
    }

    /// Inline false-positive path: compress + encrypt + commit a chunk whose ChunkId
    /// was already computed by the worker. Avoids re-hashing via `store_chunk`.
    pub fn commit_chunk_inline(
        &mut self,
        chunk_id: ChunkId,
        data: &[u8],
        compression: compress::Compression,
        pack_type: PackType,
    ) -> Result<u32> {
        debug_assert_eq!(
            ChunkId::compute(self.crypto.chunk_id_key(), data),
            chunk_id,
            "inline commit: chunk_id mismatch"
        );
        let compressed = compress::compress(compression, data)?;
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            &chunk_id.0,
            &compressed,
            self.crypto.as_ref(),
        )?;
        self.commit_prepacked_chunk(chunk_id, packed, pack_type)
    }

    /// Commit a pre-compressed and pre-encrypted chunk to the selected pack writer.
    /// Returns the stored size in bytes.
    pub fn commit_prepacked_chunk(
        &mut self,
        chunk_id: ChunkId,
        packed: Vec<u8>,
        pack_type: PackType,
    ) -> Result<u32> {
        let stored_size = packed.len() as u32;

        // Add blob and check flush in a scoped borrow
        let should_flush = {
            let ws = self
                .write_session
                .as_mut()
                .expect("no active write session");
            let writer = match pack_type {
                PackType::Data => &mut ws.data_pack_writer,
                PackType::Tree => &mut ws.tree_pack_writer,
            };
            writer.add_blob(chunk_id, packed)?;
            writer.should_flush()
        };

        if should_flush {
            self.flush_writer_async(pack_type)?;
        }

        Ok(stored_size)
    }

    /// Update index entries for a freshly sealed pack.
    fn apply_sealed_entries(&mut self, pack_id: PackId, entries: Vec<pack::PackedChunkEntry>) {
        if self
            .write_session
            .as_mut()
            .expect("no active write session")
            .apply_sealed_entries(pack_id, entries, &mut self.chunk_index)
        {
            self.index_dirty = true;
        }
    }

    /// Seal a pack writer and upload in the background.
    /// The index is updated immediately; the upload proceeds in a separate thread.
    ///
    /// The `SealedPack` is destructured: `entries` consumed on the main thread
    /// for index updates, `data` (owning the mmap or Vec) moved into the upload
    /// thread. `pack_id` is `Copy` so it's used in both places.
    fn flush_writer_async(&mut self, pack_type: PackType) -> Result<()> {
        // Keep upload fan-out bounded to avoid excessive memory/thread pressure.
        self.cap_pending_uploads()?;

        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        let SealedPack {
            pack_id,
            entries,
            data,
        } = match pack_type {
            PackType::Data => ws.data_pack_writer.seal()?,
            PackType::Tree => ws.tree_pack_writer.seal()?,
        };

        // Recalculate data pack target after each data pack flush.
        if pack_type == PackType::Data {
            ws.session_packs_flushed += 1;
            let total = ws.persisted_pack_count + ws.session_packs_flushed;
            let new_target = compute_data_pack_target(
                total,
                self.config.min_pack_size,
                self.config.max_pack_size,
            );
            ws.data_pack_writer.set_target_size(new_target);
        }

        // Record journal entries before apply_sealed_entries consumes them.
        let journal_chunks: Vec<PendingChunkEntry> = entries
            .iter()
            .map(
                |&(chunk_id, stored_size, offset, _refcount)| PendingChunkEntry {
                    chunk_id,
                    stored_size,
                    pack_offset: offset,
                },
            )
            .collect();
        ws.pending_journal.record_pack(pack_id, journal_chunks);

        // Record pack ID for dump rollback tracking.
        if let Some(ref mut tracker) = ws.dump_tracker {
            tracker.journal_pack_ids.push(pack_id);
        }

        // Release ws borrow before apply_sealed_entries (which needs &mut self).

        self.apply_sealed_entries(pack_id, entries);

        let storage = Arc::clone(&self.storage);
        let key = pack_id.storage_key();
        self.write_session
            .as_mut()
            .unwrap()
            .pending_uploads
            .push(std::thread::spawn(move || data.put_to(&*storage, &key)));

        Ok(())
    }

    /// Store a chunk in the repository. Returns (chunk_id, stored_size, was_new).
    /// If the chunk already exists (dedup), just increments the refcount.
    pub fn store_chunk(
        &mut self,
        data: &[u8],
        compression: compress::Compression,
        pack_type: PackType,
    ) -> Result<(ChunkId, u32, bool)> {
        let chunk_id = ChunkId::compute(self.crypto.chunk_id_key(), data);

        if let Some(stored_size) = self.bump_ref_if_exists(&chunk_id) {
            return Ok((chunk_id, stored_size, false));
        }

        // Compress
        let compressed = compress::compress(compression, data)?;

        // Encrypt and wrap in repo object envelope
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            &chunk_id.0,
            &compressed,
            self.crypto.as_ref(),
        )?;
        let stored_size = self.commit_prepacked_chunk(chunk_id, packed, pack_type)?;

        Ok((chunk_id, stored_size, true))
    }

    /// Read and decrypt a chunk from the repository.
    /// Results are cached in a weight-bounded blob cache for faster repeated access.
    pub fn read_chunk(&mut self, chunk_id: &ChunkId) -> Result<Vec<u8>> {
        let entry = *self
            .chunk_index
            .get(chunk_id)
            .ok_or_else(|| VykarError::Other(format!("chunk not found: {chunk_id}")))?;

        self.read_chunk_at(
            chunk_id,
            &entry.pack_id,
            entry.pack_offset,
            entry.stored_size,
        )
    }

    /// Read and decrypt a chunk given explicit pack location coordinates.
    /// Bypasses the chunk index — the caller supplies (pack_id, offset, stored_size)
    /// e.g. from the mmap restore cache.
    pub fn read_chunk_at(
        &mut self,
        chunk_id: &ChunkId,
        pack_id: &PackId,
        pack_offset: u64,
        stored_size: u32,
    ) -> Result<Vec<u8>> {
        if let Some(cached) = self.blob_cache.get(chunk_id) {
            return Ok(cached.to_vec());
        }

        let blob_data =
            read_blob_from_pack(self.storage.as_ref(), pack_id, pack_offset, stored_size)?;
        let compressed = unpack_object_expect_with_context(
            &blob_data,
            ObjectType::ChunkData,
            &chunk_id.0,
            self.crypto.as_ref(),
        )?;
        let plaintext = compress::decompress(&compressed)?;

        self.blob_cache.insert(*chunk_id, plaintext.clone());
        Ok(plaintext)
    }

    /// Flush all pending pack writes and wait for background uploads.
    /// No-op when no write session is active.
    pub fn flush_packs(&mut self) -> Result<()> {
        let Some(ws) = self.write_session.as_ref() else {
            return Ok(());
        };
        let flush_data = ws.data_pack_writer.has_pending();
        let flush_tree = ws.tree_pack_writer.has_pending();

        if flush_data {
            self.flush_writer_async(PackType::Data)?;
        }
        if flush_tree {
            self.flush_writer_async(PackType::Tree)?;
        }
        // Wait for all background uploads to complete before returning.
        self.write_session
            .as_mut()
            .unwrap()
            .wait_pending_uploads(&*self.storage, &*self.crypto)?;
        Ok(())
    }

    /// Best-effort cleanup after a failed backup or other operation.
    ///
    /// Seals any partial pack writers, waits for in-flight uploads to land,
    /// and writes the final `pending_index` journal so a subsequent run can
    /// recover. All errors are logged but never propagated.
    ///
    /// No-ops when no write session is active or there is nothing to clean up.
    pub fn flush_on_abort(&mut self) {
        let Some(ws) = self.write_session.as_ref() else {
            return;
        };
        let has_partial_packs =
            ws.data_pack_writer.has_pending() || ws.tree_pack_writer.has_pending();
        if ws.pending_uploads.is_empty() && ws.pending_journal.is_empty() && !has_partial_packs {
            return;
        }

        warn!("saving progress for next run\u{2026}");

        // Seal and flush any partial data/tree pack writers.
        if self
            .write_session
            .as_ref()
            .unwrap()
            .data_pack_writer
            .has_pending()
        {
            if let Err(e) = self.flush_writer_async(PackType::Data) {
                warn!("flush_on_abort: failed to seal data pack: {e}");
            }
        }
        if self
            .write_session
            .as_ref()
            .unwrap()
            .tree_pack_writer
            .has_pending()
        {
            if let Err(e) = self.flush_writer_async(PackType::Tree) {
                warn!("flush_on_abort: failed to seal tree pack: {e}");
            }
        }

        // Join all in-flight upload threads so packs land on storage.
        let ws = self.write_session.as_mut().unwrap();
        for handle in ws.pending_uploads.drain(..) {
            match handle
                .join()
                .map_err(|_| VykarError::Other("pack upload thread panicked".into()))
                .and_then(|r| r)
            {
                Ok(()) => {}
                Err(e) => warn!("flush_on_abort: upload thread failed: {e}"),
            }
        }

        // Write final pending_index so next run can recover.
        self.write_session
            .as_mut()
            .unwrap()
            .write_pending_index_best_effort(&*self.storage, &*self.crypto);

        // Clear the session so Drop doesn't fire the debug_assert.
        self.write_session = None;
    }

    /// Recover chunk→pack mappings from a previous interrupted session's
    /// `pending_index` file. Verifies each pack exists before adding entries.
    ///
    /// Must be called inside the repo lock, before `enable_tiered_dedup_mode()`.
    /// Returns the number of recovered chunk entries.
    pub fn recover_pending_index(&mut self) -> Result<usize> {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .recover_pending_index(&*self.storage, &*self.crypto, &self.chunk_index)
    }

    /// Best-effort delete of the `pending_index` file from storage.
    /// Called from the backup command after `save_state()` succeeds.
    pub fn clear_pending_index(&self, session_id: &str) {
        WriteSessionState::clear_pending_index(&*self.storage, session_id);
    }

    // --- Dump checkpoint/rollback API ---

    /// Begin a dump checkpoint: flush any pending data pack, snapshot the
    /// current `IndexDelta` state, and arm the rollback tracker so all
    /// subsequent mutations can be undone if the dump command fails.
    pub(crate) fn begin_dump_checkpoint(&mut self) -> Result<()> {
        // Force-flush the data pack writer to isolate dump data.
        let has_pending = self
            .write_session
            .as_ref()
            .expect("no active write session")
            .data_pack_writer
            .has_pending();
        if has_pending {
            self.flush_writer_async(PackType::Data)?;
        }

        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        let delta_checkpoint = ws
            .index_delta
            .as_ref()
            .map(|d| d.checkpoint())
            .unwrap_or_else(crate::index::IndexDeltaCheckpoint::empty);
        let data_pack_target_size = ws.data_pack_writer.target_size();
        ws.dump_tracker = Some(write_session::DumpRollbackTracker {
            delta_checkpoint,
            dedup_inserts: Vec::new(),
            promoted_recovered: Vec::new(),
            journal_pack_ids: Vec::new(),
            data_pack_target_size,
        });
        Ok(())
    }

    /// Commit a dump checkpoint: discard the rollback tracker (dump succeeded).
    pub(crate) fn commit_dump_checkpoint(&mut self) {
        if let Some(ws) = self.write_session.as_mut() {
            ws.dump_tracker = None;
        }
    }

    /// Roll back a dump checkpoint: undo all index mutations that occurred
    /// since `begin_dump_checkpoint()`. Packs already uploaded to storage
    /// become orphans cleaned by compact.
    pub(crate) fn rollback_dump_checkpoint(&mut self) {
        // Destructure tracker outside the ws borrow scope so we can use
        // dedup_inserts for chunk_index rollback in non-dedup mode.
        let (tracker_fields, in_tiered, in_dedup) = {
            let ws = self
                .write_session
                .as_mut()
                .expect("no active write session");
            let Some(tracker) = ws.dump_tracker.take() else {
                return;
            };
            let in_tiered = ws.tiered_dedup.is_some();
            let in_dedup = ws.dedup_index.is_some();

            // 1. Rollback IndexDelta
            if let Some(ref mut delta) = ws.index_delta {
                delta.rollback(tracker.delta_checkpoint);
            }

            // 2. Remove dedup inserts from the active dedup structure
            if in_tiered {
                for chunk_id in &tracker.dedup_inserts {
                    if let Some(ref mut tiered) = ws.tiered_dedup {
                        tiered.remove(chunk_id);
                    }
                }
            } else if in_dedup {
                for chunk_id in &tracker.dedup_inserts {
                    if let Some(ref mut dedup) = ws.dedup_index {
                        dedup.remove(chunk_id);
                    }
                }
            }

            // 3. Re-insert promoted recovered chunks
            for (chunk_id, entry) in tracker.promoted_recovered {
                ws.recovered_chunks.insert(chunk_id, entry);
            }

            // 4. Remove tracked pack IDs from pending journal
            for pack_id in &tracker.journal_pack_ids {
                ws.pending_journal.remove_pack(pack_id);
            }

            // 5. Reset data pack writer (discards any partial pack buffer)
            ws.data_pack_writer = PackWriter::new(PackType::Data, tracker.data_pack_target_size);

            (tracker.dedup_inserts, in_tiered, in_dedup)
        };

        // 6. Non-dedup mode: entries went directly into chunk_index.
        //    Must happen after dropping the ws borrow above.
        if !in_tiered && !in_dedup {
            for chunk_id in &tracker_fields {
                self.chunk_index.decrement(chunk_id);
            }
        }
    }

    /// Promote a recovered chunk into the active dedup structure and index delta.
    /// Returns the stored size if the chunk was in `recovered_chunks`, None otherwise.
    fn promote_recovered_chunk(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        let (stored_size, index_modified) = self
            .write_session
            .as_mut()
            .expect("no active write session")
            .promote_recovered_chunk(chunk_id, &mut self.chunk_index)?;
        if index_modified {
            self.index_dirty = true;
        }
        Some(stored_size)
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        if self.write_session.is_some() {
            warn!("Repository dropped with active write session");
        }
    }
}
