pub mod file_cache;
pub mod format;
pub mod identity;
pub mod lock;
pub mod manifest;
pub mod pack;
pub mod snapshot_cache;
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
};
use crate::index::dedup_cache::{self, TieredDedupIndex};
use crate::index::{
    ChunkIndex, DedupIndex, IndexBlob, IndexBlobRef, IndexDelta, PendingChunkEntry,
};
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

/// Derive a deterministic chunk_id_key for plaintext repos from the repo ID.
fn derive_plaintext_chunk_id_key(repo_id: &[u8]) -> [u8; 32] {
    use blake2::digest::{Update, VariableOutput};
    use blake2::Blake2bVar;
    let mut key = [0u8; 32];
    let mut hasher = Blake2bVar::new(32).unwrap();
    hasher.update(repo_id);
    hasher.finalize_variable(&mut key).unwrap();
    key
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
            version: 2,
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
                    let chunk_id_key = derive_plaintext_chunk_id_key(&repo_config.id);
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

        // Store empty IndexBlob (compressed with ZSTD)
        let manifest = Manifest::new();
        let chunk_index = ChunkIndex::new();
        let index_blob = IndexBlob {
            generation: 0,
            chunks: chunk_index.clone(),
        };
        let index_packed = pack_object_streaming_with_context(
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            64, // empty index is tiny
            crypto.as_ref(),
            |buf| {
                compress::compress_stream_zstd(buf, 3, |encoder| {
                    rmp_serde::encode::write(encoder, &index_blob)?;
                    Ok(())
                })
            },
        )?;
        storage.put("index", &index_packed)?;

        // Write advisory index.gen sidecar
        storage.put("index.gen", &0u64.to_le_bytes())?;

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
            index_generation: 0,
            index_dirty: false,
            file_cache_dirty: false,
            rebuild_dedup_cache: false,
            cache_dir_override: cache_dir,
            write_session: None,
            lock_fence: None,
        })
    }

    /// Open an existing repository with the given options.
    ///
    /// By default (`OpenOptions::new()`), neither the chunk index nor the file
    /// cache is loaded. Use `.with_index()` and/or `.with_file_cache()` to
    /// opt in.
    pub fn open(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
        opts: OpenOptions,
    ) -> Result<Self> {
        let mut repo = Self::open_inner(storage, passphrase, cache_dir, !opts.load_file_cache)?;
        if opts.load_index {
            repo.load_chunk_index()?;
        }
        Ok(repo)
    }

    fn open_inner(
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

        if repo_config.version != 2 {
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
                let chunk_id_key = derive_plaintext_chunk_id_key(&repo_config.id);
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

        // Read advisory index.gen sidecar (cache hint only, not trusted for writes).
        let index_generation = match storage.get("index.gen")? {
            Some(data) if data.len() == 8 => {
                u64::from_le_bytes(data[..8].try_into().unwrap_or([0u8; 8]))
            }
            _ => 0,
        };

        // Refresh snapshot list from snapshots/ (replaces manifest load).
        // Resilient open: skip unreadable snapshots so a single corrupt blob
        // doesn't prevent opening the repo.
        let snapshot_entries = snapshot_cache::refresh_snapshot_cache(
            storage.as_ref(),
            crypto.as_ref(),
            &repo_config.id,
            cache_dir.as_deref(),
            false, // strict_io: false — resilient open
        )?;
        let manifest = Manifest::from_snapshot_entries(snapshot_entries);

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
            index_generation,
            index_dirty: false,
            file_cache_dirty: false,
            rebuild_dedup_cache: false,
            cache_dir_override: cache_dir,
            write_session: None,
            lock_fence: None,
        })
    }

    /// Load the chunk index from storage on demand.
    /// Always downloads the remote index blob to get the authenticated generation.
    /// Can be called after opening without `load_index` to lazily load the index.
    /// Also recalculates the data pack writer target from the loaded index.
    pub fn load_chunk_index(&mut self) -> Result<()> {
        let (gen, index) = self.reload_full_index_with_generation()?;
        self.index_generation = gen;
        self.chunk_index = index;
        // Best-effort rewrite index.gen
        let _ = self.storage.put("index.gen", &gen.to_le_bytes());
        self.rebase_pack_target_from_index();
        Ok(())
    }

    /// Load the chunk index from storage, bypassing the local blob cache.
    /// Use this for operations like `check` that must verify what's actually
    /// in the remote repository.
    ///
    /// NOTE: Does not update `index_generation` — only suitable for read-only
    /// operations. Use `load_chunk_index()` for write paths.
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
        let mut ws = WriteSessionState::new(data_target, tree_target, 2);
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

    /// Try to open the mmap'd restore cache for this repository.
    /// Returns `None` if the cache is missing, stale, or corrupt.
    pub fn open_restore_cache(&self) -> Option<dedup_cache::MmapRestoreCache> {
        dedup_cache::MmapRestoreCache::open(
            &self.config.id,
            self.index_generation,
            self.cache_dir_override.as_deref(),
        )
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
        let generation = self.index_generation;
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

    /// Resolve a cache-hit chunk reference by bumping its refcount.
    /// Returns the stored size from the authoritative index/recovered entry.
    /// Errors if the chunk is not found in any state (committed, recovered,
    /// or pending pack writer) — this indicates storage corruption.
    pub fn reuse_cached_chunk_ref(&mut self, chunk_id: &ChunkId) -> Result<u32> {
        self.bump_ref_if_exists(chunk_id).ok_or_else(|| {
            VykarError::Other(format!(
                "cache hit references unresolvable chunk {chunk_id}"
            ))
        })
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
                // Download fresh remote index and apply delta.
                // No reconcile() needed: callers use with_maintenance_lock()
                // which guarantees no concurrent sessions are active.
                let mut full_index = self.reload_full_index()?;
                delta.apply_to(&mut full_index);
                self.chunk_index = full_index;
                self.index_dirty = true;
            } else if self.rebuild_dedup_cache {
                // Empty delta: index unchanged, but caches may need rebuilding.
                // Try to rebuild caches from full_index_cache if available.
                let mut rebuilt_from_cache = false;
                let cd = self.cache_dir_override.as_deref();
                if let Some(cache_path) = dedup_cache::full_index_cache_path(&self.config.id, cd) {
                    if dedup_cache::MmapFullIndexCache::open(
                        &self.config.id,
                        self.index_generation,
                        cd,
                    )
                    .is_some()
                    {
                        let gen = self.index_generation;
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

    /// Save the chunk index back to storage.
    /// When a write session is active: flushes pending packs, applies dedup delta.
    /// When no session is active: just persists dirty index/file_cache.
    /// Only writes components that have been marked dirty.
    pub fn save_state(&mut self) -> Result<()> {
        let deferred_index_load = if self.write_session.is_some() {
            self.apply_write_session()?
        } else {
            false
        };

        // When the index changes, rotate index_generation so the local dedup
        // cache is invalidated.
        if self.index_dirty {
            self.index_generation = rand::thread_rng().next_u64();
        }

        if self.index_dirty {
            self.persist_index()?;
        }

        if self.rebuild_dedup_cache {
            self.rebuild_local_caches(false);
            self.rebuild_dedup_cache = false;
        }

        // Save file cache before hydrating chunk_index to reduce peak memory.
        // Capture error instead of early-returning so we can hydrate first.
        let fc_result = self.save_file_cache_if_dirty();

        // Always hydrate chunk_index — postcondition: self.chunk_index is valid
        // on all exit paths (success and error).
        if deferred_index_load {
            // Try local full_index_cache first (fast, no storage round-trip),
            // fall back to reloading from remote storage if cache is unavailable.
            self.chunk_index = dedup_cache::load_chunk_index_from_full_cache(
                &self.config.id,
                self.index_generation,
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
    /// 3. Refresh snapshot list from storage.
    /// 4. Check snapshot name uniqueness against fresh list.
    /// 5. Download fresh remote index, reconcile delta, persist index.
    /// 6. Write `snapshots/<id>` to storage — **commit point**.
    /// 7. Update local manifest + snapshot cache.
    /// 8. Save file cache and consume write session.
    pub fn commit_concurrent_session(
        &mut self,
        snapshot_entry: manifest::SnapshotEntry,
        snapshot_packed: Vec<u8>,
        new_file_cache: &mut file_cache::FileCache,
    ) -> Result<()> {
        self.commit_concurrent_session_with_progress(
            snapshot_entry,
            snapshot_packed,
            new_file_cache,
            &mut None::<Box<dyn FnMut(crate::commands::backup::BackupProgressEvent)>>,
        )
    }

    /// Commit a concurrent backup session with progress reporting.
    ///
    /// 1. Flush packs and join uploads.
    /// 2. Take the delta from the write session.
    /// 3. Refresh snapshot list from storage.
    /// 4. Check snapshot name uniqueness against fresh list.
    /// 5. Download fresh remote index, reconcile delta, persist index.
    /// 6. Write `snapshots/<id>` to storage — **commit point**.
    /// 7. Update local manifest + snapshot cache.
    /// 8. Save file cache and consume write session.
    pub fn commit_concurrent_session_with_progress(
        &mut self,
        snapshot_entry: manifest::SnapshotEntry,
        snapshot_packed: Vec<u8>,
        new_file_cache: &mut file_cache::FileCache,
        progress: &mut Option<impl FnMut(crate::commands::backup::BackupProgressEvent)>,
    ) -> Result<()> {
        use crate::commands::backup::BackupProgressEvent;

        macro_rules! emit_stage {
            ($progress:expr, $stage:expr) => {{
                let stage_start = std::time::Instant::now();
                if let Some(ref mut cb) = $progress {
                    cb(BackupProgressEvent::CommitStage { stage: $stage });
                }
                (stage_start, $stage)
            }};
        }

        macro_rules! log_stage_elapsed {
            ($ctx:expr) => {
                debug!(
                    stage = $ctx.1,
                    elapsed_ms = $ctx.0.elapsed().as_millis() as u64,
                    "commit stage complete"
                );
            };
        }

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

        // 3. Refresh snapshot list (unreadable blobs are skipped — a garbage
        //    snapshot that can't be decrypted cannot conflict with a valid name).
        let ctx = emit_stage!(progress, "refresh snapshots");
        self.refresh_snapshot_list()?;
        log_stage_elapsed!(ctx);

        // 4. Check snapshot name uniqueness against fresh list.
        if self.manifest.find_snapshot(&snapshot_entry.name).is_some() {
            return Err(VykarError::SnapshotAlreadyExists(
                snapshot_entry.name.clone(),
            ));
        }

        // 5. Download fresh remote index, reconcile delta, persist index.
        let mut deferred_chunk_index_hydrate = false;
        if let Some(delta) = delta {
            if !delta.is_empty() {
                let ctx = emit_stage!(progress, "fetch index");
                let raw_blob = self.fetch_raw_index_blob()?;
                log_stage_elapsed!(ctx);

                // Try fast path: compare raw blob against cached copy.
                let fast_path_taken = if let Some(ref raw_data) = raw_blob {
                    self.try_fast_path_commit(raw_data, &delta, progress)?
                } else {
                    false
                };

                if fast_path_taken {
                    // chunk_index hydration deferred until after the snapshot
                    // commit point so a local cache error can't abort the
                    // backup after the remote index has already been updated.
                    deferred_chunk_index_hydrate = true;
                } else {
                    let ctx = emit_stage!(progress, "decode index");
                    let fresh_index = if let Some(ref raw_data) = raw_blob {
                        Self::decode_raw_index_blob(raw_data, self.crypto.as_ref())?
                    } else {
                        (0, ChunkIndex::new())
                    };
                    log_stage_elapsed!(ctx);

                    let ctx = emit_stage!(progress, "reconcile");
                    let reconciled = delta.reconcile(&fresh_index.1)?;
                    log_stage_elapsed!(ctx);

                    let ctx = emit_stage!(progress, "verify packs");
                    self.verify_delta_packs(&reconciled)?;
                    log_stage_elapsed!(ctx);

                    let mut fresh_index = fresh_index.1;
                    reconciled.apply_to(&mut fresh_index);
                    self.chunk_index = fresh_index;
                    self.index_dirty = true;
                    self.index_generation = rand::thread_rng().next_u64();

                    let ctx = emit_stage!(progress, "write index");
                    self.persist_index()?;
                    log_stage_elapsed!(ctx);
                }
            } else if self.rebuild_dedup_cache {
                // Empty delta but caches need rebuilding (tiered dedup was active).
                // chunk_index was dropped — reload from remote for cache rebuild.
                let ctx = emit_stage!(progress, "fetch index");
                self.chunk_index = self.reload_full_index()?;
                log_stage_elapsed!(ctx);
            }
        }

        // Defensive: persist index if dirty but no delta (unreachable today
        // because backup always activates dedup mode, but guards future callers).
        if self.index_dirty {
            self.index_generation = rand::thread_rng().next_u64();
            self.persist_index()?;
        }

        // 6. Write snapshots/<id> — commit point.
        let ctx = emit_stage!(progress, "write snapshot");
        self.check_lock_fence()?;
        self.storage
            .put(&snapshot_entry.id.storage_key(), &snapshot_packed)?;
        log_stage_elapsed!(ctx);

        // 7. Update local manifest.
        self.manifest.timestamp = Utc::now();
        self.manifest.snapshots.push(snapshot_entry);

        // Hydrate chunk_index after the commit point (fast-path deferred).
        // Best-effort: a local cache read failure here is non-fatal since the
        // remote index is already committed. Falls back to remote reload.
        if deferred_chunk_index_hydrate {
            let cd = self.cache_dir_override.as_deref();
            self.chunk_index = dedup_cache::load_chunk_index_from_full_cache(
                &self.config.id,
                self.index_generation,
                cd,
            )
            .or_else(|e| {
                warn!("fast path: local cache hydration failed ({e}), reloading from remote");
                self.reload_full_index()
            })?;
        }

        // Merge active sections if any were produced (filesystem backup).
        // Dump-only runs produce no active sections and skip this block.
        let sections = new_file_cache.take_active_sections();
        if !sections.is_empty() {
            for (key, section) in sections {
                self.file_cache.merge_section(&key, section);
            }
            self.file_cache_dirty = true;
        }

        // Persist if dirty — covers both the merge above AND any prior
        // invalidation that set the dirty flag (e.g., stale sections removed
        // at backup start).
        if let Err(e) = self.save_file_cache_if_dirty() {
            warn!("failed to save file cache after concurrent commit: {e}");
        }

        // Rebuild local caches if needed. When the fast path already wrote the
        // full cache (deferred_chunk_index_hydrate), skip the redundant sort.
        let ctx = emit_stage!(progress, "rebuild local caches");
        if self.rebuild_dedup_cache {
            self.rebuild_local_caches(deferred_chunk_index_hydrate);
            self.rebuild_dedup_cache = false;
        }
        log_stage_elapsed!(ctx);

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
        const BATCH_VERIFY_THRESHOLD: usize = 32;

        let pack_ids: std::collections::HashSet<PackId> =
            delta.new_entries.iter().map(|e| e.pack_id).collect();

        let shards: std::collections::HashSet<String> = pack_ids
            .iter()
            .map(|id| format!("packs/{}", id.shard_prefix()))
            .collect();

        if pack_ids.len() >= BATCH_VERIFY_THRESHOLD && pack_ids.len() > shards.len() {
            // Many packs — batch-verify via shard listing.
            let mut known_packs: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            let mut fallback_shards: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            for shard in &shards {
                match self.storage.list(shard) {
                    Ok(keys) => known_packs.extend(keys),
                    Err(_) => {
                        fallback_shards.insert(shard.clone());
                    }
                }
            }

            for pack_id in &pack_ids {
                let shard_dir = format!("packs/{}", pack_id.shard_prefix());
                if fallback_shards.contains(&shard_dir) {
                    // list() failed for this shard — fall back to per-pack exists() with ? propagation.
                    let key = pack_id.storage_key();
                    if !self.storage.exists(&key)? {
                        return Err(VykarError::Other(format!(
                            "commit failed: pack {} missing from storage (stale session or concurrent deletion)",
                            pack_id
                        )));
                    }
                } else if !known_packs.contains(&pack_id.storage_key()) {
                    return Err(VykarError::Other(format!(
                        "commit failed: pack {} missing from storage (stale session or concurrent deletion)",
                        pack_id
                    )));
                }
            }
        } else {
            // Few packs — per-pack exists() is cheaper than listing entire shards.
            for pack_id in &pack_ids {
                let key = pack_id.storage_key();
                if !self.storage.exists(&key)? {
                    return Err(VykarError::Other(format!(
                        "commit failed: pack {} missing from storage (stale session or concurrent deletion)",
                        pack_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Serialize, encrypt, and write the IndexBlob (generation + chunks) to storage.
    /// Also writes the advisory `index.gen` sidecar and caches the raw encrypted
    /// blob locally for the fast-path commit on the next backup run.
    fn persist_index(&mut self) -> Result<()> {
        let generation = self.index_generation;
        let estimated_msgpack = self.chunk_index.len().saturating_mul(80) + 16;
        let estimated = 1 + zstd::zstd_safe::compress_bound(estimated_msgpack);
        let index_packed = pack_object_streaming_with_context(
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            estimated,
            self.crypto.as_ref(),
            |buf| {
                let blob = IndexBlobRef {
                    generation,
                    chunks: &self.chunk_index,
                };
                compress::compress_stream_zstd(buf, 3, |encoder| {
                    rmp_serde::encode::write(encoder, &blob)?;
                    Ok(())
                })
            },
        )?;
        self.check_lock_fence()?;
        self.storage.put("index", &index_packed)?;
        // Advisory sidecar — best-effort, non-fatal.
        let _ = self.storage.put("index.gen", &generation.to_le_bytes());
        self.index_dirty = false;

        // Cache the raw blob for future fast-path checks (best-effort).
        if let Err(e) = dedup_cache::write_index_blob_cache(
            &index_packed,
            generation,
            &self.config.id,
            self.cache_dir_override.as_deref(),
        ) {
            debug!("failed to write index blob cache: {e}");
        }
        Ok(())
    }

    /// Rebuild all local caches: full index cache (1 sort), then derive
    /// dedup + restore caches from it (O(n) streaming, no sort).
    ///
    /// When `full_cache_fresh` is true, the full index cache is already
    /// up-to-date (e.g. from a fast-path merge) and only the derivation
    /// step runs.
    fn rebuild_local_caches(&self, full_cache_fresh: bool) {
        let cd = self.cache_dir_override.as_deref();
        if !full_cache_fresh {
            if let Err(e) = dedup_cache::build_full_index_cache(
                &self.chunk_index,
                self.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to build full index cache: {e}");
            }
        }
        if let Some(full_path) = dedup_cache::full_index_cache_path(&self.config.id, cd) {
            if let Err(e) = dedup_cache::build_dedup_cache_from_full_cache(
                &full_path,
                self.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to rebuild dedup cache from full cache: {e}");
            }
            if let Err(e) = dedup_cache::build_restore_cache_from_full_cache(
                &full_path,
                self.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to rebuild restore cache from full cache: {e}");
            }
        }
    }

    /// Re-list snapshots/ and rebuild the in-memory manifest.
    /// Used by concurrent session commit to get a fresh snapshot list.
    pub fn refresh_snapshot_list(&mut self) -> Result<()> {
        // Strict I/O: fail on GET errors so a transient failure can't hide an
        // existing snapshot name and allow a duplicate during commit.
        let entries = snapshot_cache::refresh_snapshot_cache(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            &self.config.id,
            self.cache_dir_override.as_deref(),
            true, // strict_io: true — commit uniqueness check
        )?;
        self.manifest = Manifest::from_snapshot_entries(entries);
        Ok(())
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

    /// Reload the full chunk index from storage (always downloads from remote).
    fn reload_full_index(&self) -> Result<ChunkIndex> {
        self.reload_full_index_with_generation()
            .map(|(_, index)| index)
    }

    /// Reload the full chunk index + generation from storage.
    fn reload_full_index_with_generation(&self) -> Result<(u64, ChunkIndex)> {
        if let Some(index_data) = self.storage.get("index")? {
            let blob = Self::decode_index_blob_full(&index_data, self.crypto.as_ref())?;
            Ok((blob.generation, blob.chunks))
        } else {
            Ok((0, ChunkIndex::new()))
        }
    }

    /// Decrypt, decompress, and deserialize an index blob into an `IndexBlob`.
    fn decode_index_blob_full(index_data: &[u8], crypto: &dyn CryptoEngine) -> Result<IndexBlob> {
        let compressed = unpack_object_expect_with_context(
            index_data,
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            crypto,
        )?;
        let index_bytes = compress::decompress_metadata(&compressed)?;
        Ok(rmp_serde::from_slice(&index_bytes)?)
    }

    /// Fetch the raw encrypted index blob from storage without decoding.
    fn fetch_raw_index_blob(&self) -> Result<Option<Vec<u8>>> {
        self.storage.get("index")
    }

    /// Decode an already-fetched raw index blob into (generation, ChunkIndex).
    fn decode_raw_index_blob(raw: &[u8], crypto: &dyn CryptoEngine) -> Result<(u64, ChunkIndex)> {
        let blob = Self::decode_index_blob_full(raw, crypto)?;
        Ok((blob.generation, blob.chunks))
    }

    /// Try the fast-path commit: if the remote index blob matches the cached
    /// copy, skip decode + reconcile. Uses AEAD ciphertext comparison —
    /// identical ciphertext guarantees identical plaintext.
    ///
    /// Try the fast-path commit: if the remote index blob matches the cached
    /// copy, skip decode + reconcile. Uses AEAD ciphertext comparison —
    /// identical ciphertext guarantees identical plaintext.
    ///
    /// On success: merges the full index cache, persists the merged index to
    /// storage, and caches the raw blob. Local dedup/restore cache derivation
    /// and `chunk_index` hydration are left to the caller (after the snapshot
    /// commit point) so that local-only failures cannot abort a committed backup.
    ///
    /// Returns `true` if the fast path was taken.
    fn try_fast_path_commit(
        &mut self,
        raw_blob: &[u8],
        delta: &IndexDelta,
        progress: &mut Option<impl FnMut(crate::commands::backup::BackupProgressEvent)>,
    ) -> Result<bool> {
        use crate::commands::backup::BackupProgressEvent;

        let cd = self.cache_dir_override.as_deref();
        let cached = dedup_cache::read_index_blob_cache(&self.config.id, self.index_generation, cd);
        let Some(cached_blob) = cached else {
            debug!("fast path: no cached index blob, falling through to slow path");
            return Ok(false);
        };

        if raw_blob != cached_blob.as_slice() {
            debug!("fast path: remote index changed, falling through to slow path");
            return Ok(false);
        }

        // Index unchanged — try to open the full index cache for merge.
        let Some(full_cache_path) = dedup_cache::full_index_cache_path(&self.config.id, cd) else {
            debug!("fast path: no cache dir, falling through to slow path");
            return Ok(false);
        };

        let old_cache =
            dedup_cache::MmapFullIndexCache::open_path(&full_cache_path, self.index_generation);
        let Some(old_cache) = old_cache else {
            debug!("fast path: full index cache missing or stale, falling through to slow path");
            return Ok(false);
        };

        if let Some(ref mut cb) = progress {
            cb(BackupProgressEvent::CommitStage {
                stage: "index unchanged, fast path",
            });
        }
        let fast_start = std::time::Instant::now();

        // Verify packs before merging.
        let ctx_start = std::time::Instant::now();
        self.verify_delta_packs(delta)?;
        debug!(
            stage = "verify packs",
            elapsed_ms = ctx_start.elapsed().as_millis() as u64,
            "commit stage complete"
        );

        // Merge old cache + delta into new full cache.
        self.index_generation = rand::thread_rng().next_u64();
        let new_cache_path = full_cache_path.with_extension("merged");
        dedup_cache::merge_full_index_cache(
            &old_cache,
            delta,
            self.index_generation,
            &new_cache_path,
        )?;
        // Drop the mmap BEFORE renaming — on Windows, mapped files block replacement.
        drop(old_cache);
        std::fs::rename(&new_cache_path, &full_cache_path)?;

        // Serialize the merged cache as the new index blob and persist.
        let merged_cache =
            dedup_cache::MmapFullIndexCache::open_path(&full_cache_path, self.index_generation)
                .ok_or_else(|| VykarError::Other("failed to reopen merged cache".into()))?;

        let index_packed = dedup_cache::serialize_full_cache_as_index_blob(
            &merged_cache,
            self.index_generation,
            self.crypto.as_ref(),
        )?;

        self.check_lock_fence()?;
        self.storage.put("index", &index_packed)?;
        let _ = self
            .storage
            .put("index.gen", &self.index_generation.to_le_bytes());
        self.index_dirty = false;

        // Cache the raw blob for next fast-path check (best-effort).
        if let Err(e) = dedup_cache::write_index_blob_cache(
            &index_packed,
            self.index_generation,
            &self.config.id,
            cd,
        ) {
            debug!("failed to write index blob cache: {e}");
        }
        // Dedup/restore cache derivation is deferred to the post-commit
        // rebuild block (after the snapshot write) to minimize lock hold time.
        // rebuild_dedup_cache stays true so the post-commit block picks it up.

        debug!(
            elapsed_ms = fast_start.elapsed().as_millis() as u64,
            "fast-path commit complete"
        );

        Ok(true)
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

    /// Maximum gap (in bytes) between two blobs in the same pack that will be
    /// merged into a single range read.
    const COALESCE_GAP: u64 = 256 * 1024; // 256 KiB

    /// Maximum total size of a single coalesced range read.
    const COALESCE_MAX: u64 = 16 * 1024 * 1024; // 16 MiB

    /// Read multiple chunks via coalesced range reads and append plaintext to `out`.
    ///
    /// Each entry in `chunks` is `(ChunkId, PackId, pack_offset, stored_size)`.
    /// Output is appended to `out` in the same order as `chunks`.
    /// Cache hits are served from `blob_cache`; misses are grouped by pack and
    /// coalesced into large range reads to minimise HTTP round-trips.
    pub fn read_chunks_coalesced_into(
        &mut self,
        chunks: &[(ChunkId, PackId, u64, u32)],
        out: &mut Vec<u8>,
    ) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        // --- Phase 1: cache scan ---
        // Result slots: Some(plaintext) for cache hits, None for misses.
        let mut slots: Vec<Option<Vec<u8>>> = Vec::with_capacity(chunks.len());
        // Blobs that need fetching, grouped by pack.
        let mut pack_blobs: std::collections::HashMap<PackId, Vec<CoalescedBlob>> =
            std::collections::HashMap::new();

        for (idx, (chunk_id, pack_id, pack_offset, stored_size)) in chunks.iter().enumerate() {
            if let Some(cached) = self.blob_cache.get(chunk_id) {
                slots.push(Some(cached.to_vec()));
            } else {
                slots.push(None);
                pack_blobs.entry(*pack_id).or_default().push(CoalescedBlob {
                    result_idx: idx,
                    chunk_id: *chunk_id,
                    pack_offset: *pack_offset,
                    stored_size: *stored_size,
                });
            }
        }

        // Drain contiguous completed slots into `out`, advancing the cursor.
        let drain_ready =
            |slots: &mut Vec<Option<Vec<u8>>>, next_emit: &mut usize, out: &mut Vec<u8>| {
                while *next_emit < slots.len() {
                    if let Some(data) = slots[*next_emit].take() {
                        out.extend_from_slice(&data);
                        *next_emit += 1;
                    } else {
                        break;
                    }
                }
            };

        let mut next_emit: usize = 0;
        drain_ready(&mut slots, &mut next_emit, out);

        // All cache hits — done.
        if pack_blobs.is_empty() {
            return Ok(());
        }

        // --- Phase 2: coalesce ---
        let mut groups: Vec<CoalescedGroup> = Vec::new();

        for (pack_id, mut blobs) in pack_blobs {
            blobs.sort_by_key(|b| b.pack_offset);

            let mut iter = blobs.into_iter();
            let first = iter.next().unwrap();
            let mut cur_start = first.pack_offset;
            let mut cur_end = first.pack_offset + first.stored_size as u64;
            let mut cur_blobs = vec![first];

            for blob in iter {
                let blob_end = blob.pack_offset + blob.stored_size as u64;
                let gap = blob.pack_offset.saturating_sub(cur_end);
                let merged_size = blob_end - cur_start;

                if gap <= Self::COALESCE_GAP && merged_size <= Self::COALESCE_MAX {
                    cur_end = blob_end;
                    cur_blobs.push(blob);
                } else {
                    groups.push(CoalescedGroup {
                        pack_id,
                        read_start: cur_start,
                        read_end: cur_end,
                        blobs: cur_blobs,
                    });
                    cur_start = blob.pack_offset;
                    cur_end = blob_end;
                    cur_blobs = vec![blob];
                }
            }
            groups.push(CoalescedGroup {
                pack_id,
                read_start: cur_start,
                read_end: cur_end,
                blobs: cur_blobs,
            });
        }

        // Sort groups so the one containing the earliest-needed slot is first.
        groups.sort_by_key(|g| g.blobs.iter().map(|b| b.result_idx).min().unwrap());

        // --- Phase 3: read + decrypt + incremental drain ---
        for group in groups {
            let pack_key = group.pack_id.storage_key();
            let read_len = group.read_end - group.read_start;

            let raw_data = self
                .storage
                .get_range(&pack_key, group.read_start, read_len)?
                .ok_or_else(|| VykarError::Other(format!("pack not found: {}", group.pack_id)))?;

            for blob in &group.blobs {
                let local_offset = (blob.pack_offset - group.read_start) as usize;
                let local_end = local_offset + blob.stored_size as usize;
                if local_end > raw_data.len() {
                    return Err(VykarError::Other(format!(
                        "blob extends beyond downloaded range in pack {}",
                        group.pack_id
                    )));
                }

                let blob_data = &raw_data[local_offset..local_end];
                let compressed = unpack_object_expect_with_context(
                    blob_data,
                    ObjectType::ChunkData,
                    &blob.chunk_id.0,
                    self.crypto.as_ref(),
                )?;
                let plaintext = compress::decompress(&compressed)?;

                self.blob_cache.insert(blob.chunk_id, plaintext.clone());
                slots[blob.result_idx] = Some(plaintext);

                drain_ready(&mut slots, &mut next_emit, out);
            }
        }

        // Final drain (safety net for groups not sorted by earliest slot).
        drain_ready(&mut slots, &mut next_emit, out);

        debug_assert_eq!(next_emit, slots.len(), "not all chunks were emitted");
        Ok(())
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
    pub fn recover_pending_index(&mut self) -> Result<write_session::PendingIndexRecovery> {
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

#[cfg(test)]
impl Repository {
    /// Clear the blob cache (test-only).
    pub fn clear_blob_cache(&mut self) {
        self.blob_cache = BlobCache::new(self.blob_cache.max_bytes);
    }
}

/// A single blob within a coalesced read group.
struct CoalescedBlob {
    /// Index in the original `chunks` slice (determines output order).
    result_idx: usize,
    chunk_id: ChunkId,
    pack_offset: u64,
    stored_size: u32,
}

/// A group of adjacent blobs from the same pack that will be fetched in one range read.
struct CoalescedGroup {
    pack_id: PackId,
    read_start: u64,
    read_end: u64, // exclusive
    blobs: Vec<CoalescedBlob>,
}

impl Drop for Repository {
    fn drop(&mut self) {
        if self.write_session.is_some() {
            warn!("Repository dropped with active write session");
        }
    }
}
