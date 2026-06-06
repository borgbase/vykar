use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use rand::RngCore;

use super::file_cache::FileCache;
use super::format::{pack_object_streaming_with_context, ObjectType};
use super::manifest::Manifest;
use super::snapshot_cache;
use super::{
    BlobCache, EncryptionMode, OpenOptions, RepoConfig, Repository, BLOB_CACHE_MAX_BYTES,
    INDEX_OBJECT_CONTEXT,
};
use crate::compress;
use crate::config::{
    default_max_pack_size, default_min_pack_size, ChunkerConfig, RepositoryConfig,
};
use crate::index::{ChunkIndex, IndexBlob};
use vykar_crypto::key::{EncryptedKey, MasterKey};
use vykar_crypto::{self as crypto, CryptoEngine, PlaintextEngine};
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};

/// Derive a deterministic chunk_id_key for plaintext repos from the repo ID.
fn derive_plaintext_chunk_id_key(repo_id: &[u8]) -> [u8; 32] {
    use blake2::digest::{Update, VariableOutput};
    use blake2::Blake2bVar;
    let mut key = [0u8; 32];
    let mut hasher = Blake2bVar::new(32).expect("valid BLAKE2b output length");
    hasher.update(repo_id);
    hasher
        .finalize_variable(&mut key)
        .expect("output buffer matches BLAKE2b length");
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
        let mut rng = rand::rng();
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
                    let master_key = MasterKey::generate()?;
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
                    let master_key = MasterKey::generate()?;
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
            Some(data) => <[u8; 8]>::try_from(data.as_slice())
                .map(u64::from_le_bytes)
                .unwrap_or(0),
            None => 0,
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
}
