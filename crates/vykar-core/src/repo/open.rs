use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use rand::Rng;

use super::file_cache::FileCache;
use super::format::{pack_object_streaming_with_context, ObjectType};
use super::key::{self, KeyProofCtx};
use super::manifest::Manifest;
use super::snapshot_cache;
use super::{
    BlobCache, EncryptionMode, OpenOptions, RepoConfig, RepoFormat, Repository,
    BLOB_CACHE_MAX_BYTES, INDEX_OBJECT_CONTEXT, KEY_PRIMARY, KEY_SECONDARY,
};
use crate::compress;
use crate::config::{
    default_max_pack_size, default_min_pack_size, ChunkerConfig, RepositoryConfig,
};
use crate::index::{ChunkIndex, IndexBlob};
use vykar_crypto::key::{EncryptedKey, MasterKey};
use vykar_crypto::{CryptoEngine, PlaintextEngine};
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkHasher;
use vykar_types::error::{Result, VykarError};

/// Domain-separation context for the format-v3 plaintext chunk-ID key.
///
/// `derive_key` rather than `blake3::hash`: it is a distinct BLAKE3 mode with
/// its own flag bits, so this key lives in a different domain from the chunk
/// digests (keyed mode) and the pack digests (unkeyed mode) instead of sharing
/// one with `blake3::hash`.
const PLAINTEXT_CHUNK_ID_KEY_CONTEXT: &str =
    "vykar 2026-01-01 plaintext repository chunk-id key v1";

/// Derive the deterministic chunk-ID key for a plaintext repository.
///
/// **Format-dependent, and the only auxiliary hash that is.** This value *is*
/// the chunk-ID key, so a v2 repository must reproduce it byte-identically or
/// lose its entire dedup index.
///
/// The key is not secret: `repo_id` is stored unencrypted in the repository
/// `config`. It exists so plaintext repositories use the same keyed hashing
/// path as encrypted ones, which buys corruption detection, not tamper
/// resistance.
pub(crate) fn derive_plaintext_chunk_id_key(repo_id: &[u8], format: RepoFormat) -> [u8; 32] {
    match format {
        // Frozen: RustCrypto BLAKE2b-256, exactly as v2 repositories were
        // written. Do not "modernise" this to blake2b_simd without checking
        // it byte-for-byte. `Blake2b256` sets the same digest length in the
        // BLAKE2b parameter block as the `Blake2bVar::new(32)` this replaced,
        // so the output is unchanged; `v2_fixture_compat` is the guard.
        RepoFormat::V2 => {
            use blake2::{Blake2b256, Digest};
            let mut hasher = Blake2b256::new();
            hasher.update(repo_id);
            hasher.finalize().into()
        }
        RepoFormat::V3 => blake3::derive_key(PLAINTEXT_CHUNK_ID_KEY_CONTEXT, repo_id),
    }
}

/// Best-effort repository label for the capability-probe error message.
fn storage_label(repo_config_opts: Option<&RepositoryConfig>) -> String {
    repo_config_opts.map_or_else(|| "the configured server".to_string(), |c| c.url.clone())
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
        // Library callers can bypass YAML validation. Reject unsupported
        // parameters before writing a new repository's immutable config.
        chunker_params.validate_even_sizes()?;
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

        // New repositories are always created at the current format. There is
        // deliberately no flag and no config key to choose otherwise: the
        // choice would only ever be "make this repo unreadable by newer
        // binaries for no benefit".
        let format = RepoFormat::CURRENT;

        let repo_config = RepoConfig {
            version: format.version(),
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
                    let chunk_hasher = ChunkHasher::new(
                        format.chunk_hash(),
                        derive_plaintext_chunk_id_key(&repo_config.id, format),
                    );
                    (Arc::new(PlaintextEngine::new(chunk_hasher)), None)
                }
                mode => {
                    let master_key = MasterKey::generate()?;
                    let pass = passphrase.ok_or_else(|| {
                        VykarError::Config("passphrase required for encrypted repository".into())
                    })?;
                    let enc_key = master_key.to_encrypted(pass)?;
                    (key::build_engine(mode, format, &master_key)?, Some(enc_key))
                }
            };

        // Pre-flight: refuse to create a repository the server cannot serve.
        //
        // Load-bearing, because `init` writes only non-pack keys (config,
        // keys/repokey, index, index.gen) and an old server accepts every one
        // of them. Without this probe `vykar init` would *succeed* against an
        // old server and only the first `backup` would fail, leaving a repo
        // nobody can write to.
        match storage.server_capabilities() {
            Ok(caps) => {
                if !caps.supports_hash(format.chunk_hash()) {
                    return Err(VykarError::Config(format!(
                        "vykar-server at {} (version {}) does not support {} repositories \
                         (format v{}). Upgrade the server to 0.20 or later, or keep using \
                         an existing repository.",
                        storage_label(repo_config_opts),
                        caps.version,
                        format.chunk_hash().as_str().to_uppercase(),
                        format.version(),
                    )));
                }
            }
            // No server behind this backend — nothing to probe.
            Err(VykarError::UnsupportedBackend(_)) => {}
            Err(err) => return Err(err),
        }

        storage.bind_content_hash(format.chunk_hash())?;

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

        // Store encrypted key if applicable, in two copies.
        //
        // The same serialized bytes go to both keys, deliberately: a second
        // `to_encrypted` call would produce a different salt and nonce, and
        // comparing the two copies byte-for-byte is what makes corruption
        // distinguishable from a wrong passphrase later.
        let mut key_blob: Option<Vec<u8>> = None;
        if let Some(enc_key) = &encrypted_key {
            if !server_did_init {
                storage.create_dir("keys/")?;
            }
            let key_data = rmp_serde::to_vec(enc_key)?;
            storage.put(KEY_PRIMARY, &key_data)?;
            storage.put(KEY_SECONDARY, &key_data)?;
            key_blob = Some(key_data);
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
            // Freshly initialized repo — `snapshots/` is empty.
            skipped_snapshots: Vec::new(),
            chunk_index,
            config: repo_config,
            format,
            file_cache: FileCache::new(),
            blob_cache: BlobCache::new(BLOB_CACHE_MAX_BYTES),
            index_generation: 0,
            index_dirty: false,
            file_cache_dirty: false,
            rebuild_dedup_cache: false,
            cache_dir_override: cache_dir,
            write_session: None,
            lock_fence: None,
            key_blob,
            index_sidecar_write: None,
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
        Self::open_shared(Arc::from(storage), passphrase, cache_dir, opts)
    }

    /// [`Self::open`] over a backend the caller keeps a handle to, e.g. to
    /// delete a session marker when the open fails.
    pub(crate) fn open_shared(
        storage: Arc<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
        opts: OpenOptions,
    ) -> Result<Self> {
        let (mut repo, file_cache_load) = Self::open_inner(storage, passphrase, cache_dir, &opts)?;
        // The index GET overlaps the file cache load (local disk), which
        // runs on its own thread; it is decoded only after that load has
        // finished, so the two never hold their transient buffers at once.
        let raw_index = opts.load_index.then(|| repo.storage.get("index"));
        if let Some(handle) = file_cache_load {
            repo.file_cache = handle
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        }
        if let Some(raw_index) = raw_index {
            let index = Self::decode_full_index(raw_index?, repo.crypto.as_ref())?;
            // `index_generation` still holds the `index.gen` hint read in
            // `open_inner`, before the index GET.
            let sidecar = repo.index_generation;
            repo.install_chunk_index(index, Some(sidecar));
        }
        Ok(repo)
    }

    /// Open the repository. With `opts.load_file_cache` the local file
    /// cache is loaded on a helper thread that the caller joins: it reads
    /// only local disk, so it overlaps the remaining round trips of open.
    #[allow(clippy::type_complexity)]
    fn open_inner(
        storage: Arc<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
        opts: &OpenOptions,
    ) -> Result<(Self, Option<std::thread::JoinHandle<FileCache>>)> {
        // Read config
        let config_data = storage
            .get("config")?
            .ok_or_else(|| VykarError::RepoNotFound("config not found".into()))?;
        let repo_config: RepoConfig = rmp_serde::from_slice(&config_data)?;

        // The single version gate. Everything downstream asks the resolved
        // `RepoFormat`, never the raw integer.
        let format = RepoFormat::from_version(repo_config.version)?;

        if opts.require_encryption && repo_config.encryption == EncryptionMode::None {
            return Err(VykarError::PlaintextRepository(
                opts.repo_url.clone().unwrap_or_default(),
            ));
        }

        // Tell the backend which upload-integrity header to send. It was
        // constructed before the config could be read, so this is the first
        // moment the algorithm is known.
        storage.bind_content_hash(format.chunk_hash())?;

        if repo_config.max_pack_size > 512 * 1024 * 1024 {
            return Err(VykarError::Config(format!(
                "max_pack_size ({}) exceeds hard limit of 512 MiB",
                repo_config.max_pack_size
            )));
        }

        // Build crypto engine. `key::load_master_key` is the single place the
        // key copies are read, so corroboration, diagnosis and backfill live
        // in exactly one place.
        let (crypto, key_blob): (Arc<dyn CryptoEngine>, Option<Vec<u8>>) =
            match &repo_config.encryption {
                EncryptionMode::None => {
                    let chunk_hasher = ChunkHasher::new(
                        format.chunk_hash(),
                        derive_plaintext_chunk_id_key(&repo_config.id, format),
                    );
                    (Arc::new(PlaintextEngine::new(chunk_hasher)), None)
                }
                mode => {
                    let ctx = KeyProofCtx {
                        repo_config: &repo_config,
                        format,
                        // `--trust-repo` waives the pin, so the pin must not be
                        // consulted at all in that case.
                        url: if opts.trust_repo {
                            None
                        } else {
                            opts.repo_url.as_deref()
                        },
                        cache_dir: cache_dir.as_deref(),
                    };
                    let loaded = key::load_master_key(
                        storage.as_ref(),
                        passphrase,
                        &ctx,
                        !opts.skip_key_backfill,
                    )?;
                    (
                        key::build_engine(mode, format, &loaded.master_key)?,
                        Some(loaded.blob),
                    )
                }
            };

        // Load the file cache from local disk (not from the repo) while the
        // round trips below run. Spawned only now that the crypto engine
        // that decrypts it exists.
        let file_cache_load = if opts.load_file_cache {
            let crypto = Arc::clone(&crypto);
            let repo_id = repo_config.id.clone();
            let cache_dir = cache_dir.clone();
            let spawned = std::thread::Builder::new()
                .name("file-cache-load".into())
                .spawn(move || FileCache::load(&repo_id, crypto.as_ref(), cache_dir.as_deref()));
            match spawned {
                Ok(handle) => Some(handle),
                Err(e) => {
                    return Err(VykarError::Other(format!(
                        "failed to spawn file cache load thread: {e}"
                    )))
                }
            }
        } else {
            None
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
        let refresh = snapshot_cache::refresh_snapshot_cache(
            storage.as_ref(),
            crypto.as_ref(),
            &repo_config.id,
            cache_dir.as_deref(),
            false, // strict_io: false — resilient open
        )?;
        let skipped_snapshots = refresh.skipped;
        let manifest = Manifest::from_snapshot_entries(refresh.entries);

        let repo = Repository {
            storage,
            crypto,
            manifest,
            skipped_snapshots,
            chunk_index: ChunkIndex::new(),
            config: repo_config,
            format,
            // Filled in by `open` from `file_cache_load`.
            file_cache: FileCache::new(),
            blob_cache: BlobCache::new(BLOB_CACHE_MAX_BYTES),
            index_generation,
            index_dirty: false,
            file_cache_dirty: false,
            rebuild_dedup_cache: false,
            cache_dir_override: cache_dir,
            write_session: None,
            lock_fence: None,
            key_blob,
            index_sidecar_write: None,
        };
        Ok((repo, file_cache_load))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MemoryBackend;
    use std::sync::Mutex;
    use vykar_storage::{delegate_storage_backend, InnerBackend};

    /// Logs every `get` key in call order.
    struct GetLog {
        inner: Arc<dyn StorageBackend>,
        gets: Arc<Mutex<Vec<String>>>,
    }

    impl InnerBackend for GetLog {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &*self.inner
        }
    }

    delegate_storage_backend! {
        for GetLog;
        except [get];

        fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.gets.lock().unwrap().push(key.to_string());
            self.inner.get(key)
        }
    }

    /// With `with_index`, the index is still fetched after the `index.gen`
    /// hint it is compared against (see `install_chunk_index`), and it is
    /// installed with its generation.
    #[test]
    fn open_with_index_reads_the_sidecar_before_the_index() {
        crate::testutil::init_test_environment();
        let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        Repository::init(
            Box::new(Arc::clone(&inner)),
            EncryptionMode::None,
            ChunkerConfig::default(),
            None,
            None,
            None,
        )
        .unwrap();
        // Make the sidecar disagree, so the open must repair it.
        inner.put("index.gen", &7u64.to_le_bytes()).unwrap();

        let gets = Arc::new(Mutex::new(Vec::new()));
        let storage = Box::new(GetLog {
            inner: Arc::clone(&inner),
            gets: Arc::clone(&gets),
        });
        let cache = tempfile::tempdir().unwrap();
        let repo = Repository::open(
            storage,
            None,
            Some(cache.path().to_path_buf()),
            OpenOptions::new().with_index(),
        )
        .unwrap();

        let gets = gets.lock().unwrap().clone();
        let pos = |k: &str| gets.iter().position(|g| g == k).expect(k);
        assert!(pos("index.gen") < pos("index"), "{gets:?}");
        assert_eq!(repo.index_generation, 0);
        assert_eq!(
            inner.get("index.gen").unwrap().unwrap(),
            0u64.to_le_bytes(),
            "the stale sidecar is rewritten to the index's generation"
        );
    }

    /// Pins the plaintext chunk-ID key derivation for both formats, through
    /// the production helper. This value *is* the dedup identity of every
    /// unencrypted repository, so changing either digest silently invalidates
    /// their indexes. Do not regenerate.
    #[test]
    fn plaintext_key_derivation_known_answers() {
        let repo_id = [0x11u8; 32];
        assert_eq!(
            hex::encode(derive_plaintext_chunk_id_key(&repo_id, RepoFormat::V2)),
            "d4ffaeeac45aa41825e0bc3f875570af061acbf0b950ad752ff0f9463fe13ad5"
        );
        assert_eq!(
            hex::encode(derive_plaintext_chunk_id_key(&repo_id, RepoFormat::V3)),
            "947392f5ba41ab94ef39cc84b74bbb5a0c578bf7841b6b6cf76b17ad4a9e5b67"
        );
    }
}
