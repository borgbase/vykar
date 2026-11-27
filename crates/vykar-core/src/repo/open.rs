use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use rand::Rng;

use super::file_cache::FileCache;
use super::index_store;
use super::key::{self, KeyProofCtx};
use super::manifest::Manifest;
use super::snapshot_cache;
use super::tree_cache::TreeChunkCache;
use super::writer_mode::{ModeCheck, init_time_check};
use super::{
    ChunkerParams, EncryptionMode, HostEnv, InitOptions, KEY_PRIMARY, KEY_SECONDARY,
    MAX_PACK_SIZE_LIMIT, OpenOptions, Repo, RepoConfig, RepoFormat, WriterMode,
    default_max_pack_size, default_min_pack_size,
};
use crate::index::LiveIndex;
use crate::index::head::IndexHead;
use vykar_crypto::CryptoEngine;
use vykar_crypto::key::{EncryptedKey, MasterKey};
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};

/// Best-effort repository label for the capability-probe error message.
fn storage_label(init_opts: Option<&InitOptions>) -> String {
    init_opts
        .and_then(|o| o.url.clone())
        .unwrap_or_else(|| "the configured server".to_string())
}

#[cfg(any(test, feature = "sim", feature = "testing"))]
thread_local! {
    /// Set by [`with_cheap_kdf`]: init on this thread wraps the master key
    /// with Argon2id parameters cheap enough for the simulator. On by
    /// default in this crate's own unit-test binary, off everywhere else.
    static CHEAP_KDF: std::cell::Cell<bool> = const { std::cell::Cell::new(cfg!(test)) };
}

/// Run `f` with every `Repo::init` on this thread wrapping the
/// master key with Argon2id t = 1, m = 8 KiB, p = 1 (the simulator's
/// encrypted template; tests). Opens derive with the parameters the blob
/// stores, so later opens of such a repository are cheap without it.
/// Not API: test support (features sim, testing).
#[cfg(any(test, feature = "sim", feature = "testing"))]
pub fn with_cheap_kdf<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            CHEAP_KDF.with(|c| c.set(self.0));
        }
    }
    let _restore = Restore(CHEAP_KDF.with(|c| c.replace(true)));
    f()
}

/// Wrap a new repository's master key with the passphrase: the product's
/// Argon2id parameters, or [`with_cheap_kdf`]'s inside it.
fn wrap_master_key(master_key: &MasterKey, pass: &str) -> Result<EncryptedKey> {
    #[cfg(any(test, feature = "sim", feature = "testing"))]
    if CHEAP_KDF.with(std::cell::Cell::get) {
        return master_key.to_encrypted_with_kdf(pass, 1, 8, 1);
    }
    master_key.to_encrypted(pass)
}

/// The file cache loading beside the open, and the stamp of the file it read.
type FileCacheLoad =
    std::thread::JoinHandle<(FileCache, Option<super::file_cache::FileCacheStamp>)>;

impl Repo {
    /// Initialize a new repository.
    pub fn init(
        storage: Box<dyn StorageBackend>,
        encryption: EncryptionMode,
        chunker_params: ChunkerParams,
        passphrase: Option<&str>,
        init_opts: Option<&InitOptions>,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self> {
        // New repositories are always created at the current format. There is
        // deliberately no flag and no config key to choose otherwise: the
        // choice would only ever be "make this repo unreadable by newer
        // binaries for no benefit".
        Self::init_at_format(
            storage,
            encryption,
            chunker_params,
            passphrase,
            init_opts,
            cache_dir,
            RepoFormat::CURRENT,
        )
    }

    /// [`Self::init`] at an explicit format, for tests of formats `init`
    /// does not create.
    #[doc(hidden)]
    pub fn init_at_format(
        storage: Box<dyn StorageBackend>,
        encryption: EncryptionMode,
        chunker_params: ChunkerParams,
        passphrase: Option<&str>,
        init_opts: Option<&InitOptions>,
        cache_dir: Option<PathBuf>,
        format: RepoFormat,
    ) -> Result<Self> {
        if encryption == EncryptionMode::None {
            return Err(VykarError::Config(super::PLAINTEXT_MODE_REFUSAL.into()));
        }
        if format.is_upgrade_only() {
            return Err(VykarError::Config(format!(
                "format v{} repositories can no longer be created",
                format.version()
            )));
        }
        // Library callers can bypass YAML validation. Reject unsupported
        // parameters before writing a new repository's immutable config.
        chunker_params.validate_stored()?;
        let storage: Arc<dyn StorageBackend> = Arc::from(storage);

        // Check that the repo doesn't already exist
        if storage.exists("config")? {
            return Err(VykarError::RepoAlreadyExists("repository".into()));
        }

        // Generate repo ID
        let mut rng = rand::rng();
        let mut repo_id = vec![0u8; 32];
        rng.fill_bytes(&mut repo_id);

        let min_pack_size = init_opts
            .map(|o| o.min_pack_size)
            .unwrap_or_else(default_min_pack_size);
        let max_pack_size = init_opts
            .map(|o| o.max_pack_size)
            .unwrap_or_else(default_max_pack_size);

        if max_pack_size > MAX_PACK_SIZE_LIMIT {
            return Err(VykarError::Config(format!(
                "max_pack_size ({max_pack_size}) exceeds hard limit of 512 MiB"
            )));
        }
        if min_pack_size > max_pack_size {
            return Err(VykarError::Config(format!(
                "min_pack_size ({min_pack_size}) exceeds max_pack_size ({max_pack_size})"
            )));
        }

        let mut repo_config = RepoConfig::new_v6(
            repo_id,
            chunker_params.clone(),
            encryption.clone(),
            Utc::now(),
            min_pack_size,
            max_pack_size,
            format.chunk_hash(),
        );

        // Generate master key and crypto engine
        let master_key = MasterKey::generate()?;
        let pass = passphrase.ok_or_else(|| {
            VykarError::Config("passphrase required for encrypted repository".into())
        })?;
        let enc_key = wrap_master_key(&master_key, pass)?;
        let crypto: Arc<dyn CryptoEngine> = key::build_engine(&encryption, format, &master_key)?;

        // Pre-flight: refuse to create a repository the server cannot serve.
        //
        // Load-bearing, because `init` writes only non-pack keys (config,
        // keys/repokey, index.h/1) and an old server accepts every one
        // of them. Without this probe `vykar init` would *succeed* against an
        // old server and only the first `backup` would fail, leaving a repo
        // nobody can write to.
        if let Some(server) = storage.server_ops() {
            let caps = server.capabilities()?;
            if !caps.supports_hash(format.chunk_hash()) {
                return Err(VykarError::Config(format!(
                    "vykar-server at {} (version {}) does not support {} repositories \
                     (format v{}). Upgrade the server to 0.20 or later, or keep using \
                     an existing repository.",
                    storage_label(init_opts),
                    caps.version,
                    format.chunk_hash().as_str().to_uppercase(),
                    format.version(),
                )));
            }
            if !caps.segmented_index {
                return Err(VykarError::Config(format!(
                    "vykar-server at {} (version {}) does not support the segmented \
                     index of format v{}. Upgrade the server first.",
                    storage_label(init_opts),
                    caps.version,
                    format.version(),
                )));
            }
            if !caps.serves_v6_key_dirs() {
                return Err(VykarError::Config(format!(
                    "vykar-server at {} (version {}) does not accept the index.h/ and \
                     packmaps/ keys of format v{}. Upgrade the server first.",
                    storage_label(init_opts),
                    caps.version,
                    format.version(),
                )));
            }
        }

        storage.bind_content_hash(format.chunk_hash())?;

        // Try server-side init (creates keys/, snapshots/, locks/, packs/* in one request).
        // Placed after all validation and crypto setup so a failure above doesn't
        // leave a partially scaffolded repo on the server.
        let server_did_init = match storage.server_ops() {
            Some(server) => {
                server.init()?;
                true
            }
            None => false,
        };

        // The probe keys of the init-time check live under `sessions/`
        // (vykar_storage::PROBE_DIR). A server-side init leaves the
        // directory to the server, whose PUT creates it.
        if !server_did_init {
            storage.create_dir(super::sessions::SESSIONS_PREFIX)?;
        }
        // The verdict is permanent (only `vykar upgrade --commit-mode`
        // changes it), so init runs the strict check: sequential plus the
        // concurrent race. It is the first put-if-absent on this backend
        // instance. Undecided fails the init before `config` exists.
        let check = init_time_check(storage.as_ref()).map_err(|e| {
            VykarError::Other(format!(
                "could not decide whether the storage honours put-if-absent ({e}); the \
                 repository was not created (no config was written). Retry `vykar init`"
            ))
        })?;
        let single_writer_reason = match check {
            ModeCheck::Multi => {
                tracing::debug!("init: the storage passed the put-if-absent check; multi-writer");
                repo_config.set_writer_mode(WriterMode::Multi);
                None
            }
            ModeCheck::Single { reason } => {
                tracing::info!("init: the repository is single-writer ({reason})");
                Some(reason)
            }
        };

        // Store config (unencrypted)
        let config_data = repo_config.encode()?;
        storage.put("config", &config_data)?;

        // Store the encrypted key, in two copies.
        //
        // The same serialized bytes go to both keys, deliberately: a second
        // `to_encrypted` call would produce a different salt and nonce, and
        // comparing the two copies byte-for-byte is what makes corruption
        // distinguishable from a wrong passphrase later.
        if !server_did_init {
            storage.create_dir("keys/")?;
        }
        let key_blob = rmp_serde::to_vec(&enc_key)?;
        storage.put(KEY_PRIMARY, &key_blob)?;
        storage.put(KEY_SECONDARY, &key_blob)?;

        let manifest = Manifest::new();
        let chunk_index = LiveIndex::empty();
        // Server-side init does not create `index.d/` or `index.h/`: a server
        // that predates them refuses to start with the directories present.
        storage.create_dir(index_store::INDEX_OBJECT_DIR)?;
        storage.create_dir(index_store::HEAD_PREFIX)?;
        // A plain PUT (04 step 8 D5), whatever the mode: nothing can contend
        // before the repository has a config.
        let head = IndexHead::initial();
        index_store::put_head(storage.as_ref(), &head, crypto.as_ref())?;
        let index_generation = head.generation;
        let index_head = Some(head);

        // Create directory structure (skip if server already did it)
        if !server_did_init {
            storage.create_dir("snapshots/")?;
            for i in 0u8..=255 {
                storage.create_dir(&format!("packs/{:02x}/", i))?;
            }
        }

        let tree_cache = TreeChunkCache::for_repo(&repo_config.id, cache_dir.as_deref());
        Ok(Repo {
            storage,
            crypto,
            manifest,
            // Freshly initialized repo — `snapshots/` is empty.
            skipped_snapshots: Vec::new(),
            chunk_index,
            config: repo_config,
            format,
            file_cache: FileCache::new(),
            index_generation,
            index_dirty: false,
            file_cache_dirty: false,
            file_cache_stamp: None,
            file_cache_written: Default::default(),
            file_cache_dropped: Default::default(),
            cache_dir_override: cache_dir,
            tree_cache,
            host_env: HostEnv::system(),
            key_blob,
            index_head,
            fold_policy: Default::default(),
            fold_requested: super::index_write::FoldRequest::None,
            created_head: None,
            repo_url: init_opts.and_then(|o| o.url.clone()),
            single_writer_reason,
            commit_mode: None,
            pending_sweep: None,
            index_gc_grace: index_store::IndexGcGrace::DEFAULT,
            store_time: None,
            exclusive_check_reuse: super::sessions::EXCLUSIVE_CHECK_REUSE,
            live_sessions: None,
            pack_grace: super::ages::T_PACK,
            segment_grace: super::ages::T_PACK,
            segment_ages: None,
            relocation: Default::default(),
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
        // Local cache catch-up and segment downloads overlap the file cache
        // load; the index is built only after it finished.
        let synced = opts.load_index.then(|| {
            let head = repo.index_head.take();
            repo.sync_index(head, true)
        });
        if let Some(handle) = file_cache_load {
            let (cache, stamp) = handle
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            repo.file_cache = cache;
            repo.file_cache_stamp = stamp;
        }
        if let Some(synced) = synced {
            let start = std::time::Instant::now();
            let synced = synced?;
            let source = synced.source;
            repo.install_synced_index(synced);
            tracing::debug!(
                ?source,
                generation = repo.index_generation,
                entries = repo.chunk_index.len(),
                elapsed_ms = start.elapsed().as_millis() as u64,
                "index loaded at open"
            );
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
    ) -> Result<(Self, Option<FileCacheLoad>)> {
        // Read config
        let config_data = storage
            .get("config")?
            .ok_or_else(|| VykarError::RepoNotFound("config not found".into()))?;
        let decoded = RepoConfig::decode(&config_data)?;
        let format = decoded.validate()?;
        if format.is_upgrade_only() {
            if !opts.legacy_format {
                return Err(super::upgrade::legacy_format_error(format));
            }
            if opts.load_index {
                return Err(VykarError::Other(
                    "a v2/v3 index is read only by `vykar upgrade`".into(),
                ));
            }
        }
        let repo_config = decoded.into_repo_config();

        // Tell the backend which upload-integrity header to send. It was
        // constructed before the config could be read, so this is the first
        // moment the algorithm is known.
        storage.bind_content_hash(format.chunk_hash())?;

        // Build crypto engine. `key::load_master_key` is the single place the
        // key copies are read, so corroboration, diagnosis and backfill live
        // in exactly one place.
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
        let loaded =
            key::load_master_key(storage.as_ref(), passphrase, &ctx, !opts.skip_key_backfill)?;
        let crypto: Arc<dyn CryptoEngine> =
            key::build_engine(&repo_config.encryption, format, &loaded.master_key)?;
        let key_blob = loaded.blob;

        // Load the file cache from local disk (not from the repo) while the
        // round trips below run. Spawned only now that the crypto engine
        // that decrypts it exists.
        let file_cache_load = if opts.load_file_cache {
            let crypto = Arc::clone(&crypto);
            let repo_id = repo_config.id.clone();
            let cache_dir = cache_dir.clone();
            let spawned = std::thread::Builder::new()
                .name("file-cache-load".into())
                .spawn(move || {
                    FileCache::load_stamped(&repo_id, crypto.as_ref(), cache_dir.as_deref())
                });
            match spawned {
                Ok(handle) => Some(handle),
                Err(e) => {
                    return Err(VykarError::Other(format!(
                        "failed to spawn file cache load thread: {e}"
                    )));
                }
            }
        } else {
            None
        };

        // Leftovers of local cache writes interrupted by a kill or power
        // loss (only files unwritten for a day), and the fixed-name caches
        // of older binaries.
        crate::index::dedup_cache::sweep_stale_cache_temps(&repo_config.id, cache_dir.as_deref());

        let mut index_generation = 0;

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

        // The head, read once and only after the snapshot list. A writer
        // PUTs the head before its snapshot, so this head covers every listed
        // snapshot. Opens without the index still use it: a later index
        // load syncs to it (`load_chunk_index_for_snapshot_view`). A handle
        // opened for the upgrade has none.
        let index_head = if format.is_upgrade_only() {
            None
        } else {
            match index_store::fetch_head(storage.as_ref(), crypto.as_ref()) {
                Ok(head) => {
                    index_generation = head.generation;
                    Some(head)
                }
                Err(e) if opts.tolerate_broken_index => {
                    tracing::warn!("the index head cannot be read: {e}");
                    None
                }
                Err(e) => return Err(e),
            }
        };

        let tree_cache = TreeChunkCache::for_repo(&repo_config.id, cache_dir.as_deref());
        let repo = Repo {
            storage,
            crypto,
            manifest,
            skipped_snapshots,
            chunk_index: LiveIndex::empty(),
            config: repo_config,
            format,
            // Filled in by `open` from `file_cache_load`.
            file_cache: FileCache::new(),
            index_generation,
            index_dirty: false,
            file_cache_dirty: false,
            file_cache_stamp: None,
            file_cache_written: Default::default(),
            file_cache_dropped: Default::default(),
            cache_dir_override: cache_dir,
            tree_cache,
            host_env: opts.host_env.for_new_run(),
            key_blob,
            index_head,
            fold_policy: Default::default(),
            fold_requested: super::index_write::FoldRequest::None,
            created_head: None,
            repo_url: opts.repo_url.clone(),
            single_writer_reason: None,
            commit_mode: None,
            pending_sweep: None,
            index_gc_grace: index_store::IndexGcGrace::DEFAULT,
            store_time: None,
            exclusive_check_reuse: super::sessions::EXCLUSIVE_CHECK_REUSE,
            live_sessions: None,
            pack_grace: super::ages::T_PACK,
            segment_grace: super::ages::T_PACK,
            segment_ages: None,
            relocation: Default::default(),
        };
        Ok((repo, file_cache_load))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MemoryBackend;
    use vykar_types::chunk_id::ChunkId;
    use vykar_types::pack_id::PackId;

    /// A repository whose index was persisted as a base, with its table
    /// file in `cache`.
    fn repo_with_caches(cache: &std::path::Path) -> (Arc<dyn StorageBackend>, Repo) {
        crate::testutil::init_test_environment();
        let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let mut repo = Repo::init(
            Box::new(Arc::clone(&inner)),
            EncryptionMode::Aes256Gcm,
            ChunkerParams::default(),
            Some(crate::testing::TEST_PASSPHRASE),
            None,
            Some(cache.to_path_buf()),
        )
        .unwrap();
        let pack = PackId::from_bytes([0xAA; 32]);
        let mut writer = repo.unchecked_writer();
        let index = writer.chunk_index_mut();
        for i in 0..3u8 {
            index.add(
                ChunkId::from_bytes([i; 32]),
                100 + u32::from(i),
                pack,
                200 * u64::from(i),
            );
        }
        index.set_pack_trailer(pack, 0);
        // A fold, so that the base's table file is in the cache dir.
        repo.request_index_fold();
        repo.unchecked_writer().save_state().unwrap();
        (inner, repo)
    }

    #[test]
    fn open_sweeps_stale_cache_temp_files() {
        let cache = tempfile::tempdir().unwrap();
        let (inner, repo) = repo_with_caches(cache.path());
        let id = repo.config.id.clone();
        let dir = crate::cache_paths::repo_cache_dir(&id, Some(cache.path())).unwrap();
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(25 * 3600);
        let stale_temp = dir.join(".full_index_cache.AbC123.tmp");
        let stale_legacy = dir.join("full_index_cache.merged");
        let stale_table_temp = dir.join(".chunk_table.Old123.tmp");
        let fresh_temp = dir.join(".dedup_cache.Fresh1.tmp");
        let legacy_cache = dir.join("full_index_cache");
        for path in [&stale_temp, &stale_legacy, &stale_table_temp] {
            std::fs::File::create(path)
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        std::fs::write(&fresh_temp, b"fresh").unwrap();
        std::fs::write(&legacy_cache, b"legacy").unwrap();
        let base = repo.index_head().unwrap().base.as_ref().unwrap().name;
        let table = crate::index::table::base_table_path(&dir, &base);
        std::fs::File::options()
            .write(true)
            .open(&table)
            .unwrap()
            .set_modified(old)
            .unwrap();
        drop(repo);

        let open = || {
            Repo::open(
                Box::new(Arc::clone(&inner)),
                Some(crate::testing::TEST_PASSPHRASE),
                Some(cache.path().to_path_buf()),
                OpenOptions::new(),
            )
            .unwrap()
        };
        drop(open());

        assert!(!stale_temp.exists());
        assert!(!stale_legacy.exists());
        assert!(!stale_table_temp.exists());
        assert!(fresh_temp.exists());
        assert!(
            !legacy_cache.exists(),
            "legacy caches are deleted at any age"
        );
        assert!(crate::index::ChunkTable::open_file(&table).is_some());

        // The cache dir is swept on every open, not rate-limited.
        let second = dir.join(".restore_cache.XyZ789.tmp");
        std::fs::File::create(&second)
            .unwrap()
            .set_modified(old)
            .unwrap();
        drop(open());
        assert!(!second.exists());
    }
}
