use std::path::{Path, PathBuf};

use crate::commands;
use crate::compress::Compression;
use crate::config::{
    EncryptionConfig, EncryptionModeConfig, HooksConfig, RepositoryConfig, ResolvedRepo,
    SourceEntry, SourceHooksConfig, VykarConfig,
};
use crate::index::{ChunkIndex, IndexBlob};
use crate::repo::format::{ObjectType, pack_object_streaming_with_context};
use crate::repo::{OpenOptions, RepoConfig, RepoFormat, Repository};
use crate::snapshot::SnapshotStats;
use vykar_crypto::CryptoEngine;

// Re-exported so `helpers::init_test_environment()` call sites keep working
// while a single `Once` governs the whole unit-test binary.
pub use crate::testutil::init_test_environment;

pub fn make_test_config(repo_dir: &Path) -> VykarConfig {
    init_test_environment();

    VykarConfig {
        repository: RepositoryConfig {
            url: repo_dir.to_string_lossy().to_string(),
            min_pack_size: 32 * 1024 * 1024,
            max_pack_size: 512 * 1024 * 1024,
            ..Default::default()
        },
        encryption: EncryptionConfig {
            mode: EncryptionModeConfig::None,
            passphrase: None,
            passcommand: None,
        },
        one_file_system: true,
        ..Default::default()
    }
}

pub fn init_repo(repo_dir: &Path) -> VykarConfig {
    let config = make_test_config(repo_dir);
    commands::init::run(&config, None).unwrap();
    config
}

/// The `IndexBlob` a v2/v3 binary writes at `index`: zstd-compressed msgpack,
/// packed as `ObjectType::ChunkIndex` with context `b"index"`.
pub(crate) fn legacy_index_blob(
    index: &ChunkIndex,
    generation: u64,
    crypto: &dyn CryptoEngine,
) -> Vec<u8> {
    let blob = IndexBlob {
        generation,
        chunks: index.clone(),
    };
    pack_object_streaming_with_context(ObjectType::ChunkIndex, b"index", 64, crypto, |buf| {
        crate::compress::compress_stream_zstd(buf, 3, |encoder| {
            rmp_serde::encode::write(encoder, &blob)?;
            Ok(())
        })
    })
    .unwrap()
}

/// Turn the v4/v5 repository at `repo_dir` into the v2/v3 repository 0.24
/// writes for the same data: `index` becomes the [`legacy_index_blob`] of its
/// index, `index.gen` is written, `index.d/` removed, `config` set to the
/// legacy version, and this repository's local cache directory under
/// `cache_dir` deleted. Packs and snapshots are identical between v2/v4 and
/// v3/v5 (see `RepoFormat`). Returns the legacy format.
pub(crate) fn downgrade_to_legacy(
    repo_dir: &Path,
    passphrase: Option<&str>,
    cache_dir: &Path,
) -> RepoFormat {
    const GENERATION: u64 = 0x24;
    let scratch = tempfile::tempdir().unwrap();
    let storage =
        vykar_storage::local_backend::LocalBackend::new(repo_dir.to_str().unwrap()).unwrap();
    let repo = Repository::open(
        Box::new(storage),
        passphrase,
        Some(scratch.path().to_path_buf()),
        OpenOptions::new().with_index(),
    )
    .unwrap();
    let legacy = match repo.format() {
        RepoFormat::V7 => RepoFormat::V2,
        RepoFormat::V6 => RepoFormat::V3,
        other => panic!("not a segmented format: {other:?}"),
    };
    let blob = legacy_index_blob(
        &repo.chunk_index().to_chunk_index(),
        GENERATION,
        repo.crypto.as_ref(),
    );
    let repo_id = repo.config.id.clone();
    drop(repo);

    std::fs::write(repo_dir.join("index"), blob).unwrap();
    std::fs::write(repo_dir.join("index.gen"), GENERATION.to_le_bytes()).unwrap();
    std::fs::remove_dir_all(repo_dir.join("index.d")).unwrap();
    std::fs::remove_dir_all(repo_dir.join("index.h")).unwrap();
    let mut config: RepoConfig =
        rmp_serde::from_slice(&std::fs::read(repo_dir.join("config")).unwrap()).unwrap();
    config.version = legacy.version();
    std::fs::write(repo_dir.join("config"), rmp_serde::to_vec(&config).unwrap()).unwrap();
    if let Some(dir) = crate::cache_paths::repo_cache_dir(&repo_id, Some(cache_dir))
        && dir.exists()
    {
        std::fs::remove_dir_all(dir).unwrap();
    }
    legacy
}

pub fn source_entry(path: &Path, label: &str) -> SourceEntry {
    SourceEntry {
        paths: vec![path.to_string_lossy().to_string()],
        label: label.to_string(),
        exclude: Vec::new(),
        exclude_if_present: Vec::new(),
        one_file_system: true,
        git_ignore: false,
        xattrs_enabled: false,
        hooks: SourceHooksConfig::default(),
        retention: None,
        repos: Vec::new(),
        command_dumps: Vec::new(),
    }
}

pub fn resolved_repo(config: VykarConfig, sources: Vec<SourceEntry>) -> ResolvedRepo {
    ResolvedRepo {
        label: None,
        config,
        global_hooks: HooksConfig::default(),
        repo_hooks: HooksConfig::default(),
        sources,
    }
}

pub fn open_local_repo(repo_dir: &Path) -> Repository {
    init_test_environment();
    let storage = Box::new(
        vykar_storage::local_backend::LocalBackend::new(repo_dir.to_str().unwrap()).unwrap(),
    );
    Repository::open(
        storage,
        None,
        None,
        crate::repo::OpenOptions::new().with_index(),
    )
    .unwrap()
}

/// Delete the local tree-chunk cache of the repository at `repo_dir`, so the
/// next item-stream read goes to storage (for tests whose point is a tree
/// read that fails).
pub fn clear_tree_cache(repo_dir: &Path) {
    let repo = open_local_repo(repo_dir);
    let root = repo
        .tree_cache()
        .expect("test repositories have a cache dir")
        .root()
        .to_path_buf();
    drop(repo);
    match std::fs::remove_dir_all(&root) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("cannot clear {}: {e}", root.display()),
    }
}

/// Load the on-disk `SnapshotListCache` for an unencrypted local repo without
/// going through `Repository::open` (which would heal the cache against the
/// remote `snapshots/` listing). Lets tests observe what delete/prune persisted
/// to the cache before any reopen.
pub fn load_snapshot_cache_from_disk(
    repo_dir: &Path,
) -> crate::repo::snapshot_cache::SnapshotListCache {
    use crate::repo::RepoFormat;
    use vykar_types::chunk_id::ChunkHasher;

    init_test_environment();
    let config_data = std::fs::read(repo_dir.join("config")).unwrap();
    let repo_config: crate::repo::RepoConfig = rmp_serde::from_slice(&config_data).unwrap();
    let format = RepoFormat::from_version(repo_config.version).unwrap();

    // Unencrypted repo: the chunk-ID key is derived from repo_id by the same
    // format-dependent helper `open()` uses.
    let key = crate::repo::derive_plaintext_chunk_id_key(&repo_config.id, format);
    let crypto = vykar_crypto::PlaintextEngine::new(ChunkHasher::new(format.chunk_hash(), key));

    crate::repo::snapshot_cache::SnapshotListCache::load(&repo_config.id, &crypto, None)
}

pub fn backup_single_source(
    config: &VykarConfig,
    source_dir: &Path,
    source_label: &str,
    snapshot_name: &str,
) -> SnapshotStats {
    try_backup_single_source(config, source_dir, source_label, snapshot_name)
        .unwrap()
        .stats
}

/// [`backup_single_source`] with a passphrase, for encrypted repositories.
pub fn backup_single_source_with(
    config: &VykarConfig,
    passphrase: Option<&str>,
    source_dir: &Path,
    source_label: &str,
    snapshot_name: &str,
) -> SnapshotStats {
    try_backup_with(config, passphrase, source_dir, source_label, snapshot_name)
        .unwrap()
        .stats
}

/// [`backup_single_source`], returning the backup's result.
pub fn try_backup_single_source(
    config: &VykarConfig,
    source_dir: &Path,
    source_label: &str,
    snapshot_name: &str,
) -> vykar_types::error::Result<commands::backup::BackupOutcome> {
    try_backup_with(config, None, source_dir, source_label, snapshot_name)
}

fn try_backup_with(
    config: &VykarConfig,
    passphrase: Option<&str>,
    source_dir: &Path,
    source_label: &str,
    snapshot_name: &str,
) -> vykar_types::error::Result<commands::backup::BackupOutcome> {
    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    commands::backup::run(
        config,
        commands::backup::BackupRequest {
            snapshot_name,
            passphrase,
            source_paths: &source_paths,
            source_label,
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
}

/// RAII guard that sets an env var and restores its previous value on drop.
///
/// Tests that touch process env must serialize themselves on `CWD_LOCK`; this
/// only handles the restore half. That keeps env-mutating tests apart, but a
/// concurrently running test that reads the environment without the lock can
/// still race with the mutation, the usual compromise in a threaded harness.
pub struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    pub fn set(key: &'static str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        // SAFETY: see the type docs; callers hold `CWD_LOCK`.
        #[allow(unsafe_code)]
        unsafe {
            std::env::set_var(key, val);
        }
        Self { key, prev }
    }

    pub fn unset(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        // SAFETY: see the type docs; callers hold `CWD_LOCK`.
        #[allow(unsafe_code)]
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: as in `set`; the guard drops before the caller's `CWD_LOCK`
        // guard, which is declared first.
        #[allow(unsafe_code)]
        unsafe {
            match &self.prev {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

/// A resolved repo identified only by URL and label — for selector tests, which
/// never open the repository.
pub fn make_test_repo(url: &str, label: Option<&str>) -> ResolvedRepo {
    ResolvedRepo {
        label: label.map(str::to_string),
        ..resolved_repo(
            VykarConfig {
                repository: RepositoryConfig {
                    url: url.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            Vec::new(),
        )
    }
}

/// A source entry with the values `normalize_sources` would produce for a
/// bare `sources: [/home/<label>]` entry.
pub fn make_test_source(label: &str) -> SourceEntry {
    SourceEntry {
        // Mirrors the serde defaults for a bare source: `one_file_system` off,
        // xattrs on. `source_entry` above chooses the opposite of both because
        // it feeds real backups in a sandbox.
        one_file_system: false,
        xattrs_enabled: true,
        ..source_entry(Path::new(&format!("/home/{label}")), label)
    }
}

/// Fails `get` of the keys `fail` selects with an injected storage error,
/// the first `times` times (`usize::MAX`: always).
pub(crate) struct FailGet {
    inner: std::sync::Arc<dyn vykar_storage::StorageBackend>,
    fail: Box<dyn Fn(&str) -> bool + Send + Sync>,
    times: std::sync::Mutex<usize>,
}

impl FailGet {
    pub(crate) fn new(
        inner: std::sync::Arc<dyn vykar_storage::StorageBackend>,
        fail: impl Fn(&str) -> bool + Send + Sync + 'static,
        times: usize,
    ) -> Self {
        Self {
            inner,
            fail: Box::new(fail),
            times: std::sync::Mutex::new(times),
        }
    }

    fn should_fail(&self, key: &str) -> bool {
        if !(self.fail)(key) {
            return false;
        }
        let mut times = self.times.lock().unwrap();
        if *times == 0 {
            return false;
        }
        if *times != usize::MAX {
            *times -= 1;
        }
        true
    }
}

impl vykar_storage::InnerBackend for FailGet {
    fn inner_backend(&self) -> &dyn vykar_storage::StorageBackend {
        &*self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for FailGet;
    except [get];

    fn get(&self, key: &str) -> vykar_types::error::Result<Option<Vec<u8>>> {
        if self.should_fail(key) {
            return Err(vykar_types::error::VykarError::Other(format!(
                "injected GET failure: {key}"
            )));
        }
        self.inner.get(key)
    }
}

/// Selects the PUTs a [`FaultyPut`] fails, by key and data.
pub(crate) type PutFilter = Box<dyn Fn(&str, &[u8]) -> bool + Send + Sync>;

/// Logs `put` and `delete` keys in call order and fails each `put` for
/// which `fail(key, data)` holds, without writing it. Batch deletes are
/// forwarded unlogged; on a backend without them the fallback's `delete`
/// calls are logged.
pub(crate) struct FaultyPut {
    pub inner: std::sync::Arc<dyn vykar_storage::StorageBackend>,
    pub fail: PutFilter,
    pub ops: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

/// A PUT of an index head.
pub(crate) fn is_head_put(key: &str, data: &[u8]) -> bool {
    key.starts_with(crate::repo::index_store::HEAD_PREFIX)
        && data.first() == Some(&(crate::repo::format::ObjectType::IndexHead as u8))
}

/// The file of the latest index head of the local repository at `repo_dir`.
pub(crate) fn latest_head_path(repo_dir: &Path) -> PathBuf {
    std::fs::read_dir(repo_dir.join("index.h"))
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            let generation = vykar_protocol::head_generation(&format!("index.h/{name}"))?;
            Some((generation, e.path()))
        })
        .max_by_key(|(g, _)| *g)
        .map(|(_, p)| p)
        .expect("an index head")
}

/// Delete every index head of the local repository at `repo_dir`.
pub(crate) fn remove_index_heads(repo_dir: &Path) {
    for entry in std::fs::read_dir(repo_dir.join("index.h"))
        .unwrap()
        .flatten()
    {
        std::fs::remove_file(entry.path()).unwrap();
    }
}

/// The key of the latest index head (`index.h/<gen>`).
pub(crate) fn latest_head_key(storage: &dyn vykar_storage::StorageBackend) -> String {
    let generation = *crate::repo::index_store::list_head_generations(storage)
        .unwrap()
        .last()
        .expect("an index head");
    crate::repo::index_store::head_key(generation)
}

/// A PUT of an index base object.
pub(crate) fn is_base_put(key: &str, data: &[u8]) -> bool {
    key.starts_with(crate::repo::index_store::INDEX_OBJECT_DIR)
        && data.first() == Some(&(crate::repo::format::ObjectType::IndexBase as u8))
}

impl vykar_storage::InnerBackend for FaultyPut {
    fn inner_backend(&self) -> &dyn vykar_storage::StorageBackend {
        &*self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for FaultyPut;
    except [put, delete, put_pack, begin_staged_put, conditional, batch_delete, profile];

    // Test wrapper: keeps every write on `put`.
    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

    // Keeps every delete on the logged `delete`.
    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        None
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        self.inner.profile()
    }

    // Packs and staged writes are not logged.
    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> vykar_types::error::Result<()> {
        self.inner.put_pack(key, data)
    }

    fn begin_staged_put(
        &self,
        prefix: &str,
    ) -> vykar_types::error::Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        self.inner.begin_staged_put(prefix)
    }

    fn put(&self, key: &str, data: &[u8]) -> vykar_types::error::Result<()> {
        self.ops.lock().unwrap().push(format!("put {key}"));
        if (self.fail)(key, data) {
            return Err(vykar_types::error::VykarError::Other(format!(
                "injected failure: put {key}"
            )));
        }
        self.inner.put(key, data)
    }

    fn delete(&self, key: &str) -> vykar_types::error::Result<()> {
        self.ops.lock().unwrap().push(format!("delete {key}"));
        self.inner.delete(key)
    }
}
