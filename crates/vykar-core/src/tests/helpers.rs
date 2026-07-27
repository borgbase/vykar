use std::path::Path;

use crate::commands;
use crate::compress::Compression;
use crate::config::{
    EncryptionConfig, EncryptionModeConfig, HooksConfig, RepositoryConfig, ResolvedRepo,
    SourceEntry, SourceHooksConfig, VykarConfig,
};
use crate::repo::Repository;
use crate::snapshot::SnapshotStats;

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

/// Load the on-disk `SnapshotListCache` for an unencrypted local repo without
/// going through `Repository::open` (which would heal the cache against the
/// remote `snapshots/` listing). Lets tests observe what delete/prune persisted
/// to the cache before any reopen.
pub fn load_snapshot_cache_from_disk(
    repo_dir: &Path,
) -> crate::repo::snapshot_cache::SnapshotListCache {
    use blake2::digest::{Update, VariableOutput};
    use blake2::Blake2bVar;

    init_test_environment();
    let config_data = std::fs::read(repo_dir.join("config")).unwrap();
    let repo_config: crate::repo::RepoConfig = rmp_serde::from_slice(&config_data).unwrap();

    // Unencrypted repo: chunk_id_key = BLAKE2b(repo_id) (see open.rs).
    let mut key = [0u8; 32];
    let mut hasher = Blake2bVar::new(32).unwrap();
    hasher.update(&repo_config.id);
    hasher.finalize_variable(&mut key).unwrap();
    let crypto = vykar_crypto::PlaintextEngine::new(&key);

    crate::repo::snapshot_cache::SnapshotListCache::load(&repo_config.id, &crypto, None)
}

pub fn backup_single_source(
    config: &VykarConfig,
    source_dir: &Path,
    source_label: &str,
    snapshot_name: &str,
) -> SnapshotStats {
    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    commands::backup::run(
        config,
        commands::backup::BackupRequest {
            snapshot_name,
            passphrase: None,
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
    .unwrap()
    .stats
}

/// RAII guard that sets an env var and restores its previous value on drop.
///
/// Tests that touch process env must serialize themselves; this only handles
/// the restore half.
pub struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    pub fn set(key: &'static str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, val);
        Self { key, prev }
    }

    pub fn unset(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::remove_var(key);
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
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
