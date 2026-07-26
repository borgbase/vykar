//! Fixtures shared by the `tests/*.rs` integration binaries.
//!
//! Each binary compiles this module independently and uses only part of it,
//! hence the blanket `dead_code` allow.

#![allow(dead_code)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
// Test-only env mutation; SAFETY per block.
#![allow(unsafe_code)]

use std::path::Path;
use std::sync::Once;

use vykar_core::commands;
use vykar_core::compress::Compression;
use vykar_core::config::{
    CheckConfig, ChunkerConfig, CompactConfig, CompressionConfig, EncryptionConfig,
    EncryptionModeConfig, RepositoryConfig, ResourceLimitsConfig, RetentionConfig, RetryConfig,
    ScheduleConfig, SourceEntry, SourceHooksConfig, VykarConfig, XattrsConfig,
};
use vykar_core::repo::{OpenOptions, Repository};
use vykar_core::snapshot::SnapshotStats;
use vykar_storage::local_backend::LocalBackend;

static TEST_ENV_INIT: Once = Once::new();

/// Point `HOME`/`XDG_CACHE_HOME` at a per-process temp directory so tests never
/// touch the developer's real cache. Idempotent.
pub fn init_test_environment() {
    TEST_ENV_INIT.call_once(|| {
        let base = std::env::temp_dir().join(format!("vykar-tests-{}", std::process::id()));
        let home = base.join("home");
        let cache = base.join("cache");
        let _ = std::fs::create_dir_all(&home);
        let _ = std::fs::create_dir_all(&cache);
        // SAFETY: Once::call_once runs this single-threaded at test-process
        // startup before any threads are spawned.
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("XDG_CACHE_HOME", &cache);
        }
    });
}

/// Plaintext, local-backend config rooted at `repo_dir`.
pub fn make_test_config(repo_dir: &Path) -> VykarConfig {
    init_test_environment();

    VykarConfig {
        repository: RepositoryConfig {
            url: repo_dir.to_string_lossy().to_string(),
            region: None,
            access_key_id: None,
            secret_access_key: None,
            sftp_key: None,
            sftp_known_hosts: None,
            sftp_timeout: None,
            access_token: None,
            allow_insecure_http: false,
            min_pack_size: 32 * 1024 * 1024,
            max_pack_size: 512 * 1024 * 1024,
            retry: RetryConfig::default(),
            s3_soft_delete: false,
        },
        encryption: EncryptionConfig {
            mode: EncryptionModeConfig::None,
            passphrase: None,
            passcommand: None,
        },
        exclude_patterns: Vec::new(),
        exclude_if_present: Vec::new(),
        one_file_system: true,
        git_ignore: false,
        chunker: ChunkerConfig::default(),
        compression: CompressionConfig::default(),
        retention: RetentionConfig::default(),
        xattrs: XattrsConfig::default(),
        schedule: ScheduleConfig::default(),
        limits: ResourceLimitsConfig::default(),
        compact: CompactConfig::default(),
        check: CheckConfig::default(),
        cache_dir: None,
        trust_repo: false,
        hostname_override: None,
    }
}

/// Open the repository at `repo_dir` with the chunk index loaded.
pub fn open_local_repo(repo_dir: &Path, passphrase: Option<&str>) -> Repository {
    init_test_environment();
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    Repository::open(storage, passphrase, None, OpenOptions::new().with_index()).unwrap()
}

/// Like [`open_local_repo`], but also enables the local file cache. Tests that
/// exercise snapshot-cache behaviour must use the uncached variant.
pub fn open_local_repo_cached(repo_dir: &Path, passphrase: Option<&str>) -> Repository {
    init_test_environment();
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    Repository::open(
        storage,
        passphrase,
        None,
        OpenOptions::new().with_index().with_file_cache(),
    )
    .unwrap()
}

/// Back up `source_dir` under `snapshot_name` with the standard test request.
pub fn backup_source(
    config: &VykarConfig,
    source_dir: &Path,
    source_label: &str,
    snapshot_name: &str,
    passphrase: Option<&str>,
    xattrs_enabled: bool,
) -> SnapshotStats {
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
            xattrs_enabled,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap()
    .stats
}

/// Single-path source entry with default hooks and no excludes.
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
