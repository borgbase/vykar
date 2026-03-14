use std::path::Path;
use std::sync::Once;

use crate::commands;
use crate::compress::Compression;
use crate::config::{
    ChunkerConfig, CompactConfig, CompressionConfig, EncryptionConfig, EncryptionModeConfig,
    HooksConfig, RepositoryConfig, ResolvedRepo, ResourceLimitsConfig, RetentionConfig,
    RetryConfig, ScheduleConfig, SourceEntry, SourceHooksConfig, VykarConfig, XattrsConfig,
};
use crate::repo::Repository;
use crate::snapshot::SnapshotStats;

static TEST_ENV_INIT: Once = Once::new();

pub fn init_test_environment() {
    TEST_ENV_INIT.call_once(|| {
        let base = std::env::temp_dir().join(format!("vykar-tests-{}", std::process::id()));
        let home = base.join("home");
        let cache = base.join("cache");
        let _ = std::fs::create_dir_all(&home);
        let _ = std::fs::create_dir_all(&cache);
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("XDG_CACHE_HOME", &cache);
        }
    });
}

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
        cache_dir: None,
        trust_repo: false,
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
    Repository::open(storage, None, None).unwrap()
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
