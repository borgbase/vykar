//! End-to-end tests for regular-file source paths.
//!
//! Regression guard against the 0b10124 walker-unification bug where passing
//! a file (not a directory) as `source_paths[0]` caused `read_dir` to fail
//! with ENOTDIR. Covers initial backup, restore, and incremental re-backup.

use std::path::Path;
use std::sync::Once;

use vykar_core::commands;
use vykar_core::compress::Compression;
use vykar_core::config::{
    CheckConfig, ChunkerConfig, CompactConfig, CompressionConfig, EncryptionConfig,
    EncryptionModeConfig, RepositoryConfig, ResourceLimitsConfig, RetentionConfig, RetryConfig,
    ScheduleConfig, VykarConfig, XattrsConfig,
};

static TEST_ENV_INIT: Once = Once::new();

fn init_test_environment() {
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

fn make_test_config(repo_dir: &Path) -> VykarConfig {
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

fn backup_paths(
    config: &VykarConfig,
    source_paths: &[String],
    source_label: &str,
    snapshot_name: &str,
) {
    commands::backup::run(
        config,
        commands::backup::BackupRequest {
            snapshot_name,
            passphrase: None,
            source_paths,
            source_label,
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap();
}

#[test]
fn backup_and_restore_single_file_source() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let src_dir = tmp.path().join("src");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&src_dir).unwrap();

    let file = src_dir.join("hello.txt");
    std::fs::write(&file, b"world").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    // Regression check: backing up a regular-file source must NOT crash
    // with "Not a directory (os error 20)".
    let source_paths = vec![file.to_string_lossy().to_string()];
    backup_paths(&config, &source_paths, "hello", "snap-v1");

    let restore_dir = tmp.path().join("restore-v1");
    let stats = commands::restore::run(
        &config,
        None,
        "snap-v1",
        restore_dir.to_str().unwrap(),
        None,
        false,
        false,
    )
    .unwrap();
    assert_eq!(stats.files, 1);
    assert_eq!(
        std::fs::read(restore_dir.join("hello.txt")).unwrap(),
        b"world"
    );

    // Incremental re-backup — exercises the walker on a file source with a
    // parent snapshot in play (parent-reuse index inversion must handle the
    // empty-remainder case).
    std::fs::write(&file, b"world v2").unwrap();
    backup_paths(&config, &source_paths, "hello", "snap-v2");

    let restore_dir2 = tmp.path().join("restore-v2");
    commands::restore::run(
        &config,
        None,
        "snap-v2",
        restore_dir2.to_str().unwrap(),
        None,
        false,
        false,
    )
    .unwrap();
    assert_eq!(
        std::fs::read(restore_dir2.join("hello.txt")).unwrap(),
        b"world v2"
    );
}

#[test]
fn backup_mixed_file_and_directory_sources() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let dir = tmp.path().join("dir");
    let file = tmp.path().join("standalone.txt");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("inside.txt"), b"inside").unwrap();
    std::fs::write(&file, b"standalone").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![
        dir.to_string_lossy().to_string(),
        file.to_string_lossy().to_string(),
    ];
    backup_paths(&config, &source_paths, "mixed", "snap");

    let restore_dir = tmp.path().join("restore");
    commands::restore::run(
        &config,
        None,
        "snap",
        restore_dir.to_str().unwrap(),
        None,
        false,
        false,
    )
    .unwrap();

    assert_eq!(
        std::fs::read(restore_dir.join("dir/inside.txt")).unwrap(),
        b"inside"
    );
    assert_eq!(
        std::fs::read(restore_dir.join("standalone.txt")).unwrap(),
        b"standalone"
    );
}
