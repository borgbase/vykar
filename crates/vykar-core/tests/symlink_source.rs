//! End-to-end test for a symlink-to-directory source.
//!
//! Matches the pre-0b10124 behaviour of the `ignore`-crate walker: a symlink
//! source has its target descended transparently. This test guards against
//! future regressions where the walker would either refuse or treat the
//! symlink itself as a single entry.

#![cfg(unix)]

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

#[test]
fn backup_symlink_to_directory_descends_target() {
    use std::os::unix::fs as unix_fs;

    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let real_dir = tmp.path().join("real-docs");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir(&real_dir).unwrap();
    std::fs::write(real_dir.join("note.txt"), b"hi").unwrap();
    std::fs::create_dir(real_dir.join("sub")).unwrap();
    std::fs::write(real_dir.join("sub/inner.txt"), b"inner").unwrap();

    // Symlink `docs -> real-docs`. Configured source is the symlink.
    let link = tmp.path().join("docs");
    unix_fs::symlink(&real_dir, &link).unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![link.to_string_lossy().to_string()];
    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "docs",
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

    let restore_dir = tmp.path().join("restore");
    commands::restore::run(
        &config,
        None,
        "snap",
        restore_dir.to_str().unwrap(),
        None,
        false,
    )
    .unwrap();

    // Symlink source with a single configured path → SkipRoot: descendants
    // restore directly into the restore dir, matching pre-regression behavior.
    assert_eq!(std::fs::read(restore_dir.join("note.txt")).unwrap(), b"hi");
    assert_eq!(
        std::fs::read(restore_dir.join("sub/inner.txt")).unwrap(),
        b"inner"
    );
}
