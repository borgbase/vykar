use std::path::Path;
use std::sync::Once;
use std::time::Duration;

use vykar_core::commands;
use vykar_core::compress::Compression;
use vykar_core::config::{
    ChunkerConfig, CommandDump, CompactConfig, CompressionConfig, EncryptionConfig,
    EncryptionModeConfig, RepositoryConfig, ResourceLimitsConfig, RetentionConfig, RetryConfig,
    ScheduleConfig, SourceEntry, SourceHooksConfig, VykarConfig, XattrsConfig,
};
use vykar_core::repo::lock;
use vykar_core::repo::{EncryptionMode, Repository};
use vykar_storage::local_backend::LocalBackend;
use vykar_types::error::VykarError;

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
            sftp_max_connections: None,
            access_token: None,
            allow_insecure_http: false,
            min_pack_size: 32 * 1024 * 1024,
            max_pack_size: 512 * 1024 * 1024,
            retry: RetryConfig::default(),
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
    }
}

fn source_entry(path: &Path, label: &str) -> SourceEntry {
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

fn open_local_repo(repo_dir: &Path, passphrase: Option<&str>) -> Repository {
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    Repository::open(storage, passphrase, None).unwrap()
}

fn backup_source(
    config: &VykarConfig,
    source_dir: &Path,
    source_label: &str,
    snapshot_name: &str,
    passphrase: Option<&str>,
) -> vykar_core::snapshot::SnapshotStats {
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
        },
    )
    .unwrap()
    .stats
}

#[test]
fn lifecycle_delete_compact_check_and_restore() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let mut config = make_test_config(&repo_dir);
    config.chunker = ChunkerConfig {
        min_size: 8 * 1024,
        avg_size: 16 * 1024,
        max_size: 64 * 1024,
    };

    let payload_v1: Vec<u8> = (0u32..512 * 1024).map(|i| (i % 251) as u8).collect();
    let payload_v2: Vec<u8> = (0u32..512 * 1024).map(|i| (i % 199) as u8).collect();
    std::fs::write(source_dir.join("data.bin"), &payload_v1).unwrap();

    commands::init::run(&config, None).unwrap();
    backup_source(&config, &source_dir, "src-a", "snap-v1", None);

    std::fs::write(source_dir.join("data.bin"), &payload_v2).unwrap();
    std::fs::write(source_dir.join("new.txt"), b"new file").unwrap();
    backup_source(&config, &source_dir, "src-a", "snap-v2", None);

    let delete_stats = commands::delete::run(&config, None, "snap-v1", false, None).unwrap();
    assert_eq!(delete_stats.snapshot_name, "snap-v1");
    assert!(delete_stats.chunks_deleted > 0);

    let compact_stats = commands::compact::run(&config, None, 0.0, None, false, None).unwrap();
    assert!(compact_stats.space_freed > 0);

    let check = commands::check::run(&config, None, true, false).unwrap();
    assert!(
        check.errors.is_empty(),
        "check errors: {:?}",
        check
            .errors
            .iter()
            .map(|e| format!("[{}] {}", e.context, e.message))
            .collect::<Vec<_>>()
    );

    let restore_dir = tmp.path().join("restore");
    let extract_stats = commands::restore::run(
        &config,
        None,
        "snap-v2",
        restore_dir.to_str().unwrap(),
        None,
        config.xattrs.enabled,
    )
    .unwrap();
    assert_eq!(extract_stats.files, 2);
    assert_eq!(
        std::fs::read(restore_dir.join("data.bin")).unwrap(),
        payload_v2
    );
    assert_eq!(
        std::fs::read_to_string(restore_dir.join("new.txt")).unwrap(),
        "new file"
    );

    let repo = open_local_repo(&repo_dir, None);
    assert!(repo.manifest().find_snapshot("snap-v1").is_none());
    assert!(repo.manifest().find_snapshot("snap-v2").is_some());
}

#[test]
fn prune_compact_check_and_restore_kept_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_a = tmp.path().join("source-a");
    let source_b = tmp.path().join("source-b");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_a).unwrap();
    std::fs::create_dir_all(&source_b).unwrap();

    std::fs::write(source_a.join("a.txt"), b"alpha-v1").unwrap();
    std::fs::write(source_b.join("b.txt"), b"bravo-v1").unwrap();

    let mut config = make_test_config(&repo_dir);
    config.retention = RetentionConfig {
        keep_last: Some(1),
        ..RetentionConfig::default()
    };

    commands::init::run(&config, None).unwrap();
    backup_source(&config, &source_a, "src-a", "snap-a1", None);
    std::thread::sleep(Duration::from_millis(2));
    backup_source(&config, &source_b, "src-b", "snap-b1", None);
    std::thread::sleep(Duration::from_millis(2));
    std::fs::write(source_a.join("a.txt"), b"alpha-v2").unwrap();
    backup_source(&config, &source_a, "src-a", "snap-a2", None);

    let sources = vec![
        source_entry(&source_a, "src-a"),
        source_entry(&source_b, "src-b"),
    ];
    let source_filter = vec!["src-a".to_string()];
    let (prune_stats, list_entries) =
        commands::prune::run(&config, None, false, true, &sources, &source_filter, None).unwrap();

    assert_eq!(prune_stats.pruned, 1);
    assert_eq!(prune_stats.kept, 1);
    assert!(list_entries.iter().any(|e| e.action == "prune"));
    assert!(list_entries.iter().any(|e| e.action == "keep"));

    let compact_stats = commands::compact::run(&config, None, 0.0, None, false, None).unwrap();
    assert!(
        compact_stats.packs_repacked > 0
            || compact_stats.packs_deleted_empty > 0
            || compact_stats.space_freed > 0
    );

    let check = commands::check::run(&config, None, true, false).unwrap();
    assert!(check.errors.is_empty());

    let repo = open_local_repo(&repo_dir, None);
    let names: Vec<_> = repo
        .manifest()
        .snapshots
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(repo.manifest().snapshots.len(), 2);
    assert!(!names.contains(&"snap-a1"));
    assert!(names.contains(&"snap-a2"));
    assert!(names.contains(&"snap-b1"));

    let restore_a = tmp.path().join("restore-a");
    commands::restore::run(
        &config,
        None,
        "snap-a2",
        restore_a.to_str().unwrap(),
        None,
        config.xattrs.enabled,
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(restore_a.join("a.txt")).unwrap(),
        "alpha-v2"
    );

    let restore_b = tmp.path().join("restore-b");
    commands::restore::run(
        &config,
        None,
        "snap-b1",
        restore_b.to_str().unwrap(),
        None,
        config.xattrs.enabled,
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(restore_b.join("b.txt")).unwrap(),
        "bravo-v1"
    );
}

fn run_encrypted_lifecycle(mode: EncryptionModeConfig, expected_mode: EncryptionMode) {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let payload: Vec<u8> = (0u32..512 * 1024).map(|i| (i % 241) as u8).collect();
    std::fs::write(source_dir.join("secret.bin"), &payload).unwrap();

    let passphrase = "correct-passphrase";
    let wrong_passphrase = "wrong-passphrase";

    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = mode;

    let repo = commands::init::run(&config, Some(passphrase)).unwrap();
    assert_eq!(repo.config.encryption, expected_mode);
    drop(repo);

    backup_source(
        &config,
        &source_dir,
        "encrypted-src",
        "snap-secret",
        Some(passphrase),
    );

    let check = commands::check::run(&config, Some(passphrase), true, false).unwrap();
    assert!(check.errors.is_empty());

    let restore_dir = tmp.path().join("restore");
    commands::restore::run(
        &config,
        Some(passphrase),
        "snap-secret",
        restore_dir.to_str().unwrap(),
        None,
        config.xattrs.enabled,
    )
    .unwrap();
    assert_eq!(
        std::fs::read(restore_dir.join("secret.bin")).unwrap(),
        payload
    );

    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    let wrong_open = Repository::open(storage, Some(wrong_passphrase), None);
    assert!(matches!(wrong_open, Err(VykarError::DecryptionFailed)));

    let wrong_extract = commands::restore::run(
        &config,
        Some(wrong_passphrase),
        "snap-secret",
        tmp.path().join("bad-restore").to_str().unwrap(),
        None,
        config.xattrs.enabled,
    );
    assert!(matches!(wrong_extract, Err(VykarError::DecryptionFailed)));

    let wrong_check = commands::check::run(&config, Some(wrong_passphrase), true, false);
    assert!(matches!(wrong_check, Err(VykarError::DecryptionFailed)));
}

#[test]
fn encrypted_aes256gcm_roundtrip_and_wrong_passphrase_failure() {
    run_encrypted_lifecycle(EncryptionModeConfig::Aes256Gcm, EncryptionMode::Aes256Gcm);
}

#[test]
fn encrypted_chacha20_poly1305_roundtrip_and_wrong_passphrase_failure() {
    run_encrypted_lifecycle(
        EncryptionModeConfig::Chacha20Poly1305,
        EncryptionMode::Chacha20Poly1305,
    );
}

#[test]
fn command_dump_failure_does_not_mutate_repository_state() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("stable.txt"), b"stable-data").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();
    backup_source(&config, &source_dir, "src-a", "snap-baseline", None);

    let before = open_local_repo(&repo_dir, None);
    let snapshots_before = before.manifest().snapshots.len();
    let chunks_before = before.chunk_index().len();
    drop(before);

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();
    let dumps = vec![CommandDump {
        name: "fail.txt".to_string(),
        command: "false".to_string(),
    }];

    let result = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-atomic-fail",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "src-a",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &dumps,
        },
    );
    assert!(result.is_err());

    let after = open_local_repo(&repo_dir, None);
    assert_eq!(after.manifest().snapshots.len(), snapshots_before);
    assert_eq!(after.chunk_index().len(), chunks_before);
    assert!(after.manifest().find_snapshot("snap-atomic-fail").is_none());
    assert!(after.manifest().find_snapshot("snap-baseline").is_some());
}

#[test]
fn backup_fails_when_repository_lock_is_held_by_another_process() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("locked.txt"), b"lock-test-data").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let storage = LocalBackend::new(repo_dir.to_str().unwrap()).unwrap();
    let guard = lock::acquire_lock(&storage).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let blocked = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-while-locked",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "src-a",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
        },
    );

    lock::release_lock(&storage, guard).unwrap();
    assert!(matches!(blocked, Err(VykarError::Locked(_))));

    backup_source(&config, &source_dir, "src-a", "snap-after-lock", None);
    let repo = open_local_repo(&repo_dir, None);
    assert!(repo.manifest().find_snapshot("snap-after-lock").is_some());
}

// ---------------------------------------------------------------------------
// Index-free restore via restore cache
// ---------------------------------------------------------------------------

#[test]
fn restore_loads_items_via_restore_cache_without_index() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"hello").unwrap();
    backup_source(&config, &source_dir, "src", "snap1", None);

    // Open repo WITHOUT loading the chunk index
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    let mut repo = Repository::open_without_index(storage, None, None).unwrap();

    // Restore cache should exist (built by save_state during backup)
    let cache = repo
        .open_restore_cache()
        .expect("restore cache should exist after backup");

    // Load items via cache — should succeed without loading full index
    let items =
        commands::list::load_snapshot_items_via_lookup(&mut repo, "snap1", |id| cache.lookup(id))
            .unwrap();

    assert!(!items.is_empty());
    assert!(items.iter().any(|i| i.path.contains("file.txt")));

    // The chunk index must still be empty — never loaded
    assert!(
        repo.chunk_index().is_empty(),
        "chunk index should not have been loaded"
    );
}

#[test]
fn restore_falls_back_to_index_on_cache_miss() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"hello").unwrap();
    backup_source(&config, &source_dir, "src", "snap1", None);

    // Read repo_id and index_generation
    let repo = open_local_repo(&repo_dir, None);
    let repo_id = repo.config.id.clone();
    let generation = repo.manifest().index_generation;
    drop(repo);

    // Overwrite restore cache: valid generation, but 0 entries.
    // open_restore_cache() will succeed, but every lookup() returns None,
    // triggering the ChunkNotInIndex fallback in restore.rs.
    let cache_path =
        vykar_core::index::dedup_cache::restore_cache_path(&repo_id, None).expect("cache path");
    let empty_index = vykar_core::index::ChunkIndex::new();
    vykar_core::index::dedup_cache::build_restore_cache_to_path(
        &empty_index,
        generation,
        &cache_path,
    )
    .unwrap();

    // Run full restore — must succeed via the fallback branch
    let restore_dir = tmp.path().join("restore");
    let stats = commands::restore::run(
        &config,
        None,
        "snap1",
        restore_dir.to_str().unwrap(),
        None,
        false,
    )
    .unwrap();

    assert_eq!(stats.files, 1);
    assert_eq!(
        std::fs::read_to_string(restore_dir.join("file.txt")).unwrap(),
        "hello"
    );
}

#[test]
fn restore_works_without_restore_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"hello").unwrap();
    backup_source(&config, &source_dir, "src", "snap1", None);

    // Delete the restore cache file
    let repo = open_local_repo(&repo_dir, None);
    let repo_id = repo.config.id.clone();
    drop(repo);
    let cache_path =
        vykar_core::index::dedup_cache::restore_cache_path(&repo_id, None).expect("cache path");
    let _ = std::fs::remove_file(&cache_path);

    // Full restore should still work via the no-cache path
    let restore_dir = tmp.path().join("restore");
    let stats = commands::restore::run(
        &config,
        None,
        "snap1",
        restore_dir.to_str().unwrap(),
        None,
        false,
    )
    .unwrap();

    assert_eq!(stats.files, 1);
    assert_eq!(
        std::fs::read_to_string(restore_dir.join("file.txt")).unwrap(),
        "hello"
    );
}
