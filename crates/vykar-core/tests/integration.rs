use vykar_core::commands;
use vykar_core::compress::Compression;
use vykar_core::config::{
    ChunkerConfig, CompactConfig, CompressionConfig, EncryptionConfig, EncryptionModeConfig,
    RepositoryConfig, ResourceLimitsConfig, RetentionConfig, RetryConfig, ScheduleConfig,
    VykarConfig, XattrsConfig,
};
use vykar_core::repo::pack::PackType;
use vykar_core::repo::{EncryptionMode, Repository};
use vykar_core::snapshot::item::ItemType;
use vykar_storage::local_backend::LocalBackend;

static TEST_ENV_INIT: std::sync::Once = std::sync::Once::new();

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

fn init_local_repo(dir: &std::path::Path) -> Repository {
    init_test_environment();
    let storage = Box::new(LocalBackend::new(dir.to_str().unwrap()).unwrap());
    let mut repo = Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        None,
    )
    .unwrap();
    repo.begin_write_session().unwrap();
    repo
}

fn open_local_repo(dir: &std::path::Path) -> Repository {
    init_test_environment();
    let storage = Box::new(LocalBackend::new(dir.to_str().unwrap()).unwrap());
    Repository::open(storage, None, None).unwrap()
}

fn make_test_config(repo_dir: &std::path::Path) -> VykarConfig {
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
        hostname_override: None,
    }
}

fn xattr_test_name() -> &'static str {
    "user.vykar.test"
}

fn supports_xattrs(dir: &std::path::Path) -> bool {
    let probe = dir.join(".xattr-probe");
    if std::fs::write(&probe, b"probe").is_err() {
        return false;
    }

    let name = xattr_test_name();
    let supported = match xattr::set(&probe, name, b"1") {
        Ok(()) => true,
        Err(_) => false,
    };
    let _ = xattr::remove(&probe, name);
    let _ = std::fs::remove_file(&probe);
    supported
}

#[test]
fn init_store_reopen_read() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    // Init and store chunks
    let data1 = b"chunk one data for integration test";
    let data2 = b"chunk two data for integration test";
    let (id1, id2) = {
        let mut repo = init_local_repo(dir);
        let (id1, _, _) = repo
            .store_chunk(data1, Compression::Lz4, PackType::Data)
            .unwrap();
        let (id2, _, _) = repo
            .store_chunk(data2, Compression::Lz4, PackType::Data)
            .unwrap();
        repo.save_state().unwrap();
        (id1, id2)
    };

    // Reopen and verify
    let mut repo = open_local_repo(dir);
    assert_eq!(repo.chunk_index().len(), 2);
    let read1 = repo.read_chunk(&id1).unwrap();
    let read2 = repo.read_chunk(&id2).unwrap();
    assert_eq!(read1, data1);
    assert_eq!(read2, data2);
}

#[test]
fn snapshot_list_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"reopen-test").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_patterns: Vec<String> = Vec::new();
    let exclude_if_present: Vec<String> = Vec::new();

    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "test-snapshot",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap();

    // Reopen and check snapshot list
    let repo = open_local_repo(&repo_dir);
    assert_eq!(repo.manifest().snapshots.len(), 1);
    assert_eq!(repo.manifest().snapshots[0].name, "test-snapshot");
}

#[test]
fn init_auto_mode_persists_concrete_encryption_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();

    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Auto;

    let repo = commands::init::run(&config, Some("test-passphrase")).unwrap();
    let selected = repo.config.encryption.clone();
    assert!(matches!(
        selected,
        EncryptionMode::Aes256Gcm | EncryptionMode::Chacha20Poly1305
    ));
    drop(repo);

    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    let reopened = Repository::open(storage, Some("test-passphrase"), None).unwrap();
    assert_eq!(reopened.config.encryption, selected);
}

#[test]
fn backup_exclude_if_present_skips_marked_directories() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(source_dir.join("keep")).unwrap();
    std::fs::create_dir_all(source_dir.join("skip")).unwrap();
    std::fs::write(source_dir.join("keep").join("keep.txt"), b"keep").unwrap();
    std::fs::write(source_dir.join("skip").join("skip.txt"), b"skip").unwrap();
    std::fs::write(source_dir.join("skip").join(".nobackup"), b"").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present = vec![".nobackup".to_string()];
    let exclude_patterns: Vec<String> = Vec::new();

    let stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-marker",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
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
    .stats;

    assert_eq!(stats.nfiles, 1);
}

#[test]
fn backup_git_ignore_respected_when_enabled() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(source_dir.join("target")).unwrap();
    std::fs::write(source_dir.join(".gitignore"), b"target/\n").unwrap();
    std::fs::write(source_dir.join("keep.txt"), b"keep").unwrap();
    std::fs::write(source_dir.join("target").join("ignored.txt"), b"ignore me").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let stats_without_gitignore = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-no-gitignore",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
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
    .stats;

    let stats_with_gitignore = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-with-gitignore",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: true,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap()
    .stats;

    assert_eq!(stats_without_gitignore.nfiles, 3);
    assert_eq!(stats_with_gitignore.nfiles, 2);
}

#[test]
fn backup_deduplicates_identical_files_and_extracts_correctly() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let payload: Vec<u8> = (0u32..512 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(source_dir.join("a.bin"), &payload).unwrap();
    std::fs::write(source_dir.join("b.bin"), &payload).unwrap();

    let mut config = make_test_config(&repo_dir);
    config.chunker = ChunkerConfig {
        min_size: 8 * 1024,
        avg_size: 16 * 1024,
        max_size: 64 * 1024,
    };

    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();
    let stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-dedup",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
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
    .stats;

    assert_eq!(stats.nfiles, 2);
    assert!(stats.deduplicated_size > 0);
    assert!(stats.deduplicated_size < stats.compressed_size);

    let mut repo = open_local_repo(&repo_dir);
    let items = commands::list::load_snapshot_items(&mut repo, "snap-dedup").unwrap();
    let file_items: Vec<_> = items
        .iter()
        .filter(|item| item.entry_type == ItemType::RegularFile)
        .collect();
    assert_eq!(file_items.len(), 2);

    let first_ids: Vec<_> = file_items[0].chunks.iter().map(|c| c.id).collect();
    let second_ids: Vec<_> = file_items[1].chunks.iter().map(|c| c.id).collect();
    assert!(!first_ids.is_empty());
    assert_eq!(first_ids, second_ids);

    for chunk_id in first_ids {
        let entry = repo.chunk_index().get(&chunk_id).unwrap();
        assert_eq!(entry.refcount, 2);
    }

    let restore_dir = tmp.path().join("restore");
    let extract_stats = commands::restore::run(
        &config,
        None,
        "snap-dedup",
        restore_dir.to_str().unwrap(),
        None,
        config.xattrs.enabled,
    )
    .unwrap();
    assert_eq!(extract_stats.files, 2);

    assert_eq!(std::fs::read(restore_dir.join("a.bin")).unwrap(), payload);
    assert_eq!(std::fs::read(restore_dir.join("b.bin")).unwrap(), payload);
}

#[test]
fn backup_run_with_progress_emits_events_and_final_stats() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("one.txt"), b"one").unwrap();
    std::fs::write(source_dir.join("two.txt"), b"two").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let mut events = Vec::new();
    let mut on_progress = |event| events.push(event);

    let stats = commands::backup::run_with_progress(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-progress",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
        Some(&mut on_progress),
        None,
    )
    .unwrap()
    .stats;

    let file_started_count = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                commands::backup::BackupProgressEvent::FileStarted { .. }
            )
        })
        .count();
    assert_eq!(file_started_count, 2);

    let final_stats_event = events
        .iter()
        .rev()
        .find_map(|event| match event {
            commands::backup::BackupProgressEvent::StatsUpdated {
                nfiles,
                original_size,
                compressed_size,
                deduplicated_size,
                ..
            } => Some((
                *nfiles,
                *original_size,
                *compressed_size,
                *deduplicated_size,
            )),
            _ => None,
        })
        .expect("expected at least one StatsUpdated event");

    assert_eq!(final_stats_event.0, stats.nfiles);
    assert_eq!(final_stats_event.1, stats.original_size);
    assert_eq!(final_stats_event.2, stats.compressed_size);
    assert_eq!(final_stats_event.3, stats.deduplicated_size);
}

#[test]
fn backup_and_restore_preserves_file_xattrs_when_enabled() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    if !supports_xattrs(&source_dir) {
        return;
    }

    let source_file = source_dir.join("file.txt");
    std::fs::write(&source_file, b"hello xattrs").unwrap();

    let attr_name = xattr_test_name();
    let attr_value = b"vykar-value".to_vec();
    xattr::set(&source_file, attr_name, &attr_value).unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-xattrs",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: true,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap();

    let mut repo = open_local_repo(&repo_dir);
    let items = commands::list::load_snapshot_items(&mut repo, "snap-xattrs").unwrap();
    let item = items.iter().find(|i| i.path == "file.txt").unwrap();
    let stored = item
        .xattrs
        .as_ref()
        .and_then(|map| map.get(attr_name))
        .cloned();
    assert_eq!(stored, Some(attr_value.clone()));

    let restore_dir = tmp.path().join("restore");
    commands::restore::run(
        &config,
        None,
        "snap-xattrs",
        restore_dir.to_str().unwrap(),
        None,
        true,
    )
    .unwrap();

    let restored_file = restore_dir.join("file.txt");
    let restored = xattr::get(&restored_file, attr_name).unwrap();
    assert_eq!(restored, Some(attr_value));
}

#[test]
fn backup_skips_xattrs_when_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    if !supports_xattrs(&source_dir) {
        return;
    }

    let source_file = source_dir.join("file.txt");
    std::fs::write(&source_file, b"hello xattrs").unwrap();

    let attr_name = xattr_test_name();
    xattr::set(&source_file, attr_name, b"vykar-value").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-no-xattrs",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap();

    let mut repo = open_local_repo(&repo_dir);
    let items = commands::list::load_snapshot_items(&mut repo, "snap-no-xattrs").unwrap();
    let item = items.iter().find(|i| i.path == "file.txt").unwrap();
    assert!(item.xattrs.is_none());
}

#[test]
fn file_cache_persists_and_matches_snapshot_items() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let payload_a: Vec<u8> = (0u32..256 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(source_dir.join("a.bin"), &payload_a).unwrap();
    std::fs::write(source_dir.join("b.bin"), b"small file").unwrap();

    let mut config = make_test_config(&repo_dir);
    config.chunker = ChunkerConfig {
        min_size: 8 * 1024,
        avg_size: 16 * 1024,
        max_size: 64 * 1024,
    };

    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    // First backup — populates the file cache.
    let stats1 = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-1",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
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
    .stats;
    assert_eq!(stats1.nfiles, 2);
    assert!(
        stats1.deduplicated_size > 0,
        "first backup should store new data"
    );

    // Verify file cache was persisted and its chunk_refs match the snapshot.
    {
        let mut repo = open_local_repo(&repo_dir);
        let items = commands::list::load_snapshot_items(&mut repo, "snap-1").unwrap();
        let files: Vec<_> = items
            .iter()
            .filter(|i| i.entry_type == ItemType::RegularFile)
            .collect();
        assert_eq!(files.len(), 2);

        // Cache keys are canonicalized (matches walker behavior).
        let canonical_source = std::fs::canonicalize(&source_dir).unwrap();
        let canonical_roots = vec![canonical_source.to_string_lossy().to_string()];

        // Set up the active section for lookup (keyed by canonical roots).
        repo.file_cache_mut()
            .activate_for_walk_roots(&canonical_roots);

        // Each file's cache entry should be findable via lookup with matching metadata.
        for file_item in &files {
            let abs_path = canonical_source.join(&file_item.path);
            let abs_str = abs_path.to_str().unwrap();
            let meta = std::fs::symlink_metadata(&abs_path).unwrap();
            let ft = meta.file_type();
            let ms = vykar_core::platform::fs::summarize_metadata(&meta, &ft);
            let cached_refs = repo
                .file_cache()
                .lookup(
                    abs_str,
                    ms.device,
                    ms.inode,
                    ms.mtime_ns,
                    ms.ctime_ns,
                    ms.size,
                )
                .unwrap_or_else(|| panic!("cache should have entry for {}", file_item.path));
            let cached_ids: Vec<_> = cached_refs.iter().map(|c| c.id).collect();
            let snap_ids: Vec<_> = file_item.chunks.iter().map(|c| c.id).collect();
            assert_eq!(
                cached_ids, snap_ids,
                "cache chunk_refs should match snapshot for {}",
                file_item.path
            );
        }
    }

    // Second backup — unchanged files. Produces identical snapshot.
    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-2",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
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
    .unwrap();

    let mut repo = open_local_repo(&repo_dir);
    let items1 = commands::list::load_snapshot_items(&mut repo, "snap-1").unwrap();
    let items2 = commands::list::load_snapshot_items(&mut repo, "snap-2").unwrap();
    let files1: Vec<_> = items1
        .iter()
        .filter(|i| i.entry_type == ItemType::RegularFile)
        .collect();
    let files2: Vec<_> = items2
        .iter()
        .filter(|i| i.entry_type == ItemType::RegularFile)
        .collect();
    for (f1, f2) in files1.iter().zip(files2.iter()) {
        let ids1: Vec<_> = f1.chunks.iter().map(|c| c.id).collect();
        let ids2: Vec<_> = f2.chunks.iter().map(|c| c.id).collect();
        assert_eq!(
            ids1, ids2,
            "unchanged file {} should have same chunks",
            f1.path
        );
    }

    // Every chunk should have refcount 2 (one per snapshot).
    for file_item in &files1 {
        for cr in &file_item.chunks {
            let entry = repo.chunk_index().get(&cr.id).unwrap();
            assert_eq!(entry.refcount, 2, "chunk {} refcount", cr.id);
        }
    }
}

#[test]
fn file_cache_misses_on_modified_file() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let payload: Vec<u8> = (0u32..256 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(source_dir.join("unchanged.bin"), &payload).unwrap();
    std::fs::write(source_dir.join("modified.bin"), &payload).unwrap();

    let mut config = make_test_config(&repo_dir);
    config.chunker = ChunkerConfig {
        min_size: 8 * 1024,
        avg_size: 16 * 1024,
        max_size: 64 * 1024,
    };

    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    // First backup.
    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-1",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
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
    .unwrap();

    // Collect chunk IDs from the first snapshot.
    let snap1_chunks: std::collections::HashMap<String, Vec<_>> = {
        let mut repo = open_local_repo(&repo_dir);
        let items = commands::list::load_snapshot_items(&mut repo, "snap-1").unwrap();
        items
            .iter()
            .filter(|i| i.entry_type == ItemType::RegularFile)
            .map(|i| (i.path.clone(), i.chunks.iter().map(|c| c.id).collect()))
            .collect()
    };

    // Modify one file with completely different content.
    let new_payload: Vec<u8> = (0u32..256 * 1024).map(|i| (i % 199) as u8).collect();
    std::fs::write(source_dir.join("modified.bin"), &new_payload).unwrap();

    // Second backup — cache should miss on modified.bin, hit on unchanged.bin.
    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-2",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
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
    .unwrap();

    let mut repo = open_local_repo(&repo_dir);
    let items2 = commands::list::load_snapshot_items(&mut repo, "snap-2").unwrap();
    let files2: std::collections::HashMap<String, Vec<_>> = items2
        .iter()
        .filter(|i| i.entry_type == ItemType::RegularFile)
        .map(|i| (i.path.clone(), i.chunks.iter().map(|c| c.id).collect()))
        .collect();

    // unchanged.bin should have the same chunks.
    assert_eq!(
        snap1_chunks["unchanged.bin"], files2["unchanged.bin"],
        "unchanged file should keep the same chunks"
    );

    // modified.bin should have different chunks.
    assert_ne!(
        snap1_chunks["modified.bin"], files2["modified.bin"],
        "modified file should have new chunks"
    );

    // The cache should now reflect the new content for modified.bin.
    // Cache keys are canonicalized (matches walker behavior).
    let canonical_source = std::fs::canonicalize(&source_dir).unwrap();
    let canonical_roots = vec![canonical_source.to_string_lossy().to_string()];
    repo.file_cache_mut()
        .activate_for_walk_roots(&canonical_roots);
    let abs_modified = canonical_source.join("modified.bin");
    let abs_str = abs_modified.to_str().unwrap();
    let meta = std::fs::symlink_metadata(&abs_modified).unwrap();
    let ft = meta.file_type();
    let ms = vykar_core::platform::fs::summarize_metadata(&meta, &ft);
    let cached_refs = repo
        .file_cache()
        .lookup(
            abs_str,
            ms.device,
            ms.inode,
            ms.mtime_ns,
            ms.ctime_ns,
            ms.size,
        )
        .expect("cache should have entry for modified.bin after re-backup");
    let cached_ids: Vec<_> = cached_refs.iter().map(|c| c.id).collect();
    assert_eq!(
        cached_ids, files2["modified.bin"],
        "cache should be updated with new chunks for modified.bin"
    );
}

#[test]
fn info_reports_repository_statistics() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let payload: Vec<u8> = (0u32..256 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(source_dir.join("a.bin"), &payload).unwrap();
    std::fs::write(source_dir.join("b.bin"), &payload).unwrap();

    let mut config = make_test_config(&repo_dir);
    config.chunker = ChunkerConfig {
        min_size: 8 * 1024,
        avg_size: 16 * 1024,
        max_size: 64 * 1024,
    };

    commands::init::run(&config, None).unwrap();

    let empty = commands::info::run(&config, None).unwrap();
    assert_eq!(empty.snapshot_count, 0);
    assert!(empty.last_snapshot_time.is_none());
    assert_eq!(empty.raw_size, 0);
    assert_eq!(empty.compressed_size, 0);
    assert_eq!(empty.deduplicated_size, 0);
    assert_eq!(empty.unique_stored_size, 0);
    assert_eq!(empty.referenced_stored_size, 0);
    assert_eq!(empty.unique_chunks, 0);

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();
    let backup_stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-info",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
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
    .stats;

    let info = commands::info::run(&config, None).unwrap();
    assert_eq!(info.snapshot_count, 1);
    assert!(info.last_snapshot_time.is_some());
    assert_eq!(info.raw_size, backup_stats.original_size);
    assert_eq!(info.compressed_size, backup_stats.compressed_size);
    assert_eq!(info.deduplicated_size, backup_stats.deduplicated_size);
    assert!(info.unique_chunks > 0);
    assert!(info.unique_stored_size > 0);
    assert!(info.referenced_stored_size >= info.unique_stored_size);
}

#[test]
fn command_dump_backup_and_restore() {
    let repo_dir = tempfile::tempdir().unwrap();
    init_local_repo(repo_dir.path());
    let config = make_test_config(repo_dir.path());

    let dumps = vec![vykar_core::config::CommandDump {
        name: "hello.txt".to_string(),
        command: "echo hello world".to_string(),
    }];

    // Backup with command dumps only (no source paths)
    let source_paths: Vec<String> = Vec::new();
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-dumps",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "dumps",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &dumps,
            verbose: false,
        },
    )
    .unwrap()
    .stats;

    assert_eq!(stats.nfiles, 1);
    assert!(stats.original_size > 0);

    // List snapshot contents — verify .vykar-dumps/hello.txt appears
    let mut repo = open_local_repo(repo_dir.path());
    let items = commands::list::load_snapshot_items(&mut repo, "snap-dumps").unwrap();
    let dump_items: Vec<_> = items
        .iter()
        .filter(|i| i.path == ".vykar-dumps/hello.txt")
        .collect();
    assert_eq!(dump_items.len(), 1);
    assert_eq!(dump_items[0].entry_type, ItemType::RegularFile);
    assert_eq!(dump_items[0].size, 12); // "hello world\n"

    // Verify the .vykar-dumps directory item exists
    let dir_items: Vec<_> = items.iter().filter(|i| i.path == ".vykar-dumps").collect();
    assert_eq!(dir_items.len(), 1);
    assert_eq!(dir_items[0].entry_type, ItemType::Directory);

    // Extract and verify file contents
    let extract_dir = tempfile::tempdir().unwrap();
    commands::restore::run(
        &config,
        None,
        "snap-dumps",
        extract_dir.path().to_str().unwrap(),
        None,
        false,
    )
    .unwrap();

    let dump_file = extract_dir.path().join(".vykar-dumps/hello.txt");
    assert!(dump_file.exists(), "dump file should exist after restore");
    let contents = std::fs::read_to_string(&dump_file).unwrap();
    assert_eq!(contents, "hello world\n");
}

#[test]
fn command_dump_failing_command_aborts_backup() {
    let repo_dir = tempfile::tempdir().unwrap();
    init_local_repo(repo_dir.path());
    let config = make_test_config(repo_dir.path());

    let dumps = vec![vykar_core::config::CommandDump {
        name: "fail.txt".to_string(),
        command: "false".to_string(),
    }];

    let source_paths: Vec<String> = Vec::new();
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let result = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-fail",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "dumps",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &dumps,
            verbose: false,
        },
    );

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("command_dump 'fail.txt' failed"),
        "unexpected error: {err_msg}"
    );
}

#[test]
fn command_dump_mixed_with_files() {
    let repo_dir = tempfile::tempdir().unwrap();
    init_local_repo(repo_dir.path());
    let config = make_test_config(repo_dir.path());

    // Create a source directory with a regular file
    let source_dir = tempfile::tempdir().unwrap();
    std::fs::write(source_dir.path().join("real.txt"), "real file\n").unwrap();

    let dumps = vec![vykar_core::config::CommandDump {
        name: "dump.txt".to_string(),
        command: "echo dump output".to_string(),
    }];

    let source_paths = vec![source_dir.path().to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-mixed",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "mixed",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &dumps,
            verbose: false,
        },
    )
    .unwrap()
    .stats;

    // Should have both the real file and the dump
    assert_eq!(stats.nfiles, 2);

    let mut repo = open_local_repo(repo_dir.path());
    let items = commands::list::load_snapshot_items(&mut repo, "snap-mixed").unwrap();
    let has_real = items.iter().any(|i| i.path == "real.txt");
    let has_dump = items.iter().any(|i| i.path == ".vykar-dumps/dump.txt");
    assert!(has_real, "should contain real.txt");
    assert!(has_dump, "should contain .vykar-dumps/dump.txt");

    // Extract and verify both files
    let extract_dir = tempfile::tempdir().unwrap();
    commands::restore::run(
        &config,
        None,
        "snap-mixed",
        extract_dir.path().to_str().unwrap(),
        None,
        false,
    )
    .unwrap();

    let real_contents = std::fs::read_to_string(extract_dir.path().join("real.txt")).unwrap();
    assert_eq!(real_contents, "real file\n");

    let dump_contents =
        std::fs::read_to_string(extract_dir.path().join(".vykar-dumps/dump.txt")).unwrap();
    assert_eq!(dump_contents, "dump output\n");
}

/// Backup 500 small files (1 KiB each) + 1 large file, verify roundtrip.
/// Tests both pipeline and sequential paths via two backups with different configs.
#[test]
fn backup_many_small_files_plus_large_file_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    // Create 500 small files (1 KiB each) with unique content.
    for i in 0..500 {
        let content: Vec<u8> = (0..1024).map(|j| ((i * 7 + j * 13) % 251) as u8).collect();
        std::fs::write(source_dir.join(format!("small_{i:04}.bin")), &content).unwrap();
    }

    // Create 1 large file (256 KiB).
    let large_payload: Vec<u8> = (0u32..256 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(source_dir.join("large.bin"), &large_payload).unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    // First backup (pipeline path — default pipeline_depth > 0).
    let stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-small-1",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::Lz4,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap()
    .stats;

    assert_eq!(stats.nfiles, 501);
    assert!(stats.original_size > 0);

    // Verify all items present in the snapshot.
    {
        let mut repo = open_local_repo(&repo_dir);
        let items = commands::list::load_snapshot_items(&mut repo, "snap-small-1").unwrap();
        let files: Vec<_> = items
            .iter()
            .filter(|i| i.entry_type == ItemType::RegularFile)
            .collect();
        assert_eq!(files.len(), 501, "all files should be in snapshot");

        // Walk order depends on filesystem (inode order on ext4/xfs, filename
        // order elsewhere). Just verify all expected files are present.
        let mut paths: Vec<_> = items
            .iter()
            .filter(|i| i.entry_type == ItemType::RegularFile)
            .map(|i| i.path.clone())
            .collect();
        paths.sort();
        let expected: Vec<String> = {
            let mut v: Vec<_> = (0..500)
                .map(|i| format!("small_{i:04}.bin"))
                .chain(std::iter::once("large.bin".to_string()))
                .collect();
            v.sort();
            v
        };
        assert_eq!(paths, expected, "all expected files should be present");
    }

    // Extract and verify all file contents.
    let restore_dir = tmp.path().join("restore1");
    commands::restore::run(
        &config,
        None,
        "snap-small-1",
        restore_dir.to_str().unwrap(),
        None,
        false,
    )
    .unwrap();

    for i in 0..500 {
        let expected: Vec<u8> = (0..1024).map(|j| ((i * 7 + j * 13) % 251) as u8).collect();
        let restored = std::fs::read(restore_dir.join(format!("small_{i:04}.bin"))).unwrap();
        assert_eq!(restored, expected, "small file {i} content mismatch");
    }
    assert_eq!(
        std::fs::read(restore_dir.join("large.bin")).unwrap(),
        large_payload
    );

    // Second backup (sequential path — single thread).
    let mut seq_config = make_test_config(&repo_dir);
    seq_config.limits.threads = 1;

    let stats2 = commands::backup::run(
        &seq_config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-small-2",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::Lz4,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap()
    .stats;

    assert_eq!(stats2.nfiles, 501);

    // Verify the second snapshot also has correct walk order.
    {
        let mut repo = open_local_repo(&repo_dir);
        let items = commands::list::load_snapshot_items(&mut repo, "snap-small-2").unwrap();
        let files: Vec<_> = items
            .iter()
            .filter(|i| i.entry_type == ItemType::RegularFile)
            .collect();
        assert_eq!(files.len(), 501);

        let mut paths: Vec<_> = items
            .iter()
            .filter(|i| i.entry_type == ItemType::RegularFile)
            .map(|i| i.path.clone())
            .collect();
        paths.sort();
        let expected: Vec<String> = {
            let mut v: Vec<_> = (0..500)
                .map(|i| format!("small_{i:04}.bin"))
                .chain(std::iter::once("large.bin".to_string()))
                .collect();
            v.sort();
            v
        };
        assert_eq!(
            paths, expected,
            "all expected files should be present (seq)"
        );
    }

    // Extract and verify second snapshot too.
    let restore_dir2 = tmp.path().join("restore2");
    commands::restore::run(
        &config,
        None,
        "snap-small-2",
        restore_dir2.to_str().unwrap(),
        None,
        false,
    )
    .unwrap();

    for i in 0..500 {
        let expected: Vec<u8> = (0..1024).map(|j| ((i * 7 + j * 13) % 251) as u8).collect();
        let restored = std::fs::read(restore_dir2.join(format!("small_{i:04}.bin"))).unwrap();
        assert_eq!(restored, expected, "small file {i} content mismatch (seq)");
    }
    assert_eq!(
        std::fs::read(restore_dir2.join("large.bin")).unwrap(),
        large_payload
    );
}

/// Verify pipeline threshold splitting: a file above the large_file_threshold
/// takes the LargeFile (streaming) path while a smaller file takes the
/// ProcessedFile (buffered) path. Both should round-trip correctly.
#[test]
fn backup_pipeline_threshold_splitting_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    // 12 MiB file → should exceed large_file_threshold (LargeFile path).
    let large_payload: Vec<u8> = (0u32..12 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(source_dir.join("large.bin"), &large_payload).unwrap();

    // 2 MiB file → should stay under threshold (ProcessedFile path).
    let small_payload: Vec<u8> = (0u32..2 * 1024 * 1024).map(|i| (i % 199) as u8).collect();
    std::fs::write(source_dir.join("small.bin"), &small_payload).unwrap();

    let mut config = make_test_config(&repo_dir);
    // Use 2 threads to trigger pipeline mode.
    config.limits.threads = 2;

    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-threshold",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::Lz4,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap()
    .stats;

    assert_eq!(stats.nfiles, 2);
    assert!(stats.original_size > 0);

    // Extract and verify contents match.
    let restore_dir = tmp.path().join("restore");
    commands::restore::run(
        &config,
        None,
        "snap-threshold",
        restore_dir.to_str().unwrap(),
        None,
        false,
    )
    .unwrap();

    assert_eq!(
        std::fs::read(restore_dir.join("large.bin")).unwrap(),
        large_payload,
        "large file content mismatch after pipeline threshold split"
    );
    assert_eq!(
        std::fs::read(restore_dir.join("small.bin")).unwrap(),
        small_payload,
        "small file content mismatch after pipeline threshold split"
    );
}

/// Verify that pipeline mode preserves deterministic walk order even when
/// files have very different processing times.
#[test]
fn backup_pipeline_preserves_walk_order_with_mixed_file_sizes() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::create_dir_all(source_dir.join("dir")).unwrap();

    // Intentionally vary file sizes so worker completion order differs from path order.
    let a_large: Vec<u8> = (0u32..12 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let b_small: Vec<u8> = (0u32..64 * 1024).map(|i| (i % 197) as u8).collect();
    let c_medium: Vec<u8> = (0u32..2 * 1024 * 1024).map(|i| (i % 191) as u8).collect();
    let d_small: Vec<u8> = (0u32..32 * 1024).map(|i| (i % 173) as u8).collect();

    std::fs::write(source_dir.join("a-large.bin"), &a_large).unwrap();
    std::fs::write(source_dir.join("b-small.bin"), &b_small).unwrap();
    std::fs::write(source_dir.join("c-medium.bin"), &c_medium).unwrap();
    std::fs::write(source_dir.join("dir").join("d-small.bin"), &d_small).unwrap();

    let mut config = make_test_config(&repo_dir);
    // Use 2 threads to trigger pipeline mode.
    config.limits.threads = 2;

    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-order",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::Lz4,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap()
    .stats;

    assert_eq!(stats.nfiles, 4);

    let mut repo = open_local_repo(&repo_dir);
    let items = commands::list::load_snapshot_items(&mut repo, "snap-order").unwrap();
    let mut paths: Vec<_> = items.iter().map(|i| i.path.clone()).collect();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "a-large.bin",
            "b-small.bin",
            "c-medium.bin",
            "dir",
            "dir/d-small.bin"
        ],
        "all expected items should be present"
    );
}

/// Verify a second pipeline backup that includes all three runtime paths:
/// cache-hit files, new buffered files (ProcessedFile), and new large streamed files.
#[test]
fn backup_pipeline_mixed_cache_hit_processed_and_large_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let keep_small: Vec<u8> = (0u32..2 * 1024 * 1024).map(|i| (i % 211) as u8).collect();
    let keep_large: Vec<u8> = (0u32..12 * 1024 * 1024).map(|i| (i % 199) as u8).collect();

    std::fs::write(source_dir.join("keep-small.bin"), &keep_small).unwrap();
    std::fs::write(source_dir.join("keep-large.bin"), &keep_large).unwrap();

    let mut config = make_test_config(&repo_dir);
    // Use 2 threads to trigger pipeline mode.
    config.limits.threads = 2;

    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-mixed-1",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::Lz4,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap();

    let new_small: Vec<u8> = (0u32..2 * 1024 * 1024).map(|i| (i % 181) as u8).collect();
    let new_large: Vec<u8> = (0u32..12 * 1024 * 1024).map(|i| (i % 167) as u8).collect();

    std::fs::write(source_dir.join("new-small.bin"), &new_small).unwrap();
    std::fs::write(source_dir.join("new-large.bin"), &new_large).unwrap();

    let stats2 = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-mixed-2",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::Lz4,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap()
    .stats;

    assert_eq!(stats2.nfiles, 4);
    assert!(stats2.original_size > 0);

    let restore_dir = tmp.path().join("restore-mixed");
    commands::restore::run(
        &config,
        None,
        "snap-mixed-2",
        restore_dir.to_str().unwrap(),
        None,
        false,
    )
    .unwrap();

    assert_eq!(
        std::fs::read(restore_dir.join("keep-small.bin")).unwrap(),
        keep_small
    );
    assert_eq!(
        std::fs::read(restore_dir.join("keep-large.bin")).unwrap(),
        keep_large
    );
    assert_eq!(
        std::fs::read(restore_dir.join("new-small.bin")).unwrap(),
        new_small
    );
    assert_eq!(
        std::fs::read(restore_dir.join("new-large.bin")).unwrap(),
        new_large
    );
}

#[test]
fn backup_emits_intermediate_progress_during_large_file() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    // Write a file large enough to trigger multiple batch flushes.
    // With fixed transform_batch_bytes = 32 MiB, a 70 MiB file triggers
    // at least 2 intermediate flushes + 1 final flush = 3 StatsUpdated events.
    let big_data = vec![0xABu8; 70 * 1024 * 1024];
    std::fs::write(source_dir.join("big.bin"), &big_data).unwrap();

    let mut config = make_test_config(&repo_dir);
    // Force sequential path (single thread).
    config.limits.threads = 1;

    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let mut events = Vec::new();
    let mut on_progress = |event| events.push(event);

    commands::backup::run_with_progress(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-intermediate",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
        Some(&mut on_progress),
        None,
    )
    .unwrap();

    // Scope assertions to the big.bin window: from FileStarted{big.bin} until
    // the final StatsUpdated with current_file: Some(..big.bin).  This avoids
    // false failures from StatsUpdated events emitted for other entries
    // (directories, small files, cache hits) outside the large-file window.
    let big_start = events
        .iter()
        .position(|e| matches!(e, commands::backup::BackupProgressEvent::FileStarted { path } if path.ends_with("big.bin")))
        .expect("expected FileStarted for big.bin");

    let big_end = events
        .iter()
        .rposition(|e| matches!(e, commands::backup::BackupProgressEvent::StatsUpdated { current_file: Some(f), .. } if f.ends_with("big.bin")))
        .expect("expected final StatsUpdated for big.bin");

    let big_stats: Vec<_> = events[big_start..=big_end]
        .iter()
        .filter_map(|e| match e {
            commands::backup::BackupProgressEvent::StatsUpdated {
                original_size,
                current_file,
                ..
            } => Some((*original_size, current_file.clone())),
            _ => None,
        })
        .collect();

    // Must have at least 3 events in the window: ≥2 intermediate (None) + 1 final (Some).
    assert!(
        big_stats.len() >= 3,
        "expected at least 3 StatsUpdated events for big.bin, got {}",
        big_stats.len()
    );

    // Intermediate events (all but last) should have current_file: None.
    for (i, (_size, file)) in big_stats.iter().take(big_stats.len() - 1).enumerate() {
        assert!(
            file.is_none(),
            "intermediate StatsUpdated[{i}] should have current_file: None, got {file:?}"
        );
    }

    // Last event in the window should identify big.bin.
    let (_, last_file) = big_stats.last().unwrap();
    assert!(
        last_file.as_ref().is_some_and(|f| f.ends_with("big.bin")),
        "final StatsUpdated should reference big.bin, got {last_file:?}"
    );

    // original_size must increase monotonically across the window.
    for window in big_stats.windows(2) {
        assert!(
            window[1].0 >= window[0].0,
            "original_size should increase: {} -> {}",
            window[0].0,
            window[1].0
        );
    }
}

#[cfg(unix)]
#[test]
fn command_dump_emits_progress_events() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let mut config = make_test_config(&repo_dir);
    // Use small chunker params so 10 MiB produces many chunks, ensuring
    // multiple intermediate progress events at the 4 MiB threshold even
    // with content-defined boundary variance.
    config.chunker = ChunkerConfig {
        min_size: 1024,
        avg_size: 4096,
        max_size: 16384,
    };

    commands::init::run(&config, None).unwrap();

    // 10 MiB dump — with small chunks this produces ~600+ chunks,
    // guaranteeing multiple 4 MiB progress emissions.
    let dumps = vec![vykar_core::config::CommandDump {
        name: "big_dump.bin".to_string(),
        command: "head -c 10485760 /dev/urandom".to_string(),
    }];

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let mut events = Vec::new();
    let mut on_progress = |event| events.push(event);

    commands::backup::run_with_progress(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-dump-progress",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &dumps,
            verbose: false,
        },
        Some(&mut on_progress),
        None,
    )
    .unwrap();

    // 1. Must have a FileStarted for the dump.
    let dump_start = events
        .iter()
        .position(|e| {
            matches!(e, commands::backup::BackupProgressEvent::FileStarted { path } if path == ".vykar-dumps/big_dump.bin")
        })
        .expect("expected FileStarted for .vykar-dumps/big_dump.bin");

    // 2. Find the final StatsUpdated with current_file identifying the dump.
    let dump_end = events
        .iter()
        .rposition(|e| {
            matches!(e, commands::backup::BackupProgressEvent::StatsUpdated { current_file: Some(f), .. } if f == ".vykar-dumps/big_dump.bin")
        })
        .expect("expected final StatsUpdated for .vykar-dumps/big_dump.bin");

    // 3. Collect StatsUpdated events in the window.
    let dump_stats: Vec<_> = events[dump_start..=dump_end]
        .iter()
        .filter_map(|e| match e {
            commands::backup::BackupProgressEvent::StatsUpdated {
                original_size,
                current_file,
                ..
            } => Some((*original_size, current_file.clone())),
            _ => None,
        })
        .collect();

    // At least 2 events: >=1 intermediate (None) + 1 final (Some).
    assert!(
        dump_stats.len() >= 2,
        "expected at least 2 StatsUpdated events for big_dump.bin, got {}",
        dump_stats.len()
    );

    // 4. Intermediate events have current_file: None; final has Some.
    for (i, (_size, file)) in dump_stats.iter().take(dump_stats.len() - 1).enumerate() {
        assert!(
            file.is_none(),
            "intermediate StatsUpdated[{i}] should have current_file: None, got {file:?}"
        );
    }

    let (_, last_file) = dump_stats.last().unwrap();
    assert!(
        last_file
            .as_ref()
            .is_some_and(|f| f == ".vykar-dumps/big_dump.bin"),
        "final StatsUpdated should reference big_dump.bin, got {last_file:?}"
    );

    // 5. original_size increases monotonically.
    for window in dump_stats.windows(2) {
        assert!(
            window[1].0 >= window[0].0,
            "original_size should increase: {} -> {}",
            window[0].0,
            window[1].0
        );
    }
}

#[cfg(unix)]
#[test]
fn command_dump_mixed_progress_events() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    // Create a regular file.
    std::fs::write(source_dir.join("hello.txt"), "hello world\n").unwrap();

    let config = make_test_config(&repo_dir);

    commands::init::run(&config, None).unwrap();

    let dumps = vec![vykar_core::config::CommandDump {
        name: "mixed_dump.txt".to_string(),
        command: "echo dump content".to_string(),
    }];

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let mut events = Vec::new();
    let mut on_progress = |event| events.push(event);

    commands::backup::run_with_progress(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-mixed-progress",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &dumps,
            verbose: false,
        },
        Some(&mut on_progress),
        None,
    )
    .unwrap();

    // FileStarted events should appear for both the regular file and the dump.
    let file_started_paths: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            commands::backup::BackupProgressEvent::FileStarted { path } => Some(path.clone()),
            _ => None,
        })
        .collect();

    assert!(
        file_started_paths.iter().any(|p| p.ends_with("hello.txt")),
        "expected FileStarted for hello.txt, got: {file_started_paths:?}"
    );
    assert!(
        file_started_paths
            .iter()
            .any(|p| p == ".vykar-dumps/mixed_dump.txt"),
        "expected FileStarted for .vykar-dumps/mixed_dump.txt, got: {file_started_paths:?}"
    );

    // StatsUpdated events should cover both (final nfiles == 2).
    let final_stats = events
        .iter()
        .rev()
        .find_map(|e| match e {
            commands::backup::BackupProgressEvent::StatsUpdated { nfiles, .. } => Some(*nfiles),
            _ => None,
        })
        .expect("expected at least one StatsUpdated");

    assert_eq!(final_stats, 2, "final nfiles should be 2 (file + dump)");
}

#[test]
fn verbose_file_processed_events_classify_new_modified_unchanged() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::create_dir_all(&cache_dir).unwrap();
    std::fs::write(source_dir.join("alpha.txt"), b"alpha content").unwrap();
    std::fs::write(source_dir.join("beta.txt"), b"beta content").unwrap();

    let config = {
        let mut cfg = make_test_config(&repo_dir);
        cfg.cache_dir = Some(cache_dir.to_string_lossy().to_string());
        cfg
    };
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    // Helper to extract FileProcessed events.
    fn file_processed_events(
        events: &[commands::backup::BackupProgressEvent],
    ) -> Vec<(String, commands::backup::FileStatus, u64)> {
        events
            .iter()
            .filter_map(|e| match e {
                commands::backup::BackupProgressEvent::FileProcessed {
                    path,
                    status,
                    added_bytes,
                } => Some((path.clone(), *status, *added_bytes)),
                _ => None,
            })
            .collect()
    }

    // --- First backup: all files should be New ---
    let mut events1 = Vec::new();
    let mut cb1 = |e| events1.push(e);
    commands::backup::run_with_progress(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap1",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: true,
        },
        Some(&mut cb1),
        None,
    )
    .unwrap();
    let fp1 = file_processed_events(&events1);
    assert_eq!(
        fp1.len(),
        2,
        "expected 2 FileProcessed events, got {}",
        fp1.len()
    );
    for (path, status, added_bytes) in &fp1 {
        assert_eq!(
            *status,
            commands::backup::FileStatus::New,
            "first backup: {path} should be New"
        );
        assert!(
            *added_bytes > 0,
            "first backup: {path} should have added_bytes > 0"
        );
    }

    // --- Second backup (no changes): all files should be Unchanged ---
    let mut events2 = Vec::new();
    let mut cb2 = |e| events2.push(e);
    commands::backup::run_with_progress(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap2",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: true,
        },
        Some(&mut cb2),
        None,
    )
    .unwrap();
    let fp2 = file_processed_events(&events2);
    assert_eq!(
        fp2.len(),
        2,
        "expected 2 FileProcessed events, got {}",
        fp2.len()
    );
    for (path, status, added_bytes) in &fp2 {
        assert_eq!(
            *status,
            commands::backup::FileStatus::Unchanged,
            "second backup: {path} should be Unchanged"
        );
        assert_eq!(
            *added_bytes, 0,
            "second backup: {path} should have added_bytes == 0"
        );
    }

    // --- Modify one file, third backup: one Modified, one Unchanged ---
    // Ensure mtime changes (some filesystems have 1s resolution).
    std::thread::sleep(std::time::Duration::from_millis(1100));
    std::fs::write(source_dir.join("alpha.txt"), b"alpha CHANGED content").unwrap();

    let mut events3 = Vec::new();
    let mut cb3 = |e| events3.push(e);
    commands::backup::run_with_progress(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap3",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: true,
        },
        Some(&mut cb3),
        None,
    )
    .unwrap();
    let fp3 = file_processed_events(&events3);
    assert_eq!(
        fp3.len(),
        2,
        "expected 2 FileProcessed events, got {}",
        fp3.len()
    );

    let alpha = fp3
        .iter()
        .find(|(p, _, _)| p.contains("alpha"))
        .expect("alpha event");
    let beta = fp3
        .iter()
        .find(|(p, _, _)| p.contains("beta"))
        .expect("beta event");

    assert_eq!(
        alpha.1,
        commands::backup::FileStatus::Modified,
        "third backup: alpha should be Modified"
    );
    assert!(
        alpha.2 > 0,
        "third backup: modified file should have added_bytes > 0"
    );

    assert_eq!(
        beta.1,
        commands::backup::FileStatus::Unchanged,
        "third backup: beta should be Unchanged"
    );
    assert_eq!(
        beta.2, 0,
        "third backup: unchanged file should have added_bytes == 0"
    );
}

// ---------------------------------------------------------------------------
// Bug 1 verification: plaintext chunk_id_key consistency across init/open
// ---------------------------------------------------------------------------

#[test]
fn plaintext_chunk_id_key_consistent_across_init_and_open() {
    init_test_environment();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();

    let data = b"hello world";

    // Init a plaintext repo and compute a ChunkId
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    let repo = Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        None,
    )
    .unwrap();
    let key_init = *repo.crypto.chunk_id_key();
    let id_init = vykar_types::chunk_id::ChunkId::compute(&key_init, data);
    drop(repo);

    // Re-open the same repo and compute the ChunkId for the same data
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    let repo = Repository::open(storage, None, None).unwrap();
    let key_open = *repo.crypto.chunk_id_key();
    let id_open = vykar_types::chunk_id::ChunkId::compute(&key_open, data);

    assert_eq!(
        key_init, key_open,
        "chunk_id_key must be identical after init and open"
    );
    assert_eq!(
        id_init, id_open,
        "ChunkId for same data must match across init and open"
    );
}

// ---------------------------------------------------------------------------
// Bug 2 verification: SessionGuard blocks maintenance
// ---------------------------------------------------------------------------

#[test]
fn session_guard_blocks_maintenance() {
    use vykar_core::commands::util::with_maintenance_lock;
    use vykar_core::repo::lock;

    init_test_environment();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();

    // Init a repo so with_maintenance_lock can open it
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    let repo = Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        None,
    )
    .unwrap();
    drop(repo);

    // Register a read session and adopt it with a guard
    let storage: std::sync::Arc<dyn vykar_storage::StorageBackend> =
        std::sync::Arc::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    let session_id = format!("{:032x}", rand::random::<u128>());
    lock::register_session(storage.as_ref(), &session_id).unwrap();
    let guard =
        lock::SessionGuard::adopt(std::sync::Arc::clone(&storage), session_id.clone()).unwrap();

    // with_maintenance_lock must refuse while the session is active
    let mut repo2 = open_local_repo(&repo_dir);
    let err = with_maintenance_lock(&mut repo2, |_| Ok(())).unwrap_err();
    assert!(
        matches!(err, vykar_types::error::VykarError::ActiveSessions(_)),
        "expected ActiveSessions, got: {err}"
    );

    // Drop the guard — session deregistered
    drop(guard);

    // Maintenance should now succeed
    let mut repo3 = open_local_repo(&repo_dir);
    with_maintenance_lock(&mut repo3, |_| Ok(())).unwrap();
}
