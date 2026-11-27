#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use vykar_core::commands;
use vykar_core::commands::restore::{RestoreOptions, RestoreRequest, RestoreSelection};
use vykar_core::compress::Compression;
use vykar_core::config::{ChunkerConfig, CommandDump, EncryptionModeConfig, RetentionConfig};
use vykar_core::repo::control::CancelToken;
use vykar_core::repo::sessions;
use vykar_core::repo::{EncryptionMode, OpenOptions, Repo};
use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend};
use vykar_types::error::VykarError;

use crate::common::{
    backup_source, exercise_pack_naming, make_test_config, open_local_repo, source_entry,
};
use vykar_types::hash::HashAlgorithm;

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

    common::init_repo(&config);
    backup_source(
        &config,
        &source_dir,
        "src-a",
        "snap-v1",
        common::pass(&config),
        config.xattrs.enabled,
    );

    std::fs::write(source_dir.join("data.bin"), &payload_v2).unwrap();
    std::fs::write(source_dir.join("new.txt"), b"new file").unwrap();
    backup_source(
        &config,
        &source_dir,
        "src-a",
        "snap-v2",
        common::pass(&config),
        config.xattrs.enabled,
    );

    let delete_result = commands::delete::run(
        &config,
        common::pass(&config),
        &["snap-v1"],
        false,
        &CancelToken::new(),
    )
    .unwrap();
    assert!(delete_result.warnings.is_empty());
    let stats = delete_result
        .stats
        .first()
        .expect("delete returned no stats");
    assert_eq!(stats.snapshot_name, "snap-v1");

    // snap-v1's backup segment and packs are old: the sweep takes its
    // entries (compact keeps those of segments younger than T_pack).
    common::age_repo_objects(&repo_dir, &["index.d", "packs"], common::PAST_T_PACK);
    let compact_stats = commands::compact::run(
        &config,
        common::pass(&config),
        &commands::compact::CompactRequest {
            threshold: 0.0,
            max_repack_size: None,
            dry_run: false,
        },
        &CancelToken::new(),
    )
    .unwrap();
    assert!(compact_stats.chunks_unreferenced > 0);
    assert!(compact_stats.space_freed > 0);

    let check = commands::check::run(
        &config,
        common::pass(&config),
        &commands::check::CheckRequest {
            verify_data: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        check.errors.is_empty(),
        "check errors: {:?}",
        check
            .errors
            .iter()
            .map(|e| format!("[{}] {}", e.context, e.message))
            .collect::<Vec<_>>()
    );

    // `verify_chunks` so the restore re-hashes every chunk under BLAKE3.
    let restore_dir = tmp.path().join("restore");
    let extract_stats = commands::restore::run(
        &config,
        common::pass(&config),
        &RestoreRequest {
            snapshot: "snap-v2",
            dest: restore_dir.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions {
                xattrs_enabled: config.xattrs.enabled,
                verify_chunks: true,
                ..Default::default()
            },
        },
        None,
        &CancelToken::new(),
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

    let repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
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

    common::init_repo(&config);
    backup_source(
        &config,
        &source_a,
        "src-a",
        "snap-a1",
        common::pass(&config),
        config.xattrs.enabled,
    );
    std::thread::sleep(Duration::from_millis(2));
    backup_source(
        &config,
        &source_b,
        "src-b",
        "snap-b1",
        common::pass(&config),
        config.xattrs.enabled,
    );
    std::thread::sleep(Duration::from_millis(2));
    std::fs::write(source_a.join("a.txt"), b"alpha-v2").unwrap();
    backup_source(
        &config,
        &source_a,
        "src-a",
        "snap-a2",
        common::pass(&config),
        config.xattrs.enabled,
    );

    let sources = vec![
        source_entry(&source_a, "src-a"),
        source_entry(&source_b, "src-b"),
    ];
    let source_filter = vec!["src-a".to_string()];
    let (prune_stats, list_entries) = commands::prune::run(
        &config,
        common::pass(&config),
        &commands::prune::PruneRequest {
            list: true,
            sources: &sources,
            source_filter: &source_filter,
            ..Default::default()
        },
        &CancelToken::new(),
    )
    .unwrap();

    assert_eq!(prune_stats.pruned, 1);
    assert_eq!(prune_stats.kept, 1);
    assert!(list_entries.iter().any(|e| e.action == "prune"));
    assert!(list_entries.iter().any(|e| e.action == "keep"));

    // The pruned snapshot's segment and packs are old (see above).
    common::age_repo_objects(&repo_dir, &["index.d", "packs"], common::PAST_T_PACK);
    let compact_stats = commands::compact::run(
        &config,
        common::pass(&config),
        &commands::compact::CompactRequest {
            threshold: 0.0,
            max_repack_size: None,
            dry_run: false,
        },
        &CancelToken::new(),
    )
    .unwrap();
    assert!(
        compact_stats.packs_repacked > 0
            || compact_stats.packs_deleted_empty > 0
            || compact_stats.space_freed > 0
    );

    let check = commands::check::run(
        &config,
        common::pass(&config),
        &commands::check::CheckRequest {
            verify_data: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(check.errors.is_empty());

    let repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
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
        common::pass(&config),
        &RestoreRequest {
            snapshot: "snap-a2",
            dest: restore_a.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions {
                xattrs_enabled: config.xattrs.enabled,
                ..Default::default()
            },
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(restore_a.join("a.txt")).unwrap(),
        "alpha-v2"
    );

    let restore_b = tmp.path().join("restore-b");
    commands::restore::run(
        &config,
        common::pass(&config),
        &RestoreRequest {
            snapshot: "snap-b1",
            dest: restore_b.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions {
                xattrs_enabled: config.xattrs.enabled,
                ..Default::default()
            },
        },
        None,
        &CancelToken::new(),
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
    assert_eq!(repo.config().encryption, expected_mode);
    drop(repo);

    backup_source(
        &config,
        &source_dir,
        "encrypted-src",
        "snap-secret",
        Some(passphrase),
        config.xattrs.enabled,
    );

    let check = commands::check::run(
        &config,
        Some(passphrase),
        &commands::check::CheckRequest {
            verify_data: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(check.errors.is_empty());

    let restore_dir = tmp.path().join("restore");
    commands::restore::run(
        &config,
        Some(passphrase),
        &RestoreRequest {
            snapshot: "snap-secret",
            dest: restore_dir.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions {
                xattrs_enabled: config.xattrs.enabled,
                ..Default::default()
            },
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(
        std::fs::read(restore_dir.join("secret.bin")).unwrap(),
        payload
    );

    // The headline diagnosis: two intact, byte-identical copies plus a failed
    // unwrap is *weighted* evidence for a wrong passphrase, and the wording
    // must stay hedged — see `key_copies` tests for the full guard.
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    let wrong_open = Repo::open(storage, Some(wrong_passphrase), None, OpenOptions::new());
    let msg = wrong_open
        .err()
        .expect("wrong passphrase must fail")
        .to_string();
    assert!(
        msg.contains("likely incorrect passphrase; both key copies match"),
        "unexpected wrong-passphrase message: {msg}"
    );

    let wrong_extract = commands::restore::run(
        &config,
        Some(wrong_passphrase),
        &RestoreRequest {
            snapshot: "snap-secret",
            dest: tmp.path().join("bad-restore").to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions {
                xattrs_enabled: config.xattrs.enabled,
                ..Default::default()
            },
        },
        None,
        &CancelToken::new(),
    );
    let msg = wrong_extract.expect_err("restore must fail").to_string();
    assert!(
        msg.contains("likely incorrect passphrase"),
        "unexpected restore message: {msg}"
    );

    let wrong_check = commands::check::run(
        &config,
        Some(wrong_passphrase),
        &commands::check::CheckRequest {
            verify_data: true,
            ..Default::default()
        },
    );
    let msg = wrong_check.expect_err("check must fail").to_string();
    assert!(
        msg.contains("likely incorrect passphrase"),
        "unexpected check message: {msg}"
    );
}

/// Packs written into a v3 repository — by backup and by repack — are named
/// by their BLAKE3 digest.
#[test]
fn v3_pack_names_survive_backup_and_compaction() {
    let passphrase = Some("pack-naming-passphrase");
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
    vykar_core::testing::with_cheap_kdf(|| commands::init::run(&config, passphrase)).unwrap();

    exercise_pack_naming(&config, passphrase, &source_dir, HashAlgorithm::Blake3);
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
    common::init_repo(&config);
    backup_source(
        &config,
        &source_dir,
        "src-a",
        "snap-baseline",
        common::pass(&config),
        config.xattrs.enabled,
    );

    let before = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
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
            passphrase: Some(common::TEST_PASSPHRASE),
            source_paths: &source_paths,
            source_label: "src-a",
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

    let after = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
    assert_eq!(after.manifest().snapshots.len(), snapshots_before);
    assert_eq!(after.chunk_index().len(), chunks_before);
    assert!(after.manifest().find_snapshot("snap-atomic-fail").is_none());
    assert!(after.manifest().find_snapshot("snap-baseline").is_some());
}

#[test]
fn backup_fails_while_an_exclusive_session_is_live() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("locked.txt"), b"lock-test-data").unwrap();

    let config = make_test_config(&repo_dir);
    common::init_repo(&config);

    // A live exclusive session (`check --repair`, upgrade, pack maps;
    // 10-single-protocol step 8; a held lock before): its marker, written
    // now by another machine.
    let storage = LocalBackend::new(repo_dir.to_str().unwrap()).unwrap();
    let now = chrono::Utc::now();
    let entry = sessions::SessionEntry {
        hostname: "other-host".into(),
        pid: 4242,
        registered_at: now.to_rfc3339(),
        last_refresh: now.to_rfc3339(),
        boot_id: None,
        installation_id: None,
        pid_ns: None,
        elapsed_secs: Some(0),
        committing: false,
        writes_heads: false,
    };
    let marker = format!("sessions/x-{:032x}.json", rand::random::<u128>());
    vykar_storage::StorageBackend::put(&storage, &marker, &serde_json::to_vec(&entry).unwrap())
        .unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let blocked = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-while-locked",
            passphrase: Some(common::TEST_PASSPHRASE),
            source_paths: &source_paths,
            source_label: "src-a",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    );

    vykar_storage::StorageBackend::delete(&storage, &marker).unwrap();
    assert!(
        matches!(
            &blocked,
            Err(VykarError::Locked(holder))
                if holder.key == marker
                    && holder.to_string().contains("exclusive maintenance run")
        ),
        "expected Locked naming {marker}, got {:?}",
        blocked.err()
    );

    backup_source(
        &config,
        &source_dir,
        "src-a",
        "snap-after-lock",
        common::pass(&config),
        config.xattrs.enabled,
    );
    let repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
    assert!(repo.manifest().find_snapshot("snap-after-lock").is_some());
}

// ---------------------------------------------------------------------------
// Restore through the local chunk table
// ---------------------------------------------------------------------------

/// The local index files (base tables and cached segments) of `repo_id` in
/// the default cache dir.
fn chunk_tables(repo_id: &[u8]) -> Vec<std::path::PathBuf> {
    let dir = filecache_path(repo_id)
        .parent()
        .expect("repo cache dir")
        .to_path_buf();
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("index_base.") || n.starts_with("index_segment."))
        })
        .collect()
}

/// With the local table gone, restore downloads the index, restores, and
/// leaves a table behind for the next run.
#[test]
fn restore_downloads_the_index_when_the_local_table_is_gone() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let config = make_test_config(&repo_dir);
    common::init_repo(&config);
    std::fs::write(source_dir.join("file.txt"), b"hello").unwrap();
    backup_source(
        &config,
        &source_dir,
        "src",
        "snap1",
        common::pass(&config),
        config.xattrs.enabled,
    );

    let repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
    let repo_id = repo.config().id.clone();
    drop(repo);
    let tables = chunk_tables(&repo_id);
    assert!(!tables.is_empty(), "the backup left a table");
    for path in tables {
        std::fs::remove_file(path).unwrap();
    }

    let restore_dir = tmp.path().join("restore");
    let stats = commands::restore::run(
        &config,
        common::pass(&config),
        &RestoreRequest {
            snapshot: "snap1",
            dest: restore_dir.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions::default(),
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();

    assert_eq!(stats.files, 1);
    assert_eq!(
        std::fs::read_to_string(restore_dir.join("file.txt")).unwrap(),
        "hello"
    );
    assert_eq!(
        chunk_tables(&repo_id).len(),
        1,
        "the downloaded index is kept"
    );
}

/// Back up one small file and return the config, repo ID, index generation
/// and the file's single data chunk.
fn one_file_repo(
    tmp: &std::path::Path,
) -> (
    vykar_core::config::VykarConfig,
    Vec<u8>,
    u64,
    vykar_types::chunk_id::ChunkId,
) {
    let repo_dir = tmp.join("repo");
    let source_dir = tmp.join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let config = make_test_config(&repo_dir);
    common::init_repo(&config);
    std::fs::write(source_dir.join("file.txt"), b"hello").unwrap();
    backup_source(
        &config,
        &source_dir,
        "src",
        "snap1",
        common::pass(&config),
        config.xattrs.enabled,
    );

    let mut repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
    let items =
        commands::list::load_snapshot_items(&mut repo, "snap1", &CancelToken::new()).unwrap();
    let file = items
        .iter()
        .find(|i| i.path.ends_with("file.txt"))
        .expect("file item");
    assert_eq!(file.chunks.len(), 1);
    (
        config,
        repo.config().id.clone(),
        repo.index_generation(),
        file.chunks.first().expect("data chunk").id,
    )
}

/// The local table at the open head serves the item stream and the data
/// chunks: with every `index.d/` object removed (the head stays, open reads
/// it), the restore can only succeed without downloading the index.
#[test]
fn restore_reads_the_local_table_without_index_downloads() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, _, _, _) = one_file_repo(tmp.path());

    // Remove the index's base and segments (the head stays, open reads it):
    // the restore can only succeed if it never loads the index, i.e.
    // resolves every chunk through the restore cache.
    let objects = tmp.path().join("repo").join("index.d");
    for entry in std::fs::read_dir(&objects).unwrap() {
        std::fs::remove_file(entry.unwrap().path()).unwrap();
    }

    let restore_dir = tmp.path().join("restore");
    let stats = commands::restore::run(
        &config,
        common::pass(&config),
        &RestoreRequest {
            snapshot: "snap1",
            dest: restore_dir.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions::default(),
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();

    assert_eq!(stats.files, 1);
    assert_eq!(
        std::fs::read_to_string(restore_dir.join("file.txt")).unwrap(),
        "hello"
    );
}

/// A chunk that is unreadable where the index places it is a failure:
/// strict mode reports the source as unreadable, continue mode marks the
/// file damaged.
#[test]
fn restore_reports_an_unreadable_data_chunk() {
    for continue_on_error in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let (config, _, _, data_chunk) = one_file_repo(tmp.path());
        let repo_dir = tmp.path().join("repo");
        let repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
        let pack_id = repo.chunk_index().get(&data_chunk).unwrap().pack_id;
        drop(repo);
        std::fs::remove_file(repo_dir.join(pack_id.storage_key())).unwrap();

        let restore_dir = tmp.path().join("restore");
        let result = commands::restore::run(
            &config,
            common::pass(&config),
            &RestoreRequest {
                snapshot: "snap1",
                dest: restore_dir.to_str().unwrap(),
                selection: RestoreSelection::All,
                options: commands::restore::RestoreOptions {
                    continue_on_error,
                    ..Default::default()
                },
            },
            None,
            &CancelToken::new(),
        );
        if continue_on_error {
            let stats = result.unwrap();
            assert_eq!(stats.damaged_total, 1);
            assert!(!restore_dir.join("file.txt").exists());
        } else {
            let err = result.unwrap_err();
            assert!(
                matches!(
                    err,
                    vykar_types::error::VykarError::RestoreSourceUnreadable(_)
                ),
                "got: {err:?}"
            );
        }
    }
}

#[test]
fn restore_streams_a_multi_chunk_item_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = make_test_config(&repo_dir);
    common::init_repo(&config);
    // Enough entries for an item stream of several 32-512 KiB chunks.
    const N: usize = 6000;
    for d in 0..20 {
        let dir = source_dir.join(format!("dir-{d:02}"));
        std::fs::create_dir_all(&dir).unwrap();
        for f in 0..N / 20 {
            std::fs::write(dir.join(format!("file-{f:04}.txt")), format!("{d}/{f}")).unwrap();
        }
    }
    backup_source(
        &config,
        &source_dir,
        "src",
        "snap1",
        common::pass(&config),
        config.xattrs.enabled,
    );
    let repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
    let ptrs = commands::list::load_snapshot_meta(&repo, "snap1")
        .unwrap()
        .item_ptrs
        .len();
    assert!(ptrs > 1, "expected a multi-chunk item stream, got {ptrs}");
    drop(repo);

    let restore_dir = tmp.path().join("restore");
    let stats = commands::restore::run(
        &config,
        common::pass(&config),
        &RestoreRequest {
            snapshot: "snap1",
            dest: restore_dir.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions::default(),
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();

    assert_eq!(stats.files, N as u64);
    for (d, f) in [(0, 0), (7, 123), (19, N / 20 - 1)] {
        assert_eq!(
            std::fs::read_to_string(
                restore_dir
                    .join(format!("dir-{d:02}"))
                    .join(format!("file-{f:04}.txt"))
            )
            .unwrap(),
            format!("{d}/{f}")
        );
    }
}

/// Counts GETs of the remote index head, including at open.
struct IndexGetCounter {
    inner: LocalBackend,
    index_gets: Arc<AtomicUsize>,
}

impl InnerBackend for IndexGetCounter {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for IndexGetCounter;
    except [get];

    fn get(&self, key: &str) -> vykar_types::error::Result<Option<Vec<u8>>> {
        if key.starts_with("index.h/") {
            self.index_gets.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get(key)
    }
}

/// Back up `snap1` and `snap2` (distinct content) and return the repo ID and
/// index generation.
fn two_snapshot_repo(tmp: &std::path::Path) -> (vykar_core::config::VykarConfig, Vec<u8>, u64) {
    let repo_dir = tmp.join("repo");
    let source_dir = tmp.join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let config = make_test_config(&repo_dir);
    common::init_repo(&config);
    std::fs::write(source_dir.join("a.txt"), b"first").unwrap();
    backup_source(
        &config,
        &source_dir,
        "src",
        "snap1",
        common::pass(&config),
        config.xattrs.enabled,
    );
    std::fs::write(source_dir.join("b.txt"), b"second").unwrap();
    backup_source(
        &config,
        &source_dir,
        "src",
        "snap2",
        common::pass(&config),
        config.xattrs.enabled,
    );

    let repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
    let ids = (repo.config().id.clone(), repo.index_generation());
    drop(repo);
    (config, ids.0, ids.1)
}

fn open_counting_repo(repo_dir: &std::path::Path) -> (Repo, Arc<AtomicUsize>) {
    let index_gets = Arc::new(AtomicUsize::new(0));
    let storage = Box::new(IndexGetCounter {
        inner: LocalBackend::new(repo_dir.to_str().unwrap()).unwrap(),
        index_gets: index_gets.clone(),
    });
    let repo = Repo::open(
        storage,
        Some(common::TEST_PASSPHRASE),
        None,
        OpenOptions::new(),
    )
    .unwrap();
    (repo, index_gets)
}

fn stream_paths(stream: &[u8]) -> Vec<String> {
    let mut paths = Vec::new();
    commands::list::for_each_decoded_item(stream, |item| {
        paths.push(item.path);
        Ok(())
    })
    .unwrap();
    paths
}

#[test]
fn item_stream_reader_loads_index_once_across_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let (_config, _repo_id, _) = two_snapshot_repo(tmp.path());

    let (mut repo, index_gets) = open_counting_repo(&tmp.path().join("repo"));
    let mut reader = commands::list::ItemStreamReader::new(&repo);
    let s1 = reader
        .load(&mut repo, "snap1", &CancelToken::new())
        .unwrap();
    let s2 = reader
        .load(&mut repo, "snap2", &CancelToken::new())
        .unwrap();

    assert!(!stream_paths(&s1).iter().any(|p| p.contains("b.txt")));
    assert!(stream_paths(&s2).iter().any(|p| p.contains("b.txt")));
    assert!(reader.index_loaded());
    assert_eq!(index_gets.load(Ordering::Relaxed), 1);
}
#[test]
fn item_stream_reader_propagates_interrupt_without_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    two_snapshot_repo(tmp.path());

    let (mut repo, index_gets) = open_counting_repo(&tmp.path().join("repo"));
    let mut reader = commands::list::ItemStreamReader::new(&repo);
    let cancel = CancelToken::new();
    cancel.cancel();
    let err = reader.load(&mut repo, "snap1", &cancel).unwrap_err();

    assert!(matches!(err, VykarError::Interrupted), "got {err:?}");
    assert_eq!(index_gets.load(Ordering::Relaxed), 1);
    assert!(repo.chunk_index().is_empty());
}

// ---------------------------------------------------------------------------
// File cache: key by source paths, not label
// ---------------------------------------------------------------------------

/// Compute the local file cache path for a given repo ID.
fn filecache_path(repo_id: &[u8]) -> std::path::PathBuf {
    let cache_base = std::env::var("XDG_CACHE_HOME")
        .map(std::path::PathBuf::from)
        .unwrap();
    cache_base
        .join("vykar")
        .join(hex::encode(repo_id))
        .join("filecache")
}

/// Integration test A: local file cache survives a label rename.
///
/// Backup with "old-label", then backup the same paths under "new-label".
/// All files should hit the cache (status: Unchanged).
#[test]
fn file_cache_survives_label_rename() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("a.txt"), b"alpha").unwrap();
    std::fs::write(source_dir.join("b.txt"), b"bravo").unwrap();

    let config = make_test_config(&repo_dir);
    common::init_repo(&config);

    // First backup with "old-label".
    backup_source(
        &config,
        &source_dir,
        "old-label",
        "snap-old",
        common::pass(&config),
        config.xattrs.enabled,
    );

    // Second backup with "new-label", same paths, same files.
    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_patterns: Vec<String> = Vec::new();
    let exclude_if_present: Vec<String> = Vec::new();

    let mut events = Vec::new();
    let mut on_progress = |event: commands::backup::BackupProgressEvent| events.push(event);

    commands::backup::run_with_progress(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-new",
            passphrase: Some(common::TEST_PASSPHRASE),
            source_paths: &source_paths,
            source_label: "new-label",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: true,
        },
        Some(&mut on_progress),
        &CancelToken::new(),
    )
    .unwrap();

    // All FileProcessed events should be Unchanged (cache hit).
    let file_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            commands::backup::BackupProgressEvent::FileProcessed { path, status, .. } => {
                Some((path.clone(), *status))
            }
            _ => None,
        })
        .collect();

    assert!(
        !file_events.is_empty(),
        "expected FileProcessed events but got none"
    );
    for (path, status) in &file_events {
        assert_eq!(
            *status,
            commands::backup::FileStatus::Unchanged,
            "file {path} should be Unchanged (cache hit) but was {status:?}"
        );
    }
}

/// Integration test B: parent fallback survives a label rename.
///
/// Backup with "old-label", delete local file cache, then backup the same
/// paths under "new-label". The parent snapshot filter uses paths only, so
/// the parent fallback should still produce Unchanged status for all files.
#[test]
fn parent_fallback_survives_label_rename() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("a.txt"), b"alpha").unwrap();
    std::fs::write(source_dir.join("b.txt"), b"bravo").unwrap();

    let config = make_test_config(&repo_dir);
    common::init_repo(&config);

    // First backup with "old-label".
    backup_source(
        &config,
        &source_dir,
        "old-label",
        "snap-old",
        common::pass(&config),
        config.xattrs.enabled,
    );

    // Delete the local file cache to force a cold start (parent fallback path).
    let repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
    let filecache = filecache_path(&repo.config().id);
    drop(repo);
    let _ = std::fs::remove_file(&filecache);

    // Second backup with "new-label", same paths, same files.
    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_patterns: Vec<String> = Vec::new();
    let exclude_if_present: Vec<String> = Vec::new();

    let mut events = Vec::new();
    let mut on_progress = |event: commands::backup::BackupProgressEvent| events.push(event);

    commands::backup::run_with_progress(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-new",
            passphrase: Some(common::TEST_PASSPHRASE),
            source_paths: &source_paths,
            source_label: "new-label",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: true,
        },
        Some(&mut on_progress),
        &CancelToken::new(),
    )
    .unwrap();

    // All FileProcessed events should be Unchanged (parent fallback hit).
    let file_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            commands::backup::BackupProgressEvent::FileProcessed { path, status, .. } => {
                Some((path.clone(), *status))
            }
            _ => None,
        })
        .collect();

    assert!(
        !file_events.is_empty(),
        "expected FileProcessed events but got none"
    );
    for (path, status) in &file_events {
        assert_eq!(
            *status,
            commands::backup::FileStatus::Unchanged,
            "file {path} should be Unchanged (parent fallback hit) but was {status:?}"
        );
    }
}

#[test]
fn command_dump_only_backup_does_not_load_or_mutate_file_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.cache_dir = Some(cache_dir.to_string_lossy().to_string());
    common::init_repo(&config);

    // Step 1: filesystem backup to populate the file cache on disk.
    std::fs::write(source_dir.join("data.txt"), b"hello world").unwrap();
    backup_source(
        &config,
        &source_dir,
        "src-a",
        "snap-fs",
        common::pass(&config),
        config.xattrs.enabled,
    );

    // Step 2: locate the filecache file and snapshot its contents + mtime.
    let repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
    let repo_id_hex = hex::encode(&repo.config().id);
    drop(repo);

    let filecache_path = cache_dir.join(&repo_id_hex).join("filecache");
    assert!(
        filecache_path.exists(),
        "filecache should exist after filesystem backup: {}",
        filecache_path.display()
    );
    let bytes_before = std::fs::read(&filecache_path).unwrap();
    let mtime_before = std::fs::metadata(&filecache_path)
        .unwrap()
        .modified()
        .unwrap();

    // Step 3: run a command-dump-only backup (no filesystem paths).
    let exclude_patterns: Vec<String> = Vec::new();
    let exclude_if_present: Vec<String> = Vec::new();
    let dumps = vec![CommandDump {
        name: "echo.txt".to_string(),
        command: "echo dump-data".to_string(),
    }];
    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-dump",
            passphrase: Some(common::TEST_PASSPHRASE),
            source_paths: &[],
            source_label: "dump-only",
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
    .unwrap();

    // Step 4: assert the filecache file is byte-identical (not rewritten).
    let bytes_after = std::fs::read(&filecache_path).unwrap();
    let mtime_after = std::fs::metadata(&filecache_path)
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        bytes_before, bytes_after,
        "filecache bytes should be identical after dump-only backup"
    );
    assert_eq!(
        mtime_before, mtime_after,
        "filecache mtime should be unchanged after dump-only backup"
    );

    // Verify the dump-only snapshot was actually created.
    let repo = open_local_repo(&repo_dir, Some(common::TEST_PASSPHRASE));
    assert!(repo.manifest().find_snapshot("snap-dump").is_some());
}
