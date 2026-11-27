use crate::commands;
use crate::repo::control::CancelToken;
use vykar_types::error::VykarError;

use super::helpers::{
    PAST_T_PACK, age_repo_objects, backup_single_source, clear_tree_cache, init_repo,
    load_snapshot_cache_from_disk, open_local_repo, pass,
};

fn compact(config: &crate::config::VykarConfig) -> commands::compact::CompactStats {
    commands::compact::run(
        config,
        pass(config),
        &commands::compact::CompactRequest {
            threshold: 10.0,
            max_repack_size: None,
            dry_run: false,
        },
        &CancelToken::new(),
    )
    .unwrap()
}

#[test]
fn delete_missing_snapshot_returns_snapshot_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);

    let result = commands::delete::run(
        &config,
        pass(&config),
        &["does-not-exist"],
        false,
        &CancelToken::new(),
    );
    assert!(matches!(result, Err(VykarError::SnapshotNotFound(name)) if name == "does-not-exist"));
}

#[test]
fn delete_dry_run_resolves_names_without_mutation() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"delete-dry-run-data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-delete-dry");

    let before = open_local_repo(&repo_dir);
    let snapshots_before = before.manifest().snapshots.len();
    let chunks_before = before.chunk_index().len();
    drop(before);

    let result = commands::delete::run(
        &config,
        pass(&config),
        &["snap-delete-dry"],
        true,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(result.stats.len(), 1);
    assert!(result.warnings.is_empty());
    let stats = &result.stats[0];
    assert_eq!(stats.snapshot_name, "snap-delete-dry");

    let after = open_local_repo(&repo_dir);
    assert_eq!(after.manifest().snapshots.len(), snapshots_before);
    assert_eq!(after.chunk_index().len(), chunks_before);
}

#[test]
fn delete_removes_the_snapshot_and_compact_its_chunks() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"delete-live-data").unwrap();

    let config = init_repo(&repo_dir);
    // Another snapshot stays, so the sweep is not one to empty.
    let keep_dir = tmp.path().join("keep");
    std::fs::create_dir_all(&keep_dir).unwrap();
    std::fs::write(keep_dir.join("kept.txt"), b"kept-data").unwrap();
    backup_single_source(&config, &keep_dir, "src-k", "snap-keep");
    let kept_chunks = open_local_repo(&repo_dir).chunk_index().len();
    backup_single_source(&config, &source_dir, "src-a", "snap-delete-live");

    let before = open_local_repo(&repo_dir);
    assert!(
        before
            .manifest()
            .find_snapshot("snap-delete-live")
            .is_some()
    );
    let chunks_before = before.chunk_index().len();
    assert!(chunks_before > 0);
    drop(before);

    let result = commands::delete::run(
        &config,
        pass(&config),
        &["snap-delete-live"],
        false,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(result.stats.len(), 1);
    assert!(result.warnings.is_empty());
    let stats = &result.stats[0];
    assert_eq!(stats.snapshot_name, "snap-delete-live");

    // Delete only removes the snapshot object; compact sweeps its chunks.
    let after = open_local_repo(&repo_dir);
    assert!(after.manifest().find_snapshot("snap-delete-live").is_none());
    assert_eq!(after.chunk_index().len(), chunks_before);
    drop(after);
    // The deleted snapshot's backup segment is old: its entries go (04
    // step 7 keeps those of segments younger than T_pack).
    age_repo_objects(&repo_dir, &["index.d", "packs"], PAST_T_PACK);
    let swept = compact(&config);
    assert_eq!(
        swept.chunks_unreferenced,
        (chunks_before - kept_chunks) as u64
    );
    assert_eq!(open_local_repo(&repo_dir).chunk_index().len(), kept_chunks);
}

#[test]
fn delete_persists_snapshot_cache_immediately() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"delete-cache-data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-cache-del");

    // Opening once heals the local cache from the remote listing, so it now
    // contains the snapshot. This is the state a stale cache would keep.
    drop(open_local_repo(&repo_dir));
    let before = load_snapshot_cache_from_disk(&repo_dir);
    assert!(before.entries.values().any(|e| e.name == "snap-cache-del"));

    commands::delete::run(
        &config,
        pass(&config),
        &["snap-cache-del"],
        false,
        &CancelToken::new(),
    )
    .unwrap();

    // The on-disk cache reflects the deletion immediately — without a reopen
    // that would run refresh_snapshot_cache's remote-list retain.
    let after = load_snapshot_cache_from_disk(&repo_dir);
    assert!(
        !after.entries.values().any(|e| e.name == "snap-cache-del"),
        "deleted snapshot still present in on-disk cache"
    );
}

#[test]
fn delete_multiple_snapshots_in_single_call() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let config = init_repo(&repo_dir);

    // Create 3 snapshots with distinct data.
    std::fs::write(source_dir.join("a.txt"), b"aaa").unwrap();
    backup_single_source(&config, &source_dir, "src", "snap-1");

    std::fs::write(source_dir.join("b.txt"), b"bbb").unwrap();
    backup_single_source(&config, &source_dir, "src", "snap-2");

    std::fs::write(source_dir.join("c.txt"), b"ccc").unwrap();
    backup_single_source(&config, &source_dir, "src", "snap-3");

    let before = open_local_repo(&repo_dir);
    assert_eq!(before.manifest().snapshots.len(), 3);
    drop(before);

    // Delete first two in one call.
    let result = commands::delete::run(
        &config,
        pass(&config),
        &["snap-1", "snap-2"],
        false,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(result.stats.len(), 2);
    assert!(result.warnings.is_empty());
    assert_eq!(result.stats[0].snapshot_name, "snap-1");
    assert_eq!(result.stats[1].snapshot_name, "snap-2");

    let after = open_local_repo(&repo_dir);
    assert!(after.manifest().find_snapshot("snap-1").is_none());
    assert!(after.manifest().find_snapshot("snap-2").is_none());
    assert!(after.manifest().find_snapshot("snap-3").is_some());
    // snap-3 still has chunks
    assert!(!after.chunk_index().is_empty());
    drop(after);

    // Compact sweeps, then snap-3 must restore intact.
    compact(&config);
    let restore_dir = tmp.path().join("restore");
    commands::restore::run(
        &config,
        pass(&config),
        "snap-3",
        restore_dir.to_str().unwrap(),
        None,
        false,
        false,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(restore_dir.join("a.txt")).unwrap(),
        "aaa"
    );
    assert_eq!(
        std::fs::read_to_string(restore_dir.join("b.txt")).unwrap(),
        "bbb"
    );
    assert_eq!(
        std::fs::read_to_string(restore_dir.join("c.txt")).unwrap(),
        "ccc"
    );
}

#[test]
fn delete_fails_fast_if_any_snapshot_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src", "snap-exists");

    // Request deletion of existing + nonexistent — should fail before any mutation.
    let result = commands::delete::run(
        &config,
        pass(&config),
        &["snap-exists", "nonexistent"],
        false,
        &CancelToken::new(),
    );
    assert!(matches!(&result, Err(VykarError::SnapshotNotFound(name)) if name == "nonexistent"));

    // Existing snapshot must still be present.
    let after = open_local_repo(&repo_dir);
    assert!(after.manifest().find_snapshot("snap-exists").is_some());
}

#[test]
fn delete_deduplicates_snapshot_names() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"dedup-data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src", "snap-dup");

    let before = open_local_repo(&repo_dir);
    let chunks_before = before.chunk_index().len();
    drop(before);

    // Pass the same name twice — should succeed (deduped to one delete).
    let result = commands::delete::run(
        &config,
        pass(&config),
        &["snap-dup", "snap-dup"],
        false,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(result.stats.len(), 1);
    assert!(result.warnings.is_empty());
    assert_eq!(result.stats[0].snapshot_name, "snap-dup");

    let after = open_local_repo(&repo_dir);
    assert!(after.manifest().find_snapshot("snap-dup").is_none());
    assert_eq!(after.chunk_index().len(), chunks_before);
}

/// Delete reads nothing but the snapshot listing: with every pack gone it
/// still deletes the snapshot object and warns about nothing.
#[test]
fn delete_reads_no_pack() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"no-pack-reads").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src", "snap-gone");

    let packs_dir = repo_dir.join("packs");
    for shard in std::fs::read_dir(&packs_dir).unwrap() {
        let shard = shard.unwrap().path();
        if !shard.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(&shard).unwrap() {
            std::fs::remove_file(entry.unwrap().path()).unwrap();
        }
    }
    clear_tree_cache(&repo_dir);

    let result = commands::delete::run(
        &config,
        pass(&config),
        &["snap-gone"],
        false,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(result.stats.len(), 1);
    assert!(result.warnings.is_empty(), "{:?}", result.warnings);
    let after = open_local_repo(&repo_dir);
    assert!(after.manifest().find_snapshot("snap-gone").is_none());
}
