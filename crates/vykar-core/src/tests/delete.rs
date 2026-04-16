use crate::commands;
use vykar_types::error::VykarError;

use super::helpers::{backup_single_source, init_repo, open_local_repo};

#[test]
fn delete_missing_snapshot_returns_snapshot_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);

    let result = commands::delete::run(&config, None, &["does-not-exist"], false, None);
    assert!(matches!(result, Err(VykarError::SnapshotNotFound(name)) if name == "does-not-exist"));
}

#[test]
fn delete_dry_run_reports_impact_without_mutation() {
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

    let all_stats = commands::delete::run(&config, None, &["snap-delete-dry"], true, None).unwrap();
    assert_eq!(all_stats.len(), 1);
    let stats = &all_stats[0];
    assert_eq!(stats.snapshot_name, "snap-delete-dry");
    assert!(stats.chunks_deleted > 0);
    assert!(stats.space_freed > 0);

    let after = open_local_repo(&repo_dir);
    assert_eq!(after.manifest().snapshots.len(), snapshots_before);
    assert_eq!(after.chunk_index().len(), chunks_before);
}

#[test]
fn delete_snapshot_removes_manifest_entry_and_chunk_refs() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"delete-live-data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-delete-live");

    let before = open_local_repo(&repo_dir);
    assert!(before
        .manifest()
        .find_snapshot("snap-delete-live")
        .is_some());
    let chunks_before = before.chunk_index().len();
    assert!(chunks_before > 0);
    drop(before);

    let all_stats =
        commands::delete::run(&config, None, &["snap-delete-live"], false, None).unwrap();
    assert_eq!(all_stats.len(), 1);
    let stats = &all_stats[0];
    assert_eq!(stats.snapshot_name, "snap-delete-live");
    assert!(stats.chunks_deleted > 0);

    let after = open_local_repo(&repo_dir);
    assert!(after.manifest().find_snapshot("snap-delete-live").is_none());
    assert_eq!(after.chunk_index().len(), 0);
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
    let all_stats =
        commands::delete::run(&config, None, &["snap-1", "snap-2"], false, None).unwrap();
    assert_eq!(all_stats.len(), 2);
    assert_eq!(all_stats[0].snapshot_name, "snap-1");
    assert_eq!(all_stats[1].snapshot_name, "snap-2");

    let after = open_local_repo(&repo_dir);
    assert!(after.manifest().find_snapshot("snap-1").is_none());
    assert!(after.manifest().find_snapshot("snap-2").is_none());
    assert!(after.manifest().find_snapshot("snap-3").is_some());
    // snap-3 still has chunks
    assert!(!after.chunk_index().is_empty());
    drop(after);

    // Restore snap-3 and verify its data is intact (catches over-decrement).
    let restore_dir = tmp.path().join("restore");
    commands::restore::run(
        &config,
        None,
        "snap-3",
        restore_dir.to_str().unwrap(),
        None,
        false,
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
    let result = commands::delete::run(&config, None, &["snap-exists", "nonexistent"], false, None);
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
    let all_stats =
        commands::delete::run(&config, None, &["snap-dup", "snap-dup"], false, None).unwrap();
    assert_eq!(all_stats.len(), 1);
    assert_eq!(all_stats[0].snapshot_name, "snap-dup");

    let after = open_local_repo(&repo_dir);
    assert!(after.manifest().find_snapshot("snap-dup").is_none());
    assert_eq!(after.chunk_index().len(), 0);

    // Verify the freed count matches the original chunk count.
    assert_eq!(all_stats[0].chunks_deleted, chunks_before as u64);
}

#[test]
fn delete_dry_run_accounts_for_shared_chunks() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("shared.txt"), b"shared-content-for-dedup").unwrap();

    let config = init_repo(&repo_dir);

    // Two snapshots from the same source — chunks are shared (refcount=2).
    backup_single_source(&config, &source_dir, "src", "snap-a");
    backup_single_source(&config, &source_dir, "src", "snap-b");

    // Dry-run: delete both. The shared chunks should be counted as freed
    // (they appear in the second snapshot's stats after the first "removed"
    // them from refcount=2 to refcount=1 in the scratch index).
    let dry_stats =
        commands::delete::run(&config, None, &["snap-a", "snap-b"], true, None).unwrap();
    assert_eq!(dry_stats.len(), 2);

    let dry_total_freed: u64 = dry_stats.iter().map(|s| s.space_freed).sum();
    let dry_total_chunks: u64 = dry_stats.iter().map(|s| s.chunks_deleted).sum();

    // Now actually delete both and compare.
    let real_stats =
        commands::delete::run(&config, None, &["snap-a", "snap-b"], false, None).unwrap();
    let real_total_freed: u64 = real_stats.iter().map(|s| s.space_freed).sum();
    let real_total_chunks: u64 = real_stats.iter().map(|s| s.chunks_deleted).sum();

    assert_eq!(dry_total_chunks, real_total_chunks);
    assert_eq!(dry_total_freed, real_total_freed);
}
