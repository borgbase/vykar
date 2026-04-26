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

    let result = commands::delete::run(&config, None, &["snap-delete-dry"], true, None).unwrap();
    assert_eq!(result.stats.len(), 1);
    assert!(result.warnings.is_empty());
    let stats = &result.stats[0];
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

    let result = commands::delete::run(&config, None, &["snap-delete-live"], false, None).unwrap();
    assert_eq!(result.stats.len(), 1);
    assert!(result.warnings.is_empty());
    let stats = &result.stats[0];
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
    let result = commands::delete::run(&config, None, &["snap-1", "snap-2"], false, None).unwrap();
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

    // Restore snap-3 and verify its data is intact (catches over-decrement).
    let restore_dir = tmp.path().join("restore");
    commands::restore::run(
        &config,
        None,
        "snap-3",
        restore_dir.to_str().unwrap(),
        None,
        false,
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
    let result =
        commands::delete::run(&config, None, &["snap-dup", "snap-dup"], false, None).unwrap();
    assert_eq!(result.stats.len(), 1);
    assert!(result.warnings.is_empty());
    assert_eq!(result.stats[0].snapshot_name, "snap-dup");

    let after = open_local_repo(&repo_dir);
    assert!(after.manifest().find_snapshot("snap-dup").is_none());
    assert_eq!(after.chunk_index().len(), 0);

    // Verify the freed count matches the original chunk count.
    assert_eq!(result.stats[0].chunks_deleted, chunks_before as u64);
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
    let dry_result =
        commands::delete::run(&config, None, &["snap-a", "snap-b"], true, None).unwrap();
    assert_eq!(dry_result.stats.len(), 2);

    let dry_total_freed: u64 = dry_result.stats.iter().map(|s| s.space_freed).sum();
    let dry_total_chunks: u64 = dry_result.stats.iter().map(|s| s.chunks_deleted).sum();

    // Now actually delete both and compare.
    let real_result =
        commands::delete::run(&config, None, &["snap-a", "snap-b"], false, None).unwrap();
    let real_total_freed: u64 = real_result.stats.iter().map(|s| s.space_freed).sum();
    let real_total_chunks: u64 = real_result.stats.iter().map(|s| s.chunks_deleted).sum();

    assert_eq!(dry_total_chunks, real_total_chunks);
    assert_eq!(dry_total_freed, real_total_freed);
}

/// When a snapshot's blob has been deleted from storage (commit point
/// reached) but its Phase 3 refcount cleanup fails, the operation must
/// return `Ok(DeleteResult)` with the failed snapshot in `warnings` rather
/// than propagating an error that would falsely suggest the delete failed.
#[test]
fn delete_phase3_refcount_failure_returns_warning_not_error() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"refcount-phase3-fault").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src", "snap-fail");

    // Remove every pack file so `load_item_stream_from_ptrs` fails in Phase 3
    // (Phase 2 still succeeds — it only deletes snapshots/<id>).
    let packs_dir = repo_dir.join("packs");
    for shard in std::fs::read_dir(&packs_dir).unwrap() {
        let shard = shard.unwrap().path();
        if !shard.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(&shard).unwrap() {
            let entry = entry.unwrap();
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    let result = commands::delete::run(&config, None, &["snap-fail"], false, None)
        .expect("delete must not fail after the commit point — Phase 3 errors become warnings");

    // Phase 2 succeeded (blob is gone), Phase 3 failed (no stats collected).
    assert!(
        result.stats.is_empty(),
        "stats must be empty on Phase 3 failure"
    );
    assert!(
        !result.warnings.is_empty(),
        "Phase 3 failure must produce a warning"
    );
    let combined = result.warnings.join("\n");
    assert!(
        combined.contains("snap-fail"),
        "warning should reference the failing snapshot: {combined}"
    );
    assert!(
        combined.contains("vykar check --repair"),
        "warning should point operators at the recovery tool: {combined}"
    );

    // Snapshot blob must actually be gone from storage despite the Phase 3 failure.
    let after = open_local_repo(&repo_dir);
    assert!(
        after.manifest().find_snapshot("snap-fail").is_none(),
        "snapshot blob should be deleted from storage even when Phase 3 fails"
    );
}

/// Regression test for the GUI wrapper: a single-snapshot delete whose Phase 3
/// fails must surface as `Ok(DeleteResult { stats: [], warnings: [..] })` and
/// must not panic when callers use `result.stats.first()` to format output.
#[test]
fn delete_snapshot_wrapper_handles_empty_stats() {
    use crate::app::operations;

    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"wrapper-phase3-fault").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src", "snap-wrapper");

    // Break Phase 3 by removing all pack files.
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

    let result = operations::delete_snapshot(&config, None, "snap-wrapper")
        .expect("delete_snapshot must not fail after commit point");
    assert!(result.stats.is_empty());
    assert_eq!(result.warnings.len(), 1);
    assert!(result.warnings[0].contains("snap-wrapper"));
}
