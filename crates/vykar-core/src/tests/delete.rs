use crate::commands;
use crate::repo::Repository;
use vykar_storage::local_backend::LocalBackend;
use vykar_types::error::VykarError;

use super::helpers::{backup_single_source, init_repo, init_test_environment};

fn open_local_repo(repo_dir: &std::path::Path) -> Repository {
    init_test_environment();
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    Repository::open(storage, None, None).unwrap()
}

#[test]
fn delete_missing_snapshot_returns_snapshot_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);

    let result = commands::delete::run(&config, None, "does-not-exist", false, None);
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

    let stats = commands::delete::run(&config, None, "snap-delete-dry", true, None).unwrap();
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

    let stats = commands::delete::run(&config, None, "snap-delete-live", false, None).unwrap();
    assert_eq!(stats.snapshot_name, "snap-delete-live");
    assert!(stats.chunks_deleted > 0);

    let after = open_local_repo(&repo_dir);
    assert!(after.manifest().find_snapshot("snap-delete-live").is_none());
    assert_eq!(after.chunk_index().len(), 0);
}
