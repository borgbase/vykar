use crate::commands;

use super::helpers::{backup_single_source, init_repo};

#[test]
fn delete_repo_removes_local_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"repo-delete-data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-del-repo");

    assert!(repo_dir.join("config").exists());

    let stats = commands::delete_repo::run(&config).unwrap();
    assert!(stats.keys_deleted > 0);
    assert!(stats.unknown_entries.is_empty());
    assert!(stats.root_removed);
    assert!(!repo_dir.exists());
}

#[test]
fn delete_repo_rejects_nonexistent_repo() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("empty-repo");
    std::fs::create_dir_all(&repo_dir).unwrap();

    let config = super::helpers::make_test_config(&repo_dir);

    let result = commands::delete_repo::run(&config);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");
}

#[test]
fn delete_repo_preserves_unknown_files() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-1");

    // Plant unknown files at the repo root level
    std::fs::write(repo_dir.join("README.md"), b"do not delete me").unwrap();
    std::fs::write(repo_dir.join("notes.txt"), b"also keep me").unwrap();

    let stats = commands::delete_repo::run(&config).unwrap();
    assert!(stats.keys_deleted > 0);

    // Unknown entries should be reported
    assert_eq!(stats.unknown_entries.len(), 2);
    assert!(stats.unknown_entries.contains(&"README.md".to_string()));
    assert!(stats.unknown_entries.contains(&"notes.txt".to_string()));

    // Unknown files should still exist on disk
    assert!(repo_dir.join("README.md").exists());
    assert!(repo_dir.join("notes.txt").exists());

    // Repo root should NOT be removed (it still has unknown files)
    assert!(!stats.root_removed);
    assert!(repo_dir.exists());

    // But vykar files should be gone
    assert!(!repo_dir.join("config").exists());
    assert!(!repo_dir.join("index").exists());
    assert!(!repo_dir.join("index.gen").exists());
    assert!(!repo_dir.join("keys").exists());
}

#[test]
fn delete_repo_handles_partial_repo() {
    // Simulate a partially-deleted repo: config/manifest/index are gone,
    // but packs and snapshots remain (e.g. from a failed prior delete).
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"partial-repo-data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-partial");

    // Remove the files that would have been deleted in a partial first attempt
    std::fs::remove_file(repo_dir.join("config")).unwrap();
    std::fs::remove_file(repo_dir.join("index")).unwrap();
    std::fs::remove_file(repo_dir.join("index.gen")).unwrap();

    // Verify config is gone (old code would fail here with RepoNotFound)
    assert!(!repo_dir.join("config").exists());

    // delete_repo should still succeed because packs/snapshots/keys remain
    let stats = commands::delete_repo::run(&config).unwrap();
    assert!(stats.keys_deleted > 0, "should have deleted remaining keys");
    assert!(stats.unknown_entries.is_empty());
    assert!(stats.root_removed, "repo dir should be fully removed");
    assert!(!repo_dir.exists());
}

#[test]
fn delete_repo_handles_temp_files() {
    // Server-created .tmp.* files should be recognized as known repo keys
    // and included in the delete, not left behind as "unknown".
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"temp-file-data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-tmp");

    // Plant server temp files at root level (from interrupted PUTs)
    std::fs::write(repo_dir.join(".tmp.config.0"), b"partial config").unwrap();
    std::fs::write(repo_dir.join(".tmp.manifest.0"), b"partial manifest").unwrap();

    let stats = commands::delete_repo::run(&config).unwrap();
    assert!(stats.keys_deleted > 0);
    // Temp files should be classified as known, not unknown
    assert!(
        stats.unknown_entries.is_empty(),
        "temp files should not be unknown: {:?}",
        stats.unknown_entries
    );
    assert!(stats.root_removed);
    assert!(!repo_dir.exists());
}

#[test]
fn delete_repo_deletes_all_nested_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    // Create enough data to produce multiple chunks spread across pack shards
    for i in 0..20 {
        let data = vec![(i as u8).wrapping_mul(37); 64 * 1024];
        std::fs::write(source_dir.join(format!("file_{i}.bin")), &data).unwrap();
    }

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-nested");

    // Verify we actually have nested keys (packs under shard dirs, snapshots, etc.)
    let backend = crate::storage::backend_from_config(&config.repository, 1).unwrap();
    let all_keys_before = backend.list("").unwrap();
    let nested_count = all_keys_before.iter().filter(|k| k.contains('/')).count();
    assert!(
        nested_count > 0,
        "expected nested keys (packs, snapshots), got none"
    );
    drop(backend);

    let stats = commands::delete_repo::run(&config).unwrap();
    assert_eq!(stats.keys_deleted, all_keys_before.len() as u64);
    assert!(stats.unknown_entries.is_empty());
    assert!(stats.root_removed);
    assert!(!repo_dir.exists());
}
