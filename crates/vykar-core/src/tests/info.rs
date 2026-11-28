use std::time::Duration;

use crate::commands;

use super::helpers::{backup_single_source, init_repo, pass};

#[test]
fn info_empty_repo_reports_zero_sizes_and_no_last_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);

    let stats = commands::info::run(&config, pass(&config)).unwrap();
    assert_eq!(stats.snapshot_count, 0);
    assert!(stats.last_snapshot_at.is_none());
    assert_eq!(stats.original_size, 0);
    assert_eq!(stats.compressed_size, 0);
    assert_eq!(stats.deduplicated_size, 0);
    assert_eq!(stats.unique_stored_size, 0);
    assert_eq!(stats.referenced_stored_size, None);
    assert!(stats.last_sweep_at.is_none());
    assert_eq!(stats.unique_chunk_count, 0);
}

#[test]
fn info_reports_referenced_size_larger_than_unique_when_chunks_are_reused() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("a.bin"), b"same-content-for-dedup").unwrap();
    std::fs::write(source_dir.join("b.bin"), b"same-content-for-dedup").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-info-1");
    std::thread::sleep(Duration::from_millis(2));
    backup_single_source(&config, &source_dir, "src-a", "snap-info-2");

    // "Referenced" is the last sweep's figure: none before a compact.
    let stats = commands::info::run(&config, pass(&config)).unwrap();
    assert_eq!(stats.referenced_stored_size, None);
    assert!(stats.last_sweep_at.is_none());

    commands::compact::run(
        &config,
        pass(&config),
        &commands::compact::CompactRequest {
            threshold: 10.0,
            max_repack_size: None,
            dry_run: false,
        },
        &crate::repo::control::CancelToken::new(),
    )
    .unwrap();
    let stats = commands::info::run(&config, pass(&config)).unwrap();
    assert_eq!(stats.snapshot_count, 2);
    assert!(stats.last_snapshot_at.is_some());
    assert!(stats.last_sweep_at.is_some());
    assert!(stats.unique_chunk_count > 0);
    assert!(stats.unique_stored_size > 0);
    let referenced = stats.referenced_stored_size.expect("a sweep ran");
    assert!(referenced > stats.unique_stored_size);
}
