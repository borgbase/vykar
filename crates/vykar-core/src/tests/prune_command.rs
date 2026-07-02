use std::time::Duration;

use crate::commands;
use crate::config::RetentionConfig;
use vykar_types::error::VykarError;

use super::helpers::{
    backup_single_source, init_repo, load_snapshot_cache_from_disk, make_test_config,
    open_local_repo, source_entry,
};

#[test]
fn prune_errors_when_no_retention_rules_are_configured() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let config = init_repo(&repo_dir);
    let sources = vec![source_entry(&source_dir, "src-a")];
    let err = commands::prune::run(&config, None, true, false, &sources, &[], None)
        .err()
        .unwrap();
    assert!(
        matches!(err, VykarError::Config(msg) if msg.contains("no retention rules configured"))
    );
}

#[test]
fn prune_dry_run_reports_without_mutating_state() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("f.txt"), b"prune-dry").unwrap();

    let mut config = init_repo(&repo_dir);
    config.retention = RetentionConfig {
        keep_last: Some(1),
        ..RetentionConfig::default()
    };

    backup_single_source(&config, &source_dir, "src-a", "snap-a-1");
    std::thread::sleep(Duration::from_millis(2));
    backup_single_source(&config, &source_dir, "src-a", "snap-a-2");

    let before = open_local_repo(&repo_dir);
    assert_eq!(before.manifest().snapshots.len(), 2);
    drop(before);

    let sources = vec![source_entry(&source_dir, "src-a")];
    let (stats, list_entries) =
        commands::prune::run(&config, None, true, true, &sources, &[], None)
            .expect("dry-run prune should succeed");

    assert_eq!(stats.kept, 1);
    assert_eq!(stats.pruned, 1);
    assert_eq!(stats.chunks_deleted, 0);
    assert_eq!(stats.space_freed, 0);
    assert_eq!(list_entries.len(), 2);

    let after = open_local_repo(&repo_dir);
    assert_eq!(after.manifest().snapshots.len(), 2);
}

#[test]
fn prune_source_filter_only_prunes_matching_label() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_a = tmp.path().join("source-a");
    let source_b = tmp.path().join("source-b");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_a).unwrap();
    std::fs::create_dir_all(&source_b).unwrap();
    std::fs::write(source_a.join("a.txt"), b"a").unwrap();
    std::fs::write(source_b.join("b.txt"), b"b").unwrap();

    let mut config = make_test_config(&repo_dir);
    config.retention = RetentionConfig {
        keep_last: Some(1),
        ..RetentionConfig::default()
    };
    commands::init::run(&config, None).unwrap();

    backup_single_source(&config, &source_a, "src-a", "snap-a-1");
    std::thread::sleep(Duration::from_millis(2));
    backup_single_source(&config, &source_b, "src-b", "snap-b-1");
    std::thread::sleep(Duration::from_millis(2));
    backup_single_source(&config, &source_a, "src-a", "snap-a-2");

    let sources = vec![
        source_entry(&source_a, "src-a"),
        source_entry(&source_b, "src-b"),
    ];
    let source_filter = vec!["src-a".to_string()];
    let (stats, _list_entries) =
        commands::prune::run(&config, None, false, false, &sources, &source_filter, None).unwrap();

    assert_eq!(stats.pruned, 1);
    assert_eq!(stats.kept, 1);

    let after = open_local_repo(&repo_dir);
    let names: Vec<_> = after
        .manifest()
        .snapshots
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(after.manifest().snapshots.len(), 2);
    assert!(names.contains(&"snap-a-2"));
    assert!(names.contains(&"snap-b-1"));
}

/// Regression test for issue #138: when the local config has no `sources:`
/// block (e.g. a central server running prune against a repo populated by
/// other clients), prune must still group snapshots by `source_label`. Each
/// label is its own retention bucket regardless of local config.
#[test]
fn prune_groups_by_label_even_without_configured_sources() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_a = tmp.path().join("source-a");
    let source_b = tmp.path().join("source-b");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_a).unwrap();
    std::fs::create_dir_all(&source_b).unwrap();
    std::fs::write(source_a.join("a.txt"), b"a").unwrap();
    std::fs::write(source_b.join("b.txt"), b"b").unwrap();

    let mut config = init_repo(&repo_dir);
    config.retention = RetentionConfig {
        keep_last: Some(1),
        ..RetentionConfig::default()
    };

    backup_single_source(&config, &source_a, "a", "snap-a");
    std::thread::sleep(Duration::from_millis(2));
    backup_single_source(&config, &source_b, "b", "snap-b");

    let (stats, _) = commands::prune::run(&config, None, false, false, &[], &[], None)
        .expect("prune should succeed without a sources block");

    assert_eq!(stats.pruned, 0);
    assert_eq!(stats.kept, 2);

    let after = open_local_repo(&repo_dir);
    let names: Vec<_> = after
        .manifest()
        .snapshots
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert!(names.contains(&"snap-a"));
    assert!(names.contains(&"snap-b"));
}

#[test]
fn prune_persists_snapshot_cache_immediately() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("f.txt"), b"prune-cache").unwrap();

    let mut config = init_repo(&repo_dir);
    config.retention = RetentionConfig {
        keep_last: Some(1),
        ..RetentionConfig::default()
    };

    backup_single_source(&config, &source_dir, "src-a", "snap-old");
    std::thread::sleep(Duration::from_millis(2));
    backup_single_source(&config, &source_dir, "src-a", "snap-new");

    // Heal the cache so it reflects both snapshots before the prune.
    drop(open_local_repo(&repo_dir));
    let before = load_snapshot_cache_from_disk(&repo_dir);
    assert!(before.entries.values().any(|e| e.name == "snap-old"));

    let (stats, _) = commands::prune::run(&config, None, false, false, &[], &[], None).unwrap();
    assert_eq!(stats.pruned, 1);
    assert_eq!(stats.kept, 1);

    // The on-disk cache drops the pruned snapshot immediately, without a reopen.
    let after = load_snapshot_cache_from_disk(&repo_dir);
    assert!(
        !after.entries.values().any(|e| e.name == "snap-old"),
        "pruned snapshot still present in on-disk cache"
    );
    assert!(after.entries.values().any(|e| e.name == "snap-new"));
}

/// Without any configured sources, prune still needs a retention rule to do
/// anything — verify the consolidated guard fires.
#[test]
fn prune_errors_without_retention_rules_when_sources_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();

    let config = init_repo(&repo_dir);
    let err = commands::prune::run(&config, None, true, false, &[], &[], None)
        .err()
        .unwrap();
    assert!(
        matches!(err, VykarError::Config(msg) if msg.contains("no retention rules configured"))
    );
}

/// When the snapshot blobs have been deleted from storage (commit point
/// reached) but Phase 3 refcount cleanup fails, `prune::run` must return
/// `Ok((stats, _))` with `stats.warnings` populated rather than propagating
/// an error that would falsely suggest the prune failed.
#[test]
fn prune_phase3_refcount_failure_returns_warning_not_error() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("f.txt"), b"prune-fault").unwrap();

    let mut config = init_repo(&repo_dir);
    config.retention = RetentionConfig {
        keep_last: Some(1),
        ..RetentionConfig::default()
    };

    backup_single_source(&config, &source_dir, "src-a", "snap-a-1");
    std::thread::sleep(Duration::from_millis(2));
    backup_single_source(&config, &source_dir, "src-a", "snap-a-2");

    // Break Phase 3 by removing every pack file. Phase 2 only deletes
    // `snapshots/<id>` and still succeeds.
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

    let sources = vec![source_entry(&source_dir, "src-a")];
    let (stats, _) = commands::prune::run(&config, None, false, false, &sources, &[], None)
        .expect("prune must not fail after commit point — Phase 3 errors become warnings");

    // One snapshot was pruned (blob deleted) but its Phase 3 cleanup failed.
    assert_eq!(stats.pruned, 1);
    assert!(!stats.warnings.is_empty(), "expected a Phase 3 warning");
    let combined = stats.warnings.join("\n");
    assert!(
        combined.contains("vykar check --repair"),
        "warning should point operators at the recovery tool: {combined}"
    );
}
