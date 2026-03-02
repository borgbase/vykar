use std::time::Duration;

use crate::commands;
use crate::config::RetentionConfig;
use crate::repo::Repository;
use vykar_storage::local_backend::LocalBackend;
use vykar_types::error::VykarError;

use super::helpers::{
    backup_single_source, init_repo, init_test_environment, make_test_config, source_entry,
};

fn open_local_repo(repo_dir: &std::path::Path) -> Repository {
    init_test_environment();
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    Repository::open(storage, None, None).unwrap()
}

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
