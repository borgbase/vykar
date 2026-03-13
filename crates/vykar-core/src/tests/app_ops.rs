use crate::app::operations::run_backup_selection;
use vykar_types::error::VykarError;

use super::helpers::{init_repo, resolved_repo, source_entry};

#[test]
fn run_backup_selection_rejects_empty_sources() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);
    let repo = resolved_repo(config, Vec::new());

    let err = run_backup_selection(&repo, &[], None, None, false, None)
        .err()
        .unwrap();
    assert!(matches!(err, VykarError::Config(msg) if msg.contains("no sources configured")));
}

#[test]
fn run_backup_selection_returns_created_source_report() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("a.txt"), b"backup-report").unwrap();

    let config = init_repo(&repo_dir);
    let sources = vec![source_entry(&source_dir, "src-a")];
    let repo = resolved_repo(config, sources.clone());

    let report = run_backup_selection(&repo, &sources, None, None, false, None).unwrap();
    assert_eq!(report.created.len(), 1);
    assert_eq!(report.created[0].source_label, "src-a");
    assert_eq!(report.created[0].source_paths.len(), 1);
    assert!(report.created[0].stats.nfiles > 0);
    assert!(!report.created[0].snapshot_name.is_empty());
}
