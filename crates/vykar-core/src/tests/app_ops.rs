use crate::app::operations::{run_backup_for_all_repos, run_backup_for_repo};
use crate::config::{HooksConfig, ResolvedRepo};
use vykar_types::error::VykarError;

use super::helpers::{init_repo, source_entry};

#[test]
fn run_backup_for_repo_rejects_empty_sources() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);

    let err = run_backup_for_repo(&config, &[], None).err().unwrap();
    assert!(matches!(err, VykarError::Config(msg) if msg.contains("no sources configured")));
}

#[test]
fn run_backup_for_all_repos_propagates_passphrase_lookup_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);

    let repos = vec![ResolvedRepo {
        label: Some("repo-a".into()),
        config: config.clone(),
        global_hooks: HooksConfig::default(),
        repo_hooks: HooksConfig::default(),
        sources: Vec::new(),
    }];

    let mut lookup = |_repo: &ResolvedRepo| -> vykar_types::error::Result<Option<String>> {
        Err(VykarError::Other("lookup failed".into()))
    };

    let err = run_backup_for_all_repos(&repos, &mut lookup).err().unwrap();
    assert!(matches!(err, VykarError::Other(msg) if msg == "lookup failed"));
}

#[test]
fn run_backup_for_repo_returns_created_source_report() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("a.txt"), b"backup-report").unwrap();

    let config = init_repo(&repo_dir);
    let sources = vec![source_entry(&source_dir, "src-a")];

    let report = run_backup_for_repo(&config, &sources, None).unwrap();
    assert_eq!(report.created.len(), 1);
    assert_eq!(report.created[0].source_label, "src-a");
    assert_eq!(report.created[0].source_paths.len(), 1);
    assert!(report.created[0].stats.nfiles > 0);
    assert!(!report.created[0].snapshot_name.is_empty());
}
