use tempfile::TempDir;

use crate::commands;
use crate::compress::Compression;

use super::helpers::{make_test_config, source_entry};

#[test]
fn open_repo_on_empty_storage_includes_url() {
    let repo_dir = TempDir::new().unwrap();
    let config = make_test_config(repo_dir.path());
    let result = commands::util::open_repo(&config, None, crate::repo::OpenOptions::new());
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected RepoNotFound error"),
    };
    let msg = err.to_string();
    assert!(
        msg.contains(&config.repository.url),
        "error should contain repo URL '{url}', got: {msg}",
        url = config.repository.url,
    );
}

#[test]
fn backup_on_uninitialized_repo_includes_url() {
    let repo_dir = TempDir::new().unwrap();
    let source_dir = TempDir::new().unwrap();

    // Create at least one file so backup doesn't bail with "no source paths".
    std::fs::write(source_dir.path().join("file.txt"), b"hello").unwrap();

    let mut config = make_test_config(repo_dir.path());
    let entry = source_entry(source_dir.path(), "test");
    config.exclude_patterns = entry.exclude.clone();

    let source_paths: Vec<String> = entry.paths.clone();
    let result = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap1",
            passphrase: None,
            source_paths: &source_paths,
            source_label: &entry.label,
            exclude_patterns: &config.exclude_patterns,
            exclude_if_present: &config.exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    );
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected RepoNotFound error"),
    };

    let msg = err.to_string();
    assert!(
        msg.contains(&config.repository.url),
        "error should contain repo URL '{url}', got: {msg}",
        url = config.repository.url,
    );
}
