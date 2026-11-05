use tempfile::TempDir;

use super::helpers;

#[test]
fn backup_records_hostname_override_in_snapshot() {
    let repo_dir = TempDir::new().unwrap();
    let source_dir = TempDir::new().unwrap();
    std::fs::write(source_dir.path().join("f.txt"), b"data").unwrap();

    let mut config = helpers::init_repo(repo_dir.path());
    config.hostname_override = Some("CustomHost".to_string());
    helpers::backup_single_source(&config, source_dir.path(), "src", "snap-host");

    let repo = helpers::open_local_repo(repo_dir.path());
    let entry = repo.manifest().find_snapshot("snap-host").unwrap();
    assert_eq!(entry.hostname, "CustomHost");
}

#[test]
fn backup_uses_short_hostname_by_default() {
    let repo_dir = TempDir::new().unwrap();
    let source_dir = TempDir::new().unwrap();
    std::fs::write(source_dir.path().join("f.txt"), b"data").unwrap();

    let config = helpers::init_repo(repo_dir.path());
    helpers::backup_single_source(&config, source_dir.path(), "src", "snap-default");

    let repo = helpers::open_local_repo(repo_dir.path());
    let entry = repo.manifest().find_snapshot("snap-default").unwrap();
    // Should not contain dots (short hostname)
    assert!(!entry.hostname.contains('.') || entry.hostname == "unknown");
}

#[test]
fn backup_whitespace_hostname_falls_back_to_default() {
    let repo_dir = TempDir::new().unwrap();
    let source_dir = TempDir::new().unwrap();
    std::fs::write(source_dir.path().join("f.txt"), b"data").unwrap();

    let mut config = helpers::init_repo(repo_dir.path());
    config.hostname_override = Some("   ".to_string());
    helpers::backup_single_source(&config, source_dir.path(), "src", "snap-ws");

    let repo = helpers::open_local_repo(repo_dir.path());
    let entry = repo.manifest().find_snapshot("snap-ws").unwrap();
    // Should fall back to short_hostname(), not be whitespace
    assert_ne!(entry.hostname.trim(), "");
    assert!(!entry.hostname.contains('.') || entry.hostname == "unknown");
}
