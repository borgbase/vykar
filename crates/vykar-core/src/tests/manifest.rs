use chrono::{Duration, Utc};

use crate::repo::manifest::{Manifest, SnapshotEntry};
use vykar_types::snapshot_id::SnapshotId;

fn make_entry(name: &str) -> SnapshotEntry {
    SnapshotEntry {
        name: name.to_string(),
        id: SnapshotId([0u8; 32]),
        time: Utc::now(),
        source_label: String::new(),
        label: String::new(),
        source_paths: Vec::new(),
        hostname: String::new(),
    }
}

fn make_entry_at(name: &str, offset_secs: i64) -> SnapshotEntry {
    SnapshotEntry {
        name: name.to_string(),
        id: SnapshotId([0u8; 32]),
        time: Utc::now() + Duration::seconds(offset_secs),
        source_label: String::new(),
        label: String::new(),
        source_paths: Vec::new(),
        hostname: String::new(),
    }
}

#[test]
fn new_manifest_has_no_snapshots() {
    let m = Manifest::new();
    assert!(m.snapshots.is_empty());
    assert_eq!(m.version, 1);
}

#[test]
fn find_snapshot_returns_match() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry("backup-1"));
    m.snapshots.push(make_entry("backup-2"));
    let found = m.find_snapshot("backup-2");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "backup-2");
}

#[test]
fn find_snapshot_returns_none() {
    let m = Manifest::new();
    assert!(m.find_snapshot("nonexistent").is_none());
}

#[test]
fn remove_snapshot_removes_and_returns() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry("backup-1"));
    m.snapshots.push(make_entry("backup-2"));
    let removed = m.remove_snapshot("backup-1");
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().name, "backup-1");
    assert_eq!(m.snapshots.len(), 1);
    assert!(m.find_snapshot("backup-1").is_none());
}

#[test]
fn remove_snapshot_returns_none() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry("backup-1"));
    let removed = m.remove_snapshot("nonexistent");
    assert!(removed.is_none());
    assert_eq!(m.snapshots.len(), 1);
}

#[test]
fn from_snapshot_entries() {
    let entries = vec![make_entry("backup-1"), make_entry("backup-2")];
    let m = Manifest::from_snapshot_entries(entries);

    assert_eq!(m.version, 1);
    assert_eq!(m.snapshots.len(), 2);
    assert_eq!(m.snapshots[0].name, "backup-1");
    assert_eq!(m.snapshots[1].name, "backup-2");
}

// ---------------------------------------------------------------------------
// resolve_snapshot tests
// ---------------------------------------------------------------------------

#[test]
fn resolve_snapshot_latest_returns_most_recent() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry_at("aaa11111", -10));
    m.snapshots.push(make_entry_at("bbb22222", 0));
    m.snapshots.push(make_entry_at("ccc33333", -5));

    let entry = m.resolve_snapshot("latest").unwrap();
    assert_eq!(entry.name, "bbb22222");
}

#[test]
fn resolve_snapshot_latest_case_insensitive() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry("aaa11111"));

    assert_eq!(m.resolve_snapshot("Latest").unwrap().name, "aaa11111");
    assert_eq!(m.resolve_snapshot("LATEST").unwrap().name, "aaa11111");
}

#[test]
fn resolve_snapshot_latest_empty_repo_returns_snapshot_not_found() {
    let m = Manifest::new();
    let err = m.resolve_snapshot("latest").unwrap_err().to_string();
    assert!(err.contains("snapshot not found"));
    assert!(err.contains("latest"));
}

#[test]
fn resolve_snapshot_exact_match() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry("aaa11111"));
    m.snapshots.push(make_entry("bbb22222"));

    let entry = m.resolve_snapshot("aaa11111").unwrap();
    assert_eq!(entry.name, "aaa11111");
}

#[test]
fn resolve_snapshot_not_found() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry("aaa11111"));

    let err = m.resolve_snapshot("zzz").unwrap_err().to_string();
    assert!(err.contains("snapshot not found"));
}
