use chrono::{Duration, Utc};

use crate::config::RetentionConfig;
use crate::prune::{apply_policy, parse_duration, PruneDecision};
use crate::repo::manifest::SnapshotEntry;
use vykar_types::snapshot_id::SnapshotId;

#[test]
fn parse_duration_days() {
    let d = parse_duration("7d").unwrap();
    assert_eq!(d, Duration::days(7));
}

#[test]
fn parse_duration_hours() {
    let d = parse_duration("48h").unwrap();
    assert_eq!(d, Duration::hours(48));
}

#[test]
fn parse_duration_weeks() {
    let d = parse_duration("2w").unwrap();
    assert_eq!(d, Duration::weeks(2));
}

#[test]
fn parse_duration_pure_numeric() {
    // Pure numeric → days (borg convention)
    let d = parse_duration("30").unwrap();
    assert_eq!(d, Duration::days(30));
}

#[test]
fn parse_duration_invalid() {
    assert!(parse_duration("").is_err());
    assert!(parse_duration("abc").is_err());
    assert!(parse_duration("5x").is_err());
}

fn make_snapshots(count: usize) -> Vec<SnapshotEntry> {
    let now = Utc::now();
    (0..count)
        .map(|i| SnapshotEntry {
            name: format!("backup-{i}"),
            id: SnapshotId([i as u8; 32]),
            time: now - Duration::hours(i as i64),
            source_label: String::new(),
            label: String::new(),
            source_paths: Vec::new(),
            hostname: String::new(),
        })
        .collect()
}

#[test]
fn keep_last_n() {
    let snapshots = make_snapshots(5);
    let policy = RetentionConfig {
        keep_last: Some(2),
        ..Default::default()
    };
    let now = Utc::now();
    let results = apply_policy(&snapshots, &policy, now).unwrap();
    assert_eq!(results.len(), 5);

    let kept: Vec<_> = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .collect();
    let pruned: Vec<_> = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Prune))
        .collect();

    assert_eq!(kept.len(), 2);
    assert_eq!(pruned.len(), 3);
    // The 2 most recent should be kept (backup-0 and backup-1)
    assert!(kept.iter().any(|e| e.snapshot_name == "backup-0"));
    assert!(kept.iter().any(|e| e.snapshot_name == "backup-1"));
}

#[test]
fn empty_snapshots_returns_empty() {
    let policy = RetentionConfig {
        keep_last: Some(5),
        ..Default::default()
    };
    let results = apply_policy(&[], &policy, Utc::now()).unwrap();
    assert!(results.is_empty());
}
