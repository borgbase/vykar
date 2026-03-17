use chrono::{DateTime, Duration, TimeZone, Utc};

use crate::config::RetentionConfig;
use crate::prune::{apply_policy, parse_timespan, PruneDecision};
use crate::repo::manifest::SnapshotEntry;
use vykar_types::snapshot_id::SnapshotId;

#[test]
fn parse_duration_days() {
    let d = parse_timespan("7d").unwrap();
    assert_eq!(d, Duration::days(7));
}

#[test]
fn parse_duration_hours() {
    let d = parse_timespan("48h").unwrap();
    assert_eq!(d, Duration::hours(48));
}

#[test]
fn parse_duration_weeks() {
    let d = parse_timespan("2w").unwrap();
    assert_eq!(d, Duration::weeks(2));
}

#[test]
fn parse_duration_pure_numeric() {
    // Pure numeric → days (borg convention)
    let d = parse_timespan("30").unwrap();
    assert_eq!(d, Duration::days(30));
}

#[test]
fn parse_duration_months() {
    let d = parse_timespan("6m").unwrap();
    assert_eq!(d, Duration::days(6 * 30));
}

#[test]
fn parse_duration_years() {
    let d = parse_timespan("1y").unwrap();
    assert_eq!(d, Duration::days(365));
}

#[test]
fn parse_duration_invalid() {
    assert!(parse_timespan("").is_err());
    assert!(parse_timespan("abc").is_err());
    assert!(parse_timespan("5x").is_err());
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

fn snap_at(name: &str, time: DateTime<Utc>) -> SnapshotEntry {
    SnapshotEntry {
        name: name.to_string(),
        id: SnapshotId([0; 32]),
        time,
        source_label: String::new(),
        label: String::new(),
        source_paths: Vec::new(),
        hostname: String::new(),
    }
}

#[test]
fn keep_hourly() {
    let snapshots: Vec<SnapshotEntry> = (0..5)
        .map(|h| {
            snap_at(
                &format!("h{h}"),
                Utc.with_ymd_and_hms(2025, 1, 15, h, 0, 0).unwrap(),
            )
        })
        .collect();

    let policy = RetentionConfig {
        keep_hourly: Some(3),
        ..Default::default()
    };
    let now = Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap();
    let results = apply_policy(&snapshots, &policy, now).unwrap();

    let kept_count = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .count();
    assert_eq!(kept_count, 3);

    // Newest 3 hours (4, 3, 2) should be kept
    let kept_names: Vec<&str> = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .map(|r| r.snapshot_name.as_str())
        .collect();
    assert!(kept_names.contains(&"h4"));
    assert!(kept_names.contains(&"h3"));
    assert!(kept_names.contains(&"h2"));
}

#[test]
fn keep_daily() {
    let snapshots: Vec<SnapshotEntry> = (0..5)
        .map(|d| {
            snap_at(
                &format!("d{d}"),
                Utc.with_ymd_and_hms(2025, 1, 11 + d, 12, 0, 0).unwrap(),
            )
        })
        .collect();

    let policy = RetentionConfig {
        keep_daily: Some(3),
        ..Default::default()
    };
    let now = Utc.with_ymd_and_hms(2025, 1, 20, 0, 0, 0).unwrap();
    let results = apply_policy(&snapshots, &policy, now).unwrap();

    let kept_count = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .count();
    assert_eq!(kept_count, 3);

    // Newest 3 days (Jan 15, 14, 13)
    let kept_names: Vec<&str> = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .map(|r| r.snapshot_name.as_str())
        .collect();
    assert!(kept_names.contains(&"d4")); // Jan 15
    assert!(kept_names.contains(&"d3")); // Jan 14
    assert!(kept_names.contains(&"d2")); // Jan 13
}

#[test]
fn keep_weekly() {
    // Mon 2024-12-30 = ISO week 1 of 2025
    // Sun 2024-12-29 = ISO week 52 of 2024
    // Mon 2024-12-23 = ISO week 52 of 2024 (same week as Dec 29)
    // Mon 2024-12-16 = ISO week 51 of 2024
    let snapshots = vec![
        snap_at("w1", Utc.with_ymd_and_hms(2024, 12, 30, 12, 0, 0).unwrap()),
        snap_at(
            "w52a",
            Utc.with_ymd_and_hms(2024, 12, 29, 12, 0, 0).unwrap(),
        ),
        snap_at(
            "w52b",
            Utc.with_ymd_and_hms(2024, 12, 23, 12, 0, 0).unwrap(),
        ),
        snap_at("w51", Utc.with_ymd_and_hms(2024, 12, 16, 12, 0, 0).unwrap()),
    ];

    let policy = RetentionConfig {
        keep_weekly: Some(2),
        ..Default::default()
    };
    let now = Utc.with_ymd_and_hms(2025, 1, 5, 0, 0, 0).unwrap();
    let results = apply_policy(&snapshots, &policy, now).unwrap();

    let kept_count = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .count();
    assert_eq!(kept_count, 2);

    // Week 1/2025 (Dec 30) and week 52/2024 (Dec 29, newest in that bucket)
    let kept_names: Vec<&str> = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .map(|r| r.snapshot_name.as_str())
        .collect();
    assert!(kept_names.contains(&"w1"));
    assert!(kept_names.contains(&"w52a"));
}

#[test]
fn keep_monthly() {
    let snapshots = vec![
        snap_at("feb", Utc.with_ymd_and_hms(2025, 2, 10, 12, 0, 0).unwrap()),
        snap_at("jan", Utc.with_ymd_and_hms(2025, 1, 20, 12, 0, 0).unwrap()),
        snap_at("dec", Utc.with_ymd_and_hms(2024, 12, 5, 12, 0, 0).unwrap()),
    ];

    let policy = RetentionConfig {
        keep_monthly: Some(2),
        ..Default::default()
    };
    let now = Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap();
    let results = apply_policy(&snapshots, &policy, now).unwrap();

    let kept_count = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .count();
    assert_eq!(kept_count, 2);

    let kept_names: Vec<&str> = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .map(|r| r.snapshot_name.as_str())
        .collect();
    assert!(kept_names.contains(&"feb"));
    assert!(kept_names.contains(&"jan"));
}

#[test]
fn keep_yearly() {
    let snapshots = vec![
        snap_at("y2025", Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap()),
        snap_at("y2024", Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap()),
        snap_at("y2023", Utc.with_ymd_and_hms(2023, 6, 1, 0, 0, 0).unwrap()),
    ];

    let policy = RetentionConfig {
        keep_yearly: Some(2),
        ..Default::default()
    };
    let now = Utc.with_ymd_and_hms(2025, 12, 31, 0, 0, 0).unwrap();
    let results = apply_policy(&snapshots, &policy, now).unwrap();

    let kept_count = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .count();
    assert_eq!(kept_count, 2);

    let kept_names: Vec<&str> = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .map(|r| r.snapshot_name.as_str())
        .collect();
    assert!(kept_names.contains(&"y2025"));
    assert!(kept_names.contains(&"y2024"));
}

#[test]
fn keep_within() {
    let now = Utc::now();
    let snapshots = vec![
        snap_at("recent_1h", now - Duration::hours(1)),
        snap_at("recent_23h", now - Duration::hours(23)),
        snap_at("old_49h", now - Duration::hours(49)),
    ];

    let policy = RetentionConfig {
        keep_within: Some("2d".to_string()),
        ..Default::default()
    };
    let results = apply_policy(&snapshots, &policy, now).unwrap();

    let kept_count = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .count();
    let pruned_count = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Prune))
        .count();
    assert_eq!(kept_count, 2);
    assert_eq!(pruned_count, 1);

    // The 49h-old snapshot exceeds 48h (2d) and should be pruned
    let pruned_names: Vec<&str> = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Prune))
        .map(|r| r.snapshot_name.as_str())
        .collect();
    assert!(pruned_names.contains(&"old_49h"));
}

#[test]
fn combined_rules_union() {
    // 3 snapshots across 3 different days
    let snapshots = vec![
        snap_at("day3", Utc.with_ymd_and_hms(2025, 1, 3, 12, 0, 0).unwrap()),
        snap_at("day2", Utc.with_ymd_and_hms(2025, 1, 2, 12, 0, 0).unwrap()),
        snap_at("day1", Utc.with_ymd_and_hms(2025, 1, 1, 12, 0, 0).unwrap()),
    ];

    let policy = RetentionConfig {
        keep_last: Some(1),
        keep_daily: Some(2),
        ..Default::default()
    };
    let now = Utc.with_ymd_and_hms(2025, 1, 5, 0, 0, 0).unwrap();
    let results = apply_policy(&snapshots, &policy, now).unwrap();

    // keep_last=1 keeps day3; keep_daily=2 keeps day3 (already kept) and day2.
    // Union = exactly {day3, day2} kept; day1 pruned.
    let kept: Vec<_> = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Keep { .. }))
        .collect();
    let pruned: Vec<_> = results
        .iter()
        .filter(|r| matches!(r.decision, PruneDecision::Prune))
        .collect();
    assert_eq!(kept.len(), 2);
    assert_eq!(pruned.len(), 1);
    assert_eq!(pruned[0].snapshot_name, "day1");

    // day3 must carry both "last" and "daily" reasons
    let day3 = kept.iter().find(|r| r.snapshot_name == "day3").unwrap();
    if let PruneDecision::Keep { reasons } = &day3.decision {
        assert!(
            reasons.iter().any(|r| r.starts_with("last")),
            "day3 missing 'last' reason: {reasons:?}"
        );
        assert!(
            reasons.iter().any(|r| r.starts_with("daily")),
            "day3 missing 'daily' reason: {reasons:?}"
        );
    }

    // day2 must carry a "daily" reason
    let day2 = kept.iter().find(|r| r.snapshot_name == "day2").unwrap();
    if let PruneDecision::Keep { reasons } = &day2.decision {
        assert!(
            reasons.iter().any(|r| r.starts_with("daily")),
            "day2 missing 'daily' reason: {reasons:?}"
        );
    }
}

#[test]
fn refuses_to_prune_all() {
    let snapshots = vec![
        snap_at("a", Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()),
        snap_at("b", Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap()),
    ];

    // keep_last: Some(0) means the loop breaks immediately — nothing is kept
    let policy = RetentionConfig {
        keep_last: Some(0),
        ..Default::default()
    };
    let now = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
    let result = apply_policy(&snapshots, &policy, now);
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("ALL snapshots"));
}
