use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Datelike, IsoWeek, Timelike, Utc};

use crate::config::RetentionConfig;
use crate::repo::manifest::SnapshotEntry;
use vykar_types::error::{Result, VykarError};

#[derive(Debug, Clone)]
pub enum PruneDecision {
    Keep { reasons: Vec<String> },
    Prune,
}

#[derive(Debug, Clone)]
pub struct PruneEntry {
    pub snapshot_name: String,
    pub snapshot_time: DateTime<Utc>,
    pub decision: PruneDecision,
}

/// Parse a duration string like "2d", "48h", "1w", "6m", "1y".
/// Pure numeric values are treated as days (borg convention).
pub fn parse_duration(s: &str) -> Result<chrono::Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(VykarError::Config("empty duration string".into()));
    }

    // Try pure numeric → days
    if let Ok(n) = s.parse::<i64>() {
        return Ok(chrono::Duration::days(n));
    }

    // Split into numeric part and suffix
    let (num_str, suffix) = s.split_at(
        s.find(|c: char| !c.is_ascii_digit())
            .ok_or_else(|| VykarError::Config(format!("invalid duration: '{s}'")))?,
    );
    let n: i64 = num_str
        .parse()
        .map_err(|_| VykarError::Config(format!("invalid duration number: '{num_str}'")))?;

    match suffix {
        "h" | "H" => Ok(chrono::Duration::hours(n)),
        "d" | "D" => Ok(chrono::Duration::days(n)),
        "w" | "W" => Ok(chrono::Duration::weeks(n)),
        "m" | "M" => Ok(chrono::Duration::days(n * 30)),
        "y" | "Y" => Ok(chrono::Duration::days(n * 365)),
        _ => Err(VykarError::Config(format!(
            "unknown duration suffix: '{suffix}'"
        ))),
    }
}

/// Time bucket key types for each retention rule.
type HourlyKey = (i32, u32, u32); // (year, ordinal_day, hour)
type DailyKey = (i32, u32); // (year, ordinal_day)
type WeeklyKey = (i32, u32); // (iso_year, iso_week)
type MonthlyKey = (i32, u32); // (year, month)
type YearlyKey = (i32,); // (year,)

fn hourly_key(t: &DateTime<Utc>) -> HourlyKey {
    (t.year(), t.ordinal(), t.hour())
}

fn daily_key(t: &DateTime<Utc>) -> DailyKey {
    (t.year(), t.ordinal())
}

fn weekly_key(t: &DateTime<Utc>) -> WeeklyKey {
    let iw: IsoWeek = t.iso_week();
    (iw.year(), iw.week())
}

fn monthly_key(t: &DateTime<Utc>) -> MonthlyKey {
    (t.year(), t.month())
}

fn yearly_key(t: &DateTime<Utc>) -> YearlyKey {
    (t.year(),)
}

/// Apply a bucket-based retention rule. For each new bucket encountered (up to `max_buckets`),
/// keep the newest snapshot in that bucket. Already-kept snapshots still register their bucket
/// but don't consume a keeper slot.
fn apply_bucket_rule<K: Eq + std::hash::Hash>(
    indices: &[usize],
    times: &[DateTime<Utc>],
    kept: &mut HashSet<usize>,
    reasons: &mut HashMap<usize, Vec<String>>,
    max_buckets: usize,
    key_fn: impl Fn(&DateTime<Utc>) -> K,
    rule_name: &str,
) {
    let mut seen_buckets: HashSet<K> = HashSet::new();
    let mut kept_count = 0usize;

    for &idx in indices {
        let bucket = key_fn(&times[idx]);
        if seen_buckets.contains(&bucket) {
            continue;
        }
        seen_buckets.insert(bucket);

        if kept.contains(&idx) {
            // Already kept by another rule — bucket is consumed but no new keeper needed
            reasons
                .entry(idx)
                .or_default()
                .push(format!("{rule_name} #{}", kept_count + 1));
            kept_count += 1;
        } else if kept_count < max_buckets {
            kept.insert(idx);
            reasons
                .entry(idx)
                .or_default()
                .push(format!("{rule_name} #{}", kept_count + 1));
            kept_count += 1;
        }

        if kept_count >= max_buckets {
            break;
        }
    }
}

/// Apply the retention policy to a list of snapshot entries.
/// Returns a PruneEntry for each snapshot, sorted newest-first.
pub fn apply_policy(
    snapshots: &[SnapshotEntry],
    policy: &RetentionConfig,
    now: DateTime<Utc>,
) -> Result<Vec<PruneEntry>> {
    if snapshots.is_empty() {
        return Ok(Vec::new());
    }

    // Build sorted indices (newest first)
    let mut indices: Vec<usize> = (0..snapshots.len()).collect();
    indices.sort_by(|&a, &b| snapshots[b].time.cmp(&snapshots[a].time));

    let times: Vec<DateTime<Utc>> = snapshots.iter().map(|a| a.time).collect();

    let mut kept: HashSet<usize> = HashSet::new();
    let mut reasons: HashMap<usize, Vec<String>> = HashMap::new();

    // keep_within
    if let Some(ref within_str) = policy.keep_within {
        let dur = parse_duration(within_str)?;
        let cutoff = now - dur;
        for &idx in &indices {
            if times[idx] >= cutoff {
                kept.insert(idx);
                reasons.entry(idx).or_default().push("within".into());
            }
        }
    }

    // keep_last
    if let Some(n) = policy.keep_last {
        for (i, &idx) in indices.iter().enumerate() {
            if i >= n {
                break;
            }
            kept.insert(idx);
            reasons
                .entry(idx)
                .or_default()
                .push(format!("last #{}", i + 1));
        }
    }

    // Bucket rules: hourly → daily → weekly → monthly → yearly
    if let Some(n) = policy.keep_hourly {
        apply_bucket_rule(
            &indices,
            &times,
            &mut kept,
            &mut reasons,
            n,
            hourly_key,
            "hourly",
        );
    }
    if let Some(n) = policy.keep_daily {
        apply_bucket_rule(
            &indices,
            &times,
            &mut kept,
            &mut reasons,
            n,
            daily_key,
            "daily",
        );
    }
    if let Some(n) = policy.keep_weekly {
        apply_bucket_rule(
            &indices,
            &times,
            &mut kept,
            &mut reasons,
            n,
            weekly_key,
            "weekly",
        );
    }
    if let Some(n) = policy.keep_monthly {
        apply_bucket_rule(
            &indices,
            &times,
            &mut kept,
            &mut reasons,
            n,
            monthly_key,
            "monthly",
        );
    }
    if let Some(n) = policy.keep_yearly {
        apply_bucket_rule(
            &indices,
            &times,
            &mut kept,
            &mut reasons,
            n,
            yearly_key,
            "yearly",
        );
    }

    // Safety check: refuse if all snapshots would be pruned
    let prune_count = snapshots.len() - kept.len();
    if prune_count == snapshots.len() {
        return Err(VykarError::Other(
            "refusing to prune: policy would remove ALL snapshots".into(),
        ));
    }

    // Build results in newest-first order
    let entries: Vec<PruneEntry> = indices
        .iter()
        .map(|&idx| {
            let decision = if let Some(r) = reasons.remove(&idx) {
                PruneDecision::Keep { reasons: r }
            } else {
                PruneDecision::Prune
            };
            PruneEntry {
                snapshot_name: snapshots[idx].name.clone(),
                snapshot_time: snapshots[idx].time,
                decision,
            }
        })
        .collect();

    Ok(entries)
}

/// Apply retention policies per source label.
///
/// Snapshots are grouped by `source_label`. Each group uses either a
/// source-specific retention policy (from `source_retentions`) or the
/// `default_retention` if none is defined. Snapshots with an empty
/// source_label are grouped under "".
pub fn apply_policy_by_label(
    snapshots: &[SnapshotEntry],
    default_retention: &RetentionConfig,
    source_retentions: &HashMap<String, RetentionConfig>,
    now: DateTime<Utc>,
) -> Result<Vec<PruneEntry>> {
    // Group snapshots by source_label (preserving original indices).
    let mut groups: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, entry) in snapshots.iter().enumerate() {
        groups
            .entry(entry.source_label.as_str())
            .or_default()
            .push(i);
    }

    let mut all_entries: Vec<PruneEntry> = Vec::new();

    for (label, indices) in &groups {
        let group_snapshots: Vec<SnapshotEntry> =
            indices.iter().map(|&i| snapshots[i].clone()).collect();

        let policy = source_retentions.get(*label).unwrap_or(default_retention);

        if !policy.has_any_rule() {
            // No retention rules for this group — keep all
            for entry in &group_snapshots {
                all_entries.push(PruneEntry {
                    snapshot_name: entry.name.clone(),
                    snapshot_time: entry.time,
                    decision: PruneDecision::Keep {
                        reasons: vec!["no policy".into()],
                    },
                });
            }
            continue;
        }

        let group_entries = apply_policy(&group_snapshots, policy, now)?;
        all_entries.extend(group_entries);
    }

    // Sort newest-first across all groups
    all_entries.sort_by(|a, b| b.snapshot_time.cmp(&a.snapshot_time));

    Ok(all_entries)
}
