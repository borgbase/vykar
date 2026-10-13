use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use chrono::Utc;

use crate::config::{RetentionConfig, SourceEntry, VykarConfig};
use crate::prune::{apply_policy_by_label, PruneDecision};
use vykar_types::error::{Result, VykarError};

use super::snapshot_ops::{prepare_removal_targets, remove_snapshots};
use super::util::with_open_repo_maintenance_lock;
use crate::repo::OpenOptions;

/// Aggregate result of a prune operation.
///
/// Phase 2 (deleting `snapshots/<id>` blobs) is the commit point. After that,
/// per-snapshot refcount cleanup and `save_state()` are best-effort — failures
/// are collected into `warnings` rather than propagated as errors, since the
/// snapshot blobs are already durably removed from storage.
///
/// Stats accuracy caveats:
/// - Partial per-snapshot failures contribute no impact to
///   `chunks_deleted`/`space_freed`, so aggregate totals may under-report what
///   was actually freed in-memory. If `save_state()` also fails, totals will
///   over-report what was persisted remotely (the remote index still shows
///   the pre-prune refcounts). `vykar check --repair` is the canonical
///   recovery path for accurate refcount accounting.
pub struct PruneStats {
    pub kept: usize,
    pub pruned: usize,
    pub chunks_deleted: u64,
    pub space_freed: u64,
    pub warnings: Vec<String>,
}

/// Formatted list entry for --list output.
pub struct PruneListEntry {
    pub action: String,
    pub snapshot_name: String,
    pub reasons: Vec<String>,
}

pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    dry_run: bool,
    list: bool,
    sources: &[SourceEntry],
    source_filter: &[String],
    shutdown: Option<&AtomicBool>,
) -> Result<(PruneStats, Vec<PruneListEntry>)> {
    with_open_repo_maintenance_lock(
        config,
        passphrase,
        OpenOptions::new().with_index(),
        |repo| {
            let now = Utc::now();

            // When --source is given, restrict to matching snapshots only
            let target_snapshots = if source_filter.is_empty() {
                repo.manifest().snapshots.clone()
            } else {
                repo.manifest()
                    .snapshots
                    .iter()
                    .filter(|e| source_filter.contains(&e.source_label))
                    .cloned()
                    .collect()
            };

            // Build per-source retention map
            let source_retentions: HashMap<String, RetentionConfig> = sources
                .iter()
                .filter_map(|s| s.retention.as_ref().map(|r| (s.label.clone(), r.clone())))
                .collect();

            // Always group by source_label — labels are intrinsic to each
            // snapshot (recorded at backup time), so retention must apply
            // per-label regardless of whether the local config has a
            // `sources:` block. (Issue #138.)
            if !config.retention.has_any_rule()
                && source_retentions.values().all(|r| !r.has_any_rule())
            {
                return Err(VykarError::Config(
                    "no retention rules configured — set at least one keep_* option in the retention section or per-source".into(),
                ));
            }

            let decisions = apply_policy_by_label(
                &target_snapshots,
                &config.retention,
                &source_retentions,
                now,
            )?;

            // Build list output
            let mut list_entries = Vec::new();
            let mut kept = 0usize;
            let mut to_prune: Vec<String> = Vec::new();

            for entry in &decisions {
                match &entry.decision {
                    PruneDecision::Keep { reasons } => {
                        kept += 1;
                        if list || dry_run {
                            list_entries.push(PruneListEntry {
                                action: "keep".into(),
                                snapshot_name: entry.snapshot_name.clone(),
                                reasons: reasons.clone(),
                            });
                        }
                    }
                    PruneDecision::Prune => {
                        to_prune.push(entry.snapshot_name.clone());
                        if list || dry_run {
                            list_entries.push(PruneListEntry {
                                action: "prune".into(),
                                snapshot_name: entry.snapshot_name.clone(),
                                reasons: Vec::new(),
                            });
                        }
                    }
                }
            }

            if dry_run {
                return Ok((
                    PruneStats {
                        kept,
                        pruned: to_prune.len(),
                        chunks_deleted: 0,
                        space_freed: 0,
                        warnings: Vec::new(),
                    },
                    list_entries,
                ));
            }

            // Process oldest first
            to_prune.reverse();

            let targets = prepare_removal_targets(repo, &to_prune, shutdown)?;
            let mut warnings: Vec<String> = Vec::new();
            let removed = remove_snapshots(repo, targets, "prune", shutdown, &mut warnings)?;
            let total_chunks_deleted = removed.iter().map(|r| r.impact.chunks_deleted).sum();
            let total_space_freed = removed.iter().map(|r| r.impact.space_freed).sum();

            Ok((
                PruneStats {
                    kept,
                    pruned: to_prune.len(),
                    chunks_deleted: total_chunks_deleted,
                    space_freed: total_space_freed,
                    warnings,
                },
                list_entries,
            ))
        },
    )
}
