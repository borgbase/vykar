use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use chrono::Utc;
use tracing::warn;

use crate::config::{RetentionConfig, SourceEntry, VykarConfig};
use crate::prune::{apply_policy, apply_policy_by_label, PruneDecision};
use vykar_types::error::{Result, VykarError};

use super::list::{load_snapshot_item_stream, load_snapshot_meta};
use super::snapshot_ops::decrement_snapshot_chunk_refs;
use super::util::{check_interrupted, with_open_repo_maintenance_lock};

pub struct PruneStats {
    pub kept: usize,
    pub pruned: usize,
    pub chunks_deleted: u64,
    pub space_freed: u64,
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
    with_open_repo_maintenance_lock(config, passphrase, |repo| {
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

        let has_sources = !sources.is_empty();

        let decisions = if has_sources {
            // Label-aware: group by source_label and apply per-source retention
            if !config.retention.has_any_rule()
                && source_retentions.values().all(|r| !r.has_any_rule())
            {
                return Err(VykarError::Config(
                    "no retention rules configured — set at least one keep_* option in the retention section or per-source".into(),
                ));
            }
            apply_policy_by_label(
                &target_snapshots,
                &config.retention,
                &source_retentions,
                now,
            )?
        } else {
            // No sources — fall back to flat policy
            if !config.retention.has_any_rule() {
                return Err(VykarError::Config(
                    "no retention rules configured — set at least one keep_* option in the retention section".into(),
                ));
            }
            apply_policy(&target_snapshots, &config.retention, now)?
        };

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
                },
                list_entries,
            ));
        }

        // Delete pruned snapshots (process oldest first)
        to_prune.reverse();
        let mut total_chunks_deleted = 0u64;
        let mut total_space_freed = 0u64;
        let mut metadata_keys_to_delete: Vec<(String, String)> = Vec::with_capacity(to_prune.len());

        for snapshot_name in &to_prune {
            check_interrupted(shutdown)?;
            // Get snapshot ID before we modify manifest
            let snapshot_key = repo
                .manifest()
                .find_snapshot(snapshot_name)
                .map(|e| e.id.storage_key())
                .ok_or_else(|| VykarError::SnapshotNotFound(snapshot_name.clone()))?;

            let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
            let items_stream = load_snapshot_item_stream(repo, snapshot_name)?;

            // Orphaned blobs remain in pack files until a future `compact` command.
            let impact =
                decrement_snapshot_chunk_refs(repo, &items_stream, &snapshot_meta.item_ptrs)?;
            total_chunks_deleted += impact.chunks_deleted;
            total_space_freed += impact.space_freed;

            // Remove from manifest
            repo.manifest_mut().remove_snapshot(snapshot_name);
            metadata_keys_to_delete.push((snapshot_name.clone(), snapshot_key));
        }

        // Single atomic save after all deletions
        repo.save_state()?;

        // Best-effort cleanup of snapshot metadata objects after state commit.
        for (snapshot_name, snapshot_key) in metadata_keys_to_delete {
            if let Err(err) = repo.storage.delete(&snapshot_key) {
                warn!(
                    snapshot = %snapshot_name,
                    key = %snapshot_key,
                    error = %err,
                    "failed to delete snapshot metadata object after state commit"
                );
            }
        }

        Ok((
            PruneStats {
                kept,
                pruned: to_prune.len(),
                chunks_deleted: total_chunks_deleted,
                space_freed: total_space_freed,
            },
            list_entries,
        ))
    })
}
