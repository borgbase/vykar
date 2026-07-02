use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use chrono::Utc;

use crate::config::{RetentionConfig, SourceEntry, VykarConfig};
use crate::prune::{apply_policy_by_label, PruneDecision};
use vykar_types::error::{Result, VykarError};

use super::list::load_snapshot_meta;
use super::snapshot_ops::{try_cleanup_deleted_snapshot_refs, warn_and_push};
use super::util::{check_interrupted, with_open_repo_maintenance_lock};
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

            // Phase 1: Load snapshot keys and item_ptrs BEFORE deleting anything.
            // Only metadata is retained — item streams are loaded lazily in Phase 3
            // to avoid holding all pruned snapshots' items in memory at once.
            struct PruneTarget {
                snapshot_name: String,
                snapshot_key: String,
                item_ptrs: Vec<vykar_types::chunk_id::ChunkId>,
            }
            let mut targets: Vec<PruneTarget> = Vec::with_capacity(to_prune.len());
            for snapshot_name in &to_prune {
                check_interrupted(shutdown)?;
                let snapshot_key = repo
                    .manifest()
                    .find_snapshot(snapshot_name)
                    .map(|e| e.id.storage_key())
                    .ok_or_else(|| VykarError::SnapshotNotFound(snapshot_name.clone()))?;
                let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
                targets.push(PruneTarget {
                    snapshot_name: snapshot_name.clone(),
                    snapshot_key,
                    item_ptrs: snapshot_meta.item_ptrs,
                });
            }

            // Phase 2: Delete all pruned snapshot objects FIRST (commit point).
            for target in &targets {
                check_interrupted(shutdown)?;
                repo.check_lock_fence()?;
                repo.storage.delete(&target.snapshot_key).map_err(|e| {
                    VykarError::Other(format!(
                        "failed to delete snapshot object {}: {e}",
                        target.snapshot_name
                    ))
                })?;
            }

            // Phase 3: Decrement refcounts. Item streams are reconstructed one at a
            // time from packs (still on storage) using the saved item_ptrs.
            // Per-snapshot failures are best-effort — the blob has already
            // been deleted and cannot be recovered, so we collect warnings
            // and continue rather than aborting the batch.
            let mut total_chunks_deleted = 0u64;
            let mut total_space_freed = 0u64;
            let mut warnings: Vec<String> = Vec::new();
            for target in targets {
                if let Some(impact) = try_cleanup_deleted_snapshot_refs(
                    repo,
                    &target.snapshot_name,
                    &target.item_ptrs,
                    &mut warnings,
                ) {
                    total_chunks_deleted += impact.chunks_deleted;
                    total_space_freed += impact.space_freed;
                    repo.manifest_mut().remove_snapshot(&target.snapshot_name);
                }
            }

            // Single atomic save after all deletions (best-effort).
            if let Err(e) = repo.save_state() {
                warn_and_push(
                    &mut warnings,
                    format!(
                        "snapshots were deleted from storage, but persisting refcount \
                         changes failed: {e}. The chunks_deleted/space_freed totals \
                         reported reflect intended cleanup that did NOT commit — the \
                         remote index still shows the original refcounts and the next \
                         operation will see the pre-prune state. Run `vykar check \
                         --repair` to recover accurate accounting."
                    ),
                );
            }

            // Keep the local snapshot cache consistent with the mutated
            // manifest (best-effort). Snapshot removal from storage is already
            // committed, so this only affects local listing freshness.
            repo.persist_snapshot_cache_from_manifest();

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
