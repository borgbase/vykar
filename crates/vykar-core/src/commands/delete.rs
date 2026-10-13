use std::collections::HashSet;
use std::sync::atomic::AtomicBool;

use vykar_types::error::Result;

use super::list::load_item_stream_from_ptrs;
use super::snapshot_ops::{
    decrement_chunk_refs_on_index, prepare_removal_targets, remove_snapshots,
};
use super::util::{check_interrupted, with_open_repo_maintenance_lock};
use crate::repo::OpenOptions;

pub struct DeleteStats {
    pub snapshot_name: String,
    pub chunks_deleted: u64,
    pub space_freed: u64,
}

/// Aggregate result of a delete operation.
///
/// Phase 2 (deleting `snapshots/<id>` blobs) is the commit point. After that,
/// per-snapshot refcount cleanup and `save_state()` are best-effort — failures
/// are collected into `warnings` rather than propagated as errors, since the
/// snapshot is already durably removed from storage.
///
/// Stats accuracy caveats:
/// - A snapshot whose Phase 3 refcount cleanup failed will NOT appear in
///   `stats`, even though its blob was deleted from storage. Check `warnings`
///   for those cases.
/// - Because `decrement_chunk_refs_on_index` mutates the index during item-
///   stream decoding, partial failures can leave refcounts not yet decremented
///   (refcounts are never under-decremented — only over-inflated). Aggregate
///   `space_freed`/`chunks_deleted` may under-report what was actually freed
///   in-memory, and will over-report what was persisted if `save_state()` also
///   fails. `vykar check --repair` is the canonical recovery path for accurate
///   refcount accounting.
pub struct DeleteResult {
    pub stats: Vec<DeleteStats>,
    pub warnings: Vec<String>,
}

pub fn run(
    config: &crate::config::VykarConfig,
    passphrase: Option<&str>,
    snapshot_names: &[&str],
    dry_run: bool,
    shutdown: Option<&AtomicBool>,
) -> Result<DeleteResult> {
    // Phase 0: Deduplicate while preserving order.
    let mut seen = HashSet::new();
    let unique_names: Vec<&str> = snapshot_names
        .iter()
        .copied()
        .filter(|name| seen.insert(*name))
        .collect();

    with_open_repo_maintenance_lock(
        config,
        passphrase,
        OpenOptions::new().with_index(),
        |repo| {
            let targets = prepare_removal_targets(repo, &unique_names, shutdown)?;

            if dry_run {
                // Scratch-index simulation: clone the real index and run
                // decrements against the clone so shared chunks are counted
                // correctly across the batch.
                let mut scratch = repo.chunk_index().clone();
                let mut all_stats = Vec::with_capacity(targets.len());

                for target in &targets {
                    check_interrupted(shutdown)?;
                    let items_stream =
                        load_item_stream_from_ptrs(repo, &target.item_ptrs, shutdown)?;
                    let impact = decrement_chunk_refs_on_index(
                        &mut scratch,
                        &items_stream,
                        &target.item_ptrs,
                    )?;

                    all_stats.push(DeleteStats {
                        snapshot_name: target.snapshot_name.clone(),
                        chunks_deleted: impact.chunks_deleted,
                        space_freed: impact.space_freed,
                    });
                }

                return Ok(DeleteResult {
                    stats: all_stats,
                    warnings: Vec::new(),
                });
            }

            let mut warnings: Vec<String> = Vec::new();
            let all_stats = remove_snapshots(repo, targets, "delete", shutdown, &mut warnings)?
                .into_iter()
                .map(|r| DeleteStats {
                    snapshot_name: r.snapshot_name,
                    chunks_deleted: r.impact.chunks_deleted,
                    space_freed: r.impact.space_freed,
                })
                .collect();

            Ok(DeleteResult {
                stats: all_stats,
                warnings,
            })
        },
    )
}
