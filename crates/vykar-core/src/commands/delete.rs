use std::collections::HashSet;
use std::sync::atomic::AtomicBool;

use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

use super::list::{load_item_stream_from_ptrs, load_snapshot_meta};
use super::snapshot_ops::{decrement_chunk_refs_on_index, decrement_snapshot_chunk_refs};
use super::util::{check_interrupted, with_open_repo_maintenance_lock};
use crate::repo::OpenOptions;

pub struct DeleteStats {
    pub snapshot_name: String,
    pub chunks_deleted: u64,
    pub space_freed: u64,
}

pub fn run(
    config: &crate::config::VykarConfig,
    passphrase: Option<&str>,
    snapshot_names: &[&str],
    dry_run: bool,
    shutdown: Option<&AtomicBool>,
) -> Result<Vec<DeleteStats>> {
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
            // Validate all snapshots exist before any mutation.
            for name in &unique_names {
                repo.manifest().find_snapshot(name).ok_or_else(|| {
                    vykar_types::error::VykarError::SnapshotNotFound((*name).into())
                })?;
            }

            // Phase 1: Capture lightweight metadata (keys + item_ptrs only).
            struct DeleteTarget {
                snapshot_name: String,
                snapshot_key: String,
                item_ptrs: Vec<ChunkId>,
            }
            let mut targets: Vec<DeleteTarget> = Vec::with_capacity(unique_names.len());
            for name in &unique_names {
                check_interrupted(shutdown)?;
                let snapshot_key = repo
                    .manifest()
                    .find_snapshot(name)
                    .map(|e| e.id.storage_key())
                    .expect("validated above");
                let snapshot_meta = load_snapshot_meta(repo, name)?;
                targets.push(DeleteTarget {
                    snapshot_name: name.to_string(),
                    snapshot_key,
                    item_ptrs: snapshot_meta.item_ptrs,
                });
            }

            if dry_run {
                // Scratch-index simulation: clone the real index and run
                // decrements against the clone so shared chunks are counted
                // correctly across the batch.
                let mut scratch = repo.chunk_index().clone();
                let mut all_stats = Vec::with_capacity(targets.len());

                for target in &targets {
                    check_interrupted(shutdown)?;
                    let items_stream = load_item_stream_from_ptrs(repo, &target.item_ptrs)?;
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

                return Ok(all_stats);
            }

            // Phase 2: Delete snapshot blobs (commit point).
            for target in &targets {
                check_interrupted(shutdown)?;
                repo.check_lock_fence()?;
                repo.storage.delete(&target.snapshot_key)?;
            }

            // Phase 3: Decrement refcounts with lazy item-stream loading.
            let mut all_stats = Vec::with_capacity(targets.len());
            for target in targets {
                let items_stream = load_item_stream_from_ptrs(repo, &target.item_ptrs)?;
                let impact = decrement_snapshot_chunk_refs(repo, &items_stream, &target.item_ptrs)?;
                repo.manifest_mut().remove_snapshot(&target.snapshot_name);
                all_stats.push(DeleteStats {
                    snapshot_name: target.snapshot_name,
                    chunks_deleted: impact.chunks_deleted,
                    space_freed: impact.space_freed,
                });
            }

            repo.save_state()?;

            Ok(all_stats)
        },
    )
}
