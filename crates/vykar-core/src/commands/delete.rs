use crate::config::VykarConfig;
use vykar_types::error::Result;

use super::list::{load_snapshot_item_stream, load_snapshot_meta};
use super::snapshot_ops::{count_snapshot_chunk_impact, decrement_snapshot_chunk_refs};
use super::util::with_open_repo_maintenance_lock;
use crate::repo::OpenOptions;

pub struct DeleteStats {
    pub snapshot_name: String,
    pub chunks_deleted: u64,
    pub space_freed: u64,
}

pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dry_run: bool,
    _shutdown: Option<&std::sync::atomic::AtomicBool>,
) -> Result<DeleteStats> {
    with_open_repo_maintenance_lock(
        config,
        passphrase,
        OpenOptions::new().with_index(),
        |repo| {
            // Verify snapshot exists
            let entry = repo
                .manifest()
                .find_snapshot(snapshot_name)
                .ok_or_else(|| {
                    vykar_types::error::VykarError::SnapshotNotFound(snapshot_name.into())
                })?;
            let snapshot_key = entry.id.storage_key();

            // Load snapshot metadata and item stream BEFORE deleting.
            let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
            let items_stream = load_snapshot_item_stream(repo, snapshot_name)?;

            if dry_run {
                let impact =
                    count_snapshot_chunk_impact(repo, &items_stream, &snapshot_meta.item_ptrs)?;

                return Ok(DeleteStats {
                    snapshot_name: snapshot_name.to_string(),
                    chunks_deleted: impact.chunks_deleted,
                    space_freed: impact.space_freed,
                });
            }

            // Delete snapshot object FIRST (commit point).
            // Must succeed — failure aborts before touching refcounts.
            repo.check_lock_fence()?;
            repo.storage.delete(&snapshot_key)?;

            // Remove from in-memory manifest
            repo.manifest_mut().remove_snapshot(snapshot_name);

            // Decrement refcounts using pre-loaded data.
            let impact =
                decrement_snapshot_chunk_refs(repo, &items_stream, &snapshot_meta.item_ptrs)?;

            // Persist index state
            repo.save_state()?;

            Ok(DeleteStats {
                snapshot_name: snapshot_name.to_string(),
                chunks_deleted: impact.chunks_deleted,
                space_freed: impact.space_freed,
            })
        },
    )
}
