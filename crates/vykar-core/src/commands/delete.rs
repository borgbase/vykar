use crate::config::VykarConfig;
use tracing::warn;
use vykar_types::error::Result;

use super::list::{load_snapshot_item_stream, load_snapshot_meta};
use super::snapshot_ops::{count_snapshot_chunk_impact, decrement_snapshot_chunk_refs};
use super::util::with_open_repo_maintenance_lock;

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
    with_open_repo_maintenance_lock(config, passphrase, |repo| {
        // Verify snapshot exists
        let entry = repo
            .manifest()
            .find_snapshot(snapshot_name)
            .ok_or_else(|| {
                vykar_types::error::VykarError::SnapshotNotFound(snapshot_name.into())
            })?;
        let snapshot_key = entry.id.storage_key();

        // Load snapshot metadata and item stream to find all chunk refs.
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

        // Orphaned blobs remain in pack files until a future `compact` command.
        let impact = decrement_snapshot_chunk_refs(repo, &items_stream, &snapshot_meta.item_ptrs)?;

        // Remove from manifest
        repo.manifest_mut().remove_snapshot(snapshot_name);

        // Persist state
        repo.save_state()?;

        // Best-effort cleanup of snapshot metadata object.
        // If this fails after state is persisted, the repo remains consistent and
        // only leaves an orphaned metadata object.
        if let Err(err) = repo.storage.delete(&snapshot_key) {
            warn!(
                snapshot = %snapshot_name,
                key = %snapshot_key,
                error = %err,
                "failed to delete snapshot metadata object after state commit"
            );
        }

        Ok(DeleteStats {
            snapshot_name: snapshot_name.to_string(),
            chunks_deleted: impact.chunks_deleted,
            space_freed: impact.space_freed,
        })
    })
}
