use tracing::warn;

use crate::index::ChunkIndex;
use crate::repo::Repository;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

use super::list;

/// Append `msg` to `warnings` and emit `tracing::warn!` for it. Both actions
/// always happen together: warnings are the surface-visible half, tracing is
/// the log-file half — `warn_and_push` prevents drift.
pub fn warn_and_push(warnings: &mut Vec<String>, msg: String) {
    warn!("{msg}");
    warnings.push(msg);
}

/// Attempt Phase 3 refcount cleanup for a single snapshot whose blob has
/// already been deleted (the commit point has been crossed). On success
/// returns the impact; on failure appends a warning referencing
/// `vykar check --repair` as the recovery path and returns `None`.
///
/// Shared between `commands::delete` and `commands::prune` — they differ only
/// in how they handle the returned impact (per-snapshot stats vs aggregate).
pub fn try_cleanup_deleted_snapshot_refs(
    repo: &mut Repository,
    snapshot_name: &str,
    item_ptrs: &[ChunkId],
    warnings: &mut Vec<String>,
) -> Option<SnapshotChunkImpact> {
    let result = (|| -> Result<SnapshotChunkImpact> {
        let items_stream = list::load_item_stream_from_ptrs(repo, item_ptrs)?;
        decrement_chunk_refs_on_index(repo.chunk_index_mut(), &items_stream, item_ptrs)
    })();
    match result {
        Ok(impact) => Some(impact),
        Err(e) => {
            warn_and_push(
                warnings,
                format!(
                    "snapshot '{snapshot_name}' was deleted from storage, but refcount \
                     cleanup failed: {e}. Refcounts and space accounting will remain \
                     inflated until `vykar check --repair` rebuilds them from surviving \
                     snapshots."
                ),
            );
            None
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SnapshotChunkImpact {
    pub chunks_deleted: u64,
    pub space_freed: u64,
}

/// Decrement all chunk refs for a snapshot's items against an arbitrary index.
///
/// Used by both the live delete path (operating on the repo's real index) and
/// the dry-run path (operating on a cloned scratch index).
pub fn decrement_chunk_refs_on_index(
    index: &mut ChunkIndex,
    items_stream: &[u8],
    item_ptrs: &[ChunkId],
) -> Result<SnapshotChunkImpact> {
    let mut impact = SnapshotChunkImpact::default();

    list::for_each_decoded_item(items_stream, |item| {
        for chunk_ref in &item.chunks {
            if let Some((rc, size)) = index.decrement(&chunk_ref.id) {
                if rc == 0 {
                    impact.chunks_deleted += 1;
                    impact.space_freed += size as u64;
                }
            }
        }
        Ok(())
    })?;

    for chunk_id in item_ptrs {
        if let Some((rc, size)) = index.decrement(chunk_id) {
            if rc == 0 {
                impact.chunks_deleted += 1;
                impact.space_freed += size as u64;
            }
        }
    }

    Ok(impact)
}

/// Persist the refcount decrements made after a batch of snapshot blobs was
/// deleted, and re-sync the local snapshot list cache with the mutated
/// manifest.
///
/// Both steps are past the commit point, so neither can fail the operation:
/// a `save_state()` error becomes a warning that names `vykar check --repair`
/// as the recovery path, and the cache refresh is best-effort (a stale cache
/// only affects local listing freshness and self-heals on the next open).
///
/// `op` names the operation in the warning ("delete" / "prune").
pub fn commit_snapshot_removals(repo: &mut Repository, op: &str, warnings: &mut Vec<String>) {
    if let Err(e) = repo.save_state() {
        warn_and_push(
            warnings,
            format!(
                "snapshots were deleted from storage, but persisting refcount \
                 changes failed: {e}. The chunks_deleted/space_freed totals \
                 reported reflect intended cleanup that did NOT commit — the \
                 remote index still shows the original refcounts and the next \
                 operation will see the pre-{op} state. Run `vykar check \
                 --repair` to recover accurate accounting."
            ),
        );
    }

    repo.persist_snapshot_cache_from_manifest();
}
