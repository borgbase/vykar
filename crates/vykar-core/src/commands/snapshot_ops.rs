use crate::index::ChunkIndex;
use crate::repo::Repository;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

use super::list;

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

/// Decrement all chunk refs owned by a snapshot and return orphaned-space impact.
pub fn decrement_snapshot_chunk_refs(
    repo: &mut Repository,
    items_stream: &[u8],
    item_ptrs: &[ChunkId],
) -> Result<SnapshotChunkImpact> {
    decrement_chunk_refs_on_index(repo.chunk_index_mut(), items_stream, item_ptrs)
}
