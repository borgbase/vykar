use crate::repo::Repository;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

use super::list;

#[derive(Debug, Default, Clone, Copy)]
pub struct SnapshotChunkImpact {
    pub chunks_deleted: u64,
    pub space_freed: u64,
}

/// Count chunks that would become unreferenced if a snapshot is deleted.
pub fn count_snapshot_chunk_impact(
    repo: &Repository,
    items_stream: &[u8],
    item_ptrs: &[ChunkId],
) -> Result<SnapshotChunkImpact> {
    let mut impact = SnapshotChunkImpact::default();

    list::for_each_decoded_item(items_stream, |item| {
        for chunk_ref in &item.chunks {
            if let Some(entry) = repo.chunk_index().get(&chunk_ref.id) {
                if entry.refcount == 1 {
                    impact.chunks_deleted += 1;
                    impact.space_freed += entry.stored_size as u64;
                }
            }
        }
        Ok(())
    })?;

    for chunk_id in item_ptrs {
        if let Some(entry) = repo.chunk_index().get(chunk_id) {
            if entry.refcount == 1 {
                impact.chunks_deleted += 1;
                impact.space_freed += entry.stored_size as u64;
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
    let mut impact = SnapshotChunkImpact::default();

    list::for_each_decoded_item(items_stream, |item| {
        for chunk_ref in &item.chunks {
            if let Some((rc, size)) = repo.chunk_index_mut().decrement(&chunk_ref.id) {
                if rc == 0 {
                    impact.chunks_deleted += 1;
                    impact.space_freed += size as u64;
                }
            }
        }
        Ok(())
    })?;

    for chunk_id in item_ptrs {
        if let Some((rc, size)) = repo.chunk_index_mut().decrement(chunk_id) {
            if rc == 0 {
                impact.chunks_deleted += 1;
                impact.space_freed += size as u64;
            }
        }
    }

    Ok(impact)
}
