use crate::compress::Compression;
use crate::repo::pack::PackType;
use crate::repo::Repository;
use crate::snapshot::item::{ChunkRef, Item};
use crate::snapshot::SnapshotStats;
use vykar_types::error::Result;

use super::chunk_process::WorkerChunk;

/// Commit worker chunks into a repository, updating item and stats.
///
/// Shared by `ProcessedFile` and `FileSegment` consumer arms.
pub(super) fn process_worker_chunks(
    repo: &mut Repository,
    item: &mut Item,
    chunks: impl IntoIterator<Item = WorkerChunk>,
    stats: &mut SnapshotStats,
    compression: Compression,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<()> {
    for worker_chunk in chunks {
        match worker_chunk {
            WorkerChunk::Prepared(prepared) => {
                let size = prepared.uncompressed_size;
                let existing = if dedup_filter.is_some() {
                    repo.bump_ref_prefilter_miss(&prepared.chunk_id)
                } else {
                    repo.bump_ref_if_exists(&prepared.chunk_id)
                };
                if let Some(csize) = existing {
                    stats.original_size += size as u64;
                    stats.compressed_size += csize as u64;
                    item.chunks.push(ChunkRef {
                        id: prepared.chunk_id,
                        size,
                        csize,
                    });
                } else {
                    let csize = repo.commit_prepacked_chunk(
                        prepared.chunk_id,
                        prepared.packed,
                        PackType::Data,
                    )?;
                    stats.original_size += size as u64;
                    stats.compressed_size += csize as u64;
                    stats.deduplicated_size += csize as u64;
                    item.chunks.push(ChunkRef {
                        id: prepared.chunk_id,
                        size,
                        csize,
                    });
                }
            }
            WorkerChunk::Hashed(hashed) => {
                let size = hashed.data.len() as u32;
                if let Some(csize) = repo.bump_ref_prefilter_hit(&hashed.chunk_id) {
                    // True dedup hit — skip transform.
                    stats.original_size += size as u64;
                    stats.compressed_size += csize as u64;
                    item.chunks.push(ChunkRef {
                        id: hashed.chunk_id,
                        size,
                        csize,
                    });
                } else {
                    // False positive — inline compress+encrypt.
                    let csize = repo.commit_chunk_inline(
                        hashed.chunk_id,
                        &hashed.data,
                        compression,
                        PackType::Data,
                    )?;
                    stats.original_size += size as u64;
                    stats.compressed_size += csize as u64;
                    stats.deduplicated_size += csize as u64;
                    item.chunks.push(ChunkRef {
                        id: hashed.chunk_id,
                        size,
                        csize,
                    });
                }
            }
        }
    }
    Ok(())
}

/// Increment chunk refcounts, update stats, populate item.chunks.
///
/// Callers handle file-cache insertion themselves (path types differ).
pub(super) fn commit_cache_hit(
    repo: &mut Repository,
    item: &mut Item,
    cached_refs: Vec<ChunkRef>,
    stats: &mut SnapshotStats,
) {
    let mut file_original: u64 = 0;
    let mut file_compressed: u64 = 0;
    for cr in &cached_refs {
        repo.increment_chunk_ref(&cr.id);
        file_original += cr.size as u64;
        file_compressed += cr.csize as u64;
    }
    stats.nfiles += 1;
    stats.original_size += file_original;
    stats.compressed_size += file_compressed;
    item.chunks = cached_refs;
}
