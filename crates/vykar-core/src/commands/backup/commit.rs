use crate::compress::Compression;
use crate::repo::file_cache::CachedChunks;
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

/// Resolve and bump refcounts for cache-hit chunks, update stats, populate item.chunks.
///
/// Uses `reuse_cached_chunk_ref` which correctly handles committed, recovered
/// (with promotion), and pending chunks. Returns an error if any chunk is
/// unresolvable — this indicates storage corruption, not a recoverable
/// condition (and must NOT silently fall back to a cache miss, which would
/// mask repo/index corruption).
///
/// `csize` is not carried in the local filecache (see `CachedChunkRef`);
/// it's hydrated here from the authoritative `stored_size` returned by
/// `reuse_cached_chunk_ref`.
///
/// Callers handle file-cache insertion themselves (path types differ).
pub(super) fn commit_cache_hit(
    repo: &mut Repository,
    item: &mut Item,
    cached_refs: &CachedChunks,
    stats: &mut SnapshotStats,
) -> Result<()> {
    let refs = cached_refs.as_slice();
    let mut hydrated = Vec::with_capacity(refs.len());
    let mut file_original: u64 = 0;
    let mut file_compressed: u64 = 0;
    for cr in refs {
        let stored_size = repo.reuse_cached_chunk_ref(&cr.id)?;
        file_original += cr.size as u64;
        file_compressed += stored_size as u64;
        hydrated.push(ChunkRef {
            id: cr.id,
            size: cr.size,
            csize: stored_size,
        });
    }
    // Commit only after every ref resolved — leaves `item` untouched on
    // the unresolvable-chunk error path.
    item.chunks = hydrated;
    stats.nfiles += 1;
    stats.original_size += file_original;
    stats.compressed_size += file_compressed;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::file_cache::CachedChunkRef;
    use crate::snapshot::item::ItemType;
    use crate::testutil::test_repo_plaintext;
    use vykar_types::chunk_id::ChunkId;

    fn empty_item() -> Item {
        Item {
            path: "ghost.txt".into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: Some(0),
            size: 42,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
        }
    }

    #[test]
    fn commit_cache_hit_errors_on_missing_chunk() {
        // Missing-chunk semantics: a cached chunk id that isn't resolvable
        // via `reuse_cached_chunk_ref` must surface as an error, never as
        // a silent downgrade to a cache miss (that would mask repo/index
        // corruption). This guards the invariant called out in the plan.
        let mut repo = test_repo_plaintext();
        let mut item = empty_item();
        let mut stats = SnapshotStats::default();
        let phantom = CachedChunks::Single(CachedChunkRef {
            id: ChunkId::from_bytes([0x77; 32]),
            size: 42,
        });

        let err = commit_cache_hit(&mut repo, &mut item, &phantom, &mut stats)
            .expect_err("missing chunk must surface as an error");
        assert!(
            err.to_string().contains("unresolvable chunk"),
            "expected 'unresolvable chunk' in error, got: {err}",
        );
        // On the error path, nothing should have been attributed to stats
        // or written into `item.chunks`.
        assert!(item.chunks.is_empty());
        assert_eq!(stats.nfiles, 0);
        assert_eq!(stats.original_size, 0);
        assert_eq!(stats.compressed_size, 0);
    }
}
