use crate::compress::Compression;
use crate::index::dedup_cache::DedupPrefilter;
use crate::repo::file_cache::CachedChunks;
use crate::repo::pack::PackType;
use crate::repo::Repository;
use crate::snapshot::item::{ChunkRef, Item};
use crate::snapshot::SnapshotStats;
use vykar_types::error::{Result, VykarError};

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
    prefilter: Option<&DedupPrefilter>,
) -> Result<()> {
    for worker_chunk in chunks {
        match worker_chunk {
            WorkerChunk::Prepared(prepared) => {
                let size = prepared.uncompressed_size;
                // With a prefilter, the worker established the chunk is not
                // in the pre-session set; it can only dedup against chunks
                // added during this session.
                let existing = if prefilter.is_some() {
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
            WorkerChunk::Existing(existing) => {
                let prefilter = prefilter.ok_or_else(|| {
                    VykarError::Other(format!(
                        "BUG: pre-session dedup hit for chunk {} without a prefilter",
                        existing.chunk_id
                    ))
                })?;
                repo.bump_ref_presession_hit(
                    &existing.chunk_id,
                    existing.stored_size,
                    existing.cache_position,
                    prefilter,
                )?;
                let size = existing.uncompressed_size;
                let csize = existing.stored_size;
                stats.original_size += size as u64;
                stats.compressed_size += csize as u64;
                item.chunks.push(ChunkRef {
                    id: existing.chunk_id,
                    size,
                    csize,
                });
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
    use crate::testutil::test_repo_plaintext;
    use vykar_types::chunk_id::ChunkId;

    fn empty_item() -> Item {
        Item {
            ctime: Some(0),
            size: 42,
            ..Item::test_file("ghost.txt")
        }
    }

    mod tiered {
        use super::*;
        use crate::commands::backup::chunk_process::{classify_chunk, WorkerChunk};
        use crate::index::dedup_cache::{
            build_dedup_cache_to_path, chunk_id_to_u64, MmapDedupCache, TieredDedupIndex,
        };
        use crate::index::ChunkIndex;
        use std::borrow::Cow;
        use vykar_types::pack_id::PackId;

        /// Write a dedup cache holding `stored` and return its open path.
        fn write_cache(dir: &tempfile::TempDir, stored: &[(ChunkId, u32)]) -> std::path::PathBuf {
            let path = dir.path().join("dedup_cache");
            let mut index = ChunkIndex::new();
            for &(id, stored_size) in stored {
                index.add(id, stored_size, PackId::from_bytes([0x01; 32]), 0);
            }
            build_dedup_cache_to_path(&index, 1, &path).unwrap();
            path
        }

        fn classify(repo: &Repository, prefilter: &DedupPrefilter, data: &[u8]) -> WorkerChunk {
            let id = ChunkId::compute(repo.crypto.chunk_hasher(), data);
            classify_chunk(
                id,
                Cow::Borrowed(data),
                Some(prefilter),
                Compression::None,
                repo.crypto.as_ref(),
            )
            .unwrap()
        }

        #[test]
        fn presession_hit_records_reference_without_data() {
            let mut repo = test_repo_plaintext();
            let data = b"already stored before this session".to_vec();
            let id = ChunkId::compute(repo.crypto.chunk_hasher(), &data);
            let dir = tempfile::tempdir().unwrap();
            let path = write_cache(&dir, &[(id, 77)]);
            repo.enable_tiered_dedup_mode_with(MmapDedupCache::open_path(&path, 1).unwrap());
            let prefilter = repo.dedup_prefilter().unwrap();

            let chunk = classify(&repo, &prefilter, &data);
            assert!(matches!(chunk, WorkerChunk::Existing(_)));
            let mut item = empty_item();
            let mut stats = SnapshotStats::default();
            process_worker_chunks(
                &mut repo,
                &mut item,
                [chunk],
                &mut stats,
                Compression::None,
                Some(&prefilter),
            )
            .unwrap();

            assert_eq!(item.chunks.len(), 1);
            assert_eq!(item.chunks[0].id, id);
            assert_eq!(item.chunks[0].size, data.len() as u32);
            assert_eq!(item.chunks[0].csize, 77);
            assert_eq!(stats.original_size, data.len() as u64);
            assert_eq!(stats.compressed_size, 77);
            assert_eq!(stats.deduplicated_size, 0);
            assert_eq!(repo.delta_refcount_bump(&id), 1);
        }

        /// A data-less hit whose verdict came from a different mapping than
        /// the active tiered index must fail loudly, not record a reference.
        #[test]
        fn presession_hit_from_foreign_prefilter_errors() {
            let mut repo = test_repo_plaintext();
            let data = b"already stored before this session".to_vec();
            let id = ChunkId::compute(repo.crypto.chunk_hasher(), &data);
            let dir = tempfile::tempdir().unwrap();
            let path = write_cache(&dir, &[(id, 77)]);
            repo.enable_tiered_dedup_mode_with(MmapDedupCache::open_path(&path, 1).unwrap());
            let foreign = TieredDedupIndex::new(MmapDedupCache::open_path(&path, 1).unwrap())
                .prefilter()
                .unwrap();

            let chunk = classify(&repo, &foreign, &data);
            let mut item = empty_item();
            let mut stats = SnapshotStats::default();
            let err = process_worker_chunks(
                &mut repo,
                &mut item,
                [chunk],
                &mut stats,
                Compression::None,
                Some(&foreign),
            )
            .expect_err("foreign prefilter must be rejected");
            assert!(err.to_string().contains("pre-session dedup hit"), "{err}");
            assert!(item.chunks.is_empty());
            assert_eq!(stats.original_size, 0);
            assert_eq!(repo.delta_refcount_bump(&id), 0);
        }

        /// Filter false positives are packed in the worker; two copies of
        /// the same new chunk must still dedup against each other.
        #[test]
        fn false_positives_packed_in_worker_dedup_within_session() {
            let mut repo = test_repo_plaintext();
            let data = b"new in this session, colliding filter key".to_vec();
            let id = ChunkId::compute(repo.crypto.chunk_hasher(), &data);
            let mut colliding = *id.as_bytes();
            colliding[31] ^= 0xFF;
            let colliding = ChunkId::from_bytes(colliding);
            assert_eq!(chunk_id_to_u64(&colliding), chunk_id_to_u64(&id));
            let dir = tempfile::tempdir().unwrap();
            let path = write_cache(&dir, &[(colliding, 77)]);
            repo.enable_tiered_dedup_mode_with(MmapDedupCache::open_path(&path, 1).unwrap());
            let prefilter = repo.dedup_prefilter().unwrap();

            let mut stats = SnapshotStats::default();
            let mut items = [empty_item(), empty_item()];
            for item in &mut items {
                let chunk = classify(&repo, &prefilter, &data);
                assert!(matches!(chunk, WorkerChunk::Prepared(_)));
                process_worker_chunks(
                    &mut repo,
                    item,
                    [chunk],
                    &mut stats,
                    Compression::None,
                    Some(&prefilter),
                )
                .unwrap();
            }

            let csize = items[0].chunks[0].csize;
            assert_eq!(items[1].chunks[0].id, id);
            assert_eq!(items[1].chunks[0].csize, csize);
            assert_eq!(stats.original_size, 2 * data.len() as u64);
            assert_eq!(stats.compressed_size, 2 * csize as u64);
            assert_eq!(stats.deduplicated_size, csize as u64, "stored once");
            repo.flush_packs().unwrap();
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
