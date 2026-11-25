use tracing::debug;

use crate::index::table::DedupPrefilter;
use crate::platform::fs;
use crate::repo::file_cache::{CachedChunks, FileCache, RetainedHit};
use crate::repo::pack::PackType;
use crate::repo::{Repo, WriteSession};
use crate::snapshot::SnapshotStats;
use crate::snapshot::item::{ChunkRef, Item};
use vykar_types::error::{Result, VykarError};

use super::chunk_process::{ExistingAt, WorkerChunk};
use super::{
    BackupProgressEvent, FileStatus, ListingWriter, emit_progress, emit_stats_progress,
    stamp_item_metadata,
};

/// Commit worker chunks into a repository, updating item and stats.
///
/// Shared by `ProcessedFile` and `FileSegment` consumer arms.
pub(super) fn process_worker_chunks(
    repo: &Repo,
    ws: &mut WriteSession,
    item: &mut Item,
    chunks: impl IntoIterator<Item = WorkerChunk>,
    stats: &mut SnapshotStats,
    prefilter: Option<&DedupPrefilter>,
) -> Result<()> {
    for worker_chunk in chunks {
        match worker_chunk {
            WorkerChunk::Prepared(prepared) => {
                let size = prepared.uncompressed_size;
                // With a prefilter, the worker established the chunk is not
                // in the session's view; it can only dedup against chunks
                // added during this session.
                let existing = if prefilter.is_some() {
                    ws.bump_ref_prefilter_miss(repo, &prepared.chunk_id)
                } else {
                    ws.bump_ref_if_exists(repo, &prepared.chunk_id)
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
                    let csize = ws.commit_prepacked_chunk(
                        repo,
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
                match existing.at {
                    ExistingAt::Table(position) => ws.bump_ref_presession_hit(
                        repo,
                        &existing.chunk_id,
                        existing.stored_size,
                        position,
                        prefilter,
                    )?,
                    ExistingAt::Added => ws.bump_ref_presession_added(
                        repo,
                        &existing.chunk_id,
                        existing.stored_size,
                        prefilter,
                    )?,
                }
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
        }
    }
    Ok(())
}

/// Resolve cache-hit chunks and record their reuse, update stats, populate item.chunks.
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
/// Counting the file, the listing append and the file-cache update are the
/// epilogue's ([`finish_file`]).
pub(super) fn commit_cache_hit(
    repo: &Repo,
    ws: &mut WriteSession,
    item: &mut Item,
    cached_refs: &CachedChunks,
    stats: &mut SnapshotStats,
) -> Result<()> {
    let refs = cached_refs.as_slice();
    let mut hydrated = Vec::with_capacity(refs.len());
    let mut file_original: u64 = 0;
    let mut file_compressed: u64 = 0;
    for cr in refs {
        let stored_size = ws.reuse_cached_chunk_ref(repo, &cr.id)?;
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
    stats.original_size += file_original;
    stats.compressed_size += file_compressed;
    Ok(())
}

/// What the per-file epilogue needs to know about the file cache.
pub(super) struct FileCaches<'c> {
    /// The loaded cache of the previous run: decides New vs Modified in
    /// verbose output.
    pub(super) old: &'c FileCache,
    /// This run's write cache.
    pub(super) new: &'c mut FileCache,
}

/// How a finished file's content was obtained; decides stamping, the
/// verbose status, the file-cache update and where the item goes.
pub(super) enum FileKind<'c> {
    /// Content read and committed by the pipeline (a whole file, or every
    /// segment of a large one). The item is stamped from `pre_meta`, the
    /// descriptor `fstat` the drift checks validated, and a fresh cache
    /// entry is built from the item's chunk list.
    Read {
        abs_path: &'c str,
        pre_meta: fs::MetadataSummary,
        caches: FileCaches<'c>,
    },
    /// Unchanged file whose chunks came from the file cache or the parent
    /// reuse index. The item keeps its walk metadata.
    CacheHit {
        abs_path: &'c str,
        metadata: fs::MetadataSummary,
        cached_refs: CachedChunks,
        retained: Option<RetainedHit>,
        caches: FileCaches<'c>,
    },
    /// A command dump: no file cache, no stamp, no verbose event. Dumps run
    /// in config order but are listed in name order, so the item is
    /// collected into `items` and appended by the caller after sorting.
    Dump { items: &'c mut Vec<Item> },
}

/// A file (or command dump) whose content is fully committed.
pub(super) struct FileOutcome<'c> {
    /// `item.chunks` is complete.
    pub(super) item: Item,
    /// `stats.deduplicated_size` when the file started (from
    /// [`start_file`]); the verbose `added_bytes` is measured from it.
    pub(super) dedup_before: u64,
    pub(super) kind: FileKind<'c>,
}

/// Announce a file to progress. Returns the `dedup_before` baseline for
/// its [`FileOutcome`]. Call before any of the file's chunks are committed.
pub(super) fn start_file(
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    stats: &SnapshotStats,
    path: &str,
) -> u64 {
    if let Some(cb) = progress.as_deref_mut() {
        cb(BackupProgressEvent::FileStarted {
            path: path.to_owned(),
        });
    }
    stats.deduplicated_size
}

/// The per-file epilogue, run once a file's content is fully committed:
/// counts the file, stamps its metadata, reports it, appends it to the
/// listing (or the dump list), updates the file cache, and emits the final
/// stats event. Knows nothing about rollback: call it only after the
/// file's commit is final.
pub(super) fn finish_file(
    repo: &Repo,
    ws: &mut WriteSession,
    stats: &mut SnapshotStats,
    listing: &mut ListingWriter,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    verbose: bool,
    outcome: FileOutcome<'_>,
) -> Result<()> {
    let FileOutcome {
        mut item,
        dedup_before,
        kind,
    } = outcome;
    stats.nfiles += 1;
    match kind {
        FileKind::Read {
            abs_path,
            pre_meta,
            caches,
        } => {
            stamp_item_metadata(&mut item, &pre_meta);
            if verbose {
                let status = if caches.old.contains(abs_path) {
                    FileStatus::Modified
                } else {
                    FileStatus::New
                };
                emit_progress(
                    progress,
                    BackupProgressEvent::FileProcessed {
                        path: item.path.clone(),
                        status,
                        added_bytes: stats.deduplicated_size.saturating_sub(dedup_before),
                    },
                );
            }
            listing.push(repo, ws, &item)?;
            caches.new.insert(
                abs_path,
                pre_meta.device,
                pre_meta.inode,
                pre_meta.mtime_ns,
                pre_meta.ctime_ns,
                pre_meta.size,
                CachedChunks::from_chunk_refs(&item.chunks),
            );
        }
        FileKind::CacheHit {
            abs_path,
            metadata,
            cached_refs,
            retained,
            caches,
        } => {
            if verbose {
                emit_progress(
                    progress,
                    BackupProgressEvent::FileProcessed {
                        path: item.path.clone(),
                        status: FileStatus::Unchanged,
                        added_bytes: 0,
                    },
                );
            }
            listing.push(repo, ws, &item)?;
            caches.new.insert_cache_hit(
                abs_path,
                retained,
                metadata.device,
                metadata.inode,
                metadata.mtime_ns,
                metadata.ctime_ns,
                metadata.size,
                cached_refs,
            );
            debug!(path = %item.path, "file cache hit");
        }
        FileKind::Dump { items } => {
            let path = item.path.clone();
            items.push(item);
            emit_stats_progress(progress, stats, Some(path));
            return Ok(());
        }
    }
    emit_stats_progress(progress, stats, Some(std::mem::take(&mut item.path)));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::file_cache::CachedChunkRef;
    use crate::testutil::test_repo_plaintext_with_session;
    use vykar_types::chunk_id::ChunkId;

    fn empty_item() -> Item {
        Item {
            ctime: Some(0),
            size: 42,
            ..Item::test_file("ghost.txt")
        }
    }

    mod prefilter {
        use super::*;
        use crate::commands::backup::chunk_process::{WorkerChunk, classify_chunk};
        use crate::compress::Compression;
        use crate::index::ChunkIndex;
        use crate::index::dedup_cache::chunk_id_to_u64;
        use std::borrow::Cow;
        use vykar_types::pack_id::PackId;

        /// An index holding `stored`.
        fn index_of(stored: &[(ChunkId, u32)]) -> ChunkIndex {
            let mut index = ChunkIndex::new();
            for &(id, stored_size) in stored {
                index.add(id, stored_size, PackId::from_bytes([0x01; 32]), 0);
            }
            index
        }

        fn classify(repo: &Repo, prefilter: &DedupPrefilter, data: &[u8]) -> WorkerChunk {
            let id = ChunkId::compute(repo.crypto().chunk_hasher(), data);
            classify_chunk(
                id,
                Cow::Borrowed(data),
                Some(prefilter),
                Compression::None,
                repo.crypto().as_ref(),
            )
            .unwrap()
        }

        #[test]
        fn presession_hit_records_reference_without_data() {
            let (mut repo, mut ws) = test_repo_plaintext_with_session();
            let data = b"already stored before this session".to_vec();
            let id = ChunkId::compute(repo.crypto().chunk_hasher(), &data);
            repo.set_session_table_for_test(&mut ws, &index_of(&[(id, 77)]));
            let prefilter = ws.dedup_prefilter().unwrap();

            let chunk = classify(&repo, &prefilter, &data);
            assert!(matches!(chunk, WorkerChunk::Existing(_)));
            let mut item = empty_item();
            let mut stats = SnapshotStats::default();
            process_worker_chunks(
                &repo,
                &mut ws,
                &mut item,
                [chunk],
                &mut stats,
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
            assert!(ws.delta_reuses(&id));
        }

        /// BK-2 (worker path): a chunk added by an applied segment is a
        /// data-less hit recorded by key.
        #[test]
        fn presession_hit_on_an_applied_addition_records_keyed_reuse() {
            let (mut repo, mut ws) = test_repo_plaintext_with_session();
            let data = b"added by an earlier write".to_vec();
            let (id, size, _) = ws
                .store_chunk(&repo, &data, Compression::None, PackType::Data)
                .unwrap();
            repo.unchecked_writer().save_session(ws).unwrap();
            assert!(repo.chunk_index().table().unwrap().is_empty(), "no base");
            ws = repo.begin_write_session(&crate::repo::WriteSessionOptions::default());
            let prefilter = ws.dedup_prefilter().unwrap();

            let chunk = classify(&repo, &prefilter, &data);
            let WorkerChunk::Existing(existing) = &chunk else {
                panic!("an applied addition must be a data-less hit");
            };
            assert_eq!(
                existing.at,
                crate::commands::backup::chunk_process::ExistingAt::Added
            );
            let mut item = empty_item();
            let mut stats = SnapshotStats::default();
            process_worker_chunks(
                &repo,
                &mut ws,
                &mut item,
                [chunk],
                &mut stats,
                Some(&prefilter),
            )
            .unwrap();
            assert_eq!(item.chunks[0].csize, size);
            assert_eq!(stats.deduplicated_size, 0);
            let delta = ws.write_session_delta_for_test();
            assert!(delta.reused.is_keyed(&id));
            ws.flush_packs(&repo).unwrap();
        }

        /// A data-less hit whose verdict came from a different table than
        /// the session's must fail loudly, not record a reference.
        #[test]
        fn presession_hit_from_foreign_prefilter_errors() {
            let (mut repo, mut ws) = test_repo_plaintext_with_session();
            let data = b"already stored before this session".to_vec();
            let id = ChunkId::compute(repo.crypto().chunk_hasher(), &data);
            let index = index_of(&[(id, 77)]);
            repo.set_session_table_for_test(&mut ws, &index);
            let foreign = crate::testutil::prefilter_of(&index);

            let chunk = classify(&repo, &foreign, &data);
            let mut item = empty_item();
            let mut stats = SnapshotStats::default();
            let err = process_worker_chunks(
                &repo,
                &mut ws,
                &mut item,
                [chunk],
                &mut stats,
                Some(&foreign),
            )
            .expect_err("foreign prefilter must be rejected");
            assert!(err.to_string().contains("pre-session dedup hit"), "{err}");
            assert!(item.chunks.is_empty());
            assert_eq!(stats.original_size, 0);
            assert!(!ws.delta_reuses(&id));
        }

        /// Filter false positives are packed in the worker; two copies of
        /// the same new chunk must still dedup against each other.
        #[test]
        fn false_positives_packed_in_worker_dedup_within_session() {
            let (mut repo, mut ws) = test_repo_plaintext_with_session();
            let data = b"new in this session, colliding filter key".to_vec();
            let id = ChunkId::compute(repo.crypto().chunk_hasher(), &data);
            let mut colliding = *id.as_bytes();
            colliding[31] ^= 0xFF;
            let colliding = ChunkId::from_bytes(colliding);
            assert_eq!(chunk_id_to_u64(&colliding), chunk_id_to_u64(&id));
            repo.set_session_table_for_test(&mut ws, &index_of(&[(colliding, 77)]));
            let prefilter = ws.dedup_prefilter().unwrap();

            let mut stats = SnapshotStats::default();
            let mut items = [empty_item(), empty_item()];
            for item in &mut items {
                let chunk = classify(&repo, &prefilter, &data);
                assert!(matches!(chunk, WorkerChunk::Prepared(_)));
                process_worker_chunks(&repo, &mut ws, item, [chunk], &mut stats, Some(&prefilter))
                    .unwrap();
            }

            let csize = items[0].chunks[0].csize;
            assert_eq!(items[1].chunks[0].id, id);
            assert_eq!(items[1].chunks[0].csize, csize);
            assert_eq!(stats.original_size, 2 * data.len() as u64);
            assert_eq!(stats.compressed_size, 2 * csize as u64);
            assert_eq!(stats.deduplicated_size, csize as u64, "stored once");
            ws.flush_packs(&repo).unwrap();
        }
    }

    mod epilogue {
        use super::*;
        use crate::commands::list::for_each_decoded_item;
        use crate::compress::Compression;

        fn meta(base: u32) -> fs::MetadataSummary {
            fs::MetadataSummary {
                mode: 0o600 + base,
                uid: 1000 + base,
                gid: 2000 + base,
                mtime_ns: 3000 + base as i64,
                ctime_ns: 4000 + base as i64,
                device: 5,
                inode: 6 + base as u64,
                nlink: 1,
                size: 7000 + base as u64,
                is_dataless: false,
            }
        }

        fn caches() -> (FileCache, FileCache) {
            let mut old = FileCache::new();
            old.begin_sections(&["/src".into()], &[0]);
            let mut new = FileCache::new();
            new.begin_sections(&["/src".into()], &[0]);
            (old, new)
        }

        /// A file item with one stored chunk and walk metadata from `walk`.
        fn stored_item(repo: &Repo, ws: &mut WriteSession, walk: &fs::MetadataSummary) -> Item {
            let data = b"epilogue chunk";
            let (id, csize, _) = ws
                .store_chunk(repo, data, Compression::None, PackType::Data)
                .unwrap();
            Item {
                mode: walk.mode,
                uid: walk.uid,
                gid: walk.gid,
                mtime: walk.mtime_ns,
                ctime: Some(walk.ctime_ns),
                size: walk.size,
                chunks: vec![ChunkRef {
                    id,
                    size: data.len() as u32,
                    csize,
                }],
                ..Item::test_file("a")
            }
        }

        /// The items a finished listing holds.
        fn listed(mut repo: Repo, mut ws: WriteSession, listing: ListingWriter) -> Vec<Item> {
            let ptrs = listing.finish(&repo, &mut ws).unwrap().item_ptrs;
            ws.flush_packs(&repo).unwrap();
            repo.unchecked_writer().save_session(ws).unwrap();
            let mut items = Vec::new();
            for id in &ptrs {
                let chunk = repo.read_chunk(id).unwrap();
                for_each_decoded_item(&chunk, |item| {
                    items.push(item);
                    Ok(())
                })
                .unwrap();
            }
            items
        }

        fn assert_final_stats(event: &BackupProgressEvent, path: &str) {
            assert!(
                matches!(
                    event,
                    BackupProgressEvent::StatsUpdated { nfiles: 1, current_file: Some(p), .. }
                        if p == path
                ),
                "{event:?}"
            );
        }

        /// Finishes a read file whose `pre_meta` differs from its walk metadata
        /// and checks what every read file gets; returns the events.
        fn run_read(old_has_path: bool, verbose: bool) -> Vec<BackupProgressEvent> {
            let (repo, mut ws) = test_repo_plaintext_with_session();
            let mut listing = ListingWriter::new(repo.crypto().chunk_hasher(), Compression::None);
            let (mut old, mut new) = caches();
            if old_has_path {
                old.insert("/src/a", 1, 1, 1, 1, 1, CachedChunks::from_chunk_refs(&[]));
            }
            let walk = meta(0);
            let pre_meta = meta(1);
            let item = stored_item(&repo, &mut ws, &walk);
            let mut stats = SnapshotStats {
                deduplicated_size: 100,
                ..SnapshotStats::default()
            };
            let mut events = Vec::new();
            {
                let mut cb = |e| events.push(e);
                let mut progress: Option<&mut dyn FnMut(BackupProgressEvent)> = Some(&mut cb);
                let dedup_before = stats.deduplicated_size;
                stats.deduplicated_size += 42;
                finish_file(
                    &repo,
                    &mut ws,
                    &mut stats,
                    &mut listing,
                    &mut progress,
                    verbose,
                    FileOutcome {
                        item,
                        dedup_before,
                        kind: FileKind::Read {
                            abs_path: "/src/a",
                            pre_meta,
                            caches: FileCaches {
                                old: &old,
                                new: &mut new,
                            },
                        },
                    },
                )
                .unwrap();
            }
            assert_eq!(stats.nfiles, 1);
            let hit = new
                .lookup(
                    "/src/a",
                    pre_meta.device,
                    pre_meta.inode,
                    pre_meta.mtime_ns,
                    pre_meta.ctime_ns,
                    pre_meta.size,
                )
                .expect("cache entry under pre_meta");
            let items = listed(repo, ws, listing);
            assert_eq!(items.len(), 1);
            assert_eq!(hit.as_slice()[0].id, items[0].chunks[0].id);
            let it = &items[0];
            assert_eq!(
                (it.mode, it.uid, it.gid, it.size, it.mtime, it.ctime),
                (
                    pre_meta.mode,
                    pre_meta.uid,
                    pre_meta.gid,
                    pre_meta.size,
                    pre_meta.mtime_ns,
                    Some(pre_meta.ctime_ns)
                ),
                "stamped from pre_meta"
            );
            events
        }

        #[test]
        fn finish_file_read_stamps_lists_caches_and_reports() {
            let events = run_read(false, true);
            assert_eq!(events.len(), 2, "{events:?}");
            assert!(
                matches!(
                    &events[0],
                    BackupProgressEvent::FileProcessed {
                        path,
                        status: FileStatus::New,
                        added_bytes: 42,
                    } if path == "a"
                ),
                "{:?}",
                events[0]
            );
            assert_final_stats(&events[1], "a");
        }

        #[test]
        fn finish_file_read_reports_modified_when_previously_cached() {
            let events = run_read(true, true);
            assert_eq!(events.len(), 2, "{events:?}");
            assert!(
                matches!(
                    &events[0],
                    BackupProgressEvent::FileProcessed {
                        status: FileStatus::Modified,
                        added_bytes: 42,
                        ..
                    }
                ),
                "{:?}",
                events[0]
            );
        }

        #[test]
        fn finish_file_not_verbose_emits_only_stats() {
            let events = run_read(false, false);
            assert_eq!(events.len(), 1, "{events:?}");
            assert_final_stats(&events[0], "a");
        }

        #[test]
        fn finish_file_cache_hit_keeps_walk_metadata() {
            let (repo, mut ws) = test_repo_plaintext_with_session();
            let mut listing = ListingWriter::new(repo.crypto().chunk_hasher(), Compression::None);
            let (old, mut new) = caches();
            let walk = meta(0);
            let item = stored_item(&repo, &mut ws, &walk);
            let cached_refs = CachedChunks::from_chunk_refs(&item.chunks);
            let mut stats = SnapshotStats::default();
            let mut events = Vec::new();
            {
                let mut cb = |e| events.push(e);
                let mut progress: Option<&mut dyn FnMut(BackupProgressEvent)> = Some(&mut cb);
                finish_file(
                    &repo,
                    &mut ws,
                    &mut stats,
                    &mut listing,
                    &mut progress,
                    true,
                    FileOutcome {
                        item,
                        dedup_before: 0,
                        kind: FileKind::CacheHit {
                            abs_path: "/src/a",
                            metadata: walk,
                            cached_refs,
                            retained: None,
                            caches: FileCaches {
                                old: &old,
                                new: &mut new,
                            },
                        },
                    },
                )
                .unwrap();
            }
            assert_eq!(stats.nfiles, 1);
            assert_eq!(events.len(), 2, "{events:?}");
            assert!(
                matches!(
                    &events[0],
                    BackupProgressEvent::FileProcessed {
                        status: FileStatus::Unchanged,
                        added_bytes: 0,
                        ..
                    }
                ),
                "{:?}",
                events[0]
            );
            assert_final_stats(&events[1], "a");
            assert!(
                new.lookup(
                    "/src/a",
                    walk.device,
                    walk.inode,
                    walk.mtime_ns,
                    walk.ctime_ns,
                    walk.size
                )
                .is_some()
            );
            let items = listed(repo, ws, listing);
            assert_eq!(items.len(), 1);
            let it = &items[0];
            assert_eq!(
                (it.mode, it.uid, it.gid, it.size, it.mtime, it.ctime),
                (
                    walk.mode,
                    walk.uid,
                    walk.gid,
                    walk.size,
                    walk.mtime_ns,
                    Some(walk.ctime_ns)
                ),
                "cache hits keep the walk metadata"
            );
        }

        #[test]
        fn finish_file_dump_defers_item() {
            let (repo, mut ws) = test_repo_plaintext_with_session();
            let mut listing = ListingWriter::new(repo.crypto().chunk_hasher(), Compression::None);
            let mut items = Vec::new();
            let mut stats = SnapshotStats::default();
            let mut events = Vec::new();
            {
                let mut cb = |e| events.push(e);
                let mut progress: Option<&mut dyn FnMut(BackupProgressEvent)> = Some(&mut cb);
                finish_file(
                    &repo,
                    &mut ws,
                    &mut stats,
                    &mut listing,
                    &mut progress,
                    true,
                    FileOutcome {
                        item: Item::test_file("vykar-dumps/x"),
                        dedup_before: 0,
                        kind: FileKind::Dump { items: &mut items },
                    },
                )
                .unwrap();
            }
            assert_eq!(stats.nfiles, 1);
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].path, "vykar-dumps/x");
            assert_eq!(events.len(), 1, "no FileProcessed for a dump: {events:?}");
            assert_final_stats(&events[0], "vykar-dumps/x");
            assert!(listing.finish(&repo, &mut ws).unwrap().item_ptrs.is_empty());
        }

        #[test]
        fn start_file_announces_and_returns_baseline() {
            let stats = SnapshotStats {
                deduplicated_size: 77,
                ..SnapshotStats::default()
            };
            let mut events = Vec::new();
            {
                let mut cb = |e| events.push(e);
                let mut progress: Option<&mut dyn FnMut(BackupProgressEvent)> = Some(&mut cb);
                assert_eq!(start_file(&mut progress, &stats, "a/b"), 77);
            }
            assert_eq!(events.len(), 1);
            assert!(
                matches!(&events[0], BackupProgressEvent::FileStarted { path } if path == "a/b"),
                "{:?}",
                events[0]
            );
            let mut none: Option<&mut dyn FnMut(BackupProgressEvent)> = None;
            assert_eq!(start_file(&mut none, &stats, "a/b"), 77);
        }
    }

    /// Counting the file is the epilogue's: a resolved cache hit leaves
    /// `nfiles` alone.
    #[test]
    fn commit_cache_hit_does_not_count_the_file() {
        let (repo, mut ws) = test_repo_plaintext_with_session();
        let data = b"cached content";
        let (id, csize, _) = ws
            .store_chunk(
                &repo,
                data,
                crate::compress::Compression::None,
                PackType::Data,
            )
            .unwrap();
        let mut item = empty_item();
        let mut stats = SnapshotStats::default();
        let cached = CachedChunks::Single(CachedChunkRef {
            id,
            size: data.len() as u32,
        });
        commit_cache_hit(&repo, &mut ws, &mut item, &cached, &mut stats).unwrap();
        assert_eq!(item.chunks.len(), 1);
        assert_eq!(item.chunks[0].csize, csize);
        assert_eq!(stats.nfiles, 0);
        assert_eq!(stats.original_size, data.len() as u64);
        ws.flush_packs(&repo).unwrap();
    }

    #[test]
    fn commit_cache_hit_errors_on_missing_chunk() {
        // Missing-chunk semantics: a cached chunk id that isn't resolvable
        // via `reuse_cached_chunk_ref` must surface as an error, never as
        // a silent downgrade to a cache miss (that would mask repo/index
        // corruption). This guards the invariant called out in the plan.
        let (repo, mut ws) = test_repo_plaintext_with_session();
        let mut item = empty_item();
        let mut stats = SnapshotStats::default();
        let phantom = CachedChunks::Single(CachedChunkRef {
            id: ChunkId::from_bytes([0x77; 32]),
            size: 42,
        });

        let err = commit_cache_hit(&repo, &mut ws, &mut item, &phantom, &mut stats)
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
