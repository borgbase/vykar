use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tracing::{debug, warn};

use crate::chunker;
use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::limits::{self, ByteRateLimiter};
use crate::platform::fs;
use crate::repo::file_cache::{CachedChunks, FileCache, ParentReuseIndex};
use crate::repo::Repository;
use crate::snapshot::item::Item;
use crate::snapshot::SnapshotStats;
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use super::chunk_process::classify_chunk;
use super::commit::process_worker_chunks;
use super::concurrency::{BudgetGuard, ByteBudget};
use super::source::ResolvedSource;
use super::walk::{build_walk_iter, reserve_budget, WalkEntry};
use super::BackupProgressEvent;

// ---------------------------------------------------------------------------
// Parallel file processing pipeline (crossbeam-channel)
// ---------------------------------------------------------------------------

/// Result from a parallel worker.
pub(crate) enum ProcessedEntry {
    /// Small/medium file: chunks classified by xor filter (hash-only or fully transformed).
    ProcessedFile {
        item: Item,
        abs_path: String,
        /// Pre-read fstat taken in the worker. Replaces walk-time `metadata`
        /// for item/cache fields so drift-checked values flow to disk.
        pre_meta: fs::MetadataSummary,
        chunks: Vec<super::WorkerChunk>,
        /// Bytes acquired from ByteBudget; consumer must release after committing.
        acquired_bytes: usize,
    },
    /// Segment of a large file: worker processed one fixed-size slice.
    FileSegment {
        /// Only present for segment 0; `None` for continuations.
        item: Option<Item>,
        abs_path: Arc<str>,
        /// Pre-read fstat from segment 0's worker — the canonical meta that
        /// populates the final item fields. `None` on continuations (each
        /// segment's worker already verified its own `pre_meta` against the
        /// walker's metadata, so by transitivity every segment agrees).
        pre_meta: Option<fs::MetadataSummary>,
        chunks: Vec<super::WorkerChunk>,
        acquired_bytes: usize,
        segment_index: usize,
        num_segments: usize,
    },
    /// File cache hit — consumer just bumps refcounts.
    CacheHit {
        item: Item,
        abs_path: String,
        metadata: fs::MetadataSummary,
        cached_refs: CachedChunks,
    },
    /// Non-file item (directory, symlink, zero-size file).
    NonFile {
        item: Item,
    },
    /// A file skipped due to a soft error (permission denied, not found,
    /// or drift detected between walk and open / during read). No data
    /// was committed for this file.
    Skipped {
        path: String,
        /// Pre-formatted reason (avoids carrying `VykarError` across threads).
        reason: String,
    },
    /// The walker reported a soft error before it could materialize a
    /// path (e.g. directory-iteration `EACCES`). The walker has already
    /// logged the failing path via `tracing::warn!`, so the consumer
    /// just bumps the error counter without emitting a pathless GUI
    /// warning. Mirrors `sequential.rs` `WalkEvent::Skipped` handling.
    WalkSkip,
    /// A segment of a large file was skipped.
    ///
    /// If `segment_index == 0` nothing was committed. If `segment_index > 0`,
    /// earlier segments may already be committed — the consumer invokes
    /// cross-segment rollback. `segment_index` + `num_segments` together
    /// drive the drain count for the skipped remainder.
    SegmentSkipped {
        segment_index: usize,
        num_segments: usize,
        path: Arc<str>,
        reason: String,
    },
    SourceStarted {
        path: String,
    },
    SourceFinished {
        path: String,
    },
}

/// Estimate the number of chunks a file will produce, for pre-sizing Vecs.
fn estimate_chunk_count(data_len: u64, avg_chunk_size: u32) -> usize {
    if avg_chunk_size == 0 {
        return 1;
    }
    let est = (data_len / avg_chunk_size as u64).saturating_add(1);
    est.min(4096) as usize
}

/// Wrapper that converts soft I/O errors (and file-drift errors) into
/// `Skipped` / `SegmentSkipped` variants.
///
/// For segment N>0, the consumer performs a cross-segment rollback of the
/// earlier segments' refcounts/dedup inserts before draining the rest, so
/// it is now safe to convert soft errors at any segment index.
#[allow(clippy::too_many_arguments)]
fn process_file_worker(
    entry: WalkEntry,
    chunk_id_key: &[u8; 32],
    crypto: &dyn CryptoEngine,
    compression: Compression,
    chunker_config: &ChunkerConfig,
    read_limiter: Option<&ByteRateLimiter>,
    budget: &ByteBudget,
    pre_acquired_bytes: usize,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<ProcessedEntry> {
    // Extract info needed for soft-error conversion before moving entry.
    let (path_for_skip, seg_path_for_skip, segment_info) = match &entry {
        WalkEntry::File { abs_path, .. } => (Some(abs_path.clone()), None, None),
        WalkEntry::FileSegment {
            abs_path,
            segment_index,
            num_segments,
            ..
        } => (
            None,
            Some(abs_path.clone()),
            Some((*segment_index, *num_segments)),
        ),
        _ => (None, None, None),
    };

    match process_file_worker_inner(
        entry,
        chunk_id_key,
        crypto,
        compression,
        chunker_config,
        read_limiter,
        budget,
        pre_acquired_bytes,
        dedup_filter,
    ) {
        Ok(processed) => Ok(processed),
        Err(e) if e.is_soft_file_error() => {
            if let Some(path) = path_for_skip {
                let reason = e.to_string();
                warn!(path = %path, error = %e, "skipping file in pipeline (soft error)");
                Ok(ProcessedEntry::Skipped { path, reason })
            } else if let (Some(path), Some((segment_index, num_segments))) =
                (seg_path_for_skip, segment_info)
            {
                let reason = e.to_string();
                warn!(
                    path = %path,
                    segment_index,
                    error = %e,
                    "skipping segmented file in pipeline (soft error)"
                );
                Ok(ProcessedEntry::SegmentSkipped {
                    segment_index,
                    num_segments,
                    path,
                    reason,
                })
            } else {
                Err(e)
            }
        }
        Err(e) => Err(e),
    }
}

/// Inner implementation: process a single walk entry in a parallel worker thread.
///
/// Budget bytes are pre-acquired by the walk thread; `pre_acquired_bytes`
/// is wrapped in a [`BudgetGuard`] for error safety (auto-release on `?` bail).
#[allow(clippy::too_many_arguments)]
fn process_file_worker_inner(
    entry: WalkEntry,
    chunk_id_key: &[u8; 32],
    crypto: &dyn CryptoEngine,
    compression: Compression,
    chunker_config: &ChunkerConfig,
    read_limiter: Option<&ByteRateLimiter>,
    budget: &ByteBudget,
    pre_acquired_bytes: usize,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<ProcessedEntry> {
    match entry {
        WalkEntry::File {
            item,
            abs_path,
            metadata,
            file_size: _,
        } => {
            // Budget was pre-acquired by the walk thread. Wrap in a guard for
            // error safety — if we `?`-bail, the guard drops and releases bytes.
            let guard = BudgetGuard::from_pre_acquired(budget, pre_acquired_bytes);

            let mut source = super::read_source::BackupSource::open(Path::new(&abs_path))
                .map_err(VykarError::Io)?;
            let pre_meta = fs::fstat_summary(source.file()).map_err(VykarError::Io)?;

            // Walk-to-open drift check — catches pre-open mutation and
            // rename-atop (device+inode differ).
            if !fs::metadata_matches(&pre_meta, &metadata) {
                return Err(VykarError::FileChangedDuringRead {
                    path: abs_path.clone(),
                });
            }

            // Small file (< min_chunk_size): read whole, single chunk.
            if pre_meta.size < chunker_config.min_size as u64 {
                let mut data = Vec::with_capacity(pre_meta.size as usize);
                if let Some(limiter) = read_limiter {
                    limits::LimitedReader::new(&mut source, Some(limiter))
                        .read_to_end(&mut data)
                        .map_err(VykarError::Io)?;
                } else {
                    source.read_to_end(&mut data).map_err(VykarError::Io)?;
                }

                let post_meta = fs::fstat_summary(source.file()).map_err(VykarError::Io)?;
                if !fs::metadata_matches(&pre_meta, &post_meta)
                    || data.len() as u64 != pre_meta.size
                {
                    return Err(VykarError::FileChangedDuringRead {
                        path: abs_path.clone(),
                    });
                }

                let chunk_id = ChunkId::compute(chunk_id_key, &data);
                let worker_chunk =
                    classify_chunk(chunk_id, data, dedup_filter, compression, crypto)?;

                let acquired_bytes = guard.defuse();
                return Ok(ProcessedEntry::ProcessedFile {
                    item,
                    abs_path,
                    pre_meta,
                    chunks: vec![worker_chunk],
                    acquired_bytes,
                });
            }

            // Medium file: read, chunk via FastCDC, then hash → classify each chunk.
            let mut total_bytes: u64 = 0;
            let mut worker_chunks =
                Vec::with_capacity(estimate_chunk_count(pre_meta.size, chunker_config.avg_size));
            {
                let chunk_stream = chunker::chunk_stream(
                    limits::LimitedReader::new(&mut source, read_limiter),
                    chunker_config,
                );

                for chunk_result in chunk_stream {
                    let chunk = chunk_result.map_err(|e| match e {
                        fastcdc::v2020::Error::IoError(ioe) => VykarError::Io(ioe),
                        other => {
                            VykarError::Other(format!("chunking failed for {abs_path}: {other}"))
                        }
                    })?;

                    total_bytes = total_bytes.saturating_add(chunk.data.len() as u64);
                    let chunk_id = ChunkId::compute(chunk_id_key, &chunk.data);
                    worker_chunks.push(classify_chunk(
                        chunk_id,
                        chunk.data,
                        dedup_filter,
                        compression,
                        crypto,
                    )?);
                }
            }

            let post_meta = fs::fstat_summary(source.file()).map_err(VykarError::Io)?;
            if !fs::metadata_matches(&pre_meta, &post_meta) || total_bytes != pre_meta.size {
                return Err(VykarError::FileChangedDuringRead {
                    path: abs_path.clone(),
                });
            }

            let acquired_bytes = guard.defuse();
            Ok(ProcessedEntry::ProcessedFile {
                item,
                abs_path,
                pre_meta,
                chunks: worker_chunks,
                acquired_bytes,
            })
        }

        WalkEntry::FileSegment {
            item,
            abs_path,
            metadata,
            segment_index,
            num_segments,
            offset,
            len,
        } => {
            let guard = BudgetGuard::from_pre_acquired(budget, pre_acquired_bytes);

            let mut source = super::read_source::BackupSource::open(Path::new(&*abs_path))
                .map_err(VykarError::Io)?;
            let pre_meta = fs::fstat_summary(source.file()).map_err(VykarError::Io)?;

            // Walk-to-open drift check. Segmented reads are a plan driven
            // by walk-time size (`num_segments`/`offset`/`len`). Any drift
            // invalidates the plan — skip the segment.
            if !fs::metadata_matches(&pre_meta, &metadata) {
                return Err(VykarError::FileChangedDuringRead {
                    path: abs_path.to_string(),
                });
            }

            source.seek_from_start(offset).map_err(VykarError::Io)?;

            let mut worker_chunks =
                Vec::with_capacity(estimate_chunk_count(len, chunker_config.avg_size));
            {
                let reader = (&mut source).take(len);
                let chunk_stream = chunker::chunk_stream(
                    limits::LimitedReader::new(reader, read_limiter),
                    chunker_config,
                );

                for chunk_result in chunk_stream {
                    let chunk = chunk_result.map_err(|e| match e {
                        fastcdc::v2020::Error::IoError(ioe) => VykarError::Io(ioe),
                        other => {
                            VykarError::Other(format!("chunking failed for {abs_path}: {other}"))
                        }
                    })?;

                    let chunk_id = ChunkId::compute(chunk_id_key, &chunk.data);
                    worker_chunks.push(classify_chunk(
                        chunk_id,
                        chunk.data,
                        dedup_filter,
                        compression,
                        crypto,
                    )?);
                }
            }

            // Intra-segment drift check. `file.take(len)` legitimately stops
            // short, so we don't short-read-guard; the post-fstat catches
            // mutation of size/mtime/ctime/device/inode.
            let post_meta = fs::fstat_summary(source.file()).map_err(VykarError::Io)?;
            if !fs::metadata_matches(&pre_meta, &post_meta) {
                return Err(VykarError::FileChangedDuringRead {
                    path: abs_path.to_string(),
                });
            }

            let acquired_bytes = guard.defuse();
            Ok(ProcessedEntry::FileSegment {
                item,
                abs_path,
                // Only segment 0's pre_meta is consumed downstream.
                pre_meta: (segment_index == 0).then_some(pre_meta),
                chunks: worker_chunks,
                acquired_bytes,
                segment_index,
                num_segments,
            })
        }

        WalkEntry::CacheHit {
            item,
            abs_path,
            metadata,
            cached_refs,
        } => Ok(ProcessedEntry::CacheHit {
            item,
            abs_path,
            metadata,
            cached_refs,
        }),

        WalkEntry::NonFile { item } => Ok(ProcessedEntry::NonFile { item }),

        WalkEntry::Skipped => Ok(ProcessedEntry::WalkSkip),

        WalkEntry::SourceStarted { path } => Ok(ProcessedEntry::SourceStarted { path }),

        WalkEntry::SourceFinished { path } => Ok(ProcessedEntry::SourceFinished { path }),
    }
}

/// Tracks in-progress accumulation of a segmented large file.
struct LargeFileAccum {
    item: Item,
    abs_path: Arc<str>,
    /// Pre-read fstat captured by segment 0's worker. Each segment's worker
    /// already validates its own `pre_meta` against the walker's metadata,
    /// so by transitivity all segments agree — we use this value at
    /// finalization to populate the item fields.
    metadata: fs::MetadataSummary,
    next_expected_index: usize,
    num_segments: usize,
    /// Baseline `deduplicated_size` when segment 0 started (for verbose added_bytes).
    dedup_baseline: u64,
    /// Stats byte-counter snapshot taken at segment 0 (restored on rollback).
    stats_snap: crate::snapshot::ByteCounterSnapshot,
}

/// Take the current segmented accumulator, roll back any commits from
/// earlier segments, restore stats, and emit a skip warning. Returns the
/// number of remaining segments the consumer loop should drain
/// (`num_segments - current_segment_index - 1`).
///
/// Called by:
/// - the main loop when the worker reports `SegmentSkipped` at
///   `segment_index > 0` with an active accumulator,
/// - the main loop when a `WorkerErr` arrives soft during accumulation.
///
/// The rollback checkpoint is armed for the entire lifetime of the
/// accumulator, so aborting it here is unconditional.
fn rollback_and_skip_large_file(
    repo: &mut Repository,
    stats: &mut SnapshotStats,
    large_file_accum: &mut Option<LargeFileAccum>,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    current_segment_index: usize,
    reason: &str,
) -> usize {
    let Some(accum) = large_file_accum.take() else {
        return 0;
    };
    repo.abort_rollback_checkpoint();
    stats.restore_byte_counters(accum.stats_snap);
    stats.errors += 1;
    super::emit_post_commit_warning(
        progress,
        format!("skipping file '{}': {reason}", accum.abs_path),
    );
    accum.num_segments.saturating_sub(current_segment_index + 1)
}

/// Validate and update the segment accumulator state machine.
///
/// For segment 0: arms the rollback checkpoint, snapshots stats byte
/// counters, and installs the accumulator (errors if one already exists).
/// Ordering matters: the overlap check runs first, then the checkpoint is
/// armed, then the accumulator is installed. That way a nested-segmentation
/// error cannot leave a stray checkpoint, and accumulator-present always
/// implies checkpoint-armed.
///
/// For continuations: validates ordering, file identity, and segment count.
/// Cross-segment metadata drift is not checked here — each segment's
/// worker already verifies `pre_meta` against the walker's metadata, which
/// is identical across segments.
#[allow(clippy::too_many_arguments)]
fn validate_segment_accum(
    large_file_accum: &mut Option<LargeFileAccum>,
    item: Option<Item>,
    abs_path: Arc<str>,
    pre_meta: Option<fs::MetadataSummary>,
    segment_index: usize,
    num_segments: usize,
    dedup_baseline: u64,
    repo: &mut Repository,
    stats: &SnapshotStats,
) -> Result<()> {
    if segment_index == 0 {
        if large_file_accum.is_some() {
            return Err(VykarError::Other("nested large file segmentation".into()));
        }
        let item =
            item.ok_or_else(|| VykarError::Other("BUG: segment 0 must carry item".into()))?;
        let pre_meta = pre_meta
            .ok_or_else(|| VykarError::Other("BUG: segment 0 must carry pre_meta".into()))?;
        // Arm the rollback checkpoint BEFORE installing the accumulator so
        // the invariant `accum.is_some() ⇒ checkpoint armed` holds
        // unconditionally.
        repo.begin_rollback_checkpoint()?;
        let stats_snap = stats.snapshot_byte_counters();
        *large_file_accum = Some(LargeFileAccum {
            item,
            abs_path,
            metadata: pre_meta,
            next_expected_index: 1,
            dedup_baseline,
            num_segments,
            stats_snap,
        });
    } else {
        let accum = large_file_accum
            .as_mut()
            .ok_or_else(|| VykarError::Other("FileSegment without preceding segment 0".into()))?;
        if segment_index != accum.next_expected_index {
            return Err(VykarError::Other(format!(
                "segment index mismatch: expected {}, got {segment_index}",
                accum.next_expected_index
            )));
        }
        if abs_path != accum.abs_path {
            return Err(VykarError::Other("segment file identity mismatch".into()));
        }
        if num_segments != accum.num_segments {
            return Err(VykarError::Other(format!(
                "segment count mismatch: expected {}, got {num_segments}",
                accum.num_segments
            )));
        }
        accum.next_expected_index += 1;
    }
    Ok(())
}

/// Consume a single processed entry: dedup check, pack commit, item stream, file cache.
#[allow(clippy::too_many_arguments)]
fn consume_processed_entry(
    entry: ProcessedEntry,
    repo: &mut Repository,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    items_config: &ChunkerConfig,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    compression: Compression,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    budget: &ByteBudget,
    dedup_filter: Option<&xorf::Xor8>,
    large_file_accum: &mut Option<LargeFileAccum>,
    old_file_cache: &FileCache,
    verbose: bool,
) -> Result<()> {
    use super::{append_item_to_stream, emit_stats_progress, FileStatus};

    match entry {
        ProcessedEntry::ProcessedFile {
            mut item,
            abs_path,
            pre_meta,
            chunks,
            acquired_bytes,
        } => {
            if let Some(cb) = progress.as_deref_mut() {
                cb(BackupProgressEvent::FileStarted {
                    path: item.path.clone(),
                });
            }

            let dedup_before = if verbose { stats.deduplicated_size } else { 0 };

            process_worker_chunks(repo, &mut item, chunks, stats, compression, dedup_filter)?;

            // Release budget bytes now that chunks are committed.
            budget.release(acquired_bytes);

            stats.nfiles += 1;

            // Replace walk-time metadata with the pre-read fstat values so
            // the persisted `Item` matches the bytes we just committed.
            item.mode = pre_meta.mode;
            item.uid = pre_meta.uid;
            item.gid = pre_meta.gid;
            item.size = pre_meta.size;
            item.mtime = pre_meta.mtime_ns;
            item.ctime = Some(pre_meta.ctime_ns);

            if verbose {
                let added_bytes = stats.deduplicated_size - dedup_before;
                let status = if old_file_cache.contains(&abs_path) {
                    FileStatus::Modified
                } else {
                    FileStatus::New
                };
                super::emit_progress(
                    progress,
                    BackupProgressEvent::FileProcessed {
                        path: item.path.clone(),
                        status,
                        added_bytes,
                    },
                );
            }

            append_item_to_stream(
                repo,
                item_stream,
                item_ptrs,
                &item,
                items_config,
                compression,
            )?;

            new_file_cache.insert(
                &abs_path,
                pre_meta.device,
                pre_meta.inode,
                pre_meta.mtime_ns,
                pre_meta.ctime_ns,
                pre_meta.size,
                CachedChunks::from_chunk_refs(&item.chunks),
            );

            emit_stats_progress(progress, stats, Some(std::mem::take(&mut item.path)));
        }

        ProcessedEntry::FileSegment {
            item,
            abs_path,
            pre_meta,
            chunks,
            acquired_bytes,
            segment_index,
            num_segments,
        } => {
            // For segment 0, fire progress event before validation consumes `item`.
            if segment_index == 0 {
                if let (Some(cb), Some(it)) = (progress.as_deref_mut(), item.as_ref()) {
                    cb(BackupProgressEvent::FileStarted {
                        path: it.path.clone(),
                    });
                }
            }

            let dedup_baseline = if verbose && segment_index == 0 {
                stats.deduplicated_size
            } else {
                0
            };

            validate_segment_accum(
                large_file_accum,
                item,
                abs_path,
                pre_meta,
                segment_index,
                num_segments,
                dedup_baseline,
                repo,
                stats,
            )?;

            // Process chunks via shared helper.
            let accum = large_file_accum.as_mut().ok_or_else(|| {
                VykarError::Other("BUG: large_file_accum missing after segment validation".into())
            })?;
            process_worker_chunks(
                repo,
                &mut accum.item,
                chunks,
                stats,
                compression,
                dedup_filter,
            )?;
            budget.release(acquired_bytes);

            if segment_index < num_segments - 1 {
                emit_stats_progress(progress, stats, None);
            }

            if segment_index == num_segments - 1 {
                // Last segment: finalize.
                let mut accum = large_file_accum.take().ok_or_else(|| {
                    VykarError::Other(
                        "BUG: large_file_accum missing at segment finalization".into(),
                    )
                })?;
                stats.nfiles += 1;

                // Commit the checkpoint — all segments landed successfully.
                repo.commit_rollback_checkpoint();

                // Update item metadata from segment 0's pre_meta (matches
                // the chunks we committed across all segments).
                let canon = accum.metadata;
                accum.item.mode = canon.mode;
                accum.item.uid = canon.uid;
                accum.item.gid = canon.gid;
                accum.item.size = canon.size;
                accum.item.mtime = canon.mtime_ns;
                accum.item.ctime = Some(canon.ctime_ns);

                if verbose {
                    let added_bytes = stats.deduplicated_size - accum.dedup_baseline;
                    let status = if old_file_cache.contains(&accum.abs_path) {
                        FileStatus::Modified
                    } else {
                        FileStatus::New
                    };
                    super::emit_progress(
                        progress,
                        BackupProgressEvent::FileProcessed {
                            path: accum.item.path.clone(),
                            status,
                            added_bytes,
                        },
                    );
                }

                append_item_to_stream(
                    repo,
                    item_stream,
                    item_ptrs,
                    &accum.item,
                    items_config,
                    compression,
                )?;

                new_file_cache.insert(
                    &accum.abs_path,
                    canon.device,
                    canon.inode,
                    canon.mtime_ns,
                    canon.ctime_ns,
                    canon.size,
                    CachedChunks::from_chunk_refs(&accum.item.chunks),
                );

                emit_stats_progress(progress, stats, Some(std::mem::take(&mut accum.item.path)));
            }
        }

        ProcessedEntry::CacheHit {
            mut item,
            abs_path,
            metadata,
            cached_refs,
        } => {
            if let Some(cb) = progress.as_deref_mut() {
                cb(BackupProgressEvent::FileStarted {
                    path: item.path.clone(),
                });
            }

            super::commit::commit_cache_hit(repo, &mut item, &cached_refs, stats)?;

            if verbose {
                super::emit_progress(
                    progress,
                    BackupProgressEvent::FileProcessed {
                        path: item.path.clone(),
                        status: FileStatus::Unchanged,
                        added_bytes: 0,
                    },
                );
            }

            append_item_to_stream(
                repo,
                item_stream,
                item_ptrs,
                &item,
                items_config,
                compression,
            )?;

            new_file_cache.insert(
                &abs_path,
                metadata.device,
                metadata.inode,
                metadata.mtime_ns,
                metadata.ctime_ns,
                metadata.size,
                cached_refs,
            );

            debug!(path = %item.path, "file cache hit (parallel)");
            emit_stats_progress(progress, stats, Some(std::mem::take(&mut item.path)));
        }

        ProcessedEntry::NonFile { item } => {
            append_item_to_stream(
                repo,
                item_stream,
                item_ptrs,
                &item,
                items_config,
                compression,
            )?;
        }

        ProcessedEntry::SourceStarted { path } => {
            super::emit_progress(
                progress,
                BackupProgressEvent::SourceStarted { source_path: path },
            );
        }

        ProcessedEntry::SourceFinished { path } => {
            super::emit_progress(
                progress,
                BackupProgressEvent::SourceFinished { source_path: path },
            );
        }

        // Skipped entries are handled in the consumer loop before reaching here.
        ProcessedEntry::Skipped { .. }
        | ProcessedEntry::SegmentSkipped { .. }
        | ProcessedEntry::WalkSkip => {
            unreachable!(
                "Skipped/SegmentSkipped/WalkSkip should be handled before consume_processed_entry"
            );
        }
    }

    Ok(())
}

/// Result from the pipeline's internal channels.
///
/// Workers send `Ok` or `WorkerErr` (both carrying a sequence index for
/// reordering). The walk thread sends `WalkErr` for fail-fast errors that
/// need no reordering.
enum PipelineResult {
    /// Successful processing — `(seq_idx, entry)`.
    Ok(usize, Box<ProcessedEntry>),
    /// Worker error — `(seq_idx, error)`. Delivered in order via the
    /// reorder buffer so earlier successes are committed first.
    WorkerErr(usize, VykarError),
    /// Walk-thread error — no index, triggers immediate fail-fast.
    WalkErr(VykarError),
}

/// Immutable inputs to the parallel pipeline. Bundled so `run_parallel_pipeline`
/// and its walk/worker/consumer stages can thread them through without passing
/// 20+ individual arguments.
#[derive(Clone, Copy)]
pub(crate) struct PipelineCtx<'a> {
    pub sources: &'a [ResolvedSource],
    pub exclude_patterns: &'a [String],
    pub exclude_if_present: &'a [String],
    pub one_file_system: bool,
    pub git_ignore: bool,
    pub xattrs_enabled: bool,
    pub file_cache: &'a FileCache,
    pub crypto: &'a Arc<dyn CryptoEngine>,
    pub compression: Compression,
    pub read_limiter: Option<&'a ByteRateLimiter>,
    pub num_workers: usize,
    pub readahead_depth: usize,
    pub segment_size: u64,
    pub items_config: &'a ChunkerConfig,
    pub pipeline_buffer_bytes: usize,
    pub dedup_filter: Option<&'a xorf::Xor8>,
    pub shutdown: Option<&'a AtomicBool>,
    pub verbose: bool,
    pub parent_reuse_index: Option<&'a ParentReuseIndex>,
}

/// Mutable output buffers written by the pipeline's consumer stage.
pub(crate) struct PipelineBuffers<'a> {
    pub item_stream: &'a mut Vec<u8>,
    pub item_ptrs: &'a mut Vec<ChunkId>,
    pub stats: &'a mut SnapshotStats,
    pub new_file_cache: &'a mut FileCache,
}

/// Run the parallel file processing pipeline using crossbeam-channel.
///
/// Walk → bounded work channel → N worker threads → bounded result channel
/// → reorder buffer → sequential consumer.
///
/// Unlike the previous pariter-based design, no single thread both feeds
/// work and delivers results, eliminating the deadlock where budget
/// acquisition in the walk thread could block result delivery.
pub(crate) fn run_parallel_pipeline(
    repo: &mut Repository,
    ctx: &PipelineCtx<'_>,
    bufs: &mut PipelineBuffers<'_>,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
) -> Result<()> {
    let PipelineCtx {
        sources,
        exclude_patterns,
        exclude_if_present,
        one_file_system,
        git_ignore,
        xattrs_enabled,
        file_cache,
        crypto,
        compression,
        read_limiter,
        num_workers,
        readahead_depth,
        segment_size,
        items_config,
        pipeline_buffer_bytes,
        dedup_filter,
        shutdown,
        verbose,
        parent_reuse_index,
    } = *ctx;
    let PipelineBuffers {
        item_stream,
        item_ptrs,
        stats,
        new_file_cache,
    } = bufs;
    debug_assert!(segment_size > 0, "segment_size must be non-zero");
    debug_assert!(num_workers > 0, "num_workers must be non-zero");
    let chunk_id_key = *crypto.chunk_id_key();
    let chunker_config = repo.config.chunker_params.clone();
    let budget = ByteBudget::new(pipeline_buffer_bytes);

    // Channel capacities.
    // work_cap: scheduling slack between walk and workers.
    // result_cap: reorder buffer headroom between workers and consumer.
    // ByteBudget enforces the byte-level memory cap on top of these.
    let work_cap = num_workers * 2;
    let result_cap = num_workers + readahead_depth;

    std::thread::scope(|s| {
        // Stage A → B: walk thread sends pre-budgeted entries to workers.
        let (work_tx, work_rx) = crossbeam_channel::bounded::<(usize, WalkEntry, usize)>(work_cap);

        // Stage B → C: workers send ordered results to consumer.
        let (result_tx, result_rx) = crossbeam_channel::bounded::<PipelineResult>(result_cap);

        // Reference so `move` closures capture `&ByteBudget`, not move it.
        let budget_ref = &budget;

        // --- Stage A: Walk thread — iterate + acquire budget in walk order ---
        let walk_result_tx = result_tx.clone();
        s.spawn(move || {
            let walk_iter = build_walk_iter(
                sources,
                exclude_patterns,
                exclude_if_present,
                one_file_system,
                git_ignore,
                xattrs_enabled,
                file_cache,
                segment_size,
                parent_reuse_index,
            );

            let mut seq_idx: usize = 0;
            for entry_result in walk_iter {
                if shutdown.is_some_and(|f| f.load(Ordering::Relaxed)) {
                    break;
                }
                match entry_result {
                    Ok(entry) => {
                        let acquired = match reserve_budget(&entry, budget_ref) {
                            Ok(n) => n,
                            Err(_) => {
                                // Budget poisoned — stop walking. Consumer
                                // already knows about the error.
                                return;
                            }
                        };
                        if work_tx.send((seq_idx, entry, acquired)).is_err() {
                            // Workers/consumer gone — release budget and stop.
                            budget_ref.release(acquired);
                            return;
                        }
                        seq_idx += 1;
                    }
                    Err(e) => {
                        // Walk error: fail-fast, no index needed.
                        let _ = walk_result_tx.send(PipelineResult::WalkErr(e));
                        return;
                    }
                }
            }
            // work_tx drops here → workers drain remaining items and exit.
        });

        // --- Stage B: N worker threads ---
        // Drop originals after spawning so channels close when workers exit.
        for _ in 0..num_workers {
            let rx = work_rx.clone();
            let tx = result_tx.clone();
            let chunker_cfg = chunker_config.clone();
            s.spawn(move || {
                for (idx, entry, pre_acquired) in rx {
                    let result = process_file_worker(
                        entry,
                        &chunk_id_key,
                        &**crypto,
                        compression,
                        &chunker_cfg,
                        read_limiter,
                        budget_ref,
                        pre_acquired,
                        dedup_filter,
                    );
                    let msg = match result {
                        Ok(processed) => PipelineResult::Ok(idx, Box::new(processed)),
                        Err(e) => PipelineResult::WorkerErr(idx, e),
                    };
                    if tx.send(msg).is_err() {
                        return; // Consumer gone.
                    }
                }
            });
        }
        // Drop originals — channels now close only when all clones drop.
        drop(work_rx);
        drop(result_tx);

        // --- Stage C: Consumer with reorder buffer ---
        let mut next_expected: usize = 0;
        let mut pending: BTreeMap<usize, std::result::Result<ProcessedEntry, VykarError>> =
            BTreeMap::new();
        let mut consume_err: Option<VykarError> = None;
        let mut large_file_accum: Option<LargeFileAccum> = None;
        // When segment 0 is skipped, we drain remaining segments silently.
        let mut segments_to_skip: usize = 0;

        for msg in &result_rx {
            if shutdown.is_some_and(|f| f.load(Ordering::Relaxed)) {
                budget.poison();
                consume_err = Some(VykarError::Interrupted);
                break;
            }
            match msg {
                PipelineResult::Ok(idx, entry) => {
                    pending.insert(idx, Ok(*entry));
                }
                PipelineResult::WorkerErr(idx, e) => {
                    pending.insert(idx, Err(e));
                }
                PipelineResult::WalkErr(e) => {
                    if e.is_soft_file_error() {
                        // Soft walk error — count it and continue.
                        stats.errors += 1;
                        continue;
                    }
                    // Walk errors bypass the reorder buffer — some earlier
                    // entries already dispatched to workers may not be
                    // consumed. This is fine: the backup will fail, no
                    // snapshot is saved, and in-memory pack buffers are
                    // dropped on return.
                    budget.poison();
                    consume_err = Some(e);
                    break;
                }
            }

            // Drain consecutive entries starting from next_expected.
            while let Some(result) = pending.remove(&next_expected) {
                next_expected += 1;

                // Draining remaining segments of a skipped file.
                if segments_to_skip > 0 {
                    if let Ok(ProcessedEntry::FileSegment { acquired_bytes, .. }) = &result {
                        budget.release(*acquired_bytes);
                    }
                    // SegmentSkipped / errors: budget already released by guard.
                    segments_to_skip -= 1;
                    continue;
                }

                match result {
                    Ok(ProcessedEntry::Skipped { path, reason }) => {
                        stats.errors += 1;
                        super::emit_post_commit_warning(
                            progress,
                            format!("skipping file '{path}': {reason}"),
                        );
                    }
                    Ok(ProcessedEntry::WalkSkip) => {
                        // Walker already logged the failing path with
                        // tracing::warn!; surface only the error count.
                        stats.errors += 1;
                    }
                    Ok(ProcessedEntry::SegmentSkipped {
                        segment_index,
                        num_segments,
                        path,
                        reason,
                    }) => {
                        if segment_index == 0 || large_file_accum.is_none() {
                            // Nothing committed yet — simple count + drain.
                            stats.errors += 1;
                            super::emit_post_commit_warning(
                                progress,
                                format!("skipping file '{path}': {reason}"),
                            );
                            segments_to_skip = num_segments.saturating_sub(segment_index + 1);
                        } else {
                            // Mid-accumulation skip: rollback earlier segments.
                            segments_to_skip = rollback_and_skip_large_file(
                                repo,
                                stats,
                                &mut large_file_accum,
                                progress,
                                segment_index,
                                &reason,
                            );
                        }
                    }
                    Ok(entry) => {
                        if let Err(e) = consume_processed_entry(
                            entry,
                            repo,
                            stats,
                            new_file_cache,
                            items_config,
                            item_stream,
                            item_ptrs,
                            compression,
                            progress,
                            &budget,
                            dedup_filter,
                            &mut large_file_accum,
                            file_cache,
                            verbose,
                        ) {
                            budget.poison();
                            consume_err = Some(e);
                            break;
                        }
                    }
                    Err(e) => {
                        if e.is_soft_file_error() {
                            if large_file_accum.is_some() {
                                // Mid-accumulation soft worker error: rollback
                                // earlier segments. `next_expected_index`
                                // is the segment we were about to process,
                                // so current_segment_index is one less.
                                let current = large_file_accum
                                    .as_ref()
                                    .map(|a| a.next_expected_index)
                                    .unwrap_or(0);
                                segments_to_skip = rollback_and_skip_large_file(
                                    repo,
                                    stats,
                                    &mut large_file_accum,
                                    progress,
                                    current,
                                    &e.to_string(),
                                );
                            } else {
                                stats.errors += 1;
                            }
                            continue;
                        }
                        budget.poison();
                        consume_err = Some(e);
                        break;
                    }
                }
            }

            if consume_err.is_some() {
                break;
            }
        }

        // Drop receiver to unblock any workers stuck on result_tx.send().
        drop(result_rx);

        // Verify no in-progress segmented file was left incomplete.
        if consume_err.is_none() {
            if let Some(accum) = &large_file_accum {
                consume_err = Some(VykarError::Other(format!(
                    "incomplete segmented file '{}': received {}/{} segments",
                    accum.abs_path, accum.next_expected_index, accum.num_segments,
                )));
            }
        }

        debug_assert!(
            budget.peak_acquired() <= pipeline_buffer_bytes,
            "pipeline exceeded memory budget: peak {} > cap {}",
            budget.peak_acquired(),
            pipeline_buffer_bytes,
        );

        if let Some(e) = consume_err {
            return Err(e);
        }

        // The walk thread may have exited cleanly on shutdown without
        // the consumer ever seeing a message.  Catch that here.
        if shutdown.is_some_and(|f| f.load(Ordering::Relaxed)) {
            return Err(VykarError::Interrupted);
        }

        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::item::ItemType;
    use std::sync::atomic::Ordering;

    // -----------------------------------------------------------------------
    // Segment accumulator validation tests
    // -----------------------------------------------------------------------

    fn test_item(path: &str) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: 1024,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
        }
    }

    fn test_metadata() -> fs::MetadataSummary {
        fs::MetadataSummary {
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            device: 0,
            inode: 0,
            size: 1024,
        }
    }

    fn test_stats() -> SnapshotStats {
        SnapshotStats::default()
    }

    #[test]
    fn segment_out_of_order() {
        let mut repo = crate::testutil::test_repo_plaintext();
        repo.enable_dedup_mode();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            Some(meta),
            0,
            3,
            0,
            &mut repo,
            &stats,
        )
        .unwrap();

        // Skip segment 1, feed segment 2 → error.
        let err = validate_segment_accum(
            &mut accum,
            None,
            "/tmp/file_a".into(),
            None,
            2,
            3,
            0,
            &mut repo,
            &stats,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("segment index mismatch"),
            "expected 'segment index mismatch', got: {err}"
        );
        repo.abort_rollback_checkpoint();
    }

    #[test]
    fn segment_file_identity_mismatch() {
        let mut repo = crate::testutil::test_repo_plaintext();
        repo.enable_dedup_mode();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 for file A.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            Some(meta),
            0,
            3,
            0,
            &mut repo,
            &stats,
        )
        .unwrap();

        // Feed segment 1 with different abs_path → error.
        let err = validate_segment_accum(
            &mut accum,
            None,
            "/tmp/file_b".into(),
            None,
            1,
            3,
            0,
            &mut repo,
            &stats,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("segment file identity mismatch"),
            "expected 'segment file identity mismatch', got: {err}"
        );
        repo.abort_rollback_checkpoint();
    }

    #[test]
    fn segment_nested_start() {
        let mut repo = crate::testutil::test_repo_plaintext();
        repo.enable_dedup_mode();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 for file A (3 segments).
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            Some(meta),
            0,
            3,
            0,
            &mut repo,
            &stats,
        )
        .unwrap();

        // Feed segment 0 for file B before file A completes → error.
        // The overlap check must fire BEFORE begin_rollback_checkpoint, so
        // the tracker remains armed from file A only.
        let err = validate_segment_accum(
            &mut accum,
            Some(test_item("file_b")),
            "/tmp/file_b".into(),
            Some(meta),
            0,
            2,
            0,
            &mut repo,
            &stats,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("nested large file segmentation"),
            "expected 'nested large file segmentation', got: {err}"
        );
        repo.abort_rollback_checkpoint();
    }

    #[test]
    fn segment_without_start() {
        let mut repo = crate::testutil::test_repo_plaintext();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;

        // Feed segment 1 with no prior segment 0 → error.
        let err = validate_segment_accum(
            &mut accum,
            None,
            "/tmp/file_a".into(),
            None,
            1,
            3,
            0,
            &mut repo,
            &stats,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("FileSegment without preceding segment 0"),
            "expected 'FileSegment without preceding segment 0', got: {err}"
        );
    }

    #[test]
    fn incomplete_accumulator_check() {
        let mut repo = crate::testutil::test_repo_plaintext();
        repo.enable_dedup_mode();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 and 1 of a 3-segment file, but not segment 2.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            Some(meta),
            0,
            3,
            0,
            &mut repo,
            &stats,
        )
        .unwrap();
        validate_segment_accum(
            &mut accum,
            None,
            "/tmp/file_a".into(),
            None,
            1,
            3,
            0,
            &mut repo,
            &stats,
        )
        .unwrap();

        // Simulate the post-loop check from run_parallel_pipeline.
        assert!(accum.is_some(), "accum should still be active");
        let a = accum.as_ref().unwrap();
        assert_eq!(a.next_expected_index, 2);
        assert_eq!(a.num_segments, 3);
        // The real pipeline generates this error:
        let err_msg = format!(
            "incomplete segmented file '{}': received {}/{} segments",
            a.abs_path, a.next_expected_index, a.num_segments,
        );
        assert!(
            err_msg.contains("incomplete segmented file"),
            "expected incomplete message, got: {err_msg}"
        );
        repo.abort_rollback_checkpoint();
    }

    #[test]
    fn segment_count_mismatch() {
        let mut repo = crate::testutil::test_repo_plaintext();
        repo.enable_dedup_mode();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 with num_segments=3.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            Some(meta),
            0,
            3,
            0,
            &mut repo,
            &stats,
        )
        .unwrap();

        // Feed segment 1 with different num_segments → error.
        let err = validate_segment_accum(
            &mut accum,
            None,
            "/tmp/file_a".into(),
            None,
            1,
            5,
            0,
            &mut repo,
            &stats,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("segment count mismatch"),
            "expected 'segment count mismatch', got: {err}"
        );
        repo.abort_rollback_checkpoint();
    }

    #[test]
    fn segment_happy_path() {
        let mut repo = crate::testutil::test_repo_plaintext();
        repo.enable_dedup_mode();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed all 3 segments in order.
        for i in 0..3 {
            let item = if i == 0 {
                Some(test_item("file_a"))
            } else {
                None
            };
            let pre_meta = (i == 0).then_some(meta);
            validate_segment_accum(
                &mut accum,
                item,
                "/tmp/file_a".into(),
                pre_meta,
                i,
                3,
                0,
                &mut repo,
                &stats,
            )
            .unwrap();
        }

        // Accumulator should be present with next_expected_index == 3.
        let a = accum.as_ref().unwrap();
        assert_eq!(a.next_expected_index, 3);
        assert_eq!(a.num_segments, 3);
        assert_eq!(a.item.path, "file_a");
        assert_eq!(&*a.abs_path, "/tmp/file_a");
        repo.abort_rollback_checkpoint();
    }

    /// Drives `rollback_and_skip_large_file` directly (Mechanism B): an
    /// accumulator midway through a 4-segment file is rolled back, byte
    /// counters are restored, and the drain count for the remaining
    /// segments is reported correctly.
    #[test]
    fn rollback_and_skip_large_file_drains_and_restores_stats() {
        let mut repo = crate::testutil::test_repo_plaintext();
        repo.enable_dedup_mode();

        // Arm the checkpoint just like validate_segment_accum would for
        // segment 0.
        repo.begin_rollback_checkpoint().unwrap();
        assert!(repo.rollback_tracker_armed());

        // Snapshot initial byte counters (all zeros), then dirty them so
        // we can verify they get restored.
        let stats_snap = SnapshotStats::default().snapshot_byte_counters();
        let mut stats = SnapshotStats {
            deduplicated_size: 12345,
            original_size: 99999,
            compressed_size: 4242,
            errors: 0,
            ..Default::default()
        };

        let accum = LargeFileAccum {
            item: test_item("big"),
            abs_path: Arc::from("/tmp/big"),
            metadata: test_metadata(),
            next_expected_index: 2,
            num_segments: 4,
            dedup_baseline: 0,
            stats_snap,
        };
        let mut accum = Some(accum);
        let mut progress: Option<&mut dyn FnMut(BackupProgressEvent)> = None;

        let drain = rollback_and_skip_large_file(
            &mut repo,
            &mut stats,
            &mut accum,
            &mut progress,
            2,
            "drift",
        );

        // 4 segments, skipped at index 2 → segments 3 remain to drain.
        assert_eq!(drain, 1, "4 segs, skipped at idx 2 → 1 remaining");
        assert!(accum.is_none(), "accumulator cleared");
        assert!(!repo.rollback_tracker_armed(), "checkpoint must be aborted");
        assert_eq!(stats.errors, 1);
        assert_eq!(
            stats.deduplicated_size, 0,
            "deduplicated_size restored from snapshot"
        );
        assert_eq!(
            stats.original_size, 0,
            "original_size restored from snapshot"
        );
        assert_eq!(
            stats.compressed_size, 0,
            "compressed_size restored from snapshot"
        );
    }

    /// Idempotency: calling `rollback_and_skip_large_file` with no
    /// accumulator is a no-op — drain count of zero, stats untouched.
    #[test]
    fn rollback_and_skip_large_file_no_accum_is_noop() {
        let mut repo = crate::testutil::test_repo_plaintext();
        let mut stats = SnapshotStats {
            deduplicated_size: 7,
            ..Default::default()
        };
        let mut accum: Option<LargeFileAccum> = None;
        let mut progress: Option<&mut dyn FnMut(BackupProgressEvent)> = None;

        let drain = rollback_and_skip_large_file(
            &mut repo,
            &mut stats,
            &mut accum,
            &mut progress,
            0,
            "ignored",
        );

        assert_eq!(drain, 0);
        assert_eq!(stats.errors, 0, "no error counted when accum was empty");
        assert_eq!(
            stats.deduplicated_size, 7,
            "stats untouched when accum was empty"
        );
    }

    // -----------------------------------------------------------------------
    // Crossbeam-channel pipeline tests
    // -----------------------------------------------------------------------

    /// Regression test: pipeline must not deadlock when large items exhaust
    /// the budget and workers complete out of order.
    ///
    /// Simulates the scenario that caused the original pariter deadlock:
    /// small budget + large items + workers finishing in reverse order.
    #[test]
    fn pipeline_no_deadlock_with_small_budget_and_large_items() {
        use std::sync::atomic::AtomicBool;
        use std::time::Duration;

        let completed = Arc::new(AtomicBool::new(false));
        let completed2 = Arc::clone(&completed);

        // Run the pipeline logic in a scoped thread so we can timeout.
        let handle = std::thread::spawn(move || {
            let budget = ByteBudget::new(200); // Small budget
            let num_workers = 4;
            let work_cap = num_workers * 2;
            let result_cap = num_workers + 4;

            let (work_tx, work_rx) = crossbeam_channel::bounded::<(usize, usize, usize)>(work_cap);
            let (result_tx, result_rx) = crossbeam_channel::bounded::<(usize, usize)>(result_cap);

            std::thread::scope(|s| {
                let budget_ref = &budget;

                // Walk thread: send 10 items, each requiring 100 bytes of budget.
                s.spawn(move || {
                    for i in 0..10 {
                        let acquired = budget_ref.acquire(100).unwrap();
                        work_tx.send((i, acquired, i)).unwrap();
                    }
                });

                // Workers: process items with forced out-of-order completion.
                for _ in 0..num_workers {
                    let rx = work_rx.clone();
                    let tx = result_tx.clone();
                    s.spawn(move || {
                        for (idx, acquired, delay_key) in &rx {
                            // Reverse-order sleep: item 0 sleeps longest.
                            if delay_key < 3 {
                                std::thread::sleep(Duration::from_millis(
                                    (3 - delay_key) as u64 * 5,
                                ));
                            }
                            let _ = tx.send((idx, acquired));
                        }
                    });
                }
                drop(work_rx);
                drop(result_tx);

                // Consumer: reorder and release budget.
                let mut next_expected = 0usize;
                let mut pending: BTreeMap<usize, usize> = BTreeMap::new();

                for (idx, acquired) in &result_rx {
                    pending.insert(idx, acquired);
                    while let Some(acq) = pending.remove(&next_expected) {
                        budget_ref.release(acq);
                        next_expected += 1;
                    }
                }

                assert_eq!(next_expected, 10, "all items should be consumed");
            });

            completed2.store(true, Ordering::SeqCst);
        });

        // If this times out, we have a deadlock.
        let timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if completed.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        assert!(
            completed.load(Ordering::SeqCst),
            "pipeline deadlocked — did not complete within {timeout:?}"
        );
        handle.join().unwrap();
    }

    /// Walk errors must propagate cleanly even when workers are active.
    #[test]
    fn pipeline_walk_error_propagates_cleanly() {
        let budget = ByteBudget::new(1000);
        let num_workers = 2;
        let work_cap = num_workers * 2;
        let result_cap = num_workers + 4;

        std::thread::scope(|s| {
            let (work_tx, work_rx) = crossbeam_channel::bounded::<(usize, u8)>(work_cap);
            let (result_tx, result_rx) = crossbeam_channel::bounded::<PipelineResult>(result_cap);

            let walk_result_tx = result_tx.clone();
            let budget_ref = &budget;

            // Walk thread: send 3 successes, then a walk error.
            s.spawn(move || {
                for i in 0..3 {
                    let _ = work_tx.send((i, 0));
                }
                let _ = walk_result_tx.send(PipelineResult::WalkErr(VykarError::Other(
                    "walk failed".into(),
                )));
                // Drop work_tx to let workers drain.
            });

            // Workers: passthrough.
            for _ in 0..num_workers {
                let rx = work_rx.clone();
                let tx = result_tx.clone();
                s.spawn(move || {
                    for (idx, _) in rx {
                        let entry = ProcessedEntry::NonFile {
                            item: test_item(&format!("item_{idx}")),
                        };
                        let _ = tx.send(PipelineResult::Ok(idx, Box::new(entry)));
                    }
                });
            }
            drop(work_rx);
            drop(result_tx);

            // Consumer: collect results.
            let mut consumed = Vec::new();
            let mut walk_err = None;
            let mut next_expected = 0usize;
            let mut pending: BTreeMap<usize, ProcessedEntry> = BTreeMap::new();

            for msg in &result_rx {
                match msg {
                    PipelineResult::Ok(idx, entry) => {
                        pending.insert(idx, *entry);
                    }
                    PipelineResult::WorkerErr(_, _) => unreachable!(),
                    PipelineResult::WalkErr(e) => {
                        budget_ref.poison();
                        walk_err = Some(e);
                        break;
                    }
                }

                while let Some(entry) = pending.remove(&next_expected) {
                    if let ProcessedEntry::NonFile { item } = &entry {
                        consumed.push(item.path.clone());
                    }
                    next_expected += 1;
                }
            }

            // Items before the walk error should have been consumed.
            // (Some or all of items 0-2, depending on channel timing.)
            assert!(walk_err.is_some(), "walk error should have been received");
            assert!(
                walk_err.unwrap().to_string().contains("walk failed"),
                "should contain the walk error message"
            );
        });
    }

    /// Budget bytes must not leak when the consumer hits an error mid-stream.
    #[test]
    fn pipeline_consumer_error_releases_budget() {
        let cap = 1000usize;
        let budget = Arc::new(ByteBudget::new(cap));

        std::thread::scope(|s| {
            let budget_ref = &*budget;
            let num_workers = 2;
            let (work_tx, work_rx) = crossbeam_channel::bounded::<(usize, usize)>(num_workers * 2);
            let (result_tx, result_rx) =
                crossbeam_channel::bounded::<(usize, usize)>(num_workers + 4);

            // Walk thread: send 5 items, each 100 bytes.
            s.spawn(move || {
                for i in 0..5 {
                    let acquired = budget_ref.acquire(100).unwrap();
                    if work_tx.send((i, acquired)).is_err() {
                        budget_ref.release(acquired);
                        return;
                    }
                }
            });

            // Workers: passthrough.
            for _ in 0..num_workers {
                let rx = work_rx.clone();
                let tx = result_tx.clone();
                s.spawn(move || {
                    for (idx, acquired) in rx {
                        if tx.send((idx, acquired)).is_err() {
                            budget_ref.release(acquired);
                            return;
                        }
                    }
                });
            }
            drop(work_rx);
            drop(result_tx);

            // Consumer: fail on item 2, release budget for consumed items.
            let mut next_expected = 0usize;
            let mut pending: BTreeMap<usize, usize> = BTreeMap::new();

            for (idx, acquired) in &result_rx {
                pending.insert(idx, acquired);
                while let Some(acq) = pending.remove(&next_expected) {
                    if next_expected == 2 {
                        // Simulate consumer error: poison + release this item.
                        budget_ref.poison();
                        budget_ref.release(acq);
                        next_expected += 1;
                        break;
                    }
                    budget_ref.release(acq);
                    next_expected += 1;
                }
                if budget_ref.state.lock().unwrap().poisoned {
                    // Release any remaining pending items.
                    for acq in pending.values() {
                        budget_ref.release(*acq);
                    }
                    pending.clear();
                    break;
                }
            }

            // Drain remaining results and release their budget.
            for (_, acquired) in &result_rx {
                budget_ref.release(acquired);
            }
        });

        // After all threads exit, budget should be fully available.
        let st = budget.state.lock().unwrap();
        assert_eq!(
            st.available, st.capacity,
            "budget leaked: available={}, capacity={}",
            st.available, st.capacity
        );
    }

    /// BudgetGuard must release bytes when a worker panics.
    #[test]
    fn pipeline_budget_guard_releases_on_worker_panic() {
        let budget = ByteBudget::new(200);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::thread::scope(|s| {
                let budget_ref = &budget;
                s.spawn(move || {
                    let _guard = BudgetGuard::from_pre_acquired(budget_ref, 100);
                    panic!("simulated worker panic");
                });
            });
        }));

        assert!(result.is_err(), "scope should propagate panic");

        // Guard should have released 100 bytes on drop during unwind.
        let acquired = budget.acquire(200).unwrap();
        assert_eq!(
            acquired, 200,
            "full budget should be available after panic cleanup"
        );
        budget.release(200);
    }

    // -----------------------------------------------------------------------
    // estimate_chunk_count tests
    // -----------------------------------------------------------------------

    #[test]
    fn estimate_chunk_count_basic() {
        // 10 MiB file, 128 KiB avg → ~80 chunks + 1
        assert_eq!(estimate_chunk_count(10 * 1024 * 1024, 128 * 1024), 81);
    }

    #[test]
    fn estimate_chunk_count_zero_avg() {
        assert_eq!(estimate_chunk_count(1024, 0), 1);
    }

    #[test]
    fn estimate_chunk_count_zero_data() {
        assert_eq!(estimate_chunk_count(0, 128 * 1024), 1);
    }

    #[test]
    fn estimate_chunk_count_clamps_large() {
        // u64::MAX should clamp to 4096
        assert_eq!(estimate_chunk_count(u64::MAX, 1), 4096);
    }
}
