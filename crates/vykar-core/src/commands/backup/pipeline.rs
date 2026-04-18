use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tracing::{debug, warn};

use crate::chunker;
use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::limits::{self, ByteRateLimiter};
use crate::platform::fs;
use crate::repo::file_cache::{FileCache, ParentReuseIndex};
use crate::repo::Repository;
use crate::snapshot::item::{ChunkRef, Item};
use crate::snapshot::SnapshotStats;
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use super::chunk_process::classify_chunk;
use super::commit::process_worker_chunks;
use super::concurrency::{BudgetGuard, ByteBudget};
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
        metadata: fs::MetadataSummary,
        chunks: Vec<super::WorkerChunk>,
        /// Bytes acquired from ByteBudget; consumer must release after committing.
        acquired_bytes: usize,
    },
    /// Segment of a large file: worker processed one fixed-size slice.
    FileSegment {
        /// Only present for segment 0; `None` for continuations.
        item: Option<Item>,
        abs_path: Arc<str>,
        metadata: fs::MetadataSummary,
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
        cached_refs: Arc<Vec<ChunkRef>>,
    },
    /// Non-file item (directory, symlink, zero-size file).
    NonFile {
        item: Item,
    },
    /// A file skipped due to a soft error (permission denied, not found).
    /// No data was committed for this file.
    Skipped,
    /// Segment 0 of a large file was skipped due to a soft error.
    /// The consumer must drain remaining segments via `segments_to_skip`.
    SegmentSkipped {
        num_segments: usize,
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

/// Wrapper that converts soft I/O errors into `Skipped` / `SegmentSkipped`
/// for entries where no data has been committed yet.
///
/// Segment N>0 errors are NEVER converted — they propagate as hard errors
/// because earlier segments may already be committed.
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
    let is_regular_file = matches!(&entry, WalkEntry::File { .. });
    let segment_info = match &entry {
        WalkEntry::FileSegment {
            segment_index,
            num_segments,
            ..
        } => Some((*segment_index, *num_segments)),
        _ => None,
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
            if is_regular_file {
                warn!(error = %e, "skipping file in pipeline (soft error)");
                Ok(ProcessedEntry::Skipped)
            } else if let Some((0, num_segments)) = segment_info {
                // Segment 0 only — safe because no data committed yet.
                warn!(error = %e, "skipping segmented file in pipeline (soft error on segment 0)");
                Ok(ProcessedEntry::SegmentSkipped { num_segments })
            } else {
                // Segment N>0: NOT safe to convert — propagate as hard error.
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
            file_size,
        } => {
            // Budget was pre-acquired by the walk thread. Wrap in a guard for
            // error safety — if we `?`-bail, the guard drops and releases bytes.
            let guard = BudgetGuard::from_pre_acquired(budget, pre_acquired_bytes);

            // Small file (< min_chunk_size): read whole, single chunk.
            if file_size < chunker_config.min_size as u64 {
                let mut file = File::open(Path::new(&abs_path)).map_err(VykarError::Io)?;
                let mut data = Vec::with_capacity(file_size as usize);
                if let Some(limiter) = read_limiter {
                    limits::LimitedReader::new(&mut file, Some(limiter))
                        .read_to_end(&mut data)
                        .map_err(VykarError::Io)?;
                } else {
                    file.read_to_end(&mut data).map_err(VykarError::Io)?;
                }

                let chunk_id = ChunkId::compute(chunk_id_key, &data);
                let worker_chunk =
                    classify_chunk(chunk_id, data, dedup_filter, compression, crypto)?;

                let acquired_bytes = guard.defuse();
                return Ok(ProcessedEntry::ProcessedFile {
                    item,
                    abs_path,
                    metadata,
                    chunks: vec![worker_chunk],
                    acquired_bytes,
                });
            }

            // Medium file: read, chunk via FastCDC, then hash → classify each chunk.
            let file = File::open(Path::new(&abs_path)).map_err(VykarError::Io)?;
            let chunk_stream = chunker::chunk_stream(
                limits::LimitedReader::new(file, read_limiter),
                chunker_config,
            );

            let mut worker_chunks =
                Vec::with_capacity(estimate_chunk_count(file_size, chunker_config.avg_size));
            for chunk_result in chunk_stream {
                let chunk = chunk_result.map_err(|e| match e {
                    fastcdc::v2020::Error::IoError(ioe) => VykarError::Io(ioe),
                    other => VykarError::Other(format!("chunking failed for {abs_path}: {other}")),
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

            let acquired_bytes = guard.defuse();
            Ok(ProcessedEntry::ProcessedFile {
                item,
                abs_path,
                metadata,
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

            let mut file = File::open(Path::new(&*abs_path)).map_err(VykarError::Io)?;
            file.seek(std::io::SeekFrom::Start(offset))
                .map_err(VykarError::Io)?;
            let reader = file.take(len);

            let chunk_stream = chunker::chunk_stream(
                limits::LimitedReader::new(reader, read_limiter),
                chunker_config,
            );

            let mut worker_chunks =
                Vec::with_capacity(estimate_chunk_count(len, chunker_config.avg_size));
            for chunk_result in chunk_stream {
                let chunk = chunk_result.map_err(|e| match e {
                    fastcdc::v2020::Error::IoError(ioe) => VykarError::Io(ioe),
                    other => VykarError::Other(format!("chunking failed for {abs_path}: {other}")),
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

            let acquired_bytes = guard.defuse();
            Ok(ProcessedEntry::FileSegment {
                item,
                abs_path,
                metadata,
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

        WalkEntry::Skipped => Ok(ProcessedEntry::Skipped),

        WalkEntry::SourceStarted { path } => Ok(ProcessedEntry::SourceStarted { path }),

        WalkEntry::SourceFinished { path } => Ok(ProcessedEntry::SourceFinished { path }),
    }
}

/// Tracks in-progress accumulation of a segmented large file.
struct LargeFileAccum {
    item: Item,
    abs_path: Arc<str>,
    metadata: fs::MetadataSummary,
    next_expected_index: usize,
    num_segments: usize,
    /// Baseline `deduplicated_size` when segment 0 started (for verbose added_bytes).
    dedup_baseline: u64,
}

/// Validate and update the segment accumulator state machine.
///
/// For segment 0: initializes the accumulator (errors if one already exists).
/// For continuations: validates ordering, file identity, and segment count.
/// Returns `Ok(())` on success.
fn validate_segment_accum(
    large_file_accum: &mut Option<LargeFileAccum>,
    item: Option<Item>,
    abs_path: Arc<str>,
    metadata: fs::MetadataSummary,
    segment_index: usize,
    num_segments: usize,
    dedup_baseline: u64,
) -> Result<()> {
    if segment_index == 0 {
        if large_file_accum.is_some() {
            return Err(VykarError::Other("nested large file segmentation".into()));
        }
        let item =
            item.ok_or_else(|| VykarError::Other("BUG: segment 0 must carry item".into()))?;
        *large_file_accum = Some(LargeFileAccum {
            item,
            abs_path,
            metadata,
            next_expected_index: 1,
            dedup_baseline,
            num_segments,
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
            metadata,
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
                metadata.device,
                metadata.inode,
                metadata.mtime_ns,
                metadata.ctime_ns,
                metadata.size,
                Arc::new(std::mem::take(&mut item.chunks)),
            );

            emit_stats_progress(progress, stats, Some(std::mem::take(&mut item.path)));
        }

        ProcessedEntry::FileSegment {
            item,
            abs_path,
            metadata,
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
                metadata,
                segment_index,
                num_segments,
                dedup_baseline,
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
                    accum.metadata.device,
                    accum.metadata.inode,
                    accum.metadata.mtime_ns,
                    accum.metadata.ctime_ns,
                    accum.metadata.size,
                    Arc::new(std::mem::take(&mut accum.item.chunks)),
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
        ProcessedEntry::Skipped | ProcessedEntry::SegmentSkipped { .. } => {
            unreachable!("Skipped/SegmentSkipped should be handled before consume_processed_entry");
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
    pub source_paths: &'a [String],
    pub multi_path: bool,
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
        source_paths,
        multi_path,
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
                source_paths,
                multi_path,
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
                    Ok(ProcessedEntry::Skipped) => {
                        stats.errors += 1;
                    }
                    Ok(ProcessedEntry::SegmentSkipped { num_segments }) => {
                        stats.errors += 1;
                        // Skip the remaining N-1 segments.
                        segments_to_skip = num_segments.saturating_sub(1);
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
                        // Safety net: soft WorkerErr on a regular file (no
                        // data committed). Only safe if we're not mid-accumulation.
                        if e.is_soft_file_error() && large_file_accum.is_none() {
                            stats.errors += 1;
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

    #[test]
    fn segment_out_of_order() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            meta,
            0,
            3,
            0,
        )
        .unwrap();

        // Skip segment 1, feed segment 2 → error.
        let err = validate_segment_accum(&mut accum, None, "/tmp/file_a".into(), meta, 2, 3, 0)
            .unwrap_err();
        assert!(
            err.to_string().contains("segment index mismatch"),
            "expected 'segment index mismatch', got: {err}"
        );
    }

    #[test]
    fn segment_file_identity_mismatch() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 for file A.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            meta,
            0,
            3,
            0,
        )
        .unwrap();

        // Feed segment 1 with different abs_path → error.
        let err = validate_segment_accum(&mut accum, None, "/tmp/file_b".into(), meta, 1, 3, 0)
            .unwrap_err();
        assert!(
            err.to_string().contains("segment file identity mismatch"),
            "expected 'segment file identity mismatch', got: {err}"
        );
    }

    #[test]
    fn segment_nested_start() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 for file A (3 segments).
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            meta,
            0,
            3,
            0,
        )
        .unwrap();

        // Feed segment 0 for file B before file A completes → error.
        let err = validate_segment_accum(
            &mut accum,
            Some(test_item("file_b")),
            "/tmp/file_b".into(),
            meta,
            0,
            2,
            0,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("nested large file segmentation"),
            "expected 'nested large file segmentation', got: {err}"
        );
    }

    #[test]
    fn segment_without_start() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 1 with no prior segment 0 → error.
        let err = validate_segment_accum(&mut accum, None, "/tmp/file_a".into(), meta, 1, 3, 0)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("FileSegment without preceding segment 0"),
            "expected 'FileSegment without preceding segment 0', got: {err}"
        );
    }

    #[test]
    fn incomplete_accumulator_check() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 and 1 of a 3-segment file, but not segment 2.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            meta,
            0,
            3,
            0,
        )
        .unwrap();
        validate_segment_accum(&mut accum, None, "/tmp/file_a".into(), meta, 1, 3, 0).unwrap();

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
    }

    #[test]
    fn segment_count_mismatch() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 with num_segments=3.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            meta,
            0,
            3,
            0,
        )
        .unwrap();

        // Feed segment 1 with different num_segments → error.
        let err = validate_segment_accum(&mut accum, None, "/tmp/file_a".into(), meta, 1, 5, 0)
            .unwrap_err();
        assert!(
            err.to_string().contains("segment count mismatch"),
            "expected 'segment count mismatch', got: {err}"
        );
    }

    #[test]
    fn segment_happy_path() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed all 3 segments in order.
        for i in 0..3 {
            let item = if i == 0 {
                Some(test_item("file_a"))
            } else {
                None
            };
            validate_segment_accum(&mut accum, item, "/tmp/file_a".into(), meta, i, 3, 0).unwrap();
        }

        // Accumulator should be present with next_expected_index == 3.
        let a = accum.as_ref().unwrap();
        assert_eq!(a.next_expected_index, 3);
        assert_eq!(a.num_segments, 3);
        assert_eq!(a.item.path, "file_a");
        assert_eq!(&*a.abs_path, "/tmp/file_a");
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
