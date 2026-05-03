//! Parallel file processing pipeline (crossbeam-channel).
//!
//! Walk → bounded work channel → N worker threads → bounded result channel
//! → reorder buffer → sequential consumer.
//!
//! This file is the orchestrator: it owns the `thread::scope`, the channels,
//! the `ByteBudget`, and the reorder buffer / skip-drain state machine that
//! wraps the consumer call. The three stage-specific submodules are:
//! - [`worker`] — chunking / hashing / encryption per file or segment.
//! - [`consumer`] — per-entry commit: pack write, item stream, file cache, stats.
//! - [`segmentation`] — large-file segmentation state machine (accumulator + cross-segment rollback) used by both the consumer and the orchestrator's skip-drain handling.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::limits::ByteRateLimiter;
use crate::platform::fs;
use crate::repo::file_cache::{CachedChunks, FileCache, ParentReuseIndex};
use crate::repo::Repository;
use crate::snapshot::SnapshotStats;
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use super::concurrency::ByteBudget;
use super::source::ResolvedSource;
use super::walk::{build_walk_iter, reserve_budget, WalkEntry};
use super::BackupProgressEvent;

mod consumer;
mod segmentation;
#[cfg(test)]
mod test_support;
mod worker;

use segmentation::{rollback_and_skip_large_file, LargeFileAccum};

// ---------------------------------------------------------------------------
// Channel message types
// ---------------------------------------------------------------------------

/// Result from a parallel worker.
pub(super) enum ProcessedEntry {
    /// Small/medium file: chunks classified by xor filter (hash-only or fully transformed).
    ProcessedFile {
        item: crate::snapshot::item::Item,
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
        item: Option<crate::snapshot::item::Item>,
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
        item: crate::snapshot::item::Item,
        abs_path: String,
        metadata: fs::MetadataSummary,
        cached_refs: CachedChunks,
    },
    /// Non-file item (directory, symlink, zero-size file).
    NonFile {
        item: crate::snapshot::item::Item,
    },
    /// A file skipped due to a soft error (permission denied, not found,
    /// or drift detected between walk and open / during read). No data
    /// was committed for this file.
    Skipped {
        path: String,
        /// Pre-formatted reason (avoids carrying `VykarError` across threads).
        reason: String,
    },
    /// A macOS dataless (FileProvider placeholder) file skipped at walk time
    /// because neither the local file cache nor the parent reuse index had
    /// a matching entry. No I/O was performed (no hydration, no read).
    /// Counted into a per-source running total and surfaced as a single
    /// end-of-source summary warning.
    DatalessSkipped {
        path: String,
    },
    /// The walker reported a soft error before it could materialize an
    /// `Item` (e.g. directory-iteration `EACCES`, Windows unsupported
    /// reparse tag, cloud-file unavailable). Carries the failing path and
    /// pre-formatted reason so the consumer can emit a path-bearing
    /// warning. Mirrors `sequential.rs` `WalkEvent::Skipped` handling.
    WalkSkip {
        path: String,
        reason: String,
    },
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

// ---------------------------------------------------------------------------
// Public-to-`backup`-mod entry points
// ---------------------------------------------------------------------------

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
                    let result = worker::process_file_worker(
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
        // Per-source running total of dataless cloud-only files skipped
        // (no parent-reuse hit). Reset on SourceStarted; flushed as a single
        // summary warning on SourceFinished.
        let mut dataless_skipped: u64 = 0;

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
                    Ok(ProcessedEntry::WalkSkip { path, reason }) => {
                        stats.errors += 1;
                        super::emit_post_commit_warning(
                            progress,
                            format!("skipping entry '{path}': {reason}"),
                        );
                    }
                    Ok(ProcessedEntry::DatalessSkipped { path }) => {
                        // Walker already emitted a tracing::debug! with the
                        // path; the consumer just bumps the per-source count
                        // so the SourceFinished arm can flush the summary.
                        let _ = path;
                        dataless_skipped += 1;
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
                        // Reset/flush per-source dataless counter around
                        // source boundaries before delegating to the consumer.
                        match &entry {
                            ProcessedEntry::SourceStarted { .. } => {
                                dataless_skipped = 0;
                            }
                            ProcessedEntry::SourceFinished { .. } if dataless_skipped > 0 => {
                                super::emit_dataless_summary(progress, dataless_skipped);
                                dataless_skipped = 0;
                            }
                            _ => {}
                        }
                        if let Err(e) = consumer::consume_processed_entry(
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
    use test_support::test_item;

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
        use super::super::concurrency::BudgetGuard;

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
}
