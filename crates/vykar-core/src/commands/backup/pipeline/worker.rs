//! Per-entry worker stage: chunking, hashing, classification, encryption.
//!
//! `process_file_worker` is invoked from worker threads for each `WalkEntry`
//! the walker emits. It opens the file, runs FastCDC chunking (or a single
//! whole-file read for sub-min-chunk files), hashes each chunk, classifies
//! it via the dedup filter, and packages the result as a `ProcessedEntry`
//! for the consumer. Soft I/O / drift errors are converted into
//! `Skipped` / `SegmentSkipped` variants here so the consumer can count
//! them without aborting the whole backup.

use std::sync::Arc;

use tracing::warn;

use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::index::dedup_cache::DedupPrefilter;
use crate::limits::ByteRateLimiter;
use crate::platform::fs;
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::{ChunkHasher, ChunkId};
use vykar_types::error::{Result, VykarError};

use super::super::chunk_process::classify_chunk;
use super::super::concurrency::{BudgetGuard, ByteBudget};
use super::super::drift::{ReadPlan, open_checked, read_range_drift_checked};
use super::super::walk::{WalkEntry, into_lossy_string};
use super::ProcessedEntry;

/// Estimate the number of chunks a file will produce, for pre-sizing Vecs.
fn estimate_chunk_count(data_len: u64, avg_chunk_size: u32) -> usize {
    if avg_chunk_size == 0 {
        return 1;
    }
    let est = (data_len / avg_chunk_size as u64).saturating_add(1);
    est.min(4096) as usize
}

/// Process a single walk entry in a parallel worker thread.
///
/// Soft I/O / drift errors are converted into `Skipped` / `SegmentSkipped`
/// variants in-line. Each `WalkEntry` arm destructures `abs_path` once,
/// runs the work in a closure that borrows it, and then either constructs
/// the success variant or the skip variant — eliminating the upfront clone
/// the previous wrapper needed.
///
/// For segment N>0, the consumer performs a cross-segment rollback of the
/// earlier segments' refcounts/dedup inserts before draining the rest, so
/// it is now safe to convert soft errors at any segment index.
///
/// Budget bytes are pre-acquired by the walk thread; `pre_acquired_bytes`
/// is wrapped in a [`BudgetGuard`] for error safety (auto-release on `?` bail).
#[allow(clippy::too_many_arguments)]
pub(super) fn process_file_worker(
    entry: WalkEntry,
    chunk_hasher: &ChunkHasher,
    crypto: &dyn CryptoEngine,
    compression: Compression,
    chunker_config: &ChunkerConfig,
    read_limiter: Option<&ByteRateLimiter>,
    budget: &ByteBudget,
    pre_acquired_bytes: usize,
    prefilter: Option<&DedupPrefilter>,
) -> Result<ProcessedEntry> {
    match entry {
        WalkEntry::File {
            item,
            abs_path,
            metadata,
            file_size: _,
        } => {
            // Open via the real OS path (may be non-UTF-8 on Unix); the lossy
            // string is only the cache key / display form, built after the read.
            let work = (|| -> Result<(fs::MetadataSummary, Vec<super::super::chunk_process::WorkerChunk>, usize)> {
                // Budget was pre-acquired by the walk thread. Wrap in a guard for
                // error safety — if we `?`-bail, the guard drops and releases bytes.
                let guard = BudgetGuard::from_pre_acquired(budget, pre_acquired_bytes);

                let (mut source, pre_meta) = open_checked(&abs_path, &metadata)?;

                // Small file (< min_chunk_size): read whole, single chunk.
                let (plan, capacity) = if pre_meta.size < chunker_config.min_size as u64 {
                    (ReadPlan::Whole, 1)
                } else {
                    (
                        ReadPlan::Chunked,
                        estimate_chunk_count(pre_meta.size, chunker_config.avg_size),
                    )
                };

                let mut worker_chunks = Vec::with_capacity(capacity);
                read_range_drift_checked(
                    &mut source,
                    &abs_path.to_string_lossy(),
                    &pre_meta,
                    plan,
                    chunker_config,
                    read_limiter,
                    |data| {
                        let chunk_id = ChunkId::compute(chunk_hasher, &data);
                        worker_chunks
                            .push(classify_chunk(chunk_id, data, prefilter, compression, crypto)?);
                        Ok(())
                    },
                )?;

                let acquired_bytes = guard.defuse();
                Ok((pre_meta, worker_chunks, acquired_bytes))
            })();
            let abs_path = into_lossy_string(abs_path);

            match work {
                Ok((pre_meta, chunks, acquired_bytes)) => Ok(ProcessedEntry::ProcessedFile {
                    item,
                    abs_path,
                    pre_meta,
                    chunks,
                    acquired_bytes,
                }),
                Err(e) if e.is_soft_file_error() => {
                    let reason = super::super::skip_reason(&e);
                    warn!(path = %abs_path, error = %e, "skipping file in pipeline (soft error)");
                    Ok(ProcessedEntry::Skipped {
                        path: abs_path,
                        reason,
                    })
                }
                Err(e) => Err(e),
            }
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
            let work = (|| -> Result<(fs::MetadataSummary, Vec<super::super::chunk_process::WorkerChunk>, usize)> {
                let guard = BudgetGuard::from_pre_acquired(budget, pre_acquired_bytes);

                // Segmented reads are a plan driven by walk-time size
                // (`num_segments`/`offset`/`len`). Any drift — before or
                // during the read — invalidates the plan, so the segment is
                // skipped and the consumer rolls back its siblings.
                let (mut source, pre_meta) = open_checked(&abs_path, &metadata)?;

                let mut worker_chunks =
                    Vec::with_capacity(estimate_chunk_count(len, chunker_config.avg_size));
                read_range_drift_checked(
                    &mut source,
                    &abs_path.to_string_lossy(),
                    &pre_meta,
                    ReadPlan::Segment { offset, len },
                    chunker_config,
                    read_limiter,
                    |data| {
                        let chunk_id = ChunkId::compute(chunk_hasher, &data);
                        worker_chunks
                            .push(classify_chunk(chunk_id, data, prefilter, compression, crypto)?);
                        Ok(())
                    },
                )?;

                let acquired_bytes = guard.defuse();
                Ok((pre_meta, worker_chunks, acquired_bytes))
            })();
            let abs_path: Arc<str> = Arc::from(abs_path.to_string_lossy());

            match work {
                Ok((pre_meta, chunks, acquired_bytes)) => Ok(ProcessedEntry::FileSegment {
                    item,
                    abs_path,
                    // Only segment 0's pre_meta is consumed downstream.
                    pre_meta: (segment_index == 0).then_some(pre_meta),
                    chunks,
                    acquired_bytes,
                    segment_index,
                    num_segments,
                }),
                Err(e) if e.is_soft_file_error() => {
                    let reason = super::super::skip_reason(&e);
                    warn!(
                        path = %abs_path,
                        segment_index,
                        error = %e,
                        "skipping segmented file in pipeline (soft error)"
                    );
                    Ok(ProcessedEntry::SegmentSkipped {
                        segment_index,
                        num_segments,
                        path: abs_path,
                        reason,
                    })
                }
                Err(e) => Err(e),
            }
        }

        // The walk thread converts these itself; kept for completeness.
        other => match into_processed_without_read(other) {
            Ok(processed) => Ok(processed),
            Err(_) => Err(VykarError::Other(
                "internal: file entry not handled by worker".into(),
            )),
        },
    }
}

/// Convert a walk entry that needs no file read (cache hit, non-file item,
/// walk-time skip, source marker) straight into its `ProcessedEntry`.
///
/// Returns the entry unchanged as `Err` for `File` / `FileSegment`, which
/// need a worker. Used by both the worker and the walk thread, which sends
/// these entries to the consumer in batches without a worker hop.
#[allow(clippy::result_large_err)] // `Err` hands the entry back unchanged; same size as `Ok`.
pub(super) fn into_processed_without_read(
    entry: WalkEntry,
) -> std::result::Result<ProcessedEntry, WalkEntry> {
    Ok(match entry {
        e @ (WalkEntry::File { .. } | WalkEntry::FileSegment { .. }) => return Err(e),

        WalkEntry::CacheHit {
            item,
            abs_path,
            metadata,
            cached_refs,
            retained,
        } => ProcessedEntry::CacheHit {
            item,
            abs_path,
            metadata,
            cached_refs,
            retained,
        },

        WalkEntry::NonFile { item } => ProcessedEntry::NonFile { item },

        WalkEntry::Skipped { path, reason } => ProcessedEntry::WalkSkip { path, reason },

        WalkEntry::SkippedDataless { path, kind } => ProcessedEntry::DatalessSkipped { path, kind },

        WalkEntry::SkippedUnsupported { path, file_type } => {
            ProcessedEntry::UnsupportedSkipped { path, file_type }
        }

        WalkEntry::SourceStarted { path } => ProcessedEntry::SourceStarted { path },

        WalkEntry::SourceFinished { path } => ProcessedEntry::SourceFinished { path },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
