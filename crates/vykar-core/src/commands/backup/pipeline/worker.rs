//! Per-entry worker stage: chunking, hashing, classification, encryption.
//!
//! `process_file_worker` is invoked from worker threads for each `WalkEntry`
//! the walker emits. It opens the file, runs FastCDC chunking (or a single
//! whole-file read for sub-min-chunk files), hashes each chunk, classifies
//! it via the dedup filter, and packages the result as a `ProcessedEntry`
//! for the consumer. Soft I/O / drift errors are converted into
//! `Skipped` / `SegmentSkipped` variants here so the consumer can count
//! them without aborting the whole backup.

use std::io::Read;
use std::path::Path;

use tracing::warn;

use crate::chunker;
use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::limits::{self, ByteRateLimiter};
use crate::platform::fs;
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use super::super::chunk_process::classify_chunk;
use super::super::concurrency::{BudgetGuard, ByteBudget};
use super::super::read_source::BackupSource;
use super::super::walk::WalkEntry;
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
            // Borrow `abs_path` inside the closure — keeping it owned by the
            // outer scope means we can move it into either the success
            // (`ProcessedFile`) or skip (`Skipped`) variant without a clone.
            let work = (|| -> Result<(fs::MetadataSummary, Vec<super::super::chunk_process::WorkerChunk>, usize)> {
                // Budget was pre-acquired by the walk thread. Wrap in a guard for
                // error safety — if we `?`-bail, the guard drops and releases bytes.
                let guard = BudgetGuard::from_pre_acquired(budget, pre_acquired_bytes);

                let mut source = BackupSource::open(Path::new(&abs_path)).map_err(VykarError::Io)?;
                let pre_meta = fs::fstat_summary(source.file()).map_err(VykarError::Io)?;

                // Walk-to-open drift check — catches pre-open mutation and
                // rename-atop (device+inode differ).
                if !fs::metadata_matches(&pre_meta, &metadata) {
                    return Err(VykarError::FileChangedDuringRead {
                        path: abs_path.clone(),
                        dataless: pre_meta.is_dataless,
                    });
                }

                // Small file (< min_chunk_size): read whole, single chunk.
                if pre_meta.size < chunker_config.min_size as u64 {
                    // On 32-bit hosts a `u64 -> usize` cast would silently truncate
                    // a multi-GiB file's pre-allocation; refuse upfront.
                    let cap = usize::try_from(pre_meta.size).map_err(|_| {
                        VykarError::Other(format!(
                            "file {abs_path} too large for this platform: {} bytes",
                            pre_meta.size,
                        ))
                    })?;
                    let mut data = Vec::with_capacity(cap);
                    // Hard-cap at pre_meta.size + 1 so an intra-read append can't
                    // grow `data` past budget; the +1 sentinel trips the post-read
                    // `data.len() != pre_meta.size` drift check below.
                    let mut reader = (&mut source).take(pre_meta.size + 1);
                    if let Some(limiter) = read_limiter {
                        limits::LimitedReader::new(reader, Some(limiter))
                            .read_to_end(&mut data)
                            .map_err(VykarError::Io)?;
                    } else {
                        reader.read_to_end(&mut data).map_err(VykarError::Io)?;
                    }

                    let post_meta = fs::fstat_summary(source.file()).map_err(VykarError::Io)?;
                    if !fs::metadata_matches(&pre_meta, &post_meta)
                        || data.len() as u64 != pre_meta.size
                    {
                        return Err(VykarError::FileChangedDuringRead {
                            path: abs_path.clone(),
                            dataless: post_meta.is_dataless,
                        });
                    }

                    let chunk_id = ChunkId::compute(chunk_id_key, &data);
                    let worker_chunk =
                        classify_chunk(chunk_id, data, dedup_filter, compression, crypto)?;

                    let acquired_bytes = guard.defuse();
                    return Ok((pre_meta, vec![worker_chunk], acquired_bytes));
                }

                // Medium file: read, chunk via FastCDC, then hash → classify each chunk.
                let mut total_bytes: u64 = 0;
                let mut worker_chunks = Vec::with_capacity(estimate_chunk_count(
                    pre_meta.size,
                    chunker_config.avg_size,
                ));
                {
                    // Hard-cap at pre_meta.size + 1 so an intra-read append can't
                    // feed unbounded bytes through the chunker/classifier; the +1
                    // sentinel trips the post-read `total_bytes != pre_meta.size`
                    // drift check below.
                    let reader = (&mut source).take(pre_meta.size + 1);
                    let chunk_stream = chunker::chunk_stream(
                        limits::LimitedReader::new(reader, read_limiter),
                        chunker_config,
                    );

                    for chunk_result in chunk_stream {
                        let chunk = chunk_result.map_err(|e| match e {
                            fastcdc::v2020::Error::IoError(ioe) => VykarError::Io(ioe),
                            other => VykarError::Other(format!(
                                "chunking failed for {abs_path}: {other}"
                            )),
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
                        dataless: post_meta.is_dataless,
                    });
                }

                let acquired_bytes = guard.defuse();
                Ok((pre_meta, worker_chunks, acquired_bytes))
            })();

            match work {
                Ok((pre_meta, chunks, acquired_bytes)) => Ok(ProcessedEntry::ProcessedFile {
                    item,
                    abs_path,
                    pre_meta,
                    chunks,
                    acquired_bytes,
                }),
                Err(e) if e.is_soft_file_error() => {
                    let reason = e.to_string();
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

                let mut source =
                    BackupSource::open(Path::new(&*abs_path)).map_err(VykarError::Io)?;
                let pre_meta = fs::fstat_summary(source.file()).map_err(VykarError::Io)?;

                // Walk-to-open drift check. Segmented reads are a plan driven
                // by walk-time size (`num_segments`/`offset`/`len`). Any drift
                // invalidates the plan — skip the segment.
                if !fs::metadata_matches(&pre_meta, &metadata) {
                    return Err(VykarError::FileChangedDuringRead {
                        path: abs_path.to_string(),
                        dataless: pre_meta.is_dataless,
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
                            other => VykarError::Other(format!(
                                "chunking failed for {abs_path}: {other}"
                            )),
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
                        dataless: post_meta.is_dataless,
                    });
                }

                let acquired_bytes = guard.defuse();
                Ok((pre_meta, worker_chunks, acquired_bytes))
            })();

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
                    let reason = e.to_string();
                    warn!(
                        path = %abs_path,
                        segment_index,
                        error = %e,
                        "skipping segmented file in pipeline (soft error)"
                    );
                    // abs_path is `Arc<str>`; cloning it is a refcount bump.
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

        WalkEntry::Skipped { path, reason } => Ok(ProcessedEntry::WalkSkip { path, reason }),

        WalkEntry::SkippedDataless { path } => Ok(ProcessedEntry::DatalessSkipped { path }),

        WalkEntry::SourceStarted { path } => Ok(ProcessedEntry::SourceStarted { path }),

        WalkEntry::SourceFinished { path } => Ok(ProcessedEntry::SourceFinished { path }),
    }
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
