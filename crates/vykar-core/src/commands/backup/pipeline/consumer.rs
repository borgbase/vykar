//! Per-entry commit handler.
//!
//! Called from the orchestrator's reorder/drain loop ([`super`]) once a
//! `ProcessedEntry` has been restored to walk-order. For each entry this
//! commits pack-bound chunks via `process_worker_chunks`, advances the
//! segmented-file state machine (delegating to [`super::segmentation`]),
//! updates stats / file-cache / item stream, and surfaces progress events.
//! Releases budget bytes once chunks are committed so the walker can
//! proceed. Skip / rollback handling lives in the orchestrator, not here.

use tracing::debug;

use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::repo::file_cache::{CachedChunks, FileCache};
use crate::repo::Repository;
use crate::snapshot::SnapshotStats;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use super::super::commit::{commit_cache_hit, process_worker_chunks};
use super::super::concurrency::ByteBudget;
use super::super::{
    append_item_to_stream, emit_progress, emit_stats_progress, BackupProgressEvent, FileStatus,
};
use super::segmentation::{validate_segment_accum, LargeFileAccum};
use super::ProcessedEntry;

/// Consume a single processed entry: dedup check, pack commit, item stream, file cache.
#[allow(clippy::too_many_arguments)]
pub(super) fn consume_processed_entry(
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
                emit_progress(
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
                    emit_progress(
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

            commit_cache_hit(repo, &mut item, &cached_refs, stats)?;

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
            emit_progress(
                progress,
                BackupProgressEvent::SourceStarted { source_path: path },
            );
        }

        ProcessedEntry::SourceFinished { path } => {
            emit_progress(
                progress,
                BackupProgressEvent::SourceFinished { source_path: path },
            );
        }

        // Skipped entries are handled in the consumer loop before reaching here.
        ProcessedEntry::Skipped { .. }
        | ProcessedEntry::SegmentSkipped { .. }
        | ProcessedEntry::WalkSkip
        | ProcessedEntry::DatalessSkipped { .. } => {
            // Not reached: the orchestrator handles these variants before
            // calling into `consume_processed_entry`.
            unreachable!(
                "Skipped/SegmentSkipped/WalkSkip/DatalessSkipped should be handled before consume_processed_entry"
            );
        }
    }

    Ok(())
}
