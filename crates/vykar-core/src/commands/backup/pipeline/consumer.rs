//! Per-entry commit handler.
//!
//! Called from the orchestrator's reorder/drain loop ([`super`]) once a
//! `ProcessedEntry` has been restored to walk-order. For each entry this
//! commits pack-bound chunks via `process_worker_chunks`, advances the
//! segmented-file state machine (delegating to [`super::segmentation`]),
//! and hands each finished file to the per-file epilogue
//! (`commit::finish_file`), which updates stats / file-cache, appends the
//! entry to the listing and surfaces progress events.
//! Releases budget bytes once chunks are committed so the walker can
//! proceed. Skip handling lives in the orchestrator, not here.

use crate::index::table::DedupPrefilter;
use crate::repo::file_cache::FileCache;
use crate::repo::{Repo, WriteSession};
use crate::snapshot::SnapshotStats;
use vykar_types::error::{Result, VykarError};

use super::super::commit::{
    FileCaches, FileKind, FileOutcome, commit_cache_hit, finish_file, process_worker_chunks,
    start_file,
};
use super::super::concurrency::ByteBudget;
use super::super::{BackupProgressEvent, ListingWriter, emit_progress, emit_stats_progress};
use super::ProcessedEntry;
use super::segmentation::{LargeFileAccum, validate_segment_accum};

/// Consume a single processed entry: dedup check, pack commit, listing, file cache.
#[allow(clippy::too_many_arguments)]
pub(super) fn consume_processed_entry(
    entry: ProcessedEntry,
    repo: &Repo,
    ws: &mut WriteSession,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    listing: &mut ListingWriter,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    budget: &ByteBudget,
    prefilter: Option<&DedupPrefilter>,
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
            let dedup_before = start_file(progress, stats, &item.path);

            process_worker_chunks(repo, ws, &mut item, chunks, stats, prefilter)?;

            // Release budget bytes now that chunks are committed.
            budget.release(acquired_bytes);

            finish_file(
                repo,
                ws,
                stats,
                listing,
                progress,
                verbose,
                FileOutcome {
                    item,
                    dedup_before,
                    kind: FileKind::Read {
                        abs_path: &abs_path,
                        pre_meta,
                        caches: FileCaches {
                            old: old_file_cache,
                            new: new_file_cache,
                        },
                    },
                },
            )?;
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
            // Segment 0 announces the file before validation consumes `item`.
            let dedup_before = match (segment_index, item.as_ref()) {
                (0, Some(it)) => start_file(progress, stats, &it.path),
                _ => 0,
            };

            validate_segment_accum(
                large_file_accum,
                item,
                abs_path,
                pre_meta,
                segment_index,
                num_segments,
                dedup_before,
                ws,
                stats,
            )?;

            // Process chunks via shared helper.
            let accum = large_file_accum.as_mut().ok_or_else(|| {
                VykarError::Other("BUG: large_file_accum missing after segment validation".into())
            })?;
            process_worker_chunks(repo, ws, &mut accum.item, chunks, stats, prefilter)?;
            budget.release(acquired_bytes);

            if segment_index < num_segments - 1 {
                emit_stats_progress(progress, stats, None);
            }

            if segment_index == num_segments - 1 {
                // Last segment: finalize.
                let LargeFileAccum {
                    item,
                    abs_path,
                    metadata,
                    txn,
                    ..
                } = large_file_accum.take().ok_or_else(|| {
                    VykarError::Other(
                        "BUG: large_file_accum missing at segment finalization".into(),
                    )
                })?;

                // All segments landed: keep the file's staged reuse.
                let dedup_before = txn.commit(ws);

                // Segment 0's pre_meta is canonical: it matches the chunks
                // committed across all segments.
                finish_file(
                    repo,
                    ws,
                    stats,
                    listing,
                    progress,
                    verbose,
                    FileOutcome {
                        item,
                        dedup_before,
                        kind: FileKind::Read {
                            abs_path: &abs_path,
                            pre_meta: metadata,
                            caches: FileCaches {
                                old: old_file_cache,
                                new: new_file_cache,
                            },
                        },
                    },
                )?;
            }
        }

        ProcessedEntry::CacheHit {
            mut item,
            abs_path,
            metadata,
            cached_refs,
            retained,
        } => {
            let dedup_before = start_file(progress, stats, &item.path);

            commit_cache_hit(repo, ws, &mut item, &cached_refs, stats)?;

            finish_file(
                repo,
                ws,
                stats,
                listing,
                progress,
                verbose,
                FileOutcome {
                    item,
                    dedup_before,
                    kind: FileKind::CacheHit {
                        abs_path: &abs_path,
                        metadata,
                        cached_refs,
                        retained,
                        caches: FileCaches {
                            old: old_file_cache,
                            new: new_file_cache,
                        },
                    },
                },
            )?;
        }

        ProcessedEntry::NonFile { item } => {
            listing.push(repo, ws, &item)?;
        }

        ProcessedEntry::SourceStarted { path, root } => {
            listing.begin_segment(repo, ws, root)?;
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
        | ProcessedEntry::WalkSkip { .. }
        | ProcessedEntry::DatalessSkipped { .. }
        | ProcessedEntry::UnsupportedSkipped { .. } => {
            return Err(VykarError::Other(
                "internal error: skipped backup entry reached consume_processed_entry".into(),
            ));
        }
    }

    Ok(())
}
