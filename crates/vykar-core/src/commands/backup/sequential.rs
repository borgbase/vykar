use std::fs::File;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use rayon::prelude::*;
use tracing::{debug, warn};

use crate::commands::util::check_interrupted;

use crate::chunker;
use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::limits::{self, ByteRateLimiter};
use crate::platform::fs;
use crate::repo::file_cache::{FileCache, ParentReuseIndex};
use crate::repo::Repository;
use crate::snapshot::item::{Item, ItemType};
use crate::snapshot::SnapshotStats;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use super::chunk_process::{classify_chunk, WorkerChunk};
use super::commit::process_worker_chunks;
use super::source::ResolvedSource;
use super::walk::{is_soft_io_error, materialize_item, InodeSortedWalk, Materialized, WalkEvent};
use super::{append_item_to_stream, emit_progress, emit_stats_progress};
use super::{BackupProgressEvent, FileStatus};
use vykar_crypto::CryptoEngine;

/// Classify raw data chunks into `WorkerChunk`s, optionally using a rayon pool
/// for parallel compression/hashing.
fn classify_chunks(
    chunks: Vec<Vec<u8>>,
    chunk_id_key: &[u8; 32],
    dedup_filter: Option<&xorf::Xor8>,
    compression: Compression,
    crypto: &dyn CryptoEngine,
    transform_pool: Option<&rayon::ThreadPool>,
) -> Result<Vec<WorkerChunk>> {
    let classify = |data: Vec<u8>| -> Result<WorkerChunk> {
        let chunk_id = ChunkId::compute(chunk_id_key, &data);
        classify_chunk(chunk_id, data, dedup_filter, compression, crypto)
    };

    let results: Vec<Result<WorkerChunk>> = if let Some(pool) = transform_pool {
        pool.install(|| chunks.into_par_iter().map(classify).collect())
    } else {
        chunks.into_iter().map(classify).collect()
    };

    results.into_iter().collect()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn flush_regular_file_batch(
    repo: &mut Repository,
    compression: Compression,
    chunk_id_key: &[u8; 32],
    transform_pool: Option<&rayon::ThreadPool>,
    raw_chunks: &mut Vec<Vec<u8>>,
    item: &mut Item,
    stats: &mut SnapshotStats,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<()> {
    if raw_chunks.is_empty() {
        return Ok(());
    }

    let taken = std::mem::take(raw_chunks);
    let worker_chunks = classify_chunks(
        taken,
        chunk_id_key,
        dedup_filter,
        compression,
        repo.crypto.as_ref(),
        transform_pool,
    )?;
    process_worker_chunks(repo, item, worker_chunks, stats, compression, dedup_filter)
}

/// Tracks a small file pending in the cross-file batch.
struct PendingBatchFile {
    item: Item,
    metadata_summary: fs::MetadataSummary,
    abs_path: String,
    chunk_count: usize,
}

/// Accumulates chunks from many small files for a single rayon dispatch.
struct CrossFileBatch {
    files: Vec<PendingBatchFile>,
    raw_chunks: Vec<Vec<u8>>,
    pending_bytes: usize,
}

/// Flush threshold: 32 MiB or 8192 chunks.
pub(super) const CROSS_BATCH_MAX_BYTES: usize = 32 * 1024 * 1024;
pub(super) const CROSS_BATCH_MAX_CHUNKS: usize = 8192;

impl CrossFileBatch {
    fn new() -> Self {
        Self {
            files: Vec::new(),
            raw_chunks: Vec::new(),
            pending_bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    fn should_flush(&self) -> bool {
        self.pending_bytes >= CROSS_BATCH_MAX_BYTES
            || self.raw_chunks.len() >= CROSS_BATCH_MAX_CHUNKS
    }

    fn add_file(
        &mut self,
        item: Item,
        data: Vec<u8>,
        metadata_summary: fs::MetadataSummary,
        abs_path: String,
    ) {
        self.pending_bytes += data.len();
        self.raw_chunks.push(data);
        self.files.push(PendingBatchFile {
            item,
            metadata_summary,
            abs_path,
            chunk_count: 1,
        });
    }
}

/// Flush all accumulated small-file chunks in a single rayon dispatch, then
/// distribute results back to their owning files in walk order.
#[allow(clippy::too_many_arguments)]
fn flush_cross_file_batch(
    batch: &mut CrossFileBatch,
    repo: &mut Repository,
    compression: Compression,
    chunk_id_key: &[u8; 32],
    transform_pool: Option<&rayon::ThreadPool>,
    items_config: &ChunkerConfig,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    dedup_filter: Option<&xorf::Xor8>,
    verbose: bool,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let taken = std::mem::take(&mut batch.raw_chunks);
    let mut worker_chunks = classify_chunks(
        taken,
        chunk_id_key,
        dedup_filter,
        compression,
        repo.crypto.as_ref(),
        transform_pool,
    )?;

    for file in batch.files.drain(..) {
        if let Some(cb) = progress.as_deref_mut() {
            cb(BackupProgressEvent::FileStarted {
                path: file.item.path.clone(),
            });
        }

        let mut item = file.item;

        let dedup_before = if verbose { stats.deduplicated_size } else { 0 };

        process_worker_chunks(
            repo,
            &mut item,
            worker_chunks.drain(..file.chunk_count),
            stats,
            compression,
            dedup_filter,
        )?;

        stats.nfiles += 1;

        if verbose {
            let added_bytes = stats.deduplicated_size - dedup_before;
            let status = if repo.file_cache().contains(&file.abs_path) {
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
            &file.abs_path,
            file.metadata_summary.device,
            file.metadata_summary.inode,
            file.metadata_summary.mtime_ns,
            file.metadata_summary.ctime_ns,
            file.metadata_summary.size,
            Arc::new(std::mem::take(&mut item.chunks)),
        );

        emit_stats_progress(progress, stats, Some(std::mem::take(&mut item.path)));
    }

    batch.pending_bytes = 0;

    Ok(())
}

pub(super) fn build_transform_pool(max_threads: usize) -> Result<Option<rayon::ThreadPool>> {
    // max_threads == 1 means explicitly sequential (no pool).
    if max_threads == 1 {
        return Ok(None);
    }

    // max_threads == 0 means use all available cores (rayon default).
    let mut builder = rayon::ThreadPoolBuilder::new();
    if max_threads > 1 {
        builder = builder.num_threads(max_threads);
    }

    builder
        .build()
        .map(Some)
        .map_err(|e| VykarError::Other(format!("failed to create rayon thread pool: {e}")))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn process_regular_file_item(
    repo: &mut Repository,
    entry_path: &Path,
    metadata_summary: fs::MetadataSummary,
    compression: Compression,
    transform_pool: Option<&rayon::ThreadPool>,
    read_limiter: Option<&ByteRateLimiter>,
    item: &mut Item,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    max_pending_transform_bytes: usize,
    max_pending_file_actions: usize,
    dedup_filter: Option<&xorf::Xor8>,
    verbose: bool,
    parent_reuse_index: Option<&ParentReuseIndex>,
) -> Result<()> {
    if let Some(cb) = progress.as_deref_mut() {
        cb(BackupProgressEvent::FileStarted {
            path: item.path.clone(),
        });
    }

    // File-level cache: skip read/chunk/compress/encrypt for unchanged files.
    let abs_path = entry_path.to_string_lossy().to_string();
    let file_size = metadata_summary.size;

    let effective_hit = super::resolve_cache_hit(
        repo.file_cache(),
        parent_reuse_index,
        &abs_path,
        &metadata_summary,
    );

    if let Some(cached_refs) = effective_hit {
        super::commit::commit_cache_hit(repo, item, &cached_refs, stats)?;

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

        new_file_cache.insert(
            &abs_path,
            metadata_summary.device,
            metadata_summary.inode,
            metadata_summary.mtime_ns,
            metadata_summary.ctime_ns,
            file_size,
            cached_refs,
        );

        debug!(path = %item.path, "file cache hit");
        emit_stats_progress(progress, stats, Some(item.path.clone()));
        return Ok(());
    }

    let chunk_id_key = *repo.crypto.chunk_id_key();
    // Check old cache for new-vs-modified before opening file (avoids borrow conflict).
    let was_in_old_cache = if verbose {
        repo.file_cache().contains(&abs_path)
    } else {
        false
    };
    let dedup_before = if verbose { stats.deduplicated_size } else { 0 };

    let file = File::open(entry_path).map_err(VykarError::Io)?;
    let chunk_stream = chunker::chunk_stream(
        limits::LimitedReader::new(file, read_limiter),
        &repo.config.chunker_params,
    );

    let mut raw_chunks: Vec<Vec<u8>> = Vec::new();
    let mut pending_bytes: usize = 0;

    for chunk_result in chunk_stream {
        let chunk = chunk_result.map_err(|e| match e {
            fastcdc::v2020::Error::IoError(ioe) => VykarError::Io(ioe),
            other => VykarError::Other(format!(
                "chunking failed for {}: {other}",
                entry_path.display()
            )),
        })?;

        let data_len = chunk.data.len();
        pending_bytes = pending_bytes.saturating_add(data_len);
        raw_chunks.push(chunk.data);

        if pending_bytes >= max_pending_transform_bytes
            || raw_chunks.len() >= max_pending_file_actions
        {
            flush_regular_file_batch(
                repo,
                compression,
                &chunk_id_key,
                transform_pool,
                &mut raw_chunks,
                item,
                stats,
                dedup_filter,
            )?;
            emit_stats_progress(progress, stats, None);
            pending_bytes = 0;
        }
    }

    flush_regular_file_batch(
        repo,
        compression,
        &chunk_id_key,
        transform_pool,
        &mut raw_chunks,
        item,
        stats,
        dedup_filter,
    )?;

    stats.nfiles += 1;

    if verbose {
        let added_bytes = stats.deduplicated_size - dedup_before;
        let status = if was_in_old_cache {
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

    // Update file cache with the chunks we just stored.
    new_file_cache.insert(
        &abs_path,
        metadata_summary.device,
        metadata_summary.inode,
        metadata_summary.mtime_ns,
        metadata_summary.ctime_ns,
        file_size,
        Arc::new(item.chunks.clone()),
    );

    emit_stats_progress(progress, stats, Some(item.path.clone()));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn process_source_path(
    repo: &mut Repository,
    source: &ResolvedSource,
    exclude_patterns: &[String],
    exclude_if_present: &[String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    compression: Compression,
    items_config: &ChunkerConfig,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    max_pending_transform_bytes: usize,
    max_pending_file_actions: usize,
    read_limiter: Option<&ByteRateLimiter>,
    transform_pool: Option<&rayon::ThreadPool>,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    dedup_filter: Option<&xorf::Xor8>,
    shutdown: Option<&AtomicBool>,
    verbose: bool,
    parent_reuse_index: Option<&ParentReuseIndex>,
) -> Result<()> {
    emit_progress(
        progress,
        BackupProgressEvent::SourceStarted {
            source_path: source.configured.clone(),
        },
    );

    let chunk_id_key = *repo.crypto.chunk_id_key();
    let min_chunk_size = repo.config.chunker_params.min_size as u64;
    let mut cross_batch = CrossFileBatch::new();

    let inode_walk = InodeSortedWalk::new(
        source,
        exclude_patterns,
        exclude_if_present,
        one_file_system,
        git_ignore,
    )?;

    for event_result in inode_walk {
        check_interrupted(shutdown)?;

        let walked = match event_result {
            Ok(WalkEvent::Entry(walked)) => walked,
            Ok(WalkEvent::Skipped) => {
                stats.errors += 1;
                continue;
            }
            Err(e) => return Err(e),
        };

        let (mut item, entry_path, metadata_summary) =
            match materialize_item(walked, xattrs_enabled) {
                Ok(Materialized::Entry {
                    item,
                    abs_path,
                    metadata,
                }) => (item, abs_path, metadata),
                Ok(Materialized::SoftError) => {
                    stats.errors += 1;
                    continue;
                }
                Ok(Materialized::Unsupported) => continue,
                Err(e) => return Err(e),
            };

        // For regular files, chunk and store the content.
        if item.entry_type == ItemType::RegularFile && metadata_summary.size > 0 {
            // Small-file fast path: read directly, accumulate in cross-file batch.
            if metadata_summary.size < min_chunk_size {
                // Flush batch before cache-hit check since it may need walk-order items.
                let abs_path = entry_path.to_string_lossy().to_string();

                let effective_hit = super::resolve_cache_hit(
                    repo.file_cache(),
                    parent_reuse_index,
                    &abs_path,
                    &metadata_summary,
                );

                if let Some(cached_refs) = effective_hit {
                    // Flush batch to preserve walk order before the cache-hit item.
                    flush_cross_file_batch(
                        &mut cross_batch,
                        repo,
                        compression,
                        &chunk_id_key,
                        transform_pool,
                        items_config,
                        item_stream,
                        item_ptrs,
                        stats,
                        new_file_cache,
                        progress,
                        dedup_filter,
                        verbose,
                    )?;

                    if let Some(cb) = progress.as_deref_mut() {
                        cb(BackupProgressEvent::FileStarted {
                            path: item.path.clone(),
                        });
                    }

                    super::commit::commit_cache_hit(repo, &mut item, &cached_refs, stats)?;

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

                    new_file_cache.insert(
                        &abs_path,
                        metadata_summary.device,
                        metadata_summary.inode,
                        metadata_summary.mtime_ns,
                        metadata_summary.ctime_ns,
                        metadata_summary.size,
                        cached_refs,
                    );

                    debug!(path = %item.path, "file cache hit (sequential small)");
                    emit_stats_progress(progress, stats, Some(item.path.clone()));
                } else {
                    // Cache miss — read and add to batch.
                    let data = match std::fs::read(&entry_path) {
                        Ok(d) => d,
                        Err(e) => {
                            if is_soft_io_error(&e) {
                                warn!(path = %entry_path.display(), error = %e, "skipping file (read error)");
                                stats.errors += 1;
                                continue;
                            }
                            return Err(VykarError::Io(e));
                        }
                    };
                    cross_batch.add_file(item, data, metadata_summary, abs_path);

                    if cross_batch.should_flush() {
                        flush_cross_file_batch(
                            &mut cross_batch,
                            repo,
                            compression,
                            &chunk_id_key,
                            transform_pool,
                            items_config,
                            item_stream,
                            item_ptrs,
                            stats,
                            new_file_cache,
                            progress,
                            dedup_filter,
                            verbose,
                        )?;
                    }
                    continue; // item will be appended by flush
                }
            } else {
                // Large file — flush batch first to maintain walk order.
                flush_cross_file_batch(
                    &mut cross_batch,
                    repo,
                    compression,
                    &chunk_id_key,
                    transform_pool,
                    items_config,
                    item_stream,
                    item_ptrs,
                    stats,
                    new_file_cache,
                    progress,
                    dedup_filter,
                    verbose,
                )?;

                if let Err(e) = process_regular_file_item(
                    repo,
                    &entry_path,
                    metadata_summary,
                    compression,
                    transform_pool,
                    read_limiter,
                    &mut item,
                    stats,
                    new_file_cache,
                    progress,
                    max_pending_transform_bytes,
                    max_pending_file_actions,
                    dedup_filter,
                    verbose,
                    parent_reuse_index,
                ) {
                    if e.is_soft_file_error() {
                        warn!(path = %entry_path.display(), error = %e, "skipping file");
                        stats.errors += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        } else {
            // Non-regular-file — flush batch first to maintain walk order.
            flush_cross_file_batch(
                &mut cross_batch,
                repo,
                compression,
                &chunk_id_key,
                transform_pool,
                items_config,
                item_stream,
                item_ptrs,
                stats,
                new_file_cache,
                progress,
                dedup_filter,
                verbose,
            )?;
        }

        // Stream item metadata to avoid materializing a full Vec<Item>.
        append_item_to_stream(
            repo,
            item_stream,
            item_ptrs,
            &item,
            items_config,
            compression,
        )?;
    }

    // Flush any remaining small files in the cross-file batch.
    flush_cross_file_batch(
        &mut cross_batch,
        repo,
        compression,
        &chunk_id_key,
        transform_pool,
        items_config,
        item_stream,
        item_ptrs,
        stats,
        new_file_cache,
        progress,
        dedup_filter,
        verbose,
    )?;

    emit_progress(
        progress,
        BackupProgressEvent::SourceFinished {
            source_path: source.configured.clone(),
        },
    );

    Ok(())
}
