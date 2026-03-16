mod chunk_process;
mod command_dump;
mod commit;
mod concurrency;
pub(crate) mod pipeline;
mod sequential;
mod walk;

pub(crate) use chunk_process::WorkerChunk;

use std::sync::atomic::AtomicBool;

use chrono::Utc;
use tracing::{debug, info, warn};

use super::util::check_interrupted;
use crate::compress::Compression;
use crate::config::{ChunkerConfig, CommandDump, VykarConfig};
use std::sync::Arc;

use crate::limits;
use crate::platform::fs;
use crate::repo::file_cache::{FileCache, ParentReuseBuilder, ParentReuseIndex};
use crate::repo::format::{pack_object_with_context, ObjectType};
use crate::repo::lock;
use crate::repo::manifest::SnapshotEntry;
use crate::repo::pack::PackType;
use crate::repo::Repository;
use crate::snapshot::item::Item;
use crate::snapshot::{SnapshotMeta, SnapshotStats};
use crate::storage;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::snapshot_id::SnapshotId;

use std::collections::HashSet;

use walk::items_chunker_config;

/// Normalize a relative path to always use `/` as the separator.
///
/// On Windows, `Path::to_string_lossy()` produces backslash-separated paths,
/// but the repository format uses `/` exclusively. This function is a no-op
/// on Unix (the compiler elides it), but always compiled so it can be tested
/// on any platform.
#[inline]
pub(crate) fn normalize_rel_path(path: String) -> String {
    if cfg!(windows) {
        path.replace('\\', "/")
    } else {
        path
    }
}

pub(crate) fn flush_item_stream_chunk(
    repo: &mut Repository,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    compression: Compression,
) -> Result<()> {
    if item_stream.is_empty() {
        return Ok(());
    }
    let chunk_data = std::mem::take(item_stream);
    let (chunk_id, _csize, _is_new) = repo.store_chunk(&chunk_data, compression, PackType::Tree)?;
    item_ptrs.push(chunk_id);
    Ok(())
}

pub(crate) fn append_item_to_stream(
    repo: &mut Repository,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    item: &Item,
    items_config: &ChunkerConfig,
    compression: Compression,
) -> Result<()> {
    rmp_serde::encode::write(item_stream, item)?;
    if item_stream.len() >= items_config.avg_size as usize {
        flush_item_stream_chunk(repo, item_stream, item_ptrs, compression)?;
    }
    Ok(())
}

/// Result of a backup run, containing stats and partial-success flag.
#[derive(Debug, Clone)]
pub struct BackupOutcome {
    pub stats: SnapshotStats,
    /// `true` when one or more files were skipped due to soft errors.
    pub is_partial: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileStatus {
    New,
    Modified,
    Unchanged,
}

#[derive(Debug, Clone)]
pub enum BackupProgressEvent {
    SourceStarted {
        source_path: String,
    },
    SourceFinished {
        source_path: String,
    },
    FileStarted {
        path: String,
    },
    StatsUpdated {
        nfiles: u64,
        original_size: u64,
        compressed_size: u64,
        deduplicated_size: u64,
        errors: u64,
        current_file: Option<String>,
    },
    FileProcessed {
        path: String,
        status: FileStatus,
        /// Bytes of new (deduplicated) data added for this file.
        added_bytes: u64,
    },
}

pub(crate) fn emit_progress(
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    event: BackupProgressEvent,
) {
    if let Some(callback) = progress.as_deref_mut() {
        callback(event);
    }
}

pub(crate) fn emit_stats_progress(
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    stats: &SnapshotStats,
    current_file: Option<String>,
) {
    emit_progress(
        progress,
        BackupProgressEvent::StatsUpdated {
            nfiles: stats.nfiles,
            original_size: stats.original_size,
            compressed_size: stats.compressed_size,
            deduplicated_size: stats.deduplicated_size,
            errors: stats.errors,
            current_file,
        },
    );
}

/// Run `vykar backup` for one or more source directories.
pub struct BackupRequest<'a> {
    pub snapshot_name: &'a str,
    pub passphrase: Option<&'a str>,
    pub source_paths: &'a [String],
    pub source_label: &'a str,
    pub exclude_patterns: &'a [String],
    pub exclude_if_present: &'a [String],
    pub one_file_system: bool,
    pub git_ignore: bool,
    pub xattrs_enabled: bool,
    pub compression: Compression,
    pub command_dumps: &'a [CommandDump],
    /// When true, emit `FileProcessed` events for per-file verbose output.
    pub verbose: bool,
}

pub fn run(config: &VykarConfig, req: BackupRequest<'_>) -> Result<BackupOutcome> {
    run_with_progress(config, req, None, None)
}

pub fn run_with_progress(
    config: &VykarConfig,
    req: BackupRequest<'_>,
    mut progress: Option<&mut dyn FnMut(BackupProgressEvent)>,
    shutdown: Option<&AtomicBool>,
) -> Result<BackupOutcome> {
    let snapshot_name = req.snapshot_name;
    let passphrase = req.passphrase;
    let source_paths = req.source_paths;
    let source_label = req.source_label;
    let exclude_patterns = req.exclude_patterns;
    let exclude_if_present = req.exclude_if_present;
    let one_file_system = req.one_file_system;
    let git_ignore = req.git_ignore;
    let xattrs_enabled = if req.xattrs_enabled && !fs::xattrs_supported() {
        warn!("xattrs requested but not supported on this platform; continuing without xattrs");
        false
    } else {
        req.xattrs_enabled
    };
    let compression = req.compression;
    let command_dumps = req.command_dumps;
    // Verbose per-file events only make sense when there is a callback to receive them.
    let verbose = req.verbose && progress.is_some();

    if source_paths.is_empty() && command_dumps.is_empty() {
        return Err(VykarError::Other(
            "no source paths or command dumps specified".into(),
        ));
    }
    if one_file_system && !cfg!(unix) {
        warn!("one_file_system filtering has limited support on this platform");
    }

    let multi_path = source_paths.len() > 1;

    // Build walk-root pairs: (original_basename, canonicalized_walk_root).
    // The walker uses the original basename for snapshot item prefixes, but
    // canonicalizes the root for abs_path construction (at least on Linux).
    // The parent reuse builder needs both to reconstruct matching abs_paths.
    let walk_roots: Vec<(String, String)> = source_paths
        .iter()
        .map(|sp| {
            let p = std::path::Path::new(sp);
            let basename = p
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| sp.clone());
            let walk_root = std::fs::canonicalize(p)
                .unwrap_or_else(|_| {
                    if p.is_absolute() {
                        p.to_path_buf()
                    } else {
                        std::env::current_dir().unwrap_or_default().join(p)
                    }
                })
                .to_string_lossy()
                .to_string();
            (basename, walk_root)
        })
        .collect();

    let _nice_guard = match limits::NiceGuard::apply(config.limits.nice) {
        Ok(guard) => guard,
        Err(e) => {
            warn!("could not apply limits.nice={}: {e}", config.limits.nice);
            None
        }
    };
    let max_pending_transform_bytes = config.limits.transform_batch_bytes();
    let max_pending_file_actions = config.limits.max_pending_actions();
    let upload_concurrency = config.limits.upload_concurrency();

    // Resolve effective worker count before building the rayon pool so we
    // can right-size it in pipeline mode (avoids 2× thread oversubscription).
    let num_workers = config.limits.effective_threads();

    // Pipeline mode when we have more than 1 worker thread.
    let use_pipeline = num_workers > 1;

    let transform_pool = if use_pipeline {
        // Pipeline mode doesn't need a rayon pool (no inline large-file processing).
        None
    } else {
        sequential::build_transform_pool(config.limits.threads)?
    };

    let backend = storage::backend_from_config(&config.repository, upload_concurrency)?;
    let backend = limits::wrap_storage_backend(backend, &config.limits);

    // Generate a unique session ID for this backup.
    let session_id = format!("{:032x}", rand::random::<u128>());

    // ── Phase 1: Register session and upload (no lock) ──────────────────

    // Register session marker and probe for maintenance lock.
    lock::register_session(backend.as_ref(), &session_id)?;
    debug!(session_id, "backup session registered");

    // Open repo after session registration (minimizes T0→commit window).
    // If open fails, deregister the session so it doesn't block maintenance for 72h.
    let mut repo = match Repository::open(
        backend,
        passphrase,
        super::util::cache_dir_from_config(config),
    ) {
        Ok(r) => {
            if let Err(e) = super::util::verify_repo_identity(config, &r) {
                if let Ok(cleanup) = storage::backend_from_config(&config.repository, 1) {
                    lock::deregister_session(cleanup.as_ref(), &session_id);
                }
                return Err(e);
            }
            r
        }
        Err(e) => {
            let e = super::util::enrich_repo_not_found(e, &config.repository.url);
            // Create a fresh backend just for deregistration (the original was consumed by open).
            if let Ok(cleanup_backend) = storage::backend_from_config(&config.repository, 1) {
                lock::deregister_session(cleanup_backend.as_ref(), &session_id);
            }
            return Err(e);
        }
    };

    // Wrap Phase 1 in a closure that deregisters the session on error.
    let phase1_result = (|| -> Result<(SnapshotEntry, Vec<u8>, FileCache, SnapshotStats)> {
        // Check snapshot name is unique (best-effort, re-checked at commit).
        if repo.manifest().find_snapshot(snapshot_name).is_some() {
            return Err(VykarError::SnapshotAlreadyExists(snapshot_name.into()));
        }

        // Activate write session with per-session pending_index.
        repo.begin_write_session()?;
        repo.set_write_session_id(session_id.clone());

        // Recover chunk→pack mappings from a previous interrupted session.
        match repo.recover_pending_index() {
            Ok(recovery) => {
                if recovery.recovered_chunks > 0 {
                    info!(
                        recovered_chunks = recovery.recovered_chunks,
                        sessions = recovery.recovered_sessions.len(),
                        "recovered pending index from interrupted session"
                    );
                }
            }
            Err(e) => {
                warn!("failed to recover pending index: {e}");
            }
        }

        // Invalidate file cache sections whose anchor snapshot no longer exists.
        {
            let snapshot_ids: HashSet<SnapshotId> =
                repo.manifest().snapshots.iter().map(|s| s.id).collect();
            let invalidated = repo
                .file_cache_mut()
                .invalidate_missing_snapshots(&|id| snapshot_ids.contains(id));
            if invalidated > 0 {
                info!(
                    invalidated,
                    "invalidated file cache sections for deleted snapshots"
                );
                repo.mark_file_cache_dirty();
            }
        }

        // Set up read cache + write cache sections for filesystem sources.
        let canonical_roots: Vec<String> =
            walk_roots.iter().map(|(_, root)| root.clone()).collect();
        let mut parent_reuse_index: Option<ParentReuseIndex> = None;
        if !source_paths.is_empty() {
            let section_valid = repo
                .file_cache_mut()
                .activate_for_walk_roots(&canonical_roots);

            if section_valid {
                info!(
                    source_label,
                    "file cache: section valid, using cached metadata"
                );
            } else if let Some(reason) = repo.file_cache().diagnose_sections(&canonical_roots) {
                info!(source_label, reason = %reason, "file cache: section invalid, cold start");
            }

            if !section_valid {
                // Cold start: build parent reuse index from latest matching
                // snapshot. Uses incremental builder inside the streaming
                // callback to avoid materializing Vec<Item>.
                let latest = repo
                    .manifest()
                    .snapshots
                    .iter()
                    .filter(|s| s.source_paths == source_paths)
                    .max_by_key(|s| s.time);
                if let Some(parent_entry) = latest {
                    let parent_name = parent_entry.name.clone();
                    let mut builder = ParentReuseBuilder::new(walk_roots.clone(), multi_path);
                    let stream_result =
                        super::list::for_each_snapshot_item(&mut repo, &parent_name, |item| {
                            builder.push(item);
                            Ok(())
                        });
                    match stream_result {
                        Ok(()) => {
                            parent_reuse_index = builder.finish();
                            if parent_reuse_index.is_some() {
                                info!(
                                    parent = parent_name,
                                    "built parent reuse index for cold-start fallback"
                                );
                            } else {
                                debug!(
                                    parent = parent_name,
                                    "parent snapshot lacks ctime on filesystem files, skipping parent fallback"
                                );
                            }
                        }
                        Err(e) => {
                            debug!(
                                parent = parent_name,
                                error = %e,
                                "failed to load parent snapshot for reuse index"
                            );
                        }
                    }
                }
            }
        }

        // Switch to tiered dedup mode to minimize memory during backup.
        repo.enable_tiered_dedup_mode();

        let dedup_filter = repo.dedup_filter();

        let time_start = Utc::now();
        let mut stats = SnapshotStats::default();
        let mut item_stream = Vec::new();
        let mut item_ptrs: Vec<ChunkId> = Vec::new();
        let items_config = items_chunker_config();
        let mut new_file_cache = FileCache::new();

        // Prepare write cache: create empty section for inserts.
        if !source_paths.is_empty() {
            new_file_cache.begin_sections(&canonical_roots);
        }

        // Execute command dumps before walking filesystem.
        command_dump::process_command_dumps(
            &mut repo,
            command_dumps,
            compression,
            &items_config,
            &mut item_stream,
            &mut item_ptrs,
            &mut stats,
            &mut progress,
            time_start,
        )?;

        // Apply configurable upload concurrency.
        repo.set_max_in_flight_uploads(upload_concurrency);

        let pipeline_depth = config.limits.effective_pipeline_depth();
        let pipeline_buffer_bytes = config.limits.pipeline_buffer_bytes();

        if use_pipeline && !source_paths.is_empty() {
            let file_cache_snapshot = repo.take_file_cache();
            let crypto = std::sync::Arc::clone(&repo.crypto);
            let segment_size = config
                .limits
                .segment_size_bytes()
                .min(pipeline_buffer_bytes) as u64;

            let pipeline_result = pipeline::run_parallel_pipeline(
                &mut repo,
                source_paths,
                multi_path,
                exclude_patterns,
                exclude_if_present,
                one_file_system,
                git_ignore,
                xattrs_enabled,
                &file_cache_snapshot,
                &crypto,
                compression,
                None, // no source-file read limiter
                num_workers,
                pipeline_depth,
                segment_size,
                &items_config,
                &mut item_stream,
                &mut item_ptrs,
                &mut stats,
                &mut new_file_cache,
                &mut progress,
                pipeline_buffer_bytes,
                dedup_filter.as_deref(),
                shutdown,
                verbose,
                parent_reuse_index.as_ref(),
            );
            // Always restore before propagating — keeps repo.file_cache valid
            // for commit-time merge.
            repo.restore_file_cache(file_cache_snapshot);
            pipeline_result?;
        } else {
            for source_path in source_paths {
                check_interrupted(shutdown)?;

                sequential::process_source_path(
                    &mut repo,
                    source_path,
                    multi_path,
                    exclude_patterns,
                    exclude_if_present,
                    one_file_system,
                    git_ignore,
                    xattrs_enabled,
                    compression,
                    &items_config,
                    &mut item_stream,
                    &mut item_ptrs,
                    &mut stats,
                    &mut new_file_cache,
                    max_pending_transform_bytes,
                    max_pending_file_actions,
                    None, // no source-file read limiter
                    transform_pool.as_ref(),
                    &mut progress,
                    dedup_filter.as_deref(),
                    shutdown,
                    verbose,
                    parent_reuse_index.as_ref(),
                )?;
            }
        }
        // Bail before committing if shutdown was requested during the walk.
        check_interrupted(shutdown)?;

        flush_item_stream_chunk(&mut repo, &mut item_stream, &mut item_ptrs, compression)?;

        let time_end = Utc::now();

        // Build snapshot metadata.
        let hostname = config
            .hostname_override
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .unwrap_or_else(crate::platform::short_hostname);
        let username = std::env::var("USER").unwrap_or_else(|_| "unknown".into());

        let snapshot_meta = SnapshotMeta {
            name: snapshot_name.to_string(),
            hostname: hostname.clone(),
            username,
            time: time_start,
            time_end,
            chunker_params: repo.config.chunker_params.clone(),
            comment: String::new(),
            item_ptrs,
            stats: stats.clone(),
            source_label: source_label.to_string(),
            source_paths: source_paths.to_vec(),
            label: String::new(),
        };

        // Generate snapshot ID and pack the blob (but DO NOT write to storage yet).
        // Writing snapshots/<id> is deferred to Phase 2 (commit barrier).
        let snapshot_id = SnapshotId::generate();

        // Finalize the write cache section with the snapshot ID.
        if !source_paths.is_empty() {
            new_file_cache.finalize_sections(snapshot_id);
        }
        let meta_bytes = rmp_serde::to_vec(&snapshot_meta)?;
        let snapshot_packed = pack_object_with_context(
            ObjectType::SnapshotMeta,
            snapshot_id.as_bytes(),
            &meta_bytes,
            repo.crypto.as_ref(),
        )?;

        let snapshot_entry = SnapshotEntry {
            name: snapshot_name.to_string(),
            id: snapshot_id,
            time: time_start,
            source_label: source_label.to_string(),
            label: String::new(),
            source_paths: source_paths.to_vec(),
            hostname,
        };

        Ok((snapshot_entry, snapshot_packed, new_file_cache, stats))
    })();

    // On Phase 1 error: flush packs, deregister.
    let (snapshot_entry, snapshot_packed, mut new_file_cache, stats) = match phase1_result {
        Ok(result) => result,
        Err(e) => {
            repo.flush_on_abort();
            // Do NOT save file cache on abort — the on-disk cache from the last
            // successful run is still valid. Saving here would persist the
            // depleted (invalidated) cache and destroy future cache hits.
            lock::deregister_session(repo.storage.as_ref(), &session_id);
            return Err(e);
        }
    };

    // ── Phase 2: Commit (exclusive lock) ────────────────────────────────
    //
    // new_file_cache is passed by &mut so the caller retains ownership.
    // On success, commit_concurrent_session consumes the active section
    // (merges it into the persistent cache).

    let commit_result = (|| -> Result<()> {
        let guard = lock::acquire_lock_with_retry(repo.storage.as_ref(), 10, 500)?;
        let fence = lock::build_lock_fence(&guard, Arc::clone(&repo.storage));
        repo.set_lock_fence(fence);

        let result =
            repo.commit_concurrent_session(snapshot_entry, snapshot_packed, &mut new_file_cache);

        // Deregister session while holding the lock.
        lock::deregister_session(repo.storage.as_ref(), &session_id);

        repo.clear_lock_fence();
        match lock::release_lock(repo.storage.as_ref(), guard) {
            Ok(()) => {}
            Err(release_err) => {
                warn!("failed to release repository lock: {release_err}");
                if result.is_ok() {
                    return Err(release_err);
                }
            }
        }

        result?;

        // Clean up the pending_index now that all entries are in the persisted
        // index. Best-effort — a stale pending_index is harmless.
        repo.clear_pending_index(&session_id);

        Ok(())
    })();

    if commit_result.is_err() {
        // flush_on_abort writes the remote sessions/<id>.index so the next
        // run's recover_pending_index() can discover our packs. Must run on
        // ALL Phase 2 failures (lock acquisition, commit, lock release).
        repo.flush_on_abort();
        lock::deregister_session(repo.storage.as_ref(), &session_id);
    }
    commit_result?;

    if stats.errors > 0 {
        info!(
            "Snapshot '{}' created: {} files, {} errors, {} original, {} compressed, {} deduplicated",
            snapshot_name,
            stats.nfiles,
            stats.errors,
            stats.original_size,
            stats.compressed_size,
            stats.deduplicated_size
        );
    } else {
        info!(
            "Snapshot '{}' created: {} files, {} original, {} compressed, {} deduplicated",
            snapshot_name,
            stats.nfiles,
            stats.original_size,
            stats.compressed_size,
            stats.deduplicated_size
        );
    }

    let is_partial = stats.errors > 0;
    Ok(BackupOutcome { stats, is_partial })
}
