//! The two phases of a backup run, driven by `run_against_repo`, and the session owner they share.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use chrono::Utc;
use tracing::{debug, info, warn};

use super::instrument::{BackupInstrumentation, PipelineTimings};
use super::{
    BackupProgressEvent, BackupRequest, DatalessTally, ItemStreamCutter, Prepared, Workers,
    command_dump, emit_post_commit_warning, emit_progress, flush_item_stream_chunk, pipeline,
    sequential, source, source_paths_match,
};
use crate::commands::util::check_interrupted;
use crate::config::VykarConfig;
use crate::repo::file_cache::{FileCache, ParentReuseBuilder, ParentReuseIndex, ParentReuseRoot};
use crate::repo::format::{ObjectType, pack_object_with_context};
use crate::repo::lock;
use crate::repo::manifest::SnapshotEntry;
use crate::repo::{Repository, WriteSessionOptions};
use crate::snapshot::{SnapshotMeta, SnapshotStats};
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::snapshot_id::SnapshotId;

/// The backup's registered session. Owns the heartbeat guard until the
/// commit point hands it to the cleanup thread; dropping this struct (or
/// `end()`) runs the guard's Drop: stop-thread → join-thread → deregister.
pub(super) struct BackupSession {
    id: String,
    guard: Option<lock::SessionGuard>,
}

impl BackupSession {
    /// Adopt the already-registered session `id` into a heartbeat guard.
    /// On failure the marker is deregistered before the error is returned.
    pub(super) fn adopt(storage: &Arc<dyn StorageBackend>, id: String) -> Result<Self> {
        match lock::SessionGuard::adopt(Arc::clone(storage), id.clone()) {
            Ok(g) => Ok(Self { id, guard: Some(g) }),
            Err(e) => {
                lock::deregister_session(storage.as_ref(), &id);
                Err(e)
            }
        }
    }

    /// Deregister now. A no-op once the guard was handed off or ended.
    fn end(&mut self) {
        drop(self.guard.take());
    }
}

/// What Phase 1 hands to Phase 2 and to the final report.
pub(super) struct Phase1Output {
    pub(super) snapshot_entry: SnapshotEntry,
    pub(super) snapshot_packed: Vec<u8>,
    pub(super) new_file_cache: FileCache,
    pub(super) stats: SnapshotStats,
    pub(super) instrumentation: BackupInstrumentation,
}

/// Phase 1: prepare the write session, run command dumps and the walk, and
/// upload every pack (no lock). Returns the packed snapshot for Phase 2.
#[allow(clippy::too_many_arguments)]
pub(super) fn phase1_upload(
    repo: &mut Repository,
    config: &VykarConfig,
    req: &BackupRequest<'_>,
    prepared: &Prepared<'_>,
    workers: &Workers,
    session_opts: &WriteSessionOptions,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    shutdown: Option<&AtomicBool>,
    dataless: &mut DatalessTally,
) -> Result<Phase1Output> {
    let phase1_start = Instant::now();
    let _span = tracing::debug_span!("backup_phase1").entered();
    let snapshot_name = req.snapshot_name;
    let source_paths = req.source_paths;
    let source_label = req.source_label;
    let exclude_if_present = req.exclude_if_present;
    let one_file_system = req.one_file_system;
    let git_ignore = req.git_ignore;
    let compression = req.compression;
    let command_dumps = req.command_dumps;
    // Verbose per-file events only make sense when there is a callback to receive them.
    let verbose = req.verbose && progress.is_some();
    let xattrs_enabled = prepared.xattrs_enabled;
    let exclude_patterns: &[String] = &prepared.exclude_patterns;
    let resolved_sources: &[source::ResolvedSource] = &prepared.resolved_sources;
    let max_pending_transform_bytes = prepared.max_pending_transform_bytes;
    let max_pending_file_actions = prepared.max_pending_file_actions;
    let num_workers = workers.num_workers;
    let use_pipeline = workers.use_pipeline;
    let transform_pool = &workers.transform_pool;

    // Existing repositories bypass YAML validation and may store odd
    // parameters from before FastCDC 5, or anything at all if `config` was
    // tampered with (it is unauthenticated). Check before any upload or dump
    // execution, while retaining the normal session cleanup on failure.
    // Do not reject these in Repository::open: restore must remain usable.
    repo.config.chunker_params.validate_stored().map_err(|e| {
        VykarError::Config(format!(
            "cannot back up with stored repository chunker parameters: {e}; \
             changing YAML does not change stored parameters. Existing snapshots \
             can still be restored; initialize a new repository with even \
             chunker sizes for future backups"
        ))
    })?;

    // Check snapshot name is unique (best-effort, re-checked at commit).
    if repo.manifest().find_snapshot(snapshot_name).is_some() {
        return Err(VykarError::SnapshotAlreadyExists(snapshot_name.into()));
    }

    // Activate the write session with the backup's options (per-session
    // `pending_index`, upload limits, staging), so command dumps already run
    // under the backup's upload limits.
    repo.begin_write_session(session_opts)?;

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
    let canonical_roots: Vec<String> = resolved_sources
        .iter()
        .map(|s| s.abs_source_str.clone())
        .collect();
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
                .filter(|s| source_paths_match(&s.source_paths, source_paths))
                .max_by_key(|s| s.time);
            if let Some(parent_entry) = latest {
                let parent_name = parent_entry.name.clone();
                let parent_roots: Vec<ParentReuseRoot> = resolved_sources
                    .iter()
                    .map(|s| s.parent_reuse_root())
                    .collect();
                let mut builder = ParentReuseBuilder::new(parent_roots);
                let stream_result = crate::commands::list::for_each_snapshot_item(
                    repo,
                    &parent_name,
                    shutdown,
                    |item| {
                        builder.push(item);
                        Ok(())
                    },
                );
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
            } else {
                info!(
                    source_label,
                    "no parent snapshot found for cold-start fallback"
                );
            }
        }
    }

    // Switch to tiered dedup mode to minimize memory during backup.
    repo.enable_tiered_dedup_mode();

    let prefilter = repo.dedup_prefilter();

    let time_start = Utc::now();
    let mut stats = SnapshotStats::default();
    let mut item_stream = Vec::new();
    let mut item_ptrs: Vec<ChunkId> = Vec::new();
    let item_cutter = ItemStreamCutter::new(repo.crypto.chunk_hasher());
    let mut new_file_cache = FileCache::new();

    // Prepare write cache: the loaded sections' sizes pre-size the
    // retained-hit lists (see `begin_sections`).
    if !source_paths.is_empty() {
        let capacity_hints: Vec<usize> = canonical_roots
            .iter()
            .map(|root| repo.file_cache().section_len(root))
            .collect();
        new_file_cache.begin_sections(&canonical_roots, &capacity_hints);
    }

    // Execute command dumps before walking filesystem.
    let walk_start = Instant::now();
    command_dump::process_command_dumps(
        repo,
        command_dumps,
        compression,
        &item_cutter,
        &mut item_stream,
        &mut item_ptrs,
        &mut stats,
        progress,
        time_start,
    )?;

    let pipeline_depth = config.limits.effective_pipeline_depth();
    let pipeline_buffer_bytes = config.limits.pipeline_buffer_for_workers(num_workers);

    let mut pipeline_timings: Option<PipelineTimings> = None;
    if use_pipeline && !source_paths.is_empty() {
        let file_cache_snapshot = repo.take_file_cache();
        let crypto = std::sync::Arc::clone(&repo.crypto);
        let segment_size = config
            .limits
            .segment_size_bytes()
            .min(pipeline_buffer_bytes) as u64;

        let pipeline_ctx = pipeline::PipelineCtx {
            sources: resolved_sources,
            exclude_patterns,
            exclude_if_present,
            one_file_system,
            git_ignore,
            xattrs_enabled,
            file_cache: &file_cache_snapshot,
            crypto: &crypto,
            compression,
            read_limiter: None,
            num_workers,
            readahead_depth: pipeline_depth,
            segment_size,
            item_cutter: &item_cutter,
            pipeline_buffer_bytes,
            prefilter: prefilter.as_ref(),
            shutdown,
            verbose,
            parent_reuse_index: parent_reuse_index.as_ref(),
        };
        let mut pipeline_bufs = pipeline::PipelineBuffers {
            item_stream: &mut item_stream,
            item_ptrs: &mut item_ptrs,
            stats: &mut stats,
            new_file_cache: &mut new_file_cache,
            dataless,
        };
        let pipeline_result =
            pipeline::run_parallel_pipeline(repo, &pipeline_ctx, &mut pipeline_bufs, progress);
        // Always restore before propagating — keeps repo.file_cache valid
        // for commit-time merge.
        repo.restore_file_cache(file_cache_snapshot);
        pipeline_timings = Some(pipeline_result?);
    } else {
        let seq_ctx = sequential::SequentialCtx {
            exclude_patterns,
            exclude_if_present,
            one_file_system,
            git_ignore,
            xattrs_enabled,
            compression,
            item_cutter: &item_cutter,
            max_pending_transform_bytes,
            max_pending_file_actions,
            read_limiter: None, // no source-file read limiter
            transform_pool: transform_pool.as_ref(),
            prefilter: prefilter.as_ref(),
            shutdown,
            verbose,
            parent_reuse_index: parent_reuse_index.as_ref(),
        };
        let mut seq_bufs = pipeline::PipelineBuffers {
            item_stream: &mut item_stream,
            item_ptrs: &mut item_ptrs,
            stats: &mut stats,
            new_file_cache: &mut new_file_cache,
            dataless,
        };
        for source in resolved_sources {
            check_interrupted(shutdown)?;
            sequential::process_source_path(repo, source, &seq_ctx, &mut seq_bufs, progress)?;
        }
    }
    let walk = walk_start.elapsed();
    // The walk was the last reader of the loaded cache's sections for
    // these roots. Move them into the write cache now: during the walk
    // the write cache recorded only the path hashes of exact hits, so
    // keeping their loaded entries (and dropping the rest) completes it
    // without a second full table. Doing it here instead of at commit
    // also means the old and new caches are never both resident while
    // the commit allocates its buffers. What is saved does not change
    // (see `absorb_read_sections`): `begin_sections` created a
    // write-cache section for every root, so `commit_finalize`
    // overwrites each moved key via `merge_section`, and a failed backup
    // never saves the file cache (see the Phase 1 abort path in `run_against_repo`).
    if !source_paths.is_empty() {
        let carried = new_file_cache.absorb_read_sections(repo.file_cache_mut());
        debug!(
            carried,
            "moved unchanged entries of the loaded file cache into the new one"
        );
    }

    // Bail before committing if shutdown was requested during the walk.
    check_interrupted(shutdown)?;

    flush_item_stream_chunk(repo, &mut item_stream, &mut item_ptrs, compression)?;

    // Seal partial packs and wait for every upload here, without the lock,
    // so Phase 2 holds it only for the index/snapshot writes. The final
    // journal write overlaps taking the lock; the commit's own flush
    // joins it (and retries it inline if it failed) before anything
    // else, and long before the journal is deleted.
    emit_progress(
        progress,
        BackupProgressEvent::CommitStage {
            stage: "upload packs",
        },
    );
    let flush_start = Instant::now();
    repo.flush_packs_journal_in_background()?;
    let final_flush = flush_start.elapsed();
    let instrumentation = BackupInstrumentation {
        threads: num_workers,
        phase1: phase1_start.elapsed(),
        walk,
        final_flush,
        pipeline: pipeline_timings,
        write: repo.write_metrics().unwrap_or_default(),
    };
    instrumentation.emit(&stats);
    check_interrupted(shutdown)?;

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
        ext: None,
        format_version: crate::snapshot::CURRENT_FORMAT_VERSION,
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

    Ok(Phase1Output {
        snapshot_entry,
        snapshot_packed,
        new_file_cache,
        stats,
        instrumentation,
    })
}

/// Phase 2: take the commit lock, commit the snapshot, release the lock.
/// Deregisters `session` on every path that reaches the commit (see the
/// comments inline); a lock-acquisition failure leaves it to the caller.
pub(super) fn phase2_commit(
    repo: &mut Repository,
    session: &mut BackupSession,
    snapshot_entry: SnapshotEntry,
    snapshot_packed: Vec<u8>,
    new_file_cache: &mut FileCache,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    shutdown: Option<&AtomicBool>,
) -> Result<()> {
    let guard = lock::acquire_lock_with_deadline(
        repo.storage.as_ref(),
        lock::COMMIT_LOCK_MAX_WAIT,
        500,
        shutdown,
        |_| {
            emit_progress(
                progress,
                BackupProgressEvent::CommitStage {
                    stage: "waiting for commit lock",
                },
            );
        },
    )?;
    let fence = lock::build_lock_fence(&guard, Arc::clone(&repo.storage));
    repo.set_lock_fence(fence);

    // Once the snapshot is committed, deregister the session and delete
    // its journal on a helper thread while the commit finishes its local
    // post-commit work (file cache save, cache rebuilds), instead of
    // after it. The lock is still held throughout and released only
    // after that thread is joined, below: deregistration happens under
    // the lock as before, and the local caches are still rebuilt under
    // it. The guard's Drop stops the heartbeat thread and joins it
    // before the marker is deleted, so no concurrent refresh_session
    // call can race with this delete. This sequencing is why the commit
    // path releases the lock itself instead of running under
    // `with_repo_lock`.
    let repo_storage = Arc::clone(&repo.storage);
    let mut cleanup: Option<std::thread::JoinHandle<()>> = None;
    let result = repo.commit_concurrent_session_with_progress(
        snapshot_entry,
        snapshot_packed,
        new_file_cache,
        progress,
        || {
            let guard = session.guard.take();
            let storage = Arc::clone(&repo_storage);
            let session_id = session.id.clone();
            let spawned = std::thread::Builder::new()
                .name("session-cleanup".into())
                .spawn(move || {
                    drop(guard);
                    // All entries are in the persisted index now. Best-effort —
                    // a stale journal is harmless.
                    crate::repo::session_journal::clear_pending_index(
                        storage.as_ref(),
                        &session_id,
                    );
                });
            match spawned {
                Ok(handle) => cleanup = Some(handle),
                Err(e) => warn!("failed to spawn session cleanup thread: {e}"),
            }
        },
    );
    let journal_cleared = cleanup.is_some();
    if let Some(handle) = cleanup.take() {
        let _ = handle.join();
    }
    // Not reached by the helper (failed commit, or no thread): the same
    // cleanup, inline.
    session.end();

    crate::commands::util::clear_fence_and_release(
        repo,
        guard,
        result.is_ok(),
        // Unlike `run_under_fence`, this path has a progress sink, so the
        // release warning reaches GUI/CLI consumers and not just the log.
        &mut |msg| emit_post_commit_warning(progress, msg),
    );

    result?;
    if !journal_cleared {
        repo.clear_pending_index(&session.id);
    }

    Ok(())
}
