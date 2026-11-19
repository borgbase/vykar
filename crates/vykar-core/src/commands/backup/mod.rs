mod chunk_process;
mod command_dump;
mod commit;
mod concurrency;
mod drift;
mod item_cut;
mod phases;
pub(crate) mod pipeline;
mod sequential;
mod source;
mod walk;

pub(crate) mod read_source;

pub(crate) use chunk_process::WorkerChunk;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::compress::Compression;
use crate::config::{CommandDump, ResourceLimitsConfig, VykarConfig};

use crate::limits;
use crate::platform::fs;
use crate::repo::file_cache::{CachedChunks, FileCache, ParentReuseIndex, RetainedHit};
use crate::repo::lock;
use crate::repo::pack::PackType;
use crate::repo::{Repository, WriteSessionOptions};
use crate::snapshot::item::Item;
use crate::snapshot::SnapshotStats;
use crate::storage;
use vykar_common::display::{format_bytes, format_count};
use vykar_storage::{BackendProfile, StorageBackend};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

pub(crate) use item_cut::ItemStreamCutter;

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

/// Outcome of looking up a file in the local file cache and parent reuse index.
///
/// Tri-state so the caller can route dataless (cloud-only macOS placeholder)
/// files into a never-open code path: hit reuses chunks from a prior snapshot
/// without touching the file, miss-with-dataless skips the file outright
/// rather than falling through to a normal `read()` which would trigger
/// asynchronous hydration via `fileproviderd`.
#[derive(Debug)]
pub(crate) enum CacheResolution {
    /// Reuse cached chunks; the file is not opened. The token is present
    /// when the chunks come from an exact match in the local file cache, so
    /// the write cache can keep the loaded entry instead of a copy.
    Hit(CachedChunks, Option<RetainedHit>),
    /// No reuse — caller should open + chunk normally.
    Miss,
    /// File is dataless and neither the local file cache nor the parent
    /// reuse index has a matching entry. Caller must skip the file (no
    /// hydration, no read).
    SkipDataless,
}

/// Resolve a cache hit by checking the file cache, falling back to the parent
/// reuse index.
///
/// Dataless files (`summary.is_dataless == true`) use a ctime-tolerant lookup
/// path: ctime is dropped from the equality check because `SF_DATALESS` flag
/// toggles bump ctime even when content is unchanged. They first try the
/// warm `FileCache` (matches on `(device, inode, size, mtime)`) and then fall
/// back to the parent-reuse index (matches on `(path, size, mtime)`); if both
/// miss, the result is `SkipDataless` — **never** `Miss`, so the caller
/// cannot accidentally read and hydrate.
fn resolve_cache_hit(
    file_cache: &FileCache,
    parent_reuse_index: Option<&ParentReuseIndex>,
    abs_path: &str,
    summary: &fs::MetadataSummary,
    raw_path: bool,
) -> CacheResolution {
    // The parent-reuse index is keyed on the lossy display path (no inode), so
    // a non-UTF8 name could collide with a different file's entry — including a
    // valid-UTF8 name that contains U+FFFD. Bypass it entirely for raw-path
    // items; the inode-keyed local cache below is still safe.
    if summary.is_dataless {
        if let Some(chunks) = file_cache.lookup_dataless(
            abs_path,
            summary.device,
            summary.inode,
            summary.mtime_ns,
            summary.size,
        ) {
            // ctime was not compared, so the entry written for this file can
            // differ from the loaded one: no retain token.
            return CacheResolution::Hit(chunks, None);
        }
        if raw_path {
            return CacheResolution::SkipDataless;
        }
        return match parent_reuse_index
            .and_then(|idx| idx.lookup_dataless(abs_path, summary.size, summary.mtime_ns))
        {
            Some(chunks) => CacheResolution::Hit(chunks, None),
            None => CacheResolution::SkipDataless,
        };
    }

    let local = file_cache.lookup_retainable(
        abs_path,
        summary.device,
        summary.inode,
        summary.mtime_ns,
        summary.ctime_ns,
        summary.size,
    );
    if let Some((chunks, retained)) = local {
        return CacheResolution::Hit(chunks, Some(retained));
    }

    if raw_path {
        return CacheResolution::Miss;
    }

    let parent = parent_reuse_index
        .and_then(|idx| idx.lookup(abs_path, summary.size, summary.mtime_ns, summary.ctime_ns));
    match parent {
        Some(chunks) => CacheResolution::Hit(chunks, None),
        None => CacheResolution::Miss,
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
    cutter: &ItemStreamCutter,
    compression: Compression,
) -> Result<()> {
    let start = item_stream.len();
    rmp_serde::encode::write(item_stream, item)?;
    let entry_len = item_stream.len() - start;
    if cutter.should_cut(&item.path, entry_len, item_stream.len()) {
        flush_item_stream_chunk(repo, item_stream, item_ptrs, compression)?;
    }
    Ok(())
}

/// Run-local tally of cloud-only (macOS `FileProvider`) content that could not
/// be captured, accumulated across all sources of one backup run.
///
/// Deliberately **not** part of `SnapshotStats` — see the note on that type for
/// why these counters cannot live in the snapshot wire format.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DatalessTally {
    /// Dataless placeholder files skipped with no prior backup to carry forward.
    pub files: u64,
    /// Dataless directories whose listing failed — each one omits a subtree.
    pub dirs: u64,
}

/// Result of a backup run, containing stats and partial-success flag.
#[derive(Debug, Clone)]
pub struct BackupOutcome {
    pub stats: SnapshotStats,
    /// `true` when one or more files were skipped due to soft errors.
    pub is_partial: bool,
    /// Cloud-only files omitted from the snapshot because neither the local
    /// file cache nor the parent snapshot could supply their content. Run-local
    /// (never persisted) and deliberately **not** folded into `is_partial`:
    /// a machine with iCloud Drive would then report every nightly run as
    /// failed forever.
    pub dataless_files_skipped: u64,
    /// Cloud-only directories whose listing could not be materialized. Each one
    /// means an entire subtree is missing from the snapshot. Same reporting
    /// rules as `dataless_files_skipped`.
    pub dataless_dirs_skipped: u64,
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
    /// Emitted at each phase-2 commit stage boundary for progress reporting.
    CommitStage {
        stage: &'static str,
    },
    /// A non-fatal post-commit issue. The snapshot is durably committed.
    /// `tracing::warn!` has already fired; this surfaces the warning to the
    /// progress callback (CLI/GUI) since the GUI does not subscribe to tracing.
    Warning {
        message: String,
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

/// Emit the once-per-source summary warning(s) for macOS cloud-only content
/// that could not be captured. Per-entry `tracing::debug!` lines fire from the
/// walker; these single high-signal lines surface the totals to the CLI/GUI so
/// users notice tens-of-GB of cloud-only content was skipped without flooding
/// the activity log.
///
/// Up to two clauses are emitted. The directory clause is blunt on purpose:
/// an unlistable directory omits an unbounded subtree, not one file.
pub(crate) fn emit_dataless_summary(
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    files: u64,
    dirs: u64,
) {
    if files > 0 {
        let count = format_count(files);
        let msg = format!(
            "dataless: skipped {count} cloud-only files with no prior backup \
             (run while files are downloaded to back them up)"
        );
        warn!("{msg}");
        emit_progress(progress, BackupProgressEvent::Warning { message: msg });
    }
    if dirs > 0 {
        let count = format_count(dirs);
        let msg = format!(
            "dataless: {count} cloud-only directories could not be listed — \
             their contents are absent from this snapshot"
        );
        warn!("{msg}");
        emit_progress(progress, BackupProgressEvent::Warning { message: msg });
    }
}

/// Emit a post-commit warning: `tracing::warn!` + `BackupProgressEvent::Warning`
/// always happen together. The tracing half reaches stderr (via the tracing
/// subscriber in the CLI); the progress event half reaches the GUI log panel.
/// Keeping both sides in one helper prevents drift.
///
/// Generic over `F: FnMut(..)` so callers that hold either a concrete closure
/// (`&mut Option<impl FnMut(..)>`) or a type-erased one
/// (`&mut Option<&mut dyn FnMut(..)>`) can use it.
pub(crate) fn emit_post_commit_warning<F: FnMut(BackupProgressEvent)>(
    progress: &mut Option<F>,
    msg: String,
) {
    warn!("{msg}");
    if let Some(ref mut cb) = progress {
        cb(BackupProgressEvent::Warning { message: msg });
    }
}

/// Render a soft per-file error for a `skipping file '<path>': <reason>`
/// warning. Drift errors embed the offending path in their `Display`, which
/// would repeat the path the warning template already names — render those
/// without it.
pub(crate) fn skip_reason(e: &VykarError) -> String {
    match e {
        VykarError::FileChangedDuringRead { dataless, .. } => {
            let suffix = if *dataless {
                " (cloud-only file, hydration in progress)"
            } else {
                ""
            };
            format!("file changed during read{suffix}")
        }
        other => other.to_string(),
    }
}

/// Run `body` inside a rollback-guarded scope.
///
/// Arms a rollback checkpoint on the repo and snapshots the stats byte
/// counters. On `Ok` the checkpoint is committed; on `Err` the checkpoint
/// is rolled back (undoing refcount bumps, dedup inserts, recovered-chunk
/// promotion, and partial pack writer state) and the stats byte counters
/// are restored to their pre-scope values.
///
/// Closure form because a drop-guard holding `&mut Repository` + `&mut
/// SnapshotStats` can't compose with bodies that need to pass those same
/// mutable references to helpers like `flush_regular_file_batch`.
pub(crate) fn with_rollback_checkpoint<R>(
    repo: &mut Repository,
    stats: &mut SnapshotStats,
    body: impl FnOnce(&mut Repository, &mut SnapshotStats) -> Result<R>,
) -> Result<R> {
    let stats_snap = stats.snapshot_byte_counters();
    repo.begin_rollback_checkpoint()?;
    match body(repo, stats) {
        Ok(r) => {
            repo.commit_rollback_checkpoint();
            Ok(r)
        }
        Err(e) => {
            repo.abort_rollback_checkpoint();
            stats.restore_byte_counters(stats_snap);
            Err(e)
        }
    }
}

/// Replace an item's walk-time metadata with the pre-read `fstat`.
///
/// Every path that commits file content does this before persisting the
/// `Item`: the descriptor-based stat is the one the drift checks validated,
/// so it — not the walk's path-based stat — is what the stored chunks
/// correspond to.
pub(crate) fn stamp_item_metadata(item: &mut Item, meta: &fs::MetadataSummary) {
    item.mode = meta.mode;
    item.uid = meta.uid;
    item.gid = meta.gid;
    item.size = meta.size;
    item.mtime = meta.mtime_ns;
    item.ctime = Some(meta.ctime_ns);
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
    progress: Option<&mut dyn FnMut(BackupProgressEvent)>,
    shutdown: Option<&AtomicBool>,
) -> Result<BackupOutcome> {
    run_impl(config, req, progress, shutdown, None)
}

/// Wraps the storage backend before the backup uses it (tests only).
pub(crate) type BackendWrapper<'a> =
    &'a dyn Fn(Box<dyn vykar_storage::StorageBackend>) -> Box<dyn vykar_storage::StorageBackend>;

/// Where a backup failed, decided at the call site because error variants
/// don't identify scope (a corrupt key is `Other`, bad stored chunker params
/// are `Config` like a missing source).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureScope {
    /// Before any repository I/O: the source itself is unusable.
    Source,
    /// Repository open, upload or commit (including interrupts).
    Repository,
}

/// [`run_with_progress`] with an optional backend wrapper, so tests can
/// observe or fault-inject storage traffic.
pub(crate) fn run_impl(
    config: &VykarConfig,
    req: BackupRequest<'_>,
    progress: Option<&mut dyn FnMut(BackupProgressEvent)>,
    shutdown: Option<&AtomicBool>,
    wrap_backend: Option<BackendWrapper<'_>>,
) -> Result<BackupOutcome> {
    run_scoped(config, req, progress, shutdown, wrap_backend).map_err(|(_, e)| e)
}

/// [`run_impl`] that also reports whether a failure was source-local.
pub(crate) fn run_scoped(
    config: &VykarConfig,
    req: BackupRequest<'_>,
    progress: Option<&mut dyn FnMut(BackupProgressEvent)>,
    shutdown: Option<&AtomicBool>,
    wrap_backend: Option<BackendWrapper<'_>>,
) -> std::result::Result<BackupOutcome, (FailureScope, VykarError)> {
    let prepared = prepare(config, &req).map_err(|e| (FailureScope::Source, e))?;
    run_against_repo(config, &req, prepared, progress, shutdown, wrap_backend)
        .map_err(|e| (FailureScope::Repository, e))
}

/// Source-local setup that touches no repository state.
struct Prepared<'a> {
    xattrs_enabled: bool,
    exclude_patterns: std::borrow::Cow<'a, [String]>,
    resolved_sources: Vec<source::ResolvedSource>,
    max_pending_transform_bytes: usize,
    max_pending_file_actions: usize,
}

/// Transform worker setup, sized to the backend.
struct Workers {
    num_workers: usize,
    use_pipeline: bool,
    transform_pool: Option<rayon::ThreadPool>,
}

fn resolve_workers(limits: &ResourceLimitsConfig, profile: BackendProfile) -> Result<Workers> {
    // Resolve effective worker count before building the rayon pool so we
    // can right-size it in pipeline mode (avoids 2× thread oversubscription).
    let num_workers = limits.effective_backup_threads(profile.local);

    // Pipeline mode when we have more than 1 worker thread.
    let use_pipeline = num_workers > 1;

    let transform_pool = if use_pipeline {
        // Pipeline mode doesn't need a rayon pool (no inline large-file processing).
        None
    } else {
        sequential::build_transform_pool(num_workers)?
    };

    Ok(Workers {
        num_workers,
        use_pipeline,
        transform_pool,
    })
}

fn prepare<'a>(config: &VykarConfig, req: &BackupRequest<'a>) -> Result<Prepared<'a>> {
    let source_paths = req.source_paths;
    let command_dumps = req.command_dumps;
    let xattrs_enabled = if req.xattrs_enabled && !fs::xattrs_supported() {
        warn!("xattrs requested but not supported on this platform; continuing without xattrs");
        false
    } else {
        req.xattrs_enabled
    };

    if source_paths.is_empty() && command_dumps.is_empty() {
        return Err(VykarError::Other(
            "no source paths or command dumps specified".into(),
        ));
    }
    if req.one_file_system && !cfg!(unix) {
        warn!("one_file_system filtering has limited support on this platform");
    }

    // When command dumps are active, exclude the vykar-dumps/ directory from
    // the filesystem walk to prevent duplicate paths in the snapshot.
    let exclude_patterns = if command_dumps.is_empty() {
        std::borrow::Cow::Borrowed(req.exclude_patterns)
    } else {
        let mut v = req.exclude_patterns.to_vec();
        v.push("/vykar-dumps".to_string());
        warn!("excluding vykar-dumps/ from filesystem walk (reserved for command dump output)");
        std::borrow::Cow::Owned(v)
    };

    let multi_path = source_paths.len() > 1;

    // Resolve every configured source up-front: stat, canonicalize, derive
    // snapshot prefix + emission policy, and reject equal-prefix or nested-
    // canonical-root collisions. This runs BEFORE opening the repo so any
    // source-validation failure short-circuits with no session registered.
    let resolved_sources = source::ResolvedSource::resolve_all(source_paths, multi_path)?;

    if let Err(e) = limits::apply_process_nice(config.limits.nice) {
        warn!("could not apply limits.nice={}: {e}", config.limits.nice);
    }

    Ok(Prepared {
        xattrs_enabled,
        exclude_patterns,
        resolved_sources,
        max_pending_transform_bytes: config.limits.transform_batch_bytes(),
        max_pending_file_actions: config.limits.max_pending_actions(),
    })
}

fn run_against_repo(
    config: &VykarConfig,
    req: &BackupRequest<'_>,
    prepared: Prepared<'_>,
    mut progress: Option<&mut dyn FnMut(BackupProgressEvent)>,
    shutdown: Option<&AtomicBool>,
    wrap_backend: Option<BackendWrapper<'_>>,
) -> Result<BackupOutcome> {
    let snapshot_name = req.snapshot_name;
    let passphrase = req.passphrase;
    let source_paths = req.source_paths;

    let backend = storage::backend_from_config(&config.repository, config.limits.connections)?;
    let backend = limits::wrap_storage_backend(backend, &config.limits);
    let backend = match wrap_backend {
        Some(wrap) => wrap(backend),
        None => backend,
    };
    let backend: Arc<dyn StorageBackend> = Arc::from(backend);
    let profile = backend.profile();
    let workers = resolve_workers(&config.limits, profile)?;

    // Generate a unique session ID for this backup.
    let session_id = format!("{:032x}", rand::random::<u128>());
    let session_opts = WriteSessionOptions::for_backup(session_id.clone(), profile);

    // ── Phase 1: Register session and upload (no lock) ──────────────────
    //
    // Ends with all packs durable in storage (and recorded in the session's
    // pending_index journal), so Phase 2 has no uploads left to wait for.

    // Register session marker and probe for maintenance lock.
    lock::register_session(backend.as_ref(), &session_id)?;
    debug!(session_id, "backup session registered");

    // Open repo after session registration (minimizes T0→commit window).
    // If open fails, deregister through the same backend so the session
    // doesn't block maintenance until the marker ages out (45 min).
    //
    // When there are no filesystem source paths (command-dump-only backup),
    // skip loading the file cache — it's never consulted for command dumps
    // and can be large (~736K entries).
    let cache_dir = super::util::cache_dir_from_config(config);
    let opts = if source_paths.is_empty() {
        crate::repo::OpenOptions::new().with_index()
    } else {
        crate::repo::OpenOptions::new()
            .with_index()
            .with_file_cache()
    };
    let opts = super::util::with_identity_context(opts, config);
    let open_result = Repository::open_shared(Arc::clone(&backend), passphrase, cache_dir, opts);
    let mut repo = match open_result {
        Ok(r) => {
            if let Err(e) = super::util::verify_repo_identity(config, &r) {
                lock::deregister_session(backend.as_ref(), &session_id);
                return Err(e);
            }
            r
        }
        Err(e) => {
            let e = super::util::enrich_repo_not_found(e, &config.repository.url);
            lock::deregister_session(backend.as_ref(), &session_id);
            return Err(e);
        }
    };
    drop(backend);

    // Adopt the session into a guard so the heartbeat thread is independent
    // of the upload pipeline. `BackupSession` owns the guard, and every
    // deregistration is the guard's Drop (stop-thread → join-thread →
    // deregister). Error returns rely on `session` dropping at the return,
    // after `flush_on_abort`; Phase 2 ends it explicitly where it must precede
    // the lock release, or hands it to the cleanup thread at the commit point.
    // `session` is declared after `repo` so it drops before `Repository::drop`;
    // those error returns depend on that order.
    let mut session = phases::BackupSession::adopt(&repo.storage, session_id)?;

    // Run-local tally of unreachable cloud-only content. Declared here because
    // Phase 1 below borrows it while `BackupOutcome` is built after it
    // returns. Never persisted into the snapshot (see `SnapshotStats`).
    let mut dataless = DatalessTally::default();

    // Write session, command dumps, walk and uploads (no lock).
    let phase1_result = phases::phase1_upload(
        &mut repo,
        config,
        req,
        &prepared,
        &workers,
        &session_opts,
        &mut progress,
        shutdown,
        &mut dataless,
    );

    // On Phase 1 error: flush packs, then deregister (`session` drops at the return).
    let phases::Phase1Output {
        snapshot_entry,
        snapshot_packed,
        mut new_file_cache,
        stats,
    } = match phase1_result {
        Ok(result) => result,
        Err(e) => {
            repo.flush_on_abort();
            // Do NOT save file cache on abort — the on-disk cache from the last
            // successful run is still valid. Saving here would persist the
            // depleted (invalidated) cache and destroy future cache hits.
            return Err(e);
        }
    };

    // ── Phase 2: Commit (exclusive lock) ────────────────────────────────
    //
    // new_file_cache is passed by &mut so the caller retains ownership.
    // On success, commit_concurrent_session_with_progress consumes the active
    // section (merges it into the persistent cache).

    let commit_result = phases::phase2_commit(
        &mut repo,
        &mut session,
        snapshot_entry,
        snapshot_packed,
        &mut new_file_cache,
        &mut progress,
        shutdown,
    );

    if commit_result.is_err() {
        // flush_on_abort writes the remote sessions/<id>.index so the next
        // run's recover_pending_index() can discover our packs. Must run on
        // ALL Phase 2 failures (lock acquisition, commit, lock release).
        // `session` drops at the return below, after this flush; a failed
        // commit has already deregistered it before releasing the lock.
        repo.flush_on_abort();
    }
    commit_result?;

    if stats.errors > 0 {
        info!(
            "Snapshot '{}' created: {} files, {} errors, {} original, {} compressed, {} deduplicated",
            snapshot_name,
            format_count(stats.nfiles),
            format_count(stats.errors),
            format_bytes(stats.original_size),
            format_bytes(stats.compressed_size),
            format_bytes(stats.deduplicated_size)
        );
    } else {
        info!(
            "Snapshot '{}' created: {} files, {} original, {} compressed, {} deduplicated",
            snapshot_name,
            format_count(stats.nfiles),
            format_bytes(stats.original_size),
            format_bytes(stats.compressed_size),
            format_bytes(stats.deduplicated_size)
        );
    }

    // `is_partial` intentionally ignores the dataless tally: skipped cloud-only
    // content is reported, not treated as a failure (see `BackupOutcome`).
    let is_partial = stats.errors > 0;
    Ok(BackupOutcome {
        stats,
        is_partial,
        dataless_files_skipped: dataless.files,
        dataless_dirs_skipped: dataless.dirs,
    })
}

/// Order-independent comparison of source path lists.
fn source_paths_match(a: &[String], b: &[String]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut sorted_a: Vec<&str> = a.iter().map(|s| s.as_str()).collect();
    let mut sorted_b: Vec<&str> = b.iter().map(|s| s.as_str()).collect();
    sorted_a.sort();
    sorted_b.sort();
    sorted_a == sorted_b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_paths_match_order_independent() {
        let a = vec!["/b".to_string(), "/a".to_string()];
        let b = vec!["/a".to_string(), "/b".to_string()];
        assert!(source_paths_match(&a, &b));
    }

    #[test]
    fn source_paths_match_different_lengths() {
        let a = vec!["/a".to_string()];
        let b = vec!["/a".to_string(), "/b".to_string()];
        assert!(!source_paths_match(&a, &b));
    }

    #[test]
    fn source_paths_match_empty_vs_nonempty() {
        let a: Vec<String> = vec![];
        let b = vec!["/a".to_string()];
        assert!(!source_paths_match(&a, &b));
    }

    #[test]
    fn source_paths_match_both_empty() {
        let a: Vec<String> = vec![];
        let b: Vec<String> = vec![];
        assert!(source_paths_match(&a, &b));
    }

    #[test]
    fn source_paths_match_identical_order() {
        let a = vec!["/a".to_string(), "/b".to_string()];
        let b = vec!["/a".to_string(), "/b".to_string()];
        assert!(source_paths_match(&a, &b));
    }

    /// Collect the `Warning` messages `emit_dataless_summary` produces for a
    /// given `(files, dirs)` pair.
    fn dataless_warnings(files: u64, dirs: u64) -> Vec<String> {
        let mut msgs: Vec<String> = Vec::new();
        let mut cb = |evt: BackupProgressEvent| {
            if let BackupProgressEvent::Warning { message } = evt {
                msgs.push(message);
            }
        };
        let mut progress: Option<&mut dyn FnMut(BackupProgressEvent)> = Some(&mut cb);
        emit_dataless_summary(&mut progress, files, dirs);
        msgs
    }

    #[test]
    fn dataless_summary_files_only() {
        let msgs = dataless_warnings(7, 0);
        assert_eq!(msgs.len(), 1, "{msgs:?}");
        assert!(msgs[0].contains("skipped 7 cloud-only files"), "{msgs:?}");
        assert!(!msgs[0].contains("directories"), "{msgs:?}");
    }

    /// The directory clause must say the *contents* are gone — each unlistable
    /// directory omits an unbounded subtree, not one file.
    #[test]
    fn dataless_summary_dirs_only() {
        let msgs = dataless_warnings(0, 3);
        assert_eq!(msgs.len(), 1, "{msgs:?}");
        assert!(
            msgs[0].contains("3 cloud-only directories could not be listed"),
            "{msgs:?}"
        );
        assert!(
            msgs[0].contains("absent from this snapshot"),
            "should spell out that the subtree is missing: {msgs:?}"
        );
    }

    #[test]
    fn dataless_summary_both_clauses() {
        let msgs = dataless_warnings(4, 2);
        assert_eq!(msgs.len(), 2, "{msgs:?}");
        assert!(msgs[0].contains("4 cloud-only files"), "{msgs:?}");
        assert!(msgs[1].contains("2 cloud-only directories"), "{msgs:?}");
    }

    #[test]
    fn dataless_summary_silent_when_nothing_skipped() {
        assert!(dataless_warnings(0, 0).is_empty());
    }

    fn dataless_metadata(size: u64, mtime_ns: i64) -> fs::MetadataSummary {
        fs::MetadataSummary {
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime_ns,
            // ctime drifts when SF_DATALESS toggles — must NOT influence
            // the dataless-tolerant lookup.
            ctime_ns: 999_999,
            device: 0,
            inode: 0,
            nlink: 1,
            size,
            is_dataless: true,
        }
    }

    #[test]
    fn resolve_cache_hit_dataless_no_parent_returns_skip() {
        let file_cache = FileCache::new();
        let summary = dataless_metadata(4096, 1000);
        let res = resolve_cache_hit(&file_cache, None, "/src/a.txt", &summary, false);
        assert!(matches!(res, CacheResolution::SkipDataless));
    }

    #[test]
    fn resolve_cache_hit_dataless_with_parent_match_returns_hit() {
        use crate::repo::file_cache::{ParentReuseBuilder, ParentReusePolicy, ParentReuseRoot};
        use crate::snapshot::item::{Item, ItemType};
        use vykar_types::chunk_id::ChunkId;

        let item = Item {
            path: "a.txt".into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 1000,
            atime: None,
            // Parent ctime differs from the dataless one — the lookup must
            // ignore ctime and still hit.
            ctime: Some(2000),
            size: 4096,
            chunks: vec![crate::snapshot::item::ChunkRef {
                id: ChunkId::from_bytes([0xCC; 32]),
                size: 4096,
                csize: 2048,
            }],
            link_target: None,
            xattrs: None,
            raw_names: None,
            hardlink: None,
        };
        let mut builder = ParentReuseBuilder::new(vec![ParentReuseRoot {
            abs_root: "/src".into(),
            policy: ParentReusePolicy::SkipRoot,
        }]);
        builder.push(item);
        let parent_index = builder.finish().unwrap();

        let file_cache = FileCache::new();
        let summary = dataless_metadata(4096, 1000);
        let abs = std::path::Path::new("/src")
            .join("a.txt")
            .to_string_lossy()
            .to_string();
        let res = resolve_cache_hit(&file_cache, Some(&parent_index), &abs, &summary, false);
        match res {
            CacheResolution::Hit(chunks, retained) => {
                assert_eq!(chunks.len(), 1);
                assert!(retained.is_none(), "only exact local hits are retainable");
            }
            other => panic!("expected Hit, got {other:?}"),
        }
    }

    /// A raw-path item must bypass the (lossy-keyed) parent-reuse index on
    /// lookup: with a matching parent entry, `raw_path=false` hits but
    /// `raw_path=true` returns `Miss` — the U+FFFD-collision protection.
    #[test]
    fn resolve_cache_hit_raw_path_bypasses_parent_reuse() {
        use crate::repo::file_cache::{ParentReuseBuilder, ParentReusePolicy, ParentReuseRoot};
        use crate::snapshot::item::{Item, ItemType};
        use vykar_types::chunk_id::ChunkId;

        let item = Item {
            path: "a.txt".into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 1000,
            atime: None,
            ctime: Some(2000),
            size: 4096,
            chunks: vec![crate::snapshot::item::ChunkRef {
                id: ChunkId::from_bytes([0xCC; 32]),
                size: 4096,
                csize: 2048,
            }],
            link_target: None,
            xattrs: None,
            raw_names: None,
            hardlink: None,
        };
        let mut builder = ParentReuseBuilder::new(vec![ParentReuseRoot {
            abs_root: "/src".into(),
            policy: ParentReusePolicy::SkipRoot,
        }]);
        builder.push(item);
        let parent_index = builder.finish().unwrap();

        let file_cache = FileCache::new();
        let mut summary = dataless_metadata(4096, 1000);
        summary.is_dataless = false;
        summary.ctime_ns = 2000;
        let abs = std::path::Path::new("/src")
            .join("a.txt")
            .to_string_lossy()
            .to_string();

        // Non-raw path: parent reuse hits.
        match resolve_cache_hit(&file_cache, Some(&parent_index), &abs, &summary, false) {
            CacheResolution::Hit(chunks, retained) => {
                assert_eq!(chunks.len(), 1);
                assert!(retained.is_none(), "only exact local hits are retainable");
            }
            other => panic!("expected Hit, got {other:?}"),
        }
        // Raw path: parent reuse bypassed → Miss.
        assert!(matches!(
            resolve_cache_hit(&file_cache, Some(&parent_index), &abs, &summary, true),
            CacheResolution::Miss
        ));
    }

    /// Helper: build a `FileCache` with a single warm dataless-eligible entry
    /// under `/src` keyed at `abs_path`. The stored ctime is intentionally
    /// far from any value the caller will pass in `summary.ctime_ns` so that
    /// any test asserting reuse must be ignoring ctime.
    fn warm_file_cache_with(
        abs_path: &str,
        device: u64,
        inode: u64,
        mtime_ns: i64,
        size: u64,
    ) -> FileCache {
        use crate::repo::file_cache::{CachedChunkRef, CachedChunks};
        use vykar_types::chunk_id::ChunkId;

        let mut cache = FileCache::new();
        cache.begin_sections(&["/src".to_string()], &[1]);
        cache.insert(
            abs_path,
            device,
            inode,
            mtime_ns,
            // Stored ctime far from the dataless summary's ctime — the
            // dataless lookup must ignore it.
            123_456_789,
            size,
            CachedChunks::Single(CachedChunkRef {
                id: ChunkId::from_bytes([0xAB; 32]),
                size: size as u32,
            }),
        );
        cache
    }

    #[test]
    fn resolve_cache_hit_warm_dataless_uses_local_cache() {
        let abs = std::path::Path::new("/src")
            .join("a.txt")
            .to_string_lossy()
            .to_string();
        let device = 42;
        let inode = 7;
        let mtime_ns = 1_000;
        let size = 4096;

        let cache = warm_file_cache_with(&abs, device, inode, mtime_ns, size);
        let mut summary = dataless_metadata(size, mtime_ns);
        summary.device = device;
        summary.inode = inode;

        let res = resolve_cache_hit(&cache, None, &abs, &summary, false);
        match res {
            CacheResolution::Hit(chunks, retained) => {
                assert_eq!(chunks.len(), 1);
                assert!(retained.is_none(), "only exact local hits are retainable");
            }
            other => panic!("expected Hit from warm cache, got {other:?}"),
        }
    }

    /// An exact (non-dataless) local hit carries a retain token, unlike the
    /// dataless hit above, whose lookup does not compare ctime.
    #[test]
    fn resolve_cache_hit_exact_local_hit_is_retainable() {
        let abs = std::path::Path::new("/src")
            .join("a.txt")
            .to_string_lossy()
            .to_string();
        let cache = warm_file_cache_with(&abs, 42, 7, 1_000, 4096);
        let mut summary = dataless_metadata(4096, 1_000);
        summary.device = 42;
        summary.inode = 7;
        summary.ctime_ns = 123_456_789;
        summary.is_dataless = false;
        match resolve_cache_hit(&cache, None, &abs, &summary, false) {
            CacheResolution::Hit(chunks, retained) => {
                assert_eq!(chunks.len(), 1);
                assert!(retained.is_some());
            }
            other => panic!("expected Hit, got {other:?}"),
        }
        // Any differing compared field is a miss, never a retained hit.
        summary.ctime_ns += 1;
        assert!(matches!(
            resolve_cache_hit(&cache, None, &abs, &summary, false),
            CacheResolution::Miss
        ));
    }

    #[test]
    fn resolve_cache_hit_warm_dataless_falls_back_to_parent() {
        use crate::repo::file_cache::{ParentReuseBuilder, ParentReusePolicy, ParentReuseRoot};
        use crate::snapshot::item::{Item, ItemType};
        use vykar_types::chunk_id::ChunkId;

        // Empty file cache — nothing matches in the warm path.
        let file_cache = FileCache::new();

        let item = Item {
            path: "a.txt".into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 1_000,
            atime: None,
            ctime: Some(2_000),
            size: 4096,
            chunks: vec![crate::snapshot::item::ChunkRef {
                id: ChunkId::from_bytes([0xCC; 32]),
                size: 4096,
                csize: 2048,
            }],
            link_target: None,
            xattrs: None,
            raw_names: None,
            hardlink: None,
        };
        let mut builder = ParentReuseBuilder::new(vec![ParentReuseRoot {
            abs_root: "/src".into(),
            policy: ParentReusePolicy::SkipRoot,
        }]);
        builder.push(item);
        let parent_index = builder.finish().unwrap();

        let summary = dataless_metadata(4096, 1_000);
        let abs = std::path::Path::new("/src")
            .join("a.txt")
            .to_string_lossy()
            .to_string();
        let res = resolve_cache_hit(&file_cache, Some(&parent_index), &abs, &summary, false);
        match res {
            CacheResolution::Hit(chunks, retained) => {
                assert_eq!(chunks.len(), 1);
                assert!(retained.is_none(), "only exact local hits are retainable");
            }
            other => panic!("expected Hit from parent index, got {other:?}"),
        }
    }

    #[test]
    fn resolve_cache_hit_warm_dataless_skips_when_neither_matches() {
        let file_cache = FileCache::new();
        let summary = dataless_metadata(4096, 1_000);
        let abs = std::path::Path::new("/src")
            .join("a.txt")
            .to_string_lossy()
            .to_string();
        let res = resolve_cache_hit(&file_cache, None, &abs, &summary, false);
        assert!(matches!(res, CacheResolution::SkipDataless));
    }
}
