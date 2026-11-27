//! The two phases of a backup run, driven by `run_scoped`, and the session owner they share:
//! Phase 1 (`phase1_upload`), and Phase 2 as [`PendingCommit`] and [`CommitInProgress`].

use std::collections::HashSet;
use std::sync::Arc;

use tracing::{debug, info, warn};

use super::instrument::{BackupInstrumentation, PipelineTimings};
use super::{
    BackupOutcome, BackupProgressEvent, BackupRequest, DatalessTally, ListingWriter, Prepared,
    command_dump, emit_progress, pipeline, source, source_paths_match,
};
use crate::config::VykarConfig;
use crate::repo::ages::HEAD_LANDING_ALLOWANCE;
use crate::repo::control::{CancelToken, CommitEvent, EventSink};
use crate::repo::file_cache::{FileCache, ParentReuseBuilder, ParentReuseIndex, ParentReuseRoot};
use crate::repo::format::{ObjectType, pack_object_with_context};
#[cfg(any(test, feature = "sim"))]
use crate::repo::machine::{Machine, StepKind};
use crate::repo::machine::{StepCx, drive_blocking};
use crate::repo::manifest::SnapshotEntry;
use crate::repo::sessions;
use crate::repo::{BackupCommit, Committed, Fence, HostEnv, Repo, WriteSession, WriterMode};
use crate::snapshot::{SnapshotMeta, SnapshotStats};
use vykar_common::display::{format_bytes, format_count};
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};
use vykar_types::snapshot_id::SnapshotId;

/// The backup's registered session. Owns the heartbeat guard until the
/// commit point hands it to the cleanup thread; dropping this struct (or
/// `end()`) runs the guard's Drop: stop-thread → join-thread → deregister.
pub(super) struct BackupSession {
    id: String,
    guard: Option<sessions::SessionGuard>,
}

impl BackupSession {
    /// Adopt the already-registered session into a heartbeat guard. On
    /// failure the marker is deregistered before the error is returned.
    pub(super) fn adopt(
        storage: &Arc<dyn StorageBackend>,
        registration: sessions::Registration,
        env: HostEnv,
    ) -> Result<Self> {
        let id = registration.session_id().to_string();
        match sessions::SessionGuard::adopt(Arc::clone(storage), registration, env.clone()) {
            Ok(g) => Ok(Self { id, guard: Some(g) }),
            Err(e) => {
                sessions::deregister_session(storage.as_ref(), &id, &env);
                Err(e)
            }
        }
    }

    /// The session's marker record; `None` once the guard was handed off.
    pub(super) fn marker(&self) -> Option<Arc<sessions::SessionMarker>> {
        self.guard.as_ref().map(sessions::SessionGuard::marker)
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

/// Phase 1: prepare the write session `ws` (begun with the backup's options:
/// per-session `pending_index`, upload limits, staging, so command dumps
/// already run under the backup's upload limits), run command dumps and the
/// walk, and upload every pack (no lock). Returns the packed snapshot for
/// Phase 2.
#[allow(clippy::too_many_arguments)]
pub(super) fn phase1_upload(
    repo: &mut Repo,
    ws: &mut WriteSession,
    config: &VykarConfig,
    req: &BackupRequest<'_>,
    prepared: &Prepared<'_>,
    num_workers: usize,
    progress: Option<&dyn EventSink<BackupProgressEvent>>,
    cancel: &CancelToken,
    dataless: &mut DatalessTally,
    marker: Option<Arc<sessions::SessionMarker>>,
) -> Result<Phase1Output> {
    let env = repo.host_env().clone();
    let clock = Arc::clone(&env.clock);
    let phase1_start = clock.monotonic();
    let _span = tracing::debug_span!("backup_phase1").entered();
    let snapshot_name = req.snapshot_name;
    let source_paths = req.source_paths;
    let source_label = req.source_label;
    let exclude_if_present = req.exclude_if_present;
    let one_file_system = req.one_file_system;
    let git_ignore = req.git_ignore;
    let compression = req.compression;
    let command_dumps = req.command_dumps;
    // Verbose per-file events only make sense when there is a sink to receive them.
    let verbose = req.verbose && progress.is_some();
    let xattrs_enabled = prepared.xattrs_enabled;
    let exclude_patterns: &[String] = &prepared.exclude_patterns;
    let resolved_sources: &[source::ResolvedSource] = &prepared.resolved_sources;

    // Existing repositories bypass YAML validation and may store odd
    // parameters from before FastCDC 5, or anything at all if `config` was
    // tampered with (it is unauthenticated). Check before any upload or dump
    // execution, while retaining the normal session cleanup on failure.
    // Do not reject these in `Repo::open`: restore must remain usable.
    repo.config()
        .chunker_params
        .validate_stored()
        .map_err(|e| {
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

    if let Some(m) = &marker {
        ws.attach_session_marker(Arc::clone(m));
    }

    // Recover chunk→pack mappings from a previous interrupted session.
    match ws.recover_pending_index(repo, cancel) {
        Ok(recovery) => {
            if recovery.recovered_chunks > 0 {
                info!(
                    recovered_chunks = recovery.recovered_chunks,
                    sessions = recovery.recovered_sessions.len(),
                    "recovered pending index from interrupted session"
                );
            }
            // The recovered packs are as old as the session's start must
            // be for compact's horizon and the commit rule (04 "Backup
            // commit" 1).
            if let (Some(age), Some(m)) = (recovery.oldest_pack_age, &marker) {
                m.pull_back(age);
                info!(
                    age_secs = age.as_secs(),
                    "recovered journal packs pull the session start back"
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
            .invalidate_missing_snapshots_collect(&|id| snapshot_ids.contains(id));
        if !invalidated.is_empty() {
            info!(
                invalidated = invalidated.len(),
                "invalidated file cache sections for deleted snapshots"
            );
            repo.note_file_cache_dropped(invalidated);
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
                    cancel,
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

    // Workers confirm dedup hits against the session's table themselves.
    let prefilter = ws.dedup_prefilter();

    let time_start = clock.now_utc();
    let mut stats = SnapshotStats::default();
    let mut listing = ListingWriter::new(repo.crypto().chunk_hasher(), compression);
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
    let walk_start = clock.monotonic();
    command_dump::process_command_dumps(
        repo,
        ws,
        command_dumps,
        compression,
        &mut listing,
        &mut stats,
        progress,
        time_start,
    )?;

    let pipeline_depth = config.limits.effective_pipeline_depth();
    let pipeline_buffer_bytes = config.limits.pipeline_buffer_for_workers(num_workers);

    let mut pipeline_timings: Option<PipelineTimings> = None;
    if !source_paths.is_empty() {
        let file_cache_snapshot = repo.take_file_cache();
        let crypto = std::sync::Arc::clone(repo.crypto());
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
            num_workers,
            readahead_depth: pipeline_depth,
            segment_size,
            pipeline_buffer_bytes,
            prefilter: prefilter.as_ref(),
            cancel,
            verbose,
            parent_reuse_index: parent_reuse_index.as_ref(),
        };
        let mut pipeline_bufs = pipeline::PipelineBuffers {
            listing: &mut listing,
            stats: &mut stats,
            new_file_cache: &mut new_file_cache,
            dataless,
        };
        let pipeline_result =
            pipeline::run_parallel_pipeline(repo, ws, &pipeline_ctx, &mut pipeline_bufs, progress);
        // Always restore before propagating — keeps repo.file_cache valid
        // for commit-time merge.
        repo.restore_file_cache(file_cache_snapshot);
        pipeline_timings = Some(pipeline_result?);
    }
    let walk = clock.elapsed(walk_start);
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
    // never saves the file cache (see the Phase 1 abort path in `upload_prepared`).
    if !source_paths.is_empty() {
        let carried = new_file_cache.absorb_read_sections(repo.file_cache_mut());
        debug!(
            carried,
            "moved unchanged entries of the loaded file cache into the new one"
        );
    }

    // Bail before committing if shutdown was requested during the walk.
    cancel.check()?;

    let listing = listing.finish(repo, ws)?;

    // Seal partial packs and wait for every upload here, before the commit
    // is protected, so Phase 2 is only the index/snapshot writes. The final
    // journal write overlaps the protection; the commit's own flush
    // joins it (and retries it inline if it failed) before anything
    // else, and long before the journal is deleted.
    emit_progress(
        progress,
        BackupProgressEvent::CommitStage {
            stage: "upload packs",
        },
    );
    let flush_start = clock.monotonic();
    ws.flush_packs_journal_in_background(repo)?;
    let final_flush = clock.elapsed(flush_start);
    let instrumentation = BackupInstrumentation {
        threads: num_workers,
        phase1: clock.elapsed(phase1_start),
        walk,
        final_flush,
        pipeline: pipeline_timings,
        write: ws.write_metrics(),
        memory: ws.memory.snapshot(),
    };
    instrumentation.emit(&stats);
    cancel.check()?;

    let time_end = clock.now_utc();

    // Build snapshot metadata.
    let hostname = config
        .hostname_override
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .unwrap_or_else(|| env.host.short_hostname());
    let username = std::env::var("USER").unwrap_or_else(|_| "unknown".into());

    let snapshot_meta = SnapshotMeta {
        name: snapshot_name.to_string(),
        hostname: hostname.clone(),
        username,
        time: time_start,
        time_end,
        chunker_params: repo.config().chunker_params.clone(),
        comment: String::new(),
        item_ptrs: listing.item_ptrs,
        stats: stats.clone(),
        source_label: source_label.to_string(),
        source_paths: source_paths.to_vec(),
        label: String::new(),
        ext: listing.ext,
        format_version: listing.format_version,
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
        repo.crypto().as_ref(),
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

/// What the end of a run reports, carried from Phase 1 through the commit.
pub(super) struct RunReport {
    pub(super) new_file_cache: FileCache,
    pub(super) stats: SnapshotStats,
    pub(super) instrumentation: BackupInstrumentation,
    pub(super) dataless: DatalessTally,
    pub(super) snapshot_name: String,
}

/// A backup between its phases: session registered, packs uploaded,
/// snapshot packed, no lock held. `session` is the first field so it drops
/// (and deregisters) before `repo` and `write` (whose journal joins its
/// background write on drop), as the locals of the upload did. One protocol
/// protects its commit in both modes ([`Self::protect_commit`]): the
/// commit mark and the pre-head listing of `sessions/`, and on a single-writer
/// repository the single-writer check around the mark
/// (10-single-protocol step 7).
pub(crate) struct PendingCommit {
    pub(super) session: BackupSession,
    pub(super) repo: Repo,
    pub(super) write: WriteSession,
    pub(super) snapshot_entry: SnapshotEntry,
    pub(super) snapshot_packed: Vec<u8>,
    pub(super) report: RunReport,
    /// The mode the protection runs in ([`Self::decide_mode`]; the suspend
    /// rule may change it).
    pub(super) mode: Option<WriterMode>,
    /// The marker's current key carries `committing`.
    pub(super) marked: bool,
    /// The suspend rule restarted the marker record at the mark: every
    /// attempt of the commit re-checks its reused chunks.
    pub(super) restarted: bool,
    /// How often the suspend rule changed the mode in this protection.
    pub(super) mode_changes: u8,
}

/// What [`PendingCommit::suspend_rule`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Protection {
    /// The commit may begin.
    Proceed,
    /// The re-read named the other mode: the protection runs again from its
    /// start in it (the marker is unmarked).
    Restart,
}

/// Phase 2 in progress: the marker flagged and the commit machine. No lock
/// is held. Production drives it with [`Self::drive`], the simulator one
/// step at a time with [`Self::step`]; either way [`Self::finish`] ends it.
/// Dropped without `finish` (a crashed client in the simulator), the
/// session deregisters.
pub(crate) struct CommitInProgress {
    session: BackupSession,
    repo: Repo,
    /// The exclusion the commit's steps run under: `Fence::backup_commit`.
    fence: Fence,
    machine: BackupCommit,
    report: RunReport,
}

impl PendingCommit {
    #[cfg(any(test, feature = "sim"))]
    pub(crate) fn repo_mut(&mut self) -> &mut Repo {
        &mut self.repo
    }

    /// The handle's mode ([`Repo::commit_mode`]), decided right after open
    /// (`backup::upload`); here it hits the cache.
    pub(crate) fn decide_mode(&mut self) -> Result<WriterMode> {
        let mode = self.repo.commit_mode()?;
        self.mode = Some(mode);
        Ok(mode)
    }

    /// The simulator's driver: the mode the protection runs in.
    #[cfg(any(test, feature = "sim"))]
    pub(crate) fn protection_mode(&self) -> Option<WriterMode> {
        self.mode
    }

    /// The simulator's driver: whether the marker's current key carries
    /// `committing`.
    #[cfg(any(test, feature = "sim"))]
    pub(crate) fn is_marked(&self) -> bool {
        self.marked
    }

    fn mode(&self) -> Result<WriterMode> {
        self.mode.ok_or_else(|| {
            VykarError::Other("BUG: a commit protection before its mode was decided".into())
        })
    }

    /// Production: the whole protection of the commit, with one deadline
    /// (`COMMIT_MAX_WAIT` from its start), reporting "waiting for
    /// another writer" once contended. Each attempt:
    /// - single-writer: unless marked, the listing before the mark
    ///   ([`Self::list_before_mark`]) and the mark; the listing after it
    ///   ([`Self::list_after_mark`], which unmarks on contention); the
    ///   pre-head listing of `sessions/` ([`Self::try_probe`]; a live
    ///   exclusive session keeps the mark and waits); the suspend rule
    ///   ([`Self::suspend_rule`]);
    /// - multi-writer: unless marked, the mark; the pre-head listing; the
    ///   suspend rule.
    ///
    /// A `Restart` of the suspend rule runs the attempt again at once in the
    /// new mode. `cancel` is checked before every attempt and during the
    /// waits (`Interrupted`).
    pub(crate) fn protect_commit(
        &mut self,
        progress: Option<&dyn EventSink<BackupProgressEvent>>,
        cancel: &CancelToken,
    ) -> Result<()> {
        if self.mode.is_none() {
            self.decide_mode()?;
        }
        let env = self.repo.host_env().clone();
        sessions::wait_while_contended(
            &env,
            sessions::COMMIT_MAX_WAIT,
            500,
            cancel,
            |_| {
                emit_progress(
                    progress,
                    BackupProgressEvent::CommitStage {
                        stage: "waiting for another writer",
                    },
                );
            },
            || self.attempt(),
        )
    }

    /// One attempt of [`Self::protect_commit`].
    fn attempt(&mut self) -> Result<()> {
        loop {
            match self.mode()? {
                WriterMode::Single => {
                    if !self.marked {
                        self.list_before_mark()?;
                        self.mark_committing()?;
                    }
                    self.list_after_mark()?;
                }
                WriterMode::Multi => {
                    if !self.marked {
                        self.mark_committing()?;
                    }
                }
            }
            self.try_probe()?;
            match self.suspend_rule()? {
                Protection::Proceed => return Ok(()),
                Protection::Restart => {}
            }
        }
    }

    /// Single-writer, before the mark (unflagged): `Err(Locked(holder))`
    /// while another live session may write heads (a compact, a committing
    /// backup), so that two backups do not both mark and back off.
    pub(crate) fn list_before_mark(&mut self) -> Result<()> {
        match sessions::single_writer_listing(
            self.repo.storage().as_ref(),
            self.repo.host_env(),
            &self.session.id,
        )? {
            None => Ok(()),
            Some(writer) => Err(VykarError::Locked(writer.into_holder())),
        }
    }

    /// Write a marker key with `committing` (the first half of the marker
    /// handshake, both modes). A failed PUT fails the commit.
    pub(crate) fn mark_committing(&mut self) -> Result<()> {
        let marker = self.marker()?;
        let key = marker.mark(
            self.repo.storage().as_ref(),
            self.repo.host_env().clock.as_ref(),
            sessions::MarkerFlags {
                committing: true,
                writes_heads: false,
            },
        )?;
        self.marked = true;
        debug!(key, "session marked committing");
        Ok(())
    }

    /// Single-writer, after the mark: another live head writer → write a key
    /// without flags and `Err(Locked(holder))` (the protection waits
    /// unflagged and marks again); none → `Ok`. A failed unmark PUT fails
    /// the commit.
    pub(crate) fn list_after_mark(&mut self) -> Result<()> {
        let Some(writer) = sessions::single_writer_listing(
            self.repo.storage().as_ref(),
            self.repo.host_env(),
            &self.session.id,
        )?
        else {
            return Ok(());
        };
        self.unmark()?;
        Err(VykarError::Locked(writer.into_holder()))
    }

    /// Write a marker key without flags.
    fn unmark(&mut self) -> Result<()> {
        let marker = self.marker()?;
        let key = marker.mark(
            self.repo.storage().as_ref(),
            self.repo.host_env().clock.as_ref(),
            sessions::MarkerFlags::default(),
        )?;
        self.marked = false;
        debug!(key, "session unmarked");
        Ok(())
    }

    /// The pre-head listing of `sessions/` after the mark
    /// ([`sessions::exclusive_listing`] with the marker's current key; named for
    /// the probe of `locks/` it replaced in 10-single-protocol step 8), both
    /// modes: `Ok` records "now" (the key's mtime, for the fold's segment
    /// ages); `Err(Locked)` while an exclusive session is live (the mark
    /// stays, the commit waits).
    pub(crate) fn try_probe(&mut self) -> Result<()> {
        let (_, key) = self.marker()?.current();
        let now =
            sessions::exclusive_listing(self.repo.storage().as_ref(), self.repo.host_env(), &key)?;
        self.repo.set_store_time(now);
        Ok(())
    }

    /// The suspend rule (10-single-protocol step 7, open decision 3), after
    /// a successful probe. `Proceed` for a session whose marker record is
    /// [continuous](sessions::MarkerReport::continuous) (no request). A record
    /// that is not (a machine suspended over `MARKER_GAP_LIMIT`, marker
    /// writes failing that long, a slow marker write, a miss) means another
    /// client may have taken the session for ended, so it missed what ran
    /// meanwhile. Then, in order:
    /// 1. read `config` again ([`Repo::reread_writer_mode`]; another
    ///    identity or format refuses). A mode other than the one the
    ///    protection ran in: decide it ([`Repo::commit_mode`]; a
    ///    multi-writer one runs the put-if-absent check), unmark and return
    ///    `Restart` (once; a second change in one protection refuses);
    /// 2. the pack-age term at the mark ([`sessions::head_landing_check`] with
    ///    [`HEAD_LANDING_ALLOWANCE`] on the record as it is:
    ///    `StaleChunksDuringCommit` when the packs may be too old);
    /// 3. restart the record from the mark ([`sessions::SessionMarker::restart_record`]),
    ///    which keeps the term binding at the fences around the head PUT,
    ///    and have every attempt of the commit re-check its reused chunks;
    ///    `Proceed`.
    pub(crate) fn suspend_rule(&mut self) -> Result<Protection> {
        let marker = self.marker()?;
        let clock = Arc::clone(&self.repo.host_env().clock);
        let report = marker.report(clock.as_ref());
        if report.continuous() {
            return Ok(Protection::Proceed);
        }
        info!(
            session_id = %self.session.id,
            ?report,
            "this backup's session marker record has a gap (machine suspended?) or a miss; \
             another client may have taken it for ended: re-reading config and re-checking \
             before the commit"
        );
        let now = self.repo.reread_writer_mode()?;
        if Some(now) != self.mode {
            if self.mode_changes > 0 {
                return Err(VykarError::Other(
                    "the repository's commit mode changed twice during this commit; nothing \
                     was committed, run the backup again"
                        .into(),
                ));
            }
            self.mode_changes += 1;
            self.repo.commit_mode()?;
            self.mode = Some(now);
            self.unmark()?;
            return Ok(Protection::Restart);
        }
        sessions::head_landing_check(
            &self.session.id,
            &report,
            marker.elapsed(clock.as_ref()),
            HEAD_LANDING_ALLOWANCE,
        )?;
        marker.restart_record();
        self.restarted = true;
        Ok(Protection::Proceed)
    }

    fn marker(&self) -> Result<Arc<sessions::SessionMarker>> {
        self.session.marker().ok_or_else(|| {
            VykarError::Other("BUG: a backup commit without a session marker".into())
        })
    }

    /// Join the background journal write Phase 1 leaves running (the
    /// simulator's driver, so that nothing runs between its steps;
    /// production joins it in the commit's flush).
    #[cfg(any(test, feature = "sim"))]
    pub(crate) fn join_background_writes(&mut self) {
        self.write.join_journal_write();
    }

    /// Build the commit machine and its fence (the backup's, which passes:
    /// the machine checks its marker itself). No storage access; "now" is
    /// the marked key's mtime, recorded by the probe.
    pub(crate) fn begin_commit(self) -> CommitInProgress {
        let Self {
            session,
            repo,
            write,
            snapshot_entry,
            snapshot_packed,
            report,
            restarted,
            ..
        } = self;
        let machine = BackupCommit::new(write, snapshot_entry, snapshot_packed, session.marker())
            .recheck_reuse_always(restarted);
        CommitInProgress {
            session,
            repo,
            fence: Fence::backup_commit(),
            machine,
            report,
        }
    }

    /// The protection gave up (or failed): flush the packs (`flush_on_abort`
    /// writes the journal, so the next run's recovery can find them), then
    /// drop, which deregisters the session.
    pub(crate) fn abandon(self) {
        let Self {
            session,
            repo,
            write,
            ..
        } = self;
        // flush_on_abort writes the remote sessions/<id>.index so the next
        // run's recover_pending_index() can discover our packs.
        write.flush_on_abort(&repo);
        // Deregisters the session.
        drop(session);
    }
}

impl CommitInProgress {
    /// The steps the commit machine can run next; empty once it ended.
    #[cfg(any(test, feature = "sim"))]
    pub(crate) fn enabled(&self) -> Vec<StepKind> {
        self.machine.enabled()
    }

    /// Run enabled step `pick` of the commit machine (the simulator).
    #[cfg(any(test, feature = "sim"))]
    pub(crate) fn step(&mut self, pick: usize, events: &dyn EventSink<CommitEvent>) -> Result<()> {
        self.machine.step(
            &mut StepCx {
                repo: &mut self.repo,
                fence: &self.fence,
                events,
            },
            pick,
        )
    }

    /// Tests: the fence the commit's steps run under.
    #[cfg(test)]
    pub(crate) fn fence(&self) -> &Fence {
        &self.fence
    }

    /// Tests: whether the commit re-checks its reused chunks on every
    /// attempt (the suspend rule restarted its record).
    #[cfg(test)]
    pub(crate) fn rechecks_reuse_always(&self) -> bool {
        self.machine.rechecks_reuse_always()
    }

    /// The machine's output once it finished without error.
    #[cfg(any(test, feature = "sim"))]
    pub(crate) fn take_output(&mut self) -> Option<Committed> {
        self.machine.take_output()
    }

    /// Production: run the commit machine to the end.
    pub(crate) fn drive(&mut self, events: &dyn EventSink<CommitEvent>) -> Result<Committed> {
        drive_blocking(
            &mut self.machine,
            &mut StepCx {
                repo: &mut self.repo,
                fence: &self.fence,
                events,
            },
        )
    }

    /// End Phase 2 once the machine ended with `result`: the post-commit
    /// work and deregistration on success; on failure, deregistration, then
    /// `flush_on_abort`.
    /// `cancel` only shortens the post-commit wait for the local
    /// file-cache lock.
    pub(crate) fn finish(
        self,
        result: Result<Committed>,
        progress: Option<&dyn EventSink<BackupProgressEvent>>,
        cancel: &CancelToken,
    ) -> Result<BackupOutcome> {
        let Self {
            mut session,
            mut repo,
            fence: _,
            machine,
            mut report,
        } = self;
        // Handed to the post-commit work on success, flushed on failure.
        let mut write = Some(machine.into_write_session());

        // Once the snapshot is committed, deregister the session and delete
        // its journal on a helper thread while the commit finishes its local
        // post-commit work (the file cache save), instead of after it. No
        // lock is held: deregistration, which ends the `committing` flag,
        // follows the snapshot PUT, which is all the marker handshake, the
        // single-writer check and a compact's mark need. The guard's Drop
        // stops the heartbeat thread and joins it before the markers are
        // deleted, so no concurrent refresh call can race with this delete.
        let repo_storage = Arc::clone(repo.storage());
        let mut cleanup: Option<std::thread::JoinHandle<()>> = None;
        let result = result.map(|committed| {
            let events = |e: CommitEvent| emit_progress(progress, e.into());
            let write = write.take().expect("the session is taken once");
            repo.finish_backup_commit(
                committed,
                write,
                &mut report.new_file_cache,
                &events,
                cancel,
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
        });
        let journal_cleared = cleanup.is_some();
        if let Some(handle) = cleanup.take() {
            let _ = handle.join();
        }
        // Not reached by the helper (failed commit, or no thread): the same
        // cleanup, inline.
        session.end();

        // The store time, live sessions and segment ages.
        repo.clear_session_state();

        if let Err(e) = result {
            // flush_on_abort writes the remote sessions/<id>.index so the next
            // run's recover_pending_index() can discover our packs. A failed
            // commit has deregistered the session first; `session` drops after this flush.
            if let Some(write) = write.take() {
                write.flush_on_abort(&repo);
            }
            return Err(e);
        }
        if !journal_cleared {
            repo.clear_pending_index(&session.id);
        }

        let RunReport {
            stats,
            instrumentation,
            dataless,
            snapshot_name,
            ..
        } = report;
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
            instrumentation,
        })
    }
}
