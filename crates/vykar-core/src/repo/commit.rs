use chrono::Utc;
use tracing::{debug, warn};

use super::Repository;
use super::control::{CommitEvent, EventSink};
use super::file_cache;
use super::index_store::{self, CacheLocation, SyncedIndex};
use super::manifest::{self, Manifest};
use super::snapshot_cache;
use crate::index::dedup_cache;
use crate::index::head::IndexHead;
use crate::index::segment;
use crate::index::{ChunkIndex, IndexDelta};
use vykar_crypto::CryptoEngine;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// Emit a `CommitEvent::Stage` and return a context
/// (start time, stage name) for later `log_stage_elapsed`.
fn emit_stage(
    events: &dyn EventSink<CommitEvent>,
    stage: &'static str,
) -> (std::time::Instant, &'static str) {
    let start = std::time::Instant::now();
    events.emit(CommitEvent::Stage { stage });
    (start, stage)
}

/// `tracing::warn!` plus `CommitEvent::Warning`, always together.
fn post_commit_warning(events: &dyn EventSink<CommitEvent>, message: String) {
    warn!("{message}");
    events.emit(CommitEvent::Warning { message });
}

/// Join a scoped helper thread, re-raising its panic on the caller.
fn join_scoped<T>(handle: std::thread::ScopedJoinHandle<'_, T>) -> T {
    handle
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

fn log_stage_elapsed(ctx: (std::time::Instant, &'static str)) {
    debug!(
        stage = ctx.1,
        elapsed_ms = ctx.0.elapsed().as_millis() as u64,
        "commit stage complete"
    );
}

impl Repository {
    /// Load the chunk index from storage on demand: GET the head and bring
    /// the local full index cache to it. Can be called after opening without
    /// `load_index` to lazily load the index. Also recalculates the data pack
    /// writer target from the loaded index.
    pub fn load_chunk_index(&mut self) -> Result<()> {
        let synced = self.sync_index(None, true)?;
        self.install_synced_index(synced);
        Ok(())
    }

    /// Load the index for the snapshot view already captured at open.
    pub(crate) fn load_chunk_index_for_snapshot_view(&mut self) -> Result<()> {
        // Open reads the head after listing snapshots, so it covers the
        // whole view. Sync re-reads it if a fold has removed an object.
        let synced = self.sync_index(self.index_head.clone(), true)?;
        self.install_synced_index(synced);
        Ok(())
    }

    /// Load the chunk index from storage, ignoring the local full index
    /// cache. Use this for operations like `check` that must verify what's
    /// actually in the remote repository: every object of the chain is
    /// fetched and verified. The head is kept for later writes.
    pub fn load_chunk_index_uncached(&mut self) -> Result<()> {
        let synced = self.sync_index(None, false)?;
        self.install_synced_index(synced);
        Ok(())
    }

    /// Load the chunk index as storage holds it now. For maintenance
    /// operations that hold the repository lock, so nothing can change it
    /// until the lock is released. Replaces any index loaded at open.
    pub(crate) fn load_chunk_index_under_lock(&mut self) -> Result<()> {
        debug_assert!(self.write_session.is_none() && !self.index_dirty);
        let start = std::time::Instant::now();
        let synced = self.sync_index(None, true)?;
        let source = synced.source;
        self.install_synced_index(synced);
        debug!(
            ?source,
            generation = self.index_generation,
            entries = self.chunk_index.len(),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "index loaded under maintenance lock"
        );
        Ok(())
    }

    /// Bring the local full index cache to `head`, or to
    /// a freshly read head. `use_local: false` ignores and leaves alone the
    /// local cache (the result lives in a temp file), so every object of the
    /// chain is fetched and verified.
    pub(super) fn sync_index(
        &mut self,
        head: Option<IndexHead>,
        use_local: bool,
    ) -> Result<SyncedIndex> {
        let head = match head {
            Some(head) => head,
            None => index_store::fetch_head(self.storage.as_ref(), self.crypto.as_ref())?,
        };
        let path = use_local
            .then(|| {
                dedup_cache::full_index_cache_path(
                    &self.config.id,
                    self.cache_dir_override.as_deref(),
                )
            })
            .flatten();
        let location = CacheLocation {
            path: path.as_deref(),
            scratch: self.index_scratch.take(),
            use_local,
        };
        let start = std::time::Instant::now();
        let synced = index_store::sync_full_cache(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            head,
            location,
            self.request_width(),
        )
        .map_err(|e| match e {
            VykarError::InvalidFormat(msg) => VykarError::InvalidFormat(format!(
                "{msg}; `vykar check --repair` rebuilds the index from what can still be \
                 read and may remove snapshots whose data it then cannot locate (run it \
                 with `--dry-run` first to see which)"
            )),
            e => e,
        })?;
        debug!(
            source = ?synced.source,
            entries = synced.head.entries,
            segments = synced.head.segments.len(),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "index cache synced to head"
        );
        Ok(synced)
    }

    /// Make a synced full cache the in-memory index.
    pub(super) fn install_synced_index(&mut self, synced: SyncedIndex) {
        self.chunk_index = dedup_cache::chunk_index_from_full_cache(&synced.cache);
        drop(synced.cache);
        self.index_generation = dedup_cache::generation_for_state(&synced.head.state_hash);
        self.index_head = Some(synced.head);
        self.index_scratch = synced.scratch;
        self.local_caches_stale = false;
        self.rebase_pack_target_from_index();
    }

    /// Flush pending packs and wait for uploads, then drop the tiered dedup
    /// index and take the session's `IndexDelta` out for processing. The
    /// delta's positional bumps keep the dedup cache mapped until they are
    /// applied; everything else in the tiered index is released here.
    ///
    /// On the concurrent backup path Phase 1 has normally drained uploads
    /// already, so the flush here is a no-op safety net; for
    /// `apply_write_session` it is the real flush.
    ///
    /// Must only be called when `write_session` is `Some`. The session itself
    /// stays active: on failure the caller can still run `flush_on_abort()`,
    /// which does not need the structures taken here.
    fn flush_and_take_session_delta(&mut self) -> Result<Option<IndexDelta>> {
        self.flush_packs()?;

        let ws = self.ws_mut();
        ws.tiered_dedup.take();
        let delta = ws.index_delta.take();
        if delta.is_some() {
            ws.dedup_index = None;
        }
        Ok(delta)
    }

    /// Flush pending packs, wait for uploads, and apply the dedup delta from
    /// the active write session. Must only be called when `write_session` is `Some`.
    ///
    /// On success, the session's dedup structures and delta are consumed
    /// (moved into the chunk index or cache). On failure, the session remains
    /// active (not consumed) so the caller can invoke `flush_on_abort()` for
    /// best-effort journal persistence. Note that `tiered_dedup` and
    /// `index_delta` may already be taken out of the session at that point —
    /// `flush_on_abort` does not need them (it only seals packs, joins uploads,
    /// and writes the journal).
    /// Returns `true` when chunk_index hydration should be deferred to after
    /// persistence (reduces peak memory when incremental update or cache
    /// rebuild succeeds).
    fn apply_write_session(&mut self) -> Result<bool> {
        let delta = self.flush_and_take_session_delta()?;

        let mut deferred_index_load = false;

        if let Some(delta) = delta {
            if !delta.is_empty() {
                // The head as storage holds it now, from the local full
                // cache where it can be; the write then diffs against it.
                // Without a head (`check --repair` rebuilding the index) the
                // loaded index is the one to extend.
                if self.index_head.is_some() {
                    let synced = self.sync_index(None, true)?;
                    self.install_synced_index(synced);
                }
                delta.apply_to(&mut self.chunk_index);
                self.index_dirty = true;
            } else if self.rebuild_dedup_cache {
                // Empty delta: index unchanged, but caches may need rebuilding.
                // Try to rebuild caches from full_index_cache if available.
                let mut rebuilt_from_cache = false;
                let cd = self.cache_dir_override.as_deref();
                let generation = self.index_generation;
                if let Some(full) =
                    dedup_cache::MmapFullIndexCache::open(&self.config.id, generation, cd)
                {
                    let id = &self.config.id;
                    let dedup_ok =
                        dedup_cache::build_dedup_cache_from_full_cache(&full, id, cd).is_ok();
                    let restore_ok =
                        dedup_cache::build_restore_cache_from_full_cache(&full, id, cd).is_ok();
                    if dedup_ok && restore_ok {
                        self.rebuild_dedup_cache = false;
                        rebuilt_from_cache = true;
                        // Defer chunk_index hydration to reduce peak memory.
                        deferred_index_load = true;
                    } else {
                        warn!("cache rebuild from full_index_cache partially failed, falling back");
                    }
                }
                if !rebuilt_from_cache {
                    // No valid full cache — must reload full index for slow-path cache rebuild.
                    self.chunk_index = self.reload_full_index()?;
                }
            } else {
                // Empty delta, no cache rebuild needed.
                // Still must restore chunk_index for postcondition (deferred).
                deferred_index_load = true;
            }
        }

        Ok(deferred_index_load)
    }

    /// Save the chunk index back to storage.
    /// When a write session is active: flushes pending packs, applies dedup delta.
    /// When no session is active: just persists dirty index/file_cache.
    /// Only writes components that have been marked dirty.
    pub fn save_state(&mut self) -> Result<()> {
        let deferred_index_load = if self.write_session.is_some() {
            self.apply_write_session()?
        } else {
            false
        };

        // An index write derives `index_generation` from the new state, which
        // invalidates the local dedup cache, and leaves the local full cache
        // at that state.
        let persisted = self.index_dirty;
        if persisted {
            self.persist_segmented_index()?;
        }

        if self.rebuild_dedup_cache {
            self.rebuild_local_caches(persisted);
            self.rebuild_dedup_cache = false;
            self.local_caches_stale = false;
        } else if persisted {
            self.local_caches_stale = true;
        }

        // Save file cache before hydrating chunk_index to reduce peak memory.
        // Capture error instead of early-returning so we can hydrate first.
        let fc_result = self.save_file_cache_if_dirty();

        // Always hydrate chunk_index — postcondition: self.chunk_index is valid
        // on all exit paths (success and error).
        if deferred_index_load {
            // Try local full_index_cache first (fast, no storage round-trip),
            // fall back to reloading from remote storage if cache is unavailable.
            self.chunk_index = self
                .chunk_index_from_local_cache()
                .or_else(|_| self.reload_full_index())?;
        }

        // Now propagate any file cache save error
        fc_result?;

        // Consume the write session on success — all entries are now in the
        // persisted index. The pending_index file itself is deleted later by
        // clear_pending_index() from the backup command.
        self.write_session = None;

        Ok(())
    }

    /// Commit a concurrent backup session with progress reporting.
    ///
    /// 1. Flush packs and join uploads.
    /// 2. Take the delta from the write session.
    /// 3. Refresh snapshot list from storage.
    /// 4. Check snapshot name uniqueness against fresh list.
    /// 5. GET the head, merge the delta into the index, persist it.
    /// 6. Write `snapshots/<id>` to storage — **commit point**.
    /// 7. Update local manifest + snapshot cache.
    /// 8. Save file cache and consume write session.
    ///
    /// `on_commit_point` runs right after step 6 succeeded, before the
    /// local post-commit work of steps 7-8, so the caller can start remote
    /// cleanup that only a committed session may do (deregistering it,
    /// deleting its journal) while that work runs. No journal write of this
    /// session is in flight when it is called.
    pub fn commit_concurrent_session_with_progress(
        &mut self,
        snapshot_entry: manifest::SnapshotEntry,
        snapshot_packed: Vec<u8>,
        new_file_cache: &mut file_cache::FileCache,
        events: &dyn EventSink<CommitEvent>,
        on_commit_point: impl FnOnce(),
    ) -> Result<()> {
        // The reads that start the commit are independent of each other. The
        // index head (only fetched when there is a delta to merge) is fetched
        // on this thread while a helper thread refreshes the snapshot list
        // and then probes the session marker. All of these are reads under
        // the commit lock; every write still follows them. Results are judged
        // in the old order: probe, flush, snapshot list and name check, index.
        let session_id = self.ws().session_id.clone();
        let taken = self.flush_and_take_session_delta();
        let fetch_index = matches!(&taken, Ok(Some(delta)) if !delta.is_empty());
        let marker_key =
            (session_id != "default").then(|| super::lock::session_marker_key(&session_id));
        let storage = self.storage.as_ref();
        let crypto = self.crypto.as_ref();
        let repo_id = self.config.id.as_slice();
        let cache_dir = self.cache_dir_override.as_deref();
        let ctx = emit_stage(events, "refresh snapshots");
        let (side, prefetched) = std::thread::scope(|scope| {
            let side = taken.is_ok().then(|| {
                scope.spawn(move || {
                    // Strict I/O: fail on GET errors so a transient failure
                    // can't hide an existing snapshot name and allow a
                    // duplicate during commit.
                    let refresh = snapshot_cache::refresh_snapshot_cache(
                        storage, crypto, repo_id, cache_dir, true,
                    );
                    let marker = marker_key.map(|key| storage.exists(&key));
                    (refresh, marker)
                })
            });
            let prefetched = fetch_index.then(|| index_store::fetch_head(storage, crypto));
            (side.map(join_scoped), prefetched)
        });
        log_stage_elapsed(ctx);
        let (refreshed, marker) = match side {
            Some((refresh, marker)) => (Some(refresh), marker),
            None => (None, None),
        };

        // Session-existence probe. If the marker was reaped by maintenance
        // (e.g. because we missed too many heartbeats under extreme clock
        // skew), surface the same typed error the later reconcile path
        // would produce, so callers matching on `StaleChunksDuringCommit`
        // catch this path too. Inconclusive storage errors fall through —
        // the reconcile step is still a safety net.
        match marker {
            Some(Ok(false)) => {
                warn!(
                    session_id,
                    "session marker reaped before commit; retry the backup"
                );
                return Err(VykarError::StaleChunksDuringCommit);
            }
            Some(Err(e)) => {
                debug!(error = %e, "session-existence probe failed, proceeding");
            }
            Some(Ok(true)) | None => {}
        }
        let delta = taken?;
        if let Some(refresh) = refreshed {
            self.apply_snapshot_refresh(refresh?);
        }

        let deferred_chunk_index_hydrate =
            self.commit_prepare(&snapshot_entry, delta, prefetched, events)?;

        // ---- COMMIT POINT ----
        let ctx = emit_stage(events, "write snapshot");
        self.check_lock_fence()?;
        self.storage
            .put(&snapshot_entry.id.storage_key(), &snapshot_packed)?;
        log_stage_elapsed(ctx);
        // ---- after this point, no `?` may escape ----

        // A journal PUT landing after the caller deleted the journal would
        // resurrect it, so none may still be running.
        if let Some(ws) = self.write_session.as_mut() {
            ws.join_journal_write();
        }
        on_commit_point();

        // Manifest update is a post-commit in-memory op. Keeping it inline keeps
        // the ordering visible next to the PUT; commit_finalize handles the rest.
        self.manifest.timestamp = Utc::now();
        self.manifest.snapshots.push(snapshot_entry);
        // Keep the local snapshot list cache in step with the commit, so the
        // next open does not re-download this snapshot's blob. Best-effort:
        // the cache is pruned against `snapshots/` on every open.
        self.persist_snapshot_cache_from_manifest();

        self.commit_finalize(new_file_cache, deferred_chunk_index_hydrate, events);
        Ok(())
    }

    /// Pre-commit work up to (but not including) the snapshot PUT, once the
    /// caller has flushed packs, taken the session `delta`, refreshed the
    /// snapshot list (unreadable blobs are skipped — a garbage snapshot
    /// that can't be decrypted cannot conflict with a valid name) and, for
    /// a non-empty delta, fetched the index head (`prefetched`):
    ///   4. Enforce snapshot name uniqueness against the fresh list.
    ///   5. Merge the delta into the index at that head, persist it.
    ///
    /// Returns `true` when chunk_index hydration should be deferred to
    /// `commit_finalize`.
    fn commit_prepare(
        &mut self,
        snapshot_entry: &manifest::SnapshotEntry,
        delta: Option<IndexDelta>,
        prefetched: Option<Result<IndexHead>>,
        events: &dyn EventSink<CommitEvent>,
    ) -> Result<bool> {
        // 4. Check snapshot name uniqueness against fresh list.
        if self.manifest.find_snapshot(&snapshot_entry.name).is_some() {
            return Err(VykarError::SnapshotAlreadyExists(
                snapshot_entry.name.clone(),
            ));
        }

        // 5. Merge the delta into the index at the head, persist it.
        let mut deferred_chunk_index_hydrate = false;
        if let Some(delta) = delta {
            if !delta.is_empty() {
                let head = match prefetched {
                    Some(head) => head?,
                    None => index_store::fetch_head(self.storage.as_ref(), self.crypto.as_ref())?,
                };
                self.segmented_commit(head, delta, events)?;
            }
            // The local full cache is at the index state; the in-memory
            // index is rebuilt from it after the commit point.
            deferred_chunk_index_hydrate = true;
        }

        // Defensive: persist index if dirty but no delta (unreachable today
        // because backup always activates dedup mode, but guards future callers).
        if self.index_dirty {
            self.persist_segmented_index()?;
        }

        Ok(deferred_chunk_index_hydrate)
    }

    /// Bring the local full cache to `head` (normally
    /// already there), merge-walk the session's `delta` over it into the next
    /// state and its segment while the session's packs are verified, and
    /// publish. The snapshot PUT that follows remains the commit point.
    fn segmented_commit(
        &mut self,
        head: IndexHead,
        delta: IndexDelta,
        events: &dyn EventSink<CommitEvent>,
    ) -> Result<()> {
        let ctx = emit_stage(events, "fetch index");
        let synced = self.sync_index(Some(head), true)?;
        log_stage_elapsed(ctx);
        let SyncedIndex {
            head,
            cache,
            scratch,
            ..
        } = synced;
        self.index_scratch = scratch;
        let anchor = index_store::cache_anchor(self.full_cache_path().as_deref());

        let ctx = emit_stage(events, "reconcile");
        let pack_ids = Self::delta_pack_ids(&delta);
        let persisted_packs = self.persisted_pack_count();
        let width = self.request_width();
        let storage = self.storage.as_ref();
        let (verified, merged) = std::thread::scope(|scope| {
            let verify = scope.spawn(|| {
                super::pack_verify::verify_packs_present(storage, &pack_ids, persisted_packs, width)
            });
            let merged = segment::merge_delta_to_temp(&cache, &delta, &anchor);
            (join_scoped(verify), merged)
        });
        // Unmap the predecessor before its file is replaced (Windows).
        drop(cache);
        drop(delta);
        // A stale bump is the typed error callers retry on; report it first.
        let (diff, next) = merged?;
        verified?;
        log_stage_elapsed(ctx);

        let ctx = emit_stage(events, "write index");
        self.publish_index_state(Some(head), Some(diff), next)?;
        log_stage_elapsed(ctx);
        Ok(())
    }

    /// Post-commit best-effort work. The `-> ()` return type is load-bearing:
    /// it statically forbids `?` from escaping a path that runs after the
    /// snapshot has already been durably committed. All internal failures
    /// must be reported via `post_commit_warning`.
    fn commit_finalize(
        &mut self,
        new_file_cache: &mut file_cache::FileCache,
        deferred_chunk_index_hydrate: bool,
        events: &dyn EventSink<CommitEvent>,
    ) {
        // The commit deferred `chunk_index` hydration past the commit point.
        // It runs last (below): neither the file-cache save nor
        // `rebuild_local_caches(true)` reads `self.chunk_index`, so doing it
        // first would only keep the hydrated index alive next to the file
        // cache's serialization buffer and raise the commit's peak memory.

        // Merge active sections if any were produced (filesystem backup).
        // Dump-only runs produce no active sections and skip this block.
        let sections = new_file_cache.take_active_sections();
        if !sections.is_empty() {
            for (key, section) in sections {
                self.file_cache.merge_section(&key, section);
            }
            self.file_cache_dirty = true;
        }

        // Persist if dirty — covers both the merge above AND any prior
        // invalidation that set the dirty flag (e.g., stale sections removed
        // at backup start).
        let ctx = (std::time::Instant::now(), "save file cache");
        if let Err(e) = self.save_file_cache_if_dirty() {
            post_commit_warning(
                events,
                format!(
                    "snapshot was successfully committed, but saving the local file \
                     cache failed: {e}. The next backup run will fall back to a \
                     cold-start walk for any affected sources instead of using cached \
                     metadata."
                ),
            );
        }
        log_stage_elapsed(ctx);

        // Rebuild local caches if needed. When the index write already left
        // the full cache (deferred_chunk_index_hydrate), skip the redundant sort;
        // the derivation then streams the on-disk full_index_cache and does
        // not read `self.chunk_index`, which is not hydrated yet.
        let ctx = emit_stage(events, "rebuild local caches");
        if self.rebuild_dedup_cache {
            self.rebuild_local_caches(deferred_chunk_index_hydrate);
            self.rebuild_dedup_cache = false;
            self.local_caches_stale = false;
        }
        log_stage_elapsed(ctx);

        if deferred_chunk_index_hydrate {
            self.hydrate_deferred_chunk_index(events);
        }

        // Clean up recovered index journals from previous interrupted sessions.
        if let Some(ws) = self.write_session.as_mut() {
            ws.cleanup_recovered_indices(&*self.storage);
        }

        // Consume write session.
        self.write_session = None;
    }

    /// Restore `self.chunk_index` after a commit, which never built the
    /// in-memory index: from the merged local full_index_cache, falling back
    /// to the remote index.
    ///
    /// Best-effort: a failure here is non-fatal since the remote index is
    /// already committed; it leaves `chunk_index` stale and the next
    /// Repository operation calls `reload_full_index` and rehydrates from
    /// remote.
    fn hydrate_deferred_chunk_index(&mut self, events: &dyn EventSink<CommitEvent>) {
        match self.chunk_index_from_local_cache().or_else(|e| {
            warn!("local cache hydration failed ({e}), reloading from remote");
            self.reload_full_index()
        }) {
            Ok(idx) => {
                self.chunk_index = idx;
            }
            Err(e) => {
                post_commit_warning(
                    events,
                    format!(
                        "snapshot was successfully committed, but hydrating the \
                         in-memory chunk index from local cache and remote storage \
                         both failed: {e}. The on-disk chunk index is intact; the \
                         next repository operation will reload it."
                    ),
                );
            }
        }
    }

    /// The packs a delta's new entries live in.
    fn delta_pack_ids(delta: &IndexDelta) -> std::collections::HashSet<PackId> {
        delta.new_entries.iter().map(|e| e.pack_id).collect()
    }

    /// Packs already in the repository, a cost hint for `pack_lookup`.
    fn persisted_pack_count(&self) -> usize {
        self.write_session
            .as_ref()
            .map_or(0, |ws| ws.persisted_pack_count)
    }

    /// Rebuild the local full/dedup/restore caches after a maintenance
    /// operation persisted the index (see `local_caches_stale`). Local and
    /// best-effort, and safe without the repository lock: every cache writer
    /// renames a uniquely named temp file into place, and each cache carries
    /// the generation of the index it holds. A copy that a concurrent process
    /// outdates is a miss for anything that promotes it to the index, and at
    /// worst an older consistent view for the dedup and restore readers,
    /// which commit and read verification catch.
    /// Only refreshes caches this machine already keeps (an existing
    /// `full_index_cache`), and only while `chunk_index` is exactly the
    /// persisted index (`!index_dirty`, no write session).
    pub(crate) fn refresh_stale_local_caches(&mut self) {
        if !self.local_caches_stale || self.index_dirty || self.write_session.is_some() {
            return;
        }
        let cd = self.cache_dir_override.as_deref();
        if !dedup_cache::full_index_cache_path(&self.config.id, cd).is_some_and(|p| p.exists()) {
            return;
        }
        let start = std::time::Instant::now();
        // An index write leaves the full cache at the new state.
        self.rebuild_local_caches(true);
        self.local_caches_stale = false;
        debug!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            generation = self.index_generation,
            "rebuilt local caches after maintenance index write"
        );
    }

    /// Rebuild all local caches: full index cache (1 sort), then derive
    /// dedup + restore caches from it (O(n) streaming, no sort).
    ///
    /// When `full_cache_fresh` is true, the full index cache is already
    /// up-to-date (e.g. from an index write) and only the derivation
    /// step runs. The full cache is opened and verified once; both
    /// derivations stream from that mapping.
    fn rebuild_local_caches(&self, full_cache_fresh: bool) {
        let cd = self.cache_dir_override.as_deref();
        let generation = self.index_generation;
        if !full_cache_fresh
            && let Err(e) = dedup_cache::build_full_index_cache(
                &self.chunk_index,
                generation,
                &self.config.id,
                cd,
            )
        {
            warn!("failed to build full index cache: {e}");
        }
        let Some(full_path) = dedup_cache::full_index_cache_path(&self.config.id, cd) else {
            return;
        };
        let Some(full) = dedup_cache::MmapFullIndexCache::open_path(&full_path, generation) else {
            warn!(
                "failed to rebuild dedup and restore caches: full index cache not found or stale"
            );
            return;
        };
        if let Err(e) = dedup_cache::build_dedup_cache_from_full_cache(&full, &self.config.id, cd) {
            warn!("failed to rebuild dedup cache from full cache: {e}");
        }
        if let Err(e) = dedup_cache::build_restore_cache_from_full_cache(&full, &self.config.id, cd)
        {
            warn!("failed to rebuild restore cache from full cache: {e}");
        }
    }

    /// Persist the local snapshot list cache from the current in-memory
    /// manifest.
    ///
    /// Commit, delete and prune mutate `manifest.snapshots` in memory but do
    /// not touch the persisted `snapshot_list` cache; without this the cache
    /// only self-heals on the next `open()` (the remote-list retain drops
    /// removed entries, and added ones are re-downloaded). Calling this after
    /// a mutation keeps the local cache consistent immediately.
    ///
    /// Best-effort: failures are warned and swallowed, matching
    /// `refresh_snapshot_cache`'s save behavior — a stale cache is corrected on
    /// the next open and never affects correctness.
    pub fn persist_snapshot_cache_from_manifest(&self) {
        let mut cache = snapshot_cache::SnapshotListCache::default();
        for entry in &self.manifest.snapshots {
            cache.entries.insert(entry.id.to_hex(), entry.clone());
        }
        if let Err(e) = cache.save(
            &self.config.id,
            self.crypto.as_ref(),
            self.cache_dir_override.as_deref(),
        ) {
            warn!("failed to persist snapshot list cache after mutation: {e}");
        }
    }

    /// Re-list snapshots/ and rebuild the in-memory manifest.
    /// Used by concurrent session commit to get a fresh snapshot list.
    pub fn refresh_snapshot_list(&mut self) -> Result<()> {
        // Strict I/O: fail on GET errors so a transient failure can't hide an
        // existing snapshot name and allow a duplicate during commit.
        let refresh = snapshot_cache::refresh_snapshot_cache(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            &self.config.id,
            self.cache_dir_override.as_deref(),
            true, // strict_io: true — commit uniqueness check
        )?;
        self.apply_snapshot_refresh(refresh);
        Ok(())
    }

    /// Replace the in-memory manifest with a fresh snapshot listing.
    fn apply_snapshot_refresh(&mut self, refresh: snapshot_cache::SnapshotRefresh) {
        self.skipped_snapshots = refresh.skipped;
        self.manifest = Manifest::from_snapshot_entries(refresh.entries);
    }

    /// Reload the full chunk index from storage (always downloads from remote).
    fn reload_full_index(&self) -> Result<ChunkIndex> {
        Self::fetch_full_index(self.storage.as_ref(), self.crypto.as_ref()).map(|(_, i)| i)
    }

    /// Download and decode the full chunk index + generation using only the
    /// storage and crypto handles, for readers that cannot borrow the
    /// repository. Does not touch the repository's own index or local
    /// caches: the index is built from its base and segments in a temp file.
    pub(crate) fn fetch_full_index(
        storage: &dyn vykar_storage::StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<(u64, ChunkIndex)> {
        let head = index_store::fetch_head(storage, crypto)?;
        let location = CacheLocation {
            path: None,
            scratch: None,
            use_local: false,
        };
        let width = storage.profile().request_width.max(1);
        let synced = index_store::sync_full_cache(storage, crypto, head, location, width)?;
        Ok((
            dedup_cache::generation_for_state(&synced.head.state_hash),
            dedup_cache::chunk_index_from_full_cache(&synced.cache),
        ))
    }
}
