use chrono::Utc;
use tracing::{debug, warn};

use super::Repository;
use super::control::{CommitEvent, EventSink};
use super::file_cache;
use super::index_store::{self, CacheLocation, SyncedIndex};
use super::manifest::{self, Manifest};
use super::snapshot_cache;
use super::write_session::SessionView;
use crate::index::head::{IndexHead, SegmentKind};
use crate::index::segment_v2::{Location, SegmentBody};
use crate::index::table::IndexState;
use crate::index::{IndexDelta, LiveIndex};
#[cfg(test)]
use std::sync::Arc;
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::ChunkId;
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
    /// a local chunk table to it. Can be called after opening without
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

    /// Load the chunk index from storage, ignoring the local chunk tables.
    /// Use this for operations like `check` that must verify what's
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

    /// Bring an index to `head`, or to a freshly read head: the
    /// repository's own index first, then the local files of the cache dir.
    /// `use_local: false` ignores and leaves alone the local files (the
    /// table lives in a temp file), so every object the head names is
    /// fetched and verified.
    pub(super) fn sync_index(
        &mut self,
        head: Option<IndexHead>,
        use_local: bool,
    ) -> Result<SyncedIndex> {
        let head = match head {
            Some(head) => head,
            None => index_store::fetch_head(self.storage.as_ref(), self.crypto.as_ref())?,
        };
        let dir = use_local.then(|| self.cache_dir()).flatten();
        let location = CacheLocation {
            dir: dir.as_deref(),
            live: use_local.then_some(&self.chunk_index),
            use_local,
        };
        let start = std::time::Instant::now();
        let synced = index_store::sync_index(
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
            entries = synced.index.len(),
            segments = synced.head.segments.len(),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "index cache synced to head"
        );
        Ok(synced)
    }

    /// Make a synced index the in-memory index.
    pub(super) fn install_synced_index(&mut self, synced: SyncedIndex) {
        self.chunk_index = synced.index;
        self.index_generation = synced.head.generation;
        self.index_head = Some(synced.head);
        self.rebase_pack_target_from_index();
    }

    /// Flush pending packs and wait for uploads, then take the session's
    /// `IndexDelta` (with the trailer lengths of the session's packs) and
    /// its view out for processing, and drop its lookup state. The delta's
    /// positional reuse record keeps the session's table mapped until it is
    /// checked.
    ///
    /// On the concurrent backup path Phase 1 has normally drained uploads
    /// already, so the flush here is a no-op safety net; for
    /// `apply_write_session` it is the real flush.
    ///
    /// Must only be called when `write_session` is `Some`. The session itself
    /// stays active: on failure the caller can still run `flush_on_abort()`,
    /// which does not need the structures taken here.
    fn flush_and_take_session_delta(
        &mut self,
    ) -> Result<(Option<IndexDelta>, Option<SessionView>)> {
        self.flush_packs()?;

        let ws = self.ws_mut();
        let mut delta = ws.index_delta.take();
        if let Some(delta) = delta.as_mut() {
            delta.pack_trailers = std::mem::take(&mut ws.pack_trailers);
        }
        let view = ws.view.take();
        ws.session_new = Default::default();
        Ok((delta, view))
    }

    /// Flush pending packs, wait for uploads, and apply the session's delta
    /// to the in-memory index. Must only be called when `write_session` is
    /// `Some`.
    ///
    /// The delta is applied to the index as loaded: the write that follows
    /// refuses a head that moved since. On failure, the session remains
    /// active (not consumed) so the caller can invoke `flush_on_abort()` for
    /// best-effort journal persistence; the delta may already be taken out
    /// of the session at that point, which `flush_on_abort` does not need.
    fn apply_write_session(&mut self) -> Result<()> {
        if let (Some(delta), _) = self.flush_and_take_session_delta()?
            && !delta.is_empty()
        {
            delta.apply_to(&mut self.chunk_index);
            self.index_dirty = true;
        }
        Ok(())
    }

    /// Save the chunk index back to storage.
    /// When a write session is active: flushes pending packs, applies its delta.
    /// When no session is active: just persists dirty index/file_cache.
    /// Only writes components that have been marked dirty.
    pub fn save_state(&mut self) -> Result<()> {
        if self.write_session.is_some() {
            self.apply_write_session()?;
        }

        // An index write leaves the repository's table at the new state.
        if self.index_dirty {
            self.persist_segmented_index()?;
        }

        self.save_file_cache_if_dirty()?;

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
    /// 5. GET the head, sync to it, write the session's additions as a
    ///    backup segment (re-checking reused chunks when the head changed in
    ///    a way that can have removed entries).
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
        let fetch_index = matches!(&taken, Ok((Some(delta), _)) if !delta.is_empty());
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
        // skew), surface the same typed error the later reuse check
        // would produce, so callers matching on `StaleChunksDuringCommit`
        // catch this path too. Inconclusive storage errors fall through —
        // the reuse check is still a safety net.
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
        let (delta, view) = taken?;
        if let Some(refresh) = refreshed {
            self.apply_snapshot_refresh(refresh?);
        }

        self.commit_prepare(&snapshot_entry, delta, view, prefetched, events)?;

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

        self.commit_finalize(new_file_cache, events);
        Ok(())
    }

    /// Pre-commit work up to (but not including) the snapshot PUT, once the
    /// caller has flushed packs, taken the session `delta`, refreshed the
    /// snapshot list (unreadable blobs are skipped — a garbage snapshot
    /// that can't be decrypted cannot conflict with a valid name) and, for
    /// a non-empty delta, fetched the index head (`prefetched`):
    ///   4. Enforce snapshot name uniqueness against the fresh list.
    ///   5. Write the delta's additions at that head.
    fn commit_prepare(
        &mut self,
        snapshot_entry: &manifest::SnapshotEntry,
        delta: Option<IndexDelta>,
        view: Option<SessionView>,
        prefetched: Option<Result<IndexHead>>,
        events: &dyn EventSink<CommitEvent>,
    ) -> Result<()> {
        // 4. Check snapshot name uniqueness against fresh list.
        if self.manifest.find_snapshot(&snapshot_entry.name).is_some() {
            return Err(VykarError::SnapshotAlreadyExists(
                snapshot_entry.name.clone(),
            ));
        }

        // 5. Write the delta's additions at the head. The repository's
        // index becomes the head's, with the new segment applied.
        if let Some(delta) = delta
            && !delta.is_empty()
        {
            let head = match prefetched {
                Some(head) => head?,
                None => index_store::fetch_head(self.storage.as_ref(), self.crypto.as_ref())?,
            };
            self.segmented_commit(head, delta, view, events)?;
        }

        // Defensive: persist index if dirty but no delta (unreachable today
        // because a backup session records every change in its delta, but
        // guards future callers).
        if self.index_dirty {
            self.persist_segmented_index()?;
        }

        Ok(())
    }

    /// Bring the index to `head` (normally the repository's own, already
    /// there), check the reused chunks when the head moved in a way that
    /// can have removed entries, and write the session's new chunks as one
    /// backup segment while the session's packs are verified. The snapshot
    /// PUT that follows remains the commit point.
    fn segmented_commit(
        &mut self,
        head: IndexHead,
        delta: IndexDelta,
        view: Option<SessionView>,
        events: &dyn EventSink<CommitEvent>,
    ) -> Result<()> {
        let ctx = emit_stage(events, "fetch index");
        let synced = self.sync_index(Some(head), true)?;
        log_stage_elapsed(ctx);
        let recheck = needs_recheck(view.as_ref().map(|v| (&v.state, v.sweep_gen)), &synced.head);
        // Nothing else may hold the view's applied layer when the segment
        // is applied (`Arc::make_mut` would copy it).
        drop(view);

        let ctx = emit_stage(events, "check reuse");
        // The additions: one per chunk (its first location), minus chunks
        // the synced index already holds (another session committed them
        // first; this session's copies are dead bytes).
        let additions = commit_additions(&delta, &synced.index);
        let pack_ids: std::collections::HashSet<PackId> =
            additions.iter().map(|(_, l)| l.pack_id).collect();
        let persisted_packs = self.persisted_pack_count();
        let width = self.request_width();
        let storage = self.storage.as_ref();
        let (verified, reused) = std::thread::scope(|scope| {
            let verify = scope.spawn(|| {
                super::pack_verify::verify_packs_present(storage, &pack_ids, persisted_packs, width)
            });
            let reused = if recheck {
                check_reused_chunks(&delta, &synced.index)
            } else {
                debug!(
                    "index head unchanged since the session began, except for backup segments; reused chunks not re-checked"
                );
                Ok(())
            };
            (join_scoped(verify), reused)
        });
        // A stale chunk is the typed error callers retry on; report it first.
        reused?;
        verified?;
        let mut pack_trailers: Vec<(PackId, u32)> = Vec::with_capacity(pack_ids.len());
        for pack in &pack_ids {
            let trailer_len = delta.pack_trailers.get(pack).copied().ok_or_else(|| {
                VykarError::Other(format!(
                    "BUG: the backup commit does not know the trailer length of pack {pack}"
                ))
            })?;
            pack_trailers.push((*pack, trailer_len));
        }
        pack_trailers.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        drop(delta);
        log_stage_elapsed(ctx);

        let ctx = emit_stage(events, "write index");
        let SyncedIndex { head, .. } = &synced;
        let head = head.clone();
        self.install_synced_index(synced);
        if additions.is_empty() {
            debug!("backup added no chunks; no index write");
            log_stage_elapsed(ctx);
            return Ok(());
        }
        let seg = SegmentBody {
            kind: SegmentKind::Backup,
            additions,
            removals: Vec::new(),
            relocations: Vec::new(),
            pack_trailers,
        };
        self.publish_index_state(Some(head), Some(seg), true)?;
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
        events: &dyn EventSink<CommitEvent>,
    ) {
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

        // Clean up recovered index journals from previous interrupted sessions.
        if let Some(ws) = self.write_session.as_mut() {
            ws.cleanup_recovered_indices(&*self.storage);
        }

        // Consume write session.
        self.write_session = None;
    }

    /// Packs already in the repository, a cost hint for `pack_lookup`.
    fn persisted_pack_count(&self) -> usize {
        self.write_session
            .as_ref()
            .map_or(0, |ws| ws.persisted_pack_count)
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

    /// Download the index at a freshly read head (its base into a temp-file
    /// table, its segments applied), using only the storage and crypto
    /// handles, for readers that cannot borrow the repository. Does not touch
    /// the repository's own index or the local files.
    pub(crate) fn fetch_full_index(
        storage: &dyn vykar_storage::StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<LiveIndex> {
        let head = index_store::fetch_head(storage, crypto)?;
        let location = CacheLocation {
            dir: None,
            live: None,
            use_local: false,
        };
        let width = storage.profile().request_width.max(1);
        let synced = index_store::sync_index(storage, crypto, head, location, width)?;
        Ok(synced.index)
    }
}

/// Whether entries a session reused can have been removed between the head
/// its view was built from (`from`: the view's objects and that head's
/// `sweep.gen`) and `now`: no view; another `sweep.gen`; another base; a
/// segment list that does not start with the view's; or any later segment
/// that is not a backup segment (backup segments only add).
pub(crate) fn needs_recheck(from: Option<(&IndexState, u64)>, now: &IndexHead) -> bool {
    let Some((state, sweep_gen)) = from else {
        return true;
    };
    if now.sweep.generation != sweep_gen
        || now.base.as_ref().map(|b| b.name) != state.base
        || now.segments.len() < state.segments.len()
    {
        return true;
    }
    let (old, new) = now.segments.split_at(state.segments.len());
    !old.iter()
        .map(|s| s.name)
        .eq(state.segments.iter().copied())
        || new.iter().any(|s| s.kind != SegmentKind::Backup)
}

/// The delta's new entries in chunk-ID order, one per chunk (the first
/// location recorded), without the chunks `index` already holds.
fn commit_additions(delta: &IndexDelta, index: &LiveIndex) -> Vec<(ChunkId, Location)> {
    let mut seen = std::collections::HashSet::with_capacity(delta.new_entries.len());
    let mut out: Vec<(ChunkId, Location)> = delta
        .new_entries
        .iter()
        .filter(|e| seen.insert(e.chunk_id))
        .filter(|e| !index.contains(&e.chunk_id))
        .map(|e| {
            (
                e.chunk_id,
                Location {
                    pack_id: e.pack_id,
                    pack_offset: e.pack_offset,
                    stored_size: e.stored_size,
                },
            )
        })
        .collect();
    out.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    out
}

/// Every chunk the session reused must be in `index` or among the delta's
/// new entries; else [`VykarError::StaleChunksDuringCommit`].
fn check_reused_chunks(delta: &IndexDelta, index: &LiveIndex) -> Result<()> {
    let new: std::collections::HashSet<ChunkId> =
        delta.new_entries.iter().map(|e| e.chunk_id).collect();
    if delta
        .reused
        .iter()
        .any(|id| !index.contains(&id) && !new.contains(&id))
    {
        return Err(VykarError::StaleChunksDuringCommit);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::head::{BaseRef, ObjectName, SegmentRef};
    use crate::index::table::IndexState;

    fn name(b: u8) -> ObjectName {
        ObjectName([b; 32])
    }

    fn seg(b: u8, kind: SegmentKind) -> SegmentRef {
        SegmentRef {
            name: name(b),
            size: 1,
            kind,
            entries: 1,
        }
    }

    fn head(base: Option<u8>, segments: Vec<SegmentRef>, sweep: u64) -> IndexHead {
        let mut h = IndexHead::initial();
        h.generation = 10;
        h.base = base.map(|b| BaseRef {
            name: name(b),
            size: 1,
            entries: 1,
        });
        h.segments = segments;
        h.sweep.generation = sweep;
        h
    }

    /// BK-3: when reused chunks are checked again at commit.
    #[test]
    fn needs_recheck_matrix() {
        let from = head(Some(1), vec![seg(2, SegmentKind::Backup)], 3);
        let state = IndexState::of_head(&from);
        let view = Some((&state, 3));
        assert!(!needs_recheck(view, &from), "same head");
        let mut more = from.clone();
        more.segments.push(seg(3, SegmentKind::Backup));
        more.segments.push(seg(4, SegmentKind::Backup));
        assert!(!needs_recheck(view, &more), "only new backup segments");
        let mut swept = from.clone();
        swept.sweep.generation = 9;
        assert!(needs_recheck(view, &swept), "new sweep.gen");
        let rebased = head(Some(7), vec![seg(2, SegmentKind::Backup)], 3);
        assert!(needs_recheck(view, &rebased), "new base");
        let mut maint = from.clone();
        maint.segments.push(seg(3, SegmentKind::Maintenance));
        assert!(needs_recheck(view, &maint), "a new maintenance segment");
        let other = head(Some(1), vec![seg(5, SegmentKind::Backup)], 3);
        assert!(needs_recheck(view, &other), "segments not a prefix");
        let shorter = head(Some(1), vec![], 3);
        assert!(needs_recheck(view, &shorter), "segments dropped");
        assert!(needs_recheck(None, &from), "no view");
    }

    /// BK-4 (keyed and positional reuse): a reused chunk missing from the
    /// synced index is stale unless the session added it itself.
    #[test]
    fn reused_chunks_missing_from_the_index_are_stale() {
        let id = |b: u8| ChunkId::from_bytes([b; 32]);
        let pack = PackId::from_bytes([9; 32]);
        let mut session_start = crate::index::ChunkIndex::new();
        session_start.add(id(1), 10, pack, 0);
        session_start.add(id(2), 10, pack, 20);
        let table = crate::testutil::base_table_of(&session_start);
        let mut now = LiveIndex::from_table(Arc::clone(&table));

        let mut keyed = IndexDelta::new();
        keyed.reused.insert_key(&id(1));
        let mut positional = IndexDelta::with_table(Arc::clone(&table));
        let (p, _) = table.find(&id(2)).unwrap();
        positional.reused.insert_position(&id(2), p);
        assert!(
            positional
                .reused
                .positional()
                .is_some_and(|r| !r.is_empty())
        );
        assert!(check_reused_chunks(&keyed, &now).is_ok());
        assert!(check_reused_chunks(&positional, &now).is_ok());

        now.remove(&id(1));
        now.remove(&id(2));
        for delta in [&keyed, &positional] {
            assert!(matches!(
                check_reused_chunks(delta, &now),
                Err(VykarError::StaleChunksDuringCommit)
            ));
        }
        // A chunk the session itself stored again is not stale.
        keyed.add_new_entry(id(1), 10, pack, 40);
        assert!(check_reused_chunks(&keyed, &now).is_ok());
        // The additions leave out what the index holds.
        let mut delta = IndexDelta::new();
        delta.add_new_entry(id(2), 10, pack, 0);
        delta.add_new_entry(id(3), 10, pack, 60);
        delta.add_new_entry(id(3), 10, pack, 80);
        let fresh = LiveIndex::from_table(Arc::clone(&table));
        let adds = commit_additions(&delta, &fresh);
        // CM-2: reuse a rolled-back scope recorded is gone from the record,
        // so a chunk removed since does not fail the commit.
        let mut rolled = IndexDelta::with_table(Arc::clone(&table));
        let cp = rolled.checkpoint();
        rolled
            .reused
            .insert_position(&id(1), table.find(&id(1)).unwrap().0);
        rolled.reused.insert_key(&id(2));
        rolled.rollback(cp);
        assert!(rolled.is_empty());
        assert!(check_reused_chunks(&rolled, &now).is_ok());
        assert_eq!(adds.len(), 1);
        assert_eq!(
            (adds[0].0, adds[0].1.pack_offset),
            (id(3), 60),
            "the first location"
        );
    }
}
