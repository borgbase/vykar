use tracing::{debug, warn};

use super::pack::{compute_data_pack_target, compute_tree_pack_target, PackType, PackWriter};
use super::write_session::{self, WriteSessionState};
use super::Repository;
use crate::index::dedup_cache::{self, DedupPrefilter, TieredDedupIndex};
use crate::index::{ChunkIndex, DedupIndex, IndexDelta};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

impl Repository {
    /// Activate a write session for backup.
    ///
    /// Creates a fresh `WriteSessionState` with pack targets computed from the
    /// current chunk index and repo config. Must be called before any write-path
    /// methods (`store_chunk`, `flush_packs`, dedup modes, etc.).
    ///
    /// Returns an error if a session is already active (caller must `save_state()`
    /// or `flush_on_abort()` first).
    pub fn begin_write_session(&mut self) -> Result<()> {
        if self.write_session.is_some() {
            return Err(VykarError::Other("write session already active".into()));
        }
        let num_packs = self.chunk_index.count_distinct_packs();
        let data_target = compute_data_pack_target(
            num_packs,
            self.config.min_pack_size,
            self.config.max_pack_size,
        );
        let tree_target = compute_tree_pack_target(self.config.min_pack_size);
        let mut ws = WriteSessionState::new(data_target, tree_target, 2, self.content_hash());
        ws.persisted_pack_count = num_packs;
        self.write_session = Some(ws);
        Ok(())
    }

    /// Set the session ID on the active write session (for per-session pending_index).
    pub fn set_write_session_id(&mut self, session_id: String) {
        if let Some(ws) = self.write_session.as_mut() {
            ws.session_id = session_id;
        }
    }

    /// Recompute pack-target state from `self.chunk_index`.
    ///
    /// Sets `persisted_pack_count` from the index, resets `session_packs_flushed`,
    /// and updates the data pack writer's target size. Called after any operation
    /// that brings `chunk_index` in sync with persisted storage (load or save).
    /// No-op when no write session is active.
    pub(super) fn rebase_pack_target_from_index(&mut self) {
        let Some(ws) = self.write_session.as_mut() else {
            return;
        };
        // NOTE: count_distinct_packs() includes tree packs, which slightly
        // inflates the data pack target. Tree packs are a small fraction of
        // total packs (~1-2 per backup) so the effect is negligible (~2% via
        // sqrt scaling). A proper fix would require persisting pack type
        // metadata in the index or manifest.
        let num_packs = self.chunk_index.count_distinct_packs();
        ws.persisted_pack_count = num_packs;
        ws.session_packs_flushed = 0;
        let data_target = compute_data_pack_target(
            num_packs,
            self.config.min_pack_size,
            self.config.max_pack_size,
        );
        ws.data_pack_writer.set_target_size(data_target);
    }

    /// Apply backpressure to keep the number of in-flight uploads bounded.
    pub(super) fn cap_pending_uploads(&mut self, incoming: usize) -> Result<()> {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .cap_pending_uploads(&*self.storage, &*self.crypto, incoming)
    }

    /// Bound sealed-but-unwritten packs by bytes as well as by count
    /// (see `WriteSessionState::cap_pending_upload_bytes`).
    pub fn set_cap_pending_upload_bytes(&mut self, enabled: bool) {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .cap_pending_upload_bytes = enabled;
    }

    /// Stream data packs to the backend while they fill, where it supports
    /// that (see `WriteSessionState::stream_data_packs`).
    pub fn set_stream_data_packs(&mut self, enabled: bool) {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .stream_data_packs = enabled;
    }

    /// Set the maximum number of in-flight background pack uploads.
    pub fn set_max_in_flight_uploads(&mut self, n: usize) {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .max_in_flight_uploads = n.max(1);
    }

    /// Switch to dedup-only index mode to reduce memory during backup.
    ///
    /// Builds a lightweight `DedupIndex` (chunk_id → stored_size only) from the
    /// full `ChunkIndex`, then drops the full index to reclaim memory. All
    /// mutations are recorded in an `IndexDelta` and merged back at save time.
    ///
    /// For 10M chunks this reduces steady-state memory from ~800 MB to ~450 MB.
    pub fn enable_dedup_mode(&mut self) {
        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        if ws.dedup_index.is_some() {
            return; // already enabled
        }
        let dedup = DedupIndex::from_chunk_index(&self.chunk_index);
        // Drop the full index to reclaim memory
        self.chunk_index = ChunkIndex::new();
        ws.dedup_index = Some(dedup);
        ws.index_delta = Some(IndexDelta::new());
    }

    /// Switch to tiered dedup mode for minimal memory usage during backup.
    ///
    /// Tries to open a local mmap'd dedup cache validated against the manifest's
    /// `index_generation`. On success: drops the full `ChunkIndex`, builds an
    /// xor filter over the cache's keys, and routes all lookups through the three-tier structure
    /// (~12 MB RSS for 10M chunks instead of ~680 MB).
    ///
    /// On failure (no cache, stale generation, corrupt file): falls back to the
    /// existing `DedupIndex` HashMap path.
    pub fn enable_tiered_dedup_mode(&mut self) {
        {
            let ws = self
                .write_session
                .as_ref()
                .expect("no active write session");
            if ws.tiered_dedup.is_some() || ws.dedup_index.is_some() {
                return; // already in a dedup mode
            }
        }

        self.rebuild_dedup_cache = true;
        let generation = self.index_generation;
        if let Some(mmap_cache) = dedup_cache::MmapDedupCache::open(
            &self.config.id,
            generation,
            self.cache_dir_override.as_deref(),
        ) {
            // Drop the full index to reclaim memory *before* building the xor
            // filter: filter construction needs scratch space of the same
            // order as the index itself, and nothing on this path reads the
            // index again (the mmap cache now answers every lookup). The
            // fallback below still has the index because it only runs when
            // the cache failed to open.
            self.chunk_index = ChunkIndex::new();
            let tiered = TieredDedupIndex::new(mmap_cache);
            debug!(?tiered, "tiered dedup mode: using mmap cache");
            // Bumps for chunks already in the cache are recorded by position
            // in this same cache instance (`bump_ref_*` in chunks.rs).
            let index_delta =
                IndexDelta::with_cached_bumps(std::sync::Arc::clone(tiered.mmap_cache()));
            let ws = self
                .write_session
                .as_mut()
                .expect("write session active while enabling tiered dedup");
            ws.tiered_dedup = Some(tiered);
            ws.index_delta = Some(index_delta);
        } else {
            debug!("tiered dedup mode: no valid cache, falling back to DedupIndex");
            self.enable_dedup_mode();
        }
    }

    /// Return the worker-side prefilter for whichever dedup mode is active.
    /// Returns `None` when no dedup mode or no write session is active, or the
    /// mode has no xor filter.
    ///
    /// In tiered mode the prefilter also carries the immutable mmap tier, so
    /// workers can confirm filter hits themselves. The `DedupIndex` fallback
    /// only shares the filter: its map is owned and mutated by the session,
    /// so its hits stay unconfirmed until the consumer resolves them.
    pub(crate) fn dedup_prefilter(&self) -> Option<DedupPrefilter> {
        let ws = self.write_session.as_ref()?;
        if let Some(ref tiered) = ws.tiered_dedup {
            return tiered.prefilter();
        }
        if let Some(ref dedup) = ws.dedup_index {
            return dedup.xor_filter().map(DedupPrefilter::filter_only);
        }
        None
    }

    /// Check if a chunk exists in the index (works in normal, dedup, and tiered modes).
    /// Falls through to chunk_index when no write session is active.
    pub fn chunk_exists(&self, id: &ChunkId) -> bool {
        if let Some(ref ws) = self.write_session {
            if let Some(ref tiered) = ws.tiered_dedup {
                return tiered.contains(id);
            }
            if let Some(ref dedup) = ws.dedup_index {
                return dedup.contains(id);
            }
        }
        self.chunk_index.contains(id)
    }

    /// Best-effort cleanup after a failed backup or other operation.
    ///
    /// Seals any partial pack writers, waits for in-flight uploads to land,
    /// and writes the final `pending_index` journal so a subsequent run can
    /// recover. All errors are logged but never propagated.
    ///
    /// No-ops when no write session is active or there is nothing to clean up.
    pub fn flush_on_abort(&mut self) {
        let Some(ws) = self.write_session.as_ref() else {
            return;
        };
        let has_partial_packs =
            ws.data_pack_writer.has_pending() || ws.tree_pack_writer.has_pending();
        if ws.pending_uploads.is_empty() && ws.pending_journal.is_empty() && !has_partial_packs {
            return;
        }

        warn!("saving progress for next run\u{2026}");

        // Seal and flush any partial data/tree pack writers.
        // Sample both has_pending() flags before flushing either writer:
        // the data and tree pack writers are independent, so flushing one
        // can't change the other's pending state. This avoids reborrowing
        // `self.write_session` between the two if-checks.
        let flush_data = ws.data_pack_writer.has_pending();
        let flush_tree = ws.tree_pack_writer.has_pending();

        if flush_data {
            if let Err(e) = self.flush_writer_async(PackType::Data) {
                warn!("flush_on_abort: failed to seal data pack: {e}");
            }
        }
        if flush_tree {
            if let Err(e) = self.flush_writer_async(PackType::Tree) {
                warn!("flush_on_abort: failed to seal tree pack: {e}");
            }
        }

        // Join all in-flight upload threads so packs land on storage.
        let ws = self
            .write_session
            .as_mut()
            .expect("write session active while flushing abort state");
        for upload in std::mem::take(&mut ws.pending_uploads) {
            if let Err(e) = ws.finish_upload(upload) {
                warn!("flush_on_abort: upload thread failed: {e}");
            }
        }

        // Write final pending_index so next run can recover.
        self.write_session
            .as_mut()
            .expect("write session active while writing abort journal")
            .write_pending_index_best_effort(&*self.storage, &*self.crypto);

        // Clear the session so Drop doesn't fire the debug_assert.
        self.write_session = None;
    }

    /// Recover chunk→pack mappings from a previous interrupted session's
    /// `pending_index` file. Verifies each pack exists before adding entries.
    ///
    /// Must be called inside the repo lock, before `enable_tiered_dedup_mode()`.
    pub fn recover_pending_index(&mut self) -> Result<write_session::PendingIndexRecovery> {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .recover_pending_index(&*self.storage, &*self.crypto, &self.chunk_index)
    }

    /// Best-effort delete of the `pending_index` file from storage.
    /// Called from the backup command after `save_state()` succeeds.
    pub fn clear_pending_index(&self, session_id: &str) {
        WriteSessionState::clear_pending_index(&*self.storage, session_id);
    }

    // --- Rollback checkpoint API ---
    //
    // Used by (a) streaming command dumps and (b) per-file backup chunk
    // loops that must survive mid-read drift detection. Single-slot: the
    // two callers never overlap (the backup serial consumer arms and
    // commits before any dump runs, and vice versa).

    /// Begin a rollback checkpoint: flush any pending data pack, snapshot
    /// the current `IndexDelta` state, and arm the rollback tracker so all
    /// subsequent mutations can be undone if the guarded scope fails.
    ///
    /// Requires an active dedup mode (tiered or plain). The backup pipeline
    /// always enables one of these before any callsite, and the rollback
    /// machinery only tracks mutations that flow through `index_delta`.
    #[allow(
        clippy::panic_in_result_fn,
        reason = "asserts encode programmer invariants on internal state"
    )]
    pub(crate) fn begin_rollback_checkpoint(&mut self) -> Result<()> {
        {
            let ws = self
                .write_session
                .as_ref()
                .expect("no active write session");
            assert!(
                ws.rollback_tracker.is_none(),
                "begin_rollback_checkpoint called while another checkpoint is armed"
            );
            assert!(
                ws.tiered_dedup.is_some() || ws.dedup_index.is_some(),
                "begin_rollback_checkpoint requires an active dedup mode"
            );
        }

        // Force-flush the data pack writer to isolate the scope's data.
        let has_pending = self
            .write_session
            .as_ref()
            .expect("no active write session")
            .data_pack_writer
            .has_pending();
        if has_pending {
            self.flush_writer_async(PackType::Data)?;
        }

        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        let delta_checkpoint = ws
            .index_delta
            .as_mut()
            .expect("dedup mode implies index_delta is set")
            .checkpoint();
        let data_pack_target_size = ws.data_pack_writer.target_size();
        ws.rollback_tracker = Some(write_session::RollbackTracker {
            delta_checkpoint,
            dedup_inserts: Vec::new(),
            promoted_recovered: Vec::new(),
            journal_pack_ids: Vec::new(),
            data_pack_target_size,
        });
        Ok(())
    }

    /// Commit a rollback checkpoint: discard the tracker (scope succeeded).
    pub(crate) fn commit_rollback_checkpoint(&mut self) {
        if let Some(ws) = self.write_session.as_mut() {
            // Only discard the delta's undo log when a tracker was actually
            // armed (mirrors `begin_rollback_checkpoint`); leave the no-tracker
            // case untouched.
            if ws.rollback_tracker.take().is_some() {
                if let Some(ref mut delta) = ws.index_delta {
                    delta.discard_checkpoint();
                }
            }
        }
    }

    /// Roll back the active checkpoint: undo all index mutations that
    /// occurred since `begin_rollback_checkpoint()`. Packs already uploaded
    /// to storage become orphans cleaned by compact.
    pub(crate) fn abort_rollback_checkpoint(&mut self) {
        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        let Some(tracker) = ws.rollback_tracker.take() else {
            return;
        };

        // 1. Rollback IndexDelta (always Some when a checkpoint was armed).
        if let Some(ref mut delta) = ws.index_delta {
            delta.rollback(tracker.delta_checkpoint);
        }

        // 2. Remove dedup inserts from the active dedup structure.
        if let Some(ref mut tiered) = ws.tiered_dedup {
            for chunk_id in &tracker.dedup_inserts {
                tiered.remove(chunk_id);
            }
        } else if let Some(ref mut dedup) = ws.dedup_index {
            for chunk_id in &tracker.dedup_inserts {
                dedup.remove(chunk_id);
            }
        }

        // 3. Re-insert promoted recovered chunks.
        for (chunk_id, entry) in tracker.promoted_recovered {
            ws.recovered_chunks.insert(chunk_id, entry);
        }

        // 4. Remove tracked packs from the pending journal, and keep uploads
        // still in flight from journaling them when they complete.
        for pack_id in &tracker.journal_pack_ids {
            ws.pending_journal.remove_pack(pack_id);
        }
        let rolled_back: std::collections::HashSet<_> = tracker.journal_pack_ids.iter().collect();
        for upload in &mut ws.pending_uploads {
            if rolled_back.contains(&upload.pack_id) {
                upload.journal = false;
            }
        }

        // 5. Reset data pack writer (discards any partial pack buffer).
        // Carry the algorithm over from the writer being replaced rather than
        // re-deriving it — a reset must not be able to change a pack's name.
        let hash_algo = ws.data_pack_writer.hash_algorithm();
        ws.data_pack_writer =
            PackWriter::new(PackType::Data, tracker.data_pack_target_size, hash_algo);
    }

    /// Promote a recovered chunk into the active dedup structure and index delta.
    /// Returns the stored size if the chunk was in `recovered_chunks`, None otherwise.
    pub(super) fn promote_recovered_chunk(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        let (stored_size, index_modified) = self
            .write_session
            .as_mut()
            .expect("no active write session")
            .promote_recovered_chunk(chunk_id, &mut self.chunk_index)?;
        if index_modified {
            self.index_dirty = true;
        }
        Some(stored_size)
    }
}

#[cfg(test)]
impl Repository {
    /// Current data pack target size in bytes (for testing).
    pub(crate) fn data_pack_target(&self) -> usize {
        self.write_session
            .as_ref()
            .expect("no active write session")
            .data_pack_writer
            .target_size()
    }

    /// Enter tiered dedup mode over an explicit mmap cache (for testing).
    pub(crate) fn enable_tiered_dedup_mode_with(&mut self, cache: dedup_cache::MmapDedupCache) {
        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        let tiered = TieredDedupIndex::new(cache);
        ws.index_delta = Some(IndexDelta::with_cached_bumps(std::sync::Arc::clone(
            tiered.mmap_cache(),
        )));
        ws.tiered_dedup = Some(tiered);
    }

    /// Refcount bumps recorded in the index delta for `id`, by key or by
    /// dedup-cache position (for testing).
    pub(crate) fn delta_refcount_bump(&self, id: &ChunkId) -> u32 {
        let Some(delta) = self
            .write_session
            .as_ref()
            .and_then(|ws| ws.index_delta.as_ref())
        else {
            return 0;
        };
        let keyed = delta.refcount_bumps.get(id).copied().unwrap_or(0);
        let positional = delta.cached_bumps.as_ref().map_or(0, |cached| {
            cached
                .cache()
                .find_with_position(id)
                .map_or(0, |(position, _)| cached.count_at(position as usize))
        });
        keyed + positional
    }

    /// True if a rollback checkpoint is currently armed (for testing).
    pub(crate) fn rollback_tracker_armed(&self) -> bool {
        self.write_session
            .as_ref()
            .and_then(|ws| ws.rollback_tracker.as_ref())
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use crate::testutil::test_repo_plaintext;

    #[test]
    fn rollback_checkpoint_armed_and_committed_round_trips() {
        let mut repo = test_repo_plaintext();
        repo.enable_dedup_mode();
        assert!(!repo.rollback_tracker_armed());
        repo.begin_rollback_checkpoint().unwrap();
        assert!(repo.rollback_tracker_armed());
        repo.commit_rollback_checkpoint();
        assert!(!repo.rollback_tracker_armed());
    }

    #[test]
    fn rollback_checkpoint_rollback_clears_tracker() {
        let mut repo = test_repo_plaintext();
        repo.enable_dedup_mode();
        repo.begin_rollback_checkpoint().unwrap();
        repo.abort_rollback_checkpoint();
        assert!(!repo.rollback_tracker_armed());
    }

    #[test]
    #[should_panic(expected = "begin_rollback_checkpoint called while another checkpoint is armed")]
    fn cannot_begin_overlapping_rollback_checkpoint() {
        let mut repo = test_repo_plaintext();
        repo.enable_dedup_mode();
        repo.begin_rollback_checkpoint().unwrap();
        // Second begin must panic via the overlap assertion.
        let _ = repo.begin_rollback_checkpoint();
    }

    #[test]
    #[should_panic(expected = "begin_rollback_checkpoint requires an active dedup mode")]
    fn begin_rollback_checkpoint_without_dedup_mode_panics() {
        let mut repo = test_repo_plaintext();
        // No enable_dedup_mode call — must panic.
        let _ = repo.begin_rollback_checkpoint();
    }

    /// A finished upload is journaled on the next stored chunk, not only when
    /// the next pack is sealed.
    #[test]
    fn finished_upload_is_journaled_before_next_seal() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let mut repo = test_repo_plaintext();
        repo.enable_dedup_mode();
        repo.store_chunk(b"first pack chunk", Compression::None, PackType::Data)
            .unwrap();
        repo.flush_writer_async(PackType::Data).unwrap();
        let pack_id = repo.write_session.as_ref().unwrap().pending_uploads[0].pack_id;
        while !repo.write_session.as_ref().unwrap().pending_uploads[0]
            .handle
            .is_finished()
        {
            std::thread::yield_now();
        }

        repo.store_chunk(b"next chunk", Compression::None, PackType::Data)
            .unwrap();

        let ws = repo.write_session.as_ref().unwrap();
        assert!(ws.pending_uploads.is_empty());
        assert!(ws
            .pending_journal
            .to_wire()
            .iter()
            .any(|e| e.pack_id == pack_id));
        repo.write_session = None;
    }

    /// Files directly in `<root>/packs`, i.e. staging temp files.
    fn staging_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(root.join("packs"))
            .map(|rd| {
                rd.map(|e| e.unwrap().path())
                    .filter(|p| p.is_file())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// On a local backend a streamed data pack holds no heap, is published
    /// under its content hash by the upload thread, and is journaled only
    /// after that; tree packs stay buffered.
    #[test]
    fn streamed_data_pack_is_committed_then_journaled() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;
        use vykar_types::pack_id::PackId;

        let dir = tempfile::tempdir().unwrap();
        let mut repo = test_repo_plaintext();
        repo.storage = std::sync::Arc::new(
            vykar_storage::local_backend::LocalBackend::new(dir.path().to_str().unwrap()).unwrap(),
        );
        repo.enable_dedup_mode();
        repo.set_stream_data_packs(true);

        repo.store_chunk(b"streamed chunk", Compression::None, PackType::Data)
            .unwrap();
        repo.store_chunk(b"tree chunk", Compression::None, PackType::Tree)
            .unwrap();
        {
            let ws = repo.write_session.as_ref().unwrap();
            assert_eq!(ws.data_pack_writer.buffer_capacity(), 0);
            assert!(ws.tree_pack_writer.buffer_capacity() > 0);
        }
        assert_eq!(staging_files(dir.path()).len(), 1, "data pack is staged");

        repo.flush_writer_async(PackType::Data).unwrap();
        let (pack_id, bytes) = {
            let ws = repo.write_session.as_ref().unwrap();
            (ws.pending_uploads[0].pack_id, ws.pending_uploads[0].bytes)
        };
        assert_eq!(bytes, 0, "a staged pack counts no heap bytes");

        repo.flush_packs().unwrap();
        assert!(staging_files(dir.path()).is_empty());
        let stored = repo.storage.get(&pack_id.storage_key()).unwrap().unwrap();
        assert_eq!(PackId::compute(&stored, repo.content_hash()), pack_id);
        let ws = repo.write_session.as_ref().unwrap();
        assert!(ws
            .pending_journal
            .to_wire()
            .iter()
            .any(|e| e.pack_id == pack_id));
        repo.write_session = None;
    }

    /// Rolling back a checkpoint, or dropping the session, discards the
    /// partially written staged pack and leaves no temp file behind.
    #[test]
    fn discarded_staged_pack_leaves_no_temp_file() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let dir = tempfile::tempdir().unwrap();
        let mut repo = test_repo_plaintext();
        repo.storage = std::sync::Arc::new(
            vykar_storage::local_backend::LocalBackend::new(dir.path().to_str().unwrap()).unwrap(),
        );
        repo.enable_dedup_mode();
        repo.set_stream_data_packs(true);

        repo.begin_rollback_checkpoint().unwrap();
        repo.store_chunk(b"rolled back", Compression::None, PackType::Data)
            .unwrap();
        assert_eq!(staging_files(dir.path()).len(), 1);
        repo.abort_rollback_checkpoint();
        assert!(staging_files(dir.path()).is_empty());

        repo.store_chunk(b"abandoned", Compression::None, PackType::Data)
            .unwrap();
        assert_eq!(staging_files(dir.path()).len(), 1);
        repo.write_session = None;
        assert!(staging_files(dir.path()).is_empty());
    }

    /// A pack sealed inside a checkpoint whose upload finishes only after the
    /// rollback must not be journaled: its chunks were rolled back.
    #[test]
    fn rollback_keeps_in_flight_upload_out_of_journal() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let mut repo = test_repo_plaintext();
        repo.enable_dedup_mode();
        let (gated, tx) =
            crate::testutil::PackUploadBackend::gated(std::sync::Arc::clone(&repo.storage));
        repo.storage = std::sync::Arc::new(gated);

        repo.begin_rollback_checkpoint().unwrap();
        repo.store_chunk(b"rolled back chunk", Compression::None, PackType::Data)
            .unwrap();
        repo.flush_writer_async(PackType::Data).unwrap();
        let pack_id = {
            let ws = repo.write_session.as_ref().unwrap();
            assert_eq!(ws.pending_uploads.len(), 1);
            assert!(
                !ws.pending_uploads[0].handle.is_finished(),
                "upload is gated"
            );
            ws.pending_uploads[0].pack_id
        };

        repo.abort_rollback_checkpoint();
        tx.send(()).unwrap();
        repo.flush_packs().unwrap();

        let ws = repo.write_session.as_ref().unwrap();
        assert!(ws.pending_uploads.is_empty());
        assert!(
            ws.pending_journal
                .to_wire()
                .iter()
                .all(|e| e.pack_id != pack_id),
            "rolled-back pack must not be journaled"
        );
        assert!(
            repo.storage.exists(&pack_id.storage_key()).unwrap(),
            "the upload still completes; the pack is an orphan for compact"
        );
        repo.write_session = None;
    }
}
