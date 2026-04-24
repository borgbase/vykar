use tracing::{debug, warn};

use super::pack::{compute_data_pack_target, compute_tree_pack_target, PackType, PackWriter};
use super::write_session::{self, WriteSessionState};
use super::Repository;
use crate::index::dedup_cache::{self, TieredDedupIndex};
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
        let mut ws = WriteSessionState::new(data_target, tree_target, 2);
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
    pub(super) fn cap_pending_uploads(&mut self) -> Result<()> {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .cap_pending_uploads(&*self.storage, &*self.crypto)
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
    /// `index_generation`. On success: builds an xor filter, drops the full
    /// `ChunkIndex`, and routes all lookups through the three-tier structure
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
            let tiered = TieredDedupIndex::new(mmap_cache);
            debug!(?tiered, "tiered dedup mode: using mmap cache");
            // Drop the full index to reclaim memory.
            self.chunk_index = ChunkIndex::new();
            let ws = self.write_session.as_mut().unwrap();
            ws.tiered_dedup = Some(tiered);
            ws.index_delta = Some(IndexDelta::new());
        } else {
            debug!("tiered dedup mode: no valid cache, falling back to DedupIndex");
            self.enable_dedup_mode();
        }
    }

    /// Return the pre-built xor filter from whichever dedup mode is active.
    /// Returns `None` when no dedup mode or no write session is active.
    pub fn dedup_filter(&self) -> Option<std::sync::Arc<xorf::Xor8>> {
        let ws = self.write_session.as_ref()?;
        if let Some(ref tiered) = ws.tiered_dedup {
            return tiered.xor_filter();
        }
        if let Some(ref dedup) = ws.dedup_index {
            return dedup.xor_filter();
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
        if self
            .write_session
            .as_ref()
            .unwrap()
            .data_pack_writer
            .has_pending()
        {
            if let Err(e) = self.flush_writer_async(PackType::Data) {
                warn!("flush_on_abort: failed to seal data pack: {e}");
            }
        }
        if self
            .write_session
            .as_ref()
            .unwrap()
            .tree_pack_writer
            .has_pending()
        {
            if let Err(e) = self.flush_writer_async(PackType::Tree) {
                warn!("flush_on_abort: failed to seal tree pack: {e}");
            }
        }

        // Join all in-flight upload threads so packs land on storage.
        let ws = self.write_session.as_mut().unwrap();
        for handle in ws.pending_uploads.drain(..) {
            match handle
                .join()
                .map_err(|_| VykarError::Other("pack upload thread panicked".into()))
                .and_then(|r| r)
            {
                Ok(()) => {}
                Err(e) => warn!("flush_on_abort: upload thread failed: {e}"),
            }
        }

        // Write final pending_index so next run can recover.
        self.write_session
            .as_mut()
            .unwrap()
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
            .as_ref()
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
            ws.rollback_tracker = None;
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

        // 4. Remove tracked pack IDs from pending journal.
        for pack_id in &tracker.journal_pack_ids {
            ws.pending_journal.remove_pack(pack_id);
        }

        // 5. Reset data pack writer (discards any partial pack buffer).
        ws.data_pack_writer = PackWriter::new(PackType::Data, tracker.data_pack_target_size);
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
}
