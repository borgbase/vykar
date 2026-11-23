use tracing::warn;

use super::Repository;
use super::journal_recovery::PendingIndexRecovery;
use super::pack::{PackType, PackWriter, compute_data_pack_target, compute_tree_pack_target};
use super::write_session::{
    DEFAULT_SESSION_ID, RollbackTracker, SessionView, WriteMetricsSnapshot, WriteSessionState,
    active, active_mut,
};
use crate::index::IndexDelta;
use crate::index::table::DedupPrefilter;
use vykar_storage::BackendProfile;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

/// `max_in_flight_uploads` of `WriteSessionOptions::default()`.
const DEFAULT_MAX_IN_FLIGHT_UPLOADS: usize = 2;

/// How a write session runs. `Default` is what non-backup writers (repair,
/// tests) use.
#[derive(Debug, Clone)]
pub struct WriteSessionOptions {
    /// Names the session's `pending_index` journal.
    pub session_id: String,
    /// Bound on in-flight background pack uploads; a backup takes it from
    /// [`BackendProfile::transfer_width`]. The queue admits
    /// `UPLOAD_QUEUE_HEADROOM` more sealed packs before the producer blocks
    /// (see `UploadQueue::wait_below_count_cap`).
    pub max_in_flight_uploads: usize,
    /// Also bound sealed-but-unwritten packs by bytes
    /// (see `UploadQueue::cap_pending_upload_bytes`).
    pub cap_upload_bytes: bool,
    /// Stream data packs to the backend while they fill
    /// (see `WriteSessionState::stream_data_packs`).
    pub stream_data_packs: bool,
}

impl Default for WriteSessionOptions {
    fn default() -> Self {
        Self {
            session_id: DEFAULT_SESSION_ID.to_string(),
            max_in_flight_uploads: DEFAULT_MAX_IN_FLIGHT_UPLOADS,
            cap_upload_bytes: false,
            stream_data_packs: false,
        }
    }
}

impl WriteSessionOptions {
    /// Options for a backup against a backend with `profile`.
    pub fn for_backup(session_id: String, profile: BackendProfile) -> Self {
        Self {
            session_id,
            max_in_flight_uploads: profile.transfer_width,
            cap_upload_bytes: profile.local,
            // Data packs go straight to a temp file in the repository where the
            // backend can do that (local), instead of holding up to two pack
            // buffers (filling + sealed) on the heap.
            stream_data_packs: profile.staged_put,
        }
    }
}

impl Repository {
    /// The active write session. Panics when none is active.
    #[track_caller]
    pub(super) fn ws(&self) -> &WriteSessionState {
        active(&self.write_session)
    }

    /// The active write session, mutably. Panics when none is active.
    #[track_caller]
    pub(super) fn ws_mut(&mut self) -> &mut WriteSessionState {
        active_mut(&mut self.write_session)
    }

    /// Activate a write session.
    ///
    /// Creates a fresh `WriteSessionState` with pack targets computed from the
    /// current chunk index and repo config. Must be called before any write-path
    /// methods (`store_chunk`, `flush_packs`, etc.).
    ///
    /// Over an unedited index the session resolves existing chunks against
    /// the repository's index view as it is now (the table and the head's
    /// segments applied over it), and records reuse of table chunks by table
    /// position (see `ReusedChunks`), of segment additions by key; backup
    /// workers get a prefilter over the same view. Over an index with unsaved
    /// edits (repair) every lookup and every reuse goes through the live index
    /// by key, and there is no prefilter.
    ///
    /// Applies every field of `opts`.
    ///
    /// Returns an error if a session is already active (caller must `save_state()`
    /// or `flush_on_abort()` first).
    pub fn begin_write_session(&mut self, opts: &WriteSessionOptions) -> Result<()> {
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
        let mut ws = WriteSessionState::new(
            data_target,
            tree_target,
            opts.max_in_flight_uploads.max(1),
            self.content_hash(),
        );
        ws.uploads.cap_pending_upload_bytes = opts.cap_upload_bytes;
        ws.persisted_pack_count = num_packs;
        ws.session_id.clone_from(&opts.session_id);
        ws.stream_data_packs = opts.stream_data_packs;
        ws.existing_packs = Some(std::sync::Arc::new(
            super::existing_pack::ExistingPacks::new(self.host_env.clone()),
        ));
        ws.view = self.chunk_index.view().map(|view| SessionView {
            view,
            state: self.chunk_index.state().clone(),
            sweep_gen: self.index_head.as_ref().map_or(0, |h| h.sweep.generation),
        });
        // Without a head the view cannot be compared with the latest head
        // at commit: the commit re-checks every reused chunk.
        if self.index_head.is_none() {
            ws.view = None;
        }
        ws.index_delta = Some(match &ws.view {
            Some(v) => IndexDelta::with_table(std::sync::Arc::clone(v.view.table())),
            None => IndexDelta::new(),
        });
        self.write_session = Some(ws);
        Ok(())
    }

    /// The backup's session marker, pulled back by the age of every pack
    /// upload an append-only server refuses because the pack is stored
    /// already (`existing_pack`). No-op without an active write session.
    pub(crate) fn attach_session_marker(
        &mut self,
        marker: std::sync::Arc<super::lock::SessionMarker>,
    ) {
        if let Some(existing) = self
            .write_session
            .as_ref()
            .and_then(|ws| ws.existing_packs.as_ref())
        {
            existing.attach_marker(marker);
        }
    }

    /// Independent read-only requests worth issuing at once against this
    /// repository's backend (journal recovery's pack lookups, commit-time
    /// pack verification, item-stream range reads). At least 1.
    pub(super) fn request_width(&self) -> usize {
        self.storage.profile().request_width.max(1)
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
        active_mut(&mut self.write_session).cap_pending_uploads(
            &self.storage,
            &self.crypto,
            incoming,
        )
    }

    /// Pipeline budget to lend to the upload queue (see
    /// `WriteSessionState::upload_slack_bytes`); 0 without a write session.
    pub(crate) fn upload_slack_bytes(&self) -> usize {
        self.write_session
            .as_ref()
            .map_or(0, |ws| ws.upload_slack_bytes())
    }

    /// Snapshot of the active write session's diagnostic counters; `None` without one.
    pub(crate) fn write_metrics(&self) -> Option<WriteMetricsSnapshot> {
        self.write_session.as_ref().map(|ws| ws.metrics.snapshot())
    }

    /// The worker-side prefilter over the session's view: `None` without a
    /// write session or a view (an edited or unloaded index).
    pub(crate) fn dedup_prefilter(&self) -> Option<DedupPrefilter> {
        let ws = self.write_session.as_ref()?;
        ws.view
            .as_ref()
            .map(|v| DedupPrefilter::new(v.view.clone()))
    }

    /// Check if a chunk exists: stored earlier in the active session, or in
    /// the index.
    pub fn chunk_exists(&self, id: &ChunkId) -> bool {
        if let Some(ref ws) = self.write_session
            && ws.session_new.contains_key(id)
        {
            return true;
        }
        self.chunk_index.contains(id)
    }

    /// Wait for the session's background journal write, if any (the
    /// simulator's driver, after Phase 1). No-op without a write session.
    #[cfg(any(test, feature = "sim"))]
    pub(crate) fn join_journal_write(&mut self) {
        if let Some(ws) = self.write_session.as_mut() {
            ws.join_journal_write();
        }
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
        if ws.uploads.is_empty() && ws.journal.entries.is_empty() && !has_partial_packs {
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

        if flush_data && let Err(e) = self.flush_writer_async(PackType::Data) {
            warn!("flush_on_abort: failed to seal data pack: {e}");
        }
        if flush_tree && let Err(e) = self.flush_writer_async(PackType::Tree) {
            warn!("flush_on_abort: failed to seal tree pack: {e}");
        }

        // Join all in-flight upload threads so packs land on storage.
        let ws = self.ws_mut();
        for upload in ws.uploads.take_all() {
            if let Err(e) = ws.finish_upload(upload) {
                warn!("flush_on_abort: upload thread failed: {e}");
            }
        }

        // Write final pending_index so next run can recover.
        active_mut(&mut self.write_session)
            .write_pending_index_best_effort(&*self.storage, &*self.crypto);

        // Clear the session so Drop doesn't fire the debug_assert.
        self.write_session = None;
    }

    /// Recover chunk→pack mappings from a previous interrupted session's
    /// `pending_index` file. Verifies each pack exists before adding entries.
    ///
    /// Must be called inside the repo lock, before any chunk is stored.
    /// Pack lookups use the backend's request width
    /// (`BackendProfile::request_width`) and stop when `cancel` fires.
    pub fn recover_pending_index(
        &mut self,
        cancel: &super::control::CancelToken,
    ) -> Result<PendingIndexRecovery> {
        let width = self.request_width();
        active_mut(&mut self.write_session).recover_pending_index(
            &*self.storage,
            &*self.crypto,
            &self.chunk_index,
            width,
            self.host_env.clock.as_ref(),
            cancel,
        )
    }

    /// Best-effort delete of the `pending_index` file from storage.
    /// Called from the backup command after `save_state()` succeeds.
    pub fn clear_pending_index(&self, session_id: &str) {
        super::session_journal::clear_pending_index(&*self.storage, session_id);
    }

    // --- Rollback checkpoint API ---
    //
    // Used by (a) streaming command dumps and (b) per-file backup chunk
    // loops that must survive mid-read drift detection. Single-slot: the
    // two callers never overlap (the backup serial consumer arms and
    // commits before any dump runs, and vice versa).

    /// Begin a rollback checkpoint: flush any pending data pack, snapshot
    /// the current `IndexDelta` state, and arm the rollback tracker so all
    /// subsequent mutations can be undone if the guarded scope fails. Every
    /// mutation of a session flows through its `index_delta`.
    #[allow(
        clippy::panic_in_result_fn,
        reason = "asserts encode programmer invariants on internal state"
    )]
    pub(crate) fn begin_rollback_checkpoint(&mut self) -> Result<()> {
        {
            let ws = self.ws();
            assert!(
                ws.rollback_tracker.is_none(),
                "begin_rollback_checkpoint called while another checkpoint is armed"
            );
        }

        // Force-flush the data pack writer to isolate the scope's data.
        let has_pending = self.ws().data_pack_writer.has_pending();
        if has_pending {
            self.flush_writer_async(PackType::Data)?;
            self.ws_mut().metrics.counters.forced_data_seals += 1;
        }

        let ws = self.ws_mut();
        let delta_checkpoint = ws
            .index_delta
            .as_mut()
            .expect("an active session has an index delta")
            .checkpoint();
        let data_pack_target_size = ws.data_pack_writer.target_size();
        ws.rollback_tracker = Some(RollbackTracker {
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
            if ws.rollback_tracker.take().is_some()
                && let Some(ref mut delta) = ws.index_delta
            {
                delta.discard_checkpoint();
            }
        }
    }

    /// Roll back the active checkpoint: undo all index mutations that
    /// occurred since `begin_rollback_checkpoint()`. Packs already uploaded
    /// to storage become orphans cleaned by compact.
    pub(crate) fn abort_rollback_checkpoint(&mut self) {
        let ws = self.ws_mut();
        let Some(tracker) = ws.rollback_tracker.take() else {
            return;
        };

        // 1. Rollback IndexDelta (always Some when a checkpoint was armed).
        if let Some(ref mut delta) = ws.index_delta {
            delta.rollback(tracker.delta_checkpoint);
        }

        // 2. Forget the chunks the scope stored or promoted.
        for chunk_id in &tracker.dedup_inserts {
            ws.session_new.remove(chunk_id);
        }

        // 3. Re-insert promoted recovered chunks.
        for (chunk_id, entry) in tracker.promoted_recovered {
            ws.recovered_chunks.insert(chunk_id, entry);
        }

        // 4. Remove tracked packs from the pending journal, and keep uploads
        // still in flight from journaling them when they complete.
        for pack_id in &tracker.journal_pack_ids {
            ws.journal.entries.remove_pack(pack_id);
        }
        ws.uploads.exclude_from_journal(&tracker.journal_pack_ids);

        // 5. Reset data pack writer (discards any partial pack buffer).
        // Carry the algorithm over from the writer being replaced rather than
        // re-deriving it — a reset must not be able to change a pack's name.
        let hash_algo = ws.data_pack_writer.hash_algorithm();
        ws.data_pack_writer =
            PackWriter::new(PackType::Data, tracker.data_pack_target_size, hash_algo);
    }

    /// Promote a recovered chunk into the session and its index delta.
    /// Returns the stored size if the chunk was in `recovered_chunks`, None otherwise.
    pub(super) fn promote_recovered_chunk(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        active_mut(&mut self.write_session).promote_recovered_chunk(chunk_id)
    }
}

#[cfg(test)]
impl Repository {
    /// Current data pack target size in bytes (for testing).
    pub(crate) fn data_pack_target(&self) -> usize {
        self.ws().data_pack_writer.target_size()
    }

    /// Make `index` the repository's table and restart the active session's
    /// lookups and delta over it, as if the session had begun with it
    /// loaded (for testing).
    pub(crate) fn set_session_table_for_test(&mut self, index: &crate::index::ChunkIndex) {
        use std::sync::Arc;
        let table = crate::testutil::base_table_of(index);
        self.chunk_index = crate::index::LiveIndex::from_table(Arc::clone(&table));
        let view = SessionView {
            view: self
                .chunk_index
                .view()
                .expect("an unedited index has a view"),
            state: self.chunk_index.state().clone(),
            sweep_gen: self.index_head.as_ref().map_or(0, |h| h.sweep.generation),
        };
        let ws = self.ws_mut();
        ws.index_delta = Some(IndexDelta::with_table(Arc::clone(&table)));
        ws.view = Some(view);
    }

    /// The active session's index delta (for testing).
    pub(crate) fn write_session_delta_for_test(&self) -> &IndexDelta {
        self.ws().index_delta.as_ref().unwrap()
    }

    /// Whether the session's delta records `id` as reused, by key or by
    /// table position (for testing).
    pub(crate) fn delta_reuses(&self, id: &ChunkId) -> bool {
        self.write_session
            .as_ref()
            .and_then(|ws| ws.index_delta.as_ref())
            .is_some_and(|delta| delta.reused.contains(id))
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
    use super::WriteSessionOptions;
    use crate::testutil::test_repo_plaintext;
    use std::sync::Arc;
    use vykar_storage::{BackendProfile, InnerBackend, StorageBackend, delegate_storage_backend};

    #[test]
    fn for_backup_derives_from_profile() {
        let local = WriteSessionOptions::for_backup(
            "s".into(),
            BackendProfile {
                local: true,
                staged_put: true,
                request_width: 5,
                transfer_width: 7,
            },
        );
        assert_eq!(local.session_id, "s");
        assert_eq!(local.max_in_flight_uploads, 7);
        assert!(local.cap_upload_bytes);
        assert!(local.stream_data_packs);

        let remote = WriteSessionOptions::for_backup(
            "s".into(),
            BackendProfile {
                local: false,
                staged_put: false,
                request_width: 15,
                transfer_width: 3,
            },
        );
        assert_eq!(remote.max_in_flight_uploads, 3);
        assert!(!remote.cap_upload_bytes);
        assert!(!remote.stream_data_packs);
    }

    /// Upload limits are in force from `begin_write_session` on, so command
    /// dumps already run under them.
    #[test]
    fn begin_write_session_applies_every_option() {
        let mut repo = test_repo_plaintext();
        repo.write_session = None;
        let mut opts = WriteSessionOptions {
            session_id: "abc".into(),
            max_in_flight_uploads: 7,
            cap_upload_bytes: true,
            stream_data_packs: true,
        };
        repo.begin_write_session(&opts).unwrap();
        {
            let ws = repo.write_session.as_ref().unwrap();
            assert_eq!(ws.session_id, "abc");
            assert!(ws.stream_data_packs);
            assert_eq!(ws.uploads.max_in_flight_uploads, 7);
            assert!(ws.uploads.cap_pending_upload_bytes);
        }

        repo.write_session = None;
        opts.max_in_flight_uploads = 0;
        repo.begin_write_session(&opts).unwrap();
        assert_eq!(
            repo.write_session
                .as_ref()
                .unwrap()
                .uploads
                .max_in_flight_uploads,
            1
        );
        repo.write_session = None;
    }

    struct ZeroWidth(Arc<dyn StorageBackend>);

    impl InnerBackend for ZeroWidth {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &*self.0
        }
    }

    delegate_storage_backend! {
        for ZeroWidth;
        except [profile];

        fn profile(&self) -> BackendProfile {
            BackendProfile {
                request_width: 0,
                ..self.0.profile()
            }
        }
    }

    #[test]
    fn request_width_follows_the_backend() {
        let mut repo = test_repo_plaintext();
        assert_eq!(repo.request_width(), 2);
        repo.write_session = None;
        assert_eq!(repo.request_width(), 2);

        repo.storage = Arc::new(ZeroWidth(Arc::clone(&repo.storage)));
        assert_eq!(repo.request_width(), 1);
    }

    #[test]
    fn rollback_checkpoint_armed_and_committed_round_trips() {
        let mut repo = test_repo_plaintext();
        assert!(!repo.rollback_tracker_armed());
        repo.begin_rollback_checkpoint().unwrap();
        assert!(repo.rollback_tracker_armed());
        repo.commit_rollback_checkpoint();
        assert!(!repo.rollback_tracker_armed());
    }

    #[test]
    fn rollback_checkpoint_rollback_clears_tracker() {
        let mut repo = test_repo_plaintext();
        repo.begin_rollback_checkpoint().unwrap();
        repo.abort_rollback_checkpoint();
        assert!(!repo.rollback_tracker_armed());
    }

    #[test]
    #[should_panic(expected = "begin_rollback_checkpoint called while another checkpoint is armed")]
    fn cannot_begin_overlapping_rollback_checkpoint() {
        let mut repo = test_repo_plaintext();
        repo.begin_rollback_checkpoint().unwrap();
        // Second begin must panic via the overlap assertion.
        let _ = repo.begin_rollback_checkpoint();
    }

    /// A finished upload is journaled on the next stored chunk, not only when
    /// the next pack is sealed.
    #[test]
    fn finished_upload_is_journaled_before_next_seal() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let mut repo = test_repo_plaintext();
        repo.store_chunk(b"first pack chunk", Compression::None, PackType::Data)
            .unwrap();
        repo.flush_writer_async(PackType::Data).unwrap();
        let pack_id = repo.write_session.as_ref().unwrap().uploads.pending()[0].pack_id;
        while !repo.write_session.as_ref().unwrap().uploads.pending()[0]
            .handle
            .is_finished()
        {
            std::thread::yield_now();
        }

        repo.store_chunk(b"next chunk", Compression::None, PackType::Data)
            .unwrap();

        let ws = repo.write_session.as_ref().unwrap();
        assert!(ws.uploads.is_empty());
        assert!(
            ws.journal
                .entries
                .to_wire()
                .iter()
                .any(|e| e.pack_id == pack_id)
        );
        repo.write_session = None;
    }

    /// The write counters match what reached storage, and count the seal
    /// forced by a rollback checkpoint.
    #[test]
    fn write_metrics_count_seals_bytes_and_forced_seals() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let mut repo = test_repo_plaintext();
        repo.store_chunk(b"first", Compression::None, PackType::Data)
            .unwrap();
        repo.begin_rollback_checkpoint().unwrap();
        repo.commit_rollback_checkpoint();
        repo.store_chunk(b"second", Compression::None, PackType::Data)
            .unwrap();
        repo.store_chunk(b"tree", Compression::None, PackType::Tree)
            .unwrap();
        repo.flush_packs().unwrap();

        let m = repo.write_metrics().unwrap();
        assert_eq!(m.data_packs, 2);
        assert_eq!(m.tree_packs, 1);
        assert_eq!(m.forced_data_seals, 1);
        assert!(m.seal > std::time::Duration::ZERO || m.upload_put > std::time::Duration::ZERO);

        let stored: u64 = repo
            .storage
            .list("packs/")
            .unwrap()
            .iter()
            .map(|key| repo.storage.get(key).unwrap().unwrap().len() as u64)
            .sum();
        assert_eq!(m.data_pack_bytes + m.tree_pack_bytes, stored);
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
        repo.write_session.as_mut().unwrap().stream_data_packs = true;

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
            (
                ws.uploads.pending()[0].pack_id,
                ws.uploads.pending()[0].bytes,
            )
        };
        assert_eq!(bytes, 0, "a staged pack counts no heap bytes");

        repo.flush_packs().unwrap();
        assert!(staging_files(dir.path()).is_empty());
        let stored = repo.storage.get(&pack_id.storage_key()).unwrap().unwrap();
        assert_eq!(PackId::compute(&stored, repo.content_hash()), pack_id);
        let ws = repo.write_session.as_ref().unwrap();
        assert!(
            ws.journal
                .entries
                .to_wire()
                .iter()
                .any(|e| e.pack_id == pack_id)
        );
        repo.write_session = None;
    }

    /// Stores a pack, releases its buffer, then blocks until the gate opens,
    /// like an HTTP backend waiting on a slow completion.
    struct ReleaseThenBlock {
        inner: Arc<dyn StorageBackend>,
        released_tx: std::sync::mpsc::Sender<()>,
        gate_rx: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl InnerBackend for ReleaseThenBlock {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &*self.inner
        }
    }

    delegate_storage_backend! {
        for ReleaseThenBlock;
        except [put_pack, begin_staged_put, conditional, profile];

        // Test wrapper: keeps every write on `put`.
        fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
            None
        }

        fn put_pack(
            &self,
            key: &str,
            data: vykar_storage::PackData,
        ) -> vykar_types::error::Result<()> {
            let (bytes, release) = data.into_parts();
            self.inner.put(key, &bytes)?;
            drop(bytes);
            drop(release);
            let _ = self.released_tx.send(());
            let _ = self.gate_rx.lock().unwrap().recv();
            Ok(())
        }

        fn begin_staged_put(
            &self,
            _prefix: &str,
        ) -> vykar_types::error::Result<Option<Box<dyn vykar_storage::StagedPut>>> {
            Ok(None)
        }

        fn profile(&self) -> BackendProfile {
            BackendProfile {
                staged_put: false,
                ..self.inner.profile()
            }
        }
    }

    /// A heap pack whose upload released its buffer stops counting while its
    /// upload thread is still running, and is journaled once it returns.
    #[test]
    fn early_released_pack_stops_counting_before_its_upload_returns() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let mut repo = test_repo_plaintext();
        let (released_tx, released_rx) = std::sync::mpsc::channel();
        let (gate_tx, gate_rx) = std::sync::mpsc::channel();
        repo.storage = Arc::new(ReleaseThenBlock {
            inner: Arc::clone(&repo.storage),
            released_tx,
            gate_rx: std::sync::Mutex::new(gate_rx),
        });

        repo.store_chunk(b"x", Compression::None, PackType::Data)
            .unwrap();
        repo.flush_writer_async(PackType::Data).unwrap();
        released_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        let pack_id = {
            let upload = &repo.write_session.as_ref().unwrap().uploads.pending()[0];
            assert!(upload.bytes > 0);
            assert!(!upload.handle.is_finished());
            assert_eq!(upload.held_bytes(), 0);
            upload.pack_id
        };

        gate_tx.send(()).unwrap();
        repo.flush_packs().unwrap();
        let ws = repo.write_session.as_ref().unwrap();
        assert!(
            ws.journal
                .entries
                .to_wire()
                .iter()
                .any(|e| e.pack_id == pack_id)
        );
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
        repo.write_session.as_mut().unwrap().stream_data_packs = true;

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
        let (gated, tx) =
            crate::testutil::PackUploadBackend::gated(std::sync::Arc::clone(&repo.storage));
        repo.storage = std::sync::Arc::new(gated);

        repo.begin_rollback_checkpoint().unwrap();
        repo.store_chunk(b"rolled back chunk", Compression::None, PackType::Data)
            .unwrap();
        repo.flush_writer_async(PackType::Data).unwrap();
        let pack_id = {
            let ws = repo.write_session.as_ref().unwrap();
            assert_eq!(ws.uploads.len(), 1);
            assert!(
                !ws.uploads.pending()[0].handle.is_finished(),
                "upload is gated"
            );
            ws.uploads.pending()[0].pack_id
        };

        repo.abort_rollback_checkpoint();
        tx.send(()).unwrap();
        repo.flush_packs().unwrap();

        let ws = repo.write_session.as_ref().unwrap();
        assert!(ws.uploads.is_empty());
        assert!(
            ws.journal
                .entries
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

    /// Over an unedited index a session finds existing chunks in the table
    /// and records their reuse by table position, not by key; workers get a
    /// prefilter over the same table.
    #[test]
    fn session_dedups_against_the_table_by_position() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let mut repo = test_repo_plaintext();
        let (id, size, _) = repo
            .store_chunk(
                b"stored before the session",
                Compression::None,
                PackType::Data,
            )
            .unwrap();
        // A fold, so that the chunk is in the table rather than in an
        // applied segment.
        repo.request_index_fold();
        repo.save_state().unwrap();

        repo.begin_write_session(&WriteSessionOptions::default())
            .unwrap();
        let prefilter = repo.dedup_prefilter().expect("prefilter over the table");
        assert!(Arc::ptr_eq(
            prefilter.view().table(),
            repo.chunk_index().table().unwrap()
        ));
        let (again, again_size, is_new) = repo
            .store_chunk(
                b"stored before the session",
                Compression::None,
                PackType::Data,
            )
            .unwrap();
        assert_eq!((again, again_size, is_new), (id, size, false));
        assert_eq!(repo.reuse_cached_chunk_ref(&id).unwrap(), size);

        let ws = repo.write_session.as_ref().unwrap();
        let delta = ws.index_delta.as_ref().unwrap();
        assert!(
            !delta.reused.is_keyed(&id),
            "no keyed record for a table hit"
        );
        assert!(repo.delta_reuses(&id));

        // Reuse is recorded in the delta but not persisted.
        let before = repo.chunk_index().get(&id).unwrap();
        repo.save_state().unwrap();
        assert_eq!(repo.chunk_index().get(&id).unwrap(), before);
    }

    /// Over an index with unsaved edits (repair) a session sees the edits:
    /// lookups and reuse go through the live index by key, and there is no
    /// prefilter over the (outdated) table.
    #[test]
    fn session_over_an_edited_index_records_by_key() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let mut repo = test_repo_plaintext();
        let (kept, _, _) = repo
            .store_chunk(b"kept chunk", Compression::None, PackType::Data)
            .unwrap();
        let (removed, _, _) = repo
            .store_chunk(b"removed chunk", Compression::None, PackType::Data)
            .unwrap();
        repo.save_state().unwrap();

        assert!(repo.chunk_index_mut().remove(&removed).is_some());
        repo.begin_write_session(&WriteSessionOptions::default())
            .unwrap();
        assert!(repo.dedup_prefilter().is_none());
        assert!(
            repo.bump_ref_if_exists(&removed).is_none(),
            "the removal is seen"
        );
        assert!(repo.bump_ref_if_exists(&kept).is_some());
        let delta = repo
            .write_session
            .as_ref()
            .unwrap()
            .index_delta
            .as_ref()
            .unwrap();
        assert!(delta.reused.is_keyed(&kept));
        assert!(delta.reused.positional().is_none());

        repo.save_state().unwrap();
        assert!(!repo.chunk_index().contains(&removed));
        assert!(repo.chunk_index().contains(&kept));
    }
}
