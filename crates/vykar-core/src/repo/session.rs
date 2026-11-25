use tracing::warn;

use super::Repo;
use super::journal_recovery::PendingIndexRecovery;
use super::pack::{PackType, compute_data_pack_target, compute_tree_pack_target};
use super::write_session::{DEFAULT_SESSION_ID, SessionView, WriteMetricsSnapshot, WriteSession};
use crate::index::IndexDelta;
use crate::index::table::DedupPrefilter;
use vykar_storage::BackendProfile;
#[cfg(test)]
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

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
    /// (see `WriteSession::stream_data_packs`).
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

impl Repo {
    /// Begin a write session over this repository's index as it is now.
    ///
    /// Creates a fresh [`WriteSession`] with pack targets computed from the
    /// current chunk index and repo config. The chunk-store and flush
    /// methods are on the session; a save
    /// (`RepoWriter::save_session`) or a backup commit consumes it, and
    /// [`WriteSession::flush_on_abort`] ends it on failure.
    ///
    /// Over an unedited index the session resolves existing chunks against
    /// the repository's index view as it is now (the table and the head's
    /// segments applied over it), and records reuse of table chunks by table
    /// position (see `ReusedChunks`), of segment additions by key; backup
    /// workers get a prefilter over the same view. Over an index with unsaved
    /// edits (repair) every lookup and every reuse goes through the live index
    /// by key, and there is no prefilter.
    ///
    /// Applies every field of `opts`. No I/O.
    pub fn begin_write_session(&self, opts: &WriteSessionOptions) -> WriteSession {
        let num_packs = self.chunk_index.count_distinct_packs();
        let data_target = compute_data_pack_target(
            num_packs,
            self.config.min_pack_size,
            self.config.max_pack_size,
        );
        let tree_target = compute_tree_pack_target(self.config.min_pack_size);
        let mut ws = WriteSession::new(
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
        ws
    }

    /// Independent read-only requests worth issuing at once against this
    /// repository's backend (journal recovery's pack lookups, commit-time
    /// pack verification, item-stream range reads). At least 1.
    pub(super) fn request_width(&self) -> usize {
        self.storage.profile().request_width.max(1)
    }

    /// Best-effort delete of the `pending_index` file from storage.
    /// Called from the backup command after the commit succeeds.
    pub fn clear_pending_index(&self, session_id: &str) {
        super::session_journal::clear_pending_index(&*self.storage, session_id);
    }

    /// Make `index` the repository's table and restart `ws`'s lookups and
    /// delta over it, as if the session had begun with it loaded (for
    /// testing).
    #[cfg(test)]
    pub(crate) fn set_session_table_for_test(
        &mut self,
        ws: &mut WriteSession,
        index: &crate::index::ChunkIndex,
    ) {
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
        ws.index_delta = Some(IndexDelta::with_table(Arc::clone(&table)));
        ws.view = Some(view);
    }
}

impl WriteSession {
    /// The backup's session marker, pulled back by the age of every pack
    /// upload an append-only server refuses because the pack is stored
    /// already (`existing_pack`).
    pub(crate) fn attach_session_marker(&self, marker: std::sync::Arc<super::lock::SessionMarker>) {
        if let Some(existing) = self.existing_packs.as_ref() {
            existing.attach_marker(marker);
        }
    }

    /// Snapshot of the session's diagnostic counters.
    pub(crate) fn write_metrics(&self) -> WriteMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// The worker-side prefilter over the session's view: `None` without a
    /// view (an edited or unloaded index).
    pub(crate) fn dedup_prefilter(&self) -> Option<DedupPrefilter> {
        self.view
            .as_ref()
            .map(|v| DedupPrefilter::new(v.view.clone()))
    }

    /// Best-effort cleanup after a failed backup or other operation; ends
    /// the session.
    ///
    /// Seals any partial pack writers, waits for in-flight uploads to land,
    /// and writes the final `pending_index` journal so a subsequent run can
    /// recover. All errors are logged but never propagated.
    ///
    /// Returns at once when there is nothing to clean up.
    pub fn flush_on_abort(mut self, repo: &Repo) {
        self.finished = true;
        let has_partial_packs =
            self.data_pack_writer.has_pending() || self.tree_pack_writer.has_pending();
        if self.uploads.is_empty() && self.journal.entries.is_empty() && !has_partial_packs {
            return;
        }

        warn!("saving progress for next run\u{2026}");

        // Seal and flush any partial data/tree pack writers.
        // Sample both has_pending() flags before flushing either writer:
        // the data and tree pack writers are independent, so flushing one
        // can't change the other's pending state.
        let flush_data = self.data_pack_writer.has_pending();
        let flush_tree = self.tree_pack_writer.has_pending();

        if flush_data && let Err(e) = self.flush_writer_async(repo, PackType::Data) {
            warn!("flush_on_abort: failed to seal data pack: {e}");
        }
        if flush_tree && let Err(e) = self.flush_writer_async(repo, PackType::Tree) {
            warn!("flush_on_abort: failed to seal tree pack: {e}");
        }

        // Join all in-flight upload threads so packs land on storage.
        for upload in self.uploads.take_all() {
            if let Err(e) = self.finish_upload(upload) {
                warn!("flush_on_abort: upload thread failed: {e}");
            }
        }

        // Write final pending_index so next run can recover.
        self.write_pending_index_best_effort(&*repo.storage, &*repo.crypto);
    }

    /// Recover chunk→pack mappings from a previous interrupted session's
    /// `pending_index` file. Verifies each pack exists before adding entries.
    ///
    /// Must be called inside the repo lock, before any chunk is stored.
    /// Pack lookups use the backend's request width
    /// (`BackendProfile::request_width`) and stop when `cancel` fires.
    pub fn recover_pending_index(
        &mut self,
        repo: &Repo,
        cancel: &super::control::CancelToken,
    ) -> Result<PendingIndexRecovery> {
        self.recover_pending_index_from(
            &*repo.storage,
            &*repo.crypto,
            &repo.chunk_index,
            repo.request_width(),
            repo.host_env.clock.as_ref(),
            cancel,
        )
    }

    // --- Per-file reuse stage (FileTxn) ---
    //
    // A file committed in pieces (a segmented large file) stages the reuse
    // it records, so a skip can drop it. Its new chunks are never undone:
    // they are in the session's packs and stay in the index unreferenced
    // (the next compact sweeps them). One stage at a time: the backup
    // consumer is serial.

    /// Open the stage: reuse first recorded from here on can be dropped.
    /// Panics if a stage is open (see [`ReusedChunks::checkpoint`]).
    ///
    /// [`ReusedChunks::checkpoint`]: crate::index::reused::ReusedChunks::checkpoint
    pub(crate) fn stage_file_reuse(&mut self) {
        self.index_delta
            .as_mut()
            .expect("an active session has an index delta")
            .reused
            .checkpoint();
    }

    /// Keep the staged reuse and close the stage. No-op without one.
    pub(crate) fn keep_file_reuse(&mut self) {
        if let Some(delta) = self.index_delta.as_mut() {
            delta.reused.discard_checkpoint();
        }
    }

    /// Drop the reuse first recorded since [`Self::stage_file_reuse`] and
    /// close the stage. No-op without one.
    pub(crate) fn drop_file_reuse(&mut self) {
        if let Some(delta) = self.index_delta.as_mut() {
            delta.reused.rollback();
        }
    }
}

#[cfg(test)]
impl WriteSession {
    /// Current data pack target size in bytes (for testing).
    pub(crate) fn data_pack_target(&self) -> usize {
        self.data_pack_writer.target_size()
    }

    /// The session's index delta (for testing).
    pub(crate) fn write_session_delta_for_test(&self) -> &IndexDelta {
        self.index_delta.as_ref().unwrap()
    }

    /// Whether the session's delta records `id` as reused, by key or by
    /// table position (for testing).
    pub(crate) fn delta_reuses(&self, id: &ChunkId) -> bool {
        self.index_delta
            .as_ref()
            .is_some_and(|delta| delta.reused.contains(id))
    }

    /// True while a file's reuse is staged (for testing).
    pub(crate) fn file_reuse_staged(&self) -> bool {
        self.index_delta
            .as_ref()
            .is_some_and(|d| d.reused.is_armed())
    }
}

#[cfg(test)]
mod tests {
    use super::WriteSessionOptions;
    use crate::testutil::test_repo_plaintext_with_session;
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
        let (repo, mut ws) = test_repo_plaintext_with_session();
        drop(ws);
        let mut opts = WriteSessionOptions {
            session_id: "abc".into(),
            max_in_flight_uploads: 7,
            cap_upload_bytes: true,
            stream_data_packs: true,
        };
        ws = repo.begin_write_session(&opts);
        {
            assert_eq!(ws.session_id, "abc");
            assert!(ws.stream_data_packs);
            assert_eq!(ws.uploads.max_in_flight_uploads, 7);
            assert!(ws.uploads.cap_pending_upload_bytes);
        }

        drop(ws);
        opts.max_in_flight_uploads = 0;
        ws = repo.begin_write_session(&opts);
        assert_eq!(ws.uploads.max_in_flight_uploads, 1);
        drop(ws);
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
        let (mut repo, ws) = test_repo_plaintext_with_session();
        assert_eq!(repo.request_width(), 2);
        drop(ws);
        assert_eq!(repo.request_width(), 2);

        repo.storage = Arc::new(ZeroWidth(Arc::clone(&repo.storage)));
        assert_eq!(repo.request_width(), 1);
    }

    /// `holds_unflushed_work` (the drop warning): not for a fresh session,
    /// yes once a stored chunk sits in an open pack, not once `flush_packs`
    /// has uploaded it and written the journal.
    #[test]
    fn holds_unflushed_work_follows_the_flush() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let (repo, mut ws) = test_repo_plaintext_with_session();
        assert!(!ws.holds_unflushed_work());
        ws.store_chunk(
            &repo,
            b"a chunk in an open pack",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
        assert!(ws.holds_unflushed_work());
        ws.flush_packs(&repo).unwrap();
        assert!(!ws.holds_unflushed_work());
    }

    #[test]
    fn file_reuse_stage_round_trips() {
        let (_repo, mut ws) = test_repo_plaintext_with_session();
        assert!(!ws.file_reuse_staged());
        ws.stage_file_reuse();
        assert!(ws.file_reuse_staged());
        ws.keep_file_reuse();
        assert!(!ws.file_reuse_staged());
        ws.stage_file_reuse();
        ws.drop_file_reuse();
        assert!(!ws.file_reuse_staged());
    }

    #[test]
    #[should_panic(
        expected = "ReusedChunks::checkpoint() called while another checkpoint is armed"
    )]
    fn cannot_stage_two_files() {
        let (_repo, mut ws) = test_repo_plaintext_with_session();
        ws.stage_file_reuse();
        // A second stage must panic via the overlap assertion.
        ws.stage_file_reuse();
    }

    /// A finished upload is journaled on the next stored chunk, not only when
    /// the next pack is sealed.
    #[test]
    fn finished_upload_is_journaled_before_next_seal() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let (repo, mut ws) = test_repo_plaintext_with_session();
        ws.store_chunk(
            &repo,
            b"first pack chunk",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
        ws.flush_writer_async(&repo, PackType::Data).unwrap();
        let pack_id = ws.uploads.pending()[0].pack_id;
        while !ws.uploads.pending()[0].handle.is_finished() {
            std::thread::yield_now();
        }

        ws.store_chunk(&repo, b"next chunk", Compression::None, PackType::Data)
            .unwrap();

        assert!(ws.uploads.is_empty());
        assert!(
            ws.journal
                .entries
                .to_wire()
                .iter()
                .any(|e| e.pack_id == pack_id)
        );
        drop(ws);
    }

    /// The write counters match what reached storage.
    #[test]
    fn write_metrics_count_seals_and_bytes() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let (repo, mut ws) = test_repo_plaintext_with_session();
        ws.store_chunk(&repo, b"first", Compression::None, PackType::Data)
            .unwrap();
        ws.store_chunk(&repo, b"second", Compression::None, PackType::Data)
            .unwrap();
        ws.store_chunk(&repo, b"tree", Compression::None, PackType::Tree)
            .unwrap();
        ws.flush_packs(&repo).unwrap();

        let m = ws.write_metrics();
        assert_eq!(m.data_packs, 1, "first and second share the pack");
        assert_eq!(m.tree_packs, 1);
        assert!(m.seal > std::time::Duration::ZERO || m.upload_put > std::time::Duration::ZERO);

        let stored: u64 = repo
            .storage
            .list("packs/")
            .unwrap()
            .iter()
            .map(|key| repo.storage.get(key).unwrap().unwrap().len() as u64)
            .sum();
        assert_eq!(m.data_pack_bytes + m.tree_pack_bytes, stored);
        drop(ws);
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
        let (mut repo, mut ws) = test_repo_plaintext_with_session();
        repo.storage = std::sync::Arc::new(
            vykar_storage::local_backend::LocalBackend::new(dir.path().to_str().unwrap()).unwrap(),
        );
        ws.stream_data_packs = true;

        ws.store_chunk(&repo, b"streamed chunk", Compression::None, PackType::Data)
            .unwrap();
        ws.store_chunk(&repo, b"tree chunk", Compression::None, PackType::Tree)
            .unwrap();
        {
            assert_eq!(ws.data_pack_writer.buffer_capacity(), 0);
            assert!(ws.tree_pack_writer.buffer_capacity() > 0);
        }
        assert_eq!(staging_files(dir.path()).len(), 1, "data pack is staged");

        ws.flush_writer_async(&repo, PackType::Data).unwrap();
        let (pack_id, bytes) = {
            (
                ws.uploads.pending()[0].pack_id,
                ws.uploads.pending()[0].bytes,
            )
        };
        assert_eq!(bytes, 0, "a staged pack counts no heap bytes");

        ws.flush_packs(&repo).unwrap();
        assert!(staging_files(dir.path()).is_empty());
        let stored = repo.storage.get(&pack_id.storage_key()).unwrap().unwrap();
        assert_eq!(PackId::compute(&stored, repo.content_hash()), pack_id);
        assert!(
            ws.journal
                .entries
                .to_wire()
                .iter()
                .any(|e| e.pack_id == pack_id)
        );
        drop(ws);
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

        let (mut repo, mut ws) = test_repo_plaintext_with_session();
        let (released_tx, released_rx) = std::sync::mpsc::channel();
        let (gate_tx, gate_rx) = std::sync::mpsc::channel();
        repo.storage = Arc::new(ReleaseThenBlock {
            inner: Arc::clone(&repo.storage),
            released_tx,
            gate_rx: std::sync::Mutex::new(gate_rx),
        });

        ws.store_chunk(&repo, b"x", Compression::None, PackType::Data)
            .unwrap();
        ws.flush_writer_async(&repo, PackType::Data).unwrap();
        released_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        let pack_id = {
            let upload = &ws.uploads.pending()[0];
            assert!(upload.bytes > 0);
            assert!(!upload.handle.is_finished());
            assert_eq!(upload.held_bytes(), 0);
            upload.pack_id
        };

        gate_tx.send(()).unwrap();
        ws.flush_packs(&repo).unwrap();
        assert!(
            ws.journal
                .entries
                .to_wire()
                .iter()
                .any(|e| e.pack_id == pack_id)
        );
        drop(ws);
    }

    /// Dropping the session discards the partially written staged pack and
    /// leaves no temp file behind.
    #[test]
    fn discarded_staged_pack_leaves_no_temp_file() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let dir = tempfile::tempdir().unwrap();
        let (mut repo, mut ws) = test_repo_plaintext_with_session();
        repo.storage = std::sync::Arc::new(
            vykar_storage::local_backend::LocalBackend::new(dir.path().to_str().unwrap()).unwrap(),
        );
        ws.stream_data_packs = true;

        ws.store_chunk(&repo, b"abandoned", Compression::None, PackType::Data)
            .unwrap();
        assert_eq!(staging_files(dir.path()).len(), 1);
        drop(ws);
        assert!(staging_files(dir.path()).is_empty());
    }

    /// A pack sealed while a file's reuse is staged, whose upload finishes
    /// only after the file is dropped, is journaled like any other: the
    /// skipped file's chunks stay in the session. A following session
    /// recovers the pack and adopts a chunk it hits as a new entry with a
    /// known trailer length.
    #[test]
    fn skipped_file_pack_is_journaled() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let (mut repo, mut ws) = test_repo_plaintext_with_session();
        // A store with mtimes: recovery adopts journal packs by their age.
        let (gated, tx) = crate::testutil::PackUploadBackend::gated(Arc::new(
            crate::testutil::MemoryBackend::with_mtimes(),
        ));
        repo.storage = Arc::new(gated);

        ws.stage_file_reuse();
        let (chunk_id, _, _) = ws
            .store_chunk(
                &repo,
                b"skipped file chunk",
                Compression::None,
                PackType::Data,
            )
            .unwrap();
        ws.flush_writer_async(&repo, PackType::Data).unwrap();
        let pack_id = {
            assert_eq!(ws.uploads.len(), 1);
            assert!(
                !ws.uploads.pending()[0].handle.is_finished(),
                "upload is gated"
            );
            ws.uploads.pending()[0].pack_id
        };

        ws.drop_file_reuse();
        tx.send(()).unwrap();
        ws.flush_packs(&repo).unwrap();

        assert!(ws.uploads.is_empty());
        assert!(
            ws.journal
                .entries
                .to_wire()
                .iter()
                .any(|e| e.pack_id == pack_id),
            "a skipped file's pack is journaled"
        );
        ws.flush_on_abort(&repo);
        assert!(repo.storage.exists("sessions/default.index").unwrap());

        // The next session recovers the pack and promotes a chunk it hits.
        let mut ws2 = repo.begin_write_session(&WriteSessionOptions {
            session_id: "next".into(),
            ..Default::default()
        });
        // The recovering session's own marker: its mtime is "now" on the
        // store's clock, which the journal pack's age is judged against.
        repo.storage.put("sessions/next.json", b"{}").unwrap();
        let recovery = ws2
            .recover_pending_index(&repo, &crate::repo::control::CancelToken::new())
            .unwrap();
        assert_eq!(recovery.recovered_chunks, 1);
        assert!(ws2.reuse_if_exists(&repo, &chunk_id).is_some());
        let delta = ws2.write_session_delta_for_test();
        assert!(
            delta
                .new_entries
                .iter()
                .any(|e| e.chunk_id == chunk_id && e.pack_id == pack_id),
            "the hit is a new entry on the recovered pack"
        );
        assert!(!ws2.delta_reuses(&chunk_id));
        assert!(
            ws2.pack_trailers.contains_key(&pack_id),
            "the recovered pack's trailer length is known"
        );
        drop(ws2);
    }

    /// Over an unedited index a session finds existing chunks in the table
    /// and records their reuse by table position, not by key; workers get a
    /// prefilter over the same table.
    #[test]
    fn session_dedups_against_the_table_by_position() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let (mut repo, mut ws) = test_repo_plaintext_with_session();
        let (id, size, _) = ws
            .store_chunk(
                &repo,
                b"stored before the session",
                Compression::None,
                PackType::Data,
            )
            .unwrap();
        // A fold, so that the chunk is in the table rather than in an
        // applied segment.
        repo.request_index_fold();
        repo.unchecked_writer().save_session(ws).unwrap();

        ws = repo.begin_write_session(&WriteSessionOptions::default());
        let prefilter = ws.dedup_prefilter().expect("prefilter over the table");
        assert!(Arc::ptr_eq(
            prefilter.view().table(),
            repo.chunk_index().table().unwrap()
        ));
        let (again, again_size, is_new) = ws
            .store_chunk(
                &repo,
                b"stored before the session",
                Compression::None,
                PackType::Data,
            )
            .unwrap();
        assert_eq!((again, again_size, is_new), (id, size, false));
        assert_eq!(ws.reuse_cached_chunk_ref(&repo, &id).unwrap(), size);

        let delta = ws.index_delta.as_ref().unwrap();
        assert!(
            !delta.reused.is_keyed(&id),
            "no keyed record for a table hit"
        );
        assert!(ws.delta_reuses(&id));

        // Reuse is recorded in the delta but not persisted.
        let before = repo.chunk_index().get(&id).unwrap();
        repo.unchecked_writer().save_session(ws).unwrap();
        assert_eq!(repo.chunk_index().get(&id).unwrap(), before);
    }

    /// Over an index with unsaved edits (repair) a session sees the edits:
    /// lookups and reuse go through the live index by key, and there is no
    /// prefilter over the (outdated) table.
    #[test]
    fn session_over_an_edited_index_records_by_key() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;

        let (mut repo, mut ws) = test_repo_plaintext_with_session();
        let (kept, _, _) = ws
            .store_chunk(&repo, b"kept chunk", Compression::None, PackType::Data)
            .unwrap();
        let (removed, _, _) = ws
            .store_chunk(&repo, b"removed chunk", Compression::None, PackType::Data)
            .unwrap();
        repo.unchecked_writer().save_session(ws).unwrap();

        assert!(
            repo.unchecked_writer()
                .chunk_index_mut()
                .remove(&removed)
                .is_some()
        );
        ws = repo.begin_write_session(&WriteSessionOptions::default());
        assert!(ws.dedup_prefilter().is_none());
        assert!(
            ws.reuse_if_exists(&repo, &removed).is_none(),
            "the removal is seen"
        );
        assert!(ws.reuse_if_exists(&repo, &kept).is_some());
        let delta = ws.index_delta.as_ref().unwrap();
        assert!(delta.reused.is_keyed(&kept));
        assert!(delta.reused.positional().is_none());

        repo.unchecked_writer().save_session(ws).unwrap();
        assert!(!repo.chunk_index().contains(&removed));
        assert!(repo.chunk_index().contains(&kept));
    }
}
