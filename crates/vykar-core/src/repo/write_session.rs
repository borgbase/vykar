use std::collections::HashMap as StdHashMap;
use std::sync::Arc;

use crate::index::dedup_cache::TieredDedupIndex;
use crate::index::{
    ChunkIndex, DedupIndex, IndexDelta, IndexDeltaCheckpoint, PendingChunkEntry,
    RecoveredChunkEntry,
};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;
use vykar_types::hash::HashAlgorithm;
use vykar_types::pack_id::PackId;

use super::journal_recovery::{self, PendingIndexRecovery, RecoveryTarget};
use super::pack::{PackType, PackWriter, PackedChunkEntry};
use super::session_journal::SessionJournal;
use super::upload_queue::{HeldBytes, PendingUpload, UploadQueue};

pub(super) const DEFAULT_SESSION_ID: &str = "default";

/// Tracks index mutations between `begin_rollback_checkpoint` and commit so
/// they can be undone if the guarded scope (command dump, or per-file backup
/// chunk loop) fails mid-stream.
///
/// The primitive is single-slot: only one tracker can be armed at a time.
/// The backup serial consumer and the dump command never overlap.
pub(crate) struct RollbackTracker {
    /// Truncation point for IndexDelta.
    pub delta_checkpoint: IndexDeltaCheckpoint,
    /// ChunkIds inserted into tiered_dedup or dedup_index during this scope.
    pub dedup_inserts: Vec<ChunkId>,
    /// Entries promoted out of recovered_chunks — saved to re-insert on rollback.
    pub promoted_recovered: Vec<(ChunkId, RecoveredChunkEntry)>,
    /// PackIds added to PendingIndexJournal during this scope.
    pub journal_pack_ids: Vec<PackId>,
    /// Data pack writer target size at checkpoint time (for reset on rollback).
    pub data_pack_target_size: usize,
}

/// Write-path state that is active during a backup session.
///
/// Composes the pack writers, the upload queue, the session's crash-recovery
/// journal, the dedup / index-delta / rollback state and the chunks recovered
/// from interrupted sessions — all transient state that only exists while
/// chunks are being written. It wires each confirmed upload into the journal.
pub(crate) struct WriteSessionState {
    /// Crash-recovery journal of confirmed packs and its writes. Declared
    /// first so its `Drop` (joining a background write) runs before any other
    /// field is dropped, as `WriteSessionState`'s own `Drop` used to.
    pub(crate) journal: SessionJournal,
    /// Pack writer for file content chunks.
    pub(crate) data_pack_writer: PackWriter,
    /// Pack writer for item-stream metadata chunks.
    pub(crate) tree_pack_writer: PackWriter,
    /// Background pack uploads; a pack enters `journal` only after its
    /// upload returns `Ok`.
    pub(crate) uploads: UploadQueue,
    /// Stream data packs into the backend's staging area as they fill
    /// instead of buffering them on the heap, when the backend supports it
    /// (`StorageBackend::begin_staged_put`; local repositories). A staged
    /// pack holds no heap, so it counts 0 against the upload byte cap.
    pub(crate) stream_data_packs: bool,
    /// Lightweight dedup-only index used during backup to save memory.
    /// When active, `chunk_index` on Repository is empty.
    pub(crate) dedup_index: Option<DedupIndex>,
    /// Three-tier dedup index (xor filter + mmap + session HashMap).
    /// When active, both `chunk_index` and `dedup_index` are empty.
    pub(crate) tiered_dedup: Option<TieredDedupIndex>,
    /// Tracks index mutations while in dedup mode, applied at save time.
    pub(crate) index_delta: Option<IndexDelta>,
    /// Chunks recovered from a previous interrupted session's `pending_index`.
    pub(crate) recovered_chunks: StdHashMap<ChunkId, RecoveredChunkEntry>,
    /// `.index` keys to delete after successful commit.
    /// `.json` markers are never deleted here — their lifecycle is managed
    /// exclusively by `deregister_session()` (normal exit) and
    /// `cleanup_stale_sessions()` (>45 min since last refresh, under
    /// maintenance lock).
    pub(crate) recovered_index_keys: Vec<String>,
    /// Number of distinct packs (data + tree) in the persisted index at load time.
    pub(crate) persisted_pack_count: usize,
    /// Number of data packs flushed during the current session.
    pub(crate) session_packs_flushed: usize,
    /// Session ID for per-session pending_index files.
    /// Defaults to `"default"` for non-backup callers/tests.
    pub(crate) session_id: String,
    /// Active rollback tracker — set during streaming command dumps and
    /// during per-file backup chunk loops that need to survive mid-read
    /// drift detection. Single-slot by design.
    pub(crate) rollback_tracker: Option<RollbackTracker>,
}

/// Borrow the active write session from the `Repository::write_session` field.
#[track_caller]
pub(super) fn active(ws: &Option<WriteSessionState>) -> &WriteSessionState {
    ws.as_ref().expect("no active write session")
}

/// Mutably borrow the active write session from the `Repository::write_session` field.
#[track_caller]
pub(super) fn active_mut(ws: &mut Option<WriteSessionState>) -> &mut WriteSessionState {
    ws.as_mut().expect("no active write session")
}

impl WriteSessionState {
    /// Create a new write session state with the given pack writer targets.
    pub(crate) fn new(
        data_target: usize,
        tree_target: usize,
        max_in_flight_uploads: usize,
        hash_algo: HashAlgorithm,
    ) -> Self {
        Self {
            journal: SessionJournal::new(),
            data_pack_writer: PackWriter::new(PackType::Data, data_target, hash_algo),
            tree_pack_writer: PackWriter::new(PackType::Tree, tree_target, hash_algo),
            uploads: UploadQueue::new(max_in_flight_uploads),
            stream_data_packs: false,
            dedup_index: None,
            tiered_dedup: None,
            index_delta: None,
            recovered_chunks: StdHashMap::new(),
            recovered_index_keys: Vec::new(), // only .index keys, never .json
            persisted_pack_count: 0,
            session_packs_flushed: 0,
            session_id: DEFAULT_SESSION_ID.to_string(),
            rollback_tracker: None,
        }
    }

    /// Run `upload` on a background thread and queue it for joining.
    /// `bytes` is the heap the upload holds until it releases it or returns.
    pub(crate) fn spawn_upload<F>(
        &mut self,
        pack_id: PackId,
        journal_chunks: Vec<PendingChunkEntry>,
        bytes: usize,
        upload: F,
    ) where
        F: FnOnce(HeldBytes) -> Result<()> + Send + 'static,
    {
        self.uploads.spawn(pack_id, journal_chunks, bytes, upload);
    }

    /// Join one upload and journal its pack if it succeeded and was not
    /// rolled back. A failed upload is never journaled.
    pub(crate) fn finish_upload(&mut self, upload: PendingUpload) -> Result<()> {
        if let Some((pack_id, chunks)) = UploadQueue::finish(upload)? {
            self.journal.entries.record_pack(pack_id, chunks);
        }
        Ok(())
    }

    /// Join all finished upload threads, propagating the first error.
    pub(crate) fn drain_finished_uploads(
        &mut self,
        storage: &Arc<dyn StorageBackend>,
        crypto: &Arc<dyn CryptoEngine>,
    ) -> Result<()> {
        let journal = &mut self.journal.entries;
        self.uploads
            .drain_finished(|id, chunks| journal.record_pack(id, chunks))?;
        self.maybe_write_pending_index(storage, crypto);
        Ok(())
    }

    /// Wait for all background pack uploads to finish.
    pub(crate) fn wait_pending_uploads(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<()> {
        let result = self.join_all_uploads();
        // Final flush of journal before returning (best-effort). Skipped when
        // unchanged since the last successful write, so a repeated flush
        // (Phase 1 drain, then commit) does not PUT the journal twice.
        if self.journal.needs_write() {
            self.write_pending_index_best_effort(storage, crypto);
        }
        result
    }

    /// [`Self::wait_pending_uploads`], but the final journal write runs in
    /// the background like the debounced ones, so the caller can go on
    /// (backup: take the commit lock) during its round trip.
    ///
    /// The next `wait_pending_uploads` (the commit's own flush) joins it and
    /// rewrites the journal synchronously if it failed; dropping the session
    /// joins it too. So it can never land after the journal was deleted.
    pub(crate) fn wait_pending_uploads_journal_in_background(
        &mut self,
        storage: &Arc<dyn StorageBackend>,
        crypto: &Arc<dyn CryptoEngine>,
    ) -> Result<()> {
        let result = self.join_all_uploads();
        if self.journal.needs_write() {
            self.journal.start_write(&self.session_id, storage, crypto);
        }
        result
    }

    /// Join every queued upload (journaling the successful ones) and any
    /// background journal write; the first upload error, if any.
    fn join_all_uploads(&mut self) -> Result<()> {
        let journal = &mut self.journal.entries;
        let result = self
            .uploads
            .join_all(|id, chunks| journal.record_pack(id, chunks));
        self.join_journal_write();
        result
    }

    /// Apply backpressure to keep the number of in-flight uploads bounded.
    /// Session-marker refreshes run in a dedicated heartbeat thread owned by
    /// `SessionGuard`, so this path no longer touches the marker.
    pub(crate) fn cap_pending_uploads(
        &mut self,
        storage: &Arc<dyn StorageBackend>,
        crypto: &Arc<dyn CryptoEngine>,
        incoming: usize,
    ) -> Result<()> {
        self.drain_finished_uploads(storage, crypto)?;
        let journal = &mut self.journal.entries;
        if self
            .uploads
            .wait_below_count_cap(|id, chunks| journal.record_pack(id, chunks))?
        {
            self.maybe_write_pending_index(storage, crypto);
        }
        let cap = self.data_pack_writer.target_size();
        let journal = &mut self.journal.entries;
        if self
            .uploads
            .wait_below_byte_cap(incoming, cap, |id, chunks| journal.record_pack(id, chunks))?
        {
            self.maybe_write_pending_index(storage, crypto);
        }
        Ok(())
    }

    /// Pipeline budget to lend to the upload queue; see
    /// `UploadQueue::slack_bytes`.
    pub(crate) fn upload_slack_bytes(&self) -> usize {
        self.uploads
            .slack_bytes(self.data_pack_writer.target_size())
    }

    /// Start a background journal write once enough new packs are
    /// confirmed; see `SessionJournal::maybe_write`.
    pub(crate) fn maybe_write_pending_index(
        &mut self,
        storage: &Arc<dyn StorageBackend>,
        crypto: &Arc<dyn CryptoEngine>,
    ) {
        self.journal.maybe_write(&self.session_id, storage, crypto);
    }

    /// Wait for the background journal write, if any, and record its
    /// outcome. Best-effort like every journal write: failures are logged.
    pub(crate) fn join_journal_write(&mut self) {
        self.journal.join_write();
    }

    /// Serialize and write the pending index journal to storage.
    /// Best-effort: logs a warning on failure, never propagates errors.
    pub(crate) fn write_pending_index_best_effort(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) {
        self.journal
            .write_best_effort(&self.session_id, storage, crypto);
    }

    // --- Sealed entry dispatch ---

    /// Update index entries for a freshly sealed pack.
    ///
    /// In dedup/tiered mode, entries go into the session's dedup structure and
    /// index delta. In normal mode, entries go directly into `chunk_index`.
    /// Returns `true` if `chunk_index` was modified (caller should set `index_dirty`).
    pub(crate) fn apply_sealed_entries(
        &mut self,
        pack_id: PackId,
        entries: Vec<PackedChunkEntry>,
        chunk_index: &mut ChunkIndex,
    ) -> bool {
        if self.tiered_dedup.is_some() {
            for (chunk_id, stored_size, offset, refcount) in entries {
                if let Some(ref mut tiered) = self.tiered_dedup {
                    tiered.insert(chunk_id, stored_size);
                }
                if let Some(ref mut delta) = self.index_delta {
                    delta.add_new_entry(chunk_id, stored_size, pack_id, offset, refcount);
                }
                if let Some(ref mut tracker) = self.rollback_tracker {
                    tracker.dedup_inserts.push(chunk_id);
                }
            }
            false
        } else if self.dedup_index.is_some() {
            for (chunk_id, stored_size, offset, refcount) in entries {
                if let Some(ref mut dedup) = self.dedup_index {
                    dedup.insert(chunk_id, stored_size);
                }
                if let Some(ref mut delta) = self.index_delta {
                    delta.add_new_entry(chunk_id, stored_size, pack_id, offset, refcount);
                }
                if let Some(ref mut tracker) = self.rollback_tracker {
                    tracker.dedup_inserts.push(chunk_id);
                }
            }
            false
        } else {
            // Non-dedup mode: `begin_rollback_checkpoint` asserts a dedup
            // mode is active, so no tracker can exist here.
            debug_assert!(self.rollback_tracker.is_none());
            for (chunk_id, stored_size, offset, refcount) in entries {
                chunk_index.add(chunk_id, stored_size, pack_id, offset);
                for _ in 1..refcount {
                    chunk_index.increment_refcount(&chunk_id);
                }
            }
            true
        }
    }

    /// Recover chunk→pack mappings from interrupted sessions' journals into
    /// this session; see `journal_recovery::recover_pending_index`.
    pub(crate) fn recover_pending_index(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        chunk_index: &ChunkIndex,
        width: usize,
    ) -> Result<PendingIndexRecovery> {
        journal_recovery::recover_pending_index(
            storage,
            crypto,
            chunk_index,
            &self.session_id,
            self.persisted_pack_count,
            width,
            &mut RecoveryTarget {
                recovered_chunks: &mut self.recovered_chunks,
                recovered_index_keys: &mut self.recovered_index_keys,
                journal: &mut self.journal.entries,
            },
        )
    }

    /// Delete recovered `.index` files from storage after successful commit.
    /// Never deletes `.json` markers. Best-effort: logs on failure.
    pub(crate) fn cleanup_recovered_indices(&mut self, storage: &dyn StorageBackend) {
        journal_recovery::delete_recovered_indices(storage, &mut self.recovered_index_keys);
    }

    /// Promote a recovered chunk into the active dedup structure and index delta.
    /// Returns the stored size if the chunk was in `recovered_chunks`, None otherwise.
    ///
    /// In non-dedup mode, the chunk is added directly to `chunk_index` and
    /// the caller should set `index_dirty = true` when this returns `Some`.
    pub(crate) fn promote_recovered_chunk(
        &mut self,
        chunk_id: &ChunkId,
        chunk_index: &mut ChunkIndex,
    ) -> Option<(u32, bool)> {
        let entry = self.recovered_chunks.remove(chunk_id)?;

        // Record for rollback now that we own the entry.
        if let Some(ref mut tracker) = self.rollback_tracker {
            tracker.promoted_recovered.push((*chunk_id, entry.clone()));
            tracker.dedup_inserts.push(*chunk_id);
        }

        // Promote into active dedup structure.
        if let Some(ref mut tiered) = self.tiered_dedup {
            tiered.insert(*chunk_id, entry.stored_size);
        } else if let Some(ref mut dedup) = self.dedup_index {
            dedup.insert(*chunk_id, entry.stored_size);
        } else {
            chunk_index.add(
                *chunk_id,
                entry.stored_size,
                entry.pack_id,
                entry.pack_offset,
            );
            // Record in delta as a new entry with refcount=1.
            if let Some(ref mut delta) = self.index_delta {
                delta.add_new_entry(
                    *chunk_id,
                    entry.stored_size,
                    entry.pack_id,
                    entry.pack_offset,
                    1,
                );
            }
            return Some((entry.stored_size, true));
        }

        // Record in delta as a new entry with refcount=1.
        if let Some(ref mut delta) = self.index_delta {
            delta.add_new_entry(
                *chunk_id,
                entry.stored_size,
                entry.pack_id,
                entry.pack_offset,
                1,
            );
        }

        Some((entry.stored_size, false))
    }

    // --- Dedup query helpers ---

    /// Check only pending pack writers for a dedup hit (shared helper).
    pub(crate) fn bump_ref_pending(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        if let Some(s) = self.data_pack_writer.get_pending_stored_size(chunk_id) {
            self.data_pack_writer.increment_pending(chunk_id);
            return Some(s);
        }
        if let Some(s) = self.tree_pack_writer.get_pending_stored_size(chunk_id) {
            self.tree_pack_writer.increment_pending(chunk_id);
            return Some(s);
        }
        None
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::compress;
    use crate::index::{
        DecodedJournal, PendingIndexJournal, PendingPackEntry, decode_pending_journal,
    };
    use crate::repo::format::{ObjectType, unpack_object_expect_with_context};
    use crate::repo::lock::session_index_key;
    use crate::repo::session_journal::{JOURNAL_WRITE_INTERVAL, PENDING_INDEX_OBJECT_CONTEXT};
    use crate::repo::upload_queue::UPLOAD_QUEUE_HEADROOM;
    use crate::testutil::{MemoryBackend, PackUploadBackend, init_test_environment};
    use vykar_crypto::PlaintextEngine;
    use vykar_types::error::VykarError;

    /// Create a PlaintextEngine for tests.
    pub(in crate::repo) fn test_crypto() -> PlaintextEngine {
        PlaintextEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]))
    }

    /// A self-consistent pack: `PackId` is the hash of its bytes and its one
    /// chunk ends exactly at the end of the object.
    pub(in crate::repo) fn fake_pack(seed: u8) -> (PackId, Vec<u8>, PendingPackEntry) {
        let data: Vec<u8> = (0..64u8).map(|i| i.wrapping_mul(seed)).collect();
        let pack_id = PackId::compute(&data, HashAlgorithm::Blake2b);
        let entry = PendingPackEntry {
            pack_id,
            chunks: vec![PendingChunkEntry {
                chunk_id: ChunkId::from_bytes([seed; 32]),
                stored_size: 64 - 13,
                pack_offset: 13,
            }],
        };
        (pack_id, data, entry)
    }

    // ── Journal dirty guard ──────────────────────────────────────────────

    /// Counts journal PUTs; optionally fails the next one.
    struct JournalCountingBackend {
        inner: MemoryBackend,
        journal_puts: std::sync::atomic::AtomicUsize,
        fail_next: std::sync::atomic::AtomicBool,
    }

    impl JournalCountingBackend {
        fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                journal_puts: Default::default(),
                fail_next: Default::default(),
            }
        }
    }

    impl vykar_storage::InnerBackend for JournalCountingBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for JournalCountingBackend;
        except [put, put_pack, begin_staged_put, profile];

        // Keep packs on the intercepted `put` path.
        fn begin_staged_put(
            &self,
            _prefix: &str,
        ) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
            Ok(None)
        }

        fn profile(&self) -> vykar_storage::BackendProfile {
            vykar_storage::BackendProfile {
                staged_put: false,
                ..self.inner.profile()
            }
        }

        fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            use std::sync::atomic::Ordering;
            if key.ends_with(".index") {
                if self.fail_next.swap(false, Ordering::SeqCst) {
                    return Err(VykarError::Other("injected journal failure".into()));
                }
                self.journal_puts.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.put(key, data)
        }

        fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
            self.inner.put_pack(key, data)
        }
    }

    fn journal_session() -> WriteSessionState {
        init_test_environment();
        let mut ws = WriteSessionState::new(1 << 20, 1 << 20, 2, HashAlgorithm::Blake2b);
        ws.session_id = "s1".to_string();
        ws
    }

    fn chunk(b: u8) -> Vec<PendingChunkEntry> {
        vec![PendingChunkEntry {
            chunk_id: ChunkId::from_bytes([b; 32]),
            stored_size: 10,
            pack_offset: 0,
        }]
    }

    fn journal_puts(storage: &JournalCountingBackend) -> usize {
        storage
            .journal_puts
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn stored_pack_ids(storage: &dyn StorageBackend, crypto: &dyn CryptoEngine) -> Vec<PackId> {
        let data = storage.get(&session_index_key("s1")).unwrap().unwrap();
        let compressed = unpack_object_expect_with_context(
            &data,
            ObjectType::PendingIndex,
            PENDING_INDEX_OBJECT_CONTEXT,
            crypto,
        )
        .unwrap();
        let serialized = compress::decompress_metadata(&compressed).unwrap();
        match decode_pending_journal(&serialized).unwrap() {
            DecodedJournal::V2(wire) => wire.into_iter().map(|e| e.pack_id).collect(),
            other => panic!("session journal must be v2, got {other:?}"),
        }
    }

    #[test]
    fn wait_pending_uploads_skips_unchanged_journal() {
        let storage = JournalCountingBackend::new();
        let crypto = test_crypto();
        let mut ws = journal_session();
        ws.journal
            .entries
            .record_pack(PackId::from_bytes([1; 32]), chunk(1));

        ws.wait_pending_uploads(&storage, &crypto).unwrap();
        ws.wait_pending_uploads(&storage, &crypto).unwrap();
        assert_eq!(journal_puts(&storage), 1);
    }

    #[test]
    fn wait_pending_uploads_retries_after_failed_journal_write() {
        let storage = JournalCountingBackend::new();
        let crypto = test_crypto();
        let mut ws = journal_session();
        ws.journal
            .entries
            .record_pack(PackId::from_bytes([1; 32]), chunk(1));

        storage
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        ws.wait_pending_uploads(&storage, &crypto).unwrap();
        assert_eq!(journal_puts(&storage), 0);

        ws.wait_pending_uploads(&storage, &crypto).unwrap();
        assert_eq!(journal_puts(&storage), 1);
    }

    #[test]
    fn wait_pending_uploads_rewrites_after_remove_and_record() {
        let storage = JournalCountingBackend::new();
        let crypto = test_crypto();
        let mut ws = journal_session();
        let a = PackId::from_bytes([1; 32]);
        let b = PackId::from_bytes([2; 32]);
        ws.journal.entries.record_pack(a, chunk(1));
        ws.wait_pending_uploads(&storage, &crypto).unwrap();

        // Same length, different content.
        ws.journal.entries.remove_pack(&a);
        ws.journal.entries.record_pack(b, chunk(2));
        ws.wait_pending_uploads(&storage, &crypto).unwrap();

        assert_eq!(journal_puts(&storage), 2);
        assert_eq!(stored_pack_ids(&storage, &crypto), vec![b]);
    }

    /// Phase 1's final flush writes the journal in the background; the
    /// commit's flush joins it and does not write it again, or rewrites it
    /// synchronously when the background write failed.
    #[test]
    fn background_final_journal_write_is_joined_by_the_next_flush() {
        let storage = Arc::new(JournalCountingBackend::new());
        let dyn_storage: Arc<dyn StorageBackend> = storage.clone();
        let crypto: Arc<dyn CryptoEngine> = Arc::new(test_crypto());
        let mut ws = journal_session();
        ws.journal
            .entries
            .record_pack(PackId::from_bytes([1; 32]), chunk(1));

        ws.wait_pending_uploads_journal_in_background(&dyn_storage, &crypto)
            .unwrap();
        assert!(
            ws.journal.write_in_flight(),
            "final write runs in background"
        );
        ws.wait_pending_uploads(&*storage, &*crypto).unwrap();
        assert!(!ws.journal.write_in_flight());
        assert_eq!(journal_puts(&storage), 1);

        ws.journal
            .entries
            .record_pack(PackId::from_bytes([2; 32]), chunk(2));
        storage
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        ws.wait_pending_uploads_journal_in_background(&dyn_storage, &crypto)
            .unwrap();
        ws.wait_pending_uploads(&*storage, &*crypto).unwrap();
        assert_eq!(journal_puts(&storage), 2, "failed write retried inline");
        assert_eq!(
            stored_pack_ids(&*storage, &*crypto).len(),
            2,
            "journal holds both packs"
        );
    }

    fn record_packs(ws: &mut WriteSessionState, seeds: std::ops::Range<u8>) {
        for b in seeds {
            ws.journal
                .entries
                .record_pack(PackId::from_bytes([b; 32]), chunk(b));
        }
    }

    /// The debounced journal write runs in the background; the final write
    /// joins it first and then stores the newest state.
    #[test]
    fn background_journal_write_is_joined_before_final_write() {
        let storage = Arc::new(JournalCountingBackend::new());
        let dyn_storage: Arc<dyn StorageBackend> = storage.clone();
        let crypto: Arc<dyn CryptoEngine> = Arc::new(test_crypto());
        let mut ws = journal_session();
        record_packs(&mut ws, 1..1 + JOURNAL_WRITE_INTERVAL as u8);
        ws.maybe_write_pending_index(&dyn_storage, &crypto);
        assert!(
            ws.journal.write_in_flight(),
            "debounced write runs in background"
        );
        // Confirmed while the write is in flight: must still reach storage.
        record_packs(&mut ws, 100..100 + JOURNAL_WRITE_INTERVAL as u8);

        ws.wait_pending_uploads(&*storage, &*crypto).unwrap();
        assert!(!ws.journal.write_in_flight());
        assert_eq!(
            ws.journal.written_rev(),
            Some(ws.journal.entries.revision())
        );
        assert_eq!(
            stored_pack_ids(&*storage, &*crypto).len(),
            2 * JOURNAL_WRITE_INTERVAL
        );
    }

    /// A failed background write leaves its revision unwritten, so the next
    /// debounced call (or the final write) retries.
    #[test]
    fn failed_background_journal_write_is_retried() {
        let storage = Arc::new(JournalCountingBackend::new());
        let dyn_storage: Arc<dyn StorageBackend> = storage.clone();
        let crypto: Arc<dyn CryptoEngine> = Arc::new(test_crypto());
        let mut ws = journal_session();
        storage
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        record_packs(&mut ws, 1..1 + JOURNAL_WRITE_INTERVAL as u8);
        ws.maybe_write_pending_index(&dyn_storage, &crypto);
        ws.join_journal_write();
        assert_eq!(journal_puts(&storage), 0);
        assert_eq!(ws.journal.written_rev(), None);

        ws.maybe_write_pending_index(&dyn_storage, &crypto);
        ws.join_journal_write();
        assert_eq!(journal_puts(&storage), 1);
        assert_eq!(
            ws.journal.written_rev(),
            Some(ws.journal.entries.revision())
        );
    }

    #[test]
    fn journal_revision_tracks_mutations() {
        let mut j = PendingIndexJournal::new();
        let r0 = j.revision();
        j.record_pack(PackId::from_bytes([1; 32]), chunk(1));
        let r1 = j.revision();
        assert_ne!(r0, r1);
        j.remove_pack(&PackId::from_bytes([1; 32]));
        assert_ne!(r1, j.revision());

        j.record_pack(PackId::from_bytes([3; 32]), chunk(3));
        let back = PendingIndexJournal::from_wire(j.to_wire());
        assert_eq!(back.len(), 1);
        assert_eq!(back.to_wire()[0].pack_id, PackId::from_bytes([3; 32]));
    }

    // ── Upload confirmation ──────────────────────────────────────────────

    fn spawn_upload(
        ws: &mut WriteSessionState,
        storage: &std::sync::Arc<PackUploadBackend>,
        seed: u8,
    ) {
        let (pack_id, data, entry) = fake_pack(seed);
        let storage = std::sync::Arc::clone(storage);
        ws.spawn_upload(pack_id, entry.chunks, 0, move |_| {
            storage.put_owned(&pack_id.storage_key(), data)
        });
    }

    /// Queue an upload that holds `bytes` and completes only when the
    /// returned sender fires.
    fn gated_upload(
        ws: &mut WriteSessionState,
        storage: &std::sync::Arc<PackUploadBackend>,
        seed: u8,
        bytes: usize,
    ) -> std::sync::mpsc::Sender<()> {
        let (pack_id, data, entry) = fake_pack(seed);
        let storage = std::sync::Arc::clone(storage);
        let (release, gate) = std::sync::mpsc::channel::<()>();
        ws.spawn_upload(pack_id, entry.chunks, bytes, move |_held| {
            let _ = gate.recv();
            storage.put_owned(&pack_id.storage_key(), data)
        });
        release
    }

    #[test]
    fn byte_cap_blocks_until_queued_pack_is_written() {
        let storage = std::sync::Arc::new(PackUploadBackend::new(std::sync::Arc::new(
            MemoryBackend::new(),
        )));
        let crypto = test_crypto();
        let dyn_storage: Arc<dyn StorageBackend> = storage.clone();
        let dyn_crypto: Arc<dyn CryptoEngine> = Arc::new(test_crypto());
        let mut ws = journal_session();
        let target = ws.data_pack_writer.target_size();

        // Disabled: a queued pack plus the incoming one may exceed the
        // target; only the count limit applies.
        let release = gated_upload(&mut ws, &storage, 1, target);
        ws.cap_pending_uploads(&dyn_storage, &dyn_crypto, target)
            .unwrap();
        assert_eq!(ws.uploads.len(), 1);
        release.send(()).unwrap();
        ws.wait_pending_uploads(&*storage, &crypto).unwrap();

        // Enabled: the same call waits for the queued pack to be written.
        ws.uploads.cap_pending_upload_bytes = true;
        let release = gated_upload(&mut ws, &storage, 2, target);
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            release.send(()).unwrap();
        });
        let start = std::time::Instant::now();
        ws.cap_pending_uploads(&dyn_storage, &dyn_crypto, target)
            .unwrap();
        assert!(start.elapsed() >= std::time::Duration::from_millis(100));
        assert!(ws.uploads.is_empty());
        releaser.join().unwrap();

        // A pack that fits next to the queued one does not block.
        let release = gated_upload(&mut ws, &storage, 3, target / 4);
        ws.cap_pending_uploads(&dyn_storage, &dyn_crypto, target / 2)
            .unwrap();
        assert_eq!(ws.uploads.len(), 1);
        release.send(()).unwrap();
        ws.wait_pending_uploads(&*storage, &crypto).unwrap();

        // An empty queue never blocks, even for an oversized pack.
        ws.cap_pending_uploads(&dyn_storage, &dyn_crypto, 4 * target)
            .unwrap();
    }

    #[test]
    fn upload_slack_counts_unfinished_packs_beyond_the_slots() {
        let storage = std::sync::Arc::new(PackUploadBackend::new(std::sync::Arc::new(
            MemoryBackend::new(),
        )));
        let crypto = test_crypto();
        let mut ws = journal_session();
        ws.uploads.max_in_flight_uploads = 2;
        let target = ws.data_pack_writer.target_size();

        // Within the slots: nothing to lend.
        let r1 = gated_upload(&mut ws, &storage, 1, target);
        let r2 = gated_upload(&mut ws, &storage, 2, target);
        assert_eq!(ws.upload_slack_bytes(), 0);
        // Two packs queued behind the slots: half of them is lent.
        let r3 = gated_upload(&mut ws, &storage, 3, target);
        let r4 = gated_upload(&mut ws, &storage, 4, target);
        assert_eq!(ws.upload_slack_bytes(), target);
        // A finished (not yet joined) upload no longer counts.
        r1.send(()).unwrap();
        let start = std::time::Instant::now();
        while ws.upload_slack_bytes() != target / 2 {
            assert!(start.elapsed() < std::time::Duration::from_secs(10));
            std::thread::yield_now();
        }
        // The local byte cap bounds the queue itself: never lend.
        ws.uploads.cap_pending_upload_bytes = true;
        assert_eq!(ws.upload_slack_bytes(), 0);

        for r in [r2, r3, r4] {
            r.send(()).unwrap();
        }
        ws.wait_pending_uploads(&*storage, &crypto).unwrap();
    }

    #[test]
    fn failed_uploads_are_never_journaled() {
        let storage = std::sync::Arc::new(PackUploadBackend::new(std::sync::Arc::new(
            MemoryBackend::new(),
        )));
        let crypto = test_crypto();
        let mut ws = journal_session();
        let (bad, _, _) = fake_pack(2);
        storage.fail.lock().unwrap().insert(bad.storage_key());

        spawn_upload(&mut ws, &storage, 1);
        spawn_upload(&mut ws, &storage, 2);
        spawn_upload(&mut ws, &storage, 3);

        // What `flush_on_abort` does: join everything, warn on failure.
        for upload in ws.uploads.take_all() {
            let _ = ws.finish_upload(upload);
        }
        ws.write_pending_index_best_effort(&*storage, &crypto);

        let mut journaled = stored_pack_ids(&*storage, &crypto);
        journaled.sort_by_key(|p| *p.as_bytes());
        let mut expected = vec![fake_pack(1).0, fake_pack(3).0];
        expected.sort_by_key(|p| *p.as_bytes());
        assert_eq!(journaled, expected);
    }

    #[test]
    fn wait_pending_uploads_journals_only_after_success() {
        let storage = std::sync::Arc::new(PackUploadBackend::new(std::sync::Arc::new(
            MemoryBackend::new(),
        )));
        let crypto = test_crypto();
        let mut ws = journal_session();
        let (bad, _, _) = fake_pack(2);
        storage.fail.lock().unwrap().insert(bad.storage_key());
        spawn_upload(&mut ws, &storage, 1);
        spawn_upload(&mut ws, &storage, 2);

        assert!(ws.wait_pending_uploads(&*storage, &crypto).is_err());
        assert_eq!(stored_pack_ids(&*storage, &crypto), vec![fake_pack(1).0]);
    }

    /// With every upload slot taken, the consumer resumes as soon as any
    /// upload finishes — not only when the oldest (or an arbitrary) one does.
    #[test]
    fn count_cap_wakes_on_first_finished_upload() {
        let storage = std::sync::Arc::new(PackUploadBackend::new(std::sync::Arc::new(
            MemoryBackend::new(),
        )));
        let crypto = test_crypto();
        let dyn_storage: Arc<dyn StorageBackend> = storage.clone();
        let dyn_crypto: Arc<dyn CryptoEngine> = Arc::new(test_crypto());
        let mut ws = journal_session();
        let slots = ws.uploads.max_in_flight_uploads + UPLOAD_QUEUE_HEADROOM;
        let mut gates: Vec<_> = (1..=slots as u8)
            .map(|seed| gated_upload(&mut ws, &storage, seed, 0))
            .collect();
        let last = gates.pop().unwrap();
        last.send(()).unwrap();
        // Releases the rest once the cap returns, or after a timeout so a
        // regression fails the assertions below instead of hanging.
        let (returned, wait) = std::sync::mpsc::channel::<()>();
        let watchdog = std::thread::spawn(move || {
            let _ = wait.recv_timeout(std::time::Duration::from_secs(10));
            for gate in gates {
                let _ = gate.send(());
            }
        });

        let start = std::time::Instant::now();
        ws.cap_pending_uploads(&dyn_storage, &dyn_crypto, 0)
            .unwrap();
        returned.send(()).unwrap();
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
        assert_eq!(ws.uploads.len(), slots - 1);
        let journaled: Vec<PackId> = ws
            .journal
            .entries
            .to_wire()
            .iter()
            .map(|e| e.pack_id)
            .collect();
        assert_eq!(journaled, vec![fake_pack(slots as u8).0]);

        watchdog.join().unwrap();
        ws.wait_pending_uploads(&*storage, &crypto).unwrap();
        assert_eq!(ws.journal.entries.len(), slots);
    }

    /// Dropping the session joins a background journal write before the
    /// session's other fields drop, so the write still lands.
    #[test]
    fn dropping_session_joins_background_journal_write() {
        let backend = Arc::new(crate::repo::session_journal::tests::SlowPutBackend::new());
        let storage: Arc<dyn StorageBackend> = backend.clone();
        let crypto: Arc<dyn CryptoEngine> = Arc::new(test_crypto());
        let mut ws = journal_session();
        record_packs(&mut ws, 1..1 + JOURNAL_WRITE_INTERVAL as u8);
        ws.maybe_write_pending_index(&storage, &crypto);
        assert!(ws.journal.write_in_flight());

        drop(ws);
        assert!(backend.put_done.load(std::sync::atomic::Ordering::SeqCst));
        assert!(backend.inner.exists(&session_index_key("s1")).unwrap());
    }
}
