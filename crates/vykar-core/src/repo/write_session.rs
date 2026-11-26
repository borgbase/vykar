use std::collections::HashMap as StdHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::index::hasher::ChunkIdHashMap;
use crate::index::table::{IndexState, IndexView};
use crate::index::{
    IndexDelta, LiveIndex, PendingChunkEntry, PendingIndexJournal, RecoveredChunkEntry,
};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::hash::HashAlgorithm;
use vykar_types::pack_id::PackId;

use super::host_env::Clock;
use super::journal_recovery::{self, PendingIndexRecovery, RecoveryTarget};
use super::memory_budget::{HeldBytes, MemoryBudget};
use super::pack::{PackType, PackWriter, PackedChunkEntry};
use super::session_journal::SessionJournal;
use super::upload_queue::{ConfirmedPack, PendingUpload, UploadQueue};

pub(super) const DEFAULT_SESSION_ID: &str = "default";

/// Diagnostic counters of one write session, for the backup's debug summary.
/// Updated on the thread that seals packs; `upload_put_nanos` and
/// `upload_seal_nanos` by upload threads.
#[derive(Debug, Default)]
pub(crate) struct WriteMetrics {
    pub(crate) counters: WriteMetricsSnapshot,
    /// Sum of `put_to` durations over all upload threads (they overlap, so this
    /// can exceed wall time). Read after the uploads are joined.
    pub(crate) upload_put_nanos: Arc<AtomicU64>,
    /// Sum of `PackTail::finish` durations over all upload threads (trailer
    /// encryption and the pack ID hash); overlapping like `upload_put_nanos`.
    pub(crate) upload_seal_nanos: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WriteMetricsSnapshot {
    pub(crate) data_packs: u64,
    pub(crate) tree_packs: u64,
    pub(crate) data_pack_bytes: u64,
    pub(crate) tree_pack_bytes: u64,
    /// Time the sealing thread spent closing packs (`PackWriter::close`);
    /// the upload threads build the trailer and the pack ID (`upload_seal`).
    pub(crate) seal: Duration,
    /// Time the sealing thread spent in `wait_for_pack_room` (upload backpressure).
    pub(crate) upload_cap_wait: Duration,
    /// Time spent joining uploads in `flush_packs*`.
    pub(crate) final_upload_wait: Duration,
    /// Filled from `upload_put_nanos` by `snapshot()`.
    pub(crate) upload_put: Duration,
    /// Filled from `upload_seal_nanos` by `snapshot()`.
    pub(crate) upload_seal: Duration,
}

impl WriteMetrics {
    pub(crate) fn snapshot(&self) -> WriteMetricsSnapshot {
        WriteMetricsSnapshot {
            upload_put: Duration::from_nanos(self.upload_put_nanos.load(Ordering::Relaxed)),
            upload_seal: Duration::from_nanos(self.upload_seal_nanos.load(Ordering::Relaxed)),
            ..self.counters
        }
    }
}

/// Saturating `Duration` → u64 nanoseconds (584 years).
pub(crate) fn duration_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// Write-path state that is active during a backup session.
///
/// Composes the pack writers, the upload queue, the session's crash-recovery
/// journal, the dedup / index-delta state and the chunks recovered
/// from interrupted sessions — all transient state that only exists while
/// chunks are being written. It wires each confirmed upload into the journal.
pub struct WriteSession {
    /// Crash-recovery journal of confirmed packs and its writes. Declared
    /// first so its `Drop` (joining a background write) runs before any other
    /// field is dropped.
    pub(crate) journal: SessionJournal,
    /// Pack writer for file content chunks.
    pub(crate) data_pack_writer: PackWriter,
    /// Pack writer for item-stream metadata chunks.
    pub(crate) tree_pack_writer: PackWriter,
    /// Pack uploads, run on a fixed pool of upload threads; a pack enters
    /// `journal`, `index_delta` and `pack_trailers` only after its upload
    /// returns `Ok`. Its first failure fails the session
    /// (`refuse_if_failed`).
    pub(crate) uploads: UploadQueue,
    /// The bytes the backup buffers: its pipeline, open packs and sealed
    /// packs waiting for upload. Shared with the pipeline's walk thread and
    /// with the upload threads, which release sealed packs into it.
    pub(crate) memory: Arc<MemoryBudget>,
    /// Stream data packs into the backend's staging area as they fill
    /// instead of buffering them on the heap, when the backend supports it
    /// (`StorageBackend::begin_staged_put`; local repositories). A staged
    /// pack holds no heap, so it charges nothing to `memory`.
    pub(crate) stream_data_packs: bool,
    /// What lookups and positional reuse resolve against: the repository's
    /// index when the session began, unless it was edited or not loaded
    /// (then every lookup goes through the live index by key). Immutable for
    /// the session.
    pub(crate) view: Option<SessionView>,
    /// `trailer_len` of every pack whose upload this session confirmed, or
    /// that it recovered.
    pub(crate) pack_trailers: StdHashMap<PackId, u32>,
    /// Chunks sealed or promoted in this session -> stored size.
    pub(crate) session_new: ChunkIdHashMap<u32>,
    /// Every index mutation of the session; `Some` from
    /// `begin_write_session` until the commit or save takes it.
    pub(crate) index_delta: Option<IndexDelta>,
    /// Chunks recovered from a previous interrupted session's `pending_index`.
    pub(crate) recovered_chunks: StdHashMap<ChunkId, RecoveredChunkEntry>,
    /// `.index` keys to delete after successful commit.
    /// `.json` markers are never deleted here — their lifecycle is managed
    /// exclusively by `deregister_session()` (normal exit) and
    /// `cleanup_stale_sessions()` (newest marker written >45 min ago by its
    /// mtime, under maintenance lock).
    pub(crate) recovered_index_keys: Vec<String>,
    /// Number of distinct packs (data + tree) in the persisted index at load time.
    pub(crate) persisted_pack_count: usize,
    /// Number of data packs flushed during the current session.
    pub(crate) session_packs_flushed: usize,
    /// Session ID for per-session pending_index files.
    /// Defaults to `"default"` for non-backup callers/tests.
    pub(crate) session_id: String,
    /// Diagnostic counters for the backup's debug summary.
    pub(crate) metrics: WriteMetrics,
    /// Accepts a pack upload an append-only server refused because the pack
    /// is stored already; set by `begin_write_session`.
    pub(crate) existing_packs: Option<std::sync::Arc<super::existing_pack::ExistingPacks>>,
    /// Set by the methods that consume the session (a save, a commit, an
    /// abort flush): its `Drop` no longer warns.
    pub(crate) finished: bool,
}

impl Drop for WriteSession {
    fn drop(&mut self) {
        if !self.finished && self.holds_unflushed_work() {
            tracing::warn!("write session dropped with unflushed packs or journal entries");
        }
    }
}

/// The index state a write session dedups against, captured at its begin
/// (R3 of 05 step 9): the view, the objects it was built from and the
/// head's `sweep.gen` then. The commit compares them with the latest head
/// to decide whether reused chunks must be checked again.
pub(crate) struct SessionView {
    pub(crate) view: IndexView,
    pub(crate) state: IndexState,
    pub(crate) sweep_gen: u64,
}

impl WriteSession {
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
            memory: MemoryBudget::new(max_in_flight_uploads, data_target, tree_target),
            stream_data_packs: false,
            view: None,
            pack_trailers: StdHashMap::new(),
            session_new: ChunkIdHashMap::default(),
            index_delta: None,
            recovered_chunks: StdHashMap::new(),
            recovered_index_keys: Vec::new(), // only .index keys, never .json
            persisted_pack_count: 0,
            session_packs_flushed: 0,
            session_id: DEFAULT_SESSION_ID.to_string(),
            metrics: WriteMetrics::default(),
            existing_packs: None,
            finished: false,
        }
    }

    /// Partial packs, queued uploads, or journal entries not yet written:
    /// work a drop of the session would lose.
    pub(crate) fn holds_unflushed_work(&self) -> bool {
        self.data_pack_writer.has_pending()
            || self.tree_pack_writer.has_pending()
            || !self.uploads.is_empty()
            || self.journal.needs_write()
    }

    /// Whether an upload or submit of this session failed: its pack is
    /// lost while its chunks stay dedup hits (`session_new`), so the
    /// session stores and saves nothing more.
    pub(crate) fn upload_failed(&self) -> bool {
        self.uploads.failure().is_some()
    }

    /// Err once an upload of this session failed (see
    /// [`Self::upload_failed`]); the error carries the first failure's text.
    pub(super) fn refuse_if_failed(&self) -> Result<()> {
        match self.uploads.failure() {
            Some(first) => Err(VykarError::Other(format!(
                "a pack upload of this write session failed, so it stores and saves nothing \
                 more: {first}"
            ))),
            None => Ok(()),
        }
    }

    /// Queue `upload` for an upload thread; it returns the ID of the pack
    /// it stored, whose `entries` and `trailer_len` are recorded once it is
    /// confirmed. `held` is the pack's charge to `memory`, released when
    /// the upload drops it or returns. Err only when an upload thread cannot
    /// be started.
    pub(crate) fn submit_upload<F>(
        &mut self,
        entries: Vec<PackedChunkEntry>,
        trailer_len: u32,
        held: HeldBytes,
        upload: F,
    ) -> Result<()>
    where
        F: FnOnce(HeldBytes) -> Result<PackId> + Send + 'static,
    {
        // The queue's slot count is the source (tests change it after
        // `new`); lending uses the same one.
        self.memory.set_slots(self.uploads.max_in_flight_uploads);
        self.uploads.submit(entries, trailer_len, held, upload)
    }

    /// Join one upload and record its pack if it succeeded. A failed
    /// upload is never journaled.
    pub(crate) fn finish_upload(&mut self, upload: PendingUpload) -> Result<()> {
        let confirmed = UploadQueue::finish(upload).map_err(|e| self.uploads.record_failure(e))?;
        record_confirmed(
            &mut self.journal.entries,
            &mut self.pack_trailers,
            &mut self.index_delta,
            confirmed,
        );
        Ok(())
    }

    /// Join all finished uploads, propagating the first error.
    pub(crate) fn drain_finished_uploads(
        &mut self,
        storage: &Arc<dyn StorageBackend>,
        crypto: &Arc<dyn CryptoEngine>,
    ) -> Result<()> {
        let (journal, trailers, delta) = (
            &mut self.journal.entries,
            &mut self.pack_trailers,
            &mut self.index_delta,
        );
        self.uploads
            .drain_finished(|c| record_confirmed(journal, trailers, delta, c))?;
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
        let (journal, trailers, delta) = (
            &mut self.journal.entries,
            &mut self.pack_trailers,
            &mut self.index_delta,
        );
        let result = self
            .uploads
            .join_all(|c| record_confirmed(journal, trailers, delta, c));
        self.join_journal_write();
        result
    }

    /// Before closing a pack of `incoming` heap bytes: join finished uploads
    /// (journaling them), wait for an upload slot, then, as the memory
    /// budget's policy asks (`MemoryBudget::sealed_room`; local
    /// repositories), join uploads until the sealed packs plus `incoming`
    /// fit the data-pack target or none is pending. An empty queue never
    /// blocks, so one upload is always in flight. Never fails because the
    /// pipeline was poisoned: an aborted backup still closes its packs.
    /// Session-marker refreshes run in a dedicated heartbeat thread owned by
    /// `SessionGuard`, so this path does not touch the marker.
    pub(crate) fn wait_for_pack_room(
        &mut self,
        storage: &Arc<dyn StorageBackend>,
        crypto: &Arc<dyn CryptoEngine>,
        incoming: usize,
    ) -> Result<()> {
        self.drain_finished_uploads(storage, crypto)?;
        let (journal, trailers, delta) = (
            &mut self.journal.entries,
            &mut self.pack_trailers,
            &mut self.index_delta,
        );
        if self
            .uploads
            .wait_for_slot(|c| record_confirmed(journal, trailers, delta, c))?
        {
            self.maybe_write_pending_index(storage, crypto);
        }
        // Without it, the slot bound alone allows W + H sealed packs of up
        // to the 192 MiB target each; when the disk falls behind, they pile
        // up and peak RSS jumps by one pack per straggler.
        let mut joined = false;
        while !self.memory.sealed_room(incoming) {
            let (journal, trailers, delta) = (
                &mut self.journal.entries,
                &mut self.pack_trailers,
                &mut self.index_delta,
            );
            if !self
                .uploads
                .join_one(|c| record_confirmed(journal, trailers, delta, c))?
            {
                break;
            }
            joined = true;
        }
        if joined {
            self.maybe_write_pending_index(storage, crypto);
        }
        Ok(())
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

    // --- Closed and confirmed packs ---

    /// Make a closed pack's chunks dedup hits at once (`session_new`); its
    /// index entries wait for the upload (`record_confirmed`).
    pub(crate) fn record_closed(&mut self, entries: &[PackedChunkEntry]) {
        for &(chunk_id, stored_size, _) in entries {
            self.session_new.insert(chunk_id, stored_size);
        }
    }

    /// Recover chunk→pack mappings from interrupted sessions' journals into
    /// this session; see `journal_recovery::recover_pending_index`.
    pub(crate) fn recover_pending_index_from(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        chunk_index: &LiveIndex,
        width: usize,
        clock: &dyn Clock,
        cancel: &super::control::CancelToken,
    ) -> Result<PendingIndexRecovery> {
        journal_recovery::recover_pending_index(
            storage,
            crypto,
            chunk_index,
            &self.session_id,
            self.persisted_pack_count,
            width,
            &mut RecoveryTarget {
                clock,
                cancel,
                recovered_chunks: &mut self.recovered_chunks,
                recovered_index_keys: &mut self.recovered_index_keys,
                journal: &mut self.journal.entries,
                pack_trailers: &mut self.pack_trailers,
            },
        )
    }

    /// Delete recovered `.index` files from storage after successful commit.
    /// Never deletes `.json` markers. Best-effort: logs on failure.
    pub(crate) fn cleanup_recovered_indices(&mut self, storage: &dyn StorageBackend) {
        journal_recovery::delete_recovered_indices(storage, &mut self.recovered_index_keys);
    }

    /// Promote a recovered chunk into the session (`session_new` and the
    /// delta, as a new entry). Returns the stored size if
    /// the chunk was in `recovered_chunks`, None otherwise.
    pub(crate) fn promote_recovered_chunk(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        let entry = self.recovered_chunks.remove(chunk_id)?;

        self.session_new.insert(*chunk_id, entry.stored_size);
        if let Some(ref mut delta) = self.index_delta {
            delta.add_new_entry(
                *chunk_id,
                entry.stored_size,
                entry.pack_id,
                entry.pack_offset,
            );
        }

        Some(entry.stored_size)
    }

    // --- Dedup query helpers ---

    /// Check only pending pack writers for a dedup hit (shared helper). A
    /// pending chunk is among the session's own additions once its pack is
    /// sealed, so no reuse is recorded.
    pub(super) fn find_pending(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        self.data_pack_writer
            .get_pending_stored_size(chunk_id)
            .or_else(|| self.tree_pack_writer.get_pending_stored_size(chunk_id))
    }
}

/// A confirmed upload: its pack enters the journal, its chunks the
/// session's delta and its trailer length `pack_trailers`. A free function
/// over the three fields, so a caller can hold `&mut self.uploads` at the
/// same time.
fn record_confirmed(
    journal: &mut PendingIndexJournal,
    pack_trailers: &mut StdHashMap<PackId, u32>,
    index_delta: &mut Option<IndexDelta>,
    confirmed: ConfirmedPack,
) {
    let ConfirmedPack {
        pack_id,
        entries,
        trailer_len,
    } = confirmed;
    let chunks: Vec<PendingChunkEntry> = entries
        .iter()
        .map(|&(chunk_id, stored_size, offset)| PendingChunkEntry {
            chunk_id,
            stored_size,
            pack_offset: offset,
        })
        .collect();
    journal.record_pack(pack_id, chunks);
    pack_trailers.insert(pack_id, trailer_len);
    if let Some(delta) = index_delta {
        for (chunk_id, stored_size, offset) in entries {
            delta.add_new_entry(chunk_id, stored_size, pack_id, offset);
        }
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
    use vykar_crypto::testing::DeterministicEngine;
    use vykar_types::error::VykarError;

    /// A deterministic AEAD engine for tests.
    pub(in crate::repo) fn test_crypto() -> DeterministicEngine {
        DeterministicEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]))
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
        except [put, put_pack, begin_staged_put, conditional, profile];

        // Test wrapper: keeps every write on `put`.
        fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
            None
        }

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

    fn journal_session() -> WriteSession {
        init_test_environment();
        let mut ws = WriteSession::new(1 << 20, 1 << 20, 2, HashAlgorithm::Blake2b);
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

    fn record_packs(ws: &mut WriteSession, seeds: std::ops::Range<u8>) {
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

    /// `fake_pack`'s chunks as the entries of a closed pack.
    fn fake_entries(entry: &PendingPackEntry) -> Vec<PackedChunkEntry> {
        entry
            .chunks
            .iter()
            .map(|c| (c.chunk_id, c.stored_size, c.pack_offset))
            .collect()
    }

    fn spawn_upload(ws: &mut WriteSession, storage: &std::sync::Arc<PackUploadBackend>, seed: u8) {
        let (pack_id, data, entry) = fake_pack(seed);
        let storage = std::sync::Arc::clone(storage);
        ws.submit_upload(fake_entries(&entry), 0, HeldBytes::none(), move |_| {
            storage.put_owned(&pack_id.storage_key(), data)?;
            Ok(pack_id)
        })
        .unwrap();
    }

    /// Queue an upload that holds `bytes` and completes only when the
    /// returned sender fires.
    fn gated_upload(
        ws: &mut WriteSession,
        storage: &std::sync::Arc<PackUploadBackend>,
        seed: u8,
        bytes: usize,
    ) -> std::sync::mpsc::Sender<()> {
        let (pack_id, data, entry) = fake_pack(seed);
        let storage = std::sync::Arc::clone(storage);
        let (release, gate) = std::sync::mpsc::channel::<()>();
        let held = ws.memory.hold_sealed(bytes);
        ws.submit_upload(fake_entries(&entry), 0, held, move |_held| {
            let _ = gate.recv();
            storage.put_owned(&pack_id.storage_key(), data)?;
            Ok(pack_id)
        })
        .unwrap();
        release
    }

    #[test]
    fn byte_cap_blocks_until_queued_pack_is_written() {
        use crate::repo::memory_budget::PackPolicy;

        let storage = std::sync::Arc::new(PackUploadBackend::new(std::sync::Arc::new(
            MemoryBackend::new(),
        )));
        let crypto = test_crypto();
        let dyn_storage: Arc<dyn StorageBackend> = storage.clone();
        let dyn_crypto: Arc<dyn CryptoEngine> = Arc::new(test_crypto());
        let mut ws = journal_session();
        let target = ws.data_pack_writer.target_size();

        // LendHalf: a queued pack plus the incoming one may exceed the
        // target; only the slot bound applies.
        assert_eq!(ws.memory.pack_policy(), PackPolicy::LendHalf);
        let release = gated_upload(&mut ws, &storage, 1, target);
        ws.wait_for_pack_room(&dyn_storage, &dyn_crypto, target)
            .unwrap();
        assert_eq!(ws.uploads.len(), 1);
        release.send(()).unwrap();
        ws.wait_pending_uploads(&*storage, &crypto).unwrap();

        // CapAtTarget: the same call waits for the queued pack to be written.
        ws.memory.configure(PackPolicy::CapAtTarget, true);
        let release = gated_upload(&mut ws, &storage, 2, target);
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            release.send(()).unwrap();
        });
        let start = std::time::Instant::now();
        ws.wait_for_pack_room(&dyn_storage, &dyn_crypto, target)
            .unwrap();
        assert!(start.elapsed() >= std::time::Duration::from_millis(100));
        assert!(ws.uploads.is_empty());
        releaser.join().unwrap();

        // A pack that fits next to the queued one does not block.
        let release = gated_upload(&mut ws, &storage, 3, target / 4);
        ws.wait_for_pack_room(&dyn_storage, &dyn_crypto, target / 2)
            .unwrap();
        assert_eq!(ws.uploads.len(), 1);
        release.send(()).unwrap();
        ws.wait_pending_uploads(&*storage, &crypto).unwrap();

        // An empty queue never blocks, even for an oversized pack.
        ws.wait_for_pack_room(&dyn_storage, &dyn_crypto, 4 * target)
            .unwrap();
        assert_eq!(ws.memory.snapshot().sealed_packs, 0);
    }

    /// LendHalf never waits on bytes, however much is sealed.
    #[test]
    fn lend_half_never_waits_on_bytes() {
        let storage = std::sync::Arc::new(PackUploadBackend::new(std::sync::Arc::new(
            MemoryBackend::new(),
        )));
        let crypto = test_crypto();
        let dyn_storage: Arc<dyn StorageBackend> = storage.clone();
        let dyn_crypto: Arc<dyn CryptoEngine> = Arc::new(test_crypto());
        let mut ws = journal_session();
        let target = ws.data_pack_writer.target_size();
        let release = gated_upload(&mut ws, &storage, 1, 10 * target);
        ws.wait_for_pack_room(&dyn_storage, &dyn_crypto, 10 * target)
            .unwrap();
        assert_eq!(ws.uploads.len(), 1);
        release.send(()).unwrap();
        ws.wait_pending_uploads(&*storage, &crypto).unwrap();
    }

    #[test]
    fn lending_counts_unfinished_packs_beyond_the_slots() {
        use crate::repo::memory_budget::PackPolicy;

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
        assert_eq!(ws.memory.lent(), 0);
        // Two packs queued behind the slots: half of them is lent.
        let r3 = gated_upload(&mut ws, &storage, 3, target);
        let r4 = gated_upload(&mut ws, &storage, 4, target);
        assert_eq!(ws.memory.lent(), target);
        // A finished (not yet joined) upload no longer counts.
        r1.send(()).unwrap();
        let start = std::time::Instant::now();
        while ws.memory.lent() != target / 2 {
            assert!(start.elapsed() < std::time::Duration::from_secs(10));
            std::thread::yield_now();
        }
        assert_eq!(ws.uploads.len(), 4);
        // The local cap bounds the queue itself: never lend.
        ws.memory.configure(PackPolicy::CapAtTarget, true);
        assert_eq!(ws.memory.lent(), 0);

        for r in [r2, r3, r4] {
            r.send(()).unwrap();
        }
        ws.wait_pending_uploads(&*storage, &crypto).unwrap();
        assert_eq!(ws.memory.snapshot().sealed_packs, 0);
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
        ws.wait_for_pack_room(&dyn_storage, &dyn_crypto, 0).unwrap();
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
