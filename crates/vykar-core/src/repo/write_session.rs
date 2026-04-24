use std::collections::HashMap as StdHashMap;
use std::thread::JoinHandle;

use tracing::{debug, warn};

use crate::compress;
use crate::index::dedup_cache::TieredDedupIndex;
use crate::index::{
    ChunkIndex, DedupIndex, IndexDelta, IndexDeltaCheckpoint, PendingIndexJournal,
    RecoveredChunkEntry,
};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::format::{pack_object_with_context, unpack_object_expect_with_context, ObjectType};
use super::lock::{session_index_key, SessionEntry, SESSIONS_PREFIX, SESSION_STALE_SECS};
use super::pack::{PackType, PackWriter, PackedChunkEntry};

/// Extra upload handles allowed beyond `max_in_flight_uploads` before blocking.
const UPLOAD_QUEUE_HEADROOM: usize = 2;

/// Number of new packs between debounced `pending_index` writes.
const JOURNAL_WRITE_INTERVAL: usize = 8;

const DEFAULT_SESSION_ID: &str = "default";
const PENDING_INDEX_OBJECT_CONTEXT: &[u8] = b"pending_index";

/// Result of recovering pending index journals from interrupted sessions.
pub struct PendingIndexRecovery {
    /// Total number of chunk entries recovered.
    pub recovered_chunks: usize,
    /// Session IDs whose journals were successfully recovered.
    pub recovered_sessions: Vec<String>,
}

/// Result of attempting to recover a single session's pending index journal.
enum RecoverResult {
    /// No journal data found at key.
    NotFound,
    /// Journal was corrupt (decrypt/decompress/deserialize failed).
    Corrupt,
    /// Successfully recovered N chunk entries (may be 0 if all already indexed).
    Ok(usize),
}

/// Marker state for a session's `.json` file during recovery scanning.
#[derive(Debug)]
enum MarkerState {
    /// Marker exists, parseable, `last_refresh` within `SESSION_STALE_SECS`.
    Active,
    /// Marker exists, parseable, `last_refresh` strictly older than
    /// `SESSION_STALE_SECS`.
    Stale,
    /// Marker exists but unreadable or unparseable.
    Unknown,
}

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
/// Groups pack assembly, upload queue, dedup mode, crash recovery journal,
/// and pack sizing counters — all transient state that only exists while
/// chunks are being written.
pub(crate) struct WriteSessionState {
    /// Pack writer for file content chunks.
    pub(crate) data_pack_writer: PackWriter,
    /// Pack writer for item-stream metadata chunks.
    pub(crate) tree_pack_writer: PackWriter,
    /// Background pack upload threads waiting to be joined.
    pub(crate) pending_uploads: Vec<JoinHandle<Result<()>>>,
    /// Configurable limit for in-flight background pack uploads.
    pub(crate) max_in_flight_uploads: usize,
    /// Lightweight dedup-only index used during backup to save memory.
    /// When active, `chunk_index` on Repository is empty.
    pub(crate) dedup_index: Option<DedupIndex>,
    /// Three-tier dedup index (xor filter + mmap + session HashMap).
    /// When active, both `chunk_index` and `dedup_index` are empty.
    pub(crate) tiered_dedup: Option<TieredDedupIndex>,
    /// Tracks index mutations while in dedup mode, applied at save time.
    pub(crate) index_delta: Option<IndexDelta>,
    /// Journal of chunk→pack mappings for packs flushed in the current session.
    pub(crate) pending_journal: PendingIndexJournal,
    /// Number of packs in journal when last written to storage (for debouncing).
    pub(crate) pending_journal_last_written: usize,
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

impl WriteSessionState {
    /// Create a new write session state with the given pack writer targets.
    pub(crate) fn new(
        data_target: usize,
        tree_target: usize,
        max_in_flight_uploads: usize,
    ) -> Self {
        Self {
            data_pack_writer: PackWriter::new(PackType::Data, data_target),
            tree_pack_writer: PackWriter::new(PackType::Tree, tree_target),
            pending_uploads: Vec::new(),
            max_in_flight_uploads,
            dedup_index: None,
            tiered_dedup: None,
            index_delta: None,
            pending_journal: PendingIndexJournal::new(),
            pending_journal_last_written: 0,
            recovered_chunks: StdHashMap::new(),
            recovered_index_keys: Vec::new(), // only .index keys, never .json
            persisted_pack_count: 0,
            session_packs_flushed: 0,
            session_id: DEFAULT_SESSION_ID.to_string(),
            rollback_tracker: None,
        }
    }

    /// Return the storage key for this session's pending_index.
    fn pending_index_key(&self) -> String {
        session_index_key(&self.session_id)
    }

    // --- Upload management ---

    /// Join all finished upload threads, propagating the first error.
    pub(crate) fn drain_finished_uploads(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<()> {
        let mut i = 0;
        while i < self.pending_uploads.len() {
            if self.pending_uploads[i].is_finished() {
                let handle = self.pending_uploads.swap_remove(i);
                handle
                    .join()
                    .map_err(|_| VykarError::Other("pack upload thread panicked".into()))??;
            } else {
                i += 1;
            }
        }
        self.maybe_write_pending_index(storage, crypto);
        Ok(())
    }

    /// Wait for all background pack uploads to finish.
    pub(crate) fn wait_pending_uploads(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<()> {
        let mut first_err: Option<VykarError> = None;
        for handle in self.pending_uploads.drain(..) {
            let res = handle
                .join()
                .map_err(|_| VykarError::Other("pack upload thread panicked".into()))
                .and_then(|r| r);
            if first_err.is_none() {
                if let Err(e) = res {
                    first_err = Some(e);
                }
            }
        }
        // Final flush of journal before returning (best-effort).
        if !self.pending_journal.is_empty() {
            self.write_pending_index_best_effort(storage, crypto);
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Apply backpressure to keep the number of in-flight uploads bounded.
    /// Session-marker refreshes run in a dedicated heartbeat thread owned by
    /// `SessionGuard`, so this path no longer touches the marker.
    pub(crate) fn cap_pending_uploads(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<()> {
        self.drain_finished_uploads(storage, crypto)?;
        if self.pending_uploads.len()
            >= self
                .max_in_flight_uploads
                .saturating_add(UPLOAD_QUEUE_HEADROOM)
        {
            // All slots + buffer full — block on one handle.
            let handle = self.pending_uploads.swap_remove(0);
            handle
                .join()
                .map_err(|_| VykarError::Other("pack upload thread panicked".into()))??;
            self.maybe_write_pending_index(storage, crypto);
        }
        Ok(())
    }

    // --- Journal writes ---

    /// Write the pending index journal to storage (debounced helper).
    pub(crate) fn maybe_write_pending_index(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) {
        let current = self.pending_journal.len();
        if current >= self.pending_journal_last_written + JOURNAL_WRITE_INTERVAL {
            self.write_pending_index_best_effort(storage, crypto);
        }
    }

    /// Serialize and write the pending index journal to storage.
    /// Best-effort: logs a warning on failure, never propagates errors.
    pub(crate) fn write_pending_index_best_effort(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) {
        if self.pending_journal.is_empty() {
            return;
        }
        match self.write_pending_index(storage, crypto) {
            Ok(()) => {
                self.pending_journal_last_written = self.pending_journal.len();
            }
            Err(e) => {
                warn!("failed to write pending_index: {e}");
            }
        }
    }

    /// Serialize, compress, encrypt, and write the pending index journal to storage.
    fn write_pending_index(
        &self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<()> {
        let wire = self.pending_journal.to_wire();
        let serialized = rmp_serde::to_vec(&wire)?;
        let compressed = compress::compress(compress::Compression::Zstd { level: 3 }, &serialized)?;
        let packed = pack_object_with_context(
            ObjectType::PendingIndex,
            PENDING_INDEX_OBJECT_CONTEXT,
            &compressed,
            crypto,
        )?;
        let key = self.pending_index_key();
        storage.put(&key, &packed)?;
        debug!(
            packs = wire.len(),
            bytes = packed.len(),
            key = %key,
            "wrote pending_index to storage"
        );
        Ok(())
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

    // --- Recovery ---

    /// Recover chunk→pack mappings from previous interrupted sessions'
    /// `pending_index` files. Scans all `.index` files under `sessions/`,
    /// classifying each by its companion `.json` marker state.
    ///
    /// A journal is recoverable if it is **orphaned** (no companion `.json`)
    /// or **stale** (companion `.json` has `last_refresh` older than 45 min).
    /// Active sessions and sessions with unreadable markers are skipped.
    ///
    /// Must be called inside the repo lock, before `enable_tiered_dedup_mode()`.
    /// Returns the total number of recovered chunk entries.
    pub(crate) fn recover_pending_index(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        chunk_index: &ChunkIndex,
    ) -> Result<PendingIndexRecovery> {
        let all_keys = storage.list(SESSIONS_PREFIX)?;
        let now = chrono::Utc::now();

        // Build marker state map from .json keys.
        let mut marker_states: StdHashMap<String, MarkerState> = StdHashMap::new();
        for key in &all_keys {
            let Some(session_id) = key
                .strip_prefix(SESSIONS_PREFIX)
                .and_then(|s| s.strip_suffix(".json"))
            else {
                continue;
            };
            let state = match storage.get(key) {
                Ok(Some(data)) => match serde_json::from_slice::<SessionEntry>(&data) {
                    Ok(entry) => {
                        let ts = chrono::DateTime::parse_from_rfc3339(&entry.last_refresh).or_else(
                            |_| chrono::DateTime::parse_from_rfc3339(&entry.registered_at),
                        );
                        match ts {
                            Ok(ts) => {
                                let age = now.signed_duration_since(ts.with_timezone(&chrono::Utc));
                                if age.num_seconds() > SESSION_STALE_SECS {
                                    MarkerState::Stale
                                } else {
                                    MarkerState::Active
                                }
                            }
                            Err(_) => MarkerState::Unknown,
                        }
                    }
                    Err(_) => MarkerState::Unknown,
                },
                _ => MarkerState::Unknown,
            };
            marker_states.insert(session_id.to_string(), state);
        }

        // Collect .index keys, skip our own session.
        let my_key = self.pending_index_key();
        let index_keys: Vec<(String, String)> = all_keys
            .iter()
            .filter_map(|key| {
                let session_id = key
                    .strip_prefix(SESSIONS_PREFIX)
                    .and_then(|s| s.strip_suffix(".index"))?;
                Some((key.clone(), session_id.to_string()))
            })
            .filter(|(key, _)| *key != my_key)
            .collect();

        let mut total_recovered = 0usize;
        let mut recovered_sessions = Vec::new();
        for (index_key, session_id) in &index_keys {
            match marker_states.get(session_id) {
                None => {}                     // orphan — no .json, recoverable
                Some(MarkerState::Stale) => {} // stale — recoverable
                Some(MarkerState::Active) => continue,
                Some(MarkerState::Unknown) => continue,
            };

            match self.recover_single_index(storage, crypto, chunk_index, index_key) {
                Ok(RecoverResult::Ok(n)) => {
                    self.recovered_index_keys.push(index_key.clone());
                    recovered_sessions.push(session_id.clone());
                    total_recovered += n;
                }
                Ok(RecoverResult::Corrupt) => {
                    // Only delete the corrupt .index — never touch .json markers.
                    // Session marker lifecycle is managed by deregister_session()
                    // and cleanup_stale_sessions() (>45 min since last_refresh,
                    // under maintenance lock).
                    warn!(key = %index_key, "corrupt pending index journal, deleting");
                    let _ = storage.delete(index_key);
                }
                Ok(RecoverResult::NotFound) => {}
                Err(e) => {
                    warn!(key = %index_key, error = %e, "failed to recover pending index, skipping");
                }
            }
        }

        if total_recovered > 0 {
            warn!(
                recovered_chunks = total_recovered,
                journals = self.recovered_index_keys.len(),
                "recovered pending index entries from interrupted sessions"
            );
        }
        Ok(PendingIndexRecovery {
            recovered_chunks: total_recovered,
            recovered_sessions,
        })
    }

    /// Attempt to recover a single session's pending index journal.
    fn recover_single_index(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        chunk_index: &ChunkIndex,
        key: &str,
    ) -> Result<RecoverResult> {
        let data = match storage.get(key)? {
            Some(d) => d,
            None => return Ok(RecoverResult::NotFound),
        };

        let compressed = match unpack_object_expect_with_context(
            &data,
            ObjectType::PendingIndex,
            PENDING_INDEX_OBJECT_CONTEXT,
            crypto,
        ) {
            Ok(c) => c,
            Err(e) => {
                warn!(key = %key, error = %e, "pending_index: decrypt failed");
                return Ok(RecoverResult::Corrupt);
            }
        };

        let serialized = match compress::decompress_metadata(&compressed) {
            Ok(s) => s,
            Err(e) => {
                warn!(key = %key, error = %e, "pending_index: decompress failed");
                return Ok(RecoverResult::Corrupt);
            }
        };

        let wire: Vec<crate::index::PendingPackEntry> = match rmp_serde::from_slice(&serialized) {
            Ok(w) => w,
            Err(e) => {
                warn!(key = %key, error = %e, "pending_index: deserialize failed");
                return Ok(RecoverResult::Corrupt);
            }
        };

        debug!(
            key = %key,
            packs = wire.len(),
            "found pending index from interrupted session, verifying packs\u{2026}"
        );

        // Batch-verify pack existence by listing shard directories instead of
        // issuing one HEAD request per pack (significant speedup for REST/S3).
        let shards: std::collections::HashSet<String> = wire
            .iter()
            .map(|e| format!("packs/{}", e.pack_id.shard_prefix()))
            .collect();
        let mut known_packs: std::collections::HashSet<String> = std::collections::HashSet::new();
        for shard in &shards {
            match storage.list(shard) {
                Ok(keys) => known_packs.extend(keys),
                Err(e) => {
                    warn!("pending_index: failed to list {shard}: {e}, falling back to per-pack checks");
                    for entry in &wire {
                        if format!("packs/{}", entry.pack_id.shard_prefix()) == *shard
                            && storage
                                .exists(&entry.pack_id.storage_key())
                                .unwrap_or(false)
                        {
                            known_packs.insert(entry.pack_id.storage_key());
                        }
                    }
                }
            }
        }

        let mut recovered = 0usize;
        for pack_entry in &wire {
            let pack_key = pack_entry.pack_id.storage_key();
            if !known_packs.contains(&pack_key) {
                warn!(
                    pack_id = %pack_entry.pack_id,
                    "pending_index: pack missing from storage, skipping"
                );
                continue;
            }

            for chunk in &pack_entry.chunks {
                // Only add if not already in the chunk index (e.g. from a
                // successful prior save that didn't delete pending_index).
                if !chunk_index.contains(&chunk.chunk_id) {
                    self.recovered_chunks.insert(
                        chunk.chunk_id,
                        RecoveredChunkEntry {
                            stored_size: chunk.stored_size,
                            pack_id: pack_entry.pack_id,
                            pack_offset: chunk.pack_offset,
                        },
                    );
                    recovered += 1;
                }
            }

            // Seed journal so re-interruption preserves these entries.
            self.pending_journal
                .record_pack(pack_entry.pack_id, pack_entry.chunks.clone());
        }

        debug!(
            key = %key,
            packs = wire.len(),
            recovered_chunks = recovered,
            "recovered pending_index entries"
        );
        Ok(RecoverResult::Ok(recovered))
    }

    /// Best-effort delete of the session's pending index file from storage.
    pub(crate) fn clear_pending_index(storage: &dyn StorageBackend, session_id: &str) {
        let key = session_index_key(session_id);
        match storage.delete(&key) {
            Ok(()) => {
                debug!(key = %key, "cleared pending_index from storage");
            }
            Err(e) => {
                warn!(key = %key, "failed to clear pending_index: {e}");
            }
        }
    }

    /// Delete recovered `.index` files from storage after successful commit.
    /// Never deletes `.json` markers — their lifecycle is managed by
    /// `deregister_session()` and `cleanup_stale_sessions()`.
    /// Best-effort: logs on failure.
    pub(crate) fn cleanup_recovered_indices(&mut self, storage: &dyn StorageBackend) {
        for index_key in self.recovered_index_keys.drain(..) {
            if let Err(e) = storage.delete(&index_key) {
                warn!(key = %index_key, error = %e, "failed to delete recovered index journal");
            }
        }
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
        // Record for rollback before removing from recovered_chunks.
        if let Some(ref mut tracker) = self.rollback_tracker {
            if let Some(recovered) = self.recovered_chunks.get(chunk_id) {
                tracker
                    .promoted_recovered
                    .push((*chunk_id, recovered.clone()));
                tracker.dedup_inserts.push(*chunk_id);
            }
        }

        let entry = self.recovered_chunks.remove(chunk_id)?;

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
mod tests {
    use super::*;
    use crate::compress;
    use crate::index::{ChunkIndex, PendingChunkEntry, PendingPackEntry};
    use crate::repo::format::{pack_object_with_context, ObjectType};
    use crate::repo::lock::{session_index_key, session_marker_key, SessionEntry};
    use crate::testutil::{init_test_environment, MemoryBackend};
    use vykar_crypto::PlaintextEngine;

    /// Create a PlaintextEngine for tests.
    fn test_crypto() -> PlaintextEngine {
        PlaintextEngine::new(&[0xAA; 32])
    }

    /// Write a valid pending index journal to storage at the given key.
    /// Returns the PackId and ChunkId used.
    fn write_valid_journal(
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        key: &str,
    ) -> (PackId, ChunkId) {
        let pack_id = PackId([0x11; 32]);
        let chunk_id = ChunkId([0x22; 32]);

        // Create the pack file so verification passes.
        let pack_key = pack_id.storage_key();
        storage.put(&pack_key, b"fake pack data").unwrap();

        let entries = vec![PendingPackEntry {
            pack_id,
            chunks: vec![PendingChunkEntry {
                chunk_id,
                stored_size: 100,
                pack_offset: 0,
            }],
        }];

        let serialized = rmp_serde::to_vec(&entries).unwrap();
        let compressed =
            compress::compress(compress::Compression::Zstd { level: 3 }, &serialized).unwrap();
        let packed = pack_object_with_context(
            ObjectType::PendingIndex,
            PENDING_INDEX_OBJECT_CONTEXT,
            &compressed,
            crypto,
        )
        .unwrap();
        storage.put(key, &packed).unwrap();

        (pack_id, chunk_id)
    }

    /// Write a session marker JSON with a specific last_refresh time.
    fn write_session_marker(storage: &dyn StorageBackend, session_id: &str, last_refresh: &str) {
        let key = session_marker_key(session_id);
        let entry = SessionEntry {
            hostname: "test".to_string(),
            pid: 1234,
            registered_at: last_refresh.to_string(),
            last_refresh: last_refresh.to_string(),
        };
        let data = serde_json::to_vec(&entry).unwrap();
        storage.put(&key, &data).unwrap();
    }

    fn stale_timestamp() -> String {
        let stale = chrono::Utc::now() - chrono::Duration::hours(2);
        stale.to_rfc3339()
    }

    fn fresh_timestamp() -> String {
        chrono::Utc::now().to_rfc3339()
    }

    #[test]
    fn recover_orphan_index_from_different_session() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let chunk_index = ChunkIndex::new();

        // Write a valid journal for session "old" with no .json marker.
        let index_key = session_index_key("old");
        let (_pack_id, _chunk_id) = write_valid_journal(&storage, &crypto, &index_key);

        // Create a WriteSessionState with a different session ID.
        let mut ws = WriteSessionState::new(1024, 1024, 1);
        ws.session_id = "new".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index)
            .unwrap();
        assert!(
            recovery.recovered_chunks > 0,
            "should recover chunks from orphan journal"
        );
        assert_eq!(recovery.recovered_sessions, vec!["old"]);
        assert_eq!(ws.recovered_index_keys.len(), 1);
        assert_eq!(ws.recovered_index_keys[0], index_key);
    }

    #[test]
    fn recover_stale_index() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let chunk_index = ChunkIndex::new();

        // Write a stale .json marker and companion .index.
        write_session_marker(&storage, "stale", &stale_timestamp());
        let index_key = session_index_key("stale");
        write_valid_journal(&storage, &crypto, &index_key);

        let mut ws = WriteSessionState::new(1024, 1024, 1);
        ws.session_id = "current".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index)
            .unwrap();
        assert!(
            recovery.recovered_chunks > 0,
            "should recover chunks from stale session"
        );
        assert_eq!(recovery.recovered_sessions, vec!["stale"]);
        assert_eq!(ws.recovered_index_keys.len(), 1);
        assert_eq!(ws.recovered_index_keys[0], index_key);
        // .json marker must NOT be tracked for deletion — it's managed by
        // deregister_session() / cleanup_stale_sessions() only.
        assert!(
            storage.exists(&session_marker_key("stale")).unwrap(),
            "stale .json marker must not be deleted by recovery"
        );
    }

    #[test]
    fn skip_active_session_index() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let chunk_index = ChunkIndex::new();

        // Write an active (fresh) .json marker and companion .index.
        write_session_marker(&storage, "active", &fresh_timestamp());
        let index_key = session_index_key("active");
        write_valid_journal(&storage, &crypto, &index_key);

        let mut ws = WriteSessionState::new(1024, 1024, 1);
        ws.session_id = "current".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index)
            .unwrap();
        assert_eq!(
            recovery.recovered_chunks, 0,
            "should skip active session's journal"
        );
        assert!(recovery.recovered_sessions.is_empty());
        assert!(ws.recovered_index_keys.is_empty());
    }

    #[test]
    fn skip_unknown_marker_session_index() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let chunk_index = ChunkIndex::new();

        // Write invalid JSON as the session marker.
        storage
            .put(&session_marker_key("bad"), b"not valid json")
            .unwrap();
        let index_key = session_index_key("bad");
        write_valid_journal(&storage, &crypto, &index_key);

        let mut ws = WriteSessionState::new(1024, 1024, 1);
        ws.session_id = "current".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index)
            .unwrap();
        assert_eq!(
            recovery.recovered_chunks, 0,
            "should skip session with unknown marker state"
        );
        assert!(recovery.recovered_sessions.is_empty());
        assert!(ws.recovered_index_keys.is_empty());
    }

    #[test]
    fn mixed_valid_and_corrupt_journals() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let chunk_index = ChunkIndex::new();

        // Valid orphan journal.
        let valid_key = session_index_key("valid");
        write_valid_journal(&storage, &crypto, &valid_key);

        // Corrupt orphan journal (random bytes).
        let corrupt_key = session_index_key("corrupt");
        storage.put(&corrupt_key, b"random garbage").unwrap();

        let mut ws = WriteSessionState::new(1024, 1024, 1);
        ws.session_id = "current".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index)
            .unwrap();
        assert!(
            recovery.recovered_chunks > 0,
            "should recover from the valid journal"
        );
        assert_eq!(recovery.recovered_sessions, vec!["valid"]);

        // Valid journal tracked for cleanup.
        assert_eq!(ws.recovered_index_keys.len(), 1);
        assert_eq!(ws.recovered_index_keys[0], valid_key);

        // Corrupt journal should have been deleted.
        assert!(
            !storage.exists(&corrupt_key).unwrap(),
            "corrupt .index should be deleted"
        );
    }

    #[test]
    fn corrupt_stale_journal_deletes_index_preserves_json() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let chunk_index = ChunkIndex::new();

        // Write a stale .json marker.
        write_session_marker(&storage, "old", &stale_timestamp());
        // Write a corrupt .index companion.
        let index_key = session_index_key("old");
        storage.put(&index_key, b"corrupt data").unwrap();

        let mut ws = WriteSessionState::new(1024, 1024, 1);
        ws.session_id = "current".to_string();

        let _recovered = ws
            .recover_pending_index(&storage, &crypto, &chunk_index)
            .unwrap();

        // Corrupt .index should be deleted.
        assert!(
            !storage.exists(&index_key).unwrap(),
            "corrupt .index should be deleted"
        );
        // .json marker must NOT be deleted — a live backup could still own it.
        assert!(
            storage.exists(&session_marker_key("old")).unwrap(),
            "stale .json must not be deleted by recovery"
        );
    }

    #[test]
    fn zero_recovery_journals_tracked_for_cleanup() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();

        // Write a valid orphan journal.
        let index_key = session_index_key("old");
        let (_pack_id, chunk_id) = write_valid_journal(&storage, &crypto, &index_key);

        // Pre-populate the chunk index so recovery yields 0 new chunks.
        let mut chunk_index = ChunkIndex::new();
        chunk_index.add(chunk_id, 100, PackId([0x11; 32]), 0);

        let mut ws = WriteSessionState::new(1024, 1024, 1);
        ws.session_id = "current".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index)
            .unwrap();
        assert_eq!(recovery.recovered_chunks, 0, "all chunks already indexed");
        assert_eq!(recovery.recovered_sessions, vec!["old"]);
        // The journal should still be tracked for cleanup.
        assert_eq!(
            ws.recovered_index_keys.len(),
            1,
            "Ok(0) journal should be tracked for cleanup"
        );
    }

    #[test]
    fn cleanup_deletes_index_files_only() {
        init_test_environment();
        let storage = MemoryBackend::new();

        // Simulate an orphan entry (index only, no json).
        let orphan_index = session_index_key("orphan");
        storage.put(&orphan_index, b"data").unwrap();

        // Simulate a stale entry (both index and json exist in storage).
        let stale_index = session_index_key("stale");
        let stale_json = session_marker_key("stale");
        storage.put(&stale_index, b"data").unwrap();
        storage.put(&stale_json, b"data").unwrap();

        let mut ws = WriteSessionState::new(1024, 1024, 1);
        ws.recovered_index_keys = vec![orphan_index.clone(), stale_index.clone()];

        ws.cleanup_recovered_indices(&storage);

        assert!(
            !storage.exists(&orphan_index).unwrap(),
            "orphan .index should be deleted"
        );
        assert!(
            !storage.exists(&stale_index).unwrap(),
            "stale .index should be deleted"
        );
        // .json marker must NOT be touched by cleanup.
        assert!(
            storage.exists(&stale_json).unwrap(),
            "stale .json must not be deleted by cleanup"
        );
        assert!(ws.recovered_index_keys.is_empty(), "keys should be drained");
    }
}
