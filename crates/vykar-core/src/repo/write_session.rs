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
use super::lock::session_index_key;
use super::pack::{PackType, PackWriter, PackedChunkEntry};

/// Extra upload handles allowed beyond `max_in_flight_uploads` before blocking.
const UPLOAD_QUEUE_HEADROOM: usize = 2;

/// Number of new packs between debounced `pending_index` writes.
const JOURNAL_WRITE_INTERVAL: usize = 8;

const DEFAULT_SESSION_ID: &str = "default";
const PENDING_INDEX_OBJECT_CONTEXT: &[u8] = b"pending_index";

/// Tracks index mutations during a streaming command dump so they can be
/// rolled back if the dump command fails mid-stream.
pub(crate) struct DumpRollbackTracker {
    /// Truncation point for IndexDelta.
    pub delta_checkpoint: IndexDeltaCheckpoint,
    /// ChunkIds inserted into tiered_dedup or dedup_index during this dump.
    pub dedup_inserts: Vec<ChunkId>,
    /// Entries promoted out of recovered_chunks — saved to re-insert on rollback.
    pub promoted_recovered: Vec<(ChunkId, RecoveredChunkEntry)>,
    /// PackIds added to PendingIndexJournal during this dump.
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
    /// Number of distinct packs (data + tree) in the persisted index at load time.
    pub(crate) persisted_pack_count: usize,
    /// Number of data packs flushed during the current session.
    pub(crate) session_packs_flushed: usize,
    /// Session ID for per-session pending_index files.
    /// Defaults to `"default"` for non-backup callers/tests.
    pub(crate) session_id: String,
    /// Wall-clock time of last session marker refresh (for throttling).
    pub(crate) last_session_refresh: std::time::Instant,
    /// Active dump rollback tracker (set during streaming command dumps).
    pub(crate) dump_tracker: Option<DumpRollbackTracker>,
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
            persisted_pack_count: 0,
            session_packs_flushed: 0,
            session_id: DEFAULT_SESSION_ID.to_string(),
            last_session_refresh: std::time::Instant::now(),
            dump_tracker: None,
        }
    }

    /// Refresh the session marker if enough time has passed (~15 min).
    /// No-op when session_id is the default (non-backup callers don't register sessions).
    pub(crate) fn maybe_refresh_session(&mut self, storage: &dyn StorageBackend) {
        const REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15 * 60);
        if self.session_id != DEFAULT_SESSION_ID
            && self.last_session_refresh.elapsed() >= REFRESH_INTERVAL
        {
            crate::repo::lock::refresh_session(storage, &self.session_id);
            self.last_session_refresh = std::time::Instant::now();
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
    /// Also refreshes the session marker if enough time has passed.
    pub(crate) fn cap_pending_uploads(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<()> {
        self.drain_finished_uploads(storage, crypto)?;
        // Refresh session marker (throttled, ~15 min). Placed here so both
        // sequential and pipeline modes benefit (both call cap_pending_uploads).
        self.maybe_refresh_session(storage);
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
                if let Some(ref mut tracker) = self.dump_tracker {
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
                if let Some(ref mut tracker) = self.dump_tracker {
                    tracker.dedup_inserts.push(chunk_id);
                }
            }
            false
        } else {
            for (chunk_id, stored_size, offset, refcount) in entries {
                chunk_index.add(chunk_id, stored_size, pack_id, offset);
                for _ in 1..refcount {
                    chunk_index.increment_refcount(&chunk_id);
                }
                if let Some(ref mut tracker) = self.dump_tracker {
                    tracker.dedup_inserts.push(chunk_id);
                }
            }
            true
        }
    }

    // --- Recovery ---

    /// Recover chunk→pack mappings from a previous interrupted session's
    /// `pending_index` file. Verifies each pack exists before adding entries.
    ///
    /// Must be called inside the repo lock, before `enable_tiered_dedup_mode()`.
    /// Returns the number of recovered chunk entries.
    pub(crate) fn recover_pending_index(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        chunk_index: &ChunkIndex,
    ) -> Result<usize> {
        let key = self.pending_index_key();
        let data = match storage.get(&key)? {
            Some(d) => d,
            None => return Ok(0),
        };

        let compressed = match unpack_object_expect_with_context(
            &data,
            ObjectType::PendingIndex,
            PENDING_INDEX_OBJECT_CONTEXT,
            crypto,
        ) {
            Ok(c) => c,
            Err(e) => {
                warn!("pending_index: decrypt failed, skipping recovery: {e}");
                return Ok(0);
            }
        };

        let serialized = match compress::decompress_metadata(&compressed) {
            Ok(s) => s,
            Err(e) => {
                warn!("pending_index: decompress failed, skipping recovery: {e}");
                return Ok(0);
            }
        };

        let wire: Vec<crate::index::PendingPackEntry> = match rmp_serde::from_slice(&serialized) {
            Ok(w) => w,
            Err(e) => {
                warn!("pending_index: deserialize failed, skipping recovery: {e}");
                return Ok(0);
            }
        };

        warn!(
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
            packs = wire.len(),
            recovered_chunks = recovered,
            "recovered pending_index entries"
        );
        Ok(recovered)
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
        // Record for dump rollback before removing from recovered_chunks.
        if let Some(ref mut tracker) = self.dump_tracker {
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
