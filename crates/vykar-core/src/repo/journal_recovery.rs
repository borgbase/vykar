use std::collections::HashMap as StdHashMap;

use tracing::{debug, warn};

use crate::compress;
use crate::index::{
    ChunkIndex, DecodedJournal, PendingIndexJournal, RecoveredChunkEntry, decode_pending_journal,
};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

use super::format::{ObjectType, unpack_object_expect_with_context};
use super::lock::{SESSION_STALE_SECS, SESSIONS_PREFIX, SessionEntry, session_index_key};
use super::pack_recovery::verify_journal_packs;
use super::session_journal::PENDING_INDEX_OBJECT_CONTEXT;

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
    /// Versioned journal from a newer binary. Left in storage untouched so
    /// that binary can still recover it.
    UnknownVersion,
    /// Unversioned journal from an older binary, which listed packs before
    /// their upload was confirmed. Not trusted; deleted.
    Legacy,
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

/// The parts of the write session that recovery fills in.
pub(crate) struct RecoveryTarget<'a> {
    pub(crate) recovered_chunks: &'a mut StdHashMap<ChunkId, RecoveredChunkEntry>,
    pub(crate) recovered_index_keys: &'a mut Vec<String>,
    /// Seeded with every accepted pack so a re-interruption preserves it.
    pub(crate) journal: &'a mut PendingIndexJournal,
}

/// Recover chunk→pack mappings from previous interrupted sessions'
/// `pending_index` files. Scans all `.index` files under `sessions/`,
/// classifying each by its companion `.json` marker state.
///
/// A journal is recoverable if it is **orphaned** (no companion `.json`)
/// or **stale** (companion `.json` has `last_refresh` older than 45 min).
/// Active sessions and sessions with unreadable markers are skipped.
///
/// Must be called inside the repo lock, before `enable_tiered_dedup_mode()`.
/// Returns the total number of recovered chunk entries. `persisted_packs` and
/// `width` steer the pack lookups (see `pack_lookup`).
pub(crate) fn recover_pending_index(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    chunk_index: &ChunkIndex,
    own_session_id: &str,
    persisted_packs: usize,
    width: usize,
    target: &mut RecoveryTarget<'_>,
) -> Result<PendingIndexRecovery> {
    let all_keys = storage.list(SESSIONS_PREFIX)?;
    let now = chrono::Utc::now();

    // Collect .index keys, skip our own session.
    let my_key = session_index_key(own_session_id);
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

    // Build the marker state map from .json keys. Only markers of the
    // sessions collected above are ever consulted, so only those are
    // fetched — a marker without a foreign journal (our own included)
    // cannot change the outcome.
    let journal_sessions: std::collections::HashSet<&str> =
        index_keys.iter().map(|(_, id)| id.as_str()).collect();
    let mut marker_states: StdHashMap<String, MarkerState> = StdHashMap::new();
    for key in &all_keys {
        let Some(session_id) = key
            .strip_prefix(SESSIONS_PREFIX)
            .and_then(|s| s.strip_suffix(".json"))
        else {
            continue;
        };
        if !journal_sessions.contains(session_id) {
            continue;
        }
        let state = match storage.get(key) {
            Ok(Some(data)) => match serde_json::from_slice::<SessionEntry>(&data) {
                Ok(entry) => {
                    let ts = chrono::DateTime::parse_from_rfc3339(&entry.last_refresh)
                        .or_else(|_| chrono::DateTime::parse_from_rfc3339(&entry.registered_at));
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

    let mut total_recovered = 0usize;
    let mut recovered_sessions = Vec::new();
    for (index_key, session_id) in &index_keys {
        match marker_states.get(session_id) {
            None => {}                     // orphan — no .json, recoverable
            Some(MarkerState::Stale) => {} // stale — recoverable
            Some(MarkerState::Active) => continue,
            Some(MarkerState::Unknown) => continue,
        };

        match recover_single_index(
            storage,
            crypto,
            chunk_index,
            index_key,
            persisted_packs,
            width,
            target,
        ) {
            Ok(RecoverResult::Ok(n)) => {
                target.recovered_index_keys.push(index_key.clone());
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
            Ok(RecoverResult::Legacy) => {
                warn!(
                    key = %index_key,
                    "discarding pending index journal from an older vykar version; \
                     its chunks will be uploaded again"
                );
                let _ = storage.delete(index_key);
            }
            Ok(RecoverResult::NotFound | RecoverResult::UnknownVersion) => {}
            Err(e) => {
                warn!(key = %index_key, error = %e, "failed to recover pending index, skipping");
            }
        }
    }

    if total_recovered > 0 {
        warn!(
            recovered_chunks = total_recovered,
            journals = target.recovered_index_keys.len(),
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
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    chunk_index: &ChunkIndex,
    key: &str,
    persisted_packs: usize,
    width: usize,
    target: &mut RecoveryTarget<'_>,
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

    let wire = match decode_pending_journal(&serialized) {
        Ok(DecodedJournal::V2(w)) => w,
        Ok(DecodedJournal::Legacy) => return Ok(RecoverResult::Legacy),
        Ok(DecodedJournal::Unknown(v)) => {
            warn!(
                "pending_index {key}: journal version {v} not understood by this binary; \
                 leaving it in place"
            );
            return Ok(RecoverResult::UnknownVersion);
        }
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

    let accepted = verify_journal_packs(storage, &wire, persisted_packs, width);

    let mut recovered = 0usize;
    let pack_count = wire.len();
    for pack_entry in wire {
        // Rejected packs were already logged by `verify_journal_packs`
        // and are not re-seeded into this session's journal.
        if !accepted.contains(&pack_entry.pack_id) {
            continue;
        }

        for chunk in &pack_entry.chunks {
            // Only add if not already in the chunk index (e.g. from a
            // successful prior save that didn't delete pending_index).
            if !chunk_index.contains(&chunk.chunk_id) {
                target.recovered_chunks.insert(
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
        target
            .journal
            .record_pack(pack_entry.pack_id, pack_entry.chunks);
    }

    debug!(
        key = %key,
        packs = pack_count,
        recovered_chunks = recovered,
        "recovered pending_index entries"
    );
    Ok(RecoverResult::Ok(recovered))
}

/// Delete recovered `.index` files from storage after successful commit.
/// Never deletes `.json` markers — their lifecycle is managed by
/// `deregister_session()` and `cleanup_stale_sessions()`.
/// Best-effort: logs on failure.
pub(crate) fn delete_recovered_indices(storage: &dyn StorageBackend, keys: &mut Vec<String>) {
    for index_key in keys.drain(..) {
        if let Err(e) = storage.delete(&index_key) {
            warn!(key = %index_key, error = %e, "failed to delete recovered index journal");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{PendingPackEntry, encode_pending_journal};
    use crate::repo::format::pack_object_with_context;
    use crate::repo::lock::session_marker_key;
    use crate::repo::write_session::WriteSessionState;
    use crate::repo::write_session::tests::{fake_pack, test_crypto};
    use crate::testutil::{MemoryBackend, init_test_environment};
    use vykar_types::hash::HashAlgorithm;
    use vykar_types::pack_id::PackId;

    #[derive(Clone, Copy)]
    enum Wire {
        V2,
        Legacy,
    }

    fn put_journal(
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        key: &str,
        entries: &[PendingPackEntry],
        wire: Wire,
    ) {
        let serialized = match wire {
            Wire::V2 => encode_pending_journal(entries).unwrap(),
            Wire::Legacy => rmp_serde::to_vec(entries).unwrap(),
        };
        put_journal_bytes(storage, crypto, key, &serialized);
    }

    fn put_journal_bytes(
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        key: &str,
        serialized: &[u8],
    ) {
        let compressed =
            compress::compress(compress::Compression::Zstd { level: 3 }, serialized).unwrap();
        let packed = pack_object_with_context(
            ObjectType::PendingIndex,
            PENDING_INDEX_OBJECT_CONTEXT,
            &compressed,
            crypto,
        )
        .unwrap();
        storage.put(key, &packed).unwrap();
    }

    /// Write a journal naming one intact pack (stored as well).
    fn write_journal(
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        key: &str,
        wire: Wire,
    ) -> (PackId, ChunkId) {
        let (pack_id, data, entry) = fake_pack(0x11);
        storage.put(&pack_id.storage_key(), &data).unwrap();
        let chunk_id = entry.chunks[0].chunk_id;
        put_journal(storage, crypto, key, &[entry], wire);
        (pack_id, chunk_id)
    }

    /// Write a valid (v2) pending index journal to storage at the given key.
    /// Returns the PackId and ChunkId used.
    fn write_valid_journal(
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        key: &str,
    ) -> (PackId, ChunkId) {
        write_journal(storage, crypto, key, Wire::V2)
    }

    fn recover_from(storage: &dyn StorageBackend) -> (WriteSessionState, PendingIndexRecovery) {
        let crypto = test_crypto();
        let mut ws = WriteSessionState::new(1024, 1024, 1, HashAlgorithm::Blake2b);
        ws.session_id = "current".to_string();
        let recovery = ws
            .recover_pending_index(storage, &crypto, &ChunkIndex::new(), 1)
            .unwrap();
        (ws, recovery)
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
        let mut ws = WriteSessionState::new(1024, 1024, 1, HashAlgorithm::Blake2b);
        ws.session_id = "new".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index, 1)
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

        let mut ws = WriteSessionState::new(1024, 1024, 1, HashAlgorithm::Blake2b);
        ws.session_id = "current".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index, 1)
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

        let mut ws = WriteSessionState::new(1024, 1024, 1, HashAlgorithm::Blake2b);
        ws.session_id = "current".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index, 1)
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

        let mut ws = WriteSessionState::new(1024, 1024, 1, HashAlgorithm::Blake2b);
        ws.session_id = "current".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index, 1)
            .unwrap();
        assert_eq!(
            recovery.recovered_chunks, 0,
            "should skip session with unknown marker state"
        );
        assert!(recovery.recovered_sessions.is_empty());
        assert!(ws.recovered_index_keys.is_empty());
    }

    /// Records every `get` key, to show which markers recovery fetches.
    struct GetLoggingBackend {
        inner: MemoryBackend,
        gets: std::sync::Mutex<Vec<String>>,
    }

    impl vykar_storage::InnerBackend for GetLoggingBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for GetLoggingBackend;
        except [get];

        fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.gets.lock().unwrap().push(key.to_string());
            self.inner.get(key)
        }
    }

    /// Recovery reads only the markers of foreign sessions that have a
    /// journal; a marker without one (our own included) is never fetched,
    /// and the stale / active classification of the others is unchanged.
    #[test]
    fn recovery_fetches_only_markers_with_foreign_journals() {
        init_test_environment();
        let storage = GetLoggingBackend {
            inner: MemoryBackend::new(),
            gets: Default::default(),
        };
        let crypto = test_crypto();
        write_session_marker(&storage, "current", &fresh_timestamp());
        write_session_marker(&storage, "other", &fresh_timestamp());
        write_session_marker(&storage, "active", &fresh_timestamp());
        write_valid_journal(&storage, &crypto, &session_index_key("active"));
        write_session_marker(&storage, "stale", &stale_timestamp());
        write_valid_journal(&storage, &crypto, &session_index_key("stale"));

        let (ws, recovery) = recover_from(&storage);
        assert_eq!(recovery.recovered_sessions, vec!["stale"]);
        assert_eq!(ws.recovered_index_keys, vec![session_index_key("stale")]);

        let gets = storage.gets.lock().unwrap().clone();
        let mut markers: Vec<&String> = gets.iter().filter(|k| k.ends_with(".json")).collect();
        markers.sort();
        assert_eq!(
            markers,
            [&session_marker_key("active"), &session_marker_key("stale")],
            "all GETs: {gets:?}"
        );
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

        let mut ws = WriteSessionState::new(1024, 1024, 1, HashAlgorithm::Blake2b);
        ws.session_id = "current".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index, 1)
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

        let mut ws = WriteSessionState::new(1024, 1024, 1, HashAlgorithm::Blake2b);
        ws.session_id = "current".to_string();

        let _recovered = ws
            .recover_pending_index(&storage, &crypto, &chunk_index, 1)
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
        chunk_index.add(chunk_id, 100, PackId::from_bytes([0x11; 32]), 0);

        let mut ws = WriteSessionState::new(1024, 1024, 1, HashAlgorithm::Blake2b);
        ws.session_id = "current".to_string();

        let recovery = ws
            .recover_pending_index(&storage, &crypto, &chunk_index, 1)
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

        let mut ws = WriteSessionState::new(1024, 1024, 1, HashAlgorithm::Blake2b);
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

    // ── Recovery verification ────────────────────────────────────────────

    #[test]
    fn v2_journal_with_exact_length_is_recovered() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let (pack_id, chunk_id) =
            write_journal(&storage, &crypto, &session_index_key("old"), Wire::V2);

        let (ws, recovery) = recover_from(&storage);
        assert_eq!(recovery.recovered_chunks, 1);
        assert_eq!(ws.recovered_chunks[&chunk_id].pack_id, pack_id);
        assert_eq!(ws.journal.entries.len(), 1, "recovered pack is re-seeded");
    }

    #[test]
    fn v2_journal_with_short_pack_is_skipped_and_not_reseeded() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let (pack_id, data, entry) = fake_pack(0x11);
        storage
            .put(&pack_id.storage_key(), &data[..data.len() - 1])
            .unwrap();
        put_journal(
            &storage,
            &crypto,
            &session_index_key("old"),
            &[entry],
            Wire::V2,
        );

        let (ws, recovery) = recover_from(&storage);
        assert_eq!(recovery.recovered_chunks, 0);
        assert!(ws.recovered_chunks.is_empty());
        assert!(
            ws.journal.entries.is_empty(),
            "skipped pack must not be re-seeded"
        );
        // The journal itself was processed and will be cleaned up.
        assert_eq!(recovery.recovered_sessions, vec!["old"]);
    }

    #[test]
    fn v2_journal_trusts_length_without_reading_content() {
        // Same-length but different bytes: v2 skips the content check.
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let (pack_id, mut data, entry) = fake_pack(0x11);
        data[20] ^= 0xFF;
        storage.put(&pack_id.storage_key(), &data).unwrap();
        put_journal(
            &storage,
            &crypto,
            &session_index_key("old"),
            &[entry],
            Wire::V2,
        );

        let (_ws, recovery) = recover_from(&storage);
        assert_eq!(recovery.recovered_chunks, 1);
    }

    #[test]
    fn legacy_journal_is_discarded_even_with_intact_pack() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let key = session_index_key("old");
        write_journal(&storage, &crypto, &key, Wire::Legacy);

        let (ws, recovery) = recover_from(&storage);
        assert_eq!(recovery.recovered_chunks, 0);
        assert!(recovery.recovered_sessions.is_empty());
        assert!(ws.recovered_chunks.is_empty());
        assert!(ws.journal.entries.is_empty());
        assert!(!storage.exists(&key).unwrap(), "legacy journal is deleted");
    }

    #[test]
    fn overflowing_pack_offset_is_skipped_without_panic() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let (pack_id, data, mut entry) = fake_pack(0x11);
        storage.put(&pack_id.storage_key(), &data).unwrap();
        entry.chunks[0].pack_offset = u64::MAX - 1;
        put_journal(
            &storage,
            &crypto,
            &session_index_key("old"),
            &[entry],
            Wire::V2,
        );

        let (_ws, recovery) = recover_from(&storage);
        assert_eq!(recovery.recovered_chunks, 0);
    }

    /// Counts `size()` calls and listings under `packs/`. `MemoryBackend`
    /// uses the default `list_sized` (no sizes).
    struct LookupCountingBackend {
        inner: MemoryBackend,
        size_calls: std::sync::atomic::AtomicUsize,
        pack_lists: std::sync::atomic::AtomicUsize,
    }

    impl LookupCountingBackend {
        fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                size_calls: Default::default(),
                pack_lists: Default::default(),
            }
        }

        fn count_pack_list(&self, prefix: &str) {
            if prefix.starts_with("packs/") {
                self.pack_lists
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        fn counts(&self) -> (usize, usize) {
            let load =
                |c: &std::sync::atomic::AtomicUsize| c.load(std::sync::atomic::Ordering::SeqCst);
            (load(&self.pack_lists), load(&self.size_calls))
        }
    }

    impl vykar_storage::InnerBackend for LookupCountingBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for LookupCountingBackend;
        except [list, size, list_sized, list_meta, list_tree_sized];

        fn list_meta(
            &self,
            prefix: &str,
        ) -> vykar_types::error::Result<Vec<vykar_storage::ListedObject>> {
            self.list_sized(prefix)
        }

        fn size(&self, key: &str) -> Result<Option<u64>> {
            self.size_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.size(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.count_pack_list(prefix);
            self.inner.list(prefix)
        }

        fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
            self.count_pack_list(prefix);
            self.inner.list_sized(prefix)
        }

        fn list_tree_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
            self.count_pack_list(prefix);
            self.inner.list_tree_sized(prefix)
        }
    }

    /// A single pack is sized with one `size()`: the cost model picks
    /// per-pack lookups over a shard listing (one round either way).
    #[test]
    fn single_pack_is_sized_with_one_size_call() {
        init_test_environment();
        let storage = LookupCountingBackend::new();
        let crypto = test_crypto();
        write_journal(&storage, &crypto, &session_index_key("old"), Wire::V2);

        let (_ws, recovery) = recover_from(&storage);
        assert_eq!(recovery.recovered_chunks, 1);
        assert_eq!(storage.counts(), (0, 1));
    }

    /// `persisted_pack_count` reaches the pack lookup: three packs in one
    /// shard are listed in an empty repository, but sized one by one when
    /// the repository is large (each shard listing would paginate). The
    /// `3` case fails if the count is swapped with the width (1 here).
    #[test]
    fn persisted_pack_count_steers_journal_pack_lookups() {
        init_test_environment();
        for (persisted, expected) in [(0, (1, 3)), (3, (1, 3)), (1_000_000, (0, 3))] {
            let storage = LookupCountingBackend::new();
            let crypto = test_crypto();
            let entries: Vec<PendingPackEntry> = (0..3u8)
                .map(|i| {
                    let mut b = [0x11u8; 32];
                    b[1] = i;
                    let pack_id = PackId::from_bytes(b);
                    storage.put(&pack_id.storage_key(), &[0u8; 64]).unwrap();
                    PendingPackEntry {
                        pack_id,
                        chunks: vec![crate::index::PendingChunkEntry {
                            chunk_id: ChunkId::from_bytes([i; 32]),
                            stored_size: 64 - 13,
                            pack_offset: 13,
                        }],
                    }
                })
                .collect();
            put_journal(
                &storage,
                &crypto,
                &session_index_key("old"),
                &entries,
                Wire::V2,
            );

            let mut ws = WriteSessionState::new(1024, 1024, 1, HashAlgorithm::Blake2b);
            ws.session_id = "current".to_string();
            ws.persisted_pack_count = persisted;
            let recovery = ws
                .recover_pending_index(&storage, &crypto, &ChunkIndex::new(), 1)
                .unwrap();
            assert_eq!(recovery.recovered_chunks, 3, "{persisted}");
            assert_eq!(storage.counts(), expected, "{persisted}");
        }
    }

    #[test]
    fn unknown_journal_version_is_left_in_place() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let (pack_id, data, entry) = fake_pack(0x11);
        storage.put(&pack_id.storage_key(), &data).unwrap();
        let key = session_index_key("future");
        let serialized = rmp_serde::to_vec(&(3u8, vec![entry])).unwrap();
        put_journal_bytes(&storage, &crypto, &key, &serialized);

        let (mut ws, recovery) = recover_from(&storage);
        assert_eq!(recovery.recovered_chunks, 0);
        assert!(recovery.recovered_sessions.is_empty());
        assert!(!ws.recovered_index_keys.contains(&key));
        assert!(storage.exists(&key).unwrap(), "kept after recovery");

        ws.cleanup_recovered_indices(&storage);
        assert!(storage.exists(&key).unwrap(), "kept after cleanup");
    }
}
