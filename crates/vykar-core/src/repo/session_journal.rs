use std::sync::Arc;
use std::thread::JoinHandle;

use tracing::{debug, warn};

use crate::compress;
use crate::index::{PendingIndexJournal, PendingPackEntry, encode_pending_journal};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::error::Result;

use super::format::{ObjectType, pack_object_with_context};
use super::lock::session_index_key;

/// Number of new packs between debounced `pending_index` writes.
pub(super) const JOURNAL_WRITE_INTERVAL: usize = 8;
pub(super) const PENDING_INDEX_OBJECT_CONTEXT: &[u8] = b"pending_index";

/// A debounced journal PUT running in the background, so the consumer
/// keeps sealing and starting pack uploads during the round trip.
struct JournalWrite {
    /// Journal revision and pack count captured in the payload.
    revision: u64,
    len: usize,
    handle: JoinHandle<Result<()>>,
}

/// This session's `pending_index` journal and the state of its writes to
/// storage. At most one background write runs at a time; every synchronous
/// write joins it first, and a new one starts only after it was joined, so a
/// newer payload is never overwritten by an older one. Dropping the journal
/// joins a running write, so none can land after the journal was deleted.
pub(crate) struct SessionJournal {
    /// Chunk→pack mappings for packs confirmed in the current session.
    pub(crate) entries: PendingIndexJournal,
    /// Number of packs in `entries` when last written (for debouncing).
    last_written: usize,
    /// Revision last written successfully (`None` = never written).
    written_rev: Option<u64>,
    write: Option<JournalWrite>,
}

impl Drop for SessionJournal {
    /// Never leave a journal PUT running past the session: it could land
    /// after the session's journal was deleted.
    fn drop(&mut self) {
        if let Some(write) = self.write.take() {
            let _ = write.handle.join();
        }
    }
}

impl SessionJournal {
    pub(super) fn new() -> Self {
        Self {
            entries: PendingIndexJournal::new(),
            last_written: 0,
            written_rev: None,
            write: None,
        }
    }

    /// Whether `entries` holds packs not yet in a successful write.
    pub(super) fn needs_write(&self) -> bool {
        !self.entries.is_empty() && self.written_rev != Some(self.entries.revision())
    }

    /// Start a background journal write once `JOURNAL_WRITE_INTERVAL` new
    /// packs are confirmed (debounced helper). While one is in flight, no
    /// second one starts; the next call after it lands writes the newer
    /// state. A failed write leaves its revision unwritten, so the next call
    /// retries.
    pub(super) fn maybe_write(
        &mut self,
        session_id: &str,
        storage: &Arc<dyn StorageBackend>,
        crypto: &Arc<dyn CryptoEngine>,
    ) {
        if let Some(write) = &self.write {
            if !write.handle.is_finished() {
                return;
            }
            self.join_write();
        }
        if self.entries.len() >= self.last_written + JOURNAL_WRITE_INTERVAL {
            self.start_write(session_id, storage, crypto);
        }
    }

    /// Start writing the current journal state on a background thread.
    /// The caller must have joined any previous write.
    pub(super) fn start_write(
        &mut self,
        session_id: &str,
        storage: &Arc<dyn StorageBackend>,
        crypto: &Arc<dyn CryptoEngine>,
    ) {
        debug_assert!(self.write.is_none());
        let wire = self.entries.to_wire();
        let key = session_index_key(session_id);
        let storage = Arc::clone(storage);
        let crypto = Arc::clone(crypto);
        self.write = Some(JournalWrite {
            revision: self.entries.revision(),
            len: self.entries.len(),
            handle: std::thread::spawn(move || write_journal(&wire, &key, &*storage, &*crypto)),
        });
    }

    /// Wait for the background journal write, if any, and record its
    /// outcome. Best-effort like every journal write: failures are logged.
    pub(super) fn join_write(&mut self) {
        let Some(write) = self.write.take() else {
            return;
        };
        match write.handle.join() {
            Ok(Ok(())) => {
                self.last_written = write.len;
                self.written_rev = Some(write.revision);
            }
            Ok(Err(e)) => warn!("failed to write pending_index: {e}"),
            Err(_) => warn!("pending_index writer thread panicked"),
        }
    }

    /// Serialize and write the pending index journal to storage.
    /// Best-effort: logs a warning on failure, never propagates errors.
    pub(super) fn write_best_effort(
        &mut self,
        session_id: &str,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) {
        self.join_write();
        if self.entries.is_empty() {
            return;
        }
        match self.write_now(session_id, storage, crypto) {
            Ok(()) => {
                self.last_written = self.entries.len();
                self.written_rev = Some(self.entries.revision());
            }
            Err(e) => {
                warn!("failed to write pending_index: {e}");
            }
        }
    }

    /// Serialize, compress, encrypt, and write the pending index journal to storage.
    fn write_now(
        &self,
        session_id: &str,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<()> {
        write_journal(
            &self.entries.to_wire(),
            &session_index_key(session_id),
            storage,
            crypto,
        )
    }

    #[cfg(test)]
    pub(super) fn write_in_flight(&self) -> bool {
        self.write.is_some()
    }

    #[cfg(test)]
    pub(super) fn written_rev(&self) -> Option<u64> {
        self.written_rev
    }
}

/// Serialize, compress, encrypt, and write a pending index journal.
fn write_journal(
    wire: &[PendingPackEntry],
    key: &str,
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
) -> Result<()> {
    let serialized = encode_pending_journal(wire)?;
    let compressed = compress::compress(compress::Compression::Zstd { level: 3 }, &serialized)?;
    let packed = pack_object_with_context(
        ObjectType::PendingIndex,
        PENDING_INDEX_OBJECT_CONTEXT,
        &compressed,
        crypto,
    )?;
    storage.put(key, &packed)?;
    debug!(
        packs = wire.len(),
        bytes = packed.len(),
        key = %key,
        "wrote pending_index to storage"
    );
    Ok(())
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

#[cfg(test)]
pub(super) mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::index::PendingChunkEntry;
    use crate::repo::write_session::tests::test_crypto;
    use crate::testutil::{MemoryBackend, init_test_environment};
    use vykar_types::chunk_id::ChunkId;
    use vykar_types::pack_id::PackId;

    /// Sleeps in `put`, then flags that the PUT completed.
    pub(in crate::repo) struct SlowPutBackend {
        pub(in crate::repo) inner: MemoryBackend,
        pub(in crate::repo) put_done: AtomicBool,
    }

    impl SlowPutBackend {
        pub(in crate::repo) fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                put_done: AtomicBool::new(false),
            }
        }
    }

    impl vykar_storage::InnerBackend for SlowPutBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for SlowPutBackend;
        except [put, put_pack, begin_staged_put, profile];

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
            std::thread::sleep(std::time::Duration::from_millis(100));
            self.inner.put(key, data)?;
            self.put_done.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
            self.inner.put_pack(key, data)
        }
    }

    fn record(journal: &mut SessionJournal, seed: u8) {
        journal.entries.record_pack(
            PackId::from_bytes([seed; 32]),
            vec![PendingChunkEntry {
                chunk_id: ChunkId::from_bytes([seed; 32]),
                stored_size: 10,
                pack_offset: 0,
            }],
        );
    }

    #[test]
    fn dropping_journal_joins_background_write() {
        init_test_environment();
        let backend = Arc::new(SlowPutBackend::new());
        let storage: Arc<dyn StorageBackend> = backend.clone();
        let crypto: Arc<dyn CryptoEngine> = Arc::new(test_crypto());
        let mut journal = SessionJournal::new();
        for seed in 1..=JOURNAL_WRITE_INTERVAL as u8 {
            record(&mut journal, seed);
        }
        journal.maybe_write("s1", &storage, &crypto);
        assert!(journal.write_in_flight());

        drop(journal);
        assert!(backend.put_done.load(Ordering::SeqCst));
        assert!(backend.inner.exists(&session_index_key("s1")).unwrap());
    }

    #[test]
    fn write_best_effort_uses_session_key_and_marks_written() {
        init_test_environment();
        let storage = MemoryBackend::new();
        let crypto = test_crypto();
        let mut journal = SessionJournal::new();
        record(&mut journal, 1);

        journal.write_best_effort("abc", &storage, &crypto);
        assert!(storage.exists(&session_index_key("abc")).unwrap());
        assert!(!journal.needs_write());
        assert_eq!(journal.written_rev(), Some(journal.entries.revision()));
    }
}
