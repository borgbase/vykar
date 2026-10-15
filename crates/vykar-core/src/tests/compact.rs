use crate::commands::compact::compact_repo;
use crate::compress::Compression;
use crate::repo::pack::PackType;
use crate::testutil::test_repo_plaintext;

/// Helper: store two distinct chunks and flush, returning their chunk IDs.
fn store_two_chunks(
    repo: &mut crate::repo::Repository,
) -> (
    vykar_types::chunk_id::ChunkId,
    vykar_types::chunk_id::ChunkId,
) {
    let (id_a, _, _) = repo
        .store_chunk(
            b"chunk-a-data-for-compact-test",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    let (id_b, _, _) = repo
        .store_chunk(
            b"chunk-b-data-for-compact-test",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    repo.save_state().unwrap();
    (id_a, id_b)
}

#[test]
fn compact_clean_repo_no_repacking() {
    let mut repo = test_repo_plaintext();

    // Store chunks — all live, no dead blobs
    store_two_chunks(&mut repo);

    let stats = compact_repo(&mut repo, 10.0, None, false, 4, None).unwrap();
    assert_eq!(stats.packs_repacked, 0);
    assert_eq!(stats.packs_deleted_empty, 0);
    assert_eq!(stats.space_freed, 0);
}

#[test]
fn compact_after_delete_repacks() {
    let mut repo = test_repo_plaintext();
    let (id_a, id_b) = store_two_chunks(&mut repo);

    // Remove chunk A from the index (simulates delete decrementing refcount to 0)
    repo.chunk_index_mut().decrement(&id_a);
    repo.save_state().unwrap();

    // Compact should detect the dead blob and repack
    let stats = compact_repo(&mut repo, 1.0, None, false, 4, None).unwrap();

    // Should have repacked (or deleted) at least one pack
    assert!(
        stats.packs_repacked > 0 || stats.packs_deleted_empty > 0,
        "expected some packing activity, got: {stats:?}",
    );
    assert!(stats.space_freed > 0);

    // Chunk B should still be readable after compaction
    let data = repo.read_chunk(&id_b).unwrap();
    assert_eq!(data, b"chunk-b-data-for-compact-test");
}

#[test]
fn compact_threshold_filters() {
    let mut repo = test_repo_plaintext();

    // Store 10 chunks, then delete 1 — that's ~10% dead
    let mut ids = Vec::new();
    for i in 0..10 {
        let data = format!("chunk-number-{i}-for-threshold-test-padding-data");
        let (id, _, _) = repo
            .store_chunk(data.as_bytes(), Compression::None, PackType::Data)
            .unwrap();
        ids.push(id);
    }
    repo.save_state().unwrap();

    // Remove one chunk
    repo.chunk_index_mut().decrement(&ids[0]);
    repo.save_state().unwrap();

    // With a very high threshold (90%), nothing should be repacked
    let stats = compact_repo(&mut repo, 90.0, None, false, 4, None).unwrap();
    assert_eq!(stats.packs_repacked, 0);
    assert_eq!(stats.packs_deleted_empty, 0);
}

#[test]
fn compact_dry_run_does_not_modify() {
    let mut repo = test_repo_plaintext();
    let (id_a, id_b) = store_two_chunks(&mut repo);

    // Remove chunk A to create dead blobs
    repo.chunk_index_mut().decrement(&id_a);
    repo.save_state().unwrap();

    // Count packs before
    let packs_before = count_all_packs(&repo);

    let stats = compact_repo(&mut repo, 1.0, None, true, 4, None).unwrap();

    // Dry run should report activity
    assert!(stats.packs_repacked > 0 || stats.packs_deleted_empty > 0 || stats.space_freed > 0);

    // But pack count should be unchanged
    let packs_after = count_all_packs(&repo);
    assert_eq!(packs_before, packs_after, "dry run should not modify packs");

    // Both old chunks' pack data should still exist in storage
    // Chunk B should still be readable
    let data = repo.read_chunk(&id_b).unwrap();
    assert_eq!(data, b"chunk-b-data-for-compact-test");
}

#[test]
fn compact_empty_pack_deleted() {
    let mut repo = test_repo_plaintext();

    // Store a single chunk and flush
    let (id_a, _, _) = repo
        .store_chunk(
            b"lone-chunk-data-for-empty-pack-test",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    repo.save_state().unwrap();

    // Remove it — the pack is now entirely dead
    repo.chunk_index_mut().decrement(&id_a);
    repo.save_state().unwrap();

    let stats = compact_repo(&mut repo, 1.0, None, false, 4, None).unwrap();

    assert_eq!(stats.packs_deleted_empty, 1);
    assert_eq!(stats.packs_repacked, 0);
    assert!(stats.space_freed > 0);
}

/// Count total pack files across all shard dirs.
fn count_all_packs(repo: &crate::repo::Repository) -> usize {
    let mut count = 0;
    for shard in 0u16..256 {
        let prefix = format!("packs/{:02x}/", shard);
        if let Ok(keys) = repo.storage.list(&prefix) {
            count += keys.iter().filter(|k| !k.ends_with('/')).count();
        }
    }
    count
}

mod server_side {
    use std::sync::{Arc, Mutex};

    use super::store_two_chunks;
    use crate::commands::compact::compact_repo;
    use crate::compress::Compression;
    use crate::config::ChunkerConfig;
    use crate::repo::pack::PackType;
    use crate::repo::{EncryptionMode, Repository};
    use crate::testutil::{init_test_environment, MemoryBackend};
    use vykar_protocol::{RepackOperationResult, RepackPlanRequest, RepackResultResponse};
    use vykar_storage::{delegate_storage_backend, InnerBackend, StorageBackend};
    use vykar_types::chunk_id::ChunkId;
    use vykar_types::error::{Result, VykarError};
    use vykar_types::pack_id::PackId;

    #[derive(Clone, Copy)]
    enum Mode {
        /// Copy each source verbatim to a fresh pack key (offsets unchanged).
        CopyToNewPack,
        /// Return the source itself as the new pack.
        ReturnSource,
        /// Fail every repack call.
        FailRepack,
        /// Repack succeeds, but deleting any pack fails.
        FailPackDeletes,
    }

    #[derive(Clone, Default)]
    struct Log {
        events: Arc<Mutex<Vec<String>>>,
        plans: Arc<Mutex<Vec<RepackPlanRequest>>>,
    }

    impl Log {
        fn events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
        fn push(&self, event: String) {
            self.events.lock().unwrap().push(event);
        }
    }

    /// Simulates a vykar-server that writes new packs but never deletes.
    struct ServerRepackBackend {
        inner: MemoryBackend,
        mode: Mode,
        log: Log,
    }

    impl InnerBackend for ServerRepackBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for ServerRepackBackend;
        except [put, delete, put_owned, server_repack];

        fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.log.push(format!("put {key}"));
            self.inner.put(key, data)
        }

        fn delete(&self, key: &str) -> Result<()> {
            if matches!(self.mode, Mode::FailPackDeletes) && key.starts_with("packs/") {
                return Err(VykarError::Other("403 Forbidden: append-only".into()));
            }
            self.log.push(format!("delete {key}"));
            self.inner.delete(key)
        }

        fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
            self.put(key, &data)
        }

        fn server_repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
            self.log.plans.lock().unwrap().push(plan.clone());
            self.log.push("repack".into());
            if matches!(self.mode, Mode::FailRepack) {
                return Err(VykarError::Other("REST repack: timeout".into()));
            }
            let mut completed = Vec::new();
            for op in &plan.operations {
                let offsets: Vec<u64> = op.keep_blobs.iter().map(|b| b.offset).collect();
                let new_pack = if op.keep_blobs.is_empty() {
                    None
                } else if matches!(self.mode, Mode::ReturnSource) {
                    Some(op.source_pack.clone())
                } else {
                    let data = self.inner.get(&op.source_pack)?.expect("source exists");
                    let key = PackId::compute(&[&data[..], b"repacked"].concat(), plan.hash())
                        .storage_key();
                    self.inner.put(&key, &data)?;
                    Some(key)
                };
                completed.push(RepackOperationResult {
                    source_pack: op.source_pack.clone(),
                    new_pack,
                    new_offsets: offsets,
                    deleted: false,
                });
            }
            Ok(RepackResultResponse { completed })
        }
    }

    fn server_repo(mode: Mode) -> (Repository, Log) {
        init_test_environment();
        let log = Log::default();
        let storage = Box::new(ServerRepackBackend {
            inner: MemoryBackend::new(),
            mode,
            log: log.clone(),
        });
        let mut repo = Repository::init(
            storage,
            EncryptionMode::None,
            ChunkerConfig::default(),
            None,
            None,
            None,
        )
        .unwrap();
        repo.begin_write_session().unwrap();
        (repo, log)
    }

    /// One pack with a dead blob (a) and a live one (b), plus one entirely
    /// dead pack (c). Returns (b, partly-dead source key, empty source key).
    fn setup_dead_packs(repo: &mut Repository) -> (ChunkId, String, String) {
        let (id_a, id_b) = store_two_chunks(repo);
        repo.begin_write_session().unwrap();
        let (id_c, _, _) = repo
            .store_chunk(
                b"chunk-c-data-for-compact-test",
                Compression::None,
                PackType::Data,
            )
            .unwrap();
        repo.save_state().unwrap();

        let partial = pack_of(repo, &id_b);
        let empty = pack_of(repo, &id_c);
        assert_ne!(partial, empty, "c must land in its own pack");

        repo.chunk_index_mut().decrement(&id_a);
        repo.chunk_index_mut().decrement(&id_c);
        repo.save_state().unwrap();
        (id_b, partial, empty)
    }

    fn pack_of(repo: &Repository, id: &ChunkId) -> String {
        repo.chunk_index().get(id).unwrap().pack_id.storage_key()
    }

    #[test]
    fn sources_deleted_by_client_after_index_saved() {
        let (mut repo, log) = server_repo(Mode::CopyToNewPack);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let stats = compact_repo(&mut repo, 1.0, None, false, 4, None).unwrap();

        let plans = log.plans.lock().unwrap().clone();
        assert!(!plans.is_empty());
        assert!(plans
            .iter()
            .flat_map(|p| &p.operations)
            .all(|op| !op.delete_after));

        let events = log.events();
        let pos = |e: &str| {
            events
                .iter()
                .position(|x| x == e)
                .unwrap_or_else(|| panic!("missing event {e:?} in {events:?}"))
        };
        let saved = pos("put index");
        assert!(pos("repack") < saved);
        assert!(saved < pos(&format!("delete {partial}")));
        assert!(saved < pos(&format!("delete {empty}")));

        assert!(!repo.storage.exists(&partial).unwrap());
        assert!(!repo.storage.exists(&empty).unwrap());
        assert_ne!(pack_of(&repo, &id_b), partial);
        assert_eq!(
            repo.read_chunk(&id_b).unwrap(),
            b"chunk-b-data-for-compact-test"
        );
        assert_eq!(stats.packs_repacked, 1);
        assert_eq!(stats.packs_deleted_empty, 1);
        assert!(stats.space_freed > 0);
    }

    #[test]
    fn source_returned_as_new_pack_is_kept() {
        let (mut repo, log) = server_repo(Mode::ReturnSource);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        compact_repo(&mut repo, 1.0, None, false, 4, None).unwrap();

        let events = log.events();
        assert!(!events.contains(&format!("delete {partial}")), "{events:?}");
        assert!(events.contains(&format!("delete {empty}")), "{events:?}");
        assert!(repo.storage.exists(&partial).unwrap());
        assert_eq!(pack_of(&repo, &id_b), partial);
        assert_eq!(
            repo.read_chunk(&id_b).unwrap(),
            b"chunk-b-data-for-compact-test"
        );
    }

    #[test]
    fn repack_error_leaves_sources_and_index() {
        let (mut repo, log) = server_repo(Mode::FailRepack);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        assert!(compact_repo(&mut repo, 1.0, None, false, 4, None).is_err());

        let events = log.events();
        assert!(
            events
                .iter()
                .all(|e| !e.starts_with("delete ") && e != "put index"),
            "{events:?}"
        );
        assert!(repo.storage.exists(&partial).unwrap());
        assert!(repo.storage.exists(&empty).unwrap());
        assert_eq!(pack_of(&repo, &id_b), partial);
    }

    #[test]
    fn delete_failure_keeps_index_on_new_packs() {
        let (mut repo, log) = server_repo(Mode::FailPackDeletes);
        let (id_b, partial, _empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let err = compact_repo(&mut repo, 1.0, None, false, 4, None).unwrap_err();
        assert!(err.to_string().contains("append-only"), "{err}");

        let new_pack = pack_of(&repo, &id_b);
        assert_ne!(new_pack, partial);
        assert!(repo.storage.exists(&new_pack).unwrap());
        assert!(
            repo.storage.exists(&partial).unwrap(),
            "orphan source stays"
        );
        assert_eq!(
            repo.read_chunk(&id_b).unwrap(),
            b"chunk-b-data-for-compact-test"
        );

        // The new location is persisted, not just held in memory.
        assert_eq!(log.events(), ["repack", "put index", "put index.gen"]);
    }
}
