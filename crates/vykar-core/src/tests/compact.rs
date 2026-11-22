use crate::commands::compact::compact_repo;
use crate::compress::Compression;
use crate::repo::control::CancelToken;
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

    let stats = compact_repo(&mut repo, 10.0, None, false, 4, &CancelToken::new()).unwrap();
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
    let stats = compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();

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
    let stats = compact_repo(&mut repo, 90.0, None, false, 4, &CancelToken::new()).unwrap();
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

    let stats = compact_repo(&mut repo, 1.0, None, true, 4, &CancelToken::new()).unwrap();

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

    let stats = compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();

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

/// How the [`crate::testutil::RecordingBackend`] under a test repo lists packs.
#[derive(Clone, Copy, Debug)]
enum Listing {
    /// Per-shard listings without sizes (a REST server before v0.22).
    Plain,
    /// Per-shard listings with sizes (S3, SFTP, local).
    Sized,
    /// One tree listing with sizes (a vykar-server).
    Tree,
    /// One tree listing without sizes (a vykar-server before `?list&sized`).
    UnsizedTree,
    /// The tree listing fails; per-shard listings with sizes.
    FailingTree,
}

fn repo_on_recording_backend(
    listing: Listing,
) -> (crate::repo::Repository, crate::testutil::PutLog) {
    use crate::testutil::RecordingBackend;
    crate::testutil::init_test_environment();
    let (backend, log) = match listing {
        Listing::Plain => RecordingBackend::new(),
        Listing::Sized => RecordingBackend::with_sized_listing(),
        Listing::Tree => RecordingBackend::with_tree_listing(),
        Listing::UnsizedTree => RecordingBackend::with_unsized_tree_listing(),
        Listing::FailingTree => RecordingBackend::with_failing_tree_listing(),
    };
    let mut repo = crate::repo::Repository::init_at_format(
        Box::new(backend),
        crate::repo::EncryptionMode::None,
        crate::repo::ChunkerParams::default(),
        None,
        None,
        None,
        crate::repo::RepoFormat::V5,
    )
    .unwrap();
    repo.begin_write_session(&crate::repo::WriteSessionOptions::default())
        .unwrap();
    (repo, log)
}

/// Compact takes pack sizes from the listing and calls `size()` only for
/// packs listed without one (old REST servers). A vykar-server answers with
/// one tree listing; if that fails, compact lists per shard. The analysis is
/// identical in every case.
#[test]
fn compact_sizes_packs_from_the_listing() {
    for dead in [false, true] {
        let mut runs = Vec::new();
        for listing in [
            Listing::Sized,
            Listing::Plain,
            Listing::Tree,
            Listing::UnsizedTree,
            Listing::FailingTree,
        ] {
            let (mut repo, log) = repo_on_recording_backend(listing);
            // A data and a tree chunk land in two packs.
            let (data_id, _, _) = repo
                .store_chunk(
                    b"data-chunk-for-listing-test",
                    Compression::None,
                    PackType::Data,
                )
                .unwrap();
            repo.store_chunk(
                b"tree-chunk-for-listing-test",
                Compression::None,
                PackType::Tree,
            )
            .unwrap();
            repo.save_state().unwrap();
            if dead {
                repo.chunk_index_mut().decrement(&data_id);
                repo.save_state().unwrap();
            }
            let packs = count_all_packs(&repo) as u64;
            assert!(packs >= 2, "packs {packs}");

            log.reset_metadata_counts();
            let stats = compact_repo(&mut repo, 10.0, None, dead, 4, &CancelToken::new()).unwrap();
            let ctx = format!("{listing:?} dead={dead}");
            let (tree_lists, sized_lists, sizes) = match listing {
                Listing::Plain => (0, 256, packs),
                Listing::Sized => (0, 256, 0),
                Listing::Tree => (1, 0, 0),
                Listing::UnsizedTree => (1, 0, packs),
                Listing::FailingTree => (1, 256, 0),
            };
            assert_eq!(log.tree_list_count(), tree_lists, "{ctx}");
            assert_eq!(log.sized_list_count(), sized_lists, "{ctx}");
            // The one plain listing is the `index.d/` garbage scan that
            // ends a compact that is not a dry run.
            assert_eq!(log.list_count(), u64::from(!dead), "{ctx}");
            assert_eq!(log.size_count(), sizes, "{ctx}");
            runs.push((
                stats.packs_total,
                stats.packs_corrupt,
                stats.packs_orphan,
                stats.blobs_live,
                stats.packs_repacked,
                stats.packs_deleted_empty,
                stats.space_freed,
            ));
        }
        for run in &runs[1..] {
            assert_eq!(run, &runs[0], "dead={dead}");
        }
        if dead {
            assert!(runs[0].6 > 0, "dry run reports dead space: {:?}", runs[0]);
        }
    }
}

/// A pack-like key directly under `packs/` is outside every shard: the tree
/// listing must not hand it to the analysis (which could delete it as an
/// orphan), just as the per-shard listings never see it.
#[test]
fn compact_tree_listing_ignores_keys_outside_shards() {
    let stray = format!("packs/{}", hex::encode([0xcd; 32]));
    let mut runs = Vec::new();
    for listing in [Listing::Tree, Listing::Sized] {
        let (mut repo, _log) = repo_on_recording_backend(listing);
        store_two_chunks(&mut repo);
        repo.storage
            .put(&stray, &[0u8; vykar_protocol::PACK_HEADER_SIZE + 64])
            .unwrap();

        let stats = compact_repo(&mut repo, 0.0, None, false, 4, &CancelToken::new()).unwrap();
        assert!(repo.storage.exists(&stray).unwrap(), "{listing:?}");
        runs.push((stats.packs_total, stats.packs_orphan));
    }
    assert_eq!(runs[0], runs[1]);
}

mod server_side {
    use crate::repo::control::CancelToken;
    use std::sync::{Arc, Mutex};

    use super::store_two_chunks;
    use crate::commands::compact::compact_repo;
    use crate::compress::Compression;
    use crate::index::ChunkIndexEntry;
    use crate::index::segment::Segment;
    use crate::repo::ChunkerParams;
    use crate::repo::index_store;
    use crate::repo::index_write::FoldPolicy;
    use crate::repo::pack::PackType;
    use crate::repo::{EncryptionMode, OpenOptions, RepoFormat, Repository};
    use crate::testutil::{MemoryBackend, init_test_environment};
    use vykar_protocol::{RepackOperationResult, RepackPlanRequest, RepackResultResponse};
    use vykar_storage::{
        BatchDelete, InnerBackend, ServerOp, ServerOps, StorageBackend, delegate_storage_backend,
    };
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
        /// Repack succeeds and per-pack deletes succeed; the batch delete fails.
        FailBatchDelete,
        /// The server does not advertise `repack` (one older than the field).
        NotOffered,
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
        /// Offer a batch delete (as a vykar-server does); otherwise
        /// `batch_delete()` is None.
        batch: bool,
        log: Log,
    }

    impl InnerBackend for ServerRepackBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for ServerRepackBackend;
        except [
            put, delete, put_owned, put_pack, begin_staged_put, conditional, batch_delete,
            server_ops, profile
        ];

        // Test wrapper: keeps every write on `put`.
        fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
            None
        }

        fn batch_delete(&self) -> Option<&dyn BatchDelete> {
            self.batch.then_some(self as &dyn BatchDelete)
        }

        fn server_ops(&self) -> Option<&dyn ServerOps> {
            Some(self)
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

        fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
            self.put(key, data.as_slice())
        }
    }

    impl ServerOps for ServerRepackBackend {
        fn offers(&self, op: ServerOp) -> bool {
            op == ServerOp::Repack && !matches!(self.mode, Mode::NotOffered)
        }

        // The repository is created on this fake, so it must accept the format.
        fn capabilities(&self) -> Result<vykar_protocol::ServerCapabilities> {
            Ok(crate::testutil::fake_server_caps())
        }

        fn init(&self) -> Result<()> {
            Ok(())
        }

        fn repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
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

    impl BatchDelete for ServerRepackBackend {
        fn delete_keys(
            &self,
            keys: &[String],
            scope: vykar_storage::BatchDeleteScope,
        ) -> Result<()> {
            if matches!(self.mode, Mode::FailPackDeletes)
                && keys.iter().any(|k| k.starts_with("packs/"))
            {
                return Err(VykarError::Other("403 Forbidden: batch-delete".into()));
            }
            if matches!(self.mode, Mode::FailBatchDelete) {
                return Err(VykarError::Other("REST batch-delete: HTTP 408".into()));
            }
            self.log
                .push(format!("batch-delete {scope:?} {}", keys.join(",")));
            for key in keys {
                self.inner.delete(key)?;
            }
            Ok(())
        }
    }

    fn server_repo(mode: Mode, batch: bool, format: RepoFormat) -> (Repository, Log) {
        init_test_environment();
        let log = Log::default();
        let storage = Box::new(ServerRepackBackend {
            inner: MemoryBackend::new(),
            mode,
            batch,
            log: log.clone(),
        });
        let mut repo = Repository::init_at_format(
            storage,
            EncryptionMode::None,
            ChunkerParams::default(),
            None,
            None,
            None,
            format,
        )
        .unwrap();
        repo.begin_write_session(&crate::repo::WriteSessionOptions::default())
            .unwrap();
        (repo, log)
    }

    /// One pack with a dead blob (a) and a live one (b), plus one entirely
    /// dead pack (c). Returns (b, partly-dead source key, empty source key).
    fn setup_dead_packs(repo: &mut Repository) -> (ChunkId, String, String) {
        let (id_a, id_b) = store_two_chunks(repo);
        repo.begin_write_session(&crate::repo::WriteSessionOptions::default())
            .unwrap();
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
        let (mut repo, log) = server_repo(Mode::CopyToNewPack, false, RepoFormat::V5);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let stats = compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();

        let plans = log.plans.lock().unwrap().clone();
        assert!(!plans.is_empty());
        assert!(
            plans
                .iter()
                .flat_map(|p| &p.operations)
                .all(|op| !op.delete_after)
        );

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

    /// A server that does not advertise `repack` gets a client-side repack:
    /// no plan is sent, and the dead space is still freed.
    #[test]
    fn server_repack_not_offered_repacks_on_the_client() {
        let (mut repo, log) = server_repo(Mode::NotOffered, false, RepoFormat::V5);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let stats = compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();

        assert!(log.plans.lock().unwrap().is_empty());
        assert!(!log.events().iter().any(|e| e == "repack"));
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
        let (mut repo, log) = server_repo(Mode::ReturnSource, false, RepoFormat::V5);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();

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
        let (mut repo, log) = server_repo(Mode::FailRepack, false, RepoFormat::V5);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        assert!(compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).is_err());

        let events = log.events();
        assert!(
            events.iter().all(|e| !e.starts_with("delete ")
                && e != "put index"
                && !e.starts_with("put index.d/")),
            "{events:?}"
        );
        assert!(repo.storage.exists(&partial).unwrap());
        assert!(repo.storage.exists(&empty).unwrap());
        assert_eq!(pack_of(&repo, &id_b), partial);
    }

    /// `(packs_repacked, packs_deleted_empty, space_freed)` of a successful
    /// compact over [`setup_dead_packs`].
    fn delete_stats(mode: Mode, batch: bool) -> (u64, u64, u64) {
        let (mut repo, _log) = server_repo(mode, batch, RepoFormat::V5);
        setup_dead_packs(&mut repo);
        let stats = compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();
        (
            stats.packs_repacked,
            stats.packs_deleted_empty,
            stats.space_freed,
        )
    }

    fn batch_events(events: &[String]) -> Vec<&String> {
        events
            .iter()
            .filter(|e| e.starts_with("batch-delete"))
            .collect()
    }

    #[test]
    fn sources_deleted_in_one_batch_after_index_saved() {
        let (mut repo, log) = server_repo(Mode::CopyToNewPack, true, RepoFormat::V5);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let stats = compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();

        let events = log.events();
        let batches = batch_events(&events);
        assert_eq!(batches.len(), 1, "{events:?}");
        let batch = batches[0];
        assert!(batch.starts_with("batch-delete KeysOnly "), "{batch}");
        assert!(
            batch.contains(&partial) && batch.contains(&empty),
            "{batch}"
        );
        let pos = |pred: &dyn Fn(&String) -> bool| events.iter().position(pred).unwrap();
        assert!(pos(&|e| e == "put index") < pos(&|e| e == batch));
        assert!(
            events.iter().all(|e| !e.starts_with("delete packs/")),
            "{events:?}"
        );

        assert!(!repo.storage.exists(&partial).unwrap());
        assert!(!repo.storage.exists(&empty).unwrap());
        assert_eq!(
            repo.read_chunk(&id_b).unwrap(),
            b"chunk-b-data-for-compact-test"
        );
        assert_eq!(
            (
                stats.packs_repacked,
                stats.packs_deleted_empty,
                stats.space_freed
            ),
            delete_stats(Mode::CopyToNewPack, false)
        );
    }

    #[test]
    fn batch_never_names_a_source_returned_as_new_pack() {
        let (mut repo, log) = server_repo(Mode::ReturnSource, true, RepoFormat::V5);
        let (_id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();

        let events = log.events();
        let batches = batch_events(&events);
        assert_eq!(batches.len(), 1, "{events:?}");
        assert!(batches[0].contains(&empty), "{events:?}");
        assert!(!batches[0].contains(&partial), "{events:?}");
        assert!(repo.storage.exists(&partial).unwrap());
    }

    #[test]
    fn failed_batch_delete_falls_back_to_per_pack_deletes() {
        let (mut repo, log) = server_repo(Mode::FailBatchDelete, true, RepoFormat::V5);
        let (_id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let stats = compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();

        let events = log.events();
        let pos = |e: &str| {
            events
                .iter()
                .position(|x| x == e)
                .unwrap_or_else(|| panic!("missing event {e:?} in {events:?}"))
        };
        let saved = pos("put index");
        assert!(saved < pos(&format!("delete {partial}")));
        assert!(saved < pos(&format!("delete {empty}")));
        assert!(!repo.storage.exists(&partial).unwrap());
        assert!(!repo.storage.exists(&empty).unwrap());
        assert_eq!(
            (
                stats.packs_repacked,
                stats.packs_deleted_empty,
                stats.space_freed
            ),
            delete_stats(Mode::CopyToNewPack, false)
        );
    }

    /// A second handle on `repo`'s storage with an empty cache directory, index loaded.
    fn cold_open(repo: &Repository) -> (tempfile::TempDir, Repository) {
        let dir = tempfile::tempdir().unwrap();
        let cold = Repository::open(
            Box::new(Arc::clone(&repo.storage)),
            None,
            Some(dir.path().to_path_buf()),
            OpenOptions::new().with_index(),
        )
        .unwrap();
        (dir, cold)
    }

    fn sorted_entries(repo: &Repository) -> Vec<(ChunkId, ChunkIndexEntry)> {
        let mut v: Vec<_> = repo.chunk_index().iter().map(|(id, e)| (*id, *e)).collect();
        v.sort_by_key(|(id, _)| *id.as_bytes());
        v
    }

    /// A cold open equals the warm index, `id_b` reads back from it, and
    /// every pack it names exists.
    fn assert_cold_open_matches(repo: &Repository, id_b: &ChunkId, ctx: &str) {
        let (_dir, cold) = cold_open(repo);
        assert_eq!(sorted_entries(&cold), sorted_entries(repo), "{ctx}");
        assert_eq!(
            cold.read_chunk(id_b).unwrap(),
            b"chunk-b-data-for-compact-test",
            "{ctx}"
        );
        for (id, entry) in cold.chunk_index().iter() {
            assert!(
                repo.storage.exists(&entry.pack_id.storage_key()).unwrap(),
                "{ctx}: pack of {id} is missing"
            );
        }
    }

    /// The last segment the head names, decoded.
    fn last_segment(repo: &Repository) -> Segment {
        let seg = repo.index_head().unwrap().segments.last().unwrap().clone();
        let bytes = repo
            .storage
            .get(&index_store::object_key(&seg.name))
            .unwrap()
            .unwrap();
        index_store::decode_segment(&seg, &bytes, repo.crypto.as_ref()).unwrap()
    }

    fn is_pack_delete(event: &str) -> bool {
        event.starts_with("delete packs/")
            || (event.starts_with("batch-delete") && event.contains("packs/"))
    }

    fn is_segment_put(event: &str) -> bool {
        event.starts_with("put index.d/")
    }

    #[test]
    fn server_repack_persists_the_index_before_deleting_sources() {
        for batch in [false, true] {
            let ctx = format!("batch {batch}");
            let (mut repo, log) = server_repo(Mode::CopyToNewPack, batch, RepoFormat::V5);
            let (id_b, partial, empty) = setup_dead_packs(&mut repo);
            log.events.lock().unwrap().clear();

            compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();

            let events = log.events();
            let first_delete = events
                .iter()
                .position(|e| is_pack_delete(e))
                .unwrap_or_else(|| panic!("{ctx}: no pack delete in {events:?}"));
            let before = &events[..first_delete];
            let heads: Vec<usize> = (0..before.len())
                .filter(|&i| before[i] == "put index")
                .collect();
            assert_eq!(heads.len(), 1, "{ctx}: {events:?}");
            let segments: Vec<usize> = (0..before.len())
                .filter(|&i| is_segment_put(&before[i]))
                .collect();
            assert_eq!(segments.len(), 1, "{ctx}: {events:?}");
            assert!(segments[0] < heads[0], "{ctx}: {events:?}");
            assert!(
                !events.iter().any(|e| e == "put index.gen"),
                "{ctx}: {events:?}"
            );

            assert!(!repo.storage.exists(&partial).unwrap(), "{ctx}");
            assert!(!repo.storage.exists(&empty).unwrap(), "{ctx}");
            assert_cold_open_matches(&repo, &id_b, &ctx);
            let seg = last_segment(&repo);
            assert_eq!(seg.relocations.len(), 1, "{ctx}: {seg:?}");
            assert!(seg.deltas.is_empty(), "{ctx}: {seg:?}");
            assert!(seg.new_entries.is_empty(), "{ctx}: {seg:?}");
        }
    }

    #[test]
    fn server_repack_delete_failure_keeps_the_index_on_new_packs_segmented() {
        for batch in [false, true] {
            let (mut repo, log) = server_repo(Mode::FailPackDeletes, batch, RepoFormat::V5);
            let (id_b, partial, _empty) = setup_dead_packs(&mut repo);
            log.events.lock().unwrap().clear();

            let err =
                compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap_err();
            assert!(err.to_string().contains("append-only"), "{err}");
            // The batch route reports the per-pack error, not its own wrapper.
            assert!(!err.to_string().contains("batch-delete"), "{err}");

            let events = log.events();
            assert_eq!(events.len(), 3, "{events:?}");
            assert_eq!(events[0], "repack");
            let hex = events[1]
                .strip_prefix("put index.d/")
                .unwrap_or_else(|| panic!("{events:?}"));
            assert!(
                hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()),
                "{events:?}"
            );
            assert_eq!(events[2], "put index");

            let ctx = format!("batch {batch}");
            let (_dir, cold) = cold_open(&repo);
            let new_pack = pack_of(&cold, &id_b);
            assert_ne!(new_pack, partial);
            assert!(repo.storage.exists(&new_pack).unwrap());
            drop(cold);
            assert_cold_open_matches(&repo, &id_b, &ctx);
            assert!(
                repo.storage.exists(&partial).unwrap(),
                "orphan source stays"
            );
        }
    }

    #[test]
    fn server_repack_then_fold_keeps_the_index_readable_segmented() {
        let (mut repo, log) = server_repo(Mode::CopyToNewPack, true, RepoFormat::V5);
        let (id_b, _partial, _empty) = setup_dead_packs(&mut repo);
        let mut policy = FoldPolicy::default();
        policy.soft.segments = 1;
        repo.set_fold_policy(policy);
        log.events.lock().unwrap().clear();

        compact_repo(&mut repo, 1.0, None, false, 4, &CancelToken::new()).unwrap();

        let events = log.events();
        let head = repo.index_head().unwrap().clone();
        assert!(head.segments.is_empty(), "{head:?}");
        let base = head.base.as_ref().expect("fold wrote a base");

        // The relocation segment: the only segment PUT of this compact.
        let segment_puts: Vec<&String> = events
            .iter()
            .filter(|e| is_segment_put(e))
            .filter(|e| **e != format!("put {}", index_store::object_key(&base.name)))
            .collect();
        assert_eq!(segment_puts.len(), 1, "{events:?}");
        let relocation = segment_puts[0].strip_prefix("put index.d/").unwrap();
        assert!(
            head.retired.iter().any(|n| n.to_hex() == relocation),
            "{head:?}"
        );

        let base_put = events
            .iter()
            .position(|e| *e == format!("put {}", index_store::object_key(&base.name)))
            .unwrap_or_else(|| panic!("no base PUT in {events:?}"));
        let deletes: Vec<usize> = (0..events.len())
            .filter(|&i| is_pack_delete(&events[i]))
            .collect();
        assert!(!deletes.is_empty(), "{events:?}");
        assert!(deletes.iter().all(|&i| i < base_put), "{events:?}");

        assert_cold_open_matches(&repo, &id_b, "fold");
    }
}

/// Records `remove_stale_temp_files` calls.
struct SweepRecorder {
    inner: std::sync::Arc<dyn vykar_storage::StorageBackend>,
    calls: std::sync::Mutex<Vec<std::time::Duration>>,
}

impl vykar_storage::InnerBackend for SweepRecorder {
    fn inner_backend(&self) -> &dyn vykar_storage::StorageBackend {
        &*self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for SweepRecorder;
    except [remove_stale_temp_files];

    fn remove_stale_temp_files(
        &self,
        older_than: std::time::Duration,
    ) -> vykar_types::error::Result<usize> {
        self.calls.lock().unwrap().push(older_than);
        Ok(2)
    }
}

#[test]
fn compact_sweeps_temp_files_once_and_never_on_dry_run() {
    for dry_run in [true, false] {
        let mut repo = test_repo_plaintext();
        store_two_chunks(&mut repo);
        let recorder = std::sync::Arc::new(SweepRecorder {
            inner: std::sync::Arc::clone(&repo.storage),
            calls: Default::default(),
        });
        repo.storage = recorder.clone();

        let stats = compact_repo(&mut repo, 10.0, None, dry_run, 4, &CancelToken::new()).unwrap();
        let calls = recorder.calls.lock().unwrap().clone();
        if dry_run {
            assert!(calls.is_empty(), "dry run must not sweep");
            assert_eq!(stats.temp_files_removed, 0);
        } else {
            assert_eq!(calls, vec![std::time::Duration::from_secs(3600)]);
            assert_eq!(stats.temp_files_removed, 2);
        }
    }
}

/// Counts `size` calls.
struct SizeCounter {
    inner: std::sync::Arc<dyn vykar_storage::StorageBackend>,
    calls: std::sync::atomic::AtomicUsize,
}

impl vykar_storage::InnerBackend for SizeCounter {
    fn inner_backend(&self) -> &dyn vykar_storage::StorageBackend {
        &*self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for SizeCounter;
    except [size];

    fn size(&self, key: &str) -> vykar_types::error::Result<Option<u64>> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.size(key)
    }
}

/// A shutdown raised before analysis stops it before the first pack is
/// claimed.
#[test]
fn compact_honours_shutdown_during_analysis() {
    let mut repo = test_repo_plaintext();
    store_two_chunks(&mut repo);
    let counter = std::sync::Arc::new(SizeCounter {
        inner: std::sync::Arc::clone(&repo.storage),
        calls: Default::default(),
    });
    repo.storage = counter.clone();
    let packs_before = count_all_packs(&repo);
    assert!(packs_before > 0);

    let shutdown = CancelToken::new();
    shutdown.cancel();
    let err = compact_repo(&mut repo, 1.0, None, false, 4, &shutdown).unwrap_err();
    assert!(
        matches!(err, vykar_types::error::VykarError::Interrupted),
        "{err:?}"
    );
    assert_eq!(counter.calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(count_all_packs(&repo), packs_before);
}
