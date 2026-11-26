use crate::commands::compact::compact_repo_without_mark;
use crate::compress::Compression;
use crate::repo::control::CancelToken;
use crate::repo::pack::PackType;
use crate::repo::pack_trailer::{TrailerPackType, parse_footer, read_trailer_from_pack_bytes};
use crate::testutil::{test_repo, test_repo_with_session};

/// Helper: store two distinct chunks in a write session of their own and
/// save it, returning their chunk IDs.
fn store_two_chunks(
    repo: &mut crate::repo::Repo,
) -> (
    vykar_types::chunk_id::ChunkId,
    vykar_types::chunk_id::ChunkId,
) {
    let mut ws = repo.begin_write_session(&crate::repo::WriteSessionOptions::default());
    let (id_a, _, _) = ws
        .store_chunk(
            repo,
            b"chunk-a-data-for-compact-test",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    let (id_b, _, _) = ws
        .store_chunk(
            repo,
            b"chunk-b-data-for-compact-test",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    repo.unchecked_writer().save_session(ws).unwrap();
    (id_a, id_b)
}

#[test]
fn compact_clean_repo_no_repacking() {
    let mut repo = test_repo();

    // Store chunks — all live, no dead blobs
    store_two_chunks(&mut repo);

    let stats = compact_repo_without_mark(
        &mut repo.unchecked_writer(),
        10.0,
        None,
        false,
        4,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(stats.packs_repacked, 0);
    assert_eq!(stats.packs_deleted_empty, 0);
    assert_eq!(stats.space_freed, 0);
}

/// The trailer of the pack at `key` lists exactly the index entries
/// that name the pack, under `pack_type`.
fn assert_trailer_matches_index(repo: &crate::repo::Repo, key: &str, pack_type: TrailerPackType) {
    let bytes = repo.storage().get(key).unwrap().unwrap();
    let trailer = read_trailer_from_pack_bytes(&bytes, repo.crypto().as_ref())
        .unwrap()
        .unwrap_or_else(|| panic!("{key} has no trailer"));
    assert_eq!(trailer.pack_type, pack_type);
    let mut indexed: Vec<(vykar_types::chunk_id::ChunkId, u32, u64)> = repo
        .chunk_index()
        .iter()
        .filter(|(_, e)| e.pack_id.storage_key() == key)
        .map(|(id, e)| (id, e.stored_size, e.pack_offset))
        .collect();
    indexed.sort_by_key(|e| e.2);
    let listed: Vec<(vykar_types::chunk_id::ChunkId, u32, u64)> = trailer
        .entries
        .iter()
        .map(|e| (e.chunk_id, e.stored_size, u64::from(e.offset)))
        .collect();
    assert_eq!(listed, indexed, "{key}");
}

/// A pack whose only unindexed bytes are its trailer is never a candidate,
/// whatever the threshold.
#[test]
fn compact_ignores_trailer_only_overhead() {
    let mut repo = test_repo();
    store_two_chunks(&mut repo);
    for threshold in [0.0, 1.0, 10.0] {
        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            threshold,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(stats.packs_repacked, 0, "{threshold}");
        assert_eq!(stats.packs_deleted_empty, 0, "{threshold}");
        assert_eq!(stats.space_freed, 0, "{threshold}");
    }
}

/// A repack's output, trailer included, is not a candidate for the next
/// compact.
#[test]
fn compact_is_idempotent_after_repack() {
    let mut repo = test_repo();
    let (id_a, _id_b) = store_two_chunks(&mut repo);
    repo.unchecked_writer().chunk_index_mut().remove(&id_a);
    repo.unchecked_writer().save_state().unwrap();
    let first = compact_repo_without_mark(
        &mut repo.unchecked_writer(),
        1.0,
        None,
        false,
        4,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(first.packs_repacked, 1, "{first:?}");

    let second = compact_repo_without_mark(
        &mut repo.unchecked_writer(),
        1.0,
        None,
        false,
        4,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(second.packs_repacked, 0, "{second:?}");
    assert_eq!(second.packs_deleted_empty, 0, "{second:?}");
    assert_eq!(second.space_freed, 0, "{second:?}");
}

/// The client repack's output ends in a `Repacked` trailer that lists the
/// pack's index entries.
#[test]
fn client_repack_writes_a_trailer() {
    let mut repo = test_repo();
    let (id_a, id_b) = store_two_chunks(&mut repo);
    repo.unchecked_writer().chunk_index_mut().remove(&id_a);
    repo.unchecked_writer().save_state().unwrap();
    compact_repo_without_mark(
        &mut repo.unchecked_writer(),
        1.0,
        None,
        false,
        4,
        &CancelToken::new(),
    )
    .unwrap();

    let key = repo.chunk_index().get(&id_b).unwrap().pack_id.storage_key();
    assert_trailer_matches_index(&repo, &key, TrailerPackType::Repacked);
}

/// Counts 12-byte range reads (the trailer footer reads compact made
/// before the index recorded trailer lengths).
struct CountingFooterReads {
    inner: std::sync::Arc<dyn vykar_storage::StorageBackend>,
    footer_reads: std::sync::atomic::AtomicUsize,
}

impl vykar_storage::InnerBackend for CountingFooterReads {
    fn inner_backend(&self) -> &dyn vykar_storage::StorageBackend {
        &*self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for CountingFooterReads;
    except [get_range];

    fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> vykar_types::error::Result<Option<Vec<u8>>> {
        if length == 12 {
            self.footer_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.get_range(key, offset, length)
    }
}

/// TRL-1: the trailer overhead comes from the index's `trailer_len`:
/// compact of a clean repository repacks nothing at any threshold and reads
/// no pack footer.
#[test]
fn compact_reads_no_pack_footer() {
    let mut repo = test_repo();
    store_two_chunks(&mut repo);
    let counting = std::sync::Arc::new(CountingFooterReads {
        inner: std::sync::Arc::clone(repo.storage()),
        footer_reads: std::sync::atomic::AtomicUsize::new(0),
    });
    repo.set_storage_for_test(counting.clone());
    for threshold in [0.0, 10.0] {
        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            threshold,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(stats.packs_repacked, 0, "{stats:?}");
        assert_eq!(stats.space_freed, 0, "{stats:?}");
    }
    assert_eq!(
        counting
            .footer_reads
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

/// TRL-2 (client repack): the repack output's recorded trailer length is
/// its real frame.
#[test]
fn client_repack_records_the_output_trailer_length() {
    let mut repo = test_repo();
    let (id_a, id_b) = store_two_chunks(&mut repo);
    repo.unchecked_writer().chunk_index_mut().remove(&id_a);
    repo.unchecked_writer().save_state().unwrap();
    compact_repo_without_mark(
        &mut repo.unchecked_writer(),
        1.0,
        None,
        false,
        4,
        &CancelToken::new(),
    )
    .unwrap();
    let pack = repo.chunk_index().get(&id_b).unwrap().pack_id;
    let bytes = repo.storage().get(&pack.storage_key()).unwrap().unwrap();
    let blob_len = parse_footer(&bytes).unwrap();
    assert_eq!(
        repo.chunk_index().pack_trailer_len(&pack),
        Some(4 + blob_len)
    );
    // And after a cold load.
    let seg = repo.index_head().unwrap().segments.last().unwrap().clone();
    let stored = repo
        .storage()
        .get(&crate::repo::index_store::object_key(&seg.name))
        .unwrap()
        .unwrap();
    let seg =
        crate::repo::index_store::decode_segment_v2(&seg, &stored, repo.crypto().as_ref()).unwrap();
    assert!(seg.pack_trailers.contains(&(pack, 4 + blob_len)), "{seg:?}");
}

#[test]
fn compact_after_delete_repacks() {
    let mut repo = test_repo();
    let (id_a, id_b) = store_two_chunks(&mut repo);

    // Remove chunk A from the index (as a sweep does once nothing references it)
    repo.unchecked_writer().chunk_index_mut().remove(&id_a);
    repo.unchecked_writer().save_state().unwrap();

    // Compact should detect the dead blob and repack
    let stats = compact_repo_without_mark(
        &mut repo.unchecked_writer(),
        1.0,
        None,
        false,
        4,
        &CancelToken::new(),
    )
    .unwrap();

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
    let (mut repo, mut ws) = test_repo_with_session();

    // Store 10 chunks, then delete 1 — that's ~10% dead
    let mut ids = Vec::new();
    for i in 0..10 {
        let data = format!("chunk-number-{i}-for-threshold-test-padding-data");
        let (id, _, _) = ws
            .store_chunk(&repo, data.as_bytes(), Compression::None, PackType::Data)
            .unwrap();
        ids.push(id);
    }
    repo.unchecked_writer().save_session(ws).unwrap();

    // Remove one chunk
    repo.unchecked_writer().chunk_index_mut().remove(&ids[0]);
    repo.unchecked_writer().save_state().unwrap();

    // With a very high threshold (90%), nothing should be repacked
    let stats = compact_repo_without_mark(
        &mut repo.unchecked_writer(),
        90.0,
        None,
        false,
        4,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(stats.packs_repacked, 0);
    assert_eq!(stats.packs_deleted_empty, 0);
}

#[test]
fn compact_dry_run_does_not_modify() {
    let mut repo = test_repo();
    let (id_a, id_b) = store_two_chunks(&mut repo);

    // Remove chunk A to create dead blobs
    repo.unchecked_writer().chunk_index_mut().remove(&id_a);
    repo.unchecked_writer().save_state().unwrap();

    // Count packs before
    let packs_before = count_all_packs(&repo);

    let stats = compact_repo_without_mark(
        &mut repo.unchecked_writer(),
        1.0,
        None,
        true,
        4,
        &CancelToken::new(),
    )
    .unwrap();

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
    let (mut repo, mut ws) = test_repo_with_session();

    // Store a single chunk and flush
    let (id_a, _, _) = ws
        .store_chunk(
            &repo,
            b"lone-chunk-data-for-empty-pack-test",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    repo.unchecked_writer().save_session(ws).unwrap();

    // Remove it — the pack is now entirely dead
    repo.unchecked_writer().chunk_index_mut().remove(&id_a);
    repo.unchecked_writer().save_state().unwrap();
    // Subject: deletion mechanics, not the age guard.
    repo.set_pack_grace(std::time::Duration::ZERO);

    let stats = compact_repo_without_mark(
        &mut repo.unchecked_writer(),
        1.0,
        None,
        false,
        4,
        &CancelToken::new(),
    )
    .unwrap();

    assert_eq!(stats.packs_deleted_empty, 1);
    assert_eq!(stats.packs_repacked, 0);
    assert!(stats.space_freed > 0);
}

/// Count total pack files across all shard dirs.
fn count_all_packs(repo: &crate::repo::Repo) -> usize {
    let mut count = 0;
    for shard in 0u16..256 {
        let prefix = format!("packs/{:02x}/", shard);
        if let Ok(keys) = repo.storage().list(&prefix) {
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
) -> (
    crate::repo::Repo,
    crate::repo::WriteSession,
    crate::testutil::PutLog,
) {
    use crate::testutil::RecordingBackend;
    crate::testutil::init_test_environment();
    let (backend, log) = match listing {
        Listing::Plain => RecordingBackend::new(),
        Listing::Sized => RecordingBackend::with_sized_listing(),
        Listing::Tree => RecordingBackend::with_tree_listing(),
        Listing::UnsizedTree => RecordingBackend::with_unsized_tree_listing(),
        Listing::FailingTree => RecordingBackend::with_failing_tree_listing(),
    };
    let repo = crate::repo::Repo::init_at_format(
        Box::new(backend),
        crate::repo::EncryptionMode::Aes256Gcm,
        crate::repo::ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
        crate::repo::RepoFormat::CURRENT,
    )
    .unwrap();
    let ws = repo.begin_write_session(&crate::repo::WriteSessionOptions::default());
    (repo, ws, log)
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
            let (mut repo, mut ws, log) = repo_on_recording_backend(listing);
            // Subject: the listing, not the age guard (no mtimes here).
            repo.set_pack_grace(std::time::Duration::ZERO);
            // A data and a tree chunk land in two packs.
            let (data_id, _, _) = ws
                .store_chunk(
                    &repo,
                    b"data-chunk-for-listing-test",
                    Compression::None,
                    PackType::Data,
                )
                .unwrap();
            ws.store_chunk(
                &repo,
                b"tree-chunk-for-listing-test",
                Compression::None,
                PackType::Tree,
            )
            .unwrap();
            repo.unchecked_writer().save_session(ws).unwrap();
            if dead {
                repo.unchecked_writer().chunk_index_mut().remove(&data_id);
                repo.unchecked_writer().save_state().unwrap();
            }
            let packs = count_all_packs(&repo) as u64;
            assert!(packs >= 2, "packs {packs}");

            log.reset_metadata_counts();
            let stats = compact_repo_without_mark(
                &mut repo.unchecked_writer(),
                10.0,
                None,
                dead,
                4,
                &CancelToken::new(),
            )
            .unwrap();
            let ctx = format!("{listing:?} dead={dead}");
            let (tree_lists, sized_lists, sizes) = match listing {
                Listing::Plain => (0, 256, packs),
                Listing::Sized => (0, 256, 0),
                Listing::Tree => (1, 0, 0),
                Listing::UnsizedTree => (1, 0, packs),
                Listing::FailingTree => (1, 256, 0),
            };
            // A compact that is not a dry run ends with the index upkeep and
            // the pack-map pass: the latest head's listing and the
            // `packmaps/` listing (plain), and the garbage collection's
            // listings of `index.h/` and `index.d/` with mtimes (sized).
            let upkeep = u64::from(!dead);
            assert_eq!(log.tree_list_count(), tree_lists, "{ctx}");
            assert_eq!(log.sized_list_count(), sized_lists + 2 * upkeep, "{ctx}");
            assert_eq!(log.list_count(), 2 * upkeep, "{ctx}");
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
        let (mut repo, _ws, _log) = repo_on_recording_backend(listing);
        store_two_chunks(&mut repo);
        repo.storage()
            .put(&stray, &[0u8; vykar_protocol::PACK_HEADER_SIZE + 64])
            .unwrap();

        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            0.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();
        assert!(repo.storage().exists(&stray).unwrap(), "{listing:?}");
        runs.push((stats.packs_total, stats.packs_orphan));
    }
    assert_eq!(runs[0], runs[1]);
}

mod server_side {
    use crate::repo::control::CancelToken;
    use std::sync::{Arc, Mutex};

    use super::assert_trailer_matches_index;
    use super::store_two_chunks;
    use crate::commands::compact::compact_repo_without_mark;
    use crate::compress::Compression;
    use crate::index::ChunkIndexEntry;
    use crate::index::segment_v2::SegmentBody;
    use crate::repo::ChunkerParams;
    use crate::repo::index_store;
    use crate::repo::index_write::FoldPolicy;
    use crate::repo::pack::PackType;
    use crate::repo::pack_trailer::{
        TrailerPackType, framed_offsets, read_trailer_from_pack_bytes,
    };
    use crate::repo::{EncryptionMode, OpenOptions, Repo, RepoFormat};
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
        /// Repack as a vykar-server does: the kept blobs framed after a pack
        /// header, then the operation's trailer, under the hash of the bytes.
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
        /// The server advertises `repack` but not `repack_trailer` (0.8 to
        /// 0.24).
        NoTrailer,
        /// As `CopyToNewPack`, but every returned offset is one too high.
        WrongOffsets,
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
            match self.mode {
                Mode::NotOffered => false,
                Mode::NoTrailer => op == ServerOp::Repack,
                _ => matches!(op, ServerOp::Repack | ServerOp::RepackTrailer),
            }
        }

        // The repository is created on this fake, so it must accept the format.
        fn capabilities(&self) -> Result<vykar_protocol::ServerCapabilities> {
            let mut caps = crate::testutil::fake_server_caps();
            caps.repack_trailer = !matches!(self.mode, Mode::NotOffered | Mode::NoTrailer);
            Ok(caps)
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
            if plan.has_trailer()
                && plan.protocol_version() < vykar_protocol::REPACK_TRAILER_PROTOCOL_VERSION
            {
                return Err(VykarError::Other(
                    "test fake: a trailer plan must declare protocol 3".into(),
                ));
            }
            let mut completed = Vec::new();
            for op in &plan.operations {
                if op.trailer.is_some() == op.keep_blobs.is_empty() {
                    return Err(VykarError::Other(
                        "test fake: every op that keeps blobs carries a trailer, no other does"
                            .into(),
                    ));
                }
                let mut offsets: Vec<u64> =
                    framed_offsets(op.keep_blobs.iter().map(|b| b.length as u32)).collect();
                let new_pack = if op.keep_blobs.is_empty() {
                    None
                } else if matches!(self.mode, Mode::ReturnSource) {
                    // As a server whose output already exists: the predicted
                    // offsets, and the existing pack.
                    Some(op.source_pack.clone())
                } else {
                    let data = self.inner.get(&op.source_pack)?.expect("source exists");
                    let mut out = data[..vykar_protocol::PACK_HEADER_SIZE].to_vec();
                    for blob in &op.keep_blobs {
                        let (start, len) = (blob.offset as usize, blob.length as usize);
                        out.extend_from_slice(&(len as u32).to_le_bytes());
                        out.extend_from_slice(&data[start..start + len]);
                    }
                    if let Some(trailer) = &op.trailer {
                        out.extend_from_slice(&(trailer.len() as u32).to_le_bytes());
                        out.extend_from_slice(trailer);
                    }
                    let key = PackId::compute(&out, plan.hash()).storage_key();
                    self.inner.put(&key, &out)?;
                    Some(key)
                };
                if matches!(self.mode, Mode::WrongOffsets) {
                    for o in &mut offsets {
                        *o += 1;
                    }
                }
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

    fn server_repo(mode: Mode, batch: bool, format: RepoFormat) -> (Repo, Log) {
        init_test_environment();
        let log = Log::default();
        let storage = Box::new(ServerRepackBackend {
            inner: MemoryBackend::new(),
            mode,
            batch,
            log: log.clone(),
        });
        let mut repo = Repo::init_at_format(
            storage,
            EncryptionMode::Aes256Gcm,
            ChunkerParams::default(),
            Some(crate::testing::TEST_PASSPHRASE),
            None,
            None,
            format,
        )
        .unwrap();
        // Subject: repack and deletion mechanics, not the age guard.
        repo.set_pack_grace(std::time::Duration::ZERO);
        (repo, log)
    }

    /// One pack with a dead blob (a) and a live one (b), plus one entirely
    /// dead pack (c). Returns (b, partly-dead source key, empty source key).
    fn setup_dead_packs(repo: &mut Repo) -> (ChunkId, String, String) {
        let (id_a, id_b) = store_two_chunks(repo);
        let mut ws = repo.begin_write_session(&crate::repo::WriteSessionOptions::default());
        let (id_c, _, _) = ws
            .store_chunk(
                repo,
                b"chunk-c-data-for-compact-test",
                Compression::None,
                PackType::Data,
            )
            .unwrap();
        repo.unchecked_writer().save_session(ws).unwrap();

        let partial = pack_of(repo, &id_b);
        let empty = pack_of(repo, &id_c);
        assert_ne!(partial, empty, "c must land in its own pack");

        repo.unchecked_writer().chunk_index_mut().remove(&id_a);
        repo.unchecked_writer().chunk_index_mut().remove(&id_c);
        repo.unchecked_writer().save_state().unwrap();
        (id_b, partial, empty)
    }

    /// As [`setup_dead_packs`], but the first blob (a, at offset 13) stays
    /// live and b dies, so the live blobs are a prefix of the source and a
    /// server returning the source names their offsets correctly. Returns
    /// (a, partly-dead source key, empty source key).
    fn setup_dead_packs_live_first(repo: &mut Repo) -> (ChunkId, String, String) {
        let (id_a, id_b) = store_two_chunks(repo);
        let mut ws = repo.begin_write_session(&crate::repo::WriteSessionOptions::default());
        let (id_c, _, _) = ws
            .store_chunk(
                repo,
                b"chunk-c-data-for-compact-test",
                Compression::None,
                PackType::Data,
            )
            .unwrap();
        repo.unchecked_writer().save_session(ws).unwrap();

        let partial = pack_of(repo, &id_a);
        let empty = pack_of(repo, &id_c);
        assert_ne!(partial, empty, "c must land in its own pack");
        assert_eq!(repo.chunk_index().get(&id_a).unwrap().pack_offset, 13);

        repo.unchecked_writer().chunk_index_mut().remove(&id_b);
        repo.unchecked_writer().chunk_index_mut().remove(&id_c);
        repo.unchecked_writer().save_state().unwrap();
        (id_a, partial, empty)
    }

    fn pack_of(repo: &Repo, id: &ChunkId) -> String {
        repo.chunk_index().get(id).unwrap().pack_id.storage_key()
    }

    #[test]
    fn sources_deleted_by_client_after_index_saved() {
        let (mut repo, log) = server_repo(Mode::CopyToNewPack, false, RepoFormat::CURRENT);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();

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
                .position(|x| x == e || (e.ends_with('/') && x.starts_with(e)))
                .unwrap_or_else(|| panic!("missing event {e:?} in {events:?}"))
        };
        let saved = pos("put index.h/");
        assert!(pos("repack") < saved);
        assert!(saved < pos(&format!("delete {partial}")));
        assert!(saved < pos(&format!("delete {empty}")));

        assert!(!repo.storage().exists(&partial).unwrap());
        assert!(!repo.storage().exists(&empty).unwrap());
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
        let (mut repo, log) = server_repo(Mode::NotOffered, false, RepoFormat::CURRENT);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();

        assert!(log.plans.lock().unwrap().is_empty());
        assert!(!log.events().iter().any(|e| e == "repack"));
        assert!(!repo.storage().exists(&partial).unwrap());
        assert!(!repo.storage().exists(&empty).unwrap());
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
        let (mut repo, log) = server_repo(Mode::ReturnSource, false, RepoFormat::CURRENT);
        let (id_a, partial, empty) = setup_dead_packs_live_first(&mut repo);
        log.events.lock().unwrap().clear();

        compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();

        let events = log.events();
        assert!(!events.contains(&format!("delete {partial}")), "{events:?}");
        assert!(events.contains(&format!("delete {empty}")), "{events:?}");
        assert!(repo.storage().exists(&partial).unwrap());
        assert_eq!(pack_of(&repo, &id_a), partial);
        assert_eq!(
            repo.read_chunk(&id_a).unwrap(),
            b"chunk-a-data-for-compact-test"
        );
    }

    #[test]
    fn repack_error_leaves_sources_and_index() {
        let (mut repo, log) = server_repo(Mode::FailRepack, false, RepoFormat::CURRENT);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        assert!(
            compact_repo_without_mark(
                &mut repo.unchecked_writer(),
                1.0,
                None,
                false,
                4,
                &CancelToken::new()
            )
            .is_err()
        );

        let events = log.events();
        assert!(
            events.iter().all(|e| !e.starts_with("delete ")
                && !e.starts_with("put index.h/")
                && !e.starts_with("put index.d/")),
            "{events:?}"
        );
        assert!(repo.storage().exists(&partial).unwrap());
        assert!(repo.storage().exists(&empty).unwrap());
        assert_eq!(pack_of(&repo, &id_b), partial);
    }

    /// `(packs_repacked, packs_deleted_empty, space_freed)` of a successful
    /// compact over [`setup_dead_packs`].
    fn delete_stats(mode: Mode, batch: bool) -> (u64, u64, u64) {
        let (mut repo, _log) = server_repo(mode, batch, RepoFormat::CURRENT);
        setup_dead_packs(&mut repo);
        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();
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
        let (mut repo, log) = server_repo(Mode::CopyToNewPack, true, RepoFormat::CURRENT);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();

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
        assert!(pos(&|e| e.starts_with("put index.h/")) < pos(&|e| e == batch));
        assert!(
            events.iter().all(|e| !e.starts_with("delete packs/")),
            "{events:?}"
        );

        assert!(!repo.storage().exists(&partial).unwrap());
        assert!(!repo.storage().exists(&empty).unwrap());
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
        let (mut repo, log) = server_repo(Mode::ReturnSource, true, RepoFormat::CURRENT);
        let (_id_a, partial, empty) = setup_dead_packs_live_first(&mut repo);
        log.events.lock().unwrap().clear();

        compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();

        let events = log.events();
        let batches = batch_events(&events);
        assert_eq!(batches.len(), 1, "{events:?}");
        assert!(batches[0].contains(&empty), "{events:?}");
        assert!(!batches[0].contains(&partial), "{events:?}");
        assert!(repo.storage().exists(&partial).unwrap());
    }

    #[test]
    fn failed_batch_delete_falls_back_to_per_pack_deletes() {
        let (mut repo, log) = server_repo(Mode::FailBatchDelete, true, RepoFormat::CURRENT);
        let (_id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();

        let events = log.events();
        let pos = |e: &str| {
            events
                .iter()
                .position(|x| x == e || (e.ends_with('/') && x.starts_with(e)))
                .unwrap_or_else(|| panic!("missing event {e:?} in {events:?}"))
        };
        let saved = pos("put index.h/");
        assert!(saved < pos(&format!("delete {partial}")));
        assert!(saved < pos(&format!("delete {empty}")));
        assert!(!repo.storage().exists(&partial).unwrap());
        assert!(!repo.storage().exists(&empty).unwrap());
        assert_eq!(
            (
                stats.packs_repacked,
                stats.packs_deleted_empty,
                stats.space_freed
            ),
            delete_stats(Mode::CopyToNewPack, false)
        );
    }

    /// Every plan carries trailers at protocol 3; each trailer opens under the
    /// repository's key and lists the kept chunks at the offsets the server
    /// writes them to, and the new pack's trailer matches the index.
    #[test]
    fn server_repack_sends_trailers() {
        let (mut repo, log) = server_repo(Mode::CopyToNewPack, true, RepoFormat::CURRENT);
        let (id_b, _partial, _empty) = setup_dead_packs(&mut repo);
        compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();

        let plans = log.plans.lock().unwrap().clone();
        assert!(!plans.is_empty());
        let mut kept_ops = 0;
        for plan in &plans {
            assert_eq!(plan.protocol_version(), 3);
            for op in &plan.operations {
                let Some(blob) = &op.trailer else {
                    assert!(op.keep_blobs.is_empty());
                    continue;
                };
                kept_ops += 1;
                let trailer =
                    crate::repo::pack_trailer::PackTrailer::open_blob(blob, repo.crypto().as_ref())
                        .unwrap();
                assert_eq!(trailer.pack_type, TrailerPackType::Repacked);
                let lengths: Vec<u32> = op.keep_blobs.iter().map(|b| b.length as u32).collect();
                let listed: Vec<u32> = trailer.entries.iter().map(|e| e.stored_size).collect();
                assert_eq!(listed, lengths);
                assert!(trailer.offsets().eq(framed_offsets(lengths)));
                assert!(trailer.entries.iter().any(|e| e.chunk_id == id_b));
            }
        }
        assert_eq!(kept_ops, 1);

        let new_pack = pack_of(&repo, &id_b);
        let bytes = repo.storage().get(&new_pack).unwrap().unwrap();
        assert!(
            read_trailer_from_pack_bytes(&bytes, repo.crypto().as_ref())
                .unwrap()
                .is_some()
        );
        assert_trailer_matches_index(&repo, &new_pack, TrailerPackType::Repacked);
    }

    /// A server with `repack` but without `repack_trailer` would drop the
    /// trailers: the client repacks, with the client path's results.
    #[test]
    fn server_without_repack_trailer_repacks_on_the_client() {
        let (mut repo, log) = server_repo(Mode::NoTrailer, false, RepoFormat::CURRENT);
        let (id_b, partial, empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();

        assert!(log.plans.lock().unwrap().is_empty());
        assert!(!repo.storage().exists(&partial).unwrap());
        assert!(!repo.storage().exists(&empty).unwrap());
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
            delete_stats(Mode::NotOffered, false)
        );
        assert_trailer_matches_index(&repo, &pack_of(&repo, &id_b), TrailerPackType::Repacked);
    }

    /// Offsets that differ from the layout the trailer describes are refused:
    /// the index keeps the source, which is not deleted. (The delete-only
    /// operation ahead of it in the batch is applied, as any operation before
    /// a failed one is.)
    #[test]
    fn wrong_server_offsets_are_refused() {
        let (mut repo, log) = server_repo(Mode::WrongOffsets, true, RepoFormat::CURRENT);
        let (id_b, partial, _empty) = setup_dead_packs(&mut repo);
        log.events.lock().unwrap().clear();

        let err = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("differ from the layout"), "{err}");
        assert!(repo.storage().exists(&partial).unwrap());
        assert_eq!(pack_of(&repo, &id_b), partial);
        let (_dir, cold) = cold_open(&repo);
        assert_eq!(pack_of(&cold, &id_b), partial);
        assert!(
            !log.events()
                .iter()
                .any(|e| e.contains(&partial) && is_pack_delete(e)),
            "{:?}",
            log.events()
        );
    }

    /// A second handle on `repo`'s storage with an empty cache directory, index loaded.
    fn cold_open(repo: &Repo) -> (tempfile::TempDir, Repo) {
        let dir = tempfile::tempdir().unwrap();
        let cold = Repo::open(
            Box::new(Arc::clone(repo.storage())),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(dir.path().to_path_buf()),
            OpenOptions::new().with_index(),
        )
        .unwrap();
        (dir, cold)
    }

    fn sorted_entries(repo: &Repo) -> Vec<(ChunkId, ChunkIndexEntry)> {
        let mut v: Vec<_> = repo.chunk_index().iter().collect();
        v.sort_by_key(|(id, _)| *id.as_bytes());
        v
    }

    /// A cold open equals the warm index, `id_b` reads back from it, and
    /// every pack it names exists.
    fn assert_cold_open_matches(repo: &Repo, id_b: &ChunkId, ctx: &str) {
        let (_dir, cold) = cold_open(repo);
        assert_eq!(sorted_entries(&cold), sorted_entries(repo), "{ctx}");
        assert_eq!(
            cold.read_chunk(id_b).unwrap(),
            b"chunk-b-data-for-compact-test",
            "{ctx}"
        );
        for (id, entry) in cold.chunk_index().iter() {
            assert!(
                repo.storage().exists(&entry.pack_id.storage_key()).unwrap(),
                "{ctx}: pack of {id} is missing"
            );
        }
    }

    /// The last segment the head names, decoded.
    fn last_segment(repo: &Repo) -> SegmentBody {
        let seg = repo.index_head().unwrap().segments.last().unwrap().clone();
        let bytes = repo
            .storage()
            .get(&index_store::object_key(&seg.name))
            .unwrap()
            .unwrap();
        index_store::decode_segment_v2(&seg, &bytes, repo.crypto().as_ref()).unwrap()
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
            let (mut repo, log) = server_repo(Mode::CopyToNewPack, batch, RepoFormat::CURRENT);
            let (id_b, partial, empty) = setup_dead_packs(&mut repo);
            log.events.lock().unwrap().clear();

            compact_repo_without_mark(
                &mut repo.unchecked_writer(),
                1.0,
                None,
                false,
                4,
                &CancelToken::new(),
            )
            .unwrap();

            let events = log.events();
            let first_delete = events
                .iter()
                .position(|e| is_pack_delete(e))
                .unwrap_or_else(|| panic!("{ctx}: no pack delete in {events:?}"));
            let before = &events[..first_delete];
            let heads: Vec<usize> = (0..before.len())
                .filter(|&i| before[i].starts_with("put index.h/"))
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

            assert!(!repo.storage().exists(&partial).unwrap(), "{ctx}");
            assert!(!repo.storage().exists(&empty).unwrap(), "{ctx}");
            assert_cold_open_matches(&repo, &id_b, &ctx);
            let seg = last_segment(&repo);
            assert_eq!(seg.relocations.len(), 1, "{ctx}: {seg:?}");
            assert!(seg.removals.is_empty(), "{ctx}: {seg:?}");
            assert!(seg.additions.is_empty(), "{ctx}: {seg:?}");
            // TRL-2 (server repack): the output pack's recorded trailer
            // length is its real frame.
            let output = seg.relocations[0].1.pack_id;
            let bytes = repo.storage().get(&output.storage_key()).unwrap().unwrap();
            let blob_len = crate::repo::pack_trailer::parse_footer(&bytes).unwrap();
            assert_eq!(seg.pack_trailers, vec![(output, 4 + blob_len)], "{ctx}");
        }
    }

    #[test]
    fn server_repack_delete_failure_keeps_the_index_on_new_packs_segmented() {
        for batch in [false, true] {
            let (mut repo, log) = server_repo(Mode::FailPackDeletes, batch, RepoFormat::CURRENT);
            let (id_b, partial, _empty) = setup_dead_packs(&mut repo);
            log.events.lock().unwrap().clear();

            let err = compact_repo_without_mark(
                &mut repo.unchecked_writer(),
                1.0,
                None,
                false,
                4,
                &CancelToken::new(),
            )
            .unwrap_err();
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
            assert!(events[2].starts_with("put index.h/"), "{events:?}");

            let ctx = format!("batch {batch}");
            let (_dir, cold) = cold_open(&repo);
            let new_pack = pack_of(&cold, &id_b);
            assert_ne!(new_pack, partial);
            assert!(repo.storage().exists(&new_pack).unwrap());
            drop(cold);
            assert_cold_open_matches(&repo, &id_b, &ctx);
            assert!(
                repo.storage().exists(&partial).unwrap(),
                "orphan source stays"
            );
        }
    }

    #[test]
    fn server_repack_then_fold_keeps_the_index_readable_segmented() {
        let (mut repo, log) = server_repo(Mode::CopyToNewPack, true, RepoFormat::CURRENT);
        let (id_b, _partial, _empty) = setup_dead_packs(&mut repo);
        let mut policy = FoldPolicy::default();
        policy.soft.segments = 1;
        repo.set_fold_policy(policy);
        log.events.lock().unwrap().clear();

        compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();

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
        let mut repo = test_repo();
        store_two_chunks(&mut repo);
        let recorder = std::sync::Arc::new(SweepRecorder {
            inner: std::sync::Arc::clone(repo.storage()),
            calls: Default::default(),
        });
        repo.set_storage_for_test(recorder.clone());

        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            10.0,
            None,
            dry_run,
            4,
            &CancelToken::new(),
        )
        .unwrap();
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

/// A cancellation requested before analysis stops it before the first pack is
/// claimed.
#[test]
fn compact_honours_shutdown_during_analysis() {
    let mut repo = test_repo();
    store_two_chunks(&mut repo);
    let counter = std::sync::Arc::new(SizeCounter {
        inner: std::sync::Arc::clone(repo.storage()),
        calls: Default::default(),
    });
    repo.set_storage_for_test(counter.clone());
    let packs_before = count_all_packs(&repo);
    assert!(packs_before > 0);

    let cancel = CancelToken::new();
    cancel.cancel();
    let err = compact_repo_without_mark(&mut repo.unchecked_writer(), 1.0, None, false, 4, &cancel)
        .unwrap_err();
    assert!(
        matches!(err, vykar_types::error::VykarError::Interrupted),
        "{err:?}"
    );
    assert_eq!(counter.calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(count_all_packs(&repo), packs_before);
}

/// 04 step 5: orphan packs (no index entry names them) go only once they are
/// T_pack old on the store's clock, read against the lock's reading of
/// "now"; of unknown age they stay.
mod store_clock_ages {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};

    use crate::commands::compact::compact_repo_without_mark;
    use crate::compress::Compression;
    use crate::repo::control::CancelToken;
    use crate::repo::pack::PackType;
    use crate::repo::{ChunkerParams, EncryptionMode, Repo, WriteSessionOptions};
    use crate::testutil::{MemoryBackend, init_test_environment};
    use vykar_storage::{InnerBackend, StorageBackend, StoreNow, delegate_storage_backend};
    use vykar_types::error::Result;

    /// A memory backend whose `list_meta` reports mtimes: `now` for every
    /// key unless `set_age` gave it another. With `tree` it is also a
    /// server whose `list_tree_meta` answers the same listing in one
    /// request (counted in `tree_lists`).
    struct MtimeBackend {
        inner: MemoryBackend,
        now: SystemTime,
        mtimes: Mutex<std::collections::HashMap<String, SystemTime>>,
        tree: bool,
        tree_lists: AtomicUsize,
    }

    impl MtimeBackend {
        fn new(tree: bool) -> Self {
            Self {
                inner: MemoryBackend::new(),
                now: SystemTime::UNIX_EPOCH + Duration::from_secs(1 << 31),
                mtimes: Mutex::default(),
                tree,
                tree_lists: AtomicUsize::new(0),
            }
        }

        fn set_age(&self, key: &str, age: Duration) {
            self.mtimes
                .lock()
                .unwrap()
                .insert(key.to_string(), self.now - age);
        }

        fn listing(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
            let mtimes = self.mtimes.lock().unwrap();
            Ok(self
                .inner
                .list_sized(prefix)?
                .into_iter()
                .map(|mut o| {
                    o.mtime = Some(mtimes.get(&o.key).copied().unwrap_or(self.now));
                    o
                })
                .collect())
        }
    }

    impl InnerBackend for MtimeBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    impl vykar_storage::ServerOps for MtimeBackend {
        fn offers(&self, op: vykar_storage::ServerOp) -> bool {
            op == vykar_storage::ServerOp::ListTree
        }

        fn capabilities(&self) -> Result<vykar_protocol::ServerCapabilities> {
            Ok(crate::testutil::fake_server_caps())
        }

        fn init(&self) -> Result<()> {
            Ok(())
        }

        fn list_tree_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
            self.tree_lists.fetch_add(1, Ordering::Relaxed);
            self.listing(prefix)
        }
    }

    delegate_storage_backend! {
        for MtimeBackend;
        except [list_meta, server_ops];

        fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
            self.listing(prefix)
        }

        fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
            self.tree.then_some(self as &dyn vykar_storage::ServerOps)
        }
    }

    /// Three orphan packs, one chunk each; returns their keys.
    fn three_orphans(repo: &mut Repo) -> Vec<String> {
        let mut keys = Vec::new();
        for i in 0..3 {
            let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
            let data = format!("orphan-chunk-{i}-for-the-age-guard");
            let (id, _, _) = ws
                .store_chunk(repo, data.as_bytes(), Compression::None, PackType::Data)
                .unwrap();
            repo.unchecked_writer().save_session(ws).unwrap();
            keys.push(repo.chunk_index().get(&id).unwrap().pack_id.storage_key());
            repo.unchecked_writer().chunk_index_mut().remove(&id);
            repo.unchecked_writer().save_state().unwrap();
        }
        keys.dedup();
        assert_eq!(keys.len(), 3, "one pack per chunk");
        keys
    }

    #[test]
    fn young_orphans_are_kept_and_old_ones_deleted() {
        young_orphans_kept_old_deleted(false);
    }

    /// The same through a one-request tree listing (a vykar-server): the
    /// mtimes `list_tree_meta` reports reach the age guard.
    #[test]
    fn young_orphans_are_kept_through_a_tree_listing() {
        young_orphans_kept_old_deleted(true);
    }

    fn young_orphans_kept_old_deleted(tree: bool) {
        init_test_environment();
        let backend = Arc::new(MtimeBackend::new(tree));
        let shared: Arc<dyn StorageBackend> = backend.clone();
        let mut repo = Repo::init(
            Box::new(shared),
            EncryptionMode::Aes256Gcm,
            ChunkerParams::default(),
            Some(crate::testing::TEST_PASSPHRASE),
            None,
            None,
        )
        .unwrap();
        let keys = three_orphans(&mut repo);
        let minute = Duration::from_secs(60);
        backend.set_age(&keys[0], Duration::from_secs(3600));
        backend.set_age(&keys[1], Duration::from_secs(12 * 3600) - minute);
        backend.set_age(&keys[2], Duration::from_secs(12 * 3600) + minute);
        repo.set_lock_store_time(Some(StoreNow::at(backend.now)));

        let dry = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            true,
            4,
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(
            (
                dry.packs_orphan,
                dry.packs_orphan_young,
                dry.packs_deleted_empty
            ),
            (3, 2, 1)
        );
        assert!(keys.iter().all(|k| backend.exists(k).unwrap()));

        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(
            (
                stats.packs_orphan,
                stats.packs_orphan_young,
                stats.packs_deleted_empty
            ),
            (3, 2, 1)
        );
        assert!(backend.exists(&keys[0]).unwrap());
        assert!(backend.exists(&keys[1]).unwrap());
        assert!(!backend.exists(&keys[2]).unwrap());
        assert_eq!(
            backend.tree_lists.load(Ordering::Relaxed),
            if tree { 2 } else { 0 },
            "one tree listing per compact run"
        );
    }

    /// No mtimes (and no reading of "now"): every orphan is kept.
    #[test]
    fn orphans_of_unknown_age_are_kept() {
        init_test_environment();
        let mut repo = Repo::init(
            Box::new(MemoryBackend::new()),
            EncryptionMode::Aes256Gcm,
            ChunkerParams::default(),
            Some(crate::testing::TEST_PASSPHRASE),
            None,
            None,
        )
        .unwrap();
        let keys = three_orphans(&mut repo);
        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(
            (
                stats.packs_orphan,
                stats.packs_orphan_young,
                stats.packs_orphan_unknown_age
            ),
            (3, 0, 3)
        );
        assert_eq!(stats.packs_deleted_empty, 0);
        assert!(keys.iter().all(|k| repo.storage().exists(k).unwrap()));
    }
}

/// A repository on a store with mtimes, and the store.
fn repo_with_mtimes() -> (
    crate::repo::Repo,
    crate::repo::WriteSession,
    std::sync::Arc<crate::testutil::MemoryBackend>,
) {
    crate::testutil::init_test_environment();
    let mem = std::sync::Arc::new(crate::testutil::MemoryBackend::with_mtimes());
    let storage: std::sync::Arc<dyn vykar_storage::StorageBackend> = mem.clone();
    let repo = crate::repo::Repo::init(
        Box::new(storage),
        crate::repo::EncryptionMode::Aes256Gcm,
        crate::repo::ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap();
    let ws = repo.begin_write_session(&crate::repo::WriteSessionOptions::default());
    (repo, ws, mem)
}

/// 04 "Compact" 3: an orphan pack goes only before every live session's
/// horizon (its marker's mtime − elapsed − M), at zero grace too. An hour
/// old orphan is inside the horizon of a session that has run for 2 h, and
/// outside that of one that just started.
#[test]
fn orphans_inside_a_live_horizon_are_kept() {
    use crate::repo::lock::{LiveSession, SessionEntry};
    let now = std::time::SystemTime::now();
    for (elapsed_hours, kept) in [(2u64, true), (0, false)] {
        let (mut repo, mut ws, mem) = repo_with_mtimes();
        let (id, _, _) = ws
            .store_chunk(
                &repo,
                b"an orphan of a live backup",
                Compression::None,
                PackType::Data,
            )
            .unwrap();
        repo.unchecked_writer().save_session(ws).unwrap();
        let pack = repo.chunk_index().get(&id).unwrap().pack_id;
        repo.unchecked_writer().chunk_index_mut().remove(&id);
        repo.unchecked_writer().save_state().unwrap();
        repo.set_pack_grace(std::time::Duration::ZERO);
        mem.set_mtime(
            &pack.storage_key(),
            now - std::time::Duration::from_secs(3600),
        );
        let stamp = chrono::Utc::now().to_rfc3339();
        repo.set_live_sessions(Some(vec![LiveSession {
            session_id: "live".into(),
            entry: Some(SessionEntry {
                hostname: "elsewhere".into(),
                pid: 1,
                registered_at: stamp.clone(),
                last_refresh: stamp,
                boot_id: None,
                installation_id: None,
                pid_ns: None,
                elapsed_secs: Some(elapsed_hours * 3600),
                committing: false,
                writes_heads: false,
            }),
            mtime: Some(now),
        }]));
        let stats = compact_repo_without_mark(
            &mut repo.unchecked_writer(),
            1.0,
            None,
            false,
            4,
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(stats.packs_orphan, 1);
        if kept {
            assert_eq!(stats.packs_orphan_live, 1);
            assert_eq!(stats.packs_deleted_empty, 0);
            assert!(repo.storage().exists(&pack.storage_key()).unwrap());
        } else {
            assert_eq!(stats.packs_orphan_live, 0);
            assert_eq!(stats.packs_deleted_empty, 1);
            assert!(!repo.storage().exists(&pack.storage_key()).unwrap());
        }
    }
}

/// 04 "Compact" 1: compact lists the packs first, then the sessions, then
/// reads the head, then lists the snapshots (the mark).
#[test]
fn compact_lists_packs_before_sessions_and_head() {
    use crate::sim::Sim;
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &crate::sim::SourceTree::seeded("t0", 2, 16 * 1024))
        .expect_ok();
    let r = sim.compact(c);
    r.expect_ok();
    let recs = crate::tests::commit_op_order::run_records(&sim, &r, false);
    let lock = recs
        .iter()
        .position(|r| r.op.is_write() && r.key.starts_with("locks/"))
        .expect("the maintenance lock");
    let first = |prefix: &str| {
        recs.iter()
            .enumerate()
            .skip(lock + 1)
            .find(|(_, r)| r.key.starts_with(prefix))
            .map(|(i, _)| i)
            .unwrap_or_else(|| panic!("no {prefix} op under the lock"))
    };
    let order = ["packs/", "sessions/", "index.h/", "snapshots/"].map(first);
    assert!(
        order.windows(2).all(|w| w[0] < w[1]),
        "first ops under the lock at {order:?} (packs/, sessions/, index.h/, snapshots/)"
    );
}

/// The pack listing precedes the mark: a pack PUT during the mark is not
/// analysed by this run, however old its mtime, while an old orphan listed
/// before goes.
#[test]
fn a_pack_uploaded_during_the_mark_is_not_analysed() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
    use vykar_types::pack_id::PackId;

    /// Puts `late` (with an old mtime) at the first `snapshots/` listing
    /// once armed.
    struct PutDuringMark {
        inner: Arc<crate::testutil::MemoryBackend>,
        armed: AtomicBool,
        late: String,
        old: std::time::SystemTime,
    }

    impl InnerBackend for PutDuringMark {
        fn inner_backend(&self) -> &dyn StorageBackend {
            self.inner.as_ref()
        }
    }

    delegate_storage_backend! {
        for PutDuringMark;
        except [list, list_sized, list_meta, server_ops];

        fn list(&self, prefix: &str) -> vykar_types::error::Result<Vec<String>> {
            self.during_mark(prefix)?;
            self.inner.list(prefix)
        }

        fn list_sized(
            &self,
            prefix: &str,
        ) -> vykar_types::error::Result<Vec<vykar_storage::ListedObject>> {
            self.during_mark(prefix)?;
            self.inner.list_sized(prefix)
        }

        fn list_meta(
            &self,
            prefix: &str,
        ) -> vykar_types::error::Result<Vec<vykar_storage::ListedObject>> {
            self.during_mark(prefix)?;
            self.inner.list_meta(prefix)
        }

        fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
            None
        }
    }

    impl PutDuringMark {
        fn during_mark(&self, prefix: &str) -> vykar_types::error::Result<()> {
            if prefix.starts_with("snapshots/") && self.armed.swap(false, Ordering::SeqCst) {
                self.inner
                    .put(&self.late, b"a pack uploaded during the mark")?;
                self.inner.set_mtime(&self.late, self.old);
            }
            Ok(())
        }
    }

    crate::testutil::init_test_environment();
    let mem = Arc::new(crate::testutil::MemoryBackend::with_mtimes());
    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(13 * 3600);
    let early = PackId::from_bytes([0x11; 32]).storage_key();
    let late = PackId::from_bytes([0x22; 32]).storage_key();
    let wrapped = Arc::new(PutDuringMark {
        inner: Arc::clone(&mem),
        armed: AtomicBool::new(false),
        late: late.clone(),
        old,
    });
    let storage: Arc<dyn StorageBackend> = wrapped.clone();
    let mut repo = crate::repo::Repo::init(
        Box::new(storage),
        crate::repo::EncryptionMode::Aes256Gcm,
        crate::repo::ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap();
    mem.put(&early, b"an orphan pack listed before the mark")
        .unwrap();
    mem.set_mtime(&early, old);
    wrapped.armed.store(true, Ordering::SeqCst);

    let stats =
        crate::commands::compact::run_on_repo(&mut repo, 0.1, None, false, &CancelToken::new())
            .unwrap();
    assert!(
        !wrapped.armed.load(Ordering::SeqCst),
        "the mark listed snapshots"
    );
    assert_eq!(stats.packs_orphan, 1, "{stats:?}");
    assert_eq!(stats.packs_deleted_empty, 1, "{stats:?}");
    assert!(!mem.exists(&early).unwrap(), "the listed old orphan goes");
    assert!(
        mem.exists(&late).unwrap(),
        "the pack PUT during the mark stays"
    );
}
