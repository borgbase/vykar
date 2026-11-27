//! Backup commits by additions (05 step 9): what a backup segment holds,
//! dedup against the head's applied segments, and the trailer lengths of
//! its pack table.

use std::sync::Arc;

use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;

use crate::compress::Compression;
use crate::index::segment_v2::SegmentBody;
use crate::index::table::PrefilterVerdict;
use crate::repo::index_store;
use crate::repo::pack::PackType;
use crate::repo::{
    ChunkerParams, EncryptionMode, OpenOptions, Repo, WriteSession, WriteSessionOptions,
};
use crate::testutil::MemoryBackend;

use super::helpers::{backup_single_source, make_test_config, pass};

fn init(mem: &Arc<dyn StorageBackend>) -> Repo {
    crate::testutil::init_test_environment();
    Repo::init(
        Box::new(Arc::clone(mem)),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap()
}

/// A handle on `mem` with the index loaded and no cache dir.
fn open(mem: &Arc<dyn StorageBackend>) -> Repo {
    Repo::open(
        Box::new(Arc::clone(mem)),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        OpenOptions::new().with_index(),
    )
    .unwrap()
}

/// Commit `ws` as a backup snapshot `name`.
fn commit(repo: &mut Repo, ws: WriteSession, name: &str) {
    let entry = crate::repo::manifest::SnapshotEntry {
        name: name.into(),
        id: vykar_types::snapshot_id::SnapshotId::generate(),
        time: chrono::Utc::now(),
        source_label: String::new(),
        label: String::new(),
        source_paths: Vec::new(),
        hostname: String::new(),
    };
    let mut file_cache = crate::repo::file_cache::FileCache::new();
    repo.commit_session_blocking(
        ws,
        entry,
        b"snapshot".to_vec(),
        &mut file_cache,
        &|_: crate::repo::control::CommitEvent| {},
    )
    .unwrap();
}

fn store(repo: &Repo, ws: &mut WriteSession, data: &[u8]) -> ChunkId {
    ws.store_chunk(repo, data, Compression::None, PackType::Data)
        .unwrap()
        .0
}

/// The last segment the latest head names, decoded.
fn last_segment(repo: &Repo) -> SegmentBody {
    let head = index_store::fetch_head(repo.storage().as_ref(), repo.crypto().as_ref()).unwrap();
    let r = head.segments.last().unwrap();
    let bytes = repo
        .storage()
        .get(&index_store::object_key(&r.name))
        .unwrap()
        .unwrap();
    index_store::decode_segment_v2(r, &bytes, repo.crypto().as_ref()).unwrap()
}

/// The real trailer frame length of `pack`.
fn frame_len(repo: &Repo, pack: &vykar_types::pack_id::PackId) -> u32 {
    let bytes = repo.storage().get(&pack.storage_key()).unwrap().unwrap();
    4 + crate::repo::pack_trailer::parse_footer(&bytes).unwrap()
}

/// BK-2 and R7: a chunk that exists only in an applied segment (no base) is
/// deduplicated by the next backup, through the prefilter (PresentAdded)
/// and by key on the consumer path, and its reuse recorded by key.
#[test]
fn a_chunk_in_an_applied_segment_is_deduplicated() {
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut repo = init(&mem);
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    let a = store(&repo, &mut ws, b"chunk added by the first backup");
    commit(&mut repo, ws, "s1");
    drop(repo);

    let mut repo = open(&mem);
    let head = repo.index_head().unwrap().clone();
    assert!(head.base.is_none() && head.segments.len() == 1, "{head:?}");
    let packs_before = mem.list("packs/").unwrap().len();
    ws = repo.begin_write_session(&WriteSessionOptions::default());
    let prefilter = ws
        .dedup_prefilter()
        .expect("a prefilter over a segments-only index");
    let size = repo.chunk_index().get(&a).unwrap().stored_size;
    assert_eq!(
        prefilter.lookup(&a),
        PrefilterVerdict::PresentAdded { stored_size: size }
    );
    assert_eq!(
        ws.store_chunk(
            &repo,
            b"chunk added by the first backup",
            Compression::None,
            PackType::Data
        )
        .unwrap(),
        (a, size, false)
    );
    assert!(ws.delta_reuses(&a));
    commit(&mut repo, ws, "s2");
    assert_eq!(
        mem.list("packs/").unwrap().len(),
        packs_before,
        "no new pack"
    );
    // Nothing new, so no segment: a head only, the same state at the next
    // generation (Q2, on a single-writer repository too since
    // 10-single-protocol step 7).
    let after = repo.index_head().unwrap();
    assert_eq!(after.generation, head.generation + 1, "{after:?}");
    assert_eq!(after.base, head.base, "nothing new, nothing written");
    assert_eq!(
        after.segments, head.segments,
        "nothing new, nothing written"
    );
}

/// A first backup into an empty repository has a prefilter (over the empty
/// table) and dedups within itself.
#[test]
fn a_first_backup_into_an_empty_repository_dedups_within_itself() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    let data = vec![0x5au8; 256 * 1024];
    std::fs::write(src.join("a.bin"), &data).unwrap();
    std::fs::write(src.join("b.bin"), &data).unwrap();
    let config = make_test_config(&repo_dir);
    crate::commands::init::run(&config, pass(&config)).unwrap();
    let stats = backup_single_source(&config, &src, "src", "s1");
    assert!(
        stats.deduplicated_size <= stats.original_size / 2 + 4096,
        "{stats:?}"
    );
    // The second backup dedups against the segment only (no base yet).
    std::fs::write(src.join("c.bin"), &data).unwrap();
    let stats = backup_single_source(&config, &src, "src", "s2");
    assert!(stats.deduplicated_size < 64 * 1024, "{stats:?}");
}

/// BK-5 and R2: two sessions store the same new chunk; the second to commit
/// leaves it out of its segment. The second session's first pack held only
/// that chunk: its segment's pack table names only the packs of its
/// remaining additions, each with its real trailer length (BK-6).
#[test]
fn the_second_session_omits_chunks_the_first_committed() {
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut repo = init(&mem);
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    store(&repo, &mut ws, b"an older chunk");
    commit(&mut repo, ws, "s0");
    drop(repo);

    let mut a = open(&mem);
    let mut b = open(&mem);
    let mut wa = a.begin_write_session(&WriteSessionOptions::default());
    let mut wb = b.begin_write_session(&WriteSessionOptions::default());
    let shared = store(&a, &mut wa, b"a chunk both sessions store");
    wa.flush_packs(&a).unwrap();
    let only_a = store(&a, &mut wa, b"a chunk only the first stores");
    assert_eq!(store(&b, &mut wb, b"a chunk both sessions store"), shared);
    commit(&mut b, wb, "b");
    let b_seg = last_segment(&b);
    assert_eq!(
        b_seg
            .additions
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
        vec![shared]
    );

    commit(&mut a, wa, "a");
    let a_seg = last_segment(&a);
    assert_eq!(
        a_seg
            .additions
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
        vec![only_a],
        "the shared chunk is b's"
    );
    let pack = a_seg.additions[0].1.pack_id;
    assert_eq!(a_seg.pack_trailers, vec![(pack, frame_len(&a, &pack))]);
    // The shared chunk resolves to b's copy; a's first pack holds dead bytes.
    let cold = open(&mem);
    assert_eq!(
        cold.chunk_index().get(&shared).unwrap().pack_id,
        b_seg.additions[0].1.pack_id
    );
    assert!(cold.chunk_index().contains(&only_a));
}

/// R2: a session that recovers a journal pack and does not promote from it
/// commits; the recovered pack is not in its segment.
#[test]
fn a_recovered_pack_that_is_not_used_stays_out_of_the_segment() {
    // A store with mtimes: recovery adopts journal packs by their age.
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::with_mtimes());
    let mut repo = init(&mem);
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    store(&repo, &mut ws, b"committed first");
    commit(&mut repo, ws, "s0");
    drop(repo);

    let interrupted = open(&mem);
    let mut wi = interrupted.begin_write_session(&WriteSessionOptions {
        session_id: "interrupted".into(),
        ..WriteSessionOptions::default()
    });
    store(&interrupted, &mut wi, b"stored by the interrupted session");
    wi.flush_packs(&interrupted).unwrap();
    wi.flush_on_abort(&interrupted);
    drop(interrupted);
    assert!(
        mem.list("sessions/")
            .unwrap()
            .iter()
            .any(|k| k.ends_with(".index")),
        "the interrupted session left its journal"
    );

    let mut repo = open(&mem);
    ws = repo.begin_write_session(&WriteSessionOptions {
        session_id: "recovering".into(),
        ..WriteSessionOptions::default()
    });
    // The recovering session's own marker gives recovery "now".
    mem.put("sessions/recovering.json", b"{}").unwrap();
    let recovered = ws
        .recover_pending_index(&repo, &crate::repo::control::CancelToken::new())
        .unwrap();
    assert_eq!(recovered.recovered_chunks, 1);
    let new = store(&repo, &mut ws, b"a chunk of this session");
    commit(&mut repo, ws, "s1");
    let seg = last_segment(&repo);
    assert_eq!(
        seg.additions.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![new]
    );
    assert_eq!(seg.pack_trailers.len(), 1);
}
