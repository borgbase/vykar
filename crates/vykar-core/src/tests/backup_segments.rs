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
use crate::repo::{ChunkerParams, EncryptionMode, OpenOptions, Repository, WriteSessionOptions};
use crate::testutil::MemoryBackend;

use super::helpers::{backup_single_source, make_test_config};

fn init(mem: &Arc<dyn StorageBackend>) -> Repository {
    crate::testutil::init_test_environment();
    Repository::init(
        Box::new(Arc::clone(mem)),
        EncryptionMode::None,
        ChunkerParams::default(),
        None,
        None,
        None,
    )
    .unwrap()
}

/// A handle on `mem` with the index loaded and no cache dir.
fn open(mem: &Arc<dyn StorageBackend>) -> Repository {
    Repository::open(
        Box::new(Arc::clone(mem)),
        None,
        None,
        OpenOptions::new().with_index(),
    )
    .unwrap()
}

/// Commit the active session as a backup snapshot `name`.
fn commit(repo: &mut Repository, name: &str) {
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
        entry,
        b"snapshot".to_vec(),
        &mut file_cache,
        &|_: crate::repo::control::CommitEvent| {},
    )
    .unwrap();
}

fn store(repo: &mut Repository, data: &[u8]) -> ChunkId {
    repo.store_chunk(data, Compression::None, PackType::Data)
        .unwrap()
        .0
}

/// The last segment the latest head names, decoded.
fn last_segment(repo: &Repository) -> SegmentBody {
    let head = index_store::fetch_head(repo.storage.as_ref(), repo.crypto.as_ref()).unwrap();
    let r = head.segments.last().unwrap();
    let bytes = repo
        .storage
        .get(&index_store::object_key(&r.name))
        .unwrap()
        .unwrap();
    index_store::decode_segment_v2(r, &bytes, repo.crypto.as_ref()).unwrap()
}

/// The real trailer frame length of `pack`.
fn frame_len(repo: &Repository, pack: &vykar_types::pack_id::PackId) -> u32 {
    let bytes = repo.storage.get(&pack.storage_key()).unwrap().unwrap();
    4 + crate::repo::pack_trailer::parse_footer(&bytes).unwrap()
}

/// BK-2 and R7: a chunk that exists only in an applied segment (no base) is
/// deduplicated by the next backup, through the prefilter (PresentAdded)
/// and by key on the consumer path, and its reuse recorded by key.
#[test]
fn a_chunk_in_an_applied_segment_is_deduplicated() {
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut repo = init(&mem);
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    let a = store(&mut repo, b"chunk added by the first backup");
    commit(&mut repo, "s1");
    drop(repo);

    let mut repo = open(&mem);
    let head = repo.index_head().unwrap().clone();
    assert!(head.base.is_none() && head.segments.len() == 1, "{head:?}");
    let packs_before = mem.list("packs/").unwrap().len();
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    let prefilter = repo
        .dedup_prefilter()
        .expect("a prefilter over a segments-only index");
    let size = repo.chunk_index().get(&a).unwrap().stored_size;
    assert_eq!(
        prefilter.lookup(&a),
        PrefilterVerdict::PresentAdded { stored_size: size }
    );
    assert_eq!(
        repo.store_chunk(
            b"chunk added by the first backup",
            Compression::None,
            PackType::Data
        )
        .unwrap(),
        (a, size, false)
    );
    assert!(repo.delta_reuses(&a));
    commit(&mut repo, "s2");
    assert_eq!(
        mem.list("packs/").unwrap().len(),
        packs_before,
        "no new pack"
    );
    assert_eq!(
        repo.index_head().unwrap(),
        &head,
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
    crate::commands::init::run(&config, None).unwrap();
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
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    store(&mut repo, b"an older chunk");
    commit(&mut repo, "s0");
    drop(repo);

    let mut a = open(&mem);
    let mut b = open(&mem);
    a.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    b.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    let shared = store(&mut a, b"a chunk both sessions store");
    a.flush_packs().unwrap();
    let only_a = store(&mut a, b"a chunk only the first stores");
    assert_eq!(store(&mut b, b"a chunk both sessions store"), shared);
    commit(&mut b, "b");
    let b_seg = last_segment(&b);
    assert_eq!(
        b_seg
            .additions
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
        vec![shared]
    );

    commit(&mut a, "a");
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
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut repo = init(&mem);
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    store(&mut repo, b"committed first");
    commit(&mut repo, "s0");
    drop(repo);

    let mut interrupted = open(&mem);
    interrupted
        .begin_write_session(&WriteSessionOptions {
            session_id: "interrupted".into(),
            ..WriteSessionOptions::default()
        })
        .unwrap();
    store(&mut interrupted, b"stored by the interrupted session");
    interrupted.flush_packs().unwrap();
    interrupted.flush_on_abort();
    drop(interrupted);
    assert!(
        mem.list("sessions/")
            .unwrap()
            .iter()
            .any(|k| k.ends_with(".index")),
        "the interrupted session left its journal"
    );

    let mut repo = open(&mem);
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    let recovered = repo.recover_pending_index().unwrap();
    assert_eq!(recovered.recovered_chunks, 1);
    let new = store(&mut repo, b"a chunk of this session");
    commit(&mut repo, "s1");
    let seg = last_segment(&repo);
    assert_eq!(
        seg.additions.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![new]
    );
    assert_eq!(seg.pack_trailers.len(), 1);
}
