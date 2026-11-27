//! After a random sequence of writes, the segmented index read cold (a fresh
//! cache dir) equals the index the writer holds, and the head counts it.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::*;
use vykar_storage::local_backend::LocalBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;

use crate::compress::Compression;
use crate::index::{ChunkIndexEntry, LiveIndex};
use crate::repo::ChunkerParams;
use crate::repo::index_write::FoldPolicy;
use crate::repo::pack::PackType;
use crate::repo::{EncryptionMode, OpenOptions, Repo, WriteSessionOptions};
use crate::testutil::init_test_environment;

#[derive(Debug, Clone)]
enum Op {
    /// A write session storing `payload(b)` for each b, then save_state.
    Store(Vec<u8>),
    Remove(usize),
    /// Relocate to the synthetic pack `[p; 32]`, with this stored size.
    Relocate(usize, u8, u32),
    /// A sweep: chunk k (sorted) is dropped if seed' is odd, where seed' is
    /// the seed rotated by k.
    Sweep(u64),
    /// request_index_fold with no edit.
    Fold,
    /// A backup commit storing `payload(b)` for each b: a backup segment.
    Backup(Vec<u8>),
    /// Move the k-th segment of the head (mod its count) past T_pack on the
    /// store's clock, so folds take it (04 step 7).
    Age(usize),
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => prop::collection::vec(0..12u8, 1..6).prop_map(Op::Store),
        2 => any::<usize>().prop_map(Op::Remove),
        1 => (any::<usize>(), any::<u8>(), 1..1000u32).prop_map(|(i, p, s)| Op::Relocate(i, p, s)),
        1 => any::<u64>().prop_map(Op::Sweep),
        1 => Just(Op::Fold),
        2 => prop::collection::vec(0..12u8, 1..6).prop_map(Op::Backup),
        2 => any::<usize>().prop_map(Op::Age),
    ]
}

/// Deterministic chunk data for `b`, distinct per `b`.
fn payload(b: u8) -> Vec<u8> {
    let mut data = vec![b; 16];
    data.extend((0..2048u32).map(|i| (i as u8) ^ (i >> 8) as u8 ^ b.wrapping_mul(31)));
    data
}

struct Fixture {
    scratch: PathBuf,
    repo: Repo,
    repo_dir: PathBuf,
    // Last, so the repository's mapped caches are dropped first.
    _dir: tempfile::TempDir,
}

fn open(repo_dir: &Path, cache: &Path, opts: OpenOptions) -> Repo {
    let storage = LocalBackend::new(repo_dir.to_str().unwrap()).unwrap();
    Repo::open(
        Box::new(storage),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(cache.to_path_buf()),
        opts,
    )
    .unwrap()
}

/// A v6 repository with the hard fold limit at `hard_segments` segments.
fn fixture(hard_segments: usize) -> Fixture {
    init_test_environment();
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let scratch = dir.path().join("scratch");
    fs::create_dir_all(&repo_dir).unwrap();
    fs::create_dir_all(&scratch).unwrap();
    Repo::init(
        Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        Some(dir.path().join("cache")),
    )
    .unwrap();
    let mut repo = open(
        &repo_dir,
        &dir.path().join("cache"),
        OpenOptions::new().with_index(),
    );
    let mut policy = FoldPolicy::default();
    policy.hard.segments = hard_segments;
    policy.hard.percent = 100_000;
    repo.set_fold_policy(policy);
    Fixture {
        _dir: dir,
        scratch,
        repo,
        repo_dir,
    }
}

/// The index of the repository at `dir`, opened with a fresh cache dir.
fn cold(dir: &Path, scratch: &Path) -> LiveIndex {
    let cache = tempfile::tempdir_in(scratch).unwrap();
    open(dir, cache.path(), OpenOptions::new().with_index())
        .chunk_index()
        .clone()
}

fn sorted_ids(index: &LiveIndex) -> Vec<ChunkId> {
    let mut ids: Vec<ChunkId> = index.iter().map(|(id, _)| id).collect();
    ids.sort_unstable_by_key(|id| *id.as_bytes());
    ids
}

fn sorted_entries(index: &LiveIndex) -> Vec<(ChunkId, ChunkIndexEntry)> {
    let mut v: Vec<_> = index.iter().collect();
    v.sort_unstable_by_key(|(id, _)| *id.as_bytes());
    v
}

/// Store each `payload(b)` in one write session; returns the IDs.
fn store(repo: &mut Repo, bs: &[u8]) -> Vec<ChunkId> {
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    let ids = bs
        .iter()
        .map(|&b| {
            ws.store_chunk(repo, &payload(b), Compression::None, PackType::Data)
                .unwrap()
                .0
        })
        .collect();
    repo.unchecked_writer().save_session(ws).unwrap();
    ids
}

/// Store each `payload(b)` in one write session and commit it as a backup
/// (a backup segment and a snapshot); returns the IDs.
fn backup(repo: &mut Repo, bs: &[u8]) -> Vec<ChunkId> {
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    let ids = bs
        .iter()
        .map(|&b| {
            ws.store_chunk(repo, &payload(b), Compression::None, PackType::Data)
                .unwrap()
                .0
        })
        .collect();
    let id = vykar_types::snapshot_id::SnapshotId::generate();
    let entry = crate::repo::manifest::SnapshotEntry {
        name: format!("b-{}", hex::encode(id.as_bytes())),
        id,
        time: chrono::Utc::now(),
        source_label: String::new(),
        label: String::new(),
        source_paths: Vec::new(),
        hostname: String::new(),
    };
    repo.commit_session_blocking(
        ws,
        entry,
        b"snapshot".to_vec(),
        &mut crate::repo::file_cache::FileCache::new(),
        &|_: crate::repo::control::CommitEvent| {},
    )
    .unwrap();
    ids
}

/// Move the mtime of the head's k-th segment (mod the count) back past
/// T_pack.
fn age(repo: &Repo, repo_dir: &Path, k: usize) {
    let segments = &repo.index_head().unwrap().segments;
    if segments.is_empty() {
        return;
    }
    let name = segments[k % segments.len()].name;
    let path = repo_dir
        .join(crate::repo::index_store::INDEX_OBJECT_DIR)
        .join(name.to_hex());
    let mtime = fs::metadata(&path).unwrap().modified().unwrap();
    fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(mtime - super::helpers::PAST_T_PACK)
        .unwrap();
}

/// The IDs `op` removes from an index whose sorted IDs are `ids`.
fn removed_by(op: &Op, ids: &[ChunkId]) -> Vec<ChunkId> {
    if ids.is_empty() {
        return Vec::new();
    }
    match *op {
        Op::Remove(i) => vec![ids[i % ids.len()]],
        Op::Sweep(seed) => ids
            .iter()
            .enumerate()
            .filter(|(k, _)| seed.rotate_right((k % 64) as u32) & 1 != 0)
            .map(|(_, id)| *id)
            .collect(),
        _ => Vec::new(),
    }
}

/// Apply a non-session op to `repo`. Returns false when it had nothing to pick.
fn apply_maintenance(repo: &mut Repo, op: &Op) -> bool {
    let ids = sorted_ids(repo.chunk_index());
    if ids.is_empty() {
        return false;
    }
    match *op {
        Op::Store(..) | Op::Backup(..) | Op::Age(..) => unreachable!(),
        Op::Remove(..) | Op::Sweep(..) => {
            let mut writer = repo.unchecked_writer();
            let index = writer.chunk_index_mut();
            for id in removed_by(op, &ids) {
                index.remove(&id);
            }
        }
        Op::Relocate(i, p, size) => {
            repo.unchecked_writer()
                .chunk_index_mut()
                .set_pack_trailer(PackId::from_bytes([p; 32]), 0);
            let moved = repo.unchecked_writer().chunk_index_mut().update_location(
                &ids[i % ids.len()],
                PackId::from_bytes([p; 32]),
                u64::from(size) * 7,
                size,
            );
            assert!(moved);
        }
        Op::Fold => {
            repo.request_index_fold();
            repo.unchecked_writer().mark_index_dirty();
        }
    }
    repo.unchecked_writer().save_state().unwrap();
    true
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn warm_and_cold_index_agree_on_random_op_sequences(
        hard in 2..6usize,
        ops in prop::collection::vec(arb_op(), 1..12),
    ) {
        let mut t = fixture(hard);
        // Chunk ID -> payload byte, and chunks currently at a synthetic pack.
        let mut payloads: HashMap<ChunkId, u8> = HashMap::new();
        let mut synthetic: HashSet<ChunkId> = HashSet::new();

        for op in &ops {
            // A fresh reading of "now" for every write, as a new session
            // takes one: the ages are listed again.
            t.repo.clear_session_state();
            super::helpers::set_store_now(&mut t.repo);
            // The entries before the op: a fold (or a fold that both the
            // warm and the cold reader build from the same wrong base) must
            // neither lose nor resurrect any.
            let before = sorted_entries(t.repo.chunk_index());
            let before_ids = sorted_ids(t.repo.chunk_index());
            let removed = removed_by(op, &before_ids);
            let mut added: Vec<ChunkId> = Vec::new();
            let mut moved: Option<ChunkId> = None;
            match op {
                Op::Backup(bs) => {
                    added = backup(&mut t.repo, bs);
                    for (id, &byte) in added.iter().zip(bs) {
                        payloads.insert(*id, byte);
                    }
                }
                Op::Age(k) => age(&t.repo, &t.repo_dir, *k),
                Op::Store(bs) => {
                    added = store(&mut t.repo, bs);
                    for (id, &byte) in added.iter().zip(bs) {
                        payloads.insert(*id, byte);
                    }
                }
                _ => {
                    let applied = apply_maintenance(&mut t.repo, op);
                    if let (Op::Relocate(i, ..), true) = (op, applied) {
                        // The pick was made before the op; relocation keeps the ID set.
                        let id = before_ids[i % before_ids.len()];
                        synthetic.insert(id);
                        moved = Some(id);
                    }
                }
            }
            synthetic.retain(|id| t.repo.chunk_index().contains(id));

            // Equal after Fold and Age; plus exactly the new IDs after Store
            // and Backup; minus exactly the removed IDs after Remove and
            // Sweep; one entry changed after Relocate.
            let after = sorted_entries(t.repo.chunk_index());
            let after_map: HashMap<ChunkId, ChunkIndexEntry> = after.iter().cloned().collect();
            let mut want: HashMap<ChunkId, ChunkIndexEntry> = before
                .iter()
                .filter(|(id, _)| !removed.contains(id))
                .cloned()
                .collect();
            for id in &added {
                let entry = after_map.get(id);
                prop_assert!(entry.is_some(), "added {:?} missing", id);
                want.entry(*id).or_insert(*entry.unwrap());
            }
            if let Some(id) = moved {
                let entry = after_map.get(&id);
                prop_assert!(entry.is_some(), "relocated {:?} missing", id);
                want.insert(id, *entry.unwrap());
            }
            let mut want: Vec<(ChunkId, ChunkIndexEntry)> = want.into_iter().collect();
            want.sort_unstable_by_key(|(id, _)| *id.as_bytes());
            prop_assert_eq!(&after, &want, "after {:?}", op);

            let cold = cold(&t.repo_dir, &t.scratch);
            prop_assert_eq!(sorted_entries(&cold), sorted_entries(t.repo.chunk_index()));
            prop_assert_eq!(
                t.repo.chunk_index().state(),
                &crate::index::table::IndexState::of_head(t.repo.index_head().unwrap())
            );
        }

        for id in sorted_ids(&t.repo.chunk_index().clone()) {
            if synthetic.contains(&id) {
                continue;
            }
            prop_assert_eq!(t.repo.read_chunk(&id).unwrap(), payload(payloads[&id]));
        }
    }
}
