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
use crate::repo::{EncryptionMode, OpenOptions, Repository, WriteSessionOptions};
use crate::testutil::init_test_environment;

#[derive(Debug, Clone)]
enum Op {
    /// A write session storing `payload(b)` for each b, then save_state.
    Store(Vec<u8>),
    Decrement(usize),
    /// Relocate to the synthetic pack `[p; 32]`, with this stored size.
    Relocate(usize, u8, u32),
    /// rebuild_refcounts: chunk k (sorted) gets 1 + (seed' >> 1) % 4, or is
    /// dropped if seed' is odd, where seed' is the seed rotated by k.
    Rebuild(u64),
    /// request_index_fold, then bump chunk i.
    FoldThenBump(usize),
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => prop::collection::vec(0..12u8, 1..6).prop_map(Op::Store),
        2 => any::<usize>().prop_map(Op::Decrement),
        1 => (any::<usize>(), any::<u8>(), 1..1000u32).prop_map(|(i, p, s)| Op::Relocate(i, p, s)),
        1 => any::<u64>().prop_map(Op::Rebuild),
        1 => any::<usize>().prop_map(Op::FoldThenBump),
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
    repo: Repository,
    repo_dir: PathBuf,
    // Last, so the repository's mapped caches are dropped first.
    _dir: tempfile::TempDir,
}

fn open(repo_dir: &Path, cache: &Path, opts: OpenOptions) -> Repository {
    let storage = LocalBackend::new(repo_dir.to_str().unwrap()).unwrap();
    Repository::open(Box::new(storage), None, Some(cache.to_path_buf()), opts).unwrap()
}

/// A v6 repository with the hard fold limit at `hard_segments` segments.
fn fixture(hard_segments: usize) -> Fixture {
    init_test_environment();
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let scratch = dir.path().join("scratch");
    fs::create_dir_all(&repo_dir).unwrap();
    fs::create_dir_all(&scratch).unwrap();
    Repository::init(
        Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap()),
        EncryptionMode::None,
        ChunkerParams::default(),
        None,
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
fn store(repo: &mut Repository, bs: &[u8]) -> Vec<ChunkId> {
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    let ids = bs
        .iter()
        .map(|&b| {
            repo.store_chunk(&payload(b), Compression::None, PackType::Data)
                .unwrap()
                .0
        })
        .collect();
    repo.save_state().unwrap();
    ids
}

/// Apply a non-session op to `repo`. Returns false when it had nothing to pick.
fn apply_maintenance(repo: &mut Repository, op: &Op) -> bool {
    let ids = sorted_ids(repo.chunk_index());
    if ids.is_empty() {
        return false;
    }
    match *op {
        Op::Store(..) => unreachable!(),
        Op::Decrement(i) => {
            repo.chunk_index_mut().decrement(&ids[i % ids.len()]);
        }
        Op::Relocate(i, p, size) => {
            let moved = repo.chunk_index_mut().update_location(
                &ids[i % ids.len()],
                PackId::from_bytes([p; 32]),
                u64::from(size) * 7,
                size,
            );
            assert!(moved);
        }
        Op::Rebuild(seed) => {
            let refcounts: HashMap<ChunkId, u32> = ids
                .iter()
                .enumerate()
                .filter_map(|(k, id)| {
                    let bits = seed.rotate_right((k % 64) as u32);
                    (bits & 1 == 0).then(|| (*id, 1 + ((bits >> 1) % 4) as u32))
                })
                .collect();
            repo.chunk_index_mut().rebuild_refcounts(&refcounts);
        }
        Op::FoldThenBump(i) => {
            repo.request_index_fold();
            repo.chunk_index_mut()
                .increment_refcount(&ids[i % ids.len()]);
        }
    }
    repo.save_state().unwrap();
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
            match op {
                Op::Store(bs) => {
                    let ids = store(&mut t.repo, bs);
                    for (id, &byte) in ids.iter().zip(bs) {
                        payloads.insert(*id, byte);
                    }
                }
                _ => {
                    let applied = apply_maintenance(&mut t.repo, op);
                    if let (Op::Relocate(i, ..), true) = (op, applied) {
                        let ids = sorted_ids(t.repo.chunk_index());
                        // The pick was made before the op; relocation keeps the ID set.
                        synthetic.insert(ids[i % ids.len()]);
                    }
                }
            }
            synthetic.retain(|id| t.repo.chunk_index().contains(id));

            let cold = cold(&t.repo_dir, &t.scratch);
            prop_assert_eq!(sorted_entries(&cold), sorted_entries(t.repo.chunk_index()));
            prop_assert_eq!(
                t.repo.index_head().unwrap().entries,
                t.repo.chunk_index().len() as u64
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
