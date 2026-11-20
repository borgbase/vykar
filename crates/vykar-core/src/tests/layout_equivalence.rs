//! The monolithic (v3) and segmented (v5) index layouts hold the same index
//! after the same random sequence of writes, warm and cold.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use vykar_storage::local_backend::LocalBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;

use crate::commands::util::with_maintenance_lock;
use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::index::{ChunkIndex, ChunkIndexEntry};
use crate::repo::index_write::FoldPolicy;
use crate::repo::pack::PackType;
use crate::repo::{EncryptionMode, OpenOptions, RepoFormat, Repository, WriteSessionOptions};
use crate::testutil::init_test_environment;

#[derive(Debug, Clone)]
enum Op {
    /// A dedup-mode write session storing `payload(b)` for each b, then
    /// save_state; tiered dedup mode when the flag is set.
    Store(Vec<u8>, bool),
    Decrement(usize),
    /// Relocate to the synthetic pack `[p; 32]`, with this stored size.
    Relocate(usize, u8, u32),
    /// rebuild_refcounts: chunk k (sorted) gets 1 + (seed' >> 1) % 4, or is
    /// dropped if seed' is odd, where seed' is the seed rotated by k.
    Rebuild(u64),
    /// v5: request_index_fold, then bump chunk i; v3: bump chunk i.
    FoldThenBump(usize),
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (prop::collection::vec(0..12u8, 1..6), any::<bool>())
            .prop_map(|(bs, tiered)| Op::Store(bs, tiered)),
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

struct Twin {
    scratch: PathBuf,
    v3: Repository,
    v5: Repository,
    v3_dir: PathBuf,
    v5_dir: PathBuf,
    // Last, so the repositories' mapped caches are dropped first.
    _dir: tempfile::TempDir,
}

fn open(repo_dir: &Path, cache: &Path, opts: OpenOptions) -> Repository {
    let storage = LocalBackend::new(repo_dir.to_str().unwrap()).unwrap();
    Repository::open(Box::new(storage), None, Some(cache.to_path_buf()), opts).unwrap()
}

/// Recursive copy, including empty directories.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            fs::copy(entry.path(), dest).unwrap();
        }
    }
}

/// A v3 repository and its upgraded copy (same repo_id, separate cache dirs).
fn twin(hard_segments: usize) -> Twin {
    init_test_environment();
    let dir = tempfile::tempdir().unwrap();
    let v3_dir = dir.path().join("v3");
    let v5_dir = dir.path().join("v5");
    let scratch = dir.path().join("scratch");
    fs::create_dir_all(&v3_dir).unwrap();
    fs::create_dir_all(&scratch).unwrap();
    Repository::init_at_format(
        Box::new(LocalBackend::new(v3_dir.to_str().unwrap()).unwrap()),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        Some(dir.path().join("cache-v3")),
        RepoFormat::V3,
    )
    .unwrap();
    copy_dir(&v3_dir, &v5_dir);

    let mut up = open(&v5_dir, &dir.path().join("cache-v5"), OpenOptions::new());
    with_maintenance_lock(&mut up, |r| r.upgrade_index_layout()).unwrap();
    drop(up);

    let v3 = open(
        &v3_dir,
        &dir.path().join("cache-v3"),
        OpenOptions::new().with_index(),
    );
    let mut v5 = open(
        &v5_dir,
        &dir.path().join("cache-v5"),
        OpenOptions::new().with_index(),
    );
    assert_eq!(v3.config.id, v5.config.id);
    assert_eq!(v5.format(), RepoFormat::V5);
    let mut policy = FoldPolicy::default();
    policy.hard.segments = hard_segments;
    policy.hard.percent = 100_000;
    v5.set_fold_policy(policy);
    Twin {
        _dir: dir,
        scratch,
        v3,
        v5,
        v3_dir,
        v5_dir,
    }
}

/// The index of the repository at `dir`, opened with a fresh cache dir.
fn cold(dir: &Path, scratch: &Path) -> ChunkIndex {
    let cache = tempfile::tempdir_in(scratch).unwrap();
    open(dir, cache.path(), OpenOptions::new().with_index())
        .chunk_index()
        .clone()
}

fn sorted_ids(index: &ChunkIndex) -> Vec<ChunkId> {
    let mut ids: Vec<ChunkId> = index.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable_by_key(|id| *id.as_bytes());
    ids
}

fn sorted_entries(index: &ChunkIndex) -> Vec<(ChunkId, ChunkIndexEntry)> {
    let mut v: Vec<_> = index.iter().map(|(id, e)| (*id, *e)).collect();
    v.sort_unstable_by_key(|(id, _)| *id.as_bytes());
    v
}

/// Same chunk IDs, refcounts, sizes, offsets; pack IDs related by a bijection.
fn assert_equivalent(a: &ChunkIndex, b: &ChunkIndex) -> Result<(), TestCaseError> {
    let (a, b) = (sorted_entries(a), sorted_entries(b));
    prop_assert_eq!(a.len(), b.len());
    let mut forward: HashMap<PackId, PackId> = HashMap::new();
    let mut backward: HashMap<PackId, PackId> = HashMap::new();
    for ((ia, ea), (ib, eb)) in a.iter().zip(&b) {
        prop_assert_eq!(ia, ib);
        prop_assert_eq!(ea.refcount, eb.refcount, "chunk {}", ia);
        prop_assert_eq!(ea.stored_size, eb.stored_size, "chunk {}", ia);
        prop_assert_eq!(ea.pack_offset, eb.pack_offset, "chunk {}", ia);
        let f = *forward.entry(ea.pack_id).or_insert(eb.pack_id);
        let r = *backward.entry(eb.pack_id).or_insert(ea.pack_id);
        prop_assert_eq!(f, eb.pack_id, "pack of chunk {}", ia);
        prop_assert_eq!(r, ea.pack_id, "pack of chunk {}", ia);
    }
    Ok(())
}

/// Store each `payload(b)` in one dedup-mode write session; returns the IDs.
fn store(repo: &mut Repository, bs: &[u8], tiered: bool) -> Vec<ChunkId> {
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    if tiered {
        repo.enable_tiered_dedup_mode();
    } else {
        repo.enable_dedup_mode();
    }
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
            if repo.format() == RepoFormat::V5 {
                repo.request_index_fold();
            }
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
    fn layouts_agree_on_random_op_sequences(
        hard in 2..6usize,
        ops in prop::collection::vec(arb_op(), 1..12),
    ) {
        let mut t = twin(hard);
        // Chunk ID -> payload byte, and chunks currently at a synthetic pack.
        let mut payloads: HashMap<ChunkId, u8> = HashMap::new();
        let mut synthetic: HashSet<ChunkId> = HashSet::new();

        for op in &ops {
            match op {
                Op::Store(bs, tiered) => {
                    let a = store(&mut t.v3, bs, *tiered);
                    let b = store(&mut t.v5, bs, *tiered);
                    prop_assert_eq!(&a, &b);
                    for (id, &byte) in a.iter().zip(bs) {
                        payloads.insert(*id, byte);
                    }
                }
                _ => {
                    let applied = apply_maintenance(&mut t.v3, op);
                    prop_assert_eq!(apply_maintenance(&mut t.v5, op), applied);
                    if let (Op::Relocate(i, ..), true) = (op, applied) {
                        let ids = sorted_ids(t.v3.chunk_index());
                        // The pick was made before the op; relocation keeps the ID set.
                        synthetic.insert(ids[i % ids.len()]);
                    }
                }
            }
            synthetic.retain(|id| t.v3.chunk_index().contains(id));

            assert_equivalent(t.v3.chunk_index(), t.v5.chunk_index())?;
            let cold3 = cold(&t.v3_dir, &t.scratch);
            let cold5 = cold(&t.v5_dir, &t.scratch);
            assert_equivalent(&cold3, &cold5)?;
            assert_equivalent(&cold5, t.v5.chunk_index())?;
            prop_assert_eq!(
                t.v5.index_head().unwrap().entries,
                t.v3.chunk_index().len() as u64
            );
        }

        for id in sorted_ids(&t.v3.chunk_index().clone()) {
            if synthetic.contains(&id) {
                continue;
            }
            let expected = payload(payloads[&id]);
            prop_assert_eq!(t.v3.read_chunk(&id).unwrap(), expected.clone());
            prop_assert_eq!(t.v5.read_chunk(&id).unwrap(), expected);
        }
    }
}
