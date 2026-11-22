//! `ChunkTable` files and `LiveIndex` against the `ChunkIndex` oracle.

use std::collections::HashMap;
use std::path::Path;

use proptest::prelude::*;

use super::*;
use crate::index::dedup_cache::{FullCacheWriter, chunk_index_from_full_cache};
use crate::index::segment::diff_to_temp;
use crate::testutil::EntryIter;

fn id(n: u8) -> ChunkId {
    let mut b = [0u8; 32];
    b[0] = n.wrapping_mul(37);
    b[1] = n;
    ChunkId::from_bytes(b)
}

fn pack(n: u8) -> PackId {
    PackId::from_bytes([n; 32])
}

fn record(n: u8, refcount: u32) -> FullCacheEntry {
    FullCacheEntry {
        chunk_id: id(n),
        refcount,
        stored_size: 100 + u32::from(n),
        pack_id: pack(n % 3 + 1),
        pack_offset: u64::from(n) * 10,
    }
}

/// `records` (any order) as a full cache in a temp file beside `dir/t`.
fn temp_of(dir: &Path, records: &[FullCacheEntry]) -> TempPath {
    let mut sorted = records.to_vec();
    sorted.sort_by(|a, b| a.chunk_id.as_bytes().cmp(b.chunk_id.as_bytes()));
    let mut w = FullCacheWriter::new_beside(&table_anchor(dir)).unwrap();
    for r in &sorted {
        w.push(r).unwrap();
    }
    w.finish(None).unwrap().0.into_temp_path()
}

fn scratch_of(dir: &Path, records: &[FullCacheEntry]) -> Arc<ChunkTable> {
    Arc::new(ChunkTable::scratch(temp_of(dir, records)).unwrap())
}

fn files_in(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

#[test]
fn install_publishes_under_the_state_name() {
    let dir = tempfile::tempdir().unwrap();
    let records: Vec<_> = (0..20).map(|n| record(n, 1)).collect();
    let table = ChunkTable::install(temp_of(dir.path(), &records), Some(dir.path())).unwrap();
    let (state, entries) = table.state();
    assert_eq!(entries, 20);
    let path = table_path(dir.path(), &state);
    assert_eq!(table.file_path(), Some(path.as_path()));
    assert_eq!(
        files_in(dir.path()),
        vec![format!("chunk_table.{}", hex::encode(state))]
    );
    assert_eq!(list_tables(dir.path()), vec![(state, path.clone())]);

    let opened = ChunkTable::open_file(&path).unwrap();
    assert_eq!(opened.state(), table.state());
    assert_eq!(opened.entry_vec(), table.entry_vec());
    drop((opened, table));

    // A byte flip anywhere makes the file fail verification.
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[40] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    assert!(ChunkTable::open_file(&path).is_none());

    // Without a dir the table stays a temp file, deleted with it.
    let other = tempfile::tempdir().unwrap();
    let scratch = ChunkTable::install(temp_of(other.path(), &records), None).unwrap();
    assert!(scratch.file_path().is_none());
    assert_eq!(files_in(other.path()).len(), 1);
    drop(scratch);
    assert!(files_in(other.path()).is_empty());
}

/// A second install of a state adopts the published file: it is neither
/// replaced nor copied, and the second temp file is gone.
#[test]
fn install_of_a_published_state_adopts_the_existing_table() {
    let dir = tempfile::tempdir().unwrap();
    let records: Vec<_> = (0..20).map(|n| record(n, 2)).collect();
    let first = ChunkTable::install(temp_of(dir.path(), &records), Some(dir.path())).unwrap();
    let path = first.file_path().unwrap().to_path_buf();
    #[cfg(unix)]
    let inode = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&path).unwrap());

    let second = ChunkTable::install(temp_of(dir.path(), &records), Some(dir.path())).unwrap();
    assert_eq!(second.file_path(), Some(path.as_path()));
    assert_eq!(second.entry_vec(), first.entry_vec());
    assert_eq!(files_in(dir.path()).len(), 1, "no second copy");
    #[cfg(unix)]
    assert_eq!(
        std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&path).unwrap()),
        inode,
        "the published file was not replaced"
    );
}

/// Garbage under a state's name is replaced by a valid table of that state.
#[test]
fn install_replaces_a_damaged_table_under_the_state_name() {
    let dir = tempfile::tempdir().unwrap();
    let records: Vec<_> = (0..20).map(|n| record(n, 1)).collect();
    let state = scratch_of(dir.path(), &records).state().0;
    let path = table_path(dir.path(), &state);
    std::fs::write(&path, b"not a table").unwrap();

    let table = ChunkTable::install(temp_of(dir.path(), &records), Some(dir.path())).unwrap();
    assert_eq!(table.file_path(), Some(path.as_path()));
    drop(table);
    assert!(ChunkTable::open_file(&path).is_some());
    assert_eq!(files_in(dir.path()).len(), 1);
}

#[test]
fn prune_keeps_current_mapped_and_young_tables() {
    let dir = tempfile::tempdir().unwrap();
    let install = |lo: u8| {
        let records: Vec<_> = (lo..lo + 5).map(|n| record(n, 1)).collect();
        ChunkTable::install(temp_of(dir.path(), &records), Some(dir.path())).unwrap()
    };
    let age = |t: &ChunkTable, secs: u64| {
        std::fs::File::options()
            .write(true)
            .open(t.file_path().unwrap())
            .unwrap()
            .set_modified(std::time::SystemTime::now() - Duration::from_secs(secs))
            .unwrap();
    };
    let keep = install(0);
    let mapped = install(10);
    let young = install(20);
    let old = install(30);
    age(&keep, 7200);
    age(&mapped, 7200);
    age(&old, 7200);
    let (keep_state, mapped_state, young_state, old_path) = (
        keep.state().0,
        mapped.state().0,
        young.state().0,
        old.file_path().unwrap().to_path_buf(),
    );
    drop((keep, young, old));

    assert_eq!(prune_tables(dir.path(), &keep_state, TABLE_KEEP_AGE), 1);
    assert!(!old_path.exists());
    let mut want = vec![keep_state, mapped_state, young_state];
    want.sort_unstable();
    let mut got: Vec<_> = list_tables(dir.path())
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    got.sort_unstable();
    assert_eq!(got, want);

    // Once unmapped and old, a table of another state goes.
    drop(mapped);
    assert_eq!(prune_tables(dir.path(), &keep_state, TABLE_KEEP_AGE), 1);
    assert_eq!(
        prune_tables(dir.path(), &keep_state, Duration::ZERO),
        1,
        "the young one"
    );
    assert_eq!(
        list_tables(dir.path())
            .into_iter()
            .map(|(s, _)| s)
            .collect::<Vec<_>>(),
        vec![keep_state]
    );
}

#[test]
fn prefilter_present_positions_are_table_positions() {
    let dir = tempfile::tempdir().unwrap();
    let records: Vec<_> = (0..40).map(|n| record(n, 1)).collect();
    let table = scratch_of(dir.path(), &records);
    let prefilter = DedupPrefilter::new(&table).unwrap();
    assert!(Arc::ptr_eq(prefilter.table(), &table));
    for n in 0..40 {
        let PrefilterVerdict::Present {
            stored_size,
            position,
        } = prefilter.lookup(&id(n))
        else {
            panic!("chunk {n} absent");
        };
        assert_eq!(table.chunk_id_at(position), id(n));
        assert_eq!(stored_size, 100 + u32::from(n));
    }
    for n in 40..80 {
        assert_eq!(prefilter.lookup(&id(n)), PrefilterVerdict::Absent);
    }
    // An empty table has no filter, so no prefilter.
    assert!(DedupPrefilter::new(&scratch_of(dir.path(), &[])).is_none());
}

/// A table entry stored at refcount 0 stays at 0 (it is not loaded as 1),
/// and an unedited index diffs to an empty segment over it.
#[test]
fn live_index_keeps_refcount_zero_entries() {
    let dir = tempfile::tempdir().unwrap();
    let records = vec![record(1, 0), record(2, 3)];
    let table = scratch_of(dir.path(), &records);
    let live = LiveIndex::from_table(Arc::clone(&table));
    assert_eq!(live.get(&id(1)).unwrap().refcount, 0);
    assert_eq!(live.len(), 2);

    let (diff, _) = diff_live_to_temp(table.cache(), &live, &table_anchor(dir.path())).unwrap();
    assert!(diff.segment.is_empty());
    assert_eq!(diff.state_hash_after, table.state().0);

    // The HashMap load turns it into refcount 1, a +1 delta.
    let loaded = chunk_index_from_full_cache(table.cache());
    let (diff, _) = diff_to_temp(table.cache(), &loaded, &table_anchor(dir.path())).unwrap();
    assert_eq!(diff.segment.deltas.len(), 1);
}

#[test]
fn diff_live_to_temp_refuses_a_table_at_another_state() {
    let dir = tempfile::tempdir().unwrap();
    let a = scratch_of(dir.path(), &[record(1, 1)]);
    let b = scratch_of(dir.path(), &[record(2, 1)]);
    let live = LiveIndex::from_table(a);
    let err = diff_live_to_temp(b.cache(), &live, &table_anchor(dir.path())).unwrap_err();
    assert!(matches!(err, VykarError::InvalidFormat(_)), "{err}");

    // An unloaded index pairs only with the empty state.
    let mut unloaded = LiveIndex::empty();
    unloaded.add(id(3), 10, pack(1), 0);
    assert!(diff_live_to_temp(b.cache(), &unloaded, &table_anchor(dir.path())).is_err());
    let empty = scratch_of(dir.path(), &[]);
    let (diff, _) = diff_live_to_temp(empty.cache(), &unloaded, &table_anchor(dir.path())).unwrap();
    assert_eq!(diff.segment.new_entries.len(), 1);
}

#[derive(Debug, Clone)]
enum Op {
    Add(u8, u8),
    Increment(u8),
    IncrementBy(u8, u32),
    Decrement(u8),
    UpdateLocation(u8, u8),
    Remove(u8),
    RemoveByPack(u8),
    /// Keep the chunks whose id byte is selected by the mask, at the given count.
    Rebuild(u64, u32),
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0..80u8, 1..4u8).prop_map(|(n, p)| Op::Add(n, p)),
        2 => (0..80u8).prop_map(Op::Increment),
        1 => (0..80u8, 0..5u32).prop_map(|(n, k)| Op::IncrementBy(n, k)),
        4 => (0..80u8).prop_map(Op::Decrement),
        2 => (0..80u8, 1..5u8).prop_map(|(n, p)| Op::UpdateLocation(n, p)),
        1 => (0..80u8).prop_map(Op::Remove),
        1 => (1..5u8).prop_map(Op::RemoveByPack),
        1 => (any::<u64>(), 0..4u32).prop_map(|(m, k)| Op::Rebuild(m, k)),
    ]
}

fn apply_live(index: &mut LiveIndex, op: &Op) -> String {
    match *op {
        Op::Add(n, p) => {
            index.add(id(n), 7 + u32::from(p), pack(p), u64::from(p) * 3);
            String::new()
        }
        Op::Increment(n) => {
            index.increment_refcount(&id(n));
            String::new()
        }
        Op::IncrementBy(n, k) => {
            index.increment_refcount_by(&id(n), k);
            String::new()
        }
        Op::Decrement(n) => format!("{:?}", index.decrement(&id(n))),
        Op::UpdateLocation(n, p) => {
            format!("{}", index.update_location(&id(n), pack(p), 99, 5))
        }
        Op::Remove(n) => format!("{:?}", index.remove(&id(n))),
        Op::RemoveByPack(p) => format!("{}", index.remove_by_pack(&pack(p))),
        Op::Rebuild(mask, k) => {
            index.rebuild_refcounts(&rebuild_map(mask, k));
            String::new()
        }
    }
}

fn apply_oracle(index: &mut ChunkIndex, op: &Op) -> String {
    match *op {
        Op::Add(n, p) => {
            index.add(id(n), 7 + u32::from(p), pack(p), u64::from(p) * 3);
            String::new()
        }
        Op::Increment(n) => {
            index.increment_refcount(&id(n));
            String::new()
        }
        Op::IncrementBy(n, k) => {
            index.increment_refcount_by(&id(n), k);
            String::new()
        }
        Op::Decrement(n) => format!("{:?}", index.decrement(&id(n))),
        Op::UpdateLocation(n, p) => {
            format!("{}", index.update_location(&id(n), pack(p), 99, 5))
        }
        Op::Remove(n) => format!("{:?}", index.remove(&id(n))),
        Op::RemoveByPack(p) => format!("{}", index.remove_by_pack(&pack(p))),
        Op::Rebuild(mask, k) => {
            index.rebuild_refcounts(&rebuild_map(mask, k));
            String::new()
        }
    }
}

fn rebuild_map(mask: u64, k: u32) -> HashMap<ChunkId, u32> {
    (0..80u8)
        .filter(|n| mask & (1 << (n % 64)) != 0)
        .map(|n| (id(n), k + u32::from(n % 2)))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// `LiveIndex` over a table behaves as a `ChunkIndex` loaded from it,
    /// for every read after every edit, and diffs to the same segment and
    /// the same bytes as `diff_to_temp`.
    #[test]
    fn live_index_matches_chunk_index(
        base in prop::collection::btree_map(0..80u8, 1..4u32, 0..64),
        ops in prop::collection::vec(arb_op(), 0..60),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let records: Vec<_> = base.iter().map(|(&n, &rc)| record(n, rc)).collect();
        let table = scratch_of(dir.path(), &records);
        let mut live = LiveIndex::from_table(Arc::clone(&table));
        let mut oracle = chunk_index_from_full_cache(table.cache());
        for op in &ops {
            prop_assert_eq!(apply_live(&mut live, op), apply_oracle(&mut oracle, op), "{:?}", op);
            prop_assert_eq!(live.len(), oracle.len());
            for n in 0..80u8 {
                prop_assert_eq!(live.contains(&id(n)), oracle.contains(&id(n)));
                prop_assert_eq!(live.get(&id(n)), oracle.get(&id(n)).copied());
            }
            prop_assert_eq!(live.entry_vec(), oracle.entry_vec());
            prop_assert_eq!(live.count_distinct_packs(), oracle.count_distinct_packs());
        }

        let anchor = table_anchor(dir.path());
        let (a, ta) = diff_live_to_temp(table.cache(), &live, &anchor).unwrap();
        let (b, tb) = diff_to_temp(table.cache(), &oracle, &anchor).unwrap();
        prop_assert_eq!(&a.segment, &b.segment);
        prop_assert_eq!(a.state_hash_after, b.state_hash_after);
        prop_assert_eq!(a.entries_after, b.entries_after);
        prop_assert_eq!(std::fs::read(&ta).unwrap(), std::fs::read(&tb).unwrap());
    }
}
