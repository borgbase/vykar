//! `ChunkTable` files, the segment cache and `LiveIndex` against the
//! `ChunkIndex` oracle.

use std::collections::HashMap;
use std::path::Path;

use proptest::prelude::*;

use super::*;
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

fn name(n: u8) -> ObjectName {
    ObjectName([n; 32])
}

/// An oracle index of chunks `ns`.
fn oracle_of(ns: impl IntoIterator<Item = u8>) -> ChunkIndex {
    let mut index = ChunkIndex::new();
    for n in ns {
        index.add(
            id(n),
            100 + u32::from(n),
            pack(n % 3 + 1),
            u64::from(n) * 10,
        );
    }
    index
}

/// `index` as a table file in a temp file beside `dir`'s anchor.
fn temp_of(dir: &Path, index: &ChunkIndex) -> TempPath {
    base_body_from_chunk_index(index, &table_anchor(dir))
        .unwrap()
        .0
}

fn scratch_of(dir: &Path, index: &ChunkIndex) -> Arc<ChunkTable> {
    Arc::new(ChunkTable::install(temp_of(dir, index), None, None).unwrap())
}

fn files_in(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

fn loc(p: u8, off: u64, size: u32) -> Location {
    Location {
        pack_id: pack(p),
        pack_offset: off,
        stored_size: size,
    }
}

fn entries_of(index: &impl EntryIter) -> Vec<(ChunkId, ChunkIndexEntry)> {
    index.entry_vec().into_iter().collect()
}

/// TBL-1: install publishes `index_base.<name>`; a second install adopts
/// it; a damaged file under the name is replaced once.
#[test]
fn install_publishes_adopts_and_replaces() {
    let dir = tempfile::tempdir().unwrap();
    let index = oracle_of(0..20);
    let table =
        ChunkTable::install(temp_of(dir.path(), &index), Some(name(1)), Some(dir.path())).unwrap();
    let path = base_table_path(dir.path(), &name(1));
    assert_eq!(table.file_path(), Some(path.as_path()));
    assert_eq!(table.base_name(), Some(name(1)));
    assert_eq!(
        files_in(dir.path()),
        vec![format!("index_base.{}", name(1).to_hex())]
    );
    assert_eq!(list_base_tables(dir.path()), vec![(name(1), path.clone())]);
    assert_eq!(entries_of(&table), entries_of(&index));
    #[cfg(unix)]
    let inode = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&path).unwrap());

    let second =
        ChunkTable::install(temp_of(dir.path(), &index), Some(name(1)), Some(dir.path())).unwrap();
    assert_eq!(second.file_path(), Some(path.as_path()));
    assert_eq!(files_in(dir.path()).len(), 1, "no second copy");
    #[cfg(unix)]
    assert_eq!(
        std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&path).unwrap()),
        inode,
        "the published file was not replaced"
    );
    drop((table, second));

    std::fs::write(&path, b"not a table").unwrap();
    let healed =
        ChunkTable::install(temp_of(dir.path(), &index), Some(name(1)), Some(dir.path())).unwrap();
    assert_eq!(healed.file_path(), Some(path.as_path()));
    drop(healed);
    assert!(ChunkTable::open_file(&path).is_some());
    assert_eq!(files_in(dir.path()).len(), 1);

    // Without a dir (or a name) the table stays a temp file, deleted with it.
    let other = tempfile::tempdir().unwrap();
    let scratch = ChunkTable::install(temp_of(other.path(), &index), Some(name(2)), None).unwrap();
    assert!(scratch.file_path().is_none());
    assert_eq!(files_in(other.path()).len(), 1);
    drop(scratch);
    assert!(files_in(other.path()).is_empty());
}

/// A temp file that fails verification is `InvalidFormat` (damage); one
/// that cannot be opened is a local I/O error, not damage.
#[test]
fn install_tells_damage_from_io_errors() {
    let dir = tempfile::tempdir().unwrap();
    let damaged = temp_of(dir.path(), &oracle_of(0..5));
    std::fs::write(&damaged, b"not a table").unwrap();
    assert!(matches!(
        ChunkTable::install(damaged, None, None),
        Err(vykar_types::error::VykarError::InvalidFormat(_))
    ));
    let gone = temp_of(dir.path(), &oracle_of(0..5));
    std::fs::remove_file(&gone).unwrap();
    assert!(matches!(
        ChunkTable::install(gone, None, None),
        Err(vykar_types::error::VykarError::Io(_))
    ));
}

/// TBL-2: a flipped byte in the body or the checksum, a wrong magic and a
/// short file fail the open.
#[test]
fn open_file_rejects_damage() {
    let dir = tempfile::tempdir().unwrap();
    let table = ChunkTable::install(
        temp_of(dir.path(), &oracle_of(0..20)),
        Some(name(1)),
        Some(dir.path()),
    )
    .unwrap();
    let path = table.file_path().unwrap().to_path_buf();
    drop(table);
    let good = std::fs::read(&path).unwrap();
    let damaged = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut b = good.clone();
        f(&mut b);
        std::fs::write(&path, &b).unwrap();
        ChunkTable::open_file(&path).is_none()
    };
    assert!(damaged(&|b| b[40] ^= 1), "body");
    assert!(damaged(&|b| *b.last_mut().unwrap() ^= 1), "checksum");
    assert!(damaged(&|b| b[0] ^= 1), "magic");
    assert!(damaged(&|b| b.truncate(b.len() - 1)), "short");
    assert!(damaged(&|b| b.truncate(10)), "very short");
    std::fs::write(&path, &good).unwrap();
    assert!(ChunkTable::open_file(&path).is_some());
    // Not named like a table.
    let elsewhere = dir.path().join("chunk_table.x");
    std::fs::copy(&path, &elsewhere).unwrap();
    assert!(ChunkTable::open_file(&elsewhere).is_none());
}

/// TBL-3: the prune keeps the current base, mapped files and young files;
/// it deletes old other bases, old segments not kept, and old 80-byte
/// tables.
#[test]
fn prune_keeps_current_mapped_and_young_files() {
    let dir = tempfile::tempdir().unwrap();
    let install = |n: u8| {
        ChunkTable::install(
            temp_of(dir.path(), &oracle_of(n..n + 5)),
            Some(name(n)),
            Some(dir.path()),
        )
        .unwrap()
    };
    let age = |p: &Path, secs: u64| {
        std::fs::File::options()
            .write(true)
            .open(p)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - Duration::from_secs(secs))
            .unwrap();
    };
    let keep = install(10);
    let mapped = install(20);
    let young = install(30);
    let old = install(40);
    for t in [&keep, &mapped, &old] {
        age(t.file_path().unwrap(), 7200);
    }
    let (keep_path, mapped_path, young_path, old_path) = (
        keep.file_path().unwrap().to_path_buf(),
        mapped.file_path().unwrap().to_path_buf(),
        young.file_path().unwrap().to_path_buf(),
        old.file_path().unwrap().to_path_buf(),
    );
    drop((keep, young, old));
    let seg_kept = segment_cache_path(dir.path(), &name(50));
    let seg_old = segment_cache_path(dir.path(), &name(51));
    let seg_young = segment_cache_path(dir.path(), &name(52));
    let legacy = dir
        .path()
        .join(format!("{LEGACY_TABLE_PREFIX}{}", "cd".repeat(32)));
    let foreign = dir.path().join("something_else");
    for p in [&seg_kept, &seg_old, &seg_young, &legacy, &foreign] {
        std::fs::write(p, b"x").unwrap();
    }
    for p in [&seg_kept, &seg_old, &legacy, &foreign] {
        age(p, 7200);
    }
    let keep_segments: HashSet<ObjectName> = [name(50)].into_iter().collect();

    let deleted =
        prune_local_index_files(dir.path(), Some(&name(10)), &keep_segments, TABLE_KEEP_AGE);
    assert_eq!(deleted, 3, "old base, old segment, legacy table");
    for p in [
        &keep_path,
        &mapped_path,
        &young_path,
        &seg_kept,
        &seg_young,
        &foreign,
    ] {
        assert!(p.exists(), "{}", p.display());
    }
    for p in [&old_path, &seg_old, &legacy] {
        assert!(!p.exists(), "{}", p.display());
    }
    drop(mapped);
    assert_eq!(
        prune_local_index_files(dir.path(), Some(&name(10)), &keep_segments, TABLE_KEEP_AGE),
        1,
        "unmapped now"
    );
    assert_eq!(
        prune_local_index_files(dir.path(), Some(&name(10)), &keep_segments, Duration::ZERO),
        2,
        "the young base and segment"
    );
    assert!(keep_path.exists() && seg_kept.exists());
}

/// TBL-4: the segment cache returns only bytes matching the reference, and
/// deletes a file that does not.
#[test]
fn segment_cache_checks_name_and_size() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"stored segment bytes".to_vec();
    let r = SegmentRef {
        name: ObjectName::of(&bytes),
        size: bytes.len() as u64,
        kind: SegmentKind::Backup,
        entries: 1,
    };
    assert!(read_cached_segment(dir.path(), &r).is_none());
    write_cached_segment(dir.path(), &r.name, &bytes);
    assert_eq!(read_cached_segment(dir.path(), &r), Some(bytes.clone()));
    // Written once: a second write leaves the file alone.
    write_cached_segment(dir.path(), &r.name, b"other");
    assert_eq!(read_cached_segment(dir.path(), &r), Some(bytes.clone()));

    let path = segment_cache_path(dir.path(), &r.name);
    std::fs::write(&path, b"stored segment bytez").unwrap();
    assert!(read_cached_segment(dir.path(), &r).is_none());
    assert!(!path.exists(), "a mismatching file is deleted");
}

/// TBL-5: trailer lengths and the pack count come from the pack table.
#[test]
fn pack_table_answers_trailers_and_counts() {
    let dir = tempfile::tempdir().unwrap();
    let packs = [(pack(1), 0), (pack(2), 77)];
    let mut w = BaseWriter::new(
        TableFileWriter::new_beside(&table_anchor(dir.path())).unwrap(),
        &packs,
        3,
    )
    .unwrap();
    w.push(&id(1), 0, 10, 5).unwrap();
    w.push(&id(2), 1, 20, 6).unwrap();
    w.push(&id(3), 1, 30, 7).unwrap();
    let tmp = w.finish().unwrap().finish().unwrap();
    let table = ChunkTable::install(tmp, None, None).unwrap();
    assert_eq!(table.distinct_packs(), 2);
    assert_eq!(table.pack_trailer_len(&pack(1)), Some(0));
    assert_eq!(table.pack_trailer_len(&pack(2)), Some(77));
    assert_eq!(table.pack_trailer_len(&pack(3)), None);
    let live = LiveIndex::from_table(Arc::new(table));
    assert_eq!(live.count_distinct_packs(), 2);
    assert_eq!(live.pack_trailer_len(&pack(2)), Some(77));
}

#[test]
fn prefilter_covers_table_and_applied_additions() {
    let dir = tempfile::tempdir().unwrap();
    let table = scratch_of(dir.path(), &oracle_of(0..40));
    let mut live = LiveIndex::from_table(Arc::clone(&table));
    let seg = SegmentBody {
        kind: SegmentKind::Maintenance,
        additions: sorted(vec![(id(50), loc(9, 4, 33))]),
        removals: vec![id(3)],
        relocations: Vec::new(),
        pack_trailers: vec![(pack(9), 12)],
    };
    live.apply_segment(name(1), &seg).unwrap();
    let prefilter = DedupPrefilter::new(live.view().unwrap());
    assert!(Arc::ptr_eq(prefilter.view().table(), &table));
    for n in 0..40 {
        match prefilter.lookup(&id(n)) {
            PrefilterVerdict::Present {
                stored_size,
                position,
            } => {
                assert_ne!(n, 3, "removed by the segment");
                assert_eq!(table.chunk_id_at(position), id(n));
                assert_eq!(stored_size, 100 + u32::from(n));
            }
            PrefilterVerdict::Absent => assert_eq!(n, 3),
            other => panic!("{n}: {other:?}"),
        }
    }
    assert_eq!(
        prefilter.lookup(&id(50)),
        PrefilterVerdict::PresentAdded { stored_size: 33 }
    );
    for n in 60..80 {
        assert_eq!(prefilter.lookup(&id(n)), PrefilterVerdict::Absent);
    }
    // An empty table: lookups go through the additions only.
    let mut empty = LiveIndex::from_table(Arc::new(ChunkTable::empty()));
    let mut add = seg.clone();
    add.removals.clear();
    empty.apply_segment(name(2), &add).unwrap();
    let prefilter = DedupPrefilter::new(empty.view().unwrap());
    assert_eq!(
        prefilter.lookup(&id(50)),
        PrefilterVerdict::PresentAdded { stored_size: 33 }
    );
    assert_eq!(prefilter.lookup(&id(1)), PrefilterVerdict::Absent);
}

fn sorted(mut v: Vec<(ChunkId, Location)>) -> Vec<(ChunkId, Location)> {
    v.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    v
}

/// LIVE-1: removing or relocating an absent chunk fails; adding a present
/// chunk keeps the lower (pack, offset) whatever the order.
#[test]
fn apply_segment_rules() {
    let dir = tempfile::tempdir().unwrap();
    let table = scratch_of(dir.path(), &oracle_of(0..10));
    let fresh = || LiveIndex::from_table(Arc::clone(&table));
    let maint = |additions, removals, relocations| {
        let mut s = SegmentBody {
            kind: SegmentKind::Maintenance,
            additions,
            removals,
            relocations,
            pack_trailers: Vec::new(),
        };
        let mut packs: Vec<PackId> = s
            .additions
            .iter()
            .chain(s.relocations.iter())
            .map(|(_, l): &(ChunkId, Location)| l.pack_id)
            .collect();
        packs.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        packs.dedup();
        s.pack_trailers = packs.into_iter().map(|p| (p, 0)).collect();
        s
    };
    let err = fresh()
        .apply_segment(name(1), &maint(vec![], vec![id(77)], vec![]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("removes absent chunk"), "{err}");
    let err = fresh()
        .apply_segment(
            name(1),
            &maint(vec![], vec![], vec![(id(77), loc(1, 0, 1))]),
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("relocates absent chunk"), "{err}");

    // id(1) is in pack 2 at offset 10 (oracle_of): a copy at pack 1 is
    // lower, one at pack 7 higher.
    let low = maint(vec![(id(1), loc(1, 500, 9))], vec![], vec![]);
    let high = maint(vec![(id(1), loc(7, 0, 9))], vec![], vec![]);
    for order in [[&low, &high], [&high, &low]] {
        let mut live = fresh();
        live.apply_segment(name(1), order[0]).unwrap();
        live.apply_segment(name(2), order[1]).unwrap();
        let e = live.get(&id(1)).unwrap();
        assert_eq!((e.pack_id, e.pack_offset), (pack(1), 500));
    }
    // Two additions of a new chunk resolve the same way in both orders.
    let a = maint(vec![(id(60), loc(5, 8, 1))], vec![], vec![]);
    let b = maint(vec![(id(60), loc(5, 4, 1))], vec![], vec![]);
    for order in [[&a, &b], [&b, &a]] {
        let mut live = fresh();
        live.apply_segment(name(1), order[0]).unwrap();
        live.apply_segment(name(2), order[1]).unwrap();
        assert_eq!(live.get(&id(60)).unwrap().pack_offset, 4);
        assert_eq!(live.state().segments, vec![name(1), name(2)]);
    }
    // Edits first: a bug.
    let mut live = fresh();
    live.remove(&id(1));
    assert!(live.apply_segment(name(1), &a).is_err());
}

/// R4: a pack known with another trailer length takes the larger, no error.
#[test]
fn conflicting_trailer_lengths_take_the_larger() {
    let mut live = LiveIndex::from_table(Arc::new(ChunkTable::empty()));
    let seg = |n: u8, t: u32| SegmentBody {
        kind: SegmentKind::Backup,
        additions: vec![(id(n), loc(4, u64::from(n), 1))],
        removals: Vec::new(),
        relocations: Vec::new(),
        pack_trailers: vec![(pack(4), t)],
    };
    live.apply_segment(name(1), &seg(1, 100)).unwrap();
    live.apply_segment(name(2), &seg(2, 300)).unwrap();
    live.apply_segment(name(3), &seg(3, 200)).unwrap();
    assert_eq!(live.pack_trailer_len(&pack(4)), Some(300));
}

/// LIVE-2: the pending segment is exactly the keyed change; edits that
/// changed nothing give an empty segment; an unknown trailer length is an error.
#[test]
fn pending_segment_is_the_keyed_difference() {
    let dir = tempfile::tempdir().unwrap();
    let base = oracle_of(0..10);
    let table = scratch_of(dir.path(), &base);
    let mut live = LiveIndex::from_table(Arc::clone(&table));
    live.apply_segment(
        name(1),
        &SegmentBody {
            kind: SegmentKind::Backup,
            additions: sorted(vec![(id(20), loc(8, 1, 1)), (id(21), loc(8, 2, 1))]),
            removals: Vec::new(),
            relocations: Vec::new(),
            pack_trailers: vec![(pack(8), 5)],
        },
    )
    .unwrap();

    live.add(id(1), 999, pack(9), 999); // present: left as it is
    assert!(live.is_modified());
    assert!(
        live.pending_segment(SegmentKind::Maintenance)
            .unwrap()
            .is_empty()
    );

    live.add(id(30), 3, pack(9), 7); // new
    live.remove(&id(2)); // table entry
    live.remove(&id(20)); // applied addition
    live.update_location(&id(4), pack(8), 50, 104); // table entry moved
    live.update_location(&id(21), pack(8), 60, 1); // applied addition moved
    live.remove(&id(5));
    live.add(id(5), 105, pack(8), 70); // a removed table chunk re-added
    let err = live.pending_segment(SegmentKind::Maintenance).unwrap_err();
    assert!(err.to_string().contains("trailer length"), "{err}");
    live.set_pack_trailer(pack(9), 0);
    let seg = live.pending_segment(SegmentKind::Maintenance).unwrap();
    assert_eq!(seg.additions, sorted(vec![(id(30), loc(9, 7, 3))]));
    let mut removals = vec![id(2), id(20)];
    removals.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    assert_eq!(seg.removals, removals);
    assert_eq!(
        seg.relocations,
        sorted(vec![
            (id(4), loc(8, 50, 104)),
            (id(21), loc(8, 60, 1)),
            (id(5), loc(8, 70, 105)),
        ])
    );
    assert_eq!(seg.pack_trailers, vec![(pack(8), 5), (pack(9), 0)]);
    seg.validate().unwrap();

    // LIVE-3: a view taken before the edits does not see them, and after
    // mark_written the index equals a fresh apply of the same segments.
    let mut fresh = LiveIndex::from_table(Arc::clone(&table));
    let mut replay = live.loaded_state();
    let view = replay.view().unwrap();
    assert!(view.find_added(&id(30)).is_none());
    fresh
        .apply_segment(name(1), &{
            let mut s = SegmentBody::empty(SegmentKind::Backup);
            s.additions = sorted(vec![(id(20), loc(8, 1, 1)), (id(21), loc(8, 2, 1))]);
            s.pack_trailers = vec![(pack(8), 5)];
            s
        })
        .unwrap();
    fresh.apply_segment(name(2), &seg).unwrap();
    live.mark_written(name(2));
    assert!(!live.is_modified());
    assert_eq!(entries_of(&live), entries_of(&fresh));
    assert_eq!(live.state(), fresh.state());
    replay.apply_segment(name(2), &seg).unwrap();
    assert_eq!(entries_of(&replay), entries_of(&fresh));
    assert!(view.find_added(&id(30)).is_none(), "the view is immutable");

    // LIVE-4: the base body of an index with applied segments (and edits)
    // reads back as the same entries.
    live.add(id(40), 9, pack(8), 90);
    let (w, n) = live
        .write_base_body(TableFileWriter::new_beside(&table_anchor(dir.path())).unwrap())
        .unwrap();
    assert_eq!(n, live.len() as u64);
    let folded = ChunkTable::install(w.finish().unwrap(), None, None).unwrap();
    assert_eq!(entries_of(&folded), entries_of(&live));
    assert_eq!(folded.pack_trailer_len(&pack(8)), Some(5));
}

/// LIVE-5: positions cover the table and the entries added by segments and
/// edits.
#[test]
fn index_positions_cover_table_and_added_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut index = LiveIndex::from_table(scratch_of(dir.path(), &oracle_of(0..10)));
    index
        .apply_segment(
            name(1),
            &SegmentBody {
                kind: SegmentKind::Backup,
                additions: sorted(vec![(id(25), loc(9, 1, 1))]),
                removals: Vec::new(),
                relocations: Vec::new(),
                pack_trailers: vec![(pack(9), 0)],
            },
        )
        .unwrap();
    index.remove(&id(2));
    index.remove(&id(5));
    for n in [20, 21, 22] {
        index.add(id(n), 7, pack(9), u64::from(n));
    }
    index.add(id(5), 105, pack(4), 50);

    let positions = index.positions();
    assert_eq!(positions.len(), 14);
    let mut seen = HashMap::new();
    positions.for_each_live(|p, chunk, e| {
        assert!((p as usize) < positions.len());
        assert!(seen.insert(chunk, (p, e)).is_none(), "{chunk} twice");
    });
    let mut previous = None;
    positions.for_each_live(|p, _, _| {
        assert!(previous.is_none_or(|q| q < p), "not in position order");
        previous = Some(p);
    });
    let expected: HashMap<ChunkId, ChunkIndexEntry> = index.iter().collect();
    assert_eq!(seen.len(), expected.len());
    for (chunk, (p, e)) in &seen {
        assert_eq!(expected.get(chunk), Some(e), "{chunk}");
        assert_eq!(positions.find(chunk), Some(*p));
    }
    for n in 0..30 {
        assert_eq!(
            positions.find(&id(n)).is_some(),
            index.contains(&id(n)),
            "{n}"
        );
    }
    assert_eq!(
        positions.find(&id(2)),
        None,
        "a removed table entry has no position"
    );
}

#[derive(Debug, Clone)]
enum Op {
    Add(u8, u8),
    UpdateLocation(u8, u8),
    Remove(u8),
    RemoveByPack(u8),
    /// Keep only the chunks whose id byte is selected by the mask (a sweep).
    Sweep(u64),
    /// Write the pending edits as a segment (a write).
    Write,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0..80u8, 1..4u8).prop_map(|(n, p)| Op::Add(n, p)),
        2 => (0..80u8, 1..5u8).prop_map(|(n, p)| Op::UpdateLocation(n, p)),
        5 => (0..80u8).prop_map(Op::Remove),
        1 => (1..5u8).prop_map(Op::RemoveByPack),
        1 => any::<u64>().prop_map(Op::Sweep),
        2 => Just(Op::Write),
    ]
}

fn apply_live(index: &mut LiveIndex, op: &Op) -> String {
    match *op {
        Op::Add(n, p) => {
            index.add(id(n), 7 + u32::from(p), pack(p), u64::from(p) * 3);
            String::new()
        }
        Op::UpdateLocation(n, p) => {
            format!("{}", index.update_location(&id(n), pack(p), 99, 5))
        }
        Op::Remove(n) => format!("{:?}", index.remove(&id(n))),
        Op::RemoveByPack(p) => format!("{}", index.remove_by_pack(&pack(p))),
        Op::Sweep(mask) => {
            for n in (0..80u8).filter(|n| !swept_keeps(mask, *n)) {
                index.remove(&id(n));
            }
            String::new()
        }
        Op::Write => String::new(),
    }
}

fn apply_oracle(index: &mut ChunkIndex, op: &Op) -> String {
    match *op {
        Op::Add(n, p) => {
            index.add(id(n), 7 + u32::from(p), pack(p), u64::from(p) * 3);
            String::new()
        }
        Op::UpdateLocation(n, p) => {
            format!("{}", index.update_location(&id(n), pack(p), 99, 5))
        }
        Op::Remove(n) => format!("{:?}", index.remove(&id(n))),
        Op::RemoveByPack(p) => format!("{}", index.remove_by_pack(&pack(p))),
        Op::Sweep(mask) => {
            for n in (0..80u8).filter(|n| !swept_keeps(mask, *n)) {
                index.remove(&id(n));
            }
            String::new()
        }
        Op::Write => String::new(),
    }
}

fn swept_keeps(mask: u64, n: u8) -> bool {
    mask & (1 << (n % 64)) != 0
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    /// LIVE-6: a `LiveIndex` over a table behaves as a `ChunkIndex`, for
    /// every read after every edit, across writes (pending segment, then
    /// mark_written); and the segments written, applied in order to the
    /// same table, give the same entries (a re-sync).
    #[test]
    fn live_index_matches_chunk_index(
        base in prop::collection::btree_set(0..80u8, 0..64),
        ops in prop::collection::vec(arb_op(), 0..60),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let oracle0 = oracle_of(base.iter().copied());
        let table = scratch_of(dir.path(), &oracle0);
        let mut live = LiveIndex::from_table(Arc::clone(&table));
        let mut oracle = oracle0.clone();
        let mut written: Vec<SegmentBody> = Vec::new();
        for p in 1..5u8 {
            live.set_pack_trailer(pack(p), 0);
        }
        for op in &ops {
            prop_assert_eq!(apply_live(&mut live, op), apply_oracle(&mut oracle, op), "{:?}", op);
            if matches!(op, Op::Write) {
                let seg = live.pending_segment(SegmentKind::Maintenance).unwrap();
                if seg.is_empty() {
                    // Edits that changed nothing are discarded.
                    live.discard_edits();
                } else {
                    seg.validate().unwrap();
                    live.mark_written(name(written.len() as u8));
                    written.push(seg);
                }
                for p in 1..5u8 {
                    live.set_pack_trailer(pack(p), 0);
                }
            }
            prop_assert_eq!(live.len(), oracle.len());
            for n in 0..80u8 {
                prop_assert_eq!(live.contains(&id(n)), oracle.contains(&id(n)));
                prop_assert_eq!(
                    live.get(&id(n)).map(|e| (e.pack_id, e.pack_offset, e.stored_size)),
                    oracle.get(&id(n)).map(|e| (e.pack_id, e.pack_offset, e.stored_size))
                );
            }
            prop_assert_eq!(entries_of(&live), entries_of(&oracle));
            prop_assert!(live.count_distinct_packs() >= oracle.count_distinct_packs());
        }
        let seg = live.pending_segment(SegmentKind::Maintenance).unwrap();
        if !seg.is_empty() {
            live.mark_written(name(written.len() as u8));
            written.push(seg);
        }
        let mut resync = LiveIndex::from_table(Arc::clone(&table));
        for (k, seg) in written.iter().enumerate() {
            resync.apply_segment(name(k as u8), seg).unwrap();
        }
        prop_assert_eq!(entries_of(&resync), entries_of(&oracle));
    }
}
