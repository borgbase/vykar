use std::path::{Path, PathBuf};

use proptest::prelude::*;

use super::*;
use crate::index::dedup_cache::{
    build_full_index_cache_to_path, chunk_index_from_full_cache, generation_for_state,
};

use super::model;

fn id(b: u8) -> ChunkId {
    ChunkId::from_bytes([b; 32])
}

fn pack(b: u8) -> PackId {
    PackId::from_bytes([b; 32])
}

fn entry(chunk: ChunkId, refcount: u32, p: u8, offset: u64) -> FullCacheEntry {
    FullCacheEntry {
        chunk_id: chunk,
        refcount,
        stored_size: 100 + offset as u32,
        pack_id: pack(p),
        pack_offset: offset,
    }
}

fn index_from(entries: &[FullCacheEntry]) -> ChunkIndex {
    let mut index = ChunkIndex::new();
    for e in entries {
        index.add(e.chunk_id, e.stored_size, e.pack_id, e.pack_offset);
        if e.refcount > 1 {
            index.increment_refcount_by(&e.chunk_id, e.refcount - 1);
        }
    }
    index
}

/// Write `index` as a full cache at `dir/name` and open it by content.
fn write_cache(index: &ChunkIndex, dir: &Path, name: &str) -> MmapFullIndexCache {
    let path = dir.join(name);
    build_full_index_cache_to_path(index, 1, &path).unwrap();
    MmapFullIndexCache::open_verified(&path).unwrap()
}

fn sorted(index: &ChunkIndex) -> Vec<(ChunkId, u32, u32, PackId, u64)> {
    let mut v: Vec<_> = index
        .iter()
        .map(|(id, e)| (*id, e.refcount, e.stored_size, e.pack_id, e.pack_offset))
        .collect();
    v.sort_unstable_by_key(|e| *e.0.as_bytes());
    v
}

fn target(dir: &Path) -> PathBuf {
    dir.join("out")
}

/// Diff `prev` against `next`, check the written cache and a separate apply
/// both reproduce `next`, and return the diff.
fn diff_checked(prev: &MmapFullIndexCache, next: &ChunkIndex, dir: &Path) -> Diff {
    let expected = write_cache(next, dir, "expected");
    let (diff, tmp) = diff_to_temp(prev, next, &target(dir)).unwrap();
    assert_eq!(diff.state_hash_after, expected.state_hash());
    assert_eq!(diff.entries_after, next.len() as u64);

    let written = MmapFullIndexCache::open_verified(&tmp).unwrap();
    assert_eq!(written.state_hash(), expected.state_hash());
    assert_eq!(sorted(&chunk_index_from_full_cache(&written)), sorted(next));

    let decoded = Segment::decode(&diff.segment.encode()).unwrap();
    assert_eq!(decoded, diff.segment);
    let (applied, hash, count) = apply_to_temp(prev, &[diff.step()], &target(dir)).unwrap();
    assert_eq!(hash, expected.state_hash());
    assert_eq!(count, next.len() as u64);
    assert_eq!(
        std::fs::read(&applied).unwrap(),
        std::fs::read(&tmp).unwrap()
    );
    diff
}

#[test]
fn varint_roundtrip_and_canonical() {
    for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
        let mut buf = Vec::new();
        put_uvarint(&mut buf, v);
        let mut input = buf.as_slice();
        assert_eq!(get_uvarint(&mut input).unwrap(), v);
        assert!(input.is_empty());
    }
    // Overlong zero, overflowing 10th byte, truncated.
    assert!(get_uvarint(&mut [0x80u8, 0x00].as_slice()).is_err());
    let mut over = vec![0xffu8; 9];
    over.push(0x02);
    assert!(get_uvarint(&mut over.as_slice()).is_err());
    assert!(get_uvarint(&mut [0x80u8].as_slice()).is_err());
    for v in [0i64, 1, -1, i64::MAX, i64::MIN, -(u32::MAX as i64)] {
        assert_eq!(unzigzag(zigzag(v)), v);
    }
}

#[test]
fn empty_index_state() {
    let dir = tempfile::tempdir().unwrap();
    let empty = write_cache(&ChunkIndex::new(), dir.path(), "empty");
    assert_eq!(empty.state_hash(), *blake3::hash(b"").as_bytes());
    assert_eq!(empty.entry_count(), 0);

    let next = index_from(&[entry(id(1), 1, 1, 0), entry(id(2), 3, 1, 100)]);
    let diff = diff_checked(&empty, &next, dir.path());
    assert_eq!(diff.segment.new_entries.len(), 2);
    assert!(diff.segment.deltas.is_empty());
}

#[test]
fn removal_relocation_and_bump() {
    let dir = tempfile::tempdir().unwrap();
    let prev_entries = [
        entry(id(1), 2, 1, 0),
        entry(id(2), 1, 1, 100),
        entry(id(3), 5, 2, 0),
        entry(id(4), 1, 2, 100),
    ];
    let prev = write_cache(&index_from(&prev_entries), dir.path(), "prev");

    let mut next = index_from(&prev_entries);
    next.decrement(&id(2)); // to 0: removed
    next.increment_refcount_by(&id(1), 3);
    next.update_location(&id(3), pack(9), 7, 42);
    next.add(id(5), 10, pack(9), 99);

    let diff = diff_checked(&prev, &next, dir.path());
    let s = &diff.segment;
    assert_eq!(
        s.deltas,
        vec![
            RefcountDelta {
                position: 0,
                delta: 3
            },
            RefcountDelta {
                position: 1,
                delta: -1
            },
        ]
    );
    assert_eq!(s.relocations.len(), 1);
    assert_eq!(s.relocations[0].position, 2);
    assert_eq!(s.new_entries.len(), 1);
    assert_eq!(diff.entries_after, 4);
}

#[test]
fn refcount_at_u32_max() {
    let dir = tempfile::tempdir().unwrap();
    let prev_entries = [entry(id(1), 1, 1, 0), entry(id(2), u32::MAX - 1, 1, 1)];
    let prev = write_cache(&index_from(&prev_entries), dir.path(), "prev");

    // 1 -> u32::MAX and u32::MAX - 1 -> 0 are both representable.
    let mut next = index_from(&prev_entries);
    next.increment_refcount_by(&id(1), u32::MAX - 1);
    next.remove(&id(2));
    let diff = diff_checked(&prev, &next, dir.path());
    assert_eq!(diff.segment.deltas[0].delta, i64::from(u32::MAX) - 1);
    assert_eq!(diff.segment.deltas[1].delta, -(i64::from(u32::MAX) - 1));

    // One more than u32::MAX is rejected.
    let mut bad = diff.segment.clone();
    bad.deltas[0].delta += 1;
    let err = apply_unchecked(&prev, &bad, &target(dir.path())).unwrap_err();
    assert!(err.to_string().contains("refcount out of range"), "{err}");
}

#[test]
fn negative_result_and_bad_ops_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let prev_entries = [entry(id(1), 3, 1, 0), entry(id(3), 1, 1, 1)];
    let prev = write_cache(&index_from(&prev_entries), dir.path(), "prev");
    let base = Segment::empty(prev.state_hash(), 2);
    let t = target(dir.path());

    let mut s = base.clone();
    s.deltas.push(RefcountDelta {
        position: 0,
        delta: -4,
    });
    assert!(apply_unchecked(&prev, &s, &t).is_err());

    // Removal and relocation of the same entry.
    let mut s = base.clone();
    s.deltas.push(RefcountDelta {
        position: 1,
        delta: -1,
    });
    s.relocations.push(Relocation {
        position: 1,
        pack_id: pack(2),
        pack_offset: 0,
        stored_size: 1,
    });
    assert!(apply_unchecked(&prev, &s, &t).is_err());

    // A new entry already present, also when the predecessor entry is removed.
    let mut s = base.clone();
    s.new_entries.push(entry(id(3), 1, 2, 0));
    assert!(apply_unchecked(&prev, &s, &t).is_err());
    s.deltas.push(RefcountDelta {
        position: 1,
        delta: -1,
    });
    assert!(apply_unchecked(&prev, &s, &t).is_err());

    // A segment claiming more predecessor entries than there are.
    let mut s = base.clone();
    s.prev_entries = 3;
    assert!(apply_unchecked(&prev, &s, &t).is_err());
}

#[test]
fn chain_mismatch_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let prev_entries = [entry(id(1), 1, 1, 0)];
    let prev = write_cache(&index_from(&prev_entries), dir.path(), "prev");
    let mut next = index_from(&prev_entries);
    next.increment_refcount(&id(1));
    let diff = diff_checked(&prev, &next, dir.path());
    let t = target(dir.path());

    let mut wrong_after = diff.step();
    wrong_after.state_hash_after[0] ^= 1;
    assert!(apply_to_temp(&prev, &[wrong_after], &t).is_err());

    let mut wrong_count = diff.step();
    wrong_count.entries_after += 1;
    assert!(apply_to_temp(&prev, &[wrong_count], &t).is_err());

    // The same segment twice: the second does not follow the first.
    assert!(apply_to_temp(&prev, &[diff.step(), diff.step()], &t).is_err());

    let mut wrong_prev = diff.segment.clone();
    wrong_prev.prev_state_hash[0] ^= 1;
    let step = ChainStep {
        segment: &wrong_prev,
        ..diff.step()
    };
    assert!(apply_to_temp(&prev, &[step], &t).is_err());
}

#[test]
fn empty_chain_copies_the_predecessor() {
    let dir = tempfile::tempdir().unwrap();
    let prev = write_cache(&index_from(&[entry(id(1), 1, 1, 0)]), dir.path(), "prev");
    let hash = apply(&prev, &[], &target(dir.path())).unwrap();
    assert_eq!(hash, prev.state_hash());
    let out = MmapFullIndexCache::open_path(&target(dir.path()), generation_for_state(&hash));
    assert!(out.is_some(), "header carries the state-derived generation");
}

#[test]
fn zero_refcount_predecessor_entry() {
    let dir = tempfile::tempdir().unwrap();
    let mut index = index_from(&[entry(id(1), 1, 1, 0), entry(id(2), 1, 1, 1)]);
    index.rebuild_refcounts(&[(id(1), 0), (id(2), 1)].into_iter().collect());
    let prev = write_cache(&index, dir.path(), "prev");

    // Kept unchanged: no op needed, and apply keeps it. (Not via
    // `diff_checked`: `chunk_index_from_full_cache` loads refcount 0 as 1.)
    let (diff, _) = diff_to_temp(&prev, &index, &target(dir.path())).unwrap();
    assert!(diff.segment.is_empty());
    assert_eq!(diff.state_hash_after, prev.state_hash());
    let hash = apply(&prev, &[diff.step()], &target(dir.path())).unwrap();
    assert_eq!(hash, prev.state_hash());

    // Removing it has no representation.
    let mut next = index.clone();
    next.remove(&id(1));
    assert!(diff_to_temp(&prev, &next, &target(dir.path())).is_err());
}

#[test]
fn decode_rejects_malformed() {
    let seg = Segment {
        prev_state_hash: [7; 32],
        prev_entries: 10,
        deltas: vec![
            RefcountDelta {
                position: 2,
                delta: 1,
            },
            RefcountDelta {
                position: 9,
                delta: -3,
            },
        ],
        relocations: vec![Relocation {
            position: 4,
            pack_id: pack(3),
            pack_offset: 1 << 40,
            stored_size: 5,
        }],
        new_entries: vec![entry(id(1), 1, 1, 0), entry(id(2), 2, 1, 9)],
    };
    let bytes = seg.encode();
    assert_eq!(Segment::decode(&bytes).unwrap(), seg);

    // Truncation anywhere and trailing bytes.
    for n in 0..bytes.len() {
        assert!(Segment::decode(&bytes[..n]).is_err(), "prefix {n}");
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(Segment::decode(&trailing).is_err());

    // Version.
    let mut v = bytes.clone();
    v[0] = 2;
    assert!(Segment::decode(&v).is_err());

    // Duplicate and unsorted new entries.
    for new_entries in [
        vec![entry(id(1), 1, 1, 0), entry(id(1), 1, 1, 0)],
        vec![entry(id(2), 1, 1, 0), entry(id(1), 1, 1, 0)],
        vec![entry(id(1), 0, 1, 0)],
    ] {
        let s = Segment {
            new_entries,
            ..Segment::empty([0; 32], 0)
        };
        let (bytes, _) = encode_unchecked(&s);
        assert!(Segment::decode(&bytes).is_err());
    }

    // Position at or beyond prev_entries, zero delta.
    for deltas in [
        vec![RefcountDelta {
            position: 10,
            delta: 1,
        }],
        vec![RefcountDelta {
            position: 0,
            delta: 0,
        }],
    ] {
        let s = Segment {
            deltas,
            ..Segment::empty([0; 32], 10)
        };
        let (bytes, _) = encode_unchecked(&s);
        assert!(Segment::decode(&bytes).is_err());
    }
}

/// Encode without validation, for malformed inputs.
fn encode_unchecked(s: &Segment) -> (Vec<u8>, SegmentSizes) {
    s.encode_raw()
}

#[test]
fn decode_bounds_counts_before_allocating() {
    let mut bytes = vec![SEGMENT_VERSION];
    bytes.extend_from_slice(&[0; 32]);
    put_uvarint(&mut bytes, u64::MAX); // prev_entries
    put_uvarint(&mut bytes, u64::MAX / 2); // n_deltas
    put_uvarint(&mut bytes, 0);
    put_uvarint(&mut bytes, 0);
    let err = Segment::decode(&bytes).unwrap_err();
    assert!(err.to_string().contains("counts exceed"), "{err}");
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Op {
    Bump(usize, u32),
    Decrement(usize),
    Remove(usize),
    Relocate(usize, u8, u64),
    Add(u8, u32),
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (any::<usize>(), 1..5u32).prop_map(|(i, n)| Op::Bump(i, n)),
        2 => any::<usize>().prop_map(Op::Decrement),
        1 => any::<usize>().prop_map(Op::Remove),
        1 => (any::<usize>(), any::<u8>(), any::<u64>()).prop_map(|(i, p, o)| Op::Relocate(i, p, o)),
        2 => (any::<u8>(), 1..4u32).prop_map(|(b, rc)| Op::Add(b, rc)),
    ]
}

fn arb_index() -> impl Strategy<Value = Vec<FullCacheEntry>> {
    prop::collection::btree_map(any::<[u8; 32]>(), (1..6u32, any::<u8>(), 0..1000u64), 0..40)
        .prop_map(|m| {
            m.into_iter()
                .map(|(b, (rc, p, o))| entry(ChunkId::from_bytes(b), rc, p, o))
                .collect()
        })
}

fn mutate(index: &ChunkIndex, ops: &[Op], salt: u8) -> ChunkIndex {
    let mut next = index.clone();
    let ids = |n: &ChunkIndex| {
        let mut v: Vec<ChunkId> = n.iter().map(|(id, _)| *id).collect();
        v.sort_unstable_by_key(|id| *id.as_bytes());
        v
    };
    for op in ops {
        let cur = ids(&next);
        let pick = |i: usize| cur.get(i % cur.len().max(1)).copied();
        match *op {
            Op::Bump(i, n) => {
                if let Some(id) = pick(i) {
                    next.increment_refcount_by(&id, n);
                }
            }
            Op::Decrement(i) => {
                if let Some(id) = pick(i) {
                    next.decrement(&id);
                }
            }
            Op::Remove(i) => {
                if let Some(id) = pick(i) {
                    next.remove(&id);
                }
            }
            Op::Relocate(i, p, o) => {
                if let Some(id) = pick(i) {
                    next.update_location(&id, pack(p), o, (o % 977) as u32);
                }
            }
            Op::Add(b, rc) => {
                let mut bytes = [b; 32];
                bytes[31] = salt;
                let id = ChunkId::from_bytes(bytes);
                if !next.contains(&id) {
                    next.add(id, 50, pack(b), u64::from(b));
                    next.increment_refcount_by(&id, rc - 1);
                }
            }
        }
    }
    next
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// apply(prev, diff(prev, next)) == next, with equal state hash.
    #[test]
    fn diff_then_apply_roundtrips(
        prev in arb_index(),
        ops in prop::collection::vec(arb_op(), 0..60),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let prev_index = index_from(&prev);
        let prev_cache = write_cache(&prev_index, dir.path(), "prev");
        let next = mutate(&prev_index, &ops, 0);
        diff_checked(&prev_cache, &next, dir.path());
    }

    /// Composing N segments in one pipeline equals applying them one by one,
    /// and both equal the final state.
    #[test]
    fn chain_composes(
        start in arb_index(),
        rounds in prop::collection::vec(prop::collection::vec(arb_op(), 0..20), 1..6),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let start_index = index_from(&start);
        let start_cache = write_cache(&start_index, dir.path(), "s0");

        let mut diffs = Vec::new();
        let mut cur_index = start_index;
        let mut cur_cache = write_cache(&cur_index, dir.path(), "cur");
        for (k, ops) in rounds.iter().enumerate() {
            let next = mutate(&cur_index, ops, k as u8 + 1);
            diffs.push(diff_checked(&cur_cache, &next, dir.path()));
            cur_cache = write_cache(&next, dir.path(), &format!("s{}", k + 1));
            cur_index = next;
        }

        let steps: Vec<ChainStep<'_>> = diffs.iter().map(Diff::step).collect();
        let composed = dir.path().join("composed");
        let hash = apply(&start_cache, &steps, &composed).unwrap();
        prop_assert_eq!(hash, cur_cache.state_hash());

        let mut stepwise = MmapFullIndexCache::open_verified(&dir.path().join("s0")).unwrap();
        for (k, step) in steps.iter().enumerate() {
            let out = dir.path().join(format!("step{k}"));
            apply(&stepwise, std::slice::from_ref(step), &out).unwrap();
            stepwise = MmapFullIndexCache::open_verified(&out).unwrap();
        }
        prop_assert_eq!(stepwise.state_hash(), hash);
        prop_assert_eq!(
            std::fs::read(&composed).unwrap(),
            std::fs::read(dir.path().join(format!("step{}", steps.len() - 1))).unwrap()
        );
    }

    /// Decoding arbitrary bytes never panics, and whatever decodes
    /// re-encodes to the same bytes.
    #[test]
    fn decode_is_total_and_canonical(bytes in prop::collection::vec(any::<u8>(), 0..300)) {
        if let Ok(seg) = Segment::decode(&bytes) {
            prop_assert_eq!(seg.encode(), bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// Measurement against real index states
// ---------------------------------------------------------------------------

/// Diff two real full index caches and report segment size, per-section
/// size, compressed size, and pass times. Needs two copies of a repository's
/// `full_index_cache` taken some time apart:
///
/// ```text
/// VYKAR_SEGMENT_PREV=/path/a VYKAR_SEGMENT_NEXT=/path/b \
///   cargo test --release -p vykar-core --lib \
///   index::segment::tests::measure_real_index_states -- --ignored --nocapture
/// ```
#[test]
#[ignore = "measurement tool; set VYKAR_SEGMENT_PREV and VYKAR_SEGMENT_NEXT"]
#[allow(clippy::print_stderr, reason = "the report is this tool's output")]
fn measure_real_index_states() {
    let (Some(prev_path), Some(next_path)) = (
        std::env::var_os("VYKAR_SEGMENT_PREV"),
        std::env::var_os("VYKAR_SEGMENT_NEXT"),
    ) else {
        eprintln!("VYKAR_SEGMENT_PREV / VYKAR_SEGMENT_NEXT not set, skipping");
        return;
    };
    let ms = |t: std::time::Instant| t.elapsed().as_millis();
    let dir = tempfile::tempdir().unwrap();

    let prev = MmapFullIndexCache::open_verified(Path::new(&prev_path))
        .expect("VYKAR_SEGMENT_PREV is not a valid full index cache");
    let next_cache = MmapFullIndexCache::open_verified(Path::new(&next_path))
        .expect("VYKAR_SEGMENT_NEXT is not a valid full index cache");
    let t = std::time::Instant::now();
    let next = chunk_index_from_full_cache(&next_cache);
    eprintln!("load next into ChunkIndex: {} ms", ms(t));

    let t = std::time::Instant::now();
    let (diff, _tmp) = diff_to_temp(&prev, &next, &target(dir.path())).unwrap();
    let diff_ms = ms(t);
    assert_eq!(diff.state_hash_after, next_cache.state_hash());

    let t = std::time::Instant::now();
    let (bytes, sizes) = diff.segment.encode_with_sizes();
    let encode_ms = ms(t);
    let t = std::time::Instant::now();
    let zstd3 = zstd::bulk::compress(&bytes, 3).unwrap().len();
    let zstd3_ms = ms(t);
    let zstd19 = zstd::bulk::compress(&bytes, 19).unwrap().len();
    let comp = |section: &[u8]| zstd::bulk::compress(section, 3).unwrap().len();
    let (h, dp, dv) = (
        sizes.header,
        sizes.header + sizes.delta_positions,
        sizes.header + sizes.delta_positions + sizes.delta_values,
    );
    let rl = dv + sizes.relocations;

    let t = std::time::Instant::now();
    let decoded = Segment::decode(&bytes).unwrap();
    let decode_ms = ms(t);
    let t = std::time::Instant::now();
    let hash = apply(&prev, &[diff.step()], &target(dir.path())).unwrap();
    let apply_ms = ms(t);
    assert_eq!(hash, next_cache.state_hash());
    assert_eq!(decoded, diff.segment);

    let prev_n = f64::from(prev.entry_count());
    let s = &diff.segment;
    let bumped = s.deltas.iter().filter(|d| d.delta > 0).count();
    let decremented = s.deltas.iter().filter(|d| d.delta < 0).count();
    eprintln!("prev entries:        {}", prev.entry_count());
    eprintln!("next entries:        {}", diff.entries_after);
    eprintln!(
        "deltas:              {} (+{bumped} / -{decremented}), fraction of prev {:.3}",
        s.deltas.len(),
        f64::from(u32::try_from(s.deltas.len()).unwrap()) / prev_n.max(1.0)
    );
    eprintln!("relocations:         {}", s.relocations.len());
    eprintln!("new entries:         {}", s.new_entries.len());
    eprintln!("segment raw:         {} B", sizes.total());
    eprintln!(
        "  delta positions:   {} B raw, {} B zstd-3",
        sizes.delta_positions,
        comp(&bytes[h..dp])
    );
    eprintln!(
        "  delta values:      {} B raw, {} B zstd-3",
        sizes.delta_values,
        comp(&bytes[dp..dv])
    );
    eprintln!(
        "  relocations:       {} B raw, {} B zstd-3",
        sizes.relocations,
        comp(&bytes[dv..rl])
    );
    eprintln!(
        "  new entries:       {} B raw, {} B zstd-3",
        sizes.new_entries,
        comp(&bytes[rl..])
    );
    eprintln!("segment zstd-3:      {zstd3} B ({zstd3_ms} ms)");
    eprintln!("segment zstd-19:     {zstd19} B");
    eprintln!("diff + write cache:  {diff_ms} ms");
    eprintln!("encode:              {encode_ms} ms");
    eprintln!("decode:              {decode_ms} ms");
    eprintln!("apply + write cache: {apply_ms} ms");
}

/// Apply one segment without checking the resulting state.
fn apply_unchecked(
    prev: &MmapFullIndexCache,
    segment: &Segment,
    target: &Path,
) -> Result<(TempPath, StateHash, u64)> {
    let mut layers = [Layer::new(segment)];
    let mut base = BaseSource {
        cache: prev,
        next: 0,
    };
    let mut w = FullCacheWriter::new_beside(target)?;
    while let Some(entry) = pull(&mut layers, &mut base)? {
        w.push(&entry)?;
    }
    let [layer] = layers;
    if layer.in_pos != segment.prev_entries {
        return Err(invalid("predecessor has fewer entries than recorded"));
    }
    let (tmp, hash, count) = w.finish(None)?;
    Ok((tmp.into_temp_path(), hash, count))
}

// ---------------------------------------------------------------------------
// merge_delta_to_temp
// ---------------------------------------------------------------------------

mod merge {
    use std::sync::Arc;

    use super::*;
    use crate::index::{ChunkTable, IndexDelta, LiveIndex};

    /// `index` as a scratch table in `dir`.
    fn table_of(index: &ChunkIndex, _dir: &Path) -> Arc<ChunkTable> {
        crate::testutil::base_table_of(index)
    }

    /// A delta whose positional bumps index a table of `session_start`.
    fn delta_over(session_start: &ChunkIndex, dir: &Path) -> IndexDelta {
        IndexDelta::with_cached_bumps(table_of(session_start, dir))
    }

    /// `delta` applied to `index` by key.
    fn apply_keyed(index: &ChunkIndex, delta: IndexDelta) -> ChunkIndex {
        let mut live = LiveIndex::empty();
        for (id, e) in index.iter() {
            live.add(*id, e.stored_size, e.pack_id, e.pack_offset);
            live.increment_refcount_by(id, e.refcount.saturating_sub(1));
        }
        delta.apply_to(&mut live);
        live.to_chunk_index()
    }

    fn bump_by_position(delta: &mut IndexDelta, chunk: ChunkId) {
        let (position, _) = delta
            .cached_bumps
            .as_ref()
            .unwrap()
            .cache()
            .find(&chunk)
            .unwrap();
        delta.bump_cached_refcount(&chunk, position);
    }

    fn new_entry(delta: &mut IndexDelta, e: FullCacheEntry) {
        delta.add_new_entry(
            e.chunk_id,
            e.stored_size,
            e.pack_id,
            e.pack_offset,
            e.refcount,
        );
    }

    /// The delta applied to `prev` by key, as the monolithic path does.
    fn expected(prev: &ChunkIndex, delta: IndexDelta) -> ChunkIndex {
        apply_keyed(prev, delta.reconcile(prev).unwrap())
    }

    /// Merge `delta` over `prev_index` and check the written state equals
    /// the keyed result, and that the segment reproduces it.
    fn check(prev_index: &ChunkIndex, delta: IndexDelta, dir: &Path) -> Diff {
        let prev = write_cache(prev_index, dir, "prev");
        let (diff, tmp) = merge_delta_to_temp(&prev, &delta, &target(dir)).unwrap();
        let written = MmapFullIndexCache::open_verified(&tmp).unwrap();
        assert_eq!(
            sorted(&chunk_index_from_full_cache(&written)),
            sorted(&expected(prev_index, delta))
        );
        assert_eq!(diff.state_hash_after, written.state_hash());
        assert_eq!(
            Segment::decode(&diff.segment.encode()).unwrap(),
            diff.segment
        );
        let (applied, hash, _) = apply_to_temp(&prev, &[diff.step()], &target(dir)).unwrap();
        assert_eq!(hash, diff.state_hash_after);
        assert_eq!(
            std::fs::read(&applied).unwrap(),
            std::fs::read(&tmp).unwrap()
        );
        diff
    }

    fn base_entries() -> Vec<FullCacheEntry> {
        (1..=8)
            .map(|b| entry(id(b * 10), 1, 1, u64::from(b)))
            .collect()
    }

    /// Positional, keyed and new-entry inputs together, against the state
    /// the session started from.
    #[test]
    fn all_inputs_against_the_session_start() {
        let dir = tempfile::tempdir().unwrap();
        let prev = index_from(&base_entries());
        let mut delta = delta_over(&prev, dir.path());
        bump_by_position(&mut delta, id(10));
        bump_by_position(&mut delta, id(10));
        bump_by_position(&mut delta, id(50));
        delta.bump_refcount(&id(50));
        new_entry(&mut delta, entry(id(15), 1, 3, 0));
        new_entry(&mut delta, entry(id(85), 2, 3, 100));
        // A keyed bump on a session-new chunk is not stale.
        delta.bump_refcount(&id(15));

        // Reuse is checked, not counted: no deltas, new entries at 1.
        let diff = check(&prev, delta, dir.path());
        assert!(diff.segment.deltas.is_empty());
        assert_eq!(diff.segment.new_entries.len(), 2);
        assert!(diff.segment.new_entries.iter().all(|e| e.refcount == 1));
    }

    /// The head moved during the session: positions index the session's
    /// dedup cache, not `prev`, and a new entry another client committed
    /// first writes nothing (the existing entry stays).
    #[test]
    fn head_moved_during_the_session() {
        let dir = tempfile::tempdir().unwrap();
        let start = index_from(&base_entries());
        let mut delta = delta_over(&start, dir.path());
        bump_by_position(&mut delta, id(30));
        bump_by_position(&mut delta, id(80));
        new_entry(&mut delta, entry(id(35), 1, 3, 0));
        new_entry(&mut delta, entry(id(5), 1, 3, 10));

        // Another client committed id(35) and id(1), and bumped id(80).
        let mut moved = start.clone();
        moved.add(id(35), 99, pack(4), 0);
        moved.add(id(1), 99, pack(4), 100);
        moved.increment_refcount(&id(80));

        let diff = check(&moved, delta, dir.path());
        assert_eq!(diff.segment.new_entries.len(), 1, "only id(5) is new");
        assert_eq!(diff.segment.new_entries[0].chunk_id, id(5));
        assert!(diff.segment.deltas.is_empty());
    }

    /// Duplicate new entries keep the first one's location, at refcount 1.
    #[test]
    fn duplicate_new_entries() {
        let dir = tempfile::tempdir().unwrap();
        let prev = index_from(&base_entries());
        let mut delta = IndexDelta::new();
        new_entry(&mut delta, entry(id(12), 1, 3, 0));
        new_entry(&mut delta, entry(id(12), 2, 4, 50));
        let prev_cache = write_cache(&prev, dir.path(), "prev");
        let (diff, _) = merge_delta_to_temp(&prev_cache, &delta, &target(dir.path())).unwrap();
        assert_eq!(diff.segment.new_entries, vec![entry(id(12), 1, 3, 0)]);
    }

    /// A bump whose chunk is in neither the index nor the new entries means
    /// the chunk was deleted during the session.
    #[test]
    fn stale_bumps_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let start = index_from(&base_entries());
        let mut pruned = start.clone();
        pruned.decrement(&id(40));
        pruned.decrement(&id(80));
        let prev = write_cache(&pruned, dir.path(), "prev");

        for victim in [id(40), id(80)] {
            let mut delta = delta_over(&start, dir.path());
            bump_by_position(&mut delta, victim);
            let err = merge_delta_to_temp(&prev, &delta, &target(dir.path())).unwrap_err();
            assert!(matches!(err, VykarError::StaleChunksDuringCommit), "{err}");

            let mut delta = IndexDelta::new();
            delta.bump_refcount(&victim);
            let err = merge_delta_to_temp(&prev, &delta, &target(dir.path())).unwrap_err();
            assert!(matches!(err, VykarError::StaleChunksDuringCommit), "{err}");
        }
    }

    /// A bump never changes a refcount, so one at u32::MAX stays as it is.
    #[test]
    fn bumps_do_not_change_refcounts() {
        let dir = tempfile::tempdir().unwrap();
        let prev = write_cache(
            &index_from(&[entry(id(1), u32::MAX, 1, 0)]),
            dir.path(),
            "prev",
        );
        let mut delta = IndexDelta::new();
        delta.bump_refcount(&id(1));
        let (diff, _) = merge_delta_to_temp(&prev, &delta, &target(dir.path())).unwrap();
        assert!(diff.segment.is_empty());
    }

    /// An empty delta changes nothing.
    #[test]
    fn empty_delta_is_an_empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let prev = index_from(&base_entries());
        let diff = check(&prev, IndexDelta::new(), dir.path());
        assert!(diff.segment.is_empty());
    }

    #[derive(Debug, Clone)]
    enum DeltaOp {
        /// Positional bump of the i-th chunk (sorted) of the session start.
        Cached(usize),
        /// Keyed bump of the i-th chunk of the session start.
        Keyed(usize),
        /// Keyed bump of the i-th new entry's chunk (a chunk new in this session).
        KeyedNew(usize),
        /// Keyed bump of a chunk in neither input: always stale.
        KeyedUnknown(u8),
        /// New entry with fresh ID `[b; 32]`, byte 30 = 0xEE: (b, refcount, pack, offset).
        NewFresh(u8, u32, u8, u64),
        /// New entry for the i-th chunk `moved` added (in prev, not in start), if any.
        NewCommittedElsewhere(usize, u32),
    }

    fn arb_delta_op() -> impl Strategy<Value = DeltaOp> {
        prop_oneof![
            6 => any::<usize>().prop_map(DeltaOp::Cached),
            3 => any::<usize>().prop_map(DeltaOp::Keyed),
            2 => any::<usize>().prop_map(DeltaOp::KeyedNew),
            1 => any::<u8>().prop_map(DeltaOp::KeyedUnknown),
            4 => (any::<u8>(), 1..4u32, any::<u8>(), 0..1000u64)
                .prop_map(|(b, rc, p, o)| DeltaOp::NewFresh(b, rc, p, o)),
            2 => (any::<usize>(), 1..4u32).prop_map(|(i, rc)| DeltaOp::NewCommittedElsewhere(i, rc)),
        ]
    }

    fn sorted_ids(index: &ChunkIndex) -> Vec<ChunkId> {
        sorted(index).into_iter().map(|e| e.0).collect()
    }

    /// `ops` recorded on a fresh delta whose positional bumps index `cache`
    /// (a table of the session start, whose chunks are `start_ids`).
    fn build_delta(
        cache: &Arc<ChunkTable>,
        start_ids: &[ChunkId],
        elsewhere: &[ChunkId],
        ops: &[DeltaOp],
    ) -> IndexDelta {
        let mut delta = IndexDelta::with_cached_bumps(Arc::clone(cache));
        let mut new_ids: Vec<ChunkId> = Vec::new();
        let pick = |ids: &[ChunkId], i: usize| (!ids.is_empty()).then(|| ids[i % ids.len()]);
        for op in ops {
            match *op {
                DeltaOp::Cached(i) => {
                    if let Some(id) = pick(start_ids, i) {
                        bump_by_position(&mut delta, id);
                    }
                }
                DeltaOp::Keyed(i) => {
                    if let Some(id) = pick(start_ids, i) {
                        delta.bump_refcount(&id);
                    }
                }
                DeltaOp::KeyedNew(i) => {
                    if let Some(id) = pick(&new_ids, i) {
                        delta.bump_refcount(&id);
                    }
                }
                DeltaOp::KeyedUnknown(b) => {
                    let mut bytes = [b; 32];
                    bytes[30] = 0xDD;
                    delta.bump_refcount(&ChunkId::from_bytes(bytes));
                }
                DeltaOp::NewFresh(b, rc, p, o) => {
                    let mut bytes = [b; 32];
                    bytes[30] = 0xEE;
                    let id = ChunkId::from_bytes(bytes);
                    new_entry(&mut delta, entry(id, rc, p, o));
                    new_ids.push(id);
                }
                DeltaOp::NewCommittedElsewhere(i, rc) => {
                    if let Some(id) = pick(elsewhere, i) {
                        new_entry(&mut delta, entry(id, rc, 0xCC, (i % 1000) as u64));
                        new_ids.push(id);
                    }
                }
            }
        }
        delta
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// Over a head that moved during the session, the merge errs as stale
        /// iff the keyed reconcile does, and otherwise writes the keyed result.
        #[test]
        fn merge_matches_keyed_reconcile_over_moved_heads(
            start in arb_index().prop_filter("non-empty start", |s| !s.is_empty()),
            moved in prop::collection::vec(arb_op(), 0..20),
            ops in prop::collection::vec(arb_delta_op(), 0..40),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let start_index = index_from(&start);
            let prev_index = mutate(&start_index, &moved, 0xAB);
            let start_ids = sorted_ids(&start_index);
            let elsewhere: Vec<ChunkId> = sorted_ids(&prev_index)
                .into_iter()
                .filter(|id| !start_index.contains(id))
                .collect();
            let cache = table_of(&start_index, dir.path());

            // `IndexDelta` is not `Clone`: build it once per side.
            let keyed = build_delta(&cache, &start_ids, &elsewhere, &ops)
                .reconcile(&prev_index)
                .map(|d| apply_keyed(&prev_index, d));
            let prev = write_cache(&prev_index, dir.path(), "prev");
            let delta = build_delta(&cache, &start_ids, &elsewhere, &ops);
            let merged = merge_delta_to_temp(&prev, &delta, &target(dir.path()));

            match (keyed, merged) {
                (
                    Err(VykarError::StaleChunksDuringCommit),
                    Err(VykarError::StaleChunksDuringCommit),
                ) => {}
                (Ok(expected), Ok((diff, tmp))) => {
                    let written = MmapFullIndexCache::open_verified(&tmp).unwrap();
                    prop_assert_eq!(
                        sorted(&chunk_index_from_full_cache(&written)),
                        sorted(&expected)
                    );
                    prop_assert_eq!(diff.state_hash_after, written.state_hash());
                    prop_assert_eq!(
                        Segment::decode(&diff.segment.encode()).unwrap(),
                        diff.segment.clone()
                    );
                    let (applied, hash, _) =
                        apply_to_temp(&prev, &[diff.step()], &target(dir.path())).unwrap();
                    prop_assert_eq!(hash, diff.state_hash_after);
                    prop_assert_eq!(
                        std::fs::read(&applied).unwrap(),
                        std::fs::read(&tmp).unwrap()
                    );
                }
                (keyed, merged) => prop_assert!(
                    false,
                    "keyed: {:?}, merged: {:?}",
                    keyed.map(|i| i.len()),
                    merged.map(|(d, _)| d.segment)
                ),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// apply_to_temp against the reference model, on adversarial chains
// ---------------------------------------------------------------------------

mod adversarial {
    use std::collections::{BTreeMap, BTreeSet};

    use super::model::{self, Reject};
    use super::*;

    /// Write `entries` (sorted, unique; refcount 0 allowed) as a full cache.
    fn write_entries(entries: &[FullCacheEntry], dir: &Path, name: &str) -> MmapFullIndexCache {
        let path = dir.join(name);
        let mut w = FullCacheWriter::new_beside(&path).unwrap();
        for e in entries {
            w.push(e).unwrap();
        }
        let (tmp, _, _) = w.finish(Some(1)).unwrap();
        tmp.persist(&path).unwrap();
        MmapFullIndexCache::open_verified(&path).unwrap()
    }

    /// Every entry of `cache`, all fields, refcount 0 included.
    fn entries_of(cache: &MmapFullIndexCache) -> Vec<FullCacheEntry> {
        (0..cache.entry_count() as usize)
            .map(|i| cache.entry_at(i))
            .collect()
    }

    #[derive(Debug, Clone)]
    enum DeltaKind {
        Small(i64),
        ToZero,
        ToMax,
    }

    #[derive(Debug, Clone)]
    enum NewId {
        Fresh([u8; 32]),
        /// A chunk an earlier layer removed.
        RemovedEarlier(usize),
    }

    /// One benign layer, by seeds resolved against the layer's input.
    #[derive(Debug, Clone)]
    struct LayerSpec {
        deltas: Vec<(usize, DeltaKind)>,
        /// (position, pack, offset, size)
        relocations: Vec<(usize, u8, u64, u32)>,
        /// (id, refcount 1..4, size, pack, offset)
        new_entries: Vec<(NewId, u32, u32, u8, u64)>,
    }

    /// One harmful change; the first field is the layer (modulo the layer count).
    #[derive(Debug, Clone)]
    enum Harm {
        /// delta = -(rc + 1)
        Underflow(usize, usize),
        /// delta = u32::MAX - rc + 1
        Overflow(usize, usize),
        /// Any non-zero delta.
        Extreme(usize, usize, i64),
        /// A new entry with an input entry's ID (first, middle or last by seed).
        NewInInput(usize, usize),
        /// A new entry re-adding a chunk this layer's delta removes.
        NewInRemovedThisLayer(usize),
        /// A relocation at a position this layer's delta removes.
        RelocateRemoved(usize),
        PrevHash(usize),
        /// Raise (true) or lower `prev_entries` by one.
        PrevEntries(usize, bool),
        HashAfter(usize),
        /// Raise (true) or lower `entries_after` by one.
        EntriesAfter(usize, bool),
    }

    impl Harm {
        fn layer(&self) -> usize {
            match *self {
                Harm::Underflow(l, _)
                | Harm::Overflow(l, _)
                | Harm::Extreme(l, _, _)
                | Harm::NewInInput(l, _)
                | Harm::NewInRemovedThisLayer(l)
                | Harm::RelocateRemoved(l)
                | Harm::PrevHash(l)
                | Harm::PrevEntries(l, _)
                | Harm::HashAfter(l)
                | Harm::EntriesAfter(l, _) => l,
            }
        }
    }

    fn arb_base() -> impl Strategy<Value = Vec<FullCacheEntry>> {
        let rc = prop_oneof![8 => 1..6u32, 1 => Just(0u32), 1 => (u32::MAX - 2)..=u32::MAX];
        prop::collection::btree_map(any::<[u8; 32]>(), (rc, any::<u8>(), 0..1000u64), 0..40)
            .prop_map(|m| {
                m.into_iter()
                    .map(|(b, (rc, p, o))| entry(ChunkId::from_bytes(b), rc, p, o))
                    .collect()
            })
    }

    fn arb_layer_spec() -> impl Strategy<Value = LayerSpec> {
        let kind = prop_oneof![
            6 => (-3i64..=3).prop_map(DeltaKind::Small),
            2 => Just(DeltaKind::ToZero),
            1 => Just(DeltaKind::ToMax),
        ];
        let new_id = prop_oneof![
            3 => any::<[u8; 32]>().prop_map(NewId::Fresh),
            1 => any::<usize>().prop_map(NewId::RemovedEarlier),
        ];
        (
            prop::collection::vec((any::<usize>(), kind), 0..12),
            prop::collection::vec(
                (any::<usize>(), any::<u8>(), any::<u64>(), any::<u32>()),
                0..4,
            ),
            prop::collection::vec(
                (new_id, 1..4u32, any::<u32>(), any::<u8>(), any::<u64>()),
                0..6,
            ),
        )
            .prop_map(|(deltas, relocations, new_entries)| LayerSpec {
                deltas,
                relocations,
                new_entries,
            })
    }

    fn arb_harm() -> impl Strategy<Value = Harm> {
        let l = 0..3usize;
        prop_oneof![
            (l.clone(), any::<usize>()).prop_map(|(l, s)| Harm::Underflow(l, s)),
            (l.clone(), any::<usize>()).prop_map(|(l, s)| Harm::Overflow(l, s)),
            (
                l.clone(),
                any::<usize>(),
                any::<i64>().prop_filter("non-zero", |v| *v != 0)
            )
                .prop_map(|(l, s, v)| Harm::Extreme(l, s, v)),
            (l.clone(), any::<usize>()).prop_map(|(l, s)| Harm::NewInInput(l, s)),
            l.clone().prop_map(Harm::NewInRemovedThisLayer),
            l.clone().prop_map(Harm::RelocateRemoved),
            l.clone().prop_map(Harm::PrevHash),
            (l.clone(), any::<bool>()).prop_map(|(l, r)| Harm::PrevEntries(l, r)),
            l.clone().prop_map(Harm::HashAfter),
            (l, any::<bool>()).prop_map(|(l, r)| Harm::EntriesAfter(l, r)),
        ]
    }

    fn rc_at(input: &[FullCacheEntry], pos: u32) -> i64 {
        i64::from(input[pos as usize].refcount)
    }

    /// The benign segment `spec` describes over `input`.
    fn layer_segment(input: &[FullCacheEntry], spec: &LayerSpec, removed: &[ChunkId]) -> Segment {
        let n = input.len();
        let max = i64::from(u32::MAX);
        let mut deltas: BTreeMap<u32, i64> = BTreeMap::new();
        let mut relocations: BTreeMap<u32, Relocation> = BTreeMap::new();
        if n > 0 {
            for (seed, kind) in &spec.deltas {
                let pos = (seed % n) as u32;
                if deltas.contains_key(&pos) {
                    continue;
                }
                let rc = rc_at(input, pos);
                let d = match *kind {
                    DeltaKind::Small(d) if rc + d < 0 => {
                        if rc == 0 {
                            1
                        } else {
                            -rc
                        }
                    }
                    DeltaKind::Small(d) if rc + d > max => max - rc,
                    DeltaKind::Small(d) => d,
                    DeltaKind::ToZero => -rc,
                    DeltaKind::ToMax => max - rc,
                };
                if d != 0 {
                    deltas.insert(pos, d);
                }
            }
            for &(seed, p, offset, size) in &spec.relocations {
                let pos = (seed % n) as u32;
                let removes = deltas.get(&pos).is_some_and(|d| rc_at(input, pos) + d == 0);
                if !removes {
                    relocations.entry(pos).or_insert(Relocation {
                        position: pos,
                        pack_id: pack(p),
                        pack_offset: offset,
                        stored_size: size,
                    });
                }
            }
        }
        let in_input: BTreeSet<[u8; 32]> = input.iter().map(|e| *e.chunk_id.as_bytes()).collect();
        let mut new_entries: BTreeMap<[u8; 32], FullCacheEntry> = BTreeMap::new();
        for (new_id, refcount, size, p, offset) in &spec.new_entries {
            let chunk = match new_id {
                NewId::Fresh(b) => ChunkId::from_bytes(*b),
                NewId::RemovedEarlier(_) if removed.is_empty() => continue,
                NewId::RemovedEarlier(i) => removed[i % removed.len()],
            };
            if in_input.contains(chunk.as_bytes()) {
                continue;
            }
            new_entries
                .entry(*chunk.as_bytes())
                .or_insert(FullCacheEntry {
                    chunk_id: chunk,
                    refcount: *refcount,
                    stored_size: *size,
                    pack_id: pack(*p),
                    pack_offset: *offset,
                });
        }
        Segment {
            prev_state_hash: model::state_hash(input),
            prev_entries: n as u64,
            deltas: deltas
                .into_iter()
                .map(|(position, delta)| RefcountDelta { position, delta })
                .collect(),
            relocations: relocations.into_values().collect(),
            new_entries: new_entries.into_values().collect(),
        }
    }

    fn set_delta(seg: &mut Segment, position: u32, delta: i64) {
        seg.deltas.retain(|d| d.position != position);
        seg.deltas.push(RefcountDelta { position, delta });
        seg.deltas.sort_by_key(|d| d.position);
    }

    fn add_new_entry(seg: &mut Segment, chunk: ChunkId) {
        seg.new_entries.retain(|e| e.chunk_id != chunk);
        seg.new_entries.push(entry(chunk, 1, 7, 0));
        seg.new_entries.sort_by_key(|e| *e.chunk_id.as_bytes());
    }

    /// A position this layer's delta removes, adding a delta to 0 if none does.
    fn removed_position(seg: &mut Segment, input: &[FullCacheEntry]) -> Option<u32> {
        if let Some(d) = seg
            .deltas
            .iter()
            .find(|d| rc_at(input, d.position) + d.delta == 0)
        {
            return Some(d.position);
        }
        let pos = input.iter().position(|e| e.refcount > 0)? as u32;
        set_delta(seg, pos, -rc_at(input, pos));
        seg.relocations.retain(|r| r.position != pos);
        Some(pos)
    }

    /// Harms to the layer's own ops, before its result is computed.
    fn harm_ops(seg: &mut Segment, input: &[FullCacheEntry], harm: &Harm) {
        let n = input.len();
        if n == 0 {
            return;
        }
        let max = i64::from(u32::MAX);
        match *harm {
            Harm::Underflow(_, s) => {
                let pos = (s % n) as u32;
                set_delta(seg, pos, -(rc_at(input, pos) + 1));
            }
            Harm::Overflow(_, s) => {
                let pos = (s % n) as u32;
                set_delta(seg, pos, max - rc_at(input, pos) + 1);
            }
            Harm::Extreme(_, s, v) => set_delta(seg, (s % n) as u32, v),
            Harm::NewInInput(_, s) => {
                let i = [0, n / 2, n - 1][s % 3];
                add_new_entry(seg, input[i].chunk_id);
            }
            Harm::NewInRemovedThisLayer(_) => {
                if let Some(pos) = removed_position(seg, input) {
                    add_new_entry(seg, input[pos as usize].chunk_id);
                }
            }
            Harm::RelocateRemoved(_) => {
                if let Some(pos) = removed_position(seg, input) {
                    seg.relocations.retain(|r| r.position != pos);
                    seg.relocations.push(Relocation {
                        position: pos,
                        pack_id: pack(0xEE),
                        pack_offset: 1,
                        stored_size: 1,
                    });
                    seg.relocations.sort_by_key(|r| r.position);
                }
            }
            _ => {}
        }
    }

    /// Harms to the chain: the segment's predecessor or the step's result.
    fn harm_chain(seg: &mut Segment, after: &mut (StateHash, u64), harm: &Harm) {
        match *harm {
            Harm::PrevHash(_) => seg.prev_state_hash[0] ^= 1,
            Harm::PrevEntries(_, true) => seg.prev_entries += 1,
            Harm::PrevEntries(_, false) if seg.prev_entries > 0 => {
                seg.prev_entries -= 1;
                let n = seg.prev_entries;
                seg.deltas.retain(|d| u64::from(d.position) < n);
                seg.relocations.retain(|r| u64::from(r.position) < n);
            }
            Harm::HashAfter(_) => after.0[0] ^= 1,
            Harm::EntriesAfter(_, true) => after.1 += 1,
            Harm::EntriesAfter(_, false) if after.1 > 0 => after.1 -= 1,
            _ => {}
        }
    }

    /// The segments and declared results of a chain over `base`, built
    /// layer by layer from the model's output, with `harm` in its layer.
    fn build_chain(
        base: &[FullCacheEntry],
        specs: &[LayerSpec],
        harm: Option<&Harm>,
    ) -> (Vec<Segment>, Vec<(StateHash, u64)>) {
        let mut input = base.to_vec();
        let mut seen: BTreeSet<[u8; 32]> = base.iter().map(|e| *e.chunk_id.as_bytes()).collect();
        let (mut segments, mut after) = (Vec::new(), Vec::new());
        for (k, spec) in specs.iter().enumerate() {
            let harm = harm.filter(|h| h.layer() % specs.len() == k);
            let present: BTreeSet<[u8; 32]> =
                input.iter().map(|e| *e.chunk_id.as_bytes()).collect();
            let removed: Vec<ChunkId> = seen
                .difference(&present)
                .map(|b| ChunkId::from_bytes(*b))
                .collect();

            let mut seg = layer_segment(&input, spec, &removed);
            if let Some(h) = harm {
                harm_ops(&mut seg, &input, h);
            }
            let result = model::apply_layer(&input, &seg, k);
            // A rejected layer declares its input state.
            let produced = result.as_deref().unwrap_or(&input);
            let mut declared = (model::state_hash(produced), produced.len() as u64);
            if let Some(h) = harm {
                harm_chain(&mut seg, &mut declared, h);
            }

            seg.validate().unwrap();
            let decoded = Segment::decode(&seg.encode()).unwrap();
            assert_eq!(decoded, seg);
            segments.push(decoded);
            after.push(declared);

            match result {
                Ok(out) => {
                    seen.extend(out.iter().map(|e| *e.chunk_id.as_bytes()));
                    input = out;
                }
                Err(_) => break,
            }
        }
        (segments, after)
    }

    fn steps<'a>(segments: &'a [Segment], after: &[(StateHash, u64)]) -> Vec<ChainStep<'a>> {
        segments
            .iter()
            .zip(after)
            .map(|(segment, &(state_hash_after, entries_after))| ChainStep {
                segment,
                state_hash_after,
                entries_after,
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// `apply_to_temp` succeeds iff the model does, with the same
        /// entries, and every failure is `InvalidFormat`.
        #[test]
        fn apply_agrees_with_the_model_on_adversarial_chains(
            base in arb_base(),
            specs in prop::collection::vec(arb_layer_spec(), 1..=3),
            harm in prop_oneof![3 => Just(None), 1 => arb_harm().prop_map(Some)],
        ) {
            let dir = tempfile::tempdir().unwrap();
            let base_cache = write_entries(&base, dir.path(), "base");
            let (segments, after) = build_chain(&base, &specs, harm.as_ref());
            let chain = steps(&segments, &after);

            let model = model::apply_chain(&base, &chain);
            if harm.is_none() {
                prop_assert!(model.is_ok(), "benign chain rejected: {:?}", model);
            }
            let real = apply_to_temp(&base_cache, &chain, &target(dir.path()));
            match (model, real) {
                (Ok(m), Ok((tmp, hash, n))) => {
                    let out = MmapFullIndexCache::open_verified(&tmp);
                    prop_assert!(out.is_some(), "output cache does not verify");
                    let out = out.unwrap();
                    prop_assert_eq!(entries_of(&out), m.clone());
                    prop_assert_eq!(hash, model::state_hash(&m));
                    prop_assert_eq!(n, m.len() as u64);
                }
                (Err(_), Err(e)) => {
                    prop_assert!(matches!(e, VykarError::InvalidFormat(_)), "{:?}", e);
                }
                (Ok(m), Err(e)) => {
                    prop_assert!(false, "model accepted ({} entries), apply failed: {}", m.len(), e);
                }
                (Err(r), Ok(_)) => prop_assert!(false, "model rejected ({:?}), apply succeeded", r),
            }
        }
    }

    /// The model rejects the hand-written bad segments above and agrees with
    /// `apply_to_temp` on good ones: neither vacuously permissive nor strict.
    #[test]
    fn model_rejects_the_hand_written_bad_segments() {
        // As in `negative_result_and_bad_ops_rejected`.
        let prev = [entry(id(1), 3, 1, 0), entry(id(3), 1, 1, 1)];
        let base = Segment::empty(model::state_hash(&prev), 2);
        let layer = |s: &Segment| model::apply_layer(&prev, s, 0);

        let mut s = base.clone();
        s.deltas.push(RefcountDelta {
            position: 0,
            delta: -4,
        });
        assert_eq!(layer(&s), Err(Reject::RefcountOutOfRange { step: 0 }));

        let mut s = base.clone();
        s.deltas.push(RefcountDelta {
            position: 1,
            delta: -1,
        });
        s.relocations.push(Relocation {
            position: 1,
            pack_id: pack(2),
            pack_offset: 0,
            stored_size: 1,
        });
        assert_eq!(layer(&s), Err(Reject::RelocationOfRemoved { step: 0 }));

        let mut s = base.clone();
        s.new_entries.push(entry(id(3), 1, 2, 0));
        assert_eq!(layer(&s), Err(Reject::NewEntryInPredecessor { step: 0 }));
        s.deltas.push(RefcountDelta {
            position: 1,
            delta: -1,
        });
        assert_eq!(layer(&s), Err(Reject::NewEntryInPredecessor { step: 0 }));

        let mut s = base.clone();
        s.prev_entries = 3;
        let step = ChainStep {
            segment: &s,
            state_hash_after: model::state_hash(&prev),
            entries_after: 2,
        };
        assert_eq!(
            model::apply_chain(&prev, &[step]),
            Err(Reject::ChainMismatch { step: 0 })
        );

        // As in `chain_mismatch_rejected`.
        let dir = tempfile::tempdir().unwrap();
        let prev = [entry(id(1), 1, 1, 0)];
        let prev_cache = write_cache(&index_from(&prev), dir.path(), "prev");
        let mut next = index_from(&prev);
        next.increment_refcount(&id(1));
        let diff = diff_checked(&prev_cache, &next, dir.path());
        assert_eq!(
            model::apply_chain(&prev, &[diff.step()]),
            Ok(vec![entry(id(1), 2, 1, 0)])
        );

        let mut wrong_after = diff.step();
        wrong_after.state_hash_after[0] ^= 1;
        assert_eq!(
            model::apply_chain(&prev, &[wrong_after]),
            Err(Reject::ResultMismatch { step: 0 })
        );
        let mut wrong_count = diff.step();
        wrong_count.entries_after += 1;
        assert_eq!(
            model::apply_chain(&prev, &[wrong_count]),
            Err(Reject::ResultMismatch { step: 0 })
        );
        assert_eq!(
            model::apply_chain(&prev, &[diff.step(), diff.step()]),
            Err(Reject::ChainMismatch { step: 1 })
        );
        let mut wrong_prev = diff.segment.clone();
        wrong_prev.prev_state_hash[0] ^= 1;
        let step = ChainStep {
            segment: &wrong_prev,
            ..diff.step()
        };
        assert_eq!(
            model::apply_chain(&prev, &[step]),
            Err(Reject::ChainMismatch { step: 0 })
        );

        // As in `removal_relocation_and_bump`: the model's result is apply's.
        let dir = tempfile::tempdir().unwrap();
        let prev = [
            entry(id(1), 2, 1, 0),
            entry(id(2), 1, 1, 100),
            entry(id(3), 5, 2, 0),
            entry(id(4), 1, 2, 100),
        ];
        let prev_cache = write_cache(&index_from(&prev), dir.path(), "prev");
        let mut next = index_from(&prev);
        next.decrement(&id(2));
        next.increment_refcount_by(&id(1), 3);
        next.update_location(&id(3), pack(9), 7, 42);
        next.add(id(5), 10, pack(9), 99);
        let diff = diff_checked(&prev_cache, &next, dir.path());
        let modelled = model::apply_chain(&prev, &[diff.step()]).unwrap();
        let (tmp, _, _) = apply_to_temp(&prev_cache, &[diff.step()], &target(dir.path())).unwrap();
        let out = MmapFullIndexCache::open_verified(&tmp).unwrap();
        assert_eq!(entries_of(&out), modelled);
        assert_eq!(modelled.len(), 4);
    }
}
