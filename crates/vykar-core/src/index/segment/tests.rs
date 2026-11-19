use std::path::{Path, PathBuf};

use proptest::prelude::*;

use super::*;
use crate::index::dedup_cache::{
    build_full_index_cache_to_path, chunk_index_from_full_cache, generation_for_state,
};

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
    use crate::index::dedup_cache::{build_dedup_cache_to_path, MmapDedupCache};
    use crate::index::IndexDelta;

    /// A delta whose positional bumps index a dedup cache of `session_start`.
    fn delta_over(session_start: &ChunkIndex, dir: &Path) -> IndexDelta {
        let path = dir.join("dedup");
        build_dedup_cache_to_path(session_start, 1, &path).unwrap();
        IndexDelta::with_cached_bumps(Arc::new(MmapDedupCache::open_path(&path, 1).unwrap()))
    }

    fn bump_by_position(delta: &mut IndexDelta, chunk: ChunkId) {
        let (position, _) = delta
            .cached_bumps
            .as_ref()
            .unwrap()
            .cache()
            .find_with_position(&chunk)
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
        let mut index = prev.clone();
        delta.reconcile(prev).unwrap().apply_to(&mut index);
        index
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

        let diff = check(&prev, delta, dir.path());
        assert_eq!(
            diff.segment.deltas,
            vec![
                RefcountDelta {
                    position: 0,
                    delta: 2
                },
                RefcountDelta {
                    position: 4,
                    delta: 2
                },
            ]
        );
        assert_eq!(diff.segment.new_entries.len(), 2);
        assert_eq!(diff.segment.new_entries[0].refcount, 2);
    }

    /// The head moved during the session: positions index the session's
    /// dedup cache, not `prev`, and a new entry another client committed
    /// first becomes a bump.
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
        assert_eq!(diff.segment.deltas.len(), 3);
    }

    /// Duplicate new entries are summed into the first one's location.
    #[test]
    fn duplicate_new_entries() {
        let dir = tempfile::tempdir().unwrap();
        let prev = index_from(&base_entries());
        let mut delta = IndexDelta::new();
        new_entry(&mut delta, entry(id(12), 1, 3, 0));
        new_entry(&mut delta, entry(id(12), 2, 4, 50));
        let prev_cache = write_cache(&prev, dir.path(), "prev");
        let (diff, _) = merge_delta_to_temp(&prev_cache, &delta, &target(dir.path())).unwrap();
        assert_eq!(diff.segment.new_entries, vec![entry(id(12), 3, 3, 0)]);
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

    /// Refcounts beyond u32 are an error, not a wrap.
    #[test]
    fn refcount_overflow_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let prev = write_cache(
            &index_from(&[entry(id(1), u32::MAX, 1, 0)]),
            dir.path(),
            "prev",
        );
        let mut delta = IndexDelta::new();
        delta.bump_refcount(&id(1));
        assert!(merge_delta_to_temp(&prev, &delta, &target(dir.path())).is_err());
    }

    /// An empty delta changes nothing.
    #[test]
    fn empty_delta_is_an_empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let prev = index_from(&base_entries());
        let diff = check(&prev, IndexDelta::new(), dir.path());
        assert!(diff.segment.is_empty());
    }
}
