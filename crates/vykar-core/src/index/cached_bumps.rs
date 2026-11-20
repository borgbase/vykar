//! Positional refcount bumps for chunks found in the mmap'd dedup cache.
//!
//! On an incremental backup almost every chunk reference resolves to a chunk
//! that already exists in the repository, i.e. to an entry of the session's
//! [`MmapDedupCache`]. Recording those bumps in a `HashMap<ChunkId, u32>`
//! duplicates the cache's whole key set in anonymous memory (37 B per bucket,
//! up to two buckets per entry), which on a no-change backup of a 10M-chunk
//! repository is ~620 MB — far more than the dedup structures the tiered mode
//! exists to keep small. The binary search that proves the chunk exists
//! already knows the entry's position, so the bump is kept as a counter in a
//! zero-initialised `Vec<u32>` indexed by that position instead: 4 B per
//! repository chunk, and pages no bump touches are never faulted in.
//!
//! The counters are only meaningful together with the cache they index, so
//! [`CachedBumps`] holds the cache (shared with `TieredDedupIndex`) and
//! resolves positions back to chunk IDs wherever a keyed view is required —
//! reconcile and `apply_to` on the slow commit path. The fast-path merge
//! against the full index cache uses the counters directly after checking
//! that both caches agree on every bumped position (see
//! `dedup_cache::merge_full_index_cache`).

// `counts` has exactly `cache.entry_count()` elements; `bump` range-checks
// every position before it is stored or journaled, and `count_at` documents
// its bound like a slice index.
#![allow(clippy::indexing_slicing)]

use std::sync::Arc;

use vykar_types::chunk_id::ChunkId;

use super::dedup_cache::MmapDedupCache;

/// Refcount increments for chunks of one [`MmapDedupCache`], by entry position.
///
/// Logically equivalent to a `HashMap<ChunkId, u32>` whose keys are
/// `cache.chunk_id_at(position)`; every recorded position is checked against
/// the bumped chunk ID when it is recorded, so the equivalence does not depend
/// on where the caller got the position from.
pub struct CachedBumps {
    cache: Arc<MmapDedupCache>,
    /// `counts[p]` = bumps recorded for the entry at position `p`.
    /// Length is `cache.entry_count()`.
    counts: Vec<u32>,
    /// Sum of `counts`, so emptiness is O(1).
    total: u64,
    /// Positions bumped since the armed rollback checkpoint, one element per
    /// bump. `Some` ⟺ a checkpoint is armed. Rolling back decrements each
    /// one, so the cost is proportional to the work being undone rather than
    /// to the repository size.
    journal: Option<Vec<u32>>,
}

impl CachedBumps {
    pub fn new(cache: Arc<MmapDedupCache>) -> Self {
        // `vec![0; n]` allocates zeroed memory (calloc), so a full backup that
        // finds few existing chunks only faults in the pages it touches.
        let counts = vec![0u32; cache.entry_count() as usize];
        Self {
            cache,
            counts,
            total: 0,
            journal: None,
        }
    }

    /// Record one refcount bump for `id`, found at `position` in this cache.
    ///
    /// Returns `false` (recording nothing) when the entry at `position` is not
    /// `id`, e.g. because the position came from a different cache; the caller
    /// must then record the bump by key. The check reads the entry the
    /// caller's binary search just touched, so it costs one 32-byte compare.
    #[must_use]
    pub fn bump(&mut self, position: u32, id: &ChunkId) -> bool {
        if position >= self.cache.entry_count()
            || self.cache.chunk_id_bytes_at(position) != id.as_bytes().as_slice()
        {
            return false;
        }
        let slot = &mut self.counts[position as usize];
        *slot += 1;
        self.total += 1;
        if let Some(journal) = self.journal.as_mut() {
            journal.push(position);
        }
        true
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Arm a rollback checkpoint. Panics if one is already armed.
    pub fn checkpoint(&mut self) {
        assert!(
            self.journal.is_none(),
            "CachedBumps::checkpoint() called while another checkpoint is armed"
        );
        self.journal = Some(Vec::new());
    }

    /// Undo every bump recorded since `checkpoint()`. No-op when none is armed.
    pub fn rollback(&mut self) {
        if let Some(journal) = self.journal.take() {
            for position in journal {
                self.counts[position as usize] -= 1;
                self.total -= 1;
            }
        }
    }

    /// Keep the bumps recorded since `checkpoint()` and disarm it.
    pub fn discard_checkpoint(&mut self) {
        self.journal = None;
    }

    /// Number of positions (equal to the cache's entry count).
    pub fn len(&self) -> usize {
        self.counts.len()
    }

    /// Bump count recorded for `position`. Panics if `position >= len()`.
    pub fn count_at(&self, position: usize) -> u32 {
        self.counts[position]
    }

    /// The cache the positions refer to.
    pub fn cache(&self) -> &MmapDedupCache {
        &self.cache
    }

    /// Every `(chunk_id, count)` with a non-zero count, in cache order.
    ///
    /// Scans the whole counter array — meant for commit-time consumers, not
    /// for per-chunk use.
    pub fn iter_nonzero(&self) -> impl Iterator<Item = (ChunkId, u32)> + '_ {
        self.counts
            .iter()
            .enumerate()
            .filter(|&(_, &count)| count != 0)
            .map(|(position, &count)| (self.cache.chunk_id_at(position as u32), count))
    }
}

impl std::fmt::Debug for CachedBumps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedBumps")
            .field("positions", &self.counts.len())
            .field("total", &self.total)
            .field("checkpoint_armed", &self.journal.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    //! Positional recording must be indistinguishable from keyed recording:
    //! every test runs the same operations against an `IndexDelta` with
    //! positional bumps and one without, and compares the commit outputs
    //! (merged full index cache bytes, `apply_to`, `reconcile`).

    use std::path::Path;

    use proptest::prelude::*;

    use super::*;
    use crate::index::dedup_cache::{
        self, build_dedup_cache_to_path, build_full_index_cache_to_path, merge_full_index_cache,
        MmapFullIndexCache,
    };
    use crate::index::{ChunkIndex, IndexDelta};
    use vykar_types::pack_id::PackId;

    const GEN: u64 = 7;

    fn index_from(ids: &[(ChunkId, u32)]) -> ChunkIndex {
        let mut index = ChunkIndex::new();
        for (i, (id, rc)) in ids.iter().enumerate() {
            index.add(*id, 100 + i as u32, PackId::from_bytes([1; 32]), i as u64);
            if *rc > 1 {
                index.increment_refcount_by(id, rc - 1);
            }
        }
        index
    }

    fn open_dedup(index: &ChunkIndex, path: &Path) -> Arc<MmapDedupCache> {
        build_dedup_cache_to_path(index, GEN, path).unwrap();
        Arc::new(MmapDedupCache::open_path(path, GEN).unwrap())
    }

    /// Sorted `(id, refcount, stored_size, pack_id, pack_offset)` view.
    fn snapshot(index: &ChunkIndex) -> Vec<(ChunkId, u32, u32, PackId, u64)> {
        let mut v: Vec<_> = index
            .iter()
            .map(|(id, e)| (*id, e.refcount, e.stored_size, e.pack_id, e.pack_offset))
            .collect();
        v.sort_unstable_by_key(|e| *e.0.as_bytes());
        v
    }

    fn merged_bytes(old: &MmapFullIndexCache, delta: &IndexDelta, out: &Path) -> Vec<u8> {
        merge_full_index_cache(old, delta, GEN + 1, out).unwrap();
        std::fs::read(out).unwrap()
    }

    /// Apply the same bump to both deltas: positionally where the chunk is
    /// in the dedup cache, by key otherwise.
    fn bump_both(
        cache: &MmapDedupCache,
        positional: &mut IndexDelta,
        keyed: &mut IndexDelta,
        id: &ChunkId,
    ) {
        match cache.find_with_position(id) {
            Some((position, _)) => positional.bump_cached_refcount(id, position),
            None => positional.bump_refcount(id),
        }
        keyed.bump_refcount(id);
    }

    #[derive(Debug, Clone)]
    enum Op {
        /// Bump a repository chunk (prefilter hit / reuse_cached_chunk_ref).
        BumpExisting(usize),
        /// Bump a repository chunk by key (increment_chunk_ref) — may also
        /// carry a positional count for the same chunk.
        BumpExistingKeyed(usize),
        /// A chunk new to this session.
        NewEntry([u8; 32], u32),
        /// A new entry that duplicates a repository chunk.
        NewEntryExisting(usize, u32),
        /// Dedup hit on a chunk created earlier in this session.
        BumpSessionNew(usize),
        Checkpoint,
        Rollback,
        Commit,
    }

    fn arb_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            8 => any::<usize>().prop_map(Op::BumpExisting),
            1 => any::<usize>().prop_map(Op::BumpExistingKeyed),
            2 => (any::<[u8; 32]>(), 1..4u32).prop_map(|(b, rc)| Op::NewEntry(b, rc)),
            1 => (any::<usize>(), 1..4u32).prop_map(|(i, rc)| Op::NewEntryExisting(i, rc)),
            2 => any::<usize>().prop_map(Op::BumpSessionNew),
            2 => Just(Op::Checkpoint),
            1 => Just(Op::Rollback),
            1 => Just(Op::Commit),
        ]
    }

    /// Run `ops` against a positional and a keyed delta. Returns both.
    fn run_ops(
        cache: &Arc<MmapDedupCache>,
        repo_ids: &[(ChunkId, u32)],
        ops: &[Op],
        allow_overlap: bool,
    ) -> (IndexDelta, IndexDelta) {
        let mut positional = IndexDelta::with_cached_bumps(Arc::clone(cache));
        let mut keyed = IndexDelta::new();
        let mut checkpoints = None;
        let pack = PackId::from_bytes([2; 32]);
        for op in ops {
            match op {
                Op::BumpExisting(i) if !repo_ids.is_empty() => {
                    let id = repo_ids[i % repo_ids.len()].0;
                    bump_both(cache, &mut positional, &mut keyed, &id);
                }
                Op::BumpExistingKeyed(i) if !repo_ids.is_empty() => {
                    let id = repo_ids[i % repo_ids.len()].0;
                    positional.bump_refcount(&id);
                    keyed.bump_refcount(&id);
                }
                Op::NewEntry(bytes, rc) => {
                    let id = ChunkId::from_bytes(*bytes);
                    let off = positional.new_entries.len() as u64;
                    positional.add_new_entry(id, 50, pack, off, *rc);
                    keyed.add_new_entry(id, 50, pack, off, *rc);
                }
                Op::NewEntryExisting(i, rc) if allow_overlap && !repo_ids.is_empty() => {
                    let id = repo_ids[i % repo_ids.len()].0;
                    let off = positional.new_entries.len() as u64;
                    positional.add_new_entry(id, 50, pack, off, *rc);
                    keyed.add_new_entry(id, 50, pack, off, *rc);
                }
                Op::BumpSessionNew(i) if !keyed.new_entries.is_empty() => {
                    let id = keyed.new_entries[i % keyed.new_entries.len()].chunk_id;
                    bump_both(cache, &mut positional, &mut keyed, &id);
                }
                Op::Checkpoint if checkpoints.is_none() => {
                    checkpoints = Some((positional.checkpoint(), keyed.checkpoint()));
                }
                Op::Rollback => {
                    if let Some((p, k)) = checkpoints.take() {
                        positional.rollback(p);
                        keyed.rollback(k);
                    }
                }
                Op::Commit if checkpoints.is_some() => {
                    checkpoints = None;
                    positional.discard_checkpoint();
                    keyed.discard_checkpoint();
                }
                _ => {}
            }
            assert_eq!(positional.is_empty(), keyed.is_empty());
        }
        (positional, keyed)
    }

    fn arb_repo() -> impl Strategy<Value = Vec<(ChunkId, u32)>> {
        prop::collection::btree_map(any::<[u8; 32]>(), 1..5u32, 0..40).prop_map(|m| {
            m.into_iter()
                .map(|(b, rc)| (ChunkId::from_bytes(b), rc))
                .collect()
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]

        /// Fast-path merge, `apply_to`, and `reconcile` + `apply_to` produce
        /// identical results for positional and keyed recording over random
        /// bump / new-entry / checkpoint / rollback sequences.
        #[test]
        fn positional_equals_keyed(
            repo_ids in arb_repo(),
            ops in prop::collection::vec(arb_op(), 0..120),
            drop_from_fresh in any::<Option<usize>>(),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let index = index_from(&repo_ids);
            let cache = open_dedup(&index, &dir.path().join("dedup"));
            let full_path = dir.path().join("full");
            build_full_index_cache_to_path(&index, GEN, &full_path).unwrap();
            let full = MmapFullIndexCache::open_path(&full_path, GEN).unwrap();

            // Overlapping new entries only on the slow path: the fast-path
            // merge never sees them in practice (and writes duplicates then;
            // `merge_with_duplicate_ids_in_full_cache_matches_keyed` covers
            // the merge side of that case).
            let (positional, keyed) = run_ops(&cache, &repo_ids, &ops, false);
            prop_assert_eq!(
                merged_bytes(&full, &positional, &dir.path().join("m1")),
                merged_bytes(&full, &keyed, &dir.path().join("m2"))
            );

            let mut a = index.clone();
            let mut b = index.clone();
            positional.apply_to(&mut a);
            keyed.apply_to(&mut b);
            prop_assert_eq!(snapshot(&a), snapshot(&b));

            // Slow path, including overlaps and a chunk deleted concurrently.
            let (positional, keyed) = run_ops(&cache, &repo_ids, &ops, true);
            let mut fresh = index.clone();
            if let (Some(i), false) = (drop_from_fresh, repo_ids.is_empty()) {
                fresh.remove(&repo_ids[i % repo_ids.len()].0);
            }
            match (positional.reconcile(&fresh), keyed.reconcile(&fresh)) {
                (Ok(p), Ok(k)) => {
                    let mut a = fresh.clone();
                    let mut b = fresh.clone();
                    p.apply_to(&mut a);
                    k.apply_to(&mut b);
                    prop_assert_eq!(snapshot(&a), snapshot(&b));
                }
                (Err(_), Err(_)) => {}
                (p, k) => prop_assert!(false, "reconcile diverged: {:?} vs {:?}", p.is_ok(), k.is_ok()),
            }
        }

        /// Overlapping new entries in the fast-path merge: the merged bytes
        /// still match the keyed merge (which writes the duplicate and bumps
        /// both copies).
        #[test]
        fn merge_with_overlapping_new_entries_matches_keyed(
            repo_ids in arb_repo(),
            ops in prop::collection::vec(arb_op(), 0..80),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let index = index_from(&repo_ids);
            let cache = open_dedup(&index, &dir.path().join("dedup"));
            let full_path = dir.path().join("full");
            build_full_index_cache_to_path(&index, GEN, &full_path).unwrap();
            let full = MmapFullIndexCache::open_path(&full_path, GEN).unwrap();

            let (positional, keyed) = run_ops(&cache, &repo_ids, &ops, true);
            prop_assert_eq!(
                merged_bytes(&full, &positional, &dir.path().join("m1")),
                merged_bytes(&full, &keyed, &dir.path().join("m2"))
            );
        }

        /// When the full index cache no longer matches the dedup cache the
        /// positions came from (index changed between session start and
        /// commit), the merge falls back to keyed lookup and still matches.
        #[test]
        fn merge_against_different_full_cache_matches_keyed(
            repo_ids in arb_repo(),
            extra in prop::collection::btree_map(any::<[u8; 32]>(), 1..5u32, 0..5),
            remove in prop::collection::vec(any::<usize>(), 0..3),
            ops in prop::collection::vec(arb_op(), 0..80),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let index = index_from(&repo_ids);
            let cache = open_dedup(&index, &dir.path().join("dedup"));

            let mut other_ids = repo_ids.clone();
            for i in remove {
                if !other_ids.is_empty() {
                    let n = other_ids.len();
                    other_ids.remove(i % n);
                }
            }
            other_ids.extend(extra.into_iter().map(|(b, rc)| (ChunkId::from_bytes(b), rc)));
            let other = index_from(&other_ids);
            let full_path = dir.path().join("full");
            build_full_index_cache_to_path(&other, GEN, &full_path).unwrap();
            let full = MmapFullIndexCache::open_path(&full_path, GEN).unwrap();

            let (positional, keyed) = run_ops(&cache, &repo_ids, &ops, false);
            prop_assert_eq!(
                merged_bytes(&full, &positional, &dir.path().join("m1")),
                merged_bytes(&full, &keyed, &dir.path().join("m2"))
            );
        }
    }

    #[test]
    fn rollback_undoes_only_bumps_since_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<_> = (0u8..4)
            .map(|i| (ChunkId::from_bytes([i; 32]), 1))
            .collect();
        let cache = open_dedup(&index_from(&ids), &dir.path().join("dedup"));
        let mut bumps = CachedBumps::new(Arc::clone(&cache));

        assert!(bumps.bump(1, &ids[1].0));
        bumps.checkpoint();
        assert!(bumps.bump(1, &ids[1].0));
        assert!(bumps.bump(2, &ids[2].0));
        assert!(bumps.bump(2, &ids[2].0));
        bumps.rollback();
        assert_eq!(
            bumps.iter_nonzero().collect::<Vec<_>>(),
            vec![(ids[1].0, 1)]
        );

        bumps.checkpoint();
        assert!(bumps.bump(3, &ids[3].0));
        bumps.discard_checkpoint();
        // Rollback without an armed checkpoint is a no-op.
        bumps.rollback();
        assert_eq!(
            bumps.iter_nonzero().collect::<Vec<_>>(),
            vec![(ids[1].0, 1), (ids[3].0, 1)]
        );
        assert!(!bumps.is_empty());
    }

    #[test]
    fn mismatched_position_falls_back_to_key() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<_> = (0u8..3)
            .map(|i| (ChunkId::from_bytes([i; 32]), 1))
            .collect();
        let cache = open_dedup(&index_from(&ids), &dir.path().join("dedup"));
        let mut delta = IndexDelta::with_cached_bumps(Arc::clone(&cache));

        // Wrong position, out-of-range position: recorded by key instead.
        delta.bump_cached_refcount(&ids[0].0, 2);
        delta.bump_cached_refcount(&ids[1].0, 99);
        assert!(delta.cached_bumps.as_ref().unwrap().is_empty());
        assert_eq!(delta.refcount_bumps.get(&ids[0].0), Some(&1));
        assert_eq!(delta.refcount_bumps.get(&ids[1].0), Some(&1));
        assert!(!delta.is_empty());

        // Delta without a positional table records by key.
        let mut plain = IndexDelta::new();
        plain.bump_cached_refcount(&ids[2].0, 2);
        assert_eq!(plain.refcount_bumps.get(&ids[2].0), Some(&1));
    }

    #[test]
    fn find_with_position_matches_sorted_order() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<_> = [9u8, 3, 7, 1]
            .iter()
            .map(|&i| (ChunkId::from_bytes([i; 32]), 1))
            .collect();
        let cache = open_dedup(&index_from(&ids), &dir.path().join("dedup"));
        for (expected_pos, byte) in [1u8, 3, 7, 9].iter().enumerate() {
            let id = ChunkId::from_bytes([*byte; 32]);
            let (pos, _) = cache.find_with_position(&id).unwrap();
            assert_eq!(pos as usize, expected_pos);
            assert_eq!(cache.chunk_id_at(pos), id);
        }
        assert!(cache
            .find_with_position(&ChunkId::from_bytes([2; 32]))
            .is_none());
    }

    /// A full index cache with a duplicated chunk ID (left behind by an
    /// earlier fast-path merge whose new entry overlapped an old one) is not
    /// strictly sorted: a positional bump would reach only one of the two
    /// copies, whereas the keyed merge bumps both. The merge must detect this
    /// and fall back.
    #[test]
    fn merge_with_duplicate_ids_in_full_cache_matches_keyed() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<_> = (0u8..4)
            .map(|i| (ChunkId::from_bytes([i * 2; 32]), 1))
            .collect();
        let index = index_from(&ids);
        let full_path = dir.path().join("full_index_cache");
        build_full_index_cache_to_path(&index, GEN, &full_path).unwrap();

        // Produce a full cache with chunk 2 duplicated.
        let mut dup = IndexDelta::new();
        dup.add_new_entry(ids[1].0, 50, PackId::from_bytes([3; 32]), 0, 1);
        let dup_gen = GEN + 10;
        {
            let full = MmapFullIndexCache::open_path(&full_path, GEN).unwrap();
            let out = dir.path().join("dup");
            merge_full_index_cache(&full, &dup, dup_gen, &out).unwrap();
            drop(full);
            std::fs::rename(&out, &full_path).unwrap();
        }
        let full = MmapFullIndexCache::open_path(&full_path, dup_gen).unwrap();
        assert_eq!(full.entry_count(), 5);

        // Dedup cache derived from it: same order and count, so the cheap
        // up-front checks pass.
        let repo_id = [0x5a; 32];
        let cache_dir = dir.path().join("cache");
        dedup_cache::build_dedup_cache_from_full_cache(&full, &repo_id, Some(&cache_dir)).unwrap();
        let cache = Arc::new(MmapDedupCache::open(&repo_id, dup_gen, Some(&cache_dir)).unwrap());

        for target in [ids[1].0, ids[2].0, ids[0].0] {
            let mut positional = IndexDelta::with_cached_bumps(Arc::clone(&cache));
            let mut keyed = IndexDelta::new();
            bump_both(&cache, &mut positional, &mut keyed, &target);
            assert!(!positional.cached_bumps.as_ref().unwrap().is_empty());
            assert_eq!(
                merged_bytes(&full, &positional, &dir.path().join("m1")),
                merged_bytes(&full, &keyed, &dir.path().join("m2"))
            );
        }
    }
}
