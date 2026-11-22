//! Positional refcount bumps for chunks found in the session's chunk table.
//!
//! On an incremental backup almost every chunk reference resolves to a chunk
//! that already exists in the repository, i.e. to an entry of the session's
//! [`ChunkTable`]. Recording those bumps in a `HashMap<ChunkId, u32>`
//! duplicates the table's whole key set in anonymous memory (37 B per bucket,
//! up to two buckets per entry), which on a no-change backup of a 10M-chunk
//! repository is ~620 MB. The binary search that proves the chunk exists
//! already knows the entry's position, so the bump is kept as a counter in a
//! zero-initialised `Vec<u32>` indexed by that position instead: 4 B per
//! repository chunk, and pages no bump touches are never faulted in.
//!
//! The counters are only meaningful together with the table they index, so
//! [`CachedBumps`] holds the table (shared with the session and the backup
//! workers' prefilter) and resolves positions back to chunk IDs, in table
//! order, wherever a keyed view is required: the commit's re-check of reused
//! chunks, `reconcile` and `apply_to`. Chunks added by the head's segments
//! (outside the table) are bumped by key.

// `counts` has exactly `table.len()` elements; `bump` range-checks
// every position before it is stored or journaled, and `count_at` documents
// its bound like a slice index.
#![allow(clippy::indexing_slicing)]

use std::sync::Arc;

use vykar_types::chunk_id::ChunkId;

use super::table::ChunkTable;

/// Refcount increments for chunks of one [`ChunkTable`], by entry position.
///
/// Logically equivalent to a `HashMap<ChunkId, u32>` whose keys are
/// `table.chunk_id_at(position)`; every recorded position is checked against
/// the bumped chunk ID when it is recorded, so the equivalence does not depend
/// on where the caller got the position from.
pub struct CachedBumps {
    cache: Arc<ChunkTable>,
    /// `counts[p]` = bumps recorded for the entry at position `p`.
    /// Length is `cache.len()`.
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
    pub fn new(cache: Arc<ChunkTable>) -> Self {
        // `vec![0; n]` allocates zeroed memory (calloc), so a full backup that
        // finds few existing chunks only faults in the pages it touches.
        let counts = vec![0u32; cache.len()];
        Self {
            cache,
            counts,
            total: 0,
            journal: None,
        }
    }

    /// Record one refcount bump for `id`, found at `position` in this table.
    ///
    /// Returns `false` (recording nothing) when the entry at `position` is not
    /// `id`, e.g. because the position came from a different table; the caller
    /// must then record the bump by key. The check reads the entry the
    /// caller's binary search just touched, so it costs one 32-byte compare.
    #[must_use]
    pub fn bump(&mut self, position: u32, id: &ChunkId) -> bool {
        if position as usize >= self.cache.len()
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

    /// Number of positions (equal to the table's entry count).
    pub fn len(&self) -> usize {
        self.counts.len()
    }

    /// Bump count recorded for `position`. Panics if `position >= len()`.
    pub fn count_at(&self, position: usize) -> u32 {
        self.counts[position]
    }

    /// The table the positions refer to.
    pub fn cache(&self) -> &ChunkTable {
        &self.cache
    }

    /// Every `(chunk_id, count)` with a non-zero count, in table order
    /// (chunk-ID order).
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
    //! positional bumps and one without, and compares what a commit reads
    //! (the logical bumps and new entries, `apply_to`, `reconcile`).

    use std::path::Path;

    use proptest::prelude::*;

    use super::*;
    use crate::index::{ChunkIndex, ChunkIndexEntry, IndexDelta, LiveIndex};
    use vykar_types::pack_id::PackId;

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

    fn open_table(index: &ChunkIndex, _dir: &Path, _name: &str) -> Arc<ChunkTable> {
        crate::testutil::base_table_of(index)
    }

    fn entries(index: &LiveIndex) -> Vec<(ChunkId, ChunkIndexEntry)> {
        index.iter().collect()
    }

    /// What a commit reads from `delta`: the logical bump of every chunk
    /// (keyed plus positional) and the new entries.
    #[allow(clippy::type_complexity)]
    fn logical(
        delta: &IndexDelta,
    ) -> (
        std::collections::BTreeMap<[u8; 32], u32>,
        Vec<(ChunkId, u32, u64, u32)>,
    ) {
        let mut bumps = std::collections::BTreeMap::new();
        for (id, n) in &delta.refcount_bumps {
            if *n > 0 {
                *bumps.entry(*id.as_bytes()).or_insert(0) += n;
            }
        }
        if let Some(cached) = delta.cached_bumps.as_ref() {
            for (id, n) in cached.iter_nonzero() {
                *bumps.entry(*id.as_bytes()).or_insert(0) += n;
            }
        }
        let new = delta
            .new_entries
            .iter()
            .map(|e| (e.chunk_id, e.stored_size, e.pack_offset, e.refcount))
            .collect();
        (bumps, new)
    }

    /// Apply the same bump to both deltas: positionally where the chunk is
    /// in the table, by key otherwise.
    fn bump_both(
        table: &ChunkTable,
        positional: &mut IndexDelta,
        keyed: &mut IndexDelta,
        id: &ChunkId,
    ) {
        match table.find(id) {
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
        table: &Arc<ChunkTable>,
        repo_ids: &[(ChunkId, u32)],
        ops: &[Op],
        allow_overlap: bool,
    ) -> (IndexDelta, IndexDelta) {
        let mut positional = IndexDelta::with_cached_bumps(Arc::clone(table));
        let mut keyed = IndexDelta::new();
        let mut checkpoints = None;
        let pack = PackId::from_bytes([2; 32]);
        for op in ops {
            match op {
                Op::BumpExisting(i) if !repo_ids.is_empty() => {
                    let id = repo_ids[i % repo_ids.len()].0;
                    bump_both(table, &mut positional, &mut keyed, &id);
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
                    bump_both(table, &mut positional, &mut keyed, &id);
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

        /// The logical bumps, `apply_to`, and `reconcile` + `apply_to`
        /// produce identical results for positional and keyed recording over
        /// random bump / new-entry / checkpoint / rollback sequences.
        #[test]
        fn positional_equals_keyed(
            repo_ids in arb_repo(),
            ops in prop::collection::vec(arb_op(), 0..120),
            drop_from_fresh in any::<Option<usize>>(),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let index = index_from(&repo_ids);
            let table = open_table(&index, dir.path(), "table");

            let (positional, keyed) = run_ops(&table, &repo_ids, &ops, true);
            prop_assert_eq!(logical(&positional), logical(&keyed));

            let mut a = LiveIndex::from_table(Arc::clone(&table));
            let mut b = LiveIndex::from_table(Arc::clone(&table));
            positional.apply_to(&mut a);
            keyed.apply_to(&mut b);
            prop_assert_eq!(entries(&a), entries(&b));

            // Reconcile, including overlaps and a chunk deleted concurrently.
            let (positional, keyed) = run_ops(&table, &repo_ids, &ops, true);
            let mut fresh = index.clone();
            if let (Some(i), false) = (drop_from_fresh, repo_ids.is_empty()) {
                fresh.remove(&repo_ids[i % repo_ids.len()].0);
            }
            let fresh_table = open_table(&fresh, dir.path(), "fresh");
            match (positional.reconcile(&fresh), keyed.reconcile(&fresh)) {
                (Ok(p), Ok(k)) => {
                    let mut a = LiveIndex::from_table(Arc::clone(&fresh_table));
                    let mut b = LiveIndex::from_table(Arc::clone(&fresh_table));
                    p.apply_to(&mut a);
                    k.apply_to(&mut b);
                    prop_assert_eq!(entries(&a), entries(&b));
                }
                (Err(_), Err(_)) => {}
                (p, k) => prop_assert!(false, "reconcile diverged: {:?} vs {:?}", p.is_ok(), k.is_ok()),
            }
        }
    }

    #[test]
    fn rollback_undoes_only_bumps_since_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<_> = (0u8..4)
            .map(|i| (ChunkId::from_bytes([i; 32]), 1))
            .collect();
        let table = open_table(&index_from(&ids), dir.path(), "table");
        let mut bumps = CachedBumps::new(Arc::clone(&table));

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
        let table = open_table(&index_from(&ids), dir.path(), "table");
        let mut delta = IndexDelta::with_cached_bumps(Arc::clone(&table));

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
    fn table_positions_follow_sorted_order() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<_> = [9u8, 3, 7, 1]
            .iter()
            .map(|&i| (ChunkId::from_bytes([i; 32]), 1))
            .collect();
        let table = open_table(&index_from(&ids), dir.path(), "table");
        for (expected_pos, byte) in [1u8, 3, 7, 9].iter().enumerate() {
            let id = ChunkId::from_bytes([*byte; 32]);
            let (pos, _) = table.find(&id).unwrap();
            assert_eq!(pos as usize, expected_pos);
            assert_eq!(table.chunk_id_at(pos), id);
        }
        assert!(table.find(&ChunkId::from_bytes([2; 32])).is_none());
    }
}
