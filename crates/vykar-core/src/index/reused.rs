//! The chunks a write session reused from the index, which its commit
//! re-checks when the head changed in a way that can have removed entries
//! (05 "Liveness"; `repo/commit.rs` `needs_recheck`). Presence only: how
//! often a chunk was reused is not recorded and nothing reads it.
//!
//! Reuse of the session's table is a bit per table position: on an
//! incremental backup almost every reference resolves to the table, and a
//! zero-initialised `Vec<u64>` costs one bit per repository chunk, with
//! pages no reuse touches never faulted in. The binary search that proves
//! the chunk exists already knows the position.

use std::collections::HashSet;
use std::sync::Arc;

use vykar_types::chunk_id::ChunkId;

use super::hasher::BuildChunkIdHasher;
use super::table::ChunkTable;

/// Reuse of one [`ChunkTable`], by entry position.
///
/// Logically a set of `table.chunk_id_at(position)`; every recorded position
/// is checked against the reused chunk ID when it is recorded, so the
/// equivalence does not depend on where the caller got the position from.
pub struct ReusedPositions {
    table: Arc<ChunkTable>,
    /// Bit `p` set: the entry at position `p` was reused. Length
    /// `table.len().div_ceil(64)`.
    bits: Vec<u64>,
    /// Number of set bits, so emptiness is O(1).
    set: u64,
    /// Positions whose bit was set (0 -> 1) since the armed checkpoint, one
    /// element each. `Some` <=> a checkpoint is armed. Rolling back clears
    /// each one, so the cost is proportional to the work being undone rather
    /// than to the repository size.
    journal: Option<Vec<u32>>,
}

impl ReusedPositions {
    pub fn new(table: Arc<ChunkTable>) -> Self {
        // `vec![0; n]` allocates zeroed memory (calloc), so a full backup that
        // finds few existing chunks only faults in the pages it touches.
        let bits = vec![0u64; table.len().div_ceil(64)];
        Self {
            table,
            bits,
            set: 0,
            journal: None,
        }
    }

    /// Record reuse of `id`, found at `position` in this table.
    ///
    /// Returns `false` (recording nothing) when the entry at `position` is not
    /// `id`, e.g. because the position came from a different table; the caller
    /// must then record it by key. The check reads the entry the caller's
    /// binary search just touched, so it costs one 32-byte compare.
    #[must_use]
    pub fn insert(&mut self, position: u32, id: &ChunkId) -> bool {
        if position as usize >= self.table.len()
            || self.table.chunk_id_bytes_at(position) != id.as_bytes().as_slice()
        {
            return false;
        }
        let mask = 1u64 << (position % 64);
        let Some(word) = self.bits.get_mut(position as usize / 64) else {
            return false;
        };
        if *word & mask == 0 {
            *word |= mask;
            self.set += 1;
            if let Some(journal) = self.journal.as_mut() {
                journal.push(position);
            }
        }
        true
    }

    /// Whether the entry at `position` was recorded.
    pub fn contains_position(&self, position: u32) -> bool {
        self.bits
            .get(position as usize / 64)
            .is_some_and(|w| w & (1u64 << (position % 64)) != 0)
    }

    pub fn is_empty(&self) -> bool {
        self.set == 0
    }

    /// The table the positions refer to.
    pub fn table(&self) -> &Arc<ChunkTable> {
        &self.table
    }

    /// The reused chunk IDs in table (chunk-ID) order. Scans the bitset word
    /// by word; meant for the commit check, not for per-chunk use.
    pub fn iter(&self) -> impl Iterator<Item = ChunkId> + '_ {
        self.bits.iter().enumerate().flat_map(move |(w, &word)| {
            let mut rest = word;
            std::iter::from_fn(move || {
                if rest == 0 {
                    return None;
                }
                let bit = rest.trailing_zeros();
                rest &= rest - 1;
                Some(self.table.chunk_id_at((w * 64) as u32 + bit))
            })
        })
    }

    /// Arm a rollback checkpoint. Panics if one is already armed.
    pub fn checkpoint(&mut self) {
        assert!(
            self.journal.is_none(),
            "ReusedPositions::checkpoint() called while another checkpoint is armed"
        );
        self.journal = Some(Vec::new());
    }

    /// Clear exactly the bits set since `checkpoint()`. No-op when none is
    /// armed.
    pub fn rollback(&mut self) {
        if let Some(journal) = self.journal.take() {
            for position in journal {
                if let Some(word) = self.bits.get_mut(position as usize / 64) {
                    *word &= !(1u64 << (position % 64));
                    self.set -= 1;
                }
            }
        }
    }

    /// Keep what was recorded since `checkpoint()` and disarm it.
    pub fn discard_checkpoint(&mut self) {
        self.journal = None;
    }
}

/// Everything a session reused: by key (chunks stored earlier in the
/// session, chunks of the view's applied segments, chunks found in an
/// edited index) and by table position.
#[derive(Default)]
pub struct ReusedChunks {
    keyed: HashSet<ChunkId, BuildChunkIdHasher>,
    positional: Option<ReusedPositions>,
    /// Keys inserted (absent before) since the armed checkpoint. `Some` <=>
    /// a checkpoint is armed.
    keyed_journal: Option<Vec<ChunkId>>,
}

impl ReusedChunks {
    /// A record that keeps reuse of `table` by position.
    pub fn with_table(table: Arc<ChunkTable>) -> Self {
        Self {
            positional: Some(ReusedPositions::new(table)),
            ..Self::default()
        }
    }

    /// Record reuse of `id` by key.
    pub fn insert_key(&mut self, id: &ChunkId) {
        if self.keyed.insert(*id)
            && let Some(journal) = self.keyed_journal.as_mut()
        {
            journal.push(*id);
        }
    }

    /// Record reuse of `id`, found at `position` in the session's table:
    /// positional when this record has a table and the position holds `id`,
    /// else by key. The recorded set is the same either way.
    pub fn insert_position(&mut self, id: &ChunkId, position: u32) {
        if let Some(positional) = self.positional.as_mut()
            && positional.insert(position, id)
        {
            return;
        }
        // Unreachable while positions come from lookups in the session's
        // table, the one this record indexes; the keyed fallback keeps the
        // set exact if that ever stops holding.
        self.insert_key(id);
    }

    pub fn is_empty(&self) -> bool {
        self.keyed.is_empty()
            && self
                .positional
                .as_ref()
                .is_none_or(ReusedPositions::is_empty)
    }

    /// Whether `id` was recorded, by key or at its table position (tests).
    pub fn contains(&self, id: &ChunkId) -> bool {
        self.keyed.contains(id)
            || self.positional.as_ref().is_some_and(|p| {
                p.table
                    .find(id)
                    .is_some_and(|(position, _)| p.contains_position(position))
            })
    }

    /// The positional part, when this record has a table.
    pub fn positional(&self) -> Option<&ReusedPositions> {
        self.positional.as_ref()
    }

    /// Every reused ID: keyed ones in unspecified order, then positional ones
    /// in table order. A chunk can appear twice; callers check membership,
    /// so duplicates are harmless.
    pub fn iter(&self) -> impl Iterator<Item = ChunkId> + '_ {
        self.keyed
            .iter()
            .copied()
            .chain(self.positional.iter().flat_map(ReusedPositions::iter))
    }

    /// Arm a rollback checkpoint. Panics if one is already armed.
    pub fn checkpoint(&mut self) {
        assert!(
            self.keyed_journal.is_none(),
            "ReusedChunks::checkpoint() called while another checkpoint is armed"
        );
        self.keyed_journal = Some(Vec::new());
        if let Some(positional) = self.positional.as_mut() {
            positional.checkpoint();
        }
    }

    /// Restore the set recorded at `checkpoint()`: what was first recorded
    /// since is cleared, what was recorded before stays (also when the scope
    /// reused it again). No-op when none is armed.
    pub fn rollback(&mut self) {
        if let Some(journal) = self.keyed_journal.take() {
            for id in journal {
                self.keyed.remove(&id);
            }
        }
        if let Some(positional) = self.positional.as_mut() {
            positional.rollback();
        }
    }

    /// Keep what was recorded since `checkpoint()` and disarm it.
    pub fn discard_checkpoint(&mut self) {
        self.keyed_journal = None;
        if let Some(positional) = self.positional.as_mut() {
            positional.discard_checkpoint();
        }
    }

    /// Whether `id` was recorded by key (tests).
    #[cfg(test)]
    pub(crate) fn is_keyed(&self, id: &ChunkId) -> bool {
        self.keyed.contains(id)
    }

    /// Whether a checkpoint is armed.
    pub fn is_armed(&self) -> bool {
        self.keyed_journal.is_some()
    }
}

impl std::fmt::Debug for ReusedChunks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReusedChunks")
            .field("keyed", &self.keyed.len())
            .field("positional", &self.positional.as_ref().map(|p| p.set))
            .field("checkpoint_armed", &self.keyed_journal.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    //! Positional recording must be indistinguishable from keyed recording:
    //! the proptest runs the same operations against a record with a table
    //! and one without, and compares what a commit reads (the reused set and
    //! the stale-chunk verdict).

    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::*;
    use crate::index::ChunkIndex;
    use vykar_types::pack_id::PackId;

    fn index_from(ids: &[ChunkId]) -> ChunkIndex {
        let mut index = ChunkIndex::new();
        for (i, id) in ids.iter().enumerate() {
            index.add(*id, 100 + i as u32, PackId::from_bytes([1; 32]), i as u64);
        }
        index
    }

    fn table_of(ids: &[ChunkId]) -> Arc<ChunkTable> {
        crate::testutil::base_table_of(&index_from(ids))
    }

    fn ids(bytes: &[u8]) -> Vec<ChunkId> {
        bytes
            .iter()
            .map(|&b| ChunkId::from_bytes([b; 32]))
            .collect()
    }

    fn set_of(record: &ReusedChunks) -> BTreeSet<[u8; 32]> {
        record.iter().map(|id| *id.as_bytes()).collect()
    }

    /// RU-1: a matching position sets the bit; a mismatching or
    /// out-of-range one records nothing positionally, and `ReusedChunks`
    /// then records it by key.
    #[test]
    fn insert_position_checks_the_id() {
        let ids = ids(&[0, 1, 2]);
        let table = table_of(&ids);
        let mut positions = ReusedPositions::new(Arc::clone(&table));
        assert!(positions.is_empty());
        assert!(!positions.insert(2, &ids[0]));
        assert!(!positions.insert(99, &ids[1]));
        assert!(positions.is_empty());
        assert!(positions.insert(1, &ids[1]));
        assert!(positions.contains_position(1));
        assert!(!positions.is_empty());

        let mut record = ReusedChunks::with_table(Arc::clone(&table));
        record.insert_position(&ids[0], 2);
        record.insert_position(&ids[1], 99);
        assert!(record.positional().unwrap().is_empty());
        assert!(record.contains(&ids[0]));
        assert!(record.contains(&ids[1]));
        assert!(!record.contains(&ids[2]));
        assert!(!record.is_empty());

        // A record without a table records by key.
        let mut plain = ReusedChunks::default();
        plain.insert_position(&ids[2], 2);
        assert!(plain.contains(&ids[2]));
    }

    /// RU-2: rollback clears exactly what the scope first recorded.
    #[test]
    fn rollback_restores_the_pre_checkpoint_set() {
        let ids = ids(&[0, 1, 2, 3, 4, 5]);
        let table = table_of(&ids[..4]);
        let mut record = ReusedChunks::with_table(Arc::clone(&table));

        record.insert_position(&ids[1], 1);
        record.insert_key(&ids[4]);
        record.checkpoint();
        record.insert_position(&ids[1], 1);
        record.insert_position(&ids[2], 2);
        record.insert_key(&ids[4]);
        record.insert_key(&ids[5]);
        record.rollback();
        assert!(!record.is_armed());
        assert_eq!(
            set_of(&record),
            [ids[1], ids[4]].iter().map(|i| *i.as_bytes()).collect()
        );

        record.checkpoint();
        record.insert_position(&ids[3], 3);
        record.insert_key(&ids[5]);
        record.discard_checkpoint();
        // Rollback without an armed checkpoint is a no-op.
        record.rollback();
        assert_eq!(
            set_of(&record),
            [ids[1], ids[3], ids[4], ids[5]]
                .iter()
                .map(|i| *i.as_bytes())
                .collect()
        );

        // A rolled-back scope that recorded the only reuse leaves it empty.
        let mut only = ReusedChunks::with_table(table);
        only.checkpoint();
        only.insert_position(&ids[0], 0);
        assert!(!only.is_empty());
        only.rollback();
        assert!(only.is_empty());
    }

    #[test]
    #[should_panic(expected = "checkpoint() called while another checkpoint is armed")]
    fn overlapping_checkpoints_panic() {
        let mut record = ReusedChunks::default();
        record.checkpoint();
        record.checkpoint();
    }

    /// RU-3: `iter` yields every recorded ID, positional ones in table order.
    #[test]
    fn iter_yields_positions_in_table_order() {
        let ids = ids(&[9, 3, 7, 1]);
        let table = table_of(&ids);
        let mut positions = ReusedPositions::new(Arc::clone(&table));
        for id in [&ids[0], &ids[2], &ids[3]] {
            let (position, _) = table.find(id).unwrap();
            assert!(positions.insert(position, id));
        }
        let got: Vec<ChunkId> = positions.iter().collect();
        assert_eq!(got, vec![ids[3], ids[2], ids[0]]);
    }

    /// A table with more than one bitset word.
    #[test]
    fn iter_crosses_words() {
        let ids: Vec<ChunkId> = (0u8..150).map(|b| ChunkId::from_bytes([b; 32])).collect();
        let table = table_of(&ids);
        let mut positions = ReusedPositions::new(Arc::clone(&table));
        for p in [0u32, 63, 64, 127, 128, 149] {
            assert!(positions.insert(p, &table.chunk_id_at(p)));
        }
        let got: Vec<ChunkId> = positions.iter().collect();
        let want: Vec<ChunkId> = [0u32, 63, 64, 127, 128, 149]
            .iter()
            .map(|&p| table.chunk_id_at(p))
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn table_positions_follow_sorted_order() {
        let ids = ids(&[9, 3, 7, 1]);
        let table = table_of(&ids);
        for (expected_pos, byte) in [1u8, 3, 7, 9].iter().enumerate() {
            let id = ChunkId::from_bytes([*byte; 32]);
            let (pos, _) = table.find(&id).unwrap();
            assert_eq!(pos as usize, expected_pos);
            assert_eq!(table.chunk_id_at(pos), id);
        }
        assert!(table.find(&ChunkId::from_bytes([2; 32])).is_none());
    }

    #[derive(Debug, Clone)]
    enum Op {
        /// Reuse a repository chunk (by position where it is in the table).
        ReuseExisting(usize),
        /// Reuse a repository chunk by key.
        ReuseExistingKeyed(usize),
        /// Reuse a chunk outside the table (session-new or segment addition).
        ReuseOther([u8; 32]),
        Checkpoint,
        Rollback,
        Commit,
    }

    fn arb_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            8 => any::<usize>().prop_map(Op::ReuseExisting),
            1 => any::<usize>().prop_map(Op::ReuseExistingKeyed),
            2 => any::<[u8; 32]>().prop_map(Op::ReuseOther),
            2 => Just(Op::Checkpoint),
            1 => Just(Op::Rollback),
            1 => Just(Op::Commit),
        ]
    }

    fn arb_repo() -> impl Strategy<Value = Vec<ChunkId>> {
        prop::collection::btree_set(any::<[u8; 32]>(), 0..40)
            .prop_map(|s| s.into_iter().map(ChunkId::from_bytes).collect())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]

        /// RU-4 and RU-5: over random reuse / checkpoint / rollback / commit
        /// sequences, a positional record, a keyed-only record and a
        /// `HashSet` model cloned at each checkpoint hold the same set after
        /// every step, and give the same stale-chunk verdict against an
        /// index that lost one of the chunks.
        #[test]
        fn positional_equals_keyed_equals_model(
            repo in arb_repo(),
            ops in prop::collection::vec(arb_op(), 0..120),
            drop in any::<Option<usize>>(),
        ) {
            let table = table_of(&repo);
            let mut positional = ReusedChunks::with_table(Arc::clone(&table));
            let mut keyed = ReusedChunks::default();
            let mut model: BTreeSet<[u8; 32]> = BTreeSet::new();
            let mut saved: Option<BTreeSet<[u8; 32]>> = None;
            for op in &ops {
                match op {
                    Op::ReuseExisting(i) if !repo.is_empty() => {
                        let id = repo[i % repo.len()];
                        let (position, _) = table.find(&id).unwrap();
                        positional.insert_position(&id, position);
                        keyed.insert_key(&id);
                        model.insert(*id.as_bytes());
                    }
                    Op::ReuseExistingKeyed(i) if !repo.is_empty() => {
                        let id = repo[i % repo.len()];
                        positional.insert_key(&id);
                        keyed.insert_key(&id);
                        model.insert(*id.as_bytes());
                    }
                    Op::ReuseOther(bytes) => {
                        let id = ChunkId::from_bytes(*bytes);
                        match table.find(&id) {
                            Some((position, _)) => positional.insert_position(&id, position),
                            None => positional.insert_key(&id),
                        }
                        keyed.insert_key(&id);
                        model.insert(*bytes);
                    }
                    Op::Checkpoint if saved.is_none() => {
                        positional.checkpoint();
                        keyed.checkpoint();
                        saved = Some(model.clone());
                    }
                    Op::Rollback => {
                        if let Some(s) = saved.take() {
                            positional.rollback();
                            keyed.rollback();
                            model = s;
                        }
                    }
                    Op::Commit if saved.is_some() => {
                        saved = None;
                        positional.discard_checkpoint();
                        keyed.discard_checkpoint();
                    }
                    _ => {}
                }
                prop_assert_eq!(set_of(&positional), model.clone());
                prop_assert_eq!(set_of(&keyed), model.clone());
                prop_assert_eq!(positional.is_empty(), model.is_empty());
                for b in &model {
                    prop_assert!(positional.contains(&ChunkId::from_bytes(*b)));
                }
            }

            let mut fresh = index_from(&repo);
            if let (Some(i), false) = (drop, repo.is_empty()) {
                fresh.remove(&repo[i % repo.len()]);
            }
            let stale = |r: &ReusedChunks| r.iter().any(|id| !fresh.contains(&id));
            prop_assert_eq!(stale(&positional), stale(&keyed));
        }
    }
}
