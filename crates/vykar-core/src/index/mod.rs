pub mod base_v2;
pub mod dedup_cache;
pub mod hasher;
pub mod head;
pub mod legacy;
pub mod reused;
pub mod segment_v2;
pub mod sweep_record;
pub mod table;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use hasher::{BuildChunkIdHasher, ChunkIdHashMap};
pub use legacy::{IndexBlob, LegacyChunkIndex, LegacyIndexEntry};
pub use reused::{ReusedChunks, ReusedPositions};
pub use table::{ChunkTable, LiveIndex};

use serde::{Deserialize, Serialize};

use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;

/// An in-memory map of chunks to their locations (tests, oracles, the
/// rebuild and the upgrade). Never serialized: the legacy wire type is
/// [`LegacyChunkIndex`].
#[derive(Debug, Clone, Default)]
pub struct ChunkIndex {
    entries: ChunkIdHashMap<ChunkIndexEntry>,
}

/// Where a chunk is stored. In-memory only: never serialized (the legacy
/// wire type is [`LegacyIndexEntry`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkIndexEntry {
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
}

impl ChunkIndex {
    pub fn new() -> Self {
        Self {
            entries: ChunkIdHashMap::default(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: ChunkIdHashMap::with_capacity_and_hasher(
                capacity,
                BuildChunkIdHasher::default(),
            ),
        }
    }

    /// Returns `true` if this chunk already exists (dedup hit).
    pub fn contains(&self, id: &ChunkId) -> bool {
        self.entries.contains_key(id)
    }

    /// Add a chunk with its pack location; a present chunk keeps its entry.
    pub fn add(&mut self, id: ChunkId, stored_size: u32, pack_id: PackId, pack_offset: u64) {
        self.entries.entry(id).or_insert(ChunkIndexEntry {
            stored_size,
            pack_id,
            pack_offset,
        });
    }

    pub fn get(&self, id: &ChunkId) -> Option<&ChunkIndexEntry> {
        self.entries.get(id)
    }

    /// Insert or replace the entry of `id` as given.
    pub(crate) fn insert_entry(&mut self, id: ChunkId, entry: ChunkIndexEntry) {
        self.entries.insert(id, entry);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ChunkId, &ChunkIndexEntry)> {
        self.entries.iter()
    }

    /// Update the storage location of an existing chunk (used by compact).
    /// Returns `true` if the chunk was found and updated.
    pub fn update_location(
        &mut self,
        id: &ChunkId,
        pack_id: PackId,
        pack_offset: u64,
        stored_size: u32,
    ) -> bool {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.pack_id = pack_id;
            entry.pack_offset = pack_offset;
            entry.stored_size = stored_size;
            true
        } else {
            false
        }
    }

    /// Count distinct pack IDs across all entries.
    pub fn count_distinct_packs(&self) -> usize {
        // Insert one by one rather than `collect()`: collecting pre-sizes the
        // set to the *chunk* count (the iterator's exact size hint), while it
        // only ever holds the pack count — hundreds of times fewer.
        let mut packs: HashSet<PackId> = HashSet::new();
        for entry in self.entries.values() {
            packs.insert(entry.pack_id);
        }
        packs.len()
    }

    /// Remove a single chunk entry. Returns the removed entry if found.
    pub fn remove(&mut self, id: &ChunkId) -> Option<ChunkIndexEntry> {
        self.entries.remove(id)
    }

    /// Remove all entries referencing the given pack. Returns the number of entries removed.
    pub fn remove_by_pack(&mut self, pack_id: &PackId) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, entry| entry.pack_id != *pack_id);
        before - self.entries.len()
    }
}

// --- Pending index journal types (for interrupted backup recovery) ---

/// A single chunk's location within a pending pack.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingChunkEntry {
    pub chunk_id: ChunkId,
    pub stored_size: u32,
    pub pack_offset: u64,
}

/// All chunks belonging to a single pack in the pending index journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingPackEntry {
    pub pack_id: PackId,
    pub chunks: Vec<PendingChunkEntry>,
}

/// In-memory journal of pack→chunk mappings for packs flushed during an
/// incomplete backup session. Keyed by `PackId` to prevent duplicate growth
/// across repeated interruption/recovery cycles.
///
/// Serialized as `Vec<PendingPackEntry>` on the wire (zstd-compressed, encrypted).
#[derive(Debug, Default)]
pub struct PendingIndexJournal {
    packs: HashMap<PackId, PendingPackEntry>,
    /// Bumped on every mutation; not serialized. Used to skip rewriting an
    /// unchanged journal (length alone is not enough: removal + insertion
    /// leaves it unchanged).
    revision: u64,
}

impl PendingIndexJournal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.packs.is_empty()
    }

    /// Number of packs recorded in this journal.
    pub fn len(&self) -> usize {
        self.packs.len()
    }

    /// Remove a pack from the journal.
    pub fn remove_pack(&mut self, pack_id: &PackId) {
        self.packs.remove(pack_id);
        self.revision += 1;
    }

    /// Mutation counter, for dirty tracking by journal writers.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Record a pack and its chunk entries. Replaces any previous entry for
    /// the same `pack_id` (idempotent for recovery seeding).
    pub fn record_pack(&mut self, pack_id: PackId, chunks: Vec<PendingChunkEntry>) {
        self.packs
            .insert(pack_id, PendingPackEntry { pack_id, chunks });
        self.revision += 1;
    }

    /// Serialize to the wire format (`Vec<PendingPackEntry>`).
    pub fn to_wire(&self) -> Vec<PendingPackEntry> {
        self.packs.values().cloned().collect()
    }

    /// Deserialize from the wire format.
    pub fn from_wire(entries: Vec<PendingPackEntry>) -> Self {
        let packs = entries.into_iter().map(|e| (e.pack_id, e)).collect();
        Self { packs, revision: 0 }
    }
}

/// Wire version written by [`encode_pending_journal`]. Version 2 journals
/// list only packs whose upload was confirmed (atomic publish), so recovery
/// can trust them after a size check. Unversioned (legacy) journals were
/// written before that guarantee and are discarded.
pub const PENDING_JOURNAL_VERSION: u8 = 2;

/// A decoded `pending_index` journal, tagged by wire version.
#[derive(Debug)]
pub enum DecodedJournal {
    /// Bare `Vec<PendingPackEntry>` from binaries before the version marker.
    Legacy,
    V2(Vec<PendingPackEntry>),
    /// `[v, ...]` with a version this binary does not understand.
    Unknown(u8),
}

/// Serialize journal entries as the positional tuple
/// `(PENDING_JOURNAL_VERSION, entries)`, i.e. msgpack `[2, [...]]`.
///
/// An older binary expects a bare array of entries; this shape fails its
/// decode, so it treats the journal as corrupt and re-uploads the chunks.
pub fn encode_pending_journal(
    entries: &[PendingPackEntry],
) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec(&(PENDING_JOURNAL_VERSION, entries))
}

/// Decode a journal written by any version. `Err` means neither the
/// versioned nor the legacy shape matched.
///
/// A versioned journal starts with a msgpack fixarray header followed by a
/// positive-fixint version. A legacy journal is an array of entry arrays, so
/// its second byte is an array header, never a fixint. Only the header is
/// inspected, so a future version with a different body shape is still
/// recognised as versioned.
pub fn decode_pending_journal(bytes: &[u8]) -> Result<DecodedJournal, rmp_serde::decode::Error> {
    if let [0x91..=0x9f, version @ 0x00..=0x7f, ..] = *bytes {
        if version != PENDING_JOURNAL_VERSION {
            return Ok(DecodedJournal::Unknown(version));
        }
        let (_, entries): (u8, Vec<PendingPackEntry>) = rmp_serde::from_slice(bytes)?;
        return Ok(DecodedJournal::V2(entries));
    }
    rmp_serde::from_slice::<Vec<PendingPackEntry>>(bytes).map(|_| DecodedJournal::Legacy)
}

/// Lightweight entry for recovered chunks (from a previous interrupted session).
/// Lives in `Repo::recovered_chunks` until promoted into the session on
/// a dedup hit.
#[derive(Debug, Clone)]
pub struct RecoveredChunkEntry {
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
}

/// Records the index mutations of a write session: the entries it added and
/// the chunks it reused.
///
/// A backup commit re-checks the reused chunks when needed and writes the
/// additions as a backup segment; other writers apply the additions to the
/// repository's `LiveIndex` at save time.
#[derive(Debug, Default)]
pub struct IndexDelta {
    /// New chunk entries added during this session.
    pub new_entries: Vec<NewChunkEntry>,
    /// The chunks this session reused from the index (presence only).
    pub reused: ReusedChunks,
    /// `trailer_len` of every pack the session sealed or recovered, filled
    /// when the delta is taken from the session.
    pub pack_trailers: HashMap<PackId, u32>,
}

/// A new chunk entry recorded during a write session.
#[derive(Debug, Clone)]
pub struct NewChunkEntry {
    pub chunk_id: ChunkId,
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
}

impl IndexDelta {
    pub fn new() -> Self {
        Self::default()
    }

    /// A delta that records reuse of `table` by position.
    pub fn with_table(table: Arc<ChunkTable>) -> Self {
        Self {
            reused: ReusedChunks::with_table(table),
            ..Self::default()
        }
    }

    /// Returns true if this delta records neither an addition nor a reuse.
    pub fn is_empty(&self) -> bool {
        self.new_entries.is_empty() && self.reused.is_empty()
    }

    /// Record a new chunk entry.
    pub fn add_new_entry(
        &mut self,
        chunk_id: ChunkId,
        stored_size: u32,
        pack_id: PackId,
        pack_offset: u64,
    ) {
        self.new_entries.push(NewChunkEntry {
            chunk_id,
            stored_size,
            pack_id,
            pack_offset,
        });
    }

    /// Apply this delta to the repository's index (the path of sessions
    /// that are not a backup commit). A new entry is added only when the
    /// index does not hold the chunk. Reuse is not persisted, so `reused`
    /// is ignored.
    pub fn apply_to(self, index: &mut LiveIndex) {
        for (pack, trailer_len) in self.pack_trailers {
            index.set_pack_trailer(pack, trailer_len);
        }
        for entry in self.new_entries {
            if !index.contains(&entry.chunk_id) {
                index.add(
                    entry.chunk_id,
                    entry.stored_size,
                    entry.pack_id,
                    entry.pack_offset,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_chunk_id(byte: u8) -> ChunkId {
        ChunkId::from_bytes([byte; 32])
    }

    fn make_pack_id(byte: u8) -> PackId {
        PackId::from_bytes([byte; 32])
    }

    #[test]
    fn pending_journal_round_trip() {
        let mut journal = PendingIndexJournal::new();
        assert!(journal.is_empty());
        assert_eq!(journal.len(), 0);

        let pack1 = make_pack_id(1);
        let pack2 = make_pack_id(2);

        journal.record_pack(
            pack1,
            vec![
                PendingChunkEntry {
                    chunk_id: make_chunk_id(10),
                    stored_size: 100,
                    pack_offset: 0,
                },
                PendingChunkEntry {
                    chunk_id: make_chunk_id(11),
                    stored_size: 200,
                    pack_offset: 100,
                },
            ],
        );
        journal.record_pack(
            pack2,
            vec![PendingChunkEntry {
                chunk_id: make_chunk_id(20),
                stored_size: 300,
                pack_offset: 0,
            }],
        );

        assert_eq!(journal.len(), 2);
        assert!(!journal.is_empty());

        // Serialize → deserialize round-trip
        let wire = journal.to_wire();
        let serialized = rmp_serde::to_vec(&wire).unwrap();
        let deserialized: Vec<PendingPackEntry> = rmp_serde::from_slice(&serialized).unwrap();
        let restored = PendingIndexJournal::from_wire(deserialized);

        assert_eq!(restored.len(), 2);
        let restored_wire = restored.to_wire();

        // Both packs present (order may differ)
        let mut pack_ids: Vec<PackId> = restored_wire.iter().map(|e| e.pack_id).collect();
        pack_ids.sort_by_key(|p| *p.as_bytes());
        assert_eq!(pack_ids, vec![pack1, pack2]);
    }

    fn sample_entries() -> Vec<PendingPackEntry> {
        vec![PendingPackEntry {
            pack_id: make_pack_id(1),
            chunks: vec![PendingChunkEntry {
                chunk_id: make_chunk_id(10),
                stored_size: 100,
                pack_offset: 13,
            }],
        }]
    }

    #[test]
    fn pending_journal_v2_round_trip() {
        let bytes = encode_pending_journal(&sample_entries()).unwrap();
        // msgpack: fixarray(2), positive fixint 2, ...
        assert_eq!(&bytes[..2], &[0x92, 0x02]);
        match decode_pending_journal(&bytes).unwrap() {
            DecodedJournal::V2(entries) => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].pack_id, make_pack_id(1));
            }
            other => panic!("expected V2, got {other:?}"),
        }
    }

    #[test]
    fn pending_journal_legacy_decodes_as_legacy() {
        // 16 entries switch to the array16 header (0xdc).
        for n in [0, 1, 2, 3, 16] {
            let entries: Vec<PendingPackEntry> = (0..n)
                .map(|i| PendingPackEntry {
                    pack_id: make_pack_id(i),
                    chunks: Vec::new(),
                })
                .collect();
            let bytes = rmp_serde::to_vec(&entries).unwrap();
            match decode_pending_journal(&bytes).unwrap() {
                DecodedJournal::Legacy => {}
                other => panic!("expected Legacy for {n} entries, got {other:?}"),
            }
        }
    }

    #[test]
    fn pending_journal_unknown_version_is_not_decoded() {
        let bytes = rmp_serde::to_vec(&(3u8, sample_entries())).unwrap();
        assert!(matches!(
            decode_pending_journal(&bytes).unwrap(),
            DecodedJournal::Unknown(3)
        ));
        // A future body shape is still recognised as versioned.
        let bytes = rmp_serde::to_vec(&(7u8, "something else", 42u32)).unwrap();
        assert!(matches!(
            decode_pending_journal(&bytes).unwrap(),
            DecodedJournal::Unknown(7)
        ));
    }

    #[test]
    fn pending_journal_v2_rejected_by_legacy_reader() {
        // What a pre-v2 binary does with a v2 journal: a decode error, which
        // it handles as a corrupt journal (deleted, chunks re-uploaded).
        let bytes = encode_pending_journal(&sample_entries()).unwrap();
        assert!(rmp_serde::from_slice::<Vec<PendingPackEntry>>(&bytes).is_err());
    }

    #[test]
    fn pending_journal_garbage_is_error() {
        assert!(decode_pending_journal(b"\xc1garbage").is_err());
    }

    #[test]
    fn pending_journal_dedup_on_reinsert() {
        let mut journal = PendingIndexJournal::new();
        let pack = make_pack_id(1);

        journal.record_pack(
            pack,
            vec![PendingChunkEntry {
                chunk_id: make_chunk_id(10),
                stored_size: 100,
                pack_offset: 0,
            }],
        );
        assert_eq!(journal.len(), 1);

        // Re-insert same pack_id — should replace, not duplicate
        journal.record_pack(
            pack,
            vec![PendingChunkEntry {
                chunk_id: make_chunk_id(10),
                stored_size: 100,
                pack_offset: 0,
            }],
        );
        assert_eq!(journal.len(), 1);

        let wire = journal.to_wire();
        assert_eq!(wire.len(), 1);
    }

    // --- IndexDelta reuse stage tests ---

    #[test]
    fn index_delta_stage_keeps_reuse_recorded_before_the_stage() {
        // A key reused before the stage, then again in it, is still
        // recorded after the stage is dropped.
        let mut delta = IndexDelta::new();
        let chunk = make_chunk_id(1);
        delta.reused.insert_key(&chunk);

        delta.reused.checkpoint();
        delta.reused.insert_key(&chunk);
        delta.reused.rollback();
        assert!(delta.reused.contains(&chunk));
        assert!(!delta.reused.is_armed());
    }

    #[test]
    fn index_delta_stage_removes_key_first_reused_in_it() {
        let mut delta = IndexDelta::new();
        let chunk = make_chunk_id(1);

        delta.reused.checkpoint();
        delta.reused.insert_key(&chunk);
        delta.reused.insert_key(&chunk);
        assert!(delta.reused.contains(&chunk));

        delta.reused.rollback();
        assert!(!delta.reused.contains(&chunk));
        assert!(delta.is_empty());
    }

    /// DELTA-1: additions only, reuse only and both are non-empty; a delta
    /// whose only reuse was dropped with its stage is empty.
    #[test]
    fn index_delta_is_empty_matrix() {
        let id = make_chunk_id(1);
        let mut additions = IndexDelta::new();
        additions.add_new_entry(id, 1, make_pack_id(1), 0);
        assert!(!additions.is_empty());

        let mut reuse = IndexDelta::new();
        reuse.reused.insert_key(&id);
        assert!(!reuse.is_empty());

        let mut both = IndexDelta::new();
        both.add_new_entry(id, 1, make_pack_id(1), 0);
        both.reused.insert_key(&make_chunk_id(2));
        assert!(!both.is_empty());

        let mut rolled = IndexDelta::new();
        rolled.reused.checkpoint();
        rolled.reused.insert_key(&id);
        rolled.reused.rollback();
        assert!(rolled.is_empty());
    }

    // --- PendingIndexJournal::remove_pack tests ---

    #[test]
    fn pending_journal_remove_pack() {
        let mut journal = PendingIndexJournal::new();
        let pack1 = make_pack_id(1);
        let pack2 = make_pack_id(2);
        let pack3 = make_pack_id(3);

        journal.record_pack(
            pack1,
            vec![PendingChunkEntry {
                chunk_id: make_chunk_id(10),
                stored_size: 100,
                pack_offset: 0,
            }],
        );
        journal.record_pack(
            pack2,
            vec![PendingChunkEntry {
                chunk_id: make_chunk_id(20),
                stored_size: 200,
                pack_offset: 0,
            }],
        );
        journal.record_pack(
            pack3,
            vec![PendingChunkEntry {
                chunk_id: make_chunk_id(30),
                stored_size: 300,
                pack_offset: 0,
            }],
        );
        assert_eq!(journal.len(), 3);

        // Remove the middle pack
        journal.remove_pack(&pack2);
        assert_eq!(journal.len(), 2);

        // Verify remaining packs
        let wire = journal.to_wire();
        let pack_ids: Vec<PackId> = wire.iter().map(|e| e.pack_id).collect();
        assert!(pack_ids.contains(&pack1));
        assert!(!pack_ids.contains(&pack2));
        assert!(pack_ids.contains(&pack3));

        // Remove non-existent pack — no-op
        journal.remove_pack(&make_pack_id(99));
        assert_eq!(journal.len(), 2);
    }

    // --- ChunkIndex mutation method tests ---

    #[test]
    fn chunk_index_remove_single() {
        let mut index = ChunkIndex::new();
        let c1 = make_chunk_id(1);
        let c2 = make_chunk_id(2);
        let pack = make_pack_id(10);
        index.add(c1, 100, pack, 0);
        index.add(c2, 200, pack, 100);
        assert_eq!(index.len(), 2);

        let removed = index.remove(&c1);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().stored_size, 100);
        assert_eq!(index.len(), 1);
        assert!(!index.contains(&c1));
        assert!(index.contains(&c2));

        // Remove non-existent — returns None
        assert!(index.remove(&make_chunk_id(99)).is_none());
    }

    #[test]
    fn chunk_index_remove_by_pack() {
        let mut index = ChunkIndex::new();
        let pack_a = make_pack_id(1);
        let pack_b = make_pack_id(2);
        index.add(make_chunk_id(10), 100, pack_a, 0);
        index.add(make_chunk_id(11), 100, pack_a, 100);
        index.add(make_chunk_id(20), 200, pack_b, 0);
        assert_eq!(index.len(), 3);

        let removed = index.remove_by_pack(&pack_a);
        assert_eq!(removed, 2);
        assert_eq!(index.len(), 1);
        assert!(index.contains(&make_chunk_id(20)));

        // Remove pack with no entries — returns 0
        assert_eq!(index.remove_by_pack(&make_pack_id(99)), 0);
    }

    /// A new entry the index already holds leaves the existing one as it is,
    /// and reuse is not applied at all.
    #[test]
    fn apply_to_adds_each_new_chunk_once_and_ignores_reuse() {
        let mut index = LiveIndex::empty();
        let c1 = make_chunk_id(1);
        let c2 = make_chunk_id(2);
        let c3 = make_chunk_id(3);
        let pack = make_pack_id(10);
        index.add(c1, 100, pack, 0);

        let mut delta = IndexDelta::new();
        delta.add_new_entry(c2, 200, pack, 100);
        delta.add_new_entry(c1, 300, make_pack_id(11), 7);
        delta.add_new_entry(c2, 400, make_pack_id(12), 9);
        delta.reused.insert_key(&c1);
        delta.reused.insert_key(&c3);

        delta.apply_to(&mut index);
        assert_eq!(index.len(), 2);
        let e1 = index.get(&c1).unwrap();
        assert_eq!((e1.stored_size, e1.pack_id, e1.pack_offset), (100, pack, 0));
        let e2 = index.get(&c2).unwrap();
        assert_eq!(
            (e2.stored_size, e2.pack_id, e2.pack_offset),
            (200, pack, 100)
        );
        assert!(!index.contains(&c3));
    }
}
