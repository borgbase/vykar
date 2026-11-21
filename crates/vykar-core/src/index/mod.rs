pub mod base;
pub mod cached_bumps;
pub mod dedup_cache;
pub mod hasher;
pub mod head;
pub mod segment;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub use cached_bumps::CachedBumps;
use hasher::{BuildChunkIdHasher, ChunkIdHashMap};

use serde::{Deserialize, Serialize};
use tracing::debug;
use xorf::Xor8;

use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;

/// Wire format of the monolithic index that formats v2/v3 store at `index`,
/// encrypted with context `b"index"`. Read only by the upgrade, and by the
/// repair of a v4/v5 `index` that an older binary overwrote; nothing writes
/// it any more (`Serialize` stays for test and fuzz encoders).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexBlob {
    pub generation: u64,
    pub chunks: ChunkIndex,
}

/// In-memory index of all chunks in the repository.
/// Maps chunk_id -> (refcount, stored_size, pack_id, pack_offset).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkIndex {
    #[serde(deserialize_with = "deserialize_entries")]
    entries: ChunkIdHashMap<ChunkIndexEntry>,
}

/// Pre-size the entry table from the msgpack length prefix so a large index
/// is allocated once instead of grown by doubling (old and new table both
/// live at each rehash). Every entry carries a distinct 32-byte chunk ID — a
/// keyed digest, incompressible — so a genuine index costs at least 32 input
/// bytes per entry; see [`crate::compress::presize`] for the bound.
fn deserialize_entries<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<ChunkIdHashMap<ChunkIndexEntry>, D::Error> {
    crate::compress::presize::deserialize_hash_map(deserializer, std::mem::size_of::<ChunkId>())
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ChunkIndexEntry {
    pub refcount: u32,
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

    /// Add a new chunk entry with its pack location.
    pub fn add(&mut self, id: ChunkId, stored_size: u32, pack_id: PackId, pack_offset: u64) {
        self.entries
            .entry(id)
            .and_modify(|e| e.refcount += 1)
            .or_insert(ChunkIndexEntry {
                refcount: 1,
                stored_size,
                pack_id,
                pack_offset,
            });
    }

    /// Increment the refcount for an existing chunk without changing its location.
    pub fn increment_refcount(&mut self, id: &ChunkId) {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.refcount += 1;
        }
    }

    /// Increment the refcount for an existing chunk by a given amount.
    pub fn increment_refcount_by(&mut self, id: &ChunkId, amount: u32) {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.refcount += amount;
        }
    }

    pub fn get(&self, id: &ChunkId) -> Option<&ChunkIndexEntry> {
        self.entries.get(id)
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

    /// Decrement refcount for a chunk. Returns the new refcount and stored_size.
    /// If refcount reaches 0, the entry is removed from the index.
    /// Returns None if the chunk is not in the index.
    pub fn decrement(&mut self, id: &ChunkId) -> Option<(u32, u32)> {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.refcount = entry.refcount.saturating_sub(1);
            let rc = entry.refcount;
            let size = entry.stored_size;
            if rc == 0 {
                self.entries.remove(id);
            }
            Some((rc, size))
        } else {
            None
        }
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

    /// Replace all refcounts from a snapshot-derived map.
    /// Entries with zero refs (not in `new_refcounts`) are removed.
    pub fn rebuild_refcounts(&mut self, new_refcounts: &HashMap<ChunkId, u32>) {
        self.entries.retain(|id, entry| {
            if let Some(&rc) = new_refcounts.get(id) {
                entry.refcount = rc;
                true
            } else {
                false
            }
        });
    }
}

/// Lightweight dedup-only index that stores only chunk_id → stored_size.
///
/// Used during backup to reduce memory: ~68 bytes per entry vs ~112 bytes
/// for the full `ChunkIndex`. For 10M chunks this saves ~400 MB of RAM.
///
/// Does not track refcounts, pack locations, or offsets — those are recorded
/// in an `IndexDelta` and merged back into the full index at save time.
#[derive(Debug)]
pub struct DedupIndex {
    entries: ChunkIdHashMap<u32>,
    xor_filter: Option<Arc<Xor8>>,
}

impl DedupIndex {
    /// Build a dedup index from the full chunk index, keeping only chunk_id → stored_size.
    pub fn from_chunk_index(full: &ChunkIndex) -> Self {
        let entries: ChunkIdHashMap<u32> = full
            .entries
            .iter()
            .map(|(id, entry)| (*id, entry.stored_size))
            .collect();
        let keys: Vec<u64> = entries.keys().map(dedup_cache::chunk_id_to_u64).collect();
        let xor_filter = dedup_cache::build_xor_filter_from_keys(&keys).map(Arc::new);
        debug!(
            "built dedup index with {} entries from full index",
            entries.len()
        );
        Self {
            entries,
            xor_filter,
        }
    }

    /// Return a shared reference to the pre-built xor filter (if any).
    pub(crate) fn xor_filter(&self) -> Option<Arc<Xor8>> {
        self.xor_filter.clone()
    }

    /// Check if a chunk exists (dedup hit).
    pub fn contains(&self, id: &ChunkId) -> bool {
        self.entries.contains_key(id)
    }

    /// Get the stored size for a chunk.
    pub fn get_stored_size(&self, id: &ChunkId) -> Option<u32> {
        self.entries.get(id).copied()
    }

    /// Insert a new chunk (used when new chunks are committed during backup).
    pub fn insert(&mut self, id: ChunkId, stored_size: u32) {
        self.entries.insert(id, stored_size);
    }

    /// Remove a session-local entry. The xor filter may still report false
    /// positives for the removed chunk — safe because the precise lookup will miss.
    pub fn remove(&mut self, id: &ChunkId) {
        self.entries.remove(id);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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

    /// Remove a pack from the journal (used by dump rollback).
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
/// Lives in `Repository::recovered_chunks` until promoted into the active dedup
/// structure on a dedup hit.
#[derive(Debug, Clone)]
pub struct RecoveredChunkEntry {
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
}

/// Marker for a `IndexDelta` rollback checkpoint. Records only the length of
/// `new_entries` at checkpoint time; refcount bumps are rolled back via the
/// delta's undo log and the positional journal in `cached_bumps` (both armed
/// by `checkpoint()`), so no map snapshot is stored.
///
/// Armed per streamed command dump and per segmented large file.
#[derive(Debug)]
pub struct IndexDeltaCheckpoint {
    new_entries_len: usize,
}

impl IndexDeltaCheckpoint {
    /// Create an empty checkpoint (for repos not using dedup mode).
    pub fn empty() -> Self {
        Self { new_entries_len: 0 }
    }
}

/// Records all index mutations that happen while in dedup-only mode.
///
/// At save time, these are applied to a freshly-loaded full `ChunkIndex`.
#[derive(Debug, Default)]
pub struct IndexDelta {
    /// New chunk entries added during this session.
    pub new_entries: Vec<NewChunkEntry>,
    /// Refcount increments recorded by chunk ID: chunks created earlier in
    /// this session, chunks found through the non-tiered `DedupIndex`, and
    /// bumps whose position could not be used (see `bump_cached_refcount`).
    pub refcount_bumps: HashMap<ChunkId, u32>,
    /// Refcount increments for chunks of the tiered mode's mmap'd dedup
    /// cache, by cache position (`None` outside tiered mode). The logical
    /// bump for a chunk is its `refcount_bumps` value plus its count here;
    /// every consumer below adds both.
    pub cached_bumps: Option<CachedBumps>,
    /// Undo log for the armed rollback checkpoint. `Some` ⟺ a checkpoint is
    /// armed. Maps each in-scope-mutated chunk to its `refcount_bumps` value at
    /// first touch (`None` = key was absent), so rollback replays exact prior
    /// state without cloning the whole map.
    pub(crate) undo_log: Option<HashMap<ChunkId, Option<u32>>>,
}

/// A new chunk entry recorded during dedup-mode backup.
#[derive(Debug, Clone)]
pub struct NewChunkEntry {
    pub chunk_id: ChunkId,
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
    pub refcount: u32,
}

impl IndexDelta {
    pub fn new() -> Self {
        Self::default()
    }

    /// A delta that records bumps for chunks of `cache` by position
    /// (tiered dedup mode).
    pub fn with_cached_bumps(cache: Arc<dedup_cache::MmapDedupCache>) -> Self {
        Self {
            cached_bumps: Some(CachedBumps::new(cache)),
            ..Self::default()
        }
    }

    /// Returns true if this delta contains no mutations.
    pub fn is_empty(&self) -> bool {
        self.new_entries.is_empty()
            && self.refcount_bumps.is_empty()
            && self.cached_bumps.as_ref().is_none_or(CachedBumps::is_empty)
    }

    /// Arm a rollback checkpoint. Subsequent `refcount_bumps` mutations are
    /// recorded in an undo log so `rollback()` can restore exact prior state
    /// without cloning the map.
    pub fn checkpoint(&mut self) -> IndexDeltaCheckpoint {
        assert!(
            self.undo_log.is_none(),
            "checkpoint() called while another checkpoint is armed"
        );
        self.undo_log = Some(HashMap::new());
        if let Some(cached) = self.cached_bumps.as_mut() {
            cached.checkpoint();
        }
        IndexDeltaCheckpoint {
            new_entries_len: self.new_entries.len(),
        }
    }

    /// Restore the delta to a previous checkpoint, discarding all mutations
    /// that occurred after it was taken.
    pub fn rollback(&mut self, cp: IndexDeltaCheckpoint) {
        self.new_entries.truncate(cp.new_entries_len);
        // Replay the undo log (empty/absent when the checkpoint was never
        // armed, e.g. `IndexDeltaCheckpoint::empty()`).
        if let Some(undo) = self.undo_log.take() {
            for (id, prev) in undo {
                match prev {
                    Some(v) => {
                        self.refcount_bumps.insert(id, v);
                    }
                    None => {
                        self.refcount_bumps.remove(&id);
                    }
                }
            }
        }
        if let Some(cached) = self.cached_bumps.as_mut() {
            cached.rollback();
        }
    }

    /// Discard the armed checkpoint without rolling back (commit path). The
    /// in-scope mutations are kept; the undo log is dropped so the next
    /// `checkpoint()` can arm cleanly.
    pub fn discard_checkpoint(&mut self) {
        self.undo_log = None;
        if let Some(cached) = self.cached_bumps.as_mut() {
            cached.discard_checkpoint();
        }
    }

    /// Record a refcount bump for a chunk found at `position` in the tiered
    /// mode's mmap'd dedup cache. Falls back to the keyed map when this delta
    /// has no positional table or the position does not hold `id`, so the
    /// recorded logical bump is the same either way.
    pub fn bump_cached_refcount(&mut self, id: &ChunkId, position: u32) {
        if let Some(cached) = self.cached_bumps.as_mut()
            && cached.bump(position, id)
        {
            return;
        }
        // Unreachable while positions come from `TieredDedupIndex::find*`
        // on the cache this table was built from; the keyed fallback
        // keeps the count exact if that ever stops holding.
        self.bump_refcount(id);
    }

    /// Record a refcount bump for an existing chunk by key.
    pub fn bump_refcount(&mut self, id: &ChunkId) {
        if let Some(undo) = self.undo_log.as_mut() {
            let prev = self.refcount_bumps.get(id).copied();
            undo.entry(*id).or_insert(prev);
        }
        *self.refcount_bumps.entry(*id).or_insert(0) += 1;
    }

    /// Record a new chunk entry.
    pub fn add_new_entry(
        &mut self,
        chunk_id: ChunkId,
        stored_size: u32,
        pack_id: PackId,
        pack_offset: u64,
        refcount: u32,
    ) {
        self.new_entries.push(NewChunkEntry {
            chunk_id,
            stored_size,
            pack_id,
            pack_offset,
            refcount,
        });
    }

    /// Reconcile this delta against a fresh index loaded at commit time.
    ///
    /// - `new_entries` already present in `fresh_index` → converted to refcount bumps
    ///   (another client uploaded the same chunk concurrently).
    /// - For each `refcount_bumps` key: verify the chunk still exists in `fresh_index`.
    ///   If missing → `Err(StaleChunksDuringCommit)` (chunk was deleted since session started).
    pub fn reconcile(mut self, fresh_index: &ChunkIndex) -> vykar_types::error::Result<Self> {
        // Partition new_entries: those already in fresh_index become refcount bumps.
        let mut still_new = Vec::new();
        for entry in self.new_entries {
            if fresh_index.contains(&entry.chunk_id) {
                // Another client already committed this chunk — convert to bumps.
                *self.refcount_bumps.entry(entry.chunk_id).or_insert(0) += entry.refcount;
            } else {
                still_new.push(entry);
            }
        }
        self.new_entries = still_new;

        // Verify all bump targets still exist.
        let new_entry_ids: HashSet<ChunkId> = self.new_entries.iter().map(|e| e.chunk_id).collect();
        let is_stale = |chunk_id: &ChunkId| {
            !fresh_index.contains(chunk_id) && !new_entry_ids.contains(chunk_id)
        };
        if self.refcount_bumps.keys().any(is_stale) {
            return Err(vykar_types::error::VykarError::StaleChunksDuringCommit);
        }
        if let Some(cached) = self.cached_bumps.as_ref()
            && cached
                .iter_nonzero()
                .any(|(chunk_id, _)| is_stale(&chunk_id))
        {
            return Err(vykar_types::error::VykarError::StaleChunksDuringCommit);
        }

        Ok(self)
    }

    /// Apply this delta to a full `ChunkIndex`.
    pub fn apply_to(self, index: &mut ChunkIndex) {
        // Apply new entries first
        for entry in self.new_entries {
            index.add(
                entry.chunk_id,
                entry.stored_size,
                entry.pack_id,
                entry.pack_offset,
            );
            // add() sets refcount=1; apply remaining refs in bulk
            if entry.refcount > 1 {
                index.increment_refcount_by(&entry.chunk_id, entry.refcount - 1);
            }
        }

        // Apply refcount bumps for pre-existing chunks
        for (id, count) in self.refcount_bumps {
            index.increment_refcount_by(&id, count);
        }
        if let Some(cached) = self.cached_bumps {
            for (id, count) in cached.iter_nonzero() {
                index.increment_refcount_by(&id, count);
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

    // --- IndexDelta::reconcile tests ---

    #[test]
    fn reconcile_new_entry_already_in_fresh_index_becomes_bump() {
        let mut fresh = ChunkIndex::new();
        let chunk_a = make_chunk_id(1);
        let pack_a = make_pack_id(10);
        fresh.add(chunk_a, 100, pack_a, 0);

        let mut delta = IndexDelta::new();
        delta.add_new_entry(chunk_a, 100, make_pack_id(20), 0, 1);

        let reconciled = delta.reconcile(&fresh).unwrap();
        // new_entries should be empty (converted to bump)
        assert!(reconciled.new_entries.is_empty());
        // refcount_bumps should have chunk_a with count=1
        assert_eq!(reconciled.refcount_bumps.get(&chunk_a), Some(&1));
    }

    #[test]
    fn reconcile_bump_target_missing_returns_error() {
        let fresh = ChunkIndex::new(); // empty index

        let mut delta = IndexDelta::new();
        let chunk_a = make_chunk_id(1);
        delta.bump_refcount(&chunk_a); // bump for a chunk not in index

        let result = delta.reconcile(&fresh);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            vykar_types::error::VykarError::StaleChunksDuringCommit
        ));
    }

    #[test]
    fn reconcile_bump_target_exists_succeeds() {
        let mut fresh = ChunkIndex::new();
        let chunk_a = make_chunk_id(1);
        let pack_a = make_pack_id(10);
        fresh.add(chunk_a, 100, pack_a, 0);

        let mut delta = IndexDelta::new();
        delta.bump_refcount(&chunk_a);

        let reconciled = delta.reconcile(&fresh).unwrap();
        assert!(reconciled.new_entries.is_empty());
        assert_eq!(reconciled.refcount_bumps.get(&chunk_a), Some(&1));
    }

    #[test]
    fn reconcile_mixed_new_and_existing() {
        let mut fresh = ChunkIndex::new();
        let chunk_a = make_chunk_id(1);
        let chunk_b = make_chunk_id(2);
        let pack_a = make_pack_id(10);
        fresh.add(chunk_a, 100, pack_a, 0);
        // chunk_b is NOT in fresh index

        let mut delta = IndexDelta::new();
        // chunk_a already exists in fresh → becomes bump
        delta.add_new_entry(chunk_a, 100, make_pack_id(20), 0, 2);
        // chunk_b is truly new
        delta.add_new_entry(chunk_b, 200, make_pack_id(30), 0, 1);

        let reconciled = delta.reconcile(&fresh).unwrap();
        assert_eq!(reconciled.new_entries.len(), 1);
        assert_eq!(reconciled.new_entries[0].chunk_id, chunk_b);
        assert_eq!(reconciled.refcount_bumps.get(&chunk_a), Some(&2));
    }

    // --- IndexDelta checkpoint/rollback tests ---

    #[test]
    fn index_delta_checkpoint_rollback() {
        let mut delta = IndexDelta::new();
        let chunk_a = make_chunk_id(1);
        let chunk_b = make_chunk_id(2);
        let chunk_c = make_chunk_id(3);
        let pack = make_pack_id(10);

        // Add initial state
        delta.add_new_entry(chunk_a, 100, pack, 0, 1);
        delta.bump_refcount(&chunk_b);

        // Checkpoint
        let cp = delta.checkpoint();
        assert_eq!(delta.new_entries.len(), 1);

        // Add more mutations after checkpoint
        delta.add_new_entry(chunk_c, 200, pack, 100, 1);
        delta.bump_refcount(&chunk_a);
        assert_eq!(delta.new_entries.len(), 2);
        assert_eq!(delta.refcount_bumps.get(&chunk_a), Some(&1));

        // Rollback
        delta.rollback(cp);
        assert_eq!(delta.new_entries.len(), 1);
        assert_eq!(delta.new_entries[0].chunk_id, chunk_a);
        assert_eq!(delta.refcount_bumps.get(&chunk_b), Some(&1));
        assert!(!delta.refcount_bumps.contains_key(&chunk_a));
    }

    #[test]
    fn index_delta_checkpoint_empty() {
        let cp = IndexDeltaCheckpoint::empty();
        let mut delta = IndexDelta::new();
        delta.add_new_entry(make_chunk_id(1), 100, make_pack_id(1), 0, 1);
        delta.rollback(cp);
        assert!(delta.new_entries.is_empty());
        assert!(delta.refcount_bumps.is_empty());
    }

    #[test]
    #[should_panic(expected = "checkpoint() called while another checkpoint is armed")]
    fn index_delta_overlapping_checkpoints_panic() {
        let mut delta = IndexDelta::new();
        let _cp1 = delta.checkpoint();
        let _cp2 = delta.checkpoint();
    }

    #[test]
    fn index_delta_rollback_restores_pre_checkpoint_count() {
        // A key bumped before the checkpoint, then bumped again in scope, must
        // roll back to its pre-checkpoint count — not vanish.
        let mut delta = IndexDelta::new();
        let chunk = make_chunk_id(1);
        delta.bump_refcount(&chunk);
        delta.bump_refcount(&chunk);
        assert_eq!(delta.refcount_bumps.get(&chunk), Some(&2));

        let cp = delta.checkpoint();
        delta.bump_refcount(&chunk);
        assert_eq!(delta.refcount_bumps.get(&chunk), Some(&3));

        delta.rollback(cp);
        assert_eq!(delta.refcount_bumps.get(&chunk), Some(&2));
        assert!(delta.undo_log.is_none());
    }

    #[test]
    fn index_delta_rollback_removes_key_first_bumped_in_scope() {
        let mut delta = IndexDelta::new();
        let chunk = make_chunk_id(1);

        let cp = delta.checkpoint();
        delta.bump_refcount(&chunk);
        assert_eq!(delta.refcount_bumps.get(&chunk), Some(&1));

        delta.rollback(cp);
        assert!(!delta.refcount_bumps.contains_key(&chunk));
    }

    #[test]
    fn index_delta_rollback_single_undo_entry_for_repeated_bumps() {
        // Same key bumped twice in scope records only one undo entry (first
        // touch), and rollback still removes it cleanly.
        let mut delta = IndexDelta::new();
        let chunk = make_chunk_id(1);

        let cp = delta.checkpoint();
        delta.bump_refcount(&chunk);
        delta.bump_refcount(&chunk);
        assert_eq!(delta.refcount_bumps.get(&chunk), Some(&2));
        assert_eq!(delta.undo_log.as_ref().unwrap().len(), 1);

        delta.rollback(cp);
        assert!(!delta.refcount_bumps.contains_key(&chunk));
    }

    #[test]
    fn index_delta_commit_then_recheckpoint() {
        // Commit path: discard_checkpoint keeps in-scope bumps and lets a
        // second checkpoint arm without panicking.
        let mut delta = IndexDelta::new();
        let chunk_a = make_chunk_id(1);
        let chunk_b = make_chunk_id(2);

        let _cp1 = delta.checkpoint();
        delta.bump_refcount(&chunk_a);
        // Commit the first scope.
        delta.discard_checkpoint();
        assert!(delta.undo_log.is_none());
        assert_eq!(delta.refcount_bumps.get(&chunk_a), Some(&1));

        // Second checkpoint arms cleanly; its bump survives commit too.
        let _cp2 = delta.checkpoint();
        delta.bump_refcount(&chunk_b);
        delta.discard_checkpoint();
        assert_eq!(delta.refcount_bumps.get(&chunk_a), Some(&1));
        assert_eq!(delta.refcount_bumps.get(&chunk_b), Some(&1));
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

    #[test]
    fn index_blob_metadata_decode_presizes_entries() {
        let mut chunks = ChunkIndex::new();
        for i in 0..30_000u32 {
            let mut id = [0u8; 32];
            id[..4].copy_from_slice(&i.to_le_bytes());
            id[31] = 0x5a;
            chunks.add(ChunkId::from_bytes(id), i, make_pack_id(1), u64::from(i));
        }
        let blob = IndexBlob {
            generation: 7,
            chunks: chunks.clone(),
        };
        let encoded = rmp_serde::to_vec(&blob).unwrap();
        let compressed =
            crate::compress::compress(crate::compress::Compression::Zstd { level: 3 }, &encoded)
                .unwrap();
        let (restored, _): (IndexBlob, u64) =
            crate::compress::decode_metadata_msgpack(&compressed).unwrap();
        assert_eq!(restored.generation, 7);
        assert_eq!(restored.chunks.len(), chunks.len());
        // Allocated once at the exact size rather than grown by doubling.
        assert_eq!(
            restored.chunks.entries.capacity(),
            ChunkIndex::with_capacity(chunks.len()).entries.capacity()
        );
    }

    #[test]
    fn index_blob_msgpack_round_trip() {
        let mut chunks = ChunkIndex::new();
        chunks.add(make_chunk_id(1), 100, make_pack_id(10), 0);
        chunks.add(make_chunk_id(2), 200, make_pack_id(20), 100);
        // Bump refcount on chunk 1
        chunks.increment_refcount(&make_chunk_id(1));

        let generation = 42u64;
        let blob = IndexBlob {
            generation,
            chunks: chunks.clone(),
        };

        let serialized = rmp_serde::to_vec(&blob).unwrap();
        let restored: IndexBlob = rmp_serde::from_slice(&serialized).unwrap();

        assert_eq!(restored.generation, generation);
        assert_eq!(restored.chunks.len(), 2);
        assert_eq!(restored.chunks.get(&make_chunk_id(1)).unwrap().refcount, 2);
        assert_eq!(
            restored.chunks.get(&make_chunk_id(2)).unwrap().stored_size,
            200
        );
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

    #[test]
    fn chunk_index_rebuild_refcounts() {
        let mut index = ChunkIndex::new();
        let c1 = make_chunk_id(1);
        let c2 = make_chunk_id(2);
        let c3 = make_chunk_id(3);
        let pack = make_pack_id(10);
        index.add(c1, 100, pack, 0);
        index.add(c2, 200, pack, 100);
        index.add(c3, 300, pack, 300);

        let mut new_refs = HashMap::new();
        new_refs.insert(c1, 5);
        new_refs.insert(c2, 1);
        // c3 not in new_refs → should be removed

        index.rebuild_refcounts(&new_refs);
        assert_eq!(index.len(), 2);
        assert_eq!(index.get(&c1).unwrap().refcount, 5);
        assert_eq!(index.get(&c2).unwrap().refcount, 1);
        assert!(!index.contains(&c3));
    }

    #[test]
    fn increment_refcount_by_adds_amount() {
        let mut index = ChunkIndex::new();
        let c = make_chunk_id(1);
        let pack = make_pack_id(10);
        index.add(c, 100, pack, 0);
        assert_eq!(index.get(&c).unwrap().refcount, 1);

        index.increment_refcount_by(&c, 5);
        assert_eq!(index.get(&c).unwrap().refcount, 6);

        // No-op for missing chunk
        index.increment_refcount_by(&make_chunk_id(99), 10);
    }

    #[test]
    fn apply_to_bulk_refcount() {
        let mut index = ChunkIndex::new();
        let c1 = make_chunk_id(1);
        let c2 = make_chunk_id(2);
        let pack = make_pack_id(10);
        index.add(c1, 100, pack, 0);

        let mut delta = IndexDelta::new();
        // New entry with refcount > 1
        delta.add_new_entry(c2, 200, pack, 100, 5);
        // Bump existing entry multiple times
        delta.bump_refcount(&c1);
        delta.bump_refcount(&c1);
        delta.bump_refcount(&c1);

        delta.apply_to(&mut index);
        assert_eq!(index.get(&c1).unwrap().refcount, 4); // 1 + 3 bumps
        assert_eq!(index.get(&c2).unwrap().refcount, 5); // new entry with rc=5
    }
}
