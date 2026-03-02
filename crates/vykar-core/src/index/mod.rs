pub mod dedup_cache;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::debug;
use xorf::Xor8;

use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;

/// In-memory index of all chunks in the repository.
/// Maps chunk_id -> (refcount, stored_size, pack_id, pack_offset).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkIndex {
    entries: HashMap<ChunkId, ChunkIndexEntry>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ChunkIndexEntry {
    pub refcount: u32,
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
}

impl ChunkIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
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

    /// Retain only entries whose ChunkId is in `needed`, then shrink to fit.
    /// Used to reduce memory after loading the full index for a restore.
    pub fn retain_chunks(&mut self, needed: &HashSet<ChunkId>) {
        self.entries.retain(|id, _| needed.contains(id));
        self.entries.shrink_to_fit();
    }

    /// Count distinct pack IDs across all entries.
    pub fn count_distinct_packs(&self) -> usize {
        let packs: std::collections::HashSet<PackId> =
            self.entries.values().map(|e| e.pack_id).collect();
        packs.len()
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
    entries: HashMap<ChunkId, u32>,
    xor_filter: Option<Arc<Xor8>>,
}

impl DedupIndex {
    /// Build a dedup index from the full chunk index, keeping only chunk_id → stored_size.
    pub fn from_chunk_index(full: &ChunkIndex) -> Self {
        let entries: HashMap<ChunkId, u32> = full
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
    }

    /// Record a pack and its chunk entries. Replaces any previous entry for
    /// the same `pack_id` (idempotent for recovery seeding).
    pub fn record_pack(&mut self, pack_id: PackId, chunks: Vec<PendingChunkEntry>) {
        self.packs
            .insert(pack_id, PendingPackEntry { pack_id, chunks });
    }

    /// Serialize to the wire format (`Vec<PendingPackEntry>`).
    pub fn to_wire(&self) -> Vec<PendingPackEntry> {
        self.packs.values().cloned().collect()
    }

    /// Deserialize from the wire format.
    pub fn from_wire(entries: Vec<PendingPackEntry>) -> Self {
        let packs = entries.into_iter().map(|e| (e.pack_id, e)).collect();
        Self { packs }
    }
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

/// Snapshot of `IndexDelta` state for checkpoint/rollback (used by dump streaming).
#[derive(Debug)]
pub struct IndexDeltaCheckpoint {
    new_entries_len: usize,
    refcount_bumps: HashMap<ChunkId, u32>,
}

impl IndexDeltaCheckpoint {
    /// Create an empty checkpoint (for repos not using dedup mode).
    pub fn empty() -> Self {
        Self {
            new_entries_len: 0,
            refcount_bumps: HashMap::new(),
        }
    }
}

/// Records all index mutations that happen while in dedup-only mode.
///
/// At save time, these are applied to a freshly-loaded full `ChunkIndex`.
#[derive(Debug, Default)]
pub struct IndexDelta {
    /// New chunk entries added during this session.
    pub new_entries: Vec<NewChunkEntry>,
    /// Refcount increments for chunks that already existed in the index.
    pub refcount_bumps: HashMap<ChunkId, u32>,
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

    /// Returns true if this delta contains no mutations.
    pub fn is_empty(&self) -> bool {
        self.new_entries.is_empty() && self.refcount_bumps.is_empty()
    }

    /// Capture a checkpoint of the current delta state for later rollback.
    pub fn checkpoint(&self) -> IndexDeltaCheckpoint {
        IndexDeltaCheckpoint {
            new_entries_len: self.new_entries.len(),
            refcount_bumps: self.refcount_bumps.clone(),
        }
    }

    /// Restore the delta to a previous checkpoint, discarding all mutations
    /// that occurred after it was taken.
    pub fn rollback(&mut self, cp: IndexDeltaCheckpoint) {
        self.new_entries.truncate(cp.new_entries_len);
        self.refcount_bumps = cp.refcount_bumps;
    }

    /// Record a refcount bump for an existing chunk.
    pub fn bump_refcount(&mut self, id: &ChunkId) {
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
        for chunk_id in self.refcount_bumps.keys() {
            if !fresh_index.contains(chunk_id)
                && !self.new_entries.iter().any(|e| e.chunk_id == *chunk_id)
            {
                return Err(vykar_types::error::VykarError::StaleChunksDuringCommit);
            }
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
            // add() sets refcount=1; apply remaining refs
            for _ in 1..entry.refcount {
                index.increment_refcount(&entry.chunk_id);
            }
        }

        // Apply refcount bumps for pre-existing chunks
        for (id, count) in self.refcount_bumps {
            for _ in 0..count {
                index.increment_refcount(&id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_chunk_id(byte: u8) -> ChunkId {
        ChunkId([byte; 32])
    }

    fn make_pack_id(byte: u8) -> PackId {
        PackId([byte; 32])
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
        pack_ids.sort_by_key(|p| p.0);
        assert_eq!(pack_ids, vec![pack1, pack2]);
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
}
