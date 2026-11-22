//! The v2/v3 monolithic index (`index`, written by vykar 0.19-0.24), as
//! those releases encoded it. Decoded only by `vykar upgrade`; encoded only
//! by tests and fuzz seeds that build legacy repositories. The field order,
//! types and serde attributes below are a frozen wire format: never change
//! them (`index_blob_golden_bytes`).

use serde::{Deserialize, Serialize};

use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;

use super::hasher::ChunkIdHashMap;
use super::{ChunkIndex, ChunkIndexEntry};

/// Wire format of `index`, encrypted with context `b"index"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexBlob {
    pub generation: u64,
    pub chunks: LegacyChunkIndex,
}

/// The `ChunkIndex` of 0.19-0.24: a one-field struct around the map.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LegacyChunkIndex {
    #[serde(deserialize_with = "deserialize_entries")]
    entries: ChunkIdHashMap<LegacyIndexEntry>,
}

/// One legacy entry, with the refcount the old releases maintained.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LegacyIndexEntry {
    pub refcount: u32,
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
}

/// Pre-size the entry table from the msgpack length prefix so a large index
/// is allocated once instead of grown by doubling (old and new table both
/// live at each rehash). Every entry carries a distinct 32-byte chunk ID — a
/// keyed digest, incompressible — so a genuine index costs at least 32 input
/// bytes per entry; see [`crate::compress::presize`] for the bound.
fn deserialize_entries<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<ChunkIdHashMap<LegacyIndexEntry>, D::Error> {
    crate::compress::presize::deserialize_hash_map(deserializer, std::mem::size_of::<ChunkId>())
}

impl LegacyChunkIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, id: &ChunkId) -> Option<&LegacyIndexEntry> {
        self.entries.get(id)
    }

    /// Insert or replace the entry of `id`.
    pub fn insert(&mut self, id: ChunkId, entry: LegacyIndexEntry) {
        self.entries.insert(id, entry);
    }

    pub fn remove(&mut self, id: &ChunkId) -> Option<LegacyIndexEntry> {
        self.entries.remove(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ChunkId, &LegacyIndexEntry)> {
        self.entries.iter()
    }

    /// The entries the upgrade carries over: refcount above 0, as
    /// `ChunkIndexEntry`, in no particular order. A refcount-0 entry was
    /// garbage the old release had not removed yet.
    pub fn live(&self) -> impl Iterator<Item = (ChunkId, ChunkIndexEntry)> + '_ {
        self.entries
            .iter()
            .filter(|(_, e)| e.refcount > 0)
            .map(|(id, e)| {
                (
                    *id,
                    ChunkIndexEntry {
                        stored_size: e.stored_size,
                        pack_id: e.pack_id,
                        pack_offset: e.pack_offset,
                    },
                )
            })
    }

    /// [`Self::live`] collected into a `ChunkIndex` (tests; the upgrade applies the same
    /// rule in `table::base_body_from_legacy`).
    pub fn to_live_index(&self) -> ChunkIndex {
        let mut index = ChunkIndex::with_capacity(self.entries.len());
        for (id, e) in self.live() {
            index.insert_entry(id, e);
        }
        index
    }
}

/// A legacy index with refcount 1 for every entry of `index` (test and
/// fuzz encoders of legacy repositories).
impl From<&ChunkIndex> for LegacyChunkIndex {
    fn from(index: &ChunkIndex) -> Self {
        let mut legacy = Self::default();
        legacy.entries.reserve(index.len());
        for (id, e) in index.iter() {
            legacy.insert(
                *id,
                LegacyIndexEntry {
                    refcount: 1,
                    stored_size: e.stored_size,
                    pack_id: e.pack_id,
                    pack_offset: e.pack_offset,
                },
            );
        }
        legacy
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

    /// The bytes of two one-entry blobs, captured from the `IndexBlob` /
    /// `ChunkIndex` / `ChunkIndexEntry` types before they were split into
    /// this module. One entry per blob keeps the bytes independent of
    /// hash-map order.
    #[test]
    fn index_blob_golden_bytes() {
        let cases = [
            (
                42u64,
                make_chunk_id(0x22),
                LegacyIndexEntry {
                    refcount: 2,
                    stored_size: 77,
                    pack_id: make_pack_id(0x11),
                    pack_offset: 1 << 33,
                },
                "922a9181dc0020222222222222222222222222222222222222222222222222222222222222222294024ddc00201111111111111111111111111111111111111111111111111111111111111111cf0000000200000000",
            ),
            (
                3u64,
                make_chunk_id(0x33),
                LegacyIndexEntry {
                    refcount: 0,
                    stored_size: 5,
                    pack_id: make_pack_id(0x44),
                    pack_offset: 9,
                },
                "92039181dc00203333333333333333333333333333333333333333333333333333333333333333940005dc0020444444444444444444444444444444444444444444444444444444444444444409",
            ),
        ];
        for (generation, id, entry, golden) in cases {
            let mut chunks = LegacyChunkIndex::new();
            chunks.insert(id, entry);
            let bytes = rmp_serde::to_vec(&IndexBlob { generation, chunks }).unwrap();
            assert_eq!(hex::encode(&bytes), golden);
            let decoded: IndexBlob = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(decoded.generation, generation);
            assert_eq!(decoded.chunks.len(), 1);
            assert_eq!(decoded.chunks.get(&id), Some(&entry));
        }
    }

    #[test]
    fn index_blob_metadata_decode_presizes_entries() {
        let mut chunks = LegacyChunkIndex::new();
        for i in 0..30_000u32 {
            let mut id = [0u8; 32];
            id[..4].copy_from_slice(&i.to_le_bytes());
            id[31] = 0x5a;
            chunks.insert(
                ChunkId::from_bytes(id),
                LegacyIndexEntry {
                    refcount: 1,
                    stored_size: i,
                    pack_id: make_pack_id(1),
                    pack_offset: u64::from(i),
                },
            );
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
        let presized: ChunkIdHashMap<LegacyIndexEntry> =
            ChunkIdHashMap::with_capacity_and_hasher(chunks.len(), Default::default());
        assert_eq!(restored.chunks.entries.capacity(), presized.capacity());
    }

    #[test]
    fn index_blob_msgpack_round_trip() {
        let mut chunks = LegacyChunkIndex::new();
        let entry = |refcount, stored_size, pack, pack_offset| LegacyIndexEntry {
            refcount,
            stored_size,
            pack_id: make_pack_id(pack),
            pack_offset,
        };
        chunks.insert(make_chunk_id(1), entry(2, 100, 10, 0));
        chunks.insert(make_chunk_id(2), entry(1, 200, 20, 100));

        let blob = IndexBlob {
            generation: 42,
            chunks,
        };
        let serialized = rmp_serde::to_vec(&blob).unwrap();
        let restored: IndexBlob = rmp_serde::from_slice(&serialized).unwrap();

        assert_eq!(restored.generation, 42);
        assert_eq!(restored.chunks.len(), 2);
        assert_eq!(
            restored.chunks.get(&make_chunk_id(1)),
            Some(&entry(2, 100, 10, 0))
        );
        assert_eq!(
            restored.chunks.get(&make_chunk_id(2)),
            Some(&entry(1, 200, 20, 100))
        );
    }

    /// `live()` drops refcount-0 entries and keeps every other entry with
    /// its location; `From<&ChunkIndex>` gives every entry refcount 1.
    #[test]
    fn live_drops_refcount_zero_and_from_sets_one() {
        let mut legacy = LegacyChunkIndex::new();
        let entry = |refcount, pack_offset| LegacyIndexEntry {
            refcount,
            stored_size: 10,
            pack_id: make_pack_id(9),
            pack_offset,
        };
        legacy.insert(make_chunk_id(1), entry(3, 0));
        legacy.insert(make_chunk_id(2), entry(0, 10));
        legacy.insert(make_chunk_id(3), entry(1, 20));

        let live = legacy.to_live_index();
        assert_eq!(live.len(), 2);
        assert!(!live.contains(&make_chunk_id(2)));
        for (id, offset) in [(1, 0), (3, 20)] {
            assert_eq!(
                live.get(&make_chunk_id(id)),
                Some(&ChunkIndexEntry {
                    stored_size: 10,
                    pack_id: make_pack_id(9),
                    pack_offset: offset,
                })
            );
        }

        let back = LegacyChunkIndex::from(&live);
        assert_eq!(back.len(), 2);
        assert_eq!(back.get(&make_chunk_id(1)), Some(&entry(1, 0)));
        assert_eq!(back.get(&make_chunk_id(3)), Some(&entry(1, 20)));
    }
}
