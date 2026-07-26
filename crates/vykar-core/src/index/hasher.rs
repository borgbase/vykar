//! Pass-through hasher for [`ChunkId`]-keyed maps.
//!
//! A `ChunkId` is already a 32-byte BLAKE2b digest, so re-hashing it with
//! SipHash — what `RandomState` does — buys nothing and costs a full hash per
//! lookup on the hottest path in the tool. This hasher takes the first 8 bytes
//! of the digest as the bucket key instead, matching what
//! `dedup_cache::chunk_id_to_u64` already does for the xor filter.
//!
//! This is safe here precisely because the digest is keyed: the chunk-ID key is
//! repository-private (derived from the repository ID even for unencrypted
//! repositories), so an attacker cannot grind inputs into a single bucket. Do
//! not reuse this for unkeyed digests such as `PathHash`.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use vykar_types::chunk_id::ChunkId;

/// A `HashMap` keyed by `ChunkId` that skips SipHash.
pub type ChunkIdHashMap<V> = HashMap<ChunkId, V, BuildChunkIdHasher>;

/// [`BuildHasher`](std::hash::BuildHasher) for [`ChunkIdHasher`].
pub type BuildChunkIdHasher = BuildHasherDefault<ChunkIdHasher>;

/// Takes the first 8 bytes written as a little-endian `u64`.
///
/// The derived `Hash` for `[u8; 32]` may emit a length prefix before the bytes
/// themselves, so integer writes are ignored and only the first byte slice is
/// captured.
#[derive(Default)]
pub struct ChunkIdHasher {
    hash: u64,
    seen: bool,
}

impl Hasher for ChunkIdHasher {
    fn write(&mut self, bytes: &[u8]) {
        if self.seen {
            return;
        }
        if let Some(prefix) = bytes.get(..8) {
            self.hash = u64::from_le_bytes(prefix.try_into().expect("8-byte slice"));
            self.seen = true;
        }
    }

    // Length prefixes and other integer writes carry no entropy for us.
    fn write_u8(&mut self, _: u8) {}
    fn write_u16(&mut self, _: u16) {}
    fn write_u32(&mut self, _: u32) {}
    fn write_u64(&mut self, _: u64) {}
    fn write_usize(&mut self, _: usize) {}
    fn write_i8(&mut self, _: i8) {}
    fn write_i16(&mut self, _: i16) {}
    fn write_i32(&mut self, _: i32) {}
    fn write_i64(&mut self, _: i64) {}
    fn write_isize(&mut self, _: isize) {}

    fn finish(&self) -> u64 {
        self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::dedup_cache::chunk_id_to_u64;
    use std::hash::Hash;

    fn hash_of(id: &ChunkId) -> u64 {
        let mut hasher = ChunkIdHasher::default();
        id.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn matches_chunk_id_to_u64() {
        for seed in 0u8..16 {
            let mut bytes = [0u8; 32];
            for (i, b) in bytes.iter_mut().enumerate() {
                *b = seed.wrapping_add(i as u8).wrapping_mul(31);
            }
            let id = ChunkId::from_bytes(bytes);
            assert_eq!(hash_of(&id), chunk_id_to_u64(&id));
        }
    }

    /// Guards the length-prefix case: if a `Hash` impl ever emitted the prefix
    /// as a byte slice rather than an integer, distinct IDs would collide.
    #[test]
    fn distinct_ids_hash_distinctly() {
        let a = ChunkId::from_bytes([1u8; 32]);
        let mut b_bytes = [1u8; 32];
        b_bytes[0] = 2;
        let b = ChunkId::from_bytes(b_bytes);
        assert_ne!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn map_round_trips_many_keys() {
        let mut map: ChunkIdHashMap<u32> = ChunkIdHashMap::default();
        let ids: Vec<ChunkId> = (0u32..512)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[..4].copy_from_slice(&i.to_le_bytes());
                ChunkId::from_bytes(bytes)
            })
            .collect();
        for (i, id) in ids.iter().enumerate() {
            map.insert(*id, i as u32);
        }
        assert_eq!(map.len(), ids.len());
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(map.get(id), Some(&(i as u32)));
        }
    }
}
