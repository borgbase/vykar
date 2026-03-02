use blake2::digest::consts::U32;
use blake2::digest::Mac;
use blake2::Blake2bMac;
use serde::{Deserialize, Serialize};
use std::fmt;

type KeyedBlake2b256 = Blake2bMac<U32>;

/// A 32-byte chunk identifier computed as keyed BLAKE2b-256.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId(pub [u8; 32]);

impl ChunkId {
    /// Compute a chunk ID using keyed BLAKE2b-256 (BLAKE2b-MAC with 32-byte output).
    pub fn compute(key: &[u8; 32], data: &[u8]) -> Self {
        let mut hasher =
            KeyedBlake2b256::new_from_slice(key).expect("valid 32-byte key for BLAKE2b");
        Mac::update(&mut hasher, data);
        let result = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&result.into_bytes());
        ChunkId(out)
    }

    /// Hex-encode the full chunk ID for use as a storage key.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// First byte as a two-char hex string, used for shard directory.
    pub fn shard_prefix(&self) -> String {
        hex::encode(&self.0[..1])
    }
}

impl fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkId({})", &self.to_hex()[..16])
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_hex()[..16])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_chunk_id_key() -> [u8; 32] {
        [0xAA; 32]
    }

    #[test]
    fn compute_deterministic() {
        let key = test_chunk_id_key();
        let data = b"hello world";
        let id1 = ChunkId::compute(&key, data);
        let id2 = ChunkId::compute(&key, data);
        assert_eq!(id1, id2);
    }

    #[test]
    fn compute_different_data_different_id() {
        let key = test_chunk_id_key();
        let id1 = ChunkId::compute(&key, b"hello");
        let id2 = ChunkId::compute(&key, b"world");
        assert_ne!(id1, id2);
    }

    #[test]
    fn compute_different_key_different_id() {
        let key1 = [0xAA; 32];
        let key2 = [0xBB; 32];
        let data = b"same data";
        let id1 = ChunkId::compute(&key1, data);
        let id2 = ChunkId::compute(&key2, data);
        assert_ne!(id1, id2);
    }

    #[test]
    fn to_hex_length() {
        let key = test_chunk_id_key();
        let id = ChunkId::compute(&key, b"test");
        assert_eq!(id.to_hex().len(), 64);
    }

    #[test]
    fn shard_prefix_is_first_byte() {
        let id = ChunkId([0xAB; 32]);
        assert_eq!(id.shard_prefix(), "ab");
    }

    #[test]
    fn empty_data_produces_valid_id() {
        let key = test_chunk_id_key();
        let id = ChunkId::compute(&key, b"");
        assert_eq!(id.to_hex().len(), 64);
        assert_ne!(id.0, [0u8; 32]);
    }

    #[test]
    fn serde_roundtrip() {
        let key = test_chunk_id_key();
        let id = ChunkId::compute(&key, b"roundtrip test");
        let serialized = rmp_serde::to_vec(&id).unwrap();
        let deserialized: ChunkId = rmp_serde::from_slice(&serialized).unwrap();
        assert_eq!(id, deserialized);
    }
}
