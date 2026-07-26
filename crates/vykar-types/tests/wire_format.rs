//! Golden MessagePack encodings for the 32-byte repository identifiers.
//!
//! `ChunkId`, `PackId` and `SnapshotId` are embedded in the chunk index, in
//! snapshot blobs and in pack references — every existing repository on disk
//! depends on their encoding. A round-trip test cannot catch a change here
//! (both halves move together), so these assert the exact bytes for a fixed
//! input. A failure means the wire format changed and old repositories became
//! unreadable; it is never a stale expectation to regenerate.

#![allow(clippy::unwrap_used)]

use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

/// Fixed 32-byte input spanning both MessagePack integer encodings: values
/// below 0x80 are positive fixints (one byte), values from 0x80 up need the
/// `0xcc` uint8 prefix.
fn golden_raw() -> [u8; 32] {
    let mut raw = [0u8; 32];
    for (i, b) in raw.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(9);
    }
    raw
}

/// `array16` header (0xdc 0x00 0x20) followed by the 32 bytes, each as a
/// positive fixint or a `0xcc`-prefixed uint8.
const GOLDEN_HEX: &str = concat!(
    "dc0020",
    "0009121b242d363f48515a636c757e",
    "cc87cc90cc99cca2ccabccb4ccbdccc6cccfccd8cce1cceaccf3ccfc",
    "050e17",
);

#[test]
fn chunk_id_msgpack_bytes_are_stable() {
    let encoded = rmp_serde::to_vec(&ChunkId::from_bytes(golden_raw())).unwrap();
    assert_eq!(hex::encode(&encoded), GOLDEN_HEX);
}

#[test]
fn pack_id_msgpack_bytes_are_stable() {
    let encoded = rmp_serde::to_vec(&PackId::from_bytes(golden_raw())).unwrap();
    assert_eq!(hex::encode(&encoded), GOLDEN_HEX);
}

#[test]
fn snapshot_id_msgpack_bytes_are_stable() {
    let encoded = rmp_serde::to_vec(&SnapshotId::from_bytes(golden_raw())).unwrap();
    assert_eq!(hex::encode(&encoded), GOLDEN_HEX);
}

/// The three ids are wire-interchangeable newtypes over `[u8; 32]`; decoding
/// must accept the golden bytes and yield the original array.
#[test]
fn golden_bytes_decode_back_to_the_same_raw_array() {
    let encoded = hex::decode(GOLDEN_HEX).unwrap();
    let chunk: ChunkId = rmp_serde::from_slice(&encoded).unwrap();
    let pack: PackId = rmp_serde::from_slice(&encoded).unwrap();
    let snapshot: SnapshotId = rmp_serde::from_slice(&encoded).unwrap();
    assert_eq!(*chunk.as_bytes(), golden_raw());
    assert_eq!(*pack.as_bytes(), golden_raw());
    assert_eq!(*snapshot.as_bytes(), golden_raw());
}
