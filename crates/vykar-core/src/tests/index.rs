use crate::index::ChunkIndex;
use crate::testutil::test_chunk_hasher;
use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;

fn make_id(data: &[u8]) -> ChunkId {
    ChunkId::compute(&test_chunk_hasher(), data)
}

fn dummy_pack_id() -> PackId {
    PackId::from_bytes([0x00; 32])
}

#[test]
fn new_index_is_empty() {
    let index = ChunkIndex::new();
    assert!(index.is_empty());
    assert_eq!(index.len(), 0);
}

#[test]
fn add_and_contains() {
    let mut index = ChunkIndex::new();
    let id = make_id(b"chunk1");
    assert!(!index.contains(&id));
    index.add(id, 100, dummy_pack_id(), 0);
    assert!(index.contains(&id));
    assert_eq!(index.len(), 1);
}

#[test]
fn add_of_a_present_chunk_keeps_its_entry() {
    let mut index = ChunkIndex::new();
    let id = make_id(b"chunk1");
    index.add(id, 100, dummy_pack_id(), 0);
    index.add(id, 300, PackId::from_bytes([0x11; 32]), 7);
    let e = index.get(&id).unwrap();
    assert_eq!(
        (e.stored_size, e.pack_id, e.pack_offset),
        (100, dummy_pack_id(), 0)
    );
    // Still only one entry
    assert_eq!(index.len(), 1);
}

#[test]
fn get_returns_none_for_missing() {
    let index = ChunkIndex::new();
    let id = make_id(b"nonexistent");
    assert!(index.get(&id).is_none());
}

#[test]
fn stored_size_preserved() {
    let mut index = ChunkIndex::new();
    let id = make_id(b"chunk1");
    index.add(id, 42, dummy_pack_id(), 0);
    assert_eq!(index.get(&id).unwrap().stored_size, 42);
}

#[test]
fn count_distinct_packs() {
    let mut index = ChunkIndex::new();
    let id1 = make_id(b"chunk1");
    let id2 = make_id(b"chunk2");
    let id3 = make_id(b"chunk3");
    let pack_a = PackId::from_bytes([0xAA; 32]);
    let pack_b = PackId::from_bytes([0xBB; 32]);
    index.add(id1, 100, pack_a, 0);
    index.add(id2, 200, pack_a, 100);
    index.add(id3, 300, pack_b, 0);
    assert_eq!(index.count_distinct_packs(), 2);
}
