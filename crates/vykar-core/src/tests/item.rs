use crate::snapshot::item::{ChunkRef, Item, ItemType};
use vykar_types::chunk_id::ChunkId;

fn make_file_item() -> Item {
    Item {
        path: "home/user/file.txt".to_string(),
        entry_type: ItemType::RegularFile,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        user: Some("testuser".to_string()),
        group: Some("testgroup".to_string()),
        mtime: 1_700_000_000_000_000_000,
        atime: Some(1_700_000_001_000_000_000),
        ctime: Some(1_700_000_002_000_000_000),
        size: 4096,
        chunks: vec![ChunkRef {
            id: ChunkId([0xAA; 32]),
            size: 4096,
            csize: 2048,
        }],
        link_target: None,
        xattrs: None,
    }
}

fn make_dir_item() -> Item {
    Item {
        path: "home/user".to_string(),
        entry_type: ItemType::Directory,
        mode: 0o755,
        uid: 1000,
        gid: 1000,
        user: None,
        group: None,
        mtime: 1_700_000_000_000_000_000,
        atime: None,
        ctime: None,
        size: 0,
        chunks: vec![],
        link_target: None,
        xattrs: None,
    }
}

fn make_symlink_item() -> Item {
    Item {
        path: "home/user/link".to_string(),
        entry_type: ItemType::Symlink,
        mode: 0o777,
        uid: 1000,
        gid: 1000,
        user: None,
        group: None,
        mtime: 1_700_000_000_000_000_000,
        atime: None,
        ctime: None,
        size: 0,
        chunks: vec![],
        link_target: Some("/usr/bin/target".to_string()),
        xattrs: None,
    }
}

#[test]
fn item_serde_roundtrip_regular_file() {
    let item = make_file_item();
    let serialized = rmp_serde::to_vec(&item).unwrap();
    let deserialized: Item = rmp_serde::from_slice(&serialized).unwrap();
    assert_eq!(deserialized.path, item.path);
    assert_eq!(deserialized.entry_type, ItemType::RegularFile);
    assert_eq!(deserialized.mode, 0o644);
    assert_eq!(deserialized.size, 4096);
    assert_eq!(deserialized.chunks.len(), 1);
    assert_eq!(deserialized.chunks[0].id, ChunkId([0xAA; 32]));
}

#[test]
fn item_serde_roundtrip_directory() {
    let item = make_dir_item();
    let serialized = rmp_serde::to_vec(&item).unwrap();
    let deserialized: Item = rmp_serde::from_slice(&serialized).unwrap();
    assert_eq!(deserialized.path, "home/user");
    assert_eq!(deserialized.entry_type, ItemType::Directory);
    assert!(deserialized.chunks.is_empty());
}

#[test]
fn item_serde_roundtrip_symlink() {
    let item = make_symlink_item();
    let serialized = rmp_serde::to_vec(&item).unwrap();
    let deserialized: Item = rmp_serde::from_slice(&serialized).unwrap();
    assert_eq!(deserialized.entry_type, ItemType::Symlink);
    assert_eq!(deserialized.link_target.as_deref(), Some("/usr/bin/target"));
}

#[test]
fn items_vec_serde_roundtrip() {
    let items = vec![make_file_item(), make_dir_item(), make_symlink_item()];
    let serialized = rmp_serde::to_vec(&items).unwrap();
    let deserialized: Vec<Item> = rmp_serde::from_slice(&serialized).unwrap();
    assert_eq!(deserialized.len(), 3);
    assert_eq!(deserialized[0].entry_type, ItemType::RegularFile);
    assert_eq!(deserialized[1].entry_type, ItemType::Directory);
    assert_eq!(deserialized[2].entry_type, ItemType::Symlink);
}
