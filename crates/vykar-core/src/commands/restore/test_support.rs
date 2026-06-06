//! Test fixtures shared across `restore/` submodule test blocks.

use crate::compress::Compression;
use crate::index::ChunkIndex;
use crate::repo::format::{pack_object_with_context, ObjectType};
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use smallvec::SmallVec;
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;

use super::plan::WriteTarget;
use super::read_groups::{PlannedBlob, ReadGroup};

pub(super) fn dummy_chunk_id(byte: u8) -> ChunkId {
    ChunkId::from_bytes([byte; 32])
}

pub(super) fn dummy_pack_id(byte: u8) -> PackId {
    PackId::from_bytes([byte; 32])
}

/// Helper: create a lookup closure from a ChunkIndex.
pub(super) fn index_lookup(
    index: &ChunkIndex,
) -> impl Fn(&ChunkId) -> Option<(PackId, u64, u32)> + '_ {
    move |id| {
        index
            .get(id)
            .map(|e| (e.pack_id, e.pack_offset, e.stored_size))
    }
}

pub(super) fn make_file_item(path: &str, chunks: Vec<(u8, u32)>) -> Item {
    Item {
        path: path.to_string(),
        entry_type: ItemType::RegularFile,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        user: None,
        group: None,
        mtime: 0,
        atime: None,
        ctime: None,
        size: chunks.iter().map(|(_, s)| *s as u64).sum(),
        chunks: chunks
            .into_iter()
            .map(|(id_byte, size)| ChunkRef {
                id: dummy_chunk_id(id_byte),
                size,
                csize: size, // not used by plan_reads
            })
            .collect(),
        link_target: None,
        xattrs: None,
    }
}

pub(super) fn make_dir_item(path: &str, mode: u32) -> Item {
    Item {
        path: path.to_string(),
        entry_type: ItemType::Directory,
        mode,
        uid: 1000,
        gid: 1000,
        user: None,
        group: None,
        mtime: 0,
        atime: None,
        ctime: None,
        size: 0,
        chunks: Vec::new(),
        link_target: None,
        xattrs: None,
    }
}

pub(super) fn make_symlink_item(path: &str, target: &str) -> Item {
    Item {
        path: path.to_string(),
        entry_type: ItemType::Symlink,
        mode: 0o777,
        uid: 1000,
        gid: 1000,
        user: None,
        group: None,
        mtime: 0,
        atime: None,
        ctime: None,
        size: 0,
        chunks: Vec::new(),
        link_target: Some(target.to_string()),
        xattrs: None,
    }
}

pub(super) fn serialize_items(items: &[Item]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for item in items {
        bytes.extend_from_slice(&rmp_serde::to_vec(item).unwrap());
    }
    bytes
}

/// Build a file Item whose `size` field is set independently from the
/// chunk sizes, so tests can construct malformed snapshot items.
pub(super) fn make_file_item_with_size(path: &str, size: u64, chunks: Vec<(u8, u32)>) -> Item {
    let mut item = make_file_item(path, chunks);
    item.size = size;
    item
}

/// Helper: compress + encrypt + pack a raw payload into a RepoObj blob.
pub(super) fn pack_blob(chunk_id: ChunkId, data: &[u8], crypto: &dyn CryptoEngine) -> Vec<u8> {
    let compressed = crate::compress::compress(Compression::None, data).unwrap();
    pack_object_with_context(
        ObjectType::ChunkData,
        chunk_id.as_bytes(),
        &compressed,
        crypto,
    )
    .unwrap()
}

/// Build a single-blob ReadGroup from the given packed bytes.
pub(super) fn single_blob_group(
    pack_id: PackId,
    chunk_id: ChunkId,
    packed: &[u8],
    expected_size: u32,
    targets: SmallVec<[WriteTarget; 1]>,
) -> ReadGroup {
    ReadGroup {
        pack_id,
        read_start: 0,
        read_end: packed.len() as u64,
        blobs: vec![PlannedBlob {
            chunk_id,
            pack_offset: 0,
            stored_size: packed.len() as u32,
            expected_size,
            targets,
        }],
    }
}

/// Concatenate multiple packed blobs into one pack buffer and build a
/// ReadGroup with one PlannedBlob per entry.
#[allow(clippy::type_complexity)]
pub(super) fn multi_blob_group(
    pack_id: PackId,
    entries: Vec<(ChunkId, Vec<u8>, u32, SmallVec<[WriteTarget; 1]>)>,
) -> (Vec<u8>, ReadGroup) {
    let mut pack_data = Vec::new();
    let mut blobs = Vec::new();
    for (chunk_id, packed, expected_size, targets) in entries {
        let offset = pack_data.len() as u64;
        let stored_size = packed.len() as u32;
        pack_data.extend_from_slice(&packed);
        blobs.push(PlannedBlob {
            chunk_id,
            pack_offset: offset,
            stored_size,
            expected_size,
            targets,
        });
    }
    let group = ReadGroup {
        pack_id,
        read_start: 0,
        read_end: pack_data.len() as u64,
        blobs,
    };
    (pack_data, group)
}
