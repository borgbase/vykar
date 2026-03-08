use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use vykar_types::chunk_id::ChunkId;

/// A single filesystem entry stored in a snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Item {
    pub path: String,
    pub entry_type: ItemType,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub group: Option<String>,
    /// Modification time in nanoseconds since Unix epoch.
    pub mtime: i64,
    #[serde(default)]
    pub atime: Option<i64>,
    #[serde(default)]
    pub ctime: Option<i64>,
    pub size: u64,
    /// For regular files: the chunks making up the content.
    #[serde(default)]
    pub chunks: Vec<ChunkRef>,
    /// For symlinks: the link target.
    #[serde(default)]
    pub link_target: Option<String>,
    /// Extended attributes.
    #[serde(default)]
    pub xattrs: Option<HashMap<String, Vec<u8>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemType {
    RegularFile,
    Directory,
    Symlink,
}

/// Reference to a chunk stored in the repository.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkRef {
    pub id: ChunkId,
    /// Uncompressed (original) size of this chunk.
    pub size: u32,
    /// Size as stored (compressed + encrypted).
    pub csize: u32,
}
