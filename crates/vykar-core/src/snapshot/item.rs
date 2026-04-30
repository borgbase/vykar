use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

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

impl Item {
    /// Validate per-item invariants. Cross-item invariants (e.g. duplicate
    /// paths) are the caller's responsibility.
    pub fn validate(&self) -> Result<()> {
        match self.entry_type {
            ItemType::RegularFile => {
                if self.link_target.is_some() {
                    return Err(VykarError::InvalidFormat(format!(
                        "item '{}': regular file has link_target",
                        self.path
                    )));
                }
                let sum: u64 = self.chunks.iter().map(|c| c.size as u64).sum();
                if sum != self.size {
                    return Err(VykarError::InvalidFormat(format!(
                        "regular file {:?} has size {} but chunk sizes sum to {} \
                         (likely produced by a vykar version before the 2026-04 TOCTOU fix \
                         when the file changed during backup; run `vykar check --repair` \
                         to drop the affected item — the snapshot is rewritten under a new \
                         id and other items are preserved)",
                        self.path, self.size, sum
                    )));
                }
            }
            ItemType::Directory => {
                if !self.chunks.is_empty() {
                    return Err(VykarError::InvalidFormat(format!(
                        "item '{}': directory has {} chunks",
                        self.path,
                        self.chunks.len()
                    )));
                }
                if self.link_target.is_some() {
                    return Err(VykarError::InvalidFormat(format!(
                        "item '{}': directory has link_target",
                        self.path
                    )));
                }
                // Deliberately do NOT assert size == 0. Historical snapshots
                // may not guarantee it and we don't want to break reads.
            }
            ItemType::Symlink => {
                if !self.chunks.is_empty() {
                    return Err(VykarError::InvalidFormat(format!(
                        "item '{}': symlink has {} chunks",
                        self.path,
                        self.chunks.len()
                    )));
                }
                let Some(target) = self.link_target.as_deref() else {
                    return Err(VykarError::InvalidFormat(format!(
                        "item '{}': symlink missing link_target",
                        self.path
                    )));
                };
                if target.is_empty() {
                    return Err(VykarError::InvalidFormat(format!(
                        "item '{}': symlink link_target is empty",
                        self.path
                    )));
                }
                if target.as_bytes().contains(&0) {
                    return Err(VykarError::InvalidFormat(format!(
                        "item '{}': symlink link_target contains NUL byte",
                        self.path
                    )));
                }
                if target.len() > 4096 {
                    return Err(VykarError::InvalidFormat(format!(
                        "item '{}': symlink link_target exceeds PATH_MAX ({} bytes)",
                        self.path,
                        target.len()
                    )));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_item(entry_type: ItemType, path: &str) -> Item {
        Item {
            path: path.to_string(),
            entry_type,
            mode: 0o644,
            uid: 0,
            gid: 0,
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

    fn chunk(size: u32) -> ChunkRef {
        ChunkRef {
            id: ChunkId([0u8; 32]),
            size,
            csize: size,
        }
    }

    #[test]
    fn validate_ok_regular_file_with_matching_size() {
        let mut item = base_item(ItemType::RegularFile, "a.txt");
        item.chunks = vec![chunk(100), chunk(50)];
        item.size = 150;
        item.validate().unwrap();
    }

    #[test]
    fn validate_ok_empty_regular_file() {
        let item = base_item(ItemType::RegularFile, "empty.txt");
        item.validate().unwrap();
    }

    #[test]
    fn validate_rejects_regular_file_with_wrong_sum() {
        let mut item = base_item(ItemType::RegularFile, "a.txt");
        item.chunks = vec![chunk(50)];
        item.size = 100;
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("chunk sizes sum to"), "got: {err}");
        assert!(err.contains("vykar check --repair"), "got: {err}");
    }

    #[test]
    fn validate_rejects_regular_file_with_link_target() {
        let mut item = base_item(ItemType::RegularFile, "a.txt");
        item.link_target = Some("target".into());
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("regular file has link_target"), "got: {err}");
    }

    #[test]
    fn validate_ok_directory() {
        let item = base_item(ItemType::Directory, "dir");
        item.validate().unwrap();
    }

    #[test]
    fn validate_rejects_directory_with_chunks() {
        let mut item = base_item(ItemType::Directory, "dir");
        item.chunks = vec![chunk(10)];
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("directory has 1 chunks"), "got: {err}");
    }

    #[test]
    fn validate_rejects_directory_with_link_target() {
        let mut item = base_item(ItemType::Directory, "dir");
        item.link_target = Some("target".into());
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("directory has link_target"), "got: {err}");
    }

    #[test]
    fn validate_ok_symlink() {
        let mut item = base_item(ItemType::Symlink, "link");
        item.link_target = Some("target".into());
        item.validate().unwrap();
    }

    #[test]
    fn validate_rejects_symlink_without_link_target() {
        let item = base_item(ItemType::Symlink, "link");
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("symlink missing link_target"), "got: {err}");
    }

    #[test]
    fn validate_rejects_symlink_with_empty_target() {
        let mut item = base_item(ItemType::Symlink, "link");
        item.link_target = Some(String::new());
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("symlink link_target is empty"), "got: {err}");
    }

    #[test]
    fn validate_rejects_symlink_with_nul_byte_target() {
        let mut item = base_item(ItemType::Symlink, "link");
        item.link_target = Some("foo\0bar".into());
        let err = item.validate().unwrap_err().to_string();
        assert!(
            err.contains("symlink link_target contains NUL byte"),
            "got: {err}"
        );
    }

    #[test]
    fn validate_rejects_symlink_with_overlong_target() {
        let mut item = base_item(ItemType::Symlink, "link");
        item.link_target = Some("a".repeat(4097));
        let err = item.validate().unwrap_err().to_string();
        assert!(
            err.contains("symlink link_target exceeds PATH_MAX"),
            "got: {err}"
        );
    }

    #[test]
    fn validate_rejects_symlink_with_chunks() {
        let mut item = base_item(ItemType::Symlink, "link");
        item.chunks = vec![chunk(10)];
        item.link_target = Some("target".into());
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("symlink has 1 chunks"), "got: {err}");
    }
}
