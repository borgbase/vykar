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
    /// Byte-faithful representations of `path` / `link_target` for entries
    /// whose names are not valid UTF-8 (Unix only). `path` / `link_target`
    /// remain the lossy UTF-8 **display** strings; these raw byte fields are
    /// the source of truth at restore time when present. `None` for the common
    /// (valid-UTF8) case — one nil byte per entry on the wire. See the Format
    /// Evolution section of `architecture.md` for the backward-readable decode
    /// contract that governs trailing optional fields like this one.
    #[serde(default)]
    pub raw_names: Option<ItemRawNames>,
    /// Hard-link group key for regular files with `nlink > 1` (Unix only),
    /// else `None`. Set to the source `(dev, ino)` so that all nodes sharing
    /// one inode can be relinked at restore time. Each node still carries its
    /// **full** chunk list, so a node whose group siblings were filtered out
    /// (partial restore) materializes from its own content — we never assume we
    /// see all N links. Trailing field for backward-readable decode (see the
    /// Format Evolution section of `architecture.md`); ships under snapshot
    /// format v1.
    #[serde(default)]
    pub hardlink: Option<HardlinkId>,
}

/// Source `(dev, ino)` identity of a regular file with `nlink > 1`, used to
/// regroup hard-linked nodes at restore time. `dev` disambiguates the same
/// `ino` across filesystems within a single snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HardlinkId {
    pub dev: u64,
    pub ino: u64,
}

/// Byte-faithful shadow of an `Item`'s name fields, populated only when the
/// corresponding display string lost information to `to_string_lossy`.
///
/// At least one of the two fields is `Some` whenever this struct is present
/// (enforced by [`Item::validate`]). Each present value is invalid UTF-8 and
/// `String::from_utf8_lossy(value)` equals the matching display string.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemRawNames {
    /// Raw bytes of the snapshot-relative path when `Item.path` is lossy.
    #[serde(default, with = "serde_bytes")]
    pub path: Option<Vec<u8>>,
    /// Raw bytes of the symlink target when `Item.link_target` is lossy.
    #[serde(default, with = "serde_bytes")]
    pub link_target: Option<Vec<u8>>,
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
    /// The path as raw bytes: the byte-faithful `raw_names.path` when present,
    /// else the UTF-8 bytes of the lossy display `path`. This is the
    /// correctness-bearing path for filesystem operations; `self.path` is the
    /// display/search string only.
    pub fn path_bytes(&self) -> &[u8] {
        match self.raw_names.as_ref().and_then(|r| r.path.as_deref()) {
            Some(raw) => raw,
            None => self.path.as_bytes(),
        }
    }

    /// The symlink target as raw bytes, or `None` for non-symlinks / symlinks
    /// missing a target. Prefers the byte-faithful `raw_names.link_target`.
    pub fn link_target_bytes(&self) -> Option<&[u8]> {
        if let Some(raw) = self
            .raw_names
            .as_ref()
            .and_then(|r| r.link_target.as_deref())
        {
            return Some(raw);
        }
        self.link_target.as_deref().map(str::as_bytes)
    }

    /// `true` when this item's path is stored byte-faithfully (non-UTF8 name).
    /// Used to bypass lossy-display-keyed parent reuse during backup.
    pub fn has_raw_path(&self) -> bool {
        self.raw_names.as_ref().is_some_and(|r| r.path.is_some())
    }

    /// Validate per-item invariants. Cross-item invariants (e.g. duplicate
    /// paths) are the caller's responsibility.
    pub fn validate(&self) -> Result<()> {
        self.validate_raw_names()?;
        // A hard-link group key is only meaningful on regular files (the v1
        // scope boundary); directories and symlinks must never carry one.
        if self.hardlink.is_some() && self.entry_type != ItemType::RegularFile {
            return Err(VykarError::InvalidFormat(format!(
                "item '{}': hardlink set on a non-regular-file entry",
                self.path
            )));
        }
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
                let Some(target) = self.link_target_bytes() else {
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
                if target.contains(&0) {
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

    /// Validate the optional `raw_names` shadow: when present it must carry at
    /// least one value, each present value must be genuinely non-UTF8 and its
    /// lossy render must equal the display shadow, and a raw `link_target`
    /// requires a symlink that also carries a display `link_target`.
    fn validate_raw_names(&self) -> Result<()> {
        let Some(raw) = self.raw_names.as_ref() else {
            return Ok(());
        };
        if raw.path.is_none() && raw.link_target.is_none() {
            return Err(VykarError::InvalidFormat(format!(
                "item '{}': raw_names present but carries no values",
                self.path
            )));
        }
        if let Some(raw_path) = raw.path.as_deref() {
            if std::str::from_utf8(raw_path).is_ok() {
                return Err(VykarError::InvalidFormat(format!(
                    "item '{}': raw_names.path is valid UTF-8 (must only shadow non-UTF8 names)",
                    self.path
                )));
            }
            if String::from_utf8_lossy(raw_path) != self.path {
                return Err(VykarError::InvalidFormat(format!(
                    "item '{}': raw_names.path lossy render does not match display path",
                    self.path
                )));
            }
        }
        if let Some(raw_target) = raw.link_target.as_deref() {
            if self.entry_type != ItemType::Symlink {
                return Err(VykarError::InvalidFormat(format!(
                    "item '{}': raw_names.link_target on a non-symlink",
                    self.path
                )));
            }
            let Some(display_target) = self.link_target.as_deref() else {
                return Err(VykarError::InvalidFormat(format!(
                    "item '{}': raw_names.link_target without a display link_target",
                    self.path
                )));
            };
            if std::str::from_utf8(raw_target).is_ok() {
                return Err(VykarError::InvalidFormat(format!(
                    "item '{}': raw_names.link_target is valid UTF-8 (must only shadow non-UTF8 targets)",
                    self.path
                )));
            }
            if String::from_utf8_lossy(raw_target) != display_target {
                return Err(VykarError::InvalidFormat(format!(
                    "item '{}': raw_names.link_target lossy render does not match display target",
                    self.path
                )));
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
            raw_names: None,
            hardlink: None,
        }
    }

    fn chunk(size: u32) -> ChunkRef {
        ChunkRef {
            id: ChunkId::from_bytes([0u8; 32]),
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

    // -----------------------------------------------------------------------
    // raw_names round-trip, back-compat, and validation
    // -----------------------------------------------------------------------

    /// A byte sequence that is invalid UTF-8 (0x80 is a stray continuation byte).
    const BAD_BYTES: &[u8] = b"bad-\x80-name";

    #[test]
    fn raw_names_round_trips_through_msgpack() {
        let mut item = base_item(ItemType::RegularFile, &String::from_utf8_lossy(BAD_BYTES));
        item.raw_names = Some(ItemRawNames {
            path: Some(BAD_BYTES.to_vec()),
            link_target: None,
        });
        item.validate().unwrap();
        let bytes = rmp_serde::to_vec(&item).unwrap();
        let decoded: Item = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded, item);
        assert_eq!(decoded.path_bytes(), BAD_BYTES);
    }

    #[test]
    fn old_array_without_raw_names_decodes_to_none() {
        // An Item from a pre-raw_names binary is a positional array with one
        // fewer element. `#[serde(default)]` must fill `raw_names` with None.
        #[derive(Serialize)]
        struct OldItem {
            path: String,
            entry_type: ItemType,
            mode: u32,
            uid: u32,
            gid: u32,
            user: Option<String>,
            group: Option<String>,
            mtime: i64,
            atime: Option<i64>,
            ctime: Option<i64>,
            size: u64,
            chunks: Vec<ChunkRef>,
            link_target: Option<String>,
            xattrs: Option<HashMap<String, Vec<u8>>>,
        }
        let old = OldItem {
            path: "a.txt".into(),
            entry_type: ItemType::RegularFile,
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
        };
        let bytes = rmp_serde::to_vec(&old).unwrap();
        let decoded: Item = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.raw_names, None);
        assert_eq!(decoded.path, "a.txt");
    }

    #[test]
    fn validate_rejects_raw_names_with_valid_utf8_path() {
        let mut item = base_item(ItemType::RegularFile, "ok.txt");
        item.raw_names = Some(ItemRawNames {
            path: Some(b"ok.txt".to_vec()),
            link_target: None,
        });
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("valid UTF-8"), "got: {err}");
    }

    #[test]
    fn validate_rejects_raw_path_lossy_mismatch() {
        // raw bytes are non-UTF8 but their lossy render does not equal `path`.
        let mut item = base_item(ItemType::RegularFile, "different");
        item.raw_names = Some(ItemRawNames {
            path: Some(BAD_BYTES.to_vec()),
            link_target: None,
        });
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("does not match display path"), "got: {err}");
    }

    #[test]
    fn validate_rejects_raw_names_with_no_values() {
        let mut item = base_item(ItemType::RegularFile, "a.txt");
        item.raw_names = Some(ItemRawNames {
            path: None,
            link_target: None,
        });
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("carries no values"), "got: {err}");
    }

    #[test]
    fn validate_rejects_raw_link_target_on_non_symlink() {
        let mut item = base_item(ItemType::RegularFile, "a.txt");
        item.raw_names = Some(ItemRawNames {
            path: None,
            link_target: Some(BAD_BYTES.to_vec()),
        });
        let err = item.validate().unwrap_err().to_string();
        assert!(err.contains("non-symlink"), "got: {err}");
    }

    #[test]
    fn validate_ok_symlink_with_raw_target() {
        let mut item = base_item(ItemType::Symlink, &String::from_utf8_lossy(BAD_BYTES));
        item.link_target = Some(String::from_utf8_lossy(BAD_BYTES).into_owned());
        item.raw_names = Some(ItemRawNames {
            path: Some(BAD_BYTES.to_vec()),
            link_target: Some(BAD_BYTES.to_vec()),
        });
        item.validate().unwrap();
        assert_eq!(item.link_target_bytes(), Some(BAD_BYTES));
    }

    // -----------------------------------------------------------------------
    // hardlink round-trip, validation, and reader-rejection
    // -----------------------------------------------------------------------

    #[test]
    fn hardlink_round_trips_through_msgpack() {
        let mut item = base_item(ItemType::RegularFile, "linked.bin");
        item.chunks = vec![chunk(64)];
        item.size = 64;
        item.hardlink = Some(HardlinkId {
            dev: 0x1234,
            ino: 0xABCD,
        });
        item.validate().unwrap();
        let bytes = rmp_serde::to_vec(&item).unwrap();
        let decoded: Item = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded, item);
        assert_eq!(
            decoded.hardlink,
            Some(HardlinkId {
                dev: 0x1234,
                ino: 0xABCD
            })
        );
    }

    #[test]
    fn validate_rejects_hardlink_on_directory() {
        let mut item = base_item(ItemType::Directory, "dir");
        item.hardlink = Some(HardlinkId { dev: 1, ino: 2 });
        let err = item.validate().unwrap_err().to_string();
        assert!(
            err.contains("hardlink set on a non-regular-file entry"),
            "got: {err}"
        );
    }

    #[test]
    fn validate_rejects_hardlink_on_symlink() {
        let mut item = base_item(ItemType::Symlink, "link");
        item.link_target = Some("target".into());
        item.hardlink = Some(HardlinkId { dev: 1, ino: 2 });
        let err = item.validate().unwrap_err().to_string();
        assert!(
            err.contains("hardlink set on a non-regular-file entry"),
            "got: {err}"
        );
    }

    /// The reverse of `old_array_without_raw_names_decodes_to_none`: a 16-field
    /// `Item` (carrying `hardlink`) must **fail** to decode into a 15-field
    /// struct that lacks it. rmp-serde rejects a longer-than-expected positional
    /// array, so a pre-hardlink reader refuses the record rather than silently
    /// mis-decoding it — the mechanism documented in `architecture.md`'s Format
    /// Evolution section ("an old reader hits a length mismatch").
    #[test]
    fn new_array_with_hardlink_rejected_by_pre_hardlink_reader() {
        // The exact field layout of `Item` *before* the `hardlink` field was
        // appended (15 positional fields, `raw_names` trailing).
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct PreHardlinkItem {
            path: String,
            entry_type: ItemType,
            mode: u32,
            uid: u32,
            gid: u32,
            user: Option<String>,
            group: Option<String>,
            mtime: i64,
            atime: Option<i64>,
            ctime: Option<i64>,
            size: u64,
            chunks: Vec<ChunkRef>,
            link_target: Option<String>,
            xattrs: Option<HashMap<String, Vec<u8>>>,
            raw_names: Option<ItemRawNames>,
        }

        let mut item = base_item(ItemType::RegularFile, "linked.bin");
        item.hardlink = Some(HardlinkId { dev: 1, ino: 2 });
        let bytes = rmp_serde::to_vec(&item).unwrap();

        let result: std::result::Result<PreHardlinkItem, _> = rmp_serde::from_slice(&bytes);
        assert!(
            result.is_err(),
            "a 16-field Item must not decode into the 15-field pre-hardlink struct"
        );
    }

    /// Symmetry with `old_array_without_raw_names_decodes_to_none`: a current
    /// reader fills `hardlink` with `None` when decoding a 15-field array
    /// written before the field existed.
    #[test]
    fn old_array_without_hardlink_decodes_to_none() {
        #[derive(Serialize)]
        struct OldItem {
            path: String,
            entry_type: ItemType,
            mode: u32,
            uid: u32,
            gid: u32,
            user: Option<String>,
            group: Option<String>,
            mtime: i64,
            atime: Option<i64>,
            ctime: Option<i64>,
            size: u64,
            chunks: Vec<ChunkRef>,
            link_target: Option<String>,
            xattrs: Option<HashMap<String, Vec<u8>>>,
            raw_names: Option<ItemRawNames>,
        }
        let old = OldItem {
            path: "a.txt".into(),
            entry_type: ItemType::RegularFile,
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
            raw_names: None,
        };
        let bytes = rmp_serde::to_vec(&old).unwrap();
        let decoded: Item = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.hardlink, None);
        assert_eq!(decoded.path, "a.txt");
    }
}
