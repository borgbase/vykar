//! Test fixtures shared across `pipeline/` submodule test blocks.

use crate::platform::fs;
use crate::snapshot::item::{Item, ItemType};
use crate::snapshot::SnapshotStats;

pub(super) fn test_item(path: &str) -> Item {
    Item {
        path: path.to_string(),
        entry_type: ItemType::RegularFile,
        mode: 0o644,
        uid: 0,
        gid: 0,
        user: None,
        group: None,
        mtime: 0,
        atime: None,
        ctime: None,
        size: 1024,
        chunks: Vec::new(),
        link_target: None,
        xattrs: None,
    }
}

pub(super) fn test_metadata() -> fs::MetadataSummary {
    fs::MetadataSummary {
        mode: 0o644,
        uid: 0,
        gid: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        device: 0,
        inode: 0,
        size: 1024,
        is_dataless: false,
    }
}

pub(super) fn test_stats() -> SnapshotStats {
    SnapshotStats::default()
}
