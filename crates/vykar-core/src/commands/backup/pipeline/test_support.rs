//! Test fixtures shared across `pipeline/` submodule test blocks.

use crate::platform::fs;
use crate::snapshot::item::Item;
use crate::snapshot::SnapshotStats;

pub(super) fn test_item(path: &str) -> Item {
    Item {
        size: 1024,
        ..Item::test_file(path)
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
        nlink: 1,
        size: 1024,
        is_dataless: false,
    }
}

pub(super) fn test_stats() -> SnapshotStats {
    SnapshotStats::default()
}
