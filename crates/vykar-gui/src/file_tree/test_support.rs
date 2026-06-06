use vykar_core::snapshot::item::{Item, ItemType};

use super::{FileTree, TreeNode};

pub(super) fn dir(path: &str) -> Item {
    Item {
        path: path.to_string(),
        entry_type: ItemType::Directory,
        mode: 0o755,
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

pub(super) fn file(path: &str, size: u64) -> Item {
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
        size,
        chunks: Vec::new(),
        link_target: None,
        xattrs: None,
    }
}

pub(super) fn find_node<'a>(tree: &'a FileTree, display_path: &str) -> Option<&'a TreeNode> {
    tree.arena.iter().find(|n| n.full_path == display_path)
}

pub(super) fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}
