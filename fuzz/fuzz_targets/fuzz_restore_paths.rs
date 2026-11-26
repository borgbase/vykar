#![no_main]
//! Restore's path sanitizer and symlink audit over an item stream. Anyone
//! who holds the repository key can write any items, so restore does not
//! trust them.
//!
//! Both are checked against an independent model over the path bytes
//! (Unix semantics, the restore host's).

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

use libfuzzer_sys::fuzz_target;
use vykar_core::snapshot::item::{Item, ItemType};

/// The relative path restore must write `raw` to, or `None` if it must
/// refuse: no leading `/`, no `..` segment, and something left once empty
/// and `.` segments are dropped.
fn model_rel_path(raw: &[u8]) -> Option<PathBuf> {
    if raw.first() == Some(&b'/') {
        return None;
    }
    let mut out = PathBuf::new();
    for segment in raw.split(|&b| b == b'/') {
        match segment {
            b"" | b"." => {}
            b".." => return None,
            part => out.push(OsStr::from_bytes(part)),
        }
    }
    (!out.as_os_str().is_empty()).then_some(out)
}

/// Whether the symlink audit must flag `raw`.
fn model_flagged(raw: &[u8]) -> bool {
    raw.first() == Some(&b'/') || raw.split(|&b| b == b'/').any(|s| s == b"..")
}

fn check(item: &Item) {
    let raw_path = item
        .raw_names
        .as_ref()
        .and_then(|r| r.path.as_deref())
        .unwrap_or(item.path.as_bytes());
    let rel = vykar_core::fuzzing::restore_rel_path(item).ok();
    assert_eq!(rel, model_rel_path(raw_path), "path {raw_path:?}");

    if let Some(rel) = rel {
        assert!(rel.components().all(|c| matches!(c, Component::Normal(_))));
        let root = Path::new("/restore/root");
        let joined = root.join(&rel);
        assert!(joined.starts_with(root));
        assert_eq!(
            joined.components().count(),
            root.components().count() + rel.components().count()
        );
    }

    if item.entry_type == ItemType::Symlink {
        let raw_target = item
            .raw_names
            .as_ref()
            .and_then(|r| r.link_target.as_deref())
            .unwrap_or(item.link_target.as_deref().unwrap_or_default().as_bytes());
        let (target, flagged) = vykar_core::fuzzing::symlink_target(item);
        assert_eq!(target.as_os_str().as_bytes(), raw_target);
        assert_eq!(flagged, model_flagged(raw_target), "target {raw_target:?}");
    }
}

fuzz_target!(|data: &[u8]| {
    let _ = vykar_core::commands::list::for_each_decoded_item(data, |item| {
        // Restore validates every item before it looks at the path.
        if item.validate().is_ok() {
            check(&item);
        }
        Ok(())
    });
});
