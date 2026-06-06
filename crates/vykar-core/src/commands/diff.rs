use std::collections::BTreeMap;

use crate::config::VykarConfig;
use crate::repo::manifest::SnapshotEntry;
use crate::repo::{OpenOptions, Repository};
use crate::snapshot::item::ItemType;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffChangeKind {
    Added,
    Removed,
    Modified,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffEntry {
    pub path: String,
    pub change: DiffChangeKind,
    pub old_size: Option<u64>,
    pub new_size: Option<u64>,
    pub size_delta: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffResult {
    pub base_snapshot: String,
    pub target_snapshot: String,
    pub entries: Vec<DiffEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileSig {
    size: u64,
    chunk_ids: Vec<ChunkId>,
}

pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_a: &str,
    snapshot_b: &str,
) -> Result<DiffResult> {
    let (mut repo, _session_guard) =
        super::util::open_repo_with_read_session(config, passphrase, OpenOptions::new())?;

    let entry_a = repo.manifest().resolve_snapshot(snapshot_a)?.clone();
    let entry_b = repo.manifest().resolve_snapshot(snapshot_b)?.clone();

    ensure_distinct_snapshots(&entry_a, &entry_b)?;

    let (base, target) = order_snapshots(entry_a, entry_b);
    let base_map = load_regular_file_map(&mut repo, &base.name)?;
    let target_map = load_regular_file_map(&mut repo, &target.name)?;

    Ok(DiffResult {
        base_snapshot: base.name,
        target_snapshot: target.name,
        entries: diff_maps(&base_map, &target_map),
    })
}

fn order_snapshots(
    snapshot_a: SnapshotEntry,
    snapshot_b: SnapshotEntry,
) -> (SnapshotEntry, SnapshotEntry) {
    if snapshot_a.time <= snapshot_b.time {
        (snapshot_a, snapshot_b)
    } else {
        (snapshot_b, snapshot_a)
    }
}

fn ensure_distinct_snapshots(snapshot_a: &SnapshotEntry, snapshot_b: &SnapshotEntry) -> Result<()> {
    if snapshot_a.id == snapshot_b.id {
        return Err(VykarError::Other(format!(
            "cannot diff snapshot '{}' against itself",
            snapshot_a.name
        )));
    }
    Ok(())
}

fn load_regular_file_map(
    repo: &mut Repository,
    snapshot_name: &str,
) -> Result<BTreeMap<String, FileSig>> {
    let stream = super::list::load_snapshot_item_stream_cache_first(repo, snapshot_name)?;
    let mut map = BTreeMap::new();
    super::list::for_each_decoded_item(&stream, |item| {
        if item.entry_type == ItemType::RegularFile {
            let chunk_ids = item.chunks.into_iter().map(|chunk| chunk.id).collect();
            map.insert(
                item.path,
                FileSig {
                    size: item.size,
                    chunk_ids,
                },
            );
        }
        Ok(())
    })?;
    Ok(map)
}

fn diff_maps(
    base: &BTreeMap<String, FileSig>,
    target: &BTreeMap<String, FileSig>,
) -> Vec<DiffEntry> {
    let mut entries = Vec::new();

    for (path, old) in base {
        match target.get(path) {
            None => entries.push(DiffEntry {
                path: path.clone(),
                change: DiffChangeKind::Removed,
                old_size: Some(old.size),
                new_size: None,
                size_delta: size_delta(old.size, 0),
            }),
            Some(new) if old.size != new.size || old.chunk_ids != new.chunk_ids => {
                entries.push(DiffEntry {
                    path: path.clone(),
                    change: DiffChangeKind::Modified,
                    old_size: Some(old.size),
                    new_size: Some(new.size),
                    size_delta: size_delta(old.size, new.size),
                });
            }
            Some(_) => {}
        }
    }

    for (path, new) in target {
        if !base.contains_key(path) {
            entries.push(DiffEntry {
                path: path.clone(),
                change: DiffChangeKind::Added,
                old_size: None,
                new_size: Some(new.size),
                size_delta: size_delta(0, new.size),
            });
        }
    }

    entries.sort_by(|a, b| a.path.cmp(&b.path));
    entries
}

fn size_delta(old_size: u64, new_size: u64) -> i64 {
    let delta = i128::from(new_size) - i128::from(old_size);
    delta.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};
    use vykar_types::snapshot_id::SnapshotId;

    use super::*;
    use crate::snapshot::item::{ChunkRef, Item};

    fn map_from(items: Vec<Item>) -> BTreeMap<String, FileSig> {
        items
            .into_iter()
            .filter(|item| item.entry_type == ItemType::RegularFile)
            .map(|item| {
                let chunk_ids = item.chunks.into_iter().map(|c| c.id).collect();
                (
                    item.path,
                    FileSig {
                        size: item.size,
                        chunk_ids,
                    },
                )
            })
            .collect()
    }

    fn diff_items(base: Vec<Item>, target: Vec<Item>) -> Vec<DiffEntry> {
        diff_maps(&map_from(base), &map_from(target))
    }

    fn file(path: &str, size: u64, chunk_bytes: &[u8]) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 1,
            atime: None,
            ctime: None,
            size,
            chunks: chunk_bytes
                .iter()
                .map(|b| ChunkRef {
                    id: ChunkId::from_bytes([*b; 32]),
                    size: size as u32,
                    csize: size as u32,
                })
                .collect(),
            link_target: None,
            xattrs: None,
        }
    }

    fn non_file(path: &str, entry_type: ItemType) -> Item {
        Item {
            path: path.to_string(),
            entry_type,
            mode: 0o755,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 1,
            atime: None,
            ctime: None,
            size: 0,
            chunks: Vec::new(),
            link_target: (entry_type == ItemType::Symlink).then(|| "target".to_string()),
            xattrs: None,
        }
    }

    fn entry(name: &str, seconds: i64) -> SnapshotEntry {
        SnapshotEntry {
            name: name.to_string(),
            id: SnapshotId::from_bytes([seconds as u8; 32]),
            time: Utc.timestamp_opt(seconds, 0).unwrap(),
            source_label: String::new(),
            label: String::new(),
            source_paths: Vec::new(),
            hostname: String::new(),
        }
    }

    fn kinds(entries: &[DiffEntry]) -> Vec<(&str, DiffChangeKind)> {
        entries
            .iter()
            .map(|e| (e.path.as_str(), e.change))
            .collect()
    }

    #[test]
    fn classifies_added_removed_modified_and_omits_unchanged() {
        let entries = diff_items(
            vec![
                file("modified.txt", 3, &[1]),
                file("removed.txt", 4, &[2]),
                file("same.txt", 5, &[3]),
            ],
            vec![
                file("added.txt", 6, &[4]),
                file("modified.txt", 7, &[5]),
                file("same.txt", 5, &[3]),
            ],
        );

        assert_eq!(
            kinds(&entries),
            vec![
                ("added.txt", DiffChangeKind::Added),
                ("modified.txt", DiffChangeKind::Modified),
                ("removed.txt", DiffChangeKind::Removed),
            ]
        );
    }

    #[test]
    fn modified_when_same_size_but_chunk_ids_differ() {
        let entries = diff_items(
            vec![file("same-size.txt", 5, &[1])],
            vec![file("same-size.txt", 5, &[2])],
        );

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].change, DiffChangeKind::Modified);
        assert_eq!(entries[0].old_size, Some(5));
        assert_eq!(entries[0].new_size, Some(5));
        assert_eq!(entries[0].size_delta, 0);
    }

    #[test]
    fn ignores_directories_symlinks_and_metadata_only_changes() {
        let mut changed_metadata = file("metadata.txt", 5, &[1]);
        changed_metadata.mtime = 99;
        changed_metadata.mode = 0o600;
        changed_metadata.uid = 2000;
        changed_metadata.gid = 2000;
        changed_metadata.xattrs = Some(HashMap::from([("k".to_string(), b"v".to_vec())]));

        let entries = diff_items(
            vec![
                file("metadata.txt", 5, &[1]),
                non_file("dir", ItemType::Directory),
                non_file("link", ItemType::Symlink),
            ],
            vec![
                changed_metadata,
                non_file("dir", ItemType::Directory),
                non_file("link", ItemType::Symlink),
            ],
        );

        assert!(entries.is_empty());
    }

    #[test]
    fn reversed_snapshot_arguments_still_order_by_time() {
        let newer = entry("newer", 20);
        let older = entry("older", 10);

        let (base, target) = order_snapshots(newer, older);

        assert_eq!(base.name, "older");
        assert_eq!(target.name, "newer");
    }

    #[test]
    fn equal_timestamps_keep_provided_order() {
        let first = entry("first", 10);
        let second = entry("second", 10);

        let (base, target) = order_snapshots(first, second);

        assert_eq!(base.name, "first");
        assert_eq!(target.name, "second");
    }

    #[test]
    fn same_snapshot_is_rejected() {
        let first = entry("snap", 10);
        let mut second = entry("alias", 20);
        second.id = first.id;

        let result = ensure_distinct_snapshots(&first, &second);

        assert!(
            matches!(result, Err(VykarError::Other(message)) if message.contains("against itself"))
        );
    }

    /// Regression: streaming items through `for_each_decoded_item` into a
    /// `FileSig` map must produce the same input that `diff_maps` operates
    /// on as the previous eager `regular_file_map(Vec<Item>)` did.
    #[test]
    fn diff_with_streaming_loader_matches_collected_loader() {
        let base_items = vec![
            file("modified.txt", 3, &[1]),
            file("removed.txt", 4, &[2]),
            file("same.txt", 5, &[3]),
            non_file("dir", ItemType::Directory),
        ];
        let target_items = vec![
            file("added.txt", 6, &[4]),
            file("modified.txt", 7, &[5]),
            file("same.txt", 5, &[3]),
            non_file("link", ItemType::Symlink),
        ];

        let eager_base = map_from(base_items.clone());
        let eager_target = map_from(target_items.clone());

        let streamed_base = stream_to_map(&base_items);
        let streamed_target = stream_to_map(&target_items);

        assert_eq!(streamed_base, eager_base);
        assert_eq!(streamed_target, eager_target);

        let entries = diff_maps(&streamed_base, &streamed_target);
        assert_eq!(
            kinds(&entries),
            vec![
                ("added.txt", DiffChangeKind::Added),
                ("modified.txt", DiffChangeKind::Modified),
                ("removed.txt", DiffChangeKind::Removed),
            ]
        );
    }

    fn stream_to_map(items: &[Item]) -> BTreeMap<String, FileSig> {
        let mut bytes = Vec::new();
        for item in items {
            bytes.extend_from_slice(&rmp_serde::to_vec(item).unwrap());
        }
        let mut map = BTreeMap::new();
        super::super::list::for_each_decoded_item(&bytes, |item| {
            if item.entry_type == ItemType::RegularFile {
                let chunk_ids = item.chunks.into_iter().map(|c| c.id).collect();
                map.insert(
                    item.path,
                    FileSig {
                        size: item.size,
                        chunk_ids,
                    },
                );
            }
            Ok(())
        })
        .unwrap();
        map
    }

    #[test]
    fn size_delta_clamps_on_overflow() {
        // (new - old) where new = u64::MAX and old = 0 overflows i64.
        assert_eq!(size_delta(0, u64::MAX), i64::MAX);
        // Symmetric negative side.
        assert_eq!(size_delta(u64::MAX, 0), i64::MIN);
        // Within range remains exact.
        assert_eq!(size_delta(10, 30), 20);
        assert_eq!(size_delta(30, 10), -20);
        assert_eq!(size_delta(0, 0), 0);
    }
}
