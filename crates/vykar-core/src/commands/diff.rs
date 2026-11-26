use std::collections::{BTreeMap, HashSet};

use super::backup::command_dump::DUMPS_ROOT;
use super::list::{
    ItemStreamReader, for_each_decoded_item, load_snapshot_meta, resolve_from_index,
};
use super::listing::ListingView;
use super::mark::{
    ChunkOutcome, OnChunkError, TreeChunkSet, default_decode_threads, walk_tree_chunks,
};
use crate::config::VykarConfig;
use crate::index::hasher::{BuildChunkIdHasher, ChunkIdHashMap};
use crate::repo::control::CancelToken;
use crate::repo::manifest::SnapshotEntry;
use crate::repo::{OpenOptions, Repo, TreeChunkSource};
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::{Item, ItemType};
use crate::snapshot::listing::rel_in;
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

/// One regular file of a side: its display path, size and chunk IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileSig {
    path: String,
    size: u64,
    chunk_ids: Vec<ChunkId>,
}

/// A regular file as decoded from a tree chunk; `raw` is
/// `raw_names.path` when present, so [`Self::key`] is `Item::path_bytes()`.
#[derive(Debug)]
struct DecodedFile {
    raw: Option<Vec<u8>>,
    sig: FileSig,
}

impl DecodedFile {
    fn from_item(item: Item) -> Self {
        Self {
            raw: item.raw_names.and_then(|r| r.path),
            sig: FileSig {
                path: item.path,
                size: item.size,
                chunk_ids: item.chunks.into_iter().map(|c| c.id).collect(),
            },
        }
    }

    /// The comparison key: the raw path bytes, else the display path's.
    fn key(&self) -> &[u8] {
        self.raw.as_deref().unwrap_or(self.sig.path.as_bytes())
    }
}

/// A side of the diff: comparison key (`Item::path_bytes()`) -> the last
/// regular file with it.
type SideMap<'a> = BTreeMap<&'a [u8], &'a FileSig>;

/// The decoded tree chunks of a diff: each chunk's regular files, in order.
type DecodedChunks = ChunkIdHashMap<Vec<DecodedFile>>;

/// Why the full diff ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FullDiffReason {
    /// A tree chunk occurs more than once in one snapshot's `item_ptrs`.
    RepeatedPointer,
    /// A snapshot's key index has two segments whose roots are equal or
    /// nested (an empty root contains every other), so one path can occur
    /// in two segments.
    NestedSegments,
    /// Two regular files of one side have the same path among the decoded
    /// chunks.
    DuplicatePath,
}

impl FullDiffReason {
    fn describe(self) -> &'static str {
        match self {
            Self::RepeatedPointer => "full: repeated pointer",
            Self::NestedSegments => "full: nested segments",
            Self::DuplicatePath => "full: duplicate path",
        }
    }
}

/// What a diff read, for the debug line and tests.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DiffStats {
    pub(crate) base_pointers: usize,
    pub(crate) target_pointers: usize,
    /// Distinct tree chunks both snapshots name.
    pub(crate) shared: usize,
    /// Distinct tree chunks decoded, over all walks.
    pub(crate) decoded_chunks: usize,
    pub(crate) stored_bytes: u64,
    pub(crate) plaintext_bytes: u64,
    pub(crate) full: Option<FullDiffReason>,
}

pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_a: &str,
    snapshot_b: &str,
) -> Result<DiffResult> {
    let mut repo =
        super::util::open_repo(config, passphrase, OpenOptions::new(), &CancelToken::new())?;

    let entry_a = repo.manifest().resolve_snapshot(snapshot_a)?.clone();
    let entry_b = repo.manifest().resolve_snapshot(snapshot_b)?.clone();

    ensure_distinct_snapshots(&entry_a, &entry_b)?;

    let (base, target) = order_snapshots(entry_a, entry_b);
    let (entries, _stats) = diff_on_repo(&mut repo, &base, &target, &CancelToken::new())?;

    Ok(DiffResult {
        base_snapshot: base.name,
        target_snapshot: target.name,
        entries,
    })
}

/// Diff `base` against `target` (already ordered) on an open repository.
///
/// A tree chunk both snapshots name holds the same entries in both, so only
/// the chunks one of them names alone are decoded, and regular files are
/// matched by their raw path bytes. When a snapshot names a tree chunk
/// twice, has a key index with nested segment roots, or two decoded regular
/// files of one side share a path, both listings are decoded in full and
/// the last file with a path wins, as in a whole-stream diff. A duplicate
/// with one copy in a shared tree chunk is not detected.
pub(crate) fn diff_on_repo(
    repo: &mut Repo,
    base: &SnapshotEntry,
    target: &SnapshotEntry,
    cancel: &CancelToken,
) -> Result<(Vec<DiffEntry>, DiffStats)> {
    let base_meta = load_snapshot_meta(repo, &base.name)?;
    let target_meta = load_snapshot_meta(repo, &target.name)?;
    ItemStreamReader::new(repo).ensure_index(repo)?;
    // Every pointer resolves, also those of the shared chunks, which are not
    // read: a dangling pointer fails the diff as a whole-stream read does.
    resolve_from_index(repo.chunk_index(), &base_meta.item_ptrs)?;
    resolve_from_index(repo.chunk_index(), &target_meta.item_ptrs)?;

    let base_ptrs = &base_meta.item_ptrs;
    let target_ptrs = &target_meta.item_ptrs;
    let base_set = id_set(base_ptrs);
    let target_set = id_set(target_ptrs);
    let mut stats = DiffStats {
        base_pointers: base_ptrs.len(),
        target_pointers: target_ptrs.len(),
        shared: base_set
            .iter()
            .filter(|id| target_set.contains(*id))
            .count(),
        ..DiffStats::default()
    };
    stats.full = if base_set.len() < base_ptrs.len() || target_set.len() < target_ptrs.len() {
        Some(FullDiffReason::RepeatedPointer)
    } else if has_nested_segments(&base_meta) || has_nested_segments(&target_meta) {
        Some(FullDiffReason::NestedSegments)
    } else {
        None
    };

    let mut files = DecodedChunks::default();
    let mut entries = None;
    if stats.full.is_none() {
        let base_only: Vec<ChunkId> = base_ptrs
            .iter()
            .filter(|id| !target_set.contains(*id))
            .copied()
            .collect();
        let target_only: Vec<ChunkId> = target_ptrs
            .iter()
            .filter(|id| !base_set.contains(*id))
            .copied()
            .collect();
        decode_into(
            repo,
            &[&base_only, &target_only],
            &mut files,
            &mut stats,
            cancel,
        )?;
        let (base_map, base_dup) = side_map(&base_only, &files)?;
        let (target_map, target_dup) = side_map(&target_only, &files)?;
        if base_dup || target_dup {
            stats.full = Some(FullDiffReason::DuplicatePath);
        } else {
            entries = Some(diff_maps(&base_map, &target_map));
        }
    }
    let entries = match entries {
        Some(entries) => entries,
        None => {
            // The chunks decoded already are not decoded again.
            let missing: Vec<ChunkId> = base_ptrs
                .iter()
                .chain(target_ptrs)
                .filter(|id| !files.contains_key(*id))
                .copied()
                .collect();
            decode_into(repo, &[&missing], &mut files, &mut stats, cancel)?;
            let (base_map, _) = side_map(base_ptrs, &files)?;
            let (target_map, _) = side_map(target_ptrs, &files)?;
            diff_maps(&base_map, &target_map)
        }
    };

    tracing::debug!(
        "diff: base {} pointers, target {} pointers, {} shared; decoded {} tree chunks ({} B stored, {} B plaintext); {}",
        stats.base_pointers,
        stats.target_pointers,
        stats.shared,
        stats.decoded_chunks,
        stats.stored_bytes,
        stats.plaintext_bytes,
        stats.full.map_or("chunk-aware", FullDiffReason::describe)
    );
    Ok((entries, stats))
}

/// A set of tree chunk IDs.
type IdSet = HashSet<ChunkId, BuildChunkIdHasher>;

fn id_set(ptrs: &[ChunkId]) -> IdSet {
    ptrs.iter().copied().collect()
}

/// Whether `meta`'s key index has two nested segment roots (see
/// [`roots_nested`]). False without a usable index.
fn has_nested_segments(meta: &SnapshotMeta) -> bool {
    let view = ListingView::new(meta);
    let Ok(index) = view.index() else {
        return false;
    };
    let roots: Vec<&[u8]> = index.segments.iter().map(|s| s.root.as_slice()).collect();
    roots_nested(&roots)
}

/// Whether two of `roots` are equal or one lies under the other (an empty
/// root contains every path), except the pair of an empty root and
/// [`DUMPS_ROOT`]: root "" only comes from a single-path directory source,
/// and with command dumps active backup excludes `/vykar-dumps` from the
/// walk (an explicit exclude, tested before the gitignore stack), so that
/// segment holds no entry under `vykar-dumps/`. The snapshot does not
/// record the exclusion; this exemption relies on it.
fn roots_nested(roots: &[&[u8]]) -> bool {
    roots.iter().enumerate().any(|(i, a)| {
        roots.iter().skip(i + 1).any(|b| {
            let dumps_pair =
                (a.is_empty() && *b == DUMPS_ROOT) || (b.is_empty() && *a == DUMPS_ROOT);
            !dumps_pair && (rel_in(a, b).is_some() || rel_in(b, a).is_some())
        })
    })
}

/// Decode every chunk of `ids` once into `files`: the regular files of
/// each chunk, in chunk order. A chunk that does not decode on its own fails
/// the diff; read errors are returned as the read returned them.
fn decode_into(
    repo: &Repo,
    ids: &[&[ChunkId]],
    files: &mut DecodedChunks,
    stats: &mut DiffStats,
    cancel: &CancelToken,
) -> Result<()> {
    let set = TreeChunkSet::build(ids);
    let decode = |_k: usize, plain: &[u8]| -> std::result::Result<Vec<DecodedFile>, VykarError> {
        let mut out = Vec::new();
        for_each_decoded_item(plain, |item| {
            if cancel.is_cancelled() {
                return Err(VykarError::Interrupted);
            }
            if item.entry_type == ItemType::RegularFile {
                out.push(DecodedFile::from_item(item));
            }
            Ok(())
        })?;
        Ok(out)
    };
    let walk = walk_tree_chunks(
        repo,
        &set,
        TreeChunkSource::CacheFirst,
        OnChunkError::Abort,
        default_decode_threads(),
        cancel,
        decode,
        |k, outcome| {
            let id = *set.id(k);
            match outcome {
                ChunkOutcome::Decoded(Ok(decoded)) => {
                    files.insert(id, decoded);
                    Ok(())
                }
                ChunkOutcome::Decoded(Err(e @ VykarError::Interrupted)) => Err(e),
                ChunkOutcome::Decoded(Err(e)) => Err(VykarError::Other(format!(
                    "tree chunk {id} does not decode on its own: {e}"
                ))),
                // Abort mode hands `collect` decoded chunks only.
                _ => Err(VykarError::Other(format!(
                    "internal error: tree chunk {id} not decoded"
                ))),
            }
        },
    )?;
    stats.decoded_chunks += walk.chunks;
    stats.stored_bytes += walk.stored_bytes;
    stats.plaintext_bytes += walk.plaintext_bytes;
    Ok(())
}

/// The regular files of the chunks `ptrs`, in stream order, the last file
/// with a key winning; and whether a key occurred twice.
fn side_map<'a>(ptrs: &[ChunkId], files: &'a DecodedChunks) -> Result<(SideMap<'a>, bool)> {
    let mut map = SideMap::new();
    let mut dup = false;
    for id in ptrs {
        let decoded = files.get(id).ok_or_else(|| {
            VykarError::Other(format!("internal error: tree chunk {id} not decoded"))
        })?;
        for f in decoded {
            dup |= map.insert(f.key(), &f.sig).is_some();
        }
    }
    Ok((map, dup))
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

fn diff_maps(base: &SideMap<'_>, target: &SideMap<'_>) -> Vec<DiffEntry> {
    let mut entries = Vec::new();

    for (key, old) in base {
        match target.get(key) {
            None => entries.push(DiffEntry {
                path: old.path.clone(),
                change: DiffChangeKind::Removed,
                old_size: Some(old.size),
                new_size: None,
                size_delta: size_delta(old.size, 0),
            }),
            Some(new) if old.size != new.size || old.chunk_ids != new.chunk_ids => {
                entries.push(DiffEntry {
                    path: old.path.clone(),
                    change: DiffChangeKind::Modified,
                    old_size: Some(old.size),
                    new_size: Some(new.size),
                    size_delta: size_delta(old.size, new.size),
                });
            }
            Some(_) => {}
        }
    }

    for (key, new) in target {
        if !base.contains_key(key) {
            entries.push(DiffEntry {
                path: new.path.clone(),
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

    /// The regular files of `items`, as the diff decodes them.
    fn files_from(items: Vec<Item>) -> Vec<DecodedFile> {
        items
            .into_iter()
            .filter(|item| item.entry_type == ItemType::RegularFile)
            .map(DecodedFile::from_item)
            .collect()
    }

    /// A side map over `files`, the last file with a key winning.
    fn map_of(files: &[DecodedFile]) -> SideMap<'_> {
        files.iter().map(|f| (f.key(), &f.sig)).collect()
    }

    #[test]
    fn roots_nested_exempts_only_the_dumps_pair() {
        let nested = |roots: &[&str]| {
            let roots: Vec<&[u8]> = roots.iter().map(|r| r.as_bytes()).collect();
            roots_nested(&roots)
        };
        assert!(!nested(&["", "vykar-dumps"]));
        assert!(!nested(&["vykar-dumps", ""]));
        assert!(nested(&["", "a"]));
        assert!(nested(&["a", "a/b"]));
        assert!(nested(&["a", "a"]));
        assert!(nested(&["", "vykar-dumps", "vykar-dumps"]));
        assert!(nested(&["", "vykar-dumps", "x"]));
        assert!(nested(&["vykar-dumps", "vykar-dumps/x"]));
        assert!(nested(&["", "vykar-dumps/x"]));
        assert!(!nested(&["a", "b"]));
        assert!(!nested(&["a", "ab"]));
        assert!(!nested(&["vykar-dumps", "a", "b"]));
    }

    fn diff_items(base: Vec<Item>, target: Vec<Item>) -> Vec<DiffEntry> {
        let (base, target) = (files_from(base), files_from(target));
        diff_maps(&map_of(&base), &map_of(&target))
    }

    fn file(path: &str, size: u64, chunk_bytes: &[u8]) -> Item {
        Item {
            uid: 1000,
            gid: 1000,
            mtime: 1,
            size,
            chunks: chunk_bytes
                .iter()
                .map(|b| ChunkRef {
                    id: ChunkId::from_bytes([*b; 32]),
                    size: size as u32,
                    csize: size as u32,
                })
                .collect(),
            ..Item::test_file(path)
        }
    }

    fn non_file(path: &str, entry_type: ItemType) -> Item {
        Item {
            entry_type,
            mode: 0o755,
            uid: 1000,
            gid: 1000,
            mtime: 1,
            link_target: (entry_type == ItemType::Symlink).then(|| "target".to_string()),
            ..Item::test_file(path)
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

    /// Regression: streaming items through `for_each_decoded_item` into
    /// decoded files must produce the same input that `diff_maps` operates
    /// on as the eager conversion of a `Vec<Item>` does.
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

        let eager_base = files_from(base_items.clone());
        let eager_target = files_from(target_items.clone());

        let streamed_base = stream_to_files(&base_items);
        let streamed_target = stream_to_files(&target_items);

        assert_eq!(map_of(&streamed_base), map_of(&eager_base));
        assert_eq!(map_of(&streamed_target), map_of(&eager_target));

        let entries = diff_maps(&map_of(&streamed_base), &map_of(&streamed_target));
        assert_eq!(
            kinds(&entries),
            vec![
                ("added.txt", DiffChangeKind::Added),
                ("modified.txt", DiffChangeKind::Modified),
                ("removed.txt", DiffChangeKind::Removed),
            ]
        );
    }

    fn stream_to_files(items: &[Item]) -> Vec<DecodedFile> {
        let mut bytes = Vec::new();
        for item in items {
            bytes.extend_from_slice(&rmp_serde::to_vec(item).unwrap());
        }
        let mut files = Vec::new();
        for_each_decoded_item(&bytes, |item| {
            if item.entry_type == ItemType::RegularFile {
                files.push(DecodedFile::from_item(item));
            }
            Ok(())
        })
        .unwrap();
        files
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
