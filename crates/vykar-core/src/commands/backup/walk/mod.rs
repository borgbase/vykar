use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::warn;

use crate::config::ChunkerConfig;
use crate::platform::fs;
use crate::repo::file_cache::{CachedChunks, FileCache, ParentReuseIndex};
use crate::snapshot::item::{Item, ItemType};
use vykar_types::error::{Result, VykarError};

use super::concurrency::ByteBudget;
use super::source::ResolvedSource;

mod inode_walk;
pub(super) use inode_walk::{InodeSortedWalk, WalkEvent, WalkedEntry};

/// Items chunker config — finer granularity for the item metadata stream.
pub(crate) fn items_chunker_config() -> ChunkerConfig {
    ChunkerConfig {
        min_size: 32 * 1024,  // 32 KiB
        avg_size: 128 * 1024, // 128 KiB
        max_size: 512 * 1024, // 512 KiB
    }
}

pub(crate) fn build_explicit_excludes(
    source: &Path,
    patterns: &[String],
) -> Result<ignore::gitignore::Gitignore> {
    let mut builder = ignore::gitignore::GitignoreBuilder::new(source);
    for pat in patterns {
        builder
            .add_line(None, pat)
            .map_err(|e| VykarError::Config(format!("invalid exclude pattern '{pat}': {e}")))?;
    }
    builder
        .build()
        .map_err(|e| VykarError::Config(format!("exclude matcher build failed: {e}")))
}

pub(crate) fn should_skip_for_device(
    one_file_system: bool,
    source_dev: u64,
    entry_dev: u64,
) -> bool {
    one_file_system && source_dev != entry_dev
}

#[cfg(unix)]
pub(super) fn read_item_xattrs(path: &Path) -> Option<HashMap<String, Vec<u8>>> {
    let names = match xattr::list(path) {
        Ok(names) => names,
        Err(e) => {
            warn!(
                path = %path.display(),
                error = %e,
                "failed to list extended attributes"
            );
            return None;
        }
    };

    let mut attrs = HashMap::new();
    for name in names {
        let key = match name.to_str() {
            Some(name) => name.to_string(),
            None => {
                warn!(
                    path = %path.display(),
                    attr = ?name,
                    "skipping extended attribute with non-UTF8 name"
                );
                continue;
            }
        };

        match xattr::get(path, &name) {
            Ok(Some(value)) => {
                attrs.insert(key, value);
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    path = %path.display(),
                    attr = %key,
                    error = %e,
                    "failed to read extended attribute"
                );
            }
        }
    }

    if attrs.is_empty() {
        None
    } else {
        Some(attrs)
    }
}

#[cfg(not(unix))]
pub(super) fn read_item_xattrs(_path: &Path) -> Option<HashMap<String, Vec<u8>>> {
    None
}

// ---------------------------------------------------------------------------
// Entry materialization — shared between pipeline and sequential paths
// ---------------------------------------------------------------------------

/// Result of converting a walked filesystem entry into an `Item`.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // Entry dominates; SoftError/Unsupported are rare early-returns
pub(super) enum Materialized {
    /// Successfully built an Item with its metadata.
    Entry {
        item: Item,
        abs_path: PathBuf,
        metadata: fs::MetadataSummary,
    },
    /// Soft I/O error (e.g. permission denied on readlink, Windows
    /// unsupported reparse tag) — caller should count as error and surface
    /// `path` + `reason` in a path-bearing warning.
    SoftError { path: PathBuf, reason: String },
    /// Unsupported file type (block device, FIFO, etc.) — silent skip.
    Unsupported,
}

/// Classify a walked filesystem entry and build an `Item` from its metadata.
///
/// Handles file-type classification, symlink target resolution, ctime
/// computation, Item construction, and xattr population. The snapshot path is
/// taken from `walked.snapshot_path` as computed by the walker.
pub(super) fn materialize_item(walked: WalkedEntry, xattrs_enabled: bool) -> Result<Materialized> {
    let file_type = walked.file_type;
    let metadata_summary = walked.metadata;

    let (entry_type, link_target) = if file_type.is_dir() {
        (ItemType::Directory, None)
    } else if file_type.is_symlink() {
        match std::fs::read_link(&walked.abs_path) {
            Ok(target) => (
                ItemType::Symlink,
                Some(target.to_string_lossy().to_string()),
            ),
            Err(e) => {
                if vykar_types::error::is_soft_backup_io_error(&e) {
                    return Ok(Materialized::SoftError {
                        path: walked.abs_path.clone(),
                        reason: format!("readlink failed: {e}"),
                    });
                }
                return Err(VykarError::Other(format!(
                    "readlink failed for '{}': {e}",
                    walked.abs_path.display()
                )));
            }
        }
    } else if file_type.is_file() {
        (ItemType::RegularFile, None)
    } else {
        return Ok(Materialized::Unsupported);
    };

    let item_ctime = if entry_type == ItemType::RegularFile {
        Some(metadata_summary.ctime_ns)
    } else {
        None
    };

    let mut item = Item {
        path: walked.snapshot_path,
        entry_type,
        mode: metadata_summary.mode,
        uid: metadata_summary.uid,
        gid: metadata_summary.gid,
        user: None,
        group: None,
        mtime: metadata_summary.mtime_ns,
        atime: None,
        ctime: item_ctime,
        size: metadata_summary.size,
        chunks: Vec::new(),
        link_target,
        xattrs: None,
    };

    if xattrs_enabled {
        item.xattrs = read_item_xattrs(&walked.abs_path);
    }

    Ok(Materialized::Entry {
        item,
        abs_path: walked.abs_path,
        metadata: metadata_summary,
    })
}

// ---------------------------------------------------------------------------
// Walk entries and iterators for the parallel pipeline
// ---------------------------------------------------------------------------

/// Walk entry produced by the sequential walk phase.
pub(super) enum WalkEntry {
    File {
        item: Item,
        abs_path: String,
        metadata: fs::MetadataSummary,
        file_size: u64,
    },
    FileSegment {
        /// Only present for segment 0; `None` for continuations.
        item: Option<Item>,
        abs_path: Arc<str>,
        metadata: fs::MetadataSummary,
        segment_index: usize,
        num_segments: usize,
        offset: u64,
        len: u64,
    },
    CacheHit {
        item: Item,
        abs_path: String,
        metadata: fs::MetadataSummary,
        cached_refs: CachedChunks,
    },
    NonFile {
        item: Item,
    },
    /// A file that was skipped due to a soft error (permission denied, not
    /// found, EIO, Windows unsupported reparse, cloud-file). Carries the
    /// failing path (snapshot/abs string form) and a pre-formatted reason
    /// so consumers can surface a path-bearing warning.
    Skipped {
        path: String,
        reason: String,
    },
    /// macOS dataless (FileProvider placeholder) file with no matching entry
    /// in the local file cache or the parent reuse index. Skipped without
    /// opening so we never trigger asynchronous hydration. Counted into a
    /// per-source running total used to emit a single end-of-source summary
    /// warning.
    SkippedDataless {
        path: String,
    },
    SourceStarted {
        path: String,
    },
    SourceFinished {
        path: String,
    },
}

/// Acquire budget for a walk entry before dispatching to a worker.
///
/// Called by the dedicated walk thread to reserve memory in walk order.
pub(super) fn reserve_budget(entry: &WalkEntry, budget: &ByteBudget) -> Result<usize> {
    match entry {
        WalkEntry::File { file_size, .. } => {
            budget.acquire(usize::try_from(*file_size).unwrap_or(usize::MAX))
        }
        WalkEntry::FileSegment { len, .. } => budget.acquire(*len as usize),
        WalkEntry::Skipped { .. } | WalkEntry::SkippedDataless { .. } => Ok(0),
        _ => Ok(0),
    }
}

/// Build a walk iterator that yields `WalkEntry` items for all resolved sources.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_walk_iter<'a>(
    sources: &'a [ResolvedSource],
    exclude_patterns: &'a [String],
    exclude_if_present: &'a [String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    file_cache: &'a FileCache,
    segment_size: u64,
    parent_reuse_index: Option<&'a ParentReuseIndex>,
) -> Box<dyn Iterator<Item = Result<WalkEntry>> + Send + 'a> {
    let iter = sources.iter().flat_map(move |source| {
        let source_started = std::iter::once(Ok(WalkEntry::SourceStarted {
            path: source.configured.clone(),
        }));

        let entries = walk_source(
            source,
            exclude_patterns,
            exclude_if_present,
            one_file_system,
            git_ignore,
            xattrs_enabled,
            file_cache,
            segment_size,
            parent_reuse_index,
        );

        let source_finished = std::iter::once(Ok(WalkEntry::SourceFinished {
            path: source.configured.clone(),
        }));

        source_started.chain(entries).chain(source_finished)
    });

    Box::new(iter)
}

/// Lazy iterator over walk entries for a single filesystem entry.
///
/// Avoids heap allocation for the common zero/single-entry cases.
/// The `Segments` variant lazily yields `FileSegment` entries for large files.
enum WalkItems {
    /// No entries (e.g. root entry, special files).
    Empty,
    /// Exactly one entry (regular file, directory, symlink, error, cache hit).
    One(Option<Result<WalkEntry>>),
    /// Large file split into N segments, yielded lazily.
    Segments {
        /// Moved into segment 0; `None` for continuations.
        item: Option<Item>,
        abs_path: Arc<str>,
        metadata: fs::MetadataSummary,
        segment_size: u64,
        file_size: u64,
        num_segments: usize,
        next: usize,
    },
}

impl Iterator for WalkItems {
    type Item = Result<WalkEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            WalkItems::Empty => None,
            WalkItems::One(val) => val.take(),
            WalkItems::Segments {
                item,
                abs_path,
                metadata,
                segment_size,
                file_size,
                num_segments,
                next,
            } => {
                let i = *next;
                if i >= *num_segments {
                    return None;
                }
                *next = i + 1;
                let offset = i as u64 * *segment_size;
                let len = (*segment_size).min(*file_size - offset);
                // Segment 0 moves the item; continuations pass None.
                let seg_item = if i == 0 { item.take() } else { None };
                Some(Ok(WalkEntry::FileSegment {
                    item: seg_item,
                    abs_path: abs_path.clone(),
                    metadata: *metadata,
                    segment_index: i,
                    num_segments: *num_segments,
                    offset,
                    len,
                }))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            WalkItems::Empty => (0, Some(0)),
            WalkItems::One(val) => {
                let n = usize::from(val.is_some());
                (n, Some(n))
            }
            WalkItems::Segments {
                num_segments, next, ..
            } => {
                let remaining = num_segments.saturating_sub(*next);
                (remaining, Some(remaining))
            }
        }
    }
}

/// Walk a single resolved source and yield WalkEntry items.
#[allow(clippy::too_many_arguments)]
fn walk_source<'a>(
    source: &'a ResolvedSource,
    exclude_patterns: &'a [String],
    exclude_if_present: &'a [String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    file_cache: &'a FileCache,
    segment_size: u64,
    parent_reuse_index: Option<&'a ParentReuseIndex>,
) -> Box<dyn Iterator<Item = Result<WalkEntry>> + Send + 'a> {
    walk_source_inode_sorted(
        source,
        exclude_patterns,
        exclude_if_present,
        one_file_system,
        git_ignore,
        xattrs_enabled,
        file_cache,
        segment_size,
        parent_reuse_index,
    )
}

/// Walk a single resolved source using the inode-sorted walker.
#[allow(clippy::too_many_arguments)]
fn walk_source_inode_sorted<'a>(
    source: &'a ResolvedSource,
    exclude_patterns: &'a [String],
    exclude_if_present: &'a [String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    file_cache: &'a FileCache,
    segment_size: u64,
    parent_reuse_index: Option<&'a ParentReuseIndex>,
) -> Box<dyn Iterator<Item = Result<WalkEntry>> + Send + 'a> {
    let inode_walk = match InodeSortedWalk::new(
        source,
        exclude_patterns,
        exclude_if_present,
        one_file_system,
        git_ignore,
    ) {
        Ok(w) => w,
        Err(e) => return Box::new(std::iter::once(Err(e))),
    };

    let walk_entries = inode_walk.flat_map(move |event_result| -> WalkItems {
        let event = match event_result {
            Ok(e) => e,
            Err(e) => {
                return WalkItems::One(Some(Err(e)));
            }
        };

        match event {
            WalkEvent::Skipped { path, reason } => WalkItems::One(Some(Ok(WalkEntry::Skipped {
                path: path.to_string_lossy().into_owned(),
                reason,
            }))),
            WalkEvent::Entry(walked) => walked_entry_to_walk_items(
                walked,
                xattrs_enabled,
                file_cache,
                segment_size,
                parent_reuse_index,
            ),
        }
    });

    Box::new(walk_entries)
}

/// Convert a `WalkedEntry` (pre-statted) into pipeline `WalkItems`.
fn walked_entry_to_walk_items(
    walked: WalkedEntry,
    xattrs_enabled: bool,
    file_cache: &FileCache,
    segment_size: u64,
    parent_reuse_index: Option<&ParentReuseIndex>,
) -> WalkItems {
    let (item, abs_path, metadata_summary) = match materialize_item(walked, xattrs_enabled) {
        Ok(Materialized::Entry {
            item,
            abs_path,
            metadata,
        }) => (item, abs_path, metadata),
        Ok(Materialized::SoftError { path, reason }) => {
            return WalkItems::One(Some(Ok(WalkEntry::Skipped {
                path: path.to_string_lossy().into_owned(),
                reason,
            })));
        }
        Ok(Materialized::Unsupported) => return WalkItems::Empty,
        Err(e) => return WalkItems::One(Some(Err(e))),
    };

    if item.entry_type == ItemType::RegularFile && metadata_summary.size > 0 {
        let abs_path = abs_path
            .into_os_string()
            .into_string()
            .unwrap_or_else(|os| os.to_string_lossy().into_owned());

        match super::resolve_cache_hit(file_cache, parent_reuse_index, &abs_path, &metadata_summary)
        {
            super::CacheResolution::Hit(cached_refs) => {
                return WalkItems::One(Some(Ok(WalkEntry::CacheHit {
                    item,
                    abs_path,
                    metadata: metadata_summary,
                    cached_refs,
                })));
            }
            super::CacheResolution::SkipDataless => {
                tracing::debug!(
                    path = %abs_path,
                    "skipping dataless cloud-only file (no cache or parent reuse)"
                );
                return WalkItems::One(Some(Ok(WalkEntry::SkippedDataless { path: abs_path })));
            }
            super::CacheResolution::Miss => {}
        }

        let file_size = metadata_summary.size;
        if file_size > segment_size {
            let num_segments = file_size.div_ceil(segment_size) as usize;
            let abs_path: Arc<str> = abs_path.into();
            WalkItems::Segments {
                item: Some(item),
                abs_path,
                metadata: metadata_summary,
                segment_size,
                file_size,
                num_segments,
                next: 0,
            }
        } else {
            WalkItems::One(Some(Ok(WalkEntry::File {
                file_size,
                item,
                abs_path,
                metadata: metadata_summary,
            })))
        }
    } else {
        WalkItems::One(Some(Ok(WalkEntry::NonFile { item })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_item(path: &str) -> Item {
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

    fn test_metadata() -> fs::MetadataSummary {
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

    #[test]
    fn one_file_system_device_filter_logic() {
        assert!(should_skip_for_device(true, 42, 43));
        assert!(!should_skip_for_device(true, 42, 42));
        assert!(!should_skip_for_device(false, 42, 43));
    }

    #[test]
    fn walk_items_empty() {
        let mut it = WalkItems::Empty;
        assert_eq!(it.size_hint(), (0, Some(0)));
        assert!(it.next().is_none());
    }

    #[test]
    fn walk_items_one() {
        let entry = Ok(WalkEntry::NonFile {
            item: test_item("x"),
        });
        let mut it = WalkItems::One(Some(entry));
        assert_eq!(it.size_hint(), (1, Some(1)));
        assert!(it.next().is_some());
        assert_eq!(it.size_hint(), (0, Some(0)));
        assert!(it.next().is_none());
    }

    #[test]
    fn walk_items_segments_lazy() {
        let meta = test_metadata();
        let mut it = WalkItems::Segments {
            item: Some(test_item("big")),
            abs_path: "/tmp/big".into(),
            metadata: meta,
            segment_size: 100,
            file_size: 250,
            num_segments: 3,
            next: 0,
        };
        assert_eq!(it.size_hint(), (3, Some(3)));

        // Segment 0 should carry Some(item).
        let seg0 = it.next().unwrap().unwrap();
        if let WalkEntry::FileSegment {
            item,
            segment_index,
            offset,
            len,
            ..
        } = seg0
        {
            assert!(item.is_some(), "segment 0 must carry item");
            assert_eq!(segment_index, 0);
            assert_eq!(offset, 0);
            assert_eq!(len, 100);
        } else {
            panic!("expected FileSegment");
        }

        // Segment 1 should carry None item.
        let seg1 = it.next().unwrap().unwrap();
        if let WalkEntry::FileSegment {
            item,
            segment_index,
            offset,
            len,
            ..
        } = seg1
        {
            assert!(item.is_none(), "continuation must carry None item");
            assert_eq!(segment_index, 1);
            assert_eq!(offset, 100);
            assert_eq!(len, 100);
        } else {
            panic!("expected FileSegment");
        }

        // Segment 2 (last): len should be remainder.
        let seg2 = it.next().unwrap().unwrap();
        if let WalkEntry::FileSegment {
            segment_index,
            offset,
            len,
            ..
        } = seg2
        {
            assert_eq!(segment_index, 2);
            assert_eq!(offset, 200);
            assert_eq!(len, 50);
        } else {
            panic!("expected FileSegment");
        }

        assert_eq!(it.size_hint(), (0, Some(0)));
        assert!(it.next().is_none());
    }

    #[test]
    fn normalize_rel_path_replaces_backslashes() {
        // Simulates what Windows `to_string_lossy()` would produce.
        let win_path = r"folder\sub\file.txt".to_string();
        let normalized = super::super::normalize_rel_path(win_path);
        if cfg!(windows) {
            assert_eq!(normalized, "folder/sub/file.txt");
        } else {
            // On Unix the function is a no-op — backslash is a valid filename char.
            assert_eq!(normalized, r"folder\sub\file.txt");
        }
    }

    #[test]
    fn normalize_rel_path_no_op_for_forward_slashes() {
        let unix_path = "folder/sub/file.txt".to_string();
        let normalized = super::super::normalize_rel_path(unix_path);
        assert_eq!(normalized, "folder/sub/file.txt");
    }

    #[test]
    fn pathbuf_into_string_matches_to_string_lossy() {
        let paths = ["/tmp/file.txt", "/home/user/Documents/photo.jpg", "/a/b/c"];
        for p in paths {
            let pb = std::path::PathBuf::from(p);
            let expected = pb.to_string_lossy().to_string();
            let actual = pb
                .into_os_string()
                .into_string()
                .unwrap_or_else(|os| os.to_string_lossy().into_owned());
            assert_eq!(actual, expected, "mismatch for path {p}");
        }
    }

    // -----------------------------------------------------------------------
    // materialize_item tests
    // -----------------------------------------------------------------------

    fn walked_from_path(path: &std::path::Path, snapshot_path: &str) -> WalkedEntry {
        let meta = std::fs::symlink_metadata(path).unwrap();
        WalkedEntry {
            abs_path: path.to_path_buf(),
            metadata: fs::summarize_metadata(&meta, &meta.file_type()),
            file_type: meta.file_type(),
            snapshot_path: snapshot_path.to_string(),
        }
    }

    #[test]
    fn materialize_regular_file() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("hello.txt");
        std::fs::write(&file, b"content").unwrap();
        let walked = walked_from_path(&file, "hello.txt");
        let result = materialize_item(walked, false).unwrap();
        match result {
            Materialized::Entry { item, metadata, .. } => {
                assert_eq!(item.entry_type, ItemType::RegularFile);
                assert_eq!(item.path, "hello.txt");
                assert_eq!(item.ctime, Some(metadata.ctime_ns));
                assert!(item.xattrs.is_none());
            }
            other => panic!("expected Entry, got {other:?}"),
        }
    }

    #[test]
    fn materialize_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("sub");
        std::fs::create_dir(&dir).unwrap();
        let walked = walked_from_path(&dir, "sub");
        let result = materialize_item(walked, false).unwrap();
        match result {
            Materialized::Entry { item, .. } => {
                assert_eq!(item.entry_type, ItemType::Directory);
                assert!(item.ctime.is_none());
            }
            other => panic!("expected Entry, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn materialize_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("target.txt");
        std::fs::write(&target, b"x").unwrap();
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let walked = walked_from_path(&link, "link");
        let result = materialize_item(walked, false).unwrap();
        match result {
            Materialized::Entry { item, .. } => {
                assert_eq!(item.entry_type, ItemType::Symlink);
                assert!(item.link_target.is_some());
                assert!(item.ctime.is_none());
            }
            other => panic!("expected Entry, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn materialize_unix_socket_unsupported() {
        let tmp = tempfile::tempdir().unwrap();
        let sock_path = tmp.path().join("test.sock");
        let listener = match std::os::unix::net::UnixListener::bind(&sock_path) {
            Ok(l) => l,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => return, // skip in restricted envs
            Err(e) => panic!("unexpected bind error: {e}"),
        };
        let _listener = listener;
        let walked = walked_from_path(&sock_path, "test.sock");
        let result = materialize_item(walked, false).unwrap();
        assert!(matches!(result, Materialized::Unsupported));
    }

    #[test]
    fn materialize_with_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("data.bin");
        std::fs::write(&file, b"x").unwrap();
        let walked = walked_from_path(&file, "myhost/data.bin");
        let result = materialize_item(walked, false).unwrap();
        match result {
            Materialized::Entry { item, .. } => {
                assert_eq!(item.path, "myhost/data.bin");
            }
            other => panic!("expected Entry, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn materialize_soft_error_on_removed_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("target.txt");
        std::fs::write(&target, b"x").unwrap();
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        // Stat the symlink to get its FileType *before* removing it.
        let walked = walked_from_path(&link, "link");

        // Remove the symlink so read_link will fail with NotFound.
        std::fs::remove_file(&link).unwrap();

        let result = materialize_item(walked, false).unwrap();
        match result {
            Materialized::SoftError { path, reason } => {
                assert_eq!(path, link);
                assert!(
                    reason.contains("readlink failed"),
                    "reason should mention readlink: {reason}"
                );
            }
            other => panic!("expected SoftError, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn walked_entry_to_walk_items_soft_error_yields_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("target.txt");
        std::fs::write(&target, b"x").unwrap();
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let walked = walked_from_path(&link, "link");
        std::fs::remove_file(&link).unwrap();

        let file_cache = FileCache::default();
        let mut items = walked_entry_to_walk_items(walked, false, &file_cache, u64::MAX, None);
        match items.next() {
            Some(Ok(WalkEntry::Skipped { path, reason })) => {
                assert_eq!(path, link.to_string_lossy());
                assert!(
                    reason.contains("readlink failed"),
                    "reason should mention readlink: {reason}"
                );
            }
            _ => panic!("expected WalkEntry::Skipped"),
        }
        assert!(items.next().is_none());
    }

    // -----------------------------------------------------------------------

    #[test]
    #[cfg(unix)]
    fn pathbuf_into_string_fallback_for_non_utf8() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        // 0x80 is not valid UTF-8
        let os = OsStr::from_bytes(b"/tmp/\x80bad");
        let pb = std::path::PathBuf::from(os);
        let expected = pb.to_string_lossy().to_string();
        let actual = pb
            .into_os_string()
            .into_string()
            .unwrap_or_else(|os| os.to_string_lossy().into_owned());
        assert_eq!(actual, expected);
        assert!(
            actual.contains('\u{FFFD}'),
            "should contain replacement char"
        );
    }
}
