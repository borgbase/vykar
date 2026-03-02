use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use ignore::WalkBuilder;
use tracing::warn;

use crate::config::ChunkerConfig;
use crate::platform::fs;
use crate::repo::file_cache::FileCache;
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use vykar_types::error::{Result, VykarError};

use super::concurrency::ByteBudget;

/// Returns `true` for I/O errors safe to skip (permission denied, not found).
pub(super) fn is_soft_io_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
    )
}

/// Returns `true` for walk errors caused by soft I/O conditions.
pub(super) fn is_soft_walk_error(e: &ignore::Error) -> bool {
    e.io_error().is_some_and(is_soft_io_error)
}

/// Items chunker config — finer granularity for the item metadata stream.
pub(super) fn items_chunker_config() -> ChunkerConfig {
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

/// Configure a WalkBuilder with standard filters (excludes, one_file_system, markers).
pub(super) fn build_configured_walker(
    source: &Path,
    exclude_patterns: &[String],
    exclude_if_present: &[String],
    one_file_system: bool,
    git_ignore: bool,
) -> Result<WalkBuilder> {
    let source_dev = std::fs::symlink_metadata(source)
        .map(|m| fs::summarize_metadata(&m, &m.file_type()).device)
        .map_err(|e| {
            VykarError::Other(format!(
                "source directory does not exist: {}: {e}",
                source.display()
            ))
        })?;

    let explicit_excludes = build_explicit_excludes(source, exclude_patterns)?;

    let mut walk_builder = WalkBuilder::new(source);
    walk_builder.follow_links(false);
    walk_builder.hidden(false);
    walk_builder.ignore(false);
    walk_builder.git_global(false);
    walk_builder.git_exclude(false);
    walk_builder.parents(git_ignore);
    walk_builder.git_ignore(git_ignore);
    walk_builder.require_git(false);
    walk_builder.sort_by_file_name(std::ffi::OsStr::cmp);

    let markers = exclude_if_present.to_vec();
    let source_path_buf = source.to_path_buf();
    walk_builder.filter_entry(move |entry| {
        let path = entry.path();
        if path == source_path_buf {
            return true;
        }

        let rel = path.strip_prefix(&source_path_buf).unwrap_or(path);
        let is_dir = entry.file_type().is_some_and(|ft| ft.is_dir());

        if explicit_excludes
            .matched_path_or_any_parents(rel, is_dir)
            .is_ignore()
        {
            return false;
        }

        if one_file_system && is_dir {
            if let Ok(metadata) = std::fs::symlink_metadata(path) {
                let entry_dev = fs::summarize_metadata(&metadata, &metadata.file_type()).device;
                if should_skip_for_device(one_file_system, source_dev, entry_dev) {
                    return false;
                }
            }
        }

        if is_dir && !markers.is_empty() {
            for marker in &markers {
                if path.join(marker).exists() {
                    return false;
                }
            }
        }

        true
    });

    Ok(walk_builder)
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
        cached_refs: Vec<ChunkRef>,
    },
    NonFile {
        item: Item,
    },
    /// A file that was skipped due to a soft error (permission denied, not found).
    Skipped,
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
        WalkEntry::Skipped => Ok(0),
        _ => Ok(0),
    }
}

/// Build a walk iterator that yields `WalkEntry` items for all source paths.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_walk_iter<'a>(
    source_paths: &'a [String],
    multi_path: bool,
    exclude_patterns: &'a [String],
    exclude_if_present: &'a [String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    file_cache: &'a FileCache,
    segment_size: u64,
) -> Box<dyn Iterator<Item = Result<WalkEntry>> + Send + 'a> {
    let iter = source_paths.iter().flat_map(move |source_path| {
        let source_started = std::iter::once(Ok(WalkEntry::SourceStarted {
            path: source_path.clone(),
        }));

        let entries = walk_source(
            source_path,
            multi_path,
            exclude_patterns,
            exclude_if_present,
            one_file_system,
            git_ignore,
            xattrs_enabled,
            file_cache,
            segment_size,
        );

        let source_finished = std::iter::once(Ok(WalkEntry::SourceFinished {
            path: source_path.clone(),
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

/// Walk a single source path and yield WalkEntry items.
#[allow(clippy::too_many_arguments)]
fn walk_source<'a>(
    source_path: &'a str,
    multi_path: bool,
    exclude_patterns: &'a [String],
    exclude_if_present: &'a [String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    file_cache: &'a FileCache,
    segment_size: u64,
) -> Box<dyn Iterator<Item = Result<WalkEntry>> + Send + 'a> {
    let source = Path::new(source_path);
    let walk_builder = match build_configured_walker(
        source,
        exclude_patterns,
        exclude_if_present,
        one_file_system,
        git_ignore,
    ) {
        Ok(wb) => wb,
        Err(e) => return Box::new(std::iter::once(Err(e))),
    };

    // Multi-path prefix item.
    let prefix = if multi_path {
        let base = source
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| source_path.to_string());
        Some(base)
    } else {
        None
    };

    let prefix_item: Box<dyn Iterator<Item = Result<WalkEntry>> + Send> =
        if let Some(ref pfx) = prefix {
            let dir_item = Item {
                path: pfx.clone(),
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
            };
            Box::new(std::iter::once(Ok(WalkEntry::NonFile { item: dir_item })))
        } else {
            Box::new(std::iter::empty())
        };

    let source_owned = source.to_path_buf();
    let prefix_clone = prefix.clone();
    let walk_entries = walk_builder
        .build()
        .flat_map(move |entry_result| -> WalkItems {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    if is_soft_walk_error(&e) {
                        warn!(error = %e, "skipping entry (walk error)");
                        return WalkItems::One(Some(Ok(WalkEntry::Skipped)));
                    }
                    return WalkItems::One(Some(Err(VykarError::Other(format!("walk error: {e}")))));
                }
            };

            let rel_path = entry
                .path()
                .strip_prefix(&source_owned)
                .unwrap_or(entry.path())
                .to_string_lossy()
                .to_string();

            if rel_path.is_empty() {
                return WalkItems::Empty;
            }

            let metadata = match std::fs::symlink_metadata(entry.path()) {
                Ok(m) => m,
                Err(e) => {
                    if is_soft_io_error(&e) {
                        warn!(path = %entry.path().display(), error = %e, "skipping entry (stat error)");
                        return WalkItems::One(Some(Ok(WalkEntry::Skipped)));
                    }
                    return WalkItems::One(Some(Err(VykarError::Other(format!(
                        "stat error for {}: {e}",
                        entry.path().display()
                    )))));
                }
            };

            let file_type = metadata.file_type();
            let metadata_summary = fs::summarize_metadata(&metadata, &file_type);

            let (entry_type, link_target) = if file_type.is_dir() {
                (ItemType::Directory, None)
            } else if file_type.is_symlink() {
                match std::fs::read_link(entry.path()) {
                    Ok(target) => (
                        ItemType::Symlink,
                        Some(target.to_string_lossy().to_string()),
                    ),
                    Err(e) => {
                        if is_soft_io_error(&e) {
                            warn!(path = %entry.path().display(), error = %e, "skipping entry (readlink error)");
                            return WalkItems::One(Some(Ok(WalkEntry::Skipped)));
                        }
                        return WalkItems::One(Some(Err(VykarError::Other(format!(
                            "readlink: {e}"
                        )))));
                    }
                }
            } else if file_type.is_file() {
                (ItemType::RegularFile, None)
            } else {
                return WalkItems::Empty; // skip special files
            };

            let item_path = match &prefix_clone {
                Some(pfx) => format!("{pfx}/{rel_path}"),
                None => rel_path,
            };

            let mut item = Item {
                path: item_path,
                entry_type,
                mode: metadata_summary.mode,
                uid: metadata_summary.uid,
                gid: metadata_summary.gid,
                user: None,
                group: None,
                mtime: metadata_summary.mtime_ns,
                atime: None,
                ctime: None,
                size: metadata_summary.size,
                chunks: Vec::new(),
                link_target,
                xattrs: None,
            };

            if xattrs_enabled {
                item.xattrs = read_item_xattrs(entry.path());
            }

            if entry_type == ItemType::RegularFile && metadata_summary.size > 0 {
                let abs_path = entry.path().to_string_lossy().to_string();

                // Check file cache (read-only).
                let cache_hit = file_cache.lookup(
                    &abs_path,
                    metadata_summary.device,
                    metadata_summary.inode,
                    metadata_summary.mtime_ns,
                    metadata_summary.ctime_ns,
                    metadata_summary.size,
                );

                if let Some(cached_refs) = cache_hit {
                    return WalkItems::One(Some(Ok(WalkEntry::CacheHit {
                        item,
                        abs_path,
                        metadata: metadata_summary,
                        cached_refs: cached_refs.to_vec(),
                    })));
                }

                let file_size = metadata_summary.size;
                if file_size > segment_size {
                    // Split large file into segments for lazy parallel processing.
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
        });

    Box::new(prefix_item.chain(walk_entries))
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
}
