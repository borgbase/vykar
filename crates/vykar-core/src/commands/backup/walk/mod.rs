use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::warn;

use crate::config::ChunkerConfig;
use crate::platform::fs;
use crate::repo::file_cache::{CachedChunks, FileCache, ParentReuseIndex};
use crate::snapshot::item::{Item, ItemRawNames, ItemType};
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
    /// Soft I/O error from the read_link step (e.g. permission denied on
    /// readlink, Windows unsupported reparse tag) — caller should count as
    /// error and surface `path` + `reason` in a path-bearing warning.
    SoftError { path: PathBuf, reason: String },
    /// Unsupported file type (socket, FIFO, block/character device) — vykar's
    /// data model can't represent it. Carries a human-readable `file_type` label
    /// so the consumer can emit a path-bearing warn-only skip (not counted as an
    /// error). `&'static str` keeps the variant small.
    Unsupported {
        path: PathBuf,
        file_type: &'static str,
    },
}

/// Assemble an `ItemRawNames` from the optional path and link-target byte
/// shadows, returning `None` when neither is present (the common case).
fn build_raw_names(
    path_raw: Option<Vec<u8>>,
    link_target_raw: Option<Vec<u8>>,
) -> Option<ItemRawNames> {
    if path_raw.is_none() && link_target_raw.is_none() {
        return None;
    }
    Some(ItemRawNames {
        path: path_raw,
        link_target: link_target_raw,
    })
}

/// Map a non-directory/file/symlink file type to a human-readable label for
/// the warn-only skip message. On unix, distinguishes socket / FIFO / block
/// device / character device via `FileTypeExt`; everything else (and all
/// non-unix entries, where this branch is effectively unreachable) falls back
/// to a generic label.
#[cfg(unix)]
fn classify_unsupported(file_type: std::fs::FileType) -> &'static str {
    use std::os::unix::fs::FileTypeExt;
    if file_type.is_socket() {
        "socket"
    } else if file_type.is_fifo() {
        "FIFO"
    } else if file_type.is_block_device() {
        "block device"
    } else if file_type.is_char_device() {
        "character device"
    } else {
        "unsupported file type"
    }
}

#[cfg(not(unix))]
fn classify_unsupported(_file_type: std::fs::FileType) -> &'static str {
    "unsupported file type"
}

/// Classify a walked filesystem entry and build an `Item` from its metadata.
///
/// Handles file-type classification, symlink target resolution, ctime
/// computation, Item construction, and xattr population. The snapshot path is
/// taken from `walked.snapshot_path` as computed by the walker.
pub(super) fn materialize_item(walked: WalkedEntry, xattrs_enabled: bool) -> Result<Materialized> {
    let file_type = walked.file_type;
    let metadata_summary = walked.metadata;

    let (entry_type, link_target, link_target_raw) = if file_type.is_dir() {
        (ItemType::Directory, None, None)
    } else if file_type.is_symlink() {
        match std::fs::read_link(&walked.abs_path) {
            Ok(target) => {
                let raw = fs::non_utf8_bytes(&target);
                (
                    ItemType::Symlink,
                    Some(target.to_string_lossy().to_string()),
                    raw,
                )
            }
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
        (ItemType::RegularFile, None, None)
    } else {
        return Ok(Materialized::Unsupported {
            path: walked.abs_path,
            file_type: classify_unsupported(file_type),
        });
    };

    let item_ctime = if entry_type == ItemType::RegularFile {
        Some(metadata_summary.ctime_ns)
    } else {
        None
    };

    let raw_names = build_raw_names(walked.snapshot_path_raw, link_target_raw);

    // Hard-link group key: regular files with more than one link to their inode
    // carry their source `(dev, ino)` so restore can relink siblings. Each node
    // still records its full chunk list, so a lone surviving member of a
    // filtered group self-materializes — we never assume we see all N links.
    // `nlink` is `1` on Windows/non-unix, so this is naturally Unix-only.
    let hardlink = if entry_type == ItemType::RegularFile && metadata_summary.nlink > 1 {
        Some(crate::snapshot::item::HardlinkId {
            dev: metadata_summary.device,
            ino: metadata_summary.inode,
        })
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
        raw_names,
        hardlink,
    };

    // Skip xattrs on dataless inodes: on macOS, `getxattr` for FileProvider-
    // managed attrs round-trips through `fileproviderd` and serialises this
    // single-threaded walker. See issue #133 for the diagnosis.
    //
    // Trade-off: on the cache-hit path, `lookup_dataless` returns only chunk
    // refs (not xattrs), so dataless cache hits now record `xattrs: None` in
    // the new snapshot. Restoring a dataless file therefore loses any
    // user-set xattrs (Finder tags etc.) it had on the source. This is
    // deliberate — preserving them would require either re-reading from disk
    // (defeats the fix) or threading them through ParentReuseIndex (out of
    // scope; see #133 fix #2).
    if xattrs_enabled && !metadata_summary.is_dataless {
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
    /// found, EIO, Windows unsupported reparse, cloud-file, locked file).
    /// Carries the
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
    /// An entry whose file type vykar can't represent (socket, FIFO,
    /// block/character device). Skipped without opening, with a per-entry
    /// warning emitted by the consumer. Unlike `Skipped`, this is **not**
    /// counted as an error and does not mark the backup partial — it mirrors
    /// the warn-only `SkippedDataless` channel but warns per entry rather than
    /// as an end-of-source summary.
    SkippedUnsupported {
        path: String,
        file_type: &'static str,
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
        WalkEntry::Skipped { .. }
        | WalkEntry::SkippedDataless { .. }
        | WalkEntry::SkippedUnsupported { .. } => Ok(0),
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
/// Avoids heap allocation for the common single-entry case. Every walked entry
/// yields at least one `WalkEntry` — even skips (soft errors, dataless,
/// unsupported special files) flow through as a `One(..)` so the consumer can
/// surface them.
/// The `Segments` variant lazily yields `FileSegment` entries for large files.
enum WalkItems {
    /// Exactly one entry (regular file, directory, symlink, error, cache hit,
    /// or a skip variant).
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
        Ok(Materialized::Unsupported { path, file_type }) => {
            return WalkItems::One(Some(Ok(WalkEntry::SkippedUnsupported {
                path: path.to_string_lossy().into_owned(),
                file_type,
            })));
        }
        Err(e) => return WalkItems::One(Some(Err(e))),
    };

    if item.entry_type == ItemType::RegularFile && metadata_summary.size > 0 {
        let abs_path = abs_path
            .into_os_string()
            .into_string()
            .unwrap_or_else(|os| os.to_string_lossy().into_owned());

        match super::resolve_cache_hit(
            file_cache,
            parent_reuse_index,
            &abs_path,
            &metadata_summary,
            item.has_raw_path(),
        ) {
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
            raw_names: None,
            hardlink: None,
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
            nlink: 1,
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

    /// Regression: dataless inodes must NOT trigger `read_item_xattrs`.
    /// On macOS, `getxattr` on FileProvider-managed attrs round-trips through
    /// `fileproviderd` and can stall the (single-threaded) walker for seconds
    /// per file. Confirms the dataless guard added to `materialize_item` —
    /// when `is_dataless: true`, the Item must come back with `xattrs: None`
    /// even when the underlying file has xattrs on disk and `xattrs_enabled`
    /// is set. The companion `is_dataless: false` case proves the read path
    /// still fires when not gated.
    #[cfg(unix)]
    #[test]
    fn materialize_item_skips_xattrs_on_dataless() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("placeholder");
        std::fs::write(&path, b"x").unwrap();
        // Best-effort xattr write — bail the test if the tempdir's filesystem
        // doesn't support xattrs (e.g. tmpfs on some Linux configs); the
        // cross-platform xattrs_supported() flag does not detect that case.
        if xattr::set(&path, "user.vykar_test", b"v").is_err() {
            return;
        }

        let real_meta = std::fs::symlink_metadata(&path).unwrap();
        let file_type = real_meta.file_type();
        let mut summary = fs::summarize_metadata(&real_meta, &file_type);
        summary.is_dataless = true;

        let walked = inode_walk::WalkedEntry {
            abs_path: path.clone(),
            metadata: summary,
            file_type,
            snapshot_path: "placeholder".into(),
            snapshot_path_raw: None,
        };

        match materialize_item(walked, true).unwrap() {
            Materialized::Entry { item, .. } => {
                assert!(
                    item.xattrs.is_none(),
                    "dataless inodes must not be xattr-read",
                );
            }
            other => panic!("expected Entry, got {other:?}"),
        }

        // Sanity: same file with `is_dataless: false` must populate xattrs.
        let mut summary_warm = fs::summarize_metadata(&real_meta, &file_type);
        summary_warm.is_dataless = false;
        let walked_warm = inode_walk::WalkedEntry {
            abs_path: path,
            metadata: summary_warm,
            file_type,
            snapshot_path: "placeholder".into(),
            snapshot_path_raw: None,
        };
        match materialize_item(walked_warm, true).unwrap() {
            Materialized::Entry { item, .. } => {
                assert!(
                    item.xattrs.is_some(),
                    "warm files with xattrs_enabled must populate xattrs",
                );
            }
            other => panic!("expected Entry, got {other:?}"),
        }
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
            snapshot_path_raw: None,
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

    /// A regular file with `nlink > 1` materializes with `hardlink: Some`
    /// carrying its `(dev, ino)`; a single-link file gets `None`.
    #[cfg(unix)]
    #[test]
    fn materialize_regular_file_captures_hardlink() {
        use crate::snapshot::item::HardlinkId;
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a.txt");
        std::fs::write(&a, b"shared").unwrap();
        let b = tmp.path().join("b.txt");
        std::fs::hard_link(&a, &b).unwrap();

        // `a` now has nlink == 2 → hardlink Some.
        let walked = walked_from_path(&a, "a.txt");
        match materialize_item(walked, false).unwrap() {
            Materialized::Entry { item, metadata, .. } => {
                assert_eq!(
                    item.hardlink,
                    Some(HardlinkId {
                        dev: metadata.device,
                        ino: metadata.inode
                    })
                );
                assert!(metadata.nlink >= 2);
            }
            other => panic!("expected Entry, got {other:?}"),
        }

        // A lone file → None.
        let solo = tmp.path().join("solo.txt");
        std::fs::write(&solo, b"x").unwrap();
        let walked = walked_from_path(&solo, "solo.txt");
        match materialize_item(walked, false).unwrap() {
            Materialized::Entry { item, .. } => assert_eq!(item.hardlink, None),
            other => panic!("expected Entry, got {other:?}"),
        }
    }

    /// A hardlinked file that hits the local file cache still emits
    /// `hardlink: Some(..)` — the Item is fully materialized (including the
    /// hardlink key) before the cache check, and the `CacheHit` path carries it
    /// through untouched.
    #[cfg(unix)]
    #[test]
    fn cache_hit_hardlinked_file_keeps_hardlink() {
        use crate::repo::file_cache::CachedChunks;
        use crate::snapshot::item::ChunkRef;
        use vykar_types::chunk_id::ChunkId;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let a = root.join("a.txt");
        std::fs::write(&a, b"shared-content").unwrap();
        let b = root.join("b.txt");
        std::fs::hard_link(&a, &b).unwrap();

        let meta = std::fs::symlink_metadata(&a).unwrap();
        let summary = fs::summarize_metadata(&meta, &meta.file_type());
        assert!(summary.nlink >= 2, "fixture must be hard-linked");

        let abs_path = a.to_string_lossy().into_owned();
        let cached = CachedChunks::from_chunk_refs(&[ChunkRef {
            id: ChunkId::from_bytes([0xAB; 32]),
            size: summary.size as u32,
            csize: summary.size as u32,
        }]);

        let mut file_cache = FileCache::new();
        let root_str = root.to_string_lossy().into_owned();
        file_cache.begin_sections(&[root_str], &[1]);
        file_cache.insert(
            &abs_path,
            summary.device,
            summary.inode,
            summary.mtime_ns,
            summary.ctime_ns,
            summary.size,
            cached,
        );

        let walked = walked_from_path(&a, "a.txt");
        let mut items = walked_entry_to_walk_items(walked, false, &file_cache, u64::MAX, None);
        match items.next() {
            Some(Ok(WalkEntry::CacheHit { item, .. })) => {
                assert!(
                    item.hardlink.is_some(),
                    "cache-hit hardlinked file must keep hardlink: Some"
                );
            }
            _ => panic!("expected WalkEntry::CacheHit"),
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
            // Skip: restricted sandbox (EPERM) or a tempdir path too long for
            // the ~104-byte sun_path limit on macOS (EINVAL). The socket is
            // just a fixture, so an un-creatable one means "can't test here".
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::InvalidInput
                ) =>
            {
                return
            }
            Err(e) => panic!("unexpected bind error: {e}"),
        };
        let _listener = listener;
        let walked = walked_from_path(&sock_path, "test.sock");
        let result = materialize_item(walked, false).unwrap();
        match result {
            Materialized::Unsupported { path, file_type } => {
                assert_eq!(file_type, "socket");
                assert_eq!(path, sock_path);
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn materialize_fifo_unsupported() {
        use nix::sys::stat::Mode;
        use nix::unistd::mkfifo;

        let tmp = tempfile::tempdir().unwrap();
        let fifo_path = tmp.path().join("test.fifo");
        if mkfifo(&fifo_path, Mode::S_IRUSR | Mode::S_IWUSR).is_err() {
            // mkfifo can fail in restricted sandboxes (e.g. EPERM) — skip there.
            return;
        }
        let walked = walked_from_path(&fifo_path, "test.fifo");
        let result = materialize_item(walked, false).unwrap();
        match result {
            Materialized::Unsupported { path, file_type } => {
                assert_eq!(file_type, "FIFO");
                assert_eq!(path, fifo_path);
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    /// A walked entry carrying `snapshot_path_raw` produces an Item whose
    /// `raw_names.path` round-trips the raw bytes, while `path` stays lossy.
    #[cfg(unix)]
    #[test]
    fn materialize_regular_file_captures_raw_path() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let tmp = tempfile::tempdir().unwrap();
        // Create a file with a non-UTF8 name (0x80 is a stray continuation byte).
        let raw_name = b"bad-\x80.bin";
        let path = tmp.path().join(OsStr::from_bytes(raw_name));
        // Some filesystems (APFS/HFS+ on macOS) reject non-UTF8 names — skip
        // there; this exercises the Linux path (ext4/xfs) and CI.
        if std::fs::write(&path, b"x").is_err() {
            return;
        }
        let display = path.file_name().unwrap().to_string_lossy().into_owned();

        let meta = std::fs::symlink_metadata(&path).unwrap();
        let walked = WalkedEntry {
            abs_path: path.clone(),
            metadata: fs::summarize_metadata(&meta, &meta.file_type()),
            file_type: meta.file_type(),
            snapshot_path: display.clone(),
            snapshot_path_raw: Some(raw_name.to_vec()),
        };
        match materialize_item(walked, false).unwrap() {
            Materialized::Entry { item, .. } => {
                assert_eq!(item.path, display);
                assert_eq!(item.path_bytes(), raw_name);
                assert!(item.has_raw_path());
                // Note: no `item.validate()` here — a freshly materialized
                // regular file carries no chunks yet (chunking is a later
                // pipeline stage), so the size-vs-chunk-sum check cannot pass.
                // Raw-path validation is covered by item.rs's
                // `validate_rejects_raw_path_lossy_mismatch`.
            }
            other => panic!("expected Entry, got {other:?}"),
        }
    }

    /// A symlink whose target is non-UTF8 captures `raw_names.link_target`.
    #[cfg(unix)]
    #[test]
    fn materialize_symlink_captures_raw_target() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let tmp = tempfile::tempdir().unwrap();
        let raw_target = b"target-\x80";
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(OsStr::from_bytes(raw_target), &link).unwrap();

        let meta = std::fs::symlink_metadata(&link).unwrap();
        let walked = WalkedEntry {
            abs_path: link,
            metadata: fs::summarize_metadata(&meta, &meta.file_type()),
            file_type: meta.file_type(),
            snapshot_path: "link".into(),
            snapshot_path_raw: None,
        };
        match materialize_item(walked, false).unwrap() {
            Materialized::Entry { item, .. } => {
                assert_eq!(item.entry_type, ItemType::Symlink);
                assert_eq!(item.link_target_bytes(), Some(&raw_target[..]));
                assert!(!item.has_raw_path(), "path is UTF-8, only target is raw");
                item.validate().unwrap();
            }
            other => panic!("expected Entry, got {other:?}"),
        }
    }

    /// Two distinct non-UTF8 names produce two distinct raw paths (no lossy
    /// collapse), verified end-to-end through the walker.
    #[cfg(unix)]
    #[test]
    fn walker_distinct_non_utf8_names_stay_distinct() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let a = b"\x80a";
        let b = b"\x80b";
        // Skip on filesystems that reject non-UTF8 names (e.g. macOS APFS).
        if std::fs::write(root.join(OsStr::from_bytes(a)), "1").is_err() {
            return;
        }
        std::fs::write(root.join(OsStr::from_bytes(b)), "2").unwrap();

        let source = ResolvedSource::resolve(&root.to_string_lossy(), false).unwrap();
        let walk = InodeSortedWalk::new(&source, &[], &[], false, false).unwrap();
        let mut raws: Vec<Vec<u8>> = Vec::new();
        for ev in walk {
            if let Ok(WalkEvent::Entry(walked)) = ev {
                if let Materialized::Entry { item, .. } = materialize_item(walked, false).unwrap() {
                    raws.push(item.path_bytes().to_vec());
                }
            }
        }
        raws.sort();
        assert_eq!(raws, vec![a.to_vec(), b.to_vec()]);
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

    /// An unsupported special file (Unix socket) must flow through the producer
    /// as a single `SkippedUnsupported` entry carrying the path + type label —
    /// the warn-only channel that does not count as an error.
    #[cfg(unix)]
    #[test]
    fn walked_entry_to_walk_items_unsupported_yields_skipped_unsupported() {
        let tmp = tempfile::tempdir().unwrap();
        let sock_path = tmp.path().join("test.sock");
        let listener = match std::os::unix::net::UnixListener::bind(&sock_path) {
            Ok(l) => l,
            // See `materialize_unix_socket_unsupported` for why we skip these.
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::InvalidInput
                ) =>
            {
                return
            }
            Err(e) => panic!("unexpected bind error: {e}"),
        };
        let _listener = listener;

        let walked = walked_from_path(&sock_path, "test.sock");
        let file_cache = FileCache::default();
        let mut items = walked_entry_to_walk_items(walked, false, &file_cache, u64::MAX, None);
        match items.next() {
            Some(Ok(WalkEntry::SkippedUnsupported { path, file_type })) => {
                assert_eq!(path, sock_path.to_string_lossy());
                assert_eq!(file_type, "socket");
            }
            _ => panic!("expected WalkEntry::SkippedUnsupported"),
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
