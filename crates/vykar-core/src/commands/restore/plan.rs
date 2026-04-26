//! Phase 2: stream snapshot items, create dirs/symlinks immediately, build the
//! file plan + chunk-target map for the parallel restore phases.

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::AtomicBool;

use smallvec::SmallVec;

use crate::platform::fs;
use crate::snapshot::item::ItemType;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use super::{apply_item_xattrs, push_metadata_warning, warn_metadata_err, RestoreStats};

/// Classification of a symlink's stored target for restore-time auditing.
/// Used to warn the operator when a snapshot carries symlinks that escape the
/// restore root or point at absolute system paths. The link itself is still
/// restored as-is — these are flags, not rejections.
#[derive(Debug, PartialEq, Eq)]
enum SymlinkSafety {
    Safe,
    Absolute,
    EscapesParent,
}

/// Classify a symlink's stored target string using host-platform path
/// semantics. Assumes snapshots are restored on the same platform they were
/// captured on (a Linux snapshot is restored on Linux, etc.); cross-platform
/// restore is unsupported.
fn classify_symlink_target(target: &str) -> SymlinkSafety {
    let path = Path::new(target);
    if path.is_absolute() {
        return SymlinkSafety::Absolute;
    }
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return SymlinkSafety::EscapesParent;
    }
    SymlinkSafety::Safe
}

/// Where to write a chunk's decompressed data.
pub(super) struct WriteTarget {
    pub(super) file_idx: usize,
    pub(super) file_offset: u64,
}

/// Output file metadata for post-restore attribute application.
pub(super) struct PlannedFile {
    pub(super) rel_path: PathBuf,
    pub(super) total_size: u64,
    pub(super) mode: u32,
    pub(super) mtime: i64,
    pub(super) xattrs: Option<HashMap<String, Vec<u8>>>,
    /// CAS flag: the first worker to open this file calls `set_len`.
    /// Prevents repeated `ftruncate` syscalls when multiple workers open
    /// the same large file across different read groups.
    pub(super) created: AtomicBool,
}

/// Aggregated write targets and expected logical size for a single chunk.
pub(super) struct ChunkTargets {
    pub(super) expected_size: u32,
    pub(super) targets: SmallVec<[WriteTarget; 1]>,
}

/// Stream items from raw bytes: create dirs/symlinks immediately, accumulate
/// regular files into bounded batches, and invoke `flush_batch` whenever a
/// batch fills up. After the stream ends a final flush is always invoked
/// (even with empty batch contents) so callers see the post-stream state.
///
/// Bounded batching keeps peak memory proportional to `batch_size` rather than
/// to total file count — a 10M-file restore that would otherwise allocate
/// gigabytes of `PlannedFile` state stays well-bounded. Cross-batch chunk
/// reuse pays a re-download cost for chunks referenced from files in
/// different batches; pack locality within a `walkdir`-ordered window keeps
/// this cost small in practice.
///
/// Directories in the snapshot stream typically precede their children (natural
/// `walkdir` order), so `verified_dirs` is populated before files/symlinks that
/// need it.  When a file or symlink appears before its parent directory item,
/// `ensure_parent_exists_within_root` handles it (the later directory item's
/// `create_dir_all` is a no-op, but mode/xattrs are still applied).
/// `verified_dirs` lives across batches and is passed to `flush_batch` by
/// reference so phase 3 can skip canonicalize for already-verified parents.
///
/// Because directories are created during decoding, a malformed item stream
/// that fails to decode partway through may leave partial directories on disk.
/// This is acceptable — directory creation is idempotent and restore is not
/// transactional.
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub(super) fn stream_and_plan<F, B>(
    items_stream: &[u8],
    dest_root: &Path,
    include_path: &mut F,
    xattrs_enabled: bool,
    stats: &mut RestoreStats,
    batch_size: usize,
    mut flush_batch: B,
) -> Result<()>
where
    F: FnMut(&str) -> bool,
    B: FnMut(
        Vec<PlannedFile>,
        HashMap<ChunkId, ChunkTargets>,
        &HashSet<PathBuf>,
        &mut RestoreStats,
    ) -> Result<()>,
{
    let mut verified_dirs: HashSet<PathBuf> = HashSet::new();
    verified_dirs.insert(dest_root.to_path_buf());
    let mut planned_files: Vec<PlannedFile> = Vec::new();
    let mut chunk_targets: HashMap<ChunkId, ChunkTargets> = HashMap::new();
    let mut rel_scratch = PathBuf::new();

    crate::commands::list::for_each_decoded_item(items_stream, |item| {
        if !include_path(&item.path) {
            return Ok(());
        }
        item.validate()?;
        match item.entry_type {
            ItemType::Directory => {
                sanitize_item_path_into(&item.path, &mut rel_scratch)?;
                let target = dest_root.join(&rel_scratch);
                ensure_path_within_root(&target, dest_root)?;
                std::fs::create_dir_all(&target)?;
                ensure_path_within_root(&target, dest_root)?;
                warn_metadata_err(stats, fs::apply_mode(&target, item.mode), &target, "mode");
                if xattrs_enabled {
                    apply_item_xattrs(&target, item.xattrs.as_ref(), stats);
                }
                verified_dirs.insert(target);
                stats.dirs += 1;
            }
            ItemType::Symlink => {
                if let Some(ref link_target) = item.link_target {
                    sanitize_item_path_into(&item.path, &mut rel_scratch)?;
                    let target = dest_root.join(&rel_scratch);
                    if target.parent().is_none_or(|p| !verified_dirs.contains(p)) {
                        ensure_parent_exists_within_root(&target, dest_root)?;
                    }
                    match classify_symlink_target(link_target) {
                        SymlinkSafety::Safe => {}
                        SymlinkSafety::Absolute => push_metadata_warning(
                            stats,
                            format!(
                                "symlink '{}' points to absolute target '{}' (restored as-is)",
                                item.path, link_target
                            ),
                        ),
                        SymlinkSafety::EscapesParent => push_metadata_warning(
                            stats,
                            format!(
                                "symlink '{}' points outside its parent ('..') target '{}' (restored as-is)",
                                item.path, link_target
                            ),
                        ),
                    }
                    let _ = std::fs::remove_file(&target);
                    fs::create_symlink(Path::new(link_target), &target)?;
                    if xattrs_enabled {
                        apply_item_xattrs(&target, item.xattrs.as_ref(), stats);
                    }
                    stats.symlinks += 1;
                }
            }
            ItemType::RegularFile => {
                sanitize_item_path_into(&item.path, &mut rel_scratch)?;
                let file_idx = planned_files.len();
                let mut file_offset: u64 = 0;
                for chunk_ref in &item.chunks {
                    let entry = chunk_targets
                        .entry(chunk_ref.id)
                        .or_insert_with(|| ChunkTargets {
                            expected_size: chunk_ref.size,
                            targets: SmallVec::new(),
                        });
                    if entry.expected_size != chunk_ref.size {
                        return Err(VykarError::InvalidFormat(format!(
                            "chunk {} has inconsistent logical sizes in snapshot metadata: {} vs {}",
                            chunk_ref.id, entry.expected_size, chunk_ref.size
                        )));
                    }
                    entry.targets.push(WriteTarget {
                        file_idx,
                        file_offset,
                    });
                    file_offset =
                        file_offset
                            .checked_add(chunk_ref.size as u64)
                            .ok_or_else(|| {
                                VykarError::InvalidFormat(format!(
                                    "file offset overflow building restore plan for {:?}",
                                    item.path
                                ))
                            })?;
                }
                // Hand the scratch buffer's allocation to the PlannedFile and
                // re-init scratch — the next sanitize_item_path_into call
                // resizes the fresh buffer to its needs. This avoids the
                // per-file PathBuf clone the scratch was meant to eliminate.
                planned_files.push(PlannedFile {
                    rel_path: std::mem::take(&mut rel_scratch),
                    total_size: file_offset,
                    mode: item.mode,
                    mtime: item.mtime,
                    xattrs: item.xattrs,
                    created: AtomicBool::new(false),
                });
                if planned_files.len() >= batch_size {
                    let batch_files = std::mem::take(&mut planned_files);
                    let batch_chunks = std::mem::take(&mut chunk_targets);
                    flush_batch(batch_files, batch_chunks, &verified_dirs, stats)?;
                }
            }
        }
        Ok(())
    })?;

    // Final flush — always invoked so the caller observes terminal
    // verified_dirs / dir-only restores even if no files remain.
    flush_batch(planned_files, chunk_targets, &verified_dirs, stats)?;
    Ok(())
}

/// Validate the restore destination: must be non-existing or empty.
/// Creates the directory if it doesn't exist. Returns the canonicalized path.
pub(super) fn validate_and_prepare_dest(dest: &str) -> Result<PathBuf> {
    let dest_path = Path::new(dest);
    if dest_path.exists() {
        let is_empty = dest_path
            .read_dir()
            .map_err(|e| VykarError::Other(format!("cannot read destination '{}': {e}", dest)))?
            .next()
            .is_none();
        if !is_empty {
            return Err(VykarError::Config(format!(
                "restore destination '{}' is not empty; use an empty or non-existing directory",
                dest
            )));
        }
    } else {
        std::fs::create_dir_all(dest_path)?;
    }
    dest_path
        .canonicalize()
        .map_err(|e| VykarError::Other(format!("invalid destination '{}': {e}", dest)))
}

pub(super) fn ensure_parent_exists_within_root(target: &Path, root: &Path) -> Result<()> {
    if let Some(parent) = target.parent() {
        ensure_path_within_root(parent, root)?;
        std::fs::create_dir_all(parent)?;
        ensure_path_within_root(parent, root)?;
    }
    Ok(())
}

fn ensure_path_within_root(path: &Path, root: &Path) -> Result<()> {
    let mut cursor = Some(path);
    while let Some(candidate) = cursor {
        if candidate.exists() {
            let canonical = candidate
                .canonicalize()
                .map_err(|e| VykarError::Other(format!("path check failed: {e}")))?;
            if !canonical.starts_with(root) {
                return Err(VykarError::InvalidFormat(format!(
                    "refusing to restore outside destination: {}",
                    path.display()
                )));
            }
            return Ok(());
        }
        cursor = candidate.parent();
    }
    Err(VykarError::InvalidFormat(format!(
        "invalid restore target path: {}",
        path.display()
    )))
}

/// Sanitize and write a snapshot item path into a caller-provided scratch buffer.
/// Reuses the PathBuf allocation across calls (~387K items), avoiding per-item
/// intermediate PathBuf allocations.
fn sanitize_item_path_into(raw: &str, out: &mut PathBuf) -> Result<()> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(VykarError::InvalidFormat(format!(
            "refusing to restore absolute path: {raw}"
        )));
    }
    out.clear();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(VykarError::InvalidFormat(format!(
                    "refusing to restore unsafe path: {raw}"
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(VykarError::InvalidFormat(format!(
            "refusing to restore empty path: {raw}"
        )));
    }
    Ok(())
}

#[cfg(test)]
fn sanitize_item_path(raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(VykarError::InvalidFormat(format!(
            "refusing to restore absolute path: {raw}"
        )));
    }
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(VykarError::InvalidFormat(format!(
                    "refusing to restore unsafe path: {raw}"
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(VykarError::InvalidFormat(format!(
            "refusing to restore empty path: {raw}"
        )));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::restore::test_support::{
        make_dir_item, make_file_item, make_file_item_with_size, make_symlink_item, serialize_items,
    };
    use crate::snapshot::item::Item;
    use tempfile::tempdir;

    /// Drains every batch into local accumulators so existing assertions can
    /// keep operating on a single (files, chunks, verified_dirs) tuple.
    #[allow(clippy::type_complexity)]
    fn collect_all<F: FnMut(&str) -> bool>(
        stream: &[u8],
        dest: &Path,
        mut filter: F,
        xattrs_enabled: bool,
        stats: &mut RestoreStats,
    ) -> Result<(
        Vec<PlannedFile>,
        HashMap<ChunkId, ChunkTargets>,
        HashSet<PathBuf>,
    )> {
        let mut all_files: Vec<PlannedFile> = Vec::new();
        let mut all_chunks: HashMap<ChunkId, ChunkTargets> = HashMap::new();
        let mut all_verified: HashSet<PathBuf> = HashSet::new();
        stream_and_plan(
            stream,
            dest,
            &mut filter,
            xattrs_enabled,
            stats,
            usize::MAX,
            |files, chunks, verified, _stats| {
                all_files.extend(files);
                for (k, v) in chunks {
                    all_chunks.insert(k, v);
                }
                all_verified.clone_from(verified);
                Ok(())
            },
        )?;
        Ok((all_files, all_chunks, all_verified))
    }

    #[test]
    fn classify_symlink_target_safe_relative() {
        assert_eq!(classify_symlink_target("foo/bar"), SymlinkSafety::Safe);
        assert_eq!(classify_symlink_target("file.txt"), SymlinkSafety::Safe);
        assert_eq!(classify_symlink_target("./foo"), SymlinkSafety::Safe);
    }

    #[cfg(unix)]
    #[test]
    fn classify_symlink_target_absolute_unix() {
        assert_eq!(
            classify_symlink_target("/etc/passwd"),
            SymlinkSafety::Absolute
        );
        assert_eq!(classify_symlink_target("/"), SymlinkSafety::Absolute);
    }

    #[test]
    fn classify_symlink_target_dotdot_traversal() {
        assert_eq!(
            classify_symlink_target("../etc/passwd"),
            SymlinkSafety::EscapesParent
        );
        assert_eq!(
            classify_symlink_target("../../escape"),
            SymlinkSafety::EscapesParent
        );
    }

    #[test]
    fn classify_symlink_target_dotdot_in_middle() {
        // Even targets that net-resolve inside warrant a warning — we do not
        // canonicalize because the snapshot's paths are not on disk yet.
        assert_eq!(
            classify_symlink_target("foo/../bar"),
            SymlinkSafety::EscapesParent
        );
    }

    #[test]
    fn stream_and_plan_warns_on_absolute_symlink() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![make_symlink_item("link", "/etc/passwd")];
        let stream = serialize_items(&items);

        let mut stats = RestoreStats::default();
        collect_all(&stream, dest, |_| true, false, &mut stats).unwrap();

        assert_eq!(stats.symlinks, 1);
        assert_eq!(stats.warnings.len(), 1);
        assert!(
            stats.warnings[0].contains("absolute target"),
            "got: {}",
            stats.warnings[0]
        );
        // Symlink was still created.
        assert!(dest
            .join("link")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[test]
    fn stream_and_plan_warns_on_dotdot_symlink() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![make_symlink_item("link", "../../escape")];
        let stream = serialize_items(&items);

        let mut stats = RestoreStats::default();
        collect_all(&stream, dest, |_| true, false, &mut stats).unwrap();

        assert_eq!(stats.symlinks, 1);
        assert_eq!(stats.warnings.len(), 1);
        assert!(
            stats.warnings[0].contains("outside its parent"),
            "got: {}",
            stats.warnings[0]
        );
    }

    #[test]
    fn stream_and_plan_no_warning_on_safe_symlink() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![
            make_dir_item("d", 0o755),
            make_symlink_item("d/link", "sibling.txt"),
        ];
        let stream = serialize_items(&items);

        let mut stats = RestoreStats::default();
        collect_all(&stream, dest, |_| true, false, &mut stats).unwrap();

        assert_eq!(stats.symlinks, 1);
        assert!(stats.warnings.is_empty(), "got: {:?}", stats.warnings);
    }

    #[test]
    fn sanitize_rejects_parent_dir_traversal() {
        let err = sanitize_item_path("../etc/passwd").unwrap_err().to_string();
        assert!(err.contains("unsafe path"));
    }

    // -----------------------------------------------------------------------
    // stream_and_plan tests
    // -----------------------------------------------------------------------

    #[test]
    fn stream_and_plan_dirs_before_files() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        // Serialize in reverse order: file before its parent dir.
        let items = vec![
            make_file_item("mydir/a.txt", vec![(0xAA, 100)]),
            make_dir_item("mydir", 0o755),
        ];
        let stream = serialize_items(&items);

        let mut stats = RestoreStats::default();
        let (planned_files, chunk_targets, _verified_dirs) =
            collect_all(&stream, dest, |_| true, false, &mut stats).unwrap();

        // Directory was created (pass 1 runs before pass 2).
        assert!(dest.join("mydir").is_dir());
        assert_eq!(stats.dirs, 1);

        // File is in planned_files.
        assert_eq!(planned_files.len(), 1);
        assert_eq!(planned_files[0].rel_path, Path::new("mydir/a.txt"));
        assert_eq!(chunk_targets.len(), 1);
    }

    #[test]
    fn stream_and_plan_respects_filter() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![
            make_dir_item("included", 0o755),
            make_dir_item("excluded", 0o755),
            make_file_item("included/a.txt", vec![(0xAA, 100)]),
            make_file_item("excluded/b.txt", vec![(0xBB, 200)]),
        ];
        let stream = serialize_items(&items);

        let mut stats = RestoreStats::default();
        let (planned_files, _chunk_targets, _verified_dirs) = collect_all(
            &stream,
            dest,
            |p: &str| p.starts_with("included"),
            false,
            &mut stats,
        )
        .unwrap();

        // Only the included directory was created.
        assert!(dest.join("included").is_dir());
        assert!(!dest.join("excluded").exists());
        assert_eq!(stats.dirs, 1);

        // Only the included file is planned.
        assert_eq!(planned_files.len(), 1);
        assert_eq!(planned_files[0].rel_path, Path::new("included/a.txt"));
    }

    #[test]
    fn stream_and_plan_only_retains_files() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let n_dirs = 5;
        let m_files = 3;
        let mut items: Vec<Item> = Vec::new();
        for i in 0..n_dirs {
            items.push(make_dir_item(&format!("dir{i}"), 0o755));
        }
        for i in 0..m_files {
            items.push(make_file_item(
                &format!("dir0/file{i}.txt"),
                vec![((0xA0 + i) as u8, 100)],
            ));
        }
        // Add a symlink too — should not be in planned_files.
        items.push(make_symlink_item("dir0/link", "file0.txt"));
        let stream = serialize_items(&items);

        let mut stats = RestoreStats::default();
        let (planned_files, _chunk_targets, _verified_dirs) =
            collect_all(&stream, dest, |_| true, false, &mut stats).unwrap();

        assert_eq!(planned_files.len(), m_files as usize);
        assert_eq!(stats.dirs, n_dirs);
        assert_eq!(stats.symlinks, 1);
    }

    #[test]
    fn stream_and_plan_decode_failure_leaves_partial_dirs() {
        // Extraction is not transactional: a decode error partway through the
        // stream may leave already-created directories on disk.  This test
        // documents that behavior as intentional.
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let mut stream = serialize_items(&[make_dir_item("aaa", 0o755)]);
        // Append garbage bytes to trigger a decode error after the first item.
        stream.extend_from_slice(&[0xFF, 0xFF, 0xFF]);

        let mut stats = RestoreStats::default();
        let result = collect_all(&stream, dest, |_| true, false, &mut stats);
        assert!(result.is_err());
        // The directory from before the corrupt bytes was still created.
        assert!(dest.join("aaa").is_dir());
    }

    #[test]
    fn stream_and_plan_symlink_before_parent_dir() {
        // Symlink appears before its parent directory in the stream.
        // Single-pass should handle this via ensure_parent_exists_within_root
        // and then apply the correct mode when the directory item arrives.
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![
            make_symlink_item("mydir/link", "target"),
            make_dir_item("mydir", 0o750),
        ];
        let stream = serialize_items(&items);

        let mut stats = RestoreStats::default();
        let (_planned_files, _chunk_targets, verified_dirs) =
            collect_all(&stream, dest, |_| true, false, &mut stats).unwrap();

        // Directory exists and is in verified_dirs.
        assert!(dest.join("mydir").is_dir());
        assert!(verified_dirs.contains(&dest.join("mydir")));

        // Symlink was created and points to the right target.
        let link_path = dest.join("mydir/link");
        assert!(link_path
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::read_link(&link_path).unwrap().to_str().unwrap(),
            "target"
        );

        // Directory has the correct mode from the directory item (not default).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dest.join("mydir"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o750);
        }

        assert_eq!(stats.dirs, 1);
        assert_eq!(stats.symlinks, 1);
    }

    #[test]
    fn stream_and_plan_file_before_parent_dir() {
        // File item appears before its parent directory in the stream.
        // The directory should still get its mode applied when the dir item
        // is processed later.
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![
            make_file_item("mydir/a.txt", vec![(0xAA, 100)]),
            make_dir_item("mydir", 0o750),
        ];
        let stream = serialize_items(&items);

        let mut stats = RestoreStats::default();
        let (planned_files, chunk_targets, verified_dirs) =
            collect_all(&stream, dest, |_| true, false, &mut stats).unwrap();

        // Directory exists with correct mode.
        assert!(dest.join("mydir").is_dir());
        assert!(verified_dirs.contains(&dest.join("mydir")));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dest.join("mydir"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o750);
        }

        // File is in planned_files.
        assert_eq!(planned_files.len(), 1);
        assert_eq!(planned_files[0].rel_path, Path::new("mydir/a.txt"));
        assert_eq!(chunk_targets.len(), 1);
        assert_eq!(stats.dirs, 1);
    }

    // -----------------------------------------------------------------------
    // validate_and_prepare_dest tests
    // -----------------------------------------------------------------------

    #[test]
    fn validate_dest_rejects_non_empty_directory() {
        let temp = tempdir().unwrap();
        // Create a file inside so it's non-empty.
        std::fs::write(temp.path().join("existing.txt"), b"data").unwrap();
        let err = validate_and_prepare_dest(temp.path().to_str().unwrap())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not empty"),
            "expected 'not empty' error, got: {err}"
        );
    }

    #[test]
    fn validate_dest_accepts_empty_directory() {
        let temp = tempdir().unwrap();
        let dest = validate_and_prepare_dest(temp.path().to_str().unwrap()).unwrap();
        assert!(dest.is_dir());
    }

    #[test]
    fn validate_dest_creates_non_existing_directory() {
        let temp = tempdir().unwrap();
        let new_dir = temp.path().join("brand-new");
        assert!(!new_dir.exists());
        let dest = validate_and_prepare_dest(new_dir.to_str().unwrap()).unwrap();
        assert!(dest.is_dir());
    }

    #[test]
    fn validate_dest_creates_nested_non_existing_directory() {
        let temp = tempdir().unwrap();
        let new_dir = temp.path().join("a/b/c");
        assert!(!new_dir.exists());
        let dest = validate_and_prepare_dest(new_dir.to_str().unwrap()).unwrap();
        assert!(dest.is_dir());
    }

    // -----------------------------------------------------------------------
    // item.size invariant tests
    // -----------------------------------------------------------------------

    #[test]
    fn stream_and_plan_rejects_size_without_chunks() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![make_file_item_with_size("a.txt", 100, vec![])];
        let stream = serialize_items(&items);

        let mut stats = RestoreStats::default();
        let err = match collect_all(&stream, dest, |_| true, false, &mut stats) {
            Ok(_) => panic!("expected size-vs-chunks mismatch error"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("chunk sizes sum to"),
            "expected size-vs-chunks mismatch error, got: {err}"
        );
    }

    #[test]
    fn stream_and_plan_rejects_size_mismatch_with_chunks() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        // item.size = 100 but chunk sums to 50.
        let items = vec![make_file_item_with_size("a.txt", 100, vec![(0xAA, 50)])];
        let stream = serialize_items(&items);

        let mut stats = RestoreStats::default();
        let err = match collect_all(&stream, dest, |_| true, false, &mut stats) {
            Ok(_) => panic!("expected size-vs-chunks mismatch error"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("chunk sizes sum to"),
            "expected size-vs-chunks mismatch error, got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // batch boundary tests
    // -----------------------------------------------------------------------

    #[test]
    fn stream_and_plan_invokes_flush_on_batch_boundary() {
        // batch_size = 2, 5 file items → flushes of size 2, 2, 1.
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items: Vec<Item> = (0..5)
            .map(|i| make_file_item(&format!("f{i}.txt"), vec![(0xA0 + i as u8, 100)]))
            .collect();
        let stream = serialize_items(&items);

        let mut sizes: Vec<usize> = Vec::new();
        let mut stats = RestoreStats::default();
        stream_and_plan(
            &stream,
            dest,
            &mut |_| true,
            false,
            &mut stats,
            2,
            |files, _chunks, _verified, _stats| {
                sizes.push(files.len());
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(sizes, vec![2, 2, 1]);
    }

    #[test]
    fn stream_and_plan_final_flush_invoked_with_no_files() {
        // No regular file items — final flush still invoked once with empty
        // contents so callers see post-stream verified_dirs for dir-only
        // restores.
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![make_dir_item("only-a-dir", 0o755)];
        let stream = serialize_items(&items);

        let mut flush_calls = 0usize;
        let mut last_verified: HashSet<PathBuf> = HashSet::new();
        let mut stats = RestoreStats::default();
        stream_and_plan(
            &stream,
            dest,
            &mut |_| true,
            false,
            &mut stats,
            100,
            |files, chunks, verified, _stats| {
                flush_calls += 1;
                assert!(files.is_empty());
                assert!(chunks.is_empty());
                last_verified.clone_from(verified);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(flush_calls, 1);
        assert!(last_verified.contains(&dest.join("only-a-dir")));
    }
}
