//! Cross-platform inode-sorted filesystem walker.
//!
//! On Linux filesystems with fixed inode tables (ext4, xfs, reiserfs),
//! sorting `symlink_metadata()` calls by inode number makes reads sweep
//! sequentially through the inode table instead of seeking randomly. This
//! yields 3-8x speedup on HDD for stat-dominated workloads (e.g. incremental
//! backups where the file cache skips most reads).
//!
//! On macOS and Windows, entries are sorted by filename for deterministic
//! snapshots. All platforms share the same filtering logic (excludes,
//! gitignore, markers, cross-device).

use std::collections::VecDeque;
use std::fs::FileType;
use std::path::{Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use tracing::debug;

use crate::platform::fs::{self, MetadataSummary};
use vykar_types::error::{is_soft_backup_io_error, Result, VykarError};

use super::super::source::{ResolvedSource, RootEmission, SourceKind};
use super::{build_explicit_excludes, should_skip_for_device};

#[cfg(unix)]
fn dir_entry_inode(entry: &std::fs::DirEntry) -> u64 {
    use std::os::unix::fs::DirEntryExt;
    entry.ino()
}

#[cfg(not(unix))]
fn dir_entry_inode(_entry: &std::fs::DirEntry) -> u64 {
    0
}

/// Returns true if inode-sorted stat reduces disk seeks on this filesystem.
/// Only beneficial on Linux ext4/xfs/reiserfs; returns false on all other platforms.
#[cfg(target_os = "linux")]
fn inode_sort_beneficial(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;

    /// Filesystem magic numbers from linux/magic.h where inode order
    /// correlates with on-disk position.
    ///
    /// The type of `statfs.f_type` differs between glibc (`__fsword_t`, i64) and
    /// musl (`c_ulong`, u64). We compare via `as u64` to work on both.
    const EXT_SUPER_MAGIC: u64 = 0xEF53; // ext2/3/4
    const XFS_SUPER_MAGIC: u64 = 0x5846_5342; // "XFSB"
    const REISERFS_SUPER_MAGIC: u64 = 0x5265_4973;

    let c_path = match std::ffi::CString::new(path.as_os_str().as_bytes()) {
        Ok(p) => p,
        Err(_) => {
            tracing::debug!(path = %path.display(), "statfs skipped: path contains null byte");
            return false;
        }
    };
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statfs(c_path.as_ptr(), &mut buf) };
    if rc != 0 {
        tracing::debug!(path = %path.display(), "statfs failed, disabling inode sort");
        return false;
    }
    matches!(
        buf.f_type as u64,
        EXT_SUPER_MAGIC | XFS_SUPER_MAGIC | REISERFS_SUPER_MAGIC
    )
}

#[cfg(not(target_os = "linux"))]
fn inode_sort_beneficial(_path: &Path) -> bool {
    false
}

/// A filesystem entry that has been statted and passed all filters.
pub(in crate::commands::backup) struct WalkedEntry {
    pub abs_path: PathBuf,
    pub metadata: MetadataSummary,
    pub file_type: FileType,
    /// Pre-computed snapshot-relative path, including the multi-path / file-
    /// source basename prefix when `RootEmission::EmitRoot` is in effect.
    pub snapshot_path: String,
}

/// Derive the snapshot-relative path from an absolute entry path.
/// Uses `abs_source` (the canonicalized walk root) for `strip_prefix`.
pub(super) fn rel_path_from_abs(abs_source: &Path, abs_path: &Path) -> String {
    let rel = abs_path
        .strip_prefix(abs_source)
        .unwrap_or(abs_path)
        .to_string_lossy()
        .to_string();
    crate::commands::backup::normalize_rel_path(rel)
}

/// Events yielded by `InodeSortedWalk`.
pub(in crate::commands::backup) enum WalkEvent {
    Entry(WalkedEntry),
    /// A soft error occurred (permission denied, not found, EIO, or a
    /// Windows-specific unsupported-reparse / cloud-file failure). Carries
    /// the failing path and a pre-formatted reason so consumers can surface
    /// a path-bearing warning rather than just bumping an opaque counter.
    Skipped {
        path: PathBuf,
        reason: String,
    },
}

/// Build a single-event `DirLevel` holding the root `WalkedEntry` for an
/// `EmitRoot` source.
fn root_entry_level(
    abs_source: &Path,
    file_type: FileType,
    metadata: MetadataSummary,
    basename: &str,
) -> DirLevel {
    let root_entry = WalkedEntry {
        abs_path: abs_source.to_path_buf(),
        metadata,
        file_type,
        snapshot_path: basename.to_string(),
    };
    DirLevel {
        events: VecDeque::from([WalkEvent::Entry(root_entry)]),
        pending_subdirs: VecDeque::new(),
    }
}

/// Raw directory entry from readdir, before stat.
struct RawDirEntry {
    path: PathBuf,
    ino: u64,
    /// From `d_type`; may be unreliable on some filesystems.
    is_dir_hint: Option<bool>,
}

/// One level in the DFS stack.
struct DirLevel {
    events: VecDeque<WalkEvent>,
    pending_subdirs: VecDeque<PathBuf>,
}

/// Inode-sorted depth-first filesystem walker.
///
/// For each directory, entries are sorted by inode number before calling
/// `symlink_metadata()`, making stat calls sweep sequentially through
/// the inode table on ext4/xfs.
pub(in crate::commands::backup) struct InodeSortedWalk {
    stack: Vec<DirLevel>,
    /// Absolute source path — used for filesystem access (so `read_dir`
    /// returns absolute paths), `strip_prefix` for relative paths, and
    /// as the base for gitignore matching.
    abs_source: PathBuf,
    source_dev: u64,
    one_file_system: bool,
    excludes: Gitignore,
    markers: Vec<String>,
    gitignore_enabled: bool,
    /// Stack of (depth, Gitignore) matchers. Depth is the DFS stack depth
    /// at which the matcher was pushed. When the stack shrinks below that
    /// depth, we pop the matcher.
    gitignore_stack: Vec<(usize, Gitignore)>,
    /// Cached result of `inode_sort_beneficial()` for the source root.
    /// Avoids per-directory `statfs()` + `CString` allocation.
    inode_sort_for_source: bool,
    /// Snapshot-root policy: `SkipRoot` (descendants only, relative to
    /// `abs_source`) or `EmitRoot` (prefix all emitted paths with `basename`).
    policy: RootEmission,
}

impl InodeSortedWalk {
    /// The canonicalized source root. Consumers must use this for `strip_prefix`.
    #[cfg(test)]
    pub fn abs_source(&self) -> &Path {
        &self.abs_source
    }

    /// Create a new inode-sorted walker for the given resolved source.
    ///
    /// The root is re-statted here (not taken from `ResolvedSource`) so that
    /// the metadata emitted for an `EmitRoot` entry reflects the filesystem
    /// state at walk time, not resolve time. `std::fs::metadata` follows
    /// symlinks — `abs_source` is already canonicalized by `ResolvedSource`.
    pub fn new(
        source: &ResolvedSource,
        exclude_patterns: &[String],
        exclude_if_present: &[String],
        one_file_system: bool,
        git_ignore: bool,
    ) -> Result<Self> {
        let abs_source = source.abs_source.clone();

        let source_meta = std::fs::metadata(&abs_source).map_err(|e| {
            VykarError::Other(format!("stat error for {}: {e}", abs_source.display()))
        })?;
        let source_ft = source_meta.file_type();
        let source_summary = fs::summarize_metadata(&source_meta, &source_ft);
        let source_dev = source_summary.device;

        let excludes = build_explicit_excludes(&abs_source, exclude_patterns)?;

        let inode_sort_for_source = inode_sort_beneficial(&abs_source);

        tracing::debug!(
            inode_sort = inode_sort_for_source,
            path = %abs_source.display(),
            "source filesystem detection"
        );

        let mut gitignore_stack = Vec::new();
        if git_ignore {
            let mut ancestors: Vec<PathBuf> = Vec::new();
            let mut cur = abs_source.parent();
            while let Some(dir) = cur {
                let gi_path = dir.join(".gitignore");
                if gi_path.is_file() {
                    ancestors.push(dir.to_path_buf());
                }
                cur = dir.parent();
            }
            // Load from root to deepest ancestor (shallowest first).
            // Each matcher is rooted at its own directory. is_gitignored()
            // receives absolute paths so that strip() can correctly remove
            // each matcher's root prefix — matching how the ignore crate's
            // WalkBuilder handles parent gitignores.
            ancestors.reverse();
            for dir in ancestors {
                let gi_path = dir.join(".gitignore");
                let mut builder = GitignoreBuilder::new(&dir);
                builder.add(&gi_path);
                if let Ok(gi) = builder.build() {
                    // Use depth 0 so these are never popped by the DFS stack.
                    gitignore_stack.push((0, gi));
                }
            }

            // Load the source directory's own .gitignore (if present).
            if source.kind == SourceKind::Directory {
                let source_gi_path = abs_source.join(".gitignore");
                if source_gi_path.is_file() {
                    let mut builder = GitignoreBuilder::new(&abs_source);
                    builder.add(&source_gi_path);
                    if let Ok(gi) = builder.build() {
                        gitignore_stack.push((0, gi));
                    }
                }
            }
        }

        let markers = exclude_if_present.to_vec();

        let mut walk = Self {
            stack: Vec::new(),
            abs_source,
            source_dev,
            one_file_system,
            excludes,
            markers,
            gitignore_enabled: git_ignore,
            gitignore_stack,
            inode_sort_for_source,
            policy: source.policy.clone(),
        };

        // Build the emission order: descendants-first (bottom of stack) so
        // DFS drains the root event on top before descending.
        match (&walk.policy.clone(), source.kind) {
            (RootEmission::SkipRoot, _) => {
                let root_level = walk.build_dir_level(&walk.abs_source)?;
                walk.stack.push(root_level);
            }
            (RootEmission::EmitRoot { basename }, SourceKind::Directory) => {
                let descendants = walk.build_dir_level(&walk.abs_source)?;
                walk.stack.push(descendants);
                walk.stack.push(root_entry_level(
                    &walk.abs_source,
                    source_ft,
                    source_summary,
                    basename,
                ));
            }
            (RootEmission::EmitRoot { basename }, SourceKind::File) => {
                // Never call read_dir on a file source. This is the regression fix.
                walk.stack.push(root_entry_level(
                    &walk.abs_source,
                    source_ft,
                    source_summary,
                    basename,
                ));
            }
        }

        Ok(walk)
    }

    /// Check if a path is excluded by the gitignore stack.
    /// `abs_path` must be the absolute path of the entry so that each
    /// matcher's `strip()` can correctly remove its own root prefix.
    fn is_gitignored(&self, abs_path: &Path, is_dir: bool) -> bool {
        // Iterate deepest-first; first non-None match wins.
        for (_, gi) in self.gitignore_stack.iter().rev() {
            let m = gi.matched_path_or_any_parents(abs_path, is_dir);
            if m.is_ignore() {
                return true;
            }
            if m.is_whitelist() {
                return false;
            }
        }
        false
    }

    /// Build a DirLevel for the given directory path.
    fn build_dir_level(&self, dir: &Path) -> Result<DirLevel> {
        let read_dir = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) => {
                if is_soft_backup_io_error(&e) {
                    return Ok(DirLevel {
                        events: VecDeque::from([WalkEvent::Skipped {
                            path: dir.to_path_buf(),
                            reason: format!("read_dir failed: {e}"),
                        }]),
                        pending_subdirs: VecDeque::new(),
                    });
                }
                return Err(VykarError::Other(format!(
                    "read_dir error for {}: {e}",
                    dir.display()
                )));
            }
        };

        // Phase 1: Collect raw entries with inode from readdir (free — no stat).
        let mut raw_entries: Vec<RawDirEntry> = Vec::new();
        // Per-entry readdir failures: the std `ReadDir` iterator yielded an
        // error before producing the entry name, so we can't name the offending
        // child — we report the parent directory in each Skipped event instead.
        let mut deferred_skips: Vec<(PathBuf, String)> = Vec::new();
        for entry_result in read_dir {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    if is_soft_backup_io_error(&e) {
                        deferred_skips.push((dir.to_path_buf(), format!("readdir failed: {e}")));
                        continue;
                    }
                    return Err(VykarError::Other(format!(
                        "readdir error in {}: {e}",
                        dir.display()
                    )));
                }
            };

            let ino = dir_entry_inode(&entry);
            let path = entry.path();

            // d_type from readdir — may be DT_UNKNOWN on some filesystems.
            let is_dir_hint = match entry.file_type() {
                Ok(ft) => Some(ft.is_dir()),
                Err(_) => None,
            };

            raw_entries.push(RawDirEntry {
                path,
                ino,
                is_dir_hint,
            });
        }

        // Phase 2: Pre-stat filtering (in place to avoid a second Vec allocation).
        raw_entries.retain(|raw| {
            // raw.path is absolute (we walk abs_source), strip abs_source
            // to get a relative path for exclude-pattern matching.
            let rel = match raw.path.strip_prefix(&self.abs_source) {
                Ok(r) => r,
                Err(_) => return false,
            };

            let is_dir = raw.is_dir_hint.unwrap_or(false);

            // Explicit excludes.
            if self
                .excludes
                .matched_path_or_any_parents(rel, is_dir)
                .is_ignore()
            {
                debug!(path = %raw.path.display(), reason = "exclude pattern", "excluded");
                return false;
            }

            // Gitignore filtering (pass absolute path for correct root stripping).
            if self.gitignore_enabled && self.is_gitignored(&raw.path, is_dir) {
                debug!(path = %raw.path.display(), reason = "gitignore", "excluded");
                return false;
            }

            // Marker file exclusion (directories only).
            if is_dir
                && !self.markers.is_empty()
                && self.markers.iter().any(|m| raw.path.join(m).exists())
            {
                debug!(path = %raw.path.display(), reason = "marker file", "excluded");
                return false;
            }

            true
        });

        // Phase 3: Sort entries for sequential disk access.
        if self.inode_sort_for_source {
            // ext4/xfs/reiserfs: inode order ≈ disk order → sequential stat.
            raw_entries.sort_unstable_by_key(|e| e.ino);
        } else {
            // Other filesystems: filename order for deterministic snapshots.
            raw_entries.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        }

        // Phase 4: Stat in inode order and post-stat filter.
        let mut events: VecDeque<WalkEvent> = VecDeque::new();
        let mut pending_subdirs = VecDeque::new();

        for raw in raw_entries {
            let metadata = match std::fs::symlink_metadata(&raw.path) {
                Ok(m) => m,
                Err(e) => {
                    if is_soft_backup_io_error(&e) {
                        events.push_back(WalkEvent::Skipped {
                            path: raw.path.clone(),
                            reason: format!("stat failed: {e}"),
                        });
                        continue;
                    }
                    return Err(VykarError::Other(format!(
                        "stat error for {}: {e}",
                        raw.path.display()
                    )));
                }
            };

            let file_type = metadata.file_type();
            let summary = fs::summarize_metadata(&metadata, &file_type);
            let actual_is_dir = file_type.is_dir();

            // Post-stat: re-check filters if d_type was unknown or actual type differs.
            if raw.is_dir_hint.is_none() || raw.is_dir_hint != Some(actual_is_dir) {
                let rel = match raw.path.strip_prefix(&self.abs_source) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                if self
                    .excludes
                    .matched_path_or_any_parents(rel, actual_is_dir)
                    .is_ignore()
                {
                    debug!(path = %raw.path.display(), reason = "exclude pattern", "excluded");
                    continue;
                }

                if self.gitignore_enabled && self.is_gitignored(&raw.path, actual_is_dir) {
                    debug!(path = %raw.path.display(), reason = "gitignore", "excluded");
                    continue;
                }

                if actual_is_dir
                    && !self.markers.is_empty()
                    && self.markers.iter().any(|m| raw.path.join(m).exists())
                {
                    debug!(path = %raw.path.display(), reason = "marker file", "excluded");
                    continue;
                }
            }

            // one_file_system: skip directories on different devices.
            if actual_is_dir
                && should_skip_for_device(self.one_file_system, self.source_dev, summary.device)
            {
                debug!(path = %raw.path.display(), reason = "different filesystem", "excluded");
                continue;
            }

            if actual_is_dir && !file_type.is_symlink() {
                pending_subdirs.push_back(raw.path.clone());
            }

            let rel = rel_path_from_abs(&self.abs_source, &raw.path);
            let snapshot_path = match &self.policy {
                RootEmission::SkipRoot => rel,
                RootEmission::EmitRoot { basename } => format!("{basename}/{rel}"),
            };

            events.push_back(WalkEvent::Entry(WalkedEntry {
                abs_path: raw.path,
                metadata: summary,
                file_type,
                snapshot_path,
            }));
        }

        // Prepend Skipped events for readdir errors from phase 1 so consumers
        // can increment stats.errors for each lost entry. `push_front` reverses
        // the deferred order, so iterate the deferred list in reverse to keep
        // the original order intact.
        for (path, reason) in deferred_skips.into_iter().rev() {
            events.push_front(WalkEvent::Skipped { path, reason });
        }

        Ok(DirLevel {
            events,
            pending_subdirs,
        })
    }

    /// Push a gitignore matcher for the given directory if it has a .gitignore.
    fn push_gitignore(&mut self, dir: &Path) {
        if !self.gitignore_enabled {
            return;
        }
        let gi_path = dir.join(".gitignore");
        if !gi_path.is_file() {
            return;
        }
        let mut builder = GitignoreBuilder::new(dir);
        builder.add(&gi_path);
        if let Ok(gi) = builder.build() {
            let depth = self.stack.len();
            self.gitignore_stack.push((depth, gi));
        }
    }

    /// Pop gitignore matchers that were pushed at depths >= current stack depth.
    fn pop_gitignore(&mut self) {
        let current_depth = self.stack.len();
        while let Some((depth, _)) = self.gitignore_stack.last() {
            if *depth >= current_depth {
                self.gitignore_stack.pop();
            } else {
                break;
            }
        }
    }
}

impl Iterator for InodeSortedWalk {
    type Item = Result<WalkEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let top = self.stack.last_mut()?;

            // Yield events first (DFS: parent dir entries before children).
            if let Some(event) = top.events.pop_front() {
                return Some(Ok(event));
            }

            // Descend into next pending subdirectory.
            if let Some(subdir) = top.pending_subdirs.pop_front() {
                // Push gitignore for this subdirectory before building its level.
                self.push_gitignore(&subdir);

                match self.build_dir_level(&subdir) {
                    Ok(level) => {
                        self.stack.push(level);
                        continue;
                    }
                    Err(e) => {
                        return Some(Err(e));
                    }
                }
            }

            // This level is exhausted — pop and clean up gitignore matchers.
            self.stack.pop();
            self.pop_gitignore();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn resolve_dir(path: &Path) -> ResolvedSource {
        ResolvedSource::resolve(&path.to_string_lossy(), false).unwrap()
    }

    #[test]
    fn walks_basic_directory_structure() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // Create: root/a.txt, root/sub/b.txt, root/sub/deep/c.txt
        fs::write(root.join("a.txt"), "a").unwrap();
        fs::create_dir_all(root.join("sub/deep")).unwrap();
        fs::write(root.join("sub/b.txt"), "b").unwrap();
        fs::write(root.join("sub/deep/c.txt"), "c").unwrap();

        let walk = InodeSortedWalk::new(&resolve_dir(root), &[], &[], false, false).unwrap();
        let abs_source = walk.abs_source().to_owned();
        let mut paths: Vec<String> = Vec::new();
        for event in walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                paths.push(rel_path_from_abs(&abs_source, &e.abs_path));
            }
        }

        paths.sort();
        assert_eq!(
            paths,
            vec!["a.txt", "sub", "sub/b.txt", "sub/deep", "sub/deep/c.txt"]
        );
    }

    #[test]
    fn respects_exclude_patterns() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        fs::write(root.join("keep.txt"), "k").unwrap();
        fs::write(root.join("skip.log"), "s").unwrap();
        fs::create_dir(root.join("logs")).unwrap();
        fs::write(root.join("logs/app.log"), "l").unwrap();

        let excludes = vec!["*.log".to_string(), "logs".to_string()];
        let walk = InodeSortedWalk::new(&resolve_dir(root), &excludes, &[], false, false).unwrap();
        let abs_source = walk.abs_source().to_owned();
        let mut paths: Vec<String> = Vec::new();
        for event in walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                paths.push(rel_path_from_abs(&abs_source, &e.abs_path));
            }
        }

        paths.sort();
        assert_eq!(paths, vec!["keep.txt"]);
    }

    #[test]
    fn respects_marker_file_exclusion() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        fs::create_dir(root.join("included")).unwrap();
        fs::write(root.join("included/file.txt"), "i").unwrap();
        fs::create_dir(root.join("excluded")).unwrap();
        fs::write(root.join("excluded/file.txt"), "e").unwrap();
        fs::write(root.join("excluded/.nobackup"), "").unwrap();

        let markers = vec![".nobackup".to_string()];
        let walk = InodeSortedWalk::new(&resolve_dir(root), &[], &markers, false, false).unwrap();
        let abs_source = walk.abs_source().to_owned();
        let mut paths: Vec<String> = Vec::new();
        for event in walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                paths.push(rel_path_from_abs(&abs_source, &e.abs_path));
            }
        }

        paths.sort();
        assert_eq!(paths, vec!["included", "included/file.txt"]);
    }

    #[cfg(unix)]
    #[test]
    fn handles_symlinks() {
        use std::os::unix::fs as unix_fs;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        fs::write(root.join("real.txt"), "r").unwrap();
        unix_fs::symlink("real.txt", root.join("link.txt")).unwrap();

        let walk = InodeSortedWalk::new(&resolve_dir(root), &[], &[], false, false).unwrap();
        let abs_source = walk.abs_source().to_owned();
        let mut found_symlink = false;
        for event in walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                if rel_path_from_abs(&abs_source, &e.abs_path) == "link.txt" {
                    assert!(e.file_type.is_symlink());
                    found_symlink = true;
                }
            }
        }
        assert!(found_symlink, "should find symlink");
    }

    #[test]
    fn empty_directory_yields_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let walk = InodeSortedWalk::new(&resolve_dir(tmp.path()), &[], &[], false, false).unwrap();
        let count = walk.count();
        assert_eq!(count, 0);
    }

    #[test]
    fn gitignore_basic() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        fs::write(root.join(".gitignore"), "*.log\nbuild/\n").unwrap();
        fs::write(root.join("keep.txt"), "k").unwrap();
        fs::write(root.join("skip.log"), "s").unwrap();
        fs::create_dir(root.join("build")).unwrap();
        fs::write(root.join("build/output.bin"), "o").unwrap();
        fs::create_dir(root.join("src")).unwrap();
        fs::write(root.join("src/main.rs"), "m").unwrap();

        let walk = InodeSortedWalk::new(&resolve_dir(root), &[], &[], false, true).unwrap();
        let abs_source = walk.abs_source().to_owned();
        let mut paths: Vec<String> = Vec::new();
        for event in walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                paths.push(rel_path_from_abs(&abs_source, &e.abs_path));
            }
        }

        paths.sort();
        // .gitignore itself is included (hidden files are not ignored).
        assert_eq!(paths, vec![".gitignore", "keep.txt", "src", "src/main.rs"]);
    }

    #[test]
    fn gitignore_nested_override() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // Root: ignore *.log
        fs::write(root.join(".gitignore"), "*.log\n").unwrap();
        // Sub: un-ignore *.log (negation)
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("sub/.gitignore"), "!*.log\n").unwrap();
        fs::write(root.join("root.log"), "r").unwrap();
        fs::write(root.join("sub/child.log"), "c").unwrap();
        fs::write(root.join("sub/child.txt"), "t").unwrap();

        let walk = InodeSortedWalk::new(&resolve_dir(root), &[], &[], false, true).unwrap();
        let abs_source = walk.abs_source().to_owned();
        let mut paths: Vec<String> = Vec::new();
        for event in walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                paths.push(rel_path_from_abs(&abs_source, &e.abs_path));
            }
        }

        paths.sort();
        // root.log is ignored, sub/child.log is un-ignored by negation
        assert!(paths.contains(&"sub/child.log".to_string()));
        assert!(!paths.contains(&"root.log".to_string()));
    }

    /// Verify that InodeSortedWalk discovers the expected set of paths for a
    /// non-trivial directory structure with excludes, marker files, gitignore,
    /// and symlinks.
    #[cfg(unix)]
    #[test]
    fn filter_combined_excludes_markers_gitignore() {
        use std::os::unix::fs as unix_fs;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // Build a non-trivial tree:
        // root/
        //   keep.txt
        //   skip.log          (excluded by *.log)
        //   .gitignore        (contains "generated/")
        //   src/
        //     main.rs
        //     lib.rs
        //   build/            (marker-excluded via .nobackup)
        //     .nobackup
        //     output.bin
        //   generated/        (gitignored)
        //     data.bin
        //   deep/
        //     nested/
        //       file.txt
        //       also_skip.log (excluded by *.log)
        //   link.txt -> keep.txt

        fs::write(root.join("keep.txt"), "k").unwrap();
        fs::write(root.join("skip.log"), "s").unwrap();
        fs::write(root.join(".gitignore"), "generated/\n").unwrap();

        fs::create_dir(root.join("src")).unwrap();
        fs::write(root.join("src/main.rs"), "m").unwrap();
        fs::write(root.join("src/lib.rs"), "l").unwrap();

        fs::create_dir(root.join("build")).unwrap();
        fs::write(root.join("build/.nobackup"), "").unwrap();
        fs::write(root.join("build/output.bin"), "o").unwrap();

        fs::create_dir(root.join("generated")).unwrap();
        fs::write(root.join("generated/data.bin"), "d").unwrap();

        fs::create_dir_all(root.join("deep/nested")).unwrap();
        fs::write(root.join("deep/nested/file.txt"), "f").unwrap();
        fs::write(root.join("deep/nested/also_skip.log"), "a").unwrap();

        unix_fs::symlink("keep.txt", root.join("link.txt")).unwrap();

        let excludes = vec!["*.log".to_string()];
        let markers = vec![".nobackup".to_string()];

        let inode_walk =
            InodeSortedWalk::new(&resolve_dir(root), &excludes, &markers, false, true).unwrap();
        let abs_source = inode_walk.abs_source().to_owned();
        let mut paths: Vec<String> = Vec::new();
        for event in inode_walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                paths.push(rel_path_from_abs(&abs_source, &e.abs_path));
            }
        }

        paths.sort();
        assert_eq!(
            paths,
            vec![
                ".gitignore",
                "deep",
                "deep/nested",
                "deep/nested/file.txt",
                "keep.txt",
                "link.txt",
                "src",
                "src/lib.rs",
                "src/main.rs",
            ]
        );
    }

    #[test]
    fn relative_source_dot_slash() {
        let _lock = crate::testutil::CWD_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        fs::create_dir(root.join("target")).unwrap();
        fs::write(root.join("target/a.txt"), "a").unwrap();
        fs::create_dir(root.join("target/sub")).unwrap();
        fs::write(root.join("target/sub/b.txt"), "b").unwrap();

        // cd into root, walk ./target
        let prev_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(root).unwrap();

        let walk = InodeSortedWalk::new(
            &ResolvedSource::resolve("./target", false).unwrap(),
            &[],
            &[],
            false,
            false,
        )
        .unwrap();
        let abs_source = walk.abs_source().to_owned();
        let mut paths: Vec<String> = Vec::new();
        for event in walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                paths.push(rel_path_from_abs(&abs_source, &e.abs_path));
            }
        }

        std::env::set_current_dir(prev_dir).unwrap();

        paths.sort();
        assert_eq!(paths, vec!["a.txt", "sub", "sub/b.txt"]);
    }

    #[test]
    fn relative_source_parent() {
        let _lock = crate::testutil::CWD_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        fs::create_dir(root.join("target")).unwrap();
        fs::write(root.join("target/a.txt"), "a").unwrap();
        fs::create_dir(root.join("sibling")).unwrap();

        // cd into sibling, walk ../target
        let prev_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(root.join("sibling")).unwrap();

        let walk = InodeSortedWalk::new(
            &ResolvedSource::resolve("../target", false).unwrap(),
            &[],
            &[],
            false,
            false,
        )
        .unwrap();
        let abs_source = walk.abs_source().to_owned();
        let mut paths: Vec<String> = Vec::new();
        for event in walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                paths.push(rel_path_from_abs(&abs_source, &e.abs_path));
            }
        }

        std::env::set_current_dir(prev_dir).unwrap();

        paths.sort();
        assert_eq!(paths, vec!["a.txt"]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn inode_sort_beneficial_tmpfs() {
        // /tmp is typically tmpfs on Linux — inode sort should not be beneficial.
        let tmp = tempfile::tempdir().unwrap();
        let result = inode_sort_beneficial(tmp.path());
        // On CI (tmpfs/overlay) this should be false. On ext4 it would be true.
        // Either way, it must not panic.
        let _ = result;
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn inode_sort_beneficial_nonexistent() {
        // Non-existent path should return false (statfs fails).
        assert!(!inode_sort_beneficial(Path::new(
            "/nonexistent/path/that/does/not/exist"
        )));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_source_root() {
        use std::os::unix::fs as unix_fs;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        fs::create_dir(root.join("real")).unwrap();
        fs::write(root.join("real/a.txt"), "a").unwrap();
        fs::create_dir(root.join("real/sub")).unwrap();
        fs::write(root.join("real/sub/b.txt"), "b").unwrap();

        // Symlink root/link -> root/real
        unix_fs::symlink(root.join("real"), root.join("link")).unwrap();

        // Walk via symlink
        let walk =
            InodeSortedWalk::new(&resolve_dir(&root.join("link")), &[], &[], false, false).unwrap();
        let abs_source = walk.abs_source().to_owned();
        let mut paths: Vec<String> = Vec::new();
        for event in walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                paths.push(rel_path_from_abs(&abs_source, &e.abs_path));
            }
        }

        paths.sort();
        assert_eq!(paths, vec!["a.txt", "sub", "sub/b.txt"]);
    }

    #[test]
    fn walker_single_file_yields_one_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("hello.txt");
        fs::write(&file, "world").unwrap();

        let source = ResolvedSource::resolve(&file.to_string_lossy(), false).unwrap();
        let walk = InodeSortedWalk::new(&source, &[], &[], false, false).unwrap();
        let events: Vec<_> = walk.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(events.len(), 1, "expected exactly one entry");
        match &events[0] {
            WalkEvent::Entry(e) => {
                assert!(e.file_type.is_file());
                assert_eq!(e.snapshot_path, "hello.txt");
            }
            _ => panic!("expected Entry"),
        }
    }

    #[test]
    fn walker_empty_file_source() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("empty");
        fs::write(&file, "").unwrap();

        let source = ResolvedSource::resolve(&file.to_string_lossy(), false).unwrap();
        let walk = InodeSortedWalk::new(&source, &[], &[], false, false).unwrap();
        let events: Vec<_> = walk.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(events.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn walker_symlink_to_dir_descends_target() {
        use std::os::unix::fs as unix_fs;
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        fs::create_dir(&real).unwrap();
        fs::write(real.join("a.txt"), "a").unwrap();
        fs::create_dir(real.join("sub")).unwrap();
        fs::write(real.join("sub/b.txt"), "b").unwrap();

        let link = tmp.path().join("link");
        unix_fs::symlink(&real, &link).unwrap();

        let source = ResolvedSource::resolve(&link.to_string_lossy(), false).unwrap();
        let walk = InodeSortedWalk::new(&source, &[], &[], false, false).unwrap();
        let mut snapshot_paths: Vec<String> = Vec::new();
        for ev in walk {
            if let Ok(WalkEvent::Entry(e)) = ev {
                snapshot_paths.push(e.snapshot_path);
            }
        }
        snapshot_paths.sort();
        assert_eq!(snapshot_paths, vec!["a.txt", "sub", "sub/b.txt"]);
    }

    #[test]
    fn walker_directory_skip_root() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("a.txt"), "a").unwrap();

        let source = ResolvedSource::resolve(&tmp.path().to_string_lossy(), false).unwrap();
        let walk = InodeSortedWalk::new(&source, &[], &[], false, false).unwrap();
        let events: Vec<_> = walk.collect::<Result<Vec<_>>>().unwrap();
        // SkipRoot: should NOT include the root directory itself.
        let paths: Vec<String> = events
            .into_iter()
            .filter_map(|e| match e {
                WalkEvent::Entry(entry) => Some(entry.snapshot_path),
                _ => None,
            })
            .collect();
        assert_eq!(paths, vec!["a.txt"]);
    }

    #[cfg(unix)]
    #[test]
    fn walker_directory_emit_root() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("data");
        fs::create_dir(&dir).unwrap();
        fs::write(dir.join("a.txt"), "a").unwrap();
        // Give the directory an obvious, non-default mode so we can verify
        // the walker reads real metadata rather than synthetic placeholders.
        fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o711)).unwrap();

        let source = ResolvedSource::resolve(&dir.to_string_lossy(), true).unwrap();

        // Resolve captures a snapshot of the source above. Mutate the
        // directory mode *before* the walker initializes so we can verify
        // the walker re-stats at init (closing the resolve→walker TOCTOU).
        fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o751)).unwrap();

        let walk = InodeSortedWalk::new(&source, &[], &[], false, false).unwrap();
        let events: Vec<_> = walk.collect::<Result<Vec<_>>>().unwrap();

        let entries: Vec<WalkedEntry> = events
            .into_iter()
            .filter_map(|e| match e {
                WalkEvent::Entry(entry) => Some(entry),
                _ => None,
            })
            .collect();
        assert!(!entries.is_empty());

        // Root entry emitted first with `snapshot_path == "data"`.
        assert_eq!(entries[0].snapshot_path, "data");
        assert!(entries[0].file_type.is_dir());
        // Walker's fresh stat at init picks up the post-resolve mutation.
        assert_eq!(entries[0].metadata.mode & 0o777, 0o751);

        // Descendants prefixed with "data/".
        let descendant_paths: Vec<String> = entries[1..]
            .iter()
            .map(|e| e.snapshot_path.clone())
            .collect();
        assert_eq!(descendant_paths, vec!["data/a.txt"]);
    }
}
