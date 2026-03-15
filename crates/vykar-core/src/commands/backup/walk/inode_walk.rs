//! Inode-sorted filesystem walker for Linux.
//!
//! On filesystems with fixed inode tables (ext4, xfs, reiserfs), sorting
//! `symlink_metadata()` calls by inode number makes reads sweep sequentially
//! through the inode table instead of seeking randomly. This yields 3-8x
//! speedup on HDD for stat-dominated workloads (e.g. incremental backups
//! where the file cache skips most reads).
//!
//! Non-Linux platforms continue using the `ignore`-based walker.

use std::collections::VecDeque;
use std::fs::FileType;
use std::os::unix::fs::DirEntryExt;
use std::path::{Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use tracing::warn;

use crate::platform::fs::{self, MetadataSummary};
use vykar_types::error::{Result, VykarError};

use super::{build_explicit_excludes, is_soft_io_error, should_skip_for_device};

/// A filesystem entry that has been statted and passed all filters.
pub(in crate::commands::backup) struct WalkedEntry {
    pub abs_path: PathBuf,
    pub metadata: MetadataSummary,
    pub file_type: FileType,
}

/// Derive the snapshot-relative path from an absolute entry path.
/// Uses `abs_source` (the canonicalized walk root) for `strip_prefix`.
pub(in crate::commands::backup) fn rel_path_from_abs(abs_source: &Path, abs_path: &Path) -> String {
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
    /// A soft error occurred (permission denied, not found, EIO).
    Skipped,
}

/// Raw directory entry from readdir, before stat.
struct RawDirEntry {
    path: PathBuf,
    ino: u64,
    /// From `d_type`; may be unreliable on some filesystems.
    is_dir_hint: Option<bool>,
    is_symlink_hint: bool,
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
}

impl InodeSortedWalk {
    /// The canonicalized source root. Consumers must use this for `strip_prefix`.
    pub fn abs_source(&self) -> &Path {
        &self.abs_source
    }

    /// Create a new inode-sorted walker for the given source directory.
    pub fn new(
        source: &Path,
        exclude_patterns: &[String],
        exclude_if_present: &[String],
        one_file_system: bool,
        git_ignore: bool,
    ) -> Result<Self> {
        let source_meta = std::fs::symlink_metadata(source).map_err(|e| {
            VykarError::Other(format!(
                "source directory does not exist: {}: {e}",
                source.display()
            ))
        })?;
        let source_ft = source_meta.file_type();
        let source_summary = fs::summarize_metadata(&source_meta, &source_ft);
        let source_dev = source_summary.device;

        let excludes = build_explicit_excludes(source, exclude_patterns)?;

        // Canonicalize the source path so that read_dir returns absolute paths
        // (needed for gitignore prefix stripping) and so that the parent
        // directory chain for ancestor .gitignore discovery is correct even
        // for sources containing `..` components. This matches the ignore
        // crate's `add_parents` which also calls `canonicalize()`.
        let abs_source = std::fs::canonicalize(source).unwrap_or_else(|_| {
            if source.is_absolute() {
                source.to_path_buf()
            } else {
                std::env::current_dir().unwrap_or_default().join(source)
            }
        });

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
            let source_gi_path = abs_source.join(".gitignore");
            if source_gi_path.is_file() {
                let mut builder = GitignoreBuilder::new(&abs_source);
                builder.add(&source_gi_path);
                if let Ok(gi) = builder.build() {
                    gitignore_stack.push((0, gi));
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
        };

        // Build the root DirLevel for the source directory.
        // Walk abs_source so that read_dir returns absolute paths.
        let root_level = walk.build_dir_level(&walk.abs_source)?;
        walk.stack.push(root_level);

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
                if is_soft_io_error(&e) {
                    warn!(path = %dir.display(), error = %e, "skipping directory (read_dir error)");
                    return Ok(DirLevel {
                        events: VecDeque::from([WalkEvent::Skipped]),
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
        let mut skipped_count: usize = 0;
        for entry_result in read_dir {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    if is_soft_io_error(&e) {
                        warn!(path = %dir.display(), error = %e, "skipping entry (readdir error)");
                        skipped_count += 1;
                        continue;
                    }
                    return Err(VykarError::Other(format!(
                        "readdir error in {}: {e}",
                        dir.display()
                    )));
                }
            };

            let ino = entry.ino();
            let path = entry.path();

            // d_type from readdir — may be DT_UNKNOWN on some filesystems.
            let (is_dir_hint, is_symlink_hint) = match entry.file_type() {
                Ok(ft) => (Some(ft.is_dir()), ft.is_symlink()),
                Err(_) => (None, false),
            };

            raw_entries.push(RawDirEntry {
                path,
                ino,
                is_dir_hint,
                is_symlink_hint,
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
                return false;
            }

            // Gitignore filtering (pass absolute path for correct root stripping).
            if self.gitignore_enabled && self.is_gitignored(&raw.path, is_dir) {
                return false;
            }

            // Marker file exclusion (directories only).
            if is_dir && !self.markers.is_empty() {
                if self.markers.iter().any(|m| raw.path.join(m).exists()) {
                    return false;
                }
            }

            true
        });

        // Phase 3: Sort by inode number.
        raw_entries.sort_unstable_by_key(|e| e.ino);

        // Phase 4: Stat in inode order and post-stat filter.
        let mut events: VecDeque<WalkEvent> = VecDeque::new();
        let mut pending_subdirs = VecDeque::new();

        for raw in raw_entries {
            let metadata = match std::fs::symlink_metadata(&raw.path) {
                Ok(m) => m,
                Err(e) => {
                    if is_soft_io_error(&e) {
                        warn!(path = %raw.path.display(), error = %e, "skipping entry (stat error)");
                        events.push_back(WalkEvent::Skipped);
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
                    continue;
                }

                if self.gitignore_enabled && self.is_gitignored(&raw.path, actual_is_dir) {
                    continue;
                }

                if actual_is_dir && !self.markers.is_empty() {
                    if self.markers.iter().any(|m| raw.path.join(m).exists()) {
                        continue;
                    }
                }
            }

            // one_file_system: skip directories on different devices.
            if actual_is_dir
                && should_skip_for_device(self.one_file_system, self.source_dev, summary.device)
            {
                continue;
            }

            if actual_is_dir && !file_type.is_symlink() {
                pending_subdirs.push_back(raw.path.clone());
            }

            events.push_back(WalkEvent::Entry(WalkedEntry {
                abs_path: raw.path,
                metadata: summary,
                file_type,
            }));
        }

        // Prepend Skipped events for readdir errors from phase 1 so consumers
        // can increment stats.errors for each lost entry.
        for _ in 0..skipped_count {
            events.push_front(WalkEvent::Skipped);
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
    use std::os::unix::fs as unix_fs;

    #[test]
    fn walks_basic_directory_structure() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // Create: root/a.txt, root/sub/b.txt, root/sub/deep/c.txt
        fs::write(root.join("a.txt"), "a").unwrap();
        fs::create_dir_all(root.join("sub/deep")).unwrap();
        fs::write(root.join("sub/b.txt"), "b").unwrap();
        fs::write(root.join("sub/deep/c.txt"), "c").unwrap();

        let walk = InodeSortedWalk::new(root, &[], &[], false, false).unwrap();
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
        let walk = InodeSortedWalk::new(root, &excludes, &[], false, false).unwrap();
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
        let walk = InodeSortedWalk::new(root, &[], &markers, false, false).unwrap();
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

    #[test]
    fn handles_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        fs::write(root.join("real.txt"), "r").unwrap();
        unix_fs::symlink("real.txt", root.join("link.txt")).unwrap();

        let walk = InodeSortedWalk::new(root, &[], &[], false, false).unwrap();
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
        let walk = InodeSortedWalk::new(tmp.path(), &[], &[], false, false).unwrap();
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

        let walk = InodeSortedWalk::new(root, &[], &[], false, true).unwrap();
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

        let walk = InodeSortedWalk::new(root, &[], &[], false, true).unwrap();
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

    /// Verify that InodeSortedWalk discovers the same set of paths as the
    /// ignore-crate walker for a non-trivial directory structure with
    /// excludes, marker files, and gitignore.
    #[test]
    fn filter_equivalence_with_ignore_walker() {
        use super::build_configured_walker;

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

        // Add a symlink for good measure.
        unix_fs::symlink("keep.txt", root.join("link.txt")).unwrap();

        let excludes = vec!["*.log".to_string()];
        let markers = vec![".nobackup".to_string()];

        // Collect paths from InodeSortedWalk.
        let inode_walk = InodeSortedWalk::new(root, &excludes, &markers, false, true).unwrap();
        let abs_source = inode_walk.abs_source().to_owned();
        let mut inode_paths: Vec<String> = Vec::new();
        for event in inode_walk {
            if let Ok(WalkEvent::Entry(e)) = event {
                inode_paths.push(rel_path_from_abs(&abs_source, &e.abs_path));
            }
        }

        // Collect paths from ignore-crate walker.
        let walk_builder = build_configured_walker(root, &excludes, &markers, false, true).unwrap();
        let mut ignore_paths: Vec<String> = Vec::new();
        for entry in walk_builder.build() {
            let entry = entry.unwrap();
            let rel = entry
                .path()
                .strip_prefix(root)
                .unwrap_or(entry.path())
                .to_string_lossy()
                .to_string();
            if rel.is_empty() {
                continue;
            }
            ignore_paths.push(rel);
        }

        inode_paths.sort();
        ignore_paths.sort();

        assert_eq!(
            inode_paths, ignore_paths,
            "InodeSortedWalk and ignore walker must discover identical paths"
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

        let walk = InodeSortedWalk::new(Path::new("./target"), &[], &[], false, false).unwrap();
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

        let walk = InodeSortedWalk::new(Path::new("../target"), &[], &[], false, false).unwrap();
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

    #[test]
    fn symlinked_source_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        fs::create_dir(root.join("real")).unwrap();
        fs::write(root.join("real/a.txt"), "a").unwrap();
        fs::create_dir(root.join("real/sub")).unwrap();
        fs::write(root.join("real/sub/b.txt"), "b").unwrap();

        // Symlink root/link -> root/real
        unix_fs::symlink(root.join("real"), root.join("link")).unwrap();

        // Walk via symlink
        let walk = InodeSortedWalk::new(&root.join("link"), &[], &[], false, false).unwrap();
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
}
