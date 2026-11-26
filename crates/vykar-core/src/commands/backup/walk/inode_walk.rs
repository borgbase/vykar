//! Cross-platform inode-sorted filesystem walker.
//!
//! On Linux filesystems with fixed inode tables (ext4, xfs, reiserfs),
//! sorting `symlink_metadata()` calls by inode number makes reads sweep
//! sequentially through the inode table instead of seeking randomly. This
//! yields 3-8x speedup on HDD for stat-dominated workloads (e.g. incremental
//! backups where the file cache skips most reads).
//!
//! Elsewhere (macOS, Windows, other Linux filesystems) stat runs in name
//! order. All platforms share the same filtering logic (excludes, gitignore,
//! markers, cross-device).
//!
//! Entries are emitted, and subdirectories descended, in name order (the
//! bytes of the snapshot path, see `emit_key`) on every filesystem; on
//! ext4/xfs/reiserfs stat runs in inode order first and the directory is
//! re-sorted before emission. Every directory's entries are emitted before
//! any of its descendants, so each root's entries come out in dfs-name order.

use std::collections::VecDeque;
use std::fs::FileType;
use std::path::{Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use tracing::debug;

use crate::platform::fs::{self, MetadataSummary, dir_entry_inode, inode_sort_beneficial};
use vykar_types::error::{Result, VykarError, is_fileprovider_deadlock, is_soft_backup_io_error};

use super::super::source::{ResolvedSource, RootEmission, SourceKind};
use super::{
    DatalessKind, DirSkipKind, build_explicit_excludes, classify_dir_skip, should_skip_for_device,
};

/// Order in which a directory's entries are emitted (and its subdirectories
/// descended). Stat order is unaffected: it stays inode-sorted wherever
/// `inode_sort_beneficial` holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmitOrder {
    /// Stat order: inode order on ext4/xfs/reiserfs, name order elsewhere.
    /// The emission order before 06 step 3, kept only so `walk_measure` can
    /// time it against `Name`. Never used by a backup.
    Stat,
    /// Name order on every filesystem (06 order 1, "dfs-name"). What
    /// backups use.
    Name,
}

/// Sort key of a walk event for name-ordered emission: an entry's snapshot
/// path as the item stores it (what `Item::path_bytes` returns: the raw
/// shadow when present, else the display path). Skip events write no item
/// and are keyed on their absolute path instead, so they do not interleave
/// with the entries by name but cluster (usually before them); that only has
/// to be deterministic.
fn emit_key(event: &WalkEvent) -> &[u8] {
    match event {
        WalkEvent::Entry(e) => e
            .snapshot_path_raw
            .as_deref()
            .unwrap_or(e.snapshot_path.as_bytes()),
        WalkEvent::Skipped { path, .. } | WalkEvent::SkippedDataless { path, .. } => {
            path.as_os_str().as_encoded_bytes()
        }
    }
}

/// Total order for name-ordered emission: `emit_key`, then the encoded
/// absolute path, so that two entries whose lossy paths collide (unpaired
/// surrogates on Windows) still come out in a fixed order.
fn emit_cmp(a: &WalkEvent, b: &WalkEvent) -> std::cmp::Ordering {
    emit_key(a).cmp(emit_key(b)).then_with(|| {
        event_path(a)
            .as_os_str()
            .as_encoded_bytes()
            .cmp(event_path(b).as_os_str().as_encoded_bytes())
    })
}

fn event_path(event: &WalkEvent) -> &Path {
    match event {
        WalkEvent::Entry(e) => &e.abs_path,
        WalkEvent::Skipped { path, .. } | WalkEvent::SkippedDataless { path, .. } => path,
    }
}

/// A filesystem entry that has been statted and passed all filters.
pub(in crate::commands::backup) struct WalkedEntry {
    pub abs_path: PathBuf,
    pub metadata: MetadataSummary,
    pub file_type: FileType,
    /// Pre-computed snapshot-relative path (lossy UTF-8 **display** form),
    /// including the multi-path / file-source prefix when
    /// `RootEmission::EmitRoot` is in effect.
    pub snapshot_path: String,
    /// Byte-faithful form of `snapshot_path` when any component (the
    /// `EmitRoot` prefix or the relative path) is not valid UTF-8. `None` for
    /// the common case. Unix-only; always `None` elsewhere.
    pub snapshot_path_raw: Option<Vec<u8>>,
}

/// Derive the snapshot-relative path from an absolute entry path.
/// Uses `abs_source` (the canonicalized walk root) for `strip_prefix`.
pub(super) fn rel_path_from_abs(abs_source: &Path, abs_path: &Path) -> String {
    let rel = fs::strip_path_prefix(abs_path, abs_source)
        .unwrap_or(abs_path)
        .to_string_lossy()
        .to_string();
    crate::commands::backup::normalize_rel_path(rel)
}

/// Byte-faithful counterpart to [`rel_path_from_abs`]: returns the raw relative
/// path bytes when the relative path is not valid UTF-8 (Unix only), else
/// `None`. `normalize_rel_path` is a no-op on Unix so no separator rewrite is
/// needed. Delegates the Unix-only byte extraction to the shared
/// [`crate::platform::fs::non_utf8_bytes`] helper.
pub(super) fn rel_raw_from_abs(abs_source: &Path, abs_path: &Path) -> Option<Vec<u8>> {
    let rel = fs::strip_path_prefix(abs_path, abs_source).unwrap_or(abs_path);
    crate::platform::fs::non_utf8_bytes(rel)
}

/// Combine the snapshot-root prefix (per `policy`/`prefix_raw`) with a relative
/// path into the display string and its byte-faithful shadow. The combined path
/// is byte-faithful whenever *either* the `EmitRoot` prefix or the relative path
/// is not valid UTF-8 — built from the canonical bytes of each part (raw if
/// present, else its UTF-8 bytes) joined by `/`. `SkipRoot` passes the relative
/// path through unchanged.
fn join_snapshot_path(
    policy: &RootEmission,
    prefix_raw: Option<&[u8]>,
    rel: String,
    rel_raw: Option<Vec<u8>>,
) -> (String, Option<Vec<u8>>) {
    match policy {
        RootEmission::SkipRoot => (rel, rel_raw),
        RootEmission::EmitRoot { prefix } => {
            let display = format!("{prefix}/{rel}");
            let raw = if prefix_raw.is_some() || rel_raw.is_some() {
                let mut v = Vec::new();
                match prefix_raw {
                    Some(p) => v.extend_from_slice(p),
                    None => v.extend_from_slice(prefix.as_bytes()),
                }
                v.push(b'/');
                match rel_raw.as_deref() {
                    Some(r) => v.extend_from_slice(r),
                    None => v.extend_from_slice(rel.as_bytes()),
                }
                Some(v)
            } else {
                None
            };
            (display, raw)
        }
    }
}

/// Events yielded by `InodeSortedWalk`.
pub(in crate::commands::backup) enum WalkEvent {
    Entry(WalkedEntry),
    /// A soft error occurred (permission denied, not found, EIO, or a
    /// Windows-specific unsupported-reparse / cloud-file / locked-file
    /// (sharing or lock violation) failure). Carries
    /// the failing path and a pre-formatted reason so consumers can surface
    /// a path-bearing warning rather than just bumping an opaque counter.
    Skipped {
        path: PathBuf,
        reason: String,
    },
    /// A cloud-only (macOS `FileProvider`) directory whose listing could not be
    /// materialized in this process context. Its whole subtree is absent from
    /// the snapshot, but this is a tallied omission rather than an error — see
    /// [`classify_dir_skip`].
    SkippedDataless {
        path: PathBuf,
        kind: DatalessKind,
    },
}

/// Returns `true` if `dir` is a macOS `FileProvider` placeholder (dataless).
/// A failed `lstat` answers "not dataless", which routes the caller to the
/// visible soft-error channel — the conservative direction.
fn dir_is_dataless(dir: &Path) -> bool {
    match std::fs::symlink_metadata(dir) {
        Ok(m) => {
            let file_type = m.file_type();
            fs::summarize_metadata(&m, &file_type).is_dataless
        }
        Err(_) => false,
    }
}

/// Build the skip event for a directory whose listing failed with a soft I/O
/// error.
///
/// The extra `lstat` fires only when the error is a `FileProvider` deadlock, so
/// it costs nothing on the ordinary `EACCES` path — and even then only on a
/// path that has already failed.
fn dir_skip_event(dir: &Path, e: &std::io::Error, reason: String) -> WalkEvent {
    let provider_deadlock = is_fileprovider_deadlock(e);
    // Short-circuits: no stat unless the errno already points at FileProvider.
    let is_dataless = provider_deadlock && dir_is_dataless(dir);
    match classify_dir_skip(provider_deadlock, is_dataless) {
        DirSkipKind::Dataless => {
            debug!(
                path = %dir.display(),
                "cloud-only directory could not be listed; subtree omitted"
            );
            WalkEvent::SkippedDataless {
                path: dir.to_path_buf(),
                kind: DatalessKind::Directory,
            }
        }
        DirSkipKind::SoftIo => WalkEvent::Skipped {
            path: dir.to_path_buf(),
            reason,
        },
    }
}

/// Build a single-event `DirLevel` holding the root `WalkedEntry` for an
/// `EmitRoot` source.
fn root_entry_level(
    abs_source: &Path,
    file_type: FileType,
    metadata: MetadataSummary,
    prefix: &str,
    prefix_raw: Option<&[u8]>,
) -> DirLevel {
    let root_entry = WalkedEntry {
        abs_path: abs_source.to_path_buf(),
        metadata,
        file_type,
        snapshot_path: prefix.to_string(),
        snapshot_path_raw: prefix_raw.map(|b| b.to_vec()),
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
/// the inode table on ext4/xfs. With [`EmitOrder::Name`] (every backup) they
/// are re-sorted by name after stat and emitted in that order.
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
    emit_order: EmitOrder,
    /// Snapshot-root policy: `SkipRoot` (descendants only, relative to
    /// `abs_source`) or `EmitRoot` (prefix all emitted paths with `prefix`).
    policy: RootEmission,
    /// Byte-faithful form of the `EmitRoot` prefix when it is not valid UTF-8
    /// (Unix only). `None` for `SkipRoot` or a UTF-8 prefix.
    prefix_raw: Option<Vec<u8>>,
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
        Self::new_inner(
            source,
            exclude_patterns,
            exclude_if_present,
            one_file_system,
            git_ignore,
            EmitOrder::Name,
            None,
        )
    }

    /// [`Self::new`] with an explicit emission order (`Stat` is for
    /// measurement and tests only) and, when
    /// `inode_sort_override` is `Some`, a forced inode-sort decision instead
    /// of `inode_sort_beneficial`.
    pub(in crate::commands::backup) fn new_inner(
        source: &ResolvedSource,
        exclude_patterns: &[String],
        exclude_if_present: &[String],
        one_file_system: bool,
        git_ignore: bool,
        emit_order: EmitOrder,
        inode_sort_override: Option<bool>,
    ) -> Result<Self> {
        let abs_source = source.abs_source.clone();

        let source_meta = std::fs::metadata(&abs_source).map_err(|e| {
            VykarError::Other(format!("stat error for {}: {e}", abs_source.display()))
        })?;
        let source_ft = source_meta.file_type();
        let source_summary = fs::summarize_metadata(&source_meta, &source_ft);
        let source_dev = source_summary.device;

        let excludes = build_explicit_excludes(&abs_source, exclude_patterns)?;

        let inode_sort_for_source =
            inode_sort_override.unwrap_or_else(|| inode_sort_beneficial(&abs_source));

        tracing::debug!(
            inode_sort = inode_sort_for_source,
            emit_order = ?emit_order,
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
            emit_order,
            policy: source.policy.clone(),
            prefix_raw: source.prefix_raw.clone(),
        };

        // Build the emission order: descendants-first (bottom of stack) so
        // DFS drains the root event on top before descending.
        match (&walk.policy.clone(), source.kind) {
            (RootEmission::SkipRoot, _) => {
                let root_level = walk.build_dir_level(&walk.abs_source)?;
                walk.stack.push(root_level);
            }
            (RootEmission::EmitRoot { prefix }, SourceKind::Directory) => {
                let descendants = walk.build_dir_level(&walk.abs_source)?;
                walk.stack.push(descendants);
                walk.stack.push(root_entry_level(
                    &walk.abs_source,
                    source_ft,
                    source_summary,
                    prefix,
                    source.prefix_raw.as_deref(),
                ));
            }
            (RootEmission::EmitRoot { prefix }, SourceKind::File) => {
                // Never call read_dir on a file source. This is the regression fix.
                walk.stack.push(root_entry_level(
                    &walk.abs_source,
                    source_ft,
                    source_summary,
                    prefix,
                    source.prefix_raw.as_deref(),
                ));
            }
        }

        Ok(walk)
    }

    /// Whether stat runs in inode order for this source.
    pub(in crate::commands::backup) fn inode_sort(&self) -> bool {
        self.inode_sort_for_source
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

    /// Combine the snapshot-root prefix (if any) with a relative path, in both
    /// display and byte-faithful forms. Delegates to the pure
    /// [`join_snapshot_path`] free function so the joining logic is unit-testable
    /// without constructing a walker.
    fn combined_snapshot_path(
        &self,
        rel: String,
        rel_raw: Option<Vec<u8>>,
    ) -> (String, Option<Vec<u8>>) {
        join_snapshot_path(&self.policy, self.prefix_raw.as_deref(), rel, rel_raw)
    }

    /// Build a DirLevel for the given directory path.
    fn build_dir_level(&self, dir: &Path) -> Result<DirLevel> {
        let read_dir = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) => {
                if is_soft_backup_io_error(&e) {
                    return Ok(DirLevel {
                        events: VecDeque::from([dir_skip_event(
                            dir,
                            &e,
                            format!("read_dir failed: {e}"),
                        )]),
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
        // child — we report the parent directory in each skip event instead.
        let mut deferred_skips: Vec<WalkEvent> = Vec::new();
        for entry_result in read_dir {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    if is_soft_backup_io_error(&e) {
                        deferred_skips.push(dir_skip_event(
                            dir,
                            &e,
                            format!("readdir failed: {e}"),
                        ));
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
            let rel = match fs::strip_path_prefix(&raw.path, &self.abs_source) {
                Some(r) => r,
                None => return false,
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
            // Other filesystems: filename order for stat (emission order is
            // set in phase 5).
            raw_entries.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        }

        // Phase 4: Stat in inode order and post-stat filter.
        let mut events: VecDeque<WalkEvent> = VecDeque::new();

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
                let rel = match fs::strip_path_prefix(&raw.path, &self.abs_source) {
                    Some(r) => r,
                    None => continue,
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

            let rel = rel_path_from_abs(&self.abs_source, &raw.path);
            let rel_raw = rel_raw_from_abs(&self.abs_source, &raw.path);
            let (snapshot_path, snapshot_path_raw) = self.combined_snapshot_path(rel, rel_raw);

            events.push_back(WalkEvent::Entry(WalkedEntry {
                abs_path: raw.path,
                metadata: summary,
                file_type,
                snapshot_path,
                snapshot_path_raw,
            }));
        }

        // Phase 5: name order for emission (06 order 1). On inode-sorted
        // filesystems this re-sorts the stat order; elsewhere the events are
        // already in this order and the sort is a linear pass.
        if self.emit_order == EmitOrder::Name {
            events.make_contiguous().sort_unstable_by(emit_cmp);
        }

        // Subdirectories are descended in the order their entries are emitted,
        // which is what makes every subtree follow its directory's entries in
        // the same order. `file_type` is from `symlink_metadata`, so a
        // symlink to a directory is not descended.
        let pending_subdirs: VecDeque<PathBuf> = events
            .iter()
            .filter_map(|ev| match ev {
                WalkEvent::Entry(e) if e.file_type.is_dir() => Some(e.abs_path.clone()),
                _ => None,
            })
            .collect();

        // Prepend skip events for readdir errors from phase 1 so consumers can
        // account for each lost entry. `push_front` reverses the deferred order,
        // so iterate the deferred list in reverse to keep the original order
        // intact.
        for event in deferred_skips.into_iter().rev() {
            events.push_front(event);
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
    use std::collections::HashMap;
    use std::fs;

    fn resolve_dir(path: &Path) -> ResolvedSource {
        ResolvedSource::resolve(&path.to_string_lossy(), false).unwrap()
    }

    fn emit_root(prefix: &str) -> RootEmission {
        RootEmission::EmitRoot {
            prefix: prefix.to_string(),
        }
    }

    #[test]
    fn join_snapshot_path_skiproot_passes_through() {
        let (d, r) = join_snapshot_path(&RootEmission::SkipRoot, None, "a/b.txt".into(), None);
        assert_eq!(d, "a/b.txt");
        assert_eq!(r, None);
        // SkipRoot preserves an existing raw shadow verbatim.
        let (_d, r) = join_snapshot_path(
            &RootEmission::SkipRoot,
            None,
            "x".into(),
            Some(b"\x80x".to_vec()),
        );
        assert_eq!(r, Some(b"\x80x".to_vec()));
    }

    #[test]
    fn join_snapshot_path_emitroot_all_utf8_has_no_raw() {
        let (d, r) = join_snapshot_path(&emit_root("pre"), None, "file.txt".into(), None);
        assert_eq!(d, "pre/file.txt");
        assert_eq!(r, None, "neither part is raw → no byte shadow");
    }

    /// The combined path must be byte-faithful when *only the prefix* is raw,
    /// joining the raw prefix bytes with the UTF-8 relative bytes.
    #[test]
    fn join_snapshot_path_raw_prefix_only() {
        let prefix_raw = b"\x80pre";
        let display_prefix = String::from_utf8_lossy(prefix_raw).into_owned();
        let (d, r) = join_snapshot_path(
            &emit_root(&display_prefix),
            Some(prefix_raw),
            "file.txt".into(),
            None,
        );
        assert_eq!(d, format!("{display_prefix}/file.txt"));
        assert_eq!(r, Some(b"\x80pre/file.txt".to_vec()));
    }

    /// And when *only the relative path* is raw, joining the UTF-8 prefix bytes
    /// with the raw relative bytes.
    #[test]
    fn join_snapshot_path_raw_rel_only() {
        let (d, r) = join_snapshot_path(
            &emit_root("pre"),
            None,
            String::from_utf8_lossy(b"\x80f").into_owned(),
            Some(b"\x80f".to_vec()),
        );
        assert_eq!(d, format!("pre/{}", String::from_utf8_lossy(b"\x80f")));
        assert_eq!(r, Some(b"pre/\x80f".to_vec()));
    }

    /// When both parts are raw, both byte shadows are joined.
    #[test]
    fn join_snapshot_path_both_raw() {
        let (_d, r) = join_snapshot_path(
            &emit_root(&String::from_utf8_lossy(b"\x80p")),
            Some(b"\x80p"),
            String::from_utf8_lossy(b"\x80f").into_owned(),
            Some(b"\x80f".to_vec()),
        );
        assert_eq!(r, Some(b"\x80p/\x80f".to_vec()));
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
            if let Ok(WalkEvent::Entry(e)) = event
                && rel_path_from_abs(&abs_source, &e.abs_path) == "link.txt"
            {
                assert!(e.file_type.is_symlink());
                found_symlink = true;
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

        // Root entry emitted first; under the new multi-path prefix scheme,
        // the prefix is the full absolute configured path with leading `/`
        // stripped (e.g. `tmp/.../data`).
        let root_prefix = entries[0].snapshot_path.clone();
        assert!(root_prefix.ends_with("/data"), "got: {root_prefix}");
        assert!(entries[0].file_type.is_dir());
        // Walker's fresh stat at init picks up the post-resolve mutation.
        assert_eq!(entries[0].metadata.mode & 0o777, 0o751);

        // Descendants prefixed with `<root_prefix>/`.
        let descendant_paths: Vec<String> = entries[1..]
            .iter()
            .map(|e| e.snapshot_path.clone())
            .collect();
        assert_eq!(descendant_paths, vec![format!("{root_prefix}/a.txt")]);
    }

    /// Root files `z.txt`..`a.txt`, subdirs `d3, d1, d2` and `d1/n2, d1/n1`,
    /// each created in reverse name order so inode order is not name order.
    fn make_reverse_tree(root: &Path) {
        for c in (b'a'..=b'z').rev() {
            fs::write(root.join(format!("{}.txt", c as char)), "x").unwrap();
        }
        for d in ["d3", "d1", "d2"] {
            fs::create_dir(root.join(d)).unwrap();
            for f in (1..=5).rev() {
                fs::write(root.join(format!("{d}/f{f}")), "x").unwrap();
            }
        }
        for n in ["n2", "n1"] {
            fs::create_dir(root.join("d1").join(n)).unwrap();
            fs::write(root.join(format!("d1/{n}/g")), "x").unwrap();
        }
    }

    fn walk_paths(root: &Path, order: EmitOrder, inode_sort: bool) -> Vec<String> {
        InodeSortedWalk::new_inner(
            &resolve_dir(root),
            &[],
            &[],
            false,
            false,
            order,
            Some(inode_sort),
        )
        .unwrap()
        .map(|ev| match ev.unwrap() {
            WalkEvent::Entry(e) => e.snapshot_path,
            _ => panic!("unexpected skip event"),
        })
        .collect()
    }

    fn parent_and_name(path: &str) -> (&str, &str) {
        path.rsplit_once('/').unwrap_or(("", path))
    }

    #[cfg(unix)]
    #[test]
    fn name_emission_matches_name_sort_under_inode_stat() {
        let tmp = tempfile::tempdir().unwrap();
        make_reverse_tree(tmp.path());

        let name_inode = walk_paths(tmp.path(), EmitOrder::Name, true);
        let name_plain = walk_paths(tmp.path(), EmitOrder::Stat, false);
        assert_eq!(name_inode, name_plain);

        let stat_inode = walk_paths(tmp.path(), EmitOrder::Stat, true);
        assert_ne!(
            stat_inode, name_inode,
            "precondition does not hold: this filesystem hands out inodes in name order"
        );

        let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
        for p in &name_inode {
            let (parent, name) = parent_and_name(p);
            children.entry(parent).or_default().push(name);
        }
        for (parent, names) in &children {
            let mut sorted = names.clone();
            sorted.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            assert_eq!(names, &sorted, "children of {parent:?} not in name order");
        }
    }

    #[cfg(unix)]
    #[test]
    fn stat_emission_keeps_inode_order() {
        use std::os::unix::fs::MetadataExt;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        make_reverse_tree(root);

        let paths = walk_paths(root, EmitOrder::Stat, true);
        let root_children: Vec<&str> = paths
            .iter()
            .map(String::as_str)
            .filter(|p| !p.contains('/'))
            .collect();
        let mut by_ino = root_children.clone();
        by_ino.sort_by_key(|p| fs::symlink_metadata(root.join(p)).unwrap().ino());
        assert_eq!(root_children, by_ino);

        let dirs: Vec<&str> = root_children
            .iter()
            .copied()
            .filter(|p| root.join(p).is_dir())
            .collect();
        let mut descent: Vec<&str> = Vec::new();
        for p in &paths {
            if let Some((top, _)) = p.split_once('/')
                && descent.last() != Some(&top)
            {
                descent.push(top);
            }
        }
        assert_eq!(descent, dirs);
    }

    #[test]
    fn name_emission_noop_without_inode_sort() {
        let tmp = tempfile::tempdir().unwrap();
        make_reverse_tree(tmp.path());
        assert_eq!(
            walk_paths(tmp.path(), EmitOrder::Name, false),
            walk_paths(tmp.path(), EmitOrder::Stat, false)
        );
    }

    /// 06 order 1: (parent components, name), components compared as bytes.
    fn dfs_name_key(p: &[u8]) -> (Vec<&[u8]>, &[u8]) {
        let mut c: Vec<&[u8]> = p.split(|b| *b == b'/').collect();
        let name = c.pop().unwrap();
        (c, name)
    }

    fn entry_path_bytes(e: &WalkedEntry) -> Vec<u8> {
        e.snapshot_path_raw
            .clone()
            .unwrap_or_else(|| e.snapshot_path.as_bytes().to_vec())
    }

    fn walk_path_bytes(source: &ResolvedSource, inode_sort: bool) -> Vec<Vec<u8>> {
        InodeSortedWalk::new_inner(
            source,
            &[],
            &[],
            false,
            false,
            EmitOrder::Name,
            Some(inode_sort),
        )
        .unwrap()
        .map(|ev| match ev.unwrap() {
            WalkEvent::Entry(e) => entry_path_bytes(&e),
            _ => panic!("unexpected skip event"),
        })
        .collect()
    }

    fn position(paths: &[Vec<u8>], p: &[u8]) -> usize {
        paths
            .iter()
            .position(|q| q == p)
            .unwrap_or_else(|| panic!("{:?} not walked", String::from_utf8_lossy(p)))
    }

    /// Names whose dfs-name order differs from the byte order of the joined
    /// path (`a.b` vs `a/x`), case and non-ASCII siblings, and on Unix
    /// non-UTF-8 names whose raw bytes sort differently from their lossy
    /// display form. Everything is created in reverse name order so inode
    /// order is not name order. `B` is left out where the
    /// filesystem does not distinguish case (default APFS, NTFS); `C` keeps
    /// an uppercase-before-lowercase sibling everywhere.
    fn make_awkward_tree(root: &Path) -> AwkwardTree {
        let mut created: Vec<Vec<u8>> = Vec::new();
        let mut mixed_case = true;
        let files = ["z", "é", "b", "B", "C", "ab", "a b", "a-b"];
        for name in files {
            if name == "B" && root.join(name).symlink_metadata().is_ok() {
                // `b` already answers to `B`: case-insensitive.
                mixed_case = false;
                continue;
            }
            fs::write(root.join(name), "x").unwrap();
            created.push(name.as_bytes().to_vec());
        }
        for dir in ["a.b", "a"] {
            fs::create_dir(root.join(dir)).unwrap();
            created.push(dir.as_bytes().to_vec());
        }
        fs::write(root.join("a.b/x"), "x").unwrap();
        fs::write(root.join("a/y"), "x").unwrap();
        fs::create_dir(root.join("a/x")).unwrap();
        fs::write(root.join("a/x/deep"), "x").unwrap();
        for p in ["a.b/x", "a/y", "a/x", "a/x/deep"] {
            created.push(p.as_bytes().to_vec());
        }

        #[cfg(unix)]
        let raw = {
            use std::ffi::OsStr;
            use std::os::unix::ffi::OsStrExt;
            // Some filesystems (APFS) refuse non-UTF-8 names; then only the
            // UTF-8 names below are tested.
            let raw = crate::testutil::try_raw_name(&root.join(OsStr::from_bytes(b"\xff")), |p| {
                fs::write(p, "x")
            });
            fs::write(root.join("\u{10000}"), "x").unwrap();
            created.push("\u{10000}".as_bytes().to_vec());
            fs::write(root.join("né"), "x").unwrap();
            created.push("né".as_bytes().to_vec());
            if raw {
                created.push(b"\xff".to_vec());
                let d = root.join(OsStr::from_bytes(b"n\x80"));
                fs::create_dir(&d).unwrap();
                created.push(b"n\x80".to_vec());
                fs::write(d.join("c"), "x").unwrap();
                created.push(b"n\x80/c".to_vec());
            }
            raw
        };
        #[cfg(not(unix))]
        let raw = false;
        AwkwardTree {
            created,
            raw,
            mixed_case,
        }
    }

    struct AwkwardTree {
        created: Vec<Vec<u8>>,
        /// The non-UTF-8 names were created.
        raw: bool,
        /// Both `b` and `B` were created.
        mixed_case: bool,
    }

    fn assert_dfs_name_order(paths: &[Vec<u8>]) {
        for w in paths.windows(2) {
            assert!(
                dfs_name_key(&w[0]) < dfs_name_key(&w[1]),
                "not in dfs-name order: {:?} then {:?}",
                String::from_utf8_lossy(&w[0]),
                String::from_utf8_lossy(&w[1]),
            );
        }
    }

    #[test]
    fn name_emission_is_dfs_name_order() {
        let tmp = tempfile::tempdir().unwrap();
        let tree = make_awkward_tree(tmp.path());
        let mut expected = tree.created;
        expected.sort_by(|a, b| dfs_name_key(a).cmp(&dfs_name_key(b)));

        let source = resolve_dir(tmp.path());
        let with_inode = walk_path_bytes(&source, true);
        let without = walk_path_bytes(&source, false);
        assert_eq!(with_inode, expected);
        assert_eq!(without, expected);
        assert_dfs_name_order(&with_inode);

        // Not the byte order of the joined paths: `a.b` (a child of the
        // root) before `a/x`, and the whole `a/` subtree before `a.b/`'s.
        let p = |s: &[u8]| position(&with_inode, s);
        assert!(p(b"a.b") < p(b"a/x"));
        assert!(p(b"a/x/deep") < p(b"a.b/x"));
        assert!(p(b"C") < p(b"a") && p(b"ab") < p(b"b"));
        if tree.mixed_case {
            assert!(p(b"B") < p(b"C"));
        }
        if tree.raw {
            // Raw bytes, not the lossy display form, decide the order.
            assert!(p(b"n\x80") < p("né".as_bytes()));
            assert!(p("\u{10000}".as_bytes()) < p(b"\xff"));
        }
    }

    #[test]
    fn name_emission_is_dfs_name_order_under_emit_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("data");
        fs::create_dir(&root).unwrap();
        let created = make_awkward_tree(&root).created;

        let source = ResolvedSource::resolve(&root.to_string_lossy(), true).unwrap();
        let paths = walk_path_bytes(&source, true);
        assert_eq!(paths, walk_path_bytes(&source, false));
        assert_eq!(paths.len(), created.len() + 1);
        let prefix = paths[0].clone();
        assert!(prefix.ends_with(b"/data"), "root entry first");
        let mut with_slash = prefix.clone();
        with_slash.push(b'/');
        assert!(paths[1..].iter().all(|p| p.starts_with(&with_slash)));
        assert_dfs_name_order(&paths);
    }
}
