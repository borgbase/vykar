//! `vykar restore`: reconstruct files from a snapshot.
//!
//! The implementation is split into a set of focused submodules; this file
//! is the orchestrator that wires them together and exposes the public API.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use tracing::info;

use crate::config::VykarConfig;
use crate::platform::fs;
use vykar_common::display::{format_bytes, format_count};
use vykar_types::error::{Result, VykarError};

mod finalize;
mod parallel;
mod plan;
mod read_groups;
#[cfg(test)]
mod test_support;

// ---------------------------------------------------------------------------
// Constants for coalesced parallel restore
// ---------------------------------------------------------------------------

/// Maximum gap (in bytes) between blobs in the same pack that will be coalesced
/// into a single range read rather than issuing separate requests.
pub(super) const MAX_COALESCE_GAP: u64 = 256 * 1024; // 256 KiB

/// Maximum size of a single coalesced range read.
pub(super) const MAX_READ_SIZE: u64 = 16 * 1024 * 1024; // 16 MiB

// MAX_READER_THREADS removed — uses config.limits.restore_concurrency() instead.

/// Maximum number of simultaneously open output files per restore worker.
/// Caps fd usage while still avoiding per-chunk open/close churn.
pub(super) const MAX_OPEN_FILES_PER_GROUP: usize = 16;

/// Maximum size (in bytes) of the write accumulator before flushing.
/// Batching consecutive same-file writes into a single `pwrite64` reduces
/// syscall count and inode rwsem contention.
pub(super) const MAX_WRITE_BATCH: usize = 1024 * 1024; // 1 MiB

/// Maximum number of regular-file `PlannedFile` entries kept in memory at
/// once during a restore.  Each batch goes through plan → read groups →
/// write → metadata, then is dropped before the next batch begins.  This
/// keeps peak `PlannedFile` memory bounded regardless of snapshot size — a
/// 10M-file snapshot no longer requires gigabytes of plan state up front,
/// and a malicious snapshot cannot DoS the host before any data is read.
const RESTORE_BATCH_FILES: usize = 100_000;

/// Cap on non-fatal metadata warnings retained in `RestoreStats.warnings`.
/// Failures past this cap are counted in `warnings_suppressed` only — this
/// prevents an unbounded `Vec` (and matching unbounded `tracing::warn!`
/// stream) when the destination filesystem rejects every metadata call.
const MAX_RESTORE_WARNINGS: usize = 64;

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct RestoreStats {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    pub total_bytes: u64,
    pub warnings: Vec<String>,
    pub warnings_suppressed: u64,
}

/// Run `vykar restore`.
pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    pattern: Option<&str>,
    xattrs_enabled: bool,
) -> Result<RestoreStats> {
    let filter = pattern
        .map(|p| {
            globset::GlobBuilder::new(p)
                .literal_separator(false)
                .build()
                .map(|g| g.compile_matcher())
        })
        .transpose()
        .map_err(|e| VykarError::Config(format!("invalid pattern: {e}")))?;

    restore_with_filter(
        config,
        passphrase,
        snapshot_name,
        dest,
        xattrs_enabled,
        move |path| filter.as_ref().is_none_or(|matcher| matcher.is_match(path)),
    )
}

/// Run `vykar restore` for a selected set of paths.
///
/// An item is included if its path exactly matches an entry in `selected_paths`,
/// or if any prefix of its path matches (i.e. a parent directory was selected).
pub fn run_selected(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    selected_paths: &HashSet<String>,
    xattrs_enabled: bool,
) -> Result<RestoreStats> {
    restore_with_filter(
        config,
        passphrase,
        snapshot_name,
        dest,
        xattrs_enabled,
        |path| path_matches_selection(path, selected_paths),
    )
}

// ---------------------------------------------------------------------------
// Core restore logic — phased approach
// ---------------------------------------------------------------------------

fn restore_with_filter<F>(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    xattrs_enabled: bool,
    mut include_path: F,
) -> Result<RestoreStats>
where
    F: FnMut(&str) -> bool,
{
    let (mut repo, _session_guard) = super::util::open_repo_with_read_session(
        config,
        passphrase,
        crate::repo::OpenOptions::new(),
    )?;
    // Shrink blob cache for restore — the parallel pipeline reads pack data
    // directly via storage.get_range(), so the cache only serves the small
    // item-stream tree-pack chunks. 2 MiB is plenty.
    repo.set_blob_cache_max_bytes(2 * 1024 * 1024);
    let mut stats = RestoreStats::default();
    let xattrs_enabled = if xattrs_enabled && !fs::xattrs_supported() {
        push_metadata_warning(
            &mut stats,
            "xattrs requested but not supported on this platform; continuing without xattrs"
                .to_string(),
        );
        false
    } else {
        xattrs_enabled
    };

    // Try to open the mmap restore cache before loading the index.
    let restore_cache = repo.open_restore_cache();

    // Resolve "latest" or exact snapshot name
    let resolved_name = repo
        .manifest()
        .resolve_snapshot(snapshot_name)?
        .name
        .clone();
    if resolved_name != snapshot_name {
        info!("Resolved '{}' to snapshot {}", snapshot_name, resolved_name);
    }

    // Load raw item stream bytes (not decoded Items).  When the restore cache
    // is available, read tree-pack chunks via the cache to avoid loading the
    // full chunk index.
    let items_stream = if let Some(ref cache) = restore_cache {
        match super::list::load_snapshot_item_stream_via_lookup(&mut repo, &resolved_name, |id| {
            cache.lookup(id)
        }) {
            Ok(stream) => stream,
            Err(_) => {
                info!("restore cache incomplete or stale, falling back to full index");
                repo.load_chunk_index()?;
                super::list::load_snapshot_item_stream(&mut repo, &resolved_name)?
            }
        }
    } else {
        repo.load_chunk_index()?;
        super::list::load_snapshot_item_stream(&mut repo, &resolved_name)?
    };

    let dest_root = plan::validate_and_prepare_dest(dest)?;

    // Create a hidden temp directory inside dest so all writes happen there.
    // On success we rename top-level entries into dest; on failure we
    // remove_dir_all the temp root so no partial files are left at dest.
    let temp_dir_name = format!(
        ".vykar-restore-{:016x}",
        rand::Rng::random::<u64>(&mut rand::rng())
    );
    let temp_root = dest_root.join(&temp_dir_name);
    std::fs::create_dir_all(&temp_root)?;

    let cleanup = |e: VykarError| -> VykarError {
        let _ = std::fs::remove_dir_all(&temp_root);
        e
    };

    // Stream items in bounded batches.  Each batch goes through plan → read
    // groups → write → metadata, then is dropped before the next batch
    // begins, keeping peak memory bounded by `RESTORE_BATCH_FILES`.
    // Per-batch metadata application happens inside `temp_root` before the
    // final rename, so the inode already carries mode/mtime/xattrs by the
    // time it lands at `dest_root` — no path-based reopen window.
    let mut total_bytes: u64 = 0;
    let mut total_files: u64 = 0;
    plan::stream_and_plan(
        &items_stream,
        &temp_root,
        &mut include_path,
        xattrs_enabled,
        &mut stats,
        RESTORE_BATCH_FILES,
        |batch_files, batch_chunks, verified_dirs, stats| -> Result<()> {
            if batch_files.is_empty() {
                return Ok(());
            }

            let mut groups = if !batch_chunks.is_empty() {
                if repo.chunk_index().is_empty() {
                    repo.load_chunk_index()?;
                }
                let index = repo.chunk_index();
                read_groups::build_read_groups(batch_chunks, |id| {
                    index
                        .get(id)
                        .map(|e| (e.pack_id, e.pack_offset, e.stored_size))
                })?
            } else {
                Vec::new()
            };

            // Sort groups by pack ID (shard-aligned) then offset for sequential I/O.
            groups.sort_by(|a, b| {
                a.pack_id
                    .0
                    .cmp(&b.pack_id.0)
                    .then(a.read_start.cmp(&b.read_start))
            });

            tracing::debug!(
                "batch: {} coalesced read groups for {} files",
                groups.len(),
                batch_files.len(),
            );

            // Phase 3: Ensure parent directories exist + create empty files.
            // Non-empty files are created on first write in Phase 4 (avoids
            // the double-open: create + set_len here, then reopen for writing).
            // Safety: parents verified during directory creation are trusted —
            // this is a single-process operation so no concurrent destination
            // tampering can occur. Unverified parents still get the full
            // canonicalize check.
            for pf in &batch_files {
                let full_path = temp_root.join(&pf.rel_path);
                if full_path
                    .parent()
                    .is_none_or(|p| !verified_dirs.contains(p))
                {
                    plan::ensure_parent_exists_within_root(&full_path, &temp_root)?;
                }
                if pf.total_size == 0 {
                    std::fs::File::create(&full_path)?;
                }
            }

            // Phase 4: Parallel restore — download ranges, decrypt, decompress, write.
            let bytes = parallel::execute_parallel_restore(
                &batch_files,
                groups,
                &repo.storage,
                repo.crypto.as_ref(),
                &temp_root,
                config.limits.restore_concurrency(),
            )?;

            // Phase 5b: apply per-file metadata in temp_root.
            finalize::apply_file_metadata(&batch_files, &temp_root, xattrs_enabled, stats);

            total_bytes += bytes;
            total_files += batch_files.len() as u64;
            Ok(())
        },
    )
    .map_err(&cleanup)?;

    drop(items_stream);
    repo.clear_chunk_index();

    // Phase 5a: rename temp subtrees into the final destination.  At this
    // point all file metadata is already on the inodes, so the rename has
    // no observable TOCTOU window.
    finalize::move_temp_to_dest(&temp_root, &dest_root).map_err(&cleanup)?;

    stats.files = total_files;
    stats.total_bytes = total_bytes;

    info!(
        "Restored {} files, {} dirs, {} symlinks ({})",
        format_count(stats.files),
        format_count(stats.dirs),
        format_count(stats.symlinks),
        format_bytes(stats.total_bytes)
    );

    if stats.warnings_suppressed > 0 {
        tracing::warn!(
            "{} additional metadata warnings suppressed",
            stats.warnings_suppressed
        );
    }

    Ok(stats)
}

// ---------------------------------------------------------------------------
// Helpers shared across submodules
// ---------------------------------------------------------------------------

/// Check if a path matches the selection set.
/// A path matches if it's exactly in the set, or any prefix (ancestor) is in the set.
fn path_matches_selection(path: &str, selected: &HashSet<String>) -> bool {
    if selected.contains(path) {
        return true;
    }
    // Check if any ancestor is selected
    let mut current = path;
    while let Some(slash_idx) = current.rfind('/') {
        current = &current[..slash_idx];
        if selected.contains(current) {
            return true;
        }
    }
    false
}

/// Record a non-fatal metadata failure. Up to `MAX_RESTORE_WARNINGS` are
/// both logged via `tracing::warn!` and stored in `stats.warnings`; beyond
/// that, only `stats.warnings_suppressed` is incremented.
pub(super) fn push_metadata_warning(stats: &mut RestoreStats, msg: String) {
    if stats.warnings.len() < MAX_RESTORE_WARNINGS {
        tracing::warn!("{msg}");
        stats.warnings.push(msg);
    } else {
        stats.warnings_suppressed += 1;
    }
}

pub(super) fn apply_item_xattrs(
    target: &Path,
    xattrs: Option<&HashMap<String, Vec<u8>>>,
    stats: &mut RestoreStats,
) {
    let Some(xattrs) = xattrs else {
        return;
    };

    let mut names: Vec<&str> = xattrs.keys().map(String::as_str).collect();
    names.sort_unstable();

    for name in names {
        let Some(value) = xattrs.get(name) else {
            continue;
        };

        #[cfg(unix)]
        if let Err(e) = xattr::set(target, name, value) {
            push_metadata_warning(
                stats,
                format!("failed to apply xattr {name} on {}: {e}", target.display()),
            );
        }
        #[cfg(not(unix))]
        {
            let _ = target;
            let _ = name;
            let _ = value;
            let _ = stats;
        }
    }
}

/// If `result` is `Err`, record a metadata warning describing the failure.
pub(super) fn warn_metadata_err<T>(
    stats: &mut RestoreStats,
    result: std::io::Result<T>,
    path: &Path,
    op: &str,
) {
    if let Err(e) = result {
        push_metadata_warning(
            stats,
            format!("failed to apply {op} on {}: {e}", path.display()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_matches_selection_ancestors() {
        let mut selected = HashSet::new();
        selected.insert("docs/notes".to_string());
        assert!(path_matches_selection("docs/notes/todo.txt", &selected));
        assert!(path_matches_selection("docs/notes", &selected));
        assert!(!path_matches_selection("docs/other", &selected));
        assert!(!path_matches_selection("documents", &selected));
    }

    #[test]
    fn push_metadata_warning_caps_vec_and_counts_suppressed() {
        let mut stats = RestoreStats::default();
        for i in 0..MAX_RESTORE_WARNINGS + 1 {
            push_metadata_warning(&mut stats, format!("msg {i}"));
        }
        assert_eq!(stats.warnings.len(), MAX_RESTORE_WARNINGS);
        assert_eq!(stats.warnings_suppressed, 1);
        // The 65th message is the one that should have been suppressed, not a
        // replacement of an earlier one.
        let suppressed = format!("msg {}", MAX_RESTORE_WARNINGS);
        assert!(!stats.warnings.iter().any(|w| w == &suppressed));
    }
}
