//! `vykar restore`: reconstruct files from a snapshot.
//!
//! The implementation is split into a set of focused submodules; this file
//! is the orchestrator that wires them together and exposes the public API.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
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

/// Scan a decoded item stream for entries carrying a byte-faithful `raw_names`
/// shadow (non-UTF8 names). Returns `Some((count, first_example_display_path))`
/// when any are present, else `None`.
///
/// Pure (no filesystem access), so the non-Unix preflight can call it *before*
/// any destination mutation — guaranteeing the abort leaves the filesystem
/// untouched. Compiled on non-Unix (where the preflight uses it) and in all
/// test builds (where it is unit-tested cross-platform).
#[cfg(any(not(unix), test))]
fn detect_raw_entries(items_stream: &[u8]) -> Result<Option<(usize, String)>> {
    let mut count = 0usize;
    let mut example: Option<String> = None;
    super::list::for_each_decoded_item(items_stream, |item| {
        if item.raw_names.is_some() {
            count += 1;
            if example.is_none() {
                example = Some(item.path.clone());
            }
        }
        Ok(())
    })?;
    Ok(example.map(|e| (count, e)))
}

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

/// Upper bound on the combined number of hard-link tracking entries
/// (`group_reps` + `pending_links`) held during a restore. `group_reps` is
/// inherently `O(distinct hardlinked inodes)` — a link may reference a
/// representative seen arbitrarily earlier, so the map must persist for the
/// whole stream. To keep a huge or malicious snapshot from exhausting memory,
/// tracking stops once this cap is reached: further hard-linked members are
/// materialized as independent files (always safe — every node carries its own
/// chunks), degrading only link-sharing, never correctness. Set to a multiple
/// of [`RESTORE_BATCH_FILES`].
pub(super) const MAX_HARDLINK_TRACKED: usize = 4 * RESTORE_BATCH_FILES;

/// Vykar-reserved prefix for the hidden directory restore stages writes into
/// (`{RESTORE_TEMP_PREFIX}{16 lowercase hex}`). A directory matching this exact
/// shape is owned by Vykar and is swept by `validate_and_prepare_dest` on the
/// next restore run; see [`is_reserved_temp_dir_name`].
pub(super) const RESTORE_TEMP_PREFIX: &str = ".vykar-restore-";

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
    /// True hard links created — extra names on a representative's inode
    /// (`hard_link(2)` succeeded). Disjoint from `files`: representatives count
    /// in `files`, their links count here.
    pub hardlinks: u64,
    /// Members that *could not* be hard-linked (e.g. `EMLINK`, or a filesystem
    /// without hard-link support) and were restored as independent copies of
    /// the representative's content — separate inodes, so they are counted as
    /// `files`, not `hardlinks`. Tracked separately only for transparency.
    pub hardlink_copies: u64,
    pub total_bytes: u64,
    pub warnings: Vec<String>,
    pub warnings_suppressed: u64,
}

/// Run `vykar restore`.
///
/// When `verify_chunks` is true, every restored chunk's plaintext is fed
/// through `ChunkId::compute` and matched against the snapshot's stored
/// `chunk_id`. AEAD already authenticates ciphertext under the standard
/// threat model, so this is defense-in-depth against writer-side bugs only.
pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    pattern: Option<&str>,
    xattrs_enabled: bool,
    verify_chunks: bool,
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
        verify_chunks,
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
    verify_chunks: bool,
) -> Result<RestoreStats> {
    restore_with_filter(
        config,
        passphrase,
        snapshot_name,
        dest,
        xattrs_enabled,
        verify_chunks,
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
    verify_chunks: bool,
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

    // Byte-faithful restore of non-UTF8 names is Unix-only. On other platforms
    // run a single read-only metadata pass over the loaded stream *before* any
    // filesystem mutation (dest prep / temp-root creation) and refuse if the
    // snapshot carries any raw (non-UTF8) entry. The pass deliberately does not
    // apply the user filter (the real streaming filter is a stateful FnMut that
    // cannot be safely re-invoked), so a non-Unix restore is refused if *any*
    // entry is non-UTF8, even one a filter would exclude — acceptable for this
    // extreme edge.
    #[cfg(not(unix))]
    {
        if let Some((raw_count, example)) = detect_raw_entries(&items_stream)? {
            return Err(VykarError::Other(format!(
                "snapshot contains {raw_count} entr{} with non-UTF8 name(s) \
                 (e.g. '{}') that cannot be restored byte-faithfully on this \
                 platform; restore on a Unix host",
                if raw_count == 1 { "y" } else { "ies" },
                example
            )));
        }
    }

    let dest_root = plan::validate_and_prepare_dest(dest)?;

    // Create a hidden temp directory inside dest so all writes happen there.
    // On success we rename top-level entries into dest; on failure we
    // remove_dir_all the temp root so no partial files are left at dest.
    let temp_dir_name = format!(
        "{}{:016x}",
        RESTORE_TEMP_PREFIX,
        rand::RngExt::random::<u64>(&mut rand::rng())
    );
    let temp_root = dest_root.join(&temp_dir_name);
    std::fs::create_dir_all(&temp_root)?;

    // Cleanup for failures *before* finalization (stream/plan/write errors).
    // `force_remove_temp_tree` (not `remove_dir_all`) so restrictive `000`
    // dirs applied during streaming can't make cleanup itself `EACCES`.
    // Finalization decides its own cleanup (rollback); see `move_temp_to_dest`.
    let cleanup = |e: VykarError| -> VykarError {
        let _ = finalize::force_remove_temp_tree(&temp_root);
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
    // Ownership is reassigned only under an effective-root restore (tar/restic
    // gate); non-root restores skip chown entirely (behavior unchanged).
    let restore_as_root = fs::is_effective_root();
    // Hard-link grouping state, owned here and threaded into the streaming
    // planner: `group_reps` maps each hard-link group to its materialized
    // representative; `pending_links` queues the non-representative members to
    // relink after all representatives are on disk (see `create_hardlinks`).
    let mut group_reps: HashMap<crate::snapshot::item::HardlinkId, plan::RepInfo> = HashMap::new();
    let mut pending_links: Vec<plan::PendingLink> = Vec::new();
    let mut plan_out = plan::stream_and_plan(
        &items_stream,
        &temp_root,
        &mut include_path,
        xattrs_enabled,
        &mut stats,
        RESTORE_BATCH_FILES,
        &mut group_reps,
        &mut pending_links,
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
                    .as_bytes()
                    .cmp(b.pack_id.as_bytes())
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
                verify_chunks,
            )?;

            // Phase 5b: apply per-file metadata + fsync each file in temp_root.
            finalize::apply_file_metadata(
                &batch_files,
                &temp_root,
                xattrs_enabled,
                restore_as_root,
                stats,
            )?;

            total_bytes += bytes;
            total_files += batch_files.len() as u64;
            Ok(())
        },
    )
    .map_err(&cleanup)?;

    drop(items_stream);
    repo.clear_chunk_index();

    // Symlink metadata (F1/F5: lchown → xattrs → mtime) is applied while the
    // links still live in `temp_root` — the no-reopen property holds for the
    // security-sensitive lchown, and a `rename` never disturbs a symlink's own
    // metadata.
    finalize::apply_symlink_metadata(&plan_out.symlinks, restore_as_root, &mut stats);

    // Relink hard-link group members now that every representative is written
    // and still in `temp_root` — both link operands share the temp filesystem,
    // so the links survive the move below as ordinary directory entries.
    // `hard_link` bumps only ctime (not preserved), leaving the representative's
    // restored mtime untouched.
    // Like the streaming/write phase above, a failure here is *before*
    // finalization (nothing has been moved into `dest`), so route it through
    // `cleanup` to remove the populated staging directory. Copy-fallback
    // siblings physically write bytes; fold them into the restored byte total.
    total_bytes += finalize::create_hardlinks(
        &pending_links,
        &group_reps,
        &temp_root,
        xattrs_enabled,
        restore_as_root,
        &mut stats,
    )
    .map_err(&cleanup)?;

    // Phase 5a: rename temp subtrees into the final destination.  All file
    // metadata is already on the inodes, so the rename has no observable TOCTOU
    // window.  Directories are still at their staging mode (`item.mode | 0o700`,
    // owner-writable) so a captured read-only top-level dir can be moved: a
    // cross-parent `rename` of a directory updates its `..` entry and therefore
    // needs write permission on the directory itself.
    // No `cleanup` here: `move_temp_to_dest` owns finalization cleanup — it
    // rolls a graceful failure back to an empty `dest`, or returns a distinct
    // "remove before retrying" error if even rollback fails.
    finalize::move_temp_to_dest(&temp_root, &dest_root)?;

    // Directory metadata (F1/F2: chown → xattrs → mode → mtime, deepest-first)
    // is applied *after* the move, for two reasons: the captured restrictive
    // mode must follow the cross-parent rename above, and a parent's mtime/mode
    // must be the last write to its inode (after every child, including file
    // batches, has landed).  Rebase the staged temp paths onto `dest_root`; the
    // single-process, validated-empty-dest model (see Phase 3) makes the
    // path-based application safe.
    //
    // Crash window (accepted): a SIGKILL between `move_temp_to_dest` and the end
    // of this pass leaves a *complete* tree at `dest` whose directories still
    // carry their owner-writable staging mode (`item.mode | 0o700`) and may lack
    // ownership/xattrs/mtimes.  File data and metadata are intact — only
    // directory attributes are degraded.  This extends the documented
    // mid-rename limitation (`move_temp_to_dest`): once any entry has moved,
    // `dest` is non-empty and a retry is rejected by `validate_and_prepare_dest`
    // until the operator clears it, so re-running restore into a fresh
    // destination is the recovery path.  All failures inside the pass itself are
    // already non-fatal warnings, so a partial pass never aborts the restore.
    for node in &mut plan_out.dirs {
        if let Ok(rel) = node.path.strip_prefix(&temp_root) {
            node.path = dest_root.join(rel);
        }
    }
    finalize::apply_dir_metadata(&mut plan_out.dirs, restore_as_root, &mut stats);

    // Copy-fallback siblings are independent inodes, so they are files, not
    // hard links. They were never `PlannedFile`s (and so are absent from
    // `total_files`); fold them into the file count here for an honest total.
    stats.files = total_files + stats.hardlink_copies;
    stats.total_bytes = total_bytes;

    info!(
        "Restored {} files, {} dirs, {} symlinks, {} hard links ({})",
        format_count(stats.files),
        format_count(stats.dirs),
        format_count(stats.symlinks),
        format_count(stats.hardlinks),
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

/// True only when `name` is [`RESTORE_TEMP_PREFIX`] followed by exactly 16
/// lowercase hex digits — the strict shape `validate_and_prepare_dest` produces
/// via `format!("{}{:016x}", ...)`. Near-miss names (wrong length, uppercase,
/// non-hex, a different suffix) return `false` and are preserved. Whether the
/// entry is actually a directory is the caller's responsibility (via
/// `file_type`), so a *file* or symlink bearing this name is never swept.
pub(super) fn is_reserved_temp_dir_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    let Some(suffix) = name.strip_prefix(RESTORE_TEMP_PREFIX) else {
        return false;
    };
    suffix.len() == 16
        && suffix
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

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
    fn is_reserved_temp_dir_name_strict_shape() {
        let ok = |s: &str| is_reserved_temp_dir_name(OsStr::new(s));
        // Exactly prefix + 16 lowercase hex digits.
        assert!(ok(".vykar-restore-0123456789abcdef"));
        assert!(ok(".vykar-restore-ffffffffffffffff"));
        assert!(ok(".vykar-restore-0000000000000000"));
        // Wrong length (15 / 17).
        assert!(!ok(".vykar-restore-0123456789abcde"));
        assert!(!ok(".vykar-restore-0123456789abcdef0"));
        // Uppercase / non-hex.
        assert!(!ok(".vykar-restore-0123456789ABCDEF"));
        assert!(!ok(".vykar-restore-0123456789abcdeg"));
        // Different / missing suffix.
        assert!(!ok(".vykar-restore-notes"));
        assert!(!ok(".vykar-restore-"));
        assert!(!ok(".vykar-restore"));
        assert!(!ok("vykar-restore-0123456789abcdef"));
    }

    /// The non-Unix preflight's detection helper is pure (no fs) and reports the
    /// count + first example for raw entries, or `None` when the stream is clean.
    /// Running it before any destination prep is what makes the non-Unix abort a
    /// no-write operation; this pins the detection logic on every platform.
    #[test]
    fn detect_raw_entries_reports_raw_items_only() {
        use crate::snapshot::item::{Item, ItemRawNames, ItemType};

        fn item(path: &str, raw: Option<ItemRawNames>) -> Item {
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
                size: 0,
                chunks: Vec::new(),
                link_target: None,
                xattrs: None,
                raw_names: raw,
                hardlink: None,
            }
        }

        fn encode(items: &[Item]) -> Vec<u8> {
            let mut buf = Vec::new();
            for it in items {
                rmp_serde::encode::write(&mut buf, it).unwrap();
            }
            buf
        }

        // Clean stream → None.
        let clean = encode(&[item("a.txt", None), item("b.txt", None)]);
        assert!(detect_raw_entries(&clean).unwrap().is_none());

        // One raw entry among normal ones → Some((1, that path)).
        let raw_names = ItemRawNames {
            path: Some(b"r-\x80.bin".to_vec()),
            link_target: None,
        };
        let raw_display = String::from_utf8_lossy(b"r-\x80.bin").into_owned();
        let mixed = encode(&[
            item("a.txt", None),
            item(&raw_display, Some(raw_names)),
            item("c.txt", None),
        ]);
        let (count, example) = detect_raw_entries(&mixed).unwrap().unwrap();
        assert_eq!(count, 1);
        assert_eq!(example, raw_display);
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
