//! Phase 5a (rename) and 5b (chmod/utimes/xattrs): apply file metadata in the
//! temp restore tree per batch, then rename top-level entries into the final
//! destination once all batches have been written.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use crate::platform::fs;
use vykar_types::error::{Result, VykarError};

use super::plan::{PendingLink, PlannedFile, PlannedNode, RepInfo};
use super::{apply_item_xattrs, push_metadata_warning, warn_metadata_err, RestoreStats};
use crate::snapshot::item::HardlinkId;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

#[cfg(test)]
use std::cell::{Cell, RefCell};

/// Phase 5b: apply file metadata (mode, mtime, xattrs) to a batch of files
/// while they still live inside `temp_root`.  Doing this before the final
/// rename closes a path-based reopen window — the inode keeps its metadata
/// after `move_temp_to_dest` because `rename(2)` only changes directory
/// entries, not inodes.  fd-based fchmod/futimens avoid a redundant path
/// lookup; on fd open failure the path-based call is used as a fallback.
pub(super) fn apply_file_metadata(
    planned_files: &[PlannedFile],
    temp_root: &Path,
    xattrs_enabled: bool,
    restore_as_root: bool,
    stats: &mut RestoreStats,
) -> Result<()> {
    for pf in planned_files {
        let target_path = temp_root.join(&pf.rel_path);
        let (mtime_secs, mtime_nanos) = split_unix_nanos(pf.mtime);

        // Open a writable handle on every platform — needed because the final
        // `sync_all` (`FlushFileBuffers` on Windows) requires write access, and
        // opening *before* mode application keeps a to-be-read-only file
        // writable at open time. A failure here is a hard error: we cannot make
        // the data durable without it. Self-heals a restrictive-umask
        // owner-write strip on `PermissionDenied` (chmod 0o600, retry once).
        let file = open_writable_for_finalize(&target_path)?;

        // Uniform metadata order: chown → xattrs → chmod → mtime. chown clears
        // setuid/setgid + `security.capability`, so it must precede xattrs and
        // the final chmod; xattrs run while the inode is still owner-writable
        // (before a possibly read-only final mode); the captured mode is
        // applied last; mtime is last because chown/chmod/setxattr bump ctime,
        // not mtime.
        if restore_as_root {
            warn_metadata_err(
                stats,
                fs::chown_fd(&file, pf.uid, pf.gid),
                &target_path,
                "owner",
            );
            #[cfg(test)]
            record_op("chown", pf.uid, pf.gid);
        }

        // xattrs remain path-based (no fd-based xattr API in std).
        if xattrs_enabled {
            apply_item_xattrs(&target_path, pf.xattrs.as_ref(), stats);
        }
        #[cfg(test)]
        record_op("xattr", 0, 0);

        // fd-based fchmod/futimens are Unix-only; on other platforms fall
        // through to the path-based calls to avoid silent no-ops. Only the
        // *final* failure (after the path-based fallback) is recorded as a
        // warning — intermediate fd failures that succeed on the fallback are
        // not user-facing.
        #[cfg(unix)]
        {
            if fs::apply_mode_fd(&file, pf.mode).is_err() {
                warn_metadata_err(
                    stats,
                    fs::apply_mode(&target_path, pf.mode),
                    &target_path,
                    "mode",
                );
            }
            if fs::set_file_mtime_fd(&file, mtime_secs, mtime_nanos).is_err() {
                warn_metadata_err(
                    stats,
                    fs::set_file_mtime(&target_path, mtime_secs, mtime_nanos),
                    &target_path,
                    "mtime",
                );
            }
        }
        #[cfg(not(unix))]
        {
            warn_metadata_err(
                stats,
                fs::apply_mode(&target_path, pf.mode),
                &target_path,
                "mode",
            );
            warn_metadata_err(
                stats,
                fs::set_file_mtime(&target_path, mtime_secs, mtime_nanos),
                &target_path,
                "mtime",
            );
        }
        // Recorded outside the cfg blocks so the ordering test (which is not
        // platform-gated) sees `chmod` then `mtime` on every platform; both
        // branches apply mode before mtime.
        #[cfg(test)]
        {
            record_op("chmod", 0, 0);
            record_op("mtime", 0, 0);
        }

        // F3-a: make file data + inode (size/mode/mtime) durable. `sync_all`
        // (not `sync_data`) flushes the metadata applied above with the data.
        inj_fsync_file(&file).map_err(|e| {
            VykarError::Other(format!("failed to fsync {}: {e}", target_path.display()))
        })?;
    }
    Ok(())
}

/// Deepest-first directory metadata pass (F1/F2). Sorts in place so a parent's
/// mode/mtime is the last write to that inode after all its children land.
/// Each dir is still at staging mode `item.mode | 0o700` (owner-writable)
/// during the xattr step; the captured mode is applied after. Infallible —
/// failures become non-fatal warnings.
pub(super) fn apply_dir_metadata(
    dirs: &mut [PlannedNode],
    restore_as_root: bool,
    stats: &mut RestoreStats,
) {
    // Reverse by component count → deepest paths first.
    dirs.sort_by_key(|d| std::cmp::Reverse(d.path.components().count()));

    for d in dirs.iter() {
        if restore_as_root {
            warn_metadata_err(
                stats,
                fs::chown_path(&d.path, d.uid, d.gid),
                &d.path,
                "owner",
            );
            #[cfg(test)]
            record_op("chown", d.uid, d.gid);
        }
        // `xattrs` is already `None` when the restore disabled xattrs, so this
        // honors `xattrs_enabled` without a flag.
        apply_item_xattrs(&d.path, d.xattrs.as_ref(), stats);
        #[cfg(test)]
        record_op("xattr", 0, 0);
        warn_metadata_err(stats, fs::apply_mode(&d.path, d.mode), &d.path, "mode");
        #[cfg(test)]
        record_op("chmod", 0, 0);
        let (secs, nanos) = split_unix_nanos(d.mtime);
        warn_metadata_err(
            stats,
            fs::set_file_mtime(&d.path, secs, nanos),
            &d.path,
            "mtime",
        );
        #[cfg(test)]
        record_op("mtime", 0, 0);
    }
}

/// Symlink metadata pass (F1/F5). No chmod — symlink permission bits are
/// ignored on Linux. Order: lchown → xattrs → mtime. Infallible (warnings).
pub(super) fn apply_symlink_metadata(
    symlinks: &[PlannedNode],
    restore_as_root: bool,
    stats: &mut RestoreStats,
) {
    for s in symlinks {
        if restore_as_root {
            warn_metadata_err(
                stats,
                fs::lchown_path(&s.path, s.uid, s.gid),
                &s.path,
                "owner",
            );
            #[cfg(test)]
            record_op("lchown", s.uid, s.gid);
        }
        apply_item_xattrs(&s.path, s.xattrs.as_ref(), stats);
        #[cfg(test)]
        record_op("xattr", 0, 0);
        let (secs, nanos) = split_unix_nanos(s.mtime);
        warn_metadata_err(
            stats,
            fs::set_symlink_mtime(&s.path, secs, nanos),
            &s.path,
            "mtime",
        );
        #[cfg(test)]
        record_op("mtime", 0, 0);
    }
}

/// Relink hard-link group members to their representatives. Runs after all
/// representatives are materialized in `temp_root` and before the final move,
/// so both operands of every `hard_link` live on the same filesystem and the
/// resulting link survives `move_temp_to_dest` as an ordinary directory entry.
///
/// A pending link is *file content the caller asked to restore*, so a failure
/// to place that content is fatal (`Err`) — the same contract as a normal
/// file's write/fsync in [`apply_file_metadata`]. Only a successful copy
/// fallback (content placed, fidelity degraded to a separate inode) is a
/// warning.
///
/// Fallback ladder (single, concrete — distinct from the plan-time divergence
/// path which materializes from a member's own chunks):
///   1. `hard_link(rep, link)` — the common path; one inode, N names.
///   2. On failure (unsupported fs, `EMLINK` link-count cap, cross-device):
///      `copy(rep, link)` — the representative is already on disk with identical
///      content, so no chunk re-fetch is needed. The copy is then finalized
///      through the **same** [`apply_file_metadata`] path a normal file uses
///      (chown → xattrs → chmod → mtime → `fsync`), so it is durable, carries
///      the representative's mode/owner/xattrs, and preserves setuid/setgid
///      (mode is re-applied after the ownership change clears it). It is counted
///      as a `file` (separate inode), not a hard link.
///   3. On copy failure: **fatal** — the requested path would otherwise be
///      silently absent from a "successful" restore.
///
/// Metadata policy for genuine links (step 1): a hard link is one inode, so all
/// of mode/uid/gid/mtime/xattrs are inode attributes shared by every name. A
/// member only reaches `pending_links` after matching the representative's
/// content fingerprint, i.e. it *is* a genuine link, so it necessarily shares
/// the representative's metadata — inheriting it is the only physically possible
/// outcome, not a silent collapse. (If the same `(dev, ino)` was captured twice
/// with divergent metadata mid-backup, that is an inherent hard-link/TOCTOU
/// limitation shared with borg/restic, not something splitting into two inodes
/// could fix.)
///
/// Containment: the link target's parent is validated against `temp_root`
/// **unconditionally** (not gated on `!parent.exists()`). A pre-existing parent
/// can be a symlink the snapshot planted that resolves outside the restore root;
/// skipping the check there would let `hard_link`/`copy` write through it. The
/// link's own final component is defended too: any pre-existing entry there is
/// unlinked (no-follow) before linking, and the copy fallback creates a fresh
/// file (`O_EXCL`, never following a symlink) — otherwise a planted symlink at
/// the link's exact path would make `hard_link` fail `EEXIST` and the copy
/// write through it. A containment violation is fatal, matching the normal-file
/// path's `ensure_path_within_root`.
///
/// Returns the number of bytes physically written by copy fallbacks (a true
/// hard link writes none), so the caller can fold them into `total_bytes`.
pub(super) fn create_hardlinks(
    pending_links: &[PendingLink],
    group_reps: &HashMap<HardlinkId, RepInfo>,
    temp_root: &Path,
    xattrs_enabled: bool,
    restore_as_root: bool,
    stats: &mut RestoreStats,
) -> Result<u64> {
    let mut copied_bytes: u64 = 0;
    for link in pending_links {
        let Some(rep) = group_reps.get(&link.id) else {
            // A link is only ever queued after its representative is recorded,
            // and representatives are never evicted — so this is unreachable.
            // Treat a violation as fatal rather than dropping requested content:
            // returning Ok here would mean a "successful" restore missing a file.
            return Err(VykarError::Other(format!(
                "internal error: hard link '{}' has no recorded representative",
                link.link_rel.display()
            )));
        };
        let rep_abs = temp_root.join(&rep.rel_path);
        let link_abs = temp_root.join(&link.link_rel);

        // The representative is resolved by *path* at finalize time, so guard
        // against a malformed snapshot that replaced it between planning and
        // now: a later same-path item can overwrite it — in particular the
        // symlink arm unlinks the file and plants a symlink in its place. Were
        // that allowed to stand, `copy_into_new`'s source `File::open` would
        // follow the symlink and read external host data into the restore, and
        // `hard_link` would link the symlink itself. Require the exact regular
        // file we materialized (type + size); anything else is a corrupt
        // snapshot and fatal. Safe without TOCTOU concern: this pass is
        // single-threaded and nothing mutates `temp_root` between here and the
        // link/copy below.
        let rep_meta = std::fs::symlink_metadata(&rep_abs).map_err(|e| {
            VykarError::Other(format!(
                "hard-link representative '{}' is unreadable before relinking: {e}",
                rep.rel_path.display()
            ))
        })?;
        if !rep_meta.file_type().is_file() || rep_meta.len() != rep.size {
            return Err(VykarError::Other(format!(
                "hard-link representative '{}' was replaced before relinking \
                 (expected a {}-byte regular file); refusing to link '{}'",
                rep.rel_path.display(),
                rep.size,
                link.link_rel.display()
            )));
        }

        // Always validate (and create) the parent within `temp_root`. The check
        // canonicalizes the nearest existing ancestor, so a planted symlinked
        // parent resolving outside the root is rejected here even though it
        // "exists". A violation or creation failure is fatal — we will not write
        // a link target whose parent we cannot prove is contained.
        super::plan::ensure_parent_exists_within_root(&link_abs, temp_root)?;

        // Defend the link's own final component: a malformed snapshot can place
        // a symlink item at this exact path. `hard_link(2)` would then fail
        // `EEXIST` and the copy fallback, opening with create+truncate, would
        // follow the symlink and write through it (possibly outside temp_root).
        // Unlink any pre-existing entry first — `remove_file` operates on the
        // symlink itself, it does not follow — so both the link and the copy act
        // on an absent destination. (A real directory here can't be removed this
        // way; the link/copy then fail and we abort below, never escaping.)
        let _ = std::fs::remove_file(&link_abs);

        match inj_hard_link(&rep_abs, &link_abs) {
            Ok(()) => {
                stats.hardlinks += 1;
            }
            Err(link_err) => {
                // Fallback: independent copy of the representative's content.
                // The plan-time fingerprint gate (`RepInfo::chunks_fp`)
                // guarantees the representative's content equals what this
                // member declared, so copying from it is not a substitution.
                // `copy_into_new` creates the destination with `O_EXCL`, so it
                // never follows or overwrites a symlink racing back in.
                match copy_into_new(&rep_abs, &link_abs) {
                    Ok(bytes) => {
                        // Finalize the copy through the exact normal-file path:
                        // a single `PlannedFile` built from the representative's
                        // on-disk metadata, so the copy is fsync'd, owns the
                        // representative's mode/owner/xattrs, and keeps
                        // setuid/setgid (mode re-applied after chown). A failure
                        // in this durability/metadata pass is fatal, same as for
                        // a normal file.
                        let xattrs = if xattrs_enabled {
                            read_xattrs_map(&rep_abs, stats)
                        } else {
                            None
                        };
                        let pf = PlannedFile {
                            rel_path: link.link_rel.clone(),
                            total_size: 0, // unused by apply_file_metadata
                            mode: rep_mode_on_disk(&rep_abs),
                            mtime: link.mtime,
                            uid: link.uid,
                            gid: link.gid,
                            xattrs,
                            created: AtomicBool::new(false),
                        };
                        apply_file_metadata(
                            std::slice::from_ref(&pf),
                            temp_root,
                            xattrs_enabled,
                            restore_as_root,
                            stats,
                        )?;
                        // Separate inode → a file, not a hard link. Its bytes
                        // were physically written, so they count toward
                        // total_bytes (a true hard link writes none).
                        copied_bytes += bytes;
                        stats.hardlink_copies += 1;
                        push_metadata_warning(
                            stats,
                            format!(
                                "could not hard-link '{}' to '{}' ({link_err}); restored as an \
                                 independent copy (separate inode)",
                                link.link_rel.display(),
                                rep.rel_path.display()
                            ),
                        );
                    }
                    Err(copy_err) => {
                        // Both paths failed: the requested content cannot be
                        // placed. Fatal — never report success with a file
                        // missing from the destination.
                        return Err(VykarError::Other(format!(
                            "failed to restore hard link '{}': hard_link failed ({link_err}) \
                             and copy fallback failed ({copy_err})",
                            link.link_rel.display()
                        )));
                    }
                }
            }
        }
    }
    Ok(copied_bytes)
}

/// Copy `src`'s contents into a freshly created `dst`, returning bytes written.
/// `create_new` (`O_CREAT | O_EXCL`) refuses to open an existing path and never
/// follows a symlink at the final component, so a planted symlink at the link's
/// own filename cannot redirect the write outside `temp_root`. Mode/owner/mtime
/// are applied separately by the caller's `apply_file_metadata` pass.
fn copy_into_new(src: &Path, dst: &Path) -> io::Result<u64> {
    let mut input = std::fs::File::open(src)?;
    let mut out = OpenOptions::new().write(true).create_new(true).open(dst)?;
    io::copy(&mut input, &mut out)
}

/// The representative's permission bits as stored on disk, for stamping onto a
/// copy-fallback sibling. On Unix this includes setuid/setgid/sticky; falls
/// back to `0o644` if the stat fails. Non-Unix has no comparable bits.
fn rep_mode_on_disk(rep_abs: &Path) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(rep_abs)
            .map(|m| m.mode())
            .unwrap_or(0o644)
    }
    #[cfg(not(unix))]
    {
        let _ = rep_abs;
        0o644
    }
}

/// Read the representative's extended attributes into the map shape
/// [`apply_item_xattrs`] consumes, so a copy-fallback sibling carries them too.
/// Failures (list error, per-name get error, non-UTF8 name) emit restore
/// warnings rather than silently dropping attributes, matching the backup-side
/// xattr reader. Returns `None` when there are no xattrs or no platform support.
fn read_xattrs_map(rep_abs: &Path, stats: &mut RestoreStats) -> Option<HashMap<String, Vec<u8>>> {
    #[cfg(unix)]
    {
        let names = match xattr::list(rep_abs) {
            Ok(names) => names,
            Err(e) => {
                push_metadata_warning(
                    stats,
                    format!(
                        "failed to list xattrs of '{}' for hard-link copy: {e}",
                        rep_abs.display()
                    ),
                );
                return None;
            }
        };
        let mut map = HashMap::new();
        for name in names {
            let Some(name_str) = name.to_str() else {
                push_metadata_warning(
                    stats,
                    format!(
                        "skipping non-UTF8 xattr name on '{}' for hard-link copy",
                        rep_abs.display()
                    ),
                );
                continue;
            };
            match xattr::get(rep_abs, &name) {
                Ok(Some(value)) => {
                    map.insert(name_str.to_string(), value);
                }
                Ok(None) => {}
                Err(e) => push_metadata_warning(
                    stats,
                    format!(
                        "failed to read xattr {name_str} of '{}' for hard-link copy: {e}",
                        rep_abs.display()
                    ),
                ),
            }
        }
        (!map.is_empty()).then_some(map)
    }
    #[cfg(not(unix))]
    {
        let _ = (rep_abs, stats);
        None
    }
}

/// Open `path` writable for the finalize fsync, self-healing on a
/// restrictive-umask owner-write strip. `write_buf` and the empty-file
/// `File::create` create files with the default mode filtered by the process
/// umask, so an exotic owner-write-stripping umask (e.g. `0o277`) yields a
/// `0o400` file and this reopen fails `EACCES`. On `PermissionDenied` only, a
/// path chmod (not umask-filtered) grants `0o600` and the open is retried once;
/// the caller's final fchmod still applies the captured mode. Zero cost on
/// normal umasks (first open succeeds).
fn open_writable_for_finalize(path: &Path) -> Result<std::fs::File> {
    let open_err = |e: io::Error| {
        VykarError::Other(format!("failed to open {} for fsync: {e}", path.display()))
    };
    match inj_open_writable(path) {
        Ok(file) => Ok(file),
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            let _ = fs::apply_mode(path, 0o600);
            inj_open_writable(path).map_err(open_err)
        }
        Err(e) => Err(open_err(e)),
    }
}

/// Phase 5a: move all top-level entries from `temp_root` into `dest_root`, then
/// fsync `dest_root` so the moved entry names are durable (F3-b), then remove
/// the now-empty temp root.
///
/// A *graceful* failure (disk-full, EIO) during the rename loop or the dest
/// fsync is rolled back: every entry already moved is renamed back into
/// `temp_root`, which is then discarded so `dest` is empty and a retry starts
/// clean. If even the rollback fails, both locations are left in place and a
/// distinct error tells the operator to remove them before retrying.
///
/// A SIGKILL landing *mid-loop* is the documented limitation (see the module
/// plan): partially-moved entries can be stranded in `dest_root` that the sweep
/// cannot safely remove → manual cleanup before retry.
pub(super) fn move_temp_to_dest(temp_root: &Path, dest_root: &Path) -> Result<()> {
    // Snapshot the entries before mutating the tree — renaming entries out of
    // `temp_root` while iterating its `read_dir` is undefined on some
    // filesystems.
    let entries: Vec<std::fs::DirEntry> =
        std::fs::read_dir(temp_root)?.collect::<io::Result<Vec<_>>>()?;

    let mut moved: Vec<PathBuf> = Vec::new();
    for entry in &entries {
        let from = entry.path();
        let to = dest_root.join(entry.file_name());
        if let Err(e) = inj_rename(&from, &to) {
            let err = VykarError::Other(format!(
                "failed to move '{}' into '{}': {e}",
                from.display(),
                dest_root.display()
            ));
            return rollback_finalization(temp_root, dest_root, &moved, err);
        }
        moved.push(to);
    }

    // F3-b: persist the moved top-level entry names (accepted durability
    // boundary — intermediate directory names are left to the filesystem).
    if let Err(e) = inj_fsync_dir(dest_root) {
        let err = VykarError::Other(format!(
            "failed to fsync restore destination '{}': {e}",
            dest_root.display()
        ));
        return rollback_finalization(temp_root, dest_root, &moved, err);
    }

    // Success: best-effort removal of the (now empty) temp root. If it lingers,
    // the next run's sweep reclaims it — don't fail the restore over it.
    if let Err(e) = std::fs::remove_dir(temp_root) {
        tracing::warn!(
            "restore succeeded but temp dir '{}' could not be removed (will be swept next run): {e}",
            temp_root.display()
        );
    }
    Ok(())
}

/// Reverse-rename every already-moved entry back into `temp_root` (newest
/// first). On full success, discard `temp_root` so `dest` is empty and a retry
/// is clean, then return the underlying error. If any reverse-rename fails,
/// leave both locations and return a distinct, actionable error.
fn rollback_finalization(
    temp_root: &Path,
    dest_root: &Path,
    moved: &[PathBuf],
    orig: VykarError,
) -> Result<()> {
    for final_path in moved.iter().rev() {
        let Some(name) = final_path.file_name() else {
            continue;
        };
        let back = temp_root.join(name);
        if inj_rollback_rename(final_path, &back).is_err() {
            return Err(VykarError::Other(format!(
                "restore could not be finalized or rolled back; '{}' is partially \
                 populated and '{}' remains — remove them before retrying",
                dest_root.display(),
                temp_root.display()
            )));
        }
    }
    // All entries are back in `temp_root`; discard the staged tree.
    force_remove_temp_tree(temp_root)?;
    Err(orig)
}

// ---------------------------------------------------------------------------
// Restrictive-safe, symlink-correct removal
// ---------------------------------------------------------------------------

/// Recursively remove a restore temp tree, tolerating restrictive (`000`) dir
/// modes a killed restore can leave behind (dir modes are applied during
/// streaming). `std::fs::remove_dir_all` opens each directory before removing
/// it, so it `EACCES`es on a mode-`000` dir even when empty; this helper grants
/// itself owner `rwx` on **every** directory it is about to clear — `dir` and
/// each subdirectory alike — before reading it.
///
/// `DirEntry::file_type()` never follows symlinks, so a directory symlink is
/// removed as a link (Unix: `remove_file`; Windows: `remove_dir` on the reparse
/// point) and its target is never touched.
pub(super) fn force_remove_temp_tree(dir: &Path) -> io::Result<()> {
    // Grant owner rwx so our own read_dir/remove_dir can't EACCES on this dir.
    #[cfg(unix)]
    {
        let _ = fs::apply_mode(dir, 0o700);
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;

        if file_type.is_dir() {
            // Real subdirectory — recurse (grants + clears it).
            force_remove_temp_tree(&path)?;
            continue;
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileTypeExt;
            if file_type.is_symlink_dir() {
                // Directory symlink / junction: remove the reparse point
                // itself, never the target.
                std::fs::remove_dir(&path)?;
                continue;
            }
        }

        // File, or a file/Unix symlink — `remove_file` unlinks the entry
        // (the link, not its target).
        std::fs::remove_file(&path)?;
    }

    std::fs::remove_dir(dir)
}

// ---------------------------------------------------------------------------
// Injectable fsync/rename wrappers — the only fsync/rename calls restore makes.
// `platform/fs.rs` stays fault-unaware; the fault hooks live here, above it.
// ---------------------------------------------------------------------------

fn inj_fsync_file(file: &std::fs::File) -> io::Result<()> {
    #[cfg(test)]
    if FAULT_FILE_FSYNC.with(|c| c.get()) {
        return Err(io::Error::other("injected file fsync failure"));
    }
    file.sync_all()
}

fn inj_fsync_dir(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if FAULT_DIR_FSYNC.with(|c| c.get()) {
        return Err(io::Error::other("injected dest_root fsync failure"));
    }

    #[cfg(unix)]
    {
        let dir = std::fs::File::open(path)?;
        fs::fsync_dir_file(&dir)
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn inj_rename(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(test)]
    {
        // Semantics: "fail after N successful moves". `Some(1)` lets the first
        // rename succeed and fails the second; one-shot (consumed on firing).
        let fail = FAULT_FORWARD_RENAME.with(|c| match c.get() {
            Some(0) => {
                c.set(None);
                true
            }
            Some(n) => {
                c.set(Some(n - 1));
                false
            }
            None => false,
        });
        if fail {
            return Err(io::Error::other("injected forward rename failure"));
        }
    }
    std::fs::rename(from, to)
}

/// `hard_link` with a test-only fault hook so the copy fallback in
/// [`create_hardlinks`] can be exercised without an exotic filesystem.
fn inj_hard_link(original: &Path, link: &Path) -> io::Result<()> {
    #[cfg(test)]
    {
        if FAULT_HARD_LINK.with(|c| c.get()) {
            return Err(io::Error::other("injected hard_link failure"));
        }
    }
    fs::hard_link(original, link)
}

fn inj_rollback_rename(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(test)]
    if FAULT_ROLLBACK_RENAME.with(|c| c.get()) {
        return Err(io::Error::other("injected rollback rename failure"));
    }
    std::fs::rename(from, to)
}

/// The single open-for-write seam used by `open_writable_for_finalize`. Tests
/// can force a given `ErrorKind` on the Nth (0-indexed) call and count calls,
/// driving the `PermissionDenied → chmod → retry` recovery and asserting that
/// non-`PermissionDenied` errors are not retried.
fn inj_open_writable(path: &Path) -> io::Result<std::fs::File> {
    #[cfg(test)]
    {
        let call = OPEN_WRITABLE_CALLS.with(|c| {
            let v = c.get();
            c.set(v + 1);
            v
        });
        if let Some((target, kind)) = OPEN_WRITABLE_FAULT.with(|c| c.get()) {
            if call == target {
                return Err(io::Error::from(kind));
            }
        }
    }
    OpenOptions::new().write(true).open(path)
}

// ---------------------------------------------------------------------------
// Thread-local fault state (tests only).
//
// Thread-locals, not globals+mutex: every injected op runs on the test's own
// calling thread (the parallel write workers never fsync or rename), so
// thread-locals isolate unrelated parallel restore tests from each other.
// ---------------------------------------------------------------------------

#[cfg(test)]
thread_local! {
    static FAULT_FILE_FSYNC: Cell<bool> = const { Cell::new(false) };
    static FAULT_DIR_FSYNC: Cell<bool> = const { Cell::new(false) };
    /// Forces `inj_hard_link` to fail (exercising the copy fallback) while set.
    static FAULT_HARD_LINK: Cell<bool> = const { Cell::new(false) };
    static FAULT_FORWARD_RENAME: Cell<Option<usize>> = const { Cell::new(None) };
    static FAULT_ROLLBACK_RENAME: Cell<bool> = const { Cell::new(false) };
    /// `Some((call_index, kind))` forces `inj_open_writable` to return `kind`
    /// on its `call_index`-th (0-based) invocation; `None` disables the fault.
    static OPEN_WRITABLE_FAULT: Cell<Option<(usize, io::ErrorKind)>> = const { Cell::new(None) };
    /// Count of `inj_open_writable` invocations since the last reset.
    static OPEN_WRITABLE_CALLS: Cell<usize> = const { Cell::new(0) };
    /// Ordered metadata-op trace `(tag, uid, gid)` recorded by the three
    /// finalize passes. Lets a non-root test assert the exact apply order and
    /// the uid/gid passed to chown.
    static METADATA_OPS: RefCell<Vec<(&'static str, u32, u32)>> = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
fn record_op(tag: &'static str, uid: u32, gid: u32) {
    METADATA_OPS.with(|c| c.borrow_mut().push((tag, uid, gid)));
}

#[cfg(test)]
struct FaultGuard;

#[cfg(test)]
impl Drop for FaultGuard {
    fn drop(&mut self) {
        FAULT_FILE_FSYNC.with(|c| c.set(false));
        FAULT_DIR_FSYNC.with(|c| c.set(false));
        FAULT_HARD_LINK.with(|c| c.set(false));
        FAULT_FORWARD_RENAME.with(|c| c.set(None));
        FAULT_ROLLBACK_RENAME.with(|c| c.set(false));
        OPEN_WRITABLE_FAULT.with(|c| c.set(None));
        OPEN_WRITABLE_CALLS.with(|c| c.set(0));
        METADATA_OPS.with(|c| c.borrow_mut().clear());
    }
}

fn split_unix_nanos(total_nanos: i64) -> (i64, u32) {
    let secs = total_nanos.div_euclid(1_000_000_000);
    let nanos = total_nanos.rem_euclid(1_000_000_000) as u32;
    (secs, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;

    /// Reserved-shape temp dir name for a test staging root inside `dest`.
    fn temp_name() -> &'static str {
        ".vykar-restore-0123456789abcdef"
    }

    /// Build a `PlannedFile` for a file already written at `temp_root/rel`.
    fn planned(rel: &str, size: u64) -> PlannedFile {
        PlannedFile {
            rel_path: PathBuf::from(rel),
            total_size: size,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
            created: AtomicBool::new(true),
        }
    }

    /// True if `dir` still contains any `.vykar-restore-*` entry.
    fn has_reserved_temp(dir: &Path) -> bool {
        std::fs::read_dir(dir)
            .unwrap()
            .any(|e| super::super::is_reserved_temp_dir_name(&e.unwrap().file_name()))
    }

    /// Mirror the restore orchestrator's finalization phase: apply metadata,
    /// then move into dest. A metadata error triggers the pre-finalization
    /// `cleanup` (force-remove the temp root); `move_temp_to_dest` owns its
    /// own cleanup (rollback).
    fn finalize_phase(files: &[PlannedFile], temp_root: &Path, dest_root: &Path) -> Result<()> {
        let mut stats = RestoreStats::default();
        if let Err(e) = apply_file_metadata(files, temp_root, false, false, &mut stats) {
            let _ = force_remove_temp_tree(temp_root);
            return Err(e);
        }
        move_temp_to_dest(temp_root, dest_root)
    }

    #[test]
    fn round_trip_nested_dirs_and_symlinks_durable() {
        let dest = tempdir().unwrap();
        let dest_root = dest.path();
        let temp_root = dest_root.join(temp_name());
        std::fs::create_dir_all(temp_root.join("sub/deep")).unwrap();
        std::fs::write(temp_root.join("top.txt"), b"top-data").unwrap();
        std::fs::write(temp_root.join("sub/a.txt"), b"a-data").unwrap();
        std::fs::write(temp_root.join("sub/deep/b.txt"), b"b-data").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("a.txt", temp_root.join("sub/link")).unwrap();

        let files = [
            planned("top.txt", 8),
            planned("sub/a.txt", 6),
            planned("sub/deep/b.txt", 6),
        ];
        finalize_phase(&files, &temp_root, dest_root).unwrap();

        assert_eq!(
            std::fs::read(dest_root.join("top.txt")).unwrap(),
            b"top-data"
        );
        assert_eq!(
            std::fs::read(dest_root.join("sub/a.txt")).unwrap(),
            b"a-data"
        );
        assert_eq!(
            std::fs::read(dest_root.join("sub/deep/b.txt")).unwrap(),
            b"b-data"
        );
        #[cfg(unix)]
        assert_eq!(
            std::fs::read_link(dest_root.join("sub/link")).unwrap(),
            Path::new("a.txt")
        );
        // Temp dir is gone.
        assert!(!has_reserved_temp(dest_root));
    }

    #[test]
    fn file_fsync_failure_is_hard_error_and_cleans_dest() {
        let _guard = FaultGuard;
        FAULT_FILE_FSYNC.with(|c| c.set(true));

        let dest = tempdir().unwrap();
        let dest_root = dest.path();
        let temp_root = dest_root.join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();
        std::fs::write(temp_root.join("f.txt"), b"data").unwrap();

        let files = [planned("f.txt", 4)];
        let err = finalize_phase(&files, &temp_root, dest_root).unwrap_err();
        assert!(err.to_string().contains("fsync"), "got: {err}");
        // dest is empty (temp swept by cleanup).
        assert_eq!(std::fs::read_dir(dest_root).unwrap().count(), 0);
    }

    #[test]
    fn dest_fsync_failure_rolls_back_and_cleans_dest() {
        let _guard = FaultGuard;
        FAULT_DIR_FSYNC.with(|c| c.set(true));

        let dest = tempdir().unwrap();
        let dest_root = dest.path();
        let temp_root = dest_root.join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();
        std::fs::write(temp_root.join("f.txt"), b"data").unwrap();

        let files = [planned("f.txt", 4)];
        let err = finalize_phase(&files, &temp_root, dest_root).unwrap_err();
        assert!(err.to_string().contains("fsync"), "got: {err}");
        // Rollback moved the entry back into temp_root, then discarded it.
        assert_eq!(std::fs::read_dir(dest_root).unwrap().count(), 0);
    }

    #[test]
    fn forward_rename_failure_after_one_move_rolls_back() {
        let _guard = FaultGuard;
        // Two top-level entries: first rename succeeds, second fails.
        FAULT_FORWARD_RENAME.with(|c| c.set(Some(1)));

        let dest = tempdir().unwrap();
        let dest_root = dest.path();
        let temp_root = dest_root.join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();
        std::fs::write(temp_root.join("a.txt"), b"aa").unwrap();
        std::fs::write(temp_root.join("b.txt"), b"bb").unwrap();

        let files = [planned("a.txt", 2), planned("b.txt", 2)];
        let err = finalize_phase(&files, &temp_root, dest_root).unwrap_err();
        assert!(err.to_string().contains("failed to move"), "got: {err}");
        // Rollback restored an empty dest.
        assert_eq!(std::fs::read_dir(dest_root).unwrap().count(), 0);
    }

    #[test]
    fn rollback_rename_failure_leaves_both_and_reports() {
        let _guard = FaultGuard;
        FAULT_FORWARD_RENAME.with(|c| c.set(Some(1)));
        FAULT_ROLLBACK_RENAME.with(|c| c.set(true));

        let dest = tempdir().unwrap();
        let dest_root = dest.path();
        let temp_root = dest_root.join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();
        std::fs::write(temp_root.join("a.txt"), b"aa").unwrap();
        std::fs::write(temp_root.join("b.txt"), b"bb").unwrap();

        let files = [planned("a.txt", 2), planned("b.txt", 2)];
        let err = finalize_phase(&files, &temp_root, dest_root).unwrap_err();
        assert!(
            err.to_string().contains("remove them before retrying"),
            "got: {err}"
        );
        // Both locations remain: one entry moved into dest, the temp dir stays.
        assert!(temp_root.is_dir());
        // dest has the temp dir plus exactly one moved entry.
        let names: Vec<_> = std::fs::read_dir(dest_root)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert!(has_reserved_temp(dest_root));
        assert_eq!(
            names.len(),
            2,
            "expected temp dir + one moved entry, got: {names:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn force_remove_handles_restrictive_dirs() {
        use std::os::unix::fs::PermissionsExt;

        // (i) reserved leftover containing an empty 000 subdir.
        let dest = tempdir().unwrap();
        let leftover = dest.path().join(temp_name());
        let locked_sub = leftover.join("locked");
        std::fs::create_dir_all(&locked_sub).unwrap();
        std::fs::set_permissions(&locked_sub, std::fs::Permissions::from_mode(0o000)).unwrap();
        // Plain remove_dir_all EACCESes on the 000 subdir.
        assert!(std::fs::remove_dir_all(&leftover).is_err());
        force_remove_temp_tree(&leftover).unwrap();
        assert!(!leftover.exists());

        // (ii) the reserved leftover dir itself is mode 000.
        let dest2 = tempdir().unwrap();
        let leftover2 = dest2.path().join(temp_name());
        std::fs::create_dir_all(&leftover2).unwrap();
        std::fs::write(leftover2.join("inner.txt"), b"x").unwrap();
        std::fs::set_permissions(&leftover2, std::fs::Permissions::from_mode(0o000)).unwrap();
        assert!(std::fs::remove_dir_all(&leftover2).is_err());
        force_remove_temp_tree(&leftover2).unwrap();
        assert!(!leftover2.exists());
    }

    #[cfg(windows)]
    #[test]
    fn force_remove_does_not_follow_dir_symlink() {
        let dest = tempdir().unwrap();
        // A target directory outside the leftover, with a file we must not lose.
        let target = dest.path().join("target");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(target.join("keep.txt"), b"keep").unwrap();

        let leftover = dest.path().join(temp_name());
        std::fs::create_dir_all(&leftover).unwrap();
        // Directory symlink inside the leftover pointing at `target`.
        std::os::windows::fs::symlink_dir(&target, leftover.join("link")).unwrap();

        force_remove_temp_tree(&leftover).unwrap();
        assert!(!leftover.exists());
        // The symlink target and its contents are untouched.
        assert_eq!(std::fs::read(target.join("keep.txt")).unwrap(), b"keep");
    }

    /// `create_hardlinks` links a queued member to its representative inside
    /// `temp_root`: the link shares the representative's inode and content, and
    /// `stats.hardlinks` is incremented.
    #[cfg(unix)]
    #[test]
    fn create_hardlinks_links_member_to_representative() {
        use std::os::unix::fs::MetadataExt;

        let dest = tempdir().unwrap();
        // Canonicalize the base: production derives temp_root from the
        // canonicalized dest_root, and the containment check compares against
        // it (matters on macOS where /var is a symlink to /private/var).
        let temp_root = dest.path().canonicalize().unwrap().join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();
        // Representative already materialized in temp_root.
        std::fs::write(temp_root.join("a.txt"), b"shared-content").unwrap();

        let id = HardlinkId { dev: 1, ino: 2 };
        let mut group_reps = HashMap::new();
        group_reps.insert(
            id,
            RepInfo {
                rel_path: PathBuf::from("a.txt"),
                size: 14,
                // create_hardlinks does not read the fingerprint (the gate is in
                // stream_and_plan); a placeholder is sufficient here.
                chunks_fp: [0u8; 32],
            },
        );
        let pending = [PendingLink {
            link_rel: PathBuf::from("b.txt"),
            id,
            mtime: 0,
            uid: 0,
            gid: 0,
        }];

        let mut stats = RestoreStats::default();
        create_hardlinks(&pending, &group_reps, &temp_root, false, false, &mut stats).unwrap();

        assert_eq!(stats.hardlinks, 1);
        let a_ino = std::fs::metadata(temp_root.join("a.txt")).unwrap().ino();
        let b_ino = std::fs::metadata(temp_root.join("b.txt")).unwrap().ino();
        assert_eq!(a_ino, b_ino, "link must share the representative's inode");
        assert_eq!(
            std::fs::read(temp_root.join("b.txt")).unwrap(),
            b"shared-content"
        );
    }

    /// A pending link whose representative is missing is a (theoretically
    /// unreachable) internal-consistency violation: it must be fatal, not a
    /// silently-dropped file. Asserts `create_hardlinks` returns `Err` and
    /// counts no link.
    #[test]
    fn create_hardlinks_missing_representative_is_fatal() {
        let dest = tempdir().unwrap();
        let temp_root = dest.path().join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();

        let id = HardlinkId { dev: 9, ino: 9 };
        let group_reps: HashMap<HardlinkId, RepInfo> = HashMap::new();
        let pending = [PendingLink {
            link_rel: PathBuf::from("orphan.txt"),
            id,
            mtime: 0,
            uid: 0,
            gid: 0,
        }];

        let mut stats = RestoreStats::default();
        let err = create_hardlinks(&pending, &group_reps, &temp_root, false, false, &mut stats)
            .unwrap_err()
            .to_string();
        assert_eq!(stats.hardlinks, 0);
        assert!(err.contains("no recorded representative"), "got: {err}");
    }

    /// Finding 1: a pending link whose parent is a symlink escaping `temp_root`
    /// must be rejected — the containment check runs unconditionally, even when
    /// the (symlinked) parent already exists. Asserts `create_hardlinks` returns
    /// `Err` and that nothing is written through the symlink to the outside dir.
    #[cfg(unix)]
    #[test]
    fn create_hardlinks_rejects_symlinked_parent_escape() {
        let dest = tempdir().unwrap();
        let base = dest.path().canonicalize().unwrap();
        let temp_root = base.join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();
        // A directory outside temp_root that the planted symlink resolves to.
        let outside = base.join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        // Representative inside temp_root.
        std::fs::write(temp_root.join("a.txt"), b"shared-content").unwrap();
        // Planted symlink: temp_root/evil -> outside (exists, resolves out).
        std::os::unix::fs::symlink(&outside, temp_root.join("evil")).unwrap();

        let id = HardlinkId { dev: 1, ino: 2 };
        let mut group_reps = HashMap::new();
        group_reps.insert(
            id,
            RepInfo {
                rel_path: PathBuf::from("a.txt"),
                size: 14,
                chunks_fp: [0u8; 32],
            },
        );
        let pending = [PendingLink {
            link_rel: PathBuf::from("evil/payload.txt"),
            id,
            mtime: 0,
            uid: 0,
            gid: 0,
        }];

        let mut stats = RestoreStats::default();
        let result = create_hardlinks(&pending, &group_reps, &temp_root, false, false, &mut stats);

        assert!(result.is_err(), "escaping parent must be rejected");
        assert_eq!(stats.hardlinks, 0);
        assert!(
            !outside.join("payload.txt").exists(),
            "nothing may be written through the escaping symlink"
        );
    }

    /// Findings 3 + 4: when `hard_link` fails, the copy fallback produces an
    /// independent inode that (a) carries the representative's mode — including
    /// setgid — and xattrs via the normal finalization path, and (b) is counted
    /// as a `hardlink_copy`, never a `hardlink`.
    #[cfg(unix)]
    #[test]
    fn create_hardlinks_copy_fallback_preserves_mode_and_counts_as_copy() {
        use std::os::unix::fs::MetadataExt;

        let _guard = FaultGuard;
        FAULT_HARD_LINK.with(|c| c.set(true)); // force the copy fallback

        let dest = tempdir().unwrap();
        let temp_root = dest.path().canonicalize().unwrap().join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();
        let rep_abs = temp_root.join("a.txt");
        std::fs::write(&rep_abs, b"shared-content").unwrap();
        // Representative carries a distinctive mode (setgid + 0o750).
        let rep_mode = 0o2750;
        fs::apply_mode(&rep_abs, rep_mode).unwrap();
        // Best-effort xattr on the representative; only asserted if it stuck
        // (tmpfs/overlay in CI may not support user xattrs).
        let xattr_ok = xattr::set(&rep_abs, "user.vykar_test", b"v1").is_ok();

        let id = HardlinkId { dev: 1, ino: 2 };
        let mut group_reps = HashMap::new();
        group_reps.insert(
            id,
            RepInfo {
                rel_path: PathBuf::from("a.txt"),
                size: 14,
                chunks_fp: [0u8; 32],
            },
        );
        let pending = [PendingLink {
            link_rel: PathBuf::from("b.txt"),
            id,
            mtime: 0,
            uid: 0,
            gid: 0,
        }];

        let mut stats = RestoreStats::default();
        // xattrs_enabled = true so the copy carries the rep's xattrs.
        let copied =
            create_hardlinks(&pending, &group_reps, &temp_root, true, false, &mut stats).unwrap();

        // Counted as a copy, not a hard link.
        assert_eq!(stats.hardlink_copies, 1);
        assert_eq!(stats.hardlinks, 0);
        // Finding 3: copied bytes are returned so the caller can add them to
        // total_bytes (a true hard link would return 0).
        assert_eq!(copied, b"shared-content".len() as u64);

        let link_abs = temp_root.join("b.txt");
        let rep_meta = std::fs::metadata(&rep_abs).unwrap();
        let link_meta = std::fs::metadata(&link_abs).unwrap();
        // Separate inode (a real copy, not a shared one).
        assert_ne!(rep_meta.ino(), link_meta.ino(), "copy must be a new inode");
        // Same content.
        assert_eq!(std::fs::read(&link_abs).unwrap(), b"shared-content");
        // Mode (incl. setgid) carried over.
        assert_eq!(
            link_meta.mode() & 0o7777,
            rep_mode,
            "copy must carry the representative's mode (setgid preserved)"
        );
        if xattr_ok {
            assert_eq!(
                xattr::get(&link_abs, "user.vykar_test").unwrap().as_deref(),
                Some(&b"v1"[..]),
                "copy must carry the representative's xattrs"
            );
        }
    }

    /// Finding 1: a symlink planted at the link's *exact* path must not be
    /// followed by the copy fallback. `hard_link` is forced to fail so the copy
    /// path runs; the pre-existing symlink (pointing at an external file) must
    /// be unlinked first and the copy created fresh — the external file is left
    /// untouched and the link path becomes a regular file with the rep's bytes.
    #[cfg(unix)]
    #[test]
    fn create_hardlinks_copy_fallback_does_not_follow_symlink_at_dest() {
        let _guard = FaultGuard;
        FAULT_HARD_LINK.with(|c| c.set(true)); // force the copy fallback

        let dest = tempdir().unwrap();
        let base = dest.path().canonicalize().unwrap();
        let temp_root = base.join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();

        // External file the planted symlink targets — must survive untouched.
        let outside = base.join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        let secret = outside.join("secret.txt");
        std::fs::write(&secret, b"DO-NOT-OVERWRITE").unwrap();

        // Representative content + a symlink planted at the link's own path.
        std::fs::write(temp_root.join("a.txt"), b"rep-content").unwrap();
        std::os::unix::fs::symlink(&secret, temp_root.join("b.txt")).unwrap();

        let id = HardlinkId { dev: 1, ino: 2 };
        let mut group_reps = HashMap::new();
        group_reps.insert(
            id,
            RepInfo {
                rel_path: PathBuf::from("a.txt"),
                size: 11,
                chunks_fp: [0u8; 32],
            },
        );
        let pending = [PendingLink {
            link_rel: PathBuf::from("b.txt"),
            id,
            mtime: 0,
            uid: 0,
            gid: 0,
        }];

        let mut stats = RestoreStats::default();
        create_hardlinks(&pending, &group_reps, &temp_root, false, false, &mut stats).unwrap();

        // The external file was never written through the symlink.
        assert_eq!(std::fs::read(&secret).unwrap(), b"DO-NOT-OVERWRITE");
        // The link path is now a real regular file (not a symlink) with the
        // representative's content.
        let link_abs = temp_root.join("b.txt");
        assert!(
            !std::fs::symlink_metadata(&link_abs)
                .unwrap()
                .file_type()
                .is_symlink(),
            "planted symlink must have been replaced by a regular file"
        );
        assert_eq!(std::fs::read(&link_abs).unwrap(), b"rep-content");
        assert_eq!(stats.hardlink_copies, 1);
    }

    /// Finding (representative replacement): a malformed snapshot replaces the
    /// representative's path with a symlink to an external file before relinking.
    /// `create_hardlinks` must refuse — never following the symlink to copy host
    /// data, never linking it — and leave the external file untouched.
    #[cfg(unix)]
    #[test]
    fn create_hardlinks_rejects_symlink_replaced_representative() {
        let dest = tempdir().unwrap();
        let base = dest.path().canonicalize().unwrap();
        let temp_root = base.join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();

        // External file the (replaced) representative now points at.
        let outside = base.join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        let secret = outside.join("secret.txt");
        std::fs::write(&secret, b"HOST-SECRET").unwrap();

        // The representative path is a symlink, not the regular file we planned.
        std::os::unix::fs::symlink(&secret, temp_root.join("a.txt")).unwrap();

        let id = HardlinkId { dev: 1, ino: 2 };
        let mut group_reps = HashMap::new();
        group_reps.insert(
            id,
            RepInfo {
                rel_path: PathBuf::from("a.txt"),
                size: 11,
                chunks_fp: [0u8; 32],
            },
        );
        let pending = [PendingLink {
            link_rel: PathBuf::from("b.txt"),
            id,
            mtime: 0,
            uid: 0,
            gid: 0,
        }];

        let mut stats = RestoreStats::default();
        let err = create_hardlinks(&pending, &group_reps, &temp_root, false, false, &mut stats)
            .unwrap_err()
            .to_string();

        assert!(err.contains("was replaced before relinking"), "got: {err}");
        // External data was neither read into a copy nor linked.
        assert_eq!(std::fs::read(&secret).unwrap(), b"HOST-SECRET");
        assert!(
            !temp_root.join("b.txt").exists(),
            "no link/copy may be produced from a replaced representative"
        );
        assert_eq!(stats.hardlinks, 0);
        assert_eq!(stats.hardlink_copies, 0);
    }

    #[test]
    fn split_unix_nanos_handles_negative_values() {
        let (secs, nanos) = split_unix_nanos(-1);
        assert_eq!(secs, -1);
        assert_eq!(nanos, 999_999_999);
    }

    #[test]
    fn split_unix_nanos_handles_positive_values() {
        let (secs, nanos) = split_unix_nanos(1_500_000_000);
        assert_eq!(secs, 1);
        assert_eq!(nanos, 500_000_000);
    }

    // -----------------------------------------------------------------------
    // Metadata ordering + ownership args (recorder seam, runs unprivileged).
    // -----------------------------------------------------------------------

    fn recorded_ops() -> Vec<(&'static str, u32, u32)> {
        METADATA_OPS.with(|c| c.borrow().clone())
    }

    fn recorded_tags() -> Vec<&'static str> {
        recorded_ops().into_iter().map(|(t, _, _)| t).collect()
    }

    #[test]
    fn apply_file_metadata_root_order_and_chown_args() {
        let _guard = FaultGuard;
        METADATA_OPS.with(|c| c.borrow_mut().clear());

        let dest = tempdir().unwrap();
        let temp_root = dest.path().join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();
        std::fs::write(temp_root.join("f.txt"), b"data").unwrap();

        let mut pf = planned("f.txt", 4);
        pf.uid = 4242;
        pf.gid = 8484;
        let files = [pf];

        let mut stats = RestoreStats::default();
        // restore_as_root = true exercises the privileged branch even though the
        // test process is unprivileged (fchown fails → warning, op recorded).
        apply_file_metadata(&files, &temp_root, false, true, &mut stats).unwrap();

        assert_eq!(recorded_tags(), vec!["chown", "xattr", "chmod", "mtime"]);
        let chown = recorded_ops()
            .into_iter()
            .find(|(t, _, _)| *t == "chown")
            .unwrap();
        assert_eq!((chown.1, chown.2), (4242, 8484));
    }

    #[test]
    fn apply_dir_metadata_root_order_and_chown_args() {
        let _guard = FaultGuard;
        METADATA_OPS.with(|c| c.borrow_mut().clear());

        let dest = tempdir().unwrap();
        let d = dest.path().join("d");
        std::fs::create_dir_all(&d).unwrap();

        let mut dirs = vec![PlannedNode {
            path: d,
            mode: 0o755,
            mtime: 0,
            uid: 11,
            gid: 22,
            xattrs: None,
        }];
        let mut stats = RestoreStats::default();
        apply_dir_metadata(&mut dirs, true, &mut stats);

        assert_eq!(recorded_tags(), vec!["chown", "xattr", "chmod", "mtime"]);
        let chown = recorded_ops()
            .into_iter()
            .find(|(t, _, _)| *t == "chown")
            .unwrap();
        assert_eq!((chown.1, chown.2), (11, 22));
    }

    #[cfg(unix)]
    #[test]
    fn apply_symlink_metadata_root_order_and_chown_args() {
        let _guard = FaultGuard;
        METADATA_OPS.with(|c| c.borrow_mut().clear());

        let dest = tempdir().unwrap();
        let link = dest.path().join("l");
        std::os::unix::fs::symlink("target", &link).unwrap();

        let syms = vec![PlannedNode {
            path: link,
            mode: 0,
            mtime: 0,
            uid: 33,
            gid: 44,
            xattrs: None,
        }];
        let mut stats = RestoreStats::default();
        apply_symlink_metadata(&syms, true, &mut stats);

        // No chmod for symlinks.
        assert_eq!(recorded_tags(), vec!["lchown", "xattr", "mtime"]);
        let chown = recorded_ops()
            .into_iter()
            .find(|(t, _, _)| *t == "lchown")
            .unwrap();
        assert_eq!((chown.1, chown.2), (33, 44));
    }

    #[test]
    fn apply_dir_metadata_sorts_deepest_first() {
        let _guard = FaultGuard;
        let dest = tempdir().unwrap();
        // Build nested staging dirs so apply_mode/utimes don't error.
        std::fs::create_dir_all(dest.path().join("a/b/c")).unwrap();

        let node = |rel: &str| PlannedNode {
            path: dest.path().join(rel),
            mode: 0o755,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        };
        // Intentionally shallow-first input order.
        let mut dirs = vec![node("a"), node("a/b"), node("a/b/c")];
        let mut stats = RestoreStats::default();
        apply_dir_metadata(&mut dirs, false, &mut stats);

        let order: Vec<_> = dirs.iter().map(|d| d.path.clone()).collect();
        assert_eq!(order[0], dest.path().join("a/b/c"));
        assert_eq!(order[2], dest.path().join("a"));
    }

    // -----------------------------------------------------------------------
    // Umask-robust reopen (the PermissionDenied → chmod 0o600 → retry branch).
    // -----------------------------------------------------------------------

    #[cfg(unix)]
    #[test]
    fn apply_file_metadata_recovers_from_permission_denied_reopen() {
        use std::collections::HashMap;
        use std::os::unix::fs::PermissionsExt;

        let _guard = FaultGuard;
        let dest = tempdir().unwrap();
        let temp_root = dest.path().join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();
        let fpath = temp_root.join("f.txt");
        std::fs::write(&fpath, b"data").unwrap();
        // Stage the file at a genuinely owner-unwritable mode (simulating a
        // restrictive-umask create). This makes the recovery `chmod 0o600`
        // load-bearing: without it, the *retry* open would hit the real 0o400
        // file and fail EACCES on a non-root process — so the test would only
        // pass because the recovery chmod actually grants owner-write.
        std::fs::set_permissions(&fpath, std::fs::Permissions::from_mode(0o400)).unwrap();

        // Reset the call counter — earlier (unguarded) tests on a reused thread
        // may have incremented it.
        OPEN_WRITABLE_CALLS.with(|c| c.set(0));
        // Force the FIRST open to PermissionDenied (the umask outcome we cannot
        // reproduce without mutating the global umask); the retry (call 1) opens
        // the real file, which only succeeds because recovery chmod'd it 0o600.
        OPEN_WRITABLE_FAULT.with(|c| c.set(Some((0, io::ErrorKind::PermissionDenied))));

        // Probe whether this fs accepts a user.* xattr; if so we assert it lands.
        let xattr_ok = {
            let probe = temp_root.join(".probe");
            std::fs::write(&probe, b"p").unwrap();
            let ok = xattr::set(&probe, "user.vykar.test", b"1").is_ok();
            let _ = std::fs::remove_file(&probe);
            ok
        };

        let mut pf = planned("f.txt", 4);
        pf.mode = 0o400; // captured final mode is read-only
        let mut x = HashMap::new();
        x.insert("user.vykar.test".to_string(), b"v".to_vec());
        pf.xattrs = Some(x);
        let files = [pf];

        let mut stats = RestoreStats::default();
        // (1) reopen recovers → Ok.
        apply_file_metadata(&files, &temp_root, true, false, &mut stats).unwrap();
        assert_eq!(
            OPEN_WRITABLE_CALLS.with(|c| c.get()),
            2,
            "expected one retry"
        );

        // (3) final captured mode is applied (not the 0o600 staging mode).
        let mode = std::fs::metadata(&fpath).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o400);

        // (2) xattr restored — proves staging to 0o600 let setxattr succeed.
        if xattr_ok {
            assert_eq!(
                xattr::get(&fpath, "user.vykar.test").unwrap(),
                Some(b"v".to_vec())
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn apply_file_metadata_non_permission_open_error_not_retried() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = FaultGuard;
        let dest = tempdir().unwrap();
        let temp_root = dest.path().join(temp_name());
        std::fs::create_dir_all(&temp_root).unwrap();
        let fpath = temp_root.join("f.txt");
        std::fs::write(&fpath, b"data").unwrap();
        // A distinctive mode to detect any staging chmod (which would set 0o600).
        std::fs::set_permissions(&fpath, std::fs::Permissions::from_mode(0o642)).unwrap();

        OPEN_WRITABLE_CALLS.with(|c| c.set(0));
        // (4) Non-PermissionDenied error on the first open must NOT be retried.
        OPEN_WRITABLE_FAULT.with(|c| c.set(Some((0, io::ErrorKind::NotFound))));

        let files = [planned("f.txt", 4)];
        let mut stats = RestoreStats::default();
        let err = apply_file_metadata(&files, &temp_root, false, false, &mut stats).unwrap_err();
        assert!(err.to_string().contains("for fsync"), "got: {err}");
        // Open attempted exactly once.
        assert_eq!(OPEN_WRITABLE_CALLS.with(|c| c.get()), 1);
        // No staging chmod occurred (mode unchanged).
        let mode = std::fs::metadata(&fpath).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o642);
    }
}
