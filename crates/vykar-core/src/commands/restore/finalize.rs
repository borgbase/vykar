//! Phase 5a (rename) and 5b (chmod/utimes/xattrs): apply file metadata in the
//! temp restore tree per batch, then rename top-level entries into the final
//! destination once all batches have been written.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use crate::platform::fs;
use vykar_types::error::{Result, VykarError};

use super::plan::PlannedFile;
use super::{apply_item_xattrs, warn_metadata_err, RestoreStats};

#[cfg(test)]
use std::cell::Cell;

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
    stats: &mut RestoreStats,
) -> Result<()> {
    for pf in planned_files {
        let target_path = temp_root.join(&pf.rel_path);
        // xattrs remain path-based (no fd-based xattr API in std).
        if xattrs_enabled {
            apply_item_xattrs(&target_path, pf.xattrs.as_ref(), stats);
        }
        let (mtime_secs, mtime_nanos) = split_unix_nanos(pf.mtime);

        // Open a writable handle on every platform — needed because the final
        // `sync_all` (`FlushFileBuffers` on Windows) requires write access, and
        // opening *before* mode application keeps a to-be-read-only file
        // writable at open time. A failure here is a hard error: we cannot make
        // the data durable without it.
        let file = OpenOptions::new()
            .write(true)
            .open(&target_path)
            .map_err(|e| {
                VykarError::Other(format!(
                    "failed to open {} for fsync: {e}",
                    target_path.display()
                ))
            })?;

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

        // F3-a: make file data + inode (size/mode/mtime) durable. `sync_all`
        // (not `sync_data`) flushes the metadata applied above with the data.
        inj_fsync_file(&file).map_err(|e| {
            VykarError::Other(format!("failed to fsync {}: {e}", target_path.display()))
        })?;
    }
    Ok(())
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

fn inj_rollback_rename(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(test)]
    if FAULT_ROLLBACK_RENAME.with(|c| c.get()) {
        return Err(io::Error::other("injected rollback rename failure"));
    }
    std::fs::rename(from, to)
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
    static FAULT_FORWARD_RENAME: Cell<Option<usize>> = const { Cell::new(None) };
    static FAULT_ROLLBACK_RENAME: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
struct FaultGuard;

#[cfg(test)]
impl Drop for FaultGuard {
    fn drop(&mut self) {
        FAULT_FILE_FSYNC.with(|c| c.set(false));
        FAULT_DIR_FSYNC.with(|c| c.set(false));
        FAULT_FORWARD_RENAME.with(|c| c.set(None));
        FAULT_ROLLBACK_RENAME.with(|c| c.set(false));
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
        if let Err(e) = apply_file_metadata(files, temp_root, false, &mut stats) {
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
}
