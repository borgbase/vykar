//! Phase 5a (rename) and 5b (chmod/utimes/xattrs): apply file metadata in the
//! temp restore tree per batch, then rename top-level entries into the final
//! destination once all batches have been written.

use std::path::Path;

use crate::platform::fs;
use vykar_types::error::Result;

use super::plan::PlannedFile;
use super::{apply_item_xattrs, warn_metadata_err, RestoreStats};

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
) {
    for pf in planned_files {
        let target_path = temp_root.join(&pf.rel_path);
        // xattrs remain path-based (no fd-based xattr API in std).
        if xattrs_enabled {
            apply_item_xattrs(&target_path, pf.xattrs.as_ref(), stats);
        }
        let (mtime_secs, mtime_nanos) = split_unix_nanos(pf.mtime);
        // fd-based fchmod/futimens are Unix-only; on other platforms
        // fall through to the path-based calls to avoid silent no-ops.
        // Only the *final* failure (after the path-based fallback) is
        // recorded as a warning — intermediate fd failures that succeed
        // on the fallback are not user-facing.
        #[cfg(unix)]
        {
            if let Ok(file) = std::fs::File::open(&target_path) {
                let mode_res = fs::apply_mode_fd(&file, pf.mode);
                if mode_res.is_err() {
                    warn_metadata_err(
                        stats,
                        fs::apply_mode(&target_path, pf.mode),
                        &target_path,
                        "mode",
                    );
                }
                let mtime_res = fs::set_file_mtime_fd(&file, mtime_secs, mtime_nanos);
                if mtime_res.is_err() {
                    warn_metadata_err(
                        stats,
                        fs::set_file_mtime(&target_path, mtime_secs, mtime_nanos),
                        &target_path,
                        "mtime",
                    );
                }
            } else {
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
    }
}

/// Phase 5a: move all top-level entries from `temp_root` to `dest_root`,
/// then remove the (now empty) temp root.
pub(super) fn move_temp_to_dest(temp_root: &Path, dest_root: &Path) -> Result<()> {
    for entry in std::fs::read_dir(temp_root)? {
        let entry = entry?;
        let final_path = dest_root.join(entry.file_name());
        std::fs::rename(entry.path(), &final_path)?;
    }
    std::fs::remove_dir(temp_root)?; // now empty
    Ok(())
}

fn split_unix_nanos(total_nanos: i64) -> (i64, u32) {
    let secs = total_nanos.div_euclid(1_000_000_000);
    let nanos = total_nanos.rem_euclid(1_000_000_000) as u32;
    (secs, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

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
