//! Temp files for atomic writes, and the sweep that removes their debris.
//!
//! PUT and repack write to `.tmp.*` files and rename them into place. RAII
//! (`tempfile::TempPath`) unlinks them on every in-process failure, including
//! a cancelled handler future; what remains is debris from SIGKILL / power
//! loss. The sweep removes that debris during the directory walk the server
//! already does for quota accounting.

use std::path::Path;
use std::time::Duration;

use tracing::{info, warn};

/// Create an RAII temp file in `dir` for an atomic write. The `TempPath`
/// unlinks on drop; `persist` it to commit.
///
/// One synchronous `open(O_CREAT|O_EXCL)` so the file and its owner come into
/// existence together — an async create could be cancelled with the open still
/// in flight on the blocking pool, leaking the file. Random names also rule
/// out two server processes on one `data_dir` truncating each other's upload.
///
/// Mode is `0666 & !umask`, what `File::create` gives, so committed objects
/// keep the permissions the server has always written (tempfile's own default
/// is `0600`).
pub(crate) fn create_temp_in(dir: &Path) -> std::io::Result<(std::fs::File, tempfile::TempPath)> {
    let mut builder = tempfile::Builder::new();
    builder
        .prefix(vykar_protocol::TEMP_FILE_PREFIX)
        .rand_bytes(16);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o666));
    }
    Ok(builder.tempfile_in(dir)?.into_parts())
}

/// A temp file older than this (by mtime) is debris and is removed.
///
/// 24 h is far beyond any plausible single-object PUT (a 128 MiB pack at
/// 1 Mbit/s is ~17 min) and still self-heals daily. It also keeps the sweep
/// safe against a second server process on the same `data_dir` (rolling
/// restart, accidental double start): its in-flight uploads are fresh.
///
/// The gate is a heuristic, not ownership. A SIGSTOPped server or a stalled
/// upload can hold a >24 h-old temp file. If it is swept anyway: on Unix the
/// writer keeps writing to the unlinked inode and its final `persist` fails
/// with NotFound, so the PUT returns 5xx and the client retries; on Windows
/// the unlink fails with a sharing violation, is logged, and is retried next
/// sweep. Either way a retried upload, never a half-written committed object.
pub(crate) const TEMP_DEBRIS_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// Interval of the background rescan/sweep. Hourly against a 24 h threshold
/// is plenty, and the walk is the same one every completed backup triggers.
pub(crate) const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct SweepStats {
    pub files: u64,
    pub bytes: u64,
}

/// One recursive walk that sums regular-file sizes and, when
/// `sweep_older_than` is `Some`, removes aged temp debris on the way.
///
/// Returns `(usage_bytes, swept)`. Only bytes whose `remove_file` succeeded
/// are excluded from usage; a failed deletion is logged and stays counted.
/// Symlinks are neither followed nor counted — a link inside `data_dir` must
/// not send the sweeper outside the repository.
pub(crate) fn scan_usage(data_dir: &Path, sweep_older_than: Option<Duration>) -> (u64, SweepStats) {
    let mut usage = 0u64;
    let mut swept = SweepStats::default();
    walk(data_dir, sweep_older_than, &mut usage, &mut swept);
    if swept.files > 0 {
        info!(
            files = swept.files,
            bytes = swept.bytes,
            "removed aged temp-file debris"
        );
    }
    (usage, swept)
}

fn walk(dir: &Path, sweep_older_than: Option<Duration>, usage: &mut u64, swept: &mut SweepStats) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        // `file_type()` does not follow symlinks (unlike `Path::is_dir`).
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if file_type.is_dir() {
            walk(&path, sweep_older_than, usage, swept);
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let len = meta.len();

        if let Some(max_age) = sweep_older_than {
            let name = entry.file_name();
            if vykar_protocol::is_temp_file(&name.to_string_lossy())
                && is_older_than(&meta, max_age)
            {
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        swept.files += 1;
                        swept.bytes += len;
                        continue;
                    }
                    Err(e) => {
                        warn!(path = %path.display(), "failed to remove temp-file debris: {e}");
                    }
                }
            }
        }
        *usage += len;
    }
}

/// mtime-based, so a slow-but-live upload (which touches mtime on every
/// write) measures time since its last byte. A future mtime (clock skew)
/// yields `Err` from `elapsed()` and counts as not aged.
fn is_older_than(meta: &std::fs::Metadata, max_age: Duration) -> bool {
    meta.modified()
        .and_then(|m| m.elapsed().map_err(std::io::Error::other))
        .is_ok_and(|age| age > max_age)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    /// Write `data` and set its mtime. One write-mode handle for both: on
    /// Windows `SetFileTime` needs `FILE_WRITE_ATTRIBUTES`, which a read-only
    /// `File::open` handle lacks.
    fn write_with_mtime(path: &Path, data: &[u8], mtime: SystemTime) {
        use std::io::Write;
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(data).unwrap();
        f.set_modified(mtime).unwrap();
    }

    fn write_aged(path: &Path, data: &[u8], age: Duration) {
        write_with_mtime(path, data, SystemTime::now() - age);
    }

    fn make_tree(root: &Path) {
        std::fs::create_dir_all(root.join("packs/ab")).unwrap();
        std::fs::write(root.join("config"), vec![1u8; 100]).unwrap();
        std::fs::write(root.join("packs/ab").join("a".repeat(64)), vec![2u8; 1000]).unwrap();
    }

    #[test]
    fn sweeps_aged_temps_and_excludes_them_from_usage() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        make_tree(root);
        let old_a = root.join("packs/ab/.tmp.deadbeef.0");
        let old_b = root.join("packs/ab/.repack_tmp.legacy");
        let fresh = root.join("packs/ab/.tmp.fresh");
        write_aged(&old_a, &[0u8; 300], Duration::from_secs(25 * 3600));
        write_aged(&old_b, &[0u8; 200], Duration::from_secs(25 * 3600));
        std::fs::write(&fresh, [0u8; 50]).unwrap();

        let (usage, swept) = scan_usage(root, Some(TEMP_DEBRIS_MAX_AGE));

        assert!(!old_a.exists(), "aged .tmp. debris removed");
        assert!(!old_b.exists(), "aged legacy debris removed");
        assert!(fresh.exists(), "fresh temp survives");
        assert!(root.join("config").exists());
        assert!(root.join("packs/ab").join("a".repeat(64)).exists());
        assert_eq!(
            swept,
            SweepStats {
                files: 2,
                bytes: 500
            }
        );
        assert_eq!(
            usage,
            100 + 1000 + 50,
            "swept bytes excluded, fresh temp counted"
        );
    }

    #[cfg(unix)]
    #[test]
    fn temp_file_has_file_create_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let (_f, temp_path) = create_temp_in(tmp.path()).unwrap();
        let reference = tmp.path().join("reference");
        std::fs::File::create(&reference).unwrap();

        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&temp_path), mode(&reference));
        assert!(temp_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(vykar_protocol::TEMP_FILE_PREFIX));
    }

    #[test]
    fn none_is_pure_dir_size() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        make_tree(root);
        let old = root.join("packs/ab/.tmp.deadbeef.0");
        write_aged(&old, &[0u8; 300], Duration::from_secs(25 * 3600));

        let (usage, swept) = scan_usage(root, None);

        assert!(old.exists(), "no sweep without a threshold");
        assert_eq!(swept, SweepStats::default());
        assert_eq!(usage, 1400);
    }

    #[test]
    fn future_mtime_is_not_aged() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        make_tree(root);
        let skewed = root.join("packs/ab/.tmp.skewed");
        write_with_mtime(
            &skewed,
            &[0u8; 10],
            SystemTime::now() + Duration::from_secs(48 * 3600),
        );

        let (usage, swept) = scan_usage(root, Some(TEMP_DEBRIS_MAX_AGE));
        assert!(skewed.exists());
        assert_eq!(swept.files, 0);
        assert_eq!(usage, 1110);
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_directory_is_not_followed() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("repo");
        make_tree(&root);
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        let victim = outside.join(".tmp.x");
        write_aged(&victim, &[0u8; 777], Duration::from_secs(25 * 3600));
        std::os::unix::fs::symlink(&outside, root.join("packs/zz")).unwrap();

        let (usage, swept) = scan_usage(&root, Some(TEMP_DEBRIS_MAX_AGE));

        assert!(victim.exists(), "file outside the tree must survive");
        assert_eq!(swept.files, 0);
        assert_eq!(usage, 1100, "symlinked content not counted");
    }

    #[cfg(unix)]
    #[test]
    fn failed_deletion_stays_counted() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        make_tree(root);
        let shard = root.join("packs/ab");
        let old = shard.join(".tmp.stuck");
        write_aged(&old, &[0u8; 300], Duration::from_secs(25 * 3600));
        std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o555)).unwrap();

        // Root bypasses directory mode bits, so the setup cannot make
        // `remove_file` fail there. Probe rather than check the uid (libc's
        // `geteuid` is unsafe and the workspace denies unsafe code).
        if std::fs::File::create(shard.join("probe")).is_ok() {
            std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o755)).unwrap();
            eprintln!("skipping: directory permissions are not enforced for this user");
            return;
        }

        let (usage, swept) = scan_usage(root, Some(TEMP_DEBRIS_MAX_AGE));

        // Restore before asserting so tempdir cleanup works even on failure.
        std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(old.exists(), "unremovable file survives");
        assert_eq!(swept.files, 0);
        assert_eq!(usage, 1400, "undeleted debris stays counted");
    }
}
