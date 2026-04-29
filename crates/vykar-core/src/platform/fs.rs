use std::fs::{FileType, Metadata};
use std::path::Path;

#[derive(Debug, Clone, Copy)]
pub struct MetadataSummary {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    /// macOS-only: file is a FileProvider dataless placeholder (iCloud Drive,
    /// Dropbox, OneDrive, etc.). `read()` would trigger asynchronous hydration
    /// and metadata churn, so the backup walker skips or reuses-from-parent
    /// instead. Always `false` on non-macOS platforms.
    pub is_dataless: bool,
}

/// Stat an open file descriptor and build a `MetadataSummary`.
pub fn fstat_summary(file: &std::fs::File) -> std::io::Result<MetadataSummary> {
    let meta = file.metadata()?;
    let ft = meta.file_type();
    Ok(summarize_metadata(&meta, &ft))
}

/// True iff both summaries identify the same file content & identity.
///
/// Used for walk-vs-open and pre-vs-post drift checks during backup.
/// Compares `size`, `mtime_ns`, `ctime_ns`, `device`, and `inode`. On
/// Windows `device`/`inode` are always `0` (see [`summarize_metadata`]),
/// so the rename-atop guard is effectively Unix-only.
///
/// `is_dataless` is intentionally **not** part of this check — drift
/// detection is orthogonal to the dataless classification, which is
/// resolved separately at walk time.
pub fn metadata_matches(a: &MetadataSummary, b: &MetadataSummary) -> bool {
    a.size == b.size
        && a.mtime_ns == b.mtime_ns
        && a.ctime_ns == b.ctime_ns
        && a.device == b.device
        && a.inode == b.inode
}

pub fn summarize_metadata(metadata: &Metadata, file_type: &FileType) -> MetadataSummary {
    #[cfg(unix)]
    {
        let _ = file_type;
        use std::os::unix::fs::MetadataExt;

        let is_dataless = {
            #[cfg(target_os = "macos")]
            {
                use std::os::macos::fs::MetadataExt as MacExt;
                // SF_DATALESS (BSD): inode is a FileProvider placeholder
                // (iCloud Drive, Dropbox, OneDrive, etc.). Reading the file
                // would trigger asynchronous hydration via `fileproviderd`.
                const SF_DATALESS: u32 = 0x40000000;
                (MacExt::st_flags(metadata) & SF_DATALESS) != 0
            }
            #[cfg(not(target_os = "macos"))]
            {
                false
            }
        };

        MetadataSummary {
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            mtime_ns: metadata.mtime() * 1_000_000_000 + metadata.mtime_nsec(),
            ctime_ns: metadata.ctime() * 1_000_000_000 + metadata.ctime_nsec(),
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.len(),
            is_dataless,
        }
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        let readonly = metadata.permissions().readonly();
        let mode = if file_type.is_dir() {
            if readonly {
                0o555
            } else {
                0o755
            }
        } else if readonly {
            0o444
        } else {
            0o644
        };

        MetadataSummary {
            mode,
            uid: 0,
            gid: 0,
            mtime_ns: windows_filetime_to_unix_ns(metadata.last_write_time()),
            ctime_ns: windows_filetime_to_unix_ns(metadata.creation_time()),
            device: 0,
            inode: 0,
            size: metadata.file_size(),
            is_dataless: false,
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        MetadataSummary {
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            device: 0,
            inode: 0,
            size: metadata.len(),
            is_dataless: false,
        }
    }
}

pub fn apply_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
    }

    #[cfg(windows)]
    {
        let mut perms = std::fs::metadata(path)?.permissions();
        perms.set_readonly((mode & 0o200) == 0);
        std::fs::set_permissions(path, perms)
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        let _ = mode;
        Ok(())
    }
}

/// Apply file permissions via an open file descriptor (avoids path lookup).
pub fn apply_mode_fd(file: &std::fs::File, mode: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let ret = unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) };
        if ret == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (file, mode);
        Ok(())
    }
}

/// Set file mtime via an open file descriptor (avoids path lookup).
pub fn set_file_mtime_fd(file: &std::fs::File, secs: i64, nanos: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let times = [
            libc::timespec {
                tv_sec: 0,
                tv_nsec: libc::UTIME_OMIT,
            },
            libc::timespec {
                tv_sec: secs as _,
                tv_nsec: nanos as _,
            },
        ];
        let ret = unsafe { libc::futimens(file.as_raw_fd(), times.as_ptr()) };
        if ret == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(windows)]
    {
        use std::fs::FileTimes;
        use std::time::{Duration, SystemTime};

        let time = if secs >= 0 {
            SystemTime::UNIX_EPOCH + Duration::new(secs as u64, nanos)
        } else {
            SystemTime::UNIX_EPOCH - Duration::new(secs.unsigned_abs(), 0) + Duration::new(0, nanos)
        };
        file.set_times(FileTimes::new().set_modified(time))
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, secs, nanos);
        Ok(())
    }
}

pub fn create_symlink(link_target: &Path, target: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(link_target, target)
    }

    #[cfg(windows)]
    {
        let file_err = std::os::windows::fs::symlink_file(link_target, target).err();
        if file_err.is_none() {
            return Ok(());
        }

        match std::os::windows::fs::symlink_dir(link_target, target) {
            Ok(()) => Ok(()),
            Err(dir_err) => Err(std::io::Error::new(
                dir_err.kind(),
                format!(
                    "failed to create symlink as file ({}) and directory ({})",
                    file_err.unwrap(),
                    dir_err
                ),
            )),
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = link_target;
        let _ = target;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "symlink creation is not supported on this platform",
        ))
    }
}

pub fn set_file_mtime(path: &Path, secs: i64, nanos: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contains null")
        })?;
        let times = [
            libc::timespec {
                tv_sec: 0,
                tv_nsec: libc::UTIME_OMIT,
            },
            libc::timespec {
                tv_sec: secs as _,
                tv_nsec: nanos as _,
            },
        ];
        if unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) } == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(windows)]
    {
        use std::fs::{FileTimes, OpenOptions};
        use std::os::windows::fs::OpenOptionsExt;
        use std::time::{Duration, SystemTime};

        const FILE_WRITE_ATTRIBUTES: u32 = 0x0100;
        let time = if secs >= 0 {
            SystemTime::UNIX_EPOCH + Duration::new(secs as u64, nanos)
        } else {
            SystemTime::UNIX_EPOCH - Duration::new(secs.unsigned_abs(), 0) + Duration::new(0, nanos)
        };
        let file = OpenOptions::new()
            .access_mode(FILE_WRITE_ATTRIBUTES)
            .open(path)?;
        file.set_times(FileTimes::new().set_modified(time))
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, secs, nanos);
        Ok(())
    }
}

pub fn xattrs_supported() -> bool {
    cfg!(unix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    #[test]
    fn set_file_mtime_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.txt");
        std::fs::write(&path, b"hello").unwrap();

        let target_secs: i64 = 1_700_000_000;
        set_file_mtime(&path, target_secs, 0).unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        let mtime = meta.modified().unwrap();
        let since_epoch = mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let diff = (since_epoch.as_secs() as i64 - target_secs).unsigned_abs();
        assert!(diff <= 1, "mtime off by {diff} seconds");
    }

    #[test]
    fn set_file_mtime_on_readonly_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("readonly.txt");
        std::fs::write(&path, b"data").unwrap();

        // Make file read-only
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&path, perms).unwrap();

        let target_secs: i64 = 1_600_000_000;
        // Both Unix (utimensat is path-based) and Windows (FILE_WRITE_ATTRIBUTES)
        // should succeed on read-only files.
        set_file_mtime(&path, target_secs, 0).unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        let mtime = meta.modified().unwrap();
        let since_epoch = mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let diff = (since_epoch.as_secs() as i64 - target_secs).unsigned_abs();
        assert!(diff <= 1, "mtime off by {diff} seconds");
    }

    #[test]
    fn apply_mode_fd_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mode_fd.txt");
        std::fs::write(&path, b"data").unwrap();

        let file = std::fs::File::open(&path).unwrap();
        apply_mode_fd(&file, 0o755).unwrap();
        drop(file);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o755);
        }
    }

    #[test]
    fn apply_mode_fd_readonly_transition() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ro_fd.txt");
        std::fs::write(&path, b"data").unwrap();

        // Set read-only via path-based first
        apply_mode(&path, 0o444).unwrap();

        // Now use fd-based to set it back to read-write
        let file = std::fs::File::open(&path).unwrap();
        apply_mode_fd(&file, 0o644).unwrap();
        drop(file);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o644);
        }
    }

    #[test]
    fn set_file_mtime_fd_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mtime_fd.txt");
        std::fs::write(&path, b"data").unwrap();

        // Windows needs FILE_WRITE_ATTRIBUTES, so open with write access.
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let target_secs: i64 = 1_700_000_000;
        set_file_mtime_fd(&file, target_secs, 0).unwrap();
        drop(file);

        let meta = std::fs::metadata(&path).unwrap();
        let mtime = meta.modified().unwrap();
        let since_epoch = mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let diff = (since_epoch.as_secs() as i64 - target_secs).unsigned_abs();
        assert!(diff <= 1, "mtime off by {diff} seconds");
    }

    /// `metadata_matches` must catch mutations in every guarded field.
    /// Mutating any one of size / mtime_ns / ctime_ns / device / inode
    /// should return false; the baseline should return true.
    #[test]
    fn metadata_matches_covers_all_fields() {
        let base = MetadataSummary {
            mode: 0o644,
            uid: 100,
            gid: 100,
            mtime_ns: 111,
            ctime_ns: 222,
            device: 333,
            inode: 444,
            size: 555,
            is_dataless: false,
        };
        assert!(metadata_matches(&base, &base));

        let mut m = base;
        m.size = 1;
        assert!(!metadata_matches(&base, &m));

        let mut m = base;
        m.mtime_ns += 1;
        assert!(!metadata_matches(&base, &m));

        let mut m = base;
        m.ctime_ns += 1;
        assert!(!metadata_matches(&base, &m));

        let mut m = base;
        m.device = 0;
        assert!(!metadata_matches(&base, &m));

        let mut m = base;
        m.inode = 0;
        assert!(!metadata_matches(&base, &m));

        // Changing mode/uid/gid alone does NOT count as a content-identity
        // change — by design, only size/mtime/ctime/device/inode are
        // content-identity fields.
        let mut m = base;
        m.mode = 0o777;
        m.uid = 1;
        m.gid = 1;
        assert!(metadata_matches(&base, &m));

        // Flipping is_dataless does NOT break content identity — drift
        // detection is orthogonal to dataless classification.
        let mut m = base;
        m.is_dataless = true;
        assert!(metadata_matches(&base, &m));
    }

    /// Non-dataless metadata produces `is_dataless: false` on every platform.
    /// (Cannot fake `SF_DATALESS` cheaply in a unit test; manual macOS
    /// verification covers the positive case.)
    #[test]
    fn summarize_metadata_marks_normal_files_not_dataless() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.txt");
        std::fs::write(&path, b"hello").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        let ft = meta.file_type();
        let summary = summarize_metadata(&meta, &ft);
        assert!(!summary.is_dataless);
    }

    #[test]
    fn set_file_mtime_negative_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.txt");
        std::fs::write(&path, b"ancient").unwrap();

        // 1969-12-31T23:59:59 UTC — one second before the Unix epoch.
        let target_secs: i64 = -1;
        // Should not panic on any platform.
        let result = set_file_mtime(&path, target_secs, 0);
        // Unix handles negative timestamps natively. Windows SystemTime can
        // represent pre-epoch times, so this should succeed on both.
        assert!(result.is_ok(), "pre-epoch mtime failed: {result:?}");
    }
}

#[cfg(windows)]
fn windows_filetime_to_unix_ns(filetime_100ns: u64) -> i64 {
    // FILETIME epoch is 1601-01-01, Unix epoch is 1970-01-01.
    const EPOCH_OFFSET_100NS: i128 = 11644473600i128 * 10_000_000i128;
    let value_100ns = filetime_100ns as i128 - EPOCH_OFFSET_100NS;
    let nanos = value_100ns.saturating_mul(100);
    nanos.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}
