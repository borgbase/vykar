// libc fchmod/fchown/chown/lchown/geteuid for file mode and ownership writes;
// SAFETY documented per block.
#![allow(unsafe_code)]

use std::ffi::{OsStr, OsString};
use std::fs::{File, FileType, Metadata, OpenOptions};
use std::io;
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
    /// Hard-link count. `1` on Windows and the non-unix fallback (where vykar
    /// treats every file as unlinked). Used by the backup walker to decide
    /// whether a regular file participates in a hard-link group.
    pub nlink: u64,
    pub size: u64,
    /// macOS-only: file is a FileProvider dataless placeholder (iCloud Drive,
    /// Dropbox, OneDrive, etc.). `read()` would trigger asynchronous hydration
    /// and metadata churn, so the backup walker skips or reuses-from-parent
    /// instead. Always `false` on non-macOS platforms.
    pub is_dataless: bool,
}

/// The raw OS bytes of a path when it is **not** valid UTF-8 (Unix only), else
/// `None`. The single home for the "non-UTF8 `Path` → byte shadow" extraction
/// shared by the backup walker (relative paths and symlink targets); the
/// `cfg(unix)` split lives here once rather than being copied per call site.
#[cfg(unix)]
pub(crate) fn non_utf8_bytes(path: &Path) -> Option<Vec<u8>> {
    use std::os::unix::ffi::OsStrExt;
    if path.to_str().is_some() {
        return None;
    }
    Some(path.as_os_str().as_bytes().to_vec())
}

#[cfg(not(unix))]
pub(crate) fn non_utf8_bytes(_path: &Path) -> Option<Vec<u8>> {
    None
}

/// `Path::starts_with` with a byte-level fast path on Unix.
///
/// The backup walk asks this once or more per file with the same few source
/// roots, and the component-wise comparison (both paths re-parsed into
/// `Components` each time) is measurable there. When `path`'s bytes start
/// with `base`'s bytes and the match ends on a separator boundary, every
/// component of `base` is also a component of `path` in the same position,
/// so the answer is `true`; anything else (including `/a/b` vs `/a/bc`, or a
/// `base` with a trailing separator that `path` lacks) is decided by the std
/// comparison. Non-Unix platforms always use the std comparison, keeping
/// their prefix/verbatim-path semantics untouched.
#[inline]
pub(crate) fn path_starts_with(path: &Path, base: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let (p, b) = (path.as_os_str().as_bytes(), base.as_os_str().as_bytes());
        if let Some(rest) = p.strip_prefix(b)
            && (rest.is_empty() || rest.first() == Some(&b'/') || b.last() == Some(&b'/'))
        {
            return true;
        }
    }
    path.starts_with(base)
}

/// `Path::strip_prefix` with a byte-level fast path on Unix.
///
/// Same idea as [`path_starts_with`]. The fast path only answers when the
/// result is unambiguous: the remainder after the separator boundary is
/// returned verbatim, which equals std's result unless it starts with a `.`
/// component or ends in a separator / `.` component (std trims those); those
/// rare shapes, and every non-match, go to `Path::strip_prefix`.
#[inline]
pub(crate) fn strip_path_prefix<'a>(path: &'a Path, base: &Path) -> Option<&'a Path> {
    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let (p, b) = (path.as_os_str().as_bytes(), base.as_os_str().as_bytes());
        // An empty `base` keeps a leading root (`"/a"` minus `""` is `"/a"`),
        // so it is left to std.
        if let (false, Some(mut rest)) = (b.is_empty(), p.strip_prefix(b))
            && (rest.is_empty() || rest.first() == Some(&b'/') || b.last() == Some(&b'/'))
        {
            while let Some(r) = rest.strip_prefix(b"/") {
                rest = r;
            }
            let ambiguous = rest == b"."
                || rest.starts_with(b"./")
                || rest.ends_with(b"/")
                || rest.ends_with(b"/.");
            if !ambiguous {
                return Some(Path::new(OsStr::from_bytes(rest)));
            }
        }
    }
    path.strip_prefix(base).ok()
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
                const SF_DATALESS: u32 = 0x4000_0000;
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
            nlink: metadata.nlink(),
            size: metadata.len(),
            is_dataless,
        }
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        let readonly = metadata.permissions().readonly();
        let mode = if file_type.is_dir() {
            if readonly { 0o555 } else { 0o755 }
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
            nlink: 1,
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
            nlink: 1,
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
        // SAFETY: fd is borrowed from the open `file` and remains valid for
        // this call; mode is a plain integer with no pointer dereference.
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

/// Set file mtime via an open file descriptor (avoids path lookup). Atime is
/// left untouched.
pub fn set_file_mtime_fd(file: &std::fs::File, secs: i64, nanos: u32) -> std::io::Result<()> {
    file.set_times(mtime_only(secs, nanos)?)
}

/// `FileTimes` carrying only the given mtime; std passes `UTIME_OMIT` (or a
/// null pointer on Windows) for the unset atime.
fn mtime_only(secs: i64, nanos: u32) -> std::io::Result<std::fs::FileTimes> {
    use std::time::{Duration, SystemTime};

    let base = if secs >= 0 {
        SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(secs as u64))
    } else {
        SystemTime::UNIX_EPOCH.checked_sub(Duration::from_secs(secs.unsigned_abs()))
    };
    let time = base
        .and_then(|t| t.checked_add(Duration::from_nanos(nanos.into())))
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "mtime out of range")
        })?;
    Ok(std::fs::FileTimes::new().set_modified(time))
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
                    file_err.expect("symlink_file failed before symlink_dir fallback"),
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

/// Create a hard link at `link` pointing to the same inode as `original`.
/// Both paths must live on the same filesystem (the restore caller guarantees
/// this by linking inside its temp root). Returns `Unsupported` on platforms
/// without a hard-link API.
pub fn hard_link(original: &Path, link: &Path) -> std::io::Result<()> {
    #[cfg(any(unix, windows))]
    {
        std::fs::hard_link(original, link)
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (original, link);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "hard links are not supported on this platform",
        ))
    }
}

/// Set a path's mtime, following symlinks. Atime is left untouched.
pub fn set_file_mtime(path: &Path, secs: i64, nanos: u32) -> std::io::Result<()> {
    std::fs::set_times(path, mtime_only(secs, nanos)?)
}

/// Set a symlink's own mtime without following it (`AT_SYMLINK_NOFOLLOW`).
/// No-op on non-Unix platforms.
pub fn set_symlink_mtime(path: &Path, secs: i64, nanos: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::set_times_nofollow(path, mtime_only(secs, nanos)?)
    }

    #[cfg(not(unix))]
    {
        let _ = (path, secs, nanos);
        Ok(())
    }
}

/// True iff the process's effective uid is 0 (root). The tar/restic-style gate
/// for whether restore should attempt to reassign ownership. Always `false` on
/// non-Unix platforms.
pub fn is_effective_root() -> bool {
    #[cfg(unix)]
    {
        // SAFETY: geteuid is always safe — it takes no arguments, reads no
        // memory, and cannot fail.
        unsafe { libc::geteuid() == 0 }
    }

    #[cfg(not(unix))]
    {
        false
    }
}

/// Reassign ownership of an open file via `fchown` (no path lookup, no symlink
/// follow concern). No-op on non-Unix.
pub fn chown_fd(file: &std::fs::File, uid: u32, gid: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: fd is borrowed from the open `file` and valid for the call;
        // uid/gid are plain integers.
        let ret = unsafe { libc::fchown(file.as_raw_fd(), uid as libc::uid_t, gid as libc::gid_t) };
        if ret == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (file, uid, gid);
        Ok(())
    }
}

/// Reassign ownership of a path via `chown` (follows symlinks). Used for
/// directories, where no fd is held. No-op on non-Unix.
pub fn chown_path(path: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contains null")
        })?;
        // SAFETY: `c_path` is a valid NUL-terminated CString owned in this
        // scope; uid/gid are plain integers.
        let ret = unsafe { libc::chown(c_path.as_ptr(), uid as libc::uid_t, gid as libc::gid_t) };
        if ret == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (path, uid, gid);
        Ok(())
    }
}

/// Reassign ownership of a symlink itself via `lchown` (no follow). No-op on
/// non-Unix.
pub fn lchown_path(path: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contains null")
        })?;
        // SAFETY: `c_path` is a valid NUL-terminated CString owned in this
        // scope; uid/gid are plain integers.
        let ret = unsafe { libc::lchown(c_path.as_ptr(), uid as libc::uid_t, gid as libc::gid_t) };
        if ret == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (path, uid, gid);
        Ok(())
    }
}

pub fn xattrs_supported() -> bool {
    cfg!(unix)
}

// ---- Byte paths and separators -------------------------------------------

/// The bytes of an OS string where paths are byte strings (Unix); `None`
/// elsewhere, where vykar records no raw (byte-faithful) names.
#[inline]
pub(crate) fn os_str_bytes(s: &OsStr) -> Option<&[u8]> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Some(s.as_bytes())
    }
    #[cfg(not(unix))]
    {
        let _ = s;
        None
    }
}

/// A path from raw name bytes (Unix); `None` elsewhere.
#[inline]
pub(crate) fn path_from_bytes(b: &[u8]) -> Option<&Path> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Some(Path::new(OsStr::from_bytes(b)))
    }
    #[cfg(not(unix))]
    {
        let _ = b;
        None
    }
}

/// Whether snapshots can carry, and restore can write, byte-faithful
/// non-UTF-8 names (`ItemRawNames`). Unix only.
pub(crate) fn raw_names_supported() -> bool {
    cfg!(unix)
}

/// Whether `\` is an ordinary byte in a path component (Unix). Elsewhere it
/// is a separator, and globset normalizes it in candidate paths.
pub(crate) fn backslash_is_literal() -> bool {
    cfg!(unix)
}

/// Normalize a relative path to always use `/` as the separator.
///
/// On Windows, `Path::to_string_lossy()` produces backslash-separated paths,
/// but the repository format uses `/` exclusively. This function is a no-op
/// on Unix (the compiler elides it), but always compiled so it can be tested
/// on any platform.
#[inline]
pub(crate) fn normalize_rel_path(path: String) -> String {
    if cfg!(windows) {
        path.replace('\\', "/")
    } else {
        path
    }
}

/// An absolute, lexically clean path as a rootless `/`-separated string with
/// no leading or trailing `/`: Unix drops the root `/`; Windows strips a
/// verbatim `\\?\` prefix (dunce), turns `\` into `/`, drops the drive
/// colon and a UNC `//`. May be empty (the caller falls back to a basename).
pub(crate) fn rootless_slash_path(abs_clean: &Path) -> String {
    #[cfg(unix)]
    {
        let s = abs_clean.to_string_lossy();
        s.trim_prefix('/').trim_end_matches('/').to_string()
    }
    #[cfg(windows)]
    {
        // dunce::simplified strips the \\?\ verbatim prefix if present.
        let simplified = dunce::simplified(abs_clean);
        let s = simplified.to_string_lossy().replace('\\', "/");
        // Drive letter: "C:/Users/..." → "C/Users/..."
        // UNC root:    "//server/share/..." → "server/share/..."
        let s = s.replacen(':', "", 1);
        s.trim_start_matches('/').trim_end_matches('/').to_string()
    }
}

// ---- Walk ---------------------------------------------------------------

/// Inode number of a directory entry from `readdir` (Unix); 0 elsewhere.
#[cfg(unix)]
#[inline]
pub(crate) fn dir_entry_inode(entry: &std::fs::DirEntry) -> u64 {
    use std::os::unix::fs::DirEntryExt;
    entry.ino()
}

#[cfg(not(unix))]
#[inline]
pub(crate) fn dir_entry_inode(_entry: &std::fs::DirEntry) -> u64 {
    0
}

/// Returns true if inode-sorted stat reduces disk seeks on this filesystem.
/// Only beneficial on Linux ext4/xfs/reiserfs; returns false on all other platforms.
#[cfg(target_os = "linux")]
pub(crate) fn inode_sort_beneficial(path: &Path) -> bool {
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
    // SAFETY: `libc::statfs` is a C plain-old-data struct of integer fields;
    // an all-zero bit pattern is a valid (if uninitialized) instance and
    // statfs(2) overwrites every field on success.
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c_path` is a valid NUL-terminated CString owned in this scope;
    // `&mut buf` is a unique exclusive reference to a properly-aligned statfs
    // for the kernel to populate.
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
pub(crate) fn inode_sort_beneficial(_path: &Path) -> bool {
    false
}

/// Map a non-directory/file/symlink file type to a human-readable label for
/// the warn-only skip message. On unix, distinguishes socket / FIFO / block
/// device / character device via `FileTypeExt`; everything else (and all
/// non-unix entries, where this branch is effectively unreachable) falls back
/// to a generic label.
#[cfg(unix)]
pub(crate) fn unsupported_file_type_label(file_type: std::fs::FileType) -> &'static str {
    use std::os::unix::fs::FileTypeExt;
    if file_type.is_socket() {
        "socket"
    } else if file_type.is_fifo() {
        "FIFO"
    } else if file_type.is_block_device() {
        "block device"
    } else if file_type.is_char_device() {
        "character device"
    } else {
        "unsupported file type"
    }
}

#[cfg(not(unix))]
pub(crate) fn unsupported_file_type_label(_file_type: std::fs::FileType) -> &'static str {
    "unsupported file type"
}

/// Whether `MetadataSummary::device` carries real device IDs, which
/// `one_file_system` compares. Unix only (0 on Windows, see `summarize_metadata`).
pub(crate) fn device_ids_supported() -> bool {
    cfg!(unix)
}

// ---- Extended attributes (the `xattr` crate; none exist off Unix) --------

/// Names of `path`'s extended attributes (`xattr::list`). Off Unix: none.
pub(crate) fn xattr_list(path: &Path) -> io::Result<impl Iterator<Item = OsString>> {
    #[cfg(unix)]
    {
        ::xattr::list(path)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(std::iter::empty::<OsString>())
    }
}

/// One attribute's value (`xattr::get`). Off Unix: `Ok(None)`.
pub(crate) fn xattr_get(path: &Path, name: &OsStr) -> io::Result<Option<Vec<u8>>> {
    #[cfg(unix)]
    {
        ::xattr::get(path, name)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, name);
        Ok(None)
    }
}

/// Set one attribute (`xattr::set`). Off Unix: a no-op `Ok(())`.
pub(crate) fn xattr_set(path: &Path, name: &str, value: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        ::xattr::set(path, name, value)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, name, value);
        Ok(())
    }
}

// ---- Restore writes -----------------------------------------------------

/// Refuse to follow a symlink at the final component when opening
/// (`O_NOFOLLOW`; the open fails `ELOOP`). No-op off Unix. Sets, not ORs,
/// the custom flags, as the callers did.
#[inline]
pub(crate) fn set_nofollow(opts: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(not(unix))]
    {
        let _ = opts;
    }
}

/// Write all of `buf` at `offset`: `pwrite` (`FileExt::write_all_at`) on
/// Unix; seek then `write_all` elsewhere (moves the file cursor).
#[inline]
pub(crate) fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, offset)
    }
    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = file;
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(buf)
    }
}

/// Apply `mode` to an open regular file: via its fd on Unix (no path lookup,
/// never follows the leaf), via `path` elsewhere (`apply_mode_fd` is a no-op there).
pub(crate) fn apply_mode_open(file: &File, path: &Path, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        let _ = path;
        apply_mode_fd(file, mode)
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        apply_mode(path, mode)
    }
}

/// Set an open regular file's mtime: via its fd on Unix, via `path` elsewhere.
pub(crate) fn set_mtime_open(file: &File, path: &Path, secs: i64, nanos: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        let _ = path;
        set_file_mtime_fd(file, secs, nanos)
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        set_file_mtime(path, secs, nanos)
    }
}

/// Full `st_mode` (file-type bits included) of `path` without following a
/// symlink (Unix); `None` if the lstat fails, and always `None` elsewhere
/// (no syscall).
pub(crate) fn lstat_mode(path: &Path) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::symlink_metadata(path).ok().map(|m| m.mode())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// Grant the owner rwx on a directory about to be cleared (Unix
/// `chmod 0o700`). No-op elsewhere: a Windows `apply_mode` would clear the
/// read-only flag, which the old code never did.
pub(crate) fn grant_owner_rwx(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        apply_mode(dir, 0o700)
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// A directory symlink or junction (Windows `FileTypeExt::is_symlink_dir`),
/// which must be removed with `remove_dir`. Always `false` elsewhere, where a
/// symlink of any kind is removed with `remove_file`.
pub(crate) fn is_symlink_dir(file_type: FileType) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileTypeExt;
        file_type.is_symlink_dir()
    }
    #[cfg(not(windows))]
    {
        let _ = file_type;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    /// The fast paths must agree with std on every shape, including roots that
    /// are byte prefixes of each other, `/`, trailing separators, `.`/`..`
    /// components, doubled separators and relative paths.
    #[test]
    fn path_prefix_helpers_match_std() {
        let samples = [
            "",
            "/",
            "//",
            "/a",
            "/a/",
            "/a//",
            "/a/b",
            "/a/b/",
            "/a/bc",
            "/a/bc/d",
            "/a/b/c",
            "/a/b//c",
            "/a/b/c/",
            "/a/b/c/.",
            "/a/b/./c",
            "/a/b/.",
            "/a/b/..",
            "/a/b/../c",
            "/a/b/.c",
            "/a/b.",
            "/a/b./c",
            "/a/./b",
            "/a/./b/c",
            "/a//b/c",
            "/a/b/c/d/e.txt",
            ".",
            "./",
            "./a",
            "a",
            "a/",
            "a/b",
            "ab",
            "..",
            "../a",
            "/a/b/c//",
        ];
        for p in samples {
            for b in samples {
                let (pp, bp) = (Path::new(p), Path::new(b));
                assert_eq!(
                    path_starts_with(pp, bp),
                    pp.starts_with(bp),
                    "starts_with({p:?}, {b:?})"
                );
                assert_eq!(
                    strip_path_prefix(pp, bp),
                    pp.strip_prefix(bp).ok(),
                    "strip_prefix({p:?}, {b:?})"
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn path_prefix_helpers_handle_non_utf8() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let base = Path::new(OsStr::from_bytes(b"/src/\x80dir"));
        let child = Path::new(OsStr::from_bytes(b"/src/\x80dir/f\xff.txt"));
        let sibling = Path::new(OsStr::from_bytes(b"/src/\x80dirx/f"));
        assert!(path_starts_with(child, base));
        assert!(!path_starts_with(sibling, base));
        assert_eq!(
            strip_path_prefix(child, base),
            Some(Path::new(OsStr::from_bytes(b"f\xff.txt")))
        );
        assert_eq!(strip_path_prefix(sibling, base), None);
    }

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
        // Path-based on Unix; std opens with FILE_WRITE_ATTRIBUTES on Windows.
        // Both succeed on read-only files.
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

    #[cfg(unix)]
    #[test]
    fn set_symlink_mtime_does_not_follow() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.txt");
        let link = dir.path().join("link");
        std::fs::write(&target, b"data").unwrap();
        set_file_mtime(&target, 1_600_000_000, 0).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        set_symlink_mtime(&link, 1_700_000_000, 0).unwrap();

        let secs = |m: std::fs::Metadata| {
            m.modified()
                .unwrap()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        };
        assert_eq!(
            secs(std::fs::symlink_metadata(&link).unwrap()),
            1_700_000_000
        );
        assert_eq!(secs(std::fs::metadata(&target).unwrap()), 1_600_000_000);
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
            nlink: 1,
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
    #[cfg(unix)]
    #[test]
    fn os_str_bytes_and_path_from_bytes_roundtrip() {
        let bytes: &[u8] = b"a\xffb";
        let path = path_from_bytes(bytes).unwrap();
        assert_eq!(os_str_bytes(path.as_os_str()), Some(bytes));
    }

    #[cfg(not(unix))]
    #[test]
    fn os_str_bytes_and_path_from_bytes_roundtrip() {
        assert!(path_from_bytes(b"ab").is_none());
        assert!(os_str_bytes(OsStr::new("ab")).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn set_nofollow_refuses_symlink_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let link = dir.path().join("link");
        std::fs::write(&target, b"keep").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let mut opts = OpenOptions::new();
        opts.write(true);
        set_nofollow(&mut opts);
        let err = opts.open(&link).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ELOOP));
        assert_eq!(std::fs::read(&target).unwrap(), b"keep");
        opts.open(&target).unwrap();
    }

    #[test]
    fn write_all_at_writes_at_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, [0u8; 8]).unwrap();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        write_all_at(&f, b"ab", 3).unwrap();
        write_all_at(&f, b"z", 0).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            [b'z', 0, 0, b'a', b'b', 0, 0, 0]
        );
    }

    #[cfg(unix)]
    #[test]
    fn lstat_mode_reads_mode_and_not_symlink_target() {
        use std::os::unix::fs::PermissionsExt;
        // POSIX file-type bits; `libc::S_IF*` are `u16` on macOS, `u32` on Linux.
        const S_IFMT: u32 = 0o170_000;
        const S_IFREG: u32 = 0o100_000;
        const S_IFLNK: u32 = 0o120_000;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"x").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let m = lstat_mode(&path).unwrap();
        assert_eq!(m & 0o7777, 0o640);
        assert_eq!(m & S_IFMT, S_IFREG);

        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        let m = lstat_mode(&link).unwrap();
        assert_eq!(m & S_IFMT, S_IFLNK);

        assert!(lstat_mode(&dir.path().join("missing")).is_none());
    }

    #[cfg(not(unix))]
    #[test]
    fn lstat_mode_reads_mode_and_not_symlink_target() {
        let dir = tempfile::tempdir().unwrap();
        assert!(lstat_mode(dir.path()).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn rootless_slash_path_unix() {
        assert_eq!(rootless_slash_path(Path::new("/a/b")), "a/b");
        assert_eq!(rootless_slash_path(Path::new("/")), "");
        assert_eq!(rootless_slash_path(Path::new("/a")), "a");
    }

    #[test]
    fn is_symlink_dir_false_for_plain_entries() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        let sub = dir.path().join("d");
        std::fs::write(&file, b"x").unwrap();
        std::fs::create_dir(&sub).unwrap();
        for p in [&file, &sub] {
            let ft = std::fs::symlink_metadata(p).unwrap().file_type();
            assert!(!is_symlink_dir(ft));
        }
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
