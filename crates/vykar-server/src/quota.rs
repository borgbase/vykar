use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::Arc;

use tracing::{debug, info};

/// 50 MiB safety margin subtracted from free-space-based limits.
const FREE_SPACE_MARGIN: u64 = 50 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaSource {
    /// Limit set via `--quota` flag.
    Explicit,
    /// Limit from XFS/ext4 user or project quota via `quotactl`.
    FsQuota,
    /// Limit derived from filesystem free space minus safety margin.
    FreeSpace,
    /// No quota detected — unlimited.
    Unlimited,
}

impl QuotaSource {
    fn to_u8(self) -> u8 {
        match self {
            Self::Explicit => 0,
            Self::FsQuota => 1,
            Self::FreeSpace => 2,
            Self::Unlimited => 3,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Explicit,
            1 => Self::FsQuota,
            2 => Self::FreeSpace,
            _ => Self::Unlimited,
        }
    }
}

pub struct QuotaState {
    source: AtomicU8,
    effective_limit: AtomicU64,
    /// If true, `--quota` was explicitly set; `refresh()` is a no-op.
    explicit: bool,
    data_dir: PathBuf,
}

impl QuotaState {
    pub fn new(source: QuotaSource, limit: u64, explicit: bool, data_dir: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            source: AtomicU8::new(source.to_u8()),
            effective_limit: AtomicU64::new(limit),
            explicit,
            data_dir,
        })
    }

    /// Current effective quota limit in bytes. 0 = unlimited.
    pub fn limit(&self) -> u64 {
        self.effective_limit.load(Relaxed)
    }

    /// Current quota source.
    pub fn source(&self) -> QuotaSource {
        QuotaSource::from_u8(self.source.load(Relaxed))
    }

    /// Re-detect quota from the filesystem. **Blocking** — call via `spawn_blocking`.
    ///
    /// No-op if the quota was set explicitly via `--quota`.
    pub fn refresh(&self, current_usage: u64) {
        if self.explicit {
            return;
        }

        let (new_source, new_limit) = detect_auto(&self.data_dir, current_usage);

        self.effective_limit.store(new_limit, Relaxed);
        self.source.store(new_source.to_u8(), Relaxed);
    }
}

/// Detect quota for `data_dir`.
///
/// If `explicit_override` is `Some(n)` with `n > 0`, returns `(Explicit, n)`.
/// Otherwise tries filesystem quota, then free space, then unlimited.
pub fn detect_quota(
    data_dir: &Path,
    explicit_override: Option<u64>,
    current_usage: u64,
) -> (QuotaSource, u64) {
    if let Some(limit) = explicit_override {
        if limit > 0 {
            return (QuotaSource::Explicit, limit);
        }
    }
    detect_auto(data_dir, current_usage)
}

/// Auto-detection (no explicit override).
fn detect_auto(data_dir: &Path, current_usage: u64) -> (QuotaSource, u64) {
    #[cfg(target_os = "linux")]
    if let Some(limit) = detect_fs_quota(data_dir) {
        return (QuotaSource::FsQuota, limit);
    }

    if let Some(avail) = available_free_space(data_dir) {
        let limit = current_usage.saturating_add(avail);
        return (QuotaSource::FreeSpace, limit);
    }

    (QuotaSource::Unlimited, 0)
}

// ---------------------------------------------------------------------------
// Free space detection (Unix: Linux + macOS)
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn available_free_space(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;

    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 {
        debug!("statvfs failed: {}", std::io::Error::last_os_error());
        return None;
    }

    // Casts needed for cross-platform: types vary between Linux and macOS.
    #[allow(clippy::unnecessary_cast)]
    let avail = (stat.f_bavail as u64).checked_mul(stat.f_frsize as u64)?;
    Some(avail.saturating_sub(FREE_SPACE_MARGIN))
}

#[cfg(not(unix))]
fn available_free_space(_path: &Path) -> Option<u64> {
    None
}

// ---------------------------------------------------------------------------
// XFS / ext4 quota detection (Linux only)
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn detect_fs_quota(data_dir: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::io::AsRawFd;

    let file = match std::fs::File::open(data_dir) {
        Ok(f) => f,
        Err(e) => {
            info!(path = %data_dir.display(), err = %e, "quota: failed to open data dir");
            return None;
        }
    };
    let fd = file.as_raw_fd();

    // Try user quota for the running process
    let uid = unsafe { libc::getuid() };
    if let Some(limit) = xfs_get_quota_fd(fd, uid, USRQUOTA) {
        info!(uid, limit, "quota: detected user quota (process uid)");
        return Some(limit);
    }

    // Try user quota for the data directory owner (covers running as root)
    if let Ok(meta) = std::fs::metadata(data_dir) {
        let dir_uid = meta.uid();
        if dir_uid != uid {
            if let Some(limit) = xfs_get_quota_fd(fd, dir_uid, USRQUOTA) {
                info!(dir_uid, limit, "quota: detected user quota (dir owner)");
                return Some(limit);
            }
        }
    }

    // Fall back to project quota
    if let Some(proj_id) = get_project_id(data_dir) {
        debug!(proj_id, "quota: project id on data dir");
        if proj_id > 0 {
            if let Some(limit) = xfs_get_quota_fd(fd, proj_id, PRJQUOTA) {
                info!(proj_id, limit, "quota: detected project quota");
                return Some(limit);
            }
        }
    }

    info!("quota: no filesystem quota detected");
    None
}

// quotactl constants (from linux headers)
#[cfg(target_os = "linux")]
const USRQUOTA: libc::c_int = 0;
#[cfg(target_os = "linux")]
const PRJQUOTA: libc::c_int = 2;
#[cfg(target_os = "linux")]
const Q_XGETQUOTA: libc::c_int = 0x5803;

/// Build quotactl command: `QCMD(cmd, type) = (cmd << 8) | type`
#[cfg(target_os = "linux")]
fn qcmd(cmd: libc::c_int, quota_type: libc::c_int) -> libc::c_int {
    (cmd << 8) | quota_type
}

/// `fs_disk_quota` from `<linux/dqblk_xfs.h>` — used by Q_XGETQUOTA.
#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Default)]
struct FsDiskQuota {
    d_version: i8,
    d_flags: u8,
    d_fieldmask: u16,
    d_id: u32,
    d_blk_hardlimit: u64,
    d_blk_softlimit: u64,
    d_ino_hardlimit: u64,
    d_ino_softlimit: u64,
    d_bcount: u64,
    d_icount: u64,
    d_itimer: i32,
    d_btimer: i32,
    d_iwarns: u16,
    d_bwarns: u16,
    d_padding2: i32,
    d_rtb_hardlimit: u64,
    d_rtb_softlimit: u64,
    d_rtbcount: u64,
    d_rtbtimer: i32,
    d_rtbwarns: u16,
    d_padding3: i16,
    d_padding4: [i8; 8],
}

/// Query XFS/ext4 quota via `quotactl_fd` (Linux 5.14+).
///
/// Uses a file descriptor instead of a device path, which works inside
/// sandboxed systemd namespaces where `/dev/mapper/*` is not accessible.
/// Returns the hard limit in bytes, or `None` if not set / error.
#[cfg(target_os = "linux")]
fn xfs_get_quota_fd(fd: std::os::unix::io::RawFd, id: u32, quota_type: libc::c_int) -> Option<u64> {
    // quotactl_fd(2) — syscall 443 on x86_64, 443 on aarch64
    const SYS_QUOTACTL_FD: libc::c_long = 443;

    let cmd = qcmd(Q_XGETQUOTA, quota_type);

    let mut dq = FsDiskQuota::default();
    let ret = unsafe {
        libc::syscall(
            SYS_QUOTACTL_FD,
            fd as libc::c_uint,
            cmd as libc::c_uint,
            id as libc::c_int,
            &mut dq as *mut FsDiskQuota as *mut libc::c_void,
        )
    };

    if ret != 0 {
        debug!(
            id,
            quota_type,
            err = %std::io::Error::last_os_error(),
            "quota: quotactl_fd Q_XGETQUOTA failed"
        );
        return None;
    }

    if dq.d_blk_hardlimit == 0 {
        debug!(
            id,
            quota_type, "quota: quotactl_fd ok but no hard limit set"
        );
        return None;
    }

    // d_blk_hardlimit is in 512-byte basic blocks
    Some(dq.d_blk_hardlimit * 512)
}

/// Read project ID via `FS_IOC_FSGETXATTR` ioctl.
#[cfg(target_os = "linux")]
fn get_project_id(path: &Path) -> Option<u32> {
    use std::os::unix::io::AsRawFd;

    // libc::Ioctl is c_ulong on glibc, c_int on musl; the kernel truncates to u32.
    const FS_IOC_FSGETXATTR: libc::Ioctl = 0x801C_581Fu32 as i32 as libc::Ioctl;

    #[repr(C)]
    #[derive(Default)]
    struct FsxAttr {
        fsx_xflags: u32,
        fsx_extsize: u32,
        fsx_nextents: u32,
        fsx_projid: u32,
        fsx_cowextsize: u32,
        fsx_pad: [u8; 8],
    }

    let file = std::fs::File::open(path).ok()?;
    let mut attr = FsxAttr::default();

    let ret = unsafe {
        libc::ioctl(
            file.as_raw_fd(),
            FS_IOC_FSGETXATTR,
            &mut attr as *mut FsxAttr,
        )
    };

    if ret != 0 {
        debug!(
            path = %path.display(),
            err = %std::io::Error::last_os_error(),
            "FS_IOC_FSGETXATTR failed"
        );
        return None;
    }

    Some(attr.fsx_projid)
}

/// Format bytes as a human-readable string for logging.
fn format_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;

    let b = bytes as f64;
    if b >= GIB {
        format!("{:.1} GiB", b / GIB)
    } else {
        format!("{:.1} MiB", b / MIB)
    }
}

/// Log the detected quota at startup.
pub fn log_quota(source: QuotaSource, limit: u64) {
    match source {
        QuotaSource::Explicit => {
            info!("quota: source=Explicit limit={}", format_bytes(limit));
        }
        QuotaSource::FsQuota => {
            info!("quota: source=FsQuota limit={}", format_bytes(limit));
        }
        QuotaSource::FreeSpace => {
            info!(
                "quota: source=FreeSpace limit={} (after 50 MiB margin)",
                format_bytes(limit)
            );
        }
        QuotaSource::Unlimited => {
            info!("quota: source=Unlimited");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_override_takes_precedence() {
        let (source, limit) = detect_quota(Path::new("/tmp"), Some(1024), 0);
        assert_eq!(source, QuotaSource::Explicit);
        assert_eq!(limit, 1024);
    }

    #[test]
    fn explicit_zero_triggers_auto_detect() {
        // --quota 0 should NOT be treated as explicit
        let (source, _limit) = detect_quota(Path::new("/tmp"), Some(0), 0);
        assert_ne!(source, QuotaSource::Explicit);
    }

    #[cfg(unix)]
    #[test]
    fn available_free_space_returns_some_on_root() {
        let avail = available_free_space(Path::new("/"));
        assert!(avail.is_some(), "statvfs on / should succeed");
        assert!(avail.unwrap() > 0, "/ should have some free space");
    }

    #[cfg(unix)]
    #[test]
    fn detect_quota_free_space_at_least_usage() {
        let (source, limit) = detect_quota(Path::new("/tmp"), None, 1000);
        // On any real system we should get FreeSpace or FsQuota
        assert!(
            source == QuotaSource::FreeSpace || source == QuotaSource::FsQuota,
            "expected FreeSpace or FsQuota on a real system, got {source:?}"
        );
        assert!(limit >= 1000, "limit should be at least current_usage");
    }

    #[test]
    fn refresh_is_noop_when_explicit() {
        let qs = QuotaState::new(QuotaSource::Explicit, 5000, true, PathBuf::from("/tmp"));
        qs.refresh(0);
        assert_eq!(qs.limit(), 5000);
        assert_eq!(qs.source(), QuotaSource::Explicit);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn get_project_id_on_tmp() {
        // /tmp on most Linux systems returns project_id 0
        let proj_id = get_project_id(Path::new("/tmp"));
        assert!(proj_id.is_some(), "FS_IOC_FSGETXATTR should work on /tmp");
        assert_eq!(proj_id.unwrap(), 0);
    }
}
