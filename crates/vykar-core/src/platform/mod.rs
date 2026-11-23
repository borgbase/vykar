// libc::kill liveness probe, clock_gettime and (in tests) geteuid; SAFETY
// documented per block.
#![allow(unsafe_code)]

pub mod fs;
pub use vykar_common::paths;
pub mod shell;

/// Strip the domain suffix from a hostname (everything after the first `.`).
pub fn strip_hostname_domain(hostname: &str) -> String {
    match hostname.find('.') {
        Some(pos) => hostname[..pos].to_string(),
        None => hostname.to_string(),
    }
}

/// Return the short hostname (domain stripped).
/// On macOS, gethostname() returns a network-dependent FQDN; truncating
/// at the first dot yields the stable local hostname. On Linux/Windows
/// this is typically a no-op.
pub fn short_hostname() -> String {
    strip_hostname_domain(&hostname())
}

/// Return the system hostname, or `"unknown"` if it cannot be determined.
pub fn hostname() -> String {
    #[cfg(unix)]
    {
        nix::unistd::gethostname()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".into())
    }

    #[cfg(windows)]
    {
        std::env::var("COMPUTERNAME").unwrap_or_else(|_| "unknown".into())
    }
}

/// Return a stable identifier for the current boot where the OS exposes one.
///
/// Linux publishes this at `/proc/sys/kernel/random/boot_id`. Other platforms
/// return `None`, so callers must treat boot-based stale detection as
/// unavailable rather than guessing.
///
/// Cached for the process lifetime — boot ID is constant once the process
/// starts, so the underlying read happens at most once.
pub fn boot_id() -> Option<String> {
    static CACHED: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    CACHED.get_or_init(read_boot_id).clone()
}

fn read_boot_id() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// File name of the installation ID under `<config dir>/vykar/`.
pub const INSTALLATION_ID_FILE: &str = "installation-id";

/// This installation's ID: 32 lowercase hex digits, read from
/// `<config dir>/vykar/installation-id` and created there on first use.
/// `None` when there is no config directory, or the file cannot be read,
/// created or parsed; each case is warned once. Read once per process.
///
/// Test builds of this crate use a process-local random ID and touch no
/// file, so the unit tests never write into a real config directory; the
/// file logic is tested through `load_or_create_installation_id`.
pub fn installation_id() -> Option<String> {
    static CACHED: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    CACHED
        .get_or_init(|| {
            #[cfg(test)]
            {
                Some(format!("{:032x}", rand::random::<u128>()))
            }
            #[cfg(not(test))]
            {
                let Some(dir) = paths::config_dir().map(|d| d.join("vykar")) else {
                    tracing::warn!("no config directory; same-host lock reaping is off");
                    return None;
                };
                load_or_create_installation_id(&dir)
            }
        })
        .clone()
}

/// The installation ID in `dir`, created if absent.
///
/// An existing file is never overwritten: a malformed or unreadable one
/// gives `None`, so one installation is never split in two while its
/// processes hold entries. Creation is race-safe across processes.
pub(crate) fn load_or_create_installation_id(dir: &std::path::Path) -> Option<String> {
    let path = dir.join(INSTALLATION_ID_FILE);
    match read_installation_id(&path) {
        Ok(id) => return id,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::warn!(
                "could not read installation ID file {}: {e}; same-host lock reaping is off \
                 until it is readable (fix its permissions or remove it)",
                path.display()
            );
            return None;
        }
    }
    match create_installation_id(dir, &path) {
        Ok(id) => Some(id),
        Err(e) => {
            // Another process may have created it (AlreadyExists), or the
            // failure came after a concurrent creation: read what is there.
            if e.kind() == std::io::ErrorKind::AlreadyExists || path.exists() {
                match read_installation_id(&path) {
                    Ok(id) => return id,
                    Err(e) => {
                        tracing::warn!(
                            "could not read installation ID file {}: {e}; same-host lock \
                             reaping is off",
                            path.display()
                        );
                        return None;
                    }
                }
            }
            tracing::warn!(
                "could not create {}: {e}; same-host lock reaping is off",
                path.display()
            );
            None
        }
    }
}

/// Read and validate the file at `path`. `Ok(None)` for a malformed file,
/// which is warned about and left alone.
fn read_installation_id(path: &std::path::Path) -> std::io::Result<Option<String>> {
    let s = std::fs::read_to_string(path)?;
    let id = s.trim();
    if id.len() == 32 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        Ok(Some(id.to_string()))
    } else {
        tracing::warn!(
            "installation ID file {} is malformed; same-host lock reaping is off until it is \
             removed",
            path.display()
        );
        Ok(None)
    }
}

/// Write a fresh ID to `path` unless a file is already there.
fn create_installation_id(
    dir: &std::path::Path,
    path: &std::path::Path,
) -> std::io::Result<String> {
    use std::io::Write;
    std::fs::create_dir_all(dir)?;
    let id = format!("{:032x}", rand::random::<u128>());
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(format!("{id}\n").as_bytes())?;
    // Readable by the user's other processes, also when `sudo` created it:
    // the value is written in plaintext into every repository anyway.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tmp.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o644))?;
    }
    let _ = tmp.as_file().sync_all();
    tmp.persist_noclobber(path).map_err(|e| e.error)?;
    Ok(id)
}

/// Inode of this process's PID namespace (`/proc/self/ns/pid`). Linux
/// only; read once per process.
pub fn pid_namespace() -> Option<u64> {
    static CACHED: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        #[cfg(target_os = "linux")]
        {
            // metadata follows the symlink to the nsfs inode.
            std::fs::metadata("/proc/self/ns/pid")
                .ok()
                .map(|m| std::os::unix::fs::MetadataExt::ino(&m))
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    })
}

/// Time since boot, suspend included (`CLOCK_BOOTTIME`). Linux only.
pub fn uptime() -> Option<std::time::Duration> {
    #[cfg(target_os = "linux")]
    {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is a valid, writable timespec on our stack.
        let ret = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
        if ret != 0 || ts.tv_sec < 0 || !(0..1_000_000_000).contains(&ts.tv_nsec) {
            return None;
        }
        Some(std::time::Duration::new(
            ts.tv_sec as u64,
            ts.tv_nsec as u32,
        ))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Check whether a process with the given PID is alive on the local machine.
///
/// On Unix, uses `kill(pid, 0)`: returns `true` if the process exists (even if
/// owned by another user — `EPERM` still means alive). On non-Unix platforms,
/// conservatively returns `true` to avoid false-positive stale detection.
pub fn is_pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let Some(pid) = i32::try_from(pid).ok().filter(|&p| p > 0) else {
            return true; // out-of-range PID — conservatively assume alive
        };
        // SAFETY: kill(pid, 0) performs no actual signal delivery; it only
        // checks whether the process exists.
        let ret = unsafe { libc::kill(pid, 0) };
        if ret == 0 {
            return true;
        }
        // EPERM means the process exists but we lack permission to signal it.
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    #[cfg(not(unix))]
    {
        let _ = pid;
        true // conservative: assume alive
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_hostname_domain_removes_suffix() {
        assert_eq!(strip_hostname_domain("MiniBeard.local"), "MiniBeard");
        assert_eq!(strip_hostname_domain("MiniBeard.fritz.box"), "MiniBeard");
    }

    #[test]
    fn strip_hostname_domain_noop_without_dot() {
        assert_eq!(strip_hostname_domain("myhost"), "myhost");
        assert_eq!(strip_hostname_domain(""), "");
    }

    #[test]
    fn short_hostname_returns_nonempty() {
        let h = short_hostname();
        assert!(!h.is_empty() || hostname() == "unknown");
    }

    fn id_ok(id: &Option<String>) -> bool {
        id.as_deref().is_some_and(|s| {
            s.len() == 32 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        })
    }

    #[test]
    fn installation_id_is_created_and_reused() {
        let dir = tempfile::tempdir().unwrap();
        let first = load_or_create_installation_id(dir.path());
        assert!(id_ok(&first), "{first:?}");
        let raw = std::fs::read_to_string(dir.path().join(INSTALLATION_ID_FILE)).unwrap();
        assert_eq!(raw, format!("{}\n", first.as_deref().unwrap()));
        assert_eq!(load_or_create_installation_id(dir.path()), first);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join(INSTALLATION_ID_FILE))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o644);
        }
    }

    #[test]
    fn installation_id_reads_existing_file_with_whitespace() {
        let dir = tempfile::tempdir().unwrap();
        let id = "0123456789abcdef0123456789abcdef";
        std::fs::write(dir.path().join(INSTALLATION_ID_FILE), format!("  {id}\n\n")).unwrap();
        assert_eq!(
            load_or_create_installation_id(dir.path()).as_deref(),
            Some(id)
        );
    }

    #[test]
    fn malformed_installation_id_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(INSTALLATION_ID_FILE);
        for body in [
            &b"0123456789ABCDEF0123456789ABCDEF\n"[..],
            b"short\n",
            b"",
            b"\xff\xfe",
        ] {
            std::fs::write(&path, body).unwrap();
            assert_eq!(load_or_create_installation_id(dir.path()), None);
            assert_eq!(std::fs::read(&path).unwrap(), body);
        }
    }

    #[test]
    fn installation_id_creates_missing_parent() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a").join("vykar");
        let id = load_or_create_installation_id(&nested);
        assert!(id_ok(&id));
        assert!(nested.join(INSTALLATION_ID_FILE).is_file());
    }

    #[cfg(unix)]
    fn running_as_root() -> bool {
        // SAFETY: geteuid takes no arguments and is always sound.
        unsafe { libc::geteuid() == 0 }
    }

    #[cfg(unix)]
    #[test]
    fn read_only_directory_gives_none_and_no_file() {
        use std::os::unix::fs::PermissionsExt;
        if running_as_root() {
            return; // root ignores the mode
        }
        let dir = tempfile::tempdir().unwrap();
        let ro = dir.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o555)).unwrap();
        let id = load_or_create_installation_id(&ro);
        let exists = ro.join(INSTALLATION_ID_FILE).exists();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(id, None);
        assert!(!exists);
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_installation_id_is_not_replaced() {
        use std::os::unix::fs::PermissionsExt;
        if running_as_root() {
            return; // root ignores the mode
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(INSTALLATION_ID_FILE);
        let body = "0123456789abcdef0123456789abcdef\n";
        std::fs::write(&path, body).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
        let id = load_or_create_installation_id(dir.path());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(id, None);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), body);
    }

    #[test]
    fn concurrent_creation_agrees_on_one_id() {
        let dir = tempfile::tempdir().unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let ids: Vec<Option<String>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let barrier = std::sync::Arc::clone(&barrier);
                    let dir = dir.path();
                    s.spawn(move || {
                        barrier.wait();
                        load_or_create_installation_id(dir)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert!(id_ok(&ids[0]), "{ids:?}");
        assert!(ids.iter().all(|id| *id == ids[0]), "{ids:?}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_pid_namespace_and_uptime() {
        let ns = pid_namespace();
        assert!(ns.is_some());
        assert_eq!(pid_namespace(), ns);
        let a = uptime().expect("CLOCK_BOOTTIME");
        let b = uptime().expect("CLOCK_BOOTTIME");
        assert!(a > std::time::Duration::ZERO);
        assert!(b >= a);
    }
}
