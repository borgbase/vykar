//! Filesystem durability primitives shared by the client, the storage
//! backends, and the server.
//!
//! Renaming or creating a file is only durable once its *directory* has been
//! fsynced. Every component that writes into a repository does this, so the
//! platform split lives here once instead of in each of them.
//!
//! Apple targets need a second platform split. Rust's `File::sync_all` and
//! `File::sync_data` do not issue `fsync(2)` there — they issue
//! `fcntl(F_FULLFSYNC)`, a device-level barrier that `smbfs`, NFS, exFAT and
//! FUSE do not implement and reject with `ENOTSUP`. `fsync_file` and
//! `fdatasync_file` fall back to plain `fsync(2)` in exactly that case, so a
//! repository on a network share still gets a real durability barrier. If the
//! fallback itself fails the error propagates: a backup tool must never report
//! a commit that was not made durable.

use std::fs::File;
use std::io;
use std::path::Path;

/// Open `dir` and fsync it so its newly-created/renamed entries become
/// durable.
///
/// No-op on non-Unix: Windows cannot open a directory as a `std::fs::File`,
/// and there is no portable equivalent. On Apple targets this inherits the
/// `F_FULLFSYNC` -> `fsync(2)` fallback described at the module level.
///
/// # Errors
///
/// Returns an error if the directory cannot be opened or `fsync` fails. On
/// non-Unix this is infallible (the directory is never opened).
pub fn fsync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fsync_file(&File::open(dir)?)
    }

    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Flush `file`'s data *and* metadata to stable storage.
///
/// Use instead of [`File::sync_all`] for anything written into a repository.
///
/// # Errors
///
/// Returns an error if the sync fails, including when the Apple fallback to
/// plain `fsync(2)` also fails.
#[cfg(not(target_vendor = "apple"))]
pub fn fsync_file(file: &File) -> io::Result<()> {
    file.sync_all()
}

/// Flush `file`'s data to stable storage, without requiring a metadata flush.
///
/// Use instead of [`File::sync_data`] for anything written into a repository.
///
/// # Errors
///
/// Returns an error if the sync fails, including when the Apple fallback to
/// plain `fsync(2)` also fails.
#[cfg(not(target_vendor = "apple"))]
pub fn fdatasync_file(file: &File) -> io::Result<()> {
    file.sync_data()
}

/// Flush `file`'s data *and* metadata to stable storage.
///
/// Use instead of [`File::sync_all`] for anything written into a repository.
///
/// # Errors
///
/// Returns an error if the sync fails, including when the Apple fallback to
/// plain `fsync(2)` also fails.
#[cfg(target_vendor = "apple")]
pub fn fsync_file(file: &File) -> io::Result<()> {
    sync_with_fallback(|| file.sync_all(), || plain_fsync(file))
}

/// Flush `file`'s data to stable storage, without requiring a metadata flush.
///
/// Use instead of [`File::sync_data`] for anything written into a repository.
///
/// # Errors
///
/// Returns an error if the sync fails, including when the Apple fallback to
/// plain `fsync(2)` also fails.
#[cfg(target_vendor = "apple")]
pub fn fdatasync_file(file: &File) -> io::Result<()> {
    // The fallback is a full fsync: Darwin has no separate fdatasync barrier
    // to drop back to, and over-syncing is the safe direction.
    sync_with_fallback(|| file.sync_data(), || plain_fsync(file))
}

/// Run `primary`, retrying with `fallback` only when the filesystem reports
/// the operation as unsupported. Any other error propagates untouched.
#[cfg(target_vendor = "apple")]
fn sync_with_fallback(
    primary: impl FnOnce() -> io::Result<()>,
    fallback: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    match primary() {
        Err(e) if is_unsupported(&e) => fallback(),
        other => other,
    }
}

/// `ENOTSUP` (45) and `EOPNOTSUPP` (102) are distinct constants on Darwin,
/// unlike Linux where both are 95, so both have to be matched. `EINVAL` is
/// deliberately excluded — falling back on it would also mask genuine misuse.
#[cfg(target_vendor = "apple")]
fn is_unsupported(e: &io::Error) -> bool {
    use nix::errno::Errno;
    matches!(
        e.raw_os_error(),
        Some(code) if code == Errno::ENOTSUP as i32 || code == Errno::EOPNOTSUPP as i32
    )
}

/// Plain `fsync(2)`, the one barrier `smbfs` and friends do implement.
#[cfg(target_vendor = "apple")]
fn plain_fsync(file: &File) -> io::Result<()> {
    retry_on_eintr(|| nix::unistd::fsync(file))
}

/// `fsync(2)` is interruptible; std's own sync paths retry on `EINTR` and so
/// must this one.
#[cfg(target_vendor = "apple")]
fn retry_on_eintr(mut call: impl FnMut() -> nix::Result<()>) -> io::Result<()> {
    loop {
        match call() {
            Ok(()) => return Ok(()),
            Err(nix::errno::Errno::EINTR) => {}
            Err(e) => return Err(io::Error::from_raw_os_error(e as i32)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn fsync_dir_ok_on_existing_err_on_missing() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        assert!(
            fsync_dir(tmp.path()).is_ok(),
            "fsync of existing dir succeeds"
        );
        assert!(
            fsync_dir(&tmp.path().join("nope")).is_err(),
            "fsync of missing dir errors"
        );
    }

    #[test]
    fn sync_file_helpers_succeed_on_regular_file() {
        let tmp = tempfile::NamedTempFile::new().expect("create tempfile");
        fsync_file(tmp.as_file()).expect("fsync_file succeeds");
        fdatasync_file(tmp.as_file()).expect("fdatasync_file succeeds");
    }

    #[cfg(target_vendor = "apple")]
    mod apple {
        use super::*;
        use std::cell::Cell;

        fn err(code: i32) -> io::Error {
            io::Error::from_raw_os_error(code)
        }

        #[test]
        fn fallback_not_invoked_when_primary_succeeds() {
            let called = Cell::new(false);
            let r = sync_with_fallback(
                || Ok(()),
                || {
                    called.set(true);
                    Ok(())
                },
            );
            assert!(r.is_ok());
            assert!(
                !called.get(),
                "fallback must not run after a successful sync"
            );
        }

        #[test]
        fn fallback_invoked_for_both_unsupported_errnos() {
            for code in [nix::libc::ENOTSUP, nix::libc::EOPNOTSUPP] {
                let called = Cell::new(false);
                let r = sync_with_fallback(
                    || Err(err(code)),
                    || {
                        called.set(true);
                        Ok(())
                    },
                );
                assert!(r.is_ok(), "fallback result is returned for errno {code}");
                assert!(called.get(), "fallback runs for errno {code}");
            }
        }

        #[test]
        fn other_errors_propagate_without_fallback() {
            let called = Cell::new(false);
            let r = sync_with_fallback(
                || Err(err(nix::libc::EIO)),
                || {
                    called.set(true);
                    Ok(())
                },
            );
            assert_eq!(r.unwrap_err().raw_os_error(), Some(nix::libc::EIO));
            assert!(!called.get(), "fallback must not mask a real I/O error");
        }

        #[test]
        fn fallback_failure_propagates() {
            let r =
                sync_with_fallback(|| Err(err(nix::libc::ENOTSUP)), || Err(err(nix::libc::EIO)));
            assert_eq!(
                r.unwrap_err().raw_os_error(),
                Some(nix::libc::EIO),
                "a failed fallback is never reported as a durable sync"
            );
        }

        #[test]
        fn retry_on_eintr_retries_then_succeeds() {
            let calls = Cell::new(0);
            let r = retry_on_eintr(|| {
                calls.set(calls.get() + 1);
                if calls.get() <= 2 {
                    Err(nix::errno::Errno::EINTR)
                } else {
                    Ok(())
                }
            });
            assert!(r.is_ok());
            assert_eq!(calls.get(), 3);
        }

        #[test]
        fn retry_on_eintr_propagates_other_errors() {
            let r = retry_on_eintr(|| Err(nix::errno::Errno::EIO));
            assert_eq!(r.unwrap_err().raw_os_error(), Some(nix::libc::EIO));
        }
    }
}
