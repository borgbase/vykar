//! Filesystem durability primitives shared by the client, the storage
//! backends, and the server.
//!
//! Renaming or creating a file is only durable once its *directory* has been
//! fsynced. Every component that writes into a repository does this, so the
//! platform split lives here once instead of in each of them.

use std::path::Path;

/// Open `dir` and fsync it so its newly-created/renamed entries become
/// durable.
///
/// No-op on non-Unix: Windows cannot open a directory as a `std::fs::File`,
/// and there is no portable equivalent.
///
/// # Errors
///
/// Returns an error if the directory cannot be opened or `fsync` fails. On
/// non-Unix this is infallible (the directory is never opened).
pub fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()
    }

    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
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
}
