//! Local cache locations shared by `repo` and `index`.
//!
//! Sits below both so that `index` does not reach into `repo` for a path.

use std::path::{Path, PathBuf};

use vykar_common::paths;

/// File name of the temp-file anchor, used for syncs that bypass the local
/// cache and when there is no cache directory.
pub(crate) const TEMP_ANCHOR_FILE: &str = "vykar-full-index-cache";

/// Compute the per-repo cache directory.
///
/// With `cache_dir_override`: `<override>/<repo_id_hex>/`
/// Without: `<platform_cache_dir>/vykar/<repo_id_hex>/`
pub(crate) fn repo_cache_dir(repo_id: &[u8], cache_dir_override: Option<&Path>) -> Option<PathBuf> {
    let base = match cache_dir_override {
        Some(dir) => Some(dir.to_path_buf()),
        None => paths::cache_dir().map(|d| d.join("vykar")),
    };
    base.map(|b| b.join(hex::encode(repo_id)))
}

#[cfg(test)]
mod tests {
    #[test]
    fn repo_cache_dir_default() {
        let repo_id = [0xABu8; 32];
        let dir = super::repo_cache_dir(&repo_id, None);
        assert!(dir.is_some());
        let d = dir.unwrap();
        assert!(d.to_string_lossy().contains("vykar"));
        assert!(d.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    #[test]
    fn repo_cache_dir_with_override() {
        let repo_id = [0xCDu8; 32];
        let temp = tempfile::tempdir().unwrap();
        let override_root = temp.path().join("vykar-cache");
        let dir = super::repo_cache_dir(&repo_id, Some(override_root.as_path())).unwrap();
        assert!(dir.starts_with(&override_root));
        assert!(dir.to_string_lossy().contains(&hex::encode(repo_id)));
    }
}
