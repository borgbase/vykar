use std::path::Path;

use tracing::info;

use crate::config::VykarConfig;
use crate::storage::backend_from_config;
use vykar_storage::{parse_repo_url, ParsedUrl};
use vykar_types::error::{Result, VykarError};

pub use vykar_protocol::is_known_repo_key;

#[derive(Debug)]
pub struct DeleteRepoStats {
    pub keys_deleted: u64,
    pub unknown_entries: Vec<String>,
    pub root_removed: bool,
    pub is_local: bool,
}

pub fn run(config: &VykarConfig) -> Result<DeleteRepoStats> {
    let backend = backend_from_config(&config.repository, config.limits.connections)?;

    let (known, unknown): (Vec<String>, Vec<String>) = backend
        .list("")?
        .into_iter()
        .partition(|k| is_known_repo_key(k));

    // If there are no known repo keys at all, this isn't a vykar repository
    if known.is_empty() {
        return Err(VykarError::RepoNotFound(config.repository.url.clone()));
    }

    // No maintenance lock, deliberately: taking one is a write, which would
    // block deleting a repo on a full disk/quota or a read-only backend, and
    // it would buy nothing — deleting the repository breaks a concurrent
    // backup regardless of whether its temp files go too.
    //
    // `list` hides temp files from interrupted uploads; sweep them regardless
    // of age so the directories can be removed. Runs before deleting known keys:
    // if the sweep fails, the repo is still intact and a rerun retries it.
    let temps_deleted = backend.remove_stale_temp_files(std::time::Duration::ZERO)?;

    match backend.batch_delete_keys(&known) {
        Ok(()) => {}
        Err(VykarError::UnsupportedBackend(_)) => {
            for key in &known {
                backend.delete(key)?;
            }
        }
        Err(e) => return Err(e),
    }
    let keys_deleted = (known.len() + temps_deleted) as u64;

    let parsed = parse_repo_url(&config.repository.url)?;
    let mut root_removed = false;
    let is_local = parsed.is_local();

    if let ParsedUrl::Local { path } = parsed {
        let repo_path = Path::new(&path);

        // Drop the backend so it releases any handles before directory cleanup
        drop(backend);

        // Clean up empty directories (non-recursive remove_dir only succeeds if empty)
        for i in 0..=0xFFu32 {
            let _ = std::fs::remove_dir(repo_path.join(format!("packs/{:02x}", i)));
        }
        for dir in &[
            "packs",
            "keys",
            "snapshots",
            "locks",
            "sessions",
            "pending_index",
        ] {
            let _ = std::fs::remove_dir(repo_path.join(dir));
        }

        // If no unknown entries, try to remove the repo root (only succeeds if empty)
        if unknown.is_empty() && std::fs::remove_dir(repo_path).is_ok() {
            root_removed = true;
        }

        info!(path = %path, keys = keys_deleted, unknown = unknown.len(), "deleted local repository");
    } else {
        info!(url = %config.repository.url, keys = keys_deleted, unknown = unknown.len(), "deleted remote repository");
    }

    Ok(DeleteRepoStats {
        keys_deleted,
        unknown_entries: unknown,
        root_removed,
        is_local,
    })
}
