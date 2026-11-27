use std::path::Path;

use tracing::info;

use crate::config::VykarConfig;
use crate::storage::backend_from_config;
use vykar_storage::{BatchDeleteScope, ParsedUrl, StorageBackend, parse_repo_url};
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
    let backend =
        backend_from_config(&config.repository.storage_config(config.limits.connections))?;

    let (known, unknown): (Vec<String>, Vec<String>) = backend
        .list("")?
        .into_iter()
        .partition(|k| is_known_repo_key(k));

    // If there are no known repo keys at all, this isn't a vykar repository
    if known.is_empty() {
        return Err(VykarError::RepoNotFound(config.repository.url.clone()));
    }

    // No session, deliberately: registering one is a write, which would
    // block deleting a repo on a full disk/quota or a read-only backend, and
    // it would buy nothing — deleting the repository breaks a concurrent
    // backup regardless of whether its temp files go too.
    //
    // `list` hides temp files from interrupted uploads; sweep them regardless
    // of age so the directories can be removed. Runs before deleting known keys:
    // if the sweep fails, the repo is still intact and a rerun retries it.
    let temps_deleted = backend.remove_stale_temp_files(std::time::Duration::ZERO)?;

    delete_known(backend.as_ref(), &known)?;
    let keys_deleted = (known.len() + temps_deleted) as u64;

    let parsed = parse_repo_url(&config.repository.url)?;
    let mut root_removed = false;
    let is_local = backend.profile().local;

    if let ParsedUrl::Local { path } = parsed {
        let repo_path = Path::new(&path);

        // Drop the backend so it releases any handles before directory cleanup
        drop(backend);

        // Clean up empty directories (non-recursive remove_dir only succeeds if empty)
        for i in 0..=0xFFu32 {
            let _ = std::fs::remove_dir(repo_path.join(format!("packs/{:02x}", i)));
        }
        for dir in vykar_protocol::KNOWN_ROOT_DIRS {
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

/// Delete every known repository key: one batch where the backend has one
/// (with the whole-repository cleanup), else key by key.
fn delete_known(backend: &dyn StorageBackend, known: &[String]) -> Result<()> {
    match backend.batch_delete() {
        Some(batch) => batch.delete_keys(known, BatchDeleteScope::WholeRepository),
        None => {
            for key in known {
                backend.delete(key)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use vykar_storage::{BatchDelete, InnerBackend, delegate_storage_backend};

    use super::*;
    use crate::testutil::MemoryBackend;

    /// Records the batch-delete scope and every per-key delete. Offers a batch
    /// delete only when `batch_supported`.
    struct ScopeRecorder {
        inner: MemoryBackend,
        batch_supported: bool,
        scopes: Mutex<Vec<BatchDeleteScope>>,
        deletes: Mutex<Vec<String>>,
    }

    impl ScopeRecorder {
        fn new(batch_supported: bool) -> Self {
            Self {
                inner: MemoryBackend::new(),
                batch_supported,
                scopes: Mutex::default(),
                deletes: Mutex::default(),
            }
        }
    }

    impl InnerBackend for ScopeRecorder {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for ScopeRecorder;
        except [delete, batch_delete];

        fn delete(&self, key: &str) -> Result<()> {
            self.deletes.lock().unwrap().push(key.to_string());
            self.inner.delete(key)
        }

        fn batch_delete(&self) -> Option<&dyn BatchDelete> {
            self.batch_supported.then_some(self as &dyn BatchDelete)
        }
    }

    impl BatchDelete for ScopeRecorder {
        fn delete_keys(&self, keys: &[String], scope: BatchDeleteScope) -> Result<()> {
            self.scopes.lock().unwrap().push(scope);
            for key in keys {
                self.inner.delete(key)?;
            }
            Ok(())
        }
    }

    fn known() -> Vec<String> {
        vec!["config".into(), "index".into(), "packs/ab/x".into()]
    }

    #[test]
    fn delete_known_batches_with_whole_repository_scope() {
        let backend = ScopeRecorder::new(true);
        delete_known(&backend, &known()).unwrap();
        assert_eq!(
            *backend.scopes.lock().unwrap(),
            vec![BatchDeleteScope::WholeRepository]
        );
        assert!(backend.deletes.lock().unwrap().is_empty());
    }

    #[test]
    fn delete_known_falls_back_to_per_key_deletes() {
        let backend = ScopeRecorder::new(false);
        delete_known(&backend, &known()).unwrap();
        assert!(
            backend.scopes.lock().unwrap().is_empty(),
            "no batch is attempted"
        );
        assert_eq!(*backend.deletes.lock().unwrap(), known());
    }
}
