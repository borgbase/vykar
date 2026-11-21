//! Cheap snapshot-set listing for the daemon status worker's change
//! detection (GitHub #159).
//!
//! A single `LIST snapshots/` (no passphrase or index read) tells the worker
//! whether a repo changed outside the daemon (a CLI delete/prune, a backup
//! from another host), so the expensive full refresh runs only when needed.

use std::collections::HashSet;

use vykar_core::config::ResolvedRepo;
use vykar_storage::StorageBackend;
use vykar_types::error::Result;

/// List the `<id>` portion of every `snapshots/<id>` key for a repo, using a
/// bare storage backend (no passphrase/KDF — same pattern as `break_lock`).
pub(super) fn list_snapshot_ids(repo: &ResolvedRepo) -> Result<HashSet<String>> {
    let storage = vykar_core::storage::backend_from_config(
        &repo
            .config
            .repository
            .storage_config(1)
            .with_cancel(crate::signal::shutdown_token().clone()),
    )?;
    snapshot_ids_from_backend(storage.as_ref())
}

/// Extract snapshot ID keys from a `snapshots/` listing.
fn snapshot_ids_from_backend(storage: &dyn StorageBackend) -> Result<HashSet<String>> {
    let keys = storage.list("snapshots/")?;
    Ok(keys
        .into_iter()
        .filter_map(|k| {
            k.strip_prefix("snapshots/")
                .filter(|id| !id.is_empty())
                .map(str::to_string)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use vykar_types::error::VykarError;

    /// Minimal in-memory backend for exercising `snapshot_ids_from_backend`.
    struct FakeBackend {
        keys: Vec<String>,
        fail_list: bool,
    }

    impl StorageBackend for FakeBackend {
        fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }
        fn exists(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            if self.fail_list {
                return Err(VykarError::Other("simulated list failure".into()));
            }
            Ok(self
                .keys
                .iter()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }
        fn get_range(&self, _key: &str, _offset: u64, _length: u64) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn create_dir(&self, _key: &str) -> Result<()> {
            Ok(())
        }

        fn profile(&self) -> vykar_storage::BackendProfile {
            vykar_storage::BackendProfile {
                local: false,
                staged_put: false,
                request_width: 1,
                transfer_width: 1,
            }
        }

        vykar_storage::unsupported_server_ops!();
    }

    #[test]
    fn snapshot_ids_strips_prefix_and_drops_empty() {
        let backend = FakeBackend {
            keys: vec![
                "snapshots/aaaa".into(),
                "snapshots/bbbb".into(),
                "snapshots/".into(), // directory marker — must be ignored
            ],
            fail_list: false,
        };
        let ids = snapshot_ids_from_backend(&backend).unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains("aaaa"));
        assert!(ids.contains("bbbb"));
    }

    #[test]
    fn snapshot_ids_propagates_list_error() {
        let backend = FakeBackend {
            keys: vec![],
            fail_list: true,
        };
        assert!(snapshot_ids_from_backend(&backend).is_err());
    }
}
