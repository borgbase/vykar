//! Cheap out-of-band change detection for the daemon status page.
//!
//! The status snapshot served by the HTTP page is only rebuilt by
//! `status::refresh_repos` at startup, on SIGHUP reload, and after each backup
//! cycle. With a long schedule, any change made outside the daemon (a CLI
//! delete/prune, a backup from another host) would otherwise stay stale on the
//! page until the next cycle — potentially 24h (GitHub #159).
//!
//! This module polls each repo with a single `LIST snapshots/` (no passphrase
//! or index read) and triggers the expensive full refresh only when the set of
//! snapshot IDs actually changed.

use std::collections::{HashMap, HashSet};

use vykar_core::config::ResolvedRepo;
use vykar_storage::StorageBackend;
use vykar_types::error::Result;

use super::status::{self, SharedStatus};

/// Tracks the last-seen snapshot-ID set per repo so out-of-band changes can be
/// detected with a cheap LIST instead of a full status refresh.
pub(crate) struct StatusPoller {
    last_seen: HashMap<String, HashSet<String>>,
}

fn repo_name(repo: &ResolvedRepo) -> String {
    repo.label
        .as_deref()
        .unwrap_or(&repo.config.repository.url)
        .to_string()
}

/// List the `<id>` portion of every `snapshots/<id>` key for a repo, using a
/// bare storage backend (no passphrase/KDF — same pattern as `break_lock`).
fn list_snapshot_ids(repo: &ResolvedRepo) -> Result<HashSet<String>> {
    let storage = vykar_core::storage::backend_from_config(&repo.config.repository, 1)?;
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

impl StatusPoller {
    pub(crate) fn new() -> Self {
        Self {
            last_seen: HashMap::new(),
        }
    }

    /// Reset the last-seen sets to the current on-storage state.
    ///
    /// Call after a full `refresh_repos` (startup, reload, post-cycle) so the
    /// next poll compares against fresh state rather than re-triggering a
    /// refresh. A repo whose LIST fails is left absent, so the next successful
    /// poll for it will conservatively trigger one refresh.
    pub(crate) fn reset(&mut self, repos: &[ResolvedRepo]) {
        self.last_seen.clear();
        for repo in repos {
            let name = repo_name(repo);
            match list_snapshot_ids(repo) {
                Ok(ids) => {
                    self.last_seen.insert(name, ids);
                }
                Err(e) => {
                    tracing::debug!(repo = %name, error = %e, "status poll: seed listing failed");
                }
            }
        }
    }

    /// Poll every repo for snapshot-set changes; on any change, run the full
    /// `status::refresh_repos` and update the baseline. Returns whether a
    /// refresh was triggered.
    ///
    /// LIST failures are logged at debug and skipped: a transient network error
    /// leaves the baseline untouched and does not churn the page.
    pub(crate) fn poll_and_refresh(
        &mut self,
        status: &SharedStatus,
        repos: &[ResolvedRepo],
    ) -> bool {
        let mut changed = false;
        for repo in repos {
            let name = repo_name(repo);
            match list_snapshot_ids(repo) {
                Ok(ids) => {
                    if self.record(name, ids) {
                        changed = true;
                    }
                }
                Err(e) => {
                    tracing::debug!(repo = %name, error = %e, "status poll: listing failed, skipping");
                }
            }
        }
        if changed {
            tracing::debug!("status poll: snapshot set changed, refreshing status");
            status::refresh_repos(status, repos);
        }
        changed
    }

    /// Update the last-seen set for one repo, returning true if it changed.
    /// A repo not previously seen counts as changed.
    fn record(&mut self, name: String, ids: HashSet<String>) -> bool {
        let changed = self
            .last_seen
            .get(&name)
            .map(|prev| prev != &ids)
            .unwrap_or(true);
        self.last_seen.insert(name, ids);
        changed
    }
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

    #[test]
    fn record_detects_first_sight_then_stability() {
        let mut poller = StatusPoller::new();
        let set_a: HashSet<String> = ["aaaa".to_string()].into_iter().collect();

        // First observation of a repo counts as a change.
        assert!(poller.record("repo1".into(), set_a.clone()));
        // Same set again → no change.
        assert!(!poller.record("repo1".into(), set_a.clone()));
    }

    #[test]
    fn record_detects_added_and_removed_snapshots() {
        let mut poller = StatusPoller::new();
        let one: HashSet<String> = ["aaaa".to_string()].into_iter().collect();
        let two: HashSet<String> = ["aaaa".to_string(), "bbbb".to_string()]
            .into_iter()
            .collect();

        assert!(poller.record("repo1".into(), one.clone()));
        // Snapshot added.
        assert!(poller.record("repo1".into(), two.clone()));
        // Snapshot removed (the #159 delete case).
        assert!(poller.record("repo1".into(), one.clone()));
        // Stable.
        assert!(!poller.record("repo1".into(), one));
    }

    #[test]
    fn record_tracks_repos_independently() {
        let mut poller = StatusPoller::new();
        let set: HashSet<String> = ["aaaa".to_string()].into_iter().collect();

        assert!(poller.record("repo1".into(), set.clone()));
        // A different repo's first sight is independent.
        assert!(poller.record("repo2".into(), set.clone()));
        assert!(!poller.record("repo1".into(), set));
    }
}
