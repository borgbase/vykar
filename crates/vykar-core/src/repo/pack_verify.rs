//! Commit-time check that every pack a session's index delta references is
//! still present in storage.
//!
//! This is the backstop against a concurrent `compact` deleting a session's
//! packs after its marker was reaped: it runs under the commit lock and checks
//! **every** pack. Only the request pattern is chosen here — never which packs
//! are checked. Each strategy issues independent read-only requests, so they
//! may run concurrently without changing what is verified.
//!
//! A backend with a batch size API (vykar-server's `?sizes`) answers for all
//! packs in one request. Without one — any other backend, an old server, or
//! a failed batch request — the per-pack or per-shard strategies run instead,
//! so the batch path can only make verification cheaper, never weaker.

use std::collections::{HashMap, HashSet};

use tracing::debug;
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// Keys per page of a paginated listing (the S3 ListObjectsV2 maximum).
const LIST_PAGE_KEYS: usize = 1000;

/// Number of pack shard directories.
const PACK_SHARDS: usize = 256;

/// How the presence of the packs is established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Strategy {
    /// One `exists()` per pack.
    Exists,
    /// One `list("packs/<xx>")` per touched shard.
    ShardLists,
}

/// Pick the strategy with the fewest sequential round trips when `width`
/// requests run at once. `repo_packs` estimates the packs in the repository
/// (persisted plus this session's); it only steers cost, never correctness.
/// Ties go to `exists`, which returns less data.
///
/// A recursive `list("packs/")` is deliberately not a candidate: it is one
/// paginated request on S3, but a serial walk of all 256 shard directories
/// on SFTP (tens of seconds at WAN latency), and nothing here can tell the
/// two apart.
pub(crate) fn choose_strategy(
    packs: usize,
    shards: usize,
    repo_packs: usize,
    width: usize,
) -> Strategy {
    let width = width.max(1);
    let exists_rounds = packs.div_ceil(width);
    let pages_per_shard = repo_packs
        .div_ceil(PACK_SHARDS)
        .div_ceil(LIST_PAGE_KEYS)
        .max(1);
    let shard_rounds = shards.div_ceil(width).saturating_mul(pages_per_shard);
    if exists_rounds <= shard_rounds {
        Strategy::Exists
    } else {
        Strategy::ShardLists
    }
}

fn missing(pack_id: &PackId) -> VykarError {
    VykarError::Other(format!(
        "commit failed: pack {pack_id} missing from storage (stale session or concurrent deletion)"
    ))
}

/// Verify that every pack in `pack_ids` exists in `storage`, issuing up to
/// `width` requests at once. Returns the "pack ... missing" error for an
/// absent pack, and propagates any `exists()` error. A failed listing is not
/// an error by itself: the packs it would have covered fall back to
/// `exists()`.
pub(crate) fn verify_packs_present(
    storage: &dyn StorageBackend,
    pack_ids: &HashSet<PackId>,
    persisted_packs: usize,
    width: usize,
) -> Result<()> {
    verify_packs_present_with(storage, pack_ids, persisted_packs, width, None)
}

/// [`verify_packs_present`] with an optional forced strategy (tests).
pub(crate) fn verify_packs_present_with(
    storage: &dyn StorageBackend,
    pack_ids: &HashSet<PackId>,
    persisted_packs: usize,
    width: usize,
    force: Option<Strategy>,
) -> Result<()> {
    if pack_ids.is_empty() {
        return Ok(());
    }
    // A forced strategy (tests) skips the batch to exercise the fallbacks.
    if force.is_none() {
        if let Some(result) = verify_batch(storage, pack_ids) {
            return result;
        }
    }
    let mut by_shard: HashMap<String, Vec<PackId>> = HashMap::new();
    for id in pack_ids {
        by_shard.entry(id.shard_prefix()).or_default().push(*id);
    }
    let repo_packs = persisted_packs.saturating_add(pack_ids.len());
    let strategy =
        force.unwrap_or_else(|| choose_strategy(pack_ids.len(), by_shard.len(), repo_packs, width));
    debug!(
        packs = pack_ids.len(),
        shards = by_shard.len(),
        repo_packs,
        width,
        ?strategy,
        "verifying session packs"
    );

    match strategy {
        Strategy::Exists => {
            let ids: Vec<PackId> = pack_ids.iter().copied().collect();
            check_exists(storage, &ids, width)
        }
        Strategy::ShardLists => {
            let groups: Vec<(String, Vec<PackId>)> = by_shard.into_iter().collect();
            super::bounded::try_for_each_bounded(width, &groups, |(shard, ids)| {
                verify_shard(storage, shard, ids)
            })
        }
    }
}

/// Check every pack with one [`StorageBackend::object_sizes`] batch.
///
/// `None` when the batch gave no answer (unsupported, or the request failed):
/// the caller then verifies with per-pack or per-shard requests, so an error
/// here never counts as "present". An answer is authoritative: a pack it
/// reports absent fails the commit, as a failed `exists()` would.
fn verify_batch(storage: &dyn StorageBackend, pack_ids: &HashSet<PackId>) -> Option<Result<()>> {
    let ids: Vec<PackId> = pack_ids.iter().copied().collect();
    let keys: Vec<String> = ids.iter().map(PackId::storage_key).collect();
    match storage.object_sizes(&keys) {
        Ok(sizes) if sizes.len() == ids.len() => Some(
            match ids.iter().zip(&sizes).find(|(_, size)| size.is_none()) {
                Some((id, _)) => Err(missing(id)),
                None => Ok(()),
            },
        ),
        Ok(sizes) => {
            debug!(
                expected = ids.len(),
                got = sizes.len(),
                "batch pack sizes answered the wrong count, falling back"
            );
            None
        }
        Err(VykarError::UnsupportedBackend(_)) => None,
        Err(e) => {
            debug!(error = %e, "batch pack sizes failed, falling back");
            None
        }
    }
}

/// List one shard and check its expected packs against that listing only.
/// The listing is dropped before returning, so no global key set is built.
fn verify_shard(storage: &dyn StorageBackend, shard: &str, ids: &[PackId]) -> Result<()> {
    match storage.list(&format!("packs/{shard}")) {
        Ok(keys) => {
            let listed: HashSet<&str> = keys.iter().map(String::as_str).collect();
            for id in ids {
                if !listed.contains(id.storage_key().as_str()) {
                    return Err(missing(id));
                }
            }
            Ok(())
        }
        Err(e) => {
            // list() failed for this shard — per-pack exists() with error
            // propagation, exactly as the serial implementation did.
            debug!(shard, error = %e, "shard listing failed, falling back to exists()");
            for id in ids {
                if !storage.exists(&id.storage_key())? {
                    return Err(missing(id));
                }
            }
            Ok(())
        }
    }
}

fn check_exists(storage: &dyn StorageBackend, ids: &[PackId], width: usize) -> Result<()> {
    super::bounded::try_for_each_bounded(width, ids, |id| {
        if storage.exists(&id.storage_key())? {
            Ok(())
        } else {
            Err(missing(id))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MemoryBackend;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use vykar_storage::{delegate_storage_backend, InnerBackend};

    /// Counts requests and can fail listings or `exists()`. With `batch` it
    /// also answers `object_sizes` (one request), which `fail_batch` fails.
    struct Probe {
        inner: MemoryBackend,
        fail_list: bool,
        fail_exists: bool,
        batch: bool,
        fail_batch: bool,
        lists: AtomicUsize,
        exists: AtomicUsize,
        batches: AtomicUsize,
    }

    impl Probe {
        fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                fail_list: false,
                fail_exists: false,
                batch: false,
                fail_batch: false,
                lists: AtomicUsize::new(0),
                exists: AtomicUsize::new(0),
                batches: AtomicUsize::new(0),
            }
        }

        fn with_batch() -> Self {
            Self {
                batch: true,
                ..Self::new()
            }
        }
    }

    impl InnerBackend for Probe {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for Probe;
        except [exists, list, list_sized, object_sizes];

        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            if self.fail_list {
                return Err(VykarError::Other("injected list failure".into()));
            }
            self.inner.list(prefix)
        }

        fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            if self.fail_list {
                return Err(VykarError::Other("injected list failure".into()));
            }
            self.inner.list_sized(prefix)
        }

        fn exists(&self, key: &str) -> Result<bool> {
            self.exists.fetch_add(1, Ordering::SeqCst);
            if self.fail_exists {
                return Err(VykarError::Other("injected exists failure".into()));
            }
            self.inner.exists(key)
        }

        fn object_sizes(&self, keys: &[String]) -> Result<Vec<Option<u64>>> {
            if !self.batch {
                return Err(VykarError::UnsupportedBackend("probe".into()));
            }
            self.batches.fetch_add(1, Ordering::SeqCst);
            if self.fail_batch {
                return Err(VykarError::Other("injected batch failure".into()));
            }
            keys.iter().map(|k| self.inner.size(k)).collect()
        }
    }

    fn pack(i: u32) -> PackId {
        let mut b = [0u8; 32];
        // Spread across shards, several packs per shard.
        b[0] = (i % 40) as u8;
        b[1..5].copy_from_slice(&i.to_le_bytes());
        PackId::from_hex(&hex::encode(b)).unwrap()
    }

    fn stored(storage: &Probe, n: u32) -> HashSet<PackId> {
        (0..n)
            .map(|i| {
                let id = pack(i);
                storage.inner.put(&id.storage_key(), b"p").unwrap();
                id
            })
            .collect()
    }

    const ALL: [Strategy; 2] = [Strategy::Exists, Strategy::ShardLists];

    #[test]
    fn all_present_passes_every_strategy() {
        let storage = Probe::new();
        let ids = stored(&storage, 100);
        // An unrelated pack in the repository must not matter.
        storage.inner.put(&pack(500).storage_key(), b"x").unwrap();
        for s in ALL {
            for width in [1, 6] {
                verify_packs_present_with(&storage, &ids, 0, width, Some(s)).unwrap();
            }
        }
    }

    #[test]
    fn missing_pack_fails_every_strategy() {
        let storage = Probe::new();
        let ids = stored(&storage, 100);
        let gone = pack(37);
        storage.inner.delete(&gone.storage_key()).unwrap();
        for s in ALL {
            for width in [1, 6] {
                let err = verify_packs_present_with(&storage, &ids, 0, width, Some(s))
                    .unwrap_err()
                    .to_string();
                assert!(
                    err.contains(&format!("pack {gone} missing from storage")),
                    "{s:?}/{width}: {err}"
                );
            }
        }
    }

    #[test]
    fn failed_listing_falls_back_to_exists() {
        let mut storage = Probe::new();
        storage.fail_list = true;
        let ids = stored(&storage, 100);
        verify_packs_present_with(&storage, &ids, 0, 6, Some(Strategy::ShardLists)).unwrap();
        assert_eq!(storage.exists.load(Ordering::SeqCst), 100);

        storage.inner.delete(&pack(3).storage_key()).unwrap();
        let err = verify_packs_present_with(&storage, &ids, 0, 6, Some(Strategy::ShardLists))
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing from storage"), "{err}");
    }

    #[test]
    fn exists_errors_propagate() {
        let mut storage = Probe::new();
        storage.fail_list = true;
        storage.fail_exists = true;
        let ids = stored(&storage, 50);
        for s in ALL {
            let err = verify_packs_present_with(&storage, &ids, 0, 6, Some(s))
                .unwrap_err()
                .to_string();
            assert!(err.contains("injected exists failure"), "{s:?}: {err}");
        }
    }

    #[test]
    fn shard_lists_issue_one_request_per_touched_shard() {
        let storage = Probe::new();
        let ids = stored(&storage, 100);
        verify_packs_present_with(&storage, &ids, 0, 6, Some(Strategy::ShardLists)).unwrap();
        assert_eq!(storage.lists.load(Ordering::SeqCst), 40);
        assert_eq!(storage.exists.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn batch_checks_every_pack_in_one_request() {
        let storage = Probe::with_batch();
        let ids = stored(&storage, 100);
        verify_packs_present(&storage, &ids, 0, 6).unwrap();
        assert_eq!(storage.batches.load(Ordering::SeqCst), 1);
        assert_eq!(storage.lists.load(Ordering::SeqCst), 0);
        assert_eq!(storage.exists.load(Ordering::SeqCst), 0);

        let gone = pack(37);
        storage.inner.delete(&gone.storage_key()).unwrap();
        let err = verify_packs_present(&storage, &ids, 0, 6)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(&format!("pack {gone} missing from storage")),
            "{err}"
        );
        // An absent pack is the batch's verdict, not a reason to re-check.
        assert_eq!(storage.lists.load(Ordering::SeqCst), 0);
        assert_eq!(storage.exists.load(Ordering::SeqCst), 0);
    }

    /// A failed batch never counts as "present": verification falls back to
    /// the per-shard/per-pack requests, which still catch a missing pack.
    #[test]
    fn failed_batch_falls_back_to_full_verification() {
        let mut storage = Probe::with_batch();
        storage.fail_batch = true;
        let ids = stored(&storage, 100);
        verify_packs_present(&storage, &ids, 0, 6).unwrap();
        assert_eq!(storage.batches.load(Ordering::SeqCst), 1);
        assert!(storage.lists.load(Ordering::SeqCst) + storage.exists.load(Ordering::SeqCst) > 0);

        storage.inner.delete(&pack(3).storage_key()).unwrap();
        let err = verify_packs_present(&storage, &ids, 0, 6)
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing from storage"), "{err}");
    }

    /// Without a batch API the cost model's requests run, as before.
    #[test]
    fn unsupported_batch_uses_the_cost_model() {
        let storage = Probe::new();
        let ids = stored(&storage, 100);
        verify_packs_present(&storage, &ids, 0, 6).unwrap();
        assert_eq!(storage.batches.load(Ordering::SeqCst), 0);
        assert_eq!(storage.lists.load(Ordering::SeqCst), 40);
    }

    #[test]
    fn cost_model() {
        // Full backup: 157 shard listings need fewer rounds than 245 HEADs.
        assert_eq!(choose_strategy(245, 157, 245, 6), Strategy::ShardLists);
        // Incremental with a couple of packs: a single round of HEADs.
        assert_eq!(choose_strategy(2, 2, 10_000, 6), Strategy::Exists);
        assert_eq!(choose_strategy(1, 1, 1, 1), Strategy::Exists);
        // One pack per shard: HEADs tie with listings and win.
        assert_eq!(choose_strategy(40, 40, 40, 6), Strategy::Exists);
        // Shards so full that each listing paginates: HEADs.
        assert_eq!(choose_strategy(245, 157, 1_000_000, 6), Strategy::Exists);
    }
}
