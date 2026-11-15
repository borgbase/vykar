//! Commit-time check that every pack a session's index delta references is
//! still present in storage.
//!
//! This is the backstop against a concurrent `compact` deleting a session's
//! packs after its marker was reaped: it runs under the commit lock and checks
//! **every** pack. Only the request pattern is chosen here — never which packs
//! are checked. Each strategy issues independent read-only requests, so they
//! may run concurrently without changing what is verified.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

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
    /// One recursive `list("packs/")` for the whole repository.
    FlatList,
}

/// Pick the strategy with the fewest sequential round trips when `width`
/// requests run at once. `repo_packs` estimates the packs in the repository
/// (persisted plus this session's); it only steers cost, never correctness.
/// Ties go to the request that returns less data: `exists` before shard
/// listings before the flat listing.
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
    // A paginated listing follows continuation tokens, so its pages are serial.
    let flat_rounds = repo_packs.div_ceil(LIST_PAGE_KEYS).max(1);
    if exists_rounds <= shard_rounds && exists_rounds <= flat_rounds {
        Strategy::Exists
    } else if shard_rounds <= flat_rounds {
        Strategy::ShardLists
    } else {
        Strategy::FlatList
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
            run_bounded(width, &groups, |(shard, ids)| {
                verify_shard(storage, shard, ids)
            })
        }
        Strategy::FlatList => {
            drop(by_shard);
            match storage.list("packs/") {
                Ok(keys) => {
                    let mut expected: HashMap<String, PackId> =
                        pack_ids.iter().map(|id| (id.storage_key(), *id)).collect();
                    for key in &keys {
                        expected.remove(key.as_str());
                    }
                    match expected.values().next() {
                        Some(id) => Err(missing(id)),
                        None => Ok(()),
                    }
                }
                Err(e) => {
                    debug!(error = %e, "flat pack listing failed, falling back to exists()");
                    let ids: Vec<PackId> = pack_ids.iter().copied().collect();
                    check_exists(storage, &ids, width)
                }
            }
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
    run_bounded(width, ids, |id| {
        if storage.exists(&id.storage_key())? {
            Ok(())
        } else {
            Err(missing(id))
        }
    })
}

/// Run `task` on every item, on up to `width` scoped threads. Stops handing
/// out work after the first error and returns it; `Ok` only if every task
/// succeeded. With `width <= 1` (or a single item) it runs inline, in order.
fn run_bounded<T, F>(width: usize, items: &[T], task: F) -> Result<()>
where
    T: Sync,
    F: Fn(&T) -> Result<()> + Sync,
{
    let threads = width.min(items.len());
    if threads <= 1 {
        return items.iter().try_for_each(task);
    }
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let first_err: Mutex<Option<VykarError>> = Mutex::new(None);
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| loop {
                if failed.load(Ordering::Relaxed) {
                    break;
                }
                let Some(item) = items.get(next.fetch_add(1, Ordering::Relaxed)) else {
                    break;
                };
                if let Err(e) = task(item) {
                    failed.store(true, Ordering::Relaxed);
                    let mut slot = first_err.lock().expect("pack verify error lock poisoned");
                    if slot.is_none() {
                        *slot = Some(e);
                    }
                    break;
                }
            });
        }
    });
    match first_err
        .into_inner()
        .expect("pack verify error lock poisoned")
    {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MemoryBackend;
    use std::sync::atomic::AtomicUsize;
    use vykar_storage::{delegate_storage_backend, InnerBackend};

    /// Counts requests and can fail listings or `exists()`.
    struct Probe {
        inner: MemoryBackend,
        fail_list: bool,
        fail_exists: bool,
        lists: AtomicUsize,
        exists: AtomicUsize,
    }

    impl Probe {
        fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                fail_list: false,
                fail_exists: false,
                lists: AtomicUsize::new(0),
                exists: AtomicUsize::new(0),
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
        except [exists, list, list_sized];

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

    const ALL: [Strategy; 3] = [Strategy::Exists, Strategy::ShardLists, Strategy::FlatList];

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
        for s in [Strategy::ShardLists, Strategy::FlatList] {
            verify_packs_present_with(&storage, &ids, 0, 6, Some(s)).unwrap();
        }
        assert_eq!(storage.exists.load(Ordering::SeqCst), 200);

        storage.inner.delete(&pack(3).storage_key()).unwrap();
        for s in [Strategy::ShardLists, Strategy::FlatList] {
            let err = verify_packs_present_with(&storage, &ids, 0, 6, Some(s))
                .unwrap_err()
                .to_string();
            assert!(err.contains("missing from storage"), "{s:?}: {err}");
        }
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
    fn cost_model() {
        // Full backup into an empty repo: one flat page beats 157 shards.
        assert_eq!(choose_strategy(245, 157, 245, 6), Strategy::FlatList);
        // Incremental with a couple of packs: a single round of HEADs.
        assert_eq!(choose_strategy(2, 2, 10_000, 6), Strategy::Exists);
        assert_eq!(choose_strategy(1, 1, 1, 1), Strategy::Exists);
        // Large repository, many new packs: shard listings.
        assert_eq!(choose_strategy(245, 157, 100_000, 6), Strategy::ShardLists);
        // Serial width keeps the shard listing where it wins.
        assert_eq!(choose_strategy(245, 157, 50_000, 1), Strategy::FlatList);
        assert_eq!(choose_strategy(245, 157, 200_000, 1), Strategy::ShardLists);
    }
}
