//! Object sizes for a set of packs, shared by commit verification
//! (`pack_verify`) and journal recovery (`pack_recovery`). This module only
//! picks the request pattern; each caller decides what an answer means.
//!
//! A backend with a batch size API (vykar-server's `?sizes`) answers for all
//! packs in one request, and that answer is authoritative. Without one — any
//! other backend, an old server, or a failed batch request — the cost model
//! picks per-pack `size()` or per-shard sized listings, so the batch can only
//! make a lookup cheaper, never weaker. Every request is independent and
//! read-only, so requests may run concurrently without changing the answer.
//!
//! A recursive `list("packs/")` is deliberately not a strategy: it is one
//! paginated request on S3, but a serial walk of all 256 shard directories on
//! SFTP (tens of seconds at WAN latency), and nothing here can tell the two
//! apart.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, PoisonError};

use super::control::CancelToken;
use super::pack_listing::PACKS_PREFIX;
use tracing::debug;
use vykar_storage::{ServerOp, StorageBackend};
use vykar_types::error::Result;
use vykar_types::pack_id::PackId;

/// Keys per page of a paginated listing (the S3 ListObjectsV2 maximum).
const LIST_PAGE_KEYS: usize = 1000;

/// Number of pack shard directories.
const PACK_SHARDS: usize = 256;

/// Per pack: `Ok(Some(size))` present, `Ok(None)` absent, `Err` the lookup
/// failed. A pack with no entry was not looked up (only after
/// [`OnFailure::Stop`]); callers treat that like a failure, never as present.
pub(crate) type PackSizes = HashMap<PackId, Result<Option<u64>>>;

/// How sizes are fetched when the batch gave no answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Strategy {
    /// One `size()` per pack.
    PerPack,
    /// One `list_sized("packs/<xx>")` per touched shard; `size()` for packs
    /// the listing names without a size, and for every pack of a shard whose
    /// listing failed. On the local backend a sized listing stats every file
    /// in the shard, a cost [`choose_strategy`] does not model.
    ShardLists,
}

/// Whether the lookup keeps going after a pack that is absent or failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnFailure {
    /// Look up every pack (journal recovery: rejects, never aborts).
    Continue,
    /// Start no new request once any pack is absent or failed (commit
    /// verification, under the commit lock). Packs not reached have no entry.
    Stop,
}

/// Pick the strategy with the fewest sequential round trips when `width`
/// requests run at once. `repo_packs` estimates the packs in the repository
/// (persisted plus the ones looked up); it only steers cost, never
/// correctness. Ties go to per-pack, which returns less data.
pub(crate) fn choose_strategy(
    packs: usize,
    shards: usize,
    repo_packs: usize,
    width: usize,
) -> Strategy {
    let width = width.max(1);
    let per_pack_rounds = packs.div_ceil(width);
    let pages_per_shard = repo_packs
        .div_ceil(PACK_SHARDS)
        .div_ceil(LIST_PAGE_KEYS)
        .max(1);
    let shard_rounds = shards.div_ceil(width).saturating_mul(pages_per_shard);
    if per_pack_rounds <= shard_rounds {
        Strategy::PerPack
    } else {
        Strategy::ShardLists
    }
}

/// Sizes of `ids` (duplicates are looked up once), with up to `width`
/// requests in flight. `repo_packs` estimates the packs in the repository and
/// only steers cost. `force` skips the batch and the cost model (tests).
pub(crate) fn lookup(
    storage: &dyn StorageBackend,
    ids: &[PackId],
    repo_packs: usize,
    width: usize,
    force: Option<Strategy>,
    on_failure: OnFailure,
) -> PackSizes {
    let unique: Vec<PackId> = ids
        .iter()
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if unique.is_empty() {
        return PackSizes::new();
    }
    if force.is_none()
        && let Some(sizes) = batch(storage, &unique)
    {
        return sizes;
    }
    let mut by_shard: HashMap<String, Vec<PackId>> = HashMap::new();
    for id in &unique {
        by_shard
            .entry(format!("packs/{}", id.shard_prefix()))
            .or_default()
            .push(*id);
    }
    let strategy =
        force.unwrap_or_else(|| choose_strategy(unique.len(), by_shard.len(), repo_packs, width));
    debug!(
        packs = unique.len(),
        shards = by_shard.len(),
        repo_packs,
        width,
        ?strategy,
        ?on_failure,
        "looking up pack sizes"
    );

    let out = Mutex::new(PackSizes::with_capacity(unique.len()));
    let record = || out.lock().unwrap_or_else(PoisonError::into_inner);
    match strategy {
        Strategy::PerPack => fan_out(width, &unique, on_failure, |id| {
            let r = size_of(storage, id);
            let ok = matches!(r, Ok(Some(_)));
            record().insert(*id, r);
            ok
        }),
        Strategy::ShardLists => {
            let groups: Vec<(String, Vec<PackId>)> = by_shard.into_iter().collect();
            fan_out(width, &groups, on_failure, |(prefix, ids)| {
                let rs = shard(storage, prefix, ids, on_failure);
                let ok = rs.iter().all(|(_, r)| matches!(r, Ok(Some(_))));
                record().extend(rs);
                ok
            });
        }
    }
    out.into_inner().unwrap_or_else(PoisonError::into_inner)
}

/// Store-clock mtimes of `ids`: one `list_meta("packs/<xx>/")` per shard
/// they fall in, up to `width` at once, none started once `cancel` fires. A
/// pack the listings do not name, or name without an mtime, maps to `None`;
/// a failed shard listing maps its packs to `None`, and a pack whose shard
/// was not listed (cancelled) has no entry (callers treat both as unknown
/// age). That is min(packs, 256) listings, each paged at
/// `LIST_PAGE_KEYS`: a journal of a few packs touches a few shards, one of
/// thousands touches all 256. Per shard rather than one tree listing
/// (vykar-server's `ListTree` fills mtimes too): a tree listing returns
/// every pack of the repository however few the journal names.
pub(crate) fn pack_mtimes(
    storage: &dyn StorageBackend,
    ids: &[PackId],
    width: usize,
    cancel: &CancelToken,
) -> HashMap<PackId, Option<std::time::SystemTime>> {
    let mut by_shard: HashMap<String, Vec<PackId>> = HashMap::new();
    for id in ids {
        by_shard
            .entry(format!("{PACKS_PREFIX}{}/", id.shard_prefix()))
            .or_default()
            .push(*id);
    }
    let groups: Vec<(String, Vec<PackId>)> = by_shard.into_iter().collect();
    let out = Mutex::new(HashMap::with_capacity(ids.len()));
    super::bounded::for_each_bounded(width.max(1), &groups, cancel, |(prefix, ids)| {
        let listed: HashMap<String, Option<std::time::SystemTime>> = match storage.list_meta(prefix)
        {
            Ok(objs) => objs.into_iter().map(|o| (o.key, o.mtime)).collect(),
            Err(e) => {
                debug!(shard = %prefix, error = %e, "shard listing failed; pack ages unknown");
                HashMap::new()
            }
        };
        let mut out = out.lock().unwrap_or_else(PoisonError::into_inner);
        for id in ids {
            out.insert(*id, listed.get(&id.storage_key()).copied().flatten());
        }
    });
    out.into_inner().unwrap_or_else(PoisonError::into_inner)
}

/// One `object_sizes` batch. `None` = no answer (not offered, failed, wrong
/// count): the caller falls back, so a failed batch never counts as present.
/// An answer is authoritative: `None` in it is absent, not re-checked.
fn batch(storage: &dyn StorageBackend, ids: &[PackId]) -> Option<PackSizes> {
    let server = storage
        .server_ops()
        .filter(|s| s.offers(ServerOp::ObjectSizes))?;
    let keys: Vec<String> = ids.iter().map(PackId::storage_key).collect();
    match server.object_sizes(&keys) {
        Ok(sizes) if sizes.len() == ids.len() => Some(
            ids.iter()
                .copied()
                .zip(sizes)
                .map(|(id, s)| (id, Ok(s)))
                .collect(),
        ),
        Ok(sizes) => {
            debug!(
                expected = ids.len(),
                got = sizes.len(),
                "batch pack sizes answered the wrong count, falling back"
            );
            None
        }
        Err(e) => {
            debug!(error = %e, "batch pack sizes failed, falling back");
            None
        }
    }
}

/// Run `task` per item with up to `width` threads. `task` returns `true` when
/// every pack it covered was present. With `OnFailure::Stop` the first `false`
/// stops handing out items; with `Continue` every item runs.
fn fan_out<T: Sync>(
    width: usize,
    items: &[T],
    on_failure: OnFailure,
    task: impl Fn(&T) -> bool + Sync,
) {
    match on_failure {
        OnFailure::Stop => {
            let _ = super::bounded::try_for_each_bounded(width, items, |t| {
                if task(t) { Ok(()) } else { Err(()) }
            });
        }
        OnFailure::Continue => {
            super::bounded::for_each_bounded(width, items, &CancelToken::new(), |t| {
                task(t);
            })
        }
    }
}

fn size_of(storage: &dyn StorageBackend, id: &PackId) -> Result<Option<u64>> {
    storage.size(&id.storage_key())
}

/// One shard: its sized listing, `size()` where the listing has no size or
/// failed. With `Stop`, nothing after the first absent/failed pack. Returns
/// the outcomes recorded (all of `ids` unless stopped).
fn shard(
    storage: &dyn StorageBackend,
    shard: &str,
    ids: &[PackId],
    on_failure: OnFailure,
) -> Vec<(PackId, Result<Option<u64>>)> {
    let listed: Option<HashMap<String, Option<u64>>> = match storage.list_sized(shard) {
        Ok(objs) => Some(objs.into_iter().map(|o| (o.key, o.size)).collect()),
        Err(e) => {
            debug!(shard, error = %e, "shard listing failed, falling back to per-pack size()");
            None
        }
    };
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let r = match &listed {
            Some(map) => match map.get(&id.storage_key()) {
                Some(Some(n)) => Ok(Some(*n)),
                Some(None) => size_of(storage, id),
                None => Ok(None),
            },
            None => size_of(storage, id),
        };
        let failed = !matches!(r, Ok(Some(_)));
        out.push((*id, r));
        if failed && on_failure == OnFailure::Stop {
            break;
        }
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::testutil::MemoryBackend;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::ThreadId;
    use std::time::Duration;
    use vykar_storage::{InnerBackend, ListedObject, delegate_storage_backend};
    use vykar_types::error::VykarError;

    /// Counts requests; flags pick the request path and inject failures.
    pub(crate) struct Probe {
        pub(crate) inner: MemoryBackend,
        /// Offer `object_sizes` (a server behind it); otherwise `server_ops()` is None.
        pub(crate) batch: bool,
        pub(crate) fail_batch: bool,
        /// Drop the batch's last answer.
        pub(crate) short_batch: bool,
        /// `list_sized` reports sizes (like S3/SFTP/local); otherwise `size: None`.
        pub(crate) sized_listing: bool,
        /// Fails `list` and `list_sized`.
        pub(crate) fail_list: bool,
        pub(crate) fail_size: bool,
        pub(crate) batches: AtomicUsize,
        /// `list` and `list_sized` calls.
        pub(crate) lists: AtomicUsize,
        pub(crate) sizes: AtomicUsize,
        /// Lookups must never call `exists()`.
        pub(crate) exists: AtomicUsize,
        /// In-flight `list_sized` + `size` calls.
        pub(crate) in_flight: AtomicUsize,
        pub(crate) max_in_flight: AtomicUsize,
        pub(crate) list_threads: Mutex<Vec<ThreadId>>,
    }

    impl Probe {
        pub(crate) fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                batch: false,
                fail_batch: false,
                short_batch: false,
                sized_listing: false,
                fail_list: false,
                fail_size: false,
                batches: AtomicUsize::new(0),
                lists: AtomicUsize::new(0),
                sizes: AtomicUsize::new(0),
                exists: AtomicUsize::new(0),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                list_threads: Mutex::new(Vec::new()),
            }
        }

        pub(crate) fn with_batch() -> Self {
            Self {
                batch: true,
                ..Self::new()
            }
        }

        pub(crate) fn count(c: &AtomicUsize) -> usize {
            c.load(Ordering::SeqCst)
        }

        fn enter(&self) {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(1));
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl InnerBackend for Probe {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for Probe;
        except [exists, list, size, list_sized, list_meta, server_ops];

        fn list_meta(
            &self,
            prefix: &str,
        ) -> vykar_types::error::Result<Vec<vykar_storage::ListedObject>> {
            self.list_sized(prefix)
        }

        fn exists(&self, key: &str) -> Result<bool> {
            self.exists.fetch_add(1, Ordering::SeqCst);
            self.inner.exists(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            if self.fail_list {
                return Err(VykarError::Other("injected list failure".into()));
            }
            self.inner.list(prefix)
        }

        fn size(&self, key: &str) -> Result<Option<u64>> {
            self.sizes.fetch_add(1, Ordering::SeqCst);
            self.enter();
            if self.fail_size {
                return Err(VykarError::Other("injected size failure".into()));
            }
            self.inner.size(key)
        }

        fn list_sized(&self, prefix: &str) -> Result<Vec<ListedObject>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.list_threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            self.enter();
            if self.fail_list {
                return Err(VykarError::Other("injected list failure".into()));
            }
            self.inner
                .list(prefix)?
                .into_iter()
                .map(|key| {
                    let size = if self.sized_listing {
                        self.inner.size(&key)?
                    } else {
                        None
                    };
                    Ok(ListedObject {
                        key,
                        size,
                        mtime: None,
                    })
                })
                .collect()
        }

        /// A server answering `?sizes` exactly when `batch`.
        fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
            self.batch.then_some(self as &dyn vykar_storage::ServerOps)
        }
    }

    impl vykar_storage::ServerOps for Probe {
        fn offers(&self, op: vykar_storage::ServerOp) -> bool {
            op == vykar_storage::ServerOp::ObjectSizes
        }

        fn capabilities(&self) -> Result<vykar_protocol::ServerCapabilities> {
            Ok(vykar_protocol::ServerCapabilities::default())
        }

        fn init(&self) -> Result<()> {
            Ok(())
        }

        fn object_sizes(&self, keys: &[String]) -> Result<Vec<Option<u64>>> {
            self.batches.fetch_add(1, Ordering::SeqCst);
            if self.fail_batch {
                return Err(VykarError::Other("injected batch failure".into()));
            }
            let mut sizes: Vec<Option<u64>> = keys
                .iter()
                .map(|k| self.inner.size(k))
                .collect::<Result<_>>()?;
            if self.short_batch {
                sizes.pop();
            }
            Ok(sizes)
        }
    }

    pub(crate) fn pack(i: u32) -> PackId {
        let mut b = [0u8; 32];
        // Spread across shards, several packs per shard.
        b[0] = (i % 40) as u8;
        b[1..5].copy_from_slice(&i.to_le_bytes());
        PackId::from_hex(&hex::encode(b)).unwrap()
    }

    fn len_of(i: u32) -> u64 {
        u64::from(10 + i % 7)
    }

    /// 100 packs over 40 shards, stored with length `len_of(i)`.
    fn stored(storage: &Probe) -> Vec<PackId> {
        (0..100)
            .map(|i| {
                let id = pack(i);
                storage
                    .inner
                    .put(&id.storage_key(), &vec![0u8; len_of(i) as usize])
                    .unwrap();
                id
            })
            .collect()
    }

    /// Never stored; in shard 0 like `pack(0)`.
    fn absent() -> PackId {
        pack(1_000)
    }

    fn ok(sizes: &PackSizes, id: &PackId) -> std::result::Result<Option<u64>, String> {
        match sizes.get(id) {
            Some(Ok(size)) => Ok(*size),
            Some(Err(e)) => Err(e.to_string()),
            None => Err("no entry".into()),
        }
    }

    type MakeProbe = fn() -> Probe;

    fn configs() -> Vec<(&'static str, MakeProbe)> {
        vec![
            ("batch", Probe::with_batch),
            ("fail_batch", || Probe {
                fail_batch: true,
                ..Probe::with_batch()
            }),
            ("short_batch", || Probe {
                short_batch: true,
                ..Probe::with_batch()
            }),
            ("sized_listing", || Probe {
                sized_listing: true,
                ..Probe::new()
            }),
            ("unsized_listing", Probe::new),
            ("fail_list", || Probe {
                fail_list: true,
                ..Probe::new()
            }),
        ]
    }

    const FORCES: [Option<Strategy>; 3] =
        [None, Some(Strategy::PerPack), Some(Strategy::ShardLists)];

    #[test]
    fn every_path_returns_the_same_sizes() {
        for (name, make) in configs() {
            for force in FORCES {
                for width in [1, 6] {
                    let storage = make();
                    let mut ids = stored(&storage);
                    ids.push(absent());
                    let sizes = lookup(&storage, &ids, 0, width, force, OnFailure::Continue);
                    let ctx = format!("{name}/{force:?}/{width}");
                    assert_eq!(sizes.len(), 101, "{ctx}");
                    for i in 0..100 {
                        assert_eq!(ok(&sizes, &pack(i)), Ok(Some(len_of(i))), "{ctx}");
                    }
                    assert_eq!(ok(&sizes, &absent()), Ok(None), "{ctx}");
                    assert_eq!(Probe::count(&storage.exists), 0, "{ctx}");
                }
            }
        }
    }

    #[test]
    fn batch_is_authoritative() {
        let storage = Probe::with_batch();
        let mut ids = stored(&storage);
        ids.push(absent());
        let sizes = lookup(&storage, &ids, 0, 6, None, OnFailure::Continue);
        assert_eq!(ok(&sizes, &absent()), Ok(None));
        assert_eq!(Probe::count(&storage.batches), 1);
        assert_eq!(Probe::count(&storage.lists), 0);
        assert_eq!(Probe::count(&storage.sizes), 0);
    }

    #[test]
    fn failed_or_short_batch_falls_back() {
        for (name, storage) in [
            (
                "fail_batch",
                Probe {
                    fail_batch: true,
                    ..Probe::with_batch()
                },
            ),
            (
                "short_batch",
                Probe {
                    short_batch: true,
                    ..Probe::with_batch()
                },
            ),
        ] {
            let ids = stored(&storage);
            let sizes = lookup(&storage, &ids, 0, 6, None, OnFailure::Continue);
            assert_eq!(sizes.len(), 100, "{name}");
            assert_eq!(Probe::count(&storage.batches), 1, "{name}");
            assert_eq!(Probe::count(&storage.lists), 40, "{name}");
        }
    }

    /// Listed packs without a size get one `size()`; unlisted ones none.
    #[test]
    fn unsized_listing_sizes_each_listed_pack() {
        let storage = Probe::new();
        let mut ids = stored(&storage);
        ids.push(absent());
        let sizes = lookup(
            &storage,
            &ids,
            0,
            6,
            Some(Strategy::ShardLists),
            OnFailure::Continue,
        );
        assert_eq!(ok(&sizes, &absent()), Ok(None));
        assert_eq!(Probe::count(&storage.sizes), 100);
    }

    #[test]
    fn sized_listing_needs_no_size() {
        let storage = Probe {
            sized_listing: true,
            ..Probe::new()
        };
        let ids = stored(&storage);
        lookup(
            &storage,
            &ids,
            0,
            6,
            Some(Strategy::ShardLists),
            OnFailure::Continue,
        );
        assert_eq!(Probe::count(&storage.lists), 40);
        assert_eq!(Probe::count(&storage.sizes), 0);
    }

    #[test]
    fn failed_listing_sizes_every_pack() {
        let storage = Probe {
            fail_list: true,
            ..Probe::new()
        };
        let ids = stored(&storage);
        lookup(
            &storage,
            &ids,
            0,
            6,
            Some(Strategy::ShardLists),
            OnFailure::Continue,
        );
        assert_eq!(Probe::count(&storage.lists), 40);
        assert_eq!(Probe::count(&storage.sizes), 100);
    }

    #[test]
    fn per_pack_sizes_each_pack_once() {
        let storage = Probe::new();
        let ids = stored(&storage);
        lookup(
            &storage,
            &ids,
            0,
            6,
            Some(Strategy::PerPack),
            OnFailure::Continue,
        );
        assert_eq!(Probe::count(&storage.sizes), 100);
        assert_eq!(Probe::count(&storage.lists), 0);
        assert_eq!(Probe::count(&storage.batches), 0);
    }

    #[test]
    fn continue_records_every_error() {
        for force in [Strategy::PerPack, Strategy::ShardLists] {
            let storage = Probe {
                fail_list: true,
                fail_size: true,
                ..Probe::new()
            };
            let ids = stored(&storage);
            let sizes = lookup(&storage, &ids, 0, 6, Some(force), OnFailure::Continue);
            assert_eq!(sizes.len(), 100, "{force:?}");
            assert!(sizes.values().all(Result::is_err), "{force:?}");
            assert_eq!(Probe::count(&storage.sizes), 100, "{force:?}");
        }
    }

    #[test]
    fn stop_starts_no_request_after_a_failure() {
        let failing = || Probe {
            fail_size: true,
            ..Probe::new()
        };

        let storage = failing();
        let ids = stored(&storage);
        let sizes = lookup(
            &storage,
            &ids,
            0,
            1,
            Some(Strategy::PerPack),
            OnFailure::Stop,
        );
        assert_eq!(Probe::count(&storage.sizes), 1);
        assert_eq!(sizes.len(), 1);
        assert!(sizes.values().all(Result::is_err));

        let storage = failing();
        let ids = stored(&storage);
        lookup(
            &storage,
            &ids,
            0,
            6,
            Some(Strategy::PerPack),
            OnFailure::Stop,
        );
        let n = Probe::count(&storage.sizes);
        assert!((1..=6).contains(&n), "sizes {n}");

        let storage = Probe {
            fail_list: true,
            ..failing()
        };
        let ids = stored(&storage);
        lookup(
            &storage,
            &ids,
            0,
            1,
            Some(Strategy::ShardLists),
            OnFailure::Stop,
        );
        assert_eq!(Probe::count(&storage.lists), 1);
        assert_eq!(Probe::count(&storage.sizes), 1);

        // An absent pack stops the lookup like an error does.
        let none_stored: Vec<PackId> = (1_000..1_100).map(pack).collect();
        let storage = Probe::new();
        let sizes = lookup(
            &storage,
            &none_stored,
            0,
            1,
            Some(Strategy::PerPack),
            OnFailure::Stop,
        );
        assert_eq!(Probe::count(&storage.sizes), 1);
        assert_eq!(sizes.len(), 1);
        assert!(sizes.values().all(|r| matches!(r, Ok(None))));

        let storage = Probe {
            sized_listing: true,
            ..Probe::new()
        };
        let sizes = lookup(
            &storage,
            &none_stored,
            0,
            1,
            Some(Strategy::ShardLists),
            OnFailure::Stop,
        );
        assert_eq!(Probe::count(&storage.lists), 1);
        assert_eq!(Probe::count(&storage.sizes), 0);
        assert_eq!(sizes.len(), 1);
        assert!(sizes.values().all(|r| matches!(r, Ok(None))));
    }

    #[test]
    fn duplicates_are_looked_up_once() {
        let storage = Probe::new();
        let ids = stored(&storage);
        let (a, b) = (ids[0], ids[1]);
        let sizes = lookup(
            &storage,
            &[a, a, b],
            0,
            1,
            Some(Strategy::PerPack),
            OnFailure::Continue,
        );
        assert_eq!(sizes.len(), 2);
        assert_eq!(Probe::count(&storage.sizes), 2);
    }

    #[test]
    fn empty_input_issues_no_requests() {
        let storage = Probe::with_batch();
        assert!(lookup(&storage, &[], 0, 6, None, OnFailure::Continue).is_empty());
        assert_eq!(Probe::count(&storage.batches), 0);
        assert_eq!(Probe::count(&storage.lists), 0);
        assert_eq!(Probe::count(&storage.sizes), 0);
        assert_eq!(Probe::count(&storage.exists), 0);
    }

    #[test]
    fn fan_out_respects_width() {
        let me = std::thread::current().id();

        let storage = Probe::new();
        let ids = stored(&storage);
        lookup(
            &storage,
            &ids,
            0,
            3,
            Some(Strategy::ShardLists),
            OnFailure::Continue,
        );
        let max = Probe::count(&storage.max_in_flight);
        assert!((1..=3).contains(&max), "max in flight {max}");
        // Wider than 1 fans out: no listing runs on the caller's thread.
        {
            let threads = storage.list_threads.lock().unwrap();
            assert_eq!(threads.len(), 40);
            assert!(threads.iter().all(|t| *t != me));
        }

        let storage = Probe::new();
        let ids = stored(&storage);
        lookup(
            &storage,
            &ids,
            0,
            1,
            Some(Strategy::ShardLists),
            OnFailure::Continue,
        );
        assert_eq!(Probe::count(&storage.max_in_flight), 1);
        // Width 1 runs inline on the caller's thread.
        {
            let threads = storage.list_threads.lock().unwrap();
            assert_eq!(threads.len(), 40);
            assert!(threads.iter().all(|t| *t == me));
        }

        let storage = Probe::new();
        let ids = stored(&storage);
        lookup(
            &storage,
            &ids,
            0,
            3,
            Some(Strategy::PerPack),
            OnFailure::Continue,
        );
        let max = Probe::count(&storage.max_in_flight);
        assert!((1..=3).contains(&max), "max in flight {max}");
    }

    #[test]
    fn cost_model() {
        // Full backup: 157 shard listings need fewer rounds than 245 HEADs.
        assert_eq!(choose_strategy(245, 157, 245, 6), Strategy::ShardLists);
        // Incremental with a couple of packs: a single round of HEADs.
        assert_eq!(choose_strategy(2, 2, 10_000, 6), Strategy::PerPack);
        assert_eq!(choose_strategy(1, 1, 1, 1), Strategy::PerPack);
        // One pack per shard: HEADs tie with listings and win.
        assert_eq!(choose_strategy(40, 40, 40, 6), Strategy::PerPack);
        // Shards so full that each listing paginates: HEADs.
        assert_eq!(choose_strategy(245, 157, 1_000_000, 6), Strategy::PerPack);
    }
}
