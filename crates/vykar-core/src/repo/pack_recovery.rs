//! Deciding which packs named in a recovered `pending_index` journal are safe
//! to adopt.
//!
//! A journal entry claims "pack P holds these chunks at these offsets". A
//! journal lists a pack only after its atomic upload returned `Ok`, so the
//! object at P's key is a complete upload of P. The length check here guards
//! against a rename lost to a server power failure and anything foreign at
//! the key.
//!
//! Sizes come from one batch request where the backend has one
//! (vykar-server's `?sizes`), as at commit (`pack_verify`); otherwise from one
//! sized listing per touched shard, several at once. Only the request pattern
//! differs between the two; the length check is the same.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, PoisonError};

use tracing::{debug, warn};

use crate::index::PendingPackEntry;
use vykar_storage::StorageBackend;
use vykar_types::error::VykarError;
use vykar_types::pack_id::PackId;

/// Exact on-disk length of a pack: `header | (u32 len | blob)*` with no
/// trailer, so the last blob's end. `None` on overflow or no chunks.
pub(crate) fn expected_pack_len(entry: &PendingPackEntry) -> Option<u64> {
    let mut max = None;
    for c in &entry.chunks {
        let end = c.pack_offset.checked_add(u64::from(c.stored_size))?;
        max = Some(max.map_or(end, |m: u64| m.max(end)));
    }
    max
}

/// Return the subset of `entries` whose packs are safe to adopt. Every
/// rejected pack is logged here. Up to `width` lookups run at once.
pub(crate) fn verify_journal_packs(
    storage: &dyn StorageBackend,
    entries: &[PendingPackEntry],
    width: usize,
) -> HashSet<PackId> {
    let mut sized: Vec<(&PendingPackEntry, u64)> = Vec::with_capacity(entries.len());
    for entry in entries {
        match expected_pack_len(entry) {
            Some(len) => sized.push((entry, len)),
            None => warn!(
                pack_id = %entry.pack_id,
                "pending_index: pack has no chunks or offsets overflow, skipping"
            ),
        }
    }

    let ids: Vec<PackId> = sized.iter().map(|(e, _)| e.pack_id).collect();
    let actual = lookup_sizes(storage, &ids, width);
    let mut accepted = HashSet::with_capacity(sized.len());
    for (entry, expected) in sized {
        match actual.get(&entry.pack_id).copied().flatten() {
            Some(size) if size == expected => {
                accepted.insert(entry.pack_id);
            }
            Some(size) => warn!(
                pack_id = %entry.pack_id,
                expected,
                actual = size,
                "pending_index: pack length mismatch, skipping"
            ),
            None => warn!(
                pack_id = %entry.pack_id,
                "pending_index: pack missing from storage, skipping"
            ),
        }
    }
    accepted
}

/// Object sizes for `packs`; `None` means missing (or its lookup failed).
///
/// One [`StorageBackend::object_sizes`] batch where the backend answers it.
/// Otherwise — no batch API, a failed batch, or an answer of the wrong
/// length — one sized listing per touched shard, up to `width` at once.
fn lookup_sizes(
    storage: &dyn StorageBackend,
    packs: &[PackId],
    width: usize,
) -> HashMap<PackId, Option<u64>> {
    if packs.is_empty() {
        return HashMap::new();
    }
    if let Some(sizes) = batch_sizes(storage, packs) {
        return sizes;
    }
    shard_sizes(storage, packs, width)
}

/// Sizes from one batch. `None` when the batch gave no answer; the caller
/// then lists shards. An answer is authoritative: an absent pack is missing.
fn batch_sizes(
    storage: &dyn StorageBackend,
    packs: &[PackId],
) -> Option<HashMap<PackId, Option<u64>>> {
    let keys: Vec<String> = packs.iter().map(PackId::storage_key).collect();
    match storage.object_sizes(&keys) {
        Ok(sizes) if sizes.len() == packs.len() => Some(packs.iter().copied().zip(sizes).collect()),
        Ok(sizes) => {
            debug!(
                expected = packs.len(),
                got = sizes.len(),
                "pending_index: batch pack sizes answered the wrong count, \
                 falling back to shard listings"
            );
            None
        }
        Err(VykarError::UnsupportedBackend(_)) => None,
        Err(e) => {
            debug!(
                error = %e,
                "pending_index: batch pack sizes failed, falling back to shard listings"
            );
            None
        }
    }
}

/// Sizes from one sized listing per touched shard, up to `width` at once.
fn shard_sizes(
    storage: &dyn StorageBackend,
    packs: &[PackId],
    width: usize,
) -> HashMap<PackId, Option<u64>> {
    let mut by_shard: HashMap<String, Vec<PackId>> = HashMap::new();
    for pack_id in packs {
        by_shard
            .entry(format!("packs/{}", pack_id.shard_prefix()))
            .or_default()
            .push(*pack_id);
    }
    let groups: Vec<(String, Vec<PackId>)> = by_shard.into_iter().collect();
    let out = Mutex::new(HashMap::with_capacity(packs.len()));
    super::bounded::for_each_bounded(width, &groups, None, |(shard, ids)| {
        let sizes = list_shard_sizes(storage, shard, ids);
        out.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(sizes);
    });
    out.into_inner().unwrap_or_else(PoisonError::into_inner)
}

/// One shard: its sized listing, with `size()` for each pack the listing
/// does not size, or for every pack when the listing fails.
fn list_shard_sizes(
    storage: &dyn StorageBackend,
    shard: &str,
    ids: &[PackId],
) -> Vec<(PackId, Option<u64>)> {
    let listed: Option<HashMap<String, Option<u64>>> = match storage.list_sized(shard) {
        Ok(objs) => Some(objs.into_iter().map(|o| (o.key, o.size)).collect()),
        Err(e) => {
            warn!("pending_index: failed to list {shard}: {e}, falling back to per-pack checks");
            None
        }
    };
    ids.iter()
        .map(|pack_id| {
            let size = match &listed {
                Some(map) => match map.get(&pack_id.storage_key()) {
                    Some(Some(size)) => Some(*size),
                    Some(None) => size_of(storage, pack_id),
                    None => None,
                },
                None => size_of(storage, pack_id),
            };
            (*pack_id, size)
        })
        .collect()
}

fn size_of(storage: &dyn StorageBackend, pack_id: &PackId) -> Option<u64> {
    match storage.size(&pack_id.storage_key()) {
        Ok(size) => size,
        Err(e) => {
            warn!(pack_id = %pack_id, "pending_index: size lookup failed: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::PendingChunkEntry;
    use crate::testutil::MemoryBackend;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::ThreadId;
    use std::time::Duration;
    use vykar_storage::{delegate_storage_backend, InnerBackend, ListedObject};
    use vykar_types::chunk_id::ChunkId;
    use vykar_types::error::Result;

    /// Counts requests; flags pick the request path and inject failures.
    struct Probe {
        inner: MemoryBackend,
        /// Answer `object_sizes`; otherwise report it unsupported.
        batch: bool,
        fail_batch: bool,
        /// Drop the batch's last answer.
        short_batch: bool,
        /// `list_sized` reports sizes (like REST/S3); otherwise `size: None`.
        sized_listing: bool,
        fail_list: bool,
        fail_size: bool,
        batches: AtomicUsize,
        lists: AtomicUsize,
        sizes: AtomicUsize,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        list_threads: Mutex<Vec<ThreadId>>,
    }

    impl Probe {
        fn new() -> Self {
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
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                list_threads: Mutex::new(Vec::new()),
            }
        }

        fn with_batch() -> Self {
            Self {
                batch: true,
                ..Self::new()
            }
        }

        fn count(c: &AtomicUsize) -> usize {
            c.load(Ordering::SeqCst)
        }
    }

    impl InnerBackend for Probe {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for Probe;
        except [list, size, list_sized, object_sizes];

        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            if self.fail_list {
                return Err(VykarError::Other("injected list failure".into()));
            }
            self.inner.list(prefix)
        }

        fn size(&self, key: &str) -> Result<Option<u64>> {
            self.sizes.fetch_add(1, Ordering::SeqCst);
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
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(1));
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
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
                    Ok(ListedObject { key, size })
                })
                .collect()
        }

        fn object_sizes(&self, keys: &[String]) -> Result<Vec<Option<u64>>> {
            if !self.batch {
                return Err(VykarError::UnsupportedBackend("probe".into()));
            }
            self.batches.fetch_add(1, Ordering::SeqCst);
            if self.fail_batch {
                return Err(VykarError::Other("injected batch failure".into()));
            }
            let mut sizes: Vec<Option<u64>> =
                keys.iter().map(|k| self.inner.size(k)).collect::<Result<_>>()?;
            if self.short_batch {
                sizes.pop();
            }
            Ok(sizes)
        }
    }

    fn pack(i: u32) -> PackId {
        let mut b = [0u8; 32];
        // Spread across shards, several packs per shard.
        b[0] = (i % 40) as u8;
        b[1..5].copy_from_slice(&i.to_le_bytes());
        PackId::from_hex(&hex::encode(b)).unwrap()
    }

    fn entry(i: u32, len: u32) -> PendingPackEntry {
        PendingPackEntry {
            pack_id: pack(i),
            chunks: vec![PendingChunkEntry {
                chunk_id: ChunkId::from_bytes([i as u8; 32]),
                stored_size: len,
                pack_offset: 0,
            }],
        }
    }

    fn len_of(i: u32) -> u32 {
        10 + i % 7
    }

    /// 100 sized entries over 40 shards plus one with no chunks. Packs 3/4
    /// are stored one byte short/long, pack 5 is not stored, and a foreign
    /// pack sits in storage. Returns the entries and the exact-length set.
    fn fixture(storage: &Probe) -> (Vec<PendingPackEntry>, HashSet<PackId>) {
        let mut entries = Vec::new();
        let mut exact = HashSet::new();
        for i in 0..100 {
            let len = len_of(i);
            entries.push(entry(i, len));
            let stored = match i {
                3 => Some(len - 1),
                4 => Some(len + 1),
                5 => None,
                _ => {
                    exact.insert(pack(i));
                    Some(len)
                }
            };
            if let Some(n) = stored {
                storage
                    .inner
                    .put(&pack(i).storage_key(), &vec![0u8; n as usize])
                    .unwrap();
            }
        }
        entries.push(PendingPackEntry {
            pack_id: pack(200),
            chunks: Vec::new(),
        });
        storage
            .inner
            .put(&pack(500).storage_key(), b"foreign")
            .unwrap();
        (entries, exact)
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

    #[test]
    fn every_path_accepts_exactly_the_exact_length_packs() {
        for (name, make) in configs() {
            for width in [1, 6] {
                let storage = make();
                let (entries, exact) = fixture(&storage);
                assert_eq!(
                    verify_journal_packs(&storage, &entries, width),
                    exact,
                    "{name}/{width}"
                );
            }
        }
    }

    /// The batch's answer is final: the absent pack is not re-checked.
    #[test]
    fn batch_answers_every_pack_in_one_request() {
        let storage = Probe::with_batch();
        let (entries, exact) = fixture(&storage);
        assert_eq!(verify_journal_packs(&storage, &entries, 6), exact);
        assert_eq!(Probe::count(&storage.batches), 1);
        assert_eq!(Probe::count(&storage.lists), 0);
        assert_eq!(Probe::count(&storage.sizes), 0);
    }

    #[test]
    fn unsupported_batch_lists_each_touched_shard_once() {
        let storage = Probe {
            sized_listing: true,
            ..Probe::new()
        };
        let (entries, exact) = fixture(&storage);
        assert_eq!(verify_journal_packs(&storage, &entries, 6), exact);
        assert_eq!(Probe::count(&storage.batches), 0);
        assert_eq!(Probe::count(&storage.lists), 40);
        assert_eq!(Probe::count(&storage.sizes), 0);
    }

    #[test]
    fn failed_or_short_batch_falls_back_to_shard_lists() {
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
            let (entries, exact) = fixture(&storage);
            assert_eq!(verify_journal_packs(&storage, &entries, 6), exact, "{name}");
            assert_eq!(Probe::count(&storage.batches), 1, "{name}");
            assert_eq!(Probe::count(&storage.lists), 40, "{name}");
        }
    }

    /// Unlisted packs are missing without a `size()`; listed ones get one.
    #[test]
    fn unsized_listing_sizes_each_listed_pack() {
        let storage = Probe::new();
        let (entries, exact) = fixture(&storage);
        assert_eq!(verify_journal_packs(&storage, &entries, 6), exact);
        assert_eq!(Probe::count(&storage.sizes), 99);
    }

    #[test]
    fn failed_listing_falls_back_to_size_per_pack() {
        let storage = Probe {
            fail_list: true,
            ..Probe::new()
        };
        let (entries, exact) = fixture(&storage);
        assert_eq!(verify_journal_packs(&storage, &entries, 6), exact);
        assert_eq!(Probe::count(&storage.lists), 40);
        assert_eq!(Probe::count(&storage.sizes), 100);
    }

    /// Every shard is still processed when lookups fail: run-all, not
    /// fail-fast.
    #[test]
    fn size_errors_reject_without_aborting() {
        let storage = Probe {
            fail_list: true,
            fail_size: true,
            ..Probe::new()
        };
        let (entries, _) = fixture(&storage);
        assert!(verify_journal_packs(&storage, &entries, 6).is_empty());
        assert_eq!(Probe::count(&storage.sizes), 100);
    }

    #[test]
    fn no_sized_entries_issue_no_requests() {
        let storage = Probe::with_batch();
        let entries = vec![PendingPackEntry {
            pack_id: pack(1),
            chunks: Vec::new(),
        }];
        assert!(verify_journal_packs(&storage, &entries, 6).is_empty());
        assert_eq!(Probe::count(&storage.batches), 0);
        assert_eq!(Probe::count(&storage.lists), 0);
        assert_eq!(Probe::count(&storage.sizes), 0);
    }

    #[test]
    fn shard_fallback_respects_width() {
        let storage = Probe::new();
        let (entries, _) = fixture(&storage);
        verify_journal_packs(&storage, &entries, 3);
        let max = Probe::count(&storage.max_in_flight);
        assert!((1..=3).contains(&max), "max in flight {max}");
        // Wider than 1 fans out: no listing runs on the caller's thread.
        let me = std::thread::current().id();
        {
            let threads = storage.list_threads.lock().unwrap();
            assert_eq!(threads.len(), 40);
            assert!(threads.iter().all(|t| *t != me));
        }

        let storage = Probe::new();
        let (entries, _) = fixture(&storage);
        verify_journal_packs(&storage, &entries, 1);
        assert_eq!(Probe::count(&storage.max_in_flight), 1);
        // Width 1 runs inline on the caller's thread, as before.
        let threads = storage.list_threads.lock().unwrap();
        assert_eq!(threads.len(), 40);
        assert!(threads.iter().all(|t| *t == me));
    }

    #[test]
    fn expected_len_is_last_blob_end() {
        let e = PendingPackEntry {
            pack_id: PackId::from_bytes([0; 32]),
            chunks: vec![
                PendingChunkEntry {
                    chunk_id: ChunkId::from_bytes([1; 32]),
                    stored_size: 10,
                    pack_offset: 13,
                },
                PendingChunkEntry {
                    chunk_id: ChunkId::from_bytes([2; 32]),
                    stored_size: 5,
                    pack_offset: 27,
                },
            ],
        };
        assert_eq!(expected_pack_len(&e), Some(32));
        let empty = PendingPackEntry {
            pack_id: e.pack_id,
            chunks: Vec::new(),
        };
        assert_eq!(expected_pack_len(&empty), None);
    }
}
