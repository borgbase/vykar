//! Deciding which packs named in a recovered `pending_index` journal are safe
//! to adopt.
//!
//! A journal entry claims "pack P holds these chunks at these offsets". A
//! journal lists a pack only after its atomic upload returned `Ok`, so the
//! object at P's key is a complete upload of P. The length check here guards
//! against a rename lost to a server power failure and anything foreign at
//! the key.
//!
//! Sizes come from `pack_lookup` (batch, else the cost model); every failure
//! rejects the pack, none aborts.

use std::collections::HashSet;

use tracing::warn;

use crate::index::PendingPackEntry;
use vykar_storage::StorageBackend;
use vykar_types::pack_id::PackId;

use super::pack_lookup::{self, OnFailure, Strategy};

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
/// rejected pack is logged here. `persisted_packs` (packs in the loaded index)
/// only steers the request pattern. Up to `width` lookups run at once.
pub(crate) fn verify_journal_packs(
    storage: &dyn StorageBackend,
    entries: &[PendingPackEntry],
    persisted_packs: usize,
    width: usize,
) -> HashSet<PackId> {
    verify_journal_packs_with(storage, entries, persisted_packs, width, None)
}

/// [`verify_journal_packs`] with an optional forced strategy (tests).
fn verify_journal_packs_with(
    storage: &dyn StorageBackend,
    entries: &[PendingPackEntry],
    persisted_packs: usize,
    width: usize,
    force: Option<Strategy>,
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
    let actual = pack_lookup::lookup(
        storage,
        &ids,
        persisted_packs.saturating_add(ids.len()),
        width,
        force,
        OnFailure::Continue,
    );
    let mut accepted = HashSet::with_capacity(sized.len());
    for (entry, expected) in sized {
        // `get`, not `remove`: duplicate entries for a pack see the same answer.
        match actual.get(&entry.pack_id) {
            Some(Ok(Some(size))) if *size == expected => {
                accepted.insert(entry.pack_id);
            }
            Some(Ok(Some(size))) => warn!(
                pack_id = %entry.pack_id,
                expected,
                actual = *size,
                "pending_index: pack length mismatch, skipping"
            ),
            Some(Err(e)) => warn!(
                pack_id = %entry.pack_id,
                error = %e,
                "pending_index: pack size lookup failed, skipping"
            ),
            // Absent, or (unreachable under `Continue`) not looked up.
            Some(Ok(None)) | None => warn!(
                pack_id = %entry.pack_id,
                "pending_index: pack missing from storage, skipping"
            ),
        }
    }
    accepted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::PendingChunkEntry;
    use crate::repo::pack_lookup::tests::{pack, Probe};
    use vykar_types::chunk_id::ChunkId;

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
        let forces = [None, Some(Strategy::PerPack), Some(Strategy::ShardLists)];
        for (name, make) in configs() {
            for force in forces {
                for width in [1, 6] {
                    let storage = make();
                    let (entries, exact) = fixture(&storage);
                    assert_eq!(
                        verify_journal_packs_with(&storage, &entries, 0, width, force),
                        exact,
                        "{name}/{force:?}/{width}"
                    );
                }
            }
        }
    }

    /// Few packs in a large repository: one `size()` each, no listing.
    #[test]
    fn few_packs_in_a_large_repo_are_sized_per_pack() {
        let storage = Probe::new();
        let entries = vec![entry(0, 10), entry(1, 11)];
        for e in &entries {
            let len = expected_pack_len(e).unwrap() as usize;
            storage
                .inner
                .put(&e.pack_id.storage_key(), &vec![0u8; len])
                .unwrap();
        }
        let accepted = verify_journal_packs(&storage, &entries, 10_000, 6);
        assert_eq!(accepted, entries.iter().map(|e| e.pack_id).collect());
        assert_eq!(Probe::count(&storage.lists), 0);
        assert_eq!(Probe::count(&storage.sizes), 2);
    }

    /// `persisted_packs` alone flips the fixture between the two strategies;
    /// the accepted set does not change.
    #[test]
    fn persisted_packs_steer_the_strategy() {
        let storage = Probe::new();
        let (entries, exact) = fixture(&storage);
        let looked_up: HashSet<PackId> = entries
            .iter()
            .filter(|e| expected_pack_len(e).is_some())
            .map(|e| e.pack_id)
            .collect();
        assert_eq!(
            verify_journal_packs(&storage, &entries, 1_000_000, 6),
            exact
        );
        assert_eq!(Probe::count(&storage.lists), 0);
        assert_eq!(Probe::count(&storage.sizes), looked_up.len());

        let storage = Probe::new();
        let (entries, exact) = fixture(&storage);
        assert_eq!(verify_journal_packs(&storage, &entries, 0, 6), exact);
        assert_eq!(Probe::count(&storage.lists), 40);
    }

    /// The batch's answer is final: the absent pack is not re-checked.
    #[test]
    fn batch_answers_every_pack_in_one_request() {
        let storage = Probe::with_batch();
        let (entries, exact) = fixture(&storage);
        assert_eq!(verify_journal_packs(&storage, &entries, 0, 6), exact);
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
        assert_eq!(verify_journal_packs(&storage, &entries, 0, 6), exact);
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
            assert_eq!(
                verify_journal_packs(&storage, &entries, 0, 6),
                exact,
                "{name}"
            );
            assert_eq!(Probe::count(&storage.batches), 1, "{name}");
            assert_eq!(Probe::count(&storage.lists), 40, "{name}");
        }
    }

    /// Unlisted packs are missing without a `size()`; listed ones get one.
    #[test]
    fn unsized_listing_sizes_each_listed_pack() {
        let storage = Probe::new();
        let (entries, exact) = fixture(&storage);
        assert_eq!(verify_journal_packs(&storage, &entries, 0, 6), exact);
        assert_eq!(Probe::count(&storage.sizes), 99);
    }

    #[test]
    fn failed_listing_falls_back_to_size_per_pack() {
        let storage = Probe {
            fail_list: true,
            ..Probe::new()
        };
        let (entries, exact) = fixture(&storage);
        assert_eq!(verify_journal_packs(&storage, &entries, 0, 6), exact);
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
        assert!(verify_journal_packs(&storage, &entries, 0, 6).is_empty());
        assert_eq!(Probe::count(&storage.sizes), 100);
    }

    #[test]
    fn no_sized_entries_issue_no_requests() {
        let storage = Probe::with_batch();
        let entries = vec![PendingPackEntry {
            pack_id: pack(1),
            chunks: Vec::new(),
        }];
        assert!(verify_journal_packs(&storage, &entries, 0, 6).is_empty());
        assert_eq!(Probe::count(&storage.batches), 0);
        assert_eq!(Probe::count(&storage.lists), 0);
        assert_eq!(Probe::count(&storage.sizes), 0);
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
