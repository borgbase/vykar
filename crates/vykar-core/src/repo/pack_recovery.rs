//! Deciding which packs named in a recovered `pending_index` journal are safe
//! to adopt.
//!
//! A journal entry claims "pack P holds these chunks at these offsets". A
//! journal lists a pack only after its atomic upload returned `Ok`, so the
//! object at P's key is a complete upload of P. The length check here guards
//! against a rename lost to a server power failure and anything foreign at
//! the key. A pack is the end of its last chunk long (written before pack
//! trailers), or that plus exactly the trailer frame listing the entry's
//! chunks; the trailer's length is known from the chunk count, so no content
//! is read.
//!
//! Sizes come from `pack_lookup` (batch, else the cost model); every failure
//! rejects the pack, none aborts.

use std::collections::HashMap;
#[cfg(test)]
use std::collections::HashSet;

use tracing::warn;

use crate::index::PendingPackEntry;
use crate::repo::pack_trailer::trailer_blob_len;
use vykar_storage::StorageBackend;
use vykar_types::pack_id::PackId;

use super::pack_lookup::{self, OnFailure, Strategy};

/// End of the last blob of a pack: `header | (u32 len | blob)*`, which is
/// the pack's length without a trailer and the start of its trailer frame
/// with one. `None` on overflow or no chunks.
pub(crate) fn expected_pack_len(entry: &PendingPackEntry) -> Option<u64> {
    let mut max = None;
    for c in &entry.chunks {
        let end = c.pack_offset.checked_add(u64::from(c.stored_size))?;
        max = Some(max.map_or(end, |m: u64| m.max(end)));
    }
    max
}

/// The lengths a pack of `entry` may have: without a trailer, and with the
/// trailer frame listing exactly its chunks.
fn accepted_pack_lens(entry: &PendingPackEntry) -> Option<[u64; 2]> {
    let end = expected_pack_len(entry)?;
    let frame = 4 + trailer_blob_len(entry.chunks.len()) as u64;
    Some([end, end.checked_add(frame)?])
}

/// Return the subset of `entries` whose packs are safe to adopt: a pack's
/// stored length must be the end of its last chunk, or that plus exactly one
/// trailer frame for the entry's chunk count. Every rejected pack is logged
/// here. `persisted_packs` (packs in the loaded index) only steers the
/// request pattern. Up to `width` lookups run at once.
#[cfg(test)]
pub(crate) fn verify_journal_packs(
    storage: &dyn StorageBackend,
    entries: &[PendingPackEntry],
    persisted_packs: usize,
    width: usize,
) -> HashSet<PackId> {
    verify_journal_packs_with(storage, entries, persisted_packs, width, None)
        .into_keys()
        .collect()
}

/// [`verify_journal_packs`], with each accepted pack's trailer frame length
/// (0 when its length is the one without a trailer): the `trailer_len` the
/// index records for it.
pub(crate) fn verify_journal_packs_trailers(
    storage: &dyn StorageBackend,
    entries: &[PendingPackEntry],
    persisted_packs: usize,
    width: usize,
) -> HashMap<PackId, u32> {
    verify_journal_packs_with(storage, entries, persisted_packs, width, None)
}

/// [`verify_journal_packs`] with an optional forced strategy (tests).
fn verify_journal_packs_with(
    storage: &dyn StorageBackend,
    entries: &[PendingPackEntry],
    persisted_packs: usize,
    width: usize,
    force: Option<Strategy>,
) -> HashMap<PackId, u32> {
    let mut sized: Vec<(&PendingPackEntry, [u64; 2])> = Vec::with_capacity(entries.len());
    for entry in entries {
        match accepted_pack_lens(entry) {
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
    let mut accepted = HashMap::with_capacity(sized.len());
    for (entry, expected) in sized {
        // `get`, not `remove`: duplicate entries for a pack see the same answer.
        match actual.get(&entry.pack_id) {
            Some(Ok(Some(size))) if expected.contains(size) => {
                let trailer_len = if *size == expected[0] {
                    0
                } else {
                    (expected[1] - expected[0]) as u32
                };
                accepted.insert(entry.pack_id, trailer_len);
            }
            Some(Ok(Some(size))) => warn!(
                pack_id = %entry.pack_id,
                without_trailer = expected[0],
                with_trailer = expected[1],
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
    use crate::repo::pack_lookup::tests::{Probe, pack};
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
                        verify_journal_packs_with(&storage, &entries, 0, width, force)
                            .into_keys()
                            .collect::<HashSet<_>>(),
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

    /// The entry and bytes of a pack sealed by `PackWriter` with three blobs.
    fn sealed_pack(crypto: &dyn vykar_crypto::CryptoEngine) -> (PendingPackEntry, Vec<u8>) {
        use crate::repo::pack::{PackType, PackWriter};
        let mut writer = PackWriter::new(
            PackType::Data,
            1 << 20,
            vykar_types::hash::HashAlgorithm::Blake2b,
        );
        for i in 0..3u8 {
            writer
                .add_blob(ChunkId::from_bytes([i; 32]), vec![i; 20 + usize::from(i)])
                .unwrap();
        }
        let sealed = writer.seal(crypto).unwrap();
        let entry = PendingPackEntry {
            pack_id: sealed.pack_id,
            chunks: sealed
                .entries
                .iter()
                .map(|&(chunk_id, stored_size, pack_offset)| PendingChunkEntry {
                    chunk_id,
                    stored_size,
                    pack_offset,
                })
                .collect(),
        };
        (entry, sealed.data.as_bytes().unwrap().to_vec())
    }

    /// A pack ending in the trailer frame for its chunk count is adopted;
    /// one byte more or less is not.
    #[test]
    fn trailer_pack_is_adopted_by_exact_length() {
        let aes = vykar_crypto::aes_gcm::Aes256GcmEngine::new(
            &[0x11; 32],
            crate::testutil::chunk_hasher_for([0x22; 32]),
        );
        let (entry, data) = sealed_pack(&aes);
        let full = data.len();
        for (len, accepted) in [(full, true), (full - 1, false), (full + 1, false)] {
            let storage = Probe::new();
            let mut bytes = data.clone();
            bytes.resize(len, 0);
            storage
                .inner
                .put(&entry.pack_id.storage_key(), &bytes)
                .unwrap();
            let entries = std::slice::from_ref(&entry);
            let got = verify_journal_packs(&storage, entries, 0, 1);
            assert_eq!(got.contains(&entry.pack_id), accepted, "{len}");
        }
        // The same entry without its trailer (a pack from before trailers).
        let storage = Probe::new();
        let end = expected_pack_len(&entry).unwrap() as usize;
        storage
            .inner
            .put(&entry.pack_id.storage_key(), &data[..end])
            .unwrap();
        let got = verify_journal_packs(&storage, std::slice::from_ref(&entry), 0, 1);
        assert!(got.contains(&entry.pack_id), "no trailer");
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
