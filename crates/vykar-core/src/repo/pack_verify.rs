//! Commit-time check that every pack a session's index delta references is
//! still present in storage.
//!
//! This is the backstop against a concurrent `compact` deleting a session's
//! packs after its marker was reaped: it runs under the commit lock and checks
//! **every** pack. Only the request pattern is chosen here — never which packs
//! are checked. Each strategy issues independent read-only requests, so they
//! may run concurrently without changing what is verified.
//!
//! Sizes come from `pack_lookup`; this module decides what an answer means.

use std::collections::HashSet;

use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::pack_lookup::{self, OnFailure, PackSizes, Strategy};

fn missing(pack_id: &PackId) -> VykarError {
    VykarError::Other(format!(
        "commit failed: pack {pack_id} missing from storage (stale session or concurrent deletion)"
    ))
}

/// Verify that every pack in `pack_ids` exists in `storage`, up to `width`
/// requests at once. A pack the lookup reports absent, or did not reach,
/// is the "pack ... missing" error; a failed lookup's error propagates. A
/// failed batch or listing is not an error by itself: a failed batch falls
/// back to the cost model, a failed listing to per-pack `size()`.
pub(crate) fn verify_packs_present(
    storage: &dyn StorageBackend,
    pack_ids: &HashSet<PackId>,
    persisted_packs: usize,
    width: usize,
) -> Result<()> {
    verify_with(storage, pack_ids, persisted_packs, width, None)
}

/// [`verify_packs_present`] with an optional forced strategy (tests).
fn verify_with(
    storage: &dyn StorageBackend,
    pack_ids: &HashSet<PackId>,
    persisted_packs: usize,
    width: usize,
    force: Option<Strategy>,
) -> Result<()> {
    if pack_ids.is_empty() {
        return Ok(());
    }
    let ids: Vec<PackId> = pack_ids.iter().copied().collect();
    let sizes = pack_lookup::lookup(
        storage,
        &ids,
        persisted_packs.saturating_add(ids.len()),
        width,
        force,
        OnFailure::Stop,
    );
    resolve(&ids, sizes)
}

/// The verdict for `ids`. When several packs fail, an absent pack wins over a
/// lookup error, which wins over a pack the lookup did not reach.
fn resolve(ids: &[PackId], mut sizes: PackSizes) -> Result<()> {
    let (mut absent, mut failed, mut unreached) = (None, None, None);
    for id in ids {
        match sizes.get(id) {
            Some(Ok(Some(_))) => {}
            Some(Ok(None)) => {
                absent.get_or_insert(id);
            }
            Some(Err(_)) => {
                failed.get_or_insert(id);
            }
            None => {
                unreached.get_or_insert(id);
            }
        }
    }
    if let Some(id) = absent {
        return Err(missing(id));
    }
    if let Some(id) = failed {
        if let Some(Err(e)) = sizes.remove(id) {
            return Err(e);
        }
    }
    match unreached {
        Some(id) => Err(missing(id)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::pack_lookup::tests::{pack, Probe};

    fn stored(storage: &Probe, n: u32) -> HashSet<PackId> {
        (0..n)
            .map(|i| {
                let id = pack(i);
                storage.inner.put(&id.storage_key(), b"p").unwrap();
                id
            })
            .collect()
    }

    const ALL: [Strategy; 2] = [Strategy::PerPack, Strategy::ShardLists];

    #[test]
    fn all_present_passes_every_strategy() {
        let storage = Probe::new();
        let ids = stored(&storage, 100);
        // An unrelated pack in the repository must not matter.
        storage.inner.put(&pack(500).storage_key(), b"x").unwrap();
        for s in ALL {
            for width in [1, 6] {
                verify_with(&storage, &ids, 0, width, Some(s)).unwrap();
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
                let err = verify_with(&storage, &ids, 0, width, Some(s))
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
    fn failed_listing_falls_back_to_size() {
        let mut storage = Probe::new();
        storage.fail_list = true;
        let ids = stored(&storage, 100);
        verify_with(&storage, &ids, 0, 6, Some(Strategy::ShardLists)).unwrap();
        assert_eq!(Probe::count(&storage.sizes), 100);

        storage.inner.delete(&pack(3).storage_key()).unwrap();
        let err = verify_with(&storage, &ids, 0, 6, Some(Strategy::ShardLists))
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing from storage"), "{err}");
    }

    #[test]
    fn size_errors_propagate() {
        let mut storage = Probe::new();
        storage.fail_list = true;
        storage.fail_size = true;
        let ids = stored(&storage, 50);
        for s in ALL {
            let err = verify_with(&storage, &ids, 0, 6, Some(s))
                .unwrap_err()
                .to_string();
            assert!(err.contains("injected size failure"), "{s:?}: {err}");
        }
    }

    /// Under the commit lock a failing backend must not be asked about every
    /// remaining pack.
    #[test]
    fn stops_at_first_failure() {
        let mut storage = Probe::new();
        storage.fail_size = true;
        let ids = stored(&storage, 100);
        verify_with(&storage, &ids, 0, 1, Some(Strategy::PerPack)).unwrap_err();
        assert_eq!(Probe::count(&storage.sizes), 1);
    }

    #[test]
    fn missing_wins_over_error() {
        let (gone, failed, ok) = (pack(1), pack(2), pack(3));
        let ids = [failed, gone, ok];
        let sizes: PackSizes = [
            (failed, Err(VykarError::Other("injected".into()))),
            (gone, Ok(None)),
            (ok, Ok(Some(1))),
        ]
        .into_iter()
        .collect();
        let err = resolve(&ids, sizes).unwrap_err().to_string();
        assert!(
            err.contains(&format!("pack {gone} missing from storage")),
            "{err}"
        );

        // A pack with no answer is never present.
        let unreached = pack(4);
        let sizes: PackSizes = [(ok, Ok(Some(1)))].into_iter().collect();
        let err = resolve(&[ok, unreached], sizes).unwrap_err().to_string();
        assert!(
            err.contains(&format!("pack {unreached} missing from storage")),
            "{err}"
        );

        // An error wins over a pack not reached.
        let sizes: PackSizes = [(failed, Err(VykarError::Other("injected".into())))]
            .into_iter()
            .collect();
        let err = resolve(&[unreached, failed], sizes)
            .unwrap_err()
            .to_string();
        assert!(err.contains("injected"), "{err}");
    }

    #[test]
    fn shard_lists_issue_one_request_per_touched_shard() {
        let storage = Probe {
            sized_listing: true,
            ..Probe::new()
        };
        let ids = stored(&storage, 100);
        verify_with(&storage, &ids, 0, 6, Some(Strategy::ShardLists)).unwrap();
        assert_eq!(Probe::count(&storage.lists), 40);
        assert_eq!(Probe::count(&storage.sizes), 0);
        assert_eq!(Probe::count(&storage.exists), 0);
    }

    #[test]
    fn batch_checks_every_pack_in_one_request() {
        let storage = Probe::with_batch();
        let ids = stored(&storage, 100);
        verify_packs_present(&storage, &ids, 0, 6).unwrap();
        assert_eq!(Probe::count(&storage.batches), 1);
        assert_eq!(Probe::count(&storage.lists), 0);
        assert_eq!(Probe::count(&storage.sizes), 0);

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
        assert_eq!(Probe::count(&storage.lists), 0);
        assert_eq!(Probe::count(&storage.sizes), 0);
    }

    /// A failed batch never counts as "present": verification falls back to
    /// the per-shard/per-pack requests, which still catch a missing pack.
    #[test]
    fn failed_batch_falls_back_to_full_verification() {
        let mut storage = Probe::with_batch();
        storage.fail_batch = true;
        let ids = stored(&storage, 100);
        verify_packs_present(&storage, &ids, 0, 6).unwrap();
        assert_eq!(Probe::count(&storage.batches), 1);
        assert!(Probe::count(&storage.lists) + Probe::count(&storage.sizes) > 0);

        storage.inner.delete(&pack(3).storage_key()).unwrap();
        let err = verify_packs_present(&storage, &ids, 0, 6)
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing from storage"), "{err}");
    }

    /// Without a batch API the cost model's requests run, as before.
    #[test]
    fn unsupported_batch_uses_the_cost_model() {
        let storage = Probe {
            sized_listing: true,
            ..Probe::new()
        };
        let ids = stored(&storage, 100);
        verify_packs_present(&storage, &ids, 0, 6).unwrap();
        assert_eq!(Probe::count(&storage.batches), 0);
        assert_eq!(Probe::count(&storage.lists), 40);
    }
}
