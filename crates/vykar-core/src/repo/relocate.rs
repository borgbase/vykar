//! Re-resolving chunks whose pack is gone (04-commit-protocol.md "Reader").
//!
//! Readers take no lock and register no marker, so a compact can repack or
//! delete a pack between a reader's planning and its range read. A 404 on
//! a pack is answered by reading the latest head: a newer head says where
//! the chunk lives now, or that it is gone (its snapshot was deleted and
//! compacted). A head that has not moved, or that still names the missing
//! pack, means the pack is lost: the reader fails with today's error.
//!
//! 04 says "retry once"; a relocation makes up to [`MAX_RELOCATE_ROUNDS`]
//! rounds because compact publishes a head and deletes its sources per batch,
//! so the head one round reads may already be superseded by the next batch.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tracing::info;

use super::Repository;
use super::index_store::{self, CacheLocation};
use crate::index::LiveIndex;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// Where the latest head a relocation read stores a chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Located {
    At {
        pack_id: PackId,
        pack_offset: u64,
        stored_size: u32,
    },
    /// The latest head has no entry for it.
    Gone,
}

/// Rounds of "read the latest head, look up again" one relocation makes
/// before it gives up with the pack-not-found error.
pub(crate) const MAX_RELOCATE_ROUNDS: u32 = 3;

/// The generation and index a relocation loaded.
type Loaded = Option<(u64, Arc<LiveIndex>)>;

/// What the relocations of one repository handle have learned.
#[derive(Default)]
struct Learned {
    /// The latest index a relocation loaded.
    latest: Loaded,
    /// Packs a relocation found lost: a 404 that no newer head explained.
    /// That verdict is final for the pack (compact publishes its head
    /// before it deletes a pack), so later reads of it fail without
    /// another head read; a pack lost with many chunks costs one.
    lost: HashSet<PackId>,
}

/// The relocation state shared by every read of one repository handle
/// (restore's workers, a mount's reads).
#[derive(Default)]
pub(crate) struct RelocationCache {
    learned: Mutex<Learned>,
}

impl RelocationCache {
    /// Forget the loaded index and the lost packs (the handle installed an
    /// index of its own).
    pub(crate) fn clear(&self) {
        *self.learned.lock().unwrap_or_else(|e| e.into_inner()) = Learned::default();
    }
}

/// What a relocation reads storage with; borrowed from the repository so
/// that restore's workers can use it after the index was cleared.
pub(crate) struct Relocator<'a> {
    storage: &'a dyn StorageBackend,
    crypto: &'a dyn CryptoEngine,
    cache_dir: Option<PathBuf>,
    width: usize,
    /// Generation of the head the reader planned its reads from.
    planned: Option<u64>,
    cache: &'a RelocationCache,
}

impl<'a> Relocator<'a> {
    pub(crate) fn new(
        storage: &'a dyn StorageBackend,
        crypto: &'a dyn CryptoEngine,
        cache_dir: Option<PathBuf>,
        width: usize,
        planned: Option<u64>,
        cache: &'a RelocationCache,
    ) -> Self {
        Self {
            storage,
            crypto,
            cache_dir,
            width: width.max(1),
            planned,
            cache,
        }
    }

    /// Locate `ids`, which the reader expected in `missing` (a pack that
    /// answered 404). One entry per ID, in order.
    ///
    /// The cache's mutex is held for the whole call, so concurrent workers
    /// that hit the same missing pack load the head once and the others
    /// answer from what it loaded.
    ///
    /// # Errors
    ///
    /// Today's pack-not-found error (`pack not found: <id>`) when the latest
    /// head is not newer than the one the reader planned from (or the one
    /// already loaded here), or still has an ID in `missing` after
    /// [`MAX_RELOCATE_ROUNDS`], and at once for a pack an earlier call found
    /// lost; storage errors and `Interrupted` as returned.
    pub(crate) fn relocate(&self, missing: &PackId, ids: &[ChunkId]) -> Result<Vec<Located>> {
        let mut learned = self.cache.learned.lock().unwrap_or_else(|e| e.into_inner());
        if learned.lost.contains(missing) {
            return Err(pack_not_found(missing));
        }
        let found = relocate_rounds(
            missing,
            ids,
            self.planned,
            &mut learned.latest,
            || {
                let head = index_store::fetch_head(self.storage, self.crypto)?;
                Ok((head.generation, head))
            },
            |head| {
                let synced = index_store::sync_index(
                    self.storage,
                    self.crypto,
                    head,
                    CacheLocation {
                        dir: self.cache_dir.as_deref(),
                        live: None,
                        use_local: true,
                    },
                    self.width,
                )?;
                Ok((synced.head.generation, synced.index))
            },
        )?;
        found.ok_or_else(|| {
            learned.lost.insert(*missing);
            pack_not_found(missing)
        })
    }
}

/// A relocator's parts owned, for a reader that outlives its borrow of the
/// repository (the item stream). Its cache is its own.
pub(crate) struct RelocationSource {
    storage: Arc<dyn StorageBackend>,
    crypto: Arc<dyn CryptoEngine>,
    cache_dir: Option<PathBuf>,
    width: usize,
    planned: Option<u64>,
    cache: RelocationCache,
}

impl RelocationSource {
    pub(crate) fn relocator(&self) -> Relocator<'_> {
        Relocator::new(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            self.cache_dir.clone(),
            self.width,
            self.planned,
            &self.cache,
        )
    }
}

impl Repository {
    /// The generation a reader of this handle planned from: that of the
    /// loaded head, `None` when there is none.
    fn planned_generation(&self) -> Option<u64> {
        self.index_head.as_ref().map(|_| self.index_generation)
    }

    /// This handle's relocator, sharing the handle's relocation cache.
    pub(crate) fn relocator(&self) -> Relocator<'_> {
        Relocator::new(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            self.cache_dir(),
            self.request_width(),
            self.planned_generation(),
            &self.relocation,
        )
    }

    /// An owned relocator source with a cache of its own.
    pub(crate) fn relocation_source(&self) -> RelocationSource {
        RelocationSource {
            storage: Arc::clone(&self.storage),
            crypto: Arc::clone(&self.crypto),
            cache_dir: self.cache_dir(),
            width: self.request_width(),
            planned: self.planned_generation(),
            cache: RelocationCache::default(),
        }
    }
}

/// Today's error text for a pack that answered 404; shared by every reader.
pub(crate) fn pack_not_found(pack_id: &PackId) -> VykarError {
    VykarError::Other(format!("pack not found: {pack_id}"))
}

/// Where `index` stores each of `ids`, or `None` while it still names
/// `missing` for one of them.
fn answer(index: &LiveIndex, missing: &PackId, ids: &[ChunkId]) -> Option<Vec<Located>> {
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(match index.get(id) {
            Some(e) if e.pack_id == *missing => return None,
            Some(e) => Located::At {
                pack_id: e.pack_id,
                pack_offset: e.pack_offset,
                stored_size: e.stored_size,
            },
            None => Located::Gone,
        });
    }
    Some(out)
}

/// The loop of [`Relocator::relocate`]: `fetch` reads the latest head
/// (its generation and the head), `load` syncs an index to it. The
/// generation is compared before anything is synced. `None` when the pack
/// is lost: no newer head, or `missing` still named after the last round.
fn relocate_rounds<H>(
    missing: &PackId,
    ids: &[ChunkId],
    planned: Option<u64>,
    current: &mut Loaded,
    mut fetch: impl FnMut() -> Result<(u64, H)>,
    mut load: impl FnMut(H) -> Result<(u64, LiveIndex)>,
) -> Result<Option<Vec<Located>>> {
    if let Some((_, index)) = current.as_ref()
        && let Some(found) = answer(index, missing, ids)
    {
        return Ok(Some(found));
    }
    for _ in 0..MAX_RELOCATE_ROUNDS {
        let known = current.as_ref().map(|(g, _)| *g).or(planned);
        let (generation, head) = fetch()?;
        if known.is_some_and(|k| generation <= k) {
            // No newer state explains the 404: the pack is lost.
            return Ok(None);
        }
        let (generation, index) = load(head)?;
        info!(
            "pack {missing} answered 404; reading its chunks again at head {generation} \
             (a concurrent compact moved them)"
        );
        let index = Arc::new(index);
        let found = answer(&index, missing, ids);
        *current = Some((generation, index));
        if found.is_some() {
            return Ok(found);
        }
    }
    Ok(None)
}

/// [`relocate_rounds`] with a lost pack as today's error.
#[cfg(test)]
fn relocate_with<H>(
    missing: &PackId,
    ids: &[ChunkId],
    planned: Option<u64>,
    current: &mut Loaded,
    fetch: impl FnMut() -> Result<(u64, H)>,
    load: impl FnMut(H) -> Result<(u64, LiveIndex)>,
) -> Result<Vec<Located>> {
    relocate_rounds(missing, ids, planned, current, fetch, load)?
        .ok_or_else(|| pack_not_found(missing))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pack(n: u8) -> PackId {
        PackId::from_bytes([n; 32])
    }

    fn chunk(n: u8) -> ChunkId {
        ChunkId::from_bytes([n; 32])
    }

    /// An index holding `id` in `pack` at offset 10.
    fn index_with(id: ChunkId, pack: PackId) -> LiveIndex {
        let mut index = LiveIndex::empty();
        index.load_empty();
        index.add(id, 5, pack, 10);
        index
    }

    fn is_pack_not_found(e: &VykarError) -> bool {
        matches!(e, VykarError::Other(m) if m == &format!("pack not found: {}", pack(1)))
    }

    #[test]
    fn relocate_with_bounds_its_rounds() {
        // Ever newer heads that still name the missing pack: gives up after
        // exactly MAX_RELOCATE_ROUNDS loads.
        let (mut fetches, mut loads, head) = (0, 0, std::cell::Cell::new(5u64));
        let mut current = None;
        let err = relocate_with(
            &pack(1),
            &[chunk(9)],
            Some(5),
            &mut current,
            || {
                fetches += 1;
                head.set(head.get() + 1);
                Ok((head.get(), ()))
            },
            |()| {
                loads += 1;
                Ok((head.get(), index_with(chunk(9), pack(1))))
            },
        )
        .unwrap_err();
        assert!(is_pack_not_found(&err), "{err:?}");
        assert_eq!((fetches, loads), (MAX_RELOCATE_ROUNDS, MAX_RELOCATE_ROUNDS));

        // The planned generation again: one head read, no load.
        let (mut fetches, mut loads) = (0, 0);
        let mut current = None;
        let err = relocate_with(
            &pack(1),
            &[chunk(9)],
            Some(5),
            &mut current,
            || {
                fetches += 1;
                Ok((5, ()))
            },
            |()| {
                loads += 1;
                Ok((5, LiveIndex::empty()))
            },
        )
        .unwrap_err();
        assert!(is_pack_not_found(&err), "{err:?}");
        assert_eq!((fetches, loads), (1, 0));
        assert!(current.is_none());

        // A newer index with the chunk elsewhere: At after one load.
        let mut loads = 0;
        let mut current = None;
        let found = relocate_with(
            &pack(1),
            &[chunk(9)],
            Some(5),
            &mut current,
            || Ok((6, ())),
            |()| {
                loads += 1;
                Ok((6, index_with(chunk(9), pack(2))))
            },
        )
        .unwrap();
        assert_eq!(
            found,
            vec![Located::At {
                pack_id: pack(2),
                pack_offset: 10,
                stored_size: 5
            }]
        );
        assert_eq!(loads, 1);

        // A second relocation is answered from `current` without a call.
        let found = relocate_with(
            &pack(1),
            &[chunk(9)],
            Some(5),
            &mut current,
            || -> Result<(u64, ())> { panic!("no head read expected") },
            |()| -> Result<(u64, LiveIndex)> { panic!("no load expected") },
        )
        .unwrap();
        assert!(matches!(found[0], Located::At { pack_id, .. } if pack_id == pack(2)));

        // A newer index without the chunk: Gone.
        let mut current = None;
        let found = relocate_with(
            &pack(1),
            &[chunk(9), chunk(8)],
            Some(5),
            &mut current,
            || Ok((6, ())),
            |()| Ok((6, index_with(chunk(8), pack(3)))),
        )
        .unwrap();
        assert_eq!(found[0], Located::Gone);
        assert!(matches!(found[1], Located::At { pack_id, .. } if pack_id == pack(3)));
    }
}
