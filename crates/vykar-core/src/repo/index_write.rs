//! Write side of the segmented index (formats v4/v5); see `index_store` for
//! the objects and the read side.
//!
//! Every write runs under the repository lock: GET the head, check it is the
//! state the in-memory index was loaded from, produce the next state and its
//! segment in one pass over the loaded table, then PUT the segment, check the
//! lock fence and PUT the head. A chain past the hard limit, a write without
//! a head (repair, upgrade) or an explicit request (compact, repair) writes a
//! base instead: a *fold*. The new state becomes the repository's table.
//!
//! Fold: PUT the new state as a base (none for the empty index), then the
//! head with no segments and the previous live set as `retired`. With a
//! previous head, every `index.d/` object it names neither as live nor as
//! retired, and the retired ones, are deleted first. Without one (repair,
//! upgrade) the unreferenced objects are deleted only after the head PUT:
//! until then they may be the only copy of the index. A reader still on the
//! previous head can finish; one on the head before it gets a 404 and
//! re-reads the head.

use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempPath;
use tracing::{debug, warn};

use super::Repository;
use super::control::CancelToken;
use super::index_rebuild::{self, RebuildCounts, UncoveredPack};
use super::index_store::{self, PackedObject, StoredHead};
use crate::index::LiveIndex;
use crate::index::dedup_cache::{self, MmapFullIndexCache, generation_for_state};
use crate::index::head::{BaseRef, HEAD_LAYOUT_VERSION, IndexHead, SegmentRef};
use crate::index::segment::Diff;
use crate::index::table::{self, ChunkTable};
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// How `check --repair` obtained the chunk index it repairs from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRecovery {
    /// The index as storage holds it: every object of the chain read, or
    /// this machine's cache of the head's state.
    Complete,
    /// `index` held a monolithic index written by an older vykar; the index
    /// is rebuilt from it.
    LegacyBlob,
    /// The index could not be read (`cause`) and was rebuilt from pack
    /// maps, pack trailers and this machine's chunk tables. Snapshots whose
    /// chunks lie in uncovered packs show up in the plan as removals.
    RebuiltFromPacks {
        cause: IndexLoss,
        counts: RebuildCounts,
    },
}

/// Why `check --repair` could not read the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexLoss {
    /// `index` is absent.
    HeadMissing,
    /// `index` cannot be decrypted or decoded.
    HeadUnreadable,
    /// The head is readable, but an object of its chain is missing or
    /// damaged.
    ChainDamaged,
}

/// What `check --repair` loaded.
#[derive(Debug, Clone)]
pub struct RepairIndexLoad {
    pub recovery: IndexRecovery,
    /// Packs a rebuilt index has no entries for, in pack ID order.
    pub uncovered_packs: Vec<UncoveredPack>,
    /// Packs a rebuilt index took from this machine's chunk tables only.
    pub packs_from_local_tables: Vec<PackId>,
}

impl RepairIndexLoad {
    fn read(recovery: IndexRecovery) -> Self {
        Self {
            recovery,
            uncovered_packs: Vec::new(),
            packs_from_local_tables: Vec::new(),
        }
    }
}

/// When a chain is folded into a new base: at `segments` segments, or when
/// the segments add up to `percent` of the base's bytes and at least
/// `min_bytes` (below that a small index would fold on every write).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FoldLimits {
    pub segments: usize,
    pub percent: u64,
    pub min_bytes: u64,
}

impl FoldLimits {
    /// Whether `head`'s chain plus `extra` more segments of `extra_bytes`
    /// reaches these limits. An empty chain never does.
    pub(crate) fn reached(&self, head: &IndexHead, extra: usize, extra_bytes: u64) -> bool {
        let count = head.segments.len() + extra;
        if count == 0 {
            return false;
        }
        let bytes = head.segments.iter().map(|s| s.size).sum::<u64>() + extra_bytes;
        let base = head.base.as_ref().map_or(0, |b| b.size);
        count >= self.segments
            || (bytes >= self.min_bytes
                && bytes.saturating_mul(100) >= base.saturating_mul(self.percent))
    }
}

/// Fold limits: `soft` is checked by compact, `hard` by every writer, so a
/// repository that is never compacted stays bounded too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FoldPolicy {
    pub soft: FoldLimits,
    pub hard: FoldLimits,
}

impl Default for FoldPolicy {
    fn default() -> Self {
        Self {
            soft: FoldLimits {
                segments: 16,
                percent: 5,
                min_bytes: 1 << 20,
            },
            hard: FoldLimits {
                segments: 64,
                percent: 20,
                min_bytes: 4 << 20,
            },
        }
    }
}

impl Repository {
    /// Set the fold limits (tests).
    #[cfg(test)]
    pub(crate) fn set_fold_policy(&mut self, policy: FoldPolicy) {
        self.fold_policy = policy;
    }

    /// The head of the index as last loaded or written by this process.
    /// `None` on a handle opened for the upgrade, and while `check --repair`
    /// rebuilds the index.
    pub fn index_head(&self) -> Option<&IndexHead> {
        self.index_head.as_ref()
    }

    /// Make the next index write a fold (a new base, garbage collected).
    pub(crate) fn request_index_fold(&mut self) {
        self.fold_requested = true;
    }

    /// The per-repo cache dir the chunk tables live in, if there is one.
    pub(super) fn cache_dir(&self) -> Option<PathBuf> {
        crate::cache_paths::repo_cache_dir(&self.config.id, self.cache_dir_override.as_deref())
    }

    /// The local chunk table at `head`'s state: the repository's own, else
    /// the table file of that state.
    pub(super) fn local_table_at(&self, head: &IndexHead) -> Option<Arc<ChunkTable>> {
        let target = (head.state_hash, head.entries);
        if let Some(t) = self.chunk_index.table()
            && t.state() == target
        {
            return Some(Arc::clone(t));
        }
        let dir = self.cache_dir()?;
        ChunkTable::open_file(&table::table_path(&dir, &head.state_hash))
            .filter(|t| t.state() == target)
            .map(Arc::new)
    }

    /// GET the head and check it is the one the in-memory index was loaded
    /// from: a write can only be expressed against that state.
    fn current_head_for_write(&self) -> Result<Option<IndexHead>> {
        let Some(loaded) = self.index_head.as_ref() else {
            // No valid head (`check --repair` rebuilding the index).
            return Ok(None);
        };
        let head = index_store::fetch_head(self.storage.as_ref(), self.crypto.as_ref())?;
        if head != *loaded {
            return Err(stale_index_error());
        }
        Ok(Some(head))
    }

    /// Persist the in-memory index (no write session): diff it against the
    /// table it was loaded over, which must be at the loaded head's state,
    /// and publish the segment, or a base without a head.
    pub(super) fn persist_segmented_index(&mut self) -> Result<()> {
        let head = self.current_head_for_write()?;
        let anchor = index_store::cache_anchor(self.cache_dir().as_deref());
        let start = std::time::Instant::now();
        let empty;
        let prev = match self.chunk_index.table() {
            Some(t) => t.cache(),
            None if head.as_ref().is_some_and(|h| h.entries > 0) => {
                return Err(VykarError::Other(
                    "BUG: the chunk index was not loaded before it was written".into(),
                ));
            }
            None => {
                empty = ChunkTable::empty(&anchor)?;
                empty.cache()
            }
        };
        if let Some(h) = head.as_ref()
            && (prev.state_hash(), u64::from(prev.entry_count())) != (h.state_hash, h.entries)
        {
            return Err(stale_index_error());
        }
        let (diff, next) = table::diff_live_to_temp(prev, &self.chunk_index, &anchor)?;
        debug!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            "index diffed against its predecessor"
        );
        // Without a head the state is written as a base; the diff is unused.
        let diff = head.is_some().then_some(diff);
        self.publish_index_state(head, diff, next)
    }

    /// Publish the index state in `next` (a full cache in a temp file) as
    /// the successor of `prev`: as `diff`'s segment, or as a base when there
    /// is no diff, the hard limit is reached or a fold was requested. Then
    /// install `next` as the repository's table, with no edits.
    ///
    /// An empty diff publishes nothing. On error nothing local changes and
    /// at most an unreferenced object is left on storage, with one
    /// exception: a failed fence check after the head PUT of a fold without
    /// a previous head returns an error although the head was written.
    pub(super) fn publish_index_state(
        &mut self,
        prev: Option<IndexHead>,
        diff: Option<Diff>,
        next: TempPath,
    ) -> Result<()> {
        let fold = std::mem::take(&mut self.fold_requested);
        if let (Some(prev), Some(diff)) = (prev.as_ref(), diff.as_ref())
            && diff.segment.is_empty()
            && !fold
        {
            debug!("index unchanged, nothing to write");
            let at_prev = |t: &Arc<ChunkTable>| t.state() == (prev.state_hash, prev.entries);
            if self.chunk_index.table().is_some_and(at_prev) || !self.chunk_index.is_loaded() {
                drop(next);
                self.chunk_index.discard_edits();
            } else {
                self.install_written_table(next);
            }
            self.index_head = Some(prev.clone());
            self.index_dirty = false;
            return Ok(());
        }

        let start = std::time::Instant::now();
        let packed = diff
            .as_ref()
            .map(|d| index_store::pack_segment(&d.segment, self.crypto.as_ref()))
            .transpose()?;
        // On a plaintext repository the same change from the same state
        // packs to the same bytes, so its name may already be in the head
        // (a state the chain returned to); an object is named only once.
        let as_segment = match (prev.as_ref(), diff.as_ref(), packed) {
            (Some(prev), Some(diff), Some(obj))
                if !fold
                    && !self.fold_policy.hard.reached(prev, 1, obj.size())
                    && !prev
                        .live_objects()
                        .chain(prev.retired.iter().copied())
                        .any(|n| n == obj.name) =>
            {
                Some((prev, diff, obj))
            }
            _ => None,
        };

        let head = match as_segment {
            Some((prev, diff, obj)) => self.put_segment(prev, diff, obj)?,
            None => {
                let cache = MmapFullIndexCache::open_verified(&next).ok_or_else(|| {
                    VykarError::Other("new index state failed verification".into())
                })?;
                self.put_base(prev.as_ref(), &cache)?
            }
        };
        debug!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            segments = head.segments.len(),
            entries = head.entries,
            "index written"
        );

        self.install_written_table(next);
        self.index_generation = generation_for_state(&head.state_hash);
        self.index_head = Some(head);
        self.index_dirty = false;
        Ok(())
    }

    /// Make the state an index write left in `next` the repository's table.
    /// The head is already published, so a failure only logs: the old index
    /// stays in place, and a later write from this handle is refused for its
    /// state until the repository is opened again.
    fn install_written_table(&mut self, next: TempPath) {
        let dir = self.cache_dir();
        match crate::index::table::ChunkTable::install(next, dir.as_deref()) {
            Ok(table) => {
                let table = Arc::new(table);
                // Replace the index before pruning: the table it mapped until
                // now is then unmapped and can be deleted once it is old enough.
                self.chunk_index = LiveIndex::from_table(Arc::clone(&table));
                index_store::prune_old_tables(&table, dir.as_deref());
            }
            Err(e) => warn!("cannot map the index state just written: {e}"),
        }
    }

    /// PUT a segment, then the head that appends it.
    fn put_segment(
        &mut self,
        prev: &IndexHead,
        diff: &Diff,
        obj: PackedObject,
    ) -> Result<IndexHead> {
        let mut head = prev.clone();
        head.segments.push(SegmentRef {
            name: obj.name,
            size: obj.size(),
            state_hash_after: diff.state_hash_after,
            entries_after: diff.entries_after,
        });
        head.state_hash = diff.state_hash_after;
        head.entries = diff.entries_after;
        head.validate()?;
        let packed_head = index_store::pack_head(&head, self.crypto.as_ref())?;

        self.storage.put(&obj.key(), &obj.bytes)?;
        self.check_lock_fence()?;
        self.storage.put("index", &packed_head)?;
        debug!(bytes = obj.size(), "index segment written");
        Ok(head)
    }

    /// Fold: write `cache`'s state as a base (no object for the empty index)
    /// and the head naming it alone, with `prev`'s live objects as retired.
    /// See the module doc for the order of garbage collection.
    pub(super) fn put_base(
        &mut self,
        prev: Option<&IndexHead>,
        cache: &MmapFullIndexCache,
    ) -> Result<IndexHead> {
        let state_hash = cache.state_hash();
        let entries = u64::from(cache.entry_count());
        let obj = (entries > 0)
            .then(|| index_store::pack_base(cache, self.crypto.as_ref()))
            .transpose()?;
        let head = IndexHead {
            layout: crate::index::head::HEAD_LAYOUT_VERSION,
            state_hash,
            entries,
            base: obj.as_ref().map(|obj| BaseRef {
                name: obj.name,
                size: obj.size(),
                state_hash,
                entries,
            }),
            segments: Vec::new(),
            // The same state written again (plaintext) has the same name.
            retired: prev
                .map(|p| {
                    p.live_objects()
                        .filter(|n| obj.as_ref().is_none_or(|o| *n != o.name))
                        .collect()
                })
                .unwrap_or_default(),
        };
        head.validate()?;
        let packed_head = index_store::pack_head(&head, self.crypto.as_ref())?;

        self.check_lock_fence()?;
        let mut deleted = match prev {
            Some(prev) => index_store::collect_garbage(self.storage.as_ref(), prev, true)?,
            None => 0,
        };
        if let Some(obj) = &obj {
            self.storage.put(&obj.key(), &obj.bytes)?;
        }
        self.check_lock_fence()?;
        self.storage.put("index", &packed_head)?;
        if prev.is_none() {
            deleted = self.collect_garbage_after_head(&head)?;
        }
        debug!(
            bytes = obj.as_ref().map_or(0, PackedObject::size),
            deleted, "index folded into a new base"
        );
        Ok(head)
    }

    /// After a fold without a previous head: delete every `index.d/` object
    /// `head` does not name. The head is already published: a failed fence
    /// check is returned (the caller must stop writing), but a listing or delete
    /// failure is only logged and leaves debris for compact.
    fn collect_garbage_after_head(&self, head: &IndexHead) -> Result<usize> {
        self.check_lock_fence()?;
        Ok(
            index_store::collect_garbage(self.storage.as_ref(), head, true).unwrap_or_else(|e| {
                warn!("failed to delete unreferenced index objects: {e}");
                0
            }),
        )
    }

    /// Index upkeep at the end of `compact`, under the lock: fold when the
    /// chain reached the soft limit, otherwise delete unreferenced
    /// `index.d/` objects (debris of interrupted writes). Safe without an
    /// age check: no other writer can be between its segment and head PUTs.
    pub(crate) fn maintain_segmented_index(&mut self) -> Result<()> {
        if self.index_dirty {
            self.save_state()?;
        }
        let Some(head) = self.current_head_for_write()? else {
            return Ok(());
        };
        if !self.fold_policy.soft.reached(&head, 0, 0) {
            let deleted = index_store::collect_garbage(self.storage.as_ref(), &head, false)?;
            if deleted > 0 {
                debug!(deleted, "deleted unreferenced index objects");
            }
            return Ok(());
        }
        let table = match self.local_table_at(&head) {
            Some(table) => table,
            None => {
                let synced = self.sync_index(Some(head.clone()), true)?;
                self.install_synced_index(synced);
                self.chunk_index
                    .table()
                    .cloned()
                    .ok_or_else(|| VykarError::Other("BUG: no table after a sync".into()))?
            }
        };
        // A fold writes the same state: the table stays the repository's.
        let new_head = self.put_base(Some(&head), table.cache())?;
        drop(table);
        self.index_generation = generation_for_state(&new_head.state_hash);
        self.index_head = Some(new_head);
        Ok(())
    }

    /// Load the chunk index for `check --repair`, accepting what other
    /// commands refuse. A monolithic blob at `index` (an older binary
    /// overwrote the head) is rebuilt from the blob. A chain that cannot be
    /// read is rebuilt from this machine's cache of the head's state when
    /// there is one. Otherwise, when the chain is damaged (an object missing
    /// or failing verification) or the head is missing or unreadable, the
    /// index is rebuilt from the packs ([`index_rebuild`]): pack maps, then
    /// trailers, then this machine's verified chunk tables. A storage or
    /// local I/O error, an object of a newer vykar, a head of an unknown
    /// layout or an `index` of an unknown type is refused, never rebuilt
    /// around. When the index is rebuilt, the head is dropped, so the
    /// repair's index write is a fresh base. The rebuild only reads storage
    /// and writes a local temp file; `cancel` stops it.
    pub(crate) fn load_chunk_index_for_repair(
        &mut self,
        cancel: &CancelToken,
    ) -> Result<RepairIndexLoad> {
        let data = self.storage.get("index")?;
        self.load_segmented_index_for_repair(data, cancel)
    }

    /// [`Self::load_chunk_index_for_repair`], from the object fetched at
    /// `index`.
    fn load_segmented_index_for_repair(
        &mut self,
        data: Option<Vec<u8>>,
        cancel: &CancelToken,
    ) -> Result<RepairIndexLoad> {
        let (index, load) = match data {
            None => {
                warn!("the index head is missing; rebuilding the index from the packs");
                self.rebuild_for_repair(IndexLoss::HeadMissing, cancel)?
            }
            Some(data) => match index_store::classify_head(&data, self.crypto.as_ref()) {
                StoredHead::Legacy => {
                    warn!(
                        "`index` holds a monolithic index; rebuilding the segmented index from it"
                    );
                    let blob =
                        super::upgrade::decode_index_blob(Some(data), self.crypto.as_ref())?.1;
                    let anchor = index_store::cache_anchor(self.cache_dir().as_deref());
                    let tmp = dedup_cache::full_cache_from_index_to_temp(&blob, &anchor)?;
                    drop(blob);
                    (
                        scratch_index(tmp)?,
                        RepairIndexLoad::read(IndexRecovery::LegacyBlob),
                    )
                }
                StoredHead::Head(head) => {
                    let err = match self.sync_index(Some(head.clone()), false) {
                        Ok(synced) => {
                            self.install_synced_index(synced);
                            return Ok(RepairIndexLoad::read(IndexRecovery::Complete));
                        }
                        Err(e) => e,
                    };
                    if let Some(table) = self.local_table_at(&head) {
                        warn!(
                            "the index chain cannot be read ({err}); rebuilding the index from \
                             this machine's cache of its latest state"
                        );
                        (
                            LiveIndex::from_table(table),
                            RepairIndexLoad::read(IndexRecovery::Complete),
                        )
                    } else if index_store::is_object_damage(&err) {
                        warn!(
                            "the index chain cannot be read ({err}); rebuilding the index from \
                             the packs"
                        );
                        self.rebuild_for_repair(IndexLoss::ChainDamaged, cancel)?
                    } else {
                        return Err(repair_load_refusal(err));
                    }
                }
                StoredHead::UnknownLayout(layout) => {
                    return Err(VykarError::Other(format!(
                        "unsupported index head layout {layout} (this version reads \
                         {HEAD_LAYOUT_VERSION}); `index` was written by a newer vykar. Upgrade \
                         vykar to repair this repository. Nothing was changed. If no newer \
                         vykar has written to this repository, `index` is damaged; restore it \
                         from a copy of the repository"
                    )));
                }
                StoredHead::UnknownType(tag) => {
                    return Err(VykarError::Other(format!(
                        "`index` holds an object of a type this vykar does not know (type tag \
                         {tag}); it was probably written by a newer vykar. Upgrade vykar to \
                         repair this repository. Nothing was changed. If no newer vykar has \
                         written to this repository, `index` is damaged; restore it from a \
                         copy of the repository"
                    )));
                }
                StoredHead::Unreadable(err) => {
                    warn!(
                        "the index head cannot be read ({err}); rebuilding the index from the packs"
                    );
                    self.rebuild_for_repair(IndexLoss::HeadUnreadable, cancel)?
                }
            },
        };
        self.chunk_index = index;
        self.index_head = None;
        self.index_dirty = true;
        self.rebase_pack_target_from_index();
        Ok(load)
    }

    /// Rebuild the index from the packs ([`index_rebuild`]). The verified
    /// chunk tables of this machine (the repository's own, if loaded, and
    /// every table file in its cache dir) back packs that describe none of
    /// their contents; they are opened only when such a pack exists.
    fn rebuild_for_repair(
        &self,
        cause: IndexLoss,
        cancel: &CancelToken,
    ) -> Result<(LiveIndex, RepairIndexLoad)> {
        let dir = self.cache_dir();
        let own = self.chunk_index.table().cloned();
        let local_tables = || {
            let mut tables: Vec<Arc<ChunkTable>> = own.into_iter().collect();
            if let Some(dir) = dir.as_deref() {
                for (state, path) in table::list_tables(dir) {
                    if tables.iter().any(|t| t.state().0 == state) {
                        continue;
                    }
                    if let Some(t) = ChunkTable::open_file(&path) {
                        tables.push(Arc::new(t));
                    }
                }
            }
            tables
        };
        let anchor = index_store::cache_anchor(self.cache_dir().as_deref());
        let rebuilt = index_rebuild::rebuild_index_from_packs(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            self.content_hash(),
            local_tables,
            &anchor,
            self.request_width(),
            cancel,
        )
        .map_err(rebuild_refusal)?;
        let index = scratch_index(rebuilt.cache)?;
        let load = RepairIndexLoad {
            recovery: IndexRecovery::RebuiltFromPacks {
                cause,
                counts: rebuilt.counts,
            },
            uncovered_packs: rebuilt.uncovered,
            packs_from_local_tables: rebuilt.from_local_tables,
        };
        Ok((index, load))
    }
}

/// The error a write returns when the head moved since the index was loaded.
fn stale_index_error() -> VykarError {
    VykarError::Other(
        "the index changed on storage after it was loaded; retry the operation".into(),
    )
}

/// `tmp` (a full cache in a temp file) as an index over a scratch table.
fn scratch_index(tmp: TempPath) -> Result<LiveIndex> {
    Ok(LiveIndex::from_table(Arc::new(ChunkTable::scratch(tmp)?)))
}

/// The `index.d/` listing for a repair without a head. A failure is named as
/// such and gets none of [`repair_load_refusal`]'s advice about objects.
pub(super) fn list_objects_for_repair(
    storage: &dyn vykar_storage::StorageBackend,
) -> Result<Vec<crate::index::head::ObjectName>> {
    index_store::list_objects(storage).map_err(|e| match e {
        VykarError::Interrupted => e,
        e => VykarError::Other(format!("listing index.d/: {e}")),
    })
}

/// The error `check --repair` returns when reading the chain fails in a way
/// that is not damage to an index object, so the index is not rebuilt.
fn repair_load_refusal(e: VykarError) -> VykarError {
    match e {
        VykarError::Interrupted | VykarError::IndexHeadUnstable => e,
        VykarError::UnsupportedVersion(v) => VykarError::Other(format!(
            "an index object was written by a newer vykar (object version {v}); upgrade \
             vykar to repair this repository. Nothing was changed"
        )),
        e => VykarError::Other(format!(
            "{e}. Repair did not treat this as index damage and changed nothing; if the \
             error persists for one `index.d/` object, removing that object from the \
             repository lets `vykar check --repair` rebuild the index from the packs"
        )),
    }
}

/// The error `check --repair` returns when the rebuild from the packs
/// fails. It reads only storage, so nothing was changed. A pack map or
/// trailer of a newer vykar is already an error naming its key.
fn rebuild_refusal(e: VykarError) -> VykarError {
    match e {
        VykarError::Interrupted => e,
        e => VykarError::Other(format!(
            "rebuilding the index from the packs: {e}. Nothing was changed"
        )),
    }
}
