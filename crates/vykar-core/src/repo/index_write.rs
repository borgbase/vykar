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
use super::index_store::{self, HeadlessOutcome, PackedObject, StoredHead};
use crate::index::LiveIndex;
use crate::index::dedup_cache::{self, MmapFullIndexCache, generation_for_state};
use crate::index::head::{BaseRef, HEAD_LAYOUT_VERSION, IndexHead, SegmentRef};
use crate::index::segment::Diff;
use crate::index::table::{self, ChunkTable};
use vykar_types::error::{Result, VykarError};

/// How `check --repair` obtained the chunk index it repairs from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRecovery {
    /// The index as storage holds it: every object of the chain read, or
    /// this machine's cache of the head's state.
    Complete,
    /// `index` held a monolithic index written by an older vykar; the index
    /// is rebuilt from it.
    LegacyBlob,
    /// `index` is absent; the index is rebuilt from nothing.
    HeadMissing,
    /// `index` cannot be read; see `index_store::rebuild_without_head`.
    /// `from_local_cache`: rebuilt from this machine's chunk table (the
    /// counts are then 0). Else from `index.d/`: `bases` bases decoded,
    /// `applied` segments applied (including ones discarded at a join),
    /// `unplaced` decoded segments that follow no rebuilt state, `damaged`
    /// objects skipped as damage.
    HeadUnreadable {
        from_local_cache: bool,
        bases: usize,
        applied: usize,
        unplaced: usize,
        damaged: usize,
    },
    /// Part of the chain is missing or damaged. The index is the state after
    /// the base and the first `applied` of `total` segments, or the empty
    /// index when `base_lost`. Snapshots whose chunks it lacks show up in
    /// the plan as removals.
    PartialChain {
        base_lost: bool,
        applied: usize,
        total: usize,
    },
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
    /// commands refuse: a monolithic blob at `index` (an older binary
    /// overwrote the head) or a chain with missing or damaged objects, which
    /// is read up to the last state that can be. A chain that cannot be read
    /// is rebuilt from this machine's cache of the head's state when there
    /// is one; otherwise a storage or local I/O error is returned, never
    /// salvaged around. An unreadable head is rebuilt from this machine's
    /// full cache, else from the objects in `index.d/` (see
    /// [`index_store::rebuild_without_head`]); a head of an unknown layout or
    /// an `index` of an unknown type is refused. When the index is rebuilt,
    /// the head is dropped, so the repair's index write is a fresh base.
    /// Returns how the index was read.
    pub(crate) fn load_chunk_index_for_repair(&mut self) -> Result<IndexRecovery> {
        let data = self.storage.get("index")?;
        self.load_segmented_index_for_repair(data)
    }

    /// [`Self::load_chunk_index_for_repair`], from the object fetched at
    /// `index`.
    fn load_segmented_index_for_repair(&mut self, data: Option<Vec<u8>>) -> Result<IndexRecovery> {
        let (index, recovery) = match data {
            None => {
                warn!("the index head is missing; rebuilding from an empty index");
                (LiveIndex::empty(), IndexRecovery::HeadMissing)
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
                    (scratch_index(tmp)?, IndexRecovery::LegacyBlob)
                }
                StoredHead::Head(head) => {
                    let err = match self.sync_index(Some(head.clone()), false) {
                        Ok(synced) => {
                            self.install_synced_index(synced);
                            return Ok(IndexRecovery::Complete);
                        }
                        Err(e) => e,
                    };
                    if let Some(table) = self.local_table_at(&head) {
                        warn!(
                            "the index chain cannot be read ({err}); rebuilding the index from \
                             this machine's cache of its latest state"
                        );
                        (LiveIndex::from_table(table), IndexRecovery::Complete)
                    } else if index_store::is_object_damage(&err) {
                        warn!("the index chain cannot be read ({err}); salvaging it");
                        let anchor = index_store::cache_anchor(self.cache_dir().as_deref());
                        let salvaged = index_store::salvage_chain(
                            self.storage.as_ref(),
                            self.crypto.as_ref(),
                            &head,
                            &anchor,
                        )
                        .map_err(repair_load_refusal)?;
                        let total = head.segments.len();
                        let recovery = if salvaged.base_read && salvaged.applied == total {
                            IndexRecovery::Complete
                        } else {
                            IndexRecovery::PartialChain {
                                base_lost: !salvaged.base_read,
                                applied: salvaged.applied,
                                total,
                            }
                        };
                        warn!(
                            applied = salvaged.applied,
                            total,
                            base_read = salvaged.base_read,
                            "rebuilding the index from the readable part of its chain"
                        );
                        let index = ChunkTable::scratch(salvaged.cache)
                            .map(|t| LiveIndex::from_table(Arc::new(t)))
                            .map_err(|_| {
                                VykarError::Other("salvaged index state failed verification".into())
                            })?;
                        (index, recovery)
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
                StoredHead::Unreadable(err) => self.chunk_index_without_head(&err)?,
            },
        };
        self.chunk_index = index;
        self.index_head = None;
        self.index_dirty = true;
        self.rebase_pack_target_from_index();
        Ok(recovery)
    }

    /// This machine's chunk table at any state: the repository's own, else
    /// the verified table file written last (newest mtime).
    fn local_table_any(&self) -> Option<Arc<ChunkTable>> {
        if let Some(t) = self.chunk_index.table() {
            return Some(Arc::clone(t));
        }
        let dir = self.cache_dir()?;
        let mut files: Vec<(std::time::SystemTime, PathBuf)> = table::list_tables(&dir)
            .into_iter()
            .filter_map(|(_, path)| {
                let mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok()?;
                Some((mtime, path))
            })
            .collect();
        files.sort_by_key(|a| std::cmp::Reverse(a.0));
        files
            .iter()
            .find_map(|(_, path)| ChunkTable::open_file(path))
            .map(Arc::new)
    }

    /// The index for `check --repair` when `index` cannot be read (`err`):
    /// this machine's chunk table, else rebuilt from `index.d/`.
    fn chunk_index_without_head(&self, err: &VykarError) -> Result<(LiveIndex, IndexRecovery)> {
        if let Some(table) = self.local_table_any() {
            warn!(
                "the index head cannot be read ({err}); rebuilding the index from this \
                 machine's cache of it, which may be older than the repository's latest state"
            );
            let recovery = IndexRecovery::HeadUnreadable {
                from_local_cache: true,
                bases: 0,
                applied: 0,
                unplaced: 0,
                damaged: 0,
            };
            return Ok((LiveIndex::from_table(table), recovery));
        }
        warn!(
            "the index head cannot be read ({err}); rebuilding the index from the objects in \
             index.d/"
        );
        let names = list_objects_for_repair(self.storage.as_ref())?;
        let anchor = index_store::cache_anchor(self.cache_dir().as_deref());
        let rebuilt = match index_store::rebuild_without_head(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            names,
            &anchor,
        )
        .map_err(repair_load_refusal)?
        {
            HeadlessOutcome::Refused(msg) => return Err(VykarError::Other(msg)),
            HeadlessOutcome::Rebuilt(r) => r,
        };
        warn!(
            bases = rebuilt.bases,
            applied = rebuilt.applied,
            unplaced = rebuilt.unplaced,
            damaged = rebuilt.damaged,
            "rebuilt the index from the objects in index.d/ without a head"
        );
        let index = ChunkTable::scratch(rebuilt.cache)
            .map(|t| LiveIndex::from_table(Arc::new(t)))
            .map_err(|_| VykarError::Other("rebuilt index state failed verification".into()))?;
        let recovery = IndexRecovery::HeadUnreadable {
            from_local_cache: false,
            bases: rebuilt.bases,
            applied: rebuilt.applied,
            unplaced: rebuilt.unplaced,
            damaged: rebuilt.damaged,
        };
        Ok((index, recovery))
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
/// that is not damage to an index object, so nothing is salvaged.
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
             repository lets `vykar check --repair` rebuild the index from the rest, which \
             may remove snapshots"
        )),
    }
}
