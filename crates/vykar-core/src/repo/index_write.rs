//! Write side of the segmented index (formats v4/v5); see `index_store` for
//! the objects and the read side.
//!
//! Every write runs under the repository lock: GET the head, bring the
//! local full cache to it, produce the next state and its segment in one
//! sorted pass, then PUT the segment, check the lock fence and PUT the head.
//! A chain past the hard limit, no local cache in the head's chain, or an
//! explicit request (compact, repair) writes a base instead: a *fold*.
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

use tempfile::TempPath;
use tracing::{debug, warn};

use super::Repository;
use super::index_store::{
    self, CacheLocation, HeadlessOutcome, PackedObject, StoredHead, SyncSource,
};
use crate::commands::check::IndexRecovery;
use crate::index::ChunkIndex;
use crate::index::dedup_cache::{self, MmapFullIndexCache, generation_for_state};
use crate::index::head::{BaseRef, HEAD_LAYOUT_VERSION, IndexHead, SegmentRef};
use crate::index::segment::{self, Diff};
use vykar_types::error::{Result, VykarError};

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

    /// The head of a segmented index as last loaded or written by this
    /// process. `None` for the monolithic layout, and for a segmented one
    /// whose index `check --repair` is rebuilding.
    pub fn index_head(&self) -> Option<&IndexHead> {
        self.index_head.as_ref()
    }

    /// Make the next index write a fold (a new base, garbage collected).
    pub(crate) fn request_index_fold(&mut self) {
        self.fold_requested = true;
    }

    /// The persistent full cache path, if there is a cache directory.
    pub(super) fn full_cache_path(&self) -> Option<PathBuf> {
        dedup_cache::full_index_cache_path(&self.config.id, self.cache_dir_override.as_deref())
    }

    /// The local full index cache at `head`'s state: the temp-file cache of
    /// this run, else the persistent one.
    pub(super) fn local_full_cache_at(&self, head: &IndexHead) -> Option<MmapFullIndexCache> {
        let target = (head.state_hash, head.entries);
        let path = self.full_cache_path();
        self.index_scratch
            .as_deref()
            .into_iter()
            .chain(path.as_deref())
            .filter_map(MmapFullIndexCache::open_verified)
            .find(|c| (c.state_hash(), u64::from(c.entry_count())) == target)
    }

    /// The in-memory index from the local full cache at the current head
    /// (segmented), or at `index_generation` (monolithic).
    pub(super) fn chunk_index_from_local_cache(&self) -> Result<ChunkIndex> {
        if self.is_segmented() {
            return self
                .index_head
                .as_ref()
                .and_then(|head| self.local_full_cache_at(head))
                .map(|cache| dedup_cache::chunk_index_from_full_cache(&cache))
                .ok_or_else(|| VykarError::Other("full index cache not found or stale".into()));
        }
        dedup_cache::load_chunk_index_from_full_cache(
            &self.config.id,
            self.index_generation,
            self.cache_dir_override.as_deref(),
        )
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
            return Err(VykarError::Other(
                "the index changed on storage after it was loaded; retry the operation".into(),
            ));
        }
        Ok(Some(head))
    }

    /// The local full cache at `head` that a write diffs against, caught up
    /// through the chain when it holds an earlier state (a lockless reader in
    /// another process may have persisted the state it read over ours).
    /// `None` when no local cache is in the chain or the catch-up fails; the
    /// write then publishes a base.
    fn predecessor_cache(&mut self, head: &IndexHead) -> Option<MmapFullIndexCache> {
        let path = self.full_cache_path();
        let mut location = CacheLocation {
            path: path.as_deref(),
            scratch: self.index_scratch.take(),
            use_local: true,
        };
        let result = index_store::catch_up_local_cache(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            head,
            &mut location,
            self.request_width(),
        );
        self.index_scratch = location.scratch;
        match result {
            Ok(Some((cache, source))) => {
                if source != SyncSource::UpToDate {
                    debug!(?source, "local full cache caught up to the loaded head");
                }
                Some(cache)
            }
            Ok(None) => None,
            Err(VykarError::IndexObjectMissing { kind, key }) => {
                warn!(
                    "index {kind} {key} is missing from storage; writing a base, which does not need it"
                );
                None
            }
            Err(e) => {
                warn!(
                    "cannot catch the local full cache up to the loaded head ({e}); writing a base"
                );
                None
            }
        }
    }

    /// Persist the in-memory `chunk_index` (no write session): diff it
    /// against the local full cache at the loaded head (caught up through
    /// the chain when it holds an earlier state) and publish the segment, or
    /// a base when there is none.
    pub(super) fn persist_segmented_index(&mut self) -> Result<()> {
        let head = self.current_head_for_write()?;
        let path = self.full_cache_path();
        let anchor = index_store::cache_anchor(path.as_deref());
        let start = std::time::Instant::now();
        let prev = match head.as_ref() {
            Some(h) => self.predecessor_cache(h),
            None => None,
        };
        let (diff, next) = match prev {
            Some(prev) => {
                let (diff, next) = segment::diff_to_temp(&prev, &self.chunk_index, &anchor)?;
                (Some(diff), next)
            }
            None => {
                if head.is_some() {
                    debug!("no local full cache in the loaded head's chain, writing a base");
                }
                let next = dedup_cache::full_cache_from_index_to_temp(&self.chunk_index, &anchor)?;
                (None, next)
            }
        };
        debug!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            "index diffed against its predecessor"
        );
        self.publish_index_state(head, diff, next)
    }

    /// Publish the index state in `next` (a full cache in a temp file) as
    /// the successor of `prev`: as `diff`'s segment, or as a base when there
    /// is no diff, the hard limit is reached or a fold was requested. Then
    /// install `next` as the local full cache.
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
            drop(next);
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

        self.index_scratch = index_store::install_cache(next, self.full_cache_path().as_deref());
        self.index_generation = generation_for_state(&head.state_hash);
        self.index_head = Some(head);
        self.index_dirty = false;
        Ok(())
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
        if !self.is_segmented() {
            return Ok(());
        }
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
        let cache = match self.local_full_cache_at(&head) {
            Some(cache) => cache,
            None => {
                let synced = self.sync_index(Some(head.clone()), true)?;
                self.index_scratch = synced.scratch;
                synced.cache
            }
        };
        let new_head = self.put_base(Some(&head), &cache)?;
        drop(cache);
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
    /// an `index` of an unknown type is refused. A monolithic `config` with a
    /// head at `index` (an interrupted upgrade) is repaired in the segmented
    /// layout, and `vykar upgrade` completes it afterwards. Either way the
    /// head is dropped, so the repair's index write is a fresh base. Returns
    /// how the index was read.
    pub(crate) fn load_chunk_index_for_repair(&mut self) -> Result<IndexRecovery> {
        let data = self.storage.get("index")?;
        let adopted =
            !self.is_segmented() && data.as_deref().is_some_and(super::upgrade::is_index_head);
        if !adopted && !self.is_segmented() {
            // As `load_chunk_index_uncached`, on the bytes fetched above.
            self.chunk_index = Self::decode_full_index(data, self.crypto.as_ref())?.1;
            self.local_caches_stale = false;
            self.rebase_pack_target_from_index();
            return Ok(IndexRecovery::Complete);
        }
        let monolithic = self.format;
        if adopted {
            self.adopt_interrupted_upgrade_layout();
        }
        let result = self.load_segmented_index_for_repair(data, adopted);
        if adopted {
            match &result {
                Ok(_) => warn!(
                    "the index is read in the segmented layout the interrupted upgrade \
                     wrote; to complete the upgrade, run `vykar upgrade` after the repair"
                ),
                Err(_) => self.format = monolithic,
            }
        }
        result
    }

    /// [`Self::load_chunk_index_for_repair`] on the segmented layout, from
    /// the object fetched at `index`.
    fn load_segmented_index_for_repair(
        &mut self,
        data: Option<Vec<u8>>,
        adopted: bool,
    ) -> Result<IndexRecovery> {
        let (index, recovery) = match data {
            None => {
                warn!("the index head is missing; rebuilding from an empty index");
                (ChunkIndex::new(), IndexRecovery::HeadMissing)
            }
            Some(data) => match index_store::classify_head(&data, self.crypto.as_ref()) {
                StoredHead::Legacy => {
                    warn!(
                        "`index` holds a monolithic index; rebuilding the segmented index from it"
                    );
                    (
                        Self::decode_full_index(Some(data), self.crypto.as_ref())?.1,
                        IndexRecovery::LegacyBlob,
                    )
                }
                StoredHead::Head(head) => {
                    let err = match self.sync_index(Some(head.clone()), false) {
                        Ok(synced) => {
                            self.install_synced_index(synced);
                            return Ok(IndexRecovery::Complete);
                        }
                        Err(e) => e,
                    };
                    if let Some(cache) = self.local_full_cache_at(&head) {
                        warn!(
                            "the index chain cannot be read ({err}); rebuilding the index from \
                             this machine's cache of its latest state"
                        );
                        (
                            dedup_cache::chunk_index_from_full_cache(&cache),
                            IndexRecovery::Complete,
                        )
                    } else if index_store::is_object_damage(&err) {
                        warn!("the index chain cannot be read ({err}); salvaging it");
                        let path = self.full_cache_path();
                        let anchor = index_store::cache_anchor(path.as_deref());
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
                        let cache = MmapFullIndexCache::open_verified(&salvaged.cache).ok_or_else(
                            || VykarError::Other("salvaged index state failed verification".into()),
                        )?;
                        (dedup_cache::chunk_index_from_full_cache(&cache), recovery)
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
                StoredHead::Unreadable(err) => self.chunk_index_without_head(&err, adopted)?,
            },
        };
        self.chunk_index = index;
        self.index_head = None;
        self.index_scratch = None;
        self.index_dirty = true;
        self.local_caches_stale = false;
        self.rebase_pack_target_from_index();
        Ok(recovery)
    }

    /// Switch this run (not `config`) to the segmented successor format.
    fn adopt_interrupted_upgrade_layout(&mut self) {
        let Some(successor) = self.format.segmented_successor() else {
            return;
        };
        warn!(
            "`config` names format v{} but `index` holds a segmented index head: an upgrade \
             was interrupted",
            self.config.version
        );
        self.format = successor;
    }

    /// This machine's full index cache at any state: this run's temp-file
    /// cache, else the persistent one.
    fn local_full_cache_any(&self) -> Option<MmapFullIndexCache> {
        let path = self.full_cache_path();
        self.index_scratch
            .as_deref()
            .into_iter()
            .chain(path.as_deref())
            .find_map(MmapFullIndexCache::open_verified)
    }

    /// The index for `check --repair` when `index` cannot be read (`err`):
    /// this machine's full cache, else rebuilt from `index.d/`.
    fn chunk_index_without_head(
        &self,
        err: &VykarError,
        adopted: bool,
    ) -> Result<(ChunkIndex, IndexRecovery)> {
        let listed = if adopted {
            let names = list_objects_for_repair(self.storage.as_ref())?;
            if names.is_empty() {
                return Err(VykarError::Other(format!(
                    "`config` names format v{}, and `index` starts like the segmented index \
                     head of an interrupted upgrade, but it cannot be read ({err}) and \
                     index.d/ holds no index objects: `index` is more likely a damaged \
                     monolithic index. Nothing was changed; restore `index` from a copy of \
                     the repository",
                    self.config.version
                )));
            }
            Some(names)
        } else {
            None
        };
        if let Some(cache) = self.local_full_cache_any() {
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
            return Ok((dedup_cache::chunk_index_from_full_cache(&cache), recovery));
        }
        warn!(
            "the index head cannot be read ({err}); rebuilding the index from the objects in \
             index.d/"
        );
        let names = match listed {
            Some(names) => names,
            None => list_objects_for_repair(self.storage.as_ref())?,
        };
        let path = self.full_cache_path();
        let anchor = index_store::cache_anchor(path.as_deref());
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
        let cache = MmapFullIndexCache::open_verified(&rebuilt.cache)
            .ok_or_else(|| VykarError::Other("rebuilt index state failed verification".into()))?;
        let recovery = IndexRecovery::HeadUnreadable {
            from_local_cache: false,
            bases: rebuilt.bases,
            applied: rebuilt.applied,
            unplaced: rebuilt.unplaced,
            damaged: rebuilt.damaged,
        };
        Ok((dedup_cache::chunk_index_from_full_cache(&cache), recovery))
    }
}

/// The `index.d/` listing for a repair without a head. A failure is named as
/// such and gets none of [`repair_load_refusal`]'s advice about objects.
fn list_objects_for_repair(
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
