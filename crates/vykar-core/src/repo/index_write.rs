//! Write side of the segmented index (formats v4/v5); see `index_store` for
//! the objects and the read side.
//!
//! Every write runs under the repository lock: GET the head, bring the
//! local full cache to it, produce the next state and its segment in one
//! sorted pass, then PUT the segment, check the lock fence and PUT the head.
//! A chain past the hard limit, a missing predecessor, or an explicit
//! request (compact, repair) writes a base instead: a *fold*.
//!
//! Fold: delete every `index.d/` object the current head names neither as
//! live nor as retired, and the retired ones; PUT the base; PUT the head
//! with no segments and the previous live set as `retired`. A reader still
//! on the previous head can finish; one on the head before it gets a 404
//! and re-reads the head.

use std::path::PathBuf;

use tempfile::TempPath;
use tracing::{debug, warn};

use super::index_store::{self, PackedObject};
use super::Repository;
use crate::index::dedup_cache::{self, generation_for_state, MmapFullIndexCache};
use crate::index::head::{BaseRef, IndexHead, SegmentRef};
use crate::index::segment::{self, Diff};
use crate::index::ChunkIndex;
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

    /// Persist the in-memory `chunk_index` (no write session): diff it
    /// against the local full cache at the loaded head and publish the
    /// segment, or a base when that cache is missing.
    pub(super) fn persist_segmented_index(&mut self) -> Result<()> {
        let head = self.current_head_for_write()?;
        let path = self.full_cache_path();
        let anchor = index_store::cache_anchor(path.as_deref());
        let start = std::time::Instant::now();
        let prev = head.as_ref().and_then(|h| self.local_full_cache_at(h));
        let (diff, next) = match prev {
            Some(prev) => {
                let (diff, next) = segment::diff_to_temp(&prev, &self.chunk_index, &anchor)?;
                (Some(diff), next)
            }
            None => {
                if head.is_some() {
                    debug!("no local full cache at the loaded head, writing a base");
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
    /// at most an unreferenced object is left on storage.
    pub(super) fn publish_index_state(
        &mut self,
        prev: Option<IndexHead>,
        diff: Option<Diff>,
        next: TempPath,
    ) -> Result<()> {
        let fold = std::mem::take(&mut self.fold_requested);
        if let (Some(prev), Some(diff)) = (prev.as_ref(), diff.as_ref()) {
            if diff.segment.is_empty() && !fold {
                debug!("index unchanged, nothing to write");
                drop(next);
                self.index_head = Some(prev.clone());
                self.index_dirty = false;
                return Ok(());
            }
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

        let (cache, scratch) = index_store::install_cache(next, self.full_cache_path().as_deref())?;
        drop(cache);
        self.index_scratch = scratch;
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

    /// Fold: garbage-collect `index.d/` including `prev`'s retired objects,
    /// PUT `cache` as the base, then the head naming it alone, with `prev`'s
    /// live objects as retired.
    pub(super) fn put_base(
        &mut self,
        prev: Option<&IndexHead>,
        cache: &MmapFullIndexCache,
    ) -> Result<IndexHead> {
        let obj = index_store::pack_base(cache, self.crypto.as_ref())?;
        let state_hash = cache.state_hash();
        let entries = u64::from(cache.entry_count());
        let head = IndexHead {
            layout: crate::index::head::HEAD_LAYOUT_VERSION,
            state_hash,
            entries,
            base: Some(BaseRef {
                name: obj.name,
                size: obj.size(),
                state_hash,
                entries,
            }),
            segments: Vec::new(),
            // The same state written again (plaintext) has the same name.
            retired: prev
                .map(|p| p.live_objects().filter(|n| *n != obj.name).collect())
                .unwrap_or_default(),
        };
        head.validate()?;
        let packed_head = index_store::pack_head(&head, self.crypto.as_ref())?;

        self.check_lock_fence()?;
        let deleted = index_store::collect_garbage(self.storage.as_ref(), prev, true)?;
        self.storage.put(&obj.key(), &obj.bytes)?;
        self.check_lock_fence()?;
        self.storage.put("index", &packed_head)?;
        debug!(bytes = obj.size(), deleted, "index folded into a new base");
        Ok(head)
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
            let deleted = index_store::collect_garbage(self.storage.as_ref(), Some(&head), false)?;
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
    /// is read up to the last state that can be. Either way the head is
    /// dropped, so the repair's index write is a fresh base.
    pub(crate) fn load_chunk_index_for_repair(&mut self) -> Result<()> {
        if !self.is_segmented() {
            return self.load_chunk_index_uncached();
        }
        let data = self.storage.get("index")?;
        let index = match data {
            Some(data) if index_store::is_legacy_index(&data) => {
                warn!("`index` holds a monolithic index; rebuilding the segmented index from it");
                Self::decode_full_index(Some(data), self.crypto.as_ref())?.1
            }
            None => {
                warn!("the index head is missing; rebuilding from an empty index");
                ChunkIndex::new()
            }
            Some(data) => {
                let head = index_store::decode_head(&data, self.crypto.as_ref())?;
                match self.sync_index(Some(head.clone()), false) {
                    Ok(synced) => {
                        self.install_synced_index(synced);
                        return Ok(());
                    }
                    Err(e) => warn!("the index chain cannot be read ({e}); salvaging it"),
                }
                if let Some(cache) = self.local_full_cache_at(&head) {
                    warn!("rebuilding the index from this machine's cache of its latest state");
                    dedup_cache::chunk_index_from_full_cache(&cache)
                } else {
                    let path = self.full_cache_path();
                    let anchor = index_store::cache_anchor(path.as_deref());
                    let (tmp, applied) = index_store::salvage_chain(
                        self.storage.as_ref(),
                        self.crypto.as_ref(),
                        &head,
                        &anchor,
                    )?;
                    warn!(
                        applied,
                        total = head.segments.len(),
                        "rebuilding the index from the readable part of its chain"
                    );
                    let cache = MmapFullIndexCache::open_verified(&tmp).ok_or_else(|| {
                        VykarError::Other("salvaged index state failed verification".into())
                    })?;
                    dedup_cache::chunk_index_from_full_cache(&cache)
                }
            }
        };
        self.chunk_index = index;
        self.index_head = None;
        self.index_scratch = None;
        self.index_dirty = true;
        self.local_caches_stale = false;
        self.rebase_pack_target_from_index();
        Ok(())
    }
}
