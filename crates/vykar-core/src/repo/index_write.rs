//! Write side of the index (format v6); see `index_store` for the objects
//! and the read side. The write sequence itself (the PUTs, the fence, the
//! head) is the steppable machine of `publish_machine.rs`.
//!
//! Every write runs under the repository lock: read the latest head, check
//! it is the head the in-memory index was loaded from, express the edits as
//! one keyed segment ([`LiveIndex::pending_segment`], a walk over the
//! overlay only), then PUT the segment (and a sweep record, when a sweep is
//! pending), check the lock fence and PUT the head of the next generation at
//! `index.h/<gen>`. The segment is then applied in memory; nothing local is
//! rewritten. A chain past the hard limit, a write without a head (repair,
//! upgrade) or an explicit request (compact, repair) writes a base instead:
//! a *fold*, which also becomes the repository's local table file.
//!
//! Fold: PUT the new state as a base (none for the empty index), then the
//! head with no segments and the previous live set as `retired`. Nothing is
//! deleted before the head PUT. After it, old heads and unreferenced
//! `index.d/` objects are deleted by age
//! ([`index_store::collect_index_garbage`]), so a reader still on a recent
//! head can finish and a concurrent writer's fresh object survives.

use std::path::PathBuf;
use std::sync::Arc;

use tracing::{debug, warn};

use super::Repository;
use super::control::{CancelToken, CommitEvent};
use super::index_rebuild::{self, RebuildCounts, UncoveredPack};
use super::index_store::{self, PackedObject, StoredHead};
use super::machine::{StepCx, drive_blocking};
use super::publish_machine::PublishMachine;
use crate::index::LiveIndex;
use crate::index::head::{HEAD_LAYOUT_VERSION, IndexHead};
use crate::index::head::{SegmentKind, SweepState};
use crate::index::segment_v2::SegmentBody;
use crate::index::sweep_record::SweepRecord;
use crate::index::table::{self, ChunkTable, IndexState};
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// How `check --repair` obtained the chunk index it repairs from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRecovery {
    /// The index as storage holds it: every object of the chain read, or
    /// this machine's cache of the head's state.
    Complete,
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
    /// No head is listed under `index.h/`.
    HeadMissing,
    /// The latest head cannot be decrypted or decoded.
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

/// What the next index write records in the head's `sweep`.
#[derive(Debug, Clone)]
pub(crate) enum PendingSweep {
    /// A sweep ran: write this record (its `generation` filled in at write time)
    /// and set `sweep.generation` to the new head's generation.
    Swept(SweepRecord),
    /// `check --repair`: `sweep.generation` = the new head's generation; the
    /// record carries over.
    Repair,
    /// The upgrade's first head: [`SweepState::default`].
    Initial,
}

impl Repository {
    /// Set what the next index write records in the head's `sweep`.
    pub(crate) fn set_pending_sweep(&mut self, p: PendingSweep) {
        self.pending_sweep = Some(p);
    }

    /// Set the grace of the index garbage collection (tests).
    #[cfg(test)]
    pub(crate) fn set_index_gc_grace(&mut self, grace: std::time::Duration) {
        self.index_gc_grace = grace;
    }

    /// The next head's generation: `prev.generation + 1`; with no readable head
    /// (repair, upgrade), the highest listed generation + 1, or 1, so a new
    /// head never reuses a key.
    pub(super) fn next_head_gen(&self, prev: Option<&IndexHead>) -> Result<u64> {
        match prev {
            Some(p) => Ok(p.generation + 1),
            None => Ok(index_store::list_head_generations(self.storage.as_ref())?
                .last()
                .map_or(1, |g| g + 1)),
        }
    }

    /// The `sweep` of the head at `generation`, and the record object to PUT
    /// before it. Takes `self.pending_sweep`. With nothing pending: `prev`'s
    /// sweep when there is a `prev`, repair semantics when there is not (a
    /// write without a head may have removed anything). Also repair
    /// semantics when the write removes entries (`removes`) and nothing is
    /// pending, so that no removal leaves `sweep.generation` unchanged.
    ///
    /// The pending sweep is taken even when the write then fails (a PUT, the
    /// fence): it is not restored. The removals stay in the in-memory index,
    /// so a later write from this handle records them as a repair (a
    /// `sweep.generation` of its own, the previous record kept), never as an
    /// unchanged sweep. Compact returns the error and does not write again;
    /// its next run marks and sweeps from scratch.
    pub(super) fn next_sweep(
        &mut self,
        prev: Option<&IndexHead>,
        generation: u64,
        removes: bool,
    ) -> Result<(SweepState, Option<PackedObject>)> {
        let prev_record = prev.and_then(|p| p.sweep.record);
        let pending = match self.pending_sweep.take() {
            Some(p) => Some(p),
            None if prev.is_none() => Some(PendingSweep::Repair),
            None if removes => {
                debug!(
                    "index write removes entries with no sweep pending; recording it as a repair"
                );
                Some(PendingSweep::Repair)
            }
            None => None,
        };
        Ok(match pending {
            None => (prev.map(|p| p.sweep).unwrap_or_default(), None),
            Some(PendingSweep::Initial) => (SweepState::default(), None),
            Some(PendingSweep::Repair) => (
                SweepState {
                    generation,
                    record: prev_record,
                },
                None,
            ),
            Some(PendingSweep::Swept(mut record)) => {
                record.generation = generation;
                let obj = index_store::pack_sweep_record(&record, self.crypto.as_ref())?;
                (
                    SweepState {
                        generation,
                        record: Some(crate::index::head::ObjectRef {
                            name: obj.name,
                            size: obj.size(),
                        }),
                    },
                    Some(obj),
                )
            }
        })
    }

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

    /// Whether the in-memory index is loaded at `head`'s state (the same
    /// base and segments).
    pub(super) fn index_at(&self, head: &IndexHead) -> bool {
        self.chunk_index.is_loaded() && *self.chunk_index.state() == IndexState::of_head(head)
    }

    /// Whether this machine's cache dir holds, verified, every object
    /// `head` names live: its base's table (or no base) and each segment.
    /// A sync to `head` then reads nothing from storage.
    fn local_copies_cover(&self, head: &IndexHead) -> bool {
        let Some(dir) = self.cache_dir() else {
            return false;
        };
        head.base.as_ref().is_none_or(|b| {
            ChunkTable::open_file(&table::base_table_path(&dir, &b.name))
                .is_some_and(|t| t.len() as u64 == b.entries)
        }) && head
            .segments
            .iter()
            .all(|s| table::read_cached_segment(&dir, s).is_some())
    }

    /// Read the latest head and check it is the one the in-memory index was
    /// loaded from (generation included): a write can only be expressed
    /// against that state.
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

    /// Persist the in-memory index (no write session): its edits as one
    /// maintenance segment over the loaded head, which must be the latest, or
    /// a base without a head (repair, upgrade).
    pub(super) fn persist_segmented_index(&mut self) -> Result<()> {
        let head = self.current_head_for_write()?;
        let Some(head) = head else {
            return self.publish_index_state(None, None, false);
        };
        if !self.chunk_index.is_loaded() {
            if !head.is_empty() {
                return Err(VykarError::Other(
                    "BUG: the chunk index was not loaded before it was written".into(),
                ));
            }
            // An index never loaded over the empty head is that head's.
            self.chunk_index.load_empty();
        }
        if *self.chunk_index.state() != IndexState::of_head(&head) {
            return Err(stale_index_error());
        }
        let start = std::time::Instant::now();
        let seg = self.chunk_index.pending_segment(SegmentKind::Maintenance)?;
        debug!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            additions = seg.additions.len(),
            removals = seg.removals.len(),
            relocations = seg.relocations.len(),
            "index edits collected"
        );
        self.publish_index_state(Some(head), Some(seg), false)
    }

    /// Publish `seg` as the successor of `prev`: as a segment, or as a base
    /// (a fold) when there is no `prev`, the hard limit is reached, a fold
    /// was requested or the packed segment's name is already named by
    /// `prev` (plaintext repeat).
    ///
    /// `backup`: `seg` is a backup commit's additions over an unedited
    /// index at `prev`; after the head PUT it is applied to the index (a
    /// fold first adds them as edits, the base being written from the
    /// index, and drops them again when the fold fails). Otherwise `seg` is
    /// the index's pending edits, which become its applied state.
    ///
    /// An empty segment publishes nothing (edits that changed nothing, such
    /// as an add of a present chunk, are discarded), or only a head when a
    /// sweep is pending. On error nothing
    /// local changes (a requested fold stays requested) and at most an
    /// unreferenced object is left on storage.
    pub(super) fn publish_index_state(
        &mut self,
        prev: Option<IndexHead>,
        seg: Option<SegmentBody>,
        backup: bool,
    ) -> Result<()> {
        let mut m = PublishMachine::publish(self, prev, seg, backup);
        let events = |_: CommitEvent| {};
        drive_blocking(
            &mut m,
            &mut StepCx {
                repo: self,
                events: &events,
            },
        )
        .map(|_| ())
    }

    /// Fold: write the index's current state as a base (no object for the
    /// empty index) and the head naming it alone, with `prev`'s live objects
    /// as retired. Nothing is deleted before the head PUT; after it, the
    /// age-gated garbage collection runs, best-effort. The base's body
    /// becomes the repository's table (`index_base.<name>`), with no
    /// segments and no edits. `removes`: the new state lacks entries
    /// `prev`'s has (for the sweep bookkeeping).
    pub(super) fn put_base(
        &mut self,
        prev: Option<&IndexHead>,
        removes: bool,
    ) -> Result<IndexHead> {
        let mut m = PublishMachine::base(prev.cloned(), removes);
        let events = |_: CommitEvent| {};
        drive_blocking(
            &mut m,
            &mut StepCx {
                repo: self,
                events: &events,
            },
        )
    }

    /// Index upkeep at the end of `compact`, under the lock: fold when the
    /// chain reached the soft limit, then delete old heads and unreferenced
    /// `index.d/` objects. The deletion is age-gated, so it never deletes a
    /// concurrent writer's fresh segment (H4-segment).
    pub(crate) fn maintain_segmented_index(&mut self) -> Result<()> {
        if self.index_dirty {
            self.save_state()?;
        }
        let Some(head) = self.current_head_for_write()? else {
            return Ok(());
        };
        let latest = if self.fold_policy.soft.reached(&head, 0, 0) {
            if !self.index_at(&head) || self.chunk_index.is_modified() {
                let synced = self.sync_index(Some(head.clone()), true)?;
                self.install_synced_index(synced);
            }
            // A fold writes the same state as a base.
            let new_head = self.put_base(Some(&head), false)?;
            self.index_generation = new_head.generation;
            self.index_head = Some(new_head.clone());
            new_head
        } else {
            head
        };
        let gc = index_store::collect_index_garbage(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            &latest,
            self.index_gc_grace,
        )?;
        if gc.heads_deleted > 0 || gc.objects_deleted > 0 || gc.skipped.is_some() {
            debug!(
                heads_deleted = gc.heads_deleted,
                objects_deleted = gc.objects_deleted,
                skipped = gc.skipped,
                "index garbage collected"
            );
        }
        Ok(())
    }

    /// Load the chunk index for `check --repair`, accepting what other
    /// commands refuse. When the chain is damaged on storage (an object
    /// missing or failing verification), the head's state is loaded from
    /// this machine's copies of its objects (its local base table and
    /// cached segments, each verified) when they cover it; the index is then
    /// dirty and a fold is requested, so the repair's index write is a fresh
    /// base and head naming only what it uploads. Otherwise a damaged chain,
    /// or a missing or unreadable head, makes the index be rebuilt from the
    /// packs ([`index_rebuild`]): pack maps, then trailers, then this
    /// machine's verified chunk tables. A storage or local I/O error, an
    /// object of a newer vykar, a head of an unknown layout or features, or
    /// a head of an unknown type is refused, never repaired around, local
    /// copies or not. When the index is rebuilt, the head is dropped, so the
    /// repair's index write is a fresh base. The rebuild only reads storage
    /// and writes a local temp file; `cancel` stops it.
    pub(crate) fn load_chunk_index_for_repair(
        &mut self,
        cancel: &CancelToken,
    ) -> Result<RepairIndexLoad> {
        let data = index_store::fetch_latest_head_bytes(self.storage.as_ref())?;
        self.load_segmented_index_for_repair(data, cancel)
    }

    /// [`Self::load_chunk_index_for_repair`], from the latest head fetched
    /// (its generation and bytes).
    fn load_segmented_index_for_repair(
        &mut self,
        data: Option<(u64, Vec<u8>)>,
        cancel: &CancelToken,
    ) -> Result<RepairIndexLoad> {
        let (index, load) = match data {
            None => {
                warn!("the index head is missing; rebuilding the index from the packs");
                self.rebuild_for_repair(IndexLoss::HeadMissing, cancel)?
            }
            Some((generation, data)) => {
                match index_store::classify_head(generation, &data, self.crypto.as_ref()) {
                    StoredHead::Head(head) => {
                        let err = match self.sync_index(Some(head.clone()), false) {
                            Ok(synced) => {
                                self.install_synced_index(synced);
                                return Ok(RepairIndexLoad::read(IndexRecovery::Complete));
                            }
                            Err(e) => e,
                        };
                        let damaged = index_store::is_object_damage(&err);
                        let local = (damaged && self.local_copies_cover(&head))
                            .then(|| self.sync_index(Some(head.clone()), true).ok())
                            .flatten();
                        if let Some(synced) = local {
                            warn!(
                                "the index chain is damaged on storage ({err}); using this \
                                 machine's copies of its objects"
                            );
                            // Storage still holds the damaged chain: the
                            // repair's index write is a fold, a fresh base
                            // and a head that name only objects it uploads.
                            self.install_synced_index(synced);
                            self.index_dirty = true;
                            self.request_index_fold();
                            return Ok(RepairIndexLoad::read(IndexRecovery::Complete));
                        } else if damaged {
                            warn!(
                                "the index chain cannot be read ({err}); rebuilding the index from \
                             the packs"
                            );
                            self.rebuild_for_repair(IndexLoss::ChainDamaged, cancel)?
                        } else {
                            return Err(repair_load_refusal(err));
                        }
                    }
                    StoredHead::UnknownLayout(layout) if layout < HEAD_LAYOUT_VERSION => {
                        return Err(VykarError::Other(format!(
                            "unsupported index head layout {layout} (this version reads \
                         {HEAD_LAYOUT_VERSION}); the index head was written by an older \
                         development build of vykar, which this vykar cannot repair. Nothing \
                         was changed. Recreate the repository"
                        )));
                    }
                    StoredHead::UnknownLayout(layout) => {
                        return Err(VykarError::Other(format!(
                            "unsupported index head layout {layout} (this version reads \
                         {HEAD_LAYOUT_VERSION}); the index head was written by a newer vykar. \
                         Upgrade vykar to repair this repository. Nothing was changed. If no \
                         newer vykar has written to this repository, the index head is damaged; \
                         restore it from a copy of the repository"
                        )));
                    }
                    StoredHead::UnknownFeatures(names) => {
                        return Err(VykarError::Other(format!(
                            "the index head uses feature(s) {} that this vykar does not know; it \
                         was written by a newer vykar. Upgrade vykar to repair this \
                         repository. Nothing was changed. If no newer vykar has written to \
                         this repository, the index head is damaged; restore it from a copy \
                         of the repository",
                            names.join(", ")
                        )));
                    }
                    StoredHead::UnknownType(tag) => {
                        return Err(VykarError::Other(format!(
                            "the index head holds an object of a type this vykar does not know \
                         (type tag {tag}); it was probably written by a newer vykar. Upgrade \
                         vykar to repair this repository. Nothing was changed. If no newer \
                         vykar has written to this repository, the index head is damaged; \
                         restore it from a copy of the repository"
                        )));
                    }
                    StoredHead::Unreadable(err) => {
                        warn!(
                            "the index head cannot be read ({err}); rebuilding the index from the packs"
                        );
                        self.rebuild_for_repair(IndexLoss::HeadUnreadable, cancel)?
                    }
                }
            }
        };
        self.chunk_index = index;
        self.index_head = None;
        self.index_dirty = true;
        self.rebase_pack_target_from_index();
        Ok(load)
    }

    /// Rebuild the index from the packs ([`index_rebuild`]). The verified
    /// base tables of this machine (the repository's own, if loaded, and
    /// every `index_base.*` file in its cache dir) back packs that describe
    /// none of their contents; they are opened only when such a pack exists.
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
                for (name, path) in table::list_base_tables(dir) {
                    if tables.iter().any(|t| t.base_name() == Some(name)) {
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
        let index = LiveIndex::from_table(Arc::new(ChunkTable::install(rebuilt.body, None, None)?));
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
