//! Write side of the index (format v6); see `index_store` for the objects
//! and the read side. The write sequence itself (the PUTs, the fence, the
//! head) is the steppable machine of `publish_machine.rs`.
//!
//! Every write runs under the repository lock: read the latest head, check
//! it is the head the in-memory index was loaded from, express the edits as
//! one keyed segment ([`LiveIndex::pending_segment`], a walk over the
//! overlay only), then PUT the segment (and a sweep record, when a sweep is
//! pending), check the lock fence and create the head of the next generation
//! at `index.h/<gen>` (put-if-absent where the backend offers it and its
//! check passed, else a plain PUT under the lock: 04 step 8). A generation
//! another writer took ends a maintenance write with "index changed …
//! retry"; a backup commit rebases and tries again. The segment is then
//! applied in memory; nothing local is
//! rewritten. A chain past the hard limit, a write without a head (repair,
//! upgrade) or an explicit request (compact, repair) writes a base instead:
//! a *fold*, which also becomes the repository's local table file.
//!
//! Fold: PUT the new state as a base (none for the empty index), then the
//! head naming it and the previous objects it folded as `retired`. A fold
//! writes into the base only the longest prefix of the chain that is
//! *foldable* (04 step 7): maintenance segments, and backup segments at
//! least T_pack old on the store's clock ([`super::ages::foldable`]). The
//! segments after that prefix stay listed after the new base, followed by
//! the write's own segment when it is not foldable (a backup's own segment
//! never is at the production grace); the fold limits count only the
//! prefix, and a fold with nothing to fold writes a plain segment. A write
//! without a head and `check --repair`'s fold fold every segment (a *full*
//! fold). Nothing is deleted before the head PUT.
//! After it, old heads and unreferenced `index.d/` objects are deleted by
//! age ([`index_store::collect_index_garbage`]), so a reader still on a
//! recent head can finish and a concurrent writer's fresh object survives;
//! only right after this handle created the latest head.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::{debug, warn};

use super::Repo;
use super::control::{CancelToken, CommitEvent};
use super::index_rebuild::{self, RebuildCounts, UncoveredPack};
use super::index_store::{self, PackedObject, StoredHead};
use super::lock::MarkerStamp;
use super::machine::{StepCx, drive_blocking};
use super::publish_machine::{PublishMachine, Published};
use super::writer::Fence;
use crate::index::LiveIndex;
use crate::index::head::{BaseRef, HEAD_LAYOUT_VERSION, IndexHead, SegmentRef};
use crate::index::head::{SegmentKind, SweepState};
use crate::index::segment_v2::SegmentBody;
use crate::index::sweep_record::SweepRecord;
use crate::index::table::{self, ChunkTable, IndexState};
use vykar_types::chunk_id::ChunkId;
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

/// What fold the next index write was asked for (ordered: the stronger of
/// two requests wins).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FoldRequest {
    #[default]
    None,
    /// Fold the foldable prefix (04 step 7): compact's soft-limit fold, and
    /// the hard limit's stand-in in the simulator and tests.
    Eligible,
    /// Fold every segment, young ones too: `check --repair`, which runs only
    /// without live sessions and must name only objects it uploads, and the
    /// upgrade (no previous head).
    Full,
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
    /// Whether a chain of `segments` over `base`, plus one more segment of
    /// `extra` bytes when given, reaches these limits. An empty chain never
    /// does. Callers pass the foldable prefix (04 step 7): young backup
    /// segments do not count.
    pub(crate) fn reached(
        &self,
        base: Option<&BaseRef>,
        segments: &[SegmentRef],
        extra: Option<u64>,
    ) -> bool {
        let count = segments.len() + usize::from(extra.is_some());
        if count == 0 {
            return false;
        }
        let bytes = segments.iter().map(|s| s.size).sum::<u64>() + extra.unwrap_or(0);
        let base = base.map_or(0, |b| b.size);
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

impl Repo {
    /// Set what the next index write records in the head's `sweep`.
    pub(crate) fn set_pending_sweep(&mut self, p: PendingSweep) {
        self.pending_sweep = Some(p);
    }

    /// Set both graces of the index garbage collection, heads' and
    /// objects' (tests).
    #[cfg(test)]
    pub(crate) fn set_index_gc_grace(&mut self, grace: std::time::Duration) {
        self.index_gc_grace = index_store::IndexGcGrace::both(grace);
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

    /// Make the next index write fold the foldable prefix of the chain (a
    /// new base, garbage collected; young backup segments stay segments).
    /// With nothing foldable the write is a plain segment.
    #[cfg(any(test, feature = "sim"))]
    pub(crate) fn request_index_fold(&mut self) {
        self.fold_requested = self.fold_requested.max(FoldRequest::Eligible);
    }

    /// Make the next index write a full fold: every segment, young backup
    /// segments too, goes into the new base (`check --repair`).
    pub(crate) fn request_full_index_fold(&mut self) {
        self.fold_requested = FoldRequest::Full;
    }

    /// The per-repo cache dir the chunk tables live in, if there is one.
    pub(crate) fn cache_dir(&self) -> Option<PathBuf> {
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
    pub(super) fn persist_segmented_index(&mut self, fence: &Fence) -> Result<()> {
        match self.persist_segmented_index_inner(fence)? {
            MaintenanceWrite::Written => Ok(()),
            MaintenanceWrite::Moved => Err(stale_index_error()),
        }
    }

    /// [`Self::persist_segmented_index`] for lock-free maintenance (04 step
    /// 9): a head that moved since the index was loaded, or a generation
    /// another writer created, is [`MaintenanceWrite::Moved`] instead of the
    /// "index changed" error. Nothing on storage names a moved write (its
    /// objects are unreferenced garbage); the index edits are kept and the
    /// pending sweep is consumed, as on any failure. Not dirty: nothing is
    /// written. Saves the file cache after a write, as `save_state` does.
    /// `RepoWriter::persist_maintenance` calls it with its fence.
    pub(super) fn persist_maintenance_under(&mut self, fence: &Fence) -> Result<MaintenanceWrite> {
        if !self.index_dirty {
            return Ok(MaintenanceWrite::Written);
        }
        let written = self.persist_segmented_index_inner(fence);
        if !matches!(written, Ok(MaintenanceWrite::Written)) {
            self.pending_sweep = None;
        }
        if matches!(written, Ok(MaintenanceWrite::Written)) {
            self.save_file_cache_if_dirty()?;
        }
        written
    }

    /// Rebase a repack batch's pending relocations onto the latest head (04
    /// step 9, lock-free compact): take the pending segment (relocations and
    /// their packs' trailers only; anything else is a BUG error), discard
    /// the edits, load the index at the latest head, and re-apply each
    /// relocation whose chunk that index still holds in one of `sources`
    /// (the batch's source packs), with its new pack's trailer length.
    /// Another writer's head in between is safe to rebase over by this
    /// match: a chunk a sweep removed, or another compact moved, is dropped
    /// (its copy in this batch's new pack is dead bytes for a later
    /// compact). Returns how many relocations were dropped, and the sources
    /// at least one relocation was re-applied from.
    /// `RepoWriter::rebase_relocations` calls it.
    pub(super) fn rebase_relocations_under(
        &mut self,
        sources: &std::collections::HashSet<vykar_types::pack_id::PackId>,
    ) -> Result<(
        usize,
        std::collections::HashSet<vykar_types::pack_id::PackId>,
    )> {
        let seg = self.chunk_index.pending_segment(SegmentKind::Maintenance)?;
        if !seg.additions.is_empty() || !seg.removals.is_empty() {
            return Err(VykarError::Other(
                "BUG: a repack batch to rebase holds additions or removals".into(),
            ));
        }
        let trailers: std::collections::HashMap<_, _> = seg.pack_trailers.into_iter().collect();
        self.discard_index_edits();
        self.load_chunk_index_under_lock()?;
        let mut dropped = 0;
        let mut kept_from = std::collections::HashSet::new();
        for (id, loc) in seg.relocations {
            let from = self
                .chunk_index
                .get(&id)
                .map(|e| e.pack_id)
                .filter(|p| sources.contains(p));
            let here = from.is_some();
            kept_from.extend(from);
            let trailer = trailers.get(&loc.pack_id).copied();
            match (here, trailer) {
                (true, Some(trailer_len)) => {
                    self.index_dirty = true;
                    let index = &mut self.chunk_index;
                    index.set_pack_trailer(loc.pack_id, trailer_len);
                    index.update_location(&id, loc.pack_id, loc.pack_offset, loc.stored_size);
                }
                (true, None) => {
                    return Err(VykarError::Other(format!(
                        "BUG: a repack relocation into pack {} without its trailer length",
                        loc.pack_id
                    )));
                }
                (false, _) => dropped += 1,
            }
        }
        Ok((dropped, kept_from))
    }

    /// [`Self::persist_segmented_index`], a moved head or a taken generation
    /// being [`MaintenanceWrite::Moved`].
    fn persist_segmented_index_inner(&mut self, fence: &Fence) -> Result<MaintenanceWrite> {
        let listed_at = MarkerStamp::now(self.host_env().clock.as_ref());
        let head = match self.current_head_for_write() {
            Ok(head) => head,
            Err(e) if is_stale_index_error(&e) => return Ok(MaintenanceWrite::Moved),
            Err(e) => return Err(e),
        };
        let Some(head) = head else {
            // The fold's own listing chooses the generation.
            return self.publish_index_state(None, None, false, None, fence);
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
            return Ok(MaintenanceWrite::Moved);
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
        self.publish_index_state(Some(head), Some(seg), false, Some(listed_at), fence)
    }

    /// Publish `seg` as the successor of `prev`: as a segment, or with a
    /// fold. A full fold (every segment into the base) when there is no
    /// `prev` or a full fold was requested (`check --repair`); a fold
    /// of the foldable prefix (04 step 7) when a fold was requested or the
    /// prefix reaches the hard limit and something is foldable, keeping the
    /// young backup segments after the new base and `seg` after them when it
    /// is not foldable (a backup's own segment never is at the production
    /// grace).
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
    /// unreferenced object is left on storage. A generation another writer
    /// created is [`MaintenanceWrite::Moved`] (its callers under the lock
    /// make it the "index changed" error: 04 "Compact" 4, a maintenance
    /// write stops). `listed_at`: when the head listing that chose the
    /// generation started (`PublishMachine::listed_at`).
    fn publish_index_state(
        &mut self,
        prev: Option<IndexHead>,
        seg: Option<SegmentBody>,
        backup: bool,
        listed_at: Option<MarkerStamp>,
        fence: &Fence,
    ) -> Result<MaintenanceWrite> {
        let mut m = PublishMachine::publish(self, prev, seg, backup).listed_at(listed_at);
        let events = |_: CommitEvent| {};
        match drive_blocking(
            &mut m,
            &mut StepCx {
                repo: self,
                fence,
                events: &events,
            },
        )? {
            Published::Head(_) => Ok(MaintenanceWrite::Written),
            Published::Taken { .. } => Ok(MaintenanceWrite::Moved),
        }
    }

    /// Fold: write the index's current state as a base (no object for the
    /// empty index) and the head naming it, with `prev`'s folded objects as
    /// retired. `request` `Full` folds every segment; `Eligible` only the
    /// foldable prefix of `prev`'s chain (04 step 7), keeping the young
    /// backup segments after the new base, and writes nothing when no
    /// segment is foldable (the output is then `prev`). Nothing is deleted
    /// before the head PUT; after it, the age-gated garbage collection runs,
    /// best-effort. The base's body becomes the repository's table
    /// (`index_base.<name>`), with the kept segments applied over it and no
    /// edits. `removes`: the new state lacks entries `prev`'s has (for the
    /// sweep bookkeeping). A generation another writer created is the
    /// "index changed" error. `listed_at` as for
    /// [`Self::publish_index_state`].
    pub(super) fn put_base(
        &mut self,
        prev: Option<&IndexHead>,
        removes: bool,
        request: FoldRequest,
        listed_at: Option<MarkerStamp>,
        fence: &Fence,
    ) -> Result<IndexHead> {
        let mut m = PublishMachine::base(prev.cloned(), removes, request).listed_at(listed_at);
        let events = |_: CommitEvent| {};
        match drive_blocking(
            &mut m,
            &mut StepCx {
                repo: self,
                fence,
                events: &events,
            },
        )? {
            Published::Head(head) => Ok(head),
            Published::Taken { .. } => Err(stale_index_error()),
        }
    }

    /// Index upkeep at the end of `compact`: fold when the chain reached
    /// the soft limit, then delete old heads and unreferenced `index.d/`
    /// objects. The deletion is age-gated, so it never deletes a concurrent
    /// writer's fresh segment (H4-segment). The soft limit counts only the
    /// foldable prefix; young backup segments stay in the head (04 step 7).
    /// The deletion runs only when this handle created the latest head (the
    /// fold's, or the run's last index write): a writer that read an older
    /// head then finds its generation taken (04 step 8). `on_moved`: what a
    /// head that moved since the index was loaded, or a fold whose
    /// generation another writer created, does: the "index changed" error
    /// under the lock (`Fail`), or nothing more this run (`Skip`, a
    /// lock-free compact, 04 step 9: the fold and the collection run in a
    /// later compact or a backup's fold). `RepoWriter::maintain_segmented_index`
    /// saves a dirty index first, then calls it with its fence.
    pub(super) fn maintain_segmented_index_under(
        &mut self,
        on_moved: OnMoved,
        fence: &Fence,
    ) -> Result<()> {
        let skip = |e: VykarError| match on_moved {
            OnMoved::Skip if is_stale_index_error(&e) => {
                debug!("the index head moved; index fold and garbage collection skipped");
                Ok(())
            }
            _ => Err(e),
        };
        let listed_at = MarkerStamp::now(self.host_env().clock.as_ref());
        let head = match self.current_head_for_write() {
            Ok(head) => head,
            Err(e) => return skip(e),
        };
        let Some(head) = head else {
            return Ok(());
        };
        let k = self.foldable_prefix(&head.segments);
        let latest = if self.fold_policy.soft.reached(
            head.base.as_ref(),
            head.segments.get(..k).unwrap_or_default(),
            None,
        ) {
            if !self.index_at(&head) || self.chunk_index.is_modified() {
                let synced = self.sync_index(Some(head.clone()), true)?;
                self.install_synced_index(synced);
            }
            // A fold writes the same state as a base and the kept segments.
            let new_head = match self.put_base(
                Some(&head),
                false,
                FoldRequest::Eligible,
                Some(listed_at),
                fence,
            ) {
                Ok(h) => h,
                Err(e) => return skip(e),
            };
            self.index_generation = new_head.generation;
            self.index_head = Some(new_head.clone());
            new_head
        } else {
            head
        };
        if self.created_head != Some(latest.generation) {
            debug!("no index head written by this run; index garbage collection skipped");
            return Ok(());
        }
        if self.is_lock_free() {
            // A lock-free compact's marker check (04 step 9) guards its
            // deletes too.
            fence.check()?;
        }
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
                            self.request_full_index_fold();
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
        let local_tables = self.local_tables_for_repair();
        let anchor = index_store::cache_anchor(self.cache_dir().as_deref());
        let rebuilt = index_rebuild::rebuild_index_from_packs(
            self.storage.as_ref(),
            self.crypto.as_ref(),
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

    /// The verified base tables of this machine for a repair: the
    /// repository's own, if loaded, and every `index_base.*` file in its
    /// cache dir. Opened only when the returned closure is called.
    fn local_tables_for_repair(&self) -> impl FnOnce() -> Vec<Arc<ChunkTable>> + use<> {
        let dir = self.cache_dir();
        let own = self.chunk_index.table().cloned();
        move || {
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
        }
    }

    /// `check --repair`: entries, for chunks the loaded index lacks, of the
    /// listed packs it names none of
    /// ([`index_rebuild::recover_unindexed_entries`]). Read-only. Errors are
    /// framed like the rebuild's: nothing was changed.
    pub(crate) fn unindexed_pack_entries(
        &self,
        cancel: &CancelToken,
    ) -> Result<index_rebuild::Recovered> {
        let named: HashSet<PackId> = self.chunk_index.iter().map(|(_, e)| e.pack_id).collect();
        index_rebuild::recover_unindexed_entries(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            &named,
            |id| !self.chunk_index.contains(id),
            self.local_tables_for_repair(),
            self.request_width(),
            cancel,
        )
        .map_err(reindex_refusal)
    }

    /// Add `ids`' entries from `recovered` to the in-memory index (a
    /// present chunk is left alone), with their packs' trailer lengths, and
    /// mark the index dirty. Returns the number of chunks added and their
    /// packs (sorted, deduplicated).
    pub(crate) fn add_recovered_entries(
        &mut self,
        recovered: &index_rebuild::Recovered,
        ids: impl IntoIterator<Item = ChunkId>,
    ) -> (usize, Vec<PackId>) {
        let mut added = 0;
        let mut packs = Vec::new();
        for id in ids {
            let Some(&(pack, offset, size)) = recovered.entries.get(&id) else {
                continue;
            };
            if self.chunk_index.contains(&id) {
                continue;
            }
            // Every pack an entry names needs a trailer length in an index
            // write: the trailer's where it was read, else 0 (a map or a
            // local table described the pack), as the rebuild records it.
            // A length the index already knows is kept.
            if self.chunk_index.pack_trailer_len(&pack).is_none()
                || recovered.trailer_lens.contains_key(&pack)
            {
                self.chunk_index.set_pack_trailer(
                    pack,
                    recovered.trailer_lens.get(&pack).copied().unwrap_or(0),
                );
            }
            self.chunk_index.add(id, size, pack, offset);
            added += 1;
            packs.push(pack);
        }
        packs.sort_unstable_by_key(|p| *p.as_bytes());
        packs.dedup();
        if added > 0 {
            self.index_dirty = true;
        }
        (added, packs)
    }
}

/// The error a write returns when the head moved since the index was loaded,
/// or another writer created the generation it tried.
fn stale_index_error() -> VykarError {
    VykarError::IndexChanged
}

/// Whether `e` is [`stale_index_error`]. Not `IndexHeadContention`: a
/// write that lost its head create on every attempt is a failure, not a move.
fn is_stale_index_error(e: &VykarError) -> bool {
    matches!(e, VykarError::IndexChanged)
}

/// What a lock-free maintenance write found (04 step 9, phase F part 10d).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceWrite {
    /// The write is published (or there was nothing to write).
    Written,
    /// The latest head is not the loaded one, or the head create found its
    /// generation taken: nothing on storage names this write.
    Moved,
}

/// What [`super::RepoWriter::maintain_segmented_index`] does on a moved head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OnMoved {
    /// The "index changed" error (under the lock).
    Fail,
    /// Skip the fold and the garbage collection (lock-free compact).
    Skip,
}

/// The error of a backup commit whose head generation another writer took
/// on each of its `attempts` (04 "Backup commit" 7).
pub(crate) fn head_contention_error(generation: u64, attempts: u32) -> VykarError {
    VykarError::IndexHeadContention {
        detail: format!(
            "index head {generation} was created by another writer on each of {attempts} \
             attempts; retry the backup"
        ),
    }
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

/// The error of a failed lookup of the entries the index lost.
pub(crate) fn reindex_refusal(e: VykarError) -> VykarError {
    match e {
        VykarError::Interrupted => e,
        e => VykarError::Other(format!(
            "re-indexing the chunks the index lost from the packs: {e}. Nothing was changed"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only the stale-index error is a move; contention, and an `Other`
    /// carrying the same text, are failures.
    #[test]
    fn stale_index_error_matches_only_index_changed() {
        assert!(is_stale_index_error(&VykarError::IndexChanged));
        assert!(is_stale_index_error(&stale_index_error()));
        assert!(!is_stale_index_error(&head_contention_error(1, 3)));
        assert!(!is_stale_index_error(&VykarError::Other(
            "the index changed on storage after it was loaded; retry the operation".into()
        )));
    }
    use crate::index::head::{ObjectName, SegmentKind};

    fn seg(byte: u8, size: u64) -> SegmentRef {
        SegmentRef {
            name: ObjectName([byte; 32]),
            size,
            kind: SegmentKind::Backup,
            entries: 1,
        }
    }

    #[test]
    fn fold_limits_count_only_the_given_segments() {
        let limits = FoldLimits {
            segments: 3,
            percent: 20,
            min_bytes: 100,
        };
        let base = BaseRef {
            name: ObjectName([9; 32]),
            size: 1000,
            entries: 10,
        };
        let chain = [seg(1, 10), seg(2, 10), seg(3, 10)];
        assert!(!limits.reached(Some(&base), &[], None), "an empty chain");
        assert!(limits.reached(Some(&base), &chain, None), "by count");
        // A prefix of two does not reach the count; the extra segment does.
        assert!(!limits.reached(Some(&base), &chain[..2], None));
        assert!(limits.reached(Some(&base), &chain[..2], Some(10)));
        assert!(!limits.reached(None, &[], Some(10)), "one small segment");
        // By bytes: 20 % of the base and at least min_bytes.
        let big = [seg(4, 150), seg(5, 60)];
        assert!(
            limits.reached(Some(&base), &big, None),
            "210 >= 200 and 100"
        );
        assert!(!limits.reached(Some(&base), &big[..1], None), "150 < 200");
        assert!(limits.reached(Some(&base), &big[..1], Some(50)));
        // No base: min_bytes alone.
        assert!(limits.reached(None, &big[..1], None));
        assert!(!limits.reached(None, &[seg(6, 99)], None));
    }
}
