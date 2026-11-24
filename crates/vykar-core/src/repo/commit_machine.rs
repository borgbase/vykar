//! The backup commit as a steppable machine: [`BackupCommit`], from the
//! final flush through the PUT of `snapshots/<id>`, the commit point. The
//! work after it, which no error may escape, is
//! [`Repository::finish_backup_commit`].
//!
//! The steps, in order:
//!
//! 1. Flush packs, join uploads, take the session's delta.
//! 2. The start reads, independent of each other: refresh the snapshot
//!    list, then probe the session marker (the probe follows the refresh);
//!    fetch the index head when there is a delta to merge. Production runs
//!    them concurrently, as one batch; the simulator one at a time.
//! 3. Judge them in the old order: marker (the probe together with what the
//!    heartbeat recorded, [`lock::commit_marker_check`]), snapshot list and
//!    name check, head.
//! 4. Sync the index to the head, check reuse and verify the session's
//!    packs, then publish the additions as a backup segment (a
//!    [`PublishMachine`], stepped from here). A pack found missing while
//!    the session's marker is gone is `StaleChunksDuringCommit`: compact
//!    deleted the packs of a reaped session.
//!    The publish's fence before the head PUT also runs the commit rule's
//!    pack-age term ([`HeadCheck`]). A commit that adds no chunk writes no
//!    index under the lock; lock-free (04 step 9) it creates a head anyway,
//!    the head read's state at generation + 1 ([`PublishMachine::force_head`]),
//!    so a concurrent sweep conflicts with it.
//!    4b. If another writer created the head's generation (04 step 8): back
//!    off, then repeat from 2 with the additions kept (at most
//!    [`HEAD_CREATE_ATTEMPTS`] attempts). The sync re-checks reused chunks
//!    when the head moved, since the session began, in a way that can
//!    remove entries; only packs not verified yet are verified.
//! 5. Check the fence (and, when a head was written, the pack-age term
//!    again with no allowance: the binding check; lock-free, that the
//!    session marker is still there) and PUT the snapshot.
//!
//! Under the commit lock, or lock-free (04 step 9): then no lock and no
//! fence; the session marker carries `committing` from before the flush
//! (`PendingCommit::wait_until_unlocked`) until deregistration.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use super::commit::{check_reused_chunks, commit_additions, needs_recheck};
use super::index_store::{self, SyncedIndex};
use super::index_write::head_contention_error;
use super::lock::{self, MarkerStamp, SessionMarker};
use super::machine::{
    Machine, StepCx, StepKind, bad_pick, emit_stage, join_scoped, log_stage_elapsed,
};
use super::manifest::SnapshotEntry;
use super::publish_machine::{HeadCheck, PublishMachine, Published};
use super::snapshot_cache::{self, SnapshotRefresh};
use super::write_session::SessionView;
use super::{CommitMode, Repository};
use crate::index::head::{IndexHead, SegmentKind};
use crate::index::segment_v2::{Location, SegmentBody};
use crate::index::table::IndexState;
use crate::index::{IndexDelta, NewChunkEntry, ReusedChunks};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// Attempts at creating a backup's head (04 "Backup commit" 7).
pub(crate) const HEAD_CREATE_ATTEMPTS: u32 = 8;

/// The wait before attempt `attempt` (2..): 200 ms × 2^(attempt − 2), at
/// most 5 s, plus a jitter in [0, base/2) taken from BLAKE3(session_id ‖
/// attempt), so a simulated case replays. About 16–24 s for all eight.
pub(crate) fn retry_delay(session_id: &str, attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(2).min(5);
    let base_ms = (200u64 << exp).min(5_000);
    let mut hasher = blake3::Hasher::new();
    hasher.update(session_id.as_bytes());
    hasher.update(&attempt.to_le_bytes());
    let digest = hasher.finalize();
    let mut word = [0u8; 8];
    word.copy_from_slice(&digest.as_bytes()[..8]);
    let jitter_ms = u64::from_le_bytes(word) % (base_ms / 2).max(1);
    Duration::from_millis(base_ms + jitter_ms)
}

/// What a backup commit keeps of its session's delta across the publish,
/// for a retry after a taken generation: the reuse record, the new entries
/// the additions left out because the index held their chunks (a later
/// head may have removed them), and the trailer lengths. The additions
/// themselves come back from the publish (`Published::Taken`).
struct RetryDelta {
    reused: ReusedChunks,
    dropped: Vec<NewChunkEntry>,
    pack_trailers: HashMap<PackId, u32>,
}

/// The snapshot a finished [`BackupCommit`] wrote at its commit point.
pub(crate) struct Committed {
    pub(crate) entry: SnapshotEntry,
}

/// A backup's commit, under the lock or lock-free: the machine of steps
/// 1-5 above.
pub(crate) struct BackupCommit {
    entry: Option<SnapshotEntry>,
    packed: Vec<u8>,
    session_id: String,
    /// The session's marker record; `None` for a session without a marker
    /// (repository-level tests).
    marker: Option<Arc<SessionMarker>>,
    /// The open `CommitEvent::Stage`, closed with `log_stage_elapsed`.
    stage: Option<(std::time::Instant, &'static str)>,
    /// This commit's index write created a head: the fence before the
    /// snapshot PUT runs the pack-age term again, with no allowance.
    wrote_head: bool,
    /// The attempt at creating the head, from 1.
    attempt: u32,
    /// The session view's state and `sweep.gen`, from the first sync: a
    /// retry's reuse re-check is judged against where the session began.
    start_view: Option<(IndexState, u64)>,
    /// Packs whose presence an earlier attempt verified.
    verified_packs: HashSet<PackId>,
    /// Kept from the pack check until the head is created.
    retry: Option<RetryDelta>,
    /// The delta a retry starts from (between a taken create and the reads).
    delta: Option<IndexDelta>,
    /// Just before the head read that chose this attempt's generation.
    listed_at: Option<MarkerStamp>,
    /// Lock-free (04 step 9): no lock fence; the marker must still be
    /// there before the snapshot PUT.
    lock_free: bool,
    /// This attempt's index write is a head only (lock-free, no chunk
    /// added).
    head_only: bool,
    state: CommitState,
}

enum CommitState {
    Flush,
    /// The start reads.
    Reads(Box<StartReads>),
    /// Judge the start reads (`Local`).
    Judge(Box<StartReads>),
    SyncIndex(Box<SyncIndex>),
    VerifyPacks(Box<Verify>),
    Publish(Box<PublishMachine>),
    /// Wait before the next attempt (`Backoff`).
    Backoff,
    PersistIndex,
    Fence,
    PutSnapshot,
    Done(Option<Committed>),
    Failed,
}

/// A start read: not needed, pending, or done with its result.
enum Slot<T> {
    NotNeeded,
    Pending,
    Done(T),
}

impl<T> Slot<T> {
    fn pending(&self) -> bool {
        matches!(self, Slot::Pending)
    }

    fn needed(yes: bool) -> Self {
        if yes { Slot::Pending } else { Slot::NotNeeded }
    }
}

struct StartReads {
    delta: Option<IndexDelta>,
    view: Option<SessionView>,
    refresh: Slot<Result<SnapshotRefresh>>,
    /// `NotNeeded` for a session without a marker.
    marker: Slot<Result<bool>>,
    /// `NotNeeded` unless the delta is non-empty.
    head: Slot<Result<IndexHead>>,
}

/// One of the start reads.
#[derive(Clone, Copy)]
enum Read {
    Refresh,
    Marker,
    Head,
}

struct SyncIndex {
    head: IndexHead,
    delta: IndexDelta,
    view: Option<SessionView>,
}

struct Verify {
    synced: SyncedIndex,
    delta: IndexDelta,
    additions: Vec<(ChunkId, Location)>,
    pack_ids: std::collections::HashSet<PackId>,
    recheck: bool,
}

impl StartReads {
    /// The pending reads that can run now, in a fixed order: the refresh,
    /// the marker probe once the refresh is done (it followed the refresh
    /// on one thread), the head.
    fn enabled(&self) -> Vec<Read> {
        let mut out = Vec::new();
        if self.refresh.pending() {
            out.push(Read::Refresh);
        }
        if self.marker.pending() && !self.refresh.pending() {
            out.push(Read::Marker);
        }
        if self.head.pending() {
            out.push(Read::Head);
        }
        out
    }

    fn any_pending(&self) -> bool {
        self.refresh.pending() || self.marker.pending() || self.head.pending()
    }
}

impl Read {
    fn kind(self) -> StepKind {
        match self {
            Read::Refresh => StepKind::RefreshSnapshots,
            Read::Marker => StepKind::ProbeMarker,
            Read::Head => StepKind::FetchHead,
        }
    }
}

/// Strict I/O: fail on GET errors so a transient failure can't hide an
/// existing snapshot name and allow a duplicate during commit.
fn refresh_snapshots(repo: &Repository) -> Result<SnapshotRefresh> {
    snapshot_cache::refresh_snapshot_cache(
        repo.storage.as_ref(),
        repo.crypto.as_ref(),
        repo.config.id.as_slice(),
        repo.cache_dir_override.as_deref(),
        true,
    )
}

impl BackupCommit {
    /// The commit of the active write session as `entry` (`packed`: its
    /// snapshot blob), whose marker record is `marker`. No storage access.
    /// Panics when no write session is active.
    pub(crate) fn new(
        repo: &Repository,
        entry: SnapshotEntry,
        packed: Vec<u8>,
        marker: Option<Arc<SessionMarker>>,
        mode: CommitMode,
    ) -> Self {
        Self {
            lock_free: mode == CommitMode::LockFree,
            entry: Some(entry),
            packed,
            session_id: repo.ws().session_id.clone(),
            marker,
            stage: None,
            wrote_head: false,
            attempt: 1,
            start_view: None,
            verified_packs: HashSet::new(),
            retry: None,
            delta: None,
            listed_at: None,
            head_only: false,
            state: CommitState::Flush,
        }
    }

    fn open_stage(&mut self, cx: &StepCx<'_>, stage: &'static str) {
        self.stage = Some(emit_stage(cx.events, stage));
    }

    fn close_stage(&mut self) {
        if let Some(ctx) = self.stage.take() {
            log_stage_elapsed(ctx);
        }
    }

    fn entry(&self) -> Result<&SnapshotEntry> {
        self.entry
            .as_ref()
            .ok_or_else(|| VykarError::Other("BUG: the backup commit has no snapshot".into()))
    }

    /// Run the step `pick` of the current state, or (`pick == None`) every
    /// enabled step at once.
    fn run(&mut self, cx: &mut StepCx<'_>, pick: Option<usize>) -> Result<()> {
        let enabled = self.enabled();
        if enabled.is_empty() || pick.is_some_and(|p| p >= enabled.len()) {
            return Err(bad_pick("the backup commit", pick.unwrap_or(0), &enabled));
        }
        let state = std::mem::replace(&mut self.state, CommitState::Failed);
        self.state = match state {
            CommitState::Flush => self.flush(cx)?,
            CommitState::Reads(mut reads) => {
                match pick {
                    Some(p) => {
                        let read = reads
                            .enabled()
                            .get(p)
                            .copied()
                            .ok_or_else(|| bad_pick("the backup commit", p, &enabled))?;
                        self.read_one(cx.repo, &mut reads, read);
                    }
                    None => self.read_all(cx.repo, &mut reads),
                }
                if reads.any_pending() {
                    CommitState::Reads(reads)
                } else {
                    self.close_stage();
                    CommitState::Judge(reads)
                }
            }
            CommitState::Judge(reads) => self.judge(cx.repo, *reads)?,
            CommitState::SyncIndex(s) => self.sync(cx, *s)?,
            CommitState::VerifyPacks(v) => self.verify(cx, *v)?,
            CommitState::Publish(mut m) => {
                match pick {
                    Some(p) => m.step(cx, p)?,
                    None => m.step_all(cx)?,
                }
                if m.enabled().is_empty() {
                    self.wrote_head = m.wrote_head();
                    let published = m.take_output().ok_or_else(|| {
                        VykarError::Other("BUG: the index write finished without a head".into())
                    })?;
                    self.close_stage();
                    match published {
                        Published::Head(_) => {
                            self.retry = None;
                            after_index(cx.repo)
                        }
                        Published::Taken { generation, seg } => self.taken(generation, seg)?,
                    }
                } else {
                    CommitState::Publish(m)
                }
            }
            CommitState::Backoff => {
                cx.repo
                    .host_env()
                    .clock
                    .sleep(retry_delay(&self.session_id, self.attempt + 1));
                self.attempt += 1;
                self.open_stage(cx, "refresh snapshots");
                CommitState::Reads(Box::new(StartReads {
                    delta: self.delta.take(),
                    view: None,
                    refresh: Slot::Pending,
                    marker: Slot::needed(self.marker.is_some()),
                    head: Slot::Pending,
                }))
            }
            CommitState::PersistIndex => {
                // Unreachable today (a backup session records every change
                // in its delta); guards future callers.
                cx.repo.persist_segmented_index()?;
                CommitState::Fence
            }
            CommitState::Fence => {
                self.open_stage(cx, "write snapshot");
                cx.repo.check_lock_fence()?;
                self.check_landed_head(cx.repo)?;
                self.check_marker_before_snapshot(cx.repo)?;
                CommitState::PutSnapshot
            }
            CommitState::PutSnapshot => {
                // ---- COMMIT POINT ----
                let key = self.entry()?.id.storage_key();
                cx.repo.storage.put(&key, &self.packed)?;
                self.close_stage();
                let entry = self.entry.take().ok_or_else(|| {
                    VykarError::Other("BUG: the backup commit has no snapshot".into())
                })?;
                CommitState::Done(Some(Committed { entry }))
            }
            CommitState::Done(_) | CommitState::Failed => {
                return Err(bad_pick("the backup commit", pick.unwrap_or(0), &enabled));
            }
        };
        Ok(())
    }

    /// The head create found `generation` taken: rebuild the delta from the
    /// additions the publish handed back and what [`RetryDelta`] kept, and
    /// back off; after [`HEAD_CREATE_ATTEMPTS`] the "index changed" error.
    fn taken(&mut self, generation: u64, seg: Option<SegmentBody>) -> Result<CommitState> {
        if self.attempt >= HEAD_CREATE_ATTEMPTS {
            return Err(head_contention_error(generation, self.attempt));
        }
        // A head-only write (a lock-free commit that added no chunk) hands
        // back no segment: the delta is what `RetryDelta` kept.
        let Some(kept) = self.retry.take() else {
            return Err(VykarError::Other(
                "BUG: a backup's taken index write kept no retry delta".into(),
            ));
        };
        if seg.is_none() && !self.head_only {
            return Err(VykarError::Other(
                "BUG: a backup's taken index write returned no segment".into(),
            ));
        }
        tracing::info!(
            attempt = self.attempt,
            generation,
            "index head taken by another writer; rebasing the commit"
        );
        let mut new_entries: Vec<NewChunkEntry> = seg
            .iter()
            .flat_map(|seg| seg.additions.iter())
            .map(|(id, loc)| NewChunkEntry {
                chunk_id: *id,
                stored_size: loc.stored_size,
                pack_id: loc.pack_id,
                pack_offset: loc.pack_offset,
            })
            .collect();
        drop(seg);
        new_entries.extend(kept.dropped);
        self.delta = Some(IndexDelta {
            new_entries,
            reused: kept.reused,
            pack_trailers: kept.pack_trailers,
        });
        Ok(CommitState::Backoff)
    }

    /// The binding pack-age check (04 I8): once this commit's head PUT has
    /// returned, a session whose marker record is not sound and whose packs
    /// may be [`PACK_AGE_LIMIT`](super::ages::PACK_AGE_LIMIT) old by now does not write its snapshot.
    /// The head stays without its snapshot, the state of a commit whose
    /// snapshot PUT failed: the error is `StaleChunksDuringCommit`, and a
    /// retry deduplicates against that head (it indexes this run's chunks).
    fn check_landed_head(&self, repo: &Repository) -> Result<()> {
        let (true, Some(m)) = (self.wrote_head, &self.marker) else {
            return Ok(());
        };
        let clock = repo.host_env().clock.as_ref();
        lock::head_landing_check(
            &self.session_id,
            &m.report(clock),
            m.elapsed(clock),
            Duration::ZERO,
        )
    }

    /// A lock-free commit (04 step 9) writes its snapshot only while its
    /// marker was never reaped: the marker handshake keeps lock holders
    /// out of this commit, and a compact's mark relies on the marker until
    /// the snapshot is listed, both only while the marker is visible. A
    /// present current key and a [continuous](lock::MarkerReport::continuous)
    /// record mean no reap. A miss alone is not enough: a reaper deletes
    /// the keys its listing showed, so one that listed key n aged (the
    /// record has a gap over `MARKER_GAP_LIMIT`, since a key ages only
    /// unrefreshed) and deletes it after the heartbeat wrote n + 1 and found
    /// n present leaves n + 1 present and no miss recorded, while it
    /// believes the session reaped and did not yield to it. Otherwise
    /// `MarkerLostDuringCommit`: the head stays without its snapshot. A
    /// retry reads its sources again (the failed run saved no file cache)
    /// but deduplicates against that head, which indexes this run's chunks:
    /// it uploads only what it would have uploaded beside a committed run.
    /// A session whose record was already not continuous before its mark
    /// does not get here: it commits under the lock
    /// (`PendingCommit::commit_mode`), so this refusal catches only a
    /// discontinuity that arose while committing. A failed probe fails the
    /// commit with its error.
    fn check_marker_before_snapshot(&self, repo: &Repository) -> Result<()> {
        let (true, Some(m)) = (self.lock_free, &self.marker) else {
            return Ok(());
        };
        let present = m.probe(repo.storage.as_ref())?;
        let report = m.report(repo.host_env().clock.as_ref());
        if present && report.continuous() {
            return Ok(());
        }
        if !present {
            m.note_missing();
            warn!(
                session_id = self.session_id,
                "the session marker was gone before the snapshot was written; a concurrent \
                 compact may have swept its chunks; retry the backup"
            );
        } else {
            warn!(
                session_id = self.session_id,
                ?report,
                "the session marker's record is not continuous before the snapshot write \
                 (unrefreshed for a while, a slow marker write, or a miss): a lock holder may \
                 have reaped it; retry the backup"
            );
        }
        Err(VykarError::MarkerLostDuringCommit)
    }

    /// Step 1. The "refresh snapshots" stage starts whether or not the
    /// flush failed; on failure no read runs and the flush error returns.
    fn flush(&mut self, cx: &mut StepCx<'_>) -> Result<CommitState> {
        let taken = cx.repo.flush_and_take_session_delta();
        self.open_stage(cx, "refresh snapshots");
        let (delta, view) = match taken {
            Ok(t) => t,
            Err(e) => {
                self.close_stage();
                return Err(e);
            }
        };
        let fetch_index = delta.as_ref().is_some_and(|d| !d.is_empty());
        Ok(CommitState::Reads(Box::new(StartReads {
            delta,
            view,
            refresh: Slot::Pending,
            marker: Slot::needed(self.marker.is_some()),
            head: Slot::needed(fetch_index),
        })))
    }

    fn read_one(&mut self, repo: &Repository, reads: &mut StartReads, read: Read) {
        match read {
            Read::Refresh => reads.refresh = Slot::Done(refresh_snapshots(repo)),
            Read::Marker => {
                if let Some(marker) = &self.marker {
                    reads.marker = Slot::Done(marker.probe(repo.storage.as_ref()));
                }
            }
            Read::Head => {
                self.listed_at = Some(MarkerStamp::now(repo.host_env().clock.as_ref()));
                reads.head = Slot::Done(index_store::fetch_head(
                    repo.storage.as_ref(),
                    repo.crypto.as_ref(),
                ));
            }
        }
    }

    /// The pending start reads as today's code runs them: a helper thread
    /// refreshes the snapshot list and then probes the marker, while this
    /// thread fetches the head. All are reads under the commit lock; every
    /// write follows them.
    fn read_all(&mut self, repo: &Repository, reads: &mut StartReads) {
        let do_refresh = reads.refresh.pending();
        let marker = reads
            .marker
            .pending()
            .then(|| self.marker.clone())
            .flatten();
        let do_head = reads.head.pending();
        if do_head {
            // Before the refresh's share too: an over-estimate of the time
            // since the head listing is the safe direction.
            self.listed_at = Some(MarkerStamp::now(repo.host_env().clock.as_ref()));
        }
        let storage = repo.storage.as_ref();
        let crypto = repo.crypto.as_ref();
        let repo_id = repo.config.id.as_slice();
        let cache_dir = repo.cache_dir_override.as_deref();
        let (side, head) = std::thread::scope(|scope| {
            let side = (do_refresh || marker.is_some()).then(|| {
                scope.spawn(move || {
                    let refresh = do_refresh.then(|| {
                        snapshot_cache::refresh_snapshot_cache(
                            storage, crypto, repo_id, cache_dir, true,
                        )
                    });
                    let marker = marker.map(|m| m.probe(storage));
                    (refresh, marker)
                })
            });
            let head = do_head.then(|| index_store::fetch_head(storage, crypto));
            (side.map(join_scoped), head)
        });
        if let Some((refresh, marker)) = side {
            if let Some(r) = refresh {
                reads.refresh = Slot::Done(r);
            }
            if let Some(m) = marker {
                reads.marker = Slot::Done(m);
            }
        }
        if let Some(h) = head {
            reads.head = Slot::Done(h);
        }
    }

    /// Step 3: probe, snapshot list and name check, index head.
    fn judge(&mut self, repo: &mut Repository, reads: StartReads) -> Result<CommitState> {
        let StartReads {
            delta,
            view,
            refresh,
            marker,
            head,
        } = reads;
        // Session-existence check. If the marker was reaped by maintenance
        // (e.g. because we missed too many heartbeats under extreme clock
        // skew), surface the same typed error the later reuse check would
        // produce, so callers matching on `StaleChunksDuringCommit` catch
        // this path too. The heartbeat's record is read after the probe, so
        // a reap a refresh found during the probe is seen. An inconclusive
        // probe falls through only when the record is continuous; the reuse
        // check is still a safety net.
        if let (Slot::Done(probe), Some(m)) = (marker, &self.marker) {
            let clock = repo.host_env().clock.clone();
            let report = m.report(clock.as_ref());
            // A current key found gone is recorded, so the checks at the
            // fences around the head PUT see it too. After the report, so
            // this judgement names the probe's finding, not a refresh's.
            if matches!(probe, Ok(false)) {
                m.note_missing();
            }
            lock::commit_marker_check(&self.session_id, probe, &report, m.elapsed(clock.as_ref()))?;
        }
        match refresh {
            Slot::Done(refresh) => repo.apply_snapshot_refresh(refresh?),
            Slot::NotNeeded | Slot::Pending => {
                return Err(VykarError::Other(
                    "BUG: the backup commit judged before refreshing the snapshot list".into(),
                ));
            }
        }

        // Snapshot name uniqueness against the fresh list.
        let name = &self.entry()?.name;
        if repo.manifest.find_snapshot(name).is_some() {
            return Err(VykarError::SnapshotAlreadyExists(name.clone()));
        }

        // The delta's additions are written at the head. The repository's
        // index becomes the head's, with the new segment applied.
        match delta {
            Some(delta) if !delta.is_empty() => {
                let head = match head {
                    Slot::Done(head) => head?,
                    Slot::NotNeeded | Slot::Pending => {
                        self.listed_at = Some(MarkerStamp::now(repo.host_env().clock.as_ref()));
                        index_store::fetch_head(repo.storage.as_ref(), repo.crypto.as_ref())?
                    }
                };
                Ok(CommitState::SyncIndex(Box::new(SyncIndex {
                    head,
                    delta,
                    view,
                })))
            }
            _ => Ok(after_index(repo)),
        }
    }

    /// Step 4, first half: bring the index to the head (normally the
    /// repository's own, already there) and collect the additions.
    fn sync(&mut self, cx: &mut StepCx<'_>, s: SyncIndex) -> Result<CommitState> {
        let SyncIndex { head, delta, view } = s;
        self.open_stage(cx, "fetch index");
        let synced = cx.repo.sync_index(Some(head), true)?;
        self.close_stage();
        let recheck = match &self.start_view {
            Some((state, sweep_gen)) => needs_recheck(Some((state, *sweep_gen)), &synced.head),
            None => needs_recheck(view.as_ref().map(|v| (&v.state, v.sweep_gen)), &synced.head),
        };
        if self.attempt == 1 {
            self.start_view = view.as_ref().map(|v| (v.state.clone(), v.sweep_gen));
        }
        // Nothing else may hold the view's applied layer when the segment
        // is applied (`Arc::make_mut` would copy it).
        drop(view);

        self.open_stage(cx, "check reuse");
        // The additions: one per chunk (its first location), minus chunks
        // the synced index already holds (another session committed them
        // first; this session's copies are dead bytes).
        let additions = commit_additions(&delta, &synced.index);
        let pack_ids = additions.iter().map(|(_, l)| l.pack_id).collect();
        Ok(CommitState::VerifyPacks(Box::new(Verify {
            synced,
            delta,
            additions,
            pack_ids,
            recheck,
        })))
    }

    /// Step 4, second half: check the reused chunks when the head moved in
    /// a way that can have removed entries, while the session's packs are
    /// verified; then start the index write.
    fn verify(&mut self, cx: &mut StepCx<'_>, v: Verify) -> Result<CommitState> {
        let Verify {
            synced,
            delta,
            additions,
            pack_ids,
            recheck,
        } = v;
        let repo = &mut *cx.repo;
        let persisted_packs = repo.persisted_pack_count();
        let width = repo.request_width();
        let storage = repo.storage.as_ref();
        // A retry verifies only the packs an earlier attempt did not.
        let unverified: HashSet<PackId> = pack_ids
            .iter()
            .filter(|p| !self.verified_packs.contains(*p))
            .copied()
            .collect();
        let (verified, reused) = std::thread::scope(|scope| {
            let verify = scope.spawn(|| {
                super::pack_verify::verify_packs_present(
                    storage,
                    &unverified,
                    persisted_packs,
                    width,
                )
            });
            let reused = if recheck {
                check_reused_chunks(&delta, &synced.index)
            } else {
                debug!(
                    "index head unchanged since the session began, except for backup segments; reused chunks not re-checked"
                );
                Ok(())
            };
            (join_scoped(verify), reused)
        });
        // A stale chunk is the typed error callers retry on; report it first.
        reused?;
        if let Err(e) = verified {
            // A missing pack while the session's marker is gone: compact
            // deleted the packs of a reaped session, which is what
            // `StaleChunksDuringCommit` means. With the marker present it is
            // storage loss and keeps its own error.
            if let Some(m) = &self.marker {
                let gone = m.report(repo.host_env().clock.as_ref()).missing
                    || matches!(m.probe(storage), Ok(false));
                if gone {
                    warn!(
                        session_id = self.session_id,
                        error = %e,
                        "session pack check failed and the session marker is gone; retry the backup"
                    );
                    return Err(VykarError::StaleChunksDuringCommit);
                }
            }
            return Err(e);
        }
        self.verified_packs.extend(unverified);
        let mut pack_trailers: Vec<(PackId, u32)> = Vec::with_capacity(pack_ids.len());
        for pack in &pack_ids {
            let trailer_len = delta.pack_trailers.get(pack).copied().ok_or_else(|| {
                VykarError::Other(format!(
                    "BUG: the backup commit does not know the trailer length of pack {pack}"
                ))
            })?;
            pack_trailers.push((*pack, trailer_len));
        }
        pack_trailers.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        // A retry needs the reuse record and the new entries the additions
        // left out; the additions come back from a taken publish.
        let IndexDelta {
            new_entries,
            reused,
            pack_trailers: trailers,
        } = delta;
        let dropped = dropped_entries(new_entries, &synced.index);
        self.retry = Some(RetryDelta {
            reused,
            dropped,
            pack_trailers: trailers,
        });
        self.close_stage();

        self.open_stage(cx, "write index");
        let repo = &mut *cx.repo;
        let head = synced.head.clone();
        repo.install_synced_index(synced);
        let check = self.marker.as_ref().map(|marker| HeadCheck {
            session_id: self.session_id.clone(),
            marker: Arc::clone(marker),
        });
        self.head_only = additions.is_empty();
        if additions.is_empty() {
            if !self.lock_free {
                debug!("backup added no chunks; no index write");
                self.retry = None;
                self.close_stage();
                return Ok(after_index(repo));
            }
            // Lock-free (04 step 9, Q2): a head anyway, the head read's
            // state at its generation + 1. Without it a reuse-only commit
            // conflicts with nothing a concurrent sweep writes, and a sweep
            // whose second session listing preceded this commit's mark could
            // remove a chunk it reused.
            debug!("backup added no chunks; writing a head only (lock-free)");
            return Ok(CommitState::Publish(Box::new(
                PublishMachine::publish(repo, Some(head), None, true)
                    .force_head()
                    .with_head_check(check)
                    .listed_at(self.listed_at),
            )));
        }
        let seg = SegmentBody {
            kind: SegmentKind::Backup,
            additions,
            removals: Vec::new(),
            relocations: Vec::new(),
            pack_trailers,
        };
        Ok(CommitState::Publish(Box::new(
            PublishMachine::publish(repo, Some(head), Some(seg), true)
                .with_head_check(check)
                .listed_at(self.listed_at),
        )))
    }
}

/// The new entries [`commit_additions`] leaves out because `index` holds
/// their chunks, in their order (normally none: another session committed
/// the same chunks first).
fn dropped_entries(
    new_entries: Vec<NewChunkEntry>,
    index: &crate::index::LiveIndex,
) -> Vec<NewChunkEntry> {
    new_entries
        .into_iter()
        .filter(|e| index.contains(&e.chunk_id))
        .collect()
}

/// After the index write (or none): the defensive persist of a dirty index,
/// else the fence.
fn after_index(repo: &Repository) -> CommitState {
    if repo.index_dirty {
        CommitState::PersistIndex
    } else {
        CommitState::Fence
    }
}

impl Machine for BackupCommit {
    type Output = Committed;

    fn enabled(&self) -> Vec<StepKind> {
        match &self.state {
            CommitState::Flush => vec![StepKind::Flush],
            CommitState::Reads(reads) => reads.enabled().into_iter().map(Read::kind).collect(),
            CommitState::Judge(_) => vec![StepKind::Local],
            CommitState::SyncIndex(_) => vec![StepKind::SyncIndex],
            CommitState::VerifyPacks(_) => vec![StepKind::VerifyPacks],
            CommitState::Publish(m) => m.enabled(),
            CommitState::Backoff => vec![StepKind::Backoff],
            CommitState::PersistIndex => vec![StepKind::PersistIndex],
            CommitState::Fence => vec![StepKind::Fence],
            CommitState::PutSnapshot => vec![StepKind::PutSnapshot],
            CommitState::Done(_) | CommitState::Failed => Vec::new(),
        }
    }

    fn step(&mut self, cx: &mut StepCx<'_>, pick: usize) -> Result<()> {
        let result = self.run(cx, Some(pick));
        if result.is_err() {
            self.state = CommitState::Failed;
        }
        result
    }

    fn step_all(&mut self, cx: &mut StepCx<'_>) -> Result<()> {
        let result = self.run(cx, None);
        if result.is_err() {
            self.state = CommitState::Failed;
        }
        result
    }

    fn take_output(&mut self) -> Option<Committed> {
        match &mut self.state {
            CommitState::Done(c) => c.take(),
            _ => None,
        }
    }
}
