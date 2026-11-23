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
//! 5. Check the fence and PUT the snapshot.

use std::sync::Arc;

use tracing::{debug, warn};

use super::Repository;
use super::commit::{check_reused_chunks, commit_additions, needs_recheck};
use super::index_store::{self, SyncedIndex};
use super::lock::{self, SessionMarker};
use super::machine::{
    Machine, StepCx, StepKind, bad_pick, emit_stage, join_scoped, log_stage_elapsed,
};
use super::manifest::SnapshotEntry;
use super::publish_machine::PublishMachine;
use super::snapshot_cache::{self, SnapshotRefresh};
use super::write_session::SessionView;
use crate::index::IndexDelta;
use crate::index::head::{IndexHead, SegmentKind};
use crate::index::segment_v2::{Location, SegmentBody};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// The snapshot a finished [`BackupCommit`] wrote at its commit point.
pub(crate) struct Committed {
    pub(crate) entry: SnapshotEntry,
}

/// A backup's commit under the lock: the machine of steps 1-5 above.
pub(crate) struct BackupCommit {
    entry: Option<SnapshotEntry>,
    packed: Vec<u8>,
    session_id: String,
    /// The session's marker record; `None` for a session without a marker
    /// (repository-level tests).
    marker: Option<Arc<SessionMarker>>,
    /// The open `CommitEvent::Stage`, closed with `log_stage_elapsed`.
    stage: Option<(std::time::Instant, &'static str)>,
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
    ) -> Self {
        Self {
            entry: Some(entry),
            packed,
            session_id: repo.ws().session_id.clone(),
            marker,
            stage: None,
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
                    m.take_output().ok_or_else(|| {
                        VykarError::Other("BUG: the index write finished without a head".into())
                    })?;
                    self.close_stage();
                    after_index(cx.repo)
                } else {
                    CommitState::Publish(m)
                }
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

    fn read_one(&self, repo: &Repository, reads: &mut StartReads, read: Read) {
        match read {
            Read::Refresh => reads.refresh = Slot::Done(refresh_snapshots(repo)),
            Read::Marker => {
                if let Some(marker) = &self.marker {
                    reads.marker = Slot::Done(marker.probe(repo.storage.as_ref()));
                }
            }
            Read::Head => {
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
    fn read_all(&self, repo: &Repository, reads: &mut StartReads) {
        let do_refresh = reads.refresh.pending();
        let marker = reads
            .marker
            .pending()
            .then(|| self.marker.clone())
            .flatten();
        let do_head = reads.head.pending();
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
            let report = m.report(repo.host_env().clock.as_ref());
            lock::commit_marker_check(&self.session_id, probe, &report)?;
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
        let recheck = needs_recheck(view.as_ref().map(|v| (&v.state, v.sweep_gen)), &synced.head);
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
        let (verified, reused) = std::thread::scope(|scope| {
            let verify = scope.spawn(|| {
                super::pack_verify::verify_packs_present(storage, &pack_ids, persisted_packs, width)
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
        drop(delta);
        self.close_stage();

        self.open_stage(cx, "write index");
        let repo = &mut *cx.repo;
        let head = synced.head.clone();
        repo.install_synced_index(synced);
        if additions.is_empty() {
            debug!("backup added no chunks; no index write");
            self.close_stage();
            return Ok(after_index(repo));
        }
        let seg = SegmentBody {
            kind: SegmentKind::Backup,
            additions,
            removals: Vec::new(),
            relocations: Vec::new(),
            pack_trailers,
        };
        Ok(CommitState::Publish(Box::new(PublishMachine::publish(
            repo,
            Some(head),
            Some(seg),
            true,
        ))))
    }
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
