//! The index write as a steppable machine: [`PublishMachine`], the shared
//! head-publish path of every index write (backup commits, maintenance,
//! repair, upgrade). The write sequences are those of `index_write.rs`'s
//! module doc, one storage PUT per step:
//!
//! - head only (a sweep pending, nothing else to write): [record] → fence →
//!   head; a forced head (a lock-free backup that added no chunk,
//!   [`PublishMachine::force_head`]): fence → head → old heads deleted;
//! - segment: [record] → segment → fence → head;
//! - fold: [list heads] → [base] → [record] → fence → head → garbage
//!   collection;
//! - partial fold (04 step 7): [fold-prefix] → [base] → [record] →
//!   [segment] → fence → head → garbage collection. The base holds the
//!   longest foldable prefix of the chain; the young backup segments after
//!   it stay named by the head, followed by the write's own segment when it
//!   is not foldable.
//!
//! A backup's write also runs the commit rule's pack-age term at its fence
//! ([`HeadCheck`]), the last step before the head PUT on every path.
//!
//! The head step creates the head (04 step 8, [`index_store::create_head`]):
//! put-if-absent where the backend offers it and its check passed (the
//! check runs at the fence, before the lock and pack-age checks), else a
//! plain PUT under the lock. When another writer holds the generation the
//! write ends there: nothing local changes (the error cleanup runs), the
//! objects it PUT are unreferenced garbage, no garbage collection runs, and
//! the output is [`Published::Taken`]. A backup commit rebases and tries
//! again; every other writer stops with "index changed".

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::{debug, warn};

use super::ages::{
    HEAD_FRESHNESS_LIMIT, HEAD_LANDING_ALLOWANCE, INDEX_WRITE_LIMIT, SLOW_INDEX_WRITE_TEXT,
};
use super::index_store::{self, HeadPut, PackedObject};
use super::index_write::FoldRequest;
use super::lock::{self, MarkerStamp, SessionMarker};
use super::machine::{Machine, StepCx, StepKind, bad_pick};
use crate::index::LiveIndex;
use crate::index::head::{
    BaseRef, HEAD_LAYOUT_VERSION, IndexHead, ObjectName, SegmentKind, SegmentRef,
};
use crate::index::segment_v2::SegmentBody;
use crate::index::table::{self, ChunkTable, IndexState, TableFileWriter};
use vykar_types::error::{Result, VykarError};

/// What an index write ended with.
pub(crate) enum Published {
    /// The head this write created, or the previous head when nothing was
    /// written.
    Head(IndexHead),
    /// Another head holds the generation this write tried. Nothing local
    /// changed; the objects it PUT are unreferenced garbage. `seg`: a
    /// backup's own segment body, handed back for its retry.
    Taken {
        generation: u64,
        seg: Option<SegmentBody>,
    },
}

/// One index write, from the state to publish to the head that names it.
/// Built by [`PublishMachine::publish`] (what
/// `Repo::publish_index_state` does) or [`PublishMachine::base`]
/// (`Repo::put_base`); its output is the head written, or the
/// previous head when nothing was, or [`Published::Taken`].
pub(crate) struct PublishMachine {
    /// `Some(request)`: a publish, which took `request` from the
    /// repository's `fold_requested` and restores it on error, and which
    /// updates the repository's head bookkeeping. `None`: a base write
    /// alone.
    publish: Option<FoldRequest>,
    /// The fold asked for: a publish's request, or a base write's.
    request: FoldRequest,
    backup: bool,
    /// A fold added a backup's additions to the index as edits: an error
    /// before the head PUT drops them again.
    added: bool,
    /// A backup's pack-age term, run at the fence before the head PUT.
    head_check: Option<HeadCheck>,
    /// This write created its head.
    wrote_head: bool,
    /// Write a head even when nothing changed (`Self::force_head`).
    force_head: bool,
    /// Just before the head listing that chose this write's generation
    /// (suspend included, [`MarkerStamp::until`]); the machine's first step
    /// when the caller gave none.
    listed_at: Option<MarkerStamp>,
    start: std::time::Instant,
    state: State,
}

/// What a backup's index write checks at its fence, after the lock fence:
/// the commit rule's pack-age term against the session's marker record.
pub(crate) struct HeadCheck {
    pub(crate) session_id: String,
    pub(crate) marker: Arc<SessionMarker>,
}

impl HeadCheck {
    /// [`lock::head_landing_check`] with [`HEAD_LANDING_ALLOWANCE`]: the
    /// fence is the last step before the head PUT on every path.
    ///
    /// [`HEAD_LANDING_ALLOWANCE`]: super::ages::HEAD_LANDING_ALLOWANCE
    fn run(&self, repo: &super::Repo) -> Result<()> {
        let clock = repo.host_env().clock.as_ref();
        lock::head_landing_check(
            &self.session_id,
            &self.marker.report(clock),
            self.marker.elapsed(clock),
            super::ages::HEAD_LANDING_ALLOWANCE,
        )
    }
}

enum State {
    /// Choose the path (`Local`).
    Start {
        prev: Option<IndexHead>,
        seg: Option<SegmentBody>,
    },
    /// A fold of the foldable prefix over a chain with backup segments:
    /// list the segments' ages, decide, and fetch what a partial fold
    /// needs (`FoldPrefix`).
    FoldPrefix(Box<PrefixPrep>),
    /// Fold: write the base body (`Local`).
    FoldBody(Box<FoldPrep>),
    /// Fold without a previous head: LIST `index.h/` for the generation.
    FoldGeneration(Box<FoldBase>),
    /// Fold: the sweep and the head (`Local`).
    FoldHead(Box<FoldBase>, u64),
    /// The PUTs, fence and garbage collection, then the finish.
    Write(Box<Writes>),
    Done(Option<IndexHead>),
    /// The head create found `generation` taken; a backup's own segment.
    Lost(u64, Option<SegmentBody>),
    Failed,
}

/// A fold candidate whose foldable prefix is still to be judged.
struct PrefixPrep {
    prev: IndexHead,
    /// The write's own change (non-empty) and its packed segment.
    own: Option<(SegmentBody, PackedObject)>,
    removes: bool,
    /// A fold was asked for: fold whatever is foldable, limits or not.
    forced: bool,
}

struct FoldPrep {
    prev: Option<IndexHead>,
    seg: Option<SegmentBody>,
    removes: bool,
    mode: FoldMode,
}

/// What a fold writes into its base.
enum FoldMode {
    /// Every segment and the write's own change: the base is the index's
    /// full state (a backup's additions added as edits first).
    Full,
    /// The first `k` segments of `prev` (04 step 7). The others stay
    /// segments, followed by `appended` (the write's own segment) when
    /// given. With `k` = the chain's length the base is the index's loaded
    /// state; otherwise the table plus the first `k` segments.
    Partial {
        k: usize,
        /// Per segment of `prev`: its bytes when the local cache lacks
        /// them (fetched), `None` when the cache holds them.
        bytes: Vec<Option<Vec<u8>>>,
        appended: Option<PackedObject>,
    },
}

/// A fold's base, written to a local table, not yet published.
struct FoldBase {
    prev: Option<IndexHead>,
    removes: bool,
    dir: Option<PathBuf>,
    table: ChunkTable,
    obj: Option<PackedObject>,
    entries: u64,
    /// Segments of `prev` the head keeps after the new base, with their
    /// bytes when the local cache lacks them.
    kept: Vec<(SegmentRef, Option<Vec<u8>>)>,
    /// The write's own segment, listed after the kept ones.
    appended: Option<(PackedObject, SegmentBody)>,
    /// A backup's own segment folded into the base (a full fold), kept for
    /// a retry after a taken generation.
    folded: Option<SegmentBody>,
}

/// The rest of a write: storage steps in order, then the finish.
struct Writes {
    steps: VecDeque<StepKind>,
    head: IndexHead,
    /// The head's bytes; a head-only write packs them at its PUT. Kept
    /// after the create, which compares them with what the key holds.
    packed_head: Option<Vec<u8>>,
    base: Option<PackedObject>,
    record: Option<PackedObject>,
    segment: Option<PackedObject>,
    /// The size of the segment PUT, logged once the head is written.
    segment_written: Option<u64>,
    /// Taken just before the request of the write's first object PUT (base,
    /// sweep record or segment): the write's age against
    /// [`INDEX_WRITE_LIMIT`] at its head create (`None`: a head-only
    /// write, which PUTs no object).
    objects_from: Option<MarkerStamp>,
    /// How the head create ended.
    outcome: Option<HeadPut>,
    finish: Finish,
}

enum Finish {
    /// Head only: drop the edits (they changed nothing).
    Unchanged,
    /// Apply the segment (backup) or mark the edits written.
    Segment { name: ObjectName, seg: SegmentBody },
    /// The base becomes the repository's table, with the kept segments
    /// and the appended one applied over it.
    Fold(Box<FoldFinish>),
}

/// What a fold's finish installs.
struct FoldFinish {
    dir: Option<PathBuf>,
    table: ChunkTable,
    name: Option<ObjectName>,
    bytes: u64,
    gc: index_store::IndexGcStats,
    /// The kept segments, with their bytes when the local cache lacks them.
    kept: Vec<(SegmentRef, Option<Vec<u8>>)>,
    /// The write's own segment, applied last.
    appended: Option<(ObjectName, SegmentBody)>,
    /// See [`FoldBase::folded`].
    folded: Option<SegmentBody>,
}

impl PublishMachine {
    /// Publish `seg` as the successor of `prev`: as a segment, or with a
    /// fold. A full fold when there is no `prev`, a full fold was requested
    /// or the packed segment's name is already named by `prev` (plaintext
    /// repeat); a fold of the foldable prefix when a fold was requested or
    /// the foldable prefix (with the write's own segment, when foldable)
    /// reaches the hard limit, and there is something to fold. Takes
    /// `repo.fold_requested`. See `Repo::publish_index_state` for
    /// `backup` and the errors.
    pub(crate) fn publish(
        repo: &mut super::Repo,
        prev: Option<IndexHead>,
        seg: Option<SegmentBody>,
        backup: bool,
    ) -> Self {
        let request = std::mem::take(&mut repo.fold_requested);
        Self {
            publish: Some(request),
            request,
            backup,
            added: false,
            head_check: None,
            wrote_head: false,
            force_head: false,
            listed_at: None,
            start: std::time::Instant::now(),
            state: State::Start { prev, seg },
        }
    }

    /// A fold of the index's current state (`Repo::put_base`):
    /// `Full` (or no `prev`) folds every segment, `Eligible` the foldable
    /// prefix of `prev`'s chain; with nothing foldable it writes nothing
    /// and outputs `prev`.
    pub(crate) fn base(prev: Option<IndexHead>, removes: bool, request: FoldRequest) -> Self {
        let state = if prev.is_some() && request != FoldRequest::Full {
            State::Start { prev, seg: None }
        } else {
            State::FoldBody(Box::new(FoldPrep {
                prev,
                seg: None,
                removes,
                mode: FoldMode::Full,
            }))
        };
        Self {
            publish: None,
            request: request.max(FoldRequest::Eligible),
            backup: false,
            added: false,
            head_check: None,
            wrote_head: false,
            force_head: false,
            listed_at: None,
            start: std::time::Instant::now(),
            state,
        }
    }

    /// Write a head when there is nothing else to write: the predecessor's
    /// state (base, segments, sweep) at generation + 1, created with
    /// put-if-absent, then the head part of the garbage collection
    /// ([`index_store::collect_old_heads`]). A lock-free backup that added
    /// no chunk but reused some (04 step 9, Q2): without a head of its own
    /// nothing would conflict with a concurrent sweep that removes a chunk
    /// it reused, since compact's second session listing alone does not
    /// cover a backup whose head read followed compact's.
    pub(crate) fn force_head(mut self) -> Self {
        self.force_head = true;
        self
    }

    /// Apply `check` at the fence before the head PUT (backups only).
    pub(crate) fn with_head_check(mut self, check: Option<HeadCheck>) -> Self {
        self.head_check = check;
        self
    }

    /// Whether this write created its head.
    pub(crate) fn wrote_head(&self) -> bool {
        self.wrote_head
    }

    /// When the head listing that chose this write's generation started
    /// (the backup's head read, `current_head_for_write`'s fetch). A slow
    /// create is checked against it (04 step 8 D10).
    pub(crate) fn listed_at(mut self, t: Option<MarkerStamp>) -> Self {
        self.listed_at = t;
        self
    }

    fn run(&mut self, cx: &mut StepCx<'_>) -> Result<()> {
        let repo = &mut *cx.repo;
        if self.listed_at.is_none() {
            self.listed_at = Some(MarkerStamp::now(repo.host_env().clock.as_ref()));
        }
        let state = std::mem::replace(&mut self.state, State::Failed);
        self.state = match state {
            State::Start { prev, seg } => self.start(repo, prev, seg)?,
            State::FoldPrefix(prep) => self.fold_prefix(repo, *prep)?,
            State::FoldBody(prep) => self.fold_body(repo, *prep)?,
            State::FoldGeneration(base) => {
                self.listed_at = Some(MarkerStamp::now(repo.host_env().clock.as_ref()));
                let generation = repo.next_head_gen(None)?;
                State::FoldHead(base, generation)
            }
            State::FoldHead(base, generation) => fold_head(repo, *base, generation)?,
            State::Write(mut w) => match w.steps.pop_front() {
                Some(kind) => {
                    write_step(
                        repo,
                        cx.fence,
                        &mut w,
                        kind,
                        self.head_check.as_ref(),
                        self.listed_at.as_ref(),
                    )?;
                    match (kind, w.outcome) {
                        (StepKind::PutHead, Some(HeadPut::Taken)) => self.lost(repo, *w),
                        (StepKind::PutHead, _) => {
                            self.wrote_head = true;
                            State::Write(w)
                        }
                        _ => State::Write(w),
                    }
                }
                None => State::Done(Some(self.finish(repo, *w))),
            },
            State::Done(_) | State::Lost(..) | State::Failed => {
                return Err(VykarError::Other(
                    "BUG: a finished index write was stepped".into(),
                ));
            }
        };
        Ok(())
    }

    fn start(
        &mut self,
        repo: &mut super::Repo,
        prev: Option<IndexHead>,
        seg: Option<SegmentBody>,
    ) -> Result<State> {
        let request = self.request;
        if let (Some(prev), Some(seg)) = (prev.as_ref(), seg.as_ref())
            && seg.is_empty()
            && request == FoldRequest::None
        {
            return self.unchanged(repo, prev.clone());
        }

        let removes = seg.as_ref().is_some_and(|s| !s.removals.is_empty());
        let packed = seg
            .as_ref()
            .filter(|s| !s.is_empty())
            .map(|s| index_store::pack_segment_v2(s, repo.crypto.as_ref()))
            .transpose()?;
        let full = |prev, seg| {
            Ok(State::FoldBody(Box::new(FoldPrep {
                prev,
                seg,
                removes,
                mode: FoldMode::Full,
            })))
        };
        let Some(prev) = prev else {
            return full(None, seg);
        };
        if request == FoldRequest::Full {
            return full(Some(prev), seg);
        }
        // On a plaintext repository the same change from the same state
        // packs to the same bytes, so its name may already be in the head
        // (a state the chain returned to); an object is named only once, so
        // the write folds every segment, young ones too (04 step 7 D7).
        if let Some(obj) = packed.as_ref()
            && prev.named_objects().any(|n| n == obj.name)
        {
            let backups = prev
                .segments
                .iter()
                .filter(|s| s.kind == SegmentKind::Backup)
                .count();
            if backups > 0 && !repo.segment_grace().is_zero() {
                tracing::info!(
                    backup_segments = backups,
                    "an index write repeats an object the head names; folding every segment"
                );
            }
            return full(Some(prev), seg);
        }
        let own = match (seg, packed) {
            (Some(seg), Some(obj)) => Some((seg, obj)),
            _ => None,
        };
        let forced = request == FoldRequest::Eligible;
        // An upper bound of the prefix check: the whole chain, and the own
        // segment when it can be folded. No listing when it is not reached.
        let own_extra = own
            .as_ref()
            .filter(|(seg, _)| own_foldable(repo, seg))
            .map(|(_, obj)| obj.size());
        if !forced
            && !(own.is_some()
                && repo
                    .fold_policy
                    .hard
                    .reached(prev.base.as_ref(), &prev.segments, own_extra))
        {
            return match own {
                Some((seg, obj)) => segment_writes(repo, prev, seg, obj, removes),
                None => self.unchanged(repo, prev),
            };
        }
        let prep = PrefixPrep {
            prev,
            own,
            removes,
            forced,
        };
        let judged = !repo.segment_grace().is_zero()
            && prep
                .prev
                .segments
                .iter()
                .any(|s| s.kind == SegmentKind::Backup);
        if judged {
            return Ok(State::FoldPrefix(Box::new(prep)));
        }
        // Every segment of the chain is foldable.
        let k = prep.prev.segments.len();
        self.decide(repo, prep, k)
    }

    /// The fold-prefix step: the segments' ages (one listing per lock
    /// hold; a failed listing means unknown, so young), the decision, and
    /// the bytes a partial fold needs that the local cache lacks.
    fn fold_prefix(&mut self, repo: &mut super::Repo, prep: PrefixPrep) -> Result<State> {
        if repo.needs_segment_ages(&prep.prev.segments)
            && let Err(e) = repo.load_segment_ages()
        {
            // 04 step 7, fail closed: no age is known, so no backup segment
            // is folded; a write never fails on it.
            warn!("cannot list index.d/ for the segments' ages ({e}); keeping them as segments");
        }
        // A backup segment the head names and the listing lacks: a stale
        // listing (the segment is there; its age is unknown, so young) or a
        // lost object. A garbage collection deletes objects only after its
        // caller created the latest head (04 step 8), so a named one is
        // never collected; one absent is damage, and a write over it would
        // publish a chain no cold client can load: refused. Re-checked with
        // an EXISTS each, only when the listing lacks one.
        if let Some(ages) = repo.segment_ages() {
            let unlisted: Vec<String> = prep
                .prev
                .segments
                .iter()
                .filter(|s| s.kind == SegmentKind::Backup && !ages.listed(&s.name))
                .map(|s| index_store::object_key(&s.name))
                .collect();
            for key in unlisted {
                match repo.storage.exists(&key) {
                    Ok(true) => debug!(
                        key,
                        "the listing of index.d/ lacks a segment the head names; it exists \
                         (a stale listing): kept as a segment"
                    ),
                    Ok(false) => {
                        return Err(VykarError::IndexObjectMissing {
                            kind: "segment",
                            key,
                        });
                    }
                    Err(e) => warn!(
                        key,
                        error = %e,
                        "the listing of index.d/ lacks a segment the head names and it \
                         cannot be checked; kept as a segment"
                    ),
                }
            }
        }
        let k = repo.foldable_prefix(&prep.prev.segments);
        let state = self.decide(repo, prep, k)?;
        let State::FoldBody(mut fold) = state else {
            return Ok(state);
        };
        let fetched = match (&fold.mode, fold.prev.as_ref()) {
            (FoldMode::Partial { k, .. }, Some(prev)) if *k < prev.segments.len() => {
                segment_bytes(repo, &prev.segments, *k)
            }
            _ => return Ok(State::FoldBody(fold)),
        };
        match fetched {
            Ok(b) => {
                if let FoldMode::Partial { bytes, .. } = &mut fold.mode {
                    *bytes = b;
                }
                Ok(State::FoldBody(fold))
            }
            // A backup never fails on a fold it can skip: its segment alone.
            Err(e) if self.backup && !matches!(e, VykarError::IndexObjectMissing { .. }) => {
                let FoldPrep {
                    prev,
                    seg,
                    removes,
                    mode,
                    ..
                } = *fold;
                match (prev, seg, mode) {
                    (
                        Some(prev),
                        Some(seg),
                        FoldMode::Partial {
                            appended: Some(obj),
                            ..
                        },
                    ) => {
                        warn!("cannot read the index segments for a fold ({e}); writing a segment");
                        segment_writes(repo, prev, seg, obj, removes)
                    }
                    _ => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Fold the first `k` (foldable) segments of `prep.prev`, when that
    /// folds something and the fold was asked for or the prefix reaches the
    /// hard limit; otherwise the write's own segment, or nothing.
    fn decide(&mut self, repo: &mut super::Repo, prep: PrefixPrep, k: usize) -> Result<State> {
        let PrefixPrep {
            prev,
            own,
            removes,
            forced,
        } = prep;
        let len = prev.segments.len();
        let own_folds = own.as_ref().is_none_or(|(seg, _)| own_foldable(repo, seg));
        let extra = own
            .as_ref()
            .filter(|_| k == len && own_folds)
            .map(|(_, obj)| obj.size());
        let something = k > 0 || extra.is_some();
        let prefix = prev.segments.get(..k).unwrap_or_default();
        let fold = something
            && (forced
                || repo
                    .fold_policy
                    .hard
                    .reached(prev.base.as_ref(), prefix, extra));
        if !fold {
            if forced {
                debug!("requested fold: no segment old enough; writing a segment");
            }
            return match own {
                Some((seg, obj)) => segment_writes(repo, prev, seg, obj, removes),
                None if self.publish.is_some() => self.unchanged(repo, prev),
                None => Ok(State::Done(Some(prev))),
            };
        }
        let (seg, obj) = match own {
            Some((seg, obj)) => (Some(seg), Some(obj)),
            None => (None, None),
        };
        let mode = if k == len && own_folds {
            FoldMode::Full
        } else {
            FoldMode::Partial {
                k,
                bytes: Vec::new(),
                appended: obj,
            }
        };
        Ok(State::FoldBody(Box::new(FoldPrep {
            prev: Some(prev),
            seg,
            removes,
            mode,
        })))
    }

    /// An empty change: nothing to write, or a head only when a sweep is
    /// pending or the head is forced (the same state, a new generation and
    /// the sweep).
    fn unchanged(&mut self, repo: &mut super::Repo, prev: IndexHead) -> Result<State> {
        if repo.pending_sweep.is_none() && !self.force_head {
            debug!("index unchanged, nothing to write");
            return Ok(State::Done(Some(self.bookkeeping(repo, prev, true))));
        }
        let mut head = prev.successor();
        let (sweep, record) = repo.next_sweep(Some(&prev), head.generation, false)?;
        head.sweep = sweep;
        head.validate()?;
        let mut writes = Writes::new(head, None, None, record, None, Finish::Unchanged);
        if self.force_head {
            writes.steps.push_back(StepKind::CollectGarbage);
        }
        Ok(State::Write(Box::new(writes)))
    }

    /// Fold: the base body written into a local table. A full fold first
    /// adds a backup's additions to the index as edits and writes the
    /// index's state; a partial fold writes the index's loaded state (the
    /// whole chain) or the table plus the foldable prefix, and leaves the
    /// index untouched until the finish.
    fn fold_body(&mut self, repo: &mut super::Repo, prep: FoldPrep) -> Result<State> {
        let FoldPrep {
            prev,
            seg,
            removes,
            mode,
        } = prep;
        let dir = repo.cache_dir();
        let anchor = index_store::cache_anchor(dir.as_deref());
        let writer = TableFileWriter::new_beside(&anchor)?;
        let (w, entries, kept, appended, folded) = match mode {
            FoldMode::Full => {
                if self.backup
                    && let Some(seg) = seg.as_ref()
                {
                    self.added = true;
                    for (id, loc) in &seg.additions {
                        repo.chunk_index
                            .add(*id, loc.stored_size, loc.pack_id, loc.pack_offset);
                    }
                    for (pack, trailer_len) in &seg.pack_trailers {
                        repo.chunk_index.set_pack_trailer(*pack, *trailer_len);
                    }
                }
                let (w, entries) = repo.chunk_index.write_base_body(writer)?;
                // A backup keeps its segment until the head is created: a
                // taken generation hands it back for the retry.
                let folded = if self.backup { seg } else { None };
                (w, entries, Vec::new(), None, folded)
            }
            FoldMode::Partial {
                k,
                mut bytes,
                appended,
            } => {
                let prev = prev.as_ref().ok_or_else(|| {
                    VykarError::Other("BUG: a partial fold without a previous head".into())
                })?;
                if *repo.chunk_index.state() != IndexState::of_head(prev) {
                    return Err(VykarError::Other(
                        "BUG: a partial fold over an index not at its head".into(),
                    ));
                }
                let (w, entries) = if k == prev.segments.len() {
                    repo.chunk_index.write_base_body(writer)?
                } else {
                    let table = repo.chunk_index.table().ok_or_else(|| {
                        VykarError::Other("BUG: a partial fold over an unloaded index".into())
                    })?;
                    let mut prefix = LiveIndex::from_table(Arc::clone(table));
                    for (r, b) in prev.segments.iter().zip(bytes.iter_mut()).take(k) {
                        // A fetched segment's bytes go as soon as it is
                        // decoded.
                        let fetched = b.take();
                        let body = segment_body(repo, dir.as_deref(), r, fetched.as_deref())?;
                        drop(fetched);
                        prefix.apply_segment(r.name, &body)?;
                    }
                    prefix.write_base_body(writer)?
                };
                let kept = prev
                    .segments
                    .iter()
                    .cloned()
                    .zip(bytes.into_iter().chain(std::iter::repeat_with(|| None)))
                    .skip(k)
                    .collect();
                let appended = appended.zip(seg);
                (w, entries, kept, appended, None)
            }
        };
        let table = ChunkTable::install(w.finish()?, None, None)?;
        let obj = (entries > 0)
            .then(|| index_store::pack_base_body(table.body(), repo.crypto.as_ref()))
            .transpose()?;
        let next = prev.as_ref().map(|p| p.generation + 1);
        let base = Box::new(FoldBase {
            prev,
            removes,
            dir,
            table,
            obj,
            entries,
            kept,
            appended,
            folded,
        });
        Ok(match next {
            Some(generation) => State::FoldHead(base, generation),
            None => State::FoldGeneration(base),
        })
    }

    /// The last step: the repository's index and, for a publish, its head
    /// bookkeeping. Nothing here fails the write: the head is published. (A
    /// kept segment whose cached copy does not verify is fetched again; if
    /// that fails the index is emptied with a warning and reloads.)
    fn finish(&mut self, repo: &mut super::Repo, w: Writes) -> IndexHead {
        let Writes { head, finish, .. } = w;
        match finish {
            Finish::Unchanged => {
                debug!(
                    generation = head.generation,
                    "index head written (no change to the entries)"
                );
                return self.bookkeeping(repo, head, true);
            }
            Finish::Segment { name, seg } => {
                if self.backup {
                    if let Err(e) = repo.chunk_index.apply_segment(name, &seg) {
                        // The head is published; the next load syncs to it.
                        warn!("cannot apply the index segment just written: {e}");
                        repo.chunk_index = LiveIndex::empty();
                    }
                } else {
                    repo.chunk_index.mark_written(name);
                }
            }
            Finish::Fold(fold) => {
                let FoldFinish {
                    dir,
                    table,
                    name,
                    bytes,
                    gc,
                    kept,
                    appended,
                    ..
                } = *fold;
                debug!(
                    bytes,
                    generation = head.generation,
                    kept = kept.len(),
                    heads_deleted = gc.heads_deleted,
                    objects_deleted = gc.objects_deleted,
                    "index folded into a new base"
                );
                // The head is published: the new state becomes the
                // repository's table. Replace the index before pruning, so
                // the table it mapped until now is unmapped and can be
                // deleted once it is old enough.
                let table = match name {
                    Some(name) => table.publish(name, dir.as_deref()),
                    None => ChunkTable::empty(),
                };
                repo.chunk_index = LiveIndex::from_table(Arc::new(table));
                // The segments the head keeps after the base, then the
                // write's own: one decoded at a time.
                let rebuilt = (|| -> Result<()> {
                    for (r, b) in &kept {
                        let body = segment_body(repo, dir.as_deref(), r, b.as_deref())?;
                        repo.chunk_index.apply_segment(r.name, &body)?;
                    }
                    if let Some((name, seg)) = &appended {
                        repo.chunk_index.apply_segment(*name, seg)?;
                    }
                    Ok(())
                })();
                if let Err(e) = rebuilt {
                    // The head is published; the next load syncs to it.
                    warn!("cannot rebuild the index after a fold: {e}");
                    repo.chunk_index = LiveIndex::empty();
                }
                index_store::prune_local_files(dir.as_deref(), &head);
            }
        }
        if self.publish.is_some() {
            debug!(
                elapsed_ms = self.start.elapsed().as_millis() as u64,
                generation = head.generation,
                segments = head.segments.len(),
                entries = repo.chunk_index.len(),
                "index written"
            );
        }
        self.bookkeeping(repo, head, false)
    }

    /// A publish's head bookkeeping (and, for an unchanged state, dropping
    /// the edits); a base write alone leaves both to its caller.
    fn bookkeeping(&self, repo: &mut super::Repo, head: IndexHead, unchanged: bool) -> IndexHead {
        if self.publish.is_some() {
            if unchanged {
                repo.chunk_index.discard_edits();
            }
            repo.index_generation = head.generation;
            repo.index_head = Some(head.clone());
            repo.index_dirty = false;
        }
        head
    }

    /// The head create found the generation taken: undo as after an error
    /// and keep a backup's own segment for its retry.
    fn lost(&mut self, repo: &mut super::Repo, w: Writes) -> State {
        let generation = w.head.generation;
        self.on_error(repo);
        tracing::info!(
            generation,
            "index head {generation} was created by another writer"
        );
        let seg = match w.finish {
            _ if !self.backup => None,
            Finish::Segment { seg, .. } => Some(seg),
            Finish::Fold(fold) => {
                let FoldFinish {
                    appended, folded, ..
                } = *fold;
                appended.map(|(_, seg)| seg).or(folded)
            }
            Finish::Unchanged => None,
        };
        State::Lost(generation, seg)
    }

    /// Undo what an error leaves behind: a fold's backup additions, and a
    /// publish's fold request.
    fn on_error(&mut self, repo: &mut super::Repo) {
        if self.added {
            // The index is replaced only after the head PUT; the backup's
            // additions were its only edits.
            repo.chunk_index.discard_edits();
        }
        if let Some(request) = self.publish {
            repo.fold_requested = repo.fold_requested.max(request);
        }
    }
}

impl Writes {
    /// The storage steps of a write, in order: base, record, segment,
    /// fence, head, and (a fold) the garbage collection.
    fn new(
        head: IndexHead,
        packed_head: Option<Vec<u8>>,
        base: Option<PackedObject>,
        record: Option<PackedObject>,
        segment: Option<PackedObject>,
        finish: Finish,
    ) -> Self {
        let mut steps = VecDeque::new();
        if base.is_some() {
            steps.push_back(StepKind::PutBase);
        }
        if record.is_some() {
            steps.push_back(StepKind::PutSweepRecord);
        }
        if segment.is_some() {
            steps.push_back(StepKind::PutSegment);
        }
        steps.push_back(StepKind::Fence);
        steps.push_back(StepKind::PutHead);
        if matches!(finish, Finish::Fold(_)) {
            steps.push_back(StepKind::CollectGarbage);
        }
        Self {
            steps,
            head,
            packed_head,
            base,
            record,
            segment,
            segment_written: None,
            objects_from: None,
            outcome: None,
            finish,
        }
    }
}

/// The segment path: `seg` as one more segment after `prev`'s chain.
fn segment_writes(
    repo: &mut super::Repo,
    prev: IndexHead,
    seg: SegmentBody,
    obj: PackedObject,
    removes: bool,
) -> Result<State> {
    let mut head = prev.successor();
    head.segments.push(SegmentRef {
        name: obj.name,
        size: obj.size(),
        kind: seg.kind,
        entries: seg.entries(),
    });
    let (sweep, record) = repo.next_sweep(Some(&prev), head.generation, removes)?;
    head.sweep = sweep;
    head.validate()?;
    let packed_head = index_store::pack_head(&head, repo.crypto.as_ref())?;
    let finish = Finish::Segment {
        name: obj.name,
        seg,
    };
    Ok(State::Write(Box::new(Writes::new(
        head,
        Some(packed_head),
        None,
        record,
        Some(obj),
        finish,
    ))))
}

/// Whether the write's own segment can go into a base: a maintenance
/// segment, or any at zero grace (a backup's own segment is age 0).
fn own_foldable(repo: &super::Repo, seg: &SegmentBody) -> bool {
    seg.kind == SegmentKind::Maintenance || repo.segment_grace().is_zero()
}

/// A partial fold's bytes of `segments` (all of them: the first `k` are
/// decoded into the base, the rest re-applied after the head PUT): `None`
/// where the local cache holds a file of the segment's size (checked by
/// metadata, not read: [`segment_body`] reads and verifies it once, when
/// it is decoded); the others fetched in one batch. A fetched kept segment
/// goes into the cache when there is one, and is held only without one.
/// Under the lock a named object is never deleted, so an absent one is
/// damage.
fn segment_bytes(
    repo: &super::Repo,
    segments: &[SegmentRef],
    k: usize,
) -> Result<Vec<Option<Vec<u8>>>> {
    let dir = repo.cache_dir();
    let cached: Vec<bool> = segments
        .iter()
        .map(|r| {
            dir.as_deref()
                .is_some_and(|d| table::cached_segment_present(d, r))
        })
        .collect();
    let wanted: Vec<usize> = (0..segments.len())
        .filter(|&i| !cached.get(i).copied().unwrap_or(false))
        .collect();
    let mut out: Vec<Option<Vec<u8>>> = vec![None; segments.len()];
    if wanted.is_empty() {
        return Ok(out);
    }
    let keys: Vec<String> = wanted
        .iter()
        .filter_map(|&i| segments.get(i))
        .map(|r| index_store::object_key(&r.name))
        .collect();
    match index_store::fetch_all(repo.storage.as_ref(), &keys, repo.request_width())? {
        index_store::Fetched::All(fetched) => {
            for (&i, b) in wanted.iter().zip(fetched) {
                match (dir.as_deref(), segments.get(i)) {
                    (Some(d), Some(r)) if i >= k => table::write_cached_segment(d, &r.name, &b),
                    _ => {
                        if let Some(slot) = out.get_mut(i) {
                            *slot = Some(b);
                        }
                    }
                }
            }
            Ok(out)
        }
        index_store::Fetched::Absent(j) => {
            let key = keys.get(j).cloned().unwrap_or_default();
            Err(VykarError::IndexObjectMissing {
                kind: "segment",
                key,
            })
        }
    }
}

/// The decoded body of segment `r`: from `bytes`, else the local cache's
/// verified copy, else (a cached file that [`segment_bytes`] saw but that
/// does not verify, or went since) one GET from the store.
fn segment_body(
    repo: &super::Repo,
    dir: Option<&std::path::Path>,
    r: &SegmentRef,
    bytes: Option<&[u8]>,
) -> Result<SegmentBody> {
    let held;
    let bytes = match bytes {
        Some(b) => b,
        None => {
            held = match dir.and_then(|d| table::read_cached_segment(d, r)) {
                Some(b) => b,
                None => {
                    let key = index_store::object_key(&r.name);
                    repo.storage
                        .get(&key)?
                        .ok_or(VykarError::IndexObjectMissing {
                            kind: "segment",
                            key,
                        })?
                }
            };
            &held
        }
    };
    index_store::decode_segment_v2(r, bytes, repo.crypto.as_ref())
}

/// Fold: the sweep, the head naming the base, the kept segments and the
/// appended one, with `prev`'s other live objects as retired, and its
/// bytes.
fn fold_head(repo: &mut super::Repo, base: FoldBase, generation: u64) -> Result<State> {
    let FoldBase {
        prev,
        removes,
        dir,
        table,
        obj,
        entries,
        kept,
        appended,
        folded,
    } = base;
    let mut segments: Vec<SegmentRef> = kept.iter().map(|(r, _)| r.clone()).collect();
    let (segment, appended) = match appended {
        Some((seg_obj, seg)) => {
            segments.push(SegmentRef {
                name: seg_obj.name,
                size: seg_obj.size(),
                kind: seg.kind,
                entries: seg.entries(),
            });
            let name = seg_obj.name;
            (Some(seg_obj), Some((name, seg)))
        }
        None => (None, None),
    };
    let kept_names: Vec<ObjectName> = segments.iter().map(|s| s.name).collect();
    let (sweep, record) = repo.next_sweep(prev.as_ref(), generation, removes)?;
    let head = IndexHead {
        layout: HEAD_LAYOUT_VERSION,
        generation,
        features: Vec::new(),
        base: obj.as_ref().map(|obj| BaseRef {
            name: obj.name,
            size: obj.size(),
            entries,
        }),
        segments,
        // The same state written again (plaintext) has the same name; the
        // kept segments stay live.
        retired: prev
            .as_ref()
            .map(|p| {
                p.live_objects()
                    .filter(|n| obj.as_ref().is_none_or(|o| *n != o.name))
                    .filter(|n| !kept_names.contains(n))
                    .collect()
            })
            .unwrap_or_default(),
        sweep,
    };
    head.validate()?;
    let packed_head = index_store::pack_head(&head, repo.crypto.as_ref())?;
    let finish = Finish::Fold(Box::new(FoldFinish {
        dir,
        table,
        name: obj.as_ref().map(|o| o.name),
        bytes: obj.as_ref().map_or(0, PackedObject::size),
        gc: index_store::IndexGcStats::default(),
        kept,
        appended,
        folded,
    }));
    Ok(State::Write(Box::new(Writes::new(
        head,
        Some(packed_head),
        obj,
        record,
        segment,
        finish,
    ))))
}

/// After a create that returned more than [`HEAD_FRESHNESS_LIMIT`] after the
/// listing that chose `generation` (04 step 8 D10): one listing of
/// `index.h/`. A generation more than `HEADS_KEPT` below the latest is one
/// another writer created after that listing and the garbage collection
/// deleted before this create re-made it: readers take the latest, so the
/// write counts as taken. The re-made head is deleted again, best effort,
/// and only when the key still holds `packed`, the bytes this write created
/// (a GET and a comparison first): other bytes, or a failed GET, mean the
/// key may be another writer's, which is left alone. No reader takes a head
/// that far below the latest, and no writer builds on it.
fn fresh_generation(
    storage: &dyn vykar_storage::StorageBackend,
    generation: u64,
    packed: &[u8],
) -> Result<HeadPut> {
    let latest = index_store::list_head_generations(storage)?
        .last()
        .copied()
        .unwrap_or(generation);
    if generation + index_store::HEADS_KEPT >= latest {
        return Ok(HeadPut::Created);
    }
    let key = index_store::head_key(generation);
    match storage.get(&key) {
        Ok(Some(stored)) if stored == packed => {
            if let Err(e) = storage.delete(&key) {
                debug!(key, error = %e, "could not delete a re-made old index head");
            }
        }
        Ok(_) => debug!(
            key,
            "a re-made old index head holds other bytes now (or is gone); left alone"
        ),
        Err(e) => debug!(
            key,
            error = %e,
            "cannot read a re-made old index head; left alone"
        ),
    }
    Ok(HeadPut::Taken)
}

/// An index write that took longer than `limit` from its first object
/// upload (`index_store::INDEX_OBJECT_GRACE`).
fn slow_index_write(limit: std::time::Duration) -> VykarError {
    let mins = limit.as_secs() / 60;
    VykarError::Other(format!(
        "{SLOW_INDEX_WRITE_TEXT} {} h {} min since its first object upload; retry",
        mins / 60,
        mins % 60
    ))
}

/// The error of a head create that returned more than `limit` after the
/// write's first object upload: the refusal's text (the simulator matches
/// its start), then what happened.
fn slow_index_write_after_create(limit: std::time::Duration) -> VykarError {
    VykarError::Other(format!(
        "{}. The index head was written without its snapshot; the next run continues \
         from it. Run `vykar check` if the repository fails to open",
        slow_index_write(limit)
    ))
}

/// One storage step of a write. `listed_at`: see
/// [`PublishMachine::listed_at`].
fn write_step(
    repo: &mut super::Repo,
    fence: &super::writer::Fence,
    w: &mut Writes,
    kind: StepKind,
    head_check: Option<&HeadCheck>,
    listed_at: Option<&MarkerStamp>,
) -> Result<()> {
    let missing = || {
        VykarError::Other(format!(
            "BUG: index write step {kind:?} has nothing to write"
        ))
    };
    let stamp = |repo: &super::Repo| MarkerStamp::now(repo.host_env().clock.as_ref());
    match kind {
        StepKind::PutBase => {
            let obj = w.base.take().ok_or_else(missing)?;
            w.objects_from.get_or_insert(stamp(repo));
            repo.storage.put(&obj.key(), &obj.bytes)?;
            Ok(())
        }
        StepKind::PutSweepRecord => {
            let rec = w.record.take().ok_or_else(missing)?;
            w.objects_from.get_or_insert(stamp(repo));
            repo.storage.put(&rec.key(), &rec.bytes)?;
            Ok(())
        }
        StepKind::PutSegment => {
            let obj = w.segment.take().ok_or_else(missing)?;
            w.objects_from.get_or_insert(stamp(repo));
            repo.storage.put(&obj.key(), &obj.bytes)?;
            if let Some(dir) = repo.cache_dir() {
                table::write_cached_segment(&dir, &obj.name, &obj.bytes);
            }
            w.segment_written = Some(obj.size());
            Ok(())
        }
        StepKind::Fence => {
            // The put-if-absent check (once per backend instance) runs
            // before the fence, so the fence and the pack-age term stay the
            // last checks before the head create. Undecided fails the write
            // like a failed PUT; `Unsupported`, or a backend that offers no
            // put-if-absent, falls back to a plain PUT, logged once per
            // handle (never silently unconditional).
            // A lock-free writer (04 step 9) holds no lock: it fails
            // instead (O4).
            let lock_free = repo.is_lock_free();
            match repo.storage.conditional() {
                Some(cond) => {
                    if let vykar_storage::ConditionalSupport::Unsupported { reason } =
                        cond.verify()?
                    {
                        if lock_free {
                            return Err(index_store::lock_free_without_condition(Some(&reason)));
                        }
                        index_store::note_fallback(&mut repo.head_fallback, Some(&reason));
                    }
                }
                None if lock_free => {
                    return Err(index_store::lock_free_without_condition(None));
                }
                None => {
                    index_store::note_fallback(&mut repo.head_fallback, None);
                }
            }
            fence.check()?;
            match head_check {
                Some(check) => check.run(repo),
                None => Ok(()),
            }
        }
        StepKind::PutHead => {
            let generation = w.head.generation;
            // Refused before the create when the write's objects may no
            // longer outlive the landing of its head (`INDEX_OBJECT_GRACE`):
            // the error cleanup runs and the caller retries with fresh
            // objects.
            let early = INDEX_WRITE_LIMIT.saturating_sub(HEAD_LANDING_ALLOWANCE);
            if let Some(from) = w.objects_from.as_ref()
                && from.until(&stamp(repo)) > early
            {
                return Err(slow_index_write(early));
            }
            let packed = match w.packed_head.take() {
                Some(packed) => packed,
                None => index_store::pack_head(&w.head, repo.crypto.as_ref())?,
            };
            let lock_free = repo.is_lock_free();
            let created = index_store::create_head(
                repo.storage.as_ref(),
                generation,
                &packed,
                &mut repo.head_fallback,
                lock_free,
            );
            let mut outcome = match created {
                Ok(outcome) => outcome,
                Err(e) => {
                    w.packed_head = Some(packed);
                    return Err(e);
                }
            };
            let now = stamp(repo);
            if outcome == HeadPut::Created
                && listed_at.is_some_and(|t| t.until(&now) > HEAD_FRESHNESS_LIMIT)
            {
                outcome = fresh_generation(repo.storage.as_ref(), generation, &packed)?;
            }
            w.packed_head = Some(packed);
            w.outcome = Some(outcome);
            if outcome == HeadPut::Taken {
                return Ok(());
            }
            repo.created_head = Some(generation);
            if let Some(from) = w.objects_from.as_ref()
                && from.until(&now) > INDEX_WRITE_LIMIT
            {
                // The head is created, but a collector may have judged its
                // objects old before it landed: no snapshot names it (`check`
                // names a collected object).
                tracing::error!(
                    generation,
                    elapsed_secs = from.until(&now).as_secs(),
                    "index head created after its objects' grace; the write fails"
                );
                return Err(slow_index_write_after_create(INDEX_WRITE_LIMIT));
            }
            if let Some(bytes) = w.segment_written {
                debug!(bytes, generation, "index segment written");
            }
            Ok(())
        }
        StepKind::CollectGarbage => {
            // A lock-free maintenance run's marker check (04 step 9) guards
            // its deletes: when it fails the collection is skipped (the
            // head has landed; the run's next check ends it). A lock-free
            // backup's fence passes; under the lock nothing changes.
            if repo.is_lock_free()
                && let Err(e) = fence.check()
            {
                warn!("index garbage collection skipped: {e}");
                return Ok(());
            }
            // A fold collects heads and objects; a forced head only heads
            // (`PublishMachine::force_head`).
            let gc = match &w.finish {
                Finish::Fold(_) => index_store::collect_index_garbage(
                    repo.storage.as_ref(),
                    repo.crypto.as_ref(),
                    &w.head,
                    repo.index_gc_grace,
                ),
                _ => index_store::collect_old_heads(
                    repo.storage.as_ref(),
                    &w.head,
                    repo.index_gc_grace.heads,
                ),
            }
            .unwrap_or_else(|e| {
                warn!("failed to delete old index objects: {e}");
                Default::default()
            });
            if gc.heads_deleted > 0 {
                debug!(heads_deleted = gc.heads_deleted, "old index heads deleted");
            }
            if let Finish::Fold(fold) = &mut w.finish {
                fold.gc = gc;
            }
            Ok(())
        }
        other => Err(VykarError::Other(format!(
            "BUG: {other:?} is not a step of an index write"
        ))),
    }
}

impl Machine for PublishMachine {
    type Output = Published;

    fn enabled(&self) -> Vec<StepKind> {
        match &self.state {
            State::FoldGeneration(_) => vec![StepKind::ListHeads],
            State::Write(w) => vec![w.steps.front().copied().unwrap_or(StepKind::Local)],
            State::FoldPrefix(_) => vec![StepKind::FoldPrefix],
            State::Start { .. } | State::FoldBody(_) | State::FoldHead(..) => {
                vec![StepKind::Local]
            }
            State::Done(_) | State::Lost(..) | State::Failed => Vec::new(),
        }
    }

    fn step(&mut self, cx: &mut StepCx<'_>, pick: usize) -> Result<()> {
        let result = if pick == 0 && !self.enabled().is_empty() {
            self.run(cx)
        } else {
            Err(bad_pick("the index write", pick, &self.enabled()))
        };
        if result.is_err() {
            self.state = State::Failed;
            self.on_error(cx.repo);
        }
        result
    }

    fn take_output(&mut self) -> Option<Published> {
        match std::mem::replace(&mut self.state, State::Failed) {
            State::Done(Some(head)) => Some(Published::Head(head)),
            State::Lost(generation, seg) => Some(Published::Taken { generation, seg }),
            other => {
                self.state = other;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// D10's clean-up of a re-made old head compares bytes first: the key
    /// holding this write's bytes is deleted, one holding other bytes (another
    /// writer's) is left alone; both count as taken. A generation within
    /// `HEADS_KEPT` of the latest is created and nothing is read.
    #[test]
    fn a_remade_old_head_is_deleted_only_when_it_is_ours() {
        use vykar_storage::StorageBackend;
        let mine: &[u8] = b"head bytes of this write";
        let latest = 2 + index_store::HEADS_KEPT + 1;
        for (stored, deleted) in [(mine, true), (&b"another writer's head"[..], false)] {
            let s = crate::testutil::MemoryBackend::new();
            s.put(&index_store::head_key(2), stored).unwrap();
            s.put(&index_store::head_key(latest), b"latest").unwrap();
            assert_eq!(fresh_generation(&s, 2, mine).unwrap(), HeadPut::Taken);
            assert_eq!(
                s.get(&index_store::head_key(2)).unwrap().is_none(),
                deleted,
                "stored {stored:?}"
            );
        }
        let s = crate::testutil::MemoryBackend::new();
        s.put(&index_store::head_key(3), b"other").unwrap();
        s.put(
            &index_store::head_key(2 + index_store::HEADS_KEPT),
            b"latest",
        )
        .unwrap();
        assert_eq!(fresh_generation(&s, 3, mine).unwrap(), HeadPut::Created);
        assert!(s.get(&index_store::head_key(3)).unwrap().is_some());
    }
}
