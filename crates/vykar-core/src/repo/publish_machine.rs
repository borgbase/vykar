//! The index write as a steppable machine: [`PublishMachine`], the shared
//! head-publish path of every index write (backup commits, maintenance,
//! repair, upgrade). The write sequences are those of `index_write.rs`'s
//! module doc, one storage PUT per step:
//!
//! - head only (a sweep pending, nothing else to write): [record] → fence →
//!   head;
//! - segment: [record] → segment → fence → head;
//! - fold: [list heads] → [base] → [record] → fence → head → garbage
//!   collection.
//!
//! A backup's write also runs the commit rule's pack-age term at its fence
//! ([`HeadCheck`]), the last step before the head PUT on every path.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::{debug, warn};

use super::index_store::{self, PackedObject};
use super::lock::{self, SessionMarker};
use super::machine::{Machine, StepCx, StepKind, bad_pick};
use crate::index::LiveIndex;
use crate::index::head::{BaseRef, HEAD_LAYOUT_VERSION, IndexHead, ObjectName, SegmentRef};
use crate::index::segment_v2::SegmentBody;
use crate::index::table::{self, ChunkTable, TableFileWriter};
use vykar_types::error::{Result, VykarError};

/// One index write, from the state to publish to the head that names it.
/// Built by [`PublishMachine::publish`] (what
/// `Repository::publish_index_state` does) or [`PublishMachine::base`]
/// (`Repository::put_base`); its output is the head written, or the
/// previous head when nothing was.
pub(crate) struct PublishMachine {
    /// `Some(fold)`: a publish, which took `fold` from the repository's
    /// `fold_requested` and ORs it back on error, and which updates the
    /// repository's head bookkeeping. `None`: a base write alone.
    publish: Option<bool>,
    backup: bool,
    /// A fold added a backup's additions to the index as edits: an error
    /// before the head PUT drops them again.
    added: bool,
    /// A backup's pack-age term, run at the fence before the head PUT.
    head_check: Option<HeadCheck>,
    /// The head PUT returned.
    wrote_head: bool,
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
    fn run(&self, repo: &super::Repository) -> Result<()> {
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
    /// Fold: write the base body (`Local`).
    FoldBody(Box<FoldPrep>),
    /// Fold without a previous head: LIST `index.h/` for the generation.
    FoldGeneration(Box<FoldBase>),
    /// Fold: the sweep and the head (`Local`).
    FoldHead(Box<FoldBase>, u64),
    /// The PUTs, fence and garbage collection, then the finish.
    Write(Box<Writes>),
    Done(Option<IndexHead>),
    Failed,
}

struct FoldPrep {
    prev: Option<IndexHead>,
    seg: Option<SegmentBody>,
    removes: bool,
}

/// A fold's base, written to a local table, not yet published.
struct FoldBase {
    prev: Option<IndexHead>,
    removes: bool,
    dir: Option<PathBuf>,
    table: ChunkTable,
    obj: Option<PackedObject>,
    entries: u64,
}

/// The rest of a write: storage steps in order, then the finish.
struct Writes {
    steps: VecDeque<StepKind>,
    head: IndexHead,
    /// The head's bytes; a head-only write packs them at its PUT.
    packed_head: Option<Vec<u8>>,
    base: Option<PackedObject>,
    record: Option<PackedObject>,
    segment: Option<PackedObject>,
    /// The size of the segment PUT, logged once the head is written.
    segment_written: Option<u64>,
    finish: Finish,
}

enum Finish {
    /// Head only: drop the edits (they changed nothing).
    Unchanged,
    /// Apply the segment (backup) or mark the edits written.
    Segment { name: ObjectName, seg: SegmentBody },
    /// The base becomes the repository's table.
    Fold {
        dir: Option<PathBuf>,
        table: ChunkTable,
        name: Option<ObjectName>,
        bytes: u64,
        gc: index_store::IndexGcStats,
    },
}

impl PublishMachine {
    /// Publish `seg` as the successor of `prev`: as a segment, or as a
    /// base (a fold) when there is no `prev`, the hard limit is reached, a
    /// fold was requested or the packed segment's name is already named by
    /// `prev` (plaintext repeat). Takes `repo.fold_requested`. See
    /// `Repository::publish_index_state` for `backup` and the errors.
    pub(crate) fn publish(
        repo: &mut super::Repository,
        prev: Option<IndexHead>,
        seg: Option<SegmentBody>,
        backup: bool,
    ) -> Self {
        let fold = std::mem::take(&mut repo.fold_requested);
        Self {
            publish: Some(fold),
            backup,
            added: false,
            head_check: None,
            wrote_head: false,
            start: std::time::Instant::now(),
            state: State::Start { prev, seg },
        }
    }

    /// A fold of the index's current state (`Repository::put_base`).
    pub(crate) fn base(prev: Option<IndexHead>, removes: bool) -> Self {
        Self {
            publish: None,
            backup: false,
            added: false,
            head_check: None,
            wrote_head: false,
            start: std::time::Instant::now(),
            state: State::FoldBody(Box::new(FoldPrep {
                prev,
                seg: None,
                removes,
            })),
        }
    }

    /// Apply `check` at the fence before the head PUT (backups only).
    pub(crate) fn with_head_check(mut self, check: Option<HeadCheck>) -> Self {
        self.head_check = check;
        self
    }

    /// Whether this write's head PUT returned.
    pub(crate) fn wrote_head(&self) -> bool {
        self.wrote_head
    }

    fn fold(&self) -> bool {
        self.publish.unwrap_or(false)
    }

    fn run(&mut self, cx: &mut StepCx<'_>) -> Result<()> {
        let repo = &mut *cx.repo;
        let state = std::mem::replace(&mut self.state, State::Failed);
        self.state = match state {
            State::Start { prev, seg } => self.start(repo, prev, seg)?,
            State::FoldBody(prep) => self.fold_body(repo, *prep)?,
            State::FoldGeneration(base) => {
                let generation = repo.next_head_gen(None)?;
                State::FoldHead(base, generation)
            }
            State::FoldHead(base, generation) => fold_head(repo, *base, generation)?,
            State::Write(mut w) => match w.steps.pop_front() {
                Some(kind) => {
                    write_step(repo, &mut w, kind, self.head_check.as_ref())?;
                    if kind == StepKind::PutHead {
                        self.wrote_head = true;
                    }
                    State::Write(w)
                }
                None => State::Done(Some(self.finish(repo, *w))),
            },
            State::Done(_) | State::Failed => {
                return Err(VykarError::Other(
                    "BUG: a finished index write was stepped".into(),
                ));
            }
        };
        Ok(())
    }

    fn start(
        &mut self,
        repo: &mut super::Repository,
        prev: Option<IndexHead>,
        seg: Option<SegmentBody>,
    ) -> Result<State> {
        let fold = self.fold();
        if let (Some(prev), Some(seg)) = (prev.as_ref(), seg.as_ref())
            && seg.is_empty()
            && !fold
        {
            if repo.pending_sweep.is_none() {
                debug!("index unchanged, nothing to write");
                return Ok(State::Done(Some(self.bookkeeping(
                    repo,
                    prev.clone(),
                    true,
                ))));
            }
            // A head-only write: the same state, a new generation and the
            // pending sweep.
            let mut head = prev.successor();
            let (sweep, record) = repo.next_sweep(Some(prev), head.generation, false)?;
            head.sweep = sweep;
            head.validate()?;
            return Ok(State::Write(Box::new(Writes::new(
                head,
                None,
                None,
                record,
                None,
                Finish::Unchanged,
            ))));
        }

        let removes = seg.as_ref().is_some_and(|s| !s.removals.is_empty());
        let packed = seg
            .as_ref()
            .filter(|s| !s.is_empty())
            .map(|s| index_store::pack_segment_v2(s, repo.crypto.as_ref()))
            .transpose()?;
        // On a plaintext repository the same change from the same state
        // packs to the same bytes, so its name may already be in the head
        // (a state the chain returned to); an object is named only once.
        match (prev, seg, packed) {
            (Some(prev), Some(seg), Some(obj))
                if !fold
                    && !repo.fold_policy.hard.reached(&prev, 1, obj.size())
                    && !prev.named_objects().any(|n| n == obj.name) =>
            {
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
            (prev, seg, _) => Ok(State::FoldBody(Box::new(FoldPrep { prev, seg, removes }))),
        }
    }

    /// Fold: a backup's additions become edits of the index, whose state is
    /// written as the base body into a local table.
    fn fold_body(&mut self, repo: &mut super::Repository, prep: FoldPrep) -> Result<State> {
        let FoldPrep { prev, seg, removes } = prep;
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
        let dir = repo.cache_dir();
        let anchor = index_store::cache_anchor(dir.as_deref());
        let (w, entries) = repo
            .chunk_index
            .write_base_body(TableFileWriter::new_beside(&anchor)?)?;
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
        });
        Ok(match next {
            Some(generation) => State::FoldHead(base, generation),
            None => State::FoldGeneration(base),
        })
    }

    /// The last step: the repository's index and, for a publish, its head
    /// bookkeeping. Nothing here fails: the head is published.
    fn finish(&mut self, repo: &mut super::Repository, w: Writes) -> IndexHead {
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
            Finish::Fold {
                dir,
                table,
                name,
                bytes,
                gc,
            } => {
                debug!(
                    bytes,
                    generation = head.generation,
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
    fn bookkeeping(
        &self,
        repo: &mut super::Repository,
        head: IndexHead,
        unchanged: bool,
    ) -> IndexHead {
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

    /// Undo what an error leaves behind: a fold's backup additions, and a
    /// publish's fold request.
    fn on_error(&mut self, repo: &mut super::Repository) {
        if self.added {
            // The index is replaced only after the head PUT; the backup's
            // additions were its only edits.
            repo.chunk_index.discard_edits();
        }
        if let Some(fold) = self.publish {
            repo.fold_requested |= fold;
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
        if matches!(finish, Finish::Fold { .. }) {
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
            finish,
        }
    }
}

/// Fold: the sweep, the head naming the base alone with `prev`'s live
/// objects as retired, and its bytes.
fn fold_head(repo: &mut super::Repository, base: FoldBase, generation: u64) -> Result<State> {
    let FoldBase {
        prev,
        removes,
        dir,
        table,
        obj,
        entries,
    } = base;
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
        segments: Vec::new(),
        // The same state written again (plaintext) has the same name.
        retired: prev
            .as_ref()
            .map(|p| {
                p.live_objects()
                    .filter(|n| obj.as_ref().is_none_or(|o| *n != o.name))
                    .collect()
            })
            .unwrap_or_default(),
        sweep,
    };
    head.validate()?;
    let packed_head = index_store::pack_head(&head, repo.crypto.as_ref())?;
    let finish = Finish::Fold {
        dir,
        table,
        name: obj.as_ref().map(|o| o.name),
        bytes: obj.as_ref().map_or(0, PackedObject::size),
        gc: index_store::IndexGcStats::default(),
    };
    Ok(State::Write(Box::new(Writes::new(
        head,
        Some(packed_head),
        obj,
        record,
        None,
        finish,
    ))))
}

/// One storage step of a write.
fn write_step(
    repo: &mut super::Repository,
    w: &mut Writes,
    kind: StepKind,
    head_check: Option<&HeadCheck>,
) -> Result<()> {
    let missing = || {
        VykarError::Other(format!(
            "BUG: index write step {kind:?} has nothing to write"
        ))
    };
    match kind {
        StepKind::PutBase => {
            let obj = w.base.take().ok_or_else(missing)?;
            repo.storage.put(&obj.key(), &obj.bytes)
        }
        StepKind::PutSweepRecord => {
            let rec = w.record.take().ok_or_else(missing)?;
            repo.storage.put(&rec.key(), &rec.bytes)
        }
        StepKind::PutSegment => {
            let obj = w.segment.take().ok_or_else(missing)?;
            repo.storage.put(&obj.key(), &obj.bytes)?;
            if let Some(dir) = repo.cache_dir() {
                table::write_cached_segment(&dir, &obj.name, &obj.bytes);
            }
            w.segment_written = Some(obj.size());
            Ok(())
        }
        StepKind::Fence => {
            repo.check_lock_fence()?;
            match head_check {
                Some(check) => check.run(repo),
                None => Ok(()),
            }
        }
        StepKind::PutHead => {
            match w.packed_head.take() {
                Some(packed) => repo
                    .storage
                    .put(&index_store::head_key(w.head.generation), &packed)?,
                None => {
                    index_store::put_head(repo.storage.as_ref(), &w.head, repo.crypto.as_ref())?
                }
            }
            if let Some(bytes) = w.segment_written {
                debug!(
                    bytes,
                    generation = w.head.generation,
                    "index segment written"
                );
            }
            Ok(())
        }
        StepKind::CollectGarbage => {
            let gc = index_store::collect_index_garbage(
                repo.storage.as_ref(),
                repo.crypto.as_ref(),
                &w.head,
                repo.index_gc_grace,
            )
            .unwrap_or_else(|e| {
                warn!("failed to delete old index objects: {e}");
                Default::default()
            });
            if let Finish::Fold { gc: slot, .. } = &mut w.finish {
                *slot = gc;
            }
            Ok(())
        }
        other => Err(VykarError::Other(format!(
            "BUG: {other:?} is not a step of an index write"
        ))),
    }
}

impl Machine for PublishMachine {
    type Output = IndexHead;

    fn enabled(&self) -> Vec<StepKind> {
        match &self.state {
            State::FoldGeneration(_) => vec![StepKind::ListHeads],
            State::Write(w) => vec![w.steps.front().copied().unwrap_or(StepKind::Local)],
            State::Start { .. } | State::FoldBody(_) | State::FoldHead(..) => {
                vec![StepKind::Local]
            }
            State::Done(_) | State::Failed => Vec::new(),
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

    fn take_output(&mut self) -> Option<IndexHead> {
        match &mut self.state {
            State::Done(head) => head.take(),
            _ => None,
        }
    }
}
