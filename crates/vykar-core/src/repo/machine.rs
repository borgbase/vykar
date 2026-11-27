//! Steppable state machines of the commit path (08 step 4, option (c)).
//!
//! A step does local work plus at most one protocol-key storage operation,
//! or one of the compounds named on [`StepKind`]. Production runs every
//! machine to the end with [`drive_blocking`], on the calling thread; the
//! simulator's driver (`sim::driver`) runs one step at a time, between
//! steps of other clients.

use super::Repo;
use super::control::{CommitEvent, EventSink};
use vykar_types::error::{Result, VykarError};

/// What a step does to storage. The simulator's trace and its guard test
/// read it; production ignores it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum StepKind {
    /// No storage access (a transition between storage steps).
    Local,
    /// Flush packs and join uploads; may PUT packs, pack maps and the
    /// session journal `sessions/<id>.index`.
    Flush,
    /// Compound: LIST `snapshots/`, GET the blobs not cached.
    RefreshSnapshots,
    /// EXISTS the session's current marker key `sessions/<id>[.<n>].json`
    /// (again with the new key if a refresh moved it), after an EXISTS of
    /// each superseded key whose refresh check failed.
    ProbeMarker,
    /// Compound: LIST `index.h/`, GET the latest (re-list on a 404).
    FetchHead,
    /// Compound: GET the `index.d/` objects not held locally; on a missing
    /// one, LIST/GET `index.h/`.
    SyncIndex,
    /// Pack existence (`packs/`; not a protocol key) and the reuse check;
    /// on a failed pack check, EXISTS the session marker.
    VerifyPacks,
    /// LIST `index.h/` (a fold without a previous head).
    ListHeads,
    /// Compound: LIST_META `index.d/` (the segments' ages) unless this
    /// run listed it already; then GET the segments a partial fold needs that the
    /// local cache lacks (04 step 7).
    FoldPrefix,
    /// PUT the sweep record `index.d/<name>`.
    PutSweepRecord,
    /// PUT a segment `index.d/<name>`.
    PutSegment,
    /// PUT a base `index.d/<name>`.
    PutBase,
    /// Compound: the put-if-absent check on a handle's first index write
    /// (two creates, a GET and a DELETE of a probe under `sessions/`), then
    /// the writer's fence (an EXISTS of its session marker where the fence
    /// checks one) and, for a backup, the pack-age term.
    Fence,
    /// Compound: create `index.h/<gen>` (put-if-absent where offered and
    /// checked at the fence; a plain PUT otherwise), GET it after an
    /// `AlreadyExists` or an error; LIST `index.h/` after a slow create, and
    /// DELETE the head it re-made when that generation had been collected.
    PutHead,
    /// Compound, best-effort: index garbage collection after a fold
    /// (`index.d/` listed before `index.h/`).
    CollectGarbage,
    /// Compound, unreachable today: the defensive persist of a dirty index.
    PersistIndex,
    /// PUT `snapshots/<id>`: the commit point.
    PutSnapshot,
    /// Wait before the next attempt of a backup commit whose head
    /// generation another writer took. No storage access.
    Backoff,
}

/// A kind of storage access, as [`StepKind::may_touch`] lists them.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Access {
    /// GET, range GET, EXISTS and SIZE.
    Read,
    /// LIST, LIST_SIZED and LIST_META.
    List,
    /// PUT of any kind.
    Write,
    /// DELETE and batch DELETE.
    Delete,
}

#[cfg(test)]
use Access::{Delete, List, Read, Write};

#[cfg(test)]
const FETCH_HEAD: &[(Access, &str)] = &[(List, "index.h/"), (Read, "index.h/")];
#[cfg(test)]
const PERSIST_INDEX: &[(Access, &str)] = &[
    (List, "index.h/"),
    (Read, "index.h/"),
    (Write, "index.d/"),
    (Read, "index.d/"),
    (Write, "index.h/"),
    (List, "index.d/"),
    (Delete, "index.h/"),
    (Delete, "index.d/"),
];
#[cfg(test)]
const PUT_HEAD: &[(Access, &str)] = &[
    (Write, "index.h/"),
    (Read, "index.h/"),
    (List, "index.h/"),
    (Delete, "index.h/"),
];

#[cfg(any(test, feature = "sim"))]
impl StepKind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            StepKind::Local => "local",
            StepKind::Flush => "flush",
            StepKind::RefreshSnapshots => "refresh-snapshots",
            StepKind::ProbeMarker => "probe-marker",
            StepKind::FetchHead => "fetch-head",
            StepKind::SyncIndex => "sync-index",
            StepKind::VerifyPacks => "verify-packs",
            StepKind::ListHeads => "list-heads",
            StepKind::FoldPrefix => "fold-prefix",
            StepKind::PutSweepRecord => "put-sweep-record",
            StepKind::PutSegment => "put-segment",
            StepKind::PutBase => "put-base",
            StepKind::Fence => "fence",
            StepKind::PutHead => "put-head",
            StepKind::CollectGarbage => "collect-garbage",
            StepKind::PersistIndex => "persist-index",
            StepKind::PutSnapshot => "put-snapshot",
            StepKind::Backoff => "backoff",
        }
    }

    /// The step whose [`name`](Self::name) is `name`.
    #[cfg(test)]
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        ALL.iter().copied().find(|k| k.name() == name)
    }

    /// The storage accesses this step may make: (access, key prefix).
    #[cfg(test)]
    pub(crate) fn may_touch(self) -> &'static [(Access, &'static str)] {
        match self {
            StepKind::Local => &[],
            // Phase 1 normally drained the uploads; this is the safety net.
            StepKind::Flush => &[
                (Write, "packs/"),
                (Write, "packmaps/"),
                (Write, "sessions/"),
            ],
            StepKind::RefreshSnapshots => &[(List, "snapshots/"), (Read, "snapshots/")],
            StepKind::ProbeMarker => &[(Read, "sessions/")],
            StepKind::FetchHead => FETCH_HEAD,
            StepKind::SyncIndex => &[(Read, "index.d/"), (List, "index.h/"), (Read, "index.h/")],
            StepKind::VerifyPacks => &[(Read, "packs/"), (List, "packs/"), (Read, "sessions/")],
            StepKind::ListHeads => &[(List, "index.h/")],
            StepKind::FoldPrefix => &[(List, "index.d/"), (Read, "index.d/")],
            StepKind::PutSweepRecord | StepKind::PutSegment | StepKind::PutBase => {
                &[(Write, "index.d/")]
            }
            // The fence runs the maintenance or exclusive session's marker
            // check (an EXISTS under `sessions/`) and, for writers other
            // than a backup, the put-if-absent check (once per backend
            // instance), whose probe lives under `sessions/`. During a v2/v3
            // upgrade the fence also does EXISTS and PUT on the legacy key
            // under `locks/` (`upgrade::LegacyUpgradeLock`); this table is
            // checked only on v6 repositories.
            StepKind::Fence => &[
                (Read, "sessions/"),
                (Write, "sessions/"),
                (Delete, "sessions/"),
            ],
            StepKind::PutHead => PUT_HEAD,
            StepKind::CollectGarbage => &[
                (List, "index.h/"),
                (List, "index.d/"),
                (Read, "index.h/"),
                (Delete, "index.h/"),
                (Delete, "index.d/"),
            ],
            StepKind::PersistIndex => PERSIST_INDEX,
            StepKind::PutSnapshot => &[(Write, "snapshots/")],
            StepKind::Backoff => &[],
        }
    }
}

#[cfg(test)]
const ALL: &[StepKind] = &[
    StepKind::Local,
    StepKind::Flush,
    StepKind::RefreshSnapshots,
    StepKind::ProbeMarker,
    StepKind::FetchHead,
    StepKind::SyncIndex,
    StepKind::VerifyPacks,
    StepKind::ListHeads,
    StepKind::FoldPrefix,
    StepKind::PutSweepRecord,
    StepKind::PutSegment,
    StepKind::PutBase,
    StepKind::Fence,
    StepKind::PutHead,
    StepKind::Backoff,
    StepKind::CollectGarbage,
    StepKind::PersistIndex,
    StepKind::PutSnapshot,
];

/// What a step runs with.
pub(crate) struct StepCx<'a> {
    pub(crate) repo: &'a mut Repo,
    /// The exclusion the steps' writes run under.
    pub(crate) fence: &'a super::writer::Fence,
    pub(crate) events: &'a dyn EventSink<CommitEvent>,
}

/// A steppable machine of the commit path.
pub(crate) trait Machine {
    type Output;

    /// The steps that can run next, in a fixed order. Empty once the machine
    /// has finished (with an output or an error). More than one only for a
    /// batch of independent reads.
    fn enabled(&self) -> Vec<StepKind>;

    /// Run enabled step `pick` (an index into `enabled()`). An `Err` ends
    /// the machine; its cleanup has run when it returns. A `pick` out of
    /// range is a BUG error and also ends it.
    fn step(&mut self, cx: &mut StepCx<'_>, pick: usize) -> Result<()>;

    /// Production: run every enabled step, concurrently where today's code
    /// does (the commit's start reads). Default: `step(cx, 0)`.
    fn step_all(&mut self, cx: &mut StepCx<'_>) -> Result<()> {
        self.step(cx, 0)
    }

    /// The output once finished without error; `None` before, or after an
    /// error, or when taken already.
    fn take_output(&mut self) -> Option<Self::Output>;
}

/// Run `m` to the end on the calling thread.
pub(crate) fn drive_blocking<M: Machine>(m: &mut M, cx: &mut StepCx<'_>) -> Result<M::Output> {
    while !m.enabled().is_empty() {
        m.step_all(cx)?;
    }
    m.take_output()
        .ok_or_else(|| VykarError::Other("BUG: a commit machine finished without an output".into()))
}

/// The error of a `pick` that names no enabled step.
pub(super) fn bad_pick(machine: &str, pick: usize, enabled: &[StepKind]) -> VykarError {
    VykarError::Other(format!(
        "BUG: {machine} has no enabled step {pick} (enabled: {enabled:?})"
    ))
}

/// Emit a `CommitEvent::Stage` and return a context (start time, stage
/// name) for a later [`log_stage_elapsed`].
pub(super) fn emit_stage(
    events: &dyn EventSink<CommitEvent>,
    stage: &'static str,
) -> (std::time::Instant, &'static str) {
    let start = std::time::Instant::now();
    events.emit(CommitEvent::Stage { stage });
    (start, stage)
}

pub(super) fn log_stage_elapsed(ctx: (std::time::Instant, &'static str)) {
    tracing::debug!(
        stage = ctx.1,
        elapsed_ms = ctx.0.elapsed().as_millis() as u64,
        "commit stage complete"
    );
}

/// Join a scoped helper thread, re-raising its panic on the caller.
pub(super) fn join_scoped<T>(handle: std::thread::ScopedJoinHandle<'_, T>) -> T {
    handle
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_names_round_trip() {
        for k in ALL {
            assert_eq!(StepKind::from_name(k.name()), Some(*k));
        }
        assert_eq!(StepKind::from_name("nope"), None);
    }
}
