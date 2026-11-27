//! What the runner records: every client run, and the snapshot set the store
//! held after each run, release and tamper.

use std::collections::BTreeMap;

use vykar_types::error::VykarError;

use super::fault::{ClientId, INJECTED_MARKER, ParkedId};
use super::runner::SourceTree;

#[derive(Clone, Debug)]
pub enum ClientOp {
    Init,
    Backup { snapshot: String },
    Delete { names: Vec<String> },
    Prune,
    Compact,
    Check,
    CheckRepair,
    Restore { snapshot: String },
    BreakLock,
    Custom { label: String },
}

/// Errors a correct client may return under contention or injected faults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AllowedError {
    Locked,
    ActiveSessions,
    StaleChunksDuringCommit,
    /// A commit's marker reaped or its record broken between its
    /// commit mark and its snapshot write (`MarkerLostDuringCommit`).
    MarkerLostDuringCommit,
    LockExpired,
    IndexChanged,
    Interrupted,
    Injected,
    /// Compact refused because the live snapshots could not be marked.
    MarkRefused,
    /// A reader whose planned chunk a concurrent compact removed
    /// (`ChunkRemovedDuringRead`, also inside `RestoreSourceUnreadable`).
    RemovedDuringRead,
    /// An index write refused at its head create because its first object
    /// upload started longer ago than its objects' grace allows
    /// (`ages::INDEX_WRITE_LIMIT`; a time jump inside the write).
    SlowIndexWrite,
    /// A single-writer head write that found another writer's bytes after
    /// its PUT (`ConcurrentHeadWrite`): the correct reaction to a defeated
    /// single-writer check (10-single-protocol step 7, decision D4).
    ConcurrentHeadWrite,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorClass {
    Allowed(AllowedError),
    Disallowed,
}

#[derive(Clone, Debug)]
pub struct ErrorRecord {
    pub display: String,
    pub class: ErrorClass,
}

/// The start of `IndexChanged`'s and `IndexHeadContention`'s text, matched
/// by substring for wrapped errors.
pub const INDEX_CHANGED_TEXT: &str = "the index changed on storage after it was loaded";

pub fn classify(err: &VykarError) -> ErrorClass {
    let allowed = match err {
        VykarError::Locked(_) => Some(AllowedError::Locked),
        VykarError::ActiveSessions(_) => Some(AllowedError::ActiveSessions),
        VykarError::StaleChunksDuringCommit => Some(AllowedError::StaleChunksDuringCommit),
        VykarError::MarkerLostDuringCommit => Some(AllowedError::MarkerLostDuringCommit),
        VykarError::LockExpired(_) => Some(AllowedError::LockExpired),
        VykarError::Interrupted => Some(AllowedError::Interrupted),
        VykarError::IndexChanged | VykarError::IndexHeadContention { .. } => {
            Some(AllowedError::IndexChanged)
        }
        // A refused mark whose cause is an index change classifies as the
        // change, as the substring arm below did before the variant existed.
        VykarError::MarkRefused(inner) if inner.to_string().contains(INDEX_CHANGED_TEXT) => {
            Some(AllowedError::IndexChanged)
        }
        VykarError::MarkRefused(_) => Some(AllowedError::MarkRefused),
        VykarError::SlowIndexWrite { .. } => Some(AllowedError::SlowIndexWrite),
        VykarError::ConcurrentHeadWrite { .. } => Some(AllowedError::ConcurrentHeadWrite),
        // Text fallbacks: a wrapper that formats one of these into a new
        // Other (repair_load_refusal, check's Other(msg.clone()), the CLI's
        // hooked command) still carries the text.
        VykarError::Other(m) if m.contains(INDEX_CHANGED_TEXT) => Some(AllowedError::IndexChanged),
        VykarError::Other(m) if m.starts_with(crate::commands::compact::MARK_REFUSED) => {
            Some(AllowedError::MarkRefused)
        }
        VykarError::Other(m) if m.starts_with(crate::repo::ages::SLOW_INDEX_WRITE_TEXT) => {
            Some(AllowedError::SlowIndexWrite)
        }
        _ => None,
    };
    if let Some(a) = allowed {
        return ErrorClass::Allowed(a);
    }
    if err.is_removed_during_read() {
        return ErrorClass::Allowed(AllowedError::RemovedDuringRead);
    }
    let mut cur: Option<&dyn std::error::Error> = Some(err);
    while let Some(e) = cur {
        if e.to_string().contains(INJECTED_MARKER) {
            return ErrorClass::Allowed(AllowedError::Injected);
        }
        cur = e.source();
    }
    ErrorClass::Disallowed
}

#[derive(Clone, Debug)]
pub enum RunOutcome {
    Ok {
        /// Snapshot names this run removed by name: delete's deleted names,
        /// prune's pruned names, repair's removed snapshots.
        removed: Vec<String>,
        detail: String,
    },
    Err(ErrorRecord),
}

#[derive(Clone, Debug)]
pub struct RunRecord {
    pub index: usize,
    pub client: ClientId,
    pub op: ClientOp,
    /// Store step before the run.
    pub first_step: u64,
    /// Store step when the run returned.
    pub last_step: u64,
    pub outcome: RunOutcome,
    /// The run held a shared session for its protected writes: a backup
    /// that marked its session committing, a compact, prune, delete or
    /// shared custom run that registered its marker. Invariant 13's
    /// `WriteWithoutSession` reads it. Never true together with `exclusive`.
    pub shared: bool,
    /// The run registered an exclusive session (10-single-protocol step 8):
    /// judged by invariant 13 against its `x-` marker.
    pub exclusive: bool,
}

impl RunRecord {
    pub fn is_ok(&self) -> bool {
        matches!(self.outcome, RunOutcome::Ok { .. })
    }

    pub fn removed(&self) -> &[String] {
        match &self.outcome {
            RunOutcome::Ok { removed, .. } => removed,
            RunOutcome::Err(_) => &[],
        }
    }

    pub fn detail(&self) -> &str {
        match &self.outcome {
            RunOutcome::Ok { detail, .. } => detail,
            RunOutcome::Err(e) => &e.display,
        }
    }

    pub fn error_class(&self) -> Option<&ErrorClass> {
        match &self.outcome {
            RunOutcome::Ok { .. } => None,
            RunOutcome::Err(e) => Some(&e.class),
        }
    }

    /// Panics with the run's error unless it succeeded.
    pub fn expect_ok(&self) -> &Self {
        assert!(
            self.is_ok(),
            "run {} {:?} failed: {}",
            self.index,
            self.op,
            self.detail()
        );
        self
    }
}

#[derive(Clone, Debug)]
pub enum ObservationCause {
    Init,
    Run(usize),
    Release {
        parked: ParkedId,
        issuing_run: Option<usize>,
    },
    Tamper,
}

#[derive(Clone, Debug)]
pub struct Observation {
    pub cause: ObservationCause,
    pub step: u64,
    /// `snapshots/<id>` key -> name, `None` if the blob did not decode.
    pub snapshots: BTreeMap<String, Option<String>>,
}

impl Observation {
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.snapshots.values().filter_map(|n| n.as_deref())
    }
}

/// `runs` is in run-index order and `observations` in step order. A spawned
/// run is recorded when it is joined, so while runs are in flight `runs`
/// can have gaps.
#[derive(Clone, Debug, Default)]
pub struct History {
    pub runs: Vec<RunRecord>,
    pub observations: Vec<Observation>,
    /// Source tree of each backup, by snapshot name.
    pub trees: BTreeMap<String, SourceTree>,
}
