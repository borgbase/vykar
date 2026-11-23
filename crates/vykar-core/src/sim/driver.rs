//! The deterministic driver over the steppable commit machines (08 step 4).
//!
//! A [`Case`] is a scenario (hosts, one script of commands per client), a
//! fault plan and a choice sequence. The driver runs it single-threaded in
//! the sense that exactly one step runs at a time and nothing runs between
//! steps: at every decision it lists the actions that can run next in a
//! fixed order (a client's next step, the release of a parked effect, a jump
//! of virtual time, a crash), takes the next choice modulo their number
//! (past the end of the choices, 0), and runs that action to its end. Client
//! code still uses its own threads inside a step (the upload pipeline, pack
//! verification, the cleanup thread at the commit point), and each backup
//! keeps its heartbeat thread; after every step the driver waits until each
//! live heartbeat is blocked on virtual time again, so the store sees
//! nothing between decisions.
//!
//! A backup is upload (registration, open and phase 1), then up to
//! [`LOCK_ATTEMPTS`] single attempts at the commit lock, then one decision
//! per step of its `BackupCommit` machine
//! (`Local` steps run right after the step before them), then its finish.
//! Compact is one decision (`Maintenance::Whole`, every seed recorded
//! before 04 step 7) or one decision per protocol-key operation
//! (`Stepped`: gate-stepped on its own thread; the commit machines are
//! steppable, compact is not a machine; see [`is_maintenance_step`]). No
//! time jump is offered while a stepped compact is in flight: it holds the
//! maintenance lock for its whole run, and a jump inside it would make it
//! the victim of an aged-lock reap (H4's family), which no signature
//! describes. Jumps before and after it, and crashes, are offered.
//! Delete is one decision. Both run their index writes through the same
//! publish machine, driven to the end inside the command.
//!
//! [`run_case`] judges the end state with the checker and classifies what
//! it found by [`Signature`]. The [`Trace`] names no random key or time,
//! so its canonical form is the same on every run of a case.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::checker::{Invariant, Report, Violation, ViolationKind, run_subject};
use super::env::SimMachine;
use super::fault::{
    ClientId, FaultAction, FaultRule, INJECTED_MARKER, KeyMatch, OpKind, OpMatch, ParkedId,
};
use super::gate::{GateId, GateRule};
use super::history::{ClientOp, RunOutcome};
use super::log::{OpOutcome, OpRecord, key_class};
use super::runner::{Call, GATE_TIMEOUT, RunResult, Sim, SourceTree, Spawned, seeded_bytes};
use crate::commands::backup::{self, BackupRequest, CommitInProgress, PendingCommit};
use crate::compress::Compression;
use crate::repo::Committed;
use crate::repo::control::{CancelToken, CommitEvent};
use crate::repo::machine::StepKind;
use vykar_storage::StorageBackend;
use vykar_types::error::VykarError;

/// Single attempts at the commit lock before a backup gives up with
/// `Locked` (production waits up to 2 hours of real time instead).
pub const LOCK_ATTEMPTS: u32 = 3;

/// More decisions than this in one case is a harness bug.
pub const MAX_DECISIONS: usize = 400;

/// Jumps of virtual time a decision can offer (1 min, 16 min, 46 min,
/// 3 h 1 min, 6 h 1 min): past the heartbeat poll, the session refresh,
/// the session staleness, the lock refresh and the lock staleness.
pub const ADVANCES: [u64; 5] = [61, 960, 2760, 10860, 21660];

/// At most this many jumps of time per case.
pub const MAX_ADVANCES: usize = 3;

/// Real time to wait for the heartbeats to settle after a step.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Lock staleness of today's protocol (`DEFAULT_STALE_LOCK_SECS`), on the
/// store's clock since 04 step 5.
const STALE_LOCK_SECS: i64 = 6 * 3600;

/// Session staleness of today's protocol (`SESSION_STALE_SECS`), on the
/// store's clock since 04 step 5.
const STALE_SESSION_SECS: i64 = 45 * 60;

/// Size of each file of a backup's tree.
const FILE_SIZE: usize = 8 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HostPlan {
    /// Every client on its own host (`sim-<name>`).
    Distinct,
    /// Every client on hostname "ubuntu" with its own boot ID
    /// (`boot-<name>`) and one installation: cloned machines, H1's
    /// condition before 04 step 1.
    SameHostname,
    /// As `SameHostname`, but every client after the first boots
    /// [`LATE_BOOT_SECS`] after it is placed. The driver's time moves only
    /// by jumps of at least a minute, so after one such a client's uptime
    /// is shorter than the age of the first client's entries and it takes
    /// them for its own earlier boot's: the known hazard H1-late-clone.
    LateClone,
}

/// How long after placement a later client of [`HostPlan::LateClone`]
/// boots.
pub const LATE_BOOT_SECS: i64 = 30;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// One small tree: one file of 8 KiB seeded by the snapshot name, and
    /// with `shared` the file every shared tree (and the control snapshot
    /// `c0`) has. `fold`: request a fold before the commit (the hard
    /// limit's stand-in, which takes the same code path): the commit folds
    /// the foldable prefix (c0's segment, maintenance segments, case
    /// segments older than T_pack) and keeps its own segment and every
    /// younger one.
    Backup {
        shared: bool,
        fold: bool,
    },
    Compact,
    /// Delete the template's control snapshot `c0`. At most one per case.
    DeleteControl,
}

/// The keys a fault matches: `index.h/`, `index.d/`, `snapshots/`,
/// `locks/`, `sessions/`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyClass {
    IndexHead,
    IndexObject,
    Snapshot,
    Lock,
    Session,
}

impl KeyClass {
    pub fn prefix(self) -> &'static str {
        match self {
            KeyClass::IndexHead => "index.h/",
            KeyClass::IndexObject => "index.d/",
            KeyClass::Snapshot => "snapshots/",
            KeyClass::Lock => "locks/",
            KeyClass::Session => "sessions/",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultKind {
    Fail,
    FailApplied,
    Delay,
    Duplicate,
}

impl FaultKind {
    fn action(self) -> FaultAction {
        match self {
            FaultKind::Fail => FaultAction::Fail,
            FaultKind::FailApplied => FaultAction::FailApplied,
            FaultKind::Delay => FaultAction::Delay,
            FaultKind::Duplicate => FaultAction::Duplicate,
        }
    }
}

/// A fault on the `nth` write (1-based) of `class` by client `client`
/// (an index into [`Case::clients`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultSpec {
    pub client: u8,
    pub class: KeyClass,
    pub nth: u8,
    pub kind: FaultKind,
}

fn yes() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Case {
    pub hosts: HostPlan,
    /// One script per client (1..=3 clients, named a, b, c), run in order.
    pub clients: Vec<Vec<Command>>,
    pub faults: Vec<FaultSpec>,
    /// Offer one `Crash` action.
    pub crash: bool,
    /// Offer `Advance` actions.
    #[serde(default = "yes")]
    pub advances: bool,
    /// Consumed one per decision; past the end, as `policy` says.
    pub choices: Vec<u16>,
    /// What the driver does past the end of `choices`.
    #[serde(default)]
    pub policy: Policy,
    /// Wall-clock skew in seconds of each client, by index into `clients`
    /// (missing: 0). Since 04 step 5 no protocol decision reads a client's
    /// wall clock, so a case must behave the same under any skews.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skews: Vec<i64>,
    /// How Compact runs; DeleteControl is always one decision.
    #[serde(default, skip_serializing_if = "Maintenance::is_whole")]
    pub maintenance: Maintenance,
}

/// How the driver runs a Compact command.
///
/// Since 04 step 7 (phase F part 8b) `Stepped` is drawn in cases with time
/// jumps and with crashes, but the driver offers no jump while a stepped
/// compact is in flight: one held between its fence check and its head
/// PUT, or between the head PUT and its pack deletes, across a 6 h jump
/// would become the aged-lock victim itself (H4's family, a known hazard
/// with its own scripted scenario), and the signatures describe backups as
/// victims. 8b's mechanisms need no jump inside compact: the horizon is
/// read at the session listing, the age at the pack listing, I8 at the
/// commit. That a jump between compact's listings only makes its "now"
/// (the lock key's mtime) older, which errs young, is argued, not searched.
/// A crash of the compact's client stays as before
/// (`crash_inside_a_stepped_compact`). 04 step 8 makes maintenance heads
/// put-if-absent and can offer jumps inside it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Maintenance {
    /// One decision for the whole command (every seed recorded before 04
    /// step 7).
    #[default]
    Whole,
    /// One decision per step operation ([`is_maintenance_step`]), the
    /// command on its own thread, held at a gate between decisions.
    Stepped,
}

impl Maintenance {
    // serde's `skip_serializing_if` passes a reference.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn is_whole(&self) -> bool {
        *self == Maintenance::Whole
    }
}

/// Most step operations one stepped compact makes; one more is a harness
/// bug (at least twice the largest count the determinism test measures).
pub const MAX_MAINTENANCE_STEPS: usize = 96;

/// The shard listing `list_packs` issues first on a backend without a
/// tree listing (the simulator's store): the step "before compact lists
/// packs".
const FIRST_PACK_SHARD: &str = "packs/00/";

/// Whether `op` on `key` is a step boundary of a stepped compact: every
/// operation under `sessions/`, `index.h/`, `index.d/`, `snapshots/` and
/// `packmaps/`, the lock fence (an EXISTS of the lock key), every delete
/// under `packs/`, and the listing of the first pack shard. Pack PUTs
/// (repack outputs) and pack reads are not steps: they are not where
/// clients interact, and the listing of the other shards runs inside the
/// decision of the first.
///
/// The other operations under `locks/` (the acquisition with its pre-check
/// listing, clock probe, PUT and listing, a reap, the release) run inside a
/// decision, as a backup's lock attempt is one decision: the checker counts
/// a lock key as held from its PUT (`sim::checker`, invariant 7), so a
/// contender's tentative key must not outlive a decision. Held between its
/// pre-check and its PUT, compact would publish a key after another client
/// acquired the lock compact had found free, and back off only at its next
/// step. The raw lock protocol op by op is `tests::sim_exhaustive`'s
/// gate-stepped search.
pub fn is_maintenance_step(op: OpKind, key: &str) -> bool {
    if key.starts_with("locks/") {
        return op == OpKind::Exists;
    }
    const PROTOCOL: [&str; 5] = [
        "sessions/",
        "index.h/",
        "index.d/",
        "snapshots/",
        "packmaps/",
    ];
    if PROTOCOL.iter().any(|p| key.starts_with(p)) {
        return true;
    }
    if key.starts_with("packs/") {
        return matches!(op, OpKind::Delete | OpKind::BatchDelete)
            || (op == OpKind::ListMeta && key == FIRST_PACK_SHARD);
    }
    false
}

/// What the driver does past the end of a case's choices.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Policy {
    /// Action 0: the first client's next step.
    #[default]
    First,
    /// The first action [`free_actions`] marks free: the client that stepped
    /// last continues; when it has no step, or its last step found the lock
    /// taken or ended its command, the lowest client with a step; when no
    /// client has work, `Stop`. The bounded search (`sim::explore`) runs
    /// cases with it.
    Continue,
}

/// The label of a lock attempt that found the lock taken.
pub const LABEL_LOCKED: &str = "lock (locked)";

/// Which of `actions` are free under preemption bounding, given the
/// decisions before it (`previous`): the pick-0 step of the client that
/// stepped last, if it has one and its last step neither was labelled
/// [`LABEL_LOCKED`] (it yields) nor ended its command (a switch at a
/// command's end is no preemption); otherwise every client's pick-0 step;
/// and `Stop`. Releases, time jumps, crashes, picks other than 0, and steps
/// of another client while the last one can continue are not free.
pub fn free_actions(actions: &[Action], previous: &[TraceEntry]) -> Vec<bool> {
    let last = previous.iter().rev().find_map(|e| match e.action {
        Action::Step { client, .. } => Some((client, e)),
        _ => None,
    });
    let continuing = last.and_then(|(client, e)| {
        let yielded = e.label == LABEL_LOCKED || e.ended;
        let can_step = actions.contains(&Action::Step { client, pick: 0 });
        (!yielded && can_step).then_some(client)
    });
    actions
        .iter()
        .map(|a| match a {
            Action::Stop => true,
            Action::Step { client, pick: 0 } => continuing.is_none_or(|c| c == *client),
            _ => false,
        })
        .collect()
}

impl Case {
    /// Choices that are not the default.
    pub fn non_default_choices(&self) -> usize {
        self.choices.iter().filter(|c| **c != 0).count()
    }
}

/// A store with init and the control backup `c0` (client "ctl", with the
/// shared file), forked per case.
pub struct Template {
    sim: Sim,
}

impl Default for Template {
    fn default() -> Self {
        Self::new()
    }
}

impl Template {
    pub fn new() -> Self {
        Self::build(true)
    }

    /// The template without the jump after c0's backup: c0's segment is
    /// young in every case, so a sweep keeps its entries and a commit's fold
    /// keeps it as a segment (the young-entries rule of 04 step 7 under
    /// search, `tests::sim_exhaustive`).
    pub fn young() -> Self {
        Self::build(false)
    }

    fn build(aged: bool) -> Self {
        let mut sim = Sim::new();
        let ctl = sim.client("ctl");
        sim.backup(ctl, "c0", &backup_tree("c0", true)).expect_ok();
        if aged {
            // c0 predates every case by more than T_pack, as a real
            // repository's older snapshots do: its segment is foldable and
            // its entries sweepable (04 step 7), so the fold path and the
            // reuse of a swept chunk stay reachable without jumps.
            sim.advance(crate::repo::ages::T_PACK + std::time::Duration::from_secs(60));
        }
        Self { sim }
    }

    /// A fork of the template, as each case starts from.
    pub fn fork(&self) -> Sim {
        self.sim.fork_for_case()
    }
}

/// The tree the driver backs up as snapshot `name` (client `a`'s first
/// backup is `a1`).
pub fn backup_tree(name: &str, shared: bool) -> SourceTree {
    let t = SourceTree::new().file("own.bin", seeded_bytes(name, FILE_SIZE));
    if shared {
        t.file("shared.bin", seeded_bytes("shared", FILE_SIZE))
    } else {
        t
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    /// The next step of client `client`; `pick` chooses among a commit's
    /// start reads.
    Step {
        client: u8,
        pick: u8,
    },
    /// Apply parked effect `parked`.
    Release {
        parked: u64,
    },
    Advance {
        secs: u64,
    },
    Crash {
        client: u8,
    },
    /// End the case (every client done); unreleased effects never land.
    Stop,
}

/// What a case's damage looks like. Every variant but `Unknown` is a known
/// property of today's protocol, named here with the plan that removes it.
///
/// Each signature claims the violations it explains, and is inserted only
/// when it claims one; a violation no signature claims, or a skipped
/// evaluation without a cascading signature, adds `Unknown` next to the
/// named ones. Most signatures claim narrowly: the lock-exclusion violation
/// they are derived from, or the violations of one run and its snapshot.
/// `TwoLockHolders`, `LateHeadWrite`, `LateHeadThenCompact` and
/// `LateHeadSegmentCollected` cascade:
/// they leave real damage that later commands fail on, so each claims every
/// violation not known to come before its hazard event (the reap, the
/// released head write), and every skipped evaluation. A violation with a
/// known store step earlier than the hazard is not theirs. Only
/// lock-exclusion violations carry a store step today, so a cascading
/// signature excuses every other violation of its case: "no `Unknown`" is a
/// weak guard for such cases until the hazards are gone. Part 5 of
/// clean-design phase F (04 step 1) removed `TwoLockHolders`' source;
/// part 7 (04 step 5: ages on the store's clock, young orphan packs kept)
/// narrowed `AgedSessionReaped`, `AgedLockPacksCollected` and
/// `LateHeadThenCompact`; part 8c (04 step 7: young backup segments keep
/// their entries from a sweep and stay out of folds; packs a sweep empties
/// are kept while young) narrowed `AgedSessionReaped` and
/// `LateSnapshotWrite` and put `AgedLockPacksCollected` out of the
/// driver's reach; part 9 (04 step 8) removes the late head writes.
///
/// The driver's time jumps move the store and every client clock together,
/// so a jump is a genuine stall of every client, and ages on the store's
/// clock see it like the clients' clocks did; a case's `skews` move only
/// client wall clocks, which no protocol decision reads since 04 step 5.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Signature {
    /// H1: a protected write by a client whose live lock another client on
    /// the same hostname (another boot ID) reaped while it was younger than
    /// the stale age. Also derived, without a checker violation, when the
    /// victim's run makes an ordinary (not released) write to `index.d/`,
    /// `index.h/` or `snapshots/` after a young reap: a delayed or
    /// duplicated lock PUT of the victim's can recreate the reaped key, so
    /// the checker sees a holder (seed `h1-duplicated-lock`). A young reap
    /// of a live lock by another client: H1 before 04 step 1 (installation
    /// ID); since then only [`HostPlan::LateClone`] produces it (the known
    /// hazard H1-late-clone), so a case on another host plan that shows it
    /// is a regression.
    TwoLockHolders,
    /// H1-session: a live backup's session marker deleted by another client
    /// (same hostname, other boot ID) while younger than the session
    /// staleness; compact then deletes the backup's uploads and its commit
    /// fails. Claims the victim run's `DisallowedError` and the violations
    /// of its snapshot; a victim that fails with an allowed error leaves a
    /// clean case and no signature. A young reap of a live marker by
    /// another client: H1 before 04 step 1 (installation ID); since then
    /// only [`HostPlan::LateClone`] produces it (H1-late-clone), so a case
    /// on another host plan that shows it is a regression.
    ///
    /// Since 04 step 2 (phase F part 6) a victim whose pack check fails
    /// while its marker is gone fails with `StaleChunksDuringCommit`
    /// (allowed), so such a case is clean and carries no signature; the
    /// signature claims a victim's disallowed error or its snapshot's
    /// violations when one remains: a reap after the pack check, or a late
    /// copy of the victim's marker PUT (delayed or duplicated by the
    /// transport) that lands after the reap and recreates the key, so the
    /// commit finds the marker present and keeps the untyped pack-missing
    /// error (seed `aged-session-reaped-late-marker-write`). Every marker
    /// key is written once by the client, so "young" is judged by the write
    /// time of the key that was deleted.
    ///
    /// Since 04 step 7 (phase F part 8b) compact runs beside live sessions
    /// and keeps their orphan uploads inside their marker's horizon as well
    /// as by age; a reaped marker leaves only the age guard, and a victim
    /// whose marker record is broken commits only while its packs are
    /// younger than T_pack − M − 30 min (I8). Since phase F part 8c packs
    /// the compact's own sweep empties obey the age guard too, and the
    /// sweep leaves the additions of backup segments younger than T_pack.
    SessionReaped,
    /// A live backup's session marker deleted as stale by another client
    /// after a jump of virtual time past the session staleness (a suspended
    /// machine: the heartbeat counts polls, so one long jump is one poll);
    /// compact then deletes the backup's uploads and its commit fails.
    /// Claims like `SessionReaped`; since 04 step 2 a victim whose pack
    /// check fails with its marker gone fails typed and leaves no signature,
    /// so what remains are the routes `SessionReaped` names (the late
    /// marker copy is the one campaigns found).
    /// The documented limit of today's heartbeat. Since 04 step 5 the reap
    /// is left (46 min without a marker write is stale on any clock) but
    /// its damage narrows: compact keeps the victim's orphan uploads younger
    /// than T_pack. Packs the compact's own sweep emptied still go at once:
    /// a victim that published its head but not yet its snapshot loses them
    /// (seed `aged-session-reaped-swept-packs`). Since 04 step 7 (phase F
    /// part 8b) compact runs beside live sessions and keeps their orphan
    /// uploads inside their marker's horizon as well as by age; a reaped
    /// marker leaves only the age guard, and a victim whose marker record
    /// is broken commits only while its packs are younger than T_pack − M
    /// − 30 min (I8). Since 04 step 7's young segments (phase F part 8c) a
    /// victim's own segment stays a segment until T_pack old and the sweep
    /// leaves its additions, and packs a sweep empties are kept while
    /// young: the swept-packs route needs a stall of T_pack in all between
    /// the victim's head PUT and the compact (seed
    /// `aged-session-reaped-swept-packs-old-segment`). Fixed by 04 step 8.
    AgedSessionReaped,
    /// A protected write by a client whose lock another client reaped by
    /// age (not written for more than the 6 h stale age: the driver's time
    /// jumps, a genuine stall of the holder). The fallback protocol's
    /// documented limit; since 04 step 5 the age is the lock key's mtime on
    /// the store's clock, which only a stall (or a store clock step) moves
    /// past the stale age. 04 steps 8 and 9 take the lock off the commit
    /// path. Claims the lock-exclusion violation it is derived from.
    AgedLockReaped,
    /// A protected write while the store held a lock key that a delayed or
    /// duplicated lock PUT left behind after its writer had deleted it (a
    /// key with no holder). The fallback protocol's limit; 04 step 8 takes
    /// the lock off the commit path. Claims the lock-exclusion violation it
    /// is derived from.
    OrphanLockKey,
    /// H3: a released write of `index.h/` (a head PUT that timed out and
    /// landed after the lock was released), and a committed snapshot lost
    /// its index coverage: one whose backup returned Ok, whose
    /// `snapshots/` PUT applied before the release, or whose backup's own
    /// head the release overwrote before its (not delayed) snapshot PUT.
    /// Fixed by 04 step 8.
    LateHeadWrite,
    /// H3-compact: a released write of `index.h/`, and the index names an
    /// absent pack. Since 04 step 5 compact deletes only orphan packs older
    /// than T_pack (12 h). A jump after the failed commit ages its segment
    /// past T_idx too, so the compact's index GC takes it and the case shows
    /// `LateHeadSegmentCollected` instead; the packs must age before the
    /// segment is written (jumps between the uploads and the commit lock,
    /// seed `late-head-then-compact-old-packs`). Since 04 step 7 (phase F
    /// part 8b) I8 narrows it further: a backup whose marker went
    /// unrefreshed commits only while its packs are younger than T_pack − M
    /// − 30 min at its fence, so the packs must age 40 to 60 min after the
    /// fence while the segment stays younger than T_idx, which the driver's
    /// jumps cannot make (that seed now shows its commit refusing). A
    /// backup whose marker is refreshed throughout passes I8 at any age, so
    /// production still reaches it; the driver never refreshes markers (its
    /// clock moves only by jumps), and the scripted test
    /// `a_late_head_of_a_refreshed_backup_names_packs_compact_deleted`
    /// (tests/sim.rs) covers it. Fixed by 04 step 8.
    LateHeadThenCompact,
    /// H3 meeting index GC (04 "Current issues" 2 and 3): a head PUT that
    /// timed out names a segment of its failed commit; before the late
    /// write lands, a later publisher's `collect_index_garbage` (any
    /// publisher, not only compact) deletes that segment, which no head
    /// named and which was older than `INDEX_GC_GRACE` after a jump of
    /// time. The released head then names an absent segment. Requires a
    /// released `index.h/` write at step h and a `HeadObjectAbsent` whose
    /// object had an applied delete before h and after that head PUT was
    /// issued (while it was in flight), or, when the delete came
    /// after h (the deleter's GC judged by its own head at the generation
    /// the release then overwrote), an object written by the released
    /// head's own run; cascades from h like `LateHeadWrite`. Fixed by 04
    /// step 8 (create-if-absent commits: the late head finds its
    /// generation taken).
    LateHeadSegmentCollected,
    /// 04 "Current issues" 3 with the second holder coming from the 6 h
    /// lock staleness: victim V wrote an `index.d/` segment S under its
    /// lock; a jump of time ages the lock past the stale age (and S past
    /// `INDEX_GC_GRACE`); another client reaps the lock and its index GC
    /// deletes S, which no head names yet (the limit
    /// `collect_index_garbage`'s doc names); V's head PUT, outside any
    /// lock, then names S. Requires all of: an aged reap of V's lock at
    /// step r, an applied PUT of S by V, an applied delete of S by another
    /// client after r, a later non-released `index.h/` PUT by V that
    /// violates lock exclusion, and S named absent (`HeadObjectAbsent`,
    /// or a run's `DisallowedError` when a later fold of V's published a
    /// head without S). Claims that
    /// `HeadObjectAbsent`, the `ChainUnloadable` naming S, the
    /// `DisallowedError` naming S of runs that end after V's head PUT, and
    /// the skipped evaluations; the head PUT's own lock violation stays
    /// `AgedLockReaped`'s. Left by 04 step 5 (the lock is reaped by genuine
    /// age, the segment collected at T_idx = 1 h); fixed by 04 step 8
    /// (create-if-absent commits take the lock off the commit path).
    AgedLockSegmentCollected,
    /// The same aged reap with packs instead of a segment: victim V's lock
    /// and session marker age past their staleness in one jump of time;
    /// another client's compact reaps both and deletes V's uploaded packs
    /// as orphans; V's head PUT, outside any lock, then publishes entries
    /// naming them (directly, or through a later fold of V's that carries
    /// them). Requires an aged reap of V's lock at step r, an applied
    /// delete after r by another client of a pack V PUT, and a later
    /// non-released `index.h/` PUT by V that violates lock exclusion;
    /// claims the `IndexNamesAbsentPack` of those packs. Since 04 step 5
    /// V's packs must be at least T_pack (12 h) old on the store's clock at
    /// the compact: a jump between V's uploads and its commit lock, one
    /// before its fence (which refreshes the lock) and one after (seed
    /// `aged-lock-packs-collected-old-packs`). Since 04 step 7 (phase F
    /// part 8b) I8 narrows it: V's head must pass its fence before its
    /// packs are T_pack − M − 30 min old (11 h 20 min), so the last jump
    /// must come after the fence (a stall after it, the lost-lock hazard).
    /// Since phase F part 8c no driver case reaches it: V writes every
    /// index object before its fence, so the jump that ages its lock ages
    /// them past T_idx too, and the compact's index GC collects them (no
    /// head names them yet). V's head then names an absent segment, and a
    /// later eligible fold of V's keeps that segment named (a segment its listing
    /// lacks reads young; before 8c the fold carried its entries into a
    /// base from V's cache and hid the loss): `AgedLockSegmentCollected`
    /// (seeds `aged-lock-packs-collected{,-old-packs}`). More jumps would
    /// not change that, so it is not `MAX_ADVANCES` (3) that keeps it out
    /// of reach. The scripted test
    /// `an_aged_lock_head_names_packs_compact_deleted` (tests/sim.rs)
    /// shows it with the compact's `index.d/` deletes failing. In production
    /// it is reached whenever the compact's index GC does not take V's
    /// segment after its packs went: a failed best-effort delete, a GC
    /// stopped by an unreadable retained head, a compact that ends between
    /// its pack deletes and its index maintenance, or a later full fold of
    /// V's from its warm index. Fixed by 04 step 8.
    AgedLockPacksCollected,
    /// A delayed `snapshots/` write released after a compact swept its
    /// chunks: invariant 1 fails with no lock violation other than late
    /// index writes (`LateIndexWrite`), writes beside an orphan lock key
    /// (`OrphanLockKey`), and `index.d/` writes of another run than the
    /// late snapshot's after an aged reap of that run's lock
    /// (`AgedLockReaped`; seed `late-snapshot-beside-aged-lock-reap`): a
    /// segment no head names leaves the late snapshot's index coverage
    /// alone, while the reaped run's `index.h/` or `snapshots/` writes, or a
    /// reap of the late snapshot's own run, do not qualify. Claims the
    /// violations of the snapshot whose PUT was the released write.
    /// Narrowed by 04 step 7 (phase F part 8c): the late snapshot's own new
    /// chunks survive while its backup segment is younger than T_pack;
    /// what remains needs a stall of T_pack between its head and the
    /// compact (seed `late-snapshot-write-old-segment`), or a chunk it
    /// reused from a snapshot whose segment is old and that nothing else
    /// visible references. Fixed by 04 step 8 (the commit point is the
    /// head).
    LateSnapshotWrite,
    /// A released write of `index.h/` or `index.d/` that landed outside
    /// the lock and damaged nothing else (invariant 7 only): the late-write
    /// mechanism of H3 without its damage. Claims the lock-exclusion
    /// violations at released index writes that nothing else claims.
    /// Fixed by 04 step 8.
    LateIndexWrite,
    /// A violation no signature above claims, or anything not evaluated
    /// with no cascading signature.
    Unknown,
}

impl Signature {
    /// Signatures whose damage cascades (see [`Signature`]).
    pub fn cascades(self) -> bool {
        matches!(
            self,
            Signature::TwoLockHolders
                | Signature::LateHeadWrite
                | Signature::LateHeadThenCompact
                | Signature::LateHeadSegmentCollected
        )
    }

    /// Signatures that name a hazard (all but `Unknown`).
    pub fn is_hazard(self) -> bool {
        self != Signature::Unknown
    }
}

/// One decision of a trace.
#[derive(Clone, Debug)]
pub struct TraceEntry {
    pub n: usize,
    pub choice: u16,
    pub options: usize,
    pub action: Action,
    /// What ran: the step's name, "upload", "lock", "finish", "whole", or
    /// the action's name; steps run at once after it follow with `+`.
    pub label: String,
    /// Each op as "client op key-class outcome", in issue order.
    pub ops: Vec<String>,
    /// Store steps of the decision: `(first, last]`.
    pub steps: (u64, u64),
    /// The actions offered at this decision, in the driver's order.
    pub actions: Vec<Action>,
    /// A `Step` that ended its client's command (a finish, a whole
    /// command, a failed upload, a lock attempt that gave up).
    pub ended: bool,
    /// The store clock after the decision.
    pub clock: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct Trace {
    pub case: Case,
    pub entries: Vec<TraceEntry>,
    pub signatures: Vec<Signature>,
    pub report: String,
}

impl Trace {
    /// Entries without random names or times: the action, the label, and
    /// the ops sorted within the entry. A client's reads of `packs/` (not a
    /// protocol key) count once per entry, without their outcome: whether a
    /// presence check SIZEs each pack or LISTs their shards depends on how
    /// the random pack IDs fall into shards.
    pub fn canonical(&self) -> Vec<String> {
        self.entries
            .iter()
            .map(|e| {
                let mut ops: Vec<String> = e
                    .ops
                    .iter()
                    .map(|line| {
                        let parts: Vec<&str> = line.split(' ').collect();
                        match parts.as_slice() {
                            [client, op, "packs/", _]
                                if !matches!(*op, "PutPack" | "Put" | "Delete" | "BatchDelete") =>
                            {
                                format!("{client} reads packs/")
                            }
                            _ => line.clone(),
                        }
                    })
                    .collect();
                ops.sort();
                ops.dedup_by(|a, b| a == b && a.ends_with(" reads packs/"));
                format!("{:?} {} [{}]", e.action, e.label, ops.join(", "))
            })
            .collect()
    }

    /// Human-readable: a first line `case: <json>`, one line per entry
    /// (non-default choices marked `*`), the signatures and the report.
    pub fn render(&self) -> String {
        let mut out = format!(
            "case: {}\n",
            serde_json::to_string(&self.case).expect("case serializes")
        );
        for e in &self.entries {
            let mark = if e.choice == 0 { ' ' } else { '*' };
            out.push_str(&format!(
                "{mark}{:>3} {}/{} {:?} {}\n",
                e.n, e.choice, e.options, e.action, e.label
            ));
            for op in &e.ops {
                out.push_str(&format!("        {op}\n"));
            }
        }
        out.push_str(&format!("signatures: {:?}\n", self.signatures));
        out.push_str(&self.report);
        out
    }

    /// Write [`Self::render`] to `$VYKAR_SIM_TRACE_DIR/<name>.trace`, or
    /// under `<temp>/vykar-sim`.
    pub fn write(&self, name: &str) -> std::io::Result<PathBuf> {
        let dir = std::env::var_os("VYKAR_SIM_TRACE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("vykar-sim"));
        self.write_in(&dir, name)
    }

    /// Write [`Self::render`] to `<dir>/<name>.trace`, creating `dir`.
    pub fn write_in(&self, dir: &std::path::Path, name: &str) -> std::io::Result<PathBuf> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{name}.trace"));
        std::fs::write(&path, self.render())?;
        Ok(path)
    }
}

/// Parse a written trace's first line back into its case.
pub fn case_from_trace(text: &str) -> Result<Case, String> {
    let line = text.lines().next().ok_or("empty trace")?;
    let json = line
        .strip_prefix("case: ")
        .ok_or("the first line does not start with `case: `")?;
    serde_json::from_str(json).map_err(|e| format!("bad case: {e}"))
}

pub struct Outcome {
    pub report: Report,
    pub signatures: BTreeSet<Signature>,
    pub trace: Trace,
    /// The simulation the case ran in, for tests that read its log.
    pub sim: Sim,
}

/// Run `case` on a fork of `template`'s store and judge the end state.
pub fn run_case(template: &Template, case: &Case) -> Outcome {
    let mut d = Driver::new(template, case);
    d.run();
    d.verdict()
}

/// A run the driver started.
struct RunCtx {
    index: usize,
    first_step: u64,
    op: ClientOp,
}

enum Phase {
    /// Between commands.
    Idle,
    /// Uploaded; waiting for the commit lock.
    Lock(Box<LockWait>),
    /// The commit machine runs.
    Commit(Box<Running>),
    /// The machine ended with this result; the finish is next.
    Finish(Box<Running>, Box<vykar_types::error::Result<Committed>>),
    /// A stepped compact, held at `gates[next]`.
    Maint(Box<MaintRun>),
}

/// A gate-stepped maintenance run: its thread, one gate per step
/// operation (the `n`th matches the `n`th step op) and the gate it is
/// held at.
struct MaintRun {
    spawned: Spawned,
    gates: Vec<GateId>,
    next: usize,
}

struct LockWait {
    run: RunCtx,
    pending: PendingCommit,
    attempts: u32,
    fold: bool,
    /// The wait's reading of the store's clock, shared by its attempts as
    /// `acquire_lock_with_deadline` shares it.
    store_clock: crate::repo::ages::StoreClock,
}

struct Running {
    run: RunCtx,
    commit: CommitInProgress,
}

struct Actor {
    name: &'static str,
    client: ClientId,
    script: VecDeque<Command>,
    backups: usize,
    phase: Phase,
}

impl Actor {
    fn has_work(&self) -> bool {
        !matches!(self.phase, Phase::Idle) || !self.script.is_empty()
    }

    /// Its session is registered and its heartbeat runs. A stepped compact
    /// registers no session.
    fn heartbeat(&self) -> bool {
        !matches!(self.phase, Phase::Idle | Phase::Maint(_))
    }
}

const NAMES: [&str; 3] = ["a", "b", "c"];

struct Driver<'c> {
    sim: Sim,
    case: &'c Case,
    actors: Vec<Actor>,
    advances: usize,
    crashed: bool,
    entries: Vec<TraceEntry>,
}

/// The commit's progress events; the driver has no use for them.
fn no_events(_: CommitEvent) {}

impl<'c> Driver<'c> {
    fn new(template: &Template, case: &'c Case) -> Self {
        assert!(
            (1..=NAMES.len()).contains(&case.clients.len()),
            "a case has 1 to 3 clients"
        );
        assert!(
            case.clients
                .iter()
                .flatten()
                .filter(|c| **c == Command::DeleteControl)
                .count()
                <= 1,
            "at most one DeleteControl per case"
        );
        let mut sim = template.fork();
        let actors: Vec<Actor> = case
            .clients
            .iter()
            .zip(NAMES)
            .map(|(script, name)| {
                let client = match case.hosts {
                    HostPlan::Distinct => sim.client(name),
                    HostPlan::SameHostname => {
                        sim.client_on(name, "ubuntu", Some(&format!("boot-{name}")))
                    }
                    HostPlan::LateClone => {
                        let mut machine = SimMachine::new("ubuntu", Some(&format!("boot-{name}")));
                        if name != NAMES[0] {
                            let placed = chrono::Duration::from_std(sim.time().elapsed())
                                .expect("sim time in range");
                            machine = machine
                                .booted_at(placed + chrono::Duration::seconds(LATE_BOOT_SECS));
                        }
                        sim.client_at(name, machine)
                    }
                };
                Actor {
                    name,
                    client,
                    script: script.iter().cloned().collect(),
                    backups: 0,
                    phase: Phase::Idle,
                }
            })
            .collect();
        for (actor, &skew) in actors.iter().zip(&case.skews) {
            if skew != 0 {
                sim.set_skew(actor.client, chrono::Duration::seconds(skew));
            }
        }
        for f in &case.faults {
            let Some(actor) = actors.get(usize::from(f.client)) else {
                continue;
            };
            // Lock faults land on lock keys only, never on a clock probe
            // (`locks/vykar-clock-*`), so the count of a class's writes
            // is the same with and without contention.
            let key = match f.class {
                KeyClass::Lock => KeyMatch::LockKey,
                class => KeyMatch::Prefix(class.prefix().into()),
            };
            sim.fault(
                FaultRule::new(OpMatch::AnyWrite, key, f.kind.action())
                    .client(actor.client)
                    .nth(u32::from(f.nth.max(1))),
            );
        }
        Self {
            sim,
            case,
            actors,
            advances: 0,
            crashed: false,
            entries: Vec::new(),
        }
    }

    /// The actions of the next decision, in the fixed order.
    fn actions(&self) -> Vec<Action> {
        let parked: Vec<Action> = self
            .sim
            .store()
            .parked()
            .iter()
            .map(|p| Action::Release { parked: p.id.0 })
            .collect();
        if !self.actors.iter().any(Actor::has_work) {
            let mut out = vec![Action::Stop];
            out.extend(parked);
            return out;
        }
        let mut out = Vec::new();
        for (i, a) in self.actors.iter().enumerate() {
            if !a.has_work() {
                continue;
            }
            let client = i as u8;
            let picks = match &a.phase {
                Phase::Commit(r) => r.commit.enabled().len().max(1),
                _ => 1,
            };
            for pick in 0..picks {
                out.push(Action::Step {
                    client,
                    pick: pick as u8,
                });
            }
        }
        out.extend(parked);
        // No jump inside a stepped compact (see `Maintenance`).
        let in_maint = self
            .actors
            .iter()
            .any(|a| matches!(a.phase, Phase::Maint(_)));
        if self.case.advances && self.advances < MAX_ADVANCES && !in_maint {
            out.extend(ADVANCES.iter().map(|&secs| Action::Advance { secs }));
        }
        if self.case.crash && !self.crashed {
            for (i, a) in self.actors.iter().enumerate() {
                if a.has_work() {
                    out.push(Action::Crash { client: i as u8 });
                }
            }
        }
        out
    }

    fn run(&mut self) {
        for n in 0.. {
            assert!(
                n < MAX_DECISIONS,
                "case not done after {MAX_DECISIONS} decisions:\n{}",
                self.trace_text()
            );
            let actions = self.actions();
            let index = match (self.case.choices.get(n), self.case.policy) {
                (Some(c), _) => usize::from(*c) % actions.len(),
                (None, Policy::First) => 0,
                (None, Policy::Continue) => free_actions(&actions, &self.entries)
                    .iter()
                    .position(|f| *f)
                    .unwrap_or_else(|| {
                        panic!("no free action among {actions:?}:\n{}", self.trace_text())
                    }),
            };
            // Under `First` the raw choice, as given; under `Continue` the
            // index taken.
            let choice = match self.case.policy {
                Policy::First => self.case.choices.get(n).copied().unwrap_or(0),
                Policy::Continue => u16::try_from(index).expect("fewer than 2^16 actions"),
            };
            let action = actions[index].clone();
            let first = self.sim.store().step();
            let mut ended = false;
            let label = match &action {
                Action::Stop => {
                    self.push_entry(n, choice, actions, action, "stop".into(), first, false);
                    return;
                }
                Action::Step { client, pick } => {
                    let i = usize::from(*client);
                    let label = self.step_actor(i, usize::from(*pick));
                    ended = matches!(self.actors[i].phase, Phase::Idle);
                    label
                }
                Action::Release { parked } => {
                    self.sim.release(ParkedId(*parked));
                    "release".into()
                }
                Action::Advance { secs } => {
                    self.advances += 1;
                    self.sim.advance(Duration::from_secs(*secs));
                    "advance".into()
                }
                Action::Crash { client } => {
                    self.crashed = true;
                    self.crash(usize::from(*client));
                    "crash".into()
                }
            };
            self.settle();
            self.push_entry(n, choice, actions, action, label, first, ended);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push_entry(
        &mut self,
        n: usize,
        choice: u16,
        actions: Vec<Action>,
        action: Action,
        label: String,
        first: u64,
        ended: bool,
    ) {
        let store = self.sim.store();
        self.entries.push(TraceEntry {
            n,
            choice,
            options: actions.len(),
            action,
            label,
            ops: Vec::new(),
            steps: (first, store.step()),
            clock: store.now(),
            actions,
            ended,
        });
    }

    /// Wait until every live heartbeat is blocked on virtual time again.
    fn settle(&self) {
        let alive = self.actors.iter().filter(|a| a.heartbeat()).count();
        assert!(
            self.sim.time().wait_for_waiters(alive, SETTLE_TIMEOUT),
            "heartbeats did not settle to {alive} waiters within {SETTLE_TIMEOUT:?} \
                 (waiters: {:?}):\n{}",
            self.sim.time().waiters(),
            self.trace_text()
        );
    }

    fn label(&self, i: usize, label: &'static str) {
        self.sim
            .store()
            .set_label(self.actors[i].client, Some(label));
    }

    fn unlabel(&self, i: usize) {
        self.sim.store().set_label(self.actors[i].client, None);
    }

    /// Run actor `i`'s next step; returns its label.
    fn step_actor(&mut self, i: usize, pick: usize) -> String {
        let phase = std::mem::replace(&mut self.actors[i].phase, Phase::Idle);
        let label = match phase {
            Phase::Idle => self.start_command(i),
            Phase::Lock(w) => self.lock_attempt(i, w),
            Phase::Commit(r) => self.commit_step(i, r, pick),
            Phase::Finish(r, result) => self.finish(i, r, *result),
            Phase::Maint(m) => self.maint_step(i, m),
        };
        self.unlabel(i);
        label
    }

    /// Start a stepped compact on its own thread and wait until it is held
    /// at its first step operation.
    fn start_stepped_compact(&mut self, i: usize) -> String {
        let c = self.actors[i].client;
        let gates: Vec<GateId> = (1..=MAX_MAINTENANCE_STEPS as u32)
            .map(|n| {
                self.sim.gate(
                    GateRule::new(c, OpMatch::Any, KeyMatch::Any)
                        .filter(is_maintenance_step)
                        .nth(n),
                )
            })
            .collect();
        self.label(i, "compact");
        let spawned = self.sim.spawn(c, Call::Compact);
        let m = Box::new(MaintRun {
            spawned,
            gates,
            next: 0,
        });
        self.await_maint(i, m);
        "compact start".into()
    }

    /// Let the operation a stepped compact is held at run, then wait until
    /// it is held at the next one or has ended.
    fn maint_step(&mut self, i: usize, mut m: Box<MaintRun>) -> String {
        let gate = m.gates[m.next];
        let held = self.sim.store().gate_paused(gate).unwrap_or_else(|| {
            panic!(
                "harness: stepped compact not held at its gate:
{}",
                self.trace_text()
            )
        });
        let label = format!("compact {:?} {}", held.op, key_class(&held.key));
        self.label(i, "compact");
        self.sim.resume(gate);
        m.next += 1;
        self.await_maint(i, m);
        label
    }

    /// Wait for `m` to be held at `gates[next]` (phase `Maint`) or to
    /// end (joined and recorded; phase `Idle`). Asserts that every held op
    /// comes from the run's own thread and that no gate was missed.
    fn await_maint(&mut self, i: usize, m: Box<MaintRun>) {
        assert!(
            m.next < m.gates.len(),
            "harness: a stepped compact made more than {MAX_MAINTENANCE_STEPS} step ops:
{}",
            self.trace_text()
        );
        let held = self
            .sim
            .poll_paused(m.gates[m.next], GATE_TIMEOUT)
            .unwrap_or_else(|e| {
                panic!(
                    "harness: {e}:
{}",
                    self.trace_text()
                )
            });
        match held {
            Some(p) => {
                let thread = format!(
                    "sim-{}-{}",
                    self.sim.store().client_name(self.actors[i].client),
                    m.spawned.index()
                );
                assert_eq!(
                    p.thread,
                    thread,
                    "harness: a stepped compact held off its main thread:
{}",
                    self.trace_text()
                );
                self.actors[i].phase = Phase::Maint(m);
            }
            None => self.end_maint(*m),
        }
    }

    /// Open every gate of `m` not passed yet (each must still be armed or
    /// held: one that is neither was missed) and join the run.
    fn end_maint(&mut self, m: MaintRun) {
        let MaintRun {
            spawned,
            gates,
            next,
        } = m;
        for g in &gates[next..] {
            if let Err(e) = self.sim.store().resume_gate(*g) {
                panic!(
                    "harness: stepped compact gate {g:?}: {e}:
{}",
                    self.trace_text()
                );
            }
        }
        self.sim.join(spawned);
    }

    fn start_command(&mut self, i: usize) -> String {
        let Some(cmd) = self.actors[i].script.pop_front() else {
            return "idle".into();
        };
        let c = self.actors[i].client;
        match cmd {
            Command::Backup { shared, fold } => {
                self.label(i, "upload");
                self.actors[i].backups += 1;
                let snapshot = format!("{}{}", self.actors[i].name, self.actors[i].backups);
                let src = self
                    .sim
                    .prepare_backup_source(&snapshot, backup_tree(&snapshot, shared));
                let started = self.sim.begin_run(c);
                let first_step = self.sim.store().step();
                let run = RunCtx {
                    index: started.index,
                    first_step,
                    op: ClientOp::Backup {
                        snapshot: snapshot.clone(),
                    },
                };
                let mut cfg = started.cfg;
                cfg.limits.threads = 1;
                let handle = self.sim.store().handle(c);
                let wrap = move |_: Box<dyn StorageBackend>| -> Box<dyn StorageBackend> {
                    Box::new(handle.clone())
                };
                let source_paths = vec![src.to_string_lossy().into_owned()];
                let req = BackupRequest {
                    snapshot_name: &snapshot,
                    passphrase: None,
                    source_paths: &source_paths,
                    source_label: "sim",
                    exclude_patterns: &[],
                    exclude_if_present: &[],
                    one_file_system: true,
                    git_ignore: false,
                    xattrs_enabled: false,
                    compression: Compression::None,
                    command_dumps: &[],
                    verbose: false,
                };
                match backup::upload(
                    &cfg,
                    &req,
                    &mut None,
                    &CancelToken::new(),
                    Some(&wrap),
                    started.env,
                ) {
                    Ok(mut pending) => {
                        pending.join_background_writes();
                        self.actors[i].phase = Phase::Lock(Box::new(LockWait {
                            run,
                            pending,
                            attempts: 0,
                            fold,
                            store_clock: Default::default(),
                        }));
                    }
                    Err((_, e)) => self.end_run(i, run, Err(e)),
                }
                "upload".into()
            }
            Command::Compact if self.case.maintenance == Maintenance::Stepped => {
                self.start_stepped_compact(i)
            }
            Command::Compact => {
                self.label(i, "whole");
                self.sim.call(c, Call::Compact);
                "whole compact".into()
            }
            Command::DeleteControl => {
                self.label(i, "whole");
                self.sim.call(
                    c,
                    Call::Delete {
                        names: vec!["c0".into()],
                    },
                );
                "whole delete".into()
            }
        }
    }

    fn lock_attempt(&mut self, i: usize, mut w: Box<LockWait>) -> String {
        self.label(i, "lock");
        match w.pending.try_lock(&mut w.store_clock) {
            Ok(guard) => {
                let LockWait {
                    run,
                    mut pending,
                    fold,
                    ..
                } = *w;
                if fold {
                    pending.repo_mut().request_index_fold();
                }
                let commit = pending.begin_commit(guard);
                let mut label = String::from("lock");
                self.run_locals(i, Box::new(Running { run, commit }), Ok(()), &mut label);
                label
            }
            Err(VykarError::Locked(_)) if w.attempts + 1 < LOCK_ATTEMPTS => {
                w.attempts += 1;
                self.actors[i].phase = Phase::Lock(w);
                LABEL_LOCKED.into()
            }
            Err(e) => {
                let LockWait { run, pending, .. } = *w;
                pending.abandon();
                self.end_run(i, run, Err(e));
                "lock (gave up)".into()
            }
        }
    }

    fn commit_step(&mut self, i: usize, mut r: Box<Running>, pick: usize) -> String {
        let kind = r
            .commit
            .enabled()
            .get(pick)
            .copied()
            .unwrap_or(StepKind::Local);
        let mut label = kind.name().to_string();
        self.label(i, kind.name());
        let result = r.commit.step(pick, &no_events);
        self.run_locals(i, r, result, &mut label);
        label
    }

    /// After a step: run the `Local` steps that follow at once, then set
    /// the phase from where the machine stands.
    fn run_locals(
        &mut self,
        i: usize,
        mut r: Box<Running>,
        mut result: vykar_types::error::Result<()>,
        label: &mut String,
    ) {
        while result.is_ok() && r.commit.enabled() == [StepKind::Local] {
            self.label(i, StepKind::Local.name());
            label.push_str(" +local");
            result = r.commit.step(0, &no_events);
        }
        self.actors[i].phase = match result {
            Err(e) => Phase::Finish(r, Box::new(Err(e))),
            Ok(()) if r.commit.enabled().is_empty() => {
                let out = r.commit.take_output().ok_or_else(|| {
                    VykarError::Other("BUG: the commit machine ended without an output".into())
                });
                Phase::Finish(r, Box::new(out))
            }
            Ok(()) => Phase::Commit(r),
        };
    }

    fn finish(
        &mut self,
        i: usize,
        r: Box<Running>,
        result: vykar_types::error::Result<Committed>,
    ) -> String {
        self.label(i, "finish");
        let Running { run, commit } = *r;
        let outcome = commit
            .finish(result, &mut None)
            .map(|o| (Vec::new(), format!("partial: {}", o.is_partial)));
        self.end_run(i, run, outcome);
        "finish".into()
    }

    fn end_run(&mut self, i: usize, run: RunCtx, result: RunResult) {
        let c = self.actors[i].client;
        self.sim
            .finish_driven_run(c, run.index, run.op, run.first_step, result);
    }

    /// Crash actor `i`: every later op of it fails; its session and
    /// repository are dropped without cleanup (the deregistration fails),
    /// a run in progress ends with an injected error, and the rest of its
    /// script is dropped.
    fn crash(&mut self, i: usize) {
        let c = self.actors[i].client;
        self.label(i, "crash");
        self.sim.crash(c);
        self.actors[i].script.clear();
        let phase = std::mem::replace(&mut self.actors[i].phase, Phase::Idle);
        let run = match phase {
            Phase::Idle => None,
            Phase::Lock(w) => {
                let LockWait { run, pending, .. } = *w;
                drop(pending);
                Some(run)
            }
            Phase::Commit(r) | Phase::Finish(r, _) => {
                let Running { run, commit } = *r;
                drop(commit);
                Some(run)
            }
            // The crashed client's ops fail from here on; let the thread
            // run to its end and record it as `Sim::crash` leaves it.
            Phase::Maint(m) => {
                self.end_maint(*m);
                None
            }
        };
        if let Some(run) = run {
            let err = VykarError::Other(format!("{INJECTED_MARKER}: client crashed"));
            self.end_run(i, run, Err(err));
        }
        self.unlabel(i);
    }

    fn trace(&self, report: &Report, signatures: &BTreeSet<Signature>) -> Trace {
        let log = self.sim.store().log();
        let names: BTreeMap<ClientId, String> = log
            .iter()
            .map(|r| (r.client, self.sim.store().client_name(r.client)))
            .collect();
        let entries = self
            .entries
            .iter()
            .map(|e| {
                let mut e = e.clone();
                e.ops = log
                    .iter()
                    .filter(|r| r.step > e.steps.0 && r.step <= e.steps.1)
                    .map(|r| op_line(r, &names))
                    .collect();
                e
            })
            .collect();
        Trace {
            case: self.case.clone(),
            entries,
            signatures: signatures.iter().copied().collect(),
            report: report.to_string(),
        }
    }

    fn trace_text(&self) -> String {
        self.trace(&Report::default(), &BTreeSet::new()).render()
    }

    fn verdict(self) -> Outcome {
        let report = self.sim.check_invariants(&[
            Invariant::CommittedSnapshotsRestore,
            Invariant::AcknowledgedCommitsDurable,
            Invariant::NoSilentRemoval,
            Invariant::IndexIntegrity,
            Invariant::AllowedErrors,
            Invariant::LockExclusion,
            Invariant::OrphanGrace,
            Invariant::MarkerGapCommit,
        ]);
        let signatures = classify(&self.sim, &report, &self.entries);
        let trace = self.trace(&report, &signatures);
        Outcome {
            report,
            signatures,
            trace,
            sim: self.sim,
        }
    }
}

/// "client op key-class outcome".
fn op_line(r: &OpRecord, names: &BTreeMap<ClientId, String>) -> String {
    format!(
        "{} {:?} {} {}",
        names.get(&r.client).map_or("?", String::as_str),
        r.op,
        key_class(&r.key),
        r.outcome.name()
    )
}

fn is_lock_key(key: &str) -> bool {
    key.starts_with("locks/") && key.ends_with(".json")
}

/// A lock key deleted by a client other than its writer.
struct Reap {
    /// The store step of the delete.
    step: u64,
    /// The key's writer.
    victim: ClientId,
    /// The writer's run.
    run: Option<usize>,
    /// The lock's age in seconds at the delete: since the key's last
    /// applied write, on the trace clock.
    age: Option<i64>,
}

impl Reap {
    fn young(&self) -> bool {
        self.age.is_some_and(|a| a <= STALE_LOCK_SECS)
    }

    fn aged(&self) -> bool {
        self.age.is_some_and(|a| a > STALE_LOCK_SECS)
    }
}

/// Which violations a case's signatures explain. A signature is inserted
/// only with at least one violation it claims; a violation, or a skipped
/// evaluation, that no signature claims inserts `Unknown`.
struct Claims<'r> {
    report: &'r Report,
    claimed: Vec<bool>,
    sigs: BTreeSet<Signature>,
    /// A non-cascading signature explains the skipped evaluations.
    skipped: bool,
}

impl<'r> Claims<'r> {
    fn new(report: &'r Report) -> Self {
        Self {
            report,
            claimed: vec![false; report.violations.len()],
            sigs: BTreeSet::new(),
            skipped: false,
        }
    }

    /// Claim for `sig` every violation `pred` accepts; insert `sig` when
    /// there is one.
    fn claim(&mut self, sig: Signature, pred: impl Fn(&Violation) -> bool) -> bool {
        let mut any = false;
        for (i, v) in self.report.violations.iter().enumerate() {
            if pred(v) {
                self.claimed[i] = true;
                any = true;
            }
        }
        if any {
            self.sigs.insert(sig);
        }
        any
    }

    fn claim_index(&mut self, sig: Signature, i: usize) {
        self.claimed[i] = true;
        self.sigs.insert(sig);
    }

    /// A cascading signature (see [`Signature`]) found at store step
    /// `hazard` claims every violation not known to come before it.
    fn cascade(&mut self, sig: Signature, hazard: u64) {
        self.claim(sig, |v| v.step.is_none_or(|s| s >= hazard));
    }

    fn finish(mut self) -> BTreeSet<Signature> {
        let cascading = self.sigs.iter().any(|s| s.cascades());
        if self.claimed.iter().any(|c| !c)
            || (!cascading && !self.skipped && !self.report.not_evaluated.is_empty())
        {
            self.sigs.insert(Signature::Unknown);
        }
        self.sigs
    }
}

/// What the case's damage looks like: the hazard signatures that claim its
/// violations, and `Unknown` when any violation (or skipped evaluation) is
/// left unclaimed.
pub(crate) fn classify(sim: &Sim, report: &Report, entries: &[TraceEntry]) -> BTreeSet<Signature> {
    let log = sim.store().log();
    let runs = &sim.history().runs;
    let released = |r: &OpRecord| matches!(r.outcome, OpOutcome::Released(_));
    let first_released_write = |prefix: &str| {
        log.iter()
            .filter(|r| r.op.is_write() && released(r) && r.key.starts_with(prefix))
            .map(|r| r.step)
            .min()
    };
    let clock_at = |step: u64| {
        entries
            .iter()
            .find(|e| step > e.steps.0 && step <= e.steps.1)
            .map(|e| e.clock)
    };
    let mut claims = Claims::new(report);

    // Lock keys deleted by a client other than their writer, with the
    // lock's age then. Since 04 step 5 production ages a lock by its mtime
    // on the store's clock, which every applied write renews (the fence
    // refresh rewrites the key), so the age is taken from the key's last
    // applied write, on the trace clock. Driver jumps move the store and
    // every client together, so the two agree.
    let mut writers: BTreeMap<&str, (ClientId, Option<usize>, u64)> = BTreeMap::new();
    let mut reaps: Vec<Reap> = Vec::new();
    for r in &log {
        if r.op.is_write() && r.applied() && is_lock_key(&r.key) {
            writers.insert(&r.key, (r.client, r.run, r.step));
        }
        for k in r.deleted_keys() {
            if let Some((w, run, written)) = writers.get(k).copied()
                && w != r.client
            {
                let age = clock_at(written)
                    .zip(clock_at(r.step))
                    .map(|(t, now)| (now - t).num_seconds());
                reaps.push(Reap {
                    step: r.step,
                    victim: w,
                    run,
                    age,
                });
            }
        }
    }
    let lock_violation_at = |step: u64| {
        report
            .violations
            .iter()
            .any(|v| v.invariant == Invariant::LockExclusion && v.step == Some(step))
    };

    // Lock-exclusion violations at a write that was not a release: each is
    // claimed by the lock hazard it follows from. `TwoLockHolders` then
    // cascades from its earliest reap.
    let mut two_holders: Option<u64> = None;
    let mut orphan_key_violations: BTreeSet<usize> = BTreeSet::new();
    // The violations `AgedLockReaped` claims, with the violating write's
    // run and key.
    let mut aged_reap_violations: BTreeMap<usize, (Option<usize>, &str)> = BTreeMap::new();
    for (i, v) in report.violations.iter().enumerate() {
        if v.invariant != Invariant::LockExclusion {
            continue;
        }
        let Some(rec) = v.step.and_then(|s| log.iter().find(|r| r.step == s)) else {
            continue;
        };
        if released(rec) {
            // A late index write: judged below.
            continue;
        }
        let reaped: Vec<&Reap> = reaps
            .iter()
            .filter(|x| x.step < rec.step && x.victim == rec.client)
            .collect();
        let young_reap = reaped.iter().filter(|x| x.young()).map(|x| x.step).min();
        if let Some(s) = young_reap {
            claims.claim_index(Signature::TwoLockHolders, i);
            two_holders = Some(two_holders.map_or(s, |h| h.min(s)));
        } else if reaped.iter().any(|x| x.aged()) {
            claims.claim_index(Signature::AgedLockReaped, i);
            aged_reap_violations.insert(i, (rec.run, rec.key.as_str()));
        } else if log
            .iter()
            .any(|r| r.step < rec.step && r.op.is_write() && released(r) && is_lock_key(&r.key))
        {
            claims.claim_index(Signature::OrphanLockKey, i);
            orphan_key_violations.insert(i);
        }
    }

    // A young reap whose victim's run goes on to make a protected write
    // (an ordinary write, so the run was live at the reap; a released one
    // is a late write and judged as such)
    // is H1 even when the checker saw the victim as a holder: a delayed or
    // duplicated lock PUT of the victim's, released after the reap,
    // recreated the reaped key, so the victim's writes are not lock
    // violations while the reaper's are under two locks. Cascades from the
    // reap like the violations above.
    for reap in reaps.iter().filter(|x| x.young()) {
        let writes_on = log.iter().any(|r| {
            r.step > reap.step
                && r.client == reap.victim
                && r.run == reap.run
                && r.op.is_write()
                && r.applied()
                && !released(r)
                && ["index.d/", "index.h/", "snapshots/"]
                    .iter()
                    .any(|p| r.key.starts_with(p))
        });
        if writes_on {
            two_holders = Some(two_holders.map_or(reap.step, |h| h.min(reap.step)));
        }
    }

    // An aged reap of V's lock, another client's index GC deleting V's
    // segment S after it, and V's head PUT outside the lock naming S.
    for reap in reaps.iter().filter(|x| x.aged()) {
        for seg in log.iter().filter(|r| {
            r.client == reap.victim
                && r.op.is_write()
                && r.applied()
                && r.key.starts_with("index.d/")
        }) {
            let s = seg.key.as_str();
            let collected = log.iter().any(|r| {
                r.step > reap.step && r.client != reap.victim && r.deleted_keys().contains(&s)
            });
            let head = log
                .iter()
                .filter(|r| {
                    r.step > reap.step
                        && r.step > seg.step
                        && r.client == reap.victim
                        && r.op.is_write()
                        && r.applied()
                        && !released(r)
                        && r.key.starts_with("index.h/")
                        && lock_violation_at(r.step)
                })
                .map(|r| r.step)
                .min();
            // S named absent: by the end state's head, or by a run that
            // failed on it before a later fold of V's published a head
            // without it.
            let absent = report.violations.iter().any(|v| match v.kind {
                ViolationKind::HeadObjectAbsent => v.subject == s,
                ViolationKind::DisallowedError => v.detail.contains(s),
                _ => false,
            });
            let Some(head) = head.filter(|_| collected && absent) else {
                continue;
            };
            // Runs that read the chain after the head PUT and failed on S.
            let later: BTreeSet<String> = runs
                .iter()
                .filter(|x| x.last_step > head)
                .map(run_subject)
                .collect();
            if claims.claim(Signature::AgedLockSegmentCollected, |v| match v.kind {
                ViolationKind::HeadObjectAbsent => v.subject == s,
                ViolationKind::ChainUnloadable => v.detail.contains(s),
                ViolationKind::DisallowedError => {
                    later.contains(&v.subject) && v.detail.contains(s)
                }
                _ => false,
            }) {
                claims.skipped = true;
            }
        }
    }

    // An aged reap of V's lock, another client's compact deleting V's packs
    // after it (V's session marker aged too, so they were orphans), and V's
    // head PUT outside the lock publishing entries that name them.
    for reap in reaps.iter().filter(|x| x.aged()) {
        let collected: Vec<(&str, u64)> = log
            .iter()
            .filter(|r| {
                r.client == reap.victim
                    && r.op.is_write()
                    && r.applied()
                    && r.key.starts_with("packs/")
            })
            .filter_map(|p| {
                log.iter()
                    .find(|r| {
                        r.step > reap.step
                            && r.client != reap.victim
                            && r.deleted_keys().contains(&p.key.as_str())
                    })
                    .map(|r| (p.key.as_str(), r.step))
            })
            .collect();
        let named = |pack: &str| {
            collected.iter().any(|(k, deleted)| {
                *k == pack
                    && log.iter().any(|r| {
                        r.step > *deleted
                            && r.client == reap.victim
                            && r.op.is_write()
                            && r.applied()
                            && !released(r)
                            && r.key.starts_with("index.h/")
                            && lock_violation_at(r.step)
                    })
            })
        };
        claims.claim(Signature::AgedLockPacksCollected, |v| {
            v.kind == ViolationKind::IndexNamesAbsentPack && named(&v.subject)
        });
    }

    // Session markers deleted by another client while their writer's run
    // was still in progress: the victim run's disallowed error and the
    // violations of its snapshot.
    let mut marker_writes: BTreeMap<&str, (ClientId, Option<usize>, u64)> = BTreeMap::new();
    for r in &log {
        if r.op.is_write()
            && r.applied()
            && r.key.starts_with("sessions/")
            && r.key.ends_with(".json")
        {
            marker_writes.insert(&r.key, (r.client, r.run, r.step));
        }
        for k in r.deleted_keys() {
            let Some((writer, run, written)) = marker_writes.get(k).copied() else {
                continue;
            };
            let Some(victim) = run
                .and_then(|i| runs.iter().find(|x| x.index == i))
                .filter(|x| x.last_step > r.step)
            else {
                continue;
            };
            if writer == r.client {
                continue;
            }
            let young = clock_at(written)
                .zip(clock_at(r.step))
                .is_some_and(|(w, now)| (now - w).num_seconds() <= STALE_SESSION_SECS);
            let sig = if young {
                Signature::SessionReaped
            } else {
                Signature::AgedSessionReaped
            };
            let run_subject = run_subject(victim);
            let snapshot = match &victim.op {
                ClientOp::Backup { snapshot } => Some(snapshot.as_str()),
                _ => None,
            };
            claims.claim(sig, |v| {
                (v.kind == ViolationKind::DisallowedError && v.subject == run_subject)
                    || snapshot == Some(v.subject.as_str())
            });
        }
    }

    // A released head write cascades from its step when it left a
    // committed snapshot without index coverage, or the index naming an
    // absent pack. Committed: its backup returned Ok, or its `snapshots/`
    // PUT applied and either came before the release, or was not itself a
    // release and the late head overwrote the backup's own head (a backup
    // that crashed or failed after its snapshot PUT).
    let late_head = first_released_write("index.h/");
    if let Some(hazard) = late_head {
        let snapshot_of = |run: Option<usize>| match run
            .and_then(|i| runs.iter().find(|x| x.index == i))
            .map(|x| &x.op)
        {
            Some(ClientOp::Backup { snapshot }) => Some(snapshot.as_str()),
            _ => None,
        };
        let late_key = log
            .iter()
            .find(|r| r.step == hazard)
            .map_or("", |r| r.key.as_str());
        // The run's own head, at the key the release then overwrote.
        let head_before = |run: Option<usize>| {
            log.iter().any(|r| {
                r.step < hazard
                    && r.run == run
                    && r.op.is_write()
                    && r.applied()
                    && r.key == late_key
            })
        };
        let acknowledged: BTreeSet<&str> = runs
            .iter()
            .filter_map(|r| match (&r.op, &r.outcome) {
                (ClientOp::Backup { snapshot }, RunOutcome::Ok { .. }) => Some(snapshot.as_str()),
                _ => None,
            })
            .chain(
                log.iter()
                    .filter(|r| {
                        r.op.is_write()
                            && r.applied()
                            && r.key.starts_with("snapshots/")
                            && (r.step < hazard || (!released(r) && head_before(r.run)))
                    })
                    .filter_map(|r| snapshot_of(r.run)),
            )
            .collect();
        let after = |v: &Violation| v.step.is_none_or(|s| s >= hazard);
        if report.violations.iter().any(|v| {
            after(v)
                && matches!(
                    v.kind,
                    ViolationKind::TreeChunkNotInIndex | ViolationKind::FileChunkNotInIndex
                )
                && acknowledged.contains(v.subject.as_str())
        }) {
            claims.cascade(Signature::LateHeadWrite, hazard);
        }
        if report
            .violations
            .iter()
            .any(|v| after(v) && v.kind == ViolationKind::IndexNamesAbsentPack)
        {
            claims.cascade(Signature::LateHeadThenCompact, hazard);
        }
    }
    // A released head write over a collected segment: cascades from the
    // first released head after a delete of an object a head names absent,
    // or, when the delete came after it, from a released head whose own
    // run wrote that object (the deleter's GC judged by the head it had
    // just written at the generation the release then overwrote).
    // (first delete, writer's run) of each object a head names absent.
    let absent_deleted: Vec<(u64, Option<usize>)> = report
        .violations
        .iter()
        .filter(|v| v.kind == ViolationKind::HeadObjectAbsent)
        .filter_map(|v| {
            let deleted = log
                .iter()
                .filter(|r| r.deleted_keys().contains(&v.subject.as_str()))
                .map(|r| r.step)
                .min()?;
            let writer = log
                .iter()
                .find(|r| r.op.is_write() && r.applied() && r.key == v.subject)
                .and_then(|r| r.run);
            Some((deleted, writer))
        })
        .collect();
    // The step a released write was issued (parked) at: the delete must
    // fall while the head was in flight, or any earlier loss of a named
    // object would be excused by an unrelated late head.
    let parked_at = |r: &OpRecord| match r.outcome {
        OpOutcome::Released(id) => log
            .iter()
            .find(|p| matches!(p.outcome, OpOutcome::Parked(pid, _) if pid == id))
            .map(|p| p.step),
        _ => None,
    };
    if let Some(hazard) = log
        .iter()
        .filter(|r| r.op.is_write() && released(r) && r.key.starts_with("index.h/"))
        .filter(|r| {
            let issued = parked_at(r);
            absent_deleted.iter().any(|(d, w)| {
                (*d < r.step && issued.is_some_and(|p| p < *d)) || (w.is_some() && *w == r.run)
            })
        })
        .map(|r| r.step)
        .min()
    {
        claims.cascade(Signature::LateHeadSegmentCollected, hazard);
    }
    if let Some(hazard) = two_holders {
        claims.cascade(Signature::TwoLockHolders, hazard);
    }

    // A released snapshot PUT of run `late`: the violations of the snapshot
    // it wrote, when no lock violation is one of two holders: each is a
    // late index write (judged below), a write beside an orphan lock key,
    // or an `index.d/` write of another run after an aged reap of its lock
    // (a segment no head names cannot change the late snapshot's index
    // coverage; the reaped run's `index.h/` or `snapshots/` writes can, and
    // a reap of the late snapshot's own run is the cause itself).
    let no_second_holder = |late: Option<usize>| {
        report.violations.iter().enumerate().all(|(i, v)| {
            v.invariant != Invariant::LockExclusion
                || orphan_key_violations.contains(&i)
                || aged_reap_violations
                    .get(&i)
                    .is_some_and(|(run, key)| key.starts_with("index.d/") && *run != late)
                || v.step
                    .and_then(|s| log.iter().find(|r| r.step == s))
                    .is_some_and(|r| released(r) && r.op.is_write() && r.key.starts_with("index."))
        })
    };
    for r in log
        .iter()
        .filter(|r| r.op.is_write() && released(r) && r.key.starts_with("snapshots/"))
    {
        let Some(ClientOp::Backup { snapshot }) = r
            .run
            .and_then(|i| runs.iter().find(|x| x.index == i))
            .map(|x| &x.op)
        else {
            continue;
        };
        if no_second_holder(r.run)
            && report.violated_for(Invariant::CommittedSnapshotsRestore, snapshot)
        {
            claims.claim(Signature::LateSnapshotWrite, |v| v.subject == *snapshot);
        }
    }

    // Lock-exclusion violations at a released index write that nothing
    // above explains.
    let unclaimed_late_index: Vec<usize> = report
        .violations
        .iter()
        .enumerate()
        .filter(|(i, v)| {
            !claims.claimed[*i]
                && v.invariant == Invariant::LockExclusion
                && v.step
                    .and_then(|s| log.iter().find(|r| r.step == s))
                    .is_some_and(|r| released(r) && r.op.is_write() && r.key.starts_with("index."))
        })
        .map(|(i, _)| i)
        .collect();
    for i in unclaimed_late_index {
        claims.claim_index(Signature::LateIndexWrite, i);
    }

    claims.finish()
}
