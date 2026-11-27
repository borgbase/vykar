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
//! A backup is upload (registration, open, phase 1 and the commit mode's
//! decision), then its commit protection one step per decision, with up to
//! [`PROTECTION_ATTEMPTS`] contended steps: on a single-writer repository
//! the listing of `sessions/` before the mark, the commit mark, the listing
//! after it and the pre-head listing of `sessions/` for a live exclusive
//! session (10-single-protocol steps 7 and 8); on a multi-writer one the
//! mark and the pre-head listing; the suspend rule runs in the pre-head
//! listing's decision. Then one decision per step of its `BackupCommit`
//! machine (`Local` steps run right after the step before them), then its
//! finish.
//! Compact is one decision (`Maintenance::Whole`, every seed recorded
//! before 04 step 7) or one decision per protocol-key operation
//! (`Stepped`: gate-stepped on its own thread; the commit machines are
//! steppable, compact is not a machine; see [`is_maintenance_step`]). A
//! stepped compact runs in a session (it registers a session marker, its
//! heartbeat running beside its gated main thread), so jumps of time are
//! offered inside it as well as before and after it; crashes are offered
//! too.
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
use super::history::{ClientOp, RunOutcome, RunRecord};
use super::log::{OpOutcome, OpRecord, key_class};
use super::runner::{Call, GATE_TIMEOUT, RunResult, Sim, SourceTree, Spawned, seeded_bytes};
pub use super::store::StoreKind;
use crate::commands::backup::{self, BackupRequest, CommitInProgress, PendingCommit};
use crate::compress::Compression;
use crate::repo::control::{CancelToken, CommitEvent};
use crate::repo::machine::StepKind;
use crate::repo::{Committed, WriterMode};
use vykar_storage::StorageBackend;
use vykar_types::error::VykarError;

/// Contended attempts of a backup's commit protection (a single-writer
/// listing that found another head writer, a pre-head listing that found a
/// live exclusive session) before it gives up with `Locked` (production
/// waits up to 2 hours of real time instead).
pub const PROTECTION_ATTEMPTS: u32 = 3;

/// More decisions than this in one case is a harness bug.
pub const MAX_DECISIONS: usize = 400;

/// Jumps of virtual time a decision can offer (1 min, 16 min, 46 min,
/// 6 h 1 min): past the heartbeat poll, the session refresh, the marker-gap
/// limit and the session staleness, and (two of them) T_pack,
/// INDEX_OBJECT_GRACE, PACK_AGE_LIMIT and INDEX_WRITE_LIMIT; nothing shorter
/// reaches 12 h within [`MAX_ADVANCES`]. Between 46 min and 6 h 1 min lie
/// HEAD_FRESHNESS_LIMIT (50 min), T_IDX (1 h), CLOCK_PROBE_DEBRIS_AGE (1 h)
/// and COMMIT_MAX_WAIT (2 h): crossing 50 min or 1 h without crossing 6 h
/// costs two jumps, and a case past T_IDX, under 6 h and past T_pack is out
/// of one case's reach (scripted tests cover those thresholds). The
/// 3 h 1 min jump of the lock's refresh went with the lock
/// (10-single-protocol step 10).
pub const ADVANCES: [u64; 4] = [61, 960, 2760, 21660];

/// At most this many jumps of time per case.
pub const MAX_ADVANCES: usize = 3;

/// Real time to wait for the heartbeats to settle after a step.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);

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
/// `sessions/`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyClass {
    IndexHead,
    IndexObject,
    Snapshot,
    Session,
}

impl KeyClass {
    pub fn prefix(self) -> &'static str {
        match self {
            KeyClass::IndexHead => "index.h/",
            KeyClass::IndexObject => "index.d/",
            KeyClass::Snapshot => "snapshots/",
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
#[serde(deny_unknown_fields)]
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
    /// The store's kind (04 step 8): `Plain` (the fallback, every seed
    /// recorded before 04 step 8) or one with put-if-absent. On `Conditional`
    /// the checker also judges invariant 12 and every retained head.
    #[serde(default, skip_serializing_if = "StoreKind::is_plain")]
    pub storage: StoreKind,
    /// The template a case forks. Required since 10-single-protocol step
    /// 10: a seed or trace without it, or with "Plain", does not parse.
    pub template: TemplateKind,
}

/// The repository a [`Template`] holds. `Plain` (the label of cases
/// recorded before 10-single-protocol step 2, which ran encrypted since)
/// went in step 10.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TemplateKind {
    /// AES-256-GCM with a cheap KDF: object names are random per run
    /// (nonces), so nothing the driver decides may depend on them.
    Encrypted,
}

/// How the driver runs a Compact command.
///
/// Since 04 step 7 (phase F part 8b) `Stepped` is drawn in cases with time
/// jumps and with crashes. A stepped compact runs in a session (every
/// driver compact since 10-single-protocol step 7), so jumps are offered
/// inside it; its marker's heartbeat keeps it live across them, and a jump
/// inside it only makes its "now" (its marker key's mtime, read at its
/// session listing) older, which errs young. A crash of the compact's
/// client is offered too (`crash_inside_a_stepped_compact`).
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

/// The name of a session's heartbeat thread: a stepped compact's heartbeat
/// runs beside its gated main thread, its operations are not steps.
const HEARTBEAT_THREAD: &str = crate::repo::sessions::HEARTBEAT_THREAD_NAME;

/// Most step operations one stepped compact makes; one more is a harness
/// bug (at least twice the largest count the determinism test measures:
/// 47 for a lock-free compact in phase F part 10d, which registers, lists
/// sessions twice and deregisters; each of its re-marks, at most two more,
/// adds a session listing with its GETs, a snapshot listing with its GETs
/// and a head read, so the bound leaves room for both).
pub const MAX_MAINTENANCE_STEPS: usize = 192;

/// The shard listing `list_packs` issues first on a backend without a
/// tree listing (the simulator's store): the step "before compact lists
/// packs".
const FIRST_PACK_SHARD: &str = "packs/00/";

/// Whether `op` on `key` is a step boundary of a stepped compact: every
/// operation under `sessions/`, `index.h/`, `index.d/`, `snapshots/` and
/// `packmaps/`, every delete under `packs/`, and the listing of the first
/// pack shard. Pack PUTs (repack outputs) and pack reads are not steps:
/// they are not where clients interact, and the listing of the other shards
/// runs inside the decision of the first.
///
/// A probe (`crate::repo::ages::is_probe_key`: the put-if-absent check's
/// `sessions/vykar-probe-*`, the clock probes) is never a step: the check
/// runs inside the step that needs it.
pub fn is_maintenance_step(op: OpKind, key: &str) -> bool {
    if crate::repo::ages::is_probe_key(key) {
        return false;
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
    /// last continues; when it has no step, or its last step found
    /// contention ([`LABEL_CONTENDED`]) or ended its command, the lowest client
    /// with a step; when no client has work, `Stop`. The bounded search
    /// (`sim::explore`) runs cases with it.
    Continue,
}

/// The label of a backup's protection step that found contention: a
/// single-writer listing that found another head writer (the backup
/// waits, unmarked), or a pre-head listing of `sessions/` that found a live
/// exclusive session (10-single-protocol step 8). "lock (locked)" until
/// step 10; recorded seeds carry the value, so it stays.
pub const LABEL_CONTENDED: &str = "wait (locked)";

/// The label of a backup's commit mark (04 step 9; both modes since
/// 10-single-protocol step 7).
pub const LABEL_MARK_COMMITTING: &str = "mark-committing";

/// The label of a single-writer backup's listing of `sessions/` before or
/// after its mark (10-single-protocol step 7) that found no other head
/// writer (one that did is [`LABEL_CONTENDED`]).
pub const LABEL_LIST_WRITERS: &str = "list-writers";

/// The label of a backup's pre-head listing of `sessions/` that found no
/// live exclusive session (one that did is [`LABEL_CONTENDED`]); the suspend
/// rule runs in the same decision. "probe-locks" until 10-single-protocol
/// step 10.
pub const LABEL_LIST_EXCLUSIVE: &str = "list-exclusive";

/// Which of `actions` are free under preemption bounding, given the
/// decisions before it (`previous`): the pick-0 step of the client that
/// stepped last, if it has one and its last step neither was labelled
/// [`LABEL_CONTENDED`] (it yields) nor ended its command (a switch at a
/// command's end is no preemption); otherwise every client's pick-0 step;
/// and `Stop`. Releases, time jumps, crashes, picks other than 0, and steps
/// of another client while the last one can continue are not free.
pub fn free_actions(actions: &[Action], previous: &[TraceEntry]) -> Vec<bool> {
    let last = previous.iter().rev().find_map(|e| match e.action {
        Action::Step { client, .. } => Some((client, e)),
        _ => None,
    });
    let continuing = last.and_then(|(client, e)| {
        let yielded = e.label == LABEL_CONTENDED || e.ended;
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
/// shared file), forked per case: an encrypted repository on the fallback
/// store (`StoreKind::Plain`), whatever `runner::with_store_kind` says.
pub struct Template {
    sim: Sim,
    kind: TemplateKind,
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

    pub fn kind(&self) -> TemplateKind {
        self.kind
    }

    /// The template without the jump after c0's backup: c0's segment is
    /// young in every case, so a sweep keeps its entries and a commit's fold
    /// keeps it as a segment (the young-entries rule of 04 step 7 under
    /// search, `tests::sim_exhaustive`).
    pub fn young() -> Self {
        Self::build(false)
    }

    fn build(aged: bool) -> Self {
        let mut sim = Sim::with_config(super::store::SimConfig::default());
        let ctl = sim.client("ctl");
        sim.backup(ctl, "c0", &backup_tree("c0", true)).expect_ok();
        if aged {
            // c0 predates every case by more than T_pack, as a real
            // repository's older snapshots do: its segment is foldable and
            // its entries sweepable (04 step 7), so the fold path and the
            // reuse of a swept chunk stay reachable without jumps.
            sim.advance(crate::repo::ages::T_PACK + std::time::Duration::from_secs(60));
        }
        Self {
            sim,
            kind: TemplateKind::Encrypted,
        }
    }

    /// A fork of the template, as each case starts from.
    pub fn fork(&self) -> Sim {
        self.sim.fork_for_case()
    }

    /// A fork of the template on a store of `kind` (the template itself is
    /// built on `Plain`).
    pub fn fork_as(&self, kind: StoreKind) -> Sim {
        self.sim.fork_for_case_as(kind)
    }
}

/// The aged template, built on first use: what a caller that runs cases
/// holds (the campaign, the seed runner, replays).
#[derive(Default)]
pub struct Templates {
    encrypted: std::cell::OnceCell<Template>,
}

impl Templates {
    pub fn new() -> Self {
        Self::default()
    }

    /// The template of `kind`, built on first use.
    pub fn get(&self, kind: TemplateKind) -> &Template {
        match kind {
            TemplateKind::Encrypted => self.encrypted.get_or_init(Template::new),
        }
    }

    /// [`run_case`] on the template `case` names.
    pub fn run(&self, case: &Case) -> Outcome {
        run_case(self.get(case.template), case)
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
/// named ones. Most signatures claim narrowly: the violations of one run
/// and its snapshot, or the ones their evidence names. `LateHeadWrite`,
/// `LateHeadThenCompact` and `LateHeadSegmentCollected` cascade: they
/// leave real damage that later commands fail on, so each claims every
/// violation not known to come before its hazard event (the released head
/// write), and every skipped evaluation, but never an invariant-13 one
/// (`ExclusiveSessionReaped` alone claims those). Violations of
/// invariants 10, 11, 12 and 13 carry a store step; a cascading signature
/// claims those at or after its hazard step and every step-less one, so
/// "no `Unknown`" is a weak guard for such cases. Part 7 of clean-design
/// phase F (04 step 5: ages on the store's clock, young orphan packs kept)
/// narrowed `AgedSessionReaped` and `LateHeadThenCompact`; part 8c (04
/// step 7: young backup segments keep their entries from a sweep and stay
/// out of folds; packs a sweep empties are kept while young) narrowed
/// `AgedSessionReaped` and `LateSnapshotWrite`; part 9 (04 step 8: heads
/// created with put-if-absent) closes `LateHeadWrite`,
/// `LateHeadThenCompact` and `LateHeadSegmentCollected` on multi-writer
/// repositories; single-writer ones (`StoreKind::Plain`) keep them.
/// 10-single-protocol step 10 removed the lock's signatures with the lock
/// and named the single-writer residual (`SingleWriterHeadRace`) and the
/// reaped exclusive session (`ExclusiveSessionReaped`).
///
/// The driver's time jumps move the store and every client clock together,
/// so a jump is a genuine stall of every client, and ages on the store's
/// clock see it like the clients' clocks did; a case's `skews` move only
/// client wall clocks, which no protocol decision reads since 04 step 5.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Signature {
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
    /// − 30 min (I8). A commit probes its marker before its snapshot PUT,
    /// so a reap before that probe ends it `MarkerLostDuringCommit`. Since
    /// 04 step 7's young segments (phase F part 8c) a
    /// victim's own segment stays a segment until T_pack old and the sweep
    /// leaves its additions, and packs a sweep empties are kept while
    /// young: the swept-packs route needs a stall of T_pack in all between
    /// the victim's head PUT and the compact (seed
    /// `aged-session-reaped-swept-packs-old-segment`). Left by part 9 (04
    /// step 8): the sweep cannot mark a snapshot not yet written; part 10
    /// (04 step 9 and 05's `reused` section). Since phase F part 10d's fix
    /// round also a compact's residual: it checks its marker
    /// before every head create and batch of deletes, so what remains is a
    /// reap by age while a head create is in flight after that check. The
    /// signature claims the compact's repack outputs another client deleted
    /// after the reap (`IndexNamesAbsentPack`) and the restores that need
    /// them; nothing else. That claim cannot tell a reap after the
    /// compact's marker check from a compact that never checked: with the
    /// fence in place the two look alike, so the scripted tests in which the
    /// compact stops at its marker check guard the fence, not campaigns.
    /// Since part 10d's fix rounds 2 and 3 also a backup whose session a
    /// compact found stale by age at its listing, judged as
    /// production judges it (the newest key against the compact's own "now"),
    /// whichever run deleted the stale keys: the victim then wrote its next
    /// key (its commit mark) and the compact deleted its old uploads as
    /// orphans while that key was present (invariant 10; seed
    /// `aged-session-reaped-mark-after-listing-lockfree`). The victim's record has
    /// a gap and its packs are past `PACK_AGE_LIMIT`, so its commit refuses
    /// before a head. Claims those pack deletes, of packs past
    /// `PACK_AGE_LIMIT` at the compact's "now", by the listing run, while
    /// the victim wrote no snapshot and every key of it present was written
    /// after the listing; nothing else.
    AgedSessionReaped,
    /// H3: a released write of `index.h/` (a head PUT that timed out and
    /// landed after the next commit), and a committed snapshot lost
    /// its index coverage: one whose backup returned Ok, whose
    /// `snapshots/` PUT applied before the release, or whose backup's own
    /// head the release overwrote before its (not delayed) snapshot PUT.
    /// Closed on conditional stores by 04 step 8 (phase F part 9): a
    /// released create finds its generation taken; the fallback (`Plain`)
    /// keeps it. Also the racy store's route (`StoreKind::RacyRelease`, known
    /// hazard H-racy): a released put-if-absent create lands late and
    /// overwrites whatever head is there.
    LateHeadWrite,
    /// H3-compact: a released write of `index.h/`, and the index names an
    /// absent pack. Since 04 step 5 compact deletes only orphan packs older
    /// than T_pack (12 h). A jump after the failed commit ages its segment
    /// past the object grace too (T_idx until phase F part 10b2, T_pack
    /// since: `index_store::INDEX_OBJECT_GRACE`), so the compact's index GC
    /// takes it and the case shows `LateHeadSegmentCollected` instead; the packs must age before the
    /// segment is written (jumps between the uploads and the commit protection,
    /// seed `late-head-then-compact-old-packs`). Since 04 step 7 (phase F
    /// part 8b) I8 narrows it further: a backup whose marker went
    /// unrefreshed commits only while its packs are younger than T_pack − M
    /// − 30 min at its fence, so the packs must age 40 to 60 min after the
    /// fence while the segment stays younger than the object grace, which
    /// the driver's jumps could not make while that grace was T_idx (that
    /// seed shows its commit refusing; with T_pack since part 10b2 the
    /// window is open again in principle, not re-searched by a seed). A
    /// backup whose marker is refreshed throughout passes I8 at any age, so
    /// production still reaches it; the driver never refreshes markers (its
    /// clock moves only by jumps), and the scripted test
    /// `a_late_head_of_a_refreshed_backup_names_packs_compact_deleted`
    /// (tests/sim.rs) covers it. Closed on conditional stores by 04 step 8
    /// (phase F part 9): the late create cannot replace compact's head; the
    /// fallback (`Plain`) keeps it.
    LateHeadThenCompact,
    /// H3 meeting index GC (04 "Current issues" 2 and 3): a head PUT that
    /// timed out names a segment of its failed commit; before the late
    /// write lands, a later publisher's `collect_index_garbage` (any
    /// publisher, not only compact) deletes that segment, which no head
    /// named and which was older than the object grace
    /// (`INDEX_OBJECT_GRACE`, 12 h since phase F part 10b2) after jumps of
    /// time. The released head then names an absent segment. Requires a
    /// released `index.h/` write at step h and a `HeadObjectAbsent` whose
    /// object had an applied delete before h and after that head PUT was
    /// issued (while it was in flight), or, when the delete came
    /// after h (the deleter's GC judged by its own head at the generation
    /// the release then overwrote), an object written by the released
    /// head's own run; cascades from h like `LateHeadWrite`. Closed on
    /// conditional stores by 04 step 8 (phase F part 9): a collector runs
    /// only right after it created the latest head, so the late create
    /// finds its generation taken; the fallback (`Plain`) keeps it.
    LateHeadSegmentCollected,
    /// A delayed `snapshots/` write released after a compact swept its
    /// chunks: invariant 1 fails with no invariant-13 violation in the case.
    /// Claims the violations of the snapshot whose PUT was the released
    /// write. Narrowed by 04 step 7 (phase F part 8c): the late snapshot's
    /// own new chunks survive while its backup segment is younger than
    /// T_pack; what remains needs a stall of T_pack between its head and the
    /// compact (seed `late-snapshot-write-old-segment`), or a chunk it
    /// reused from a snapshot whose segment is old and that nothing else
    /// visible references. Left by part 9: a head visible before its
    /// snapshot needs 05's `reused` section or snapshot-before-head (part 10,
    /// 04 step 9).
    LateSnapshotWrite,
    /// The single-writer residual (10-single-protocol step 7's defeated
    /// check, named by step 10): a run's plain PUT of `index.h/<g>`, issued
    /// and applied at once (a released late write is `LateHeadWrite`'s
    /// mechanism), after its own EXISTS of that key found it absent, while
    /// another run wrote the key between the EXISTS and the PUT: two writers
    /// of one generation, the second replacing the first. Requires the stall
    /// and reap that make it the residual: between the EXISTS and the PUT,
    /// another client's applied delete of a marker key the PUT's run wrote,
    /// over the session staleness after that run's last marker write. Routes:
    /// a stall over the session staleness between the EXISTS and the PUT
    /// whose marker another writer reaped (a compact; a backup; a
    /// multi-writer backup after `upgrade --commit-mode multi`): known hazard
    /// H-late-head-single. Not planned to close; closed where a plain PUT
    /// cannot replace a head (vykar-server answers 409, read as Taken).
    /// Claims the replacement (`HeadOverwritten` at the PUT), the replaced
    /// head's snapshot's lost index coverage, the absent packs the replaced
    /// head's run deleted before the PUT, and a compact's refusal to mark
    /// that snapshot; never cascades.
    SingleWriterHeadRace,
    /// An exclusive run writes after another client deleted its live
    /// exclusive marker: young (a late clone misjudging the holder, known
    /// hazard H1-late-clone) or aged (a stall over 45 min past its write
    /// check). Claims that run's invariant-13 violations after the delete.
    ExclusiveSessionReaped,
    /// A violation no signature above claims, or anything not evaluated
    /// with no cascading signature.
    Unknown,
}

impl Signature {
    /// Signatures whose damage cascades (see [`Signature`]).
    pub fn cascades(self) -> bool {
        matches!(
            self,
            Signature::LateHeadWrite
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
    /// What ran: the step's name, "upload", a protection step's label,
    /// "finish", "whole", or the action's name; steps run at once after it
    /// follow with `+`.
    pub label: String,
    /// Each op as "client op key-class outcome", in issue order.
    pub ops: Vec<String>,
    /// Store steps of the decision: `(first, last]`.
    pub steps: (u64, u64),
    /// The actions offered at this decision, in the driver's order.
    pub actions: Vec<Action>,
    /// A `Step` that ended its client's command (a finish, a whole
    /// command, a failed upload, a protection that gave up).
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
///
/// Panics when `template` is not of the case's `template` kind.
pub fn run_case(template: &Template, case: &Case) -> Outcome {
    assert_eq!(
        template.kind, case.template,
        "a case of template {:?} run on a {:?} template",
        case.template, template.kind
    );
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
    /// Uploaded: the commit protection (the single-writer listings, the
    /// mark, the pre-head listing of `sessions/`, the suspend rule).
    Probe(Box<ProbeWait>),
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
    /// The compact's session (04 step 9), from its registration key's PUT
    /// (its first step op).
    session: Option<String>,
    /// Its heartbeat thread runs: registered and adopted, not yet ended.
    /// Adopted is known from the first step op after the registration's
    /// listing of `sessions/` (10-single-protocol step 8: the first one
    /// held after the registration key's PUT; a retry of it, made only
    /// while an exclusive session's key is listed, is one too) that is not
    /// a delete of its own key: any other step op after the registration
    /// (the single-writer check's listing of `sessions/`, sessions #1, or
    /// the listing of the first pack shard) follows the adoption. Any
    /// delete of its own key (a failed registration, or the deregistration,
    /// which stops the heartbeat before its first delete) ends it.
    heartbeat: bool,
    /// A delete of its own marker key was held: no heartbeat from here on.
    ended: bool,
    /// The registration's listing of `sessions/` was held.
    listed: bool,
    /// The registration probe passed and the session was adopted.
    adopted: bool,
}

impl MaintRun {
    /// Track the run's session from the step op it is held at;
    /// `exclusive_listed`: `sessions/` holds an exclusive session's key (the
    /// registration may retry its listing).
    fn observe(&mut self, held: &super::gate::Paused, exclusive_listed: bool) {
        let own_key = |id: &str| {
            held.key
                .strip_prefix("sessions/")
                .is_some_and(|rest| rest.starts_with(id))
        };
        let registration = self.session.is_none()
            && held.op.is_write()
            && held.key.starts_with("sessions/")
            && held.key.ends_with(".json");
        if registration {
            self.session = crate::repo::sessions::parse_session_marker_key(&held.key)
                .map(|(id, _)| id.to_string());
            self.heartbeat = false;
            return;
        }
        if let Some(id) = self.session.as_deref()
            && held.op.is_delete()
            && own_key(id)
        {
            self.ended = true;
        }
        let listing = held.op == OpKind::ListMeta && held.key == "sessions/";
        if self.session.is_some() && !self.ended {
            if listing && !self.adopted && (!self.listed || exclusive_listed) {
                self.listed = true;
            } else if self.listed {
                self.adopted = true;
            }
        }
        self.heartbeat = self.adopted && !self.ended;
    }
}

/// A backup between its upload and its commit machine: the steps of
/// `PendingCommit::protect_commit`, one decision each, with up to
/// [`PROTECTION_ATTEMPTS`] contended ones. Single-writer: the listing
/// before the mark, the mark, the listing after it, the pre-head listing;
/// multi-writer: the mark, the pre-head listing. The suspend rule runs in
/// the pre-head listing's decision.
struct ProbeWait {
    run: RunCtx,
    pending: PendingCommit,
    /// The listing before the mark passed (single-writer).
    listed_before: bool,
    /// The listing after the mark passed (single-writer).
    listed_after: bool,
    attempts: u32,
    fold: bool,
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

    /// Its session is registered and its heartbeat runs: a stepped compact's
    /// once adopted (`MaintRun::heartbeat`).
    fn heartbeat(&self) -> bool {
        match &self.phase {
            Phase::Idle => false,
            Phase::Maint(m) => m.heartbeat,
            _ => true,
        }
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
        let mut sim = template.fork_as(case.storage);
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
            // Session faults never land on a probe (`sessions/vykar-probe-*`,
            // `sessions/vykar-clock-*`), so a seed's nth session write is
            // the same marker write as before the probes moved there.
            let key = match f.class {
                KeyClass::Session => KeyMatch::SessionKey,
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
        if self.case.advances && self.advances < MAX_ADVANCES {
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
            Phase::Probe(w) => self.probe_step(i, w),
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
                        .not_on_thread(HEARTBEAT_THREAD)
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
            session: None,
            heartbeat: false,
            ended: false,
            listed: false,
            adopted: false,
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
    fn await_maint(&mut self, i: usize, mut m: Box<MaintRun>) {
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
                let exclusive_listed = self
                    .sim
                    .store()
                    .keys("sessions/x-")
                    .iter()
                    .any(|k| k.ends_with(".json"));
                m.observe(&p, exclusive_listed);
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
            ..
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
                    passphrase: cfg.encryption.passphrase.as_deref(),
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
                        // The mode was decided right after open (its
                        // put-if-absent check wrote probes under sessions/
                        // before the upload); this hits the cache.
                        match pending.decide_mode() {
                            Ok(_) => {
                                self.actors[i].phase = Phase::Probe(Box::new(ProbeWait {
                                    run,
                                    pending,
                                    listed_before: false,
                                    listed_after: false,
                                    attempts: 0,
                                    fold,
                                }));
                            }
                            Err(e) => {
                                pending.abandon();
                                self.end_run(i, run, Err(e));
                            }
                        }
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

    /// One step of a backup's commit protection, mirroring
    /// `PendingCommit::protect_commit`: the listing before the mark
    /// (single-writer), the mark, the listing after it (single-writer), or
    /// the pre-head listing of `sessions/` with the suspend rule; a listing
    /// that finds no live exclusive session and a rule that proceeds start
    /// the commit machine in the same decision.
    fn probe_step(&mut self, i: usize, mut w: Box<ProbeWait>) -> String {
        let single = w.pending.protection_mode() == Some(WriterMode::Single);
        let marked = w.pending.is_marked();
        if single && !marked && !w.listed_before {
            self.label(i, LABEL_LIST_WRITERS);
            return match w.pending.list_before_mark() {
                Ok(()) => {
                    w.listed_before = true;
                    self.actors[i].phase = Phase::Probe(w);
                    LABEL_LIST_WRITERS.into()
                }
                Err(e) => self.contended(i, w, e, LABEL_LIST_WRITERS),
            };
        }
        if !marked {
            self.label(i, LABEL_MARK_COMMITTING);
            return match w.pending.mark_committing() {
                Ok(()) => {
                    w.listed_before = false;
                    self.actors[i].phase = Phase::Probe(w);
                    LABEL_MARK_COMMITTING.into()
                }
                Err(e) => {
                    let ProbeWait { run, pending, .. } = *w;
                    pending.abandon();
                    self.end_run(i, run, Err(e));
                    format!("{LABEL_MARK_COMMITTING} (failed)")
                }
            };
        }
        if single && !w.listed_after {
            self.label(i, LABEL_LIST_WRITERS);
            return match w.pending.list_after_mark() {
                Ok(()) => {
                    w.listed_after = true;
                    self.actors[i].phase = Phase::Probe(w);
                    LABEL_LIST_WRITERS.into()
                }
                // Unmarked by the listing: the next attempt lists before
                // the mark again.
                Err(e) => self.contended(i, w, e, LABEL_LIST_WRITERS),
            };
        }
        self.label(i, LABEL_LIST_EXCLUSIVE);
        let probed = w
            .pending
            .try_probe()
            .and_then(|()| w.pending.suspend_rule());
        match probed {
            Ok(backup::Protection::Proceed) => {
                let ProbeWait {
                    run,
                    mut pending,
                    fold,
                    ..
                } = *w;
                if fold {
                    pending.repo_mut().request_index_fold();
                }
                let commit = pending.begin_commit();
                let mut label = String::from(LABEL_LIST_EXCLUSIVE);
                self.run_locals(i, Box::new(Running { run, commit }), Ok(()), &mut label);
                label
            }
            Ok(backup::Protection::Restart) => {
                // Unmarked; the protection runs again in the new mode.
                w.listed_before = false;
                w.listed_after = false;
                self.actors[i].phase = Phase::Probe(w);
                format!("{LABEL_LIST_EXCLUSIVE} (mode changed)")
            }
            Err(e) => self.contended(i, w, e, LABEL_LIST_EXCLUSIVE),
        }
    }

    /// A protection step of actor `i` that failed with `e` (`label`: the
    /// step's): `Locked` with attempts left waits (the next attempt starts
    /// at the top of the protection: a single-writer backup the listing
    /// unmarked lists before the mark again, a marked one after it); any
    /// other error, or the last attempt, abandons the commit.
    fn contended(&mut self, i: usize, mut w: Box<ProbeWait>, e: VykarError, label: &str) -> String {
        match e {
            VykarError::Locked(_) if w.attempts + 1 < PROTECTION_ATTEMPTS => {
                w.attempts += 1;
                w.listed_before = false;
                w.listed_after = false;
                self.actors[i].phase = Phase::Probe(w);
                LABEL_CONTENDED.into()
            }
            e => {
                let ProbeWait { run, pending, .. } = *w;
                pending.abandon();
                self.end_run(i, run, Err(e));
                format!("{label} (gave up)")
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
            .finish(result, &mut None, &crate::repo::control::CancelToken::new())
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
            Phase::Probe(w) => {
                let ProbeWait { run, pending, .. } = *w;
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
        // Invariant 12 is asked for by the repository's mode: heads are
        // put-if-absent on a multi-writer repository (10-single-protocol
        // step 7; before it, by the store's honouring of creates).
        let set = Invariant::judged(self.case.storage.records_multi_writer());
        let report = self.sim.check_invariants(&set);
        let signatures = classify(&self.sim, &report);
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
    /// `hazard` claims every violation not known to come before it, but no
    /// invariant-13 violation (`ExclusiveSessionReaped` alone claims those).
    fn cascade(&mut self, sig: Signature, hazard: u64) {
        self.claim(sig, |v| {
            v.invariant != Invariant::ExclusiveSessions && v.step.is_none_or(|s| s >= hazard)
        });
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

/// What [`classify_evidence`] reads of a simulation: its store log and its
/// runs.
#[derive(Clone, Debug)]
pub(crate) struct Evidence {
    pub log: Vec<OpRecord>,
    pub runs: Vec<RunRecord>,
}

impl Evidence {
    pub(crate) fn of(sim: &Sim) -> Self {
        Self {
            log: sim.store().log(),
            runs: sim.history().runs.clone(),
        }
    }
}

/// What the case's damage looks like: the hazard signatures that claim its
/// violations, and `Unknown` when any violation (or skipped evaluation) is
/// left unclaimed.
pub(crate) fn classify(sim: &Sim, report: &Report) -> BTreeSet<Signature> {
    classify_evidence(&Evidence::of(sim), report)
}

/// The end state judged as the driver judges a case (`Invariant::judged`
/// by the repository's recorded mode) and classified: what a known
/// hazard's `Reproduced` carries.
pub(crate) fn classify_end_state(sim: &Sim) -> (Report, BTreeSet<Signature>) {
    let multi = sim.store().writer_mode() == Some(WriterMode::Multi);
    let report = sim.check_invariants(&Invariant::judged(multi));
    let signatures = classify(sim, &report);
    (report, signatures)
}

/// The run's snapshot, when it is a backup.
fn snapshot_of(runs: &[RunRecord], run: Option<usize>) -> Option<&str> {
    match run
        .and_then(|i| runs.iter().find(|x| x.index == i))
        .map(|x| &x.op)
    {
        Some(ClientOp::Backup { snapshot }) => Some(snapshot.as_str()),
        _ => None,
    }
}

/// [`classify`] on evidence a test may have edited (to show that a
/// signature's claim needs each of its conditions). Every age is read from
/// the records' store clock (`OpRecord::at`), which a scripted run has as
/// a driver run does.
pub(crate) fn classify_evidence(ev: &Evidence, report: &Report) -> BTreeSet<Signature> {
    let log = ev.log.as_slice();
    let runs = &ev.runs;
    let released = |r: &OpRecord| matches!(r.outcome, OpOutcome::Released(_));
    let first_released_write = |prefix: &str| {
        log.iter()
            .filter(|r| r.op.is_write() && released(r) && r.key.starts_with(prefix))
            .map(|r| r.step)
            .min()
    };
    let is_marker = |k: &str| k.starts_with("sessions/") && k.ends_with(".json");
    let mut claims = Claims::new(report);

    // F1's residual (phase F part 10d): a compact checks its marker before
    // each head create and each batch of deletes, so what is left is a stall
    // over the session staleness between that check and the head create it
    // guards, the marker reaped meanwhile. The late create lands and names
    // the compact's own repack outputs another client deleted as old orphans
    // after the reap. Claimed: absent outputs and the restores that need
    // them; nothing else.
    // Ages on the store's clock of each record (`OpRecord::at`), which the
    // trace clock equals in a driver run and which a scripted run has too.
    let mut aged_marker_reaps: Vec<(u64, ClientId, Option<usize>)> = Vec::new();
    {
        let mut written: BTreeMap<&str, (ClientId, Option<usize>, DateTime<Utc>)> = BTreeMap::new();
        for r in log {
            if r.op.is_write()
                && r.applied()
                && r.key.starts_with("sessions/")
                && r.key.ends_with(".json")
            {
                written.insert(&r.key, (r.client, r.run, r.at));
            }
            for k in r.deleted_keys() {
                if let Some(&(w, run, at)) = written.get(k)
                    && w != r.client
                    && (r.at - at).num_seconds() > STALE_SESSION_SECS
                {
                    aged_marker_reaps.push((r.step, w, run));
                }
            }
        }
    }
    for &(reap_step, w, run) in &aged_marker_reaps {
        let Some(victim) = run
            .and_then(|i| runs.iter().find(|x| x.index == i))
            .filter(|x| x.shared && matches!(x.op, ClientOp::Compact))
        else {
            continue;
        };
        let mine = |r: &OpRecord| r.client == w && r.run == Some(victim.index);
        let Some(h) = log
            .iter()
            .find(|r| {
                r.step > reap_step
                    && mine(r)
                    && r.op == OpKind::PutIfAbsent
                    && r.applied()
                    && r.key.starts_with("index.h/")
            })
            .map(|r| r.step)
        else {
            continue;
        };
        let outputs: BTreeSet<&str> = log
            .iter()
            .filter(|r| mine(r) && r.op.is_write() && r.applied() && r.key.starts_with("packs/"))
            .map(|r| r.key.as_str())
            .collect();
        let deleted: BTreeSet<&str> = log
            .iter()
            .filter(|r| r.step > reap_step && r.step < h && r.client != w)
            .flat_map(|r| r.deleted_keys())
            .filter(|k| outputs.contains(k))
            .collect();
        claims.claim(Signature::AgedSessionReaped, |v| match v.kind {
            ViolationKind::IndexNamesAbsentPack => deleted.contains(v.subject.as_str()),
            _ if v.invariant == Invariant::CommittedSnapshotsRestore => {
                deleted.iter().any(|p| v.detail.contains(p))
            }
            _ => false,
        });
    }

    // Session markers deleted by another client while their writer's run
    // was still in progress: the victim run's disallowed error and the
    // violations of its snapshot.
    // Young or aged by the store clock of each record (`OpRecord::at`).
    let mut marker_writes: BTreeMap<&str, (ClientId, Option<usize>, DateTime<Utc>)> =
        BTreeMap::new();
    for r in log {
        if r.op.is_write()
            && r.applied()
            && r.key.starts_with("sessions/")
            && r.key.ends_with(".json")
        {
            marker_writes.insert(&r.key, (r.client, r.run, r.at));
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
            let young = (r.at - written).num_seconds() <= STALE_SESSION_SECS;
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

    // A backup's pack deleted by a run that found the backup's session stale
    // by age (phase F part 10d, fix rounds 2 and 3). The deleting run R
    // judged sessions as production does: at its last `sessions/` listing
    // with mtimes L before the delete (a plain listing, such as the sweep's
    // look for committing sessions, judges no age), the victim's newest
    // marker key was older than the session staleness at R's "now"
    // (`sessions::reap_sessions` judges a session by its newest key, against
    // the mtime of R's own newest marker key, written before L), so no
    // horizon held the
    // victim's packs, and they were orphans past `PACK_AGE_LIMIT`. Who deleted the victim's stale keys does not matter:
    // R itself (seed `aged-session-reaped-mark-after-listing-lockfree`), or
    // an earlier run. The victim then wrote a
    // key after L (its commit mark, or a refresh after a stall), present at
    // the delete: invariant 10's `LiveSessionPackDeleted`. Its record has a
    // gap over `MARKER_GAP_LIMIT` and its elapsed time is past
    // `PACK_AGE_LIMIT`, so every commit check refuses it (I8) before a head.
    // Claimed: those deletes, by any run, of a backup run that
    // wrote no snapshot, while every marker key of the victim present at the
    // delete was written after L; nothing else.
    let pack_age_limit = chrono::Duration::from_std(crate::repo::ages::PACK_AGE_LIMIT)
        .expect("PACK_AGE_LIMIT fits a chrono duration");
    let stale_at_listing = |v: &Violation| -> bool {
        if v.kind != ViolationKind::LiveSessionPackDeleted {
            return false;
        }
        let mut words = v.subject.splitn(3, ' ');
        let (Some("run"), Some(victim), Some(pack)) = (words.next(), words.next(), words.next())
        else {
            return false;
        };
        let Ok(victim) = victim.parse::<usize>() else {
            return false;
        };
        let Some(at) = v.step else { return false };
        let Some(deleter) = log
            .iter()
            .find(|r| r.step == at && r.deleted_keys().contains(&pack))
            .and_then(|r| r.run)
        else {
            return false;
        };
        if !runs
            .iter()
            .any(|x| x.index == victim && matches!(x.op, ClientOp::Backup { .. }))
        {
            return false;
        }
        let mine = |r: &&OpRecord, run: usize| r.run == Some(run) && r.op.is_write() && r.applied();
        if log
            .iter()
            .any(|r| mine(&r, victim) && r.key.starts_with("snapshots/"))
        {
            return false;
        }
        let Some(listing) = log.iter().rfind(|r| {
            r.step < at
                && r.run == Some(deleter)
                && r.op == OpKind::ListMeta
                && r.key == "sessions/"
                && r.outcome == OpOutcome::Ok
        }) else {
            return false;
        };
        let before = |r: &&OpRecord| r.step < listing.step;
        // The deleter's "now": its newest marker key.
        let now = log
            .iter()
            .filter(before)
            .rfind(|r| mine(r, deleter) && is_marker(&r.key))
            .map(|r| r.at);
        let Some(now) = now else { return false };
        // The victim's newest marker key before the listing, stale there.
        let Some(newest) = log
            .iter()
            .filter(before)
            .rfind(|r| mine(r, victim) && is_marker(&r.key))
        else {
            return false;
        };
        if (now - newest.at).num_seconds() <= STALE_SESSION_SECS {
            return false;
        }
        // The pack was past the pack-age limit at that "now".
        let Some(put) = log
            .iter()
            .rfind(|r| r.step < at && r.op.is_write() && r.applied() && r.key == pack)
        else {
            return false;
        };
        if now - put.at < pack_age_limit {
            return false;
        }
        // Every marker key of the victim present at the delete was written
        // after the listing.
        let mut present: BTreeMap<&str, u64> = BTreeMap::new();
        for r in log.iter().filter(|r| r.step < at && r.applied()) {
            if mine(&r, victim) && is_marker(&r.key) {
                present.insert(&r.key, r.step);
            }
            for k in r.deleted_keys() {
                present.remove(k);
            }
        }
        present.values().all(|&w| w > listing.step)
    };
    claims.claim(Signature::AgedSessionReaped, stale_at_listing);

    // The single-writer residual: run r's plain head PUT p, issued and
    // applied at once, after r's own EXISTS e of that key found it absent,
    // while another run wrote the key between e and p. Only with the stall
    // and reap that make it the residual: between e and p another client
    // deleted a marker key r wrote, over the session staleness after r's
    // last marker write before that delete. Claims p's `HeadOverwritten`,
    // the replaced head's snapshot's lost coverage, the absent packs the
    // replaced head's run deleted before p, and a compact's refusal to mark
    // that snapshot.
    for p in log.iter().filter(|p| {
        p.op == OpKind::Put && p.key.starts_with("index.h/") && p.outcome == OpOutcome::Ok
    }) {
        let Some(r) = p.run else { continue };
        let Some(e) = log.iter().rfind(|x| {
            x.step < p.step
                && x.run == Some(r)
                && x.op == OpKind::Exists
                && x.key == p.key
                && x.outcome == OpOutcome::NotFound
        }) else {
            continue;
        };
        let Some(w) = log.iter().rfind(|x| {
            x.step > e.step
                && x.step < p.step
                && x.op.is_write()
                && x.applied()
                && x.key == p.key
                && x.run != p.run
        }) else {
            continue;
        };
        let reaped = log
            .iter()
            .filter(|d| d.step > e.step && d.step < p.step && d.client != p.client)
            .any(|d| {
                d.deleted_keys().iter().any(|k| {
                    let own = log.iter().any(|m| {
                        m.step < d.step
                            && m.run == Some(r)
                            && m.op.is_write()
                            && m.applied()
                            && m.key == *k
                    });
                    let last = log
                        .iter()
                        .filter(|m| {
                            m.step < d.step
                                && m.run == Some(r)
                                && m.op.is_write()
                                && m.applied()
                                && is_marker(&m.key)
                        })
                        .map(|m| m.at)
                        .max();
                    is_marker(k)
                        && own
                        && last.is_some_and(|l| (d.at - l).num_seconds() > STALE_SESSION_SECS)
                })
            });
        if !reaped {
            continue;
        }
        let snapshot = snapshot_of(runs, w.run);
        let deleted: BTreeSet<&str> = log
            .iter()
            .filter(|d| d.step < p.step && w.run.is_some() && d.run == w.run)
            .flat_map(|d| d.deleted_keys())
            .filter(|k| k.starts_with("packs/"))
            .collect();
        claims.claim(Signature::SingleWriterHeadRace, |v| match v.kind {
            ViolationKind::HeadOverwritten => v.step == Some(p.step),
            ViolationKind::TreeChunkNotInIndex | ViolationKind::FileChunkNotInIndex => {
                snapshot == Some(v.subject.as_str())
            }
            ViolationKind::IndexNamesAbsentPack => deleted.contains(v.subject.as_str()),
            ViolationKind::WedgedCompact => {
                snapshot.is_some_and(|s| v.detail.contains(&format!("snapshot '{s} (")))
            }
            _ => false,
        });
    }

    // A released head write cascades from its step when it left a
    // committed snapshot without index coverage, or the index naming an
    // absent pack. Committed: its backup returned Ok, or its `snapshots/`
    // PUT applied and either came before the release, or was not itself a
    // release and the late head overwrote the backup's own head (a backup
    // that crashed or failed after its snapshot PUT).
    let late_head = first_released_write("index.h/");
    if let Some(hazard) = late_head {
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
                    .filter_map(|r| snapshot_of(runs, r.run)),
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
    // A released snapshot PUT of run `late`: the violations of the snapshot
    // it wrote, when the case has no invariant-13 violation.
    let no_exclusive_violation = !report.violated(Invariant::ExclusiveSessions);
    for r in log
        .iter()
        .filter(|r| r.op.is_write() && released(r) && r.key.starts_with("snapshots/"))
    {
        let Some(snapshot) = snapshot_of(runs, r.run) else {
            continue;
        };
        if no_exclusive_violation
            && report.violated_for(Invariant::CommittedSnapshotsRestore, snapshot)
        {
            claims.claim(Signature::LateSnapshotWrite, |v| v.subject == *snapshot);
        }
    }

    // An exclusive run's write after another client deleted its live
    // exclusive marker: the run's invariant-13 violations, when the delete
    // precedes the first of them.
    let exclusive_violation = |v: &Violation| {
        let run = v
            .subject
            .strip_prefix("run ")
            .and_then(|n| n.parse::<usize>().ok());
        match (v.kind, v.step, run) {
            (
                ViolationKind::ExclusiveWriteWithoutHold
                | ViolationKind::ExclusiveWriteUnderOtherHold
                | ViolationKind::ExclusiveWriteUnderSeveralHolds,
                Some(s),
                Some(run),
            ) => Some((s, run)),
            _ => None,
        }
    };
    let mut first_violation: BTreeMap<usize, u64> = BTreeMap::new();
    for (s, run) in report.violations.iter().filter_map(exclusive_violation) {
        first_violation
            .entry(run)
            .and_modify(|f| *f = (*f).min(s))
            .or_insert(s);
    }
    for (i, v) in report.violations.iter().enumerate() {
        let Some((_, run)) = exclusive_violation(v) else {
            continue;
        };
        let s = first_violation[&run];
        let in_progress = |at: u64| {
            runs.iter()
                .find(|x| x.index == run)
                .is_none_or(|x| x.last_step > at)
        };
        let reaped = log.iter().filter(|d| d.step < s).any(|d| {
            d.deleted_keys().iter().any(|k| {
                is_exclusive_marker(k)
                    && log.iter().any(|w| {
                        w.step < d.step
                            && w.run == Some(run)
                            && w.client != d.client
                            && w.op.is_write()
                            && w.applied()
                            && w.key == *k
                    })
            }) && in_progress(d.step)
        });
        if reaped {
            claims.claim_index(Signature::ExclusiveSessionReaped, i);
        }
    }

    claims.finish()
}

/// An exclusive session's marker key (`sessions/x-*.json`).
fn is_exclusive_marker(key: &str) -> bool {
    crate::repo::sessions::parse_session_marker_key(key)
        .is_some_and(|(id, _)| crate::repo::sessions::is_exclusive_session(id))
}
