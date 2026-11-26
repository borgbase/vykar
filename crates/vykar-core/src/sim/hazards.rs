//! Known protocol hazards, each as a scenario that reproduces it, and the
//! regressions they turn into once fixed.
//!
//! The scenarios that need two clients in flight at once are in
//! [`interleavings`](super::interleavings).
//!
//! A hazard's test asserts that it still reproduces. When a fix lands, the
//! scenario reports `NotReproduced` and the entry moves from `KNOWN_HAZARDS`
//! to `REGRESSIONS`, where the same scenario must then hold. 05 step 8 moved
//! H5 (the mark refuses) and H4-segment (index garbage is collected by age)
//! there; 04 step 1 moved H1 and H1-session (installation ID); 04 step 2
//! moved H2-marker (non-resurrecting marker refresh). 04 step 5 (ages on
//! the store's clock) moved H4's skew route there (H4-skew) and kept H4 on
//! a store clock step; H3-compact-young is a regression, and H3-compact and
//! H5-repair now need orphans older than T_pack. 10-single-protocol step 1
//! (repair re-indexes the chunks the index lost) moved H5-repair there.
//!
//! 04 step 8 (put-if-absent heads) closes H3, H3-compact and H4 on stores
//! that honour put-if-absent: each hazard states what it does on the
//! conditional store ([`OnConditional`]); the scenarios run there under
//! [`with_store_kind`], and their conditional twins are regressions
//! (H3-conditional, H3-compact-conditional). Every regression names the
//! store kinds it holds on. Since 10-single-protocol step 2 every scenario
//! runs on an encrypted repository, so the fallback store means lock mode
//! with the marker handshake and the conditional store means lock-free
//! clients, which take no lock: the lock hazards have no prerequisite
//! there, and the lock regressions hold on the fallback store only. H-racy is a known hazard of
//! a store that passes the sequential check yet lets a late create win.
//!
//! The driver (`sim::driver`) finds these hazards, and more of today's
//! protocol, under random schedules; the shapes of damage it recognises are
//! documented on `driver::Signature`, each with the plan that removes it.

use std::collections::BTreeSet;

use super::checker::{Invariant, Report, ViolationKind, head_object_keys};
use super::fault::{ClientId, FaultAction, FaultRule, KeyMatch, OpMatch};
use super::gate::GateRule;
use super::history::{AllowedError, ErrorClass};
use super::interleavings;
use super::runner::{Call, GATE_TIMEOUT, Sim, SourceTree, with_store_kind};
use super::store::{SimConfig, StoreKind};

#[derive(Clone, Debug)]
pub enum HazardOutcome {
    /// The violation occurs; `evidence` says what was seen.
    Reproduced { evidence: String },
    /// The scenario ran as designed, but the violation no longer occurs.
    NotReproduced { evidence: String },
    /// A precondition failed (the fault did not fire, a control run failed):
    /// fix the scenario.
    ScenarioBroken { reason: String },
    /// The hazard this scenario builds on no longer reproduces, so there is
    /// nothing to test. Says nothing about whether this hazard is fixed.
    PrerequisiteNotReproduced {
        prerequisite: &'static str,
        evidence: String,
    },
}

/// What a known hazard does on a store that honours put-if-absent
/// ([`StoreKind::Conditional`]); `run` reproduces it on the fallback
/// ([`StoreKind::Plain`]).
#[derive(Clone, Copy, Debug)]
pub enum OnConditional {
    /// It reproduces there too: the lock still carries it.
    Reproduces,
    /// It does not reproduce; the regression of this id holds there.
    Closed(&'static str),
    /// Its prerequisite does not reproduce there.
    NoPrerequisite,
    /// The scenario sets its own store kind.
    OwnKind,
}

pub struct KnownHazard {
    pub id: &'static str,
    pub summary: &'static str,
    /// The plan expected to remove it.
    pub fixed_by: &'static str,
    pub run: fn() -> HazardOutcome,
    pub conditional: OnConditional,
}

/// The fallback and the conditional store.
pub const BOTH_KINDS: &[StoreKind] = &[StoreKind::Plain, StoreKind::Conditional];

/// Scenarios that once reproduced a hazard and must now hold, on each of
/// `kinds` (`run` under [`with_store_kind`]).
pub struct Regression {
    pub id: &'static str,
    pub summary: &'static str,
    pub run: fn() -> Report,
    pub kinds: &'static [StoreKind],
}

pub const KNOWN_HAZARDS: &[KnownHazard] = &[
    KnownHazard {
        id: "H3",
        summary: "head PUT times out and lands after the lock is released; \
                  B's committed entries are dropped",
        fixed_by: "04-commit-protocol.md step 8 on conditional backends (the late create \
                   finds its generation taken); the fallback keeps it",
        run: h3_late_head_put,
        conditional: OnConditional::Closed("H3-conditional"),
    },
    KnownHazard {
        id: "H3-compact",
        summary: "head PUT times out, a compact runs, then the stale head lands \
                  over compact's head and names the packs compact deleted as orphans",
        fixed_by: "04-commit-protocol.md step 8 on conditional backends; the fallback \
                   keeps it; step 5 narrowed it to orphans older than T_pack",
        run: h3_compact_then_late_head,
        conditional: OnConditional::Closed("H3-compact-conditional"),
    },
    KnownHazard {
        id: "H1-late-clone",
        summary: "a clone of the installation that booted after the holder's last write \
                  takes a live lock for one of its earlier boot and reaps it",
        fixed_by: "not planned: a history of this installation's own boot IDs; \
                   04 step 5 narrows it for markers",
        run: interleavings::h1_late_clone,
        // Lock-free clients take no lock (10-single-protocol step 2: the
        // conditional store's clients are encrypted, so lock-free).
        conditional: OnConditional::NoPrerequisite,
    },
    KnownHazard {
        id: "H2",
        summary: "lock refresh recreates a reaped key: two holders, both fences pass \
                  (route: the store's clock steps 3 h forward)",
        fixed_by: "04-commit-protocol.md step 2 (non-resurrecting lock refresh, fallback only)",
        run: interleavings::h2_lock_refresh_resurrects,
        // Lock-free clients take no lock (10-single-protocol step 2).
        conditional: OnConditional::NoPrerequisite,
    },
    KnownHazard {
        id: "H4",
        summary: "compact loses its lock, deletes sources with no fence after its head \
                  PUT, and a stale head names the deleted packs (route: the store's \
                  clock steps 6 h 1 min forward)",
        fixed_by: "04-commit-protocol.md step 8 on conditional backends (compact's \
                   and the backup's creates of one generation: one wins, the backup \
                   rebases); the fallback keeps it",
        run: interleavings::h4_compact_deletes_after_lost_lock,
        // Lock-free clients take no lock (10-single-protocol step 2); the
        // regression H4-conditional, of a lock-mode compact losing its lock
        // on that store, went with the unencrypted template.
        conditional: OnConditional::NoPrerequisite,
    },
    KnownHazard {
        id: "H-racy",
        summary: "a store that passes the sequential put-if-absent check but lets a late \
                  create overwrite (IDrive e2): two writers both believe they created \
                  the generation; a head is replaced (invariant 12)",
        fixed_by: "not planned: s3_conditional_writes: on is the user's assertion \
                   (03 step 4 open decision); harmless while the lock is held",
        run: h_racy,
        conditional: OnConditional::OwnKind,
    },
    KnownHazard {
        id: "H-mixed-aged-lock",
        summary: "mixed modes on a store whose plain PUT replaces heads (local, SFTP, S3): \
                  a lock-mode backup stalls 6 h between its fence and its head PUT, a \
                  lock-free backup reaps the aged lock and creates the generation, and the \
                  late plain head PUT replaces it (invariant 12)",
        fixed_by: "not planned: the lock's documented limit (AgedLockReaped, R4 of phase F \
                   part 10); only a lock-mode writer's own stall of 6 h opens it",
        run: h_mixed_aged_lock,
        conditional: OnConditional::OwnKind,
    },
    KnownHazard {
        id: "H-committing-reaped",
        summary: "a lock-free backup that reused chunks of a deleted snapshot stalls 46 min \
                  after its marker probe, before its snapshot PUT (its heartbeat failing): a \
                  lock-free compact reaps its committing marker, finds no session \
                  committing, sweeps the reused chunks and deletes their packs; the late \
                  snapshot PUT commits a snapshot that does not restore (invariant 1)",
        fixed_by: "not planned: the marker's documented limit (R1 of phase F part 10, \
                   AgedSessionReaped); the post-head marker probe narrows it to a stall \
                   after that probe",
        run: h_committing_reaped,
        conditional: OnConditional::OwnKind,
    },
    KnownHazard {
        id: "H-mixed-late-create",
        summary: "mixed modes on a store whose plain PUT replaces heads (local, SFTP, S3): \
                  a lock-free backup's head create times out and the backup fails and \
                  deregisters without a snapshot; the abandoned create lands after a \
                  lock-mode backup read the head, whose plain PUT then replaces it \
                  (invariant 12); nothing committed is damaged",
        fixed_by: "not planned: the lock protocol's fallback beside lock-free writers \
                   (LateHeadCreateReplaced, phase F part 10c); the failed run's objects \
                   are debris",
        run: h_mixed_late_create,
        conditional: OnConditional::OwnKind,
    },
];

pub const REGRESSIONS: &[Regression] = &[
    Regression {
        id: "H5",
        summary: "after a lost update, compact refuses (the mark fails) and deletes \
                  none of the unindexed packs",
        run: h5_regression,
        // Built on H3's lost update, which a conditional store does not
        // produce (H3-conditional).
        kinds: &[StoreKind::Plain],
    },
    Regression {
        id: "H5-repair",
        summary: "after a lost update, check --repair re-indexes the snapshot from the packs \
                  instead of removing it; a compact T_pack later keeps its packs and it restores",
        run: h5_repair_regression,
        // Built on H3's lost update (fallback store only).
        kinds: &[StoreKind::Plain],
    },
    Regression {
        id: "H1",
        summary: "same hostname: a live lock is kept by cloned machines, namesakes (also \
                  one booted after the holder's write) and containers (installation ID, \
                  boot ID and uptime, PID namespace)",
        run: interleavings::h1_regression,
        // Its subject is a lock, which lock-free clients never take.
        kinds: &[StoreKind::Plain],
    },
    Regression {
        id: "H1-session",
        summary: "same hostname: a live backup's marker is kept; compact proceeds beside it \
                  and keeps its uploads",
        run: interleavings::h1_session_regression,
        kinds: BOTH_KINDS,
    },
    Regression {
        id: "H2-marker",
        summary: "a reaped session marker is never written again; the refresh finds it \
                  missing; the backup commits while its uploads are young (I8) and its \
                  snapshot restores",
        run: interleavings::h2_marker_regression,
        kinds: BOTH_KINDS,
    },
    Regression {
        id: "H4-segment",
        summary: "compact upkeep no longer deletes a concurrent writer's fresh segment \
                  (age-gated GC)",
        run: interleavings::h4_segment_regression,
        kinds: BOTH_KINDS,
    },
    Regression {
        id: "H4-skew",
        summary: "a client 6 h 1 min ahead no longer reaps compact's live lock \
                  (store-clock ages)",
        run: interleavings::h4_skew_regression,
        // Its subject is a lock, which lock-free clients never take.
        kinds: &[StoreKind::Plain],
    },
    Regression {
        id: "H3-compact-young",
        summary: "a compact soon after a backup lost its head PUT keeps the backup's \
                  orphan packs (younger than T_pack); the late head names present packs",
        run: h3_compact_young_regression,
        kinds: BOTH_KINDS,
    },
    Regression {
        id: "H3-conditional",
        summary: "on a conditional store the late head finds its generation taken \
                  (released-refused); b1 keeps its entries",
        run: h3_conditional_regression,
        kinds: &[StoreKind::Conditional],
    },
    Regression {
        id: "H3-compact-conditional",
        summary: "on a conditional store the late head cannot replace compact's head; the \
                  index names no pack compact deleted",
        run: h3_compact_conditional_regression,
        kinds: &[StoreKind::Conditional],
    },
];

/// Panics unless `id` is a known hazard that reproduces.
pub fn assert_reproduces(id: &str) {
    let hazard = KNOWN_HAZARDS
        .iter()
        .find(|h| h.id == id)
        .unwrap_or_else(|| panic!("{id} is not in KNOWN_HAZARDS"));
    match with_store_kind(StoreKind::Plain, hazard.run) {
        HazardOutcome::Reproduced { .. } => {}
        HazardOutcome::NotReproduced { evidence } => panic!(
            "{id} no longer reproduces: move it from KNOWN_HAZARDS to REGRESSIONS \
             (expected fixed by {}).\n{evidence}",
            hazard.fixed_by
        ),
        HazardOutcome::ScenarioBroken { reason } => {
            panic!("{id}: scenario broken, fix the scenario.\n{reason}")
        }
        HazardOutcome::PrerequisiteNotReproduced {
            prerequisite,
            evidence,
        } => panic!(
            "{id} cannot run: {prerequisite} no longer reproduces, so there is no \
             damage to build on. This does not show {id} fixed; rebuild {id} on \
             another route to the damage or retire it.\n{evidence}"
        ),
    }
}

pub fn assert_regression_holds(id: &str) {
    let regression = REGRESSIONS
        .iter()
        .find(|r| r.id == id)
        .unwrap_or_else(|| panic!("{id} is not in REGRESSIONS"));
    for &kind in regression.kinds {
        let report = with_store_kind(kind, regression.run);
        assert!(
            report.is_clean(),
            "regression {id} ({}) fails on {kind:?}:\n{report}",
            regression.summary
        );
    }
}

/// Panics unless known hazard `id` does on a conditional store what its
/// [`OnConditional`] says.
pub fn assert_conditional_expectation(id: &str) {
    let hazard = KNOWN_HAZARDS
        .iter()
        .find(|h| h.id == id)
        .unwrap_or_else(|| panic!("{id} is not in KNOWN_HAZARDS"));
    let outcome = with_store_kind(StoreKind::Conditional, hazard.run);
    let ok = match hazard.conditional {
        OnConditional::Reproduces | OnConditional::OwnKind => {
            matches!(outcome, HazardOutcome::Reproduced { .. })
        }
        OnConditional::Closed(regression) => {
            assert!(
                REGRESSIONS.iter().any(|r| r.id == regression),
                "{id}: its conditional regression {regression} is not in REGRESSIONS"
            );
            matches!(outcome, HazardOutcome::NotReproduced { .. })
        }
        OnConditional::NoPrerequisite => {
            matches!(outcome, HazardOutcome::PrerequisiteNotReproduced { .. })
        }
    };
    assert!(
        ok,
        "{id} on a conditional store: expected {:?}, got {outcome:?}",
        hazard.conditional
    );
}

const FILE_SIZE: usize = 64 * 1024;

type Setup = (Sim, ClientId, ClientId, ClientId);

/// When `a`'s parked head PUT lands. Only `AfterB` is the hazard; the others
/// neutralise it through the harness, so a test can show what a scenario
/// reports once the hazard is gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LateHead {
    /// After the next client commits on top of the old head.
    AfterB,
    /// Before `b` starts: harmless, nothing has committed since `a`.
    BeforeB,
    /// Never: the parked effect is dropped.
    Never,
}

pub(super) fn broken(reason: impl Into<String>) -> HazardOutcome {
    HazardOutcome::ScenarioBroken {
        reason: reason.into(),
    }
}

/// The highest `index.h/<gen>` key in the store.
pub(super) fn latest_head_key(sim: &Sim) -> Option<String> {
    sim.store()
        .keys("index.h/")
        .into_iter()
        .filter_map(|k| Some((vykar_protocol::head_generation(&k)?, k)))
        .max_by_key(|(g, _)| *g)
        .map(|(_, k)| k)
}

/// `a` backs up `a1` with its head PUT (under `index.h/`) delayed, so the
/// commit fails after `a` stored its segment and released the lock.
fn park_a_head(sim: &mut Sim, a: ClientId) -> Result<(), String> {
    park_a_head_with(sim, a, None)
}

/// [`park_a_head`]; with `packs_aged`, a is held at its first `index.d/`
/// PUT, after its pack uploads, while the store's clock steps by that much:
/// a's packs are then that old on the store's clock and its segment is new.
fn park_a_head_with(
    sim: &mut Sim,
    a: ClientId,
    packs_aged: Option<chrono::Duration>,
) -> Result<(), String> {
    sim.fault(
        FaultRule::new(
            OpMatch::AnyWrite,
            KeyMatch::Prefix("index.h/".into()),
            FaultAction::Delay,
        )
        .client(a),
    );
    let tree = SourceTree::seeded("a1", 3, FILE_SIZE);
    let r = match packs_aged {
        None => sim.backup(a, "a1", &tree),
        Some(by) => {
            let gate = sim.gate(GateRule::new(
                a,
                OpMatch::AnyWrite,
                KeyMatch::Prefix("index.d/".into()),
            ));
            let run = sim.spawn(
                a,
                Call::Backup {
                    snapshot: "a1".into(),
                    tree,
                },
            );
            if sim.poll_paused(gate, GATE_TIMEOUT)?.is_none() {
                let r = sim.join(run);
                return Err(format!("a1 ended before its segment PUT: {}", r.detail()));
            }
            sim.step_store_clock(by);
            sim.resume(gate);
            sim.join(run)
        }
    };
    if r.error_class() != Some(&ErrorClass::Allowed(AllowedError::Injected)) {
        return Err(format!(
            "a1 should fail with the injected fault, got {:?}: {}\n\
             If the commit point no longer PUTs a head under `index.h/`, \
             retarget the fault rule to the new commit key before reading \
             anything into this outcome.",
            r.outcome,
            r.detail()
        ));
    }
    let parked = sim.store().parked();
    if parked.len() != 1 || !parked[0].key.starts_with("index.h/") || parked[0].client != a {
        return Err(format!("expected a's head PUT parked, got {parked:?}"));
    }
    let at = parked[0].issued_at_step;
    let segment_first = sim.store().log().iter().any(|r| {
        r.client == a
            && r.step < at
            && r.op.is_write()
            && r.applied()
            && r.key.starts_with("index.d/")
    });
    if !segment_first {
        return Err("a wrote no index.d/ object before its head PUT was parked".into());
    }
    Ok(())
}

/// Apply or drop every parked effect, as `late` says.
fn land(sim: &mut Sim, late: LateHead) {
    match late {
        LateHead::AfterB | LateHead::BeforeB => {
            sim.release_all();
        }
        LateHead::Never => {
            for p in sim.store().parked() {
                sim.discard(p.id);
            }
        }
    }
}

/// H3 up to and including the late landing:
///
/// 1. `c` backs up `c0` (the control snapshot).
/// 2. `a`'s write of its head under `index.h/` is delayed, so `a1`'s commit fails
///    after its segment is stored and the lock is released.
/// 3. `b` backs up `b1` and commits on top of the old head.
/// 4. The repository is consistent; then `a`'s head PUT lands.
///
/// `late` moves or drops the landing of step 4.
fn h3_setup(late: LateHead) -> Result<Setup, String> {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));

    let r = sim.backup(c, "c0", &SourceTree::seeded("c0", 3, FILE_SIZE));
    if !r.is_ok() {
        return Err(format!("control backup c0 failed: {}", r.detail()));
    }

    park_a_head(&mut sim, a)?;
    if late == LateHead::BeforeB {
        land(&mut sim, late);
    }

    let r = sim.backup(b, "b1", &SourceTree::seeded("b1", 3, FILE_SIZE));
    if !r.is_ok() {
        return Err(format!("b1 should commit: {}", r.detail()));
    }

    let before = sim.check_invariants(Invariant::STEP1);
    if !before.is_clean() {
        return Err(format!(
            "repository not consistent before the release:\n{before}"
        ));
    }

    if late != LateHead::BeforeB {
        land(&mut sim, late);
    }
    Ok((sim, a, b, c))
}

/// Controls that hold whether or not the hazard reproduces: `c0` intact, no
/// acknowledged commit lost or silently removed, no disallowed error.
pub(super) fn base_controls(report: &Report) -> Result<(), String> {
    if report.violated_for(Invariant::CommittedSnapshotsRestore, "c0") {
        return Err(format!("control c0 damaged:\n{report}"));
    }
    for inv in [
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::AllowedErrors,
    ] {
        if report.violated(inv) {
            return Err(format!("{inv:?} violated, expected to hold:\n{report}"));
        }
    }
    Ok(())
}

/// `b1` lost its index entries: some of its tree chunks are unindexed.
fn b1_unindexed(report: &Report) -> bool {
    report.has_kind(ViolationKind::TreeChunkNotInIndex, "b1")
}

/// `b1` restores and the index holds together.
fn b1_healthy(report: &Report) -> bool {
    !report.violated_for(Invariant::CommittedSnapshotsRestore, "b1")
        && !report.violated(Invariant::IndexIntegrity)
}

/// Neither the expected signature nor healthy.
pub(super) fn different_damage(evidence: impl std::fmt::Display) -> HazardOutcome {
    broken(format!("damage of a different shape:\n{evidence}"))
}

/// H3: the late head PUT drops `b1`'s index entries.
pub fn h3_late_head_put() -> HazardOutcome {
    h3_with(LateHead::AfterB)
}

/// H3 with the landing of `a`'s head PUT set by `late`.
pub fn h3_with(late: LateHead) -> HazardOutcome {
    let (sim, a, b, _) = match h3_setup(late) {
        Ok(s) => s,
        Err(e) => return broken(e),
    };
    let report = sim.check_invariants(Invariant::STEP1);
    if let Err(e) = base_controls(&report) {
        return broken(e);
    }
    if !b1_unindexed(&report) {
        if !b1_healthy(&report) {
            return different_damage(&report);
        }
        return HazardOutcome::NotReproduced {
            evidence: format!("b1 is intact\n{report}"),
        };
    }

    // The mechanism: the chain is intact but is a's, and b's segment is no
    // longer part of it.
    if report.violated(Invariant::IndexIntegrity) {
        return broken(format!(
            "IndexIntegrity violated, expected to hold:\n{report}"
        ));
    }
    let writer = latest_head_key(&sim)
        .and_then(|k| sim.store().object(&k))
        .map(|o| o.writer);
    if writer != Some(a) {
        return broken(format!("head last written by {writer:?}, expected a"));
    }
    let b_segments = sim.store().keys_written_by(b, "index.d/");
    if b_segments.is_empty() {
        return broken("b wrote no index.d/ object");
    }
    let named = match head_object_keys(sim.store()) {
        Ok(n) => n,
        Err(e) => return broken(format!("head does not decode: {e}")),
    };
    let still_named: Vec<&String> = b_segments.iter().filter(|k| named.contains(*k)).collect();
    if !still_named.is_empty() {
        return broken(format!(
            "b1 unindexed, yet the head still names b's segment {still_named:?}\n{report}"
        ));
    }
    HazardOutcome::Reproduced {
        evidence: format!("{report}"),
    }
}

/// Whether `client`'s parked head create landed refused (it found its
/// generation taken): what closes H3 on a conditional store.
pub(super) fn late_head_refused(sim: &Sim, client: ClientId) -> bool {
    sim.store().log().iter().any(|r| {
        r.client == client
            && r.key.starts_with("index.h/")
            && matches!(r.outcome, super::log::OpOutcome::ReleasedRefused(_))
    })
}

/// H3-conditional: H3's schedule on a conditional store. a's late create
/// of the generation b created lands refused; STEP8 holds (invariant 12
/// and every retained head included).
pub fn h3_conditional_regression() -> Report {
    let mut out = Report::default();
    let (sim, a, _, _) = match h3_setup(LateHead::AfterB) {
        Ok(s) => s,
        Err(e) => {
            out.add(ViolationKind::DisallowedError, "setup", e);
            return out;
        }
    };
    if !late_head_refused(&sim, a) {
        out.add(
            ViolationKind::DisallowedError,
            "scenario",
            "a's late head create did not land refused",
        );
    }
    out.extend(sim.check_invariants(Invariant::STEP8));
    out
}

/// H-racy: on a store that passes the sequential check but lets a late
/// create overwrite (`StoreKind::RacyRelease`), H3's schedule replaces b's
/// head with a's: `HeadOverwritten` (invariant 12), and b1 unindexed.
pub fn h_racy() -> HazardOutcome {
    let mut sim = Sim::with_config(SimConfig {
        kind: StoreKind::RacyRelease,
        ..SimConfig::default()
    });
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    let r = sim.backup(c, "c0", &SourceTree::seeded("c0", 3, FILE_SIZE));
    if !r.is_ok() {
        return broken(format!("control backup c0 failed: {}", r.detail()));
    }
    if let Err(e) = park_a_head(&mut sim, a) {
        return broken(e);
    }
    let r = sim.backup(b, "b1", &SourceTree::seeded("b1", 3, FILE_SIZE));
    if !r.is_ok() {
        return broken(format!("b1 should commit: {}", r.detail()));
    }
    sim.release_all();
    let report = sim.check_invariants(Invariant::STEP8);
    let overwritten = report
        .violations
        .iter()
        .any(|v| v.kind == ViolationKind::HeadOverwritten);
    let evidence = format!("{report}");
    if overwritten && b1_unindexed(&report) {
        HazardOutcome::Reproduced { evidence }
    } else if report.is_clean() {
        HazardOutcome::NotReproduced { evidence }
    } else {
        different_damage(evidence)
    }
}

/// H-mixed-aged-lock (phase F part 10c, R4): on an encrypted repository
/// over `StoreKind::ConditionalOverwrite`, lock-mode backup l (its handle
/// offers no put-if-absent) is held after its fence, at its plain head
/// PUT; 6 h 1 min pass (its lock ages past the stale age); lock-free
/// backup f reaps the lock at registration, commits the generation l
/// computed; l's PUT then replaces f's head: `HeadOverwritten`, and f1
/// loses its index coverage.
pub fn h_mixed_aged_lock() -> HazardOutcome {
    let mut sim = Sim::with_config(SimConfig {
        kind: StoreKind::ConditionalOverwrite,
        ..SimConfig::default()
    });
    let (l, f) = (sim.client("l"), sim.client("f"));
    sim.set_lock_client(l);
    let gate = sim.gate(GateRule::new(
        l,
        OpMatch::Kind(super::fault::OpKind::Put),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let rl = sim.spawn(
        l,
        Call::Backup {
            snapshot: "l1".into(),
            tree: SourceTree::seeded("l1", 3, FILE_SIZE),
        },
    );
    if sim.poll_paused(gate, GATE_TIMEOUT).ok().flatten().is_none() {
        return broken(String::from("l never reached its head PUT"));
    }
    sim.advance(std::time::Duration::from_secs(6 * 3600 + 60));
    let rf = sim.backup(f, "f1", &SourceTree::seeded("f1", 3, FILE_SIZE));
    if !rf.is_ok() || !rf.lock_free {
        return broken(format!("f1 should commit lock-free: {}", rf.detail()));
    }
    sim.resume(gate);
    let rl = sim.join(rl);
    let report = sim.check_invariants(Invariant::STEP8);
    let overwritten = report
        .violations
        .iter()
        .any(|v| v.kind == ViolationKind::HeadOverwritten);
    let evidence = format!("l1: {}; {report}", rl.detail());
    if overwritten {
        HazardOutcome::Reproduced { evidence }
    } else if report.is_clean() {
        HazardOutcome::NotReproduced { evidence }
    } else {
        different_damage(evidence)
    }
}

/// H-committing-reaped (phase F part 10d, R1): on an encrypted repository
/// over `StoreKind::Conditional`, snapshot "old" is deleted and its segment
/// ages past T_pack; lock-free backup b of the same source again adds no
/// chunk (it reuses all of "old"'s), creates its head and is held after its
/// marker probe, at its snapshot PUT. Its heartbeat's next refresh fails
/// and 46 min pass, so its committing marker is stale on the store's clock:
/// a lock-free compact reaps it, its second session listing finds no
/// session committing, its mark (of "keep" only) finds "old"'s chunks
/// unreferenced, and it sweeps them and deletes their packs. b's snapshot
/// PUT then commits a snapshot that does not restore.
pub fn h_committing_reaped() -> HazardOutcome {
    let mut sim = Sim::with_config(SimConfig {
        kind: StoreKind::Conditional,
        ..SimConfig::default()
    });
    let (a, b) = (sim.client("a"), sim.client("b"));
    for name in ["old", "keep"] {
        let run = sim.backup(a, name, &SourceTree::seeded(name, 3, FILE_SIZE));
        if !run.is_ok() {
            return broken(format!("{name}: {}", run.detail()));
        }
    }
    if !sim.delete(a, &["old"]).is_ok() {
        return broken(String::from("the delete of old failed"));
    }
    sim.advance(std::time::Duration::from_secs(12 * 3600 + 60));
    let gate = sim.gate(GateRule::new(
        b,
        OpMatch::Kind(super::fault::OpKind::Put),
        KeyMatch::Prefix("snapshots/".into()),
    ));
    let rb = sim.spawn(
        b,
        Call::BackupSameSource {
            snapshot: "again".into(),
            of: "old".into(),
        },
    );
    if sim.poll_paused(gate, GATE_TIMEOUT).ok().flatten().is_none() {
        let rb = sim.join(rb);
        return broken(format!("b never reached its snapshot PUT: {}", rb.detail()));
    }
    sim.fault(
        FaultRule::new(
            OpMatch::Kind(super::fault::OpKind::Put),
            KeyMatch::Prefix("sessions/".into()),
            FaultAction::Fail,
        )
        .client(b),
    );
    sim.advance(std::time::Duration::from_secs(46 * 60));
    let rc = sim.compact(a);
    if !rc.is_ok() || !rc.lock_free {
        return broken(format!("the compact should run lock-free: {}", rc.detail()));
    }
    sim.resume(gate);
    let rb = sim.join(rb);
    if !rb.is_ok() || !rb.lock_free {
        return broken(format!("b should commit lock-free: {}", rb.detail()));
    }
    let report = sim.check_invariants(Invariant::STEP8);
    let lost = report
        .violations
        .iter()
        .any(|v| v.invariant == Invariant::CommittedSnapshotsRestore && v.subject == "again");
    let evidence = format!("compact: {}; {report}", rc.detail());
    if lost {
        HazardOutcome::Reproduced { evidence }
    } else if report.is_clean() {
        HazardOutcome::NotReproduced { evidence }
    } else {
        different_damage(evidence)
    }
}

/// H-mixed-late-create (phase F part 10c): on an encrypted repository over
/// `StoreKind::ConditionalOverwrite`, lock-free backup f's head create is
/// delayed (a request the client abandoned on a timeout); its GET finds the
/// key absent, so f fails with the error, deregistered and without a
/// snapshot. Lock-mode backup l (its handle offers no put-if-absent) lists
/// no session to yield to and is held at its plain head PUT; f's create
/// lands; l's PUT replaces it: `HeadOverwritten` (invariant 12) and
/// nothing else (l1 restores, f wrote no snapshot).
pub fn h_mixed_late_create() -> HazardOutcome {
    let mut sim = Sim::with_config(SimConfig {
        kind: StoreKind::ConditionalOverwrite,
        ..SimConfig::default()
    });
    let (f, l) = (sim.client("f"), sim.client("l"));
    sim.set_lock_client(l);
    sim.fault(
        FaultRule::new(
            OpMatch::Kind(super::fault::OpKind::PutIfAbsent),
            KeyMatch::Prefix("index.h/".into()),
            FaultAction::Delay,
        )
        .client(f),
    );
    let rf = sim.backup(f, "f1", &SourceTree::seeded("f1", 3, FILE_SIZE));
    if rf.error_class() != Some(&ErrorClass::Allowed(AllowedError::Injected)) {
        return broken(format!(
            "f1 should fail with the injected fault: {}",
            rf.detail()
        ));
    }
    let parked = sim.store().parked();
    if parked.len() != 1 || !parked[0].key.starts_with("index.h/") {
        return broken(format!("expected f's head create parked, got {parked:?}"));
    }
    if !sim.store().keys_written_by(f, "snapshots/").is_empty() {
        return broken("f wrote a snapshot");
    }
    let gate = sim.gate(GateRule::new(
        l,
        OpMatch::Kind(super::fault::OpKind::Put),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let rl = sim.spawn(
        l,
        Call::Backup {
            snapshot: "l1".into(),
            tree: SourceTree::seeded("l1", 3, FILE_SIZE),
        },
    );
    if sim.poll_paused(gate, GATE_TIMEOUT).ok().flatten().is_none() {
        let rl = sim.join(rl);
        return broken(format!("l never reached its head PUT: {}", rl.detail()));
    }
    sim.release_all();
    sim.resume(gate);
    let rl = sim.join(rl);
    if !rl.is_ok() || rl.lock_free {
        return broken(format!("l1 should commit under the lock: {}", rl.detail()));
    }
    let report = sim.check_invariants(Invariant::STEP8);
    let evidence = format!("{report}");
    let only_overwrite = !report.violations.is_empty()
        && report
            .violations
            .iter()
            .all(|v| v.kind == ViolationKind::HeadOverwritten);
    if only_overwrite {
        HazardOutcome::Reproduced { evidence }
    } else if report.is_clean() {
        HazardOutcome::NotReproduced { evidence }
    } else {
        different_damage(evidence)
    }
}

/// The pack keys `b` wrote; `Err` unless there are some and all are present.
fn b_packs_present(sim: &Sim, b: ClientId) -> Result<Vec<String>, String> {
    let packs = sim.store().keys_written_by(b, "packs/");
    if packs.is_empty() {
        return Err("b wrote no packs".into());
    }
    let missing: Vec<&String> = packs
        .iter()
        .filter(|k| sim.store().object(k).is_none())
        .collect();
    if !missing.is_empty() {
        return Err(format!("b's packs {missing:?} absent before compact"));
    }
    Ok(packs)
}

/// Keys `client` deleted during run `run`, from the op log. A delete of a
/// key that was already absent does not count.
pub(super) fn deleted_in_run(sim: &Sim, client: ClientId, run: usize) -> BTreeSet<String> {
    sim.store()
        .log()
        .iter()
        .filter(|r| r.client == client && r.run == Some(run))
        .flat_map(|r| {
            r.deleted_keys()
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// H3 has damaged `b1`, or the scenarios built on it have nothing to test.
fn lost_update(sim: &Sim) -> Result<(), HazardOutcome> {
    let report = sim.check_invariants(Invariant::STEP1);
    if let Err(e) = base_controls(&report) {
        return Err(broken(e));
    }
    if !b1_unindexed(&report) {
        if !b1_healthy(&report) {
            return Err(different_damage(&report));
        }
        return Err(HazardOutcome::PrerequisiteNotReproduced {
            prerequisite: "H3",
            evidence: format!("no lost update to build on: b1 is intact\n{report}"),
        });
    }
    Ok(())
}

/// H5 as a regression: after H3, compact by `c` must refuse (the mark finds
/// `b1`'s tree chunks missing from the index) and delete none of `b`'s
/// packs. The report holds the controls (`c0`, invariants 2, 3 and 8), a
/// `DisallowedError` unless the compact ended `Allowed(MarkRefused)`, and
/// `PackAbsent` for every pack of `b` that is gone. `b1`'s invariant-1
/// damage from H3 is deliberately not part of it.
pub fn h5_regression() -> Report {
    let mut out = Report::default();
    let (mut sim, _, b, c) = match h3_setup(LateHead::AfterB) {
        Ok(s) => s,
        Err(e) => {
            out.add(ViolationKind::DisallowedError, "setup", e);
            return out;
        }
    };
    let b_packs = sim.store().keys_written_by(b, "packs/");
    let run = sim.compact(c);
    let report = sim.check_invariants(Invariant::STEP1);
    for v in report.violations {
        let control = v.subject == "c0"
            || matches!(
                v.invariant,
                Invariant::AcknowledgedCommitsDurable
                    | Invariant::NoSilentRemoval
                    | Invariant::AllowedErrors
            );
        if control {
            out.add(v.kind, v.subject, v.detail);
        }
    }
    if run.error_class() != Some(&ErrorClass::Allowed(AllowedError::MarkRefused)) {
        out.add(
            ViolationKind::DisallowedError,
            format!("run {} compact", run.index),
            format!(
                "compact should refuse with the mark error: {:?}",
                run.outcome
            ),
        );
    }
    if b_packs.is_empty() {
        out.add(ViolationKind::PackAbsent, "b", "b wrote no packs");
    }
    for k in b_packs {
        if sim.store().object(&k).is_none() {
            out.add(ViolationKind::PackAbsent, "b", format!("pack {k} gone"));
        }
    }
    out
}

/// H5 on an H3 setup whose landing is set by `late`.
pub fn h5_with(late: LateHead) -> HazardOutcome {
    let (mut sim, _, b, c) = match h3_setup(late) {
        Ok(s) => s,
        Err(e) => return broken(e),
    };
    if let Err(outcome) = lost_update(&sim) {
        return outcome;
    }
    let b_packs = match b_packs_present(&sim, b) {
        Ok(p) => p,
        Err(e) => return broken(e),
    };
    let run = sim.compact(c);
    let refused = run.error_class() == Some(&ErrorClass::Allowed(AllowedError::MarkRefused));
    if !run.is_ok() && !refused {
        return broken(format!("compact failed: {}", run.detail()));
    }
    let report = sim.check_invariants(Invariant::STEP1);
    if let Err(e) = base_controls(&report) {
        return broken(e);
    }
    if report.violated(Invariant::IndexIntegrity) {
        return broken(format!(
            "IndexIntegrity violated, expected to hold:\n{report}"
        ));
    }
    let deleted = deleted_in_run(&sim, c, run.index);
    let gone: Vec<&String> = b_packs
        .iter()
        .filter(|k| sim.store().object(k).is_none())
        .collect();
    let by_compact = b_packs.iter().all(|k| deleted.contains(k));
    let evidence = format!(
        "compact: {}\nb's packs {b_packs:?}, gone {gone:?}, all deleted by compact: \
         {by_compact}\n{report}",
        run.detail()
    );
    if b1_unindexed(&report) && gone.len() == b_packs.len() && by_compact {
        HazardOutcome::Reproduced { evidence }
    } else if refused && gone.is_empty() {
        HazardOutcome::NotReproduced { evidence }
    } else {
        different_damage(evidence)
    }
}

/// H5-repair as a regression: after H3, `check --repair` by `c` must
/// re-index b1 (`ReindexChunks`) and remove no snapshot, and the compact
/// T_pack + 1 min later must succeed. The report holds every violation of
/// the step-1 invariants afterwards (b1 and c0 restore, invariants 2 and 3,
/// `IndexIntegrity`, `AllowedErrors`), a `DisallowedError` for a failed
/// or removing repair or compact, and `PackAbsent` for every pack of `b`
/// that is gone.
pub fn h5_repair_regression() -> Report {
    let mut out = Report::default();
    let (mut sim, _, b, c) = match h3_setup(LateHead::AfterB) {
        Ok(s) => s,
        Err(e) => {
            out.add(ViolationKind::DisallowedError, "setup", e);
            return out;
        }
    };
    if !b1_unindexed(&sim.check_invariants(Invariant::STEP1)) {
        out.add(
            ViolationKind::DisallowedError,
            "setup",
            "setup: no lost update",
        );
        return out;
    }
    let b_packs = sim.store().keys_written_by(b, "packs/");
    let repair = sim.check_repair(c);
    if !repair.is_ok() {
        out.add(
            ViolationKind::DisallowedError,
            format!("run {} check --repair", repair.index),
            format!("repair failed: {}", repair.detail()),
        );
        return out;
    }
    if !repair.removed().is_empty() {
        out.add(
            ViolationKind::DisallowedError,
            format!("run {} check --repair", repair.index),
            format!("repair removed {:?}", repair.removed()),
        );
    }
    if !repair.detail().contains("ReindexChunks") {
        out.add(
            ViolationKind::DisallowedError,
            format!("run {} check --repair", repair.index),
            format!("repair did not re-index: {}", repair.detail()),
        );
    }
    sim.advance(ORPHANS_OLD);
    let run = sim.compact(c);
    if !run.is_ok() {
        out.add(
            ViolationKind::DisallowedError,
            format!("run {} compact", run.index),
            format!("compact failed: {}", run.detail()),
        );
    }
    for v in sim.check_invariants(Invariant::STEP1).violations {
        out.add(v.kind, v.subject, v.detail);
    }
    if b_packs.is_empty() {
        out.add(ViolationKind::PackAbsent, "b", "b wrote no packs");
    }
    for k in b_packs {
        if sim.store().object(&k).is_none() {
            out.add(ViolationKind::PackAbsent, "b", format!("pack {k} gone"));
        }
    }
    out
}

/// `check --repair` between H3 and the compact of H5, on an H3 setup whose
/// landing is set by `late`.
///
/// Since 04 step 5 the compact runs T_pack + 1 min after the repair: the
/// packs the repair left without entries are true orphans, kept while
/// younger than T_pack.
///
/// Observed before 10-single-protocol step 1, on `f7181ea` plus this
/// harness, three runs, all the same: repair applied
/// `RemoveDanglingSnapshot { "b1", missing_chunks: 1 }` and
/// `RemoveUnreferencedEntries`, so b1 was gone after the repair; the compact
/// that followed deleted 4 orphan packs (b's two, and a's two, which no
/// snapshot referenced once the repair swept the index): a silent removal.
/// Since that step the repair re-indexes b1 from b's packs (`ReindexChunks`),
/// b1 survives the compact and restores, and b's packs stay
/// (`NotReproduced`; the regression is [`h5_repair_regression`]).
pub fn h5_repair_with(late: LateHead) -> HazardOutcome {
    let (mut sim, _, b, c) = match h3_setup(late) {
        Ok(s) => s,
        Err(e) => return broken(e),
    };
    if let Err(outcome) = lost_update(&sim) {
        return outcome;
    }
    let b_packs = match b_packs_present(&sim, b) {
        Ok(p) => p,
        Err(e) => return broken(e),
    };
    let repair = sim.check_repair(c);
    if !repair.is_ok() {
        return broken(format!("repair failed: {}", repair.detail()));
    }
    let b1_after_repair = sim
        .history()
        .observations
        .last()
        .is_some_and(|o| o.names().any(|n| n == "b1"));
    sim.advance(ORPHANS_OLD);
    let run = sim.compact(c);
    if !run.is_ok() {
        return broken(format!("compact failed: {}", run.detail()));
    }
    let b1_after_compact = sim
        .history()
        .observations
        .last()
        .is_some_and(|o| o.names().any(|n| n == "b1"));
    let report = sim.check_invariants(Invariant::STEP1);
    if report.violated_for(Invariant::CommittedSnapshotsRestore, "c0")
        || report.violated(Invariant::IndexIntegrity)
        || report.violated(Invariant::AllowedErrors)
    {
        return broken(format!("controls failed:\n{report}"));
    }
    let deleted = deleted_in_run(&sim, c, run.index);
    let gone = b_packs.iter().all(|k| sim.store().object(k).is_none());
    let survive = b_packs.iter().all(|k| sim.store().object(k).is_some());
    let by_compact = b_packs.iter().all(|k| deleted.contains(k));
    let evidence = format!(
        "repair applied {}; b1 present after repair: {b1_after_repair}, after \
         compact: {b1_after_compact}\ncompact: {}\n\
         b's packs gone: {gone}, all deleted by compact: {by_compact}\n{report}",
        repair.detail(),
        run.detail()
    );
    if report.violated_for(Invariant::AcknowledgedCommitsDurable, "b1")
        && report.violated_for(Invariant::NoSilentRemoval, "b1")
        && !report.violated(Invariant::CommittedSnapshotsRestore)
        && gone
        && by_compact
    {
        HazardOutcome::Reproduced { evidence }
    } else if b1_after_compact
        && !report.violated_for(Invariant::CommittedSnapshotsRestore, "b1")
        && !report.violated(Invariant::AcknowledgedCommitsDurable)
        && !report.violated(Invariant::NoSilentRemoval)
        && survive
    {
        HazardOutcome::NotReproduced { evidence }
    } else {
        different_damage(evidence)
    }
}

/// A's head PUT is parked, a compact runs alone, then the PUT lands.
///
/// Since 05 step 8 compact writes a head of its own (the sweep record's)
/// at the generation a's parked head names, and its index garbage
/// collection is age-gated, so a's segment survives. The compact deletes
/// a's packs as orphans; a's late head then overwrites compact's head and
/// names those packs. Before 05 step 8 the landed head named a's segment,
/// which compact had deleted: loud (every command refused until repair).
/// Now the head is valid and names deleted packs: silent until `check` or
/// a restore, and later backups can dedup against those entries. It needs
/// a lost lock plus a late PUT; 04 step 8 (create-if-absent heads) fixes it.
///
/// Since 04 step 5 the compact keeps orphan packs younger than T_pack. A
/// plain wait of T_pack would also age a's segment past the object grace
/// (T_idx then; T_pack, the same 12 h, since phase F part 10b2), and the
/// compact's index GC would collect it (the loud shape again). The route
/// is a store clock step of T_pack + 1 min between a's pack uploads and
/// its segment PUT (an NTP correction on the server, as for H2 and H4):
/// the packs are old orphans, the segment is new. Without the step it is
/// the regression H3-compact-young.
pub fn h3_compact_then_late_head() -> HazardOutcome {
    h3_compact_with(LateHead::AfterB)
}

/// The wait before a compact that must see a backup's packs as old orphans.
const ORPHANS_OLD: std::time::Duration =
    crate::repo::ages::T_PACK.saturating_add(std::time::Duration::from_secs(60));

/// The H3-compact runs: a's head PUT parked (with a's packs aged by
/// `packs_aged` on the store's clock), a compact runs alone, then the PUT
/// lands as `late` says. Returns the sim, a, the compact's run and the
/// report after the landing.
fn h3_compact_runs(
    late: LateHead,
    packs_aged: Option<chrono::Duration>,
) -> Result<(Sim, ClientId, super::history::RunRecord, Report), HazardOutcome> {
    let mut sim = Sim::new();
    let (a, c) = (sim.client("a"), sim.client("c"));
    let r = sim.backup(c, "c0", &SourceTree::seeded("c0", 3, FILE_SIZE));
    if !r.is_ok() {
        return Err(broken(format!("control backup c0 failed: {}", r.detail())));
    }
    park_a_head_with(&mut sim, a, packs_aged).map_err(broken)?;
    if late == LateHead::BeforeB {
        land(&mut sim, late);
    }
    let run = sim.compact(c);
    if !run.is_ok() {
        return Err(broken(format!("compact failed: {}", run.detail())));
    }
    let before = sim.check_invariants(Invariant::STEP1);
    if !before.is_clean() {
        return Err(broken(format!(
            "not consistent before the release:\n{before}"
        )));
    }
    if late != LateHead::BeforeB {
        land(&mut sim, late);
    }
    let report = sim.check_invariants(Invariant::STEP1);
    Ok((sim, a, run, report))
}

/// H3-compact-young: H3-compact without the wait. The compact keeps a's
/// packs (orphans younger than T_pack), so the late head names packs that
/// exist: a clean STEP1 report with every pack a wrote present.
pub fn h3_compact_young_regression() -> Report {
    let mut out = Report::default();
    let (sim, a, _, report) = match h3_compact_runs(LateHead::AfterB, None) {
        Ok(run) => run,
        Err(outcome) => {
            out.add(
                ViolationKind::DisallowedError,
                "scenario",
                format!("{outcome:?}"),
            );
            return out;
        }
    };
    let a_packs = sim.store().keys_written_by(a, "packs/");
    if a_packs.is_empty() {
        out.add(
            ViolationKind::DisallowedError,
            "scenario",
            "a wrote no packs",
        );
    }
    for k in a_packs {
        if sim.store().object(&k).is_none() {
            out.add(
                ViolationKind::IndexNamesAbsentPack,
                &k,
                "a's young orphan pack deleted",
            );
        }
    }
    out.extend(report);
    out
}

/// H3-compact-conditional: H3-compact's schedule (a's packs old orphans) on
/// a conditional store. The late head cannot replace compact's head: it
/// lands refused, and STEP8 holds.
pub fn h3_compact_conditional_regression() -> Report {
    let mut out = Report::default();
    let step = chrono::Duration::from_std(ORPHANS_OLD).expect("T_pack in range");
    let (sim, a, _, _) = match h3_compact_runs(LateHead::AfterB, Some(step)) {
        Ok(run) => run,
        Err(outcome) => {
            out.add(
                ViolationKind::DisallowedError,
                "scenario",
                format!("{outcome:?}"),
            );
            return out;
        }
    };
    if !late_head_refused(&sim, a) {
        out.add(
            ViolationKind::DisallowedError,
            "scenario",
            "a's late head create did not land refused",
        );
    }
    out.extend(sim.check_invariants(Invariant::STEP8));
    out
}

/// H3-compact with the landing set by `late`; `AfterB` lands it after the
/// compact, `BeforeB` before it.
pub fn h3_compact_with(late: LateHead) -> HazardOutcome {
    let step = chrono::Duration::from_std(ORPHANS_OLD).expect("T_pack in range");
    let (sim, a, run, report) = match h3_compact_runs(late, Some(step)) {
        Ok(run) => run,
        Err(outcome) => return outcome,
    };
    let c = run.client;
    let a_packs = sim.store().keys_written_by(a, "packs/");
    let deleted = deleted_in_run(&sim, c, run.index);
    let dangling: Vec<&String> = a_packs
        .iter()
        .filter(|k| deleted.contains(*k) && report.has_kind(ViolationKind::IndexNamesAbsentPack, k))
        .collect();
    let latest_by_a = latest_head_key(&sim)
        .and_then(|k| sim.store().object(&k))
        .is_some_and(|o| o.writer == a);
    let evidence = format!(
        "compact: {}\nlatest head written by a: {latest_by_a}; the index names a's \
         packs deleted by compact: {dangling:?}\n{report}",
        run.detail()
    );
    if let Err(e) = base_controls(&report) {
        return broken(e);
    }
    if latest_by_a && !dangling.is_empty() {
        HazardOutcome::Reproduced { evidence }
    } else if report.is_clean() {
        HazardOutcome::NotReproduced { evidence }
    } else {
        different_damage(evidence)
    }
}
