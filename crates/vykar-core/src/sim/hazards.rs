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
//! H5-repair now need orphans older than T_pack.
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
use super::runner::{Call, GATE_TIMEOUT, Sim, SourceTree};

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

pub struct KnownHazard {
    pub id: &'static str,
    pub summary: &'static str,
    /// The plan expected to remove it.
    pub fixed_by: &'static str,
    pub run: fn() -> HazardOutcome,
}

/// Scenarios that once reproduced a hazard and must now hold.
pub struct Regression {
    pub id: &'static str,
    pub summary: &'static str,
    pub run: fn() -> Report,
}

pub const KNOWN_HAZARDS: &[KnownHazard] = &[
    KnownHazard {
        id: "H3",
        summary: "head PUT times out and lands after the lock is released; \
                  B's committed entries are dropped",
        fixed_by: "04-commit-protocol.md (conditional-create commit point)",
        run: h3_late_head_put,
    },
    KnownHazard {
        id: "H5-repair",
        summary: "check --repair between H3 and compact removes the snapshot \
                  instead of re-indexing it",
        fixed_by: "05-index-gc-format.md (rebuild from packs)",
        run: h5_repair_then_compact,
    },
    KnownHazard {
        id: "H3-compact",
        summary: "head PUT times out, a compact runs, then the stale head lands \
                  over compact's head and names the packs compact deleted as orphans",
        fixed_by: "04-commit-protocol.md step 8 (create-if-absent heads); step 5 \
                   narrowed it to orphans older than T_pack",
        run: h3_compact_then_late_head,
    },
    KnownHazard {
        id: "H1-late-clone",
        summary: "a clone of the installation that booted after the holder's last write \
                  takes a live lock for one of its earlier boot and reaps it",
        fixed_by: "not planned: a history of this installation's own boot IDs; \
                   04 step 5 narrows it for markers",
        run: interleavings::h1_late_clone,
    },
    KnownHazard {
        id: "H2",
        summary: "lock refresh recreates a reaped key: two holders, both fences pass \
                  (route: the store's clock steps 3 h forward)",
        fixed_by: "04-commit-protocol.md step 2 (non-resurrecting lock refresh, fallback only)",
        run: interleavings::h2_lock_refresh_resurrects,
    },
    KnownHazard {
        id: "H4",
        summary: "compact loses its lock, deletes sources with no fence after its head \
                  PUT, and a stale head names the deleted packs (route: the store's \
                  clock steps 6 h 1 min forward)",
        fixed_by: "04-commit-protocol.md step 3 (fence before deletes; narrows) and \
                   step 8 (create-if-absent heads)",
        run: interleavings::h4_compact_deletes_after_lost_lock,
    },
];

pub const REGRESSIONS: &[Regression] = &[
    Regression {
        id: "H5",
        summary: "after a lost update, compact refuses (the mark fails) and deletes \
                  none of the unindexed packs",
        run: h5_regression,
    },
    Regression {
        id: "H1",
        summary: "same hostname: a live lock is kept by cloned machines, namesakes (also \
                  one booted after the holder's write) and containers (installation ID, \
                  boot ID and uptime, PID namespace)",
        run: interleavings::h1_regression,
    },
    Regression {
        id: "H1-session",
        summary: "same hostname: a live backup's marker is kept, compact refuses with \
                  ActiveSessions",
        run: interleavings::h1_session_regression,
    },
    Regression {
        id: "H2-marker",
        summary: "a reaped session marker is never written again; the refresh finds it \
                  missing and the backup refuses as stale",
        run: interleavings::h2_marker_regression,
    },
    Regression {
        id: "H4-segment",
        summary: "compact upkeep no longer deletes a concurrent writer's fresh segment \
                  (age-gated GC)",
        run: interleavings::h4_segment_regression,
    },
    Regression {
        id: "H4-skew",
        summary: "a client 6 h 1 min ahead no longer reaps compact's live lock \
                  (store-clock ages)",
        run: interleavings::h4_skew_regression,
    },
    Regression {
        id: "H3-compact-young",
        summary: "a compact soon after a backup lost its head PUT keeps the backup's \
                  orphan packs (younger than T_pack); the late head names present packs",
        run: h3_compact_young_regression,
    },
];

/// Panics unless `id` is a known hazard that reproduces.
pub fn assert_reproduces(id: &str) {
    let hazard = KNOWN_HAZARDS
        .iter()
        .find(|h| h.id == id)
        .unwrap_or_else(|| panic!("{id} is not in KNOWN_HAZARDS"));
    match (hazard.run)() {
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
    let report = (regression.run)();
    assert!(
        report.is_clean(),
        "regression {id} ({}) fails:\n{report}",
        regression.summary
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

/// `check --repair` between H3 and the compact of H5.
///
/// Since 04 step 5 the compact runs T_pack + 1 min after the repair: the
/// packs the repair left without entries are true orphans, kept while
/// younger than T_pack.
///
/// Observed on `f7181ea` plus this harness, three runs, all the same:
/// repair applies `RemoveDanglingSnapshot { "b1", missing_chunks: 1 }` and
/// `RemoveUnreferencedEntries`, so b1 is gone after the repair; the compact
/// that follows deletes 4 orphan packs (b's two, and a's two, which no
/// snapshot references once the repair swept the index). Repair does not stop H5: it
/// turns the lost update into a silent removal. Invariants 2 and 3 are
/// violated for b1, invariants 1, 4 and 8 hold, and b's packs are gone.
pub fn h5_repair_then_compact() -> HazardOutcome {
    h5_repair_with(LateHead::AfterB)
}

/// H5-repair on an H3 setup whose landing is set by `late`.
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
/// plain wait of T_pack would also age a's segment past T_idx, and the
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
