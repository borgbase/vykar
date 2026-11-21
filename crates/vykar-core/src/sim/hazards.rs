//! Known protocol hazards, each as a scenario that reproduces it, and the
//! regressions they turn into once fixed.
//!
//! A hazard's test asserts that it still reproduces. When a fix lands, the
//! scenario reports `NotReproduced` and the entry moves from `KNOWN_HAZARDS`
//! to `REGRESSIONS`, where the same scenario must then hold.

use std::collections::BTreeSet;

use super::checker::{Invariant, Report, ViolationKind, head_object_keys};
use super::fault::{ClientId, FaultAction, FaultRule};
use super::history::{AllowedError, ErrorClass};
use super::runner::{Sim, SourceTree};

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
        id: "H5",
        summary: "after a lost update, a legitimate compact deletes the unindexed packs",
        fixed_by: "05-index-gc-format.md (mark before every deletion)",
        run: h5_compact_after_lost_update,
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
                  and names objects compact deleted",
        fixed_by: "04-commit-protocol.md (conditional-create commit point)",
        run: h3_compact_then_late_head,
    },
];

pub const REGRESSIONS: &[Regression] = &[];

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

fn broken(reason: impl Into<String>) -> HazardOutcome {
    HazardOutcome::ScenarioBroken {
        reason: reason.into(),
    }
}

/// `a` backs up `a1` with its head `index` PUT delayed, so the commit
/// fails after `a` stored its segment and released the lock.
fn park_a_head(sim: &mut Sim, a: ClientId) -> Result<(), String> {
    sim.fault(FaultRule::put("index", FaultAction::Delay).client(a));
    let r = sim.backup(a, "a1", &SourceTree::seeded("a1", 3, FILE_SIZE));
    if r.error_class() != Some(&ErrorClass::Allowed(AllowedError::Injected)) {
        return Err(format!(
            "a1 should fail with the injected fault, got {:?}: {}\n\
             If the commit point no longer PUTs `index`, retarget the fault \
             rule to the new commit key before reading anything into this \
             outcome.",
            r.outcome,
            r.detail()
        ));
    }
    let parked = sim.store().parked();
    if parked.len() != 1 || parked[0].key != "index" || parked[0].client != a {
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
/// 2. `a`'s write of the head `index` is delayed, so `a1`'s commit fails
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
fn base_controls(report: &Report) -> Result<(), String> {
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
fn different_damage(evidence: impl std::fmt::Display) -> HazardOutcome {
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
    if !report.not_evaluated_for(Invariant::NoRefcountDeflation, "b1") {
        return broken(format!(
            "invariant 5 should be not evaluated for b1 (its tree chunks are \
             unindexed):\n{report}"
        ));
    }
    let writer = sim.store().object("index").map(|o| o.writer);
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
fn deleted_in_run(sim: &Sim, client: ClientId, run: usize) -> BTreeSet<String> {
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

/// H5: after H3, a legitimate compact deletes `b1`'s packs as orphans.
pub fn h5_compact_after_lost_update() -> HazardOutcome {
    h5_with(LateHead::AfterB)
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
    if !run.is_ok() {
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
    } else if gone.is_empty() {
        HazardOutcome::NotReproduced { evidence }
    } else {
        different_damage(evidence)
    }
}

/// `check --repair` between H3 and the compact of H5.
///
/// Observed on `f7181ea` plus this harness, three runs, all the same:
/// repair applies `RemoveDanglingSnapshot { "b1", missing_chunks: 1 }` and
/// `RebuildRefcounts`, so b1 is gone after the repair; the compact that
/// follows deletes 4 orphan packs (b's two, and a's two, which no snapshot
/// references once refcounts are rebuilt). Repair does not stop H5: it
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
/// Observed on `f7181ea` plus this harness, three runs, all the same: the
/// compact deletes a's two packs as orphans and a's unreferenced segment;
/// the landed head names that segment. Invariant 4 is violated
/// (`HeadObjectAbsent` for a's segment, `ChainUnloadable`), and with the
/// chain unloadable invariants 1 and 5 cannot be evaluated for any
/// snapshot, the control `c0` included.
pub fn h3_compact_then_late_head() -> HazardOutcome {
    h3_compact_with(LateHead::AfterB)
}

/// H3-compact with the landing set by `late`; `AfterB` lands it after the
/// compact, `BeforeB` before it.
pub fn h3_compact_with(late: LateHead) -> HazardOutcome {
    let mut sim = Sim::new();
    let (a, c) = (sim.client("a"), sim.client("c"));
    let r = sim.backup(c, "c0", &SourceTree::seeded("c0", 3, FILE_SIZE));
    if !r.is_ok() {
        return broken(format!("control backup c0 failed: {}", r.detail()));
    }
    if let Err(e) = park_a_head(&mut sim, a) {
        return broken(e);
    }
    if late == LateHead::BeforeB {
        land(&mut sim, late);
    }
    let run = sim.compact(c);
    if !run.is_ok() {
        return broken(format!("compact failed: {}", run.detail()));
    }
    let before = sim.check_invariants(Invariant::STEP1);
    if !before.is_clean() {
        return broken(format!("not consistent before the release:\n{before}"));
    }
    if late != LateHead::BeforeB {
        land(&mut sim, late);
    }
    let report = sim.check_invariants(Invariant::STEP1);
    let a_segments = sim.store().keys_written_by(a, "index.d/");
    let deleted = deleted_in_run(&sim, c, run.index);
    let dangling: Vec<&String> = a_segments
        .iter()
        .filter(|k| deleted.contains(*k) && report.has_kind(ViolationKind::HeadObjectAbsent, k))
        .collect();
    let evidence = format!(
        "compact: {}\nhead names a's segments deleted by compact: {dangling:?}\n{report}",
        run.detail()
    );
    if let Err(e) = base_controls(&report) {
        return broken(e);
    }
    if !dangling.is_empty() {
        HazardOutcome::Reproduced { evidence }
    } else if report.is_clean() {
        HazardOutcome::NotReproduced { evidence }
    } else {
        different_damage(evidence)
    }
}
