use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::config::RetentionConfig;
use crate::sim::checker::{self, Invariant, ViolationKind, head_object_keys};
use crate::sim::hazards::{self, HazardOutcome, KNOWN_HAZARDS, LateHead, REGRESSIONS};
use crate::sim::history::{AllowedError, ErrorClass, INDEX_CHANGED_TEXT, classify};
use crate::sim::{FaultAction, FaultRule, KeyMatch, OpKind, OpMatch, OpOutcome, Sim, SourceTree};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{ActiveSessionList, LockHolder, VykarError};

const KIB: usize = 1024;

fn tree(seed: &str) -> SourceTree {
    SourceTree::seeded(seed, 3, 64 * KIB).file("sub/note.txt", format!("note {seed}"))
}

#[test]
fn baseline_clean() {
    let mut sim = Sim::new();
    let (c, d) = (sim.client("c"), sim.client("d"));
    sim.check_invariants(Invariant::STEP1).assert_clean();

    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.check_invariants(Invariant::STEP1).assert_clean();
    sim.backup(d, "t1", &tree("t1")).expect_ok();
    sim.check_invariants(Invariant::STEP1).assert_clean();

    let restore = sim.restore(c, "t0");
    restore.expect_ok();
    // A single directory source restores its contents directly under dest.
    let restored = SourceTree::read_from(&sim.restore_dir(&restore)).unwrap();
    assert_eq!(restored, tree("t0"));
    sim.check_invariants(Invariant::STEP1).assert_clean();

    let check = sim.check(c);
    check.expect_ok();
    assert!(check.detail().starts_with("0 errors"), "{}", check.detail());
    sim.check_invariants(Invariant::STEP1).assert_clean();

    let del = sim.delete(c, &["t0"]);
    del.expect_ok();
    assert_eq!(del.removed(), ["t0".to_string()]);
    sim.check_invariants(Invariant::STEP1).assert_clean();

    sim.compact(c).expect_ok();
    sim.check_invariants(Invariant::STEP1).assert_clean();
    sim.check_repair(c).expect_ok();
    sim.check_invariants(Invariant::STEP1).assert_clean();

    // No client op lands after its run returned (releases aside).
    let runs = &sim.history().runs;
    for rec in sim.store().log() {
        if matches!(rec.outcome, OpOutcome::Released(_)) {
            continue;
        }
        if let Some(i) = rec.run {
            assert!(
                rec.step <= runs[i].last_step,
                "op after run {i} returned: {rec:?}"
            );
        }
    }
}

#[test]
fn read_commands_register_session_before_open() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let runs = [sim.check(c), sim.restore(c, "t0")];
    let log = sim.store().log();
    for run in &runs {
        run.expect_ok();
        let ops: Vec<_> = log.iter().filter(|r| r.run == Some(run.index)).collect();
        let marker = ops
            .iter()
            .position(|r| {
                r.op == OpKind::Put && r.key.starts_with("sessions/") && r.key.ends_with(".json")
            })
            .unwrap_or_else(|| panic!("run {} registered no session", run.index));
        let key = &ops[marker].key;
        let config_get = ops
            .iter()
            .position(|r| r.op == OpKind::Get && r.key == "config")
            .expect("open reads config");
        assert!(marker < config_get, "session registered after open");
        let last_session_op = ops.iter().rev().find(|r| &r.key == key).unwrap();
        assert_eq!(last_session_op.op, OpKind::Delete);
    }
}

#[test]
fn prune_removes_by_policy_without_violation() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    for name in ["p0", "p1", "p2"] {
        sim.backup(c, name, &tree(name)).expect_ok();
    }
    let run = sim.prune(
        c,
        RetentionConfig {
            keep_last: Some(1),
            ..Default::default()
        },
    );
    run.expect_ok();
    let removed: BTreeSet<&str> = run.removed().iter().map(String::as_str).collect();
    assert_eq!(removed, BTreeSet::from(["p0", "p1"]));
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

#[test]
fn checker_detects_missing_pack() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let packs = sim.store().keys_written_by(c, "packs/");
    let pack = packs.first().expect("backup wrote a pack").clone();
    assert!(sim.tamper_delete(&pack));
    let report = sim.check_invariants(Invariant::STEP1);
    assert!(
        report.violated_for(Invariant::CommittedSnapshotsRestore, "t0"),
        "{report}"
    );
    assert!(
        report.has_kind(ViolationKind::IndexNamesAbsentPack, &pack),
        "{report}"
    );
}

#[test]
fn checker_detects_missing_index_object() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let named = head_object_keys(sim.store()).unwrap();
    let key = named.iter().next().expect("head names an object").clone();
    assert!(sim.tamper_delete(&key));
    let report = sim.check_invariants(Invariant::STEP1);
    assert!(
        report.has_kind(ViolationKind::HeadObjectAbsent, &key),
        "{report}"
    );
    assert!(report.violated(Invariant::IndexIntegrity));
}

#[test]
fn checker_detects_silent_snapshot_removal() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let key = sim.store().keys("snapshots/").pop().expect("snapshot blob");
    assert!(sim.tamper_delete(&key));
    let report = sim.check_invariants(Invariant::STEP1);
    assert!(
        report.violated_for(Invariant::AcknowledgedCommitsDurable, "t0"),
        "{report}"
    );
    assert!(
        report.violated_for(Invariant::NoSilentRemoval, "t0"),
        "{report}"
    );
}

#[test]
fn checker_detects_refcount_deflation() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    // Two files with the same bytes: their chunk has two references.
    let dup = crate::sim::runner::seeded_bytes("dup", 64 * KIB);
    let t = SourceTree::new()
        .file("a.bin", dup.clone())
        .file("b.bin", dup);
    sim.backup(c, "t0", &t).expect_ok();
    sim.check_invariants(Invariant::STEP1).assert_clean();

    let mut picked: Option<ChunkId> = None;
    sim.custom(c, "deflate", |repo| {
        repo.load_chunk_index_under_lock()?;
        let id = repo
            .chunk_index()
            .iter()
            .find(|(_, e)| e.refcount >= 2)
            .map(|(id, _)| id)
            .expect("a chunk with two references");
        repo.chunk_index_mut().decrement(&id);
        repo.mark_index_dirty();
        picked = Some(id);
        repo.save_state()
    })
    .expect_ok();
    let id = picked.unwrap();

    let report = sim.check_invariants(Invariant::STEP1);
    assert!(
        report.has_kind(ViolationKind::RefcountDeflated, &id.to_string()),
        "{report}"
    );
    assert!(
        !report.violated(Invariant::CommittedSnapshotsRestore),
        "{report}"
    );
}

#[test]
fn checker_detects_restore_mismatch() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let mut history = sim.history().clone();
    let edited = tree("t0").file("f000.bin", b"not what was backed up".to_vec());
    history.trees.insert("t0".into(), edited);
    let report = checker::check_invariants(sim.store(), &history, Invariant::STEP1);
    assert!(
        report.has_kind(ViolationKind::RestoreDiffers, "t0"),
        "{report}"
    );
}

#[test]
fn classify_errors() {
    let allowed = |e: VykarError, a: AllowedError| assert_eq!(classify(&e), ErrorClass::Allowed(a));
    allowed(
        VykarError::Locked(LockHolder {
            key: "locks/x".into(),
            details: None,
        }),
        AllowedError::Locked,
    );
    allowed(
        VykarError::ActiveSessions(ActiveSessionList(Vec::new())),
        AllowedError::ActiveSessions,
    );
    allowed(
        VykarError::StaleChunksDuringCommit,
        AllowedError::StaleChunksDuringCommit,
    );
    allowed(
        VykarError::LockExpired("x".into()),
        AllowedError::LockExpired,
    );
    allowed(VykarError::Interrupted, AllowedError::Interrupted);
    allowed(
        VykarError::Other(format!("{INDEX_CHANGED_TEXT}; retry the operation")),
        AllowedError::IndexChanged,
    );
    allowed(
        VykarError::Other(format!(
            "{}: Fail on Get config",
            crate::sim::INJECTED_MARKER
        )),
        AllowedError::Injected,
    );
    allowed(
        VykarError::RestoreSourceUnreadable(Box::new(VykarError::Other(format!(
            "{}: Fail",
            crate::sim::INJECTED_MARKER
        )))),
        AllowedError::Injected,
    );
    assert_eq!(
        classify(&VykarError::IndexObjectMissing {
            kind: "segment",
            key: "index.d/x".into(),
        }),
        ErrorClass::Disallowed
    );
    assert_eq!(
        classify(&VykarError::Other("boom".into())),
        ErrorClass::Disallowed
    );
}

#[test]
fn allowed_and_disallowed_errors() {
    let mut sim = Sim::new();
    let (c, d) = (sim.client("c"), sim.client("d"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();

    let r = sim.custom(c, "locked", |_| {
        Err(VykarError::Locked(LockHolder {
            key: "locks/x".into(),
            details: None,
        }))
    });
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Locked))
    );
    let r = sim.custom(c, "index changed", |_| {
        Err(VykarError::Other(format!(
            "{INDEX_CHANGED_TEXT}; retry the operation"
        )))
    });
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::IndexChanged))
    );
    sim.fault(
        FaultRule::new(
            OpMatch::Kind(OpKind::Get),
            KeyMatch::Exact("config".into()),
            FaultAction::Fail,
        )
        .client(d),
    );
    let r = sim.check(d);
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Injected)),
        "{}",
        r.detail()
    );
    sim.check_invariants(Invariant::STEP1).assert_clean();

    let r = sim.custom(c, "disallowed", |_| {
        Err(VykarError::IndexObjectMissing {
            kind: "segment",
            key: "index.d/x".into(),
        })
    });
    let report = sim.check_invariants(Invariant::STEP1);
    assert!(
        report.violated_for(
            Invariant::AllowedErrors,
            &format!("run {} {:?}", r.index, r.op)
        ),
        "{report}"
    );
}

#[test]
fn crash_then_break_lock_then_backup() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    sim.fault(
        FaultRule::new(
            OpMatch::AnyWrite,
            KeyMatch::Prefix("locks/".into()),
            FaultAction::Crash,
        )
        .client(a),
    );
    let r = sim.compact(a);
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Injected)),
        "{}",
        r.detail()
    );
    assert!(sim.store().is_crashed(a));
    // The crash hit the lock PUT itself, so no lock was written.
    assert!(sim.store().keys("locks/").is_empty());

    // The guard against a 10-minute real-time commit wait, on a planted lock.
    sim.tamper_put("locks/planted.json", b"{}");
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        sim.backup(b, "t1", &tree("t1"));
    }))
    .expect_err("backup must refuse to start with a lock present");
    let msg = panicked
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(msg.contains("break_lock"), "{msg}");

    sim.break_lock(b, true).expect_ok();
    sim.backup(b, "t1", &tree("t1")).expect_ok();
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

#[test]
fn known_hazard_h3_late_head_put() {
    hazards::assert_reproduces("H3");
}

#[test]
fn known_hazard_h5_compact_after_lost_update() {
    hazards::assert_reproduces("H5");
}

#[test]
fn known_hazard_h5_repair_does_not_help() {
    hazards::assert_reproduces("H5-repair");
}

#[test]
fn known_hazard_h3_compact_then_late_head() {
    hazards::assert_reproduces("H3-compact");
}

fn expect_not_reproduced(id: &str, outcome: HazardOutcome) {
    assert!(
        matches!(outcome, HazardOutcome::NotReproduced { .. }),
        "{id} neutralised should be NotReproduced: {outcome:?}"
    );
}

fn expect_no_prerequisite(id: &str, outcome: HazardOutcome) {
    assert!(
        matches!(
            outcome,
            HazardOutcome::PrerequisiteNotReproduced {
                prerequisite: "H3",
                ..
            }
        ),
        "{id} on a neutralised H3 should report the missing prerequisite: {outcome:?}"
    );
}

// A fixed hazard must read as fixed, not as a broken scenario. Each scenario
// runs with its late write moved or dropped by the harness.

#[test]
fn neutralised_h3_not_reproduced() {
    for late in [LateHead::BeforeB, LateHead::Never] {
        expect_not_reproduced("H3", hazards::h3_with(late));
    }
}

#[test]
fn neutralised_h3_leaves_h5_without_prerequisite() {
    for late in [LateHead::BeforeB, LateHead::Never] {
        expect_no_prerequisite("H5", hazards::h5_with(late));
        expect_no_prerequisite("H5-repair", hazards::h5_repair_with(late));
    }
}

#[test]
fn neutralised_h3_compact_not_reproduced() {
    for late in [LateHead::BeforeB, LateHead::Never] {
        expect_not_reproduced("H3-compact", hazards::h3_compact_with(late));
    }
}

/// The ids with a test above, hazards and regressions alike.
const TESTED_IDS: &[&str] = &["H3", "H5", "H5-repair", "H3-compact"];

#[test]
fn known_hazard_ids_unique_and_listed() {
    let ids: Vec<&str> = KNOWN_HAZARDS
        .iter()
        .map(|h| h.id)
        .chain(REGRESSIONS.iter().map(|r| r.id))
        .collect();
    let unique: BTreeSet<&str> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "duplicate id in {ids:?}");
    assert_eq!(unique, TESTED_IDS.iter().copied().collect::<BTreeSet<_>>());
}

#[test]
fn regressions_hold() {
    for r in REGRESSIONS {
        hazards::assert_regression_holds(r.id);
    }
}
