use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::config::RetentionConfig;
use crate::sim::checker::{self, Invariant, ViolationKind, head_object_keys};
use crate::sim::hazards::{self, HazardOutcome, KNOWN_HAZARDS, LateHead, REGRESSIONS};
use crate::sim::history::{AllowedError, ErrorClass, INDEX_CHANGED_TEXT, classify};
use crate::sim::{
    ClientId, FaultAction, FaultRule, KeyMatch, OpKind, OpMatch, OpOutcome, Sim, SourceTree,
};
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

    // A planted lock that never ages out: the backup's registration probe
    // retries on virtual time and gives up with Locked.
    sim.tamper_put("locks/planted.json", b"{}");
    let (virt, real) = (sim.time().elapsed(), Instant::now());
    let r = sim.backup(b, "t1", &tree("t1"));
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Locked)),
        "{}",
        r.detail()
    );
    // Two backoff sleeps of at least 1.5 s and 3 s.
    assert!(
        sim.time().elapsed() - virt >= Duration::from_secs(4),
        "{:?}",
        sim.time().elapsed() - virt
    );
    assert!(
        real.elapsed() < Duration::from_secs(5),
        "{:?}",
        real.elapsed()
    );

    sim.break_lock(b, true).expect_ok();
    // The failed backup used up its name.
    sim.backup(b, "t2", &tree("t2")).expect_ok();
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// A fault that crashes `c` on its first delete under `locks/`: its lock
/// release, so the lock stays behind.
fn crash_on_lock_release(sim: &Sim, c: ClientId) {
    sim.fault(
        FaultRule::new(
            OpMatch::AnyDelete,
            KeyMatch::Prefix("locks/".into()),
            FaultAction::Crash,
        )
        .client(c),
    );
}

/// `c` takes the lock and crashes releasing it.
fn hold_lock_and_crash(sim: &mut Sim, c: ClientId) {
    crash_on_lock_release(sim, c);
    sim.custom(c, "hold", |_| Ok(()));
    assert!(sim.store().is_crashed(c));
    assert_eq!(sim.store().keys("locks/").len(), 1);
}

#[test]
fn crashed_lock_holder_ages_out() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    hold_lock_and_crash(&mut sim, a);

    // Another machine cannot tell the holder is dead.
    let r = sim.backup(b, "t1", &tree("t1"));
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Locked)),
        "{}",
        r.detail()
    );

    sim.advance(Duration::from_secs(6 * 3600 + 60));
    sim.backup(b, "t2", &tree("t2")).expect_ok();
    assert!(sim.store().keys("locks/").is_empty());
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

#[test]
fn same_machine_reaps_dead_holder_at_once() {
    let mut sim = Sim::new();
    let a = sim.client_on("a", "hostx", Some("boot1"));
    let c = sim.client_on("c", "hostx", Some("boot1"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    hold_lock_and_crash(&mut sim, a);
    let held = sim.store().keys("locks/");

    sim.backup(c, "t1", &tree("t1")).expect_ok();
    for key in &held {
        assert!(sim.store().object(key).is_none(), "{key} not reaped");
    }
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

#[test]
fn same_hostname_other_boot_reaps_at_once() {
    let mut sim = Sim::new();
    let a = sim.client_on("a", "hostx", Some("boot1"));
    let d = sim.client_on("d", "hostx", Some("boot2"));
    sim.backup(d, "t0", &tree("t0")).expect_ok();
    hold_lock_and_crash(&mut sim, a);

    sim.backup(d, "t1", &tree("t1")).expect_ok();
    assert!(sim.store().keys("locks/").is_empty());
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// `a`'s backup crashes on its first pack upload, leaving its session
/// marker behind. Returns the marker key.
fn crash_backup_leaving_marker(sim: &mut Sim, a: ClientId) -> String {
    sim.fault(
        FaultRule::new(
            OpMatch::AnyWrite,
            KeyMatch::Prefix("packs/".into()),
            FaultAction::Crash,
        )
        .client(a),
    );
    let r = sim.backup(a, "t1", &tree("t1"));
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Injected)),
        "{}",
        r.detail()
    );
    let markers: Vec<String> = sim
        .store()
        .keys("sessions/")
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .collect();
    assert_eq!(markers.len(), 1, "{markers:?}");
    markers.into_iter().next().unwrap()
}

#[test]
fn foreign_session_blocks_compact_until_stale() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    let marker = crash_backup_leaving_marker(&mut sim, a);

    let r = sim.compact(b);
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::ActiveSessions)),
        "{}",
        r.detail()
    );

    sim.advance(Duration::from_secs(46 * 60));
    sim.compact(b).expect_ok();
    assert!(sim.store().object(&marker).is_none());
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

#[test]
fn same_machine_session_reaped_at_once() {
    let mut sim = Sim::new();
    let a = sim.client_on("a", "hostx", Some("boot1"));
    let c = sim.client_on("c", "hostx", Some("boot1"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let marker = crash_backup_leaving_marker(&mut sim, a);

    sim.compact(c).expect_ok();
    assert!(sim.store().object(&marker).is_none());
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

#[test]
fn stale_session_journal_recovered_after_staleness() {
    let mut sim = Sim::new();
    // One machine, so `c` reaps the lock `a` leaves; journal recovery judges
    // the marker by age alone.
    let a = sim.client_on("a", "hostx", Some("boot1"));
    let c = sim.client_on("c", "hostx", Some("boot1"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    // The commit joins the journal write before its first snapshot write.
    sim.fault(
        FaultRule::new(
            OpMatch::AnyWrite,
            KeyMatch::Prefix("snapshots/".into()),
            FaultAction::Crash,
        )
        .client(a),
    );
    let r = sim.backup(a, "t1", &tree("t1"));
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Injected)),
        "{}",
        r.detail()
    );
    let journals: Vec<String> = sim
        .store()
        .keys("sessions/")
        .into_iter()
        .filter(|k| k.ends_with(".index"))
        .collect();
    assert_eq!(journals.len(), 1, "a's journal: {journals:?}");
    let journal = &journals[0];

    // The marker is fresh: the journal belongs to a live-looking session.
    sim.backup(c, "t2", &tree("t2")).expect_ok();
    assert!(sim.store().object(journal).is_some(), "recovered too early");

    sim.advance(Duration::from_secs(46 * 60));
    sim.backup(c, "t3", &tree("t3")).expect_ok();
    assert!(
        sim.store().object(journal).is_none(),
        "stale journal not recovered"
    );
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

#[test]
fn prune_keep_within_uses_virtual_time() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "p0", &tree("p0")).expect_ok();
    sim.advance(Duration::from_secs(48 * 3600));
    sim.backup(c, "p1", &tree("p1")).expect_ok();
    let run = sim.prune(
        c,
        RetentionConfig {
            keep_within: Some("1d".into()),
            ..Default::default()
        },
    );
    run.expect_ok();
    assert_eq!(run.removed(), ["p0".to_string()]);
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

#[test]
fn markers_carry_client_identity() {
    let mut sim = Sim::new();
    let a = sim.client_on("a", "hosta", Some("b1"));
    let b = sim.client("b");
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    hold_lock_and_crash(&mut sim, a);
    let key = sim.store().keys("locks/").pop().unwrap();
    let entry: serde_json::Value =
        serde_json::from_slice(&sim.store().object(&key).unwrap().data).unwrap();
    let pid = sim.host(a).pid;
    assert_eq!(entry["hostname"], "hosta");
    assert_eq!(entry["pid"], pid);
    assert_eq!(entry["boot_id"], "b1");

    let r = sim.backup(b, "t1", &tree("t1"));
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Locked)),
        "{}",
        r.detail()
    );
    assert!(
        r.detail().contains("hosta") && r.detail().contains(&format!("PID {pid}")),
        "{}",
        r.detail()
    );
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
