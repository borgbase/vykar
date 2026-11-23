use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::config::RetentionConfig;
use crate::sim::checker::{self, Invariant, ViolationKind, head_object_keys};
use crate::sim::hazards::{self, HazardOutcome, KNOWN_HAZARDS, LateHead, REGRESSIONS};
use crate::sim::history::{AllowedError, ErrorClass, INDEX_CHANGED_TEXT, classify};
use crate::sim::interleavings::{self, H2Reap, Hosts, MarkerReap, Order};
use crate::sim::{
    Call, ClientId, FaultAction, FaultRule, GateRule, INIT_PID_NS, KeyMatch, OpKind, OpMatch,
    OpOutcome, Sim, SimMachine, SourceTree,
};
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

/// A clone of hostx's installation (same hostname, other boot ID, booted
/// long before) cannot tell a's lock from a live one: it waits for its age.
#[test]
fn clone_keeps_lock_until_stale() {
    let mut sim = Sim::new();
    let a = sim.client_on("a", "hostx", Some("boot1"));
    let d = sim.client_on("d", "hostx", Some("boot2"));
    sim.backup(d, "t0", &tree("t0")).expect_ok();
    hold_lock_and_crash(&mut sim, a);

    let r = sim.backup(d, "t1", &tree("t1"));
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Locked)),
        "{}",
        r.detail()
    );
    sim.advance(Duration::from_secs(6 * 3600 + 60));
    sim.backup(d, "t2", &tree("t2")).expect_ok();
    assert!(sim.store().keys("locks/").is_empty());
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// hostx rebooted: its new boot reaps the lock of the earlier boot once
/// the lock is older than the uptime; a clone that booted long ago does
/// not.
#[test]
fn rebooted_machine_reaps_lock_of_earlier_boot() {
    let mut sim = Sim::new();
    let a = sim.client_on("a", "hostx", Some("boot1"));
    let clone = sim.client_on("clone", "hostx", Some("boot3"));
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    hold_lock_and_crash(&mut sim, a);
    sim.advance(Duration::from_secs(10 * 60));
    let booted = chrono::Duration::from_std(sim.time().elapsed()).unwrap();
    let d = sim.client_at(
        "d",
        SimMachine::new("hostx", Some("boot2")).booted_at(booted),
    );
    sim.advance(Duration::from_secs(60));

    let r = sim.backup(clone, "t1", &tree("t1"));
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Locked)),
        "{}",
        r.detail()
    );
    sim.backup(d, "t2", &tree("t2")).expect_ok();
    assert!(sim.store().keys("locks/").is_empty());
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// Containers on one kernel with one hostname and one config volume: PIDs
/// of another namespace are not visible, so the marker waits for its age.
#[test]
fn containers_sharing_a_hostname_reap_by_age_only() {
    let mut sim = Sim::new();
    let a = sim.client_at(
        "a",
        SimMachine::new("hostx", Some("boot1")).pid_ns(Some(INIT_PID_NS + 1)),
    );
    let c = sim.client_at(
        "c",
        SimMachine::new("hostx", Some("boot1")).pid_ns(Some(INIT_PID_NS + 2)),
    );
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let marker = crash_backup_leaving_marker(&mut sim, a);

    let r = sim.compact(c);
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::ActiveSessions)),
        "{}",
        r.detail()
    );
    sim.advance(Duration::from_secs(46 * 60));
    sim.compact(c).expect_ok();
    assert!(sim.store().object(&marker).is_none());
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// A lock in the format of 0.24 and older (no installation ID) from c's
/// own hostname and boot, with a dead PID, is judged by age only.
#[test]
fn old_format_lock_from_same_hostname_waits_for_age() {
    let mut sim = Sim::new();
    let c = sim.client_on("c", "hostx", Some("boot1"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let body = serde_json::json!({
        "hostname": "hostx",
        "pid": 999_999,
        "time": sim.time().now_utc().to_rfc3339(),
        "boot_id": "boot1",
    });
    sim.tamper_put("locks/old-client.json", &serde_json::to_vec(&body).unwrap());

    let r = sim.backup(c, "t1", &tree("t1"));
    assert_eq!(
        r.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Locked)),
        "{}",
        r.detail()
    );
    sim.advance(Duration::from_secs(6 * 3600 + 60));
    sim.backup(c, "t2", &tree("t2")).expect_ok();
    assert!(sim.store().keys("locks/").is_empty());
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
    assert_eq!(entry["installation_id"], "install-hosta");
    assert_eq!(entry["pid_ns"], INIT_PID_NS);

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
fn session_markers_carry_client_identity() {
    let mut sim = Sim::new();
    let a = sim.client_on("a", "hosta", Some("b1"));
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let gate = sim.gate(sessions_list(a));
    let sa = sim.spawn(a, backup_call("t1"));
    sim.expect_paused(gate, "a's journal-recovery LIST");
    let markers: Vec<String> = sim
        .store()
        .keys("sessions/")
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .collect();
    assert_eq!(markers.len(), 1, "{markers:?}");
    let entry: serde_json::Value =
        serde_json::from_slice(&sim.store().object(&markers[0]).unwrap().data).unwrap();
    assert_eq!(entry["hostname"], "hosta");
    assert_eq!(entry["pid"], sim.host(a).pid);
    assert_eq!(entry["boot_id"], "b1");
    assert_eq!(entry["installation_id"], "install-hosta");
    assert_eq!(entry["pid_ns"], INIT_PID_NS);
    sim.resume(gate);
    sim.join(sa).expect_ok();
}

#[test]
fn known_hazard_h3_late_head_put() {
    hazards::assert_reproduces("H3");
}

/// H5 is a regression: after the lost update compact refuses and every pack
/// of b survives.
#[test]
fn h5_after_lost_update_compact_refuses() {
    expect_not_reproduced("H5", hazards::h5_with(LateHead::AfterB));
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
const TESTED_IDS: &[&str] = &[
    "H3",
    "H5",
    "H5-repair",
    "H3-compact",
    "H1",
    "H1-session",
    "H1-late-clone",
    "H2",
    "H2-marker",
    "H4",
    "H4-segment",
];

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

// ── Scripted pause points (08 step 3) ───────────────────────────────────

fn sessions_list(c: ClientId) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::List),
        KeyMatch::Exact("sessions/".into()),
    )
}

fn backup_call(name: &str) -> Call {
    Call::Backup {
        snapshot: name.into(),
        tree: tree(name),
    }
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|run| run.to_string()))
        .unwrap_or_default()
}

#[test]
fn spawned_run_pauses_and_resumes() {
    let mut sim = Sim::new();
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let gate = sim.gate(sessions_list(a));
    let run = sim.spawn(a, backup_call("a1"));
    let held = sim.expect_paused(gate, "journal recovery");
    assert!(held.thread.starts_with("sim-a-"), "{}", held.thread);
    assert_eq!(held.run, Some(run.index()));
    assert_eq!(sim.in_flight(), vec![a]);
    sim.resume(gate);
    let rec = sim.join(run);
    rec.expect_ok();
    assert_eq!(sim.history().runs[rec.index].index, rec.index);
    assert!(sim.in_flight().is_empty());
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

#[test]
fn runs_recorded_in_index_order() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    let gate = sim.gate(sessions_list(a));
    let run = sim.spawn(a, backup_call("a1"));
    sim.expect_paused(gate, "journal recovery");
    let rb = sim.call(b, backup_call("b1"));
    rb.expect_ok();
    sim.resume(gate);
    let ra = sim.join(run);
    ra.expect_ok();
    assert!(ra.index < rb.index);
    let runs = &sim.history().runs;
    assert!(runs.windows(2).all(|w| w[0].index < w[1].index));
    let obs = &sim.history().observations;
    assert!(obs.windows(2).all(|w| w[0].step <= w[1].step));
    // Every record of a client's run carries that run's index.
    for rec in sim.store().log() {
        if rec.client == a || rec.client == b {
            let run = rec.run.expect("client op outside a run");
            assert_eq!(runs[run].client, rec.client, "{rec:?}");
        }
    }
    sim.check_invariants(Invariant::STEP3).assert_clean();
}

#[test]
fn wait_paused_none_when_run_ends_first() {
    let mut sim = Sim::new();
    let a = sim.client("a");
    let gate = sim.gate(GateRule::new(
        a,
        OpMatch::AnyDelete,
        KeyMatch::Prefix("snapshots/".into()),
    ));
    let run = sim.spawn(a, backup_call("a1"));
    assert!(sim.wait_paused(gate).is_none());
    sim.join(run).expect_ok();
}

#[test]
fn inline_run_with_armed_gate_panics() {
    let mut sim = Sim::new();
    let a = sim.client("a");
    sim.gate(sessions_list(a));
    let held = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sim.compact(a)))
        .expect_err("inline run with an armed gate");
    assert!(
        panic_text(held.as_ref()).contains("spawn"),
        "{}",
        panic_text(held.as_ref())
    );
}

#[test]
fn check_invariants_panics_while_in_flight() {
    let mut sim = Sim::new();
    let a = sim.client("a");
    let gate = sim.gate(sessions_list(a));
    let run = sim.spawn(a, backup_call("a1"));
    sim.expect_paused(gate, "journal recovery");
    let held = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sim.check_invariants(Invariant::STEP1)
    }));
    assert!(held.is_err());
    sim.resume(gate);
    sim.join(run).expect_ok();
}

#[test]
fn assert_settled_panics_on_unpaused_run() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let run = sim.spawn(
        a,
        Call::Custom {
            label: "blocked".into(),
            f: Box::new(move |_| {
                let _ = rx.recv();
                Ok(())
            }),
        },
    );
    let held = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sim.compact(b)))
        .expect_err("call while a runs unpaused");
    assert!(
        panic_text(held.as_ref()).contains("running unpaused"),
        "{}",
        panic_text(held.as_ref())
    );
    tx.send(()).unwrap();
    sim.join(run).expect_ok();
}

#[test]
fn drop_opens_gates() {
    let mut sim = Sim::new();
    let a = sim.client("a");
    let gate = sim.gate(sessions_list(a));
    let run = sim.spawn(a, backup_call("a1"));
    sim.expect_paused(gate, "journal recovery");
    let handle = run.into_handle();
    drop(sim);
    let start = Instant::now();
    while !handle.is_finished() {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "held run did not finish"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn step3_clean_on_sequential_flows() {
    let mut sim = Sim::new();
    let (c, d) = (sim.client("c"), sim.client("d"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.backup(d, "t1", &tree("t1")).expect_ok();
    sim.backup(c, "t2", &tree("t2")).expect_ok();
    sim.restore(c, "t0").expect_ok();
    sim.check(c).expect_ok();
    sim.delete(c, &["t0"]).expect_ok();
    sim.prune(
        d,
        RetentionConfig {
            keep_last: Some(1),
            ..Default::default()
        },
    )
    .expect_ok();
    sim.compact(c).expect_ok();
    sim.check_repair(c).expect_ok();
    sim.break_lock(d, false).expect_ok();
    sim.check_invariants(Invariant::STEP3).assert_clean();
}

/// `c` backs up t0; `a` deletes it and is held at its snapshot delete.
fn held_delete() -> (
    Sim,
    ClientId,
    crate::sim::Spawned,
    crate::sim::GateId,
    String,
) {
    let mut sim = Sim::new();
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let gate = sim.gate(GateRule::new(
        a,
        OpMatch::AnyDelete,
        KeyMatch::Prefix("snapshots/".into()),
    ));
    let run = sim.spawn(
        a,
        Call::Delete {
            names: vec!["t0".into()],
        },
    );
    sim.expect_paused(gate, "a's snapshot delete");
    let key = sim
        .store()
        .keys_written_by(a, "locks/")
        .pop()
        .expect("a holds a lock");
    (sim, a, run, gate, key)
}

const PLANTED_LOCK: &str = "locks/00000000000000000000-zz.json";
const LOCK_BODY: &[u8] = br#"{"hostname":"elsewhere","pid":1,"time":"2026-01-01T00:00:00Z"}"#;

#[test]
fn lock_exclusion_without_lock() {
    let (mut sim, _, run, gate, key) = held_delete();
    assert!(sim.tamper_delete(&key));
    sim.resume(gate);
    let rec = sim.join(run);
    let report = sim.check_invariants(&[Invariant::LockExclusion]);
    assert!(
        report.has_kind(
            ViolationKind::WriteWithoutLock,
            &format!("run {}", rec.index)
        ),
        "{report}"
    );
}

#[test]
fn lock_exclusion_other_and_several() {
    let (mut sim, _, run, gate, _) = held_delete();
    sim.tamper_put(PLANTED_LOCK, LOCK_BODY);
    sim.resume(gate);
    let rec = sim.join(run);
    let report = sim.check_invariants(&[Invariant::LockExclusion]);
    assert!(
        report.has_kind(
            ViolationKind::WriteUnderSeveralLocks,
            &format!("run {}", rec.index)
        ),
        "{report}"
    );

    let (mut sim, _, run, gate, key) = held_delete();
    assert!(sim.tamper_delete(&key));
    sim.tamper_put(PLANTED_LOCK, LOCK_BODY);
    sim.resume(gate);
    let rec = sim.join(run);
    let report = sim.check_invariants(&[Invariant::LockExclusion]);
    assert!(
        report.has_kind(
            ViolationKind::WriteUnderOtherLock,
            &format!("run {}", rec.index)
        ),
        "{report}"
    );
}

#[test]
fn wedge_on_unageable_lock() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.tamper_put("locks/planted.json", b"{}");
    let (keys, log_len) = (sim.store().keys(""), sim.store().log().len());
    let report = sim.check_invariants(&[Invariant::NoPermanentWedge]);
    assert!(
        report.has_kind(ViolationKind::WedgedBackup, "repository"),
        "{report}"
    );
    assert_eq!(sim.store().keys(""), keys);
    assert_eq!(sim.store().log().len(), log_len);
}

#[test]
fn wedge_check_skips_malformed_marker() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.tamper_put("sessions/bad.json", b"not json");
    let report = sim.check_invariants(&[Invariant::NoPermanentWedge]);
    assert!(report.violations.is_empty(), "{report}");
    assert!(
        report.not_evaluated_for(Invariant::NoPermanentWedge, "sessions/bad.json"),
        "{report}"
    );
}

#[test]
fn known_hazard_h1_late_clone() {
    hazards::assert_reproduces("H1-late-clone");
}

#[test]
fn known_hazard_h2_lock_refresh_resurrects() {
    hazards::assert_reproduces("H2");
}

#[test]
fn known_hazard_h2_marker_refresh_resurrects() {
    hazards::assert_reproduces("H2-marker");
}

#[test]
fn known_hazard_h4_compact_deletes_after_lost_lock() {
    hazards::assert_reproduces("H4");
}

#[test]
fn neutralised_h1_not_reproduced() {
    expect_not_reproduced("H1", interleavings::h1_with(Hosts::Distinct));
}

#[test]
fn neutralised_h1_session_not_reproduced() {
    expect_not_reproduced(
        "H1-session",
        interleavings::h1_session_with(Hosts::Distinct),
    );
}

#[test]
fn neutralised_h2_not_reproduced() {
    expect_not_reproduced("H2", interleavings::h2_with(H2Reap::BeforeFence));
}

#[test]
fn neutralised_h2_marker_not_reproduced() {
    expect_not_reproduced(
        "H2-marker",
        interleavings::h2_marker_with(MarkerReap::BeforeGet),
    );
}

#[test]
fn neutralised_h4_not_reproduced() {
    expect_not_reproduced(
        "H4",
        interleavings::h4_with(Hosts::Skewed, Order::Sequential),
    );
}

#[test]
fn neutralised_h4_segment_not_reproduced() {
    expect_not_reproduced(
        "H4-segment",
        interleavings::h4_segment_with(Hosts::Skewed, Order::Sequential),
    );
}

/// Without the skew no client reaps compact's live lock: on distinct hosts,
/// and since 04 step 1 on cloned machines either (the H1 fix seen through
/// H4).
#[test]
fn h4_without_a_reaper_has_no_prerequisite() {
    for hosts in [Hosts::Distinct, Hosts::SameHostname] {
        for outcome in [
            interleavings::h4_with(hosts, Order::Interleaved),
            interleavings::h4_segment_with(hosts, Order::Interleaved),
        ] {
            assert!(
                matches!(
                    outcome,
                    HazardOutcome::PrerequisiteNotReproduced {
                        prerequisite: "a reaped live lock",
                        ..
                    }
                ),
                "H4 on {hosts:?} should report the missing prerequisite: {outcome:?}"
            );
        }
    }
}

// ── Mark and sweep (05 step 8) ──────────────────────────────────────────

/// Mutations by `client` during run `run` under `prefixes`.
fn mutations_under(sim: &Sim, client: ClientId, run: usize, prefixes: &[&str]) -> Vec<String> {
    sim.store()
        .log()
        .iter()
        .filter(|r| r.client == client && r.run == Some(run) && r.applied() && r.op.is_mutation())
        .flat_map(|r| {
            r.all_keys()
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .filter(|k| prefixes.iter().any(|p| k.starts_with(p)))
        .collect()
}

fn allowed_as(run: &crate::sim::history::RunRecord, e: AllowedError) -> bool {
    run.error_class() == Some(&ErrorClass::Allowed(e))
}

/// SIM-1: a compact while a backup is between its uploads and its commit
/// refuses (the session is live) and touches no pack and no index object;
/// the backup then commits.
#[test]
fn sweep_against_a_live_backup_refuses() {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let gb = sim.gate(
        GateRule::new(
            b,
            OpMatch::Kind(OpKind::List),
            KeyMatch::Exact("locks/".into()),
        )
        .nth(2),
    );
    let sb = sim.spawn(
        b,
        Call::Backup {
            snapshot: "b1".into(),
            tree: tree("b1"),
        },
    );
    sim.expect_paused(gb, "b's commit-lock sweep");
    assert!(!sim.store().keys_written_by(b, "packs/").is_empty());
    let ra = sim.compact(a);
    assert!(
        allowed_as(&ra, AllowedError::ActiveSessions),
        "{:?}",
        ra.outcome
    );
    assert!(
        mutations_under(&sim, a, ra.index, &["packs/", "index.h/", "index.d/"]).is_empty(),
        "compact mutated under a live session"
    );
    sim.resume(gb);
    sim.join(sb).expect_ok();
    sim.check_invariants(interleavings::CHECKED).assert_clean();
}

/// c0, t0 and t1 (t1 shares t0's first file), then t0 deleted: a compact
/// has t0's other chunks to sweep.
fn sweepable(sim: &mut Sim, c: ClientId) {
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let t0 = SourceTree::seeded("x", 3, 64 * KIB);
    sim.backup(c, "t0", &t0).expect_ok();
    let t1 = SourceTree::new().file(
        "f000.bin",
        crate::sim::runner::seeded_bytes("x/0", 64 * KIB),
    );
    sim.backup(c, "t1", &t1).expect_ok();
    sim.delete(c, &["t0"]).expect_ok();
}

/// SIM-2: a backup that starts while a compact is between its sweep's
/// first index write and its head is refused (the lock is held); the
/// compact completes and the repository is consistent.
#[test]
fn backup_during_a_sweep_is_locked_out() {
    for prefix in ["index.h/", "index.d/"] {
        let mut sim = Sim::new();
        let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
        sweepable(&mut sim, c);
        let ga = sim.gate(GateRule::new(
            a,
            OpMatch::AnyWrite,
            KeyMatch::Prefix(prefix.into()),
        ));
        let sa = sim.spawn(a, Call::Compact);
        sim.expect_paused(ga, "compact a's first index write");
        let rb = sim.backup(b, "b1", &tree("b1"));
        assert!(
            allowed_as(&rb, AllowedError::Locked),
            "{prefix}: {:?}",
            rb.outcome
        );
        sim.resume(ga);
        let ra = sim.join(sa);
        ra.expect_ok();
        assert!(ra.detail().contains("unreferenced"), "{}", ra.detail());
        assert!(!ra.detail().contains("unreferenced 0"), "{}", ra.detail());
        sim.check_invariants(interleavings::CHECKED).assert_clean();
    }
}

/// SIM-3: a crash, or an applied write whose answer is lost, at every
/// mutation of a sweeping compact leaves the repository consistent; once
/// the lock is stale another client's compact completes the sweep.
#[test]
fn a_crash_at_every_write_of_the_sweep_is_recoverable() {
    let probe = {
        let mut sim = Sim::new();
        let (a, c) = (sim.client("a"), sim.client("c"));
        sweepable(&mut sim, c);
        let run = sim.compact(a);
        run.expect_ok();
        sim.store()
            .log()
            .iter()
            .filter(|r| {
                r.client == a && r.run == Some(run.index) && r.applied() && r.op.is_mutation()
            })
            .count() as u32
    };
    assert!(probe > 3, "the probe compact made {probe} mutations");
    for k in 1..=probe {
        for action in [FaultAction::Crash, FaultAction::FailApplied] {
            let case = format!("k={k} {action:?}");
            let mut sim = Sim::new();
            let (a, c) = (sim.client("a"), sim.client("c"));
            sweepable(&mut sim, c);
            sim.fault(
                FaultRule::new(OpMatch::AnyMutation, KeyMatch::Any, action)
                    .client(a)
                    .nth(k),
            );
            let ra = sim.compact(a);
            let faulted = sim
                .store()
                .log()
                .iter()
                .filter(|r| r.client == a && r.run == Some(ra.index) && r.op.is_mutation())
                .nth(k as usize - 1)
                .map(|r| r.key.clone());
            let report = sim.check_invariants(Invariant::STEP1);
            let trace: Vec<String> = sim
                .store()
                .log()
                .iter()
                .filter(|r| r.client == a && r.run == Some(ra.index) && r.op.is_mutation())
                .map(|r| format!("{:?} {} {:?}", r.op, r.key, r.outcome))
                .collect();
            assert!(
                report.is_clean(),
                "{case} at {faulted:?}: {:?}\n{trace:#?}\n{report}",
                ra.outcome
            );
            sim.advance(Duration::from_secs(6 * 3600 + 60));
            let rc = sim.compact(c);
            assert!(rc.is_ok(), "{case} at {faulted:?}: {}", rc.detail());
            let report = sim.check_invariants(Invariant::STEP1);
            assert!(report.is_clean(), "{case} at {faulted:?}: {report}");
            let again = sim.compact(c);
            assert!(
                again.detail().ends_with("unreferenced 0"),
                "{case} at {faulted:?}: {}",
                again.detail()
            );
        }
    }
}

/// SIM-4: a reaped session that commits afterwards (H2-marker's net
/// effect, modelled with tamper). The sweep removed t0's entries it reused
/// and compact deleted its uploaded pack, so its commit fails as stale and
/// no snapshot is written.
#[test]
fn a_reaped_session_that_commits_afterwards_fails_as_stale() {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let x = SourceTree::seeded("x", 3, 64 * KIB);
    sim.backup(c, "t0", &x).expect_ok();
    sim.delete(c, &["t0"]).expect_ok();

    let ga = sim.gate(
        GateRule::new(
            a,
            OpMatch::Kind(OpKind::List),
            KeyMatch::Exact("locks/".into()),
        )
        .nth(2),
    );
    let sa = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: x.file("new.bin", b"a file of its own".to_vec()),
        },
    );
    sim.expect_paused(ga, "a's commit-lock sweep");
    assert!(!sim.store().keys_written_by(a, "packs/").is_empty());
    let marker = sim
        .store()
        .keys_written_by(a, "sessions/")
        .into_iter()
        .find(|k| k.ends_with(".json"))
        .expect("a's marker");
    let bytes = sim.store().object(&marker).expect("marker present").data;
    assert!(sim.tamper_delete(&marker));

    let rb = sim.compact(b);
    rb.expect_ok();
    assert!(!rb.detail().ends_with("unreferenced 0"), "{}", rb.detail());
    let a_packs = sim.store().keys_written_by(a, "packs/");
    assert!(
        a_packs.iter().all(|k| sim.store().object(k).is_none()),
        "orphans deleted"
    );

    sim.tamper_put(&marker, &bytes);
    sim.resume(ga);
    let ra = sim.join(sa);
    assert!(
        allowed_as(&ra, AllowedError::StaleChunksDuringCommit),
        "{:?}",
        ra.outcome
    );
    let names: Vec<String> = sim
        .history()
        .observations
        .last()
        .unwrap()
        .names()
        .map(String::from)
        .collect();
    assert!(!names.contains(&"a1".to_string()), "{names:?}");
    sim.check_invariants(interleavings::CHECKED).assert_clean();
}

/// SIM-5 (the H5 regression is in `regressions_hold`): a head that drops a
/// live snapshot's entries makes the next compact refuse, and no pack goes.
#[test]
fn a_head_that_drops_live_entries_makes_compact_refuse() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    sim.backup(c, "t1", &tree("t1")).expect_ok();
    sim.custom(c, "drop t1's entries", |repo| {
        repo.load_chunk_index_under_lock()?;
        let items = crate::commands::list::load_snapshot_items(
            repo,
            "t1",
            &crate::repo::control::CancelToken::new(),
        )?;
        let ids: Vec<_> = items
            .iter()
            .flat_map(|i| i.chunks.iter().map(|c| c.id))
            .collect();
        for id in ids {
            repo.chunk_index_mut().remove(&id);
        }
        repo.save_state()
    })
    .expect_ok();
    let packs_before = sim.store().keys("packs/");
    let run = sim.compact(c);
    assert!(
        allowed_as(&run, AllowedError::MarkRefused),
        "{:?}",
        run.outcome
    );
    assert_eq!(sim.store().keys("packs/"), packs_before, "no pack deleted");
    let report = sim.check_invariants(Invariant::STEP1);
    assert!(
        report.violated_for(Invariant::CommittedSnapshotsRestore, "t1"),
        "{report}"
    );
    assert!(
        !report.violated_for(Invariant::CommittedSnapshotsRestore, "c0"),
        "{report}"
    );
    for inv in [
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::IndexIntegrity,
        Invariant::AllowedErrors,
    ] {
        assert!(!report.violated(inv), "{inv:?}: {report}");
    }
}
