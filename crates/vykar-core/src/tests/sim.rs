use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::config::RetentionConfig;
use crate::sim::checker::{self, Invariant, ViolationKind, head_object_keys};
use crate::sim::hazards::{self, HazardOutcome, KNOWN_HAZARDS, LateHead, REGRESSIONS};
use crate::sim::history::{AllowedError, ErrorClass, INDEX_CHANGED_TEXT, RunOutcome, classify};
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

    // t0's segment is seconds old: the sweep keeps its entries (04 step 7);
    // check --repair then removes them (it runs without live sessions).
    let compact = sim.compact(c);
    compact.expect_ok();
    assert!(
        compact_stat(compact.detail(), "kept-young") > 0,
        "{}",
        compact.detail()
    );
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

/// Readers register no marker and take no lock (04 step 7): a restore and a
/// check make no request under `sessions/` or `locks/`.
#[test]
fn readers_register_no_marker() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let runs = [sim.check(c), sim.restore(c, "t0")];
    let log = sim.store().log();
    for run in &runs {
        run.expect_ok();
        let protocol: Vec<_> = log
            .iter()
            .filter(|r| r.run == Some(run.index))
            .filter(|r| r.key.starts_with("sessions/") || r.key.starts_with("locks/"))
            .collect();
        assert!(protocol.is_empty(), "run {}: {protocol:?}", run.index);
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
        VykarError::Other(format!(
            "{} 11 h 20 min since its first object upload; retry",
            crate::repo::ages::SLOW_INDEX_WRITE_TEXT
        )),
        AllowedError::SlowIndexWrite,
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

    // The typed forms classify like their text forms above.
    allowed(VykarError::IndexChanged, AllowedError::IndexChanged);
    allowed(
        crate::repo::index_write::head_contention_error(5, 3),
        AllowedError::IndexChanged,
    );
    allowed(
        VykarError::MarkRefused(Box::new(VykarError::Other("x".into()))),
        AllowedError::MarkRefused,
    );
    // A refused mark caused by an index change is the change (the text arm
    // for "index changed" came first before the variant existed).
    allowed(
        VykarError::MarkRefused(Box::new(VykarError::IndexChanged)),
        AllowedError::IndexChanged,
    );
    for head_written in [false, true] {
        allowed(
            VykarError::SlowIndexWrite {
                limit_mins: 680,
                head_written,
            },
            AllowedError::SlowIndexWrite,
        );
    }
    allowed(
        VykarError::Other(format!(
            "{}, nothing was changed: x. Run `vykar check` for details",
            crate::commands::compact::MARK_REFUSED
        )),
        AllowedError::MarkRefused,
    );
    assert_eq!(
        classify(&VykarError::PackNotFound(
            vykar_types::pack_id::PackId::from_bytes([1; 32])
        )),
        ErrorClass::Disallowed
    );
}

#[test]
fn typed_errors_keep_the_text_prefixes() {
    let refused = VykarError::MarkRefused(Box::new(VykarError::Other("x".into())));
    assert!(
        refused
            .to_string()
            .starts_with(crate::commands::compact::MARK_REFUSED),
        "{refused}"
    );
    for head_written in [false, true] {
        let slow = VykarError::SlowIndexWrite {
            limit_mins: 680,
            head_written,
        };
        assert!(
            slow.to_string()
                .starts_with(crate::repo::ages::SLOW_INDEX_WRITE_TEXT),
            "{slow}"
        );
    }
    for e in [
        VykarError::IndexChanged,
        VykarError::IndexHeadContention { detail: "x".into() },
    ] {
        assert!(e.to_string().contains(INDEX_CHANGED_TEXT), "{e}");
    }
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
    sim.fault(FaultRule::new(OpMatch::AnyWrite, KeyMatch::LockKey, FaultAction::Crash).client(a));
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

/// A fault that crashes `c` on its first delete of a lock key: its lock
/// release, so the lock stays behind.
fn crash_on_lock_release(sim: &Sim, c: ClientId) {
    sim.fault(FaultRule::new(OpMatch::AnyDelete, KeyMatch::LockKey, FaultAction::Crash).client(c));
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

    // Since 04 step 7 compact runs beside the young marker and keeps it.
    sim.compact(c).expect_ok();
    assert!(sim.store().object(&marker).is_some());
    sim.advance(Duration::from_secs(46 * 60));
    sim.compact(c).expect_ok();
    assert!(sim.store().object(&marker).is_none());
    sim.check_invariants(Invariant::STEP7).assert_clean();
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

/// `a`'s backup crashes at its commit lock's PUT, after every pack is
/// uploaded, leaving its marker and journal behind. Returns the marker key
/// and a's packs.
fn crash_backup_after_upload(
    sim: &mut Sim,
    a: ClientId,
    tree: &SourceTree,
) -> (String, Vec<String>) {
    sim.fault(FaultRule::new(OpMatch::AnyWrite, KeyMatch::LockKey, FaultAction::Crash).client(a));
    let r = sim.backup(a, "x1", tree);
    assert!(allowed_as(&r, AllowedError::Injected), "{}", r.detail());
    let marker = sim
        .store()
        .keys_written_by(a, "sessions/")
        .into_iter()
        .find(|k| k.ends_with(".json"))
        .expect("a's marker");
    assert!(sim.store().object(&marker).is_some(), "the marker is left");
    let packs = sim.store().keys_written_by(a, "packs/");
    assert!(!packs.is_empty(), "a uploaded before the crash");
    (marker, packs)
}

/// The count `name` of a compact run's detail ("packs 4 ... live 0").
fn compact_stat(detail: &str, name: &str) -> u64 {
    let words: Vec<&str> = detail.split_whitespace().collect();
    words
        .iter()
        .position(|w| *w == name)
        .and_then(|i| words.get(i + 1))
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no {name} in {detail:?}"))
}

/// 04 step 7's done criterion: a crashed backup no longer blocks compact.
/// Compact runs at once and keeps the crashed backup's uploads (young, and
/// inside its live marker's horizon); 46 min later the marker is reaped
/// and the uploads are still kept (young); T_pack later they go.
#[test]
fn a_crashed_backup_no_longer_blocks_compact() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    let (marker, packs) = crash_backup_after_upload(&mut sim, a, &tree("x1"));

    let r = sim.compact(b);
    r.expect_ok();
    let kept = compact_stat(r.detail(), "young") + compact_stat(r.detail(), "live");
    assert!(kept >= packs.len() as u64, "{}", r.detail());
    assert!(sim.store().object(&marker).is_some());

    sim.advance(Duration::from_secs(46 * 60));
    let r = sim.compact(b);
    r.expect_ok();
    assert!(sim.store().object(&marker).is_none(), "reaped once stale");
    assert!(
        compact_stat(r.detail(), "young") >= packs.len() as u64,
        "{}",
        r.detail()
    );
    for k in &packs {
        assert!(sim.store().object(k).is_some(), "{k} deleted while young");
    }

    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
    sim.compact(b).expect_ok();
    for k in &packs {
        assert!(sim.store().object(k).is_none(), "{k} kept once old");
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
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

/// H5-repair is a regression: repair re-indexes b1 from b's packs, the
/// compact T_pack later deletes none of them and b1 restores.
#[test]
fn h5_repair_reindexes_the_lost_update() {
    expect_not_reproduced("H5-repair", hazards::h5_repair_with(LateHead::AfterB));
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
    "H4-skew",
    "H3-compact-young",
    "H-racy",
    "H3-conditional",
    "H3-compact-conditional",
    "H-mixed-aged-lock",
    "H-mixed-late-create",
    "H-committing-reaped",
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

/// Every regression holds on each store kind it names.
#[test]
fn regressions_hold() {
    for r in REGRESSIONS {
        hazards::assert_regression_holds(r.id);
    }
}

/// H-racy (04 step 8): a store that passes the sequential check but lets a
/// late create win replaces a head.
#[test]
fn known_hazard_h_racy() {
    hazards::assert_reproduces("H-racy");
}

/// H-mixed-aged-lock (phase F part 10c, R4): a lock-mode backup whose lock
/// aged out while it stalled before its plain head PUT replaces the head a
/// lock-free backup created meanwhile.
#[test]
fn known_hazard_h_mixed_aged_lock() {
    hazards::assert_reproduces("H-mixed-aged-lock");
}

/// H-mixed-late-create (phase F part 10c): a failed lock-free backup's
/// late head create is replaced by a lock-mode backup's plain head PUT;
/// the overwrite is the only damage.
#[test]
fn known_hazard_h_mixed_late_create() {
    hazards::assert_reproduces("H-mixed-late-create");
}

/// H-committing-reaped (phase F part 10d, R1): a lock-free backup whose
/// committing marker is reaped by age after its marker probe, before its
/// snapshot PUT, commits a snapshot a lock-free compact swept meanwhile.
#[test]
fn known_hazard_h_committing_reaped() {
    hazards::assert_reproduces("H-committing-reaped");
}

/// What each known hazard does on a store that honours put-if-absent: H3,
/// H3-compact and H4 are closed (their twins are regressions), the lock
/// hazards remain.
#[test]
fn known_hazards_on_a_conditional_store() {
    for h in KNOWN_HAZARDS {
        hazards::assert_conditional_expectation(h.id);
    }
}

// ── Scripted pause points (08 step 3) ───────────────────────────────────

fn sessions_list(c: ClientId) -> GateRule {
    GateRule::new(c, OpMatch::AnyList, KeyMatch::Exact("sessions/".into()))
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
    // The deleted and pruned snapshots' segments are young: their entries
    // stay (04 step 7) until check --repair removes them.
    let compact = sim.compact(c);
    compact.expect_ok();
    assert!(
        compact_stat(compact.detail(), "kept-young") > 0,
        "{}",
        compact.detail()
    );
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

/// A lock whose body and key carry no time used to wedge the repository;
/// since 04 step 5 it is aged by its mtime and reaped after 6 h. The check
/// runs on a fork: the store is untouched.
#[test]
fn unparseable_lock_ages_out_by_mtime() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.tamper_put("locks/planted.json", b"{}");
    let (keys, log_len) = (sim.store().keys(""), sim.store().log().len());
    let report = sim.check_invariants(&[Invariant::NoPermanentWedge]);
    report.assert_clean();
    assert_eq!(sim.store().keys(""), keys);
    assert_eq!(sim.store().log().len(), log_len);
}

/// A malformed marker is no longer a documented exception of invariant 9:
/// it ages out by its mtime.
#[test]
fn wedge_check_ages_out_malformed_marker() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.tamper_put("sessions/bad.json", b"not json");
    let report = sim.check_invariants(&[Invariant::NoPermanentWedge]);
    report.assert_clean();
    assert!(
        !report.not_evaluated_for(Invariant::NoPermanentWedge, "sessions/bad.json"),
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

/// Both placements of the H2-marker regression, one by one (a clearer
/// failure than the regression report).
#[test]
fn h2_marker_placements_not_reproduced() {
    for reap in [MarkerReap::BeforeRefresh, MarkerReap::AfterPut] {
        expect_not_reproduced("H2-marker", interleavings::h2_marker_with(reap));
    }
}

#[test]
fn neutralised_h4_not_reproduced() {
    expect_not_reproduced(
        "H4",
        interleavings::h4_with(Hosts::StoreStepped, Order::Sequential),
    );
}

#[test]
fn neutralised_h4_segment_not_reproduced() {
    expect_not_reproduced(
        "H4-segment",
        interleavings::h4_segment_with(Hosts::StoreStepped, Order::Sequential),
    );
}

/// Without a store clock step no client reaps compact's live lock: on
/// distinct hosts, since 04 step 1 on cloned machines either (the H1 fix
/// seen through H4), and since 04 step 5 with b's clock 6 h 1 min ahead.
#[test]
fn h4_without_a_reaper_has_no_prerequisite() {
    for hosts in [Hosts::Distinct, Hosts::SameHostname, Hosts::Skewed] {
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

/// A live marker written before 04 step 7 (no `elapsed_secs`) gives compact
/// no horizon: it deletes no orphan pack, not even a crashed backup's 12 h
/// old ones, nor the old packs its own sweep emptied (since phase F part 8c
/// those obey I1 like any orphan); repack goes on.
#[test]
fn an_unknown_horizon_keeps_every_orphan() {
    let mut sim = Sim::new();
    let (x, c) = (sim.client("x"), sim.client("c"));
    let (_marker, packs) = crash_backup_after_upload(&mut sim, x, &tree("x1"));
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
    sweepable(&mut sim, c);
    let now = sim.time().now_utc().to_rfc3339();
    let body = serde_json::json!({
        "hostname": "elsewhere",
        "pid": 4242,
        "registered_at": now,
        "last_refresh": now,
    });
    sim.tamper_put(
        "sessions/old-writer.json",
        &serde_json::to_vec(&body).unwrap(),
    );
    let r = sim.compact(c);
    r.expect_ok();
    let d = r.detail();
    assert!(compact_stat(d, "unreferenced") > 0, "{d}");
    // The crashed backup's packs and the ones the sweep emptied.
    assert!(compact_stat(d, "live") > packs.len() as u64, "{d}");
    assert_eq!(compact_stat(d, "deleted_empty"), 0, "{d}");
    assert!(compact_stat(d, "repacked") > 0, "{d}");
    for k in &packs {
        assert!(
            sim.store().object(k).is_some(),
            "{k} deleted without a horizon"
        );
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// SIM-1: a compact while a backup is between its uploads and its commit
/// runs beside it (04 step 7) and deletes none of its packs; the backup
/// then commits and restores.
#[test]
fn a_sweep_beside_a_live_backup_keeps_its_uploads() {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let gb = sim.gate(GateRule::new(b, OpMatch::AnyList, KeyMatch::Exact("locks/".into())).nth(2));
    let sb = sim.spawn(
        b,
        Call::Backup {
            snapshot: "b1".into(),
            tree: tree("b1"),
        },
    );
    sim.expect_paused(gb, "b's commit-lock sweep");
    let b_packs = sim.store().keys_written_by(b, "packs/");
    assert!(!b_packs.is_empty());
    let ra = sim.compact(a);
    ra.expect_ok();
    let deleted = mutations_under(&sim, a, ra.index, &["packs/"]);
    assert!(
        b_packs.iter().all(|k| !deleted.contains(k)),
        "compact deleted a live backup's pack: {deleted:?}"
    );
    sim.resume(gb);
    sim.join(sb).expect_ok();
    sim.restore(c, "b1").expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// c0, t0 and t1 (t1 shares t0's first file), then t0 deleted and T_pack
/// passed: a compact has t0's other chunks to sweep.
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
    // t0's segment is old: the sweep takes its entries (04 step 7).
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
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
                again.detail().contains(" unreferenced 0 "),
                "{case} at {faulted:?}: {}",
                again.detail()
            );
        }
    }
}

/// SIM-4: a reaped session that commits afterwards (H2-marker's net
/// effect, modelled with tamper). t0 was deleted more than T_pack ago, so
/// the sweep removed t0's entries it reused, and its commit fails as stale
/// and no snapshot is written (contrast
/// `a_dedup_hit_on_a_fresh_chunk_survives_a_concurrent_sweep`). Its own packs,
/// seconds old, are orphans younger than T_pack: compact keeps them (04
/// step 5).
#[test]
fn a_reaped_session_that_commits_afterwards_fails_as_stale() {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let x = SourceTree::seeded("x", 3, 64 * KIB);
    sim.backup(c, "t0", &x).expect_ok();
    sim.delete(c, &["t0"]).expect_ok();
    // t0's segment is old, so the sweep takes the entries a reuses (04
    // step 7 keeps those of segments younger than T_pack).
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));

    let ga = sim.gate(GateRule::new(a, OpMatch::AnyList, KeyMatch::Exact("locks/".into())).nth(2));
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
        a_packs.iter().all(|k| sim.store().object(k).is_some()),
        "young orphans kept"
    );
    let young: usize = rb
        .detail()
        .split_once(" young ")
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .expect("compact detail names its young orphans");
    assert!(young >= a_packs.len(), "{}", rb.detail());

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

/// D9's other half: a pack of a backup goes missing while its marker is
/// present. That is storage loss, not a reaped session: the commit keeps the
/// untyped "pack ... missing" error.
#[test]
fn missing_pack_with_a_live_marker_stays_untyped() {
    let mut sim = Sim::new();
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let ga = sim.gate(GateRule::new(a, OpMatch::AnyList, KeyMatch::Exact("locks/".into())).nth(2));
    let sa = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: SourceTree::seeded("own", 3, 64 * KIB),
        },
    );
    sim.expect_paused(ga, "a's commit-lock sweep");
    let pack = sim
        .store()
        .keys_written_by(a, "packs/")
        .into_iter()
        .next()
        .expect("a uploaded a pack");
    assert!(sim.tamper_delete(&pack));
    let marker = sim
        .store()
        .keys_written_by(a, "sessions/")
        .into_iter()
        .find(|k| k.ends_with(".json"))
        .expect("a's marker");
    assert!(sim.store().object(&marker).is_some(), "the marker is live");

    sim.resume(ga);
    let ra = sim.join(sa);
    assert!(
        !allowed_as(&ra, AllowedError::StaleChunksDuringCommit),
        "{:?}",
        ra.outcome
    );
    assert!(
        ra.detail().contains("missing from storage"),
        "{}",
        ra.detail()
    );
}

/// A reaped session whose packs compact deleted, with nothing reused that
/// the sweep removed (a tree of its own), so the reuse re-check passes and
/// the pack check fails: typed as stale (D9). The store's clock steps
/// T_pack forward before the compact, so a's packs are old orphans (04
/// step 5 keeps younger ones) while no client's time moves. The commit's first marker
/// probe is made inconclusive (an injected EXISTS error, with a continuous
/// record it falls through), else it would refuse first; the re-probe after
/// the failed pack check finds the marker gone.
#[test]
fn reaped_marker_and_deleted_packs_fail_typed() {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let x = SourceTree::seeded("x", 3, 64 * KIB);
    sim.backup(c, "t0", &x).expect_ok();
    sim.delete(c, &["t0"]).expect_ok();

    let ga = sim.gate(GateRule::new(a, OpMatch::AnyList, KeyMatch::Exact("locks/".into())).nth(2));
    let sa = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: SourceTree::seeded("own", 3, 64 * KIB),
        },
    );
    sim.expect_paused(ga, "a's commit-lock sweep");
    let marker = sim
        .store()
        .keys_written_by(a, "sessions/")
        .into_iter()
        .find(|k| k.ends_with(".json"))
        .expect("a's marker");
    assert!(sim.tamper_delete(&marker));
    sim.step_store_clock(
        chrono::Duration::from_std(crate::repo::ages::T_PACK).unwrap()
            + chrono::Duration::minutes(1),
    );
    let rb = sim.compact(b);
    rb.expect_ok();
    let a_packs = sim.store().keys_written_by(a, "packs/");
    assert!(!a_packs.is_empty());
    assert!(
        a_packs.iter().all(|k| sim.store().object(k).is_none()),
        "orphans deleted"
    );
    sim.fault(
        FaultRule::new(
            OpMatch::Kind(OpKind::Exists),
            KeyMatch::Prefix("sessions/".into()),
            FaultAction::Fail,
        )
        .client(a),
    );

    sim.resume(ga);
    let ra = sim.join(sa);
    assert!(
        allowed_as(&ra, AllowedError::StaleChunksDuringCommit),
        "{:?}",
        ra.outcome
    );
    let probes: Vec<OpOutcome> = sim
        .store()
        .log()
        .into_iter()
        .filter(|r| r.client == a && r.op == OpKind::Exists && r.key.starts_with("sessions/"))
        .map(|r| r.outcome)
        .collect();
    assert_eq!(probes.len(), 2, "the probe and the re-probe: {probes:?}");
    sim.check_invariants(interleavings::CHECKED).assert_clean();
}

/// 04 step 2, A1, with 04 step 7's pack-age term: the registration key's
/// presence check fails at the heartbeat's refresh and again at the
/// commit's probe. Whether a reaper deleted the key after the refresh's
/// PUT or not, the record is not sound; the backup's uploads are minutes
/// old, young enough to be kept by compact's age guard, so it commits and
/// its snapshot restores. (With old packs it refuses with the probe's
/// error: `tests::lock::commit_marker_check_rules`.)
#[test]
fn a_marker_key_that_stays_unchecked_commits_while_young() {
    const WAIT: Duration = Duration::from_secs(5);
    for reaped in [true, false] {
        let mut sim = Sim::new();
        let c = sim.client("c");
        sim.backup(c, "t0", &tree("t0")).expect_ok();
        let a = sim.client("a");
        // A's first main-thread operation after the heartbeat started.
        let g_main = sim.gate(GateRule::new(
            a,
            OpMatch::AnyList,
            KeyMatch::Exact("sessions/".into()),
        ));
        let sa = sim.spawn(
            a,
            Call::Backup {
                snapshot: "a1".into(),
                tree: tree("a1"),
            },
        );
        sim.expect_paused(g_main, "a's journal-recovery LIST");
        let gen0 = sim
            .store()
            .keys_written_by(a, "sessions/")
            .into_iter()
            .find(|k| k.ends_with(".json"))
            .expect("a's marker");
        // The refresh's check, then the commit probe's.
        for nth in [1, 2] {
            sim.fault(
                FaultRule::new(
                    OpMatch::Kind(OpKind::Exists),
                    KeyMatch::Exact(gen0.clone()),
                    FaultAction::Fail,
                )
                .client(a)
                .nth(nth),
            );
        }
        // The heartbeat's first refresh: 30 polls of 30 s.
        assert!(sim.time().wait_for_waiters(1, WAIT));
        for _ in 0..30 {
            sim.advance(Duration::from_secs(30));
            assert!(sim.time().wait_for_waiters(1, WAIT));
        }
        assert!(
            sim.store()
                .keys_written_by(a, "sessions/")
                .iter()
                .any(|k| k.ends_with(".1.json")),
            "the refresh wrote generation 1"
        );
        if reaped {
            assert!(sim.tamper_delete(&gen0));
        }

        sim.resume(g_main);
        let ra = sim.join(sa);
        assert!(ra.is_ok(), "reaped {reaped}: {:?}", ra.outcome);
        assert!(
            !sim.store().keys_written_by(a, "snapshots/").is_empty(),
            "reaped {reaped}: a wrote its snapshot"
        );
        sim.restore(c, "a1").expect_ok();
        sim.check_invariants(Invariant::STEP7).assert_clean();
    }
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

// ── Store-clock ages (04 step 5) ────────────────────────────────────────

fn hours(h: i64) -> chrono::Duration {
    chrono::Duration::hours(h)
}

/// a's compact is held at its first `index.h/` write, holding the lock; b
/// backs up with its wall clock skewed by `b_skew`, a's by `a_skew`.
fn lock_reap_attempt(a_skew: chrono::Duration, b_skew: chrono::Duration) {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.set_skew(a, a_skew);
    sim.set_skew(b, b_skew);
    let ga = sim.gate(GateRule::new(
        a,
        OpMatch::AnyWrite,
        KeyMatch::Prefix("index.h/".into()),
    ));
    let sa = sim.spawn(a, Call::Compact);
    sim.expect_paused(ga, "a's first head PUT");
    let rb = sim.backup(b, "b1", &tree("b1"));
    assert!(allowed_as(&rb, AllowedError::Locked), "{:?}", rb.outcome);
    sim.resume(ga);
    sim.join(sa).expect_ok();
    sim.check_invariants(interleavings::CHECKED).assert_clean();
}

/// a's backup is held at its commit-lock sweep, its marker live; b
/// compacts with its wall clock skewed by `b_skew`, a's by `a_skew`.
fn marker_reap_attempt(a_skew: chrono::Duration, b_skew: chrono::Duration) {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.set_skew(a, a_skew);
    sim.set_skew(b, b_skew);
    let ga = sim.gate(GateRule::new(a, OpMatch::AnyList, KeyMatch::Exact("locks/".into())).nth(2));
    let sa = sim.spawn(a, backup_call("a1"));
    sim.expect_paused(ga, "a's commit-lock sweep");
    let marker = sim
        .store()
        .keys_written_by(a, "sessions/")
        .into_iter()
        .find(|k| k.ends_with(".json"))
        .expect("a's marker");
    let rb = sim.compact(b);
    rb.expect_ok();
    assert!(
        sim.store().object(&marker).is_some(),
        "a's live marker reaped"
    );
    sim.resume(ga);
    sim.join(sa).expect_ok();
    sim.check_invariants(interleavings::CHECKED).assert_clean();
}

/// A client 7 h ahead no longer reaps a live lock (it was older than 6 h
/// by that client's clock).
#[test]
fn client_ahead_does_not_reap_a_live_lock() {
    lock_reap_attempt(chrono::Duration::zero(), hours(7));
}

/// A client 1 h ahead no longer reaps a live marker.
#[test]
fn client_ahead_does_not_reap_a_live_marker() {
    marker_reap_attempt(chrono::Duration::zero(), hours(1));
}

/// A holder whose clock is behind writes entries that look old to others'
/// clocks; they are judged by their mtimes and not reaped early.
#[test]
fn client_behind_is_not_reaped_early() {
    lock_reap_attempt(hours(-7), chrono::Duration::zero());
    marker_reap_attempt(hours(-1), chrono::Duration::zero());
}

/// a's backup fails after its uploads. A compact 46 min later by a client
/// 13 h ahead keeps a's packs (younger than T_pack on the store's clock);
/// T_pack later the next compact deletes them.
#[test]
fn young_orphans_survive_a_compact_hours_ahead() {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.fault(
        FaultRule::new(
            OpMatch::AnyWrite,
            KeyMatch::Prefix("index.d/".into()),
            FaultAction::Fail,
        )
        .client(a),
    );
    let ra = sim.backup(a, "a1", &tree("a1"));
    assert!(!ra.is_ok(), "{:?}", ra.outcome);
    let a_packs = sim.store().keys_written_by(a, "packs/");
    assert!(!a_packs.is_empty());

    sim.advance(Duration::from_secs(46 * 60));
    sim.set_skew(b, hours(13));
    sim.compact(b).expect_ok();
    for k in &a_packs {
        assert!(sim.store().object(k).is_some(), "{k} deleted while young");
    }

    sim.advance(crate::repo::ages::T_PACK);
    sim.compact(b).expect_ok();
    for k in &a_packs {
        assert!(sim.store().object(k).is_none(), "{k} kept once old");
    }
    sim.check_invariants(interleavings::CHECKED).assert_clean();
}

// ── Young entries (04 step 7) ──────────────────────────────────────────

/// The packs `c` wrote for its backup of `name` (`tree`).
fn backup_packs(sim: &mut Sim, c: ClientId, name: &str, tree: &SourceTree) -> Vec<String> {
    let before = sim.store().keys_written_by(c, "packs/");
    sim.backup(c, name, tree).expect_ok();
    let packs: Vec<String> = sim
        .store()
        .keys_written_by(c, "packs/")
        .into_iter()
        .filter(|k| !before.contains(k))
        .collect();
    assert!(!packs.is_empty(), "{name} wrote no pack");
    packs
}

/// A sweep leaves the entries of a backup segment younger than T_pack: a
/// snapshot deleted right after its backup keeps its chunks and packs
/// through a compact. Once T_pack has passed, a compact sweeps them and
/// deletes the packs it emptied (old by then).
#[test]
fn young_backup_entries_survive_a_sweep() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let t0_packs = backup_packs(&mut sim, c, "t0", &tree("t0"));
    sim.delete(c, &["t0"]).expect_ok();
    let r = sim.compact(c);
    r.expect_ok();
    let d = r.detail();
    assert!(compact_stat(d, "kept-young") > 0, "{d}");
    assert_eq!(compact_stat(d, "unreferenced"), 0, "{d}");
    for k in &t0_packs {
        assert!(sim.store().object(k).is_some(), "{k} deleted young");
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();

    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
    let r = sim.compact(c);
    r.expect_ok();
    let d = r.detail();
    assert!(compact_stat(d, "unreferenced") > 0, "{d}");
    assert_eq!(compact_stat(d, "kept-young"), 0, "{d}");
    for k in &t0_packs {
        assert!(sim.store().object(k).is_none(), "{k} kept once old");
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// 04's motivating case: a backup that reuses chunks of a snapshot deleted
/// minutes after its backup survives a compact beside it. The sweep keeps
/// t0's entries (its segment is young), so a's reuse holds, no pack of a or
/// t0 goes, and a commits and restores. Contrast SIM-4
/// (`a_reaped_session_that_commits_afterwards_fails_as_stale`), where t0's
/// segment is old.
#[test]
fn a_dedup_hit_on_a_fresh_chunk_survives_a_concurrent_sweep() {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let x = SourceTree::seeded("x", 3, 64 * KIB);
    let t0_packs = backup_packs(&mut sim, c, "t0", &x);
    sim.delete(c, &["t0"]).expect_ok();

    let ga = sim.gate(GateRule::new(a, OpMatch::AnyList, KeyMatch::Exact("locks/".into())).nth(2));
    let sa = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: x.clone().file("new.bin", b"a file of its own".to_vec()),
        },
    );
    sim.expect_paused(ga, "a's commit-lock sweep");
    let a_packs = sim.store().keys_written_by(a, "packs/");
    assert!(!a_packs.is_empty());
    let rb = sim.compact(b);
    rb.expect_ok();
    let detail = rb.detail();
    assert!(compact_stat(detail, "kept-young") > 0, "{detail}");
    assert_eq!(compact_stat(detail, "unreferenced"), 0, "{detail}");
    let deleted = mutations_under(&sim, b, rb.index, &["packs/"]);
    assert!(
        a_packs
            .iter()
            .chain(&t0_packs)
            .all(|k| !deleted.contains(k)),
        "{deleted:?}"
    );
    sim.resume(ga);
    sim.join(sa).expect_ok();
    let restore = sim.restore(c, "a1");
    restore.expect_ok();
    let restored = SourceTree::read_from(&sim.restore_dir(&restore)).unwrap();
    assert_eq!(restored, x.file("new.bin", b"a file of its own".to_vec()));
    sim.check_invariants(interleavings::CHECKED).assert_clean();
}

/// Packs a sweep empties obey I1 in full (phase F part 8c): a compact with
/// no segment grace sweeps t0's fresh entries at once, and keeps t0's
/// packs, now without entries, because they are younger than T_pack.
/// T_pack later a normal compact deletes them.
#[test]
fn a_pack_the_sweep_empties_is_kept_while_young() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let t0_packs = backup_packs(&mut sim, c, "t0", &tree("t0"));
    sim.delete(c, &["t0"]).expect_ok();
    let mut stats = None;
    sim.custom(
        c,
        "compact without segment grace",
        |repo: &mut crate::repo::RepoWriter<'_>| {
            repo.load_chunk_index_under_lock()?;
            repo.set_segment_grace(Duration::ZERO);
            stats = Some(crate::commands::compact::compact_repo(
                repo,
                10.0,
                None,
                false,
                1,
                &crate::repo::control::CancelToken::new(),
            )?);
            Ok(())
        },
    )
    .expect_ok();
    let stats = stats.expect("the compact ran");
    assert!(stats.chunks_unreferenced > 0, "{stats:?}");
    assert_eq!(stats.chunks_unreferenced_young, 0, "{stats:?}");
    assert!(
        stats.packs_orphan_young >= t0_packs.len() as u64,
        "{stats:?}"
    );
    for k in &t0_packs {
        assert!(sim.store().object(k).is_some(), "{k} deleted young");
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();

    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
    sim.compact(c).expect_ok();
    for k in &t0_packs {
        assert!(sim.store().object(k).is_none(), "{k} kept once old");
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// `compact --dry-run` applies the young-entries rule too: it counts the
/// entries it would keep as the real run then keeps them, and writes
/// nothing.
#[test]
fn compact_dry_run_counts_young_entries() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.delete(c, &["t0"]).expect_ok();
    let data_keys = |sim: &Sim| {
        let mut keys = Vec::new();
        for p in ["index.h/", "index.d/", "packs/", "snapshots/"] {
            keys.extend(sim.store().keys(p));
        }
        keys
    };
    let before = data_keys(&sim);
    let mut dry = None;
    sim.custom(
        c,
        "compact --dry-run",
        |repo: &mut crate::repo::RepoWriter<'_>| {
            repo.load_chunk_index_under_lock()?;
            dry = Some(crate::commands::compact::compact_repo(
                repo,
                10.0,
                None,
                true,
                1,
                &crate::repo::control::CancelToken::new(),
            )?);
            Ok(())
        },
    )
    .expect_ok();
    let dry = dry.expect("the dry run ran");
    assert!(dry.chunks_unreferenced_young > 0, "{dry:?}");
    assert_eq!(dry.chunks_unreferenced, 0, "{dry:?}");
    assert_eq!(data_keys(&sim), before, "a dry run writes nothing");
    let real = sim.compact(c);
    real.expect_ok();
    assert_eq!(
        compact_stat(real.detail(), "kept-young"),
        dry.chunks_unreferenced_young,
        "{}",
        real.detail()
    );
}

/// D14, documented: an identical re-PUT of an `index.d/` key keeps its
/// first mtime in the simulator's store (as on vykar-server), so an object
/// whose bytes repeat an older unnamed object reads as old as that object.
/// With random nonces no client writes such a segment. Part 9 owns this
/// (forward note 3).
#[test]
fn a_segment_that_repeats_an_old_object_reads_old() {
    use crate::index::head::ObjectName;
    use crate::repo::ages::{SegmentAges, T_PACK};
    use vykar_storage::StorageBackend;

    let mut sim = Sim::new();
    let c = sim.client("c");
    let store = sim.store().handle(c);
    let bytes = b"the same segment bytes".to_vec();
    let name = ObjectName::of(&bytes);
    let key = crate::repo::index_store::object_key(&name);
    store.put(&key, &bytes).unwrap();
    sim.advance(Duration::from_secs(13 * 3600));
    store.put(&key, &bytes).unwrap();
    store.put("locks/probe", b"now").unwrap();
    let listing = store.list_meta("locks/").unwrap();
    let now = vykar_storage::StoreNow::of_written(&listing, "locks/probe");
    assert!(now.is_some());
    let ages = SegmentAges::list(&store, now).unwrap();
    assert!(
        ages.is_old(&name, T_PACK),
        "the re-PUT kept the first mtime"
    );
}

/// D17: a forward step of the store's clock ages every object at once, a
/// live lock too: b's backup reaps a's lock after a 6 h 1 min step. The
/// route H2 and H4 take; not a hazard of its own.
#[test]
fn a_store_clock_step_ages_every_object() {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let ga = sim.gate(GateRule::new(
        a,
        OpMatch::AnyWrite,
        KeyMatch::Prefix("index.h/".into()),
    ));
    let sa = sim.spawn(a, Call::Compact);
    sim.expect_paused(ga, "a's first head PUT");
    let lock = sim
        .store()
        .keys_written_by(a, "locks/")
        .into_iter()
        .find(|k| k.ends_with(".json"))
        .expect("a holds a lock");
    sim.step_store_clock(chrono::Duration::seconds(6 * 3600 + 60));
    let rb = sim.backup(b, "b1", &tree("b1"));
    assert!(sim.store().object(&lock).is_none(), "{:?}", rb.outcome);
    sim.resume(ga);
    let _ = sim.join(sa);
}

/// A young malformed marker makes compact under the lock yield: the lock
/// holder's half of the marker handshake counts it as flagged (fail
/// closed), so compact is refused `Locked` and deletes no orphan pack, not
/// even one older than T_pack (a crashed backup's). Once 45 min old by its
/// mtime it is reaped and the next compact deletes the orphans. Invariant 9
/// no longer skips it. Lock mode: the default store offers no
/// put-if-absent.
#[test]
fn malformed_marker_ages_out() {
    let mut sim = Sim::new();
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let (_marker, packs) = crash_backup_after_upload(&mut sim, a, &tree("x1"));
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
    sim.tamper_put("sessions/x.json", b"{");
    let r = sim.compact(c);
    assert!(allowed_as(&r, AllowedError::Locked), "{}", r.detail());
    for k in &packs {
        assert!(
            sim.store().object(k).is_some(),
            "{k} deleted beside a young malformed marker"
        );
    }
    sim.advance(Duration::from_secs(46 * 60));
    sim.compact(c).expect_ok();
    assert!(sim.store().object("sessions/x.json").is_none());
    for k in &packs {
        assert!(
            sim.store().object(k).is_none(),
            "{k} kept once the marker aged out"
        );
    }
    let report = sim.check_invariants(Invariant::STEP7);
    assert!(
        !report.not_evaluated_for(Invariant::NoPermanentWedge, "sessions/x.json"),
        "{report}"
    );
    report.assert_clean();
}

// --- Readers without markers (04 step 7) ---

/// `x` holds files A and B, `y` only A (the same bytes): deleting `x`
/// leaves A's pack half dead, so a compact repacks it.
fn two_file_tree() -> SourceTree {
    SourceTree::new()
        .file(
            "a.bin",
            crate::sim::runner::seeded_bytes("file-a", 64 * KIB),
        )
        .file(
            "b.bin",
            crate::sim::runner::seeded_bytes("file-b", 64 * KIB),
        )
}

fn one_file_tree() -> SourceTree {
    SourceTree::new().file(
        "a.bin",
        crate::sim::runner::seeded_bytes("file-a", 64 * KIB),
    )
}

/// Gate on `c`'s first range read of a pack.
fn first_pack_read(c: ClientId) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::GetRange),
        KeyMatch::Prefix("packs/".into()),
    )
}

/// The ops of run `index`.
fn run_ops(sim: &Sim, index: usize) -> Vec<crate::sim::OpRecord> {
    sim.store()
        .log()
        .into_iter()
        .filter(|r| r.run == Some(index))
        .collect()
}

/// Packs deleted by client `c` with an applied delete.
fn packs_deleted_by(sim: &Sim, c: ClientId) -> BTreeSet<String> {
    sim.store()
        .log()
        .into_iter()
        .filter(|r| r.client == c && r.key.starts_with("packs/"))
        .filter(|r| matches!(r.op, OpKind::Delete | OpKind::BatchDelete))
        .filter(|r| matches!(r.outcome, OpOutcome::Ok))
        .map(|r| r.key)
        .collect()
}

/// "Compact runs during a restore" (04 step 7): a restore of `y` held at
/// its first pack read; meanwhile `x` is deleted and compacted, which
/// repacks the pack holding `y`'s data (no `ActiveSessions`: the restore
/// registered no marker). The restore reads the moved chunks where the
/// latest head stores them and restores `y` exactly.
#[test]
fn restore_during_compact_reads_relocated_chunks() {
    let mut sim = Sim::new();
    let (c, r, b) = (sim.client("c"), sim.client("r"), sim.client("b"));
    sim.backup(c, "x", &two_file_tree()).expect_ok();
    sim.backup(c, "y", &one_file_tree()).expect_ok();
    // x's segment is old: the compact sweeps it and repacks (04 step 7).
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
    let planned: BTreeSet<String> = sim.store().keys("packs/").into_iter().collect();

    let gate = sim.gate(first_pack_read(r));
    let spawned = sim.spawn(
        r,
        Call::Restore {
            snapshot: "y".into(),
        },
    );
    let held = sim.expect_paused(gate, "restore reads a pack");
    sim.delete(b, &["x"]).expect_ok();
    let compact = sim.compact(b);
    compact.expect_ok();
    let deleted = packs_deleted_by(&sim, b);
    assert!(
        deleted.iter().any(|p| planned.contains(p)),
        "compact deleted none of the packs the restore planned from: {}",
        compact.detail()
    );
    sim.resume(gate);
    let run = sim.join(spawned);
    run.expect_ok();
    let restored = SourceTree::read_from(&sim.restore_dir(&run)).unwrap();
    assert_eq!(restored, one_file_tree());
    let ops = run_ops(&sim, run.index);
    assert!(
        ops.iter()
            .any(|o| o.step > held.at_step && o.op == OpKind::List && o.key == "index.h/"),
        "the restore did not read the head again (no relocation)"
    );
    let protocol: Vec<_> = ops
        .into_iter()
        .filter(|o| o.key.starts_with("sessions/") || o.key.starts_with("locks/"))
        .collect();
    assert!(protocol.is_empty(), "{protocol:?}");
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// A restore of `x` held at its first pack read while `x` is deleted and
/// compacted ends with `ChunkRemovedDuringRead`, an allowed error.
#[test]
fn restore_of_a_snapshot_deleted_during_the_read_is_allowed() {
    let mut sim = Sim::new();
    let (c, r, b) = (sim.client("c"), sim.client("r"), sim.client("b"));
    sim.backup(c, "x", &two_file_tree()).expect_ok();
    sim.backup(c, "y", &one_file_tree()).expect_ok();
    // x's segment is old: the compact sweeps it (04 step 7).
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));

    let gate = sim.gate(first_pack_read(r));
    let spawned = sim.spawn(
        r,
        Call::Restore {
            snapshot: "x".into(),
        },
    );
    sim.expect_paused(gate, "restore reads a pack");
    sim.delete(b, &["x"]).expect_ok();
    sim.compact(b).expect_ok();
    sim.resume(gate);
    let run = sim.join(spawned);
    let RunOutcome::Err(e) = &run.outcome else {
        panic!("the restore of a removed snapshot succeeded: {run:?}");
    };
    assert_eq!(
        e.class,
        ErrorClass::Allowed(AllowedError::RemovedDuringRead),
        "{}",
        e.display
    );
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// A check held at its first pack op while `x` is deleted and compacted
/// (packs its index names go) finds them missing, sees the head moved, and
/// checks again on the latest head: 0 errors.
#[test]
fn check_during_compact_reports_no_damage() {
    let mut sim = Sim::new();
    let (c, r, b) = (sim.client("c"), sim.client("r"), sim.client("b"));
    sim.backup(c, "x", &two_file_tree()).expect_ok();
    sim.backup(c, "y", &one_file_tree()).expect_ok();
    // x's segment is old: the compact sweeps it (04 step 7).
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));

    let gate = sim.gate(GateRule::new(
        r,
        OpMatch::Any,
        KeyMatch::Prefix("packs/".into()),
    ));
    let spawned = sim.spawn(r, Call::Check);
    let held = sim.expect_paused(gate, "check reads packs");
    sim.delete(b, &["x"]).expect_ok();
    sim.compact(b).expect_ok();
    assert!(!packs_deleted_by(&sim, b).is_empty());
    sim.resume(gate);
    let run = sim.join(spawned);
    run.expect_ok();
    assert!(run.detail().starts_with("0 errors"), "{}", run.detail());
    let head_lists_after = run_ops(&sim, run.index)
        .iter()
        .filter(|o| o.step > held.at_step && o.op == OpKind::List && o.key == "index.h/")
        .count();
    assert!(head_lists_after >= 2, "no second pass: {head_lists_after}");
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// A pack lost under an unmoved head is reported, once, and the check does
/// not run again. A lost data pack costs one extra head read (LIST and
/// GET), the check's own after its scan; a lost tree pack costs that plus
/// one for the relocation that finds it lost, however many reads of the
/// pack fail afterwards.
#[test]
fn check_reports_a_lost_pack_once() {
    lost_pack_head_reads(true, 1);
    lost_pack_head_reads(false, 2);
}

/// Deletes the data pack (the larger one) or the tree pack of a two-file
/// backup and asserts how many more head reads the check makes than a
/// clean one.
fn lost_pack_head_reads(data_pack: bool, extra: usize) {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "x", &two_file_tree()).expect_ok();
    let head_reads = |sim: &Sim, index: usize| {
        let ops = run_ops(sim, index);
        let n = |op| {
            ops.iter()
                .filter(|o| o.op == op && o.key.starts_with("index.h/"))
                .count()
        };
        (n(OpKind::List), n(OpKind::Get))
    };
    let clean = sim.check(c);
    assert!(clean.detail().starts_with("0 errors"), "{}", clean.detail());
    let (lists, gets) = head_reads(&sim, clean.index);

    // Upload order varies; the data pack (128 KiB) is the larger one.
    let mut packs = sim.store().keys_written_by(c, "packs/");
    assert_eq!(packs.len(), 2, "{packs:?}");
    packs.sort_by_key(|k| sim.store().object(k).map_or(0, |o| o.data.len()));
    let pack = packs[usize::from(data_pack)].clone();
    assert!(sim.tamper_delete(&pack));
    let lost = sim.check(c);
    assert!(!lost.detail().starts_with("0 errors"), "{}", lost.detail());
    assert_eq!(
        head_reads(&sim, lost.index),
        (lists + extra, gets + extra),
        "data_pack {data_pack}"
    );
}

/// A2: a snapshot listed when the check opened and deleted (no compact)
/// before the check loads its metadata is skipped, not reported corrupt:
/// prune and delete write no head, so no second pass would excuse it.
#[test]
fn check_skips_a_snapshot_deleted_during_the_check() {
    let mut sim = Sim::new();
    let (c, r, b) = (sim.client("c"), sim.client("r"), sim.client("b"));
    sim.backup(c, "x", &two_file_tree()).expect_ok();
    sim.backup(c, "y", &one_file_tree()).expect_ok();

    // The open lists the head once; the index load lists it again, before
    // the snapshot metadata is read.
    let gate = sim.gate(
        GateRule::new(
            r,
            OpMatch::Kind(OpKind::List),
            KeyMatch::Prefix("index.h/".into()),
        )
        .nth(2),
    );
    let spawned = sim.spawn(r, Call::Check);
    sim.expect_paused(gate, "check loads the index");
    sim.delete(b, &["x"]).expect_ok();
    sim.resume(gate);
    let run = sim.join(spawned);
    run.expect_ok();
    assert!(run.detail().starts_with("0 errors"), "{}", run.detail());
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

// ── Maintenance beside backups (04 step 7) ──────────────────────────────

/// a's session marker key (generation 0).
fn marker_of(sim: &Sim, a: ClientId) -> String {
    sim.store()
        .keys_written_by(a, "sessions/")
        .into_iter()
        .find(|k| k.ends_with(".json"))
        .expect("a's marker")
}

/// Spawn a's backup of `a1` and hold it at its commit lock's stale-lock
/// sweep, after every pack is uploaded.
fn hold_after_upload(
    sim: &mut Sim,
    a: ClientId,
) -> (crate::sim::GateId, crate::sim::runner::Spawned) {
    let ga = sim.gate(GateRule::new(a, OpMatch::AnyList, KeyMatch::Exact("locks/".into())).nth(2));
    let sa = sim.spawn(a, backup_call("a1"));
    sim.expect_paused(ga, "a's commit-lock sweep");
    assert!(!sim.store().keys_written_by(a, "packs/").is_empty());
    (ga, sa)
}

/// 04 step 7's done criterion: compact runs during a backup. b deletes c0
/// (a's tree shares nothing with it) and compacts while a is between its
/// uploads and its commit: compact sweeps c0's chunks, keeps a's packs and
/// leaves a's marker; a commits and restores.
#[test]
fn compact_runs_during_a_backup() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(b, "c0", &tree("c0")).expect_ok();
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    // c0's segment is old, so the sweep takes its chunks (04 step 7).
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
    let (ga, sa) = hold_after_upload(&mut sim, a);
    let a_packs = sim.store().keys_written_by(a, "packs/");
    let marker = marker_of(&sim, a);
    sim.delete(b, &["c0"]).expect_ok();
    let rb = sim.compact(b);
    rb.expect_ok();
    assert!(
        compact_stat(rb.detail(), "unreferenced") > 0,
        "{}",
        rb.detail()
    );
    for k in &a_packs {
        assert!(
            sim.store().object(k).is_some(),
            "{k} deleted under a live backup"
        );
    }
    assert!(sim.store().object(&marker).is_some());
    sim.resume(ga);
    sim.join(sa).expect_ok();
    sim.restore(b, "a1").expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// A live marker's horizon keeps orphan packs older than T_pack: a
/// session that says it has run for 13 h covers a crashed backup's 12 h
/// old packs until the marker goes.
#[test]
fn the_horizon_keeps_uploads_older_than_t_pack() {
    let mut sim = Sim::new();
    let (x, b) = (sim.client("x"), sim.client("b"));
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    let (_marker, packs) = crash_backup_after_upload(&mut sim, x, &tree("x1"));
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
    let now = sim.time().now_utc().to_rfc3339();
    let body = serde_json::json!({
        "hostname": "elsewhere",
        "pid": 4242,
        "registered_at": now,
        "last_refresh": now,
        "elapsed_secs": 13 * 3600,
    });
    let live = "sessions/longrunning.json";
    sim.tamper_put(live, &serde_json::to_vec(&body).unwrap());
    let r = sim.compact(b);
    r.expect_ok();
    assert!(
        compact_stat(r.detail(), "live") >= packs.len() as u64,
        "{}",
        r.detail()
    );
    for k in &packs {
        assert!(
            sim.store().object(k).is_some(),
            "{k} deleted inside a live horizon"
        );
    }
    assert!(sim.tamper_delete(live));
    sim.compact(b).expect_ok();
    for k in &packs {
        assert!(
            sim.store().object(k).is_none(),
            "{k} kept with no horizon over it"
        );
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// B8: the horizon end to end. a is held after its uploads while its
/// heartbeat refreshes the marker every 15 min for more than 12 h; b's
/// compact then finds a's packs older than T_pack and keeps them by a's
/// horizon alone; a commits with a sound record and restores.
#[test]
fn a_refreshed_marker_keeps_uploads_older_than_t_pack() {
    const WAIT: Duration = Duration::from_secs(5);
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    let (ga, sa) = hold_after_upload(&mut sim, a);
    let a_packs = sim.store().keys_written_by(a, "packs/");
    let polls = (crate::repo::ages::T_PACK.as_secs() + 30 * 60) / 30;
    assert!(sim.time().wait_for_waiters(1, WAIT));
    for _ in 0..polls {
        sim.advance(Duration::from_secs(30));
        assert!(
            sim.time().wait_for_waiters(1, WAIT),
            "the heartbeat waits again"
        );
    }
    let refreshes = sim
        .store()
        .keys_written_by(a, "sessions/")
        .iter()
        .filter(|k| k.ends_with(".json"))
        .count();
    assert!(refreshes > 48, "{refreshes} marker keys");
    let rb = sim.compact(b);
    rb.expect_ok();
    assert!(
        compact_stat(rb.detail(), "live") >= a_packs.len() as u64,
        "{}",
        rb.detail()
    );
    for k in &a_packs {
        assert!(
            sim.store().object(k).is_some(),
            "{k} deleted inside a's horizon"
        );
    }
    sim.resume(ga);
    sim.join(sa).expect_ok();
    sim.restore(b, "a1").expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// LateHeadThenCompact with a sound marker record: a is held after its
/// uploads for more than T_pack while its heartbeat refreshes the marker,
/// so I8 lets it commit; its head PUT is delayed and the commit fails
/// (its session, and with it its horizon, ends). b's compact deletes a's
/// packs as orphans older than T_pack and keeps a's segment (written after
/// the hold, younger than the 12 h `INDEX_OBJECT_GRACE`); the late head then lands and names that segment, so the index
/// names absent packs. The driver's verdict on this store claims it as
/// exactly `LateHeadThenCompact` (fixed by 04 step 8).
#[test]
fn a_late_head_of_a_refreshed_backup_names_packs_compact_deleted() {
    use crate::sim::driver::{Signature, classify as classify_case};
    const WAIT: Duration = Duration::from_secs(5);
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    sim.fault(
        FaultRule::new(
            OpMatch::AnyWrite,
            KeyMatch::Prefix("index.h/".into()),
            FaultAction::Delay,
        )
        .client(a),
    );
    let (ga, sa) = hold_after_upload(&mut sim, a);
    let a_packs = sim.store().keys_written_by(a, "packs/");
    let polls = (crate::repo::ages::T_PACK.as_secs() + 30 * 60) / 30;
    assert!(sim.time().wait_for_waiters(1, WAIT));
    for _ in 0..polls {
        sim.advance(Duration::from_secs(30));
        assert!(
            sim.time().wait_for_waiters(1, WAIT),
            "the heartbeat waits again"
        );
    }
    let refreshes = sim
        .store()
        .keys_written_by(a, "sessions/")
        .iter()
        .filter(|k| k.ends_with(".json"))
        .count();
    assert!(refreshes > 48, "{refreshes} marker keys");
    sim.resume(ga);
    let ra = sim.join(sa);
    assert!(allowed_as(&ra, AllowedError::Injected), "{:?}", ra.outcome);
    assert!(
        !sim.store().keys_written_by(a, "index.d/").is_empty(),
        "a's segment"
    );
    sim.compact(b).expect_ok();
    for k in &a_packs {
        assert!(sim.store().object(k).is_none(), "{k} kept");
    }
    assert_eq!(sim.release_all().len(), 1, "a's head PUT");
    let report = sim.check_invariants(&[
        Invariant::CommittedSnapshotsRestore,
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::IndexIntegrity,
        Invariant::AllowedErrors,
        Invariant::LockExclusion,
        Invariant::OrphanGrace,
        Invariant::MarkerGapCommit,
    ]);
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.kind == ViolationKind::IndexNamesAbsentPack),
        "{report:?}"
    );
    assert_eq!(
        classify_case(&sim, &report, &[]),
        BTreeSet::from([Signature::LateHeadThenCompact]),
        "{report:?}"
    );
}

/// AgedLockPacksCollected (no seed reaches it since phase F part 8c): a
/// is held after its uploads while 6 h 1 min pass (one heartbeat poll, so
/// its marker goes stale), takes its commit lock and is held again at its
/// head PUT, after the fence; another 6 h 1 min age its lock past the
/// stale age and its packs past T_pack. b's compact reaps the lock and the
/// marker and deletes a's packs as orphans; its index GC keeps a's segment
/// (written after the first jump, about 6 h old: younger than the 12 h
/// `INDEX_OBJECT_GRACE`), so a's head, published outside any lock, names a
/// present segment whose entries name absent packs. The driver's verdict
/// claims it as exactly `AgedLockReaped` (the head PUT) and
/// `AgedLockPacksCollected` (fixed by 04 step 8 on conditional stores).
#[test]
fn an_aged_lock_head_names_packs_compact_deleted() {
    use crate::sim::driver::{Action, Signature, TraceEntry, classify as classify_case};
    const WAIT: Duration = Duration::from_secs(5);
    const JUMP: Duration = Duration::from_secs(6 * 3600 + 60);
    // The classifier ages a reaped lock on the trace clock: one entry per
    // stretch of store steps between the jumps, with the store clock then.
    let mut entries: Vec<TraceEntry> = Vec::new();
    let mut span = |sim: &Sim, label: &str| {
        let first = entries.last().map_or(0, |e| e.steps.1);
        let last = sim.store().log().last().map_or(0, |r| r.step);
        entries.push(TraceEntry {
            n: entries.len(),
            choice: 0,
            options: 1,
            action: Action::Stop,
            label: label.into(),
            ops: Vec::new(),
            steps: (first, last),
            actions: Vec::new(),
            ended: false,
            clock: sim.store().now(),
        });
    };
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    let (ga, sa) = hold_after_upload(&mut sim, a);
    let a_packs = sim.store().keys_written_by(a, "packs/");
    span(&sim, "upload");
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.advance(JUMP);
    assert!(sim.time().wait_for_waiters(1, WAIT));
    let gh = sim.gate(GateRule::new(
        a,
        OpMatch::AnyWrite,
        KeyMatch::Prefix("index.h/".into()),
    ));
    sim.resume(ga);
    sim.expect_paused(gh, "a's head PUT");
    let a_segments = sim.store().keys_written_by(a, "index.d/");
    assert!(!a_segments.is_empty(), "a's segment");
    span(&sim, "commit to the fence");
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.advance(JUMP);
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.compact(b).expect_ok();
    span(&sim, "compact");
    for k in &a_packs {
        assert!(sim.store().object(k).is_none(), "{k} kept");
    }
    for k in &a_segments {
        assert!(sim.store().object(k).is_some(), "{k} collected");
    }
    sim.resume(gh);
    let _ = sim.join(sa);
    span(&sim, "head PUT");
    let report = sim.check_invariants(&[
        Invariant::CommittedSnapshotsRestore,
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::IndexIntegrity,
        Invariant::AllowedErrors,
        Invariant::LockExclusion,
        Invariant::OrphanGrace,
        Invariant::MarkerGapCommit,
    ]);
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.kind == ViolationKind::IndexNamesAbsentPack),
        "{report:?}"
    );
    assert_eq!(
        classify_case(&sim, &report, &entries),
        BTreeSet::from([Signature::AgedLockReaped, Signature::AgedLockPacksCollected]),
        "{report:?}"
    );
}

/// I8 at the judgement: a's marker went unrefreshed for 11 h 30 min (one
/// poll: a gap, no reap), so its uploads may be old enough for compact;
/// the commit refuses before writing anything.
#[test]
fn a_broken_marker_record_with_old_packs_refuses_at_the_judgement() {
    const WAIT: Duration = Duration::from_secs(5);
    let mut sim = Sim::new();
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let (ga, sa) = hold_after_upload(&mut sim, a);
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.advance(Duration::from_secs(11 * 3600 + 30 * 60));
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.resume(ga);
    let ra = sim.join(sa);
    assert!(
        allowed_as(&ra, AllowedError::StaleChunksDuringCommit),
        "{:?}",
        ra.outcome
    );
    for prefix in ["index.d/", "index.h/", "snapshots/"] {
        assert!(
            sim.store().keys_written_by(a, prefix).is_empty(),
            "a wrote {prefix}"
        );
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// A reaped marker with young uploads: b's compact 46 min later reaps a's
/// marker as stale and keeps its packs by age; a's commit finds the marker
/// gone and commits (I8: its packs are young); a1 restores.
#[test]
fn a_broken_marker_record_with_young_packs_commits() {
    const WAIT: Duration = Duration::from_secs(5);
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(b, "t0", &tree("t0")).expect_ok();
    let (ga, sa) = hold_after_upload(&mut sim, a);
    let marker = marker_of(&sim, a);
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.advance(Duration::from_secs(46 * 60));
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.compact(b).expect_ok();
    assert!(sim.store().object(&marker).is_none(), "a's marker reaped");
    sim.resume(ga);
    sim.join(sa).expect_ok();
    sim.restore(b, "a1").expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// I8 at the fence before the head PUT: a's judgement passed at 11 h (a
/// gap, packs young enough), then 30 min pass while a is held at its
/// segment PUT; the fence refuses, and a writes no head.
#[test]
fn the_fence_before_the_head_put_applies_the_pack_age_term() {
    const WAIT: Duration = Duration::from_secs(5);
    let mut sim = Sim::new();
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let (ga, sa) = hold_after_upload(&mut sim, a);
    let gs = sim.gate(GateRule::new(
        a,
        OpMatch::AnyWrite,
        KeyMatch::Prefix("index.d/".into()),
    ));
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.advance(Duration::from_secs(11 * 3600));
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.resume(ga);
    sim.expect_paused(gs, "a's segment PUT");
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.advance(Duration::from_secs(30 * 60));
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.resume(gs);
    let ra = sim.join(sa);
    assert!(
        allowed_as(&ra, AllowedError::StaleChunksDuringCommit),
        "{:?}",
        ra.outcome
    );
    assert!(
        sim.store().keys_written_by(a, "index.h/").is_empty(),
        "a wrote a head"
    );
    assert!(sim.store().keys_written_by(a, "snapshots/").is_empty());
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// B2, the binding check: a's fence before the head PUT passed at 11 h 10
/// min; its head PUT lands 45 min later, when the packs may be T_pack − M
/// old; the check after it refuses the snapshot. The head stays without
/// its snapshot.
#[test]
fn a_head_that_lands_late_writes_no_snapshot() {
    const WAIT: Duration = Duration::from_secs(5);
    let mut sim = Sim::new();
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let (ga, sa) = hold_after_upload(&mut sim, a);
    let gh = sim.gate(GateRule::new(
        a,
        OpMatch::AnyWrite,
        KeyMatch::Prefix("index.h/".into()),
    ));
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.advance(Duration::from_secs(11 * 3600 + 10 * 60));
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.resume(ga);
    sim.expect_paused(gh, "a's head PUT");
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.advance(Duration::from_secs(45 * 60));
    assert!(sim.time().wait_for_waiters(1, WAIT));
    sim.resume(gh);
    let ra = sim.join(sa);
    assert!(
        allowed_as(&ra, AllowedError::StaleChunksDuringCommit),
        "{:?}",
        ra.outcome
    );
    assert!(
        !sim.store().keys_written_by(a, "index.h/").is_empty(),
        "a wrote its head"
    );
    assert!(
        sim.store().keys_written_by(a, "snapshots/").is_empty(),
        "no snapshot"
    );
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// `x`'s backup fails at its segment PUT: its session ends, its journal
/// stays (`flush_on_abort`) and its packs are orphans. Returns them.
fn fail_backup_leaving_journal(sim: &mut Sim, x: ClientId, tree: &SourceTree) -> Vec<String> {
    sim.fault(
        FaultRule::new(
            OpMatch::AnyWrite,
            KeyMatch::Prefix("index.d/".into()),
            FaultAction::Fail,
        )
        .client(x),
    );
    let r = sim.backup(x, "x1", tree);
    assert!(allowed_as(&r, AllowedError::Injected), "{}", r.detail());
    assert!(
        sim.store()
            .keys("sessions/")
            .iter()
            .any(|k| k.ends_with(".index")),
        "x's journal"
    );
    let packs = sim.store().keys_written_by(x, "packs/");
    assert!(!packs.is_empty());
    packs
}

/// Storage keys of the packs the current index names, read through the
/// observer.
fn indexed_pack_keys(sim: &Sim) -> std::collections::BTreeSet<String> {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = crate::sim::checker::open_observer(sim.store(), tmp.path()).unwrap();
    repo.load_chunk_index_uncached().unwrap();
    repo.chunk_index()
        .iter()
        .map(|(_, e)| e.pack_id.storage_key())
        .collect()
}

/// The journal rule: a journal whose packs are older than T_pack/2 is not
/// recovered; the next backup of the same tree uploads its chunks again,
/// and the old packs stay as orphans for compact.
#[test]
fn a_journal_older_than_half_t_pack_is_not_recovered() {
    let mut sim = Sim::new();
    let (x, b, c) = (sim.client("x"), sim.client("b"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let x_packs = fail_backup_leaving_journal(&mut sim, x, &tree("x1"));
    sim.advance(Duration::from_secs(6 * 3600 + 60));
    sim.backup(b, "b1", &tree("x1")).expect_ok();
    let b_packs = sim.store().keys_written_by(b, "packs/");
    let indexed = indexed_pack_keys(&sim);
    assert!(
        x_packs.iter().all(|k| !indexed.contains(k)),
        "b recovered x's journal: the index names x's packs: x {x_packs:?}, indexed {indexed:?}"
    );
    assert!(
        !b_packs.is_empty() && b_packs.iter().all(|k| indexed.contains(k)),
        "b uploaded x's chunks itself: b {b_packs:?}, indexed {indexed:?}"
    );
    for k in &x_packs {
        assert!(sim.store().object(k).is_some(), "{k} is left as an orphan");
    }
    sim.restore(c, "b1").expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// A recovered journal pulls the session's start back: b recovers x's 46
/// min old packs, and its first refresh writes an `elapsed_secs` of at
/// least the refresh interval plus their age.
#[test]
fn a_recovered_journal_pulls_the_marker_back() {
    const WAIT: Duration = Duration::from_secs(5);
    let mut sim = Sim::new();
    let (x, b, c) = (sim.client("x"), sim.client("b"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let x_packs = fail_backup_leaving_journal(&mut sim, x, &tree("x1"));
    sim.advance(Duration::from_secs(46 * 60));
    let gb = sim.gate(GateRule::new(b, OpMatch::AnyList, KeyMatch::Exact("locks/".into())).nth(2));
    let sb = sim.spawn(
        b,
        Call::Backup {
            snapshot: "b1".into(),
            tree: tree("x1"),
        },
    );
    sim.expect_paused(gb, "b's commit-lock sweep");
    let b_packs = sim.store().keys_written_by(b, "packs/");
    assert!(
        b_packs.iter().all(|k| !x_packs.contains(k)),
        "b recovered x's packs: x {x_packs:?}, b {b_packs:?}"
    );
    assert!(sim.time().wait_for_waiters(1, WAIT));
    for _ in 0..30 {
        sim.advance(Duration::from_secs(30));
        assert!(sim.time().wait_for_waiters(1, WAIT));
    }
    let gen1 = sim
        .store()
        .keys_written_by(b, "sessions/")
        .into_iter()
        .find(|k| k.ends_with(".1.json"))
        .expect("b's first refresh");
    let entry: crate::repo::lock::SessionEntry =
        serde_json::from_slice(&sim.store().object(&gen1).unwrap().data).unwrap();
    let floor = (15 + 46) * 60;
    assert!(
        entry.elapsed_secs >= Some(floor),
        "{:?}",
        entry.elapsed_secs
    );
    sim.resume(gb);
    sim.join(sb).expect_ok();
    for k in &x_packs {
        assert!(sim.store().object(k).is_some());
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
}
