//! Lock-free backups (04 step 9, phase F part 10c) on the simulator: two
//! backups commit without a lock; a backup waits for a live exclusive
//! session at its pre-head listing and an exclusive session refuses beside a
//! committing backup; a crashed committing marker blocks exclusive sessions
//! until it is stale. Exclusive sessions list `sessions/` only after their
//! marker PUT; a marker
//! reaped by age beside its refresh, an undecided put-if-absent check and a
//! cancel during the wait each fail the backup as they should. A backup
//! whose marker record has a gap before its commit commits lock-free after
//! the suspend rule.
//!
//! Since 10-single-protocol step 7 no backup or compact takes the lock, and
//! since step 8 no command does: the holders here are exclusive sessions
//! (`Call::ExclusiveCustom`, `Call::ExclusiveDelete`), as `check --repair`,
//! upgrade and pack maps run. `a_locked_compact_fails_locked_beside_a_committing_backup`
//! moved to `tests::single_writer` (a single-writer compact), and
//! `a_backup_with_a_gap_yields_to_a_committing_backup` became
//! `tests::single_writer::a_gap_backup_commits_beside_a_committing_backup`.

use crate::repo::control::CancelToken;
use crate::sim::history::{AllowedError, ErrorClass};
use crate::sim::{
    Call, ClientId, FaultAction, FaultRule, GateId, GateRule, Invariant, KeyMatch, OpKind, OpMatch,
    OpRecord, Sim, SimConfig, SourceTree, Spawned, StoreKind, TimeMode,
};

const FILE_SIZE: usize = 16 * 1024;

fn tree(name: &str) -> SourceTree {
    SourceTree::seeded(name, 2, FILE_SIZE)
}

fn backup_call(name: &str) -> Call {
    Call::Backup {
        snapshot: name.into(),
        tree: tree(name),
    }
}

/// An encrypted repository (the lock-free path exists only there) on a
/// store of `kind`.
fn encrypted(kind: StoreKind) -> Sim {
    Sim::with_config(SimConfig {
        kind,
        ..SimConfig::default()
    })
}

/// The records of `client`'s run `run`.
fn records(sim: &Sim, client: ClientId, run: usize) -> Vec<OpRecord> {
    sim.store()
        .log()
        .into_iter()
        .filter(|r| r.client == client && r.run == Some(run))
        .collect()
}

/// Writes of lock keys (`locks/*.json`, never a probe).
fn lock_writes(records: &[OpRecord]) -> Vec<&OpRecord> {
    records
        .iter()
        .filter(|r| {
            r.op.is_write() && r.key.starts_with("locks/") && KeyMatch::LockKey.matches(&r.key)
        })
        .collect()
}

fn is_protected(key: &str) -> bool {
    ["index.h/", "index.d/", "snapshots/"]
        .iter()
        .any(|p| key.starts_with(p))
}

/// `c`'s first PUT of a snapshot: a lock-free commit is between its head
/// create and its commit point there, its marker flagged.
fn snapshot_put(c: ClientId) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::Put),
        KeyMatch::Prefix("snapshots/".into()),
    )
}

/// `c`'s `n`th listing of `sessions/`.
fn sessions_listing(c: ClientId, n: u32) -> GateRule {
    GateRule::new(c, OpMatch::AnyList, KeyMatch::Exact("sessions/".into())).nth(n)
}

/// An exclusive session that writes a head: a fold
/// ([`Call::ExclusiveCustom`]), as `check --repair`, upgrade and pack maps
/// run since 10-single-protocol step 8 (a fold under the maintenance lock
/// before).
fn exclusive_fold() -> Call {
    Call::ExclusiveCustom {
        label: "exclusive fold".into(),
        f: Box::new(|repo| {
            repo.load_chunk_index_under_lock()?;
            repo.request_index_fold();
            repo.mark_index_dirty();
            repo.save_state()
        }),
    }
}

/// An exclusive session that deletes snapshot `name` ([`Call::ExclusiveDelete`]).
fn exclusive_delete(name: &str) -> Call {
    Call::ExclusiveDelete {
        names: vec![name.into()],
    }
}

/// `c`'s snapshot delete: an [`exclusive_delete`] holds its session there.
fn snapshot_delete(c: ClientId) -> GateRule {
    GateRule::new(c, OpMatch::AnyDelete, KeyMatch::Prefix("snapshots/".into()))
}

/// `c`'s commit mark: its first marker key after registration.
fn commit_mark(c: ClientId) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::Put),
        KeyMatch::Prefix("sessions/".into()),
    )
    .filter(|_, k| k.ends_with(".1.json"))
}

/// Whether `key` is an exclusive session's marker key.
fn is_exclusive_key(key: &str) -> bool {
    key.starts_with("sessions/x-") && key.ends_with(".json")
}

/// The first exclusive marker key `c` wrote in `log` and the step its run
/// deleted it.
fn first_exclusive(log: &[OpRecord], c: ClientId) -> (OpRecord, u64) {
    let put = log
        .iter()
        .find(|r| r.client == c && r.op == OpKind::Put && is_exclusive_key(&r.key))
        .expect("an exclusive marker written")
        .clone();
    let released = log
        .iter()
        .find(|r| r.client == c && r.step > put.step && r.op.is_delete() && r.key == put.key)
        .expect("the marker deleted")
        .step;
    (put, released)
}

/// Delete every marker key of `c` (tamper): a reaper that misjudged its
/// session (I3), so an exclusive session can register beside it.
fn reap_markers_of(sim: &mut Sim, c: ClientId) {
    for key in sim.store().keys_written_by(c, "sessions/") {
        if key.ends_with(".json") && sim.store().object(&key).is_some() {
            sim.tamper_delete(&key);
        }
    }
}

fn expect_active_sessions(run: &crate::sim::RunRecord) {
    assert_eq!(
        run.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::ActiveSessions)),
        "{}",
        run.detail()
    );
}

fn expect_locked(run: &crate::sim::RunRecord) {
    assert_eq!(
        run.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Locked)),
        "{}",
        run.detail()
    );
}

/// Two lock-free backups whose head creates meet: neither writes a lock
/// key, the one whose generation was taken rebases and commits, and both
/// snapshots restore.
#[test]
fn two_lock_free_backups_commit_without_a_lock() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    let gate = sim.gate(GateRule::new(
        a,
        OpMatch::Kind(OpKind::PutIfAbsent),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let ra = sim.spawn(a, backup_call("a1"));
    sim.expect_paused(gate, "a's head create");
    let rb = sim.call(b, backup_call("b1"));
    rb.expect_ok();
    sim.resume(gate);
    let ra = sim.join(ra);
    ra.expect_ok();
    assert!(ra.lock_free && rb.lock_free, "both commit lock-free");
    let (ra_recs, rb_recs) = (records(&sim, a, ra.index), records(&sim, b, rb.index));
    for recs in [&ra_recs, &rb_recs] {
        assert!(lock_writes(recs).is_empty(), "{:?}", lock_writes(recs));
    }
    let taken = ra_recs
        .iter()
        .filter(|r| r.op == OpKind::PutIfAbsent && r.key.starts_with("index.h/"))
        .filter(|r| r.outcome == crate::sim::OpOutcome::Exists)
        .count();
    assert_eq!(taken, 1, "a's first create was taken by b's head");
    for name in ["a1", "b1"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// The pre-head listing (10-single-protocol step 8; the handshake's
/// session side before): a backup whose marker was reaped (a misjudging
/// reaper, I3) while it was at its commit mark lets an exclusive session
/// register; at its pre-head listing it finds that session live
/// (`Locked`), waits with its mark kept and creates no head while the
/// exclusive marker exists; once the session ended it goes on (the suspend
/// rule: its record shows the miss) and ends Ok or with an allowed error.
/// The exclusive session deletes (a delete under the maintenance lock
/// before step 8, a lock-mode backup before step 7).
#[test]
fn a_commit_waits_for_a_live_exclusive_session() {
    // The retry of the pre-head listing: multi-writer, the listings are the
    // registration's, the open's, the pre-head one (Locked) and its retry;
    // single-writer, two more (before and after the mark), and the retry
    // lists after the mark first.
    for (kind, retry) in [(StoreKind::Conditional, 4), (StoreKind::Plain, 6)] {
        let mut sim = encrypted(kind);
        let (f, l) = (sim.client("f"), sim.client("l"));
        sim.backup(l, "l0", &tree("l0")).expect_ok();
        let f_retry = sim.gate(sessions_listing(f, retry));
        let f_mark = sim.gate(commit_mark(f));
        let rf = sim.spawn(f, backup_call("f1"));
        sim.expect_paused(f_mark, "f's commit mark");
        reap_markers_of(&mut sim, f);
        let l_held = sim.gate(snapshot_delete(l));
        let rl = sim.spawn(l, exclusive_delete("l0"));
        sim.expect_paused(l_held, "l's snapshot delete in its exclusive session");
        sim.resume(f_mark);
        sim.expect_paused(f_retry, "f's retry of its pre-head listing");
        let log = sim.store().log();
        assert!(
            !log.iter()
                .any(|r| r.client == f && r.op.is_write() && r.key.starts_with("index.h/")),
            "{kind:?}: f created a head while l's exclusive session was live"
        );
        sim.resume(l_held);
        let rl = sim.join(rl);
        rl.expect_ok();
        assert!(!rl.lock_free && rl.exclusive);
        sim.resume(f_retry);
        let rf = sim.join(rf);
        assert!(
            rf.is_ok() || matches!(rf.error_class(), Some(ErrorClass::Allowed(_))),
            "{kind:?}: {}",
            rf.detail()
        );
        let set = if kind.records_multi_writer() {
            Invariant::STEP8
        } else {
            Invariant::STEP7
        };
        sim.check_invariants(set).assert_clean();
    }
}

/// An exclusive session (a head writer, [`exclusive_fold`]) started while a
/// backup is between its head create and its snapshot PUT refuses
/// (`ActiveSessions`) and writes nothing; run again after the backup ended
/// it writes its head. No head is replaced on a store whose plain PUT
/// overwrites (invariant 12 clean on the multi-writer one). Until
/// 10-single-protocol step 8 the holder took the lock and yielded with
/// `Locked` (a lock-mode backup that waited before step 7).
#[test]
fn an_exclusive_session_refuses_beside_a_committing_backup() {
    for kind in [StoreKind::ConditionalOverwrite, StoreKind::Plain] {
        let mut sim = encrypted(kind);
        let (f, l) = (sim.client("f"), sim.client("l"));
        let f_snap = sim.gate(snapshot_put(f));
        let rf = sim.spawn(f, backup_call("f1"));
        sim.expect_paused(f_snap, "f before its snapshot PUT");
        let rl = sim.call(l, exclusive_fold());
        expect_active_sessions(&rl);
        assert!(
            !records(&sim, l, rl.index)
                .iter()
                .any(|r| r.op.is_write() && is_protected(&r.key)),
            "{kind:?}: l wrote beside the committing backup"
        );
        sim.resume(f_snap);
        let rf = sim.join(rf);
        rf.expect_ok();
        assert!(rf.lock_free);
        // The segments old enough to fold (04 step 7), so the fold writes a
        // head.
        sim.advance(crate::repo::ages::T_PACK + std::time::Duration::from_secs(60));
        let rl = sim.call(l, exclusive_fold());
        rl.expect_ok();
        assert!(!rl.lock_free);
        assert!(
            records(&sim, l, rl.index)
                .iter()
                .any(|r| r.op.is_write() && r.applied() && r.key.starts_with("index.h/")),
            "{kind:?}: the exclusive session wrote no head"
        );
        assert!(sim.store().head_overwrites().is_empty(), "{kind:?}");
        sim.restore(f, "f1").expect_ok();
        let set = if kind.records_multi_writer() {
            Invariant::STEP8
        } else {
            Invariant::STEP7
        };
        sim.check_invariants(set).assert_clean();
    }
}

/// A backup that crashed after its commit mark leaves a flagged marker: an
/// exclusive session refuses beside it (`ActiveSessions`) until it is stale
/// (45 min on the store's clock), and so does a single-writer compact
/// (`Locked`, its single-writer check); then they reap it and proceed.
/// Nothing is wedged. On a single-writer repository (the fallback store);
/// the first holder took the maintenance lock and yielded `Locked` before
/// 10-single-protocol step 8.
#[test]
fn a_crashed_committing_marker_stops_blocking_after_45_minutes() {
    let mut sim = encrypted(StoreKind::Plain);
    let (f, l, c) = (sim.client("f"), sim.client("l"), sim.client("c"));
    let f_head = sim.gate(GateRule::new(
        f,
        OpMatch::Kind(OpKind::Put),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let rf = sim.spawn(f, backup_call("f1"));
    sim.expect_paused(f_head, "f after its mark");
    sim.crash(f);
    sim.resume(f_head);
    assert!(!sim.join(rf).is_ok());
    let flagged = sim
        .store()
        .keys("sessions/")
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .any(|k| {
            let body = sim.store().object(&k).expect("listed").data;
            String::from_utf8_lossy(&body).contains("\"committing\":true")
        });
    assert!(flagged, "the crashed backup's marker stays flagged");
    expect_active_sessions(&sim.call(l, exclusive_fold()));
    expect_locked(&sim.compact(c));
    sim.advance(std::time::Duration::from_secs(46 * 60));
    sim.call(l, exclusive_fold()).expect_ok();
    sim.compact(c).expect_ok();
    sim.backup(l, "l1", &tree("l1")).expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// The exclusive registration's order (10-single-protocol step 8, X1): its
/// listing of `sessions/` (the first op of the run on `sessions/` after
/// its marker PUT) comes after that PUT, before the marker's delete, and
/// before any protected write. `released`: the step at which the marker
/// was deleted (`None`: not deleted in the run). Returns the step of the
/// listing.
fn assert_lists_sessions_after_its_marker(
    sim: &Sim,
    c: ClientId,
    run: usize,
    put: &OpRecord,
    released: Option<u64>,
) -> u64 {
    let recs = records(sim, c, run);
    let listing = recs
        .iter()
        .find(|r| r.step > put.step && OpMatch::AnyList.matches(r.op) && r.key == "sessions/")
        .unwrap_or_else(|| panic!("no sessions/ listing after the marker PUT: {recs:?}"));
    assert!(
        listing.step < released.unwrap_or(u64::MAX),
        "the listing after the marker PUT at {} came after its delete: {recs:?}",
        put.step
    );
    assert!(
        !recs.iter().any(|r| r.step < listing.step
            && OpMatch::AnyList.matches(r.op)
            && r.key == "sessions/"),
        "sessions/ listed before the marker PUT: {recs:?}"
    );
    let early = recs
        .iter()
        .find(|r| r.step < listing.step && r.op.is_write() && is_protected(&r.key));
    assert!(
        early.is_none(),
        "a protected write before the session listing: {early:?}"
    );
    listing.step
}

/// X1 in the order that only a registration listing after its marker PUT
/// survives: the exclusive session (a head writer, [`exclusive_fold`]) is
/// held at its marker PUT, the backup registers, marks and commits up to
/// its snapshot PUT, then the exclusive marker is written. Its listing of
/// `sessions/` sees the backup's marker and it refuses (`ActiveSessions`,
/// its marker deleted, nothing protected written). With the order swapped
/// (`sessions/` listed before the PUT) it would list before the backup's
/// registration, run and this test fails. The holder took the lock before
/// 10-single-protocol step 8.
#[test]
fn an_exclusive_session_lists_sessions_after_its_marker_put() {
    for kind in [StoreKind::ConditionalOverwrite, StoreKind::Plain] {
        let mut sim = encrypted(kind);
        let (f, l) = (sim.client("f"), sim.client("l"));
        let l_put = sim.gate(
            GateRule::new(
                l,
                OpMatch::Kind(OpKind::Put),
                KeyMatch::Prefix("sessions/x-".into()),
            )
            .filter(|_, k| k.ends_with(".json")),
        );
        let rl = sim.spawn(l, exclusive_fold());
        sim.expect_paused(l_put, "l's marker PUT");
        let f_snap = sim.gate(snapshot_put(f));
        let rf = sim.spawn(f, backup_call("f1"));
        sim.expect_paused(f_snap, "f before its snapshot PUT");
        sim.resume(l_put);
        let rl = sim.join(rl);
        expect_active_sessions(&rl);
        let log = sim.store().log();
        let (put, released) = first_exclusive(&log, l);
        assert_lists_sessions_after_its_marker(&sim, l, rl.index, &put, Some(released));
        sim.resume(f_snap);
        let rf = sim.join(rf);
        rf.expect_ok();
        assert!(rf.lock_free);
        sim.call(l, exclusive_fold()).expect_ok();
        assert!(sim.store().head_overwrites().is_empty(), "{kind:?}");
        let set = if kind.records_multi_writer() {
            Invariant::STEP8
        } else {
            Invariant::STEP7
        };
        sim.check_invariants(set).assert_clean();
    }
}

/// The same order for the exclusive commands: a delete as an exclusive
/// session ([`exclusive_delete`]) and `check --repair` list `sessions/`
/// only after their marker PUT, and before any protected write (after
/// their lock was confirmed before 10-single-protocol step 8).
#[test]
fn exclusive_runs_list_sessions_after_their_marker_put() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    for name in ["a1", "a2"] {
        sim.backup(a, name, &tree(name)).expect_ok();
    }
    let rd = sim.call(a, exclusive_delete("a1"));
    rd.expect_ok();
    let rc = sim.call(a, Call::CheckRepair);
    rc.expect_ok();
    for run in [&rd, &rc] {
        assert!(
            run.exclusive,
            "run {} registered no exclusive session",
            run.index
        );
        let recs = records(&sim, a, run.index);
        let put = recs
            .iter()
            .find(|r| r.op == OpKind::Put && is_exclusive_key(&r.key))
            .unwrap_or_else(|| panic!("run {} wrote no exclusive marker: {recs:?}", run.index))
            .clone();
        let released = recs
            .iter()
            .find(|r| r.step > put.step && r.op.is_delete() && r.key == put.key)
            .map(|r| r.step);
        assert_lists_sessions_after_its_marker(&sim, a, run.index, &put, released);
        assert!(
            !recs.iter().any(|r| r.key.starts_with("locks/")),
            "run {} touched locks/: {recs:?}",
            run.index
        );
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// F3: a lock holder that listed the lock-free writer's flagged key n when
/// it was stale (the heartbeat held for 47 min) deletes it after the
/// heartbeat wrote key n + 1 and found n still present: n + 1 is present
/// and no miss is recorded, yet the holder took the session for reaped and
/// did not yield. The record's gap (over `MARKER_GAP_LIMIT`) is what tells:
/// the commit refuses its snapshot with `MarkerLostDuringCommit`. The gap
/// arose after the commit mark, so the backup had decided lock-free.
#[test]
fn a_marker_reaped_by_age_beside_its_refresh_refuses_the_snapshot() {
    const WAIT: std::time::Duration = std::time::Duration::from_secs(5);
    let mut sim = encrypted(StoreKind::Conditional);
    let (f, c) = (sim.client("f"), sim.client("c"));
    let f_head = sim.gate(GateRule::new(
        f,
        OpMatch::Kind(OpKind::PutIfAbsent),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let rf = sim.spawn(f, backup_call("f1"));
    sim.expect_paused(f_head, "f after its mark and probe");
    // F's main thread is held: only its heartbeat writes sessions/ now.
    let g_put = sim.gate(
        GateRule::new(f, OpMatch::AnyWrite, KeyMatch::Prefix("sessions/".into()))
            .filter(|_, k| k.ends_with(".json")),
    );
    assert!(sim.time().wait_for_waiters(1, WAIT), "f's heartbeat waits");
    for i in 0..30 {
        sim.advance(std::time::Duration::from_secs(30));
        if i < 29 {
            assert!(sim.time().wait_for_waiters(1, WAIT), "f's heartbeat waits");
        }
    }
    sim.expect_paused(g_put, "the heartbeat's refresh PUT");
    // The flagged key is 47 min old on the store's clock: stale.
    sim.advance(std::time::Duration::from_secs(32 * 60));
    let flagged: Vec<String> = sim
        .store()
        .keys("sessions/")
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .collect();
    let c_reap = sim.gate(GateRule::new(c, OpMatch::AnyDelete, KeyMatch::SessionKey));
    let rc = sim.spawn(c, Call::Compact);
    sim.expect_paused(c_reap, "c's reap of f's stale marker");
    // The refresh writes n + 1 and finds the listed keys present.
    sim.resume(g_put);
    assert!(sim.time().wait_for_waiters(1, WAIT), "the refresh ended");
    sim.resume(c_reap);
    let rc = sim.join(rc);
    rc.expect_ok();
    let reaped: Vec<String> = records(&sim, c, rc.index)
        .into_iter()
        .filter(|r| r.op.is_delete() && flagged.contains(&r.key))
        .map(|r| r.key)
        .collect();
    assert!(!reaped.is_empty(), "c reaped f's marker: {flagged:?}");
    let left: Vec<String> = sim
        .store()
        .keys("sessions/")
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .collect();
    assert!(
        left.iter().any(|k| !flagged.contains(k)),
        "the refresh's key n + 1 is present: {left:?}"
    );
    sim.resume(f_head);
    let rf = sim.join(rf);
    assert_eq!(
        rf.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::MarkerLostDuringCommit)),
        "{}",
        rf.detail()
    );
    assert!(rf.lock_free, "f decided lock-free before the gap");
    assert!(lock_writes(&records(&sim, f, rf.index)).is_empty());
    assert!(
        !records(&sim, f, rf.index)
            .iter()
            .any(|r| r.op.is_write() && r.key.starts_with("snapshots/")),
        "f wrote its snapshot"
    );
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// An undecided put-if-absent check (its probe create fails) fails the
/// backup right after open, before the upload (10-single-protocol step 5):
/// no guess either way, so no lock is taken (no fallback to lock mode), no
/// pack, head or snapshot written; what the run wrote is its session
/// marker and the check's probe. The next backup checks again and commits
/// lock-free.
#[test]
fn an_undecided_put_if_absent_check_fails_the_backup() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    sim.fault(
        FaultRule::new(
            OpMatch::Kind(OpKind::PutIfAbsent),
            KeyMatch::Prefix(crate::repo::ages::PUT_IF_ABSENT_PROBE_PREFIX.into()),
            FaultAction::Fail,
        )
        .client(a),
    );
    let run = sim.backup(a, "a1", &tree("a1"));
    assert!(!run.is_ok(), "the backup failed");
    assert!(!run.lock_free);
    let recs = records(&sim, a, run.index);
    let stray: Vec<&OpRecord> = recs
        .iter()
        .filter(|r| r.op.is_mutation())
        .filter(|r| {
            !(r.key.starts_with("sessions/")
                || (r.key.starts_with("locks/") && !KeyMatch::LockKey.matches(&r.key)))
        })
        .collect();
    assert!(stray.is_empty(), "{}: {stray:?}", run.detail());
    assert!(lock_writes(&recs).is_empty(), "no lock was taken");
    assert!(
        sim.store()
            .keys("sessions/")
            .iter()
            .all(|k| !k.ends_with(".json")),
        "the session deregistered"
    );
    let again = sim.backup(a, "a2", &tree("a2"));
    again.expect_ok();
    assert!(again.lock_free);
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// A cancel while a lock-free commit waits for a live exclusive session
/// (its marker flagged; its earlier keys reaped by a misjudging reaper, as
/// in [`a_commit_waits_for_a_live_exclusive_session`]) returns `Interrupted` within one sleep slice (500 ms), with no
/// further probe. The cancelled backup leaves what any abandoned backup
/// leaves: its packs and its journal (`sessions/<id>.index`, for the next
/// run's recovery); its marker keys, the flagged one included, are deleted
/// at deregistration, so lock holders no longer yield to it; no lock, head
/// or snapshot.
#[test]
fn a_cancel_during_the_lock_free_wait_returns_within_a_slice() {
    let mut sim = encrypted(StoreKind::ConditionalOverwrite);
    let (f, l) = (sim.client("f"), sim.client("l"));
    sim.backup(l, "l0", &tree("l0")).expect_ok();
    // F's listings of sessions/: its registration's, the open's, the
    // pre-head one after its mark (finds L's session), the one after one
    // backoff.
    let f_retry = sim.gate(sessions_listing(f, 4));
    let f_mark = sim.gate(commit_mark(f));
    let cancel = CancelToken::new();
    let rf = sim.spawn(
        f,
        Call::BackupCancellable {
            snapshot: "f1".into(),
            tree: tree("f1"),
            cancel: cancel.clone(),
        },
    );
    sim.expect_paused(f_mark, "f's commit mark");
    reap_markers_of(&mut sim, f);
    // The holder: a delete as an exclusive session (under the maintenance
    // lock until 10-single-protocol step 8, a lock-mode backup until step 7).
    let l_head = sim.gate(snapshot_delete(l));
    let rl = sim.spawn(l, exclusive_delete("l0"));
    sim.expect_paused(l_head, "l's snapshot delete in its exclusive session");
    sim.resume(f_mark);
    sim.expect_paused(f_retry, "f's second probe");
    // From here f's backoff sleeps until the test advances time.
    sim.time().set_mode(TimeMode::Manual);
    sim.resume(f_retry);
    let sleeping = |sim: &Sim| {
        sim.time()
            .waiters()
            .iter()
            .any(|w| w.thread.starts_with(&format!("sim-f-{}", rf.index())))
    };
    let start = std::time::Instant::now();
    while !sleeping(&sim) {
        assert!(
            start.elapsed() < std::time::Duration::from_secs(30),
            "f did not reach its backoff sleep: {:?} {:?} {:?}",
            sim.time().waiters(),
            sim.store().paused(),
            records(&sim, f, rf.index())
                .iter()
                .rev()
                .take(5)
                .collect::<Vec<_>>()
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let probes = |sim: &Sim| {
        sim.store()
            .log()
            .iter()
            .filter(|r| r.client == f && OpMatch::AnyList.matches(r.op) && r.key == "sessions/")
            .count()
    };
    let before = probes(&sim);
    cancel.cancel();
    // One slice of the backoff (at least 750 ms after the second probe).
    sim.time().advance(std::time::Duration::from_millis(500));
    sim.time().set_mode(TimeMode::Auto);
    let rf = sim.join(rf);
    assert_eq!(
        rf.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::Interrupted)),
        "{}",
        rf.detail()
    );
    assert_eq!(probes(&sim), before, "no probe after the cancel");
    let recs = records(&sim, f, rf.index);
    assert!(lock_writes(&recs).is_empty());
    assert!(
        !recs.iter().any(|r| r.op.is_write() && is_protected(&r.key)),
        "f wrote a head or a snapshot"
    );
    let f_keys = sim.store().keys_written_by(f, "sessions/");
    let left: Vec<&String> = f_keys
        .iter()
        .filter(|k| sim.store().object(k).is_some())
        .collect();
    assert!(
        left.iter().all(|k| k.ends_with(".index")) && !left.is_empty(),
        "only f's journal is left: {left:?}"
    );
    sim.resume(l_head);
    let rl = sim.join(rl);
    rl.expect_ok();
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// A gate on `c`'s first pack PUT: during its upload, before its commit
/// mode is decided.
fn first_pack_put(c: ClientId) -> GateRule {
    GateRule::new(c, OpMatch::AnyWrite, KeyMatch::Prefix("packs/".into()))
}

/// Suspend `c` for 35 min mid-upload: its marker record then has a gap
/// over `MARKER_GAP_LIMIT` before it decides its commit mode. Returns the
/// gate it is held at.
fn suspend_mid_upload(sim: &mut Sim, c: ClientId, name: &str) -> (GateId, Spawned) {
    let gate = sim.gate(first_pack_put(c));
    let run = sim.spawn(c, backup_call(name));
    sim.expect_paused(gate, "the first pack PUT");
    sim.advance(std::time::Duration::from_secs(35 * 60));
    (gate, run)
}

/// A backup whose marker record already has a gap over
/// `MARKER_GAP_LIMIT` when it is about to commit (its machine suspended
/// for 35 min mid-upload) commits lock-free, on an encrypted repository on
/// the conditional store (10-single-protocol step 7, the suspend rule; it
/// committed under the lock before): no lock key written, a GET of
/// `config` after its commit mark (the rule's re-read), put-if-absent
/// heads, the snapshot committed and restorable. The next backup's record
/// is continuous: no `config` GET in its commit.
#[test]
fn a_backup_with_a_gap_before_its_commit_commits_lock_free() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    let (gate, ra) = suspend_mid_upload(&mut sim, a, "a1");
    sim.resume(gate);
    let ra = sim.join(ra);
    ra.expect_ok();
    assert!(ra.lock_free, "a commits lock-free");
    let recs = records(&sim, a, ra.index);
    assert!(lock_writes(&recs).is_empty(), "a lock was taken: {recs:?}");
    let mark = recs
        .iter()
        .position(|r| r.op == OpKind::Put && r.key.ends_with(".1.json"))
        .expect("a's commit mark");
    assert!(
        recs[mark..]
            .iter()
            .any(|r| r.op == OpKind::Get && r.key == "config"),
        "no config GET after the mark: {recs:?}"
    );
    let heads: Vec<&OpRecord> = recs
        .iter()
        .filter(|r| r.op.is_write() && r.key.starts_with("index.h/"))
        .collect();
    assert!(!heads.is_empty(), "{recs:?}");
    assert!(
        heads.iter().all(|r| r.op == OpKind::PutIfAbsent),
        "{heads:?}"
    );
    sim.restore(a, "a1").expect_ok();
    let again = sim.backup(a, "a2", &tree("a2"));
    again.expect_ok();
    assert!(again.lock_free);
    let recs = records(&sim, a, again.index);
    let mark = recs
        .iter()
        .position(|r| r.op == OpKind::Put && r.key.ends_with(".1.json"))
        .expect("a2's commit mark");
    assert!(
        !recs[mark..]
            .iter()
            .any(|r| r.op == OpKind::Get && r.key == "config"),
        "a continuous record re-read config: {recs:?}"
    );
    sim.check_invariants(Invariant::STEP8).assert_clean();
}
