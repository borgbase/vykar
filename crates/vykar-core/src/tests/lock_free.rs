//! Lock-free backups (04 step 9, phase F part 10c) on the simulator: two
//! backups commit without a lock; a backup waits for a held lock and a
//! lock holder yields to a committing backup (the marker handshake); a
//! crashed committing marker blocks lock holders until it is stale. Lock
//! holders list sessions only after their lock is confirmed; a marker
//! reaped by age beside its refresh, an undecided put-if-absent check and a
//! cancel during the wait each fail the backup as they should. A backup
//! whose marker record has a gap before its commit commits lock-free after
//! the suspend rule.
//!
//! Since 10-single-protocol step 7 no backup or compact takes the lock: the
//! lock holders here hold the maintenance lock as `check --repair`, upgrade
//! and pack maps do until step 9 (`Call::LockedCustom`,
//! `Call::LockedDelete`). `a_locked_compact_fails_locked_beside_a_committing_backup`
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

/// `c`'s `n`th listing of `locks/`.
fn locks_listing(c: ClientId, n: u32) -> GateRule {
    GateRule::new(c, OpMatch::AnyList, KeyMatch::LockKey).nth(n)
}

/// A lock holder that writes a head: a fold under the maintenance lock
/// ([`Call::LockedCustom`]), as `check --repair`, upgrade and pack maps
/// hold the lock until 10-single-protocol step 9 (lock-mode backups, the
/// holders here before step 7, exist no more).
fn locked_fold() -> Call {
    Call::LockedCustom {
        label: "locked fold".into(),
        f: Box::new(|repo| {
            repo.load_chunk_index_under_lock()?;
            repo.request_index_fold();
            repo.mark_index_dirty();
            repo.save_state()
        }),
    }
}

/// A lock holder that deletes snapshot `name` ([`Call::LockedDelete`]).
fn locked_delete(name: &str) -> Call {
    Call::LockedDelete {
        names: vec![name.into()],
    }
}

/// `c`'s snapshot delete: a [`locked_delete`] holds its lock there.
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

/// The first lock key `c` wrote in `log` and the step its run released it.
fn first_lock(log: &[OpRecord], c: ClientId) -> (OpRecord, u64) {
    let put = log
        .iter()
        .find(|r| r.client == c && r.op == OpKind::Put && KeyMatch::LockKey.matches(&r.key))
        .expect("a lock key written")
        .clone();
    let released = log
        .iter()
        .find(|r| r.client == c && r.step > put.step && r.op.is_delete() && r.key == put.key)
        .expect("the lock released")
        .step;
    (put, released)
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

/// The handshake, the session side: a backup (lock-free on a multi-writer
/// repository, single-writer on the fallback store) that meets a held lock
/// at its probe waits (its marker flagged) and creates no head until the
/// holder released it, then commits. The holder deletes under the
/// maintenance lock (a lock-mode backup until 10-single-protocol step 7).
#[test]
fn a_lock_free_commit_waits_for_a_held_lock() {
    for kind in [StoreKind::Conditional, StoreKind::Plain] {
        let mut sim = encrypted(kind);
        let (f, l) = (sim.client("f"), sim.client("l"));
        sim.backup(l, "l0", &tree("l0")).expect_ok();
        // F's listings of locks/: its registration probe, the probe after
        // its mark (finds L's lock), the probe after one backoff.
        let f_retry = sim.gate(locks_listing(f, 3));
        let f_mark = sim.gate(commit_mark(f));
        let rf = sim.spawn(f, backup_call("f1"));
        sim.expect_paused(f_mark, "f's commit mark");
        let l_held = sim.gate(snapshot_delete(l));
        let rl = sim.spawn(l, locked_delete("l0"));
        sim.expect_paused(l_held, "l's snapshot delete under its lock");
        sim.resume(f_mark);
        sim.expect_paused(f_retry, "f's second probe");
        let log = sim.store().log();
        assert!(
            !log.iter()
                .any(|r| r.client == f && r.op.is_write() && r.key.starts_with("index.h/")),
            "{kind:?}: f created a head while l held the lock"
        );
        sim.resume(l_held);
        let rl = sim.join(rl);
        rl.expect_ok();
        assert!(!rl.lock_free);
        sim.resume(f_retry);
        let rf = sim.join(rf);
        rf.expect_ok();
        assert!(rf.lock_free);
        sim.check_invariants(Invariant::STEP8).assert_clean();
    }
}

/// The handshake, lock-holder side: a holder of the maintenance lock that
/// writes a head ([`locked_fold`]) and acquires the lock while a backup is
/// between its head create and its snapshot PUT refuses (`Locked`) and
/// releases the lock before any protected write; run again after the
/// backup ended it writes its head. No head is replaced on a store whose
/// plain PUT overwrites (invariant 12 clean on the multi-writer one). Until
/// 10-single-protocol step 7 the holder was a lock-mode backup, which
/// waited and retried instead of refusing.
#[test]
fn a_lock_holder_yields_to_a_committing_backup() {
    for kind in [StoreKind::ConditionalOverwrite, StoreKind::Plain] {
        let mut sim = encrypted(kind);
        let (f, l) = (sim.client("f"), sim.client("l"));
        let f_snap = sim.gate(snapshot_put(f));
        let rf = sim.spawn(f, backup_call("f1"));
        sim.expect_paused(f_snap, "f before its snapshot PUT");
        let rl = sim.call(l, locked_fold());
        expect_locked(&rl);
        let log = sim.store().log();
        let (first_put, released) = first_lock(&log, l);
        assert!(
            !log.iter().any(|r| r.client == l
                && r.step > first_put.step
                && r.step < released
                && r.op.is_write()
                && is_protected(&r.key)),
            "{kind:?}: l wrote under a lock it should have yielded"
        );
        sim.resume(f_snap);
        let rf = sim.join(rf);
        rf.expect_ok();
        assert!(rf.lock_free);
        // The segments old enough to fold (04 step 7), so the fold writes a
        // head.
        sim.advance(crate::repo::ages::T_PACK + std::time::Duration::from_secs(60));
        let rl = sim.call(l, locked_fold());
        rl.expect_ok();
        assert!(!rl.lock_free);
        assert!(
            records(&sim, l, rl.index)
                .iter()
                .any(|r| r.op.is_write() && r.applied() && r.key.starts_with("index.h/")),
            "{kind:?}: the holder wrote no head"
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

/// A backup that crashed after its commit mark leaves a flagged marker:
/// lock holders yield to it (`Locked`) until it is stale (45 min on the
/// store's clock), and so does a single-writer compact (its single-writer
/// check); then they reap it and proceed. Nothing is wedged. Since
/// 10-single-protocol step 7 on a single-writer repository (the fallback
/// store), its holders the maintenance lock's ([`locked_fold`]) and a
/// compact (lock-mode clients' lock and compact before).
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
    expect_locked(&sim.call(l, locked_fold()));
    expect_locked(&sim.compact(c));
    sim.advance(std::time::Duration::from_secs(46 * 60));
    sim.call(l, locked_fold()).expect_ok();
    sim.compact(c).expect_ok();
    sim.backup(l, "l1", &tree("l1")).expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// Every op of `c`'s run `run` on a lock key or a listing of `locks/` or
/// `sessions/`, in order.
fn lock_ops(sim: &Sim, c: ClientId, run: usize) -> Vec<OpRecord> {
    records(sim, c, run)
        .into_iter()
        .filter(|r| {
            KeyMatch::LockKey.matches(&r.key)
                || (OpMatch::AnyList.matches(r.op) && (r.key == "locks/" || r.key == "sessions/"))
        })
        .collect()
}

/// The lock holder's order (04 step 9, the marker handshake): the
/// listing of `sessions/` that judges the lock it holds comes after that
/// lock's PUT and after the `locks/` listing that confirmed it, and before
/// any protected write under it. `released`: the step at which that lock
/// was deleted (`None`: held to the end of the run). Returns the step of
/// the session listing.
fn assert_lists_sessions_after_its_confirmed_lock(
    sim: &Sim,
    c: ClientId,
    run: usize,
    lock_put: &OpRecord,
    released: Option<u64>,
) -> u64 {
    let ops = lock_ops(sim, c, run);
    let confirm = ops
        .iter()
        .find(|r| r.step > lock_put.step && OpMatch::AnyList.matches(r.op) && r.key == "locks/")
        .unwrap_or_else(|| panic!("no locks/ listing after the lock PUT: {ops:?}"));
    let end = released.unwrap_or(u64::MAX);
    // The listing judged under this lock: the first one after its PUT. It
    // must follow the confirming listing and precede the release.
    let listing = ops
        .iter()
        .find(|r| r.step > lock_put.step && OpMatch::AnyList.matches(r.op) && r.key == "sessions/")
        .unwrap_or_else(|| panic!("no sessions/ listing after the lock PUT: {ops:?}"));
    assert!(
        listing.step > confirm.step,
        "sessions/ listed at {} before the lock PUT at {} was confirmed at {}: {ops:?}",
        listing.step,
        lock_put.step,
        confirm.step
    );
    assert!(
        listing.step < end,
        "the first sessions/ listing after the lock PUT at {} came at {}, after the release at {end}: {ops:?}",
        lock_put.step,
        listing.step
    );
    let early = records(sim, c, run).into_iter().find(|r| {
        r.step > lock_put.step && r.step < listing.step && r.op.is_write() && is_protected(&r.key)
    });
    assert!(
        early.is_none(),
        "a protected write before the session listing: {early:?}"
    );
    listing.step
}

/// The handshake, lock-holder side, in the order that only a holder whose
/// session listing follows its confirmed lock survives: the holder (a head
/// writer under the maintenance lock, [`locked_fold`]; a lock-mode backup
/// until 10-single-protocol step 7) is held at its lock PUT, the backup
/// marks, probes `locks/` (empty) and commits up to its snapshot PUT, then
/// the holder writes its lock. Its listing of `sessions/` after the lock
/// is confirmed sees the flagged marker and it yields (`Locked`, the lock
/// released with nothing protected written). With the holder's order
/// swapped (`sessions/` listed before the lock PUT) it lists before the
/// mark, does not yield and this test fails.
#[test]
fn a_lock_holder_lists_sessions_after_its_lock_is_confirmed() {
    for kind in [StoreKind::ConditionalOverwrite, StoreKind::Plain] {
        let mut sim = encrypted(kind);
        let (f, l) = (sim.client("f"), sim.client("l"));
        let l_put = sim.gate(GateRule::new(
            l,
            OpMatch::Kind(OpKind::Put),
            KeyMatch::LockKey,
        ));
        let rl = sim.spawn(l, locked_fold());
        sim.expect_paused(l_put, "l's lock PUT");
        let f_snap = sim.gate(snapshot_put(f));
        let rf = sim.spawn(f, backup_call("f1"));
        sim.expect_paused(f_snap, "f before its snapshot PUT");
        sim.resume(l_put);
        let rl = sim.join(rl);
        expect_locked(&rl);
        let log = sim.store().log();
        let (first_put, released) = first_lock(&log, l);
        assert_lists_sessions_after_its_confirmed_lock(
            &sim,
            l,
            rl.index,
            &first_put,
            Some(released),
        );
        let mark = log
            .iter()
            .find(|r| {
                r.client == f
                    && r.op == OpKind::Put
                    && r.key.starts_with("sessions/")
                    && r.key.ends_with(".1.json")
            })
            .expect("f marked its commit");
        assert!(mark.step < first_put.step, "f marked before l's lock PUT");
        sim.resume(f_snap);
        let rf = sim.join(rf);
        rf.expect_ok();
        assert!(rf.lock_free);
        sim.call(l, locked_fold()).expect_ok();
        assert!(sim.store().head_overwrites().is_empty(), "{kind:?}");
        let set = if kind.records_multi_writer() {
            Invariant::STEP8
        } else {
            Invariant::STEP7
        };
        sim.check_invariants(set).assert_clean();
    }
}

/// The same order for the maintenance lock's holders
/// (`with_maintenance_lock`: the listing right after the lock): a delete
/// under it ([`locked_delete`]) and `check --repair` list `sessions/` only
/// after their lock is confirmed, and before any protected write. Until
/// 10-single-protocol step 7 the subjects were a lock client's delete and
/// compact, which run in a session since.
#[test]
fn maintenance_lists_sessions_after_its_lock_is_confirmed() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    for name in ["a1", "a2"] {
        sim.backup(a, name, &tree(name)).expect_ok();
    }
    let rd = sim.call(a, locked_delete("a1"));
    rd.expect_ok();
    let rc = sim.call(a, Call::CheckRepair);
    rc.expect_ok();
    for run in [&rd, &rc] {
        let ops = lock_ops(&sim, a, run.index);
        let put = ops
            .iter()
            .find(|r| r.op == OpKind::Put && KeyMatch::LockKey.matches(&r.key))
            .unwrap_or_else(|| panic!("run {} took no lock: {ops:?}", run.index))
            .clone();
        let released = ops
            .iter()
            .find(|r| r.step > put.step && r.op.is_delete() && r.key == put.key)
            .map(|r| r.step);
        assert_lists_sessions_after_its_confirmed_lock(&sim, a, run.index, &put, released);
        assert!(
            !ops.iter().any(|r| r.step < put.step
                && OpMatch::AnyList.matches(r.op)
                && r.key == "sessions/"),
            "run {} listed sessions/ before its lock PUT: {ops:?}",
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

/// A cancel while a lock-free commit waits for a held lock (its marker
/// flagged) returns `Interrupted` within one sleep slice (500 ms), with no
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
    // F's listings of locks/: its registration probe, the probe after its
    // mark (finds L's lock), the probe after one backoff.
    let f_retry = sim.gate(locks_listing(f, 3));
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
    // The holder: a delete under the maintenance lock (a lock-mode backup
    // until 10-single-protocol step 7).
    let l_head = sim.gate(snapshot_delete(l));
    let rl = sim.spawn(l, locked_delete("l0"));
    sim.expect_paused(l_head, "l's snapshot delete under its lock");
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
            .filter(|r| r.client == f && OpMatch::AnyList.matches(r.op) && r.key == "locks/")
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
