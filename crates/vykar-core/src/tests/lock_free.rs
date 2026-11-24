//! Lock-free backups (04 step 9, phase F part 10c) on the simulator: two
//! backups commit without a lock; a lock-free commit waits for a held lock
//! and a lock holder yields to a committing backup (the marker
//! handshake); a crashed committing marker blocks lock holders until it is
//! stale; plaintext repositories keep the lock. Lock holders list
//! sessions only after their lock is confirmed; a marker reaped by age
//! beside its refresh, an undecided put-if-absent check and a cancel during
//! the wait each fail the backup as they should. A backup whose marker
//! record has a gap before its commit commits under the lock, and yields
//! there to a committing lock-free backup.

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
        encrypted: true,
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

/// The handshake, lock-free side: a lock-free commit that meets a held
/// lock at its probe waits (its marker flagged) and creates no head until
/// the holder released it, then commits.
#[test]
fn a_lock_free_commit_waits_for_a_held_lock() {
    let mut sim = encrypted(StoreKind::ConditionalOverwrite);
    let (f, l) = (sim.client("f"), sim.client("l"));
    sim.set_lock_client(l);
    // F's listings of locks/: its registration probe, the probe after its
    // mark (finds L's lock), the probe after one backoff.
    let f_retry = sim.gate(locks_listing(f, 3));
    let f_mark = sim.gate(
        GateRule::new(
            f,
            OpMatch::Kind(OpKind::Put),
            KeyMatch::Prefix("sessions/".into()),
        )
        .filter(|_, k| k.ends_with(".1.json")),
    );
    let rf = sim.spawn(f, backup_call("f1"));
    sim.expect_paused(f_mark, "f's commit mark");
    let l_head = sim.gate(GateRule::new(
        l,
        OpMatch::Kind(OpKind::Put),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let rl = sim.spawn(l, backup_call("l1"));
    sim.expect_paused(l_head, "l's head PUT under its lock");
    sim.resume(f_mark);
    sim.expect_paused(f_retry, "f's second probe");
    let log = sim.store().log();
    assert!(
        !log.iter()
            .any(|r| r.client == f && r.op.is_write() && r.key.starts_with("index.h/")),
        "f created a head while l held the lock"
    );
    sim.resume(l_head);
    let rl = sim.join(rl);
    rl.expect_ok();
    assert!(!rl.lock_free);
    sim.resume(f_retry);
    let rf = sim.join(rf);
    rf.expect_ok();
    assert!(rf.lock_free);
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// The handshake, lock-holder side: a lock-mode backup that acquires the
/// lock while a lock-free one is between its head create and its snapshot
/// PUT releases it again before any protected write, and commits after the
/// lock-free one ended; no head is replaced on a store whose plain PUT
/// overwrites (invariant 12 clean).
#[test]
fn a_lock_holder_yields_to_a_committing_backup() {
    let mut sim = encrypted(StoreKind::ConditionalOverwrite);
    let (f, l) = (sim.client("f"), sim.client("l"));
    sim.set_lock_client(l);
    let f_snap = sim.gate(snapshot_put(f));
    let rf = sim.spawn(f, backup_call("f1"));
    sim.expect_paused(f_snap, "f before its snapshot PUT");
    let l_second = sim.gate(GateRule::new(l, OpMatch::Kind(OpKind::Put), KeyMatch::LockKey).nth(2));
    let rl = sim.spawn(l, backup_call("l1"));
    sim.expect_paused(l_second, "l's second lock attempt");
    // L's first attempt: the lock key written, then deleted, nothing
    // protected in between.
    let log = sim.store().log();
    let first_put = log
        .iter()
        .find(|r| r.client == l && r.op == OpKind::Put && KeyMatch::LockKey.matches(&r.key))
        .expect("l wrote a lock key")
        .clone();
    let released = log
        .iter()
        .find(|r| {
            r.client == l && r.step > first_put.step && r.op.is_delete() && r.key == first_put.key
        })
        .expect("l released its first lock");
    assert!(
        !log.iter().any(|r| r.client == l
            && r.step > first_put.step
            && r.step < released.step
            && r.op.is_write()
            && is_protected(&r.key)),
        "l wrote under a lock it should have yielded"
    );
    sim.resume(f_snap);
    let rf = sim.join(rf);
    rf.expect_ok();
    assert!(rf.lock_free);
    sim.resume(l_second);
    let rl = sim.join(rl);
    rl.expect_ok();
    assert!(!rl.lock_free);
    assert!(sim.store().head_overwrites().is_empty());
    for name in ["f1", "l1"] {
        sim.restore(f, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// A compact (under the maintenance lock in 10c) that meets a committing
/// lock-free backup at its session listing fails `Locked` naming the
/// committing host, before any write.
#[test]
fn a_locked_compact_fails_locked_beside_a_committing_backup() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (f, c) = (sim.client("f"), sim.client("c"));
    let f_snap = sim.gate(snapshot_put(f));
    let rf = sim.spawn(f, backup_call("f1"));
    sim.expect_paused(f_snap, "f before its snapshot PUT");
    let rc = sim.compact(c);
    expect_locked(&rc);
    let host = sim.host(f).hostname;
    assert!(rc.detail().contains(&host), "{}", rc.detail());
    let wrote = records(&sim, c, rc.index)
        .iter()
        .any(|r| r.op.is_write() && (is_protected(&r.key) || r.key.starts_with("packs/")));
    assert!(!wrote, "the compact wrote before yielding");
    sim.resume(f_snap);
    sim.join(rf).expect_ok();
    sim.compact(c).expect_ok();
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// A lock-free backup that crashed after its commit mark leaves a flagged
/// marker: lock holders yield to it (`Locked`) until it is stale (45 min
/// on the store's clock), then reap it and proceed. Nothing is wedged.
#[test]
fn a_crashed_committing_marker_stops_blocking_after_45_minutes() {
    let mut sim = encrypted(StoreKind::ConditionalOverwrite);
    let (f, l, c) = (sim.client("f"), sim.client("l"), sim.client("c"));
    sim.set_lock_client(l);
    let f_head = sim.gate(GateRule::new(
        f,
        OpMatch::Kind(OpKind::PutIfAbsent),
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
    expect_locked(&sim.compact(c));
    sim.advance(std::time::Duration::from_secs(46 * 60));
    sim.compact(c).expect_ok();
    // The reap cleared it: a lock-mode backup commits at once.
    let rl = sim.backup(l, "l1", &tree("l1"));
    rl.expect_ok();
    assert!(!rl.lock_free);
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// A plaintext repository never commits lock-free (O1): on a store that
/// offers put-if-absent its backups still take the commit lock.
#[test]
fn plaintext_repositories_commit_under_the_lock() {
    let mut sim = Sim::with_config(SimConfig {
        kind: StoreKind::Conditional,
        ..SimConfig::default()
    });
    let a = sim.client("a");
    let run = sim.backup(a, "a1", &tree("a1"));
    run.expect_ok();
    assert!(!run.lock_free);
    let recs = records(&sim, a, run.index);
    assert!(!lock_writes(&recs).is_empty(), "the commit lock was taken");
    sim.check_invariants(Invariant::STEP8).assert_clean();
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
/// session listing follows its confirmed lock survives: the holder is held
/// at its lock PUT, the lock-free backup marks, probes `locks/` (empty) and
/// commits up to its snapshot PUT, then the holder writes its lock. Its
/// listing of `sessions/` after the lock is confirmed sees the flagged
/// marker and it yields (releases the lock with nothing protected written).
/// With the holder's order swapped (`sessions/` listed before the lock PUT)
/// it lists before the mark, does not yield and this test fails.
#[test]
fn a_lock_holder_lists_sessions_after_its_lock_is_confirmed() {
    let mut sim = encrypted(StoreKind::ConditionalOverwrite);
    let (f, l) = (sim.client("f"), sim.client("l"));
    sim.set_lock_client(l);
    let l_put = sim.gate(GateRule::new(
        l,
        OpMatch::Kind(OpKind::Put),
        KeyMatch::LockKey,
    ));
    let rl = sim.spawn(l, backup_call("l1"));
    sim.expect_paused(l_put, "l's lock PUT");
    let f_snap = sim.gate(snapshot_put(f));
    let rf = sim.spawn(f, backup_call("f1"));
    sim.expect_paused(f_snap, "f before its snapshot PUT");
    let l_second = sim.gate(GateRule::new(l, OpMatch::Kind(OpKind::Put), KeyMatch::LockKey).nth(2));
    sim.resume(l_put);
    sim.expect_paused(l_second, "l's second lock attempt");
    let log = sim.store().log();
    let l_run = log
        .iter()
        .find(|r| r.client == l)
        .and_then(|r| r.run)
        .expect("l ran");
    let first_put = log
        .iter()
        .find(|r| r.client == l && r.op == OpKind::Put && KeyMatch::LockKey.matches(&r.key))
        .expect("l wrote a lock key")
        .clone();
    let released = log
        .iter()
        .find(|r| {
            r.client == l && r.step > first_put.step && r.op.is_delete() && r.key == first_put.key
        })
        .expect("l yielded: it released its first lock")
        .step;
    assert_lists_sessions_after_its_confirmed_lock(&sim, l, l_run, &first_put, Some(released));
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
    sim.resume(l_second);
    let rl = sim.join(rl);
    rl.expect_ok();
    assert!(!rl.lock_free);
    assert!(sim.store().head_overwrites().is_empty());
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// The same order for both maintenance entry points on an encrypted
/// repository: delete (`with_maintenance_lock`: the listing right after
/// the lock) and compact (`with_maintenance_lock_unlisted`: its pack
/// listing, then the session listing) list `sessions/` only after their
/// lock is confirmed, and before any protected write.
#[test]
fn maintenance_lists_sessions_after_its_lock_is_confirmed() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    for name in ["a1", "a2"] {
        sim.backup(a, name, &tree(name)).expect_ok();
    }
    let rd = sim.delete(a, &["a1"]);
    rd.expect_ok();
    let rc = sim.compact(a);
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
    let c_reap = sim.gate(GateRule::new(
        c,
        OpMatch::AnyDelete,
        KeyMatch::Prefix("sessions/".into()),
    ));
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
/// backup: no guess either way, so no lock is taken (no fallback to lock
/// mode), no head or snapshot written; what the run wrote is its session
/// marker, its packs and journal, and the check's probe. The next backup
/// checks again and commits lock-free.
#[test]
fn an_undecided_put_if_absent_check_fails_the_backup() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    sim.fault(
        FaultRule::new(
            OpMatch::Kind(OpKind::PutIfAbsent),
            KeyMatch::Prefix("locks/".into()),
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
                || r.key.starts_with("packs/")
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
    sim.set_lock_client(l);
    // F's listings of locks/: its registration probe, the probe after its
    // mark (finds L's lock), the probe after one backoff.
    let f_retry = sim.gate(locks_listing(f, 3));
    let f_mark = sim.gate(
        GateRule::new(
            f,
            OpMatch::Kind(OpKind::Put),
            KeyMatch::Prefix("sessions/".into()),
        )
        .filter(|_, k| k.ends_with(".1.json")),
    );
    let shutdown = CancelToken::new();
    let rf = sim.spawn(
        f,
        Call::BackupCancellable {
            snapshot: "f1".into(),
            tree: tree("f1"),
            shutdown: shutdown.clone(),
        },
    );
    sim.expect_paused(f_mark, "f's commit mark");
    let l_head = sim.gate(GateRule::new(
        l,
        OpMatch::Kind(OpKind::Put),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let rl = sim.spawn(l, backup_call("l1"));
    sim.expect_paused(l_head, "l's head PUT under its lock");
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
    shutdown.cancel();
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
/// for 35 min mid-upload) commits under the lock, on an encrypted
/// repository on the conditional store: no commit mark (`lock_free` is the
/// store's record of one), a lock key written, the snapshot committed and
/// restorable. Lock-free it would refuse its snapshot every time: the
/// record never becomes continuous again.
#[test]
fn a_backup_with_a_gap_before_its_commit_commits_under_the_lock() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    let (gate, ra) = suspend_mid_upload(&mut sim, a, "a1");
    sim.resume(gate);
    let ra = sim.join(ra);
    ra.expect_ok();
    assert!(!ra.lock_free, "a commits under the lock");
    assert!(
        !lock_writes(&records(&sim, a, ra.index)).is_empty(),
        "the commit lock was taken"
    );
    sim.restore(a, "a1").expect_ok();
    // The next backup's record is continuous again: lock-free.
    let again = sim.backup(a, "a2", &tree("a2"));
    again.expect_ok();
    assert!(again.lock_free);
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// The handshake still holds for a backup that commits under the lock
/// because of a gap in its record: it lists `sessions/` after its
/// confirmed lock, finds a lock-free backup committing (flagged, between
/// its head create and its snapshot PUT), releases the lock with nothing
/// protected written, and commits after the lock-free one ended.
#[test]
fn a_backup_with_a_gap_yields_to_a_committing_backup() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (d, f) = (sim.client("d"), sim.client("f"));
    let (d_upload, rd) = suspend_mid_upload(&mut sim, d, "d1");
    let f_snap = sim.gate(snapshot_put(f));
    let rf = sim.spawn(f, backup_call("f1"));
    sim.expect_paused(f_snap, "f before its snapshot PUT");
    let d_second = sim.gate(GateRule::new(d, OpMatch::Kind(OpKind::Put), KeyMatch::LockKey).nth(2));
    sim.resume(d_upload);
    sim.expect_paused(d_second, "d's second lock attempt");
    let log = sim.store().log();
    let first_put = log
        .iter()
        .find(|r| r.client == d && r.op == OpKind::Put && KeyMatch::LockKey.matches(&r.key))
        .expect("d wrote a lock key")
        .clone();
    let released = log
        .iter()
        .find(|r| {
            r.client == d && r.step > first_put.step && r.op.is_delete() && r.key == first_put.key
        })
        .expect("d yielded: it released its first lock")
        .step;
    assert_lists_sessions_after_its_confirmed_lock(&sim, d, rd.index(), &first_put, Some(released));
    assert!(
        !log.iter().any(|r| r.client == d
            && r.step > first_put.step
            && r.step < released
            && r.op.is_write()
            && is_protected(&r.key)),
        "d wrote under a lock it should have yielded"
    );
    sim.resume(f_snap);
    let rf = sim.join(rf);
    rf.expect_ok();
    assert!(rf.lock_free);
    sim.resume(d_second);
    let rd = sim.join(rd);
    rd.expect_ok();
    assert!(!rd.lock_free, "d commits under the lock");
    for name in ["d1", "f1"] {
        sim.restore(f, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}
