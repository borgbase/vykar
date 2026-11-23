//! The interleaving tests of the advisory lock, on the simulator: each
//! client runs on its own thread, held and resumed at gates (08 step 3).
//! They assert what `tests/lock.rs` asserted with hooked backends.

use crate::sim::history::{AllowedError, ErrorClass};
use crate::sim::{Call, ClientId, GateRule, KeyMatch, OpKind, OpMatch, RunRecord, Sim, SourceTree};

/// Lock keys only: a contended acquire's clock probe
/// (`locks/vykar-clock-*`) is neither gated nor counted.
fn locks() -> KeyMatch {
    KeyMatch::LockKey
}

/// A run that takes the repo lock and does nothing under it.
fn hold() -> Call {
    Call::Custom {
        label: "hold".into(),
        f: Box::new(|_| Ok(())),
    }
}

fn assert_allowed(run: &RunRecord, e: AllowedError) {
    assert_eq!(
        run.error_class(),
        Some(&ErrorClass::Allowed(e)),
        "{}",
        run.detail()
    );
}

fn lock_writes(sim: &Sim) -> usize {
    sim.store()
        .log()
        .iter()
        .filter(|r| r.op.is_write() && KeyMatch::LockKey.matches(&r.key))
        .count()
}

fn tree(seed: &str) -> SourceTree {
    SourceTree::seeded(seed, 2, 16 * 1024)
}

fn list_of(c: ClientId, prefix: &str) -> GateRule {
    GateRule::new(c, OpMatch::AnyList, KeyMatch::Exact(prefix.into()))
}

/// Both contenders pass the pre-check and PUT before either LISTs: each
/// sees the other's key, so both back off.
#[test]
fn simultaneous_acquires_both_back_off() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    let pa = sim.gate(GateRule::new(a, OpMatch::AnyWrite, locks()));
    let la = sim.gate(list_of(a, "locks/").nth(2));
    let da = sim.gate(GateRule::new(a, OpMatch::AnyDelete, locks()));
    let pb = sim.gate(GateRule::new(b, OpMatch::AnyWrite, locks()));
    let lb = sim.gate(list_of(b, "locks/").nth(2));

    let sa = sim.spawn(a, hold());
    sim.expect_paused(pa, "a's lock PUT");
    let sb = sim.spawn(b, hold());
    sim.expect_paused(pb, "b's lock PUT");
    sim.resume(pa);
    sim.expect_paused(la, "a's confirming LIST");
    sim.resume(pb);
    sim.expect_paused(lb, "b's confirming LIST");
    sim.resume(la);
    sim.expect_paused(da, "a backs off");
    sim.resume(lb);
    assert_allowed(&sim.join(sb), AllowedError::Locked);
    sim.resume(da);
    assert_allowed(&sim.join(sa), AllowedError::Locked);

    assert_eq!(lock_writes(&sim), 2);
    assert!(sim.store().keys("locks/").is_empty());
}

/// A foreign key with a later timestamp (a clock ahead of ours) appears
/// after our pre-check but before our LIST. We must lose to it.
#[test]
fn late_key_after_precheck_blocks() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.set_skew(b, chrono::Duration::minutes(1));
    let pa = sim.gate(GateRule::new(a, OpMatch::AnyWrite, locks()));
    let db = sim.gate(GateRule::new(b, OpMatch::AnyDelete, locks()));

    let sa = sim.spawn(a, hold());
    sim.expect_paused(pa, "a's lock PUT");
    let sb = sim.spawn(b, hold());
    sim.expect_paused(db, "b's release");
    let b_key = sim
        .store()
        .keys_written_by(b, "locks/")
        .pop()
        .expect("b holds the lock");
    sim.resume(pa);
    let ra = sim.join(sa);
    assert_allowed(&ra, AllowedError::Locked);
    // The Locked display names the holder by host and PID.
    let holder = format!("{} (PID {})", sim.host(b).hostname, sim.host(b).pid);
    assert!(ra.detail().contains(&holder), "{}", ra.detail());
    let a_key = sim
        .store()
        .keys_written_by(a, "locks/")
        .pop()
        .expect("a wrote its key");
    assert!(
        b_key > a_key,
        "b's key {b_key} is not the later one ({a_key})"
    );
    // The loser removed its own key and left the holder's.
    assert_eq!(sim.store().keys("locks/"), vec![b_key.clone()]);
    sim.resume(db);
    sim.join(sb).expect_ok();
    assert!(sim.store().keys("locks/").is_empty());
}

/// The session marker is written, then the backup is held before its
/// probe LIST. Maintenance must see the marker: since 04 step 7 it runs
/// beside the session (it reads the marker for its horizon and keeps it);
/// then the probe finds no lock and the backup commits.
#[test]
fn session_registered_first_is_seen_by_maintenance() {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let g = sim.gate(list_of(a, "locks/"));
    let sa = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: tree("a1"),
        },
    );
    sim.expect_paused(g, "a's registration probe");
    let marker = sim
        .store()
        .keys_written_by(a, "sessions/")
        .into_iter()
        .find(|k| k.ends_with(".json"))
        .expect("a's marker");
    let rb = sim.compact(b);
    rb.expect_ok();
    let read = sim.store().log().iter().any(|r| {
        r.client == b && r.run == Some(rb.index) && r.op == OpKind::Get && r.key == marker
    });
    assert!(read, "b's compact did not read a's marker");
    assert!(sim.store().object(&marker).is_some());
    assert!(sim.store().keys("locks/").is_empty());
    sim.resume(g);
    sim.join(sa).expect_ok();
}

/// Maintenance holds the lock and is held before listing sessions. A
/// backup registering now must see the lock and back out.
#[test]
fn maintenance_lock_first_blocks_session() {
    let mut sim = Sim::new();
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let g = sim.gate(list_of(a, "sessions/"));
    let sa = sim.spawn(a, Call::Compact);
    sim.expect_paused(g, "a's session cleanup");
    assert_allowed(&sim.backup(b, "b1", &tree("b1")), AllowedError::Locked);
    let b_markers: Vec<String> = sim
        .store()
        .keys_written_by(b, "sessions/")
        .into_iter()
        .filter(|k| k.ends_with(".json") && sim.store().object(k).is_some())
        .collect();
    assert!(b_markers.is_empty(), "{b_markers:?}");
    sim.resume(g);
    sim.join(sa).expect_ok();
    assert!(sim.store().keys("locks/").is_empty());
}
