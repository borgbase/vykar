//! Exclusive sessions (10-single-protocol step 8) on the simulator: each
//! client runs on its own thread, held and resumed at gates (08 step 3).
//! An exclusive session's registration (marker PUT, then one listing of
//! `sessions/`) excludes every other session in both orders: two exclusive
//! registrations never both pass, a live exclusive session refuses every
//! shared one (`Locked`) and every live session refuses an exclusive one
//! (`ActiveSessions`); a backup whose marker vanished stops before its head
//! at its pre-head listing; an exclusive run whose marker is lost writes
//! nothing more; a crashed exclusive session stops blocking after 45 min, at
//! once on its own machine, or after `break-lock`. The
//! interleavings of the advisory lock these tests covered before step 8 are
//! the lock's own, deleted with it in step 9.

use crate::config::RetentionConfig;
use crate::sim::history::{AllowedError, ErrorClass};
use crate::sim::{
    Call, ClientId, FaultAction, FaultRule, GateId, GateRule, Invariant, KeyMatch, OpKind, OpMatch,
    OpRecord, RunRecord, Sim, SimConfig, SourceTree, Spawned, StoreKind,
};

const BOTH_KINDS: [StoreKind; 2] = [StoreKind::Plain, StoreKind::Conditional];

fn sim_on(kind: StoreKind) -> Sim {
    Sim::with_config(SimConfig {
        kind,
        ..SimConfig::default()
    })
}

fn tree(seed: &str) -> SourceTree {
    SourceTree::seeded(seed, 2, 16 * 1024)
}

fn backup_call(name: &str) -> Call {
    Call::Backup {
        snapshot: name.into(),
        tree: tree(name),
    }
}

/// The key `ExclusiveCustom`'s held body probes: a gate on its EXISTS holds
/// the run inside its exclusive session.
const INSIDE: &str = "snapshots/gate";

/// An exclusive session whose body checks [`INSIDE`] (a read: no write).
fn exclusive_hold() -> Call {
    Call::ExclusiveCustom {
        label: "hold".into(),
        f: Box::new(|w| w.storage().exists(INSIDE).map(|_| ())),
    }
}

/// `c` inside its exclusive session (at the body's EXISTS).
fn inside(c: ClientId) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::Exists),
        KeyMatch::Exact(INSIDE.into()),
    )
}

/// `c`'s `n`th listing of `sessions/` with mtimes.
fn sessions_listing(c: ClientId, n: u32) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::ListMeta),
        KeyMatch::Exact("sessions/".into()),
    )
    .nth(n)
}

fn exclusive_prefix() -> KeyMatch {
    KeyMatch::Prefix("sessions/x-".into())
}

fn exclusive_keys(sim: &Sim) -> Vec<String> {
    sim.store()
        .keys("sessions/x-")
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .collect()
}

fn records(sim: &Sim, c: ClientId, run: usize) -> Vec<OpRecord> {
    sim.store()
        .log()
        .into_iter()
        .filter(|r| r.client == c && r.run == Some(run))
        .collect()
}

fn assert_allowed(run: &RunRecord, e: AllowedError) {
    assert_eq!(
        run.error_class(),
        Some(&ErrorClass::Allowed(e)),
        "{}",
        run.detail()
    );
}

/// `c` wrote nothing but its session's own keys in `run`.
fn wrote_only_sessions(sim: &Sim, c: ClientId, run: usize) -> bool {
    records(sim, c, run)
        .iter()
        .all(|r| !r.op.is_mutation() || r.key.starts_with("sessions/"))
}

/// (a) Both PUT their marker before either lists: each sees the other's
/// key, so both refuse `ActiveSessions` and delete their marker.
#[test]
fn two_exclusive_registrations_both_refuse() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    let la = sim.gate(sessions_listing(a, 1));
    let da = sim.gate(GateRule::new(a, OpMatch::AnyDelete, exclusive_prefix()));
    let lb = sim.gate(sessions_listing(b, 1));
    let sa = sim.spawn(a, exclusive_hold());
    sim.expect_paused(la, "a's registration listing");
    assert_eq!(exclusive_keys(&sim).len(), 1);
    let sb = sim.spawn(b, exclusive_hold());
    sim.expect_paused(lb, "b's registration listing");
    assert_eq!(exclusive_keys(&sim).len(), 2);
    sim.resume(la);
    sim.expect_paused(da, "a's refusal deletes its marker");
    sim.resume(lb);
    let rb = sim.join(sb);
    sim.resume(da);
    let ra = sim.join(sa);
    assert_allowed(&ra, AllowedError::ActiveSessions);
    assert_allowed(&rb, AllowedError::ActiveSessions);
    assert!(exclusive_keys(&sim).is_empty());
    assert!(wrote_only_sessions(&sim, a, ra.index));
    assert!(wrote_only_sessions(&sim, b, rb.index));
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// (a') A's listing completes before b's PUT: a runs, and b's listing, after
/// its PUT, sees a's live marker and refuses.
#[test]
fn an_exclusive_listing_before_the_others_put_runs() {
    let mut sim = Sim::new();
    let (a, b) = (sim.client("a"), sim.client("b"));
    let ga = sim.gate(inside(a));
    let pb = sim.gate(GateRule::new(
        b,
        OpMatch::Kind(OpKind::Put),
        exclusive_prefix(),
    ));
    let sa = sim.spawn(a, exclusive_hold());
    sim.expect_paused(ga, "a inside its session");
    let sb = sim.spawn(b, exclusive_hold());
    sim.expect_paused(pb, "b's marker PUT");
    sim.resume(pb);
    let rb = sim.join(sb);
    assert_allowed(&rb, AllowedError::ActiveSessions);
    sim.resume(ga);
    sim.join(sa).expect_ok();
    assert!(exclusive_keys(&sim).is_empty());
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// (b) While an exclusive session is live, a backup, compact, prune and
/// delete each refuse `Locked` naming it (its host and PID), leaving no
/// marker; once it ended they run.
#[test]
fn a_live_exclusive_session_refuses_backup_compact_prune_and_delete() {
    for kind in BOTH_KINDS {
        let mut sim = sim_on(kind);
        let (a, b) = (sim.client("a"), sim.client("b"));
        sim.backup(b, "b0", &tree("b0")).expect_ok();
        sim.backup(b, "b1", &tree("b1")).expect_ok();
        let ga = sim.gate(inside(a));
        let sa = sim.spawn(a, exclusive_hold());
        sim.expect_paused(ga, "a inside its session");
        let pid = sim.host(a).pid;
        let refused = [
            sim.backup(b, "b2", &tree("b2")),
            sim.compact(b),
            sim.prune(
                b,
                RetentionConfig {
                    keep_last: Some(1),
                    ..Default::default()
                },
            ),
            sim.delete(b, &["b0"]),
        ];
        for r in &refused {
            assert_allowed(r, AllowedError::Locked);
            assert!(
                r.detail().contains("exclusive maintenance run")
                    && r.detail().contains(&format!("PID {pid}")),
                "{kind:?}: {}",
                r.detail()
            );
            assert!(wrote_only_sessions(&sim, b, r.index), "{kind:?}");
        }
        let b_markers: Vec<String> = sim
            .store()
            .keys("sessions/")
            .into_iter()
            .filter(|k| k.ends_with(".json") && !k.starts_with("sessions/x-"))
            .collect();
        assert!(b_markers.is_empty(), "{kind:?}: {b_markers:?}");
        sim.resume(ga);
        sim.join(sa).expect_ok();
        sim.backup(b, "b3", &tree("b3")).expect_ok();
        sim.compact(b).expect_ok();
        sim.check_invariants(Invariant::STEP1).assert_clean();
    }
}

/// (c) A live shared session (a backup mid-upload, a compact after its
/// registration) refuses `check --repair`, a mode switch and an exclusive
/// delete with `ActiveSessions` naming it.
#[test]
fn a_live_shared_session_refuses_the_exclusive_ones() {
    for kind in BOTH_KINDS {
        let mut sim = sim_on(kind);
        let (a, b) = (sim.client("a"), sim.client("b"));
        sim.backup(a, "a0", &tree("a0")).expect_ok();
        let ga = sim.gate(GateRule::new(
            a,
            OpMatch::AnyWrite,
            KeyMatch::Prefix("packs/".into()),
        ));
        let sa = sim.spawn(a, backup_call("a1"));
        sim.expect_paused(ga, "a's first pack PUT");
        let a_session = sim
            .store()
            .keys_written_by(a, "sessions/")
            .into_iter()
            .rev()
            .find(|k| k.ends_with(".json"))
            .and_then(|k| {
                crate::repo::sessions::parse_session_marker_key(&k).map(|(id, _)| id.to_string())
            })
            .expect("a's marker");
        let mode = if kind.records_multi_writer() {
            crate::repo::WriterMode::Single
        } else {
            crate::repo::WriterMode::Multi
        };
        for r in [
            sim.check_repair(b),
            sim.call(b, Call::SwitchMode { mode }),
            sim.call(
                b,
                Call::ExclusiveDelete {
                    names: vec!["a0".into()],
                },
            ),
        ] {
            assert_allowed(&r, AllowedError::ActiveSessions);
            assert!(r.detail().contains(&a_session), "{kind:?}: {}", r.detail());
            assert!(wrote_only_sessions(&sim, b, r.index), "{kind:?}");
        }
        sim.resume(ga);
        sim.join(sa).expect_ok();

        // A compact after its registration (at its pack listing).
        let gc = sim.gate(GateRule::new(
            a,
            OpMatch::Kind(OpKind::ListMeta),
            KeyMatch::Prefix("packs/".into()),
        ));
        let sc = sim.spawn(a, Call::Compact);
        sim.expect_paused(gc, "a's compact at its pack listing");
        assert_allowed(&sim.check_repair(b), AllowedError::ActiveSessions);
        sim.resume(gc);
        sim.join(sc).expect_ok();
        sim.check_repair(b).expect_ok();
        sim.check_invariants(Invariant::STEP1).assert_clean();
    }
}

/// (d) A backup whose marker a misjudging reaper deleted (I3) while it
/// uploaded lets an exclusive session register; at its pre-head listing the
/// backup finds that session live (`Locked`) and waits: while the exclusive
/// marker exists it writes no index object, head or snapshot. Once the
/// session ended the backup ends Ok (the suspend rule) or with an allowed
/// error. Invariant 7 does not judge the backup (a shared run): this test's
/// assertions are what sees a shared write under an exclusive hold until
/// step 10.
#[test]
fn an_exclusive_after_a_backups_registration_stops_its_head_write() {
    for kind in BOTH_KINDS {
        let mut sim = sim_on(kind);
        let (a, c) = (sim.client("a"), sim.client("c"));
        sim.backup(a, "a0", &tree("a0")).expect_ok();
        let ga = sim.gate(GateRule::new(
            a,
            OpMatch::AnyWrite,
            KeyMatch::Prefix("packs/".into()),
        ));
        let sa = sim.spawn(a, backup_call("a1"));
        sim.expect_paused(ga, "a's first pack PUT");
        for key in sim.store().keys_written_by(a, "sessions/") {
            if key.ends_with(".json") && sim.store().object(&key).is_some() {
                sim.tamper_delete(&key);
            }
        }
        let gc = sim.gate(inside(c));
        let sc = sim.spawn(c, exclusive_hold());
        sim.expect_paused(gc, "c inside its exclusive session");
        let c_key = exclusive_keys(&sim).pop().expect("c's marker");
        // a's retry of its pre-head listing: held there, after one Locked.
        let retry = if kind.records_multi_writer() { 4 } else { 6 };
        let ar = sim.gate(sessions_listing(a, retry));
        sim.resume(ga);
        sim.expect_paused(ar, "a's retry of its pre-head listing");
        let protected = |r: &OpRecord| {
            r.op.is_write()
                && ["index.h/", "index.d/", "snapshots/"]
                    .iter()
                    .any(|p| r.key.starts_with(p))
        };
        let a_ops: Vec<String> = sim
            .store()
            .log()
            .iter()
            .filter(|r| r.client == a && r.run == Some(sa.index()))
            .map(|r| format!("{} {:?} {} {:?}", r.step, r.op, r.key, r.label))
            .collect();
        assert!(
            !sim.store()
                .log()
                .iter()
                .any(|r| r.client == a && r.run == Some(sa.index()) && protected(r)),
            "{kind:?}: a wrote while c's exclusive session was live: {a_ops:#?}"
        );
        assert!(sim.store().object(&c_key).is_some());
        sim.resume(gc);
        sim.join(sc).expect_ok();
        sim.resume(ar);
        let ra = sim.join(sa);
        assert!(
            ra.is_ok() || matches!(ra.error_class(), Some(ErrorClass::Allowed(_))),
            "{kind:?}: {}",
            ra.detail()
        );
        let set = if kind.records_multi_writer() {
            Invariant::STEP8
        } else {
            Invariant::STEP7
        };
        sim.check_invariants(set).assert_clean();
    }
}

/// A repository on which `check --repair` deletes a dangling snapshot and
/// writes the index: two backups, then a pack of the second snapshot
/// deleted (tamper). (A deleted key copy is restored by the open's key
/// backfill, before the repair registers, so it is not one of the repair's
/// writes.)
fn damaged(sim: &mut Sim, c: ClientId) {
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let before = sim.store().keys("packs/");
    sim.backup(c, "t1", &tree("t1")).expect_ok();
    let lost = sim
        .store()
        .keys("packs/")
        .into_iter()
        .find(|k| !before.contains(k))
        .expect("t1 wrote a pack");
    sim.tamper_delete(&lost);
}

/// A repository on which `check --repair` rewrites a snapshot item by
/// item (A1: the write session's sites): t0, then t1 with t0's files and
/// one more, whose data pack is deleted (tamper). The repair drops that
/// file from t1, uploads t1's rewritten tree in a new pack, writes the
/// index, PUTs the new snapshot, deletes the old one and writes the index
/// again. (It writes no pack map: `Call::CheckRepair` does not verify
/// data, and only a damaged trailer or chunk, which that finds, or an index
/// rebuilt from local tables plans one.)
fn item_damaged(sim: &mut Sim, c: ClientId) {
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let before = sim.store().keys("packs/");
    let t1 = tree("t0").file(
        "new.bin",
        crate::sim::runner::seeded_bytes("t1/new", 16 * 1024),
    );
    sim.backup(c, "t1", &t1).expect_ok();
    let mut tree_packs = Vec::new();
    sim.custom(c, "tree packs", |w| {
        w.load_chunk_index_uncached()?;
        let meta = crate::commands::list::load_snapshot_meta(w, "t1")?;
        for id in &meta.item_ptrs {
            if let Some(e) = w.chunk_index().get(id) {
                tree_packs.push(e.pack_id.storage_key());
            }
        }
        Ok(())
    })
    .expect_ok();
    let lost = sim
        .store()
        .keys("packs/")
        .into_iter()
        .find(|k| !before.contains(k) && !tree_packs.contains(k))
        .expect("t1 wrote a data pack");
    sim.tamper_delete(&lost);
}

/// `c`'s mutations outside `sessions/` in `run` after its exclusive
/// registration (its marker, refreshes and journal excluded, and the open's
/// writes before the registration).
fn data_mutations(sim: &Sim, c: ClientId, run: usize) -> Vec<OpRecord> {
    let recs = records(sim, c, run);
    let registered = recs
        .iter()
        .position(|r| r.op == OpKind::Put && r.key.starts_with("sessions/x-"))
        .unwrap_or(recs.len());
    recs.into_iter()
        .skip(registered)
        .filter(|r| r.op.is_mutation() && !r.key.starts_with("sessions/"))
        .collect()
}

/// `c`'s records in `run` after `held` (the first record of `held`'s op
/// and key) that are applied mutations other than of its exclusive
/// session's marker keys (refreshes, deregistration): its journal and
/// every data write.
fn written_after(sim: &Sim, c: ClientId, run: usize, held: &OpRecord) -> Vec<OpRecord> {
    let recs = records(sim, c, run);
    let at = recs
        .iter()
        .position(|r| r.key == held.key && r.op == held.op)
        .unwrap_or_else(|| panic!("the held op {held:?} not logged"));
    recs.into_iter()
        .skip(at + 1)
        .filter(|r| {
            r.op.is_mutation()
                && r.applied()
                && !(r.key.starts_with("sessions/x-") && r.key.ends_with(".json"))
        })
        .collect()
}

/// The `packs/` keys the index names, as `c` reads it (in an exclusive
/// session that writes nothing).
fn indexed_packs(sim: &mut Sim, c: ClientId) -> std::collections::BTreeSet<String> {
    let mut packs = std::collections::BTreeSet::new();
    sim.custom(c, "indexed packs", |w| {
        w.load_chunk_index_uncached()?;
        packs.extend(w.chunk_index().iter().map(|(_, e)| e.pack_id.storage_key()));
        Ok(())
    })
    .expect_ok();
    packs
}

/// The snapshot names of `c`'s open.
fn snapshot_names(sim: &mut Sim, c: ClientId) -> Vec<String> {
    let mut names = Vec::new();
    sim.custom(c, "names", |w| {
        names.extend(w.manifest().snapshots.iter().map(|s| s.name.clone()));
        Ok(())
    })
    .expect_ok();
    names
}

/// What a rerun after case (e) on [`damaged`] is judged by: STEP1 except
/// invariants 2 and 3, which flag t1, lost to the fixture's tamper and
/// removed by the repair as asked. Not invariant 7 either: the aborted
/// run's held write lands after its marker's removal (the write past its
/// check, the residual by design), which it reads as `WriteWithoutLock`.
const AFTER_REPAIR: &[Invariant] = &[
    Invariant::CommittedSnapshotsRestore,
    Invariant::IndexIntegrity,
    Invariant::AllowedErrors,
];

/// [`AFTER_REPAIR`] on [`item_damaged`], without invariant 1 too: t1 is
/// rewritten without the file whose pack was lost, which it reads as a
/// restore that differs. Every snapshot's restore is still asserted Ok.
const AFTER_ITEM_REPAIR: &[Invariant] = &[Invariant::IndexIntegrity, Invariant::AllowedErrors];

/// Case (e) on the repository `fixture` builds: a repair whose marker is
/// removed while one of its writes is held past its check, at every write
/// of the repair outside `sessions/` in turn. The held write lands; the
/// run writes nothing more (no data write, no journal: the write
/// session's `flush_on_abort` is refused too) and ends
/// `MarkerLostDuringCommit` when a write followed (never a wrapped,
/// disallowed error: A1, R5), Ok after the last one, with nothing left to
/// check. Then (A3) a rerun of the repair converges, clean under
/// `after_rerun`, and the packs the aborted run uploaded are orphans that a
/// compact after `T_PACK` collects, or packs the index it wrote names;
/// every snapshot restores. Returns the probed writes.
fn marker_lost_at_every_write(
    fixture: fn(&mut Sim, ClientId),
    after_rerun: &[Invariant],
) -> Vec<OpRecord> {
    let probe = {
        let mut sim = Sim::new();
        let c = sim.client("c");
        fixture(&mut sim, c);
        let r = sim.check_repair(c);
        r.expect_ok();
        data_mutations(&sim, c, r.index)
    };
    // The gate counts the open's writes too.
    let open_writes = {
        let mut sim = Sim::new();
        let c = sim.client("c");
        fixture(&mut sim, c);
        let r = sim.check_repair(c);
        records(&sim, c, r.index)
            .iter()
            .filter(|m| m.op.is_mutation() && !m.key.starts_with("sessions/"))
            .count()
            - data_mutations(&sim, c, r.index).len()
    };
    let mut cases = Vec::new();
    for n in 1..=(open_writes + probe.len()) as u32 {
        let mut sim = Sim::new();
        let c = sim.client("c");
        fixture(&mut sim, c);
        let gate: GateId = sim.gate(
            GateRule::new(c, OpMatch::AnyMutation, KeyMatch::Any)
                .filter(|_, k| !k.starts_with("sessions/"))
                .not_on_thread(crate::repo::sessions::HEARTBEAT_THREAD_NAME)
                .nth(n),
        );
        let run: Spawned = sim.spawn(c, Call::CheckRepair);
        let held = sim.expect_paused(gate, "the repair's write");
        if exclusive_keys(&sim).is_empty() {
            // A write of the open, before the registration: not a case.
            sim.resume(gate);
            sim.join(run).expect_ok();
            continue;
        }
        for key in exclusive_keys(&sim) {
            sim.tamper_delete(&key);
        }
        sim.advance(std::time::Duration::from_secs(6));
        sim.resume(gate);
        let r = sim.join(run);
        let at = format!("n={n} at {} {:?}", held.key, held.op);
        let mutations = data_mutations(&sim, c, r.index);
        let landed = mutations
            .iter()
            .find(|m| m.key == held.key && m.op == held.op)
            .unwrap_or_else(|| panic!("{at}: the held op not logged"))
            .clone();
        assert!(landed.applied(), "{at}: the held op landed");
        let after = written_after(&sim, c, r.index, &landed);
        assert!(after.is_empty(), "{at}: written after the loss: {after:?}");
        let last = probe
            .last()
            .is_some_and(|p| p.key == held.key && p.op == held.op);
        if last {
            assert!(r.is_ok(), "{at}: {}", r.detail());
        } else {
            assert_eq!(
                r.error_class(),
                Some(&ErrorClass::Allowed(AllowedError::MarkerLostDuringCommit)),
                "{at}: {}",
                r.detail()
            );
            let tried: Vec<&OpRecord> = mutations
                .iter()
                .skip_while(|m| !(m.key == held.key && m.op == held.op))
                .skip(1)
                .collect();
            assert!(
                tried.is_empty(),
                "{at}: a refused write was attempted past the check: {tried:?}"
            );
        }
        sim.check_invariants(&[Invariant::AllowedErrors])
            .assert_clean();

        // Finding (not converged, reported): the rewritten snapshot's PUT
        // landed and the old blob's DELETE was refused, so two blobs name
        // t1 (a crash between the two leaves the same). Repair looks
        // snapshots up by name, so the rerun depends on which of the two
        // the listing puts first: its sweep can abort ("cannot load snapshot
        // 't1' while sweeping the index"). Both blobs are asserted to stay;
        // the rerun is not judged here.
        if landed.op.is_write() && landed.key.starts_with("snapshots/") {
            // The old blob's DELETE was not even attempted (`tried` above).
            assert!(sim.store().object(&landed.key).is_some(), "{at}");
            cases.push(landed);
            continue;
        }

        // A3: a rerun converges; what the aborted run uploaded is an orphan
        // for a later compact, or named by the index it wrote.
        let uploaded: Vec<String> = mutations
            .iter()
            .filter(|m| m.applied() && m.op.is_write() && m.key.starts_with("packs/"))
            .map(|m| m.key.clone())
            .collect();
        let rerun = sim.check_repair(c);
        assert!(rerun.is_ok(), "{at}: rerun: {}", rerun.detail());
        sim.check_invariants(after_rerun).assert_clean();
        sim.advance(crate::repo::ages::T_PACK + std::time::Duration::from_secs(60));
        sim.compact(c).expect_ok();
        let indexed = indexed_packs(&mut sim, c);
        for key in &uploaded {
            assert!(
                sim.store().object(key).is_none() || indexed.contains(key),
                "{at}: {key}, uploaded by the aborted run, neither collected nor indexed"
            );
        }
        for name in snapshot_names(&mut sim, c) {
            sim.restore(c, &name).expect_ok();
        }
        sim.check_invariants(after_rerun).assert_clean();
        cases.push(landed);
    }
    assert_eq!(
        cases.len(),
        probe.len(),
        "every write of the repair was a case"
    );
    cases
}

/// (e) A repair that deletes a dangling snapshot and writes the index
/// ([`damaged`]), its marker lost at every write in turn
/// ([`marker_lost_at_every_write`], Gate 1, A1, A3).
#[test]
fn a_repair_whose_marker_is_lost_writes_nothing_more() {
    let probed = marker_lost_at_every_write(damaged, AFTER_REPAIR);
    assert!(
        probed
            .iter()
            .any(|r| r.op.is_delete() && r.key.starts_with("snapshots/")),
        "the repair deletes a snapshot: {probed:?}"
    );
    assert!(
        probed.iter().any(|r| r.key.starts_with("index.h/")),
        "the repair writes a head: {probed:?}"
    );
}

/// (e) on a repair that rewrites a snapshot item by item
/// ([`item_damaged`]): the probed sites include its write session's pack
/// upload (the sites ahead of `ws.upload_failed()`, its `save_session`
/// and `flush_on_abort`), the new snapshot's PUT and the old one's
/// DELETE.
#[test]
fn an_item_level_repair_whose_marker_is_lost_writes_nothing_more() {
    let probed = marker_lost_at_every_write(item_damaged, AFTER_ITEM_REPAIR);
    let has = |what: &str, f: &dyn Fn(&OpRecord) -> bool| {
        assert!(probed.iter().any(f), "the repair {what}: {probed:?}");
    };
    has("uploads a pack", &|r| {
        r.op.is_write() && r.key.starts_with("packs/")
    });
    has("PUTs a snapshot", &|r| {
        r.op.is_write() && r.key.starts_with("snapshots/")
    });
    has("deletes a snapshot", &|r| {
        r.op.is_delete() && r.key.starts_with("snapshots/")
    });
    has("writes a head", &|r| r.key.starts_with("index.h/"));
}

/// `c` runs an exclusive session and crashes deregistering it: its marker
/// stays.
fn crashed_exclusive(sim: &mut Sim, c: ClientId) -> Vec<String> {
    sim.fault(FaultRule::new(OpMatch::AnyDelete, exclusive_prefix(), FaultAction::Crash).client(c));
    // The crash hits the deregistration, after the action: the run's
    // result is the action's (a failed deregistration is warned only).
    sim.call(c, exclusive_hold());
    assert!(sim.store().is_crashed(c));
    let keys = exclusive_keys(sim);
    assert_eq!(keys.len(), 1);
    keys
}

/// (f1) A crashed exclusive session blocks a backup on another machine
/// until its marker is 45 min old; the backup then reaps it.
#[test]
fn a_crashed_exclusive_stops_blocking_after_45_minutes() {
    let mut sim = Sim::new();
    let (c, b) = (sim.client("c"), sim.client("b"));
    sim.backup(b, "b0", &tree("b0")).expect_ok();
    let keys = crashed_exclusive(&mut sim, c);
    assert_allowed(&sim.backup(b, "b1", &tree("b1")), AllowedError::Locked);
    sim.advance(std::time::Duration::from_secs(44 * 60));
    assert_allowed(&sim.backup(b, "b2", &tree("b2")), AllowedError::Locked);
    sim.advance(std::time::Duration::from_secs(2 * 60));
    let r = sim.backup(b, "b3", &tree("b3"));
    r.expect_ok();
    for key in &keys {
        assert!(
            records(&sim, b, r.index)
                .iter()
                .any(|o| o.op.is_delete() && &o.key == key),
            "b reaped {key}"
        );
    }
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

// (f2), a crashed exclusive session reaped at once by a process on its own
// machine (same hostname, installation and boot; its PID dead), is
// `tests::sim::same_machine_reaps_dead_exclusive_holder_at_once`.

/// (f3) `break-lock` removes a crashed exclusive session's marker at once
/// (since 10-single-protocol step 9 it always clears session markers).
#[test]
fn a_crashed_exclusive_stops_blocking_after_break_lock() {
    let mut sim = Sim::new();
    let (c, b) = (sim.client("c"), sim.client("b"));
    sim.backup(b, "b0", &tree("b0")).expect_ok();
    crashed_exclusive(&mut sim, c);
    assert_allowed(&sim.backup(b, "b1", &tree("b1")), AllowedError::Locked);
    sim.break_lock(b).expect_ok();
    assert!(exclusive_keys(&sim).is_empty());
    sim.backup(b, "b2", &tree("b2")).expect_ok();
    sim.check_invariants(Invariant::STEP1).assert_clean();
}
