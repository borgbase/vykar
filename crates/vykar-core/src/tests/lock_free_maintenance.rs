//! Lock-free maintenance (04 step 9, phase F part 10d) on the simulator:
//! compact, prune and delete run without the repository lock on an
//! encrypted repository whose storage passed the put-if-absent check, beside
//! lock-free backups and beside each other. A compact skips its sweep beside
//! a committing backup, marks again when a backup committed during its mark
//! (incrementally) and gives the sweep up after three moved heads; a repack
//! batch rebases onto a backup's head, and drops a relocation another
//! compact made; the mark skips a snapshot deleted after its listing; a
//! backup that adds no chunk creates a head (Q2), also after a taken
//! generation, and that head conflicts with a concurrent sweep. Repair
//! stays apart from maintenance as an exclusive session (10-single-protocol
//! step 8; by the marker handshake before). Since
//! 10-single-protocol step 7 the tests whose second writer was a lock
//! client (a lock-mode backup or compact) run on a single-writer repository
//! instead: compacts refuse each other, a backup waits for a compact, and
//! the stalled-compact residuals with a single-writer backup. Each invariant that judges a lock-free run's deletes now that
//! invariant 7 exempts it flags a wrong delete (V3).

use crate::repo::sessions::MarkerFlags;
use crate::sim::history::{AllowedError, ErrorClass};
use crate::sim::{
    Call, ClientId, FaultAction, FaultRule, GateRule, Invariant, KeyMatch, OpKind, OpMatch,
    OpOutcome, OpRecord, Sim, SimConfig, SourceTree, StoreKind, ViolationKind,
};

const FILE_SIZE: usize = 16 * 1024;

/// T_pack and a minute: backup segments and orphan packs this old are
/// no longer young.
const PAST_T_PACK: std::time::Duration = std::time::Duration::from_secs(12 * 3600 + 60);

fn tree(name: &str) -> SourceTree {
    SourceTree::seeded(name, 2, FILE_SIZE)
}

fn backup_call(name: &str) -> Call {
    Call::Backup {
        snapshot: name.into(),
        tree: tree(name),
    }
}

/// An encrypted repository on a store of `kind`: maintenance runs
/// lock-free where the client's handle offers put-if-absent.
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

/// Head creates (put-if-absent of `index.h/`) among `recs`.
fn head_creates(recs: &[OpRecord]) -> Vec<&OpRecord> {
    recs.iter()
        .filter(|r| r.op == OpKind::PutIfAbsent && r.key.starts_with("index.h/"))
        .collect()
}

/// Writes under the legacy lock directory (`sessions::LEGACY_LOCKS_PREFIX`)
/// among `recs`, which no v6 client makes.
fn lock_writes(recs: &[OpRecord]) -> usize {
    recs.iter()
        .filter(|r| {
            r.op.is_write()
                && r.key
                    .starts_with(crate::repo::sessions::LEGACY_LOCKS_PREFIX)
        })
        .count()
}

/// `c`'s `n`th plain listing of `sessions/` (a lock-free compact's
/// sessions #2; its sessions #1 lists with mtimes).
fn second_session_listing(c: ClientId, n: u32) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::List),
        KeyMatch::Exact("sessions/".into()),
    )
    .nth(n)
}

/// `c`'s `n`th head create.
fn head_create(c: ClientId, n: u32) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::PutIfAbsent),
        KeyMatch::Prefix("index.h/".into()),
    )
    .nth(n)
}

/// `c`'s first PUT of a snapshot (a lock-free commit is between its head
/// create and its commit point there, its marker flagged).
fn snapshot_put(c: ClientId) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::Put),
        KeyMatch::Prefix("snapshots/".into()),
    )
}

/// `c`'s `n`th single-writer head write (10-single-protocol step 7): its
/// EXISTS of the generation, the first op of the head step.
fn plain_head_write(c: ClientId, n: u32) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::Exists),
        KeyMatch::Prefix("index.h/".into()),
    )
    .not_on_thread(crate::repo::sessions::HEARTBEAT_THREAD_NAME)
    .nth(n)
}

/// `c`'s single-writer listing of `sessions/` before its commit mark, its
/// `n`th attempt (the registration and the open list first,
/// 10-single-protocol step 8).
fn before_mark(c: ClientId, n: u32) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::ListMeta),
        KeyMatch::Exact("sessions/".into()),
    )
    .nth(n + 2)
}

/// `c`'s `n`th listing of `sessions/` with mtimes: for a compact the first
/// is its registration's (10-single-protocol step 8); then, on a
/// single-writer repository, its single-writer check; then its sessions #1.
fn sessions_listing(c: ClientId, n: u32) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::ListMeta),
        KeyMatch::Exact("sessions/".into()),
    )
    .nth(n)
}

fn expect_class(run: &crate::sim::RunRecord, class: AllowedError) {
    assert_eq!(
        run.error_class(),
        Some(&ErrorClass::Allowed(class)),
        "{}",
        run.detail()
    );
}

/// A repository with two snapshots, `old` deleted and both segments past
/// T_pack: the next compact sweeps `old`'s chunks and repacks or deletes
/// the packs that held them.
fn swept_and_aged(sim: &mut Sim, a: ClientId) {
    sim.backup(a, "old", &tree("old")).expect_ok();
    sim.backup(a, "keep", &tree("keep")).expect_ok();
    sim.delete(a, &["old"]).expect_ok();
    sim.advance(PAST_T_PACK);
}

/// Compact, prune and delete on an encrypted repository whose storage
/// passed the check take no lock: each registers a session marker (compact
/// with `writes_heads`, the others without), writes no lock key, and its
/// run is lock-free.
#[test]
fn maintenance_runs_without_the_lock() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    swept_and_aged(&mut sim, a);
    let rc = sim.compact(a);
    rc.expect_ok();
    let rd = sim.delete(a, &["keep"]);
    rd.expect_ok();
    let flags: Vec<(usize, bool)> = [&rc, &rd]
        .iter()
        .map(|run| {
            let recs = records(&sim, a, run.index);
            assert!(run.shared, "run {} is lock-free", run.index);
            assert_eq!(lock_writes(&recs), 0, "run {} wrote a lock key", run.index);
            let marker = sim
                .store()
                .log()
                .into_iter()
                .find(|r| {
                    r.run == Some(run.index)
                        && r.op == OpKind::Put
                        && r.key.starts_with("sessions/")
                        && r.key.ends_with(".json")
                })
                .expect("a registered marker");
            (run.index, marker.key.ends_with(".json"))
        })
        .collect();
    assert_eq!(flags.len(), 2);
    assert!(
        rc.detail().contains("unreferenced 4") || rc.detail().contains("unreferenced"),
        "{}",
        rc.detail()
    );
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// 3.4's step 4: a lock-free compact whose second session listing finds a
/// backup committing (gated between its head create and its snapshot PUT)
/// removes nothing this run (`sweep_skipped: Committing`) and goes on; the
/// backup commits and every snapshot restores.
#[test]
fn compact_skips_the_sweep_beside_a_committing_backup() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    swept_and_aged(&mut sim, a);
    let b_snap = sim.gate(snapshot_put(b));
    let rb = sim.spawn(b, backup_call("b1"));
    sim.expect_paused(b_snap, "b between its head create and its snapshot PUT");
    let rc = sim.compact(a);
    rc.expect_ok();
    assert!(rc.shared);
    assert!(
        rc.detail().contains("sweep-skipped Some(Committing)"),
        "{}",
        rc.detail()
    );
    sim.resume(b_snap);
    sim.join(rb).expect_ok();
    for name in ["keep", "b1"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
    // The next compact sweeps.
    let again = sim.compact(a);
    again.expect_ok();
    assert!(
        again.detail().contains("sweep-skipped None"),
        "{}",
        again.detail()
    );
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// U1: a backup that commits whole while a lock-free compact is held
/// before its second session listing moves the head: compact's snapshot
/// listing shows the backup's snapshot, the head read after it differs from
/// the loaded one, and compact marks again on the latest head (a second
/// sessions #2), not "compact refused"; it then sweeps, and the backup's
/// chunks stay.
#[test]
fn compact_re_marks_when_a_backup_commits_during_its_mark() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    swept_and_aged(&mut sim, a);
    let held = sim.gate(second_session_listing(a, 1));
    let rc = sim.spawn(a, Call::Compact);
    sim.expect_paused(held, "compact before its second session listing");
    let rb = sim.backup(b, "b1", &tree("b1"));
    rb.expect_ok();
    assert!(rb.shared);
    sim.resume(held);
    let rc = sim.join(rc);
    rc.expect_ok();
    assert!(
        rc.detail().contains("sweep-skipped None"),
        "{}",
        rc.detail()
    );
    let listings = records(&sim, a, rc.index)
        .iter()
        .filter(|r| r.op == OpKind::List && r.key == "sessions/")
        .count();
    assert_eq!(listings, 2, "compact marked again: {}", rc.detail());
    for name in ["keep", "b1"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// Q4/U8's bound: when a backup commits during each of
/// `COMPACT_MARK_ATTEMPTS` marks, the compact removes nothing this run
/// (`sweep_skipped: Contention`), warns, and goes on; nothing is damaged.
#[test]
fn compact_skips_the_sweep_after_three_moved_heads() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    swept_and_aged(&mut sim, a);
    let gates: Vec<_> = (1..=crate::commands::compact::COMPACT_MARK_ATTEMPTS)
        .map(|n| sim.gate(second_session_listing(a, n)))
        .collect();
    let rc = sim.spawn(a, Call::Compact);
    for (n, gate) in gates.iter().enumerate() {
        sim.expect_paused(*gate, "compact before a second session listing");
        let name = format!("b{n}");
        sim.backup(b, &name, &tree(&name)).expect_ok();
        sim.resume(*gate);
    }
    let rc = sim.join(rc);
    rc.expect_ok();
    assert!(
        rc.detail().contains("sweep-skipped Some(Contention)"),
        "{}",
        rc.detail()
    );
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// 3.6: a backup that creates the head a lock-free compact's repack batch
/// was about to create makes the batch's create taken; the batch rebases
/// its relocations onto the backup's head, creates the next generation and
/// deletes its sources. Every snapshot restores, the backup's included.
#[test]
fn a_repack_batch_rebases_onto_a_backup_head() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    // Shared packs: the deleted snapshot's chunks share packs with the kept
    // one's, so the sweep leaves dead bytes to repack.
    let shared = |name: &str| {
        SourceTree::seeded(name, 1, FILE_SIZE).file("shared.bin", vec![7u8; FILE_SIZE])
    };
    sim.backup(a, "old", &shared("old")).expect_ok();
    sim.backup(a, "keep", &shared("keep")).expect_ok();
    sim.delete(a, &["old"]).expect_ok();
    sim.advance(PAST_T_PACK);
    // The sweep's head is the first create; the repack batch's the second.
    let batch = sim.gate(head_create(a, 2));
    let rc = sim.spawn(a, Call::Compact);
    sim.expect_paused(batch, "the repack batch's head create");
    let rb = sim.backup(b, "b1", &tree("b1"));
    rb.expect_ok();
    sim.resume(batch);
    let rc = sim.join(rc);
    rc.expect_ok();
    let creates = records(&sim, a, rc.index);
    let creates = head_creates(&creates);
    assert!(
        creates.iter().any(|r| r.outcome == OpOutcome::Exists),
        "the batch's create was taken: {creates:?}"
    );
    assert!(
        creates.last().is_some_and(|r| r.outcome == OpOutcome::Ok),
        "the rebased batch created a head: {creates:?}"
    );
    assert!(!rc.detail().contains("repacked 0"), "{}", rc.detail());
    for name in ["keep", "b1"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// Two lock-free compacts repack one pack: the first is held at its repack
/// batch's head create after it wrote its new pack; the second runs whole
/// (its sweep finds nothing left, it repacks the same source and deletes
/// it). The first's create is then taken, its rebase finds the chunks in
/// the second's pack, not in its source, and drops every relocation; it
/// deletes nothing a head names, counts no pack repacked and reports its
/// output abandoned (D3 of 10d's measurement). Nothing is damaged.
#[test]
fn a_rebase_drops_a_relocation_another_compact_made() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, c) = (sim.client("a"), sim.client("c"));
    let shared = |name: &str| {
        SourceTree::seeded(name, 1, FILE_SIZE).file("shared.bin", vec![9u8; FILE_SIZE])
    };
    sim.backup(a, "old", &shared("old")).expect_ok();
    sim.backup(a, "keep", &shared("keep")).expect_ok();
    sim.delete(a, &["old"]).expect_ok();
    sim.advance(PAST_T_PACK);
    let batch = sim.gate(head_create(a, 2));
    let r1 = sim.spawn(a, Call::Compact);
    sim.expect_paused(batch, "the first compact's repack batch create");
    let r2 = sim.compact(c);
    r2.expect_ok();
    assert!(!r2.detail().contains("repacked 0"), "{}", r2.detail());
    sim.resume(batch);
    let r1 = sim.join(r1);
    r1.expect_ok();
    // Its result is truthful: nothing repacked, its output abandoned.
    assert!(r1.detail().contains("repacked 0 "), "{}", r1.detail());
    assert!(!r1.detail().contains("abandoned 0"), "{}", r1.detail());
    assert!(r2.detail().contains("abandoned 0"), "{}", r2.detail());
    let recs = records(&sim, a, r1.index);
    assert!(
        head_creates(&recs)
            .iter()
            .any(|r| r.outcome == OpOutcome::Exists),
        "the first compact's batch create was taken"
    );
    sim.restore(a, "keep").expect_ok();
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// W4: a snapshot deleted (lock-free) after a lock-free compact listed
/// `snapshots/` and before its GET is skipped: a second listing taken
/// after the GETs no longer shows it. The compact completes and nothing is
/// damaged.
#[test]
fn delete_during_the_mark_is_skipped() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, d) = (sim.client("a"), sim.client("d"));
    sim.backup(a, "a1", &tree("a1")).expect_ok();
    sim.backup(a, "a2", &tree("a2")).expect_ok();
    let first_get = sim.gate(GateRule::new(
        a,
        OpMatch::Kind(OpKind::Get),
        KeyMatch::Prefix("snapshots/".into()),
    ));
    let rc = sim.spawn(a, Call::Compact);
    let held = sim.expect_paused(first_get, "compact's first snapshot GET");
    // Delete the snapshot whose GET is held.
    let held_name = sim
        .history()
        .runs
        .iter()
        .find(|r| {
            records(&sim, r.client, r.index)
                .iter()
                .any(|x| x.op == OpKind::Put && x.key == held.key)
        })
        .and_then(|r| match &r.op {
            crate::sim::ClientOp::Backup { snapshot } => Some(snapshot.clone()),
            _ => None,
        })
        .expect("the held snapshot's backup");
    let rd = sim.delete(d, &[held_name.as_str()]);
    rd.expect_ok();
    assert!(rd.shared);
    sim.resume(first_get);
    let rc = sim.join(rc);
    rc.expect_ok();
    let listings = records(&sim, a, rc.index)
        .iter()
        .filter(|r| r.op == OpKind::List && r.key == "snapshots/")
        .count();
    assert!(listings >= 2, "the mark listed snapshots/ again");
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// Every other read error of a listed snapshot still fails the mark: an
/// injected GET failure is `compact refused` (nothing changed).
#[test]
fn the_mark_still_fails_on_other_read_errors() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    sim.backup(a, "a1", &tree("a1")).expect_ok();
    sim.fault(
        FaultRule::new(
            OpMatch::Kind(OpKind::Get),
            KeyMatch::Prefix("snapshots/".into()),
            FaultAction::Fail,
        )
        .client(a),
    );
    let rc = sim.compact(a);
    assert!(!rc.is_ok(), "{}", rc.detail());
    assert!(
        rc.detail().contains(crate::commands::compact::MARK_REFUSED),
        "{}",
        rc.detail()
    );
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// Q2 (U7): a lock-free backup that adds no chunk (the same source again:
/// equal items, equal tree chunks) creates a head anyway, the
/// predecessor's state at generation + 1, and still commits.
#[test]
fn a_reuse_only_lock_free_commit_creates_a_head() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    sim.backup(a, "a1", &tree("a1")).expect_ok();
    let r2 = sim.call(
        a,
        Call::BackupSameSource {
            snapshot: "a2".into(),
            of: "a1".into(),
        },
    );
    r2.expect_ok();
    assert!(r2.shared);
    let recs = records(&sim, a, r2.index);
    assert!(
        !recs
            .iter()
            .any(|r| r.op.is_write() && r.key.starts_with("index.d/")),
        "a2 added no chunk: {recs:?}"
    );
    assert_eq!(head_creates(&recs).len(), 1, "a2 created one head");
    sim.restore(a, "a2").expect_ok();
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// U9: a reuse-only head adds no segment and triggers no fold, so it runs
/// the head part of the garbage collection itself: after a stream of
/// unchanged backups more than an hour apart, `index.h/` holds the latest
/// head and the 16 below it, not one key per backup.
#[test]
fn reuse_only_heads_collect_old_heads() {
    let mut sim = encrypted(StoreKind::Conditional);
    let a = sim.client("a");
    sim.backup(a, "a1", &tree("a1")).expect_ok();
    for i in 2..=21 {
        sim.advance(std::time::Duration::from_secs(61 * 60));
        let run = sim.call(
            a,
            Call::BackupSameSource {
                snapshot: format!("a{i}"),
                of: "a1".into(),
            },
        );
        run.expect_ok();
    }
    let heads = sim.store().keys("index.h/").len();
    assert!(heads <= 17, "{heads} heads kept");
    sim.restore(a, "a21").expect_ok();
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// U7: a reuse-only commit whose generation another backup took rebases
/// (its delta rebuilt from what it kept) and commits at the next one.
#[test]
fn a_taken_reuse_only_commit_retries_and_commits() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(a, "a1", &tree("a1")).expect_ok();
    let create = sim.gate(head_create(a, 1));
    let r2 = sim.spawn(
        a,
        Call::BackupSameSource {
            snapshot: "a2".into(),
            of: "a1".into(),
        },
    );
    sim.expect_paused(create, "a2's head create");
    sim.backup(b, "b1", &tree("b1")).expect_ok();
    sim.resume(create);
    let r2 = sim.join(r2);
    r2.expect_ok();
    let recs = records(&sim, a, r2.index);
    let creates = head_creates(&recs);
    assert_eq!(creates.len(), 2, "{creates:?}");
    assert_eq!(creates[0].outcome, OpOutcome::Exists);
    assert_eq!(creates[1].outcome, OpOutcome::Ok);
    for name in ["a2", "b1"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// 3.4's counterexample, the reason for Q2: compact's second session
/// listing precedes the reuse-only backup's commit mark, and compact's
/// sweep head create follows the backup's head read; the backup reuses
/// chunks of a deleted snapshot that compact's mark found unreferenced.
/// The backup's own head takes compact's generation, so compact marks
/// again, sees the backup's snapshot and keeps the chunks: the backup's
/// snapshot restores.
#[test]
fn a_reuse_only_commit_conflicts_with_a_concurrent_sweep() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.backup(a, "old", &tree("old")).expect_ok();
    sim.backup(a, "keep", &tree("keep")).expect_ok();
    sim.delete(a, &["old"]).expect_ok();
    sim.advance(PAST_T_PACK);
    let sweep = sim.gate(head_create(a, 1));
    let rc = sim.spawn(a, Call::Compact);
    sim.expect_paused(sweep, "compact's sweep head create");
    let rb = sim.call(
        b,
        Call::BackupSameSource {
            snapshot: "again".into(),
            of: "old".into(),
        },
    );
    rb.expect_ok();
    assert!(rb.shared);
    sim.resume(sweep);
    let rc = sim.join(rc);
    rc.expect_ok();
    let recs = records(&sim, a, rc.index);
    assert_eq!(
        head_creates(&recs).first().map(|r| r.outcome.clone()),
        Some(OpOutcome::Exists),
        "the backup's head took compact's generation"
    );
    for name in ["keep", "again"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// Repair runs as an exclusive session (10-single-protocol step 8; under
/// the lock before) and refuses live sessions: beside a compact (registered
/// with `writes_heads`, held) and beside a delete (an unflagged marker) it
/// refuses with `ActiveSessions` (beside the compact it yielded `Locked`
/// before step 8); a compact whose registration listing meets repair's live
/// exclusive session fails `Locked`. Each proceeds once the other is done.
#[test]
fn repair_and_a_lock_free_compact_exclude_each_other() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, r) = (sim.client("a"), sim.client("r"));
    sim.backup(a, "a1", &tree("a1")).expect_ok();
    let held = sim.gate(sessions_listing(a, 2));
    let rc = sim.spawn(a, Call::Compact);
    sim.expect_paused(held, "compact's sessions #1");
    expect_class(&sim.check_repair(r), AllowedError::ActiveSessions);
    sim.resume(held);
    sim.join(rc).expect_ok();

    // A lock-free delete, registered and held before its delete.
    sim.backup(a, "a2", &tree("a2")).expect_ok();
    let delete_held = sim.gate(GateRule::new(
        a,
        OpMatch::Kind(OpKind::Delete),
        KeyMatch::Prefix("snapshots/".into()),
    ));
    let rd = sim.spawn(
        a,
        Call::Delete {
            names: vec!["a2".into()],
        },
    );
    sim.expect_paused(delete_held, "the delete, registered");
    expect_class(&sim.check_repair(r), AllowedError::ActiveSessions);
    sim.resume(delete_held);
    sim.join(rd).expect_ok();

    // Repair held in its exclusive session, after its registration (at its
    // re-read of `config`, the open's read being the first): compact fails
    // Locked.
    let repair_held = sim.gate(
        GateRule::new(
            r,
            OpMatch::Kind(OpKind::Get),
            KeyMatch::Exact("config".into()),
        )
        .nth(2),
    );
    let rr = sim.spawn(r, Call::CheckRepair);
    sim.expect_paused(repair_held, "repair in its exclusive session");
    assert!(
        sim.store()
            .keys("sessions/x-")
            .iter()
            .any(|k| k.ends_with(".json")),
        "repair registered"
    );
    expect_class(&sim.compact(a), AllowedError::Locked);
    sim.resume(repair_held);
    sim.join(rr).expect_ok();
    sim.compact(a).expect_ok();
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// 04 "Current issues" 4-6 gone: a backup registers, uploads and commits
/// lock-free while a lock-free compact is in flight (held after its pack
/// listing); the compact then completes and every snapshot restores.
#[test]
fn a_backup_starts_and_commits_during_a_lock_free_compact() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    swept_and_aged(&mut sim, a);
    let held = sim.gate(sessions_listing(a, 2));
    let rc = sim.spawn(a, Call::Compact);
    sim.expect_paused(held, "compact after its pack listing");
    let rb = sim.backup(b, "b1", &tree("b1"));
    rb.expect_ok();
    assert!(rb.shared);
    sim.resume(held);
    sim.join(rc).expect_ok();
    for name in ["keep", "b1"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// Two compacts on a single-writer repository (10-single-protocol step 7;
/// a lock-mode compact beside a lock-free one before): each registers with
/// `writes_heads` before its single-writer check, so the one that checks
/// second finds the other and refuses `Locked` before any write, in either
/// order.
#[test]
fn single_writer_compacts_refuse_each_other() {
    let mut sim = encrypted(StoreKind::Plain);
    let (f, l) = (sim.client("f"), sim.client("l"));
    swept_and_aged(&mut sim, f);
    for (first, second) in [(f, l), (l, f)] {
        // The first's single-writer check: registered and flagged.
        let held = sim.gate(sessions_listing(first, 2));
        let rf = sim.spawn(first, Call::Compact);
        sim.expect_paused(held, "the first compact, registered");
        let rl = sim.compact(second);
        expect_class(&rl, AllowedError::Locked);
        assert!(
            !records(&sim, second, rl.index)
                .iter()
                .any(|r| r.op.is_write()
                    && (r.key.starts_with("index.") || r.key.starts_with("packs/"))),
            "the second compact wrote before refusing"
        );
        sim.resume(held);
        sim.join(rf).expect_ok();
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

// ── 10d review, F1 and F2: a stalled compact, a recovered journal pack ──

/// `c`'s marker writes (`sessions/*.json`): with its main thread held at
/// a gate, its heartbeat's refresh.
fn heartbeat_put(c: ClientId) -> GateRule {
    GateRule::new(c, OpMatch::AnyWrite, KeyMatch::Prefix("sessions/".into()))
        .filter(|_, k| k.ends_with(".json"))
}

/// `c`'s next index object write on its main thread (a sweep's or a repack
/// batch's segment: before the publish's fence and head create).
fn index_object_put(c: ClientId) -> GateRule {
    GateRule::new(c, OpMatch::AnyWrite, KeyMatch::Prefix("index.d/".into()))
        .not_on_thread(crate::repo::sessions::HEARTBEAT_THREAD_NAME)
}

/// Two snapshots sharing a file, `old` deleted and everything past T_pack:
/// the next compact sweeps `old`'s own chunks and repacks the shared pack.
fn shared_swept_and_aged(sim: &mut Sim, a: ClientId) {
    let shared = |name: &str| {
        SourceTree::seeded(name, 1, FILE_SIZE).file(
            "shared.bin",
            crate::sim::runner::seeded_bytes("shared", FILE_SIZE),
        )
    };
    sim.backup(a, "old", &shared("old")).expect_ok();
    sim.backup(a, "keep", &shared("keep")).expect_ok();
    sim.delete(a, &["old"]).expect_ok();
    sim.advance(PAST_T_PACK);
}

/// F1 (a): a lock-free compact stalls 12 h after it wrote a repack batch's
/// output pack, before the batch's segment and head (a suspended laptop).
/// Another compact (threshold 100: it repacks no pack with live entries)
/// reaps the stalled run's marker and deletes the output as an old orphan.
/// Resumed, the stalled compact's marker check fails before the batch's
/// head create (`MarkerLostDuringCommit`): no head names the deleted
/// output, no source is deleted, and every snapshot restores. Without the
/// check its head relocated `keep`'s chunks into the deleted pack.
#[test]
fn a_compact_stalled_before_its_batch_head_stops_at_its_marker_check() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.set_compact_threshold(c, 100.0);
    shared_swept_and_aged(&mut sim, a);
    let output = sim.gate(GateRule::new(
        a,
        OpMatch::AnyWrite,
        KeyMatch::Prefix("packs/".into()),
    ));
    let ra = sim.spawn(a, Call::Compact);
    sim.expect_paused(output, "a's repack output PUT, after its sweep head");
    // Held right after the output's PUT: at the batch persist's head read,
    // before its first index object (the slow-write refusal counts from
    // there).
    let persist = sim.gate(
        GateRule::new(a, OpMatch::AnyRead, KeyMatch::Prefix("index.h/".into()))
            .not_on_thread(crate::repo::sessions::HEARTBEAT_THREAD_NAME),
    );
    sim.resume(output);
    sim.expect_paused(persist, "the repack batch's persist, its output written");
    let outputs: Vec<String> = records(&sim, a, ra.index())
        .into_iter()
        .filter(|r| r.op.is_write() && r.applied() && r.key.starts_with("packs/"))
        .map(|r| r.key)
        .collect();
    assert!(!outputs.is_empty(), "a wrote its repack output");
    // The heartbeat counts polls: one long jump is one poll, no refresh.
    // The gate holds any refresh until c has reaped the marker.
    let hb = sim.gate(heartbeat_put(a));
    sim.advance(PAST_T_PACK);
    let rc = sim.compact(c);
    rc.expect_ok();
    assert!(rc.shared);
    for k in &outputs {
        assert!(
            sim.store().object(k).is_none(),
            "c deleted a's output {k} as an old orphan: {}",
            rc.detail()
        );
    }
    sim.resume(hb);
    sim.resume(persist);
    let ra = sim.join(ra);
    sim.restore(a, "keep").expect_ok();
    sim.check_invariants(Invariant::STEP8).assert_clean();
    expect_class(&ra, AllowedError::MarkerLostDuringCommit);
    let recs = records(&sim, a, ra.index);
    assert_eq!(
        head_creates(&recs).len(),
        1,
        "a created only its sweep head"
    );
    assert!(
        !recs
            .iter()
            .any(|r| r.op.is_delete() && r.key.starts_with("packs/")),
        "a deleted no pack"
    );
    sim.compact(a).expect_ok();
    sim.restore(a, "keep").expect_ok();
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// `AgedSessionReaped` claims the residual's violations and nothing else:
/// with the victim not in a session (`RunRecord::shared`), or no reap aged
/// (every record at one instant), they are `Unknown`; an unrelated violation
/// stays `Unknown`.
fn assert_narrow_aged_session_claim(sim: &Sim, report: &crate::sim::Report, victim: usize) {
    use crate::sim::Signature;
    use crate::sim::driver::{Evidence, classify_evidence};
    use std::collections::BTreeSet;
    let ev = Evidence::of(sim);
    let judge = |e: &Evidence, r: &crate::sim::Report| classify_evidence(e, r);
    assert_eq!(
        judge(&ev, report),
        BTreeSet::from([Signature::AgedSessionReaped]),
        "{report}"
    );
    let mut e = ev.clone();
    for r in &mut e.runs {
        if r.index == victim {
            r.shared = false;
        }
    }
    assert_eq!(
        judge(&e, report),
        BTreeSet::from([Signature::Unknown]),
        "the victim not in a session"
    );
    let mut e = ev.clone();
    let t0 = e.log[0].at;
    for r in &mut e.log {
        r.at = t0;
    }
    assert_eq!(
        judge(&e, report),
        BTreeSet::from([Signature::Unknown]),
        "no reap aged"
    );
    let mut unrelated = report.clone();
    unrelated.violations.push(crate::sim::Violation {
        invariant: Invariant::CommittedSnapshotsRestore,
        kind: ViolationKind::RestoreDiffers,
        subject: "unrelated".into(),
        detail: "injected by the test".into(),
        step: None,
    });
    assert_eq!(
        judge(&ev, &unrelated),
        BTreeSet::from([Signature::AgedSessionReaped, Signature::Unknown]),
        "an unrelated violation"
    );
}

/// The residual of F1 (a) (documented; signature `AgedSessionReaped`): the
/// stall falls between the marker check and the head create it guards (a
/// request in flight while the machine sleeps 12 h). Another compact's
/// sweep head would take that generation, so the late create would find it
/// taken and the retry's check would stop the run; here that compact skips
/// its sweep beside a committing backup (no head), deletes the output as an
/// old orphan, and the late create lands and names it: `keep` no longer
/// restores. Claimed by `AgedSessionReaped` alone, as the backup's R1 is.
#[test]
fn a_compact_stalled_after_its_marker_check_is_aged_session_reaped() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.set_compact_threshold(c, 100.0);
    shared_swept_and_aged(&mut sim, a);
    let batch = sim.gate(head_create(a, 2));
    let ra = sim.spawn(a, Call::Compact);
    sim.expect_paused(batch, "the repack batch's head create, after its check");
    let hb = sim.gate(heartbeat_put(a));
    sim.advance(PAST_T_PACK);
    let b_head = sim.gate(head_create(b, 1));
    let rb = sim.spawn(b, backup_call("b1"));
    sim.expect_paused(b_head, "b committing");
    let rc = sim.compact(c);
    rc.expect_ok();
    assert!(
        rc.detail().contains("sweep-skipped Some(Committing)"),
        "{}",
        rc.detail()
    );
    sim.resume(hb);
    sim.resume(batch);
    let ra = sim.join(ra);
    sim.resume(b_head);
    let _ = sim.join(rb);
    let victim = ra.index;
    let recs = records(&sim, a, ra.index);
    let creates = head_creates(&recs);
    assert!(
        creates.len() == 2 && creates.iter().all(|r| r.outcome == OpOutcome::Ok),
        "a's late batch create landed: {}",
        ra.detail()
    );
    let report = sim.check_invariants(Invariant::STEP8);
    assert!(!report.violations.is_empty(), "the residual reproduces");
    assert_narrow_aged_session_claim(&sim, &report, victim);
}

/// The residual of F1 (b), re-derived on a single-writer repository by
/// 10-single-protocol step 7 (its victim was a lock-path backup, which
/// committed a reuse-only snapshot without a head): the compact stalls
/// 46 min between its marker check and its sweep head write (its EXISTS of
/// the generation). The backup's single-writer listing reaps the stale
/// marker and it commits its reuse-only snapshot, with a head now (Q2 on
/// both modes). That head takes the generation the sweep head was for: the
/// compact's EXISTS finds it, the sweep writes nothing, and `again`
/// restores. The damage the lock-path backup showed needs a head-less
/// reuse-only commit, which no binary makes.
#[test]
fn a_sweep_stalled_after_its_marker_check_is_aged_session_reaped() {
    let mut sim = encrypted(StoreKind::Plain);
    let (a, c, l) = (sim.client("a"), sim.client("c"), sim.client("l"));
    swept_and_aged(&mut sim, a);
    let sweep = sim.gate(plain_head_write(c, 1));
    let rc = sim.spawn(c, Call::Compact);
    sim.expect_paused(sweep, "c's sweep head write, after its check");
    let hb = sim.gate(heartbeat_put(c));
    sim.advance(std::time::Duration::from_secs(46 * 60));
    let rl = sim.call(
        l,
        Call::BackupSameSource {
            snapshot: "again".into(),
            of: "old".into(),
        },
    );
    rl.expect_ok();
    assert!(
        records(&sim, l, rl.index)
            .iter()
            .any(|r| r.op == OpKind::Put && r.key.starts_with("index.h/")),
        "the reuse-only commit wrote a head"
    );
    sim.resume(hb);
    sim.resume(sweep);
    let rc = sim.join(rc);
    assert!(
        !records(&sim, c, rc.index)
            .iter()
            .any(|r| r.op == OpKind::Put && r.key.starts_with("index.h/")),
        "the stalled sweep wrote its head: {}",
        rc.detail()
    );
    for name in ["keep", "again"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// F1 (b): a compact stalls 46 min after its mark, before its sweep's
/// index write. A backup reaps the stale marker at its listing, so it does
/// not wait, and commits a reuse-only snapshot of the deleted `old`'s
/// source. Resumed, the compact's marker check fails before the sweep head
/// (`MarkerLostDuringCommit`), so the sweep planned from the older mark
/// removes nothing; `again` restores. Without the check the sweep removed
/// `again`'s chunks and deleted their packs. Since 10-single-protocol step
/// 7 on a single-writer repository, the backup single-writer (a lock
/// client's lock-path backup before, whose reuse-only commit wrote no
/// head; it writes one now).
#[test]
fn a_compact_stalled_before_its_sweep_stops_beside_a_lock_path_backup() {
    let mut sim = encrypted(StoreKind::Plain);
    let (a, c, l) = (sim.client("a"), sim.client("c"), sim.client("l"));
    swept_and_aged(&mut sim, a);
    let sweep = sim.gate(index_object_put(c));
    let rc = sim.spawn(c, Call::Compact);
    sim.expect_paused(sweep, "c's sweep index write, after its mark");
    // One jump is one heartbeat poll (no refresh); the gate holds any
    // refresh until l has reaped the marker.
    let hb = sim.gate(heartbeat_put(c));
    sim.advance(std::time::Duration::from_secs(46 * 60));
    let rl = sim.call(
        l,
        Call::BackupSameSource {
            snapshot: "again".into(),
            of: "old".into(),
        },
    );
    rl.expect_ok();
    assert!(rl.shared);
    assert!(
        records(&sim, l, rl.index)
            .iter()
            .any(|r| r.op == OpKind::Put && r.key.starts_with("index.h/")),
        "the reuse-only commit wrote a head"
    );
    sim.resume(hb);
    sim.resume(sweep);
    let rc = sim.join(rc);
    for name in ["keep", "again"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
    expect_class(&rc, AllowedError::MarkerLostDuringCommit);
    assert!(
        !records(&sim, c, rc.index)
            .iter()
            .any(|r| r.op.is_write() && r.key.starts_with("index.h/")),
        "the stalled compact wrote a head"
    );
}

/// F1 (b) without a stall: a backup cannot commit between a compact's mark
/// and its sweep head. On a single-writer repository (10-single-protocol
/// step 7; a lock client's lock-path backup before) its listing before its
/// mark finds the compact's `writes_heads` marker (kept for the compact's
/// whole run) and it waits, unmarked, until the compact is done; then its
/// reuse of the swept chunks fails it typed (`StaleChunksDuringCommit`) or
/// what it commits restores.
#[test]
fn a_lock_path_backup_waits_for_a_lock_free_compact() {
    let mut sim = encrypted(StoreKind::Plain);
    let (a, c, l) = (sim.client("a"), sim.client("c"), sim.client("l"));
    swept_and_aged(&mut sim, a);
    let sweep = sim.gate(plain_head_write(c, 1));
    let rc = sim.spawn(c, Call::Compact);
    sim.expect_paused(sweep, "c's sweep head write, after its mark");
    let second = sim.gate(before_mark(l, 2));
    let sl = sim.spawn(
        l,
        Call::BackupSameSource {
            snapshot: "again".into(),
            of: "old".into(),
        },
    );
    sim.expect_paused(
        second,
        "l's second listing before its mark, after it waited",
    );
    let recs = records(&sim, l, sl.index());
    assert!(
        !recs
            .iter()
            .any(|r| r.op.is_write()
                && (r.key.starts_with("snapshots/") || r.key.starts_with("index."))),
        "l wrote beside the compact"
    );
    assert!(
        !recs
            .iter()
            .any(|r| r.op == OpKind::Put && r.key.ends_with(".1.json")),
        "l marked its commit beside the compact"
    );
    sim.resume(sweep);
    let rc = sim.join(rc);
    rc.expect_ok();
    assert!(
        rc.detail().contains("sweep-skipped None"),
        "{}",
        rc.detail()
    );
    sim.resume(second);
    let rl = sim.join(sl);
    if rl.is_ok() {
        sim.restore(a, "again").expect_ok();
    } else {
        expect_class(&rl, AllowedError::StaleChunksDuringCommit);
    }
    sim.restore(a, "keep").expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// F2: a repack source gains an entry after the batch's head read. B1 runs
/// long (its index loaded before `a` committed chunk X) and uploads pack P'
/// = {X, Y}; its head adds only Y (X is indexed already) and it crashes
/// before its snapshot. `a` is deleted and a compact (threshold 100)
/// sweeps X's other copy. B2 recovers B1's journal (P' younger than
/// T_pack/2) and adopts X into P', then holds before its head create.
/// Compact C finds P' with live Y and dead X: lock-free it does not repack
/// a pack younger than T_pack (or inside a live horizon), so P' stays; B2's
/// create is taken by nothing of C's batch, B2 commits, and its snapshot
/// restores. Without the rule C repacked P', won the generation, deleted
/// P', and B2's rebased head named X in it.
#[test]
fn a_young_pack_a_backup_adopts_is_not_a_lock_free_repack_source() {
    const WAIT: std::time::Duration = std::time::Duration::from_secs(5);
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b, c0, d, c) = (
        sim.client("a"),
        sim.client("b"),
        sim.client("c0"),
        sim.client("d"),
        sim.client("c"),
    );
    sim.set_compact_threshold(c0, 100.0);
    let chunk_x = crate::sim::runner::seeded_bytes("x", FILE_SIZE);
    let chunk_y = crate::sim::runner::seeded_bytes("y", FILE_SIZE);
    let tree_a = SourceTree::new().file("x.bin", chunk_x.clone());
    let tree_b = SourceTree::new()
        .file("x.bin", chunk_x)
        .file("y.bin", chunk_y);
    sim.backup(a, "keep", &tree("keep")).expect_ok();
    // B1 loads the index, then is held before its packs. Its data pack
    // (P') and its tree pack upload on separate threads, in either order:
    // hold both, so P' is written after the wait whichever comes first.
    let upload = sim.gate(GateRule::new(
        b,
        OpMatch::AnyWrite,
        KeyMatch::Prefix("packs/".into()),
    ));
    let upload_2 =
        sim.gate(GateRule::new(b, OpMatch::AnyWrite, KeyMatch::Prefix("packs/".into())).nth(2));
    let s1 = sim.spawn(
        b,
        Call::Backup {
            snapshot: "b1".into(),
            tree: tree_b.clone(),
        },
    );
    sim.expect_paused(upload, "b1 before its upload");
    sim.backup(a, "a", &tree_a).expect_ok();
    sim.delete(a, &["a"]).expect_ok();
    // 6 h 45 min, the heartbeat polling every 30 s and refreshing: b1's
    // record stays continuous, so it commits lock-free.
    assert!(sim.time().wait_for_waiters(1, WAIT), "b1's heartbeat waits");
    for _ in 0..810 {
        sim.advance(std::time::Duration::from_secs(30));
        assert!(sim.time().wait_for_waiters(1, WAIT), "b1's heartbeat waits");
    }
    sim.fault(
        FaultRule::new(
            OpMatch::AnyWrite,
            KeyMatch::Prefix("snapshots/".into()),
            FaultAction::Crash,
        )
        .client(b),
    );
    sim.expect_paused(upload_2, "b1 before its second upload");
    sim.resume(upload);
    sim.resume(upload_2);
    let r1 = sim.join(s1);
    expect_class(&r1, AllowedError::Injected);
    assert_eq!(
        lock_writes(&records(&sim, b, r1.index)),
        0,
        "b1 committed lock-free: {}",
        r1.detail()
    );
    let b1_packs = sim.store().keys_written_by(b, "packs/");
    // X's other copy is 12 h old and unreferenced: swept; P' is 5 h old.
    sim.advance(std::time::Duration::from_secs(5 * 3600 + 20 * 60));
    let r0 = sim.compact(c0);
    r0.expect_ok();
    assert!(!r0.detail().contains("unreferenced 0 "), "{}", r0.detail());
    for k in &b1_packs {
        assert!(sim.store().object(k).is_some(), "c0 kept b1's pack {k}");
    }
    let b2_head = sim.gate(head_create(d, 1));
    let s2 = sim.spawn(
        d,
        Call::Backup {
            snapshot: "b2".into(),
            tree: tree_b,
        },
    );
    sim.expect_paused(b2_head, "b2's head create");
    let rc = sim.compact(c);
    rc.expect_ok();
    assert!(
        rc.detail().contains("sweep-skipped Some(Committing)"),
        "{}",
        rc.detail()
    );
    sim.resume(b2_head);
    sim.join(s2).expect_ok();
    for name in ["keep", "b2"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
    for k in &b1_packs {
        assert!(
            sim.store().object(k).is_some(),
            "c kept b1's young pack {k}: {}",
            rc.detail()
        );
    }
}

/// F2's horizon half: a pack with unused space 12 h old that lies inside a
/// live backup's horizon is not a lock-free repack source either. B
/// registers and is held before its first pack; `old` and `keep` (sharing
/// a file) are written after B's start, `old` is deleted, and 12 h pass
/// with B's heartbeat refreshing. The compact sweeps `old`'s chunks but
/// does not repack the shared pack (B's start, minus M, is older than it);
/// once B has committed, the next compact repacks it.
#[test]
fn an_old_pack_inside_a_live_horizon_is_not_a_lock_free_repack_source() {
    const WAIT: std::time::Duration = std::time::Duration::from_secs(5);
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    let upload = sim.gate(GateRule::new(
        b,
        OpMatch::AnyWrite,
        KeyMatch::Prefix("packs/".into()),
    ));
    let s = sim.spawn(b, backup_call("b1"));
    sim.expect_paused(upload, "b1 before its upload");
    let shared = |name: &str| {
        SourceTree::seeded(name, 1, FILE_SIZE).file(
            "shared.bin",
            crate::sim::runner::seeded_bytes("shared", FILE_SIZE),
        )
    };
    sim.backup(a, "old", &shared("old")).expect_ok();
    sim.backup(a, "keep", &shared("keep")).expect_ok();
    sim.delete(a, &["old"]).expect_ok();
    // 12 h 1 min, the heartbeat polling every 30 s and refreshing: b1 stays
    // live, its horizon its start.
    assert!(sim.time().wait_for_waiters(1, WAIT), "b1's heartbeat waits");
    for _ in 0..(PAST_T_PACK.as_secs() / 30 + 1) {
        sim.advance(std::time::Duration::from_secs(30));
        assert!(sim.time().wait_for_waiters(1, WAIT), "b1's heartbeat waits");
    }
    let held = sim.compact(a);
    held.expect_ok();
    assert!(
        !held.detail().contains("unreferenced 0 "),
        "{}",
        held.detail()
    );
    assert!(held.detail().contains(" repacked 0 "), "{}", held.detail());
    assert!(held.detail().ends_with(" held 1"), "{}", held.detail());
    sim.resume(upload);
    sim.join(s).expect_ok();
    let after = sim.compact(a);
    after.expect_ok();
    assert!(
        !after.detail().contains(" repacked 0 "),
        "{}",
        after.detail()
    );
    assert!(after.detail().ends_with(" held 0"), "{}", after.detail());
    for name in ["keep", "b1"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

// ── V3: what judges a lock-free run's deletes ───────────────────────────
//
// Invariant 13 judges a shared run (`RunRecord::shared`) only under an
// exclusive hold. Each test below makes a shared maintenance run
// (`Sim::custom_shared`, a registered session) delete what it must not, and
// shows the invariant that judges that delete flags it.

/// A pack the latest head names, deleted: invariant 4 (the index names an
/// absent pack) and invariant 1 (the snapshot no longer restores).
#[test]
fn a_lock_free_delete_of_a_named_pack_is_flagged() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, c) = (sim.client("a"), sim.client("c"));
    let ra = sim.backup(a, "a1", &tree("a1"));
    ra.expect_ok();
    let pack = records(&sim, a, ra.index)
        .into_iter()
        .find(|r| r.op.is_write() && r.key.starts_with("packs/"))
        .expect("a1 uploaded a pack")
        .key;
    let run = sim.custom_shared(c, "delete a named pack", MarkerFlags::default(), |repo| {
        repo.storage().delete(&pack)
    });
    run.expect_ok();
    assert!(run.shared);
    let report = sim.check_invariants(Invariant::STEP8);
    let kinds: Vec<ViolationKind> = report.violations.iter().map(|v| v.kind).collect();
    assert!(
        kinds.contains(&ViolationKind::IndexNamesAbsentPack),
        "{report}"
    );
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.invariant == Invariant::CommittedSnapshotsRestore),
        "{report}"
    );
    assert!(
        !report
            .violations
            .iter()
            .any(|v| v.invariant == Invariant::ExclusiveSessions),
        "invariant 13 judges a shared run only under an exclusive hold: {report}"
    );
}

/// An orphan pack younger than T_pack (a live backup's upload, not yet
/// committed), deleted: invariant 10.
#[test]
fn a_lock_free_delete_of_a_young_orphan_is_flagged() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (b, c) = (sim.client("b"), sim.client("c"));
    let b_snap = sim.gate(GateRule::new(
        b,
        OpMatch::Kind(OpKind::PutIfAbsent),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let rb = sim.spawn(b, backup_call("b1"));
    sim.expect_paused(b_snap, "b before its head create");
    let pack = sim
        .store()
        .log()
        .into_iter()
        .find(|r| r.client == b && r.op.is_write() && r.key.starts_with("packs/"))
        .expect("b uploaded a pack")
        .key;
    let run = sim.custom_shared(c, "delete a young orphan", MarkerFlags::default(), |repo| {
        repo.storage().delete(&pack)
    });
    run.expect_ok();
    sim.resume(b_snap);
    let _ = sim.join(rb);
    let report = sim.check_invariants(Invariant::STEP8);
    assert!(
        report.violations.iter().any(|v| matches!(
            v.kind,
            ViolationKind::YoungOrphanDeleted | ViolationKind::LiveSessionPackDeleted
        )),
        "{report}"
    );
}

/// A committed snapshot deleted by a run that does not report it removed:
/// invariant 3.
#[test]
fn a_lock_free_silent_snapshot_delete_is_flagged() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(a, "a1", &tree("a1")).expect_ok();
    let key = sim.store().keys("snapshots/").pop().expect("a1's snapshot");
    sim.custom_shared(c, "delete a snapshot", MarkerFlags::default(), |repo| {
        repo.storage().delete(&key)
    })
    .expect_ok();
    let report = sim.check_invariants(Invariant::STEP8);
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.kind == ViolationKind::SilentRemoval),
        "{report}"
    );
}

/// An index object the latest head names, deleted: invariant 4.
#[test]
fn a_lock_free_delete_of_a_named_index_object_is_flagged() {
    let mut sim = encrypted(StoreKind::Conditional);
    let (a, c) = (sim.client("a"), sim.client("c"));
    let ra = sim.backup(a, "a1", &tree("a1"));
    ra.expect_ok();
    let object = records(&sim, a, ra.index)
        .into_iter()
        .find(|r| r.op.is_write() && r.key.starts_with("index.d/"))
        .expect("a1 wrote its segment")
        .key;
    sim.custom_shared(
        c,
        "delete a named index object",
        MarkerFlags::default(),
        |repo| repo.storage().delete(&object),
    )
    .expect_ok();
    let report = sim.check_invariants(Invariant::STEP8);
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.invariant == Invariant::IndexIntegrity),
        "{report}"
    );
}

/// A head replaced by other bytes (a lock-free writer must only create):
/// invariant 12.
#[test]
fn a_lock_free_head_overwrite_is_flagged() {
    let mut sim = encrypted(StoreKind::ConditionalOverwrite);
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(a, "a1", &tree("a1")).expect_ok();
    sim.backup(a, "a2", &tree("a2")).expect_ok();
    let heads = sim.store().keys("index.h/");
    let (older, latest) = (
        heads[heads.len() - 2].clone(),
        heads[heads.len() - 1].clone(),
    );
    sim.custom_shared(c, "overwrite a head", MarkerFlags::default(), |repo| {
        let bytes = repo.storage().get(&older)?.expect("the older head");
        repo.storage().put(&latest, &bytes)
    })
    .expect_ok();
    let report = sim.check_invariants(Invariant::STEP8);
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.kind == ViolationKind::HeadOverwritten),
        "{report}"
    );
}
