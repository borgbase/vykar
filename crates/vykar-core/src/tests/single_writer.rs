//! Single-writer commits without the lock (10-single-protocol step 7), on
//! the simulator: the single-writer head write (EXISTS, plain PUT,
//! read-back) at the commit machine's level and through full backups, the
//! single-writer check (a backup waits, unflagged, while another head
//! writer is live; a compact refuses beside one), the suspend rule (a
//! backup whose marker record is not continuous at its commit mark reads
//! `config` again, applies the pack-age term, restarts its record and
//! re-checks its reuse on every attempt), and maintenance's re-read of
//! `config` after its registration. Searched schedules are in
//! `tests::sim_exhaustive` (`single_writer_checks_race`,
//! `single_writer_compact_race`, `gapped_backup_against_*`).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::compress::Compression;
use crate::repo::ages::T_PACK;
use crate::repo::control::CommitEvent;
use crate::repo::machine::{Machine, StepCx, StepKind};
use crate::repo::pack::PackType;
use crate::repo::{BackupCommit, Fence, OpenOptions, Repo, WriteSessionOptions, WriterMode};
use crate::sim::history::{AllowedError, ErrorClass};
use crate::sim::{
    Call, ClientId, GateId, GateRule, Invariant, KeyMatch, OpKind, OpMatch, OpRecord, RunRecord,
    Sim, SimConfig, SourceTree, Spawned, StoreKind,
};
use vykar_types::error::VykarError;

const FILE_SIZE: usize = 16 * 1024;

/// Past the session staleness (45 min): a marker unwritten this long is
/// reaped.
const STALE: Duration = Duration::from_secs(46 * 60);

fn sim_of(kind: StoreKind) -> Sim {
    Sim::with_config(SimConfig {
        kind,
        ..SimConfig::default()
    })
}

fn tree(name: &str) -> SourceTree {
    SourceTree::seeded(name, 2, FILE_SIZE)
}

fn backup_call(name: &str) -> Call {
    Call::Backup {
        snapshot: name.into(),
        tree: tree(name),
    }
}

/// The records of `client`'s run `run`.
fn records(sim: &Sim, client: ClientId, run: usize) -> Vec<OpRecord> {
    sim.store()
        .log()
        .into_iter()
        .filter(|r| r.client == client && r.run == Some(run))
        .collect()
}

fn expect_class(run: &RunRecord, class: AllowedError) {
    assert_eq!(
        run.error_class(),
        Some(&ErrorClass::Allowed(class)),
        "{}",
        run.detail()
    );
}

/// `c`'s `n`th listing of `sessions/` with mtimes (the open lists first,
/// then a single-writer backup lists before its mark, after it, and its
/// pre-head listing lists once more).
fn sessions_listing(c: ClientId, n: u32) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::ListMeta),
        KeyMatch::Exact("sessions/".into()),
    )
    .nth(n)
}

/// `c`'s first PUT of a snapshot: a commit is between its head write and
/// its commit point there, its marker flagged.
fn snapshot_put(c: ClientId) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::Put),
        KeyMatch::Prefix("snapshots/".into()),
    )
}

/// The commit mark's PUT among `recs`: the first marker key after the
/// registration's.
fn mark_position(recs: &[OpRecord]) -> usize {
    recs.iter()
        .position(|r| {
            r.op == OpKind::Put && r.key.starts_with("sessions/") && r.key.ends_with(".1.json")
        })
        .unwrap_or_else(|| panic!("no commit mark: {recs:?}"))
}

/// Writes of `index.h/` among `recs`.
fn head_writes(recs: &[OpRecord]) -> Vec<&OpRecord> {
    recs.iter()
        .filter(|r| r.op.is_write() && r.key.starts_with("index.h/"))
        .collect()
}

/// Spawn `c`'s backup of `name` and hold it at its first commit step
/// (its second listing of `sessions/`, after every upload), then suspend
/// it for `gap` ([`suspend_for`]): its marker record is not continuous at
/// its mark, and with `gap` over the session staleness its marker is stale
/// (reapable) meanwhile.
fn suspend_at_commit(sim: &mut Sim, c: ClientId, call: Call, gap: Duration) -> (GateId, Spawned) {
    let gate = sim.gate(sessions_listing(c, 2));
    let run = sim.spawn(c, call);
    sim.expect_paused(gate, "the backup's first commit step");
    suspend_for(sim, gap);
    (gate, run)
}

/// With `c`'s main thread held at a gate, advance time by `gap`: one long
/// jump is one heartbeat poll, which writes no marker (a refresh is due
/// after 30 polls), so `c`'s last marker write is `gap` old after it.
fn suspend_for(sim: &mut Sim, gap: Duration) {
    assert!(
        sim.time().wait_for_waiters(1, Duration::from_secs(10)),
        "the heartbeat waits"
    );
    sim.advance(gap);
    assert!(
        sim.time().wait_for_waiters(1, Duration::from_secs(10)),
        "the heartbeat waits again"
    );
}

// ── The single-writer head write at the commit machine's level ─────────

/// A handle of `c` on the simulator's store, with the index loaded, outside
/// any run (no lock, no session marker).
fn open(sim: &Sim, c: ClientId) -> Repo {
    Repo::open(
        Box::new(sim.store().handle(c)),
        sim.store().passphrase(),
        Some(sim.client_dir(c).join("repo-cache")),
        OpenOptions::new().with_index().host_env(sim.host_env(c)),
    )
    .unwrap()
}

fn entry(name: &str) -> crate::repo::manifest::SnapshotEntry {
    crate::repo::manifest::SnapshotEntry {
        name: name.into(),
        id: vykar_types::snapshot_id::SnapshotId::generate(),
        time: chrono::Utc::now(),
        source_label: String::new(),
        label: String::new(),
        source_paths: Vec::new(),
        hostname: String::new(),
    }
}

/// A session of `repo` storing `chunk`, and its commit machine.
fn session(repo: &mut Repo, name: &str, chunk: &[u8]) -> BackupCommit {
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    ws.store_chunk(repo, chunk, Compression::None, PackType::Data)
        .unwrap();
    BackupCommit::new(ws, entry(name), b"snapshot".to_vec(), None)
}

fn no_events(_: CommitEvent) {}

/// Step `m` (pick 0) until `stop` accepts its enabled steps or it ends.
fn step_until(
    repo: &mut Repo,
    m: &mut BackupCommit,
    stop: impl Fn(&[StepKind]) -> bool,
) -> vykar_types::error::Result<()> {
    loop {
        let enabled = m.enabled();
        if enabled.is_empty() || stop(&enabled) {
            return Ok(());
        }
        m.step(
            &mut StepCx {
                repo,
                fence: &Fence::unchecked(),
                events: &no_events,
            },
            0,
        )?;
    }
}

/// Wait (real time, at most 60 s) until `gate` holds an operation of a
/// thread outside any run (`Sim::expect_paused` needs a run in flight).
fn held(sim: &Sim, gate: GateId, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while sim.store().gate_paused(gate).is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: gate not reached"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn at_put_head(enabled: &[StepKind]) -> bool {
    enabled == [StepKind::PutHead]
}

/// The chunk ID `repo` stores `data` under.
fn chunk_id(repo: &Repo, data: &[u8]) -> vykar_types::chunk_id::ChunkId {
    vykar_types::chunk_id::ChunkId::compute(repo.crypto().chunk_hasher(), data)
}

/// Whether the latest index on storage (a fresh handle of `c`) holds
/// `data`'s chunk.
fn indexed(sim: &Sim, c: ClientId, data: &[u8]) -> bool {
    let repo = Repo::open(
        Box::new(sim.store().handle(c)),
        sim.store().passphrase(),
        Some(sim.client_dir(c).join("fresh-cache")),
        OpenOptions::new().with_index().host_env(sim.host_env(c)),
    )
    .unwrap();
    repo.chunk_index().get(&chunk_id(&repo, data)).is_some()
}

/// Two machines at their head write of one generation, both past their
/// EXISTS (both found it absent), their PUTs held at gates; `order` drives
/// them on from there.
fn two_head_writes(
    sim: &Sim,
    (a, b): (ClientId, ClientId),
    order: impl FnOnce(GateId, GateId),
) -> (
    vykar_types::error::Result<()>,
    vykar_types::error::Result<()>,
) {
    let (mut ra, mut rb) = (open(sim, a), open(sim, b));
    let mut ma = session(&mut ra, "a1", b"chunk of a");
    let mut mb = session(&mut rb, "b1", b"chunk of b");
    step_until(&mut ra, &mut ma, at_put_head).unwrap();
    step_until(&mut rb, &mut mb, at_put_head).unwrap();
    let put = |c| {
        sim.gate(GateRule::new(
            c,
            OpMatch::Kind(OpKind::Put),
            KeyMatch::Prefix("index.h/".into()),
        ))
    };
    let (ga, gb) = (put(a), put(b));
    std::thread::scope(|s| {
        let ta = s.spawn(|| step_until(&mut ra, &mut ma, |_| false));
        held(sim, ga, "a's head PUT, after its EXISTS");
        let tb = s.spawn(|| step_until(&mut rb, &mut mb, |_| false));
        held(sim, gb, "b's head PUT, after its EXISTS");
        order(ga, gb);
        (ta.join().unwrap(), tb.join().unwrap())
    })
}

/// 6.3 (a): two plain head writes of one generation whose EXISTS both found
/// it absent (a defeated single-writer check): a's PUT and read-back, then
/// b's: both read back their own bytes and commit, and b's head replaced
/// a's, so a's chunk is no longer indexed (a lost update, H3's shape on a
/// single-writer repository). The full-backup version, with the compact's
/// refusal and the repair, is
/// [`a_lost_update_of_two_full_backups_is_refused_by_compact_and_repaired`].
#[test]
fn two_plain_head_writes_of_one_generation_lose_an_update() {
    let mut sim = sim_of(StoreKind::Plain);
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let (ra, rb) = two_head_writes(&sim, (a, b), |ga, gb| {
        sim.resume(ga);
        // a's read-back and snapshot before b's PUT lands.
        std::thread::sleep(Duration::from_millis(200));
        sim.resume(gb);
    });
    ra.unwrap();
    rb.unwrap();
    assert!(!indexed(&sim, c, b"chunk of a"), "a's update survived");
    assert!(indexed(&sim, c, b"chunk of b"));
}

/// 6.3 (a'): a's PUT, then b's, then a's read-back finds b's bytes: a fails
/// `ConcurrentHeadWrite` and writes no snapshot; b reads back its own bytes
/// and commits. No damage: b's head stands and a's segment is debris.
#[test]
fn a_head_overwritten_before_the_read_back_fails_concurrent_head_write() {
    let mut sim = sim_of(StoreKind::Plain);
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let (ra, rb) = two_head_writes(&sim, (a, b), |ga, gb| {
        // Armed once both are at their PUT: a's next head read is its
        // read-back.
        let ga_get = sim.gate(GateRule::new(
            a,
            OpMatch::Kind(OpKind::Get),
            KeyMatch::Prefix("index.h/".into()),
        ));
        sim.resume(ga);
        held(&sim, ga_get, "a's read-back");
        sim.resume(gb);
        // b's read-back and its snapshot run while a waits at its own.
        std::thread::sleep(Duration::from_millis(200));
        sim.resume(ga_get);
    });
    assert!(
        matches!(ra, Err(VykarError::ConcurrentHeadWrite { .. })),
        "{ra:?}"
    );
    rb.unwrap();
    assert!(
        sim.store().keys_written_by(a, "snapshots/").is_empty(),
        "a wrote a snapshot"
    );
    assert!(indexed(&sim, c, b"chunk of b"));
}

/// 6.3 (c): b's base went stale (a committed the next generation after b's
/// head read): b's EXISTS finds the generation, b rebases and writes the
/// one after it, with no PUT at the taken one; both chunks are indexed.
#[test]
fn a_stale_base_rebases_on_the_exists() {
    let mut sim = sim_of(StoreKind::Plain);
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let (mut ra, mut rb) = (open(&sim, a), open(&sim, b));
    let mut mb = session(&mut rb, "b1", b"chunk of b");
    step_until(&mut rb, &mut mb, at_put_head).unwrap();
    let taken = rb.index_head().unwrap().generation + 1;
    let mut ma = session(&mut ra, "a1", b"chunk of a");
    step_until(&mut ra, &mut ma, |_| false).unwrap();
    let from = sim.store().step();
    step_until(&mut rb, &mut mb, |_| false).unwrap();
    let ops: Vec<OpRecord> = sim
        .store()
        .log()
        .into_iter()
        .filter(|r| r.client == b && r.step > from && r.key.starts_with("index.h/"))
        .collect();
    let key = crate::repo::index_store::head_key(taken);
    assert!(
        ops.iter().any(|r| r.op == OpKind::Exists && r.key == key),
        "{ops:?}"
    );
    assert!(
        !ops.iter().any(|r| r.op == OpKind::Put && r.key == key),
        "b wrote the taken generation: {ops:?}"
    );
    assert!(
        ops.iter()
            .any(|r| r.op == OpKind::Put && r.key == crate::repo::index_store::head_key(taken + 1)),
        "{ops:?}"
    );
    assert!(indexed(&sim, c, b"chunk of a"));
    assert!(indexed(&sim, c, b"chunk of b"));
}

// ── Full backups ────────────────────────────────────────────────────────

/// The Done-when's lost update with two full backups, through the
/// documented defeat of the single-writer check (an age reap): a is held
/// at its head PUT (after its EXISTS found the generation absent) while
/// 46 min pass (a suspended machine); b's listing reaps a's stale marker, b
/// commits that generation; a's PUT then replaces b's head, a reads back
/// its own bytes, and its snapshot fence finds its marker gone: a ends
/// `MarkerLostDuringCommit` with no snapshot, and b1 lost its index
/// coverage. Compact refuses (`MarkRefused`) and deletes none of b's packs;
/// `check --repair` re-indexes b1; T_pack later compact runs, and b1 and
/// c0 restore. Departure from the Done-when's text: only b's snapshot and
/// the control's restore, since a never commits. The stale-base case has no
/// full-backup form: the check keeps the second writer out (I1), so it is
/// shown at the machine's level ([`a_stale_base_rebases_on_the_exists`]).
#[test]
fn a_lost_update_of_two_full_backups_is_refused_by_compact_and_repaired() {
    let mut sim = sim_of(StoreKind::Plain);
    let (a, b, c) = (sim.client("a"), sim.client("b"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let ga = sim.gate(GateRule::new(
        a,
        OpMatch::Kind(OpKind::Put),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let sa = sim.spawn(a, backup_call("a1"));
    sim.expect_paused(ga, "a's head PUT, after its EXISTS");
    suspend_for(&mut sim, STALE);
    let rb = sim.backup(b, "b1", &tree("b1"));
    rb.expect_ok();
    let b_packs = sim.store().keys_written_by(b, "packs/");
    assert!(!b_packs.is_empty());
    sim.resume(ga);
    let ra = sim.join(sa);
    expect_class(&ra, AllowedError::MarkerLostDuringCommit);
    assert!(sim.store().keys_written_by(a, "snapshots/").is_empty());
    let report = sim.check_invariants(Invariant::STEP1);
    assert!(
        report.violated_for(Invariant::CommittedSnapshotsRestore, "b1"),
        "b1 kept its index coverage: {report}"
    );
    // The residual's third route (two backups): the single-writer race.
    assert_eq!(
        crate::sim::driver::classify_end_state(&sim).1,
        std::collections::BTreeSet::from([crate::sim::Signature::SingleWriterHeadRace]),
        "{report}"
    );

    let rc = sim.compact(c);
    expect_class(&rc, AllowedError::MarkRefused);
    for k in &b_packs {
        assert!(sim.store().object(k).is_some(), "{k} deleted");
    }
    sim.check_repair(c).expect_ok();
    sim.advance(T_PACK + Duration::from_secs(60));
    sim.compact(c).expect_ok();
    sim.restore(c, "b1").expect_ok();
    sim.restore(c, "c0").expect_ok();
    sim.check_invariants(Invariant::STEP1).assert_clean();
}

/// 6.3 (b): a is held after its mark, before its listing after the mark;
/// b's listing before its mark finds a committing and b waits (unmarked).
/// a's listing finds b unflagged and a commits; then b commits, its head
/// write after a's snapshot. No generation is written twice.
#[test]
fn two_single_writer_backups_never_both_pass_the_post_mark_listing() {
    let mut sim = sim_of(StoreKind::Plain);
    let (a, b) = (sim.client("a"), sim.client("b"));
    let ga = sim.gate(sessions_listing(a, 3));
    let sa = sim.spawn(a, backup_call("a1"));
    sim.expect_paused(ga, "a after its mark");
    let gb = sim.gate(sessions_listing(b, 3));
    let sb = sim.spawn(b, backup_call("b1"));
    sim.expect_paused(gb, "b's second listing before its mark");
    assert!(
        !sim.store()
            .keys_written_by(b, "sessions/")
            .iter()
            .any(|k| k.ends_with(".1.json")),
        "b marked beside a committing backup"
    );
    sim.resume(ga);
    sim.join(sa).expect_ok();
    sim.resume(gb);
    sim.join(sb).expect_ok();
    let log = sim.store().log();
    let a_snapshot = log
        .iter()
        .find(|r| r.client == a && r.op == OpKind::Put && r.key.starts_with("snapshots/"))
        .expect("a's snapshot")
        .step;
    let b_exists = log
        .iter()
        .find(|r| r.client == b && r.op == OpKind::Exists && r.key.starts_with("index.h/"))
        .expect("b's head write")
        .step;
    assert!(
        a_snapshot < b_exists,
        "b's head write overlapped a's commit"
    );
    let mut heads: Vec<String> = log
        .iter()
        .filter(|r| r.op == OpKind::Put && r.applied() && r.key.starts_with("index.h/"))
        .map(|r| r.key.clone())
        .collect();
    let n = heads.len();
    heads.sort();
    heads.dedup();
    assert_eq!(heads.len(), n, "a generation written twice");
    for name in ["a1", "b1"] {
        sim.restore(a, name).expect_ok();
    }
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// 6.3 (f): a compact beside a committing backup refuses (`Locked`, naming
/// the backup's host) after its listings, having written nothing but its
/// marker; the backup commits; a second compact runs.
#[test]
fn a_compact_beside_a_committing_backup_refuses_and_the_backup_commits() {
    let mut sim = sim_of(StoreKind::Plain);
    let (b, c) = (sim.client("b"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let gb = sim.gate(snapshot_put(b));
    let sb = sim.spawn(b, backup_call("b1"));
    sim.expect_paused(gb, "b before its snapshot PUT");
    let rc = sim.compact(c);
    expect_class(&rc, AllowedError::Locked);
    let host = sim.host(b).hostname;
    assert!(rc.detail().contains(&host), "{}", rc.detail());
    let wrote: Vec<OpRecord> = records(&sim, c, rc.index)
        .into_iter()
        .filter(|r| r.op.is_mutation() && !r.key.starts_with("sessions/"))
        .collect();
    assert!(
        wrote.is_empty(),
        "the compact wrote before refusing: {wrote:?}"
    );
    sim.resume(gb);
    sim.join(sb).expect_ok();
    sim.compact(c).expect_ok();
    sim.restore(c, "b1").expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

// ── The suspend rule ────────────────────────────────────────────────────

/// 6.3 (h), closing step 5's window: a backup opened on a single-writer
/// repository is suspended 46 min at its commit, its marker stale; the
/// repository is switched to multi-writer meanwhile (the switch reaps the
/// stale marker). Resumed, the backup re-reads `config` after its mark and
/// probe, takes the multi-writer mode (its put-if-absent check passes),
/// unmarks and runs the protection again: its heads are put-if-absent,
/// never a plain PUT, and its snapshot restores.
#[test]
fn a_suspended_backup_takes_the_multi_writer_mode_the_repository_switched_to() {
    let mut sim = sim_of(StoreKind::Conditional);
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.call(
        c,
        Call::SwitchMode {
            mode: WriterMode::Single,
        },
    )
    .expect_ok();
    let (gate, sa) = suspend_at_commit(&mut sim, a, backup_call("a1"), STALE);
    sim.call(
        c,
        Call::SwitchMode {
            mode: WriterMode::Multi,
        },
    )
    .expect_ok();
    sim.resume(gate);
    let ra = sim.join(sa);
    ra.expect_ok();
    let recs = records(&sim, a, ra.index);
    let mark = mark_position(&recs);
    let reread = recs[mark..]
        .iter()
        .position(|r| r.op == OpKind::Get && r.key == "config")
        .map(|i| i + mark)
        .expect("a's re-read of config after its mark");
    assert!(
        recs[reread..].iter().any(|r| r.op == OpKind::Put
            && r.key.starts_with("sessions/")
            && r.key.ends_with(".json")),
        "a unmarked and marked again: {recs:?}"
    );
    let heads = head_writes(&recs);
    assert!(!heads.is_empty(), "{recs:?}");
    assert!(
        heads.iter().all(|r| r.op == OpKind::PutIfAbsent),
        "a plain head PUT on a multi-writer repository: {heads:?}"
    );
    sim.restore(c, "a1").expect_ok();
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// 6.3 (i): the other direction. A backup opened on a multi-writer
/// repository is suspended, the repository switched to single-writer; the
/// re-read restarts its protection in the single-writer mode: its listing
/// before the mark after the re-read, its head written with EXISTS, plain
/// PUT and read-back.
#[test]
fn a_suspended_backup_takes_the_single_writer_mode_the_repository_switched_to() {
    let mut sim = sim_of(StoreKind::Conditional);
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(c, "c0", &tree("c0")).expect_ok();
    let (gate, sa) = suspend_at_commit(&mut sim, a, backup_call("a1"), STALE);
    sim.call(
        c,
        Call::SwitchMode {
            mode: WriterMode::Single,
        },
    )
    .expect_ok();
    sim.resume(gate);
    let ra = sim.join(sa);
    ra.expect_ok();
    let recs = records(&sim, a, ra.index);
    let mark = mark_position(&recs);
    let reread = recs[mark..]
        .iter()
        .position(|r| r.op == OpKind::Get && r.key == "config")
        .map(|i| i + mark)
        .expect("a's re-read of config after its mark");
    assert!(
        recs[reread..]
            .iter()
            .any(|r| r.op == OpKind::ListMeta && r.key == "sessions/"),
        "no single-writer listing after the re-read: {recs:?}"
    );
    let heads = head_writes(&recs);
    assert!(!heads.is_empty(), "{recs:?}");
    assert!(
        heads.iter().all(|r| r.op == OpKind::Put),
        "a put-if-absent head on a single-writer repository: {heads:?}"
    );
    assert!(
        recs.iter()
            .any(|r| r.op == OpKind::Exists && r.key.starts_with("index.h/")),
        "{recs:?}"
    );
    sim.restore(c, "a1").expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// 6.3 (j): a suspended backup that reused chunks of `old` (a backup of the
/// same untouched source) while `old` was deleted and a compact, which
/// reaped its stale marker, swept those chunks: resumed, the suspend rule
/// restarts its record and its re-check of reused chunks fails it
/// `StaleChunksDuringCommit`. No snapshot; `old` is gone as asked.
#[test]
fn a_suspended_backup_refuses_when_a_sweep_removed_a_chunk_it_reused() {
    let mut sim = sim_of(StoreKind::Plain);
    let (a, c) = (sim.client("a"), sim.client("c"));
    sim.backup(c, "old", &tree("old")).expect_ok();
    sim.backup(c, "keep", &tree("keep")).expect_ok();
    sim.advance(T_PACK + Duration::from_secs(60));
    let (gate, sa) = suspend_at_commit(
        &mut sim,
        a,
        Call::BackupSameSource {
            snapshot: "again".into(),
            of: "old".into(),
        },
        STALE,
    );
    sim.delete(c, &["old"]).expect_ok();
    let rc = sim.compact(c);
    rc.expect_ok();
    sim.resume(gate);
    let ra = sim.join(sa);
    expect_class(&ra, AllowedError::StaleChunksDuringCommit);
    assert!(sim.store().keys_written_by(a, "snapshots/").is_empty());
    sim.restore(c, "keep").expect_ok();
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// 6.3 (k): a backup suspended past `PACK_AGE_LIMIT` at its commit (its
/// packs may be old enough for compact) refuses `StaleChunksDuringCommit`
/// at its mark, before any index write. Invariant 11 (this case) holds.
#[test]
fn a_suspended_backup_with_old_packs_refuses_before_any_head() {
    let mut sim = sim_of(StoreKind::Plain);
    let a = sim.client("a");
    let (gate, sa) = suspend_at_commit(
        &mut sim,
        a,
        backup_call("a1"),
        T_PACK + Duration::from_secs(60),
    );
    sim.resume(gate);
    let ra = sim.join(sa);
    expect_class(&ra, AllowedError::StaleChunksDuringCommit);
    let recs = records(&sim, a, ra.index);
    assert!(
        !recs
            .iter()
            .any(|r| r.op.is_write() && r.key.starts_with("index.")),
        "an index write: {recs:?}"
    );
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// 6.3 (l): a backup whose repository's `config` names another repository
/// after its suspension (replaced meanwhile) refuses at its re-read with
/// nothing committed: no head, no snapshot.
#[test]
fn a_suspended_backup_whose_repository_changed_refuses() {
    let mut sim = sim_of(StoreKind::Plain);
    let a = sim.client("a");
    let (gate, sa) =
        suspend_at_commit(&mut sim, a, backup_call("a1"), Duration::from_secs(35 * 60));
    let stored = sim.store().object("config").unwrap().data;
    let crate::repo::DecodedConfig::Map(mut config) =
        crate::repo::RepoConfig::decode(&stored).unwrap()
    else {
        panic!("a v6 config");
    };
    config.id = vec![0x42; config.id.len()];
    sim.tamper_put("config", &config.encode().unwrap());
    sim.resume(gate);
    let ra = sim.join(sa);
    assert!(
        ra.detail().contains("the repository config changed"),
        "{}",
        ra.detail()
    );
    let recs = records(&sim, a, ra.index);
    assert!(head_writes(&recs).is_empty(), "{recs:?}");
    assert!(sim.store().keys_written_by(a, "snapshots/").is_empty());
}

/// 6.3 (m), replacing `lock_free::a_backup_with_a_gap_yields_to_a_committing_backup`:
/// a backup with a gap in its record before its commit, beside a backup
/// that is committing. On a multi-writer repository both commit (one head
/// create is taken and rebased); on a single-writer one the gap backup's
/// listing before its mark finds the other committing and it waits, then
/// commits after it.
#[test]
fn a_gap_backup_commits_beside_a_committing_backup() {
    for kind in [StoreKind::Conditional, StoreKind::Plain] {
        let mut sim = sim_of(kind);
        let (d, f) = (sim.client("d"), sim.client("f"));
        let gd = sim.gate(GateRule::new(
            d,
            OpMatch::AnyWrite,
            KeyMatch::Prefix("packs/".into()),
        ));
        let sd = sim.spawn(d, backup_call("d1"));
        sim.expect_paused(gd, "d's first pack PUT");
        sim.advance(Duration::from_secs(35 * 60));
        let gf = sim.gate(snapshot_put(f));
        let sf = sim.spawn(f, backup_call("f1"));
        sim.expect_paused(gf, "f before its snapshot PUT");
        if kind == StoreKind::Plain {
            let wait = sim.gate(sessions_listing(d, 3));
            sim.resume(gd);
            sim.expect_paused(wait, "d's second listing before its mark");
            assert!(
                !sim.store()
                    .keys_written_by(d, "sessions/")
                    .iter()
                    .any(|k| k.ends_with(".1.json") || k.ends_with(".2.json")),
                "{kind:?}: d marked beside a committing backup"
            );
            sim.resume(gf);
            sim.join(sf).expect_ok();
            sim.resume(wait);
            sim.join(sd).expect_ok();
        } else {
            sim.resume(gd);
            sim.join(sd).expect_ok();
            sim.resume(gf);
            sim.join(sf).expect_ok();
        }
        for name in ["d1", "f1"] {
            sim.restore(f, name).expect_ok();
        }
        let set = if kind.records_multi_writer() {
            Invariant::STEP8
        } else {
            Invariant::STEP7
        };
        sim.check_invariants(set).assert_clean();
    }
}

/// A6: the commit machine of a backup whose record the suspend rule
/// restarted re-checks its reused chunks on every attempt
/// (`recheck_reuse_always`, wired through `PendingCommit::begin_commit`);
/// one whose record is continuous does not.
#[test]
fn a_restarted_record_makes_every_attempt_recheck_reuse() {
    let mut sim = sim_of(StoreKind::Conditional);
    let a = sim.client("a");
    let gapped = Arc::new(AtomicBool::new(false));
    let gate = sim.gate(GateRule::new(
        a,
        OpMatch::AnyWrite,
        KeyMatch::Prefix("packs/".into()),
    ));
    let sa = sim.spawn(
        a,
        Call::BackupRecordingRecheck {
            snapshot: "a1".into(),
            tree: tree("a1"),
            rechecks: Arc::clone(&gapped),
        },
    );
    sim.expect_paused(gate, "a's first pack PUT");
    sim.advance(Duration::from_secs(35 * 60));
    sim.resume(gate);
    sim.join(sa).expect_ok();
    assert!(
        gapped.load(Ordering::SeqCst),
        "the restart did not reach the machine"
    );
    let continuous = Arc::new(AtomicBool::new(true));
    sim.call(
        a,
        Call::BackupRecordingRecheck {
            snapshot: "a2".into(),
            tree: tree("a2"),
            rechecks: Arc::clone(&continuous),
        },
    )
    .expect_ok();
    assert!(!continuous.load(Ordering::SeqCst));
}

// ── Maintenance re-reads config after its registration (A1) ────────────

/// A gate on `c`'s registration: its first marker key PUT.
fn registration(c: ClientId) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::Put),
        KeyMatch::Prefix("sessions/".into()),
    )
}

/// A compact that opened a single-writer repository and registers after
/// the repository was switched to multi-writer (between its open and its
/// registration, where neither side would see the other) runs in the new
/// mode: its re-read after the registration takes multi-writer, so its
/// heads are put-if-absent and it makes no single-writer check.
#[test]
fn a_compact_registered_after_a_switch_to_multi_writer_runs_multi_writer() {
    let mut sim = sim_of(StoreKind::Conditional);
    let (c, d) = (sim.client("c"), sim.client("d"));
    sim.backup(d, "d0", &tree("d0")).expect_ok();
    sim.call(
        d,
        Call::SwitchMode {
            mode: WriterMode::Single,
        },
    )
    .expect_ok();
    let gc = sim.gate(registration(c));
    let sc = sim.spawn(c, Call::Compact);
    sim.expect_paused(gc, "c's registration, after its open");
    sim.call(
        d,
        Call::SwitchMode {
            mode: WriterMode::Multi,
        },
    )
    .expect_ok();
    sim.resume(gc);
    let rc = sim.join(sc);
    rc.expect_ok();
    let recs = records(&sim, c, rc.index);
    let gets = recs
        .iter()
        .filter(|r| r.op == OpKind::Get && r.key == "config")
        .count();
    assert_eq!(gets, 2, "the open's and the re-read's config GET: {recs:?}");
    let heads = head_writes(&recs);
    assert!(!heads.is_empty(), "{recs:?}");
    assert!(
        heads.iter().all(|r| r.op == OpKind::PutIfAbsent),
        "{heads:?}"
    );
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// The other direction: a compact that opened a multi-writer repository
/// and registers after a switch to single-writer makes the single-writer
/// check and writes its heads with EXISTS, plain PUT and read-back.
#[test]
fn a_compact_registered_after_a_switch_to_single_writer_runs_single_writer() {
    let mut sim = sim_of(StoreKind::Conditional);
    let (c, d) = (sim.client("c"), sim.client("d"));
    sim.backup(d, "d0", &tree("d0")).expect_ok();
    let gc = sim.gate(registration(c));
    let sc = sim.spawn(c, Call::Compact);
    sim.expect_paused(gc, "c's registration, after its open");
    sim.call(
        d,
        Call::SwitchMode {
            mode: WriterMode::Single,
        },
    )
    .expect_ok();
    sim.resume(gc);
    let rc = sim.join(sc);
    rc.expect_ok();
    let recs = records(&sim, c, rc.index);
    let heads = head_writes(&recs);
    assert!(!heads.is_empty());
    assert!(heads.iter().all(|r| r.op == OpKind::Put), "{heads:?}");
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// A compact of a client whose storage offers no put-if-absent, opened on
/// a single-writer repository, registered after a switch to multi-writer:
/// the re-read takes multi-writer and the client refuses
/// (`MultiWriterUnsupported`), having written nothing but its marker.
#[test]
fn a_compact_registered_after_a_switch_it_cannot_follow_refuses() {
    let mut sim = sim_of(StoreKind::Conditional);
    let (u, d) = (sim.client("u"), sim.client("d"));
    sim.set_client_without_conditional(u);
    sim.backup(d, "d0", &tree("d0")).expect_ok();
    sim.call(
        d,
        Call::SwitchMode {
            mode: WriterMode::Single,
        },
    )
    .expect_ok();
    let gu = sim.gate(registration(u));
    let su = sim.spawn(u, Call::Compact);
    sim.expect_paused(gu, "u's registration, after its open");
    sim.call(
        d,
        Call::SwitchMode {
            mode: WriterMode::Multi,
        },
    )
    .expect_ok();
    sim.resume(gu);
    let ru = sim.join(su);
    assert!(
        ru.detail().contains("multi-writer") && !ru.is_ok(),
        "{}",
        ru.detail()
    );
    let wrote: Vec<OpRecord> = records(&sim, u, ru.index)
        .into_iter()
        .filter(|r| r.op.is_mutation() && !r.key.starts_with("sessions/"))
        .collect();
    assert!(wrote.is_empty(), "{wrote:?}");
}
