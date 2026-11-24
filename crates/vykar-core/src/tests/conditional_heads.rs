//! Index heads created with put-if-absent (04 step 8, phase F part 9): the
//! create, a lost reply, a taken generation and the backup's retry, the
//! fallback of a backend that ignores the condition, the index garbage
//! collection's claim, and invariant 12 of the simulator's checker. On the
//! simulator's conditional store (`StoreKind::Conditional`) unless named.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::compress::Compression;
use crate::index::head::IndexHead;
use crate::repo::control::CommitEvent;
use crate::repo::host_env::{Clock, HostEnv, StopSignal};
use crate::repo::index_store;
use crate::repo::machine::{Machine, StepCx, StepKind};
use crate::repo::pack::PackType;
use crate::repo::{BackupCommit, OpenOptions, Repository, WriteSessionOptions};
use crate::sim::checker::open_observer;
use crate::sim::history::{AllowedError, ErrorClass};
use crate::sim::{
    Call, ClientId, ClientOp, FaultAction, FaultRule, GateRule, Invariant, KeyMatch, OpKind,
    OpMatch, OpOutcome, OpRecord, Sim, SimConfig, SourceTree, StoreKind, ViolationKind,
};
use vykar_types::error::VykarError;

const FILE_SIZE: usize = 16 * 1024;

fn sim_of(kind: StoreKind) -> Sim {
    Sim::with_config(SimConfig {
        kind,
        ..SimConfig::default()
    })
}

fn tree(name: &str) -> SourceTree {
    SourceTree::seeded(name, 2, FILE_SIZE)
}

/// Invariants 1 to 4 and 12 (and 8): what a conditional store keeps while a
/// lock-exclusion violation (7) is expected.
const NOT_LOCK: &[Invariant] = &[
    Invariant::CommittedSnapshotsRestore,
    Invariant::AcknowledgedCommitsDurable,
    Invariant::NoSilentRemoval,
    Invariant::IndexIntegrity,
    Invariant::AllowedErrors,
    Invariant::HeadsImmutable,
];

/// The records of `client`'s run `run`.
fn records(sim: &Sim, client: ClientId, run: usize) -> Vec<OpRecord> {
    sim.store()
        .log()
        .into_iter()
        .filter(|r| r.client == client && r.run == Some(run))
        .collect()
}

fn head_creates(records: &[OpRecord]) -> Vec<&OpRecord> {
    records
        .iter()
        .filter(|r| r.op == OpKind::PutIfAbsent && r.key.starts_with("index.h/"))
        .collect()
}

/// The latest head, decoded through the observer.
fn latest_head(sim: &Sim) -> IndexHead {
    let tmp = tempfile::tempdir().unwrap();
    let repo = open_observer(sim.store(), tmp.path()).unwrap();
    index_store::fetch_head(repo.storage.as_ref(), repo.crypto.as_ref()).unwrap()
}

/// `head`'s state at generation `generation`, packed: a valid head another
/// writer could have created there.
fn planted(sim: &Sim, head: &IndexHead, generation: u64) -> Vec<u8> {
    let tmp = tempfile::tempdir().unwrap();
    let repo = open_observer(sim.store(), tmp.path()).unwrap();
    let mut h = head.clone();
    h.generation = generation;
    index_store::pack_head(&h, repo.crypto.as_ref()).unwrap()
}

// ── The create ──────────────────────────────────────────────────────────

/// A backup creates its head with one put-if-absent; the run's first create
/// runs the check (two creates, one GET and one DELETE of a probe under
/// `locks/`), which leaves nothing behind.
#[test]
fn backup_creates_its_head() {
    let mut sim = sim_of(StoreKind::Conditional);
    let a = sim.client("a");
    let run = sim.backup(a, "a1", &tree("a1"));
    run.expect_ok();
    let recs = records(&sim, a, run.index);
    let creates = head_creates(&recs);
    assert_eq!(creates.len(), 1, "{creates:?}");
    assert_eq!(creates[0].key, "index.h/2");
    assert_eq!(creates[0].outcome, OpOutcome::Ok);
    assert!(
        !recs
            .iter()
            .any(|r| r.op == OpKind::Put && r.key.starts_with("index.h/")),
        "no plain head PUT"
    );
    let probe = |op: OpKind| {
        recs.iter()
            .filter(|r| r.op == op && r.key.starts_with("locks/vykar-probe-"))
            .count()
    };
    assert_eq!(probe(OpKind::PutIfAbsent), 2);
    assert_eq!(probe(OpKind::Get), 1);
    assert_eq!(probe(OpKind::Delete), 1);
    assert!(sim.store().keys("locks/vykar-probe-").is_empty());
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// A create whose reply is lost (applied, then an error) is recognised by
/// the bytes the key holds: the backup commits.
#[test]
fn lost_reply_is_recognised_by_the_bytes() {
    let mut sim = sim_of(StoreKind::Conditional);
    let a = sim.client("a");
    sim.fault(
        FaultRule::new(
            OpMatch::Kind(OpKind::PutIfAbsent),
            KeyMatch::Prefix("index.h/".into()),
            FaultAction::FailApplied,
        )
        .client(a),
    );
    let run = sim.backup(a, "a1", &tree("a1"));
    run.expect_ok();
    let recs = records(&sim, a, run.index);
    assert!(
        recs.iter()
            .any(|r| r.op == OpKind::Get && r.key == "index.h/2" && r.outcome == OpOutcome::Ok),
        "the create's GET"
    );
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// A create with an unknown outcome whose key is absent fails the commit
/// with the error. Landing before any other commit it extends the head it
/// was built on (no snapshot: a leak only; its late write is invariant 7's);
/// landing after another commit it finds the generation taken.
#[test]
fn unknown_outcome_fails_and_a_late_create_is_harmless() {
    for other_first in [false, true] {
        let mut sim = sim_of(StoreKind::Conditional);
        let (a, b) = (sim.client("a"), sim.client("b"));
        sim.fault(
            FaultRule::new(
                OpMatch::Kind(OpKind::PutIfAbsent),
                KeyMatch::Prefix("index.h/".into()),
                FaultAction::Delay,
            )
            .client(a),
        );
        let run = sim.backup(a, "a1", &tree("a1"));
        assert_eq!(
            run.error_class(),
            Some(&ErrorClass::Allowed(AllowedError::Injected)),
            "{}",
            run.detail()
        );
        if other_first {
            sim.backup(b, "b1", &tree("b1")).expect_ok();
            sim.release_all();
            let late = sim.store().log().last().cloned().unwrap();
            assert!(
                matches!(late.outcome, OpOutcome::ReleasedRefused(_)),
                "{late:?}"
            );
            sim.check_invariants(Invariant::STEP8).assert_clean();
        } else {
            sim.release_all();
            let late = sim.store().log().last().cloned().unwrap();
            assert!(matches!(late.outcome, OpOutcome::Released(_)), "{late:?}");
            sim.backup(b, "b1", &tree("b1")).expect_ok();
            assert_eq!(latest_head(&sim).generation, 3);
            sim.check_invariants(NOT_LOCK).assert_clean();
        }
    }
}

/// A backend that ignores the condition (Garage) fails the check; heads
/// fall back to plain PUTs, and the backup commits. The warning itself is
/// observed in `the_fallback_is_noted_once_per_handle`.
#[test]
fn ignoring_backend_falls_back_with_a_warning() {
    let mut sim = sim_of(StoreKind::Ignoring);
    let a = sim.client("a");
    for name in ["a1", "a2"] {
        let run = sim.backup(a, name, &tree(name));
        run.expect_ok();
        let recs = records(&sim, a, run.index);
        assert!(head_creates(&recs).is_empty(), "{recs:?}");
        assert_eq!(
            recs.iter()
                .filter(|r| r.op == OpKind::Put && r.key.starts_with("index.h/"))
                .count(),
            1
        );
        // Each run (process) checks once.
        assert_eq!(
            recs.iter()
                .filter(|r| r.op == OpKind::PutIfAbsent && r.key.starts_with("locks/"))
                .count(),
            2
        );
    }
    assert!(sim.store().keys("locks/vykar-probe-").is_empty());
    sim.check_invariants(Invariant::STEP7).assert_clean();
}

/// Two lock holders (the H2 route: a lock key re-created after another
/// client took the lock, modelled by tampering) cannot lose a head: the
/// holder whose create finds its generation taken rebases. Invariants 1-4
/// and 12 hold; 7 fails, as it must with two holders. The clients back up
/// different content (identical plaintext states would give identical
/// heads, which both may count as created).
#[test]
fn two_lock_holders_cannot_lose_a_head() {
    let mut sim = sim_of(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    let gate = sim.gate(GateRule::new(
        a,
        OpMatch::Kind(OpKind::PutIfAbsent),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let sa = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: tree("a1"),
        },
    );
    sim.expect_paused(gate, "a's head create");
    let lock = sim
        .store()
        .keys("locks/")
        .into_iter()
        .find(|k| k.ends_with(".json"))
        .expect("a holds the lock");
    let bytes = sim.store().object(&lock).unwrap().data;
    sim.tamper_delete(&lock);
    let sb = sim.spawn(
        b,
        Call::Backup {
            snapshot: "b1".into(),
            tree: tree("b1"),
        },
    );
    sim.join(sb).expect_ok();
    sim.tamper_put(&lock, &bytes);
    sim.resume(gate);
    let ra = sim.join(sa);
    ra.expect_ok();
    let creates = head_creates(&records(&sim, a, ra.index))
        .into_iter()
        .map(|r| (r.key.clone(), r.outcome.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        creates,
        vec![
            ("index.h/2".to_string(), OpOutcome::Exists),
            ("index.h/3".to_string(), OpOutcome::Ok)
        ]
    );
    sim.check_invariants(NOT_LOCK).assert_clean();
    assert!(
        sim.check_invariants(&[Invariant::LockExclusion])
            .violated(Invariant::LockExclusion)
    );
}

/// Compact's repack batch finds its head's generation taken: compact stops
/// with "index changed" before deleting the batch's sources, whose repack
/// outputs are then orphan packs collected by age.
#[test]
fn maintenance_head_taken_stops_before_deletes() {
    let mut sim = sim_of(StoreKind::Conditional);
    let c = sim.client("c");
    let x = SourceTree::seeded("x", 3, 64 * 1024);
    sim.backup(c, "t0", &x).expect_ok();
    let t1 = SourceTree::new().file(
        "f000.bin",
        crate::sim::runner::seeded_bytes("x/0", 64 * 1024),
    );
    sim.backup(c, "t1", &t1).expect_ok();
    sim.delete(c, &["t0"]).expect_ok();
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
    // The first head create is the sweep's, the second the repack batch's.
    let gate = sim.gate(
        GateRule::new(
            c,
            OpMatch::Kind(OpKind::PutIfAbsent),
            KeyMatch::Prefix("index.h/".into()),
        )
        .nth(2),
    );
    let run = sim.spawn(c, Call::Compact);
    let held = sim.expect_paused(gate, "the repack batch's head create");
    let generation = vykar_protocol::head_generation(&held.key).unwrap();
    let head = latest_head(&sim);
    assert_eq!(head.generation + 1, generation);
    let bytes = planted(&sim, &head, generation);
    sim.tamper_put(&held.key, &bytes);
    sim.resume(gate);
    let run = sim.join(run);
    assert_eq!(
        run.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::IndexChanged)),
        "{}",
        run.detail()
    );
    let recs = records(&sim, c, run.index);
    let taken_at = recs
        .iter()
        .find(|r| r.key == held.key && r.outcome == OpOutcome::Exists)
        .expect("the taken create")
        .step;
    assert!(
        !recs
            .iter()
            .any(|r| r.step > taken_at && r.deleted_keys().iter().any(|k| k.starts_with("packs/"))),
        "no pack deleted after the taken create"
    );
    let outputs: Vec<String> = recs
        .iter()
        .filter(|r| r.op.is_write() && r.applied() && r.key.starts_with("packs/"))
        .map(|r| r.key.clone())
        .collect();
    assert!(!outputs.is_empty(), "the batch repacked");
    sim.check_invariants(Invariant::STEP8).assert_clean();
    // The repack outputs no head names are orphans: kept while young; once
    // T_pack old, collected unless a later repack wrote the same pack
    // again (plaintext packs are named by their content) and the index
    // names it.
    sim.compact(c).expect_ok();
    assert!(outputs.iter().all(|k| sim.store().object(k).is_some()));
    sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
    sim.compact(c).expect_ok();
    let tmp = tempfile::tempdir().unwrap();
    let mut observer = open_observer(sim.store(), tmp.path()).unwrap();
    observer.load_chunk_index_uncached().unwrap();
    let named: std::collections::BTreeSet<String> = observer
        .chunk_index()
        .iter()
        .map(|(_, e)| e.pack_id.storage_key())
        .collect();
    for k in &outputs {
        assert!(
            sim.store().object(k).is_none() || named.contains(k),
            "{k} is an orphan older than T_pack"
        );
    }
    sim.check_invariants(Invariant::STEP8).assert_clean();
}

/// On a local repository (LocalBackend: no-clobber rename) a backup creates
/// its head and its check leaves no probe under `locks/`.
#[test]
fn local_backup_leaves_no_probe() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let config = crate::tests::helpers::init_repo(&repo_dir);
    let source = tmp.path().join("src");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(source.join("a"), "alpha").unwrap();
    crate::tests::helpers::backup_single_source(&config, &source, "src", "s1");
    let locks: Vec<String> = std::fs::read_dir(repo_dir.join("locks"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !locks.iter().any(|n| n.starts_with("vykar-probe-")),
        "{locks:?}"
    );
    assert!(
        repo_dir
            .join("index.h")
            .join(vykar_protocol::head_key(2).trim_start_matches("index.h/"))
            .exists()
    );
}

// ── Repository-level: a backup's commit machine stepped by hand ─────────

/// A clock whose monotonic time can be frozen while virtual time moves on:
/// a suspended machine, whose uptime (`CLOCK_BOOTTIME`) keeps counting.
struct Suspendable {
    inner: Arc<dyn Clock>,
    frozen: Mutex<Option<Instant>>,
}

impl Clock for Suspendable {
    fn now_utc(&self) -> chrono::DateTime<chrono::Utc> {
        self.inner.now_utc()
    }
    fn monotonic(&self) -> Instant {
        self.frozen
            .lock()
            .unwrap()
            .unwrap_or_else(|| self.inner.monotonic())
    }
    fn sleep(&self, d: Duration) {
        self.inner.sleep(d);
    }
    fn wait_timeout(&self, signal: &StopSignal, timeout: Duration) -> bool {
        self.inner.wait_timeout(signal, timeout)
    }
    fn uptime(&self) -> Option<Duration> {
        self.inner.uptime()
    }
}

/// A handle of `c` on the simulator's store, with the index loaded, outside
/// any run (no lock, no session marker).
fn open(sim: &Sim, c: ClientId, env: HostEnv) -> Repository {
    Repository::open(
        Box::new(sim.store().handle(c)),
        None,
        Some(sim.client_dir(c).join("repo-cache")),
        OpenOptions::new().with_index().host_env(env),
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

/// A session of `repo` storing `chunks`, and its commit machine.
fn session(repo: &mut Repository, name: &str, chunks: &[&[u8]]) -> BackupCommit {
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    for c in chunks {
        repo.store_chunk(c, Compression::None, PackType::Data)
            .unwrap();
    }
    BackupCommit::new(
        repo,
        entry(name),
        b"snapshot".to_vec(),
        None,
        crate::repo::CommitMode::Lock,
    )
}

fn no_events(_: CommitEvent) {}

/// Step `m` (pick 0) until `stop` accepts its enabled steps or it ends;
/// returns the steps taken.
fn step_until(
    repo: &mut Repository,
    m: &mut BackupCommit,
    stop: impl Fn(&[StepKind]) -> bool,
) -> vykar_types::error::Result<Vec<StepKind>> {
    let mut taken = Vec::new();
    loop {
        let enabled = m.enabled();
        if enabled.is_empty() || stop(&enabled) {
            return Ok(taken);
        }
        taken.push(enabled[0]);
        m.step(
            &mut StepCx {
                repo,
                events: &no_events,
            },
            0,
        )?;
    }
}

fn at_put_head(enabled: &[StepKind]) -> bool {
    enabled == [StepKind::PutHead]
}

/// Commit a whole session of `repo` (no lock), finishing it.
fn commit_whole(repo: &mut Repository, name: &str, chunks: &[&[u8]]) {
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    for c in chunks {
        repo.store_chunk(c, Compression::None, PackType::Data)
            .unwrap();
    }
    let mut file_cache = crate::repo::file_cache::FileCache::new();
    repo.commit_session_blocking(
        entry(name),
        b"snapshot".to_vec(),
        &mut file_cache,
        &no_events,
    )
    .unwrap();
}

/// The ops `client` issued after store step `from` (outside runs).
fn ops_after(sim: &Sim, client: ClientId, from: u64) -> Vec<OpRecord> {
    sim.store()
        .log()
        .into_iter()
        .filter(|r| r.client == client && r.step > from)
        .collect()
}

/// A backup whose generation another writer created rebases: it backs
/// off, repeats the start reads, re-syncs (the other's backup segment needs
/// no reuse re-check), verifies no pack again and creates the next
/// generation.
#[test]
fn taken_generation_rebases_and_commits() {
    let mut sim = sim_of(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    let mut ra = open(&sim, a, sim.host_env(a));
    let mut rb = open(&sim, b, sim.host_env(b));
    let mut ma = session(&mut ra, "a1", &[b"chunk of a"]);
    step_until(&mut ra, &mut ma, at_put_head).unwrap();
    commit_whole(&mut rb, "b1", &[b"chunk of b"]);
    let before = sim.store().step();
    let steps = step_until(&mut ra, &mut ma, |_| false).unwrap();
    assert!(ma.take_output().is_some());
    assert_eq!(steps.first(), Some(&StepKind::PutHead));
    assert_eq!(steps.get(1), Some(&StepKind::Backoff), "{steps:?}");
    assert!(steps.contains(&StepKind::RefreshSnapshots));
    assert_eq!(
        steps.iter().filter(|s| **s == StepKind::PutHead).count(),
        2,
        "{steps:?}"
    );
    let ops = ops_after(&sim, a, before);
    let creates: Vec<(String, OpOutcome)> = head_creates(&ops)
        .into_iter()
        .map(|r| (r.key.clone(), r.outcome.clone()))
        .collect();
    assert_eq!(
        creates,
        vec![
            ("index.h/2".to_string(), OpOutcome::Exists),
            ("index.h/3".to_string(), OpOutcome::Ok)
        ]
    );
    assert!(
        !ops.iter().any(|r| r.key.starts_with("packs/")),
        "the retry verified no pack again: {ops:?}"
    );
    let head = latest_head(&sim);
    assert_eq!(head.generation, 3);
    assert_eq!(head.segments.len(), 2);
    for name in head.named_objects() {
        assert!(
            sim.store()
                .object(&index_store::object_key(&name))
                .is_some()
        );
    }
    assert!(
        sim.check_invariants(&[Invariant::HeadsImmutable, Invariant::IndexIntegrity])
            .is_clean()
    );
}

/// A backup whose generation a maintenance write took, which removed a
/// chunk the backup reused: the retry re-checks reuse (a maintenance
/// segment since the session began) and fails `StaleChunksDuringCommit`;
/// nothing of it is published.
#[test]
fn taken_generation_after_a_sweep_rechecks_reuse() {
    let mut sim = sim_of(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    let mut rb = open(&sim, b, sim.host_env(b));
    commit_whole(&mut rb, "b0", &[b"shared chunk"]);
    let mut ra = open(&sim, a, sim.host_env(a));
    let mut ma = session(&mut ra, "a1", &[b"shared chunk", b"own chunk of a"]);
    step_until(&mut ra, &mut ma, at_put_head).unwrap();
    // b removes the shared chunk and publishes a maintenance segment.
    let mut rb = open(&sim, b, sim.host_env(b));
    let id = vykar_types::chunk_id::ChunkId::compute(rb.crypto.chunk_hasher(), b"shared chunk");
    assert!(rb.chunk_index_mut().remove(&id).is_some());
    rb.save_state().unwrap();
    let generation = latest_head(&sim).generation;
    let snapshots = sim.store().keys("snapshots/");
    let err = step_until(&mut ra, &mut ma, |_| false).unwrap_err();
    assert!(matches!(err, VykarError::StaleChunksDuringCommit), "{err}");
    assert_eq!(latest_head(&sim).generation, generation);
    assert_eq!(sim.store().keys("snapshots/"), snapshots);
}

/// A generation taken on each of eight attempts: the backup gives up with
/// the "index changed" error, which names the attempts.
#[test]
fn contention_gives_up_after_eight_attempts() {
    let mut sim = sim_of(StoreKind::Conditional);
    let a = sim.client("a");
    let mut ra = open(&sim, a, sim.host_env(a));
    let mut ma = session(&mut ra, "a1", &[b"chunk of a"]);
    let mut planted_heads = 0;
    let err = loop {
        if let Err(e) = step_until(&mut ra, &mut ma, at_put_head) {
            break e;
        }
        assert!(!ma.enabled().is_empty(), "committed despite contention");
        let head = latest_head(&sim);
        let next = head.generation + 1;
        sim.tamper_put(&index_store::head_key(next), &planted(&sim, &head, next));
        planted_heads += 1;
        if let Err(e) = ma.step(
            &mut StepCx {
                repo: &mut ra,
                events: &no_events,
            },
            0,
        ) {
            break e;
        }
    };
    assert_eq!(planted_heads, 8);
    let text = err.to_string();
    assert!(
        text.contains(crate::sim::history::INDEX_CHANGED_TEXT),
        "{text}"
    );
    assert!(text.contains("8 attempts"), "{text}");
    assert_eq!(
        crate::sim::history::classify(&err),
        ErrorClass::Allowed(AllowedError::IndexChanged)
    );
    assert!(sim.store().keys("snapshots/").is_empty());
}

/// A create slower than T_idx − M after the listing that chose its
/// generation lists `index.h/` once: a recent generation is created. With
/// the machine suspended in between (monotonic time frozen, uptime
/// counting) the same: the time is measured suspend included.
#[test]
fn slow_create_checks_the_generation() {
    for suspended in [false, true] {
        let mut sim = sim_of(StoreKind::Conditional);
        let a = sim.client("a");
        let base = sim.host_env(a);
        let clock = Arc::new(Suspendable {
            inner: Arc::clone(&base.clock),
            frozen: Mutex::new(None),
        });
        let env = HostEnv {
            clock: clock.clone(),
            ..base
        };
        let mut ra = open(&sim, a, env);
        let mut ma = session(&mut ra, "a1", &[b"chunk of a"]);
        step_until(&mut ra, &mut ma, at_put_head).unwrap();
        if suspended {
            *clock.frozen.lock().unwrap() = Some(clock.inner.monotonic());
        }
        sim.advance(Duration::from_secs(51 * 60));
        let before = sim.store().step();
        step_until(&mut ra, &mut ma, |_| false).unwrap();
        assert!(ma.take_output().is_some());
        let ops = ops_after(&sim, a, before);
        let create = ops
            .iter()
            .position(|r| r.op == OpKind::PutIfAbsent && r.key == "index.h/2")
            .expect("the create");
        assert_eq!(
            ops.get(create + 1).map(|r| (r.op, r.key.as_str())),
            Some((OpKind::List, "index.h/")),
            "suspended {suspended}: {ops:?}"
        );
        assert_eq!(latest_head(&sim).generation, 2);
    }
}

/// D10: a slow create re-makes a generation another writer created after
/// this writer's listing and the garbage collection deleted since. The
/// listing after the create shows it more than `HEADS_KEPT` below the
/// latest: taken; the re-made head is deleted again and the backup rebases
/// onto the latest.
#[test]
fn recreated_old_generation_counts_as_taken() {
    let mut sim = sim_of(StoreKind::Conditional);
    let (a, b) = (sim.client("a"), sim.client("b"));
    let mut ra = open(&sim, a, sim.host_env(a));
    let mut ma = session(&mut ra, "a1", &[b"chunk of a"]);
    step_until(&mut ra, &mut ma, at_put_head).unwrap();
    let mut rb = open(&sim, b, sim.host_env(b));
    commit_whole(&mut rb, "b0", &[b"chunk b0"]);
    sim.advance(crate::repo::ages::T_IDX + Duration::from_secs(60));
    for i in 1..=19u32 {
        let chunk = format!("chunk b{i}");
        if i == 19 {
            // A fold collects garbage; every segment is young, so a full one.
            rb.request_full_index_fold();
        }
        commit_whole(&mut rb, &format!("b{i}"), &[chunk.as_bytes()]);
    }
    assert!(sim.store().object("index.h/2").is_none(), "collected");
    let latest = latest_head(&sim).generation;
    let before = sim.store().step();
    let steps = step_until(&mut ra, &mut ma, |_| false).unwrap();
    assert!(ma.take_output().is_some());
    assert_eq!(steps.get(1), Some(&StepKind::Backoff), "{steps:?}");
    let ops = ops_after(&sim, a, before);
    assert!(
        ops.iter()
            .any(|r| r.op == OpKind::Delete && r.key == "index.h/2"),
        "the re-made head deleted: {ops:?}"
    );
    assert!(sim.store().object("index.h/2").is_none());
    assert_eq!(latest_head(&sim).generation, latest + 1);
    let report = sim.check_invariants(&[Invariant::HeadsImmutable, Invariant::IndexIntegrity]);
    assert!(report.is_clean(), "{report}");
}

/// A fold whose listing of `index.d/` lacks a backup segment the head names
/// re-checks it: absent, the write is refused (`IndexObjectMissing`) rather
/// than publishing a chain no cold client can load (forward note 8 of 8c).
#[test]
fn a_named_segment_the_store_lacks_refuses_the_fold() {
    let mut sim = sim_of(StoreKind::Conditional);
    let a = sim.client("a");
    let mut ra = open(&sim, a, sim.host_env(a));
    commit_whole(&mut ra, "a1", &[b"chunk a1"]);
    commit_whole(&mut ra, "a2", &[b"chunk a2"]);
    let head = latest_head(&sim);
    let lost = index_store::object_key(&head.segments[0].name);
    sim.tamper_delete(&lost);
    // "Now" on the store's clock, as a held lock gives it.
    ra.set_lock_store_time(Some(vykar_storage::StoreNow::at(sim.store().now().into())));
    ra.request_index_fold();
    let mut m = session(&mut ra, "a3", &[b"chunk a3"]);
    let err = step_until(&mut ra, &mut m, |_| false).unwrap_err();
    assert!(
        matches!(&err, VykarError::IndexObjectMissing { key, .. } if *key == lost),
        "{err}"
    );
    assert_eq!(latest_head(&sim).generation, head.generation);
}

/// Phase F part 10b2: an index write whose first object PUT started more
/// than `INDEX_WRITE_LIMIT` − `HEAD_LANDING_ALLOWANCE` (11 h 20 min) before
/// its head create refuses the create (its objects may be collected before
/// the head lands); a minute less still commits. A retry with fresh objects
/// commits.
#[test]
fn an_index_write_older_than_the_object_grace_is_refused() {
    use crate::repo::ages::{HEAD_LANDING_ALLOWANCE, INDEX_WRITE_LIMIT};
    let edge = INDEX_WRITE_LIMIT - HEAD_LANDING_ALLOWANCE;
    assert_eq!(edge, Duration::from_secs(11 * 3600 + 20 * 60));
    let minute = Duration::from_secs(60);
    for (stall, refused) in [(edge - minute, false), (edge + minute, true)] {
        let mut sim = sim_of(StoreKind::Conditional);
        let a = sim.client("a");
        let mut ra = open(&sim, a, sim.host_env(a));
        commit_whole(&mut ra, "a0", &[b"chunk a0"]);
        let before = latest_head(&sim);
        let mut m = session(&mut ra, "a1", &[b"chunk a1"]);
        step_until(&mut ra, &mut m, at_put_head).unwrap();
        sim.advance(stall);
        let from = sim.store().step();
        let result = step_until(&mut ra, &mut m, |_| false);
        if !refused {
            result.unwrap();
            assert!(m.take_output().is_some());
            assert_eq!(latest_head(&sim).generation, before.generation + 1);
            continue;
        }
        let err = result.unwrap_err();
        assert_eq!(
            crate::sim::history::classify(&err),
            ErrorClass::Allowed(AllowedError::SlowIndexWrite),
            "{err}"
        );
        let creates: Vec<OpRecord> = ops_after(&sim, a, from)
            .into_iter()
            .filter(|r| {
                r.key.starts_with("index.h/") && r.op != OpKind::Get && r.op != OpKind::List
            })
            .collect();
        assert!(creates.is_empty(), "no head written: {creates:?}");
        assert_eq!(latest_head(&sim), before);
        let mut retry = open(&sim, a, sim.host_env(a));
        commit_whole(&mut retry, "a2", &[b"chunk a2"]);
        assert_eq!(latest_head(&sim).generation, before.generation + 1);
    }
}

/// Phase F part 10b2: a head create that returns more than
/// `INDEX_WRITE_LIMIT` (11 h 50 min) after the write's first object PUT
/// started fails the backup before its snapshot: the head is created, but
/// a collector may have judged its objects old before it landed.
#[test]
fn a_head_created_after_the_object_grace_fails_the_backup() {
    let mut sim = sim_of(StoreKind::Conditional);
    let a = sim.client("a");
    sim.backup(a, "a0", &tree("a0")).expect_ok();
    let before = latest_head(&sim);
    let gate = sim.gate(GateRule::new(
        a,
        OpMatch::Kind(OpKind::PutIfAbsent),
        KeyMatch::Prefix("index.h/".into()),
    ));
    let run = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: tree("a1"),
        },
    );
    sim.expect_paused(gate, "a's head create");
    sim.advance(crate::repo::ages::INDEX_WRITE_LIMIT + Duration::from_secs(60));
    sim.resume(gate);
    let run = sim.join(run);
    assert_eq!(
        run.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::SlowIndexWrite)),
        "{}",
        run.detail()
    );
    assert_eq!(latest_head(&sim).generation, before.generation + 1);
    let recs = records(&sim, a, run.index);
    assert!(
        !recs.iter().any(|r| r.key.starts_with("snapshots/")
            && r.op != OpKind::List
            && r.op != OpKind::Get),
        "no snapshot written: {recs:?}"
    );
    assert!(
        run.detail()
            .contains("written without its snapshot; the next run continues from it"),
        "{}",
        run.detail()
    );
    let report = sim.check_invariants(&[Invariant::HeadsImmutable, Invariant::IndexIntegrity]);
    assert!(report.is_clean(), "{report}");
    // The next run continues from the snapshot-less head.
    sim.restore(a, "a0").expect_ok();
    sim.backup(a, "a2", &tree("a2")).expect_ok();
    assert_eq!(latest_head(&sim).generation, before.generation + 2);
    sim.restore(a, "a2").expect_ok();
    sim.check_invariants(&[Invariant::HeadsImmutable, Invariant::IndexIntegrity])
        .assert_clean();
}

/// The index garbage collection runs only right after its caller created
/// the latest head: a handle that wrote no head deletes no old unreferenced
/// object; after its own head write it does.
#[test]
fn gc_needs_the_callers_own_head() {
    let mut sim = sim_of(StoreKind::Conditional);
    let a = sim.client("a");
    let junk = format!("index.d/{}", "ab".repeat(32));
    sim.tamper_put(&junk, b"unreferenced");
    sim.advance(index_store::INDEX_OBJECT_GRACE + Duration::from_secs(60));
    let mut ra = open(&sim, a, sim.host_env(a));
    ra.maintain_segmented_index(crate::repo::index_write::OnMoved::Fail)
        .unwrap();
    assert!(sim.store().object(&junk).is_some(), "no head of its own");
    ra.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    ra.store_chunk(b"chunk of a", Compression::None, PackType::Data)
        .unwrap();
    ra.save_state().unwrap();
    ra.maintain_segmented_index(crate::repo::index_write::OnMoved::Fail)
        .unwrap();
    assert!(
        sim.store().object(&junk).is_none(),
        "collected after its head"
    );
}

/// The fallback notice (04 "Fallback", never silently unconditional): a
/// handle on a store that ignores the condition notes it at warn on its
/// first index write and not again; another handle on the same store notes
/// it again (once per opened repository, not per process). A backend
/// without put-if-absent from the start is noted at info; a store that
/// passes the check notes nothing.
#[test]
fn the_fallback_is_noted_once_per_handle() {
    use crate::repo::index_store::HeadFallback;
    let cases = [
        (StoreKind::Ignoring, Some(HeadFallback::Refused)),
        (StoreKind::Plain, Some(HeadFallback::NotOffered)),
        (StoreKind::Conditional, None),
    ];
    for (kind, want) in cases {
        let mut sim = sim_of(kind);
        let (a, b) = (sim.client("a"), sim.client("b"));
        let mut ra = open(&sim, a, sim.host_env(a));
        assert_eq!(ra.head_fallback(), None, "{kind:?}: before any write");
        commit_whole(&mut ra, "a1", &[b"chunk a1"]);
        assert_eq!(ra.head_fallback(), want, "{kind:?}");
        commit_whole(&mut ra, "a2", &[b"chunk a2"]);
        assert_eq!(ra.head_fallback(), want, "{kind:?}: kept");
        let mut rb = open(&sim, b, sim.host_env(b));
        assert_eq!(rb.head_fallback(), None, "{kind:?}: a new handle");
        commit_whole(&mut rb, "b1", &[b"chunk b1"]);
        assert_eq!(rb.head_fallback(), want, "{kind:?}: noted by b too");
    }
}

/// The handle `handle` of `c`, a warm reopen (the same cache directory)
/// and a cold open (an empty one) agree on the latest head and the chunk
/// index, and each chunk of `chunks` reads back through the warm and the
/// cold handle.
fn index_agrees(sim: &Sim, c: ClientId, handle: &Repository, chunks: &[&[u8]]) {
    let head = latest_head(sim);
    let warm = open(sim, c, sim.host_env(c));
    let cold_dir = tempfile::tempdir().unwrap();
    let cold = Repository::open(
        Box::new(sim.store().handle(c)),
        None,
        Some(cold_dir.path().to_path_buf()),
        OpenOptions::new().with_index().host_env(sim.host_env(c)),
    )
    .unwrap();
    for (what, r) in [("handle", handle), ("warm", &warm), ("cold", &cold)] {
        assert_eq!(r.index_head(), Some(&head), "{what}");
        assert_eq!(r.chunk_index().len(), handle.chunk_index().len(), "{what}");
    }
    for chunk in chunks {
        let id = vykar_types::chunk_id::ChunkId::compute(handle.crypto.chunk_hasher(), chunk);
        let entry = handle.chunk_index().get(&id).expect("indexed");
        for (what, r) in [("warm", &warm), ("cold", &cold)] {
            assert_eq!(r.chunk_index().get(&id), Some(entry), "{what}");
            assert_eq!(r.read_chunk(&id).unwrap(), chunk.to_vec(), "{what}");
        }
    }
}

/// A backup whose generation is taken while its write folds: the fold hands
/// the backup's own segment back (a partial fold's appended segment; a full
/// fold's segment, whose additions it had added to the index as edits and
/// dropped again) and the retry folds over the new head. After it the
/// handle's index, a warm reopen and a cold open agree, and every chunk,
/// the backup's own included, reads back.
#[test]
fn taken_generation_during_a_fold_rebases_and_commits() {
    for full in [false, true] {
        let mut sim = sim_of(StoreKind::Conditional);
        let (a, b) = (sim.client("a"), sim.client("b"));
        let mut rb = open(&sim, b, sim.host_env(b));
        commit_whole(&mut rb, "b0", &[b"chunk b0"]);
        commit_whole(&mut rb, "b1", &[b"chunk b1"]);
        // b0's and b1's segments are old: the prefix a partial fold folds.
        sim.advance(crate::repo::ages::T_PACK + Duration::from_secs(60));
        let mut ra = open(&sim, a, sim.host_env(a));
        let mut ma = session(&mut ra, "a1", &[b"chunk of a"]);
        ra.set_lock_store_time(Some(vykar_storage::StoreNow::at(sim.store().now().into())));
        if full {
            // Every segment foldable, the backup's own too: a full fold.
            ra.set_segment_grace(Duration::ZERO);
        }
        ra.request_index_fold();
        let first = step_until(&mut ra, &mut ma, at_put_head).unwrap();
        commit_whole(&mut rb, "b2", &[b"chunk b2"]);
        let taken = latest_head(&sim).generation;
        let steps = step_until(&mut ra, &mut ma, |_| false).unwrap();
        assert!(ma.take_output().is_some(), "full {full}");
        assert_eq!(
            steps.get(1),
            Some(&StepKind::Backoff),
            "full {full}: {steps:?}"
        );
        // Both attempts fold: a base, and (partial) the backup's own segment.
        for attempt in [first.as_slice(), &steps[1..]] {
            assert!(
                attempt.contains(&StepKind::PutBase),
                "full {full}: {attempt:?}"
            );
            assert_eq!(
                attempt.contains(&StepKind::PutSegment),
                !full,
                "full {full}: {attempt:?}"
            );
        }
        let head = latest_head(&sim);
        assert_eq!(head.generation, taken + 1, "full {full}");
        assert!(head.base.is_some(), "full {full}: folded");
        // Partial: b2's young segment and a's own stay segments.
        let segments = if full { 0 } else { 2 };
        assert_eq!(head.segments.len(), segments, "full {full}: {head:?}");
        let chunks: [&[u8]; 4] = [b"chunk b0", b"chunk b1", b"chunk b2", b"chunk of a"];
        index_agrees(&sim, a, &ra, &chunks);
        let report = sim.check_invariants(&[Invariant::HeadsImmutable, Invariant::IndexIntegrity]);
        assert!(report.is_clean(), "full {full}: {report}");
    }
}

// ── The checker's invariant 12 and retained heads ──────────────────────

/// Two backups (heads 2 and 3) on a conditional store, judged clean.
fn two_backups() -> Sim {
    let mut sim = sim_of(StoreKind::Conditional);
    let a = sim.client("a");
    sim.backup(a, "a1", &tree("a1")).expect_ok();
    sim.backup(a, "a2", &tree("a2")).expect_ok();
    sim.check_invariants(Invariant::STEP8).assert_clean();
    sim
}

fn head_at(sim: &Sim, generation: u64) -> IndexHead {
    let tmp = tempfile::tempdir().unwrap();
    let repo = open_observer(sim.store(), tmp.path()).unwrap();
    let data = sim
        .store()
        .object(&index_store::head_key(generation))
        .unwrap()
        .data;
    index_store::classify_head(generation, &data, repo.crypto.as_ref())
        .into_head()
        .unwrap()
}

/// Replace head `generation` by `head` without an overwrite record (the key
/// deleted first), as damage the overwrite part would not see.
fn replace_quietly(sim: &mut Sim, head: &IndexHead) {
    let key = index_store::head_key(head.generation);
    let bytes = planted(sim, head, head.generation);
    sim.tamper_delete(&key);
    sim.tamper_put(&key, &bytes);
}

#[test]
fn checker_flags_a_head_overwrite() {
    let mut sim = two_backups();
    let head = head_at(&sim, 2);
    let mut other = head.clone();
    other.retired.clear();
    other.segments.clear();
    let bytes = planted(&sim, &other, 2);
    sim.tamper_put("index.h/2", &bytes);
    let report = sim.check_invariants(Invariant::STEP8);
    assert!(
        report.has_kind(ViolationKind::HeadOverwritten, "index.h/2"),
        "{report}"
    );
}

#[test]
fn checker_flags_a_head_that_skips_its_predecessor() {
    let mut sim = two_backups();
    let mut head = head_at(&sim, 3);
    let dropped = head.segments.remove(0);
    replace_quietly(&mut sim, &head);
    let report = sim.check_invariants(Invariant::STEP8);
    assert!(
        report.has_kind(ViolationKind::HeadSkipsPredecessor, "index.h/3"),
        "{report}"
    );
    assert!(!report.has_kind(ViolationKind::HeadOverwritten, "index.h/3"));
    // The fallback does not judge it.
    assert!(
        !sim.check_invariants(Invariant::STEP7)
            .has_kind(ViolationKind::HeadSkipsPredecessor, "index.h/3")
    );
    let _ = dropped;
}

#[test]
fn checker_flags_a_retained_head_naming_an_absent_object() {
    let mut sim = two_backups();
    let mut head = head_at(&sim, 2);
    let absent = crate::index::head::ObjectName([0xcd; 32]);
    head.retired.push(absent);
    replace_quietly(&mut sim, &head);
    let key = index_store::object_key(&absent);
    let report = sim.check_invariants(Invariant::STEP8);
    assert!(
        report.has_kind(ViolationKind::RetainedHeadObjectAbsent, &key),
        "{report}"
    );
    assert!(
        !sim.check_invariants(Invariant::STEP7)
            .has_kind(ViolationKind::RetainedHeadObjectAbsent, &key)
    );
}

/// `check --repair` that rebuilds the index writes a head without a
/// previous one: it names nothing of its predecessor and is exempt.
#[test]
fn checker_exempts_a_repair_head() {
    let mut sim = two_backups();
    let head = head_at(&sim, 3);
    let segment = index_store::object_key(&head.segments[1].name);
    sim.tamper_delete(&segment);
    let r = sim.client("r");
    let run = sim.check_repair(r);
    run.expect_ok();
    assert!(matches!(
        sim.history().runs.last().map(|x| &x.op),
        Some(ClientOp::CheckRepair)
    ));
    let latest = latest_head(&sim);
    let report = sim.check_invariants(Invariant::STEP8);
    assert!(
        !report
            .violations
            .iter()
            .any(|v| v.kind == ViolationKind::HeadSkipsPredecessor),
        "head {}: {report}",
        latest.generation
    );
}

// ── A lock-free writer never falls back (04 step 9, O4) ────────────────

/// A backend whose put-if-absent check passes and whose later creates are
/// refused as "unsupported", reaching `create_head`'s fallback branch
/// directly (`vykar_storage::Verifier` would turn such a refusal into a
/// failed write first).
struct RefusesAfterCheck {
    inner: Arc<dyn vykar_storage::StorageBackend>,
}

impl vykar_storage::InnerBackend for RefusesAfterCheck {
    fn inner_backend(&self) -> &dyn vykar_storage::StorageBackend {
        &*self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for RefusesAfterCheck;
    except [conditional];

    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        Some(self)
    }
}

impl vykar_storage::ConditionalWrite for RefusesAfterCheck {
    fn put_if_absent(
        &self,
        _key: &str,
        _data: &[u8],
    ) -> vykar_types::error::Result<vykar_storage::Created> {
        Err(vykar_storage::put_if_absent_unavailable(
            "test: refused after the check",
        ))
    }
    fn verify(&self) -> vykar_types::error::Result<vykar_storage::ConditionalSupport> {
        Ok(vykar_storage::ConditionalSupport::Verified)
    }
}

/// O4: an encrypted repository whose storage passed the check commits
/// lock-free; a create refused after that fails the commit, and no head is
/// written with a plain PUT (there is no lock to make one safe).
#[test]
fn a_refusal_after_a_passed_check_fails_the_commit() {
    let mut sim = Sim::with_config(SimConfig {
        kind: StoreKind::Conditional,
        encrypted: true,
        ..SimConfig::default()
    });
    let a = sim.client("a");
    let before = sim.store().step();
    let mut repo = Repository::open(
        Box::new(RefusesAfterCheck {
            inner: Arc::new(sim.store().handle(a)),
        }),
        Some(crate::sim::SIM_PASSPHRASE),
        Some(sim.client_dir(a).join("repo-cache")),
        OpenOptions::new().with_index().host_env(sim.host_env(a)),
    )
    .unwrap();
    assert_eq!(
        repo.commit_mode().unwrap(),
        crate::repo::CommitMode::LockFree
    );
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    repo.store_chunk(b"chunk of a", Compression::None, PackType::Data)
        .unwrap();
    let mut m = BackupCommit::new(
        &repo,
        entry("a1"),
        b"snapshot".to_vec(),
        None,
        crate::repo::CommitMode::LockFree,
    );
    let err = step_until(&mut repo, &mut m, |_| false).unwrap_err();
    assert!(err.to_string().contains("never writes"), "{err}");
    repo.flush_on_abort();
    let ops = ops_after(&sim, a, before);
    assert!(
        !ops.iter()
            .any(|r| r.op.is_write() && r.key.starts_with("index.h/")),
        "a head was written: {ops:?}"
    );
    assert!(
        !ops.iter()
            .any(|r| r.key.starts_with("snapshots/") && r.op.is_write()),
        "{ops:?}"
    );
}
