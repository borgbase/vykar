//! The repository's writer mode (10-single-protocol step 5): a
//! multi-writer repository refuses every write of a client whose storage
//! offers no put-if-absent, with nothing written; the mode switch; init's
//! check deciding the mode.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::config::RetentionConfig;
use crate::repo::{Repo, WriterMode, WriterModeReport};
use crate::sim::{ClientId, Invariant, OpKind, OpRecord, Sim, SimConfig, SourceTree, StoreKind};
use crate::testutil::MemoryBackend;
use vykar_storage::{
    ConditionalSupport, ConditionalWrite, Created, InnerBackend, StorageBackend, Verifier,
    delegate_storage_backend,
};
use vykar_types::error::{Result, VykarError};

fn tree(name: &str) -> SourceTree {
    SourceTree::seeded(name, 2, 16 * 1024)
}

/// The records of `client`'s run `run`.
fn records(sim: &Sim, client: ClientId, run: usize) -> Vec<OpRecord> {
    sim.store()
        .log()
        .into_iter()
        .filter(|r| r.client == client && r.run == Some(run))
        .collect()
}

/// Every stored object outside `sessions/`, key → bytes.
fn objects_outside_sessions(sim: &Sim) -> BTreeMap<String, Vec<u8>> {
    sim.store()
        .keys("")
        .into_iter()
        .filter(|k| !k.starts_with("sessions/"))
        .map(|k| {
            let data = sim.store().object(&k).unwrap().data;
            (k, data)
        })
        .collect()
}

/// STEP8 without `AllowedErrors`: these tests make runs refuse on purpose
/// (`MultiWriterUnsupported` is configuration, not contention, and is not
/// an allowed simulator error).
const STEP8_WITH_REFUSALS: &[Invariant] = &[
    Invariant::CommittedSnapshotsRestore,
    Invariant::AcknowledgedCommitsDurable,
    Invariant::NoSilentRemoval,
    Invariant::IndexIntegrity,
    Invariant::ExclusiveSessions,
    Invariant::NoPermanentWedge,
    Invariant::OrphanGrace,
    Invariant::MarkerGapCommit,
    Invariant::HeadsImmutable,
];

fn multi_writer_sim() -> (Sim, ClientId, ClientId) {
    let mut sim = Sim::with_config(SimConfig {
        kind: StoreKind::Conditional,
        ..SimConfig::default()
    });
    assert_eq!(sim.store().writer_mode(), Some(WriterMode::Multi));
    let (ctl, u) = (sim.client("ctl"), sim.client("u"));
    sim.backup(ctl, "c1", &tree("c1")).expect_ok();
    sim.backup(ctl, "c2", &tree("c2")).expect_ok();
    sim.set_client_without_conditional(u);
    (sim, ctl, u)
}

fn assert_refused(sim: &Sim, u: ClientId, run: &crate::sim::RunRecord, what: &str) {
    assert!(!run.is_ok(), "{what} succeeded");
    let detail = run.detail();
    assert!(detail.contains("multi-writer"), "{what}: {detail}");
    assert!(detail.contains("--commit-mode single"), "{what}: {detail}");
    let stray: Vec<OpRecord> = records(sim, u, run.index)
        .into_iter()
        .filter(|r| r.op.is_mutation() && !r.key.starts_with("sessions/"))
        .collect();
    assert!(stray.is_empty(), "{what}: {stray:?}");
}

/// A client whose storage offers no put-if-absent refuses backup, compact
/// (and its report and dry run), prune, delete and `check --repair` on a
/// multi-writer repository, right after open: nothing outside `sessions/`
/// is written, and no session marker is left.
#[test]
fn a_client_without_put_if_absent_refuses_every_write_on_a_multi_writer_repository() {
    let (mut sim, _ctl, u) = multi_writer_sim();
    let before = objects_outside_sessions(&sim);
    let runs = [
        ("backup", sim.backup(u, "u1", &tree("u1"))),
        ("compact", sim.compact(u)),
        (
            "prune",
            sim.prune(
                u,
                RetentionConfig {
                    keep_last: Some(1),
                    ..Default::default()
                },
            ),
        ),
        ("delete", sim.delete(u, &["c1"])),
        ("check --repair", sim.check_repair(u)),
    ];
    for (what, run) in &runs {
        assert_refused(&sim, u, run, what);
    }
    assert!(
        sim.store()
            .keys("sessions/")
            .iter()
            .all(|k| !k.ends_with(".json")),
        "{:?}",
        sim.store().keys("sessions/")
    );
    assert_eq!(objects_outside_sessions(&sim), before);
    sim.check_invariants(STEP8_WITH_REFUSALS).assert_clean();
}

/// `compact --report-unreferenced` and `compact --dry-run` write nothing,
/// but they ask the commit mode first, so they refuse too (accepted in
/// step 5).
#[test]
fn compact_report_and_dry_run_refuse_on_a_multi_writer_repository() {
    let (sim, _ctl, u) = multi_writer_sim();
    let before = objects_outside_sessions(&sim);
    let step = sim.store().step();
    let cache = tempfile::tempdir().unwrap();
    let mut repo = Repo::open(
        Box::new(sim.store().handle(u)),
        sim.store().passphrase(),
        Some(cache.path().to_path_buf()),
        crate::repo::OpenOptions::new().host_env(sim.host_env(u)),
    )
    .unwrap();
    let cancel = crate::repo::control::CancelToken::new();
    let report = crate::commands::compact::report_unreferenced_on_repo(&mut repo, &cancel)
        .expect_err("the report refused");
    let dry = crate::commands::compact::run_on_repo(&mut repo, 10.0, None, true, &cancel)
        .expect_err("the dry run refused");
    for err in [report, dry] {
        assert!(
            matches!(err, VykarError::MultiWriterUnsupported { .. }),
            "{err:?}"
        );
        let text = err.to_string();
        assert!(text.contains("cannot run the command"), "{text}");
        assert!(!text.contains("committed"), "{text}");
    }
    assert!(
        !sim.store()
            .log()
            .iter()
            .any(|r| r.step > step && r.op.is_mutation()),
        "nothing written"
    );
    assert_eq!(objects_outside_sessions(&sim), before);
}

fn switch(
    sim: &mut Sim,
    c: ClientId,
    want: WriterMode,
) -> (crate::sim::RunRecord, Option<WriterModeReport>) {
    let report = RefCell::new(None);
    let run = sim.custom(c, "switch", |w| {
        *report.borrow_mut() = Some(w.set_writer_mode(want)?);
        Ok(())
    });
    (run, report.into_inner())
}

/// The client whose storage lost put-if-absent switches the repository to
/// single-writer, then backs up as the single writer (no lock since
/// 10-single-protocol step 7) with a plain head PUT; its switch back to
/// multi-writer refuses. A client whose storage passes the
/// check switches back, after which the first client refuses again and the
/// other commits lock-free.
#[test]
fn switch_to_single_and_back() {
    let (mut sim, ctl, u) = multi_writer_sim();
    let before = objects_outside_sessions(&sim);
    let (run, report) = switch(&mut sim, u, WriterMode::Single);
    run.expect_ok();
    assert_eq!(report.unwrap().mode, WriterMode::Single);
    assert_eq!(sim.store().writer_mode(), Some(WriterMode::Single));
    let after = objects_outside_sessions(&sim);
    let changed: Vec<&String> = after
        .iter()
        .filter(|(k, v)| before.get(*k) != Some(*v))
        .map(|(k, _)| k)
        .collect();
    assert_eq!(changed, vec!["config"], "only config changed");

    let run = sim.backup(u, "u1", &tree("u1"));
    run.expect_ok();
    let recs = records(&sim, u, run.index);
    assert!(
        !recs.iter().any(|r| r.op.is_write()
            && r.key
                .starts_with(crate::repo::sessions::LEGACY_LOCKS_PREFIX)),
        "a lock was taken: {recs:?}"
    );
    let heads: Vec<&OpRecord> = recs
        .iter()
        .filter(|r| r.op.is_write() && r.key.starts_with("index.h/"))
        .collect();
    assert_eq!(heads.len(), 1, "{heads:?}");
    assert_eq!(heads[0].op, OpKind::Put, "{heads:?}");

    let config = sim.store().object("config").unwrap().data;
    let (run, _) = switch(&mut sim, u, WriterMode::Multi);
    assert!(!run.is_ok());
    assert!(
        run.detail().contains("stays single-writer"),
        "{}",
        run.detail()
    );
    assert_eq!(sim.store().object("config").unwrap().data, config);

    let (run, report) = switch(&mut sim, ctl, WriterMode::Multi);
    run.expect_ok();
    assert!(report.unwrap().changed);
    assert!(
        records(&sim, ctl, run.index).iter().any(|r| {
            r.op == OpKind::PutIfAbsent
                && r.key
                    .starts_with(vykar_storage::PUT_IF_ABSENT_PROBE_KEY_PREFIX)
        }),
        "the switch ran the check"
    );
    assert_eq!(sim.store().writer_mode(), Some(WriterMode::Multi));

    let run = sim.backup(u, "u2", &tree("u2"));
    assert_refused(&sim, u, &run, "u's backup after the switch back");
    let run = sim.backup(ctl, "c3", &tree("c3"));
    run.expect_ok();
    assert!(run.shared);
    for name in ["c1", "c2", "u1", "c3"] {
        sim.restore(ctl, name).expect_ok();
    }
    sim.check_invariants(STEP8_WITH_REFUSALS).assert_clean();
}

/// A switch to the mode the repository already has writes nothing but its
/// exclusive session's own marker keys (10-single-protocol step 8; the
/// lock's keys before).
#[test]
fn a_noop_switch_writes_nothing() {
    let (mut sim, ctl, _) = multi_writer_sim();
    let (run, report) = switch(&mut sim, ctl, WriterMode::Multi);
    run.expect_ok();
    let report = report.unwrap();
    assert!(!report.changed);
    assert_eq!(report.mode, WriterMode::Multi);
    let stray: Vec<OpRecord> = records(&sim, ctl, run.index)
        .into_iter()
        .filter(|r| r.op.is_mutation() && !r.key.starts_with("sessions/"))
        .collect();
    assert!(stray.is_empty(), "{stray:?}");
}

// ── Init's check ────────────────────────────────────────────────────────

/// A memory backend whose put-if-absent answers as `raw` scripts it,
/// behind a real `Verifier`.
struct Scripted {
    inner: Arc<MemoryBackend>,
    verifier: Verifier,
    /// Creates per key so far.
    calls: Mutex<HashMap<String, usize>>,
    /// `verify_at_init` fails undecided.
    undecided_init: bool,
}

impl Scripted {
    fn new(inner: Arc<MemoryBackend>, undecided_init: bool) -> Self {
        Self {
            inner,
            verifier: Verifier::new(),
            calls: Mutex::new(HashMap::new()),
            undecided_init,
        }
    }

    /// Honest, except that the first two creates of each race key both
    /// win: the sequential check passes, the race fails.
    fn raw(&self, key: &str, data: &[u8]) -> Result<Created> {
        let mut calls = self.calls.lock().unwrap();
        let n = calls.entry(key.to_string()).or_default();
        *n += 1;
        if key.contains("-race") && *n <= 2 {
            self.inner.put(key, data)?;
            return Ok(Created::Yes);
        }
        if self.inner.exists(key)? {
            return Ok(Created::AlreadyExists);
        }
        self.inner.put(key, data)?;
        Ok(Created::Yes)
    }
}

impl InnerBackend for Scripted {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for Scripted;
    except [conditional];

    fn conditional(&self) -> Option<&dyn ConditionalWrite> {
        (!self.verifier.refused()).then_some(self as &dyn ConditionalWrite)
    }
}

impl ConditionalWrite for Scripted {
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<Created> {
        self.verifier
            .put_if_absent("scripted", &*self.inner, &|k, d| self.raw(k, d), key, data)
    }
    fn verify(&self) -> Result<ConditionalSupport> {
        self.verifier.verify(&*self.inner, &|k, d| self.raw(k, d))
    }
    fn verify_at_init(&self) -> Result<ConditionalSupport> {
        if self.undecided_init {
            return Err(VykarError::Other("io".into()));
        }
        self.verifier
            .verify_at_init(&*self.inner, &|k, d| self.raw(k, d))
    }
}

fn init(storage: Box<dyn StorageBackend>) -> Result<Repo> {
    crate::repo::with_cheap_kdf(|| {
        Repo::init(
            storage,
            crate::repo::EncryptionMode::Aes256Gcm,
            crate::repo::ChunkerParams::default(),
            Some(crate::sim::SIM_PASSPHRASE),
            None,
            None,
        )
    })
}

/// Init's check is undecided: no repository (no `config`), and a second
/// init over a working backend succeeds.
#[test]
fn an_undecided_init_check_creates_no_repository() {
    let inner = Arc::new(MemoryBackend::new());
    let err = init(Box::new(Scripted::new(Arc::clone(&inner), true)))
        .err()
        .expect("init failed");
    assert!(err.to_string().contains("was not created"), "{err}");
    assert!(!inner.exists("config").unwrap());
    let repo = init(Box::new(Scripted::new(Arc::clone(&inner), false))).unwrap();
    assert!(inner.exists("config").unwrap());
    drop(repo);
}

/// The sequential check passes and the concurrent race fails: init
/// records single-writer, with the race's verdict as the reason.
#[test]
fn a_failed_init_race_records_single_writer() {
    let inner = Arc::new(MemoryBackend::new());
    let repo = init(Box::new(Scripted::new(Arc::clone(&inner), false))).unwrap();
    assert_eq!(repo.writer_mode(), WriterMode::Single);
    let reason = repo.single_writer_reason().expect("a reason");
    assert!(
        reason.starts_with(vykar_storage::IGNORES_CONDITION),
        "{reason}"
    );
    assert!(reason.contains("concurrent"), "{reason}");
    let crate::repo::DecodedConfig::Map(config) =
        crate::repo::RepoConfig::decode(&inner.get("config").unwrap().unwrap()).unwrap()
    else {
        panic!("a map config");
    };
    assert!(config.features.is_empty(), "{:?}", config.features);
}

// ── The re-read of config (10-single-protocol step 7) ───────────────────

/// `c`'s handle on `sim`'s store, opened as a writer opens it.
fn open_handle(sim: &Sim, c: ClientId) -> Repo {
    Repo::open(
        Box::new(sim.store().handle(c)),
        sim.store().passphrase(),
        Some(sim.client_dir(c).join("repo-cache")),
        crate::repo::OpenOptions::new().host_env(sim.host_env(c)),
    )
    .unwrap()
}

/// The stored config, decoded, changed by `change` and written back.
fn rewrite_config(sim: &mut Sim, change: impl FnOnce(&mut crate::repo::RepoConfig)) {
    let stored = sim.store().object("config").unwrap().data;
    let crate::repo::DecodedConfig::Map(mut config) =
        crate::repo::RepoConfig::decode(&stored).unwrap()
    else {
        panic!("a v6 config");
    };
    change(&mut config);
    sim.tamper_put("config", &config.encode().unwrap());
}

/// `reread_writer_mode`: the same config keeps the mode; a mode added or
/// removed is adopted, the cached commit mode decided again; another
/// identity or chunk hash refuses as a changed config; an unknown feature
/// is refused as validate refuses it; a missing config is an error.
#[test]
fn the_config_re_read_adopts_a_mode_and_refuses_another_repository() {
    let mut sim = Sim::with_config(SimConfig {
        kind: StoreKind::Conditional,
        ..SimConfig::default()
    });
    let c = sim.client("c");
    let mut repo = open_handle(&sim, c);
    assert_eq!(repo.commit_mode().unwrap(), WriterMode::Multi);
    assert_eq!(repo.reread_writer_mode().unwrap(), WriterMode::Multi);
    assert_eq!(repo.writer_mode(), WriterMode::Multi);

    rewrite_config(&mut sim, |c| c.set_writer_mode(WriterMode::Single));
    assert_eq!(repo.reread_writer_mode().unwrap(), WriterMode::Single);
    assert_eq!(repo.writer_mode(), WriterMode::Single);
    assert_eq!(
        repo.commit_mode().unwrap(),
        WriterMode::Single,
        "decided again"
    );

    rewrite_config(&mut sim, |c| c.set_writer_mode(WriterMode::Multi));
    assert_eq!(repo.reread_writer_mode().unwrap(), WriterMode::Multi);
    assert_eq!(repo.commit_mode().unwrap(), WriterMode::Multi);

    let changed = |e: VykarError| e.to_string().contains("the repository config changed");
    let stored = sim.store().object("config").unwrap().data;
    rewrite_config(&mut sim, |c| c.id = vec![0x42; c.id.len()]);
    assert!(changed(repo.reread_writer_mode().unwrap_err()));
    sim.tamper_put("config", &stored);
    rewrite_config(&mut sim, |c| {
        c.chunk_hash = if c.chunk_hash == "blake3" {
            "blake2b".into()
        } else {
            "blake3".into()
        };
    });
    assert!(changed(repo.reread_writer_mode().unwrap_err()));
    sim.tamper_put("config", &stored);
    // Store-level tampering: the checker's observer would refuse to open
    // these configs.
    let crate::repo::DecodedConfig::Map(mut future) =
        crate::repo::RepoConfig::decode(&stored).unwrap()
    else {
        panic!("a v6 config");
    };
    future.features.push("from-the-future".into());
    sim.store().tamper_put("config", &future.encode().unwrap());
    let err = repo.reread_writer_mode().unwrap_err();
    assert!(err.to_string().contains("newer vykar"), "{err}");
    assert!(sim.store().tamper_delete("config"));
    assert!(repo.reread_writer_mode().is_err());
}
