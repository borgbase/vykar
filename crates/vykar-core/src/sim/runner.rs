//! `Sim`: the runner. Clients run on the calling thread, or on their own
//! thread with `spawn`, paused and resumed at gates (their own worker
//! threads are real), each with its own cache directory and all through one
//! `SimStore`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use vykar_storage::StorageBackend;
use vykar_types::error::Result;

use super::checker::{Invariant, Report, ViolationKind, check_invariants};
use super::env::{SimHosts, SimMachine, SimTime, TimeMode};
use super::fault::{ClientId, FaultRule, ParkedId, RuleId};
use super::gate::{GateId, GateRule, Paused};
use super::history::{
    ClientOp, ErrorRecord, History, Observation, ObservationCause, RunOutcome, RunRecord, classify,
};
use super::store::{SIM_PASSPHRASE, SimConfig, SimStore, StoreKind};

thread_local! {
    /// The store kind `Sim::new` uses on this thread (`with_store_kind`).
    static DEFAULT_KIND: std::cell::Cell<StoreKind> =
        const { std::cell::Cell::new(StoreKind::Plain) };
}

/// Run `f` with `Sim::new` making stores of `kind` on this thread: how a
/// hazard or regression scenario runs on both the fallback and the
/// conditional store without a parameter of its own.
/// The store kind `Sim::new` uses on this thread (`with_store_kind`).
pub(crate) fn default_store_kind() -> StoreKind {
    DEFAULT_KIND.with(std::cell::Cell::get)
}

pub fn with_store_kind<T>(kind: StoreKind, f: impl FnOnce() -> T) -> T {
    struct Restore(StoreKind);
    impl Drop for Restore {
        fn drop(&mut self) {
            DEFAULT_KIND.with(|k| k.set(self.0));
        }
    }
    let _restore = Restore(DEFAULT_KIND.with(|k| k.replace(kind)));
    f()
}
use crate::commands::check::{self, RepairAction, RepairMode};
use crate::commands::restore::{self, RestoreOptions};
use crate::commands::{backup, compact, delete, prune, util};
use crate::compress::Compression;
use crate::config::{
    EncryptionConfig, EncryptionModeConfig, RepositoryConfig, RetentionConfig, VykarConfig,
};
use crate::repo::control::CancelToken;
use crate::repo::{
    ChunkerParams, EncryptionMode, HostEnv, HostIdentity, OpenOptions, Repo, RepoWriter, identity,
    sessions,
};

/// A source directory's regular files, by `/`-separated relative path.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceTree {
    files: BTreeMap<String, Vec<u8>>,
}

impl SourceTree {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn file(mut self, rel: &str, data: impl Into<Vec<u8>>) -> Self {
        self.files.insert(rel.to_string(), data.into());
        self
    }

    /// `files` files of `size` bytes, content from BLAKE3-XOF(seed, i):
    /// deterministic, incompressible.
    pub fn seeded(seed: &str, files: usize, size: usize) -> Self {
        let mut tree = Self::new();
        for i in 0..files {
            tree = tree.file(
                &format!("f{i:03}.bin"),
                seeded_bytes(&format!("{seed}/{i}"), size),
            );
        }
        tree
    }

    pub fn files(&self) -> &BTreeMap<String, Vec<u8>> {
        &self.files
    }

    pub(crate) fn write_to(&self, dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        for (rel, data) in &self.files {
            let path = dir.join(rel);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, data)?;
        }
        Ok(())
    }

    /// Read a restored directory back (regular files only).
    pub(crate) fn read_from(dir: &Path) -> std::io::Result<Self> {
        fn walk(base: &Path, dir: &Path, out: &mut SourceTree) -> std::io::Result<()> {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let ty = entry.file_type()?;
                let path = entry.path();
                if ty.is_dir() {
                    walk(base, &path, out)?;
                } else if ty.is_file() {
                    let rel = path
                        .strip_prefix(base)
                        .expect("walked path under base")
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy().into_owned())
                        .collect::<Vec<_>>()
                        .join("/");
                    out.files.insert(rel, std::fs::read(&path)?);
                }
            }
            Ok(())
        }
        let mut tree = Self::new();
        walk(dir, dir, &mut tree)?;
        Ok(tree)
    }
}

/// `size` bytes of BLAKE3 XOF output keyed by `seed`.
pub fn seeded_bytes(seed: &str, size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    blake3::Hasher::new()
        .update(seed.as_bytes())
        .finalize_xof()
        .fill(&mut buf);
    buf
}

/// Result of a client body: names removed, and a detail line.
pub(super) type RunResult = Result<(Vec<String>, String)>;

/// A client body: store, config, client dir, run index, environment.
type Body = Box<dyn FnOnce(&SimStore, VykarConfig, &Path, usize, HostEnv) -> RunResult + Send>;

/// A body run in an exclusive session by [`Call::Custom`].
pub type CustomFn = Box<dyn FnOnce(&mut RepoWriter<'_>) -> Result<()> + Send + 'static>;

/// One client command, owned so it can run on its own thread.
pub enum Call {
    Backup {
        snapshot: String,
        tree: SourceTree,
    },
    /// [`Call::Backup`] that stops (`Interrupted`) once `cancel` is
    /// cancelled, as a signal stops the CLI's.
    BackupCancellable {
        snapshot: String,
        tree: SourceTree,
        cancel: CancelToken,
    },
    /// [`Call::Backup`] of the source directory snapshot `of` was taken
    /// from, untouched since: equal items (path, mtime, ctime), hence equal
    /// tree chunks, so the backup adds no chunk (a reuse-only commit).
    BackupSameSource {
        snapshot: String,
        of: String,
    },
    Delete {
        names: Vec<String>,
    },
    Prune {
        retention: RetentionConfig,
    },
    Compact,
    Check,
    CheckRepair,
    Restore {
        snapshot: String,
    },
    /// `vykar clear-sessions` without its confirmation: everything under the
    /// legacy lock directory (`sessions::LEGACY_LOCKS_PREFIX`,
    /// `sessions::clear_legacy_locks`), then every session object
    /// (`sessions::clear_all_sessions`).
    ClearSessions,
    /// Opens like delete and runs `f` as an exclusive session
    /// (`util::with_exclusive_session`, 10-single-protocol step 8): a
    /// maintenance writer.
    Custom {
        label: String,
        f: CustomFn,
    },
    /// [`Call::Delete`]'s delete as an exclusive session
    /// (`util::with_exclusive_session`, 10-single-protocol step 8): an
    /// exclusive writer that deletes, standing in for `check --repair`,
    /// upgrade and pack maps in the hazards and tests whose exclusive holder
    /// was a delete. History op: `ClientOp::Delete` (`RunRecord::exclusive`).
    ExclusiveDelete {
        names: Vec<String>,
    },
    /// [`Call::Custom`]: `f` as an exclusive session. History op:
    /// `ClientOp::Custom`.
    ExclusiveCustom {
        label: String,
        f: CustomFn,
    },
    /// Tests: [`Call::Backup`] that records in `rechecks` whether its commit
    /// machine re-checks reused chunks on every attempt (the suspend rule's
    /// `recheck_reuse_always`, 10-single-protocol step 7), as
    /// `PendingCommit::begin_commit` built it.
    #[cfg(test)]
    BackupRecordingRecheck {
        snapshot: String,
        tree: SourceTree,
        rechecks: std::sync::Arc<std::sync::atomic::AtomicBool>,
    },
    /// `vykar upgrade --commit-mode` on a v6 repository
    /// (`commands::upgrade::upgrade_repo` on a handle opened with
    /// `OpenOptions::legacy_format`, as `run_with_mode` opens it): the
    /// production switch, as an exclusive session, refusing beside live
    /// sessions. History op: `ClientOp::Custom { label: "switch-mode" }`.
    SwitchMode {
        mode: crate::repo::WriterMode,
    },
}

/// Virtual time added before every run, so consecutive runs never share a
/// timestamp.
const RUN_TICK: Duration = Duration::from_secs(1);

/// Real time `join` waits for a spawned run to end.
const JOIN_TIMEOUT: Duration = Duration::from_secs(120);

/// Real time `wait_paused` waits for a gate.
pub const GATE_TIMEOUT: Duration = Duration::from_secs(60);

/// Ageing before the probe of invariant 9: past the 45 min staleness of every
/// marker, shared and exclusive; nothing on a v6 repository reads the legacy
/// lock directory.
const WEDGE_AGE: Duration = Duration::from_secs(46 * 60);

/// A run started with [`Sim::spawn`]; [`Sim::join`] records it.
#[must_use = "a spawned run must be joined"]
pub struct Spawned {
    index: usize,
    client: ClientId,
    op: ClientOp,
    handle: JoinHandle<Finished>,
}

impl Spawned {
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn client(&self) -> ClientId {
        self.client
    }

    /// The run's thread, for tests of the harness itself.
    #[cfg(test)]
    pub(crate) fn into_handle(self) -> JoinHandle<Finished> {
        self.handle
    }
}

/// What a run's thread hands back.
pub(crate) struct Finished {
    outcome: RunOutcome,
    first_step: u64,
    last_step: u64,
    observation: Observation,
}

/// What `begin_run` hands the body.
pub(super) struct Started {
    pub(super) index: usize,
    pub(super) cfg: VykarConfig,
    pub(super) dir: PathBuf,
    pub(super) env: HostEnv,
}

/// A spawned run not yet joined.
struct InFlight {
    index: usize,
    /// Set when the run's thread is done, even by a panic.
    done: Arc<AtomicBool>,
}

/// Sets its flag when dropped: on return and on unwind; then wakes the
/// gate waiters, so `poll_paused` sees the end at once.
struct DoneOnDrop(Arc<AtomicBool>, SimStore);

impl Drop for DoneOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
        self.1.notify_gates();
    }
}

pub struct Sim {
    store: SimStore,
    root: tempfile::TempDir,
    repo_url: String,
    history: History,
    /// Client root: `cache/`, `restore-<run>/`.
    clients: BTreeMap<ClientId, PathBuf>,
    /// Virtual time of every client, linked to the store clock.
    time: Arc<SimTime>,
    hosts: SimHosts,
    skews: BTreeMap<ClientId, chrono::Duration>,
    /// `compact.threshold` of a client whose config sets one
    /// ([`Sim::set_compact_threshold`]).
    compact_thresholds: BTreeMap<ClientId, f64>,
    /// The index of the next run; reserved when the run starts.
    next_run: usize,
    in_flight: BTreeMap<ClientId, InFlight>,
}

impl Default for Sim {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Sim {
    /// Open every gate so held runs can finish. Nothing is joined: a run a
    /// failing test left in flight ends, or fails, on its own.
    fn drop(&mut self) {
        self.store.open_all_gates();
    }
}

impl Sim {
    /// A store of the default config (an encrypted repository), whose kind
    /// is [`with_store_kind`]'s when called inside it.
    pub fn new() -> Self {
        Self::with_config(SimConfig {
            kind: DEFAULT_KIND.with(std::cell::Cell::get),
            ..SimConfig::default()
        })
    }

    /// Creates the store, registers client "init" and runs Init as run 0.
    /// The repository is AES-256-GCM with the passphrase [`SIM_PASSPHRASE`]
    /// and a cheap KDF (`repo::with_cheap_kdf`; Init runs inline, on this
    /// thread).
    pub fn with_config(config: SimConfig) -> Self {
        let mut sim = Self::from_store(SimStore::new(config));
        let init = sim.client("init");
        sim.run_inline(init, ClientOp::Init, |store, cfg, _, _, _| {
            let repo = crate::repo::with_cheap_kdf(|| {
                Repo::init(
                    Box::new(store.handle(init)),
                    EncryptionMode::Aes256Gcm,
                    ChunkerParams::default(),
                    cfg.encryption.passphrase.as_deref(),
                    Some(&cfg.repository.init_options()),
                    util::cache_dir_from_config(&cfg),
                )
            })?;
            // As `commands::init`: pin at once, with the init client's cache.
            identity::verify_or_pin(
                &cfg.repository.url,
                &repo.config().id,
                repo.crypto().chunk_hasher().key(),
                util::cache_dir_from_config(&cfg).as_deref(),
                true,
            )?;
            Ok((Vec::new(), String::new()))
        })
        .expect_ok();
        sim
    }

    /// A `Sim` around `store`, with no init run, no clients and an empty
    /// history; time starts at the store clock.
    fn from_store(store: SimStore) -> Self {
        #[cfg(test)]
        crate::testutil::init_test_environment();

        let root = tempfile::tempdir().expect("sim root");
        // Never read or written: backup builds a LocalBackend for it and the
        // wrapper replaces it (LocalBackend::new only canonicalizes).
        let url_dir = root.path().join("repo-url-placeholder");
        std::fs::create_dir_all(&url_dir).expect("placeholder dir");
        let repo_url = url_dir.to_string_lossy().into_owned();
        let time = SimTime::for_store(&store, TimeMode::Auto);
        let hosts = SimHosts::new(store.clone());
        Self {
            store,
            root,
            repo_url,
            history: History::default(),
            clients: BTreeMap::new(),
            time,
            hosts,
            skews: BTreeMap::new(),
            compact_thresholds: BTreeMap::new(),
            next_run: 0,
            in_flight: BTreeMap::new(),
        }
    }

    /// `c`'s config sets `compact.threshold` to `threshold` (percent of
    /// unused space from which a pack is repacked; 100 repacks only packs
    /// with no live entry): two machines that compact with different
    /// thresholds.
    pub fn set_compact_threshold(&mut self, c: ClientId, threshold: f64) {
        self.compact_thresholds.insert(c, threshold);
    }

    pub fn store(&self) -> &SimStore {
        &self.store
    }

    /// Every later handle of `c` offers no put-if-absent but reads the
    /// real `config` ([`SimStore::set_client_without_conditional`]).
    pub fn set_client_without_conditional(&self, c: ClientId) {
        self.store.set_client_without_conditional(c);
    }

    pub fn history(&self) -> &History {
        &self.history
    }

    /// A client on its own machine: hostname `sim-<name>`, boot ID
    /// `boot-<name>`.
    pub fn client(&mut self, name: &str) -> ClientId {
        self.client_on(name, &format!("sim-{name}"), Some(&format!("boot-{name}")))
    }

    /// A client on host `hostname` with boot ID `boot_id`, installation
    /// `install-<hostname>` ([`SimMachine::new`]). The same hostname means
    /// the same installation, so two clients with the same pair share a
    /// machine; the same hostname with another boot ID is a **clone** of that
    /// installation (cloned VMs copy the config directory), H1's hardest case
    /// before 04 step 1.
    pub fn client_on(&mut self, name: &str, hostname: &str, boot_id: Option<&str>) -> ClientId {
        self.client_at(name, SimMachine::new(hostname, boot_id))
    }

    /// A client on `machine`.
    pub fn client_at(&mut self, name: &str, machine: SimMachine) -> ClientId {
        let id = self.store.register_client(name);
        self.hosts.register(id, &machine);
        let dir = self
            .root
            .path()
            .join("clients")
            .join(format!("{}-{name}", id.0));
        std::fs::create_dir_all(dir.join("cache")).expect("client dir");
        self.clients.insert(id, dir);
        id
    }

    /// Wall-clock skew of `c` against the shared time (and the store).
    pub fn set_skew(&mut self, c: ClientId, skew: chrono::Duration) {
        self.skews.insert(c, skew);
    }

    /// Advance client time and the store clock together.
    pub fn advance(&mut self, by: Duration) {
        self.assert_settled();
        self.time.advance(by);
    }

    /// Step the store's clock alone, in either direction (an NTP correction
    /// on the server): client clocks stay; every age on the store's clock
    /// moves by `by`.
    pub fn step_store_clock(&mut self, by: chrono::Duration) {
        self.assert_settled();
        self.store.step_clock(by);
    }

    pub fn time(&self) -> &Arc<SimTime> {
        &self.time
    }

    /// The identity of `c`'s current or last run.
    pub fn host(&self, c: ClientId) -> HostIdentity {
        self.hosts.host(c)
    }

    /// The environment `c`'s commands run with.
    pub fn host_env(&self, c: ClientId) -> HostEnv {
        let skew = self
            .skews
            .get(&c)
            .copied()
            .unwrap_or_else(chrono::Duration::zero);
        let hosts = self.hosts.clone();
        HostEnv {
            clock: Arc::new(self.time.clock(skew).booted_at(self.hosts.booted_at(c))),
            host: self.hosts.host(c),
            pid_alive: Arc::new(move |local, pid| hosts.pid_alive(local, pid)),
            timestamp_fallback_warned: Arc::default(),
        }
    }

    pub fn client_dir(&self, c: ClientId) -> &Path {
        self.clients.get(&c).expect("unknown client")
    }

    /// Where `restore` wrote the output of run `run`.
    pub fn restore_dir(&self, run: &RunRecord) -> PathBuf {
        self.client_dir(run.client)
            .join(format!("restore-{}", run.index))
    }

    /// Panics on an invalid rule.
    pub fn fault(&self, rule: FaultRule) -> RuleId {
        self.store.add_fault(rule).expect("invalid fault rule")
    }

    pub fn crash(&mut self, client: ClientId) {
        self.assert_settled();
        self.store.crash(client);
    }

    pub fn release(&mut self, id: ParkedId) {
        self.assert_settled();
        let issuing_run = self
            .store
            .parked()
            .into_iter()
            .find(|p| p.id == id)
            .and_then(|p| p.run);
        self.store.release(id).expect("release");
        self.observe(ObservationCause::Release {
            parked: id,
            issuing_run,
        });
    }

    /// Drop a parked effect: the late request never lands. The store does
    /// not change, so nothing is observed.
    pub fn discard(&mut self, id: ParkedId) {
        self.assert_settled();
        self.store.discard(id).expect("discard");
    }

    /// One observation per release.
    pub fn release_all(&mut self) -> Vec<ParkedId> {
        self.assert_settled();
        let ids: Vec<ParkedId> = self.store.parked().iter().map(|p| p.id).collect();
        for id in &ids {
            self.release(*id);
        }
        ids
    }

    pub fn tamper_delete(&mut self, key: &str) -> bool {
        self.assert_settled();
        let existed = self.store.tamper_delete(key);
        self.observe(ObservationCause::Tamper);
        existed
    }

    pub fn tamper_put(&mut self, key: &str, data: &[u8]) {
        self.assert_settled();
        self.store.tamper_put(key, data);
        self.observe(ObservationCause::Tamper);
    }

    pub fn backup(&mut self, c: ClientId, snapshot: &str, tree: &SourceTree) -> RunRecord {
        self.call(
            c,
            Call::Backup {
                snapshot: snapshot.to_string(),
                tree: tree.clone(),
            },
        )
    }

    pub fn delete(&mut self, c: ClientId, names: &[&str]) -> RunRecord {
        let names = names.iter().map(|n| n.to_string()).collect();
        self.call(c, Call::Delete { names })
    }

    pub fn prune(&mut self, c: ClientId, retention: RetentionConfig) -> RunRecord {
        self.call(c, Call::Prune { retention })
    }

    pub fn compact(&mut self, c: ClientId) -> RunRecord {
        self.call(c, Call::Compact)
    }

    pub fn check(&mut self, c: ClientId) -> RunRecord {
        self.call(c, Call::Check)
    }

    /// `check --repair` in `RepairMode::Apply`.
    pub fn check_repair(&mut self, c: ClientId) -> RunRecord {
        self.call(c, Call::CheckRepair)
    }

    pub fn restore(&mut self, c: ClientId, snapshot: &str) -> RunRecord {
        self.call(
            c,
            Call::Restore {
                snapshot: snapshot.to_string(),
            },
        )
    }

    pub fn clear_sessions(&mut self, c: ClientId) -> RunRecord {
        self.call(c, Call::ClearSessions)
    }

    /// Opens like prune/delete/compact and runs `f` as an exclusive session
    /// (`util::with_exclusive_session`), on the calling thread (`f` may borrow). [`Call::Custom`] is the
    /// owned form, for [`Sim::spawn`].
    pub fn custom(
        &mut self,
        c: ClientId,
        label: &str,
        f: impl FnOnce(&mut RepoWriter<'_>) -> Result<()>,
    ) -> RunRecord {
        self.assert_inline_ok(c);
        let op = ClientOp::Custom {
            label: label.to_string(),
        };
        self.run_inline(c, op, |store, cfg, _, _, env| {
            custom_body(store, c, cfg, env, f)
        })
    }

    /// [`Self::custom`] in a maintenance session
    /// (`util::with_maintenance_session` with `flags`) instead of an
    /// exclusive one:
    /// what a shared maintenance writer may do, for scripted tests of
    /// what the checker judges in shared runs. The run is
    /// `RunRecord::shared`.
    #[cfg(test)]
    pub(crate) fn custom_shared(
        &mut self,
        c: ClientId,
        label: &str,
        flags: crate::repo::sessions::MarkerFlags,
        f: impl FnOnce(&mut RepoWriter<'_>) -> Result<()>,
    ) -> RunRecord {
        self.assert_inline_ok(c);
        let op = ClientOp::Custom {
            label: label.to_string(),
        };
        self.run_inline(c, op, |store, cfg, _, _, env| {
            let mut repo = open_for(store, c, &cfg, env)?;
            util::with_maintenance_session(&mut repo, flags, |repo, _| f(repo))?;
            Ok((Vec::new(), String::new()))
        })
    }

    /// Run `call` on the calling thread. Panics if `c` has a run in flight or
    /// an armed gate (an inline run that reaches a gate would block the test).
    pub fn call(&mut self, c: ClientId, call: Call) -> RunRecord {
        self.assert_inline_ok(c);
        let (op, body) = self.prepare(c, call);
        self.run_inline(c, op, body)
    }

    /// Start `call` on its own thread, named "sim-<client name>-<run index>",
    /// and return at once. Panics if `c` already has a run in flight.
    ///
    /// The scripted pattern keeps one client runnable at a time (virtual
    /// time is in Auto mode): follow every `spawn` with `wait_paused` or
    /// `join` before starting or advancing anything else, which
    /// `assert_settled` checks. A gate on an operation of a worker thread
    /// (an upload, a rayon task) would leave the run's main thread running
    /// while the client counts as paused; gate main-thread operations.
    /// Even then, a backup held on its main thread can still have its last
    /// journal PUT in flight on another thread, so a verdict must not depend
    /// on where `sessions/*.index` lands in the log.
    ///
    /// `check_no_silent_removal` attributes a disappearance to the cause of
    /// the next observation. A spawned run that deletes a snapshot and then
    /// pauses, while another run ends first, reads as a `SilentRemoval`.
    pub fn spawn(&mut self, c: ClientId, call: Call) -> Spawned {
        assert!(
            !self.in_flight.contains_key(&c),
            "client {} already has a run in flight",
            self.store.client_name(c)
        );
        self.assert_settled();
        let (op, body) = self.prepare(c, call);
        let started = self.begin_run(c);
        let index = started.index;
        let first_step = self.store.step();
        let done = Arc::new(AtomicBool::new(false));
        self.in_flight.insert(
            c,
            InFlight {
                index,
                done: Arc::clone(&done),
            },
        );
        let (store, hosts) = (self.store.clone(), self.hosts.clone());
        let name = format!("sim-{}-{index}", self.store.client_name(c));
        let handle = std::thread::Builder::new()
            .name(name)
            .spawn(move || {
                let _done = DoneOnDrop(done, store.clone());
                let Started { cfg, dir, env, .. } = started;
                let result = body(&store, cfg, &dir, index, env);
                finish_run(&store, &hosts, c, index, false, first_step, result)
            })
            .expect("spawn sim run");
        Spawned {
            index,
            client: c,
            op,
            handle,
        }
    }

    /// Wait (real time, at most JOIN_TIMEOUT) for the run to end and record it.
    /// Re-raises a panic of the run's thread.
    pub fn join(&mut self, s: Spawned) -> RunRecord {
        let start = Instant::now();
        while !s.handle.is_finished() {
            assert!(
                start.elapsed() < JOIN_TIMEOUT,
                "run {} of {} did not end within {JOIN_TIMEOUT:?}; held now: {:?}",
                s.index,
                self.store.client_name(s.client),
                self.store.paused()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let finished = match s.handle.join() {
            Ok(f) => f,
            Err(panic) => {
                self.in_flight.remove(&s.client);
                std::panic::resume_unwind(panic)
            }
        };
        self.record(s.index, s.client, s.op, finished)
    }

    /// Panics on an invalid rule.
    pub fn gate(&self, rule: GateRule) -> GateId {
        self.store.add_gate(rule).expect("invalid gate rule")
    }

    /// Wait (real time, at most `timeout`) until the gate holds an
    /// operation. `Ok(None)` if the gate is open or missed, its client has
    /// no run in flight, or the run ended without reaching it; `Err` on
    /// timeout. The non-panicking form, for scenarios.
    pub fn poll_paused(
        &self,
        gate: GateId,
        timeout: Duration,
    ) -> std::result::Result<Option<Paused>, String> {
        let client = self.store.gate_client(gate).expect("unknown gate");
        let deadline = Instant::now() + timeout;
        loop {
            let done = self.in_flight.get(&client).map(|f| Arc::clone(&f.done));
            let ended_now = || done.as_ref().is_none_or(|d| d.load(Ordering::SeqCst));
            if let Some(p) = self
                .store
                .wait_gate_or(gate, Duration::from_millis(20), &ended_now)
            {
                return Ok(Some(p));
            }
            if !self.store.gate_armed(gate) {
                // Open or missed, or it took its operation after the wait
                // above timed out: look once more.
                return Ok(self.store.gate_paused(gate));
            }
            let ended = self
                .in_flight
                .get(&client)
                .is_none_or(|f| f.done.load(Ordering::SeqCst));
            if ended {
                // One more look: the gate may have been reached just before
                // the run ended.
                return Ok(self.store.gate_paused(gate));
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "gate {gate:?} of {} not reached within {timeout:?}; held now: {:?}",
                    self.store.client_name(client),
                    self.store.paused()
                ));
            }
        }
    }

    /// Wait until the gate holds an operation. None if the gate's client has
    /// no run in flight, or its run ended without reaching the gate. Panics
    /// after GATE_TIMEOUT (60 s real), listing `store.paused()`.
    pub fn wait_paused(&self, gate: GateId) -> Option<Paused> {
        self.poll_paused(gate, GATE_TIMEOUT)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// `wait_paused`, panicking on None with `what`.
    pub fn expect_paused(&self, gate: GateId, what: &str) -> Paused {
        self.wait_paused(gate)
            .unwrap_or_else(|| panic!("{what}: gate {gate:?} not reached before the run ended"))
    }

    /// Let the held operation proceed, or disarm an armed gate.
    pub fn resume(&self, gate: GateId) {
        self.store.resume_gate(gate).expect("resume gate");
    }

    /// Clients with a spawned run not yet joined.
    pub fn in_flight(&self) -> Vec<ClientId> {
        self.in_flight.keys().copied().collect()
    }

    /// Every run in flight has ended or is held by a gate.
    fn assert_settled(&self) {
        for (c, f) in &self.in_flight {
            assert!(
                f.done.load(Ordering::SeqCst) || self.store.client_paused(*c),
                "client {} (run {}) is running unpaused; wait_paused or join it first",
                self.store.client_name(*c),
                f.index
            );
        }
    }

    fn assert_inline_ok(&self, c: ClientId) {
        let name = self.store.client_name(c);
        assert!(
            !self.in_flight.contains_key(&c),
            "client {name} has a run in flight; join it first"
        );
        assert!(
            !self.store.has_armed_gate(c),
            "client {name} has an armed gate, at which an inline run would block \
             the test; start the run with spawn"
        );
        self.assert_settled();
    }

    /// Judge the store and the history. Invariant 9 runs on a fork of the
    /// store. Panics while a run is in flight.
    pub fn check_invariants(&self, set: &[Invariant]) -> Report {
        assert!(
            self.in_flight.is_empty(),
            "check_invariants with runs in flight: {:?}; join them first",
            self.in_flight.keys().collect::<Vec<_>>()
        );
        for (i, r) in self.history.runs.iter().enumerate() {
            assert_eq!(r.index, i, "run indexes not contiguous");
        }
        let rest: Vec<Invariant> = set
            .iter()
            .copied()
            .filter(|i| *i != Invariant::NoPermanentWedge)
            .collect();
        let mut report = check_invariants(&self.store, &self.history, &rest);
        if set.contains(&Invariant::NoPermanentWedge) {
            report.extend(self.check_no_wedge());
        }
        report
    }

    /// Invariant 9: once every marker has aged past its limit, a
    /// backup and a compact from a new client succeed. Runs on a fork, so the
    /// store and the history are not touched.
    ///
    /// Malformed markers age out like the others since 04 step 5 (ages are
    /// mtimes on the store's clock, so client skew does not matter either).
    /// On an append-only `SimConfig` the probe compact fails.
    fn check_no_wedge(&self) -> Report {
        let mut report = Report::default();
        let mut probe = Sim::from_store(self.store.fork());
        probe.advance(WEDGE_AGE);
        let c = probe.client("wedge-probe");
        let r = probe.backup(
            c,
            "wedge-probe",
            &SourceTree::seeded("wedge-probe", 1, 4096),
        );
        if !r.is_ok() {
            report.add(
                ViolationKind::WedgedBackup,
                "repository",
                format!("backup after ageing {WEDGE_AGE:?} failed: {}", r.detail()),
            );
        }
        let r = probe.compact(c);
        if !r.is_ok() {
            report.add(
                ViolationKind::WedgedCompact,
                "repository",
                format!("compact after ageing {WEDGE_AGE:?} failed: {}", r.detail()),
            );
        }
        report
    }

    pub(super) fn client_config(&self, c: ClientId) -> VykarConfig {
        let mut cfg = VykarConfig {
            repository: RepositoryConfig {
                url: self.repo_url.clone(),
                ..Default::default()
            },
            encryption: EncryptionConfig {
                mode: EncryptionModeConfig::Aes256Gcm,
                passphrase: Some(SIM_PASSPHRASE.to_string()),
                passcommand: None,
            },
            cache_dir: Some(
                self.client_dir(c)
                    .join("cache")
                    .to_string_lossy()
                    .into_owned(),
            ),
            one_file_system: true,
            trust_repo: false,
            ..Default::default()
        };
        cfg.limits.threads = 2;
        if let Some(&threshold) = self.compact_thresholds.get(&c) {
            cfg.compact.threshold = threshold;
        }
        cfg
    }

    /// The op and the body of `call`. A backup's source tree is written
    /// here, on the calling thread.
    fn prepare(&mut self, c: ClientId, call: Call) -> (ClientOp, Body) {
        match call {
            Call::Backup { snapshot, tree } => {
                self.prepare_backup(c, snapshot, tree, CancelToken::new())
            }
            Call::BackupCancellable {
                snapshot,
                tree,
                cancel,
            } => self.prepare_backup(c, snapshot, tree, cancel),
            Call::BackupSameSource { snapshot, of } => {
                let tree = self
                    .history
                    .trees
                    .get(&of)
                    .unwrap_or_else(|| panic!("no source of snapshot {of}"))
                    .clone();
                assert!(
                    !self.history.trees.contains_key(&snapshot),
                    "snapshot name {snapshot} reused"
                );
                self.history.trees.insert(snapshot.clone(), tree);
                let src = self.root.path().join("sources").join(&of);
                self.backup_body(c, snapshot, src, CancelToken::new())
            }
            Call::Delete { names } => {
                let op = ClientOp::Delete {
                    names: names.clone(),
                };
                let body: Body = Box::new(
                    move |store: &SimStore, cfg: VykarConfig, _: &Path, _: usize, env: HostEnv| {
                        let mut repo = open_for(store, c, &cfg, env)?;
                        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
                        let result =
                            delete::run_on_repo(&mut repo, &refs, false, &CancelToken::new())?;
                        let removed = result
                            .stats
                            .iter()
                            .map(|s| s.snapshot_name.clone())
                            .collect();
                        Ok((removed, result.warnings.join("; ")))
                    },
                );
                (op, body)
            }
            Call::Prune { retention } => {
                let body: Body = Box::new(
                    move |store: &SimStore,
                          mut cfg: VykarConfig,
                          _: &Path,
                          _: usize,
                          env: HostEnv| {
                        cfg.retention = retention;
                        let mut repo = open_for(store, c, &cfg, env)?;
                        let (stats, list) = prune::run_on_repo(
                            &mut repo,
                            &cfg,
                            false,
                            true,
                            &[],
                            &[],
                            &CancelToken::new(),
                        )?;
                        let removed = list
                            .into_iter()
                            .filter(|e| e.action == "prune")
                            .map(|e| e.snapshot_name)
                            .collect();
                        Ok((
                            removed,
                            format!("kept {}, pruned {}", stats.kept, stats.pruned),
                        ))
                    },
                );
                (ClientOp::Prune, body)
            }
            Call::Compact => {
                let body: Body = Box::new(
                    move |store: &SimStore, cfg: VykarConfig, _: &Path, _: usize, env: HostEnv| {
                        let mut repo = open_for(store, c, &cfg, env)?;
                        let s = compact::run_on_repo(
                            &mut repo,
                            cfg.compact.threshold,
                            None,
                            false,
                            &CancelToken::new(),
                        )?;
                        Ok((
                            Vec::new(),
                            format!(
                                "packs {} repacked {} deleted_empty {} orphan {} young {} \
                                 unknown_age {} corrupt {} unreferenced {} live {} kept-young {} \
                                 sweep-skipped {:?} abandoned {} held {}",
                                s.packs_total,
                                s.packs_repacked,
                                s.packs_deleted_empty,
                                s.packs_orphan,
                                s.packs_orphan_young,
                                s.packs_orphan_unknown_age,
                                s.packs_corrupt,
                                s.chunks_unreferenced,
                                s.packs_orphan_live,
                                s.chunks_unreferenced_young,
                                s.sweep_skipped,
                                s.packs_abandoned,
                                s.packs_repack_held
                            ),
                        ))
                    },
                );
                (ClientOp::Compact, body)
            }
            Call::Check => {
                let body: Body = Box::new(
                    move |store: &SimStore, cfg: VykarConfig, _: &Path, _: usize, env: HostEnv| {
                        let mut repo = util::open_repo_on(
                            Box::new(store.handle(c)),
                            &util::open_request(&cfg),
                            cfg.encryption.passphrase.as_deref(),
                            OpenOptions::new().host_env(env),
                        )?;
                        let result = check::run_on_repo(
                            &mut repo,
                            &cfg,
                            false,
                            true,
                            None,
                            100,
                            false,
                            &CancelToken::new(),
                        )?;
                        Ok((
                            Vec::new(),
                            format!("{} errors {:?}", result.errors.len(), result.errors),
                        ))
                    },
                );
                (ClientOp::Check, body)
            }
            Call::CheckRepair => {
                let body: Body = Box::new(
                    move |store: &SimStore, cfg: VykarConfig, _: &Path, _: usize, env: HostEnv| {
                        let mode = RepairMode::Apply;
                        let mut repo = util::open_repo_on(
                            Box::new(store.handle(c)),
                            &util::open_request(&cfg),
                            cfg.encryption.passphrase.as_deref(),
                            check::repair_open_options(mode).host_env(env),
                        )?;
                        let result = check::run_with_repair_on_repo(
                            &mut repo,
                            &cfg,
                            false,
                            mode,
                            None,
                            &CancelToken::new(),
                        )?;
                        let removed = result
                            .applied
                            .iter()
                            .filter_map(|a| match a {
                                RepairAction::RemoveDanglingSnapshot { snapshot_name, .. } => {
                                    Some(snapshot_name.clone())
                                }
                                RepairAction::RemoveCorruptSnapshot { name: Some(n), .. } => {
                                    Some(n.clone())
                                }
                                _ => None,
                            })
                            .collect();
                        Ok((removed, format!("{:?}", result.applied)))
                    },
                );
                (ClientOp::CheckRepair, body)
            }
            #[cfg(test)]
            Call::BackupRecordingRecheck {
                snapshot,
                tree,
                rechecks,
            } => {
                let src = self.prepare_backup_source(&snapshot, tree);
                let op = ClientOp::Backup {
                    snapshot: snapshot.clone(),
                };
                let body: Body = Box::new(
                    move |store: &SimStore, cfg: VykarConfig, _: &Path, _: usize, env: HostEnv| {
                        let handle = store.handle(c);
                        let wrap = move |_: Box<dyn StorageBackend>| -> Box<dyn StorageBackend> {
                            Box::new(handle.clone())
                        };
                        let source_paths = vec![src.to_string_lossy().into_owned()];
                        let req = backup::BackupRequest {
                            snapshot_name: &snapshot,
                            passphrase: cfg.encryption.passphrase.as_deref(),
                            source_paths: &source_paths,
                            source_label: "sim",
                            exclude_patterns: &[],
                            exclude_if_present: &[],
                            one_file_system: true,
                            git_ignore: false,
                            xattrs_enabled: false,
                            compression: Compression::None,
                            command_dumps: &[],
                            verbose: false,
                        };
                        let cancel = CancelToken::new();
                        let mut pending =
                            backup::upload(&cfg, &req, &mut None, &cancel, Some(&wrap), env)
                                .map_err(|(_, e)| e)?;
                        if let Err(e) = pending.protect_commit(&mut None, &cancel) {
                            pending.abandon();
                            return Err(e);
                        }
                        let mut commit = pending.begin_commit();
                        rechecks.store(
                            commit.rechecks_reuse_always(),
                            std::sync::atomic::Ordering::SeqCst,
                        );
                        let result = commit.drive(&|_: crate::repo::control::CommitEvent| {});
                        let outcome = commit.finish(result, &mut None, &cancel)?;
                        Ok((Vec::new(), format!("partial: {}", outcome.is_partial)))
                    },
                );
                (op, body)
            }
            Call::ExclusiveCustom { label, f } => {
                let body: Body = Box::new(
                    move |store: &SimStore, cfg: VykarConfig, _: &Path, _: usize, env: HostEnv| {
                        let mut repo = open_for(store, c, &cfg, env)?;
                        util::with_exclusive_session(&mut repo, f)?;
                        Ok((Vec::new(), String::new()))
                    },
                );
                (ClientOp::Custom { label }, body)
            }
            Call::ExclusiveDelete { names } => {
                let op = ClientOp::Delete {
                    names: names.clone(),
                };
                let body: Body = Box::new(
                    move |store: &SimStore, cfg: VykarConfig, _: &Path, _: usize, env: HostEnv| {
                        let mut repo = open_for(store, c, &cfg, env)?;
                        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
                        let result = util::with_exclusive_session(&mut repo, |w| {
                            delete::delete_on_writer(w, &refs, false, &CancelToken::new())
                        })?;
                        let removed = result
                            .stats
                            .iter()
                            .map(|s| s.snapshot_name.clone())
                            .collect();
                        Ok((removed, result.warnings.join("; ")))
                    },
                );
                (op, body)
            }
            Call::SwitchMode { mode } => {
                let body: Body = Box::new(
                    move |store: &SimStore, cfg: VykarConfig, _: &Path, _: usize, env: HostEnv| {
                        let mut repo = util::open_repo_on(
                            Box::new(store.handle(c)),
                            &util::open_request(&cfg),
                            cfg.encryption.passphrase.as_deref(),
                            OpenOptions::new().legacy_format().host_env(env),
                        )?;
                        let outcome =
                            crate::commands::upgrade::upgrade_repo(&mut repo, Some(mode))?;
                        Ok((Vec::new(), format!("{outcome:?}")))
                    },
                );
                (
                    ClientOp::Custom {
                        label: "switch-mode".into(),
                    },
                    body,
                )
            }
            Call::Restore { snapshot } => {
                let op = ClientOp::Restore {
                    snapshot: snapshot.clone(),
                };
                let body: Body = Box::new(
                    move |store: &SimStore,
                          cfg: VykarConfig,
                          dir: &Path,
                          idx: usize,
                          env: HostEnv| {
                        let mut repo = util::open_repo_on(
                            Box::new(store.handle(c)),
                            &util::open_request(&cfg),
                            cfg.encryption.passphrase.as_deref(),
                            OpenOptions::new().host_env(env),
                        )?;
                        let dest = dir.join(format!("restore-{idx}"));
                        let stats = restore::run_on_repo(
                            &mut repo,
                            &snapshot,
                            &dest.to_string_lossy(),
                            None,
                            RestoreOptions {
                                xattrs_enabled: false,
                                verify_chunks: true,
                                continue_on_error: false,
                            },
                            &CancelToken::new(),
                        )?;
                        Ok((Vec::new(), format!("{} files", stats.files)))
                    },
                );
                (op, body)
            }
            Call::ClearSessions => {
                let body: Body = Box::new(
                    move |store: &SimStore, _: VykarConfig, _: &Path, _: usize, _: HostEnv| {
                        let handle = store.handle(c);
                        let locks = sessions::clear_legacy_locks(&handle)?;
                        let cleared = sessions::clear_all_sessions(&handle)?;
                        Ok((Vec::new(), format!("locks {locks}, sessions {cleared}")))
                    },
                );
                (ClientOp::ClearSessions, body)
            }
            Call::Custom { label, f } => {
                let body: Body = Box::new(
                    move |store: &SimStore, cfg: VykarConfig, _: &Path, _: usize, env: HostEnv| {
                        custom_body(store, c, cfg, env, f)
                    },
                );
                (ClientOp::Custom { label }, body)
            }
        }
    }

    /// The op and the body of a backup of `tree` as `snapshot`, stopped by
    /// `cancel`.
    fn prepare_backup(
        &mut self,
        c: ClientId,
        snapshot: String,
        tree: SourceTree,
        cancel: CancelToken,
    ) -> (ClientOp, Body) {
        let src = self.prepare_backup_source(&snapshot, tree);
        self.backup_body(c, snapshot, src, cancel)
    }

    /// The op and the body of a backup of `src` as `snapshot`.
    fn backup_body(
        &mut self,
        c: ClientId,
        snapshot: String,
        src: PathBuf,
        cancel: CancelToken,
    ) -> (ClientOp, Body) {
        let op = ClientOp::Backup {
            snapshot: snapshot.clone(),
        };
        let body: Body = Box::new(
            move |store: &SimStore, cfg: VykarConfig, _: &Path, _: usize, env: HostEnv| {
                let handle = store.handle(c);
                let wrap = move |_: Box<dyn StorageBackend>| -> Box<dyn StorageBackend> {
                    Box::new(handle.clone())
                };
                let source_paths = vec![src.to_string_lossy().into_owned()];
                let outcome = backup::run_impl(
                    &cfg,
                    backup::BackupRequest {
                        snapshot_name: &snapshot,
                        passphrase: cfg.encryption.passphrase.as_deref(),
                        source_paths: &source_paths,
                        source_label: "sim",
                        exclude_patterns: &[],
                        exclude_if_present: &[],
                        one_file_system: true,
                        git_ignore: false,
                        xattrs_enabled: false,
                        compression: Compression::None,
                        command_dumps: &[],
                        verbose: false,
                    },
                    None,
                    &cancel,
                    Some(&wrap),
                    env,
                )?;
                Ok((Vec::new(), format!("partial: {}", outcome.is_partial)))
            },
        );
        (op, body)
    }

    /// Write the source tree of backup `snapshot` and record it for the
    /// checker; returns the source directory.
    pub(super) fn prepare_backup_source(&mut self, snapshot: &str, tree: SourceTree) -> PathBuf {
        assert!(
            !self.history.trees.contains_key(snapshot),
            "snapshot name {snapshot} reused"
        );
        let src = self.root.path().join("sources").join(snapshot);
        tree.write_to(&src).expect("write source tree");
        // Items carry mtime and ctime, and a tree chunk is stored by its
        // content: two snapshots of equal trees would share their tree
        // chunk exactly when both sources were written within one
        // timestamp tick of the filesystem (4 ms on tmpfs), and the later
        // backup would then commit without a segment. Each snapshot's
        // files get an mtime of their own, derived from its name, so no
        // schedule depends on the wall clock.
        let h = snapshot.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        });
        let stamp = std::time::UNIX_EPOCH
            + std::time::Duration::new(
                1_000_000_000 + h % 500_000_000,
                ((h >> 32) % 1_000_000_000) as u32,
            );
        for rel in tree.files.keys() {
            std::fs::File::options()
                .write(true)
                .open(src.join(rel))
                .and_then(|f| f.set_modified(stamp))
                .expect("stamp source file");
        }
        self.history.trees.insert(snapshot.to_string(), tree);
        src
    }

    /// A new `Sim` on a fork of this one's store, with its history (runs,
    /// observations, source trees) and run counter, for one case of the
    /// driver. Its clients are new: this one's are not carried over.
    pub(super) fn fork_for_case(&self) -> Sim {
        self.fork_for_case_as(self.store.config().kind)
    }

    /// [`Self::fork_for_case`] on a store of another kind.
    pub(super) fn fork_for_case_as(&self, kind: StoreKind) -> Sim {
        assert!(
            self.in_flight.is_empty(),
            "fork_for_case with runs in flight"
        );
        let mut sim = Sim::from_store(self.store.fork_as(kind));
        sim.history = self.history.clone();
        sim.next_run = self.next_run;
        sim
    }

    /// Record a run the driver started with `begin_run` and ran itself.
    pub(super) fn finish_driven_run(
        &mut self,
        c: ClientId,
        index: usize,
        op: ClientOp,
        first_step: u64,
        result: RunResult,
    ) -> RunRecord {
        let finished = finish_run(
            &self.store,
            &self.hosts,
            c,
            index,
            false,
            first_step,
            result,
        );
        self.record(index, c, op, finished)
    }

    /// Reserve the run's index, tick the clock, and start `c`'s process.
    pub(super) fn begin_run(&mut self, c: ClientId) -> Started {
        let index = self.next_run;
        self.next_run += 1;
        let cfg = self.client_config(c);
        let dir = self.client_dir(c).to_path_buf();
        self.time.advance(RUN_TICK);
        // Each run is a new process: a fresh PID, alive until it returns.
        self.hosts.start_run(c);
        let env = self.host_env(c);
        self.store.set_current_run(c, Some(index));
        Started {
            index,
            cfg,
            dir,
            env,
        }
    }

    /// Run `body` on the calling thread and record it.
    fn run_inline(
        &mut self,
        c: ClientId,
        op: ClientOp,
        body: impl FnOnce(&SimStore, VykarConfig, &Path, usize, HostEnv) -> RunResult,
    ) -> RunRecord {
        let is_init = matches!(op, ClientOp::Init);
        let started = self.begin_run(c);
        let first_step = self.store.step();
        let result = body(
            &self.store,
            started.cfg,
            &started.dir,
            started.index,
            started.env,
        );
        let finished = finish_run(
            &self.store,
            &self.hosts,
            c,
            started.index,
            is_init,
            first_step,
            result,
        );
        self.record(started.index, c, op, finished)
    }

    /// Insert the run in index order and its observation in step order.
    fn record(&mut self, index: usize, c: ClientId, op: ClientOp, f: Finished) -> RunRecord {
        // A shared run: a backup once it marked its session `committing`,
        // maintenance (compact, prune, delete, a shared custom run) by its
        // registered session marker. An exclusive session's marker (`x-`,
        // step 8: repair, a mode switch, a custom run) is not counted;
        // invariant 13 judges such a run against it.
        let shared = match op {
            ClientOp::Backup { .. } => self.store.marked_committing_in_run(index),
            ClientOp::Compact
            | ClientOp::Prune
            | ClientOp::Delete { .. }
            | ClientOp::Custom { .. } => self.store.registered_in_run(index),
            _ => false,
        };
        let record = RunRecord {
            index,
            client: c,
            op,
            first_step: f.first_step,
            last_step: f.last_step,
            outcome: f.outcome,
            shared,
            exclusive: self.store.exclusive_in_run(index),
        };
        let at = self.history.runs.partition_point(|r| r.index < index);
        self.history.runs.insert(at, record.clone());
        self.insert_observation(f.observation);
        self.in_flight.remove(&c);
        record
    }

    fn insert_observation(&mut self, obs: Observation) {
        let at = self
            .history
            .observations
            .partition_point(|o| o.step <= obs.step);
        self.history.observations.insert(at, obs);
    }

    fn observe(&mut self, cause: ObservationCause) {
        let obs = observe_store(&self.store, cause);
        self.insert_observation(obs);
    }
}

/// Open the repository as delete, prune and compact do.
fn open_for(store: &SimStore, c: ClientId, cfg: &VykarConfig, env: HostEnv) -> Result<Repo> {
    util::open_repo_on(
        Box::new(store.handle(c)),
        &util::open_request(cfg),
        cfg.encryption.passphrase.as_deref(),
        OpenOptions::new().host_env(env),
    )
}

fn custom_body(
    store: &SimStore,
    c: ClientId,
    cfg: VykarConfig,
    env: HostEnv,
    f: impl FnOnce(&mut RepoWriter<'_>) -> Result<()>,
) -> RunResult {
    let mut repo = open_for(store, c, &cfg, env)?;
    util::with_exclusive_session(&mut repo, f)?;
    Ok((Vec::new(), String::new()))
}

/// End of a run, on the run's thread: untag it, end its process, and take
/// the observation of the store it left.
fn finish_run(
    store: &SimStore,
    hosts: &SimHosts,
    c: ClientId,
    index: usize,
    is_init: bool,
    first_step: u64,
    result: RunResult,
) -> Finished {
    let last_step = store.step();
    store.set_current_run(c, None);
    hosts.set_running(c, false);
    let outcome = match result {
        Ok((removed, detail)) => RunOutcome::Ok { removed, detail },
        Err(e) => RunOutcome::Err(ErrorRecord {
            display: e.to_string(),
            class: classify(&e),
        }),
    };
    let observation = observe_store(
        store,
        if is_init {
            ObservationCause::Init
        } else {
            ObservationCause::Run(index)
        },
    );
    Finished {
        outcome,
        first_step,
        last_step,
        observation,
    }
}

/// The snapshot set the store holds now, read through a fresh observer
/// open. A failing open is a harness failure.
fn observe_store(store: &SimStore, cause: ObservationCause) -> Observation {
    let cache = tempfile::tempdir().expect("observer cache");
    let repo = Repo::open(
        Box::new(store.observer()),
        store.passphrase(),
        Some(cache.path().to_path_buf()),
        OpenOptions::new()
            .tolerate_broken_index()
            .skip_key_backfill(),
    )
    .unwrap_or_else(|e| panic!("observer open failed after {cause:?}: {e}"));
    let by_key: BTreeMap<String, String> = repo
        .manifest()
        .snapshots
        .iter()
        .map(|e| (e.id.storage_key(), e.name.clone()))
        .collect();
    let snapshots = store
        .keys("snapshots/")
        .into_iter()
        .map(|k| {
            let name = by_key.get(&k).cloned();
            (k, name)
        })
        .collect();
    Observation {
        cause,
        step: store.step(),
        snapshots,
    }
}
