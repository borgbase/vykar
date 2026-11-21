//! `Sim`: the sequential runner. Clients run one after another on the
//! calling thread (their own worker threads are real), each with its own
//! cache directory and all through one `SimStore`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use vykar_storage::StorageBackend;
use vykar_types::error::Result;

use super::checker::{Invariant, Report, check_invariants};
use super::fault::{ClientId, FaultRule, ParkedId, RuleId};
use super::history::{
    ClientOp, ErrorRecord, History, Observation, ObservationCause, RunOutcome, RunRecord, classify,
};
use super::store::{SimConfig, SimStore};
use crate::commands::check::{self, RepairAction, RepairMode};
use crate::commands::restore::{self, RestoreOptions};
use crate::commands::{backup, compact, delete, prune, util};
use crate::compress::Compression;
use crate::config::{
    ChunkerConfig, EncryptionConfig, EncryptionModeConfig, RepositoryConfig, RetentionConfig,
    VykarConfig,
};
use crate::repo::control::CancelToken;
use crate::repo::{EncryptionMode, OpenOptions, Repository, identity, lock};

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
type RunResult = Result<(Vec<String>, String)>;

pub struct Sim {
    store: SimStore,
    root: tempfile::TempDir,
    repo_url: String,
    history: History,
    /// Client root: `cache/`, `restore-<run>/`.
    clients: BTreeMap<ClientId, PathBuf>,
}

impl Default for Sim {
    fn default() -> Self {
        Self::new()
    }
}

impl Sim {
    pub fn new() -> Self {
        Self::with_config(SimConfig::default())
    }

    /// Creates the store, registers client "init" and runs Init as run 0.
    pub fn with_config(config: SimConfig) -> Self {
        #[cfg(test)]
        crate::testutil::init_test_environment();

        let root = tempfile::tempdir().expect("sim root");
        // Never read or written: backup builds a LocalBackend for it and the
        // wrapper replaces it (LocalBackend::new only canonicalizes).
        let url_dir = root.path().join("repo-url-placeholder");
        std::fs::create_dir_all(&url_dir).expect("placeholder dir");
        let repo_url = url_dir.to_string_lossy().into_owned();
        let mut sim = Self {
            store: SimStore::new(config),
            root,
            repo_url,
            history: History::default(),
            clients: BTreeMap::new(),
        };
        let init = sim.client("init");
        sim.run(init, ClientOp::Init, |store, cfg, _, _| {
            let repo = Repository::init(
                Box::new(store.handle(init)),
                EncryptionMode::None,
                ChunkerConfig::default(),
                None,
                Some(&cfg.repository),
                util::cache_dir_from_config(&cfg),
            )?;
            // As `commands::init`: pin at once, with the init client's cache.
            identity::verify_or_pin(
                &cfg.repository.url,
                &repo.config.id,
                repo.crypto.chunk_hasher().key(),
                util::cache_dir_from_config(&cfg).as_deref(),
                true,
            )?;
            Ok((Vec::new(), String::new()))
        })
        .expect_ok();
        sim
    }

    pub fn store(&self) -> &SimStore {
        &self.store
    }

    pub fn history(&self) -> &History {
        &self.history
    }

    pub fn client(&mut self, name: &str) -> ClientId {
        let id = self.store.register_client(name);
        let dir = self
            .root
            .path()
            .join("clients")
            .join(format!("{}-{name}", id.0));
        std::fs::create_dir_all(dir.join("cache")).expect("client dir");
        self.clients.insert(id, dir);
        id
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
        self.store.crash(client);
    }

    pub fn release(&mut self, id: ParkedId) {
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
        self.store.discard(id).expect("discard");
    }

    /// One observation per release.
    pub fn release_all(&mut self) -> Vec<ParkedId> {
        let ids: Vec<ParkedId> = self.store.parked().iter().map(|p| p.id).collect();
        for id in &ids {
            self.release(*id);
        }
        ids
    }

    pub fn tamper_delete(&mut self, key: &str) -> bool {
        let existed = self.store.tamper_delete(key);
        self.observe(ObservationCause::Tamper);
        existed
    }

    pub fn tamper_put(&mut self, key: &str, data: &[u8]) {
        self.store.tamper_put(key, data);
        self.observe(ObservationCause::Tamper);
    }

    pub fn backup(&mut self, c: ClientId, snapshot: &str, tree: &SourceTree) -> RunRecord {
        assert!(
            !self.history.trees.contains_key(snapshot),
            "snapshot name {snapshot} reused"
        );
        // A live-looking lock would make the commit wait 10 minutes of real
        // time; step 1 has no clock to age it out.
        let locks = self.store.keys("locks/");
        assert!(
            locks.is_empty(),
            "backup {snapshot}: lock {locks:?} present; step 1 has no clock; run break_lock first"
        );
        let src = self.root.path().join("sources").join(snapshot);
        tree.write_to(&src).expect("write source tree");
        self.history
            .trees
            .insert(snapshot.to_string(), tree.clone());
        let name = snapshot.to_string();
        self.run(
            c,
            ClientOp::Backup {
                snapshot: name.clone(),
            },
            move |store, cfg, _, _| {
                let handle = store.handle(c);
                let wrap = move |_: Box<dyn StorageBackend>| -> Box<dyn StorageBackend> {
                    Box::new(handle.clone())
                };
                let source_paths = vec![src.to_string_lossy().into_owned()];
                let outcome = backup::run_impl(
                    &cfg,
                    backup::BackupRequest {
                        snapshot_name: &name,
                        passphrase: None,
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
                    &CancelToken::new(),
                    Some(&wrap),
                )?;
                Ok((Vec::new(), format!("partial: {}", outcome.is_partial)))
            },
        )
    }

    pub fn delete(&mut self, c: ClientId, names: &[&str]) -> RunRecord {
        let op = ClientOp::Delete {
            names: names.iter().map(|n| n.to_string()).collect(),
        };
        self.run(c, op, |store, cfg, _, _| {
            let mut repo =
                util::open_repo_on(Box::new(store.handle(c)), &cfg, None, OpenOptions::new())?;
            let result = delete::run_on_repo(&mut repo, names, false, &CancelToken::new())?;
            let removed = result
                .stats
                .iter()
                .map(|s| s.snapshot_name.clone())
                .collect();
            Ok((removed, result.warnings.join("; ")))
        })
    }

    pub fn prune(&mut self, c: ClientId, retention: RetentionConfig) -> RunRecord {
        self.run(c, ClientOp::Prune, |store, mut cfg, _, _| {
            cfg.retention = retention;
            let mut repo =
                util::open_repo_on(Box::new(store.handle(c)), &cfg, None, OpenOptions::new())?;
            let (stats, list) =
                prune::run_on_repo(&mut repo, &cfg, false, true, &[], &[], &CancelToken::new())?;
            let removed = list
                .into_iter()
                .filter(|e| e.action == "prune")
                .map(|e| e.snapshot_name)
                .collect();
            Ok((
                removed,
                format!("kept {}, pruned {}", stats.kept, stats.pruned),
            ))
        })
    }

    pub fn compact(&mut self, c: ClientId) -> RunRecord {
        self.run(c, ClientOp::Compact, |store, cfg, _, _| {
            let mut repo =
                util::open_repo_on(Box::new(store.handle(c)), &cfg, None, OpenOptions::new())?;
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
                    "packs {} repacked {} deleted_empty {} orphan {} corrupt {}",
                    s.packs_total,
                    s.packs_repacked,
                    s.packs_deleted_empty,
                    s.packs_orphan,
                    s.packs_corrupt
                ),
            ))
        })
    }

    pub fn check(&mut self, c: ClientId) -> RunRecord {
        self.run(c, ClientOp::Check, |store, cfg, _, _| {
            let (mut repo, _session_guard) = util::open_repo_with_read_session_on(
                Arc::new(store.handle(c)),
                &cfg,
                None,
                OpenOptions::new(),
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
        })
    }

    /// `check --repair` in `RepairMode::Apply`.
    pub fn check_repair(&mut self, c: ClientId) -> RunRecord {
        self.run(c, ClientOp::CheckRepair, |store, cfg, _, _| {
            let mode = RepairMode::Apply;
            let mut repo = util::open_repo_on(
                Box::new(store.handle(c)),
                &cfg,
                None,
                check::repair_open_options(mode),
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
                    RepairAction::RemoveCorruptSnapshot { name: Some(n), .. } => Some(n.clone()),
                    _ => None,
                })
                .collect();
            Ok((removed, format!("{:?}", result.applied)))
        })
    }

    pub fn restore(&mut self, c: ClientId, snapshot: &str) -> RunRecord {
        let op = ClientOp::Restore {
            snapshot: snapshot.to_string(),
        };
        self.run(c, op, |store, cfg, dir, idx| {
            let (mut repo, _session_guard) = util::open_repo_with_read_session_on(
                Arc::new(store.handle(c)),
                &cfg,
                None,
                OpenOptions::new(),
            )?;
            let dest = dir.join(format!("restore-{idx}"));
            let stats = restore::run_on_repo(
                &mut repo,
                snapshot,
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
        })
    }

    pub fn break_lock(&mut self, c: ClientId, sessions: bool) -> RunRecord {
        self.run(c, ClientOp::BreakLock { sessions }, |store, _, _, _| {
            let handle = store.handle(c);
            let locks = lock::break_lock(&handle)?;
            let cleared = if sessions {
                lock::clear_all_sessions(&handle)?
            } else {
                0
            };
            Ok((Vec::new(), format!("locks {locks}, sessions {cleared}")))
        })
    }

    /// Opens like prune/delete/compact and runs `f` under `with_repo_lock`.
    pub fn custom(
        &mut self,
        c: ClientId,
        label: &str,
        f: impl FnOnce(&mut Repository) -> Result<()>,
    ) -> RunRecord {
        let op = ClientOp::Custom {
            label: label.to_string(),
        };
        self.run(c, op, |store, cfg, _, _| {
            let mut repo =
                util::open_repo_on(Box::new(store.handle(c)), &cfg, None, OpenOptions::new())?;
            util::with_repo_lock(&mut repo, f)?;
            Ok((Vec::new(), String::new()))
        })
    }

    pub fn check_invariants(&self, set: &[Invariant]) -> Report {
        check_invariants(&self.store, &self.history, set)
    }

    fn client_config(&self, c: ClientId) -> VykarConfig {
        let mut cfg = VykarConfig {
            repository: RepositoryConfig {
                url: self.repo_url.clone(),
                ..Default::default()
            },
            encryption: EncryptionConfig {
                mode: EncryptionModeConfig::None,
                passphrase: None,
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
        cfg
    }

    fn run(
        &mut self,
        c: ClientId,
        op: ClientOp,
        body: impl FnOnce(&SimStore, VykarConfig, &Path, usize) -> RunResult,
    ) -> RunRecord {
        let index = self.history.runs.len();
        let cfg = self.client_config(c);
        let dir = self.client_dir(c).to_path_buf();
        let is_init = matches!(op, ClientOp::Init);
        self.store.set_current_run(Some(index));
        let first_step = self.store.step();
        let result = body(&self.store, cfg, &dir, index);
        let last_step = self.store.step();
        self.store.set_current_run(None);
        let outcome = match result {
            Ok((removed, detail)) => RunOutcome::Ok { removed, detail },
            Err(e) => RunOutcome::Err(ErrorRecord {
                display: e.to_string(),
                class: classify(&e),
            }),
        };
        let record = RunRecord {
            index,
            client: c,
            op,
            first_step,
            last_step,
            outcome,
        };
        self.history.runs.push(record.clone());
        self.observe(if is_init {
            ObservationCause::Init
        } else {
            ObservationCause::Run(index)
        });
        record
    }

    /// Record the snapshot set the store holds now, read through a fresh
    /// observer open. A failing open is a harness failure.
    fn observe(&mut self, cause: ObservationCause) {
        let cache = tempfile::tempdir().expect("observer cache");
        let repo = Repository::open(
            Box::new(self.store.observer()),
            None,
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
        let snapshots = self
            .store
            .keys("snapshots/")
            .into_iter()
            .map(|k| {
                let name = by_key.get(&k).cloned();
                (k, name)
            })
            .collect();
        self.history.observations.push(Observation {
            cause,
            step: self.store.step(),
            snapshots,
        });
    }
}
