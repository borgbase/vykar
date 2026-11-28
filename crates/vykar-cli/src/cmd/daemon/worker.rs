//! Background status worker for the daemon page (GitHub #206).
//!
//! One thread owns all status I/O and per-repo status state, so a slow or
//! unreachable repository never blocks the daemon loop, the HTTP page, or the
//! other repos' rows. The daemon loop only sends messages:
//!
//! - `Configure` at startup and on reload,
//! - `CycleDone` after each backup cycle, naming the repos that ran and the
//!   repos skipped by a hook or because they were unavailable.
//!
//! Both carry the passphrases the daemon's cycles accepted. The worker never
//! runs a passcommand: a repo whose passphrase comes from one is not contacted
//! until a cycle accepted its passphrase, and stays `pending` until then.
//! Status I/O reuses that value; if the secret behind the passcommand changes,
//! a refresh before the next cycle fails and marks the row `error`, and the
//! next cycle hands over the new value. A repo whose passphrase was withdrawn
//! (a reload changed its source, or a cycle handed over none) keeps its last
//! state and numbers and is not refreshed until a later cycle hands over a
//! value.
//!
//! A skipped repo is not contacted until its next cycle runs. A repo whose
//! status fetch or poll fails is marked `error` and is likewise left alone
//! until its next cycle or a reload. Status I/O does not retry.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

use vykar_core::app::passphrase::resolving_runs_command;
use vykar_core::config::ResolvedRepo;
use vykar_types::error::Result;
use zeroize::Zeroizing;

use super::poll;
use super::release_malloc_arenas;
use super::status::{self, RepoInfo, RepoState, SharedStatus, SnapshotRow};

/// How often `Ok` repos are polled for out-of-band snapshot changes (#159).
const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Upper bound on how long the worker blocks waiting for a message, so it
/// notices `SHUTDOWN` and the end of a cycle promptly.
const TICK: Duration = Duration::from_secs(1);

pub(super) enum Msg {
    Configure {
        repos: Arc<Vec<ResolvedRepo>>,
        /// Index-aligned with `repos`: the value each accepted under its
        /// current passphrase source, if any.
        passphrases: Vec<Option<Zeroizing<String>>>,
    },
    CycleDone {
        ran: Vec<String>,
        skipped: Vec<String>,
        /// For repos in `ran`: the passphrase each accepted, if any.
        passphrases: Vec<(String, Option<Zeroizing<String>>)>,
    },
}

/// Repository I/O used by the worker; a trait so the state machine is
/// testable without a repository.
pub(super) trait StatusIo: Sync {
    fn list_ids(&self, repo: &ResolvedRepo) -> Result<HashSet<String>>;
    /// `pass` is a value a cycle accepted, else `None`: resolve from an
    /// inline passphrase or VYKAR_PASSPHRASE only, never a passcommand.
    fn fetch(
        &self,
        repo: &ResolvedRepo,
        pass: Option<&str>,
    ) -> Result<(RepoInfo, Vec<SnapshotRow>)>;
}

struct RepoStatusIo;

impl StatusIo for RepoStatusIo {
    fn list_ids(&self, repo: &ResolvedRepo) -> Result<HashSet<String>> {
        poll::list_snapshot_ids(repo)
    }

    fn fetch(
        &self,
        repo: &ResolvedRepo,
        pass: Option<&str>,
    ) -> Result<(RepoInfo, Vec<SnapshotRow>)> {
        status::fetch_repo(repo, pass)
    }
}

struct Entry {
    name: String,
    /// The repo's config with retries disabled; used for all status I/O.
    repo: ResolvedRepo,
    state: RepoState,
    info: RepoInfo,
    snapshots: Vec<SnapshotRow>,
    baseline: Option<HashSet<String>>,
    dirty: bool,
    /// Accepted by the daemon's cycles under the current source; never printed.
    passphrase: Option<Zeroizing<String>>,
}

impl Entry {
    /// Status I/O would not run a passcommand: a cycle handed a value, or
    /// resolution uses an inline passphrase or VYKAR_PASSPHRASE.
    fn can_resolve(&self) -> bool {
        self.passphrase.is_some() || !resolving_runs_command(&self.repo.config)
    }
}

/// Status I/O must not sit through the backend retry schedule: a dead repo
/// costs one connect timeout, not minutes.
fn without_retries(repo: &ResolvedRepo) -> ResolvedRepo {
    let mut repo = repo.clone();
    repo.config.repository.retry.max_retries = 0;
    repo
}

struct Worker<I: StatusIo> {
    io: I,
    status: SharedStatus,
    cycle_running: Arc<AtomicBool>,
    shutdown: &'static AtomicBool,
    entries: Vec<Entry>,
    next_poll: Instant,
}

impl<I: StatusIo> Worker<I> {
    fn new(
        io: I,
        status: SharedStatus,
        cycle_running: Arc<AtomicBool>,
        shutdown: &'static AtomicBool,
    ) -> Self {
        Self {
            io,
            status,
            cycle_running,
            shutdown,
            entries: Vec::new(),
            next_poll: Instant::now() + STATUS_POLL_INTERVAL,
        }
    }

    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Configure { repos, passphrases } => self.configure(&repos, passphrases),
            Msg::CycleDone {
                ran,
                skipped,
                mut passphrases,
            } => {
                for e in &mut self.entries {
                    if ran.contains(&e.name) {
                        e.dirty = true;
                        e.passphrase = passphrases
                            .iter_mut()
                            .find(|(name, _)| *name == e.name)
                            .and_then(|(_, p)| p.take());
                    } else if skipped.contains(&e.name) {
                        e.state = RepoState::Skipped;
                        e.dirty = false;
                    }
                }
                self.publish();
            }
        }
    }

    fn configure(&mut self, repos: &[ResolvedRepo], passphrases: Vec<Option<Zeroizing<String>>>) {
        let mut old = std::mem::take(&mut self.entries);
        let mut passphrases = passphrases.into_iter();
        for repo in repos {
            let passphrase = passphrases.next().flatten();
            let name = repo.label_or_url().to_string();
            let url = &repo.config.repository.url;
            let kept = old
                .iter()
                .position(|e| e.name == name && &e.repo.config.repository.url == url)
                .map(|i| old.swap_remove(i));
            let entry = match kept {
                Some(mut e) => {
                    e.repo = without_retries(repo);
                    e.passphrase = passphrase;
                    if e.state == RepoState::Error {
                        // The reload may have fixed it.
                        e.state = RepoState::Pending;
                        e.dirty = true;
                    }
                    e
                }
                None => Entry {
                    name,
                    repo: without_retries(repo),
                    state: RepoState::Pending,
                    info: RepoInfo::placeholder(repo),
                    snapshots: Vec::new(),
                    baseline: None,
                    dirty: true,
                    passphrase,
                },
            };
            self.entries.push(entry);
        }
        self.publish();
    }

    fn publish(&self) {
        let rows = self
            .entries
            .iter()
            .map(|e| {
                let mut row = e.info.clone();
                row.status = e.state;
                row
            })
            .collect();
        let snapshots = self
            .entries
            .iter()
            .flat_map(|e| e.snapshots.iter().cloned())
            .collect();
        status::publish(&self.status, rows, snapshots);
    }

    fn may_do_io(&self) -> bool {
        !self.shutdown.load(Ordering::SeqCst) && !self.cycle_running.load(Ordering::SeqCst)
    }

    /// Refresh every dirty entry, publishing after each. Returns whether any
    /// I/O was attempted.
    ///
    /// The LISTs run in parallel, since that is where an unreachable repo
    /// spends its connect timeout; each full fetch then runs on this thread as
    /// soon as its LIST succeeds, so a dead repo never holds back the others'
    /// rows and only one index is loaded at a time.
    fn refresh_dirty(&mut self) -> bool {
        if !self.may_do_io() {
            return false;
        }
        // A dirty entry that cannot resolve without a passcommand stays dirty
        // and keeps its state until a cycle hands it a passphrase.
        let dirty: Vec<(usize, ResolvedRepo, Option<Zeroizing<String>>)> = self
            .entries
            .iter_mut()
            .enumerate()
            .filter(|(_, e)| e.dirty && e.can_resolve())
            .map(|(i, e)| {
                e.dirty = false;
                (i, e.repo.clone(), e.passphrase.clone())
            })
            .collect();
        if dirty.is_empty() {
            return false;
        }

        let (tx, rx) = mpsc::channel();
        std::thread::scope(|scope| {
            for (idx, repo, _) in &dirty {
                let tx = tx.clone();
                let io = &self.io;
                scope.spawn(move || {
                    let _ = tx.send((*idx, io.list_ids(repo)));
                });
            }
            drop(tx);

            for (idx, listed) in rx {
                let (_, repo, pass) = dirty.iter().find(|(i, ..)| *i == idx).expect("sent idx");
                let result = match listed {
                    // A cycle started meanwhile: leave the fetch for later.
                    Ok(_) if !self.may_do_io() => {
                        if let Some(entry) = self.entries.get_mut(idx) {
                            entry.dirty = true;
                        }
                        continue;
                    }
                    Ok(ids) => self
                        .io
                        .fetch(repo, pass.as_deref().map(String::as_str))
                        .map(|fetched| (ids, fetched)),
                    Err(e) => Err(e),
                };
                let Some(entry) = self.entries.get_mut(idx) else {
                    continue;
                };
                match result {
                    Ok((ids, (mut info, snapshots))) => {
                        info.refreshed_at = Some(status::now());
                        entry.info = info;
                        entry.snapshots = snapshots;
                        entry.baseline = Some(ids);
                        entry.state = RepoState::Ok;
                    }
                    Err(e) => {
                        tracing::warn!(
                            repo = %entry.name,
                            error = %e,
                            "status refresh failed; not contacting this repository again until its next cycle"
                        );
                        entry.state = RepoState::Error;
                    }
                }
                self.publish();
            }
        });
        true
    }

    /// LIST `Ok` entries and mark those whose snapshot set changed as dirty.
    fn poll(&mut self) {
        let mut failed = false;
        for entry in &mut self.entries {
            if entry.state != RepoState::Ok {
                continue;
            }
            if self.shutdown.load(Ordering::SeqCst) || self.cycle_running.load(Ordering::SeqCst) {
                break;
            }
            match self.io.list_ids(&entry.repo) {
                Ok(ids) => {
                    if entry.baseline.as_ref() != Some(&ids) {
                        tracing::debug!(repo = %entry.name, "status poll: snapshot set changed");
                        entry.dirty = true;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        repo = %entry.name,
                        error = %e,
                        "status poll failed; not polling this repository again until its next cycle"
                    );
                    entry.state = RepoState::Error;
                    failed = true;
                }
            }
        }
        if failed {
            self.publish();
        }
    }

    /// One unit of work after messages were applied: refresh dirty entries
    /// and, when due, poll. No I/O while a cycle runs.
    fn work(&mut self, now: Instant) {
        if !self.may_do_io() {
            return;
        }
        let mut did_io = self.refresh_dirty();
        if now >= self.next_poll {
            self.next_poll = now + STATUS_POLL_INTERVAL;
            self.poll();
            did_io |= self.refresh_dirty();
        }
        if did_io {
            release_malloc_arenas();
        }
    }

    fn run(mut self, rx: Receiver<Msg>) {
        loop {
            match rx.recv_timeout(TICK) {
                Ok(msg) => self.handle(msg),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
            while let Ok(msg) = rx.try_recv() {
                self.handle(msg);
            }
            if self.shutdown.load(Ordering::SeqCst) {
                return;
            }
            self.work(Instant::now());
        }
    }
}

/// Spawn the status worker. It exits on `shutdown` or when the sender is
/// dropped; it is not joined, since it may be inside a connect timeout.
pub(super) fn spawn(
    status: SharedStatus,
    cycle_running: Arc<AtomicBool>,
    shutdown: &'static AtomicBool,
) -> std::io::Result<Sender<Msg>> {
    let (tx, rx) = mpsc::channel();
    let worker = Worker::new(RepoStatusIo, status, cycle_running, shutdown);
    std::thread::Builder::new()
        .name("vykar-status".into())
        .spawn(move || worker.run(rx))?;
    Ok(tx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use vykar_core::config::{RepositoryConfig, VykarConfig};
    use vykar_types::error::VykarError;

    static NO_SHUTDOWN: AtomicBool = AtomicBool::new(false);

    #[derive(Default)]
    struct FakeIo {
        ids: Mutex<HashMap<String, HashSet<String>>>,
        failing: Mutex<HashSet<String>>,
        lists: Mutex<HashMap<String, usize>>,
        fetches: Mutex<HashMap<String, usize>>,
        passes: Mutex<HashMap<String, Option<String>>>,
    }

    impl FakeIo {
        fn fail(&self, url: &str, on: bool) {
            let mut f = self.failing.lock().unwrap();
            if on {
                f.insert(url.to_string());
            } else {
                f.remove(url);
            }
        }
        fn set_ids(&self, url: &str, ids: &[&str]) {
            self.ids.lock().unwrap().insert(
                url.to_string(),
                ids.iter().map(|s| (*s).to_string()).collect(),
            );
        }
        fn lists(&self, url: &str) -> usize {
            self.lists.lock().unwrap().get(url).copied().unwrap_or(0)
        }
        fn fetches(&self, url: &str) -> usize {
            self.fetches.lock().unwrap().get(url).copied().unwrap_or(0)
        }
        /// The `pass` of the last fetch of `url` (also `None` if never fetched).
        fn last_pass(&self, url: &str) -> Option<String> {
            self.passes.lock().unwrap().get(url).cloned().flatten()
        }
        fn assert_no_retries(repo: &ResolvedRepo) {
            assert_eq!(repo.config.repository.retry.max_retries, 0);
        }
        fn check(&self, repo: &ResolvedRepo) -> Result<()> {
            Self::assert_no_retries(repo);
            if self
                .failing
                .lock()
                .unwrap()
                .contains(&repo.config.repository.url)
            {
                return Err(VykarError::Other("unreachable".into()));
            }
            Ok(())
        }
    }

    impl StatusIo for &FakeIo {
        fn list_ids(&self, repo: &ResolvedRepo) -> Result<HashSet<String>> {
            let url = &repo.config.repository.url;
            *self.lists.lock().unwrap().entry(url.clone()).or_default() += 1;
            self.check(repo)?;
            Ok(self
                .ids
                .lock()
                .unwrap()
                .get(url)
                .cloned()
                .unwrap_or_default())
        }

        fn fetch(
            &self,
            repo: &ResolvedRepo,
            pass: Option<&str>,
        ) -> Result<(RepoInfo, Vec<SnapshotRow>)> {
            let url = &repo.config.repository.url;
            self.passes
                .lock()
                .unwrap()
                .insert(url.clone(), pass.map(str::to_string));
            let n = {
                let mut f = self.fetches.lock().unwrap();
                let n = f.entry(url.clone()).or_default();
                *n += 1;
                *n
            };
            self.check(repo)?;
            let mut info = RepoInfo::placeholder(repo);
            info.snapshot_count = Some(n);
            let entry = vykar_core::repo::manifest::SnapshotEntry {
                name: format!("{url}-{n}"),
                id: vykar_types::snapshot_id::SnapshotId::from_bytes([0; 32]),
                time: chrono::DateTime::from_timestamp(i64::try_from(n).unwrap() * 60, 0).unwrap(),
                source_label: String::new(),
                label: String::new(),
                source_paths: Vec::new(),
                hostname: String::new(),
            };
            let snap = SnapshotRow {
                repo_name: repo.label_or_url().to_string(),
                snapshot: vykar_core::app::views::SnapshotRecord::new(&entry, None),
            };
            Ok((info, vec![snap]))
        }
    }

    fn repo(label: &str, url: &str) -> ResolvedRepo {
        let config = VykarConfig {
            repository: RepositoryConfig {
                url: url.to_string(),
                ..RepositoryConfig::default()
            },
            ..VykarConfig::default()
        };
        ResolvedRepo {
            label: Some(label.to_string()),
            config,
            global_hooks: Default::default(),
            repo_hooks: Default::default(),
            sources: Vec::new(),
        }
    }

    /// [`repo`] whose passphrase comes from a passcommand.
    fn repo_with_passcommand(label: &str, url: &str, cmd: &str) -> ResolvedRepo {
        let mut r = repo(label, url);
        r.config.encryption.passcommand = Some(cmd.to_string());
        r
    }

    fn worker(io: &FakeIo) -> (Worker<&FakeIo>, Arc<AtomicBool>) {
        let running = Arc::new(AtomicBool::new(false));
        let w = Worker::new(io, status::new_shared(), running.clone(), &NO_SHUTDOWN);
        (w, running)
    }

    fn configure(w: &mut Worker<&FakeIo>, repos: Vec<ResolvedRepo>) {
        let passes = vec![None; repos.len()];
        configure_with(w, repos, &passes);
    }

    /// `passes` is index-aligned with `repos`.
    fn configure_with(w: &mut Worker<&FakeIo>, repos: Vec<ResolvedRepo>, passes: &[Option<&str>]) {
        w.handle(Msg::Configure {
            repos: Arc::new(repos),
            passphrases: passes
                .iter()
                .map(|p| p.map(|s| Zeroizing::new(s.to_string())))
                .collect(),
        });
    }

    fn cycle_done(w: &mut Worker<&FakeIo>, ran: &[&str], skipped: &[&str]) {
        cycle_done_with(w, &[], ran, skipped);
    }

    /// `passes` names the passphrase each repo in `ran` accepted; a repo in
    /// `ran` it does not name accepted none.
    fn cycle_done_with(
        w: &mut Worker<&FakeIo>,
        passes: &[(&str, Option<&str>)],
        ran: &[&str],
        skipped: &[&str],
    ) {
        w.handle(Msg::CycleDone {
            ran: ran.iter().map(|s| (*s).to_string()).collect(),
            skipped: skipped.iter().map(|s| (*s).to_string()).collect(),
            passphrases: passes
                .iter()
                .map(|(n, p)| ((*n).to_string(), p.map(|s| Zeroizing::new(s.to_string()))))
                .collect(),
        });
    }

    fn row(w: &Worker<&FakeIo>, name: &str) -> RepoInfo {
        let s = w.status.read().unwrap();
        s.repos.iter().find(|r| r.name == name).unwrap().clone()
    }

    fn later(w: &Worker<&FakeIo>) -> Instant {
        w.next_poll + Duration::from_secs(1)
    }

    #[test]
    fn configure_publishes_placeholders_before_io() {
        let io = FakeIo::default();
        let (mut w, _) = worker(&io);
        configure(&mut w, vec![repo("a", "/a"), repo("b", "/b")]);

        let s = w.status.read().unwrap();
        assert_eq!(s.repos.len(), 2);
        assert!(s.repos.iter().all(|r| r.status == RepoState::Pending));
        assert_eq!(io.lists("/a") + io.fetches("/a"), 0);
    }

    #[test]
    fn skipped_repo_is_never_contacted() {
        let io = FakeIo::default();
        let (mut w, running) = worker(&io);
        running.store(true, Ordering::SeqCst);
        configure(&mut w, vec![repo("dead", "/dead"), repo("ok", "/ok")]);
        cycle_done(&mut w, &["ok"], &["dead"]);
        running.store(false, Ordering::SeqCst);

        w.work(Instant::now());
        let t = later(&w);
        w.work(t);

        assert_eq!(io.lists("/dead") + io.fetches("/dead"), 0);
        assert_eq!(row(&w, "dead").status, RepoState::Skipped);
        assert_eq!(row(&w, "ok").status, RepoState::Ok);
        assert_eq!(io.fetches("/ok"), 1);
    }

    #[test]
    fn failure_keeps_previous_row_and_stops_contact_until_cycle() {
        let io = FakeIo::default();
        let (mut w, _) = worker(&io);
        configure(&mut w, vec![repo("r", "/r")]);
        w.work(Instant::now());
        assert_eq!(row(&w, "r").snapshot_count, Some(1));

        io.fail("/r", true);
        cycle_done(&mut w, &["r"], &[]);
        w.work(Instant::now());
        let r = row(&w, "r");
        assert_eq!(r.status, RepoState::Error);
        assert_eq!(r.snapshot_count, Some(1));
        assert!(r.refreshed_at.is_some());
        assert_eq!(w.status.read().unwrap().recent_snapshots.len(), 1);

        let calls = io.lists("/r") + io.fetches("/r");
        let t = later(&w);
        w.work(t);
        w.work(Instant::now());
        assert_eq!(io.lists("/r") + io.fetches("/r"), calls);

        io.fail("/r", false);
        cycle_done(&mut w, &["r"], &[]);
        w.work(Instant::now());
        assert_eq!(row(&w, "r").status, RepoState::Ok);
    }

    #[test]
    fn poll_failure_marks_error_and_stops_polling() {
        let io = FakeIo::default();
        let (mut w, _) = worker(&io);
        configure(&mut w, vec![repo("r", "/r")]);
        w.work(Instant::now());

        io.fail("/r", true);
        let t = later(&w);
        w.work(t);
        assert_eq!(row(&w, "r").status, RepoState::Error);

        let lists = io.lists("/r");
        let t = later(&w);
        w.work(t);
        assert_eq!(io.lists("/r"), lists);
    }

    #[test]
    fn poll_fetches_only_changed_repos() {
        let io = FakeIo::default();
        let (mut w, _) = worker(&io);
        configure(&mut w, vec![repo("a", "/a"), repo("b", "/b")]);
        w.work(Instant::now());
        assert_eq!((io.fetches("/a"), io.fetches("/b")), (1, 1));

        let t = later(&w);
        w.work(t);
        assert_eq!((io.fetches("/a"), io.fetches("/b")), (1, 1));
        assert_eq!(io.lists("/a"), 2);

        io.set_ids("/b", &["new"]);
        let t = later(&w);
        w.work(t);
        assert_eq!((io.fetches("/a"), io.fetches("/b")), (1, 2));
    }

    #[test]
    fn no_io_while_cycle_running() {
        let io = FakeIo::default();
        let (mut w, running) = worker(&io);
        running.store(true, Ordering::SeqCst);
        configure(&mut w, vec![repo("a", "/a")]);
        let t = later(&w);
        w.work(t);
        assert_eq!(io.lists("/a"), 0);

        running.store(false, Ordering::SeqCst);
        w.work(Instant::now());
        assert_eq!(io.fetches("/a"), 1);
    }

    #[test]
    fn reload_retains_skip_retries_error_and_drops_removed() {
        let io = FakeIo::default();
        let (mut w, running) = worker(&io);
        running.store(true, Ordering::SeqCst);
        configure(
            &mut w,
            vec![
                repo("skip", "/skip"),
                repo("err", "/err"),
                repo("gone", "/gone"),
                repo("moved", "/moved-old"),
            ],
        );
        cycle_done(&mut w, &["err", "gone", "moved"], &["skip"]);
        running.store(false, Ordering::SeqCst);
        io.fail("/err", true);
        w.work(Instant::now());
        assert_eq!(row(&w, "err").status, RepoState::Error);

        io.fail("/err", false);
        configure(
            &mut w,
            vec![
                repo("skip", "/skip"),
                repo("err", "/err"),
                repo("moved", "/moved-new"),
            ],
        );
        assert_eq!(row(&w, "skip").status, RepoState::Skipped);
        assert_eq!(row(&w, "err").status, RepoState::Pending);
        assert_eq!(row(&w, "moved").status, RepoState::Pending);
        assert_eq!(row(&w, "moved").url, "/moved-new");
        assert_eq!(w.status.read().unwrap().repos.len(), 3);

        w.work(Instant::now());
        assert_eq!(io.lists("/skip"), 0);
        assert_eq!(row(&w, "err").status, RepoState::Ok);
        assert_eq!(io.fetches("/moved-new"), 1);
    }

    #[test]
    fn queued_messages_coalesce_into_one_refresh() {
        let io = FakeIo::default();
        let (mut w, _) = worker(&io);
        let repos = vec![repo("a", "/a")];
        configure(&mut w, repos.clone());
        cycle_done(&mut w, &["a"], &[]);
        configure(&mut w, repos);
        cycle_done(&mut w, &["a"], &[]);
        w.work(Instant::now());
        assert_eq!(io.fetches("/a"), 1);
    }

    #[test]
    fn passcommand_repo_waits_for_a_cycle() {
        let io = FakeIo::default();
        let (mut w, _) = worker(&io);
        configure(
            &mut w,
            vec![
                repo_with_passcommand("pc", "/pc", "false"),
                repo("plain", "/plain"),
            ],
        );
        w.work(Instant::now());
        let t = later(&w);
        w.work(t);
        assert_eq!(io.lists("/pc") + io.fetches("/pc"), 0);
        assert_eq!(row(&w, "pc").status, RepoState::Pending);
        assert_eq!((io.fetches("/plain"), io.last_pass("/plain")), (1, None));
        assert_eq!(row(&w, "plain").status, RepoState::Ok);

        cycle_done_with(&mut w, &[("pc", Some("s"))], &["pc"], &[]);
        w.work(Instant::now());
        assert_eq!(io.fetches("/pc"), 1);
        assert_eq!(io.last_pass("/pc"), Some("s".into()));
        assert_eq!(row(&w, "pc").status, RepoState::Ok);
    }

    #[test]
    fn cycle_without_accepted_value_leaves_passcommand_repo_alone() {
        let io = FakeIo::default();
        let (mut w, _) = worker(&io);
        configure(&mut w, vec![repo_with_passcommand("pc", "/pc", "false")]);
        cycle_done(&mut w, &["pc"], &[]);
        w.work(Instant::now());
        let t = later(&w);
        w.work(t);
        assert_eq!(io.lists("/pc") + io.fetches("/pc"), 0);
        assert_eq!(row(&w, "pc").status, RepoState::Pending);
    }

    #[test]
    fn reload_takes_the_passphrase_the_service_kept() {
        let io = FakeIo::default();
        let (mut w, _) = worker(&io);
        configure(&mut w, vec![repo_with_passcommand("pc", "/pc", "old")]);
        cycle_done_with(&mut w, &[("pc", Some("s"))], &["pc"], &[]);
        w.work(Instant::now());
        assert_eq!(io.fetches("/pc"), 1);

        // The source changed, so the service dropped the value: no contact.
        io.set_ids("/pc", &["new"]);
        configure(&mut w, vec![repo_with_passcommand("pc", "/pc", "new")]);
        let t = later(&w);
        w.work(t);
        assert_eq!(io.fetches("/pc"), 1);

        // The service kept a value: it is used, also under a new label.
        configure_with(
            &mut w,
            vec![repo_with_passcommand("renamed", "/pc", "new")],
            &[Some("t")],
        );
        w.work(Instant::now());
        assert_eq!(io.fetches("/pc"), 2);
        assert_eq!(io.last_pass("/pc"), Some("t".into()));
        assert_eq!(row(&w, "renamed").status, RepoState::Ok);
    }

    #[test]
    fn skipped_cycle_keeps_handed_value() {
        let io = FakeIo::default();
        let (mut w, _) = worker(&io);
        configure(&mut w, vec![repo_with_passcommand("pc", "/pc", "false")]);
        cycle_done_with(&mut w, &[("pc", Some("s"))], &["pc"], &[]);
        cycle_done(&mut w, &[], &["pc"]);
        assert_eq!(row(&w, "pc").status, RepoState::Skipped);

        // The skip kept the value: once dirty again, the repo is fetched with it.
        w.entries[0].dirty = true;
        w.work(Instant::now());
        assert_eq!(io.fetches("/pc"), 1);
        assert_eq!(io.last_pass("/pc"), Some("s".into()));
    }
}
