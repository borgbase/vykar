//! Backup Phase 1 must leave every pack durable before Phase 2 takes the
//! commit lock, so the lock is never held across pack uploads.
//!
//! The `register_session` probe only lists `locks/`, so every `locks/` PUT is
//! the commit lock.

use std::fs;
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::error::{Result, VykarError};

use super::helpers::pass;
use crate::commands::backup;
use crate::repo::control::CancelToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    PackPutStart,
    PackPutEnd,
    LockPut,
    SnapshotPut,
    MarkerDelete,
    JournalDelete,
    LockDelete,
    SegmentPut,
    BasePut,
    HeadPut,
    IndexGenPut,
}

#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    cv: Condvar,
}

impl Gate {
    fn wait(&self) {
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.cv.wait(open).unwrap();
        }
    }

    fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.cv.notify_all();
    }
}

/// Runs once, at the first `locks/` PUT (the commit lock), before it is forwarded.
type AtLock = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

fn at_lock(f: impl FnOnce() + Send + 'static) -> Option<AtLock> {
    Some(Arc::new(Mutex::new(Some(Box::new(f)))))
}

#[derive(Clone, Default)]
struct Shared {
    events: Arc<Mutex<Vec<Event>>>,
    gate: Option<Arc<Gate>>,
    pack_entered: Option<Arc<Mutex<mpsc::Sender<()>>>>,
    fail_packs: bool,
    /// Delete one stored pack just before the commit lock is taken, as a
    /// concurrent compact would after reaping this session's marker.
    delete_pack_at_lock: bool,
    /// Answer `object_sizes` like a vykar-server with `?sizes` does, so the
    /// commit verifies its packs with one batch instead of per-pack requests.
    batch_sizes: bool,
    /// Number of `object_sizes` batches answered.
    batches: Arc<Mutex<usize>>,
    /// Fail every `locks/` PUT, so the commit lock is never acquired.
    fail_lock_put: bool,
    /// Set by the first successful pack PUT and passed to the backup as its
    /// cancel token, so the run is interrupted once packs are durable.
    interrupt_after_pack: Option<CancelToken>,
    /// Fail every `snapshots/` PUT, after the index write it follows.
    fail_snapshot_put: bool,
    /// Another client's work, run at the commit lock PUT.
    at_lock: Option<AtLock>,
    /// Fail every `exists` of a session marker once the commit lock PUT was
    /// seen, so the commit's marker probe is inconclusive.
    fail_marker_probe: bool,
    /// Number of marker `exists` calls failed by `fail_marker_probe`.
    marker_probe_failures: Arc<Mutex<usize>>,
}

struct ObservingBackend {
    inner: Box<dyn StorageBackend>,
    shared: Shared,
}

impl ObservingBackend {
    fn record(&self, e: Event) {
        self.shared.events.lock().unwrap().push(e);
    }

    /// Records the index objects a commit writes.
    fn record_index_put(&self, key: &str, data: &[u8]) {
        use crate::repo::format::ObjectType;
        let tag = data.first().copied();
        let event = if key.starts_with(crate::repo::index_store::HEAD_PREFIX)
            && tag == Some(ObjectType::IndexHead as u8)
        {
            Event::HeadPut
        } else if key == "index.gen" {
            Event::IndexGenPut
        } else if key.starts_with(crate::repo::index_store::INDEX_OBJECT_DIR) {
            match tag {
                Some(t) if t == ObjectType::IndexBase as u8 => Event::BasePut,
                Some(t) if t == ObjectType::IndexSegment as u8 => Event::SegmentPut,
                _ => return,
            }
        } else {
            return;
        };
        self.record(event);
    }

    fn put_with(&self, key: &str, put: impl FnOnce() -> Result<()>) -> Result<()> {
        if key.starts_with("locks/") {
            self.record(Event::LockPut);
            if self.shared.fail_lock_put {
                return Err(VykarError::Other("injected lock PUT failure".into()));
            }
            let hook = self
                .shared
                .at_lock
                .as_ref()
                .and_then(|h| h.lock().unwrap().take());
            if let Some(f) = hook {
                f();
            }
            if self.shared.delete_pack_at_lock {
                let victim = self
                    .inner
                    .list("packs/")?
                    .into_iter()
                    .find(|k| !k.ends_with('/'))
                    .expect("a stored pack to delete");
                self.inner.delete(&victim)?;
            }
            return put();
        }
        if key.starts_with("snapshots/") {
            self.record(Event::SnapshotPut);
            if self.shared.fail_snapshot_put {
                return Err(VykarError::Other("injected snapshot PUT failure".into()));
            }
        }
        if !key.starts_with("packs/") {
            return put();
        }
        self.record(Event::PackPutStart);
        if let Some(tx) = &self.shared.pack_entered {
            let _ = tx.lock().unwrap().send(());
        }
        if let Some(gate) = &self.shared.gate {
            gate.wait();
        }
        let res = if self.shared.fail_packs {
            Err(VykarError::Other("injected pack PUT failure".into()))
        } else {
            put()
        };
        if res.is_ok()
            && let Some(flag) = &self.shared.interrupt_after_pack
        {
            flag.cancel();
        }
        self.record(Event::PackPutEnd);
        res
    }
}

impl InnerBackend for ObservingBackend {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for ObservingBackend;
    except [
        put, delete, exists, put_owned, put_pack, begin_staged_put, conditional, batch_delete,
        server_ops, profile
    ];

    // Test wrapper: keeps every write on `put`.
    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

    // Keeps every delete on the recorded `delete`.
    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        None
    }

    // With `batch_sizes`, a server answering `?sizes` (counted).
    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        if self.shared.batch_sizes {
            Some(self)
        } else {
            self.inner.server_ops()
        }
    }

    // Keep packs on the intercepted `put` path. The recorded order must include every pack write.
    fn begin_staged_put(
        &self,
        _prefix: &str,
    ) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        Ok(None)
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            staged_put: false,
            ..self.inner.profile()
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.record_index_put(key, data);
        self.put_with(key, || self.inner.put(key, data))
    }

    fn delete(&self, key: &str) -> Result<()> {
        if key.starts_with("locks/") {
            self.record(Event::LockDelete);
        } else if key.starts_with("sessions/") && key.ends_with(".json") {
            self.record(Event::MarkerDelete);
        } else if key.starts_with("sessions/") && key.ends_with(".index") {
            self.record(Event::JournalDelete);
        }
        self.inner.delete(key)
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.record_index_put(key, &data);
        self.put_with(key, || self.inner.put_owned(key, data))
    }

    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        self.put_with(key, || self.inner.put_pack(key, data))
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let marker = key.starts_with("sessions/") && key.ends_with(".json");
        if marker
            && self.shared.fail_marker_probe
            && self.shared.events.lock().unwrap().contains(&Event::LockPut)
        {
            *self.shared.marker_probe_failures.lock().unwrap() += 1;
            return Err(VykarError::Other("injected marker probe failure".into()));
        }
        self.inner.exists(key)
    }
}

impl vykar_storage::ServerOps for ObservingBackend {
    fn offers(&self, op: vykar_storage::ServerOp) -> bool {
        op == vykar_storage::ServerOp::ObjectSizes
    }

    fn capabilities(&self) -> Result<vykar_protocol::ServerCapabilities> {
        Ok(crate::testutil::fake_server_caps())
    }

    fn init(&self) -> Result<()> {
        Ok(())
    }

    fn object_sizes(&self, keys: &[String]) -> Result<Vec<Option<u64>>> {
        *self.shared.batches.lock().unwrap() += 1;
        keys.iter().map(|k| self.inner.size(k)).collect()
    }
}

fn setup() -> (
    tempfile::TempDir,
    crate::config::VykarConfig,
    std::path::PathBuf,
) {
    crate::testutil::init_test_environment();
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src_dir = dir.path().join("src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(src_dir.join("a.bin"), vec![0x5au8; 64 * 1024]).unwrap();
    fs::write(src_dir.join("b.txt"), b"hello").unwrap();
    let config = crate::tests::helpers::init_repo(&repo_dir);
    (dir, config, src_dir)
}

/// [`setup`] on a plaintext repository.
// Plaintext-only: 10-single-protocol step 3 removes this.
fn setup_plaintext() -> (
    tempfile::TempDir,
    crate::config::VykarConfig,
    std::path::PathBuf,
) {
    crate::testutil::init_test_environment();
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src_dir = dir.path().join("src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(src_dir.join("a.bin"), vec![0x5au8; 64 * 1024]).unwrap();
    fs::write(src_dir.join("b.txt"), b"hello").unwrap();
    let mut config = crate::tests::helpers::make_test_config(&repo_dir);
    config.encryption.mode = crate::config::EncryptionModeConfig::None;
    config.encryption.passphrase = None;
    crate::commands::init::run(&config, None).unwrap();
    (dir, config, src_dir)
}

fn run_backup(
    config: &crate::config::VykarConfig,
    src: &Path,
    shared: &Shared,
) -> Result<backup::BackupOutcome> {
    run_backup_with_passphrase(config, src, shared, pass(config))
}

fn run_backup_with_passphrase(
    config: &crate::config::VykarConfig,
    src: &Path,
    shared: &Shared,
    passphrase: Option<&str>,
) -> Result<backup::BackupOutcome> {
    let source_paths = vec![src.to_string_lossy().to_string()];
    let cancel = shared.interrupt_after_pack.clone().unwrap_or_default();
    let shared = shared.clone();
    let wrap = move |inner: Box<dyn StorageBackend>| -> Box<dyn StorageBackend> {
        Box::new(ObservingBackend {
            inner,
            shared: shared.clone(),
        })
    };
    backup::run_impl(
        config,
        backup::BackupRequest {
            snapshot_name: "snap",
            passphrase,
            source_paths: &source_paths,
            source_label: "test",
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: crate::compress::Compression::None,
            command_dumps: &[],
            verbose: false,
        },
        None,
        &cancel,
        Some(&wrap),
        crate::repo::HostEnv::system(),
    )
}

fn repo_keys(config: &crate::config::VykarConfig, prefix: &str) -> Vec<String> {
    let storage =
        crate::storage::backend_from_config(&config.repository.storage_config(1)).unwrap();
    let mut keys: Vec<String> = storage
        .list(prefix)
        .unwrap()
        .into_iter()
        .filter(|k| !k.ends_with('/'))
        .collect();
    keys.sort();
    keys
}

#[test]
fn uploads_complete_before_commit_lock() {
    let (_dir, config, src) = setup();
    let (tx, rx) = mpsc::channel();
    let gate = Arc::new(Gate::default());
    let shared = Shared {
        gate: Some(Arc::clone(&gate)),
        pack_entered: Some(Arc::new(Mutex::new(tx))),
        ..Default::default()
    };

    let opener = {
        let gate = Arc::clone(&gate);
        std::thread::spawn(move || {
            // Give the backup thread time to (wrongly) reach the commit lock
            // while a pack upload is parked on the gate.
            let _ = rx.recv_timeout(Duration::from_secs(30));
            std::thread::sleep(Duration::from_millis(300));
            gate.open();
        })
    };

    let result = run_backup(&config, &src, &shared);
    gate.open();
    opener.join().unwrap();
    result.unwrap();

    let events = shared.events.lock().unwrap().clone();
    assert!(
        events.contains(&Event::PackPutStart),
        "expected at least one pack PUT: {events:?}"
    );
    let commit_lock = events
        .iter()
        .position(|e| *e == Event::LockPut)
        .expect("commit lock PUT");
    assert_eq!(
        events.iter().filter(|e| **e == Event::LockPut).count(),
        1,
        "expected exactly one locks/ PUT (the commit lock): {events:?}"
    );
    assert!(
        !events[commit_lock..]
            .iter()
            .any(|e| matches!(e, Event::PackPutStart | Event::PackPutEnd)),
        "pack upload activity after the commit lock was taken: {events:?}"
    );
}

#[test]
fn early_flush_failure_aborts_cleanly() {
    let (_dir, config, src) = setup();
    let shared = Shared {
        fail_packs: true,
        ..Default::default()
    };

    let err = run_backup(&config, &src, &shared).unwrap_err();
    assert!(
        err.to_string().contains("injected"),
        "unexpected error: {err}"
    );

    let events = shared.events.lock().unwrap().clone();
    assert_eq!(
        events.iter().filter(|e| **e == Event::LockPut).count(),
        0,
        "a backup that never commits must not PUT to locks/: {events:?}"
    );
    assert!(repo_keys(&config, "snapshots/").is_empty());
    assert!(repo_keys(&config, "locks/").is_empty());
    let sessions = repo_keys(&config, "sessions/");
    // Every pack upload failed, and only confirmed uploads are journaled.
    assert!(
        !sessions.iter().any(|k| k.ends_with(".index")),
        "failed packs must not be journaled: {sessions:?}"
    );
    assert!(
        !sessions.iter().any(|k| k.ends_with(".json")),
        "session marker should be removed: {sessions:?}"
    );
}

fn assert_missing_pack_fails_commit(config: &crate::config::VykarConfig, src: &Path) {
    assert_missing_pack_fails_commit_with(config, src, false);
}

fn assert_missing_pack_fails_commit_with(
    config: &crate::config::VykarConfig,
    src: &Path,
    batch_sizes: bool,
) {
    let packs_before = repo_keys(config, "packs/").len();
    let storage =
        crate::storage::backend_from_config(&config.repository.storage_config(1)).unwrap();
    let index_before = latest_head(&*storage);
    let objects_before = repo_keys(config, "index.d/");
    let shared = Shared {
        delete_pack_at_lock: true,
        batch_sizes,
        ..Default::default()
    };
    let err = run_backup(config, src, &shared).unwrap_err().to_string();
    assert!(
        err.contains("missing from storage"),
        "expected the missing-pack commit error, got: {err}"
    );
    assert_eq!(packs_before, 0, "the repository starts empty");
    assert!(
        repo_keys(config, "snapshots/").is_empty(),
        "no snapshot may be committed over a missing pack"
    );
    assert!(repo_keys(config, "locks/").is_empty());
    assert_eq!(
        latest_head(&*storage),
        index_before,
        "the head must not move"
    );
    assert_eq!(
        repo_keys(config, "index.d/"),
        objects_before,
        "no index object written"
    );
    let events = shared.events.lock().unwrap().clone();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::SegmentPut | Event::BasePut | Event::HeadPut)),
        "{events:?}"
    );
}

/// Few session packs: verified with one `size()` per pack.
#[test]
fn pack_deleted_before_commit_fails_backup_few_packs() {
    let (_dir, config, src) = setup();
    assert_missing_pack_fails_commit(&config, &src);
}

/// A backend with a batch size API (vykar-server `?sizes`) verifies every
/// session pack with one request and still refuses to commit over a pack
/// deleted after Phase 1. The next backup issues two batches: one to check
/// the packs of the failed run's orphaned journal during recovery, one for
/// its commit.
#[test]
fn pack_deleted_before_commit_fails_backup_batch_sizes() {
    let (_dir, config, src) = setup();
    assert_missing_pack_fails_commit_with(&config, &src, true);

    let shared = Shared {
        batch_sizes: true,
        ..Default::default()
    };
    run_backup(&config, &src, &shared).unwrap();
    assert_eq!(
        *shared.batches.lock().unwrap(),
        2,
        "one batch to check the failed run's journal, one for the commit"
    );
    assert_eq!(repo_keys(&config, "snapshots/").len(), 1);
}

/// Many session packs (one chunk per pack): verified by shard listings.
#[test]
fn pack_deleted_before_commit_fails_backup_many_packs() {
    crate::testutil::init_test_environment();
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    for i in 0..80u32 {
        let body: Vec<u8> = (0..512u32)
            .map(|j| (i.wrapping_mul(31) ^ j) as u8)
            .collect();
        fs::write(
            src.join(format!("f{i}.bin")),
            [&i.to_le_bytes()[..], &body].concat(),
        )
        .unwrap();
    }
    let mut config = crate::tests::helpers::make_test_config(&repo_dir);
    config.repository.min_pack_size = 1;
    config.repository.max_pack_size = 4 * 1024;
    crate::commands::init::run(&config, pass(&config)).unwrap();

    assert_missing_pack_fails_commit(&config, &src);
}

/// The same many-pack backup commits when nothing is deleted, and every
/// session pack it verified is present.
#[test]
fn many_pack_backup_commits() {
    crate::testutil::init_test_environment();
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    for i in 0..80u32 {
        fs::write(
            src.join(format!("f{i}.bin")),
            format!("file {i} {}", "x".repeat(400)),
        )
        .unwrap();
    }
    let mut config = crate::tests::helpers::make_test_config(&repo_dir);
    config.repository.min_pack_size = 1;
    config.repository.max_pack_size = 4 * 1024;
    crate::commands::init::run(&config, pass(&config)).unwrap();

    run_backup(&config, &src, &Shared::default()).unwrap();
    assert!(
        repo_keys(&config, "packs/").len() >= 32,
        "expected one pack per chunk"
    );
    assert_eq!(repo_keys(&config, "snapshots/").len(), 1);
}

/// After the commit point the session is deregistered and its journal
/// deleted (on a helper thread, alongside the local post-commit work), and
/// only then is the commit lock released. Nothing is left behind.
#[test]
fn commit_cleans_up_session_before_releasing_lock() {
    let (_dir, config, src) = setup();
    let shared = Shared::default();
    run_backup(&config, &src, &shared).unwrap();

    let events = shared.events.lock().unwrap().clone();
    let pos = |e: Event| {
        events
            .iter()
            .position(|x| *x == e)
            .unwrap_or_else(|| panic!("{e:?} missing: {events:?}"))
    };
    assert!(
        pos(Event::SnapshotPut) < pos(Event::MarkerDelete),
        "{events:?}"
    );
    assert!(
        pos(Event::MarkerDelete) < pos(Event::JournalDelete),
        "{events:?}"
    );
    assert!(
        pos(Event::JournalDelete) < pos(Event::LockDelete),
        "{events:?}"
    );
    assert!(repo_keys(&config, "sessions/").is_empty());
    assert!(repo_keys(&config, "locks/").is_empty());
}

/// The commit records the new snapshot in the local snapshot list cache, so
/// the next open does not have to download its blob again.
#[test]
fn commit_persists_snapshot_cache_immediately() {
    use crate::tests::helpers::{backup_single_source, load_snapshot_cache_from_disk};

    let (dir, config, src) = setup();
    let repo_dir = dir.path().join("repo");

    backup_single_source(&config, &src, "src-a", "snap-1");
    let after_first = load_snapshot_cache_from_disk(&repo_dir);
    assert!(
        after_first.entries.values().any(|e| e.name == "snap-1"),
        "committed snapshot missing from on-disk cache"
    );

    std::thread::sleep(Duration::from_millis(2));
    backup_single_source(&config, &src, "src-a", "snap-2");
    let after_second = load_snapshot_cache_from_disk(&repo_dir);
    let mut names: Vec<_> = after_second
        .entries
        .values()
        .map(|e| e.name.clone())
        .collect();
    names.sort();
    assert_eq!(names, ["snap-1", "snap-2"]);
}

fn session_keys(config: &crate::config::VykarConfig) -> (bool, bool) {
    let sessions = repo_keys(config, "sessions/");
    (
        sessions.iter().any(|k| k.ends_with(".json")),
        sessions.iter().any(|k| k.ends_with(".index")),
    )
}

/// A commit that fails after taking the lock deregisters the session while
/// still holding the lock, and keeps the journal so the next run can recover
/// the uploaded packs.
#[test]
fn failed_commit_deregisters_before_releasing_lock() {
    let (_dir, config, src) = setup();
    let shared = Shared {
        delete_pack_at_lock: true,
        ..Default::default()
    };
    let err = run_backup(&config, &src, &shared).unwrap_err().to_string();
    assert!(
        err.contains("missing from storage"),
        "unexpected error: {err}"
    );

    let events = shared.events.lock().unwrap().clone();
    let pos = |e: Event| {
        events
            .iter()
            .position(|x| *x == e)
            .unwrap_or_else(|| panic!("{e:?} missing: {events:?}"))
    };
    assert!(
        pos(Event::MarkerDelete) < pos(Event::LockDelete),
        "{events:?}"
    );
    assert!(!events.contains(&Event::JournalDelete), "{events:?}");
    let (marker, journal) = session_keys(&config);
    assert!(!marker, "session marker should be removed");
    assert!(journal, "the journal must survive for recovery");
    assert!(repo_keys(&config, "locks/").is_empty());
    assert!(repo_keys(&config, "snapshots/").is_empty());
}

/// A backup that cannot take the commit lock deregisters its session and
/// keeps the journal.
#[test]
fn lock_acquisition_failure_deregisters_and_keeps_journal() {
    let (_dir, config, src) = setup();
    let shared = Shared {
        fail_lock_put: true,
        ..Default::default()
    };
    let err = run_backup(&config, &src, &shared).unwrap_err().to_string();
    assert!(
        err.contains("injected lock PUT failure"),
        "unexpected error: {err}"
    );

    let events = shared.events.lock().unwrap().clone();
    let count = |e: Event| events.iter().filter(|x| **x == e).count();
    assert_eq!(count(Event::LockPut), 1, "{events:?}");
    assert_eq!(count(Event::SnapshotPut), 0, "{events:?}");
    // A failed PUT may still have committed, so the key is removed once.
    assert_eq!(count(Event::LockDelete), 1, "{events:?}");
    assert_eq!(count(Event::JournalDelete), 0, "{events:?}");
    assert!(events.contains(&Event::MarkerDelete), "{events:?}");
    let (marker, journal) = session_keys(&config);
    assert!(!marker, "session marker should be removed");
    assert!(journal, "the journal must survive for recovery");
    assert!(repo_keys(&config, "locks/").is_empty());
    assert!(repo_keys(&config, "snapshots/").is_empty());
}

/// An interrupt after packs were uploaded deregisters the session without
/// touching the commit lock, and keeps the journal.
#[test]
fn interrupt_after_upload_deregisters_and_keeps_journal() {
    let (_dir, config, src) = setup();
    let shared = Shared {
        interrupt_after_pack: Some(CancelToken::new()),
        ..Default::default()
    };
    let err = run_backup(&config, &src, &shared).unwrap_err();
    assert!(
        matches!(err, VykarError::Interrupted),
        "unexpected error: {err}"
    );

    let events = shared.events.lock().unwrap().clone();
    assert!(!events.contains(&Event::LockPut), "{events:?}");
    assert!(events.contains(&Event::MarkerDelete), "{events:?}");
    let (marker, journal) = session_keys(&config);
    assert!(!marker, "session marker should be removed");
    assert!(journal, "the journal must survive for recovery");
    assert!(repo_keys(&config, "snapshots/").is_empty());
}

/// A repository encrypted with passphrase "right", and a source to back up.
fn setup_encrypted() -> (
    tempfile::TempDir,
    crate::config::VykarConfig,
    std::path::PathBuf,
) {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src_dir = dir.path().join("src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(src_dir.join("a.txt"), b"x").unwrap();
    let mut config = crate::tests::helpers::make_test_config(&repo_dir);
    config.encryption.mode = crate::config::EncryptionModeConfig::Aes256Gcm;
    crate::commands::init::run(&config, Some("right")).unwrap();
    (dir, config, src_dir)
}

fn session_markers(config: &crate::config::VykarConfig) -> Vec<String> {
    repo_keys(config, "sessions/")
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .collect()
}

/// A backup whose open fails removes its session marker through the backend
/// it registered with.
#[test]
fn open_failure_deregisters_backup_session() {
    let (_dir, config, src) = setup_encrypted();
    let shared = Shared::default();
    let err = run_backup_with_passphrase(&config, &src, &shared, Some("wrong")).unwrap_err();
    assert!(err.is_passphrase_failure(), "{err:?}");

    let events = shared.events.lock().unwrap().clone();
    assert!(events.contains(&Event::MarkerDelete), "{events:?}");
    assert!(session_markers(&config).is_empty());
}

/// A reader's open (every read command opens this way since 04 step 7)
/// that fails writes no session marker: readers register none.
#[test]
fn reader_open_failure_writes_no_session_marker() {
    let (_dir, config, _src) = setup_encrypted();
    let res = crate::commands::util::open_repo(
        &config,
        Some("wrong"),
        crate::repo::OpenOptions::new(),
        &crate::repo::control::CancelToken::new(),
    );
    assert!(res.is_err());
    assert!(session_markers(&config).is_empty());
}

/// A v6 commit writes one segment, then the head, then the snapshot, all
/// under the commit lock, and never `index.gen`.
#[test]
fn segmented_commit_puts_segment_then_head_then_snapshot() {
    let (_dir, config, src) = setup();
    let shared = Shared::default();
    run_backup(&config, &src, &shared).unwrap();

    let events = shared.events.lock().unwrap().clone();
    let writes: Vec<(usize, Event)> = events
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, e)| {
            matches!(
                e,
                Event::SegmentPut
                    | Event::BasePut
                    | Event::HeadPut
                    | Event::IndexGenPut
                    | Event::SnapshotPut
            )
        })
        .collect();
    assert_eq!(
        writes.iter().map(|(_, e)| *e).collect::<Vec<_>>(),
        [Event::SegmentPut, Event::HeadPut, Event::SnapshotPut],
        "{events:?}"
    );
    let lock = events.iter().position(|e| *e == Event::LockPut).unwrap();
    assert_eq!(
        events.iter().filter(|e| **e == Event::LockPut).count(),
        1,
        "{events:?}"
    );
    let unlock = events.iter().position(|e| *e == Event::LockDelete).unwrap();
    assert!(
        writes.iter().all(|(i, _)| lock < *i && *i < unlock),
        "{events:?}"
    );
    let storage =
        crate::storage::backend_from_config(&config.repository.storage_config(1)).unwrap();
    assert!(!storage.exists("index.gen").unwrap());
}

type IndexIds = std::collections::HashSet<vykar_types::chunk_id::ChunkId>;

fn index_ids(repo_dir: &Path) -> IndexIds {
    crate::tests::helpers::open_local_repo(repo_dir)
        .chunk_index()
        .iter()
        .map(|(id, _)| id)
        .collect()
}

/// The file called `name` somewhere below `dir`.
fn find_file(dir: &Path, name: &str) -> Option<std::path::PathBuf> {
    for entry in fs::read_dir(dir).ok()? {
        let p = entry.ok()?.path();
        if p.is_dir() {
            if let Some(found) = find_file(&p, name) {
                return Some(found);
            }
        } else if p.file_name().and_then(|s| s.to_str()) == Some(name) {
            return Some(p);
        }
    }
    None
}

fn assert_check_clean(config: &crate::config::VykarConfig) {
    let result = crate::commands::check::run(config, pass(config), false, false).unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
}

/// [`assert_check_clean`] with data verification on.
fn assert_check_clean_verified(config: &crate::config::VykarConfig) {
    let result = crate::commands::check::run(config, pass(config), true, false).unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
}

fn compact(config: &crate::config::VykarConfig) {
    crate::commands::compact::run(config, pass(config), 0.0, None, false, &CancelToken::new())
        .unwrap();
}

fn assert_superset(packs: &[String], config: &crate::config::VykarConfig) {
    let now = repo_keys(config, "packs/");
    let lost: Vec<_> = packs.iter().filter(|p| !now.contains(p)).collect();
    assert!(lost.is_empty(), "packs lost: {lost:?}");
}

/// Restores `snap` and compares it with the source `setup` wrote.
fn assert_restores_snap(config: &crate::config::VykarConfig, dest: &Path) {
    assert_restores(config, "snap", dest);
}

/// Restores `snapshot` and compares it with the files `setup` wrote.
fn assert_restores(config: &crate::config::VykarConfig, snapshot: &str, dest: &Path) {
    crate::commands::restore::run(
        config,
        pass(config),
        snapshot,
        dest.to_str().unwrap(),
        None,
        false,
        true,
        &CancelToken::new(),
    )
    .unwrap();
    let a = find_file(dest, "a.bin").expect("a.bin restored");
    assert_eq!(fs::read(a).unwrap(), vec![0x5au8; 64 * 1024]);
    let b = find_file(dest, "b.txt").expect("b.txt restored");
    assert_eq!(fs::read(b).unwrap(), b"hello");
}

/// A backup whose snapshot PUT fails after the index write: the head moved,
/// no snapshot exists. Returns the packs and index entries it left.
fn fail_after_the_index_write(
    config: &crate::config::VykarConfig,
    src: &Path,
    repo_dir: &Path,
) -> (Vec<String>, IndexIds) {
    let storage =
        crate::storage::backend_from_config(&config.repository.storage_config(1)).unwrap();
    let index_before = latest_head(&*storage);
    let shared = Shared {
        fail_snapshot_put: true,
        ..Default::default()
    };
    let err = run_backup(config, src, &shared).unwrap_err().to_string();
    assert!(err.contains("injected snapshot PUT failure"), "{err}");

    let events = shared.events.lock().unwrap().clone();
    let pos = |e: Event| {
        events
            .iter()
            .position(|x| *x == e)
            .unwrap_or_else(|| panic!("{e:?} missing: {events:?}"))
    };
    assert!(pos(Event::SegmentPut) < pos(Event::HeadPut), "{events:?}");
    assert!(pos(Event::HeadPut) < pos(Event::SnapshotPut), "{events:?}");

    assert_ne!(latest_head(&*storage), index_before);
    let repo = crate::tests::helpers::open_local_repo(repo_dir);
    let head = repo.index_head().unwrap();
    assert_eq!(head.segments.len(), 1, "{head:?}");
    assert!(head.segments[0].entries > 0, "{head:?}");
    drop(repo);
    assert!(repo_keys(config, "snapshots/").is_empty());

    let packs = repo_keys(config, "packs/");
    assert!(!packs.is_empty());
    let ids = index_ids(repo_dir);
    assert!(!ids.is_empty());
    (packs, ids)
}

/// Compact right after a commit whose snapshot PUT failed keeps the packs
/// the committed index names; the next backup dedups against them, and
/// `check --repair` sweeps the index without data loss.
#[test]
fn snapshot_put_failure_after_the_index_write_keeps_its_packs() {
    use crate::commands::check::{IndexRecovery, RepairAction, RepairMode};

    let (dir, config, src) = setup();
    let repo_dir = dir.path().join("repo");
    let (packs, _) = fail_after_the_index_write(&config, &src, &repo_dir);

    // No snapshot names the entries: compact refuses to sweep to empty.
    let err = crate::commands::compact::run(
        &config,
        pass(&config),
        0.0,
        None,
        false,
        &CancelToken::new(),
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.starts_with(crate::commands::compact::MARK_REFUSED),
        "{err}"
    );
    assert_eq!(repo_keys(&config, "packs/"), packs);

    let out = run_backup(&config, &src, &Shared::default()).unwrap();
    assert_eq!(out.stats.deduplicated_size, 0, "{:?}", out.stats);
    assert_superset(&packs, &config);
    assert_eq!(repo_keys(&config, "snapshots/").len(), 1);
    assert_check_clean(&config);

    let repair = |mode| {
        crate::commands::check::run_with_repair(
            &config,
            pass(&config),
            false,
            mode,
            None,
            &CancelToken::new(),
        )
        .unwrap()
    };
    let plan = repair(RepairMode::PlanOnly).plan;
    assert!(!plan.has_data_loss, "{:?}", plan.actions);
    assert_eq!(plan.index_recovery, IndexRecovery::Complete);
    assert!(
        matches!(
            plan.actions.as_slice(),
            [RepairAction::RemoveUnreferencedEntries]
        ),
        "{:?}",
        plan.actions
    );
    repair(RepairMode::Apply);

    compact(&config);
    assert_superset(&packs, &config);
    assert_check_clean(&config);
    assert_restores_snap(&config, &dir.path().join("restore"));
}

/// A retry right after a commit whose snapshot PUT failed (its journal still
/// present) dedups against the committed index; compact then keeps every
/// pack.
#[test]
fn retry_after_a_snapshot_put_failure_dedups_and_keeps_its_packs() {
    let (dir, config, src) = setup();
    let repo_dir = dir.path().join("repo");
    let (packs, _) = fail_after_the_index_write(&config, &src, &repo_dir);
    assert_eq!(session_keys(&config), (false, true));

    let out = run_backup(&config, &src, &Shared::default()).unwrap();
    assert_eq!(out.stats.deduplicated_size, 0, "{:?}", out.stats);
    assert_superset(&packs, &config);
    assert_eq!(repo_keys(&config, "snapshots/").len(), 1);
    assert_check_clean(&config);

    compact(&config);
    assert_superset(&packs, &config);
    assert_check_clean(&config);
    assert_restores_snap(&config, &dir.path().join("restore"));
}

// --- the head moved during the session ---

/// `config` seen from another machine (its own cache dir under `dir`).
fn other_client(dir: &Path, config: &crate::config::VykarConfig) -> crate::config::VykarConfig {
    let mut other = config.clone();
    other.cache_dir = Some(dir.join("other-cache").to_string_lossy().into_owned());
    other
}

/// The repository as a client with an empty cache dir at `cache` reads it.
fn cold_repo(config: &crate::config::VykarConfig, cache: &Path) -> crate::repo::Repo {
    let storage =
        crate::storage::backend_from_config(&config.repository.storage_config(1)).unwrap();
    crate::repo::Repo::open(
        storage,
        Some(crate::testing::TEST_PASSPHRASE),
        Some(cache.to_path_buf()),
        crate::repo::OpenOptions::new().with_index(),
    )
    .unwrap()
}

/// Another client commits the same chunks while this session runs: the
/// commit catches up on the moved head and writes nothing for them (the
/// existing entries stay; reuse is not counted). Here every chunk of the
/// session was committed first, so the commit writes no index object and
/// no head, and both snapshots restore.
#[test]
fn concurrent_commit_of_the_same_chunks_writes_nothing_for_them() {
    use crate::index::head::IndexHead;

    let (dir, config, src) = setup();
    let other = other_client(dir.path(), &config);

    let seen: Arc<Mutex<Option<IndexHead>>> = Arc::default();
    let hook = {
        let (seen, config, src) = (Arc::clone(&seen), config.clone(), src.clone());
        let cold = dir.path().join("cold-at-lock");
        move || {
            crate::tests::helpers::backup_single_source(&other, &src, "test", "other");
            let repo = cold_repo(&config, &cold);
            *seen.lock().unwrap() = Some(repo.index_head().unwrap().clone());
        }
    };
    let shared = Shared {
        at_lock: at_lock(hook),
        ..Default::default()
    };
    run_backup(&config, &src, &shared).unwrap();
    let head_at_lock = seen.lock().unwrap().take().expect("the hook ran");
    assert_eq!(head_at_lock.segments.len(), 1, "{head_at_lock:?}");

    let repo = cold_repo(&config, &dir.path().join("cold-after"));
    assert_eq!(repo.index_head().unwrap(), &head_at_lock);
    drop(repo);
    let events = shared.events.lock().unwrap().clone();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::SegmentPut | Event::HeadPut)),
        "{events:?}"
    );

    assert_check_clean_verified(&config);
    assert_restores(&config, "snap", &dir.path().join("restore-snap"));
    assert_restores(&config, "other", &dir.path().join("restore-other"));
}

/// The session was reaped (its marker aged out, and maintenance deleted the
/// snapshot it deduplicated against and compact swept that snapshot's
/// chunks) while its marker probe was inconclusive, so the commit's re-check
/// of reused chunks is the safety net (the sweep moved `sweep.gen`): it
/// finds reused chunks that are gone and fails the commit before any index
/// write. Also when the session only reuses chunks (a delta of reuse only
/// still runs the check, BK-2/R4; CM-1), and (BK-4) whether the session
/// found them in an applied segment (reuse by key) or, after a fold, in the
/// table (reuse by position).
///
/// The aged session is reaped by maintenance under the lock, which on a
/// put-if-absent store only a plaintext repository takes.
// Plaintext only to reach lock mode here: 10-single-protocol step 3 moves
// this to an encrypted repository on a store without put-if-absent, or
// deletes it with a reason.
#[test]
fn chunks_deleted_during_the_session_fail_the_commit_as_stale() {
    for (new_chunk, fold_first) in [(true, false), (false, false), (true, true), (false, true)] {
        let (dir, config, src) = setup_plaintext();
        // A snapshot that stays, so the sweep is not one to empty.
        let src0 = dir.path().join("src0");
        fs::create_dir_all(&src0).unwrap();
        fs::write(src0.join("z.bin"), vec![0x77u8; 16 * 1024]).unwrap();
        crate::tests::helpers::backup_single_source(&config, &src0, "test0", "s0");
        crate::tests::helpers::backup_single_source(&config, &src, "test", "s1");
        if fold_first {
            let mut repo = crate::commands::util::open_repo(
                &config,
                pass(&config),
                crate::repo::OpenOptions::new().with_index(),
                &CancelToken::new(),
            )
            .unwrap();
            crate::commands::util::with_maintenance_lock(&mut repo, |repo| {
                repo.request_full_index_fold();
                repo.mark_index_dirty();
                repo.save_state()
            })
            .unwrap();
            assert!(repo.index_head().unwrap().segments.is_empty());
        }
        if new_chunk {
            // A new chunk, so the session uploads a pack and journals it.
            fs::write(src.join("c.bin"), vec![0xc3u8; 32 * 1024]).unwrap();
        }
        let other = other_client(dir.path(), &config);

        type AtLockState = (Option<(String, Vec<u8>)>, Vec<String>);
        let seen: Arc<Mutex<Option<AtLockState>>> = Arc::default();
        let repo_dir = dir.path().join("repo");
        let hook = {
            let (seen, config) = (Arc::clone(&seen), config.clone());
            move || {
                let storage =
                    crate::storage::backend_from_config(&config.repository.storage_config(1))
                        .unwrap();
                let markers: Vec<String> = storage
                    .list("sessions/")
                    .unwrap()
                    .into_iter()
                    .filter(|k| k.ends_with(".json"))
                    .collect();
                assert_eq!(markers.len(), 1, "{markers:?}");
                let key = &markers[0];
                let mut entry: serde_json::Value =
                    serde_json::from_slice(&storage.get(key).unwrap().unwrap()).unwrap();
                let old = (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
                entry["registered_at"] = old.clone().into();
                entry["last_refresh"] = old.into();
                storage
                    .put(key, &serde_json::to_vec(&entry).unwrap())
                    .unwrap();
                // Its age is its mtime on the storage's clock (04 step 5).
                fs::File::options()
                    .write(true)
                    .open(repo_dir.join(key))
                    .unwrap()
                    .set_modified(std::time::SystemTime::now() - Duration::from_secs(2 * 3600))
                    .unwrap();

                crate::commands::delete::run(
                    &other,
                    pass(&other),
                    &["s1"],
                    false,
                    &CancelToken::new(),
                )
                .unwrap();
                assert!(
                    !storage.exists(key).unwrap(),
                    "maintenance reaped the marker"
                );
                // s1's segment is old, so the sweep takes its chunks (04
                // step 7 keeps those of segments younger than T_pack).
                crate::tests::helpers::age_repo_objects(
                    &repo_dir,
                    &["index.d"],
                    crate::tests::helpers::PAST_T_PACK,
                );
                let swept = crate::commands::compact::run(
                    &other,
                    pass(&other),
                    10.0,
                    None,
                    false,
                    &CancelToken::new(),
                )
                .unwrap();
                assert!(swept.chunks_unreferenced > 0, "{swept:?}");

                let head = latest_head(&*storage);
                *seen.lock().unwrap() = Some((head, repo_keys(&config, "index.d/")));
            }
        };
        let shared = Shared {
            at_lock: at_lock(hook),
            fail_marker_probe: true,
            ..Default::default()
        };
        let err = run_backup(&config, &src, &shared).unwrap_err();
        assert!(
            matches!(err, VykarError::StaleChunksDuringCommit),
            "{new_chunk}: expected StaleChunksDuringCommit, got: {err}"
        );
        assert!(
            *shared.marker_probe_failures.lock().unwrap() > 0,
            "the marker probe never ran"
        );
        let (head_at_lock, objects_at_lock) = seen.lock().unwrap().take().expect("the hook ran");

        let storage =
            crate::storage::backend_from_config(&config.repository.storage_config(1)).unwrap();
        assert_eq!(latest_head(&*storage), head_at_lock, "the head moved");
        assert_eq!(repo_keys(&config, "index.d/"), objects_at_lock);
        let events = shared.events.lock().unwrap().clone();
        let lock = events.iter().position(|e| *e == Event::LockPut).unwrap();
        assert!(
            !events[lock..]
                .iter()
                .any(|e| matches!(e, Event::SegmentPut | Event::BasePut | Event::HeadPut)),
            "{events:?}"
        );
        assert!(repo_keys(&config, "snapshots/").len() == 1, "only s0");
        assert!(repo_keys(&config, "locks/").is_empty());
        let (marker, journal) = session_keys(&config);
        assert!(!marker, "the reaped marker stays gone");
        if new_chunk {
            // The cleanup that reaped the marker gives its journal one run
            // of grace, and the failed commit keeps it for recovery.
            assert!(journal, "the journal must survive for recovery");
        }

        run_backup(&config, &src, &Shared::default()).unwrap();
        assert_check_clean(&config);
        let dest = dir.path().join("restore");
        assert_restores_snap(&config, &dest);
        if new_chunk {
            let c = find_file(&dest, "c.bin").expect("c.bin restored");
            assert_eq!(fs::read(c).unwrap(), vec![0xc3u8; 32 * 1024]);
        }
    }
}

/// The latest head's key and bytes.
fn latest_head(storage: &dyn StorageBackend) -> Option<(String, Vec<u8>)> {
    let generation = *crate::repo::index_store::list_head_generations(storage)
        .unwrap()
        .last()?;
    let key = crate::repo::index_store::head_key(generation);
    let bytes = storage.get(&key).unwrap()?;
    Some((key, bytes))
}
