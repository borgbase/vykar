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
    /// shutdown flag, so the run is interrupted once packs are durable.
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
        let event = if key == "index" && tag == Some(ObjectType::IndexHead as u8) {
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
        put, delete, exists, put_owned, put_pack, begin_staged_put, object_sizes, profile,
    ];

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

    fn object_sizes(&self, keys: &[String]) -> Result<Vec<Option<u64>>> {
        if !self.shared.batch_sizes {
            return self.inner.object_sizes(keys);
        }
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

fn run_backup(
    config: &crate::config::VykarConfig,
    src: &Path,
    shared: &Shared,
) -> Result<backup::BackupOutcome> {
    run_backup_with_passphrase(config, src, shared, None)
}

fn run_backup_with_passphrase(
    config: &crate::config::VykarConfig,
    src: &Path,
    shared: &Shared,
    passphrase: Option<&str>,
) -> Result<backup::BackupOutcome> {
    let source_paths = vec![src.to_string_lossy().to_string()];
    let shutdown = shared.interrupt_after_pack.clone().unwrap_or_default();
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
        &shutdown,
        Some(&wrap),
    )
}

fn repo_keys(config: &crate::config::VykarConfig, prefix: &str) -> Vec<String> {
    let storage = crate::storage::backend_from_config(&config.repository, 1).unwrap();
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
    let storage = crate::storage::backend_from_config(&config.repository, 1).unwrap();
    let index_before = storage.get("index").unwrap();
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
        storage.get("index").unwrap(),
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
    crate::commands::init::run(&config, None).unwrap();

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
    crate::commands::init::run(&config, None).unwrap();

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
    assert_eq!(count(Event::LockDelete), 0, "{events:?}");
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

/// A read session whose open fails removes its session marker.
#[test]
fn open_failure_deregisters_read_session() {
    let (_dir, config, _src) = setup_encrypted();
    let res = crate::commands::util::open_repo_with_read_session(
        &config,
        Some("wrong"),
        crate::repo::OpenOptions::new(),
    );
    assert!(res.is_err());
    assert!(session_markers(&config).is_empty());
}

/// A v5 commit writes one segment, then the head, then the snapshot, all
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
    let storage = crate::storage::backend_from_config(&config.repository, 1).unwrap();
    assert!(!storage.exists("index.gen").unwrap());
}

type Refcounts = std::collections::HashMap<vykar_types::chunk_id::ChunkId, u32>;

fn refcounts(repo_dir: &Path) -> Refcounts {
    crate::tests::helpers::open_local_repo(repo_dir)
        .chunk_index()
        .iter()
        .map(|(id, e)| (*id, e.refcount))
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
    let result = crate::commands::check::run(config, None, false, false).unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
}

/// [`assert_check_clean`] with data verification on.
fn assert_check_clean_verified(config: &crate::config::VykarConfig) {
    let result = crate::commands::check::run(config, None, true, false).unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
}

fn compact(config: &crate::config::VykarConfig) {
    crate::commands::compact::run(config, None, 0.0, None, false, &CancelToken::new()).unwrap();
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
        None,
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
/// no snapshot exists. Returns the packs and refcounts it left.
fn fail_after_the_index_write(
    config: &crate::config::VykarConfig,
    src: &Path,
    repo_dir: &Path,
) -> (Vec<String>, Refcounts) {
    let storage = crate::storage::backend_from_config(&config.repository, 1).unwrap();
    let index_before = storage.get("index").unwrap();
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

    assert_ne!(storage.get("index").unwrap(), index_before);
    let repo = crate::tests::helpers::open_local_repo(repo_dir);
    let head = repo.index_head().unwrap();
    assert_eq!(head.segments.len(), 1, "{head:?}");
    assert!(head.entries > 0, "{head:?}");
    drop(repo);
    assert!(repo_keys(config, "snapshots/").is_empty());

    let packs = repo_keys(config, "packs/");
    assert!(!packs.is_empty());
    let r1 = refcounts(repo_dir);
    assert!(!r1.is_empty());
    assert!(r1.values().all(|&r| r > 0), "{r1:?}");
    (packs, r1)
}

/// Compact right after a commit whose snapshot PUT failed keeps the packs
/// the committed index names; the next backup dedups against them, and
/// `check --repair` corrects the inflated refcounts without data loss.
#[test]
fn snapshot_put_failure_after_the_index_write_keeps_its_packs() {
    use crate::commands::check::{IndexRecovery, RepairAction, RepairMode};

    let (dir, config, src) = setup();
    let repo_dir = dir.path().join("repo");
    let (packs, r1) = fail_after_the_index_write(&config, &src, &repo_dir);

    compact(&config);
    assert_eq!(repo_keys(&config, "packs/"), packs);

    let out = run_backup(&config, &src, &Shared::default()).unwrap();
    assert_eq!(out.stats.deduplicated_size, 0, "{:?}", out.stats);
    assert_superset(&packs, &config);
    assert_eq!(repo_keys(&config, "snapshots/").len(), 1);
    assert_check_clean(&config);

    let r2 = refcounts(&repo_dir);
    let sum = |r: &Refcounts| -> u64 { r1.keys().map(|k| u64::from(r[k])).sum() };
    assert!(sum(&r2) > sum(&r1), "refcounts not inflated: {r1:?} {r2:?}");

    let repair = |mode| {
        crate::commands::check::run_with_repair(
            &config,
            None,
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
        matches!(plan.actions.as_slice(), [RepairAction::RebuildRefcounts]),
        "{:?}",
        plan.actions
    );
    repair(RepairMode::Apply);

    let r3 = refcounts(&repo_dir);
    let shared: Vec<_> = r1.keys().filter(|k| r3.contains_key(k)).collect();
    assert!(!shared.is_empty());
    for k in shared {
        assert_eq!(r3[k], r1[k], "{k:?}");
    }

    compact(&config);
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
fn cold_repo(config: &crate::config::VykarConfig, cache: &Path) -> crate::repo::Repository {
    let storage = crate::storage::backend_from_config(&config.repository, 1).unwrap();
    crate::repo::Repository::open(
        storage,
        None,
        Some(cache.to_path_buf()),
        crate::repo::OpenOptions::new().with_index(),
    )
    .unwrap()
}

/// The index as a client with an empty cache dir at `cache` reads it.
fn cold_index(config: &crate::config::VykarConfig, cache: &Path) -> crate::index::ChunkIndex {
    cold_repo(config, cache).chunk_index().clone()
}

fn sorted_entries(
    index: &crate::index::ChunkIndex,
) -> Vec<(
    vykar_types::chunk_id::ChunkId,
    crate::index::ChunkIndexEntry,
)> {
    let mut v: Vec<_> = index.iter().map(|(id, e)| (*id, *e)).collect();
    v.sort_by_key(|(id, _)| *id.as_bytes());
    v
}

/// Another client commits the same chunks while this session runs: the
/// commit catches up on the moved head and turns its new entries into bumps.
#[test]
fn concurrent_commit_of_the_same_chunks_becomes_bumps() {
    use crate::commands::check::RepairMode;
    use crate::index::head::IndexHead;

    let (dir, config, src) = setup();
    let repo_dir = dir.path().join("repo");
    let other = other_client(dir.path(), &config);

    type AtLockState = (Refcounts, Vec<vykar_types::chunk_id::ChunkId>, IndexHead);
    let seen: Arc<Mutex<Option<AtLockState>>> = Arc::default();
    let hook = {
        let (seen, config, src) = (Arc::clone(&seen), config.clone(), src.clone());
        let cold = dir.path().join("cold-at-lock");
        move || {
            crate::tests::helpers::backup_single_source(&other, &src, "test", "other");
            let repo = cold_repo(&config, &cold);
            let refcounts: Refcounts = repo
                .chunk_index()
                .iter()
                .map(|(id, e)| (*id, e.refcount))
                .collect();
            let ids = sorted_entries(repo.chunk_index())
                .into_iter()
                .map(|(id, _)| id)
                .collect();
            let head = repo.index_head().unwrap().clone();
            *seen.lock().unwrap() = Some((refcounts, ids, head));
        }
    };
    let shared = Shared {
        at_lock: at_lock(hook),
        ..Default::default()
    };
    run_backup(&config, &src, &shared).unwrap();
    let (at_lock_refcounts, other_ids, head_at_lock) =
        seen.lock().unwrap().take().expect("the hook ran");
    assert_eq!(head_at_lock.segments.len(), 1, "{head_at_lock:?}");

    // One more segment over the moved head, not a base.
    let repo = cold_repo(&config, &dir.path().join("cold-after"));
    let head = repo.index_head().unwrap().clone();
    assert_eq!(head.base, head_at_lock.base, "{head:?}");
    assert_eq!(head.segments.len(), 2, "{head:?}");
    assert_eq!(head.segments[0], head_at_lock.segments[0], "{head:?}");
    let seg_ref = &head.segments[1];
    let bytes = repo
        .storage
        .get(&crate::repo::index_store::object_key(&seg_ref.name))
        .unwrap()
        .unwrap();
    let seg =
        crate::repo::index_store::decode_segment(seg_ref, &bytes, repo.crypto.as_ref()).unwrap();
    assert_eq!(seg.prev_entries, other_ids.len() as u64);
    assert!(
        seg.new_entries.iter().all(|e| other_ids
            .binary_search_by_key(e.chunk_id.as_bytes(), |id| *id.as_bytes())
            .is_err()),
        "a chunk the other client committed was written as a new entry: {seg:?}"
    );
    drop(repo);

    // Repair recounts references; its result is the oracle for the bumps.
    let refs = refcounts(&repo_dir);
    let plan = crate::commands::check::run_with_repair(
        &config,
        None,
        false,
        RepairMode::Apply,
        None,
        &CancelToken::new(),
    )
    .unwrap()
    .plan;
    assert!(!plan.has_data_loss, "{:?}", plan.actions);
    let repaired = refcounts(&repo_dir);
    assert_eq!(refs, repaired, "the merge lost or doubled a refcount");
    for (pos, id) in other_ids.iter().enumerate() {
        let delta = seg
            .deltas
            .iter()
            .find(|d| d.position as usize == pos)
            .map_or(0, |d| d.delta);
        let ours = i64::from(repaired[id]) - i64::from(at_lock_refcounts[id]);
        assert_eq!(delta, ours, "chunk {id}");
    }
    assert!(seg.deltas.iter().all(|d| d.delta > 0), "{seg:?}");
    assert!(!seg.deltas.is_empty(), "{seg:?}");

    assert_check_clean_verified(&config);
    let warm = crate::tests::helpers::open_local_repo(&repo_dir);
    assert_eq!(
        sorted_entries(&cold_index(&config, &dir.path().join("cold-end"))),
        sorted_entries(warm.chunk_index())
    );
    drop(warm);
    assert_restores(&config, "snap", &dir.path().join("restore-snap"));
    assert_restores(&config, "other", &dir.path().join("restore-other"));
}

/// The session was reaped (its marker aged out and maintenance deleted the
/// snapshot it deduplicated against) while its marker probe was
/// inconclusive, so the merge is the safety net: it finds bumps on chunks
/// that are gone and fails the commit before any index write.
#[test]
fn chunks_deleted_during_the_session_fail_the_commit_as_stale() {
    let (dir, config, src) = setup();
    crate::tests::helpers::backup_single_source(&config, &src, "test", "s1");
    // A new chunk, so the session uploads a pack and journals it.
    fs::write(src.join("c.bin"), vec![0xc3u8; 32 * 1024]).unwrap();
    let other = other_client(dir.path(), &config);

    type AtLockState = (Option<Vec<u8>>, Vec<String>);
    let seen: Arc<Mutex<Option<AtLockState>>> = Arc::default();
    let hook = {
        let (seen, config) = (Arc::clone(&seen), config.clone());
        move || {
            let storage = crate::storage::backend_from_config(&config.repository, 1).unwrap();
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

            crate::commands::delete::run(&other, None, &["s1"], false, &CancelToken::new())
                .unwrap();
            assert!(
                !storage.exists(key).unwrap(),
                "maintenance reaped the marker"
            );

            let head = storage.get("index").unwrap();
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
        "expected StaleChunksDuringCommit, got: {err}"
    );
    assert!(
        *shared.marker_probe_failures.lock().unwrap() > 0,
        "the marker probe never ran"
    );
    let (head_at_lock, objects_at_lock) = seen.lock().unwrap().take().expect("the hook ran");

    let storage = crate::storage::backend_from_config(&config.repository, 1).unwrap();
    assert_eq!(
        storage.get("index").unwrap(),
        head_at_lock,
        "the head moved"
    );
    assert_eq!(repo_keys(&config, "index.d/"), objects_at_lock);
    let events = shared.events.lock().unwrap().clone();
    let lock = events.iter().position(|e| *e == Event::LockPut).unwrap();
    assert!(
        !events[lock..]
            .iter()
            .any(|e| matches!(e, Event::SegmentPut | Event::BasePut | Event::HeadPut)),
        "{events:?}"
    );
    assert!(repo_keys(&config, "snapshots/").is_empty());
    assert!(repo_keys(&config, "locks/").is_empty());
    let (marker, journal) = session_keys(&config);
    assert!(!marker, "the reaped marker stays gone");
    // The cleanup that reaped the marker gives its journal one run of
    // grace, and the failed commit keeps it for recovery.
    assert!(journal, "the journal must survive for recovery");

    run_backup(&config, &src, &Shared::default()).unwrap();
    assert_check_clean(&config);
    let dest = dir.path().join("restore");
    assert_restores_snap(&config, &dest);
    let c = find_file(&dest, "c.bin").expect("c.bin restored");
    assert_eq!(fs::read(c).unwrap(), vec![0xc3u8; 32 * 1024]);
}
