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

use vykar_storage::{delegate_storage_backend, InnerBackend, StorageBackend};
use vykar_types::error::{Result, VykarError};

use crate::commands::backup;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    PackPutStart,
    PackPutEnd,
    LockPut,
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

#[derive(Clone, Default)]
struct Shared {
    events: Arc<Mutex<Vec<Event>>>,
    gate: Option<Arc<Gate>>,
    pack_entered: Option<Arc<Mutex<mpsc::Sender<()>>>>,
    fail_packs: bool,
    /// Delete one stored pack just before the commit lock is taken, as a
    /// concurrent compact would after reaping this session's marker.
    delete_pack_at_lock: bool,
}

struct ObservingBackend {
    inner: Box<dyn StorageBackend>,
    shared: Shared,
}

impl ObservingBackend {
    fn record(&self, e: Event) {
        self.shared.events.lock().unwrap().push(e);
    }

    fn put_with(&self, key: &str, put: impl FnOnce() -> Result<()>) -> Result<()> {
        if key.starts_with("locks/") {
            self.record(Event::LockPut);
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
    except [put, put_owned, begin_staged_put];

    // Keep packs on the intercepted `put` path. The recorded order must include every pack write.
    fn begin_staged_put(
        &self,
        _prefix: &str,
    ) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        Ok(None)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.put_with(key, || self.inner.put(key, data))
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.put_with(key, || self.inner.put_owned(key, data))
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
    let source_paths = vec![src.to_string_lossy().to_string()];
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
            passphrase: None,
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
        None,
        Some(&wrap),
    )
}

fn repo_keys(config: &crate::config::VykarConfig, prefix: &str) -> Vec<String> {
    let storage = crate::storage::backend_from_config(&config.repository, 1).unwrap();
    storage
        .list(prefix)
        .unwrap()
        .into_iter()
        .filter(|k| !k.ends_with('/'))
        .collect()
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
    let packs_before = repo_keys(config, "packs/").len();
    let shared = Shared {
        delete_pack_at_lock: true,
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
}

/// Few session packs: verified with one `exists()` per pack.
#[test]
fn pack_deleted_before_commit_fails_backup_few_packs() {
    let (_dir, config, src) = setup();
    assert_missing_pack_fails_commit(&config, &src);
}

/// Many session packs (one chunk per pack): verified by listing.
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
