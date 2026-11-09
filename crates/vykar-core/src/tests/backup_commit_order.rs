//! Backup Phase 1 must leave every pack durable before Phase 2 takes the
//! commit lock, so the lock is never held across pack uploads.
//!
//! The first `locks/` PUT of every backup is the `register_session` probe; the
//! commit lock is any `locks/` PUT after that.

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
    except [put, put_owned];

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
        .rposition(|e| *e == Event::LockPut)
        .expect("commit lock PUT");
    assert!(
        events.iter().filter(|e| **e == Event::LockPut).count() >= 2,
        "expected registration probe + commit lock: {events:?}"
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
        1,
        "only the registration probe may touch locks/: {events:?}"
    );
    assert!(repo_keys(&config, "snapshots/").is_empty());
    assert!(repo_keys(&config, "locks/").is_empty());
    let sessions = repo_keys(&config, "sessions/");
    assert!(
        sessions.iter().any(|k| k.ends_with(".index")),
        "journal should be kept for recovery: {sessions:?}"
    );
    assert!(
        !sessions.iter().any(|k| k.ends_with(".json")),
        "session marker should be removed: {sessions:?}"
    );
}
