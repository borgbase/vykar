//! UPG-2: `vykar upgrade` of the frozen v2/v3 fixtures, interrupted at
//! every write it makes (the write failing, and, for the head and the
//! `config` PUT, the write landing but the call failing), then run again:
//! the second run completes, and the result is what an uninterrupted upgrade
//! gives. Until the `config` switch a released-format reader still reads the
//! legacy index unchanged.

#![allow(clippy::unwrap_used)]
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use vykar_core::commands;
use vykar_core::index::ChunkIndexEntry;
use vykar_core::repo::{OpenOptions, RepoConfig, Repository};
use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use crate::common::fixtures::{self, Fixture};

/// Fails the `fail_at`-th mutation (1-based; 0: none) that is not a lock
/// file's, after applying it when `applied`. Logs every counted mutation.
struct Interrupt {
    inner: Arc<dyn StorageBackend>,
    fail_at: usize,
    applied: bool,
    count: AtomicUsize,
    log: Mutex<Vec<String>>,
}

impl Interrupt {
    fn mutate(&self, op: String, f: impl FnOnce() -> Result<()>) -> Result<()> {
        let n = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        self.log.lock().unwrap().push(op.clone());
        if n != self.fail_at {
            return f();
        }
        if self.applied {
            f()?;
        }
        Err(VykarError::Other(format!(
            "injected failure at write {n}: {op}"
        )))
    }
}

impl InnerBackend for Interrupt {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for Interrupt;
    except [put, delete, put_pack, begin_staged_put, conditional, batch_delete, profile];

    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        None
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        self.inner.profile()
    }

    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        self.inner.put_pack(key, data)
    }

    fn begin_staged_put(&self, prefix: &str) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        self.inner.begin_staged_put(prefix)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if key.starts_with("locks/") {
            return self.inner.put(key, data);
        }
        self.mutate(format!("put {key}"), || self.inner.put(key, data))
    }

    fn delete(&self, key: &str) -> Result<()> {
        if key.starts_with("locks/") {
            return self.inner.delete(key);
        }
        self.mutate(format!("delete {key}"), || self.inner.delete(key))
    }
}

/// Run the upgrade of `fx` through an `Interrupt`; its result and log.
fn upgrade_through(fx: &Fixture, fail_at: usize, applied: bool) -> (Result<()>, Vec<String>) {
    let storage = Arc::new(Interrupt {
        inner: Arc::new(LocalBackend::new(fx.repo_dir().to_str().unwrap()).unwrap()),
        fail_at,
        applied,
        count: AtomicUsize::new(0),
        log: Mutex::new(Vec::new()),
    });
    let mut repo = Repository::open(
        Box::new(Arc::clone(&storage) as Arc<dyn StorageBackend>),
        fx.passphrase(),
        Some(fx.cache_dir()),
        OpenOptions::new().legacy_format(),
    )
    .unwrap();
    let result =
        commands::util::with_maintenance_lock(&mut repo, |r| r.upgrade_index_layout()).map(|_| ());
    drop(repo);
    let log = storage.log.lock().unwrap().clone();
    (result, log)
}

/// The entries, sorted, as strings.
fn entries(index: impl Iterator<Item = (ChunkId, ChunkIndexEntry)>) -> Vec<String> {
    let mut v: Vec<String> = index
        .map(|(id, e)| format!("{id:?} {} {} {}", e.pack_id, e.pack_offset, e.stored_size))
        .collect();
    v.sort();
    v
}

fn legacy_entries(fx: &Fixture) -> Vec<String> {
    // What v6 keeps: the live legacy entries (refcount above 0).
    let index = fx.open_legacy().legacy_index().unwrap();
    entries(index.live())
}

/// UPG-1 on an upgraded fixture.
fn assert_upgraded(fx: &Fixture, legacy: &[String], hash: &str, label: &str) {
    let bytes = std::fs::read(fx.repo_dir().join("config")).unwrap();
    let config = RepoConfig::decode(&bytes).unwrap();
    assert_eq!(config.validate().unwrap().version(), 6, "{label}");
    assert_eq!(config.into_repo_config().chunk_hash, hash, "{label}");
    assert!(!fx.repo_dir().join("index").exists(), "{label}");
    assert!(!fx.repo_dir().join("index.gen").exists(), "{label}");
    let repo = fx.open_cold(&format!("cold-{}", label.replace(['/', ' '], "-")));
    let head = repo.index_head().unwrap();
    assert_eq!(head.layout, 3, "{label}");
    assert!(head.base.is_some() && head.segments.is_empty(), "{label}");
    assert_eq!(entries(repo.chunk_index().iter()), legacy, "{label}");
    drop(repo);
    let maps = commands::pack_maps::run(
        &fx.config(),
        fx.passphrase(),
        &vykar_core::repo::control::CancelToken::new(),
    )
    .unwrap();
    assert_eq!(maps.packs_mapped, 0, "{label}: every pack is mapped");
    let check = commands::check::run(&fx.config(), fx.passphrase(), true, false).unwrap();
    assert!(check.errors.is_empty(), "{label}: {:?}", check.errors);
}

/// A write without the object name (encrypted objects get new names on
/// every run).
fn kind(write: &str) -> String {
    for dir in ["packmaps/", "index.d/"] {
        if let Some(i) = write.find(dir) {
            return write[..i + dir.len()].to_string();
        }
    }
    write.to_string()
}

fn interrupted_at_every_write(prefix: &str, mode: &str, version: u64, hash: &str) {
    // The probe run counts the upgrade's writes.
    let fx = fixtures::extract(prefix, mode, version);
    let (result, writes) = upgrade_through(&fx, 0, false);
    result.unwrap();
    let switch = writes
        .iter()
        .rposition(|w| w == "put config")
        .expect("the version switch");
    assert!(writes.len() > 4, "{writes:?}");

    for k in 1..=writes.len() {
        let op = &writes[k - 1];
        let mut modes = vec![false];
        if op == "put config" && k - 1 == switch || op.starts_with("put index.h/") {
            modes.push(true);
        }
        for applied in modes {
            let label = format!("{prefix}-{mode} write {k} ({op}, applied {applied})");
            let fx = fixtures::extract(prefix, mode, version);
            let legacy = legacy_entries(&fx);
            let (result, log) = upgrade_through(&fx, k, applied);
            assert_eq!(
                log.get(k - 1).map(|w| kind(w)),
                Some(kind(op)),
                "{label}: the writes differ: {log:?}"
            );
            match result {
                Err(e) => assert!(e.to_string().contains("injected failure"), "{label}: {e}"),
                // Only the best-effort deletes after the switch may fail
                // without failing the upgrade (UPG-3).
                Ok(()) => assert!(op.starts_with("delete "), "{label}: the upgrade succeeded"),
            }
            if k - 1 < switch || (k - 1 == switch && !applied) {
                assert_eq!(
                    legacy_entries(&fx),
                    legacy,
                    "{label}: the legacy index changed"
                );
            }
            commands::upgrade::run(&fx.config(), fx.passphrase())
                .unwrap_or_else(|e| panic!("{label}: the second run failed: {e}"));
            assert_upgraded(&fx, &legacy, hash, &label);
        }
    }
}

#[test]
fn v3_upgrade_interrupted_at_every_write_completes() {
    interrupted_at_every_write("v3", "aes256gcm", 3, "blake3");
}

#[test]
fn v2_upgrade_interrupted_at_every_write_completes() {
    interrupted_at_every_write("v2", "none", 2, "blake2b");
}
