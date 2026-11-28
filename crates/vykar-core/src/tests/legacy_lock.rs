//! The v2/v3 upgrade's legacy lock key (`repo::LegacyUpgradeLock`,
//! 10-single-protocol step 6): a lock vykar 0.24 reads as its own,
//! rewritten every few minutes and listed after, failing once gone, beside
//! another lock or unrewritten for 5.5 h.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::error::{Result, VykarError};

use super::vendored_v0_24_lock as v024;
use crate::repo::{HostEnv, HostIdentity, LegacyUpgradeLock};
use crate::sim::env::{SimTime, TimeMode};
use crate::testutil::MemoryBackend;

/// What runs on the inner backend before the next PUT to a key.
type BeforePut = Box<dyn FnOnce(&dyn StorageBackend) + Send>;

/// A memory backend with mtimes that counts the requests reaching it, can
/// fail PUTs to one key or every LIST, and can run a hook before the next
/// PUT to one key.
struct Counted {
    inner: MemoryBackend,
    requests: AtomicUsize,
    puts: Mutex<Vec<String>>,
    fail_puts_to: Mutex<Option<String>>,
    /// Fail every `list` (the acquisition uses `list_meta`).
    fail_lists: AtomicBool,
    before_put_to: Mutex<Option<(String, BeforePut)>>,
}

impl Counted {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryBackend::with_mtimes(),
            requests: AtomicUsize::new(0),
            puts: Mutex::new(Vec::new()),
            fail_puts_to: Mutex::new(None),
            fail_lists: AtomicBool::new(false),
            before_put_to: Mutex::new(None),
        })
    }

    fn note(&self) {
        self.requests.fetch_add(1, Ordering::SeqCst);
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    fn puts_to(&self, key: &str) -> usize {
        self.puts
            .lock()
            .unwrap()
            .iter()
            .filter(|k| *k == key)
            .count()
    }
}

impl InnerBackend for Counted {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for Counted;
    except [get, put, delete, exists, list, put_pack, begin_staged_put, list_sized, list_meta,
            conditional, batch_delete, server_ops, profile];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.note();
        self.inner.get(key)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.note();
        self.puts.lock().unwrap().push(key.to_string());
        let hook = {
            let mut armed = self.before_put_to.lock().unwrap();
            match armed.take() {
                Some((k, f)) if k == key => Some(f),
                other => {
                    *armed = other;
                    None
                }
            }
        };
        if let Some(f) = hook {
            f(&self.inner);
        }
        if self.fail_puts_to.lock().unwrap().as_deref() == Some(key) {
            return Err(VykarError::Other(format!("injected failure: put {key}")));
        }
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.note();
        self.inner.delete(key)
    }
    fn exists(&self, key: &str) -> Result<bool> {
        self.note();
        self.inner.exists(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.note();
        if self.fail_lists.load(Ordering::SeqCst) {
            return Err(VykarError::Other(format!("injected failure: list {prefix}")));
        }
        self.inner.list(prefix)
    }
    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        self.put(key, data.as_slice())
    }
    fn begin_staged_put(&self, _prefix: &str) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        Ok(None)
    }
    fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.note();
        self.inner.list_sized(prefix)
    }
    fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.note();
        self.inner.list_meta(prefix)
    }
    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }
    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        None
    }
    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        None
    }
    fn profile(&self) -> vykar_storage::BackendProfile {
        self.inner.profile()
    }
}

/// The upgrader's identity: not this machine's hostname, so the vendored
/// 0.24 code (which reads the real hostname) judges its key by age only.
fn upgrader() -> HostIdentity {
    HostIdentity {
        hostname: "upgrader".into(),
        pid: 4242,
        boot_id: Some("boot-u".into()),
        installation_id: Some("inst-u".into()),
        pid_ns: Some(7),
    }
}

/// A sim clock starting now, and `host` on it with every PID alive or dead.
fn env_with(host: HostIdentity, alive: bool) -> (Arc<SimTime>, HostEnv) {
    let time = SimTime::new(Utc::now(), TimeMode::Auto);
    let env = HostEnv::system()
        .with_clock(Arc::new(time.clock(Duration::zero())))
        .with_host(host)
        .with_pid_alive(Arc::new(move |_, _| alive));
    (time, env)
}

fn env() -> (Arc<SimTime>, HostEnv) {
    env_with(upgrader(), true)
}

fn advance(time: &SimTime, by: Duration) {
    time.advance(by.to_std().unwrap());
}

fn entry_of(storage: &dyn StorageBackend, key: &str) -> v024::LockEntry {
    serde_json::from_slice(&storage.get(key).unwrap().unwrap()).unwrap()
}

fn time_of(entry: &v024::LockEntry) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&entry.time)
        .unwrap()
        .with_timezone(&Utc)
}

fn lock_keys(storage: &dyn StorageBackend) -> Vec<String> {
    let mut keys = v024::list_lock_keys(storage).unwrap();
    keys.sort();
    keys
}

fn expired_containing(result: Result<()>, text: &str) {
    match result {
        Err(VykarError::LockExpired(msg)) => assert!(msg.contains(text), "{msg}"),
        other => panic!("expected LockExpired containing {text:?}, got {other:?}"),
    }
}

/// A 0.24 lock body, as `v024::acquire_lock` writes it.
fn v024_body(hostname: &str, pid: u32, boot_id: Option<&str>, time: DateTime<Utc>) -> Vec<u8> {
    serde_json::to_vec(&v024::LockEntry {
        hostname: hostname.into(),
        pid,
        time: time.to_rfc3339(),
        boot_id: boot_id.map(str::to_string),
    })
    .unwrap()
}

fn acquire(storage: &Arc<Counted>, env: &HostEnv) -> Result<LegacyUpgradeLock> {
    LegacyUpgradeLock::acquire(Arc::clone(storage) as Arc<dyn StorageBackend>, env)
}

#[test]
fn acquire_writes_a_0_24_lock() {
    let storage = Counted::new();
    let (time, env) = env();
    let lock = acquire(&storage, &env).unwrap();
    let key = lock.key().to_string();
    let name = key
        .strip_prefix("locks/")
        .and_then(|k| k.strip_suffix(".json"))
        .unwrap();
    let (micros, uuid) = name.split_once('-').unwrap();
    assert!(
        micros.len() == 20 && micros.bytes().all(|b| b.is_ascii_digit()),
        "{key}"
    );
    assert!(
        uuid.len() == 32 && uuid.bytes().all(|b| b.is_ascii_hexdigit()),
        "{key}"
    );
    assert_eq!(lock_keys(&*storage), vec![key.clone()]);

    let entry = entry_of(&*storage, &key);
    assert_eq!(entry.hostname, "upgrader");
    assert_eq!(entry.pid, 4242);
    assert_eq!(entry.boot_id.as_deref(), Some("boot-u"));
    assert_eq!(time_of(&entry), time.now_utc());
    assert!(!v024::should_cleanup_lock(
        &key,
        Some(&entry),
        time.now_utc(),
        Duration::hours(6),
        "other-host",
        None,
        &|_| true,
    ));
    lock.release().unwrap();
    assert!(lock_keys(&*storage).is_empty());
}

#[test]
fn acquire_refuses_beside_a_live_lock() {
    let storage = Counted::new();
    let (_time, env) = env();
    let guard = v024::acquire_lock(&*storage).unwrap();
    let foreign = guard.key().to_string();
    let Err(VykarError::Locked(holder)) = acquire(&storage, &env) else {
        panic!("expected Locked");
    };
    assert_eq!(holder.key, foreign);
    let details = holder.details.unwrap();
    assert_eq!(details.hostname, crate::platform::hostname());
    assert_eq!(lock_keys(&*storage), vec![foreign]);
}

#[test]
fn acquire_reaps_a_lock_aged_6h_on_the_store_clock() {
    for (age, reaped) in [
        (Duration::minutes(6 * 60 + 1), true),
        (Duration::minutes(5 * 60 + 59), false),
    ] {
        let storage = Counted::new();
        let (time, env) = env();
        let foreign = "locks/00000000000000000001-0123456789abcdef0123456789abcdef.json";
        // Its `time` is young: the store's clock decides.
        storage
            .put(foreign, &v024_body("other-host", 7, None, time.now_utc()))
            .unwrap();
        let mtime = std::time::SystemTime::now() - age.to_std().unwrap();
        storage.inner.set_mtime(foreign, mtime);
        let result = acquire(&storage, &env);
        if reaped {
            let lock = result.unwrap();
            assert_eq!(lock_keys(&*storage), vec![lock.key().to_string()]);
            lock.release().unwrap();
        } else {
            let Err(VykarError::Locked(holder)) = result else {
                panic!("{age}: expected Locked");
            };
            assert_eq!(holder.key, foreign);
            assert_eq!(lock_keys(&*storage), vec![foreign.to_string()]);
        }
    }
}

/// A 0.24 lock carries no installation ID, so the same-host rule makes no
/// claim about it even with this hostname, this boot and a dead PID: it is
/// judged by age.
#[test]
fn a_0_24_lock_from_this_hostname_waits_for_age() {
    let storage = Counted::new();
    let (time, env) = env_with(upgrader(), false);
    let foreign = "locks/00000000000000000001-0123456789abcdef0123456789abcdef.json";
    storage
        .put(
            foreign,
            &v024_body("upgrader", 99, Some("boot-u"), time.now_utc()),
        )
        .unwrap();
    let Err(VykarError::Locked(holder)) = acquire(&storage, &env) else {
        panic!("expected Locked");
    };
    assert_eq!(holder.key, foreign);
    assert_eq!(lock_keys(&*storage), vec![foreign.to_string()]);
}

#[test]
fn check_rewrites_every_five_minutes() {
    let storage = Counted::new();
    let (time, env) = env();
    let lock = acquire(&storage, &env).unwrap();
    let key = lock.key().to_string();
    assert_eq!(storage.puts_to(&key), 1);

    // A check with no rewrite due costs one EXISTS.
    advance(&time, Duration::minutes(4));
    let before = storage.requests();
    lock.check().unwrap();
    assert_eq!(storage.requests() - before, 1);
    assert_eq!(storage.puts_to(&key), 1);

    // One with a rewrite: EXISTS, PUT and the LIST of locks/.
    advance(&time, Duration::minutes(2));
    let before = storage.requests();
    lock.check().unwrap();
    assert_eq!(storage.requests() - before, 3);
    assert_eq!(storage.puts_to(&key), 2);
    assert_eq!(time_of(&entry_of(&*storage, &key)), time.now_utc());
    assert_eq!(lock_keys(&*storage), vec![key.clone()]);

    // The interval counts from the rewrite.
    advance(&time, Duration::minutes(4));
    lock.check().unwrap();
    assert_eq!(storage.puts_to(&key), 2);
    lock.release().unwrap();
}

#[test]
fn check_fails_after_5_5h_without_a_rewrite() {
    let storage = Counted::new();
    let (time, env) = env();
    let lock = acquire(&storage, &env).unwrap();
    let key = lock.key().to_string();
    *storage.fail_puts_to.lock().unwrap() = Some(key.clone());

    advance(&time, Duration::minutes(5 * 60 + 29));
    lock.check().unwrap();
    advance(&time, Duration::minutes(2));
    expired_containing(lock.check(), "last confirmed rewrite");
    expired_containing(lock.check(), "last confirmed rewrite");
    // Still ours: release deletes it.
    assert!(storage.exists(&key).unwrap());
    lock.release().unwrap();
    assert!(!storage.exists(&key).unwrap());
}

#[test]
fn check_after_the_key_is_gone() {
    let storage = Counted::new();
    let (_time, env) = env();
    let lock = acquire(&storage, &env).unwrap();
    let key = lock.key().to_string();
    storage.delete(&key).unwrap();
    expired_containing(lock.check(), "removed");
    let before = storage.requests();
    expired_containing(lock.check(), "removed");
    assert_eq!(storage.requests(), before);
    lock.release().unwrap();
    assert_eq!(storage.requests(), before);
}

#[test]
fn check_refuses_on_a_clock_anomaly() {
    let storage = Counted::new();
    let (time, env) = env();
    let lock = acquire(&storage, &env).unwrap();
    time.jump_wall(Duration::minutes(-10));
    expired_containing(lock.check(), "clock anomaly");
    lock.release().unwrap();
}

/// A vykar 0.24 client on the upgrader's hostname that takes the key for
/// stale (it sees the PID dead there: the documented same-hostname residual
/// of step 6) reaps it between the check's EXISTS and its rewrite PUT and
/// takes its own lock; the PUT then recreates the upgrade's key beside it
/// (H2's shape on the legacy key; 10-single-protocol step 9 retired H2 with
/// this binary's lock). The LIST of `locks/` after the rewrite sees the
/// other lock: the check deletes the upgrade's key and fails, and every
/// later check fails without a request.
#[test]
fn a_rewrite_after_a_0_24_reap_stops_the_upgrade() {
    let storage = Counted::new();
    let (time, env) = env();
    let lock = acquire(&storage, &env).unwrap();
    let key = lock.key().to_string();
    let theirs: Arc<Mutex<Option<String>>> = Arc::default();
    {
        let (theirs, ours) = (Arc::clone(&theirs), key.clone());
        let reap_and_take: BeforePut = Box::new(move |inner: &dyn StorageBackend| {
            // 0.24 on the upgrader's hostname and boot, its PID dead there.
            let left = v024::cleanup_stale_locks_inner(
                inner,
                Duration::hours(6),
                "upgrader",
                Some("boot-u"),
                |_| false,
            )
            .unwrap();
            assert!(
                !left.contains(&ours),
                "0.24 reaps the upgrade's key: {left:?}"
            );
            let guard = v024::acquire_lock(inner).unwrap();
            *theirs.lock().unwrap() = Some(guard.key().to_string());
        });
        *storage.before_put_to.lock().unwrap() = Some((key.clone(), reap_and_take));
    }
    // The rewrite is due: EXISTS finds the key, 0.24 reaps it and takes
    // its own lock, the rewrite PUT lands and the LIST after it sees theirs.
    advance(&time, Duration::minutes(6));
    expired_containing(lock.check(), "another lock");
    let theirs = theirs
        .lock()
        .unwrap()
        .clone()
        .expect("the 0.24 client took its lock");
    expired_containing(lock.check(), &theirs);
    assert_eq!(lock_keys(&*storage), vec![theirs.clone()], "ours deleted");
    let before = storage.requests();
    advance(&time, Duration::minutes(6));
    expired_containing(lock.check(), "another lock");
    assert_eq!(storage.requests(), before);
    lock.release().unwrap();
    assert_eq!(storage.requests(), before);
    assert_eq!(lock_keys(&*storage), vec![theirs]);
}

/// Keys under `locks/` that are not `*.json` (probe debris) are no lock:
/// the listing after a rewrite ignores them, as the acquisition does.
#[test]
fn check_ignores_non_lock_keys_under_locks() {
    let storage = Counted::new();
    let (time, env) = env();
    let lock = acquire(&storage, &env).unwrap();
    let key = lock.key().to_string();
    storage.put("locks/probe-x", b"x").unwrap();
    advance(&time, Duration::minutes(6));
    lock.check().unwrap();
    assert_eq!(storage.puts_to(&key), 2);
    lock.release().unwrap();
    assert!(!storage.exists(&key).unwrap());
    assert!(storage.exists("locks/probe-x").unwrap());
}

/// A failed LIST after a rewrite counts as a failed rewrite: the check
/// passes and the next check rewrites and lists again, until one LIST
/// succeeds.
#[test]
fn a_failed_list_after_a_rewrite_is_retried() {
    let storage = Counted::new();
    let (time, env) = env();
    let lock = acquire(&storage, &env).unwrap();
    let key = lock.key().to_string();
    storage.fail_lists.store(true, Ordering::SeqCst);

    advance(&time, Duration::minutes(6));
    lock.check().unwrap();
    assert_eq!(storage.puts_to(&key), 2);
    advance(&time, Duration::minutes(1));
    lock.check().unwrap();
    assert_eq!(storage.puts_to(&key), 3, "still due");

    storage.fail_lists.store(false, Ordering::SeqCst);
    advance(&time, Duration::minutes(1));
    lock.check().unwrap();
    assert_eq!(storage.puts_to(&key), 4);
    advance(&time, Duration::minutes(1));
    lock.check().unwrap();
    assert_eq!(storage.puts_to(&key), 4, "confirmed");
    lock.release().unwrap();
}
