use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread::JoinHandle;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::ages::{CLOCK_PROBE_DEBRIS_AGE, CLOCK_PROBE_PREFIX, StoreClock};
use super::control::CancelToken;
use super::host_env::{Clock, HostEnv, StopSignal};
use vykar_storage::{ListedObject, StorageBackend, StoreNow};
use vykar_types::error::{LockHolder, LockHolderDetails, Result, VykarError};

/// A simple advisory lock stored in `locks/<uuid>.json`.
#[derive(Debug, Serialize, Deserialize)]
struct LockEntry {
    hostname: String,
    pid: u32,
    time: String,
    #[serde(default)]
    boot_id: Option<String>,
    #[serde(default)]
    installation_id: Option<String>,
    #[serde(default)]
    pid_ns: Option<u64>,
}

impl LockEntry {
    /// An entry for `env`'s process, stamped `time`.
    fn new(env: &HostEnv, time: DateTime<Utc>) -> Self {
        Self {
            hostname: env.host.hostname.clone(),
            pid: env.host.pid,
            time: time.to_rfc3339(),
            boot_id: env.host.boot_id.clone(),
            installation_id: env.host.installation_id.clone(),
            pid_ns: env.host.pid_ns,
        }
    }
}

const LOCKS_PREFIX: &str = "locks/";
const DEFAULT_STALE_LOCK_SECS: i64 = 6 * 60 * 60; // 6 hours

/// Refresh the lock file every 3 hours to prevent stale-lock cleanup.
const LOCK_REFRESH_INTERVAL_SECS: u64 = 3 * 60 * 60;

/// Abort if the lock has not been refreshed for this long.
/// Set to DEFAULT_STALE_LOCK_SECS minus 30 minutes.
const LOCK_MAX_UNREFRESHED_SECS: u64 = (DEFAULT_STALE_LOCK_SECS as u64) - 30 * 60;

/// Handle to an acquired lock.
#[derive(Debug)]
pub struct LockGuard {
    key: String,
    /// Wall-clock time of acquisition, read after the confirming LIST.
    acquired_at: DateTime<Utc>,
    /// The lock key's mtime in the confirming LIST: "now" on the store's
    /// clock at acquisition. `None` when the storage reports no mtime.
    store_acquired: Option<StoreNow>,
    /// Clock probes the confirming LIST showed older than
    /// `CLOCK_PROBE_DEBRIS_AGE` by `store_acquired`: debris of failed
    /// deletes, for [`Self::remove_probe_debris`].
    probe_debris: Vec<String>,
}

impl LockGuard {
    pub fn key(&self) -> &str {
        &self.key
    }

    /// "Now" on the store's clock when the lock was acquired: the mtime the
    /// storage gave this lock's key.
    pub(crate) fn store_time(&self) -> Option<StoreNow> {
        self.store_acquired
    }

    /// Delete the clock probes the acquisition listed as debris (best
    /// effort, debug log). An uncontended acquirer never probes, so without
    /// this nothing would remove debris while the lock is uncontended.
    pub(crate) fn remove_probe_debris(&self, storage: &dyn StorageBackend) {
        for key in &self.probe_debris {
            if let Err(e) = storage.delete(key) {
                debug!(key, error = %e, "could not delete an old clock probe");
            }
        }
    }
}

/// Acquire an advisory lock on the repository.
///
/// The lock is held only if, after our PUT, a LIST shows our key and no other.
/// Any other key means we lose, regardless of timestamps, so a client with a
/// skewed clock cannot jump an established holder. If two contenders PUT
/// concurrently, both see each other and both back off.
///
/// Relies on the [`StorageBackend::list`] contract: a LIST reflects every PUT
/// that completed before it started, including other clients' PUTs.
pub fn acquire_lock(storage: &dyn StorageBackend, env: &HostEnv) -> Result<LockGuard> {
    acquire_lock_reading(storage, env, &mut StoreClock::default())
}

/// [`acquire_lock`] with the store-clock reading of the wait it is an
/// attempt of: the attempts of one wait share `clock`, so a contended
/// wait probes the store's clock once, not once per attempt.
pub(crate) fn acquire_lock_reading(
    storage: &dyn StorageBackend,
    env: &HostEnv,
    clock: &mut StoreClock,
) -> Result<LockGuard> {
    // Don't publish a key while a holder exists: waiters would otherwise
    // collide with each other on every retry.
    let held = cleanup_stale_locks(
        storage,
        Duration::seconds(DEFAULT_STALE_LOCK_SECS),
        env,
        NowSource::Probe(clock),
    )?;
    if let Some(holder_key) = held.first() {
        return Err(VykarError::Locked(build_lock_holder(
            storage,
            holder_key,
            env.clock.as_ref(),
        )));
    }

    let now = env.now_utc();
    let entry = LockEntry::new(env, now);

    let uuid = format!("{:032x}", rand::random::<u128>());
    // The timestamp prefix is not used for ordering. It is kept because older
    // clients still order by it, and stale reaping falls back to it when the
    // entry body is unreadable.
    let ts = now.timestamp_micros();
    let key = format!("{LOCKS_PREFIX}{ts:020}-{uuid}.json");
    let data = serde_json::to_vec(&entry)
        .map_err(|e| vykar_types::error::VykarError::Other(format!("lock serialize: {e}")))?;

    if let Err(e) = storage.put(&key, &data) {
        // A PUT can fail after it committed (a lost reply, or a retry wait
        // cut by a cancel). The key is ours alone: never leave it behind.
        let _ = storage.delete(&key);
        return Err(e);
    }

    let mut listing = match storage.list_meta(LOCKS_PREFIX) {
        Ok(listing) => listing,
        Err(e) => {
            // Our key may be the only one: never leave it behind.
            let _ = storage.delete(&key);
            return Err(e);
        }
    };
    let store_acquired = StoreNow::of_written(&listing, &key);
    let probe_debris = store_acquired.map_or_else(Vec::new, |now| {
        listing
            .iter()
            .filter(|o| {
                o.key.starts_with(CLOCK_PROBE_PREFIX)
                    && !is_lock_key(&o.key)
                    && o.mtime
                        .is_some_and(|m| now.age_of(m) > CLOCK_PROBE_DEBRIS_AGE)
            })
            .map(|o| o.key.clone())
            .collect()
    });
    listing.retain(|o| is_lock_key(&o.key));
    let mut keys: Vec<String> = listing.into_iter().map(|o| o.key).collect();
    keys.sort();
    if !keys.contains(&key) {
        // PUT succeeded but LIST doesn't include our key — storage-layer bug
        // or an unsupported eventual-consistency model. Clean up and fail.
        let _ = storage.delete(&key);
        return Err(VykarError::Other(
            "lock acquisition failed: written lock key not returned by LIST".into(),
        ));
    }
    if let Some(other) = keys.iter().find(|k| **k != key) {
        let _ = storage.delete(&key);
        return Err(VykarError::Locked(build_lock_holder(
            storage,
            other,
            env.clock.as_ref(),
        )));
    }

    Ok(LockGuard {
        key,
        acquired_at: env.now_utc(),
        store_acquired,
        probe_debris,
    })
}

/// Release an advisory lock.
pub fn release_lock(storage: &dyn StorageBackend, guard: LockGuard) -> Result<()> {
    storage.delete(&guard.key)
}

/// Forcibly remove all advisory locks from the repository.
///
/// This is a recovery mechanism for stale locks left by killed processes.
/// No passphrase is needed — lock files are unencrypted JSON.
/// Returns the number of locks removed. Clock probes left under `locks/`
/// (`locks/vykar-clock-*`, not locks) are deleted too and not counted.
pub fn break_lock(storage: &dyn StorageBackend) -> Result<usize> {
    let mut removed: usize = 0;

    for key in storage.list(LOCKS_PREFIX)? {
        if is_lock_key(&key) {
            storage.delete(&key)?;
            removed += 1;
        } else if key.starts_with(CLOCK_PROBE_PREFIX) {
            storage.delete(&key)?;
        }
    }

    Ok(removed)
}

/// Whether `key` is a lock key (`locks/*.json`); a clock probe is not.
fn is_lock_key(key: &str) -> bool {
    key.starts_with(LOCKS_PREFIX) && key.ends_with(".json")
}

/// Describe the process holding `lock_key`, for the `Locked` error message.
///
/// Falls back to `LockHolder { details: None }` when the lock object cannot be
/// fetched or decoded — the holder may have released it between our LIST and
/// this GET, so this is a normal race, not an error. The age is read on
/// `clock` after the GET.
fn build_lock_holder(
    storage: &dyn StorageBackend,
    lock_key: &str,
    clock: &dyn Clock,
) -> LockHolder {
    let bare = || LockHolder {
        key: lock_key.to_string(),
        details: None,
    };
    let Some(data) = storage.get(lock_key).ok().flatten() else {
        return bare();
    };
    let Ok(entry) = serde_json::from_slice::<LockEntry>(&data) else {
        return bare();
    };
    // `lock_timestamp` falls back to the key's microsecond prefix when the
    // entry's own `time` field is unparseable.
    let age = lock_timestamp(Some(&entry), lock_key).map_or_else(
        || "unknown".to_string(),
        |ts| format_age_since(&clock.now_utc(), &ts),
    );
    LockHolder {
        key: lock_key.to_string(),
        details: Some(LockHolderDetails {
            hostname: entry.hostname,
            pid: entry.pid,
            age,
        }),
    }
}

/// Fail with `Locked` if any non-stale lock is present. Writes no lock key.
///
/// Safe against a concurrent maintenance lock without taking one: the caller
/// writes its session marker before this LIST, and maintenance writes its lock
/// before listing sessions, so at least one of the two sees the other.
///
/// `marker_key` is the marker the caller has just written: its mtime is
/// "now" on the store's clock for the lock ages.
fn probe_unlocked(storage: &dyn StorageBackend, env: &HostEnv, marker_key: &str) -> Result<()> {
    let held = cleanup_stale_locks(
        storage,
        Duration::seconds(DEFAULT_STALE_LOCK_SECS),
        env,
        NowSource::Written(marker_key),
    )?;
    match held.first() {
        Some(key) => Err(VykarError::Locked(build_lock_holder(
            storage,
            key,
            env.clock.as_ref(),
        ))),
        None => Ok(()),
    }
}

/// Where a reaper reads "now" on the store's clock. Resolved only when the
/// listing it judges reports an mtime for at least one lock key.
pub(crate) enum NowSource<'a> {
    /// `key`, which this client has just written: LIST its directory with
    /// mtimes and take the key's mtime.
    Written(&'a str),
    /// The wait's reading of the store's clock, extrapolated or refreshed
    /// by a clock probe (`ages::StoreClock::now`).
    Probe(&'a mut StoreClock),
}

impl NowSource<'_> {
    /// The reading, and when a probe was made the fresher listing of
    /// `locks/` it made. A failure to read is "now unknown", never an
    /// error: a reaper then reaps nothing by age, and its caller retries as
    /// it would on a live holder.
    fn resolve(
        self,
        storage: &dyn StorageBackend,
        env: &HostEnv,
    ) -> (Option<StoreNow>, Option<Vec<ListedObject>>) {
        match self {
            Self::Written(key) => {
                let dir = key.rfind('/').map_or("", |i| &key[..=i]);
                match storage.list_meta(dir) {
                    Ok(listing) => (StoreNow::of_written(&listing, key), None),
                    Err(e) => {
                        debug!(key, error = %e, "could not read the store's clock; reaping nothing by age");
                        (None, None)
                    }
                }
            }
            Self::Probe(clock) => clock.now(storage, env),
        }
    }
}

/// An entry's age as a reaper judges it.
#[derive(Clone, Copy, Debug)]
enum EntryAge {
    /// On the store's clock: (store now − the entry's mtime).
    Store(std::time::Duration),
    /// The listing reported no mtime (test doubles; a vykar-server older
    /// than `?list&sized&mtime`; an S3 `LastModified` that does not
    /// parse): the entry's own timestamp against `env`'s clock, as before
    /// 04 step 5.
    Timestamp(Option<Duration>),
    /// The listing reported an mtime but "now" could not be read: never
    /// stale by age (fail closed).
    Unknown,
}

impl EntryAge {
    /// The age of an entry listed with `mtime`, judged at `now`;
    /// `timestamp` gives the fallback when there is no mtime (warned once
    /// per run of `env`).
    fn of(
        mtime: Option<std::time::SystemTime>,
        now: Option<StoreNow>,
        env: &HostEnv,
        timestamp: impl FnOnce() -> Option<Duration>,
    ) -> Self {
        match (mtime, now) {
            (Some(mtime), Some(now)) => Self::Store(now.age_of(mtime)),
            (Some(_), None) => Self::Unknown,
            (None, _) => {
                warn_timestamp_fallback(env);
                Self::Timestamp(timestamp())
            }
        }
    }

    fn exceeds(self, max_age: Duration) -> bool {
        match self {
            Self::Store(age) => max_age.to_std().is_ok_and(|max| age > max),
            Self::Timestamp(age) => age.is_some_and(|age| age > max_age),
            Self::Unknown => false,
        }
    }

    /// The age as the same-host rule compares it with local uptime.
    fn as_std(self) -> Option<std::time::Duration> {
        match self {
            Self::Store(age) => Some(age),
            Self::Timestamp(age) => age.and_then(|a| a.to_std().ok()),
            Self::Unknown => None,
        }
    }
}

/// One warning per opened repository (`HostEnv::for_new_run`), and one
/// more for a backup's session registration, which runs before the open,
/// when a lock or marker is aged by the timestamp inside it because the
/// storage listed it without an mtime: those ages depend on the writer's
/// and this machine's clocks.
fn warn_timestamp_fallback(env: &HostEnv) {
    if env.first_timestamp_fallback() {
        warn!(
            "the storage listed locks or session markers without modification times; \
             judging their age by the timestamps inside them (client clocks)"
        );
    }
}

/// Remove stale locks and return the keys that remain.
///
/// Ages are on the store's clock: (store now − the lock key's mtime), with
/// "now" read from `now`. When the listing reports no mtime the lock's own
/// timestamp (or its key prefix) is aged against `env`'s clock instead;
/// when "now" cannot be read nothing is stale by age (fail closed).
///
/// A lock is stale when it is older than `max_age`, or when [`same_host`]
/// finds it was left by a dead process or an earlier boot of this machine.
/// With "now" known, clock probes older than an hour (debris of a failed
/// delete) are removed, best effort.
pub(crate) fn cleanup_stale_locks(
    storage: &dyn StorageBackend,
    max_age: Duration,
    env: &HostEnv,
    now: NowSource<'_>,
) -> Result<Vec<String>> {
    let mut listing = storage.list_meta(LOCKS_PREFIX)?;
    let mut store_now = None;
    if listing
        .iter()
        .any(|o| is_lock_key(&o.key) && o.mtime.is_some())
    {
        let (reading, fresher) = now.resolve(storage, env);
        store_now = reading;
        if let Some(fresher) = fresher {
            // The probe's listing is the later one: judge by it.
            listing = fresher;
        }
    }
    let local_now = env.now_utc();
    let mut surviving = Vec::new();
    for object in &listing {
        let key = object.key.as_str();
        if !is_lock_key(key) {
            if let Some(now) = store_now
                && key.starts_with(CLOCK_PROBE_PREFIX)
                && object
                    .mtime
                    .is_some_and(|m| now.age_of(m) > CLOCK_PROBE_DEBRIS_AGE)
                && let Err(e) = storage.delete(key)
            {
                debug!(key, error = %e, "could not delete an old clock probe");
            }
            continue;
        }
        let Some(data) = storage.get(key)? else {
            continue;
        };
        let entry = serde_json::from_slice::<LockEntry>(&data).ok();
        let age = EntryAge::of(object.mtime, store_now, env, || {
            lock_timestamp(entry.as_ref(), key).map(|ts| local_now.signed_duration_since(ts))
        });
        if should_cleanup_lock(key, entry.as_ref(), age, max_age, env) {
            storage.delete(key)?;
        } else {
            surviving.push(key.to_string());
        }
    }
    surviving.sort();
    Ok(surviving)
}

fn should_cleanup_lock(
    key: &str,
    entry: Option<&LockEntry>,
    age: EntryAge,
    max_age: Duration,
    env: &HostEnv,
) -> bool {
    if let Some(entry) = entry {
        let verdict = same_host(
            &EntryHost {
                hostname: &entry.hostname,
                pid: entry.pid,
                boot_id: entry.boot_id.as_deref(),
                installation_id: entry.installation_id.as_deref(),
                pid_ns: entry.pid_ns,
                age: age.as_std(),
            },
            env,
        );
        if matches!(verdict, SameHost::DeadProcess | SameHost::EarlierBoot) {
            debug!(
                lock_key = key,
                ?verdict,
                "cleaning lock left on this machine"
            );
            return true;
        }
    }

    age.exceeds(max_age)
}

/// Who wrote a lock or session entry, as the same-host rule reads it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EntryHost<'a> {
    pub hostname: &'a str,
    pub pid: u32,
    pub boot_id: Option<&'a str>,
    pub installation_id: Option<&'a str>,
    pub pid_ns: Option<u64>,
    /// How long ago the entry was last written: on the store's clock (its
    /// mtime), or by its own timestamp when the listing reports no mtime.
    pub age: Option<std::time::Duration>,
}

/// What the same-host rule says about an entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SameHost {
    /// Not provably this machine: judged by age alone.
    Unknown,
    /// This machine and boot, and the process is alive.
    Alive,
    /// This machine and boot, and the process is gone: stale.
    DeadProcess,
    /// This installation, an earlier boot: older than our uptime, stale.
    EarlierBoot,
}

/// The same-host verdict on `entry` for `env`.
///
/// "This machine" needs the same hostname **and** the same installation ID
/// (both known), so namesakes (other installations) and old clients (no
/// installation ID) are judged by age alone. This narrows the old
/// same-hostname rules with one widening, for session markers: the old
/// marker rule needed a dead PID, while `EarlierBoot` reaps a marker of
/// this installation's earlier boot even when a local process now has that
/// PID number (04 step 1). With another boot ID the entry is stale only once
/// it is older than our uptime: a reboot left it behind, while a cloned
/// machine that shares the installation wrote it after both booted. With
/// the same boot (or no boot ID on either side, as on macOS) and the same
/// PID namespace, a dead PID makes it stale.
///
/// Residual: a clone that booted after its sibling's last write reaps the
/// sibling's live entry (for markers about one refresh interval after the
/// clone's boot, for locks up to the lock refresh interval).
pub(crate) fn same_host(entry: &EntryHost<'_>, env: &HostEnv) -> SameHost {
    let local = &env.host;
    if entry.hostname != local.hostname {
        return SameHost::Unknown;
    }
    let (Some(mine), Some(theirs)) = (local.installation_id.as_deref(), entry.installation_id)
    else {
        return SameHost::Unknown;
    };
    if mine != theirs {
        return SameHost::Unknown;
    }
    match (local.boot_id.as_deref(), entry.boot_id) {
        (Some(a), Some(b)) if a != b => {
            // Our earlier boot, or a clone of this installation that wrote
            // it after both machines booted.
            let older = entry
                .age
                .zip(env.clock.uptime())
                .is_some_and(|(age, up)| age > up);
            return if older {
                SameHost::EarlierBoot
            } else {
                SameHost::Unknown
            };
        }
        (Some(_), Some(_)) | (None, None) => {}
        // One side does not know its boot: no claim.
        _ => return SameHost::Unknown,
    }
    if local.pid_ns != entry.pid_ns {
        return SameHost::Unknown;
    }
    if env.is_pid_alive(entry.pid) {
        SameHost::Alive
    } else {
        SameHost::DeadProcess
    }
}

fn lock_timestamp(entry: Option<&LockEntry>, key: &str) -> Option<chrono::DateTime<Utc>> {
    entry
        .and_then(|entry| {
            chrono::DateTime::parse_from_rfc3339(&entry.time)
                .ok()
                .map(|ts| ts.with_timezone(&Utc))
        })
        .or_else(|| lock_key_timestamp(key))
}

fn lock_key_timestamp(key: &str) -> Option<chrono::DateTime<Utc>> {
    let raw = key.strip_prefix(LOCKS_PREFIX)?.strip_suffix(".json")?;
    let (micros, _) = raw.split_once('-')?;
    if micros.len() != 20 || !micros.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let micros = micros.parse::<i64>().ok()?;
    chrono::DateTime::<Utc>::from_timestamp_micros(micros)
}

/// Build a lock fence closure that verifies the lock is still valid.
///
/// Another client can only acquire the lock once our key is gone, so the
/// existence check detects loss of the lock at the next check. It is
/// detection, not prevention: a storage write already in flight when the key
/// is removed (`break-lock`, stale reaping) still completes.
///
/// The returned closure checks:
/// 1. The lock file still exists on storage (not cleaned up by another client).
/// 2. Time since last refresh has not exceeded `LOCK_MAX_UNREFRESHED_SECS`.
/// 3. If the refresh interval has elapsed, rewrites the lock file with a fresh timestamp.
pub fn build_lock_fence(
    guard: &LockGuard,
    storage: Arc<dyn StorageBackend>,
    env: HostEnv,
) -> Arc<dyn Fn() -> Result<()> + Send + Sync> {
    let lock_key = guard.key.clone();
    // A clock before the epoch gives a negative value, which the anomaly
    // check in `verify_lock_validity` refuses.
    let last_refreshed = Arc::new(AtomicI64::new(guard.acquired_at.timestamp()));

    Arc::new(move || {
        verify_lock_validity(
            &lock_key,
            last_refreshed.load(Ordering::SeqCst),
            &*storage,
            env.clock.as_ref(),
        )?;

        // Refresh lock file if refresh interval has elapsed.
        let now_secs = env.now_utc().timestamp();
        let elapsed = now_secs - last_refreshed.load(Ordering::SeqCst);
        if elapsed >= LOCK_REFRESH_INTERVAL_SECS as i64 {
            let entry = LockEntry::new(&env, env.now_utc());
            if let Ok(data) = serde_json::to_vec(&entry)
                && storage.put(&lock_key, &data).is_ok()
            {
                last_refreshed.store(now_secs, Ordering::SeqCst);
                debug!(lock_key = %lock_key, "lock file refreshed");
            }
        }

        Ok(())
    })
}

/// Verify that a lock is still valid by checking existence and time elapsed.
///
/// The time is read on `clock` after the existence check. It is wall-clock
/// time, so a suspended machine finds its lock unrefreshed on resume.
///
/// This is the core validation logic, exposed for unit testing.
pub fn verify_lock_validity(
    lock_key: &str,
    last_refreshed_secs: i64,
    storage: &dyn StorageBackend,
    clock: &dyn Clock,
) -> Result<()> {
    // 1. Check lock file still exists.
    if !storage.exists(lock_key)? {
        return Err(VykarError::LockExpired(
            "lock file removed by another client".into(),
        ));
    }

    // 2. Check time since last refresh.
    let now_secs = clock.now_utc().timestamp();

    let elapsed = now_secs - last_refreshed_secs;

    // Fail closed on clock anomaly (negative elapsed, or a clock before the
    // epoch).
    if elapsed < 0 || now_secs < 0 {
        return Err(VykarError::LockExpired(
            "clock anomaly detected; refusing to write with potentially stale lock".into(),
        ));
    }

    if elapsed > LOCK_MAX_UNREFRESHED_SECS as i64 {
        return Err(VykarError::LockExpired(format!(
            "lock unrefreshed for {elapsed}s (limit: {LOCK_MAX_UNREFRESHED_SECS}s); \
             machine may have been suspended"
        )));
    }

    Ok(())
}

// ── Session markers ──────────────────────────────────────────────────────

/// Prefix of every session object: the markers `sessions/<id>.json`
/// (generation 0, written at registration, the shape every released binary
/// writes) and `sessions/<id>.<n>.json` (the `n`-th refresh attempt), and the
/// journal `sessions/<id>.index`. Of a session's markers the newest
/// generation is the live one.
pub(crate) const SESSIONS_PREFIX: &str = "sessions/";
/// Sessions whose newest marker was last written strictly longer ago than
/// this (by its mtime on the store's clock; by its `last_refresh` when the
/// listing reports no mtime) are treated as stale. Shared between maintenance cleanup
/// ([`cleanup_stale_sessions`]) and pending-index recovery.
///
/// Set to 3× the 15-minute session heartbeat interval — two missed refreshes
/// still keep a session live, a third marks it dead.
pub(crate) const SESSION_STALE_SECS: i64 = 45 * 60;

/// Storage key of generation `generation` of a session's marker:
/// `sessions/<id>.json` for 0 (written at registration, the shape every
/// released binary writes), `sessions/<id>.<n>.json` for the `n`-th refresh
/// attempt. Session IDs contain no '.' (callers generate 32 hex digits).
pub(crate) fn session_marker_key_at(session_id: &str, generation: u64) -> String {
    if generation == 0 {
        format!("{SESSIONS_PREFIX}{session_id}.json")
    } else {
        format!("{SESSIONS_PREFIX}{session_id}.{generation}.json")
    }
}

/// [`session_marker_key_at`] for generation 0: `sessions/<id>.json`.
pub(crate) fn session_marker_key(session_id: &str) -> String {
    session_marker_key_at(session_id, 0)
}

/// Storage key for a session's pending index journal: `sessions/<id>.index`.
pub(crate) fn session_index_key(session_id: &str) -> String {
    format!("{SESSIONS_PREFIX}{session_id}.index")
}

/// The session and generation a `.json` key under `sessions/` names.
///
/// Total over such keys, so every marker counts (fail closed): a stem
/// `<x>.<n>` with `n` canonical decimal ≥ 1 (no leading zero) and `x`
/// non-empty is (`x`, n); any other stem is (stem, 0). `None` for keys
/// outside `sessions/`, without the `.json` suffix, or with an empty stem.
pub(crate) fn parse_session_marker_key(key: &str) -> Option<(&str, u64)> {
    let stem = key.strip_prefix(SESSIONS_PREFIX)?.strip_suffix(".json")?;
    if stem.is_empty() {
        return None;
    }
    if let Some((id, n)) = stem.rsplit_once('.')
        && !id.is_empty()
        && !n.is_empty()
        && !n.starts_with('0')
        && n.bytes().all(|b| b.is_ascii_digit())
        && let Ok(generation) = n.parse::<u64>()
    {
        return Some((id, generation));
    }
    Some((stem, 0))
}

/// The `.json` markers among `keys`, grouped by session ID (ascending), each
/// session's keys in ascending generation: the last is the live marker.
fn group_session_markers(keys: &[String]) -> std::collections::BTreeMap<&str, Vec<(u64, &str)>> {
    let mut sessions: std::collections::BTreeMap<&str, Vec<(u64, &str)>> =
        std::collections::BTreeMap::new();
    for key in keys {
        if let Some((id, generation)) = parse_session_marker_key(key) {
            sessions
                .entry(id)
                .or_default()
                .push((generation, key.as_str()));
        }
    }
    for markers in sessions.values_mut() {
        markers.sort_unstable();
    }
    sessions
}

/// A session marker stored at `sessions/<session_id>[.<n>].json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionEntry {
    pub hostname: String,
    pub pid: u32,
    pub registered_at: String,
    pub last_refresh: String,
    #[serde(default)]
    pub boot_id: Option<String>,
    #[serde(default)]
    pub installation_id: Option<String>,
    #[serde(default)]
    pub pid_ns: Option<u64>,
}

impl SessionEntry {
    /// The marker of `env`'s process, registered at `registered_at`, last
    /// written at `last_refresh`. The one place that fills the identity
    /// fields.
    pub(crate) fn new(
        env: &HostEnv,
        registered_at: DateTime<Utc>,
        last_refresh: DateTime<Utc>,
    ) -> Self {
        Self {
            hostname: env.host.hostname.clone(),
            pid: env.host.pid,
            registered_at: registered_at.to_rfc3339(),
            last_refresh: last_refresh.to_rfc3339(),
            boot_id: env.host.boot_id.clone(),
            installation_id: env.host.installation_id.clone(),
            pid_ns: env.host.pid_ns,
        }
    }
}

/// When a marker write completed, on every clock a gap is measured with.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MarkerStamp {
    mono: std::time::Instant,
    uptime: Option<std::time::Duration>,
    wall: DateTime<Utc>,
}

impl MarkerStamp {
    pub(crate) fn now(clock: &dyn Clock) -> Self {
        Self {
            mono: clock.monotonic(),
            uptime: clock.uptime(),
            wall: clock.now_utc(),
        }
    }

    /// Time from `self` to `later`, suspend included: the uptime difference
    /// (`CLOCK_BOOTTIME`) when both have one, else the larger of the
    /// monotonic difference and the wall-clock difference (clamped at
    /// zero). Never less than the monotonic difference.
    pub(crate) fn until(&self, later: &MarkerStamp) -> std::time::Duration {
        let mono = later.mono.saturating_duration_since(self.mono);
        match (self.uptime, later.uptime) {
            (Some(a), Some(b)) => b.saturating_sub(a).max(mono),
            _ => {
                let wall = (later.wall - self.wall)
                    .to_std()
                    .unwrap_or(std::time::Duration::ZERO);
                wall.max(mono)
            }
        }
    }
}

/// A registered session: what [`SessionGuard::adopt`] needs.
#[derive(Debug)]
pub struct Registration {
    session_id: String,
    entry: SessionEntry,
    written: MarkerStamp,
}

impl Registration {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

/// Register a backup session. Writes a session marker, then checks (without
/// writing a lock key) that no maintenance operation holds the lock.
///
/// If a lock is held (maintenance or another commit), retries briefly
/// (another backup's commit phase is typically seconds). If still locked
/// after retries, deletes the marker and returns `Err(Locked)`.
///
/// Blocks the calling thread on `env.clock` between attempts. Callers running
/// inside an async runtime must invoke this via `tokio::task::spawn_blocking`
/// (see `commands/mount.rs` for the established pattern).
pub fn register_session(
    storage: &dyn StorageBackend,
    session_id: &str,
    env: &HostEnv,
) -> Result<Registration> {
    register_session_inner(storage, session_id, 3, 2000, env)
}

pub(crate) fn register_session_inner(
    storage: &dyn StorageBackend,
    session_id: &str,
    max_attempts: usize,
    base_delay_ms: u64,
    env: &HostEnv,
) -> Result<Registration> {
    let key = session_marker_key(session_id);
    let now = env.now_utc();
    let entry = SessionEntry::new(env, now, now);
    let data = serde_json::to_vec(&entry)
        .map_err(|e| VykarError::Other(format!("session serialize: {e}")))?;
    if let Err(e) = storage.put(&key, &data) {
        // The PUT may have committed although it failed (a lost reply, or a
        // retry wait cut by a cancel): do not leave the marker behind.
        let _ = storage.delete(&key);
        return Err(e);
    }
    let written = MarkerStamp::now(env.clock.as_ref());

    match retry_while_locked(max_attempts, base_delay_ms, env.clock.as_ref(), || {
        probe_unlocked(storage, env, &key)
    }) {
        Ok(()) => {
            debug!(session_id, "session registered, no active lock");
            Ok(Registration {
                session_id: session_id.to_string(),
                entry,
                written,
            })
        }
        Err(e) => {
            // Lock held or unexpected error — clean up and propagate.
            let _ = storage.delete(&key);
            Err(e)
        }
    }
}

/// Deregister a session before a [`SessionGuard`] exists: delete its
/// registration marker. Best-effort: retries twice on failure.
///
/// Blocks the calling thread on `env.clock` between retries. Callers running
/// inside an async runtime must invoke this via `tokio::task::spawn_blocking`
/// (see `commands/mount.rs` for the established pattern).
pub fn deregister_session(storage: &dyn StorageBackend, session_id: &str, env: &HostEnv) {
    deregister_keys(storage, session_id, &[session_marker_key(session_id)], env);
}

/// Delete `keys` of session `session_id`, each with 3 attempts 500 ms apart
/// on `env.clock`, warning after the last.
fn deregister_keys(storage: &dyn StorageBackend, session_id: &str, keys: &[String], env: &HostEnv) {
    for key in keys {
        for attempt in 0..3 {
            match storage.delete(key) {
                Ok(()) => {
                    debug!(session_id, key = %key, "session marker deleted");
                    break;
                }
                Err(e) => {
                    if attempt < 2 {
                        debug!(session_id, key = %key, error = %e, "retrying session deregister");
                        env.clock.sleep(std::time::Duration::from_millis(500));
                    } else {
                        warn!(session_id, key = %key, error = %e, "failed to deregister session after retries");
                    }
                }
            }
        }
    }
}

/// Longest stretch without a successful marker write that 04's commit rule
/// tolerates ("not refreshed for more than 30 minutes"): two refresh
/// intervals, below the 45-minute staleness.
pub(crate) const MARKER_GAP_LIMIT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// What a session knows about its marker; shared by the heartbeat (the only
/// writer) and the commit.
///
/// The `n`-th refresh attempt PUTs a new key, generation `n`, checks with an
/// EXISTS that the key it replaces is still there, and deletes the key
/// before that one. No key is ever written twice, so a key a reaper deleted
/// cannot come back through this client's requests (a late copy of an
/// earlier PUT, delayed or duplicated by the transport, still can), and a
/// refresh that finds its previous key gone knows the marker was missing. A
/// live session keeps two keys for a whole refresh interval: a LIST that is
/// not atomic can pass the new key's position before its PUT, but then
/// reaches the old key, which still exists. The one assumption left is that a LIST does not miss a key that
/// existed for its whole duration.
pub(crate) struct SessionMarker {
    session_id: String,
    /// Identity and `registered_at` of every key this session writes.
    entry: SessionEntry,
    state: std::sync::Mutex<MarkerState>,
}

struct MarkerState {
    /// Generation of the key that is the marker now: the newest one whose
    /// PUT succeeded.
    current: u64,
    /// When the current key's PUT completed.
    current_written: MarkerStamp,
    /// Highest generation attempted; a key is never attempted twice.
    attempted: u64,
    /// Generations whose key may exist: the current one, earlier ones not
    /// yet deleted, keys whose PUT failed (it may have landed) and keys
    /// whose DELETE failed. Deregistration deletes all of them.
    owned: std::collections::BTreeSet<u64>,
    /// Superseded keys whose presence check failed: checked again at
    /// every heartbeat poll, at the next refresh and by the commit's probe, never deleted before an
    /// EXISTS found them.
    unchecked: std::collections::BTreeSet<u64>,
    /// Superseded keys found present, deleted by the next refresh.
    retired: std::collections::BTreeSet<u64>,
    /// When the last successful marker write completed: registration, or a
    /// write whose predecessors were all found present.
    last_ok: MarkerStamp,
    longest_gap: std::time::Duration,
    missing: bool,
    refreshes: u32,
    failed_refreshes: u32,
}

impl MarkerState {
    /// Apply one presence check of the superseded key `generation`: `true`
    /// if it settled the key (found or found gone). A result for a key that
    /// is no longer waiting for a check (the other checker settled it) is
    /// ignored, so a session's own delete of a settled key never reads as a
    /// reap.
    fn settle(&mut self, generation: u64, present: bool) {
        if !self.unchecked.remove(&generation) {
            return;
        }
        if present {
            self.retired.insert(generation);
        } else {
            self.owned.remove(&generation);
            self.missing = true;
        }
    }

    /// The writes up to the current key are all proven: close the stretch.
    fn close_gap(&mut self) -> std::time::Duration {
        let gap = self.last_ok.until(&self.current_written);
        self.longest_gap = self.longest_gap.max(gap);
        self.last_ok = self.current_written;
        gap
    }
}

/// A snapshot of the record, with the open stretch since the last
/// successful write included in `longest_gap`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MarkerReport {
    pub missing: bool,
    pub longest_gap: std::time::Duration,
    /// Superseded keys whose presence check has not succeeded yet.
    pub unchecked: usize,
    pub refreshes: u32,
    pub failed_refreshes: u32,
}

impl MarkerReport {
    /// Never missing, every superseded key checked, and never unrefreshed
    /// longer than [`MARKER_GAP_LIMIT`].
    pub(crate) fn continuous(&self) -> bool {
        !self.missing && self.unchecked == 0 && self.longest_gap <= MARKER_GAP_LIMIT
    }
}

/// What one refresh attempt found.
#[derive(Debug)]
pub(crate) enum RefreshOutcome {
    /// New key written, every superseded key confirmed present. `gap`: time
    /// since the last successful write.
    Refreshed { gap: std::time::Duration },
    /// New key written; a superseded key was gone: a reaper deleted it.
    Missing,
    /// The new key was written; a presence check failed, so a superseded key
    /// stays unchecked. The gap stays open; the heartbeat checks it again at
    /// its next poll.
    Unchecked(VykarError),
    /// The new key's PUT failed (it may exist). Nothing about the past is
    /// learned; the gap stays open.
    Failed(VykarError),
}

impl SessionMarker {
    pub(crate) fn new(registration: &Registration) -> Self {
        Self {
            session_id: registration.session_id.clone(),
            entry: registration.entry.clone(),
            state: std::sync::Mutex::new(MarkerState {
                current: 0,
                current_written: registration.written,
                attempted: 0,
                owned: std::collections::BTreeSet::from([0]),
                unchecked: std::collections::BTreeSet::new(),
                retired: std::collections::BTreeSet::new(),
                last_ok: registration.written,
                longest_gap: std::time::Duration::ZERO,
                missing: false,
                refreshes: 0,
                failed_refreshes: 0,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MarkerState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn key(&self, generation: u64) -> String {
        session_marker_key_at(&self.session_id, generation)
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The key that is the marker now, with its generation.
    pub(crate) fn current(&self) -> (u64, String) {
        let generation = self.lock().current;
        (generation, self.key(generation))
    }

    /// One refresh attempt (the heartbeat; never concurrently with itself).
    ///
    /// PUTs the next generation's key; then checks the key it replaces and
    /// every superseded key whose earlier check failed; then deletes the
    /// keys an earlier refresh found present. Nothing is written over an
    /// existing key and nothing is written when a write fails, so a reaped
    /// key stays deleted and a failed read cannot overwrite a live marker.
    pub(crate) fn refresh(
        &self,
        storage: &dyn StorageBackend,
        clock: &dyn Clock,
    ) -> RefreshOutcome {
        let (generation, to_check) = {
            let mut st = self.lock();
            st.attempted += 1;
            let generation = st.attempted;
            st.owned.insert(generation);
            let mut to_check: Vec<u64> = st.unchecked.iter().copied().collect();
            to_check.push(st.current);
            (generation, to_check)
        };
        let entry = SessionEntry {
            last_refresh: clock.now_utc().to_rfc3339(),
            ..self.entry.clone()
        };
        let put = serde_json::to_vec(&entry)
            .map_err(|e| VykarError::Other(format!("session serialize: {e}")))
            .and_then(|data| storage.put(&self.key(generation), &data));
        if let Err(e) = put {
            self.lock().failed_refreshes += 1;
            return RefreshOutcome::Failed(e);
        }
        let written = MarkerStamp::now(clock);
        let checks: Vec<(u64, Result<bool>)> = to_check
            .into_iter()
            .map(|g| (g, storage.exists(&self.key(g))))
            .collect();

        let (outcome, to_delete) = {
            let mut st = self.lock();
            // Before any delete: the probe follows the marker to the new key.
            let previous = st.current;
            st.current = generation;
            st.current_written = written;
            st.unchecked.insert(previous);
            // Found present by an earlier refresh: two generations back now.
            let to_delete: Vec<u64> = std::mem::take(&mut st.retired).into_iter().collect();
            let missing_before = st.missing;
            let mut error: Option<VykarError> = None;
            for (g, result) in checks {
                match result {
                    Ok(present) => st.settle(g, present),
                    Err(e) => {
                        error.get_or_insert(e);
                    }
                }
            }
            let outcome = if st.missing && !missing_before {
                st.last_ok = written;
                RefreshOutcome::Missing
            } else if let Some(e) = error {
                st.failed_refreshes += 1;
                RefreshOutcome::Unchecked(e)
            } else {
                // Every check answered, so every superseded key is settled.
                st.refreshes += 1;
                RefreshOutcome::Refreshed {
                    gap: st.close_gap(),
                }
            };
            (outcome, to_delete)
        };

        for g in to_delete {
            match storage.delete(&self.key(g)) {
                Ok(()) => {
                    self.lock().owned.remove(&g);
                }
                Err(e) => {
                    debug!(session_id = self.session_id, generation = g, error = %e, "failed to delete a superseded session marker; deregistration retries");
                }
            }
        }
        outcome
    }

    /// Check every superseded key whose earlier check failed again (one
    /// found gone records the marker as missing). `Ok(Some(gap))` when that
    /// settled them all with none gone, which closes the open stretch at the
    /// current key; `Ok(None)` when there was nothing to settle or a key was
    /// gone; the first error when a check failed and a key is still
    /// unchecked.
    fn check_unchecked(&self, storage: &dyn StorageBackend) -> Result<Option<std::time::Duration>> {
        let unchecked: Vec<u64> = self.lock().unchecked.iter().copied().collect();
        if unchecked.is_empty() {
            return Ok(None);
        }
        let mut error: Option<VykarError> = None;
        for g in unchecked {
            match storage.exists(&self.key(g)) {
                Ok(present) => self.lock().settle(g, present),
                Err(e) => {
                    error.get_or_insert(e);
                }
            }
        }
        let mut st = self.lock();
        if st.unchecked.is_empty() {
            return Ok((!st.missing).then(|| st.close_gap()));
        }
        match error {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }

    /// The heartbeat's poll between refreshes while a superseded key is
    /// unchecked: check those keys again and write nothing. `Refreshed`
    /// when all are settled present (`gap`: the stretch this closed),
    /// `Missing` when one was gone, `Unchecked` when a check failed again.
    pub(crate) fn recheck(&self, storage: &dyn StorageBackend) -> RefreshOutcome {
        let missing_before = self.lock().missing;
        let result = self.check_unchecked(storage);
        if self.lock().missing && !missing_before {
            return RefreshOutcome::Missing;
        }
        match result {
            Ok(gap) => RefreshOutcome::Refreshed {
                gap: gap.unwrap_or_default(),
            },
            Err(e) => RefreshOutcome::Unchecked(e),
        }
    }

    /// Whether the marker exists now: first every superseded key whose
    /// check failed is checked again (one found gone records the marker as
    /// missing; one whose check fails again fails the probe with that
    /// error, since a reap of it cannot be ruled out), then EXISTS of the
    /// current key; when that finds nothing and a refresh moved the marker
    /// meanwhile, again with the new key (at most 3 rounds, then `Err`).
    pub(crate) fn probe(&self, storage: &dyn StorageBackend) -> Result<bool> {
        self.check_unchecked(storage)?;
        for _ in 0..3 {
            let (generation, key) = self.current();
            if storage.exists(&key)? {
                return Ok(true);
            }
            if self.lock().current == generation {
                return Ok(false);
            }
        }
        Err(VykarError::Other(format!(
            "session {} marker moved during each of three probes",
            self.session_id
        )))
    }

    pub(crate) fn report(&self, clock: &dyn Clock) -> MarkerReport {
        let now = MarkerStamp::now(clock);
        let st = self.lock();
        MarkerReport {
            missing: st.missing,
            longest_gap: st.longest_gap.max(st.last_ok.until(&now)),
            unchecked: st.unchecked.len(),
            refreshes: st.refreshes,
            failed_refreshes: st.failed_refreshes,
        }
    }

    /// Every key that may exist, for deregistration.
    pub(crate) fn owned_keys(&self) -> Vec<String> {
        let owned: Vec<u64> = self.lock().owned.iter().copied().collect();
        owned.into_iter().map(|g| self.key(g)).collect()
    }
}

/// 04 "Why markers carry weight", as far as today's protocol needs it. In
/// order: a marker recorded missing refuses whatever the probe says; a
/// current key found gone refuses (no key is written twice, so that is
/// conclusive); a current key found present commits once every superseded
/// key is checked (a gap over [`MARKER_GAP_LIMIT`] is logged; refusing on
/// it needs the pack ages of 04's `T_pack` rule), and refuses while one is
/// unchecked, since its reap cannot be ruled out; a failed probe commits
/// only if the record is [continuous](MarkerReport::continuous).
pub(crate) fn commit_marker_check(
    session_id: &str,
    probe: Result<bool>,
    report: &MarkerReport,
) -> Result<()> {
    if report.missing {
        warn!(
            session_id,
            "session marker was missing at a refresh; retry the backup"
        );
        return Err(VykarError::StaleChunksDuringCommit);
    }
    match probe {
        Ok(false) => {
            warn!(
                session_id,
                "session marker reaped before commit; retry the backup"
            );
            Err(VykarError::StaleChunksDuringCommit)
        }
        Ok(true) if report.unchecked > 0 => {
            warn!(
                session_id,
                unchecked = report.unchecked,
                "a superseded session marker key could not be checked; refusing to commit"
            );
            Err(VykarError::Other(format!(
                "session {session_id}: {} superseded marker key(s) could not be checked for a reap",
                report.unchecked
            )))
        }
        Ok(true) => {
            if report.longest_gap > MARKER_GAP_LIMIT {
                info!(
                    session_id,
                    gap_secs = report.longest_gap.as_secs(),
                    "session marker went unrefreshed for a while (machine suspended?); it is still present"
                );
            }
            Ok(())
        }
        Err(e) if report.continuous() => {
            warn!(
                session_id,
                error = %e,
                "could not probe the session marker; refreshed without a gap, proceeding"
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                session_id,
                error = %e,
                gap_secs = report.longest_gap.as_secs(),
                unchecked = report.unchecked,
                "could not probe the session marker and its refreshes are not continuous; refusing to commit"
            );
            Err(e)
        }
    }
}

/// How often the heartbeat refreshes the session marker.
pub(crate) const REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15 * 60);
/// The heartbeat's poll on `env.clock`.
pub(crate) const HEARTBEAT_POLL: std::time::Duration = std::time::Duration::from_secs(30);
/// The longest wait before a failed marker PUT is tried again.
pub(crate) const PUT_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(2 * 60);

/// What the heartbeat does at one poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeartbeatAction {
    Wait,
    /// [`SessionMarker::recheck`]: check the unchecked superseded keys,
    /// write nothing.
    Recheck,
    /// [`SessionMarker::refresh`]: write the next generation's key.
    Refresh,
}

/// When the heartbeat acts, counted in polls of [`HEARTBEAT_POLL`], not
/// elapsed time, so a single long jump of the clock (a suspend) counts as
/// one poll.
///
/// A refresh is due [`REFRESH_INTERVAL`] after the last attempt; after
/// `k` failed PUTs in a row, `30 s · 2^(k−1)` after it (the next poll,
/// then 1 min), capped at [`PUT_RETRY_MAX`]: the cap is small against the
/// slack between the refresh interval and the marker staleness (30 min),
/// so a session whose store comes back writes its marker again before a
/// reaper can take it for stale. While a
/// superseded key is unchecked (a presence check failed, the PUT
/// succeeded), every poll before the next refresh rechecks it and writes
/// nothing, so an outage of presence checks adds one key per refresh
/// interval and its checks grow linearly. The gap bookkeeping is the
/// marker's: a stretch stays open until a write is proven.
#[derive(Debug)]
pub(crate) struct HeartbeatSchedule {
    since_attempt: std::time::Duration,
    due: std::time::Duration,
    failed_puts: u32,
    recheck: bool,
}

impl HeartbeatSchedule {
    pub(crate) fn new() -> Self {
        Self {
            since_attempt: std::time::Duration::ZERO,
            due: REFRESH_INTERVAL,
            failed_puts: 0,
            recheck: false,
        }
    }

    /// One poll has passed: what to do now.
    pub(crate) fn poll(&mut self) -> HeartbeatAction {
        self.since_attempt += HEARTBEAT_POLL;
        if self.since_attempt >= self.due {
            HeartbeatAction::Refresh
        } else if self.recheck {
            HeartbeatAction::Recheck
        } else {
            HeartbeatAction::Wait
        }
    }

    /// What `action` (a [`Recheck`](HeartbeatAction::Recheck) or a
    /// [`Refresh`](HeartbeatAction::Refresh)) found.
    pub(crate) fn record(&mut self, action: HeartbeatAction, outcome: &RefreshOutcome) {
        if action == HeartbeatAction::Refresh {
            self.since_attempt = std::time::Duration::ZERO;
            if matches!(outcome, RefreshOutcome::Failed(_)) {
                self.failed_puts = self.failed_puts.saturating_add(1);
                let doublings = (self.failed_puts - 1).min(16);
                self.due = (HEARTBEAT_POLL * (1u32 << doublings)).min(PUT_RETRY_MAX);
                return;
            }
            self.failed_puts = 0;
            self.due = REFRESH_INTERVAL;
        }
        self.recheck = matches!(outcome, RefreshOutcome::Unchecked(_));
    }
}

/// RAII guard that deregisters a session on drop and periodically refreshes
/// the session marker so maintenance doesn't treat it as stale.
pub struct SessionGuard {
    storage: Arc<dyn StorageBackend>,
    marker: Arc<SessionMarker>,
    refresh_handle: Option<JoinHandle<()>>,
    stop: Arc<StopSignal>,
    env: HostEnv,
}

impl SessionGuard {
    /// Adopt an already-registered session. Starts the refresh thread but
    /// does NOT call `register_session()` — the caller must have done that
    /// before opening the repo (mirrors backup's ordering).
    pub fn adopt(
        storage: Arc<dyn StorageBackend>,
        registration: Registration,
        env: HostEnv,
    ) -> Result<Self> {
        let marker = Arc::new(SessionMarker::new(&registration));
        let stop = Arc::new(StopSignal::new());
        let handle = Self::spawn_refresher(
            Arc::clone(&storage),
            Arc::clone(&marker),
            Arc::clone(&stop),
            env.clone(),
        )?;
        Ok(Self {
            storage,
            marker,
            refresh_handle: Some(handle),
            stop,
            env,
        })
    }

    pub fn session_id(&self) -> &str {
        self.marker.session_id()
    }

    /// The session's marker record, for the commit.
    pub(crate) fn marker(&self) -> Arc<SessionMarker> {
        Arc::clone(&self.marker)
    }

    /// The heartbeat polls every 30 s on `env.clock` and acts as
    /// [`HeartbeatSchedule`] says.
    fn spawn_refresher(
        storage: Arc<dyn StorageBackend>,
        marker: Arc<SessionMarker>,
        stop: Arc<StopSignal>,
        env: HostEnv,
    ) -> Result<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("session-refresh".into())
            .spawn(move || {
                let session_id = marker.session_id();
                let mut schedule = HeartbeatSchedule::new();
                loop {
                    if env.clock.wait_timeout(&stop, HEARTBEAT_POLL) {
                        break;
                    }
                    let action = schedule.poll();
                    let outcome = match action {
                        HeartbeatAction::Wait => continue,
                        HeartbeatAction::Recheck => marker.recheck(storage.as_ref()),
                        HeartbeatAction::Refresh => {
                            marker.refresh(storage.as_ref(), env.clock.as_ref())
                        }
                    };
                    match &outcome {
                        RefreshOutcome::Refreshed { gap } => {
                            if *gap > MARKER_GAP_LIMIT {
                                warn!(
                                    session_id,
                                    gap_secs = gap.as_secs(),
                                    "session marker went unrefreshed for a while (machine suspended?)"
                                );
                            } else {
                                debug!(session_id, ?action, "session marker refreshed");
                            }
                        }
                        RefreshOutcome::Missing => warn!(
                            session_id,
                            "session marker was removed by another client while this session ran; \
                             a backup in this session will not commit"
                        ),
                        RefreshOutcome::Unchecked(e) if action == HeartbeatAction::Recheck => {
                            debug!(session_id, error = %e, "superseded session marker key still unchecked");
                        }
                        RefreshOutcome::Unchecked(e) => warn!(
                            session_id,
                            error = %e,
                            "could not check a superseded session marker key; checking again at the next poll"
                        ),
                        RefreshOutcome::Failed(e) => {
                            warn!(session_id, error = %e, "failed to refresh session marker");
                        }
                    }
                    schedule.record(action, &outcome);
                }
            })
            .map_err(|e| VykarError::Other(format!("failed to spawn session refresh thread: {e}")))
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.stop.signal();
        if let Some(handle) = self.refresh_handle.take() {
            let _ = handle.join();
        }
        deregister_keys(
            self.storage.as_ref(),
            self.marker.session_id(),
            &self.marker.owned_keys(),
            &self.env,
        );
    }
}

/// List the IDs of all sessions with a marker, sorted, one per session.
pub fn list_sessions(storage: &dyn StorageBackend) -> Result<Vec<String>> {
    let keys = storage.list(SESSIONS_PREFIX)?;
    Ok(group_session_markers(&keys)
        .into_keys()
        .map(String::from)
        .collect())
}

/// Remove sessions whose marker is older than `max_age`, or left on this
/// machine. Returns the IDs of cleaned sessions.
///
/// A session is judged by its newest listed marker key; when that one is
/// stale, every listed key of the session is deleted, and an older key of a
/// session whose newest key is fresh is kept.
///
/// Ages are on the store's clock: (`now` − the marker key's mtime), `now`
/// being the mtime of an object the caller has just written (the
/// maintenance lock's key). The timestamps inside a marker are used only
/// when the listing reports no mtime; with an mtime but no `now` nothing is
/// stale by age (fail closed). A malformed marker (unparseable JSON or bad
/// timestamps) is judged by its mtime alone: it is preserved, and blocks
/// maintenance, while younger than `max_age`, and is reaped like any stale
/// marker after that; without an mtime it is preserved.
///
/// `.index` journals for cleaned sessions are **preserved** so the next backup
/// can recover uploaded-but-uncommitted chunks via `recover_pending_index()`.
/// Orphaned `.index` files from a *prior* cleanup run (no companion `.json`
/// and not cleaned this invocation) are deleted — they already had their
/// grace period.
///
/// `env` gives the fallback time and enables same-host detection: if
/// `same_host` finds the session was left by a dead process or an earlier
/// boot of this machine, it is treated as stale regardless of age. Sessions
/// from other hosts, other installations and old clients are only cleaned
/// by the `max_age` timeout.
pub fn cleanup_stale_sessions(
    storage: &dyn StorageBackend,
    max_age: Duration,
    env: &HostEnv,
    now: Option<StoreNow>,
) -> Result<Vec<String>> {
    let local_now = env.now_utc();
    let listing = storage.list_meta(SESSIONS_PREFIX)?;
    let mtimes: std::collections::HashMap<&str, Option<std::time::SystemTime>> =
        listing.iter().map(|o| (o.key.as_str(), o.mtime)).collect();
    let keys: Vec<String> = listing.iter().map(|o| o.key.clone()).collect();
    let mut cleaned = Vec::new();
    // Track which session IDs survived the first pass (not deleted).
    let mut surviving_markers: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Track session IDs cleaned in this invocation — their .index files get
    // a one-run grace period so the next backup can recover the journal.
    let mut cleaned_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

    // First pass: the .json session markers, per session.
    for (session_id, markers) in group_session_markers(&keys) {
        let Some(&(_, newest)) = markers.last() else {
            continue;
        };
        let mtime = mtimes.get(newest).copied().flatten();
        let Some(data) = storage.get(newest)? else {
            // Gone since the LIST (a rotation or a deregistration): the
            // session may still be live, so its journal is kept.
            surviving_markers.insert(session_id.to_string());
            continue;
        };
        let entry = serde_json::from_slice::<SessionEntry>(&data).ok();
        let ts = entry.as_ref().and_then(|entry| {
            chrono::DateTime::parse_from_rfc3339(&entry.last_refresh)
                .or_else(|_| chrono::DateTime::parse_from_rfc3339(&entry.registered_at))
                .ok()
                .map(|ts| ts.with_timezone(&Utc))
        });
        let age = EntryAge::of(mtime, now, env, || {
            ts.map(|ts| local_now.signed_duration_since(ts))
        });
        let (Some(entry), Some(_)) = (entry, ts) else {
            // Unparseable .json or a bad timestamp: we cannot read who wrote
            // it. Judged by its mtime alone; while young (or without an
            // mtime) it is preserved so `with_maintenance_lock` reports it as
            // a blocking session (fail-closed), together with its `.index`.
            if matches!(age, EntryAge::Store(_)) && age.exceeds(max_age) {
                debug!(
                    session_id,
                    ?age,
                    "cleaning malformed session marker, stale by its mtime"
                );
                for &(_, key) in &markers {
                    storage.delete(key)?;
                }
                cleaned_ids.insert(session_id.to_string());
                cleaned.push(session_id.to_string());
            } else {
                surviving_markers.insert(session_id.to_string());
            }
            continue;
        };

        let is_stale_by_age = age.exceeds(max_age);
        let verdict = same_host(
            &EntryHost {
                hostname: &entry.hostname,
                pid: entry.pid,
                boot_id: entry.boot_id.as_deref(),
                installation_id: entry.installation_id.as_deref(),
                pid_ns: entry.pid_ns,
                age: age.as_std(),
            },
            env,
        );
        let is_left_local = matches!(verdict, SameHost::DeadProcess | SameHost::EarlierBoot);

        if is_stale_by_age || is_left_local {
            if is_stale_by_age {
                debug!(session_id, ?age, "cleaning stale session");
            } else {
                debug!(
                    session_id,
                    pid = entry.pid,
                    ?verdict,
                    "cleaning session left on this machine"
                );
            }
            for &(_, key) in &markers {
                storage.delete(key)?;
            }
            cleaned_ids.insert(session_id.to_string());
            cleaned.push(session_id.to_string());
        } else {
            surviving_markers.insert(session_id.to_string());
        }
    }

    // Second pass: clean orphaned .index files whose .json marker no longer exists
    // AND that were not cleaned in *this* invocation. Files cleaned this run get
    // a one-run grace period so the next backup can recover their journal via
    // `recover_pending_index()`.
    for key in &keys {
        if let Some(id) = key
            .strip_prefix(SESSIONS_PREFIX)
            .and_then(|s| s.strip_suffix(".index"))
            && !surviving_markers.contains(id)
            && !cleaned_ids.contains(id)
        {
            storage.delete(key)?;
        }
    }

    Ok(cleaned)
}

/// List all sessions with their parsed marker, one per session, in
/// ascending session ID order.
///
/// Returns `(session_id, Option<SessionEntry>)` pairs for the newest listed
/// marker key of each session. The entry is `None` if that marker could not
/// be parsed (malformed JSON) or vanished since the LIST; a session with any
/// listed key is reported.
pub fn list_session_entries(
    storage: &dyn StorageBackend,
) -> Result<Vec<(String, Option<SessionEntry>)>> {
    let keys = storage.list(SESSIONS_PREFIX)?;
    let mut entries = Vec::new();
    for (session_id, markers) in group_session_markers(&keys) {
        let Some(&(_, newest)) = markers.last() else {
            continue;
        };
        let entry = storage
            .get(newest)?
            .and_then(|data| serde_json::from_slice::<SessionEntry>(&data).ok());
        entries.push((session_id.to_string(), entry));
    }
    Ok(entries)
}

/// Delete all files under `sessions/` regardless of parse result.
///
/// Returns the number of files removed. This is a recovery mechanism that
/// cleans `.json` markers, `.index` journals, and any other stray files.
pub fn clear_all_sessions(storage: &dyn StorageBackend) -> Result<usize> {
    let keys = storage.list(SESSIONS_PREFIX)?;
    let mut removed = 0usize;
    for key in &keys {
        if key.starts_with(SESSIONS_PREFIX) {
            storage.delete(key)?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// Exponential backoff (cap 2^5 × base) with ±25 % jitter, minimum 100 ms.
fn backoff_delay(attempt: usize, base_delay_ms: u64) -> u64 {
    let delay = base_delay_ms * (1 << attempt.min(5));
    let jitter = (rand::random::<u64>() % (delay / 2).max(1)).wrapping_sub(delay / 4);
    delay.wrapping_add(jitter).max(100)
}

/// Compact holder description for log fields — `Display for LockHolder` is
/// the user-facing sentence, too verbose for a log field.
fn describe_holder(holder: &LockHolder) -> String {
    holder.details.as_ref().map_or_else(
        || holder.key.clone(),
        |d| format!("{} (PID {}, held {})", d.hostname, d.pid, d.age),
    )
}

/// Run `op` until it returns something other than `Locked`, with exponential
/// backoff + jitter between attempts. Returns the last `Locked` when attempts
/// run out. Blocks the calling thread on `clock`.
fn retry_while_locked<T>(
    max_attempts: usize,
    base_delay_ms: u64,
    clock: &dyn Clock,
    mut op: impl FnMut() -> Result<T>,
) -> Result<T> {
    for attempt in 0..max_attempts {
        match op() {
            Ok(v) => return Ok(v),
            Err(VykarError::Locked(holder)) => {
                if attempt + 1 < max_attempts {
                    let delay = backoff_delay(attempt, base_delay_ms);
                    debug!(
                        attempt = attempt + 1,
                        max_attempts,
                        holder = %describe_holder(&holder),
                        delay_ms = delay,
                        "lock contention, retrying"
                    );
                    clock.sleep(std::time::Duration::from_millis(delay));
                } else {
                    return Err(VykarError::Locked(holder));
                }
            }
            Err(e) => return Err(e),
        }
    }
    // Loop only falls through when `max_attempts == 0`; every other path
    // returns inside the match.
    Err(VykarError::Other(
        "retry_while_locked called with max_attempts == 0".to_string(),
    ))
}

/// How long a backup commit waits for the repository lock.
///
/// While our session marker exists, maintenance cannot hold the lock for long
/// (`with_maintenance_lock` refuses on active sessions), so legitimate holders
/// are other backups' commits, which are short because packs are uploaded
/// before the lock is taken. A crashed holder's lock is only reaped after 6 h,
/// so waiting longer than this would mostly be waiting on a dead process.
pub const COMMIT_LOCK_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// Longest single sleep between `shutdown` checks.
const LOCK_WAIT_SLICE: std::time::Duration = std::time::Duration::from_millis(500);

/// Acquire a repo lock, retrying with backoff until `max_wait` has elapsed.
///
/// `shutdown` is checked before every attempt (including the first) and
/// during sleeps; when set, returns `Interrupted`. After the deadline one
/// final attempt is always made, so this never gives up early.
///
/// `on_contended` is called once, on the first `Locked` result. The holder it
/// receives is a hint. `acquire_lock` does not write a key while one is
/// present, so it is usually the real holder, but a contender that PUT in the
/// same window, or an old client's transient key, can still be reported.
///
/// Caveats:
/// - The deadline bounds waiting, not storage calls: a slow `acquire_lock`
///   round-trip (stale-lock sweep + PUT + LIST + DELETE) can overrun it.
/// - No fairness: waiters do not queue and a later arrival can win. Two
///   contenders that PUT in the same window both back off. With several
///   commits queued the deadline is a soft bound.
///
/// Blocks the calling thread on `env.clock`. Callers running inside an async
/// runtime must invoke this via `tokio::task::spawn_blocking`.
pub fn acquire_lock_with_deadline(
    storage: &dyn StorageBackend,
    env: &HostEnv,
    max_wait: std::time::Duration,
    base_delay_ms: u64,
    shutdown: &CancelToken,
    mut on_contended: impl FnMut(&LockHolder),
) -> Result<LockGuard> {
    let start = env.clock.monotonic();
    let deadline = start + max_wait;
    let mut attempt: usize = 0;
    let mut store_clock = StoreClock::default();
    loop {
        if shutdown.is_cancelled() {
            return Err(VykarError::Interrupted);
        }
        let holder = match acquire_lock_reading(storage, env, &mut store_clock) {
            Ok(guard) => return Ok(guard),
            Err(VykarError::Locked(holder)) => holder,
            Err(e) => return Err(e),
        };
        let now = env.clock.monotonic();
        if now >= deadline {
            return Err(VykarError::Locked(holder));
        }
        let delay = std::time::Duration::from_millis(backoff_delay(attempt, base_delay_ms))
            .min(deadline - now);
        if attempt == 0 {
            info!(
                oldest_key_from = %describe_holder(&holder),
                max_wait_secs = max_wait.as_secs(),
                "lock contended; waiting"
            );
            on_contended(&holder);
        } else {
            debug!(
                attempt = attempt + 1,
                oldest_key_from = %describe_holder(&holder),
                delay_ms = delay.as_millis() as u64,
                waited_ms = now.duration_since(start).as_millis() as u64,
                "lock still contended, retrying"
            );
        }
        let wake = now + delay;
        loop {
            let now = env.clock.monotonic();
            if now >= wake {
                break;
            }
            if shutdown.is_cancelled() {
                return Err(VykarError::Interrupted);
            }
            env.clock.sleep((wake - now).min(LOCK_WAIT_SLICE));
        }
        attempt += 1;
    }
}

/// Default stale session threshold.
pub fn default_stale_session_duration() -> Duration {
    Duration::seconds(SESSION_STALE_SECS)
}

/// Format the age between `now` and an RFC3339 `timestamp` as a compact
/// human-readable string (e.g. `"2h"`, `"1d 3h"`, `"42m"`, `"12s"`).
///
/// Returns `"unknown"` if the timestamp fails to parse.
pub fn format_age(now: &chrono::DateTime<Utc>, timestamp: &str) -> String {
    let Ok(ts) = chrono::DateTime::parse_from_rfc3339(timestamp) else {
        return "unknown".to_string();
    };
    format_age_since(now, &ts.with_timezone(&Utc))
}

/// Same as [`format_age`] but for an already-parsed timestamp.
///
/// Sub-minute ages render in seconds: for lock contention the difference
/// between a few-second commit and a minutes-long compact is the whole signal,
/// and flooring both to `"0m"` would erase it.
pub fn format_age_since(now: &chrono::DateTime<Utc>, then: &chrono::DateTime<Utc>) -> String {
    let dur = now.signed_duration_since(*then);
    let hours = dur.num_hours();
    if hours >= 24 {
        format!("{}d {}h", hours / 24, hours % 24)
    } else if hours > 0 {
        format!("{hours}h")
    } else {
        let mins = dur.num_minutes();
        if mins > 0 {
            format!("{mins}m")
        } else {
            format!("{}s", dur.num_seconds().max(0))
        }
    }
}
