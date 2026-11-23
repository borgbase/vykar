use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread::JoinHandle;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::control::CancelToken;
use super::host_env::{Clock, HostEnv, StopSignal};
use vykar_storage::StorageBackend;
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
}

impl LockGuard {
    pub fn key(&self) -> &str {
        &self.key
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
    // Don't publish a key while a holder exists: waiters would otherwise
    // collide with each other on every retry.
    let held = cleanup_stale_locks(storage, Duration::seconds(DEFAULT_STALE_LOCK_SECS), env)?;
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

    let mut keys = match list_lock_keys(storage) {
        Ok(keys) => keys,
        Err(e) => {
            // Our key may be the only one: never leave it behind.
            let _ = storage.delete(&key);
            return Err(e);
        }
    };
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
/// Returns the number of locks removed.
pub fn break_lock(storage: &dyn StorageBackend) -> Result<usize> {
    let mut removed: usize = 0;

    for key in list_lock_keys(storage)? {
        storage.delete(&key)?;
        removed += 1;
    }

    Ok(removed)
}

fn list_lock_keys(storage: &dyn StorageBackend) -> Result<Vec<String>> {
    let mut keys = storage.list(LOCKS_PREFIX)?;
    keys.retain(|k| k.starts_with(LOCKS_PREFIX) && k.ends_with(".json"));
    Ok(keys)
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
fn probe_unlocked(storage: &dyn StorageBackend, env: &HostEnv) -> Result<()> {
    let held = cleanup_stale_locks(storage, Duration::seconds(DEFAULT_STALE_LOCK_SECS), env)?;
    match held.first() {
        Some(key) => Err(VykarError::Locked(build_lock_holder(
            storage,
            key,
            env.clock.as_ref(),
        ))),
        None => Ok(()),
    }
}

/// Remove stale locks and return the keys that remain.
///
/// A lock is stale when it is older than `max_age`, or when [`same_host`]
/// finds it was left by a dead process or an earlier boot of this machine.
pub(crate) fn cleanup_stale_locks(
    storage: &dyn StorageBackend,
    max_age: Duration,
    env: &HostEnv,
) -> Result<Vec<String>> {
    let now = env.now_utc();
    let mut surviving = Vec::new();
    for key in list_lock_keys(storage)? {
        let Some(data) = storage.get(&key)? else {
            continue;
        };
        let entry = serde_json::from_slice::<LockEntry>(&data).ok();
        if should_cleanup_lock(&key, entry.as_ref(), now, max_age, env) {
            storage.delete(&key)?;
        } else {
            surviving.push(key);
        }
    }
    surviving.sort();
    Ok(surviving)
}

fn should_cleanup_lock(
    key: &str,
    entry: Option<&LockEntry>,
    now: DateTime<Utc>,
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
                written_at: lock_timestamp(Some(entry), key),
            },
            now,
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

    lock_timestamp(entry, key).is_some_and(|acquired| now.signed_duration_since(acquired) > max_age)
}

/// Who wrote a lock or session entry, as the same-host rule reads it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EntryHost<'a> {
    pub hostname: &'a str,
    pub pid: u32,
    pub boot_id: Option<&'a str>,
    pub installation_id: Option<&'a str>,
    pub pid_ns: Option<u64>,
    /// When the entry was last written, on its writer's clock.
    pub written_at: Option<DateTime<Utc>>,
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

/// The same-host verdict on `entry` for `env`, at `now` on `env`'s clock.
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
pub(crate) fn same_host(entry: &EntryHost<'_>, now: DateTime<Utc>, env: &HostEnv) -> SameHost {
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
                .written_at
                .and_then(|w| now.signed_duration_since(w).to_std().ok())
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

pub(crate) const SESSIONS_PREFIX: &str = "sessions/";
/// Sessions whose `last_refresh` is strictly older than this threshold are
/// treated as stale. Shared between maintenance cleanup
/// ([`cleanup_stale_sessions`]) and pending-index recovery.
///
/// Set to 3× the 15-minute session heartbeat interval — two missed refreshes
/// still keep a session live, a third marks it dead.
pub(crate) const SESSION_STALE_SECS: i64 = 45 * 60;

/// Storage key for a session's JSON marker: `sessions/<id>.json`.
pub(crate) fn session_marker_key(session_id: &str) -> String {
    format!("{SESSIONS_PREFIX}{session_id}.json")
}

/// Storage key for a session's pending index journal: `sessions/<id>.index`.
pub(crate) fn session_index_key(session_id: &str) -> String {
    format!("{SESSIONS_PREFIX}{session_id}.index")
}

/// Extract the session ID from a `.json` marker key, if it matches.
fn parse_session_id(key: &str) -> Option<&str> {
    key.strip_prefix(SESSIONS_PREFIX)
        .and_then(|s| s.strip_suffix(".json"))
}

/// A session marker stored at `sessions/<session_id>.json`.
#[derive(Debug, Serialize, Deserialize)]
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
) -> Result<()> {
    register_session_inner(storage, session_id, 3, 2000, env)
}

pub(crate) fn register_session_inner(
    storage: &dyn StorageBackend,
    session_id: &str,
    max_attempts: usize,
    base_delay_ms: u64,
    env: &HostEnv,
) -> Result<()> {
    let key = session_marker_key(session_id);
    let now = env.now_utc();
    let entry = SessionEntry {
        hostname: env.host.hostname.clone(),
        pid: env.host.pid,
        registered_at: now.to_rfc3339(),
        last_refresh: now.to_rfc3339(),
        boot_id: env.host.boot_id.clone(),
        installation_id: env.host.installation_id.clone(),
        pid_ns: env.host.pid_ns,
    };
    let data = serde_json::to_vec(&entry)
        .map_err(|e| VykarError::Other(format!("session serialize: {e}")))?;
    if let Err(e) = storage.put(&key, &data) {
        // The PUT may have committed although it failed (a lost reply, or a
        // retry wait cut by a cancel): do not leave the marker behind.
        let _ = storage.delete(&key);
        return Err(e);
    }

    match retry_while_locked(max_attempts, base_delay_ms, env.clock.as_ref(), || {
        probe_unlocked(storage, env)
    }) {
        Ok(()) => {
            debug!(session_id, "session registered, no active lock");
            Ok(())
        }
        Err(e) => {
            // Lock held or unexpected error — clean up and propagate.
            let _ = storage.delete(&key);
            Err(e)
        }
    }
}

/// Deregister a backup session. Best-effort: retries twice on failure.
///
/// Blocks the calling thread on `env.clock` between retries. Callers running
/// inside an async runtime must invoke this via `tokio::task::spawn_blocking`
/// (see `commands/mount.rs` for the established pattern).
pub fn deregister_session(storage: &dyn StorageBackend, session_id: &str, env: &HostEnv) {
    let key = session_marker_key(session_id);
    for attempt in 0..3 {
        match storage.delete(&key) {
            Ok(()) => {
                debug!(session_id, "session deregistered");
                return;
            }
            Err(e) => {
                if attempt < 2 {
                    debug!(session_id, error = %e, "retrying session deregister");
                    env.clock.sleep(std::time::Duration::from_millis(500));
                } else {
                    warn!(session_id, error = %e, "failed to deregister session after retries");
                }
            }
        }
    }
}

/// RAII guard that deregisters a session on drop and periodically refreshes
/// the session marker so maintenance doesn't treat it as stale.
pub struct SessionGuard {
    storage: Arc<dyn StorageBackend>,
    session_id: String,
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
        session_id: String,
        env: HostEnv,
    ) -> Result<Self> {
        let stop = Arc::new(StopSignal::new());
        let handle = Self::spawn_refresher(
            Arc::clone(&storage),
            session_id.clone(),
            Arc::clone(&stop),
            env.clone(),
        )?;
        Ok(Self {
            storage,
            session_id,
            refresh_handle: Some(handle),
            stop,
            env,
        })
    }

    /// The heartbeat counts polls, not elapsed time: it refreshes after 30
    /// polls of 30 s on `env.clock`, so a single long jump of the clock (a
    /// suspend) counts as one poll.
    fn spawn_refresher(
        storage: Arc<dyn StorageBackend>,
        session_id: String,
        stop: Arc<StopSignal>,
        env: HostEnv,
    ) -> Result<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("session-refresh".into())
            .spawn(move || {
                const REFRESH_INTERVAL: std::time::Duration =
                    std::time::Duration::from_secs(15 * 60);
                const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
                let mut elapsed = std::time::Duration::ZERO;
                loop {
                    if env.clock.wait_timeout(&stop, POLL_INTERVAL) {
                        break;
                    }
                    elapsed += POLL_INTERVAL;
                    if elapsed >= REFRESH_INTERVAL {
                        refresh_session(storage.as_ref(), &session_id, env.clock.as_ref());
                        elapsed = std::time::Duration::ZERO;
                    }
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
        deregister_session(self.storage.as_ref(), &self.session_id, &self.env);
    }
}

/// Refresh a session marker's `last_refresh` timestamp. Best-effort.
///
/// Deliberately non-resurrecting: if the marker is missing (successfully
/// read as `None`) we do **not** recreate it. Missing markers indicate the
/// session has already been deregistered — either cleanly by
/// [`deregister_session`] or by maintenance that reaped us as stale — and
/// resurrecting the key would race with that deletion and could confuse
/// maintenance into seeing a "live" session that no one owns.
///
/// On storage read errors we also bail out without writing, so an
/// inconclusive `Err` read can't overwrite a live marker's fields with
/// defaults.
pub fn refresh_session(storage: &dyn StorageBackend, session_id: &str, clock: &dyn Clock) {
    let key = session_marker_key(session_id);
    let now = clock.now_utc();

    let mut entry: SessionEntry = match storage.get(&key) {
        Ok(Some(data)) => match serde_json::from_slice::<SessionEntry>(&data) {
            Ok(e) => e,
            Err(e) => {
                warn!(session_id, error = %e, "session marker parse failed; skipping refresh");
                return;
            }
        },
        Ok(None) => {
            debug!(session_id, "session marker missing; not resurrecting");
            return;
        }
        Err(e) => {
            warn!(session_id, error = %e, "storage read failed during refresh; skipping");
            return;
        }
    };
    entry.last_refresh = now.to_rfc3339();

    let data = match serde_json::to_vec(&entry) {
        Ok(d) => d,
        Err(e) => {
            warn!(session_id, error = %e, "failed to serialize session refresh");
            return;
        }
    };
    if let Err(e) = storage.put(&key, &data) {
        warn!(session_id, error = %e, "failed to refresh session marker");
    } else {
        debug!(session_id, "session marker refreshed");
    }
}

/// List all active session IDs (without the `sessions/` prefix and `.json` suffix).
pub fn list_sessions(storage: &dyn StorageBackend) -> Result<Vec<String>> {
    let keys = storage.list(SESSIONS_PREFIX)?;
    Ok(keys
        .iter()
        .filter_map(|k| parse_session_id(k).map(String::from))
        .collect())
}

/// Remove session markers older than `max_age`, or left on this machine.
/// Returns the IDs of cleaned sessions.
///
/// `.index` journals for cleaned sessions are **preserved** so the next backup
/// can recover uploaded-but-uncommitted chunks via `recover_pending_index()`.
/// Orphaned `.index` files from a *prior* cleanup run (no companion `.json`
/// and not cleaned this invocation) are deleted — they already had their
/// grace period.
///
/// `env` gives the time and enables same-host detection: if `same_host`
/// finds the session was left by a dead process or an earlier boot of this
/// machine, it is treated as stale regardless of age. Sessions from other
/// hosts, other installations and old clients are only cleaned by the
/// `max_age` timeout.
pub fn cleanup_stale_sessions(
    storage: &dyn StorageBackend,
    max_age: Duration,
    env: &HostEnv,
) -> Result<Vec<String>> {
    let now = env.now_utc();
    let keys = storage.list(SESSIONS_PREFIX)?;
    let mut cleaned = Vec::new();
    // Track which session IDs survived the first pass (not deleted).
    let mut surviving_markers: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Track session IDs cleaned in this invocation — their .index files get
    // a one-run grace period so the next backup can recover the journal.
    let mut cleaned_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

    // First pass: process only .json session markers.
    for key in &keys {
        let Some(session_id) = parse_session_id(key) else {
            continue; // skip .index files — handled below
        };
        let Some(data) = storage.get(key)? else {
            continue;
        };
        let Ok(entry) = serde_json::from_slice::<SessionEntry>(&data) else {
            // Unparseable .json — we cannot prove this is stale. Preserve it
            // so `with_maintenance_lock` reports it as a blocking session
            // (fail-closed). Operator must use `break-lock --sessions` to
            // force-clear. Also preserve the companion `.index` so its
            // journal isn't lost.
            surviving_markers.insert(session_id.to_string());
            continue;
        };
        let ts = chrono::DateTime::parse_from_rfc3339(&entry.last_refresh)
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(&entry.registered_at));
        let Ok(ts) = ts else {
            // Bad timestamp — same treatment: fail-closed, let maintenance
            // surface it and require operator intervention.
            surviving_markers.insert(session_id.to_string());
            continue;
        };

        let ts = ts.with_timezone(&Utc);
        let is_stale_by_age = now.signed_duration_since(ts) > max_age;
        let verdict = same_host(
            &EntryHost {
                hostname: &entry.hostname,
                pid: entry.pid,
                boot_id: entry.boot_id.as_deref(),
                installation_id: entry.installation_id.as_deref(),
                pid_ns: entry.pid_ns,
                written_at: Some(ts),
            },
            now,
            env,
        );
        let is_left_local = matches!(verdict, SameHost::DeadProcess | SameHost::EarlierBoot);

        if is_stale_by_age {
            debug!(session_id, age_hours = %((now - ts).num_hours()), "cleaning stale session");
            storage.delete(key)?;
            cleaned_ids.insert(session_id.to_string());
            cleaned.push(session_id.to_string());
        } else if is_left_local {
            debug!(
                session_id,
                pid = entry.pid,
                ?verdict,
                "cleaning session left on this machine"
            );
            storage.delete(key)?;
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

/// List all session entries with their parsed content.
///
/// Returns `(session_id, Option<SessionEntry>)` pairs. The entry is `None`
/// if the marker file could not be parsed (malformed JSON).
pub fn list_session_entries(
    storage: &dyn StorageBackend,
) -> Result<Vec<(String, Option<SessionEntry>)>> {
    let keys = storage.list(SESSIONS_PREFIX)?;
    let mut entries = Vec::new();
    for key in &keys {
        let Some(session_id) = parse_session_id(key) else {
            continue;
        };
        let entry = storage
            .get(key)?
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
    loop {
        if shutdown.is_cancelled() {
            return Err(VykarError::Interrupted);
        }
        let holder = match acquire_lock(storage, env) {
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
