use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::SystemTime;

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};

/// A simple advisory lock stored in `locks/<uuid>.json`.
#[derive(Debug, Serialize, Deserialize)]
struct LockEntry {
    hostname: String,
    pid: u32,
    time: String,
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
    acquired_at: SystemTime,
}

impl LockGuard {
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Acquire an advisory lock on the repository.
pub fn acquire_lock(storage: &dyn StorageBackend) -> Result<LockGuard> {
    let hostname = crate::platform::hostname();
    let pid = std::process::id();

    cleanup_stale_locks(storage, Duration::seconds(DEFAULT_STALE_LOCK_SECS))?;

    let now = Utc::now();
    let entry = LockEntry {
        hostname,
        pid,
        time: now.to_rfc3339(),
    };

    let uuid = format!("{:032x}", rand::random::<u128>());
    // Timestamp prefix keeps older lock keys sorted first.
    let ts = now.timestamp_micros();
    let key = format!("{LOCKS_PREFIX}{ts:020}-{uuid}.json");
    let data = serde_json::to_vec(&entry)
        .map_err(|e| vykar_types::error::VykarError::Other(format!("lock serialize: {e}")))?;

    storage.put(&key, &data)?;

    // Determine lock winner deterministically: oldest key wins.
    let mut keys = list_lock_keys(storage)?;
    keys.sort();
    if keys.first() != Some(&key) {
        // Best-effort cleanup of the lock we just wrote.
        let _ = storage.delete(&key);
        let holder = keys
            .first()
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        return Err(VykarError::Locked(holder));
    }

    Ok(LockGuard {
        key,
        acquired_at: SystemTime::now(),
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

fn cleanup_stale_locks(storage: &dyn StorageBackend, max_age: Duration) -> Result<()> {
    let now = Utc::now();
    for key in list_lock_keys(storage)? {
        let Some(data) = storage.get(&key)? else {
            continue;
        };
        let Ok(entry) = serde_json::from_slice::<LockEntry>(&data) else {
            continue;
        };
        let Ok(acquired) = chrono::DateTime::parse_from_rfc3339(&entry.time) else {
            continue;
        };
        if now.signed_duration_since(acquired.with_timezone(&Utc)) > max_age {
            let _ = storage.delete(&key);
        }
    }
    Ok(())
}

/// Build a lock fence closure that verifies the lock is still valid.
///
/// The returned closure checks:
/// 1. The lock file still exists on storage (not cleaned up by another client).
/// 2. Time since last refresh has not exceeded `LOCK_MAX_UNREFRESHED_SECS`.
/// 3. If the refresh interval has elapsed, rewrites the lock file with a fresh timestamp.
pub fn build_lock_fence(
    guard: &LockGuard,
    storage: Arc<dyn StorageBackend>,
) -> Arc<dyn Fn() -> Result<()> + Send + Sync> {
    let acquired_secs = guard
        .acquired_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    build_lock_fence_inner(guard, storage, acquired_secs)
}

/// Same as [`build_lock_fence`] but with an overridden initial epoch for testing.
#[cfg(test)]
pub fn build_lock_fence_with_epoch(
    guard: &LockGuard,
    storage: Arc<dyn StorageBackend>,
    override_epoch_secs: i64,
) -> Arc<dyn Fn() -> Result<()> + Send + Sync> {
    build_lock_fence_inner(guard, storage, override_epoch_secs)
}

fn build_lock_fence_inner(
    guard: &LockGuard,
    storage: Arc<dyn StorageBackend>,
    initial_epoch_secs: i64,
) -> Arc<dyn Fn() -> Result<()> + Send + Sync> {
    let lock_key = guard.key.clone();
    let hostname = crate::platform::hostname();
    let pid = std::process::id();

    let last_refreshed = Arc::new(AtomicI64::new(initial_epoch_secs));

    Arc::new(move || {
        verify_lock_validity(&lock_key, last_refreshed.load(Ordering::SeqCst), &*storage)?;

        // Refresh lock file if refresh interval has elapsed.
        let now_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(i64::MAX);
        let elapsed = now_secs - last_refreshed.load(Ordering::SeqCst);
        if elapsed >= LOCK_REFRESH_INTERVAL_SECS as i64 {
            let entry = LockEntry {
                hostname: hostname.clone(),
                pid,
                time: Utc::now().to_rfc3339(),
            };
            if let Ok(data) = serde_json::to_vec(&entry) {
                if storage.put(&lock_key, &data).is_ok() {
                    last_refreshed.store(now_secs, Ordering::SeqCst);
                    debug!(lock_key = %lock_key, "lock file refreshed");
                }
            }
        }

        Ok(())
    })
}

/// Verify that a lock is still valid by checking existence and time elapsed.
///
/// This is the core validation logic, exposed for unit testing.
pub fn verify_lock_validity(
    lock_key: &str,
    last_refreshed_secs: i64,
    storage: &dyn StorageBackend,
) -> Result<()> {
    // 1. Check lock file still exists.
    if !storage.exists(lock_key)? {
        return Err(VykarError::LockExpired(
            "lock file removed by another client".into(),
        ));
    }

    // 2. Check time since last refresh.
    let now_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(i64::MAX);

    let elapsed = now_secs - last_refreshed_secs;

    // Fail closed on clock anomaly (negative elapsed or clock failure).
    if elapsed < 0 || now_secs == i64::MAX {
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
/// Sessions older than this are considered stale and can be reaped.
const DEFAULT_STALE_SESSION_SECS: i64 = 72 * 60 * 60; // 72 hours

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
}

/// Register a backup session. Writes a session marker and probes the lock
/// to ensure no maintenance operation is in progress.
///
/// If a lock is held (maintenance or another commit), retries briefly
/// (another backup's commit phase is typically seconds). If still locked
/// after retries, deletes the marker and returns `Err(Locked)`.
pub fn register_session(storage: &dyn StorageBackend, session_id: &str) -> Result<()> {
    let key = session_marker_key(session_id);
    let now = Utc::now();
    let entry = SessionEntry {
        hostname: crate::platform::hostname(),
        pid: std::process::id(),
        registered_at: now.to_rfc3339(),
        last_refresh: now.to_rfc3339(),
    };
    let data = serde_json::to_vec(&entry)
        .map_err(|e| VykarError::Other(format!("session serialize: {e}")))?;
    storage.put(&key, &data)?;

    // Probe: make sure no maintenance lock is held.
    // Uses the same jittered backoff as commit-phase lock acquisition.
    match acquire_lock_with_retry(storage, 3, 2000) {
        Ok(guard) => {
            // No lock held — release our probe immediately and proceed.
            if let Err(e) = release_lock(storage, guard) {
                warn!(session_id, error = %e, "failed to release probe lock, aborting registration");
                let _ = storage.delete(&key);
                return Err(e);
            }
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
pub fn deregister_session(storage: &dyn StorageBackend, session_id: &str) {
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
                    std::thread::sleep(std::time::Duration::from_millis(500));
                } else {
                    warn!(session_id, error = %e, "failed to deregister session after retries");
                }
            }
        }
    }
}

/// Interruptible stop signal for the refresh thread.
struct StopSignal {
    mutex: Mutex<bool>,
    condvar: Condvar,
}

impl StopSignal {
    fn new() -> Self {
        Self {
            mutex: Mutex::new(false),
            condvar: Condvar::new(),
        }
    }

    /// Block up to `timeout`. Returns true immediately if signalled.
    fn wait_timeout(&self, timeout: std::time::Duration) -> bool {
        let guard = self.mutex.lock().unwrap();
        let (guard, _) = self
            .condvar
            .wait_timeout_while(guard, timeout, |stopped| !*stopped)
            .unwrap();
        *guard
    }

    /// Wake the thread immediately.
    fn signal(&self) {
        *self.mutex.lock().unwrap() = true;
        self.condvar.notify_all();
    }
}

/// RAII guard that deregisters a session on drop and periodically refreshes
/// the session marker so maintenance doesn't treat it as stale.
pub struct SessionGuard {
    storage: Arc<dyn StorageBackend>,
    session_id: String,
    refresh_handle: Option<JoinHandle<()>>,
    stop: Arc<StopSignal>,
}

impl SessionGuard {
    /// Adopt an already-registered session. Starts the refresh thread but
    /// does NOT call `register_session()` — the caller must have done that
    /// before opening the repo (mirrors backup's ordering).
    pub fn adopt(storage: Arc<dyn StorageBackend>, session_id: String) -> Result<Self> {
        let stop = Arc::new(StopSignal::new());
        let handle =
            Self::spawn_refresher(Arc::clone(&storage), session_id.clone(), Arc::clone(&stop))?;
        Ok(Self {
            storage,
            session_id,
            refresh_handle: Some(handle),
            stop,
        })
    }

    fn spawn_refresher(
        storage: Arc<dyn StorageBackend>,
        session_id: String,
        stop: Arc<StopSignal>,
    ) -> Result<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("session-refresh".into())
            .spawn(move || {
                const REFRESH_INTERVAL: std::time::Duration =
                    std::time::Duration::from_secs(15 * 60);
                const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
                let mut elapsed = std::time::Duration::ZERO;
                loop {
                    if stop.wait_timeout(POLL_INTERVAL) {
                        break;
                    }
                    elapsed += POLL_INTERVAL;
                    if elapsed >= REFRESH_INTERVAL {
                        refresh_session(storage.as_ref(), &session_id);
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
        deregister_session(self.storage.as_ref(), &self.session_id);
    }
}

/// Refresh a session marker's `last_refresh` timestamp. Best-effort.
pub fn refresh_session(storage: &dyn StorageBackend, session_id: &str) {
    let key = session_marker_key(session_id);
    let now = Utc::now();

    // Read existing entry to preserve registered_at, or create a fresh one.
    let entry = match storage.get(&key) {
        Ok(Some(data)) => {
            let mut e: SessionEntry =
                serde_json::from_slice(&data).unwrap_or_else(|_| SessionEntry {
                    hostname: crate::platform::hostname(),
                    pid: std::process::id(),
                    registered_at: now.to_rfc3339(),
                    last_refresh: now.to_rfc3339(),
                });
            e.last_refresh = now.to_rfc3339();
            e
        }
        _ => SessionEntry {
            hostname: crate::platform::hostname(),
            pid: std::process::id(),
            registered_at: now.to_rfc3339(),
            last_refresh: now.to_rfc3339(),
        },
    };

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

/// Remove session markers older than `max_age`, or from dead local processes.
/// Returns the IDs of cleaned sessions.
///
/// `.index` journals for cleaned sessions are **preserved** so the next backup
/// can recover uploaded-but-uncommitted chunks via `recover_pending_index()`.
/// Orphaned `.index` files from a *prior* cleanup run (no companion `.json`
/// and not cleaned this invocation) are deleted — they already had their
/// grace period.
///
/// `local_hostname` and `pid_alive_fn` enable same-host dead-process detection:
/// if a session's hostname matches and the PID is no longer alive, the session
/// is treated as stale regardless of age. Sessions from different hosts are
/// only cleaned by the `max_age` timeout.
pub fn cleanup_stale_sessions(
    storage: &dyn StorageBackend,
    max_age: Duration,
    local_hostname: &str,
    pid_alive_fn: impl Fn(u32) -> bool,
) -> Result<Vec<String>> {
    let now = Utc::now();
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
            // Unparseable .json — treat as stale. Keep .index for recovery.
            let _ = storage.delete(key);
            cleaned_ids.insert(session_id.to_string());
            continue;
        };
        let ts = chrono::DateTime::parse_from_rfc3339(&entry.last_refresh)
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(&entry.registered_at));
        let Ok(ts) = ts else {
            // Bad timestamp — treat as stale. Keep .index for recovery.
            let _ = storage.delete(key);
            cleaned_ids.insert(session_id.to_string());
            continue;
        };

        let is_stale_by_age = now.signed_duration_since(ts.with_timezone(&Utc)) > max_age;
        let is_dead_local = entry.hostname == local_hostname && !pid_alive_fn(entry.pid);

        if is_stale_by_age {
            debug!(session_id, age_hours = %((now - ts.with_timezone(&Utc)).num_hours()), "cleaning stale session");
            let _ = storage.delete(key);
            cleaned_ids.insert(session_id.to_string());
            cleaned.push(session_id.to_string());
        } else if is_dead_local {
            debug!(
                session_id,
                pid = entry.pid,
                "cleaning session from dead local process"
            );
            let _ = storage.delete(key);
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
        {
            if !surviving_markers.contains(id) && !cleaned_ids.contains(id) {
                let _ = storage.delete(key);
            }
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

/// Acquire a repo lock with retry and exponential backoff + jitter.
/// Returns the lock guard on success, or the last error on failure.
pub fn acquire_lock_with_retry(
    storage: &dyn StorageBackend,
    max_attempts: usize,
    base_delay_ms: u64,
) -> Result<LockGuard> {
    for attempt in 0..max_attempts {
        match acquire_lock(storage) {
            Ok(guard) => return Ok(guard),
            Err(VykarError::Locked(holder)) => {
                if attempt + 1 < max_attempts {
                    let delay = base_delay_ms * (1 << attempt.min(5));
                    // Add jitter: ±25%
                    let jitter = (rand::random::<u64>() % (delay / 2)).wrapping_sub(delay / 4);
                    let delay = delay.wrapping_add(jitter).max(100);
                    debug!(
                        attempt = attempt + 1,
                        max_attempts,
                        holder = %holder,
                        delay_ms = delay,
                        "lock contention, retrying"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(delay));
                } else {
                    return Err(VykarError::Locked(holder));
                }
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

/// Default stale session threshold.
pub fn default_stale_session_duration() -> Duration {
    Duration::seconds(DEFAULT_STALE_SESSION_SECS)
}
