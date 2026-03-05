use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use vykar_storage::{BackendLockInfo, StorageBackend};
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
const BACKEND_LOCK_ID: &str = "repo-lock";

#[derive(Debug)]
enum LockGuardKind {
    Object { key: String },
    Backend { lock_id: String },
}

/// Handle to an acquired lock.
#[derive(Debug)]
pub struct LockGuard {
    kind: LockGuardKind,
}

impl LockGuard {
    pub fn key(&self) -> &str {
        match &self.kind {
            LockGuardKind::Object { key } => key,
            LockGuardKind::Backend { lock_id } => lock_id,
        }
    }
}

/// Acquire an advisory lock on the repository.
pub fn acquire_lock(storage: &dyn StorageBackend) -> Result<LockGuard> {
    let hostname = crate::platform::hostname();
    let pid = std::process::id() as u64;

    // Prefer backend-native lock APIs when available (e.g. REST server locks).
    let backend_info = BackendLockInfo {
        hostname: hostname.clone(),
        pid,
    };
    match storage.acquire_advisory_lock(BACKEND_LOCK_ID, &backend_info) {
        Ok(()) => {
            return Ok(LockGuard {
                kind: LockGuardKind::Backend {
                    lock_id: BACKEND_LOCK_ID.to_string(),
                },
            });
        }
        Err(VykarError::UnsupportedBackend(_)) => {}
        Err(err) => return Err(err),
    }

    // Fallback to object-based lock files.
    cleanup_stale_locks(storage, Duration::seconds(DEFAULT_STALE_LOCK_SECS))?;

    let now = Utc::now();
    let entry = LockEntry {
        hostname,
        pid: pid as u32,
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
        kind: LockGuardKind::Object { key },
    })
}

/// Release an advisory lock.
pub fn release_lock(storage: &dyn StorageBackend, guard: LockGuard) -> Result<()> {
    match guard.kind {
        LockGuardKind::Object { key } => storage.delete(&key),
        LockGuardKind::Backend { lock_id } => storage.release_advisory_lock(&lock_id),
    }
}

/// Forcibly remove all advisory locks from the repository.
///
/// This is a recovery mechanism for stale locks left by killed processes.
/// No passphrase is needed — lock files are unencrypted JSON.
/// Returns the number of locks removed.
pub fn break_lock(storage: &dyn StorageBackend) -> Result<usize> {
    let mut removed: usize = 0;

    // Probe backend-native lock: try to acquire it. If we succeed, no stale
    // lock existed — release our own acquire and don't count it. If we get
    // `Locked`, a stale lock exists — force-release it and count 1.
    let dummy_info = BackendLockInfo {
        hostname: String::new(),
        pid: 0,
    };
    match storage.acquire_advisory_lock(BACKEND_LOCK_ID, &dummy_info) {
        Ok(()) => {
            // No stale lock — undo our probe acquire.
            let _ = storage.release_advisory_lock(BACKEND_LOCK_ID);
        }
        Err(VykarError::Locked(_)) => {
            // Stale lock exists — force-release it.
            storage.release_advisory_lock(BACKEND_LOCK_ID)?;
            removed += 1;
        }
        Err(VykarError::UnsupportedBackend(_)) => {}
        Err(err) => return Err(err),
    }

    // Remove all object-based lock files.
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
