//! The advisory lock and session registration of vykar v0.24.0 (commit
//! cd6782db29bff6638581758af72efcfda1267985), the last release that writes
//! format v3: what a 0.24 client does beside the v2/v3 upgrade's legacy lock
//! key (10-single-protocol step 6).
//!
//! Copied verbatim; do not edit. Each `BEGIN`/`END` block must diff against
//! `git show v0.24.0:crates/vykar-core/src/repo/lock.rs | sed -n '<range>p'`
//! with only visibility and `use` lines differing. `crate::platform`'s
//! `hostname`, `boot_id` and `is_pid_alive` have the release's signatures.

#![allow(dead_code)]
#![allow(clippy::collapsible_if)]

use std::time::SystemTime;

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;

use vykar_storage::StorageBackend;
use vykar_types::error::{LockHolder, LockHolderDetails, Result, VykarError};

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:13-21
/// A simple advisory lock stored in `locks/<uuid>.json`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LockEntry {
    pub(crate) hostname: String,
    pub(crate) pid: u32,
    pub(crate) time: String,
    #[serde(default)]
    pub(crate) boot_id: Option<String>,
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:23-24
pub(crate) const LOCKS_PREFIX: &str = "locks/";
pub(crate) const DEFAULT_STALE_LOCK_SECS: i64 = 6 * 60 * 60; // 6 hours
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:33-44
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
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:55-104
pub fn acquire_lock(storage: &dyn StorageBackend) -> Result<LockGuard> {
    let hostname = crate::platform::hostname();
    let pid = std::process::id();

    // Don't publish a key while a holder exists: waiters would otherwise
    // collide with each other on every retry.
    let held = cleanup_stale_locks(storage, Duration::seconds(DEFAULT_STALE_LOCK_SECS))?;
    if let Some(holder_key) = held.first() {
        return Err(VykarError::Locked(build_lock_holder(storage, holder_key)));
    }

    let now = Utc::now();
    let entry = LockEntry {
        hostname,
        pid,
        time: now.to_rfc3339(),
        boot_id: crate::platform::boot_id(),
    };

    let uuid = format!("{:032x}", rand::random::<u128>());
    // The timestamp prefix is not used for ordering. It is kept because older
    // clients still order by it, and stale reaping falls back to it when the
    // entry body is unreadable.
    let ts = now.timestamp_micros();
    let key = format!("{LOCKS_PREFIX}{ts:020}-{uuid}.json");
    let data = serde_json::to_vec(&entry)
        .map_err(|e| vykar_types::error::VykarError::Other(format!("lock serialize: {e}")))?;

    storage.put(&key, &data)?;

    let mut keys = list_lock_keys(storage)?;
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
        return Err(VykarError::Locked(build_lock_holder(storage, other)));
    }

    Ok(LockGuard {
        key,
        acquired_at: SystemTime::now(),
    })
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:106-109
/// Release an advisory lock.
pub fn release_lock(storage: &dyn StorageBackend, guard: LockGuard) -> Result<()> {
    storage.delete(&guard.key)
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:116-125
pub fn break_lock(storage: &dyn StorageBackend) -> Result<usize> {
    let mut removed: usize = 0;

    for key in list_lock_keys(storage)? {
        storage.delete(&key)?;
        removed += 1;
    }

    Ok(removed)
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:127-131
pub(crate) fn list_lock_keys(storage: &dyn StorageBackend) -> Result<Vec<String>> {
    let mut keys = storage.list(LOCKS_PREFIX)?;
    keys.retain(|k| k.starts_with(LOCKS_PREFIX) && k.ends_with(".json"));
    Ok(keys)
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:138-163
pub(crate) fn build_lock_holder(storage: &dyn StorageBackend, lock_key: &str) -> LockHolder {
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
        |ts| format_age_since(&Utc::now(), &ts),
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
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:170-176
pub(crate) fn probe_unlocked(storage: &dyn StorageBackend) -> Result<()> {
    let held = cleanup_stale_locks(storage, Duration::seconds(DEFAULT_STALE_LOCK_SECS))?;
    match held.first() {
        Some(key) => Err(VykarError::Locked(build_lock_holder(storage, key))),
        None => Ok(()),
    }
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:179-187
fn cleanup_stale_locks(storage: &dyn StorageBackend, max_age: Duration) -> Result<Vec<String>> {
    cleanup_stale_locks_inner(
        storage,
        max_age,
        &crate::platform::hostname(),
        crate::platform::boot_id().as_deref(),
        crate::platform::is_pid_alive,
    )
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:189-219
pub(crate) fn cleanup_stale_locks_inner(
    storage: &dyn StorageBackend,
    max_age: Duration,
    local_hostname: &str,
    local_boot_id: Option<&str>,
    pid_alive_fn: impl Fn(u32) -> bool,
) -> Result<Vec<String>> {
    let now = Utc::now();
    let mut surviving = Vec::new();
    for key in list_lock_keys(storage)? {
        let Some(data) = storage.get(&key)? else {
            continue;
        };
        let entry = serde_json::from_slice::<LockEntry>(&data).ok();
        if should_cleanup_lock(
            &key,
            entry.as_ref(),
            now,
            max_age,
            local_hostname,
            local_boot_id,
            &pid_alive_fn,
        ) {
            storage.delete(&key)?;
        } else {
            surviving.push(key);
        }
    }
    surviving.sort();
    Ok(surviving)
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:221-245
pub(crate) fn should_cleanup_lock(
    key: &str,
    entry: Option<&LockEntry>,
    now: chrono::DateTime<Utc>,
    max_age: Duration,
    local_hostname: &str,
    local_boot_id: Option<&str>,
    pid_alive_fn: &impl Fn(u32) -> bool,
) -> bool {
    if let Some(entry) = entry {
        let same_host = entry.hostname == local_hostname;
        if same_host {
            if let (Some(local), Some(remote)) = (local_boot_id, entry.boot_id.as_deref()) {
                if local != remote {
                    return true;
                }
            }
            if !pid_alive_fn(entry.pid) {
                return true;
            }
        }
    }

    lock_timestamp(entry, key).is_some_and(|acquired| now.signed_duration_since(acquired) > max_age)
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:247-265
fn lock_timestamp(entry: Option<&LockEntry>, key: &str) -> Option<chrono::DateTime<Utc>> {
    entry
        .and_then(|entry| {
            chrono::DateTime::parse_from_rfc3339(&entry.time)
                .ok()
                .map(|ts| ts.with_timezone(&Utc))
        })
        .or_else(|| lock_key_timestamp(key))
}

pub(crate) fn lock_key_timestamp(key: &str) -> Option<chrono::DateTime<Utc>> {
    let raw = key.strip_prefix(LOCKS_PREFIX)?.strip_suffix(".json")?;
    let (micros, _) = raw.split_once('-')?;
    if micros.len() != 20 || !micros.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let micros = micros.parse::<i64>().ok()?;
    chrono::DateTime::<Utc>::from_timestamp_micros(micros)
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:378
pub(crate) const SESSIONS_PREFIX: &str = "sessions/";
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:388-390
pub(crate) fn session_marker_key(session_id: &str) -> String {
    format!("{SESSIONS_PREFIX}{session_id}.json")
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:403-410
/// A session marker stored at `sessions/<session_id>.json`.
#[derive(Debug, Serialize, Deserialize)]
pub struct SessionEntry {
    pub hostname: String,
    pub pid: u32,
    pub registered_at: String,
    pub last_refresh: String,
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:426-455
pub(crate) fn register_session_inner(
    storage: &dyn StorageBackend,
    session_id: &str,
    max_attempts: usize,
    base_delay_ms: u64,
) -> Result<()> {
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

    match retry_while_locked(max_attempts, base_delay_ms, || probe_unlocked(storage)) {
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
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:765-769
fn backoff_delay(attempt: usize, base_delay_ms: u64) -> u64 {
    let delay = base_delay_ms * (1 << attempt.min(5));
    let jitter = (rand::random::<u64>() % (delay / 2).max(1)).wrapping_sub(delay / 4);
    delay.wrapping_add(jitter).max(100)
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:771-778
/// Compact holder description for log fields — `Display for LockHolder` is
/// the user-facing sentence, too verbose for a log field.
fn describe_holder(holder: &LockHolder) -> String {
    holder.details.as_ref().map_or_else(
        || holder.key.clone(),
        |d| format!("{} (PID {}, held {})", d.hostname, d.pid, d.age),
    )
}
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:783-814
fn retry_while_locked<T>(
    max_attempts: usize,
    base_delay_ms: u64,
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
                    std::thread::sleep(std::time::Duration::from_millis(delay));
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
// END

// BEGIN v0.24.0 crates/vykar-core/src/repo/lock.rs:926-941
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
// END
