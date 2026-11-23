use crate::repo::ages::StoreClock;
use crate::repo::host_env::{Clock, StopSignal};
use crate::repo::lock::{
    HEARTBEAT_POLL, HeartbeatAction, HeartbeatSchedule, MarkerReport, NowSource, RefreshOutcome,
    SESSION_STALE_SECS, SessionEntry, SessionGuard, SessionMarker, acquire_lock, break_lock,
    cleanup_stale_locks, cleanup_stale_sessions, clear_all_sessions, commit_marker_check,
    default_stale_session_duration, format_age, format_age_since, parse_session_marker_key,
    register_session, release_lock, session_marker_key, session_marker_key_at,
};
use crate::repo::{HostEnv, HostIdentity, SystemClock};
use crate::sim::env::{SimTime, TimeMode};
use crate::testutil::MemoryBackend;
use chrono::{Duration, Utc};
use std::sync::Arc;
use vykar_storage::StorageBackend;

/// Stub pid_alive function that always returns true (conservative default).
fn pid_always_alive(_pid: u32) -> bool {
    true
}

/// The system clock with a fixed uptime.
struct TestClock {
    uptime: Option<std::time::Duration>,
}

impl Clock for TestClock {
    fn now_utc(&self) -> chrono::DateTime<Utc> {
        SystemClock.now_utc()
    }
    fn monotonic(&self) -> std::time::Instant {
        SystemClock.monotonic()
    }
    fn sleep(&self, d: std::time::Duration) {
        SystemClock.sleep(d);
    }
    fn wait_timeout(&self, signal: &StopSignal, timeout: std::time::Duration) -> bool {
        SystemClock.wait_timeout(signal, timeout)
    }
    fn uptime(&self) -> Option<std::time::Duration> {
        self.uptime
    }
}

/// The identity the same-host tests run as.
fn me() -> HostIdentity {
    HostIdentity {
        hostname: "myhost".into(),
        pid: 1,
        boot_id: Some("boot-a".into()),
        installation_id: Some("inst-a".into()),
        pid_ns: Some(1),
    }
}

/// `host` on a clock with `uptime`, where `alive` decides PID liveness.
fn env_as(
    host: HostIdentity,
    alive: impl Fn(u32) -> bool + Send + Sync + 'static,
    uptime: Option<std::time::Duration>,
) -> HostEnv {
    HostEnv::system()
        .with_clock(Arc::new(TestClock { uptime }))
        .with_host(host)
        .with_pid_alive(Arc::new(move |_, p| alive(p)))
}

/// The system clock on host `hostname` with boot ID `boot_id` and no
/// installation ID (so only the age rule applies), where `alive` decides
/// PID liveness.
fn env_on(
    hostname: &str,
    boot_id: Option<&str>,
    alive: impl Fn(u32) -> bool + Send + Sync + 'static,
) -> HostEnv {
    HostEnv::system()
        .with_host(HostIdentity {
            hostname: hostname.to_string(),
            pid: 1,
            boot_id: boot_id.map(str::to_string),
            installation_id: None,
            pid_ns: None,
        })
        .with_pid_alive(Arc::new(move |_, p| alive(p)))
}

#[test]
fn acquire_and_release_lock() {
    let storage = MemoryBackend::new();
    let guard = acquire_lock(&storage, &HostEnv::system()).unwrap();
    let key = guard.key().to_string();

    // Lock key should exist in storage
    assert!(storage.exists(&key).unwrap());

    // Release should remove it
    release_lock(&storage, guard).unwrap();
    assert!(!storage.exists(&key).unwrap());
}

/// Fails the first LIST that would return a key (on an empty store, the
/// one after the lock PUT), once.
struct FailFirstNonEmptyList {
    inner: MemoryBackend,
    fired: std::sync::atomic::AtomicBool,
}

impl FailFirstNonEmptyList {
    fn check(&self, prefix: &str) -> vykar_types::error::Result<()> {
        if !self.inner.list(prefix)?.is_empty()
            && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(vykar_types::error::VykarError::Interrupted);
        }
        Ok(())
    }
}

impl vykar_storage::InnerBackend for FailFirstNonEmptyList {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for FailFirstNonEmptyList;
    except [list, list_sized, list_meta, server_ops];

    fn list_meta(
        &self,
        prefix: &str,
    ) -> vykar_types::error::Result<Vec<vykar_storage::ListedObject>> {
        self.list_sized(prefix)
    }

    fn list(&self, prefix: &str) -> vykar_types::error::Result<Vec<String>> {
        self.check(prefix)?;
        self.inner.list(prefix)
    }

    fn list_sized(
        &self,
        prefix: &str,
    ) -> vykar_types::error::Result<Vec<vykar_storage::ListedObject>> {
        self.check(prefix)?;
        self.inner.list_sized(prefix)
    }

    // No server behind the inner backend: listings stay on `list_sized`.
    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        None
    }
}

/// A LIST that fails after the lock PUT must not leave the key behind.
#[test]
fn failed_list_after_lock_put_removes_the_key() {
    let storage = FailFirstNonEmptyList {
        inner: MemoryBackend::new(),
        fired: std::sync::atomic::AtomicBool::new(false),
    };
    let err = acquire_lock(&storage, &HostEnv::system()).unwrap_err();
    assert!(
        matches!(err, vykar_types::error::VykarError::Interrupted),
        "{err}"
    );
    assert!(storage.inner.list("locks/").unwrap().is_empty());
}

#[test]
fn lock_key_in_locks_directory() {
    let storage = MemoryBackend::new();
    let guard = acquire_lock(&storage, &HostEnv::system()).unwrap();
    assert!(guard.key().starts_with("locks/"));
    assert!(guard.key().ends_with(".json"));
    release_lock(&storage, guard).unwrap();
}

#[test]
fn second_lock_is_rejected() {
    let storage = MemoryBackend::new();
    let first = acquire_lock(&storage, &HostEnv::system()).unwrap();

    let second = acquire_lock(&storage, &HostEnv::system());
    assert!(second.is_err(), "second lock acquisition should fail");
    let msg = second.unwrap_err().to_string();
    assert!(msg.contains("locked"), "unexpected error: {msg}");

    release_lock(&storage, first).unwrap();
}

/// The `Locked` message must be self-diagnosing: who holds the lock, how long
/// they have held it, and what to do about it (issue #174, where the message
/// named only host and PID and users could not tell a brief commit apart from
/// a long prune/compact on another machine).
#[test]
fn locked_error_names_holder_age_and_remedy() {
    let storage = MemoryBackend::new();
    let first = acquire_lock(&storage, &HostEnv::system()).unwrap();

    let msg = acquire_lock(&storage, &HostEnv::system())
        .unwrap_err()
        .to_string();
    assert!(
        msg.contains(&crate::platform::hostname()),
        "message should name the holder's host: {msg}"
    );
    assert!(
        msg.contains(&format!("PID {}", std::process::id())),
        "message should name the holder's PID: {msg}"
    );
    assert!(
        msg.contains("held for 0s"),
        "message should report a sub-minute age in seconds: {msg}"
    );
    assert!(
        msg.contains("prune/compact"),
        "message should name the likely cause: {msg}"
    );
    assert!(
        msg.contains("vykar break-lock"),
        "message should name the remedy: {msg}"
    );
    assert!(
        !msg.contains("locks/"),
        "a readable holder should be named by host/PID, not a raw key path: {msg}"
    );

    release_lock(&storage, first).unwrap();
}

/// An unreadable lock object is a normal race (the holder may have released it
/// between our LIST and GET), so we still fail with a usable message naming the
/// key rather than panicking or losing the remedy.
#[test]
fn locked_error_falls_back_to_key_when_holder_unparseable() {
    let storage = MemoryBackend::new();
    // Valid timestamp prefix so stale-lock cleanup does not reap it.
    let garbage_key = lock_key_at(Utc::now() - Duration::minutes(1), "garbage");
    storage.put(&garbage_key, b"not json").unwrap();

    let msg = acquire_lock(&storage, &HostEnv::system())
        .unwrap_err()
        .to_string();
    assert!(
        msg.contains(&garbage_key),
        "message should name the lock key: {msg}"
    );
    assert!(
        msg.contains("vykar break-lock"),
        "message should still name the remedy: {msg}"
    );
}

#[test]
fn format_age_renders_sub_minute_in_seconds() {
    let now = Utc::now();
    assert_eq!(
        format_age_since(&now, &(now - Duration::seconds(12))),
        "12s"
    );
    assert_eq!(
        format_age_since(&now, &(now - Duration::seconds(59))),
        "59s"
    );
    assert_eq!(format_age_since(&now, &(now - Duration::seconds(60))), "1m");
    assert_eq!(
        format_age_since(&now, &(now - Duration::minutes(42))),
        "42m"
    );
    assert_eq!(format_age_since(&now, &(now - Duration::hours(3))), "3h");
    assert_eq!(
        format_age_since(&now, &(now - Duration::hours(27))),
        "1d 3h"
    );
    // Clock skew (timestamp in the future) must not underflow into a negative.
    assert_eq!(format_age_since(&now, &(now + Duration::seconds(30))), "0s");
    // The RFC3339 wrapper delegates to the same logic.
    assert_eq!(
        format_age(&now, &(now - Duration::seconds(5)).to_rfc3339()),
        "5s"
    );
    assert_eq!(format_age(&now, "not-a-timestamp"), "unknown");
}

#[test]
fn stale_lock_is_cleaned_up() {
    let storage = MemoryBackend::new();
    let stale_key = "locks/00000000000000000000-stale.json";
    let stale_time = (Utc::now() - Duration::hours(7)).to_rfc3339();
    let stale_entry = format!(r#"{{"hostname":"old","pid":1234,"time":"{stale_time}"}}"#);
    storage.put(stale_key, stale_entry.as_bytes()).unwrap();
    assert!(storage.exists(stale_key).unwrap());

    let guard = acquire_lock(&storage, &HostEnv::system()).unwrap();
    assert!(
        !storage.exists(stale_key).unwrap(),
        "stale lock should be removed during acquisition"
    );
    release_lock(&storage, guard).unwrap();
}

fn lock_key_at(ts: chrono::DateTime<Utc>, suffix: &str) -> String {
    format!("locks/{:020}-{suffix}.json", ts.timestamp_micros())
}

fn write_lock_marker(
    storage: &dyn StorageBackend,
    key: &str,
    hostname: &str,
    pid: u32,
    time: chrono::DateTime<Utc>,
    boot_id: Option<&str>,
) {
    let mut value = serde_json::json!({
        "hostname": hostname,
        "pid": pid,
        "time": time.to_rfc3339(),
    });
    if let Some(boot_id) = boot_id {
        value["boot_id"] = serde_json::Value::String(boot_id.to_string());
    }
    storage
        .put(key, serde_json::to_vec(&value).unwrap().as_slice())
        .unwrap();
}

#[test]
fn stale_lock_cleanup_preserves_recent_foreign_host() {
    let storage = MemoryBackend::new();
    let key = lock_key_at(Utc::now(), "foreign");
    write_lock_marker(&storage, &key, "otherhost", 123, Utc::now(), Some("boot-a"));

    cleanup_stale_locks(
        &storage,
        Duration::hours(6),
        &env_on("myhost", Some("boot-b"), |_| false),
        NowSource::Probe(&mut StoreClock::default()),
    )
    .unwrap();

    assert!(storage.exists(&key).unwrap());
}

#[test]
fn stale_lock_cleanup_reaps_malformed_lock_by_key_age() {
    let storage = MemoryBackend::new();
    let old = Utc::now() - Duration::hours(7);
    let key = lock_key_at(old, "malformed");
    storage.put(&key, b"not-json").unwrap();

    cleanup_stale_locks(
        &storage,
        Duration::hours(6),
        &env_on("myhost", Some("boot-a"), |_| true),
        NowSource::Probe(&mut StoreClock::default()),
    )
    .unwrap();

    assert!(!storage.exists(&key).unwrap());
}

#[test]
fn stale_lock_cleanup_preserves_recent_old_format_lock() {
    let storage = MemoryBackend::new();
    let key = lock_key_at(Utc::now(), "old-format");
    write_lock_marker(&storage, &key, "otherhost", 123, Utc::now(), None);

    cleanup_stale_locks(
        &storage,
        Duration::hours(6),
        &env_on("myhost", Some("boot-a"), |_| false),
        NowSource::Probe(&mut StoreClock::default()),
    )
    .unwrap();

    assert!(storage.exists(&key).unwrap());
}

#[test]
fn stale_lock_cleanup_reaps_age_stale_old_format_lock() {
    let storage = MemoryBackend::new();
    let old = Utc::now() - Duration::hours(7);
    let key = lock_key_at(old, "age-stale");
    write_lock_marker(&storage, &key, "otherhost", 123, old, None);

    cleanup_stale_locks(
        &storage,
        Duration::hours(6),
        &env_on("myhost", Some("boot-a"), |_| false),
        NowSource::Probe(&mut StoreClock::default()),
    )
    .unwrap();

    assert!(!storage.exists(&key).unwrap());
}

#[test]
fn break_lock_removes_all_locks() {
    let storage = MemoryBackend::new();

    // Acquire a lock and "forget" the guard (simulates a killed process)
    let guard = acquire_lock(&storage, &HostEnv::system()).unwrap();
    let key = guard.key().to_string();
    assert!(storage.exists(&key).unwrap());
    std::mem::forget(guard);

    // break_lock should remove it
    let removed = break_lock(&storage).unwrap();
    assert_eq!(removed, 1);
    assert!(!storage.exists(&key).unwrap());
}

#[test]
fn break_lock_returns_zero_when_no_locks() {
    let storage = MemoryBackend::new();
    let removed = break_lock(&storage).unwrap();
    assert_eq!(removed, 0);
}

// --- cleanup_stale_sessions tests ---

/// Helper: write a session marker with the given last_refresh timestamp.
fn write_session_marker(storage: &MemoryBackend, session_id: &str, last_refresh: &str) {
    write_session_marker_with_host(storage, session_id, last_refresh, "test", 1);
}

/// Helper: write a session marker with a specific hostname and PID.
fn write_session_marker_with_host(
    storage: &MemoryBackend,
    session_id: &str,
    last_refresh: &str,
    hostname: &str,
    pid: u32,
) {
    let key = format!("sessions/{session_id}.json");
    let entry = crate::repo::lock::SessionEntry {
        hostname: hostname.to_string(),
        pid,
        registered_at: last_refresh.to_string(),
        last_refresh: last_refresh.to_string(),
        boot_id: None,
        installation_id: None,
        pid_ns: None,
    };
    let data = serde_json::to_vec(&entry).unwrap();
    storage.put(&key, &data).unwrap();
}

#[test]
fn cleanup_stale_sessions_preserves_active_index() {
    let storage = MemoryBackend::new();
    let now = Utc::now().to_rfc3339();

    // Active session with a companion .index file.
    write_session_marker(&storage, "sess1", &now);
    storage
        .put("sessions/sess1.index", b"journal-data")
        .unwrap();

    let cleaned = cleanup_stale_sessions(
        &storage,
        Duration::hours(72),
        &env_on("other-host", None, pid_always_alive),
        None,
    )
    .unwrap();
    assert!(cleaned.is_empty());
    assert!(storage.exists("sessions/sess1.json").unwrap());
    assert!(
        storage.exists("sessions/sess1.index").unwrap(),
        ".index should be preserved for active session"
    );
}

#[test]
fn cleanup_stale_sessions_removes_marker_preserves_index() {
    let storage = MemoryBackend::new();
    let old = (Utc::now() - Duration::hours(100)).to_rfc3339();

    // Stale session with companion .index.
    write_session_marker(&storage, "sess2", &old);
    storage
        .put("sessions/sess2.index", b"journal-data")
        .unwrap();

    let cleaned = cleanup_stale_sessions(
        &storage,
        Duration::hours(72),
        &env_on("other-host", None, pid_always_alive),
        None,
    )
    .unwrap();
    assert_eq!(cleaned, vec!["sess2"]);
    assert!(!storage.exists("sessions/sess2.json").unwrap());
    assert!(
        storage.exists("sessions/sess2.index").unwrap(),
        ".index should be preserved for recovery by next backup"
    );
}

#[test]
fn cleanup_stale_sessions_removes_orphaned_index() {
    let storage = MemoryBackend::new();

    // Orphaned .index with no companion .json marker.
    storage
        .put("sessions/orphan.index", b"journal-data")
        .unwrap();

    let cleaned = cleanup_stale_sessions(
        &storage,
        Duration::hours(72),
        &env_on("other-host", None, pid_always_alive),
        None,
    )
    .unwrap();
    assert!(cleaned.is_empty(), "no .json marker to report as cleaned");
    assert!(
        !storage.exists("sessions/orphan.index").unwrap(),
        "orphaned .index should be deleted"
    );
}

#[test]
fn cleanup_stale_sessions_skips_non_json_files() {
    let storage = MemoryBackend::new();
    let now = Utc::now().to_rfc3339();

    // Active session.
    write_session_marker(&storage, "active", &now);
    // An .index file that is NOT json — should not be parsed and deleted
    // as "unparseable".
    storage
        .put("sessions/active.index", b"\x00binary-journal")
        .unwrap();

    let cleaned = cleanup_stale_sessions(
        &storage,
        Duration::hours(72),
        &env_on("other-host", None, pid_always_alive),
        None,
    )
    .unwrap();
    assert!(cleaned.is_empty());
    assert!(
        storage.exists("sessions/active.index").unwrap(),
        ".index should not be treated as unparseable .json"
    );
}

// --- Same-host dead-process detection tests ---

#[test]
fn cleanup_removes_dead_local_session() {
    let storage = MemoryBackend::new();
    let now = Utc::now().to_rfc3339();

    // Session of this installation, boot and namespace with PID 42 — we'll
    // inject a pid_alive that says it's dead.
    let entry = SessionEntry {
        pid: 42,
        ..session_entry(&me(), &now)
    };
    storage
        .put(
            "sessions/local-dead.json",
            &serde_json::to_vec(&entry).unwrap(),
        )
        .unwrap();
    storage
        .put("sessions/local-dead.index", b"journal")
        .unwrap();

    let cleaned = cleanup_stale_sessions(
        &storage,
        Duration::hours(72),
        &env_as(me(), |_pid| false, None),
        None,
    )
    .unwrap();
    assert_eq!(cleaned, vec!["local-dead"]);
    assert!(!storage.exists("sessions/local-dead.json").unwrap());
    assert!(
        storage.exists("sessions/local-dead.index").unwrap(),
        ".index should be preserved for recovery by next backup"
    );
}

#[test]
fn cleanup_does_not_remove_remote_dead_pid() {
    let storage = MemoryBackend::new();
    let now = Utc::now().to_rfc3339();

    // Session from "remote-host" with PID 42 — even though pid_alive returns
    // false, we cannot verify remote PIDs, so the session must survive.
    write_session_marker_with_host(&storage, "remote-sess", &now, "remote-host", 42);

    let cleaned = cleanup_stale_sessions(
        &storage,
        Duration::hours(72),
        &env_on("myhost", None, |_pid| false),
        None,
    )
    .unwrap();
    assert!(
        cleaned.is_empty(),
        "remote session should not be removed by dead-PID detection"
    );
    assert!(storage.exists("sessions/remote-sess.json").unwrap());
}

#[test]
fn cleanup_second_pass_removes_orphan_index_from_prior_run() {
    let storage = MemoryBackend::new();
    let old = (Utc::now() - Duration::hours(100)).to_rfc3339();

    // Stale session with companion .index — first cleanup preserves .index.
    write_session_marker(&storage, "prior", &old);
    storage
        .put("sessions/prior.index", b"journal-data")
        .unwrap();

    let cleaned = cleanup_stale_sessions(
        &storage,
        Duration::hours(72),
        &env_on("other-host", None, pid_always_alive),
        None,
    )
    .unwrap();
    assert_eq!(cleaned, vec!["prior"]);
    assert!(!storage.exists("sessions/prior.json").unwrap());
    assert!(
        storage.exists("sessions/prior.index").unwrap(),
        ".index should survive first cleanup"
    );

    // Second cleanup: .json is gone and "prior" is not in cleaned_ids,
    // so the orphaned .index is now deleted.
    let cleaned = cleanup_stale_sessions(
        &storage,
        Duration::hours(72),
        &env_on("other-host", None, pid_always_alive),
        None,
    )
    .unwrap();
    assert!(cleaned.is_empty());
    assert!(
        !storage.exists("sessions/prior.index").unwrap(),
        ".index should be deleted on second cleanup (grace period expired)"
    );
}

// --- clear_all_sessions tests ---

#[test]
fn clear_all_sessions_removes_everything() {
    let storage = MemoryBackend::new();
    let now = Utc::now().to_rfc3339();

    // Normal session.
    write_session_marker(&storage, "s1", &now);
    storage.put("sessions/s1.index", b"journal").unwrap();

    // Malformed marker.
    storage.put("sessions/bad.json", b"not-valid-json").unwrap();

    // Orphaned .index with no .json.
    storage.put("sessions/orphan.index", b"journal").unwrap();

    let removed = clear_all_sessions(&storage).unwrap();
    assert_eq!(removed, 4); // s1.json, s1.index, bad.json, orphan.index

    assert!(!storage.exists("sessions/s1.json").unwrap());
    assert!(!storage.exists("sessions/s1.index").unwrap());
    assert!(!storage.exists("sessions/bad.json").unwrap());
    assert!(!storage.exists("sessions/orphan.index").unwrap());
}

#[test]
fn clear_all_sessions_returns_zero_when_empty() {
    let storage = MemoryBackend::new();
    let removed = clear_all_sessions(&storage).unwrap();
    assert_eq!(removed, 0);
}

// --- 45-minute stale threshold tests ---

#[test]
fn session_stale_threshold_is_45_minutes() {
    // Load-bearing for both maintenance cleanup and recovery classification.
    assert_eq!(SESSION_STALE_SECS, 45 * 60);
    assert_eq!(default_stale_session_duration().num_seconds(), 45 * 60);
}

#[test]
fn cleanup_keeps_session_at_44_minutes() {
    let storage = MemoryBackend::new();
    let forty_four = (Utc::now() - Duration::minutes(44)).to_rfc3339();
    write_session_marker(&storage, "fresh", &forty_four);

    let cleaned = cleanup_stale_sessions(
        &storage,
        default_stale_session_duration(),
        &env_on("other-host", None, pid_always_alive),
        None,
    )
    .unwrap();
    assert!(cleaned.is_empty(), "44-min session must not be reaped");
    assert!(storage.exists("sessions/fresh.json").unwrap());
}

#[test]
fn cleanup_reaps_session_at_46_minutes() {
    let storage = MemoryBackend::new();
    let forty_six = (Utc::now() - Duration::minutes(46)).to_rfc3339();
    write_session_marker(&storage, "stale", &forty_six);

    let cleaned = cleanup_stale_sessions(
        &storage,
        default_stale_session_duration(),
        &env_on("other-host", None, pid_always_alive),
        None,
    )
    .unwrap();
    assert_eq!(cleaned, vec!["stale"], "46-min session must be reaped");
    assert!(!storage.exists("sessions/stale.json").unwrap());
}

#[test]
fn cleanup_preserves_malformed_marker() {
    let storage = MemoryBackend::new();

    // Write a malformed .json marker with a companion .index.
    storage.put("sessions/bad.json", b"not-valid-json").unwrap();
    storage.put("sessions/bad.index", b"journal").unwrap();

    let cleaned = cleanup_stale_sessions(
        &storage,
        default_stale_session_duration(),
        &env_on("other-host", None, pid_always_alive),
        None,
    )
    .unwrap();

    // Malformed markers are not reported as "cleaned" and must NOT be
    // deleted while their age is unknown (no mtimes on MemoryBackend) —
    // they fail-close maintenance. With an mtime they age out after 45 min
    // (`store_clock::malformed_marker_is_reaped_once_old_by_mtime`).
    assert!(cleaned.is_empty());
    assert!(
        storage.exists("sessions/bad.json").unwrap(),
        "malformed .json must be preserved so maintenance surfaces it"
    );
    assert!(
        storage.exists("sessions/bad.index").unwrap(),
        "companion .index must be preserved for malformed markers"
    );
}

#[test]
fn cleanup_preserves_marker_with_bad_timestamp() {
    let storage = MemoryBackend::new();

    // Valid JSON structure, but timestamps fail RFC3339 parsing.
    write_session_marker(&storage, "bad-ts", "not-a-real-timestamp");

    let cleaned = cleanup_stale_sessions(
        &storage,
        default_stale_session_duration(),
        &env_on("other-host", None, pid_always_alive),
        None,
    )
    .unwrap();

    assert!(cleaned.is_empty());
    assert!(
        storage.exists("sessions/bad-ts.json").unwrap(),
        "marker with unparseable timestamp must survive cleanup"
    );
}

// --- Session marker rotation (04 step 2) ---

/// A [`MemoryBackend`] with per-key faults, and a hook run once before the
/// first EXISTS answers.
struct FaultyBackend {
    inner: Arc<MemoryBackend>,
    faults: std::sync::Mutex<Faults>,
    exists_calls: std::sync::Mutex<Vec<String>>,
    before_exists: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Default for FaultyBackend {
    fn default() -> Self {
        Self {
            inner: Arc::new(MemoryBackend::new()),
            faults: Default::default(),
            exists_calls: Default::default(),
            before_exists: Default::default(),
        }
    }
}

#[derive(Default)]
struct Faults {
    /// PUT stores the object, then fails.
    put_lands_then_fails: std::collections::HashSet<String>,
    exists_fails: std::collections::HashSet<String>,
    /// DELETE fails once, then works.
    delete_fails_once: std::collections::HashSet<String>,
    /// GET answers `None` although the object exists.
    get_none: std::collections::HashSet<String>,
}

impl FaultyBackend {
    fn faults(&self) -> std::sync::MutexGuard<'_, Faults> {
        self.faults.lock().unwrap()
    }

    fn exists_calls(&self) -> Vec<String> {
        self.exists_calls.lock().unwrap().clone()
    }
}

fn injected(key: &str) -> vykar_types::error::VykarError {
    vykar_types::error::VykarError::Other(format!("injected fault on {key}"))
}

impl StorageBackend for FaultyBackend {
    fn get(&self, key: &str) -> vykar_types::error::Result<Option<Vec<u8>>> {
        if self.faults().get_none.contains(key) {
            return Ok(None);
        }
        self.inner.get(key)
    }
    fn put(&self, key: &str, data: &[u8]) -> vykar_types::error::Result<()> {
        self.inner.put(key, data)?;
        if self.faults().put_lands_then_fails.contains(key) {
            return Err(injected(key));
        }
        Ok(())
    }
    fn delete(&self, key: &str) -> vykar_types::error::Result<()> {
        if self.faults().delete_fails_once.remove(key) {
            return Err(injected(key));
        }
        self.inner.delete(key)
    }
    fn exists(&self, key: &str) -> vykar_types::error::Result<bool> {
        if let Some(hook) = self.before_exists.lock().unwrap().take() {
            hook();
        }
        self.exists_calls.lock().unwrap().push(key.to_string());
        if self.faults().exists_fails.contains(key) {
            return Err(injected(key));
        }
        self.inner.exists(key)
    }
    fn list(&self, prefix: &str) -> vykar_types::error::Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> vykar_types::error::Result<Option<Vec<u8>>> {
        self.inner.get_range(key, offset, length)
    }
    fn create_dir(&self, key: &str) -> vykar_types::error::Result<()> {
        self.inner.create_dir(key)
    }
    fn profile(&self) -> vykar_storage::BackendProfile {
        self.inner.profile()
    }

    vykar_storage::unsupported_server_ops!();
}

const MIN: std::time::Duration = std::time::Duration::from_secs(60);

/// `me()` on a virtual clock that starts now.
fn sim_env() -> (Arc<SimTime>, HostEnv) {
    let time = SimTime::new(Utc::now(), TimeMode::Auto);
    let env = HostEnv::system()
        .with_clock(Arc::new(time.clock(Duration::zero())))
        .with_host(me());
    (time, env)
}

/// Register `id` on `storage` and build its marker record.
fn registered(storage: &dyn StorageBackend, id: &str, env: &HostEnv) -> SessionMarker {
    SessionMarker::new(&register_session(storage, id, env).unwrap())
}

fn entry_at(storage: &dyn StorageBackend, key: &str) -> SessionEntry {
    serde_json::from_slice(&storage.get(key).unwrap().unwrap()).unwrap()
}

fn session_keys(storage: &dyn StorageBackend) -> Vec<String> {
    let mut keys = storage.list("sessions/").unwrap();
    keys.sort();
    keys
}

#[test]
fn marker_keys_round_trip() {
    let id = "0123abcd";
    assert_eq!(session_marker_key_at(id, 0), session_marker_key(id));
    assert_eq!(session_marker_key(id), "sessions/0123abcd.json");
    assert_eq!(session_marker_key_at(id, 7), "sessions/0123abcd.7.json");
    assert_eq!(
        parse_session_marker_key(&session_marker_key(id)),
        Some((id, 0))
    );
    assert_eq!(
        parse_session_marker_key(&session_marker_key_at(id, 7)),
        Some((id, 7))
    );
    // Not canonical generations: the whole stem is the session, generation 0.
    assert_eq!(
        parse_session_marker_key("sessions/0123abcd.07.json"),
        Some(("0123abcd.07", 0))
    );
    assert_eq!(
        parse_session_marker_key("sessions/0123abcd.0.json"),
        Some(("0123abcd.0", 0))
    );
    assert_eq!(
        parse_session_marker_key("sessions/.7.json"),
        Some((".7", 0))
    );
    for key in ["sessions/0123abcd.index", "locks/x.json", "sessions/.json"] {
        assert_eq!(parse_session_marker_key(key), None, "{key}");
    }
}

#[test]
fn refresh_never_rewrites_a_reaped_key() {
    let storage = MemoryBackend::new();
    let (_time, env) = sim_env();
    let marker = registered(&storage, "ghost", &env);
    let gen0 = session_marker_key("ghost");
    // The reaper.
    storage.delete(&gen0).unwrap();

    let outcome = marker.refresh(&storage, env.clock.as_ref());
    assert!(matches!(outcome, RefreshOutcome::Missing), "{outcome:?}");
    assert!(
        !storage.exists(&gen0).unwrap(),
        "a reaped key is never written again"
    );
    assert!(storage.exists(&session_marker_key_at("ghost", 1)).unwrap());
    assert!(marker.report(env.clock.as_ref()).missing);

    let outcome = marker.refresh(&storage, env.clock.as_ref());
    assert!(
        matches!(outcome, RefreshOutcome::Refreshed { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        session_keys(&storage),
        [
            session_marker_key_at("ghost", 1),
            session_marker_key_at("ghost", 2)
        ]
    );
    assert!(
        marker.report(env.clock.as_ref()).missing,
        "missing is sticky"
    );
}

#[test]
fn refresh_rotates_to_the_next_key() {
    let storage = MemoryBackend::new();
    let (time, env) = sim_env();
    let marker = registered(&storage, "live", &env);
    let gen0 = session_marker_key("live");
    let registered_at = entry_at(&storage, &gen0).registered_at;
    time.advance(15 * MIN);

    let outcome = marker.refresh(&storage, env.clock.as_ref());
    assert!(
        matches!(outcome, RefreshOutcome::Refreshed { .. }),
        "{outcome:?}"
    );
    let gen1 = session_marker_key_at("live", 1);
    let entry = entry_at(&storage, &gen1);
    assert_eq!(entry.last_refresh, env.now_utc().to_rfc3339());
    assert_ne!(entry.last_refresh, registered_at);
    assert_eq!(entry.registered_at, registered_at);
    assert_eq!(entry.hostname, "myhost");
    assert_eq!(entry.pid, 1);
    assert_eq!(entry.boot_id.as_deref(), Some("boot-a"));
    assert_eq!(entry.installation_id.as_deref(), Some("inst-a"));
    assert_eq!(entry.pid_ns, Some(1));
    // The two keys overlap for a whole refresh interval.
    assert_eq!(session_keys(&storage), [gen1.clone(), gen0.clone()]);
    assert_eq!(marker.current(), (1, gen1.clone()));

    time.advance(15 * MIN);
    marker.refresh(&storage, env.clock.as_ref());
    assert_eq!(
        session_keys(&storage),
        [gen1, session_marker_key_at("live", 2)]
    );
    let report = marker.report(env.clock.as_ref());
    assert_eq!(report.refreshes, 2);
    assert_eq!(report.failed_refreshes, 0);
    assert!(!report.missing);
    assert!(report.continuous(), "{report:?}");
}

#[test]
fn refresh_put_failure_keeps_the_key_owned() {
    let storage = Arc::new(FaultyBackend::default());
    let gen1 = session_marker_key_at("put", 1);
    storage.faults().put_lands_then_fails.insert(gen1.clone());
    let (_time, env) = sim_env();
    let registration = register_session(storage.as_ref(), "put", &env).unwrap();
    let guard = SessionGuard::adopt(storage.clone(), registration, env.clone()).unwrap();
    let marker = guard.marker();

    let outcome = marker.refresh(storage.as_ref(), env.clock.as_ref());
    assert!(matches!(outcome, RefreshOutcome::Failed(_)), "{outcome:?}");
    assert_eq!(marker.current().0, 0);
    assert_eq!(marker.report(env.clock.as_ref()).failed_refreshes, 1);
    assert!(storage.exists(&gen1).unwrap(), "the failed PUT landed");

    let outcome = marker.refresh(storage.as_ref(), env.clock.as_ref());
    assert!(
        matches!(outcome, RefreshOutcome::Refreshed { .. }),
        "{outcome:?}"
    );
    assert_eq!(marker.current().0, 2);
    assert_eq!(session_keys(storage.as_ref()).len(), 3);

    drop(guard);
    assert_eq!(session_keys(storage.as_ref()), Vec::<String>::new());
}

#[test]
fn refresh_check_error_leaves_the_gap_open() {
    let storage = FaultyBackend::default();
    let gen0 = session_marker_key("chk");
    storage.faults().exists_fails.insert(gen0.clone());
    let (time, env) = sim_env();
    let marker = registered(&storage, "chk", &env);
    time.advance(15 * MIN);

    let outcome = marker.refresh(&storage, env.clock.as_ref());
    assert!(
        matches!(outcome, RefreshOutcome::Unchecked(_)),
        "{outcome:?}"
    );
    assert_eq!(marker.current().0, 1);
    assert!(
        storage.inner.exists(&gen0).unwrap(),
        "an unchecked key is not deleted"
    );
    let report = marker.report(env.clock.as_ref());
    assert_eq!(report.failed_refreshes, 1);
    assert_eq!(report.refreshes, 0);
    assert_eq!(report.unchecked, 1);
    assert!(!report.continuous());
    assert_eq!(report.longest_gap, 15 * MIN);
    time.advance(20 * MIN);
    assert_eq!(
        marker.report(env.clock.as_ref()).longest_gap,
        35 * MIN,
        "the gap keeps growing from registration"
    );

    // The check works again: the stretch closes at the newest write.
    storage.faults().exists_fails.clear();
    let outcome = marker.refresh(&storage, env.clock.as_ref());
    assert!(
        matches!(outcome, RefreshOutcome::Refreshed { .. }),
        "{outcome:?}"
    );
    let report = marker.report(env.clock.as_ref());
    assert_eq!((report.unchecked, report.missing), (0, false));
    assert_eq!(report.longest_gap, 35 * MIN);
}

/// A1: a reap hidden behind a failed presence check is found by the next
/// check, at a refresh or at the commit's probe.
#[test]
fn a_reap_behind_a_failed_check_is_found_later() {
    for at_probe in [false, true] {
        let storage = FaultyBackend::default();
        let gen0 = session_marker_key("hid");
        storage.faults().exists_fails.insert(gen0.clone());
        let (_time, env) = sim_env();
        let marker = registered(&storage, "hid", &env);
        storage.inner.delete(&gen0).unwrap();
        let outcome = marker.refresh(&storage, env.clock.as_ref());
        assert!(
            matches!(outcome, RefreshOutcome::Unchecked(_)),
            "{outcome:?}"
        );
        assert!(!marker.report(env.clock.as_ref()).missing);

        storage.faults().exists_fails.clear();
        if at_probe {
            assert!(marker.probe(&storage).unwrap(), "the current key is there");
        } else {
            let outcome = marker.refresh(&storage, env.clock.as_ref());
            assert!(matches!(outcome, RefreshOutcome::Missing), "{outcome:?}");
        }
        let report = marker.report(env.clock.as_ref());
        assert!(report.missing, "at_probe {at_probe}: {report:?}");
        assert_eq!(
            commit_marker_check("hid", Ok(true), &report)
                .unwrap_err()
                .to_string(),
            vykar_types::error::VykarError::StaleChunksDuringCommit.to_string()
        );
    }
}

#[test]
fn probe_follows_a_rotation() {
    let inner = Arc::new(MemoryBackend::new());
    let storage = FaultyBackend {
        inner: Arc::clone(&inner),
        ..Default::default()
    };
    let (_time, env) = sim_env();
    let marker = Arc::new(registered(&storage, "rot", &env));
    // Two refreshes before the probe's first answer: the second deletes the
    // key the probe asks about.
    let (m, clock) = (Arc::clone(&marker), Arc::clone(&env.clock));
    *storage.before_exists.lock().unwrap() = Some(Box::new(move || {
        m.refresh(inner.as_ref(), clock.as_ref());
        m.refresh(inner.as_ref(), clock.as_ref());
    }));

    assert!(marker.probe(&storage).unwrap());
    assert_eq!(
        storage.exists_calls(),
        [session_marker_key("rot"), session_marker_key_at("rot", 2)]
    );
    assert!(!marker.report(env.clock.as_ref()).missing);
}

#[test]
fn probe_reports_a_reaped_current_key() {
    let storage = FaultyBackend::default();
    let (_time, env) = sim_env();
    let marker = registered(&storage, "gone", &env);
    storage.delete(&session_marker_key("gone")).unwrap();
    assert!(!marker.probe(&storage).unwrap());
    assert_eq!(storage.exists_calls().len(), 1);
}

#[test]
fn gap_is_measured_on_suspend_aware_time() {
    let storage = MemoryBackend::new();
    let (time, env) = sim_env();
    let clock = env.clock.as_ref();
    let marker = registered(&storage, "nap", &env);
    time.advance(40 * MIN);
    match marker.refresh(&storage, clock) {
        RefreshOutcome::Refreshed { gap } => assert_eq!(gap, 40 * MIN),
        other => panic!("{other:?}"),
    }
    time.advance(5 * MIN);
    let report = marker.report(clock);
    assert_eq!(report.longest_gap, 40 * MIN);
    assert!(!report.continuous());

    let marker = registered(&storage, "awake", &env);
    for _ in 0..2 {
        time.advance(15 * MIN);
        marker.refresh(&storage, clock);
    }
    assert!(marker.report(clock).continuous());

    // An NTP step of the wall clock is not a gap: uptime wins over wall.
    time.advance(15 * MIN);
    time.jump_wall(Duration::hours(2));
    match marker.refresh(&storage, clock) {
        RefreshOutcome::Refreshed { gap } => assert_eq!(gap, 15 * MIN),
        other => panic!("{other:?}"),
    }
    assert!(marker.report(clock).continuous());
}

#[test]
fn commit_marker_check_rules() {
    let report = |missing: bool, gap_min: u32| MarkerReport {
        missing,
        longest_gap: gap_min * MIN,
        unchecked: 0,
        refreshes: 0,
        failed_refreshes: 0,
    };
    let stale = |r: vykar_types::error::Result<()>| {
        matches!(
            r,
            Err(vykar_types::error::VykarError::StaleChunksDuringCommit)
        )
    };
    let probe_err = || Err(injected("sessions/x.json"));
    assert!(stale(commit_marker_check("s", Ok(true), &report(true, 0))));
    assert!(stale(commit_marker_check(
        "s",
        Ok(false),
        &report(false, 0)
    )));
    assert!(commit_marker_check("s", Ok(true), &report(false, 120)).is_ok());
    assert!(commit_marker_check("s", probe_err(), &report(false, 15)).is_ok());
    let err = commit_marker_check("s", probe_err(), &report(false, 31)).unwrap_err();
    assert!(err.to_string().contains("injected fault"), "{err}");
    assert!(stale(commit_marker_check(
        "s",
        probe_err(),
        &report(true, 0)
    )));
    let unchecked = MarkerReport {
        unchecked: 1,
        ..report(false, 1)
    };
    assert!(commit_marker_check("s", probe_err(), &unchecked).is_err());
    // A1: a key still unchecked after the probe refuses even when the
    // current key was found, and not as stale: the reap is not proven.
    let err = commit_marker_check("s", Ok(true), &unchecked).unwrap_err();
    assert!(
        !matches!(err, vykar_types::error::VykarError::StaleChunksDuringCommit),
        "{err}"
    );
}

/// F1 (A1): a superseded key whose check fails at the refresh and again at
/// the commit's probe fails the probe with that check's error, and the
/// commit refuses with it, whether a reaper deleted the key (a reaper that
/// listed before PUT n deletes n−1) or not (cannot tell).
#[test]
fn a_key_that_stays_unchecked_refuses_the_commit() {
    for reaped in [true, false] {
        let storage = FaultyBackend::default();
        let gen0 = session_marker_key("unk");
        storage.faults().exists_fails.insert(gen0.clone());
        let (_time, env) = sim_env();
        let marker = registered(&storage, "unk", &env);
        let outcome = marker.refresh(&storage, env.clock.as_ref());
        assert!(
            matches!(outcome, RefreshOutcome::Unchecked(_)),
            "{outcome:?}"
        );
        if reaped {
            storage.inner.delete(&gen0).unwrap();
        }

        let probe = marker.probe(&storage);
        let probe_text = probe.as_ref().unwrap_err().to_string();
        assert_eq!(probe_text, injected(&gen0).to_string(), "reaped {reaped}");
        assert_eq!(
            storage.exists_calls().last(),
            Some(&gen0),
            "the probe stops at the unchecked key"
        );
        let report = marker.report(env.clock.as_ref());
        assert_eq!((report.missing, report.unchecked), (false, 1));
        let err = commit_marker_check("unk", probe, &report).unwrap_err();
        assert_eq!(err.to_string(), probe_text, "reaped {reaped}");
    }
}

/// F4: a reaper that listed only key n−1 before refresh n's PUT deletes it
/// after that refresh found it present. Its second LIST sees key n, so
/// maintenance refuses; the session never checks a key it found present
/// again, so it records nothing and commits: no false "missing".
#[test]
fn a_reap_after_the_check_is_refused_by_maintenance_and_not_recorded() {
    let storage = MemoryBackend::new();
    let (time, env) = sim_env();
    let marker = registered(&storage, "after", &env);
    time.advance(15 * MIN);
    let outcome = marker.refresh(&storage, env.clock.as_ref());
    assert!(
        matches!(outcome, RefreshOutcome::Refreshed { .. }),
        "{outcome:?}"
    );
    // The reaper, which listed generation 0 only.
    storage.delete(&session_marker_key("after")).unwrap();
    let listed = crate::repo::lock::list_session_entries(&storage).unwrap();
    assert_eq!(listed.len(), 1, "the second LIST sees key 1: {listed:?}");
    assert!(listed[0].1.is_some());

    assert!(marker.probe(&storage).unwrap());
    time.advance(15 * MIN);
    let outcome = marker.refresh(&storage, env.clock.as_ref());
    assert!(
        matches!(outcome, RefreshOutcome::Refreshed { .. }),
        "{outcome:?}"
    );
    let report = marker.report(env.clock.as_ref());
    assert!(report.continuous(), "{report:?}");
    assert!(commit_marker_check("after", marker.probe(&storage), &report).is_ok());
}

/// Polls until the heartbeat's next refresh, counting the poll that
/// refreshes.
fn polls_to_refresh(schedule: &mut HeartbeatSchedule) -> usize {
    (1..=60)
        .find(|_| schedule.poll() == HeartbeatAction::Refresh)
        .expect("a refresh within the regular interval")
}

/// F2 (A4): a failed PUT is retried at the next poll, then after 1 min,
/// 2 min, … up to the regular interval; a success restores the interval.
#[test]
fn a_failed_put_is_retried_with_back_off() {
    let mut schedule = HeartbeatSchedule::new();
    assert_eq!(polls_to_refresh(&mut schedule), 30);
    let mut waits = Vec::new();
    for _ in 0..7 {
        schedule.record(
            HeartbeatAction::Refresh,
            &RefreshOutcome::Failed(injected("sessions/x.1.json")),
        );
        waits.push(polls_to_refresh(&mut schedule));
    }
    // Capped at two minutes, far below the 30 minutes between the refresh
    // interval and the marker staleness.
    assert_eq!(waits, [1, 2, 4, 4, 4, 4, 4]);
    schedule.record(
        HeartbeatAction::Refresh,
        &RefreshOutcome::Refreshed {
            gap: std::time::Duration::ZERO,
        },
    );
    assert_eq!(polls_to_refresh(&mut schedule), 30);
}

/// Run `polls` heartbeat polls of `marker` on `storage`, the clock one
/// poll further each.
fn run_heartbeat(
    storage: &dyn StorageBackend,
    marker: &SessionMarker,
    schedule: &mut HeartbeatSchedule,
    time: &SimTime,
    env: &HostEnv,
    polls: usize,
) {
    for _ in 0..polls {
        time.advance(HEARTBEAT_POLL);
        let action = schedule.poll();
        let outcome = match action {
            HeartbeatAction::Wait => continue,
            HeartbeatAction::Recheck => marker.recheck(storage),
            HeartbeatAction::Refresh => marker.refresh(storage, env.clock.as_ref()),
        };
        schedule.record(action, &outcome);
    }
}

/// F2: while presence checks fail and PUTs work, the polls between
/// refreshes recheck the unchecked keys and write nothing: an outage
/// spanning two refreshes leaves one key more than a healthy run, and once
/// the checks work the next poll settles the keys.
#[test]
fn an_outage_of_presence_checks_does_not_multiply_keys() {
    // Refreshes at polls 30 and 60.
    const POLLS: usize = 88;
    let healthy = MemoryBackend::new();
    let (time, env) = sim_env();
    let marker = registered(&healthy, "ok", &env);
    let mut schedule = HeartbeatSchedule::new();
    run_heartbeat(&healthy, &marker, &mut schedule, &time, &env, POLLS);
    let healthy_keys = session_keys(&healthy).len();
    assert_eq!(healthy_keys, 2);

    let storage = FaultyBackend::default();
    storage
        .faults()
        .exists_fails
        .extend((0..10).map(|g| session_marker_key_at("out", g)));
    let (time, env) = sim_env();
    let marker = registered(&storage, "out", &env);
    let mut schedule = HeartbeatSchedule::new();
    run_heartbeat(&storage, &marker, &mut schedule, &time, &env, POLLS);
    assert_eq!(session_keys(&storage).len(), healthy_keys + 1);
    assert_eq!(marker.current().0, 2, "rechecks write no generation");
    let checks = storage.exists_calls().len();
    assert!(checks <= 2 * POLLS, "{checks} presence checks");
    let report = marker.report(env.clock.as_ref());
    assert_eq!(report.unchecked, 2);
    assert!(!report.continuous());

    storage.faults().exists_fails.clear();
    run_heartbeat(&storage, &marker, &mut schedule, &time, &env, 1);
    let report = marker.report(env.clock.as_ref());
    assert_eq!((report.unchecked, report.missing), (0, false));
    assert!(report.continuous(), "{report:?}");
    // The refresh at poll 90 deletes the keys the recheck found present.
    run_heartbeat(&storage, &marker, &mut schedule, &time, &env, 1);
    assert_eq!(
        session_keys(&storage),
        [
            session_marker_key_at("out", 2),
            session_marker_key_at("out", 3)
        ]
    );
}

/// F4: of generations 9 and 10, 10 is the newest (numeric, not the
/// listing's lexical order) in cleanup and in the listing.
#[test]
fn the_newest_generation_is_numeric() {
    let storage = MemoryBackend::new();
    let env = env_on("other-host", None, pid_always_alive);
    let ago = |m: i64| (Utc::now() - Duration::minutes(m)).to_rfc3339();
    let fresh = ago(1);
    write_session_marker(&storage, "live.9", &ago(50));
    write_session_marker(&storage, "live.10", &fresh);
    write_session_marker(&storage, "dead.9", &ago(1));
    write_session_marker(&storage, "dead.10", &ago(50));

    let cleaned =
        cleanup_stale_sessions(&storage, default_stale_session_duration(), &env, None).unwrap();
    assert_eq!(cleaned, ["dead"]);
    assert_eq!(
        session_keys(&storage),
        ["sessions/live.10.json", "sessions/live.9.json"]
    );
    let listed = crate::repo::lock::list_session_entries(&storage).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].0, "live");
    assert_eq!(listed[0].1.as_ref().unwrap().last_refresh, fresh);
}

#[test]
fn one_session_with_two_keys_is_listed_once() {
    let storage = MemoryBackend::new();
    let old = (Utc::now() - Duration::minutes(20)).to_rfc3339();
    let new = (Utc::now() - Duration::minutes(1)).to_rfc3339();
    write_session_marker(&storage, "two", &old);
    write_session_marker(&storage, "two.3", &new);

    let entries = crate::repo::lock::list_session_entries(&storage).unwrap();
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0].0, "two");
    assert_eq!(entries[0].1.as_ref().unwrap().last_refresh, new);
    assert_eq!(crate::repo::lock::list_sessions(&storage).unwrap(), ["two"]);
}

/// A3: a session is judged by its newest key.
#[test]
fn cleanup_judges_a_session_by_its_newest_key() {
    let storage = MemoryBackend::new();
    let env = env_on("other-host", None, pid_always_alive);
    let ago = |m: i64| (Utc::now() - Duration::minutes(m)).to_rfc3339();
    write_session_marker(&storage, "live", &ago(50));
    write_session_marker(&storage, "live.1", &ago(5));
    storage.put("sessions/live.index", b"journal").unwrap();
    write_session_marker(&storage, "dead", &ago(60));
    write_session_marker(&storage, "dead.1", &ago(50));
    storage.put("sessions/dead.index", b"journal").unwrap();

    let cleaned =
        cleanup_stale_sessions(&storage, default_stale_session_duration(), &env, None).unwrap();
    assert_eq!(cleaned, ["dead"]);
    assert_eq!(
        session_keys(&storage),
        [
            "sessions/dead.index",
            "sessions/live.1.json",
            "sessions/live.index",
            "sessions/live.json",
        ]
    );
    let listed = crate::repo::lock::list_session_entries(&storage).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].0, "live");
}

#[test]
fn cleanup_keeps_the_journal_of_a_vanished_marker() {
    let storage = FaultyBackend::default();
    write_session_marker(&storage.inner, "vanish", &Utc::now().to_rfc3339());
    storage.put("sessions/vanish.index", b"journal").unwrap();
    storage
        .faults()
        .get_none
        .insert(session_marker_key("vanish"));

    let env = env_on("other-host", None, pid_always_alive);
    let cleaned =
        cleanup_stale_sessions(&storage, default_stale_session_duration(), &env, None).unwrap();
    assert!(cleaned.is_empty());
    assert!(storage.exists("sessions/vanish.index").unwrap());
}

#[test]
fn deregistration_deletes_every_owned_key() {
    let storage = Arc::new(FaultyBackend::default());
    let (_time, env) = sim_env();
    let registration = register_session(storage.as_ref(), "dereg", &env).unwrap();
    let guard = SessionGuard::adopt(storage.clone(), registration, env.clone()).unwrap();
    let marker = guard.marker();
    marker.refresh(storage.as_ref(), env.clock.as_ref());
    // The second refresh deletes gen 0, and that delete fails.
    storage
        .faults()
        .delete_fails_once
        .insert(session_marker_key("dereg"));
    marker.refresh(storage.as_ref(), env.clock.as_ref());
    assert_eq!(session_keys(storage.as_ref()).len(), 3);

    drop(guard);
    assert_eq!(session_keys(storage.as_ref()), Vec::<String>::new());
}

// --- Same-host rule (04 step 1) ---

/// A full-identity session entry for `host`, written at `at`.
fn session_entry(host: &HostIdentity, at: &str) -> SessionEntry {
    SessionEntry {
        hostname: host.hostname.clone(),
        pid: host.pid,
        registered_at: at.to_string(),
        last_refresh: at.to_string(),
        boot_id: host.boot_id.clone(),
        installation_id: host.installation_id.clone(),
        pid_ns: host.pid_ns,
    }
}

/// Who wrote a planted entry, and how long ago.
#[derive(Clone)]
struct Writer {
    host: HostIdentity,
    /// Before now on the reaper's clock (negative: in the future).
    age: Duration,
    /// Write the identity keys; `false` is the format of 0.24 and older
    /// (locks carry a boot ID there, sessions do not).
    identity: bool,
}

/// An entry of PID 123 under `me()`'s identity, written now.
fn writer() -> Writer {
    Writer {
        host: HostIdentity { pid: 123, ..me() },
        age: Duration::zero(),
        identity: true,
    }
}

fn plant_lock(storage: &dyn StorageBackend, w: &Writer) -> String {
    let time = Utc::now() - w.age;
    let key = lock_key_at(time, "planted");
    let mut value = serde_json::json!({
        "hostname": w.host.hostname,
        "pid": w.host.pid,
        "time": time.to_rfc3339(),
        "boot_id": w.host.boot_id,
    });
    if w.identity {
        value["installation_id"] = serde_json::json!(w.host.installation_id);
        value["pid_ns"] = serde_json::json!(w.host.pid_ns);
    }
    storage
        .put(&key, &serde_json::to_vec(&value).unwrap())
        .unwrap();
    key
}

fn plant_session(storage: &dyn StorageBackend, w: &Writer) -> String {
    let time = (Utc::now() - w.age).to_rfc3339();
    let key = session_marker_key("planted");
    let mut value = serde_json::json!({
        "hostname": w.host.hostname,
        "pid": w.host.pid,
        "registered_at": time,
        "last_refresh": time,
    });
    if w.identity {
        value["boot_id"] = serde_json::json!(w.host.boot_id);
        value["installation_id"] = serde_json::json!(w.host.installation_id);
        value["pid_ns"] = serde_json::json!(w.host.pid_ns);
    }
    storage
        .put(&key, &serde_json::to_vec(&value).unwrap())
        .unwrap();
    key
}

/// Plant `w`'s entry once as a lock (6 h limit) and once as a session
/// marker (45 min limit), each in a fresh store, and run the reaper as
/// `env`. Returns (lock reaped, session reaped).
fn verdicts(w: &Writer, env: &HostEnv) -> (bool, bool) {
    let locks = MemoryBackend::new();
    let key = plant_lock(&locks, w);
    cleanup_stale_locks(
        &locks,
        Duration::hours(6),
        env,
        NowSource::Probe(&mut StoreClock::default()),
    )
    .unwrap();
    let lock_reaped = !locks.exists(&key).unwrap();

    let sessions = MemoryBackend::new();
    let key = plant_session(&sessions, w);
    cleanup_stale_sessions(&sessions, default_stale_session_duration(), env, None).unwrap();
    let session_reaped = !sessions.exists(&key).unwrap();
    (lock_reaped, session_reaped)
}

const HOUR: std::time::Duration = std::time::Duration::from_secs(3600);
const TEN_MIN: std::time::Duration = std::time::Duration::from_secs(600);

fn dead(_: u32) -> bool {
    false
}

fn alive(_: u32) -> bool {
    true
}

#[test]
fn same_installation_dead_pid_is_reaped() {
    assert_eq!(
        verdicts(&writer(), &env_as(me(), dead, Some(HOUR))),
        (true, true)
    );
}

#[test]
fn same_installation_live_pid_is_kept() {
    assert_eq!(
        verdicts(&writer(), &env_as(me(), alive, Some(HOUR))),
        (false, false)
    );
}

/// A cloned machine (same installation and hostname, another boot) wrote
/// the entry after we booted: not ours to reap.
#[test]
fn clone_with_other_boot_is_kept() {
    let mut w = writer();
    w.host.boot_id = Some("boot-b".into());
    w.age = Duration::minutes(10);
    assert_eq!(
        verdicts(&w, &env_as(me(), dead, Some(72 * HOUR))),
        (false, false)
    );
}

/// This machine rebooted: an entry of another boot older than our uptime
/// is from an earlier boot. Without an uptime, no claim.
#[test]
fn earlier_boot_entry_is_reaped() {
    let mut w = writer();
    w.host.boot_id = Some("boot-old".into());
    w.age = Duration::minutes(30);
    assert_eq!(
        verdicts(&w, &env_as(me(), alive, Some(TEN_MIN))),
        (true, true)
    );
    assert_eq!(verdicts(&w, &env_as(me(), dead, None)), (false, false));
}

/// Containers sharing a hostname, a config volume and a kernel: a PID of
/// another namespace cannot be probed from here.
#[test]
fn other_pid_namespace_is_kept() {
    let mut w = writer();
    w.host.pid_ns = Some(2);
    assert_eq!(
        verdicts(&w, &env_as(me(), dead, Some(HOUR))),
        (false, false)
    );
}

/// Two installations that share a hostname (and here even a boot ID).
#[test]
fn namesake_installation_is_kept() {
    let mut w = writer();
    w.host.installation_id = Some("inst-b".into());
    assert_eq!(
        verdicts(&w, &env_as(me(), dead, Some(HOUR))),
        (false, false)
    );
}

/// One installation ID on two machines with different hostnames (a copied
/// or shared home directory): the hostname still has to match.
#[test]
fn same_installation_other_hostname_is_kept() {
    let unknown_boot = |h: HostIdentity| HostIdentity {
        boot_id: None,
        pid_ns: None,
        ..h
    };
    let mut w = writer();
    w.host = unknown_boot(w.host);
    w.host.hostname = "otherhost".into();
    assert_eq!(
        verdicts(&w, &env_as(unknown_boot(me()), dead, None)),
        (false, false)
    );

    let mut w = writer();
    w.host.hostname = "otherhost".into();
    w.host.boot_id = Some("boot-old".into());
    w.age = Duration::minutes(30);
    assert_eq!(
        verdicts(&w, &env_as(me(), dead, Some(TEN_MIN))),
        (false, false)
    );
}

/// Entries of 0.24 and older carry no installation ID: age only.
#[test]
fn old_format_entry_is_reaped_by_age_only() {
    let mut w = writer();
    w.identity = false;
    assert_eq!(
        verdicts(&w, &env_as(me(), dead, Some(HOUR))),
        (false, false)
    );
    w.age = Duration::minutes(46);
    assert_eq!(verdicts(&w, &env_as(me(), dead, Some(HOUR))), (false, true));
    w.age = Duration::hours(7);
    assert_eq!(verdicts(&w, &env_as(me(), dead, Some(HOUR))), (true, true));
}

#[test]
fn no_local_installation_id_reaps_by_age_only() {
    let local = HostIdentity {
        installation_id: None,
        ..me()
    };
    assert_eq!(
        verdicts(&writer(), &env_as(local, dead, Some(HOUR))),
        (false, false)
    );
}

#[test]
fn boot_unknown_on_one_side_is_kept() {
    let mut w = writer();
    let local = HostIdentity {
        boot_id: None,
        ..me()
    };
    assert_eq!(
        verdicts(&w, &env_as(local, dead, Some(HOUR))),
        (false, false)
    );
    w.host.boot_id = None;
    assert_eq!(
        verdicts(&w, &env_as(me(), dead, Some(HOUR))),
        (false, false)
    );
}

/// Platforms without a boot ID or PID namespaces (macOS, BSD): within one
/// installation and hostname the PID decides.
#[test]
fn boot_unknown_on_both_sides_uses_pid() {
    let bare = |h: HostIdentity| HostIdentity {
        boot_id: None,
        pid_ns: None,
        ..h
    };
    let mut w = writer();
    w.host = bare(w.host);
    assert_eq!(verdicts(&w, &env_as(bare(me()), dead, None)), (true, true));
    assert_eq!(
        verdicts(&w, &env_as(bare(me()), alive, None)),
        (false, false)
    );
}

#[test]
fn future_entry_with_other_boot_is_kept() {
    let mut w = writer();
    w.host.boot_id = Some("boot-b".into());
    w.age = Duration::hours(-1);
    assert_eq!(
        verdicts(
            &w,
            &env_as(me(), dead, Some(std::time::Duration::from_secs(60)))
        ),
        (false, false)
    );
}

// --- Entry formats (D1) ---

/// The fields of a 0.24 lock entry, as 0.24 declared them.
#[derive(serde::Deserialize)]
struct LockEntryV024 {
    hostname: String,
    pid: u32,
    time: String,
    #[serde(default)]
    #[allow(dead_code)]
    boot_id: Option<String>,
}

/// The fields of a 0.24 session entry, as 0.24 declared them.
#[derive(serde::Deserialize)]
struct SessionEntryV024 {
    hostname: String,
    pid: u32,
    registered_at: String,
    last_refresh: String,
}

/// A lock and a marker written by `me()`: (lock bytes, marker bytes).
fn written_entries() -> (Vec<u8>, Vec<u8>) {
    let storage = MemoryBackend::new();
    let env = env_as(me(), alive, Some(HOUR));
    let guard = acquire_lock(&storage, &env).unwrap();
    let lock = storage.get(guard.key()).unwrap().unwrap();
    release_lock(&storage, guard).unwrap();
    register_session(&storage, "fmt", &env).unwrap();
    let marker = storage.get(&session_marker_key("fmt")).unwrap().unwrap();
    (lock, marker)
}

#[test]
fn entries_round_trip_identity() {
    let (lock, marker) = written_entries();
    let lock: serde_json::Value = serde_json::from_slice(&lock).unwrap();
    assert_eq!(lock["hostname"], "myhost");
    assert_eq!(lock["boot_id"], "boot-a");
    assert_eq!(lock["installation_id"], "inst-a");
    assert_eq!(lock["pid_ns"], 1);

    let entry: SessionEntry = serde_json::from_slice(&marker).unwrap();
    let again: SessionEntry = serde_json::from_slice(&serde_json::to_vec(&entry).unwrap()).unwrap();
    for e in [&entry, &again] {
        assert_eq!(e.hostname, "myhost");
        assert_eq!(e.pid, 1);
        assert_eq!(e.boot_id.as_deref(), Some("boot-a"));
        assert_eq!(e.installation_id.as_deref(), Some("inst-a"));
        assert_eq!(e.pid_ns, Some(1));
    }

    // Unknown values are written as null, never omitted.
    let storage = MemoryBackend::new();
    let bare = HostIdentity {
        boot_id: None,
        installation_id: None,
        pid_ns: None,
        ..me()
    };
    register_session(&storage, "bare", &env_as(bare, alive, None)).unwrap();
    let marker: serde_json::Value =
        serde_json::from_slice(&storage.get(&session_marker_key("bare")).unwrap().unwrap())
            .unwrap();
    for k in ["boot_id", "installation_id", "pid_ns"] {
        assert_eq!(marker.get(k), Some(&serde_json::Value::Null), "{k}");
    }
}

#[test]
fn entries_without_identity_parse() {
    let now = Utc::now().to_rfc3339();
    let old =
        format!(r#"{{"hostname":"h","pid":7,"registered_at":"{now}","last_refresh":"{now}"}}"#);
    let entry: SessionEntry = serde_json::from_str(&old).unwrap();
    assert_eq!(entry.hostname, "h");
    assert_eq!(entry.boot_id, None);
    assert_eq!(entry.installation_id, None);
    assert_eq!(entry.pid_ns, None);
}

/// 0.24 parses what this binary writes (it ignores the unknown keys).
#[test]
fn new_entries_parse_as_0_24() {
    let (lock, marker) = written_entries();
    let lock: LockEntryV024 = serde_json::from_slice(&lock).unwrap();
    assert_eq!(lock.hostname, "myhost");
    assert_eq!(lock.pid, 1);
    assert!(chrono::DateTime::parse_from_rfc3339(&lock.time).is_ok());
    let marker: SessionEntryV024 = serde_json::from_slice(&marker).unwrap();
    assert_eq!(marker.hostname, "myhost");
    assert_eq!(marker.pid, 1);
    assert!(chrono::DateTime::parse_from_rfc3339(&marker.registered_at).is_ok());
    assert!(chrono::DateTime::parse_from_rfc3339(&marker.last_refresh).is_ok());
}

// --- SessionGuard lifecycle tests ---

#[test]
fn session_heartbeat_refreshes_on_virtual_time() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let start = Utc::now();
    let time = SimTime::new(start, TimeMode::Auto);
    let env = HostEnv::system().with_clock(Arc::new(time.clock(Duration::zero())));
    let session_id = format!("guard-{:016x}", rand::random::<u64>());
    let wait = std::time::Duration::from_secs(5);

    let registration = register_session(storage.as_ref(), &session_id, &env).unwrap();
    let gen0 = session_marker_key(&session_id);
    let gen1 = session_marker_key_at(&session_id, 1);
    assert!(storage.exists(&gen0).unwrap());

    let guard = SessionGuard::adopt(Arc::clone(&storage), registration, env.clone()).unwrap();
    assert!(time.wait_for_waiters(1, wait));
    // The heartbeat counts 30 polls of 30 s, then refreshes.
    for _ in 0..30 {
        time.advance(std::time::Duration::from_secs(30));
        assert!(time.wait_for_waiters(1, wait));
    }
    let expected = (start + Duration::minutes(15)).timestamp();
    let real = std::time::Instant::now();
    loop {
        if let Some(data) = storage.get(&gen1).unwrap() {
            let entry: SessionEntry = serde_json::from_slice(&data).unwrap();
            let refreshed = chrono::DateTime::parse_from_rfc3339(&entry.last_refresh)
                .unwrap()
                .timestamp();
            assert_eq!(refreshed, expected, "{entry:?}");
            assert_eq!(env.now_utc().timestamp(), expected);
            break;
        }
        assert!(real.elapsed() < wait, "marker not refreshed");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    // Generation 0 stays until the next refresh: two keys overlap.
    assert!(storage.exists(&gen0).unwrap());

    drop(guard);
    assert!(
        storage.list("sessions/").unwrap().is_empty(),
        "SessionGuard::drop must deregister every key after joining the heartbeat thread"
    );
    assert!(time.waiters().is_empty(), "heartbeat thread still waits");
}

/// A4: a refresh whose PUT failed is retried at the next poll, not after
/// another interval.
#[test]
fn heartbeat_retries_a_failed_put_at_the_next_poll() {
    let storage = Arc::new(FaultyBackend::default());
    let (time, env) = sim_env();
    let wait = std::time::Duration::from_secs(5);
    let registration = register_session(storage.as_ref(), "retry", &env).unwrap();
    let gen1 = session_marker_key_at("retry", 1);
    let gen2 = session_marker_key_at("retry", 2);
    storage.faults().put_lands_then_fails.insert(gen1.clone());
    let guard = SessionGuard::adopt(storage.clone(), registration, env.clone()).unwrap();
    let appears = |key: &str| {
        let real = std::time::Instant::now();
        while !storage.inner.exists(key).unwrap() {
            assert!(real.elapsed() < wait, "{key} not written");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    };
    assert!(time.wait_for_waiters(1, wait));
    for _ in 0..30 {
        time.advance(HEARTBEAT_POLL);
        assert!(time.wait_for_waiters(1, wait));
    }
    appears(&gen1);
    assert!(time.wait_for_waiters(1, wait));
    assert_eq!(
        guard.marker().report(env.clock.as_ref()).failed_refreshes,
        1
    );
    time.advance(HEARTBEAT_POLL);
    appears(&gen2);
    assert!(time.wait_for_waiters(1, wait));
    assert_eq!(guard.marker().current().0, 2);
    drop(guard);
    assert_eq!(session_keys(storage.as_ref()), Vec::<String>::new());
}

// ── acquire_lock_with_deadline ───────────────────────────────────────────

mod deadline {
    use crate::repo::HostEnv;
    use crate::repo::control::CancelToken;
    use crate::repo::lock::{
        COMMIT_LOCK_MAX_WAIT, acquire_lock, acquire_lock_with_deadline, release_lock,
    };
    use crate::sim::env::{SimTime, TimeMode};
    use crate::testutil::MemoryBackend;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use vykar_storage::StorageBackend;
    use vykar_types::error::VykarError;

    #[test]
    fn deadline_waits_for_release() {
        let storage = Arc::new(MemoryBackend::new());
        let held = acquire_lock(&*storage, &HostEnv::system()).unwrap();
        let releaser = {
            let storage = Arc::clone(&storage);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(1));
                release_lock(&*storage, held).unwrap();
            })
        };

        let mut contended = 0;
        let guard = acquire_lock_with_deadline(
            &*storage,
            &HostEnv::system(),
            Duration::from_secs(30),
            100,
            &CancelToken::new(),
            |_| contended += 1,
        )
        .unwrap();
        releaser.join().unwrap();
        assert_eq!(contended, 1);
        release_lock(&*storage, guard).unwrap();
    }

    #[test]
    fn deadline_times_out_after_budget() {
        let storage = MemoryBackend::new();
        let _held = acquire_lock(&storage, &HostEnv::system()).unwrap();
        let time = SimTime::new(chrono::Utc::now(), TimeMode::Auto);
        let env = HostEnv::system().with_clock(Arc::new(time.clock(chrono::Duration::zero())));
        let start = Instant::now();
        let err = acquire_lock_with_deadline(
            &storage,
            &env,
            COMMIT_LOCK_MAX_WAIT,
            500,
            &CancelToken::new(),
            |_| {},
        )
        .unwrap_err();
        assert!(matches!(err, VykarError::Locked(_)), "got {err:?}");
        assert!(
            time.elapsed() >= COMMIT_LOCK_MAX_WAIT,
            "{:?}",
            time.elapsed()
        );
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "{:?}",
            start.elapsed()
        );
    }

    #[test]
    fn deadline_interrupted_before_first_attempt() {
        let storage = MemoryBackend::new();
        let flag = CancelToken::new();
        flag.cancel();
        let err = acquire_lock_with_deadline(
            &storage,
            &HostEnv::system(),
            Duration::from_secs(30),
            500,
            &flag,
            |_| {},
        )
        .unwrap_err();
        assert!(matches!(err, VykarError::Interrupted), "got {err:?}");
        assert!(storage.list("locks/").unwrap().is_empty());
    }

    #[test]
    fn deadline_interrupted_during_contention() {
        let storage = MemoryBackend::new();
        let _held = acquire_lock(&storage, &HostEnv::system()).unwrap();
        let flag = CancelToken::new();
        let setter = {
            let flag = flag.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(200));
                flag.cancel();
            })
        };
        let start = Instant::now();
        let err = acquire_lock_with_deadline(
            &storage,
            &HostEnv::system(),
            Duration::from_secs(600),
            500,
            &flag,
            |_| {},
        )
        .unwrap_err();
        setter.join().unwrap();
        assert!(matches!(err, VykarError::Interrupted), "got {err:?}");
        assert!(
            start.elapsed() < Duration::from_millis(1200),
            "{:?}",
            start.elapsed()
        );
    }
}

// ── Clock-independent exclusion ──────────────────────────────────────────

/// The interleaving tests of this module live on the simulator
/// (src/tests/sim_exclusion.rs). These stay: three single-client tests that
/// assert on one call's requests, where a gate adds nothing; and
/// lock_is_mutually_exclusive_under_threads, a free-running contention smoke
/// test with real-time jitter, which scripted pause points cannot express
/// (08 step 4 explores orderings systematically).
mod exclusion {
    use super::{lock_key_at, write_lock_marker};
    use crate::repo::HostEnv;
    use crate::repo::lock::{
        acquire_lock, register_session_inner, release_lock, session_marker_key,
    };
    use crate::testutil::{MemoryBackend, RecordingBackend};
    use chrono::Utc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    use vykar_storage::StorageBackend;
    use vykar_types::error::VykarError;

    fn lock_puts(entries: &[String]) -> usize {
        entries.iter().filter(|k| k.starts_with("locks/")).count()
    }

    fn foreign_lock(storage: &dyn StorageBackend, ts: chrono::DateTime<Utc>) -> String {
        let key = lock_key_at(ts, "foreign");
        write_lock_marker(storage, &key, "otherhost", 4242, Utc::now(), None);
        key
    }

    fn locked_key(err: VykarError) -> String {
        match err {
            VykarError::Locked(holder) => holder.key,
            other => panic!("expected Locked, got {other:?}"),
        }
    }

    /// A holder keyed in the future (its clock is ahead of ours) must still
    /// block us: ordering by key would let us "win".
    #[test]
    fn newer_foreign_key_blocks_acquisition() {
        let storage = MemoryBackend::new();
        let foreign = foreign_lock(&storage, Utc::now() + chrono::Duration::minutes(1));

        assert_eq!(
            locked_key(acquire_lock(&storage, &HostEnv::system()).unwrap_err()),
            foreign
        );
        assert_eq!(storage.list("locks/").unwrap(), vec![foreign]);
    }

    /// No PUT at all on a backend without mtimes (no clock probe); on a
    /// real backend the property is "no lock key is PUT"
    /// (`contended_acquire_writes_only_a_clock_probe`).
    #[test]
    fn contended_acquire_issues_no_put() {
        let (storage, log) = RecordingBackend::new();
        foreign_lock(&storage, Utc::now() - chrono::Duration::minutes(1));
        log.clear();

        assert!(matches!(
            acquire_lock(&storage, &HostEnv::system()),
            Err(VykarError::Locked(_))
        ));
        assert_eq!(lock_puts(&log.entries()), 0, "{:?}", log.entries());
    }

    /// No lock PUT by a registration; on a real backend the probe reads
    /// "now" from the marker it just wrote, so it writes no clock probe
    /// either.
    #[test]
    fn register_session_probe_issues_no_lock_put() {
        let (storage, log) = RecordingBackend::new();
        register_session_inner(&storage, "free", 1, 10, &HostEnv::system()).unwrap();
        assert!(storage.exists(&session_marker_key("free")).unwrap());
        assert_eq!(lock_puts(&log.entries()), 0, "{:?}", log.entries());

        let held = acquire_lock(&storage, &HostEnv::system()).unwrap();
        log.clear();
        let err =
            register_session_inner(&storage, "blocked", 1, 10, &HostEnv::system()).unwrap_err();
        assert_eq!(locked_key(err), held.key());
        assert!(!storage.exists(&session_marker_key("blocked")).unwrap());
        assert_eq!(lock_puts(&log.entries()), 0, "{:?}", log.entries());
        release_lock(&storage, held).unwrap();
    }

    /// Unforced smoke test: mutual exclusion and liveness under contention.
    #[test]
    fn lock_is_mutually_exclusive_under_threads() {
        let storage = Arc::new(MemoryBackend::new());
        let inside = Arc::new(AtomicUsize::new(0));
        let max_inside = Arc::new(AtomicUsize::new(0));
        let deadline = Instant::now() + Duration::from_secs(30);

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let (storage, inside, max_inside) = (
                    Arc::clone(&storage),
                    Arc::clone(&inside),
                    Arc::clone(&max_inside),
                );
                std::thread::spawn(move || {
                    loop {
                        match acquire_lock(&*storage, &HostEnv::system()) {
                            Ok(guard) => {
                                let n = inside.fetch_add(1, Ordering::SeqCst) + 1;
                                max_inside.fetch_max(n, Ordering::SeqCst);
                                std::thread::sleep(Duration::from_millis(2));
                                inside.fetch_sub(1, Ordering::SeqCst);
                                release_lock(&*storage, guard).unwrap();
                                return;
                            }
                            Err(VykarError::Locked(_)) => {
                                assert!(Instant::now() < deadline, "thread starved");
                                std::thread::sleep(Duration::from_micros(
                                    rand::random::<u64>() % 3000,
                                ));
                            }
                            Err(e) => panic!("unexpected error: {e}"),
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(max_inside.load(Ordering::SeqCst), 1);
        assert!(storage.list("locks/").unwrap().is_empty());
    }
}

/// Ages on the store's clock (04 step 5): mtimes decide, the timestamps
/// inside the entries do not. On a real `LocalBackend`, mtimes set by hand.
mod store_clock {
    use super::{lock_key_at, write_lock_marker};
    use crate::repo::HostEnv;
    use crate::repo::lock::{
        SessionEntry, acquire_lock, cleanup_stale_sessions, default_stale_session_duration,
        release_lock,
    };
    use crate::sim::{FaultAction, FaultRule, KeyMatch, OpMatch, SimConfig, SimStore};
    use chrono::Utc;
    use std::path::Path;
    use std::time::{Duration, SystemTime};
    use vykar_storage::StorageBackend;
    use vykar_storage::local_backend::LocalBackend;
    use vykar_types::error::VykarError;

    const HOUR: Duration = Duration::from_secs(3600);

    fn local() -> (tempfile::TempDir, LocalBackend) {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        (dir, backend)
    }

    /// Set `key`'s mtime `age` before now.
    fn age(dir: &Path, key: &str, age: Duration) {
        std::fs::File::options()
            .write(true)
            .open(dir.join(key))
            .unwrap()
            .set_modified(SystemTime::now() - age)
            .unwrap();
    }

    fn probes(storage: &dyn StorageBackend) -> Vec<String> {
        storage
            .list("locks/")
            .unwrap()
            .into_iter()
            .filter(|k| k.starts_with("locks/vykar-clock-"))
            .collect()
    }

    fn marker(storage: &dyn StorageBackend, id: &str, last_refresh: chrono::DateTime<Utc>) {
        let entry = SessionEntry {
            hostname: "otherhost".into(),
            pid: 4242,
            registered_at: last_refresh.to_rfc3339(),
            last_refresh: last_refresh.to_rfc3339(),
            boot_id: None,
            installation_id: None,
            pid_ns: None,
        };
        storage
            .put(
                &format!("sessions/{id}.json"),
                &serde_json::to_vec(&entry).unwrap(),
            )
            .unwrap();
    }

    #[test]
    fn lock_age_is_the_objects_mtime() {
        // Written just now by its JSON, 7 h old by its mtime: reaped.
        let (dir, storage) = local();
        let key = lock_key_at(Utc::now(), "foreign");
        write_lock_marker(&storage, &key, "otherhost", 4242, Utc::now(), None);
        age(dir.path(), &key, 7 * HOUR);
        let guard = acquire_lock(&storage, &HostEnv::system()).unwrap();
        assert!(!storage.exists(&key).unwrap());
        assert!(guard.store_time().is_some());
        release_lock(&storage, guard).unwrap();
        assert_eq!(probes(&storage), Vec::<String>::new());

        // 7 h old by its JSON and its key, fresh by its mtime: it blocks.
        let (_dir, storage) = local();
        let old = Utc::now() - chrono::Duration::hours(7);
        let key = lock_key_at(old, "foreign");
        write_lock_marker(&storage, &key, "otherhost", 4242, old, None);
        match acquire_lock(&storage, &HostEnv::system()) {
            Err(VykarError::Locked(holder)) => assert_eq!(holder.key, key),
            other => panic!("expected Locked, got {other:?}"),
        }
        assert!(storage.exists(&key).unwrap());
        assert_eq!(probes(&storage), Vec::<String>::new());
    }

    #[test]
    fn marker_age_is_the_objects_mtime() {
        let (dir, storage) = local();
        // Fresh by its JSON, 46 min old by its mtime.
        marker(&storage, "aged", Utc::now());
        age(
            dir.path(),
            "sessions/aged.json",
            Duration::from_secs(46 * 60),
        );
        // 2 h old by its JSON, fresh by its mtime.
        marker(&storage, "live", Utc::now() - chrono::Duration::hours(2));
        let env = HostEnv::system();
        let guard = acquire_lock(&storage, &env).unwrap();
        let cleaned = cleanup_stale_sessions(
            &storage,
            default_stale_session_duration(),
            &env,
            guard.store_time(),
        )
        .unwrap();
        assert_eq!(cleaned, vec!["aged".to_string()]);
        assert!(!storage.exists("sessions/aged.json").unwrap());
        assert!(storage.exists("sessions/live.json").unwrap());
        release_lock(&storage, guard).unwrap();
    }

    #[test]
    fn malformed_marker_is_reaped_once_old_by_mtime() {
        let (dir, storage) = local();
        storage.put("sessions/x.json", b"{").unwrap();
        storage.put("sessions/x.index", b"journal").unwrap();
        let env = HostEnv::system();
        let guard = acquire_lock(&storage, &env).unwrap();
        let max_age = default_stale_session_duration();

        // Young: kept, it blocks maintenance.
        let cleaned = cleanup_stale_sessions(&storage, max_age, &env, guard.store_time()).unwrap();
        assert!(cleaned.is_empty(), "{cleaned:?}");
        assert!(storage.exists("sessions/x.json").unwrap());

        // 46 min old by its mtime: reaped; its journal keeps one run's grace.
        age(dir.path(), "sessions/x.json", Duration::from_secs(46 * 60));
        let cleaned = cleanup_stale_sessions(&storage, max_age, &env, guard.store_time()).unwrap();
        assert_eq!(cleaned, vec!["x".to_string()]);
        assert!(!storage.exists("sessions/x.json").unwrap());
        assert!(storage.exists("sessions/x.index").unwrap());

        // Without a reading of "now" nothing is stale by age.
        storage.put("sessions/y.json", b"{").unwrap();
        age(dir.path(), "sessions/y.json", 2 * HOUR);
        let cleaned = cleanup_stale_sessions(&storage, max_age, &env, None).unwrap();
        assert!(cleaned.is_empty(), "{cleaned:?}");
        assert!(storage.exists("sessions/y.json").unwrap());
        release_lock(&storage, guard).unwrap();
    }

    #[test]
    fn clock_probe_debris_is_removed_after_an_hour() {
        let (dir, storage) = local();
        let live = lock_key_at(Utc::now(), "foreign");
        write_lock_marker(&storage, &live, "otherhost", 4242, Utc::now(), None);
        let old = "locks/vykar-clock-0123456789abcdef0123456789abcdef";
        let young = "locks/vykar-clock-fedcba9876543210fedcba9876543210";
        storage.put(old, b"vykar clock probe").unwrap();
        storage.put(young, b"vykar clock probe").unwrap();
        age(dir.path(), old, 2 * HOUR);
        age(dir.path(), young, Duration::from_secs(10 * 60));

        assert!(matches!(
            acquire_lock(&storage, &HostEnv::system()),
            Err(VykarError::Locked(_))
        ));
        assert!(storage.exists(&live).unwrap());
        assert_eq!(probes(&storage), vec![young.to_string()]);
    }

    /// `break-lock` also removes clock probes, which are not locks and are
    /// not counted.
    #[test]
    fn break_lock_removes_clock_probes_uncounted() {
        let storage = crate::testutil::MemoryBackend::new();
        let key = lock_key_at(Utc::now(), "foreign");
        write_lock_marker(&storage, &key, "otherhost", 4242, Utc::now(), None);
        storage
            .put(
                "locks/vykar-clock-0123456789abcdef0123456789abcdef",
                b"vykar clock probe",
            )
            .unwrap();
        assert_eq!(crate::repo::lock::break_lock(&storage).unwrap(), 1);
        assert!(storage.list("locks/").unwrap().is_empty());
    }

    /// Two clients of one SimStore on distinct hosts.
    fn sim_pair() -> (
        SimStore,
        crate::sim::SimClient,
        crate::sim::SimClient,
        HostEnv,
        HostEnv,
    ) {
        let store = SimStore::new(SimConfig::default());
        let (a, b) = (store.register_client("a"), store.register_client("b"));
        let env = |name: &str| {
            HostEnv::system().with_host(crate::repo::HostIdentity {
                hostname: format!("host-{name}"),
                pid: 1,
                boot_id: None,
                installation_id: None,
                pid_ns: None,
            })
        };
        (
            store.clone(),
            store.handle(a),
            store.handle(b),
            env("a"),
            env("b"),
        )
    }

    /// With a live lock listed, a contended acquire writes and deletes one
    /// clock probe under `locks/` and never a lock key.
    #[test]
    fn contended_acquire_writes_only_a_clock_probe() {
        let (store, ha, hb, env_a, env_b) = sim_pair();
        let held = acquire_lock(&ha, &env_a).unwrap();
        let from = store.log().len();
        assert!(matches!(
            acquire_lock(&hb, &env_b),
            Err(VykarError::Locked(_))
        ));
        let log = store.log();
        let ops: Vec<_> = log[from..]
            .iter()
            .filter(|r| r.client == hb.client() && r.key.starts_with("locks/"))
            .filter(|r| r.op.is_mutation())
            .collect();
        assert_eq!(ops.len(), 2, "{ops:?}");
        assert!(
            ops[0].op.is_write() && ops[0].key.starts_with("locks/vykar-clock-"),
            "{ops:?}"
        );
        assert!(ops[1].op.is_delete() && ops[1].key == ops[0].key, "{ops:?}");
        assert!(store.keys("locks/vykar-clock-").is_empty());
        release_lock(&ha, held).unwrap();
    }

    /// A probe that fails to write is "now unknown": the attempt reaps
    /// nothing by age and ends `Locked`, not with the storage error; a
    /// later attempt goes through.
    #[test]
    fn failed_clock_probe_reads_as_locked() {
        let (store, ha, hb, env_a, env_b) = sim_pair();
        store
            .add_fault(
                FaultRule::new(
                    OpMatch::AnyWrite,
                    KeyMatch::Prefix("locks/vykar-clock-".into()),
                    FaultAction::Fail,
                )
                .client(hb.client()),
            )
            .unwrap();
        let held = acquire_lock(&ha, &env_a).unwrap();
        assert!(matches!(
            acquire_lock(&hb, &env_b),
            Err(VykarError::Locked(_))
        ));
        assert_eq!(store.keys("locks/").len(), 1);
        release_lock(&ha, held).unwrap();
        let guard = acquire_lock(&hb, &env_b).unwrap();
        release_lock(&hb, guard).unwrap();
    }

    /// Two clients of one SimStore on distinct hosts, on a virtual clock
    /// the store's clock follows; b's environment runs on it.
    fn sim_pair_timed() -> (
        SimStore,
        std::sync::Arc<crate::sim::SimTime>,
        crate::sim::SimClient,
        crate::sim::SimClient,
        HostEnv,
    ) {
        let (store, ha, hb, _, env_b) = sim_pair();
        let time = crate::sim::SimTime::for_store(&store, crate::sim::TimeMode::Auto);
        let env_b = env_b.with_clock(std::sync::Arc::new(time.clock(chrono::Duration::zero())));
        (store, time, ha, hb, env_b)
    }

    /// PUTs of clock probes by `client` from log position `from`.
    fn probe_puts(store: &SimStore, client: crate::sim::ClientId, from: usize) -> usize {
        store.log()[from..]
            .iter()
            .filter(|r| r.client == client && r.op.is_write())
            .filter(|r| r.key.starts_with("locks/vykar-clock-"))
            .count()
    }

    /// A wait on a live lock probes the store's clock once while its
    /// reading is younger than `STORE_READING_MAX_AGE`, and once more per
    /// `STORE_READING_MAX_AGE` after; every probe uses one key.
    #[test]
    fn lock_wait_probes_once_per_reading() {
        use crate::repo::ages::STORE_READING_MAX_AGE;
        use crate::repo::control::CancelToken;
        use crate::repo::lock::acquire_lock_with_deadline;

        let (store, time, ha, hb, env_b) = sim_pair_timed();
        let live = lock_key_at(store.now(), "foreign");
        write_lock_marker(&ha, &live, "otherhost", 4242, store.now(), None);
        let from = store.log().len();
        let mut attempts = 0;
        let wait = STORE_READING_MAX_AGE - Duration::from_secs(30);
        let err = acquire_lock_with_deadline(&hb, &env_b, wait, 500, &CancelToken::new(), |_| {
            attempts += 1;
        })
        .unwrap_err();
        assert!(matches!(err, VykarError::Locked(_)), "{err:?}");
        assert!(time.elapsed() >= wait);
        let lists = store.log()[from..]
            .iter()
            .filter(|r| r.client == hb.client() && r.key == "locks/" && !r.op.is_mutation())
            .count();
        assert!(lists > 5, "few attempts: {lists} listings of locks/");
        assert_eq!(attempts, 1, "on_contended once");
        assert_eq!(probe_puts(&store, hb.client(), from), 1);

        // A 10-minute wait re-probes after each STORE_READING_MAX_AGE, on
        // the key of the first probe.
        let from = store.log().len();
        let err = acquire_lock_with_deadline(
            &hb,
            &env_b,
            crate::repo::lock::COMMIT_LOCK_MAX_WAIT,
            500,
            &CancelToken::new(),
            |_| {},
        )
        .unwrap_err();
        assert!(matches!(err, VykarError::Locked(_)), "{err:?}");
        let probes = probe_puts(&store, hb.client(), from);
        assert!((2..=3).contains(&probes), "{probes} probes");
        let keys: std::collections::BTreeSet<_> = store
            .log()
            .iter()
            .filter(|r| r.client == hb.client() && r.key.starts_with("locks/vykar-clock-"))
            .map(|r| r.key.clone())
            .collect();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert!(store.keys("locks/vykar-clock-").is_empty());
        assert!(store.object(&live).is_some());
    }

    /// A lock that passes the stale age during a wait is reaped by a later
    /// attempt through the extrapolated reading, without a second probe.
    #[test]
    fn lock_aging_during_a_wait_is_reaped_by_extrapolation() {
        use crate::repo::control::CancelToken;
        use crate::repo::lock::acquire_lock_with_deadline;

        let (store, time, ha, hb, env_b) = sim_pair_timed();
        let aged = lock_key_at(store.now(), "foreign");
        write_lock_marker(&ha, &aged, "otherhost", 4242, store.now(), None);
        // 30 s short of the 6 h stale age when b's wait starts.
        time.advance(6 * HOUR - Duration::from_secs(30));
        let from = store.log().len();
        let mut contended = 0;
        let guard = acquire_lock_with_deadline(
            &hb,
            &env_b,
            Duration::from_secs(4 * 60),
            500,
            &CancelToken::new(),
            |_| contended += 1,
        )
        .unwrap();
        assert_eq!(contended, 1, "the first attempt saw the lock live");
        assert!(store.object(&aged).is_none(), "reaped");
        assert_eq!(probe_puts(&store, hb.client(), from), 1);
        release_lock(&hb, guard).unwrap();
    }

    /// An uncontended acquisition probes nothing but lists the old probes
    /// for `LockGuard::remove_probe_debris`; a young probe is left.
    #[test]
    fn uncontended_acquire_removes_probe_debris() {
        let (dir, storage) = local();
        let old = "locks/vykar-clock-0123456789abcdef0123456789abcdef";
        let young = "locks/vykar-clock-fedcba9876543210fedcba9876543210";
        storage.put(old, b"vykar clock probe").unwrap();
        storage.put(young, b"vykar clock probe").unwrap();
        age(dir.path(), old, 2 * HOUR);
        age(dir.path(), young, Duration::from_secs(10 * 60));
        let guard = acquire_lock(&storage, &HostEnv::system()).unwrap();
        assert_eq!(probes(&storage).len(), 2, "acquisition deletes nothing");
        guard.remove_probe_debris(&storage);
        assert_eq!(probes(&storage), vec![young.to_string()]);
        release_lock(&storage, guard).unwrap();
    }
}
