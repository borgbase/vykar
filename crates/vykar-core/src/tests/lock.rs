use crate::repo::lock::{
    acquire_lock, break_lock, cleanup_stale_locks_inner, cleanup_stale_sessions,
    clear_all_sessions, default_stale_session_duration, refresh_session, register_session,
    release_lock, session_marker_key, SessionEntry, SessionGuard, SESSION_STALE_SECS,
};
use crate::testutil::MemoryBackend;
use chrono::{Duration, Utc};
use std::sync::Arc;
use vykar_storage::StorageBackend;

/// Stub pid_alive function that always returns true (conservative default).
fn pid_always_alive(_pid: u32) -> bool {
    true
}

#[test]
fn acquire_and_release_lock() {
    let storage = MemoryBackend::new();
    let guard = acquire_lock(&storage).unwrap();
    let key = guard.key().to_string();

    // Lock key should exist in storage
    assert!(storage.exists(&key).unwrap());

    // Release should remove it
    release_lock(&storage, guard).unwrap();
    assert!(!storage.exists(&key).unwrap());
}

#[test]
fn lock_key_in_locks_directory() {
    let storage = MemoryBackend::new();
    let guard = acquire_lock(&storage).unwrap();
    assert!(guard.key().starts_with("locks/"));
    assert!(guard.key().ends_with(".json"));
    release_lock(&storage, guard).unwrap();
}

#[test]
fn second_lock_is_rejected() {
    let storage = MemoryBackend::new();
    let first = acquire_lock(&storage).unwrap();

    let second = acquire_lock(&storage);
    assert!(second.is_err(), "second lock acquisition should fail");
    let msg = second.unwrap_err().to_string();
    assert!(msg.contains("locked"), "unexpected error: {msg}");

    release_lock(&storage, first).unwrap();
}

#[test]
fn stale_lock_is_cleaned_up() {
    let storage = MemoryBackend::new();
    let stale_key = "locks/00000000000000000000-stale.json";
    let stale_time = (Utc::now() - Duration::hours(7)).to_rfc3339();
    let stale_entry = format!(r#"{{"hostname":"old","pid":1234,"time":"{stale_time}"}}"#);
    storage.put(stale_key, stale_entry.as_bytes()).unwrap();
    assert!(storage.exists(stale_key).unwrap());

    let guard = acquire_lock(&storage).unwrap();
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
    storage: &MemoryBackend,
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
fn stale_lock_cleanup_removes_same_host_older_boot() {
    let storage = MemoryBackend::new();
    let key = lock_key_at(Utc::now(), "old-boot");
    write_lock_marker(&storage, &key, "myhost", 123, Utc::now(), Some("boot-a"));

    cleanup_stale_locks_inner(
        &storage,
        Duration::hours(6),
        "myhost",
        Some("boot-b"),
        |_| true,
    )
    .unwrap();

    assert!(!storage.exists(&key).unwrap());
}

#[test]
fn stale_lock_cleanup_removes_dead_same_host_pid() {
    let storage = MemoryBackend::new();
    let key = lock_key_at(Utc::now(), "dead-pid");
    write_lock_marker(&storage, &key, "myhost", 123, Utc::now(), Some("boot-a"));

    cleanup_stale_locks_inner(
        &storage,
        Duration::hours(6),
        "myhost",
        Some("boot-a"),
        |_| false,
    )
    .unwrap();

    assert!(!storage.exists(&key).unwrap());
}

#[test]
fn stale_lock_cleanup_preserves_live_same_host_pid() {
    let storage = MemoryBackend::new();
    let key = lock_key_at(Utc::now(), "live-pid");
    write_lock_marker(&storage, &key, "myhost", 123, Utc::now(), Some("boot-a"));

    cleanup_stale_locks_inner(
        &storage,
        Duration::hours(6),
        "myhost",
        Some("boot-a"),
        |_| true,
    )
    .unwrap();

    assert!(storage.exists(&key).unwrap());
}

#[test]
fn stale_lock_cleanup_preserves_recent_foreign_host() {
    let storage = MemoryBackend::new();
    let key = lock_key_at(Utc::now(), "foreign");
    write_lock_marker(&storage, &key, "otherhost", 123, Utc::now(), Some("boot-a"));

    cleanup_stale_locks_inner(
        &storage,
        Duration::hours(6),
        "myhost",
        Some("boot-b"),
        |_| false,
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

    cleanup_stale_locks_inner(
        &storage,
        Duration::hours(6),
        "myhost",
        Some("boot-a"),
        |_| true,
    )
    .unwrap();

    assert!(!storage.exists(&key).unwrap());
}

#[test]
fn stale_lock_cleanup_preserves_recent_old_format_lock() {
    let storage = MemoryBackend::new();
    let key = lock_key_at(Utc::now(), "old-format");
    write_lock_marker(&storage, &key, "otherhost", 123, Utc::now(), None);

    cleanup_stale_locks_inner(
        &storage,
        Duration::hours(6),
        "myhost",
        Some("boot-a"),
        |_| false,
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

    cleanup_stale_locks_inner(
        &storage,
        Duration::hours(6),
        "myhost",
        Some("boot-a"),
        |_| false,
    )
    .unwrap();

    assert!(!storage.exists(&key).unwrap());
}

#[test]
fn break_lock_removes_all_locks() {
    let storage = MemoryBackend::new();

    // Acquire a lock and "forget" the guard (simulates a killed process)
    let guard = acquire_lock(&storage).unwrap();
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
        "other-host",
        pid_always_alive,
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
        "other-host",
        pid_always_alive,
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
        "other-host",
        pid_always_alive,
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
        "other-host",
        pid_always_alive,
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

    // Session from "myhost" with PID 42 — we'll inject a pid_alive that says it's dead.
    write_session_marker_with_host(&storage, "local-dead", &now, "myhost", 42);
    storage
        .put("sessions/local-dead.index", b"journal")
        .unwrap();

    let cleaned =
        cleanup_stale_sessions(&storage, Duration::hours(72), "myhost", |_pid| false).unwrap();
    assert_eq!(cleaned, vec!["local-dead"]);
    assert!(!storage.exists("sessions/local-dead.json").unwrap());
    assert!(
        storage.exists("sessions/local-dead.index").unwrap(),
        ".index should be preserved for recovery by next backup"
    );
}

#[test]
fn cleanup_preserves_alive_local_session() {
    let storage = MemoryBackend::new();
    let now = Utc::now().to_rfc3339();

    // Session from "myhost" with PID 42 — pid_alive says it's alive.
    write_session_marker_with_host(&storage, "local-alive", &now, "myhost", 42);

    let cleaned =
        cleanup_stale_sessions(&storage, Duration::hours(72), "myhost", |_pid| true).unwrap();
    assert!(cleaned.is_empty());
    assert!(storage.exists("sessions/local-alive.json").unwrap());
}

#[test]
fn cleanup_does_not_remove_remote_dead_pid() {
    let storage = MemoryBackend::new();
    let now = Utc::now().to_rfc3339();

    // Session from "remote-host" with PID 42 — even though pid_alive returns
    // false, we cannot verify remote PIDs, so the session must survive.
    write_session_marker_with_host(&storage, "remote-sess", &now, "remote-host", 42);

    let cleaned =
        cleanup_stale_sessions(&storage, Duration::hours(72), "myhost", |_pid| false).unwrap();
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
        "other-host",
        pid_always_alive,
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
        "other-host",
        pid_always_alive,
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

// --- Lock display and S3 eventual consistency tests ---

#[test]
fn lock_contention_shows_hostname_and_pid() {
    let storage = MemoryBackend::new();
    let first = acquire_lock(&storage).unwrap();

    let second = acquire_lock(&storage);
    assert!(second.is_err());
    let err = second.unwrap_err();
    let msg = err.to_string();
    // Should contain hostname and PID, not a raw key path.
    assert!(
        msg.contains("PID"),
        "error should contain hostname/PID, got: {msg}"
    );
    assert!(
        !msg.contains("locks/"),
        "error should not contain raw key path, got: {msg}"
    );

    release_lock(&storage, first).unwrap();
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
        "other-host",
        pid_always_alive,
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
        "other-host",
        pid_always_alive,
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
        "other-host",
        pid_always_alive,
    )
    .unwrap();

    // Malformed markers are not reported as "cleaned" and must NOT be
    // deleted — they fail-close maintenance until an operator clears them.
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
        "other-host",
        pid_always_alive,
    )
    .unwrap();

    assert!(cleaned.is_empty());
    assert!(
        storage.exists("sessions/bad-ts.json").unwrap(),
        "marker with unparseable timestamp must survive cleanup"
    );
}

// --- Non-resurrecting refresh_session tests ---

#[test]
fn refresh_session_does_not_resurrect_missing_marker() {
    let storage = MemoryBackend::new();
    let key = session_marker_key("ghost");
    assert!(!storage.exists(&key).unwrap());

    // Refresh a session whose marker was already deleted (e.g. by maintenance).
    // Must NOT recreate the key — otherwise the marker would come back from
    // the dead and block the next maintenance run.
    refresh_session(&storage, "ghost");

    assert!(
        !storage.exists(&key).unwrap(),
        "refresh_session must not recreate a deleted marker"
    );
}

#[test]
fn refresh_session_updates_existing_marker() {
    let storage = MemoryBackend::new();
    let original = (Utc::now() - Duration::minutes(10)).to_rfc3339();
    write_session_marker(&storage, "live", &original);

    refresh_session(&storage, "live");

    let key = session_marker_key("live");
    let data = storage.get(&key).unwrap().unwrap();
    let entry: SessionEntry = serde_json::from_slice(&data).unwrap();
    assert_ne!(
        entry.last_refresh, original,
        "refresh_session must advance last_refresh on existing marker"
    );
    // registered_at should be preserved from the existing entry.
    assert_eq!(entry.registered_at, original);
}

// --- SessionGuard lifecycle tests ---

#[test]
fn session_guard_drop_stops_thread_before_deregister() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let session_id = format!("guard-{:016x}", rand::random::<u64>());

    register_session(storage.as_ref(), &session_id).unwrap();
    let key = session_marker_key(&session_id);
    assert!(storage.exists(&key).unwrap());

    let guard = SessionGuard::adopt(Arc::clone(&storage), session_id.clone()).unwrap();
    drop(guard);

    // After Drop completes, the marker must be gone AND the heartbeat thread
    // must not race back in to recreate it. Give the scheduler a moment and
    // re-check — if the (non-resurrecting) refresh fires after deregister,
    // the key stays gone.
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(
        !storage.exists(&key).unwrap(),
        "SessionGuard::drop must deregister after joining the heartbeat thread"
    );
}
