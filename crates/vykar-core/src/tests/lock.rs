use crate::repo::lock::{
    acquire_lock, break_lock, cleanup_stale_sessions, clear_all_sessions, release_lock,
};
use crate::testutil::MemoryBackend;
use chrono::{Duration, Utc};
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
