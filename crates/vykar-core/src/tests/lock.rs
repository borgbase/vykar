use crate::repo::lock::{acquire_lock, break_lock, cleanup_stale_sessions, release_lock};
use crate::testutil::{LockableMemoryBackend, MemoryBackend};
use chrono::{Duration, Utc};
use vykar_storage::StorageBackend;

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

// --- Backend-native advisory lock tests ---

#[test]
fn backend_lock_acquire_and_release() {
    let storage = LockableMemoryBackend::new();
    let guard = acquire_lock(&storage).unwrap();
    // Backend lock uses the lock_id, not a locks/ key
    assert_eq!(guard.key(), "repo-lock");
    release_lock(&storage, guard).unwrap();
}

#[test]
fn backend_lock_second_acquire_is_rejected() {
    let storage = LockableMemoryBackend::new();
    let first = acquire_lock(&storage).unwrap();
    let second = acquire_lock(&storage);
    assert!(second.is_err(), "second lock should fail");
    release_lock(&storage, first).unwrap();
}

#[test]
fn break_lock_removes_backend_lock() {
    let storage = LockableMemoryBackend::new();
    let guard = acquire_lock(&storage).unwrap();
    std::mem::forget(guard);

    let removed = break_lock(&storage).unwrap();
    assert_eq!(removed, 1);

    // Should be able to acquire again after break
    let guard = acquire_lock(&storage).unwrap();
    release_lock(&storage, guard).unwrap();
}

#[test]
fn break_lock_returns_zero_when_no_backend_lock_held() {
    let storage = LockableMemoryBackend::new();
    let removed = break_lock(&storage).unwrap();
    assert_eq!(removed, 0);
}

// --- cleanup_stale_sessions tests ---

/// Helper: write a session marker with the given last_refresh timestamp.
fn write_session_marker(storage: &MemoryBackend, session_id: &str, last_refresh: &str) {
    let key = format!("sessions/{session_id}.json");
    let entry = crate::repo::lock::SessionEntry {
        hostname: "test".to_string(),
        pid: 1,
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

    let cleaned = cleanup_stale_sessions(&storage, Duration::hours(72)).unwrap();
    assert!(cleaned.is_empty());
    assert!(storage.exists("sessions/sess1.json").unwrap());
    assert!(
        storage.exists("sessions/sess1.index").unwrap(),
        ".index should be preserved for active session"
    );
}

#[test]
fn cleanup_stale_sessions_removes_stale_marker_and_index() {
    let storage = MemoryBackend::new();
    let old = (Utc::now() - Duration::hours(100)).to_rfc3339();

    // Stale session with companion .index.
    write_session_marker(&storage, "sess2", &old);
    storage
        .put("sessions/sess2.index", b"journal-data")
        .unwrap();

    let cleaned = cleanup_stale_sessions(&storage, Duration::hours(72)).unwrap();
    assert_eq!(cleaned, vec!["sess2"]);
    assert!(!storage.exists("sessions/sess2.json").unwrap());
    assert!(
        !storage.exists("sessions/sess2.index").unwrap(),
        ".index should be deleted with stale session"
    );
}

#[test]
fn cleanup_stale_sessions_removes_orphaned_index() {
    let storage = MemoryBackend::new();

    // Orphaned .index with no companion .json marker.
    storage
        .put("sessions/orphan.index", b"journal-data")
        .unwrap();

    let cleaned = cleanup_stale_sessions(&storage, Duration::hours(72)).unwrap();
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

    let cleaned = cleanup_stale_sessions(&storage, Duration::hours(72)).unwrap();
    assert!(cleaned.is_empty());
    assert!(
        storage.exists("sessions/active.index").unwrap(),
        ".index should not be treated as unparseable .json"
    );
}
