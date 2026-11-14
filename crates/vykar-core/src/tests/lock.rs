use crate::repo::lock::{
    acquire_lock, break_lock, cleanup_stale_locks_inner, cleanup_stale_sessions,
    clear_all_sessions, default_stale_session_duration, format_age, format_age_since,
    refresh_session, register_session, release_lock, session_marker_key, SessionEntry,
    SessionGuard, SESSION_STALE_SECS,
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

/// The `Locked` message must be self-diagnosing: who holds the lock, how long
/// they have held it, and what to do about it (issue #174, where the message
/// named only host and PID and users could not tell a brief commit apart from
/// a long prune/compact on another machine).
#[test]
fn locked_error_names_holder_age_and_remedy() {
    let storage = MemoryBackend::new();
    let first = acquire_lock(&storage).unwrap();

    let msg = acquire_lock(&storage).unwrap_err().to_string();
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

    let msg = acquire_lock(&storage).unwrap_err().to_string();
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

// ── acquire_lock_with_deadline ───────────────────────────────────────────

mod deadline {
    use crate::repo::lock::{acquire_lock, acquire_lock_with_deadline, release_lock};
    use crate::testutil::MemoryBackend;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use vykar_storage::StorageBackend;
    use vykar_types::error::VykarError;

    #[test]
    fn deadline_waits_for_release() {
        let storage = Arc::new(MemoryBackend::new());
        let held = acquire_lock(&*storage).unwrap();
        let releaser = {
            let storage = Arc::clone(&storage);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(1));
                release_lock(&*storage, held).unwrap();
            })
        };

        let mut contended = 0;
        let guard =
            acquire_lock_with_deadline(&*storage, Duration::from_secs(30), 100, None, |_| {
                contended += 1
            })
            .unwrap();
        releaser.join().unwrap();
        assert_eq!(contended, 1);
        release_lock(&*storage, guard).unwrap();
    }

    #[test]
    fn deadline_times_out_after_budget() {
        let storage = MemoryBackend::new();
        let _held = acquire_lock(&storage).unwrap();
        let start = Instant::now();
        let err =
            acquire_lock_with_deadline(&storage, Duration::from_millis(300), 500, None, |_| {})
                .unwrap_err();
        let elapsed = start.elapsed();
        assert!(matches!(err, VykarError::Locked(_)), "got {err:?}");
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed <= Duration::from_secs(3),
            "elapsed {elapsed:?}"
        );
    }

    #[test]
    fn deadline_interrupted_before_first_attempt() {
        let storage = MemoryBackend::new();
        let flag = AtomicBool::new(true);
        let err =
            acquire_lock_with_deadline(&storage, Duration::from_secs(30), 500, Some(&flag), |_| {})
                .unwrap_err();
        assert!(matches!(err, VykarError::Interrupted), "got {err:?}");
        assert!(storage.list("locks/").unwrap().is_empty());
    }

    #[test]
    fn deadline_interrupted_during_contention() {
        let storage = MemoryBackend::new();
        let _held = acquire_lock(&storage).unwrap();
        let flag = Arc::new(AtomicBool::new(false));
        let setter = {
            let flag = Arc::clone(&flag);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(200));
                flag.store(true, Ordering::Relaxed);
            })
        };
        let start = Instant::now();
        let err = acquire_lock_with_deadline(
            &storage,
            Duration::from_secs(600),
            500,
            Some(&flag),
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

mod exclusion {
    use super::{lock_key_at, write_lock_marker};
    use crate::commands::util::with_maintenance_lock;
    use crate::config::ChunkerConfig;
    use crate::repo::lock::{
        acquire_lock, register_session_inner, release_lock, session_marker_key,
    };
    use crate::repo::{EncryptionMode, Repository};
    use crate::testutil::{HookBackend, MemoryBackend, Op, RecordingBackend};
    use chrono::Utc;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc, Barrier, Condvar, Mutex};
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

    fn on_thread(name: &str) -> bool {
        std::thread::current().name() == Some(name)
    }

    /// Parks the hooked thread once: signals `reached`, then blocks until
    /// [`Park::release`].
    struct Park {
        reached: Mutex<mpsc::Sender<()>>,
        open: Mutex<bool>,
        cv: Condvar,
        fired: AtomicBool,
    }

    impl Park {
        fn new() -> (Arc<Self>, mpsc::Receiver<()>) {
            let (tx, rx) = mpsc::channel();
            let park = Arc::new(Self {
                reached: Mutex::new(tx),
                open: Mutex::new(false),
                cv: Condvar::new(),
                fired: AtomicBool::new(false),
            });
            (park, rx)
        }

        fn park_once(&self) {
            if self.fired.swap(true, Ordering::SeqCst) {
                return;
            }
            let _ = self.reached.lock().unwrap().send(());
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.cv.wait(open).unwrap();
            }
        }

        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.cv.notify_all();
        }
    }

    fn repo_over(storage: HookBackend) -> Repository {
        crate::testutil::init_test_environment();
        Repository::init(
            Box::new(storage),
            EncryptionMode::None,
            ChunkerConfig::default(),
            None,
            None,
            None,
        )
        .unwrap()
    }

    /// A holder keyed in the future (its clock is ahead of ours) must still
    /// block us: ordering by key would let us "win".
    #[test]
    fn newer_foreign_key_blocks_acquisition() {
        let storage = MemoryBackend::new();
        let foreign = foreign_lock(&storage, Utc::now() + chrono::Duration::minutes(1));

        assert_eq!(locked_key(acquire_lock(&storage).unwrap_err()), foreign);
        assert_eq!(storage.list("locks/").unwrap(), vec![foreign]);
    }

    #[test]
    fn contended_acquire_issues_no_put() {
        let (storage, log) = RecordingBackend::new();
        foreign_lock(&storage, Utc::now() - chrono::Duration::minutes(1));
        log.clear();

        assert!(matches!(acquire_lock(&storage), Err(VykarError::Locked(_))));
        assert_eq!(lock_puts(&log.entries()), 0, "{:?}", log.entries());
    }

    #[test]
    fn register_session_probe_issues_no_lock_put() {
        let (storage, log) = RecordingBackend::new();
        register_session_inner(&storage, "free", 1, 10).unwrap();
        assert!(storage.exists(&session_marker_key("free")).unwrap());
        assert_eq!(lock_puts(&log.entries()), 0, "{:?}", log.entries());

        let held = acquire_lock(&storage).unwrap();
        log.clear();
        let err = register_session_inner(&storage, "blocked", 1, 10).unwrap_err();
        assert_eq!(locked_key(err), held.key());
        assert!(!storage.exists(&session_marker_key("blocked")).unwrap());
        assert_eq!(lock_puts(&log.entries()), 0, "{:?}", log.entries());
        release_lock(&storage, held).unwrap();
    }

    /// Both contenders pass the pre-check and PUT before either LISTs: each
    /// sees the other's key, so both must back off. DELETEs are held until both
    /// have listed (a thread reaching DELETE has finished its LIST), with a
    /// timeout so a winner fails the test instead of hanging it.
    #[test]
    fn simultaneous_acquires_both_back_off() {
        let inner = Arc::new(MemoryBackend::new());
        let put_barrier = Arc::new(Barrier::new(2));
        let list_barrier = Arc::new(Barrier::new(2));
        let deleting = Arc::new((Mutex::new(0usize), Condvar::new()));
        let wrote: Arc<Mutex<HashSet<std::thread::ThreadId>>> = Arc::default();
        let storage = Arc::new(HookBackend::new(Arc::clone(&inner), {
            let (put_barrier, list_barrier) = (Arc::clone(&put_barrier), Arc::clone(&list_barrier));
            move |op, key| {
                if !key.starts_with("locks/") {
                    return;
                }
                let me = std::thread::current().id();
                match op {
                    Op::Put => {
                        put_barrier.wait();
                        wrote.lock().unwrap().insert(me);
                    }
                    Op::List => {
                        let after_put = wrote.lock().unwrap().remove(&me);
                        if after_put {
                            list_barrier.wait();
                        }
                    }
                    Op::Delete => {
                        let (count, cv) = &*deleting;
                        let mut n = count.lock().unwrap();
                        *n += 1;
                        cv.notify_all();
                        let _ = cv
                            .wait_timeout_while(n, Duration::from_secs(5), |n| *n < 2)
                            .unwrap();
                    }
                }
            }
        }));

        let handles: Vec<_> = (0..2)
            .map(|_| {
                let storage = Arc::clone(&storage);
                std::thread::spawn(move || acquire_lock(&*storage))
            })
            .collect();
        for h in handles {
            let res = h.join().unwrap();
            assert!(matches!(res, Err(VykarError::Locked(_))), "got {res:?}");
        }
        let lock_put_count = storage
            .ops()
            .iter()
            .filter(|(op, k)| *op == Op::Put && k.starts_with("locks/"))
            .count();
        assert_eq!(lock_put_count, 2);
        assert!(inner.list("locks/").unwrap().is_empty());
    }

    /// A foreign key with a *later* timestamp appears after our pre-check but
    /// before our LIST. We must lose to it.
    #[test]
    fn late_key_after_precheck_blocks() {
        let inner = Arc::new(MemoryBackend::new());
        let late = lock_key_at(Utc::now() + chrono::Duration::minutes(1), "late");
        let storage = HookBackend::new(Arc::clone(&inner), {
            let inner = Arc::clone(&inner);
            let late = late.clone();
            let fired = AtomicBool::new(false);
            move |op, key| {
                if op == Op::Put && key.starts_with("locks/") && !fired.swap(true, Ordering::SeqCst)
                {
                    write_lock_marker(&*inner, &late, "otherhost", 4242, Utc::now(), None);
                }
            }
        });

        assert_eq!(locked_key(acquire_lock(&storage).unwrap_err()), late);
        assert_eq!(inner.list("locks/").unwrap(), vec![late]);
    }

    /// Session marker is written, then the session parks before its probe
    /// LIST. Maintenance must see the marker and refuse.
    #[test]
    fn session_registered_first_blocks_maintenance() {
        let inner = Arc::new(MemoryBackend::new());
        let (park, reached) = Park::new();
        let mut repo = repo_over(HookBackend::new(Arc::clone(&inner), {
            let park = Arc::clone(&park);
            move |op, key| {
                if op == Op::List && key == "locks/" && on_thread("session") {
                    park.park_once();
                }
            }
        }));
        let storage = Arc::clone(&repo.storage);
        let session = std::thread::Builder::new()
            .name("session".into())
            .spawn(move || register_session_inner(&*storage, "s1", 1, 10))
            .unwrap();
        reached.recv_timeout(Duration::from_secs(10)).unwrap();

        let mut ran = false;
        let err = with_maintenance_lock(&mut repo, |_| {
            ran = true;
            Ok(())
        })
        .unwrap_err();
        assert!(matches!(err, VykarError::ActiveSessions(_)), "got {err:?}");
        assert!(!ran);
        assert!(inner.list("locks/").unwrap().is_empty());

        park.release();
        session.join().unwrap().unwrap();
        assert!(inner.exists(&session_marker_key("s1")).unwrap());
    }

    /// Maintenance holds the lock and parks before listing sessions. A session
    /// registering now must see the lock and back out.
    #[test]
    fn maintenance_lock_first_blocks_session() {
        let inner = Arc::new(MemoryBackend::new());
        let (park, reached) = Park::new();
        let mut repo = repo_over(HookBackend::new(Arc::clone(&inner), {
            let park = Arc::clone(&park);
            move |op, key| {
                if op == Op::List && key == "sessions/" && on_thread("maint") {
                    park.park_once();
                }
            }
        }));
        let storage = Arc::clone(&repo.storage);
        let maint = std::thread::Builder::new()
            .name("maint".into())
            .spawn(move || {
                let mut ran = false;
                let res = with_maintenance_lock(&mut repo, |_| {
                    ran = true;
                    Ok(())
                });
                (res, ran)
            })
            .unwrap();
        reached.recv_timeout(Duration::from_secs(10)).unwrap();

        let err = register_session_inner(&*storage, "s2", 1, 10).unwrap_err();
        assert!(matches!(err, VykarError::Locked(_)), "got {err:?}");
        assert!(!inner.exists(&session_marker_key("s2")).unwrap());

        park.release();
        let (res, ran) = maint.join().unwrap();
        res.unwrap();
        assert!(ran);
        assert!(inner.list("locks/").unwrap().is_empty());
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
                std::thread::spawn(move || loop {
                    match acquire_lock(&*storage) {
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
                            std::thread::sleep(Duration::from_micros(rand::random::<u64>() % 3000));
                        }
                        Err(e) => panic!("unexpected error: {e}"),
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
