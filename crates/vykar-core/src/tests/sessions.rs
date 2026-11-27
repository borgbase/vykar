use crate::repo::host_env::{Clock, StopSignal};
use crate::repo::sessions;
use crate::repo::sessions::{
    HEARTBEAT_POLL, HeartbeatAction, HeartbeatSchedule, MarkerReport, RefreshOutcome,
    SESSION_STALE_SECS, SessionEntry, SessionGuard, SessionMarker, cleanup_stale_sessions,
    clear_all_sessions, commit_marker_check, default_stale_session_duration, format_age,
    format_age_since, parse_session_marker_key, register_session, session_marker_key,
    session_marker_key_at,
};
use crate::repo::sessions::{HeadWriter, head_landing_check, single_writer_listing};
use crate::repo::{HostEnv, HostIdentity, LegacyUpgradeLock, SystemClock};
use crate::sim::env::{SimTime, TimeMode};
use crate::testutil::MemoryBackend;
use chrono::{Duration, Utc};
use std::sync::Arc;
use vykar_storage::StorageBackend;
use vykar_types::error::VykarError;

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

fn lock_key_at(ts: chrono::DateTime<Utc>, suffix: &str) -> String {
    format!("locks/{:020}-{suffix}.json", ts.timestamp_micros())
}

/// `clear_legacy_locks` deletes everything under `locks/` and counts the
/// `.json` keys only; session markers stay.
#[test]
fn clear_legacy_locks_removes_everything_under_locks() {
    let storage = MemoryBackend::new();
    for key in [
        "locks/a.json",
        "locks/b.json",
        &format!("locks/vykar-clock-{:032x}", 7u128),
        "locks/vykar-probe-0123",
        "locks/other",
    ] {
        storage.put(key, b"x").unwrap();
    }
    write_session_marker(&storage, "s1", &Utc::now().to_rfc3339());
    assert_eq!(sessions::clear_legacy_locks(&storage).unwrap(), 2);
    assert!(storage.list("locks/").unwrap().is_empty());
    assert!(storage.exists(&session_marker_key("s1")).unwrap());

    assert_eq!(
        sessions::clear_legacy_locks(&MemoryBackend::new()).unwrap(),
        0
    );
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
    let entry = crate::repo::sessions::SessionEntry {
        hostname: hostname.to_string(),
        pid,
        registered_at: last_refresh.to_string(),
        last_refresh: last_refresh.to_string(),
        boot_id: None,
        installation_id: None,
        pid_ns: None,
        elapsed_secs: None,
        committing: false,
        writes_heads: false,
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
    /// Runs before every PUT.
    before_put: std::sync::Mutex<Option<Box<dyn Fn() + Send>>>,
}

impl Default for FaultyBackend {
    fn default() -> Self {
        Self {
            inner: Arc::new(MemoryBackend::new()),
            faults: Default::default(),
            exists_calls: Default::default(),
            before_exists: Default::default(),
            before_put: Default::default(),
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
        if let Some(hook) = self.before_put.lock().unwrap().as_ref() {
            hook();
        }
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
            commit_marker_check("hid", Ok(true), &report, OLD)
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

/// A backup an hour into its session: young enough for the pack-age term.
const YOUNG: std::time::Duration = std::time::Duration::from_secs(3600);
/// 11 h 30 min: past `PACK_AGE_LIMIT` − `HEAD_LANDING_ALLOWANCE`.
const OLD: std::time::Duration = std::time::Duration::from_secs(11 * 3600 + 30 * 60);

fn marker_report(missing: bool, gap_min: u32) -> MarkerReport {
    MarkerReport {
        missing,
        longest_gap: gap_min * MIN,
        unchecked: 0,
        refreshes: 0,
        failed_refreshes: 0,
        slow_puts: 0,
        restarted: false,
    }
}

fn is_stale(r: &vykar_types::error::Result<()>) -> bool {
    matches!(
        r,
        Err(vykar_types::error::VykarError::StaleChunksDuringCommit)
    )
}

/// 04 "Why markers carry weight" and I8: a sound record commits at any age;
/// an unsound one commits while its packs are young and refuses, with its
/// own error, once they may be old.
#[test]
fn commit_marker_check_rules() {
    use crate::repo::ages::{HEAD_LANDING_ALLOWANCE, PACK_AGE_LIMIT};
    let probe_err = || Err(injected("sessions/x.json"));
    for elapsed in [YOUNG, OLD] {
        assert!(commit_marker_check("s", Ok(true), &marker_report(false, 15), elapsed).is_ok());
        assert!(commit_marker_check("s", probe_err(), &marker_report(false, 15), elapsed).is_ok());
    }

    #[derive(Debug, PartialEq)]
    enum Refusal {
        Stale,
        Other,
        Probe,
    }
    let unchecked = MarkerReport {
        unchecked: 1,
        ..marker_report(false, 1)
    };
    let slow = MarkerReport {
        slow_puts: 1,
        ..marker_report(false, 1)
    };
    type Probe = Box<dyn Fn() -> vykar_types::error::Result<bool>>;
    let cases: Vec<(&str, Probe, MarkerReport, Refusal)> = vec![
        (
            "missing",
            Box::new(|| Ok(true)),
            marker_report(true, 0),
            Refusal::Stale,
        ),
        (
            "key gone",
            Box::new(|| Ok(false)),
            marker_report(false, 0),
            Refusal::Stale,
        ),
        (
            "gap",
            Box::new(|| Ok(true)),
            marker_report(false, 31),
            Refusal::Stale,
        ),
        ("slow put", Box::new(|| Ok(true)), slow, Refusal::Stale),
        // A key still unchecked after the probe refuses, and not as stale:
        // the reap is not proven.
        (
            "unchecked",
            Box::new(|| Ok(true)),
            unchecked.clone(),
            Refusal::Other,
        ),
        (
            "unchecked, probe failed",
            Box::new(probe_err),
            unchecked,
            Refusal::Probe,
        ),
        (
            "probe failed, gap",
            Box::new(probe_err),
            marker_report(false, 31),
            Refusal::Probe,
        ),
        (
            "probe failed, missing",
            Box::new(probe_err),
            marker_report(true, 0),
            Refusal::Stale,
        ),
    ];
    for (name, probe, report, refusal) in &cases {
        assert!(
            commit_marker_check("s", probe(), report, YOUNG).is_ok(),
            "{name}: young packs commit"
        );
        let r = commit_marker_check("s", probe(), report, OLD);
        let got = match &r {
            Ok(()) => panic!("{name}: old packs must refuse"),
            Err(vykar_types::error::VykarError::StaleChunksDuringCommit) => Refusal::Stale,
            Err(e) if e.to_string().contains("injected fault") => Refusal::Probe,
            Err(_) => Refusal::Other,
        };
        assert_eq!(&got, refusal, "{name}: {r:?}");
    }

    // The boundary: elapsed + allowance = PACK_AGE_LIMIT commits.
    let edge = PACK_AGE_LIMIT - HEAD_LANDING_ALLOWANCE;
    let gap = marker_report(false, 31);
    assert!(commit_marker_check("s", Ok(true), &gap, edge).is_ok());
    let one_more = edge + std::time::Duration::from_secs(1);
    assert!(is_stale(&commit_marker_check(
        "s",
        Ok(true),
        &gap,
        one_more
    )));
}

/// The term at the fences around the head PUT: from the record alone, with
/// the landing allowance before the PUT and none after it.
#[test]
fn head_landing_check_rules() {
    use crate::repo::ages::{HEAD_LANDING_ALLOWANCE, PACK_AGE_LIMIT};
    use crate::repo::sessions::head_landing_check;
    let zero = std::time::Duration::ZERO;
    let one = std::time::Duration::from_secs(1);
    let sound = marker_report(false, 15);
    let gap = marker_report(false, 31);
    let missing = marker_report(true, 0);
    for allowance in [HEAD_LANDING_ALLOWANCE, zero] {
        assert!(head_landing_check("s", &sound, OLD + allowance, allowance).is_ok());
        for report in [&gap, &missing] {
            assert!(head_landing_check("s", report, YOUNG, allowance).is_ok());
            let edge = PACK_AGE_LIMIT - allowance;
            assert!(head_landing_check("s", report, edge, allowance).is_ok());
            assert!(is_stale(&head_landing_check(
                "s",
                report,
                edge + one,
                allowance
            )));
        }
    }
}

/// B1: a current key the judgement's probe found gone is recorded, so the
/// fence before the head PUT sees an unsound record even when no refresh
/// noticed the reap.
#[test]
fn a_gone_key_found_at_the_judgement_refuses_at_the_fence() {
    use crate::repo::ages::HEAD_LANDING_ALLOWANCE;
    use crate::repo::sessions::head_landing_check;
    let storage = MemoryBackend::new();
    let (_time, env) = sim_env();
    let clock = env.clock.as_ref();
    let marker = registered(&storage, "judged", &env);
    storage.delete(&session_marker_key("judged")).unwrap();
    let probe = marker.probe(&storage);
    assert!(matches!(probe, Ok(false)));
    // Before the commit machine notes it, the record alone reads continuous.
    assert!(marker.report(clock).continuous());
    marker.note_missing();
    let report = marker.report(clock);
    assert!(commit_marker_check("judged", probe, &report, YOUNG).is_ok());
    assert!(is_stale(&head_landing_check(
        "judged",
        &report,
        OLD,
        HEAD_LANDING_ALLOWANCE
    )));
}

/// Registration writes `"elapsed_secs":0`; without that key its body is
/// the one written before 04 step 7.
#[test]
fn registration_writes_elapsed_zero() {
    let storage = MemoryBackend::new();
    let (_time, env) = sim_env();
    let _marker = registered(&storage, "zero", &env);
    let bytes = storage.get(&session_marker_key("zero")).unwrap().unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["elapsed_secs"], serde_json::json!(0));
    value.as_object_mut().unwrap().remove("elapsed_secs");
    let entry = entry_at(&storage, &session_marker_key("zero"));
    let before = serde_json::json!({
        "hostname": entry.hostname,
        "pid": entry.pid,
        "registered_at": entry.registered_at,
        "last_refresh": entry.last_refresh,
        "boot_id": entry.boot_id,
        "installation_id": entry.installation_id,
        "pid_ns": entry.pid_ns,
    });
    assert_eq!(value, before);
}

/// A clock whose uptime can run on while the monotonic clock stands still
/// (a suspend).
struct SuspendClock {
    start: std::time::Instant,
    /// (monotonic offset, uptime)
    at: std::sync::Mutex<(std::time::Duration, std::time::Duration)>,
}

impl SuspendClock {
    fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
            at: std::sync::Mutex::new((
                std::time::Duration::ZERO,
                std::time::Duration::from_secs(1000),
            )),
        }
    }
    fn run(&self, d: std::time::Duration) {
        let mut at = self.at.lock().unwrap();
        at.0 += d;
        at.1 += d;
    }
    fn suspend(&self, d: std::time::Duration) {
        self.at.lock().unwrap().1 += d;
    }
}

impl Clock for SuspendClock {
    fn now_utc(&self) -> chrono::DateTime<Utc> {
        SystemClock.now_utc()
    }
    fn monotonic(&self) -> std::time::Instant {
        self.start + self.at.lock().unwrap().0
    }
    fn sleep(&self, d: std::time::Duration) {
        self.run(d);
    }
    fn wait_timeout(&self, _signal: &StopSignal, timeout: std::time::Duration) -> bool {
        self.run(timeout);
        false
    }
    fn uptime(&self) -> Option<std::time::Duration> {
        Some(self.at.lock().unwrap().1)
    }
}

/// Each refresh writes the time since registration, suspend included, plus
/// the pull-back of recovered journal packs, rounded up to a second.
#[test]
fn refresh_writes_elapsed_since_registration() {
    let storage = MemoryBackend::new();
    let clock = Arc::new(SuspendClock::new());
    let env = HostEnv::system().with_clock(clock.clone()).with_host(me());
    let marker = registered(&storage, "el", &env);
    clock.run(15 * MIN);
    marker.refresh(&storage, clock.as_ref());
    assert_eq!(
        entry_at(&storage, &session_marker_key_at("el", 1)).elapsed_secs,
        Some(900)
    );
    marker.pull_back(120 * MIN);
    marker.pull_back(30 * MIN); // the largest pull-back is kept
    clock.run(15 * MIN);
    marker.refresh(&storage, clock.as_ref());
    let written = entry_at(&storage, &session_marker_key_at("el", 2)).elapsed_secs;
    assert!(written >= Some(150 * 60), "{written:?}");
    // A suspend: uptime runs on, the monotonic clock does not.
    clock.suspend(60 * MIN);
    clock.run(std::time::Duration::from_millis(1500));
    marker.refresh(&storage, clock.as_ref());
    let written = entry_at(&storage, &session_marker_key_at("el", 3)).elapsed_secs;
    assert_eq!(written, Some((30 + 120 + 60) * 60 + 2), "rounded up");
}

/// B6: a marker PUT slower than MARGIN breaks continuity, and the heartbeat
/// refreshes again at its next poll.
#[test]
fn a_slow_marker_put_breaks_continuity() {
    let (time, env) = sim_env();
    let storage = FaultyBackend::default();
    let slow_by = Arc::new(std::sync::Mutex::new(std::time::Duration::ZERO));
    {
        let (time, slow_by) = (Arc::clone(&time), Arc::clone(&slow_by));
        *storage.before_put.lock().unwrap() =
            Some(Box::new(move || time.advance(*slow_by.lock().unwrap())));
    }
    let clock = env.clock.as_ref();
    let marker = registered(&storage, "slow", &env);
    time.advance(15 * MIN);
    *slow_by.lock().unwrap() = 11 * MIN;
    let outcome = marker.refresh(&storage, clock);
    assert!(
        matches!(outcome, RefreshOutcome::Slow { .. }),
        "{outcome:?}"
    );
    let report = marker.report(clock);
    assert_eq!(report.slow_puts, 1);
    assert!(!report.continuous(), "{report:?}");
    let mut schedule = HeartbeatSchedule::new();
    schedule.record(HeartbeatAction::Refresh, &outcome);
    assert_eq!(
        schedule.poll(),
        HeartbeatAction::Refresh,
        "again at the next poll"
    );
    // A fast PUT afterwards does not restore continuity.
    *slow_by.lock().unwrap() = std::time::Duration::ZERO;
    let outcome = marker.refresh(&storage, clock);
    assert!(
        matches!(outcome, RefreshOutcome::Refreshed { .. }),
        "{outcome:?}"
    );
    assert!(!marker.report(clock).continuous());

    // A suspend during the PUT: the monotonic clock stands still, the
    // suspend-aware clock (the one `elapsed_secs` is measured on) runs on.
    let suspend = Arc::new(SuspendClock::new());
    let env = HostEnv::system()
        .with_clock(suspend.clone())
        .with_host(me());
    let storage = FaultyBackend::default();
    let marker = registered(&storage, "suspended", &env);
    {
        let suspend = Arc::clone(&suspend);
        *storage.before_put.lock().unwrap() = Some(Box::new(move || suspend.suspend(11 * MIN)));
    }
    suspend.run(15 * MIN);
    let outcome = marker.refresh(&storage, suspend.as_ref());
    assert!(
        matches!(outcome, RefreshOutcome::Slow { .. }),
        "{outcome:?}"
    );
    assert!(!marker.report(suspend.as_ref()).continuous());
}

/// 04 "Compact" 3: a live session's horizon is its newest key's mtime minus
/// `elapsed_secs` minus MARGIN; unknown without either.
#[test]
fn horizon_is_mtime_minus_elapsed_minus_margin() {
    use crate::repo::ages::MARGIN;
    use crate::repo::sessions::LiveSession;
    let mtime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
    let entry = |elapsed: Option<u64>| SessionEntry {
        elapsed_secs: elapsed,
        ..session_entry(&me(), &Utc::now().to_rfc3339())
    };
    let live = |entry: Option<SessionEntry>, mtime: Option<std::time::SystemTime>| LiveSession {
        session_id: "h".into(),
        entry,
        mtime,
    };
    assert_eq!(
        live(Some(entry(Some(3600))), Some(mtime)).horizon(),
        Some(mtime - std::time::Duration::from_secs(3600) - MARGIN)
    );
    assert_eq!(live(None, Some(mtime)).horizon(), None, "no body");
    assert_eq!(
        live(Some(entry(None)), Some(mtime)).horizon(),
        None,
        "no field"
    );
    assert_eq!(live(Some(entry(Some(0))), None).horizon(), None, "no mtime");
    assert_eq!(
        live(Some(entry(Some(u64::MAX))), Some(mtime)).horizon(),
        None,
        "underflow"
    );
}

/// D1: a body without `elapsed_secs` (0.24, and builds before 04 step 7)
/// reads as `None`; a new body still parses as 0.24's four-field marker.
#[test]
fn old_and_new_marker_bodies_parse_both_ways() {
    #[derive(serde::Deserialize)]
    #[allow(dead_code, reason = "parsed only")]
    struct SessionEntryV024 {
        hostname: String,
        pid: u32,
        registered_at: String,
        last_refresh: String,
    }
    let old = br#"{"hostname":"h","pid":7,"registered_at":"2026-01-01T00:00:00+00:00","last_refresh":"2026-01-01T00:00:00+00:00"}"#;
    let parsed: SessionEntry = serde_json::from_slice(old).unwrap();
    assert_eq!(parsed.elapsed_secs, None);
    let new = SessionEntry {
        elapsed_secs: Some(42),
        ..session_entry(&me(), &Utc::now().to_rfc3339())
    };
    let bytes = serde_json::to_vec(&new).unwrap();
    let v024: SessionEntryV024 = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v024.pid, new.pid);
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
        let err = commit_marker_check("unk", probe, &report, OLD).unwrap_err();
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
    let listed = crate::repo::sessions::list_session_entries(&storage).unwrap();
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
    assert!(commit_marker_check("after", marker.probe(&storage), &report, OLD).is_ok());
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
    let listed = crate::repo::sessions::list_session_entries(&storage).unwrap();
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

    let entries = crate::repo::sessions::list_session_entries(&storage).unwrap();
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0].0, "two");
    assert_eq!(entries[0].1.as_ref().unwrap().last_refresh, new);
    assert_eq!(
        crate::repo::sessions::list_sessions(&storage).unwrap(),
        ["two"]
    );
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
    let listed = crate::repo::sessions::list_session_entries(&storage).unwrap();
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
        elapsed_secs: None,
        committing: false,
        writes_heads: false,
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

/// Plant `w`'s entry once as a 0.24-shaped lock (6 h limit) and once as a
/// session marker (45 min limit), each in a fresh store, and run the reaper
/// as `env`: for the lock the v2/v3 upgrade's legacy key acquisition
/// (`LegacyUpgradeLock::acquire`, which reaps on the predicate this
/// binary's lock reaper used: same-host dead or earlier boot, or older than
/// 6 h), for the marker the session reaper. Returns (lock reaped, session
/// reaped).
fn verdicts(w: &Writer, env: &HostEnv) -> (bool, bool) {
    let locks = Arc::new(MemoryBackend::new());
    let key = plant_lock(locks.as_ref(), w);
    let lock_reaped =
        match LegacyUpgradeLock::acquire(Arc::clone(&locks) as Arc<dyn StorageBackend>, env) {
            Ok(lock) => {
                assert!(!locks.exists(&key).unwrap(), "acquired beside {key}");
                lock.release().unwrap();
                true
            }
            Err(VykarError::Locked(holder)) => {
                assert_eq!(holder.key, key);
                assert!(locks.exists(&key).unwrap());
                false
            }
            Err(e) => panic!("acquire: {e}"),
        };

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

/// A lock (the v2/v3 upgrade's legacy key) and a marker written by `me()`:
/// (lock bytes, marker bytes).
fn written_entries() -> (Vec<u8>, Vec<u8>) {
    let storage = Arc::new(MemoryBackend::new());
    let env = env_as(me(), alive, Some(HOUR));
    let legacy =
        LegacyUpgradeLock::acquire(Arc::clone(&storage) as Arc<dyn StorageBackend>, &env).unwrap();
    let lock = storage.get(legacy.key()).unwrap().unwrap();
    legacy.release().unwrap();
    let storage = storage.as_ref();
    register_session(storage, "fmt", &env).unwrap();
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

// ── wait_while_contended ──────────────────────────────────────────────────

mod deadline {
    use crate::repo::HostEnv;
    use crate::repo::control::CancelToken;
    use crate::repo::sessions::{COMMIT_MAX_WAIT, wait_while_contended};
    use crate::sim::env::{SimTime, TimeMode};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    use vykar_storage::StorageBackend;
    use vykar_types::error::{LockHolder, Result, VykarError};

    fn rival() -> LockHolder {
        LockHolder {
            key: "sessions/rival.json".into(),
            details: None,
        }
    }

    /// An attempt that is `Locked` until another thread lets it pass (after
    /// 1 s real time) returns once it passes; `on_contended` is called once.
    #[test]
    fn wait_while_contended_returns_once_the_holder_goes() {
        let gone = Arc::new(AtomicBool::new(false));
        let releaser = {
            let gone = Arc::clone(&gone);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(1));
                gone.store(true, Ordering::SeqCst);
            })
        };
        let mut contended = 0;
        wait_while_contended(
            &HostEnv::system(),
            Duration::from_secs(30),
            100,
            &CancelToken::new(),
            |_| contended += 1,
            || -> Result<()> {
                if gone.load(Ordering::SeqCst) {
                    Ok(())
                } else {
                    Err(VykarError::Locked(rival()))
                }
            },
        )
        .unwrap();
        releaser.join().unwrap();
        assert_eq!(contended, 1);
    }

    /// The production budget (`COMMIT_MAX_WAIT`) on virtual time: an attempt
    /// that stays `Locked` ends `Locked` once 2 h have passed there, in
    /// under 5 s of real time.
    #[test]
    fn wait_while_contended_times_out_after_budget() {
        let time = SimTime::new(chrono::Utc::now(), TimeMode::Auto);
        let env = HostEnv::system().with_clock(Arc::new(time.clock(chrono::Duration::zero())));
        let start = Instant::now();
        let err = wait_while_contended(
            &env,
            COMMIT_MAX_WAIT,
            500,
            &CancelToken::new(),
            |_| {},
            || -> Result<()> { Err(VykarError::Locked(rival())) },
        )
        .unwrap_err();
        assert!(matches!(err, VykarError::Locked(_)), "got {err:?}");
        assert!(time.elapsed() >= COMMIT_MAX_WAIT, "{:?}", time.elapsed());
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "{:?}",
            start.elapsed()
        );
    }

    /// `wait_while_contended`'s deadline (virtual time, `TimeMode::Auto`: each
    /// backoff sleep advances it at once): an attempt that stays `Locked`
    /// is retried until `max_wait` has passed, then one final attempt's
    /// `Locked(holder)` is returned; `on_contended` is called once.
    #[test]
    fn wait_while_contended_gives_up_at_the_deadline() {
        let time = SimTime::new(chrono::Utc::now(), TimeMode::Auto);
        let env = HostEnv::system().with_clock(Arc::new(time.clock(chrono::Duration::zero())));
        let max_wait = Duration::from_secs(10 * 60);
        let holder = rival();
        let (mut attempts, mut contended) = (0usize, 0usize);
        let err = wait_while_contended(
            &env,
            max_wait,
            500,
            &CancelToken::new(),
            |_| contended += 1,
            || -> Result<()> {
                attempts += 1;
                Err(VykarError::Locked(holder.clone()))
            },
        )
        .unwrap_err();
        match err {
            VykarError::Locked(h) => assert_eq!(h.key, "sessions/rival.json"),
            other => panic!("got {other:?}"),
        }
        assert!(time.elapsed() >= max_wait, "{:?}", time.elapsed());
        assert!(attempts > 2, "{attempts} attempts");
        assert_eq!(contended, 1);
    }

    /// Registers a live compact (session `rival`, flagged `writes_heads`,
    /// on another host) right after the first PUT of a marker key flagged
    /// `committing`: a backup's commit mark.
    struct RivalOnMark {
        inner: Arc<dyn StorageBackend>,
        marked: Arc<AtomicBool>,
    }

    impl vykar_storage::InnerBackend for RivalOnMark {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &*self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for RivalOnMark;
        except [put, put_pack, begin_staged_put, conditional, profile];

        // Every write on `put` (the repository is single-writer).
        fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
            None
        }

        fn profile(&self) -> vykar_storage::BackendProfile {
            self.inner.profile()
        }

        // Packs and staged writes are no marker keys.
        fn put_pack(
            &self,
            key: &str,
            data: vykar_storage::PackData,
        ) -> vykar_types::error::Result<()> {
            self.inner.put_pack(key, data)
        }

        fn begin_staged_put(
            &self,
            prefix: &str,
        ) -> vykar_types::error::Result<Option<Box<dyn vykar_storage::StagedPut>>> {
            self.inner.begin_staged_put(prefix)
        }

        fn put(&self, key: &str, data: &[u8]) -> vykar_types::error::Result<()> {
            self.inner.put(key, data)?;
            let committing = key.starts_with("sessions/")
                && serde_json::from_slice::<crate::repo::sessions::SessionEntry>(data)
                    .is_ok_and(|e| e.committing);
            if committing && !self.marked.swap(true, Ordering::SeqCst) {
                let rival = HostEnv::system().with_host(crate::repo::HostIdentity {
                    hostname: "compact-host".into(),
                    pid: 4242,
                    boot_id: Some("boot-c".into()),
                    installation_id: Some("inst-c".into()),
                    pid_ns: Some(1),
                });
                crate::repo::sessions::register_session_with(
                    &*self.inner,
                    "rival",
                    &rival,
                    crate::repo::sessions::MarkerFlags {
                        committing: false,
                        writes_heads: true,
                    },
                )?;
            }
            Ok(())
        }
    }

    /// The single-writer arm of `PendingCommit::protect_commit` gives up at
    /// its deadline (`COMMIT_MAX_WAIT`, virtual time) beside a live
    /// compact (a session flagged `writes_heads` on another host, registered
    /// right after the backup's commit mark and fresh on the store's clock
    /// throughout): its listing after the mark unmarks, every later attempt
    /// waits at the listing before the mark, and at the deadline the result
    /// is `Locked` naming the compact's session with the backup's marker
    /// left unmarked (its newest key carries no `committing`).
    #[test]
    fn a_single_writer_protection_gives_up_unmarked() {
        use crate::commands;
        use crate::repo::sessions::SessionEntry;
        use crate::repo::{EncryptionMode, OpenOptions, Repo, RepoFormat, WriterMode};
        use crate::tests::helpers::make_test_config;
        use vykar_storage::local_backend::LocalBackend;

        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        let source = tmp.path().join("src");
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("a"), "alpha").unwrap();
        let mut config = make_test_config(&repo_dir);
        let cache = tmp.path().join("cache");
        config.cache_dir = Some(cache.to_string_lossy().into_owned());
        let local = || Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
        Repo::init_at_format(
            local(),
            EncryptionMode::Aes256Gcm,
            config.chunker.params(),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(&config.repository.init_options()),
            Some(cache.clone()),
            RepoFormat::CURRENT,
        )
        .unwrap();
        let mut repo = Repo::open(
            local(),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(cache.clone()),
            OpenOptions::new().legacy_format(),
        )
        .unwrap();
        commands::upgrade::upgrade_repo(&mut repo, Some(WriterMode::Single)).unwrap();
        drop(repo);

        let storage = local();
        let marked = Arc::new(AtomicBool::new(false));
        let wrap = {
            let marked = Arc::clone(&marked);
            move |inner: Box<dyn StorageBackend>| -> Box<dyn StorageBackend> {
                Box::new(RivalOnMark {
                    inner: Arc::from(inner),
                    marked: Arc::clone(&marked),
                })
            }
        };

        let time = SimTime::new(chrono::Utc::now(), TimeMode::Auto);
        let env = HostEnv::system().with_clock(Arc::new(time.clock(chrono::Duration::zero())));
        let source_paths = vec![source.to_string_lossy().into_owned()];
        let req = commands::backup::BackupRequest {
            snapshot_name: "a1",
            passphrase: Some(crate::testing::TEST_PASSPHRASE),
            source_paths: &source_paths,
            source_label: "src",
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: crate::compress::Compression::None,
            command_dumps: &[],
            verbose: false,
        };
        let mut pending =
            commands::backup::upload(&config, &req, None, &CancelToken::new(), Some(&wrap), env)
                .map_err(|(_, e)| e)
                .unwrap();
        let start = Instant::now();
        let err = pending
            .protect_commit(None, &CancelToken::new())
            .unwrap_err();
        match &err {
            VykarError::Locked(h) => assert!(h.key.starts_with("sessions/rival"), "{h:?}"),
            other => panic!("got {other:?}"),
        }
        assert!(time.elapsed() >= COMMIT_MAX_WAIT, "{:?}", time.elapsed());
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "{:?}",
            start.elapsed()
        );
        assert!(marked.load(Ordering::SeqCst), "the backup never marked");
        assert!(!pending.is_marked());
        let mut own: Vec<String> = storage
            .list("sessions/")
            .unwrap()
            .into_iter()
            .filter(|k| k.ends_with(".json") && !k.starts_with("sessions/rival"))
            .collect();
        own.sort_by_key(|k| {
            crate::repo::sessions::parse_session_marker_key(k)
                .map(|(_, n)| n)
                .unwrap_or_default()
        });
        let newest = own.last().expect("the backup's marker");
        let entry: SessionEntry =
            serde_json::from_slice(&storage.get(newest).unwrap().unwrap()).unwrap();
        assert!(!entry.committing, "{newest} still flagged committing");
        drop(pending);
    }

    /// A cancelled token ends the wait before the first attempt.
    #[test]
    fn wait_while_contended_interrupted_before_first_attempt() {
        let flag = CancelToken::new();
        flag.cancel();
        let mut attempts = 0;
        let err = wait_while_contended(
            &HostEnv::system(),
            Duration::from_secs(30),
            500,
            &flag,
            |_| {},
            || -> Result<()> {
                attempts += 1;
                Ok(())
            },
        )
        .unwrap_err();
        assert!(matches!(err, VykarError::Interrupted), "got {err:?}");
        assert_eq!(attempts, 0);
    }

    /// A cancel during the wait (after 200 ms) ends it within a sleep slice.
    #[test]
    fn wait_while_contended_interrupted_during_contention() {
        let flag = CancelToken::new();
        let setter = {
            let flag = flag.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(200));
                flag.cancel();
            })
        };
        let start = Instant::now();
        let err = wait_while_contended(
            &HostEnv::system(),
            Duration::from_secs(600),
            500,
            &flag,
            |_| {},
            || -> Result<()> { Err(VykarError::Locked(rival())) },
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
/// (src/tests/sim_exclusion.rs). This one stays: a single-client test that
/// asserts on one call's requests, where a gate adds nothing.
mod exclusion {
    use crate::repo::HostEnv;
    use crate::repo::sessions::{register_session_inner, session_marker_key};
    use crate::sim::{SimConfig, SimStore};
    use vykar_storage::StorageBackend;
    use vykar_types::error::VykarError;

    /// Requests under `locks/` the store saw, listings included.
    fn lock_requests(store: &SimStore) -> usize {
        store
            .log()
            .iter()
            .filter(|r| r.key.starts_with("locks/"))
            .count()
    }

    fn locked_key(err: VykarError) -> String {
        match err {
            VykarError::Locked(holder) => holder.key,
            other => panic!("expected Locked, got {other:?}"),
        }
    }

    /// No request under `locks/` by a registration (10-single-protocol step
    /// 8: it lists `sessions/`, and nothing reads `locks/` since step 9); a
    /// live exclusive session refuses it `Locked` naming the exclusive
    /// marker, and its own marker is deleted.
    #[test]
    fn register_session_probe_issues_no_lock_put() {
        let store = SimStore::new(SimConfig::default());
        let storage = store.handle(store.register_client("a"));
        register_session_inner(
            &storage,
            "free",
            1,
            10,
            &HostEnv::system(),
            Default::default(),
        )
        .unwrap();
        assert!(storage.exists(&session_marker_key("free")).unwrap());
        assert_eq!(lock_requests(&store), 0);

        let held = crate::repo::sessions::register_exclusive(&storage, &HostEnv::system()).unwrap();
        let err = register_session_inner(
            &storage,
            "blocked",
            1,
            10,
            &HostEnv::system(),
            Default::default(),
        )
        .unwrap_err();
        assert_eq!(
            locked_key(err),
            session_marker_key(held.registration.session_id())
        );
        assert!(!storage.exists(&session_marker_key("blocked")).unwrap());
        assert_eq!(lock_requests(&store), 0);
    }
}

/// Ages on the store's clock (04 step 5): mtimes decide, the timestamps
/// inside the entries do not. On a real `LocalBackend`, mtimes set by hand.
mod store_clock {
    use crate::repo::HostEnv;
    use crate::repo::ages::StoreClock;
    use crate::repo::sessions::{
        SessionEntry, cleanup_stale_sessions, default_stale_session_duration,
    };
    use crate::sim::{SimConfig, SimStore};
    use chrono::Utc;
    use std::path::Path;
    use std::time::{Duration, SystemTime};
    use vykar_storage::StorageBackend;
    use vykar_storage::local_backend::LocalBackend;

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

    /// "Now" on the store's clock, read with a clock probe under `sessions/`
    /// (`StoreClock::in_sessions`, the existing-pack check's clock).
    fn store_now(storage: &dyn StorageBackend, env: &HostEnv) -> Option<vykar_storage::StoreNow> {
        StoreClock::in_sessions().now(storage, env).0
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
            elapsed_secs: None,
            committing: false,
            writes_heads: false,
        };
        storage
            .put(
                &format!("sessions/{id}.json"),
                &serde_json::to_vec(&entry).unwrap(),
            )
            .unwrap();
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
        let now = store_now(&storage, &env);
        assert!(now.is_some());
        let cleaned =
            cleanup_stale_sessions(&storage, default_stale_session_duration(), &env, now).unwrap();
        assert_eq!(cleaned, vec!["aged".to_string()]);
        assert!(!storage.exists("sessions/aged.json").unwrap());
        assert!(storage.exists("sessions/live.json").unwrap());
    }

    #[test]
    fn malformed_marker_is_reaped_once_old_by_mtime() {
        let (dir, storage) = local();
        storage.put("sessions/x.json", b"{").unwrap();
        storage.put("sessions/x.index", b"journal").unwrap();
        let env = HostEnv::system();
        let now = store_now(&storage, &env);
        let max_age = default_stale_session_duration();

        // Young: kept, it blocks maintenance.
        let cleaned = cleanup_stale_sessions(&storage, max_age, &env, now).unwrap();
        assert!(cleaned.is_empty(), "{cleaned:?}");
        assert!(storage.exists("sessions/x.json").unwrap());

        // 46 min old by its mtime: reaped; its journal keeps one run's grace.
        age(dir.path(), "sessions/x.json", Duration::from_secs(46 * 60));
        let cleaned = cleanup_stale_sessions(&storage, max_age, &env, now).unwrap();
        assert_eq!(cleaned, vec!["x".to_string()]);
        assert!(!storage.exists("sessions/x.json").unwrap());
        assert!(storage.exists("sessions/x.index").unwrap());

        // Without a reading of "now" nothing is stale by age.
        storage.put("sessions/y.json", b"{").unwrap();
        age(dir.path(), "sessions/y.json", 2 * HOUR);
        let cleaned = cleanup_stale_sessions(&storage, max_age, &env, None).unwrap();
        assert!(cleaned.is_empty(), "{cleaned:?}");
        assert!(storage.exists("sessions/y.json").unwrap());
    }

    /// Probes under `sessions/` (the put-if-absent check's, the existing-pack
    /// clock's) left by a failed delete are removed by maintenance's session
    /// reap once an hour old; young ones, live sessions, and everything when
    /// "now" is unknown are kept. A session's marker or journal is never probe debris, whatever its ID.
    #[test]
    fn reap_removes_old_session_probes_only() {
        let (dir, storage) = local();
        let old_probe = "sessions/vykar-probe-1700000000-0123456789abcdef0123456789abcdef";
        let old_clock = "sessions/vykar-clock-0123456789abcdef0123456789abcdef";
        let young = "sessions/vykar-probe-1700000001-fedcba9876543210fedcba9876543210";
        for key in [old_probe, old_clock, young] {
            storage.put(key, b"probe").unwrap();
        }
        age(dir.path(), old_probe, 2 * HOUR);
        age(dir.path(), old_clock, 2 * HOUR);
        age(dir.path(), young, Duration::from_secs(10 * 60));
        marker(&storage, "live", Utc::now());
        storage.put("sessions/live.index", b"journal").unwrap();
        age(dir.path(), "sessions/live.index", 2 * HOUR);
        // A live session whose ID starts like a probe: its old generation-0
        // marker and its old journal are no probe debris.
        let probe_named = [
            "sessions/vykar-probe-x.json",
            "sessions/vykar-probe-x.index",
        ];
        marker(&storage, "vykar-probe-x", Utc::now());
        storage.put(probe_named[1], b"journal").unwrap();
        storage
            .put(
                "sessions/vykar-probe-x.1.json",
                &storage.get(probe_named[0]).unwrap().unwrap(),
            )
            .unwrap();
        for key in probe_named {
            age(dir.path(), key, 2 * HOUR);
        }
        let all = |s: &LocalBackend| {
            let mut keys = s.list("sessions/").unwrap();
            keys.sort();
            keys
        };
        let before = all(&storage);

        let env = HostEnv::system();
        let max_age = default_stale_session_duration();
        // "Now" unknown: nothing is old.
        let cleaned = cleanup_stale_sessions(&storage, max_age, &env, None).unwrap();
        assert!(cleaned.is_empty(), "{cleaned:?}");
        assert_eq!(all(&storage), before);

        let now = store_now(&storage, &env);
        let cleaned = cleanup_stale_sessions(&storage, max_age, &env, now).unwrap();
        assert!(cleaned.is_empty(), "no session was stale: {cleaned:?}");
        assert!(!storage.exists(old_probe).unwrap());
        assert!(!storage.exists(old_clock).unwrap());
        assert!(storage.exists(young).unwrap());
        assert!(storage.exists("sessions/live.json").unwrap());
        assert!(storage.exists("sessions/live.index").unwrap());
        for key in probe_named {
            assert!(storage.exists(key).unwrap(), "{key}");
        }
    }

    /// A probe under `sessions/` is no session: listings, the reap and the
    /// committing check skip it.
    #[test]
    fn session_probes_are_not_sessions() {
        let (_dir, storage) = local();
        let probe = "sessions/vykar-probe-1700000000-0123456789abcdef0123456789abcdef";
        let clock = "sessions/vykar-clock-0123456789abcdef0123456789abcdef";
        storage.put(probe, b"probe").unwrap();
        storage.put(clock, b"vykar clock probe").unwrap();
        marker(&storage, "live", Utc::now());

        let env = HostEnv::system();
        let ids = crate::repo::sessions::list_sessions(&storage).unwrap();
        assert_eq!(ids, vec!["live".to_string()]);
        let entries = crate::repo::sessions::list_session_entries(&storage).unwrap();
        assert_eq!(entries.len(), 1, "{entries:?}");
        let (cleaned, kept) = crate::repo::sessions::cleanup_stale_sessions_keeping(
            &storage,
            default_stale_session_duration(),
            &env,
            None,
        )
        .unwrap();
        assert!(cleaned.is_empty(), "{cleaned:?}");
        let kept: Vec<&str> = kept.iter().map(|s| s.session_id.as_str()).collect();
        assert_eq!(kept, vec!["live"]);
        assert!(
            crate::repo::sessions::committing_sessions(&storage, "own")
                .unwrap()
                .is_empty()
        );
        assert!(storage.exists(probe).unwrap());
        assert!(storage.exists(clock).unwrap());
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

    /// PUTs of clock probes under `sessions/` by `client` from log position
    /// `from`.
    fn probe_puts(store: &SimStore, client: crate::sim::ClientId, from: usize) -> usize {
        store.log()[from..]
            .iter()
            .filter(|r| r.client == client && r.op.is_write())
            .filter(|r| r.key.starts_with("sessions/vykar-clock-"))
            .count()
    }

    /// `StoreClock::in_sessions` (the existing-pack check's clock): the
    /// first reading probes (one PUT under `sessions/vykar-clock-`, and the
    /// listing it made is returned); within `STORE_READING_MAX_AGE` of it a
    /// call extrapolates (reading + local monotonic time since, no request,
    /// no listing); after that a call probes again, on the same key. No
    /// probe key is left.
    #[test]
    fn store_clock_probes_once_per_reading_and_extrapolates() {
        use crate::repo::ages::STORE_READING_MAX_AGE;

        let (store, time, _ha, hb, env_b) = sim_pair_timed();
        let mut clock = StoreClock::in_sessions();
        let from = store.log().len();
        let (first, listing) = clock.now(&hb, &env_b);
        let first = first.expect("a reading");
        assert!(listing.is_some(), "the probe's listing");
        assert_eq!(probe_puts(&store, hb.client(), from), 1);

        let four = Duration::from_secs(4 * 60);
        assert!(four < STORE_READING_MAX_AGE);
        time.advance(four);
        let from = store.log().len();
        let (second, listing) = clock.now(&hb, &env_b);
        let second = second.expect("an extrapolated reading");
        assert!(listing.is_none(), "no probe, no listing");
        assert_eq!(store.log().len(), from, "no request");
        let since = second.time().duration_since(first.time()).unwrap();
        assert!(
            since >= four && since < four + Duration::from_secs(1),
            "{since:?}"
        );

        time.advance(Duration::from_secs(2 * 60));
        let (third, listing) = clock.now(&hb, &env_b);
        assert!(third.is_some() && listing.is_some(), "probed again");
        assert_eq!(probe_puts(&store, hb.client(), from), 1);
        let keys: std::collections::BTreeSet<_> = store
            .log()
            .iter()
            .filter(|r| r.client == hb.client() && r.key.starts_with("sessions/vykar-clock-"))
            .map(|r| r.key.clone())
            .collect();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert!(store.keys("sessions/vykar-clock-").is_empty());
    }
}

// ── The marker handshake (04 step 9, phase F part 10c) ──────────────────

/// `me()` on another machine: a session no same-host rule reaps.
fn elsewhere() -> HostEnv {
    HostEnv::system().with_host(HostIdentity {
        hostname: "elsewhere".into(),
        pid: 7,
        boot_id: Some("boot-x".into()),
        installation_id: Some("inst-x".into()),
        pid_ns: Some(1),
    })
}

const COMMITTING: crate::repo::sessions::MarkerFlags = crate::repo::sessions::MarkerFlags {
    committing: true,
    writes_heads: false,
};

/// Both flags false serialise to the marker JSON of before (no new keys);
/// a flagged entry parses back; a body without them parses unflagged.
#[test]
fn a_flagged_marker_round_trips_and_a_plain_one_is_unchanged() {
    let now = Utc::now();
    let plain = SessionEntry::new(&HostEnv::system().with_host(me()), now, now);
    let json = serde_json::to_string(&plain).unwrap();
    assert!(
        !json.contains("committing") && !json.contains("writes_heads"),
        "{json}"
    );
    let flagged = SessionEntry {
        committing: true,
        ..plain.clone()
    };
    let json = serde_json::to_string(&flagged).unwrap();
    assert!(json.contains("\"committing\":true"), "{json}");
    assert!(!json.contains("writes_heads"), "{json}");
    let back: SessionEntry = serde_json::from_str(&json).unwrap();
    assert!(back.committing && !back.writes_heads);
    let compact = SessionEntry {
        writes_heads: true,
        ..plain.clone()
    };
    let back: SessionEntry =
        serde_json::from_str(&serde_json::to_string(&compact).unwrap()).unwrap();
    assert!(back.writes_heads);
    let old = r#"{"hostname":"h","pid":1,"registered_at":"2026-01-01T00:00:00Z","last_refresh":"2026-01-01T00:00:00Z","elapsed_secs":3}"#;
    let back: SessionEntry = serde_json::from_str(old).unwrap();
    assert!(!back.committing && !back.writes_heads);
}

/// `mark` writes a new generation carrying the flag, and every later
/// refresh (the heartbeat's) carries it too. Marks and refreshes from two
/// threads are serialised: the current key is always the newest one
/// written, never an earlier generation that finished last.
#[test]
fn mark_writes_a_new_key_with_the_flag_and_refreshes_keep_it() {
    let storage = MemoryBackend::new();
    let (_time, env) = sim_env();
    let m = registered(&storage, "flag", &env);
    let key = m.mark(&storage, env.clock.as_ref(), COMMITTING).unwrap();
    assert_eq!(key, session_marker_key_at("flag", 1));
    assert!(entry_at(&storage, &key).committing);
    assert!(!entry_at(&storage, &session_marker_key("flag")).committing);
    assert!(matches!(
        m.refresh(&storage, env.clock.as_ref()),
        RefreshOutcome::Refreshed { .. }
    ));
    let (generation, current) = m.current();
    assert_eq!(generation, 2);
    assert!(entry_at(&storage, &current).committing);
    assert_eq!(m.flags(), COMMITTING);

    for _ in 0..100 {
        let storage = MemoryBackend::new();
        let m = registered(&storage, "race", &env);
        std::thread::scope(|s| {
            s.spawn(|| m.mark(&storage, env.clock.as_ref(), COMMITTING).unwrap());
            s.spawn(|| m.refresh(&storage, env.clock.as_ref()));
        });
        let (generation, current) = m.current();
        assert_eq!(generation, 2, "the current key is the newest written");
        assert!(storage.exists(&current).unwrap());
    }
}

/// The pre-head listing reads "now" (the caller's key's mtime) and fails
/// `Locked` on a live exclusive session.
#[test]
fn exclusive_listing_reads_now_and_refuses_beside_a_live_exclusive() {
    let storage = MemoryBackend::with_mtimes();
    let env = HostEnv::system().with_host(me());
    let m = registered(&storage, "probe", &env);
    let key = m.mark(&storage, env.clock.as_ref(), COMMITTING).unwrap();
    let written = std::time::SystemTime::now() - std::time::Duration::from_secs(5);
    storage.set_mtime(&key, written);
    assert_eq!(
        sessions::exclusive_listing(&storage, &env, &key).unwrap(),
        Some(vykar_storage::StoreNow::at(written))
    );
    let other = sessions::register_exclusive(&storage, &elsewhere()).unwrap();
    let err = sessions::exclusive_listing(&storage, &env, &key).unwrap_err();
    let VykarError::Locked(holder) = err else {
        panic!("{err}");
    };
    assert!(holder.key.starts_with("sessions/x-"), "{}", holder.key);
    assert!(holder.details.is_some());
    sessions::deregister_session(&storage, other.registration.session_id(), &env);
}

/// The pre-head listing judges only exclusive sessions: a young malformed
/// shared marker (which a reaper would keep and count) does not refuse.
#[test]
fn exclusive_listing_ignores_shared_sessions() {
    let storage = MemoryBackend::with_mtimes();
    let env = HostEnv::system().with_host(me());
    let m = registered(&storage, "own", &env);
    let (_, key) = m.current();
    storage.put("sessions/shared.json", b"not json").unwrap();
    registered(&storage, "other", &elsewhere());
    assert!(sessions::exclusive_listing(&storage, &env, &key).is_ok());
    assert!(storage.exists("sessions/shared.json").unwrap());
}

/// A young malformed exclusive marker refuses (fail closed); one older than
/// 45 min by its mtime is reaped, its journal kept, and "now" is the
/// caller's key's mtime.
#[test]
fn exclusive_listing_judges_exclusive_markers_by_age() {
    let storage = MemoryBackend::with_mtimes();
    let env = HostEnv::system().with_host(me());
    let m = registered(&storage, "own", &env);
    let (_, key) = m.current();
    let own_mtime = std::time::SystemTime::now();
    storage.set_mtime(&key, own_mtime);
    let x = format!("sessions/x-{:032x}", 9u128);
    storage.put(&format!("{x}.json"), b"not json").unwrap();
    storage.put(&format!("{x}.index"), b"journal").unwrap();
    let err = sessions::exclusive_listing(&storage, &env, &key).unwrap_err();
    let VykarError::Locked(holder) = err else {
        panic!("{err}");
    };
    assert_eq!(holder.key, format!("{x}.json"));
    assert!(holder.details.is_none());

    storage.set_mtime(
        &format!("{x}.json"),
        own_mtime - std::time::Duration::from_secs(46 * 60),
    );
    assert_eq!(
        sessions::exclusive_listing(&storage, &env, &key).unwrap(),
        Some(vykar_storage::StoreNow::at(own_mtime))
    );
    assert!(!storage.exists(&format!("{x}.json")).unwrap(), "reaped");
    assert!(
        storage.exists(&format!("{x}.index")).unwrap(),
        "journal kept"
    );
}

/// An exclusive registration: an `x-` + 32 hex ID, the other live sessions
/// returned, stale ones reaped and orphan journals deleted as maintenance
/// does, "now" from its own key.
#[test]
fn register_exclusive_lists_reaps_and_names_itself() {
    let storage = MemoryBackend::with_mtimes();
    let env = HostEnv::system().with_host(me());
    let live = registered(&storage, "live", &elsewhere());
    let (_, live_key) = live.current();
    storage.put("sessions/stale.json", br#"{"hostname":"gone","pid":1,"registered_at":"2026-01-01T00:00:00Z","last_refresh":"2026-01-01T00:00:00Z"}"#).unwrap();
    storage.set_mtime(
        "sessions/stale.json",
        std::time::SystemTime::now() - std::time::Duration::from_secs(46 * 60),
    );
    storage.put("sessions/orphan.index", b"journal").unwrap();
    let reg = sessions::register_exclusive(&storage, &env).unwrap();
    let id = reg.registration.session_id();
    assert!(
        id.len() == 34 && id.starts_with("x-") && id[2..].bytes().all(|b| b.is_ascii_hexdigit()),
        "{id}"
    );
    assert!(sessions::is_exclusive_session(id));
    assert!(storage.exists(&session_marker_key(id)).unwrap());
    assert_eq!(reg.cleaned, vec!["stale".to_string()]);
    assert_eq!(
        reg.live
            .iter()
            .map(|s| s.session_id.as_str())
            .collect::<Vec<_>>(),
        vec!["live"]
    );
    assert!(reg.now.is_some());
    assert!(!storage.exists("sessions/orphan.index").unwrap());
    assert!(storage.exists(&live_key).unwrap());
    sessions::deregister_session(&storage, id, &env);
    assert!(!storage.exists(&session_marker_key(id)).unwrap());
}

// ── 10-single-protocol step 7: the record's restart, the single-writer listing ──

/// The suspend rule's restart: a record with a gap over `MARKER_GAP_LIMIT`
/// and a miss is continuous again after `restart_record`, and marked
/// `restarted`, which keeps the pack-age term binding: past
/// `PACK_AGE_LIMIT` the landing check and the commit check refuse what
/// they would let a continuous, unrestarted record do; young they pass. A
/// refresh after the restart records no miss.
#[test]
fn restart_record_forgets_the_gap_and_keeps_the_term_binding() {
    use crate::repo::ages::{HEAD_LANDING_ALLOWANCE, PACK_AGE_LIMIT};
    let storage = MemoryBackend::new();
    let (time, env) = sim_env();
    let marker = registered(&storage, "gap", &env);
    storage.delete(&session_marker_key("gap")).unwrap();
    time.advance(31 * MIN);
    let outcome = marker.refresh(&storage, env.clock.as_ref());
    assert!(matches!(outcome, RefreshOutcome::Missing), "{outcome:?}");
    let before = marker.report(env.clock.as_ref());
    assert!(!before.continuous() && before.missing, "{before:?}");

    marker.restart_record();
    let report = marker.report(env.clock.as_ref());
    assert!(report.continuous() && report.restarted, "{report:?}");
    let old = PACK_AGE_LIMIT + MIN;
    let unrestarted = MarkerReport {
        restarted: false,
        ..report.clone()
    };
    assert!(head_landing_check("s", &unrestarted, old, std::time::Duration::ZERO).is_ok());
    assert!(is_stale(&head_landing_check(
        "s",
        &report,
        old,
        std::time::Duration::ZERO
    )));
    assert!(head_landing_check("s", &report, YOUNG, HEAD_LANDING_ALLOWANCE).is_ok());
    assert!(commit_marker_check("s", Ok(true), &unrestarted, old).is_ok());
    assert!(is_stale(&commit_marker_check("s", Ok(true), &report, old)));
    assert!(commit_marker_check("s", Ok(true), &report, YOUNG).is_ok());

    time.advance(15 * MIN);
    let outcome = marker.refresh(&storage, env.clock.as_ref());
    assert!(
        matches!(outcome, RefreshOutcome::Refreshed { .. }),
        "{outcome:?}"
    );
    let after = marker.report(env.clock.as_ref());
    assert!(
        after.continuous() && !after.missing && after.restarted,
        "{after:?}"
    );
}

/// A marker of `id` written now with `flags`.
fn write_flagged_marker(storage: &MemoryBackend, id: &str, committing: bool, writes_heads: bool) {
    let now = Utc::now().to_rfc3339();
    let entry = SessionEntry {
        committing,
        writes_heads,
        ..SessionEntry {
            hostname: "elsewhere".into(),
            pid: 7,
            registered_at: now.clone(),
            last_refresh: now,
            boot_id: None,
            installation_id: None,
            pid_ns: None,
            elapsed_secs: None,
            committing: false,
            writes_heads: false,
        }
    };
    storage
        .put(
            &format!("sessions/{id}.json"),
            &serde_json::to_vec(&entry).unwrap(),
        )
        .unwrap();
}

/// The single-writer listing: no other session, or only unflagged ones,
/// is `None`; a committing one `Committing`; a `writes_heads` one
/// `Compact`, also beside a committing one; its own session never counts;
/// a malformed newest key counts as `Compact` (fail closed); a stale
/// flagged session is reaped and not reported; orphan journals stay.
#[test]
fn single_writer_listing_finds_head_writers() {
    let env = env_on("other-host", None, pid_always_alive);
    let fresh = || {
        let storage = MemoryBackend::new();
        register_session(&storage, "own", &env).unwrap();
        storage
    };
    let list = |s: &MemoryBackend| single_writer_listing(s, &env, "own").unwrap();

    let s = fresh();
    assert!(list(&s).is_none());
    write_flagged_marker(&s, "idle", false, false);
    assert!(list(&s).is_none());
    write_flagged_marker(&s, "backup", true, false);
    assert!(matches!(list(&s), Some(HeadWriter::Committing(_))));
    write_flagged_marker(&s, "compact", false, true);
    assert!(matches!(list(&s), Some(HeadWriter::Compact(_))));

    // Its own flags never count.
    let s = MemoryBackend::new();
    let reg = crate::repo::sessions::register_session_with(
        &s,
        "own",
        &env,
        crate::repo::sessions::MarkerFlags {
            committing: true,
            writes_heads: true,
        },
    )
    .unwrap();
    drop(reg);
    assert!(list(&s).is_none());

    let s = fresh();
    s.put("sessions/bad.json", b"{").unwrap();
    assert!(matches!(list(&s), Some(HeadWriter::Compact(_))));

    let s = fresh();
    let stale = (Utc::now() - Duration::minutes(46)).to_rfc3339();
    let entry = SessionEntry {
        committing: true,
        writes_heads: true,
        ..serde_json::from_slice::<SessionEntry>(&{
            write_session_marker(&s, "gone", &stale);
            s.get("sessions/gone.json").unwrap().unwrap()
        })
        .unwrap()
    };
    s.put("sessions/gone.json", &serde_json::to_vec(&entry).unwrap())
        .unwrap();
    s.put("sessions/orphan.index", b"journal").unwrap();
    assert!(list(&s).is_none(), "a stale flagged session is reaped");
    assert!(!s.exists("sessions/gone.json").unwrap());
    assert!(s.exists("sessions/orphan.index").unwrap(), "journals stay");
}

/// `vykar clear-sessions --stale` (`sessions::clear_stale_sessions`): it
/// removes exactly what the reapers remove, on the store's clock read with
/// a probe, and never a live marker. On a real `LocalBackend`, mtimes set
/// by hand.
mod clear_stale {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use chrono::Utc;
    use vykar_storage::local_backend::LocalBackend;
    use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
    use vykar_types::error::Result;

    use super::me;
    use crate::repo::HostEnv;
    use crate::repo::sessions::{SessionEntry, clear_stale_sessions};

    const STALE: Duration = Duration::from_secs(46 * 60);

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

    /// A marker of another machine (judged by age alone), fresh by its JSON.
    fn other_host() -> SessionEntry {
        let now = Utc::now().to_rfc3339();
        SessionEntry {
            hostname: "otherhost".into(),
            pid: 4242,
            registered_at: now.clone(),
            last_refresh: now,
            boot_id: None,
            installation_id: None,
            pid_ns: None,
            elapsed_secs: Some(0),
            committing: false,
            writes_heads: false,
        }
    }

    /// A marker of this machine (`me()`) and boot, written by `pid`.
    fn this_host(pid: u32) -> SessionEntry {
        let me = me();
        SessionEntry {
            hostname: me.hostname,
            pid,
            boot_id: me.boot_id,
            installation_id: me.installation_id,
            pid_ns: me.pid_ns,
            ..other_host()
        }
    }

    fn put(storage: &dyn StorageBackend, key: &str, entry: &SessionEntry) {
        storage
            .put(key, &serde_json::to_vec(entry).unwrap())
            .unwrap();
    }

    /// Running as `me()`, where only PID 1 (this process) and 77 are alive.
    fn env() -> HostEnv {
        HostEnv::system()
            .with_host(me())
            .with_pid_alive(Arc::new(|_, pid| pid == 1 || pid == 77))
    }

    /// Stale by age on the store's clock (a shared marker, an exclusive one,
    /// a committing one, a malformed one): removed with every listed key; a
    /// session's journal is kept. Live by mtime although old by its JSON, a
    /// live committing mark and a live exclusive marker: kept, unread
    /// beyond their GET.
    #[test]
    fn removes_markers_stale_on_the_store_clock_and_keeps_live_ones() {
        let (dir, s) = local();
        let committing = SessionEntry {
            committing: true,
            ..other_host()
        };
        put(&s, "sessions/old.json", &other_host());
        put(&s, "sessions/old.3.json", &other_host());
        s.put("sessions/old.index", b"journal").unwrap();
        put(&s, "sessions/x-old.json", &other_host());
        put(&s, "sessions/oldcommit.json", &committing);
        s.put("sessions/bad.json", b"{").unwrap();
        for key in [
            "sessions/old.json",
            "sessions/old.3.json",
            "sessions/x-old.json",
            "sessions/oldcommit.json",
            "sessions/bad.json",
        ] {
            age(dir.path(), key, STALE);
        }
        let ancient = SessionEntry {
            last_refresh: (Utc::now() - chrono::Duration::hours(5)).to_rfc3339(),
            ..other_host()
        };
        put(&s, "sessions/fresh.json", &ancient);
        put(&s, "sessions/commit.json", &committing);
        put(&s, "sessions/x-live.json", &other_host());

        let out = clear_stale_sessions(&s, &env()).unwrap();
        assert!(out.store_now_known);
        assert_eq!(out.removed, ["bad", "old", "oldcommit", "x-old"]);
        let live: Vec<_> = out.live.iter().map(|l| l.session_id.as_str()).collect();
        assert_eq!(live, ["commit", "fresh", "x-live"]);
        for gone in [
            "sessions/old.json",
            "sessions/old.3.json",
            "sessions/x-old.json",
            "sessions/oldcommit.json",
            "sessions/bad.json",
        ] {
            assert!(!s.exists(gone).unwrap(), "{gone}");
        }
        for kept in [
            "sessions/old.index",
            "sessions/fresh.json",
            "sessions/commit.json",
            "sessions/x-live.json",
        ] {
            assert!(s.exists(kept).unwrap(), "{kept}");
        }
        // The clock probe left nothing behind.
        let mut left = s.list("sessions/").unwrap();
        left.sort();
        assert_eq!(
            left,
            [
                "sessions/commit.json",
                "sessions/fresh.json",
                "sessions/old.index",
                "sessions/x-live.json",
            ]
        );
    }

    /// The same-host rule: a fresh marker of this machine and boot whose
    /// process is gone is removed at once, an exclusive one included; one
    /// whose process lives, and a fresh one of another machine, are kept.
    #[test]
    fn removes_markers_of_a_process_gone_on_this_machine_at_once() {
        let (_dir, s) = local();
        put(&s, "sessions/dead.json", &this_host(9));
        put(&s, "sessions/x-dead.json", &this_host(10));
        put(&s, "sessions/alive.json", &this_host(77));
        put(&s, "sessions/x-alive.json", &this_host(77));
        put(&s, "sessions/remote.json", &other_host());

        let out = clear_stale_sessions(&s, &env()).unwrap();
        assert_eq!(out.removed, ["dead", "x-dead"]);
        let live: Vec<_> = out.live.iter().map(|l| l.session_id.as_str()).collect();
        assert_eq!(live, ["alive", "remote", "x-alive"]);
    }

    /// Without a reading of the store's clock (the probe is not listed) nothing
    /// listed with an mtime is stale by age; a process gone on this machine
    /// is still proven gone.
    #[test]
    fn without_the_store_clock_only_same_host_markers_go() {
        struct NoProbe(LocalBackend);
        impl InnerBackend for NoProbe {
            fn inner_backend(&self) -> &dyn StorageBackend {
                &self.0
            }
        }
        delegate_storage_backend! {
            for NoProbe;
            except [list_meta];

            fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
                let mut listing = self.0.list_meta(prefix)?;
                listing.retain(|o| !o.key.starts_with("sessions/vykar-clock-"));
                Ok(listing)
            }
        }
        let (dir, s) = local();
        put(&s, "sessions/old.json", &other_host());
        age(dir.path(), "sessions/old.json", STALE);
        put(&s, "sessions/dead.json", &this_host(9));
        let s = NoProbe(s);

        let out = clear_stale_sessions(&s, &env()).unwrap();
        assert!(!out.store_now_known);
        assert_eq!(out.removed, ["dead"]);
        assert!(s.exists("sessions/old.json").unwrap());
    }

    /// The race with a heartbeat: a session listed stale whose next key is
    /// written between the listing and the deletes keeps that key: only the
    /// keys the listing showed are deleted.
    #[test]
    fn a_key_written_after_the_listing_is_not_deleted() {
        struct Heartbeat(LocalBackend);
        impl InnerBackend for Heartbeat {
            fn inner_backend(&self) -> &dyn StorageBackend {
                &self.0
            }
        }
        delegate_storage_backend! {
            for Heartbeat;
            except [get];

            // The judge's GET of the newest listed key: the session's
            // heartbeat writes its next key now.
            fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
                if key == "sessions/beat.json" {
                    self.0.put(
                        "sessions/beat.1.json",
                        &serde_json::to_vec(&other_host()).unwrap(),
                    )?;
                }
                self.0.get(key)
            }
        }
        let (dir, s) = local();
        put(&s, "sessions/beat.json", &other_host());
        age(dir.path(), "sessions/beat.json", STALE);
        let s = Heartbeat(s);

        let out = clear_stale_sessions(&s, &env()).unwrap();
        assert_eq!(out.removed, ["beat"]);
        assert!(!s.exists("sessions/beat.json").unwrap());
        assert!(s.exists("sessions/beat.1.json").unwrap());
    }
    /// A failed clock-probe PUT (a store that refuses the probe) is "now
    /// unknown", as an unlisted probe is: a marker stale by its mtime is
    /// kept, one of a process gone on this machine still goes.
    #[test]
    fn a_failed_probe_put_judges_no_marker_by_age() {
        struct NoProbePut(LocalBackend);
        impl InnerBackend for NoProbePut {
            fn inner_backend(&self) -> &dyn StorageBackend {
                &self.0
            }
        }
        delegate_storage_backend! {
            for NoProbePut;
            except [put, put_pack, begin_staged_put, conditional, profile];

            // Test wrapper: keeps every write on `put`.
            fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
                None
            }

            fn profile(&self) -> vykar_storage::BackendProfile {
                self.0.profile()
            }

            fn put(&self, key: &str, data: &[u8]) -> Result<()> {
                if key.starts_with("sessions/vykar-clock-") {
                    return Err(vykar_types::error::VykarError::Other("denied".into()));
                }
                self.0.put(key, data)
            }

            // Packs and staged writes never take a probe key.
            fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
                self.0.put_pack(key, data)
            }

            fn begin_staged_put(
                &self,
                prefix: &str,
            ) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
                self.0.begin_staged_put(prefix)
            }
        }
        let (dir, s) = local();
        put(&s, "sessions/old.json", &other_host());
        age(dir.path(), "sessions/old.json", STALE);
        put(&s, "sessions/dead.json", &this_host(9));
        let s = NoProbePut(s);

        let out = clear_stale_sessions(&s, &env()).unwrap();
        assert!(!out.store_now_known);
        assert_eq!(out.removed, ["dead"]);
        let live: Vec<_> = out.live.iter().map(|l| l.session_id.as_str()).collect();
        assert_eq!(live, ["old"]);
        assert!(s.exists("sessions/old.json").unwrap());
    }

    /// Only markers go: a crash-recovery journal whose marker is gone (an
    /// interrupted backup's), the journal of a session removed now, and
    /// probe debris older than `CLOCK_PROBE_DEBRIS_AGE` all stay, however
    /// often `--stale` runs; maintenance's reap removes the orphans.
    #[test]
    fn journals_and_probe_debris_stay() {
        let (dir, s) = local();
        s.put("sessions/orphan.index", b"journal").unwrap();
        put(&s, "sessions/old.json", &other_host());
        s.put("sessions/old.index", b"journal").unwrap();
        age(dir.path(), "sessions/old.json", STALE);
        s.put("sessions/vykar-clock-debris", b"").unwrap();
        age(
            dir.path(),
            "sessions/vykar-clock-debris",
            Duration::from_secs(2 * 3600),
        );

        let first = clear_stale_sessions(&s, &env()).unwrap();
        assert!(first.store_now_known);
        assert_eq!(first.removed, ["old"]);
        let second = clear_stale_sessions(&s, &env()).unwrap();
        assert!(second.removed.is_empty());
        let mut left = s.list("sessions/").unwrap();
        left.sort();
        assert_eq!(
            left,
            [
                "sessions/old.index",
                "sessions/orphan.index",
                "sessions/vykar-clock-debris",
            ]
        );
    }

    /// `sessions::legacy_locks` on copies of the frozen v2 and v3 fixtures
    /// (`locks/` may be live), on a v6 config (dead), and on a missing or
    /// undecodable config (unknown).
    #[test]
    fn legacy_locks_are_dead_only_on_a_v6_config() {
        use crate::repo::sessions::{LegacyLocks, legacy_locks};
        for (prefix, version) in [("v2", 2u32), ("v3", 3)] {
            let tmp = tempfile::tempdir().unwrap();
            let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures")
                .join(format!("{prefix}-none.tar.gz"));
            tar::Archive::new(flate2::read::GzDecoder::new(
                std::fs::File::open(&archive).unwrap(),
            ))
            .unpack(tmp.path())
            .unwrap();
            let repo = LocalBackend::new(tmp.path().join("repo").to_str().unwrap()).unwrap();
            assert_eq!(legacy_locks(&repo), LegacyLocks::NotCurrent { version });
        }

        let (_dir, s) = local();
        assert!(matches!(legacy_locks(&s), LegacyLocks::Unknown(_)));
        s.put("config", b"\xc1").unwrap();
        assert!(matches!(legacy_locks(&s), LegacyLocks::Unknown(_)));
        let v6 = crate::repo::RepoConfig::new_v6(
            vec![0u8; 32],
            crate::chunker::ChunkerParams::default(),
            crate::repo::EncryptionMode::Aes256Gcm,
            Utc::now(),
            1 << 20,
            32 << 20,
            vykar_types::hash::HashAlgorithm::Blake3,
        );
        s.put("config", &v6.encode().unwrap()).unwrap();
        assert_eq!(legacy_locks(&s), LegacyLocks::Dead);
    }
}
