use std::sync::Arc;

use chrono::Utc;

use crate::compress::Compression;
use crate::repo::lock::{acquire_lock, build_lock_fence, release_lock, verify_lock_validity};
use crate::repo::pack::PackType;
use crate::repo::{Fence, HostEnv, SystemClock};
use crate::sim::env::{SimTime, TimeMode};
use crate::testutil::MemoryBackend;
use vykar_storage::StorageBackend;
use vykar_types::error::VykarError;

/// An environment on virtual time starting now.
fn virtual_env() -> (Arc<SimTime>, HostEnv) {
    let time = SimTime::new(Utc::now(), TimeMode::Auto);
    let env = HostEnv::system().with_clock(Arc::new(time.clock(chrono::Duration::zero())));
    (time, env)
}

const HOUR: std::time::Duration = std::time::Duration::from_secs(3600);

#[test]
fn fence_ok_for_fresh_lock() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let env = HostEnv::system();
    let guard = acquire_lock(&*storage, &env).unwrap();
    let fence = build_lock_fence(&guard, Arc::clone(&storage), env);
    fence().expect("fence should succeed for fresh lock");
    release_lock(&*storage, guard).unwrap();
}

#[test]
fn fence_fails_when_lock_file_deleted() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let env = HostEnv::system();
    let guard = acquire_lock(&*storage, &env).unwrap();
    let fence = build_lock_fence(&guard, Arc::clone(&storage), env);

    // Delete the lock file to simulate another client cleaning it up.
    storage.delete(guard.key()).unwrap();

    let err = fence().unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(_)),
        "expected LockExpired, got: {err}"
    );
}

#[test]
fn fence_fails_after_time_expiry() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (time, env) = virtual_env();
    let guard = acquire_lock(&*storage, &env).unwrap();
    let fence = build_lock_fence(&guard, Arc::clone(&storage), env);

    // Past LOCK_MAX_UNREFRESHED_SECS = 5h30m without a fence call.
    time.advance(5 * HOUR + 31 * 60 * std::time::Duration::from_secs(1));

    let err = fence().unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(ref msg) if msg.contains("unrefreshed")),
        "expected LockExpired with 'unrefreshed', got: {err}"
    );
    release_lock(&*storage, guard).unwrap();
}

#[test]
fn fence_fails_on_clock_anomaly() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (time, env) = virtual_env();
    let guard = acquire_lock(&*storage, &env).unwrap();
    let fence = build_lock_fence(&guard, Arc::clone(&storage), env);

    // The wall clock steps back an hour.
    time.jump_wall(chrono::Duration::hours(-1));

    let err = fence().unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(ref msg) if msg.contains("clock anomaly")),
        "expected LockExpired with 'clock anomaly', got: {err}"
    );

    // last_refreshed_secs in the future, on the system clock.
    let future_secs = Utc::now().timestamp() + 3600;
    let err = verify_lock_validity(guard.key(), future_secs, &*storage, &SystemClock).unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(ref msg) if msg.contains("clock anomaly")),
        "expected LockExpired with 'clock anomaly', got: {err}"
    );

    // A clock before 1970: the elapsed time is positive, the clock is not
    // believed.
    let old = SimTime::new(
        chrono::DateTime::from_timestamp(-50, 0).unwrap(),
        TimeMode::Auto,
    );
    let err = verify_lock_validity(
        guard.key(),
        -100,
        &*storage,
        &old.clock(chrono::Duration::zero()),
    )
    .unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(ref msg) if msg.contains("clock anomaly")),
        "expected LockExpired with 'clock anomaly', got: {err}"
    );
    release_lock(&*storage, guard).unwrap();
}

#[test]
fn fence_refreshes_lock_file() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (time, env) = virtual_env();
    let guard = acquire_lock(&*storage, &env).unwrap();
    let lock_key = guard.key().to_string();
    let fence = build_lock_fence(&guard, Arc::clone(&storage), env.clone());

    // Past the 3h refresh interval, within the 5.5h maximum.
    time.advance(4 * HOUR);
    fence().expect("fence should succeed");

    // The lock file was rewritten with the current time.
    let data = storage.get(&lock_key).unwrap().unwrap();
    let entry: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(entry["hostname"].as_str().unwrap(), env.host.hostname);
    let written = chrono::DateTime::parse_from_rfc3339(entry["time"].as_str().unwrap()).unwrap();
    assert_eq!(written.timestamp(), env.now_utc().timestamp());

    // 8h since acquisition but 4h since the refresh: still valid, so the
    // refresh reset the epoch.
    time.advance(4 * HOUR);
    fence().expect("fence should succeed on second call");

    release_lock(&*storage, guard).unwrap();
}

#[test]
fn fence_blocks_persist_index() {
    crate::testutil::init_test_environment();

    let (mut repo, mut ws) = crate::testutil::test_repo_plaintext_with_session();

    // Store a chunk to dirty the index.
    ws.store_chunk(&repo, b"hello world", Compression::None, PackType::Data)
        .unwrap();

    // An always-fail fence.
    let fail_fence: Arc<dyn Fn() -> vykar_types::error::Result<()> + Send + Sync> =
        Arc::new(|| Err(VykarError::LockExpired("test fence".into())));

    // save_session should fail due to the fence.
    let err = repo
        .writer(Fence::lock(fail_fence))
        .save_session(ws)
        .unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(_)),
        "expected LockExpired, got: {err}"
    );
}

#[test]
fn fence_blocks_persist_index_via_mark_dirty() {
    crate::testutil::init_test_environment();

    let mut repo = crate::testutil::test_repo_plaintext();

    // Edit the index so save_state will try to persist it.
    repo.unchecked_writer().chunk_index_mut().add(
        vykar_types::chunk_id::ChunkId::from_bytes([7; 32]),
        10,
        vykar_types::pack_id::PackId::from_bytes([8; 32]),
        0,
    );
    repo.unchecked_writer()
        .chunk_index_mut()
        .set_pack_trailer(vykar_types::pack_id::PackId::from_bytes([8; 32]), 0);

    // An always-fail fence.
    let fail_fence: Arc<dyn Fn() -> vykar_types::error::Result<()> + Send + Sync> =
        Arc::new(|| Err(VykarError::LockExpired("test fence".into())));

    // save_state should fail due to the fence.
    let err = repo
        .writer(Fence::lock(fail_fence))
        .save_state()
        .unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(_)),
        "expected LockExpired, got: {err}"
    );
}

#[test]
fn fence_none_allows_writes() {
    crate::testutil::init_test_environment();

    let (mut repo, mut ws) = crate::testutil::test_repo_plaintext_with_session();

    // Store a chunk.
    ws.store_chunk(&repo, b"hello world", Compression::None, PackType::Data)
        .unwrap();

    // An unchecked fence — save_session succeeds.
    repo.unchecked_writer().save_session(ws).unwrap();
}

#[test]
fn maintenance_lock_fence_blocks_stale_write() {
    crate::testutil::init_test_environment();

    use crate::commands::util::with_maintenance_lock;
    use crate::repo::ChunkerParams;
    use crate::repo::{EncryptionMode, Repo};

    let storage = MemoryBackend::new();
    let mut repo = Repo::init(
        Box::new(storage),
        EncryptionMode::None,
        ChunkerParams::default(),
        None,
        None,
        None,
    )
    .unwrap();

    let result = with_maintenance_lock(&mut repo, |repo| {
        // Begin a write session so save_state has something to write.
        let mut ws = repo.begin_write_session(&crate::repo::WriteSessionOptions::default());
        ws.store_chunk(repo, b"data", Compression::None, PackType::Data)?;

        // Delete the lock file to simulate suspension/expiry.
        let lock_keys = repo.storage().list("locks/").unwrap();
        for key in &lock_keys {
            if key.ends_with(".json") {
                repo.storage().delete(key).unwrap();
            }
        }

        // save_state should fail because the fence detects the deleted lock.
        repo.save_session(ws)
    });

    let err = result.unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(_)),
        "expected LockExpired, got: {err}"
    );
}
