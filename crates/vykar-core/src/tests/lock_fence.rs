use std::sync::Arc;
use std::time::SystemTime;

use crate::compress::Compression;
use crate::repo::lock::{
    acquire_lock, build_lock_fence, build_lock_fence_with_epoch, release_lock, verify_lock_validity,
};
use crate::repo::pack::PackType;
use crate::testutil::MemoryBackend;
use vykar_storage::StorageBackend;
use vykar_types::error::VykarError;

#[test]
fn fence_ok_for_fresh_lock() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let guard = acquire_lock(&*storage).unwrap();
    let fence = build_lock_fence(&guard, Arc::clone(&storage));
    fence().expect("fence should succeed for fresh lock");
    release_lock(&*storage, guard).unwrap();
}

#[test]
fn fence_fails_when_lock_file_deleted() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let guard = acquire_lock(&*storage).unwrap();
    let fence = build_lock_fence(&guard, Arc::clone(&storage));

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
    let storage = MemoryBackend::new();
    let guard = acquire_lock(&storage).unwrap();

    // Simulate lock acquired 6 hours ago (exceeds LOCK_MAX_UNREFRESHED_SECS = 5h30m).
    let now_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let old_secs = now_secs - 6 * 3600;

    let err = verify_lock_validity(guard.key(), old_secs, &storage).unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(ref msg) if msg.contains("unrefreshed")),
        "expected LockExpired with 'unrefreshed', got: {err}"
    );
    release_lock(&storage, guard).unwrap();
}

#[test]
fn fence_fails_on_clock_anomaly() {
    let storage = MemoryBackend::new();
    let guard = acquire_lock(&storage).unwrap();

    // Simulate last_refreshed_secs far in the future (clock went backwards).
    let future_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 3600;

    let err = verify_lock_validity(guard.key(), future_secs, &storage).unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(ref msg) if msg.contains("clock anomaly")),
        "expected LockExpired with 'clock anomaly', got: {err}"
    );
    release_lock(&storage, guard).unwrap();
}

#[test]
fn fence_refreshes_lock_file() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let guard = acquire_lock(&*storage).unwrap();
    let lock_key = guard.key().to_string();
    let hostname = crate::platform::hostname();

    // Set last_refreshed 4 hours ago (exceeds 3h refresh interval, within 5.5h max).
    let now_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let old_secs = now_secs - 4 * 3600;

    let fence = build_lock_fence_with_epoch(&guard, Arc::clone(&storage), old_secs);

    // The fence should succeed (4h < 5.5h max) and trigger a refresh.
    fence().expect("fence should succeed");

    // Verify lock file data was rewritten with a fresh timestamp.
    let data = storage.get(&lock_key).unwrap().unwrap();
    let entry: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(entry["hostname"].as_str().unwrap(), hostname);

    // A second call should also succeed (last_refreshed was updated by the first call).
    fence().expect("fence should succeed on second call");

    release_lock(&*storage, guard).unwrap();
}

#[test]
fn fence_blocks_persist_index() {
    crate::testutil::init_test_environment();

    let mut repo = crate::testutil::test_repo_plaintext();

    // Store a chunk to dirty the index.
    repo.store_chunk(b"hello world", Compression::None, PackType::Data)
        .unwrap();

    // Install an always-fail fence.
    let fail_fence: Arc<dyn Fn() -> vykar_types::error::Result<()> + Send + Sync> =
        Arc::new(|| Err(VykarError::LockExpired("test fence".into())));
    repo.set_lock_fence(fail_fence);

    // save_state should fail due to the fence.
    let err = repo.save_state().unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(_)),
        "expected LockExpired, got: {err}"
    );
}

#[test]
fn fence_blocks_persist_index_via_mark_dirty() {
    crate::testutil::init_test_environment();

    let mut repo = crate::testutil::test_repo_plaintext();

    // Dirty the index so save_state will try to persist it.
    repo.mark_index_dirty();

    // Install an always-fail fence.
    let fail_fence: Arc<dyn Fn() -> vykar_types::error::Result<()> + Send + Sync> =
        Arc::new(|| Err(VykarError::LockExpired("test fence".into())));
    repo.set_lock_fence(fail_fence);

    // save_state should fail due to the fence.
    let err = repo.save_state().unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(_)),
        "expected LockExpired, got: {err}"
    );
}

#[test]
fn fence_none_allows_writes() {
    crate::testutil::init_test_environment();

    let mut repo = crate::testutil::test_repo_plaintext();

    // Store a chunk.
    repo.store_chunk(b"hello world", Compression::None, PackType::Data)
        .unwrap();

    // No fence installed — save_state should succeed.
    repo.save_state().unwrap();
}

#[test]
fn maintenance_lock_fence_blocks_stale_write() {
    crate::testutil::init_test_environment();

    use crate::commands::util::with_maintenance_lock;
    use crate::config::ChunkerConfig;
    use crate::repo::{EncryptionMode, Repository};

    let storage = MemoryBackend::new();
    let mut repo = Repository::init(
        Box::new(storage),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        None,
    )
    .unwrap();

    let result = with_maintenance_lock(&mut repo, |repo| {
        // Begin a write session so save_state has something to write.
        repo.begin_write_session()?;
        repo.store_chunk(b"data", Compression::None, PackType::Data)?;

        // Delete the lock file to simulate suspension/expiry.
        let lock_keys = repo.storage.list("locks/").unwrap();
        for key in &lock_keys {
            if key.ends_with(".json") {
                repo.storage.delete(key).unwrap();
            }
        }

        // save_state should fail because the fence detects the deleted lock.
        repo.save_state()
    });

    let err = result.unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(_)),
        "expected LockExpired, got: {err}"
    );
}
