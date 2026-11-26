use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::commands::util::with_repo_lock;
use crate::compress::Compression;
use crate::repo::ChunkerParams;
use crate::repo::InitOptions;
use crate::repo::pack::PackType;
use crate::repo::{EncryptionMode, Fence, Repo, WriteSessionOptions};
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};

#[derive(Clone)]
struct AdvisoryLockBackend {
    state: Arc<AdvisoryLockBackendState>,
}

struct AdvisoryLockBackendState {
    data: Mutex<HashMap<String, Vec<u8>>>,
    release_calls: AtomicUsize,
    fail_release: bool,
    /// The next this many PUTs of a session journal fail.
    fail_journal_puts: AtomicUsize,
}

impl AdvisoryLockBackend {
    fn new(fail_release: bool) -> Self {
        Self {
            state: Arc::new(AdvisoryLockBackendState {
                data: Mutex::new(HashMap::new()),
                release_calls: AtomicUsize::new(0),
                fail_release,
                fail_journal_puts: AtomicUsize::new(0),
            }),
        }
    }

    fn release_calls(&self) -> usize {
        self.state.release_calls.load(Ordering::SeqCst)
    }
}

impl StorageBackend for AdvisoryLockBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let map = self.state.data.lock().unwrap();
        Ok(map.get(key).cloned())
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if key.ends_with(".index")
            && self
                .state
                .fail_journal_puts
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            return Err(VykarError::Other("injected journal PUT failure".into()));
        }
        let mut map = self.state.data.lock().unwrap();
        map.insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        // Track lock release via delete of lock files.
        if key.starts_with("locks/") {
            self.state.release_calls.fetch_add(1, Ordering::SeqCst);
            if self.state.fail_release {
                return Err(VykarError::Other("forced release failure".into()));
            }
        }
        let mut map = self.state.data.lock().unwrap();
        map.remove(key);
        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let map = self.state.data.lock().unwrap();
        Ok(map.contains_key(key))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let map = self.state.data.lock().unwrap();
        Ok(map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let map = self.state.data.lock().unwrap();
        let Some(data) = map.get(key) else {
            return Ok(None);
        };
        let start = offset as usize;
        let end = start.checked_add(length as usize).ok_or_else(|| {
            VykarError::Other(format!(
                "short read on {key} at offset {offset}: offset + length overflows usize"
            ))
        })?;
        if start >= data.len() || end > data.len() {
            return Err(VykarError::Other(format!(
                "short read on {key} at offset {offset}: expected {length} bytes, got {}",
                data.len().saturating_sub(start)
            )));
        }
        Ok(Some(data[start..end].to_vec()))
    }

    fn create_dir(&self, _key: &str) -> Result<()> {
        Ok(())
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            local: false,
            staged_put: false,
            request_width: 1,
            transfer_width: 1,
        }
    }

    vykar_storage::unsupported_server_ops!();
}

fn init_repo_with_backend(backend: AdvisoryLockBackend) -> Repo {
    Repo::init(
        Box::new(backend),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap()
}

#[test]
fn with_repo_lock_keeps_original_action_error_if_release_also_fails() {
    let backend = AdvisoryLockBackend::new(true);
    let mut repo = init_repo_with_backend(backend.clone());

    let result: Result<()> =
        with_repo_lock(&mut repo, |_repo| Err(VykarError::Other("boom".into())));
    assert!(matches!(result, Err(VykarError::Other(msg)) if msg == "boom"));
    assert_eq!(backend.release_calls(), 1);
}

#[test]
fn with_repo_lock_succeeds_when_release_fails_after_successful_action() {
    // Post-commit policy: if the action succeeded, a lock-release failure is
    // non-fatal (tracing::warn! only). The operation has already committed to
    // storage; propagating a release error would incorrectly report a failure.
    // The leaked lock ages out in 6 hours or can be cleared with `vykar
    // break-lock` — see commands/util.rs run_under_fence.
    let backend = AdvisoryLockBackend::new(true);
    let mut repo = init_repo_with_backend(backend.clone());

    let result: Result<()> = with_repo_lock(&mut repo, |_repo| Ok(()));
    assert!(
        result.is_ok(),
        "release failure must not fail a successful action"
    );
    assert_eq!(backend.release_calls(), 1);
}

#[test]
fn action_flushes_its_session_before_with_repo_lock_releases() {
    crate::testutil::init_test_environment();

    let backend = AdvisoryLockBackend::new(false);
    let small_config = InitOptions {
        min_pack_size: 256,
        max_pack_size: 256,
        ..Default::default()
    };
    let mut repo = Repo::init(
        Box::new(backend.clone()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(&small_config),
        None,
    )
    .unwrap();

    let result: Result<()> = with_repo_lock(&mut repo, |repo| {
        // Begin a write session, store enough to trigger a pack flush, then
        // fail: the session's owner flushes it on its error path.
        let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
        ws.store_chunk(repo, &vec![0xABu8; 300], Compression::None, PackType::Data)?;
        ws.flush_on_abort(repo);
        Err(VykarError::Other("simulated backup failure".into()))
    });

    assert!(result.is_err());
    // flush_on_abort wrote pending_index before the lock was released.
    assert!(
        repo.storage().exists("sessions/default.index").unwrap(),
        "sessions/default.index should exist after with_repo_lock error path"
    );
    assert_eq!(backend.release_calls(), 1, "lock should still be released");
}

/// A failed `save_session` flushes its session before the error returns:
/// the journal PUT during the session's own flush fails, then the fence
/// refuses the index write, and only the error arm's rewrite of the journal
/// (`flush_on_abort`) leaves `sessions/default.index` behind.
#[test]
fn a_failed_save_session_writes_the_journal() {
    crate::testutil::init_test_environment();

    let backend = AdvisoryLockBackend::new(false);
    let small_config = InitOptions {
        min_pack_size: 256,
        max_pack_size: 256,
        ..Default::default()
    };
    let mut repo = Repo::init(
        Box::new(backend.clone()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(&small_config),
        None,
    )
    .unwrap();

    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    ws.store_chunk(&repo, &[0xABu8; 300], Compression::None, PackType::Data)
        .unwrap();
    backend.state.fail_journal_puts.store(1, Ordering::SeqCst);
    let err = repo
        .writer(Fence::lock(Arc::new(|| {
            Err(VykarError::LockExpired("lost".into()))
        })))
        .save_session(ws)
        .unwrap_err();

    assert!(matches!(err, VykarError::LockExpired(_)), "{err}");
    assert_eq!(
        backend.state.fail_journal_puts.load(Ordering::SeqCst),
        0,
        "the session's own flush tried the journal first"
    );
    assert!(
        repo.storage().exists("sessions/default.index").unwrap(),
        "the failed save flushed its session"
    );
}
