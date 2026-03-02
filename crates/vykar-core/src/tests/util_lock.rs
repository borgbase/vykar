use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::commands::util::with_repo_lock;
use crate::compress::Compression;
use crate::config::{ChunkerConfig, RepositoryConfig, RetryConfig};
use crate::repo::pack::PackType;
use crate::repo::{EncryptionMode, Repository};
use vykar_storage::{BackendLockInfo, StorageBackend};
use vykar_types::error::{Result, VykarError};

#[derive(Clone)]
struct AdvisoryLockBackend {
    state: Arc<AdvisoryLockBackendState>,
}

struct AdvisoryLockBackendState {
    data: Mutex<HashMap<String, Vec<u8>>>,
    release_calls: AtomicUsize,
    fail_release: bool,
}

impl AdvisoryLockBackend {
    fn new(fail_release: bool) -> Self {
        Self {
            state: Arc::new(AdvisoryLockBackendState {
                data: Mutex::new(HashMap::new()),
                release_calls: AtomicUsize::new(0),
                fail_release,
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
        let mut map = self.state.data.lock().unwrap();
        map.insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
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

    fn acquire_advisory_lock(&self, _lock_id: &str, _info: &BackendLockInfo) -> Result<()> {
        Ok(())
    }

    fn release_advisory_lock(&self, _lock_id: &str) -> Result<()> {
        self.state.release_calls.fetch_add(1, Ordering::SeqCst);
        if self.state.fail_release {
            Err(VykarError::Other("forced release failure".into()))
        } else {
            Ok(())
        }
    }
}

fn init_repo_with_backend(backend: AdvisoryLockBackend) -> Repository {
    Repository::init(
        Box::new(backend),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
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
fn with_repo_lock_returns_release_error_when_action_succeeds() {
    let backend = AdvisoryLockBackend::new(true);
    let mut repo = init_repo_with_backend(backend.clone());

    let result: Result<()> = with_repo_lock(&mut repo, |_repo| Ok(()));
    assert!(matches!(result, Err(VykarError::Other(msg)) if msg == "forced release failure"));
    assert_eq!(backend.release_calls(), 1);
}

#[test]
fn with_repo_lock_flushes_pending_state_on_action_error() {
    crate::testutil::init_test_environment();

    let backend = AdvisoryLockBackend::new(false);
    let small_config = RepositoryConfig {
        url: String::new(),
        region: None,
        access_key_id: None,
        secret_access_key: None,
        sftp_key: None,
        sftp_known_hosts: None,
        sftp_max_connections: None,
        access_token: None,
        allow_insecure_http: false,
        min_pack_size: 256,
        max_pack_size: 256,
        retry: RetryConfig::default(),
    };
    let mut repo = Repository::init(
        Box::new(backend.clone()),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        Some(&small_config),
        None,
    )
    .unwrap();

    let result: Result<()> = with_repo_lock(&mut repo, |repo| {
        // Begin a write session, store enough to trigger a pack flush, then fail.
        repo.begin_write_session()?;
        repo.store_chunk(&vec![0xABu8; 300], Compression::None, PackType::Data)?;
        Err(VykarError::Other("simulated backup failure".into()))
    });

    assert!(result.is_err());
    // flush_on_abort should have written pending_index before releasing the lock.
    assert!(
        repo.storage.exists("sessions/default.index").unwrap(),
        "sessions/default.index should exist after with_repo_lock error path"
    );
    assert_eq!(backend.release_calls(), 1, "lock should still be released");
}
