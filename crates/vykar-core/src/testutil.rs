use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::Once;

use crate::config::ChunkerConfig;
use crate::repo::{EncryptionMode, Repository};
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};

static TEST_ENV_INIT: Once = Once::new();

pub fn init_test_environment() {
    TEST_ENV_INIT.call_once(|| {
        let base = std::env::temp_dir().join(format!("vykar-tests-{}", std::process::id()));
        let home = base.join("home");
        let cache = base.join("cache");
        let _ = std::fs::create_dir_all(&home);
        let _ = std::fs::create_dir_all(&cache);

        // Rust 2024 marks env mutation as unsafe due process-global races.
        // We do this once at test process startup to keep file-cache writes
        // under a writable temp root in sandboxed environments.
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("XDG_CACHE_HOME", &cache);
        }
    });
}

/// In-memory storage backend for testing. Thread-safe via Mutex.
pub struct MemoryBackend {
    data: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }
}

impl StorageBackend for MemoryBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let map = self.data.lock().unwrap();
        Ok(map.get(key).cloned())
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let mut map = self.data.lock().unwrap();
        map.insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let mut map = self.data.lock().unwrap();
        map.remove(key);
        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let map = self.data.lock().unwrap();
        Ok(map.contains_key(key))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let map = self.data.lock().unwrap();
        let keys: Vec<String> = map
            .keys()
            .filter(|k| k.starts_with(prefix) && !k.ends_with('/'))
            .cloned()
            .collect();
        Ok(keys)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let map = self.data.lock().unwrap();
        match map.get(key) {
            Some(data) => {
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
            None => Ok(None),
        }
    }

    fn create_dir(&self, _key: &str) -> Result<()> {
        // No-op for in-memory backend
        Ok(())
    }
}

/// Create a plaintext repository backed by MemoryBackend.
pub fn test_repo_plaintext() -> Repository {
    init_test_environment();
    let storage = Box::new(MemoryBackend::new());
    let mut repo = Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        None,
    )
    .expect("failed to init test repo");
    repo.begin_write_session()
        .expect("failed to begin write session");
    repo
}

/// Fixed chunk ID key for deterministic tests.
pub fn test_chunk_id_key() -> [u8; 32] {
    [0xAA; 32]
}

/// Shared handle to inspect which keys were written via `put()`.
#[derive(Clone)]
pub struct PutLog(std::sync::Arc<Mutex<Vec<String>>>);

impl PutLog {
    fn new() -> Self {
        Self(std::sync::Arc::new(Mutex::new(Vec::new())))
    }

    /// Return all keys that were written via `put()` since the last `clear()`.
    pub fn entries(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }

    /// Clear the recorded log.
    pub fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    fn record(&self, key: &str) {
        self.0.lock().unwrap().push(key.to_string());
    }
}

/// Storage wrapper that records which keys were passed to `put()`.
/// Delegates all operations to an inner `MemoryBackend`.
/// Use `RecordingBackend::new()` to get the backend and a shared `PutLog`.
pub struct RecordingBackend {
    inner: MemoryBackend,
    log: PutLog,
}

impl RecordingBackend {
    pub fn new() -> (Self, PutLog) {
        let log = PutLog::new();
        (
            Self {
                inner: MemoryBackend::new(),
                log: log.clone(),
            },
            log,
        )
    }
}

impl StorageBackend for RecordingBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.log.record(key);
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }
    fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.inner.get_range(key, offset, length)
    }
    fn create_dir(&self, key: &str) -> Result<()> {
        self.inner.create_dir(key)
    }
    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.log.record(key);
        self.inner.put(key, &data)
    }
}
