use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::file_cache::repo_cache_dir;
use super::format::{pack_object_with_context, unpack_object_expect_with_context, ObjectType};
use super::manifest::SnapshotEntry;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::error::Result;
use vykar_types::snapshot_id::SnapshotId;

const SNAPSHOT_CACHE_CONTEXT: &[u8] = b"snapshot_cache";

/// Cached snapshot entries, keyed by snapshot ID hex.
/// Persisted locally at `<cache>/vykar/<repo_id_hex>/snapshot_list`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotListCache {
    pub entries: HashMap<String, SnapshotEntry>,
}

impl SnapshotListCache {
    fn cache_path(repo_id: &[u8], cache_dir_override: Option<&Path>) -> Option<PathBuf> {
        repo_cache_dir(repo_id, cache_dir_override).map(|d| d.join("snapshot_list"))
    }

    /// Load the snapshot list cache from local disk.
    /// Returns an empty cache on any error.
    pub fn load(
        repo_id: &[u8],
        crypto: &dyn CryptoEngine,
        cache_dir_override: Option<&Path>,
    ) -> Self {
        let Some(path) = Self::cache_path(repo_id, cache_dir_override) else {
            return Self::default();
        };
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(_) => return Self::default(),
        };
        let plaintext = match unpack_object_expect_with_context(
            &data,
            ObjectType::SnapshotCache,
            SNAPSHOT_CACHE_CONTEXT,
            crypto,
        ) {
            Ok(pt) => pt,
            Err(_) => {
                debug!("snapshot list cache: failed to decrypt, starting fresh");
                return Self::default();
            }
        };
        match rmp_serde::from_slice(&plaintext) {
            Ok(cache) => cache,
            Err(e) => {
                debug!("snapshot list cache: failed to deserialize: {e}, starting fresh");
                Self::default()
            }
        }
    }

    /// Save the snapshot list cache to local disk.
    pub fn save(
        &self,
        repo_id: &[u8],
        crypto: &dyn CryptoEngine,
        cache_dir_override: Option<&Path>,
    ) -> Result<()> {
        let Some(path) = Self::cache_path(repo_id, cache_dir_override) else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let plaintext = rmp_serde::to_vec(self)?;
        let packed = pack_object_with_context(
            ObjectType::SnapshotCache,
            SNAPSHOT_CACHE_CONTEXT,
            &plaintext,
            crypto,
        )?;
        std::fs::write(&path, &packed)?;
        Ok(())
    }

    /// Convert to a Vec of SnapshotEntry, sorted chronologically by time.
    pub fn to_entries(&self) -> Vec<SnapshotEntry> {
        let mut entries: Vec<SnapshotEntry> = self.entries.values().cloned().collect();
        entries.sort_by_key(|e| e.time);
        entries
    }
}

/// Refresh the local snapshot cache by diffing against `snapshots/` on storage.
///
/// Decrypt/deserialize errors are always skipped with a warning — this prevents
/// a single garbage upload from bricking the repo in append-only mode.
///
/// When `strict_io` is true, I/O errors (GET failure, listed-but-not-found) are
/// treated as hard errors. Use this in the commit path where a transient failure
/// could hide an existing snapshot name and allow a duplicate.
/// When `strict_io` is false, I/O errors are warned and skipped (resilient open).
pub fn refresh_snapshot_cache(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    repo_id: &[u8],
    cache_dir_override: Option<&Path>,
    strict_io: bool,
) -> Result<Vec<SnapshotEntry>> {
    // Load existing local cache
    let mut cache = SnapshotListCache::load(repo_id, crypto, cache_dir_override);

    // List all snapshot keys on storage
    let remote_keys = storage.list("snapshots/")?;

    // Build set of remote snapshot ID hex strings
    let mut remote_ids: HashMap<String, String> = HashMap::new();
    for key in &remote_keys {
        // key is "snapshots/<id_hex>"
        if let Some(id_hex) = key.strip_prefix("snapshots/") {
            if !id_hex.is_empty() {
                remote_ids.insert(id_hex.to_string(), key.clone());
            }
        }
    }

    // Remove stale entries (in cache but not on remote)
    cache
        .entries
        .retain(|id_hex, _| remote_ids.contains_key(id_hex));

    // Load new entries (on remote but not in cache)
    for (id_hex, storage_key) in &remote_ids {
        if cache.entries.contains_key(id_hex) {
            continue;
        }

        // Parse the snapshot ID
        let snapshot_id = match SnapshotId::from_hex(id_hex) {
            Ok(id) => id,
            Err(e) => {
                warn!("skipping snapshot with invalid ID {id_hex}: {e}");
                continue;
            }
        };

        // Download and decrypt the snapshot blob
        let blob = match storage.get(storage_key) {
            Ok(Some(b)) => b,
            Ok(None) => {
                if strict_io {
                    return Err(vykar_types::error::VykarError::Other(format!(
                        "snapshot {id_hex} listed but not found (strict I/O mode)"
                    )));
                }
                warn!("snapshot {id_hex} listed but not found, skipping");
                continue;
            }
            Err(e) => {
                if strict_io {
                    return Err(vykar_types::error::VykarError::Other(format!(
                        "failed to fetch snapshot {id_hex}: {e} (strict I/O mode)"
                    )));
                }
                warn!("failed to fetch snapshot {id_hex}: {e}, skipping");
                continue;
            }
        };

        let meta_bytes = match unpack_object_expect_with_context(
            &blob,
            ObjectType::SnapshotMeta,
            snapshot_id.as_bytes(),
            crypto,
        ) {
            Ok(b) => b,
            Err(e) => {
                warn!("failed to decrypt snapshot {id_hex}: {e}, skipping");
                continue;
            }
        };

        let meta: crate::snapshot::SnapshotMeta = match rmp_serde::from_slice(&meta_bytes) {
            Ok(m) => m,
            Err(e) => {
                warn!("failed to deserialize snapshot {id_hex}: {e}, skipping");
                continue;
            }
        };

        let entry = SnapshotEntry {
            name: meta.name,
            id: snapshot_id,
            time: meta.time,
            source_label: meta.source_label,
            label: meta.label,
            source_paths: meta.source_paths,
            hostname: meta.hostname,
        };

        cache.entries.insert(id_hex.to_string(), entry);
    }

    // Persist updated cache (best-effort)
    if let Err(e) = cache.save(repo_id, crypto, cache_dir_override) {
        warn!("failed to save snapshot list cache: {e}");
    }

    Ok(cache.to_entries())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChunkerConfig;
    use crate::snapshot::SnapshotMeta;
    use crate::testutil::MemoryBackend;
    use vykar_crypto::PlaintextEngine;

    fn test_crypto() -> PlaintextEngine {
        PlaintextEngine::new(&[0xAA; 32])
    }

    fn make_snapshot_meta(name: &str) -> SnapshotMeta {
        SnapshotMeta {
            name: name.to_string(),
            hostname: "testhost".into(),
            username: "testuser".into(),
            time: chrono::Utc::now(),
            time_end: chrono::Utc::now(),
            chunker_params: ChunkerConfig::default(),
            comment: String::new(),
            item_ptrs: vec![],
            stats: Default::default(),
            source_label: "src".into(),
            source_paths: vec!["/data".into()],
            label: String::new(),
        }
    }

    /// Store a snapshot blob on the backend, returning the SnapshotId.
    fn store_snapshot(
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        name: &str,
    ) -> SnapshotId {
        let id = SnapshotId::generate();
        let meta = make_snapshot_meta(name);
        let meta_bytes = rmp_serde::to_vec(&meta).unwrap();
        let packed = pack_object_with_context(
            ObjectType::SnapshotMeta,
            id.as_bytes(),
            &meta_bytes,
            crypto,
        )
        .unwrap();
        storage.put(&id.storage_key(), &packed).unwrap();
        id
    }

    #[test]
    fn snapshot_list_cache_round_trip() {
        let entry = SnapshotEntry {
            name: "test-snap".into(),
            id: SnapshotId::generate(),
            time: chrono::Utc::now(),
            source_label: "src".into(),
            label: String::new(),
            source_paths: vec!["/data".into()],
            hostname: "host1".into(),
        };

        let mut cache = SnapshotListCache::default();
        cache.entries.insert(entry.id.to_hex(), entry.clone());

        let bytes = rmp_serde::to_vec(&cache).unwrap();
        let restored: SnapshotListCache = rmp_serde::from_slice(&bytes).unwrap();

        assert_eq!(restored.entries.len(), 1);
        let restored_entry = restored.entries.values().next().unwrap();
        assert_eq!(restored_entry.name, "test-snap");
        assert_eq!(restored_entry.hostname, "host1");
    }

    #[test]
    fn strict_io_fails_on_get_error() {
        let crypto = test_crypto();
        let storage = MemoryBackend::new();

        // Store a valid snapshot
        store_snapshot(&storage, &crypto, "snap1");

        // Store a key that will be listed but return garbage on GET
        // (simulated by storing undecryptable data — this tests decrypt skip, not I/O)
        // For a true I/O error, we need a failing backend.
        let fail_storage = FailGetBackend {
            inner: storage,
            fail_prefix: "snapshots/".to_string(),
        };

        let result = refresh_snapshot_cache(
            &fail_storage,
            &crypto,
            &[0xBB; 16],
            None,
            true, // strict_io
        );
        assert!(result.is_err(), "strict_io should fail on GET errors");
    }

    #[test]
    fn non_strict_io_skips_get_error() {
        let crypto = test_crypto();
        let storage = MemoryBackend::new();

        // Store a valid snapshot so list() returns something
        store_snapshot(&storage, &crypto, "snap1");

        let fail_storage = FailGetBackend {
            inner: storage,
            fail_prefix: "snapshots/".to_string(),
        };

        let result = refresh_snapshot_cache(
            &fail_storage,
            &crypto,
            &[0xBB; 16],
            None,
            false, // non-strict
        );
        assert!(result.is_ok(), "non-strict should skip GET errors");
        assert_eq!(result.unwrap().len(), 0, "no snapshots should be loaded");
    }

    #[test]
    fn strict_io_skips_decrypt_errors() {
        let crypto = test_crypto();
        let storage = MemoryBackend::new();

        // Store garbage data at a valid snapshot key
        let id = SnapshotId::generate();
        storage
            .put(&id.storage_key(), b"not a valid snapshot blob")
            .unwrap();

        // strict_io should skip decrypt errors (not fail hard)
        let result = refresh_snapshot_cache(
            &storage,
            &crypto,
            &[0xBB; 16],
            None,
            true, // strict_io
        );
        assert!(
            result.is_ok(),
            "strict_io should skip decrypt errors, got: {:?}",
            result.err()
        );
        assert_eq!(
            result.unwrap().len(),
            0,
            "garbage snapshot should be skipped"
        );
    }

    /// Backend that fails GET for keys matching a prefix but delegates everything else.
    struct FailGetBackend {
        inner: MemoryBackend,
        fail_prefix: String,
    }

    impl StorageBackend for FailGetBackend {
        fn get(&self, key: &str) -> vykar_types::error::Result<Option<Vec<u8>>> {
            if key.starts_with(&self.fail_prefix) {
                return Err(vykar_types::error::VykarError::Other(
                    "simulated I/O error".into(),
                ));
            }
            self.inner.get(key)
        }
        fn put(&self, key: &str, data: &[u8]) -> vykar_types::error::Result<()> {
            self.inner.put(key, data)
        }
        fn delete(&self, key: &str) -> vykar_types::error::Result<()> {
            self.inner.delete(key)
        }
        fn exists(&self, key: &str) -> vykar_types::error::Result<bool> {
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
    }
}
