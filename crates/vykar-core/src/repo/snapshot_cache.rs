use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::file_cache::{atomic_write, repo_cache_dir};
use super::format::{pack_object_with_context, unpack_object_expect_with_context, ObjectType};
use super::manifest::SnapshotEntry;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::error::Result;
use vykar_types::snapshot_id::SnapshotId;

const SNAPSHOT_CACHE_CONTEXT: &[u8] = b"snapshot_cache";

/// Why a snapshot that exists in `snapshots/` was left out of the manifest.
///
/// Every variant means the caller's view of the repository is **partial**: the
/// blob is on the server but this binary could not turn it into an entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// The stored positional array has a different field count than this build
    /// reads. The envelope is frozen and a *shorter* array still decodes via
    /// `#[serde(default)]`, so this can only be a *longer* array — a snapshot
    /// written by a newer vykar. See the Format Evolution section of
    /// `architecture.md`.
    IncompatibleEnvelope { stored_fields: Option<u32> },
    /// Decrypted and authenticated, but not a decodable `SnapshotMeta`.
    Undecodable(String),
    /// Failed AEAD decryption or authentication.
    Undecryptable(String),
    /// Listed under `snapshots/` but could not be fetched.
    Unavailable(String),
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SkipReason::IncompatibleEnvelope { stored_fields } => {
                match stored_fields {
                    Some(n) => write!(
                        f,
                        "stored envelope has {n} fields, this build reads {}",
                        crate::snapshot::SNAPSHOT_META_FIELD_COUNT
                    )?,
                    None => write!(
                        f,
                        "stored envelope does not match the {}-field layout this build reads",
                        crate::snapshot::SNAPSHOT_META_FIELD_COUNT
                    )?,
                }
                write!(f, " — written by a newer vykar; upgrade vykar on this host")
            }
            SkipReason::Undecodable(e) => write!(f, "metadata could not be decoded: {e}"),
            SkipReason::Undecryptable(e) => write!(f, "could not be decrypted: {e}"),
            SkipReason::Unavailable(e) => write!(f, "could not be fetched: {e}"),
        }
    }
}

/// A snapshot present on storage that this binary could not read.
#[derive(Debug, Clone)]
pub struct SkippedSnapshot {
    pub id_hex: String,
    pub reason: SkipReason,
}

/// Result of a snapshot-list refresh.
///
/// A non-empty `skipped` means `entries` is an **incomplete** view of the
/// repository; callers that present a snapshot list to a user must say so
/// rather than rendering a silently truncated list.
#[derive(Debug, Default)]
pub struct SnapshotRefresh {
    pub entries: Vec<SnapshotEntry>,
    pub skipped: Vec<SkippedSnapshot>,
}

/// One-line summary of an unreadable-snapshot set, or `None` when it is empty.
///
/// Returning a formatted string from the core is deliberate and consistent with
/// `PruneStats.warnings` / `CheckError.message`: it is a value the frontend
/// chooses where and whether to render, not a print.
pub fn describe_skipped(skipped: &[SkippedSnapshot]) -> Option<String> {
    if skipped.is_empty() {
        return None;
    }
    let n = skipped.len();
    let noun = if n == 1 { "snapshot" } else { "snapshots" };
    let version_skew = skipped
        .iter()
        .filter(|s| matches!(s.reason, SkipReason::IncompatibleEnvelope { .. }))
        .count();
    if version_skew == n {
        Some(format!(
            "{n} {noun} hidden: written by a newer vykar — upgrade vykar on this \
             host to see {}",
            if n == 1 { "it" } else { "them" }
        ))
    } else {
        Some(format!(
            "{n} {noun} hidden: this build could not read {} (see warnings above)",
            if n == 1 { "it" } else { "them" }
        ))
    }
}

/// Element count of a msgpack array header, if `bytes` starts with one.
///
/// Lets us report how many fields a foreign envelope actually carries;
/// `rmp_serde`'s `LengthMismatch(n)` reports `len - excess`, i.e. *our* field
/// count, which the user already knows.
fn msgpack_array_len(bytes: &[u8]) -> Option<u32> {
    match *bytes.first()? {
        b @ 0x90..=0x9f => Some(u32::from(b & 0x0f)),
        0xdc => bytes
            .get(1..3)
            .and_then(|b| <[u8; 2]>::try_from(b).ok())
            .map(|b| u32::from(u16::from_be_bytes(b))),
        0xdd => bytes
            .get(1..5)
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map(u32::from_be_bytes),
        _ => None,
    }
}

/// Classify a `SnapshotMeta` decode failure against the raw plaintext.
///
/// Callers must only use this once the AEAD decrypt has succeeded — that is
/// what makes [`SkipReason::IncompatibleEnvelope`] a sound conclusion rather
/// than a guess about possibly-corrupt bytes.
pub fn classify_decode_error(err: &rmp_serde::decode::Error, meta_bytes: &[u8]) -> SkipReason {
    match err {
        rmp_serde::decode::Error::LengthMismatch(_) => SkipReason::IncompatibleEnvelope {
            stored_fields: msgpack_array_len(meta_bytes),
        },
        other => SkipReason::Undecodable(other.to_string()),
    }
}

/// Warn once per process per snapshot ID.
///
/// `refresh_snapshot_cache` runs on every `Repository::open()` and a skipped
/// snapshot is never cached, so without this a single `vykar` run repeats the
/// same warning for each phase it runs (backup, prune, compact, check).
fn warn_skip_once(id_hex: &str, reason: &SkipReason) {
    static WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let warned = WARNED.get_or_init(|| Mutex::new(HashSet::new()));
    let mut warned = warned.lock().unwrap_or_else(|e| e.into_inner());
    if warned.insert(id_hex.to_string()) {
        warn!("snapshot {id_hex}: {reason}. Skipping — this repository listing is incomplete");
    }
}

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
        atomic_write(&path, &packed)?;
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
/// Decrypt/deserialize errors are always skipped — this prevents a single
/// garbage upload from bricking the repo in append-only mode. Each skip is
/// recorded in [`SnapshotRefresh::skipped`] so the caller can tell the user the
/// listing is partial instead of silently returning a truncated list, and is
/// warned once per process rather than once per repository open.
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
) -> Result<SnapshotRefresh> {
    let mut skipped: Vec<SkippedSnapshot> = Vec::new();

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
                let reason = SkipReason::Unavailable("listed but not found".into());
                warn_skip_once(id_hex, &reason);
                skipped.push(SkippedSnapshot {
                    id_hex: id_hex.clone(),
                    reason,
                });
                continue;
            }
            Err(e) => {
                if strict_io {
                    return Err(vykar_types::error::VykarError::Other(format!(
                        "failed to fetch snapshot {id_hex}: {e} (strict I/O mode)"
                    )));
                }
                let reason = SkipReason::Unavailable(e.to_string());
                warn_skip_once(id_hex, &reason);
                skipped.push(SkippedSnapshot {
                    id_hex: id_hex.clone(),
                    reason,
                });
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
                let reason = SkipReason::Undecryptable(e.to_string());
                warn_skip_once(id_hex, &reason);
                skipped.push(SkippedSnapshot {
                    id_hex: id_hex.clone(),
                    reason,
                });
                continue;
            }
        };

        let meta: crate::snapshot::SnapshotMeta = match rmp_serde::from_slice(&meta_bytes) {
            Ok(m) => m,
            Err(e) => {
                // Decryption already succeeded, so the blob is intact and the
                // key is right — a length mismatch here is a foreign envelope,
                // never corruption.
                let reason = classify_decode_error(&e, &meta_bytes);
                warn_skip_once(id_hex, &reason);
                skipped.push(SkippedSnapshot {
                    id_hex: id_hex.clone(),
                    reason,
                });
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

    // Stable order so a caller rendering the list gets deterministic output.
    skipped.sort_by(|a, b| a.id_hex.cmp(&b.id_hex));

    Ok(SnapshotRefresh {
        entries: cache.to_entries(),
        skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChunkerConfig;
    use crate::snapshot::{SnapshotMeta, SNAPSHOT_META_FIELD_COUNT};
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
            ext: None,
            format_version: crate::snapshot::CURRENT_FORMAT_VERSION,
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
        let packed =
            pack_object_with_context(ObjectType::SnapshotMeta, id.as_bytes(), &meta_bytes, crypto)
                .unwrap();
        storage.put(&id.storage_key(), &packed).unwrap();
        id
    }

    /// Store a snapshot whose envelope is one field *longer* than this build's
    /// — what a future vykar that appended a field to `SnapshotMeta` writes.
    ///
    /// It must be longer, not shorter: a shorter array still decodes via
    /// `#[serde(default)]` (see `old_array_decodes_with_defaults`), so a
    /// 12-field fixture would silently exercise nothing.
    fn store_future_snapshot(
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> SnapshotId {
        let id = SnapshotId::generate();
        let meta = make_snapshot_meta("from-the-future");
        let current = rmp_serde::to_vec(&meta).unwrap();

        // Re-encode as a positional array with one extra trailing element.
        let expected_header = 0x90 | u8::try_from(SNAPSHOT_META_FIELD_COUNT).unwrap();
        assert_eq!(
            current.first().copied(),
            Some(expected_header),
            "fixture assumes a {SNAPSHOT_META_FIELD_COUNT}-element fixarray"
        );
        let mut future = current.clone();
        future[0] = 0x90 | u8::try_from(SNAPSHOT_META_FIELD_COUNT + 1).unwrap();
        future.push(0xC0); // nil — the hypothetical new field

        let packed =
            pack_object_with_context(ObjectType::SnapshotMeta, id.as_bytes(), &future, crypto)
                .unwrap();
        storage.put(&id.storage_key(), &packed).unwrap();
        id
    }

    #[test]
    fn future_envelope_is_reported_not_silently_skipped() {
        let crypto = test_crypto();
        let storage = MemoryBackend::new();
        let cache_dir = tempfile::tempdir().unwrap();

        store_snapshot(&storage, &crypto, "readable");
        let future_id = store_future_snapshot(&storage, &crypto);

        let refresh = refresh_snapshot_cache(
            &storage,
            &crypto,
            &[0xC1; 16],
            Some(cache_dir.path()),
            false,
        )
        .expect("a foreign envelope must never be fatal");

        assert_eq!(
            refresh.entries.len(),
            1,
            "the readable snapshot must still list"
        );
        assert_eq!(
            refresh.skipped.len(),
            1,
            "the unreadable snapshot must be reported, not silently dropped"
        );
        assert_eq!(refresh.skipped[0].id_hex, future_id.to_hex());
        assert_eq!(
            refresh.skipped[0].reason,
            SkipReason::IncompatibleEnvelope {
                stored_fields: Some(SNAPSHOT_META_FIELD_COUNT + 1)
            },
            "a longer envelope after a successful decrypt is version skew, not corruption"
        );
    }

    #[test]
    fn incompatible_envelope_message_names_the_remedy() {
        let reason = SkipReason::IncompatibleEnvelope {
            stored_fields: Some(15),
        };
        let msg = reason.to_string();
        assert!(
            msg.contains("15"),
            "should name the stored field count: {msg}"
        );
        assert!(
            msg.contains(&SNAPSHOT_META_FIELD_COUNT.to_string()),
            "should name this build's field count: {msg}"
        );
        assert!(
            msg.contains("upgrade vykar"),
            "should name the remedy: {msg}"
        );

        let summary = describe_skipped(&[SkippedSnapshot {
            id_hex: "ab".into(),
            reason,
        }])
        .expect("a non-empty set must produce a summary");
        assert!(summary.contains("1 snapshot hidden"), "{summary}");
        assert!(summary.contains("upgrade vykar"), "{summary}");
        assert_eq!(describe_skipped(&[]), None, "empty set must stay silent");
    }

    /// The skipped list is rebuilt on every refresh — only the `warn!` is
    /// de-duplicated. A caller that opens the repo twice in one process must
    /// still be told its view is partial the second time.
    #[test]
    fn future_envelope_is_reported_on_every_refresh() {
        let crypto = test_crypto();
        let storage = MemoryBackend::new();
        let cache_dir = tempfile::tempdir().unwrap();

        store_future_snapshot(&storage, &crypto);

        for pass in 1..=2 {
            let refresh = refresh_snapshot_cache(
                &storage,
                &crypto,
                &[0xC2; 16],
                Some(cache_dir.path()),
                false,
            )
            .unwrap();
            assert_eq!(
                refresh.skipped.len(),
                1,
                "pass {pass}: a partial view must be reported every time, \
                 not only on the first open"
            );
        }
    }

    #[test]
    fn msgpack_array_len_reads_all_header_widths() {
        assert_eq!(msgpack_array_len(&[0x90]), Some(0));
        assert_eq!(msgpack_array_len(&[0x9E]), Some(14));
        assert_eq!(msgpack_array_len(&[0x9F]), Some(15));
        assert_eq!(msgpack_array_len(&[0xDC, 0x01, 0x00]), Some(256));
        assert_eq!(msgpack_array_len(&[0xDD, 0, 1, 0, 0]), Some(65536));
        assert_eq!(msgpack_array_len(&[0xC0]), None, "nil is not an array");
        assert_eq!(msgpack_array_len(&[]), None);
        assert_eq!(msgpack_array_len(&[0xDC, 0x01]), None, "truncated header");
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
        let refresh = result.unwrap();
        assert_eq!(refresh.entries.len(), 0, "no snapshots should be loaded");
        assert_eq!(
            refresh.skipped.len(),
            1,
            "the unreachable snapshot must be reported, not silently dropped"
        );
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
        let refresh = result.unwrap();
        assert_eq!(
            refresh.entries.len(),
            0,
            "garbage snapshot should be skipped"
        );
        assert!(
            matches!(
                refresh.skipped.as_slice(),
                [SkippedSnapshot {
                    reason: SkipReason::Undecryptable(_),
                    ..
                }]
            ),
            "a garbage blob is a decrypt failure, not a version skew: {:?}",
            refresh.skipped
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
