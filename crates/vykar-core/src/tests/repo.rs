use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::platform::paths;
use crate::repo::pack::PackType;
use crate::repo::EncryptionMode;
use crate::repo::Repository;
use crate::testutil::{test_repo_plaintext, MemoryBackend, PutLog, RecordingBackend};
use std::path::PathBuf;

#[test]
fn init_creates_required_keys() {
    let storage = Box::new(MemoryBackend::new());
    let repo = Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        None,
    )
    .unwrap();

    // config, manifest, and index should exist
    assert!(repo.storage.exists("config").unwrap());
    assert!(repo.storage.exists("manifest").unwrap());
    assert!(repo.storage.exists("index").unwrap());
}

#[test]
fn init_twice_fails() {
    let storage = Box::new(MemoryBackend::new());
    let repo = Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        None,
    )
    .unwrap();

    // Try to init again with the same storage (clone the Arc into a new Box)
    let result = Repository::init(
        Box::new(repo.storage.clone()),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        None,
    );
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("already exists"), "unexpected error: {err}");
}

#[test]
fn store_and_read_chunk_roundtrip() {
    let mut repo = test_repo_plaintext();
    let data = b"hello, this is chunk data for testing";
    let (chunk_id, _stored_size, is_new) = repo
        .store_chunk(data, Compression::None, PackType::Data)
        .unwrap();
    assert!(is_new);

    // Flush packs so chunks are readable
    repo.flush_packs().unwrap();

    let read_back = repo.read_chunk(&chunk_id).unwrap();
    assert_eq!(read_back, data);
}

#[test]
fn store_chunk_dedup() {
    let mut repo = test_repo_plaintext();
    let data = b"duplicate chunk data";

    let (id1, _size1, is_new1) = repo
        .store_chunk(data, Compression::None, PackType::Data)
        .unwrap();
    assert!(is_new1);

    let (id2, _size2, is_new2) = repo
        .store_chunk(data, Compression::None, PackType::Data)
        .unwrap();
    assert!(!is_new2, "second store should be a dedup hit");
    assert_eq!(id1, id2);

    // Flush packs to commit to index
    repo.flush_packs().unwrap();

    // Refcount should be 2
    let entry = repo.chunk_index().get(&id1).unwrap();
    assert_eq!(entry.refcount, 2);
}

#[test]
fn store_chunk_with_compression() {
    let mut repo = test_repo_plaintext();
    let data = b"compressible data that should survive lz4 round-trip";
    let (chunk_id, _stored_size, is_new) = repo
        .store_chunk(data, Compression::Lz4, PackType::Data)
        .unwrap();
    assert!(is_new);

    // Flush packs so chunks are readable
    repo.flush_packs().unwrap();

    let read_back = repo.read_chunk(&chunk_id).unwrap();
    assert_eq!(read_back, data);
}

#[test]
fn save_state_persists_manifest_and_index() {
    let mut repo = test_repo_plaintext();
    let data = b"persistent chunk";
    repo.store_chunk(data, Compression::None, PackType::Data)
        .unwrap();
    // Mark manifest dirty so it gets written (store_chunk only marks index dirty)
    repo.mark_manifest_dirty();
    repo.save_state().unwrap();

    // Verify manifest and index are updated in storage
    assert!(repo.storage.exists("manifest").unwrap());
    assert!(repo.storage.exists("index").unwrap());

    // Index should have one entry
    assert_eq!(repo.chunk_index().len(), 1);
}

#[test]
fn read_missing_chunk_fails() {
    let mut repo = test_repo_plaintext();
    let fake_id = vykar_types::chunk_id::ChunkId([0xFF; 32]);
    let result = repo.read_chunk(&fake_id);
    assert!(result.is_err());
}

#[test]
fn read_chunk_at_roundtrip() {
    let mut repo = test_repo_plaintext();
    let data = b"chunk data for read_chunk_at test";
    let (chunk_id, _stored_size, _) = repo
        .store_chunk(data, Compression::None, PackType::Data)
        .unwrap();
    repo.flush_packs().unwrap();

    let entry = *repo.chunk_index().get(&chunk_id).unwrap();
    let read_back = repo
        .read_chunk_at(
            &chunk_id,
            &entry.pack_id,
            entry.pack_offset,
            entry.stored_size,
        )
        .unwrap();
    assert_eq!(read_back, data);
}

// ---------------------------------------------------------------------------
// Dirty tracking tests
// ---------------------------------------------------------------------------

fn repo_on_recording_backend() -> (Repository, PutLog) {
    crate::testutil::init_test_environment();
    let (backend, log) = RecordingBackend::new();
    let mut repo = Repository::init(
        Box::new(backend),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        None,
    )
    .expect("failed to init test repo");
    repo.begin_write_session()
        .expect("failed to begin write session");
    (repo, log)
}

#[test]
fn save_state_no_mutations_skips_writes() {
    let (mut repo, log) = repo_on_recording_backend();

    // Clear the put log from init (which writes config, manifest, index)
    log.clear();

    // No mutations — save_state should not write manifest, index, or file cache
    repo.save_state().unwrap();

    let entries = log.entries();
    assert!(
        !entries.contains(&"manifest".to_string()),
        "manifest should not be written when not dirty: {entries:?}"
    );
    assert!(
        !entries.contains(&"index".to_string()),
        "index should not be written when not dirty: {entries:?}"
    );
}

#[test]
fn save_state_writes_only_dirty_components() {
    let (mut repo, log) = repo_on_recording_backend();
    log.clear();

    // Only mark manifest dirty
    repo.mark_manifest_dirty();
    repo.save_state().unwrap();

    let entries = log.entries();
    assert!(
        entries.contains(&"manifest".to_string()),
        "manifest should be written: {entries:?}"
    );
    assert!(
        !entries.contains(&"index".to_string()),
        "index should NOT be written: {entries:?}"
    );
}

#[test]
fn store_chunk_marks_index_dirty() {
    let (mut repo, log) = repo_on_recording_backend();
    log.clear();

    // Store a chunk — this goes through flush_writer_async (normal mode)
    repo.store_chunk(b"chunk data", Compression::None, PackType::Data)
        .unwrap();
    repo.save_state().unwrap();

    let entries = log.entries();
    assert!(
        entries.contains(&"index".to_string()),
        "index should be written after store_chunk: {entries:?}"
    );
}

#[test]
fn dedup_mode_empty_delta_restores_index_without_write() {
    let (mut repo, log) = repo_on_recording_backend();

    // Store some chunks in normal mode
    let data_a = b"chunk data A for dedup test";
    let (id_a, _, _) = repo
        .store_chunk(data_a, Compression::None, PackType::Data)
        .unwrap();
    repo.mark_manifest_dirty();
    repo.mark_index_dirty();
    repo.save_state().unwrap();

    // Verify chunk is in the index
    assert_eq!(repo.chunk_index().len(), 1);
    let entry_before = *repo.chunk_index().get(&id_a).unwrap();

    // Start a new write session and enable dedup mode (drops full index)
    repo.begin_write_session().unwrap();
    repo.enable_dedup_mode();
    assert!(
        repo.chunk_index().is_empty(),
        "chunk_index should be empty in dedup mode"
    );

    log.clear();

    // save_state with no new chunks — empty delta
    repo.save_state().unwrap();

    // chunk_index should be restored from storage
    assert_eq!(
        repo.chunk_index().len(),
        1,
        "chunk_index should be restored"
    );
    let entry_after = *repo.chunk_index().get(&id_a).unwrap();
    assert_eq!(entry_before.refcount, entry_after.refcount);

    // No index write should have occurred (delta was empty)
    let entries = log.entries();
    assert!(
        !entries.contains(&"index".to_string()),
        "index should NOT be rewritten for empty delta: {entries:?}"
    );
}

#[test]
fn dedup_mode_with_delta_writes_index() {
    let (mut repo, log) = repo_on_recording_backend();

    // Store initial chunks
    repo.store_chunk(b"initial chunk data", Compression::None, PackType::Data)
        .unwrap();
    repo.mark_index_dirty();
    repo.save_state().unwrap();
    assert_eq!(repo.chunk_index().len(), 1);

    // Start a new write session and enable dedup mode
    repo.begin_write_session().unwrap();
    repo.enable_dedup_mode();
    log.clear();

    // Store a NEW chunk in dedup mode
    let (new_id, _, is_new) = repo
        .store_chunk(
            b"new chunk in dedup mode",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    assert!(is_new);

    repo.save_state().unwrap();

    // Index should be written because delta had new entries
    let entries = log.entries();
    assert!(
        entries.contains(&"index".to_string()),
        "index should be written for non-empty delta: {entries:?}"
    );

    // chunk_index should contain both old and new entries
    assert_eq!(repo.chunk_index().len(), 2);
    assert!(repo.chunk_index().get(&new_id).is_some());
}

#[test]
fn dirty_flags_reset_after_save() {
    let (mut repo, log) = repo_on_recording_backend();

    // Mark everything dirty and save
    repo.mark_manifest_dirty();
    repo.mark_index_dirty();
    repo.mark_file_cache_dirty();
    repo.save_state().unwrap();

    // Clear log and save again — nothing should be written
    log.clear();
    repo.save_state().unwrap();

    let entries = log.entries();
    assert!(
        !entries.contains(&"manifest".to_string()),
        "manifest should not be rewritten: {entries:?}"
    );
    assert!(
        !entries.contains(&"index".to_string()),
        "index should not be rewritten: {entries:?}"
    );
}

#[test]
fn index_delta_is_empty() {
    use crate::index::IndexDelta;

    let empty = IndexDelta::new();
    assert!(empty.is_empty());

    let mut with_bump = IndexDelta::new();
    with_bump.bump_refcount(&vykar_types::chunk_id::ChunkId([0xAA; 32]));
    assert!(!with_bump.is_empty());
}

/// Compute the file cache path the same way `FileCache::cache_path` does.
fn file_cache_path(repo_id: &[u8]) -> Option<PathBuf> {
    paths::cache_dir().map(|base| {
        base.join("vykar")
            .join(hex::encode(repo_id))
            .join("filecache")
    })
}

#[test]
fn deferred_hydration_survives_file_cache_save_error() {
    let (mut repo, _log) = repo_on_recording_backend();

    // Store a chunk so the index has content
    let (id_a, _, _) = repo
        .store_chunk(b"hydration test chunk", Compression::None, PackType::Data)
        .unwrap();
    repo.mark_manifest_dirty();
    repo.mark_index_dirty();
    repo.save_state().unwrap();
    assert_eq!(repo.chunk_index().len(), 1);

    // Start a new write session and enable dedup mode (drops full index, activates deferred hydration path)
    repo.begin_write_session().unwrap();
    repo.enable_dedup_mode();
    assert!(repo.chunk_index().is_empty());

    // Mark file_cache dirty so save_state() will attempt file_cache.save()
    repo.mark_file_cache_dirty();

    // Block the file cache write by placing a directory where the file should go.
    // std::fs::write() will fail with "Is a directory".
    let cache_path = file_cache_path(&repo.config.id).expect("cache dir available");
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    // Remove existing file if present, then create a directory in its place
    let _ = std::fs::remove_file(&cache_path);
    std::fs::create_dir(&cache_path).unwrap();

    // save_state should fail (file cache write error)...
    let result = repo.save_state();
    assert!(
        result.is_err(),
        "save_state should fail when file cache write is blocked"
    );

    // ...but chunk_index must be hydrated despite the error
    assert_eq!(
        repo.chunk_index().len(),
        1,
        "chunk_index should be hydrated even when file_cache.save() fails"
    );
    assert!(
        repo.chunk_index().get(&id_a).is_some(),
        "chunk_index should contain the original chunk"
    );

    // Clean up: remove the blocking directory
    let _ = std::fs::remove_dir(&cache_path);
}

#[test]
fn init_rejects_oversized_max_pack_size() {
    use crate::config::{RepositoryConfig, RetryConfig};

    crate::testutil::init_test_environment();

    let min_pack = 32 * 1024 * 1024; // 32 MiB

    // Just over the 512 MiB hard limit — should fail.
    let oversized_config = RepositoryConfig {
        url: String::new(),
        region: None,
        access_key_id: None,
        secret_access_key: None,
        sftp_key: None,
        sftp_known_hosts: None,
        sftp_timeout: None,
        access_token: None,
        allow_insecure_http: false,
        min_pack_size: min_pack,
        max_pack_size: 513 * 1024 * 1024,
        retry: RetryConfig::default(),
    };

    let result = Repository::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        Some(&oversized_config),
        None,
    );
    assert!(
        result.is_err(),
        "init should reject max_pack_size > 512 MiB"
    );
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("512 MiB"), "error should mention limit: {err}");

    // Exactly 512 MiB — should succeed.
    let valid_config = RepositoryConfig {
        url: String::new(),
        region: None,
        access_key_id: None,
        secret_access_key: None,
        sftp_key: None,
        sftp_known_hosts: None,
        sftp_timeout: None,
        access_token: None,
        allow_insecure_http: false,
        min_pack_size: min_pack,
        max_pack_size: 512 * 1024 * 1024,
        retry: RetryConfig::default(),
    };

    let result = Repository::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        Some(&valid_config),
        None,
    );
    assert!(
        result.is_ok(),
        "init should accept max_pack_size == 512 MiB"
    );
}

#[test]
fn flush_on_abort_writes_pending_index() {
    use crate::compress;
    use crate::config::{RepositoryConfig, RetryConfig};
    use crate::index::PendingPackEntry;
    use crate::repo::format::{unpack_object_expect_with_context, ObjectType};

    crate::testutil::init_test_environment();

    // Use tiny pack sizes so a single chunk triggers a flush.
    let small_config = RepositoryConfig {
        url: String::new(),
        region: None,
        access_key_id: None,
        secret_access_key: None,
        sftp_key: None,
        sftp_known_hosts: None,
        sftp_timeout: None,
        access_token: None,
        allow_insecure_http: false,
        min_pack_size: 256,
        max_pack_size: 256,
        retry: RetryConfig::default(),
    };
    let mut repo = Repository::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        Some(&small_config),
        None,
    )
    .unwrap();
    repo.begin_write_session().unwrap();

    // Store a chunk large enough to exceed the 256-byte pack target.
    // After encryption envelope overhead, this will trigger flush_writer_async.
    let data = vec![0xABu8; 300];
    repo.store_chunk(&data, Compression::None, PackType::Data)
        .unwrap();

    // The journal debounce interval is 8 packs, so after 1 pack the
    // pending_index should NOT have been written to storage yet.
    assert!(
        !repo.storage.exists("sessions/default.index").unwrap(),
        "sessions/default.index should not exist yet (debounce hasn't triggered)"
    );

    // Store another small chunk that stays in the pack writer buffer.
    let data2 = vec![0xCDu8; 64];
    repo.store_chunk(&data2, Compression::None, PackType::Data)
        .unwrap();

    // Call flush_on_abort — should seal partial packs, join uploads,
    // and write pending_index.
    repo.flush_on_abort();

    // Verify pending_index now exists.
    assert!(
        repo.storage.exists("sessions/default.index").unwrap(),
        "sessions/default.index should exist after flush_on_abort"
    );

    // Decrypt and deserialize to verify contents.
    let raw = repo.storage.get("sessions/default.index").unwrap().unwrap();
    let compressed = unpack_object_expect_with_context(
        &raw,
        ObjectType::PendingIndex,
        b"pending_index",
        repo.crypto.as_ref(),
    )
    .unwrap();
    let serialized = compress::decompress_metadata(&compressed).unwrap();
    let entries: Vec<PendingPackEntry> = rmp_serde::from_slice(&serialized).unwrap();

    // Should have 2 packs: the first (auto-flushed) and the partial (sealed by abort).
    assert_eq!(
        entries.len(),
        2,
        "pending_index should contain 2 pack entries, got {}: {entries:?}",
        entries.len()
    );

    // Each pack should contain exactly 1 chunk.
    for entry in &entries {
        assert_eq!(
            entry.chunks.len(),
            1,
            "each pack should have 1 chunk: {entry:?}"
        );
    }
}

/// Storage backend that fails `put()` for pack keys (packs/*) but succeeds
/// for everything else.  Used to verify flush_on_abort's best-effort behavior.
struct FailPackUploadsBackend {
    inner: MemoryBackend,
}

impl FailPackUploadsBackend {
    fn new() -> Self {
        Self {
            inner: MemoryBackend::new(),
        }
    }
}

impl vykar_storage::StorageBackend for FailPackUploadsBackend {
    fn get(&self, key: &str) -> vykar_types::error::Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn put(&self, key: &str, data: &[u8]) -> vykar_types::error::Result<()> {
        if key.starts_with("packs/") {
            return Err(vykar_types::error::VykarError::Other(
                "simulated pack upload failure".into(),
            ));
        }
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

#[test]
fn flush_on_abort_survives_pack_upload_failure() {
    use crate::config::{RepositoryConfig, RetryConfig};

    crate::testutil::init_test_environment();

    let small_config = RepositoryConfig {
        url: String::new(),
        region: None,
        access_key_id: None,
        secret_access_key: None,
        sftp_key: None,
        sftp_known_hosts: None,
        sftp_timeout: None,
        access_token: None,
        allow_insecure_http: false,
        min_pack_size: 256,
        max_pack_size: 256,
        retry: RetryConfig::default(),
    };
    let mut repo = Repository::init(
        Box::new(FailPackUploadsBackend::new()),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        Some(&small_config),
        None,
    )
    .unwrap();
    repo.begin_write_session().unwrap();

    // Store a chunk large enough to trigger flush_writer_async.
    // The background upload thread will fail (FailPackUploadsBackend rejects packs/).
    let data = vec![0xABu8; 300];
    repo.store_chunk(&data, Compression::None, PackType::Data)
        .unwrap();

    // flush_on_abort should not panic even though upload threads fail.
    // It should still attempt to write pending_index (which targets a non-pack key).
    repo.flush_on_abort();

    // sessions/default.index should exist because put("sessions/default.index", ..) succeeds.
    assert!(
        repo.storage.exists("sessions/default.index").unwrap(),
        "sessions/default.index should be written even when pack uploads fail"
    );
}

#[test]
fn open_rejects_oversized_max_pack_size() {
    crate::testutil::init_test_environment();

    // Init a valid repo first.
    let storage = Box::new(MemoryBackend::new());
    let repo = Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
        None,
    )
    .unwrap();

    // Tamper with the stored config: set max_pack_size > 512 MiB.
    let mut tampered_config = repo.config.clone();
    tampered_config.max_pack_size = 513 * 1024 * 1024;
    let tampered_data = rmp_serde::to_vec(&tampered_config).unwrap();
    repo.storage.put("config", &tampered_data).unwrap();

    // Re-open should fail.
    let result = Repository::open(Box::new(repo.storage.clone()), None, None);
    assert!(
        result.is_err(),
        "open should reject stored max_pack_size > 512 MiB"
    );
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("512 MiB"), "error should mention limit: {err}");
}

// ---------------------------------------------------------------------------
// Dynamic pack target scaling tests
// ---------------------------------------------------------------------------

/// Helper: create a plaintext repo with tiny pack sizes for testing pack flushes.
fn repo_with_small_packs(min_pack: u32, max_pack: u32) -> Repository {
    use crate::config::{RepositoryConfig, RetryConfig};

    crate::testutil::init_test_environment();

    let small_config = RepositoryConfig {
        url: String::new(),
        region: None,
        access_key_id: None,
        secret_access_key: None,
        sftp_key: None,
        sftp_known_hosts: None,
        sftp_timeout: None,
        access_token: None,
        allow_insecure_http: false,
        min_pack_size: min_pack,
        max_pack_size: max_pack,
        retry: RetryConfig::default(),
    };
    let mut repo = Repository::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        Some(&small_config),
        None,
    )
    .unwrap();
    repo.begin_write_session().unwrap();
    repo
}

/// Store `count` unique ~1200-byte chunks into `repo`, each with a distinct
/// 4-byte prefix derived from `start..start+count`.
fn store_unique_chunks(repo: &mut Repository, start: u32, count: u32, pack_type: PackType) {
    for i in start..start + count {
        let mut data = vec![0xABu8; 1200];
        data[..4].copy_from_slice(&i.to_le_bytes());
        repo.store_chunk(&data, Compression::None, pack_type)
            .unwrap();
    }
}

#[test]
fn data_pack_flush_grows_target() {
    let mut repo = repo_with_small_packs(1024, 8192);

    let initial_target = repo.data_pack_target();
    assert_eq!(
        initial_target, 1024,
        "initial target should be min_pack_size"
    );

    // Each chunk is ~1200 bytes which exceeds the 1024 target after envelope overhead.
    // With divisor=50, need >50 packs before target grows above min.
    store_unique_chunks(&mut repo, 0, 60, PackType::Data);

    let final_target = repo.data_pack_target();
    assert!(
        final_target > initial_target,
        "target should grow after flushing packs: initial={initial_target}, final={final_target}"
    );
    assert!(
        final_target <= 8192,
        "target should not exceed max_pack_size: {final_target}"
    );
}

#[test]
fn tree_pack_flush_does_not_change_data_target() {
    let mut repo = repo_with_small_packs(1024, 8192);

    let target_before = repo.data_pack_target();

    // Store a tree chunk large enough to trigger a tree pack flush.
    let tree_data = vec![0xCDu8; 1200];
    repo.store_chunk(&tree_data, Compression::None, PackType::Tree)
        .unwrap();
    repo.flush_packs().unwrap();

    let target_after = repo.data_pack_target();
    assert_eq!(
        target_before, target_after,
        "data pack target should not change after tree pack flush"
    );
}

#[test]
fn load_chunk_index_resets_session_counter() {
    let mut repo = repo_with_small_packs(1024, 8192);

    // With divisor=50, need >50 packs before target grows above min.
    store_unique_chunks(&mut repo, 0, 60, PackType::Data);
    assert!(repo.data_pack_target() > 1024);

    // Save state to persist the index with current packs.
    repo.mark_manifest_dirty();
    repo.save_state().unwrap();

    // Reload the index — session counter should reset to 0, so the target
    // should be based only on persisted packs.
    repo.load_chunk_index().unwrap();
    repo.begin_write_session().unwrap();
    let target_after_reload = repo.data_pack_target();

    // Compute the expected target from the reloaded index's pack count.
    let expected = crate::repo::pack::compute_data_pack_target(
        repo.chunk_index().count_distinct_packs(),
        1024,
        8192,
    );
    assert_eq!(
        target_after_reload, expected,
        "after reload, target should equal compute_data_pack_target(persisted_packs)"
    );
}

#[test]
fn save_state_rebases_pack_counters() {
    let mut repo = repo_with_small_packs(1024, 8192);

    // First session: flush several packs.
    store_unique_chunks(&mut repo, 0, 20, PackType::Data);
    let target_before_save = repo.data_pack_target();
    repo.mark_manifest_dirty();
    repo.save_state().unwrap();

    // Second session on the same Repository instance: flush more packs.
    // Without rebasing, the session counter would double-count the first
    // session's packs, inflating the target.
    repo.begin_write_session().unwrap();
    store_unique_chunks(&mut repo, 100, 20, PackType::Data);
    let target_second_session = repo.data_pack_target();

    assert!(
        target_second_session >= target_before_save,
        "target should not decrease: before_save={target_before_save}, second={target_second_session}"
    );
    assert!(
        target_second_session <= 8192,
        "target should not exceed max: {target_second_session}"
    );

    // Verify via reload: the target from a fresh index load should match
    // what the reused instance computed.
    repo.mark_manifest_dirty();
    repo.save_state().unwrap();
    repo.load_chunk_index().unwrap();
    repo.begin_write_session().unwrap();
    let target_after_reload = repo.data_pack_target();

    // The reload path uses count_distinct_packs() which includes tree packs,
    // matching begin_write_session(). Both paths see the same (slightly inflated)
    // count, so the targets should be close.
    let diff = (target_second_session as i64 - target_after_reload as i64).unsigned_abs();
    assert!(
        diff <= target_after_reload as u64 / 4,
        "reused instance target ({target_second_session}) diverged too far from \
         reload target ({target_after_reload})"
    );
}

// ---------------------------------------------------------------------------
// Cross-session pending index recovery
// ---------------------------------------------------------------------------

#[test]
fn cross_session_pending_index_recovery() {
    use crate::config::{RepositoryConfig, RetryConfig};

    crate::testutil::init_test_environment();

    // Use tiny pack sizes so chunks trigger pack flushes.
    let small_config = RepositoryConfig {
        url: String::new(),
        region: None,
        access_key_id: None,
        secret_access_key: None,
        sftp_key: None,
        sftp_known_hosts: None,
        sftp_timeout: None,
        access_token: None,
        allow_insecure_http: false,
        min_pack_size: 256,
        max_pack_size: 256,
        retry: RetryConfig::default(),
    };

    // Session 1: init repo, store chunks, flush, write pending index, then drop.
    let mut repo = Repository::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        Some(&small_config),
        None,
    )
    .unwrap();
    repo.begin_write_session().unwrap();
    repo.set_write_session_id("aaa".to_string());

    // Store several chunks large enough to trigger pack flushes.
    let mut chunk_ids = Vec::new();
    for i in 0u32..3 {
        let mut data = vec![0xABu8; 300];
        data[..4].copy_from_slice(&i.to_le_bytes());
        let (chunk_id, _, _) = repo
            .store_chunk(&data, Compression::None, PackType::Data)
            .unwrap();
        chunk_ids.push(chunk_id);
    }

    // Simulate crash: flush packs and write pending index, but don't commit.
    repo.flush_on_abort();

    // Verify the journal exists under the old session ID.
    assert!(
        repo.storage.exists("sessions/aaa.index").unwrap(),
        "sessions/aaa.index should exist after flush_on_abort"
    );

    // Save the storage Arc for reuse.
    let shared_storage = repo.storage.clone();
    drop(repo);

    // Session 2: reopen the same repo with a different session ID.
    let mut repo2 = Repository::open(Box::new(shared_storage), None, None).unwrap();
    repo2.begin_write_session().unwrap();
    repo2.set_write_session_id("bbb".to_string());

    let recovered = repo2.recover_pending_index().unwrap();
    assert!(
        recovered > 0,
        "should recover chunks from session aaa's journal, got {recovered}"
    );

    // Verify that the recovered chunk count matches what we stored.
    assert_eq!(
        recovered,
        chunk_ids.len(),
        "recovered count should match stored chunk count"
    );
}

/// Verify that tree packs in the persisted index are included in the initial
/// data pack target calculation. This is a known limitation: the persisted
/// `ChunkIndex` does not distinguish data packs from tree packs, so
/// `count_distinct_packs()` counts both. The effect is negligible because
/// tree packs are a small fraction of total packs (~1-2 per backup) and the
/// sqrt scaling dampens the inflation further.
#[test]
fn initial_session_seed_includes_tree_packs() {
    let mut repo = repo_with_small_packs(1024, 8192);

    // Flush several data packs.
    store_unique_chunks(&mut repo, 0, 10, PackType::Data);

    // Flush a tree pack.
    let tree_data = vec![0xCDu8; 1200];
    repo.store_chunk(&tree_data, Compression::None, PackType::Tree)
        .unwrap();

    // Persist everything and consume the session.
    repo.mark_manifest_dirty();
    repo.save_state().unwrap();

    // Count all distinct packs in the index (data + tree).
    let total_packs = repo.chunk_index().count_distinct_packs();
    assert!(
        total_packs > 10,
        "should have data packs + at least 1 tree pack"
    );

    // Start a fresh session — initial seed uses count_distinct_packs().
    repo.begin_write_session().unwrap();
    let target = repo.data_pack_target();

    // The target should match compute_data_pack_target(total_packs, ...) —
    // i.e. it includes tree packs in the count. This documents the current
    // behavior rather than asserting the ideal (data-only) behavior.
    let expected = crate::repo::pack::compute_data_pack_target(total_packs, 1024, 8192);
    assert_eq!(
        target, expected,
        "initial session target should be based on all packs (data + tree)"
    );
}
