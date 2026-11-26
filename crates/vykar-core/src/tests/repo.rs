use std::path::PathBuf;

use crate::compress::Compression;
use crate::repo::ChunkerParams;
use crate::repo::EncryptionMode;
use crate::repo::pack::PackType;
use crate::repo::{Repo, WriteSession, WriteSessionOptions};
use crate::testutil::{MemoryBackend, PutLog, RecordingBackend, test_repo, test_repo_with_session};
use vykar_common::paths;

#[test]
fn init_creates_required_keys() {
    let storage = Box::new(MemoryBackend::new());
    let repo = Repo::init(
        storage,
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap();

    // config and the first index head exist; no `index` or `index.gen` (FMT-1).
    assert!(repo.storage().exists("config").unwrap());
    assert!(repo.storage().exists("index.h/1").unwrap());
    assert!(!repo.storage().exists("index").unwrap());
    assert!(!repo.storage().exists("index.gen").unwrap());
    assert_eq!(repo.index_head().unwrap().generation, 1);
}

#[test]
fn init_twice_fails() {
    let storage = Box::new(MemoryBackend::new());
    let repo = Repo::init(
        storage,
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap();

    // Try to init again with the same storage (clone the Arc into a new Box)
    let result = Repo::init(
        Box::new(repo.storage().clone()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    );
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("already exists"), "unexpected error: {err}");
}

#[test]
fn store_and_read_chunk_roundtrip() {
    let (mut repo, mut ws) = test_repo_with_session();
    let data = b"hello, this is chunk data for testing";
    let (chunk_id, _stored_size, is_new) = ws
        .store_chunk(&repo, data, Compression::None, PackType::Data)
        .unwrap();
    assert!(is_new);

    // A session's chunks are readable through the index once it is saved
    repo.unchecked_writer().save_session(ws).unwrap();

    let read_back = repo.read_chunk(&chunk_id).unwrap();
    assert_eq!(read_back, data);
}

#[test]
fn store_chunk_dedup() {
    let (mut repo, mut ws) = test_repo_with_session();
    let data = b"duplicate chunk data";

    let (id1, _size1, is_new1) = ws
        .store_chunk(&repo, data, Compression::None, PackType::Data)
        .unwrap();
    assert!(is_new1);

    let (id2, _size2, is_new2) = ws
        .store_chunk(&repo, data, Compression::None, PackType::Data)
        .unwrap();
    assert!(!is_new2, "second store should be a dedup hit");
    assert_eq!(id1, id2);

    // A session's chunks are in the index once it is saved.
    repo.unchecked_writer().save_session(ws).unwrap();

    // The chunk is in the index once.
    assert!(repo.chunk_index().contains(&id1));
    assert_eq!(repo.chunk_index().len(), 1);
}

#[test]
fn store_chunk_with_compression() {
    let (mut repo, mut ws) = test_repo_with_session();
    let data = b"compressible data that should survive lz4 round-trip";
    let (chunk_id, _stored_size, is_new) = ws
        .store_chunk(&repo, data, Compression::Lz4, PackType::Data)
        .unwrap();
    assert!(is_new);

    // A session's chunks are readable through the index once it is saved
    repo.unchecked_writer().save_session(ws).unwrap();

    let read_back = repo.read_chunk(&chunk_id).unwrap();
    assert_eq!(read_back, data);
}

#[test]
fn save_state_persists_index() {
    let (mut repo, mut ws) = test_repo_with_session();
    let data = b"persistent chunk";
    ws.store_chunk(&repo, data, Compression::None, PackType::Data)
        .unwrap();
    // Mark index dirty so it gets written (store_chunk only marks index dirty)
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().save_session(ws).unwrap();

    // Verify a new head is in storage
    assert!(repo.storage().exists("index.h/2").unwrap());

    // Index should have one entry
    assert_eq!(repo.chunk_index().len(), 1);
}

#[test]
fn read_missing_chunk_fails() {
    let repo = test_repo();
    let fake_id = vykar_types::chunk_id::ChunkId::from_bytes([0xFF; 32]);
    let result = repo.read_chunk(&fake_id);
    assert!(
        matches!(result, Err(vykar_types::error::VykarError::ChunkNotInIndex(id)) if id == fake_id)
    );
}

/// Without a loaded index every chunk is "not in the index", even a stored
/// one; its explicit location still reads.
#[test]
fn read_chunk_without_loaded_index_is_chunk_not_in_index() {
    let (mut repo, mut ws) = test_repo_with_session();
    let data = b"chunk read through a handle without the index";
    let (id, _, _) = ws
        .store_chunk(&repo, data, Compression::None, PackType::Data)
        .unwrap();
    ws.flush_packs(&repo).unwrap();
    repo.unchecked_writer().save_session(ws).unwrap();
    let entry = repo.chunk_index().get(&id).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let bare = Repo::open(
        Box::new(std::sync::Arc::clone(repo.storage())),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(dir.path().to_path_buf()),
        crate::repo::OpenOptions::new(),
    )
    .unwrap();

    let err = bare.read_chunk(&id).unwrap_err();
    assert!(
        matches!(err, vykar_types::error::VykarError::ChunkNotInIndex(got) if got == id),
        "got: {err}"
    );
    let read_back = bare
        .read_chunk_at(&id, &entry.pack_id, entry.pack_offset, entry.stored_size)
        .unwrap();
    assert_eq!(read_back, data);
}

#[test]
fn read_chunk_at_roundtrip() {
    let (mut repo, mut ws) = test_repo_with_session();
    let data = b"chunk data for read_chunk_at test";
    let (chunk_id, _stored_size, _) = ws
        .store_chunk(&repo, data, Compression::None, PackType::Data)
        .unwrap();
    repo.unchecked_writer().save_session(ws).unwrap();

    let entry = repo.chunk_index().get(&chunk_id).unwrap();
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

fn repo_on_recording_backend() -> (Repo, WriteSession, PutLog) {
    crate::testutil::init_test_environment();
    let (backend, log) = RecordingBackend::new();
    let repo = Repo::init(
        Box::new(backend),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .expect("failed to init test repo");
    let ws = repo.begin_write_session(&WriteSessionOptions::default());
    (repo, ws, log)
}

#[test]
fn save_state_no_mutations_skips_writes() {
    let (mut repo, ws, log) = repo_on_recording_backend();

    // Clear the put log from init (which writes config, keys and the head)
    log.clear();

    // No mutations — save_state should not write index or file cache
    repo.unchecked_writer().save_session(ws).unwrap();

    let entries = log.entries();
    assert!(
        !entries.iter().any(|k| k.starts_with("index.h/")),
        "index should not be written when not dirty: {entries:?}"
    );
}

#[test]
fn save_state_writes_only_dirty_components() {
    let (mut repo, ws, log) = repo_on_recording_backend();
    log.clear();

    // Edit the index (an unchanged index publishes nothing, dirty or not)
    repo.unchecked_writer().chunk_index_mut().add(
        vykar_types::chunk_id::ChunkId::from_bytes([7; 32]),
        10,
        vykar_types::pack_id::PackId::from_bytes([8; 32]),
        0,
    );
    repo.unchecked_writer()
        .chunk_index_mut()
        .set_pack_trailer(vykar_types::pack_id::PackId::from_bytes([8; 32]), 0);
    repo.unchecked_writer().save_session(ws).unwrap();

    let entries = log.entries();
    assert!(
        entries.iter().any(|k| k.starts_with("index.h/")),
        "index should be written: {entries:?}"
    );
}

#[test]
fn store_chunk_marks_index_dirty() {
    let (mut repo, mut ws, log) = repo_on_recording_backend();
    log.clear();

    // Store a chunk — this goes through flush_writer_async (normal mode)
    ws.store_chunk(&repo, b"chunk data", Compression::None, PackType::Data)
        .unwrap();
    repo.unchecked_writer().save_session(ws).unwrap();

    let entries = log.entries();
    assert!(
        entries.iter().any(|k| k.starts_with("index.h/")),
        "index should be written after store_chunk: {entries:?}"
    );
}

#[test]
fn empty_session_writes_no_index() {
    let (mut repo, mut ws, log) = repo_on_recording_backend();

    // Store some chunks in normal mode
    let data_a = b"chunk data A for dedup test";
    let (id_a, _, _) = ws
        .store_chunk(&repo, data_a, Compression::None, PackType::Data)
        .unwrap();
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().save_session(ws).unwrap();

    // Verify chunk is in the index
    assert_eq!(repo.chunk_index().len(), 1);
    let entry_before = repo.chunk_index().get(&id_a).unwrap();

    // A new write session keeps the loaded index.
    let ws = repo.begin_write_session(&WriteSessionOptions::default());
    assert_eq!(repo.chunk_index().len(), 1);

    log.clear();

    // save_state with no new chunks — empty delta
    repo.unchecked_writer().save_session(ws).unwrap();

    assert_eq!(repo.chunk_index().len(), 1);
    let entry_after = repo.chunk_index().get(&id_a).unwrap();
    assert_eq!(entry_before, entry_after);

    // No index write should have occurred (delta was empty)
    let entries = log.entries();
    assert!(
        !entries.iter().any(|k| k.starts_with("index.h/")),
        "index should NOT be rewritten for empty delta: {entries:?}"
    );
}

#[test]
fn session_with_new_chunks_writes_a_segment() {
    let (mut repo, mut ws, log) = repo_on_recording_backend();

    // Store initial chunks
    ws.store_chunk(
        &repo,
        b"initial chunk data",
        Compression::None,
        PackType::Data,
    )
    .unwrap();
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().save_session(ws).unwrap();
    assert_eq!(repo.chunk_index().len(), 1);

    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    log.clear();

    // Store a new chunk in the session
    let (new_id, _, is_new) = ws
        .store_chunk(
            &repo,
            b"new chunk in a session",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    assert!(is_new);

    repo.unchecked_writer().save_session(ws).unwrap();

    // The head and a segment are written because the delta had new entries
    let entries = log.entries();
    assert!(
        entries.iter().any(|k| k.starts_with("index.h/")),
        "index should be written for non-empty delta: {entries:?}"
    );
    assert!(
        entries.iter().any(|k| k.starts_with("index.d/")),
        "a segment should be written: {entries:?}"
    );

    // chunk_index should contain both old and new entries
    assert_eq!(repo.chunk_index().len(), 2);
    assert!(repo.chunk_index().get(&new_id).is_some());
}

#[test]
fn dirty_flags_reset_after_save() {
    let (mut repo, ws, log) = repo_on_recording_backend();

    // Mark everything dirty and save
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().mark_index_dirty();
    repo.mark_file_cache_dirty();
    repo.unchecked_writer().save_session(ws).unwrap();

    // Clear log and save again — nothing should be written
    log.clear();
    repo.unchecked_writer().save_state().unwrap();

    let entries = log.entries();
    assert!(
        !entries.iter().any(|k| k.starts_with("index.h/")),
        "index should not be rewritten: {entries:?}"
    );
}

#[test]
fn index_delta_is_empty() {
    use crate::index::IndexDelta;

    let empty = IndexDelta::new();
    assert!(empty.is_empty());

    let mut with_reuse = IndexDelta::new();
    with_reuse
        .reused
        .insert_key(&vykar_types::chunk_id::ChunkId::from_bytes([0xAA; 32]));
    assert!(!with_reuse.is_empty());
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
fn file_cache_save_error_leaves_the_index_in_place() {
    let (mut repo, mut ws, _log) = repo_on_recording_backend();

    // Store a chunk so the index has content
    let (id_a, _, _) = ws
        .store_chunk(
            &repo,
            b"hydration test chunk",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().save_session(ws).unwrap();
    assert_eq!(repo.chunk_index().len(), 1);

    let ws = repo.begin_write_session(&WriteSessionOptions::default());

    // Mark file_cache dirty so save_state() will attempt file_cache.save()
    repo.mark_file_cache_dirty();

    // Block the file cache write by placing a directory where the file should go.
    // std::fs::write() will fail with "Is a directory".
    let cache_path = file_cache_path(&repo.config().id).expect("cache dir available");
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    // Remove existing file if present, then create a directory in its place
    let _ = std::fs::remove_file(&cache_path);
    std::fs::create_dir(&cache_path).unwrap();

    // save_state should fail (file cache write error)...
    let result = repo.unchecked_writer().save_session(ws);
    assert!(
        result.is_err(),
        "save_state should fail when file cache write is blocked"
    );

    // ...and the index is still the loaded one
    assert_eq!(
        repo.chunk_index().len(),
        1,
        "chunk_index should be intact when file_cache.save() fails"
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
    use crate::repo::InitOptions;

    crate::testutil::init_test_environment();

    let min_pack = 32 * 1024 * 1024; // 32 MiB

    // Just over the 512 MiB hard limit — should fail.
    let oversized_config = InitOptions {
        min_pack_size: min_pack,
        max_pack_size: 513 * 1024 * 1024,
        ..Default::default()
    };

    let result = Repo::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
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
    let valid_config = InitOptions {
        min_pack_size: min_pack,
        max_pack_size: 512 * 1024 * 1024,
        ..Default::default()
    };

    let result = Repo::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
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
    use crate::index::PendingPackEntry;
    use crate::repo::InitOptions;
    use crate::repo::format::{ObjectType, unpack_object_expect_with_context};

    crate::testutil::init_test_environment();

    // Use tiny pack sizes so a single chunk triggers a flush.
    let small_config = InitOptions {
        min_pack_size: 256,
        max_pack_size: 256,
        ..Default::default()
    };
    let repo = Repo::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(&small_config),
        None,
    )
    .unwrap();
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());

    // Store a chunk large enough to exceed the 256-byte pack target.
    // After encryption envelope overhead, this will trigger flush_writer_async.
    let data = vec![0xABu8; 300];
    ws.store_chunk(&repo, &data, Compression::None, PackType::Data)
        .unwrap();

    // The journal debounce interval is 8 packs, so after 1 pack the
    // pending_index should NOT have been written to storage yet.
    assert!(
        !repo.storage().exists("sessions/default.index").unwrap(),
        "sessions/default.index should not exist yet (debounce hasn't triggered)"
    );

    // Store another small chunk that stays in the pack writer buffer.
    let data2 = vec![0xCDu8; 64];
    ws.store_chunk(&repo, &data2, Compression::None, PackType::Data)
        .unwrap();

    // Call flush_on_abort — should seal partial packs, join uploads,
    // and write pending_index.
    ws.flush_on_abort(&repo);

    // Verify pending_index now exists.
    assert!(
        repo.storage().exists("sessions/default.index").unwrap(),
        "sessions/default.index should exist after flush_on_abort"
    );

    // Decrypt and deserialize to verify contents.
    let raw = repo
        .storage()
        .get("sessions/default.index")
        .unwrap()
        .unwrap();
    let compressed = unpack_object_expect_with_context(
        &raw,
        ObjectType::PendingIndex,
        b"pending_index",
        repo.crypto().as_ref(),
    )
    .unwrap();
    let serialized = compress::decompress_metadata(&compressed).unwrap();
    let entries: Vec<PendingPackEntry> =
        match crate::index::decode_pending_journal(&serialized).unwrap() {
            crate::index::DecodedJournal::V2(entries) => entries,
            other => panic!("expected a v2 journal, got {other:?}"),
        };

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

    fn profile(&self) -> vykar_storage::BackendProfile {
        self.inner.profile()
    }

    vykar_storage::unsupported_server_ops!();
}

#[test]
fn flush_on_abort_survives_pack_upload_failure() {
    use crate::repo::InitOptions;

    crate::testutil::init_test_environment();

    let small_config = InitOptions {
        min_pack_size: 256,
        max_pack_size: 256,
        ..Default::default()
    };
    let repo = Repo::init(
        Box::new(FailPackUploadsBackend::new()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(&small_config),
        None,
    )
    .unwrap();
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());

    // Store a chunk large enough to trigger flush_writer_async.
    // The background upload thread will fail (FailPackUploadsBackend rejects packs/).
    let data = vec![0xABu8; 300];
    ws.store_chunk(&repo, &data, Compression::None, PackType::Data)
        .unwrap();

    // flush_on_abort should not panic even though upload threads fail.
    ws.flush_on_abort(&repo);

    // A pack is journaled only after its upload is confirmed. Every upload
    // failed, so the journal is empty and nothing is written: a journal
    // naming a failed pack would let the next backup adopt a damaged pack.
    assert!(
        !repo.storage().exists("sessions/default.index").unwrap(),
        "failed pack uploads must not be journaled"
    );
}

/// A pre-BLAKE3 vykar-server advertises no `hashes` in `/health`. `init` must
/// refuse *before writing anything*, rather than succeeding and leaving a
/// repository whose first `backup` fails: `init` writes only non-pack keys,
/// every one of which an old server accepts.
#[test]
fn init_refuses_a_v3_repository_against_a_pre_blake3_server() {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    crate::testutil::init_test_environment();

    // A pre-BLAKE3 /health body: no `protocol_version`, no `hashes`.
    let body = r#"{"status":"ok","version":"0.19.1"}"#;
    // `Connection: close` keeps ureq from pooling a socket this mock is about
    // to drop; reusing it would race the FIN and fail on Windows (10054).
    let health = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let not_found =
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{port}");

    // Serve /health, then record whether anything else was ever requested.
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let handle = std::thread::spawn(move || {
        listener.set_nonblocking(false).expect("blocking listener");
        while let Ok((mut stream, _)) = listener.accept() {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap_or(0);
            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line.trim().is_empty() {
                    break;
                }
                if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = v.trim().parse().unwrap_or(0);
                }
            }
            // Drain the body: closing with unread bytes sends RST, which on
            // Windows discards the reply before the client reads it.
            let mut body = vec![0u8; content_length];
            let _ = std::io::Read::read_exact(&mut reader, &mut body);
            let line = request_line.trim().to_string();
            let reply = if line.contains("/health") {
                &health
            } else {
                // Everything else 404s, so the repository reads as absent.
                &not_found
            };
            if tx.send(line).is_err() {
                break;
            }
            let _ = stream.write_all(reply.as_bytes());
            let _ = stream.flush();
        }
    });

    let storage = vykar_storage::backend_from_config(&vykar_storage::StorageConfig {
        url: url.clone(),
        region: None,
        access_key_id: None,
        secret_access_key: None,
        sftp_key: None,
        sftp_agent: None,
        sftp_known_hosts: None,
        max_connections: None,
        sftp_timeout: None,
        access_token: None,
        allow_insecure_http: true,
        retry: vykar_storage::RetryConfig {
            max_retries: 0,
            ..Default::default()
        },
        s3_soft_delete: false,
        s3_conditional_writes: Default::default(),
        cancel: vykar_types::cancel::CancelToken::new(),
    })
    .expect("REST backend builds");

    let err = Repo::init(
        storage,
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(&crate::repo::InitOptions {
            url: Some(url.clone()),
            ..Default::default()
        }),
        None,
    )
    .err()
    .expect("init must refuse a pre-BLAKE3 server");

    let message = err.to_string();
    assert!(
        message.contains("does not support") && message.contains("0.19.1"),
        "the error must name the server and its version, got: {message}"
    );
    assert!(
        message.contains("Upgrade the server"),
        "the error must say what to do, got: {message}"
    );

    // Nothing beyond the existence check and the probe may have been sent.
    let mut writes = Vec::new();
    while let Ok(line) = rx.recv_timeout(std::time::Duration::from_millis(300)) {
        if line.starts_with("PUT") || line.starts_with("POST") {
            writes.push(line);
        }
    }
    assert!(
        writes.is_empty(),
        "init must write nothing when the probe fails, but sent: {writes:?}"
    );
    drop(handle);
}

#[test]
fn init_creates_the_current_repository_format() {
    crate::testutil::init_test_environment();

    let repo = Repo::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap();

    assert_eq!(repo.config().version, 6);
    assert_eq!(repo.format(), crate::repo::RepoFormat::CURRENT);
    assert_eq!(
        repo.content_hash(),
        vykar_types::hash::HashAlgorithm::Blake3
    );
    assert_eq!(
        repo.crypto().chunk_hasher().algorithm(),
        vykar_types::hash::HashAlgorithm::Blake3
    );
}

/// A repository written by a future vykar must be refused at the version
/// gate, not decoded with known semantics.
#[test]
fn open_rejects_an_unknown_repository_version() {
    crate::testutil::init_test_environment();

    let repo = Repo::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap();

    let mut tampered_config = repo.config().clone();
    tampered_config.version = 8;
    let tampered_data = rmp_serde::to_vec_named(&tampered_config).unwrap();
    repo.storage().put("config", &tampered_data).unwrap();

    let result = Repo::open(
        Box::new(repo.storage().clone()),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        crate::repo::OpenOptions::new(),
    );
    match result {
        Err(vykar_types::error::VykarError::UnsupportedVersion(v)) => assert_eq!(v, 8),
        other => panic!(
            "expected UnsupportedVersion(8), got {:?}",
            other.map(|_| ())
        ),
    }
}

#[test]
fn open_rejects_oversized_max_pack_size() {
    crate::testutil::init_test_environment();

    // Init a valid repo first.
    let storage = Box::new(MemoryBackend::new());
    let repo = Repo::init(
        storage,
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap();

    // Tamper with the stored config: set max_pack_size > 512 MiB.
    let mut tampered_config = repo.config().clone();
    tampered_config.max_pack_size = 513 * 1024 * 1024;
    let tampered_data = tampered_config.encode().unwrap();
    repo.storage().put("config", &tampered_data).unwrap();

    // Re-open should fail.
    let result = Repo::open(
        Box::new(repo.storage().clone()),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        crate::repo::OpenOptions::new().with_index(),
    );
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
fn repo_with_small_packs(min_pack: u32, max_pack: u32) -> (Repo, WriteSession) {
    use crate::repo::InitOptions;

    crate::testutil::init_test_environment();

    let small_config = InitOptions {
        min_pack_size: min_pack,
        max_pack_size: max_pack,
        ..Default::default()
    };
    let repo = Repo::init(
        Box::new(MemoryBackend::new()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(&small_config),
        None,
    )
    .unwrap();
    let ws = repo.begin_write_session(&WriteSessionOptions::default());
    (repo, ws)
}

/// Store `count` unique ~1200-byte chunks into `repo`, each with a distinct
/// 4-byte prefix derived from `start..start+count`.
fn store_unique_chunks(
    repo: &Repo,
    ws: &mut WriteSession,
    start: u32,
    count: u32,
    pack_type: PackType,
) {
    for i in start..start + count {
        let mut data = vec![0xABu8; 1200];
        data[..4].copy_from_slice(&i.to_le_bytes());
        ws.store_chunk(repo, &data, Compression::None, pack_type)
            .unwrap();
    }
}

#[test]
fn data_pack_flush_grows_target() {
    let (repo, mut ws) = repo_with_small_packs(1024, 8192);

    let initial_target = ws.data_pack_target();
    assert_eq!(
        initial_target, 1024,
        "initial target should be min_pack_size"
    );

    // Each chunk is ~1200 bytes which exceeds the 1024 target after envelope overhead.
    // With divisor=50, need >50 packs before target grows above min.
    store_unique_chunks(&repo, &mut ws, 0, 60, PackType::Data);

    let final_target = ws.data_pack_target();
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
    let (repo, mut ws) = repo_with_small_packs(1024, 8192);

    let target_before = ws.data_pack_target();

    // Store a tree chunk large enough to trigger a tree pack flush.
    let tree_data = vec![0xCDu8; 1200];
    ws.store_chunk(&repo, &tree_data, Compression::None, PackType::Tree)
        .unwrap();
    ws.flush_packs(&repo).unwrap();

    let target_after = ws.data_pack_target();
    assert_eq!(
        target_before, target_after,
        "data pack target should not change after tree pack flush"
    );
}

#[test]
fn load_chunk_index_resets_session_counter() {
    let (mut repo, mut ws) = repo_with_small_packs(1024, 8192);

    // With divisor=50, need >50 packs before target grows above min.
    store_unique_chunks(&repo, &mut ws, 0, 60, PackType::Data);
    assert!(ws.data_pack_target() > 1024);

    // Save state to persist the index with current packs.
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().save_session(ws).unwrap();

    // Reload the index — session counter should reset to 0, so the target
    // should be based only on persisted packs.
    repo.load_chunk_index().unwrap();
    let ws = repo.begin_write_session(&WriteSessionOptions::default());
    let target_after_reload = ws.data_pack_target();

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
    let (mut repo, mut ws) = repo_with_small_packs(1024, 8192);

    // First session: flush several packs.
    store_unique_chunks(&repo, &mut ws, 0, 20, PackType::Data);
    let target_before_save = ws.data_pack_target();
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().save_session(ws).unwrap();

    // Second session on the same Repository instance: flush more packs.
    // Without rebasing, the session counter would double-count the first
    // session's packs, inflating the target.
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    store_unique_chunks(&repo, &mut ws, 100, 20, PackType::Data);
    let target_second_session = ws.data_pack_target();

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
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().save_session(ws).unwrap();
    repo.load_chunk_index().unwrap();
    ws = repo.begin_write_session(&WriteSessionOptions::default());
    let target_after_reload = ws.data_pack_target();

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
    use crate::repo::InitOptions;

    crate::testutil::init_test_environment();

    // Use tiny pack sizes so chunks trigger pack flushes.
    let small_config = InitOptions {
        min_pack_size: 256,
        max_pack_size: 256,
        ..Default::default()
    };

    // Session 1: init repo, store chunks, flush, write pending index, then drop.
    // A store with mtimes: recovery adopts journal packs by their age.
    let repo = Repo::init(
        Box::new(MemoryBackend::with_mtimes()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(&small_config),
        None,
    )
    .unwrap();
    let mut ws = repo.begin_write_session(&WriteSessionOptions {
        session_id: "aaa".into(),
        ..Default::default()
    });

    // Store several chunks large enough to trigger pack flushes.
    let mut chunk_ids = Vec::new();
    for i in 0u32..3 {
        let mut data = vec![0xABu8; 300];
        data[..4].copy_from_slice(&i.to_le_bytes());
        let (chunk_id, _, _) = ws
            .store_chunk(&repo, &data, Compression::None, PackType::Data)
            .unwrap();
        chunk_ids.push(chunk_id);
    }

    // Simulate crash: flush packs and write pending index, but don't commit.
    ws.flush_on_abort(&repo);

    // Verify the journal exists under the old session ID.
    assert!(
        repo.storage().exists("sessions/aaa.index").unwrap(),
        "sessions/aaa.index should exist after flush_on_abort"
    );

    // Save the storage Arc for reuse.
    let shared_storage = repo.storage().clone();
    drop(repo);

    // Session 2: reopen the same repo with a different session ID.
    let repo2 = Repo::open(
        Box::new(shared_storage),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        crate::repo::OpenOptions::new().with_index(),
    )
    .unwrap();
    let mut ws2 = repo2.begin_write_session(&WriteSessionOptions {
        session_id: "bbb".into(),
        ..Default::default()
    });
    // The recovering session's own marker: its mtime is "now" on the
    // store's clock, which the journal packs' ages are judged against.
    repo2.storage().put("sessions/bbb.json", b"{}").unwrap();

    let recovery = ws2
        .recover_pending_index(&repo2, &crate::repo::control::CancelToken::new())
        .unwrap();
    assert_eq!(recovery.packs_too_old, 0);
    assert!(recovery.oldest_pack_age.is_some());
    assert!(
        recovery.recovered_chunks > 0,
        "should recover chunks from session aaa's journal, got {}",
        recovery.recovered_chunks
    );

    // Verify that the recovered chunk count matches what we stored.
    assert_eq!(
        recovery.recovered_chunks,
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
    let (mut repo, mut ws) = repo_with_small_packs(1024, 8192);

    // Flush several data packs.
    store_unique_chunks(&repo, &mut ws, 0, 10, PackType::Data);

    // Flush a tree pack.
    let tree_data = vec![0xCDu8; 1200];
    ws.store_chunk(&repo, &tree_data, Compression::None, PackType::Tree)
        .unwrap();

    // Persist everything and consume the session.
    repo.unchecked_writer().mark_index_dirty();
    repo.unchecked_writer().save_session(ws).unwrap();

    // Count all distinct packs in the index (data + tree).
    let total_packs = repo.chunk_index().count_distinct_packs();
    assert!(
        total_packs > 10,
        "should have data packs + at least 1 tree pack"
    );

    // Start a fresh session — initial seed uses count_distinct_packs().
    let ws = repo.begin_write_session(&WriteSessionOptions::default());
    let target = ws.data_pack_target();

    // The target should match compute_data_pack_target(total_packs, ...) —
    // i.e. it includes tree packs in the count. This documents the current
    // behavior rather than asserting the ideal (data-only) behavior.
    let expected = crate::repo::pack::compute_data_pack_target(total_packs, 1024, 8192);
    assert_eq!(
        target, expected,
        "initial session target should be based on all packs (data + tree)"
    );
}

// ---------------------------------------------------------------------------
// The repository's table as the index (05 step 3)
// ---------------------------------------------------------------------------

/// Writing an index that was never loaded over a head with entries would
/// diff an empty index against that state; it is refused instead.
#[test]
fn persist_refuses_an_unloaded_index_over_a_non_empty_head() {
    let (mut repo, mut ws) = test_repo_with_session();
    ws.store_chunk(&repo, b"one chunk", Compression::None, PackType::Data)
        .unwrap();
    repo.unchecked_writer().save_session(ws).unwrap();
    assert!(!repo.index_head().unwrap().is_empty());

    repo.clear_chunk_index();
    repo.unchecked_writer().mark_index_dirty();
    let err = repo
        .unchecked_writer()
        .save_state()
        .unwrap_err()
        .to_string();
    assert!(err.contains("not loaded before it was written"), "{err}");
}

/// Without a cache dir, and after a load that ignores local tables, the
/// repository's own table serves every later sync: a write, a fold, a load
/// under the lock and a backup commit read no index object (R2).
#[test]
fn writes_reuse_the_live_table_without_a_cache_dir() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    crate::testutil::init_test_environment();
    let mem: Arc<dyn vykar_storage::StorageBackend> = Arc::new(MemoryBackend::new());
    let mut repo = Repo::init(
        Box::new(Arc::clone(&mem)),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap();
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    ws.store_chunk(&repo, b"first chunk", Compression::None, PackType::Data)
        .unwrap();
    repo.unchecked_writer().save_session(ws).unwrap();
    drop(repo);

    let armed = Arc::new(AtomicBool::new(false));
    let fail = Arc::clone(&armed);
    let storage = crate::tests::helpers::FailGet::new(
        Arc::clone(&mem),
        move |k| fail.load(Ordering::SeqCst) && k.starts_with("index.d/"),
        usize::MAX,
    );
    let mut repo = Repo::open(
        Box::new(storage),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        crate::repo::OpenOptions::new(),
    )
    .unwrap();
    repo.load_chunk_index_uncached().unwrap();
    armed.store(true, Ordering::SeqCst);

    // A write: segment from the live table.
    ws = repo.begin_write_session(&WriteSessionOptions::default());
    ws.store_chunk(&repo, b"second chunk", Compression::None, PackType::Data)
        .unwrap();
    repo.unchecked_writer().save_session(ws).unwrap();
    assert_eq!(repo.chunk_index().len(), 2);

    // A fold of the same state.
    let mut policy = crate::repo::index_write::FoldPolicy::default();
    policy.soft.segments = 1;
    repo.set_fold_policy(policy);
    repo.unchecked_writer()
        .maintain_segmented_index(crate::repo::index_write::OnMoved::Fail)
        .unwrap();
    assert!(repo.index_head().unwrap().segments.is_empty(), "folded");

    // A load under the lock, at the head the fold wrote.
    repo.load_chunk_index_under_lock().unwrap();
    assert_eq!(repo.chunk_index().len(), 2);

    // A backup commit merges against the live table.
    ws = repo.begin_write_session(&WriteSessionOptions::default());
    ws.store_chunk(&repo, b"third chunk", Compression::None, PackType::Data)
        .unwrap();
    let id = vykar_types::snapshot_id::SnapshotId::generate();
    let entry = crate::repo::manifest::SnapshotEntry {
        name: "s1".into(),
        id,
        time: chrono::Utc::now(),
        source_label: String::new(),
        label: String::new(),
        source_paths: Vec::new(),
        hostname: String::new(),
    };
    let mut file_cache = crate::repo::file_cache::FileCache::new();
    repo.commit_session_blocking(
        ws,
        entry,
        b"snapshot".to_vec(),
        &mut file_cache,
        &|_: crate::repo::control::CommitEvent| {},
    )
    .unwrap();
    assert_eq!(repo.chunk_index().len(), 3);
    assert_eq!(
        *repo.chunk_index().state(),
        crate::index::table::IndexState::of_head(repo.index_head().unwrap())
    );
}

/// Answers the capability probe with a server that serves the segmented
/// index but not the v6 key directories.
struct ServerWithoutV6Dirs {
    inner: MemoryBackend,
}

impl vykar_storage::InnerBackend for ServerWithoutV6Dirs {
    fn inner_backend(&self) -> &dyn vykar_storage::StorageBackend {
        &self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for ServerWithoutV6Dirs;
    except [server_ops];

    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        Some(self)
    }
}

impl vykar_storage::ServerOps for ServerWithoutV6Dirs {
    fn offers(&self, _op: vykar_storage::ServerOp) -> bool {
        false
    }

    fn capabilities(&self) -> vykar_types::error::Result<vykar_protocol::ServerCapabilities> {
        Ok(vykar_protocol::ServerCapabilities {
            status: "ok".into(),
            version: "0.24.0".into(),
            protocol_version: vykar_protocol::PROTOCOL_VERSION,
            hashes: vec!["blake3".into()],
            segmented_index: true,
            key_dirs: vec!["index.d".into()],
            ..Default::default()
        })
    }

    fn init(&self) -> vykar_types::error::Result<()> {
        Err(vykar_types::error::VykarError::Other(
            "init must not be reached".into(),
        ))
    }
}

/// FMT-3: init refuses a vykar-server that does not accept the `index.h/`
/// and `packmaps/` keys, before writing anything.
#[test]
fn init_refuses_a_server_without_the_v6_key_dirs() {
    crate::testutil::init_test_environment();
    let storage: std::sync::Arc<dyn vykar_storage::StorageBackend> =
        std::sync::Arc::new(ServerWithoutV6Dirs {
            inner: MemoryBackend::new(),
        });
    let err = Repo::init(
        Box::new(std::sync::Arc::clone(&storage)),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .err()
    .expect("init must refuse");
    let msg = err.to_string();
    assert!(
        matches!(err, vykar_types::error::VykarError::Config(_)),
        "{err:?}"
    );
    assert!(
        msg.contains("index.h/") && msg.contains("Upgrade the server first"),
        "{msg}"
    );
    assert!(storage.list("").unwrap().is_empty(), "nothing written");
}
