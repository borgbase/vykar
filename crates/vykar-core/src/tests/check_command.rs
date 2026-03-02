use std::collections::HashMap;
use std::sync::Arc;

use crate::commands;
use crate::commands::check::{
    process_verify_response, try_server_verify, verify_pack_full, CheckError, ServerVerifyOutcome,
};
use crate::index::ChunkIndexEntry;
use crate::repo::Repository;
use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{
    StorageBackend, VerifyPackResult, VerifyPacksPlanRequest, VerifyPacksResponse,
};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::helpers::{backup_single_source, init_repo, init_test_environment};

/// Mock storage backend that returns a transient error from server_verify_packs.
struct TransientFailBackend;

impl StorageBackend for TransientFailBackend {
    fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
        Ok(())
    }
    fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }
    fn exists(&self, _key: &str) -> Result<bool> {
        Ok(false)
    }
    fn list(&self, _prefix: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    fn get_range(&self, _key: &str, _offset: u64, _length: u64) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    fn create_dir(&self, _key: &str) -> Result<()> {
        Ok(())
    }
    fn server_verify_packs(&self, _plan: &VerifyPacksPlanRequest) -> Result<VerifyPacksResponse> {
        Err(VykarError::Other("transient failure".into()))
    }
}

fn open_local_repo(repo_dir: &std::path::Path) -> Repository {
    init_test_environment();
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    Repository::open(storage, None, None).unwrap()
}

#[test]
fn check_verify_data_flag_controls_data_verification_counters() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"check-verify-data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-check-verify");

    let without_verify = commands::check::run(&config, None, false, false).unwrap();
    assert!(without_verify.errors.is_empty());
    assert_eq!(without_verify.chunks_data_verified, 0);
    assert!(without_verify.chunks_existence_checked > 0);
    assert!(without_verify.packs_existence_checked > 0);

    let with_verify = commands::check::run(&config, None, true, false).unwrap();
    assert!(with_verify.errors.is_empty());
    assert!(with_verify.chunks_data_verified > 0);
    assert!(with_verify.chunks_existence_checked > 0);
    assert!(with_verify.packs_existence_checked > 0);
}

#[test]
fn check_reports_missing_pack_file_in_storage() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"check-missing-pack").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-check-pack");

    let repo = open_local_repo(&repo_dir);
    let (_chunk_id, entry) = repo.chunk_index().iter().next().unwrap();
    let pack_key = entry.pack_id.storage_key();
    let pack_path = repo_dir.join(&pack_key);
    assert!(pack_path.exists());
    std::fs::remove_file(pack_path).unwrap();

    let result = commands::check::run(&config, None, false, false).unwrap();
    assert!(result.chunks_existence_checked > 0);
    assert!(result.packs_existence_checked > 0);
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.message.contains("missing from storage")),
        "expected missing pack error, got: {:?}",
        result
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn check_reports_snapshot_metadata_load_failures() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"check-missing-meta").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-check-meta");

    let repo = open_local_repo(&repo_dir);
    let entry = repo.manifest().find_snapshot("snap-check-meta").unwrap();
    let snapshot_path = repo_dir.join("snapshots").join(entry.id.to_hex());
    assert!(snapshot_path.exists());
    std::fs::remove_file(snapshot_path).unwrap();

    let result = commands::check::run(&config, None, false, false).unwrap();
    assert_eq!(result.snapshots_checked, 0);
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.message.contains("failed to load metadata")),
        "expected metadata load error, got: {:?}",
        result
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn check_with_progress_emits_phase_events() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"check-progress").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-check-progress");

    let mut events = Vec::new();
    let mut on_progress = |event| events.push(event);

    let result =
        commands::check::run_with_progress(&config, None, false, false, Some(&mut on_progress))
            .unwrap();
    assert!(result.errors.is_empty());
    assert!(!events.is_empty());
    assert!(events.iter().any(|e| matches!(
        e,
        commands::check::CheckProgressEvent::SnapshotStarted { .. }
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        commands::check::CheckProgressEvent::PacksExistencePhaseStarted { .. }
    )));
}

#[test]
fn check_deduplicates_pack_existence_checks() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    // Write multiple small files — chunks will land in the same pack
    for i in 0..5 {
        std::fs::write(
            source_dir.join(format!("file{i}.txt")),
            format!("dedup-check-test-content-{i}"),
        )
        .unwrap();
    }

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-dedup", "snap-dedup");

    let result = commands::check::run(&config, None, false, false).unwrap();
    assert!(result.errors.is_empty());
    assert!(result.chunks_existence_checked > 0);
    assert!(result.packs_existence_checked > 0);
    // Multiple chunks should share packs, so packs checked < chunks checked
    assert!(
        result.packs_existence_checked < result.chunks_existence_checked,
        "expected fewer packs ({}) than chunks ({})",
        result.packs_existence_checked,
        result.chunks_existence_checked,
    );
}

// ---------------------------------------------------------------------------
// Unit tests for process_verify_response, try_server_verify, verify_pack_full
// ---------------------------------------------------------------------------

fn make_pack_result(
    pack_key: &str,
    hash_valid: bool,
    header_valid: bool,
    blobs_valid: bool,
    error: Option<&str>,
) -> VerifyPackResult {
    VerifyPackResult {
        pack_key: pack_key.to_string(),
        hash_valid,
        header_valid,
        blobs_valid,
        error: error.map(|s| s.to_string()),
    }
}

#[test]
fn test_process_verify_response_missing_packs() {
    let resp = VerifyPacksResponse {
        truncated: false,
        results: vec![
            make_pack_result("packs/aa/pack1", true, true, true, None),
            make_pack_result("packs/bb/pack2", true, true, true, None),
        ],
    };
    let requested = vec![
        ("packs/aa/pack1", 5),
        ("packs/bb/pack2", 3),
        ("packs/cc/pack3", 7),
    ];
    let mut errors: Vec<CheckError> = Vec::new();
    let result = process_verify_response(&resp, &requested, &mut errors);

    assert_eq!(result.packs_responded, 2);
    assert_eq!(errors.len(), 1, "expected exactly 1 error for missing pack");
    assert!(
        errors[0].message.contains("pack3"),
        "error should mention the missing pack"
    );
    assert!(errors[0]
        .message
        .contains("not included in server response"));
}

#[test]
fn test_process_verify_response_unexpected_keys() {
    let resp = VerifyPacksResponse {
        truncated: false,
        results: vec![
            make_pack_result("packs/aa/pack1", true, true, true, None),
            make_pack_result("packs/xx/surprise", true, true, true, None),
        ],
    };
    let requested = vec![("packs/aa/pack1", 4)];
    let mut errors: Vec<CheckError> = Vec::new();
    let result = process_verify_response(&resp, &requested, &mut errors);

    // No CheckError for the unexpected key (only tracing::warn)
    assert!(
        errors.is_empty(),
        "unexpected key should not produce a CheckError"
    );
    // packs_responded excludes unexpected keys
    assert_eq!(result.packs_responded, 1);
}

#[test]
fn test_process_verify_response_mixed_pass_fail() {
    let resp = VerifyPacksResponse {
        truncated: false,
        results: vec![
            make_pack_result("packs/aa/pack1", true, true, true, None),
            make_pack_result("packs/bb/pack2", false, true, true, None),
            make_pack_result("packs/cc/pack3", true, true, true, Some("I/O error")),
        ],
    };
    let requested = vec![
        ("packs/aa/pack1", 10),
        ("packs/bb/pack2", 5),
        ("packs/cc/pack3", 3),
    ];
    let mut errors: Vec<CheckError> = Vec::new();
    let result = process_verify_response(&resp, &requested, &mut errors);

    assert_eq!(result.packs_passed, 1);
    assert_eq!(
        result.chunks_verified, 10,
        "only the passing pack's chunks count"
    );
    assert_eq!(result.packs_responded, 3);
    assert_eq!(errors.len(), 2, "two packs failed");
}

#[test]
fn test_process_verify_response_duplicate_keys() {
    // Server returns the same pack_key twice — should only count it once
    let resp = VerifyPacksResponse {
        truncated: false,
        results: vec![
            make_pack_result("packs/aa/pack1", true, true, true, None),
            make_pack_result("packs/aa/pack1", true, true, true, None), // duplicate
        ],
    };
    let requested = vec![("packs/aa/pack1", 5)];
    let mut errors: Vec<CheckError> = Vec::new();
    let result = process_verify_response(&resp, &requested, &mut errors);

    assert_eq!(
        result.packs_responded, 1,
        "duplicate should not inflate count"
    );
    assert_eq!(result.packs_passed, 1);
    assert_eq!(result.chunks_verified, 5, "chunks counted only once");
    assert!(errors.is_empty());
}

#[test]
fn test_server_fallback_no_stale_errors() {
    let storage: Arc<dyn StorageBackend> = Arc::new(TransientFailBackend);
    let pack_id = PackId([0u8; 32]);
    let chunk_id = ChunkId([1u8; 32]);
    let entry = ChunkIndexEntry {
        refcount: 1,
        stored_size: 100,
        pack_id,
        pack_offset: 9,
    };
    let mut pack_chunks: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> = HashMap::new();
    pack_chunks.insert(pack_id, vec![(chunk_id, entry)]);

    let mut progress: Option<&mut dyn FnMut(commands::check::CheckProgressEvent)> = None;

    let outcome = try_server_verify(&storage, &pack_chunks, true, &mut progress);

    match outcome {
        ServerVerifyOutcome::Fallback => {} // expected
        ServerVerifyOutcome::Ok { .. } => panic!("expected Fallback, got Ok"),
    }
    // The transient error should NOT appear in the caller's error list.
    // (It's logged via tracing::warn, not propagated as a CheckError.)
}

#[test]
fn test_verify_pack_full_overflow_offset() {
    init_test_environment();
    let storage =
        LocalBackend::new(&tempfile::tempdir().unwrap().path().to_string_lossy()).unwrap();
    let crypto = vykar_crypto::PlaintextEngine::new(&[0u8; 32]);
    let chunk_id_key = [0u8; 32];
    let pack_id = PackId([0u8; 32]);

    // Create a minimal valid pack file so we get past the header check.
    let pack_data = b"VGERPACK\x01".to_vec(); // 8-byte magic + 1-byte version = 9 bytes
    let pack_key = pack_id.storage_key();
    storage.put(&pack_key, &pack_data).unwrap();

    let chunk_id = ChunkId([1u8; 32]);
    let entry = ChunkIndexEntry {
        refcount: 1,
        stored_size: 10,
        pack_id,
        pack_offset: u64::MAX,
    };

    let mut errors = Vec::new();
    let count = verify_pack_full(
        &storage,
        &crypto,
        &chunk_id_key,
        &pack_id,
        &[(chunk_id, entry)],
        &mut errors,
    );

    assert_eq!(count, 0);
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].message.contains("exceeds addressable range")
            || errors[0].message.contains("blob range overflows"),
        "expected overflow error, got: {}",
        errors[0].message
    );
}

#[test]
fn test_verify_pack_full_overflow_add() {
    init_test_environment();
    let storage =
        LocalBackend::new(&tempfile::tempdir().unwrap().path().to_string_lossy()).unwrap();
    let crypto = vykar_crypto::PlaintextEngine::new(&[0u8; 32]);
    let chunk_id_key = [0u8; 32];
    let pack_id = PackId([0u8; 32]);

    // Create a minimal valid pack file
    let pack_data = b"VGERPACK\x01".to_vec();
    let pack_key = pack_id.storage_key();
    storage.put(&pack_key, &pack_data).unwrap();

    let chunk_id = ChunkId([2u8; 32]);
    // offset + stored_size overflows usize on 64-bit: usize::MAX - 5 + 10 overflows
    let entry = ChunkIndexEntry {
        refcount: 1,
        stored_size: 10,
        pack_id,
        pack_offset: (usize::MAX - 5) as u64,
    };

    let mut errors = Vec::new();
    let count = verify_pack_full(
        &storage,
        &crypto,
        &chunk_id_key,
        &pack_id,
        &[(chunk_id, entry)],
        &mut errors,
    );

    assert_eq!(count, 0);
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].message.contains("blob range overflows"),
        "expected overflow error, got: {}",
        errors[0].message
    );
}
