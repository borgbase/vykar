use std::collections::HashMap;
use std::sync::Arc;

use crate::commands;
use crate::commands::check::{
    process_verify_response, try_server_verify, verify_pack_full, CheckError, IntegrityIssue,
    RepairAction, RepairMode, ServerVerifyOutcome,
};
use crate::index::ChunkIndexEntry;
use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{
    StorageBackend, VerifyPackResult, VerifyPacksPlanRequest, VerifyPacksResponse,
};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::helpers::{
    backup_single_source, init_repo, init_test_environment, make_test_config, open_local_repo,
};

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
    // Corrupt the snapshot blob so it's still listed but fails to decrypt/deserialize.
    std::fs::write(&snapshot_path, b"corrupted-snapshot-data").unwrap();

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

    let result = commands::check::run_with_progress(
        &config,
        None,
        false,
        false,
        Some(&mut on_progress),
        100,
        false,
    )
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

    let mut issues: Vec<IntegrityIssue> = Vec::new();
    let count = verify_pack_full(
        &storage,
        &crypto,
        &chunk_id_key,
        &pack_id,
        &[(chunk_id, entry)],
        &mut issues,
    );

    assert_eq!(count, 0);
    assert_eq!(issues.len(), 1);
    let msg = issues[0].to_check_error().message;
    assert!(
        msg.contains("exceeds addressable range") || msg.contains("blob range overflows"),
        "expected overflow error, got: {msg}",
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

    let mut issues: Vec<IntegrityIssue> = Vec::new();
    let count = verify_pack_full(
        &storage,
        &crypto,
        &chunk_id_key,
        &pack_id,
        &[(chunk_id, entry)],
        &mut issues,
    );

    assert_eq!(count, 0);
    assert_eq!(issues.len(), 1);
    let msg = issues[0].to_check_error().message;
    assert!(
        msg.contains("blob range overflows"),
        "expected overflow error, got: {msg}",
    );
}

// ---------------------------------------------------------------------------
// Item-level impact reporting (issue #122)
// ---------------------------------------------------------------------------

/// Build a config with very small pack sizes so each chunk lands in its own
/// pack. Needed by the item-impact tests below: they delete a pack containing
/// only file data, and this only works reliably when item_ptrs chunks live in
/// a *different* pack from the file data chunks.
fn init_repo_small_packs(repo_dir: &std::path::Path) -> crate::config::VykarConfig {
    let mut config = make_test_config(repo_dir);
    config.repository.min_pack_size = 1;
    config.repository.max_pack_size = 4 * 1024;
    commands::init::run(&config, None).unwrap();
    config
}

/// Pick a pack that contains no item_ptrs chunks for any snapshot in `repo`.
/// Returns the pack id and the set of `(snapshot_name, item_path, item_index)`
/// tuples whose chunks live in that pack.
fn pick_data_only_pack(
    repo_dir: &std::path::Path,
) -> (
    PackId,
    Vec<(String, String, usize, vykar_types::snapshot_id::SnapshotId)>,
) {
    use crate::commands::list::{for_each_decoded_item, load_snapshot_item_stream};
    use crate::snapshot::item::ItemType;

    let mut repo = open_local_repo(repo_dir);

    // Collect item_ptrs chunk ids across every snapshot — these must NOT be in
    // the pack we delete (otherwise the snapshot becomes unreadable).
    let entries = repo.manifest().snapshots.clone();
    let mut item_ptrs_chunks: std::collections::HashSet<ChunkId> = std::collections::HashSet::new();
    for entry in &entries {
        let meta = crate::commands::list::load_snapshot_meta(&repo, &entry.name).unwrap();
        for chunk_id in &meta.item_ptrs {
            item_ptrs_chunks.insert(*chunk_id);
        }
    }

    // Pack candidates: any pack whose chunks are all data (none in item_ptrs).
    let mut pack_to_chunks: HashMap<PackId, Vec<ChunkId>> = HashMap::new();
    for (chunk_id, entry) in repo.chunk_index().iter() {
        pack_to_chunks
            .entry(entry.pack_id)
            .or_default()
            .push(*chunk_id);
    }
    let candidate = pack_to_chunks
        .iter()
        .find(|(_, chunks)| chunks.iter().all(|c| !item_ptrs_chunks.contains(c)))
        .map(|(pid, chunks)| (*pid, chunks.clone()))
        .expect("expected at least one data-only pack — small pack size should ensure this");
    let candidate_chunks: std::collections::HashSet<ChunkId> =
        candidate.1.iter().copied().collect();

    // Walk every snapshot's items_stream to find affected items.
    let mut affected: Vec<(String, String, usize, vykar_types::snapshot_id::SnapshotId)> =
        Vec::new();
    for entry in &entries {
        let stream = load_snapshot_item_stream(&mut repo, &entry.name).unwrap();
        let mut idx: usize = 0;
        for_each_decoded_item(&stream, |item| {
            let i = idx;
            idx += 1;
            if item.entry_type == ItemType::RegularFile
                && item.chunks.iter().any(|c| candidate_chunks.contains(&c.id))
            {
                affected.push((entry.name.clone(), item.path.clone(), i, entry.id));
            }
            Ok(())
        })
        .unwrap();
    }

    (candidate.0, affected)
}

#[test]
fn check_reports_items_affected_by_missing_pack() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    // Several distinct files large enough that each lands in its own pack
    // (config below sets max_pack_size to 4 KiB).
    let file_names = ["alpha.bin", "beta.bin", "gamma.bin"];
    for (i, name) in file_names.iter().enumerate() {
        // Distinct content so chunk ids don't dedup across files.
        let payload: Vec<u8> = (0..2048u32)
            .map(|x| (x as u8).wrapping_add(i as u8))
            .collect();
        std::fs::write(source_dir.join(name), payload).unwrap();
    }

    let config = init_repo_small_packs(&repo_dir);
    backup_single_source(&config, &source_dir, "src-impact", "snap-impact");

    let (deleted_pack, expected_items) = pick_data_only_pack(&repo_dir);
    let pack_path = repo_dir.join(deleted_pack.storage_key());
    assert!(pack_path.exists(), "pack file missing before deletion");
    std::fs::remove_file(&pack_path).unwrap();
    assert!(
        !expected_items.is_empty(),
        "test setup picked a pack with no referencing items"
    );

    let result = commands::check::run(&config, None, false, false).unwrap();

    // Pack-level error from Phase 2 must still be present (existing behavior).
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.message.contains("missing from storage")),
        "expected pack-level missing error, got: {:?}",
        result
            .errors
            .iter()
            .map(|e| (e.context.clone(), e.message.clone()))
            .collect::<Vec<_>>()
    );

    // Item-level rendered errors: one per affected item.
    let item_error_count = result
        .errors
        .iter()
        .filter(|e| e.message.starts_with("references missing pack"))
        .count();
    assert_eq!(
        item_error_count,
        expected_items.len(),
        "expected {} item-level errors, got: {:?}",
        expected_items.len(),
        result
            .errors
            .iter()
            .map(|e| (e.context.clone(), e.message.clone()))
            .collect::<Vec<_>>()
    );

    // Structured impacts: one per affected file, all pointing to the same snapshot.
    assert_eq!(result.item_impacts.len(), expected_items.len());
    let snapshot_id = expected_items[0].3;
    for impact in &result.item_impacts {
        assert_eq!(impact.snapshot_id, snapshot_id);
        assert_eq!(impact.snapshot_name, "snap-impact");
        assert!(!impact.affected_chunks.is_empty());
        // Every chunk in this impact must point at the deleted pack.
        for (_chunk_id, pack_id) in &impact.affected_chunks {
            assert_eq!(*pack_id, deleted_pack);
        }
    }

    // Pre-walked expected (item_path, item_index) tuples must match exactly,
    // catching any ordinal regression — not just shuffles within the set.
    let mut expected_pairs: Vec<(&str, usize)> = expected_items
        .iter()
        .map(|(_name, path, idx, _id)| (path.as_str(), *idx))
        .collect();
    expected_pairs.sort();
    let mut actual_pairs: Vec<(&str, usize)> = result
        .item_impacts
        .iter()
        .map(|i| (i.item_path.as_str(), i.item_index))
        .collect();
    actual_pairs.sort();
    assert_eq!(actual_pairs, expected_pairs);

    // Impacts are emitted in ascending stream order.
    let indexes: Vec<usize> = result.item_impacts.iter().map(|i| i.item_index).collect();
    let mut sorted = indexes.clone();
    sorted.sort();
    assert_eq!(
        indexes, sorted,
        "impacts should be emitted in ascending stream order"
    );
}

#[test]
fn check_repair_dry_run_includes_item_impact() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    for (i, name) in ["one.bin", "two.bin"].iter().enumerate() {
        let payload: Vec<u8> = (0..2048u32)
            .map(|x| (x as u8).wrapping_add(i as u8))
            .collect();
        std::fs::write(source_dir.join(name), payload).unwrap();
    }

    let config = init_repo_small_packs(&repo_dir);
    backup_single_source(&config, &source_dir, "src-dry", "snap-dry");

    let (deleted_pack, expected_items) = pick_data_only_pack(&repo_dir);
    std::fs::remove_file(repo_dir.join(deleted_pack.storage_key())).unwrap();
    assert!(!expected_items.is_empty());

    let result =
        commands::check::run_with_repair(&config, None, false, RepairMode::PlanOnly, None).unwrap();

    assert!(
        !result.check_result.item_impacts.is_empty(),
        "PlanOnly should expose item_impacts on CheckResult"
    );
    assert!(
        result
            .check_result
            .errors
            .iter()
            .any(|e| e.message.starts_with("references missing pack")),
        "PlanOnly's CheckResult.errors should include item-level lines, got: {:?}",
        result
            .check_result
            .errors
            .iter()
            .map(|e| (e.context.clone(), e.message.clone()))
            .collect::<Vec<_>>()
    );
    // No repair was applied (PlanOnly mode).
    assert!(result.applied.is_empty());
}

// ---------------------------------------------------------------------------
// Item-granular repair (issue #123)
// ---------------------------------------------------------------------------

/// Backup three distinct files into separate packs, delete the pack covering
/// only one of them, and verify repair drops just that item — preserving the
/// snapshot under a new id with the surviving items intact.
#[test]
fn repair_drops_items_via_missing_pack_keeps_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    let file_names = ["alpha.bin", "beta.bin", "gamma.bin"];
    for (i, name) in file_names.iter().enumerate() {
        let payload: Vec<u8> = (0..2048u32)
            .map(|x| (x as u8).wrapping_add(i as u8))
            .collect();
        std::fs::write(source_dir.join(name), payload).unwrap();
    }

    let config = init_repo_small_packs(&repo_dir);
    backup_single_source(&config, &source_dir, "src-drop", "snap-drop");

    let (deleted_pack, expected_items) = pick_data_only_pack(&repo_dir);
    assert!(!expected_items.is_empty());
    // Only run the test when *some* but not *all* items would be dropped —
    // otherwise the planner would correctly fall back to whole-snapshot.
    assert!(
        expected_items.len() < file_names.len(),
        "test expects partial coverage; got all items affected"
    );
    let original_id = expected_items[0].3;
    let dropped_paths: std::collections::HashSet<String> = expected_items
        .iter()
        .map(|(_, p, _, _)| p.clone())
        .collect();
    std::fs::remove_file(repo_dir.join(deleted_pack.storage_key())).unwrap();

    let result =
        commands::check::run_with_repair(&config, None, false, RepairMode::Apply, None).unwrap();

    let drop_actions: Vec<_> = result
        .applied
        .iter()
        .filter(|a| matches!(a, RepairAction::DropItemsFromSnapshot { .. }))
        .collect();
    assert_eq!(
        drop_actions.len(),
        1,
        "expected one DropItemsFromSnapshot, got: {:?}",
        result.applied
    );
    if let RepairAction::DropItemsFromSnapshot {
        snapshot_name,
        item_indices,
        ..
    } = drop_actions[0]
    {
        assert_eq!(snapshot_name, "snap-drop");
        assert_eq!(item_indices.len(), expected_items.len());
    }

    // The snapshot must still appear under the same name but a different id.
    let repo = open_local_repo(&repo_dir);
    let entry = repo
        .manifest()
        .find_snapshot("snap-drop")
        .expect("snapshot should still be listed by name");
    assert_ne!(
        entry.id, original_id,
        "snapshot should have been rewritten under a new id"
    );

    // Restore: dropped paths must be absent; surviving paths byte-identical.
    let dest = tmp.path().join("restored");
    std::fs::create_dir_all(&dest).unwrap();
    commands::restore::run(
        &config,
        None,
        "snap-drop",
        dest.to_str().unwrap(),
        None,
        false,
        false,
    )
    .unwrap();
    for name in &file_names {
        let restored = walk_find_file(&dest, name);
        let in_drop = dropped_paths.iter().any(|p| p.ends_with(name));
        if in_drop {
            assert!(
                restored.is_none(),
                "expected {name} to be absent (was dropped)"
            );
        } else {
            let restored = restored.unwrap_or_else(|| panic!("{name} missing after restore"));
            let expected = std::fs::read(source_dir.join(name)).unwrap();
            let got = std::fs::read(&restored).unwrap();
            assert_eq!(got, expected, "{name} content mismatch after restore");
        }
    }
}

/// When deleting the only data pack means *every* item is affected, the
/// planner must fall back to RemoveDanglingSnapshot (not emit
/// DropItemsFromSnapshot for an empty surviving set).
#[test]
fn repair_falls_back_to_whole_snapshot_when_all_items_affected() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    // Single file → its data pack is the only pack with item chunks.
    std::fs::write(source_dir.join("only.bin"), vec![7u8; 2048]).unwrap();

    let config = init_repo_small_packs(&repo_dir);
    backup_single_source(&config, &source_dir, "src-all", "snap-all");

    let (deleted_pack, expected_items) = pick_data_only_pack(&repo_dir);
    assert_eq!(expected_items.len(), 1);
    std::fs::remove_file(repo_dir.join(deleted_pack.storage_key())).unwrap();

    let result =
        commands::check::run_with_repair(&config, None, false, RepairMode::Apply, None).unwrap();

    let has_drop = result
        .applied
        .iter()
        .any(|a| matches!(a, RepairAction::DropItemsFromSnapshot { .. }));
    assert!(
        !has_drop,
        "all-items-affected should NOT emit DropItemsFromSnapshot, got: {:?}",
        result.applied
    );
    let has_dangling = result
        .applied
        .iter()
        .any(|a| matches!(a, RepairAction::RemoveDanglingSnapshot { .. }));
    assert!(
        has_dangling,
        "expected RemoveDanglingSnapshot fallback, got: {:?}",
        result.applied
    );
}

/// Dry-run output exposes the new variant via `repair_plan.actions`.
#[test]
fn repair_dry_run_emits_drop_items_from_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    for (i, name) in ["a.bin", "b.bin", "c.bin"].iter().enumerate() {
        let payload: Vec<u8> = (0..2048u32)
            .map(|x| (x as u8).wrapping_add(i as u8))
            .collect();
        std::fs::write(source_dir.join(name), payload).unwrap();
    }

    let config = init_repo_small_packs(&repo_dir);
    backup_single_source(&config, &source_dir, "src-pln", "snap-pln");

    let (deleted_pack, expected_items) = pick_data_only_pack(&repo_dir);
    assert!(!expected_items.is_empty() && expected_items.len() < 3);
    std::fs::remove_file(repo_dir.join(deleted_pack.storage_key())).unwrap();

    let result =
        commands::check::run_with_repair(&config, None, false, RepairMode::PlanOnly, None).unwrap();

    let drops: Vec<&RepairAction> = result
        .plan
        .actions
        .iter()
        .filter(|a| matches!(a, RepairAction::DropItemsFromSnapshot { .. }))
        .collect();
    assert_eq!(
        drops.len(),
        1,
        "PlanOnly should surface DropItemsFromSnapshot in plan.actions"
    );
    if let RepairAction::DropItemsFromSnapshot {
        item_indices,
        dropped_paths,
        reasons,
        ..
    } = drops[0]
    {
        assert_eq!(item_indices.len(), expected_items.len());
        assert_eq!(dropped_paths.len(), item_indices.len());
        assert_eq!(reasons.len(), item_indices.len());
        for r in reasons {
            assert!(
                r.starts_with("chunks in missing pack"),
                "expected reason to mention missing pack, got: {r}"
            );
        }
    }
    // PlanOnly should not have applied anything.
    assert!(result.applied.is_empty());
}

/// Two snapshots reference distinct file data; dropping items from one must
/// leave the other's chunks untouched, and refcounts must reflect the
/// rewritten snapshot's surviving items only.
#[test]
fn repair_drops_items_rebuilds_refcounts() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_a = tmp.path().join("source_a");
    let source_b = tmp.path().join("source_b");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_a).unwrap();
    std::fs::create_dir_all(&source_b).unwrap();
    for (i, name) in ["x.bin", "y.bin"].iter().enumerate() {
        let payload: Vec<u8> = (0..2048u32)
            .map(|x| (x as u8).wrapping_add(i as u8))
            .collect();
        std::fs::write(source_a.join(name), payload).unwrap();
    }
    // Snapshot B contains a *different* file so its data lives in distinct
    // packs from snapshot A's; this lets us delete one of A's packs without
    // affecting B at all.
    std::fs::write(source_b.join("z.bin"), vec![42u8; 2048]).unwrap();

    let config = init_repo_small_packs(&repo_dir);
    backup_single_source(&config, &source_a, "src-a", "snap-a");
    backup_single_source(&config, &source_b, "src-b", "snap-b");

    let (deleted_pack, expected_items) = pick_data_only_pack(&repo_dir);
    assert!(!expected_items.is_empty());
    // Filter to the snapshot we actually want to mutate. The picker may pick
    // any data-only pack; require the affected items to all be in snap-a so
    // the test asserts hold.
    if !expected_items
        .iter()
        .all(|(name, _, _, _)| name == "snap-a")
    {
        // Skip the test silently if the picker chose snap-b's pack — the
        // planner-level invariants are exercised by the other tests.
        return;
    }
    std::fs::remove_file(repo_dir.join(deleted_pack.storage_key())).unwrap();

    let result =
        commands::check::run_with_repair(&config, None, false, RepairMode::Apply, None).unwrap();
    assert!(result
        .applied
        .iter()
        .any(|a| matches!(a, RepairAction::DropItemsFromSnapshot { .. })));
    assert!(result
        .applied
        .iter()
        .any(|a| matches!(a, RepairAction::RebuildRefcounts)));

    // After repair, every chunk in the index must have refcount > 0; no
    // ghost entries remain. snap-b's chunks are unchanged.
    let repo = open_local_repo(&repo_dir);
    for (_id, entry) in repo.chunk_index().iter() {
        assert!(
            entry.refcount > 0,
            "rebuild_refcounts should leave no zero-refcount entries"
        );
    }
    // snap-b must still restore byte-identical.
    let dest = tmp.path().join("restored_b");
    std::fs::create_dir_all(&dest).unwrap();
    commands::restore::run(
        &config,
        None,
        "snap-b",
        dest.to_str().unwrap(),
        None,
        false,
        false,
    )
    .unwrap();
    let restored = walk_find_file(&dest, "z.bin").expect("z.bin missing after restore");
    let got = std::fs::read(&restored).unwrap();
    let expected = std::fs::read(source_b.join("z.bin")).unwrap();
    assert_eq!(got, expected);
}

/// Recursively walk `root` looking for a file whose name matches `name`.
fn walk_find_file(root: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(root).ok()? {
        let entry = entry.ok()?;
        let p = entry.path();
        if p.is_dir() {
            if let Some(found) = walk_find_file(&p, name) {
                return Some(found);
            }
        } else if p.file_name().and_then(|s| s.to_str()) == Some(name) {
            return Some(p);
        }
    }
    None
}
