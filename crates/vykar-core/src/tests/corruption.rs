use std::path::{Path, PathBuf};

use test_case::test_case;

use crate::commands;
use crate::config::VykarConfig;
use crate::repo::control::CancelToken;
use vykar_protocol::PACK_HEADER_SIZE;

use super::helpers::{backup_single_source, init_repo, open_local_repo};

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(super) enum Corruption {
    BitFlipInPack,
    BitFlipInBlob,
    TruncatePack,
    ZeroFillRegion,
    DeletePack,
    DeleteIndex,
    TruncateIndex,
    CorruptSnapshot,
    CorruptConfig,
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

/// Create a repo with 10x4KB files and one snapshot "snap-base".
pub(super) fn setup_repo(tmp: &Path) -> (PathBuf, VykarConfig) {
    let repo_dir = tmp.join("repo");
    let source_dir = tmp.join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    for i in 0..10 {
        let data = vec![(i as u8).wrapping_mul(37); 4096];
        std::fs::write(source_dir.join(format!("file_{i:02}.bin")), &data).unwrap();
    }

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-base", "snap-base");
    (repo_dir, config)
}

// ---------------------------------------------------------------------------
// Helpers to locate repo artifacts
// ---------------------------------------------------------------------------

pub(super) fn find_first_pack_path(repo_dir: &Path) -> PathBuf {
    let repo = open_local_repo(repo_dir);
    let (_chunk_id, entry) = repo
        .chunk_index()
        .iter()
        .next()
        .expect("no chunks in index");
    repo_dir.join(entry.pack_id.storage_key())
}

pub(super) fn find_snapshot_path(repo_dir: &Path, name: &str) -> PathBuf {
    let repo = open_local_repo(repo_dir);
    let entry = repo
        .manifest()
        .find_snapshot(name)
        .expect("snapshot not found");
    repo_dir.join(entry.id.storage_key())
}

// ---------------------------------------------------------------------------
// Corruption applicators
// ---------------------------------------------------------------------------

/// End of a pack's blobs: the start of its trailer frame, or the pack's
/// length when it has none. The footer is readable without the key. Corrupting
/// "the middle of a pack" aims below this, at indexed blobs: no reader checks
/// the trailer yet.
fn blobs_end(data: &[u8]) -> usize {
    crate::repo::pack_trailer::parse_footer(data)
        .and_then(|len| crate::repo::pack_trailer::trailer_frame_start(data.len() as u64, len))
        .map_or(data.len(), |start| start as usize)
}

/// An offset near the middle of a pack's blobs, inside an indexed blob: the
/// middle of the blob that reaches past the midpoint of `[header, blobs_end)`.
/// Evenly sized blobs put that midpoint on a length prefix, which no reader
/// checks on its own.
fn blob_midpoint(repo_dir: &Path, data: &[u8]) -> usize {
    let mid = usize::midpoint(PACK_HEADER_SIZE, blobs_end(data)) as u64;
    let repo = open_local_repo(repo_dir);
    let (_, first) = repo
        .chunk_index()
        .iter()
        .next()
        .expect("no chunks in index");
    let mut spans: Vec<(u64, u64)> = repo
        .chunk_index()
        .iter()
        .filter(|(_, e)| e.pack_id == first.pack_id)
        .map(|(_, e)| (e.pack_offset, u64::from(e.stored_size)))
        .collect();
    spans.sort_unstable();
    let (offset, size) = spans
        .into_iter()
        .find(|(offset, size)| offset + size > mid)
        .expect("an indexed blob ends past the midpoint");
    (offset + size / 2) as usize
}

fn apply_bit_flip_in_pack(repo_dir: &Path) {
    let path = find_first_pack_path(repo_dir);
    let mut data = std::fs::read(&path).unwrap();
    let mid = blob_midpoint(repo_dir, &data);
    data[mid] ^= 0xff;
    std::fs::write(&path, &data).unwrap();
}

fn apply_bit_flip_in_blob(repo_dir: &Path) {
    let path = find_first_pack_path(repo_dir);
    let mut data = std::fs::read(&path).unwrap();
    // Past the pack header (PACK_HEADER_SIZE) and the 4-byte LE length prefix,
    // flip a byte inside the first blob's content.
    let offset = PACK_HEADER_SIZE + 4 + 1;
    assert!(
        offset < data.len(),
        "pack too small to contain a blob at expected offset"
    );
    data[offset] ^= 0xff;
    std::fs::write(&path, &data).unwrap();
}

fn apply_truncate_pack(repo_dir: &Path) {
    let path = find_first_pack_path(repo_dir);
    let data = std::fs::read(&path).unwrap();
    let half = usize::midpoint(PACK_HEADER_SIZE, blobs_end(&data));
    std::fs::write(&path, &data[..half]).unwrap();
}

fn apply_zero_fill_region(repo_dir: &Path) {
    let path = find_first_pack_path(repo_dir);
    let mut data = std::fs::read(&path).unwrap();
    let mid = blob_midpoint(repo_dir, &data);
    let end = (mid + 64).min(blobs_end(&data));
    for byte in &mut data[mid..end] {
        *byte = 0;
    }
    std::fs::write(&path, &data).unwrap();
}

fn apply_delete_pack(repo_dir: &Path) {
    let path = find_first_pack_path(repo_dir);
    std::fs::remove_file(path).unwrap();
}

fn apply_truncate_index(repo_dir: &Path) {
    let index_path = super::helpers::latest_head_path(repo_dir);
    std::fs::write(&index_path, [0u8; 10]).unwrap();
}

fn apply_delete_index(repo_dir: &Path) {
    super::helpers::remove_index_heads(repo_dir);
}

fn apply_corrupt_snapshot(repo_dir: &Path) {
    let path = find_snapshot_path(repo_dir, "snap-base");
    std::fs::write(&path, b"garbage-snapshot-data").unwrap();
}

fn apply_corrupt_config(repo_dir: &Path) {
    let config_path = repo_dir.join("config");
    std::fs::write(&config_path, b"garbage-config-data").unwrap();
}

pub(super) fn apply_corruption(corruption: Corruption, repo_dir: &Path) {
    match corruption {
        Corruption::BitFlipInPack => apply_bit_flip_in_pack(repo_dir),
        Corruption::BitFlipInBlob => apply_bit_flip_in_blob(repo_dir),
        Corruption::TruncatePack => apply_truncate_pack(repo_dir),
        Corruption::ZeroFillRegion => apply_zero_fill_region(repo_dir),
        Corruption::DeletePack => apply_delete_pack(repo_dir),
        Corruption::TruncateIndex => apply_truncate_index(repo_dir),
        Corruption::DeleteIndex => apply_delete_index(repo_dir),
        Corruption::CorruptSnapshot => apply_corrupt_snapshot(repo_dir),
        Corruption::CorruptConfig => apply_corrupt_config(repo_dir),
    }
}

// ---------------------------------------------------------------------------
// Test matrix: check detects corruption (Ok-path — check returns Ok with errors)
// ---------------------------------------------------------------------------

#[test_case(Corruption::BitFlipInPack ; "bit_flip_in_pack")]
#[test_case(Corruption::BitFlipInBlob ; "bit_flip_in_blob")]
#[test_case(Corruption::TruncatePack ; "truncate_pack")]
#[test_case(Corruption::ZeroFillRegion ; "zero_fill_region")]
#[test_case(Corruption::DeletePack ; "delete_pack")]
#[test_case(Corruption::CorruptSnapshot ; "corrupt_snapshot")]
fn check_detects_corruption_ok_path(corruption: Corruption) {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    // Healthy check should pass
    let healthy = commands::check::run(&config, None, true, false).unwrap();
    assert!(
        healthy.errors.is_empty(),
        "repo should be healthy before corruption, got errors: {:?}",
        healthy.errors
    );

    apply_corruption(corruption, &repo_dir);

    let result = commands::check::run(&config, None, true, false).unwrap();
    assert!(
        !result.errors.is_empty(),
        "{corruption:?}: check should detect corruption but returned no errors"
    );
}

// ---------------------------------------------------------------------------
// Test matrix: check detects corruption (Err-path — repo can't open or index can't load)
// ---------------------------------------------------------------------------

#[test_case(Corruption::TruncateIndex ; "truncate_index")]
#[test_case(Corruption::CorruptConfig ; "corrupt_config")]
fn check_detects_corruption_err_path(corruption: Corruption) {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(corruption, &repo_dir);

    let result = commands::check::run(&config, None, true, false);
    assert!(
        result.is_err(),
        "{corruption:?}: check should return Err, got Ok({:?})",
        result.unwrap()
    );
}

// ---------------------------------------------------------------------------
// Special case: deleted index
// ---------------------------------------------------------------------------

#[test]
fn check_with_deleted_index_head_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_delete_index(&repo_dir);

    // A segmented repository always has a head; without one the open fails.
    let err = commands::check::run(&config, None, true, false)
        .expect_err("deleted index head: check must fail");
    assert!(err.to_string().contains("index head missing"), "{err}");
}

// ---------------------------------------------------------------------------
// Test matrix: backup succeeds after pack corruption
// ---------------------------------------------------------------------------

/// Create a completely different source tree so the file cache misses.
fn create_fresh_source(tmp: &Path) -> PathBuf {
    let source2 = tmp.join("source2");
    std::fs::create_dir_all(&source2).unwrap();
    for i in 0..10 {
        let data = vec![(i as u8).wrapping_mul(53).wrapping_add(100); 4096];
        std::fs::write(source2.join(format!("fresh_{i:02}.dat")), &data).unwrap();
    }
    source2
}

#[test_case(Corruption::BitFlipInPack ; "bit_flip_in_pack")]
#[test_case(Corruption::BitFlipInBlob ; "bit_flip_in_blob")]
#[test_case(Corruption::TruncatePack ; "truncate_pack")]
#[test_case(Corruption::ZeroFillRegion ; "zero_fill_region")]
#[test_case(Corruption::DeletePack ; "delete_pack")]
fn backup_succeeds_after_pack_corruption(corruption: Corruption) {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(corruption, &repo_dir);

    let source2 = create_fresh_source(tmp.path());
    let stats = backup_single_source(&config, &source2, "src-fresh", "snap-after-corruption");
    assert!(stats.nfiles > 0, "backup should produce files");
}

#[test]
fn backup_succeeds_after_corrupt_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corrupt_snapshot(&repo_dir);

    let source2 = create_fresh_source(tmp.path());
    let stats = backup_single_source(&config, &source2, "src-fresh", "snap-after-corrupt-snap");
    assert!(stats.nfiles > 0, "backup should produce files");
}

#[test_case(Corruption::BitFlipInPack ; "bit_flip_in_pack")]
#[test_case(Corruption::BitFlipInBlob ; "bit_flip_in_blob")]
#[test_case(Corruption::TruncatePack ; "truncate_pack")]
#[test_case(Corruption::ZeroFillRegion ; "zero_fill_region")]
#[test_case(Corruption::DeletePack ; "delete_pack")]
#[test_case(Corruption::CorruptSnapshot ; "corrupt_snapshot")]
#[test_case(Corruption::TruncateIndex ; "truncate_index")]
fn repair_fixes_corruption(corruption: Corruption) {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(corruption, &repo_dir);

    let result = commands::check::run_with_repair(
        &config,
        None,
        true, // verify_data
        commands::check::RepairMode::Apply,
        None,
        &CancelToken::new(),
    )
    .unwrap();

    assert!(
        !result.applied.is_empty(),
        "{corruption:?}: repair should have applied actions"
    );
    assert!(
        result.repair_errors.is_empty(),
        "{corruption:?}: repair should have no errors, got: {:?}",
        result.repair_errors
    );

    // Verify repo is actually clean with a read-only check
    let check = commands::check::run(&config, None, true, false).unwrap();
    assert!(
        check.errors.is_empty(),
        "{corruption:?}: post-repair check should report 0 errors, got: {:?}",
        check.errors
    );
}

#[test_case(Corruption::CorruptConfig ; "corrupt_config")]
fn repair_not_possible_err_path(corruption: Corruption) {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(corruption, &repo_dir);

    let result = commands::check::run_with_repair(
        &config,
        None,
        true,
        commands::check::RepairMode::Apply,
        None,
        &CancelToken::new(),
    );
    assert!(
        result.is_err(),
        "{corruption:?}: repair should return Err, got Ok({:?})",
        result.unwrap()
    );
}
