use crate::commands::compact::compact_repo;
use crate::compress::Compression;
use crate::repo::pack::PackType;
use crate::testutil::test_repo_plaintext;

/// Helper: store two distinct chunks and flush, returning their chunk IDs.
fn store_two_chunks(
    repo: &mut crate::repo::Repository,
) -> (
    vykar_types::chunk_id::ChunkId,
    vykar_types::chunk_id::ChunkId,
) {
    let (id_a, _, _) = repo
        .store_chunk(
            b"chunk-a-data-for-compact-test",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    let (id_b, _, _) = repo
        .store_chunk(
            b"chunk-b-data-for-compact-test",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    repo.save_state().unwrap();
    (id_a, id_b)
}

#[test]
fn compact_clean_repo_no_repacking() {
    let mut repo = test_repo_plaintext();

    // Store chunks — all live, no dead blobs
    store_two_chunks(&mut repo);

    let stats = compact_repo(&mut repo, 10.0, None, false, false, None).unwrap();
    assert_eq!(stats.packs_repacked, 0);
    assert_eq!(stats.packs_deleted_empty, 0);
    assert_eq!(stats.space_freed, 0);
}

#[test]
fn compact_after_delete_repacks() {
    let mut repo = test_repo_plaintext();
    let (id_a, id_b) = store_two_chunks(&mut repo);

    // Remove chunk A from the index (simulates delete decrementing refcount to 0)
    repo.chunk_index_mut().decrement(&id_a);
    repo.save_state().unwrap();

    // Compact should detect the dead blob and repack
    let stats = compact_repo(&mut repo, 1.0, None, false, false, None).unwrap();

    // Should have repacked (or deleted) at least one pack
    assert!(
        stats.packs_repacked > 0 || stats.packs_deleted_empty > 0,
        "expected some packing activity, got: {stats:?}",
    );
    assert!(stats.space_freed > 0);

    // Chunk B should still be readable after compaction
    let data = repo.read_chunk(&id_b).unwrap();
    assert_eq!(data, b"chunk-b-data-for-compact-test");
}

#[test]
fn compact_threshold_filters() {
    let mut repo = test_repo_plaintext();

    // Store 10 chunks, then delete 1 — that's ~10% dead
    let mut ids = Vec::new();
    for i in 0..10 {
        let data = format!("chunk-number-{i}-for-threshold-test-padding-data");
        let (id, _, _) = repo
            .store_chunk(data.as_bytes(), Compression::None, PackType::Data)
            .unwrap();
        ids.push(id);
    }
    repo.save_state().unwrap();

    // Remove one chunk
    repo.chunk_index_mut().decrement(&ids[0]);
    repo.save_state().unwrap();

    // With a very high threshold (90%), nothing should be repacked
    let stats = compact_repo(&mut repo, 90.0, None, false, false, None).unwrap();
    assert_eq!(stats.packs_repacked, 0);
    assert_eq!(stats.packs_deleted_empty, 0);
}

#[test]
fn compact_dry_run_does_not_modify() {
    let mut repo = test_repo_plaintext();
    let (id_a, id_b) = store_two_chunks(&mut repo);

    // Remove chunk A to create dead blobs
    repo.chunk_index_mut().decrement(&id_a);
    repo.save_state().unwrap();

    // Count packs before
    let packs_before = count_all_packs(&repo);

    let stats = compact_repo(&mut repo, 1.0, None, true, false, None).unwrap();

    // Dry run should report activity
    assert!(stats.packs_repacked > 0 || stats.packs_deleted_empty > 0 || stats.space_freed > 0);

    // But pack count should be unchanged
    let packs_after = count_all_packs(&repo);
    assert_eq!(packs_before, packs_after, "dry run should not modify packs");

    // Both old chunks' pack data should still exist in storage
    // Chunk B should still be readable
    let data = repo.read_chunk(&id_b).unwrap();
    assert_eq!(data, b"chunk-b-data-for-compact-test");
}

#[test]
fn compact_empty_pack_deleted() {
    let mut repo = test_repo_plaintext();

    // Store a single chunk and flush
    let (id_a, _, _) = repo
        .store_chunk(
            b"lone-chunk-data-for-empty-pack-test",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    repo.save_state().unwrap();

    // Remove it — the pack is now entirely dead
    repo.chunk_index_mut().decrement(&id_a);
    repo.save_state().unwrap();

    let stats = compact_repo(&mut repo, 1.0, None, false, false, None).unwrap();

    assert_eq!(stats.packs_deleted_empty, 1);
    assert_eq!(stats.packs_repacked, 0);
    assert!(stats.space_freed > 0);
}

/// Count total pack files across all shard dirs.
fn count_all_packs(repo: &crate::repo::Repository) -> usize {
    let mut count = 0;
    for shard in 0u16..256 {
        let prefix = format!("packs/{:02x}/", shard);
        if let Ok(keys) = repo.storage.list(&prefix) {
            count += keys.iter().filter(|k| !k.ends_with('/')).count();
        }
    }
    count
}
