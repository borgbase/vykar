use std::collections::{HashMap, HashSet};

use crate::commands;
use crate::commands::check::{RepairAction, RepairMode, RepairResult};
use crate::config::VykarConfig;
use crate::repo::pack::PACK_HEADER_SIZE;
use crate::snapshot::item::ItemType;

use super::corruption::{apply_corruption, setup_repo, Corruption};
use super::helpers::{backup_single_source, init_repo, make_test_config, open_local_repo};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn has_action(actions: &[RepairAction], pred: impl Fn(&RepairAction) -> bool) -> bool {
    actions.iter().any(pred)
}

fn plan_only(config: &VykarConfig, verify_data: bool) -> RepairResult {
    commands::check::run_with_repair(config, None, verify_data, RepairMode::PlanOnly, None).unwrap()
}

fn apply_repair(config: &VykarConfig, verify_data: bool) -> RepairResult {
    commands::check::run_with_repair(config, None, verify_data, RepairMode::Apply, None).unwrap()
}

fn assert_clean_after_repair(config: &VykarConfig) {
    let result = plan_only(config, false);
    assert!(
        !result.plan.has_data_loss,
        "expected clean repo after repair, but plan has data-loss actions: {:?}",
        result.plan.actions
    );
    assert!(
        result.plan.actions.len() == 1
            && matches!(result.plan.actions[0], RepairAction::RebuildRefcounts),
        "expected only RebuildRefcounts after repair, got: {:?}",
        result.plan.actions
    );
}

// ---------------------------------------------------------------------------
// 1. Healthy repo — plan should have only RebuildRefcounts
// ---------------------------------------------------------------------------

#[test]
fn plan_only_healthy_repo_refcount_only() {
    let tmp = tempfile::tempdir().unwrap();
    let (_repo_dir, config) = setup_repo(tmp.path());

    let result = plan_only(&config, false);

    assert!(
        !result.plan.has_data_loss,
        "healthy repo should not have data-loss plan"
    );
    assert!(
        result.plan.actions.len() == 1
            && matches!(result.plan.actions[0], RepairAction::RebuildRefcounts),
        "expected only RebuildRefcounts, got: {:?}",
        result.plan.actions
    );
    assert!(
        result.applied.is_empty(),
        "PlanOnly should not produce applied actions"
    );
}

// ---------------------------------------------------------------------------
// 2. Corrupt snapshot — plan should have RemoveCorruptSnapshot
// ---------------------------------------------------------------------------

#[test]
fn plan_only_corrupt_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(Corruption::CorruptSnapshot, &repo_dir);

    let result = plan_only(&config, false);

    assert!(
        result.plan.has_data_loss,
        "corrupt snapshot plan should have data loss"
    );
    assert!(
        has_action(&result.plan.actions, |a| matches!(
            a,
            RepairAction::RemoveCorruptSnapshot { .. }
        )),
        "expected RemoveCorruptSnapshot action, got: {:?}",
        result.plan.actions
    );
}

// ---------------------------------------------------------------------------
// 3. Deleted pack — plan should have RemoveDanglingIndexEntries
// ---------------------------------------------------------------------------

#[test]
fn plan_only_delete_pack() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(Corruption::DeletePack, &repo_dir);

    let result = plan_only(&config, false);

    assert!(
        result.plan.has_data_loss,
        "deleted pack plan should have data loss"
    );
    assert!(
        has_action(&result.plan.actions, |a| matches!(
            a,
            RepairAction::RemoveDanglingIndexEntries { .. }
        )),
        "expected RemoveDanglingIndexEntries action, got: {:?}",
        result.plan.actions
    );
}

// ---------------------------------------------------------------------------
// 4. Bit flip in pack with verify_data — RemoveCorruptPack or RemoveCorruptChunks
// ---------------------------------------------------------------------------

#[test]
fn plan_only_bit_flip_pack_with_verify_data() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(Corruption::BitFlipInPack, &repo_dir);

    let result = plan_only(&config, true);

    assert!(
        result.plan.has_data_loss,
        "bit-flip pack plan should have data loss"
    );
    assert!(
        has_action(&result.plan.actions, |a| matches!(
            a,
            RepairAction::RemoveCorruptPack { .. } | RepairAction::RemoveCorruptChunks { .. }
        )),
        "expected RemoveCorruptPack or RemoveCorruptChunks, got: {:?}",
        result.plan.actions
    );
}

// ---------------------------------------------------------------------------
// 5. Bit flip in blob with verify_data — RemoveCorruptChunks
// ---------------------------------------------------------------------------

#[test]
fn plan_only_bit_flip_blob_with_verify_data() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(Corruption::BitFlipInBlob, &repo_dir);

    let result = plan_only(&config, true);

    assert!(
        result.plan.has_data_loss,
        "bit-flip blob plan should have data loss"
    );
    assert!(
        has_action(&result.plan.actions, |a| matches!(
            a,
            RepairAction::RemoveCorruptChunks { .. }
        )),
        "expected RemoveCorruptChunks, got: {:?}",
        result.plan.actions
    );
}

// ---------------------------------------------------------------------------
// 6. Invalid snapshot key — RemoveInvalidSnapshotKey
// ---------------------------------------------------------------------------

#[test]
fn plan_only_invalid_snapshot_key() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    // Write garbage to snapshots/not-valid-hex
    let snapshots_dir = repo_dir.join("snapshots");
    std::fs::write(snapshots_dir.join("not-valid-hex"), b"garbage").unwrap();

    let result = plan_only(&config, false);

    assert!(
        result.plan.has_data_loss,
        "invalid snapshot key plan should have data loss"
    );
    assert!(
        has_action(&result.plan.actions, |a| matches!(
            a,
            RepairAction::RemoveInvalidSnapshotKey { storage_key }
                if storage_key.contains("not-valid-hex")
        )),
        "expected RemoveInvalidSnapshotKey containing 'not-valid-hex', got: {:?}",
        result.plan.actions
    );
}

// ---------------------------------------------------------------------------
// 7. Orphan corrupt snapshot blob (valid hex ID, not in manifest)
// ---------------------------------------------------------------------------

#[test]
fn plan_only_orphan_corrupt_snapshot_blob() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    // Write garbage to snapshots/<64-char hex> — valid hex ID but not in manifest
    let fake_id = "aa".repeat(32); // 64 hex chars
    let snapshots_dir = repo_dir.join("snapshots");
    std::fs::write(snapshots_dir.join(&fake_id), b"garbage-orphan-snapshot").unwrap();

    let result = plan_only(&config, false);

    assert!(
        has_action(&result.plan.actions, |a| matches!(
            a,
            RepairAction::RemoveCorruptSnapshot { name, .. } if name.is_none()
        )),
        "expected RemoveCorruptSnapshot with name: None, got: {:?}",
        result.plan.actions
    );
}

// ---------------------------------------------------------------------------
// 8. PlanOnly result has empty applied and repair_errors
// ---------------------------------------------------------------------------

#[test]
fn plan_only_result_has_empty_applied() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(Corruption::CorruptSnapshot, &repo_dir);

    let result = plan_only(&config, false);

    assert!(
        result.applied.is_empty(),
        "PlanOnly should never produce applied actions, got: {:?}",
        result.applied
    );
    assert!(
        result.repair_errors.is_empty(),
        "PlanOnly should never produce repair errors, got: {:?}",
        result.repair_errors
    );
}

// ---------------------------------------------------------------------------
// 9. Apply corrupt snapshot, then verify clean
// ---------------------------------------------------------------------------

#[test]
fn apply_corrupt_snapshot_then_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(Corruption::CorruptSnapshot, &repo_dir);

    let result = apply_repair(&config, false);

    assert!(
        has_action(&result.applied, |a| matches!(
            a,
            RepairAction::RemoveCorruptSnapshot { .. }
        )),
        "expected RemoveCorruptSnapshot in applied, got: {:?}",
        result.applied
    );
    assert!(
        has_action(&result.applied, |a| matches!(
            a,
            RepairAction::RebuildRefcounts
        )),
        "expected RebuildRefcounts in applied, got: {:?}",
        result.applied
    );

    assert_clean_after_repair(&config);
}

// ---------------------------------------------------------------------------
// 10. Apply deleted pack, then verify clean
// ---------------------------------------------------------------------------

#[test]
fn apply_delete_pack_then_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(Corruption::DeletePack, &repo_dir);

    let result = apply_repair(&config, false);

    assert!(
        has_action(&result.applied, |a| matches!(
            a,
            RepairAction::RemoveDanglingIndexEntries { .. }
        )),
        "expected RemoveDanglingIndexEntries in applied, got: {:?}",
        result.applied
    );
    assert!(
        has_action(&result.applied, |a| matches!(
            a,
            RepairAction::RemoveDanglingSnapshot { .. }
        )),
        "expected RemoveDanglingSnapshot in applied, got: {:?}",
        result.applied
    );
    assert!(
        has_action(&result.applied, |a| matches!(
            a,
            RepairAction::RebuildRefcounts
        )),
        "expected RebuildRefcounts in applied, got: {:?}",
        result.applied
    );

    assert_clean_after_repair(&config);
}

// ---------------------------------------------------------------------------
// 11. Apply bit-flip pack with verify_data, then verify clean
// ---------------------------------------------------------------------------

#[test]
fn apply_bit_flip_pack_verify_data_then_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(Corruption::BitFlipInPack, &repo_dir);

    let result = apply_repair(&config, true);

    assert!(
        has_action(&result.applied, |a| matches!(
            a,
            RepairAction::RemoveCorruptPack { .. } | RepairAction::RemoveCorruptChunks { .. }
        )),
        "expected pack/chunk removal in applied, got: {:?}",
        result.applied
    );
    assert!(
        has_action(&result.applied, |a| matches!(
            a,
            RepairAction::RebuildRefcounts
        )),
        "expected RebuildRefcounts in applied, got: {:?}",
        result.applied
    );

    assert_clean_after_repair(&config);
}

// ---------------------------------------------------------------------------
// 12. verify_data is required to detect content corruption
// ---------------------------------------------------------------------------

#[test]
fn verify_data_required_for_content_corruption() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(Corruption::BitFlipInBlob, &repo_dir);

    // Without verify_data: no corrupt chunk/pack actions
    let without = plan_only(&config, false);
    assert!(
        !has_action(&without.plan.actions, |a| matches!(
            a,
            RepairAction::RemoveCorruptPack { .. } | RepairAction::RemoveCorruptChunks { .. }
        )),
        "without verify_data, should not find corrupt chunk/pack actions, got: {:?}",
        without.plan.actions
    );

    // With verify_data: should find them
    let with = plan_only(&config, true);
    assert!(
        has_action(&with.plan.actions, |a| matches!(
            a,
            RepairAction::RemoveCorruptPack { .. } | RepairAction::RemoveCorruptChunks { .. }
        )),
        "with verify_data, should find corrupt chunk/pack actions, got: {:?}",
        with.plan.actions
    );
}

// ---------------------------------------------------------------------------
// 13. Per-chunk corruption preserves sibling chunks in the same pack
// ---------------------------------------------------------------------------

#[test]
fn per_chunk_corruption_preserves_siblings() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    // Create 10 x 4KB files to get multiple chunks
    for i in 0..10 {
        let data = vec![(i as u8).wrapping_mul(37); 4096];
        std::fs::write(source_dir.join(format!("file_{i:02}.bin")), &data).unwrap();
    }

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-sibling", "snap-sibling");

    // Group chunk_index entries by pack_id, find a pack with >1 chunk
    let repo = open_local_repo(&repo_dir);
    let mut by_pack: HashMap<
        vykar_types::pack_id::PackId,
        Vec<(
            vykar_types::chunk_id::ChunkId,
            crate::index::ChunkIndexEntry,
        )>,
    > = HashMap::new();
    for (chunk_id, entry) in repo.chunk_index().iter() {
        by_pack
            .entry(entry.pack_id)
            .or_default()
            .push((*chunk_id, *entry));
    }

    let (target_pack_id, chunks_in_pack) = by_pack
        .iter()
        .find(|(_, chunks)| chunks.len() > 1)
        .expect("need a pack with more than one chunk for this test");
    let target_pack_id = *target_pack_id;
    let total_chunks_in_pack = chunks_in_pack.len();

    // Pick one chunk and XOR a byte inside its blob range
    let (_, target_entry) = &chunks_in_pack[0];
    let pack_path = repo_dir.join(target_pack_id.storage_key());
    let mut pack_data = std::fs::read(&pack_path).unwrap();
    let blob_start = PACK_HEADER_SIZE as u64 + target_entry.pack_offset;
    // Flip a byte inside the blob (past the 4-byte length prefix)
    let flip_offset = blob_start as usize + 4 + 1;
    assert!(
        flip_offset < pack_data.len(),
        "flip offset {flip_offset} exceeds pack size {}",
        pack_data.len()
    );
    pack_data[flip_offset] ^= 0xff;
    std::fs::write(&pack_path, &pack_data).unwrap();
    drop(repo);

    let result = plan_only(&config, true);

    // Should emit RemoveCorruptChunks, not RemoveCorruptPack
    assert!(
        has_action(&result.plan.actions, |a| matches!(
            a,
            RepairAction::RemoveCorruptChunks { .. }
        )),
        "expected RemoveCorruptChunks (not RemoveCorruptPack), got: {:?}",
        result.plan.actions
    );

    // The number of corrupted chunk IDs should be less than the total in the pack
    for action in &result.plan.actions {
        if let RepairAction::RemoveCorruptChunks { pack_id, chunk_ids } = action {
            if *pack_id == target_pack_id {
                assert!(
                    chunk_ids.len() < total_chunks_in_pack,
                    "corrupted {} of {} chunks — should preserve siblings",
                    chunk_ids.len(),
                    total_chunks_in_pack
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 14. Deleted index: plan includes doomed snapshots
// ---------------------------------------------------------------------------

#[test]
fn deleted_index_plan_includes_doomed_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    apply_corruption(Corruption::DeleteIndex, &repo_dir);

    // With an empty index, all snapshot chunk refs are dangling — the plan
    // should include RemoveDanglingSnapshot so the user sees the full picture.
    let plan_result = plan_only(&config, false);
    assert!(
        plan_result.plan.has_data_loss,
        "PlanOnly with deleted index should show has_data_loss=true"
    );
    assert!(
        has_action(&plan_result.plan.actions, |a| matches!(
            a,
            RepairAction::RemoveDanglingSnapshot { .. }
        )),
        "PlanOnly should include RemoveDanglingSnapshot, got: {:?}",
        plan_result.plan.actions
    );
    assert!(
        has_action(&plan_result.plan.actions, |a| matches!(
            a,
            RepairAction::RebuildRefcounts
        )),
        "expected RebuildRefcounts in plan, got: {:?}",
        plan_result.plan.actions
    );

    // Apply should also produce RemoveDanglingSnapshot.
    let apply_result = apply_repair(&config, false);
    assert!(
        has_action(&apply_result.applied, |a| matches!(
            a,
            RepairAction::RemoveDanglingSnapshot { .. }
        )),
        "Apply with deleted index should produce RemoveDanglingSnapshot, got: {:?}",
        apply_result.applied
    );
}

// ---------------------------------------------------------------------------
// 15. Refcount rebuild excludes doomed snapshots
// ---------------------------------------------------------------------------

#[test]
fn refcount_rebuild_excludes_doomed_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_a = tmp.path().join("source_a");
    let source_b = tmp.path().join("source_b");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_a).unwrap();
    std::fs::create_dir_all(&source_b).unwrap();

    // Create non-overlapping content: different byte patterns
    for i in 0..5 {
        let data_a = vec![(i as u8).wrapping_mul(11).wrapping_add(1); 4096];
        std::fs::write(source_a.join(format!("a_{i:02}.bin")), &data_a).unwrap();

        let data_b = vec![(i as u8).wrapping_mul(53).wrapping_add(128); 4096];
        std::fs::write(source_b.join(format!("b_{i:02}.bin")), &data_b).unwrap();
    }

    // Use small pack sizes to get separate packs per backup
    let mut config = make_test_config(&repo_dir);
    config.repository.min_pack_size = 8192;
    config.repository.max_pack_size = 8192;
    commands::init::run(&config, None).unwrap();

    backup_single_source(&config, &source_a, "src-a", "snap-A");
    backup_single_source(&config, &source_b, "src-b", "snap-B");

    // Open repo to collect chunk IDs per snapshot and map to pack_ids
    let mut repo = open_local_repo(&repo_dir);

    // Collect all chunk IDs referenced by snap-A
    let meta_a = commands::list::load_snapshot_meta(&repo, "snap-A").unwrap();
    let mut snap_a_chunks: HashSet<vykar_types::chunk_id::ChunkId> = HashSet::new();
    for chunk_id in &meta_a.item_ptrs {
        snap_a_chunks.insert(*chunk_id);
    }
    // Also collect file chunks from item stream
    {
        let items_stream = commands::list::load_snapshot_item_stream(&mut repo, "snap-A").unwrap();
        commands::list::for_each_decoded_item(&items_stream, |item| {
            if item.entry_type == ItemType::RegularFile {
                for chunk_ref in &item.chunks {
                    snap_a_chunks.insert(chunk_ref.id);
                }
            }
            Ok(())
        })
        .unwrap();
    }

    // Collect pack_ids referenced by snap-A chunks
    let mut snap_a_pack_ids: HashSet<vykar_types::pack_id::PackId> = HashSet::new();
    for chunk_id in &snap_a_chunks {
        if let Some(entry) = repo.chunk_index().get(chunk_id) {
            snap_a_pack_ids.insert(entry.pack_id);
        }
    }

    // Collect all chunk IDs referenced by snap-B
    let meta_b = commands::list::load_snapshot_meta(&repo, "snap-B").unwrap();
    let mut snap_b_chunks: HashSet<vykar_types::chunk_id::ChunkId> = HashSet::new();
    for chunk_id in &meta_b.item_ptrs {
        snap_b_chunks.insert(*chunk_id);
    }
    {
        let items_stream = commands::list::load_snapshot_item_stream(&mut repo, "snap-B").unwrap();
        commands::list::for_each_decoded_item(&items_stream, |item| {
            if item.entry_type == ItemType::RegularFile {
                for chunk_ref in &item.chunks {
                    snap_b_chunks.insert(chunk_ref.id);
                }
            }
            Ok(())
        })
        .unwrap();
    }

    let mut snap_b_pack_ids: HashSet<vykar_types::pack_id::PackId> = HashSet::new();
    for chunk_id in &snap_b_chunks {
        if let Some(entry) = repo.chunk_index().get(chunk_id) {
            snap_b_pack_ids.insert(entry.pack_id);
        }
    }

    // Find a pack_id only referenced by snap-A (not shared with snap-B)
    let snap_a_only_packs: Vec<_> = snap_a_pack_ids
        .difference(&snap_b_pack_ids)
        .copied()
        .collect();
    assert!(
        !snap_a_only_packs.is_empty(),
        "need at least one pack exclusive to snap-A"
    );

    let target_pack = snap_a_only_packs[0];
    let pack_path = repo_dir.join(target_pack.storage_key());
    drop(repo);

    // Delete the pack exclusive to snap-A
    std::fs::remove_file(&pack_path).unwrap();

    // Apply repair
    let result = apply_repair(&config, false);

    // (1) applied either drops items from snap-A (preferred) or removes the
    // whole snapshot — both branches must end with RebuildRefcounts.
    let drop_items = has_action(&result.applied, |a| {
        matches!(
            a,
            RepairAction::DropItemsFromSnapshot { ref snapshot_name, .. }
                if snapshot_name == "snap-A"
        )
    });
    let remove_dangling = has_action(&result.applied, |a| {
        matches!(
            a,
            RepairAction::RemoveDanglingSnapshot { ref snapshot_name, .. }
                if snapshot_name == "snap-A"
        )
    });
    assert!(
        drop_items || remove_dangling,
        "expected DropItemsFromSnapshot or RemoveDanglingSnapshot for snap-A, got: {:?}",
        result.applied
    );
    assert!(
        has_action(&result.applied, |a| matches!(
            a,
            RepairAction::RebuildRefcounts
        )),
        "expected RebuildRefcounts in applied, got: {:?}",
        result.applied
    );

    // (2) snap-B survives. snap-A may be retained (when item-level repair
    // covers it) or removed (when every snap-A item was in the deleted pack).
    let repo = open_local_repo(&repo_dir);
    assert!(
        repo.manifest().find_snapshot("snap-B").is_some(),
        "snap-B should survive the repair"
    );
    if drop_items {
        assert!(
            repo.manifest().find_snapshot("snap-A").is_some(),
            "snap-A should be retained under a new id when item-level repair applies"
        );
    } else {
        assert!(
            repo.manifest().find_snapshot("snap-A").is_none(),
            "snap-A should have been removed when whole-snapshot repair applies"
        );
    }

    // (3) refcounts for snap-B chunks should be exactly 1 (non-overlapping
    // content with snap-A by construction).
    for chunk_id in &snap_b_chunks {
        if let Some(entry) = repo.chunk_index().get(chunk_id) {
            assert_eq!(
                entry.refcount, 1,
                "snap-B chunk {chunk_id} should have refcount == 1, got {}",
                entry.refcount
            );
        }
    }
    drop(repo);

    // (4) Repo should be clean
    assert_clean_after_repair(&config);
}

// ---------------------------------------------------------------------------
// 16. PlanOnly does not take maintenance lock (sessions don't block it)
// ---------------------------------------------------------------------------

#[test]
fn plan_only_does_not_take_maintenance_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    // Create sessions/ directory and write a session marker
    let sessions_dir = repo_dir.join("sessions");
    std::fs::create_dir_all(&sessions_dir).unwrap();
    let session_id = "deadbeef-1234-5678-9abc-def012345678";
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let session_json = format!(
        r#"{{"hostname":"other-host","pid":99999,"registered_at":"{now}","last_refresh":"{now}"}}"#
    );
    std::fs::write(
        sessions_dir.join(format!("{session_id}.json")),
        session_json.as_bytes(),
    )
    .unwrap();

    // PlanOnly should succeed — it does not acquire maintenance lock
    let result = plan_only(&config, false);
    assert!(
        !result.plan.actions.is_empty(),
        "PlanOnly should produce at least RebuildRefcounts"
    );

    // Session marker should still exist
    assert!(
        sessions_dir.join(format!("{session_id}.json")).exists(),
        "session marker should not be removed by PlanOnly"
    );
}

// ---------------------------------------------------------------------------
// 17. Apply refuses with active sessions
// ---------------------------------------------------------------------------

#[test]
fn apply_refuses_with_active_sessions() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo_dir, config) = setup_repo(tmp.path());

    // Create sessions/ directory and write a session marker
    let sessions_dir = repo_dir.join("sessions");
    std::fs::create_dir_all(&sessions_dir).unwrap();
    let session_id = "deadbeef-1234-5678-9abc-def012345678";
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let session_json = format!(
        r#"{{"hostname":"other-host","pid":99999,"registered_at":"{now}","last_refresh":"{now}"}}"#
    );
    std::fs::write(
        sessions_dir.join(format!("{session_id}.json")),
        session_json.as_bytes(),
    )
    .unwrap();

    // Apply should fail with ActiveSessions
    let err = commands::check::run_with_repair(&config, None, false, RepairMode::Apply, None)
        .unwrap_err();

    assert!(
        matches!(err, vykar_types::error::VykarError::ActiveSessions(_)),
        "expected ActiveSessions error, got: {err}"
    );
}
