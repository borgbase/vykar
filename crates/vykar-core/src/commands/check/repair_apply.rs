use std::collections::{HashMap, HashSet};

use super::types::{IntegrityIssue, RepairAction, RepairPlan};
use crate::commands::list::{for_each_decoded_item, load_snapshot_item_stream, load_snapshot_meta};
use crate::compress::Compression;
use crate::index::ChunkIndexEntry;
use crate::repo::format::{pack_object_with_context, ObjectType};
use crate::repo::manifest::SnapshotEntry;
use crate::repo::Repository;
use crate::snapshot::item::ItemType;
use crate::snapshot::SnapshotMeta;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

/// Probe whether the backend supports deletes (i.e. is not append-only).
/// Tries to delete a non-existent sentinel key; if the error indicates a
/// permission or authorization failure, the backend is append-only.
pub(super) fn probe_deletes_allowed(storage: &dyn StorageBackend) -> bool {
    match storage.delete("snapshots/.repair-probe") {
        Ok(()) => true,
        // Transient/not-found errors → assume deletes are allowed.
        Err(ref e) => !e.is_write_refusal(),
    }
}

/// In-flight bookkeeping for one snapshot rewrite.
struct PreparedRewrite {
    /// Index into `plan.actions` of the originating `DropItemsFromSnapshot`,
    /// so a successful publish can be reported back via `applied.push`.
    action_idx: usize,
    old_id: SnapshotId,
    new_id: SnapshotId,
    new_entry: SnapshotEntry,
    new_packed: Vec<u8>,
}

/// Build, flush, and publish the snapshot rewrites requested by every
/// `DropItemsFromSnapshot` action in the plan.
///
/// The publish loop intentionally never returns `?`: a partial publication
/// must not bypass the trailing whole-snapshot deletes or refcount rebuild
/// in [`execute_repair`]. The only fatal case is "old DELETE failed AND
/// rollback DELETE also failed" — that returns the typed duplicate-blobs
/// error so the operator can manually intervene.
fn apply_item_level_rewrites(
    repo: &mut Repository,
    plan: &RepairPlan,
    applied: &mut Vec<RepairAction>,
    repair_errors: &mut Vec<String>,
) -> Result<()> {
    // Bail out early if there's nothing to do.
    let has_drops = plan
        .actions
        .iter()
        .any(|a| matches!(a, RepairAction::DropItemsFromSnapshot { .. }));
    if !has_drops {
        return Ok(());
    }

    // 1. Begin a write session so store_chunk can pack new tree chunks.
    repo.begin_write_session()?;

    // 2. Prepare each rewrite (defer all PUT/DELETE until after Step 4 of
    //    this routine, mirroring commit_prepare → PUT ordering).
    let mut prepared: Vec<PreparedRewrite> = Vec::new();
    for (action_idx, action) in plan.actions.iter().enumerate() {
        let RepairAction::DropItemsFromSnapshot {
            snapshot_id,
            snapshot_name,
            item_indices,
            ..
        } = action
        else {
            continue;
        };
        match prepare_rewrite(repo, *snapshot_id, snapshot_name, item_indices, action_idx) {
            Ok(rw) => prepared.push(rw),
            Err(e) => {
                repair_errors.push(format!(
                    "failed to prepare rewrite for snapshot '{snapshot_name}': {e}"
                ));
            }
        }
    }

    if prepared.is_empty() {
        // No work survived preparation — close the session via save_state so
        // any pending state is consistent. The trailing rebuild handles any
        // orphan chunks store_chunk may have produced.
        repo.flush_packs()?;
        repo.save_state()?;
        return Ok(());
    }

    // 3. Flush new data/tree packs to storage.
    repo.flush_packs()?;

    // 4. Persist the chunk index so newly-stored chunks are durable BEFORE
    //    any snapshot blob references them.
    repo.save_state()?;

    // 5. Publish each rewrite: PUT new blob, swap manifest, DELETE old.
    for rw in &prepared {
        // 5a. PUT new snapshot blob (the new commit point).
        if let Err(e) = repo.storage.put(&rw.new_id.storage_key(), &rw.new_packed) {
            repair_errors.push(format!(
                "failed to publish rewritten snapshot '{}': {e}",
                rw.new_entry.name
            ));
            continue;
        }

        // 5b. Manifest swap: remove the old name → push the new entry.
        let removed = repo.manifest_mut().remove_snapshot(&rw.new_entry.name);
        repo.manifest_mut().snapshots.push(rw.new_entry.clone());

        // 5c. DELETE old blob; on failure, attempt rollback.
        match repo.storage.delete(&rw.old_id.storage_key()) {
            Ok(()) => {
                applied.push(
                    plan.actions
                        .get(rw.action_idx)
                        .expect("rw.action_idx was assigned from enumerate(plan.actions)")
                        .clone(),
                );
            }
            Err(e) => {
                let new_delete = repo.storage.delete(&rw.new_id.storage_key());
                repo.manifest_mut().remove_snapshot(&rw.new_entry.name);
                if let Some(prev) = removed {
                    repo.manifest_mut().snapshots.push(prev);
                }

                if new_delete.is_err() {
                    return Err(VykarError::Other(format!(
                        "repair partially applied: snapshot '{}' has duplicate \
                         blobs at IDs {} (old) and {} (new); cannot delete either. \
                         Manual cleanup required, then re-run `vykar check --repair` \
                         to settle refcounts.",
                        rw.new_entry.name, rw.old_id, rw.new_id
                    )));
                }

                repair_errors.push(format!(
                    "failed to delete old blob for '{}' after rewrite; \
                     rolled back: {e}",
                    rw.new_entry.name
                ));
            }
        }
    }

    Ok(())
}

/// Re-encode an items_stream with the listed `item_indices` dropped, store
/// the resulting tree chunks (via the active write session), and pack a
/// fresh `SnapshotMeta` blob for publication. Does not write the blob.
fn prepare_rewrite(
    repo: &mut Repository,
    snapshot_id: SnapshotId,
    snapshot_name: &str,
    item_indices: &[usize],
    action_idx: usize,
) -> Result<PreparedRewrite> {
    use crate::commands::backup::{
        append_item_to_stream, flush_item_stream_chunk, items_chunker_config,
    };

    let drop: HashSet<usize> = item_indices.iter().copied().collect();

    let old_meta: SnapshotMeta = load_snapshot_meta(repo, snapshot_name)?;
    let old_stream = load_snapshot_item_stream(repo, snapshot_name, None)?;

    let chunker = items_chunker_config();
    let compression = Compression::default();

    let mut new_item_ptrs: Vec<ChunkId> = Vec::new();
    let mut buf: Vec<u8> = Vec::new();
    let mut nfiles: u64 = 0;
    let mut original_size: u64 = 0;
    let mut idx: usize = 0;

    let mut kept: usize = 0;
    for_each_decoded_item(&old_stream, |item| {
        let here = idx;
        idx += 1;
        if drop.contains(&here) {
            return Ok(());
        }
        kept += 1;
        if item.entry_type == ItemType::RegularFile {
            nfiles += 1;
            original_size += item.size;
        }
        append_item_to_stream(
            repo,
            &mut buf,
            &mut new_item_ptrs,
            &item,
            &chunker,
            compression,
        )
    })?;

    if kept == 0 {
        return Err(VykarError::Other(format!(
            "rewrite would empty snapshot '{snapshot_name}' (planner gate bypassed)"
        )));
    }

    flush_item_stream_chunk(repo, &mut buf, &mut new_item_ptrs, compression)?;

    // Build new SnapshotMeta. Time fields and source labels mirror the
    // original so users still see when the data was captured.
    let mut new_meta = old_meta.clone();
    new_meta.item_ptrs = new_item_ptrs;
    new_meta.stats.nfiles = nfiles;
    new_meta.stats.original_size = original_size;
    // Stamp the current format version on the rewritten snapshot — the
    // rewriter never carries forward a legacy 0.
    new_meta.format_version = crate::snapshot::CURRENT_FORMAT_VERSION;
    // deduplicated_size: preserved from the old meta (display-only; minor
    // inflation is acceptable per #123).

    let new_id = SnapshotId::generate();
    let meta_bytes = rmp_serde::to_vec(&new_meta)?;
    let new_packed = pack_object_with_context(
        ObjectType::SnapshotMeta,
        new_id.as_bytes(),
        &meta_bytes,
        repo.crypto.as_ref(),
    )?;

    let new_entry = SnapshotEntry {
        id: new_id,
        name: snapshot_name.to_string(),
        time: new_meta.time,
        source_label: new_meta.source_label.clone(),
        label: new_meta.label.clone(),
        source_paths: new_meta.source_paths.clone(),
        hostname: new_meta.hostname.clone(),
    };

    Ok(PreparedRewrite {
        action_idx,
        old_id: snapshot_id,
        new_id,
        new_entry,
        new_packed,
    })
}

/// Rewrite each key copy named by a [`RepairAction::RestoreKeyCopy`] from the
/// key this repository opened with.
///
/// The bytes written are [`Repository::key_blob`], not a fresh read of the
/// source copy: the scan established `from` as byte-identical to it, but
/// nothing holds that between scan and write, and the verified bytes are
/// already in hand. `from` is kept on the action for the plan printout.
///
/// Creating a missing copy succeeds even on an append-only server, which
/// blocks overwrites but not creations. *Replacing* a divergent copy is an
/// overwrite and will be refused there.
fn restore_key_copies(
    repo: &mut Repository,
    plan: &RepairPlan,
    applied: &mut Vec<RepairAction>,
    repair_errors: &mut Vec<String>,
) {
    for action in &plan.actions {
        let RepairAction::RestoreKeyCopy { to, .. } = action else {
            continue;
        };
        let Some(bytes) = repo.key_blob() else {
            repair_errors.push(format!(
                "cannot restore {to}: this repository has no key to restore it from"
            ));
            continue;
        };
        match repo.storage.put(to, bytes) {
            Ok(()) => {
                tracing::info!("restored repository key copy {to}");
                applied.push(action.clone());
            }
            Err(e) if e.is_write_refusal() => repair_errors.push(format!(
                "could not write {to}: the backend refused the write: {e}. If the \
                 repository is append-only or read-only, remove {to} there manually or \
                 restore the key with `vykar key import`."
            )),
            Err(e) => repair_errors.push(format!("failed to restore key copy {to}: {e}")),
        }
    }
}

/// Execute repair actions in the correct order.
pub(super) fn execute_repair(
    repo: &mut Repository,
    plan: &RepairPlan,
    issues: &[IntegrityIssue],
    _pack_chunks: &HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>>,
) -> Result<(Vec<RepairAction>, Vec<String>)> {
    // Refuse to proceed if any snapshots had transient I/O failures during
    // the scan — we can't safely rebuild refcounts without enumerating every
    // surviving snapshot's chunks.
    for issue in issues {
        if let IntegrityIssue::SnapshotReadFailed {
            snapshot_name,
            detail,
        } = issue
        {
            return Err(VykarError::Other(format!(
                "aborting repair: snapshot '{snapshot_name}' had a transient \
                 read failure during scan ({detail}); retry when storage is stable"
            )));
        }
    }

    let mut applied: Vec<RepairAction> = Vec::new();
    let mut repair_errors: Vec<String> = Vec::new();

    // Step 0: Restore damaged or missing repository key copies. First, because
    // it is the one repair that only ever adds redundancy, and a failure here
    // must not be masked by whatever the snapshot passes do next.
    restore_key_copies(repo, plan, &mut applied, &mut repair_errors);

    // Step 1: Remove corrupted snapshot blobs from storage.
    // If any delete fails, abort before refcount rebuild — we cannot enumerate
    // the corrupt snapshot's chunks, so persisting rebuilt refcounts would drop
    // live references (matching delete.rs:46 "must succeed" pattern).
    for action in &plan.actions {
        if let RepairAction::RemoveCorruptSnapshot { snapshot_id, name } = action {
            let key = snapshot_id.storage_key();
            match repo.storage.delete(&key) {
                Ok(()) => {
                    // Remove from manifest if present
                    if let Some(name) = name {
                        repo.manifest_mut().remove_snapshot(name);
                    }
                    applied.push(action.clone());
                }
                Err(e) => {
                    return Err(VykarError::Other(format!(
                        "aborting repair: failed to remove corrupt snapshot \
                         {snapshot_id}: {e} (cannot safely rebuild refcounts)"
                    )));
                }
            }
        }
    }

    // Step 2: Remove invalid snapshot keys from storage.
    for action in &plan.actions {
        if let RepairAction::RemoveInvalidSnapshotKey { storage_key } = action {
            match repo.storage.delete(storage_key) {
                Ok(()) => {
                    applied.push(action.clone());
                }
                Err(e) => {
                    repair_errors.push(format!(
                        "failed to remove invalid snapshot key {storage_key}: {e}"
                    ));
                }
            }
        }
    }

    // Step 3: Remove dangling index entries (missing packs).
    for action in &plan.actions {
        if let RepairAction::RemoveDanglingIndexEntries { pack_id, .. } = action {
            let removed = repo.chunk_index_mut().remove_by_pack(pack_id);
            tracing::info!("removed {removed} dangling index entries for pack {pack_id}");
            applied.push(action.clone());
        }
    }

    // Step 4: Remove content-corrupted entries (if --verify-data).
    for action in &plan.actions {
        match action {
            RepairAction::RemoveCorruptPack { pack_id, .. } => {
                let removed = repo.chunk_index_mut().remove_by_pack(pack_id);
                tracing::info!("removed {removed} index entries for corrupt pack {pack_id}");
                applied.push(action.clone());
            }
            RepairAction::RemoveCorruptChunks { chunk_ids, .. } => {
                for chunk_id in chunk_ids {
                    repo.chunk_index_mut().remove(chunk_id);
                }
                applied.push(action.clone());
            }
            _ => {}
        }
    }

    // Step 4.5: Rewrite snapshots that survived item-level repair gates.
    // Mirrors `Repository::commit_concurrent_session_with_progress`:
    // begin_write_session → store_chunk per surviving item → flush_packs →
    // save_state (persists chunks BEFORE any snapshot blob references them).
    // Per-action errors are logged and skipped; the trailing rebuild
    // garbage-collects any orphan chunks. The publish loop deliberately
    // never short-circuits with `?`: a failed PUT/DELETE must not bypass
    // Step 5 (whole-snapshot deletes) or Step 6 (refcount rebuild).
    apply_item_level_rewrites(repo, plan, &mut applied, &mut repair_errors)?;

    // Step 5: Delete doomed snapshot blobs FIRST (safe ordering: delete
    // commit-point before adjusting refcounts, matching delete.rs:46-58).
    // Only successfully-deleted doomed snapshots are excluded from refcount
    // rebuild — if a delete fails, the snapshot's chunks must remain counted.
    let mut successfully_deleted_doomed: HashSet<String> = HashSet::new();
    for action in &plan.actions {
        if let RepairAction::RemoveDanglingSnapshot { snapshot_name, .. } = action {
            if let Some(entry) = repo.manifest().find_snapshot(snapshot_name) {
                let key = entry.id.storage_key();
                match repo.storage.delete(&key) {
                    Ok(()) => {
                        repo.manifest_mut().remove_snapshot(snapshot_name);
                        applied.push(action.clone());
                        successfully_deleted_doomed.insert(snapshot_name.clone());
                    }
                    Err(e) => {
                        repair_errors.push(format!(
                            "failed to remove doomed snapshot '{snapshot_name}': {e}"
                        ));
                    }
                }
            }
        }
    }

    // Step 6: Rebuild refcounts from all surviving snapshots (excludes only
    // snapshots whose blobs were actually deleted above).
    let doomed_names: HashSet<&str> = successfully_deleted_doomed
        .iter()
        .map(|s| s.as_str())
        .collect();

    let mut new_refcounts: HashMap<ChunkId, u32> = HashMap::new();
    let surviving_entries: Vec<_> = repo
        .manifest()
        .snapshots
        .iter()
        .filter(|e| !doomed_names.contains(e.name.as_str()))
        .cloned()
        .collect();

    for entry in &surviving_entries {
        let meta = match load_snapshot_meta(repo, &entry.name) {
            Ok(m) => m,
            Err(e) => {
                return Err(VykarError::Other(format!(
                    "aborting repair: cannot load snapshot '{}' during refcount \
                     rebuild: {e} (persisting would drop live references)",
                    entry.name
                )));
            }
        };

        // Count item_ptrs chunks
        for chunk_id in &meta.item_ptrs {
            if repo.chunk_index().contains(chunk_id) {
                *new_refcounts.entry(*chunk_id).or_insert(0) += 1;
            }
        }

        // Count file chunks
        let items_stream = match load_snapshot_item_stream(repo, &entry.name, None) {
            Ok(s) => s,
            Err(e) => {
                return Err(VykarError::Other(format!(
                    "aborting repair: cannot load item stream for '{}' during \
                     refcount rebuild: {e} (persisting would drop live references)",
                    entry.name
                )));
            }
        };
        if let Err(e) = for_each_decoded_item(&items_stream, |item| {
            if item.entry_type == ItemType::RegularFile {
                for chunk_ref in &item.chunks {
                    if repo.chunk_index().contains(&chunk_ref.id) {
                        *new_refcounts.entry(chunk_ref.id).or_insert(0) += 1;
                    }
                }
            }
            Ok(())
        }) {
            return Err(VykarError::Other(format!(
                "aborting repair: failed to decode items for '{}' during \
                 refcount rebuild: {e} (persisting would drop live references)",
                entry.name
            )));
        }
    }

    repo.chunk_index_mut().rebuild_refcounts(&new_refcounts);
    applied.push(RepairAction::RebuildRefcounts);

    // Step 7: Persist index (also rewrites index.gen).
    repo.save_state()?;

    Ok((applied, repair_errors))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::check::repair_plan::build_repair_plan;
    use crate::commands::check::scan::{integrity_scan, ScanOptions};
    use crate::config::ChunkerConfig;
    use crate::repo::{EncryptionMode, KEY_PRIMARY, KEY_SECONDARY};
    use crate::testutil::MemoryBackend;

    /// **Repair writes the verified bytes, not a fresh read of the source.**
    ///
    /// The scan established `from` as byte-identical to the key that
    /// unwrapped at open, but nothing holds that between scan and write. If
    /// the source copy changes in between, the copy being restored must still
    /// receive the key this repository opened with — and the changed source
    /// must be left alone, because nothing has established what it now holds.
    #[test]
    fn restore_key_copy_writes_the_opened_key_even_if_the_source_changed() {
        let mut repo = crate::repo::Repository::init(
            Box::new(MemoryBackend::new()),
            EncryptionMode::Aes256Gcm,
            ChunkerConfig::default(),
            Some("repair-writes-key-blob"),
            None,
            None,
        )
        .unwrap();
        let opened_with = repo.key_blob().unwrap().to_vec();
        repo.storage.put(KEY_SECONDARY, b"garbage").unwrap();

        let config = crate::tests::helpers::make_test_config(std::path::Path::new("/mem"));
        let opts = ScanOptions {
            collect_chunk_refs: true,
            detect_orphans: false,
            verify_data: false,
            skip_packs: None,
            snapshot_sample_percent: None,
        };
        let scan = integrity_scan(&mut repo, &config, &opts, &mut None, None).unwrap();
        let plan = build_repair_plan(&scan, &HashMap::new(), &HashMap::new());
        assert!(
            plan.actions.iter().any(|a| matches!(
                a,
                RepairAction::RestoreKeyCopy { from, to }
                    if from == KEY_PRIMARY && to == KEY_SECONDARY
            )),
            "{:?}",
            plan.actions
        );

        // Between scan and write, the source copy is replaced with a
        // different, well-formed blob.
        let swapped = rmp_serde::to_vec(
            &vykar_crypto::key::MasterKey::generate()
                .unwrap()
                .to_encrypted("someone-else")
                .unwrap(),
        )
        .unwrap();
        repo.storage.put(KEY_PRIMARY, &swapped).unwrap();

        let (applied, errors) =
            execute_repair(&mut repo, &plan, &scan.issues, &HashMap::new()).unwrap();
        assert!(errors.is_empty(), "{errors:?}");
        assert!(applied
            .iter()
            .any(|a| matches!(a, RepairAction::RestoreKeyCopy { .. })));

        assert_eq!(
            repo.storage.get(KEY_SECONDARY).unwrap().unwrap(),
            opened_with,
            "the restored copy must hold the key this repository opened with"
        );
        assert_eq!(
            repo.storage.get(KEY_PRIMARY).unwrap().unwrap(),
            swapped,
            "the changed source must not be touched"
        );
    }

    struct ForbiddenDeleteBackend;

    impl StorageBackend for ForbiddenDeleteBackend {
        fn get(&self, _key: &str) -> vykar_types::error::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn put(&self, _key: &str, _data: &[u8]) -> vykar_types::error::Result<()> {
            Ok(())
        }
        fn delete(&self, _key: &str) -> vykar_types::error::Result<()> {
            Err(VykarError::Other("403 Forbidden".into()))
        }
        fn exists(&self, _key: &str) -> vykar_types::error::Result<bool> {
            Ok(false)
        }
        fn list(&self, _prefix: &str) -> vykar_types::error::Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn get_range(
            &self,
            _key: &str,
            _offset: u64,
            _length: u64,
        ) -> vykar_types::error::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn create_dir(&self, _key: &str) -> vykar_types::error::Result<()> {
            Ok(())
        }

        vykar_storage::unsupported_server_ops!();
    }

    #[test]
    fn probe_deletes_allowed_ok_for_normal_backend() {
        let tmp = tempfile::tempdir().unwrap();
        let backend =
            vykar_storage::local_backend::LocalBackend::new(tmp.path().to_str().unwrap()).unwrap();
        assert!(probe_deletes_allowed(&backend));
    }

    #[test]
    fn probe_deletes_allowed_false_for_forbidden_backend() {
        let backend = ForbiddenDeleteBackend;
        assert!(!probe_deletes_allowed(&backend));
    }
}
