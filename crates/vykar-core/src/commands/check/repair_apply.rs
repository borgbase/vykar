use std::collections::{HashMap, HashSet};

use super::types::{IntegrityIssue, RepairAction, RepairPlan};
use crate::commands::list::{
    for_each_decoded_item, load_snapshot_item_stream_from_storage, load_snapshot_meta,
};
use crate::commands::mark::{
    MarkOptions, MissingRefs, TreeChunkSet, default_decode_threads, mark_live,
};
use crate::compress::Compression;
use crate::index::ChunkIndexEntry;
use crate::repo::control::CancelToken;
use crate::repo::format::{ObjectType, pack_object_with_context};
use crate::repo::manifest::SnapshotEntry;
use crate::repo::{Repo, RepoWriter, TreeChunkSource, WriteSession, WriteSessionOptions};
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::ItemType;
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
/// must not bypass the trailing whole-snapshot deletes or index sweep
/// in [`execute_repair`]. The only fatal case is "old DELETE failed AND
/// rollback DELETE also failed" — that returns the typed duplicate-blobs
/// error so the operator can manually intervene.
fn apply_item_level_rewrites(
    repo: &mut RepoWriter<'_>,
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
    //    Every return below consumes it: saved, or flushed on error.
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());

    // 2. Prepare each rewrite (defer all PUT/DELETE until after Step 4 of
    //    this routine, mirroring the backup commit's index write → snapshot
    //    PUT ordering).
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
        match prepare_rewrite(
            repo,
            &mut ws,
            *snapshot_id,
            snapshot_name,
            item_indices,
            action_idx,
        ) {
            Ok(rw) => prepared.push(rw),
            Err(e) => {
                repair_errors.push(format!(
                    "failed to prepare rewrite for snapshot '{snapshot_name}': {e}"
                ));
            }
        }
    }

    if prepared.is_empty() {
        // No work survived preparation — close the session via save_session
        // so any pending state is consistent. The trailing sweep removes any
        // orphan chunks store_chunk may have produced.
        if let Err(e) = ws.flush_packs(repo) {
            ws.flush_on_abort(repo);
            return Err(e);
        }
        repo.save_session(ws)?;
        return Ok(());
    }

    // 3. Flush new data/tree packs to storage.
    if let Err(e) = ws.flush_packs(repo) {
        ws.flush_on_abort(repo);
        return Err(e);
    }

    // 4. Persist the chunk index so newly-stored chunks are durable BEFORE
    //    any snapshot blob references them.
    repo.save_session(ws)?;

    // 5. Publish each rewrite: PUT new blob, swap manifest, DELETE old.
    for rw in &prepared {
        // 5a. PUT new snapshot blob (the new commit point).
        if let Err(e) = repo.storage().put(&rw.new_id.storage_key(), &rw.new_packed) {
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
        match repo.storage().delete(&rw.old_id.storage_key()) {
            Ok(()) => {
                applied.push(
                    plan.actions
                        .get(rw.action_idx)
                        .expect("rw.action_idx was assigned from enumerate(plan.actions)")
                        .clone(),
                );
            }
            Err(e) => {
                let new_delete = repo.storage().delete(&rw.new_id.storage_key());
                repo.manifest_mut().remove_snapshot(&rw.new_entry.name);
                if let Some(prev) = removed {
                    repo.manifest_mut().snapshots.push(prev);
                }

                if new_delete.is_err() {
                    return Err(VykarError::Other(format!(
                        "repair partially applied: snapshot '{}' has duplicate \
                         blobs at IDs {} (old) and {} (new); cannot delete either. \
                         Manual cleanup required, then re-run `vykar check --repair` \
                         to sweep the index.",
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
/// the resulting tree chunks (via the write session `ws`), and pack a
/// fresh `SnapshotMeta` blob for publication. Does not write the blob.
fn prepare_rewrite(
    repo: &mut Repo,
    ws: &mut WriteSession,
    snapshot_id: SnapshotId,
    snapshot_name: &str,
    item_indices: &[usize],
    action_idx: usize,
) -> Result<PreparedRewrite> {
    use crate::commands::backup::ListingWriter;

    let drop: HashSet<usize> = item_indices.iter().copied().collect();

    let old_meta: SnapshotMeta = load_snapshot_meta(repo, snapshot_name)?;
    let old_stream =
        load_snapshot_item_stream_from_storage(repo, snapshot_name, &CancelToken::new())?;

    let mut listing = ListingWriter::new(repo.crypto().chunk_hasher(), Compression::default());
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
        listing.push(repo, ws, &item)
    })?;

    if kept == 0 {
        return Err(VykarError::Other(format!(
            "rewrite would empty snapshot '{snapshot_name}' (planner gate bypassed)"
        )));
    }

    let listing = listing.finish(repo, ws)?;

    // Build new SnapshotMeta. Time fields and source labels mirror the
    // original so users still see when the data was captured.
    let mut new_meta = old_meta.clone();
    new_meta.item_ptrs = listing.item_ptrs;
    new_meta.stats.nfiles = nfiles;
    new_meta.stats.original_size = original_size;
    // Stamp the version the rewritten listing is encoded in. The rewriter
    // re-encodes every kept item, so a format-1 or legacy-0 snapshot comes
    // out as the current format.
    new_meta.format_version = listing.format_version;
    // Replace, never keep, the old key index: it names the old tree chunks.
    // Without segment starts the rewriter writes one implicit segment, so a
    // listing with dumps or several roots comes out as order 0 (scanned).
    new_meta.ext = listing.ext;
    // deduplicated_size: preserved from the old meta (display-only; minor
    // inflation is acceptable per #123).

    let new_id = SnapshotId::generate();
    let meta_bytes = rmp_serde::to_vec(&new_meta)?;
    let new_packed = pack_object_with_context(
        ObjectType::SnapshotMeta,
        new_id.as_bytes(),
        &meta_bytes,
        repo.crypto().as_ref(),
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
/// The bytes written are [`Repo::key_blob`], not a fresh read of the
/// source copy: the scan established `from` as byte-identical to it, but
/// nothing holds that between scan and write, and the verified bytes are
/// already in hand. `from` is kept on the action for the plan printout.
///
/// Creating a missing copy succeeds even on an append-only server, which
/// blocks overwrites but not creations. *Replacing* a divergent copy is an
/// overwrite and will be refused there.
fn restore_key_copies(
    repo: &mut Repo,
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
        match repo.storage().put(to, bytes) {
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
    repo: &mut RepoWriter<'_>,
    plan: &RepairPlan,
    issues: &[IntegrityIssue],
    _pack_chunks: &HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>>,
) -> Result<(Vec<RepairAction>, Vec<String>)> {
    // Refuse to proceed if any snapshots had transient I/O failures during
    // the scan — we can't safely sweep the index without enumerating every
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
    // If any delete fails, abort before the index sweep — we cannot enumerate
    // the corrupt snapshot's chunks, so persisting a sweep would drop live
    // references (matching delete.rs:46 "must succeed" pattern).
    for action in &plan.actions {
        if let RepairAction::RemoveCorruptSnapshot { snapshot_id, name } = action {
            let key = snapshot_id.storage_key();
            match repo.storage().delete(&key) {
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
                         {snapshot_id}: {e} (cannot safely sweep the index)"
                    )));
                }
            }
        }
    }

    // Step 2: Remove invalid snapshot keys from storage.
    for action in &plan.actions {
        if let RepairAction::RemoveInvalidSnapshotKey { storage_key } = action {
            match repo.storage().delete(storage_key) {
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
    // Mirrors the backup commit (`repo::commit_machine::BackupCommit`):
    // begin_write_session → store_chunk per surviving item → flush_packs →
    // save_state (persists chunks BEFORE any snapshot blob references them).
    // Per-action errors are logged and skipped; the trailing sweep
    // garbage-collects any orphan chunks. The publish loop deliberately
    // never short-circuits with `?`: a failed PUT/DELETE must not bypass
    // Step 5 (whole-snapshot deletes) or Step 6 (the index sweep).
    apply_item_level_rewrites(repo, plan, &mut applied, &mut repair_errors)?;

    // Step 5: Delete doomed snapshot blobs FIRST (safe ordering: delete
    // commit-point before sweeping the index, matching delete.rs:46-58).
    // Only successfully-deleted doomed snapshots are excluded from the
    // sweep's mark — if a delete fails, the snapshot's chunks must stay.
    let mut successfully_deleted_doomed: HashSet<String> = HashSet::new();
    for action in &plan.actions {
        if let RepairAction::RemoveDanglingSnapshot { snapshot_name, .. } = action
            && let Some(entry) = repo.manifest().find_snapshot(snapshot_name)
        {
            let key = entry.id.storage_key();
            match repo.storage().delete(&key) {
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

    // Step 6: Sweep the index: mark from all surviving snapshots (excludes
    // only snapshots whose blobs were actually deleted above) and remove
    // every entry the mark does not reach.
    let doomed_names: HashSet<&str> = successfully_deleted_doomed
        .iter()
        .map(|s| s.as_str())
        .collect();

    let surviving_entries: Vec<_> = repo
        .manifest()
        .snapshots
        .iter()
        .filter(|e| !doomed_names.contains(e.name.as_str()))
        .cloned()
        .collect();

    // Every surviving snapshot must be marked: a snapshot left out would
    // have its references dropped by the sweep.
    let mut names: Vec<String> = Vec::with_capacity(surviving_entries.len());
    let mut ptr_lists: Vec<Vec<ChunkId>> = Vec::with_capacity(surviving_entries.len());
    for entry in &surviving_entries {
        let meta = match load_snapshot_meta(repo, &entry.name) {
            Ok(m) => m,
            Err(e) => {
                return Err(VykarError::Other(format!(
                    "aborting repair: cannot load snapshot '{}' while sweeping \
                     the index: {e} (sweeping would drop live references)",
                    entry.name
                )));
            }
        };
        names.push(entry.name.clone());
        ptr_lists.push(meta.item_ptrs);
    }

    // Decoded once per distinct tree chunk. Tree chunks come from storage,
    // never the local cache. A data reference missing from the index is
    // skipped; a tree pointer missing from the index or unreadable aborts.
    let unreferenced: Vec<ChunkId> = {
        let ptrs: Vec<&[ChunkId]> = ptr_lists.iter().map(Vec::as_slice).collect();
        let set = TreeChunkSet::build(&ptrs);
        let positions = repo.chunk_index().positions();
        let found = mark_live(
            repo,
            &set,
            &names,
            &positions,
            TreeChunkSource::Storage,
            MarkOptions {
                count: false,
                missing_refs: MissingRefs::Ignore,
                decode_threads: default_decode_threads(),
            },
            &CancelToken::new(),
        )
        .map_err(|e| {
            VykarError::Other(format!(
                "aborting repair: cannot mark the surviving snapshots: {e} \
                 (sweeping would drop live references)"
            ))
        })?;
        let mut doomed = Vec::new();
        positions.for_each_live(|p, id, _| {
            if !found.is_marked(p) {
                doomed.push(id);
            }
        });
        doomed
    };

    let index = repo.chunk_index_mut();
    for id in &unreferenced {
        index.remove(id);
    }
    tracing::info!(
        "removed {} index entries no surviving snapshot references",
        unreferenced.len()
    );
    applied.push(RepairAction::RemoveUnreferencedEntries);

    // Step 7: Persist index. This is the one write that folds the index
    // into a new base; earlier persists stay segments. Its head records
    // the repair as a sweep (`sweep.gen`), keeping the last sweep record.
    // Written also when the sweep removed nothing.
    repo.mark_index_dirty();
    repo.request_full_index_fold();
    repo.set_pending_sweep(crate::repo::index_write::PendingSweep::Repair);
    repo.save_state()?;

    // Step 8: pack maps for packs whose only description is the repaired
    // index. A pack the repair removed from the index has no entries left
    // and gets no map. A failure is a repair error: the index is written.
    for action in &plan.actions {
        if let RepairAction::WritePackMaps { pack_ids } = action {
            let only: HashSet<PackId> = pack_ids.iter().copied().collect();
            match repo.write_pack_maps(Some(&only), &CancelToken::new()) {
                Ok(stats) => {
                    tracing::info!(
                        maps = stats.maps_written,
                        packs = stats.packs_mapped,
                        "wrote pack maps"
                    );
                    applied.push(action.clone());
                }
                Err(e) => repair_errors.push(format!("failed to write pack maps: {e}")),
            }
        }
    }

    Ok((applied, repair_errors))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::check::repair_plan::build_repair_plan;
    use crate::commands::check::scan::{ScanOptions, integrity_scan};
    use crate::repo::ChunkerParams;
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
        let mut repo = crate::repo::Repo::init(
            Box::new(MemoryBackend::new()),
            EncryptionMode::Aes256Gcm,
            ChunkerParams::default(),
            Some("repair-writes-key-blob"),
            None,
            None,
        )
        .unwrap();
        let opened_with = repo.key_blob().unwrap().to_vec();
        repo.storage().put(KEY_SECONDARY, b"garbage").unwrap();

        let config = crate::tests::helpers::make_test_config(std::path::Path::new("/mem"));
        let opts = ScanOptions {
            collect_chunk_refs: true,
            detect_orphans: false,
            verify_data: false,
            skip_packs: None,
            snapshot_sample: None,
        };
        let scan =
            integrity_scan(&mut repo, &config, &opts, &mut None, &CancelToken::new()).unwrap();
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
        repo.storage().put(KEY_PRIMARY, &swapped).unwrap();

        let (applied, errors) = execute_repair(
            &mut repo.unchecked_writer(),
            &plan,
            &scan.issues,
            &HashMap::new(),
        )
        .unwrap();
        assert!(errors.is_empty(), "{errors:?}");
        assert!(
            applied
                .iter()
                .any(|a| matches!(a, RepairAction::RestoreKeyCopy { .. }))
        );

        assert_eq!(
            repo.storage().get(KEY_SECONDARY).unwrap().unwrap(),
            opened_with,
            "the restored copy must hold the key this repository opened with"
        );
        assert_eq!(
            repo.storage().get(KEY_PRIMARY).unwrap().unwrap(),
            swapped,
            "the changed source must not be touched"
        );
    }

    /// REP-2: a surviving snapshot that cannot be loaded when the index is
    /// swept aborts the repair before any index write.
    #[test]
    fn repair_aborts_when_a_surviving_snapshot_cannot_be_loaded() {
        use crate::tests::helpers::{backup_single_source, init_repo, open_local_repo};

        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        let src = tmp.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"alpha").unwrap();
        let config = init_repo(&repo_dir);
        backup_single_source(&config, &src, "src", "s1");

        let mut repo = open_local_repo(&repo_dir);
        let opts = ScanOptions {
            collect_chunk_refs: true,
            detect_orphans: false,
            verify_data: false,
            skip_packs: None,
            snapshot_sample: None,
        };
        let scan =
            integrity_scan(&mut repo, &config, &opts, &mut None, &CancelToken::new()).unwrap();
        let plan = build_repair_plan(&scan, &HashMap::new(), &HashMap::new());
        assert!(
            matches!(
                plan.actions.as_slice(),
                [RepairAction::RemoveUnreferencedEntries]
            ),
            "{:?}",
            plan.actions
        );
        let heads =
            crate::repo::index_store::list_head_generations(repo.storage().as_ref()).unwrap();

        // Between scan and apply, the snapshot object goes away.
        let id = repo.manifest().find_snapshot("s1").unwrap().id;
        repo.storage().delete(&id.storage_key()).unwrap();

        let err = execute_repair(
            &mut repo.unchecked_writer(),
            &plan,
            &scan.issues,
            &HashMap::new(),
        )
        .expect_err("the repair aborts");
        let msg = err.to_string();
        assert!(
            msg.contains("cannot load snapshot 's1' while sweeping the index"),
            "{msg}"
        );
        assert_eq!(
            crate::repo::index_store::list_head_generations(repo.storage().as_ref()).unwrap(),
            heads,
            "no head written"
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

        fn profile(&self) -> vykar_storage::BackendProfile {
            vykar_storage::BackendProfile {
                local: false,
                staged_put: false,
                request_width: 1,
                transfer_width: 1,
            }
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

    /// The listing rewriter cuts tree chunks with the same rule as backup.
    ///
    /// The cut key derives from the fresh repository's chunk-ID key, so cut
    /// positions differ between runs. The bounds have margin; a failure should
    /// be investigated, not fixed by loosening a bound.
    #[test]
    fn repair_rewrite_cuts_like_backup() {
        use crate::repo::format::unpack_object_expect_with_context;
        use crate::tests::helpers::{backup_single_source, init_repo, open_local_repo};
        use crate::tests::tree_cut::make_tree_cut_source;

        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        let source_dir = tmp.path().join("source");
        std::fs::create_dir_all(&repo_dir).unwrap();
        let mut config = init_repo(&repo_dir);
        config.xattrs.enabled = false;
        make_tree_cut_source(&source_dir);
        backup_single_source(&config, &source_dir, "src", "snap1");

        let mut repo = open_local_repo(&repo_dir);
        let old_meta = load_snapshot_meta(&repo, "snap1").unwrap();
        assert!(
            old_meta.item_ptrs.len() >= 8,
            "only {} tree chunks",
            old_meta.item_ptrs.len()
        );
        let mut nitems = 0usize;
        let stream =
            load_snapshot_item_stream_from_storage(&mut repo, "snap1", &CancelToken::new())
                .unwrap();
        for_each_decoded_item(&stream, |_| {
            nitems += 1;
            Ok(())
        })
        .unwrap();

        let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
        let id = repo.manifest().find_snapshot("snap1").unwrap().id;
        let mut rewrite = |drop: &[usize]| -> SnapshotMeta {
            let rw = prepare_rewrite(&mut repo, &mut ws, id, "snap1", drop, 0).unwrap();
            let bytes = unpack_object_expect_with_context(
                &rw.new_packed,
                ObjectType::SnapshotMeta,
                rw.new_id.as_bytes(),
                repo.crypto().as_ref(),
            )
            .unwrap();
            rmp_serde::from_slice(&bytes).unwrap()
        };

        let same = rewrite(&[]);
        assert_eq!(same.item_ptrs, old_meta.item_ptrs);
        // One implicit segment, as backup wrote it: the same index.
        assert_eq!(same.ext, old_meta.ext);
        // The rewriter stamps the format its listing is encoded in.
        assert_eq!(same.format_version, crate::snapshot::CURRENT_FORMAT_VERSION);

        let dropped = rewrite(&[nitems / 2]);
        let old: HashSet<&ChunkId> = old_meta.item_ptrs.iter().collect();
        let new = dropped
            .item_ptrs
            .iter()
            .filter(|id| !old.contains(id))
            .count();
        assert!(
            (1..=3).contains(&new),
            "{new} of {} tree chunks new after dropping one item",
            dropped.item_ptrs.len()
        );
        crate::snapshot::listing::decode_ext(dropped.ext.as_deref(), dropped.item_ptrs.len())
            .unwrap();
    }

    /// The rewriter replaces the key index of a snapshot with dumps and two
    /// roots: it writes no segment starts, so the new index is order 0
    /// (readers scan), never the old index naming the old chunks.
    #[cfg(unix)]
    #[test]
    fn repair_rewrite_replaces_the_key_index() {
        use crate::config::CommandDump;
        use crate::repo::format::unpack_object_expect_with_context;
        use crate::snapshot::listing::{NoIndex, decode_ext};
        use crate::tests::helpers::{init_repo, open_local_repo};

        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        let config = init_repo(&repo_dir);
        let roots = [tmp.path().join("b"), tmp.path().join("a")];
        for r in &roots {
            std::fs::create_dir_all(r.join("d")).unwrap();
            for f in 0..20 {
                std::fs::write(r.join(format!("d/f{f}")), "x").unwrap();
            }
        }
        let paths: Vec<String> = roots
            .iter()
            .map(|r| r.to_string_lossy().to_string())
            .collect();
        let dumps = [CommandDump {
            name: "dump".to_string(),
            command: "echo dump".to_string(),
        }];
        crate::commands::backup::run(
            &config,
            crate::commands::backup::BackupRequest {
                snapshot_name: "snap1",
                passphrase: None,
                source_paths: &paths,
                source_label: "src",
                exclude_patterns: &[],
                exclude_if_present: &[],
                one_file_system: true,
                git_ignore: false,
                xattrs_enabled: false,
                compression: Compression::None,
                command_dumps: &dumps,
                verbose: false,
            },
        )
        .unwrap();

        let mut repo = open_local_repo(&repo_dir);
        let old_meta = load_snapshot_meta(&repo, "snap1").unwrap();
        let old_index = decode_ext(old_meta.ext.as_deref(), old_meta.item_ptrs.len()).unwrap();
        assert_eq!(old_index.segments.len(), 3);

        let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
        let id = repo.manifest().find_snapshot("snap1").unwrap().id;
        let rw = prepare_rewrite(&mut repo, &mut ws, id, "snap1", &[5], 0).unwrap();
        let bytes = unpack_object_expect_with_context(
            &rw.new_packed,
            ObjectType::SnapshotMeta,
            rw.new_id.as_bytes(),
            repo.crypto().as_ref(),
        )
        .unwrap();
        let new_meta: SnapshotMeta = rmp_serde::from_slice(&bytes).unwrap();
        assert_ne!(new_meta.ext, old_meta.ext);
        assert_eq!(
            decode_ext(new_meta.ext.as_deref(), new_meta.item_ptrs.len()),
            Err(NoIndex::Unordered)
        );
    }
}
