use std::collections::{HashMap, HashSet};

use super::scan::ScanResult;
use super::types::{IndexRecovery, IntegrityIssue, RepairAction, RepairPlan};
use crate::commands::list::{
    for_each_decoded_item, load_item_stream_of_meta, load_snapshot_meta_of,
};
use crate::index::ChunkIndexEntry;
use crate::repo::Repo;
use crate::repo::control::CancelToken;
use crate::repo::manifest::SnapshotEntry;
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::Item;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

/// Build a repair plan from the detected integrity issues.
///
/// `snapshot_chunk_refs` maps each snapshot name to the set of chunk IDs it
/// references (both `item_ptrs` and file-level chunks). This allows the plan
/// to predict which snapshots become "doomed" after index entries are removed.
///
/// Item-level repair (`DropItemsFromSnapshot`) is emitted only when all
/// coverage gates in [`item_repair_gates`] are satisfied; otherwise the
/// affected snapshot falls back to whole-snapshot removal via the existing
/// `RemoveCorruptSnapshot` / `RemoveDanglingSnapshot` paths.
pub(super) fn build_repair_plan(
    scan: &ScanResult,
    pack_chunks: &HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>>,
    name_to_id: &HashMap<String, SnapshotId>,
) -> RepairPlan {
    use std::collections::BTreeMap;

    let mut actions: Vec<RepairAction> = Vec::new();
    let mut has_data_loss = false;

    // Repository key copies first. Restoring one is pure gain — `check` runs
    // only after `Repo::open` succeeded, so a key was established and
    // `key_source` names the stored copy that carries it. Repair therefore
    // never picks between two candidate keys on a guess; the unresolvable case
    // fails at open and never reaches a plan.
    if let Some(ref from) = scan.key_source {
        for to in &scan.key_copies_to_restore {
            actions.push(RepairAction::RestoreKeyCopy {
                from: from.clone(),
                to: to.clone(),
            });
        }
    }

    // ------------------------------------------------------------------
    // Pre-pass: emit InvalidSnapshotKey actions and collect ids of
    // snapshots that *must* be removed wholesale.
    // ------------------------------------------------------------------
    let mut corrupt_snapshot_meta: HashMap<SnapshotId, Option<String>> = HashMap::new();
    let mut per_snapshot_whole: HashSet<SnapshotId> = HashSet::new();
    for issue in &scan.issues {
        match issue {
            IntegrityIssue::CorruptSnapshot {
                snapshot_id,
                snapshot_name,
            } => {
                corrupt_snapshot_meta
                    .entry(*snapshot_id)
                    .or_insert_with(|| snapshot_name.clone());
                per_snapshot_whole.insert(*snapshot_id);
            }
            IntegrityIssue::DanglingItemPtr { snapshot_name, .. }
            | IntegrityIssue::UnreadableSnapshot { snapshot_name, .. }
            | IntegrityIssue::SnapshotReadFailed { snapshot_name, .. } => {
                if let Some(id) = name_to_id.get(snapshot_name) {
                    per_snapshot_whole.insert(*id);
                }
                // If the name isn't resolvable (manifest no longer carries
                // it), the snapshot is already gone from the manifest's
                // perspective — the existing chunk-removal path below still
                // emits RemoveDanglingSnapshot for it.
            }
            IntegrityIssue::InvalidSnapshotKey { storage_key } => {
                actions.push(RepairAction::RemoveInvalidSnapshotKey {
                    storage_key: storage_key.clone(),
                });
                has_data_loss = true;
            }
            _ => {}
        }
    }

    // ------------------------------------------------------------------
    // Collect candidate item-level drops, keyed by snapshot id, sorted by
    // item ordinal (BTreeMap → deterministic order in the action).
    // ------------------------------------------------------------------
    let mut per_snapshot_drops: HashMap<SnapshotId, BTreeMap<usize, (String, String)>> =
        HashMap::new();

    for impact in &scan.item_impacts {
        let mut packs: Vec<PackId> = impact.affected_chunks.iter().map(|(_, p)| *p).collect();
        packs.sort_by_key(|a| *a.as_bytes());
        packs.dedup();
        let reason = if let [only] = packs.as_slice() {
            format!("chunks in missing pack {only}")
        } else {
            let list = packs
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!("chunks in missing packs {list}")
        };
        per_snapshot_drops
            .entry(impact.snapshot_id)
            .or_default()
            .insert(impact.item_index, (impact.item_path.clone(), reason));
    }

    for issue in &scan.issues {
        match issue {
            IntegrityIssue::InvalidItem {
                snapshot_id,
                item_index,
                item_path,
                reason,
                ..
            } => {
                per_snapshot_drops.entry(*snapshot_id).or_default().insert(
                    *item_index,
                    (item_path.clone(), format!("invalid item: {reason}")),
                );
            }
            IntegrityIssue::DanglingFileChunk {
                snapshot_name,
                item_index,
                path,
                chunk_id,
                ..
            } => {
                if let Some(id) = name_to_id.get(snapshot_name) {
                    per_snapshot_drops.entry(*id).or_default().insert(
                        *item_index,
                        (path.clone(), format!("missing chunk {chunk_id}")),
                    );
                }
                // If unresolvable, the snapshot's been mutated underneath us;
                // chunks-to-remove path will doom it.
            }
            _ => {}
        }
    }

    // ------------------------------------------------------------------
    // Missing-pack and corrupt-pack actions (drives the chunks-to-remove
    // set used by the planner's surviving-items gate and by the existing
    // RemoveDanglingSnapshot path).
    // ------------------------------------------------------------------
    let mut missing_packs: HashSet<PackId> = HashSet::new();
    for issue in &scan.issues {
        if let IntegrityIssue::MissingPack { pack_id } = issue {
            missing_packs.insert(*pack_id);
        }
    }
    for pack_id in &missing_packs {
        let chunk_count = pack_chunks.get(pack_id).map_or(0, |c| c.len());
        actions.push(RepairAction::RemoveDanglingIndexEntries {
            pack_id: *pack_id,
            chunk_count,
        });
        has_data_loss = true;
    }

    let mut corrupt_packs: HashSet<PackId> = HashSet::new();
    let mut corrupt_chunks_by_pack: HashMap<PackId, Vec<ChunkId>> = HashMap::new();
    for issue in &scan.issues {
        match issue {
            IntegrityIssue::CorruptPackContent { pack_id, .. } => {
                corrupt_packs.insert(*pack_id);
            }
            IntegrityIssue::CorruptChunk {
                chunk_id, pack_id, ..
            } if !corrupt_packs.contains(pack_id) && !missing_packs.contains(pack_id) => {
                corrupt_chunks_by_pack
                    .entry(*pack_id)
                    .or_default()
                    .push(*chunk_id);
            }
            _ => {}
        }
    }
    for pack_id in &corrupt_packs {
        if missing_packs.contains(pack_id) {
            continue;
        }
        let chunk_count = pack_chunks.get(pack_id).map_or(0, |c| c.len());
        actions.push(RepairAction::RemoveCorruptPack {
            pack_id: *pack_id,
            chunk_count,
        });
        has_data_loss = true;
    }
    for (pack_id, chunk_ids) in &corrupt_chunks_by_pack {
        actions.push(RepairAction::RemoveCorruptChunks {
            pack_id: *pack_id,
            chunk_ids: chunk_ids.clone(),
        });
        has_data_loss = true;
    }

    let mut chunks_to_remove: HashSet<ChunkId> = HashSet::new();
    for pack_id in missing_packs.iter().chain(corrupt_packs.iter()) {
        if let Some(chunks) = pack_chunks.get(pack_id) {
            for (chunk_id, _) in chunks {
                chunks_to_remove.insert(*chunk_id);
            }
        }
    }
    for chunk_ids in corrupt_chunks_by_pack.values() {
        for chunk_id in chunk_ids {
            chunks_to_remove.insert(*chunk_id);
        }
    }

    // ------------------------------------------------------------------
    // Coverage check: for each snapshot with item-level candidates, prove
    // that dropping just those items is safe. Any failure → promote to
    // per_snapshot_whole (whole-snapshot removal).
    // ------------------------------------------------------------------
    let drop_ids: Vec<SnapshotId> = per_snapshot_drops.keys().copied().collect();
    for id in drop_ids {
        if per_snapshot_whole.contains(&id) {
            continue;
        }
        let drops = per_snapshot_drops
            .get(&id)
            .expect("id came from per_snapshot_drops.keys()");

        // Gate 0: data-presence.
        let count = match scan.snapshot_item_counts.get(&id) {
            Some(c) => *c,
            None => {
                per_snapshot_whole.insert(id);
                continue;
            }
        };
        let per_item = match scan.snapshot_per_item_chunks.get(&id) {
            Some(v) if v.len() == count => v,
            _ => {
                per_snapshot_whole.insert(id);
                continue;
            }
        };
        if drops.keys().any(|idx| *idx >= count) {
            per_snapshot_whole.insert(id);
            continue;
        }

        // Gate 1: item-count (refuse drops that empty the snapshot).
        if drops.len() >= count {
            per_snapshot_whole.insert(id);
            continue;
        }

        // Gate 2: item-ptrs disjointness.
        let item_ptrs_disjoint = match scan.snapshot_item_ptrs.get(&id) {
            Some(s) => s.is_disjoint(&chunks_to_remove),
            None => false,
        };
        if !item_ptrs_disjoint {
            per_snapshot_whole.insert(id);
            continue;
        }

        // Gate 3: surviving items must not reference any removed chunk.
        let drop_set: HashSet<usize> = drops.keys().copied().collect();
        let mut surviving_intersects = false;
        for (idx, item_chunks) in per_item.iter().enumerate() {
            if drop_set.contains(&idx) {
                continue;
            }
            if !item_chunks.is_disjoint(&chunks_to_remove) {
                surviving_intersects = true;
                break;
            }
        }
        if surviving_intersects {
            per_snapshot_whole.insert(id);
            continue;
        }

        // Sanity: dropped chunks include intersection with chunks_to_remove
        // for this snapshot. Cheap debug-only check.
        debug_assert!({
            let mut dropped_chunks: HashSet<ChunkId> = HashSet::new();
            for idx in drops.keys() {
                if let Some(s) = per_item.get(*idx) {
                    dropped_chunks.extend(s.iter().copied());
                }
            }
            // surviving disjoint already verified — we don't strictly need
            // dropped to include all impacted chunks of *this* snapshot,
            // but for a sanity check: if the snapshot's full ref-set
            // intersects chunks_to_remove, that intersection must be
            // entirely covered by dropped_chunks plus item_ptrs (which we
            // proved disjoint above). So intersect with chunks_to_remove
            // and ensure dropped_chunks ⊇ that intersection.
            let mut all_file_chunks: HashSet<ChunkId> = HashSet::new();
            for s in per_item.iter() {
                all_file_chunks.extend(s.iter().copied());
            }
            let needed: HashSet<ChunkId> = all_file_chunks
                .intersection(&chunks_to_remove)
                .copied()
                .collect();
            needed.is_subset(&dropped_chunks)
        });
    }

    // ------------------------------------------------------------------
    // Emit DropItemsFromSnapshot for snapshots that survived all gates.
    // Names for these are looked up via the manifest's name→id map (the
    // planner is given the surviving subset, so each id resolves).
    // ------------------------------------------------------------------
    let id_to_name: HashMap<SnapshotId, String> = name_to_id
        .iter()
        .map(|(name, id)| (*id, name.clone()))
        .collect();

    let mut item_repaired_names: HashSet<String> = HashSet::new();
    let mut item_repaired_ids: HashSet<SnapshotId> = HashSet::new();
    for (id, drops) in &per_snapshot_drops {
        if per_snapshot_whole.contains(id) {
            continue;
        }
        let snapshot_name = match id_to_name.get(id) {
            Some(n) => n.clone(),
            None => continue, // belt-and-braces: name not in manifest
        };
        let item_indices: Vec<usize> = drops.keys().copied().collect();
        let dropped_paths: Vec<String> = drops.values().map(|(p, _)| p.clone()).collect();
        let reasons: Vec<String> = drops.values().map(|(_, r)| r.clone()).collect();
        actions.push(RepairAction::DropItemsFromSnapshot {
            snapshot_id: *id,
            snapshot_name: snapshot_name.clone(),
            item_indices,
            dropped_paths,
            reasons,
        });
        item_repaired_names.insert(snapshot_name);
        item_repaired_ids.insert(*id);
        has_data_loss = true;
    }

    // ------------------------------------------------------------------
    // Emit RemoveCorruptSnapshot for ids in per_snapshot_whole that came
    // from a CorruptSnapshot, OR from an InvalidItem that did not pass the
    // coverage gates. Snapshots whose whole-snapshot signal is only
    // `DanglingItemPtr` / `UnreadableSnapshot` / `SnapshotReadFailed` are
    // covered by the dangling-snapshot path further down — emitting a
    // RemoveCorruptSnapshot for those would double-act.
    // ------------------------------------------------------------------
    let mut corrupt_snapshot_names: HashSet<String> = HashSet::new();
    let mut emitted_corrupt: HashSet<SnapshotId> = HashSet::new();

    for (id, name) in &corrupt_snapshot_meta {
        actions.push(RepairAction::RemoveCorruptSnapshot {
            snapshot_id: *id,
            name: name.clone(),
        });
        if let Some(n) = name {
            corrupt_snapshot_names.insert(n.clone());
        }
        emitted_corrupt.insert(*id);
        has_data_loss = true;
    }

    for id in &per_snapshot_whole {
        if emitted_corrupt.contains(id) {
            continue;
        }
        let invalid_item_name = scan.issues.iter().find_map(|i| match i {
            IntegrityIssue::InvalidItem {
                snapshot_id,
                snapshot_name,
                ..
            } if *snapshot_id == *id => Some(snapshot_name.clone()),
            _ => None,
        });
        // Only promote-to-corrupt for snapshots whose only signal was an
        // InvalidItem. The DanglingItemPtr/Unreadable/Read paths already
        // emit RemoveDanglingSnapshot via the doomed_missing map below.
        let has_other_whole = scan.issues.iter().any(|i| {
            matches!(
                i,
                IntegrityIssue::DanglingItemPtr { snapshot_name, .. }
                | IntegrityIssue::UnreadableSnapshot { snapshot_name, .. }
                | IntegrityIssue::SnapshotReadFailed { snapshot_name, .. }
                    if name_to_id.get(snapshot_name) == Some(id)
            )
        });
        if let Some(name_opt) = invalid_item_name
            && !has_other_whole
        {
            actions.push(RepairAction::RemoveCorruptSnapshot {
                snapshot_id: *id,
                name: name_opt.clone(),
            });
            if let Some(n) = name_opt {
                corrupt_snapshot_names.insert(n);
            }
            emitted_corrupt.insert(*id);
            has_data_loss = true;
        }
    }

    // ------------------------------------------------------------------
    // RemoveDanglingSnapshot (existing logic, but suppress when an item-
    // level repair has been emitted for the same snapshot OR the snapshot
    // is going to be removed via RemoveCorruptSnapshot).
    // ------------------------------------------------------------------
    let mut doomed_missing: HashMap<String, usize> = HashMap::new();
    for issue in &scan.issues {
        match issue {
            IntegrityIssue::DanglingItemPtr { snapshot_name, .. } => {
                *doomed_missing.entry(snapshot_name.clone()).or_insert(0) += 1;
            }
            IntegrityIssue::DanglingFileChunk { snapshot_name, .. } => {
                // If item-level repair will cover this snapshot, skip:
                // the rewritten snapshot drops the offending item.
                if name_to_id
                    .get(snapshot_name)
                    .is_some_and(|id| item_repaired_ids.contains(id))
                {
                    continue;
                }
                *doomed_missing.entry(snapshot_name.clone()).or_insert(0) += 1;
            }
            IntegrityIssue::UnreadableSnapshot { snapshot_name, .. } => {
                doomed_missing.entry(snapshot_name.clone()).or_insert(1);
            }
            _ => {}
        }
    }

    if !chunks_to_remove.is_empty() {
        for (snap_name, chunk_ids) in &scan.snapshot_chunk_refs {
            if corrupt_snapshot_names.contains(snap_name) || item_repaired_names.contains(snap_name)
            {
                continue;
            }
            let newly_missing = chunk_ids
                .iter()
                .filter(|cid| chunks_to_remove.contains(cid))
                .count();
            if newly_missing > 0 {
                *doomed_missing.entry(snap_name.clone()).or_insert(0) += newly_missing;
            }
        }
    }

    for (snap_name, missing_count) in &doomed_missing {
        if corrupt_snapshot_names.contains(snap_name) || item_repaired_names.contains(snap_name) {
            continue;
        }
        actions.push(RepairAction::RemoveDanglingSnapshot {
            snapshot_name: snap_name.clone(),
            missing_chunks: *missing_count,
        });
        has_data_loss = true;
    }

    // Pack maps for packs whose trailer cannot be trusted, and for packs
    // that lose chunks, so a later rebuild of the index takes the entries the
    // repaired index keeps (a map takes precedence over the trailer).
    let mut map_packs: Vec<PackId> = scan
        .issues
        .iter()
        .filter_map(|i| match i {
            IntegrityIssue::CorruptPackTrailer { pack_id, .. }
                if !missing_packs.contains(pack_id) && !corrupt_packs.contains(pack_id) =>
            {
                Some(*pack_id)
            }
            _ => None,
        })
        .chain(corrupt_chunks_by_pack.keys().copied())
        .collect();
    add_pack_maps(&mut actions, &mut map_packs);

    // Always end with the sweep of the index.
    actions.push(RepairAction::RemoveUnreferencedEntries);

    RepairPlan {
        actions,
        has_data_loss,
        index_recovery: IndexRecovery::Complete,
        uncovered_packs: Vec::new(),
    }
}

/// Add `packs` to the plan's one `WritePackMaps` action (sorted, without
/// duplicates), creating it before `RemoveUnreferencedEntries` (or at the end) when
/// there is none. Nothing is added for an empty `packs`.
pub(super) fn add_pack_maps(actions: &mut Vec<RepairAction>, packs: &mut Vec<PackId>) {
    if packs.is_empty() {
        return;
    }
    let existing = actions.iter_mut().find_map(|a| match a {
        RepairAction::WritePackMaps { pack_ids } => Some(pack_ids),
        _ => None,
    });
    let pack_ids = match existing {
        Some(pack_ids) => pack_ids,
        None => {
            let at = actions
                .iter()
                .position(|a| matches!(a, RepairAction::RemoveUnreferencedEntries))
                .unwrap_or(actions.len());
            actions.insert(
                at,
                RepairAction::WritePackMaps {
                    pack_ids: Vec::new(),
                },
            );
            match actions.get_mut(at) {
                Some(RepairAction::WritePackMaps { pack_ids }) => pack_ids,
                _ => return,
            }
        }
    };
    pack_ids.append(packs);
    pack_ids.sort_by_key(|p| *p.as_bytes());
    pack_ids.dedup();
}

/// Reconcile the snapshot names more than one object carries, before the
/// scan. The scan, the plan and the repair look snapshots up by name, so
/// with two objects of one name they would check one object twice and act
/// on the other (an interrupted rewrite's rerun deleted the blob of one
/// entry and kept that entry, then failed its sweep with "snapshot not
/// found").
///
/// The one such pair repair recognises is its own: an item-level rewrite
/// (`DropItemsFromSnapshot`) PUTs the rewritten object, then DELETEs the
/// original, and a run interrupted between the two leaves both. The rewrite
/// copies every field of the original's metadata but the listing ones
/// (`item_ptrs`, `stats`, `ext`, `format_version`) and keeps the original's
/// items, in order, but the dropped ones. So when two objects of one name
/// agree on those fields and one's items are the other's with some left out
/// (or all kept), the one with fewer is removed from `repo`'s manifest and
/// planned for deletion ([`RepairAction::RemoveInterruptedRewrite`]): every
/// item it has the kept one has too, so the choice cannot lose data,
/// whichever object is which. The scan then sees the original alone and
/// plans its rewrite again; equal objects keep the one listed first.
///
/// Any other name carried by more than one object refuses the repair,
/// before anything is written. Reads the item streams of both objects of
/// every such name (through the loaded index).
pub(super) fn reconcile_duplicate_names(
    repo: &mut Repo,
    cancel: &CancelToken,
) -> Result<Vec<RepairAction>> {
    use std::collections::BTreeMap;

    let mut by_name: BTreeMap<String, Vec<SnapshotEntry>> = BTreeMap::new();
    for entry in &repo.manifest().snapshots {
        by_name
            .entry(entry.name.clone())
            .or_default()
            .push(entry.clone());
    }
    let mut actions = Vec::new();
    for (name, entries) in by_name {
        if entries.len() < 2 {
            continue;
        }
        let refuse = |why: String| {
            let ids: Vec<String> = entries.iter().map(|e| e.id.to_hex()).collect();
            VykarError::Other(format!(
                "aborting repair: {} snapshot objects carry the name '{name}' ({}), and {why}; \
                 nothing was changed",
                entries.len(),
                ids.join(", ")
            ))
        };
        let [first, second] = entries.as_slice() else {
            return Err(refuse(
                "an interrupted repair leaves at most two; repair cannot tell which to keep".into(),
            ));
        };
        let (kept, removed) = match superseded_rewrite(repo, first, second, cancel) {
            Ok(Some(pair)) => pair,
            Ok(None) => {
                return Err(refuse(
                    "they are not a snapshot and its rewrite by an interrupted repair; repair \
                     cannot tell which to keep"
                        .into(),
                ));
            }
            Err(e @ VykarError::Interrupted) => return Err(e),
            Err(e) => {
                return Err(refuse(format!(
                    "repair cannot read both to tell whether one is a rewrite an interrupted \
                     repair left: {e}"
                )));
            }
        };
        tracing::warn!(
            snapshot = name,
            kept = %kept.id,
            removed = %removed.id,
            "two snapshot objects carry one name: an interrupted repair's rewrite beside its \
             original; the rewrite is removed and the original repaired again"
        );
        let removed_id = removed.id;
        actions.push(RepairAction::RemoveInterruptedRewrite {
            snapshot_name: name,
            snapshot_id: removed_id,
            kept_id: kept.id,
        });
        repo.manifest_mut().snapshots.retain(|e| e.id != removed_id);
    }
    Ok(actions)
}

/// Of two objects of one name, `(kept, removed)` when one is the other with
/// items left out (see [`reconcile_duplicate_names`]); `None` when they are
/// not.
fn superseded_rewrite<'a>(
    repo: &mut Repo,
    first: &'a SnapshotEntry,
    second: &'a SnapshotEntry,
    cancel: &CancelToken,
) -> Result<Option<(&'a SnapshotEntry, &'a SnapshotEntry)>> {
    let meta_first = load_snapshot_meta_of(repo, first)?;
    let meta_second = load_snapshot_meta_of(repo, second)?;
    if !same_but_listing(&meta_first, &meta_second) {
        return Ok(None);
    }
    let items_first = items_of(repo, &meta_first, cancel)?;
    let items_second = items_of(repo, &meta_second, cancel)?;
    Ok(if is_subsequence(&items_second, &items_first) {
        Some((first, second))
    } else if is_subsequence(&items_first, &items_second) {
        Some((second, first))
    } else {
        None
    })
}

/// Whether `a` and `b` agree on every field an item-level rewrite copies
/// from the original (all but the listing's: `item_ptrs`, `stats`, `ext`
/// and `format_version`).
fn same_but_listing(a: &SnapshotMeta, b: &SnapshotMeta) -> bool {
    a.name == b.name
        && a.hostname == b.hostname
        && a.username == b.username
        && a.time == b.time
        && a.time_end == b.time_end
        && a.chunker_params == b.chunker_params
        && a.comment == b.comment
        && a.source_label == b.source_label
        && a.source_paths == b.source_paths
        && a.label == b.label
}

fn items_of(repo: &mut Repo, meta: &SnapshotMeta, cancel: &CancelToken) -> Result<Vec<Item>> {
    let stream = load_item_stream_of_meta(repo, meta, cancel)?;
    let mut items = Vec::new();
    for_each_decoded_item(&stream, |item| {
        items.push(item);
        Ok(())
    })?;
    Ok(items)
}

/// Whether `sub` is `sup` with zero or more items left out, in order.
fn is_subsequence(sub: &[Item], sup: &[Item]) -> bool {
    let mut sup = sup.iter();
    sub.iter().all(|item| sup.any(|s| s == item))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::super::scan::{ScanCounters, ScanResult};
    use super::*;

    #[test]
    fn build_repair_plan_treats_invalid_item_as_doomed_snapshot() {
        let snapshot_id = SnapshotId::from_bytes([0x11u8; 32]);
        let snapshot_name = "bad".to_string();
        let scan = ScanResult {
            counters: ScanCounters::default(),
            issues: vec![IntegrityIssue::InvalidItem {
                snapshot_id,
                snapshot_name: Some(snapshot_name.clone()),
                item_index: 0,
                item_path: "foo.txt".into(),
                reason: "regular file has size 10 but chunk sizes sum to 20".into(),
            }],
            snapshot_chunk_refs: HashMap::new(),
            snapshot_item_ptrs: HashMap::new(),
            snapshot_per_item_chunks: HashMap::new(),
            snapshot_item_counts: HashMap::new(),
            item_impacts: Vec::new(),
            key_source: None,
            key_copies_to_restore: Vec::new(),
            listing_index_warnings: Vec::new(),
        };
        let pack_chunks: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> = HashMap::new();
        let mut name_to_id: HashMap<String, SnapshotId> = HashMap::new();
        name_to_id.insert(snapshot_name.clone(), snapshot_id);

        let plan = build_repair_plan(&scan, &pack_chunks, &name_to_id);

        assert!(plan.has_data_loss);
        let has_remove = plan.actions.iter().any(|a| {
            matches!(
                a,
                RepairAction::RemoveCorruptSnapshot { snapshot_id: id, name: Some(n) }
                    if *id == snapshot_id && n == &snapshot_name
            )
        });
        assert!(
            has_remove,
            "expected RemoveCorruptSnapshot for invalid item, got: {:?}",
            plan.actions
        );
    }

    #[test]
    fn build_repair_plan_dedupes_invalid_item_with_corrupt_snapshot() {
        let snapshot_id = SnapshotId::from_bytes([0x22u8; 32]);
        let snapshot_name = "dup".to_string();
        let scan = ScanResult {
            counters: ScanCounters::default(),
            issues: vec![
                IntegrityIssue::CorruptSnapshot {
                    snapshot_id,
                    snapshot_name: Some(snapshot_name.clone()),
                },
                IntegrityIssue::InvalidItem {
                    snapshot_id,
                    snapshot_name: Some(snapshot_name.clone()),
                    item_index: 0,
                    item_path: "foo.txt".into(),
                    reason: "reason".into(),
                },
            ],
            snapshot_chunk_refs: HashMap::new(),
            snapshot_item_ptrs: HashMap::new(),
            snapshot_per_item_chunks: HashMap::new(),
            snapshot_item_counts: HashMap::new(),
            item_impacts: Vec::new(),
            key_source: None,
            key_copies_to_restore: Vec::new(),
            listing_index_warnings: Vec::new(),
        };
        let pack_chunks: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> = HashMap::new();
        let mut name_to_id: HashMap<String, SnapshotId> = HashMap::new();
        name_to_id.insert(snapshot_name.clone(), snapshot_id);

        let plan = build_repair_plan(&scan, &pack_chunks, &name_to_id);

        let count = plan
            .actions
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    RepairAction::RemoveCorruptSnapshot { snapshot_id: id, .. }
                        if *id == snapshot_id
                )
            })
            .count();
        assert_eq!(count, 1, "expected dedupe, got: {:?}", plan.actions);
    }
}
