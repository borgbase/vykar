use std::collections::{HashMap, HashSet};

use super::scan::{ScanResult, is_transient_io};
use super::types::{IndexRecovery, IntegrityIssue, RepairAction, RepairPlan};
use crate::commands::list::{
    for_each_decoded_item, load_item_stream_of_meta, load_snapshot_meta_of,
};
use crate::index::ChunkIndexEntry;
use crate::repo::Repo;
use crate::repo::control::CancelToken;
use crate::repo::manifest::SnapshotEntry;
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::{Item, ItemType};
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

/// When [`reconcile_duplicate_names`] decides a name one of whose objects
/// names a listing chunk the index lacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Judge {
    /// Before the re-index (`reindex::reindex_lost_entries`): such a name is
    /// left undecided, its objects in the manifest, so the first scan reports
    /// the chunks and the re-index can recover them.
    DeferUnindexed,
    /// After the re-index: a chunk still missing makes that object's listing
    /// unreadable.
    Final,
}

/// What [`reconcile_duplicate_names`] decided.
#[derive(Debug, Default)]
pub(super) struct Reconciled {
    /// The removals, by ID, first after the key copies in the plan.
    pub(super) actions: Vec<RepairAction>,
    /// A name was left undecided ([`Judge::DeferUnindexed`]).
    pub(super) deferred: bool,
}

impl Reconciled {
    pub(super) fn extend(&mut self, later: Reconciled) {
        self.actions.extend(later.actions);
        self.deferred = later.deferred;
    }
}

/// Reconcile the snapshot names more than one object carries, before the
/// scan the plan is built from. The scan, the plan and the repair look
/// snapshots up by name, so with two objects of one name they would check one
/// object twice and act on the other (an interrupted rewrite's rerun deleted
/// the blob of one entry and kept that entry, then failed its sweep with
/// "snapshot not found").
///
/// The objects of such a name repair recognises are its own: an item-level
/// rewrite (`DropItemsFromSnapshot`) PUTs the rewritten object, then DELETEs
/// the original, and a run interrupted between the two leaves both (a rerun
/// before this was fixed could leave a third). The rewrite copies every field
/// of the original's metadata but the listing ones (`item_ptrs`, `ext`,
/// `format_version`, and `stats.nfiles` and `stats.original_size`, which it
/// counts from the items it keeps) and keeps the original's items, in order,
/// but the dropped ones. So of the objects of one name whose listings can be
/// read, the one every other is a rewrite of (see [`is_rewrite_of`]; of equal
/// ones, the lowest ID) is kept and the others are removed from `repo`'s
/// manifest and planned for deletion ([`RepairAction::RemoveInterruptedRewrite`]):
/// every item they have the kept one has too, so the choice cannot lose data,
/// whichever object is which. The scan then sees the kept object alone and
/// plans its repair (an original is rewritten again).
///
/// An object whose metadata or listing cannot be read for a reason that is
/// not transient (an undecryptable object, a listing chunk missing, with
/// [`Judge::Final`], from the index) is removed by ID as data loss
/// ([`RepairAction::RemoveUnreadableDuplicate`]), as repair removes a lone
/// snapshot whose listing cannot be read, when another object of its name
/// can be read and is kept.
///
/// Any other name carried by more than one object refuses the repair, before
/// anything is written, and so does a transient read failure or an object
/// written by a newer vykar. Reads the metadata and item streams of every
/// object of every such name (through the loaded index), holding those of one
/// name at once.
pub(super) fn reconcile_duplicate_names(
    repo: &mut Repo,
    judge: Judge,
    cancel: &CancelToken,
) -> Result<Reconciled> {
    use std::collections::BTreeMap;

    let mut by_name: BTreeMap<String, Vec<SnapshotEntry>> = BTreeMap::new();
    for entry in &repo.manifest().snapshots {
        by_name
            .entry(entry.name.clone())
            .or_default()
            .push(entry.clone());
    }
    let mut out = Reconciled::default();
    for (name, entries) in by_name {
        if entries.len() < 2 {
            continue;
        }
        let mut readable: Vec<ReadObject> = Vec::new();
        let mut unreadable: Vec<(SnapshotId, String)> = Vec::new();
        let mut unindexed = false;
        for entry in &entries {
            match read_object(repo, entry, cancel)? {
                Read::Listing(object) => readable.push(*object),
                Read::Unreadable(detail) => unreadable.push((entry.id, detail)),
                Read::Unindexed(detail) => {
                    unindexed = true;
                    unreadable.push((entry.id, detail));
                }
                Read::Gone => {
                    // Deleted since the listing (a dry run beside a delete):
                    // as the scan does, skipped.
                    tracing::warn!(
                        "snapshot object {} of '{name}' was deleted, skipping",
                        entry.id
                    );
                    let id = entry.id;
                    repo.manifest_mut().snapshots.retain(|e| e.id != id);
                }
            }
        }
        if unindexed && judge == Judge::DeferUnindexed {
            out.deferred = true;
            continue;
        }
        if readable.len() + unreadable.len() < 2 {
            continue;
        }
        let refuse = |why: &str| refuse_duplicate_name(&name, &entries, why);
        if readable.is_empty() {
            return Err(refuse("none of their listings can be read"));
        }
        let Some((k, kept_id)) = choose_kept(&readable) else {
            return Err(refuse(
                "they are not a snapshot and its rewrites by an interrupted repair",
            ));
        };
        let mut removed: Vec<SnapshotId> = Vec::new();
        for (i, object) in readable.iter().enumerate() {
            if i == k {
                continue;
            }
            out.actions.push(RepairAction::RemoveInterruptedRewrite {
                snapshot_name: name.clone(),
                snapshot_id: object.id,
                kept_id,
            });
            removed.push(object.id);
        }
        for (id, detail) in unreadable {
            out.actions.push(RepairAction::RemoveUnreadableDuplicate {
                snapshot_name: name.clone(),
                snapshot_id: id,
                kept_id,
                detail,
            });
            removed.push(id);
        }
        tracing::warn!(
            snapshot = name,
            kept = %kept_id,
            removed = ?removed,
            "several snapshot objects carry one name; all but one are removed"
        );
        repo.manifest_mut()
            .snapshots
            .retain(|e| !removed.contains(&e.id));
    }
    Ok(out)
}

/// The refusal for objects of one name repair cannot reconcile.
fn refuse_duplicate_name(name: &str, entries: &[SnapshotEntry], why: &str) -> VykarError {
    let ids: Vec<String> = entries.iter().map(|e| e.id.to_hex()).collect();
    let first = entries.first().map(|e| e.id.to_hex()).unwrap_or_default();
    VykarError::Other(format!(
        "aborting repair: {} snapshot objects carry the name '{name}' ({}), and {why}; repair \
         cannot tell which to keep, and nothing was changed. `vykar snapshot delete {name}` \
         removes the first one listed (by time, then ID), {first}",
        entries.len(),
        ids.join(", ")
    ))
}

/// An object of a duplicated name whose listing was read.
struct ReadObject {
    id: SnapshotId,
    meta: SnapshotMeta,
    items: Vec<Item>,
}

/// How reading one object of a duplicated name went.
enum Read {
    Listing(Box<ReadObject>),
    /// Not for a transient reason.
    Unreadable(String),
    /// A listing chunk is not in the index (`ChunkNotInIndex`).
    Unindexed(String),
    /// The object is not on storage any more.
    Gone,
}

/// Read `entry`'s object. Errs (refusing the repair) on a transient failure
/// and for an object a newer vykar wrote; passes `Interrupted` on.
fn read_object(repo: &mut Repo, entry: &SnapshotEntry, cancel: &CancelToken) -> Result<Read> {
    let refuse_transient = |e: &VykarError| {
        VykarError::Other(format!(
            "aborting repair: reading snapshot object {} of '{}' failed: {e}; retry when \
             storage is stable; nothing was changed",
            entry.id, entry.name
        ))
    };
    let meta = match load_snapshot_meta_of(repo, entry) {
        Ok(meta) => meta,
        Err(e @ VykarError::Interrupted) => return Err(e),
        Err(VykarError::SnapshotNotFound(_)) => return Ok(Read::Gone),
        Err(
            e @ (VykarError::UnsupportedSnapshotVersion { .. }
            | VykarError::IncompatibleSnapshotEnvelope { .. }),
        ) => {
            return Err(VykarError::Other(format!(
                "aborting repair: snapshot object {} of '{}' was written by a newer vykar \
                 ({e}); upgrade vykar — repair leaves it untouched; nothing was changed",
                entry.id, entry.name
            )));
        }
        Err(e) if is_transient_io(&e) => return Err(refuse_transient(&e)),
        Err(e) => return Ok(Read::Unreadable(format!("load metadata: {e}"))),
    };
    match items_of(repo, &meta, cancel) {
        Ok(items) => Ok(Read::Listing(Box::new(ReadObject {
            id: entry.id,
            meta,
            items,
        }))),
        Err(e @ VykarError::Interrupted) => Err(e),
        Err(e @ VykarError::ChunkNotInIndex(_)) => {
            Ok(Read::Unindexed(format!("load item stream: {e}")))
        }
        Err(e) if is_transient_io(&e) => Err(refuse_transient(&e)),
        Err(e) => Ok(Read::Unreadable(format!("load item stream: {e}"))),
    }
}

/// Of `objects`, the one every other is a rewrite of ([`is_rewrite_of`]),
/// with its ID; of several (equal listings), the one with the lowest ID.
/// `None` when there is none.
fn choose_kept(objects: &[ReadObject]) -> Option<(usize, SnapshotId)> {
    objects
        .iter()
        .enumerate()
        .filter(|(k, kept)| {
            objects
                .iter()
                .enumerate()
                .all(|(i, o)| i == *k || is_rewrite_of(o, kept))
        })
        .min_by_key(|(_, kept)| *kept.id.as_bytes())
        .map(|(k, kept)| (k, kept.id))
}

/// Whether `rewrite` can be an item-level rewrite of `original`, or of a
/// rewrite of it: they agree on every field the rewrite copies (metadata
/// and the copied stats), `rewrite`'s counted stats are those of its items,
/// and its items are `original`'s with zero or more left out, in order.
fn is_rewrite_of(rewrite: &ReadObject, original: &ReadObject) -> bool {
    same_but_listing(&rewrite.meta, &original.meta)
        && counted_stats_match(&rewrite.meta, &rewrite.items)
        && is_subsequence(&rewrite.items, &original.items)
}

/// Whether `a` and `b` agree on every field an item-level rewrite copies
/// from the original: all but the listing's (`item_ptrs`, `ext`,
/// `format_version`, and the stats it counts, `nfiles` and
/// `original_size`).
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
        && a.stats.compressed_size == b.stats.compressed_size
        && a.stats.deduplicated_size == b.stats.deduplicated_size
        && a.stats.errors == b.stats.errors
}

/// Whether `meta`'s `nfiles` and `original_size` are what an item-level
/// rewrite counts from `items` (its regular files and their sizes).
fn counted_stats_match(meta: &SnapshotMeta, items: &[Item]) -> bool {
    let files = items
        .iter()
        .filter(|i| i.entry_type == ItemType::RegularFile);
    let (nfiles, size) = files.fold((0u64, 0u64), |(n, s), i| (n + 1, s + i.size));
    meta.stats.nfiles == nfiles && meta.stats.original_size == size
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

    // -----------------------------------------------------------------
    // The recognition of an interrupted rewrite
    // -----------------------------------------------------------------

    fn item(path: &str, size: u64) -> Item {
        Item {
            path: path.into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 1,
            atime: None,
            ctime: None,
            size,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
            raw_names: None,
            hardlink: None,
        }
    }

    fn meta() -> SnapshotMeta {
        let time = chrono::DateTime::from_timestamp(1_700_000_000, 7).unwrap();
        SnapshotMeta {
            name: "s".into(),
            hostname: "h".into(),
            username: "u".into(),
            time,
            time_end: time,
            chunker_params: crate::chunker::ChunkerParams::default(),
            comment: String::new(),
            item_ptrs: Vec::new(),
            stats: crate::snapshot::SnapshotStats {
                compressed_size: 11,
                deduplicated_size: 5,
                errors: 1,
                ..Default::default()
            },
            source_label: "src".into(),
            source_paths: vec!["/src".into()],
            label: String::new(),
            ext: None,
            format_version: crate::snapshot::CURRENT_FORMAT_VERSION,
        }
    }

    /// An object of `items` whose counted stats are its items'.
    fn object(id: u8, items: Vec<Item>) -> ReadObject {
        let mut meta = meta();
        meta.stats.nfiles = items.len() as u64;
        meta.stats.original_size = items.iter().map(|i| i.size).sum();
        ReadObject {
            id: SnapshotId::from_bytes([id; 32]),
            meta,
            items,
        }
    }

    #[test]
    fn is_subsequence_of_equal_empty_and_reordered_lists() {
        let (a, b, c) = (item("a", 1), item("b", 2), item("c", 3));
        let abc = [a.clone(), b.clone(), c.clone()];
        assert!(is_subsequence(&abc, &abc), "equal");
        assert!(is_subsequence(&[], &abc), "empty in non-empty");
        assert!(is_subsequence(&[], &[]), "empty in empty");
        assert!(!is_subsequence(&abc, &[]), "non-empty in empty");
        assert!(
            is_subsequence(&[a.clone(), c.clone()], &abc),
            "one left out"
        );
        assert!(!is_subsequence(&[c.clone(), a.clone()], &abc), "reordered");
        assert!(
            !is_subsequence(&[b.clone(), a.clone()], &[a.clone(), b.clone()]),
            "swapped pair"
        );
        assert!(
            !is_subsequence(&[a.clone(), a.clone()], std::slice::from_ref(&a)),
            "repeated"
        );
        assert!(
            !is_subsequence(&[item("a", 9)], &abc),
            "an item that differs"
        );
    }

    #[test]
    fn same_but_listing_compares_what_a_rewrite_copies() {
        let a = meta();
        assert!(same_but_listing(&a, &a.clone()), "equal");

        // What a rewrite replaces may differ.
        let mut b = a.clone();
        b.item_ptrs = vec![ChunkId::from_bytes([1; 32])];
        b.ext = Some(vec![1]);
        b.format_version = 1;
        b.stats.nfiles = 9;
        b.stats.original_size = 9;
        assert!(same_but_listing(&a, &b), "the listing fields");

        // What it copies may not.
        type Change = fn(&mut SnapshotMeta);
        let changes: [(&str, Change); 8] = [
            ("comment", |m| m.comment = "x".into()),
            ("hostname", |m| m.hostname = "x".into()),
            ("time", |m| m.time += chrono::Duration::nanoseconds(1)),
            ("time_end", |m| {
                m.time_end += chrono::Duration::nanoseconds(1)
            }),
            ("source_paths", |m| m.source_paths.clear()),
            ("compressed_size", |m| m.stats.compressed_size += 1),
            ("deduplicated_size", |m| m.stats.deduplicated_size += 1),
            ("errors", |m| m.stats.errors += 1),
        ];
        for (field, change) in changes {
            let mut b = a.clone();
            change(&mut b);
            assert!(!same_but_listing(&a, &b), "{field}");
        }
    }

    fn kept(objects: &[ReadObject]) -> Option<usize> {
        choose_kept(objects).map(|(k, _)| k)
    }

    #[test]
    fn choose_kept_keeps_the_object_every_other_is_a_rewrite_of() {
        let (a, b, c) = (item("a", 1), item("b", 2), item("c", 3));
        let original = || object(0x50, vec![a.clone(), b.clone(), c.clone()]);

        // An original and two rewrites, in any order.
        let objects = vec![
            object(0x10, vec![a.clone(), c.clone()]),
            original(),
            object(0x90, vec![a.clone()]),
        ];
        assert_eq!(kept(&objects), Some(1));

        // Equal listings: the lowest ID.
        let objects = vec![
            object(0x90, vec![a.clone(), c.clone()]),
            object(0x10, vec![a.clone(), c.clone()]),
            object(0x50, vec![a.clone(), c.clone()]),
        ];
        assert_eq!(kept(&objects), Some(1));

        // One alone is kept.
        assert_eq!(kept(&[original()]), Some(0));

        // Two rewrites that left out different items: neither is kept.
        let objects = vec![
            object(0x10, vec![a.clone(), c.clone()]),
            object(0x90, vec![a.clone(), b.clone()]),
        ];
        assert_eq!(kept(&objects), None);

        // A rewrite whose counted stats are not its items' is none.
        let mut rewrite = object(0x10, vec![a.clone()]);
        rewrite.meta.stats.nfiles += 1;
        assert_eq!(kept(&[rewrite, original()]), None);

        // Nor one whose copied stats differ.
        let mut rewrite = object(0x10, vec![a.clone()]);
        rewrite.meta.stats.errors = 0;
        assert_eq!(kept(&[rewrite, original()]), None);
    }
}
