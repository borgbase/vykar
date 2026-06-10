use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::types::{emit_progress, CheckProgressEvent, IntegrityIssue, ItemImpact};
use crate::commands::list::{for_each_decoded_item, load_snapshot_item_stream, load_snapshot_meta};
use crate::compress;
use crate::config::VykarConfig;
use crate::index::ChunkIndexEntry;
use crate::repo::format::{unpack_object_expect_with_context, ObjectType};
use crate::repo::pack::{
    read_blob_from_pack, PACK_HEADER_SIZE, PACK_MAGIC, PACK_VERSION_MAX, PACK_VERSION_MIN,
};
use crate::repo::Repository;
use crate::snapshot::item::ItemType;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

/// Number of chunks in a pack before we download the full pack instead of
/// issuing individual range reads.
const BATCH_THRESHOLD: usize = 3;

/// Returns `true` if the error is a transient I/O failure (not proven
/// corruption). Crypto, deserialization, format, and decompression errors are
/// considered evidence of corruption.
fn is_transient_io(err: &VykarError) -> bool {
    matches!(err, VykarError::Io(_))
}

// ---------------------------------------------------------------------------
// Integrity scan (shared by read-only check and repair)
// ---------------------------------------------------------------------------

/// Options controlling the integrity scan phases.
pub(super) struct ScanOptions<'a> {
    /// Collect per-snapshot chunk refs (needed for repair plan).
    pub(super) collect_chunk_refs: bool,
    /// Detect orphan snapshot blobs on storage not in the manifest.
    pub(super) detect_orphans: bool,
    /// Run client-side crypto verification of chunk data.
    pub(super) verify_data: bool,
    /// Packs already verified server-side — skip existence and data checks for these.
    pub(super) skip_packs: Option<&'a HashSet<PackId>>,
    /// If set, sample only this percentage of snapshots in Phase 1.
    pub(super) snapshot_sample_percent: Option<u8>,
}

/// Counters collected during an integrity scan.
#[derive(Debug, Default)]
pub(super) struct ScanCounters {
    pub(super) snapshots_checked: usize,
    pub(super) items_checked: usize,
    pub(super) chunks_existence_checked: usize,
    pub(super) packs_existence_checked: usize,
    pub(super) chunks_data_verified: usize,
}

/// Output of [`repair_scan`]: counters, issues, and per-snapshot chunk refs.
pub(super) struct ScanResult {
    pub(super) counters: ScanCounters,
    pub(super) issues: Vec<IntegrityIssue>,
    /// Maps each snapshot name to the set of chunk IDs it references
    /// (item_ptrs and file chunks combined; used by the chunks-to-remove
    /// dangling-snapshot path).
    pub(super) snapshot_chunk_refs: HashMap<String, HashSet<ChunkId>>,
    /// Per-snapshot `item_ptrs` chunks, partitioned out from
    /// `snapshot_chunk_refs`. Populated whenever the scan reads a snapshot's
    /// metadata, even if the items_stream subsequently fails to load — so the
    /// item-ptrs coverage gate has data to fail-closed against.
    pub(super) snapshot_item_ptrs: HashMap<SnapshotId, HashSet<ChunkId>>,
    /// Per-snapshot, per-item file chunk sets. Indexed by 0-based item ordinal
    /// within the decoded items_stream. Empty inner sets for non-`RegularFile`
    /// items. Populated only when `collect_chunk_refs == true` AND the
    /// items_stream walk completes successfully — snapshots whose walk fails
    /// or skips will be absent from this map (the planner reads that absence
    /// as "refuse item-level repair").
    pub(super) snapshot_per_item_chunks: HashMap<SnapshotId, Vec<HashSet<ChunkId>>>,
    /// Per-snapshot total item count. Populated in lockstep with
    /// `snapshot_per_item_chunks`: present iff the items_stream walk completed.
    pub(super) snapshot_item_counts: HashMap<SnapshotId, usize>,
    /// Items whose chunks reference a pack confirmed missing in Phase 2.
    /// Empty when no missing packs were detected.
    pub(super) item_impacts: Vec<ItemImpact>,
}

/// Run the integrity scan, producing structured issues.
///
/// `ScanOptions` controls which phases run and which packs are skipped.
/// The caller is responsible for calling `repo.refresh_snapshot_list()` before
/// this function when repair-level freshness is needed.
pub(super) fn integrity_scan(
    repo: &mut Repository,
    config: &VykarConfig,
    opts: &ScanOptions,
    progress: &mut Option<&mut dyn FnMut(CheckProgressEvent)>,
) -> Result<ScanResult> {
    let mut counters = ScanCounters::default();
    let mut issues: Vec<IntegrityIssue> = Vec::new();
    let mut snapshot_chunk_refs: HashMap<String, HashSet<ChunkId>> = HashMap::new();
    let mut snapshot_item_ptrs: HashMap<SnapshotId, HashSet<ChunkId>> = HashMap::new();
    let mut snapshot_per_item_chunks: HashMap<SnapshotId, Vec<HashSet<ChunkId>>> = HashMap::new();
    let mut snapshot_item_counts: HashMap<SnapshotId, usize> = HashMap::new();

    let is_remote = vykar_storage::parse_repo_url(&config.repository.url)
        .map(|u| !u.is_local())
        .unwrap_or(false);
    let concurrency = config.limits.listing_concurrency(is_remote);

    // Phase 0: Raw storage scan for corrupted/invalid snapshots not in manifest.
    if opts.detect_orphans {
        let manifest_ids: HashSet<String> = repo
            .manifest()
            .snapshots
            .iter()
            .map(|e| e.id.to_hex())
            .collect();
        let remote_keys = repo.storage.list("snapshots/")?;
        for key in &remote_keys {
            let Some(id_hex) = key.strip_prefix("snapshots/") else {
                continue;
            };
            if id_hex.is_empty() || manifest_ids.contains(id_hex) {
                continue;
            }
            match SnapshotId::from_hex(id_hex) {
                Err(_) => {
                    issues.push(IntegrityIssue::InvalidSnapshotKey {
                        storage_key: key.clone(),
                    });
                }
                Ok(snapshot_id) => match repo.storage.get(key) {
                    Ok(Some(blob)) => {
                        let decoded = unpack_object_expect_with_context(
                            &blob,
                            ObjectType::SnapshotMeta,
                            snapshot_id.as_bytes(),
                            repo.crypto.as_ref(),
                        )
                        .and_then(|meta_bytes| {
                            rmp_serde::from_slice::<crate::snapshot::SnapshotMeta>(&meta_bytes)
                                .map_err(|e| VykarError::Other(format!("deserialize: {e}")))
                        });
                        match decoded {
                            // A too-new blob deserializes fine (the envelope is
                            // frozen): classify unsupported, never corrupt.
                            Ok(meta)
                                if meta.format_version
                                    > crate::snapshot::CURRENT_FORMAT_VERSION =>
                            {
                                issues.push(IntegrityIssue::UnsupportedSnapshotVersion {
                                    snapshot_id,
                                    snapshot_name: None,
                                    version: meta.format_version,
                                });
                            }
                            Ok(_) => {}
                            Err(_) => {
                                issues.push(IntegrityIssue::CorruptSnapshot {
                                    snapshot_id,
                                    snapshot_name: None,
                                });
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::warn!("snapshot {snapshot_id} listed but not found, skipping");
                    }
                    Err(e) => {
                        issues.push(IntegrityIssue::SnapshotReadFailed {
                            snapshot_name: format!("(orphan {snapshot_id})"),
                            detail: format!("GET failed: {e}"),
                        });
                    }
                },
            }
        }
    }

    // Phase 1: Check each snapshot in manifest
    let all_snapshot_entries = repo.manifest().snapshots.clone();
    let snapshot_entries: Vec<_> = if let Some(pct) = opts.snapshot_sample_percent {
        let total = all_snapshot_entries.len();
        if total == 0 || pct >= 100 {
            all_snapshot_entries
        } else {
            let keep = (total as u64 * pct as u64).div_ceil(100) as usize;
            let keep = keep.max(1).min(total);
            let mut rng = rand::rng();
            let indices: std::collections::HashSet<usize> =
                rand::seq::index::sample(&mut rng, total, keep)
                    .into_iter()
                    .collect();
            all_snapshot_entries
                .into_iter()
                .enumerate()
                .filter(|(i, _)| indices.contains(i))
                .map(|(_, e)| e)
                .collect()
        }
    } else {
        all_snapshot_entries
    };
    let snapshot_count = snapshot_entries.len();
    for (i, entry) in snapshot_entries.iter().enumerate() {
        emit_progress(
            progress,
            CheckProgressEvent::SnapshotStarted {
                current: i + 1,
                total: snapshot_count,
                name: entry.name.clone(),
            },
        );

        let meta = match load_snapshot_meta(repo, &entry.name) {
            Ok(m) => m,
            Err(VykarError::UnsupportedSnapshotVersion { version }) => {
                // The envelope decoded — the blob is intact, just too new.
                // Classify as unsupported (never corrupt) and skip walking
                // item_ptrs, since we can't trust a future item layout.
                issues.push(IntegrityIssue::UnsupportedSnapshotVersion {
                    snapshot_id: entry.id,
                    snapshot_name: Some(entry.name.clone()),
                    version,
                });
                continue;
            }
            Err(e) => {
                if is_transient_io(&e) {
                    issues.push(IntegrityIssue::SnapshotReadFailed {
                        snapshot_name: entry.name.clone(),
                        detail: format!("load metadata: {e}"),
                    });
                } else {
                    issues.push(IntegrityIssue::CorruptSnapshot {
                        snapshot_id: entry.id,
                        snapshot_name: Some(entry.name.clone()),
                    });
                }
                continue;
            }
        };

        // Verify item_ptrs exist in chunk index; optionally collect chunk refs.
        // `snapshot_item_ptrs` is populated unconditionally (fail-closed input
        // for the item-ptrs coverage gate), even when `collect_chunk_refs` is
        // disabled or the items_stream walk later fails.
        for chunk_id in &meta.item_ptrs {
            snapshot_item_ptrs
                .entry(entry.id)
                .or_default()
                .insert(*chunk_id);
            if opts.collect_chunk_refs {
                snapshot_chunk_refs
                    .entry(entry.name.clone())
                    .or_default()
                    .insert(*chunk_id);
            }
            if !repo.chunk_index().contains(chunk_id) {
                issues.push(IntegrityIssue::DanglingItemPtr {
                    snapshot_name: entry.name.clone(),
                    chunk_id: *chunk_id,
                });
            }
        }

        // Load item stream, check file chunks
        let items_stream = match load_snapshot_item_stream(repo, &entry.name) {
            Ok(s) => s,
            Err(e) => {
                if is_transient_io(&e) {
                    issues.push(IntegrityIssue::SnapshotReadFailed {
                        snapshot_name: entry.name.clone(),
                        detail: format!("load item stream: {e}"),
                    });
                } else {
                    issues.push(IntegrityIssue::UnreadableSnapshot {
                        snapshot_name: entry.name.clone(),
                        detail: format!("load item stream: {e}"),
                    });
                }
                continue;
            }
        };

        let mut per_snapshot_items = 0usize;
        let mut item_index = 0usize;
        let entry_name = entry.name.clone();
        let entry_id = entry.id;
        let collect_refs = opts.collect_chunk_refs;
        let mut item_issues: Vec<IntegrityIssue> = Vec::new();
        let mut file_chunk_ids: Vec<ChunkId> = Vec::new();
        let mut per_item_chunks: Vec<HashSet<ChunkId>> = Vec::new();
        let walk_result = for_each_decoded_item(&items_stream, |item| {
            let idx = item_index;
            item_index += 1;
            per_snapshot_items += 1;
            if let Err(e) = item.validate() {
                item_issues.push(IntegrityIssue::InvalidItem {
                    snapshot_id: entry_id,
                    snapshot_name: Some(entry_name.clone()),
                    item_index: idx,
                    item_path: item.path.clone(),
                    reason: e.to_string(),
                });
            }
            let mut this_item: HashSet<ChunkId> = HashSet::new();
            if item.entry_type == ItemType::RegularFile {
                for chunk_ref in &item.chunks {
                    if collect_refs {
                        file_chunk_ids.push(chunk_ref.id);
                        this_item.insert(chunk_ref.id);
                    }
                    if !repo.chunk_index().contains(&chunk_ref.id) {
                        item_issues.push(IntegrityIssue::DanglingFileChunk {
                            snapshot_name: entry_name.clone(),
                            item_index: idx,
                            path: item.path.clone(),
                            chunk_id: chunk_ref.id,
                        });
                    }
                }
            }
            if collect_refs {
                debug_assert_eq!(
                    per_item_chunks.len(),
                    idx,
                    "per_item_chunks must mirror item ordinals"
                );
                per_item_chunks.push(this_item);
            }
            Ok(())
        });
        let walk_ok = walk_result.is_ok();
        if let Err(e) = walk_result {
            issues.push(IntegrityIssue::UnreadableSnapshot {
                snapshot_name: entry.name.clone(),
                detail: format!("decode items: {e}"),
            });
        }
        issues.extend(item_issues);
        if collect_refs {
            snapshot_chunk_refs
                .entry(entry.name.clone())
                .or_default()
                .extend(file_chunk_ids);
            // Only publish per-item chunks and item count when the walk
            // completed: a partial walk would yield a vec shorter than the
            // true item count, and the planner's data-presence gate must be
            // able to reject such snapshots from item-level repair.
            if walk_ok {
                debug_assert_eq!(per_item_chunks.len(), per_snapshot_items);
                snapshot_per_item_chunks.insert(entry_id, per_item_chunks);
                snapshot_item_counts.insert(entry_id, per_snapshot_items);
            }
        } else if walk_ok {
            // collect_chunk_refs disabled (read-only check): we don't need the
            // per-item chunk sets, but record the count so a future repair
            // pass can see this snapshot was successfully walked.
            snapshot_item_counts.insert(entry_id, per_snapshot_items);
        }

        counters.items_checked += per_snapshot_items;
        counters.snapshots_checked += 1;
    }

    // Phase 2: Pack existence check
    let mut pack_chunks: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> = HashMap::new();
    for (chunk_id, entry) in repo.chunk_index().iter() {
        if let Some(skip) = opts.skip_packs {
            if skip.contains(&entry.pack_id) {
                continue;
            }
        }
        pack_chunks
            .entry(entry.pack_id)
            .or_default()
            .push((*chunk_id, *entry));
    }
    let packs_for_existence: Vec<(PackId, usize)> = pack_chunks
        .iter()
        .map(|(pack_id, chunks)| (*pack_id, chunks.len()))
        .collect();

    let mut missing_packs: HashSet<PackId> = HashSet::new();
    if !packs_for_existence.is_empty() {
        emit_progress(
            progress,
            CheckProgressEvent::PacksExistencePhaseStarted {
                total_packs: packs_for_existence.len(),
            },
        );

        let (existence_checked, missing_count, pack_issues) =
            parallel_pack_existence(&repo.storage, &packs_for_existence, concurrency);
        counters.packs_existence_checked = existence_checked;

        // Count chunks only in packs whose existence was definitively resolved
        // (Ok(true) or Ok(false)). Packs with I/O errors are not counted.
        let io_failed_packs: HashSet<PackId> = pack_issues
            .iter()
            .filter_map(|issue| match issue {
                IntegrityIssue::PackExistenceCheckFailed { pack_id, .. } => Some(*pack_id),
                _ => None,
            })
            .collect();
        counters.chunks_existence_checked = pack_chunks
            .iter()
            .filter(|(pid, _)| !io_failed_packs.contains(pid))
            .map(|(_, chunks)| chunks.len())
            .sum();

        for issue in &pack_issues {
            if let IntegrityIssue::MissingPack { pack_id } = issue {
                missing_packs.insert(*pack_id);
            }
        }

        issues.extend(pack_issues);

        emit_progress(
            progress,
            CheckProgressEvent::PacksExistenceProgress {
                checked: existence_checked,
                total_packs: packs_for_existence.len(),
                missing: missing_count,
            },
        );
    }

    // Phase 2b: Locate snapshot items affected by missing packs (issue #122).
    // Cheap on healthy repos — short-circuits when no packs are missing.
    let item_impacts = locate_items_in_missing_packs(repo, &missing_packs);

    // Phase 3: Verify data (client-side crypto verification)
    if opts.verify_data {
        let remaining_chunks: usize = pack_chunks.values().map(|chunks| chunks.len()).sum();

        emit_progress(
            progress,
            CheckProgressEvent::ChunksDataPhaseStarted {
                total_chunks: remaining_chunks,
            },
        );

        let packs_vec: Vec<(PackId, Vec<(ChunkId, ChunkIndexEntry)>)> =
            pack_chunks.into_iter().collect();

        let (data_count, data_issues) = parallel_verify_data(
            &repo.storage,
            &repo.crypto,
            repo.crypto.chunk_id_key(),
            &packs_vec,
            config.limits.verify_data_concurrency(),
            BATCH_THRESHOLD,
        );
        counters.chunks_data_verified = data_count;
        issues.extend(data_issues);

        emit_progress(
            progress,
            CheckProgressEvent::ChunksDataProgress {
                verified: data_count,
                total_chunks: remaining_chunks,
            },
        );
    }

    Ok(ScanResult {
        counters,
        issues,
        snapshot_chunk_refs,
        snapshot_item_ptrs,
        snapshot_per_item_chunks,
        snapshot_item_counts,
        item_impacts,
    })
}

/// Walk every snapshot's item stream and emit one [`ItemImpact`] per
/// regular-file item that references a chunk in `missing_packs`.
///
/// Snapshots whose item stream fails to load — or whose decode aborts mid-stream
/// — are silently skipped. Such failures are already surfaced by the main scan
/// as `UnreadableSnapshot` / `SnapshotReadFailed` issues.
fn locate_items_in_missing_packs(
    repo: &mut Repository,
    missing_packs: &HashSet<PackId>,
) -> Vec<ItemImpact> {
    if missing_packs.is_empty() {
        return Vec::new();
    }

    // Restrict the chunk → pack lookup to chunks in missing packs.
    let chunk_to_missing_pack: HashMap<ChunkId, PackId> = repo
        .chunk_index()
        .iter()
        .filter(|(_, entry)| missing_packs.contains(&entry.pack_id))
        .map(|(chunk_id, entry)| (*chunk_id, entry.pack_id))
        .collect();

    if chunk_to_missing_pack.is_empty() {
        return Vec::new();
    }

    let entries = repo.manifest().snapshots.clone();
    let mut impacts: Vec<ItemImpact> = Vec::new();

    for entry in &entries {
        let items_stream = match load_snapshot_item_stream(repo, &entry.name) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let mut item_index: usize = 0;
        let mut local: Vec<ItemImpact> = Vec::new();
        let walk = for_each_decoded_item(&items_stream, |item| {
            let idx = item_index;
            item_index += 1;
            if item.entry_type == ItemType::RegularFile {
                let mut affected: Vec<(ChunkId, PackId)> = Vec::new();
                for chunk_ref in &item.chunks {
                    if let Some(pack_id) = chunk_to_missing_pack.get(&chunk_ref.id) {
                        affected.push((chunk_ref.id, *pack_id));
                    }
                }
                if !affected.is_empty() {
                    local.push(ItemImpact {
                        snapshot_id: entry.id,
                        snapshot_name: entry.name.clone(),
                        item_index: idx,
                        item_path: item.path.clone(),
                        affected_chunks: affected,
                    });
                }
            }
            Ok(())
        });
        // Drop partial impacts if decode aborted mid-stream — the snapshot is
        // already reported as UnreadableSnapshot by the main scan, and #123
        // will treat it as whole-snapshot doomed.
        if walk.is_ok() {
            impacts.extend(local);
        }
    }

    impacts
}

/// Parallel pack existence check producing IntegrityIssue variants.
/// Returns `(packs_actually_checked, missing_count, issues)` — `packs_actually_checked`
/// counts packs whose existence was definitively resolved (Ok(true) or Ok(false));
/// packs with I/O errors are NOT counted so the summary does not claim complete coverage.
/// `missing_count` is the subset of those resolved as Ok(false).
fn parallel_pack_existence(
    storage: &Arc<dyn StorageBackend>,
    packs: &[(PackId, usize)],
    concurrency: usize,
) -> (usize, usize, Vec<IntegrityIssue>) {
    if packs.is_empty() {
        return (0, 0, Vec::new());
    }

    let work_idx = AtomicUsize::new(0);
    let present_ok = AtomicUsize::new(0);
    let missing_count = AtomicUsize::new(0);
    let issues = Mutex::new(Vec::new());

    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| loop {
                let idx = work_idx.fetch_add(1, Ordering::Relaxed);
                let Some((pack_id, _chunk_count)) = packs.get(idx) else {
                    break;
                };
                let pack_key = pack_id.storage_key();
                match storage.exists(&pack_key) {
                    Ok(true) => {
                        present_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(false) => {
                        missing_count.fetch_add(1, Ordering::Relaxed);
                        issues
                            .lock()
                            .expect("scan present issues lock not poisoned")
                            .push(IntegrityIssue::MissingPack { pack_id: *pack_id });
                    }
                    Err(e) => {
                        issues
                            .lock()
                            .expect("scan present issues lock not poisoned")
                            .push(IntegrityIssue::PackExistenceCheckFailed {
                                pack_id: *pack_id,
                                detail: e.to_string(),
                            });
                    }
                }
            });
        }
    });

    let present = present_ok.load(Ordering::Relaxed);
    let missing = missing_count.load(Ordering::Relaxed);
    (
        present + missing,
        missing,
        issues
            .into_inner()
            .expect("scan present issues lock not poisoned"),
    )
}

/// Parallel verify-data producing IntegrityIssue variants.
fn parallel_verify_data(
    storage: &Arc<dyn StorageBackend>,
    crypto: &Arc<dyn CryptoEngine>,
    chunk_id_key: &[u8; 32],
    packs: &[(PackId, Vec<(ChunkId, ChunkIndexEntry)>)],
    concurrency: usize,
    batch_threshold: usize,
) -> (usize, Vec<IntegrityIssue>) {
    if packs.is_empty() {
        return (0, Vec::new());
    }

    let work_idx = AtomicUsize::new(0);
    let verified = AtomicUsize::new(0);
    let issues = Mutex::new(Vec::new());

    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| loop {
                let idx = work_idx.fetch_add(1, Ordering::Relaxed);
                let Some((pack_id, chunks)) = packs.get(idx) else {
                    break;
                };

                let mut local_issues = Vec::new();
                let count = if chunks.len() >= batch_threshold {
                    verify_pack_full(
                        storage.as_ref(),
                        crypto.as_ref(),
                        chunk_id_key,
                        pack_id,
                        chunks,
                        &mut local_issues,
                    )
                } else {
                    verify_pack_individual(
                        storage.as_ref(),
                        crypto.as_ref(),
                        chunk_id_key,
                        pack_id,
                        chunks,
                        &mut local_issues,
                    )
                };

                verified.fetch_add(count, Ordering::Relaxed);
                if !local_issues.is_empty() {
                    issues
                        .lock()
                        .expect("scan verify issues lock not poisoned")
                        .extend(local_issues);
                }
            });
        }
    });

    (
        verified.load(Ordering::Relaxed),
        issues
            .into_inner()
            .expect("scan verify issues lock not poisoned"),
    )
}

/// Download the full pack and verify each chunk locally.
pub(crate) fn verify_pack_full(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    chunk_id_key: &[u8; 32],
    pack_id: &PackId,
    chunks: &[(ChunkId, ChunkIndexEntry)],
    issues: &mut Vec<IntegrityIssue>,
) -> usize {
    let pack_key = pack_id.storage_key();
    let pack_data = match storage.get(&pack_key) {
        Ok(Some(data)) => data,
        Ok(None) => {
            issues.push(IntegrityIssue::CorruptPackContent {
                pack_id: *pack_id,
                detail: "pack not found (full GET)".into(),
            });
            return 0;
        }
        Err(e) => {
            issues.push(IntegrityIssue::PackExistenceCheckFailed {
                pack_id: *pack_id,
                detail: format!("full GET failed: {e}"),
            });
            return 0;
        }
    };

    // Validate header — first slice/index is gated by the length check.
    #[allow(clippy::indexing_slicing)]
    let header_invalid = pack_data.len() < PACK_HEADER_SIZE
        || &pack_data[..8] != PACK_MAGIC
        || pack_data[8] < PACK_VERSION_MIN
        || pack_data[8] > PACK_VERSION_MAX;
    if header_invalid {
        issues.push(IntegrityIssue::CorruptPackContent {
            pack_id: *pack_id,
            detail: "invalid pack header".into(),
        });
        return 0;
    }

    let mut count = 0;
    for (chunk_id, entry) in chunks {
        let start = match usize::try_from(entry.pack_offset) {
            Ok(s) => s,
            Err(_) => {
                issues.push(IntegrityIssue::CorruptChunk {
                    chunk_id: *chunk_id,
                    pack_id: *pack_id,
                    detail: format!(
                        "pack_offset {} exceeds addressable range",
                        entry.pack_offset
                    ),
                });
                continue;
            }
        };
        let size = match usize::try_from(entry.stored_size) {
            Ok(s) => s,
            Err(_) => {
                issues.push(IntegrityIssue::CorruptChunk {
                    chunk_id: *chunk_id,
                    pack_id: *pack_id,
                    detail: format!(
                        "stored_size {} exceeds addressable range",
                        entry.stored_size
                    ),
                });
                continue;
            }
        };
        let end = match start.checked_add(size) {
            Some(e) => e,
            None => {
                issues.push(IntegrityIssue::CorruptChunk {
                    chunk_id: *chunk_id,
                    pack_id: *pack_id,
                    detail: format!(
                        "blob range overflows (offset={}, size={})",
                        entry.pack_offset, entry.stored_size
                    ),
                });
                continue;
            }
        };
        if end > pack_data.len() {
            issues.push(IntegrityIssue::CorruptChunk {
                chunk_id: *chunk_id,
                pack_id: *pack_id,
                detail: format!(
                    "blob range [{start}..{end}) exceeds pack size {}",
                    pack_data.len()
                ),
            });
            continue;
        }

        let raw = pack_data
            .get(start..end)
            .expect("end <= pack_data.len() (checked above)");
        count += verify_single_chunk(crypto, chunk_id_key, chunk_id, pack_id, raw, issues);
    }
    count
}

/// Verify each chunk individually via range reads.
fn verify_pack_individual(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    chunk_id_key: &[u8; 32],
    pack_id: &PackId,
    chunks: &[(ChunkId, ChunkIndexEntry)],
    issues: &mut Vec<IntegrityIssue>,
) -> usize {
    let mut count = 0;
    for (chunk_id, entry) in chunks {
        let raw = match read_blob_from_pack(storage, pack_id, entry.pack_offset, entry.stored_size)
        {
            Ok(data) => data,
            Err(e) => {
                issues.push(IntegrityIssue::CorruptChunk {
                    chunk_id: *chunk_id,
                    pack_id: *pack_id,
                    detail: format!("read failed: {e}"),
                });
                continue;
            }
        };
        count += verify_single_chunk(crypto, chunk_id_key, chunk_id, pack_id, &raw, issues);
    }
    count
}

/// Decrypt, decompress, and recompute ChunkId for one blob. Returns 1 on success, 0 on error.
fn verify_single_chunk(
    crypto: &dyn CryptoEngine,
    chunk_id_key: &[u8; 32],
    chunk_id: &ChunkId,
    pack_id: &PackId,
    raw: &[u8],
    issues: &mut Vec<IntegrityIssue>,
) -> usize {
    let compressed = match unpack_object_expect_with_context(
        raw,
        ObjectType::ChunkData,
        chunk_id.as_bytes(),
        crypto,
    ) {
        Ok(bytes) => bytes,
        Err(e) => {
            issues.push(IntegrityIssue::CorruptChunk {
                chunk_id: *chunk_id,
                pack_id: *pack_id,
                detail: format!("decrypt failed: {e}"),
            });
            return 0;
        }
    };

    let plaintext = match compress::decompress(&compressed) {
        Ok(data) => data,
        Err(e) => {
            issues.push(IntegrityIssue::CorruptChunk {
                chunk_id: *chunk_id,
                pack_id: *pack_id,
                detail: format!("decompress failed: {e}"),
            });
            return 0;
        }
    };

    let recomputed = ChunkId::compute(chunk_id_key, &plaintext);
    if &recomputed != chunk_id {
        issues.push(IntegrityIssue::CorruptChunk {
            chunk_id: *chunk_id,
            pack_id: *pack_id,
            detail: format!("ID mismatch (recomputed {recomputed})"),
        });
        return 0;
    }

    1
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Per-pack scripted responses for `exists()`, keyed by storage key.
    struct ScriptedExistsBackend {
        responses: std::sync::Mutex<HashMap<String, std::result::Result<bool, String>>>,
    }

    impl StorageBackend for ScriptedExistsBackend {
        fn get(&self, _key: &str) -> vykar_types::error::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn put(&self, _key: &str, _data: &[u8]) -> vykar_types::error::Result<()> {
            Ok(())
        }
        fn delete(&self, _key: &str) -> vykar_types::error::Result<()> {
            Ok(())
        }
        fn exists(&self, key: &str) -> vykar_types::error::Result<bool> {
            match self.responses.lock().unwrap().get(key) {
                Some(Ok(b)) => Ok(*b),
                Some(Err(msg)) => Err(VykarError::Other(msg.clone())),
                None => Ok(true),
            }
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
    }

    #[test]
    fn parallel_pack_existence_excludes_io_failures_from_checked_count() {
        let present = PackId::from_bytes([0x01u8; 32]);
        let missing = PackId::from_bytes([0x02u8; 32]);
        let errored = PackId::from_bytes([0x03u8; 32]);

        let mut responses: HashMap<String, std::result::Result<bool, String>> = HashMap::new();
        responses.insert(present.storage_key(), Ok(true));
        responses.insert(missing.storage_key(), Ok(false));
        responses.insert(errored.storage_key(), Err("simulated I/O error".into()));

        let backend: Arc<dyn StorageBackend> = Arc::new(ScriptedExistsBackend {
            responses: std::sync::Mutex::new(responses),
        });

        let packs = vec![(present, 1), (missing, 1), (errored, 1)];
        let (checked, missing_count, issues) = parallel_pack_existence(&backend, &packs, 2);

        // Definitively-resolved packs only: present + missing. The I/O-errored pack
        // must NOT be counted, otherwise progress overstates coverage.
        assert_eq!(checked, 2, "checked should exclude I/O-errored packs");
        assert_eq!(missing_count, 1, "exactly one Ok(false) pack");
        assert_eq!(
            issues.len(),
            2,
            "expected MissingPack + PackExistenceCheckFailed"
        );
        assert!(issues.iter().any(|i| matches!(
            i,
            IntegrityIssue::MissingPack { pack_id } if *pack_id == missing
        )));
        assert!(issues.iter().any(|i| matches!(
            i,
            IntegrityIssue::PackExistenceCheckFailed { pack_id, .. } if *pack_id == errored
        )));
    }
}
