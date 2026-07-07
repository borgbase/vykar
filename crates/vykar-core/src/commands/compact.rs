use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use tracing::{info, warn};

use super::util::{check_interrupted, open_repo, with_maintenance_lock};
use crate::config::VykarConfig;
use crate::repo::pack::{
    PackType, PackWriter, PACK_HEADER_SIZE, PACK_MAGIC, PACK_VERSION_MAX, PACK_VERSION_MIN,
};
use crate::repo::OpenOptions;
use crate::repo::Repository;
use vykar_storage::{
    repack_op_output_size, RepackBlobRef, RepackOperationRequest, RepackPlanRequest,
    StorageBackend, MAX_REPACK_OUTPUT_BYTES,
};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// Statistics returned by the compact command.
#[derive(Debug, Default)]
pub struct CompactStats {
    pub packs_total: u64,
    pub packs_repacked: u64,
    pub packs_deleted_empty: u64,
    pub blobs_live: u64,
    pub space_freed: u64,
    pub packs_corrupt: u64,
    pub packs_orphan: u64,
}

/// A live blob entry with its location in the source pack.
struct LiveEntry {
    chunk_id: ChunkId,
    offset: u64,
    length: u32,
}

/// Per-pack analysis of live vs dead space.
struct PackAnalysis {
    pack_id: PackId,
    storage_key: String,
    live_entries: Vec<LiveEntry>,
    total_bytes: u64,
    dead_bytes: u64,
}

pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
    shutdown: Option<&AtomicBool>,
) -> Result<CompactStats> {
    let threshold = if !threshold.is_finite() || !(0.0..=100.0).contains(&threshold) {
        let default = config.compact.threshold;
        warn!(
            value = threshold,
            default, "compact threshold out of range (0–100), using config default"
        );
        default
    } else {
        threshold
    };

    let mut repo = open_repo(config, passphrase, OpenOptions::new().with_index())?;

    let is_remote =
        vykar_storage::parse_repo_url(&config.repository.url).is_ok_and(|u| !u.is_local());
    let concurrency = config.limits.listing_concurrency(is_remote);

    with_maintenance_lock(&mut repo, |repo| {
        compact_repo(
            repo,
            threshold,
            max_repack_size,
            dry_run,
            concurrency,
            shutdown,
        )
    })
}

/// Core compact logic operating on an already-opened repository.
pub fn compact_repo(
    repo: &mut Repository,
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
    concurrency: usize,
    shutdown: Option<&AtomicBool>,
) -> Result<CompactStats> {
    let mut stats = CompactStats::default();

    // Phase 1: Analyze live/dead space using the chunk index + pack sizes.
    //
    // Build a per-pack lookup from the chunk index:
    //   pack_id → Vec<(chunk_id, stored_size, pack_offset)>
    let mut per_pack_lookup: HashMap<PackId, Vec<(ChunkId, u32, u64)>> = HashMap::new();
    for (chunk_id, entry) in repo.chunk_index().iter() {
        per_pack_lookup.entry(entry.pack_id).or_default().push((
            *chunk_id,
            entry.stored_size,
            entry.pack_offset,
        ));
    }

    // Discover all packs on disk (256 LIST calls, parallelized).
    let discovered = parallel_list_packs(repo.storage.as_ref(), concurrency)?;

    // For each discovered pack, compute live/dead space using size() + index.
    let pack_header_size = PACK_HEADER_SIZE as u64;

    let analyses_mu: Mutex<Vec<PackAnalysis>> = Mutex::new(Vec::new());
    let packs_total = AtomicU64::new(0);
    let packs_corrupt = AtomicU64::new(0);
    let packs_orphan = AtomicU64::new(0);
    let blobs_live = AtomicU64::new(0);

    let storage = repo.storage.as_ref();
    let work_idx = std::sync::atomic::AtomicUsize::new(0);

    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| loop {
                if shutdown.is_some_and(|f| f.load(Ordering::Relaxed)) {
                    break;
                }
                let idx = work_idx.fetch_add(1, Ordering::Relaxed);
                let Some((key, pack_id)) = discovered.get(idx) else {
                    break;
                };

                packs_total.fetch_add(1, Ordering::Relaxed);

                // Get pack size via metadata-only call (HEAD, stat, fs::metadata).
                let pack_size = match storage.size(key) {
                    Ok(Some(sz)) => sz,
                    Ok(None) => {
                        // Pack disappeared between list and size.
                        warn!("Pack {} disappeared before size check, skipping", pack_id);
                        packs_corrupt.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Err(e) => {
                        warn!("Skipping pack {} (size check failed): {}", pack_id, e);
                        packs_corrupt.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                let index_entries = per_pack_lookup.get(pack_id);

                if let Some(entries) = index_entries {
                    // Case A: pack has index entries (normal pack).
                    let live_bytes: u64 = entries
                        .iter()
                        .map(|(_, stored_size, _)| 4 + *stored_size as u64)
                        .sum();

                    // Sanity check: live bytes can't exceed pack payload.
                    if pack_size < pack_header_size || live_bytes > pack_size - pack_header_size {
                        warn!(
                            "Pack {} has inconsistent size (pack_size={}, live_bytes={}, header={}), marking corrupt",
                            pack_id, pack_size, live_bytes, pack_header_size
                        );
                        packs_corrupt.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    let payload = pack_size - pack_header_size;
                    let dead_bytes = payload - live_bytes;

                    blobs_live.fetch_add(entries.len() as u64, Ordering::Relaxed);

                    if dead_bytes == 0 {
                        continue;
                    }

                    #[allow(
                        clippy::cast_precision_loss,
                        reason = "ratio for display, precision loss acceptable"
                    )]
                    let unused_ratio = if payload > 0 {
                        (dead_bytes as f64 / payload as f64) * 100.0
                    } else {
                        0.0
                    };

                    if unused_ratio >= threshold {
                        let mut live_entries: Vec<LiveEntry> = entries
                            .iter()
                            .map(|(chunk_id, stored_size, pack_offset)| LiveEntry {
                                chunk_id: *chunk_id,
                                offset: *pack_offset,
                                length: *stored_size,
                            })
                            .collect();
                        // Sort by offset for deterministic output and sequential reads.
                        live_entries.sort_by_key(|e| e.offset);

                        analyses_mu
                            .lock()
                            .expect("compact analyses lock not poisoned")
                            .push(PackAnalysis {
                            pack_id: *pack_id,
                            storage_key: key.clone(),
                            live_entries,
                            total_bytes: payload,
                            dead_bytes,
                        });
                    }
                } else {
                    // Case B: orphan pack (no index entries).
                    packs_orphan.fetch_add(1, Ordering::Relaxed);

                    let payload = pack_size.saturating_sub(pack_header_size);
                    if payload > 0 {
                        analyses_mu
                            .lock()
                            .expect("compact analyses lock not poisoned")
                            .push(PackAnalysis {
                            pack_id: *pack_id,
                            storage_key: key.clone(),
                            live_entries: Vec::new(),
                            total_bytes: payload,
                            dead_bytes: payload,
                        });
                    }
                }
            });
        }
    });

    stats.packs_total = packs_total.load(Ordering::Relaxed);
    stats.packs_corrupt = packs_corrupt.load(Ordering::Relaxed);
    stats.packs_orphan = packs_orphan.load(Ordering::Relaxed);
    stats.blobs_live = blobs_live.load(Ordering::Relaxed);

    // Bail before Phase 2 if shutdown was requested during analysis.
    check_interrupted(shutdown)?;

    let mut analyses = analyses_mu
        .into_inner()
        .expect("compact analyses lock not poisoned");

    if stats.packs_corrupt > 0 {
        tracing::debug!(
            "{} corrupt pack(s) found; run `vykar check --verify-data` for details",
            stats.packs_corrupt
        );
    }
    if stats.packs_orphan > 0 {
        tracing::debug!(
            "{} orphan pack(s) found (present on disk but not referenced by index)",
            stats.packs_orphan
        );
    }

    // Sort by most wasteful first (highest dead bytes)
    analyses.sort_by_key(|a| Reverse(a.dead_bytes));
    let selected = select_analyses_by_cap(&analyses, max_repack_size);

    if dry_run {
        for a in &selected {
            #[allow(
                clippy::cast_precision_loss,
                reason = "ratio for display, precision loss acceptable"
            )]
            let pct = (a.dead_bytes as f64 / a.total_bytes as f64) * 100.0;
            if a.live_entries.is_empty() {
                info!(
                    "Would delete empty pack {} ({:.1}% unused, {} dead bytes)",
                    a.pack_id, pct, a.dead_bytes,
                );
                stats.packs_deleted_empty += 1;
            } else {
                info!(
                    "Would repack {} ({:.1}% unused, {} live blobs, {} dead bytes)",
                    a.pack_id,
                    pct,
                    a.live_entries.len(),
                    a.dead_bytes,
                );
                stats.packs_repacked += 1;
            }
            stats.space_freed += a.dead_bytes;
        }
        return Ok(stats);
    }

    if try_server_side_repack(repo, &selected, &mut stats)? {
        return Ok(stats);
    }

    // Proactive fence check at Phase 1→Phase 2 boundary.
    repo.check_lock_fence()?;

    // Phase 2: Repack
    let mut total_repacked_bytes: u64 = 0;
    let pack_target = repo.config.min_pack_size as usize;

    for analysis in &selected {
        check_interrupted(shutdown)?;
        if let Some(cap) = max_repack_size {
            if total_repacked_bytes >= cap {
                info!("Reached max-repack-size limit, stopping");
                break;
            }
        }

        if analysis.live_entries.is_empty() {
            info!("Deleting empty pack {}", analysis.pack_id);
            repo.storage.delete(&analysis.storage_key)?;
            stats.packs_deleted_empty += 1;
            stats.space_freed += analysis.total_bytes;
            continue;
        }

        info!(
            "Repacking {} ({} live blobs)",
            analysis.pack_id,
            analysis.live_entries.len(),
        );

        // Validate pack header before trusting any blob offsets. Phase 1 only
        // checked the file size, so a corrupt/truncated pack could slip through.
        let header = repo
            .storage
            .get_range(&analysis.storage_key, 0, PACK_HEADER_SIZE as u64)?
            .ok_or_else(|| {
                VykarError::Other(format!(
                    "pack {} disappeared during repack",
                    analysis.pack_id
                ))
            })?;
        // Header was just read at exactly PACK_HEADER_SIZE bytes, so the
        // length-mismatch arm filters everything else; the slicing/indexing
        // is in-bounds for any input that reaches the magic/version checks.
        #[allow(clippy::indexing_slicing)]
        let header_invalid = header.len() != PACK_HEADER_SIZE
            || &header[..8] != PACK_MAGIC
            || header[8] < PACK_VERSION_MIN
            || header[8] > PACK_VERSION_MAX;
        if header_invalid {
            warn!(
                "Skipping pack {} with invalid header during repack",
                analysis.pack_id
            );
            stats.packs_corrupt += 1;
            continue;
        }

        let mut writer = PackWriter::new(PackType::Data, pack_target);

        for entry in &analysis.live_entries {
            // Read the 4-byte length prefix together with the blob data in a
            // single range read. This cross-checks the on-disk length prefix
            // against the index's stored_size, guarding against stale/corrupt
            // index entries that could silently produce bad blobs.
            let prefix_offset = entry.offset.checked_sub(4).ok_or_else(|| {
                VykarError::Other(format!(
                    "pack {}: blob at offset {} has no room for length prefix",
                    analysis.pack_id, entry.offset,
                ))
            })?;
            let read_len = 4u64 + entry.length as u64;
            let combined = repo
                .storage
                .get_range(&analysis.storage_key, prefix_offset, read_len)?
                .ok_or_else(|| {
                    VykarError::Other(format!("pack not found: {}", analysis.pack_id))
                })?;
            if combined.len() != read_len as usize {
                return Err(VykarError::Other(format!(
                    "short read from pack {} at offset {}: expected {} bytes, got {}",
                    analysis.pack_id,
                    prefix_offset,
                    read_len,
                    combined.len()
                )));
            }
            let (len_bytes, blob_bytes) = combined.split_at(4);
            let on_disk_len = u32::from_le_bytes(
                len_bytes
                    .try_into()
                    .expect("combined.len() == 4 + entry.length (checked above)"),
            );
            if on_disk_len != entry.length {
                return Err(VykarError::Other(format!(
                    "pack {}: blob at offset {} has on-disk length {} but index says {}; \
                     run `vykar check --verify-data`",
                    analysis.pack_id, entry.offset, on_disk_len, entry.length,
                )));
            }
            let blob_data = blob_bytes.to_vec();

            writer.add_blob(entry.chunk_id, blob_data)?;
        }

        let (new_pack_id, new_entries) = writer.flush(repo.storage.as_ref())?;

        for (chunk_id, stored_size, offset, _refcount) in &new_entries {
            repo.chunk_index_mut()
                .update_location(chunk_id, new_pack_id, *offset, *stored_size);
        }

        // Save state BEFORE deleting old pack (crash safety)
        repo.save_state()?;

        repo.storage.delete(&analysis.storage_key)?;

        stats.packs_repacked += 1;
        stats.space_freed += analysis.dead_bytes;
        total_repacked_bytes += analysis.total_bytes;
    }

    if stats.packs_repacked > 0 || stats.packs_deleted_empty > 0 {
        repo.save_state()?;
    }

    Ok(stats)
}

/// Discover all pack files across 256 shard directories, in parallel.
fn parallel_list_packs(
    storage: &dyn StorageBackend,
    concurrency: usize,
) -> Result<Vec<(String, PackId)>> {
    let results: Mutex<Vec<(String, PackId)>> = Mutex::new(Vec::new());
    let errors: Mutex<Vec<VykarError>> = Mutex::new(Vec::new());
    let shard_idx = std::sync::atomic::AtomicU16::new(0);

    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| loop {
                let shard = shard_idx.fetch_add(1, Ordering::Relaxed);
                if shard >= 256 {
                    break;
                }
                let prefix = format!("packs/{:02x}/", shard);
                let keys = match storage.list(&prefix) {
                    Ok(k) => k,
                    Err(e) => {
                        errors
                            .lock()
                            .expect("compact list-pack errors lock not poisoned")
                            .push(e);
                        continue;
                    }
                };

                let mut local = Vec::new();
                for key in keys {
                    if key.ends_with('/') {
                        continue;
                    }
                    match PackId::from_storage_key(&key) {
                        Ok(id) => local.push((key, id)),
                        Err(e) => {
                            warn!("Skipping invalid pack key '{}': {}", key, e);
                        }
                    }
                }
                if !local.is_empty() {
                    results
                        .lock()
                        .expect("compact list-pack results lock not poisoned")
                        .extend(local);
                }
            });
        }
    });

    let errs = errors
        .into_inner()
        .expect("compact list-pack errors lock not poisoned");
    if let Some(first) = errs.into_iter().next() {
        return Err(first);
    }

    Ok(results
        .into_inner()
        .expect("compact list-pack results lock not poisoned"))
}

fn select_analyses_by_cap(
    analyses: &[PackAnalysis],
    max_repack_size: Option<u64>,
) -> Vec<&PackAnalysis> {
    let mut selected = Vec::new();
    let mut total = 0u64;

    for analysis in analyses {
        if let Some(cap) = max_repack_size {
            if total >= cap {
                break;
            }
        }
        selected.push(analysis);
        total = total.saturating_add(analysis.total_bytes);
    }

    selected
}

fn try_server_side_repack(
    repo: &mut Repository,
    analyses: &[&PackAnalysis],
    stats: &mut CompactStats,
) -> Result<bool> {
    if analyses.is_empty() {
        return Ok(true);
    }

    let analysis_by_key: HashMap<&str, &PackAnalysis> = analyses
        .iter()
        .map(|analysis| (analysis.storage_key.as_str(), *analysis))
        .collect();

    let mut operations = Vec::with_capacity(analyses.len());
    for analysis in analyses {
        operations.push(RepackOperationRequest {
            source_pack: analysis.storage_key.clone(),
            keep_blobs: analysis
                .live_entries
                .iter()
                .map(|entry| RepackBlobRef {
                    offset: entry.offset,
                    length: entry.length as u64,
                })
                .collect(),
            delete_after: true,
        });
    }

    // A single operation over the server's output cap cannot be split further
    // (one source pack = one op) and would be rejected with 400, so fall back
    // to client-side repack instead of sending a doomed plan. Pack size limits
    // keep this from happening in practice.
    if operations
        .iter()
        .any(|op| repack_op_output_size(op) > MAX_REPACK_OUTPUT_BYTES)
    {
        return Ok(false);
    }

    // Send one server_repack call per batch (batches stay within the server's
    // output cap; client and server agree via MAX_REPACK_OUTPUT_BYTES), and
    // apply each batch's results to the index as it completes. The server
    // deletes source packs as it goes, so results already in hand must be
    // persisted even when a later batch fails — otherwise the index would keep
    // pointing at packs the server has already deleted.
    let mut applied_any = false;
    let mut first_batch = true;
    let mut batch_err: Option<VykarError> = None;

    'batches: for batch in chunk_repack_operations(operations) {
        let plan = RepackPlanRequest {
            operations: batch,
            protocol_version: vykar_storage::PROTOCOL_VERSION,
        };
        let response = match repo.storage.server_repack(&plan) {
            Ok(resp) => resp,
            // Backend capability is stable, so only the first call can discover
            // server-side repack is unsupported. After a successful batch a
            // client-side fallback would try to re-read source packs the server
            // has already deleted, so later errors must propagate instead.
            Err(VykarError::UnsupportedBackend(_)) if first_batch => return Ok(false),
            Err(err) => {
                batch_err = Some(err);
                break 'batches;
            }
        };
        first_batch = false;

        let mut completed_by_source: HashMap<String, vykar_storage::RepackOperationResult> =
            response
                .completed
                .into_iter()
                .map(|op| (op.source_pack.clone(), op))
                .collect();
        for op in &plan.operations {
            let Some(result) = completed_by_source.remove(&op.source_pack) else {
                batch_err = Some(VykarError::Other(format!(
                    "server repack response missing operation for {}",
                    op.source_pack
                )));
                break 'batches;
            };
            let analysis = analysis_by_key
                .get(op.source_pack.as_str())
                .expect("operation was built from these analyses");
            if let Err(err) = apply_repack_result(repo, analysis, &result, stats) {
                batch_err = Some(err);
                break 'batches;
            }
            applied_any = true;
        }
    }

    // Persist index updates for every applied operation, even on a partial
    // failure — their source packs are already gone server-side.
    if applied_any || batch_err.is_none() {
        repo.save_state()?;
    }
    match batch_err {
        Some(err) => Err(err),
        None => Ok(true),
    }
}

/// Apply one completed server-side repack operation to the local chunk index
/// and stats. Errors (missing/invalid new pack, offsets mismatch) leave the
/// index untouched for this operation.
fn apply_repack_result(
    repo: &mut Repository,
    analysis: &PackAnalysis,
    result: &vykar_storage::RepackOperationResult,
    stats: &mut CompactStats,
) -> Result<()> {
    if analysis.live_entries.is_empty() {
        if result.deleted {
            stats.packs_deleted_empty += 1;
            stats.space_freed += analysis.total_bytes;
        }
        return Ok(());
    }

    let new_pack_key = result.new_pack.as_ref().ok_or_else(|| {
        VykarError::Other(format!(
            "server repack did not return new pack for {}",
            analysis.storage_key
        ))
    })?;
    let new_pack_id = PackId::from_storage_key(new_pack_key).map_err(|e| {
        VykarError::Other(format!(
            "server repack returned invalid pack key '{new_pack_key}': {e}"
        ))
    })?;

    if result.new_offsets.len() != analysis.live_entries.len() {
        return Err(VykarError::Other(format!(
            "server repack offsets mismatch for {}: expected {}, got {}",
            analysis.storage_key,
            analysis.live_entries.len(),
            result.new_offsets.len()
        )));
    }

    for (entry, new_offset) in analysis.live_entries.iter().zip(result.new_offsets.iter()) {
        repo.chunk_index_mut().update_location(
            &entry.chunk_id,
            new_pack_id,
            *new_offset,
            entry.length,
        );
    }

    stats.packs_repacked += 1;
    if result.deleted {
        stats.space_freed += analysis.dead_bytes;
    }
    Ok(())
}

/// Split repack operations into batches whose summed output stays within
/// [`MAX_REPACK_OUTPUT_BYTES`]. A single operation over the cap is caught
/// before batching (client-side repack fallback in `try_server_side_repack`),
/// so every batch produced here is within the server's limit.
fn chunk_repack_operations(
    operations: Vec<RepackOperationRequest>,
) -> Vec<Vec<RepackOperationRequest>> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_size = 0u64;
    for op in operations {
        let size = repack_op_output_size(&op);
        if !current.is_empty() && current_size.saturating_add(size) > MAX_REPACK_OUTPUT_BYTES {
            batches.push(std::mem::take(&mut current));
            current_size = 0;
        }
        current_size = current_size.saturating_add(size);
        current.push(op);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

#[cfg(test)]
mod repack_chunk_tests {
    use super::chunk_repack_operations;
    use vykar_storage::{
        repack_op_output_size, RepackBlobRef, RepackOperationRequest, MAX_REPACK_OUTPUT_BYTES,
    };

    fn op_with_blob(length: u64) -> RepackOperationRequest {
        RepackOperationRequest {
            source_pack: format!("packs/ab/{}", "ab".repeat(32)),
            keep_blobs: vec![RepackBlobRef { offset: 13, length }],
            delete_after: true,
        }
    }

    #[test]
    fn empty_input_yields_no_batches() {
        assert!(chunk_repack_operations(vec![]).is_empty());
    }

    #[test]
    fn small_ops_stay_in_one_batch() {
        let ops = vec![op_with_blob(100), op_with_blob(200), op_with_blob(300)];
        let batches = chunk_repack_operations(ops);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 3);
    }

    #[test]
    fn ops_straddling_cap_split_into_batches() {
        // Two ops each just over half the cap must not share a batch.
        let half_plus = MAX_REPACK_OUTPUT_BYTES / 2 + 1;
        let ops = vec![op_with_blob(half_plus), op_with_blob(half_plus)];
        let batches = chunk_repack_operations(ops);
        assert_eq!(
            batches.len(),
            2,
            "each op exceeds half the cap -> own batch"
        );
        for batch in &batches {
            let total: u64 = batch.iter().map(repack_op_output_size).sum();
            assert!(total <= MAX_REPACK_OUTPUT_BYTES, "batch within cap");
        }
    }

    #[test]
    fn single_oversize_op_is_its_own_batch() {
        let ops = vec![
            op_with_blob(10),
            op_with_blob(MAX_REPACK_OUTPUT_BYTES + 1),
            op_with_blob(10),
        ];
        let batches = chunk_repack_operations(ops);
        assert_eq!(batches.len(), 3, "oversize op isolates the ops around it");
    }
}
