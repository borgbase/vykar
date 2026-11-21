use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{info, warn};

use super::util::{open_repo, with_maintenance_lock};
use crate::config::VykarConfig;
use crate::repo::OpenOptions;
use crate::repo::Repository;
use crate::repo::control::CancelToken;
use crate::repo::pack::{PackType, PackWriter};
use vykar_protocol::{
    MAX_REPACK_OUTPUT_BYTES, PACK_HEADER_SIZE, RepackBlobRef, RepackOperationRequest,
    RepackPlanRequest, repack_op_output_size, validate_pack_header,
};
use vykar_storage::StorageBackend;
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
    /// Stale `.tmp.*` upload debris removed by the sweep.
    pub temp_files_removed: u64,
}

const STALE_TEMP_FILE_AGE: std::time::Duration = std::time::Duration::from_secs(3600);

/// Client-side repack persists the index once per batch of source packs (by
/// count or by their bytes) and only then deletes that batch's sources.
const SAVE_BATCH_PACKS: usize = 64;
const SAVE_BATCH_BYTES: u64 = 4 << 30;

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
    shutdown: &CancelToken,
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

    // The backend observes `shutdown` in its retry waits. `persist_and_delete`
    // does not poll it, so after a cancel its requests get one attempt each
    // and no retries, and on SFTP a read in flight is abandoned; it is
    // crash-safe by design.
    let mut repo = open_repo(config, passphrase, OpenOptions::new(), shutdown)?;
    run_on_repo(&mut repo, threshold, max_repack_size, dry_run, shutdown)
}

/// [`run`] on an already opened repository, with a sanitised `threshold`.
/// The chunk index is loaded under the maintenance lock, so any index loaded
/// at open is replaced.
pub(crate) fn run_on_repo(
    repo: &mut Repository,
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
    shutdown: &CancelToken,
) -> Result<CompactStats> {
    let concurrency = repo.storage.profile().request_width;

    with_maintenance_lock(repo, |repo| {
        repo.load_chunk_index_under_lock()?;
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
///
/// Ends, unless `dry_run`, with the segmented index's upkeep: a fold at the
/// soft limit, otherwise removal of unreferenced index objects. That also
/// runs when there was nothing to repack.
pub fn compact_repo(
    repo: &mut Repository,
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
    concurrency: usize,
    shutdown: &CancelToken,
) -> Result<CompactStats> {
    let stats = compact_packs(
        repo,
        threshold,
        max_repack_size,
        dry_run,
        concurrency,
        shutdown,
    )?;
    if !dry_run {
        repo.maintain_segmented_index()?;
    }
    Ok(stats)
}

fn compact_packs(
    repo: &mut Repository,
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
    concurrency: usize,
    shutdown: &CancelToken,
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

    // Sweep upload debris once, from the root, before listing shards. Temp
    // files younger than this may belong to a live upload.
    if !dry_run {
        match repo.storage.remove_stale_temp_files(STALE_TEMP_FILE_AGE) {
            Ok(n) => stats.temp_files_removed = n as u64,
            Err(e) => tracing::warn!("compact: failed to sweep stale temp files: {e}"),
        }
    }

    // Discover all packs: one listing on a vykar-server, else 256 per-shard
    // listings in parallel.
    let discovered = list_packs(repo.storage.as_ref(), concurrency)?;

    // For each discovered pack, compute live/dead space from its size and the
    // index. The size comes from the listing; `size()` only where it has none.
    let pack_header_size = PACK_HEADER_SIZE as u64;

    let analyses_mu: Mutex<Vec<PackAnalysis>> = Mutex::new(Vec::new());
    let packs_total = AtomicU64::new(0);
    let packs_corrupt = AtomicU64::new(0);
    let packs_orphan = AtomicU64::new(0);
    let blobs_live = AtomicU64::new(0);

    let storage = repo.storage.as_ref();
    crate::repo::bounded::for_each_bounded(concurrency, &discovered, shutdown, |p: &ListedPack| {
        packs_total.fetch_add(1, Ordering::Relaxed);
        let (key, pack_id) = (&p.key, &p.pack_id);

        // Sized listings (S3, SFTP, local, REST >= 0.22) need no extra request.
        let pack_size = match p.size {
            Some(sz) => sz,
            None => match storage.size(key) {
                Ok(Some(sz)) => sz,
                Ok(None) => {
                    // Pack disappeared between list and size.
                    warn!("Pack {} disappeared before size check, skipping", pack_id);
                    packs_corrupt.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(e) => {
                    warn!("Skipping pack {} (size check failed): {}", pack_id, e);
                    packs_corrupt.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            },
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
                return;
            }

            let payload = pack_size - pack_header_size;
            let dead_bytes = payload - live_bytes;

            blobs_live.fetch_add(entries.len() as u64, Ordering::Relaxed);

            if dead_bytes == 0 {
                return;
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

    stats.packs_total = packs_total.load(Ordering::Relaxed);
    stats.packs_corrupt = packs_corrupt.load(Ordering::Relaxed);
    stats.packs_orphan = packs_orphan.load(Ordering::Relaxed);
    stats.blobs_live = blobs_live.load(Ordering::Relaxed);

    // Bail before Phase 2 if shutdown was requested during analysis.
    shutdown.check()?;

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

    // Phase 2: Repack. Source packs, including empty ones, are deleted per
    // batch, after the index naming their replacements is persisted, so no
    // pack is deleted while the index on storage still references it.
    let mut total_repacked_bytes: u64 = 0;
    let pack_target = repo.config.min_pack_size as usize;
    let mut batch: Vec<&PackAnalysis> = Vec::new();
    let mut batch_bytes: u64 = 0;

    for analysis in &selected {
        shutdown.check()?;
        if let Some(cap) = max_repack_size
            && total_repacked_bytes >= cap
        {
            info!("Reached max-repack-size limit, stopping");
            break;
        }
        if batch.len() >= SAVE_BATCH_PACKS || batch_bytes >= SAVE_BATCH_BYTES {
            persist_and_delete(repo, &mut batch, &mut stats)?;
            batch_bytes = 0;
        }

        if analysis.live_entries.is_empty() {
            info!("Deleting empty pack {}", analysis.pack_id);
            batch.push(analysis);
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
        if validate_pack_header(&header).is_err() {
            warn!(
                "Skipping pack {} with invalid header during repack",
                analysis.pack_id
            );
            stats.packs_corrupt += 1;
            continue;
        }

        let mut writer = PackWriter::new(PackType::Data, pack_target, repo.content_hash());

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

        stats.packs_repacked += 1;
        total_repacked_bytes += analysis.total_bytes;
        batch_bytes += analysis.total_bytes;
        batch.push(analysis);
    }
    persist_and_delete(repo, &mut batch, &mut stats)?;

    Ok(stats)
}

/// Persist the index if it changed (crash safety), then delete `batch`'s
/// source packs and empty the batch.
fn persist_and_delete(
    repo: &mut Repository,
    batch: &mut Vec<&PackAnalysis>,
    stats: &mut CompactStats,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    repo.save_state()?;
    let err = delete_sources(repo.storage.as_ref(), batch, stats);
    batch.clear();
    err.map_or(Ok(()), Err)
}

/// A pack found by [`list_packs`]; `size` is `None` when the listing did not
/// report it.
struct ListedPack {
    key: String,
    pack_id: PackId,
    size: Option<u64>,
}

/// Directory under which pack shards live.
const PACKS_PREFIX: &str = "packs/";

/// Every pack in the 256 shard directories, with its size where the listing
/// reports it. One request where the backend lists a whole tree at once
/// (vykar-server); otherwise, or if that request fails, one listing per shard.
fn list_packs(storage: &dyn StorageBackend, concurrency: usize) -> Result<Vec<ListedPack>> {
    match storage.list_tree_sized(PACKS_PREFIX) {
        Ok(objects) => {
            let mut packs = Vec::with_capacity(objects.len());
            push_listed(
                objects.into_iter().filter(|o| in_pack_shard(&o.key)),
                &mut packs,
            );
            Ok(packs)
        }
        Err(VykarError::UnsupportedBackend(_)) => parallel_list_packs(storage, concurrency),
        // A cancelled listing is not a reason to list every shard instead.
        Err(e @ VykarError::Interrupted) => Err(e),
        Err(e) => {
            warn!("compact: one-request pack listing failed ({e}); listing per shard");
            parallel_list_packs(storage, concurrency)
        }
    }
}

/// Whether `key` lies below one of the shard directories `packs/00/` ..
/// `packs/ff/`, i.e. whether a per-shard listing would have returned it.
fn in_pack_shard(key: &str) -> bool {
    let Some(rest) = key.strip_prefix(PACKS_PREFIX) else {
        return false;
    };
    let mut bytes = rest.bytes();
    let hex = |b: Option<u8>| matches!(b, Some(b'0'..=b'9' | b'a'..=b'f'));
    hex(bytes.next()) && hex(bytes.next()) && bytes.next() == Some(b'/')
}

/// Turn listed objects into packs: directory markers are skipped, and
/// non-pack keys are logged and skipped.
fn push_listed(
    objects: impl IntoIterator<Item = vykar_storage::ListedObject>,
    out: &mut Vec<ListedPack>,
) {
    for vykar_storage::ListedObject { key, size } in objects {
        if key.ends_with('/') {
            continue;
        }
        match PackId::from_storage_key(&key) {
            Ok(pack_id) => out.push(ListedPack { key, pack_id, size }),
            Err(e) => warn!("Skipping invalid pack key '{}': {}", key, e),
        }
    }
}

/// Discover all pack files across 256 shard directories, in parallel, with
/// their sizes where the backend's listing reports them. The fallback of
/// [`list_packs`].
fn parallel_list_packs(
    storage: &dyn StorageBackend,
    concurrency: usize,
) -> Result<Vec<ListedPack>> {
    let results: Mutex<Vec<ListedPack>> = Mutex::new(Vec::new());
    let errors: Mutex<Vec<VykarError>> = Mutex::new(Vec::new());
    let shards: Vec<u16> = (0..256).collect();

    // Not cancellable: `shutdown` is honoured by the analysis that follows.
    crate::repo::bounded::for_each_bounded(concurrency, &shards, &CancelToken::new(), |&shard| {
        let prefix = format!("packs/{:02x}/", shard);
        let objects = match storage.list_sized(&prefix) {
            Ok(k) => k,
            Err(e) => {
                errors
                    .lock()
                    .expect("compact list-pack errors lock not poisoned")
                    .push(e);
                return;
            }
        };

        let mut local = Vec::new();
        push_listed(objects, &mut local);
        if !local.is_empty() {
            results
                .lock()
                .expect("compact list-pack results lock not poisoned")
                .extend(local);
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
        if let Some(cap) = max_repack_size
            && total >= cap
        {
            break;
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

    // The server never deletes: sources are removed below, after the index
    // pointing at their replacements is persisted. A lost or timed-out
    // response therefore leaves only unreferenced new packs behind.
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
            delete_after: false,
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
    // output cap; client and server agree via MAX_REPACK_OUTPUT_BYTES). Each
    // batch is applied, persisted, and only then are its sources deleted, so a
    // crash at any point leaves the index consistent.
    let mut first_batch = true;
    // Packs returned as some op's new pack carry live data and must never be
    // deleted as a source (the server may return the source itself).
    let mut result_packs: HashSet<String> = HashSet::new();

    for batch in chunk_repack_operations(operations) {
        // The constructor derives `protocol_version` from the algorithm, so an
        // old server rejects a BLAKE3 plan outright instead of verifying the
        // packs under the wrong algorithm.
        let plan = RepackPlanRequest::new(batch, repo.content_hash());
        let response = match repo.storage.server_repack(&plan) {
            Ok(resp) => resp,
            // Backend capability is stable, so only the first call can discover
            // server-side repack is unsupported. After a successful batch a
            // client-side fallback would redo work already applied.
            Err(VykarError::UnsupportedBackend(_)) if first_batch => return Ok(false),
            Err(err) => return Err(err),
        };
        first_batch = false;

        result_packs.extend(
            response
                .completed
                .iter()
                .filter_map(|op| op.new_pack.clone()),
        );
        let mut completed_by_source: HashMap<String, vykar_protocol::RepackOperationResult> =
            response
                .completed
                .into_iter()
                .map(|op| (op.source_pack.clone(), op))
                .collect();

        let mut applied: Vec<&PackAnalysis> = Vec::with_capacity(plan.operations.len());
        let mut batch_err: Option<VykarError> = None;
        for op in &plan.operations {
            let Some(result) = completed_by_source.remove(&op.source_pack) else {
                batch_err = Some(VykarError::Other(format!(
                    "server repack response missing operation for {}",
                    op.source_pack
                )));
                break;
            };
            let analysis = analysis_by_key
                .get(op.source_pack.as_str())
                .expect("operation was built from these analyses");
            if let Err(err) = apply_repack_result(repo, analysis, &result, stats) {
                batch_err = Some(err);
                break;
            }
            applied.push(analysis);
        }

        if applied.is_empty() {
            if let Some(err) = batch_err {
                return Err(err);
            }
            continue;
        }
        repo.save_state()?;

        // A failed delete leaves an unreferenced source behind (wasted space,
        // not an inconsistency). Finish this batch's deletes, then stop: on an
        // append-only server every later batch would only grow the repo.
        let sources: Vec<&PackAnalysis> = applied
            .into_iter()
            .filter(|a| !result_packs.contains(&a.storage_key))
            .collect();
        let delete_err = delete_sources(repo.storage.as_ref(), &sources, stats);
        if let Some(err) = batch_err.or(delete_err) {
            return Err(err);
        }
    }

    Ok(true)
}

/// Delete repacked source packs: one batch request where the backend has
/// one, else (or if the batch fails) one `delete` per pack, so every outcome
/// is attributed to its pack exactly as without the batch. Returns the first
/// per-pack error.
fn delete_sources(
    storage: &dyn StorageBackend,
    sources: &[&PackAnalysis],
    stats: &mut CompactStats,
) -> Option<VykarError> {
    if sources.is_empty() {
        return None;
    }
    let keys: Vec<String> = sources.iter().map(|a| a.storage_key.clone()).collect();
    match storage.batch_delete_keys(&keys, vykar_storage::BatchDeleteScope::KeysOnly) {
        Ok(()) => {
            for analysis in sources {
                record_deleted(analysis, stats);
            }
            return None;
        }
        Err(VykarError::UnsupportedBackend(_)) => {}
        Err(e) => warn!(
            "compact: batch delete of {} source pack(s) failed ({e}); deleting one by one",
            keys.len()
        ),
    }
    let mut first_err = None;
    for analysis in sources {
        match storage.delete(&analysis.storage_key) {
            Ok(()) => record_deleted(analysis, stats),
            Err(err) => {
                warn!(
                    "Failed to delete repacked source pack {}: {err}",
                    analysis.pack_id
                );
                first_err.get_or_insert(err);
            }
        }
    }
    first_err
}

/// Stats for one deleted source pack.
fn record_deleted(analysis: &PackAnalysis, stats: &mut CompactStats) {
    if analysis.live_entries.is_empty() {
        stats.packs_deleted_empty += 1;
        stats.space_freed += analysis.total_bytes;
    } else {
        stats.space_freed += analysis.dead_bytes;
    }
}

/// Apply one completed server-side repack operation to the local chunk index
/// and stats. Errors (missing/invalid new pack, offsets mismatch) leave the
/// index untouched for this operation. Source deletion is the caller's job.
fn apply_repack_result(
    repo: &mut Repository,
    analysis: &PackAnalysis,
    result: &vykar_protocol::RepackOperationResult,
    stats: &mut CompactStats,
) -> Result<()> {
    if analysis.live_entries.is_empty() {
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
mod listing_tests {
    use super::in_pack_shard;

    #[test]
    fn in_pack_shard_matches_exactly_the_shard_directories() {
        let hex = "ab".repeat(32);
        for key in [
            "packs/00/x".to_string(),
            format!("packs/ff/{hex}"),
            "packs/ab/nested/x".to_string(),
        ] {
            assert!(in_pack_shard(&key), "{key}");
        }
        for key in [
            format!("packs/{hex}"),
            "packs/AB/x".to_string(),
            "packs/abc/x".to_string(),
            "packs/ab".to_string(),
            "packs/a/x".to_string(),
            "packsab/x".to_string(),
            "index".to_string(),
            "snapshots/ab/x".to_string(),
        ] {
            assert!(!in_pack_shard(&key), "{key}");
        }
    }
}

#[cfg(test)]
mod repack_chunk_tests {
    use super::chunk_repack_operations;
    use vykar_protocol::{
        MAX_REPACK_OUTPUT_BYTES, RepackBlobRef, RepackOperationRequest, repack_op_output_size,
    };

    fn op_with_blob(length: u64) -> RepackOperationRequest {
        RepackOperationRequest {
            source_pack: format!("packs/ab/{}", "ab".repeat(32)),
            keep_blobs: vec![RepackBlobRef { offset: 13, length }],
            delete_after: false,
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
