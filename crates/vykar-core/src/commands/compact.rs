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
use vykar_storage::{RepackBlobRef, RepackOperationRequest, RepackPlanRequest, StorageBackend};
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
                if idx >= discovered.len() {
                    break;
                }
                let (ref key, ref pack_id) = discovered[idx];

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

                        analyses_mu.lock().unwrap().push(PackAnalysis {
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
                        analyses_mu.lock().unwrap().push(PackAnalysis {
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

    let mut analyses = analyses_mu.into_inner().unwrap();

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
    analyses.sort_by(|a, b| b.dead_bytes.cmp(&a.dead_bytes));
    let selected = select_analyses_by_cap(&analyses, max_repack_size);

    if dry_run {
        for a in &selected {
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
        if header.len() != PACK_HEADER_SIZE
            || &header[..8] != PACK_MAGIC
            || header[8] < PACK_VERSION_MIN
            || header[8] > PACK_VERSION_MAX
        {
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
            let on_disk_len = u32::from_le_bytes(combined[..4].try_into().expect("4 bytes"));
            if on_disk_len != entry.length {
                return Err(VykarError::Other(format!(
                    "pack {}: blob at offset {} has on-disk length {} but index says {}; \
                     run `vykar check --verify-data`",
                    analysis.pack_id, entry.offset, on_disk_len, entry.length,
                )));
            }
            let blob_data = combined[4..].to_vec();

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
                        errors.lock().unwrap().push(e);
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
                    results.lock().unwrap().extend(local);
                }
            });
        }
    });

    let errs = errors.into_inner().unwrap();
    if let Some(first) = errs.into_iter().next() {
        return Err(first);
    }

    Ok(results.into_inner().unwrap())
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
    let plan = RepackPlanRequest {
        operations,
        protocol_version: vykar_storage::PROTOCOL_VERSION,
    };

    let response = match repo.storage.server_repack(&plan) {
        Ok(resp) => resp,
        Err(VykarError::UnsupportedBackend(_)) => return Ok(false),
        Err(err) => return Err(err),
    };

    let mut completed_by_source: HashMap<String, vykar_storage::RepackOperationResult> = response
        .completed
        .into_iter()
        .map(|op| (op.source_pack.clone(), op))
        .collect();

    for analysis in analyses {
        let result = completed_by_source
            .remove(&analysis.storage_key)
            .ok_or_else(|| {
                VykarError::Other(format!(
                    "server repack response missing operation for {}",
                    analysis.storage_key
                ))
            })?;

        if analysis.live_entries.is_empty() {
            if result.deleted {
                stats.packs_deleted_empty += 1;
                stats.space_freed += analysis.total_bytes;
            }
            continue;
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
    }

    repo.save_state()?;
    Ok(true)
}
