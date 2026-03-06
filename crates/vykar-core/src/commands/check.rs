use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::compress;
use crate::config::VykarConfig;
use crate::index::ChunkIndexEntry;
use crate::repo::format::{unpack_object_expect_with_context, ObjectType};
use crate::repo::pack::{
    read_blob_from_pack, PACK_HEADER_SIZE, PACK_MAGIC, PACK_VERSION_MAX, PACK_VERSION_MIN,
};
use crate::snapshot::item::ItemType;
use vykar_crypto::CryptoEngine;
use vykar_storage::{
    StorageBackend, VerifyBlobRef, VerifyPackRequest, VerifyPacksPlanRequest, VerifyPacksResponse,
    PROTOCOL_VERSION,
};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::list::{for_each_decoded_item, load_snapshot_item_stream, load_snapshot_meta};

/// Number of chunks in a pack before we download the full pack instead of
/// issuing individual range reads.
const BATCH_THRESHOLD: usize = 3;
/// Maximum packs per server-side verify-packs request.
/// Guards against huge fanout on repos with many tiny packs.
const SERVER_VERIFY_BATCH_SIZE: usize = 100;
/// Maximum estimated bytes of pack data per server-side verify request.
/// At 200 MB/s (HDD) this is ~10s of server I/O; at 500 MB/s (SSD) ~4s.
const SERVER_VERIFY_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

/// A single integrity issue found during check.
#[derive(Debug)]
pub struct CheckError {
    pub context: String,
    pub message: String,
}

/// Summary of a check run.
pub struct CheckResult {
    pub snapshots_checked: usize,
    pub items_checked: usize,
    pub chunks_existence_checked: usize,
    pub packs_existence_checked: usize,
    pub chunks_data_verified: usize,
    pub errors: Vec<CheckError>,
}

#[derive(Debug, Clone)]
pub enum CheckProgressEvent {
    SnapshotStarted {
        current: usize,
        total: usize,
        name: String,
    },
    PacksExistencePhaseStarted {
        total_packs: usize,
    },
    PacksExistenceProgress {
        checked: usize,
        total_packs: usize,
    },
    ChunksDataPhaseStarted {
        total_chunks: usize,
    },
    ChunksDataProgress {
        verified: usize,
        total_chunks: usize,
    },
    ServerVerifyPhaseStarted {
        total_packs: usize,
    },
    ServerVerifyProgress {
        verified: usize,
        total_packs: usize,
    },
}

fn emit_progress(
    progress: &mut Option<&mut dyn FnMut(CheckProgressEvent)>,
    event: CheckProgressEvent,
) {
    if let Some(callback) = progress.as_deref_mut() {
        callback(event);
    }
}

/// Outcome of attempting server-side pack verification.
#[allow(dead_code)] // packs_passed is used in tests only
pub(crate) enum ServerVerifyOutcome {
    /// Server handled some or all packs. `verified_packs` is the set that was
    /// actually checked; any packs not in this set still need client-side work.
    Ok {
        errors: Vec<CheckError>,
        packs_responded: usize,
        packs_passed: usize,
        chunks_verified: usize,
        verified_packs: HashSet<PackId>,
    },
    /// Server doesn't support verify-packs at all — fall back entirely.
    Fallback,
}

/// Result of processing a single batch of server verify responses.
pub(crate) struct ProcessedVerifyResult {
    pub packs_responded: usize,
    pub packs_passed: usize,
    pub chunks_verified: usize,
}

/// Run `vykar check`.
pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    verify_data: bool,
    distrust_server: bool,
) -> Result<CheckResult> {
    run_with_progress(config, passphrase, verify_data, distrust_server, None)
}

pub fn run_with_progress(
    config: &VykarConfig,
    passphrase: Option<&str>,
    verify_data: bool,
    distrust_server: bool,
    mut progress: Option<&mut dyn FnMut(CheckProgressEvent)>,
) -> Result<CheckResult> {
    let (mut repo, _session_guard) =
        super::util::open_repo_with_read_session(config, passphrase, true, false)?;
    repo.load_chunk_index_uncached()?;

    let mut errors: Vec<CheckError> = Vec::new();
    let mut snapshots_checked: usize = 0;
    let mut items_checked: usize = 0;

    // Detect if this is a remote backend (REST/S3/SFTP) for concurrency tuning.
    let is_remote = matches!(
        vykar_storage::parse_repo_url(&config.repository.url),
        Ok(vykar_storage::ParsedUrl::Rest { .. }
            | vykar_storage::ParsedUrl::S3 { .. }
            | vykar_storage::ParsedUrl::Sftp { .. })
    );
    let concurrency = config.limits.listing_concurrency(is_remote);

    // Phase 1: Check each snapshot in manifest
    let snapshot_entries = repo.manifest().snapshots.clone();
    let snapshot_count = snapshot_entries.len();
    for (i, entry) in snapshot_entries.iter().enumerate() {
        emit_progress(
            &mut progress,
            CheckProgressEvent::SnapshotStarted {
                current: i + 1,
                total: snapshot_count,
                name: entry.name.clone(),
            },
        );

        // Load snapshot metadata
        let meta = match load_snapshot_meta(&repo, &entry.name) {
            Ok(m) => m,
            Err(e) => {
                errors.push(CheckError {
                    context: format!("snapshot '{}'", entry.name),
                    message: format!("failed to load metadata: {e}"),
                });
                continue;
            }
        };

        // Verify item_ptrs exist in chunk index
        for chunk_id in &meta.item_ptrs {
            if !repo.chunk_index().contains(chunk_id) {
                errors.push(CheckError {
                    context: format!("snapshot '{}' item_ptrs", entry.name),
                    message: format!("chunk {chunk_id} not in index"),
                });
            }
        }

        // Load item stream (needs &mut repo for blob cache), then check items
        let items_stream = match load_snapshot_item_stream(&mut repo, &entry.name) {
            Ok(s) => s,
            Err(e) => {
                errors.push(CheckError {
                    context: format!("snapshot '{}'", entry.name),
                    message: format!("failed to load items: {e}"),
                });
                continue;
            }
        };

        let mut per_snapshot_items = 0usize;
        let entry_name = entry.name.clone();
        if let Err(e) = for_each_decoded_item(&items_stream, |item| {
            per_snapshot_items += 1;
            if item.entry_type == ItemType::RegularFile {
                for chunk_ref in &item.chunks {
                    if !repo.chunk_index().contains(&chunk_ref.id) {
                        errors.push(CheckError {
                            context: format!("snapshot '{}' file '{}'", entry_name, item.path),
                            message: format!("chunk {} not in index", chunk_ref.id),
                        });
                    }
                }
            }
            Ok(())
        }) {
            errors.push(CheckError {
                context: format!("snapshot '{}'", entry.name),
                message: format!("failed to load items: {e}"),
            });
            continue;
        }

        items_checked += per_snapshot_items;
        snapshots_checked += 1;
    }

    // Build per-pack grouping from chunk index
    let chunks_existence_checked = repo.chunk_index().len();
    let mut pack_chunks: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> = HashMap::new();
    for (chunk_id, entry) in repo.chunk_index().iter() {
        pack_chunks
            .entry(entry.pack_id)
            .or_default()
            .push((*chunk_id, *entry));
    }
    // Try server-side verify for both existence and data checks
    let server_outcome = if !distrust_server {
        try_server_verify(&repo.storage, &pack_chunks, verify_data, &mut progress)
    } else {
        ServerVerifyOutcome::Fallback
    };

    // Determine which packs need client-side verification.
    let remaining_packs: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> = match &server_outcome {
        ServerVerifyOutcome::Ok { verified_packs, .. } => pack_chunks
            .iter()
            .filter(|(id, _)| !verified_packs.contains(id))
            .map(|(id, chunks)| (*id, chunks.clone()))
            .collect(),
        ServerVerifyOutcome::Fallback => pack_chunks.clone(),
    };

    let (mut packs_existence_checked, mut chunks_data_verified) = match server_outcome {
        ServerVerifyOutcome::Ok {
            errors: srv_errors,
            packs_responded,
            chunks_verified,
            ..
        } => {
            errors.extend(srv_errors);
            let data = if verify_data { chunks_verified } else { 0 };
            (packs_responded, data)
        }
        ServerVerifyOutcome::Fallback => (0, 0),
    };

    // Client-side checks for any packs not covered by server verification.
    if !remaining_packs.is_empty() {
        let remaining_total = remaining_packs.len();

        // Phase 2: Parallel pack existence check
        let packs_for_existence: Vec<(PackId, usize)> = remaining_packs
            .iter()
            .map(|(pack_id, chunks)| (*pack_id, chunks.len()))
            .collect();

        emit_progress(
            &mut progress,
            CheckProgressEvent::PacksExistencePhaseStarted {
                total_packs: remaining_total,
            },
        );

        let (existence_count, existence_errors) =
            parallel_pack_existence(&repo.storage, &packs_for_existence, concurrency);
        packs_existence_checked += existence_count;
        errors.extend(existence_errors);

        emit_progress(
            &mut progress,
            CheckProgressEvent::PacksExistenceProgress {
                checked: existence_count,
                total_packs: remaining_total,
            },
        );

        // Phase 3: Parallel verify-data (client-side crypto verification)
        if verify_data {
            let remaining_chunks: usize = remaining_packs.values().map(|chunks| chunks.len()).sum();

            emit_progress(
                &mut progress,
                CheckProgressEvent::ChunksDataPhaseStarted {
                    total_chunks: remaining_chunks,
                },
            );

            let packs_vec: Vec<(PackId, Vec<(ChunkId, ChunkIndexEntry)>)> =
                remaining_packs.into_iter().collect();

            let (data_count, data_errors) = parallel_verify_data(
                &repo.storage,
                &repo.crypto,
                repo.crypto.chunk_id_key(),
                &packs_vec,
                config.limits.verify_data_concurrency(),
                BATCH_THRESHOLD,
            );
            chunks_data_verified += data_count;
            errors.extend(data_errors);

            emit_progress(
                &mut progress,
                CheckProgressEvent::ChunksDataProgress {
                    verified: data_count,
                    total_chunks: remaining_chunks,
                },
            );
        }
    }

    Ok(CheckResult {
        snapshots_checked,
        items_checked,
        chunks_existence_checked,
        packs_existence_checked,
        chunks_data_verified,
        errors,
    })
}

// ---------------------------------------------------------------------------
// Phase 2: Parallel pack existence
// ---------------------------------------------------------------------------

fn parallel_pack_existence(
    storage: &Arc<dyn StorageBackend>,
    packs: &[(PackId, usize)],
    concurrency: usize,
) -> (usize, Vec<CheckError>) {
    if packs.is_empty() {
        return (0, Vec::new());
    }

    let work_idx = AtomicUsize::new(0);
    let checked = AtomicUsize::new(0);
    let errors = Mutex::new(Vec::new());

    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| loop {
                let idx = work_idx.fetch_add(1, Ordering::Relaxed);
                if idx >= packs.len() {
                    break;
                }
                let (pack_id, chunk_count) = &packs[idx];
                let pack_key = pack_id.storage_key();
                match storage.exists(&pack_key) {
                    Ok(true) => {}
                    Ok(false) => {
                        errors.lock().unwrap().push(CheckError {
                            context: "chunk index".into(),
                            message: format!(
                                "pack {pack_id} missing from storage \
                                     (referenced by {chunk_count} chunks)"
                            ),
                        });
                    }
                    Err(e) => {
                        errors.lock().unwrap().push(CheckError {
                            context: "chunk index".into(),
                            message: format!("pack {pack_id} existence check failed: {e}"),
                        });
                    }
                }
                checked.fetch_add(1, Ordering::Relaxed);
            });
        }
    });

    (
        checked.load(Ordering::Relaxed),
        errors.into_inner().unwrap(),
    )
}

// ---------------------------------------------------------------------------
// Phase 3: Parallel client-side verify-data
// ---------------------------------------------------------------------------

fn parallel_verify_data(
    storage: &Arc<dyn StorageBackend>,
    crypto: &Arc<dyn CryptoEngine>,
    chunk_id_key: &[u8; 32],
    packs: &[(PackId, Vec<(ChunkId, ChunkIndexEntry)>)],
    concurrency: usize,
    batch_threshold: usize,
) -> (usize, Vec<CheckError>) {
    if packs.is_empty() {
        return (0, Vec::new());
    }

    let work_idx = AtomicUsize::new(0);
    let verified = AtomicUsize::new(0);
    let errors = Mutex::new(Vec::new());

    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| loop {
                let idx = work_idx.fetch_add(1, Ordering::Relaxed);
                if idx >= packs.len() {
                    break;
                }
                let (pack_id, chunks) = &packs[idx];

                let mut local_errors = Vec::new();
                let count = if chunks.len() >= batch_threshold {
                    verify_pack_full(
                        storage.as_ref(),
                        crypto.as_ref(),
                        chunk_id_key,
                        pack_id,
                        chunks,
                        &mut local_errors,
                    )
                } else {
                    verify_pack_individual(
                        storage.as_ref(),
                        crypto.as_ref(),
                        chunk_id_key,
                        pack_id,
                        chunks,
                        &mut local_errors,
                    )
                };

                verified.fetch_add(count, Ordering::Relaxed);
                if !local_errors.is_empty() {
                    errors.lock().unwrap().extend(local_errors);
                }
            });
        }
    });

    (
        verified.load(Ordering::Relaxed),
        errors.into_inner().unwrap(),
    )
}

/// Download the full pack and verify each chunk locally.
pub(crate) fn verify_pack_full(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    chunk_id_key: &[u8; 32],
    pack_id: &PackId,
    chunks: &[(ChunkId, ChunkIndexEntry)],
    errors: &mut Vec<CheckError>,
) -> usize {
    let pack_key = pack_id.storage_key();
    let pack_data = match storage.get(&pack_key) {
        Ok(Some(data)) => data,
        Ok(None) => {
            errors.push(CheckError {
                context: "verify-data".into(),
                message: format!("pack {pack_id} not found (full GET)"),
            });
            return 0;
        }
        Err(e) => {
            errors.push(CheckError {
                context: "verify-data".into(),
                message: format!("pack {pack_id}: full GET failed: {e}"),
            });
            return 0;
        }
    };

    // Validate header
    if pack_data.len() < PACK_HEADER_SIZE
        || &pack_data[..8] != PACK_MAGIC
        || pack_data[8] < PACK_VERSION_MIN
        || pack_data[8] > PACK_VERSION_MAX
    {
        errors.push(CheckError {
            context: "verify-data".into(),
            message: format!("pack {pack_id}: invalid pack header"),
        });
        return 0;
    }

    let mut count = 0;
    for (chunk_id, entry) in chunks {
        let start = match usize::try_from(entry.pack_offset) {
            Ok(s) => s,
            Err(_) => {
                errors.push(CheckError {
                    context: "verify-data".into(),
                    message: format!(
                        "chunk {chunk_id}: pack_offset {} exceeds addressable range",
                        entry.pack_offset
                    ),
                });
                continue;
            }
        };
        let size = match usize::try_from(entry.stored_size) {
            Ok(s) => s,
            Err(_) => {
                errors.push(CheckError {
                    context: "verify-data".into(),
                    message: format!(
                        "chunk {chunk_id}: stored_size {} exceeds addressable range",
                        entry.stored_size
                    ),
                });
                continue;
            }
        };
        let end = match start.checked_add(size) {
            Some(e) => e,
            None => {
                errors.push(CheckError {
                    context: "verify-data".into(),
                    message: format!(
                        "chunk {chunk_id}: blob range overflows (offset={}, size={})",
                        entry.pack_offset, entry.stored_size
                    ),
                });
                continue;
            }
        };
        if end > pack_data.len() {
            errors.push(CheckError {
                context: "verify-data".into(),
                message: format!(
                    "chunk {chunk_id}: blob range [{start}..{end}) exceeds pack size {}",
                    pack_data.len()
                ),
            });
            continue;
        }

        let raw = &pack_data[start..end];
        count += verify_single_chunk(crypto, chunk_id_key, chunk_id, raw, errors);
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
    errors: &mut Vec<CheckError>,
) -> usize {
    let mut count = 0;
    for (chunk_id, entry) in chunks {
        let raw = match read_blob_from_pack(storage, pack_id, entry.pack_offset, entry.stored_size)
        {
            Ok(data) => data,
            Err(e) => {
                errors.push(CheckError {
                    context: "verify-data".into(),
                    message: format!("chunk {chunk_id}: read failed: {e}"),
                });
                continue;
            }
        };
        count += verify_single_chunk(crypto, chunk_id_key, chunk_id, &raw, errors);
    }
    count
}

/// Decrypt, decompress, and recompute ChunkId for one blob. Returns 1 on success, 0 on error.
fn verify_single_chunk(
    crypto: &dyn CryptoEngine,
    chunk_id_key: &[u8; 32],
    chunk_id: &ChunkId,
    raw: &[u8],
    errors: &mut Vec<CheckError>,
) -> usize {
    // Decrypt
    let compressed =
        match unpack_object_expect_with_context(raw, ObjectType::ChunkData, &chunk_id.0, crypto) {
            Ok(bytes) => bytes,
            Err(e) => {
                errors.push(CheckError {
                    context: "verify-data".into(),
                    message: format!("chunk {chunk_id}: decrypt failed: {e}"),
                });
                return 0;
            }
        };

    // Decompress
    let plaintext = match compress::decompress(&compressed) {
        Ok(data) => data,
        Err(e) => {
            errors.push(CheckError {
                context: "verify-data".into(),
                message: format!("chunk {chunk_id}: decompress failed: {e}"),
            });
            return 0;
        }
    };

    // Recompute chunk ID and compare
    let recomputed = ChunkId::compute(chunk_id_key, &plaintext);
    if &recomputed != chunk_id {
        errors.push(CheckError {
            context: "verify-data".into(),
            message: format!("chunk {chunk_id}: ID mismatch (recomputed {recomputed})"),
        });
        return 0;
    }

    1
}

// ---------------------------------------------------------------------------
// Server-side verify-packs integration
// ---------------------------------------------------------------------------

/// Estimate the on-disk size of a pack from its chunk index entries.
/// Computed from the pack wire format: header + (4-byte length prefix + blob) per
/// indexed chunk. May under-estimate if the pack contains dead/unindexed blobs.
fn estimate_pack_bytes(chunks: &[(ChunkId, ChunkIndexEntry)]) -> u64 {
    PACK_HEADER_SIZE as u64
        + chunks
            .iter()
            .map(|(_, e)| 4 + e.stored_size as u64)
            .sum::<u64>()
}

/// Try server-side pack verification. Returns `Ok` with the set of packs
/// actually verified (may be partial), or `Fallback` if the server doesn't
/// support verify-packs at all (first request fails).
pub(crate) fn try_server_verify(
    storage: &Arc<dyn StorageBackend>,
    pack_chunks: &HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>>,
    verify_data: bool,
    progress: &mut Option<&mut dyn FnMut(CheckProgressEvent)>,
) -> ServerVerifyOutcome {
    let total_packs = pack_chunks.len();
    if total_packs == 0 {
        return ServerVerifyOutcome::Ok {
            errors: Vec::new(),
            packs_responded: 0,
            packs_passed: 0,
            chunks_verified: 0,
            verified_packs: HashSet::new(),
        };
    }

    // Build verify requests from chunk index data
    let pack_list: Vec<(&PackId, &Vec<(ChunkId, ChunkIndexEntry)>)> = pack_chunks.iter().collect();

    let mut all_errors = Vec::new();
    let mut total_packs_responded: usize = 0;
    let mut total_packs_passed: usize = 0;
    let mut total_chunks_verified: usize = 0;
    let mut verified_packs: HashSet<PackId> = HashSet::new();
    let mut is_first_batch = true;

    let mut offset = 0;
    while offset < pack_list.len() {
        // Compute batch end respecting both pack count and byte volume
        let mut end = offset;
        let mut batch_bytes: u64 = 0;
        while end < pack_list.len()
            && end - offset < SERVER_VERIFY_BATCH_SIZE
            && (end == offset
                || batch_bytes + estimate_pack_bytes(pack_list[end].1) <= SERVER_VERIFY_MAX_BYTES)
        {
            batch_bytes += estimate_pack_bytes(pack_list[end].1);
            end += 1;
        }

        let batch = build_verify_request(&pack_list[offset..end], verify_data);

        let requested: Vec<(String, usize)> = pack_list[offset..end]
            .iter()
            .map(|(pack_id, chunks)| (pack_id.storage_key(), chunks.len()))
            .collect();
        let requested_refs: Vec<(&str, usize)> =
            requested.iter().map(|(k, c)| (k.as_str(), *c)).collect();

        match storage.server_verify_packs(&batch) {
            Err(VykarError::UnsupportedBackend(_)) => {
                tracing::debug!("backend does not support server-side verify-packs");
                return ServerVerifyOutcome::Fallback;
            }
            Err(e) if is_first_batch => {
                tracing::warn!("server verify-packs failed: {e}");
                return ServerVerifyOutcome::Fallback;
            }
            Err(e) => {
                // Mid-stream failure: preserve earlier results, let the caller
                // run client-side checks on the remaining packs.
                tracing::warn!("server verify-packs failed mid-stream: {e}");
                break;
            }
            Ok(resp) => {
                if is_first_batch {
                    emit_progress(
                        progress,
                        CheckProgressEvent::ServerVerifyPhaseStarted { total_packs },
                    );
                }
                let truncated = resp.truncated;
                let result = process_verify_response(&resp, &requested_refs, &mut all_errors);
                total_packs_responded += result.packs_responded;
                total_packs_passed += result.packs_passed;
                total_chunks_verified += result.chunks_verified;

                // Record which packs the server actually verified.
                let packs_covered = if truncated {
                    result.packs_responded
                } else {
                    end - offset
                };
                for &(pack_id, _) in pack_list.iter().skip(offset).take(packs_covered) {
                    verified_packs.insert(*pack_id);
                }

                if truncated {
                    // Server stopped early — advance only past the packs it
                    // actually processed so the rest get re-queued.
                    // If zero packs processed, skip one to avoid infinite loop;
                    // the caller will handle it client-side.
                    end = offset + packs_covered.max(1);
                }
                emit_progress(
                    progress,
                    CheckProgressEvent::ServerVerifyProgress {
                        verified: end,
                        total_packs,
                    },
                );
            }
        }

        is_first_batch = false;
        offset = end;
    }

    ServerVerifyOutcome::Ok {
        errors: all_errors,
        packs_responded: total_packs_responded,
        packs_passed: total_packs_passed,
        chunks_verified: total_chunks_verified,
        verified_packs,
    }
}

fn build_verify_request(
    packs: &[(&PackId, &Vec<(ChunkId, ChunkIndexEntry)>)],
    include_blobs: bool,
) -> VerifyPacksPlanRequest {
    let packs = packs
        .iter()
        .map(|(pack_id, chunks)| {
            let expected_blobs = if include_blobs {
                chunks
                    .iter()
                    .map(|(_chunk_id, entry)| VerifyBlobRef {
                        offset: entry.pack_offset,
                        length: entry.stored_size as u64,
                    })
                    .collect()
            } else {
                Vec::new()
            };
            VerifyPackRequest {
                pack_key: pack_id.storage_key(),
                expected_size: estimate_pack_bytes(chunks),
                expected_blobs,
            }
        })
        .collect();
    VerifyPacksPlanRequest {
        packs,
        protocol_version: PROTOCOL_VERSION,
    }
}

pub(crate) fn process_verify_response(
    resp: &VerifyPacksResponse,
    requested_packs: &[(&str, usize)],
    errors: &mut Vec<CheckError>,
) -> ProcessedVerifyResult {
    let requested: HashMap<&str, usize> = requested_packs.iter().copied().collect();
    let mut seen: HashSet<&str> = HashSet::new();

    let mut packs_responded: usize = 0;
    let mut packs_passed: usize = 0;
    let mut chunks_verified: usize = 0;

    for result in &resp.results {
        let Some(&chunk_count) = requested.get(result.pack_key.as_str()) else {
            // Unexpected key — server bug or version mismatch, not data corruption.
            tracing::warn!("server returned unexpected pack key: {}", result.pack_key);
            continue;
        };

        // Skip duplicate keys to prevent counter inflation
        if !seen.insert(result.pack_key.as_str()) {
            tracing::warn!("server returned duplicate pack key: {}", result.pack_key);
            continue;
        }

        packs_responded += 1;

        if result.hash_valid && result.header_valid && result.blobs_valid && result.error.is_none()
        {
            packs_passed += 1;
            chunks_verified += chunk_count;
        } else {
            let msg = if let Some(ref err_msg) = result.error {
                format!("pack {}: {err_msg}", result.pack_key)
            } else {
                let mut fails = Vec::new();
                if !result.hash_valid {
                    fails.push("hash");
                }
                if !result.header_valid {
                    fails.push("header");
                }
                if !result.blobs_valid {
                    fails.push("blobs");
                }
                format!(
                    "pack {}: verification failed ({})",
                    result.pack_key,
                    fails.join(", ")
                )
            };
            errors.push(CheckError {
                context: "server-verify".into(),
                message: msg,
            });
        }
    }

    // Check for missing packs — requested but not in server response.
    // Skip when the server truncated the response (packs will be re-queued).
    if !resp.truncated {
        for &(pack_key, _) in requested_packs {
            if !seen.contains(pack_key) {
                errors.push(CheckError {
                    context: "server-verify".into(),
                    message: format!("pack {pack_key} not included in server response"),
                });
            }
        }
    }

    ProcessedVerifyResult {
        packs_responded,
        packs_passed,
        chunks_verified,
    }
}
