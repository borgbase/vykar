use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::types::{
    emit_progress, CheckError, CheckProgressEvent, ProcessedVerifyResult, ServerVerifyOutcome,
};
use crate::index::ChunkIndexEntry;
use crate::repo::pack::PACK_HEADER_SIZE;
use vykar_storage::{
    StorageBackend, VerifyBlobRef, VerifyPackRequest, VerifyPacksPlanRequest, VerifyPacksResponse,
    PROTOCOL_VERSION,
};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::VykarError;
use vykar_types::pack_id::PackId;

/// Maximum packs per server-side verify-packs request.
/// Guards against huge fanout on repos with many tiny packs.
const SERVER_VERIFY_BATCH_SIZE: usize = 100;
/// Maximum estimated bytes of pack data per server-side verify request.
/// At 200 MB/s (HDD) this is ~10s of server I/O; at 500 MB/s (SSD) ~4s.
const SERVER_VERIFY_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

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
        // Compute batch end respecting both pack count and byte volume.
        // The `end < pack_list.len()` guard makes `pack_list[end]` and
        // `pack_list[offset..end]` in-bounds throughout this block.
        let mut end = offset;
        let mut batch_bytes: u64 = 0;
        while end < pack_list.len()
            && end - offset < SERVER_VERIFY_BATCH_SIZE
            && (end == offset
                || batch_bytes + pack_list.get(end).map_or(0, |p| estimate_pack_bytes(p.1))
                    <= SERVER_VERIFY_MAX_BYTES)
        {
            batch_bytes += pack_list.get(end).map_or(0, |p| estimate_pack_bytes(p.1));
            end += 1;
        }

        let slice = pack_list
            .get(offset..end)
            .expect("offset..end is bounded by the outer/inner while loops");
        let batch = build_verify_request(slice, verify_data);

        let requested: Vec<(String, usize)> = slice
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
