use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::compress;
use crate::compress::Compression;
use crate::config::VykarConfig;
use crate::index::ChunkIndexEntry;
use crate::repo::format::{
    pack_object_with_context, unpack_object_expect_with_context, ObjectType,
};
use crate::repo::manifest::SnapshotEntry;
use crate::repo::pack::{
    read_blob_from_pack, PACK_HEADER_SIZE, PACK_MAGIC, PACK_VERSION_MAX, PACK_VERSION_MIN,
};
use crate::repo::{OpenOptions, Repository};
use crate::snapshot::item::ItemType;
use crate::snapshot::SnapshotMeta;
use vykar_crypto::CryptoEngine;
use vykar_storage::{
    StorageBackend, VerifyBlobRef, VerifyPackRequest, VerifyPacksPlanRequest, VerifyPacksResponse,
    PROTOCOL_VERSION,
};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

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
#[derive(Debug)]
pub struct CheckResult {
    pub snapshots_checked: usize,
    pub items_checked: usize,
    pub chunks_existence_checked: usize,
    pub packs_existence_checked: usize,
    pub chunks_data_verified: usize,
    pub errors: Vec<CheckError>,
    /// Per-item impact records for snapshot items whose chunks live in a pack
    /// the existence check confirmed missing. Empty on healthy repos.
    pub item_impacts: Vec<ItemImpact>,
    /// True when the check was skipped entirely (e.g. max_percent=0 and full_every not due).
    pub skipped: bool,
}

// ---------------------------------------------------------------------------
// Structured integrity issues (for repair)
// ---------------------------------------------------------------------------

/// Structured integrity issue detected during check.
#[derive(Debug, Clone)]
pub enum IntegrityIssue {
    /// Snapshot blob fails to decrypt or deserialize.
    CorruptSnapshot {
        snapshot_id: SnapshotId,
        snapshot_name: Option<String>,
    },
    /// Raw `snapshots/<id>` with unparseable ID (never enters manifest).
    InvalidSnapshotKey { storage_key: String },
    /// Snapshot item_ptrs reference chunk not in index.
    DanglingItemPtr {
        snapshot_name: String,
        chunk_id: ChunkId,
    },
    /// File in snapshot references chunk not in index.
    DanglingFileChunk {
        snapshot_name: String,
        /// 0-based ordinal of this item within the decoded items_stream. Used
        /// by item-granular repair to locate the exact item to drop without
        /// relying on `path`, which may be duplicated across items.
        item_index: usize,
        path: String,
        chunk_id: ChunkId,
    },
    /// Pack referenced by index does not exist in storage.
    MissingPack { pack_id: PackId },
    /// Pack exists but fails header/hash/blob verification (--verify-data).
    CorruptPackContent { pack_id: PackId, detail: String },
    /// Individual chunk fails decrypt/decompress/ID check (--verify-data).
    CorruptChunk {
        chunk_id: ChunkId,
        pack_id: PackId,
        detail: String,
    },
    /// Pack existence check returned an I/O error (not confirmed missing).
    PackExistenceCheckFailed { pack_id: PackId, detail: String },
    /// Snapshot items could not be loaded or decoded (proven corruption).
    UnreadableSnapshot {
        snapshot_name: String,
        detail: String,
    },
    /// Snapshot meta or items failed to load due to I/O (not proven corrupt).
    SnapshotReadFailed {
        snapshot_name: String,
        detail: String,
    },
    /// Snapshot item failed per-item invariant validation.
    InvalidItem {
        snapshot_id: SnapshotId,
        snapshot_name: Option<String>,
        /// 0-based ordinal of this item within the decoded items_stream. Used
        /// by item-granular repair to locate the exact item to drop without
        /// relying on `item_path`, which may be duplicated across items.
        item_index: usize,
        item_path: String,
        reason: String,
    },
}

impl IntegrityIssue {
    /// Convert to a display-oriented CheckError.
    pub fn to_check_error(&self) -> CheckError {
        match self {
            IntegrityIssue::CorruptSnapshot {
                snapshot_name,
                snapshot_id,
            } => {
                let ctx = match snapshot_name {
                    Some(name) => format!("snapshot '{name}'"),
                    None => format!("snapshot {snapshot_id}"),
                };
                CheckError {
                    context: ctx,
                    message: "failed to load metadata: corrupt or undecryptable".into(),
                }
            }
            IntegrityIssue::InvalidSnapshotKey { storage_key } => CheckError {
                context: "snapshots".into(),
                message: format!("invalid snapshot key: {storage_key}"),
            },
            IntegrityIssue::DanglingItemPtr {
                snapshot_name,
                chunk_id,
            } => CheckError {
                context: format!("snapshot '{snapshot_name}' item_ptrs"),
                message: format!("chunk {chunk_id} not in index"),
            },
            IntegrityIssue::DanglingFileChunk {
                snapshot_name,
                path,
                chunk_id,
                ..
            } => CheckError {
                context: format!("snapshot '{snapshot_name}' file '{path}'"),
                message: format!("chunk {chunk_id} not in index"),
            },
            IntegrityIssue::MissingPack { pack_id } => CheckError {
                context: "chunk index".into(),
                message: format!("pack {pack_id} missing from storage"),
            },
            IntegrityIssue::CorruptPackContent { pack_id, detail } => CheckError {
                context: "verify-data".into(),
                message: format!("pack {pack_id}: {detail}"),
            },
            IntegrityIssue::CorruptChunk {
                chunk_id, detail, ..
            } => CheckError {
                context: "verify-data".into(),
                message: format!("chunk {chunk_id}: {detail}"),
            },
            IntegrityIssue::PackExistenceCheckFailed { pack_id, detail } => CheckError {
                context: "chunk index".into(),
                message: format!("pack {pack_id} existence check failed: {detail}"),
            },
            IntegrityIssue::UnreadableSnapshot {
                snapshot_name,
                detail,
            } => CheckError {
                context: format!("snapshot '{snapshot_name}'"),
                message: format!("failed to load items: {detail}"),
            },
            IntegrityIssue::SnapshotReadFailed {
                snapshot_name,
                detail,
            } => CheckError {
                context: format!("snapshot '{snapshot_name}'"),
                message: format!("I/O error: {detail}"),
            },
            IntegrityIssue::InvalidItem {
                snapshot_id,
                snapshot_name,
                item_path,
                reason,
                ..
            } => {
                let ctx = match snapshot_name {
                    Some(name) => format!("snapshot '{name}' item '{item_path}'"),
                    None => format!("snapshot {snapshot_id} item '{item_path}'"),
                };
                CheckError {
                    context: ctx,
                    message: reason.clone(),
                }
            }
        }
    }
}

/// Returns `true` if the error is a transient I/O or storage failure (not proven
/// corruption). Crypto, deserialization, format, and decompression errors are
/// considered evidence of corruption.
fn is_transient_io(err: &VykarError) -> bool {
    matches!(err, VykarError::Storage(_) | VykarError::Io(_))
}

// ---------------------------------------------------------------------------
// Per-item impact records (drives item-level repair in #123)
// ---------------------------------------------------------------------------

/// A snapshot item whose chunks live in a pack that is missing from storage.
///
/// Carries enough identity (`snapshot_id` + `item_index`) for #123's surgical
/// repair to locate the exact decoded record when rewriting a snapshot's
/// `items_stream`. `item_path` alone is not unique — duplicate paths can occur.
#[derive(Debug, Clone)]
pub struct ItemImpact {
    pub snapshot_id: SnapshotId,
    pub snapshot_name: String,
    /// 0-based ordinal of this item within the decoded items_stream. Stable
    /// across re-walks of the same stream.
    pub item_index: usize,
    pub item_path: String,
    /// `(chunk_id, pack_id)` pairs for this item's chunks that live in a pack
    /// the existence check confirmed missing. Always non-empty.
    pub affected_chunks: Vec<(ChunkId, PackId)>,
}

impl ItemImpact {
    /// Render this impact as a user-facing CheckError.
    pub fn to_check_error(&self) -> CheckError {
        let mut packs: Vec<PackId> = self.affected_chunks.iter().map(|(_, p)| *p).collect();
        packs.sort_by(|a, b| a.0.cmp(&b.0));
        packs.dedup();

        let message = if packs.len() == 1 {
            format!("references missing pack {}", packs[0])
        } else {
            let list = packs
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!("references missing packs {list}")
        };

        CheckError {
            context: format!(
                "snapshot '{}' item '{}'",
                self.snapshot_name, self.item_path
            ),
            message,
        }
    }
}

// ---------------------------------------------------------------------------
// Repair types
// ---------------------------------------------------------------------------

/// An action the repair engine will execute.
#[derive(Debug, Clone)]
pub enum RepairAction {
    RemoveCorruptSnapshot {
        snapshot_id: SnapshotId,
        name: Option<String>,
    },
    RemoveInvalidSnapshotKey {
        storage_key: String,
    },
    RemoveDanglingIndexEntries {
        pack_id: PackId,
        chunk_count: usize,
    },
    /// Pack header invalid — remove ALL index entries for this pack.
    RemoveCorruptPack {
        pack_id: PackId,
        chunk_count: usize,
    },
    /// Individual chunks failed client-side verify — remove only these entries.
    RemoveCorruptChunks {
        pack_id: PackId,
        chunk_ids: Vec<ChunkId>,
    },
    RemoveDanglingSnapshot {
        snapshot_name: String,
        missing_chunks: usize,
    },
    /// Rewrite a snapshot under a new SnapshotId with the listed item ordinals
    /// dropped. Existing storage blob for `snapshot_id` is removed; the
    /// rewritten blob is committed at the new id, the manifest entry's name is
    /// preserved. `dropped_paths` and `reasons` are parallel to `item_indices`
    /// and exist solely to power user-facing dry-run output.
    DropItemsFromSnapshot {
        snapshot_id: SnapshotId,
        snapshot_name: String,
        item_indices: Vec<usize>,
        dropped_paths: Vec<String>,
        reasons: Vec<String>,
    },
    RebuildRefcounts,
}

/// The computed plan for a repair operation.
#[derive(Debug)]
pub struct RepairPlan {
    pub actions: Vec<RepairAction>,
    pub has_data_loss: bool,
}

/// Whether to just show the plan or actually apply it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairMode {
    PlanOnly,
    Apply,
}

/// Result of a repair operation.
#[derive(Debug)]
pub struct RepairResult {
    pub check_result: CheckResult,
    pub plan: RepairPlan,
    pub applied: Vec<RepairAction>,
    pub repair_errors: Vec<String>,
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
        missing: usize,
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
    run_with_progress(
        config,
        passphrase,
        verify_data,
        distrust_server,
        None,
        100,
        false,
    )
}

/// Run check with progress reporting.
///
/// `max_percent`: percentage of packs/snapshots to check (0–100). 100 = full check.
/// `record_state`: if true and a full (100%) check succeeds, record the timestamp
///   in the local check state file. Standalone CLI passes false; daemon/GUI passes true.
pub fn run_with_progress(
    config: &VykarConfig,
    passphrase: Option<&str>,
    verify_data: bool,
    distrust_server: bool,
    mut progress: Option<&mut dyn FnMut(CheckProgressEvent)>,
    max_percent: u8,
    record_state: bool,
) -> Result<CheckResult> {
    let cache_dir = config.cache_dir.as_deref().map(std::path::Path::new);
    let full_every_dur = config.check.full_every_duration();

    // Pre-open early exit: if max_percent=0 and no full_every configured,
    // skip without opening the repo at all.
    if max_percent == 0 && full_every_dur.is_none() {
        return Ok(skipped_result());
    }

    // Open repo (needed for fingerprint check and actual scan).
    let (mut repo, _session_guard) =
        super::util::open_repo_with_read_session(config, passphrase, OpenOptions::new())?;

    // Determine effective check percentage using repo fingerprint.
    let fingerprint = compute_repo_fingerprint(&repo);
    let effective = if max_percent == 100 {
        100
    } else if let Some(ref interval) = full_every_dur {
        if crate::app::check_state::full_check_is_due(
            &config.repository.url,
            &fingerprint,
            cache_dir,
            *interval,
        ) {
            100
        } else {
            max_percent
        }
    } else {
        max_percent
    };

    // Early exit: nothing to check this cycle.
    if effective == 0 {
        return Ok(skipped_result());
    }

    repo.load_chunk_index_uncached()?;

    // Build per-pack grouping from chunk index (needed for server verify).
    let mut pack_chunks: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> = HashMap::new();
    for (chunk_id, entry) in repo.chunk_index().iter() {
        pack_chunks
            .entry(entry.pack_id)
            .or_default()
            .push((*chunk_id, *entry));
    }

    // If sampling (effective < 100), select a subset of packs.
    let sampled_out: HashSet<PackId> = if effective < 100 {
        sample_packs_out(&pack_chunks, effective)
    } else {
        HashSet::new()
    };

    // Filter pack_chunks for server verify to only include sampled-in packs.
    let verify_pack_chunks: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> =
        if sampled_out.is_empty() {
            pack_chunks.clone()
        } else {
            pack_chunks
                .iter()
                .filter(|(pid, _)| !sampled_out.contains(pid))
                .map(|(pid, chunks)| (*pid, chunks.clone()))
                .collect()
        };

    // Try server-side verify for both existence and data checks.
    let server_outcome = if !distrust_server {
        try_server_verify(
            &repo.storage,
            &verify_pack_chunks,
            verify_data,
            &mut progress,
        )
    } else {
        ServerVerifyOutcome::Fallback
    };

    let (verified_packs, srv_packs_responded, srv_chunks_verified, srv_errors) =
        match server_outcome {
            ServerVerifyOutcome::Ok {
                verified_packs,
                packs_responded,
                chunks_verified,
                errors,
                ..
            } => (verified_packs, packs_responded, chunks_verified, errors),
            ServerVerifyOutcome::Fallback => (HashSet::new(), 0, 0, Vec::new()),
        };

    // Combined skip set: sampled_out + server-verified packs.
    let mut combined_skip = sampled_out;
    combined_skip.extend(verified_packs.iter());
    let skip = if combined_skip.is_empty() {
        None
    } else {
        Some(&combined_skip)
    };

    // Sample snapshots if effective < 100.
    let snapshot_sample_percent = if effective < 100 {
        Some(effective)
    } else {
        None
    };

    let scan = integrity_scan(
        &mut repo,
        config,
        &ScanOptions {
            collect_chunk_refs: false,
            detect_orphans: false,
            verify_data,
            skip_packs: skip,
            snapshot_sample_percent,
        },
        &mut progress,
    )?;

    // Compute server-verified chunk count for existence counter.
    let srv_chunks_existence: usize = verified_packs
        .iter()
        .filter_map(|p| pack_chunks.get(p))
        .map(|c| c.len())
        .sum();

    let mut errors: Vec<CheckError> = srv_errors;
    errors.extend(scan.issues.iter().map(|i| i.to_check_error()));
    errors.extend(scan.item_impacts.iter().map(|i| i.to_check_error()));

    let result = CheckResult {
        snapshots_checked: scan.counters.snapshots_checked,
        items_checked: scan.counters.items_checked,
        chunks_existence_checked: scan.counters.chunks_existence_checked + srv_chunks_existence,
        packs_existence_checked: scan.counters.packs_existence_checked + srv_packs_responded,
        chunks_data_verified: scan.counters.chunks_data_verified
            + if verify_data { srv_chunks_verified } else { 0 },
        errors,
        item_impacts: scan.item_impacts,
        skipped: false,
    };

    // Record full check timestamp if this was a 100% run and succeeded.
    if record_state && effective == 100 && result.errors.is_empty() {
        crate::app::check_state::record_full_check(&config.repository.url, &fingerprint, cache_dir);
    }

    Ok(result)
}

fn skipped_result() -> CheckResult {
    CheckResult {
        snapshots_checked: 0,
        items_checked: 0,
        chunks_existence_checked: 0,
        packs_existence_checked: 0,
        chunks_data_verified: 0,
        errors: Vec::new(),
        item_impacts: Vec::new(),
        skipped: true,
    }
}

/// Compute a hex fingerprint from the repo's identity material.
fn compute_repo_fingerprint(repo: &crate::repo::Repository) -> String {
    let fp =
        crate::repo::identity::compute_fingerprint(&repo.config.id, repo.crypto.chunk_id_key());
    hex::encode(fp)
}

/// Select which packs to skip (sample out) for a partial check.
/// Returns the set of pack IDs that should NOT be checked.
fn sample_packs_out(
    pack_chunks: &HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>>,
    percent: u8,
) -> HashSet<PackId> {
    use rand::seq::index::sample;

    let total = pack_chunks.len();
    if total == 0 || percent >= 100 {
        return HashSet::new();
    }

    let keep = (total as u64 * percent as u64).div_ceil(100) as usize;
    let keep = keep.max(1).min(total);

    let pack_ids: Vec<PackId> = pack_chunks.keys().copied().collect();
    let mut rng = rand::rng();
    let indices = sample(&mut rng, total, keep);

    let kept: HashSet<usize> = indices.into_iter().collect();
    pack_ids
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !kept.contains(i))
        .map(|(_, pid)| pid)
        .collect()
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

// ---------------------------------------------------------------------------
// Integrity scan (shared by read-only check and repair)
// ---------------------------------------------------------------------------

/// Options controlling the integrity scan phases.
struct ScanOptions<'a> {
    /// Collect per-snapshot chunk refs (needed for repair plan).
    collect_chunk_refs: bool,
    /// Detect orphan snapshot blobs on storage not in the manifest.
    detect_orphans: bool,
    /// Run client-side crypto verification of chunk data.
    verify_data: bool,
    /// Packs already verified server-side — skip existence and data checks for these.
    skip_packs: Option<&'a HashSet<PackId>>,
    /// If set, sample only this percentage of snapshots in Phase 1.
    snapshot_sample_percent: Option<u8>,
}

/// Counters collected during an integrity scan.
#[derive(Debug, Default)]
struct ScanCounters {
    snapshots_checked: usize,
    items_checked: usize,
    chunks_existence_checked: usize,
    packs_existence_checked: usize,
    chunks_data_verified: usize,
}

/// Output of [`repair_scan`]: counters, issues, and per-snapshot chunk refs.
struct ScanResult {
    counters: ScanCounters,
    issues: Vec<IntegrityIssue>,
    /// Maps each snapshot name to the set of chunk IDs it references
    /// (item_ptrs and file chunks combined; used by the chunks-to-remove
    /// dangling-snapshot path).
    snapshot_chunk_refs: HashMap<String, HashSet<ChunkId>>,
    /// Per-snapshot `item_ptrs` chunks, partitioned out from
    /// `snapshot_chunk_refs`. Populated whenever the scan reads a snapshot's
    /// metadata, even if the items_stream subsequently fails to load — so the
    /// item-ptrs coverage gate has data to fail-closed against.
    snapshot_item_ptrs: HashMap<SnapshotId, HashSet<ChunkId>>,
    /// Per-snapshot, per-item file chunk sets. Indexed by 0-based item ordinal
    /// within the decoded items_stream. Empty inner sets for non-`RegularFile`
    /// items. Populated only when `collect_chunk_refs == true` AND the
    /// items_stream walk completes successfully — snapshots whose walk fails
    /// or skips will be absent from this map (the planner reads that absence
    /// as "refuse item-level repair").
    snapshot_per_item_chunks: HashMap<SnapshotId, Vec<HashSet<ChunkId>>>,
    /// Per-snapshot total item count. Populated in lockstep with
    /// `snapshot_per_item_chunks`: present iff the items_stream walk completed.
    snapshot_item_counts: HashMap<SnapshotId, usize>,
    /// Items whose chunks reference a pack confirmed missing in Phase 2.
    /// Empty when no missing packs were detected.
    item_impacts: Vec<ItemImpact>,
}

/// Run the integrity scan, producing structured issues.
///
/// `ScanOptions` controls which phases run and which packs are skipped.
/// The caller is responsible for calling `repo.refresh_snapshot_list()` before
/// this function when repair-level freshness is needed.
fn integrity_scan(
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
                        let is_corrupt = unpack_object_expect_with_context(
                            &blob,
                            ObjectType::SnapshotMeta,
                            snapshot_id.as_bytes(),
                            repo.crypto.as_ref(),
                        )
                        .and_then(|meta_bytes| {
                            rmp_serde::from_slice::<crate::snapshot::SnapshotMeta>(&meta_bytes)
                                .map_err(|e| VykarError::Other(format!("deserialize: {e}")))
                        })
                        .is_err();
                        if is_corrupt {
                            issues.push(IntegrityIssue::CorruptSnapshot {
                                snapshot_id,
                                snapshot_name: None,
                            });
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
                if idx >= packs.len() {
                    break;
                }
                let (pack_id, _chunk_count) = &packs[idx];
                let pack_key = pack_id.storage_key();
                match storage.exists(&pack_key) {
                    Ok(true) => {
                        present_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(false) => {
                        missing_count.fetch_add(1, Ordering::Relaxed);
                        issues
                            .lock()
                            .unwrap()
                            .push(IntegrityIssue::MissingPack { pack_id: *pack_id });
                    }
                    Err(e) => {
                        issues
                            .lock()
                            .unwrap()
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
    (present + missing, missing, issues.into_inner().unwrap())
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
                if idx >= packs.len() {
                    break;
                }
                let (pack_id, chunks) = &packs[idx];

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
                    issues.lock().unwrap().extend(local_issues);
                }
            });
        }
    });

    (
        verified.load(Ordering::Relaxed),
        issues.into_inner().unwrap(),
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

    // Validate header
    if pack_data.len() < PACK_HEADER_SIZE
        || &pack_data[..8] != PACK_MAGIC
        || pack_data[8] < PACK_VERSION_MIN
        || pack_data[8] > PACK_VERSION_MAX
    {
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

        let raw = &pack_data[start..end];
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
    let compressed =
        match unpack_object_expect_with_context(raw, ObjectType::ChunkData, &chunk_id.0, crypto) {
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
fn build_repair_plan(
    scan: &ScanResult,
    pack_chunks: &HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>>,
    name_to_id: &HashMap<String, SnapshotId>,
) -> RepairPlan {
    use std::collections::BTreeMap;

    let mut actions: Vec<RepairAction> = Vec::new();
    let mut has_data_loss = false;

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
        packs.sort_by(|a, b| a.0.cmp(&b.0));
        packs.dedup();
        let reason = if packs.len() == 1 {
            format!("chunks in missing pack {}", packs[0])
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
        let drops = &per_snapshot_drops[&id];

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
        if let Some(name_opt) = invalid_item_name {
            if !has_other_whole {
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

    // Always include refcount rebuild
    actions.push(RepairAction::RebuildRefcounts);

    RepairPlan {
        actions,
        has_data_loss,
    }
}

/// Probe whether the backend supports deletes (i.e. is not append-only).
/// Tries to delete a non-existent sentinel key; if the error indicates a
/// permission or authorization failure, the backend is append-only.
fn probe_deletes_allowed(storage: &dyn StorageBackend) -> bool {
    match storage.delete("snapshots/.repair-probe") {
        Ok(()) => true,
        Err(ref e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("permission")
                || msg.contains("forbidden")
                || msg.contains("403")
                || msg.contains("read-only")
                || msg.contains("append-only")
            {
                false
            } else {
                // Transient/not-found errors → assume deletes are allowed
                true
            }
        }
    }
}

/// Run `check --repair`.
pub fn run_with_repair(
    config: &VykarConfig,
    passphrase: Option<&str>,
    verify_data: bool,
    mode: RepairMode,
    mut progress: Option<&mut dyn FnMut(CheckProgressEvent)>,
) -> Result<RepairResult> {
    let scan_opts = ScanOptions {
        collect_chunk_refs: true,
        detect_orphans: true,
        verify_data,
        skip_packs: None,
        snapshot_sample_percent: None,
    };

    if mode == RepairMode::PlanOnly {
        // PlanOnly: read session, no lock, purely read-only.
        let (mut repo, _session_guard) =
            super::util::open_repo_with_read_session(config, passphrase, OpenOptions::new())?;
        repo.load_chunk_index_uncached()?;
        repo.refresh_snapshot_list()?;

        let scan = integrity_scan(&mut repo, config, &scan_opts, &mut progress)?;

        // Build per-pack grouping for plan
        let mut pack_chunks: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> = HashMap::new();
        for (chunk_id, entry) in repo.chunk_index().iter() {
            pack_chunks
                .entry(entry.pack_id)
                .or_default()
                .push((*chunk_id, *entry));
        }

        let name_to_id: HashMap<String, SnapshotId> = repo
            .manifest()
            .snapshots
            .iter()
            .map(|e| (e.name.clone(), e.id))
            .collect();
        let plan = build_repair_plan(&scan, &pack_chunks, &name_to_id);
        let mut errors: Vec<CheckError> = scan.issues.iter().map(|i| i.to_check_error()).collect();
        errors.extend(scan.item_impacts.iter().map(|i| i.to_check_error()));
        let check_result = CheckResult {
            snapshots_checked: scan.counters.snapshots_checked,
            items_checked: scan.counters.items_checked,
            chunks_existence_checked: scan.counters.chunks_existence_checked,
            packs_existence_checked: scan.counters.packs_existence_checked,
            chunks_data_verified: scan.counters.chunks_data_verified,
            errors,
            item_impacts: scan.item_impacts,
            skipped: false,
        };

        Ok(RepairResult {
            check_result,
            plan,
            applied: Vec::new(),
            repair_errors: Vec::new(),
        })
    } else {
        // Apply: maintenance lock, re-scan under lock, mutate state.
        super::util::with_open_repo_maintenance_lock(
            config,
            passphrase,
            OpenOptions::new(),
            |repo| {
                repo.load_chunk_index_uncached()?;
                repo.refresh_snapshot_list()?;

                let scan = integrity_scan(repo, config, &scan_opts, &mut progress)?;

                // Build per-pack grouping for plan
                let mut pack_chunks: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> =
                    HashMap::new();
                for (chunk_id, entry) in repo.chunk_index().iter() {
                    pack_chunks
                        .entry(entry.pack_id)
                        .or_default()
                        .push((*chunk_id, *entry));
                }

                let name_to_id: HashMap<String, SnapshotId> = repo
                    .manifest()
                    .snapshots
                    .iter()
                    .map(|e| (e.name.clone(), e.id))
                    .collect();
                let plan = build_repair_plan(&scan, &pack_chunks, &name_to_id);

                // If plan has data-loss actions, probe append-only before mutating.
                if plan.has_data_loss && !probe_deletes_allowed(repo.storage.as_ref()) {
                    return Err(VykarError::Other(
                        "repair requires deleting immutable snapshot objects; \
                     not supported on append-only backends"
                            .into(),
                    ));
                }

                // Execute the repair
                let (applied, repair_errors) =
                    execute_repair(repo, &plan, &scan.issues, &pack_chunks)?;

                let mut errors: Vec<CheckError> =
                    scan.issues.iter().map(|i| i.to_check_error()).collect();
                errors.extend(scan.item_impacts.iter().map(|i| i.to_check_error()));
                let check_result = CheckResult {
                    snapshots_checked: scan.counters.snapshots_checked,
                    items_checked: scan.counters.items_checked,
                    chunks_existence_checked: scan.counters.chunks_existence_checked,
                    packs_existence_checked: scan.counters.packs_existence_checked,
                    chunks_data_verified: scan.counters.chunks_data_verified,
                    errors,
                    item_impacts: scan.item_impacts,
                    skipped: false,
                };

                Ok(RepairResult {
                    check_result,
                    plan,
                    applied,
                    repair_errors,
                })
            },
        )
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
            Ok(()) => applied.push(plan.actions[rw.action_idx].clone()),
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
    let old_stream = load_snapshot_item_stream(repo, snapshot_name)?;

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

/// Execute repair actions in the correct order.
fn execute_repair(
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
        let items_stream = match load_snapshot_item_stream(repo, &entry.name) {
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

    #[test]
    fn build_repair_plan_treats_invalid_item_as_doomed_snapshot() {
        let snapshot_id = SnapshotId([0x11u8; 32]);
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
        let present = PackId([0x01u8; 32]);
        let missing = PackId([0x02u8; 32]);
        let errored = PackId([0x03u8; 32]);

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

    #[test]
    fn build_repair_plan_dedupes_invalid_item_with_corrupt_snapshot() {
        let snapshot_id = SnapshotId([0x22u8; 32]);
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
