use std::collections::{HashMap, HashSet};

use super::repair_apply::{execute_repair, probe_deletes_allowed};
use super::repair_plan::build_repair_plan;
use super::scan::{integrity_scan, ScanOptions};
use super::server_verify::try_server_verify;
use super::types::{
    CheckError, CheckProgressEvent, CheckResult, RepairMode, RepairResult, ServerVerifyOutcome,
};
use crate::config::VykarConfig;
use crate::index::ChunkIndexEntry;
use crate::repo::OpenOptions;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

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
        crate::commands::util::open_repo_with_read_session(config, passphrase, OpenOptions::new())?;

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
        let (mut repo, _session_guard) = crate::commands::util::open_repo_with_read_session(
            config,
            passphrase,
            OpenOptions::new(),
        )?;
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
        crate::commands::util::with_open_repo_maintenance_lock(
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
