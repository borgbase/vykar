use std::collections::{HashMap, HashSet};

use super::repair_apply::{execute_repair, probe_deletes_allowed};
use super::repair_plan::{add_pack_maps, build_repair_plan};
use super::scan::{ScanOptions, ScanResult, integrity_scan};
use super::server_verify::try_server_verify;
use super::types::{
    CheckError, CheckProgressEvent, CheckResult, IntegrityIssue, RepairAction, RepairIndexLoad,
    RepairMode, RepairPlan, RepairResult, ServerVerifyOutcome,
};
use crate::config::VykarConfig;
use crate::index::table::IndexState;
use crate::index::{ChunkIndexEntry, LiveIndex};
use crate::repo::control::CancelToken;
use crate::repo::index_store;
use crate::repo::{OpenOptions, Repository, needs_recheck};
use tracing::info;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

/// Chunks grouped by the pack that stores them.
type PackChunks = HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>>;

/// Group a chunk index by pack — the shape both the server-verify request and
/// the repair planner consume.
fn group_by_pack(index: &LiveIndex) -> PackChunks {
    let mut pack_chunks: PackChunks = HashMap::new();
    for (chunk_id, entry) in index.iter() {
        pack_chunks
            .entry(entry.pack_id)
            .or_default()
            .push((chunk_id, entry));
    }
    pack_chunks
}

/// Build the repair plan for a completed scan. The planner needs a snapshot
/// name → id map, derived from the manifest the scan ran against. `load`
/// says how the index was read; packs a rebuilt index took from this
/// machine's chunk tables get pack maps. Pack maps are left out of the plan,
/// with a warning, when the storage cannot hold them (an older server); when
/// asking the server fails, the error is returned (nothing has changed yet).
fn plan_repair(
    repo: &Repository,
    scan: &ScanResult,
    pack_chunks: &PackChunks,
    load: &RepairIndexLoad,
) -> Result<RepairPlan> {
    let name_to_id: HashMap<String, SnapshotId> = repo
        .manifest()
        .snapshots
        .iter()
        .map(|e| (e.name.clone(), e.id))
        .collect();
    let mut plan = build_repair_plan(scan, pack_chunks, &name_to_id);
    plan.index_recovery = load.recovery;
    plan.uncovered_packs = load.uncovered_packs.clone();
    add_pack_maps(&mut plan.actions, &mut load.packs_from_local_tables.clone());
    if plan
        .actions
        .iter()
        .any(|a| matches!(a, RepairAction::WritePackMaps { .. }))
        && !crate::repo::pack_map::pack_maps_supported(repo.storage.as_ref())?
    {
        tracing::warn!(
            "the server does not accept pack maps (packmaps/); upgrade vykar-server. The repair \
             writes none"
        );
        plan.actions
            .retain(|a| !matches!(a, RepairAction::WritePackMaps { .. }));
    }
    Ok(plan)
}

/// For `RepairMode::ApplyShown`: refuse the plan rebuilt under the lock when
/// it differs from the shown one in data loss or in how the index was read.
fn refuse_repair_if_plan_changed(mode: RepairMode, plan: &RepairPlan) -> Result<()> {
    let RepairMode::ApplyShown {
        has_data_loss,
        index_recovery,
    } = mode
    else {
        return Ok(());
    };
    if !has_data_loss && plan.has_data_loss {
        return Err(VykarError::Other(
            "aborting repair: the plan rebuilt under the repository lock removes data, \
             but the plan that was shown did not; nothing was changed. Run \
             `vykar check --repair` again to review the new plan"
                .into(),
        ));
    }
    if plan.index_recovery != index_recovery {
        return Err(VykarError::Other(
            "aborting repair: the index could be read differently than when the plan was \
             shown; nothing was changed. Run `vykar check --repair` again to review the new \
             plan"
                .into(),
        ));
    }
    Ok(())
}

impl From<ScanResult> for CheckResult {
    /// Project a completed scan into the user-facing result. `run_with_progress`
    /// folds its server-verify counters and errors in afterwards.
    fn from(scan: ScanResult) -> Self {
        let mut errors: Vec<CheckError> = scan.issues.iter().map(|i| i.to_check_error()).collect();
        errors.extend(scan.item_impacts.iter().map(|i| i.to_check_error()));
        CheckResult {
            snapshots_checked: scan.counters.snapshots_checked,
            key_files_checked: scan.counters.key_files_checked,
            items_checked: scan.counters.items_checked,
            chunks_existence_checked: scan.counters.chunks_existence_checked,
            packs_existence_checked: scan.counters.packs_existence_checked,
            chunks_data_verified: scan.counters.chunks_data_verified,
            errors,
            item_impacts: scan.item_impacts,
            skipped: false,
        }
    }
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
        &CancelToken::new(),
    )
}

/// Run check with progress reporting.
///
/// `max_percent`: percentage of packs/snapshots to check (0–100). 100 = full check.
/// `record_state`: if true and a full (100%) check succeeds, record the timestamp
///   in the local check state file. Standalone CLI passes false; daemon/GUI passes true.
#[allow(clippy::too_many_arguments)]
pub fn run_with_progress(
    config: &VykarConfig,
    passphrase: Option<&str>,
    verify_data: bool,
    distrust_server: bool,
    progress: Option<&mut dyn FnMut(CheckProgressEvent)>,
    max_percent: u8,
    record_state: bool,
    shutdown: &CancelToken,
) -> Result<CheckResult> {
    // Entry guard: on an empty repository every loop below runs zero
    // iterations, so without this a pre-set flag would return a clean `Ok` and
    // record success for work that never ran.
    shutdown.check()?;

    // Pre-open early exit: if max_percent=0 and no full_every configured,
    // skip without opening the repo at all.
    if max_percent == 0 && config.check.full_every_duration().is_none() {
        return Ok(skipped_result());
    }

    // Open repo (needed for fingerprint check and actual scan).
    let mut repo =
        crate::commands::util::open_repo(config, passphrase, OpenOptions::new(), shutdown)?;

    run_on_repo(
        &mut repo,
        config,
        verify_data,
        distrust_server,
        progress,
        max_percent,
        record_state,
        shutdown,
    )
}

/// [`run_with_progress`] on an already opened repository (opened with
/// `OpenOptions::new()`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_on_repo(
    repo: &mut Repository,
    config: &VykarConfig,
    verify_data: bool,
    distrust_server: bool,
    mut progress: Option<&mut dyn FnMut(CheckProgressEvent)>,
    max_percent: u8,
    record_state: bool,
    shutdown: &CancelToken,
) -> Result<CheckResult> {
    let cache_dir = config.cache_dir.as_deref().map(std::path::Path::new);
    let full_every_dur = config.check.full_every_duration();

    // Determine effective check percentage using repo fingerprint.
    let fingerprint = compute_repo_fingerprint(repo);
    let effective = if max_percent == 100 {
        100
    } else if let Some(ref interval) = full_every_dur {
        if super::state::full_check_is_due(
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

    // 04 step 7: readers register no marker, so a compact may move or
    // remove what a pass loaded. Findings against a head that can have lost
    // entries or packs since (the test the backup commit's reuse re-check
    // uses) are checked again on the latest head; a compact publishes per
    // batch, so this repeats, at most MAX_CHECK_PASSES passes in all. A
    // sampled check draws its sample once: every pass checks the same packs
    // and snapshots, so a finding is not lost to a new draw.
    let mut sample = None;
    let mut passes = 1;
    let mut result = check_pass(
        repo,
        config,
        verify_data,
        distrust_server,
        &mut progress,
        effective,
        &mut sample,
        shutdown,
    )?;
    while !result.errors.is_empty() {
        let latest = index_store::fetch_head(repo.storage.as_ref(), repo.crypto.as_ref())?;
        let Some(head) = repo.index_head() else {
            break;
        };
        let loaded = head.generation;
        let from = IndexState::of_head(head);
        if latest.generation <= loaded
            || !needs_recheck(Some((&from, head.sweep.generation)), &latest)
        {
            break;
        }
        let latest = latest.generation;
        if passes == MAX_CHECK_PASSES {
            result.errors.push(CheckError {
                context: "check".into(),
                message: format!(
                    "the repository's index was rewritten during every pass \
                     ({passes} passes; maintenance, or backups folding the index); the \
                     problems above may be work in progress. Re-run check when it is quiet"
                ),
            });
            break;
        }
        info!(
            loaded,
            latest,
            "check found problems and the index changed during the check; checking again on the latest head"
        );
        shutdown.check()?;
        repo.refresh_snapshot_list()?;
        passes += 1;
        result = check_pass(
            repo,
            config,
            verify_data,
            distrust_server,
            &mut progress,
            effective,
            &mut sample,
            shutdown,
        )?;
    }

    // Completion guard, before the success timestamp *and* before the result
    // is handed back. An aborted-but-clean-so-far scan has an empty `errors`
    // vec, so an extra `&&` term here would still return `Ok(partial_result)` —
    // which the cycle orchestrator maps to `StepOutcome::Ok` and the CLI prints
    // as a completed summary. Cancellation must be an error, not a short `Ok`.
    shutdown.check()?;

    // Record full check timestamp if this was a 100% run and succeeded.
    if record_state && effective == 100 && result.errors.is_empty() {
        super::state::record_full_check(&config.repository.url, &fingerprint, cache_dir);
    }

    Ok(result)
}

/// Passes one check makes at most: a pass that finds problems while the
/// head moved during it in a way that can remove entries or packs (a
/// concurrent compact) is followed by another.
const MAX_CHECK_PASSES: u32 = 3;

/// What a sampled check looks at, drawn in its first pass and kept for the
/// later ones: of these, each pass checks the packs its index still names
/// and the snapshots still listed.
struct CheckSample {
    packs: HashSet<PackId>,
    snapshots: HashSet<SnapshotId>,
}

impl CheckSample {
    /// Draw `percent` of the packs of `pack_chunks` and of the snapshots
    /// `repo` lists (at least one of each when there are any).
    fn draw(repo: &Repository, pack_chunks: &PackChunks, percent: u8) -> Self {
        let mut pack_ids: Vec<PackId> = pack_chunks.keys().copied().collect();
        // Sorted, so that a pinned draw (tests) names the same packs.
        pack_ids.sort_by_cached_key(PackId::storage_key);
        let picked = sample_indices(SampleOf::Packs, pack_ids.len(), percent);
        let packs = pick(pack_ids, &picked);
        let snapshot_ids: Vec<SnapshotId> =
            repo.manifest().snapshots.iter().map(|e| e.id).collect();
        let picked = sample_indices(SampleOf::Snapshots, snapshot_ids.len(), percent);
        let snapshots = pick(snapshot_ids, &picked);
        Self { packs, snapshots }
    }
}

/// The items of `items` at the positions `picked`.
fn pick<T: std::hash::Hash + Eq>(items: Vec<T>, picked: &HashSet<usize>) -> HashSet<T> {
    items
        .into_iter()
        .enumerate()
        .filter(|(i, _)| picked.contains(i))
        .map(|(_, x)| x)
        .collect()
}

/// What a sample is drawn of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SampleOf {
    Packs,
    Snapshots,
}

/// Tests: a draw of the positions to sample, `(of, total, keep)` to the
/// positions, in place of the random one, on this thread.
#[cfg(test)]
pub(crate) type PinnedSample = Box<dyn FnMut(SampleOf, usize, usize) -> Vec<usize>>;

#[cfg(test)]
thread_local! {
    pub(crate) static PINNED_SAMPLE: std::cell::RefCell<Option<PinnedSample>> =
        const { std::cell::RefCell::new(None) };
}

/// `percent` of the positions `0..total`, drawn at random: at least one
/// when `total > 0`.
fn sample_indices(of: SampleOf, total: usize, percent: u8) -> HashSet<usize> {
    if total == 0 || percent >= 100 {
        return (0..total).collect();
    }
    let keep = (total as u64 * percent as u64).div_ceil(100) as usize;
    let keep = keep.max(1).min(total);
    #[cfg(test)]
    if let Some(picked) = PINNED_SAMPLE.with_borrow_mut(|f| f.as_mut().map(|f| f(of, total, keep)))
    {
        return picked.into_iter().collect();
    }
    // `of` is read only by the test pin.
    let _ = of;
    rand::seq::index::sample(&mut rand::rng(), total, keep)
        .into_iter()
        .collect()
}

/// One pass of [`run_on_repo`] at the latest head: index load, server
/// verify and the integrity scan. A sampled pass (`effective < 100`) draws
/// `sample` when it is `None`, and checks what it names otherwise.
#[allow(clippy::too_many_arguments)]
fn check_pass(
    repo: &mut Repository,
    config: &VykarConfig,
    verify_data: bool,
    distrust_server: bool,
    progress: &mut Option<&mut dyn FnMut(CheckProgressEvent)>,
    effective: u8,
    sample: &mut Option<CheckSample>,
    shutdown: &CancelToken,
) -> Result<CheckResult> {
    repo.load_chunk_index_uncached()?;

    // Build per-pack grouping from chunk index (needed for server verify).
    let mut pack_chunks = group_by_pack(repo.chunk_index());

    // If sampling (effective < 100), the packs this index names outside the
    // sample are skipped.
    let sample: Option<&CheckSample> = if effective < 100 {
        Some(sample.get_or_insert_with(|| CheckSample::draw(repo, &pack_chunks, effective)))
    } else {
        None
    };
    let sampled_out: HashSet<PackId> = match sample {
        Some(s) => pack_chunks
            .keys()
            .filter(|p| !s.packs.contains(p))
            .copied()
            .collect(),
        None => HashSet::new(),
    };

    // Server verification and its counters only need sampled-in packs.
    // Filter in place to avoid copying every chunk's index entry.
    if !sampled_out.is_empty() {
        pack_chunks.retain(|pid, _| !sampled_out.contains(pid));
    }

    // Try server-side verify for both existence and data checks.
    let server_outcome = if !distrust_server {
        try_server_verify(
            &repo.storage,
            &pack_chunks,
            verify_data,
            repo.content_hash(),
            progress,
            shutdown,
        )
    } else {
        ServerVerifyOutcome::Fallback
    };
    // `try_server_verify` returns an outcome rather than a `Result`, so a
    // cancellation mid-batch needs raising explicitly here.
    shutdown.check()?;

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

    // Count server-covered chunks before releasing the grouping. The local
    // scan builds its own grouping, so retaining this one through the scan
    // would keep another full set of chunk entries alive during verification.
    let srv_chunks_existence: usize = verified_packs
        .iter()
        .filter_map(|p| pack_chunks.get(p))
        .map(|c| c.len())
        .sum();
    drop(pack_chunks);

    // Combined skip set: sampled_out + server-verified packs.
    let mut combined_skip = sampled_out;
    combined_skip.extend(verified_packs.iter());
    let skip = if combined_skip.is_empty() {
        None
    } else {
        Some(&combined_skip)
    };

    let scan = integrity_scan(
        repo,
        config,
        &ScanOptions {
            collect_chunk_refs: false,
            detect_orphans: false,
            verify_data,
            skip_packs: skip,
            snapshot_sample: sample.map(|s| &s.snapshots),
        },
        progress,
        shutdown,
    )?;

    // Fold the server-verify contribution into the scan's projection.
    let mut result = CheckResult::from(scan);
    result.chunks_existence_checked += srv_chunks_existence;
    result.packs_existence_checked += srv_packs_responded;
    if verify_data {
        result.chunks_data_verified += srv_chunks_verified;
    }
    // Server errors are reported ahead of the local scan's.
    result.errors.splice(0..0, srv_errors);
    Ok(result)
}

fn skipped_result() -> CheckResult {
    CheckResult {
        snapshots_checked: 0,
        key_files_checked: None,
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
    let fp = crate::repo::identity::compute_fingerprint(
        &repo.config.id,
        repo.crypto.chunk_hasher().key(),
    );
    hex::encode(fp)
}

/// Refuse to build or apply a repair plan when any snapshot is too new to
/// decode safely. Repair sweeps the index after decoding *every* surviving
/// snapshot; an unsupported one we cannot interpret would drop live chunk
/// references (mirrors the `SnapshotReadFailed` guard in `execute_repair`).
/// Plain `check` (no repair) still *reports* the issue and is unaffected.
///
/// Covers both ways a newer writer can outrun this build: a decodable envelope
/// stamped with a higher `format_version`, and an envelope whose field count
/// differs so `format_version` cannot be read at all. The second case is the
/// dangerous one — without this it lands in `CorruptSnapshot`, and repair
/// deletes the blob.
fn refuse_repair_if_unreadable_snapshots(scan: &ScanResult) -> Result<()> {
    let count = scan
        .issues
        .iter()
        .filter(|i| {
            matches!(
                i,
                IntegrityIssue::UnsupportedSnapshotVersion { .. }
                    | IntegrityIssue::IncompatibleSnapshotEnvelope { .. }
            )
        })
        .count();
    if count > 0 {
        return Err(VykarError::Other(format!(
            "aborting repair: {count} snapshot(s) were written by a newer vykar \
             (unsupported format version or incompatible metadata envelope); \
             upgrade vykar — repair leaves them untouched and refuses to sweep \
             the index without decoding them"
        )));
    }
    Ok(())
}

/// Run `check --repair`. `RepairMode::ApplyShown` behaves like
/// `RepairMode::Apply`, except that it refuses, before anything is written,
/// a plan rebuilt under the lock that differs from the one shown.
pub fn run_with_repair(
    config: &VykarConfig,
    passphrase: Option<&str>,
    verify_data: bool,
    mode: RepairMode,
    progress: Option<&mut dyn FnMut(CheckProgressEvent)>,
    shutdown: &CancelToken,
) -> Result<RepairResult> {
    shutdown.check()?;

    let opts = repair_open_options(mode);
    if mode == RepairMode::PlanOnly {
        let mut repo = crate::commands::util::open_repo(config, passphrase, opts, shutdown)?;
        run_with_repair_on_repo(&mut repo, config, verify_data, mode, progress, shutdown)
    } else {
        // A token nothing cancels: past its mutation boundary a repair must
        // not be cut short, and the read path beneath `execute_repair` does
        // not poll `shutdown` either. The scan stays cancellable by polling.
        let mut repo =
            crate::commands::util::open_repo(config, passphrase, opts, &CancelToken::new())?;
        run_with_repair_on_repo(&mut repo, config, verify_data, mode, progress, shutdown)
    }
}

/// Open options `run_with_repair` uses for `mode`.
pub(crate) fn repair_open_options(mode: RepairMode) -> OpenOptions {
    if mode == RepairMode::PlanOnly {
        // PlanOnly: no marker, no lock, purely read-only. `skip_key_backfill`
        // is what keeps that promise: the dry run prints "no changes applied",
        // and a key-copy backfill firing during the open underneath would make
        // that statement false.
        OpenOptions::new()
            .skip_key_backfill()
            .tolerate_broken_index()
    } else {
        OpenOptions::new().tolerate_broken_index()
    }
}

/// [`run_with_repair`] on a repository opened with
/// [`repair_open_options`]`(mode)`. PlanOnly takes no lock and registers no
/// marker; Apply/ApplyShown take the maintenance lock here.
pub(crate) fn run_with_repair_on_repo(
    repo: &mut Repository,
    config: &VykarConfig,
    verify_data: bool,
    mode: RepairMode,
    mut progress: Option<&mut dyn FnMut(CheckProgressEvent)>,
    shutdown: &CancelToken,
) -> Result<RepairResult> {
    let scan_opts = ScanOptions {
        collect_chunk_refs: true,
        detect_orphans: true,
        verify_data,
        skip_packs: None,
        snapshot_sample: None,
    };

    if mode == RepairMode::PlanOnly {
        let load = repo.load_chunk_index_for_repair(shutdown)?;
        repo.refresh_snapshot_list()?;

        let scan = integrity_scan(repo, config, &scan_opts, &mut progress, shutdown)?;

        // Refuse before building the plan so a dry-run never presents a
        // misleading executable RemoveUnreferencedEntries plan for a too-new repo.
        refuse_repair_if_unreadable_snapshots(&scan)?;

        let plan = plan_repair(repo, &scan, &group_by_pack(repo.chunk_index()), &load)?;

        // PlanOnly is cancellable end to end, and `plan_repair` is unguarded
        // CPU work over the scan; without this the CLI would print a plan built
        // from a cancelled run as though it were complete.
        shutdown.check()?;

        Ok(RepairResult {
            check_result: scan.into(),
            plan,
            applied: Vec::new(),
            repair_errors: Vec::new(),
        })
    } else {
        // Apply: maintenance lock, re-scan under lock, mutate state.
        crate::commands::util::with_maintenance_lock(repo, |repo| {
            let load = repo.load_chunk_index_for_repair(shutdown)?;
            repo.refresh_snapshot_list()?;

            let scan = integrity_scan(repo, config, &scan_opts, &mut progress, shutdown)?;

            // Refuse right after the under-lock scan — before the delete
            // probe and execute_repair — so not even the probe object is
            // written when a too-new snapshot is present.
            refuse_repair_if_unreadable_snapshots(&scan)?;

            let pack_chunks = group_by_pack(repo.chunk_index());
            let plan = plan_repair(repo, &scan, &pack_chunks, &load)?;

            // Before the delete probe.
            refuse_repair_if_plan_changed(mode, &plan)?;

            // A plan with data loss deletes objects. Refuse before the mutation
            // boundary: by capability on a server that advertises append-only
            // mode (no probe is sent), else by the delete probe.
            if plan.has_data_loss {
                crate::commands::util::refuse_on_append_only_server(
                    repo.storage.as_ref(),
                    "vykar check --repair",
                )?;
                if !probe_deletes_allowed(repo.storage.as_ref()) {
                    return Err(VykarError::Other(
                        "repair requires deleting immutable snapshot objects; \
                     not supported on append-only backends"
                            .into(),
                    ));
                }
            }

            // Mutation boundary. Everything above — the scan, the plan, the
            // append-only checks — is cancellable; from here on `execute_repair`
            // deletes immutable snapshot objects and rewrites the index, with
            // no resumable mid-state, so it and the read path beneath it
            // (`repair_apply`) deliberately do not poll the flag. This is the
            // last point at which aborting is free.
            shutdown.check()?;

            // Execute the repair
            let (applied, repair_errors) = execute_repair(repo, &plan, &scan.issues, &pack_chunks)?;

            Ok(RepairResult {
                check_result: scan.into(),
                plan,
                applied,
                repair_errors,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::{ChunkerParams, EncryptionMode, IndexRecovery};
    use crate::testutil::{MemoryBackend, fake_server_caps};
    use std::sync::Arc;
    use vykar_protocol::ServerCapabilities;
    use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};

    /// A vykar-server whose `/health` answers `caps`, or fails without.
    struct Server {
        inner: MemoryBackend,
        caps: Option<ServerCapabilities>,
    }

    impl InnerBackend for Server {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for Server;
        except [server_ops];

        fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
            Some(self)
        }
    }

    impl vykar_storage::ServerOps for Server {
        fn offers(&self, _op: vykar_storage::ServerOp) -> bool {
            false
        }

        fn capabilities(&self) -> Result<ServerCapabilities> {
            self.caps
                .clone()
                .ok_or_else(|| VykarError::Other("injected /health failure".into()))
        }

        fn init(&self) -> Result<()> {
            Ok(())
        }
    }

    /// The repair plan of a healthy repository whose rebuilt index took a
    /// pack from a local table, against a server answering `caps`.
    fn plan_against(caps: Option<ServerCapabilities>) -> Result<RepairPlan> {
        let mut repo = Repository::init(
            Box::new(MemoryBackend::new()),
            EncryptionMode::None,
            ChunkerParams::default(),
            None,
            None,
            None,
        )
        .unwrap();
        let config = crate::tests::helpers::make_test_config(std::path::Path::new("/mem"));
        let opts = ScanOptions {
            collect_chunk_refs: true,
            detect_orphans: false,
            verify_data: false,
            skip_packs: None,
            snapshot_sample: None,
        };
        let scan =
            integrity_scan(&mut repo, &config, &opts, &mut None, &CancelToken::new()).unwrap();
        repo.storage = Arc::new(Server {
            inner: MemoryBackend::new(),
            caps,
        });
        let load = RepairIndexLoad {
            recovery: IndexRecovery::Complete,
            uncovered_packs: Vec::new(),
            packs_from_local_tables: vec![PackId::from_bytes([7; 32])],
        };
        plan_repair(&repo, &scan, &PackChunks::new(), &load)
    }

    fn writes_pack_maps(plan: &RepairPlan) -> bool {
        plan.actions
            .iter()
            .any(|a| matches!(a, RepairAction::WritePackMaps { .. }))
    }

    #[test]
    fn plan_keeps_pack_maps_for_a_server_that_accepts_them() {
        assert!(writes_pack_maps(
            &plan_against(Some(fake_server_caps())).unwrap()
        ));
    }

    /// An older server: the plan writes no pack maps (and warns).
    #[test]
    fn plan_drops_pack_maps_for_a_server_without_packmaps() {
        let mut caps = fake_server_caps();
        caps.key_dirs.retain(|d| d != vykar_protocol::PACK_MAP_DIR);
        let plan = plan_against(Some(caps)).unwrap();
        assert!(!writes_pack_maps(&plan), "{:?}", plan.actions);
    }

    /// A failed `/health` fails the plan: nothing has changed yet.
    #[test]
    fn plan_fails_when_the_server_cannot_say() {
        let err = plan_against(None).err().unwrap().to_string();
        assert!(err.contains("injected /health failure"), "{err}");
    }
}
