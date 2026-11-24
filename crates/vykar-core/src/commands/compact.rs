use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tracing::{debug, info, warn};

use super::mark;
use super::util::{
    MaintenanceSession, list_live_sessions_registered, list_live_sessions_under_lock, open_repo,
    refuse_on_append_only_server, with_maintenance_lock, with_maintenance_lock_unlisted,
    with_maintenance_session,
};
use crate::config::VykarConfig;
use crate::index::head::{SegmentKind, SegmentRef};
use crate::index::sweep_record::{SWEEP_RECORD_VERSION, SweepRecord};
use crate::repo::OpenOptions;
use crate::repo::control::CancelToken;
use crate::repo::index_store;
use crate::repo::index_write::{MaintenanceWrite, OnMoved, PendingSweep};
use crate::repo::lock::{self, LiveSession, MarkerFlags};
use crate::repo::pack::PackWriter;
use crate::repo::pack_listing::{ListedPack, list_packs};
use crate::repo::pack_trailer::{
    PackTrailer, TrailerPackType, framed_offsets, trailer_blob_len, trailer_frame_len,
};
use crate::repo::{CommitMode, HEAD_CREATE_ATTEMPTS, Repository, TreeChunkSource, retry_delay};
use vykar_crypto::CryptoEngine;
use vykar_protocol::{
    MAX_KEEP_BLOBS_PER_OP, MAX_REPACK_OPS, MAX_REPACK_OUTPUT_BYTES, MAX_REPACK_PLAN_JSON_BYTES,
    MAX_REPACK_TRAILER_BYTES, PACK_HEADER_SIZE, RepackBlobRef, RepackOperationRequest,
    RepackPlanRequest, repack_op_json_bound, repack_op_output_size, repack_trailer_json_len,
    validate_pack_header,
};
use vykar_storage::{ServerOp, StorageBackend};
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
    /// Packs no index entry names: never committed, or emptied by this
    /// run's sweep.
    pub packs_orphan: u64,
    /// Orphan packs (no index entry: never committed, or emptied by this
    /// run's sweep) kept because they are younger than
    /// [`ORPHAN_PACK_GRACE`] on the store's clock (counted in
    /// `packs_orphan` too).
    pub packs_orphan_young: u64,
    /// Orphan packs kept because their age is unknown: the listing gave no
    /// mtime, or there was no reading of "now" (counted in `packs_orphan`
    /// too). On a storage that reports no modification times this is
    /// every orphan pack, every run.
    pub packs_orphan_unknown_age: u64,
    /// Orphan packs kept because they lie inside a live backup's horizon
    /// (its marker's mtime − elapsed − M), or because a live marker gives no
    /// horizon (counted in `packs_orphan` too; not in the young or
    /// unknown-age counts).
    pub packs_orphan_live: u64,
    /// Stale `.tmp.*` upload debris removed by the sweep.
    pub temp_files_removed: u64,
    /// Lock-free (04 step 9): repack outputs this run wrote that no index
    /// entry names at its end, because a rebase dropped every relocation
    /// into them (another compact had moved those chunks first). Young
    /// orphans, which a compact deletes once they are 12 h old; their
    /// sources are not counted in `packs_repacked` or `space_freed`.
    pub packs_abandoned: u64,
    /// Lock-free (04 step 9): packs with unused space over the threshold
    /// that this run did not repack because the source rule held them back:
    /// younger than [`ORPHAN_PACK_GRACE`] on the store's clock, of unknown
    /// age, or inside a live backup's horizon. A later compact repacks them.
    pub packs_repack_held: u64,
    /// Snapshots the mark covered.
    pub snapshots_marked: u64,
    /// Index entries no live snapshot references, removed by the sweep
    /// (dry run: that would be removed), and their stored bytes.
    pub chunks_unreferenced: u64,
    pub bytes_unreferenced: u64,
    /// Unmarked index entries the sweep kept because a backup segment
    /// younger than T_pack added them (04 step 7), and their stored bytes;
    /// a compact once those segments are T_pack old removes them.
    pub chunks_unreferenced_young: u64,
    pub bytes_unreferenced_young: u64,
    /// A lock-free compact (04 step 9) that removed no index entry this
    /// run, and why; `None` when the sweep ran (or nothing was
    /// unreferenced).
    pub sweep_skipped: Option<SweepSkipped>,
}

/// Why a lock-free compact removed no index entry (04 step 9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SweepSkipped {
    /// A backup was committing (its session marker says so, or cannot be
    /// read): its snapshot may not be listed yet, so this run's mark would
    /// decide nothing.
    Committing,
    /// The index head moved during each of [`COMPACT_MARK_ATTEMPTS`] marks
    /// (backups committing all the time).
    Contention,
}

/// Marks a lock-free compact makes before it gives up on its sweep for
/// this run (04 step 9): each further one marks only the snapshots listed
/// since the one before (an incremental re-mark).
pub const COMPACT_MARK_ATTEMPTS: u32 = 3;

/// Prefix of the error a compact returns when the mark fails; matched by
/// the simulator (`sim/history.rs`).
pub(crate) const MARK_REFUSED: &str = "compact refused: the live snapshots could not be marked";

const STALE_TEMP_FILE_AGE: std::time::Duration = std::time::Duration::from_secs(3600);

/// How old on the store's clock a pack no index entry names must be before
/// compact deletes it (T_pack, 04-commit-protocol.md "Constants").
pub const ORPHAN_PACK_GRACE: std::time::Duration = crate::repo::ages::T_PACK;

/// Whether an orphan with mtime `mtime` may go: always at zero grace;
/// otherwise only with an mtime, a reading of "now", and an age of at
/// least `grace`.
fn old_enough(
    mtime: Option<std::time::SystemTime>,
    now: Option<vykar_storage::StoreNow>,
    grace: std::time::Duration,
) -> bool {
    if grace.is_zero() {
        return true;
    }
    match (mtime, now) {
        (Some(mtime), Some(now)) => now.age_of(mtime) >= grace,
        _ => false,
    }
}

/// Whether an orphan with mtime `mtime` lies before every live session's
/// horizon (04 "Compact" 3): always with no live session; never when a
/// horizon is unknown (`None`) or the pack has no mtime.
fn before_horizons(
    mtime: Option<std::time::SystemTime>,
    horizons: &Option<Vec<std::time::SystemTime>>,
) -> bool {
    match horizons {
        Some(h) if h.is_empty() => true,
        Some(h) => mtime.is_some_and(|m| h.iter().all(|h| m < *h)),
        None => false,
    }
}

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

/// What a mark found in the index (`vykar compact --report-unreferenced`).
/// Nothing on storage changes to produce it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnreferencedReport {
    pub snapshots: usize,
    pub tree_chunks: usize,
    pub tree_stored_bytes: u64,
    pub items: u64,
    pub references: u64,
    pub entries: usize,
    pub referenced: usize,
    /// Index entries no live snapshot references, sorted by chunk ID. The
    /// next compact keeps those a backup segment younger than T_pack added
    /// (04 step 7).
    pub unreferenced: Vec<ChunkId>,
    pub unreferenced_bytes: u64,
    /// Packs holding at least one unreferenced entry / only unreferenced ones.
    pub packs_with_unreferenced: usize,
    pub packs_only_unreferenced: usize,
    /// Stored bytes (sum of `stored_size`) of the entries in the packs
    /// counted by `packs_only_unreferenced`.
    pub packs_only_unreferenced_bytes: u64,
}

/// Open the repository and run `report_unreferenced_on_repo`: the report of
/// `vykar compact --report-unreferenced`.
pub fn report_unreferenced(
    config: &VykarConfig,
    passphrase: Option<&str>,
    shutdown: &CancelToken,
) -> Result<UnreferencedReport> {
    let mut repo = open_repo(config, passphrase, OpenOptions::new(), shutdown)?;
    report_unreferenced_on_repo(&mut repo, shutdown)
}

/// Load the index, list snapshots strictly, mark from the tree-chunk cache
/// first, and tally the index against the mark. Fails closed. On success,
/// drops cached tree chunks no live snapshot uses.
///
/// Read-only for the repository's data: no index, pack or snapshot object is
/// written. In lock mode it runs under the maintenance lock, whose
/// stale-session cleanup can delete stale session markers and orphaned
/// session journals, exactly as `compact --dry-run` does. In lock-free mode
/// (04 step 9) it takes no lock, registers no session and reaps nothing: it
/// writes nothing a writer depends on. A mark that fails because a backup
/// committed between the head read and the snapshot listing (a moved head)
/// is retried on the latest head, at most [`COMPACT_MARK_ATTEMPTS`] times.
pub(crate) fn report_unreferenced_on_repo(
    repo: &mut Repository,
    shutdown: &CancelToken,
) -> Result<UnreferencedReport> {
    let frame = |e: VykarError| {
        if mark::passes_through(&e) {
            e
        } else {
            VykarError::Other(format!(
                "cannot report unreferenced chunks, nothing was changed: {e}. \
                 Run `vykar check` for details"
            ))
        }
    };
    match repo.commit_mode()? {
        CommitMode::Lock => with_maintenance_lock(repo, |repo| {
            repo.load_chunk_index_under_lock()?;
            report_under_lock(repo, shutdown).map_err(frame)
        }),
        CommitMode::LockFree => {
            repo.load_chunk_index_under_lock()?;
            let mut attempt = 1;
            loop {
                match report_under_lock(repo, shutdown) {
                    Ok(report) => return Ok(report),
                    Err(e) if mark::passes_through(&e) => return Err(e),
                    Err(e) if attempt < COMPACT_MARK_ATTEMPTS && head_moved(repo)? => {
                        debug!(
                            "the index head moved during the report's mark ({e}); marking again"
                        );
                        repo.load_chunk_index_under_lock()?;
                        attempt += 1;
                    }
                    Err(e) => return Err(frame(e)),
                }
            }
        }
    }
}

/// Whether the latest head on storage is not the one the index was loaded
/// at (a lock-free compact's or report's mark, 04 step 9).
fn head_moved(repo: &Repository) -> Result<bool> {
    let latest = index_store::fetch_head(repo.storage.as_ref(), repo.crypto.as_ref())?;
    Ok(repo.index_head() != Some(&latest))
}

/// The report of [`report_unreferenced_on_repo`] over the loaded index.
fn report_under_lock(repo: &mut Repository, shutdown: &CancelToken) -> Result<UnreferencedReport> {
    let snaps = mark::load_snapshots_strict(repo, shutdown)?;
    let names: Vec<String> = snaps.iter().map(mark::LiveSnapshot::label).collect();
    let ptrs: Vec<&[ChunkId]> = snaps.iter().map(|s| s.meta.item_ptrs.as_slice()).collect();
    let set = mark::TreeChunkSet::build(&ptrs);

    let positions = repo.chunk_index().positions();
    let found = mark::mark_live(
        repo,
        &set,
        &names,
        &positions,
        TreeChunkSource::CacheFirst,
        mark::MarkOptions {
            count: false,
            missing_refs: mark::MissingRefs::Abort,
            decode_threads: mark::default_decode_threads(),
        },
        shutdown,
    )?;

    let mut report = UnreferencedReport {
        snapshots: snaps.len(),
        tree_chunks: set.len(),
        tree_stored_bytes: found.walk.stored_bytes,
        items: found.items,
        references: found.references,
        ..UnreferencedReport::default()
    };
    // Per pack: (unreferenced entries, referenced entries, stored bytes).
    let mut packs: HashMap<PackId, (u32, u32, u64)> = HashMap::new();
    positions.for_each_live(|p, id, e| {
        report.entries += 1;
        let tally = packs.entry(e.pack_id).or_default();
        tally.2 += u64::from(e.stored_size);
        let marked = found.is_marked(p);
        if marked {
            report.referenced += 1;
            tally.1 += 1;
        } else {
            report.unreferenced.push(id);
            report.unreferenced_bytes += u64::from(e.stored_size);
            tally.0 += 1;
        }
    });
    drop(positions);
    for (unreferenced, referenced, bytes) in packs.values() {
        if *unreferenced > 0 {
            report.packs_with_unreferenced += 1;
            if *referenced == 0 {
                report.packs_only_unreferenced += 1;
                report.packs_only_unreferenced_bytes += bytes;
            }
        }
    }
    report
        .unreferenced
        .sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

    if let Some(cache) = repo.tree_cache() {
        cache.retain(|id| set.contains(id));
    }
    Ok(report)
}

/// [`run`] on an already opened repository, with a sanitised `threshold`.
/// The chunk index is loaded under the maintenance lock, so any index loaded
/// at open is replaced. On an append-only server a non-dry run is refused
/// before the lock.
///
/// Under the maintenance lock it lists in 04's order ("Compact" 1): the
/// packs first, then the sessions (reap and live listing, the horizons),
/// then the head (and, in the mark, the ages of `index.d/` objects). Why
/// this order is correct **under the lock**: let P be a pack the listing
/// shows as an old orphan that a head H' compact's head read does not see
/// will name. If P's session S is in the session listing, P lies after S's
/// horizon (or is a recovered journal pack S's pull-back covers) and is
/// kept. If S is not in it, either S registered after the session listing,
/// so it PUT P after the pack listing, which then does not show P (a
/// recovered journal pack adopted after the listing is younger than
/// T_pack/2 in it and kept by age); or S deregistered before it, which S
/// does only after its snapshot PUT, after H', which then precedes the
/// head read: a contradiction. A pack H' names through a reused chunk was
/// named by the head S loaded; if this run's head no longer names it, a
/// sweep or repack removed it and S's commit re-checks
/// (`StaleChunksDuringCommit`). The older order (sessions, head, packs)
/// was correct only through the registration probe against the lock.
/// One consequence under the lock (D-a2 accepts it): on a plaintext
/// repository a live backup's re-PUT, during the mark, of a pack whose
/// bytes, hence key, equal those of a pack the listing showed as an old
/// orphan no longer renews the mtime this run judges by, so compact deletes
/// the pack and that backup's commit fails with the typed "missing from
/// storage" error.
///
/// The same order holds for a lock-free compact (04 step 9: an encrypted
/// repository whose storage passed the put-if-absent check, see
/// [`Repository::commit_mode`]), which registers a session marker
/// (`writes_heads`) instead of taking the lock, and reads "now" from its
/// own marker key in the session listing. The argument above needs no lock
/// once its conditions hold: object names a writer cannot re-create after
/// the listing saw them (encrypted repositories, random nonces; a
/// plaintext pack or index object is named by its content, which is why
/// plaintext repositories keep the lock, O1); listings that show every
/// object PUT before them (the storage contract); and a session whose
/// marker was reaped while it still commits left to the post-PUT pack-age
/// check (I8) and to the lock-free commit's marker probe before its
/// snapshot PUT. What the lock gave beyond that, a head nobody else moves
/// while compact works, is replaced in [`mark_and_sweep_lock_free`] (the
/// second session listing, the re-read head, the incremental re-mark) and
/// [`persist_batch_lock_free`] (the relocation rebase).
pub(crate) fn run_on_repo(
    repo: &mut Repository,
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
    shutdown: &CancelToken,
) -> Result<CompactStats> {
    if !dry_run {
        refuse_on_append_only_server(repo.storage.as_ref(), "vykar compact")?;
    }
    let concurrency = repo.storage.profile().request_width;
    let opts = CompactOpts {
        threshold,
        max_repack_size,
        dry_run,
        concurrency,
    };

    match repo.commit_mode()? {
        CommitMode::Lock => with_maintenance_lock_unlisted(repo, |repo| {
            let listing = take_pack_listing(repo, dry_run, concurrency)?; // 1. packs
            list_live_sessions_under_lock(repo)?; // 2. sessions
            repo.load_chunk_index_under_lock()?; // 3. head
            compact_repo_listed(repo, None, listing, opts, shutdown)
        }),
        CommitMode::LockFree => {
            // A dry run writes no head: it does not make lock holders yield.
            let flags = MarkerFlags {
                writes_heads: !dry_run,
                ..MarkerFlags::default()
            };
            with_maintenance_session(repo, flags, |repo, own| {
                let listing = take_pack_listing(repo, dry_run, concurrency)?; // 1. packs
                list_live_sessions_registered(repo, own)?; // 2. sessions #1, "now"
                repo.load_chunk_index_under_lock()?; // 3. head
                compact_repo_listed(repo, Some(own), listing, opts, shutdown)
            })
        }
    }
}

/// The knobs of one compact run.
#[derive(Clone, Copy)]
struct CompactOpts {
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
    concurrency: usize,
}

/// What compact's pack listing returned, taken before the session listing
/// and the head read (04 "Compact" 1). The analysis, the repack and the
/// orphan decisions use it; a pack PUT after it is not considered by this
/// run.
pub(crate) struct PackListing {
    discovered: Vec<ListedPack>,
    temp_files_removed: u64,
}

/// Sweep stale temp files (not in a dry run), then list the packs: one
/// listing on a vykar-server, else 256 per-shard listings in parallel.
/// The sweep runs before the session listing and the mark, so a compact that
/// then refuses ("nothing was changed") has already removed the temp files
/// older than `STALE_TEMP_FILE_AGE`: upload debris only, never an object.
fn take_pack_listing(repo: &Repository, dry_run: bool, concurrency: usize) -> Result<PackListing> {
    // Sweep upload debris once, from the root, before listing shards. Temp
    // files younger than this may belong to a live upload.
    let mut temp_files_removed = 0;
    if !dry_run {
        match repo.storage.remove_stale_temp_files(STALE_TEMP_FILE_AGE) {
            Ok(n) => temp_files_removed = n as u64,
            Err(e) => tracing::warn!("compact: failed to sweep stale temp files: {e}"),
        }
    }
    let discovered = list_packs(repo.storage.as_ref(), concurrency)?;
    Ok(PackListing {
        discovered,
        temp_files_removed,
    })
}

/// Core compact logic operating on an already-opened repository whose index
/// was loaded under the maintenance lock, in this order:
///
/// 1. mark every live snapshot and sweep the unmarked entries from the
///    index (a dry run removes them from the in-memory index only);
/// 2. analyse, repack and delete packs over the swept index;
/// 3. unless `dry_run`: the index upkeep (a fold at the soft limit, the
///    age-gated garbage collection), then the deletion of pack maps whose
///    packs are all gone.
///
/// Nothing is deleted or repacked without a completed mark of the same run,
/// against the head loaded under the same lock. A dry run writes nothing
/// and leaves the handle's index as it was loaded.
///
/// It lists the packs itself, after its caller loaded the head: callers
/// get head, then packs. Production goes through `run_on_repo`, which
/// lists in 04's order.
pub fn compact_repo(
    repo: &mut Repository,
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
    concurrency: usize,
    shutdown: &CancelToken,
) -> Result<CompactStats> {
    let listing = take_pack_listing(repo, dry_run, concurrency)?;
    let opts = CompactOpts {
        threshold,
        max_repack_size,
        dry_run,
        concurrency,
    };
    compact_repo_listed(repo, None, listing, opts, shutdown)
}

/// [`compact_repo`] over a pack listing its caller took; `free`: the
/// lock-free run's session (04 step 9), `None` under the lock.
fn compact_repo_listed(
    repo: &mut Repository,
    free: Option<&MaintenanceSession>,
    listing: PackListing,
    opts: CompactOpts,
    shutdown: &CancelToken,
) -> Result<CompactStats> {
    let swept = match free {
        None => mark_and_sweep(repo, opts.dry_run, shutdown)?,
        Some(own) => mark_and_sweep_lock_free(repo, own, opts.dry_run, shutdown)?,
    };
    let mut stats = compact_after_mark(
        repo,
        free.map(MaintenanceSession::id),
        listing,
        opts,
        shutdown,
    )?;
    stats.snapshots_marked = swept.snapshots;
    stats.chunks_unreferenced = swept.entries_removed;
    stats.bytes_unreferenced = swept.bytes_removed;
    stats.chunks_unreferenced_young = swept.kept_young;
    stats.bytes_unreferenced_young = swept.kept_young_bytes;
    stats.sweep_skipped = swept.skipped;
    Ok(stats)
}

/// [`compact_repo`] without the mark, for unit tests of the pack analysis
/// and repack over an index built by hand (no snapshot references it).
/// Production code always marks first.
#[cfg(test)]
pub(crate) fn compact_repo_without_mark(
    repo: &mut Repository,
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
    concurrency: usize,
    shutdown: &CancelToken,
) -> Result<CompactStats> {
    let listing = take_pack_listing(repo, dry_run, concurrency)?;
    let opts = CompactOpts {
        threshold,
        max_repack_size,
        dry_run,
        concurrency,
    };
    compact_after_mark(repo, None, listing, opts, shutdown)
}

/// Everything [`compact_repo`] does after the mark-and-sweep; `free`: the
/// lock-free run's session ID.
fn compact_after_mark(
    repo: &mut Repository,
    free: Option<&str>,
    listing: PackListing,
    opts: CompactOpts,
    shutdown: &CancelToken,
) -> Result<CompactStats> {
    let CompactOpts {
        dry_run,
        concurrency,
        ..
    } = opts;
    let run = compact_packs(repo, free, listing, opts, shutdown);
    if dry_run {
        repo.discard_index_edits();
    }
    let PackRun {
        mut stats,
        listed,
        deleted,
        written,
    } = run?;
    if free.is_some() && !dry_run && !written.is_empty() {
        // Outputs no entry names: a rebase dropped every relocation into
        // them (another compact moved those chunks first).
        let named: HashSet<PackId> = repo.chunk_index().iter().map(|(_, e)| e.pack_id).collect();
        stats.packs_abandoned = written.iter().filter(|p| !named.contains(p)).count() as u64;
        if stats.packs_abandoned > 0 {
            info!(
                abandoned = stats.packs_abandoned,
                "compact: repack outputs abandoned (another compact repacked their chunks \
                 first); a compact deletes them once they are 12 h old"
            );
        }
    }
    if !dry_run {
        repo.maintain_segmented_index(if free.is_some() {
            OnMoved::Skip
        } else {
            OnMoved::Fail
        })?;
        let alive =
            |p: &PackId| (listed.contains(p) && !deleted.contains(p)) || written.contains(p);
        if free.is_some() {
            repo.check_lock_fence()?;
        }
        match crate::repo::pack_map::delete_dead_pack_maps(
            repo.storage.as_ref(),
            repo.crypto.as_ref(),
            concurrency,
            &alive,
            shutdown,
        ) {
            Ok(n) => debug!("deleted {n} pack maps"),
            Err(e) => warn!("compact: pack maps were not cleaned up: {e}"),
        }
    }
    Ok(stats)
}

/// What the mark found, before anything is written.
struct SweepPlan {
    snapshots: Vec<vykar_types::snapshot_id::SnapshotId>,
    tree_set: mark::TreeChunkSet,
    /// Unmarked entries, in chunk-ID order, with their stored sizes.
    unreferenced: Vec<(ChunkId, u32)>,
    entries_live: u64,
    bytes_live: u64,
    references: u64,
    referenced_bytes: u64,
    /// Unmarked entries left in the index because a young backup segment
    /// added them (04 step 7), and their stored bytes.
    kept_young: u64,
    kept_young_bytes: u64,
}

impl SweepPlan {
    fn bytes_removed(&self) -> u64 {
        self.unreferenced.iter().map(|(_, s)| u64::from(*s)).sum()
    }
}

/// The figures of a mark-and-sweep, for the stats.
#[derive(Default)]
struct Swept {
    snapshots: u64,
    entries_removed: u64,
    bytes_removed: u64,
    kept_young: u64,
    kept_young_bytes: u64,
    skipped: Option<SweepSkipped>,
}

/// Strict listing, the guards on it, the tree-chunk set, then `mark_live`
/// with `CacheFirst`, counts and `MissingRefs::Abort`. Read-only. Any error
/// is framed as [`MARK_REFUSED`], unless [`mark::passes_through`] (cancel,
/// newer vykar). "Nothing was changed" means no object: the stale temp-file
/// sweep of `take_pack_listing` ran before.
fn plan_sweep(repo: &Repository, shutdown: &CancelToken) -> Result<SweepPlan> {
    plan_sweep_inner(repo, shutdown).map_err(mark_refused)
}

/// `e` framed as [`MARK_REFUSED`], unless [`mark::passes_through`].
fn mark_refused(e: VykarError) -> VykarError {
    if mark::passes_through(&e) {
        e
    } else {
        VykarError::Other(format!(
            "{MARK_REFUSED}, nothing was changed: {e}. Run `vykar check` for details"
        ))
    }
}

/// The guards on a strict listing before a sweep may delete by it.
fn check_sweep_listing(repo: &Repository, snaps: &[mark::LiveSnapshot]) -> Result<()> {
    let entries = repo.chunk_index().len();
    // The storage contract allows a stale or empty listing; a sweep deletes
    // by it, so a listing that looks incomplete refuses.
    if snaps.is_empty() && entries > 0 {
        return Err(VykarError::Other(format!(
            "no snapshot was listed under snapshots/ while the index holds {entries} chunks; \
             this vykar does not sweep a repository to empty (`vykar delete` removes a \
             repository)"
        )));
    }
    check_listing_against_last_sweep(repo, snaps)
}

fn plan_sweep_inner(repo: &Repository, shutdown: &CancelToken) -> Result<SweepPlan> {
    let snaps = mark::load_snapshots_strict(repo, shutdown)?;
    check_sweep_listing(repo, &snaps)?;

    let names: Vec<String> = snaps.iter().map(mark::LiveSnapshot::label).collect();
    let ptrs: Vec<&[ChunkId]> = snaps.iter().map(|s| s.meta.item_ptrs.as_slice()).collect();
    let tree_set = mark::TreeChunkSet::build(&ptrs);
    let positions = repo.chunk_index().positions();
    let found = mark::mark_live(
        repo,
        &tree_set,
        &names,
        &positions,
        TreeChunkSource::CacheFirst,
        mark::MarkOptions {
            count: true,
            missing_refs: mark::MissingRefs::Abort,
            decode_threads: mark::default_decode_threads(),
        },
        shutdown,
    )?;
    let mut plan = SweepPlan {
        snapshots: snaps.iter().map(|s| s.id).collect(),
        tree_set: mark::TreeChunkSet::build(&[]),
        unreferenced: Vec::new(),
        entries_live: 0,
        bytes_live: 0,
        references: found.references,
        referenced_bytes: 0,
        kept_young: 0,
        kept_young_bytes: 0,
    };
    positions.for_each_live(|p, id, e| {
        if found.is_marked(p) {
            plan.entries_live += 1;
            plan.bytes_live += u64::from(e.stored_size);
            plan.referenced_bytes +=
                u64::from(e.stored_size) * u64::from(found.count(p).unwrap_or(0));
        } else {
            plan.unreferenced.push((id, e.stored_size));
        }
    });
    drop(positions);
    plan.tree_set = tree_set;
    plan.unreferenced
        .sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    // 04 "Compact" 6: an entry a backup segment younger than the segment
    // grace added stays, marked or not: its snapshot may not be visible yet,
    // or a running backup may have reused it.
    let (kept, kept_bytes) = keep_young_additions(repo, &mut plan.unreferenced)?;
    plan.kept_young = kept;
    plan.kept_young_bytes = kept_bytes;
    plan.snapshots
        .sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    Ok(plan)
}

/// Drop from `unreferenced` (sorted by chunk ID) every chunk a young backup
/// segment of the loaded head adds (04 step 7: a backup segment not yet
/// [`ages::foldable`]); returns the count and stored bytes dropped. A
/// segment is read from the local cache, else GET; one decoded segment is
/// held at a time; its additions and `unreferenced` are both sorted, so
/// each is one merge pass. Nothing is read when `unreferenced` is empty.
///
/// [`ages::foldable`]: crate::repo::ages::foldable
fn keep_young_additions(
    repo: &Repository,
    unreferenced: &mut Vec<(ChunkId, u32)>,
) -> Result<(u64, u64)> {
    let Some(head) = repo.index_head() else {
        return Ok((0, 0));
    };
    let young: Vec<&SegmentRef> = head
        .segments
        .iter()
        .filter(|s| {
            s.kind == SegmentKind::Backup
                && !crate::repo::ages::foldable(s, repo.segment_ages(), repo.segment_grace())
        })
        .collect();
    let (mut kept, mut kept_bytes) = (0u64, 0u64);
    let dir = repo.cache_dir();
    for r in young {
        if unreferenced.is_empty() {
            break;
        }
        let bytes = match dir
            .as_deref()
            .and_then(|d| crate::index::table::read_cached_segment(d, r))
        {
            Some(b) => b,
            None => {
                let key = index_store::object_key(&r.name);
                repo.storage
                    .get(&key)?
                    .ok_or(VykarError::IndexObjectMissing {
                        kind: "segment",
                        key,
                    })?
            }
        };
        let body = index_store::decode_segment_v2(r, &bytes, repo.crypto.as_ref())?;
        drop(bytes);
        // Both sorted by chunk ID: one merge pass.
        let mut adds = body.additions.iter().map(|(id, _)| id).peekable();
        unreferenced.retain(|(id, size)| {
            while adds.next_if(|a| a.as_bytes() < id.as_bytes()).is_some() {}
            if adds.peek().is_some_and(|a| *a == id) {
                kept += 1;
                kept_bytes += u64::from(*size);
                false
            } else {
                true
            }
        });
    }
    Ok((kept, kept_bytes))
}

/// Refuse when a snapshot the previous sweep marked is missing from the
/// listing but still exists (the listing is stale or truncated). One
/// `exists` per snapshot deleted since the last sweep. The record only
/// feeds this refusal, never a deletion; a lost or damaged record
/// ([`index_store::is_object_damage`]) skips the check with a warning (the
/// check can only refuse more). Any other error on it (storage, a record of
/// a newer version) is returned, and [`plan_sweep`] refuses.
fn check_listing_against_last_sweep(repo: &Repository, snaps: &[mark::LiveSnapshot]) -> Result<()> {
    let Some(head) = repo.index_head() else {
        return Ok(());
    };
    let record = match index_store::fetch_sweep_record(
        repo.storage.as_ref(),
        repo.crypto.as_ref(),
        head,
    ) {
        Ok(Some(r)) => r,
        Ok(None) => return Ok(()),
        Err(e) if index_store::is_object_damage(&e) => {
            warn!(
                "compact: the last sweep record is missing or damaged ({e}); not checking the listing against it"
            );
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let listed: HashSet<[u8; 32]> = snaps.iter().map(|s| *s.id.as_bytes()).collect();
    for id in &record.snapshots {
        let Ok(bytes) = <[u8; 32]>::try_from(id.as_slice()) else {
            continue;
        };
        if listed.contains(&bytes) {
            continue;
        }
        let key = vykar_types::snapshot_id::SnapshotId::from_bytes(bytes).storage_key();
        if repo.storage.exists(&key)? {
            return Err(VykarError::Other(format!(
                "{key} exists but the listing of snapshots/ did not return it; the \
                 listing is stale or incomplete"
            )));
        }
    }
    Ok(())
}

/// Remove `plan.unreferenced` from the index and persist: one maintenance
/// segment of removals plus the sweep record and a head with
/// `sweep {gen: its own generation, record}`; with nothing to remove, the
/// record and a head only. Then drop the tree-chunk cache entries the mark
/// did not reach. Not called in a dry run.
fn apply_sweep(repo: &mut Repository, plan: &SweepPlan) -> Result<()> {
    stage_sweep(repo, plan);
    repo.save_state()?;
    trim_tree_cache(repo, plan);
    Ok(())
}

/// [`apply_sweep`]'s edits: the removals and the pending sweep record,
/// not yet persisted.
fn stage_sweep(repo: &mut Repository, plan: &SweepPlan) {
    let index = repo.chunk_index_mut();
    for (id, _) in &plan.unreferenced {
        index.remove(id);
    }
    let record = SweepRecord {
        version: SWEEP_RECORD_VERSION,
        generation: 0,
        time: repo.host_env().now_utc(),
        snapshots: plan
            .snapshots
            .iter()
            .map(|id| serde_bytes::ByteBuf::from(id.as_bytes().to_vec()))
            .collect(),
        tree_chunks: plan.tree_set.len() as u64,
        entries_live: plan.entries_live,
        bytes_live: plan.bytes_live,
        entries_removed: plan.unreferenced.len() as u64,
        bytes_removed: plan.bytes_removed(),
        references: plan.references,
        referenced_bytes: plan.referenced_bytes,
    };
    repo.set_pending_sweep(PendingSweep::Swept(record));
    repo.mark_index_dirty();
}

/// Drop the tree-chunk cache entries the mark did not reach (after a
/// persisted sweep).
fn trim_tree_cache(repo: &Repository, plan: &SweepPlan) {
    if let Some(cache) = repo.tree_cache() {
        let kept = cache.retain(|id| plan.tree_set.contains(id));
        debug!(?kept, "tree-chunk cache trimmed to the marked tree chunks");
    }
}

/// Mark and sweep: [`plan_sweep`], then [`apply_sweep`] unless `dry_run`. A
/// dry run removes the unreferenced entries from the in-memory index only,
/// so that the analysis after it shows what a real run would free; the
/// caller discards those edits.
fn mark_and_sweep(repo: &mut Repository, dry_run: bool, shutdown: &CancelToken) -> Result<Swept> {
    // The ages of the head's backup segments (04 step 7), one listing per
    // lock hold; a failed listing returns before anything changed.
    if let Some(head) = repo.index_head()
        && repo.needs_segment_ages(&head.segments)
    {
        repo.load_segment_ages()?;
    }
    let plan = plan_sweep(repo, shutdown)?;
    let swept = Swept {
        snapshots: plan.snapshots.len() as u64,
        entries_removed: plan.unreferenced.len() as u64,
        bytes_removed: plan.bytes_removed(),
        kept_young: plan.kept_young,
        kept_young_bytes: plan.kept_young_bytes,
        skipped: None,
    };
    debug!(
        snapshots = swept.snapshots,
        removed = swept.entries_removed,
        bytes = swept.bytes_removed,
        kept_young = swept.kept_young,
        dry_run,
        "mark-and-sweep"
    );
    if dry_run {
        let index = repo.chunk_index_mut();
        for (id, _) in &plan.unreferenced {
            index.remove(id);
        }
    } else {
        apply_sweep(repo, &plan)?;
    }
    Ok(swept)
}

/// What a lock-free mark carries from one attempt to the next (04 step 9:
/// the incremental re-mark after a moved head).
struct MarkSoFar {
    /// The snapshots marked so far, by ID.
    marked: HashSet<[u8; 32]>,
    snapshots: Vec<vykar_types::snapshot_id::SnapshotId>,
    /// The `item_ptrs` of the snapshots marked so far.
    ptrs: Vec<Vec<ChunkId>>,
    /// The entries no snapshot marked so far references, sorted by chunk
    /// ID, with their stored sizes: every removal is one of them.
    candidates: Vec<(ChunkId, u32)>,
    references: u64,
    referenced_bytes: u64,
}

/// Drop the index edits and load the index at the latest head (a lock-free
/// mark that found the head moved).
fn reload_index(repo: &mut Repository) -> Result<()> {
    repo.discard_index_edits();
    repo.load_chunk_index_under_lock().map_err(mark_refused)
}

/// [`mark_and_sweep`] without the lock (04 step 9, 3.4 of the plan), after
/// the pack listing, sessions #1 and the head read. Each attempt:
///
/// 4. **sessions #2** ([`lock::committing_sessions`]): another session
///    that is committing (or whose marker cannot be read) → this run
///    removes nothing (`SweepSkipped::Committing`): that backup's snapshot
///    may not be listed yet, and it may have reused a chunk the mark would
///    find unreferenced. A backup that deregistered before this listing had
///    PUT its snapshot, so the listing below shows it.
/// 5. the strict snapshot listing, then the head again: a head that moved
///    since the index was loaded (a backup committed in between) reloads
///    the index and starts the attempt again, so every listed snapshot's
///    head is in the index the mark runs on. The mark marks only the
///    snapshots no earlier attempt marked, and the candidates for removal
///    are the earlier attempts' unmarked entries that are still in the
///    index and unmarked by this one; entries added since are never
///    candidates (and are young backup additions anyway). A mark that fails
///    on a moved head (a chunk not in the index) is a moved head too.
/// 6. the sweep head, created with put-if-absent at the loaded head's
///    generation + 1 ([`Repository::persist_maintenance`]): a moved head
///    or a taken generation discards the edits and goes to 4 again.
///
/// After [`COMPACT_MARK_ATTEMPTS`] the run removes nothing
/// (`SweepSkipped::Contention`); the index is then at the latest head
/// loaded. Why nothing a running backup reused can be removed: a backup
/// flags its marker `committing` before its head read and keeps the flag
/// until its snapshot is PUT, and a reuse-only commit creates a head too.
/// If sessions #2 followed the flag, the run skips (or the backup had
/// deregistered and its snapshot is marked); otherwise the backup's head
/// read followed this run's head read, and either it reads this run's sweep
/// head (its reuse re-check fails it with `StaleChunksDuringCommit`) or one
/// of the two head creates at the same generation is taken (the backup
/// rebases onto the sweep and fails the same way, or this run marks again
/// and sees the flag or the snapshot), or a head newer than this run's
/// exists before its create (moved: marks again). Residual: a backup whose
/// marker was reaped by age (a 45-minute stall) while it commits (R1). This
/// run's own marker is checked before the sweep head's create (the lock
/// fence of a lock-free run, `util::with_maintenance_session`): a reaped
/// marker means a lock-path backup no longer yielded to it and may have
/// committed a reuse-only snapshot without a head that this mark did not
/// see; only a stall inside that create remains (`AgedSessionReaped`).
fn mark_and_sweep_lock_free(
    repo: &mut Repository,
    own: &MaintenanceSession,
    dry_run: bool,
    shutdown: &CancelToken,
) -> Result<Swept> {
    // The ages of the loaded head's backup segments, listed once; segments
    // of heads read later are unknown, hence young (kept).
    if let Some(head) = repo.index_head()
        && repo.needs_segment_ages(&head.segments)
    {
        repo.load_segment_ages()?;
    }
    let mut so_far: Option<MarkSoFar> = None;
    for attempt in 1..=COMPACT_MARK_ATTEMPTS {
        shutdown.check()?;
        // 4. Sessions #2.
        let committing =
            lock::committing_sessions(repo.storage.as_ref(), own.id()).map_err(mark_refused)?;
        if !committing.is_empty() {
            info!(
                sessions = ?committing,
                "compact: a backup is committing; no index entry is removed this run"
            );
            return Ok(Swept {
                snapshots: so_far.as_ref().map_or(0, |m| m.marked.len() as u64),
                skipped: Some(SweepSkipped::Committing),
                ..Swept::default()
            });
        }
        // 5. The snapshots, then the head again.
        let snaps = mark::load_snapshots_strict(repo, shutdown).map_err(mark_refused)?;
        if head_moved(repo).map_err(mark_refused)? {
            debug!(
                attempt,
                "the index head moved before the mark; marking on the latest head"
            );
            reload_index(repo)?;
            continue;
        }
        check_sweep_listing(repo, &snaps).map_err(mark_refused)?;
        let fresh: Vec<&mark::LiveSnapshot> = snaps
            .iter()
            .filter(|s| {
                so_far
                    .as_ref()
                    .is_none_or(|m| !m.marked.contains(s.id.as_bytes()))
            })
            .collect();
        let names: Vec<String> = fresh.iter().map(|s| s.label()).collect();
        let ptrs: Vec<&[ChunkId]> = fresh.iter().map(|s| s.meta.item_ptrs.as_slice()).collect();
        let set = mark::TreeChunkSet::build(&ptrs);
        let positions = repo.chunk_index().positions();
        let found = match mark::mark_live(
            repo,
            &set,
            &names,
            &positions,
            TreeChunkSource::CacheFirst,
            mark::MarkOptions {
                count: true,
                missing_refs: mark::MissingRefs::Abort,
                decode_threads: mark::default_decode_threads(),
            },
            shutdown,
        ) {
            Ok(found) => found,
            Err(e) if mark::passes_through(&e) => return Err(e),
            Err(e) => {
                drop(positions);
                if head_moved(repo).map_err(mark_refused)? {
                    debug!(attempt, error = %e, "the mark failed on a moved index head; marking again");
                    reload_index(repo)?;
                    continue;
                }
                return Err(mark_refused(e));
            }
        };
        // One pass over the index: its totals, the bytes this mark
        // references, and (first attempt) the unmarked entries.
        let first = so_far.is_none();
        let (mut entries, mut bytes, mut referenced_bytes) = (0u64, 0u64, 0u64);
        let mut unmarked: Vec<(ChunkId, u32)> = Vec::new();
        positions.for_each_live(|p, id, e| {
            entries += 1;
            bytes += u64::from(e.stored_size);
            if found.is_marked(p) {
                referenced_bytes +=
                    u64::from(e.stored_size) * u64::from(found.count(p).unwrap_or(0));
            } else if first {
                unmarked.push((id, e.stored_size));
            }
        });
        let mut m = match so_far.take() {
            None => {
                unmarked.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                MarkSoFar {
                    marked: HashSet::new(),
                    snapshots: Vec::new(),
                    ptrs: Vec::new(),
                    candidates: unmarked,
                    references: 0,
                    referenced_bytes: 0,
                }
            }
            Some(mut m) => {
                // Still in the index (another sweep may have removed some)
                // and unmarked by the snapshots listed since.
                m.candidates
                    .retain(|(id, _)| positions.find(id).is_some_and(|p| !found.is_marked(p)));
                m
            }
        };
        drop(positions);
        for s in &fresh {
            m.marked.insert(*s.id.as_bytes());
            m.snapshots.push(s.id);
            m.ptrs.push(s.meta.item_ptrs.clone());
        }
        m.references += found.references;
        m.referenced_bytes += referenced_bytes;
        let candidate_bytes: u64 = m.candidates.iter().map(|(_, s)| u64::from(*s)).sum();
        let mut unreferenced = m.candidates.clone();
        let (kept_young, kept_young_bytes) =
            keep_young_additions(repo, &mut unreferenced).map_err(mark_refused)?;
        let mut snapshots = m.snapshots.clone();
        snapshots.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let all_ptrs: Vec<&[ChunkId]> = m.ptrs.iter().map(Vec::as_slice).collect();
        let plan = SweepPlan {
            snapshots,
            tree_set: mark::TreeChunkSet::build(&all_ptrs),
            unreferenced,
            entries_live: entries - m.candidates.len() as u64,
            bytes_live: bytes - candidate_bytes,
            references: m.references,
            referenced_bytes: m.referenced_bytes,
            kept_young,
            kept_young_bytes,
        };
        let swept = Swept {
            snapshots: plan.snapshots.len() as u64,
            entries_removed: plan.unreferenced.len() as u64,
            bytes_removed: plan.bytes_removed(),
            kept_young,
            kept_young_bytes,
            skipped: None,
        };
        debug!(
            attempt,
            snapshots = swept.snapshots,
            fresh = fresh.len(),
            removed = swept.entries_removed,
            bytes = swept.bytes_removed,
            kept_young,
            dry_run,
            "lock-free mark-and-sweep"
        );
        if dry_run {
            let index = repo.chunk_index_mut();
            for (id, _) in &plan.unreferenced {
                index.remove(id);
            }
            return Ok(swept);
        }
        // 6. The sweep head.
        stage_sweep(repo, &plan);
        match repo.persist_maintenance()? {
            MaintenanceWrite::Written => {
                trim_tree_cache(repo, &plan);
                return Ok(swept);
            }
            MaintenanceWrite::Moved => {
                debug!(
                    attempt,
                    "the sweep's index head was taken or moved; marking again"
                );
                so_far = Some(m);
                reload_index(repo)?;
            }
        }
    }
    warn!(
        "compact: the index changed during each of {COMPACT_MARK_ATTEMPTS} marks (backups \
         committing); no index entry is removed this run"
    );
    Ok(Swept {
        snapshots: so_far.as_ref().map_or(0, |m| m.marked.len() as u64),
        skipped: Some(SweepSkipped::Contention),
        ..Swept::default()
    })
}

/// What [`compact_packs`] did, with the pack IDs the pack-map pass needs.
struct PackRun {
    stats: CompactStats,
    /// Packs the listing returned.
    listed: HashSet<PackId>,
    /// Packs this run deleted.
    deleted: HashSet<PackId>,
    /// Packs this run wrote (repack outputs); never deleted, and alive for
    /// the pack-map pass even when the listing did not return them.
    written: HashSet<PackId>,
}

fn compact_packs(
    repo: &mut Repository,
    free: Option<&str>,
    listing: PackListing,
    opts: CompactOpts,
    shutdown: &CancelToken,
) -> Result<PackRun> {
    let CompactOpts {
        threshold,
        max_repack_size,
        dry_run,
        concurrency,
    } = opts;
    let PackListing {
        discovered,
        temp_files_removed,
    } = listing;
    let mut stats = CompactStats {
        temp_files_removed,
        ..CompactStats::default()
    };
    let mut deleted: HashSet<PackId> = HashSet::new();
    // Packs this run wrote. A repack's output can have the ID of a pack the
    // listing showed as an orphan (the same blobs repacked by an earlier,
    // interrupted run): that pack is live now and is never deleted.
    let mut written: HashSet<PackId> = HashSet::new();

    // Phase 1: Analyze live/dead space using the chunk index + pack sizes.
    // A pack's trailer frame (its `trailer_len` in the index) counts as
    // overhead, not as dead bytes: otherwise every repack of a small pack
    // would leave a new trailer of the same relative size, and the next
    // compact would repack it again. A pack recorded without a trailer
    // (`trailer_len` 0) whose bytes hold one is repacked at most once.
    //
    // Build a per-pack lookup from the chunk index, which the mark-and-sweep
    // before this has already swept: the swept entries' bytes count as dead,
    // and a pack left without entries is an orphan (Case B), deleted whole.
    //   pack_id → Vec<(chunk_id, stored_size, pack_offset)>
    let mut per_pack_lookup: HashMap<PackId, Vec<(ChunkId, u32, u64)>> = HashMap::new();
    for (chunk_id, entry) in repo.chunk_index().iter() {
        per_pack_lookup.entry(entry.pack_id).or_default().push((
            chunk_id,
            entry.stored_size,
            entry.pack_offset,
        ));
    }
    let trailer_lens: HashMap<PackId, u64> = per_pack_lookup
        .keys()
        .map(|p| {
            let len = repo.chunk_index().pack_trailer_len(p).unwrap_or(0);
            (*p, u64::from(len))
        })
        .collect();

    // The packs as listed before the session listing and the head read
    // (`take_pack_listing`).
    let listed: HashSet<PackId> = discovered.iter().map(|p| p.pack_id).collect();

    // For each discovered pack, compute live/dead space from its size and the
    // index. The size comes from the listing; `size()` only where it has none.
    let pack_header_size = PACK_HEADER_SIZE as u64;

    let analyses_mu: Mutex<Vec<PackAnalysis>> = Mutex::new(Vec::new());
    let packs_total = AtomicU64::new(0);
    let packs_corrupt = AtomicU64::new(0);
    let packs_orphan = AtomicU64::new(0);
    let packs_orphan_young = AtomicU64::new(0);
    let packs_orphan_unknown_age = AtomicU64::new(0);
    let packs_orphan_live = AtomicU64::new(0);
    let packs_repack_kept = AtomicU64::new(0);
    let blobs_live = AtomicU64::new(0);
    // "Now" on the store's clock: the maintenance lock's own key's mtime,
    // read at acquisition, before this listing (a pack written since has an
    // mtime after it and is the youngest possible); lock-free, the mtime of
    // this run's newest marker key in its session listing, written before
    // that listing and so before the head read and every delete (an age
    // judged by it is at most the pack's age at its delete). The horizons
    // need no "now": each is a marker key's mtime moved back.
    let store_now = repo.lock_store_time();
    let pack_grace = repo.pack_grace();
    // 04 "Compact" 3: an orphan goes only before every live session's
    // horizon. None = some horizon is unknown: no orphan goes this run.
    let horizons: Option<Vec<std::time::SystemTime>> = repo
        .live_sessions()
        .iter()
        .map(LiveSession::horizon)
        .collect();
    if horizons.is_none() {
        warn!(
            "compact: a live backup session's marker gives no horizon (malformed, or written \
             by an older vykar); no orphan pack is deleted this run"
        );
    }

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

            let trailer = trailer_lens.get(pack_id).copied().unwrap_or(0);
            // Sanity check: live bytes and the trailer can't exceed the
            // pack payload.
            if pack_size < pack_header_size
                || live_bytes.saturating_add(trailer) > pack_size - pack_header_size
            {
                warn!(
                    "Pack {} has inconsistent size (pack_size={}, live_bytes={}, header={}, trailer={}), marking corrupt",
                    pack_id, pack_size, live_bytes, pack_header_size, trailer
                );
                packs_corrupt.fetch_add(1, Ordering::Relaxed);
                return;
            }

            let payload = pack_size - pack_header_size;
            let dead_bytes = payload - live_bytes - trailer;

            blobs_live.fetch_add(entries.len() as u64, Ordering::Relaxed);

            if dead_bytes == 0 {
                return;
            }

            if unused_percent(dead_bytes, payload) >= threshold {
                // Lock-free (04 step 9): a repack source obeys the orphan
                // rule too (T_pack old on the store's clock and before
                // every live horizon), since a backup can give it a new
                // entry after this run's head read: journal recovery adopts
                // a chunk the index lacks into the pack that holds it, also
                // a pack with entries (a chunk another session committed
                // first, whose other copy was swept since). Such a backup
                // can commit after this batch's head (its create taken, it
                // rebases) and name the pack this run deletes. It adopts a
                // pack only while it is at most T_pack/2 old, and pulls its
                // start back by that age at its next marker write. A
                // session registered after sessions #1 adopts after that
                // listing, so the pack was younger than T_pack/2 then: not
                // old. One listed live did not adopt before its listed key
                // was written (that key would carry the pull-back, and the
                // pack would lie inside its horizon), so it adopts after
                // that key, which is younger than the 45 min staleness: the
                // pack was younger than T_pack/2 + 45 min then, not old.
                // Under the lock such a backup waits for the lock and then
                // fails its pack check. Cost: lock-free compact does not
                // repack packs younger than T_pack (a young pack's chunks
                // are mostly a young segment's additions, which the sweep
                // keeps anyway) or newer than a live backup's start.
                if free.is_some()
                    && !(old_enough(p.mtime, store_now, pack_grace)
                        && before_horizons(p.mtime, &horizons))
                {
                    packs_repack_kept.fetch_add(1, Ordering::Relaxed);
                    debug!(%pack_id, "keeping a pack young or inside a live backup's horizon from the repack");
                    return;
                }
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

            // A pack with no entry left (never committed, or emptied by this
            // run's sweep) goes only once T_pack old and before every live
            // horizon (04 I1). Its entries were old (the sweep keeps young
            // backup segments' additions), but its own age is not bounded by
            // theirs: a repack output, a base folded from maintenance
            // segments, or a zero test grace.
            if !old_enough(p.mtime, store_now, pack_grace) {
                if p.mtime.is_none() || store_now.is_none() {
                    packs_orphan_unknown_age.fetch_add(1, Ordering::Relaxed);
                } else {
                    packs_orphan_young.fetch_add(1, Ordering::Relaxed);
                }
                debug!(%pack_id, "keeping a young orphan pack");
                return;
            }
            // 04 "Compact" 3: not while a live backup may still name it.
            if !before_horizons(p.mtime, &horizons) {
                packs_orphan_live.fetch_add(1, Ordering::Relaxed);
                debug!(%pack_id, "keeping an orphan pack inside a live backup's horizon");
                return;
            }

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
    stats.packs_orphan_young = packs_orphan_young.load(Ordering::Relaxed);
    stats.packs_orphan_unknown_age = packs_orphan_unknown_age.load(Ordering::Relaxed);
    stats.packs_orphan_live = packs_orphan_live.load(Ordering::Relaxed);
    stats.blobs_live = blobs_live.load(Ordering::Relaxed);
    let repack_kept = packs_repack_kept.load(Ordering::Relaxed);
    stats.packs_repack_held = repack_kept;
    if repack_kept > 0 {
        info!(
            "compact: {repack_kept} pack(s) with unused space not repacked this run: younger \
             than the orphan grace (12 h) or inside a live backup's horizon"
        );
    }
    let unknown_age = stats.packs_orphan_unknown_age;
    if unknown_age > 0 {
        warn!(
            "compact: the storage reported no modification times; {unknown_age} orphan \
             pack(s) of unknown age are kept"
        );
    }

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
        return Ok(PackRun {
            stats,
            listed,
            deleted,
            written,
        });
    }

    if try_server_side_repack(
        repo,
        free,
        &selected,
        &mut stats,
        &mut deleted,
        &mut written,
    )? {
        return Ok(PackRun {
            stats,
            listed,
            deleted,
            written,
        });
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
            persist_and_delete(repo, free, &mut batch, &written, &mut stats, &mut deleted)?;
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
        let header =
            match repo
                .storage
                .get_range(&analysis.storage_key, 0, PACK_HEADER_SIZE as u64)?
            {
                Some(header) => header,
                None if free.is_some() => {
                    vanished_during_repack(analysis);
                    continue;
                }
                None => {
                    return Err(VykarError::Other(format!(
                        "pack {} disappeared during repack",
                        analysis.pack_id
                    )));
                }
            };
        if validate_pack_header(&header).is_err() {
            warn!(
                "Skipping pack {} with invalid header during repack",
                analysis.pack_id
            );
            stats.packs_corrupt += 1;
            continue;
        }

        let mut writer = PackWriter::new_repack(pack_target, repo.content_hash());

        let mut vanished = false;
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
            let combined =
                match repo
                    .storage
                    .get_range(&analysis.storage_key, prefix_offset, read_len)?
                {
                    Some(c) => c,
                    None if free.is_some() => {
                        vanished = true;
                        break;
                    }
                    None => {
                        return Err(VykarError::Other(format!(
                            "pack not found: {}",
                            analysis.pack_id
                        )));
                    }
                };
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
        if vanished {
            // Nothing of this pack is written; its blobs read so far go.
            vanished_during_repack(analysis);
            continue;
        }

        let (new_pack_id, new_entries) =
            writer.flush(repo.storage.as_ref(), repo.crypto.as_ref())?;
        written.insert(new_pack_id);
        let trailer_len = trailer_frame_len(new_entries.len(), repo.crypto.is_encrypting());
        repo.chunk_index_mut()
            .set_pack_trailer(new_pack_id, trailer_len);

        for (chunk_id, stored_size, offset) in &new_entries {
            if free.is_some() && !still_in(repo, chunk_id, &analysis.pack_id) {
                continue;
            }
            repo.chunk_index_mut()
                .update_location(chunk_id, new_pack_id, *offset, *stored_size);
        }

        if free.is_none() {
            // Lock-free, a source counts once its batch's head is created
            // and it is deleted (`record_deleted`).
            stats.packs_repacked += 1;
        }
        total_repacked_bytes += analysis.total_bytes;
        batch_bytes += analysis.total_bytes;
        batch.push(analysis);
    }
    persist_and_delete(repo, free, &mut batch, &written, &mut stats, &mut deleted)?;

    Ok(PackRun {
        stats,
        listed,
        deleted,
        written,
    })
}

/// A lock-free compact's source pack that is gone when it is read for the
/// repack (04 step 9): another compact repacked and deleted it. Nothing of
/// it is relocated; the run goes on.
fn vanished_during_repack(analysis: &PackAnalysis) {
    warn!(
        "compact: pack {} is gone (another compact repacked it); not repacked by this run",
        analysis.pack_id
    );
}

/// Whether the index still holds `chunk` in `pack` (a lock-free repack
/// relocates a chunk only from where the index has it: another compact may
/// have moved it, or swept it, since the analysis).
fn still_in(repo: &Repository, chunk: &ChunkId, pack: &PackId) -> bool {
    repo.chunk_index()
        .get(chunk)
        .is_some_and(|e| e.pack_id == *pack)
}

/// Persist the index if it changed (crash safety), then delete `batch`'s
/// source packs, except those this run wrote (`written`) and, lock-free,
/// those the persisted index still names, and empty the batch.
fn persist_and_delete(
    repo: &mut Repository,
    free: Option<&str>,
    batch: &mut Vec<&PackAnalysis>,
    written: &HashSet<PackId>,
    stats: &mut CompactStats,
    deleted: &mut HashSet<PackId>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let named = match free {
        None => {
            repo.save_state()?;
            BatchPersisted::default()
        }
        Some(session) => {
            let sources: HashSet<PackId> = batch.iter().map(|a| a.pack_id).collect();
            persist_batch_lock_free(repo, session, &sources)?
        }
    };
    let BatchPersisted { named, kept_from } = named;
    batch.retain(|a| {
        let keep = written.contains(&a.pack_id);
        if keep {
            info!(
                "Keeping pack {}: this run wrote it as a repack output",
                a.pack_id
            );
        }
        if named.contains(&a.pack_id) {
            warn!(
                "compact: keeping pack {}: the index still names it after a rebase",
                a.pack_id
            );
            return false;
        }
        !keep
    });
    let credit = credited_sources(repo.storage.as_ref(), batch, kept_from.as_ref());
    if free.is_some() && !batch.is_empty() {
        // Lock-free: the run's marker still guards its decisions (the
        // persist checked it before the head create, this right before the
        // deletes, after the stats' presence checks).
        repo.check_lock_fence()?;
    }
    let err = delete_sources(
        repo.storage.as_ref(),
        batch,
        stats,
        deleted,
        credit.as_ref(),
        free.is_some(),
    );
    batch.clear();
    err.map_or(Ok(()), Err)
}

/// A lock-free compact's persist of one repack batch (04 step 9): the
/// batch's relocations as a maintenance segment and a head created with
/// put-if-absent ([`Repository::persist_maintenance`]). When the head
/// moved (a backup or another compact wrote one), wait
/// ([`retry_delay`]), rebase the relocations onto the latest head
/// ([`Repository::rebase_relocations`]: only chunks the latest index still
/// holds in one of `sources`) and create again, at most
/// [`HEAD_CREATE_ATTEMPTS`] times, then the "index changed" error. Returns
/// the sources the written index still names after a rebase (one scan of
/// the index; none without a rebase): they are not deleted. Sources are
/// deleted only after the head naming their replacements is created. Each
/// create is preceded by the run's marker check (the publish's fence): a
/// run whose marker was reaped stops, since another compact may have
/// deleted this batch's outputs as old orphans meanwhile.
fn persist_batch_lock_free(
    repo: &mut Repository,
    session: &str,
    sources: &HashSet<PackId>,
) -> Result<BatchPersisted> {
    let mut kept_from: Option<HashSet<PackId>> = None;
    for attempt in 1..=HEAD_CREATE_ATTEMPTS {
        match repo.persist_maintenance()? {
            MaintenanceWrite::Written if kept_from.is_none() => {
                return Ok(BatchPersisted::default());
            }
            MaintenanceWrite::Written => {
                let named = repo
                    .chunk_index()
                    .iter()
                    .filter(|(_, e)| sources.contains(&e.pack_id))
                    .map(|(_, e)| e.pack_id)
                    .collect();
                return Ok(BatchPersisted { named, kept_from });
            }
            MaintenanceWrite::Moved if attempt < HEAD_CREATE_ATTEMPTS => {
                repo.host_env()
                    .clock
                    .sleep(retry_delay(session, attempt + 1));
                let (dropped, kept) = repo.rebase_relocations(sources)?;
                kept_from = Some(kept);
                debug!(
                    attempt,
                    dropped,
                    "the index head moved; the repack batch is rebased onto the latest head"
                );
            }
            MaintenanceWrite::Moved => {}
        }
    }
    Err(VykarError::Other(format!(
        "the index changed on storage after it was loaded: another writer created the \
         index head on each of {HEAD_CREATE_ATTEMPTS} attempts at a repack batch; retry the \
         compact"
    )))
}

/// What persisting one repack batch left for its deletes.
#[derive(Default)]
struct BatchPersisted {
    /// Sources the written index still names after a rebase: not deleted.
    named: HashSet<PackId>,
    /// After a rebase, the sources at least one relocation was re-applied
    /// from (`None` without a rebase).
    kept_from: Option<HashSet<PackId>>,
}

/// The sources of a batch whose delete this run takes credit for in its
/// stats: all without a rebase (`None`). After a rebase (lock-free, 04
/// step 9) another compact may have repacked and deleted some first; a
/// source counts only when it still exists before the delete and, unless it
/// is empty, kept a relocation of this run (its chunks were not all moved
/// by the other compact). One EXISTS per source of a rebased batch.
fn credited_sources(
    storage: &dyn StorageBackend,
    sources: &[&PackAnalysis],
    kept_from: Option<&HashSet<PackId>>,
) -> Option<HashSet<PackId>> {
    let kept_from = kept_from?;
    Some(
        sources
            .iter()
            .filter(|a| a.live_entries.is_empty() || kept_from.contains(&a.pack_id))
            .filter(|a| storage.exists(&a.storage_key).unwrap_or(true))
            .map(|a| a.pack_id)
            .collect(),
    )
}

/// Dead bytes as a percentage of `payload` (0 for an empty payload).
fn unused_percent(dead_bytes: u64, payload: u64) -> f64 {
    #[allow(
        clippy::cast_precision_loss,
        reason = "ratio for a threshold, precision loss acceptable"
    )]
    if payload > 0 {
        (dead_bytes as f64 / payload as f64) * 100.0
    } else {
        0.0
    }
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
    free: Option<&str>,
    analyses: &[&PackAnalysis],
    stats: &mut CompactStats,
    deleted: &mut HashSet<PackId>,
    written: &mut HashSet<PackId>,
) -> Result<bool> {
    if analyses.is_empty() {
        return Ok(true);
    }

    // A server that does not advertise `repack` (one older than the field, or
    // a failed `/health` probe) and a backend with no server behind it get a
    // client-side repack. So does a server without `repack_trailer`: every
    // operation here carries the output pack's trailer, which such a server
    // would refuse (0.8 to 0.24) or that the REST backend refuses to send.
    // The clone lets `server` outlive the `&mut repo` borrows below.
    let storage = Arc::clone(&repo.storage);
    let Some(server) = storage.server_ops() else {
        return Ok(false);
    };
    if !server.offers(ServerOp::Repack) {
        warn!(
            "compact: the server did not advertise server-side repack (an older \
             vykar-server, or its /health probe failed); repacking on the client"
        );
        return Ok(false);
    }
    if !server.offers(ServerOp::RepackTrailer) {
        warn!(
            "compact: the server does not advertise repack_trailer (a vykar-server \
             that would drop pack trailers); repacking on the client"
        );
        return Ok(false);
    }

    // The server never deletes: sources are removed below, after the index
    // pointing at their replacements is persisted. A lost or timed-out
    // response therefore leaves only unreferenced new packs behind.
    let encrypting = repo.crypto.is_encrypting();
    let operations: Vec<PlannedOp<'_>> = analyses
        .iter()
        .map(|analysis| PlannedOp::new(analysis, encrypting))
        .collect();

    // A single operation over one of the server's caps cannot be split
    // further (one source pack = one op) and would be rejected with 400, so
    // fall back to client-side repack instead of sending a doomed plan. Pack
    // size limits keep this from happening in practice.
    if operations.iter().any(PlannedOp::exceeds_server_caps) {
        return Ok(false);
    }

    // Send one server repack call per batch (batches stay within the server's
    // output, body and operation caps; client and server share the
    // constants). Each batch is applied, persisted, and only then are its
    // sources deleted, so a crash at any point leaves the index consistent.
    // Packs returned as some op's new pack carry live data and must never be
    // deleted as a source (the server may return the source itself).
    let mut result_packs: HashSet<String> = HashSet::new();

    for batch in chunk_repack_operations(operations) {
        // Trailers are built per batch, so a large selection never holds all
        // of them at once.
        let mut batch_analyses: Vec<&PackAnalysis> = Vec::with_capacity(batch.len());
        let mut requests = Vec::with_capacity(batch.len());
        for op in batch {
            batch_analyses.push(op.analysis);
            requests.push(op.into_request(repo.crypto.as_ref())?);
        }
        // The constructor derives `protocol_version` from the algorithm and
        // the trailers, so an old server rejects a BLAKE3 or trailer plan
        // outright instead of verifying the packs under the wrong algorithm.
        let plan = RepackPlanRequest::new(requests, repo.content_hash());
        let response = server.repack(&plan)?;

        for key in response
            .completed
            .iter()
            .filter_map(|op| op.new_pack.as_ref())
        {
            if let Ok(id) = PackId::from_storage_key(key) {
                written.insert(id);
            }
            result_packs.insert(key.clone());
        }
        let mut completed_by_source: HashMap<String, vykar_protocol::RepackOperationResult> =
            response
                .completed
                .into_iter()
                .map(|op| (op.source_pack.clone(), op))
                .collect();

        let mut applied: Vec<&PackAnalysis> = Vec::with_capacity(plan.operations.len());
        let mut batch_err: Option<VykarError> = None;
        for (op, analysis) in plan.operations.iter().zip(batch_analyses) {
            let Some(result) = completed_by_source.remove(&op.source_pack) else {
                batch_err = Some(VykarError::Other(format!(
                    "server repack response missing operation for {}",
                    op.source_pack
                )));
                break;
            };
            if let Err(err) = apply_repack_result(repo, free.is_some(), analysis, &result, stats) {
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
        let named = match free {
            None => {
                repo.save_state()?;
                BatchPersisted::default()
            }
            Some(session) => {
                let sources: HashSet<PackId> = applied.iter().map(|a| a.pack_id).collect();
                persist_batch_lock_free(repo, session, &sources)?
            }
        };
        let BatchPersisted { named, kept_from } = named;

        // A failed delete leaves an unreferenced source behind (wasted space,
        // not an inconsistency). Finish this batch's deletes, then stop: on an
        // append-only server every later batch would only grow the repo.
        let sources: Vec<&PackAnalysis> = applied
            .into_iter()
            .filter(|a| !result_packs.contains(&a.storage_key))
            .filter(|a| !named.contains(&a.pack_id))
            .collect();
        let credit = credited_sources(repo.storage.as_ref(), &sources, kept_from.as_ref());
        if free.is_some() && !sources.is_empty() {
            repo.check_lock_fence()?;
        }
        let delete_err = delete_sources(
            repo.storage.as_ref(),
            &sources,
            stats,
            deleted,
            credit.as_ref(),
            free.is_some(),
        );
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
    deleted: &mut HashSet<PackId>,
    credit: Option<&HashSet<PackId>>,
    count_repacked: bool,
) -> Option<VykarError> {
    let mut record = |analysis: &PackAnalysis, stats: &mut CompactStats| {
        let credited = credit.is_none_or(|c| c.contains(&analysis.pack_id));
        record_deleted(analysis, stats, deleted, credited, count_repacked);
    };
    if sources.is_empty() {
        return None;
    }
    let keys: Vec<String> = sources.iter().map(|a| a.storage_key.clone()).collect();
    if let Some(batch) = storage.batch_delete() {
        match batch.delete_keys(&keys, vykar_storage::BatchDeleteScope::KeysOnly) {
            Ok(()) => {
                for analysis in sources {
                    record(analysis, stats);
                }
                return None;
            }
            Err(e) => warn!(
                "compact: batch delete of {} source pack(s) failed ({e}); deleting one by one",
                keys.len()
            ),
        }
    }
    let mut first_err = None;
    for analysis in sources {
        match storage.delete(&analysis.storage_key) {
            Ok(()) => record(analysis, stats),
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
/// `credited`: the delete is this run's work (see [`credited_sources`]);
/// `count_repacked`: a repacked source is counted here, at its delete
/// (lock-free), not when its blobs were copied.
fn record_deleted(
    analysis: &PackAnalysis,
    stats: &mut CompactStats,
    deleted: &mut HashSet<PackId>,
    credited: bool,
    count_repacked: bool,
) {
    deleted.insert(analysis.pack_id);
    if !credited {
        return;
    }
    if !analysis.live_entries.is_empty() && count_repacked {
        stats.packs_repacked += 1;
    }
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
    lock_free: bool,
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

    // The trailer sent with the operation describes this layout; a pack
    // whose blobs sit elsewhere would contradict its own trailer.
    let expected: Vec<u64> =
        framed_offsets(analysis.live_entries.iter().map(|e| e.length)).collect();
    if result.new_offsets != expected {
        return Err(VykarError::Other(format!(
            "server repack offsets for {} differ from the layout its trailer describes: \
             expected {expected:?}, got {:?}",
            analysis.storage_key, result.new_offsets
        )));
    }

    // The operation carried the trailer of its live blobs.
    let trailer_len = trailer_frame_len(analysis.live_entries.len(), repo.crypto.is_encrypting());
    repo.chunk_index_mut()
        .set_pack_trailer(new_pack_id, trailer_len);
    for (entry, new_offset) in analysis.live_entries.iter().zip(result.new_offsets.iter()) {
        if lock_free && !still_in(repo, &entry.chunk_id, &analysis.pack_id) {
            continue;
        }
        repo.chunk_index_mut().update_location(
            &entry.chunk_id,
            new_pack_id,
            *new_offset,
            entry.length,
        );
    }

    if !lock_free {
        // Lock-free, counted when the source is deleted (`record_deleted`).
        stats.packs_repacked += 1;
    }
    Ok(())
}

/// One server repack operation before its trailer is attached.
struct PlannedOp<'a> {
    analysis: &'a PackAnalysis,
    /// `trailer: None` until the batch is sent.
    request: RepackOperationRequest,
    /// Exact trailer length; 0 for a delete-only operation (no trailer).
    trailer_len: u64,
}

impl<'a> PlannedOp<'a> {
    fn new(analysis: &'a PackAnalysis, encrypting: bool) -> Self {
        let live = &analysis.live_entries;
        let trailer_len = if live.is_empty() {
            0
        } else {
            trailer_blob_len(live.len(), encrypting) as u64
        };
        Self {
            analysis,
            request: RepackOperationRequest {
                source_pack: analysis.storage_key.clone(),
                keep_blobs: live
                    .iter()
                    .map(|entry| RepackBlobRef {
                        offset: entry.offset,
                        length: u64::from(entry.length),
                    })
                    .collect(),
                delete_after: false,
                trailer: None,
            },
            trailer_len,
        }
    }

    /// `repack_op_output_size` of the request once its trailer is attached.
    fn output_size(&self) -> u64 {
        let size = repack_op_output_size(&self.request);
        if self.trailer_len > 0 {
            size.saturating_add(4).saturating_add(self.trailer_len)
        } else {
            size
        }
    }

    /// `repack_op_json_bound` of the request once its trailer is attached.
    fn json_bound(&self) -> u64 {
        let bound = repack_op_json_bound(&self.request);
        if self.trailer_len > 0 {
            bound.saturating_add(repack_trailer_json_len(self.trailer_len))
        } else {
            bound
        }
    }

    /// Whether the server would refuse this operation even alone in a plan.
    fn exceeds_server_caps(&self) -> bool {
        self.output_size() > MAX_REPACK_OUTPUT_BYTES
            || self.trailer_len > MAX_REPACK_TRAILER_BYTES
            || self.json_bound() > MAX_REPACK_PLAN_JSON_BYTES
            || self.request.keep_blobs.len() > MAX_KEEP_BLOBS_PER_OP
    }

    /// The request with its trailer: the live blobs at the offsets the server
    /// writes them to ([`framed_offsets`]), encrypted with `crypto`.
    fn into_request(self, crypto: &dyn CryptoEngine) -> Result<RepackOperationRequest> {
        let mut request = self.request;
        if self.trailer_len == 0 {
            return Ok(request);
        }
        let trailer = PackTrailer::for_blobs(
            TrailerPackType::Repacked,
            self.analysis
                .live_entries
                .iter()
                .map(|e| (e.chunk_id, e.length)),
        )?;
        let blob = trailer.seal_blob(crypto)?;
        if blob.len() as u64 != self.trailer_len {
            return Err(VykarError::Other(format!(
                "pack trailer for {} is {} bytes, its estimate {}",
                request.source_pack,
                blob.len(),
                self.trailer_len
            )));
        }
        request.trailer = Some(blob);
        Ok(request)
    }
}

/// Split planned operations into batches whose summed output stays within
/// [`MAX_REPACK_OUTPUT_BYTES`], whose summed JSON bound stays within
/// [`MAX_REPACK_PLAN_JSON_BYTES`], and that hold at most [`MAX_REPACK_OPS`]
/// operations. A single operation over a cap is caught before batching
/// (client-side repack fallback in `try_server_side_repack`), so every batch
/// produced here is within the server's limits.
fn chunk_repack_operations(operations: Vec<PlannedOp<'_>>) -> Vec<Vec<PlannedOp<'_>>> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_size = 0u64;
    let mut current_json = 0u64;
    for op in operations {
        let size = op.output_size();
        let json = op.json_bound();
        if !current.is_empty()
            && (current_size.saturating_add(size) > MAX_REPACK_OUTPUT_BYTES
                || current_json.saturating_add(json) > MAX_REPACK_PLAN_JSON_BYTES
                || current.len() >= MAX_REPACK_OPS)
        {
            batches.push(std::mem::take(&mut current));
            current_size = 0;
            current_json = 0;
        }
        current_size = current_size.saturating_add(size);
        current_json = current_json.saturating_add(json);
        current.push(op);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

#[cfg(test)]
mod repack_chunk_tests {
    use super::{LiveEntry, PackAnalysis, PlannedOp, chunk_repack_operations};
    use crate::repo::pack_trailer::trailer_blob_len;
    use vykar_protocol::{
        MAX_REPACK_OPS, MAX_REPACK_OUTPUT_BYTES, MAX_REPACK_PLAN_JSON_BYTES,
        repack_trailer_json_len,
    };
    use vykar_types::chunk_id::ChunkId;
    use vykar_types::pack_id::PackId;

    /// A pack analysis keeping `blobs` live blobs of `length` bytes each.
    fn analysis(blobs: usize, length: u32) -> PackAnalysis {
        PackAnalysis {
            pack_id: PackId::from_bytes([0xab; 32]),
            storage_key: format!("packs/ab/{}", "ab".repeat(32)),
            live_entries: (0..blobs)
                .map(|i| LiveEntry {
                    chunk_id: ChunkId::from_bytes([i as u8; 32]),
                    offset: 13 + i as u64 * (4 + u64::from(length)),
                    length,
                })
                .collect(),
            total_bytes: 0,
            dead_bytes: 0,
        }
    }

    fn planned(analyses: &[PackAnalysis]) -> Vec<PlannedOp<'_>> {
        analyses.iter().map(|a| PlannedOp::new(a, true)).collect()
    }

    #[test]
    fn empty_input_yields_no_batches() {
        assert!(chunk_repack_operations(vec![]).is_empty());
    }

    #[test]
    fn small_ops_stay_in_one_batch() {
        let analyses = [analysis(1, 100), analysis(1, 200), analysis(1, 300)];
        let batches = chunk_repack_operations(planned(&analyses));
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 3);
    }

    #[test]
    fn ops_straddling_cap_split_into_batches() {
        // Two ops each just over half the cap must not share a batch.
        let half_plus = (MAX_REPACK_OUTPUT_BYTES / 2 + 1) as u32;
        let analyses = [analysis(1, half_plus), analysis(1, half_plus)];
        let batches = chunk_repack_operations(planned(&analyses));
        assert_eq!(
            batches.len(),
            2,
            "each op exceeds half the cap -> own batch"
        );
        for batch in &batches {
            let total: u64 = batch.iter().map(PlannedOp::output_size).sum();
            assert!(total <= MAX_REPACK_OUTPUT_BYTES, "batch within cap");
        }
    }

    #[test]
    fn single_oversize_op_is_its_own_batch() {
        let analyses = [
            analysis(1, 10),
            analysis(2, (MAX_REPACK_OUTPUT_BYTES / 2) as u32),
            analysis(1, 10),
        ];
        let ops = planned(&analyses);
        assert!(ops[1].exceeds_server_caps());
        let batches = chunk_repack_operations(ops);
        assert_eq!(batches.len(), 3, "oversize op isolates the ops around it");
    }

    /// Two ops just under half the output cap without their trailers, but
    /// over it with them, are split.
    #[test]
    fn output_budget_counts_trailers() {
        let blobs = 1000usize;
        let trailer_frame = 4 + trailer_blob_len(blobs, true) as u64;
        let pack_overhead = 9 + 4 * blobs as u64;
        // Payload per op so that 2 * (overhead + payload) == cap - 2.
        let payload = MAX_REPACK_OUTPUT_BYTES / 2 - 1 - pack_overhead;
        let length = (payload / blobs as u64) as u32;
        let analyses = [analysis(blobs, length), analysis(blobs, length)];
        let ops = planned(&analyses);
        let without: u64 = ops
            .iter()
            .map(|op| vykar_protocol::repack_op_output_size(&op.request))
            .sum();
        assert!(without <= MAX_REPACK_OUTPUT_BYTES, "{without}");
        assert!(without + 2 * trailer_frame > MAX_REPACK_OUTPUT_BYTES);
        assert_eq!(
            ops[0].output_size(),
            vykar_protocol::repack_op_output_size(&ops[0].request) + trailer_frame
        );
        assert_eq!(chunk_repack_operations(ops).len(), 2);
    }

    /// Ops whose JSON bounds sum past the plan budget are split, and every
    /// batch is within both caps.
    #[test]
    fn json_budget_splits_batches() {
        // ~64 + 82 JSON bytes per blob (with the trailer's hex): 100 000
        // blobs bound ~14 MiB per op, so five ops cannot share a plan.
        let analyses: Vec<PackAnalysis> = (0..5).map(|_| analysis(100_000, 1)).collect();
        let ops = planned(&analyses);
        let per_op = ops[0].json_bound();
        assert!(per_op > 100_000 * 64 + repack_trailer_json_len(0));
        assert!(5 * per_op > MAX_REPACK_PLAN_JSON_BYTES);
        let batches = chunk_repack_operations(ops);
        assert!(batches.len() >= 2, "{}", batches.len());
        for batch in &batches {
            let json: u64 = batch.iter().map(PlannedOp::json_bound).sum();
            let out: u64 = batch.iter().map(PlannedOp::output_size).sum();
            assert!(json <= MAX_REPACK_PLAN_JSON_BYTES, "{json}");
            assert!(out <= MAX_REPACK_OUTPUT_BYTES, "{out}");
        }
    }

    /// No batch holds more operations than the server accepts in one plan.
    #[test]
    fn op_count_cap_splits_batches() {
        let analyses: Vec<PackAnalysis> = (0..MAX_REPACK_OPS + 1).map(|_| analysis(0, 0)).collect();
        let batches = chunk_repack_operations(planned(&analyses));
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), MAX_REPACK_OPS);
        assert_eq!(batches[1].len(), 1);
    }

    /// A delete-only operation carries no trailer and costs no output.
    #[test]
    fn delete_only_op_has_no_trailer() {
        let empty = analysis(0, 0);
        let op = PlannedOp::new(&empty, true);
        assert_eq!(op.trailer_len, 0);
        assert_eq!(op.output_size(), 0);
        let engine = vykar_crypto::PlaintextEngine::new(crate::testutil::test_chunk_hasher());
        assert!(op.into_request(&engine).unwrap().trailer.is_none());
    }

    /// The trailer attached to an operation has the predicted length under
    /// either engine kind, and an engine of the other kind is refused.
    #[test]
    fn attached_trailer_matches_its_estimate() {
        let a = analysis(3, 50);
        let plain = vykar_crypto::PlaintextEngine::new(crate::testutil::test_chunk_hasher());
        let request = PlannedOp::new(&a, false).into_request(&plain).unwrap();
        assert_eq!(request.trailer.unwrap().len(), trailer_blob_len(3, false));
        let err = PlannedOp::new(&a, true).into_request(&plain).unwrap_err();
        assert!(err.to_string().contains("estimate"), "{err}");
    }
}
