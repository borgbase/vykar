//! Phase 4: parallel execution — partition read groups across worker threads,
//! download ranges, decrypt, decompress, and write to target files with
//! syscall-coalesced batching and an LRU file-handle cache.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::platform::fs;
use crate::repo::blob_read::{BlobReader, GroupEnd, ReadBuffers, coalesce_groups};
use crate::repo::control::{CancelToken, EventSink};
use crate::repo::relocate::{Located, MAX_RELOCATE_ROUNDS, Relocator, pack_not_found};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::plan::{PlannedFile, WriteTarget};
use super::read_groups::{PlannedBlob, ReadGroup};
use super::{MAX_OPEN_FILES_PER_GROUP, MAX_WRITE_BATCH, RestoreEvent};

/// A restore-worker failure, classified by where it came from.
///
/// `Source` is repository data that could not be read or decoded; continue
/// mode downgrades it to a damaged file. `Fatal` is everything else
/// (destination I/O, interrupts, internal errors). The wrapped error is the
/// untouched original in both cases.
#[derive(Debug)]
pub(super) enum ReadError {
    Source(VykarError),
    Fatal(VykarError),
}

impl ReadError {
    /// Classify a source-side failure. An interrupt is never downgraded.
    fn source(e: VykarError) -> Self {
        if matches!(e, VykarError::Interrupted) {
            Self::Fatal(e)
        } else {
            Self::Source(e)
        }
    }

    #[cfg(test)]
    pub(super) fn into_inner(self) -> VykarError {
        match self {
            Self::Source(e) | Self::Fatal(e) => e,
        }
    }
}

/// Unclassified errors (every `?` on a write path) are fatal by default.
impl From<VykarError> for ReadError {
    fn from(e: VykarError) -> Self {
        Self::Fatal(e)
    }
}

/// Sink for source-side failures that do not stop the pass (continue
/// mode): file index → first reason the file is damaged. Touched only on the
/// failure path.
#[derive(Default)]
pub(super) struct BatchFailures {
    damaged: Mutex<HashMap<usize, String>>,
}

impl BatchFailures {
    pub(super) fn record(&self, targets: &[WriteTarget], reason: &VykarError) {
        let reason = reason.to_string();
        let mut map = self
            .damaged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for t in targets {
            map.entry(t.file_idx).or_insert_with(|| reason.clone());
        }
    }

    /// Record that `blob` failed with `error`.
    fn record_blob(&self, blob: &PlannedBlob, error: VykarError) {
        self.record(&blob.targets, &error);
    }

    pub(super) fn into_map(self) -> HashMap<usize, String> {
        self.damaged
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Pick the error to report from every failed worker: the first `Fatal` in
/// join order if any, otherwise the first `Source`. When a destination failure
/// and a corruption coincide, the destination error is the one a rerun would
/// hit again, so it must not be reported as an unreadable source.
fn select_error(errors: Vec<ReadError>) -> Option<ReadError> {
    let mut first_source = None;
    for e in errors {
        match e {
            ReadError::Fatal(_) => return Some(e),
            ReadError::Source(_) if first_source.is_none() => first_source = Some(e),
            ReadError::Source(_) => {}
        }
    }
    first_source
}

/// Join every scoped worker, returning the error chosen by [`select_error`]
/// (a panic counts as `Fatal`, reported as `panic_msg`) and `Ok(())` only if
/// all of them succeeded. Any error also raises `cancelled`, so workers still
/// joining stop claiming work instead of finishing a pass whose result is
/// already discarded.
fn join_workers(
    handles: Vec<std::thread::ScopedJoinHandle<'_, std::result::Result<(), ReadError>>>,
    cancelled: &AtomicBool,
    panic_msg: &str,
) -> std::result::Result<(), ReadError> {
    let mut errors = Vec::new();
    for handle in handles {
        let err = match handle.join() {
            Ok(Ok(())) => continue,
            Ok(Err(e)) => e,
            Err(_panic) => ReadError::Fatal(VykarError::Other(panic_msg.to_string())),
        };
        cancelled.store(true, Ordering::Release);
        errors.push(err);
    }
    match select_error(errors) {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

/// How often the coordinating thread reports a running pass.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

/// Where [`execute_parallel_restore`] reports a batch while its workers run.
pub(super) struct BatchProgress<'a> {
    pub(super) sink: &'a dyn EventSink<RestoreEvent>,
    /// Files and bytes of earlier batches (the `Written` totals so far).
    pub(super) base_files: u64,
    pub(super) base_bytes: u64,
}

/// Counters of one batch's write pass. Workers update them with relaxed
/// atomics; the coordinating thread reads them for progress.
pub(super) struct WriteCounters {
    bytes: AtomicU64,
    /// Bytes still to write, per `file_idx`; `None` when nobody reads file
    /// progress (no sink), so the pass allocates nothing extra.
    left: Option<Box<[AtomicU64]>>,
    /// Files whose `left` reached 0, plus the batch's empty files.
    files_done: AtomicU64,
}

impl WriteCounters {
    /// Bytes only.
    pub(super) fn bytes_only() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            left: None,
            files_done: AtomicU64::new(0),
        }
    }

    /// Bytes and per-file completion for `files`: a file counts once its
    /// last byte is written, an empty file from the start.
    pub(super) fn tracking(files: &[PlannedFile]) -> Self {
        let empty = files.iter().filter(|f| f.total_size == 0).count();
        Self {
            bytes: AtomicU64::new(0),
            left: Some(files.iter().map(|f| AtomicU64::new(f.total_size)).collect()),
            files_done: AtomicU64::new(empty as u64),
        }
    }

    /// `n` bytes of `file_idx` were written.
    fn wrote(&self, file_idx: usize, n: u64) {
        self.bytes.fetch_add(n, Ordering::Relaxed);
        if n == 0 {
            return;
        }
        if let Some(left) = self.left.as_deref().and_then(|l| l.get(file_idx))
            && left.fetch_sub(n, Ordering::Relaxed) == n
        {
            self.files_done.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    pub(super) fn files_done(&self) -> u64 {
        self.files_done.load(Ordering::Relaxed)
    }
}

/// Raises `cancelled` when its worker unwinds, so sibling workers stop
/// claiming groups while the coordinating thread is still waiting for them.
struct CancelOnPanic<'a>(&'a AtomicBool);

impl Drop for CancelOnPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}

// ---------------------------------------------------------------------------
// Write accumulator
// ---------------------------------------------------------------------------

/// Groups the write accumulator state (buffer, target file, start offset)
/// into a single struct.  One instance per worker thread; its allocation is
/// reused across every sequence of every group the thread processes.
struct PendingWrite {
    file_idx: usize, // usize::MAX = no active sequence
    start: u64,
    buf: Vec<u8>,
}

impl PendingWrite {
    fn new() -> Self {
        Self {
            file_idx: usize::MAX,
            start: 0,
            buf: Vec::with_capacity(MAX_WRITE_BATCH),
        }
    }

    fn is_active(&self) -> bool {
        self.file_idx != usize::MAX
    }

    fn reset(&mut self) {
        self.buf.clear();
        self.file_idx = usize::MAX;
    }

    /// Start a new accumulation sequence, reusing the existing allocation.
    fn rebind(&mut self, file_idx: usize, start: u64) {
        self.buf.clear();
        self.file_idx = file_idx;
        self.start = start;
    }
}

// ---------------------------------------------------------------------------
// LRU file handle cache
// ---------------------------------------------------------------------------

/// A small LRU cache for open file handles.  Capacity is capped at
/// `MAX_OPEN_FILES_PER_GROUP` (16).  Linear scan is negligible compared
/// to the syscall cost of opening/closing files, and the small size keeps
/// the bookkeeping trivial.
struct LruHandles {
    /// Entries ordered from least-recently-used (front) to most-recently-used (back).
    entries: Vec<(usize, std::fs::File)>,
}

impl LruHandles {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(MAX_OPEN_FILES_PER_GROUP),
        }
    }

    /// Return a mutable reference to the file handle for `file_idx`,
    /// promoting it to most-recently-used.  Returns `None` if not cached.
    fn get(&mut self, file_idx: usize) -> Option<&mut std::fs::File> {
        if let Some(pos) = self.entries.iter().position(|(idx, _)| *idx == file_idx) {
            // Move to back (MRU position).
            let entry = self.entries.remove(pos);
            self.entries.push(entry);
            Some(&mut self.entries.last_mut().expect("entry was just pushed").1)
        } else {
            None
        }
    }

    /// Insert a handle for `file_idx`.  If the cache is full, evicts the
    /// least-recently-used entry (front of the vec).
    fn insert(&mut self, file_idx: usize, file: std::fs::File) {
        if self.entries.len() >= MAX_OPEN_FILES_PER_GROUP {
            self.entries.remove(0); // evict LRU
        }
        self.entries.push((file_idx, file));
    }
}

// ---------------------------------------------------------------------------
// Parallel restore execution
// ---------------------------------------------------------------------------

/// Partition `groups` (already sorted by pack_id, offset) into `num_threads`
/// buckets with pack-affinity: groups sharing a pack_id are assigned to the
/// same bucket when possible, falling back to the lightest bucket when the
/// affinity bucket would become a straggler.
//
// `bucket_bytes`/`buckets` are sized to `num_threads` and never resized; every
// index used here (`aff`, `dest`) comes from `lightest_bucket(&bucket_bytes)`
// or a prior `pack_affinity.insert(_, dest)`, both bounded by `num_threads`.
#[allow(clippy::indexing_slicing)]
fn partition_groups(groups: Vec<ReadGroup>, num_threads: usize) -> Vec<Vec<ReadGroup>> {
    let total_bytes: u64 = groups.iter().map(|g| g.read_end - g.read_start).sum();
    let cap = total_bytes / num_threads as u64 * 13 / 10; // 1.3x fair share

    let mut buckets: Vec<Vec<ReadGroup>> = (0..num_threads).map(|_| Vec::new()).collect();
    let mut bucket_bytes: Vec<u64> = vec![0; num_threads];

    // Track which bucket last saw each pack_id.
    let mut pack_affinity: HashMap<PackId, usize> = HashMap::new();

    for group in groups {
        let group_bytes = group.read_end - group.read_start;

        // Try affinity bucket first.
        let dest = if let Some(&aff) = pack_affinity.get(&group.pack_id) {
            if bucket_bytes[aff] + group_bytes <= cap {
                aff
            } else {
                // Affinity bucket would exceed cap — assign to lightest.
                lightest_bucket(&bucket_bytes)
            }
        } else {
            lightest_bucket(&bucket_bytes)
        };

        pack_affinity.insert(group.pack_id, dest);
        bucket_bytes[dest] += group_bytes;
        buckets[dest].push(group);
    }

    // Drop empty buckets.
    buckets.retain(|b| !b.is_empty());
    buckets
}

/// Return the index of the bucket with the fewest total bytes.
fn lightest_bucket(bucket_bytes: &[u64]) -> usize {
    bucket_bytes
        .iter()
        .enumerate()
        .min_by_key(|&(_, &b)| b)
        .map_or(0, |(i, _)| i)
}

/// Run every read group across a scoped pool of `readers` workers (at most
/// one per group).
///
/// With `failures` set, source-side failures are recorded in it (against the
/// files they affect) and the pass carries on; without it the first one stops the pass as
/// `ReadError::Source`.
///
/// With `relocator` set, a group whose pack answers 404 is read again where
/// the latest head stores its chunks (`repo::relocate`).
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_parallel_restore(
    files: &[PlannedFile],
    groups: Vec<ReadGroup>,
    storage: &Arc<dyn StorageBackend>,
    crypto: &dyn CryptoEngine,
    root: &Path,
    readers: usize,
    verify_chunks: bool,
    shutdown: &CancelToken,
    relocator: Option<&Relocator<'_>>,
    failures: Option<&BatchFailures>,
    progress: Option<&BatchProgress<'_>>,
) -> std::result::Result<u64, ReadError> {
    shutdown.check()?;
    if groups.is_empty() {
        return Ok(0);
    }

    let num_threads = readers.min(groups.len());
    let buckets = partition_groups(groups, num_threads);

    let counters = if progress.is_some() {
        WriteCounters::tracking(files)
    } else {
        WriteCounters::bytes_only()
    };
    let cancelled = AtomicBool::new(false);

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(buckets.len());
        // Disconnected once every worker has returned or unwound; nobody
        // sends on it.
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        // Each worker owns its bucket and drops every group (and its blob
        // list) as soon as it is processed, instead of the whole batch's plan
        // staying alive until the slowest worker finishes.
        for bucket in buckets {
            let counters = &counters;
            let cancelled = &cancelled;
            let done = done_tx.clone();

            handles.push(s.spawn(move || -> std::result::Result<(), ReadError> {
                // Bound first so it drops last: the panic guard below has
                // raised `cancelled` by the time the waiting thread wakes.
                let _done = done;
                let _cancel_on_panic = CancelOnPanic(cancelled);
                let mut bufs = ReadBuffers::default();
                // One write accumulator for the thread, not one 1 MiB
                // allocation per group.
                let mut pw = PendingWrite::new();

                for group in bucket {
                    if cancelled.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    if let Err(e) = process_read_group(
                        &group,
                        files,
                        storage,
                        crypto,
                        counters,
                        cancelled,
                        &mut bufs,
                        &mut pw,
                        root,
                        verify_chunks,
                        shutdown,
                        relocator,
                        failures,
                    ) {
                        cancelled.store(true, Ordering::Release);
                        return Err(e);
                    }

                    // Cap retained buffer capacity to avoid permanent high-water
                    // marks from outlier-large groups/blobs (~50 MiB RSS savings).
                    bufs.trim();
                }
                Ok(())
            }));
        }

        drop(done_tx);

        // Report from this thread while the workers run; they only bump
        // the counters, so the sink needs no `Sync`.
        if let Some(p) = progress {
            while let Err(RecvTimeoutError::Timeout) = done_rx.recv_timeout(PROGRESS_INTERVAL) {
                p.sink.emit(RestoreEvent::Written {
                    files: p.base_files + counters.files_done(),
                    bytes: p.base_bytes + counters.bytes(),
                });
            }
        }

        join_workers(handles, &cancelled, "restore worker panicked")?;
        // Workers return `Ok(())` when they stop early, so a cancellation would
        // otherwise surface as a short but successful byte count — and the
        // caller would go on to publish a partial tree.
        shutdown.check()?;
        Ok(counters.bytes())
    })
}

/// Per-pass state shared by every blob a worker decodes and writes.
struct BlobCtx<'a> {
    files: &'a [PlannedFile],
    reader: BlobReader<'a>,
    counters: &'a WriteCounters,
    cancelled: &'a AtomicBool,
    root: &'a Path,
    shutdown: &'a CancelToken,
    relocator: Option<&'a Relocator<'a>>,
    failures: Option<&'a BatchFailures>,
}

impl BlobCtx<'_> {
    fn stopping(&self) -> bool {
        self.cancelled.load(Ordering::Acquire) || self.shutdown.is_cancelled()
    }

    /// Record a source-side failure of `blob` in the failure sink, or turn it
    /// into the error that stops the pass when there is none (strict mode).
    fn source_failure(
        &self,
        blob: &PlannedBlob,
        e: VykarError,
    ) -> std::result::Result<(), ReadError> {
        match (self.failures, ReadError::source(e)) {
            (Some(f), ReadError::Source(e)) => {
                f.record_blob(blob, e);
                Ok(())
            }
            (_, err) => Err(err),
        }
    }
}

/// Process a single coalesced read group: open the range, then read, decrypt
/// and decompress it blob by blob and write the plaintext to target files.
///
/// The range is one [`StorageBackend::open_range`] request, read through
/// [`BlobReader::read_range`]. Backends that fetch it whole (the default)
/// buffer it in `bufs`; `LocalBackend` streams it, so `bufs` only ever holds
/// one stored blob.
///
/// `bufs` and `pw` are caller-owned scratch state reused across groups within
/// the same thread, reducing per-group allocation churn. Blobs are decrypted in
/// place inside `bufs`. The caller caps retained buffer capacity
/// ([`ReadBuffers::trim`]) between groups.
///
/// With a failure sink (continue mode) a failed group read is retried blob by blob: a short read
/// is an `Err` by the storage contract, so otherwise one truncated pack would
/// take down every blob of a 16 MiB coalesced group. A read that fails part
/// way through a streamed group keeps the blobs already written, records the
/// blob whose read failed, and retries the rest one by one.
#[allow(clippy::too_many_arguments)]
fn process_read_group(
    group: &ReadGroup,
    files: &[PlannedFile],
    storage: &Arc<dyn StorageBackend>,
    crypto: &dyn CryptoEngine,
    counters: &WriteCounters,
    cancelled: &AtomicBool,
    bufs: &mut ReadBuffers,
    pw: &mut PendingWrite,
    root: &Path,
    verify_chunks: bool,
    shutdown: &CancelToken,
    relocator: Option<&Relocator<'_>>,
    failures: Option<&BatchFailures>,
) -> std::result::Result<(), ReadError> {
    let ctx = BlobCtx {
        files,
        reader: BlobReader::new(storage.as_ref(), crypto, verify_chunks),
        counters,
        cancelled,
        root,
        shutdown,
        relocator,
        failures,
    };
    process_group_with(&ctx, group, bufs, pw, 0)
}

/// [`process_read_group`] with its context built; `depth` relocations led
/// to `group`.
fn process_group_with(
    ctx: &BlobCtx<'_>,
    group: &ReadGroup,
    bufs: &mut ReadBuffers,
    pw: &mut PendingWrite,
    depth: u32,
) -> std::result::Result<(), ReadError> {
    let failures = ctx.failures;
    // `failed_at`: index of the blob whose own read failed (a streamed piece,
    // or the only blob of the group); `None` when the group request failed as
    // a whole.
    let (err, failed_at) = match process_blobs(
        ctx,
        &group.pack_id,
        group.read_start,
        group.range_len(),
        &group.blobs,
        bufs,
        pw,
    )? {
        GroupEnd::Done | GroupEnd::Stopped => return Ok(()),
        GroupEnd::ReadFailed { index, error } => (error, Some(index)),
        GroupEnd::PackNotFound => return process_relocated(ctx, group, bufs, pw, depth),
        GroupEnd::OpenFailed(e) => (e, (group.blobs.len() == 1).then_some(0)),
    };

    if failures.is_none() || matches!(err, VykarError::Interrupted) {
        return Err(ReadError::source(err));
    }
    // Blobs are sorted by offset, so a truncated pack fails on every blob
    // from the cut onwards; stopping after a short run of failures keeps that
    // salvage while bounding a backend outage to
    // 1 + MAX_CONSECUTIVE_BLOB_READ_ERRORS requests per group.
    let (rest, consecutive) = match failed_at {
        // That blob's read already failed once: record it rather than
        // repeating the same request, and count it towards the bound.
        Some(i) => {
            let blob = group
                .blobs
                .get(i)
                .ok_or_else(|| VykarError::Other("failed blob index out of range".into()))?;
            ctx.source_failure(blob, err)?;
            (group.blobs.get(i + 1..).unwrap_or_default(), 1)
        }
        None => (&group.blobs[..], 0),
    };
    retry_blobs_one_by_one(ctx, &group.pack_id, rest, consecutive, bufs, pw)
}

/// `group`'s pack answered 404: read its blobs where the latest head stores
/// them (each new group on this thread). Blobs the latest head no longer
/// has fail with [`VykarError::ChunkRemovedDuringRead`]; a failed
/// relocation (the pack is lost, or no relocator) is today's
/// pack-not-found failure of every blob.
fn process_relocated(
    ctx: &BlobCtx<'_>,
    group: &ReadGroup,
    bufs: &mut ReadBuffers,
    pw: &mut PendingWrite,
    depth: u32,
) -> std::result::Result<(), ReadError> {
    let located = match ctx.relocator {
        Some(relocator) if depth < MAX_RELOCATE_ROUNDS => {
            let ids: Vec<_> = group.blobs.iter().map(|b| b.chunk_id).collect();
            relocator.relocate(&group.pack_id, &ids)
        }
        _ => Err(pack_not_found(&group.pack_id)),
    };
    let located = match located {
        Ok(located) => located,
        Err(e) => {
            if ctx.failures.is_none() || matches!(e, VykarError::Interrupted) {
                return Err(ReadError::source(e));
            }
            let msg = e.to_string();
            let mut first = Some(e);
            for blob in &group.blobs {
                let e = first
                    .take()
                    .unwrap_or_else(|| VykarError::Other(msg.clone()));
                ctx.source_failure(blob, e)?;
            }
            return Ok(());
        }
    };
    let mut by_pack: HashMap<PackId, Vec<PlannedBlob>> = HashMap::new();
    for (blob, at) in group.blobs.iter().zip(located) {
        match at {
            Located::Gone => ctx.source_failure(
                blob,
                VykarError::ChunkRemovedDuringRead {
                    chunk_id: blob.chunk_id,
                },
            )?,
            Located::At {
                pack_id,
                pack_offset,
                stored_size,
            } => by_pack.entry(pack_id).or_default().push(PlannedBlob {
                chunk_id: blob.chunk_id,
                pack_offset,
                stored_size,
                expected_size: blob.expected_size,
                targets: blob.targets.clone(),
            }),
        }
    }
    for moved in coalesce_groups(by_pack)? {
        if ctx.stopping() {
            return Ok(());
        }
        process_group_with(ctx, &moved, bufs, pw, depth + 1)?;
    }
    Ok(())
}

/// Continue-mode salvage: read `blobs` with one request each, recording the
/// ones that fail. `consecutive` is the run of read failures just before
/// `blobs[0]`.
fn retry_blobs_one_by_one(
    ctx: &BlobCtx<'_>,
    pack_id: &PackId,
    blobs: &[PlannedBlob],
    mut consecutive: usize,
    bufs: &mut ReadBuffers,
    pw: &mut PendingWrite,
) -> std::result::Result<(), ReadError> {
    for (i, blob) in blobs.iter().enumerate() {
        if ctx.stopping() {
            return Ok(());
        }
        let err = match process_blobs(
            ctx,
            pack_id,
            blob.pack_offset,
            u64::from(blob.stored_size),
            std::slice::from_ref(blob),
            bufs,
            pw,
        )? {
            GroupEnd::Done => {
                consecutive = 0;
                continue;
            }
            GroupEnd::Stopped => return Ok(()),
            GroupEnd::PackNotFound => pack_not_found(pack_id),
            GroupEnd::OpenFailed(e) | GroupEnd::ReadFailed { error: e, .. } => e,
        };
        consecutive += 1;
        if consecutive < MAX_CONSECUTIVE_BLOB_READ_ERRORS {
            ctx.source_failure(blob, err)?;
            continue;
        }
        let msg = format!(
            "{err} (rest of the read group not retried after \
             {consecutive} consecutive failures)"
        );
        ctx.source_failure(blob, err)?;
        for rest in blobs.get(i + 1..).unwrap_or_default() {
            ctx.source_failure(rest, VykarError::Other(msg.clone()))?;
        }
        return Ok(());
    }
    Ok(())
}

/// Per-blob retries of a failed group read stop after this many consecutive
/// storage failures; the remaining blobs of the group are marked damaged.
const MAX_CONSECUTIVE_BLOB_READ_ERRORS: usize = 3;

/// Open `[offset, offset + length)` of `pack_id`, then read, decode and
/// write `blobs` (all inside that range) through `ctx.reader`.
///
/// Decode failures are handled here, per blob. Returns how the read ended:
/// - `Done`: every blob handled (written, or recorded as damaged) and flushed.
/// - `ReadFailed { index, .. }`: every blob before `index` has been handled
///   and flushed, none from `index` on has been touched. The caller decides
///   between stopping and salvaging.
/// - `PackNotFound`, `OpenFailed`: nothing was touched.
/// - `Stopped`: the pass is stopping. The write accumulator is abandoned
///   unwritten; the caller cleans up the temp restore root on
///   error/cancellation, so no partial file reaches the final destination.
fn process_blobs(
    ctx: &BlobCtx<'_>,
    pack_id: &PackId,
    offset: u64,
    length: u64,
    blobs: &[PlannedBlob],
    bufs: &mut ReadBuffers,
    pw: &mut PendingWrite,
) -> std::result::Result<GroupEnd, ReadError> {
    let (files, counters, root) = (ctx.files, ctx.counters, ctx.root);
    let mut file_handles = LruHandles::new();

    // Write accumulator — batches consecutive same-file writes into one
    // syscall. Owned by the worker thread; anything an earlier call left
    // behind on a stop is dropped here, never written.
    pw.reset();

    let end = ctx.reader.read_range(
        pack_id,
        offset,
        length,
        blobs,
        bufs,
        || ctx.stopping(),
        |blob, decoded| {
            let decompress_buf = match decoded {
                Ok(buf) => buf,
                Err(e) => return ctx.source_failure(blob, e.into_located()),
            };
            write_targets(
                blob,
                decompress_buf,
                pw,
                &mut file_handles,
                files,
                counters,
                root,
            )?;
            Ok(())
        },
    )?;

    if matches!(end, GroupEnd::Done | GroupEnd::ReadFailed { .. }) {
        // Flush any remaining pending write.
        flush_pending(pw, &mut file_handles, files, counters, root)?;
    }
    Ok(end)
}

/// Write one decoded blob to each of its targets through the accumulator.
fn write_targets(
    blob: &PlannedBlob,
    decompress_buf: &[u8],
    pw: &mut PendingWrite,
    file_handles: &mut LruHandles,
    files: &[PlannedFile],
    counters: &WriteCounters,
    root: &Path,
) -> Result<()> {
    for target in &blob.targets {
        let contiguous = pw.is_active()
            && pw.file_idx == target.file_idx
            && target.file_offset == pw.start + pw.buf.len() as u64;
        let can_append = contiguous && pw.buf.len() + decompress_buf.len() <= MAX_WRITE_BATCH;

        if can_append {
            pw.buf.extend_from_slice(decompress_buf);
            // Eager flush when batch is full.
            if pw.buf.len() >= MAX_WRITE_BATCH {
                write_buf(
                    &pw.buf,
                    pw.file_idx,
                    pw.start,
                    file_handles,
                    files,
                    counters,
                    root,
                )?;
                pw.reset();
            }
        } else {
            // Flush any active pending write first.
            flush_pending(pw, file_handles, files, counters, root)?;

            if contiguous || decompress_buf.len() >= MAX_WRITE_BATCH {
                // Contiguous overflow or standalone large chunk — write
                // directly from decompress_buf so no data lingers in the
                // accumulator (same cancellation safety as the old code).
                write_buf(
                    decompress_buf,
                    target.file_idx,
                    target.file_offset,
                    file_handles,
                    files,
                    counters,
                    root,
                )?;
            } else {
                // Start new accumulation sequence (reuses existing allocation).
                pw.rebind(target.file_idx, target.file_offset);
                pw.buf.extend_from_slice(decompress_buf);
            }
        }
    }
    Ok(())
}

/// Write `buf` to the target file at `offset`.  Opens the (already staged)
/// file on first access.  Counts the bytes in `counters`.
fn write_buf(
    buf: &[u8],
    file_idx: usize,
    offset: u64,
    file_handles: &mut LruHandles,
    files: &[PlannedFile],
    counters: &WriteCounters,
    root: &Path,
) -> Result<()> {
    if file_handles.get(file_idx).is_none() {
        let pf = files
            .get(file_idx)
            .ok_or_else(|| VykarError::Other(format!("file_idx {file_idx} out of range")))?;
        let full_path = root.join(&pf.rel_path);
        // Staging already created and sized the file; never create or
        // truncate here, and never follow a symlink at the leaf.
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true);
        fs::set_nofollow(&mut opts);
        let handle = opts.open(&full_path).map_err(|e| {
            VykarError::Other(format!(
                "failed to open {} for writing: {e}",
                full_path.display()
            ))
        })?;
        file_handles.insert(file_idx, handle);
    }
    let fh = file_handles
        .get(file_idx)
        .ok_or_else(|| VykarError::Other("missing file handle in restore worker".into()))?;
    fs::write_all_at(fh, buf, offset)?;
    counters.wrote(file_idx, buf.len() as u64);
    Ok(())
}

/// Flush the write accumulator to disk if it contains data, then reset it.
fn flush_pending(
    pw: &mut PendingWrite,
    file_handles: &mut LruHandles,
    files: &[PlannedFile],
    counters: &WriteCounters,
    root: &Path,
) -> Result<()> {
    if pw.is_active() && !pw.buf.is_empty() {
        write_buf(
            &pw.buf,
            pw.file_idx,
            pw.start,
            file_handles,
            files,
            counters,
            root,
        )?;
    }
    pw.reset();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::restore::plan::WriteTarget;
    use crate::commands::restore::read_groups::PlannedBlob;
    use crate::commands::restore::test_support::{
        chunk_id_of, dummy_chunk_id, dummy_pack_id, multi_blob_group, pack_blob, single_blob_group,
        stage_files,
    };
    use crate::compress::Compression;
    use crate::repo::control::FnMutSink;
    use crate::repo::format::{ChunkDecodeStage, ObjectType, pack_object_with_context};
    use crate::testutil::{MemoryBackend, test_chunk_hasher};
    use smallvec::SmallVec;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;
    use vykar_crypto::PlaintextEngine;
    use vykar_crypto::aes_gcm::Aes256GcmEngine;
    use vykar_storage::RangeReader;
    use vykar_types::chunk_id::ChunkId;

    #[test]
    fn process_read_group_rejects_decode_size_mismatch() {
        let temp = tempdir().unwrap();
        let out = temp.path().join("out.bin");
        let file = std::fs::File::create(&out).unwrap();
        file.set_len(5).unwrap();

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("out.bin").into(),
            total_size: 5,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }];

        let payload = b"abc";
        let compressed = crate::compress::compress(Compression::None, payload).unwrap();
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            dummy_chunk_id(0xAA).as_bytes(),
            &compressed,
            &crypto,
        )
        .unwrap();

        let pack_id = dummy_pack_id(9);
        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let group = ReadGroup {
            pack_id,
            read_start: 0,
            read_end: packed.len() as u64,
            blobs: vec![PlannedBlob {
                chunk_id: dummy_chunk_id(0xAA),
                pack_offset: 0,
                stored_size: packed.len() as u32,
                expected_size: 5,
                targets: smallvec::smallvec![WriteTarget {
                    file_idx: 0,
                    file_offset: 0,
                }],
            }],
        };

        let counters = WriteCounters::bytes_only();
        let cancelled = AtomicBool::new(false);
        let mut bufs = ReadBuffers::default();
        let err = process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &counters,
            &cancelled,
            &mut bufs,
            &mut PendingWrite::new(),
            temp.path(),
            false,
            &CancelToken::new(),
            None,
            None,
        )
        .map_err(ReadError::into_inner)
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("size mismatch after restore decode"),
            "expected size mismatch error, got: {err}"
        );
    }

    #[test]
    fn process_read_group_large_chunk_direct_write() {
        // A single blob with expected_size >= MAX_WRITE_BATCH should be
        // written directly from decompress_buf, bypassing the accumulator.
        let temp = tempdir().unwrap();
        let out = temp.path().join("big.bin");

        let big_data = vec![0xABu8; MAX_WRITE_BATCH + 1024];
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let cid = chunk_id_of(&big_data);
        let packed = pack_blob(cid, &big_data, &crypto);

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("big.bin").into(),
            total_size: big_data.len() as u64,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }];

        let pack_id = dummy_pack_id(1);
        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let group = single_blob_group(
            pack_id,
            cid,
            &packed,
            big_data.len() as u32,
            smallvec::smallvec![WriteTarget {
                file_idx: 0,
                file_offset: 0,
            }],
        );

        stage_files(temp.path(), &files);
        let counters = WriteCounters::tracking(&files);
        let cancelled = AtomicBool::new(false);
        let mut bufs = ReadBuffers::default();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &counters,
            &cancelled,
            &mut bufs,
            &mut PendingWrite::new(),
            temp.path(),
            false,
            &CancelToken::new(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(counters.bytes(), big_data.len() as u64);
        assert_eq!(counters.files_done(), 1);
        let contents = std::fs::read(&out).unwrap();
        assert_eq!(contents, big_data);
    }

    #[test]
    fn process_read_group_exact_batch_boundary() {
        // Two consecutive contiguous blobs whose cumulative size hits exactly
        // MAX_WRITE_BATCH.  Verifies the eager flush fires at the boundary.
        let temp = tempdir().unwrap();
        let out = temp.path().join("exact.bin");

        let half = MAX_WRITE_BATCH / 2;
        let chunk_a = vec![0xAAu8; half];
        let chunk_b = vec![0xBBu8; MAX_WRITE_BATCH - half];

        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let cid_a = chunk_id_of(&chunk_a);
        let cid_b = chunk_id_of(&chunk_b);
        let packed_a = pack_blob(cid_a, &chunk_a, &crypto);
        let packed_b = pack_blob(cid_b, &chunk_b, &crypto);

        let total_size = (chunk_a.len() + chunk_b.len()) as u64;
        let files = vec![PlannedFile {
            rel_path: PathBuf::from("exact.bin").into(),
            total_size,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }];

        let pack_id = dummy_pack_id(2);
        let (pack_data, group) = multi_blob_group(
            pack_id,
            vec![
                (
                    cid_a,
                    packed_a,
                    chunk_a.len() as u32,
                    smallvec::smallvec![WriteTarget {
                        file_idx: 0,
                        file_offset: 0,
                    }],
                ),
                (
                    cid_b,
                    packed_b,
                    chunk_b.len() as u32,
                    smallvec::smallvec![WriteTarget {
                        file_idx: 0,
                        file_offset: chunk_a.len() as u64,
                    }],
                ),
            ],
        );

        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &pack_data).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        stage_files(temp.path(), &files);
        let counters = WriteCounters::tracking(&files);
        let cancelled = AtomicBool::new(false);
        let mut bufs = ReadBuffers::default();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &counters,
            &cancelled,
            &mut bufs,
            &mut PendingWrite::new(),
            temp.path(),
            false,
            &CancelToken::new(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(counters.bytes(), total_size);
        assert_eq!(counters.files_done(), 1);
        let contents = std::fs::read(&out).unwrap();
        let mut expected = chunk_a;
        expected.extend_from_slice(&chunk_b);
        assert_eq!(contents, expected);
    }

    #[test]
    fn process_read_group_cross_file_switch() {
        // Two blobs targeting different files in the same read group.
        // Verifies flush occurs on file switch and both files get correct content.
        let temp = tempdir().unwrap();
        let out_a = temp.path().join("a.bin");
        let out_b = temp.path().join("b.bin");

        let data_a = vec![0xAAu8; 4096];
        let data_b = vec![0xBBu8; 8192];

        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let cid_a = chunk_id_of(&data_a);
        let cid_b = chunk_id_of(&data_b);
        let packed_a = pack_blob(cid_a, &data_a, &crypto);
        let packed_b = pack_blob(cid_b, &data_b, &crypto);

        let files = vec![
            PlannedFile {
                rel_path: PathBuf::from("a.bin").into(),
                total_size: data_a.len() as u64,
                mode: 0o644,
                mtime: 0,
                uid: 0,
                gid: 0,
                xattrs: None,
            },
            PlannedFile {
                rel_path: PathBuf::from("b.bin").into(),
                total_size: data_b.len() as u64,
                mode: 0o644,
                mtime: 0,
                uid: 0,
                gid: 0,
                xattrs: None,
            },
        ];

        let pack_id = dummy_pack_id(3);
        let (pack_data, group) = multi_blob_group(
            pack_id,
            vec![
                (
                    cid_a,
                    packed_a,
                    data_a.len() as u32,
                    smallvec::smallvec![WriteTarget {
                        file_idx: 0,
                        file_offset: 0,
                    }],
                ),
                (
                    cid_b,
                    packed_b,
                    data_b.len() as u32,
                    smallvec::smallvec![WriteTarget {
                        file_idx: 1,
                        file_offset: 0,
                    }],
                ),
            ],
        );

        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &pack_data).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        stage_files(temp.path(), &files);
        let counters = WriteCounters::tracking(&files);
        let cancelled = AtomicBool::new(false);
        let mut bufs = ReadBuffers::default();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &counters,
            &cancelled,
            &mut bufs,
            &mut PendingWrite::new(),
            temp.path(),
            false,
            &CancelToken::new(),
            None,
            None,
        )
        .unwrap();

        let total = (data_a.len() + data_b.len()) as u64;
        assert_eq!(counters.bytes(), total);
        assert_eq!(counters.files_done(), files.len() as u64);
        assert_eq!(std::fs::read(&out_a).unwrap(), data_a);
        assert_eq!(std::fs::read(&out_b).unwrap(), data_b);
    }

    #[test]
    fn process_read_group_contiguous_batching() {
        // Multiple small contiguous blobs to the same file should be batched
        // into fewer writes.  We verify correct content at the end.
        let temp = tempdir().unwrap();
        let out = temp.path().join("batched.bin");

        let num_chunks = 8u8;
        let chunk_size = 1024usize; // well under MAX_WRITE_BATCH
        let crypto = PlaintextEngine::new(test_chunk_hasher());

        #[allow(clippy::type_complexity)]
        let mut entries: Vec<(ChunkId, Vec<u8>, u32, SmallVec<[WriteTarget; 1]>)> = Vec::new();
        let mut expected_data = Vec::new();
        for i in 0u8..num_chunks {
            let data = vec![0x10 + i; chunk_size];
            expected_data.extend_from_slice(&data);
            let cid = chunk_id_of(&data);
            let packed = pack_blob(cid, &data, &crypto);
            entries.push((
                cid,
                packed,
                chunk_size as u32,
                smallvec::smallvec![WriteTarget {
                    file_idx: 0,
                    file_offset: (i as u64) * (chunk_size as u64),
                }],
            ));
        }

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("batched.bin").into(),
            total_size: expected_data.len() as u64,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }];

        let pack_id = dummy_pack_id(4);
        let (pack_data, group) = multi_blob_group(pack_id, entries);

        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &pack_data).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        stage_files(temp.path(), &files);
        let counters = WriteCounters::tracking(&files);
        let cancelled = AtomicBool::new(false);
        let mut bufs = ReadBuffers::default();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &counters,
            &cancelled,
            &mut bufs,
            &mut PendingWrite::new(),
            temp.path(),
            false,
            &CancelToken::new(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(counters.bytes(), expected_data.len() as u64);
        assert_eq!(std::fs::read(&out).unwrap(), expected_data);
        assert_eq!(
            counters.files_done(),
            1,
            "eight batched blobs complete one file"
        );
    }

    /// `--verify` recomputes the keyed chunk ID after decode and
    /// must accept blobs whose stored chunk_id matches the actual data.
    #[test]
    fn process_read_group_verify_accepts_matching_chunk_id() {
        let temp = tempdir().unwrap();
        let out = temp.path().join("good.bin");

        let payload = b"verify-good-data";
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let cid = ChunkId::compute(&test_chunk_hasher(), payload);
        let packed = pack_blob(cid, payload, &crypto);

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("good.bin").into(),
            total_size: payload.len() as u64,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }];

        let pack_id = dummy_pack_id(7);
        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let group = single_blob_group(
            pack_id,
            cid,
            &packed,
            payload.len() as u32,
            smallvec::smallvec![WriteTarget {
                file_idx: 0,
                file_offset: 0,
            }],
        );

        stage_files(temp.path(), &files);
        let counters = WriteCounters::bytes_only();
        let cancelled = AtomicBool::new(false);
        let mut bufs = ReadBuffers::default();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &counters,
            &cancelled,
            &mut bufs,
            &mut PendingWrite::new(),
            temp.path(),
            true,
            &CancelToken::new(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(std::fs::read(&out).unwrap(), payload);
    }

    /// Run `process_read_group` on fresh scratch buffers; returns bytes written.
    fn run_group(
        group: &ReadGroup,
        files: &[PlannedFile],
        storage: &Arc<dyn StorageBackend>,
        crypto: &dyn CryptoEngine,
        root: &std::path::Path,
        verify_chunks: bool,
    ) -> Result<u64> {
        let counters = WriteCounters::bytes_only();
        process_read_group(
            group,
            files,
            storage,
            crypto,
            &counters,
            &AtomicBool::new(false),
            &mut ReadBuffers::default(),
            &mut PendingWrite::new(),
            root,
            verify_chunks,
            &CancelToken::new(),
            None,
            None,
        )
        .map_err(ReadError::into_inner)?;
        Ok(counters.bytes())
    }

    /// Encrypted repo: a blob whose stored chunk_id disagrees with its bytes
    /// (writer-side bug) passes AEAD, because it was sealed under that same
    /// wrong ID as AAD. Only `--verify` catches it; without it the blob
    /// restores, so hashing stays opt-in when encrypting.
    #[test]
    fn process_read_group_verify_rejects_chunk_id_mismatch() {
        let payload = b"verify-bad-data";
        let crypto = Aes256GcmEngine::new(&[0x11; 32], test_chunk_hasher());
        let real_cid = ChunkId::compute(crypto.chunk_hasher(), payload);
        let wrong_cid = dummy_chunk_id(0xEE);
        assert_ne!(real_cid, wrong_cid);
        let packed = pack_blob(wrong_cid, payload, &crypto);

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("bad.bin").into(),
            total_size: payload.len() as u64,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }];

        let pack_id = dummy_pack_id(8);
        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let group = single_blob_group(
            pack_id,
            wrong_cid,
            &packed,
            payload.len() as u32,
            smallvec::smallvec![WriteTarget {
                file_idx: 0,
                file_offset: 0,
            }],
        );

        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);
        let err = run_group(&group, &files, &storage, &crypto, temp.path(), true).unwrap_err();
        assert!(
            matches!(&err, VykarError::ChunkDecode(e)
                if matches!(e.stage, ChunkDecodeStage::IdMismatch { .. })
                    && e.to_string().contains("hash mismatch")),
            "expected hash mismatch error, got: {err}"
        );

        let temp2 = tempdir().unwrap();
        stage_files(temp2.path(), &files);
        run_group(&group, &files, &storage, &crypto, temp2.path(), false).unwrap();
        assert_eq!(
            std::fs::read(temp2.path().join("bad.bin")).unwrap(),
            payload
        );
    }

    /// Plaintext repo: a flipped payload byte survives decode (no AEAD, no
    /// compression, same length) and must be caught by the always-on chunk ID
    /// check even with `verify_chunks = false`.
    #[test]
    fn process_read_group_plaintext_rejects_corrupt_payload_without_verify() {
        let payload = b"plaintext-payload-that-gets-corrupted";
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let cid = chunk_id_of(payload);
        let mut packed = pack_blob(cid, payload, &crypto);
        let pos = packed
            .windows(payload.len())
            .position(|w| w == payload)
            .expect("plaintext payload is stored verbatim");
        packed[pos + payload.len() / 2] ^= 0x01;

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("bad.bin").into(),
            total_size: payload.len() as u64,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }];

        let pack_id = dummy_pack_id(10);
        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let group = single_blob_group(
            pack_id,
            cid,
            &packed,
            payload.len() as u32,
            smallvec::smallvec![WriteTarget {
                file_idx: 0,
                file_offset: 0,
            }],
        );

        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);
        let err = run_group(&group, &files, &storage, &crypto, temp.path(), false).unwrap_err();
        assert!(
            matches!(&err, VykarError::ChunkDecode(e)
                if matches!(e.stage, ChunkDecodeStage::IdMismatch { .. })
                    && e.to_string().contains("hash mismatch")),
            "expected hash mismatch error, got: {err}"
        );
        assert_eq!(
            std::fs::read(temp.path().join("bad.bin")).unwrap(),
            vec![0u8; payload.len()],
            "nothing may be written for a corrupt chunk"
        );
    }

    /// `write_buf` never follows a symlink at the leaf (`O_NOFOLLOW`), even
    /// without staging in front of it.
    #[cfg(unix)]
    #[test]
    fn write_buf_does_not_follow_symlink_leaf() {
        let temp = tempdir().unwrap();
        let victim = temp.path().join("victim");
        std::fs::write(&victim, b"untouched").unwrap();
        let root = temp.path().join("root");
        std::fs::create_dir(&root).unwrap();
        std::os::unix::fs::symlink(&victim, root.join("x")).unwrap();

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("x").into(),
            total_size: 4,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }];
        let mut handles = LruHandles::new();
        let counters = WriteCounters::bytes_only();
        let res = write_buf(b"evil", 0, 0, &mut handles, &files, &counters, &root);
        assert!(res.is_err(), "write through a symlink leaf must fail");
        assert_eq!(std::fs::read(&victim).unwrap(), b"untouched");
        assert_eq!(counters.bytes(), 0);
    }

    /// Backend that raises a shared cancellation flag from inside the range read
    /// the restore pool uses, simulating a signal that lands once workers are
    /// already running.
    struct CancelOnReadBackend {
        inner: MemoryBackend,
        flag: CancelToken,
    }

    impl vykar_storage::InnerBackend for CancelOnReadBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for CancelOnReadBackend;
        except [get_range_into, open_range];

        fn open_range<'a>(
            &self,
            key: &str,
            offset: u64,
            length: u64,
            buf: &'a mut Vec<u8>,
        ) -> vykar_types::error::Result<Option<Box<dyn vykar_storage::RangeReader + 'a>>> {
            vykar_storage::buffered_range(self, key, offset, length, buf)
        }

        fn get_range_into(
            &self,
            key: &str,
            offset: u64,
            length: u64,
            buf: &mut Vec<u8>,
        ) -> vykar_types::error::Result<bool> {
            self.flag.cancel();
            self.inner.get_range_into(key, offset, length, buf)
        }
    }

    /// One pack holding one chunk, plus the `PlannedFile` and `ReadGroup` that
    /// target it. Returns the packed bytes so the caller can build the group.
    fn one_chunk_pack(crypto: &PlaintextEngine, payload: &[u8]) -> (PackId, Vec<u8>) {
        let compressed = crate::compress::compress(Compression::None, payload).unwrap();
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            dummy_chunk_id(0xC1).as_bytes(),
            &compressed,
            crypto,
        )
        .unwrap();
        (dummy_pack_id(3), packed)
    }

    fn one_file_plan(payload_len: u64) -> Vec<PlannedFile> {
        vec![PlannedFile {
            rel_path: PathBuf::from("out.bin").into(),
            total_size: payload_len,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }]
    }

    /// A flag raised *while the pool is running* must surface as `Interrupted`.
    ///
    /// This is the post-join guard specifically: the flag starts clear, so the
    /// entry guard passes and workers do start. They then observe the flag and
    /// return `Ok(())` early, which without the post-join guard would hand back
    /// a short byte count as success — and the caller would publish a partial
    /// tree.
    #[test]
    fn execute_parallel_restore_cancelled_mid_pool_returns_interrupted() {
        let temp = tempdir().unwrap();
        let out = temp.path().join("out.bin");

        let payload = b"parallel-cancel-payload";
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let (pack_id, packed) = one_chunk_pack(&crypto, payload);

        let shutdown = CancelToken::new();
        let inner = MemoryBackend::new();
        inner.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(CancelOnReadBackend {
            inner,
            flag: shutdown.clone(),
        });

        let files = one_file_plan(payload.len() as u64);
        let groups = vec![single_blob_group(
            pack_id,
            dummy_chunk_id(0xC1),
            &packed,
            payload.len() as u32,
            smallvec::smallvec![WriteTarget {
                file_idx: 0,
                file_offset: 0,
            }],
        )];

        assert!(!shutdown.is_cancelled(), "flag must start clear");
        let err = execute_parallel_restore(
            &files,
            groups,
            &storage,
            &crypto,
            temp.path(),
            2,
            false,
            &shutdown,
            None,
            None,
            None,
        )
        .map_err(ReadError::into_inner)
        .unwrap_err();

        assert!(
            matches!(err, VykarError::Interrupted),
            "expected Interrupted, got: {err}"
        );
        assert!(
            shutdown.is_cancelled(),
            "the backend should have raised the flag mid-pool"
        );
        assert!(!out.exists(), "no data may have been written");
    }

    /// A pre-set shutdown flag must abort the pool with `Interrupted` before any
    /// worker starts. Kept separate from the mid-pool test above: this one exits
    /// at the entry guard and so cannot exercise the post-join guard.
    #[test]
    fn execute_parallel_restore_pre_cancelled_returns_interrupted() {
        let temp = tempdir().unwrap();
        let out = temp.path().join("out.bin");

        let payload = b"parallel-cancel-payload";
        let compressed = crate::compress::compress(Compression::None, payload).unwrap();
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            dummy_chunk_id(0xC1).as_bytes(),
            &compressed,
            &crypto,
        )
        .unwrap();

        let pack_id = dummy_pack_id(3);
        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("out.bin").into(),
            total_size: payload.len() as u64,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }];
        let groups = vec![single_blob_group(
            pack_id,
            dummy_chunk_id(0xC1),
            &packed,
            payload.len() as u32,
            smallvec::smallvec![WriteTarget {
                file_idx: 0,
                file_offset: 0,
            }],
        )];

        let shutdown = CancelToken::new();
        shutdown.cancel();
        let err = execute_parallel_restore(
            &files,
            groups,
            &storage,
            &crypto,
            temp.path(),
            2,
            false,
            &shutdown,
            None,
            None,
            None,
        )
        .map_err(ReadError::into_inner)
        .unwrap_err();

        assert!(
            matches!(err, VykarError::Interrupted),
            "expected Interrupted, got: {err}"
        );
        assert!(!out.exists(), "no data may have been written");
    }

    // -----------------------------------------------------------------------
    // Error classification and continue-on-error
    // -----------------------------------------------------------------------

    fn file(name: &str, size: u64) -> PlannedFile {
        PlannedFile {
            rel_path: PathBuf::from(name).into(),
            total_size: size,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }
    }

    fn target(file_idx: usize) -> SmallVec<[WriteTarget; 1]> {
        smallvec::smallvec![WriteTarget {
            file_idx,
            file_offset: 0,
        }]
    }

    /// Flip one payload byte of a plaintext packed blob (same length, so only
    /// the chunk-ID check catches it).
    fn corrupt(packed: &mut [u8], payload: &[u8]) {
        let pos = packed
            .windows(payload.len())
            .position(|w| w == payload)
            .expect("plaintext payload is stored verbatim");
        packed[pos + payload.len() / 2] ^= 0x01;
    }

    /// A plaintext pack with one blob per payload, blob `i` targeting file `i`.
    /// Blobs listed in `corrupt_idx` have a flipped byte.
    fn pack_of(
        pack_id: PackId,
        payloads: &[&[u8]],
        corrupt_idx: &[usize],
    ) -> (Vec<u8>, ReadGroup, Vec<PlannedFile>) {
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let entries = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let cid = chunk_id_of(p);
                let mut packed = pack_blob(cid, p, &crypto);
                if corrupt_idx.contains(&i) {
                    corrupt(&mut packed, p);
                }
                (cid, packed, p.len() as u32, target(i))
            })
            .collect();
        let (data, group) = multi_blob_group(pack_id, entries);
        let files = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| file(&format!("f{i}.bin"), p.len() as u64))
            .collect();
        (data, group, files)
    }

    fn storage_with(entries: &[(PackId, &[u8])]) -> Arc<dyn StorageBackend> {
        let backend = Arc::new(MemoryBackend::new());
        for (id, data) in entries {
            backend.put(&id.storage_key(), data).unwrap();
        }
        backend
    }

    fn run_group_with(
        group: &ReadGroup,
        files: &[PlannedFile],
        storage: &Arc<dyn StorageBackend>,
        root: &std::path::Path,
        failures: Option<&BatchFailures>,
    ) -> std::result::Result<(), ReadError> {
        run_group_counted(
            group,
            files,
            storage,
            root,
            failures,
            &WriteCounters::bytes_only(),
        )
    }

    /// [`run_group_with`] counting into `counters`.
    fn run_group_counted(
        group: &ReadGroup,
        files: &[PlannedFile],
        storage: &Arc<dyn StorageBackend>,
        root: &std::path::Path,
        failures: Option<&BatchFailures>,
        counters: &WriteCounters,
    ) -> std::result::Result<(), ReadError> {
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        process_read_group(
            group,
            files,
            storage,
            &crypto,
            counters,
            &AtomicBool::new(false),
            &mut ReadBuffers::default(),
            &mut PendingWrite::new(),
            root,
            false,
            &CancelToken::new(),
            None,
            failures,
        )
    }

    fn failed_indices(f: BatchFailures) -> Vec<usize> {
        let mut v: Vec<usize> = f.into_map().into_keys().collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn strict_decode_failure_is_source() {
        let pack_id = dummy_pack_id(0x20);
        let (data, group, files) = pack_of(pack_id, &[b"payload-strict-decode"], &[0]);
        let storage = storage_with(&[(pack_id, &data)]);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);
        let err = run_group_with(&group, &files, &storage, temp.path(), None).unwrap_err();
        assert!(
            matches!(&err, ReadError::Source(VykarError::ChunkDecode(e))
                if matches!(e.stage, ChunkDecodeStage::IdMismatch { .. })
                    && e.to_string().contains("hash mismatch")),
            "got: {err:?}"
        );
    }

    #[test]
    fn strict_missing_pack_is_source() {
        let pack_id = dummy_pack_id(0x21);
        let (_data, group, files) = pack_of(pack_id, &[b"payload-missing-pack"], &[]);
        let storage = storage_with(&[]);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);
        let err = run_group_with(&group, &files, &storage, temp.path(), None).unwrap_err();
        assert!(
            matches!(&err, ReadError::Source(e) if e.to_string().contains("pack not found")),
            "got: {err:?}"
        );
    }

    #[test]
    fn missing_staged_file_is_fatal_in_both_modes() {
        let pack_id = dummy_pack_id(0x22);
        let (data, group, files) = pack_of(pack_id, &[b"payload-unstaged"], &[]);
        let storage = storage_with(&[(pack_id, &data)]);
        let temp = tempdir().unwrap();
        // Not staged: the write path's open fails.
        let err = run_group_with(&group, &files, &storage, temp.path(), None).unwrap_err();
        assert!(matches!(err, ReadError::Fatal(_)), "got: {err:?}");

        let failures = BatchFailures::default();
        let err =
            run_group_with(&group, &files, &storage, temp.path(), Some(&failures)).unwrap_err();
        assert!(matches!(err, ReadError::Fatal(_)), "got: {err:?}");
        assert!(failed_indices(failures).is_empty());
    }

    #[test]
    fn interrupted_is_never_source() {
        assert!(matches!(
            ReadError::source(VykarError::Interrupted),
            ReadError::Fatal(VykarError::Interrupted)
        ));
        assert!(matches!(
            ReadError::source(VykarError::Other("x".into())),
            ReadError::Source(_)
        ));
    }

    #[test]
    fn select_error_prefers_fatal_then_first_source() {
        let src = |m: &str| ReadError::Source(VykarError::Other(m.into()));
        let fatal = || ReadError::Fatal(VykarError::Other("fatal".into()));
        let msg = |e: Option<ReadError>| e.unwrap().into_inner().to_string();

        assert!(matches!(
            select_error(vec![src("a"), fatal()]),
            Some(ReadError::Fatal(_))
        ));
        assert!(matches!(
            select_error(vec![fatal(), src("a")]),
            Some(ReadError::Fatal(_))
        ));
        let first = select_error(vec![src("a"), src("b")]);
        assert!(matches!(first, Some(ReadError::Source(_))));
        assert_eq!(msg(first), "a");
        assert!(select_error(Vec::new()).is_none());
    }

    /// Holds every range read until both workers have issued one, then fails
    /// it: `panic_key` panics (a `Fatal` worker failure), any other key returns
    /// a storage error (`Source`). Both failures happen with no cancellation
    /// check in between, so neither worker can stop the other early.
    struct BarrierBackend {
        inner: MemoryBackend,
        barrier: std::sync::Barrier,
        panic_key: String,
    }

    impl vykar_storage::InnerBackend for BarrierBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for BarrierBackend;
        except [get_range_into, open_range];

        fn open_range<'a>(
            &self,
            key: &str,
            offset: u64,
            length: u64,
            buf: &'a mut Vec<u8>,
        ) -> vykar_types::error::Result<Option<Box<dyn vykar_storage::RangeReader + 'a>>> {
            vykar_storage::buffered_range(self, key, offset, length, buf)
        }

        fn get_range_into(
            &self,
            key: &str,
            _offset: u64,
            _length: u64,
            _buf: &mut Vec<u8>,
        ) -> vykar_types::error::Result<bool> {
            self.barrier.wait();
            if key == self.panic_key {
                simulated_worker_panic();
            }
            Err(VykarError::Other("simulated read error".into()))
        }
    }

    fn simulated_worker_panic() -> ! {
        panic!("simulated worker failure")
    }

    /// A source failure and a fatal failure in concurrent workers must surface
    /// as the fatal one, in either join order. With a progress sink the
    /// coordinating thread waits for the panicking worker to unwind before it
    /// joins; the call must still return.
    #[test]
    fn concurrent_source_and_fatal_failure_reports_fatal() {
        let sink = |_: RestoreEvent| {};
        let batch = BatchProgress {
            sink: &sink,
            base_files: 0,
            base_bytes: 0,
        };
        for (swap, with_progress) in [(false, false), (true, false), (false, true), (true, true)] {
            let progress = with_progress.then_some(&batch);
            let crypto = PlaintextEngine::new(test_chunk_hasher());
            let (pack_src, pack_fatal) = (dummy_pack_id(0x30), dummy_pack_id(0x31));
            let storage: Arc<dyn StorageBackend> = Arc::new(BarrierBackend {
                inner: MemoryBackend::new(),
                barrier: std::sync::Barrier::new(2),
                panic_key: pack_fatal.storage_key(),
            });
            let files = vec![file("a.bin", 8), file("b.bin", 8)];
            let temp = tempdir().unwrap();
            stage_files(temp.path(), &files);

            let packed = [0u8; 16];
            let mut groups = vec![
                single_blob_group(pack_src, dummy_chunk_id(1), &packed, 8, target(0)),
                single_blob_group(pack_fatal, dummy_chunk_id(2), &packed, 8, target(1)),
            ];
            if swap {
                groups.reverse();
            }
            // The barrier needs both groups in separate workers; fail rather
            // than hang if partitioning ever puts them together.
            let sizes: Vec<u64> = groups.iter().map(|g| g.read_end - g.read_start).collect();
            let probe = sizes
                .iter()
                .map(|&n| ReadGroup {
                    pack_id: dummy_pack_id(0),
                    read_start: 0,
                    read_end: n,
                    blobs: Vec::new(),
                })
                .collect();
            assert_eq!(
                partition_groups(probe, 2).len(),
                2,
                "groups must not share a worker"
            );
            let err = execute_parallel_restore(
                &files,
                groups,
                &storage,
                &crypto,
                temp.path(),
                2,
                false,
                &CancelToken::new(),
                None,
                None,
                progress,
            )
            .unwrap_err();
            assert!(
                matches!(&err, ReadError::Fatal(e) if e.to_string().contains("panicked")),
                "swap={swap} progress={with_progress}: got {err:?}"
            );
        }
    }

    #[test]
    fn continue_records_only_the_corrupt_blobs_targets() {
        let pack_id = dummy_pack_id(0x40);
        let payloads: [&[u8]; 3] = [
            b"payload-zero-intact",
            b"payload-one-corrupt",
            b"payload-two-intact",
        ];
        let (data, group, files) = pack_of(pack_id, &payloads, &[1]);
        let storage = storage_with(&[(pack_id, &data)]);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let failures = BatchFailures::default();
        let counters = WriteCounters::tracking(&files);
        run_group_counted(
            &group,
            &files,
            &storage,
            temp.path(),
            Some(&failures),
            &counters,
        )
        .unwrap();
        assert_eq!(counters.files_done(), 2, "the damaged file is not counted");
        let map = failures.into_map();
        assert_eq!(map.keys().copied().collect::<Vec<_>>(), vec![1]);
        assert!(map[&1].contains("hash mismatch"), "got: {}", map[&1]);
        assert_eq!(
            std::fs::read(temp.path().join("f0.bin")).unwrap(),
            payloads[0]
        );
        assert_eq!(
            std::fs::read(temp.path().join("f2.bin")).unwrap(),
            payloads[2]
        );
    }

    #[test]
    fn continue_missing_pack_records_every_target() {
        let pack_id = dummy_pack_id(0x41);
        let payloads: [&[u8]; 2] = [b"payload-missing-a", b"payload-missing-b"];
        let (_data, group, files) = pack_of(pack_id, &payloads, &[]);
        let storage = storage_with(&[]);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let failures = BatchFailures::default();
        run_group_with(&group, &files, &storage, temp.path(), Some(&failures)).unwrap();
        assert_eq!(failed_indices(failures), vec![0, 1]);
    }

    /// The coalesced read fails as a short read; the per-blob retry still
    /// salvages every blob that lies before the truncation point.
    #[test]
    fn continue_truncated_pack_salvages_leading_blobs() {
        let pack_id = dummy_pack_id(0x42);
        let payloads: [&[u8]; 3] = [
            b"payload-before-cut",
            b"payload-across-cut",
            b"payload-after-cut",
        ];
        let (data, group, files) = pack_of(pack_id, &payloads, &[]);
        let cut = (group.blobs[1].pack_offset + 4) as usize;
        let storage = storage_with(&[(pack_id, &data[..cut])]);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        // Strict mode: the whole group fails as unreadable source.
        let err = run_group_with(&group, &files, &storage, temp.path(), None).unwrap_err();
        assert!(matches!(err, ReadError::Source(_)), "got: {err:?}");

        let failures = BatchFailures::default();
        run_group_with(&group, &files, &storage, temp.path(), Some(&failures)).unwrap();
        assert_eq!(failed_indices(failures), vec![1, 2]);
        assert_eq!(
            std::fs::read(temp.path().join("f0.bin")).unwrap(),
            payloads[0]
        );
    }

    #[test]
    fn continue_shared_chunk_damages_every_file_using_it() {
        let pack_id = dummy_pack_id(0x43);
        let payload = b"payload-shared-and-corrupt";
        let (data, mut group, _) = pack_of(pack_id, &[payload], &[0]);
        group.blobs[0].targets.push(WriteTarget {
            file_idx: 1,
            file_offset: 0,
        });
        let files = vec![
            file("a.bin", payload.len() as u64),
            file("b.bin", payload.len() as u64),
        ];
        let storage = storage_with(&[(pack_id, &data)]);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let failures = BatchFailures::default();
        run_group_with(&group, &files, &storage, temp.path(), Some(&failures)).unwrap();
        assert_eq!(failed_indices(failures), vec![0, 1]);
    }

    /// A backend that fails every read (an outage) costs the coalesced read
    /// plus a bounded number of per-blob retries, not one request per blob.
    #[test]
    fn continue_outage_bounds_per_blob_retries() {
        let pack_id = dummy_pack_id(0x44);
        let payloads: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 64]).collect();
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let (_data, group, files) = pack_of(pack_id, &refs, &[]);
        let (backend, log) = crate::testutil::RecordingBackend::new();
        // One byte: every range read past offset 0 is a short read (`Err`).
        backend.put(&pack_id.storage_key(), &[0u8]).unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(backend);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let failures = BatchFailures::default();
        run_group_with(&group, &files, &storage, temp.path(), Some(&failures)).unwrap();
        assert_eq!(failed_indices(failures), (0..10).collect::<Vec<_>>());
        assert_eq!(
            log.get_range_count(),
            1 + MAX_CONSECUTIVE_BLOB_READ_ERRORS as u64
        );
    }

    // ── Streamed ranges (`open_range`) ────────────────────────────────────

    /// Serves ranges from memory piece by piece, failing every `read_at` at an
    /// offset in `fail` (only during the first `fail_opens` opens; later
    /// opens, i.e. retries, succeed), and counts opened ranges.
    struct StreamFailBackend {
        inner: MemoryBackend,
        fail: Vec<u64>,
        fail_opens: usize,
        opens: std::sync::atomic::AtomicUsize,
    }

    struct FailingReader<'a> {
        inner: Box<dyn RangeReader + 'a>,
        fail: Vec<u64>,
    }

    impl RangeReader for FailingReader<'_> {
        fn read_at(&mut self, offset: u64, len: usize) -> Result<&mut [u8]> {
            if self.fail.contains(&offset) {
                return Err(VykarError::Other(format!(
                    "simulated read error at {offset}"
                )));
            }
            self.inner.read_at(offset, len)
        }
    }

    impl vykar_storage::InnerBackend for StreamFailBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for StreamFailBackend;
        except [open_range];

        fn open_range<'a>(
            &self,
            key: &str,
            offset: u64,
            length: u64,
            buf: &'a mut Vec<u8>,
        ) -> Result<Option<Box<dyn RangeReader + 'a>>> {
            let n = self.opens.fetch_add(1, Ordering::SeqCst);
            let fail = if n < self.fail_opens {
                self.fail.clone()
            } else {
                Vec::new()
            };
            Ok(self
                .inner
                .open_range(key, offset, length, buf)?
                .map(|inner| Box::new(FailingReader { inner, fail }) as Box<dyn RangeReader + 'a>))
        }
    }

    fn stream_fail_storage(
        pack_id: PackId,
        data: &[u8],
        group: &ReadGroup,
        fail_blobs: &[usize],
        fail_opens: usize,
    ) -> Arc<StreamFailBackend> {
        let inner = MemoryBackend::new();
        inner.put(&pack_id.storage_key(), data).unwrap();
        Arc::new(StreamFailBackend {
            inner,
            fail: fail_blobs
                .iter()
                .map(|&i| group.blobs[i].pack_offset)
                .collect(),
            fail_opens,
            opens: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn payloads(n: u8) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("streamed-payload-{i:02}").into_bytes())
            .collect()
    }

    /// A streamed read that fails part way through a group, once: the blobs
    /// before it stay written, the failed blob is recorded (its own read
    /// already failed, so it is not repeated), and the rest are salvaged with
    /// one request each.
    #[test]
    fn continue_mid_group_read_failure_salvages_the_rest() {
        let pack_id = dummy_pack_id(0x50);
        let payloads = payloads(4);
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let (data, group, files) = pack_of(pack_id, &refs, &[]);
        let backend = stream_fail_storage(pack_id, &data, &group, &[1], 1);
        let storage: Arc<dyn StorageBackend> = backend.clone();
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let failures = BatchFailures::default();
        run_group_with(&group, &files, &storage, temp.path(), Some(&failures)).unwrap();
        let map = failures.into_map();
        assert_eq!(map.keys().copied().collect::<Vec<_>>(), vec![1]);
        assert!(map[&1].contains("simulated read error"), "got: {}", map[&1]);
        for i in [0, 2, 3] {
            assert_eq!(
                std::fs::read(temp.path().join(format!("f{i}.bin"))).unwrap(),
                payloads[i],
                "blob {i}"
            );
        }
        // The group, then blobs 2 and 3 alone.
        assert_eq!(backend.opens.load(Ordering::SeqCst), 3);
    }

    /// Raises `flag` on the first piece read from any range it opens.
    struct CancelOnPieceBackend {
        inner: MemoryBackend,
        flag: CancelToken,
    }

    struct CancellingReader<'a> {
        inner: Box<dyn RangeReader + 'a>,
        flag: CancelToken,
    }

    impl RangeReader for CancellingReader<'_> {
        fn read_at(&mut self, offset: u64, len: usize) -> Result<&mut [u8]> {
            self.flag.cancel();
            self.inner.read_at(offset, len)
        }
    }

    impl vykar_storage::InnerBackend for CancelOnPieceBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for CancelOnPieceBackend;
        except [open_range];

        fn open_range<'a>(
            &self,
            key: &str,
            offset: u64,
            length: u64,
            buf: &'a mut Vec<u8>,
        ) -> Result<Option<Box<dyn RangeReader + 'a>>> {
            let flag = self.flag.clone();
            Ok(self
                .inner
                .open_range(key, offset, length, buf)?
                .map(|inner| Box::new(CancellingReader { inner, flag }) as Box<dyn RangeReader + 'a>))
        }
    }

    /// A stop seen between two blobs abandons the write accumulator: the
    /// first blob, decoded but still waiting in it, is not written.
    #[test]
    fn stop_mid_group_does_not_flush_the_accumulator() {
        let pack_id = dummy_pack_id(0x55);
        let payloads = payloads(2);
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        // Both blobs fill one file back to back, so the first one is held in
        // the accumulator to be batched with the second.
        let mut file_offset = 0;
        let entries = payloads
            .iter()
            .map(|p| {
                let cid = chunk_id_of(p);
                let target = smallvec::smallvec![WriteTarget {
                    file_idx: 0,
                    file_offset,
                }];
                file_offset += p.len() as u64;
                (cid, pack_blob(cid, p, &crypto), p.len() as u32, target)
            })
            .collect();
        let (data, group) = multi_blob_group(pack_id, entries);
        let files = vec![file("f0.bin", file_offset)];
        let shutdown = CancelToken::new();
        let inner = MemoryBackend::new();
        inner.put(&pack_id.storage_key(), &data).unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(CancelOnPieceBackend {
            inner,
            flag: shutdown.clone(),
        });
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let counters = WriteCounters::bytes_only();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &counters,
            &AtomicBool::new(false),
            &mut ReadBuffers::default(),
            &mut PendingWrite::new(),
            temp.path(),
            false,
            &shutdown,
            None,
            None,
        )
        .unwrap();
        assert!(shutdown.is_cancelled(), "the first piece raises the flag");
        assert_eq!(counters.bytes(), 0);
        assert_eq!(
            std::fs::read(temp.path().join("f0.bin")).unwrap(),
            vec![0; file_offset as usize]
        );
    }

    /// A mid-group failure that persists counts towards the consecutive
    /// bound: blob 1 (streamed), 2 and 3 (retried) fail, 4 and 5 are recorded
    /// without a request. Every damaged file is recorded exactly once.
    #[test]
    fn continue_mid_group_outage_bounds_retries() {
        let pack_id = dummy_pack_id(0x51);
        let payloads = payloads(6);
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let (data, group, files) = pack_of(pack_id, &refs, &[]);
        let backend = stream_fail_storage(pack_id, &data, &group, &[1, 2, 3, 4, 5], usize::MAX);
        let storage: Arc<dyn StorageBackend> = backend.clone();
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let failures = BatchFailures::default();
        run_group_with(&group, &files, &storage, temp.path(), Some(&failures)).unwrap();
        let map = failures.into_map();
        let mut keys: Vec<usize> = map.keys().copied().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 2, 3, 4, 5]);
        assert!(map[&5].contains("not retried"), "got: {}", map[&5]);
        assert_eq!(
            std::fs::read(temp.path().join("f0.bin")).unwrap(),
            payloads[0]
        );
        assert_eq!(
            backend.opens.load(Ordering::SeqCst),
            MAX_CONSECUTIVE_BLOB_READ_ERRORS
        );
    }

    /// Strict mode: a mid-group read failure stops the pass as `Source`.
    #[test]
    fn strict_mid_group_read_failure_is_source() {
        let pack_id = dummy_pack_id(0x52);
        let payloads = payloads(3);
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let (data, group, files) = pack_of(pack_id, &refs, &[]);
        let storage: Arc<dyn StorageBackend> = stream_fail_storage(pack_id, &data, &group, &[2], 1);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let err = run_group_with(&group, &files, &storage, temp.path(), None).unwrap_err();
        assert!(
            matches!(&err, ReadError::Source(e) if e.to_string().contains("simulated read error")),
            "got: {err:?}"
        );
    }

    /// The streaming `LocalBackend` path end to end: a group whose blobs have
    /// gaps between them (an unrequested blob in the middle and at the end).
    #[test]
    fn local_backend_streamed_group_skips_gaps() {
        let pack_id = dummy_pack_id(0x53);
        let payloads = payloads(5);
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let (data, mut group, files) = pack_of(pack_id, &refs, &[]);
        // Request blobs 0, 2 and 3 only; 1 and 4 become gaps / trailing bytes.
        group.blobs.remove(4);
        group.blobs.remove(1);
        group.read_end = group.blobs[2].pack_offset + u64::from(group.blobs[2].stored_size);

        let repo = tempdir().unwrap();
        let local =
            vykar_storage::local_backend::LocalBackend::new(repo.path().to_str().unwrap()).unwrap();
        local.put(&pack_id.storage_key(), &data).unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(local);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        run_group_with(&group, &files, &storage, temp.path(), None).unwrap();
        for i in [0, 2, 3] {
            assert_eq!(
                std::fs::read(temp.path().join(format!("f{i}.bin"))).unwrap(),
                payloads[i],
                "blob {i}"
            );
        }
    }

    /// A truncated pack on `LocalBackend` fails at open (size check), before
    /// any blob is written, so continue mode salvages exactly as on the
    /// buffered path.
    #[test]
    fn local_backend_truncated_pack_salvages_leading_blobs() {
        let pack_id = dummy_pack_id(0x54);
        let payloads = payloads(3);
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let (data, group, files) = pack_of(pack_id, &refs, &[]);
        let cut = (group.blobs[1].pack_offset + 4) as usize;

        let repo = tempdir().unwrap();
        let local =
            vykar_storage::local_backend::LocalBackend::new(repo.path().to_str().unwrap()).unwrap();
        local.put(&pack_id.storage_key(), &data[..cut]).unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(local);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let err = run_group_with(&group, &files, &storage, temp.path(), None).unwrap_err();
        assert!(
            matches!(&err, ReadError::Source(e) if e.to_string().contains("short read")),
            "got: {err:?}"
        );
        let failures = BatchFailures::default();
        run_group_with(&group, &files, &storage, temp.path(), Some(&failures)).unwrap();
        assert_eq!(failed_indices(failures), vec![1, 2]);
        assert_eq!(
            std::fs::read(temp.path().join("f0.bin")).unwrap(),
            payloads[0]
        );
    }

    /// Encrypted, zstd-compressed blobs decrypted in place inside the streamed
    /// read buffer, on `LocalBackend`, with one scratch state reused across
    /// two groups. A tampered blob fails AEAD and only its file is damaged.
    #[test]
    fn encrypted_streamed_groups_decrypt_in_place() {
        let crypto = Aes256GcmEngine::new(&[0x11; 32], test_chunk_hasher());
        let payloads: Vec<Vec<u8>> = (0..4u8)
            .map(|i| {
                let mut p = format!("encrypted-payload-{i}-")
                    .repeat(40_000)
                    .into_bytes();
                p.truncate(MAX_WRITE_BATCH / 2 + usize::from(i) * 1000);
                p
            })
            .collect();
        let pack_id = dummy_pack_id(0x60);
        let entries = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let cid = ChunkId::compute(crypto.chunk_hasher(), p);
                let compressed =
                    crate::compress::compress(Compression::Zstd { level: 3 }, p).unwrap();
                let packed = pack_object_with_context(
                    ObjectType::ChunkData,
                    cid.as_bytes(),
                    &compressed,
                    &crypto,
                )
                .unwrap();
                (cid, packed, p.len() as u32, target(i))
            })
            .collect();
        let (mut data, group) = multi_blob_group(pack_id, entries);
        let files: Vec<PlannedFile> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| file(&format!("f{i}.bin"), p.len() as u64))
            .collect();
        // Second group: blob 2 alone, tampered (one ciphertext byte).
        let tampered = dummy_pack_id(0x61);
        // First group: blobs 0, 1, 3 (blob 2 is a gap).
        let mut group1 = group;
        let b2 = group1.blobs.remove(2);
        let mut tampered_data = data.clone();
        tampered_data[b2.pack_offset as usize + 20] ^= 0x01;
        let group2 = ReadGroup {
            pack_id: tampered,
            read_start: b2.pack_offset,
            read_end: b2.pack_offset + u64::from(b2.stored_size),
            blobs: vec![b2],
        };

        let repo = tempdir().unwrap();
        let local =
            vykar_storage::local_backend::LocalBackend::new(repo.path().to_str().unwrap()).unwrap();
        local.put(&pack_id.storage_key(), &data).unwrap();
        local.put(&tampered.storage_key(), &tampered_data).unwrap();
        data.clear();
        let storage: Arc<dyn StorageBackend> = Arc::new(local);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let failures = BatchFailures::default();
        let (mut bufs, mut pw) = (ReadBuffers::default(), PendingWrite::new());
        let counters = WriteCounters::tracking(&files);
        for g in [&group1, &group2] {
            process_read_group(
                g,
                &files,
                &storage,
                &crypto,
                &counters,
                &AtomicBool::new(false),
                &mut bufs,
                &mut pw,
                temp.path(),
                false,
                &CancelToken::new(),
                None,
                Some(&failures),
            )
            .unwrap();
        }
        let map = failures.into_map();
        assert_eq!(
            counters.files_done(),
            3,
            "the damaged file is never complete"
        );
        assert_eq!(map.keys().copied().collect::<Vec<_>>(), vec![2]);
        assert!(
            map[&2].contains("decryption failed") || map[&2].contains("Decryption"),
            "got: {}",
            map[&2]
        );
        for i in [0, 1, 3] {
            assert_eq!(
                std::fs::read(temp.path().join(format!("f{i}.bin"))).unwrap(),
                payloads[i],
                "blob {i}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Progress
    // -----------------------------------------------------------------------

    #[test]
    fn write_counters_count_a_file_once_on_its_last_byte() {
        let files = vec![file("e.bin", 0), file("a.bin", 10), file("b.bin", 5)];
        let c = WriteCounters::tracking(&files);
        assert_eq!(c.files_done(), 1, "the empty file counts from the start");
        c.wrote(1, 4);
        assert_eq!(c.files_done(), 1);
        c.wrote(1, 0);
        assert_eq!(c.files_done(), 1, "an empty write completes nothing");
        c.wrote(1, 6);
        assert_eq!(c.files_done(), 2);
        c.wrote(2, 5);
        assert_eq!(c.files_done(), 3);
        assert_eq!(c.bytes(), 15);

        let b = WriteCounters::bytes_only();
        b.wrote(1, 10);
        assert_eq!(b.bytes(), 10);
        assert_eq!(b.files_done(), 0);
        assert!(b.left.is_none());
    }

    /// One blob shared by two files completes both.
    #[test]
    fn shared_blob_completes_every_target_file() {
        let pack_id = dummy_pack_id(0x44);
        let payload = b"payload-shared-intact";
        let (data, mut group, _) = pack_of(pack_id, &[payload], &[]);
        group.blobs[0].targets.push(WriteTarget {
            file_idx: 1,
            file_offset: 0,
        });
        let files = vec![
            file("a.bin", payload.len() as u64),
            file("b.bin", payload.len() as u64),
        ];
        let storage = storage_with(&[(pack_id, &data)]);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let counters = WriteCounters::tracking(&files);
        run_group_counted(&group, &files, &storage, temp.path(), None, &counters).unwrap();
        assert_eq!(counters.files_done(), 2);
        assert_eq!(counters.bytes(), 2 * payload.len() as u64);
    }

    /// Lets the first range read through and holds every later one until
    /// `gate` opens.
    struct GateBackend {
        inner: MemoryBackend,
        reads: std::sync::atomic::AtomicUsize,
        gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
    }

    impl vykar_storage::InnerBackend for GateBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for GateBackend;
        except [get_range_into, open_range];

        fn open_range<'a>(
            &self,
            key: &str,
            offset: u64,
            length: u64,
            buf: &'a mut Vec<u8>,
        ) -> vykar_types::error::Result<Option<Box<dyn vykar_storage::RangeReader + 'a>>> {
            vykar_storage::buffered_range(self, key, offset, length, buf)
        }

        fn get_range_into(
            &self,
            key: &str,
            offset: u64,
            length: u64,
            buf: &mut Vec<u8>,
        ) -> vykar_types::error::Result<bool> {
            if self.reads.fetch_add(1, Ordering::SeqCst) > 0 {
                let (open, cv) = &*self.gate;
                let (open, timeout) = cv
                    .wait_timeout_while(open.lock().unwrap(), Duration::from_secs(30), |o| !*o)
                    .unwrap();
                if !*open || timeout.timed_out() {
                    return Err(VykarError::Other("gate never opened".into()));
                }
            }
            self.inner.get_range_into(key, offset, length, buf)
        }
    }

    /// While the single worker is held before its second group, the
    /// coordinating thread reports the first file as complete, on top of the
    /// earlier batches' totals.
    #[test]
    fn execute_parallel_restore_reports_written_totals() {
        let payloads: [&[u8]; 2] = [b"written-payload-a", b"written-payload-b"];
        let len = payloads[0].len() as u64;
        assert_eq!(
            len,
            payloads[1].len() as u64,
            "equal sizes: either may go first"
        );
        let (pack_a, pack_b) = (dummy_pack_id(0x70), dummy_pack_id(0x71));
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let entries: Vec<(PackId, ChunkId, Vec<u8>)> = payloads
            .iter()
            .zip([pack_a, pack_b])
            .map(|(p, pack)| {
                let cid = chunk_id_of(p);
                (pack, cid, pack_blob(cid, p, &crypto))
            })
            .collect();
        let files: Vec<PlannedFile> = (0..2).map(|i| file(&format!("f{i}.bin"), len)).collect();
        let groups = || -> Vec<ReadGroup> {
            entries
                .iter()
                .enumerate()
                .map(|(i, (pack, cid, packed))| {
                    single_blob_group(*pack, *cid, packed, len as u32, target(i))
                })
                .collect()
        };

        // Reference run without progress.
        let plain = MemoryBackend::new();
        for (pack, _, packed) in &entries {
            plain.put(&pack.storage_key(), packed).unwrap();
        }
        let plain: Arc<dyn StorageBackend> = Arc::new(plain);
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);
        let expected = execute_parallel_restore(
            &files,
            groups(),
            &plain,
            &crypto,
            temp.path(),
            1,
            false,
            &CancelToken::new(),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(expected, 2 * len);

        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let inner = MemoryBackend::new();
        for (pack, _, packed) in &entries {
            inner.put(&pack.storage_key(), packed).unwrap();
        }
        let storage: Arc<dyn StorageBackend> = Arc::new(GateBackend {
            inner,
            reads: std::sync::atomic::AtomicUsize::new(0),
            gate: gate.clone(),
        });
        let temp = tempdir().unwrap();
        stage_files(temp.path(), &files);

        let mut events = Vec::new();
        let got = {
            let sink = FnMutSink::new(|e: RestoreEvent| {
                if let RestoreEvent::Written { files: 8, .. } = e {
                    *gate.0.lock().unwrap() = true;
                    gate.1.notify_all();
                }
                events.push(e);
            });
            let batch = BatchProgress {
                sink: &sink,
                base_files: 7,
                base_bytes: 100,
            };
            execute_parallel_restore(
                &files,
                groups(),
                &storage,
                &crypto,
                temp.path(),
                1,
                false,
                &CancelToken::new(),
                None,
                None,
                Some(&batch),
            )
            .unwrap()
        };
        assert_eq!(got, expected);
        assert!(
            events.contains(&RestoreEvent::Written {
                files: 8,
                bytes: 100 + len,
            }),
            "the held worker's first file must be reported: {events:?}"
        );
        let mut last = (7, 100);
        for e in &events {
            let RestoreEvent::Written { files, bytes } = *e else {
                panic!("only Written comes from the pass: {e:?}");
            };
            assert!(
                files >= last.0 && bytes >= last.1,
                "not monotonic: {events:?}"
            );
            assert!(
                files <= 9 && bytes <= 100 + 2 * len,
                "over the batch: {events:?}"
            );
            last = (files, bytes);
        }
    }
}
