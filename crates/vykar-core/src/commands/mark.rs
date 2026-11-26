//! The mark engine: which index entries do the live snapshots reference?
//!
//! What is read once:
//! - every object under `snapshots/`, listed on storage and decoded strictly
//!   ([`load_snapshots_strict`]);
//! - every distinct tree chunk of those snapshots, resolved through the
//!   index before any read, read in batches sorted by pack and offset, and
//!   decoded once on a pool of decode threads ([`walk_tree_chunks`]),
//!   whatever the number of snapshots naming it.
//!
//! The mark ([`mark_live`]) sets one bit per referenced index position
//! ([`IndexPositions`]) and can count references with snapshot multiplicity,
//! for the sweep record's referenced bytes. It fails closed: a key under
//! `snapshots/` that is not a snapshot ID, a listed object that is gone
//! and still listed by a second listing, a
//! snapshot object that cannot be read, decrypted or decoded, a foreign
//! envelope, a newer format, a tree pointer missing from the index, a tree
//! chunk that cannot be read or does not decode on its own, and (with
//! [`MissingRefs::Abort`]) a data reference missing from the index all fail
//! it.
//!
//! The tree-chunk source is the caller's: the report (`compact
//! --report-unreferenced`) reads the local tree-chunk cache first
//! ([`TreeChunkSource::CacheFirst`]); check and the sweep of the repair
//! read storage only ([`TreeChunkSource::Storage`]), since they verify what
//! the repository holds. The cache never supplies locations.
//!
//! Items are decoded through `Item`'s serde decoder; there is no
//! chunk-ID-only walker.

// Slot indices come from `TreeChunkSet` (below its length; its columns are
// pushed together) and batch positions from ranges over the batch. Plaintext
// ranges into the read buffer come from `push_spans`, which checks that a
// read returned one end per chunk, in order and inside the buffer.
#![allow(clippy::indexing_slicing)]

use std::ops::Range;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::commands::list::{decode_snapshot_meta, for_each_decoded_item};
use crate::index::hasher::ChunkIdHashMap;
use crate::index::table::IndexPositions;
use crate::repo::bounded::try_for_each_bounded;
use crate::repo::control::CancelToken;
use crate::repo::{Repo, TreeChunkSource};
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::ItemType;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

/// Upper bound on tree chunks per read batch.
pub(crate) const MARK_BATCH_CHUNKS: usize = 2_000;
/// Upper bound on stored bytes per read batch; bounds batch plaintext at
/// roughly 2.5x this (old-cut chunks are ~4x larger than new-cut ones).
pub(crate) const MARK_BATCH_STORED_BYTES: u64 = 64 << 20;
/// Cap on decode threads.
pub(crate) const MARK_DECODE_THREADS_MAX: usize = 8;

/// Decode threads for a traversal: `min(available_parallelism, 8)`.
pub(crate) fn default_decode_threads() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(MARK_DECODE_THREADS_MAX)
}

/// Whether `e` belongs to the errors that pass through every context frame:
/// a cancellation, and the newer-vykar class whose message carries the
/// "upgrade vykar" remedy.
pub(crate) fn passes_through(e: &VykarError) -> bool {
    matches!(
        e,
        VykarError::Interrupted
            | VykarError::UnsupportedSnapshotVersion { .. }
            | VykarError::IncompatibleSnapshotEnvelope { .. }
    )
}

// ---------------------------------------------------------------------------
// Strict listing
// ---------------------------------------------------------------------------

/// A snapshot as the strict listing loaded it.
pub(crate) struct LiveSnapshot {
    pub(crate) id: SnapshotId,
    pub(crate) meta: SnapshotMeta,
}

impl LiveSnapshot {
    /// The name for messages: the snapshot's name and its ID's 8-hex prefix.
    pub(crate) fn label(&self) -> String {
        let hex = self.id.to_hex();
        format!("{} ({})", self.meta.name, hex.get(..8).unwrap_or(&hex))
    }
}

/// Every object under `snapshots/` on storage, fetched, authenticated and
/// decoded, sorted by (time, id). Fails closed: a key that is not a snapshot
/// ID, a listed object that is gone and still listed, any read, decrypt or
/// decode failure, a foreign envelope and a format version newer than this
/// build all fail the call. A listed object whose GET finds nothing is
/// skipped only when a second listing of `snapshots/`, taken after every
/// GET, no longer shows it: a concurrent `delete` or `prune` removed it
/// (they run without the lock beside compact, 04 step 9), and a deleted
/// snapshot needs no marking. Reads storage only; the local snapshot list
/// is neither read nor written. Objects are fetched at the backend's
/// request width.
pub(crate) fn load_snapshots_strict(
    repo: &Repo,
    cancel: &CancelToken,
) -> Result<Vec<LiveSnapshot>> {
    cancel.check()?;
    let started = Instant::now();
    let keys = repo.storage().list("snapshots/")?;
    let mut listed: Vec<(String, SnapshotId)> = Vec::with_capacity(keys.len());
    for key in keys {
        let Some(hex) = key.strip_prefix("snapshots/") else {
            return Err(VykarError::Other(format!(
                "unexpected object {key} under snapshots/"
            )));
        };
        // Some backends list the directory itself.
        if hex.is_empty() {
            continue;
        }
        let id = SnapshotId::from_hex(hex)
            .map_err(|_| VykarError::Other(format!("unexpected object {key} under snapshots/")))?;
        listed.push((key, id));
    }

    let loaded: Mutex<Vec<LiveSnapshot>> = Mutex::new(Vec::with_capacity(listed.len()));
    let gone: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let width = repo.storage().profile().request_width;
    try_for_each_bounded(width, &listed, |(key, id)| {
        cancel.check()?;
        let frame = |e: VykarError| {
            if passes_through(&e) {
                e
            } else {
                VykarError::Other(format!("snapshot object {key}: {e}"))
            }
        };
        // A listed key that is gone is a concurrent delete or prune (they
        // take no lock beside a lock-free compact, 04 step 9), a reaped lock,
        // an out-of-band delete or a false "not found" (a proxy's 404, a
        // pooled filesystem branch dropping out), and in the last cases the
        // snapshot may be live. It is skipped only after a second listing,
        // taken after every GET, no longer shows it; a key still listed with
        // a not-found fails closed.
        let Some(blob) = repo.storage().get(key).map_err(frame)? else {
            gone.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(key.clone());
            return Ok(());
        };
        let meta = decode_snapshot_meta(id, &blob, repo.crypto().as_ref()).map_err(frame)?;
        loaded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(LiveSnapshot { id: *id, meta });
        Ok::<(), VykarError>(())
    })?;
    let gone = gone.into_inner().unwrap_or_else(PoisonError::into_inner);
    if !gone.is_empty() {
        cancel.check()?;
        let again: std::collections::HashSet<String> =
            repo.storage().list("snapshots/")?.into_iter().collect();
        for key in &gone {
            if again.contains(key) {
                return Err(VykarError::Other(format!("{key} was listed but is gone")));
            }
            tracing::debug!(
                key,
                "mark: a listed snapshot was deleted before its read; skipped"
            );
        }
    }
    let mut snaps = loaded.into_inner().unwrap_or_else(PoisonError::into_inner);
    snaps.sort_by(|a, b| {
        a.meta
            .time
            .cmp(&b.meta.time)
            .then_with(|| a.id.as_bytes().cmp(b.id.as_bytes()))
    });
    tracing::debug!(
        "mark: listed {} snapshot objects in {:?}",
        snaps.len(),
        started.elapsed()
    );
    Ok(snaps)
}

// ---------------------------------------------------------------------------
// Tree-chunk set
// ---------------------------------------------------------------------------

/// The distinct tree chunks of a list of snapshots, with how often each
/// occurs across all their `item_ptrs` (duplicates within one snapshot
/// included) and the first snapshot that names it (for messages).
pub(crate) struct TreeChunkSet {
    ids: Vec<ChunkId>,
    occurrences: Vec<u32>,
    first_snapshot: Vec<u32>,
    slot: ChunkIdHashMap<u32>,
}

impl TreeChunkSet {
    /// `ptrs[s]` is snapshot `s`'s `item_ptrs`. Slots are numbered in
    /// order of first occurrence.
    pub(crate) fn build(ptrs: &[&[ChunkId]]) -> Self {
        let mut set = Self {
            ids: Vec::new(),
            occurrences: Vec::new(),
            first_snapshot: Vec::new(),
            slot: ChunkIdHashMap::default(),
        };
        for (s, list) in ptrs.iter().enumerate() {
            for id in *list {
                match set.slot.get(id) {
                    Some(&k) => {
                        let n = &mut set.occurrences[k as usize];
                        *n = n.saturating_add(1);
                    }
                    None => {
                        set.slot.insert(*id, set.ids.len() as u32);
                        set.ids.push(*id);
                        set.occurrences.push(1);
                        set.first_snapshot.push(s as u32);
                    }
                }
            }
        }
        set
    }

    pub(crate) fn len(&self) -> usize {
        self.ids.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub(crate) fn id(&self, k: usize) -> &ChunkId {
        &self.ids[k]
    }

    pub(crate) fn occurrences(&self, k: usize) -> u32 {
        self.occurrences[k]
    }

    pub(crate) fn first_snapshot(&self, k: usize) -> usize {
        self.first_snapshot[k] as usize
    }

    pub(crate) fn slot(&self, id: &ChunkId) -> Option<usize> {
        self.slot.get(id).map(|&k| k as usize)
    }

    pub(crate) fn contains(&self, id: &ChunkId) -> bool {
        self.slot.contains_key(id)
    }
}

// ---------------------------------------------------------------------------
// Generic traversal
// ---------------------------------------------------------------------------

/// What the traversal does when a tree chunk cannot be resolved or read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OnChunkError {
    /// Fail the walk (mark, the repair's sweep).
    Abort,
    /// Hand the failure to `collect` and go on (check).
    Record,
}

/// One tree chunk's result as `collect` sees it.
pub(crate) enum ChunkOutcome<T> {
    Decoded(T),
    /// The index has no entry for it (`Record` only).
    NotInIndex,
    /// Reading it failed (`Record` only); the chunk's own error, shared by
    /// the chunks of a pack whose reads fail with an I/O error.
    ReadFailed(Arc<VykarError>),
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct WalkStats {
    pub(crate) chunks: usize,
    pub(crate) stored_bytes: u64,
    pub(crate) plaintext_bytes: u64,
    pub(crate) batches: usize,
    pub(crate) decode_threads: usize,
    pub(crate) read_time: Duration,
    pub(crate) decode_time: Duration,
}

/// A resolved tree chunk: (slot or stream position, chunk location).
pub(crate) type Located = (usize, (ChunkId, PackId, u64, u32));

/// Cut `located` into consecutive batches of at most `max_chunks` chunks
/// and `max_stored` stored bytes, in the order given; a batch holds at
/// least one chunk.
pub(crate) fn cut_batches_with(
    located: &[Located],
    max_chunks: usize,
    max_stored: u64,
) -> Vec<&[Located]> {
    let mut batches = Vec::new();
    let mut start = 0;
    let mut bytes = 0u64;
    for (i, (_, loc)) in located.iter().enumerate() {
        let size = u64::from(loc.3);
        if i > start && (i - start >= max_chunks || bytes + size > max_stored) {
            batches.push(&located[start..i]);
            start = i;
            bytes = 0;
        }
        bytes += size;
    }
    if start < located.len() {
        batches.push(&located[start..]);
    }
    batches
}

/// [`cut_batches_with`] at the mark's caps, [`MARK_BATCH_CHUNKS`] and
/// [`MARK_BATCH_STORED_BYTES`] (`located` sorted by pack and offset).
fn cut_batches(located: &[Located]) -> Vec<&[Located]> {
    cut_batches_with(located, MARK_BATCH_CHUNKS, MARK_BATCH_STORED_BYTES)
}

/// Record the plaintext ranges of a split read of `expected` chunks that
/// appended to the buffer from `from` on (now `buf_len` bytes long), the
/// first chunk at batch position `first`. `ends` is what
/// [`Repo::read_tree_chunks_split`] returned. An internal error
/// unless there is one end per chunk, non-decreasing and inside
/// `from..=buf_len`, so the ranges index the buffer safely.
pub(crate) fn push_spans(
    spans: &mut Vec<(usize, Range<usize>)>,
    first: usize,
    from: usize,
    ends: &[usize],
    expected: usize,
    buf_len: usize,
) -> Result<()> {
    if ends.len() != expected {
        return Err(VykarError::Other(format!(
            "internal error: a read of {expected} tree chunks returned {} chunk ends",
            ends.len()
        )));
    }
    let mut s = from;
    for (j, &e) in ends.iter().enumerate() {
        if e < s || e > buf_len {
            return Err(VykarError::Other(format!(
                "internal error: tree chunk end {e} outside {s}..={buf_len}"
            )));
        }
        spans.push((first + j, s..e));
        s = e;
    }
    Ok(())
}

/// One outcome per position of a batch of `len` chunks, in order, from the
/// decoded and the failed positions (each sorted by position). An internal
/// error when a position has no outcome or more than one, or lies outside
/// the batch: the walk proves that every chunk produced exactly one.
fn batch_outcomes<T>(
    len: usize,
    decoded: impl IntoIterator<Item = (usize, T)>,
    failed: impl IntoIterator<Item = (usize, Arc<VykarError>)>,
) -> Result<Vec<ChunkOutcome<T>>> {
    let mut decoded = decoded.into_iter().peekable();
    let mut failed = failed.into_iter().peekable();
    let mut out = Vec::with_capacity(len);
    for pos in 0..len {
        let outcome = match (
            decoded.next_if(|(p, _)| *p == pos),
            failed.next_if(|(p, _)| *p == pos),
        ) {
            (Some((_, t)), None) => ChunkOutcome::Decoded(t),
            (None, Some((_, e))) => ChunkOutcome::ReadFailed(e),
            (None, None) => {
                return Err(VykarError::Other(format!(
                    "internal error: tree chunk at batch position {pos} has no outcome"
                )));
            }
            (Some(_), Some(_)) => {
                return Err(VykarError::Other(format!(
                    "internal error: tree chunk at batch position {pos} both decoded and failed"
                )));
            }
        };
        out.push(outcome);
    }
    if decoded.next().is_some() || failed.next().is_some() {
        return Err(VykarError::Other(
            "internal error: a tree chunk outcome lies outside its batch".to_string(),
        ));
    }
    Ok(out)
}

/// Re-read the chunks of a batch whose read failed, to attribute the
/// failure (`Record` mode): pack by pack first, chunk by chunk only inside
/// a pack whose read fails. A chunk read failing with an I/O error is given
/// to the pack's remaining chunks without reading them. Fills `buf` and
/// `spans` (batch position, plaintext range) with what was read, and
/// `failed` with (batch position, error).
#[allow(clippy::too_many_arguments)]
fn reread_failed_batch(
    repo: &Repo,
    batch: &[Located],
    batch_error: &VykarError,
    source: TreeChunkSource,
    cancel: &CancelToken,
    buf: &mut Vec<u8>,
    spans: &mut Vec<(usize, Range<usize>)>,
    failed: &mut Vec<(usize, Arc<VykarError>)>,
) -> Result<()> {
    let mut ends = Vec::new();
    let mut start = 0;
    while start < batch.len() {
        let pack = batch[start].1.1;
        let end = start
            + batch[start..]
                .iter()
                .take_while(|(_, loc)| loc.1 == pack)
                .count();
        let run = &batch[start..end];
        let locs: Vec<_> = run.iter().map(|(_, loc)| *loc).collect();
        let from = buf.len();
        match repo.read_tree_chunks_split(&locs, buf, &mut ends, cancel, source) {
            Ok(()) => push_spans(spans, start, from, &ends, run.len(), buf.len())?,
            Err(e @ VykarError::Interrupted) => return Err(e),
            Err(pack_error) => {
                buf.truncate(from);
                let failed_before = failed.len();
                let mut j = 0;
                while j < run.len() {
                    cancel.check()?;
                    let from = buf.len();
                    match repo.read_tree_chunks_split(&locs[j..=j], buf, &mut ends, cancel, source)
                    {
                        Ok(()) => push_spans(spans, start + j, from, &ends, 1, buf.len())?,
                        Err(e @ VykarError::Interrupted) => return Err(e),
                        Err(e) => {
                            buf.truncate(from);
                            let io = e.io_error().is_some();
                            let e = Arc::new(e);
                            if io {
                                for k in j..run.len() {
                                    failed.push((start + k, Arc::clone(&e)));
                                }
                                break;
                            }
                            failed.push((start + j, e));
                        }
                    }
                    j += 1;
                }
                if failed.len() == failed_before {
                    tracing::warn!(
                        "reading tree chunks of pack {pack} failed and succeeded on retry: {pack_error}"
                    );
                }
            }
        }
        start = end;
    }
    if failed.is_empty() {
        // The batch is sorted by pack.
        let mut packs: Vec<PackId> = batch.iter().map(|(_, loc)| loc.1).collect();
        packs.dedup();
        let packs: Vec<String> = packs.iter().map(PackId::to_string).collect();
        tracing::warn!(
            "a read of tree chunks failed and succeeded on retry ({batch_error}); packs {}",
            packs.join(", ")
        );
    }
    Ok(())
}

/// Run `decode` over `spans` of `buf` on up to `threads` scoped threads.
/// Results come back sorted by batch position. Workers stop claiming chunks
/// once `cancel` is cancelled.
fn decode_spans<T, D>(
    batch: &[Located],
    buf: &[u8],
    spans: &[(usize, Range<usize>)],
    threads: usize,
    cancel: &CancelToken,
    decode: &D,
) -> Vec<(usize, T)>
where
    T: Send,
    D: Fn(usize, &[u8]) -> T + Sync,
{
    let run = |(pos, range): &(usize, Range<usize>)| {
        let slot = batch[*pos].0;
        (*pos, decode(slot, &buf[range.clone()]))
    };
    let threads = threads.min(spans.len());
    let mut results: Vec<(usize, T)> = if threads <= 1 {
        let mut out = Vec::with_capacity(spans.len());
        for span in spans {
            if cancel.is_cancelled() {
                break;
            }
            out.push(run(span));
        }
        out
    } else {
        let next = AtomicUsize::new(0);
        let parts: Vec<Vec<(usize, T)>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    s.spawn(|| {
                        let mut local = Vec::new();
                        loop {
                            if cancel.is_cancelled() {
                                break;
                            }
                            let Some(span) = spans.get(next.fetch_add(1, Ordering::Relaxed)) else {
                                break;
                            };
                            local.push(run(span));
                        }
                        local
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
                .collect()
        });
        parts.into_iter().flatten().collect()
    };
    results.sort_unstable_by_key(|(pos, _)| *pos);
    results
}

/// Read every chunk of `set` once from `source` and run `decode` on its
/// plaintext on up to `decode_threads` threads; `collect` gets each chunk's
/// outcome on the caller's thread, batch by batch in (pack, offset) order.
/// Resolution through `repo.chunk_index()` happens before any read: with
/// [`OnChunkError::Abort`] the first unresolved chunk in set order fails
/// the walk with [`VykarError::ChunkNotInIndex`]; with
/// [`OnChunkError::Record`] unresolved chunks are handed to `collect` as
/// [`ChunkOutcome::NotInIndex`] first, in set order, and the chunks of a
/// batch whose read fails are re-read to attribute the failure.
#[allow(clippy::too_many_arguments)]
pub(crate) fn walk_tree_chunks<T, D, C>(
    repo: &Repo,
    set: &TreeChunkSet,
    source: TreeChunkSource,
    on_error: OnChunkError,
    decode_threads: usize,
    cancel: &CancelToken,
    decode: D,
    mut collect: C,
) -> Result<WalkStats>
where
    T: Send,
    D: Fn(usize, &[u8]) -> T + Sync,
    C: FnMut(usize, ChunkOutcome<T>) -> Result<()>,
{
    cancel.check()?;
    let decode_threads = decode_threads.max(1);
    let mut stats = WalkStats {
        decode_threads,
        ..WalkStats::default()
    };
    if set.is_empty() {
        return Ok(stats);
    }

    let index = repo.chunk_index();
    let mut located: Vec<Located> = Vec::with_capacity(set.len());
    for k in 0..set.len() {
        let id = set.id(k);
        match index.get(id) {
            Some(e) => located.push((k, (*id, e.pack_id, e.pack_offset, e.stored_size))),
            None => match on_error {
                OnChunkError::Abort => return Err(VykarError::ChunkNotInIndex(*id)),
                OnChunkError::Record => collect(k, ChunkOutcome::NotInIndex)?,
            },
        }
    }
    located.sort_unstable_by(|a, b| {
        a.1.1
            .as_bytes()
            .cmp(b.1.1.as_bytes())
            .then(a.1.2.cmp(&b.1.2))
    });

    let mut buf: Vec<u8> = Vec::new();
    let mut ends: Vec<usize> = Vec::new();
    let mut spans: Vec<(usize, Range<usize>)> = Vec::new();
    let mut failed: Vec<(usize, Arc<VykarError>)> = Vec::new();
    for batch in cut_batches(&located) {
        cancel.check()?;
        buf.clear();
        spans.clear();
        failed.clear();
        let locs: Vec<_> = batch.iter().map(|(_, loc)| *loc).collect();
        let read_started = Instant::now();
        match repo.read_tree_chunks_split(&locs, &mut buf, &mut ends, cancel, source) {
            Ok(()) => push_spans(&mut spans, 0, 0, &ends, batch.len(), buf.len())?,
            Err(e @ VykarError::Interrupted) => return Err(e),
            Err(e) if on_error == OnChunkError::Abort => return Err(e),
            Err(e) => {
                buf.clear();
                reread_failed_batch(
                    repo,
                    batch,
                    &e,
                    source,
                    cancel,
                    &mut buf,
                    &mut spans,
                    &mut failed,
                )?;
            }
        }
        stats.read_time += read_started.elapsed();
        stats.batches += 1;
        stats.chunks += batch.len();
        stats.stored_bytes += batch.iter().map(|(_, loc)| u64::from(loc.3)).sum::<u64>();
        stats.plaintext_bytes += buf.len() as u64;

        let decode_started = Instant::now();
        let decoded = decode_spans(batch, &buf, &spans, decode_threads, cancel, &decode);
        stats.decode_time += decode_started.elapsed();
        cancel.check()?;

        // Hand every outcome to `collect` in batch order.
        let outcomes = batch_outcomes(batch.len(), decoded, failed.drain(..))?;
        for (&(slot, _), outcome) in batch.iter().zip(outcomes) {
            collect(slot, outcome)?;
        }
    }
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Mark
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MissingRefs {
    /// A data-chunk reference with no index entry fails the mark.
    Abort,
    /// Such a reference is skipped (the repair sweep's rule).
    Ignore,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MarkOptions {
    /// Also count references with snapshot multiplicity (reference
    /// multiplicity, for the sweep record's referenced bytes).
    pub(crate) count: bool,
    pub(crate) missing_refs: MissingRefs,
    /// Decode threads ([`default_decode_threads`] for the callers here).
    pub(crate) decode_threads: usize,
}

/// The result of a mark over one index state.
pub(crate) struct Mark {
    bits: Vec<u64>,
    counts: Option<Vec<u32>>,
    pub(crate) items: u64,
    pub(crate) references: u64,
    pub(crate) walk: WalkStats,
}

impl Mark {
    pub(crate) fn is_marked(&self, position: u32) -> bool {
        self.bits
            .get(position as usize / 64)
            .is_some_and(|w| w & (1 << (position % 64)) != 0)
    }

    /// Counted references with snapshot multiplicity (`count: true` only).
    pub(crate) fn count(&self, position: u32) -> Option<u32> {
        self.counts
            .as_ref()
            .map(|c| c.get(position as usize).copied().unwrap_or(0))
    }
}

/// What decoding one tree chunk found.
struct ChunkTally {
    items: u64,
    references: u64,
    missing: Option<ChunkId>,
    error: Option<VykarError>,
}

fn set_bit(bits: &[AtomicU64], position: u32) {
    if let Some(w) = bits.get(position as usize / 64) {
        w.fetch_or(1 << (position % 64), Ordering::Relaxed);
    }
}

fn add_count(counts: Option<&[AtomicU32]>, position: u32, n: u32) {
    if let Some(c) = counts.and_then(|c| c.get(position as usize)) {
        c.fetch_add(n, Ordering::Relaxed);
    }
}

/// Mark every index entry the snapshots reference, failing closed on any
/// unreadable or unresolvable tree chunk and on a tree chunk that does not
/// decode alone. `names[s]` names snapshot `s` of `set` in messages.
///
/// Every chunk reference of every item is marked, whatever the item type.
/// Counting (reference multiplicity, for the sweep record's referenced
/// bytes): each tree chunk counts its occurrences in `item_ptrs`, and each
/// `RegularFile` chunk reference counts as often as its tree chunk occurs.
pub(crate) fn mark_live(
    repo: &Repo,
    set: &TreeChunkSet,
    names: &[String],
    positions: &IndexPositions<'_>,
    source: TreeChunkSource,
    opts: MarkOptions,
    cancel: &CancelToken,
) -> Result<Mark> {
    let started = Instant::now();
    let name = |k: usize| {
        names
            .get(set.first_snapshot(k))
            .map_or("?", String::as_str)
            .to_string()
    };
    let bits: Vec<AtomicU64> = (0..positions.len().div_ceil(64))
        .map(|_| AtomicU64::new(0))
        .collect();
    let counts: Option<Vec<AtomicU32>> = opts
        .count
        .then(|| (0..positions.len()).map(|_| AtomicU32::new(0)).collect());

    // The tree chunks themselves.
    for k in 0..set.len() {
        let id = set.id(k);
        let Some(p) = positions.find(id) else {
            return Err(VykarError::LiveChunkNotInIndex {
                snapshot: name(k),
                chunk: *id,
                tree: true,
            });
        };
        set_bit(&bits, p);
        add_count(counts.as_deref(), p, set.occurrences(k));
    }

    let decode = |k: usize, plain: &[u8]| {
        let m = set.occurrences(k);
        let mut tally = ChunkTally {
            items: 0,
            references: 0,
            missing: None,
            error: None,
        };
        let mut missing = None;
        let result = for_each_decoded_item(plain, |item| {
            if cancel.is_cancelled() {
                return Err(VykarError::Interrupted);
            }
            tally.items += 1;
            let counted = item.entry_type == ItemType::RegularFile;
            for r in &item.chunks {
                tally.references += 1;
                match positions.find(&r.id) {
                    Some(p) => {
                        set_bit(&bits, p);
                        if counted {
                            add_count(counts.as_deref(), p, m);
                        }
                    }
                    None => {
                        if opts.missing_refs == MissingRefs::Abort {
                            missing = Some(r.id);
                            return Err(VykarError::ChunkNotInIndex(r.id));
                        }
                    }
                }
            }
            Ok(())
        });
        tally.missing = missing;
        if let Err(e) = result
            && tally.missing.is_none()
        {
            tally.error = Some(e);
        }
        tally
    };

    let mut items = 0u64;
    let mut references = 0u64;
    let mut decoded = 0usize;
    let mut collect_failed = false;
    let walked = walk_tree_chunks(
        repo,
        set,
        source,
        OnChunkError::Abort,
        opts.decode_threads,
        cancel,
        decode,
        |k, outcome| {
            let ChunkOutcome::Decoded(tally) = outcome else {
                // Abort mode hands `collect` decoded chunks only.
                collect_failed = true;
                return Err(VykarError::Other(format!(
                    "internal error: tree chunk {} not decoded",
                    set.id(k)
                )));
            };
            decoded += 1;
            items += tally.items;
            references += tally.references;
            if let Some(id) = tally.missing {
                collect_failed = true;
                return Err(VykarError::LiveChunkNotInIndex {
                    snapshot: name(k),
                    chunk: id,
                    tree: false,
                });
            }
            if let Some(e) = tally.error {
                if matches!(e, VykarError::Interrupted) {
                    return Err(e);
                }
                collect_failed = true;
                return Err(VykarError::Other(format!(
                    "tree chunk {} of snapshot '{}' does not decode: {e}",
                    set.id(k),
                    name(k)
                )));
            }
            Ok(())
        },
    );
    let walk = match walked {
        Ok(walk) => walk,
        Err(e) if collect_failed || passes_through(&e) => return Err(e),
        Err(e) => return Err(VykarError::Other(format!("reading tree chunks: {e}"))),
    };
    // The mark is only as complete as the walk: every tree chunk decoded.
    if decoded != set.len() {
        return Err(VykarError::Other(format!(
            "internal error: {decoded} of {} tree chunks decoded",
            set.len()
        )));
    }

    let mark = Mark {
        bits: bits.into_iter().map(AtomicU64::into_inner).collect(),
        counts: counts.map(|c| c.into_iter().map(AtomicU32::into_inner).collect()),
        items,
        references,
        walk,
    };
    tracing::debug!(
        "mark: {} snapshots, {} distinct tree chunks ({} B stored, {} B plaintext) in {} batches; {} items, {} refs; read {:?}, decode {:?} on {} threads, total {:?}",
        names.len(),
        set.len(),
        walk.stored_bytes,
        walk.plaintext_bytes,
        walk.batches,
        mark.items,
        mark.references,
        walk.read_time,
        walk.decode_time,
        walk.decode_threads,
        started.elapsed()
    );
    Ok(mark)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(slot: usize, pack: u8, offset: u64, size: u32) -> Located {
        (
            slot,
            (
                ChunkId::from_bytes([slot as u8; 32]),
                PackId::from_bytes([pack; 32]),
                offset,
                size,
            ),
        )
    }

    fn lens(batches: &[&[Located]]) -> Vec<usize> {
        batches.iter().map(|b| b.len()).collect()
    }

    #[test]
    fn cut_batches_bounds_chunks_and_stored_bytes() {
        assert!(cut_batches(&[]).is_empty());

        // By chunk count.
        let many: Vec<Located> = (0..2 * MARK_BATCH_CHUNKS + 1)
            .map(|i| loc(i, 1, i as u64 * 10, 10))
            .collect();
        let batches = cut_batches(&many);
        assert_eq!(lens(&batches), [MARK_BATCH_CHUNKS, MARK_BATCH_CHUNKS, 1]);
        assert_eq!(batches.concat(), many);

        // By stored bytes: a batch holds exactly the limit, never more.
        let quarter = u32::try_from(MARK_BATCH_STORED_BYTES / 4).unwrap();
        let big: Vec<Located> = (0..9).map(|i| loc(i, 1, 0, quarter)).collect();
        assert_eq!(lens(&cut_batches(&big)), [4, 4, 1]);
        let over: Vec<Located> = (0..3).map(|i| loc(i, 1, 0, quarter + 1)).collect();
        assert_eq!(lens(&cut_batches(&over)), [3]);
        let over: Vec<Located> = (0..4).map(|i| loc(i, 1, 0, quarter + 1)).collect();
        assert_eq!(lens(&cut_batches(&over)), [3, 1]);

        // A chunk above the byte limit gets a batch of its own.
        let huge = u32::try_from(MARK_BATCH_STORED_BYTES + 1).unwrap();
        let mixed = vec![loc(0, 1, 0, 5), loc(1, 1, 5, huge), loc(2, 2, 0, 5)];
        assert_eq!(lens(&cut_batches(&mixed)), [1, 1, 1]);
        assert_eq!(lens(&cut_batches(&mixed[1..2])), [1]);
    }

    #[test]
    fn push_spans_checks_the_split() {
        let mut spans = Vec::new();
        push_spans(&mut spans, 3, 10, &[12, 12, 20], 3, 20).unwrap();
        assert_eq!(spans, [(3, 10..12), (4, 12..12), (5, 12..20)]);

        let bad = |ends: &[usize], expected: usize, buf_len: usize| {
            let err = push_spans(&mut Vec::new(), 0, 10, ends, expected, buf_len).unwrap_err();
            assert!(err.to_string().starts_with("internal error:"), "{err}");
        };
        bad(&[12, 20], 3, 20); // too few ends
        bad(&[12, 14, 16, 20], 3, 20); // too many
        bad(&[9], 1, 20); // before the start
        bad(&[14, 12], 2, 20); // decreasing
        bad(&[21], 1, 20); // past the buffer
    }

    #[test]
    fn batch_outcomes_require_one_outcome_per_position() {
        let err = || Arc::new(VykarError::Other("x".into()));
        let got = batch_outcomes(4, [(0, 'a'), (2, 'c')], [(1, err()), (3, err())]).unwrap();
        let kinds: Vec<Option<char>> = got
            .iter()
            .map(|o| match o {
                ChunkOutcome::Decoded(c) => Some(*c),
                ChunkOutcome::ReadFailed(_) => None,
                ChunkOutcome::NotInIndex => panic!("not produced here"),
            })
            .collect();
        assert_eq!(kinds, [Some('a'), None, Some('c'), None]);
        assert!(batch_outcomes::<char>(0, [], []).unwrap().is_empty());

        let internal = |r: Result<Vec<ChunkOutcome<char>>>| {
            let e = r.err().expect("an internal error");
            assert!(e.to_string().starts_with("internal error:"), "{e}");
        };
        internal(batch_outcomes(3, [(0, 'a'), (2, 'c')], [])); // position 1 missing
        internal(batch_outcomes(2, [(0, 'a'), (1, 'b')], [(1, err())])); // both
        internal(batch_outcomes(1, [(0, 'a'), (1, 'b')], [])); // outside
        internal(batch_outcomes(2, [(0, 'a'), (0, 'a'), (1, 'b')], [])); // twice
    }
}
