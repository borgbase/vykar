//! Phase 4: parallel execution — partition read groups across worker threads,
//! download ranges, decrypt, decompress, and write to target files with
//! syscall-coalesced batching and an LRU file-handle cache.

use std::collections::HashMap;
#[cfg(not(unix))]
use std::io::{Seek, Write as IoWrite};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::commands::util::check_interrupted;
use crate::compress;
use crate::repo::format::{unpack_object_expect_with_context_into, verify_chunk_id, ObjectType};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkHasher;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::plan::{PlannedFile, WriteTarget};
use super::read_groups::{PlannedBlob, ReadGroup};
use super::{MAX_OPEN_FILES_PER_GROUP, MAX_WRITE_BATCH};

/// Relaxed poll of the external cancellation flag. Kept separate from the
/// local `cancelled` flag, which is the failure-propagation channel: this one
/// is raised by the user (Ctrl-C, GUI Cancel) and is checked at the same two
/// points, so cancellation granularity is one storage request.
fn is_shutting_down(shutdown: Option<&AtomicBool>) -> bool {
    shutdown.is_some_and(|f| f.load(Ordering::Relaxed))
}

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

/// Continue-mode sink: file index → first reason it is damaged. Touched only
/// on the failure path.
#[derive(Default)]
pub(super) struct BatchFailures(Mutex<HashMap<usize, String>>);

impl BatchFailures {
    pub(super) fn record(&self, targets: &[WriteTarget], reason: &VykarError) {
        let reason = reason.to_string();
        let mut map = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for t in targets {
            map.entry(t.file_idx).or_insert_with(|| reason.clone());
        }
    }

    pub(super) fn into_map(self) -> HashMap<usize, String> {
        self.0
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
// Write accumulator
// ---------------------------------------------------------------------------

/// Groups the write accumulator state (buffer, target file, start offset)
/// into a single struct.  One instance is created per `process_read_group`
/// call; its allocation is reused across all sequences within that group.
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
        .min_by_key(|(_, &b)| b)
        .map_or(0, |(i, _)| i)
}

/// Run every read group across a scoped worker pool.
///
/// With `failures` set (continue mode), source-side failures are recorded
/// against the files they affect and the pass carries on; without it the first
/// one stops the pass as `ReadError::Source`.
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_parallel_restore(
    files: &[PlannedFile],
    groups: Vec<ReadGroup>,
    storage: &Arc<dyn StorageBackend>,
    crypto: &dyn CryptoEngine,
    root: &Path,
    restore_concurrency: usize,
    verify_chunks: bool,
    shutdown: Option<&AtomicBool>,
    failures: Option<&BatchFailures>,
) -> std::result::Result<u64, ReadError> {
    check_interrupted(shutdown)?;
    if groups.is_empty() {
        return Ok(0);
    }

    let num_threads = restore_concurrency.min(groups.len());
    let buckets = partition_groups(groups, num_threads);

    let bytes_written = AtomicU64::new(0);
    let cancelled = AtomicBool::new(false);
    let chunk_hasher = crypto.chunk_hasher();

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(buckets.len());

        for bucket in &buckets {
            let bytes_written = &bytes_written;
            let cancelled = &cancelled;

            handles.push(s.spawn(move || -> std::result::Result<(), ReadError> {
                let mut data_buf = Vec::new();
                let mut decrypt_buf = Vec::new();
                let mut decompress_buf = Vec::new();

                for group in bucket {
                    if cancelled.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    if let Err(e) = process_read_group(
                        group,
                        files,
                        storage,
                        crypto,
                        bytes_written,
                        cancelled,
                        &mut data_buf,
                        &mut decrypt_buf,
                        &mut decompress_buf,
                        root,
                        verify_chunks,
                        chunk_hasher,
                        shutdown,
                        failures,
                    ) {
                        cancelled.store(true, Ordering::Release);
                        return Err(e);
                    }

                    // Cap retained buffer capacity to avoid permanent high-water
                    // marks from outlier-large groups/blobs (~50 MiB RSS savings).
                    const BUF_KEEP_CAP: usize = 2 * 1024 * 1024;
                    for buf in [&mut data_buf, &mut decrypt_buf, &mut decompress_buf] {
                        buf.clear();
                        if buf.capacity() > BUF_KEEP_CAP {
                            buf.shrink_to(BUF_KEEP_CAP);
                        }
                    }
                }
                Ok(())
            }));
        }

        join_workers(handles, &cancelled, "restore worker panicked")?;
        // Workers return `Ok(())` when they stop early, so a cancellation would
        // otherwise surface as a short but successful byte count — and the
        // caller would go on to publish a partial tree.
        check_interrupted(shutdown)?;
        Ok(bytes_written.load(Ordering::Relaxed))
    })
}

/// Per-pass state shared by every blob a worker decodes and writes.
struct BlobCtx<'a> {
    files: &'a [PlannedFile],
    crypto: &'a dyn CryptoEngine,
    bytes_written: &'a AtomicU64,
    cancelled: &'a AtomicBool,
    root: &'a Path,
    verify_chunks: bool,
    chunk_hasher: &'a ChunkHasher,
    shutdown: Option<&'a AtomicBool>,
    failures: Option<&'a BatchFailures>,
}

impl BlobCtx<'_> {
    fn stopping(&self) -> bool {
        self.cancelled.load(Ordering::Acquire) || is_shutting_down(self.shutdown)
    }

    /// Record a source-side failure for `targets` in continue mode, or turn it
    /// into the error that stops the pass in strict mode.
    fn source_failure(
        &self,
        targets: &[WriteTarget],
        e: VykarError,
    ) -> std::result::Result<(), ReadError> {
        match (self.failures, ReadError::source(e)) {
            (Some(f), ReadError::Source(e)) => {
                f.record(targets, &e);
                Ok(())
            }
            (_, err) => Err(err),
        }
    }
}

/// Process a single coalesced read group: download the range, decrypt and
/// decompress each blob, then write the plaintext data to target files.
///
/// `data_buf`, `decrypt_buf`, and `decompress_buf` are caller-owned scratch
/// buffers reused across groups within the same thread, reducing per-group
/// allocation churn. The caller caps retained capacity at 2 MiB between groups.
///
/// In continue mode a failed group read is retried blob by blob: a short read
/// is an `Err` by the storage contract, so otherwise one truncated pack would
/// take down every blob of a 16 MiB coalesced group.
#[allow(clippy::too_many_arguments)]
fn process_read_group(
    group: &ReadGroup,
    files: &[PlannedFile],
    storage: &Arc<dyn StorageBackend>,
    crypto: &dyn CryptoEngine,
    bytes_written: &AtomicU64,
    cancelled: &AtomicBool,
    data_buf: &mut Vec<u8>,
    decrypt_buf: &mut Vec<u8>,
    decompress_buf: &mut Vec<u8>,
    root: &Path,
    verify_chunks: bool,
    chunk_hasher: &ChunkHasher,
    shutdown: Option<&AtomicBool>,
    failures: Option<&BatchFailures>,
) -> std::result::Result<(), ReadError> {
    let ctx = BlobCtx {
        files,
        crypto,
        bytes_written,
        cancelled,
        root,
        verify_chunks,
        chunk_hasher,
        shutdown,
        failures,
    };
    if ctx.stopping() {
        return Ok(());
    }

    let pack_key = group.pack_id.storage_key();
    let read_len = group.read_end - group.read_start;

    match storage.get_range_into(&pack_key, group.read_start, read_len, data_buf) {
        Ok(true) => process_blobs(
            &ctx,
            &group.pack_id,
            group.read_start,
            &group.blobs,
            data_buf,
            decrypt_buf,
            decompress_buf,
        ),
        Ok(false) => {
            if failures.is_none() {
                return Err(ReadError::source(pack_not_found(&group.pack_id)));
            }
            for blob in &group.blobs {
                ctx.source_failure(&blob.targets, pack_not_found(&group.pack_id))?;
            }
            Ok(())
        }
        Err(e) => {
            if failures.is_none() || matches!(e, VykarError::Interrupted) {
                return Err(ReadError::source(e));
            }
            if let [blob] = &group.blobs[..] {
                return ctx.source_failure(&blob.targets, e);
            }
            // Blobs are sorted by offset, so a truncated pack fails on every
            // blob from the cut onwards; stopping after a short run of
            // failures keeps that salvage while bounding a backend outage to
            // 1 + MAX_CONSECUTIVE_BLOB_READ_ERRORS requests per group.
            let mut consecutive = 0;
            for (i, blob) in group.blobs.iter().enumerate() {
                if ctx.stopping() {
                    return Ok(());
                }
                let err = match storage.get_range_into(
                    &pack_key,
                    blob.pack_offset,
                    u64::from(blob.stored_size),
                    data_buf,
                ) {
                    Ok(true) => {
                        consecutive = 0;
                        process_blobs(
                            &ctx,
                            &group.pack_id,
                            blob.pack_offset,
                            std::slice::from_ref(blob),
                            data_buf,
                            decrypt_buf,
                            decompress_buf,
                        )?;
                        continue;
                    }
                    Ok(false) => pack_not_found(&group.pack_id),
                    Err(e) => e,
                };
                consecutive += 1;
                if consecutive < MAX_CONSECUTIVE_BLOB_READ_ERRORS {
                    ctx.source_failure(&blob.targets, err)?;
                    continue;
                }
                let msg = format!(
                    "{err} (rest of the read group not retried after \
                     {consecutive} consecutive failures)"
                );
                ctx.source_failure(&blob.targets, err)?;
                for rest in group.blobs.get(i + 1..).unwrap_or_default() {
                    ctx.source_failure(&rest.targets, VykarError::Other(msg.clone()))?;
                }
                return Ok(());
            }
            Ok(())
        }
    }
}

/// Per-blob retries of a failed group read stop after this many consecutive
/// storage failures; the remaining blobs of the group are marked damaged.
const MAX_CONSECUTIVE_BLOB_READ_ERRORS: usize = 3;

fn pack_not_found(pack_id: &PackId) -> VykarError {
    VykarError::Other(format!("pack not found: {pack_id}"))
}

/// Decode and write `blobs`, all located in `data`, which holds the pack bytes
/// starting at `read_start`.
fn process_blobs(
    ctx: &BlobCtx<'_>,
    pack_id: &PackId,
    read_start: u64,
    blobs: &[PlannedBlob],
    data: &[u8],
    decrypt_buf: &mut Vec<u8>,
    decompress_buf: &mut Vec<u8>,
) -> std::result::Result<(), ReadError> {
    let (files, bytes_written, root) = (ctx.files, ctx.bytes_written, ctx.root);
    let mut file_handles = LruHandles::new();

    // Write accumulator — batches consecutive same-file writes into one syscall.
    // Pre-allocated to MAX_WRITE_BATCH; reused across sequences within this group.
    let mut pw = PendingWrite::new();

    for blob in blobs {
        if ctx.stopping() {
            // Dropping `pw` loses buffered data, but the caller will
            // clean up the temp restore root on error/cancellation so
            // no partial files reach the final destination.
            return Ok(());
        }

        if let Err(e) = decode_blob(
            ctx,
            pack_id,
            read_start,
            blob,
            data,
            decrypt_buf,
            decompress_buf,
        ) {
            ctx.source_failure(&blob.targets, e)?;
            continue;
        }

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
                        &mut file_handles,
                        files,
                        bytes_written,
                        root,
                    )?;
                    pw.reset();
                }
            } else {
                // Flush any active pending write first.
                flush_pending(&mut pw, &mut file_handles, files, bytes_written, root)?;

                if contiguous || decompress_buf.len() >= MAX_WRITE_BATCH {
                    // Contiguous overflow or standalone large chunk — write
                    // directly from decompress_buf so no data lingers in the
                    // accumulator (same cancellation safety as the old code).
                    write_buf(
                        decompress_buf,
                        target.file_idx,
                        target.file_offset,
                        &mut file_handles,
                        files,
                        bytes_written,
                        root,
                    )?;
                } else {
                    // Start new accumulation sequence (reuses existing allocation).
                    pw.rebind(target.file_idx, target.file_offset);
                    pw.buf.extend_from_slice(decompress_buf);
                }
            }
        }
    }

    // Flush any remaining pending write.
    flush_pending(&mut pw, &mut file_handles, files, bytes_written, root)?;

    Ok(())
}

/// Unpack, decompress and verify one blob into `decompress_buf`. Every error
/// here is source-side.
fn decode_blob(
    ctx: &BlobCtx<'_>,
    pack_id: &PackId,
    read_start: u64,
    blob: &PlannedBlob,
    data: &[u8],
    decrypt_buf: &mut Vec<u8>,
    decompress_buf: &mut Vec<u8>,
) -> Result<()> {
    let local_offset = (blob.pack_offset - read_start) as usize;
    let local_end = local_offset + blob.stored_size as usize;

    let raw = data.get(local_offset..local_end).ok_or_else(|| {
        VykarError::Other(format!(
            "blob extends beyond downloaded range in pack {pack_id}"
        ))
    })?;
    unpack_object_expect_with_context_into(
        raw,
        ObjectType::ChunkData,
        blob.chunk_id.as_bytes(),
        ctx.crypto,
        decrypt_buf,
    )
    .map_err(|e| {
        VykarError::Other(format!(
            "chunk {} in pack {} (offset {}, size {}): {e}",
            blob.chunk_id, pack_id, blob.pack_offset, blob.stored_size
        ))
    })?;
    compress::decompress_into_with_hint(
        decrypt_buf,
        Some(blob.expected_size as usize),
        decompress_buf,
    )
    .map_err(|e| {
        VykarError::Other(format!(
            "chunk {} in pack {} (offset {}, size {}): {e}",
            blob.chunk_id, pack_id, blob.pack_offset, blob.stored_size
        ))
    })?;

    if decompress_buf.len() != blob.expected_size as usize {
        return Err(VykarError::InvalidFormat(format!(
            "chunk {} in pack {} (offset {}, size {}) size mismatch after restore decode: \
             expected {} bytes, got {} bytes",
            blob.chunk_id,
            pack_id,
            blob.pack_offset,
            blob.stored_size,
            blob.expected_size,
            decompress_buf.len()
        )));
    }

    // Without AEAD, the chunk ID is the only checksum on chunk data.
    if ctx.verify_chunks || !ctx.crypto.is_encrypting() {
        verify_chunk_id(
            ctx.chunk_hasher,
            &blob.chunk_id,
            decompress_buf,
            pack_id,
            blob.pack_offset,
            blob.stored_size,
        )?;
    }
    Ok(())
}

/// Write `buf` to the target file at `offset`.  Opens the (already staged)
/// file on first access.  Increments `bytes_written`.
fn write_buf(
    buf: &[u8],
    file_idx: usize,
    offset: u64,
    file_handles: &mut LruHandles,
    files: &[PlannedFile],
    bytes_written: &AtomicU64,
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(libc::O_NOFOLLOW);
        }
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
    #[cfg(unix)]
    fh.write_all_at(buf, offset)?;
    #[cfg(not(unix))]
    {
        fh.seek(std::io::SeekFrom::Start(offset))?;
        fh.write_all(buf)?;
    }
    bytes_written.fetch_add(buf.len() as u64, Ordering::Relaxed);
    Ok(())
}

/// Flush the write accumulator to disk if it contains data, then reset it.
fn flush_pending(
    pw: &mut PendingWrite,
    file_handles: &mut LruHandles,
    files: &[PlannedFile],
    bytes_written: &AtomicU64,
    root: &Path,
) -> Result<()> {
    if pw.is_active() && !pw.buf.is_empty() {
        write_buf(
            &pw.buf,
            pw.file_idx,
            pw.start,
            file_handles,
            files,
            bytes_written,
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
    use crate::repo::format::pack_object_with_context;
    use crate::testutil::{test_chunk_hasher, MemoryBackend};
    use smallvec::SmallVec;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tempfile::tempdir;
    use vykar_crypto::aes_gcm::Aes256GcmEngine;
    use vykar_crypto::PlaintextEngine;
    use vykar_types::chunk_id::ChunkId;

    #[test]
    fn process_read_group_rejects_decode_size_mismatch() {
        let temp = tempdir().unwrap();
        let out = temp.path().join("out.bin");
        let file = std::fs::File::create(&out).unwrap();
        file.set_len(5).unwrap();

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("out.bin"),
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

        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        let err = process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
            false,
            &test_chunk_hasher(),
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
            rel_path: PathBuf::from("big.bin"),
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
        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
            false,
            &test_chunk_hasher(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(bytes_written.load(Ordering::Relaxed), big_data.len() as u64);
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
            rel_path: PathBuf::from("exact.bin"),
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
        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
            false,
            &test_chunk_hasher(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(bytes_written.load(Ordering::Relaxed), total_size);
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
                rel_path: PathBuf::from("a.bin"),
                total_size: data_a.len() as u64,
                mode: 0o644,
                mtime: 0,
                uid: 0,
                gid: 0,
                xattrs: None,
            },
            PlannedFile {
                rel_path: PathBuf::from("b.bin"),
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
        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
            false,
            &test_chunk_hasher(),
            None,
            None,
        )
        .unwrap();

        let total = (data_a.len() + data_b.len()) as u64;
        assert_eq!(bytes_written.load(Ordering::Relaxed), total);
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
            rel_path: PathBuf::from("batched.bin"),
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
        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
            false,
            &test_chunk_hasher(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(
            bytes_written.load(Ordering::Relaxed),
            expected_data.len() as u64
        );
        assert_eq!(std::fs::read(&out).unwrap(), expected_data);
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
            rel_path: PathBuf::from("good.bin"),
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
        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
            true,
            &test_chunk_hasher(),
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
        let bytes_written = AtomicU64::new(0);
        process_read_group(
            group,
            files,
            storage,
            crypto,
            &bytes_written,
            &AtomicBool::new(false),
            &mut Vec::new(),
            &mut Vec::new(),
            &mut Vec::new(),
            root,
            verify_chunks,
            crypto.chunk_hasher(),
            None,
            None,
        )
        .map_err(ReadError::into_inner)?;
        Ok(bytes_written.load(Ordering::Relaxed))
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
            rel_path: PathBuf::from("bad.bin"),
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
            matches!(&err, VykarError::InvalidFormat(m) if m.contains("hash mismatch")),
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
            rel_path: PathBuf::from("bad.bin"),
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
            matches!(&err, VykarError::InvalidFormat(m) if m.contains("hash mismatch")),
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
            rel_path: PathBuf::from("x"),
            total_size: 4,
            mode: 0o644,
            mtime: 0,
            uid: 0,
            gid: 0,
            xattrs: None,
        }];
        let mut handles = LruHandles::new();
        let bytes_written = AtomicU64::new(0);
        let res = write_buf(b"evil", 0, 0, &mut handles, &files, &bytes_written, &root);
        assert!(res.is_err(), "write through a symlink leaf must fail");
        assert_eq!(std::fs::read(&victim).unwrap(), b"untouched");
        assert_eq!(bytes_written.load(Ordering::Relaxed), 0);
    }

    /// Backend that raises a shared cancellation flag from inside the range read
    /// the restore pool uses, simulating a signal that lands once workers are
    /// already running.
    struct CancelOnReadBackend {
        inner: MemoryBackend,
        flag: Arc<AtomicBool>,
    }

    impl vykar_storage::InnerBackend for CancelOnReadBackend {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for CancelOnReadBackend;
        except [get_range_into];

        fn get_range_into(
            &self,
            key: &str,
            offset: u64,
            length: u64,
            buf: &mut Vec<u8>,
        ) -> vykar_types::error::Result<bool> {
            self.flag.store(true, Ordering::Relaxed);
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
            rel_path: PathBuf::from("out.bin"),
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

        let shutdown = Arc::new(AtomicBool::new(false));
        let inner = MemoryBackend::new();
        inner.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(CancelOnReadBackend {
            inner,
            flag: Arc::clone(&shutdown),
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

        assert!(!shutdown.load(Ordering::Relaxed), "flag must start clear");
        let err = execute_parallel_restore(
            &files,
            groups,
            &storage,
            &crypto,
            temp.path(),
            2,
            false,
            Some(&shutdown),
            None,
        )
        .map_err(ReadError::into_inner)
        .unwrap_err();

        assert!(
            matches!(err, VykarError::Interrupted),
            "expected Interrupted, got: {err}"
        );
        assert!(
            shutdown.load(Ordering::Relaxed),
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
            rel_path: PathBuf::from("out.bin"),
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

        let shutdown = AtomicBool::new(true);
        let err = execute_parallel_restore(
            &files,
            groups,
            &storage,
            &crypto,
            temp.path(),
            2,
            false,
            Some(&shutdown),
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
            rel_path: PathBuf::from(name),
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
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        process_read_group(
            group,
            files,
            storage,
            &crypto,
            &AtomicU64::new(0),
            &AtomicBool::new(false),
            &mut Vec::new(),
            &mut Vec::new(),
            &mut Vec::new(),
            root,
            false,
            crypto.chunk_hasher(),
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
            matches!(&err, ReadError::Source(VykarError::InvalidFormat(m)) if m.contains("hash mismatch")),
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
        except [get_range_into];

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
    /// as the fatal one, in either join order.
    #[test]
    fn concurrent_source_and_fatal_failure_reports_fatal() {
        for swap in [false, true] {
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
                None,
                None,
            )
            .unwrap_err();
            assert!(
                matches!(&err, ReadError::Fatal(e) if e.to_string().contains("panicked")),
                "swap={swap}: got {err:?}"
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
        run_group_with(&group, &files, &storage, temp.path(), Some(&failures)).unwrap();
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
}
