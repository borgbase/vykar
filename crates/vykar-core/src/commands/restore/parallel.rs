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
use std::sync::Arc;

use crate::compress;
use crate::repo::format::{unpack_object_expect_with_context_into, ObjectType};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::plan::PlannedFile;
use super::read_groups::ReadGroup;
use super::{MAX_OPEN_FILES_PER_GROUP, MAX_WRITE_BATCH};

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
            Some(&mut self.entries.last_mut().unwrap().1)
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

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_parallel_restore(
    files: &[PlannedFile],
    groups: Vec<ReadGroup>,
    storage: &Arc<dyn StorageBackend>,
    crypto: &dyn CryptoEngine,
    root: &Path,
    restore_concurrency: usize,
    verify_chunks: bool,
) -> Result<u64> {
    if groups.is_empty() {
        return Ok(0);
    }

    let num_threads = restore_concurrency.min(groups.len());
    let buckets = partition_groups(groups, num_threads);

    let bytes_written = AtomicU64::new(0);
    let cancelled = AtomicBool::new(false);
    let chunk_id_key = *crypto.chunk_id_key();

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(buckets.len());

        for bucket in &buckets {
            let bytes_written = &bytes_written;
            let cancelled = &cancelled;
            let chunk_id_key = &chunk_id_key;

            handles.push(s.spawn(move || -> Result<()> {
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
                        chunk_id_key,
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

        let mut first_error: Option<VykarError> = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    cancelled.store(true, Ordering::Release);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(_panic) => {
                    cancelled.store(true, Ordering::Release);
                    if first_error.is_none() {
                        first_error = Some(VykarError::Other("restore worker panicked".into()));
                    }
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(bytes_written.load(Ordering::Relaxed)),
        }
    })
}

/// Process a single coalesced read group: download the range, decrypt and
/// decompress each blob, then write the plaintext data to target files.
///
/// `data_buf`, `decrypt_buf`, and `decompress_buf` are caller-owned scratch
/// buffers reused across groups within the same thread, reducing per-group
/// allocation churn. The caller caps retained capacity at 2 MiB between groups.
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
    chunk_id_key: &[u8; 32],
) -> Result<()> {
    if cancelled.load(Ordering::Acquire) {
        return Ok(());
    }

    let pack_key = group.pack_id.storage_key();
    let read_len = group.read_end - group.read_start;

    if !storage.get_range_into(&pack_key, group.read_start, read_len, data_buf)? {
        return Err(VykarError::Other(format!(
            "pack not found: {}",
            group.pack_id
        )));
    }
    let data = &data_buf[..];

    let mut file_handles = LruHandles::new();

    // Write accumulator — batches consecutive same-file writes into one syscall.
    // Pre-allocated to MAX_WRITE_BATCH; reused across sequences within this group.
    let mut pw = PendingWrite::new();

    for blob in &group.blobs {
        if cancelled.load(Ordering::Acquire) {
            // Dropping `pw` loses buffered data, but the caller will
            // clean up the temp restore root on error/cancellation so
            // no partial files reach the final destination.
            return Ok(());
        }

        let local_offset = (blob.pack_offset - group.read_start) as usize;
        let local_end = local_offset + blob.stored_size as usize;

        if local_end > data.len() {
            return Err(VykarError::Other(format!(
                "blob extends beyond downloaded range in pack {}",
                group.pack_id
            )));
        }

        let raw = &data[local_offset..local_end];
        unpack_object_expect_with_context_into(
            raw,
            ObjectType::ChunkData,
            &blob.chunk_id.0,
            crypto,
            decrypt_buf,
        )
        .map_err(|e| {
            VykarError::Other(format!(
                "chunk {} in pack {} (offset {}, size {}): {e}",
                blob.chunk_id, group.pack_id, blob.pack_offset, blob.stored_size
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
                blob.chunk_id, group.pack_id, blob.pack_offset, blob.stored_size
            ))
        })?;

        if decompress_buf.len() != blob.expected_size as usize {
            return Err(VykarError::InvalidFormat(format!(
                "chunk {} in pack {} (offset {}, size {}) size mismatch after restore decode: \
                 expected {} bytes, got {} bytes",
                blob.chunk_id,
                group.pack_id,
                blob.pack_offset,
                blob.stored_size,
                blob.expected_size,
                decompress_buf.len()
            )));
        }

        if verify_chunks {
            let actual = ChunkId::compute(chunk_id_key, decompress_buf);
            if actual != blob.chunk_id {
                return Err(VykarError::InvalidFormat(format!(
                    "chunk {} in pack {} (offset {}, size {}) hash mismatch after decrypt: \
                     expected {}, got {}",
                    blob.chunk_id,
                    group.pack_id,
                    blob.pack_offset,
                    blob.stored_size,
                    blob.chunk_id,
                    actual,
                )));
            }
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

/// Write `buf` to the target file at `offset`.  Opens/creates the file handle
/// on first access and calls CAS `set_len`.  Increments `bytes_written`.
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
        let pf = &files[file_idx];
        let full_path = root.join(&pf.rel_path);
        // Create-on-first-write: truncate(false) prevents one worker from
        // destroying another worker's writes to the same file.
        let handle = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&full_path)
            .map_err(|e| {
                VykarError::Other(format!(
                    "failed to open {} for writing: {e}",
                    full_path.display()
                ))
            })?;
        // CAS: only the first opener calls set_len (ftruncate).
        if !pf.created.swap(true, Ordering::AcqRel) {
            handle.set_len(pf.total_size)?;
        }
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
        dummy_chunk_id, dummy_pack_id, multi_blob_group, pack_blob, single_blob_group,
    };
    use crate::compress::Compression;
    use crate::repo::format::pack_object_with_context;
    use crate::testutil::{test_chunk_id_key, MemoryBackend};
    use smallvec::SmallVec;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tempfile::tempdir;
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
            xattrs: None,
            created: AtomicBool::new(true), // pre-created in test
        }];

        let payload = b"abc";
        let compressed = crate::compress::compress(Compression::None, payload).unwrap();
        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            &dummy_chunk_id(0xAA).0,
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
            &test_chunk_id_key(),
        )
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
        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let cid = dummy_chunk_id(0x01);
        let packed = pack_blob(cid, &big_data, &crypto);

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("big.bin"),
            total_size: big_data.len() as u64,
            mode: 0o644,
            mtime: 0,
            xattrs: None,
            created: AtomicBool::new(false),
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
            &test_chunk_id_key(),
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

        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let cid_a = dummy_chunk_id(0x0A);
        let cid_b = dummy_chunk_id(0x0B);
        let packed_a = pack_blob(cid_a, &chunk_a, &crypto);
        let packed_b = pack_blob(cid_b, &chunk_b, &crypto);

        let total_size = (chunk_a.len() + chunk_b.len()) as u64;
        let files = vec![PlannedFile {
            rel_path: PathBuf::from("exact.bin"),
            total_size,
            mode: 0o644,
            mtime: 0,
            xattrs: None,
            created: AtomicBool::new(false),
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
            &test_chunk_id_key(),
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

        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let cid_a = dummy_chunk_id(0x0A);
        let cid_b = dummy_chunk_id(0x0B);
        let packed_a = pack_blob(cid_a, &data_a, &crypto);
        let packed_b = pack_blob(cid_b, &data_b, &crypto);

        let files = vec![
            PlannedFile {
                rel_path: PathBuf::from("a.bin"),
                total_size: data_a.len() as u64,
                mode: 0o644,
                mtime: 0,
                xattrs: None,
                created: AtomicBool::new(false),
            },
            PlannedFile {
                rel_path: PathBuf::from("b.bin"),
                total_size: data_b.len() as u64,
                mode: 0o644,
                mtime: 0,
                xattrs: None,
                created: AtomicBool::new(false),
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
            &test_chunk_id_key(),
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
        let crypto = PlaintextEngine::new(&test_chunk_id_key());

        #[allow(clippy::type_complexity)]
        let mut entries: Vec<(ChunkId, Vec<u8>, u32, SmallVec<[WriteTarget; 1]>)> = Vec::new();
        let mut expected_data = Vec::new();
        for i in 0u8..num_chunks {
            let data = vec![0x10 + i; chunk_size];
            expected_data.extend_from_slice(&data);
            let cid = dummy_chunk_id(0x10 + i);
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
            xattrs: None,
            created: AtomicBool::new(false),
        }];

        let pack_id = dummy_pack_id(4);
        let (pack_data, group) = multi_blob_group(pack_id, entries);

        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &pack_data).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

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
            &test_chunk_id_key(),
        )
        .unwrap();

        assert_eq!(
            bytes_written.load(Ordering::Relaxed),
            expected_data.len() as u64
        );
        assert_eq!(std::fs::read(&out).unwrap(), expected_data);
    }

    /// `--verify` recomputes the keyed chunk ID after decrypt/decompress and
    /// must accept blobs whose stored chunk_id matches the actual data.
    #[test]
    fn process_read_group_verify_accepts_matching_chunk_id() {
        let temp = tempdir().unwrap();
        let out = temp.path().join("good.bin");

        let payload = b"verify-good-data";
        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let cid = ChunkId::compute(&test_chunk_id_key(), payload);
        let packed = pack_blob(cid, payload, &crypto);

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("good.bin"),
            total_size: payload.len() as u64,
            mode: 0o644,
            mtime: 0,
            xattrs: None,
            created: AtomicBool::new(false),
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
            &test_chunk_id_key(),
        )
        .unwrap();

        assert_eq!(std::fs::read(&out).unwrap(), payload);
    }

    /// With `--verify`, a snapshot whose stored chunk_id does not match the
    /// actual chunk bytes must abort restore with `InvalidFormat`.
    /// AEAD's `chunk_id` AAD is *not* the same value as the snapshot-recorded
    /// chunk_id when those two diverge (writer-side bug), so we encrypt under
    /// the wrong-but-self-consistent AAD to bypass AEAD's check and exercise
    /// the verify-after-decrypt path.
    #[test]
    fn process_read_group_verify_rejects_chunk_id_mismatch() {
        let temp = tempdir().unwrap();
        let _out = temp.path().join("bad.bin");

        let payload = b"verify-bad-data";
        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let real_cid = ChunkId::compute(&test_chunk_id_key(), payload);
        let wrong_cid = dummy_chunk_id(0xEE);
        assert_ne!(real_cid, wrong_cid);
        // Encrypt under the wrong (snapshot-recorded) chunk_id as AAD so the
        // AEAD check still passes — it's the post-decrypt hash recompute that
        // must catch the divergence.
        let packed = pack_blob(wrong_cid, payload, &crypto);

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("bad.bin"),
            total_size: payload.len() as u64,
            mode: 0o644,
            mtime: 0,
            xattrs: None,
            created: AtomicBool::new(false),
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
            true,
            &test_chunk_id_key(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("hash mismatch"),
            "expected hash mismatch error, got: {err}"
        );

        // Defense-in-depth nature: the same blob restores fine when verify
        // is disabled, since AEAD already accepted it.
        let temp2 = tempdir().unwrap();
        let bytes_written = AtomicU64::new(0);
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
            temp2.path(),
            false,
            &test_chunk_id_key(),
        )
        .unwrap();
    }
}
