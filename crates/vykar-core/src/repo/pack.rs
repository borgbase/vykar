use std::collections::HashMap;
use std::time::Instant;

use vykar_storage::{StagedPut, StorageBackend};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::hash::{HashAlgorithm, Hasher256};
use vykar_types::pack_id::PackId;

use vykar_protocol::{validate_pack_header, PACK_HEADER_SIZE, PACK_MAGIC, PACK_VERSION_CURRENT};

/// Distinguishes data packs (file content) from tree packs (item-stream metadata).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackType {
    Data,
    Tree,
}

/// Lightweight metadata for a blob whose data lives in the pack buffer.
struct BlobMeta {
    chunk_id: ChunkId,
    stored_size: u32,
}

/// Maximum number of blobs in a single pack file.
/// Prevents pathological cases where many tiny chunks create a pack with a huge header.
pub const MAX_BLOBS_PER_PACK: usize = 10_000;

/// Maximum age of a pack writer before it should be flushed (in seconds).
/// Forces periodic flushes even if the pack isn't full, preventing stale data
/// from sitting in memory indefinitely during long backups.
pub const PACK_MAX_AGE_SECS: u64 = 300;

/// Tuple describing one chunk's location and refcount in a sealed/flushed pack.
pub type PackedChunkEntry = (ChunkId, u32, u64, u32);

/// Result of flushing a pack to storage: (pack_id, chunk entries).
pub type FlushedPackResult = (PackId, Vec<PackedChunkEntry>);

/// Storage directory key that staged packs are written under (the final key
/// is `packs/<shard>/<id>`, below it).
const PACK_STAGING_PREFIX: &str = "packs";

/// Where a pack writer's bytes go while the pack fills.
enum PackBuffer {
    /// Heap buffer, uploaded whole once sealed.
    Memory(Vec<u8>),
    /// Streamed to the backend as blobs arrive (local repositories), hashed
    /// on the way so the pack ID is known at seal without the bytes.
    Staged {
        object: Box<dyn StagedPut>,
        hasher: Hasher256,
    },
    /// A staged append failed. The partial pack was discarded, so blobs the
    /// writer already accepted (and handed out as dedup hits) can never be
    /// stored; every later `add_blob` and `seal` fails.
    Failed,
}

/// A sealed pack ready for upload. Destructurable so callers can split ownership
/// between the main thread (pack_id + entries) and upload thread (data).
pub struct SealedPack {
    pub pack_id: PackId,
    pub entries: Vec<PackedChunkEntry>,
    pub data: SealedData,
}

/// Sealed pack data ready for upload.
pub enum SealedData {
    /// Heap buffer holding the whole pack.
    Memory(Vec<u8>),
    /// Every byte already written to the backend's staging area; uploading
    /// is the backend's commit (fsync + rename for local repositories).
    Staged(Box<dyn StagedPut>),
}

// Compile-time assertion: SealedData must be Send for thread::spawn upload.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<SealedData>();
    }
};

impl SealedData {
    /// The whole pack, when it is heap-buffered (`None` once staged: those
    /// bytes are only in the backend's temp file).
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            SealedData::Memory(v) => Some(v),
            SealedData::Staged(_) => None,
        }
    }

    /// Heap bytes held by this sealed pack (buffer capacity, not length).
    /// A staged pack lives in the backend's staging file, not on the heap;
    /// its small write buffer is not counted.
    pub fn capacity(&self) -> usize {
        match self {
            SealedData::Memory(v) => v.capacity(),
            SealedData::Staged(_) => 0,
        }
    }

    /// Upload pack data: `put_owned` for a heap buffer (zero-copy for
    /// backends that support owned buffers), `commit` for a staged one.
    pub fn put_to(self, storage: &dyn StorageBackend, key: &str) -> Result<()> {
        match self {
            SealedData::Memory(v) => storage.put_owned(key, v),
            SealedData::Staged(object) => object.commit(key),
        }
    }
}

fn failed_writer_error() -> VykarError {
    VykarError::Other("pack writer is unusable after an earlier write failure".into())
}

/// A failed append to a staged pack. Reported as `Other`, not `Io`: an EIO or
/// permission error from the *repository* must not look like the per-file
/// source read errors that `is_soft_file_error` lets the backup skip.
fn staged_write_error(e: VykarError) -> VykarError {
    match e {
        VykarError::Io(io) => VykarError::Other(format!("writing pack to repository: {io}")),
        other => other,
    }
}

/// Accumulates encrypted blobs and flushes them as pack files.
///
/// By default the pack is assembled in a heap `Vec<u8>`. When the caller
/// passes a backend to [`Self::add_blob_to`] that can stage writes (local
/// repositories), the bytes are streamed to a temp file instead, so neither
/// the filling pack nor the sealed one waiting for its fsync occupies the
/// heap. That is plain `write(2)` from the caller's thread; an earlier
/// version backed data packs with a writable file mmap (`MmapMut`), which
/// caused `D`-state hangs and `balance_dirty_pages` stalls on some systems
/// (e.g. TrueNAS/musl) and was removed.
pub struct PackWriter {
    pack_type: PackType,
    target_size: usize,
    /// The repository's content-digest algorithm, used to name sealed packs.
    /// Bound at construction so a writer cannot produce a pack ID under an
    /// algorithm the repository does not use.
    hash_algo: HashAlgorithm,
    /// Pack bytes so far. None until first blob.
    buffer: Option<PackBuffer>,
    /// Lightweight metadata per blob (no data — data lives in the buffer).
    blob_meta: Vec<BlobMeta>,
    current_size: usize,
    /// chunk_id -> (stored_size, refcount) for pending (not-yet-flushed) blobs.
    pending: HashMap<ChunkId, (u32, u32)>,
    /// When the first blob was added to the current buffer.
    first_blob_time: Option<Instant>,
}

impl PackWriter {
    pub fn new(pack_type: PackType, target_size: usize, hash_algo: HashAlgorithm) -> Self {
        Self {
            pack_type,
            target_size,
            hash_algo,
            buffer: None,
            blob_meta: Vec::new(),
            current_size: 0,
            pending: HashMap::new(),
            first_blob_time: None,
        }
    }

    /// The content-digest algorithm this writer names its packs with.
    pub fn hash_algorithm(&self) -> HashAlgorithm {
        self.hash_algo
    }

    /// Initialize the pack buffer on first blob: staged in `stage` if given
    /// and able to, otherwise on the heap.
    fn init_buffer(&mut self, stage: Option<&dyn StorageBackend>) -> Result<()> {
        let header = [PACK_MAGIC.as_slice(), &[PACK_VERSION_CURRENT]].concat();
        if let Some(storage) = stage {
            if let Some(mut object) = storage
                .begin_staged_put(PACK_STAGING_PREFIX)
                .map_err(staged_write_error)?
            {
                object.append(&header).map_err(staged_write_error)?;
                let mut hasher = Hasher256::new(self.hash_algo);
                hasher.update(&header);
                self.buffer = Some(PackBuffer::Staged { object, hasher });
                return Ok(());
            }
        }
        let mut v = Vec::with_capacity(PACK_HEADER_SIZE + self.target_size.min(512 * 1024 * 1024));
        v.extend_from_slice(&header);
        self.buffer = Some(PackBuffer::Memory(v));
        Ok(())
    }

    /// Add an encrypted blob to a heap-buffered pack. Returns the offset
    /// within the pack where the blob data starts (after the 4-byte length
    /// prefix).
    pub fn add_blob(&mut self, chunk_id: ChunkId, encrypted_blob: Vec<u8>) -> Result<u64> {
        self.add_blob_to(None, chunk_id, encrypted_blob)
    }

    /// Like [`Self::add_blob`], but a pack started by this call is streamed
    /// to `stage` when it supports staged writes
    /// ([`StorageBackend::begin_staged_put`]). The choice is made per pack,
    /// at its first blob; the pack's bytes and ID are identical either way.
    ///
    /// A failed staged write leaves the writer unusable: the blobs it holds
    /// may already be referenced as dedup hits, so the error must end the
    /// backup rather than be skipped like a per-file error.
    pub fn add_blob_to(
        &mut self,
        stage: Option<&dyn StorageBackend>,
        chunk_id: ChunkId,
        encrypted_blob: Vec<u8>,
    ) -> Result<u64> {
        if matches!(self.buffer, Some(PackBuffer::Failed)) {
            return Err(failed_writer_error());
        }
        let blob_len = encrypted_blob.len() as u32;

        // On first blob: initialize the buffer.
        if self.blob_meta.is_empty() {
            self.init_buffer(stage)?;
        }

        // Offset accounts for: pack header + bytes already buffered + this blob's 4B len prefix.
        let offset = PACK_HEADER_SIZE as u64 + self.current_size as u64 + 4;
        let additional = 4 + encrypted_blob.len();

        // Append [4B length LE][encrypted_data] into the buffer.
        let len_prefix = blob_len.to_le_bytes();
        match self.buffer.as_mut().expect("buffer initialized above") {
            PackBuffer::Memory(v) => {
                // The blob crossing the target seals this pack. Reserve only
                // its actual bytes rather than letting Vec double a large
                // buffer that will immediately move to the upload queue.
                if self.current_size + additional >= self.target_size {
                    v.reserve_exact(additional);
                }
                v.extend_from_slice(&len_prefix);
                v.extend_from_slice(&encrypted_blob);
            }
            PackBuffer::Staged { object, hasher } => {
                let written = object
                    .append(&len_prefix)
                    .and_then(|()| object.append(&encrypted_blob));
                if let Err(e) = written {
                    // Drops the staged object, which removes its temp file.
                    self.buffer = Some(PackBuffer::Failed);
                    return Err(staged_write_error(e));
                }
                hasher.update(&len_prefix);
                hasher.update(&encrypted_blob);
            }
            // Rejected on entry; kept total rather than asserted.
            PackBuffer::Failed => return Err(failed_writer_error()),
        }

        self.current_size += additional;

        if self.first_blob_time.is_none() {
            self.first_blob_time = Some(Instant::now());
        }
        self.pending.insert(chunk_id, (blob_len, 1));
        self.blob_meta.push(BlobMeta {
            chunk_id,
            stored_size: blob_len,
        });

        Ok(offset)
    }

    /// Check if a chunk is pending in this writer (not yet flushed).
    pub fn contains_pending(&self, chunk_id: &ChunkId) -> bool {
        self.pending.contains_key(chunk_id)
    }

    /// Increment refcount for a pending chunk (dedup hit within the same pack).
    pub fn increment_pending(&mut self, chunk_id: &ChunkId) {
        if let Some(entry) = self.pending.get_mut(chunk_id) {
            entry.1 += 1;
        }
    }

    /// Get stored size for a pending chunk.
    pub fn get_pending_stored_size(&self, chunk_id: &ChunkId) -> Option<u32> {
        self.pending.get(chunk_id).map(|(size, _)| *size)
    }

    /// Whether the current buffer should be flushed.
    ///
    /// Returns true when any of these conditions are met:
    /// - Pack has reached its target byte size
    /// - Pack has reached the maximum blob count (10,000)
    /// - Pack has been open longer than the max age (300 seconds)
    pub fn should_flush(&self) -> bool {
        if self.blob_meta.is_empty() {
            return false;
        }
        if self.current_size >= self.target_size {
            return true;
        }
        if self.blob_meta.len() >= MAX_BLOBS_PER_PACK {
            return true;
        }
        if let Some(first_time) = self.first_blob_time {
            if first_time.elapsed().as_secs() >= PACK_MAX_AGE_SECS {
                return true;
            }
        }
        false
    }

    /// Whether there are any pending blobs.
    pub fn has_pending(&self) -> bool {
        !self.blob_meta.is_empty()
    }

    pub fn pack_type(&self) -> PackType {
        self.pack_type
    }

    /// Heap bytes held by the in-progress buffer (0 before the first blob,
    /// and for a staged pack, whose bytes are in the backend's temp file).
    pub fn buffer_capacity(&self) -> usize {
        match &self.buffer {
            Some(PackBuffer::Memory(v)) => v.capacity(),
            Some(PackBuffer::Staged { .. } | PackBuffer::Failed) | None => 0,
        }
    }

    /// The target pack size in bytes.
    pub fn target_size(&self) -> usize {
        self.target_size
    }

    /// Update the target pack size (e.g. after flushing a pack during backup).
    pub(crate) fn set_target_size(&mut self, target: usize) {
        self.target_size = target;
    }

    /// Seal the pack: compute PackId, return a `SealedPack` that can be
    /// destructured for upload.
    pub fn seal(&mut self) -> Result<SealedPack> {
        if matches!(self.buffer, Some(PackBuffer::Failed)) {
            return Err(failed_writer_error());
        }
        if self.blob_meta.is_empty() {
            return Err(VykarError::Other("cannot seal empty pack writer".into()));
        }

        // Build results from blob_meta.
        let mut results: Vec<PackedChunkEntry> = Vec::with_capacity(self.blob_meta.len());
        let mut running_offset = PACK_HEADER_SIZE;
        for meta in &self.blob_meta {
            let offset = running_offset as u64 + 4;
            running_offset += 4 + meta.stored_size as usize;

            let refcount = self.pending.get(&meta.chunk_id).map_or(1, |(_, rc)| *rc);
            results.push((meta.chunk_id, meta.stored_size, offset, refcount));
        }

        let (pack_id, sealed_data) = match self.buffer.take().expect("buffer was initialized") {
            PackBuffer::Memory(v) => (PackId::compute(&v, self.hash_algo), SealedData::Memory(v)),
            PackBuffer::Staged { object, hasher } => (
                PackId::from_bytes(hasher.finalize()),
                SealedData::Staged(object),
            ),
            // Rejected on entry; kept total rather than asserted.
            PackBuffer::Failed => {
                self.buffer = Some(PackBuffer::Failed);
                return Err(failed_writer_error());
            }
        };

        // Clear writer state for reuse
        self.blob_meta.clear();
        self.current_size = 0;
        self.pending.clear();
        self.first_blob_time = None;

        Ok(SealedPack {
            pack_id,
            entries: results,
            data: sealed_data,
        })
    }

    /// Flush the buffered blobs into a pack file (seal + upload).
    /// Returns (pack_id, vec of (chunk_id, stored_size, offset, refcount)).
    pub fn flush(&mut self, storage: &dyn StorageBackend) -> Result<FlushedPackResult> {
        let SealedPack {
            pack_id,
            entries,
            data,
        } = self.seal()?;
        let key = pack_id.storage_key();
        match data {
            SealedData::Memory(v) => storage.put(&key, &v)?,
            staged @ SealedData::Staged(_) => staged.put_to(storage, &key)?,
        }
        Ok((pack_id, entries))
    }
}

/// Read a single blob from a pack file using a range read.
pub fn read_blob_from_pack(
    storage: &dyn StorageBackend,
    pack_id: &PackId,
    offset: u64,
    length: u32,
) -> Result<Vec<u8>> {
    let data = storage
        .get_range(&pack_id.storage_key(), offset, length as u64)?
        .ok_or_else(|| VykarError::Other(format!("pack not found: {pack_id}")))?;
    if data.len() != length as usize {
        return Err(VykarError::Other(format!(
            "short read from pack {pack_id} at offset {offset}: expected {length} bytes, got {}",
            data.len()
        )));
    }
    Ok(data)
}

/// Forward-scan pack bytes using per-blob length prefixes.
/// Returns (offset, length) pairs for each blob.
//
// Indexing into `pack_data` is gated by the header validation up-front and
// `pos + 4 <= blobs_end` / `pos + 4 + blob_len ...` per loop iteration;
// out-of-bounds is unreachable for inputs that pass those checks.
#[allow(clippy::indexing_slicing)]
pub fn scan_pack_blobs_bytes(pack_data: &[u8]) -> Result<Vec<(u64, u32)>> {
    validate_pack_header(pack_data).map_err(VykarError::InvalidFormat)?;

    let mut pos = PACK_HEADER_SIZE;
    let blobs_end = pack_data.len();
    let mut blobs = Vec::new();

    while pos + 4 <= blobs_end {
        let blob_len = u32::from_le_bytes(
            pack_data[pos..pos + 4]
                .try_into()
                .map_err(|_| VykarError::InvalidFormat("invalid blob length field".into()))?,
        );
        if pos + 4 + blob_len as usize > blobs_end {
            break;
        }
        let blob_offset = (pos + 4) as u64;
        blobs.push((blob_offset, blob_len));
        pos += 4 + blob_len as usize;
    }

    if pos != blobs_end {
        let trailing = blobs_end - pos;
        return Err(VykarError::InvalidFormat(format!(
            "truncated or corrupt pack: {trailing} trailing bytes"
        )));
    }

    Ok(blobs)
}

/// Forward-scan a pack file using per-blob length prefixes.
/// Returns (offset, length) pairs for each blob.
pub fn scan_pack_blobs(storage: &dyn StorageBackend, pack_id: &PackId) -> Result<Vec<(u64, u32)>> {
    let pack_data = storage
        .get(&pack_id.storage_key())?
        .ok_or_else(|| VykarError::Other(format!("pack not found: {pack_id}")))?;
    scan_pack_blobs_bytes(&pack_data)
}

/// Compute the dynamic target pack size for data packs.
pub fn compute_data_pack_target(
    num_data_packs: usize,
    min_pack_size: u32,
    max_pack_size: u32,
) -> usize {
    let min = min_pack_size as f64;
    let max = max_pack_size as f64;
    #[allow(
        clippy::cast_precision_loss,
        reason = "pack-target sizing; usize range fits f64 mantissa for realistic counts"
    )]
    let target = min * (num_data_packs as f64 / 50.0).sqrt();
    target.clamp(min, max) as usize
}

/// Compute the target pack size for tree packs.
pub fn compute_tree_pack_target(min_pack_size: u32) -> usize {
    let four_mib = 4 * 1024 * 1024;
    std::cmp::min(min_pack_size as usize, four_mib)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_chunk_id(byte: u8) -> ChunkId {
        ChunkId::from_bytes([byte; 32])
    }

    fn memory_bytes(data: &SealedData) -> &[u8] {
        data.as_bytes().expect("expected a heap-buffered pack")
    }

    /// Blob sets exercising the edge cases of both paths: one blob, many
    /// tiny ones, empty blobs, a blob larger than the local write buffer, and
    /// a last blob that crosses the target.
    fn blob_sets(target: usize) -> Vec<Vec<Vec<u8>>> {
        vec![
            vec![vec![0xAB; 1]],
            (0..300u32)
                .map(|i| vec![i as u8; (i % 7) as usize])
                .collect(),
            vec![vec![], vec![1, 2, 3], vec![]],
            vec![(0..200_000u32).map(|i| (i % 251) as u8).collect()],
            vec![vec![5; target - 8], vec![6; 4096]],
        ]
    }

    /// The staged path must write exactly the bytes, offsets and pack ID the
    /// heap path does, under both hash algorithms: the pack ID is a storage
    /// key and the server recomputes it from the bytes.
    #[test]
    fn staged_pack_is_byte_identical_to_buffered_pack() {
        use vykar_storage::local_backend::LocalBackend;

        let target = 1024 * 1024;
        for algo in [HashAlgorithm::Blake2b, HashAlgorithm::Blake3] {
            for blobs in blob_sets(target) {
                let dir = tempfile::tempdir().unwrap();
                let local = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();

                let mut heap = PackWriter::new(PackType::Data, target, algo);
                let mut staged = PackWriter::new(PackType::Data, target, algo);
                for (i, blob) in blobs.iter().enumerate() {
                    let mut id = [0u8; 32];
                    id[..4].copy_from_slice(&(i as u32).to_le_bytes());
                    let id = ChunkId::from_bytes(id);
                    let a = heap.add_blob(id, blob.clone()).unwrap();
                    let b = staged.add_blob_to(Some(&local), id, blob.clone()).unwrap();
                    assert_eq!(a, b, "offsets diverge");
                    assert_eq!(heap.should_flush(), staged.should_flush());
                }
                assert!(matches!(staged.buffer, Some(PackBuffer::Staged { .. })));
                assert_eq!(staged.buffer_capacity(), 0);

                let heap = heap.seal().unwrap();
                let staged = staged.seal().unwrap();
                assert_eq!(heap.pack_id, staged.pack_id, "{algo:?}");
                assert_eq!(heap.entries, staged.entries);
                assert!(matches!(staged.data, SealedData::Staged(_)));
                assert_eq!(staged.data.capacity(), 0);

                let bytes = memory_bytes(&heap.data).to_vec();
                assert_eq!(PackId::compute(&bytes, algo), staged.pack_id);
                let key = staged.pack_id.storage_key();
                staged.data.put_to(&local, &key).unwrap();
                assert_eq!(local.get(&key).unwrap().unwrap(), bytes);
                assert_eq!(
                    scan_pack_blobs(&local, &staged.pack_id).unwrap().len(),
                    blobs.len()
                );
            }
        }
    }

    /// A backend that cannot stage keeps the writer on the heap path.
    #[test]
    fn non_staging_backend_buffers_in_memory() {
        let storage = crate::testutil::MemoryBackend::new();
        let mut w = PackWriter::new(PackType::Data, 1024, HashAlgorithm::Blake3);
        w.add_blob_to(Some(&storage), dummy_chunk_id(1), vec![1; 10])
            .unwrap();
        assert!(matches!(w.buffer, Some(PackBuffer::Memory(_))));
        assert!(matches!(w.seal().unwrap().data, SealedData::Memory(_)));
    }

    /// Stages into an object whose appends start failing after `ok_appends`.
    struct FailingStageBackend {
        ok_appends: usize,
        dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    struct FailingStagedPut {
        remaining: usize,
        dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for FailingStagedPut {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl StagedPut for FailingStagedPut {
        fn append(&mut self, _data: &[u8]) -> Result<()> {
            if self.remaining == 0 {
                // EIO: soft when it comes from a source file.
                return Err(VykarError::Io(std::io::Error::from_raw_os_error(5)));
            }
            self.remaining -= 1;
            Ok(())
        }
        fn commit(self: Box<Self>, _key: &str) -> Result<()> {
            Err(VykarError::Other(
                "a failed pack must never be committed".into(),
            ))
        }
    }

    impl StorageBackend for FailingStageBackend {
        fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn put(&self, _: &str, _: &[u8]) -> Result<()> {
            Ok(())
        }
        fn delete(&self, _: &str) -> Result<()> {
            Ok(())
        }
        fn exists(&self, _: &str) -> Result<bool> {
            Ok(false)
        }
        fn list(&self, _: &str) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn get_range(&self, _: &str, _: u64, _: u64) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn create_dir(&self, _: &str) -> Result<()> {
            Ok(())
        }
        fn begin_staged_put(&self, prefix: &str) -> Result<Option<Box<dyn StagedPut>>> {
            if prefix != PACK_STAGING_PREFIX {
                return Err(VykarError::Other(format!("unexpected prefix {prefix}")));
            }
            Ok(Some(Box::new(FailingStagedPut {
                remaining: self.ok_appends,
                dropped: std::sync::Arc::clone(&self.dropped),
            })))
        }

        fn profile(&self) -> vykar_storage::BackendProfile {
            vykar_storage::BackendProfile {
                local: true,
                staged_put: true,
                request_width: 1,
            }
        }

        vykar_storage::unsupported_server_ops!();
    }

    /// A failed append poisons the writer, discards the staged object, and
    /// is not an error a backup could skip as a per-file read failure.
    #[test]
    fn staged_write_failure_poisons_writer() {
        // 0: the header append fails; 4: header, blob 1 (prefix + payload)
        // and blob 2's prefix succeed, then blob 2's payload fails.
        for ok_appends in [0, 4] {
            let backend = FailingStageBackend {
                ok_appends,
                dropped: Default::default(),
            };
            let mut w = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake3);
            if ok_appends > 0 {
                w.add_blob_to(Some(&backend), dummy_chunk_id(1), vec![1; 10])
                    .unwrap();
            }
            let err = w
                .add_blob_to(Some(&backend), dummy_chunk_id(2), vec![2; 10])
                .unwrap_err();
            assert!(!err.is_soft_file_error(), "{err}");
            assert!(
                err.to_string().contains("writing pack to repository"),
                "{err}"
            );
            assert!(
                backend.dropped.load(std::sync::atomic::Ordering::SeqCst),
                "staged object is discarded on failure"
            );
            assert!(w.get_pending_stored_size(&dummy_chunk_id(2)).is_none());

            if ok_appends > 0 {
                // Accepted blobs cannot be stored any more: sealing fails
                // instead of producing a pack without them, and so does any
                // later blob, even on the heap path.
                assert!(w.has_pending());
                assert!(w.seal().is_err());
                assert!(w.add_blob(dummy_chunk_id(3), vec![3]).is_err());
                assert!(w.seal().is_err());
            }
        }
    }

    #[test]
    fn should_flush_on_size() {
        let mut w = PackWriter::new(PackType::Data, 100, HashAlgorithm::Blake2b);
        assert!(!w.should_flush());
        w.add_blob(dummy_chunk_id(0), vec![0u8; 120]).unwrap();
        assert!(w.should_flush());
    }

    #[test]
    fn crossing_target_does_not_double_pack_capacity() {
        let target = 1024 * 1024;
        for algo in [HashAlgorithm::Blake2b, HashAlgorithm::Blake3] {
            for last_size in [4, 128, target + 1] {
                let mut w = PackWriter::new(PackType::Data, target, algo);
                w.add_blob(dummy_chunk_id(1), vec![1; target - 8]).unwrap();
                assert!(!w.should_flush());
                w.add_blob(dummy_chunk_id(2), vec![2; last_size]).unwrap();
                assert!(w.should_flush());

                let sealed = w.seal().unwrap();
                let SealedData::Memory(data) = sealed.data else {
                    panic!("no stage given, so the pack is heap-buffered");
                };
                assert_eq!(data.len(), PACK_HEADER_SIZE + target + last_size);
                assert_eq!(data.capacity(), data.len());
                assert_eq!(scan_pack_blobs_bytes(&data).unwrap().len(), 2);
                // The sealed pack's name must be its own contents under the
                // writer's algorithm — the invariant the server also relies on.
                assert_eq!(PackId::compute(&data, algo), sealed.pack_id);
            }
        }
    }

    #[test]
    fn should_flush_on_blob_count() {
        // Use a very large target size so size-based flush never triggers
        let mut w = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
        for i in 0..MAX_BLOBS_PER_PACK {
            assert!(!w.should_flush(), "should not flush at {i} blobs");
            let mut id_bytes = [0u8; 32];
            id_bytes[0..4].copy_from_slice(&(i as u32).to_le_bytes());
            w.add_blob(ChunkId::from_bytes(id_bytes), vec![1]).unwrap();
        }
        assert!(w.should_flush());
    }

    #[test]
    fn seal_resets_first_blob_time() {
        let mut w = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
        w.add_blob(dummy_chunk_id(0), vec![0u8; 10]).unwrap();
        assert!(w.first_blob_time.is_some());

        let _ = w.seal().unwrap();
        assert!(w.first_blob_time.is_none());
    }

    /// Golden-byte regression test. No header trailer — just magic + blobs.
    /// Any change to wire format or byte ordering will fail this.
    #[test]
    fn seal_deterministic_bytes() {
        // Hard-coded expected output. Structure:
        //   [0..9]   VGERPACK\x01  — magic + version
        //   [9..15]  blob 0: 4B len LE (2) + 2B data (0xDE 0xAD)
        //   [15..22] blob 1: 4B len LE (3) + 3B data (0xBE 0xEF 0x42)
        #[rustfmt::skip]
        const EXPECTED: &[u8] = &[
            // Pack header: VGERPACK + version 1
            0x56, 0x47, 0x45, 0x52, 0x50, 0x41, 0x43, 0x4b, 0x01,
            // Blob 0: len=2 LE, data=0xDE 0xAD
            0x02, 0x00, 0x00, 0x00, 0xde, 0xad,
            // Blob 1: len=3 LE, data=0xBE 0xEF 0x42
            0x03, 0x00, 0x00, 0x00, 0xbe, 0xef, 0x42,
        ];

        let mut w = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
        w.add_blob(dummy_chunk_id(0xAA), vec![0xDE, 0xAD]).unwrap();
        w.add_blob(dummy_chunk_id(0xBB), vec![0xBE, 0xEF, 0x42])
            .unwrap();

        let sealed = w.seal().unwrap();

        assert_eq!(
            memory_bytes(&sealed.data),
            EXPECTED,
            "pack wire format regression"
        );
    }

    /// Roundtrip: seal a pack, then parse it back via scan_pack_blobs.
    #[test]
    fn seal_roundtrip_scan() {
        use crate::testutil::MemoryBackend;

        let storage = MemoryBackend::new();

        let blobs: Vec<(ChunkId, Vec<u8>)> = vec![
            (dummy_chunk_id(1), vec![10u8; 50]),
            (dummy_chunk_id(2), vec![20u8; 80]),
            (dummy_chunk_id(3), vec![30u8; 30]),
        ];

        let mut w = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
        for (chunk_id, data) in &blobs {
            w.add_blob(*chunk_id, data.clone()).unwrap();
        }

        let sealed = w.seal().unwrap();

        // Store the pack so scan_pack_blobs can access it.
        storage
            .put(&sealed.pack_id.storage_key(), memory_bytes(&sealed.data))
            .unwrap();

        // Verify scan_pack_blobs returns matching (offset, length) pairs.
        let scanned = scan_pack_blobs(&storage, &sealed.pack_id).unwrap();
        assert_eq!(scanned.len(), blobs.len());
        for (i, (offset, length)) in scanned.iter().enumerate() {
            assert_eq!(*offset, sealed.entries[i].2, "scan offset mismatch at {i}");
            assert_eq!(*length, sealed.entries[i].1, "scan length mismatch at {i}");
        }
    }

    /// Seal clears writer state after success.
    #[test]
    fn seal_clears_state() {
        let mut w = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
        w.add_blob(dummy_chunk_id(1), vec![0xAA; 100]).unwrap();
        w.add_blob(dummy_chunk_id(2), vec![0xBB; 200]).unwrap();

        let sealed = w.seal().unwrap();

        assert_eq!(sealed.entries.len(), 2);
        assert!(!sealed.pack_id.as_bytes().iter().all(|&b| b == 0));
        // Writer should be clear now.
        assert!(!w.has_pending());
        assert!(w.buffer.is_none());
        assert_eq!(w.current_size, 0);
    }

    /// Both data and tree packs use heap-backed Memory buffers.
    #[test]
    fn data_and_tree_packs_both_use_memory() {
        let mut data_w = PackWriter::new(PackType::Data, 1024, HashAlgorithm::Blake2b);
        data_w.add_blob(dummy_chunk_id(0), vec![0u8; 10]).unwrap();
        assert!(
            matches!(data_w.buffer, Some(PackBuffer::Memory(_))),
            "data pack should use Memory buffer"
        );

        let mut tree_w = PackWriter::new(PackType::Tree, 1024, HashAlgorithm::Blake2b);
        tree_w.add_blob(dummy_chunk_id(0), vec![0u8; 10]).unwrap();
        assert!(
            matches!(tree_w.buffer, Some(PackBuffer::Memory(_))),
            "tree pack should use Memory buffer"
        );
    }

    /// Validates `current_size` tracks correctly across add → add → seal.
    #[test]
    fn current_size_invariant() {
        let mut w = PackWriter::new(PackType::Tree, usize::MAX, HashAlgorithm::Blake2b);

        // Add blobs, check invariant after each (Vec path).
        w.add_blob(dummy_chunk_id(1), vec![0xAA; 100]).unwrap();
        if let Some(PackBuffer::Memory(ref v)) = w.buffer {
            assert_eq!(v.len(), PACK_HEADER_SIZE + w.current_size);
        }

        w.add_blob(dummy_chunk_id(2), vec![0xBB; 50]).unwrap();
        if let Some(PackBuffer::Memory(ref v)) = w.buffer {
            assert_eq!(v.len(), PACK_HEADER_SIZE + w.current_size);
        }

        // Successful seal clears everything.
        let sealed = w.seal().unwrap();
        assert_eq!(sealed.entries.len(), 2);
        assert!(w.buffer.is_none());
        assert_eq!(w.current_size, 0);
    }

    /// SealedData::put_to dispatches Memory to put_owned.
    #[test]
    fn put_to_dispatches_correctly() {
        use std::sync::atomic::{AtomicU8, Ordering};

        const CALLED_PUT_OWNED: u8 = 2;

        struct RecordingBackend {
            called: AtomicU8,
        }
        impl StorageBackend for RecordingBackend {
            fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
                Ok(None)
            }
            fn put(&self, _: &str, _: &[u8]) -> Result<()> {
                Ok(())
            }
            fn put_owned(&self, _: &str, _: Vec<u8>) -> Result<()> {
                self.called.store(CALLED_PUT_OWNED, Ordering::SeqCst);
                Ok(())
            }
            fn delete(&self, _: &str) -> Result<()> {
                Ok(())
            }
            fn exists(&self, _: &str) -> Result<bool> {
                Ok(false)
            }
            fn list(&self, _: &str) -> Result<Vec<String>> {
                Ok(vec![])
            }
            fn get_range(&self, _: &str, _: u64, _: u64) -> Result<Option<Vec<u8>>> {
                Ok(None)
            }
            fn create_dir(&self, _: &str) -> Result<()> {
                Ok(())
            }

            fn profile(&self) -> vykar_storage::BackendProfile {
                vykar_storage::BackendProfile {
                    local: false,
                    staged_put: false,
                    request_width: 1,
                }
            }

            vykar_storage::unsupported_server_ops!();
        }

        // Memory variant → put_owned
        let backend = RecordingBackend {
            called: AtomicU8::new(0),
        };
        let data = SealedData::Memory(vec![1, 2, 3]);
        data.put_to(&backend, "test").unwrap();
        assert_eq!(
            backend.called.load(Ordering::SeqCst),
            CALLED_PUT_OWNED,
            "Memory variant should call put_owned"
        );
    }

    #[test]
    fn scan_pack_blobs_rejects_truncated_pack() {
        use crate::testutil::MemoryBackend;

        let storage = MemoryBackend::new();
        let pack_id = PackId::from_bytes([0xAB; 32]);

        // Valid pack with one 2-byte blob, then 3 trailing garbage bytes
        let mut data = Vec::new();
        data.extend_from_slice(PACK_MAGIC);
        data.push(PACK_VERSION_CURRENT);
        data.extend_from_slice(&2u32.to_le_bytes()); // blob len = 2
        data.extend_from_slice(&[0xDE, 0xAD]); // blob data
        data.extend_from_slice(&[0xFF, 0xFF, 0xFF]); // trailing garbage (incomplete frame)

        storage.put(&pack_id.storage_key(), &data).unwrap();

        let err = scan_pack_blobs(&storage, &pack_id).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("trailing bytes"),
            "expected truncation error, got: {msg}"
        );
    }

    /// Backend that intentionally returns truncated data from get_range,
    /// bypassing normal backend enforcement, to test the defense-in-depth
    /// check in read_blob_from_pack.
    struct ShortReadBackend {
        data: HashMap<String, Vec<u8>>,
    }

    impl StorageBackend for ShortReadBackend {
        fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.data.get(key).cloned())
        }
        fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }
        fn exists(&self, key: &str) -> Result<bool> {
            Ok(self.data.contains_key(key))
        }
        fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn get_range(&self, key: &str, offset: u64, _length: u64) -> Result<Option<Vec<u8>>> {
            // Intentionally return truncated data (1 byte short)
            match self.data.get(key) {
                Some(data) => {
                    let start = offset as usize;
                    let end = data.len().min(start + _length as usize);
                    if start >= data.len() {
                        return Ok(Some(Vec::new()));
                    }
                    let mut result = data[start..end].to_vec();
                    if !result.is_empty() {
                        result.pop(); // truncate by 1 byte
                    }
                    Ok(Some(result))
                }
                None => Ok(None),
            }
        }
        fn create_dir(&self, _key: &str) -> Result<()> {
            Ok(())
        }

        fn profile(&self) -> vykar_storage::BackendProfile {
            vykar_storage::BackendProfile {
                local: false,
                staged_put: false,
                request_width: 1,
            }
        }

        vykar_storage::unsupported_server_ops!();
    }

    #[test]
    fn read_blob_from_pack_rejects_short_read() {
        let pack_id = PackId::from_bytes([0xCD; 32]);
        let blob_data = vec![0xAB; 100];

        let mut storage_data = HashMap::new();
        storage_data.insert(pack_id.storage_key(), blob_data);
        let storage = ShortReadBackend { data: storage_data };

        let err = read_blob_from_pack(&storage, &pack_id, 0, 100).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("short read from pack") && msg.contains("expected 100 bytes, got 99"),
            "expected short read error, got: {msg}"
        );
    }

    #[test]
    fn compute_data_pack_target_scaling() {
        let min = 32 * 1024 * 1024u32; // 32 MiB
        let max = 192 * 1024 * 1024u32; // 192 MiB

        // At 0 packs: clamped to min
        assert_eq!(compute_data_pack_target(0, min, max), min as usize);

        // At 50 packs: min * sqrt(50/50) = min * 1 = min
        assert_eq!(compute_data_pack_target(50, min, max), min as usize);

        // At 200 packs: min * sqrt(200/50) = min * 2 = 64 MiB
        assert_eq!(compute_data_pack_target(200, min, max), 64 * 1024 * 1024);

        // At 800 packs: min * sqrt(800/50) = min * 4 = 128 MiB
        assert_eq!(compute_data_pack_target(800, min, max), 128 * 1024 * 1024);

        // At 1800 packs: min * sqrt(1800/50) = min * 6 = 192 MiB (max)
        assert_eq!(compute_data_pack_target(1800, min, max), 192 * 1024 * 1024);

        // Beyond max: clamped
        assert_eq!(compute_data_pack_target(10_000, min, max), max as usize);

        // Monotonic increase
        let mut prev = 0;
        for n in [0, 10, 50, 100, 200, 400, 800, 1600, 3200] {
            let t = compute_data_pack_target(n, min, max);
            assert!(t >= prev, "target should be monotonically non-decreasing");
            prev = t;
        }
    }

    #[test]
    fn set_target_size_after_seal() {
        let mut w = PackWriter::new(PackType::Data, 100, HashAlgorithm::Blake2b);
        w.add_blob(dummy_chunk_id(0), vec![0u8; 120]).unwrap();
        assert!(w.should_flush());

        let _ = w.seal().unwrap();

        // Update target to a larger size
        w.set_target_size(10_000);
        assert_eq!(w.target_size(), 10_000);

        // Adding a small blob should no longer trigger flush
        w.add_blob(dummy_chunk_id(1), vec![0u8; 50]).unwrap();
        assert!(!w.should_flush());
    }
}
