use std::collections::HashMap;
use std::time::Instant;

use smallvec::SmallVec;

use super::Repository;
use super::blob_read::{
    BlobReader, GroupEnd, PlannedRead, RawSink, ReadBuffers, ReadGroup, coalesce_groups,
    fetch_groups_bounded,
};
use super::control::CancelToken;
use super::format::{BlobLocation, decode_chunk_into};
use super::pack::read_blob_from_pack;
use super::tree_cache::TreeChunkCache;
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// Where an item-stream read takes its tree chunks from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TreeChunkSource {
    /// The local tree-chunk cache first, storage for misses; what is
    /// downloaded is written to the cache.
    CacheFirst,
    /// Storage only, the cache neither read nor written (check, repair, the
    /// simulator's checker: they verify what the repository holds).
    Storage,
}

/// Chunks per unit of work of the cache pass of
/// [`Repository::read_tree_chunks_into`].
const CACHE_BATCH: usize = 256;

/// Thread cap of that cache pass: it is local reads and decode, CPU work,
/// independent of the backend's request width.
const CACHE_PASS_MAX_WIDTH: usize = 8;

/// Look up `batch` (indices into `chunks`) in `cache` and decode each hit
/// like a pack read. Returns the decoded hits and the misses; an entry that
/// does not decode is deleted and is a miss.
fn probe_cache(
    cache: &TreeChunkCache,
    crypto: &dyn CryptoEngine,
    chunks: &[(ChunkId, PackId, u64, u32)],
    batch: &[usize],
) -> (Vec<DecodedBlob>, Vec<usize>) {
    let mut decoded = Vec::with_capacity(batch.len());
    let mut misses = Vec::new();
    for &i in batch {
        let Some(&(chunk_id, pack_id, pack_offset, stored_size)) = chunks.get(i) else {
            continue;
        };
        let Some(mut bytes) = cache.get(&chunk_id) else {
            misses.push(i);
            continue;
        };
        let mut plain = Vec::new();
        let at = BlobLocation {
            chunk_id,
            pack_id,
            pack_offset,
            stored_size,
        };
        match decode_chunk_into(&mut bytes, at, crypto, None, false, &mut plain) {
            Ok(()) => decoded.push(DecodedBlob {
                result_idxs: smallvec::smallvec![i],
                plaintext: plain,
            }),
            Err(e) => {
                tracing::debug!("tree-chunk cache: entry {chunk_id} does not decode: {e:?}");
                cache.remove(&chunk_id);
                misses.push(i);
            }
        }
    }
    (decoded, misses)
}

/// A unique blob of a coalesced read: one stored location, and every slot in
/// the caller's `chunks` that wants it.
struct CoalescedBlob {
    /// Indices in the original `chunks` slice (determine output order), in
    /// input order, so `result_idxs[0]` is the smallest.
    result_idxs: SmallVec<[usize; 1]>,
    chunk_id: ChunkId,
    pack_offset: u64,
    stored_size: u32,
}

impl PlannedRead for CoalescedBlob {
    fn chunk_id(&self) -> ChunkId {
        self.chunk_id
    }
    fn pack_offset(&self) -> u64 {
        self.pack_offset
    }
    fn stored_size(&self) -> u32 {
        self.stored_size
    }
}

/// A blob of a coalesced group after decoding: its slots in the caller's
/// `chunks` and its plaintext.
struct DecodedBlob {
    result_idxs: SmallVec<[usize; 1]>,
    plaintext: Vec<u8>,
}

/// Fetch `group` with one range read and decode its blobs in `group.blobs`
/// order through `decode_chunk_into`, which also verifies the chunk ID when
/// the engine does not encrypt.
fn fetch_group(
    reader: &BlobReader<'_>,
    group: &ReadGroup<CoalescedBlob>,
) -> Result<Vec<DecodedBlob>> {
    let mut bufs = ReadBuffers::default();
    let mut decoded = Vec::with_capacity(group.blobs.len());
    let end = reader.read_group(
        group,
        &mut bufs,
        || false,
        |blob, plain| {
            let plain = plain.map_err(VykarError::from)?;
            decoded.push(DecodedBlob {
                result_idxs: blob.result_idxs.clone(),
                plaintext: std::mem::take(plain),
            });
            Ok::<(), VykarError>(())
        },
    )?;
    match end {
        GroupEnd::Done => Ok(decoded),
        GroupEnd::PackNotFound => Err(VykarError::Other(format!(
            "pack not found: {}",
            group.pack_id
        ))),
        GroupEnd::OpenFailed(e) | GroupEnd::ReadFailed { error: e, .. } => Err(e),
        // Cannot happen with `|| false`; map it rather than panic.
        GroupEnd::Stopped => Err(VykarError::Interrupted),
    }
}

/// Holds out-of-order plaintext until every earlier chunk has arrived;
/// `out` always ends with `chunks[..next_emit]` in order.
struct Assembler<'a> {
    slots: Vec<Option<Vec<u8>>>,
    next_emit: usize,
    out: &'a mut Vec<u8>,
    /// When set, `out.len()` is pushed after each emitted slot.
    ends: Option<&'a mut Vec<usize>>,
}

impl<'a> Assembler<'a> {
    /// One empty slot per chunk of the read, emitting into `out`.
    fn new(len: usize, out: &'a mut Vec<u8>, ends: Option<&'a mut Vec<usize>>) -> Self {
        Self {
            slots: vec![None; len],
            next_emit: 0,
            out,
            ends,
        }
    }

    /// Final drain (safety net for groups not sorted by earliest slot);
    /// every slot has been filled by now.
    fn finish(&mut self) {
        self.drain_ready();
        debug_assert_eq!(
            self.next_emit,
            self.slots.len(),
            "not all chunks were emitted"
        );
    }

    /// Append every completed slot at the cursor to `out`.
    fn drain_ready(&mut self) {
        while let Some(slot) = self.slots.get_mut(self.next_emit) {
            if let Some(data) = slot.take() {
                self.out.extend_from_slice(&data);
                if let Some(ends) = self.ends.as_deref_mut() {
                    ends.push(self.out.len());
                }
                self.next_emit += 1;
            } else {
                break;
            }
        }
    }

    /// Slot and drain each blob of one fetched group, in blob order. A blob
    /// wanted by several slots is cloned into all but the last.
    fn accept(&mut self, blobs: Vec<DecodedBlob>) -> Result<()> {
        for blob in blobs {
            let mut plaintext = Some(blob.plaintext);
            let last = blob.result_idxs.len().saturating_sub(1);
            for (n, &idx) in blob.result_idxs.iter().enumerate() {
                let slot = self.slots.get_mut(idx).ok_or_else(|| {
                    VykarError::Other(format!(
                        "internal error: read result index {idx} out of range"
                    ))
                })?;
                *slot = if n == last {
                    plaintext.take()
                } else {
                    plaintext.clone()
                };
            }
            self.drain_ready();
        }
        Ok(())
    }
}

impl Repository {
    /// Read and decrypt a chunk located through the in-memory chunk index.
    ///
    /// # Errors
    ///
    /// [`VykarError::ChunkNotInIndex`] when the index has no entry for
    /// `chunk_id`, which is always the case on a repository opened without
    /// `OpenOptions::with_index()`.
    pub fn read_chunk(&self, chunk_id: &ChunkId) -> Result<Vec<u8>> {
        let entry = self
            .chunk_index
            .get(chunk_id)
            .ok_or(VykarError::ChunkNotInIndex(*chunk_id))?;

        self.read_chunk_at(
            chunk_id,
            &entry.pack_id,
            entry.pack_offset,
            entry.stored_size,
        )
    }

    /// Read and decrypt a chunk given explicit pack location coordinates.
    /// Bypasses the chunk index — the caller supplies (pack_id, offset, stored_size)
    /// e.g. from an index resolved earlier.
    pub fn read_chunk_at(
        &self,
        chunk_id: &ChunkId,
        pack_id: &PackId,
        pack_offset: u64,
        stored_size: u32,
    ) -> Result<Vec<u8>> {
        let mut blob_data =
            read_blob_from_pack(self.storage.as_ref(), pack_id, pack_offset, stored_size)?;
        let mut plaintext = Vec::new();
        decode_chunk_into(
            &mut blob_data,
            BlobLocation {
                chunk_id: *chunk_id,
                pack_id: *pack_id,
                pack_offset,
                stored_size,
            },
            self.crypto.as_ref(),
            None,
            false,
            &mut plaintext,
        )?;
        Ok(plaintext)
    }

    /// Read multiple chunks via coalesced range reads and append plaintext to `out`.
    ///
    /// Each entry in `chunks` is `(ChunkId, PackId, pack_offset, stored_size)`.
    /// Output is appended to `out` in the same order as `chunks`.
    /// Chunks are grouped by pack and coalesced into large range reads to
    /// minimise HTTP round-trips.
    ///
    /// Range reads go out on up to the backend's `request_width` threads (one
    /// at a time with width 1 or a single read). Each thread decrypts and
    /// decompresses its own reads; the plaintext is reassembled in `chunks`
    /// order on the caller's thread. Up to `request_width` ranges are in flight
    /// at once; a buffered backend holds each range (at most `MAX_READ_SIZE`),
    /// `LocalBackend` only the current blob. Decoded chunks that arrive ahead of
    /// an earlier read wait in memory, bounded by the stream size. A chunk
    /// listed more than once at the same location is read and decoded once.
    ///
    /// `shutdown` is polled before every range read, so a cancellation is
    /// observed within one storage round-trip per read in flight rather than
    /// after the whole stream.
    ///
    /// On error, the error of the earliest failing read in stream order is
    /// returned once the reads already in flight (up to `request_width - 1`)
    /// have finished; the contents of `out` are then unspecified.
    pub fn read_chunks_coalesced_into(
        &self,
        chunks: &[(ChunkId, PackId, u64, u32)],
        out: &mut Vec<u8>,
        shutdown: &CancelToken,
    ) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        let mut asm = Assembler::new(chunks.len(), out, None);
        let all: Vec<usize> = (0..chunks.len()).collect();
        self.read_coalesced_slots(chunks, &all, &mut asm, None, shutdown)?;
        asm.finish();
        Ok(())
    }

    /// [`Self::read_chunks_coalesced_into`] for item-stream chunks, through
    /// the local tree-chunk cache. Same output, ordering, cancellation and
    /// error contract.
    ///
    /// With [`TreeChunkSource::CacheFirst`] every chunk is first looked up in
    /// the cache (on up to `min(available_parallelism, 8)` threads, in
    /// batches of [`CACHE_BATCH`]) and decoded like a pack read; an entry
    /// that does not decode is deleted and counts as a miss. Misses are then
    /// read from storage exactly as `read_chunks_coalesced_into` reads them,
    /// and what was downloaded is written to the cache. With
    /// [`TreeChunkSource::Storage`] the cache is neither read nor written.
    /// A cache failure is never an error.
    pub(crate) fn read_tree_chunks_into(
        &self,
        chunks: &[(ChunkId, PackId, u64, u32)],
        out: &mut Vec<u8>,
        shutdown: &CancelToken,
        source: TreeChunkSource,
    ) -> Result<()> {
        self.read_tree_chunks_impl(chunks, out, None, shutdown, source)
    }

    /// [`Self::read_tree_chunks_into`], also recording where each chunk ends:
    /// after the call, chunk `i` is `out[start_i..ends[i]]` with
    /// `start_0 = out.len()` on entry and `start_i = ends[i-1]`. `ends` is
    /// cleared first. Same ordering, cancellation and error contract; on error
    /// `out` and `ends` are unspecified.
    pub(crate) fn read_tree_chunks_split(
        &self,
        chunks: &[(ChunkId, PackId, u64, u32)],
        out: &mut Vec<u8>,
        ends: &mut Vec<usize>,
        shutdown: &CancelToken,
        source: TreeChunkSource,
    ) -> Result<()> {
        ends.clear();
        ends.reserve(chunks.len());
        self.read_tree_chunks_impl(chunks, out, Some(ends), shutdown, source)
    }

    /// The body of [`Self::read_tree_chunks_into`] and
    /// [`Self::read_tree_chunks_split`].
    fn read_tree_chunks_impl(
        &self,
        chunks: &[(ChunkId, PackId, u64, u32)],
        out: &mut Vec<u8>,
        ends: Option<&mut Vec<usize>>,
        shutdown: &CancelToken,
        source: TreeChunkSource,
    ) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        let cache = match (source, self.tree_cache()) {
            (TreeChunkSource::CacheFirst, Some(cache)) => cache,
            _ => {
                let mut asm = Assembler::new(chunks.len(), out, ends);
                let all: Vec<usize> = (0..chunks.len()).collect();
                self.read_coalesced_slots(chunks, &all, &mut asm, None, shutdown)?;
                asm.finish();
                return Ok(());
            }
        };
        shutdown.check()?;
        let started = Instant::now();
        let mut asm = Assembler::new(chunks.len(), out, ends);

        // Cache pass: decode what the cache holds, collect the misses.
        let all: Vec<usize> = (0..chunks.len()).collect();
        let batches: Vec<&[usize]> = all.chunks(CACHE_BATCH).collect();
        let width = std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .min(CACHE_PASS_MAX_WIDTH);
        let crypto = self.crypto.as_ref();
        let mut misses: Vec<usize> = Vec::new();
        fetch_groups_bounded(
            width,
            &batches,
            shutdown,
            |batch| Ok(probe_cache(cache, crypto, chunks, batch)),
            |(decoded, missed)| {
                misses.extend(missed);
                asm.accept(decoded)
            },
        )?;
        misses.sort_unstable();
        let hits = chunks.len() - misses.len();

        // Storage pass over the misses, filling the cache.
        let (reads, bytes) = if misses.is_empty() {
            (0, 0)
        } else {
            let sink: RawSink<'_> = &|id: &ChunkId, raw: &[u8]| cache.insert(id, raw);
            self.read_coalesced_slots(chunks, &misses, &mut asm, Some(sink), shutdown)?
        };
        asm.finish();
        tracing::debug!(
            "tree chunk read: {} chunks, {} from cache, {} from storage in {} range reads of {} bytes, {:?}",
            chunks.len(),
            hits,
            misses.len(),
            reads,
            bytes,
            started.elapsed()
        );
        Ok(())
    }

    /// Phases 1-3 of a coalesced read over the slots in `todo` (ascending
    /// indices into `chunks`), accepted into `asm`; `sink` receives the
    /// stored bytes of every blob that decoded. Returns (range reads, bytes).
    fn read_coalesced_slots(
        &self,
        chunks: &[(ChunkId, PackId, u64, u32)],
        todo: &[usize],
        asm: &mut Assembler<'_>,
        sink: Option<RawSink<'_>>,
        shutdown: &CancelToken,
    ) -> Result<(usize, u64)> {
        // --- Phase 1: group by pack, one blob per distinct location ---
        // A repeated entry (the same chunk twice in a stream) is read and
        // decoded once: the storage range reader cannot go back over bytes it
        // has served, and the in-place decode destroys them anyway. The key is
        // the whole location, so two IDs claiming one location (corrupt index)
        // stay two blobs, and the second fails as an overlapping read.
        let mut pack_blobs: HashMap<PackId, Vec<CoalescedBlob>> = HashMap::new();
        let mut seen: HashMap<(ChunkId, PackId, u64, u32), usize> = HashMap::new();
        for &idx in todo {
            let &(chunk_id, pack_id, pack_offset, stored_size) =
                chunks.get(idx).ok_or_else(|| {
                    VykarError::Other(format!("internal error: read slot {idx} out of range"))
                })?;
            let blobs = pack_blobs.entry(pack_id).or_default();
            match seen.entry((chunk_id, pack_id, pack_offset, stored_size)) {
                std::collections::hash_map::Entry::Occupied(e) => {
                    // The stored position always exists; losing it would
                    // leave a slot unfilled and cut the stream short.
                    blobs
                        .get_mut(*e.get())
                        .ok_or_else(|| {
                            VykarError::Other(
                                "internal error: coalesced read lost a planned blob".into(),
                            )
                        })?
                        .result_idxs
                        .push(idx);
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(blobs.len());
                    blobs.push(CoalescedBlob {
                        result_idxs: smallvec::smallvec![idx],
                        chunk_id,
                        pack_offset,
                        stored_size,
                    });
                }
            }
        }
        drop(seen);

        // --- Phase 2: coalesce ---
        let mut groups = coalesce_groups(pack_blobs)?;

        // Sort groups so the one containing the earliest-needed slot is first.
        groups.sort_by_key(|g| {
            g.blobs
                .iter()
                .filter_map(|b| b.result_idxs.first().copied())
                .min()
                .expect("coalesced group is non-empty")
        });

        // --- Phase 3: read + decode (fanned out at the backend's request width) ---
        let started = Instant::now();
        let width = self.request_width();
        let threads = width.min(groups.len());
        let mut reader = BlobReader::new(self.storage.as_ref(), self.crypto.as_ref(), false);
        if let Some(sink) = sink {
            reader = reader.with_raw_sink(sink);
        }
        fetch_groups_bounded(
            width,
            &groups,
            shutdown,
            |g| fetch_group(&reader, g),
            |blobs| asm.accept(blobs),
        )?;

        let bytes: u64 = groups.iter().map(|g| g.read_end - g.read_start).sum();
        tracing::debug!(
            "coalesced read: {} chunks in {} range reads of {} bytes, width {}, {:?}",
            todo.len(),
            groups.len(),
            bytes,
            threads,
            started.elapsed()
        );
        Ok((groups.len(), bytes))
    }
}
