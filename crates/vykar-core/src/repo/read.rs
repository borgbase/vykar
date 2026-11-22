use std::collections::HashMap;
use std::time::Instant;

use smallvec::SmallVec;

use super::Repository;
use super::blob_read::{
    BlobReader, GroupEnd, PlannedRead, ReadBuffers, ReadGroup, coalesce_groups,
    fetch_groups_bounded,
};
use super::control::CancelToken;
use super::format::{BlobLocation, decode_chunk_into};
use super::pack::read_blob_from_pack;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

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
}

impl Assembler<'_> {
    /// Append every completed slot at the cursor to `out`.
    fn drain_ready(&mut self) {
        while let Some(slot) = self.slots.get_mut(self.next_emit) {
            if let Some(data) = slot.take() {
                self.out.extend_from_slice(&data);
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

        // --- Phase 1: group by pack, one blob per distinct location ---
        // A repeated entry (the same chunk twice in a stream) is read and
        // decoded once: the storage range reader cannot go back over bytes it
        // has served, and the in-place decode destroys them anyway. The key is
        // the whole location, so two IDs claiming one location (corrupt index)
        // stay two blobs, and the second fails as an overlapping read.
        let mut pack_blobs: HashMap<PackId, Vec<CoalescedBlob>> = HashMap::new();
        let mut seen: HashMap<(ChunkId, PackId, u64, u32), usize> = HashMap::new();
        for (idx, &(chunk_id, pack_id, pack_offset, stored_size)) in chunks.iter().enumerate() {
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

        let mut asm = Assembler {
            slots: vec![None; chunks.len()],
            next_emit: 0,
            out,
        };

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
        let reader = BlobReader::new(self.storage.as_ref(), self.crypto.as_ref(), false);
        fetch_groups_bounded(
            width,
            &groups,
            shutdown,
            |g| fetch_group(&reader, g),
            |blobs| asm.accept(blobs),
        )?;

        // Final drain (safety net for groups not sorted by earliest slot).
        asm.drain_ready();
        debug_assert_eq!(
            asm.next_emit,
            asm.slots.len(),
            "not all chunks were emitted"
        );

        let bytes: u64 = groups.iter().map(|g| g.read_end - g.read_start).sum();
        tracing::debug!(
            "coalesced read: {} chunks in {} range reads of {} bytes, width {}, {:?}",
            chunks.len(),
            groups.len(),
            bytes,
            threads,
            started.elapsed()
        );
        Ok(())
    }
}
