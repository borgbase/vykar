use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use super::format::{unpack_object_expect_with_context, verify_chunk_id, ObjectType};
use super::pack::read_blob_from_pack;
use super::{BlobCache, Repository};
use crate::commands::util::check_interrupted;
use crate::compress;
use crate::index::dedup_cache;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// A single blob within a coalesced read group.
struct CoalescedBlob {
    /// Index in the original `chunks` slice (determines output order).
    result_idx: usize,
    chunk_id: ChunkId,
    pack_offset: u64,
    stored_size: u32,
}

/// A group of adjacent blobs from the same pack that will be fetched in one range read.
struct CoalescedGroup {
    pack_id: PackId,
    read_start: u64,
    read_end: u64, // exclusive
    blobs: Vec<CoalescedBlob>,
}

/// A blob of a coalesced group after decoding: its slot in the caller's
/// `chunks` and its plaintext.
struct DecodedBlob {
    result_idx: usize,
    chunk_id: ChunkId,
    plaintext: Vec<u8>,
}

/// Fetch `group` with one range read and decode its blobs in `group.blobs`
/// order. Without AEAD the chunk ID is the only checksum on chunk data, so it
/// is verified here.
fn fetch_group(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    group: &CoalescedGroup,
) -> Result<Vec<DecodedBlob>> {
    let verify = !crypto.is_encrypting();
    let pack_key = group.pack_id.storage_key();
    let read_len = group.read_end - group.read_start;

    let raw_data = storage
        .get_range(&pack_key, group.read_start, read_len)?
        .ok_or_else(|| VykarError::Other(format!("pack not found: {}", group.pack_id)))?;

    let mut decoded = Vec::with_capacity(group.blobs.len());
    for blob in &group.blobs {
        let local_offset = (blob.pack_offset - group.read_start) as usize;
        let local_end = local_offset + blob.stored_size as usize;
        if local_end > raw_data.len() {
            return Err(VykarError::Other(format!(
                "blob extends beyond downloaded range in pack {}",
                group.pack_id
            )));
        }

        let blob_data = raw_data
            .get(local_offset..local_end)
            .expect("local_end <= raw_data.len() (checked above)");
        let compressed = unpack_object_expect_with_context(
            blob_data,
            ObjectType::ChunkData,
            blob.chunk_id.as_bytes(),
            crypto,
        )?;
        let plaintext = compress::decompress(&compressed)?;
        if verify {
            verify_chunk_id(
                crypto.chunk_hasher(),
                &blob.chunk_id,
                &plaintext,
                &group.pack_id,
                blob.pack_offset,
                blob.stored_size,
            )?;
        }
        decoded.push(DecodedBlob {
            result_idx: blob.result_idx,
            chunk_id: blob.chunk_id,
            plaintext,
        });
    }
    Ok(decoded)
}

/// Fetch and decode `groups` on up to `width` threads and pass each group's
/// blobs to `accept` on the calling thread, in completion order. Threads claim
/// groups in slice order and poll `shutdown` before each range read.
///
/// Returns the error of the earliest group (in `groups` order) that failed.
/// Every group before a failed one has been claimed, so for any failure that
/// does not depend on timing this is the error the sequential loop returns.
/// After the first failure no further group is claimed, and results that
/// arrive later are dropped.
fn read_groups_parallel(
    width: usize,
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    groups: &[CoalescedGroup],
    shutdown: Option<&AtomicBool>,
    mut accept: impl FnMut(Vec<DecodedBlob>) -> Result<()>,
) -> Result<()> {
    let indexed: Vec<(usize, &CoalescedGroup)> = groups.iter().enumerate().collect();
    let (tx, rx) = crossbeam_channel::unbounded::<(usize, Result<Vec<DecodedBlob>>)>();
    // Raised when `accept` fails, so workers stop claiming groups. Not raised
    // for fetch errors: an earlier, already-claimed group must still report.
    let stop = AtomicBool::new(false);
    let mut first_err: Option<(usize, VykarError)> = None;
    std::thread::scope(|s| {
        let stop = &stop;
        s.spawn(move || {
            // `Err(())` only stops handing out groups; the error goes over `tx`.
            let _ = super::bounded::try_for_each_bounded(width, &indexed, |&(idx, group)| {
                if stop.load(Ordering::Relaxed) {
                    return Err(());
                }
                let result =
                    check_interrupted(shutdown).and_then(|()| fetch_group(storage, crypto, group));
                let failed = result.is_err();
                // The receiver lives until this thread drops `tx`.
                let _ = tx.send((idx, result));
                if failed {
                    Err(())
                } else {
                    Ok(())
                }
            });
        });
        // Ends once the spawned thread returns and drops `tx`.
        for (idx, result) in rx {
            let result = match result {
                Ok(blobs) if first_err.is_none() => accept(blobs).inspect_err(|_| {
                    stop.store(true, Ordering::Relaxed);
                }),
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            if let Err(e) = result
                && first_err.as_ref().is_none_or(|(i, _)| idx < *i)
            {
                first_err = Some((idx, e));
            }
        }
    });
    first_err.map_or(Ok(()), |(_, e)| Err(e))
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

    /// Cache, slot and drain each blob of one fetched group, in blob order.
    fn accept(&mut self, cache: &mut BlobCache, blobs: Vec<DecodedBlob>) -> Result<()> {
        for blob in blobs {
            cache.insert(blob.chunk_id, blob.plaintext.clone());
            let slot = self.slots.get_mut(blob.result_idx).ok_or_else(|| {
                VykarError::Other(format!(
                    "internal error: read result index {} out of range",
                    blob.result_idx
                ))
            })?;
            *slot = Some(blob.plaintext);
            self.drain_ready();
        }
        Ok(())
    }
}

impl Repository {
    /// Try to open the mmap'd restore cache for this repository.
    /// Returns `None` if the cache is missing, stale, or corrupt.
    pub fn open_restore_cache(&self) -> Option<dedup_cache::MmapRestoreCache> {
        dedup_cache::MmapRestoreCache::open(
            &self.config.id,
            self.index_generation,
            self.cache_dir_override.as_deref(),
        )
    }

    /// Replace the blob cache with a new one of the given capacity.
    pub fn set_blob_cache_max_bytes(&mut self, max_bytes: usize) {
        self.blob_cache = BlobCache::new(max_bytes);
    }

    /// Read and decrypt a chunk from the repository.
    /// Results are cached in a weight-bounded blob cache for faster repeated access.
    pub fn read_chunk(&mut self, chunk_id: &ChunkId) -> Result<Vec<u8>> {
        let entry = *self
            .chunk_index
            .get(chunk_id)
            .ok_or_else(|| VykarError::Other(format!("chunk not found: {chunk_id}")))?;

        self.read_chunk_at(
            chunk_id,
            &entry.pack_id,
            entry.pack_offset,
            entry.stored_size,
        )
    }

    /// Read and decrypt a chunk given explicit pack location coordinates.
    /// Bypasses the chunk index — the caller supplies (pack_id, offset, stored_size)
    /// e.g. from the mmap restore cache.
    pub fn read_chunk_at(
        &mut self,
        chunk_id: &ChunkId,
        pack_id: &PackId,
        pack_offset: u64,
        stored_size: u32,
    ) -> Result<Vec<u8>> {
        if let Some(cached) = self.blob_cache.get(chunk_id) {
            return Ok(cached.to_vec());
        }

        let blob_data =
            read_blob_from_pack(self.storage.as_ref(), pack_id, pack_offset, stored_size)?;
        let compressed = unpack_object_expect_with_context(
            &blob_data,
            ObjectType::ChunkData,
            chunk_id.as_bytes(),
            self.crypto.as_ref(),
        )?;
        let plaintext = compress::decompress(&compressed)?;
        if !self.crypto.is_encrypting() {
            verify_chunk_id(
                self.crypto.chunk_hasher(),
                chunk_id,
                &plaintext,
                pack_id,
                pack_offset,
                stored_size,
            )?;
        }

        self.blob_cache.insert(*chunk_id, plaintext.clone());
        Ok(plaintext)
    }

    /// Read multiple chunks via coalesced range reads and append plaintext to `out`.
    ///
    /// Each entry in `chunks` is `(ChunkId, PackId, pack_offset, stored_size)`.
    /// Output is appended to `out` in the same order as `chunks`.
    /// Cache hits are served from `blob_cache`; misses are grouped by pack and
    /// coalesced into large range reads to minimise HTTP round-trips.
    ///
    /// Range reads go out on up to the backend's `request_width` threads (one
    /// at a time with width 1 or a single read). Each thread decrypts and
    /// decompresses its own reads; the plaintext is reassembled in `chunks`
    /// order on the caller's thread. Up to `request_width` raw ranges (each at
    /// most `MAX_READ_SIZE`) are in flight at once; decoded chunks that arrive
    /// ahead of an earlier read wait in memory, bounded by the stream size.
    ///
    /// `shutdown` is polled before every range read, so a cancellation is
    /// observed within one storage round-trip per read in flight rather than
    /// after the whole stream.
    ///
    /// On error, the error of the earliest failing read in stream order is
    /// returned once the reads already in flight (up to `request_width - 1`)
    /// have finished; the contents of `out` are then unspecified.
    pub fn read_chunks_coalesced_into(
        &mut self,
        chunks: &[(ChunkId, PackId, u64, u32)],
        out: &mut Vec<u8>,
        shutdown: Option<&AtomicBool>,
    ) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        // --- Phase 1: cache scan ---
        // Result slots: Some(plaintext) for cache hits, None for misses.
        let mut slots: Vec<Option<Vec<u8>>> = Vec::with_capacity(chunks.len());
        // Blobs that need fetching, grouped by pack.
        let mut pack_blobs: std::collections::HashMap<PackId, Vec<CoalescedBlob>> =
            std::collections::HashMap::new();
        let mut misses = 0usize;

        for (idx, (chunk_id, pack_id, pack_offset, stored_size)) in chunks.iter().enumerate() {
            if let Some(cached) = self.blob_cache.get(chunk_id) {
                slots.push(Some(cached.to_vec()));
            } else {
                slots.push(None);
                misses += 1;
                pack_blobs.entry(*pack_id).or_default().push(CoalescedBlob {
                    result_idx: idx,
                    chunk_id: *chunk_id,
                    pack_offset: *pack_offset,
                    stored_size: *stored_size,
                });
            }
        }

        let mut asm = Assembler {
            slots,
            next_emit: 0,
            out,
        };
        asm.drain_ready();

        // All cache hits — done.
        if pack_blobs.is_empty() {
            return Ok(());
        }

        // --- Phase 2: coalesce ---
        let mut groups: Vec<CoalescedGroup> = Vec::new();

        for (pack_id, blobs) in pack_blobs {
            super::coalesce::coalesce_pack_reads(
                &pack_id,
                blobs,
                |b| (b.pack_offset, b.stored_size),
                |read_start, read_end, blobs| {
                    groups.push(CoalescedGroup {
                        pack_id,
                        read_start,
                        read_end,
                        blobs,
                    })
                },
            )?;
        }

        // Sort groups so the one containing the earliest-needed slot is first.
        groups.sort_by_key(|g| {
            g.blobs
                .iter()
                .map(|b| b.result_idx)
                .min()
                .expect("coalesced group is non-empty")
        });

        // --- Phase 3: read + decode (fanned out at the backend's request width) ---
        let started = Instant::now();
        let width = self.request_width();
        let threads = width.min(groups.len());
        let storage = self.storage.as_ref();
        let crypto = self.crypto.as_ref();
        let cache = &mut self.blob_cache;
        if threads <= 1 {
            for group in &groups {
                check_interrupted(shutdown)?;
                asm.accept(cache, fetch_group(storage, crypto, group)?)?;
            }
        } else {
            read_groups_parallel(width, storage, crypto, &groups, shutdown, |blobs| {
                asm.accept(cache, blobs)
            })?;
        }

        // Final drain (safety net for groups not sorted by earliest slot).
        asm.drain_ready();
        debug_assert_eq!(
            asm.next_emit,
            asm.slots.len(),
            "not all chunks were emitted"
        );

        let bytes: u64 = groups.iter().map(|g| g.read_end - g.read_start).sum();
        tracing::debug!(
            "coalesced read: {} chunks ({} misses) in {} range reads of {} bytes, width {}, {:?}",
            chunks.len(),
            misses,
            groups.len(),
            bytes,
            threads,
            started.elapsed()
        );
        Ok(())
    }
}

#[cfg(test)]
impl Repository {
    /// Clear the blob cache (test-only).
    pub fn clear_blob_cache(&mut self) {
        self.blob_cache = BlobCache::new(self.blob_cache.max_bytes);
    }
}
