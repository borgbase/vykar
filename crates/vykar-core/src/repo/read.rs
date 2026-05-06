use super::format::{unpack_object_expect_with_context, ObjectType};
use super::pack::read_blob_from_pack;
use super::{BlobCache, Repository};
use crate::compress;
use crate::index::dedup_cache;
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

        self.blob_cache.insert(*chunk_id, plaintext.clone());
        Ok(plaintext)
    }

    /// Maximum gap (in bytes) between two blobs in the same pack that will be
    /// merged into a single range read.
    const COALESCE_GAP: u64 = 256 * 1024; // 256 KiB

    /// Maximum total size of a single coalesced range read.
    const COALESCE_MAX: u64 = 16 * 1024 * 1024; // 16 MiB

    /// Read multiple chunks via coalesced range reads and append plaintext to `out`.
    ///
    /// Each entry in `chunks` is `(ChunkId, PackId, pack_offset, stored_size)`.
    /// Output is appended to `out` in the same order as `chunks`.
    /// Cache hits are served from `blob_cache`; misses are grouped by pack and
    /// coalesced into large range reads to minimise HTTP round-trips.
    pub fn read_chunks_coalesced_into(
        &mut self,
        chunks: &[(ChunkId, PackId, u64, u32)],
        out: &mut Vec<u8>,
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

        for (idx, (chunk_id, pack_id, pack_offset, stored_size)) in chunks.iter().enumerate() {
            if let Some(cached) = self.blob_cache.get(chunk_id) {
                slots.push(Some(cached.to_vec()));
            } else {
                slots.push(None);
                pack_blobs.entry(*pack_id).or_default().push(CoalescedBlob {
                    result_idx: idx,
                    chunk_id: *chunk_id,
                    pack_offset: *pack_offset,
                    stored_size: *stored_size,
                });
            }
        }

        // Drain contiguous completed slots into `out`, advancing the cursor.
        let drain_ready =
            |slots: &mut Vec<Option<Vec<u8>>>, next_emit: &mut usize, out: &mut Vec<u8>| {
                while let Some(slot) = slots.get_mut(*next_emit) {
                    if let Some(data) = slot.take() {
                        out.extend_from_slice(&data);
                        *next_emit += 1;
                    } else {
                        break;
                    }
                }
            };

        let mut next_emit: usize = 0;
        drain_ready(&mut slots, &mut next_emit, out);

        // All cache hits — done.
        if pack_blobs.is_empty() {
            return Ok(());
        }

        // --- Phase 2: coalesce ---
        let mut groups: Vec<CoalescedGroup> = Vec::new();

        for (pack_id, mut blobs) in pack_blobs {
            blobs.sort_by_key(|b| b.pack_offset);

            let mut iter = blobs.into_iter();
            let first = iter.next().expect("pack blob group is non-empty");
            let mut cur_start = first.pack_offset;
            let mut cur_end = first.pack_offset + first.stored_size as u64;
            let mut cur_blobs = vec![first];

            for blob in iter {
                let blob_end = blob.pack_offset + blob.stored_size as u64;
                let gap = blob.pack_offset.saturating_sub(cur_end);
                let merged_size = blob_end - cur_start;

                if gap <= Self::COALESCE_GAP && merged_size <= Self::COALESCE_MAX {
                    cur_end = blob_end;
                    cur_blobs.push(blob);
                } else {
                    groups.push(CoalescedGroup {
                        pack_id,
                        read_start: cur_start,
                        read_end: cur_end,
                        blobs: cur_blobs,
                    });
                    cur_start = blob.pack_offset;
                    cur_end = blob_end;
                    cur_blobs = vec![blob];
                }
            }
            groups.push(CoalescedGroup {
                pack_id,
                read_start: cur_start,
                read_end: cur_end,
                blobs: cur_blobs,
            });
        }

        // Sort groups so the one containing the earliest-needed slot is first.
        groups.sort_by_key(|g| {
            g.blobs
                .iter()
                .map(|b| b.result_idx)
                .min()
                .expect("coalesced group is non-empty")
        });

        // --- Phase 3: read + decrypt + incremental drain ---
        for group in groups {
            let pack_key = group.pack_id.storage_key();
            let read_len = group.read_end - group.read_start;

            let raw_data = self
                .storage
                .get_range(&pack_key, group.read_start, read_len)?
                .ok_or_else(|| VykarError::Other(format!("pack not found: {}", group.pack_id)))?;

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
                    self.crypto.as_ref(),
                )?;
                let plaintext = compress::decompress(&compressed)?;

                self.blob_cache.insert(blob.chunk_id, plaintext.clone());
                let slot = slots.get_mut(blob.result_idx).ok_or_else(|| {
                    VykarError::Other(format!(
                        "internal error: read result index {} out of range",
                        blob.result_idx
                    ))
                })?;
                *slot = Some(plaintext);

                drain_ready(&mut slots, &mut next_emit, out);
            }
        }

        // Final drain (safety net for groups not sorted by earliest slot).
        drain_ready(&mut slots, &mut next_emit, out);

        debug_assert_eq!(next_emit, slots.len(), "not all chunks were emitted");
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
