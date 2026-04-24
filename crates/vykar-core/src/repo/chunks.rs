use std::sync::Arc;

use super::format::{pack_object_with_context, ObjectType};
use super::pack::{self, compute_data_pack_target, PackType, SealedPack};
use super::Repository;
use crate::compress;
use crate::index::PendingChunkEntry;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

impl Repository {
    /// Resolve a cache-hit chunk reference by bumping its refcount.
    /// Returns the stored size from the authoritative index/recovered entry.
    /// Errors if the chunk is not found in any state (committed, recovered,
    /// or pending pack writer) — this indicates storage corruption.
    pub fn reuse_cached_chunk_ref(&mut self, chunk_id: &ChunkId) -> Result<u32> {
        self.bump_ref_if_exists(chunk_id).ok_or_else(|| {
            VykarError::Other(format!(
                "cache hit references unresolvable chunk {chunk_id}"
            ))
        })
    }

    /// Increment the refcount for a chunk (works in both normal and dedup modes).
    pub fn increment_chunk_ref(&mut self, id: &ChunkId) {
        if let Some(ref mut ws) = self.write_session {
            if let Some(ref mut delta) = ws.index_delta {
                delta.bump_refcount(id);
                return;
            }
        }
        self.chunk_index.increment_refcount(id);
        self.index_dirty = true;
    }

    /// Increment refcount if this chunk already exists in committed or pending state.
    /// Returns stored size when found. Works in normal, dedup, and tiered modes.
    /// Falls back to recovered chunks from a previous interrupted session.
    pub fn bump_ref_if_exists(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        // Check dedup modes (write session required).
        let in_dedup_mode = {
            let ws = self
                .write_session
                .as_mut()
                .expect("no active write session");
            if let Some(ref tiered) = ws.tiered_dedup {
                if let Some(stored_size) = tiered.get_stored_size(chunk_id) {
                    if let Some(ref mut delta) = ws.index_delta {
                        delta.bump_refcount(chunk_id);
                    }
                    return Some(stored_size);
                }
                true
            } else if let Some(ref dedup) = ws.dedup_index {
                if let Some(stored_size) = dedup.get_stored_size(chunk_id) {
                    if let Some(ref mut delta) = ws.index_delta {
                        delta.bump_refcount(chunk_id);
                    }
                    return Some(stored_size);
                }
                true
            } else {
                false
            }
        };

        // Normal mode: check chunk_index (only when no dedup mode is active).
        if !in_dedup_mode {
            if let Some(entry) = self.chunk_index.get(chunk_id) {
                let stored_size = entry.stored_size;
                self.chunk_index.increment_refcount(chunk_id);
                self.index_dirty = true;
                return Some(stored_size);
            }
        }

        // Check recovered chunks before pending pack writers.
        if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
            return Some(stored_size);
        }

        self.bump_ref_pending(chunk_id)
    }

    /// Prefilter said "probably exists" — tiered: skip xor, check session_new → mmap → pending.
    /// Non-tiered: falls through to bump_ref_if_exists.
    pub fn bump_ref_prefilter_hit(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        let is_tiered = {
            let ws = self
                .write_session
                .as_mut()
                .expect("no active write session");
            if let Some(ref tiered) = ws.tiered_dedup {
                if let Some(stored_size) = tiered.get_stored_size_skip_filter(chunk_id) {
                    if let Some(ref mut delta) = ws.index_delta {
                        delta.bump_refcount(chunk_id);
                    }
                    return Some(stored_size);
                }
                true
            } else {
                false
            }
        };
        if is_tiered {
            if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
                return Some(stored_size);
            }
            return self.bump_ref_pending(chunk_id);
        }
        self.bump_ref_if_exists(chunk_id)
    }

    /// Prefilter said "definitely doesn't exist" — tiered: session_new → pending only.
    /// Non-tiered: falls through to bump_ref_if_exists.
    pub fn bump_ref_prefilter_miss(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        let is_tiered = {
            let ws = self
                .write_session
                .as_mut()
                .expect("no active write session");
            if let Some(ref tiered) = ws.tiered_dedup {
                if let Some(stored_size) = tiered.session_new_stored_size(chunk_id) {
                    if let Some(ref mut delta) = ws.index_delta {
                        delta.bump_refcount(chunk_id);
                    }
                    return Some(stored_size);
                }
                true
            } else {
                false
            }
        };
        if is_tiered {
            if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
                return Some(stored_size);
            }
            return self.bump_ref_pending(chunk_id);
        }
        self.bump_ref_if_exists(chunk_id)
    }

    /// Check only pending pack writers (shared helper).
    fn bump_ref_pending(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        self.write_session
            .as_mut()
            .expect("no active write session")
            .bump_ref_pending(chunk_id)
    }

    /// Inline false-positive path: compress + encrypt + commit a chunk whose ChunkId
    /// was already computed by the worker. Avoids re-hashing via `store_chunk`.
    pub fn commit_chunk_inline(
        &mut self,
        chunk_id: ChunkId,
        data: &[u8],
        compression: compress::Compression,
        pack_type: PackType,
    ) -> Result<u32> {
        debug_assert_eq!(
            ChunkId::compute(self.crypto.chunk_id_key(), data),
            chunk_id,
            "inline commit: chunk_id mismatch"
        );
        let compressed = compress::compress(compression, data)?;
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            &chunk_id.0,
            &compressed,
            self.crypto.as_ref(),
        )?;
        self.commit_prepacked_chunk(chunk_id, packed, pack_type)
    }

    /// Commit a pre-compressed and pre-encrypted chunk to the selected pack writer.
    /// Returns the stored size in bytes.
    pub fn commit_prepacked_chunk(
        &mut self,
        chunk_id: ChunkId,
        packed: Vec<u8>,
        pack_type: PackType,
    ) -> Result<u32> {
        let stored_size = packed.len() as u32;

        // Add blob and check flush in a scoped borrow
        let should_flush = {
            let ws = self
                .write_session
                .as_mut()
                .expect("no active write session");
            let writer = match pack_type {
                PackType::Data => &mut ws.data_pack_writer,
                PackType::Tree => &mut ws.tree_pack_writer,
            };
            writer.add_blob(chunk_id, packed)?;
            writer.should_flush()
        };

        if should_flush {
            self.flush_writer_async(pack_type)?;
        }

        Ok(stored_size)
    }

    /// Update index entries for a freshly sealed pack.
    fn apply_sealed_entries(
        &mut self,
        pack_id: vykar_types::pack_id::PackId,
        entries: Vec<pack::PackedChunkEntry>,
    ) {
        if self
            .write_session
            .as_mut()
            .expect("no active write session")
            .apply_sealed_entries(pack_id, entries, &mut self.chunk_index)
        {
            self.index_dirty = true;
        }
    }

    /// Seal a pack writer and upload in the background.
    /// The index is updated immediately; the upload proceeds in a separate thread.
    ///
    /// The `SealedPack` is destructured: `entries` consumed on the main thread
    /// for index updates, `data` (owning the mmap or Vec) moved into the upload
    /// thread. `pack_id` is `Copy` so it's used in both places.
    pub(super) fn flush_writer_async(&mut self, pack_type: PackType) -> Result<()> {
        // Keep upload fan-out bounded to avoid excessive memory/thread pressure.
        self.cap_pending_uploads()?;

        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        let SealedPack {
            pack_id,
            entries,
            data,
        } = match pack_type {
            PackType::Data => ws.data_pack_writer.seal()?,
            PackType::Tree => ws.tree_pack_writer.seal()?,
        };

        // Recalculate data pack target after each data pack flush.
        if pack_type == PackType::Data {
            ws.session_packs_flushed += 1;
            let total = ws.persisted_pack_count + ws.session_packs_flushed;
            let new_target = compute_data_pack_target(
                total,
                self.config.min_pack_size,
                self.config.max_pack_size,
            );
            ws.data_pack_writer.set_target_size(new_target);
        }

        // Record journal entries before apply_sealed_entries consumes them.
        let journal_chunks: Vec<PendingChunkEntry> = entries
            .iter()
            .map(
                |&(chunk_id, stored_size, offset, _refcount)| PendingChunkEntry {
                    chunk_id,
                    stored_size,
                    pack_offset: offset,
                },
            )
            .collect();
        ws.pending_journal.record_pack(pack_id, journal_chunks);

        // Record pack ID for rollback tracking (dump / per-file backup).
        if let Some(ref mut tracker) = ws.rollback_tracker {
            tracker.journal_pack_ids.push(pack_id);
        }

        // Release ws borrow before apply_sealed_entries (which needs &mut self).

        self.apply_sealed_entries(pack_id, entries);

        let storage = Arc::clone(&self.storage);
        let key = pack_id.storage_key();
        self.write_session
            .as_mut()
            .unwrap()
            .pending_uploads
            .push(std::thread::spawn(move || data.put_to(&*storage, &key)));

        Ok(())
    }

    /// Store a chunk in the repository. Returns (chunk_id, stored_size, was_new).
    /// If the chunk already exists (dedup), just increments the refcount.
    pub fn store_chunk(
        &mut self,
        data: &[u8],
        compression: compress::Compression,
        pack_type: PackType,
    ) -> Result<(ChunkId, u32, bool)> {
        let chunk_id = ChunkId::compute(self.crypto.chunk_id_key(), data);

        if let Some(stored_size) = self.bump_ref_if_exists(&chunk_id) {
            return Ok((chunk_id, stored_size, false));
        }

        // Compress
        let compressed = compress::compress(compression, data)?;

        // Encrypt and wrap in repo object envelope
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            &chunk_id.0,
            &compressed,
            self.crypto.as_ref(),
        )?;
        let stored_size = self.commit_prepacked_chunk(chunk_id, packed, pack_type)?;

        Ok((chunk_id, stored_size, true))
    }

    /// Flush all pending pack writes and wait for background uploads.
    /// No-op when no write session is active.
    pub fn flush_packs(&mut self) -> Result<()> {
        let Some(ws) = self.write_session.as_ref() else {
            return Ok(());
        };
        let flush_data = ws.data_pack_writer.has_pending();
        let flush_tree = ws.tree_pack_writer.has_pending();

        if flush_data {
            self.flush_writer_async(PackType::Data)?;
        }
        if flush_tree {
            self.flush_writer_async(PackType::Tree)?;
        }
        // Wait for all background uploads to complete before returning.
        self.write_session
            .as_mut()
            .unwrap()
            .wait_pending_uploads(&*self.storage, &*self.crypto)?;
        Ok(())
    }
}
