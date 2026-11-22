use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::Repository;
use super::format::{ObjectType, pack_object_with_context};
use super::pack::{self, PackType, SealedPack, compute_data_pack_target};
use super::write_session::{active_mut, duration_nanos};
use crate::compress;
use crate::index::PendingChunkEntry;
use crate::index::table::DedupPrefilter;
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

    /// Record a new reference to a chunk in the session's delta (the reused
    /// set a commit checks). Without a session it does nothing: refcounts
    /// are not persisted (05 step 8).
    pub fn increment_chunk_ref(&mut self, id: &ChunkId) {
        if let Some(ref mut ws) = self.write_session
            && let Some(ref mut delta) = ws.index_delta
        {
            delta.bump_refcount(id);
        }
    }

    /// An existing chunk `id` for a new reference: stored earlier in the
    /// session, else (index unedited) in the session's table, found through
    /// the filter (unless `skip_filter`) and recorded by table position, else
    /// (index edited after the session began: repair, tests) in the live
    /// index by key. Records the bump in the session delta and returns the
    /// stored size; `None` when not found.
    fn bump_existing(&mut self, id: &ChunkId, skip_filter: bool) -> Option<u32> {
        let edited = self.chunk_index.is_modified();
        let keyed_hit = if edited {
            self.chunk_index.get(id).map(|e| e.stored_size)
        } else {
            None
        };
        let ws = self.ws_mut();
        let delta = ws.index_delta.as_mut()?;
        if let Some(&stored_size) = ws.session_new.get(id) {
            delta.bump_refcount(id);
            return Some(stored_size);
        }
        if edited {
            let stored_size = keyed_hit?;
            delta.bump_refcount(id);
            return Some(stored_size);
        }
        let table = ws.table.as_ref()?;
        if !skip_filter
            && let Some(filter) = table.filter()
            && !xorf::Filter::contains(
                filter.as_ref(),
                &crate::index::dedup_cache::chunk_id_to_u64(id),
            )
        {
            return None;
        }
        let (position, entry) = table.find(id)?;
        delta.bump_cached_refcount(id, position);
        Some(entry.stored_size)
    }

    /// Increment refcount if this chunk already exists in committed or pending state.
    /// Returns stored size when found. Falls back to recovered chunks from a
    /// previous interrupted session, then to the pending pack writers.
    /// Without a write session, only looks the chunk up in the index:
    /// refcounts are not persisted (05 step 8), so nothing is recorded.
    pub fn bump_ref_if_exists(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        if self.write_session.is_none() {
            return self.chunk_index.get(chunk_id).map(|e| e.stored_size);
        }
        if let Some(stored_size) = self.bump_existing(chunk_id, false) {
            return Some(stored_size);
        }
        if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
            return Some(stored_size);
        }
        self.bump_ref_pending(chunk_id)
    }

    /// A worker confirmed `chunk_id` in the session's table through
    /// `prefilter` (`PrefilterVerdict::Present`): record the reference.
    ///
    /// No second lookup is needed: `table_position` is where the worker found
    /// the chunk, and the bump is recorded against it (the positional table
    /// re-checks that the entry there is `chunk_id` and otherwise records the
    /// bump by key). The verdict came from the very table the session
    /// resolves against (checked by identity), and that table is immutable —
    /// a chunk in it is never also in `session_new` or the recovered set,
    /// because every path consults the table before storing or promoting a
    /// chunk.
    ///
    /// Errors, rather than recording a possibly dangling reference, if the
    /// prefilter does not belong to the active session.
    pub(crate) fn bump_ref_presession_hit(
        &mut self,
        chunk_id: &ChunkId,
        stored_size: u32,
        table_position: u32,
        prefilter: &DedupPrefilter,
    ) -> Result<()> {
        // A positional hit comes from the session's immutable table. An index
        // edited during the session could have removed that entry.
        debug_assert!(
            !self.chunk_index.is_modified(),
            "pre-session dedup hit over an index edited during the session"
        );
        let ws = self.ws_mut();
        let table = ws
            .table
            .as_ref()
            .filter(|t| Arc::ptr_eq(t, prefilter.table()))
            .ok_or_else(|| {
                VykarError::Other(format!(
                    "BUG: pre-session dedup hit for chunk {chunk_id} does not match \
                     the active session's table"
                ))
            })?;
        debug_assert_eq!(
            table.find(chunk_id).map(|(p, e)| (p, e.stored_size)),
            Some((table_position, stored_size)),
            "pre-session hit must resolve to the worker's table entry"
        );
        let delta = ws
            .index_delta
            .as_mut()
            .ok_or_else(|| VykarError::Other("BUG: write session without an index delta".into()))?;
        delta.bump_cached_refcount(chunk_id, table_position);
        Ok(())
    }

    /// Prefilter said "not in the session's table" (`PrefilterVerdict::Absent`:
    /// a filter miss, or a false positive the worker ruled out in the table)
    /// — session-new → recovered → pending only.
    pub fn bump_ref_prefilter_miss(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        let found = {
            let ws = self.ws_mut();
            match ws.session_new.get(chunk_id).copied() {
                Some(stored_size) => {
                    if let Some(ref mut delta) = ws.index_delta {
                        delta.bump_refcount(chunk_id);
                    }
                    Some(stored_size)
                }
                None => None,
            }
        };
        if found.is_some() {
            return found;
        }
        if self.chunk_index.is_modified() {
            // Edits after the session began are only seen by key.
            if let Some(stored_size) = self.bump_existing(chunk_id, true) {
                return Some(stored_size);
            }
        }
        if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
            return Some(stored_size);
        }
        self.bump_ref_pending(chunk_id)
    }

    /// Check only pending pack writers (shared helper).
    fn bump_ref_pending(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        self.ws_mut().bump_ref_pending(chunk_id)
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
            ChunkId::compute(self.crypto.chunk_hasher(), data),
            chunk_id,
            "inline commit: chunk_id mismatch"
        );
        let compressed = compress::compress(compression, data)?;
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            chunk_id.as_bytes(),
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
            let ws = active_mut(&mut self.write_session);
            // Tree packs are small (<= 4 MiB) and stay on the heap.
            let (writer, stage) = match pack_type {
                PackType::Data => (
                    &mut ws.data_pack_writer,
                    ws.stream_data_packs.then_some(&*self.storage),
                ),
                PackType::Tree => (&mut ws.tree_pack_writer, None),
            };
            writer.add_blob_to(stage, chunk_id, packed)?;
            writer.should_flush()
        };

        if should_flush {
            self.flush_writer_async(pack_type)?;
        } else {
            // Journal confirmed packs as soon as their upload finishes rather
            // than at the next seal, so a SIGKILL loses as little as possible.
            let ws = active_mut(&mut self.write_session);
            if ws.uploads.any_finished() {
                ws.drain_finished_uploads(&self.storage, &self.crypto)?;
            }
        }

        Ok(stored_size)
    }

    /// Update index entries for a freshly sealed pack.
    fn apply_sealed_entries(
        &mut self,
        pack_id: vykar_types::pack_id::PackId,
        entries: Vec<pack::PackedChunkEntry>,
    ) {
        active_mut(&mut self.write_session).apply_sealed_entries(pack_id, entries);
    }

    /// Seal a pack writer and upload in the background.
    /// The index is updated immediately; the upload proceeds in a separate thread.
    ///
    /// The `SealedPack` is destructured: `entries` consumed on the main thread
    /// for index updates, `data` (the heap buffer, or the staged temp file
    /// whose fsync + rename is the upload) moved into the upload thread.
    /// `pack_id` is `Copy` so it's used in both places.
    pub(super) fn flush_writer_async(&mut self, pack_type: PackType) -> Result<()> {
        // Keep upload fan-out bounded to avoid excessive memory/thread pressure.
        let incoming = {
            let ws = self.ws();
            match pack_type {
                PackType::Data => ws.data_pack_writer.buffer_capacity(),
                PackType::Tree => ws.tree_pack_writer.buffer_capacity(),
            }
        };
        let cap_start = Instant::now();
        self.cap_pending_uploads(incoming)?;
        let cap_wait = cap_start.elapsed();

        let crypto = Arc::clone(&self.crypto);
        let ws = active_mut(&mut self.write_session);
        let seal_start = Instant::now();
        let SealedPack {
            pack_id,
            entries,
            data,
            len: pack_bytes,
        } = match pack_type {
            PackType::Data => ws.data_pack_writer.seal(crypto.as_ref())?,
            PackType::Tree => ws.tree_pack_writer.seal(crypto.as_ref())?,
        };
        let seal_time = seal_start.elapsed();

        let counters = &mut ws.metrics.counters;
        counters.upload_cap_wait += cap_wait;
        counters.seal += seal_time;
        match pack_type {
            PackType::Data => {
                counters.data_packs += 1;
                counters.data_pack_bytes += pack_bytes;
            }
            PackType::Tree => {
                counters.tree_packs += 1;
                counters.tree_pack_bytes += pack_bytes;
            }
        }

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
        // Record pack ID for rollback tracking (dump / per-file backup). The
        // journal entry itself waits for the upload to be confirmed.
        if let Some(ref mut tracker) = ws.rollback_tracker {
            tracker.journal_pack_ids.push(pack_id);
        }

        // Release ws borrow before apply_sealed_entries (which needs &mut self).

        self.apply_sealed_entries(pack_id, entries);

        let bytes = data.capacity();
        let storage = Arc::clone(&self.storage);
        let key = pack_id.storage_key();
        let put_nanos = Arc::clone(&self.ws().metrics.upload_put_nanos);
        self.ws_mut()
            .spawn_upload(pack_id, journal_chunks, bytes, move |held| {
                let _span = tracing::debug_span!("pack_upload").entered();
                let start = Instant::now();
                let result = data.put_to(&*storage, &key, move || held.release());
                put_nanos.fetch_add(duration_nanos(start.elapsed()), Ordering::Relaxed);
                result
            });

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
        let chunk_id = ChunkId::compute(self.crypto.chunk_hasher(), data);

        if let Some(stored_size) = self.bump_ref_if_exists(&chunk_id) {
            return Ok((chunk_id, stored_size, false));
        }

        // Compress
        let compressed = compress::compress(compression, data)?;

        // Encrypt and wrap in repo object envelope
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            chunk_id.as_bytes(),
            &compressed,
            self.crypto.as_ref(),
        )?;
        // New tree chunks go to the local tree-chunk cache as they are
        // written; dedup hits returned above and are not cached.
        if pack_type == PackType::Tree
            && let Some(cache) = &self.tree_cache
        {
            cache.insert(&chunk_id, &packed);
        }
        let stored_size = self.commit_prepacked_chunk(chunk_id, packed, pack_type)?;

        Ok((chunk_id, stored_size, true))
    }

    /// Flush all pending pack writes and wait for background uploads.
    /// No-op when no write session is active.
    pub fn flush_packs(&mut self) -> Result<()> {
        self.flush_packs_inner(false)
    }

    /// [`Self::flush_packs`], but the session journal's final write is left
    /// running in the background (see
    /// `WriteSessionState::wait_pending_uploads_journal_in_background`):
    /// every pack is durable on return, the journal follows. The next
    /// `flush_packs` joins it.
    pub fn flush_packs_journal_in_background(&mut self) -> Result<()> {
        self.flush_packs_inner(true)
    }

    fn flush_packs_inner(&mut self, background_journal: bool) -> Result<()> {
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
        let ws = active_mut(&mut self.write_session);
        let wait_start = Instant::now();
        let result = if background_journal {
            ws.wait_pending_uploads_journal_in_background(&self.storage, &self.crypto)
        } else {
            ws.wait_pending_uploads(&*self.storage, &*self.crypto)
        };
        ws.metrics.counters.final_upload_wait += wait_start.elapsed();
        result
    }
}
