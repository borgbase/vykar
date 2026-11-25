use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::Repo;
use super::format::{ObjectType, pack_object_with_context};
use super::pack::{ClosedPack, PackType, compute_data_pack_target};
use super::write_session::{WriteSession, duration_nanos};
use crate::compress;
use crate::index::table::DedupPrefilter;
use crate::snapshot::item::ChunkRef;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

/// A chunk a backup worker prepared (compressed and encrypted).
pub(crate) struct PreparedChunk {
    pub(crate) chunk_id: ChunkId,
    pub(crate) uncompressed_size: u32,
    pub(crate) packed: Vec<u8>,
}

/// Where the worker found an existing chunk in the session's view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExistingAt {
    /// At this position of the view's table, so the consumer can record the
    /// reuse without a second lookup.
    Table(u32),
    /// Among the additions of the view's segments: reuse recorded by key.
    Added,
}

/// A chunk the worker confirmed in the session's view.
pub(crate) struct ExistingChunk {
    pub(crate) chunk_id: ChunkId,
    pub(crate) uncompressed_size: u32,
    pub(crate) stored_size: u32,
    pub(crate) at: ExistingAt,
}

/// What a backup worker hands the session for one chunk (the result of
/// worker-side classify).
pub(crate) enum WorkerChunk {
    /// Not in the session's table, or no filter: already
    /// compressed+encrypted. The consumer still dedups it against chunks
    /// added earlier in this session.
    Prepared(PreparedChunk),
    /// Confirmed dedup hit in the session's table: no data retained.
    Existing(ExistingChunk),
}

/// A chunk for [`WriteSession::store`].
pub(crate) enum ChunkInput<'a> {
    /// Classified by a backup worker. `prefilter` is the one the worker
    /// classified with: with one, a `Prepared` chunk is known to be outside
    /// the session's table and an `Existing` hit was found through it;
    /// without one every chunk is `Prepared`.
    Worker {
        chunk: WorkerChunk,
        prefilter: Option<&'a DedupPrefilter>,
    },
    /// Plain bytes from the calling thread (the item stream, command
    /// dumps): hashed here, looked up, and compressed and encrypted only
    /// when the chunk is new.
    Plain {
        data: &'a [u8],
        compression: compress::Compression,
    },
}

/// What [`WriteSession::store`] did with a chunk.
pub(crate) struct StoredChunk {
    /// `size`: plaintext length; `csize`: stored size.
    pub(crate) chunk_ref: ChunkRef,
    /// Packed by this call: found neither in the session, nor in its view
    /// of the index, nor among recovered or pending chunks.
    pub(crate) new: bool,
}

impl WriteSession {
    /// The one chunk-store entry: record the reuse of an existing chunk,
    /// or pack a new one into `pack_type`'s writer (a new tree chunk also
    /// goes to the local tree-chunk cache).
    pub(crate) fn store(
        &mut self,
        repo: &Repo,
        chunk: ChunkInput<'_>,
        pack_type: PackType,
    ) -> Result<StoredChunk> {
        match chunk {
            ChunkInput::Worker {
                chunk: WorkerChunk::Prepared(p),
                prefilter,
            } => {
                // With a prefilter, the worker established the chunk is not
                // in the session's view; it can only dedup against chunks
                // added during this session.
                let existing = if prefilter.is_some() {
                    self.reuse_after_prefilter_miss(repo, &p.chunk_id)
                } else {
                    self.reuse_if_exists(repo, &p.chunk_id)
                };
                let (csize, new) = match existing {
                    Some(csize) => (csize, false),
                    None => (self.add_new(repo, p.chunk_id, p.packed, pack_type)?, true),
                };
                Ok(StoredChunk {
                    chunk_ref: ChunkRef {
                        id: p.chunk_id,
                        size: p.uncompressed_size,
                        csize,
                    },
                    new,
                })
            }
            ChunkInput::Worker {
                chunk: WorkerChunk::Existing(e),
                prefilter,
            } => {
                let prefilter = prefilter.ok_or_else(|| {
                    VykarError::Other(format!(
                        "BUG: pre-session dedup hit for chunk {} without a prefilter",
                        e.chunk_id
                    ))
                })?;
                match e.at {
                    ExistingAt::Table(position) => {
                        self.reuse_table_hit(repo, &e.chunk_id, e.stored_size, position, prefilter)?
                    }
                    ExistingAt::Added => {
                        self.reuse_added_hit(repo, &e.chunk_id, e.stored_size, prefilter)?
                    }
                }
                Ok(StoredChunk {
                    chunk_ref: ChunkRef {
                        id: e.chunk_id,
                        size: e.uncompressed_size,
                        csize: e.stored_size,
                    },
                    new: false,
                })
            }
            ChunkInput::Plain { data, compression } => {
                let size = u32::try_from(data.len()).map_err(|_| {
                    VykarError::Other(format!("chunk of {} bytes is too large", data.len()))
                })?;
                let id = ChunkId::compute(repo.crypto.chunk_hasher(), data);
                if let Some(csize) = self.reuse_if_exists(repo, &id) {
                    return Ok(StoredChunk {
                        chunk_ref: ChunkRef { id, size, csize },
                        new: false,
                    });
                }
                let compressed = compress::compress(compression, data)?;
                // Encrypt and wrap in repo object envelope
                let packed = pack_object_with_context(
                    ObjectType::ChunkData,
                    id.as_bytes(),
                    &compressed,
                    repo.crypto.as_ref(),
                )?;
                let csize = self.add_new(repo, id, packed, pack_type)?;
                Ok(StoredChunk {
                    chunk_ref: ChunkRef { id, size, csize },
                    new: true,
                })
            }
        }
    }

    /// Pack a new chunk: a tree chunk first goes to the local tree-chunk
    /// cache (as stored bytes), then the blob goes to the writer.
    fn add_new(
        &mut self,
        repo: &Repo,
        chunk_id: ChunkId,
        packed: Vec<u8>,
        pack_type: PackType,
    ) -> Result<u32> {
        if pack_type == PackType::Tree
            && let Some(cache) = &repo.tree_cache
        {
            cache.insert(&chunk_id, &packed);
        }
        self.commit_packed(repo, chunk_id, packed, pack_type)
    }

    /// Store plain bytes through `Self::store`. Returns (chunk_id,
    /// stored_size, was_new). For tests and tools; backup code calls `store`.
    pub fn store_chunk(
        &mut self,
        repo: &Repo,
        data: &[u8],
        compression: compress::Compression,
        pack_type: PackType,
    ) -> Result<(ChunkId, u32, bool)> {
        let s = self.store(repo, ChunkInput::Plain { data, compression }, pack_type)?;
        Ok((s.chunk_ref.id, s.chunk_ref.csize, s.new))
    }

    /// Resolve a cache-hit chunk reference, recording the reuse.
    /// Returns the stored size from the authoritative index/recovered entry.
    /// Errors if the chunk is not found in any state (committed, recovered,
    /// or pending pack writer) — this indicates storage corruption.
    pub fn reuse_cached_chunk_ref(&mut self, repo: &Repo, chunk_id: &ChunkId) -> Result<u32> {
        self.reuse_if_exists(repo, chunk_id).ok_or_else(|| {
            VykarError::Other(format!(
                "cache hit references unresolvable chunk {chunk_id}"
            ))
        })
    }

    /// An existing chunk `id` for a new reference: stored earlier in the
    /// session, else (index unedited) in the session's view: an addition of
    /// its segments (recorded by key), or its table, found through the filter
    /// (unless `skip_filter`) and recorded by table position; else (index
    /// edited after the session began: repair, tests) in the live index by
    /// key. Records the reuse in the session delta and returns the stored
    /// size; `None` when not found.
    fn reuse_existing(&mut self, repo: &Repo, id: &ChunkId, skip_filter: bool) -> Option<u32> {
        let edited = repo.chunk_index.is_modified();
        let keyed_hit = if edited {
            repo.chunk_index.get(id).map(|e| e.stored_size)
        } else {
            None
        };
        let ws = self;
        let delta = ws.index_delta.as_mut()?;
        if let Some(&stored_size) = ws.session_new.get(id) {
            delta.reused.insert_key(id);
            return Some(stored_size);
        }
        if edited {
            let stored_size = keyed_hit?;
            delta.reused.insert_key(id);
            return Some(stored_size);
        }
        let view = &ws.view.as_ref()?.view;
        if let Some(stored_size) = view.find_added(id) {
            delta.reused.insert_key(id);
            return Some(stored_size);
        }
        let table = view.table();
        if !skip_filter
            && let Some(filter) = table.filter()
            && !xorf::Filter::contains(
                filter.as_ref(),
                &crate::index::dedup_cache::chunk_id_to_u64(id),
            )
        {
            return None;
        }
        let (position, record) = table.find(id)?;
        let stored_size = view.table_hit(position, &record)?;
        delta.reused.insert_position(id, position);
        Some(stored_size)
    }

    /// Record the reuse of this chunk if it already exists in committed or
    /// pending state. Returns stored size when found. Falls back to recovered
    /// chunks from a previous interrupted session, then to the pending pack
    /// writers.
    pub(super) fn reuse_if_exists(&mut self, repo: &Repo, chunk_id: &ChunkId) -> Option<u32> {
        if let Some(stored_size) = self.reuse_existing(repo, chunk_id, false) {
            return Some(stored_size);
        }
        if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
            return Some(stored_size);
        }
        self.find_pending(chunk_id)
    }

    /// A worker confirmed `chunk_id` in the session's table through
    /// `prefilter` (`PrefilterVerdict::Present`): record the reference.
    ///
    /// No second lookup is needed: `table_position` is where the worker found
    /// the chunk, and the reuse is recorded against it (the positional record
    /// re-checks that the entry there is `chunk_id` and otherwise records it
    /// by key). The verdict came from the very view the session
    /// resolves against (checked by identity), and that view is immutable —
    /// a chunk in it is never also in `session_new` or the recovered set,
    /// because every path consults the view before storing or promoting a
    /// chunk.
    ///
    /// Errors, rather than recording a possibly dangling reference, if the
    /// prefilter does not belong to the active session.
    fn reuse_table_hit(
        &mut self,
        repo: &Repo,
        chunk_id: &ChunkId,
        stored_size: u32,
        table_position: u32,
        prefilter: &DedupPrefilter,
    ) -> Result<()> {
        debug_assert_eq!(
            prefilter
                .view()
                .table()
                .find(chunk_id)
                .map(|(p, r)| (p, prefilter.view().table_hit(p, &r))),
            Some((table_position, Some(stored_size))),
            "pre-session hit must resolve to the worker's table entry"
        );
        let delta = self.session_delta_for(repo, chunk_id, prefilter)?;
        delta.reused.insert_position(chunk_id, table_position);
        Ok(())
    }

    /// A worker found `chunk_id` among the additions of the session's view
    /// (`PrefilterVerdict::PresentAdded`): record the reference by key.
    /// The same identity check as [`Self::reuse_table_hit`].
    fn reuse_added_hit(
        &mut self,
        repo: &Repo,
        chunk_id: &ChunkId,
        stored_size: u32,
        prefilter: &DedupPrefilter,
    ) -> Result<()> {
        debug_assert_eq!(
            prefilter.view().find_added(chunk_id),
            Some(stored_size),
            "pre-session hit must resolve to the worker's addition"
        );
        let delta = self.session_delta_for(repo, chunk_id, prefilter)?;
        delta.reused.insert_key(chunk_id);
        Ok(())
    }

    /// The session's delta, once `prefilter` is checked to be over the
    /// session's view.
    fn session_delta_for(
        &mut self,
        repo: &Repo,
        chunk_id: &ChunkId,
        prefilter: &DedupPrefilter,
    ) -> Result<&mut crate::index::IndexDelta> {
        // A pre-session hit comes from the session's immutable view. An index
        // edited during the session could have removed that entry.
        debug_assert!(
            !repo.chunk_index.is_modified(),
            "pre-session dedup hit over an index edited during the session"
        );
        let ws = self;
        if !ws
            .view
            .as_ref()
            .is_some_and(|v| v.view.same_as(prefilter.view()))
        {
            return Err(VykarError::Other(format!(
                "BUG: pre-session dedup hit for chunk {chunk_id} does not match \
                 the active session's index view"
            )));
        }
        ws.index_delta
            .as_mut()
            .ok_or_else(|| VykarError::Other("BUG: write session without an index delta".into()))
    }

    /// Prefilter said "not in the session's table" (`PrefilterVerdict::Absent`:
    /// a filter miss, or a false positive the worker ruled out in the table)
    /// — session-new → recovered → pending only.
    fn reuse_after_prefilter_miss(&mut self, repo: &Repo, chunk_id: &ChunkId) -> Option<u32> {
        let found = {
            let ws = &mut *self;
            match ws.session_new.get(chunk_id).copied() {
                Some(stored_size) => {
                    if let Some(ref mut delta) = ws.index_delta {
                        delta.reused.insert_key(chunk_id);
                    }
                    Some(stored_size)
                }
                None => None,
            }
        };
        if found.is_some() {
            return found;
        }
        if repo.chunk_index.is_modified() {
            // Edits after the session began are only seen by key.
            if let Some(stored_size) = self.reuse_existing(repo, chunk_id, true) {
                return Some(stored_size);
            }
        }
        if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
            return Some(stored_size);
        }
        self.find_pending(chunk_id)
    }

    /// Commit a pre-compressed and pre-encrypted chunk to the selected pack writer.
    /// Returns the stored size in bytes.
    fn commit_packed(
        &mut self,
        repo: &Repo,
        chunk_id: ChunkId,
        packed: Vec<u8>,
        pack_type: PackType,
    ) -> Result<u32> {
        let stored_size = packed.len() as u32;

        // Add blob and check flush in a scoped borrow
        let should_flush = {
            let ws = &mut *self;
            // Tree packs are small (<= 4 MiB) and stay on the heap.
            let (writer, stage) = match pack_type {
                PackType::Data => (
                    &mut ws.data_pack_writer,
                    ws.stream_data_packs.then_some(&*repo.storage),
                ),
                PackType::Tree => (&mut ws.tree_pack_writer, None),
            };
            writer.add_blob_to(stage, chunk_id, packed)?;
            writer.should_flush()
        };

        if should_flush {
            self.flush_writer_async(repo, pack_type)?;
        } else if self.uploads.any_finished() {
            // Journal confirmed packs as soon as their upload finishes rather
            // than at the next seal, so a SIGKILL loses as little as possible.
            self.drain_finished_uploads(&repo.storage, &repo.crypto)?;
        }

        Ok(stored_size)
    }

    /// Close a pack writer and upload the pack in the background.
    ///
    /// The pack's chunks become dedup hits at once (`session_new`); their
    /// index entries, the pack's trailer length and its journal entry follow
    /// once its upload is confirmed. The upload thread finishes the pack
    /// (encrypts and appends its trailer, computes its ID) and stores it:
    /// the heap buffer, or the staged temp file whose fsync + rename is the
    /// upload.
    pub(super) fn flush_writer_async(&mut self, repo: &Repo, pack_type: PackType) -> Result<()> {
        // Keep upload fan-out bounded to avoid excessive memory/thread pressure.
        let incoming = match pack_type {
            PackType::Data => self.data_pack_writer.buffer_capacity(),
            PackType::Tree => self.tree_pack_writer.buffer_capacity(),
        };
        let cap_start = Instant::now();
        self.cap_pending_uploads(&repo.storage, &repo.crypto, incoming)?;
        let cap_wait = cap_start.elapsed();

        let encrypting = repo.crypto.is_encrypting();
        let ws = &mut *self;
        let seal_start = Instant::now();
        let ClosedPack {
            entries,
            len: pack_bytes,
            trailer_len,
            tail,
        } = match pack_type {
            PackType::Data => ws.data_pack_writer.close(encrypting)?,
            PackType::Tree => ws.tree_pack_writer.close(encrypting)?,
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
                repo.config.min_pack_size,
                repo.config.max_pack_size,
            );
            ws.data_pack_writer.set_target_size(new_target);
        }

        self.record_closed(&entries);

        let bytes = tail.heap_bytes();
        let crypto = Arc::clone(&repo.crypto);
        let storage = Arc::clone(&repo.storage);
        let put_nanos = Arc::clone(&self.metrics.upload_put_nanos);
        let seal_nanos = Arc::clone(&self.metrics.upload_seal_nanos);
        let existing = self.existing_packs.clone();
        self.submit_upload(entries, trailer_len, bytes, move |held| {
            let (pack_id, data) = {
                let _span = tracing::debug_span!("pack_seal").entered();
                let start = Instant::now();
                let finished = tail.finish(&*crypto);
                seal_nanos.fetch_add(duration_nanos(start.elapsed()), Ordering::Relaxed);
                finished?
            };
            let key = pack_id.storage_key();
            let _span = tracing::debug_span!("pack_upload").entered();
            let start = Instant::now();
            let result = data.put_to(&*storage, &key, move || held.release());
            put_nanos.fetch_add(duration_nanos(start.elapsed()), Ordering::Relaxed);
            let result = match (result, existing) {
                (Err(e), Some(existing)) => existing.settle_refused(&*storage, &key, pack_bytes, e),
                (result, _) => result,
            };
            result.map(|()| pack_id)
        })
    }

    /// Flush all pending pack writes and wait for background uploads.
    pub fn flush_packs(&mut self, repo: &Repo) -> Result<()> {
        self.flush_packs_inner(repo, false)
    }

    /// [`Self::flush_packs`], but the session journal's final write is left
    /// running in the background (see
    /// `WriteSession::wait_pending_uploads_journal_in_background`):
    /// every pack is durable on return, the journal follows. The next
    /// `flush_packs` joins it.
    pub fn flush_packs_journal_in_background(&mut self, repo: &Repo) -> Result<()> {
        self.flush_packs_inner(repo, true)
    }

    fn flush_packs_inner(&mut self, repo: &Repo, background_journal: bool) -> Result<()> {
        let flush_data = self.data_pack_writer.has_pending();
        let flush_tree = self.tree_pack_writer.has_pending();

        if flush_data {
            self.flush_writer_async(repo, PackType::Data)?;
        }
        if flush_tree {
            self.flush_writer_async(repo, PackType::Tree)?;
        }
        // Wait for all background uploads to complete before returning.
        let wait_start = Instant::now();
        let result = if background_journal {
            self.wait_pending_uploads_journal_in_background(&repo.storage, &repo.crypto)
        } else {
            self.wait_pending_uploads(&*repo.storage, &*repo.crypto)
        };
        self.metrics.counters.final_upload_wait += wait_start.elapsed();
        result
    }
}
