use chrono::Utc;
use rand::RngCore;
use tracing::{debug, warn};

use super::file_cache;
use super::format::{
    pack_object_streaming_with_context, unpack_object_expect_with_context, ObjectType,
};
use super::manifest::{self, Manifest};
use super::snapshot_cache;
use super::{Repository, INDEX_OBJECT_CONTEXT};
use crate::compress;
use crate::index::dedup_cache;
use crate::index::{ChunkIndex, IndexBlob, IndexBlobRef, IndexDelta};
use vykar_crypto::CryptoEngine;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// Emit a `CommitStage` progress event and return a context
/// (start time, stage name) for later `log_stage_elapsed`.
fn emit_stage<F>(
    progress: &mut Option<F>,
    stage: &'static str,
) -> (std::time::Instant, &'static str)
where
    F: FnMut(crate::commands::backup::BackupProgressEvent),
{
    let start = std::time::Instant::now();
    if let Some(ref mut cb) = progress {
        cb(crate::commands::backup::BackupProgressEvent::CommitStage { stage });
    }
    (start, stage)
}

fn log_stage_elapsed(ctx: (std::time::Instant, &'static str)) {
    debug!(
        stage = ctx.1,
        elapsed_ms = ctx.0.elapsed().as_millis() as u64,
        "commit stage complete"
    );
}

impl Repository {
    /// Load the chunk index from storage on demand.
    /// Always downloads the remote index blob to get the authenticated generation.
    /// Can be called after opening without `load_index` to lazily load the index.
    /// Also recalculates the data pack writer target from the loaded index.
    pub fn load_chunk_index(&mut self) -> Result<()> {
        let (gen, index) = self.reload_full_index_with_generation()?;
        self.index_generation = gen;
        self.chunk_index = index;
        // Best-effort rewrite index.gen
        let _ = self.storage.put("index.gen", &gen.to_le_bytes());
        self.rebase_pack_target_from_index();
        Ok(())
    }

    /// Load the chunk index from storage, bypassing the local blob cache.
    /// Use this for operations like `check` that must verify what's actually
    /// in the remote repository.
    ///
    /// NOTE: Does not update `index_generation` — only suitable for read-only
    /// operations. Use `load_chunk_index()` for write paths.
    pub fn load_chunk_index_uncached(&mut self) -> Result<()> {
        self.chunk_index = self.reload_full_index()?;
        self.rebase_pack_target_from_index();
        Ok(())
    }

    /// Flush pending packs, wait for uploads, and apply the dedup delta from
    /// the active write session. Must only be called when `write_session` is `Some`.
    ///
    /// On success, the session's dedup structures and delta are consumed
    /// (moved into the chunk index or cache). On failure, the session remains
    /// active (not consumed) so the caller can invoke `flush_on_abort()` for
    /// best-effort journal persistence. Note that `tiered_dedup` and
    /// `index_delta` may already be taken out of the session at that point —
    /// `flush_on_abort` does not need them (it only seals packs, joins uploads,
    /// and writes the journal).
    /// Returns `true` when chunk_index hydration should be deferred to after
    /// persistence (reduces peak memory when incremental update or cache
    /// rebuild succeeds).
    fn apply_write_session(&mut self) -> Result<bool> {
        // Flush all pending packs and wait for uploads.
        self.flush_packs()?;

        // Drop tiered dedup index (releases mmap) before reloading full index.
        // Take delta and dedup_index out of the session for processing.
        let ws = self
            .write_session
            .as_mut()
            .expect("write session active while applying commit");
        ws.tiered_dedup.take();
        let delta = ws.index_delta.take();
        if delta.is_some() {
            ws.dedup_index = None;
        }

        let mut deferred_index_load = false;

        if let Some(delta) = delta {
            if !delta.is_empty() {
                // Download fresh remote index and apply delta.
                // No reconcile() needed: callers use with_maintenance_lock()
                // which guarantees no concurrent sessions are active.
                let mut full_index = self.reload_full_index()?;
                delta.apply_to(&mut full_index);
                self.chunk_index = full_index;
                self.index_dirty = true;
            } else if self.rebuild_dedup_cache {
                // Empty delta: index unchanged, but caches may need rebuilding.
                // Try to rebuild caches from full_index_cache if available.
                let mut rebuilt_from_cache = false;
                let cd = self.cache_dir_override.as_deref();
                if let Some(cache_path) = dedup_cache::full_index_cache_path(&self.config.id, cd) {
                    if dedup_cache::MmapFullIndexCache::open(
                        &self.config.id,
                        self.index_generation,
                        cd,
                    )
                    .is_some()
                    {
                        let gen = self.index_generation;
                        let id = &self.config.id;
                        let dedup_ok = dedup_cache::build_dedup_cache_from_full_cache(
                            &cache_path,
                            gen,
                            id,
                            cd,
                        )
                        .is_ok();
                        let restore_ok = dedup_cache::build_restore_cache_from_full_cache(
                            &cache_path,
                            gen,
                            id,
                            cd,
                        )
                        .is_ok();
                        if dedup_ok && restore_ok {
                            self.rebuild_dedup_cache = false;
                            rebuilt_from_cache = true;
                            // Defer chunk_index hydration to reduce peak memory.
                            deferred_index_load = true;
                        } else {
                            warn!(
                                "cache rebuild from full_index_cache partially failed, falling back"
                            );
                        }
                    }
                }
                if !rebuilt_from_cache {
                    // No valid full cache — must reload full index for slow-path cache rebuild.
                    self.chunk_index = self.reload_full_index()?;
                }
            } else {
                // Empty delta, no cache rebuild needed.
                // Still must restore chunk_index for postcondition (deferred).
                deferred_index_load = true;
            }
        }

        Ok(deferred_index_load)
    }

    /// Save the chunk index back to storage.
    /// When a write session is active: flushes pending packs, applies dedup delta.
    /// When no session is active: just persists dirty index/file_cache.
    /// Only writes components that have been marked dirty.
    pub fn save_state(&mut self) -> Result<()> {
        let deferred_index_load = if self.write_session.is_some() {
            self.apply_write_session()?
        } else {
            false
        };

        // When the index changes, rotate index_generation so the local dedup
        // cache is invalidated.
        if self.index_dirty {
            self.index_generation = rand::rng().next_u64();
        }

        if self.index_dirty {
            self.persist_index()?;
        }

        if self.rebuild_dedup_cache {
            self.rebuild_local_caches(false);
            self.rebuild_dedup_cache = false;
        }

        // Save file cache before hydrating chunk_index to reduce peak memory.
        // Capture error instead of early-returning so we can hydrate first.
        let fc_result = self.save_file_cache_if_dirty();

        // Always hydrate chunk_index — postcondition: self.chunk_index is valid
        // on all exit paths (success and error).
        if deferred_index_load {
            // Try local full_index_cache first (fast, no storage round-trip),
            // fall back to reloading from remote storage if cache is unavailable.
            self.chunk_index = dedup_cache::load_chunk_index_from_full_cache(
                &self.config.id,
                self.index_generation,
                self.cache_dir_override.as_deref(),
            )
            .or_else(|_| self.reload_full_index())?;
        }

        // Now propagate any file cache save error
        fc_result?;

        // Consume the write session on success — all entries are now in the
        // persisted index. The pending_index file itself is deleted later by
        // clear_pending_index() from the backup command.
        self.write_session = None;

        Ok(())
    }

    /// Commit a concurrent backup session. Called while holding the exclusive lock.
    ///
    /// 1. Flush packs and join uploads.
    /// 2. Take the delta from the write session.
    /// 3. Refresh snapshot list from storage.
    /// 4. Check snapshot name uniqueness against fresh list.
    /// 5. Download fresh remote index, reconcile delta, persist index.
    /// 6. Write `snapshots/<id>` to storage — **commit point**.
    /// 7. Update local manifest + snapshot cache.
    /// 8. Save file cache and consume write session.
    pub fn commit_concurrent_session(
        &mut self,
        snapshot_entry: manifest::SnapshotEntry,
        snapshot_packed: Vec<u8>,
        new_file_cache: &mut file_cache::FileCache,
    ) -> Result<()> {
        self.commit_concurrent_session_with_progress(
            snapshot_entry,
            snapshot_packed,
            new_file_cache,
            &mut None::<Box<dyn FnMut(crate::commands::backup::BackupProgressEvent)>>,
        )
    }

    /// Commit a concurrent backup session with progress reporting.
    ///
    /// 1. Flush packs and join uploads.
    /// 2. Take the delta from the write session.
    /// 3. Refresh snapshot list from storage.
    /// 4. Check snapshot name uniqueness against fresh list.
    /// 5. Download fresh remote index, reconcile delta, persist index.
    /// 6. Write `snapshots/<id>` to storage — **commit point**.
    /// 7. Update local manifest + snapshot cache.
    /// 8. Save file cache and consume write session.
    pub fn commit_concurrent_session_with_progress(
        &mut self,
        snapshot_entry: manifest::SnapshotEntry,
        snapshot_packed: Vec<u8>,
        new_file_cache: &mut file_cache::FileCache,
        progress: &mut Option<impl FnMut(crate::commands::backup::BackupProgressEvent)>,
    ) -> Result<()> {
        // 0. Session-existence probe (fail fast, before flushing). If the
        // marker was reaped by maintenance (e.g. because we missed too many
        // heartbeats under extreme clock skew), skip the pack flush and
        // surface the same typed error the later reconcile path would
        // produce, so callers matching on `StaleChunksDuringCommit` catch
        // this path too. Inconclusive storage errors fall through — the
        // reconcile step is still a safety net.
        let session_id = self
            .write_session
            .as_ref()
            .expect("no active write session")
            .session_id
            .clone();
        if session_id != "default" {
            let marker_key = super::lock::session_marker_key(&session_id);
            match self.storage.exists(&marker_key) {
                Ok(true) => {}
                Ok(false) => {
                    warn!(
                        session_id,
                        "session marker reaped before commit; retry the backup"
                    );
                    return Err(VykarError::StaleChunksDuringCommit);
                }
                Err(e) => {
                    debug!(error = %e, "session-existence probe failed, proceeding to flush");
                }
            }
        }

        let deferred_chunk_index_hydrate = self.commit_prepare(&snapshot_entry, progress)?;

        // ---- COMMIT POINT ----
        let ctx = emit_stage(progress, "write snapshot");
        self.check_lock_fence()?;
        self.storage
            .put(&snapshot_entry.id.storage_key(), &snapshot_packed)?;
        log_stage_elapsed(ctx);
        // ---- after this point, no `?` may escape ----

        // Manifest update is a post-commit in-memory op. Keeping it inline keeps
        // the ordering visible next to the PUT; commit_finalize handles the rest.
        self.manifest.timestamp = Utc::now();
        self.manifest.snapshots.push(snapshot_entry);

        self.commit_finalize(new_file_cache, deferred_chunk_index_hydrate, progress);
        Ok(())
    }

    /// Pre-commit work up to (but not including) the snapshot PUT:
    ///   1. Flush packs and join uploads.
    ///   2. Drop tiered dedup and take the delta from the write session.
    ///   3. Refresh the snapshot list from storage.
    ///   4. Enforce snapshot name uniqueness against the fresh list.
    ///   5. Fetch/decode remote index, reconcile delta, persist index.
    ///
    /// Returns `true` when chunk_index hydration should be deferred to
    /// `commit_finalize` (fast-path case).
    fn commit_prepare(
        &mut self,
        snapshot_entry: &manifest::SnapshotEntry,
        progress: &mut Option<impl FnMut(crate::commands::backup::BackupProgressEvent)>,
    ) -> Result<bool> {
        // 1. Flush all pending packs and wait for uploads.
        self.flush_packs()?;

        // 2. Drop tiered dedup, take delta from write session.
        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        ws.tiered_dedup.take();
        let delta = ws.index_delta.take();
        if delta.is_some() {
            ws.dedup_index = None;
        }

        // 3. Refresh snapshot list (unreadable blobs are skipped — a garbage
        //    snapshot that can't be decrypted cannot conflict with a valid name).
        let ctx = emit_stage(progress, "refresh snapshots");
        self.refresh_snapshot_list()?;
        log_stage_elapsed(ctx);

        // 4. Check snapshot name uniqueness against fresh list.
        if self.manifest.find_snapshot(&snapshot_entry.name).is_some() {
            return Err(VykarError::SnapshotAlreadyExists(
                snapshot_entry.name.clone(),
            ));
        }

        // 5. Download fresh remote index, reconcile delta, persist index.
        let mut deferred_chunk_index_hydrate = false;
        if let Some(delta) = delta {
            if !delta.is_empty() {
                let ctx = emit_stage(progress, "fetch index");
                let raw_blob = self.fetch_raw_index_blob()?;
                log_stage_elapsed(ctx);

                // Try fast path: compare raw blob against cached copy.
                let fast_path_taken = if let Some(ref raw_data) = raw_blob {
                    self.try_fast_path_commit(raw_data, &delta, progress)?
                } else {
                    false
                };

                if fast_path_taken {
                    // chunk_index hydration deferred until after the snapshot
                    // commit point so a local cache error can't abort the
                    // backup after the remote index has already been updated.
                    deferred_chunk_index_hydrate = true;
                } else {
                    let ctx = emit_stage(progress, "decode index");
                    let fresh_index = if let Some(ref raw_data) = raw_blob {
                        Self::decode_raw_index_blob(raw_data, self.crypto.as_ref())?
                    } else {
                        (0, ChunkIndex::new())
                    };
                    log_stage_elapsed(ctx);

                    let ctx = emit_stage(progress, "reconcile");
                    let reconciled = delta.reconcile(&fresh_index.1)?;
                    log_stage_elapsed(ctx);

                    let ctx = emit_stage(progress, "verify packs");
                    self.verify_delta_packs(&reconciled)?;
                    log_stage_elapsed(ctx);

                    let mut fresh_index = fresh_index.1;
                    reconciled.apply_to(&mut fresh_index);
                    self.chunk_index = fresh_index;
                    self.index_dirty = true;
                    self.index_generation = rand::rng().next_u64();

                    let ctx = emit_stage(progress, "write index");
                    self.persist_index()?;
                    log_stage_elapsed(ctx);
                }
            } else if self.rebuild_dedup_cache {
                // Empty delta but caches need rebuilding (tiered dedup was active).
                // chunk_index was dropped — reload from remote for cache rebuild.
                let ctx = emit_stage(progress, "fetch index");
                self.chunk_index = self.reload_full_index()?;
                log_stage_elapsed(ctx);
            }
        }

        // Defensive: persist index if dirty but no delta (unreachable today
        // because backup always activates dedup mode, but guards future callers).
        if self.index_dirty {
            self.index_generation = rand::rng().next_u64();
            self.persist_index()?;
        }

        Ok(deferred_chunk_index_hydrate)
    }

    /// Post-commit best-effort work. The `-> ()` return type is load-bearing:
    /// it statically forbids `?` from escaping a path that runs after the
    /// snapshot has already been durably committed. All internal failures
    /// must be reported via `emit_post_commit_warning`.
    fn commit_finalize(
        &mut self,
        new_file_cache: &mut file_cache::FileCache,
        deferred_chunk_index_hydrate: bool,
        progress: &mut Option<impl FnMut(crate::commands::backup::BackupProgressEvent)>,
    ) {
        // Hydrate chunk_index after the commit point (fast-path deferred).
        // Best-effort: a failure here is non-fatal since the remote index is
        // already committed. `rebuild_local_caches(true)` (below) derives
        // caches from the on-disk full_index_cache and does not read
        // `self.chunk_index`, so leaving it stale is safe. The next Repository
        // operation calls `reload_full_index` and rehydrates from remote.
        if deferred_chunk_index_hydrate {
            let cd = self.cache_dir_override.as_deref();
            match dedup_cache::load_chunk_index_from_full_cache(
                &self.config.id,
                self.index_generation,
                cd,
            )
            .or_else(|e| {
                warn!("fast path: local cache hydration failed ({e}), reloading from remote");
                self.reload_full_index()
            }) {
                Ok(idx) => {
                    self.chunk_index = idx;
                }
                Err(e) => {
                    crate::commands::backup::emit_post_commit_warning(
                        progress,
                        format!(
                            "snapshot was successfully committed, but hydrating the \
                             in-memory chunk index from local cache and remote storage \
                             both failed: {e}. The on-disk chunk index is intact; the \
                             next repository operation will reload it."
                        ),
                    );
                }
            }
        }

        // Merge active sections if any were produced (filesystem backup).
        // Dump-only runs produce no active sections and skip this block.
        let sections = new_file_cache.take_active_sections();
        if !sections.is_empty() {
            for (key, section) in sections {
                self.file_cache.merge_section(&key, section);
            }
            self.file_cache_dirty = true;
        }

        // Persist if dirty — covers both the merge above AND any prior
        // invalidation that set the dirty flag (e.g., stale sections removed
        // at backup start).
        if let Err(e) = self.save_file_cache_if_dirty() {
            crate::commands::backup::emit_post_commit_warning(
                progress,
                format!(
                    "snapshot was successfully committed, but saving the local file \
                     cache failed: {e}. The next backup run will fall back to a \
                     cold-start walk for any affected sources instead of using cached \
                     metadata."
                ),
            );
        }

        // Rebuild local caches if needed. When the fast path already wrote the
        // full cache (deferred_chunk_index_hydrate), skip the redundant sort.
        let ctx = emit_stage(progress, "rebuild local caches");
        if self.rebuild_dedup_cache {
            self.rebuild_local_caches(deferred_chunk_index_hydrate);
            self.rebuild_dedup_cache = false;
        }
        log_stage_elapsed(ctx);

        // Clean up recovered index journals from previous interrupted sessions.
        if let Some(ws) = self.write_session.as_mut() {
            ws.cleanup_recovered_indices(&*self.storage);
        }

        // Consume write session.
        self.write_session = None;
    }

    /// Verify that all pack_ids referenced by new_entries in a delta actually
    /// exist on storage. Returns an error if any are missing.
    fn verify_delta_packs(&self, delta: &IndexDelta) -> Result<()> {
        const BATCH_VERIFY_THRESHOLD: usize = 32;

        let pack_ids: std::collections::HashSet<PackId> =
            delta.new_entries.iter().map(|e| e.pack_id).collect();

        let shards: std::collections::HashSet<String> = pack_ids
            .iter()
            .map(|id| format!("packs/{}", id.shard_prefix()))
            .collect();

        if pack_ids.len() >= BATCH_VERIFY_THRESHOLD && pack_ids.len() > shards.len() {
            // Many packs — batch-verify via shard listing.
            let mut known_packs: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            let mut fallback_shards: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            for shard in &shards {
                match self.storage.list(shard) {
                    Ok(keys) => known_packs.extend(keys),
                    Err(_) => {
                        fallback_shards.insert(shard.clone());
                    }
                }
            }

            for pack_id in &pack_ids {
                let shard_dir = format!("packs/{}", pack_id.shard_prefix());
                if fallback_shards.contains(&shard_dir) {
                    // list() failed for this shard — fall back to per-pack exists() with ? propagation.
                    let key = pack_id.storage_key();
                    if !self.storage.exists(&key)? {
                        return Err(VykarError::Other(format!(
                            "commit failed: pack {} missing from storage (stale session or concurrent deletion)",
                            pack_id
                        )));
                    }
                } else if !known_packs.contains(&pack_id.storage_key()) {
                    return Err(VykarError::Other(format!(
                        "commit failed: pack {} missing from storage (stale session or concurrent deletion)",
                        pack_id
                    )));
                }
            }
        } else {
            // Few packs — per-pack exists() is cheaper than listing entire shards.
            for pack_id in &pack_ids {
                let key = pack_id.storage_key();
                if !self.storage.exists(&key)? {
                    return Err(VykarError::Other(format!(
                        "commit failed: pack {} missing from storage (stale session or concurrent deletion)",
                        pack_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Serialize, encrypt, and write the IndexBlob (generation + chunks) to storage.
    /// Also writes the advisory `index.gen` sidecar and caches the raw encrypted
    /// blob locally for the fast-path commit on the next backup run.
    fn persist_index(&mut self) -> Result<()> {
        let generation = self.index_generation;
        let estimated_msgpack = self.chunk_index.len().saturating_mul(80) + 16;
        let estimated = 1 + zstd::zstd_safe::compress_bound(estimated_msgpack);
        let index_packed = pack_object_streaming_with_context(
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            estimated,
            self.crypto.as_ref(),
            |buf| {
                let blob = IndexBlobRef {
                    generation,
                    chunks: &self.chunk_index,
                };
                compress::compress_stream_zstd(buf, 3, |encoder| {
                    rmp_serde::encode::write(encoder, &blob)?;
                    Ok(())
                })
            },
        )?;
        self.check_lock_fence()?;
        self.storage.put("index", &index_packed)?;
        // Advisory sidecar — best-effort, non-fatal.
        let _ = self.storage.put("index.gen", &generation.to_le_bytes());
        self.index_dirty = false;

        // Cache the raw blob for future fast-path checks (best-effort).
        if let Err(e) = dedup_cache::write_index_blob_cache(
            &index_packed,
            generation,
            &self.config.id,
            self.cache_dir_override.as_deref(),
        ) {
            debug!("failed to write index blob cache: {e}");
        }
        Ok(())
    }

    /// Rebuild all local caches: full index cache (1 sort), then derive
    /// dedup + restore caches from it (O(n) streaming, no sort).
    ///
    /// When `full_cache_fresh` is true, the full index cache is already
    /// up-to-date (e.g. from a fast-path merge) and only the derivation
    /// step runs.
    fn rebuild_local_caches(&self, full_cache_fresh: bool) {
        let cd = self.cache_dir_override.as_deref();
        if !full_cache_fresh {
            if let Err(e) = dedup_cache::build_full_index_cache(
                &self.chunk_index,
                self.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to build full index cache: {e}");
            }
        }
        if let Some(full_path) = dedup_cache::full_index_cache_path(&self.config.id, cd) {
            if let Err(e) = dedup_cache::build_dedup_cache_from_full_cache(
                &full_path,
                self.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to rebuild dedup cache from full cache: {e}");
            }
            if let Err(e) = dedup_cache::build_restore_cache_from_full_cache(
                &full_path,
                self.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to rebuild restore cache from full cache: {e}");
            }
        }
    }

    /// Re-list snapshots/ and rebuild the in-memory manifest.
    /// Used by concurrent session commit to get a fresh snapshot list.
    pub fn refresh_snapshot_list(&mut self) -> Result<()> {
        // Strict I/O: fail on GET errors so a transient failure can't hide an
        // existing snapshot name and allow a duplicate during commit.
        let entries = snapshot_cache::refresh_snapshot_cache(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            &self.config.id,
            self.cache_dir_override.as_deref(),
            true, // strict_io: true — commit uniqueness check
        )?;
        self.manifest = Manifest::from_snapshot_entries(entries);
        Ok(())
    }

    /// Reload the full chunk index from storage (always downloads from remote).
    fn reload_full_index(&self) -> Result<ChunkIndex> {
        self.reload_full_index_with_generation()
            .map(|(_, index)| index)
    }

    /// Reload the full chunk index + generation from storage.
    fn reload_full_index_with_generation(&self) -> Result<(u64, ChunkIndex)> {
        if let Some(index_data) = self.storage.get("index")? {
            let blob = Self::decode_index_blob_full(&index_data, self.crypto.as_ref())?;
            Ok((blob.generation, blob.chunks))
        } else {
            Ok((0, ChunkIndex::new()))
        }
    }

    /// Decrypt, decompress, and deserialize an index blob into an `IndexBlob`.
    fn decode_index_blob_full(index_data: &[u8], crypto: &dyn CryptoEngine) -> Result<IndexBlob> {
        let compressed = unpack_object_expect_with_context(
            index_data,
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            crypto,
        )?;
        let index_bytes = compress::decompress_metadata(&compressed)?;
        Ok(rmp_serde::from_slice(&index_bytes)?)
    }

    /// Fetch the raw encrypted index blob from storage without decoding.
    fn fetch_raw_index_blob(&self) -> Result<Option<Vec<u8>>> {
        self.storage.get("index")
    }

    /// Decode an already-fetched raw index blob into (generation, ChunkIndex).
    fn decode_raw_index_blob(raw: &[u8], crypto: &dyn CryptoEngine) -> Result<(u64, ChunkIndex)> {
        let blob = Self::decode_index_blob_full(raw, crypto)?;
        Ok((blob.generation, blob.chunks))
    }

    /// Try the fast-path commit: if the remote index blob matches the cached
    /// copy, skip decode + reconcile. Uses AEAD ciphertext comparison —
    /// identical ciphertext guarantees identical plaintext.
    ///
    /// On success: merges the full index cache, persists the merged index to
    /// storage, and caches the raw blob. Local dedup/restore cache derivation
    /// and `chunk_index` hydration are left to the caller (after the snapshot
    /// commit point) so that local-only failures cannot abort a committed backup.
    ///
    /// Returns `true` if the fast path was taken.
    fn try_fast_path_commit(
        &mut self,
        raw_blob: &[u8],
        delta: &IndexDelta,
        progress: &mut Option<impl FnMut(crate::commands::backup::BackupProgressEvent)>,
    ) -> Result<bool> {
        use crate::commands::backup::BackupProgressEvent;

        let cd = self.cache_dir_override.as_deref();
        let cached = dedup_cache::read_index_blob_cache(&self.config.id, self.index_generation, cd);
        let Some(cached_blob) = cached else {
            debug!("fast path: no cached index blob, falling through to slow path");
            return Ok(false);
        };

        if raw_blob != cached_blob.as_slice() {
            debug!("fast path: remote index changed, falling through to slow path");
            return Ok(false);
        }

        // Index unchanged — try to open the full index cache for merge.
        let Some(full_cache_path) = dedup_cache::full_index_cache_path(&self.config.id, cd) else {
            debug!("fast path: no cache dir, falling through to slow path");
            return Ok(false);
        };

        let old_cache =
            dedup_cache::MmapFullIndexCache::open_path(&full_cache_path, self.index_generation);
        let Some(old_cache) = old_cache else {
            debug!("fast path: full index cache missing or stale, falling through to slow path");
            return Ok(false);
        };

        if let Some(ref mut cb) = progress {
            cb(BackupProgressEvent::CommitStage {
                stage: "index unchanged, fast path",
            });
        }
        let fast_start = std::time::Instant::now();

        // Verify packs before merging.
        let ctx_start = std::time::Instant::now();
        self.verify_delta_packs(delta)?;
        debug!(
            stage = "verify packs",
            elapsed_ms = ctx_start.elapsed().as_millis() as u64,
            "commit stage complete"
        );

        // Merge old cache + delta into new full cache.
        self.index_generation = rand::rng().next_u64();
        let new_cache_path = full_cache_path.with_extension("merged");
        dedup_cache::merge_full_index_cache(
            &old_cache,
            delta,
            self.index_generation,
            &new_cache_path,
        )?;
        // Drop the mmap BEFORE renaming — on Windows, mapped files block replacement.
        drop(old_cache);
        std::fs::rename(&new_cache_path, &full_cache_path)?;

        // Serialize the merged cache as the new index blob and persist.
        let merged_cache =
            dedup_cache::MmapFullIndexCache::open_path(&full_cache_path, self.index_generation)
                .ok_or_else(|| VykarError::Other("failed to reopen merged cache".into()))?;

        let index_packed = dedup_cache::serialize_full_cache_as_index_blob(
            &merged_cache,
            self.index_generation,
            self.crypto.as_ref(),
        )?;

        self.check_lock_fence()?;
        self.storage.put("index", &index_packed)?;
        let _ = self
            .storage
            .put("index.gen", &self.index_generation.to_le_bytes());
        self.index_dirty = false;

        // Cache the raw blob for next fast-path check (best-effort).
        if let Err(e) = dedup_cache::write_index_blob_cache(
            &index_packed,
            self.index_generation,
            &self.config.id,
            cd,
        ) {
            debug!("failed to write index blob cache: {e}");
        }
        // Dedup/restore cache derivation is deferred to the post-commit
        // rebuild block (after the snapshot write) to minimize lock hold time.
        // rebuild_dedup_cache stays true so the post-commit block picks it up.

        debug!(
            elapsed_ms = fast_start.elapsed().as_millis() as u64,
            "fast-path commit complete"
        );

        Ok(true)
    }
}
