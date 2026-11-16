use chrono::Utc;
use rand::Rng;
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

/// Join a scoped helper thread, re-raising its panic on the caller.
fn join_scoped<T>(handle: std::thread::ScopedJoinHandle<'_, T>) -> T {
    handle
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
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
        self.load_chunk_index_with_sidecar(None)
    }

    /// [`Self::load_chunk_index`], given the `index.gen` value this process
    /// just read (`Some`, with 0 standing for absent, as open reads it). The
    /// advisory sidecar is rewritten only when it differs from the
    /// authenticated generation: rewriting an equal value changes nothing,
    /// and skipping it can only avoid clobbering a newer value that a
    /// concurrent writer put there since. The sidecar is never trusted for
    /// writes either way.
    pub(super) fn load_chunk_index_with_sidecar(&mut self, sidecar: Option<u64>) -> Result<()> {
        let index = self.reload_full_index_with_generation()?;
        self.install_chunk_index(index, sidecar);
        Ok(())
    }

    /// Install an index fetched with [`Self::fetch_full_index`] *after*
    /// `sidecar` was read, as [`Self::load_chunk_index_with_sidecar`] does.
    ///
    /// The order matters for the rewrite below: a sidecar read after the
    /// index could already hold a concurrent writer's newer generation, and
    /// "fixing" it to this older one would clobber it.
    pub(super) fn install_chunk_index(&mut self, index: (u64, ChunkIndex), sidecar: Option<u64>) {
        let (gen, index) = index;
        self.index_generation = gen;
        self.chunk_index = index;
        if sidecar != Some(gen) {
            // Best-effort rewrite index.gen
            let _ = self.storage.put("index.gen", &gen.to_le_bytes());
        }
        self.rebase_pack_target_from_index();
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

    /// Flush pending packs and wait for uploads, then drop the tiered dedup
    /// index and take the session's `IndexDelta` out for processing. The
    /// delta's positional bumps keep the dedup cache mapped until they are
    /// applied; everything else in the tiered index is released here.
    ///
    /// On the concurrent backup path Phase 1 has normally drained uploads
    /// already, so the flush here is a no-op safety net; for
    /// `apply_write_session` it is the real flush.
    ///
    /// Must only be called when `write_session` is `Some`. The session itself
    /// stays active: on failure the caller can still run `flush_on_abort()`,
    /// which does not need the structures taken here.
    fn flush_and_take_session_delta(&mut self) -> Result<Option<IndexDelta>> {
        self.flush_packs()?;

        let ws = self
            .write_session
            .as_mut()
            .expect("no active write session");
        ws.tiered_dedup.take();
        let delta = ws.index_delta.take();
        if delta.is_some() {
            ws.dedup_index = None;
        }
        Ok(delta)
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
        let delta = self.flush_and_take_session_delta()?;

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

        self.join_index_sidecar_write();

        // Now propagate any file cache save error
        fc_result?;

        // Consume the write session on success — all entries are now in the
        // persisted index. The pending_index file itself is deleted later by
        // clear_pending_index() from the backup command.
        self.write_session = None;

        Ok(())
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
    ///
    /// `on_commit_point` runs right after step 6 succeeded, before the
    /// local post-commit work of steps 7-8, so the caller can start remote
    /// cleanup that only a committed session may do (deregistering it,
    /// deleting its journal) while that work runs. No journal write of this
    /// session is in flight when it is called.
    pub fn commit_concurrent_session_with_progress(
        &mut self,
        snapshot_entry: manifest::SnapshotEntry,
        snapshot_packed: Vec<u8>,
        new_file_cache: &mut file_cache::FileCache,
        progress: &mut Option<impl FnMut(crate::commands::backup::BackupProgressEvent)>,
        on_commit_point: impl FnOnce(),
    ) -> Result<()> {
        // The reads that start the commit are independent of each other. The
        // fresh remote index (megabytes; only fetched when there is a delta
        // to merge into it) is fetched on this thread while a helper thread
        // refreshes the snapshot list and then probes the session marker, so
        // the small round trips hide behind the large one. The index GET is
        // issued at once, while the helper is still loading the local
        // snapshot cache, so it takes the pooled connection that already
        // carried the open's index GET: a fresh connection starts with a
        // small receive window and takes about twice as long for the index.
        // All of these are reads under the commit lock; every write still
        // follows them. Results are judged in the old order: probe, flush,
        // snapshot list and name check, index.
        let session_id = self
            .write_session
            .as_ref()
            .expect("no active write session")
            .session_id
            .clone();
        let taken = self.flush_and_take_session_delta();
        let fetch_index = matches!(&taken, Ok(Some(delta)) if !delta.is_empty());
        let marker_key =
            (session_id != "default").then(|| super::lock::session_marker_key(&session_id));
        let storage = self.storage.as_ref();
        let crypto = self.crypto.as_ref();
        let repo_id = self.config.id.as_slice();
        let cache_dir = self.cache_dir_override.as_deref();
        let ctx = emit_stage(progress, "refresh snapshots");
        let (side, prefetched) = std::thread::scope(|scope| {
            let side = taken.is_ok().then(|| {
                scope.spawn(move || {
                    // Strict I/O: fail on GET errors so a transient failure
                    // can't hide an existing snapshot name and allow a
                    // duplicate during commit.
                    let refresh = snapshot_cache::refresh_snapshot_cache(
                        storage, crypto, repo_id, cache_dir, true,
                    );
                    let marker = marker_key.map(|key| storage.exists(&key));
                    (refresh, marker)
                })
            });
            let prefetched = fetch_index.then(|| storage.get("index"));
            (side.map(join_scoped), prefetched)
        });
        log_stage_elapsed(ctx);
        let (refreshed, marker) = match side {
            Some((refresh, marker)) => (Some(refresh), marker),
            None => (None, None),
        };

        // Session-existence probe. If the marker was reaped by maintenance
        // (e.g. because we missed too many heartbeats under extreme clock
        // skew), surface the same typed error the later reconcile path
        // would produce, so callers matching on `StaleChunksDuringCommit`
        // catch this path too. Inconclusive storage errors fall through —
        // the reconcile step is still a safety net.
        match marker {
            Some(Ok(false)) => {
                warn!(
                    session_id,
                    "session marker reaped before commit; retry the backup"
                );
                return Err(VykarError::StaleChunksDuringCommit);
            }
            Some(Err(e)) => {
                debug!(error = %e, "session-existence probe failed, proceeding");
            }
            Some(Ok(true)) | None => {}
        }
        let delta = taken?;
        if let Some(refresh) = refreshed {
            self.apply_snapshot_refresh(refresh?);
        }

        let deferred_chunk_index_hydrate =
            self.commit_prepare(&snapshot_entry, delta, prefetched, progress)?;

        // ---- COMMIT POINT ----
        let ctx = emit_stage(progress, "write snapshot");
        // The fence check overlaps the index.gen PUT the index write left
        // running; both finish before the snapshot PUT.
        let fenced = self.check_lock_fence();
        self.join_index_sidecar_write();
        fenced?;
        self.storage
            .put(&snapshot_entry.id.storage_key(), &snapshot_packed)?;
        log_stage_elapsed(ctx);
        // ---- after this point, no `?` may escape ----

        // A journal PUT landing after the caller deleted the journal would
        // resurrect it, so none may still be running.
        if let Some(ws) = self.write_session.as_mut() {
            ws.join_journal_write();
        }
        on_commit_point();

        // Manifest update is a post-commit in-memory op. Keeping it inline keeps
        // the ordering visible next to the PUT; commit_finalize handles the rest.
        self.manifest.timestamp = Utc::now();
        self.manifest.snapshots.push(snapshot_entry);
        // Keep the local snapshot list cache in step with the commit, so the
        // next open does not re-download this snapshot's blob. Best-effort:
        // the cache is pruned against `snapshots/` on every open.
        self.persist_snapshot_cache_from_manifest();

        self.commit_finalize(new_file_cache, deferred_chunk_index_hydrate, progress);
        Ok(())
    }

    /// Pre-commit work up to (but not including) the snapshot PUT, once the
    /// caller has flushed packs, taken the session `delta`, refreshed the
    /// snapshot list (unreadable blobs are skipped — a garbage snapshot
    /// that can't be decrypted cannot conflict with a valid name) and, for
    /// a non-empty delta, fetched the raw remote `index` (`prefetched`):
    ///   4. Enforce snapshot name uniqueness against the fresh list.
    ///   5. Decode remote index, reconcile delta, persist index.
    ///
    /// Returns `true` when chunk_index hydration should be deferred to
    /// `commit_finalize` (fast-path case).
    fn commit_prepare(
        &mut self,
        snapshot_entry: &manifest::SnapshotEntry,
        delta: Option<IndexDelta>,
        prefetched: Option<Result<Option<Vec<u8>>>>,
        progress: &mut Option<impl FnMut(crate::commands::backup::BackupProgressEvent)>,
    ) -> Result<bool> {
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
                let raw_blob = match prefetched {
                    Some(fetched) => fetched?,
                    None => self.storage.get("index")?,
                };
                log_stage_elapsed(ctx);

                // Try fast path: compare raw blob against cached copy.
                let fast_path_cache = raw_blob
                    .as_deref()
                    .and_then(|raw_data| self.fast_path_full_cache(raw_data));

                if let Some((old_cache, full_cache_path)) = fast_path_cache {
                    // The remote blob was only needed for the comparison; the
                    // new blob is serialized from the merged full cache. Free
                    // it before the merge and serialization peak.
                    drop(raw_blob);
                    self.fast_path_commit(old_cache, &full_cache_path, delta, progress)?;
                    // chunk_index hydration deferred until after the snapshot
                    // commit point so a local cache error can't abort the
                    // backup after the remote index has already been updated.
                    deferred_chunk_index_hydrate = true;
                } else {
                    let ctx = emit_stage(progress, "decode index");
                    let fresh_index = if let Some(raw_data) = raw_blob {
                        // Free the ciphertext before building the index from
                        // the plaintext (as `reload_full_index_with_generation`).
                        let compressed = Self::unpack_index_blob(&raw_data, self.crypto.as_ref())?;
                        drop(raw_data);
                        let blob = Self::decode_unpacked_index_blob(&compressed)?;
                        (blob.generation, blob.chunks)
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
        // The fast path deferred `chunk_index` hydration past the commit
        // point. It runs last (below): neither the file-cache save nor
        // `rebuild_local_caches(true)` reads `self.chunk_index`, so doing it
        // first would only keep the hydrated index alive next to the file
        // cache's serialization buffer and raise the commit's peak memory.

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
        // full cache (deferred_chunk_index_hydrate), skip the redundant sort;
        // the derivation then streams the on-disk full_index_cache and does
        // not read `self.chunk_index`, which is not hydrated yet.
        let ctx = emit_stage(progress, "rebuild local caches");
        if self.rebuild_dedup_cache {
            self.rebuild_local_caches(deferred_chunk_index_hydrate);
            self.rebuild_dedup_cache = false;
        }
        log_stage_elapsed(ctx);

        if deferred_chunk_index_hydrate {
            self.hydrate_deferred_chunk_index(progress);
        }

        // Clean up recovered index journals from previous interrupted sessions.
        if let Some(ws) = self.write_session.as_mut() {
            ws.cleanup_recovered_indices(&*self.storage);
        }

        // Consume write session.
        self.write_session = None;
    }

    /// Restore `self.chunk_index` after a fast-path commit, which never
    /// decoded the index: from the merged local full_index_cache, falling
    /// back to the remote index.
    ///
    /// Best-effort: a failure here is non-fatal since the remote index is
    /// already committed; it leaves `chunk_index` stale and the next
    /// Repository operation calls `reload_full_index` and rehydrates from
    /// remote.
    fn hydrate_deferred_chunk_index(
        &mut self,
        progress: &mut Option<impl FnMut(crate::commands::backup::BackupProgressEvent)>,
    ) {
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

    /// Verify that all pack_ids referenced by new_entries in a delta actually
    /// exist on storage. Returns an error if any are missing.
    ///
    /// Every pack is checked; only the request pattern adapts (see
    /// `pack_verify`), with up to `request_concurrency` requests in flight.
    fn verify_delta_packs(&self, delta: &IndexDelta) -> Result<()> {
        super::pack_verify::verify_packs_present(
            self.storage.as_ref(),
            &Self::delta_pack_ids(delta),
            self.persisted_pack_count(),
            self.request_concurrency,
        )
    }

    /// The packs a delta's new entries live in.
    fn delta_pack_ids(delta: &IndexDelta) -> std::collections::HashSet<PackId> {
        delta.new_entries.iter().map(|e| e.pack_id).collect()
    }

    /// Packs already in the repository, a cost hint for `pack_verify`.
    fn persisted_pack_count(&self) -> usize {
        self.write_session
            .as_ref()
            .map_or(0, |ws| ws.persisted_pack_count)
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
        self.put_index_blob(&index_packed, generation)
    }

    /// Write an already-packed index blob to storage under the lock fence.
    ///
    /// Shared commit epilogue: fence check, the `index` PUT, the advisory
    /// `index.gen` sidecar, clearing `index_dirty`, and refreshing the local
    /// raw-blob cache that the next run's fast path compares against. Only
    /// the `index` PUT is fatal — the sidecar and the cache are best-effort.
    fn put_index_blob(&mut self, index_packed: &[u8], generation: u64) -> Result<()> {
        self.join_index_sidecar_write();
        self.check_lock_fence()?;
        self.storage.put("index", index_packed)?;
        // Advisory sidecar — best-effort, non-fatal — written in the
        // background so its round trip overlaps the caller's next request.
        // It still starts only after the index PUT succeeded.
        let storage = std::sync::Arc::clone(&self.storage);
        let spawned = std::thread::Builder::new()
            .name("index-gen".into())
            .spawn(move || {
                let _ = storage.put("index.gen", &generation.to_le_bytes());
            });
        match spawned {
            Ok(handle) => self.index_sidecar_write = Some(handle),
            Err(_) => {
                let _ = self.storage.put("index.gen", &generation.to_le_bytes());
            }
        }
        self.index_dirty = false;

        // Cache the raw blob for future fast-path checks (best-effort).
        if let Err(e) = dedup_cache::write_index_blob_cache(
            index_packed,
            generation,
            &self.config.id,
            self.cache_dir_override.as_deref(),
        ) {
            debug!("failed to write index blob cache: {e}");
        }
        Ok(())
    }

    /// Wait for the background `index.gen` write, if one is running.
    pub(super) fn join_index_sidecar_write(&mut self) {
        if let Some(handle) = self.index_sidecar_write.take() {
            let _ = handle.join();
        }
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

    /// Persist the local snapshot list cache from the current in-memory
    /// manifest.
    ///
    /// Commit, delete and prune mutate `manifest.snapshots` in memory but do
    /// not touch the persisted `snapshot_list` cache; without this the cache
    /// only self-heals on the next `open()` (the remote-list retain drops
    /// removed entries, and added ones are re-downloaded). Calling this after
    /// a mutation keeps the local cache consistent immediately.
    ///
    /// Best-effort: failures are warned and swallowed, matching
    /// `refresh_snapshot_cache`'s save behavior — a stale cache is corrected on
    /// the next open and never affects correctness.
    pub fn persist_snapshot_cache_from_manifest(&self) {
        let mut cache = snapshot_cache::SnapshotListCache::default();
        for entry in &self.manifest.snapshots {
            cache.entries.insert(entry.id.to_hex(), entry.clone());
        }
        if let Err(e) = cache.save(
            &self.config.id,
            self.crypto.as_ref(),
            self.cache_dir_override.as_deref(),
        ) {
            warn!("failed to persist snapshot list cache after mutation: {e}");
        }
    }

    /// Re-list snapshots/ and rebuild the in-memory manifest.
    /// Used by concurrent session commit to get a fresh snapshot list.
    pub fn refresh_snapshot_list(&mut self) -> Result<()> {
        // Strict I/O: fail on GET errors so a transient failure can't hide an
        // existing snapshot name and allow a duplicate during commit.
        let refresh = snapshot_cache::refresh_snapshot_cache(
            self.storage.as_ref(),
            self.crypto.as_ref(),
            &self.config.id,
            self.cache_dir_override.as_deref(),
            true, // strict_io: true — commit uniqueness check
        )?;
        self.apply_snapshot_refresh(refresh);
        Ok(())
    }

    /// Replace the in-memory manifest with a fresh snapshot listing.
    fn apply_snapshot_refresh(&mut self, refresh: snapshot_cache::SnapshotRefresh) {
        self.skipped_snapshots = refresh.skipped;
        self.manifest = Manifest::from_snapshot_entries(refresh.entries);
    }

    /// Reload the full chunk index from storage (always downloads from remote).
    fn reload_full_index(&self) -> Result<ChunkIndex> {
        self.reload_full_index_with_generation()
            .map(|(_, index)| index)
    }

    /// Reload the full chunk index + generation from storage.
    fn reload_full_index_with_generation(&self) -> Result<(u64, ChunkIndex)> {
        Self::fetch_full_index(self.storage.as_ref(), self.crypto.as_ref())
    }

    /// Download and decode the full chunk index + generation using only the
    /// storage and crypto handles, for readers that cannot borrow the
    /// repository. Does not touch the repository's own index.
    pub(crate) fn fetch_full_index(
        storage: &dyn vykar_storage::StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<(u64, ChunkIndex)> {
        Self::decode_full_index(storage.get("index")?, crypto)
    }

    /// Decode a fetched `index` object (`None` = absent: generation 0, empty).
    pub(crate) fn decode_full_index(
        index_data: Option<Vec<u8>>,
        crypto: &dyn CryptoEngine,
    ) -> Result<(u64, ChunkIndex)> {
        let Some(index_data) = index_data else {
            return Ok((0, ChunkIndex::new()));
        };
        // Free the ciphertext before building the index from the plaintext.
        let compressed = Self::unpack_index_blob(&index_data, crypto)?;
        drop(index_data);
        let blob = Self::decode_unpacked_index_blob(&compressed)?;
        Ok((blob.generation, blob.chunks))
    }

    /// Decrypt an index blob, returning its still-compressed payload.
    fn unpack_index_blob(index_data: &[u8], crypto: &dyn CryptoEngine) -> Result<Vec<u8>> {
        unpack_object_expect_with_context(
            index_data,
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            crypto,
        )
    }

    /// Decompress and deserialize an index payload without materializing
    /// the decompressed msgpack (see [`compress::decode_metadata_msgpack`]).
    fn decode_unpacked_index_blob(compressed: &[u8]) -> Result<IndexBlob> {
        Ok(compress::decode_metadata_msgpack(compressed)?.0)
    }

    /// Fast-path precondition: the remote index blob is byte-identical to the
    /// locally cached copy (AEAD ciphertext comparison — identical ciphertext
    /// guarantees identical plaintext), and the full index cache for the
    /// current generation is present.
    ///
    /// Returns the opened full cache and its path when the fast path can be
    /// taken, `None` to fall through to the slow path. Has no side effects,
    /// so a `None` leaves the slow path exactly as if it ran alone.
    fn fast_path_full_cache(
        &self,
        raw_blob: &[u8],
    ) -> Option<(dedup_cache::MmapFullIndexCache, std::path::PathBuf)> {
        let cd = self.cache_dir_override.as_deref();
        // Compared by streaming the cache file, so no second copy of the blob
        // is materialized next to `raw_blob`.
        if !dedup_cache::index_blob_cache_matches(
            &self.config.id,
            self.index_generation,
            cd,
            raw_blob,
        ) {
            debug!(
                "fast path: cached index blob missing or different, falling through to slow path"
            );
            return None;
        }

        // Index unchanged — try to open the full index cache for merge.
        let Some(full_cache_path) = dedup_cache::full_index_cache_path(&self.config.id, cd) else {
            debug!("fast path: no cache dir, falling through to slow path");
            return None;
        };

        let Some(old_cache) =
            dedup_cache::MmapFullIndexCache::open_path(&full_cache_path, self.index_generation)
        else {
            debug!("fast path: full index cache missing or stale, falling through to slow path");
            return None;
        };
        Some((old_cache, full_cache_path))
    }

    /// Fast-path commit, once [`Self::fast_path_full_cache`] established that
    /// the remote index is unchanged: skip decode + reconcile, merge the full
    /// index cache with the delta, persist the merged index to storage, and
    /// cache the raw blob.
    ///
    /// Local dedup/restore cache derivation and `chunk_index` hydration are
    /// left to the caller (after the snapshot commit point) so that local-only
    /// failures cannot abort a committed backup.
    fn fast_path_commit(
        &mut self,
        old_cache: dedup_cache::MmapFullIndexCache,
        full_cache_path: &std::path::Path,
        delta: IndexDelta,
        progress: &mut Option<impl FnMut(crate::commands::backup::BackupProgressEvent)>,
    ) -> Result<()> {
        use crate::commands::backup::BackupProgressEvent;

        if let Some(ref mut cb) = progress {
            cb(BackupProgressEvent::CommitStage {
                stage: "index unchanged, fast path",
            });
        }
        let fast_start = std::time::Instant::now();

        // Verify the session's packs while the merged cache is built and
        // serialized: the check is network-bound, the merge local. The
        // merged cache stays at its temporary path until the check passed,
        // so a failed check leaves the local full cache and the in-memory
        // generation exactly as they were; nothing remote is written before
        // the join.
        let pack_ids = Self::delta_pack_ids(&delta);
        let persisted_packs = self.persisted_pack_count();
        let width = self.request_concurrency;
        let generation = rand::rng().next_u64();
        let new_cache_path = full_cache_path.with_extension("merged");
        let storage = self.storage.as_ref();
        let crypto = self.crypto.as_ref();
        let (verified, built) = std::thread::scope(|scope| {
            let verify = scope.spawn(|| {
                let ctx_start = std::time::Instant::now();
                let r = super::pack_verify::verify_packs_present(
                    storage,
                    &pack_ids,
                    persisted_packs,
                    width,
                );
                debug!(
                    stage = "verify packs",
                    elapsed_ms = ctx_start.elapsed().as_millis() as u64,
                    "commit stage complete"
                );
                r
            });
            let built = (|| {
                dedup_cache::merge_full_index_cache(
                    &old_cache,
                    &delta,
                    generation,
                    &new_cache_path,
                )?;
                // Drop the mmap BEFORE renaming — on Windows, mapped files
                // block replacement.
                drop(old_cache);
                // The delta is fully applied; drop it (and the dedup cache
                // mapping its positional bumps hold) before serializing.
                drop(delta);
                // Serialize the merged cache as the new index blob. The
                // mapping is dropped again before the rename below.
                let merged_cache =
                    dedup_cache::MmapFullIndexCache::open_path(&new_cache_path, generation)
                        .ok_or_else(|| VykarError::Other("failed to reopen merged cache".into()))?;
                dedup_cache::serialize_full_cache_as_index_blob(&merged_cache, generation, crypto)
            })();
            (join_scoped(verify), built)
        });
        let index_packed = match verified.and(built) {
            Ok(packed) => packed,
            Err(e) => {
                let _ = std::fs::remove_file(&new_cache_path);
                return Err(e);
            }
        };
        std::fs::rename(&new_cache_path, full_cache_path)?;
        self.index_generation = generation;

        self.put_index_blob(&index_packed, self.index_generation)?;
        // Dedup/restore cache derivation is deferred to the post-commit
        // rebuild block (after the snapshot write) to minimize lock hold time.
        // rebuild_dedup_cache stays true so the post-commit block picks it up.

        debug!(
            elapsed_ms = fast_start.elapsed().as_millis() as u64,
            "fast-path commit complete"
        );

        Ok(())
    }
}
