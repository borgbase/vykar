//! In-place upgrade from the monolithic index (formats v2/v3) to format v6
//! (v2 keeps BLAKE2b IDs, v3 BLAKE3; the config names the hash). Runs under
//! the maintenance lock, on a repository opened without its index.
//!
//! 1. Probe the server capabilities and PUT the stored `config` bytes back
//!    unchanged: a server that refuses overwrites (append-only) fails here,
//!    with the repository untouched.
//! 2. GET `index` and decode the legacy index. While `config` is
//!    positional, this blob is the repository's index; heads and bases
//!    already under `index.h/` and `index.d/` are debris of an earlier
//!    attempt, pack maps under `packmaps/` are reused.
//! 3. Write it as a base body into a local temp file (every pack at
//!    `trailer_len` 0: releases that wrote v2/v3 wrote no trailers) and make
//!    it the handle's index. Nothing is written to storage.
//! 4. Write pack maps for every pack the index names that no readable map
//!    covers (a pack with more entries than a map may hold is skipped and
//!    reported; one the index describes inconsistently stops the upgrade).
//! 5. Write the base under `index.d/` and a head at
//!    `index.h/<highest listed + 1>`. `index` is not touched.
//! 6. Rewrite `config` as the v6 map. From here every older binary refuses
//!    the repository by version.
//! 7. Delete `index`, `index.gen` and the local copy of the legacy index
//!    blob, best-effort.
//!
//! Why this order is safe: until the switch in step 6 an older binary keeps
//! working on the repository, because the legacy index is untouched. A
//! resume always rebuilds from the current legacy index, so writes an old
//! client made in between (under a lock this upgrade lost) are not dropped.
//! Maps come before the switch, so a v6 repository never has a pre-upgrade
//! pack a map could describe but does not (`check --repair` then rebuilds
//! its entries without any local file). The debris of an interrupted
//! attempt (heads, base) is collected by age. An upgrade interrupted after
//! the switch finds a current format and only retries the deletions of
//! step 7.

use tracing::{debug, info, warn};

use super::control::CancelToken;
use super::format::{ObjectType, unpack_object_expect_with_context};
use super::index_store;
use super::index_write::PendingSweep;
use super::{DecodedConfig, INDEX_OBJECT_CONTEXT, RepoConfig, RepoFormat, Repository};
use crate::compress;
use std::sync::Arc;

use crate::index::dedup_cache;
use crate::index::table::{self, ChunkTable};
use crate::index::{IndexBlob, LegacyChunkIndex, LiveIndex};
use vykar_crypto::CryptoEngine;
use vykar_types::error::{Result, VykarError};

/// What [`Repository::upgrade_index_layout`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeOutcome {
    /// The repository already uses the current format.
    AlreadyCurrent { version: u32 },
    Upgraded {
        from: u32,
        to: u32,
        /// The chunk and pack hash, which the repository keeps.
        chunk_hash: vykar_types::hash::HashAlgorithm,
        /// Index entries written to the base.
        entries: u64,
        /// Stored size of the base (0 for an empty index).
        base_bytes: u64,
        /// Packs this run wrote maps for.
        packs_mapped: usize,
        /// Packs with more entries than a map may hold: described only by
        /// the index (a repair that cannot read the index loses them).
        packs_unmapped: usize,
        /// An interrupted upgrade was completed: a head or a pack map was
        /// already in place.
        resumed: bool,
    },
}

/// The error every open except `vykar upgrade` returns for v2/v3.
pub(crate) fn legacy_format_error(format: RepoFormat) -> VykarError {
    VykarError::Other(format!(
        "this repository is in format v{}, which this vykar opens only to upgrade it: \
         run `vykar upgrade` (one-way; stop every other client of the repository and \
         upgrade vykar on all of them first)",
        format.version()
    ))
}

/// Decode a v2/v3 `IndexBlob` fetched from `index` (`None` = absent: generation
/// 0, empty). The only reader of the monolithic index, kept for the upgrade.
pub(crate) fn decode_index_blob(
    data: Option<Vec<u8>>,
    crypto: &dyn CryptoEngine,
) -> Result<(u64, LegacyChunkIndex)> {
    let Some(data) = data else {
        return Ok((0, LegacyChunkIndex::new()));
    };
    let compressed = unpack_object_expect_with_context(
        &data,
        ObjectType::ChunkIndex,
        INDEX_OBJECT_CONTEXT,
        crypto,
    )?;
    // Free the ciphertext before building the index from the plaintext.
    drop(data);
    let blob: IndexBlob = compress::decode_metadata_msgpack(&compressed)?.0;
    Ok((blob.generation, blob.chunks))
}

/// Whether `data` (the object at `index`) is a segmented index head, as the
/// development formats v4/v5 wrote it.
pub(crate) fn is_index_head(data: &[u8]) -> bool {
    data.first() == Some(&(ObjectType::IndexHead as u8))
}

impl Repository {
    /// Upgrade a v2/v3 repository to v6 in place; see the module docs.
    /// The caller holds the maintenance lock. One-way: older binaries cannot
    /// read the repository afterwards.
    pub fn upgrade_index_layout(&mut self) -> Result<UpgradeOutcome> {
        let from = self.format;
        let Some(target) = from.upgrade_target() else {
            // An upgrade interrupted after its switch left these behind.
            self.remove_legacy_index();
            return Ok(UpgradeOutcome::AlreadyCurrent {
                version: from.version(),
            });
        };

        // 1. Pre-flight.
        if let Some(server) = self.storage.server_ops() {
            let caps = server.capabilities()?;
            if !caps.segmented_index || !caps.serves_v6_key_dirs() {
                return Err(VykarError::Config(format!(
                    "vykar-server (version {}) does not support the index of format v{} \
                     (the segmented index and the index.h/ and packmaps/ keys). Upgrade \
                     the server first.",
                    caps.version,
                    target.version(),
                )));
            }
        }
        let config_bytes = self
            .storage
            .get("config")?
            .ok_or_else(|| VykarError::RepoNotFound("config not found".into()))?;
        let unchanged = matches!(
            RepoConfig::decode(&config_bytes),
            Ok(DecodedConfig::Positional(c)) if c.version == from.version() && c.id == self.config.id
        );
        if !unchanged {
            return Err(VykarError::Other(
                "the repository config changed after it was opened; retry the upgrade".into(),
            ));
        }
        self.storage.put("config", &config_bytes).map_err(|e| {
            VykarError::Other(format!(
                "the repository config cannot be rewritten ({e}). The upgrade rewrites \
                 `config` and deletes `index`; on an append-only server, disable \
                 append-only for the upgrade. Nothing was changed."
            ))
        })?;

        // 2. The legacy index: while `config` is positional, the index.
        let data = self.storage.get("index")?;
        if data.as_deref().is_some_and(is_index_head) {
            return Err(VykarError::Other(
                "`index` holds the segmented index head of an interrupted upgrade by a \
                 development build; restore `index` from a copy of the repository. \
                 Nothing was changed"
                    .into(),
            ));
        }
        let (_, index) = decode_index_blob(data, self.crypto.as_ref()).map_err(|e| {
            VykarError::Other(format!(
                "the v{} index cannot be read ({e}); nothing was changed. Restore \
                 `index` from a copy of the repository, or use the vykar release \
                 that wrote it",
                from.version()
            ))
        })?;
        debug!(entries = index.len(), "legacy index loaded");
        let had_head = !index_store::list_head_generations(self.storage.as_ref())?.is_empty();

        // 3. The base body, locally, from the live legacy entries (an entry
        // at refcount 0 was garbage the old release had not removed yet, and
        // is dropped; the other refcounts are discarded); the legacy index is
        // dropped before the maps are built from the table.
        let anchor = index_store::cache_anchor(self.cache_dir().as_deref());
        let (body, entries) = table::base_body_from_legacy(&index, &anchor).map_err(|e| {
            VykarError::Other(format!(
                "the v{} index cannot be written as a v6 base ({e}); nothing was changed",
                from.version()
            ))
        })?;
        drop(index);
        self.chunk_index = LiveIndex::from_table(Arc::new(ChunkTable::install(body, None, None)?));

        // 4. Pack maps for every pre-upgrade pack a map can describe.
        let maps = self.write_pack_maps(None, &CancelToken::new())?;
        if let Some((pack, why)) = maps.layout_violations.first() {
            return Err(VykarError::Other(format!(
                "the v{} index describes pack {pack} inconsistently ({why}); the upgrade \
                 stopped before switching the format, so the repository is still v{} \
                 (only pack maps were written). Run `vykar check --verify-data` with the \
                 release that wrote the repository",
                from.version(),
                from.version()
            )));
        }
        if maps.packs_skipped_too_many > 0 {
            warn!(
                packs = maps.packs_skipped_too_many,
                "packs with more entries than a pack map holds are described only by the index"
            );
        }
        let resumed = had_head || maps.packs_already_mapped > 0;
        if resumed {
            info!("resuming an interrupted upgrade from the current legacy index");
        }

        // 5. Base and head.
        self.storage.create_dir(index_store::HEAD_PREFIX)?;
        self.storage.create_dir(index_store::INDEX_OBJECT_DIR)?;
        self.set_pending_sweep(PendingSweep::Initial);
        let head = self.put_base(None, false, super::index_write::FoldRequest::Full, None)?;

        // 6. The version switch.
        self.switch_version(target)?;
        // 7. Leftovers.
        self.remove_legacy_index();

        let base_bytes = head.base.as_ref().map_or(0, |b| b.size);
        self.index_generation = head.generation;
        self.index_head = Some(head);
        self.index_dirty = false;
        Ok(UpgradeOutcome::Upgraded {
            from: from.version(),
            to: target.version(),
            chunk_hash: target.chunk_hash(),
            entries,
            base_bytes,
            packs_mapped: maps.packs_mapped,
            packs_unmapped: maps.packs_skipped_too_many,
            resumed,
        })
    }

    /// The v2/v3 index at `index`, on a handle opened with
    /// [`super::OpenOptions::legacy_format`]. Errors on any other format, and
    /// on a segmented head at `index` (written by a development build).
    #[doc(hidden)]
    pub fn legacy_index(&self) -> Result<LegacyChunkIndex> {
        if !self.format.is_upgrade_only() {
            return Err(VykarError::Other(format!(
                "format v{} has no legacy index",
                self.format.version()
            )));
        }
        let data = self.storage.get("index")?;
        if data.as_deref().is_some_and(is_index_head) {
            return Err(VykarError::Other(
                "`index` holds the segmented index head of an interrupted upgrade by a \
                 development build; restore `index` from a copy of the repository"
                    .into(),
            ));
        }
        Ok(decode_index_blob(data, self.crypto.as_ref())?.1)
    }

    /// Step 6: the version switch. Fence check, then the `config` PUT of
    /// the handle's config as the v6 map (its hash is already the target's).
    fn switch_version(&mut self, target: RepoFormat) -> Result<()> {
        let mut config = self.config.clone();
        config.version = target.version();
        config.chunk_hash = target.chunk_hash().as_str().to_string();
        config.features = Vec::new();
        let bytes = config.encode()?;
        self.check_lock_fence()?;
        self.storage.put("config", &bytes)?;
        self.config = config;
        self.format = target;
        Ok(())
    }

    /// Best-effort deletion, after the version switch, of `index`,
    /// `index.gen` and the local `index_blob` copy an older vykar left.
    fn remove_legacy_index(&self) {
        for key in ["index", "index.gen"] {
            match self.storage.exists(key) {
                Ok(false) => {}
                Ok(true) => {
                    if let Err(e) = self.storage.delete(key) {
                        tracing::warn!("failed to delete {key}: {e}");
                    }
                }
                Err(e) => tracing::warn!("failed to check for {key}: {e}"),
            }
        }
        if let Some(blob) =
            dedup_cache::index_blob_cache_path(&self.config.id, self.cache_dir_override.as_deref())
        {
            let _ = std::fs::remove_file(blob);
        }
    }
}
