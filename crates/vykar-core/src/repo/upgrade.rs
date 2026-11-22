//! In-place upgrade from the monolithic index (formats v2/v3) to the
//! numbered heads of v6/v7 (v2 → v7, v3 → v6). Runs under the maintenance
//! lock, on a repository opened without its index.
//!
//! 1. Probe the server capabilities and PUT the stored `config` bytes back
//!    unchanged: a server that refuses overwrites (append-only) fails here,
//!    with the repository untouched.
//! 2. GET `index` and decode the legacy index. While `config` is below 6,
//!    this blob is the repository's index; heads already under `index.h/`
//!    are debris of an earlier attempt.
//! 3. Write it as a v1 base under `index.d/` and a head at
//!    `index.h/<highest listed + 1>`. `index` is not touched.
//! 4. Rewrite `config` with the new version. From here every older binary
//!    refuses the repository by version.
//! 5. Delete `index`, `index.gen` and the local copy of the legacy index
//!    blob, best-effort.
//!
//! Why this order is safe: until the switch in step 4 an older binary keeps
//! working on the repository, because the legacy index is untouched. A
//! resume always rebuilds from the current legacy index, so writes an old
//! client made in between (under a lock this upgrade lost) are not dropped.
//! The debris of an interrupted attempt (heads, base) is collected by age.
//! An upgrade interrupted after the switch finds a current format and only
//! retries the deletions of step 5.

use tracing::{debug, info};

use super::format::{ObjectType, unpack_object_expect_with_context};
use super::index_store;
use super::index_write::PendingSweep;
use super::{INDEX_OBJECT_CONTEXT, RepoConfig, RepoFormat, Repository};
use crate::compress;
use std::sync::Arc;

use crate::index::dedup_cache::{self, generation_for_state};
use crate::index::head::IndexHead;
use crate::index::{ChunkIndex, ChunkTable, IndexBlob, LiveIndex};
use vykar_crypto::CryptoEngine;
use vykar_types::error::{Result, VykarError};

/// What [`Repository::upgrade_index_layout`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeOutcome {
    /// The repository already uses the segmented index.
    AlreadyCurrent { version: u32 },
    Upgraded {
        from: u32,
        to: u32,
        /// Index entries written to the base.
        entries: u64,
        /// Stored size of the base (0 for an empty index).
        base_bytes: u64,
        /// An interrupted upgrade was completed: the head was already in place.
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
) -> Result<(u64, ChunkIndex)> {
    let Some(data) = data else {
        return Ok((0, ChunkIndex::new()));
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
    /// Upgrade a v2/v3 repository to v7/v6 in place; see the module docs.
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
        let stored: RepoConfig = rmp_serde::from_slice(&config_bytes)?;
        if stored.version != from.version() || stored.id != self.config.id {
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

        // 2. The legacy index: while `config` is below 6, the index.
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
        let resumed = !index_store::list_head_generations(self.storage.as_ref())?.is_empty();
        if resumed {
            info!("resuming an interrupted upgrade from the current legacy index");
        }

        // 3. Base and head.
        let head = self.write_upgraded_index(&index)?;
        drop(index);
        // The state written above, as a scratch table: publish it.
        let written = std::mem::take(&mut self.chunk_index);
        if let Some(table) = written.table().cloned() {
            drop(written);
            if let Some(table) = Arc::into_inner(table) {
                let table = index_store::publish_table(table, self.cache_dir().as_deref());
                self.chunk_index = LiveIndex::from_table(Arc::new(table));
            }
        }

        // 4. The version switch.
        self.switch_version(stored, target)?;
        // 5. Leftovers.
        self.remove_legacy_index();

        let entries = head.entries;
        let base_bytes = head.base.as_ref().map_or(0, |b| b.size);
        self.index_generation = generation_for_state(&head.state_hash);
        self.index_head = Some(head);
        self.index_dirty = false;
        Ok(UpgradeOutcome::Upgraded {
            from: from.version(),
            to: target.version(),
            entries,
            base_bytes,
            resumed,
        })
    }

    /// The v2/v3 index at `index`, on a handle opened with
    /// [`super::OpenOptions::legacy_format`]. Errors on any other format, and
    /// on a segmented head at `index` (written by a development build).
    #[doc(hidden)]
    pub fn legacy_index(&self) -> Result<ChunkIndex> {
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

    /// Step 4: the version switch. Fence check, then the `config` PUT.
    fn switch_version(&mut self, mut stored: RepoConfig, target: RepoFormat) -> Result<()> {
        stored.version = target.version();
        self.check_lock_fence()?;
        self.storage.put("config", &rmp_serde::to_vec(&stored)?)?;
        self.config.version = stored.version;
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

    /// Write `index` as a base (none for an empty index) and the head naming
    /// it, at the generation above every listed head. The new state is left
    /// as the handle's index, in a scratch table.
    fn write_upgraded_index(&mut self, index: &ChunkIndex) -> Result<IndexHead> {
        self.storage.create_dir(index_store::HEAD_PREFIX)?;
        self.storage.create_dir(index_store::INDEX_OBJECT_DIR)?;
        let anchor = index_store::cache_anchor(self.cache_dir().as_deref());
        let next = dedup_cache::full_cache_from_index_to_temp(index, &anchor)?;
        let table = ChunkTable::scratch(next)
            .map_err(|_| VykarError::Other("new index state failed verification".into()))?;
        self.set_pending_sweep(PendingSweep::Initial);
        let head = self.put_base(None, table.cache())?;
        self.chunk_index = LiveIndex::from_table(Arc::new(table));
        Ok(head)
    }
}
