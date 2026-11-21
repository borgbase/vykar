//! In-place upgrade from the monolithic index (formats v2/v3) to the
//! segmented one (v4/v5). Runs under the maintenance lock, on a repository
//! opened without its index.
//!
//! 1. Probe the server capability and PUT the stored `config` bytes back
//!    unchanged: a server that refuses overwrites (append-only) fails here,
//!    with the repository untouched.
//! 2. GET `index`, decode the legacy index and write it as a base.
//! 3. Overwrite `index` with the head. From here every older binary fails on
//!    its next index read.
//! 4. Rewrite `config` with the new version, delete `index.gen` and the
//!    local copy of the legacy index blob.
//!
//! A v2/v3 `config` with a head at `index` is an upgrade interrupted after
//! step 3: the head and its chain are verified and step 4 completes. When
//! the head or its chain is damaged, step 4 (without the deletions) still
//! completes, so that `check --repair` can repair the repository as v4/v5,
//! and the upgrade returns an error that says so. Every released binary
//! already fails on such a repository, so the switch loses nothing.

use tracing::{debug, info};

use super::format::{ObjectType, unpack_object_expect_with_context};
use super::index_store::{self, StoredHead};
use super::{INDEX_OBJECT_CONTEXT, RepoConfig, RepoFormat, Repository};
use crate::compress;
use crate::index::dedup_cache::{self, MmapFullIndexCache, generation_for_state};
use crate::index::head::IndexHead;
use crate::index::{ChunkIndex, IndexBlob};
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

impl RepoFormat {
    /// The format with the same chunk hash and the segmented index, or
    /// `None` if this one already has it.
    pub fn segmented_successor(self) -> Option<Self> {
        match self {
            Self::V2 => Some(Self::V4),
            Self::V3 => Some(Self::V5),
            Self::V4 | Self::V5 => None,
        }
    }
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
/// 0, empty). The only reader of the monolithic index, kept for the upgrade
/// and for repairing a v4/v5 `index` an older binary overwrote.
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

/// Whether `data` (the object at `index`) is a segmented index head.
pub(crate) fn is_index_head(data: &[u8]) -> bool {
    data.first() == Some(&(ObjectType::IndexHead as u8))
}

impl Repository {
    /// Upgrade a v2/v3 repository to v4/v5 in place; see the module docs.
    /// The caller holds the maintenance lock. One-way: older binaries cannot
    /// read the repository afterwards.
    pub fn upgrade_index_layout(&mut self) -> Result<UpgradeOutcome> {
        let from = self.format;
        let Some(target) = from.segmented_successor() else {
            return Ok(UpgradeOutcome::AlreadyCurrent {
                version: from.version(),
            });
        };

        // 1. Pre-flight.
        match self.storage.server_capabilities() {
            Ok(caps) if !caps.segmented_index => {
                return Err(VykarError::Config(format!(
                    "vykar-server (version {}) does not support the segmented index of \
                     format v{}. Upgrade the server first.",
                    caps.version,
                    target.version(),
                )));
            }
            Ok(_) | Err(VykarError::UnsupportedBackend(_)) => {}
            Err(e) => return Err(e),
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
                 `config` and `index`; on an append-only server, disable append-only \
                 for the upgrade. Nothing was changed."
            ))
        })?;

        // 2–3. Base and head, unless an interrupted upgrade already wrote them.
        let path = self.full_cache_path();
        let (head, resumed) = match self.storage.get("index")? {
            Some(data) if is_index_head(&data) => {
                info!("completing an interrupted upgrade");
                match self.verify_interrupted_upgrade(&data)? {
                    Ok(head) => (head, true),
                    Err(damage) => {
                        // The head proves an upgrade wrote it; finish the
                        // switch so `check --repair` can repair the index.
                        self.switch_version(stored, target)?;
                        return Err(VykarError::Other(format!(
                            "completed an interrupted upgrade to format v{}, but the index it \
                             wrote is damaged ({damage}); run `vykar check --repair` (with \
                             `--dry-run` first to see what it would change)",
                            target.version()
                        )));
                    }
                }
            }
            data => {
                let (_, index) = decode_index_blob(data, self.crypto.as_ref()).map_err(|e| {
                    VykarError::Other(format!(
                        "the v{} index cannot be read ({e}); nothing was changed. Restore \
                         `index` from a copy of the repository, or use the vykar release \
                         that wrote it",
                        from.version()
                    ))
                })?;
                debug!(entries = index.len(), "legacy index loaded");
                (self.write_upgraded_index(&index)?, false)
            }
        };
        if let Some(scratch) = self.index_scratch.take() {
            self.index_scratch = index_store::install_cache(scratch, path.as_deref());
        }

        // 4. The version switch.
        self.switch_version(stored, target)?;
        self.remove_legacy_leftovers();

        let entries = head.entries;
        let base_bytes = head.base.as_ref().map_or(0, |b| b.size);
        self.index_generation = generation_for_state(&head.state_hash);
        self.index_head = Some(head);
        self.index_dirty = false;
        // The dedup and restore caches carry the old generation.
        self.local_caches_stale = true;
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
    /// on a head at `index` (an interrupted upgrade).
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
                "`index` holds the head of an interrupted upgrade; run `vykar upgrade`".into(),
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

    /// Best-effort deletion of `index.gen` and of the local `index_blob`
    /// copy an older vykar left, after the version switch.
    fn remove_legacy_leftovers(&self) {
        if let Err(e) = self.storage.delete("index.gen") {
            tracing::warn!("failed to delete index.gen: {e}");
        }
        if let Some(blob) =
            dedup_cache::index_blob_cache_path(&self.config.id, self.cache_dir_override.as_deref())
        {
            let _ = std::fs::remove_file(blob);
        }
    }

    /// The head an interrupted upgrade left at `index`, with every object of
    /// its chain fetched and verified (`Ok(Ok(head))`). `Ok(Err(e))` when the
    /// head or its chain is damaged and the switch should complete anyway.
    /// `Err` (nothing changed) for a storage error, for a head of an unknown
    /// layout or type, and for an unreadable head with an empty `index.d/`.
    fn verify_interrupted_upgrade(
        &mut self,
        data: &[u8],
    ) -> Result<std::result::Result<IndexHead, VykarError>> {
        let head = match index_store::classify_head(data, self.crypto.as_ref()) {
            StoredHead::Head(head) => head,
            StoredHead::Unreadable(e) => {
                let names = super::index_write::list_objects_for_repair(self.storage.as_ref())?;
                if names.is_empty() {
                    return Err(VykarError::Other(format!(
                        "`config` names format v{}, and `index` starts like the segmented \
                         index head of an interrupted upgrade, but it cannot be read ({e}) \
                         and index.d/ holds no index objects: `index` is more likely a \
                         damaged monolithic index. Nothing was changed; restore `index` from \
                         a copy of the repository",
                        self.config.version
                    )));
                }
                return Ok(Err(e));
            }
            other => other.into_head()?,
        };
        match self.sync_index(Some(head), false) {
            Ok(synced) => {
                // Nothing here reads the index; only the cache file is kept.
                let index_store::SyncedIndex {
                    head,
                    cache,
                    scratch,
                    ..
                } = synced;
                drop(cache);
                self.index_scratch = scratch;
                Ok(Ok(head))
            }
            Err(e) if index_store::is_object_damage(&e) => Ok(Err(e)),
            Err(e) => Err(e),
        }
    }

    /// Write `index` as a base (none for an empty index) and the head naming
    /// it; debris of an earlier attempt in `index.d/` is deleted after the
    /// head is in place. The new full cache is left in `index_scratch`.
    fn write_upgraded_index(&mut self, index: &ChunkIndex) -> Result<IndexHead> {
        self.storage.create_dir(index_store::INDEX_OBJECT_DIR)?;
        let path = self.full_cache_path();
        let anchor = index_store::cache_anchor(path.as_deref());
        let next = dedup_cache::full_cache_from_index_to_temp(index, &anchor)?;
        let cache = MmapFullIndexCache::open_verified(&next)
            .ok_or_else(|| VykarError::Other("new index state failed verification".into()))?;
        let head = self.put_base(None, &cache)?;
        drop(cache);
        self.index_scratch = Some(next);
        Ok(head)
    }
}
