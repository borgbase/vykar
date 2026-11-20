//! In-place upgrade from the monolithic index (formats v2/v3) to the
//! segmented one (v4/v5). Runs under the maintenance lock, on a repository
//! opened without its index.
//!
//! 1. Probe the server capability and PUT the stored `config` bytes back
//!    unchanged: a server that refuses overwrites (append-only) fails here,
//!    with the repository untouched.
//! 2. Load the legacy index and write it as a base.
//! 3. Overwrite `index` with the head. From here every older binary fails on
//!    its next index read.
//! 4. Rewrite `config` with the new version, delete `index.gen` and the
//!    local copy of the legacy index blob.
//!
//! A v2/v3 `config` with a head at `index` is an upgrade interrupted after
//! step 3: the head and its chain are verified and step 4 completes.

use tracing::{debug, info};

use super::format::ObjectType;
use super::index_fetch::{self, IndexObject};
use super::index_store;
use super::{RepoConfig, RepoFormat, Repository};
use crate::index::dedup_cache::{self, generation_for_state, MmapFullIndexCache};
use crate::index::head::IndexHead;
use crate::index::ChunkIndex;
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

/// The error every monolithic index read returns when `index` holds a head
/// while `config` still names a monolithic format.
pub(crate) fn interrupted_upgrade_error() -> VykarError {
    VykarError::InvalidFormat(
        "an upgrade to the segmented index format was interrupted; run `vykar upgrade` \
         to complete it"
            .into(),
    )
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
        let object = index_fetch::fetch_index_object(
            self.storage.as_ref(),
            &self.config.id,
            self.cache_dir_override.as_deref(),
        )?;
        let (head, resumed) = match object {
            IndexObject::Remote(Some(data)) if is_index_head(&data) => {
                let head = match index_store::classify_head(&data, self.crypto.as_ref()) {
                    index_store::StoredHead::Unreadable(e) => {
                        return Err(VykarError::Other(format!(
                            "the index head left by an interrupted upgrade cannot be read \
                             ({e}); run `vykar check --repair` to rebuild it, then `vykar \
                             upgrade` again"
                        )))
                    }
                    other => other.into_head()?,
                };
                info!("completing an interrupted upgrade");
                // Every object of the chain is fetched and verified.
                let synced = self.sync_index(Some(head), false).map_err(|e| {
                    if index_store::is_object_damage(&e) {
                        VykarError::Other(format!(
                            "{e}; after the repair, run `vykar upgrade` again to complete the \
                             upgrade"
                        ))
                    } else {
                        e
                    }
                })?;
                // Nothing here reads the index; only the cache file is kept.
                let index_store::SyncedIndex {
                    head,
                    cache,
                    scratch,
                    ..
                } = synced;
                drop(cache);
                self.index_scratch = scratch;
                (head, true)
            }
            object => {
                let ((_, index), source) = self.index_from_object(object)?;
                debug!(?source, entries = index.len(), "legacy index loaded");
                (self.write_upgraded_index(&index)?, false)
            }
        };
        if let Some(scratch) = self.index_scratch.take() {
            self.index_scratch = index_store::install_cache(scratch, path.as_deref());
        }

        // 4. The version switch.
        let mut config = stored;
        config.version = target.version();
        self.check_lock_fence()?;
        self.storage.put("config", &rmp_serde::to_vec(&config)?)?;
        self.config.version = config.version;
        self.format = target;
        if let Err(e) = self.storage.delete("index.gen") {
            tracing::warn!("failed to delete index.gen: {e}");
        }
        if let Some(blob) =
            dedup_cache::index_blob_cache_path(&self.config.id, self.cache_dir_override.as_deref())
        {
            let _ = std::fs::remove_file(blob);
        }

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
