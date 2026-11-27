//! In-place upgrade from the monolithic index (formats v2/v3) to format v6
//! (v2 keeps BLAKE2b IDs, v3 BLAKE3; the config names the hash), on a
//! repository opened without its index. A v2/v3 repository is upgraded
//! under the legacy lock key ([`LegacyUpgradeLock`]: 0.24's lock format,
//! rewritten every few minutes, checked before every write) and as an
//! exclusive session (10-single-protocol step 8); a v6 one (the resume after
//! the switch, a mode switch) as an exclusive session.
//!
//! 1. Probe the server capabilities and PUT the stored `config` bytes back
//!    unchanged: a server that refuses overwrites (append-only) fails here,
//!    with the repository untouched. Then decide the writer mode the switch
//!    records (10-single-protocol step 5): `--commit-mode single` without a
//!    check, otherwise the init-time put-if-absent check (`--commit-mode
//!    multi` refuses unless it passes). Undecided stops here, with nothing
//!    but the check's probes under `sessions/` written.
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
//! 6. Rewrite `config` as the v6 map, with `multi_writer` when step 1
//!    decided multi-writer. From here every older binary refuses the
//!    repository by version.
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

use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::control::CancelToken;
use super::format::{ObjectType, unpack_object_expect_with_context};
use super::host_env::HostEnv;
use super::index_store;
use super::index_write::PendingSweep;
use super::sessions::{
    EntryAge, EntryHost, LEGACY_LOCKS_PREFIX, SameHost, format_age_since, same_host,
};
use super::writer::FenceFn;
use super::writer_mode::{ModeCheck, init_time_check};
use super::{DecodedConfig, INDEX_OBJECT_CONTEXT, Repo, RepoConfig, RepoFormat, WriterMode};
use crate::compress;
use std::sync::Arc;

use crate::index::dedup_cache;
use crate::index::table::{self, ChunkTable};
use crate::index::{IndexBlob, LegacyChunkIndex, LiveIndex};
use vykar_crypto::CryptoEngine;
use vykar_storage::{StorageBackend, StoreNow};
use vykar_types::error::{LockHolder, LockHolderDetails, Result, VykarError};

/// The writer mode a [`UpgradeOutcome`] leaves the repository in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterModeReport {
    /// The mode `config` records after this run.
    pub mode: WriterMode,
    /// This run wrote it (the v2/v3 switch, or `--commit-mode` changing it).
    pub changed: bool,
    /// Why the repository is single-writer when the init-time check decided
    /// it (not when `--commit-mode single` asked for it).
    pub single_reason: Option<String>,
    /// The repository is single-writer and this handle's storage offers
    /// put-if-absent (as far as it knows without a request): `vykar upgrade
    /// --commit-mode multi` could switch it.
    pub put_if_absent_offered: bool,
}

/// What [`super::RepoWriter::upgrade_index_layout`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeOutcome {
    /// The repository already uses the current format.
    AlreadyCurrent {
        version: u32,
        writer_mode: WriterModeReport,
    },
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
        /// The mode the switch recorded.
        writer_mode: WriterModeReport,
    },
}

/// The error of a put-if-absent check that could not decide.
fn undecided(e: VykarError, what: &str) -> VykarError {
    VykarError::Other(format!(
        "could not decide whether the storage honours put-if-absent ({e}); nothing was \
         changed. Retry {what}"
    ))
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

impl Repo {
    /// Upgrade a v2/v3 repository to v6 in place; see the module docs.
    /// The caller runs as an exclusive session, holding the legacy lock key
    /// too on v2/v3; their check is `fence`
    /// (`RepoWriter::upgrade_index_layout`). One-way: older binaries cannot
    /// read the repository afterwards.
    ///
    /// `requested`: the writer mode to record (`--commit-mode`); `None`:
    /// the init-time check decides. On a v6 repository a `Some` switches
    /// the mode ([`Self::set_writer_mode_under`]).
    pub(super) fn upgrade_index_layout_under(
        &mut self,
        fence: &super::Fence,
        requested: Option<WriterMode>,
    ) -> Result<UpgradeOutcome> {
        let from = self.format;
        let Some(target) = from.upgrade_target() else {
            // An upgrade interrupted after its switch left these behind.
            self.remove_legacy_index();
            let writer_mode = match requested {
                None => self.writer_mode_report(false, None),
                Some(want) => self.set_writer_mode_under(want, fence)?,
            };
            return Ok(UpgradeOutcome::AlreadyCurrent {
                version: from.version(),
                writer_mode,
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
        // A lost exclusion (the write check: the legacy lock key, the
        // exclusive session's marker) is reported as itself.
        self.storage
            .put("config", &config_bytes)
            .map_err(|e| match e {
                e @ (VykarError::LockExpired(_) | VykarError::MarkerLostDuringCommit) => e,
                e => VykarError::Other(format!(
                    "the repository config cannot be rewritten ({e}). The upgrade rewrites \
                 `config` and deletes `index`; on an append-only server, disable \
                 append-only for the upgrade. Nothing was changed."
                )),
            })?;
        // The writer mode `switch_version` records (10-single-protocol step
        // 5), decided before anything else is written. The upgrade's own
        // head below is a plain PUT: the positional config makes the handle
        // single-writer, and the legacy lock key is held.
        let (mode, single_reason) = match requested {
            Some(WriterMode::Single) => (WriterMode::Single, None),
            Some(WriterMode::Multi) => match init_time_check(self.storage.as_ref()) {
                Ok(ModeCheck::Multi) => (WriterMode::Multi, None),
                Ok(ModeCheck::Single { reason }) => {
                    return Err(VykarError::Other(format!(
                        "the storage does not pass the put-if-absent check ({reason}), so the \
                         repository cannot be multi-writer; nothing was changed. Run without \
                         --commit-mode multi to upgrade it as single-writer"
                    )));
                }
                Err(e) => return Err(undecided(e, "the upgrade")),
            },
            None => match init_time_check(self.storage.as_ref()) {
                Ok(ModeCheck::Multi) => (WriterMode::Multi, None),
                Ok(ModeCheck::Single { reason }) => (WriterMode::Single, Some(reason)),
                Err(e) => return Err(undecided(e, "the upgrade")),
            },
        };

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
        let head = self.put_base(
            None,
            false,
            super::index_write::FoldRequest::Full,
            None,
            fence,
        )?;

        // 6. The version switch.
        self.switch_version(target, mode, fence)?;
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
            writer_mode: self.writer_mode_report(true, single_reason),
        })
    }

    /// The report of the mode `config` now records.
    fn writer_mode_report(&self, changed: bool, single_reason: Option<String>) -> WriterModeReport {
        let mode = self.writer_mode();
        WriterModeReport {
            mode,
            changed,
            single_reason,
            put_if_absent_offered: mode == WriterMode::Single
                && self.storage_offers_put_if_absent(),
        }
    }

    /// Switch a v6 repository's writer mode to `want` (`vykar upgrade
    /// --commit-mode`, 10-single-protocol step 5). The caller runs in an
    /// exclusive session, whose write check is `fence`, and so has refused
    /// live sessions. A no-op (nothing written, no check) when the mode already
    /// matches. To multi-writer the init-time check must pass on this
    /// handle; to single-writer no check runs, so a client whose storage
    /// fails it can switch. Only the one feature of `config` changes.
    ///
    /// # Errors
    ///
    /// An append-only server (refused before any write), a failed or
    /// undecided check, a config changed since open, a lost fence, a failed
    /// `config` PUT; nothing was changed in each case.
    pub(super) fn set_writer_mode_under(
        &mut self,
        want: WriterMode,
        fence: &super::Fence,
    ) -> Result<WriterModeReport> {
        if self.writer_mode() == want {
            return Ok(self.writer_mode_report(false, None));
        }
        // Before the check, so the switch writes no probe there.
        if self.storage.server_ops().is_some_and(|s| s.append_only()) {
            return Err(VykarError::Other(
                "the repository config cannot be rewritten: the repository server is \
                 append-only (it refuses overwrites). Switching the commit mode rewrites \
                 config; disable append-only for the switch. Nothing was changed"
                    .into(),
            ));
        }
        if want == WriterMode::Multi {
            match init_time_check(self.storage.as_ref()) {
                Ok(ModeCheck::Multi) => {}
                Ok(ModeCheck::Single { reason }) => {
                    return Err(VykarError::Other(format!(
                        "the storage does not pass the put-if-absent check ({reason}); the \
                         repository stays single-writer. Nothing was changed"
                    )));
                }
                Err(e) => return Err(undecided(e, "the switch")),
            }
        }
        let stored = self
            .storage
            .get("config")?
            .ok_or_else(|| VykarError::RepoNotFound("config not found".into()))?;
        let unchanged = matches!(
            RepoConfig::decode(&stored),
            Ok(DecodedConfig::Map(c)) if c.id == self.config.id
                && c.writer_mode() == self.writer_mode()
        );
        if !unchanged {
            return Err(VykarError::Other(
                "the repository config changed after it was opened; retry".into(),
            ));
        }
        let mut config = self.config.clone();
        config.set_writer_mode(want);
        let bytes = config.encode()?;
        fence.check()?;
        self.storage.put("config", &bytes).map_err(|e| {
            if matches!(
                e,
                VykarError::LockExpired(_) | VykarError::MarkerLostDuringCommit
            ) {
                // A lost exclusion (the write check) is reported as itself.
                return e;
            }
            VykarError::Other(format!(
                "the repository config cannot be rewritten ({e}). Switching the commit mode \
                 rewrites config; on an append-only server, disable append-only for the \
                 switch. Nothing was changed"
            ))
        })?;
        info!(
            mode = want.as_str(),
            "switched the repository's commit mode"
        );
        self.config = config;
        // The cached decision no longer applies.
        self.commit_mode = None;
        Ok(self.writer_mode_report(true, None))
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
    /// the handle's config as the v6 map (its hash is already the target's)
    /// recording `mode`.
    fn switch_version(
        &mut self,
        target: RepoFormat,
        mode: WriterMode,
        fence: &super::Fence,
    ) -> Result<()> {
        let mut config = self.config.clone();
        config.version = target.version();
        config.chunk_hash = target.chunk_hash().as_str().to_string();
        config.features = Vec::new();
        config.set_writer_mode(mode);
        let bytes = config.encode()?;
        fence.check()?;
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

// ── The legacy lock key (10-single-protocol step 6) ─────────────────────

/// 0.24 reaps a lock whose `time` is older than this on its own clock
/// (v0.24.0 `DEFAULT_STALE_LOCK_SECS`); also the age after which the
/// acquisition reaps a foreign lock (on the store's clock).
const LEGACY_LOCK_STALE: chrono::Duration = chrono::Duration::hours(6);
/// A check rewrites the key with a fresh `time` once this long has passed
/// since its last rewrite, so at every write the key's `time` is at most
/// this old (well under an hour), and a lapse counted from the last rewrite
/// is counted from at most this long before the last check. Its cost: a
/// write after a gap of this long or more costs an EXISTS and a PUT of the
/// key before it (at most 12 rewrites an hour).
const LEGACY_LOCK_REWRITE_SECS: i64 = 5 * 60;
/// A check fails once the last successful rewrite is older than this
/// (0.24's limit for an unrefreshed lock: 6 h − 30 min).
const LEGACY_LOCK_MAX_UNREWRITTEN_SECS: i64 = 6 * 60 * 60 - 30 * 60;

/// The body of a lock key: exactly the JSON 0.24 parses (`hostname`,
/// `pid`, `time` as RFC 3339, `boot_id`) plus this binary's identity
/// fields, which 0.24 ignores, in the field order of 0.24's lock entry.
#[derive(Debug, Serialize, Deserialize)]
struct LegacyLockEntry {
    hostname: String,
    pid: u32,
    time: String,
    #[serde(default)]
    boot_id: Option<String>,
    #[serde(default)]
    installation_id: Option<String>,
    #[serde(default)]
    pid_ns: Option<u64>,
}

impl LegacyLockEntry {
    fn new(env: &HostEnv, time: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            hostname: env.host.hostname.clone(),
            pid: env.host.pid,
            time: time.to_rfc3339(),
            boot_id: env.host.boot_id.clone(),
            installation_id: env.host.installation_id.clone(),
            pid_ns: env.host.pid_ns,
        }
    }

    fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| VykarError::Other(format!("lock serialize: {e}")))
    }
}

/// The lock key a v2/v3 upgrade holds: a 0.24 advisory lock, so 0.24
/// backups, reads and maintenance refuse while the upgrade runs, and a
/// lock of any version makes the upgrade refuse. Its [check](Self::check)
/// runs before every write of the upgrade, after the exclusive session's
/// check (the writer's fence and the session's [`WriteChecked`](super::write_check::WriteChecked) storage
/// wrapper, `commands::util::with_exclusive_session_as`), rewrites the key every few minutes
/// and fails once the key is gone or its last rewrite is 5.5 h old.
///
/// Self-contained, apart from the age and same-host judgements it shares
/// with `sessions`: it outlived this binary's lock (10-single-protocol
/// step 9).
pub(crate) struct LegacyUpgradeLock {
    shared: Arc<LegacyKey>,
}

struct LegacyKey {
    /// The unwrapped backend: the lock's own requests are never checked.
    storage: Arc<dyn StorageBackend>,
    env: HostEnv,
    key: String,
    state: Mutex<LegacyKeyState>,
}

struct LegacyKeyState {
    /// Unix seconds, on `env`'s clock, of the last successful write of the key.
    last_rewrite: i64,
    /// A check found the key gone: every later check fails, release sends nothing.
    lost: bool,
    /// A rewrite has failed (warned once).
    rewrite_failed: bool,
}

impl LegacyUpgradeLock {
    /// Write the key, list `locks/`, reap stale foreign locks and refuse
    /// (`Locked`, naming the first live one) beside a live one; the key is
    /// deleted again on every failure.
    ///
    /// The key is written before the listing, not after a listing that
    /// found no holder: judging a foreign lock's age needs "now" on the
    /// store's clock, which the key's own mtime gives. One listing is
    /// enough: a contender whose write completed before it is in it, one
    /// whose write came later lists after it and sees this key.
    pub(crate) fn acquire(storage: Arc<dyn StorageBackend>, env: &HostEnv) -> Result<Self> {
        let now = env.now_utc();
        let body = LegacyLockEntry::new(env, now).encode()?;
        // 0.24's and this binary's key shape; the prefix is kept across rewrites.
        let key = format!(
            "{LEGACY_LOCKS_PREFIX}{:020}-{:032x}.json",
            now.timestamp_micros(),
            rand::random::<u128>()
        );
        let undo = |e: VykarError| {
            // A PUT can land and still fail; the key is uniquely named.
            if let Err(del) = storage.delete(&key) {
                debug!(key, error = %del, "could not delete the upgrade's lock key");
            }
            e
        };
        storage.put(&key, &body).map_err(undo)?;
        let listing = storage.list_meta(LEGACY_LOCKS_PREFIX).map_err(undo)?;
        if !listing.iter().any(|o| o.key == key) {
            return Err(undo(VykarError::Other(
                "lock acquisition failed: written lock key not returned by LIST".into(),
            )));
        }
        let store_now = StoreNow::of_written(&listing, &key);
        let local_now = env.now_utc();
        let mut live = Vec::new();
        for object in listing.iter().filter(|o| o.key != key) {
            let other = object.key.as_str();
            // Not a lock (probe debris of development builds, say):
            // nothing to judge; `vykar clear-sessions` removes it.
            if !(other.starts_with(LEGACY_LOCKS_PREFIX) && other.ends_with(".json")) {
                continue;
            }
            let Some(data) = storage.get(other).map_err(undo)? else {
                continue;
            };
            let entry = serde_json::from_slice::<LegacyLockEntry>(&data).ok();
            let age = EntryAge::of(object.mtime, store_now, env, || {
                legacy_lock_timestamp(entry.as_ref(), other)
                    .map(|ts| local_now.signed_duration_since(ts))
            });
            let dead_here = entry.as_ref().is_some_and(|e| {
                matches!(
                    same_host(
                        &EntryHost {
                            hostname: &e.hostname,
                            pid: e.pid,
                            boot_id: e.boot_id.as_deref(),
                            installation_id: e.installation_id.as_deref(),
                            pid_ns: e.pid_ns,
                            age: age.as_std(),
                        },
                        env,
                    ),
                    SameHost::DeadProcess | SameHost::EarlierBoot
                )
            });
            if dead_here || age.exceeds(LEGACY_LOCK_STALE) {
                debug!(lock_key = other, "reaping a stale lock");
                storage.delete(other).map_err(undo)?;
            } else {
                live.push(other.to_string());
            }
        }
        live.sort();
        if let Some(holder) = live.first() {
            let holder = legacy_lock_holder(storage.as_ref(), holder, env);
            return Err(undo(VykarError::Locked(holder)));
        }
        Ok(Self {
            shared: Arc::new(LegacyKey {
                storage: Arc::clone(&storage),
                env: env.clone(),
                key,
                state: Mutex::new(LegacyKeyState {
                    last_rewrite: now.timestamp(),
                    lost: false,
                    rewrite_failed: false,
                }),
            }),
        })
    }

    pub(crate) fn key(&self) -> &str {
        &self.shared.key
    }

    /// Tests: one check ([`Self::check_fn`]).
    #[cfg(test)]
    pub(crate) fn check(&self) -> Result<()> {
        self.shared.check()
    }

    /// The check before every write, as a fence function: `LockExpired`
    /// once the key is gone (sticky, no further request), on a clock
    /// anomaly, or once the last successful rewrite is more than 5.5 h old
    /// (wall clock, so a suspended machine fails its next check); otherwise
    /// it rewrites the key when [`LEGACY_LOCK_REWRITE_SECS`] have passed
    /// since the last rewrite. A failed rewrite is warned once and retried
    /// at the next check. Every function returned shares the state.
    pub(crate) fn check_fn(&self) -> FenceFn {
        let shared = Arc::clone(&self.shared);
        Arc::new(move || shared.check())
    }

    /// Delete the key; sends nothing when a check found it gone. The error
    /// is the DELETE's.
    pub(crate) fn release(self) -> Result<()> {
        let lost = self.shared.lock_state().lost;
        if lost {
            return Ok(());
        }
        self.shared.storage.delete(&self.shared.key)
    }
}

impl LegacyKey {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, LegacyKeyState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lost_error(&self) -> VykarError {
        VykarError::LockExpired(format!(
            "the upgrade's lock key {} was removed (`vykar clear-sessions`, or a vykar 0.24 \
             client on a machine with this hostname took it for stale); nothing more is \
             written. Run `vykar upgrade` again",
            self.key
        ))
    }

    fn check(&self) -> Result<()> {
        // Held throughout: concurrent writes wait for one check.
        let mut state = self.lock_state();
        if state.lost {
            return Err(self.lost_error());
        }
        if !self.storage.exists(&self.key)? {
            state.lost = true;
            return Err(self.lost_error());
        }
        let now_secs = self.env.now_utc().timestamp();
        let elapsed = now_secs - state.last_rewrite;
        if elapsed < 0 || now_secs < 0 {
            return Err(VykarError::LockExpired(
                "clock anomaly detected; refusing to write with a lock key vykar 0.24 may \
                 judge stale"
                    .into(),
            ));
        }
        if elapsed > LEGACY_LOCK_MAX_UNREWRITTEN_SECS {
            // The key is still ours: release deletes it.
            return Err(VykarError::LockExpired(format!(
                "the upgrade's lock key was last rewritten {elapsed}s ago (limit \
                 {LEGACY_LOCK_MAX_UNREWRITTEN_SECS}s); a vykar 0.24 client may take it for \
                 stale. Nothing more is written; run `vykar upgrade` again"
            )));
        }
        if elapsed >= LEGACY_LOCK_REWRITE_SECS {
            let written = LegacyLockEntry::new(&self.env, self.env.now_utc())
                .encode()
                .and_then(|body| self.storage.put(&self.key, &body));
            match written {
                Ok(()) => {
                    state.last_rewrite = now_secs;
                    debug!(lock_key = %self.key, "upgrade lock key rewritten");
                }
                Err(e) => {
                    if !state.rewrite_failed {
                        state.rewrite_failed = true;
                        warn!(
                            lock_key = %self.key,
                            "could not rewrite the upgrade's lock key ({e}); retrying at the \
                             next write"
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

/// When a 0.24-shaped lock was written: its `time`, else the key's
/// 20-digit microsecond prefix.
fn legacy_lock_timestamp(
    entry: Option<&LegacyLockEntry>,
    key: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    entry
        .and_then(|e| chrono::DateTime::parse_from_rfc3339(&e.time).ok())
        .map(|ts| ts.with_timezone(&chrono::Utc))
        .or_else(|| {
            let raw = key
                .strip_prefix(LEGACY_LOCKS_PREFIX)?
                .strip_suffix(".json")?;
            let (micros, _) = raw.split_once('-')?;
            if micros.len() != 20 || !micros.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            chrono::DateTime::from_timestamp_micros(micros.parse().ok()?)
        })
}

/// The holder of `key` for the `Locked` error:
/// no details when it cannot be read; its age since its `time` on `env`'s clock.
fn legacy_lock_holder(storage: &dyn StorageBackend, key: &str, env: &HostEnv) -> LockHolder {
    let entry = storage
        .get(key)
        .ok()
        .flatten()
        .and_then(|data| serde_json::from_slice::<LegacyLockEntry>(&data).ok());
    let details = entry.map(|entry| {
        let age = legacy_lock_timestamp(Some(&entry), key).map_or_else(
            || "unknown".to_string(),
            |ts| format_age_since(&env.now_utc(), &ts),
        );
        LockHolderDetails {
            hostname: entry.hostname,
            pid: entry.pid,
            age,
        }
    });
    LockHolder {
        key: key.to_string(),
        details,
    }
}
