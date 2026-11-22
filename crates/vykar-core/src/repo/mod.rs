pub(crate) mod blob_read;
pub(crate) mod bounded;
pub(crate) mod coalesce;
pub mod control;
pub mod file_cache;
pub mod format;
pub mod host_env;
pub mod identity;
pub(crate) mod journal_recovery;
pub mod lock;
pub mod manifest;
pub mod pack;
pub mod pack_map;
pub mod pack_trailer;
pub(crate) mod session_journal;
pub mod snapshot_cache;
pub(crate) mod tree_cache;
mod upload_queue;
pub(crate) mod write_session;

mod chunks;
mod commit;
pub(crate) mod index_rebuild;
pub(crate) mod index_store;
pub(crate) mod index_write;
mod key;
mod open;
pub(crate) mod pack_listing;
mod pack_lookup;
mod pack_recovery;
mod pack_verify;
pub use host_env::{Clock, HostEnv, HostIdentity, StopSignal, SystemClock};
pub use index_rebuild::{RebuildCounts, UncoveredPack};
pub use index_write::{IndexLoss, IndexRecovery, RepairIndexLoad};
pub use key::{LoadedKey, select_repository_key};
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) use open::derive_plaintext_chunk_id_key;
mod read;
pub(crate) use read::TreeChunkSource;
mod session;
pub use session::WriteSessionOptions;
mod upgrade;
pub use upgrade::UpgradeOutcome;

use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use self::write_session::WriteSessionState;
pub use crate::chunker::ChunkerParams;
use crate::index::LiveIndex;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::error::Result;
use vykar_types::hash::HashAlgorithm;

use self::file_cache::FileCache;
use self::manifest::Manifest;

/// The repository layout version, parsed once from `RepoConfig::version`.
///
/// **The version implies the chunk/pack hash algorithm; there is no stored
/// `hash` field.** `RepoConfig` is serialized as a *positional* msgpack array,
/// and rmp-serde errors on an array longer than the struct
/// (`array had incorrect length`) — `#[serde(default)]` only rescues shorter
/// ones. An appended field would therefore make an old binary fail to decode
/// the config *before* it reached the version gate, replacing a clear
/// "unsupported repository version: 3" with a corruption-looking parse error.
/// `repo_config_is_exactly_seven_msgpack_elements` enforces the field count.
///
/// This is not really a conflation of two concepts: `version` answers "what
/// must a reader understand to read this repository?", and a reader computing
/// BLAKE2b chunk IDs *cannot* read a BLAKE3 repository — the storage keys and
/// the entire dedup index differ. The same holds for the index layout: v4 and
/// v5 are v2 and v3 with the segmented index. v2 and v3 are upgrade-only (see
/// [`RepoFormat::is_upgrade_only`]).
///
/// Only chunk IDs and pack IDs follow the format. Every other digest in the
/// workspace is BLAKE3 for all versions — see the "Repository Versions"
/// subsection of `docs/src/content/docs/architecture.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoFormat {
    /// Keyed/unkeyed BLAKE2b-256 chunk and pack IDs, monolithic index;
    /// upgrade-only.
    V2,
    /// Keyed/unkeyed BLAKE3 chunk and pack IDs, monolithic index;
    /// upgrade-only.
    V3,
    /// BLAKE2b-256 chunk and pack IDs, segmented index.
    V4,
    /// BLAKE3 chunk and pack IDs, segmented index.
    V5,
}

impl RepoFormat {
    /// The format `vykar init` creates. Existing repositories keep theirs.
    pub const CURRENT: Self = Self::V5;

    /// The single version gate: an unknown version is rejected here and never
    /// reaches hashing code.
    ///
    /// Deliberately not a `2 => Blake2b, _ => Blake3` mapping — that would
    /// silently hand known semantics to a future version.
    ///
    /// # Errors
    ///
    /// Returns [`vykar_types::error::VykarError::UnsupportedVersion`] for any
    /// other version.
    pub fn from_version(version: u32) -> Result<Self> {
        match version {
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            4 => Ok(Self::V4),
            5 => Ok(Self::V5),
            other => Err(vykar_types::error::VykarError::UnsupportedVersion(other)),
        }
    }

    /// The integer written to `RepoConfig::version`.
    pub fn version(self) -> u32 {
        match self {
            Self::V2 => 2,
            Self::V3 => 3,
            Self::V4 => 4,
            Self::V5 => 5,
        }
    }

    /// The algorithm this format uses for chunk IDs and pack IDs.
    pub fn chunk_hash(self) -> HashAlgorithm {
        match self {
            Self::V2 | Self::V4 => HashAlgorithm::Blake2b,
            Self::V3 | Self::V5 => HashAlgorithm::Blake3,
        }
    }

    /// v2 and v3 (monolithic index): opened only by `vykar upgrade`, through
    /// [`OpenOptions::legacy_format`]. Every other open refuses them.
    pub fn is_upgrade_only(self) -> bool {
        matches!(self, Self::V2 | Self::V3)
    }
}

/// Persisted (unencrypted) at the `config` key.
///
/// **Exactly seven fields, deliberately.** See [`RepoFormat`] for why nothing
/// may be appended.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    pub version: u32,
    pub id: Vec<u8>, // 32 bytes
    pub chunker_params: ChunkerParams,
    pub encryption: EncryptionMode,
    pub created: DateTime<Utc>,
    #[serde(default = "default_min_pack_size")]
    pub min_pack_size: u32,
    #[serde(default = "default_max_pack_size")]
    pub max_pack_size: u32,
}

pub(crate) fn default_min_pack_size() -> u32 {
    32 * 1024 * 1024 // 32 MiB
}

pub(crate) fn default_max_pack_size() -> u32 {
    192 * 1024 * 1024 // 192 MiB
}

/// Upper bound on `RepoConfig::max_pack_size`.
pub(crate) const MAX_PACK_SIZE_LIMIT: u32 = 512 * 1024 * 1024;

impl RepoConfig {
    /// The checks every open applies to a decoded `config`, which is
    /// unauthenticated even in an encrypted repository. This is the single
    /// version gate: everything downstream asks the returned [`RepoFormat`],
    /// never the raw integer.
    ///
    /// The chunker parameters are not checked here, so that restore stays
    /// usable; backup checks them (`ChunkerParams::validate_stored`).
    ///
    /// # Errors
    ///
    /// An unknown version, or a `max_pack_size` above the hard limit.
    pub fn validate(&self) -> Result<RepoFormat> {
        let format = RepoFormat::from_version(self.version)?;
        if self.max_pack_size > MAX_PACK_SIZE_LIMIT {
            return Err(vykar_types::error::VykarError::Config(format!(
                "max_pack_size ({}) exceeds hard limit of 512 MiB",
                self.max_pack_size
            )));
        }
        Ok(format)
    }
}

const INDEX_OBJECT_CONTEXT: &[u8] = b"index";

/// The repository key, stored twice.
///
/// Both objects hold the **same serialized bytes**, not two independent wraps:
/// a second `MasterKey::to_encrypted` would pick a fresh salt and nonce, and
/// being able to compare the two copies byte-for-byte is the entire point.
/// Neither is ever rewritten in place on a read path — `check --repair` heals
/// a damaged copy, visibly.
pub const KEY_PRIMARY: &str = "keys/repokey";
/// The redundant copy of [`KEY_PRIMARY`]. Absent on repositories created
/// before redundancy existed, until an open backfills it.
pub const KEY_SECONDARY: &str = "keys/repokey.2";

/// What the two key copies looked like when the repository was opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyCopyState {
    /// Unencrypted repository — there is no key file.
    NotApplicable,
    /// Both copies present and byte-identical.
    Matched,
    /// Exactly one copy was usable. `missing` names the other — either absent,
    /// or present but unreadable. An absent copy is backfilled best-effort;
    /// an unreadable one may be intact and is never overwritten.
    OneMissing { missing: &'static str },
    /// The copies differ. `good` was established as this repository's key;
    /// `bad` was not. Deliberately not self-healed at open: rewriting a key
    /// file is an overwrite, which append-only servers refuse outright, and
    /// silently overwriting a key on a read path is the wrong default anyway.
    Divergent {
        good: &'static str,
        bad: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionMode {
    None,
    Aes256Gcm,
    Chacha20Poly1305,
}

impl EncryptionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            EncryptionMode::None => "none",
            EncryptionMode::Aes256Gcm => "aes256gcm",
            EncryptionMode::Chacha20Poly1305 => "chacha20poly1305",
        }
    }
}

/// What `Repository::init` takes from the client's repository settings.
#[derive(Debug, Clone)]
pub struct InitOptions {
    /// Repository URL. Names the server in the capability-probe error only.
    pub url: Option<String>,
    pub min_pack_size: u32,
    pub max_pack_size: u32,
}

impl Default for InitOptions {
    fn default() -> Self {
        Self {
            url: None,
            min_pack_size: default_min_pack_size(),
            max_pack_size: default_max_pack_size(),
        }
    }
}

/// What opening a repository needs from the client besides its backend and
/// passphrase: where its local caches live, and the identity context.
#[derive(Debug, Clone, Default)]
pub struct OpenRequest {
    /// Repository URL: names the repository in errors and keys the identity pin.
    pub url: String,
    /// Override for the local cache root.
    pub cache_dir: Option<PathBuf>,
    /// Accept a key that contradicts the local identity pin (`--trust-repo`).
    pub trust_repo: bool,
    /// Refuse a repository whose config says it is unencrypted.
    pub require_encryption: bool,
}

/// Options controlling which expensive subsystems are loaded when opening a
/// repository. Both default to `false` (skip). Only `backup` needs both.
#[derive(Clone, Debug, Default)]
pub struct OpenOptions {
    pub load_index: bool,
    pub load_file_cache: bool,
    /// Repository URL, when the caller knows it. Only the identity pin needs
    /// it, and `Repository::open` itself has no other way to learn it.
    pub repo_url: Option<String>,
    /// Accept a key whose fingerprint contradicts the local identity pin.
    /// Mirrors `--trust-repo`, the single deliberate escape hatch.
    pub trust_repo: bool,
    /// Suppress the best-effort backfill of a missing key copy. Set by
    /// `check --repair --dry-run`, which prints "no changes applied" and must
    /// not have written one underneath that statement.
    pub skip_key_backfill: bool,
    /// Refuse a repository whose config says it is unencrypted. The repo
    /// config is unauthenticated, so only the client's own config may opt
    /// into plaintext.
    pub require_encryption: bool,
    /// Open a segmented repository whose index head cannot be read (an
    /// older binary overwrote it). Set by `check --repair`, which rebuilds
    /// the index; every other command refuses such a repository.
    pub tolerate_broken_index: bool,
    /// Open a v2/v3 repository instead of refusing it. Only `vykar upgrade`
    /// sets this. Such a handle supports `upgrade_index_layout` and
    /// `legacy_index` only; it has no head, and `load_index` is refused.
    pub legacy_format: bool,
    /// Clock and identity for the lock, session and maintenance code.
    /// Default: [`HostEnv::system`]. The simulator sets one per client.
    pub host_env: HostEnv,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_index(mut self) -> Self {
        self.load_index = true;
        self
    }
    pub fn with_file_cache(mut self) -> Self {
        self.load_file_cache = true;
        self
    }
    pub fn with_repo_url(mut self, url: impl Into<String>) -> Self {
        self.repo_url = Some(url.into());
        self
    }
    pub fn trust_repo(mut self, trust: bool) -> Self {
        self.trust_repo = trust;
        self
    }
    pub fn skip_key_backfill(mut self) -> Self {
        self.skip_key_backfill = true;
        self
    }
    pub fn require_encryption(mut self, require: bool) -> Self {
        self.require_encryption = require;
        self
    }
    pub fn tolerate_broken_index(mut self) -> Self {
        self.tolerate_broken_index = true;
        self
    }
    pub fn legacy_format(mut self) -> Self {
        self.legacy_format = true;
        self
    }
    pub fn host_env(mut self, env: HostEnv) -> Self {
        self.host_env = env;
        self
    }
}

/// A handle to an opened repository.
pub struct Repository {
    pub storage: Arc<dyn StorageBackend>,
    pub crypto: Arc<dyn CryptoEngine>,
    manifest: Manifest,
    /// Snapshots present under `snapshots/` that could not be read into the
    /// manifest. Non-empty means `manifest` is an incomplete view of the
    /// repository — see [`Repository::skipped_snapshots`].
    skipped_snapshots: Vec<snapshot_cache::SkippedSnapshot>,
    /// The chunk index: the mapped table of the loaded state plus this
    /// process's edits. Not loaded (empty) until a load or write.
    chunk_index: LiveIndex,
    pub config: RepoConfig,
    /// Resolved once from `config.version` at init/open. Every downstream
    /// caller asks the repository rather than re-inspecting the integer.
    format: RepoFormat,
    file_cache: FileCache,
    /// Generation of the loaded head's state (`generation_for_state`), the
    /// value full index cache headers carry; 0 when there is no head.
    index_generation: u64,
    /// Whether the chunk index has been modified since last persist.
    index_dirty: bool,
    /// Whether the file cache has been modified since last persist.
    file_cache_dirty: bool,
    /// Override for the cache directory root (from config `cache_dir`).
    cache_dir_override: Option<PathBuf>,
    /// Local tree-chunk cache; None without a cache dir.
    tree_cache: Option<tree_cache::TreeChunkCache>,
    /// Clock and identity of this client; see [`host_env`].
    host_env: HostEnv,
    /// Write-path state: pack writers, upload queue, dedup, journal, sizing.
    /// `None` when no write session is active (read-only operations, compact, delete, prune).
    /// Activated by `begin_write_session()` before backup.
    write_session: Option<WriteSessionState>,
    /// Lock fence: called before persisting index/manifest to verify the lock is still valid.
    lock_fence: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
    /// The key-copy bytes that actually unwrapped at open. `check` compares
    /// the stored objects against these instead of unwrapping again, which
    /// would cost another Argon2id derivation and need the passphrase threaded
    /// into the scan. `None` for unencrypted repositories.
    key_blob: Option<Vec<u8>>,
    /// The head the in-memory state was loaded from (or written as). `None`
    /// on a handle opened for the upgrade, and while `check --repair`
    /// rebuilds the index.
    index_head: Option<crate::index::head::IndexHead>,
    /// When chains are folded into a new base.
    fold_policy: index_write::FoldPolicy,
    /// Make the next index write a fold.
    fold_requested: bool,
}

impl Repository {
    /// Clock and identity of this client.
    pub fn host_env(&self) -> &HostEnv {
        &self.host_env
    }

    /// Replace the environment, e.g. on a handle from `Repository::init`.
    pub fn set_host_env(&mut self, env: HostEnv) {
        self.host_env = env;
    }

    /// The key blob that unwrapped at open, if this repository is encrypted.
    pub fn key_blob(&self) -> Option<&[u8]> {
        self.key_blob.as_deref()
    }

    /// Mark the chunk index as needing persistence on the next `save_state()`.
    pub fn mark_index_dirty(&mut self) {
        self.index_dirty = true;
    }

    /// Mark the file cache as needing persistence on the next `save_state()`.
    pub fn mark_file_cache_dirty(&mut self) {
        self.file_cache_dirty = true;
    }

    /// Save the file cache to local disk if it has been modified.
    /// Returns `Ok(())` if no save was needed or the save succeeded.
    pub fn save_file_cache_if_dirty(&mut self) -> Result<()> {
        if !self.file_cache_dirty {
            return Ok(());
        }
        self.file_cache.save(
            &self.config.id,
            self.crypto.as_ref(),
            self.cache_dir_override.as_deref(),
        )?;
        self.file_cache_dirty = false;
        Ok(())
    }

    /// The repository layout format, resolved at open time.
    pub fn format(&self) -> RepoFormat {
        self.format
    }

    /// The content-digest algorithm this repository's chunk IDs and pack IDs
    /// use. Implied by the format version and fixed for the repository's life.
    pub fn content_hash(&self) -> HashAlgorithm {
        self.format.chunk_hash()
    }

    // ----- Accessors for private fields -----

    /// Generation of the loaded index state.
    pub fn index_generation(&self) -> u64 {
        self.index_generation
    }

    /// Read-only access to the manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Snapshots that exist under `snapshots/` but could not be read into the
    /// manifest — most commonly because they were written by a newer vykar.
    ///
    /// A non-empty result means [`Repository::manifest`] is an **incomplete**
    /// view of the repository. Callers that show a snapshot list to a user must
    /// surface this rather than rendering a silently truncated list.
    pub fn skipped_snapshots(&self) -> &[snapshot_cache::SkippedSnapshot] {
        &self.skipped_snapshots
    }

    /// Mutable access to the manifest (in-memory only, never persisted to storage).
    pub fn manifest_mut(&mut self) -> &mut Manifest {
        &mut self.manifest
    }

    /// Read-only access to the chunk index.
    pub fn chunk_index(&self) -> &LiveIndex {
        &self.chunk_index
    }

    /// Mutable access to the chunk index. Automatically marks it dirty.
    pub fn chunk_index_mut(&mut self) -> &mut LiveIndex {
        self.index_dirty = true;
        &mut self.chunk_index
    }

    /// The local tree-chunk cache, if this host has a cache dir.
    pub(crate) fn tree_cache(&self) -> Option<&tree_cache::TreeChunkCache> {
        self.tree_cache.as_ref()
    }

    /// Tests: replace or disable the tree-chunk cache.
    #[cfg(test)]
    pub(crate) fn set_tree_cache(&mut self, cache: Option<tree_cache::TreeChunkCache>) {
        self.tree_cache = cache;
    }

    /// Drop every cached tree chunk the persisted index no longer holds.
    /// Only against a loaded index with no unpersisted edits: an unloaded
    /// index would reject every entry, an edited one is not what storage
    /// holds. Returns `None` when skipped. Never fails.
    pub(crate) fn drop_unreferenced_tree_chunks(&self) -> Option<tree_cache::RetainStats> {
        let cache = self.tree_cache.as_ref()?;
        if !self.chunk_index.is_loaded() || self.index_dirty || self.chunk_index.is_modified() {
            return None;
        }
        Some(cache.retain(|id| self.chunk_index.contains(id)))
    }

    /// Drop the chunk index (unmaps its table, frees its edits).
    /// Does not mark dirty — intended for memory optimization (e.g. restore).
    pub fn clear_chunk_index(&mut self) {
        self.chunk_index = LiveIndex::empty();
    }

    /// Read-only access to the file cache.
    pub fn file_cache(&self) -> &FileCache {
        &self.file_cache
    }

    /// Mutable access to the file cache (for invalidation, section setup).
    pub fn file_cache_mut(&mut self) -> &mut FileCache {
        &mut self.file_cache
    }

    /// Temporarily take the file cache out of the repository.
    /// Does not mark dirty — use `restore_file_cache` to put it back,
    /// or `set_file_cache` to replace it (which marks dirty).
    pub fn take_file_cache(&mut self) -> FileCache {
        std::mem::take(&mut self.file_cache)
    }

    /// Put a previously-taken file cache back without marking dirty.
    pub fn restore_file_cache(&mut self, cache: FileCache) {
        self.file_cache = cache;
    }

    /// Replace the file cache and mark it dirty.
    pub fn set_file_cache(&mut self, cache: FileCache) {
        self.file_cache = cache;
        self.file_cache_dirty = true;
    }

    /// Install a lock fence that will be checked before persisting index/manifest.
    pub fn set_lock_fence(&mut self, fence: Arc<dyn Fn() -> Result<()> + Send + Sync>) {
        self.lock_fence = Some(fence);
    }

    /// Remove the lock fence.
    pub fn clear_lock_fence(&mut self) {
        self.lock_fence = None;
    }

    /// Check the lock fence if one is installed. No-op if no fence is set.
    pub(crate) fn check_lock_fence(&self) -> Result<()> {
        if let Some(ref fence) = self.lock_fence {
            fence()?;
        }
        Ok(())
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        if self.write_session.is_some() {
            warn!("Repository dropped with active write session");
        }
    }
}

// Compile-time assertion: reads take `&self`, so `Arc<Repository>` must be
// shareable across threads (mount serves concurrent reads from one handle).
const _: () = {
    fn _assert_send_sync<T: Send + Sync>() {}
    fn _check() {
        _assert_send_sync::<Repository>();
    }
};

#[cfg(test)]
mod format_tests {
    use super::*;
    use vykar_types::error::VykarError;

    /// The load-bearing constraint behind "no stored `hash` field".
    ///
    /// `RepoConfig` is serialized as a positional msgpack array. rmp-serde
    /// errors on an array *longer* than the struct — `#[serde(default)]` only
    /// rescues shorter ones — so an appended field would make an old binary
    /// fail to decode the config before it reached the version gate, turning a
    /// clear "unsupported repository version" into a parse error that looks
    /// like corruption.
    ///
    /// `0x97` is msgpack fixarray with 7 elements. If this fails because a
    /// field was added, the fix is not to update the constant: it is to bump
    /// the repository version instead, which is what makes appending safe.
    #[test]
    fn repo_config_is_exactly_seven_msgpack_elements() {
        let config = RepoConfig {
            version: RepoFormat::CURRENT.version(),
            id: vec![0u8; 32],
            chunker_params: ChunkerParams::default(),
            encryption: EncryptionMode::None,
            created: chrono::Utc::now(),
            min_pack_size: default_min_pack_size(),
            max_pack_size: default_max_pack_size(),
        };
        let encoded = rmp_serde::to_vec(&config).expect("RepoConfig serializes");
        assert_eq!(
            encoded.first(),
            Some(&0x97),
            "RepoConfig must stay a 7-element msgpack array; \
             got a leading byte of {:#04x}",
            encoded.first().copied().unwrap_or(0)
        );
    }

    /// Pins the exact bytes of a fixed `RepoConfig`, chunker parameters
    /// included. The field-count test above only sees the array header; this
    /// one fails on any change to the type, order or encoding of a field,
    /// nested ones too. Never update the constant to make it pass: a
    /// difference means repositories written before and after disagree.
    #[test]
    fn repo_config_golden_bytes() {
        use chrono::TimeZone;
        const GOLDEN: &str = "9705dc0020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
            93cd0100ce00010000ce00800000\
            b04368616368613230506f6c7931333035\
            bb323032332d31312d31345432323a31333a32302e3132333435365a\
            ce02000000ce0c000000";
        let config = RepoConfig {
            version: 5,
            id: (0u8..32).collect(),
            chunker_params: ChunkerParams {
                min_size: 256,
                avg_size: 65_536,
                max_size: 8 * 1024 * 1024,
            },
            encryption: EncryptionMode::Chacha20Poly1305,
            created: chrono::Utc
                .timestamp_opt(1_700_000_000, 123_456_000)
                .unwrap(),
            min_pack_size: 32 * 1024 * 1024,
            max_pack_size: 192 * 1024 * 1024,
        };
        let bytes = rmp_serde::to_vec(&config).unwrap();
        assert_eq!(hex::encode(&bytes), GOLDEN);
        let decoded: RepoConfig = rmp_serde::from_slice(&hex::decode(GOLDEN).unwrap()).unwrap();
        assert_eq!(rmp_serde::to_vec(&decoded).unwrap(), bytes);
        assert_eq!(
            (
                decoded.chunker_params.min_size,
                decoded.chunker_params.avg_size,
                decoded.chunker_params.max_size
            ),
            (256, 65_536, 8 * 1024 * 1024)
        );
    }

    /// Pins the field names and per-field serde defaults of the stored
    /// chunker parameters: the named (map) encoding uses `min_size`,
    /// `avg_size`, `max_size`, and a shorter positional array fills the
    /// missing fields with their defaults.
    #[test]
    fn chunker_params_names_and_serde_defaults_are_pinned() {
        let params = ChunkerParams {
            min_size: 256,
            avg_size: 65_536,
            max_size: 8 * 1024 * 1024,
        };
        let named = rmp_serde::to_vec_named(&params).unwrap();
        assert_eq!(
            hex::encode(&named),
            "83a86d696e5f73697a65cd0100a86176675f73697a65ce00010000a86d61785f73697a65ce00800000"
        );
        let short: ChunkerParams =
            rmp_serde::from_slice(&hex::decode("92cd0100ce00010000").unwrap()).unwrap();
        assert_eq!((short.min_size, short.avg_size), (256, 65_536));
        assert_eq!(short.max_size, 8 * 1024 * 1024);
    }

    #[test]
    fn from_version_maps_known_versions() {
        assert_eq!(RepoFormat::from_version(2).unwrap(), RepoFormat::V2);
        assert_eq!(RepoFormat::from_version(3).unwrap(), RepoFormat::V3);
        assert_eq!(RepoFormat::from_version(4).unwrap(), RepoFormat::V4);
        assert_eq!(RepoFormat::from_version(5).unwrap(), RepoFormat::V5);
    }

    /// A future version must be refused, not silently handed known semantics.
    #[test]
    fn from_version_rejects_unknown_versions() {
        for version in [0u32, 1, 6, u32::MAX] {
            match RepoFormat::from_version(version) {
                Err(VykarError::UnsupportedVersion(v)) => assert_eq!(v, version),
                other => panic!("version {version} should be unsupported, got {other:?}"),
            }
        }
    }

    #[test]
    fn version_round_trips() {
        for format in [
            RepoFormat::V2,
            RepoFormat::V3,
            RepoFormat::V4,
            RepoFormat::V5,
        ] {
            assert_eq!(RepoFormat::from_version(format.version()).unwrap(), format);
        }
    }

    #[test]
    fn format_selects_the_hash() {
        assert_eq!(RepoFormat::V2.chunk_hash(), HashAlgorithm::Blake2b);
        assert_eq!(RepoFormat::V3.chunk_hash(), HashAlgorithm::Blake3);
        assert_eq!(RepoFormat::V4.chunk_hash(), HashAlgorithm::Blake2b);
        assert_eq!(RepoFormat::V5.chunk_hash(), HashAlgorithm::Blake3);
        assert_eq!(RepoFormat::CURRENT, RepoFormat::V5);
    }

    #[test]
    fn upgrade_only_formats() {
        assert!(RepoFormat::V2.is_upgrade_only());
        assert!(RepoFormat::V3.is_upgrade_only());
        assert!(!RepoFormat::V4.is_upgrade_only());
        assert!(!RepoFormat::V5.is_upgrade_only());
    }
}
