pub(crate) mod ages;
pub(crate) mod blob_read;
pub(crate) mod bounded;
pub(crate) mod coalesce;
pub mod control;
pub(crate) mod existing_pack;
pub mod file_cache;
pub mod format;
pub mod host_env;
pub mod identity;
pub(crate) mod journal_recovery;
pub mod manifest;
pub(crate) mod memory_budget;
pub mod pack;
pub mod pack_map;
pub mod pack_trailer;
pub(crate) mod session_journal;
pub mod sessions;
pub mod snapshot_cache;
pub(crate) mod tree_cache;
mod upload_queue;
pub(crate) mod write_session;

mod chunks;
mod commit;
pub(crate) use commit::needs_recheck;
mod commit_machine;
pub(crate) use commit_machine::{BackupCommit, Committed, HEAD_CREATE_ATTEMPTS, retry_delay};
pub(crate) mod index_rebuild;
pub(crate) mod index_store;
pub(crate) mod index_write;
mod key;
pub(crate) mod machine;
mod open;
pub(crate) mod pack_listing;
mod pack_lookup;
mod pack_recovery;
mod pack_verify;
mod publish_machine;
pub use host_env::{Clock, HostEnv, HostIdentity, StopSignal, SystemClock};
pub use index_rebuild::{RebuildCounts, UncoveredPack};
pub use index_write::{IndexLoss, IndexRecovery, RepairIndexLoad};
pub use key::{LoadedKey, select_repository_key};
#[cfg(any(test, feature = "sim", feature = "testing"))]
pub use open::{count_repo_opens, with_cheap_kdf};
mod read;
pub(crate) use read::TreeChunkSource;
pub(crate) mod relocate;
mod session;
pub use memory_budget::PackPolicy;
pub use session::WriteSessionOptions;
mod upgrade;
pub(crate) use upgrade::LegacyUpgradeLock;
pub use upgrade::{UpgradeOutcome, WriterModeReport};
pub(crate) mod write_check;
mod writer;
pub(crate) mod writer_mode;
pub(crate) use chunks::{
    ChunkInput, ExistingAt, ExistingChunk, PreparedChunk, StoredChunk, WorkerChunk,
};
pub use write_session::WriteSession;
pub use writer::RepoWriter;
pub(crate) use writer::{Fence, FenceFn};

use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub use crate::chunker::ChunkerParams;
use crate::index::LiveIndex;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::error::Result;
use vykar_types::hash::HashAlgorithm;

use self::control::CancelToken;
use self::file_cache::FileCache;
use self::manifest::Manifest;
use vykar_types::snapshot_id::SnapshotId;

/// The repository layout version, resolved once from the stored `config`.
///
/// `version` answers "what must a reader understand to read this
/// repository?". From v6 on, `config` is a msgpack **map** that names the
/// chunk and pack hash in `chunk_hash` and the features a reader must know in
/// `features`; old releases decode such a map into their seven-field struct,
/// ignore the extra keys and refuse it at their version gate
/// ("unsupported repository version: 6"), never with a parse error
/// (`tests/legacy_config_refusal.rs`).
///
/// v2 and v3 kept the positional seven-element config of 0.19-0.24 and imply
/// their hash (BLAKE2b and BLAKE3); they are upgrade-only (see
/// [`RepoFormat::is_upgrade_only`]). v4 and v5 (a segmented index under a
/// mutable `index` head) and a positional 6 or 7 (05 step 8) were
/// development formats and are refused.
///
/// Only chunk IDs and pack IDs follow the hash. Every other digest in the
/// workspace is BLAKE3 for all versions — see the "Repository Versions"
/// subsection of `docs/src/content/docs/architecture.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoFormat {
    /// BLAKE2b-256 IDs, monolithic index, positional config; upgrade-only.
    V2,
    /// BLAKE3 IDs, monolithic index, positional config; upgrade-only.
    V3,
    /// Map config, numbered heads, keyed index, mark-and-sweep. The chunk
    /// and pack hash is the config's `chunk_hash`: Blake2b only for
    /// repositories upgraded from v2.
    V6(HashAlgorithm),
}

impl RepoFormat {
    /// The format `vykar init` creates. Existing repositories keep theirs.
    pub const CURRENT: Self = Self::V6(HashAlgorithm::Blake3);

    /// The integer stored in `config`'s `version`.
    pub fn version(self) -> u32 {
        match self {
            Self::V2 => 2,
            Self::V3 => 3,
            Self::V6(_) => 6,
        }
    }

    /// The algorithm this format uses for chunk IDs and pack IDs.
    pub fn chunk_hash(self) -> HashAlgorithm {
        match self {
            Self::V2 => HashAlgorithm::Blake2b,
            Self::V3 => HashAlgorithm::Blake3,
            Self::V6(h) => h,
        }
    }

    /// v2 and v3 (monolithic index): opened only by `vykar upgrade`, through
    /// [`OpenOptions::legacy_format`]. Every other open refuses them.
    pub fn is_upgrade_only(self) -> bool {
        matches!(self, Self::V2 | Self::V3)
    }

    /// The format `vykar upgrade` converts this one to: v2 → v6 with
    /// BLAKE2b, v3 → v6 with BLAKE3. `None` for a current format.
    pub fn upgrade_target(self) -> Option<Self> {
        match self {
            Self::V2 => Some(Self::V6(HashAlgorithm::Blake2b)),
            Self::V3 => Some(Self::V6(HashAlgorithm::Blake3)),
            Self::V6(_) => None,
        }
    }
}

/// The positional `config` of formats v2 and v3, exactly as 0.19-0.24
/// wrote it. Read only to upgrade (and by tests that build fixtures).
/// **Exactly seven fields; never extended**: rmp-serde errors on an array
/// longer than the struct, so an appended field would turn an old binary's
/// version refusal into a parse error.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyRepoConfig {
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

/// Persisted (unencrypted) at the `config` key from format v6 on: a msgpack
/// map. Carries the seven old fields under their old names and types for
/// good, so old releases reach their version gate
/// (`tests/legacy_config_refusal.rs`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// [`HashAlgorithm::as_str`] of the chunk and pack hash. A string, so an
    /// unknown name is a "newer vykar" refusal, not a decode error.
    pub chunk_hash: String,
    /// Features a reader must understand: [`MULTI_WRITER_FEATURE`] on a
    /// multi-writer repository, else empty.
    pub features: Vec<String>,
}

/// The config feature of a multi-writer repository (10-single-protocol
/// step 5): clients commit side by side, with put-if-absent index heads.
/// Absent: single-writer.
pub const MULTI_WRITER_FEATURE: &str = "multi_writer";

/// Config features this binary understands. A config naming another is
/// refused as written by a newer vykar.
pub const KNOWN_CONFIG_FEATURES: &[&str] = &[MULTI_WRITER_FEATURE];

/// A repository's commit mode as its config records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriterMode {
    /// No `multi_writer`: one writer at a time, kept apart by a
    /// best-effort check of the session markers (10-single-protocol step 7).
    Single,
    /// `multi_writer`: commits side by side, with put-if-absent heads.
    Multi,
}

impl WriterMode {
    /// "single-writer" / "multi-writer", as `vykar info` prints it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single-writer",
            Self::Multi => "multi-writer",
        }
    }
}

impl std::fmt::Display for WriterMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why `encryption.mode: none` is refused while the config is parsed. The
/// whole file fails to load, so the text says the line must change before
/// any repository in it can be used.
pub const PLAINTEXT_MODE_REFUSAL: &str = "encryption.mode none is no longer supported: every repository is encrypted. Remove \
     the line or change it to auto (the default), aes256gcm or chacha20poly1305 before any \
     repository in this config file can be used; the passphrase comes from \
     encryption.passphrase, encryption.passcommand or VYKAR_PASSPHRASE. An existing \
     unencrypted repository is not opened by this vykar: create a new repository and back \
     up into it; vykar 0.24 can still restore from the old one";

/// Why an unencrypted repository is refused ([`DecodedConfig::validate`]).
pub const UNENCRYPTED_REPOSITORY_REFUSAL: &str = "this repository is unencrypted (encryption mode none); this vykar no longer \
     supports unencrypted repositories and does not open it. Create a new repository \
     (every new repository is encrypted) and back up into it; vykar 0.24 can still \
     restore from this one";

pub(crate) fn default_min_pack_size() -> u32 {
    32 * 1024 * 1024 // 32 MiB
}

pub(crate) fn default_max_pack_size() -> u32 {
    192 * 1024 * 1024 // 192 MiB
}

/// Upper bound on `RepoConfig::max_pack_size`.
pub(crate) const MAX_PACK_SIZE_LIMIT: u32 = 512 * 1024 * 1024;

/// The version of every config written as a map.
const MAP_CONFIG_VERSION: u32 = 6;

impl RepoConfig {
    /// `Multi` when `features` names [`MULTI_WRITER_FEATURE`].
    pub fn writer_mode(&self) -> WriterMode {
        if self.features.iter().any(|f| f == MULTI_WRITER_FEATURE) {
            WriterMode::Multi
        } else {
            WriterMode::Single
        }
    }

    /// Add or remove [`MULTI_WRITER_FEATURE`], keeping every other feature
    /// and their order, never duplicating it.
    pub fn set_writer_mode(&mut self, mode: WriterMode) {
        match mode {
            WriterMode::Multi => {
                if self.writer_mode() != WriterMode::Multi {
                    self.features.push(MULTI_WRITER_FEATURE.to_string());
                }
            }
            WriterMode::Single => self.features.retain(|f| f != MULTI_WRITER_FEATURE),
        }
    }

    /// The v6 config of a new repository.
    pub fn new_v6(
        id: Vec<u8>,
        chunker_params: ChunkerParams,
        encryption: EncryptionMode,
        created: DateTime<Utc>,
        min_pack_size: u32,
        max_pack_size: u32,
        chunk_hash: HashAlgorithm,
    ) -> Self {
        Self {
            version: MAP_CONFIG_VERSION,
            id,
            chunker_params,
            encryption,
            created,
            min_pack_size,
            max_pack_size,
            chunk_hash: chunk_hash.as_str().to_string(),
            features: Vec::new(),
        }
    }

    /// Serialize as a msgpack map (`to_vec_named`): nested structs as maps,
    /// `id` as an array of integers. Only version 6 is ever written as a map
    /// (a map at version 2 or 3 would be read by old releases at their own
    /// version); any other version is `InvalidFormat`.
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.version != MAP_CONFIG_VERSION {
            return Err(vykar_types::error::VykarError::InvalidFormat(format!(
                "config: version {} is not written as a map",
                self.version
            )));
        }
        Ok(rmp_serde::to_vec_named(self)?)
    }

    /// Decode `config` bytes. A msgpack map decodes as [`RepoConfig`], an
    /// array as [`LegacyRepoConfig`]; anything else is the error of decoding
    /// it as a map. Nothing is validated: see [`DecodedConfig::validate`].
    pub fn decode(bytes: &[u8]) -> Result<DecodedConfig> {
        match bytes.first() {
            Some(0x90..=0x9f | 0xdc | 0xdd) => {
                Ok(DecodedConfig::Positional(rmp_serde::from_slice(bytes)?))
            }
            _ => Ok(DecodedConfig::Map(rmp_serde::from_slice(bytes)?)),
        }
    }
}

/// The refusal of a config naming a feature outside `known`, by name: what
/// [`DecodedConfig::validate`] does with [`KNOWN_CONFIG_FEATURES`] (and what
/// an older binary with a shorter list does with a newer feature).
fn refuse_unknown_features(features: &[String], known: &[&str]) -> Result<()> {
    let unknown: Vec<&str> = features
        .iter()
        .map(String::as_str)
        .filter(|f| !known.contains(f))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(vykar_types::error::VykarError::Other(format!(
        "the repository config names feature(s) {}, which this vykar does not know; it was \
         written by a newer vykar. Upgrade vykar",
        unknown.join(", ")
    )))
}

/// What `config` held.
#[derive(Debug, Clone)]
pub enum DecodedConfig {
    /// v6 (or a later version: refused by `validate`).
    Map(RepoConfig),
    /// v2/v3 (or a refused development format).
    Positional(LegacyRepoConfig),
}

impl DecodedConfig {
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
    /// A map: a version above 6 is `UnsupportedVersion`, below 6
    /// `InvalidFormat`; an unknown `chunk_hash` or feature is refused as
    /// written by a newer vykar. Positional: 2 and 3 are accepted, 6 and 7
    /// (development builds) are refused with a "create a new repository"
    /// message, every other version is `UnsupportedVersion`. Both:
    /// encryption mode none ([`UNENCRYPTED_REPOSITORY_REFUSAL`]), a
    /// `max_pack_size` above the hard limit.
    pub fn validate(&self) -> Result<RepoFormat> {
        use vykar_types::error::VykarError;
        let (format, max_pack_size) = match self {
            Self::Map(c) => {
                if c.version > MAP_CONFIG_VERSION {
                    return Err(VykarError::UnsupportedVersion(c.version));
                }
                if c.version < MAP_CONFIG_VERSION {
                    return Err(VykarError::InvalidFormat(format!(
                        "config: a map-encoded config with version {}",
                        c.version
                    )));
                }
                let hash = match c.chunk_hash.as_str() {
                    "blake2b" => HashAlgorithm::Blake2b,
                    "blake3" => HashAlgorithm::Blake3,
                    other => {
                        return Err(VykarError::Other(format!(
                            "the repository config names chunk hash {other}, which this vykar \
                             does not know; it was written by a newer vykar. Upgrade vykar"
                        )));
                    }
                };
                refuse_unknown_features(&c.features, KNOWN_CONFIG_FEATURES)?;
                (RepoFormat::V6(hash), c.max_pack_size)
            }
            Self::Positional(c) => {
                let format = match c.version {
                    2 => RepoFormat::V2,
                    3 => RepoFormat::V3,
                    v @ (6 | 7) => {
                        return Err(VykarError::Other(format!(
                            "this repository was created by a development build of vykar \
                             (positional config, version {v}); this vykar does not open it. \
                             Create a new repository"
                        )));
                    }
                    other => return Err(VykarError::UnsupportedVersion(other)),
                };
                (format, c.max_pack_size)
            }
        };
        let encryption = match self {
            Self::Map(c) => &c.encryption,
            Self::Positional(c) => &c.encryption,
        };
        if *encryption == EncryptionMode::None {
            return Err(VykarError::Other(UNENCRYPTED_REPOSITORY_REFUSAL.into()));
        }
        if max_pack_size > MAX_PACK_SIZE_LIMIT {
            return Err(VykarError::Config(format!(
                "max_pack_size ({max_pack_size}) exceeds hard limit of 512 MiB"
            )));
        }
        Ok(format)
    }

    /// The in-memory form: a map as is; a positional config as a
    /// [`RepoConfig`] with the same seven values, its stored version (2 or 3),
    /// the hash that version implies and no features. Only after
    /// [`Self::validate`].
    pub fn into_repo_config(self) -> RepoConfig {
        match self {
            Self::Map(c) => c,
            Self::Positional(c) => {
                let hash = if c.version == 2 {
                    HashAlgorithm::Blake2b
                } else {
                    HashAlgorithm::Blake3
                };
                RepoConfig {
                    version: c.version,
                    id: c.id,
                    chunker_params: c.chunker_params,
                    encryption: c.encryption,
                    created: c.created,
                    min_pack_size: c.min_pack_size,
                    max_pack_size: c.max_pack_size,
                    chunk_hash: hash.as_str().to_string(),
                    features: Vec::new(),
                }
            }
        }
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

/// What `Repo::init` takes from the client's repository settings.
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
}

/// Options controlling which expensive subsystems are loaded when opening a
/// repository. Both default to `false` (skip). Only `backup` needs both.
#[derive(Clone, Debug, Default)]
pub struct OpenOptions {
    pub load_index: bool,
    pub load_file_cache: bool,
    /// Repository URL, when the caller knows it. Only the identity pin needs
    /// it, and `Repo::open` itself has no other way to learn it.
    pub repo_url: Option<String>,
    /// Accept a key whose fingerprint contradicts the local identity pin.
    /// Mirrors `--trust-repo`, the single deliberate escape hatch.
    pub trust_repo: bool,
    /// Suppress the best-effort backfill of a missing key copy. Set by
    /// `check --repair --dry-run`, which prints "no changes applied" and must
    /// not have written one underneath that statement.
    pub skip_key_backfill: bool,
    /// Open a segmented repository whose index head cannot be read (an
    /// older binary overwrote it). Set by `check --repair`, which rebuilds
    /// the index; every other command refuses such a repository.
    pub tolerate_broken_index: bool,
    /// Open a v2/v3 repository instead of refusing it. Only `vykar upgrade`
    /// sets this. Such a handle supports `upgrade_index_layout` and
    /// `legacy_index` only; it has no head, and `load_index` is refused.
    pub legacy_format: bool,
    /// Clock and identity for the session and maintenance code.
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
pub struct Repo {
    storage: Arc<dyn StorageBackend>,
    crypto: Arc<dyn CryptoEngine>,
    manifest: Manifest,
    /// Snapshots present under `snapshots/` that could not be read into the
    /// manifest. Non-empty means `manifest` is an incomplete view of the
    /// repository — see [`Repo::skipped_snapshots`].
    skipped_snapshots: Vec<snapshot_cache::SkippedSnapshot>,
    /// The chunk index: the mapped table of the loaded state plus this
    /// process's edits. Not loaded (empty) until a load or write.
    chunk_index: LiveIndex,
    config: RepoConfig,
    /// Resolved once from `config.version` at init/open. Every downstream
    /// caller asks the repository rather than re-inspecting the integer.
    format: RepoFormat,
    file_cache: FileCache,
    /// The generation of the loaded head; 0 when there is no head.
    index_generation: u64,
    /// Whether the chunk index has been modified since last persist.
    index_dirty: bool,
    /// Whether the file cache has been modified since last persist.
    file_cache_dirty: bool,
    /// The on-disk file cache as this handle loaded or last saved it; a
    /// different file at save time means another process saved since.
    file_cache_stamp: Option<file_cache::FileCacheStamp>,
    /// Section keys this handle wrote into `file_cache` since its last save.
    file_cache_written: std::collections::BTreeSet<String>,
    /// Anchors of the sections this handle invalidated since its last save.
    file_cache_dropped: std::collections::HashSet<SnapshotId>,
    /// Override for the cache directory root (from config `cache_dir`).
    cache_dir_override: Option<PathBuf>,
    /// Local tree-chunk cache; None without a cache dir.
    tree_cache: Option<tree_cache::TreeChunkCache>,
    /// Clock and identity of this client; see [`host_env`].
    host_env: HostEnv,
    /// The key-copy bytes that actually unwrapped at open. `check` compares
    /// the stored objects against these instead of unwrapping again, which
    /// would cost another Argon2id derivation and need the passphrase threaded
    /// into the scan.
    key_blob: Vec<u8>,
    /// The head the in-memory state was loaded from (or written as). `None`
    /// on a handle opened for the upgrade, and while `check --repair`
    /// rebuilds the index.
    index_head: Option<crate::index::head::IndexHead>,
    /// When chains are folded into a new base.
    fold_policy: index_write::FoldPolicy,
    /// What fold the next index write was asked for.
    fold_requested: index_write::FoldRequest,
    /// The last head generation this handle created (04 step 8): the index
    /// garbage collection runs only right after its caller created the
    /// latest head (`index_store::collect_index_garbage`'s precondition).
    created_head: Option<u64>,
    /// The repository URL the open or init was given
    /// (`OpenOptions::repo_url`, `InitOptions::url`); only for messages.
    repo_url: Option<String>,
    /// Set by init when it recorded single-writer: the init-time check's
    /// verdict (10-single-protocol step 5).
    single_writer_reason: Option<String>,
    /// The mode this handle commits in, decided once (`Self::commit_mode`);
    /// `None` until a writer asked, or after the mode changed.
    commit_mode: Option<WriterMode>,
    /// What the next index write records in the head's `sweep`; consumed by it.
    pending_sweep: Option<index_write::PendingSweep>,
    /// Graces of `collect_index_garbage`: `IndexGcGrace::DEFAULT`; tests
    /// lower them.
    index_gc_grace: index_store::IndexGcGrace,
    /// "Now" on the store's clock as the run's session read it (a
    /// maintenance or exclusive session's marker mtime in its listing, a
    /// backup's at its pre-head listing), for the ages the run judges.
    /// `None` outside a run or without mtimes.
    store_time: Option<vykar_storage::StoreNow>,
    /// How long one EXISTS of an exclusive session's marker vouches for it
    /// (`sessions::EXCLUSIVE_CHECK_REUSE`); tests lower it.
    exclusive_check_reuse: std::time::Duration,
    /// The sessions a maintenance run found live, for compact's horizon (an
    /// exclusive session's registration finds none, or refuses); `None`
    /// outside a run.
    live_sessions: Option<Vec<sessions::LiveSession>>,
    /// Orphan packs younger than this are kept: `ages::T_PACK`; tests lower it
    /// (zero turns the guard off, so a test double without mtimes deletes).
    pack_grace: std::time::Duration,
    /// Backup segments younger than this keep their entries from a sweep and
    /// stay out of a fold: `ages::T_PACK`; tests lower it (zero: every
    /// segment is old, the write's own included).
    segment_grace: std::time::Duration,
    /// The ages of `index.d/` objects, listed at most once per run;
    /// cleared with the session state.
    segment_ages: Option<ages::SegmentAges>,
    /// The latest index a relocation loaded, see [`relocate`].
    relocation: relocate::RelocationCache,
}

/// What [`Repo::save_file_cache_cancellable`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileCacheSave {
    /// Another process had saved since this handle loaded or saved: the
    /// number of its sections taken over. None: a plain save.
    pub(crate) merged: Option<usize>,
    /// Whether the save held `filecache.lock`.
    pub(crate) locked: bool,
}

impl Repo {
    /// Clock and identity of this client.
    pub fn host_env(&self) -> &HostEnv {
        &self.host_env
    }

    /// Replace the environment, e.g. on a handle from `Repo::init`.
    pub fn set_host_env(&mut self, env: HostEnv) {
        self.host_env = env;
    }

    /// The key blob that unwrapped at open.
    pub fn key_blob(&self) -> &[u8] {
        &self.key_blob
    }

    /// Mark the file cache as needing persistence on the next `save_state()`.
    pub fn mark_file_cache_dirty(&mut self) {
        self.file_cache_dirty = true;
    }

    /// Save the file cache to local disk if it has been modified.
    /// Returns `Ok(())` if no save was needed or the save succeeded.
    /// See `save_file_cache_cancellable`: the merge with another process's
    /// save and the local lock.
    pub fn save_file_cache_if_dirty(&mut self) -> Result<()> {
        self.save_file_cache_cancellable(&CancelToken::new())
            .map(|_| ())
    }

    /// Record that this handle invalidated the sections anchored at `ids`:
    /// a merged save does not take them back from another process's copy.
    pub(crate) fn note_file_cache_dropped(&mut self, ids: impl IntoIterator<Item = SnapshotId>) {
        self.file_cache_dropped.extend(ids);
    }

    /// Record that this handle wrote the section `key` (it replaces the
    /// other process's copy in a merged save).
    pub(crate) fn note_file_cache_written(&mut self, key: &str) {
        self.file_cache_written.insert(key.to_string());
    }

    /// Save the file cache if dirty, merged with another process's save.
    ///
    /// Two processes on one host (a daemon and a CLI run) share the cache
    /// directory. The save takes the local lock `filecache.lock` beside the
    /// file ([`file_cache::FileCacheLock`]; polled for at most
    /// [`file_cache::FILE_CACHE_LOCK_WAIT`] or until `cancel`, since it runs
    /// after the commit point, before the session ends; then it saves
    /// without the lock, with one warning). When the file is the one this
    /// handle loaded or last saved (its stamp), the save is a single atomic
    /// write as before. When another process replaced it, the copies of the
    /// sections this handle did not write are dropped as stale, the file is
    /// re-read without the sections this handle wrote, and its sections are
    /// taken over except those anchored at a snapshot this handle
    /// invalidated. For a root both processes wrote, the last save wins;
    /// every section stays consistent with its own anchor snapshot. A file
    /// that does not read, decrypt or carry this binary's format byte is not
    /// merged: the save writes everything this handle holds, as when the
    /// file is its own. A file that fails later, mid-decode, contributes
    /// nothing, and the copies of the sections this handle did not write are
    /// already dropped then. A save without the lock keeps no stamp, so the
    /// next save merges. On error `dirty` and what this handle wrote and
    /// dropped are kept.
    ///
    /// Returns None when nothing was saved (not dirty, or no cache dir).
    pub(crate) fn save_file_cache_cancellable(
        &mut self,
        cancel: &CancelToken,
    ) -> Result<Option<FileCacheSave>> {
        if !self.file_cache_dirty {
            return Ok(None);
        }
        let Some(path) =
            file_cache::FileCache::cache_path(&self.config.id, self.cache_dir_override.as_deref())
        else {
            self.file_cache_dirty = false;
            return Ok(None);
        };
        let dir = path.parent().unwrap_or(std::path::Path::new("."));
        std::fs::create_dir_all(dir)?;
        let lock = match file_cache::FileCacheLock::acquire(
            dir,
            self.host_env.clock.as_ref(),
            cancel,
            file_cache::FILE_CACHE_LOCK_WAIT,
        ) {
            Ok(lock) => Some(lock),
            Err(file_cache::FileCacheLockError::TimedOut) => {
                tracing::warn!(
                    dir = %dir.display(),
                    "another process held the file cache lock for {} s; saving the file \
                     cache without it (a save running beside this one may drop this \
                     run's sections or the other's, and the next backup of those \
                     sources walks cold)",
                    file_cache::FILE_CACHE_LOCK_WAIT.as_secs()
                );
                None
            }
            Err(file_cache::FileCacheLockError::Cancelled) => {
                tracing::info!(
                    "cancelled while waiting for the file cache lock; saving without it"
                );
                None
            }
            Err(file_cache::FileCacheLockError::Unavailable(e)) => {
                tracing::debug!(dir = %dir.display(), "file cache lock unavailable ({e}); saving without it");
                None
            }
        };
        let on_disk = file_cache::FileCacheStamp::of(&path);
        let mut merged = None;
        if on_disk.is_some()
            && on_disk != self.file_cache_stamp
            && let Some(plaintext) =
                file_cache::FileCache::read_mergeable(&path, self.crypto.as_ref())
        {
            // Drop the stale copies before the decode builds the other
            // process's sections: the two are never held together.
            self.file_cache.retain_sections_in(&self.file_cache_written);
            let other =
                file_cache::FileCache::decode_except(&path, &plaintext, &self.file_cache_written);
            drop(plaintext);
            let adopted = self
                .file_cache
                .adopt_sections(other, &self.file_cache_dropped);
            tracing::info!(
                adopted,
                "file cache merged with the sections another process saved"
            );
            merged = Some(adopted);
        }
        self.file_cache.save(
            &self.config.id,
            self.crypto.as_ref(),
            self.cache_dir_override.as_deref(),
        )?;
        // Under the lock no other save renamed over this one yet, so the file
        // is this save's. Without it the file may already be another
        // process's: no stamp, so the next save re-reads and merges.
        let locked = lock.is_some();
        self.file_cache_stamp = if locked {
            file_cache::FileCacheStamp::of(&path)
        } else {
            None
        };
        self.file_cache_written.clear();
        self.file_cache_dropped.clear();
        self.file_cache_dirty = false;
        drop(lock);
        Ok(Some(FileCacheSave { merged, locked }))
    }

    /// The repository layout format, resolved at open time.
    pub fn format(&self) -> RepoFormat {
        self.format
    }

    /// The storage backend of this repository.
    pub fn storage(&self) -> &Arc<dyn StorageBackend> {
        &self.storage
    }

    /// The crypto engine (key material) of this repository.
    pub fn crypto(&self) -> &Arc<dyn CryptoEngine> {
        &self.crypto
    }

    /// The repository's config as opened (or as the upgrade switched it).
    pub fn config(&self) -> &RepoConfig {
        &self.config
    }

    /// Tests: replace the storage (a recorder, a fault injector).
    #[cfg(test)]
    pub(crate) fn set_storage_for_test(&mut self, storage: Arc<dyn StorageBackend>) {
        self.storage = storage;
    }

    /// Replace the backend this handle uses; returns the previous one. An
    /// exclusive session wraps the backend in a write check for its run
    /// (`commands::util::with_exclusive_session`). Callers restore the
    /// previous backend after.
    pub(crate) fn swap_storage(
        &mut self,
        storage: Arc<dyn StorageBackend>,
    ) -> Arc<dyn StorageBackend> {
        std::mem::replace(&mut self.storage, storage)
    }

    /// Tests: replace the crypto engine.
    #[cfg(test)]
    pub(crate) fn set_crypto_for_test(&mut self, crypto: Arc<dyn CryptoEngine>) {
        self.crypto = crypto;
    }

    /// The content-digest algorithm this repository's chunk IDs and pack IDs
    /// use. Implied by the format version and fixed for the repository's life.
    pub fn content_hash(&self) -> HashAlgorithm {
        self.format.chunk_hash()
    }

    // ----- Accessors for private fields -----

    /// The generation of the loaded head (0: none).
    pub fn index_generation(&self) -> u64 {
        self.index_generation
    }

    /// Tests: whether the chunk index has edits not yet persisted.
    #[cfg(test)]
    pub(crate) fn index_dirty(&self) -> bool {
        self.index_dirty
    }
    /// Read-only access to the manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Snapshots that exist under `snapshots/` but could not be read into the
    /// manifest — most commonly because they were written by a newer vykar.
    ///
    /// A non-empty result means [`Repo::manifest`] is an **incomplete**
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

    /// The local tree-chunk cache, if this host has a cache dir.
    pub(crate) fn tree_cache(&self) -> Option<&tree_cache::TreeChunkCache> {
        self.tree_cache.as_ref()
    }

    /// Tests: replace or disable the tree-chunk cache.
    #[cfg(test)]
    pub(crate) fn set_tree_cache(&mut self, cache: Option<tree_cache::TreeChunkCache>) {
        self.tree_cache = cache;
    }

    /// Drop this process's unpersisted edits of the chunk index: it is its
    /// loaded table again, and not dirty.
    pub(crate) fn discard_index_edits(&mut self) {
        self.chunk_index.discard_edits();
        self.index_dirty = false;
    }

    /// Drop the chunk index (unmaps its table, frees its edits) and the
    /// index a relocation loaded (a later relocation loads one again).
    /// Does not mark dirty — intended for memory optimization (e.g. restore).
    pub fn clear_chunk_index(&mut self) {
        self.chunk_index = LiveIndex::empty();
        self.relocation.clear();
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

    /// Forget the run's session state: its reading of the store's clock,
    /// the live sessions it found and the segment ages it listed.
    pub fn clear_session_state(&mut self) {
        self.store_time = None;
        self.live_sessions = None;
        self.segment_ages = None;
    }

    /// Record the sessions the run's session listing found live.
    pub(crate) fn set_live_sessions(&mut self, s: Option<Vec<sessions::LiveSession>>) {
        self.live_sessions = s;
    }

    /// The sessions the run's session listing found live; empty outside a run.
    pub(crate) fn live_sessions(&self) -> &[sessions::LiveSession] {
        self.live_sessions.as_deref().unwrap_or_default()
    }

    /// `Err(ActiveSessions)` listing them when any session is live: for the
    /// maintenance actions that must not run beside a backup (repair,
    /// upgrade).
    pub(crate) fn refuse_live_sessions(&self) -> Result<()> {
        let live = self.live_sessions();
        if live.is_empty() {
            return Ok(());
        }
        let entries = live
            .iter()
            .map(|s| (s.session_id.clone(), s.entry.clone()))
            .collect();
        Err(vykar_types::error::VykarError::ActiveSessions(
            sessions::build_active_session_list(entries, &self.host_env.now_utc()),
        ))
    }

    /// Record the store's clock reading of the run this handle works in.
    pub(crate) fn set_store_time(&mut self, t: Option<vykar_storage::StoreNow>) {
        self.store_time = t;
    }

    /// How long one EXISTS of an exclusive session's marker vouches for it.
    pub(crate) fn exclusive_check_reuse(&self) -> std::time::Duration {
        self.exclusive_check_reuse
    }

    /// Tests: set how long one EXISTS of an exclusive session's marker
    /// vouches for it (zero: every write sends one).
    #[cfg(test)]
    pub(crate) fn set_exclusive_check_reuse(&mut self, d: std::time::Duration) {
        self.exclusive_check_reuse = d;
    }

    /// "Now" on the store's clock as the run's session read it, if any.
    pub(crate) fn store_time(&self) -> Option<vykar_storage::StoreNow> {
        self.store_time
    }

    /// Set the age below which orphan packs are kept (tests).
    #[cfg(test)]
    pub(crate) fn set_pack_grace(&mut self, grace: std::time::Duration) {
        self.pack_grace = grace;
    }

    /// The age below which orphan packs are kept.
    pub(crate) fn pack_grace(&self) -> std::time::Duration {
        self.pack_grace
    }

    /// Set the age below which backup segments are young (tests).
    #[cfg(test)]
    pub(crate) fn set_segment_grace(&mut self, grace: std::time::Duration) {
        self.segment_grace = grace;
    }

    /// The age below which backup segments are young (04 step 7).
    pub(crate) fn segment_grace(&self) -> std::time::Duration {
        self.segment_grace
    }

    /// List the ages of `index.d/` objects unless this run did
    /// already; "now" is [`Self::store_time`]. Without a "now" nothing
    /// is listed (every backup segment is young then anyway).
    ///
    /// # Errors
    ///
    /// The listing failed; nothing is cached then.
    pub(crate) fn load_segment_ages(&mut self) -> Result<()> {
        if self.segment_ages.is_some() || self.store_time.is_none() {
            return Ok(());
        }
        let ages = ages::SegmentAges::list(self.storage.as_ref(), self.store_time)?;
        self.segment_ages = Some(ages);
        Ok(())
    }

    /// The ages listed in this run, if any.
    pub(crate) fn segment_ages(&self) -> Option<&ages::SegmentAges> {
        self.segment_ages.as_ref()
    }

    /// Whether judging `segments` needs a listing: a backup segment among
    /// them, a non-zero grace, a "now", and no listing yet in this run.
    pub(crate) fn needs_segment_ages(&self, segments: &[crate::index::head::SegmentRef]) -> bool {
        !self.segment_grace.is_zero()
            && self.segment_ages.is_none()
            && self.store_time.is_some()
            && segments
                .iter()
                .any(|s| s.kind == crate::index::head::SegmentKind::Backup)
    }

    /// [`ages::foldable_prefix`] with this handle's ages and grace.
    pub(crate) fn foldable_prefix(&self, segments: &[crate::index::head::SegmentRef]) -> usize {
        ages::foldable_prefix(segments, self.segment_ages.as_ref(), self.segment_grace)
    }

    /// The repository's mode as this handle commits in it, decided once per
    /// handle from `config` (or from the re-read of the suspend rule,
    /// [`Self::reread_writer_mode`]): `Multi` once this handle's storage
    /// passed its put-if-absent check ([`Self::require_writer_mode`]; else
    /// `MultiWriterUnsupported`, never a fallback; an undecided check is an
    /// error), `Single` with no request. An error is not cached.
    pub(crate) fn commit_mode(&mut self) -> Result<WriterMode> {
        if let Some(mode) = self.commit_mode {
            return Ok(mode);
        }
        let mode = self.config.writer_mode();
        match mode {
            WriterMode::Multi => {
                self.require_writer_mode()?;
                tracing::debug!("this client creates heads with put-if-absent (multi-writer)");
            }
            WriterMode::Single => {
                tracing::debug!("this client commits as the single writer (single-writer)");
            }
        }
        self.commit_mode = Some(mode);
        Ok(mode)
    }
}

// Compile-time assertion: reads take `&self`, so `Arc<Repo>` must be
// shareable across threads (mount serves concurrent reads from one handle).
const _: () = {
    fn _assert_send_sync<T: Send + Sync>() {}
    fn _check() {
        _assert_send_sync::<Repo>();
    }
};

// Compile-time assertion: `CommitInProgress` owns a write session, and the
// simulator runs commits on spawned threads.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<WriteSession>();
    }
};

#[cfg(test)]
mod format_tests {
    use super::*;
    use vykar_types::error::VykarError;

    fn legacy(version: u32) -> LegacyRepoConfig {
        LegacyRepoConfig {
            version,
            id: vec![0u8; 32],
            chunker_params: ChunkerParams::default(),
            encryption: EncryptionMode::Aes256Gcm,
            created: chrono::Utc::now(),
            min_pack_size: default_min_pack_size(),
            max_pack_size: default_max_pack_size(),
        }
    }

    fn v6(hash: HashAlgorithm) -> RepoConfig {
        RepoConfig::new_v6(
            (0u8..32).collect(),
            ChunkerParams::default(),
            EncryptionMode::Aes256Gcm,
            chrono::Utc::now(),
            default_min_pack_size(),
            default_max_pack_size(),
            hash,
        )
    }

    /// The load-bearing constraint of the v2/v3 config.
    ///
    /// `LegacyRepoConfig` is serialized as a positional msgpack array.
    /// rmp-serde errors on an array *longer* than the struct, so an appended
    /// field would make an old binary fail to decode the config before it
    /// reached the version gate. `0x97` is msgpack fixarray with 7 elements.
    #[test]
    fn legacy_repo_config_is_exactly_seven_msgpack_elements() {
        let encoded = rmp_serde::to_vec(&legacy(3)).expect("LegacyRepoConfig serializes");
        assert_eq!(
            encoded.first(),
            Some(&0x97),
            "LegacyRepoConfig must stay a 7-element msgpack array; \
             got a leading byte of {:#04x}",
            encoded.first().copied().unwrap_or(0)
        );
    }

    /// Pins the exact bytes of a fixed positional config, chunker parameters
    /// included. Never update the constant to make it pass: a difference
    /// means repositories written before and after disagree.
    #[test]
    fn legacy_repo_config_golden_bytes() {
        use chrono::TimeZone;
        const GOLDEN: &str = "9705dc0020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
            93cd0100ce00010000ce00800000\
            b04368616368613230506f6c7931333035\
            bb323032332d31312d31345432323a31333a32302e3132333435365a\
            ce02000000ce0c000000";
        let config = LegacyRepoConfig {
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
        let decoded: LegacyRepoConfig =
            rmp_serde::from_slice(&hex::decode(GOLDEN).unwrap()).unwrap();
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

    /// CFG-1: the v6 config is a nine-entry map and round-trips.
    #[test]
    fn v6_config_is_a_map_and_round_trips() {
        for hash in [HashAlgorithm::Blake2b, HashAlgorithm::Blake3] {
            let config = v6(hash);
            let bytes = config.encode().unwrap();
            assert_eq!(bytes[0], 0x89, "nine-entry fixmap");
            let decoded = RepoConfig::decode(&bytes).unwrap();
            assert!(matches!(decoded, DecodedConfig::Map(_)));
            assert_eq!(decoded.validate().unwrap(), RepoFormat::V6(hash));
            assert_eq!(decoded.into_repo_config(), config);
        }
    }

    /// CFG-2: positional configs decode as such and keep their version.
    #[test]
    fn positional_configs_validate_as_v2_and_v3() {
        for (version, format, hash) in [
            (2, RepoFormat::V2, HashAlgorithm::Blake2b),
            (3, RepoFormat::V3, HashAlgorithm::Blake3),
        ] {
            let bytes = rmp_serde::to_vec(&legacy(version)).unwrap();
            let decoded = RepoConfig::decode(&bytes).unwrap();
            assert!(matches!(decoded, DecodedConfig::Positional(_)));
            assert_eq!(decoded.validate().unwrap(), format);
            let config = decoded.into_repo_config();
            assert_eq!(config.version, version);
            assert_eq!(config.chunk_hash, hash.as_str());
            assert!(config.features.is_empty());
        }
    }

    /// CFG-3: newer or malformed map configs are refused.
    #[test]
    fn map_configs_of_other_versions_hashes_or_features_are_refused() {
        let decode = |c: &RepoConfig| {
            RepoConfig::decode(&rmp_serde::to_vec_named(c).unwrap())
                .unwrap()
                .validate()
        };
        let mut c = v6(HashAlgorithm::Blake3);
        c.version = 7;
        assert!(matches!(decode(&c), Err(VykarError::UnsupportedVersion(7))));
        c.version = 5;
        assert!(matches!(decode(&c), Err(VykarError::InvalidFormat(_))));
        let mut c = v6(HashAlgorithm::Blake3);
        c.chunk_hash = "sha256".into();
        let err = decode(&c).unwrap_err().to_string();
        assert!(
            err.contains("sha256") && err.contains("newer vykar"),
            "{err}"
        );
        let mut c = v6(HashAlgorithm::Blake3);
        c.features = vec!["x".into()];
        let err = decode(&c).unwrap_err().to_string();
        assert!(err.contains('x') && err.contains("newer vykar"), "{err}");
        // `multi_writer` is known; another feature beside it is still refused.
        let mut c = v6(HashAlgorithm::Blake3);
        c.features = vec![MULTI_WRITER_FEATURE.into()];
        assert!(matches!(
            decode(&c),
            Ok(RepoFormat::V6(HashAlgorithm::Blake3))
        ));
        c.features.push("y".into());
        let err = decode(&c).unwrap_err().to_string();
        assert!(
            err.contains("feature(s) y,") && err.contains("newer vykar"),
            "{err}"
        );
        // A development binary from before `multi_writer` (no known features)
        // refuses a multi-writer repository by the feature's name.
        let err = refuse_unknown_features(&[MULTI_WRITER_FEATURE.into()], &[])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("feature(s) multi_writer,") && err.contains("written by a newer vykar"),
            "{err}"
        );
        let mut c = v6(HashAlgorithm::Blake3);
        c.max_pack_size = MAX_PACK_SIZE_LIMIT + 1;
        assert!(matches!(decode(&c), Err(VykarError::Config(_))));
    }

    /// `set_writer_mode` adds or removes only `multi_writer`, never twice,
    /// and the mode survives encode, decode and validate.
    #[test]
    fn set_writer_mode_round_trips() {
        let mut c = v6(HashAlgorithm::Blake3);
        assert_eq!(c.writer_mode(), WriterMode::Single);
        c.set_writer_mode(WriterMode::Multi);
        c.set_writer_mode(WriterMode::Multi);
        assert_eq!(c.features, vec![MULTI_WRITER_FEATURE.to_string()]);
        let decoded = RepoConfig::decode(&c.encode().unwrap()).unwrap();
        decoded.validate().unwrap();
        let DecodedConfig::Map(back) = decoded else {
            panic!("a map")
        };
        assert_eq!(back.writer_mode(), WriterMode::Multi);
        assert_eq!(back, c);
        // Another feature (placed directly; validate is not involved) keeps
        // its place.
        c.features = vec!["a".into(), MULTI_WRITER_FEATURE.into(), "b".into()];
        c.set_writer_mode(WriterMode::Multi);
        assert_eq!(c.features, ["a", MULTI_WRITER_FEATURE, "b"]);
        c.set_writer_mode(WriterMode::Single);
        assert_eq!(c.features, ["a", "b"]);
        assert_eq!(c.writer_mode(), WriterMode::Single);
        c.set_writer_mode(WriterMode::Single);
        assert_eq!(c.features, ["a", "b"]);
    }

    /// An otherwise valid config that says mode none is refused with the
    /// unencrypted-repository text; the version gates keep precedence.
    #[test]
    fn validate_refuses_encryption_none() {
        for version in [2u32, 3] {
            let mut c = legacy(version);
            c.encryption = EncryptionMode::None;
            let bytes = rmp_serde::to_vec(&c).unwrap();
            match RepoConfig::decode(&bytes).unwrap().validate() {
                Err(VykarError::Other(m)) => assert_eq!(m, UNENCRYPTED_REPOSITORY_REFUSAL),
                other => panic!("v{version} none should be refused, got {other:?}"),
            }
        }
        let mut c = v6(HashAlgorithm::Blake3);
        c.encryption = EncryptionMode::None;
        match RepoConfig::decode(&c.encode().unwrap()).unwrap().validate() {
            Err(VykarError::Other(m)) => assert_eq!(m, UNENCRYPTED_REPOSITORY_REFUSAL),
            other => panic!("v6 none should be refused, got {other:?}"),
        }
        for version in [6u32, 7] {
            let mut c = legacy(version);
            c.encryption = EncryptionMode::None;
            let err = RepoConfig::decode(&rmp_serde::to_vec(&c).unwrap())
                .unwrap()
                .validate()
                .unwrap_err()
                .to_string();
            assert!(err.contains("development build"), "{err}");
        }
        let mut c = v6(HashAlgorithm::Blake3);
        c.encryption = EncryptionMode::None;
        c.version = 7;
        assert!(matches!(
            RepoConfig::decode(&rmp_serde::to_vec_named(&c).unwrap())
                .unwrap()
                .validate(),
            Err(VykarError::UnsupportedVersion(7))
        ));
    }

    /// CFG-4: positional development formats are refused.
    #[test]
    fn positional_development_formats_are_refused() {
        for version in [0u32, 1, 4, 5, 8, u32::MAX] {
            let bytes = rmp_serde::to_vec(&legacy(version)).unwrap();
            match RepoConfig::decode(&bytes).unwrap().validate() {
                Err(VykarError::UnsupportedVersion(v)) => assert_eq!(v, version),
                other => panic!("version {version} should be unsupported, got {other:?}"),
            }
        }
        for version in [6u32, 7] {
            let bytes = rmp_serde::to_vec(&legacy(version)).unwrap();
            let err = RepoConfig::decode(&bytes)
                .unwrap()
                .validate()
                .unwrap_err()
                .to_string();
            assert!(err.contains("development build"), "{err}");
        }
    }

    /// CFG-5: only version 6 is written as a map.
    #[test]
    fn only_version_6_is_encoded_as_a_map() {
        let mut c = v6(HashAlgorithm::Blake3);
        c.version = 3;
        assert!(matches!(c.encode(), Err(VykarError::InvalidFormat(_))));
    }

    /// CFG-6: an unknown key in the map is ignored.
    #[test]
    fn unknown_map_keys_are_ignored() {
        #[derive(Serialize)]
        struct Newer<'a> {
            #[serde(flatten)]
            config: &'a RepoConfig,
            later_field: u32,
        }
        let config = v6(HashAlgorithm::Blake3);
        let bytes = rmp_serde::to_vec_named(&Newer {
            config: &config,
            later_field: 1,
        })
        .unwrap();
        let decoded = RepoConfig::decode(&bytes).unwrap();
        assert_eq!(
            decoded.validate().unwrap(),
            RepoFormat::V6(HashAlgorithm::Blake3)
        );
        assert_eq!(decoded.into_repo_config(), config);
    }

    #[test]
    fn format_selects_the_hash() {
        assert_eq!(RepoFormat::V2.chunk_hash(), HashAlgorithm::Blake2b);
        assert_eq!(RepoFormat::V3.chunk_hash(), HashAlgorithm::Blake3);
        for h in [HashAlgorithm::Blake2b, HashAlgorithm::Blake3] {
            assert_eq!(RepoFormat::V6(h).chunk_hash(), h);
            assert_eq!(RepoFormat::V6(h).version(), 6);
        }
        assert_eq!(RepoFormat::CURRENT, RepoFormat::V6(HashAlgorithm::Blake3));
    }

    #[test]
    fn upgrade_only_formats() {
        assert!(RepoFormat::V2.is_upgrade_only());
        assert!(RepoFormat::V3.is_upgrade_only());
        assert!(!RepoFormat::CURRENT.is_upgrade_only());
        assert_eq!(
            RepoFormat::V2.upgrade_target(),
            Some(RepoFormat::V6(HashAlgorithm::Blake2b))
        );
        assert_eq!(RepoFormat::V3.upgrade_target(), Some(RepoFormat::CURRENT));
        assert_eq!(RepoFormat::CURRENT.upgrade_target(), None);
    }
}
