// Test-only env mutation; SAFETY documented per block.
#![allow(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};
use vykar_types::chunk_id::ChunkHasher;
use vykar_types::hash::HashAlgorithm;

use crate::repo::ChunkerParams;
use crate::repo::{EncryptionMode, Repository, WriteSessionOptions};
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::error::{Result, VykarError};

static TEST_ENV_INIT: Once = Once::new();

/// Hook command that creates (or touches) `path`.
pub fn cmd_touch(path: &std::path::Path) -> String {
    #[cfg(windows)]
    {
        let escaped = path.display().to_string().replace('\'', "''");
        format!("New-Item -Path '{escaped}' -ItemType File -Force | Out-Null")
    }

    #[cfg(not(windows))]
    {
        format!("touch '{}'", path.display())
    }
}

/// A shell word that expands to `prefix`, the value of environment variable
/// `var`, then `suffix`.
pub fn env_word(prefix: &str, var: &str, suffix: &str) -> String {
    #[cfg(windows)]
    {
        format!("\"{prefix}$($env:{var}){suffix}\"")
    }

    #[cfg(not(windows))]
    {
        format!("{prefix}\"${var}\"{suffix}")
    }
}

/// Hook command that appends `value` (a raw shell word, see [`env_word`]) as a
/// line to `path`.
pub fn cmd_append(path: &std::path::Path, value: &str) -> String {
    #[cfg(windows)]
    {
        let escaped = path.display().to_string().replace('\'', "''");
        format!("Add-Content -Path '{escaped}' -Value {value}")
    }

    #[cfg(not(windows))]
    {
        format!("echo {value} >> '{}'", path.display())
    }
}

/// Hook command that renames `from` to `to`, as a mount would make a
/// repository appear.
pub fn cmd_rename(from: &std::path::Path, to: &std::path::Path) -> String {
    #[cfg(windows)]
    {
        let q = |p: &std::path::Path| p.display().to_string().replace('\'', "''");
        format!(
            "Move-Item -LiteralPath '{}' -Destination '{}'",
            q(from),
            q(to)
        )
    }

    #[cfg(not(windows))]
    {
        format!("mv '{}' '{}'", from.display(), to.display())
    }
}

/// Tests that mutate process-global state (CWD, env vars) must hold this lock.
/// Shared across all test modules (inode_walk, resolve, etc.).
pub static CWD_LOCK: Mutex<()> = Mutex::new(());

pub fn init_test_environment() {
    TEST_ENV_INIT.call_once(|| {
        let base = std::env::temp_dir().join(format!("vykar-tests-{}", std::process::id()));
        let home = base.join("home");
        let cache = base.join("cache");
        let config = base.join("config");
        let _ = std::fs::create_dir_all(&home);
        let _ = std::fs::create_dir_all(&cache);
        let _ = std::fs::create_dir_all(&config);

        // SAFETY: Rust 2024 marks env mutation as unsafe due to process-global
        // races. This runs once via `Once::call_once` at test-process startup
        // before any threads are spawned, so the race window is empty.
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("XDG_CACHE_HOME", &cache);
            // Windows reads these instead; without them the cache lands in the
            // real user profile.
            std::env::set_var("USERPROFILE", &home);
            std::env::set_var("LOCALAPPDATA", &cache);
            // The config directory (installation ID) wins over HOME when
            // set in the developer's shell.
            std::env::set_var("XDG_CONFIG_HOME", &config);
            std::env::set_var("APPDATA", &config);
        }
    });
}

/// In-memory storage backend for testing. Thread-safe via Mutex.
pub struct MemoryBackend {
    data: Mutex<HashMap<String, Vec<u8>>>,
    /// Refuse what an append-only vykar-server refuses
    /// (`vykar_protocol::KeyClass`). Keys outside the server's schema are
    /// refused an overwrite and allowed a delete, as before.
    append_only: bool,
    /// `Some`: `list_meta` reports each key's mtime, the time of its last
    /// put or what [`Self::set_mtime`] set. `None`: no mtimes.
    mtimes: Option<Mutex<HashMap<String, std::time::SystemTime>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            append_only: false,
            mtimes: None,
        }
    }

    /// A backend whose `list_meta` reports an mtime per key: the time of its
    /// last put (`SystemTime::now()`), or what [`Self::set_mtime`] set.
    pub fn with_mtimes() -> Self {
        Self {
            mtimes: Some(Mutex::new(HashMap::new())),
            ..Self::new()
        }
    }

    /// Set `key`'s mtime. Panics on a backend without mtimes.
    pub fn set_mtime(&self, key: &str, mtime: std::time::SystemTime) {
        self.mtimes
            .as_ref()
            .expect("set_mtime needs MemoryBackend::with_mtimes")
            .lock()
            .unwrap()
            .insert(key.to_string(), mtime);
    }

    /// A backend that permits creation and applies the append-only key-class
    /// table to overwrites and deletes.
    pub fn append_only() -> Self {
        Self {
            append_only: true,
            ..Self::new()
        }
    }

    /// [`Self::append_only`] with the mtimes of [`Self::with_mtimes`].
    pub fn append_only_with_mtimes() -> Self {
        Self {
            append_only: true,
            ..Self::with_mtimes()
        }
    }
}

/// An append-only server allows overwriting `key`. Refused for keys outside
/// the schema.
fn append_only_overwrite_ok(key: &str) -> bool {
    vykar_protocol::append_only_allows(key, vykar_protocol::Verb::Overwrite)
}

/// An append-only server allows deleting `key`. Allowed for keys outside the
/// schema, which only synthetic test keys are.
fn append_only_delete_ok(key: &str) -> bool {
    vykar_protocol::key_class(key)
        .is_none_or(|c| c.append_only_allows(vykar_protocol::Verb::Delete))
}

impl StorageBackend for MemoryBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let map = self.data.lock().unwrap();
        Ok(map.get(key).cloned())
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let mut map = self.data.lock().unwrap();
        if self.append_only && map.contains_key(key) && !append_only_overwrite_ok(key) {
            return Err(VykarError::Other(format!(
                "403 Forbidden: repository is append-only, {key} already exists"
            )));
        }
        map.insert(key.to_string(), data.to_vec());
        if let Some(mtimes) = &self.mtimes {
            mtimes
                .lock()
                .unwrap()
                .insert(key.to_string(), std::time::SystemTime::now());
        }
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        if self.append_only && !append_only_delete_ok(key) {
            return Err(VykarError::Other(format!(
                "403 Forbidden: repository is append-only, delete of {key} refused"
            )));
        }
        let mut map = self.data.lock().unwrap();
        map.remove(key);
        if let Some(mtimes) = &self.mtimes {
            mtimes.lock().unwrap().remove(key);
        }
        Ok(())
    }

    fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        let mut listed = self.list_sized(prefix)?;
        if let Some(mtimes) = &self.mtimes {
            let mtimes = mtimes.lock().unwrap();
            for o in &mut listed {
                o.mtime = mtimes.get(&o.key).copied();
            }
        }
        Ok(listed)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let map = self.data.lock().unwrap();
        Ok(map.contains_key(key))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let map = self.data.lock().unwrap();
        let keys: Vec<String> = map
            .keys()
            .filter(|k| k.starts_with(prefix) && !k.ends_with('/'))
            .cloned()
            .collect();
        Ok(keys)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let map = self.data.lock().unwrap();
        match map.get(key) {
            Some(data) => {
                let start = offset as usize;
                let end = start.checked_add(length as usize).ok_or_else(|| {
                    VykarError::Other(format!(
                        "short read on {key} at offset {offset}: offset + length overflows usize"
                    ))
                })?;
                if start >= data.len() || end > data.len() {
                    return Err(VykarError::Other(format!(
                        "short read on {key} at offset {offset}: expected {length} bytes, got {}",
                        data.len().saturating_sub(start)
                    )));
                }
                Ok(Some(data[start..end].to_vec()))
            }
            None => Ok(None),
        }
    }

    fn create_dir(&self, _key: &str) -> Result<()> {
        // No-op for in-memory backend
        Ok(())
    }

    // Local, default width.
    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            local: true,
            staged_put: false,
            request_width: 2,
            transfer_width: 2,
        }
    }

    vykar_storage::unsupported_server_ops!();
}

/// Create a plaintext repository backed by MemoryBackend.
pub fn test_repo_plaintext() -> Repository {
    init_test_environment();
    let storage = Box::new(MemoryBackend::new());
    let mut repo = Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerParams::default(),
        None,
        None,
        None,
    )
    .expect("failed to init test repo");
    repo.begin_write_session(&WriteSessionOptions::default())
        .expect("failed to begin write session");
    repo
}

/// Fixed chunk hasher for deterministic tests.
///
/// Pinned to BLAKE2b: these tests assert on stored digests, and format-v3
/// coverage comes from real v3 repositories, not from a synthetic hasher.
pub fn test_chunk_hasher() -> ChunkHasher {
    chunk_hasher_for([0xAA; 32])
}

/// A BLAKE2b chunk hasher over an explicit key, for tests that assert on a
/// particular key rather than the fixed one.
pub fn chunk_hasher_for(key: [u8; 32]) -> ChunkHasher {
    ChunkHasher::new(HashAlgorithm::Blake2b, key)
}

/// Shared handle to inspect backend traffic: keys written via `put()`, and
/// counts of `get_range()`, `list()`, `list_sized()`, `list_tree_meta()` and
/// `size()` calls.
#[derive(Clone)]
pub struct PutLog {
    puts: std::sync::Arc<Mutex<Vec<String>>>,
    get_ranges: std::sync::Arc<AtomicU64>,
    lists: std::sync::Arc<AtomicU64>,
    sized_lists: std::sync::Arc<AtomicU64>,
    tree_lists: std::sync::Arc<AtomicU64>,
    sizes: std::sync::Arc<AtomicU64>,
}

impl PutLog {
    fn new() -> Self {
        Self {
            puts: std::sync::Arc::new(Mutex::new(Vec::new())),
            get_ranges: std::sync::Arc::new(AtomicU64::new(0)),
            lists: std::sync::Arc::new(AtomicU64::new(0)),
            sized_lists: std::sync::Arc::new(AtomicU64::new(0)),
            tree_lists: std::sync::Arc::new(AtomicU64::new(0)),
            sizes: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn list_count(&self) -> u64 {
        self.lists.load(Ordering::Relaxed)
    }

    pub fn sized_list_count(&self) -> u64 {
        self.sized_lists.load(Ordering::Relaxed)
    }

    /// Counted `list_tree_meta()` calls (only in a tree-listing mode).
    pub fn tree_list_count(&self) -> u64 {
        self.tree_lists.load(Ordering::Relaxed)
    }

    pub fn size_count(&self) -> u64 {
        self.sizes.load(Ordering::Relaxed)
    }

    /// Reset the `list()`, `list_sized()`, `list_tree_meta()` and `size()`
    /// counters.
    pub fn reset_metadata_counts(&self) {
        for c in [
            &self.lists,
            &self.sized_lists,
            &self.tree_lists,
            &self.sizes,
        ] {
            c.store(0, Ordering::Relaxed);
        }
    }

    /// Return all keys that were written via `put()` since the last `clear()`.
    pub fn entries(&self) -> Vec<String> {
        self.puts.lock().unwrap().clone()
    }

    /// Clear the recorded put log. Leaves the `get_range()` counter alone.
    pub fn clear(&self) {
        self.puts.lock().unwrap().clear();
    }

    /// Number of `get_range()` calls since the last `reset_get_range_count()`.
    pub fn get_range_count(&self) -> u64 {
        self.get_ranges.load(Ordering::Relaxed)
    }

    /// Reset the `get_range()` counter to zero.
    pub fn reset_get_range_count(&self) {
        self.get_ranges.store(0, Ordering::Relaxed);
    }

    fn record(&self, key: &str) {
        self.puts.lock().unwrap().push(key.to_string());
    }

    fn record_get_range(&self) {
        self.get_ranges.fetch_add(1, Ordering::Relaxed);
    }
}

/// Storage wrapper that records which keys were passed to `put()` and counts
/// `get_range()` calls. Delegates all operations to an inner `MemoryBackend`.
/// Use `RecordingBackend::new()` to get the backend and a shared `PutLog`.
pub struct RecordingBackend {
    inner: MemoryBackend,
    log: PutLog,
    sized_listing: bool,
    tree_listing: TreeListing,
}

/// How [`RecordingBackend`] answers `list_tree_meta`.
enum TreeListing {
    /// No server: `server_ops()` is None, so nothing asks for a tree listing.
    Unsupported,
    /// One listing with sizes, as a vykar-server answers it.
    Answer,
    /// One listing without sizes, as a server before `?list&sized` answers it.
    AnswerUnsized,
    /// An injected error.
    Fail,
}

impl RecordingBackend {
    pub fn new() -> (Self, PutLog) {
        let log = PutLog::new();
        (
            Self {
                inner: MemoryBackend::new(),
                log: log.clone(),
                sized_listing: false,
                tree_listing: TreeListing::Unsupported,
            },
            log,
        )
    }

    /// Like [`Self::new`], but `list_sized` reports sizes (as S3/SFTP/local do).
    pub fn with_sized_listing() -> (Self, PutLog) {
        let (backend, log) = Self::new();
        (
            Self {
                sized_listing: true,
                ..backend
            },
            log,
        )
    }

    /// Like [`Self::with_sized_listing`], plus a one-request
    /// `list_tree_meta` with sizes (a vykar-server).
    pub fn with_tree_listing() -> (Self, PutLog) {
        Self::with_tree_mode(true, TreeListing::Answer)
    }

    /// Like [`Self::new`], plus a one-request `list_tree_meta` that reports
    /// no sizes (a vykar-server before `?list&sized`).
    pub fn with_unsized_tree_listing() -> (Self, PutLog) {
        Self::with_tree_mode(false, TreeListing::AnswerUnsized)
    }

    /// Like [`Self::with_sized_listing`], but `list_tree_meta` fails.
    pub fn with_failing_tree_listing() -> (Self, PutLog) {
        Self::with_tree_mode(true, TreeListing::Fail)
    }

    fn with_tree_mode(sized_listing: bool, tree_listing: TreeListing) -> (Self, PutLog) {
        let (backend, log) = Self::new();
        (
            Self {
                sized_listing,
                tree_listing,
                ..backend
            },
            log,
        )
    }

    // Sizes come from the inner backend, uncounted.
    fn sized_from_inner(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.inner
            .list(prefix)?
            .into_iter()
            .map(|key| {
                let size = self.inner.size(&key)?;
                Ok(vykar_storage::ListedObject {
                    key,
                    size,
                    mtime: None,
                })
            })
            .collect()
    }
}

/// The server behind a tree-listing [`RecordingBackend`]: tree listings only.
impl vykar_storage::ServerOps for RecordingBackend {
    fn offers(&self, op: vykar_storage::ServerOp) -> bool {
        op == vykar_storage::ServerOp::ListTree
    }

    fn capabilities(&self) -> Result<vykar_protocol::ServerCapabilities> {
        Ok(fake_server_caps())
    }

    fn init(&self) -> Result<()> {
        Ok(())
    }

    fn list_tree_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        let answer = match self.tree_listing {
            TreeListing::Unsupported => {
                return Err(VykarError::Other("no tree listing without a server".into()));
            }
            TreeListing::Answer => self.sized_from_inner(prefix),
            TreeListing::AnswerUnsized => self.inner.list_sized(prefix),
            TreeListing::Fail => Err(VykarError::Other("injected tree listing failure".into())),
        };
        self.log.tree_lists.fetch_add(1, Ordering::Relaxed);
        answer
    }
}

/// `/health` as the test fakes that implement `ServerOps` answer it: both
/// hashes, the segmented index, every key directory and the four operation
/// fields. Not `repack_trailer`: only the compact test fake appends one, and
/// it overrides the field. Not `conditional_put`.
pub(crate) fn fake_server_caps() -> vykar_protocol::ServerCapabilities {
    vykar_protocol::ServerCapabilities {
        status: "ok".into(),
        version: "test".into(),
        protocol_version: vykar_protocol::PROTOCOL_VERSION,
        hashes: vec![
            HashAlgorithm::Blake2b.as_str().into(),
            HashAlgorithm::Blake3.as_str().into(),
        ],
        pack_parts: true,
        segmented_index: true,
        key_dirs: vykar_protocol::KNOWN_ROOT_DIRS
            .iter()
            .map(|d| (*d).to_string())
            .collect(),
        conditional_put: false,
        object_sizes: true,
        list_tree: true,
        verify_packs: true,
        repack: true,
        repack_trailer: false,
        append_only: false,
    }
}

impl InnerBackend for RecordingBackend {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for RecordingBackend;
    except [
        put, list, get_range, get_range_into, open_range, put_owned, put_pack, begin_staged_put,
        size, list_sized, list_meta, conditional, server_ops, profile
    ];

    fn list_meta(
        &self,
        prefix: &str,
    ) -> vykar_types::error::Result<Vec<vykar_storage::ListedObject>> {
        self.list_sized(prefix)
    }

    // Test wrapper: keeps every write on `put`.
    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.log.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.log.sized_lists.fetch_add(1, Ordering::Relaxed);
        if !self.sized_listing {
            return self.inner.list_sized(prefix);
        }
        self.sized_from_inner(prefix)
    }

    /// A server that lists trees, in every mode but `Unsupported`.
    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        match self.tree_listing {
            TreeListing::Unsupported => None,
            _ => Some(self),
        }
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        self.log.sizes.fetch_add(1, Ordering::Relaxed);
        self.inner.size(key)
    }

    // Keep packs on the intercepted `put` path. Recorded puts must see every pack.
    fn begin_staged_put(
        &self,
        _prefix: &str,
    ) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        Ok(None)
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            staged_put: false,
            ..self.inner.profile()
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.log.record(key);
        self.inner.put(key, data)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.log.record_get_range();
        self.inner.get_range(key, offset, length)
    }

    // Counted as one range read, same as the `get_range` it used to reach
    // through the trait default. Every `open_range` (restore, coalesced
    // item-stream loads) reads via this method.
    fn get_range_into(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &mut Vec<u8>,
    ) -> Result<bool> {
        self.log.record_get_range();
        self.inner.get_range_into(key, offset, length, buf)
    }

    // One recorded range read per opened range, through `get_range_into`.
    fn open_range<'a>(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &'a mut Vec<u8>,
    ) -> Result<Option<Box<dyn vykar_storage::RangeReader + 'a>>> {
        vykar_storage::buffered_range(self, key, offset, length, buf)
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.log.record(key);
        self.inner.put(key, &data)
    }

    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        self.log.record(key);
        self.inner.put(key, data.as_slice())
    }
}

/// Pack-upload fault injection: `put_owned`/`put_pack` fail for keys in `fail`, and the
/// first pack upload of a [`Self::gated`] backend blocks until the gate's
/// sender fires (or is dropped).
pub struct PackUploadBackend {
    inner: std::sync::Arc<dyn StorageBackend>,
    pub fail: Mutex<std::collections::HashSet<String>>,
    gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

impl PackUploadBackend {
    pub fn new(inner: std::sync::Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            fail: Default::default(),
            gate: Default::default(),
        }
    }

    pub fn gated(inner: std::sync::Arc<dyn StorageBackend>) -> (Self, std::sync::mpsc::Sender<()>) {
        let (tx, rx) = std::sync::mpsc::channel();
        let backend = Self::new(inner);
        *backend.gate.lock().unwrap() = Some(rx);
        (backend, tx)
    }

    fn intercept(&self, key: &str) -> Result<()> {
        if key.starts_with("packs/") {
            let gate = self.gate.lock().unwrap().take();
            if let Some(rx) = gate {
                let _ = rx.recv();
            }
        }
        if self.fail.lock().unwrap().contains(key) {
            return Err(VykarError::Other("injected pack upload failure".into()));
        }
        Ok(())
    }
}

impl InnerBackend for PackUploadBackend {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for PackUploadBackend;
    except [put_owned, put_pack, begin_staged_put, conditional, profile];

    // Test wrapper: keeps every write on `put`.
    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

    // Keep packs off staged writes. Gating and fault injection act on
    // `put_owned`/`put_pack`.
    fn begin_staged_put(
        &self,
        _prefix: &str,
    ) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        Ok(None)
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            staged_put: false,
            ..self.inner.profile()
        }
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.intercept(key)?;
        self.inner.put_owned(key, data)
    }

    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        self.intercept(key)?;
        self.inner.put_pack(key, data)
    }
}

/// The entries of either index type, in chunk-ID order (tests).
pub(crate) trait EntryIter {
    fn entry_vec(
        &self,
    ) -> Vec<(
        vykar_types::chunk_id::ChunkId,
        crate::index::ChunkIndexEntry,
    )>;
}

impl EntryIter for crate::index::ChunkIndex {
    fn entry_vec(
        &self,
    ) -> Vec<(
        vykar_types::chunk_id::ChunkId,
        crate::index::ChunkIndexEntry,
    )> {
        let mut v: Vec<_> = self.iter().map(|(id, e)| (*id, *e)).collect();
        v.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        v
    }
}

impl EntryIter for crate::index::LiveIndex {
    fn entry_vec(
        &self,
    ) -> Vec<(
        vykar_types::chunk_id::ChunkId,
        crate::index::ChunkIndexEntry,
    )> {
        self.iter().collect()
    }
}

impl EntryIter for crate::index::ChunkTable {
    fn entry_vec(
        &self,
    ) -> Vec<(
        vykar_types::chunk_id::ChunkId,
        crate::index::ChunkIndexEntry,
    )> {
        (0..self.len() as u32)
            .map(|p| {
                let e = self.entry_at(p);
                (
                    e.chunk_id,
                    crate::index::ChunkIndexEntry {
                        stored_size: e.stored_size,
                        pack_id: e.pack_id,
                        pack_offset: e.pack_offset,
                    },
                )
            })
            .collect()
    }
}

/// `index` as a scratch base table (every pack at `trailer_len` 0), for
/// tests that build a table by hand.
pub(crate) fn base_table_of(
    index: &crate::index::ChunkIndex,
) -> std::sync::Arc<crate::index::ChunkTable> {
    let anchor = crate::repo::index_store::cache_anchor(None);
    let (tmp, _) = crate::index::table::base_body_from_chunk_index(index, &anchor).unwrap();
    std::sync::Arc::new(crate::index::ChunkTable::install(tmp, None, None).unwrap())
}

/// A dedup prefilter over `index` as a base table with nothing applied.
pub(crate) fn prefilter_of(
    index: &crate::index::ChunkIndex,
) -> crate::index::table::DedupPrefilter {
    let live = crate::index::LiveIndex::from_table(base_table_of(index));
    crate::index::table::DedupPrefilter::new(live.view().unwrap())
}

/// Save the active write session (its chunks enter the index) and begin a
/// new one, for tests that read their own chunks through the index.
pub(crate) fn save_and_resume(repo: &mut Repository) {
    repo.save_state().expect("save_state");
    repo.begin_write_session(&WriteSessionOptions::default())
        .expect("begin write session");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_backend_append_only_follows_the_table() {
        let store = MemoryBackend::append_only();
        for key in [
            "index",
            "locks/x",
            "config",
            "index.h/1",
            "snapshots/s",
            "sessions/s",
            "k",
        ] {
            store.put(key, b"one").unwrap();
        }
        store.put("index", b"two").unwrap();
        store.put("locks/x", b"two").unwrap();
        for key in ["config", "index.h/1"] {
            let err = store.put(key, b"two").unwrap_err().to_string();
            assert!(err.contains("append-only"), "{key}: {err}");
            assert_eq!(store.get(key).unwrap().unwrap(), b"one");
        }
        let err = store.delete("snapshots/s").unwrap_err().to_string();
        assert!(err.contains("append-only"), "{err}");
        assert!(store.exists("snapshots/s").unwrap());
        store.delete("sessions/s").unwrap();
        store.delete("k").unwrap();
        assert!(!store.exists("sessions/s").unwrap());
        assert!(!store.exists("k").unwrap());
    }
}
