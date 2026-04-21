use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::snapshot::item::ChunkRef;
use vykar_common::paths;
use vykar_crypto::CryptoEngine;
use vykar_types::error::Result;
use vykar_types::snapshot_id::SnapshotId;

/// Atomic write via temp file + fsync + rename. On error the temp file is
/// cleaned up automatically (NamedTempFile drops on panic/early return).
pub(crate) fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let dir = path.parent().unwrap_or(Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(data)?;
    tmp.as_file().sync_data()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

use super::format::{
    pack_object_streaming_with_context, unpack_object_expect_with_context, ObjectType,
};

/// Compute the per-repo cache directory.
///
/// With `cache_dir_override`: `<override>/<repo_id_hex>/`
/// Without: `<platform_cache_dir>/vykar/<repo_id_hex>/`
pub(crate) fn repo_cache_dir(repo_id: &[u8], cache_dir_override: Option<&Path>) -> Option<PathBuf> {
    let base = match cache_dir_override {
        Some(dir) => Some(dir.to_path_buf()),
        None => paths::cache_dir().map(|d| d.join("vykar")),
    };
    base.map(|b| b.join(hex::encode(repo_id)))
}

/// 16-byte BLAKE2b hash of a file path, used as a compact HashMap key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PathHash([u8; 16]);

impl serde::Serialize for PathHash {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for PathHash {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct PathHashVisitor;

        impl<'de> serde::de::Visitor<'de> for PathHashVisitor {
            type Value = PathHash;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("16-byte path hash")
            }

            fn visit_bytes<E: serde::de::Error>(
                self,
                v: &[u8],
            ) -> std::result::Result<PathHash, E> {
                if v.len() != 16 {
                    return Err(E::invalid_length(v.len(), &"16 bytes"));
                }
                let mut arr = [0u8; 16];
                arr.copy_from_slice(v);
                Ok(PathHash(arr))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<PathHash, E> {
                Ok(hash_path(v))
            }
        }

        deserializer.deserialize_any(PathHashVisitor)
    }
}

fn hash_path(path: &str) -> PathHash {
    let mut hasher = Blake2bVar::new(16).expect("valid output size");
    hasher.update(path.as_bytes());
    let mut out = [0u8; 16];
    hasher.finalize_variable(&mut out).expect("correct length");
    PathHash(out)
}

/// Serde helper: serialize `Arc<Vec<ChunkRef>>` as a plain sequence (wire-format
/// unchanged), deserialize via Vec then wrap in Arc.
mod arc_vec_chunk_refs {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        v: &Arc<Vec<ChunkRef>>,
        s: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        v.as_slice().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<Arc<Vec<ChunkRef>>, D::Error> {
        Vec::<ChunkRef>::deserialize(d).map(Arc::new)
    }
}

/// Cached filesystem metadata for a file, used to skip re-reading unchanged files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCacheEntry {
    pub device: u64,
    pub inode: u64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub size: u64,
    #[serde(with = "arc_vec_chunk_refs")]
    pub chunk_refs: Arc<Vec<ChunkRef>>,
}

/// A per-source section of the file cache, keyed by source paths.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSection {
    /// The snapshot ID that anchors this section's validity.
    pub anchor_snapshot_id: SnapshotId,
    /// The actual cached entries.
    pub(crate) entries: HashMap<PathHash, FileCacheEntry>,
}

/// Maps path hashes to their cached metadata and chunk references,
/// scoped by individual canonicalized source paths.
///
/// Each canonicalized source path gets its own `CacheSection` so that
/// adding or removing paths preserves cache for unchanged paths.
/// Legacy joined-key sections from prior format will linger as orphans
/// until their anchor snapshot is pruned by `invalidate_missing_snapshots`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileCache {
    sections: BTreeMap<String, CacheSection>,
    /// Runtime-only: the canonicalized source paths currently active for
    /// `insert`/`lookup`. Sorted by length descending for longest-prefix-match.
    #[serde(skip)]
    active_keys: Vec<String>,
}

/// Structured outcome of decoding an on-disk cache blob. `load` matches on
/// this so a rejected-to-empty result does not masquerade as a legitimately
/// empty decode in the logs.
enum CacheDecode {
    Loaded(FileCache),
    Rejected { reason: &'static str },
    Malformed { error: String },
}

impl FileCache {
    pub fn new() -> Self {
        Self {
            sections: BTreeMap::new(),
            active_keys: Vec::new(),
        }
    }

    /// Find the section key for a given path by longest-prefix-match.
    /// Iterates `active_keys` (sorted longest first), returns the first key
    /// where `Path::new(path).starts_with(key)`.
    fn find_section_key(&self, path: &str) -> Option<&str> {
        let p = Path::new(path);
        self.active_keys
            .iter()
            .find(|key| p.starts_with(key.as_str()))
            .map(|s| s.as_str())
    }

    /// Start new sections for the given canonicalized roots on the **write** cache.
    /// Creates one section per root (pre-sized via `capacity_hints`) and sets all as active.
    pub fn begin_sections(&mut self, roots: &[String], capacity_hints: &[usize]) {
        assert_eq!(roots.len(), capacity_hints.len());
        self.active_keys.clear();
        for (root, &hint) in roots.iter().zip(capacity_hints.iter()) {
            self.sections.insert(
                root.clone(),
                CacheSection {
                    anchor_snapshot_id: SnapshotId([0u8; 32]),
                    entries: HashMap::with_capacity(hint),
                },
            );
            self.active_keys.push(root.clone());
        }
        // Sort by length descending for longest-prefix-match.
        self.active_keys.sort_by_key(|k| std::cmp::Reverse(k.len()));
    }

    /// Activate sections matching the given canonicalized roots for lookup.
    /// Returns `true` if ALL roots have a matching section.
    /// Partial activation is fine — lookup returns `None` for paths without a
    /// cached section. Sets `active_keys` to the found keys (sorted by length desc).
    pub fn activate_for_walk_roots(&mut self, roots: &[String]) -> bool {
        self.active_keys.clear();
        let mut all_found = true;
        for root in roots {
            if self.sections.contains_key(root) {
                self.active_keys.push(root.clone());
            } else {
                all_found = false;
            }
        }
        self.active_keys.sort_by_key(|k| std::cmp::Reverse(k.len()));
        all_found
    }

    /// Clear the active lookup keys so no section is searched.
    /// Called when the cached section is invalid (pruned anchor, changed paths).
    pub fn clear_active_for_lookup(&mut self) {
        self.active_keys.clear();
    }

    /// Look up a file in the active section. Returns the cached chunk refs only if all
    /// metadata fields match exactly (device, inode, mtime_ns, ctime_ns, size).
    pub fn lookup(
        &self,
        path: &str,
        device: u64,
        inode: u64,
        mtime_ns: i64,
        ctime_ns: i64,
        size: u64,
    ) -> Option<Arc<Vec<ChunkRef>>> {
        if self.active_keys.is_empty() {
            return None;
        }
        let key = self.find_section_key(path)?;
        let section = self.sections.get(key)?;
        let key = hash_path(path);
        let entry = section.entries.get(&key)?;
        if entry.device == device
            && entry.inode == inode
            && entry.mtime_ns == mtime_ns
            && entry.ctime_ns == ctime_ns
            && entry.size == size
        {
            Some(Arc::clone(&entry.chunk_refs))
        } else {
            None
        }
    }

    /// Insert or update a file's cache entry in all matching active sections.
    ///
    /// With overlapping roots (e.g. `["/data", "/data/sub"]`), a file under
    /// `/data/sub/` is inserted into both sections. This ensures that removing
    /// the more-specific root in a later backup does not cause cache misses for
    /// files still covered by the broader root.
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &mut self,
        path: &str,
        device: u64,
        inode: u64,
        mtime_ns: i64,
        ctime_ns: i64,
        size: u64,
        chunk_refs: Arc<Vec<ChunkRef>>,
    ) {
        let p = Path::new(path);
        let path_hash = hash_path(path);
        let entry = FileCacheEntry {
            device,
            inode,
            mtime_ns,
            ctime_ns,
            size,
            chunk_refs,
        };
        let matching: Vec<String> = self
            .active_keys
            .iter()
            .filter(|key| p.starts_with(key.as_str()))
            .cloned()
            .collect();
        assert!(
            !matching.is_empty(),
            "insert called without matching active section"
        );
        // Insert into all matching sections (handles overlapping roots).
        // Clone for all but the last, move for the last.
        for key in &matching[..matching.len() - 1] {
            self.sections
                .get_mut(key)
                .expect("insert called without active section")
                .entries
                .insert(path_hash, entry.clone());
        }
        self.sections
            .get_mut(matching.last().unwrap())
            .expect("insert called without active section")
            .entries
            .insert(path_hash, entry);
    }

    /// Check if the active section has an entry for this path.
    pub fn contains(&self, path: &str) -> bool {
        let Some(key) = self.find_section_key(path) else {
            return false;
        };
        let Some(section) = self.sections.get(key) else {
            return false;
        };
        section.entries.contains_key(&hash_path(path))
    }

    /// Finalize all active sections with a snapshot ID.
    /// Called on the **write** cache after the snapshot ID is generated.
    pub fn finalize_sections(&mut self, snapshot_id: SnapshotId) {
        for key in &self.active_keys {
            let section = self
                .sections
                .get_mut(key)
                .expect("finalize_sections: active key missing from sections");
            section.anchor_snapshot_id = snapshot_id;
        }
    }

    /// Extract all finalized active sections for merging into the persistent cache.
    pub fn take_active_sections(&mut self) -> Vec<(String, CacheSection)> {
        let keys = std::mem::take(&mut self.active_keys);
        keys.into_iter()
            .filter_map(|key| self.sections.remove(&key).map(|s| (key, s)))
            .collect()
    }

    /// Replace one section in the persistent cache, leaving others untouched.
    pub fn merge_section(&mut self, label: &str, section: CacheSection) {
        self.sections.insert(label.to_string(), section);
    }

    /// Remove sections whose anchor snapshot no longer exists.
    /// Returns the number of sections invalidated.
    pub fn invalidate_missing_snapshots(&mut self, exists: &dyn Fn(&SnapshotId) -> bool) -> usize {
        let before = self.sections.len();
        self.sections
            .retain(|_label, section| exists(&section.anchor_snapshot_id));
        before - self.sections.len()
    }

    /// Return a human-readable reason why sections are missing for some roots.
    /// Returns `None` if all canonicalized roots have matching sections.
    pub fn diagnose_sections(&self, roots: &[String]) -> Option<String> {
        let missing: Vec<&str> = roots
            .iter()
            .filter(|root| !self.sections.contains_key(root.as_str()))
            .map(|root| root.as_str())
            .collect();
        if missing.is_empty() {
            None
        } else {
            Some(format!(
                "no section for paths {:?} (available sections: {})",
                missing,
                self.sections.len()
            ))
        }
    }

    /// Entry count for the section matching `root`, or 0 if absent.
    pub fn section_len(&self, root: &str) -> usize {
        self.sections.get(root).map_or(0, |s| s.entries.len())
    }

    /// Total entries across all sections.
    pub fn len(&self) -> usize {
        self.sections.values().map(|s| s.entries.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.sections.values().all(|s| s.entries.is_empty())
    }

    /// Minimum plaintext bytes per serialized entry. Used as a post-decode
    /// plausibility ratio (16-byte key + metadata ≥ ~40 bytes).
    const MIN_BYTES_PER_ENTRY: usize = 40;

    /// Decode from msgpack plaintext. Old format (flat HashMap) and bogus
    /// input collapse to an empty cache so the one-time migration path is
    /// non-fatal.
    ///
    /// Thin wrapper over the private `decode_from_plaintext_outcome` helper
    /// for callers that only need a `FileCache` and can treat rejected and
    /// malformed input alike as "start fresh".
    pub fn decode_from_plaintext(plaintext: &[u8]) -> Result<Self> {
        Ok(match Self::decode_from_plaintext_outcome(plaintext) {
            CacheDecode::Loaded(cache) => cache,
            CacheDecode::Rejected { .. } | CacheDecode::Malformed { .. } => Self::new(),
        })
    }

    /// Structured decode: lets callers distinguish "decoded fine" from
    /// "rejected to empty" from "malformed input", so log sites aren't
    /// forced to claim every empty cache was legitimately empty.
    ///
    /// The ratio check below runs *after* `rmp_serde::from_slice` has
    /// already allocated, so it doesn't prevent allocation — it only
    /// catches msgpack containers whose inflated length headers rmp_serde
    /// happened to fill with garbage.
    fn decode_from_plaintext_outcome(plaintext: &[u8]) -> CacheDecode {
        match rmp_serde::from_slice::<FileCache>(plaintext) {
            Ok(cache) => {
                let max_entries = plaintext.len() / Self::MIN_BYTES_PER_ENTRY;
                if cache.len() > max_entries {
                    return CacheDecode::Rejected {
                        reason: "entry count exceeds plausible ratio",
                    };
                }
                CacheDecode::Loaded(cache)
            }
            Err(e) => CacheDecode::Malformed {
                error: e.to_string(),
            },
        }
    }

    /// Return the local filesystem path for the cache file.
    fn cache_path(repo_id: &[u8], cache_dir_override: Option<&Path>) -> Option<PathBuf> {
        repo_cache_dir(repo_id, cache_dir_override).map(|d| d.join("filecache"))
    }

    /// Load the file cache from local disk. Returns an empty cache if the
    /// file doesn't exist or can't be read (backward-compatible).
    pub fn load(
        repo_id: &[u8],
        crypto: &dyn CryptoEngine,
        cache_dir_override: Option<&Path>,
    ) -> Self {
        let Some(path) = Self::cache_path(repo_id, cache_dir_override) else {
            return Self::new();
        };
        let plaintext = {
            let data = match std::fs::read(&path) {
                Ok(d) => d,
                Err(_) => {
                    debug!(path = %path.display(), "file cache: no cache file on disk, starting fresh");
                    return Self::new();
                }
            };
            match unpack_object_expect_with_context(
                &data,
                ObjectType::FileCache,
                b"filecache",
                crypto,
            ) {
                Ok(pt) => pt,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "file cache: failed to decrypt (stale or corrupt cache file?), starting fresh");
                    return Self::new();
                }
            }
        };
        match Self::decode_from_plaintext_outcome(&plaintext) {
            CacheDecode::Loaded(cache) => {
                let total_entries: usize = cache.sections.values().map(|s| s.entries.len()).sum();
                info!(
                    sections = cache.sections.len(),
                    entries = total_entries,
                    "file cache loaded from disk"
                );
                for (key, section) in &cache.sections {
                    info!(
                        key,
                        anchor = %hex::encode(&section.anchor_snapshot_id.0[..8]),
                        entries = section.entries.len(),
                        "file cache section"
                    );
                }
                cache
            }
            CacheDecode::Rejected { reason } => {
                info!(
                    path = %path.display(),
                    plaintext_bytes = plaintext.len(),
                    reason,
                    "file cache rejected, starting fresh"
                );
                Self::new()
            }
            CacheDecode::Malformed { error } => {
                warn!(
                    path = %path.display(),
                    plaintext_bytes = plaintext.len(),
                    error,
                    "file cache: failed to deserialize (corrupt or legacy format?), starting fresh"
                );
                Self::new()
            }
        }
    }

    /// Save the file cache to local disk.
    pub fn save(
        &self,
        repo_id: &[u8],
        crypto: &dyn CryptoEngine,
        cache_dir_override: Option<&Path>,
    ) -> Result<()> {
        let Some(path) = Self::cache_path(repo_id, cache_dir_override) else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let estimated = self.len().saturating_mul(120);
        let packed = pack_object_streaming_with_context(
            ObjectType::FileCache,
            b"filecache",
            estimated,
            crypto,
            |buf| Ok(rmp_serde::encode::write(buf, self)?),
        )?;
        debug!(
            entries = self.len(),
            sections = self.sections.len(),
            estimated_bytes = estimated,
            actual_bytes = packed.len(),
            "file cache serialized"
        );
        atomic_write(&path, &packed)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Parent reuse index — runtime-only cold-start fallback
// ---------------------------------------------------------------------------

use crate::snapshot::item::{Item, ItemType};

/// Runtime-only reuse index built from the latest matching snapshot.
/// Not persisted. Used as fallback when no valid local cache section exists.
pub struct ParentReuseIndex {
    entries: HashMap<PathHash, ParentEntry>,
}

struct ParentEntry {
    mtime_ns: i64,
    ctime_ns: i64,
    size: u64,
    chunk_refs: Arc<Vec<ChunkRef>>,
}

/// Root emission policy for a single parent-reuse source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentReusePolicy {
    /// Descendants are emitted relative to `abs_root`; no synthetic root entry
    /// is present in the snapshot.
    SkipRoot,
    /// Snapshot paths are prefixed with `basename`; when the item path equals
    /// `basename` exactly, the "remainder" is empty (file source).
    EmitRoot { basename: String },
}

/// One root plus its policy, used to invert snapshot item paths back into
/// filesystem absolute paths at parent-reuse time.
#[derive(Debug, Clone)]
pub struct ParentReuseRoot {
    pub abs_root: String,
    pub policy: ParentReusePolicy,
}

impl ParentReuseRoot {
    fn invert(&self, item_path: &str) -> Option<PathBuf> {
        match &self.policy {
            ParentReusePolicy::SkipRoot => Some(Path::new(&self.abs_root).join(item_path)),
            ParentReusePolicy::EmitRoot { basename } => {
                if item_path == basename {
                    Some(PathBuf::from(&self.abs_root))
                } else {
                    item_path
                        .strip_prefix(basename.as_str())
                        .and_then(|r| r.strip_prefix('/'))
                        .map(|rest| Path::new(&self.abs_root).join(rest))
                }
            }
        }
    }
}

/// Incremental builder for `ParentReuseIndex`.
///
/// Fed items one at a time inside a streaming callback. Call `finish()` to
/// obtain the index if the legacy gate was never tripped.
pub struct ParentReuseBuilder {
    entries: HashMap<PathHash, ParentEntry>,
    roots: Vec<ParentReuseRoot>,
    /// Set to true when a filesystem file lacks ctime (legacy gate).
    legacy_abort: bool,
}

impl ParentReuseBuilder {
    /// Create a builder from a list of `ParentReuseRoot`s. Each root carries
    /// the canonical filesystem root plus its emission policy, so inversion
    /// is uniform across SkipRoot / EmitRoot / EmitRoot-with-empty-remainder
    /// cases.
    pub fn new(roots: Vec<ParentReuseRoot>) -> Self {
        Self {
            entries: HashMap::new(),
            roots,
            legacy_abort: false,
        }
    }

    /// Feed a single item. Takes ownership to avoid cloning chunk_refs.
    /// Returns `false` if the legacy gate was tripped (caller may stop early).
    pub fn push(&mut self, item: Item) -> bool {
        if self.legacy_abort {
            return false;
        }
        if item.entry_type != ItemType::RegularFile {
            return true;
        }
        if item.path.starts_with("vykar-dumps/") {
            return true;
        }
        let Some(ctime_ns) = item.ctime else {
            self.legacy_abort = true;
            return false;
        };

        let abs_path = reconstruct_abs_path(&item.path, &self.roots);
        self.entries.insert(
            hash_path(&abs_path),
            ParentEntry {
                mtime_ns: item.mtime,
                ctime_ns,
                size: item.size,
                chunk_refs: Arc::new(item.chunks),
            },
        );
        true
    }

    /// Consume the builder and return the index, or `None` if the legacy gate
    /// was tripped.
    pub fn finish(self) -> Option<ParentReuseIndex> {
        if self.legacy_abort {
            None
        } else {
            Some(ParentReuseIndex {
                entries: self.entries,
            })
        }
    }
}

impl ParentReuseIndex {
    /// Look up a file in the parent index. Matches on (path, size, mtime, ctime).
    /// No device/inode check (not available in snapshots).
    pub fn lookup(
        &self,
        abs_path: &str,
        size: u64,
        mtime_ns: i64,
        ctime_ns: i64,
    ) -> Option<Arc<Vec<ChunkRef>>> {
        let entry = self.entries.get(&hash_path(abs_path))?;
        if entry.size == size && entry.mtime_ns == mtime_ns && entry.ctime_ns == ctime_ns {
            Some(Arc::clone(&entry.chunk_refs))
        } else {
            None
        }
    }
}

/// Reconstruct the absolute path that the walker will use for cache lookups,
/// given a snapshot item path and a list of `ParentReuseRoot`s. Returns the
/// first root that matches — duplicate basenames are rejected up front at
/// source resolution, so first-match is unambiguous for current snapshots.
///
/// Uses `Path::join` + `to_string_lossy` to produce the same string form
/// as the walker's `abs_path.to_string_lossy()`.
fn reconstruct_abs_path(item_path: &str, roots: &[ParentReuseRoot]) -> String {
    for root in roots {
        if let Some(abs) = root.invert(item_path) {
            return abs.to_string_lossy().to_string();
        }
    }
    item_path.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use vykar_types::chunk_id::ChunkId;

    /// Build an absolute path string using OS-native separators, matching what
    /// `Path::join` (and therefore `reconstruct_abs_path`) produces.
    fn native_join(root: &str, rel: &str) -> String {
        Path::new(root).join(rel).to_string_lossy().to_string()
    }

    fn sample_chunk_refs() -> Arc<Vec<ChunkRef>> {
        Arc::new(vec![ChunkRef {
            id: ChunkId([0xAA; 32]),
            size: 1024,
            csize: 512,
        }])
    }

    /// Raw Vec variant for constructing `Item` structs in tests.
    fn sample_chunk_refs_vec() -> Vec<ChunkRef> {
        vec![ChunkRef {
            id: ChunkId([0xAA; 32]),
            size: 1024,
            csize: 512,
        }]
    }

    /// Helper to build a Vec<String> from path literals.
    fn roots(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn section_based_insert_and_lookup() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 4096);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn lookup_requires_active_section() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        // Activate a different path — should not find the entry.
        assert!(!cache.activate_for_walk_roots(&roots(&["/other"])));
        let result = cache.lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 4096);
        assert!(result.is_none());

        // Switch back — should find it again.
        assert!(cache.activate_for_walk_roots(&roots(&["/tmp"])));
        let result = cache.lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 4096);
        assert!(result.is_some());
    }

    #[test]
    fn lookup_miss_wrong_path() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/other.txt", 1, 1000, 1234567890, 1234567890, 4096);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_miss_changed_mtime() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 1, 1000, 9999999999, 1234567890, 4096);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_miss_changed_ctime() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 1, 1000, 1234567890, 9999999999, 4096);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_miss_changed_size() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 8192);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_miss_changed_inode() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 1, 2000, 1234567890, 1234567890, 4096);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_miss_changed_device() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 2, 1000, 1234567890, 1234567890, 4096);
        assert!(result.is_none());
    }

    #[test]
    fn insert_overwrites_existing() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            9999999999,
            9999999999,
            8192,
            Arc::new(vec![]),
        );

        assert_eq!(cache.len(), 1);
        assert!(cache
            .lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 4096)
            .is_none());
        assert!(cache
            .lookup("/tmp/test.txt", 1, 1000, 9999999999, 9999999999, 8192)
            .is_some());
    }

    #[test]
    fn empty_cache() {
        let cache = FileCache::new();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn independent_sections() {
        let mut cache = FileCache::new();

        cache.begin_sections(&roots(&["/a"]), &[0]);
        cache.insert("/a/file.txt", 1, 100, 111, 111, 4096, sample_chunk_refs());

        cache.begin_sections(&roots(&["/b"]), &[0]);
        cache.insert("/b/file.txt", 1, 200, 222, 222, 8192, sample_chunk_refs());

        // Looking up in /b should not find /a's entry.
        assert!(cache
            .lookup("/a/file.txt", 1, 100, 111, 111, 4096)
            .is_none());
        assert!(cache
            .lookup("/b/file.txt", 1, 200, 222, 222, 8192)
            .is_some());

        // Switch to /a.
        assert!(cache.activate_for_walk_roots(&roots(&["/a"])));
        assert!(cache
            .lookup("/a/file.txt", 1, 100, 111, 111, 4096)
            .is_some());
        assert!(cache
            .lookup("/b/file.txt", 1, 200, 222, 222, 8192)
            .is_none());

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn invalidate_missing_snapshots() {
        let mut cache = FileCache::new();
        let id_a = SnapshotId([0xAA; 32]);
        let id_b = SnapshotId([0xBB; 32]);

        let key_a = "/a".to_string();
        let key_b = "/b".to_string();
        cache.sections.insert(
            key_a.clone(),
            CacheSection {
                anchor_snapshot_id: id_a,
                entries: HashMap::new(),
            },
        );
        cache.sections.insert(
            key_b.clone(),
            CacheSection {
                anchor_snapshot_id: id_b,
                entries: HashMap::new(),
            },
        );

        // Only id_a exists — id_b's section should be invalidated.
        let removed = cache.invalidate_missing_snapshots(&|id| *id == id_a);
        assert_eq!(removed, 1);
        assert!(cache.sections.contains_key(&key_a));
        assert!(!cache.sections.contains_key(&key_b));
    }

    #[test]
    fn activate_for_walk_roots_finds_sections() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/data", "/config"]), &[0, 0]);
        cache.finalize_sections(SnapshotId([0x11; 32]));

        // Both paths activate.
        assert!(cache.activate_for_walk_roots(&roots(&["/data", "/config"])));
        // Subset activates (all requested roots found).
        assert!(cache.activate_for_walk_roots(&roots(&["/data"])));
        // Superset returns false (not all found), but partial activation works.
        assert!(!cache.activate_for_walk_roots(&roots(&["/data", "/config", "/other"])));
        // Unrelated paths do not match.
        assert!(!cache.activate_for_walk_roots(&roots(&["/other"])));
    }

    #[test]
    fn activate_for_walk_roots_is_label_independent() {
        // Sections are keyed by canonicalized paths, not label.
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/data"]), &[0]);
        cache.insert("/data/a.txt", 1, 1, 1, 1, 100, sample_chunk_refs());
        cache.finalize_sections(SnapshotId([0x22; 32]));

        // Activate by the same path (regardless of what label was used).
        assert!(cache.activate_for_walk_roots(&roots(&["/data"])));
        assert!(cache.lookup("/data/a.txt", 1, 1, 1, 1, 100).is_some());
    }

    #[test]
    fn merge_section_replaces_only_target() {
        let mut cache = FileCache::new();
        let id_a = SnapshotId([0xAA; 32]);
        let id_b = SnapshotId([0xBB; 32]);

        let key_a = "/a".to_string();
        let key_b = "/b".to_string();
        cache.sections.insert(
            key_a.clone(),
            CacheSection {
                anchor_snapshot_id: id_a,
                entries: HashMap::new(),
            },
        );

        let new_section = CacheSection {
            anchor_snapshot_id: id_b,
            entries: HashMap::new(),
        };
        cache.merge_section(&key_b, new_section);

        assert_eq!(cache.sections.len(), 2);
        assert!(cache.sections.contains_key(&key_a));
        assert!(cache.sections.contains_key(&key_b));
    }

    #[test]
    fn merge_section_overwrites_same_paths() {
        let mut cache = FileCache::new();
        let id_old = SnapshotId([0xAA; 32]);
        let id_new = SnapshotId([0xBB; 32]);
        let key = "/data".to_string();

        cache.sections.insert(
            key.clone(),
            CacheSection {
                anchor_snapshot_id: id_old,
                entries: HashMap::new(),
            },
        );

        let new_section = CacheSection {
            anchor_snapshot_id: id_new,
            entries: HashMap::new(),
        };
        cache.merge_section(&key, new_section);

        // Only one section — natural key overwrite, no duplicates.
        assert_eq!(cache.sections.len(), 1);
        assert_eq!(cache.sections[&key].anchor_snapshot_id, id_new);
    }

    #[test]
    fn take_active_sections() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/src"]), &[0]);
        cache.insert("/src/a.txt", 1, 1, 1, 1, 100, sample_chunk_refs());
        cache.finalize_sections(SnapshotId([0x42; 32]));

        let taken = cache.take_active_sections();
        assert_eq!(taken.len(), 1);
        let (key, section) = &taken[0];
        assert_eq!(key, "/src");
        assert_eq!(section.entries.len(), 1);
        assert_eq!(section.anchor_snapshot_id, SnapshotId([0x42; 32]));
        assert!(cache.sections.is_empty());
    }

    #[test]
    fn take_active_sections_multi() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/a", "/b"]), &[0, 0]);
        cache.insert("/a/f.txt", 1, 1, 1, 1, 100, sample_chunk_refs());
        cache.insert("/b/g.txt", 1, 2, 2, 2, 200, sample_chunk_refs());
        cache.finalize_sections(SnapshotId([0x42; 32]));

        let taken = cache.take_active_sections();
        assert_eq!(taken.len(), 2);
        assert!(cache.sections.is_empty());
    }

    #[test]
    fn contains_checks_active_section() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        cache.insert("/tmp/a.txt", 1, 1, 1, 1, 100, sample_chunk_refs());

        assert!(cache.contains("/tmp/a.txt"));
        assert!(!cache.contains("/tmp/b.txt"));
    }

    #[test]
    fn round_trip_serialization() {
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/tmp"]), &[0]);
        for i in 0..10 {
            cache.insert(
                &format!("/tmp/file_{i}.txt"),
                1,
                1000 + i as u64,
                1234567890,
                1234567890,
                4096,
                sample_chunk_refs(),
            );
        }
        cache.finalize_sections(SnapshotId([0xDD; 32]));

        let key = "/tmp".to_string();
        let plaintext = rmp_serde::to_vec(&cache).unwrap();
        let decoded = FileCache::decode_from_plaintext(&plaintext).unwrap();
        assert_eq!(decoded.sections.len(), 1);
        assert!(decoded.sections.contains_key(&key));
        let section = &decoded.sections[&key];
        assert_eq!(section.entries.len(), 10);
        assert_eq!(section.anchor_snapshot_id, SnapshotId([0xDD; 32]));
    }

    #[test]
    fn old_format_returns_empty_cache() {
        // Old flat-map format — should gracefully return empty.
        #[derive(Serialize)]
        struct OldFileCache {
            entries: HashMap<String, FileCacheEntry>,
        }
        let mut entries = HashMap::new();
        entries.insert(
            "/tmp/old.txt".to_string(),
            FileCacheEntry {
                device: 1,
                inode: 1,
                mtime_ns: 1,
                ctime_ns: 1,
                size: 100,
                chunk_refs: sample_chunk_refs(),
            },
        );
        let old = OldFileCache { entries };
        let plaintext = rmp_serde::to_vec(&old).unwrap();
        let decoded = FileCache::decode_from_plaintext(&plaintext).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn bogus_data_returns_empty_cache() {
        let garbage = vec![0xFF, 0xFE, 0xFD];
        let result = FileCache::decode_from_plaintext(&garbage);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn repo_cache_dir_default() {
        let repo_id = [0xABu8; 32];
        let dir = super::repo_cache_dir(&repo_id, None);
        assert!(dir.is_some());
        let d = dir.unwrap();
        assert!(d.to_string_lossy().contains("vykar"));
        assert!(d.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    #[test]
    fn repo_cache_dir_with_override() {
        let repo_id = [0xCDu8; 32];
        let temp = tempfile::tempdir().unwrap();
        let override_root = temp.path().join("vykar-cache");
        let dir = super::repo_cache_dir(&repo_id, Some(override_root.as_path())).unwrap();
        assert!(dir.starts_with(&override_root));
        assert!(dir.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    /// Helper: build a ParentReuseIndex from a Vec<Item> using the builder.
    /// Accepts raw source paths and derives ParentReuseRoot (basename =
    /// last component of the path).
    fn build_parent_index(
        items: Vec<Item>,
        source_paths: &[String],
        multi_path: bool,
    ) -> Option<ParentReuseIndex> {
        let parent_roots: Vec<ParentReuseRoot> = source_paths
            .iter()
            .map(|sp| {
                let p = Path::new(sp);
                let basename = p
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| sp.clone());
                let policy = if multi_path {
                    ParentReusePolicy::EmitRoot { basename }
                } else {
                    ParentReusePolicy::SkipRoot
                };
                ParentReuseRoot {
                    abs_root: sp.clone(),
                    policy,
                }
            })
            .collect();
        let mut builder = ParentReuseBuilder::new(parent_roots);
        for item in items {
            builder.push(item);
        }
        builder.finish()
    }

    #[test]
    fn parent_reuse_index_basic() {
        let items = vec![
            Item {
                path: "a.txt".into(),
                entry_type: ItemType::RegularFile,
                mode: 0o644,
                uid: 0,
                gid: 0,
                user: None,
                group: None,
                mtime: 1000,
                atime: None,
                ctime: Some(2000),
                size: 4096,
                chunks: sample_chunk_refs_vec(),
                link_target: None,
                xattrs: None,
            },
            Item {
                path: "dir".into(),
                entry_type: ItemType::Directory,
                mode: 0o755,
                uid: 0,
                gid: 0,
                user: None,
                group: None,
                mtime: 0,
                atime: None,
                ctime: None,
                size: 0,
                chunks: Vec::new(),
                link_target: None,
                xattrs: None,
            },
        ];

        let idx = build_parent_index(items, &["/src".into()], false).unwrap();
        let path = native_join("/src", "a.txt");
        // Should find the file
        let hit = idx.lookup(&path, 4096, 1000, 2000);
        assert!(hit.is_some());
        assert_eq!(hit.unwrap().len(), 1);

        // Wrong mtime — miss
        assert!(idx.lookup(&path, 4096, 9999, 2000).is_none());
        // Wrong ctime — miss
        assert!(idx.lookup(&path, 4096, 1000, 9999).is_none());
    }

    #[test]
    fn parent_reuse_index_legacy_gate() {
        let items = vec![Item {
            path: "a.txt".into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 1000,
            atime: None,
            ctime: None, // No ctime — legacy
            size: 4096,
            chunks: sample_chunk_refs_vec(),
            link_target: None,
            xattrs: None,
        }];

        let result = build_parent_index(items, &["/src".into()], false);
        assert!(result.is_none(), "legacy gate should prevent parent index");
    }

    #[test]
    fn parent_reuse_index_ignores_dumps() {
        let items = vec![
            Item {
                path: "vykar-dumps/pg_dump".into(),
                entry_type: ItemType::RegularFile,
                mode: 0o644,
                uid: 0,
                gid: 0,
                user: None,
                group: None,
                mtime: 1000,
                atime: None,
                ctime: None, // Dumps have no ctime — should not trip legacy gate
                size: 4096,
                chunks: sample_chunk_refs_vec(),
                link_target: None,
                xattrs: None,
            },
            Item {
                path: "real.txt".into(),
                entry_type: ItemType::RegularFile,
                mode: 0o644,
                uid: 0,
                gid: 0,
                user: None,
                group: None,
                mtime: 2000,
                atime: None,
                ctime: Some(3000),
                size: 8192,
                chunks: sample_chunk_refs_vec(),
                link_target: None,
                xattrs: None,
            },
        ];

        let idx = build_parent_index(items, &["/src".into()], false).unwrap();
        // Dump item should not be indexed
        assert!(idx
            .lookup(&native_join("/src", "vykar-dumps/pg_dump"), 4096, 1000, 0)
            .is_none());
        // Real file should be indexed
        assert!(idx
            .lookup(&native_join("/src", "real.txt"), 8192, 2000, 3000)
            .is_some());
    }

    #[test]
    fn parent_reuse_index_multi_path() {
        let items = vec![Item {
            path: "home/a.txt".into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 1000,
            atime: None,
            ctime: Some(2000),
            size: 4096,
            chunks: sample_chunk_refs_vec(),
            link_target: None,
            xattrs: None,
        }];

        let idx =
            build_parent_index(items, &["/mnt/home".into(), "/mnt/data".into()], true).unwrap();
        // "home/a.txt" → matches source_path "/mnt/home" (basename "home"), reconstructs to "/mnt/home/a.txt"
        assert!(idx
            .lookup(&native_join("/mnt/home", "a.txt"), 4096, 1000, 2000)
            .is_some());
    }

    fn skiproot(abs_root: &str) -> ParentReuseRoot {
        ParentReuseRoot {
            abs_root: abs_root.to_string(),
            policy: ParentReusePolicy::SkipRoot,
        }
    }

    fn emitroot(abs_root: &str, basename: &str) -> ParentReuseRoot {
        ParentReuseRoot {
            abs_root: abs_root.to_string(),
            policy: ParentReusePolicy::EmitRoot {
                basename: basename.to_string(),
            },
        }
    }

    #[test]
    fn reconstruct_abs_path_single() {
        let roots = vec![skiproot("/data")];
        let p = reconstruct_abs_path("dir/file.txt", &roots);
        assert_eq!(p, native_join("/data", "dir/file.txt"));
    }

    #[test]
    fn reconstruct_abs_path_single_trailing_slash() {
        // Trailing slash in walk_root must not produce double-slash.
        let roots = vec![skiproot("/data/")];
        let p = reconstruct_abs_path("dir/file.txt", &roots);
        assert_eq!(p, native_join("/data/", "dir/file.txt"));
    }

    #[test]
    fn reconstruct_abs_path_multi() {
        let roots = vec![emitroot("/mnt/data", "data"), emitroot("/mnt/home", "home")];
        let p = reconstruct_abs_path("data/sub/file.txt", &roots);
        assert_eq!(p, native_join("/mnt/data", "sub/file.txt"));
    }

    #[test]
    fn reconstruct_abs_path_multi_trailing_slash() {
        let roots = vec![
            emitroot("/mnt/data/", "data"),
            emitroot("/mnt/home/", "home"),
        ];
        let p = reconstruct_abs_path("data/sub/file.txt", &roots);
        assert_eq!(p, native_join("/mnt/data/", "sub/file.txt"));
    }

    #[test]
    fn reconstruct_abs_path_symlink_multi() {
        // Symlink: docs -> /mnt/real-docs. The snapshot uses "docs" as prefix
        // (original basename), but the walk root is the canonicalized target.
        let roots = vec![
            emitroot("/mnt/real-docs", "docs"),
            emitroot("/etc", "config"),
        ];
        let p = reconstruct_abs_path("docs/readme.txt", &roots);
        assert_eq!(p, native_join("/mnt/real-docs", "readme.txt"));
    }

    #[test]
    fn parent_reuse_file_source_inversion() {
        // File source: EmitRoot with empty remainder. item_path == basename
        // → abs_root is returned as-is (no trailing component).
        let root = emitroot("/data/notes.txt", "notes.txt");
        let abs = root.invert("notes.txt").unwrap();
        assert_eq!(abs, PathBuf::from("/data/notes.txt"));

        // And longer paths don't falsely match.
        assert!(root.invert("notes.txt.bak").is_none());
        assert!(root.invert("notesXtxt").is_none());
    }

    #[test]
    fn parent_reuse_skiproot_inversion() {
        let root = skiproot("/data");
        let abs = root.invert("dir/file.txt").unwrap();
        assert_eq!(abs.to_string_lossy(), native_join("/data", "dir/file.txt"));
    }

    #[test]
    fn parent_reuse_multi_root_dispatches_correctly() {
        let roots = vec![emitroot("/mnt/data", "data"), emitroot("/mnt/home", "home")];
        let d = reconstruct_abs_path("data/file.txt", &roots);
        assert_eq!(d, native_join("/mnt/data", "file.txt"));
        let h = reconstruct_abs_path("home/file.txt", &roots);
        assert_eq!(h, native_join("/mnt/home", "file.txt"));
    }

    #[test]
    fn parent_reuse_unknown_path_returns_none() {
        let roots = vec![emitroot("/mnt/data", "data")];
        // A path whose prefix matches no root falls back to item_path as-is.
        let out = reconstruct_abs_path("other/file.txt", &roots);
        assert_eq!(out, "other/file.txt");
    }

    // ── Per-path section tests ──────────────────────────────────────────

    #[test]
    fn add_path_preserves_existing_sections() {
        // Build cache with ["/a", "/b"], persist, then activate with ["/a", "/b", "/c"].
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/a", "/b"]), &[0, 0]);
        cache.insert("/a/f.txt", 1, 1, 1, 1, 100, sample_chunk_refs());
        cache.insert("/b/g.txt", 1, 2, 2, 2, 200, sample_chunk_refs());
        cache.finalize_sections(SnapshotId([0x11; 32]));

        // Simulate persistence round-trip.
        let plaintext = rmp_serde::to_vec(&cache).unwrap();
        let mut cache = FileCache::decode_from_plaintext(&plaintext).unwrap();

        // Activate with a superset: /a and /b should activate, /c should not.
        let all_found = cache.activate_for_walk_roots(&roots(&["/a", "/b", "/c"]));
        assert!(!all_found, "not all found because /c is new");

        // /a and /b lookups still work.
        assert!(cache.lookup("/a/f.txt", 1, 1, 1, 1, 100).is_some());
        assert!(cache.lookup("/b/g.txt", 1, 2, 2, 2, 200).is_some());
        // /c has no section — lookups return None.
        assert!(cache.lookup("/c/h.txt", 1, 3, 3, 3, 300).is_none());
    }

    #[test]
    fn remove_path_leaves_remaining_sections() {
        // Build cache with ["/a", "/b"], persist, then activate with ["/a"] only.
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/a", "/b"]), &[0, 0]);
        cache.insert("/a/f.txt", 1, 1, 1, 1, 100, sample_chunk_refs());
        cache.insert("/b/g.txt", 1, 2, 2, 2, 200, sample_chunk_refs());
        cache.finalize_sections(SnapshotId([0x11; 32]));

        let plaintext = rmp_serde::to_vec(&cache).unwrap();
        let mut cache = FileCache::decode_from_plaintext(&plaintext).unwrap();

        // Activate with subset ["/a"].
        let all_found = cache.activate_for_walk_roots(&roots(&["/a"]));
        assert!(all_found, "all requested roots found");

        // /a lookups work.
        assert!(cache.lookup("/a/f.txt", 1, 1, 1, 1, 100).is_some());
        // /b is not active — lookups miss (but section still in persistent cache).
        assert!(cache.lookup("/b/g.txt", 1, 2, 2, 2, 200).is_none());
        assert_eq!(
            cache.sections.len(),
            2,
            "/b section still in persistent cache"
        );
    }

    #[test]
    fn longest_prefix_match_routing() {
        // Active keys ["/data", "/data/sub"]. Lookup routes to longest match.
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/data", "/data/sub"]), &[0, 0]);

        let refs_a = Arc::new(vec![ChunkRef {
            id: ChunkId([0xAA; 32]),
            size: 100,
            csize: 50,
        }]);
        let refs_b = Arc::new(vec![ChunkRef {
            id: ChunkId([0xBB; 32]),
            size: 200,
            csize: 100,
        }]);

        cache.insert("/data/sub/foo.txt", 1, 1, 1, 1, 100, Arc::clone(&refs_a));
        cache.insert("/data/other.txt", 1, 2, 2, 2, 200, Arc::clone(&refs_b));

        // Lookup routes to longest-prefix section.
        let hit = cache.lookup("/data/sub/foo.txt", 1, 1, 1, 1, 100).unwrap();
        assert_eq!(hit[0].id, ChunkId([0xAA; 32]));
        let hit = cache.lookup("/data/other.txt", 1, 2, 2, 2, 200).unwrap();
        assert_eq!(hit[0].id, ChunkId([0xBB; 32]));

        // Insert wrote to ALL matching sections, so /data/sub/foo.txt is also
        // in the /data section. Verify by switching to /data only.
        assert!(cache.activate_for_walk_roots(&roots(&["/data"])));
        let hit = cache.lookup("/data/sub/foo.txt", 1, 1, 1, 1, 100).unwrap();
        assert_eq!(hit[0].id, ChunkId([0xAA; 32]));
    }

    #[test]
    fn overlapping_roots_remove_specific() {
        // Build cache with ["/data", "/data/sub"], persist, then activate
        // with just ["/data"]. Files under /data/sub/ should still be found
        // in the /data section.
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/data", "/data/sub"]), &[0, 0]);
        cache.insert("/data/sub/foo.txt", 1, 1, 1, 1, 100, sample_chunk_refs());
        cache.insert("/data/sub/bar.txt", 1, 2, 2, 2, 200, sample_chunk_refs());
        cache.insert("/data/top.txt", 1, 3, 3, 3, 300, sample_chunk_refs());
        cache.finalize_sections(SnapshotId([0x11; 32]));

        // Persistence round-trip.
        let plaintext = rmp_serde::to_vec(&cache).unwrap();
        let mut cache = FileCache::decode_from_plaintext(&plaintext).unwrap();

        // Activate with just /data (removed /data/sub from config).
        assert!(cache.activate_for_walk_roots(&roots(&["/data"])));

        // All files still hit via the /data section.
        assert!(cache.lookup("/data/sub/foo.txt", 1, 1, 1, 1, 100).is_some());
        assert!(cache.lookup("/data/sub/bar.txt", 1, 2, 2, 2, 200).is_some());
        assert!(cache.lookup("/data/top.txt", 1, 3, 3, 3, 300).is_some());
    }

    #[test]
    fn overlapping_roots_add_specific() {
        // Build cache with ["/data"] only, persist, then activate with
        // ["/data", "/data/sub"]. Files under /data/sub/ should still hit
        // via the /data section (partial activation — /data/sub is new).
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/data"]), &[0]);
        cache.insert("/data/sub/foo.txt", 1, 1, 1, 1, 100, sample_chunk_refs());
        cache.insert("/data/top.txt", 1, 2, 2, 2, 200, sample_chunk_refs());
        cache.finalize_sections(SnapshotId([0x11; 32]));

        let plaintext = rmp_serde::to_vec(&cache).unwrap();
        let mut cache = FileCache::decode_from_plaintext(&plaintext).unwrap();

        // Activate with superset ["/data", "/data/sub"].
        // Returns false because /data/sub section doesn't exist.
        let all_found = cache.activate_for_walk_roots(&roots(&["/data", "/data/sub"]));
        assert!(!all_found);

        // /data is partially activated; lookups for /data/sub/foo.txt still
        // route to /data (the only active key) and hit.
        assert!(cache.lookup("/data/sub/foo.txt", 1, 1, 1, 1, 100).is_some());
        assert!(cache.lookup("/data/top.txt", 1, 2, 2, 2, 200).is_some());
    }

    #[test]
    fn root_source_matches_all() {
        // Active key ["/"] — should match any absolute path.
        let mut cache = FileCache::new();
        // Root path: basename is "", use "/" as walk_root.
        cache.begin_sections(&roots(&["/"]), &[0]);
        cache.insert("/etc/foo.txt", 1, 1, 1, 1, 100, sample_chunk_refs());

        assert!(cache.lookup("/etc/foo.txt", 1, 1, 1, 1, 100).is_some());
        assert!(cache.lookup("/var/bar.txt", 1, 1, 1, 1, 100).is_none());
    }

    #[test]
    fn begin_sections_applies_capacity_hints() {
        // Each root gets its own pre-sized section from the capacity hint.
        let mut cache = FileCache::new();
        cache.begin_sections(&roots(&["/data", "/data/sub"]), &[100, 50]);

        // Verify capacity was applied before any inserts.
        assert!(cache.sections["/data"].entries.capacity() >= 100);
        assert!(cache.sections["/data/sub"].entries.capacity() >= 50);

        // Zero hint produces a minimal allocation (same as cold start).
        let mut cache2 = FileCache::new();
        cache2.begin_sections(&roots(&["/x"]), &[0]);
        assert_eq!(cache2.sections["/x"].entries.capacity(), 0);
    }
}
