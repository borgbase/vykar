use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::Mmap;
use tracing::{debug, warn};
use xorf::{Filter, Xor8};

use crate::index::{ChunkIndex, ChunkIndexEntry, IndexDelta};
use crate::repo::file_cache::repo_cache_dir;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;
use vykar_types::pack_id::PackId;

/// Magic bytes at the start of the dedup cache file.
// Wire-format constant — DO NOT rename (backward compatibility)
const MAGIC: &[u8; 8] = b"VGDEDUP\0";

/// Current format version.
const VERSION: u32 = 1;

/// Size of the fixed header in bytes.
const HEADER_SIZE: usize = 28;

/// Size of each entry: 32-byte ChunkId + 4-byte stored_size.
const ENTRY_SIZE: usize = 36;

// ---------------------------------------------------------------------------
// Path helper
// ---------------------------------------------------------------------------

/// Return the local filesystem path for the dedup cache file.
/// `~/.cache/vykar/<repo_id_hex>/dedup_cache` (same directory as file cache).
pub fn dedup_cache_path(repo_id: &[u8], cache_dir: Option<&Path>) -> Option<PathBuf> {
    repo_cache_dir(repo_id, cache_dir).map(|d| d.join("dedup_cache"))
}

// ---------------------------------------------------------------------------
// Cache writer
// ---------------------------------------------------------------------------

/// Build the dedup cache binary file from the full chunk index.
/// Writes atomically via temp-file + rename.
pub fn build_dedup_cache(
    index: &ChunkIndex,
    generation: u64,
    repo_id: &[u8],
    cache_dir: Option<&Path>,
) -> Result<()> {
    let Some(path) = dedup_cache_path(repo_id, cache_dir) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    build_dedup_cache_to_path(index, generation, &path)
}

/// Build the dedup cache to an explicit path (used by tests).
pub fn build_dedup_cache_to_path(index: &ChunkIndex, generation: u64, path: &Path) -> Result<()> {
    // Collect and sort entries by ChunkId bytes.
    let mut entries: Vec<(ChunkId, u32)> = index
        .iter()
        .map(|(id, entry)| (*id, entry.stored_size))
        .collect();
    entries.sort_unstable_by(|a, b| a.0 .0.cmp(&b.0 .0));

    let entry_count = entries.len() as u32;

    // Stream directly to a temp file via BufWriter to avoid a second
    // in-memory copy of the entire output.
    let tmp_path = path.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let mut w = BufWriter::new(file);

    // Header
    w.write_all(MAGIC)?;
    w.write_all(&VERSION.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&entry_count.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved

    // Entries
    for (chunk_id, stored_size) in &entries {
        w.write_all(&chunk_id.0)?;
        w.write_all(&stored_size.to_le_bytes())?;
    }

    w.flush()?;
    drop(w);

    // Atomic rename into place.
    std::fs::rename(&tmp_path, path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote dedup cache"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// mmap'd cache reader
// ---------------------------------------------------------------------------

/// Memory-mapped reader over the sorted dedup cache binary file.
/// Lookups use binary search over fixed-size 36-byte entries.
pub struct MmapDedupCache {
    mmap: Mmap,
    entry_count: u32,
    index_generation: u64,
}

impl MmapDedupCache {
    /// Open and validate the dedup cache file.
    /// Returns `None` on any mismatch (missing file, wrong magic/version/generation,
    /// unexpected file size) — the caller should fall back to the HashMap path.
    pub fn open(
        repo_id: &[u8],
        expected_generation: u64,
        cache_dir: Option<&Path>,
    ) -> Option<Self> {
        // Generation 0 means "no cache ever written".
        if expected_generation == 0 {
            return None;
        }

        let path = dedup_cache_path(repo_id, cache_dir)?;
        Self::open_path(&path, expected_generation)
    }

    /// Open and validate a dedup cache file at an explicit path (used by tests).
    pub fn open_path(path: &Path, expected_generation: u64) -> Option<Self> {
        if expected_generation == 0 {
            return None;
        }

        let file = std::fs::File::open(path).ok()?;

        // SAFETY: we only read the file, and the file is written atomically
        // (temp + rename) so it's always in a consistent state.
        let mmap = unsafe { Mmap::map(&file) }.ok()?;

        if mmap.len() < HEADER_SIZE {
            debug!("dedup cache: file too small for header");
            return None;
        }

        // Validate magic
        if &mmap[0..8] != MAGIC {
            debug!("dedup cache: bad magic");
            return None;
        }

        // Validate version
        let version = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        if version != VERSION {
            debug!(version, "dedup cache: unsupported version");
            return None;
        }

        // Validate generation
        let index_generation = u64::from_le_bytes(mmap[12..20].try_into().unwrap());
        if index_generation != expected_generation {
            debug!(
                cache_gen = index_generation,
                expected_gen = expected_generation,
                "dedup cache: generation mismatch"
            );
            return None;
        }

        let entry_count = u32::from_le_bytes(mmap[20..24].try_into().unwrap());

        // Validate file size
        let expected_size = HEADER_SIZE + (entry_count as usize) * ENTRY_SIZE;
        if mmap.len() != expected_size {
            debug!(
                actual = mmap.len(),
                expected = expected_size,
                "dedup cache: file size mismatch"
            );
            return None;
        }

        debug!(
            entries = entry_count,
            generation = index_generation,
            "opened dedup cache"
        );

        Some(Self {
            mmap,
            entry_count,
            index_generation,
        })
    }

    /// Look up a chunk ID using binary search. Returns the stored_size if found.
    pub fn get_stored_size(&self, chunk_id: &ChunkId) -> Option<u32> {
        if self.entry_count == 0 {
            return None;
        }

        let target = &chunk_id.0;
        let data = &self.mmap[HEADER_SIZE..];

        let mut lo: usize = 0;
        let mut hi: usize = self.entry_count as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = mid * ENTRY_SIZE;
            let entry_id = &data[offset..offset + 32];

            match entry_id.cmp(target.as_slice()) {
                std::cmp::Ordering::Equal => {
                    let size_offset = offset + 32;
                    let stored_size =
                        u32::from_le_bytes(data[size_offset..size_offset + 4].try_into().unwrap());
                    return Some(stored_size);
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }

        None
    }

    /// Return the index_generation from the cache header.
    pub fn generation(&self) -> u64 {
        self.index_generation
    }

    /// Return the number of entries in the cache.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Iterate over all chunk IDs as u64 keys (first 8 bytes, LE) for xor filter construction.
    fn iter_u64_keys(&self) -> impl Iterator<Item = u64> + '_ {
        let data = &self.mmap[HEADER_SIZE..];
        (0..self.entry_count as usize).map(move |i| {
            let offset = i * ENTRY_SIZE;
            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
        })
    }
}

// ---------------------------------------------------------------------------
// Xor filter helpers
// ---------------------------------------------------------------------------

/// Extract the first 8 bytes of a ChunkId as a little-endian u64.
/// BLAKE2b output has excellent entropy, so this is a high-quality hash key.
pub(crate) fn chunk_id_to_u64(id: &ChunkId) -> u64 {
    u64::from_le_bytes(id.0[..8].try_into().unwrap())
}

/// Build an Xor8 filter from pre-computed u64 keys.
/// Returns `None` if the slice is empty or construction fails.
pub(crate) fn build_xor_filter_from_keys(keys: &[u64]) -> Option<Xor8> {
    if keys.is_empty() {
        return None;
    }
    match std::panic::catch_unwind(|| Xor8::from(keys)) {
        Ok(filter) => {
            debug!(entries = keys.len(), "built xor filter");
            Some(filter)
        }
        Err(_) => {
            warn!("xor filter construction panicked");
            None
        }
    }
}

/// Build an Xor8 filter from the mmap'd cache entries.
/// Returns `None` if the cache is empty or construction fails.
fn build_xor_filter(cache: &MmapDedupCache) -> Option<Xor8> {
    let keys: Vec<u64> = cache.iter_u64_keys().collect();
    build_xor_filter_from_keys(&keys)
}

// ---------------------------------------------------------------------------
// TieredDedupIndex
// ---------------------------------------------------------------------------

/// Three-tier dedup index for memory-efficient backup.
///
/// Lookup order:
/// 1. `session_new` HashMap — chunks added during this backup session (tiny, O(1))
/// 2. Xor filter — probabilistic negative filter (~0.4% FPR, ~1.2 bytes/entry)
/// 3. mmap binary search — confirms filter hit, returns stored_size (OS-paged, near-zero RSS)
pub struct TieredDedupIndex {
    xor_filter: Option<Arc<Xor8>>,
    mmap_cache: MmapDedupCache,
    session_new: HashMap<ChunkId, u32>,
}

impl TieredDedupIndex {
    /// Create a new tiered index from an opened mmap cache.
    pub fn new(mmap_cache: MmapDedupCache) -> Self {
        let xor_filter = build_xor_filter(&mmap_cache).map(Arc::new);
        Self {
            xor_filter,
            mmap_cache,
            session_new: HashMap::new(),
        }
    }

    /// Return a shared reference to the pre-built xor filter (if any).
    pub(crate) fn xor_filter(&self) -> Option<Arc<Xor8>> {
        self.xor_filter.clone()
    }

    /// Check if a chunk exists in any tier.
    pub fn contains(&self, id: &ChunkId) -> bool {
        self.get_stored_size(id).is_some()
    }

    /// Look up a chunk's stored size across all tiers.
    pub fn get_stored_size(&self, id: &ChunkId) -> Option<u32> {
        // Tier 1: session-new chunks
        if let Some(&size) = self.session_new.get(id) {
            return Some(size);
        }

        // Tier 2: xor filter (probabilistic negative)
        if let Some(ref filter) = self.xor_filter {
            let key = chunk_id_to_u64(id);
            if !filter.contains(&key) {
                // Definite negative — skip mmap lookup.
                return None;
            }
        }

        // Tier 3: mmap binary search (confirms filter hit or used when no filter)
        self.mmap_cache.get_stored_size(id)
    }

    /// Look up stored size, skipping the xor filter tier.
    /// Caller already checked the xor filter (hit) — go straight to session_new → mmap.
    pub fn get_stored_size_skip_filter(&self, id: &ChunkId) -> Option<u32> {
        if let Some(&size) = self.session_new.get(id) {
            return Some(size);
        }
        self.mmap_cache.get_stored_size(id)
    }

    /// Look up stored size in session_new only.
    /// Caller's xor filter miss guarantees the chunk is not in the mmap cache.
    pub fn session_new_stored_size(&self, id: &ChunkId) -> Option<u32> {
        self.session_new.get(id).copied()
    }

    /// Insert a new chunk discovered during this backup session.
    pub fn insert(&mut self, id: ChunkId, stored_size: u32) {
        self.session_new.insert(id, stored_size);
    }

    /// Remove a session-local entry. The mmap and xor filter tiers are
    /// read-only; false positives from the xor filter are safe because the
    /// precise lookup in `session_new` will miss.
    pub fn remove(&mut self, id: &ChunkId) {
        self.session_new.remove(id);
    }

    /// Number of entries in the session-new HashMap.
    pub fn session_new_len(&self) -> usize {
        self.session_new.len()
    }
}

impl std::fmt::Debug for TieredDedupIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredDedupIndex")
            .field("has_xor_filter", &self.xor_filter.is_some())
            .field("mmap_entries", &self.mmap_cache.entry_count())
            .field("session_new", &self.session_new.len())
            .finish()
    }
}

// ===========================================================================
// Restore cache — separate mmap'd file with full pack location data
// ===========================================================================

/// Magic bytes at the start of the restore cache file.
const RESTORE_MAGIC: &[u8; 8] = b"VGIDX\0\0\0";

/// Current restore cache format version.
const RESTORE_VERSION: u32 = 1;

/// Size of the restore cache header in bytes.
const RESTORE_HEADER_SIZE: usize = 28;

/// Size of each restore cache entry:
/// 32-byte ChunkId + 4-byte stored_size + 32-byte PackId + 8-byte pack_offset.
const RESTORE_ENTRY_SIZE: usize = 76;

/// Return the local filesystem path for the restore cache file.
pub fn restore_cache_path(repo_id: &[u8], cache_dir: Option<&Path>) -> Option<PathBuf> {
    repo_cache_dir(repo_id, cache_dir).map(|d| d.join("restore_cache"))
}

/// Build the restore cache binary file from the full chunk index.
/// Writes atomically via temp-file + rename.
pub fn build_restore_cache(
    index: &ChunkIndex,
    generation: u64,
    repo_id: &[u8],
    cache_dir: Option<&Path>,
) -> Result<()> {
    let Some(path) = restore_cache_path(repo_id, cache_dir) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    build_restore_cache_to_path(index, generation, &path)
}

/// Build the restore cache to an explicit path (used by tests).
pub fn build_restore_cache_to_path(index: &ChunkIndex, generation: u64, path: &Path) -> Result<()> {
    // Collect and sort entries by ChunkId bytes.
    let mut entries: Vec<(ChunkId, u32, PackId, u64)> = index
        .iter()
        .map(|(id, entry)| (*id, entry.stored_size, entry.pack_id, entry.pack_offset))
        .collect();
    entries.sort_unstable_by(|a, b| a.0 .0.cmp(&b.0 .0));

    let entry_count = entries.len() as u32;

    let tmp_path = path.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let mut w = BufWriter::new(file);

    // Header
    w.write_all(RESTORE_MAGIC)?;
    w.write_all(&RESTORE_VERSION.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&entry_count.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved

    // Entries
    for (chunk_id, stored_size, pack_id, pack_offset) in &entries {
        w.write_all(&chunk_id.0)?;
        w.write_all(&stored_size.to_le_bytes())?;
        w.write_all(&pack_id.0)?;
        w.write_all(&pack_offset.to_le_bytes())?;
    }

    w.flush()?;
    drop(w);

    std::fs::rename(&tmp_path, path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote restore cache"
    );

    Ok(())
}

/// Memory-mapped reader over the sorted restore cache binary file.
/// Lookups use binary search over fixed-size 76-byte entries.
pub struct MmapRestoreCache {
    mmap: Mmap,
    entry_count: u32,
}

impl MmapRestoreCache {
    /// Open and validate the restore cache file.
    /// Returns `None` on any mismatch (missing file, wrong magic/version/generation,
    /// unexpected file size).
    pub fn open(
        repo_id: &[u8],
        expected_generation: u64,
        cache_dir: Option<&Path>,
    ) -> Option<Self> {
        if expected_generation == 0 {
            return None;
        }
        let path = restore_cache_path(repo_id, cache_dir)?;
        Self::open_path(&path, expected_generation)
    }

    /// Open and validate a restore cache file at an explicit path (used by tests).
    pub fn open_path(path: &Path, expected_generation: u64) -> Option<Self> {
        if expected_generation == 0 {
            return None;
        }

        let file = std::fs::File::open(path).ok()?;

        // SAFETY: we only read the file, and the file is written atomically.
        let mmap = unsafe { Mmap::map(&file) }.ok()?;

        if mmap.len() < RESTORE_HEADER_SIZE {
            debug!("restore cache: file too small for header");
            return None;
        }

        if &mmap[0..8] != RESTORE_MAGIC {
            debug!("restore cache: bad magic");
            return None;
        }

        let version = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        if version != RESTORE_VERSION {
            debug!(version, "restore cache: unsupported version");
            return None;
        }

        let index_generation = u64::from_le_bytes(mmap[12..20].try_into().unwrap());
        if index_generation != expected_generation {
            debug!(
                cache_gen = index_generation,
                expected_gen = expected_generation,
                "restore cache: generation mismatch"
            );
            return None;
        }

        let entry_count = u32::from_le_bytes(mmap[20..24].try_into().unwrap());

        let expected_size = RESTORE_HEADER_SIZE + (entry_count as usize) * RESTORE_ENTRY_SIZE;
        if mmap.len() != expected_size {
            debug!(
                actual = mmap.len(),
                expected = expected_size,
                "restore cache: file size mismatch"
            );
            return None;
        }

        debug!(
            entries = entry_count,
            generation = index_generation,
            "opened restore cache"
        );

        Some(Self { mmap, entry_count })
    }

    /// Look up a chunk ID using binary search.
    /// Returns `(pack_id, pack_offset, stored_size)` if found.
    pub fn lookup(&self, chunk_id: &ChunkId) -> Option<(PackId, u64, u32)> {
        if self.entry_count == 0 {
            return None;
        }

        let target = &chunk_id.0;
        let data = &self.mmap[RESTORE_HEADER_SIZE..];

        let mut lo: usize = 0;
        let mut hi: usize = self.entry_count as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = mid * RESTORE_ENTRY_SIZE;
            let entry_id = &data[offset..offset + 32];

            match entry_id.cmp(target.as_slice()) {
                std::cmp::Ordering::Equal => {
                    let stored_size =
                        u32::from_le_bytes(data[offset + 32..offset + 36].try_into().unwrap());
                    let mut pack_bytes = [0u8; 32];
                    pack_bytes.copy_from_slice(&data[offset + 36..offset + 68]);
                    let pack_id = PackId(pack_bytes);
                    let pack_offset =
                        u64::from_le_bytes(data[offset + 68..offset + 76].try_into().unwrap());
                    return Some((pack_id, pack_offset, stored_size));
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }

        None
    }

    /// Return the number of entries in the cache.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }
}

// ===========================================================================
// Full index cache — sorted binary cache of the entire ChunkIndex
// ===========================================================================

/// Magic bytes for the full index cache file.
const FULL_MAGIC: &[u8; 8] = b"VGFULL\0\0";

/// Current full index cache format version.
const FULL_VERSION: u32 = 1;

/// Size of the full index cache header in bytes.
const FULL_HEADER_SIZE: usize = 28;

/// Size of each full index cache entry:
/// ChunkId(32) + refcount(4) + stored_size(4) + PackId(32) + pack_offset(8) = 80.
const FULL_ENTRY_SIZE: usize = 80;

/// Return the local filesystem path for the full index cache file.
pub fn full_index_cache_path(repo_id: &[u8], cache_dir: Option<&Path>) -> Option<PathBuf> {
    repo_cache_dir(repo_id, cache_dir).map(|d| d.join("full_index_cache"))
}

/// A single entry read from the full index cache.
#[derive(Debug, Clone, Copy)]
pub struct FullCacheEntry {
    pub chunk_id: ChunkId,
    pub refcount: u32,
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
}

/// Memory-mapped reader over the sorted full index cache binary file.
pub struct MmapFullIndexCache {
    mmap: Mmap,
    entry_count: u32,
}

impl MmapFullIndexCache {
    /// Open and validate the full index cache file.
    /// Returns `None` on any mismatch.
    pub fn open(
        repo_id: &[u8],
        expected_generation: u64,
        cache_dir: Option<&Path>,
    ) -> Option<Self> {
        if expected_generation == 0 {
            return None;
        }
        let path = full_index_cache_path(repo_id, cache_dir)?;
        Self::open_path(&path, expected_generation)
    }

    /// Open and validate at an explicit path (used by tests).
    pub fn open_path(path: &Path, expected_generation: u64) -> Option<Self> {
        if expected_generation == 0 {
            return None;
        }

        let file = std::fs::File::open(path).ok()?;
        let mmap = unsafe { Mmap::map(&file) }.ok()?;

        if mmap.len() < FULL_HEADER_SIZE {
            debug!("full index cache: file too small for header");
            return None;
        }

        if &mmap[0..8] != FULL_MAGIC {
            debug!("full index cache: bad magic");
            return None;
        }

        let version = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        if version != FULL_VERSION {
            debug!(version, "full index cache: unsupported version");
            return None;
        }

        let index_generation = u64::from_le_bytes(mmap[12..20].try_into().unwrap());
        if index_generation != expected_generation {
            debug!(
                cache_gen = index_generation,
                expected_gen = expected_generation,
                "full index cache: generation mismatch"
            );
            return None;
        }

        let entry_count = u32::from_le_bytes(mmap[20..24].try_into().unwrap());

        let expected_size = FULL_HEADER_SIZE + (entry_count as usize) * FULL_ENTRY_SIZE;
        if mmap.len() != expected_size {
            debug!(
                actual = mmap.len(),
                expected = expected_size,
                "full index cache: file size mismatch"
            );
            return None;
        }

        debug!(
            entries = entry_count,
            generation = index_generation,
            "opened full index cache"
        );

        Some(Self { mmap, entry_count })
    }

    /// Return the number of entries.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Read the i-th entry (0-indexed).
    fn entry_at(&self, i: usize) -> FullCacheEntry {
        let data = &self.mmap[FULL_HEADER_SIZE..];
        let offset = i * FULL_ENTRY_SIZE;
        let mut chunk_bytes = [0u8; 32];
        chunk_bytes.copy_from_slice(&data[offset..offset + 32]);
        let refcount = u32::from_le_bytes(data[offset + 32..offset + 36].try_into().unwrap());
        let stored_size = u32::from_le_bytes(data[offset + 36..offset + 40].try_into().unwrap());
        let mut pack_bytes = [0u8; 32];
        pack_bytes.copy_from_slice(&data[offset + 40..offset + 72]);
        let pack_offset = u64::from_le_bytes(data[offset + 72..offset + 80].try_into().unwrap());
        FullCacheEntry {
            chunk_id: ChunkId(chunk_bytes),
            refcount,
            stored_size,
            pack_id: PackId(pack_bytes),
            pack_offset,
        }
    }

    /// Iterate over all entries in order.
    pub fn iter(&self) -> impl Iterator<Item = FullCacheEntry> + '_ {
        (0..self.entry_count as usize).map(move |i| self.entry_at(i))
    }

    /// Get the ChunkId bytes for the i-th entry (for merge comparisons).
    fn chunk_id_bytes_at(&self, i: usize) -> &[u8] {
        let offset = FULL_HEADER_SIZE + i * FULL_ENTRY_SIZE;
        &self.mmap[offset..offset + 32]
    }
}

/// Write a single full cache entry to a writer.
fn write_full_entry(w: &mut BufWriter<std::fs::File>, entry: &FullCacheEntry) -> Result<()> {
    w.write_all(&entry.chunk_id.0)?;
    w.write_all(&entry.refcount.to_le_bytes())?;
    w.write_all(&entry.stored_size.to_le_bytes())?;
    w.write_all(&entry.pack_id.0)?;
    w.write_all(&entry.pack_offset.to_le_bytes())?;
    Ok(())
}

/// Write the full index cache header.
fn write_full_header(
    w: &mut BufWriter<std::fs::File>,
    generation: u64,
    entry_count: u32,
) -> Result<()> {
    w.write_all(FULL_MAGIC)?;
    w.write_all(&FULL_VERSION.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&entry_count.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved
    Ok(())
}

/// Build the full index cache from a ChunkIndex HashMap.
/// This is the slow path / bootstrap: used on first backup or after cache invalidation.
pub fn build_full_index_cache(
    index: &ChunkIndex,
    generation: u64,
    repo_id: &[u8],
    cache_dir: Option<&Path>,
) -> Result<()> {
    let Some(path) = full_index_cache_path(repo_id, cache_dir) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    build_full_index_cache_to_path(index, generation, &path)
}

/// Build the full index cache to an explicit path (used by tests).
pub fn build_full_index_cache_to_path(
    index: &ChunkIndex,
    generation: u64,
    path: &Path,
) -> Result<()> {
    let mut entries: Vec<FullCacheEntry> = index
        .iter()
        .map(|(id, e)| FullCacheEntry {
            chunk_id: *id,
            refcount: e.refcount,
            stored_size: e.stored_size,
            pack_id: e.pack_id,
            pack_offset: e.pack_offset,
        })
        .collect();
    entries.sort_unstable_by(|a, b| a.chunk_id.0.cmp(&b.chunk_id.0));

    let entry_count = entries.len() as u32;
    let tmp_path = path.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let mut w = BufWriter::new(file);

    write_full_header(&mut w, generation, entry_count)?;
    for entry in &entries {
        write_full_entry(&mut w, entry)?;
    }

    w.flush()?;
    drop(w);
    std::fs::rename(&tmp_path, path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote full index cache"
    );
    Ok(())
}

/// Two-pointer merge of an existing full cache with a sorted IndexDelta.
/// Produces a new cache file at `out_path` with the given generation.
/// O(buffer) heap — no HashMap materialization.
pub fn merge_full_index_cache(
    old_cache: &MmapFullIndexCache,
    delta: &IndexDelta,
    new_generation: u64,
    out_path: &Path,
) -> Result<()> {
    // Sort new entries by ChunkId for merge
    let mut sorted_new: Vec<&crate::index::NewChunkEntry> = delta.new_entries.iter().collect();
    sorted_new.sort_unstable_by(|a, b| a.chunk_id.0.cmp(&b.chunk_id.0));

    // Count total entries: old + new (new entries should not overlap with old)
    let total_count = old_cache.entry_count() + sorted_new.len() as u32;

    let tmp_path = out_path.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let mut w = BufWriter::new(file);

    write_full_header(&mut w, new_generation, total_count)?;

    // Two-pointer merge
    let mut old_idx: usize = 0;
    let old_count = old_cache.entry_count() as usize;
    let mut new_idx: usize = 0;

    while old_idx < old_count || new_idx < sorted_new.len() {
        let take_old = if old_idx >= old_count {
            false
        } else if new_idx >= sorted_new.len() {
            true
        } else {
            old_cache.chunk_id_bytes_at(old_idx) <= sorted_new[new_idx].chunk_id.0.as_slice()
        };

        if take_old {
            let mut entry = old_cache.entry_at(old_idx);
            // Apply refcount bump if any
            if let Some(&bump) = delta.refcount_bumps.get(&entry.chunk_id) {
                entry.refcount += bump;
            }
            write_full_entry(&mut w, &entry)?;
            old_idx += 1;
        } else {
            let ne = &sorted_new[new_idx];
            let mut refcount = ne.refcount;
            // Apply refcount bump for session-new chunks
            if let Some(&bump) = delta.refcount_bumps.get(&ne.chunk_id) {
                refcount += bump;
            }
            let entry = FullCacheEntry {
                chunk_id: ne.chunk_id,
                refcount,
                stored_size: ne.stored_size,
                pack_id: ne.pack_id,
                pack_offset: ne.pack_offset,
            };
            write_full_entry(&mut w, &entry)?;
            new_idx += 1;
        }
    }

    w.flush()?;
    drop(w);
    std::fs::rename(&tmp_path, out_path)?;

    debug!(
        entries = total_count,
        path = %out_path.display(),
        "wrote merged full index cache"
    );
    Ok(())
}

/// Load a ChunkIndex HashMap from the local full index cache.
/// Used after incremental update to hydrate `self.chunk_index` without a storage round-trip.
pub fn load_chunk_index_from_full_cache(
    repo_id: &[u8],
    generation: u64,
    cache_dir: Option<&Path>,
) -> Result<ChunkIndex> {
    let path = full_index_cache_path(repo_id, cache_dir)
        .ok_or_else(|| vykar_types::error::VykarError::Other("no cache dir available".into()))?;
    load_chunk_index_from_full_cache_path(&path, generation)
}

/// Load a ChunkIndex from an explicit path (used by tests).
pub fn load_chunk_index_from_full_cache_path(path: &Path, generation: u64) -> Result<ChunkIndex> {
    let cache = MmapFullIndexCache::open_path(path, generation).ok_or_else(|| {
        vykar_types::error::VykarError::Other("full index cache not found or stale".into())
    })?;

    let mut index = ChunkIndex::with_capacity(cache.entry_count() as usize);
    for entry in cache.iter() {
        index.add(
            entry.chunk_id,
            entry.stored_size,
            entry.pack_id,
            entry.pack_offset,
        );
        // add() sets refcount=1; apply remaining refs in bulk
        if entry.refcount > 1 {
            index.increment_refcount_by(&entry.chunk_id, entry.refcount - 1);
        }
    }
    Ok(index)
}

// ---------------------------------------------------------------------------
// Streaming msgpack serializer from full cache
// ---------------------------------------------------------------------------

/// A wrapper that implements `serde::Serialize` to match the exact format
/// produced by `#[derive(Serialize)]` on `ChunkIndex { entries: HashMap<...> }`.
///
/// With rmp_serde's compact encoding, ChunkIndex serializes as:
///   [entries_map]  — a 1-element array (struct) containing the HashMap
///
/// This wrapper replicates that layout by delegating to actual `ChunkId` and
/// `ChunkIndexEntry` values with their derive-generated serializers.
struct FullCacheSerializable<'a> {
    cache: &'a MmapFullIndexCache,
}

/// Serialize the entries map portion (HashMap<ChunkId, ChunkIndexEntry>).
struct EntriesMapView<'a> {
    cache: &'a MmapFullIndexCache,
}

impl<'a> serde::Serialize for EntriesMapView<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let count = self.cache.entry_count() as usize;
        let mut map = serializer.serialize_map(Some(count))?;
        for entry in self.cache.iter() {
            let key = entry.chunk_id;
            let value = ChunkIndexEntry {
                refcount: entry.refcount,
                stored_size: entry.stored_size,
                pack_id: entry.pack_id,
                pack_offset: entry.pack_offset,
            };
            map.serialize_entry(&key, &value)?;
        }
        map.end()
    }
}

impl<'a> serde::Serialize for FullCacheSerializable<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ChunkIndex", 1)?;
        state.serialize_field("entries", &EntriesMapView { cache: self.cache })?;
        state.end()
    }
}

/// Serialize the full index cache to a packed (encrypted) repo object.
/// Returns a single Vec suitable for upload as the "index" key.
pub fn serialize_full_cache_to_packed_object(
    cache: &MmapFullIndexCache,
    crypto: &dyn vykar_crypto::CryptoEngine,
) -> Result<Vec<u8>> {
    // Estimate: ~86 bytes/entry for msgpack, then zstd compress bound
    let estimated_msgpack = cache.entry_count() as usize * 86;
    let estimated = 1 + zstd::zstd_safe::compress_bound(estimated_msgpack);
    let serializable = FullCacheSerializable { cache };

    crate::repo::format::pack_object_streaming_with_context(
        crate::repo::format::ObjectType::ChunkIndex,
        b"index",
        estimated,
        crypto,
        |buf| {
            crate::compress::compress_stream_zstd(buf, 3, |encoder| {
                rmp_serde::encode::write(encoder, &serializable)
                    .map_err(vykar_types::error::VykarError::Serialization)?;
                Ok(())
            })
        },
    )
}

/// Wraps `FullCacheSerializable` (ChunkIndex only) in the `IndexBlob` envelope
/// (generation + chunks) so the wire format matches `IndexBlobRef`'s derived
/// `Serialize`. Necessary because `serialize_full_cache_to_packed_object` only
/// serializes the ChunkIndex portion.
struct IndexBlobFromCache<'a> {
    generation: u64,
    cache: &'a MmapFullIndexCache,
}

impl<'a> serde::Serialize for IndexBlobFromCache<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("IndexBlob", 2)?;
        state.serialize_field("generation", &self.generation)?;
        state.serialize_field("chunks", &FullCacheSerializable { cache: self.cache })?;
        state.end()
    }
}

/// Serialize the full index cache as a complete `IndexBlob` (generation + chunks)
/// packed into an encrypted repo object. Returns a single Vec suitable for
/// upload as the "index" key.
pub fn serialize_full_cache_as_index_blob(
    cache: &MmapFullIndexCache,
    generation: u64,
    crypto: &dyn vykar_crypto::CryptoEngine,
) -> Result<Vec<u8>> {
    let estimated_msgpack = cache.entry_count() as usize * 86 + 16;
    let estimated = 1 + zstd::zstd_safe::compress_bound(estimated_msgpack);
    let blob = IndexBlobFromCache { generation, cache };

    crate::repo::format::pack_object_streaming_with_context(
        crate::repo::format::ObjectType::ChunkIndex,
        b"index",
        estimated,
        crypto,
        |buf| {
            crate::compress::compress_stream_zstd(buf, 3, |encoder| {
                rmp_serde::encode::write(encoder, &blob)
                    .map_err(vykar_types::error::VykarError::Serialization)?;
                Ok(())
            })
        },
    )
}

// ---------------------------------------------------------------------------
// Cache-based dedup/restore cache builders (Step 5)
// ---------------------------------------------------------------------------

/// Build the dedup cache from the full index cache (streaming, no HashMap).
pub fn build_dedup_cache_from_full_cache(
    full_cache_path: &Path,
    generation: u64,
    repo_id: &[u8],
    cache_dir: Option<&Path>,
) -> Result<()> {
    let Some(path) = dedup_cache_path(repo_id, cache_dir) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let cache = MmapFullIndexCache::open_path(full_cache_path, generation).ok_or_else(|| {
        vykar_types::error::VykarError::Other("full index cache not found or stale".into())
    })?;

    let entry_count = cache.entry_count();
    let tmp_path = path.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let mut w = BufWriter::new(file);

    // Dedup cache header
    w.write_all(MAGIC)?;
    w.write_all(&VERSION.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&entry_count.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved

    // Stream entries: ChunkId(32) + stored_size(4)
    for entry in cache.iter() {
        w.write_all(&entry.chunk_id.0)?;
        w.write_all(&entry.stored_size.to_le_bytes())?;
    }

    w.flush()?;
    drop(w);
    std::fs::rename(&tmp_path, &path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote dedup cache from full cache"
    );
    Ok(())
}

/// Build the restore cache from the full index cache (streaming, no HashMap).
pub fn build_restore_cache_from_full_cache(
    full_cache_path: &Path,
    generation: u64,
    repo_id: &[u8],
    cache_dir: Option<&Path>,
) -> Result<()> {
    let Some(path) = restore_cache_path(repo_id, cache_dir) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let cache = MmapFullIndexCache::open_path(full_cache_path, generation).ok_or_else(|| {
        vykar_types::error::VykarError::Other("full index cache not found or stale".into())
    })?;

    let entry_count = cache.entry_count();
    let tmp_path = path.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let mut w = BufWriter::new(file);

    // Restore cache header
    w.write_all(RESTORE_MAGIC)?;
    w.write_all(&RESTORE_VERSION.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&entry_count.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved

    // Stream entries: ChunkId(32) + stored_size(4) + PackId(32) + pack_offset(8)
    for entry in cache.iter() {
        w.write_all(&entry.chunk_id.0)?;
        w.write_all(&entry.stored_size.to_le_bytes())?;
        w.write_all(&entry.pack_id.0)?;
        w.write_all(&entry.pack_offset.to_le_bytes())?;
    }

    w.flush()?;
    drop(w);
    std::fs::rename(&tmp_path, &path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote restore cache from full cache"
    );
    Ok(())
}

// ===========================================================================
// Index blob cache — caches the raw encrypted+compressed index blob locally
// ===========================================================================

/// Magic bytes at the start of the index blob cache file.
const INDEX_BLOB_MAGIC: &[u8; 8] = b"VGIDXB\0\0";

/// Current index blob cache format version.
const INDEX_BLOB_VERSION: u32 = 1;

/// Size of the index blob cache header in bytes:
/// magic(8) + version(4) + generation(8) + reserved(4) = 24.
const INDEX_BLOB_HEADER_SIZE: usize = 24;

/// Return the local filesystem path for the index blob cache file.
pub fn index_blob_cache_path(repo_id: &[u8], cache_dir: Option<&Path>) -> Option<PathBuf> {
    repo_cache_dir(repo_id, cache_dir).map(|d| d.join("index_blob"))
}

/// Read the cached index blob if it exists and matches the expected generation.
/// Returns `None` on any error (missing file, wrong magic/version/generation).
pub fn read_index_blob_cache(
    repo_id: &[u8],
    expected_generation: u64,
    cache_dir: Option<&Path>,
) -> Option<Vec<u8>> {
    if expected_generation == 0 {
        return None;
    }
    let path = index_blob_cache_path(repo_id, cache_dir)?;
    let data = std::fs::read(&path).ok()?;

    if data.len() < INDEX_BLOB_HEADER_SIZE {
        debug!("index blob cache: file too small for header");
        return None;
    }

    if &data[0..8] != INDEX_BLOB_MAGIC {
        debug!("index blob cache: bad magic");
        return None;
    }

    let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
    if version != INDEX_BLOB_VERSION {
        debug!(version, "index blob cache: unsupported version");
        return None;
    }

    let generation = u64::from_le_bytes(data[12..20].try_into().unwrap());
    if generation != expected_generation {
        debug!(
            cache_gen = generation,
            expected_gen = expected_generation,
            "index blob cache: generation mismatch"
        );
        return None;
    }

    debug!(
        generation,
        blob_bytes = data.len() - INDEX_BLOB_HEADER_SIZE,
        "index blob cache hit"
    );

    Some(data[INDEX_BLOB_HEADER_SIZE..].to_vec())
}

/// Write the raw index blob to the local cache with the given generation.
/// Atomic via temp-file + rename.
pub fn write_index_blob_cache(
    blob: &[u8],
    generation: u64,
    repo_id: &[u8],
    cache_dir: Option<&Path>,
) -> Result<()> {
    let Some(path) = index_blob_cache_path(repo_id, cache_dir) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let mut w = BufWriter::new(file);

    // Header
    w.write_all(INDEX_BLOB_MAGIC)?;
    w.write_all(&INDEX_BLOB_VERSION.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved

    // Blob payload
    w.write_all(blob)?;

    w.flush()?;
    drop(w);

    std::fs::rename(&tmp_path, &path)?;

    debug!(
        generation,
        blob_bytes = blob.len(),
        path = %path.display(),
        "wrote index blob cache"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_id_to_u64_extracts_first_8_bytes() {
        let mut id = ChunkId([0u8; 32]);
        id.0[0..8].copy_from_slice(&42u64.to_le_bytes());
        assert_eq!(chunk_id_to_u64(&id), 42);
    }

    #[test]
    fn dedup_cache_path_returns_some() {
        let repo_id = [0xABu8; 32];
        let path = dedup_cache_path(&repo_id, None);
        assert!(path.is_some());
        let p = path.unwrap();
        assert!(p.to_string_lossy().contains("dedup_cache"));
        assert!(p.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    #[test]
    fn build_and_read_dedup_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dedup_cache");

        let mut index = ChunkIndex::new();
        let pack_id = vykar_types::pack_id::PackId([0x01; 32]);

        // Insert some test entries
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            index.add(chunk_id, 100 + i as u32, pack_id, i as u64 * 100);
        }

        let generation = 12345u64;

        // Build cache
        build_dedup_cache_to_path(&index, generation, &path).unwrap();

        // Open and validate
        let cache = MmapDedupCache::open_path(&path, generation).unwrap();
        assert_eq!(cache.entry_count(), 10);
        assert_eq!(cache.generation(), generation);

        // Look up each entry
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            assert_eq!(cache.get_stored_size(&chunk_id), Some(100 + i as u32));
        }

        // Look up a non-existent entry
        let missing = ChunkId([0xEE; 32]);
        assert_eq!(cache.get_stored_size(&missing), None);
    }

    #[test]
    fn mmap_cache_rejects_wrong_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dedup_cache");

        let index = ChunkIndex::new();
        let generation = 99u64;

        build_dedup_cache_to_path(&index, generation, &path).unwrap();

        // Wrong generation should return None
        assert!(MmapDedupCache::open_path(&path, 100).is_none());

        // Correct generation should work (even with 0 entries)
        assert!(MmapDedupCache::open_path(&path, 99).is_some());
    }

    #[test]
    fn mmap_cache_rejects_generation_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dedup_cache");

        // Generation 0 means "no cache ever written" — always returns None.
        assert!(MmapDedupCache::open_path(&path, 0).is_none());
    }

    #[test]
    fn tiered_index_session_new_takes_priority() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dedup_cache");

        let mut index = ChunkIndex::new();
        let pack_id = vykar_types::pack_id::PackId([0x01; 32]);
        let chunk_id = ChunkId([0xAA; 32]);
        index.add(chunk_id, 100, pack_id, 0);

        let generation = 42u64;
        build_dedup_cache_to_path(&index, generation, &path).unwrap();

        let cache = MmapDedupCache::open_path(&path, generation).unwrap();
        let mut tiered = TieredDedupIndex::new(cache);

        // Override in session_new with different size
        tiered.insert(chunk_id, 999);

        // Should return session_new value
        assert_eq!(tiered.get_stored_size(&chunk_id), Some(999));
    }

    // -------------------------------------------------------------------
    // Restore cache tests
    // -------------------------------------------------------------------

    #[test]
    fn restore_cache_path_returns_some() {
        let repo_id = [0xCDu8; 32];
        let path = restore_cache_path(&repo_id, None);
        assert!(path.is_some());
        let p = path.unwrap();
        assert!(p.to_string_lossy().contains("restore_cache"));
        assert!(p.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    #[test]
    fn build_and_read_restore_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restore_cache");

        let mut index = ChunkIndex::new();
        let pack_a = PackId([0x01; 32]);
        let pack_b = PackId([0x02; 32]);

        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            let pack = if i < 5 { pack_a } else { pack_b };
            index.add(chunk_id, 100 + i as u32, pack, i as u64 * 1000);
        }

        let generation = 777u64;
        build_restore_cache_to_path(&index, generation, &path).unwrap();

        let cache = MmapRestoreCache::open_path(&path, generation).unwrap();
        assert_eq!(cache.entry_count(), 10);

        // Look up each entry
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            let result = cache.lookup(&chunk_id);
            assert!(result.is_some(), "chunk {i} not found in restore cache");
            let (pack_id, pack_offset, stored_size) = result.unwrap();
            let expected_pack = if i < 5 { pack_a } else { pack_b };
            assert_eq!(pack_id, expected_pack);
            assert_eq!(pack_offset, i as u64 * 1000);
            assert_eq!(stored_size, 100 + i as u32);
        }

        // Non-existent entry
        let missing = ChunkId([0xFF; 32]);
        assert!(cache.lookup(&missing).is_none());
    }

    #[test]
    fn restore_cache_rejects_wrong_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restore_cache");

        let index = ChunkIndex::new();
        build_restore_cache_to_path(&index, 50, &path).unwrap();

        assert!(MmapRestoreCache::open_path(&path, 51).is_none());
        assert!(MmapRestoreCache::open_path(&path, 50).is_some());
    }

    #[test]
    fn restore_cache_rejects_generation_zero() {
        assert!(MmapRestoreCache::open_path(Path::new("/nonexistent"), 0).is_none());
    }

    // -------------------------------------------------------------------
    // Full index cache tests
    // -------------------------------------------------------------------

    /// Helper: build a test ChunkIndex with the given number of entries.
    fn make_test_index(count: u8) -> ChunkIndex {
        let mut index = ChunkIndex::new();
        let pack_id = PackId([0x01; 32]);
        for i in 0..count {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            index.add(chunk_id, 100 + i as u32, pack_id, i as u64 * 100);
        }
        index
    }

    #[test]
    fn full_index_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_index_cache");

        let index = make_test_index(10);
        let generation = 42u64;

        build_full_index_cache_to_path(&index, generation, &path).unwrap();

        let cache = MmapFullIndexCache::open_path(&path, generation).unwrap();
        assert_eq!(cache.entry_count(), 10);

        // Verify all entries match
        for entry in cache.iter() {
            let original = index.get(&entry.chunk_id).expect("entry must exist");
            assert_eq!(entry.refcount, original.refcount);
            assert_eq!(entry.stored_size, original.stored_size);
            assert_eq!(entry.pack_id, original.pack_id);
            assert_eq!(entry.pack_offset, original.pack_offset);
        }
    }

    #[test]
    fn full_index_cache_generation_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_index_cache");

        let index = make_test_index(5);
        build_full_index_cache_to_path(&index, 42, &path).unwrap();

        assert!(MmapFullIndexCache::open_path(&path, 99).is_none());
        assert!(MmapFullIndexCache::open_path(&path, 42).is_some());
    }

    #[test]
    fn full_index_cache_merge_with_bumps_on_new_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_index_cache");

        // Build initial cache with 5 entries
        let mut index = make_test_index(5);
        let gen = 42u64;
        build_full_index_cache_to_path(&index, gen, &path).unwrap();

        // Create a delta with new entries and refcount bumps
        let mut delta = IndexDelta::new();
        let pack_id = PackId([0x02; 32]);

        // Add 3 new entries
        for i in 10u8..13 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            delta.add_new_entry(chunk_id, 200 + i as u32, pack_id, i as u64 * 200, 1);
        }

        // Bump an existing entry (chunk 0)
        let existing_id = ChunkId([0u8; 32]);
        delta.bump_refcount(&existing_id);
        delta.bump_refcount(&existing_id);

        // Bump a session-new entry (chunk 10)
        let mut new_id_bytes = [0u8; 32];
        new_id_bytes[0] = 10;
        let new_id = ChunkId(new_id_bytes);
        delta.bump_refcount(&new_id);

        // Merge
        let old_cache = MmapFullIndexCache::open_path(&path, gen).unwrap();
        let new_gen = 99u64;
        merge_full_index_cache(&old_cache, &delta, new_gen, &path).unwrap();
        drop(old_cache);

        // Also apply delta to the HashMap to get expected results
        delta.apply_to(&mut index);

        // Load from cache and compare
        let loaded = load_chunk_index_from_full_cache_path(&path, new_gen).unwrap();
        assert_eq!(loaded.len(), index.len());

        for (id, expected) in index.iter() {
            let actual = loaded.get(id).expect("entry must exist in loaded index");
            assert_eq!(
                actual.refcount, expected.refcount,
                "refcount mismatch for {:?}",
                id
            );
            assert_eq!(actual.stored_size, expected.stored_size);
            assert_eq!(actual.pack_id, expected.pack_id);
            assert_eq!(actual.pack_offset, expected.pack_offset);
        }
    }

    #[test]
    fn full_index_cache_merge_empty_delta() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_index_cache");

        let index = make_test_index(5);
        let gen = 42u64;
        build_full_index_cache_to_path(&index, gen, &path).unwrap();

        let delta = IndexDelta::new();
        let old_cache = MmapFullIndexCache::open_path(&path, gen).unwrap();
        let new_gen = 100u64;
        merge_full_index_cache(&old_cache, &delta, new_gen, &path).unwrap();
        drop(old_cache);

        let loaded = load_chunk_index_from_full_cache_path(&path, new_gen).unwrap();
        assert_eq!(loaded.len(), index.len());

        for (id, expected) in index.iter() {
            let actual = loaded.get(id).unwrap();
            assert_eq!(actual.refcount, expected.refcount);
            assert_eq!(actual.stored_size, expected.stored_size);
            assert_eq!(actual.pack_id, expected.pack_id);
            assert_eq!(actual.pack_offset, expected.pack_offset);
        }
    }

    #[test]
    fn streaming_msgpack_deserializable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_index_cache");

        let index = make_test_index(10);
        let gen = 42u64;
        build_full_index_cache_to_path(&index, gen, &path).unwrap();

        let cache = MmapFullIndexCache::open_path(&path, gen).unwrap();

        // Serialize via streaming using plaintext engine
        let engine = vykar_crypto::PlaintextEngine::new(&[0xAA; 32]);
        let packed = serialize_full_cache_to_packed_object(&cache, &engine).unwrap();

        // Decrypt (plaintext), decompress, and deserialize
        let compressed = crate::repo::format::unpack_object_expect_with_context(
            &packed,
            crate::repo::format::ObjectType::ChunkIndex,
            b"index",
            &engine,
        )
        .unwrap();
        let plaintext = crate::compress::decompress_metadata(&compressed).unwrap();
        let deserialized: ChunkIndex = rmp_serde::from_slice(&plaintext).unwrap();

        // Verify all entries match
        assert_eq!(deserialized.len(), index.len());
        for (id, expected) in index.iter() {
            let actual = deserialized.get(id).expect("entry must exist");
            assert_eq!(actual.refcount, expected.refcount);
            assert_eq!(actual.stored_size, expected.stored_size);
            assert_eq!(actual.pack_id, expected.pack_id);
            assert_eq!(actual.pack_offset, expected.pack_offset);
        }
    }

    #[test]
    fn dedup_cache_from_full_cache() {
        let dir = tempfile::tempdir().unwrap();
        let full_path = dir.path().join("full_index_cache");
        let dedup_path = dir.path().join("dedup_cache");
        let dedup_from_full_path = dir.path().join("dedup_cache_from_full");

        let index = make_test_index(10);
        let gen = 42u64;

        // Build full cache
        build_full_index_cache_to_path(&index, gen, &full_path).unwrap();

        // Build dedup cache directly from HashMap
        build_dedup_cache_to_path(&index, gen, &dedup_path).unwrap();

        // Build dedup cache from full cache
        // We need to set up the path manually since we're testing to explicit paths.
        // Use the full cache to create a dedup cache at the from_full path.
        let cache = MmapFullIndexCache::open_path(&full_path, gen).unwrap();
        let entry_count = cache.entry_count();
        let tmp = dedup_from_full_path.with_extension("tmp");
        let file = std::fs::File::create(&tmp).unwrap();
        let mut w = std::io::BufWriter::new(file);
        w.write_all(MAGIC).unwrap();
        w.write_all(&VERSION.to_le_bytes()).unwrap();
        w.write_all(&gen.to_le_bytes()).unwrap();
        w.write_all(&entry_count.to_le_bytes()).unwrap();
        w.write_all(&0u32.to_le_bytes()).unwrap();
        for entry in cache.iter() {
            w.write_all(&entry.chunk_id.0).unwrap();
            w.write_all(&entry.stored_size.to_le_bytes()).unwrap();
        }
        w.flush().unwrap();
        drop(w);
        std::fs::rename(&tmp, &dedup_from_full_path).unwrap();

        // Both dedup caches should produce identical results
        let cache1 = MmapDedupCache::open_path(&dedup_path, gen).unwrap();
        let cache2 = MmapDedupCache::open_path(&dedup_from_full_path, gen).unwrap();

        assert_eq!(cache1.entry_count(), cache2.entry_count());
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            assert_eq!(
                cache1.get_stored_size(&chunk_id),
                cache2.get_stored_size(&chunk_id)
            );
        }
    }

    #[test]
    fn restore_cache_from_full_cache() {
        let dir = tempfile::tempdir().unwrap();
        let full_path = dir.path().join("full_index_cache");
        let restore_path = dir.path().join("restore_cache");
        let restore_from_full_path = dir.path().join("restore_cache_from_full");

        let index = make_test_index(10);
        let gen = 42u64;

        // Build full cache and restore cache from HashMap
        build_full_index_cache_to_path(&index, gen, &full_path).unwrap();
        build_restore_cache_to_path(&index, gen, &restore_path).unwrap();

        // Build restore cache from full cache
        let cache = MmapFullIndexCache::open_path(&full_path, gen).unwrap();
        let entry_count = cache.entry_count();
        let tmp = restore_from_full_path.with_extension("tmp");
        let file = std::fs::File::create(&tmp).unwrap();
        let mut w = std::io::BufWriter::new(file);
        w.write_all(RESTORE_MAGIC).unwrap();
        w.write_all(&RESTORE_VERSION.to_le_bytes()).unwrap();
        w.write_all(&gen.to_le_bytes()).unwrap();
        w.write_all(&entry_count.to_le_bytes()).unwrap();
        w.write_all(&0u32.to_le_bytes()).unwrap();
        for entry in cache.iter() {
            w.write_all(&entry.chunk_id.0).unwrap();
            w.write_all(&entry.stored_size.to_le_bytes()).unwrap();
            w.write_all(&entry.pack_id.0).unwrap();
            w.write_all(&entry.pack_offset.to_le_bytes()).unwrap();
        }
        w.flush().unwrap();
        drop(w);
        std::fs::rename(&tmp, &restore_from_full_path).unwrap();

        // Both restore caches should produce identical results
        let cache1 = MmapRestoreCache::open_path(&restore_path, gen).unwrap();
        let cache2 = MmapRestoreCache::open_path(&restore_from_full_path, gen).unwrap();

        assert_eq!(cache1.entry_count(), cache2.entry_count());
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            assert_eq!(cache1.lookup(&chunk_id), cache2.lookup(&chunk_id));
        }
    }

    // -------------------------------------------------------------------
    // Index blob cache tests
    // -------------------------------------------------------------------

    #[test]
    fn index_blob_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let repo_id = [0xBBu8; 32];
        let generation = 42u64;
        let blob = b"some encrypted index data here";

        // Write
        write_index_blob_cache(blob, generation, &repo_id, Some(dir.path())).unwrap();

        // Read back
        let cached = read_index_blob_cache(&repo_id, generation, Some(dir.path()));
        assert_eq!(cached.as_deref(), Some(blob.as_slice()));
    }

    #[test]
    fn index_blob_cache_rejects_wrong_generation() {
        let dir = tempfile::tempdir().unwrap();
        let repo_id = [0xCCu8; 32];
        let blob = b"test blob";

        write_index_blob_cache(blob, 42, &repo_id, Some(dir.path())).unwrap();

        assert!(read_index_blob_cache(&repo_id, 99, Some(dir.path())).is_none());
        assert!(read_index_blob_cache(&repo_id, 42, Some(dir.path())).is_some());
    }

    #[test]
    fn index_blob_cache_rejects_generation_zero() {
        let repo_id = [0xDDu8; 32];
        assert!(read_index_blob_cache(&repo_id, 0, None).is_none());
    }

    #[test]
    fn index_blob_cache_path_returns_some() {
        let repo_id = [0xEEu8; 32];
        let path = index_blob_cache_path(&repo_id, None);
        assert!(path.is_some());
        let p = path.unwrap();
        assert!(p.to_string_lossy().contains("index_blob"));
        assert!(p.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    /// Verify that `serialize_full_cache_as_index_blob` produces data that
    /// round-trips through decrypt + decompress + `from_slice::<IndexBlob>`,
    /// uses compact positional encoding (FixArray envelope), and is semantically
    /// identical to what
    /// `IndexBlobRef`'s derived `Serialize` would produce.
    #[test]
    fn index_blob_from_cache_round_trip_and_wire_equivalence() {
        use crate::index::{ChunkIndex, IndexBlob, IndexBlobRef};
        use vykar_types::pack_id::PackId;

        let dir = tempfile::tempdir().unwrap();
        let full_path = dir.path().join("full_cache");

        // Build a ChunkIndex with a few entries (including multi-refcount).
        let mut index = ChunkIndex::new();
        let pack = PackId([0x01; 32]);
        for i in 0u8..5 {
            let mut id = [0u8; 32];
            id[0] = i;
            index.add(ChunkId(id), 100 + i as u32, pack, i as u64 * 100);
        }
        // Bump one entry's refcount
        let mut bump_id = [0u8; 32];
        bump_id[0] = 2;
        index.increment_refcount(&ChunkId(bump_id));

        let generation = 7777u64;

        // Write full cache, reopen as mmap
        build_full_index_cache_to_path(&index, generation, &full_path).unwrap();
        let cache = MmapFullIndexCache::open_path(&full_path, generation).unwrap();

        // Serialize as IndexBlob envelope via cache
        let crypto = vykar_crypto::PlaintextEngine::new(&[0xAA; 32]);
        let packed = serialize_full_cache_as_index_blob(&cache, generation, &crypto).unwrap();

        // Decrypt + decompress
        let compressed = crate::repo::format::unpack_object_expect_with_context(
            &packed,
            crate::repo::format::ObjectType::ChunkIndex,
            b"index",
            &crypto,
        )
        .unwrap();
        let decompressed = crate::compress::decompress_metadata(&compressed).unwrap();
        let blob: IndexBlob = rmp_serde::from_slice(&decompressed).unwrap();

        // Verify round-trip correctness
        assert_eq!(blob.generation, generation);
        assert_eq!(blob.chunks.len(), 5);
        assert_eq!(blob.chunks.get(&ChunkId(bump_id)).unwrap().refcount, 2);

        // Verify compact positional encoding: the outer IndexBlob must be
        // serialized as a FixArray (marker 0x92 = 2-element array), not a
        // FixMap. A map-shaped envelope would round-trip through from_slice
        // but break streaming readers (from_read) and is a format regression.
        assert_eq!(
            decompressed[0], 0x92,
            "IndexBlob envelope must be a 2-element FixArray (0x92), got {:#04x}",
            decompressed[0]
        );
        // The IndexBlobRef derive produces the same FixArray envelope.
        let ref_bytes = rmp_serde::to_vec(&IndexBlobRef {
            generation,
            chunks: &index,
        })
        .unwrap();
        assert_eq!(
            ref_bytes[0], 0x92,
            "IndexBlobRef must also produce FixArray"
        );

        // Verify semantic equivalence of all entries (byte-for-byte
        // comparison is not possible due to HashMap iteration order).
        let ref_blob: IndexBlob = rmp_serde::from_slice(&ref_bytes).unwrap();
        assert_eq!(ref_blob.generation, blob.generation);
        assert_eq!(ref_blob.chunks.len(), blob.chunks.len());
        for (id, entry) in ref_blob.chunks.iter() {
            let cache_entry = blob.chunks.get(id).expect("missing chunk in cache output");
            assert_eq!(entry, cache_entry, "mismatch for chunk {id:?}");
        }
    }
}
