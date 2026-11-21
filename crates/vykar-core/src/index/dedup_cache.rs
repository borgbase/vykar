// memmap2::Mmap::map for read-only cache files; SAFETY documented per block.
#![allow(unsafe_code)]
// Wire-format reads against header-validated cache files. `open_validated`
// checks `mmap.len() >= header_size` and requires the file length to equal
// the (overflow-checked) size the header's `entry_count` implies, so entry
// slicing is in bounds for *any* input that reaches a reader, corrupt or
// not — a header that doesn't add up is a cache miss (`None`) instead.
#![allow(clippy::indexing_slicing)]

use super::hasher::ChunkIdHashMap;
use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::Mmap;
use tempfile::{NamedTempFile, TempPath};
use tracing::{debug, info, warn};
use xorf::{Filter, Xor8};

use crate::cache_paths::repo_cache_dir;
use crate::index::{ChunkIndex, ChunkIndexEntry, IndexDelta};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;
use vykar_types::pack_id::PackId;

fn read_u32_le(bytes: [u8; 4]) -> u32 {
    u32::from_le_bytes(bytes)
}

fn read_u64_le(bytes: [u8; 8]) -> u64 {
    u64::from_le_bytes(bytes)
}

/// File names of the four index caches in the per-repo cache dir.
pub(crate) const DEDUP_CACHE_FILE: &str = "dedup_cache";
pub(crate) const RESTORE_CACHE_FILE: &str = "restore_cache";
pub(crate) const FULL_INDEX_CACHE_FILE: &str = "full_index_cache";
pub(crate) const INDEX_BLOB_CACHE_FILE: &str = "index_blob";

/// Targets `temp_file_beside` writes beside in the per-repo cache dir.
const CACHE_FILE_NAMES: [&str; 4] = [
    DEDUP_CACHE_FILE,
    RESTORE_CACHE_FILE,
    FULL_INDEX_CACHE_FILE,
    INDEX_BLOB_CACHE_FILE,
];

/// Fixed temp names that binaries up to v0.24.0 wrote beside the caches
/// (`with_extension("tmp")`, and the fast path's `full_index_cache.merged`).
const LEGACY_TEMP_NAMES: [&str; 5] = [
    "dedup_cache.tmp",
    "restore_cache.tmp",
    "full_index_cache.tmp",
    "index_blob.tmp",
    "full_index_cache.merged",
];

const TEMP_SUFFIX: &str = ".tmp";

fn temp_prefix(target_name: &str) -> String {
    format!(".{target_name}.")
}

/// A uniquely named temp file in `target`'s directory, to be persisted over
/// `target`. Unique names let concurrent writers of the same cache (two
/// processes catching up one cache directory) never share a temp file; the
/// last `persist` wins and every mapping keeps its own inode.
fn temp_file_beside(target: &Path) -> Result<NamedTempFile> {
    let dir = match target.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let prefix = temp_prefix(
        target
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("cache"),
    );
    Ok(tempfile::Builder::new()
        .prefix(&prefix)
        .suffix(TEMP_SUFFIX)
        .tempfile_in(dir)?)
}

/// Atomically move a finished temp file over `target`.
fn persist_temp(tmp: NamedTempFile, target: &Path) -> Result<()> {
    tmp.persist(target).map_err(|e| e.error)?;
    Ok(())
}

/// Flush a buffered temp-file writer and hand back the temp file.
fn into_temp(w: BufWriter<NamedTempFile>) -> Result<NamedTempFile> {
    w.into_inner().map_err(|e| e.into_error().into())
}

// ---------------------------------------------------------------------------
// Sweep of leftover temp files
// ---------------------------------------------------------------------------

/// How old (by mtime) a leftover cache temp file must be before it is deleted.
///
/// The gate is a heuristic, not ownership: a file is swept only when it has
/// not been written for 24 h. Every writer advances the mtime while it writes,
/// and the longest wait between finishing a temp file and renaming it (one
/// pack verification or one base upload) is far shorter. A process suspended
/// longer than that in between (a laptop asleep during a base upload, a
/// wall-clock jump) loses the file: the operation fails or continues without
/// the local cache, and the next run recovers. Nothing is corrupted, because
/// no cache file itself is ever unlinked or truncated.
pub(crate) const STALE_TEMP_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// Minimum time between two sweeps of the system temp dir in one process.
const TEMP_DIR_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// What a sweep removed.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TempSweep {
    pub files: u64,
    pub bytes: u64,
}

impl TempSweep {
    fn add(&mut self, other: TempSweep) {
        self.files += other.files;
        self.bytes += other.bytes;
    }
}

/// Whether `name` is a temp file `temp_file_beside` creates beside one of
/// `targets`: `.<target>.<alphanumerics>.tmp`.
fn is_cache_temp_name(name: &str, targets: &[&str]) -> bool {
    targets.iter().any(|t| {
        name.strip_prefix(temp_prefix(t).as_str())
            .and_then(|rest| rest.strip_suffix(TEMP_SUFFIX))
            .is_some_and(|mid| !mid.is_empty() && mid.chars().all(|c| c.is_ascii_alphanumeric()))
    })
}

/// Delete, in `dir` only (no recursion), the regular files whose name is a
/// temp name of one of `targets` or one of `legacy`, and whose mtime is more
/// than `max_age` old. Never fails: every error is logged and skipped.
pub(crate) fn sweep_stale_temps_in(
    dir: &Path,
    targets: &[&str],
    legacy: &[&str],
    max_age: std::time::Duration,
) -> TempSweep {
    let mut swept = TempSweep::default();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            debug!(dir = %dir.display(), error = %e, "cache temp sweep: cannot list dir");
            return swept;
        }
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if !is_cache_temp_name(name, targets) && !legacy.contains(&name) {
            continue;
        }
        let path = entry.path();
        // By path, not `entry.metadata()`: on Windows that returns the
        // directory listing's times, which can lag behind a live writer.
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.file_type().is_file() {
            continue;
        }
        // A future mtime (clock skew) makes `elapsed` fail: not stale.
        let stale = meta
            .modified()
            .ok()
            .and_then(|m| m.elapsed().ok())
            .is_some_and(|age| age > max_age);
        if !stale {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                swept.files += 1;
                swept.bytes += meta.len();
                debug!(path = %path.display(), bytes = meta.len(), "removed stale cache temp file");
            }
            // Another process swept or persisted it first.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                debug!(path = %path.display(), error = %e, "failed to remove stale cache temp file");
            }
        }
    }
    swept
}

/// Sweep the leftovers of interrupted local cache writes in a repository's
/// cache dir and, when given, the system temp dir. The temp dir holds the
/// temp-anchor files of every sync that bypasses the local cache (`check`,
/// `check --repair`, upgrade resume) and of runs without a cache dir.
fn sweep_stale_cache_temps_in(repo_cache_dir: Option<&Path>, temp_dir: Option<&Path>) -> TempSweep {
    let mut swept = TempSweep::default();
    if let Some(dir) = repo_cache_dir {
        swept.add(sweep_stale_temps_in(
            dir,
            &CACHE_FILE_NAMES,
            &LEGACY_TEMP_NAMES,
            STALE_TEMP_AGE,
        ));
    }
    if let Some(dir) = temp_dir {
        swept.add(sweep_stale_temps_in(
            dir,
            &[crate::cache_paths::TEMP_ANCHOR_FILE],
            &[],
            STALE_TEMP_AGE,
        ));
    }
    if swept.files > 0 {
        info!(
            files = swept.files,
            bytes = swept.bytes,
            "removed stale local cache temp files"
        );
    }
    swept
}

/// Whether a temp-dir sweep is due at `now`, given the time of the last one;
/// records `now` as the last sweep when it is.
fn temp_dir_sweep_due(
    last: &mut Option<std::time::Instant>,
    now: std::time::Instant,
    interval: std::time::Duration,
) -> bool {
    let due = last.is_none_or(|prev| now.saturating_duration_since(prev) >= interval);
    if due {
        *last = Some(now);
    }
    due
}

/// Sweep the leftovers of interrupted local cache writes: the repository's
/// cache dir on every call, and the system temp dir at most once per
/// `TEMP_DIR_SWEEP_INTERVAL` per process.
pub(crate) fn sweep_stale_cache_temps(repo_id: &[u8], cache_dir: Option<&Path>) -> TempSweep {
    static LAST_TEMP_DIR_SWEEP: std::sync::Mutex<Option<std::time::Instant>> =
        std::sync::Mutex::new(None);
    let temp_dir_due = {
        let mut last = LAST_TEMP_DIR_SWEEP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        temp_dir_sweep_due(
            &mut last,
            std::time::Instant::now(),
            TEMP_DIR_SWEEP_INTERVAL,
        )
    };
    // Test binaries never list or delete in the real system temp dir.
    let temp_dir = temp_dir_due
        .then(std::env::temp_dir)
        .filter(|_| !cfg!(test));
    sweep_stale_cache_temps_in(
        repo_cache_dir(repo_id, cache_dir).as_deref(),
        temp_dir.as_deref(),
    )
}

// ---------------------------------------------------------------------------
// Shared sorted-cache scaffolding
// ---------------------------------------------------------------------------

/// Static description of one sorted, fixed-entry cache file format.
///
/// All three mmap'd caches (dedup, restore, full index) share the same
/// 28-byte header — magic(8), version(4), generation(8), entry_count(4),
/// reserved(4) — followed by an array of fixed-size entries, each starting
/// with a 32-byte `ChunkId`, sorted by those bytes. `trailer_size` covers the
/// full index cache's 32-byte checksum; the other two have no trailer.
struct CacheSpec {
    magic: &'static [u8; 8],
    version: u32,
    header_size: usize,
    entry_size: usize,
    trailer_size: usize,
    /// Prefix used in the `debug!` miss messages, e.g. `"dedup cache"`.
    label: &'static str,
}

/// A cache file that passed [`CacheSpec::open_validated`].
struct ValidatedCache {
    mmap: Mmap,
    entry_count: u32,
    generation: u64,
}

impl CacheSpec {
    /// Total on-disk size a cache holding `entry_count` entries must have.
    ///
    /// Checked throughout: `entry_count` comes from the file header, so it is
    /// corruption-controlled input. On a 32-bit host `1 << 30` dedup entries
    /// times the 36-byte entry size wraps to exactly 0, which would let a
    /// header-only file pass the size check in `open_validated` and leave
    /// every subsequent entry slice out of bounds. `None` means the header
    /// describes a file that cannot exist — a malformed cache, i.e. a miss.
    fn framed_size(&self, entry_count: u32) -> Option<usize> {
        (entry_count as usize)
            .checked_mul(self.entry_size)?
            .checked_add(self.header_size)?
            .checked_add(self.trailer_size)
    }

    /// Open, memory-map, and header-validate a cache file.
    ///
    /// Returns `None` on any mismatch (missing file, wrong magic / version /
    /// generation, unexpected file size) — every caller treats that as a
    /// cache miss and falls back to its slow path. Content-level validation
    /// (the full index cache's checksum trailer) stays with the caller; this
    /// only guarantees the file's framing is what the spec describes.
    ///
    /// `expected_generation: None` skips the generation check, for callers
    /// that identify the cache by its content checksum instead.
    fn open_validated(
        &self,
        path: &Path,
        expected_generation: Option<u64>,
    ) -> Option<ValidatedCache> {
        // Generation 0 means "no cache ever written".
        if expected_generation == Some(0) {
            return None;
        }

        let file = std::fs::File::open(path).ok()?;

        // SAFETY: the mapping is read-only — we never write through it — and
        // the mapped file must not be modified in place for its lifetime.
        // That is the invariant every writer in this module upholds without
        // exception: each one builds a sibling temp file and `rename`s it
        // over the target, which swaps the directory entry and leaves this
        // inode untouched for as long as we hold it open. Any writer added
        // here must do the same. Today that is `write_sorted_cache` (dedup
        // and restore), `FullCacheWriter` (every full index cache writer,
        // including the segment apply and diff in `index::segment`), and the
        // two streaming derivations `build_dedup_cache_from_full_cache` /
        // `build_restore_cache_from_full_cache`. A vykar process racing
        // another vykar process therefore only ever reads a *stale* mapping,
        // which the generation check turns into a cache miss. A finished temp
        // file may also be mapped before it is renamed
        // (`index_store::sync_from`); the rename does not modify it.
        //
        // In-place truncation by something outside vykar (a stray editor, a
        // `>` redirect) would fault on access — no amount of bounds checking
        // can prevent that, so these files are documented as vykar-owned.
        // They live under the per-repo cache dir and are always rebuildable.
        let mmap = unsafe { Mmap::map(&file) }.ok()?;

        let label = self.label;
        if mmap.len() < self.header_size {
            debug!("{label}: file too small for header");
            return None;
        }

        if &mmap[0..8] != self.magic {
            debug!("{label}: bad magic");
            return None;
        }

        // The header_size check above guarantees these fixed-offset slices.
        let version = read_u32_le(mmap[8..12].try_into().expect("4-byte slice from header"));
        if version != self.version {
            debug!(version, "{label}: unsupported version");
            return None;
        }

        let generation = read_u64_le(mmap[12..20].try_into().expect("8-byte slice from header"));
        if expected_generation.is_some_and(|expected| generation != expected) {
            debug!(
                cache_gen = generation,
                expected_gen = expected_generation,
                "{label}: generation mismatch"
            );
            return None;
        }

        let entry_count = read_u32_le(mmap[20..24].try_into().expect("4-byte slice from header"));

        let expected_size = self.framed_size(entry_count);
        if Some(mmap.len()) != expected_size {
            debug!(
                actual = mmap.len(),
                expected = ?expected_size,
                "{label}: file size mismatch"
            );
            return None;
        }

        debug!(entries = entry_count, generation, "opened {label}");

        Some(ValidatedCache {
            mmap,
            entry_count,
            generation,
        })
    }

    /// Binary search the sorted entry array for `chunk_id`.
    ///
    /// Returns the matching entry's full byte slice (the 32-byte `ChunkId`
    /// prefix included); callers decode their own payload fields from it.
    fn find_entry<'a>(
        &self,
        mmap: &'a Mmap,
        entry_count: u32,
        chunk_id: &ChunkId,
    ) -> Option<&'a [u8]> {
        self.find_entry_with_position(mmap, entry_count, chunk_id)
            .map(|(_, entry)| entry)
    }

    /// [`find_entry`](Self::find_entry), also returning the entry's index in
    /// the sorted array.
    fn find_entry_with_position<'a>(
        &self,
        mmap: &'a Mmap,
        entry_count: u32,
        chunk_id: &ChunkId,
    ) -> Option<(u32, &'a [u8])> {
        if entry_count == 0 {
            return None;
        }

        let target = chunk_id.as_bytes();
        let data = &mmap[self.header_size..];

        let mut lo: usize = 0;
        let mut hi: usize = entry_count as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = mid * self.entry_size;
            let entry = &data[offset..offset + self.entry_size];

            match entry[..32].cmp(target.as_slice()) {
                // `mid < entry_count`, so it fits in u32.
                std::cmp::Ordering::Equal => return Some((mid as u32, entry)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }

        None
    }
}

/// Write the shared 28-byte cache header.
fn write_cache_header(
    w: &mut impl Write,
    spec: &CacheSpec,
    generation: u64,
    entry_count: u32,
) -> Result<()> {
    w.write_all(spec.magic)?;
    w.write_all(&spec.version.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&entry_count.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved
    Ok(())
}

/// Write a trailer-less sorted cache file atomically (temp file + rename),
/// streaming entries through a `BufWriter` so the output is never fully
/// materialized in memory.
///
/// Shared by the dedup and restore writers. The full index cache writer is
/// deliberately *not* routed through here: it folds a running BLAKE3
/// checksum into every entry write and appends it as a trailer.
fn write_sorted_cache<T>(
    spec: &CacheSpec,
    path: &Path,
    generation: u64,
    entries: &[T],
    mut write_entry: impl FnMut(&mut BufWriter<NamedTempFile>, &T) -> Result<()>,
) -> Result<()> {
    debug_assert_eq!(spec.trailer_size, 0, "write_sorted_cache writes no trailer");

    let entry_count = entries.len() as u32;

    let mut w = BufWriter::new(temp_file_beside(path)?);

    write_cache_header(&mut w, spec, generation, entry_count)?;
    for entry in entries {
        write_entry(&mut w, entry)?;
    }

    persist_temp(into_temp(w)?, path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote {}", spec.label
    );

    Ok(())
}

/// Magic bytes at the start of the dedup cache file.
// Wire-format constant — DO NOT rename (backward compatibility)
const MAGIC: &[u8; 8] = b"VGDEDUP\0";

/// Current format version.
const VERSION: u32 = 1;

/// Size of the fixed header in bytes.
const HEADER_SIZE: usize = 28;

/// Size of each entry: 32-byte ChunkId + 4-byte stored_size.
const ENTRY_SIZE: usize = 36;

const DEDUP_SPEC: CacheSpec = CacheSpec {
    magic: MAGIC,
    version: VERSION,
    header_size: HEADER_SIZE,
    entry_size: ENTRY_SIZE,
    trailer_size: 0,
    label: "dedup cache",
};

// ---------------------------------------------------------------------------
// Path helper
// ---------------------------------------------------------------------------

/// Return the local filesystem path for the dedup cache file.
/// `~/.cache/vykar/<repo_id_hex>/dedup_cache` (same directory as file cache).
pub fn dedup_cache_path(repo_id: &[u8], cache_dir: Option<&Path>) -> Option<PathBuf> {
    repo_cache_dir(repo_id, cache_dir).map(|d| d.join(DEDUP_CACHE_FILE))
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
    entries.sort_unstable_by_key(|a| *a.0.as_bytes());

    write_sorted_cache(
        &DEDUP_SPEC,
        path,
        generation,
        &entries,
        |w, (chunk_id, stored_size)| {
            w.write_all(chunk_id.as_bytes())?;
            w.write_all(&stored_size.to_le_bytes())?;
            Ok(())
        },
    )
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
        let validated = DEDUP_SPEC.open_validated(path, Some(expected_generation))?;
        Some(Self {
            mmap: validated.mmap,
            entry_count: validated.entry_count,
            index_generation: validated.generation,
        })
    }

    /// Look up a chunk ID using binary search. Returns the stored_size if found.
    pub fn get_stored_size(&self, chunk_id: &ChunkId) -> Option<u32> {
        let entry = DEDUP_SPEC.find_entry(&self.mmap, self.entry_count, chunk_id)?;
        Some(read_u32_le(
            entry[32..36]
                .try_into()
                .expect("4-byte slice within validated entry"),
        ))
    }

    /// Look up a chunk ID using binary search. Returns the entry's position in
    /// the sorted array together with its stored_size.
    ///
    /// The position is what [`IndexDelta`] records refcount bumps against
    /// (see [`crate::index::CachedBumps`]); it is only meaningful for this
    /// cache instance.
    pub fn find_with_position(&self, chunk_id: &ChunkId) -> Option<(u32, u32)> {
        let (position, entry) =
            DEDUP_SPEC.find_entry_with_position(&self.mmap, self.entry_count, chunk_id)?;
        let stored_size = read_u32_le(
            entry[32..36]
                .try_into()
                .expect("4-byte slice within validated entry"),
        );
        Some((position, stored_size))
    }

    /// The 32 `ChunkId` bytes of the entry at `position`.
    ///
    /// Panics if `position >= entry_count()`.
    pub(crate) fn chunk_id_bytes_at(&self, position: u32) -> &[u8] {
        assert!(
            position < self.entry_count,
            "dedup cache position out of range"
        );
        let offset = HEADER_SIZE + position as usize * ENTRY_SIZE;
        &self.mmap[offset..offset + 32]
    }

    /// The `ChunkId` of the entry at `position`.
    ///
    /// Panics if `position >= entry_count()`.
    pub(crate) fn chunk_id_at(&self, position: u32) -> ChunkId {
        ChunkId::from_bytes(
            self.chunk_id_bytes_at(position)
                .try_into()
                .expect("32-byte ChunkId slice"),
        )
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
            read_u64_le(
                data[offset..offset + 8]
                    .try_into()
                    .expect("8-byte slice within validated entry"),
            )
        })
    }
}

// ---------------------------------------------------------------------------
// Xor filter helpers
// ---------------------------------------------------------------------------

/// Extract the first 8 bytes of a ChunkId as a little-endian u64.
/// Keyed-hash output has excellent entropy, so this is a high-quality hash key.
pub(crate) fn chunk_id_to_u64(id: &ChunkId) -> u64 {
    read_u64_le(id.as_bytes()[..8].try_into().expect("ChunkId is 32 bytes"))
}

/// Build an Xor8 filter from pre-computed u64 keys.
/// Returns `None` if the slice is empty or construction fails.
///
/// The `catch_unwind` is still warranted after xorf 0.13. `Xor8::from` keeps a
/// `debug_assert!` that every key is distinct, and these keys are 64-bit
/// truncations of chunk IDs, so two distinct chunks can collide. 0.13's
/// construction fix was to BinaryFuse segment-length boundaries, which this
/// code does not use. It only catches in debug builds — the release profile
/// sets `panic = "abort"` and compiles the assertion out anyway — but a
/// missing filter is harmless either way: it is a probabilistic pre-filter, so
/// falling back to `None` re-checks chunks against the index rather than
/// losing data.
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
// DedupPrefilter — worker-side, read-only view of the pre-session chunk set
// ---------------------------------------------------------------------------

/// What a backup worker learned about a chunk from the [`DedupPrefilter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrefilterVerdict {
    /// Not in the pre-session chunk set: a filter miss (xor filters have no
    /// false negatives), or a filter false positive the mmap tier ruled out.
    /// It may still be a chunk added earlier in this session.
    Absent,
    /// Confirmed in the immutable pre-session mmap tier, at `position` in its
    /// sorted entry array (what the consumer records the refcount bump by).
    Present { stored_size: u32, position: u32 },
    /// Filter hit with no authoritative set to confirm it against (the
    /// non-tiered `DedupIndex` fallback, whose map the write session owns).
    Unconfirmed,
}

/// Read-only dedup state that parallel backup workers can query without the
/// repository: the xor filter and, in tiered mode, the mmap'd pre-session
/// cache it was built from. Cheap to clone (two `Arc`s).
///
/// Confirming a filter hit in the worker lets it drop the raw chunk bytes
/// immediately instead of holding them until the consumer resolves the hit.
/// Sound because the mmap tier is immutable for the whole session — it is
/// never written through, `TieredDedupIndex::remove` and rollback only touch
/// `session_new` — so a chunk present here is still present when the
/// consumer bumps its refcount (see `Repository::bump_ref_presession_hit`).
#[derive(Clone)]
pub(crate) struct DedupPrefilter {
    filter: Arc<Xor8>,
    presession: Option<Arc<MmapDedupCache>>,
}

impl DedupPrefilter {
    /// A filter without an authoritative set: hits stay [`PrefilterVerdict::Unconfirmed`].
    pub(crate) fn filter_only(filter: Arc<Xor8>) -> Self {
        Self {
            filter,
            presession: None,
        }
    }

    /// Classify `id` against the pre-session chunk set.
    pub(crate) fn lookup(&self, id: &ChunkId) -> PrefilterVerdict {
        if !self.filter.contains(&chunk_id_to_u64(id)) {
            return PrefilterVerdict::Absent;
        }
        match self.presession {
            Some(ref cache) => match cache.find_with_position(id) {
                Some((position, stored_size)) => PrefilterVerdict::Present {
                    stored_size,
                    position,
                },
                None => PrefilterVerdict::Absent,
            },
            None => PrefilterVerdict::Unconfirmed,
        }
    }

    /// The mmap tier `Present` verdicts came from, if any.
    pub(crate) fn presession_cache(&self) -> Option<&Arc<MmapDedupCache>> {
        self.presession.as_ref()
    }
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
    /// Shared with backup workers through [`DedupPrefilter`] and with the
    /// session's [`IndexDelta`], which records refcount bumps by position in
    /// this cache and resolves them back to chunk IDs at commit. Never
    /// mutated: the mapping is read-only and `remove` only touches
    /// `session_new`.
    mmap_cache: Arc<MmapDedupCache>,
    session_new: ChunkIdHashMap<u32>,
}

/// Where a [`TieredDedupIndex`] lookup found a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TieredHit {
    /// Created earlier in this session (`session_new` tier).
    SessionNew { stored_size: u32 },
    /// Present in the mmap'd dedup cache at `position`.
    Cached { position: u32, stored_size: u32 },
}

impl TieredHit {
    pub fn stored_size(self) -> u32 {
        match self {
            Self::SessionNew { stored_size } | Self::Cached { stored_size, .. } => stored_size,
        }
    }
}

impl TieredDedupIndex {
    /// Create a new tiered index from an opened mmap cache.
    pub fn new(mmap_cache: MmapDedupCache) -> Self {
        let xor_filter = build_xor_filter(&mmap_cache).map(Arc::new);
        Self {
            xor_filter,
            mmap_cache: Arc::new(mmap_cache),
            session_new: ChunkIdHashMap::default(),
        }
    }

    /// Worker-side prefilter that can confirm filter hits against the
    /// immutable mmap tier. `None` when no xor filter was built.
    pub(crate) fn prefilter(&self) -> Option<DedupPrefilter> {
        let filter = self.xor_filter.clone()?;
        Some(DedupPrefilter {
            filter,
            presession: Some(Arc::clone(&self.mmap_cache)),
        })
    }

    /// True if `cache` is this index's mmap tier (the very same mapping, not
    /// merely an equal file), so a worker's lookup in it is a lookup in the
    /// set this index resolves against.
    pub(crate) fn is_backed_by(&self, cache: &Arc<MmapDedupCache>) -> bool {
        Arc::ptr_eq(&self.mmap_cache, cache)
    }

    /// The mmap'd cache tier, for building a positional [`IndexDelta`]
    /// (`IndexDelta::with_cached_bumps`).
    pub(crate) fn mmap_cache(&self) -> &Arc<MmapDedupCache> {
        &self.mmap_cache
    }

    /// Check if a chunk exists in any tier.
    pub fn contains(&self, id: &ChunkId) -> bool {
        self.get_stored_size(id).is_some()
    }

    /// Look up a chunk's stored size across all tiers.
    pub fn get_stored_size(&self, id: &ChunkId) -> Option<u32> {
        self.find(id).map(TieredHit::stored_size)
    }

    /// Look up a chunk across all tiers, reporting which tier hit.
    pub fn find(&self, id: &ChunkId) -> Option<TieredHit> {
        // Tier 1: session-new chunks
        if let Some(&stored_size) = self.session_new.get(id) {
            return Some(TieredHit::SessionNew { stored_size });
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
        self.find_cached(id)
    }

    /// Look up stored size, skipping the xor filter tier.
    /// Caller already checked the xor filter (hit) — go straight to session_new → mmap.
    pub fn get_stored_size_skip_filter(&self, id: &ChunkId) -> Option<u32> {
        self.find_skip_filter(id).map(TieredHit::stored_size)
    }

    /// [`find`](Self::find) without the xor filter tier (caller already
    /// checked it and got a hit).
    pub fn find_skip_filter(&self, id: &ChunkId) -> Option<TieredHit> {
        if let Some(&stored_size) = self.session_new.get(id) {
            return Some(TieredHit::SessionNew { stored_size });
        }
        self.find_cached(id)
    }

    fn find_cached(&self, id: &ChunkId) -> Option<TieredHit> {
        self.mmap_cache
            .find_with_position(id)
            .map(|(position, stored_size)| TieredHit::Cached {
                position,
                stored_size,
            })
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

const RESTORE_SPEC: CacheSpec = CacheSpec {
    magic: RESTORE_MAGIC,
    version: RESTORE_VERSION,
    header_size: RESTORE_HEADER_SIZE,
    entry_size: RESTORE_ENTRY_SIZE,
    trailer_size: 0,
    label: "restore cache",
};

/// Return the local filesystem path for the restore cache file.
pub fn restore_cache_path(repo_id: &[u8], cache_dir: Option<&Path>) -> Option<PathBuf> {
    repo_cache_dir(repo_id, cache_dir).map(|d| d.join(RESTORE_CACHE_FILE))
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
    entries.sort_unstable_by_key(|a| *a.0.as_bytes());

    write_sorted_cache(
        &RESTORE_SPEC,
        path,
        generation,
        &entries,
        |w, (chunk_id, stored_size, pack_id, pack_offset)| {
            w.write_all(chunk_id.as_bytes())?;
            w.write_all(&stored_size.to_le_bytes())?;
            w.write_all(pack_id.as_bytes())?;
            w.write_all(&pack_offset.to_le_bytes())?;
            Ok(())
        },
    )
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
        let validated = RESTORE_SPEC.open_validated(path, Some(expected_generation))?;
        Some(Self {
            mmap: validated.mmap,
            entry_count: validated.entry_count,
        })
    }

    /// Look up a chunk ID using binary search.
    /// Returns `(pack_id, pack_offset, stored_size)` if found.
    pub fn lookup(&self, chunk_id: &ChunkId) -> Option<(PackId, u64, u32)> {
        let entry = RESTORE_SPEC.find_entry(&self.mmap, self.entry_count, chunk_id)?;
        let stored_size = read_u32_le(
            entry[32..36]
                .try_into()
                .expect("4-byte slice within validated entry"),
        );
        let mut pack_bytes = [0u8; 32];
        pack_bytes.copy_from_slice(&entry[36..68]);
        let pack_offset = read_u64_le(
            entry[68..76]
                .try_into()
                .expect("8-byte slice within validated entry"),
        );
        Some((PackId::from_bytes(pack_bytes), pack_offset, stored_size))
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
///
/// Version 3 appends a 32-byte BLAKE3 checksum trailer over the entry region
/// (version 2 used BLAKE2b-256, version 1 had no trailer). Other versions are
/// rejected by the version check, so callers fall through to the slow path
/// and rebuild — self-healing, no migration code.
const FULL_VERSION: u32 = 3;

/// Size of the full index cache header in bytes.
const FULL_HEADER_SIZE: usize = 28;

/// Size of each full index cache entry:
/// ChunkId(32) + refcount(4) + stored_size(4) + PackId(32) + pack_offset(8) = 80.
const FULL_ENTRY_SIZE: usize = 80;

/// Size of the BLAKE3 content-checksum trailer appended after the last entry,
/// verified in `open_path` to detect silent corruption of the plaintext cache
/// before it is promoted to the authoritative remote index.
const FULL_CHECKSUM_SIZE: usize = 32;

const FULL_SPEC: CacheSpec = CacheSpec {
    magic: FULL_MAGIC,
    version: FULL_VERSION,
    header_size: FULL_HEADER_SIZE,
    entry_size: FULL_ENTRY_SIZE,
    trailer_size: FULL_CHECKSUM_SIZE,
    label: "full index cache",
};

/// Return the local filesystem path for the full index cache file.
pub fn full_index_cache_path(repo_id: &[u8], cache_dir: Option<&Path>) -> Option<PathBuf> {
    repo_cache_dir(repo_id, cache_dir).map(|d| d.join(FULL_INDEX_CACHE_FILE))
}

/// A single entry read from the full index cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    generation: u64,
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
        Self::open_checked(path, Some(expected_generation))
    }

    /// Open and checksum-verify without a generation check. The cache is
    /// then identified by its whole content checksum, [`Self::state_hash`].
    pub fn open_verified(path: &Path) -> Option<Self> {
        Self::open_checked(path, None)
    }

    fn open_checked(path: &Path, expected_generation: Option<u64>) -> Option<Self> {
        let validated = FULL_SPEC.open_validated(path, expected_generation)?;
        let mmap = validated.mmap;

        // Verify the content checksum over the entry region. This is the
        // integrity gate for these plaintext local bytes, which stand in for
        // the remote index: a bit flip anywhere in the entries changes the
        // digest and misses here. Framing
        // (including the trailer's presence) was already validated by
        // `open_validated`; only this cache has content to verify.
        let entries_end = mmap.len() - FULL_CHECKSUM_SIZE;
        let computed = blake3::hash(&mmap[FULL_HEADER_SIZE..entries_end]);
        if computed.as_bytes()[..] != mmap[entries_end..] {
            debug!("full index cache: checksum mismatch");
            return None;
        }

        Some(Self {
            mmap,
            entry_count: validated.entry_count,
            generation: validated.generation,
        })
    }

    /// Return the number of entries.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Generation from the file header.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// BLAKE3 over the canonical sorted records: the checksum trailer, which
    /// `open_checked` verified against the entries.
    pub fn state_hash(&self) -> StateHash {
        let entries_end = self.mmap.len() - FULL_CHECKSUM_SIZE;
        self.mmap[entries_end..]
            .try_into()
            .expect("32-byte trailer within validated cache")
    }

    /// Read the i-th entry (0-indexed). Panics if `i >= entry_count()`.
    pub(crate) fn entry_at(&self, i: usize) -> FullCacheEntry {
        let data = &self.mmap[FULL_HEADER_SIZE..];
        let offset = i * FULL_ENTRY_SIZE;
        decode_full_entry(&data[offset..offset + FULL_ENTRY_SIZE])
    }

    /// Iterate over all entries in order.
    pub fn iter(&self) -> impl Iterator<Item = FullCacheEntry> + '_ {
        (0..self.entry_count as usize).map(move |i| self.entry_at(i))
    }

    /// Position of `chunk_id` in the sorted entry array, if present.
    pub fn find_position(&self, chunk_id: &ChunkId) -> Option<u32> {
        FULL_SPEC
            .find_entry_with_position(&self.mmap, self.entry_count, chunk_id)
            .map(|(position, _)| position)
    }

    /// Get the ChunkId bytes for the i-th entry (for merge comparisons).
    fn chunk_id_bytes_at(&self, i: usize) -> &[u8] {
        let offset = FULL_HEADER_SIZE + i * FULL_ENTRY_SIZE;
        &self.mmap[offset..offset + 32]
    }
}

/// Identity of an index state: BLAKE3 over its canonical sorted records,
/// i.e. the full index cache's checksum trailer.
pub type StateHash = [u8; 32];

/// The u64 generation that the dedup and restore caches (and the full
/// cache header) carry for an index state: the first 8 bytes of its
/// `state_hash`, with 0 mapped to 1 because generation 0 means "no cache".
pub fn generation_for_state(state_hash: &StateHash) -> u64 {
    let g = read_u64_le(state_hash[..8].try_into().expect("8-byte prefix"));
    if g == 0 { 1 } else { g }
}

/// Size of one canonical record (see [`encode_full_entry`]).
pub const CANONICAL_RECORD_SIZE: usize = FULL_ENTRY_SIZE;

/// Encode a single full cache entry into its 80-byte canonical record.
pub fn encode_full_entry(entry: &FullCacheEntry) -> [u8; FULL_ENTRY_SIZE] {
    let mut buf = [0u8; FULL_ENTRY_SIZE];
    buf[0..32].copy_from_slice(entry.chunk_id.as_bytes());
    buf[32..36].copy_from_slice(&entry.refcount.to_le_bytes());
    buf[36..40].copy_from_slice(&entry.stored_size.to_le_bytes());
    buf[40..72].copy_from_slice(entry.pack_id.as_bytes());
    buf[72..80].copy_from_slice(&entry.pack_offset.to_le_bytes());
    buf
}

/// Decode an 80-byte canonical record. Panics if `buf` is shorter.
fn decode_full_entry(buf: &[u8]) -> FullCacheEntry {
    FullCacheEntry {
        chunk_id: ChunkId::from_bytes(buf[0..32].try_into().expect("32-byte ChunkId")),
        refcount: read_u32_le(buf[32..36].try_into().expect("4-byte refcount")),
        stored_size: read_u32_le(buf[36..40].try_into().expect("4-byte stored_size")),
        pack_id: PackId::from_bytes(buf[40..72].try_into().expect("32-byte PackId")),
        pack_offset: read_u64_le(buf[72..80].try_into().expect("8-byte pack_offset")),
    }
}

/// Streaming writer for a full index cache, into a unique temp file next to
/// its eventual target.
///
/// The entry count and (optionally) the generation need not be known up
/// front: the header is written last, over a placeholder. Entries must be
/// pushed in strictly increasing chunk-ID order; the writer does not check.
pub(crate) struct FullCacheWriter {
    w: BufWriter<NamedTempFile>,
    hasher: blake3::Hasher,
    count: u64,
}

impl FullCacheWriter {
    pub(crate) fn new_beside(target: &Path) -> Result<Self> {
        let mut w = BufWriter::new(temp_file_beside(target)?);
        w.write_all(&[0u8; FULL_HEADER_SIZE])?;
        Ok(Self {
            w,
            hasher: blake3::Hasher::new(),
            count: 0,
        })
    }

    pub(crate) fn push(&mut self, entry: &FullCacheEntry) -> Result<()> {
        let buf = encode_full_entry(entry);
        self.w.write_all(&buf)?;
        self.hasher.update(&buf);
        self.count += 1;
        Ok(())
    }

    /// Append the checksum trailer and write the header. `generation: None`
    /// derives it from the state hash ([`generation_for_state`]).
    pub(crate) fn finish(
        mut self,
        generation: Option<u64>,
    ) -> Result<(NamedTempFile, StateHash, u64)> {
        use std::io::{Seek, SeekFrom};

        let entry_count = u32::try_from(self.count).map_err(|_| {
            vykar_types::error::VykarError::InvalidFormat(format!(
                "index has {} entries, more than a full index cache can hold",
                self.count
            ))
        })?;
        let state_hash: StateHash = self.hasher.finalize().into();
        self.w.write_all(&state_hash)?;
        let generation = generation.unwrap_or_else(|| generation_for_state(&state_hash));
        self.w.seek(SeekFrom::Start(0))?;
        write_cache_header(&mut self.w, &FULL_SPEC, generation, entry_count)?;
        let tmp = into_temp(self.w)?;
        Ok((tmp, state_hash, self.count))
    }
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
    entries.sort_unstable_by_key(|a| *a.chunk_id.as_bytes());

    let mut w = FullCacheWriter::new_beside(path)?;
    for entry in &entries {
        w.push(entry)?;
    }
    let (tmp, _, entry_count) = w.finish(Some(generation))?;
    persist_temp(tmp, path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote full index cache"
    );
    Ok(())
}

/// `index` as a full index cache in a temp file beside `target`, generation
/// derived from its state. Entries at refcount 0 are left out, as a segment
/// diff treats them (see [`crate::index::segment::diff_to_temp`]).
pub(crate) fn full_cache_from_index_to_temp(index: &ChunkIndex, target: &Path) -> Result<TempPath> {
    let mut entries: Vec<(&ChunkId, &crate::index::ChunkIndexEntry)> =
        index.iter().filter(|(_, e)| e.refcount > 0).collect();
    entries.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut w = FullCacheWriter::new_beside(target)?;
    for (id, e) in entries {
        w.push(&FullCacheEntry {
            chunk_id: *id,
            refcount: e.refcount,
            stored_size: e.stored_size,
            pack_id: e.pack_id,
            pack_offset: e.pack_offset,
        })?;
    }
    Ok(w.finish(None)?.0.into_temp_path())
}

/// Two-pointer merge of an existing full cache with a sorted IndexDelta.
/// Produces a new cache file at `out_path` with the given generation.
/// O(buffer) heap — no HashMap materialization in the normal case.
///
/// The delta's positional bumps ([`IndexDelta::cached_bumps`]) index the
/// session's dedup cache, not `old_cache`. They are applied by position when
/// the two agree: same entry count, identical chunk ID at every bumped
/// position, and `old_cache` strictly sorted (so a chunk ID occupies exactly
/// one position). The last two are checked during the merge itself; if either
/// fails the output is discarded and rewritten with the positional bumps
/// resolved to chunk IDs, which is exactly the keyed merge. The result is the
/// same as if every bump had been recorded by key.
pub fn merge_full_index_cache(
    old_cache: &MmapFullIndexCache,
    delta: &IndexDelta,
    new_generation: u64,
    out_path: &Path,
) -> Result<()> {
    let tmp = merge_full_index_cache_to_temp(old_cache, delta, new_generation, out_path)?;
    tmp.persist(out_path).map_err(|e| e.error)?;
    Ok(())
}

/// [`merge_full_index_cache`], leaving the result in a uniquely named temp
/// file beside `target` for the caller to persist (or drop, which deletes it).
pub fn merge_full_index_cache_to_temp(
    old_cache: &MmapFullIndexCache,
    delta: &IndexDelta,
    new_generation: u64,
    target: &Path,
) -> Result<TempPath> {
    // Sort new entries by ChunkId for merge
    let mut sorted_new: Vec<&crate::index::NewChunkEntry> = delta.new_entries.iter().collect();
    sorted_new.sort_unstable_by_key(|a| *a.chunk_id.as_bytes());

    let cached = delta.cached_bumps.as_ref().filter(|c| !c.is_empty());
    let positional = match cached {
        None => Some(CachedBumpSource::None),
        Some(c) if c.len() == old_cache.entry_count() as usize => {
            Some(CachedBumpSource::Positional(c))
        }
        Some(_) => None,
    };
    if let Some(source) = positional
        && let Some(tmp) = write_merged_full_cache(
            old_cache,
            delta,
            &sorted_new,
            &source,
            new_generation,
            target,
        )?
    {
        return Ok(tmp);
    }

    let cached = cached.expect("only positional bumps can fail the merge");
    debug!("full index cache merge: dedup cache positions do not match, merging by chunk ID");
    let mut keyed: HashMap<ChunkId, u32> = HashMap::new();
    for (id, count) in cached.iter_nonzero() {
        *keyed.entry(id).or_insert(0) += count;
    }
    let merged = write_merged_full_cache(
        old_cache,
        delta,
        &sorted_new,
        &CachedBumpSource::Keyed(keyed),
        new_generation,
        target,
    )?;
    Ok(merged.expect("keyed merge cannot be rejected"))
}

/// How [`write_merged_full_cache`] looks up the delta's positional bumps.
enum CachedBumpSource<'a> {
    /// No positional bumps.
    None,
    /// Apply `counts[i]` to `old_cache` entry `i` (validated while merging).
    Positional(&'a crate::index::CachedBumps),
    /// Positional bumps resolved to chunk IDs.
    Keyed(HashMap<ChunkId, u32>),
}

/// One merge pass into a temp file beside `target`. Returns `Ok(None)` —
/// with the temp file deleted — when `CachedBumpSource::Positional` turns
/// out not to line up with `old_cache`.
fn write_merged_full_cache(
    old_cache: &MmapFullIndexCache,
    delta: &IndexDelta,
    sorted_new: &[&crate::index::NewChunkEntry],
    cached: &CachedBumpSource<'_>,
    new_generation: u64,
    target: &Path,
) -> Result<Option<TempPath>> {
    let mut w = FullCacheWriter::new_beside(target)?;

    let keyed_bump = |id: &ChunkId| -> u32 {
        let mut bump = delta.refcount_bumps.get(id).copied().unwrap_or(0);
        if let CachedBumpSource::Keyed(map) = cached {
            bump += map.get(id).copied().unwrap_or(0);
        }
        bump
    };

    // Two-pointer merge
    let mut old_idx: usize = 0;
    let old_count = old_cache.entry_count() as usize;
    let mut new_idx: usize = 0;
    // Positional bump of the last old entry written. New entries sharing its
    // chunk ID get it too, matching what a keyed lookup would return for them.
    let mut last_old_positional: u32 = 0;

    while old_idx < old_count || new_idx < sorted_new.len() {
        let take_old = if old_idx >= old_count {
            false
        } else if new_idx >= sorted_new.len() {
            true
        } else {
            old_cache.chunk_id_bytes_at(old_idx)
                <= sorted_new[new_idx].chunk_id.as_bytes().as_slice()
        };

        if take_old {
            let mut entry = old_cache.entry_at(old_idx);
            // Apply refcount bump if any
            entry.refcount += keyed_bump(&entry.chunk_id);
            last_old_positional = 0;
            if let CachedBumpSource::Positional(bumps) = cached {
                let key = old_cache.chunk_id_bytes_at(old_idx);
                if old_idx > 0 && old_cache.chunk_id_bytes_at(old_idx - 1) >= key {
                    return Ok(None);
                }
                let count = bumps.count_at(old_idx);
                if count != 0 {
                    if bumps.cache().chunk_id_bytes_at(old_idx as u32) != key {
                        return Ok(None);
                    }
                    entry.refcount += count;
                    last_old_positional = count;
                }
            }
            w.push(&entry)?;
            old_idx += 1;
        } else {
            let ne = &sorted_new[new_idx];
            // Apply refcount bump for session-new chunks
            let mut refcount = ne.refcount + keyed_bump(&ne.chunk_id);
            // A positional bump belongs to an old entry; with `<=` above an
            // old entry with the same ID is always written just before.
            if old_idx > 0
                && old_cache.chunk_id_bytes_at(old_idx - 1) == ne.chunk_id.as_bytes().as_slice()
            {
                refcount += last_old_positional;
            }
            let entry = FullCacheEntry {
                chunk_id: ne.chunk_id,
                refcount,
                stored_size: ne.stored_size,
                pack_id: ne.pack_id,
                pack_offset: ne.pack_offset,
            };
            w.push(&entry)?;
            new_idx += 1;
        }
    }
    let (tmp, _, total_count) = w.finish(Some(new_generation))?;

    debug!(
        entries = total_count,
        target = %target.display(),
        "wrote merged full index cache"
    );
    Ok(Some(tmp.into_temp_path()))
}

/// Load a ChunkIndex from an explicit path (used by tests).
pub fn load_chunk_index_from_full_cache_path(path: &Path, generation: u64) -> Result<ChunkIndex> {
    let cache = MmapFullIndexCache::open_path(path, generation).ok_or_else(|| {
        vykar_types::error::VykarError::Other("full index cache not found or stale".into())
    })?;
    Ok(chunk_index_from_full_cache(&cache))
}

/// Build a `ChunkIndex` from an opened (header- and checksum-validated) full index cache.
pub(crate) fn chunk_index_from_full_cache(cache: &MmapFullIndexCache) -> ChunkIndex {
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
    index
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

// ---------------------------------------------------------------------------
// Cache-based dedup/restore cache builders (Step 5)
// ---------------------------------------------------------------------------

/// Build the dedup cache from an opened full index cache (streaming, no
/// HashMap). The derived cache carries the full cache's generation.
pub fn build_dedup_cache_from_full_cache(
    full_cache: &MmapFullIndexCache,
    repo_id: &[u8],
    cache_dir: Option<&Path>,
) -> Result<()> {
    let Some(path) = dedup_cache_path(repo_id, cache_dir) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let generation = full_cache.generation();
    let entry_count = full_cache.entry_count();
    let mut w = BufWriter::new(temp_file_beside(&path)?);

    // Dedup cache header
    w.write_all(MAGIC)?;
    w.write_all(&VERSION.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&entry_count.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved

    // Stream entries: ChunkId(32) + stored_size(4)
    for entry in full_cache.iter() {
        w.write_all(entry.chunk_id.as_bytes())?;
        w.write_all(&entry.stored_size.to_le_bytes())?;
    }

    persist_temp(into_temp(w)?, &path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote dedup cache from full cache"
    );
    Ok(())
}

/// Build the restore cache from an opened full index cache (streaming, no
/// HashMap). The derived cache carries the full cache's generation.
pub fn build_restore_cache_from_full_cache(
    full_cache: &MmapFullIndexCache,
    repo_id: &[u8],
    cache_dir: Option<&Path>,
) -> Result<()> {
    let Some(path) = restore_cache_path(repo_id, cache_dir) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let generation = full_cache.generation();
    let entry_count = full_cache.entry_count();
    let mut w = BufWriter::new(temp_file_beside(&path)?);

    // Restore cache header
    w.write_all(RESTORE_MAGIC)?;
    w.write_all(&RESTORE_VERSION.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&entry_count.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved

    // Stream entries: ChunkId(32) + stored_size(4) + PackId(32) + pack_offset(8)
    for entry in full_cache.iter() {
        w.write_all(entry.chunk_id.as_bytes())?;
        w.write_all(&entry.stored_size.to_le_bytes())?;
        w.write_all(entry.pack_id.as_bytes())?;
        w.write_all(&entry.pack_offset.to_le_bytes())?;
    }

    persist_temp(into_temp(w)?, &path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote restore cache from full cache"
    );
    Ok(())
}

/// The local filesystem path of the raw index blob copy that vykar 0.24 kept
/// for its conditional GET. Only the upgrade removes it.
pub fn index_blob_cache_path(repo_id: &[u8], cache_dir: Option<&Path>) -> Option<PathBuf> {
    repo_cache_dir(repo_id, cache_dir).map(|d| d.join(INDEX_BLOB_CACHE_FILE))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A header whose `entry_count` describes a file that cannot exist must
    /// report no valid size rather than wrapping into a plausible one — on a
    /// 32-bit host the real `DEDUP_SPEC` wraps to 0 at `1 << 30` entries,
    /// which would validate a header-only file and leave every entry slice
    /// out of bounds. Reproduced here with an oversized entry size so the
    /// overflow is exercised on any word size.
    #[test]
    fn framed_size_rejects_overflowing_entry_counts() {
        let spec = CacheSpec {
            entry_size: usize::MAX / 2,
            ..DEDUP_SPEC
        };
        assert_eq!(spec.framed_size(0), Some(HEADER_SIZE));
        assert_eq!(spec.framed_size(1), Some(HEADER_SIZE + usize::MAX / 2));
        assert_eq!(spec.framed_size(2), None, "entries alone overflow");
        assert_eq!(
            DEDUP_SPEC.framed_size(3),
            Some(HEADER_SIZE + 3 * ENTRY_SIZE)
        );
    }

    #[test]
    fn chunk_id_to_u64_extracts_first_8_bytes() {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&42u64.to_le_bytes());
        let id = ChunkId::from_bytes(bytes);
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
        let pack_id = vykar_types::pack_id::PackId::from_bytes([0x01; 32]);

        // Insert some test entries
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId::from_bytes(id_bytes);
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
            let chunk_id = ChunkId::from_bytes(id_bytes);
            assert_eq!(cache.get_stored_size(&chunk_id), Some(100 + i as u32));
        }

        // Look up a non-existent entry
        let missing = ChunkId::from_bytes([0xEE; 32]);
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
        let pack_id = vykar_types::pack_id::PackId::from_bytes([0x01; 32]);
        let chunk_id = ChunkId::from_bytes([0xAA; 32]);
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
        let pack_a = PackId::from_bytes([0x01; 32]);
        let pack_b = PackId::from_bytes([0x02; 32]);

        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId::from_bytes(id_bytes);
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
            let chunk_id = ChunkId::from_bytes(id_bytes);
            let result = cache.lookup(&chunk_id);
            assert!(result.is_some(), "chunk {i} not found in restore cache");
            let (pack_id, pack_offset, stored_size) = result.unwrap();
            let expected_pack = if i < 5 { pack_a } else { pack_b };
            assert_eq!(pack_id, expected_pack);
            assert_eq!(pack_offset, i as u64 * 1000);
            assert_eq!(stored_size, 100 + i as u32);
        }

        // Non-existent entry
        let missing = ChunkId::from_bytes([0xFF; 32]);
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
        let pack_id = PackId::from_bytes([0x01; 32]);
        for i in 0..count {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId::from_bytes(id_bytes);
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
    fn full_index_cache_entry_corruption_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_index_cache");

        let index = make_test_index(5);
        build_full_index_cache_to_path(&index, 42, &path).unwrap();
        assert!(MmapFullIndexCache::open_path(&path, 42).is_some());

        // Flip one byte in the first entry (right after the header).
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[FULL_HEADER_SIZE] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();

        assert!(
            MmapFullIndexCache::open_path(&path, 42).is_none(),
            "a bit flip in the entry region must fail the checksum"
        );
    }

    #[test]
    fn full_index_cache_trailer_corruption_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_index_cache");

        let index = make_test_index(5);
        build_full_index_cache_to_path(&index, 42, &path).unwrap();
        assert!(MmapFullIndexCache::open_path(&path, 42).is_some());

        // Flip one byte in the checksum trailer.
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();

        assert!(
            MmapFullIndexCache::open_path(&path, 42).is_none(),
            "a bit flip in the checksum trailer must fail validation"
        );
    }

    #[test]
    fn full_index_cache_v1_format_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_index_cache");

        // Hand-write a legacy v1 file: header with version 1, entries, no trailer.
        let generation = 42u64;
        let entry_count = 3u32;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(FULL_MAGIC);
        bytes.extend_from_slice(&1u32.to_le_bytes()); // version 1
        bytes.extend_from_slice(&generation.to_le_bytes());
        bytes.extend_from_slice(&entry_count.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes()); // reserved
        for i in 0..entry_count {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i as u8;
            bytes.extend_from_slice(&encode_full_entry(&FullCacheEntry {
                chunk_id: ChunkId::from_bytes(id_bytes),
                refcount: 1,
                stored_size: 100,
                pack_id: PackId::from_bytes([0x01; 32]),
                pack_offset: 0,
            }));
        }
        std::fs::write(&path, &bytes).unwrap();

        assert!(
            MmapFullIndexCache::open_path(&path, generation).is_none(),
            "a v1-format cache (no checksum trailer) must be rejected"
        );
    }

    #[test]
    fn full_index_cache_merge_with_bumps_on_new_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_index_cache");

        // Build initial cache with 5 entries
        let mut index = make_test_index(5);
        let generation = 42u64;
        build_full_index_cache_to_path(&index, generation, &path).unwrap();

        // Create a delta with new entries and refcount bumps
        let mut delta = IndexDelta::new();
        let pack_id = PackId::from_bytes([0x02; 32]);

        // Add 3 new entries
        for i in 10u8..13 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId::from_bytes(id_bytes);
            delta.add_new_entry(chunk_id, 200 + i as u32, pack_id, i as u64 * 200, 1);
        }

        // Bump an existing entry (chunk 0)
        let existing_id = ChunkId::from_bytes([0u8; 32]);
        delta.bump_refcount(&existing_id);
        delta.bump_refcount(&existing_id);

        // Bump a session-new entry (chunk 10)
        let mut new_id_bytes = [0u8; 32];
        new_id_bytes[0] = 10;
        let new_id = ChunkId::from_bytes(new_id_bytes);
        delta.bump_refcount(&new_id);

        // Merge
        let old_cache = MmapFullIndexCache::open_path(&path, generation).unwrap();
        let new_gen = 99u64;
        // Persist only after unmapping: Windows refuses to replace a mapped file.
        let merged = merge_full_index_cache_to_temp(&old_cache, &delta, new_gen, &path).unwrap();
        drop(old_cache);
        merged.persist(&path).unwrap();

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
        let generation = 42u64;
        build_full_index_cache_to_path(&index, generation, &path).unwrap();

        let delta = IndexDelta::new();
        let old_cache = MmapFullIndexCache::open_path(&path, generation).unwrap();
        let new_gen = 100u64;
        // Persist only after unmapping: Windows refuses to replace a mapped file.
        let merged = merge_full_index_cache_to_temp(&old_cache, &delta, new_gen, &path).unwrap();
        drop(old_cache);
        merged.persist(&path).unwrap();

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
        let generation = 42u64;
        build_full_index_cache_to_path(&index, generation, &path).unwrap();

        let cache = MmapFullIndexCache::open_path(&path, generation).unwrap();

        // Serialize via streaming using plaintext engine
        let engine =
            vykar_crypto::PlaintextEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
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
        let generation = 42u64;

        // Build full cache
        build_full_index_cache_to_path(&index, generation, &full_path).unwrap();

        // Build dedup cache directly from HashMap
        build_dedup_cache_to_path(&index, generation, &dedup_path).unwrap();

        // Build dedup cache from full cache
        // We need to set up the path manually since we're testing to explicit paths.
        // Use the full cache to create a dedup cache at the from_full path.
        let cache = MmapFullIndexCache::open_path(&full_path, generation).unwrap();
        let entry_count = cache.entry_count();
        let tmp = dedup_from_full_path.with_extension("tmp");
        let file = std::fs::File::create(&tmp).unwrap();
        let mut w = std::io::BufWriter::new(file);
        w.write_all(MAGIC).unwrap();
        w.write_all(&VERSION.to_le_bytes()).unwrap();
        w.write_all(&generation.to_le_bytes()).unwrap();
        w.write_all(&entry_count.to_le_bytes()).unwrap();
        w.write_all(&0u32.to_le_bytes()).unwrap();
        for entry in cache.iter() {
            w.write_all(entry.chunk_id.as_bytes()).unwrap();
            w.write_all(&entry.stored_size.to_le_bytes()).unwrap();
        }
        w.flush().unwrap();
        drop(w);
        std::fs::rename(&tmp, &dedup_from_full_path).unwrap();

        // Both dedup caches should produce identical results
        let cache1 = MmapDedupCache::open_path(&dedup_path, generation).unwrap();
        let cache2 = MmapDedupCache::open_path(&dedup_from_full_path, generation).unwrap();

        assert_eq!(cache1.entry_count(), cache2.entry_count());
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId::from_bytes(id_bytes);
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
        let generation = 42u64;

        // Build full cache and restore cache from HashMap
        build_full_index_cache_to_path(&index, generation, &full_path).unwrap();
        build_restore_cache_to_path(&index, generation, &restore_path).unwrap();

        // Build restore cache from full cache
        let cache = MmapFullIndexCache::open_path(&full_path, generation).unwrap();
        let entry_count = cache.entry_count();
        let tmp = restore_from_full_path.with_extension("tmp");
        let file = std::fs::File::create(&tmp).unwrap();
        let mut w = std::io::BufWriter::new(file);
        w.write_all(RESTORE_MAGIC).unwrap();
        w.write_all(&RESTORE_VERSION.to_le_bytes()).unwrap();
        w.write_all(&generation.to_le_bytes()).unwrap();
        w.write_all(&entry_count.to_le_bytes()).unwrap();
        w.write_all(&0u32.to_le_bytes()).unwrap();
        for entry in cache.iter() {
            w.write_all(entry.chunk_id.as_bytes()).unwrap();
            w.write_all(&entry.stored_size.to_le_bytes()).unwrap();
            w.write_all(entry.pack_id.as_bytes()).unwrap();
            w.write_all(&entry.pack_offset.to_le_bytes()).unwrap();
        }
        w.flush().unwrap();
        drop(w);
        std::fs::rename(&tmp, &restore_from_full_path).unwrap();

        // Both restore caches should produce identical results
        let cache1 = MmapRestoreCache::open_path(&restore_path, generation).unwrap();
        let cache2 = MmapRestoreCache::open_path(&restore_from_full_path, generation).unwrap();

        assert_eq!(cache1.entry_count(), cache2.entry_count());
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId::from_bytes(id_bytes);
            assert_eq!(cache1.lookup(&chunk_id), cache2.lookup(&chunk_id));
        }
    }

    #[test]
    fn derived_caches_from_one_mapping_match_index_builds() {
        const GEN: u64 = 77;
        let dir = tempfile::tempdir().unwrap();
        let full_path = dir.path().join("full_index_cache");
        let cd = dir.path().join("cache");
        let repo_id = [0x3cu8; 32];

        let mut index = ChunkIndex::new();
        for i in 0u8..6 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i.wrapping_mul(37);
            id_bytes[31] = i;
            index.add(
                ChunkId::from_bytes(id_bytes),
                200 + 13 * i as u32,
                PackId::from_bytes([i + 1; 32]),
                1000 * i as u64 + 7,
            );
        }
        build_full_index_cache_to_path(&index, GEN, &full_path).unwrap();

        let full = MmapFullIndexCache::open_path(&full_path, GEN).unwrap();
        assert_eq!(full.generation(), GEN);
        build_dedup_cache_from_full_cache(&full, &repo_id, Some(&cd)).unwrap();
        build_restore_cache_from_full_cache(&full, &repo_id, Some(&cd)).unwrap();

        let dedup = MmapDedupCache::open(&repo_id, full.generation(), Some(&cd)).unwrap();
        let restore = MmapRestoreCache::open(&repo_id, full.generation(), Some(&cd)).unwrap();
        assert_eq!(dedup.entry_count() as usize, index.len());
        assert_eq!(restore.entry_count() as usize, index.len());
        for (id, e) in index.iter() {
            assert_eq!(dedup.get_stored_size(id), Some(e.stored_size));
            assert_eq!(
                restore.lookup(id),
                Some((e.pack_id, e.pack_offset, e.stored_size))
            );
        }

        assert!(MmapDedupCache::open(&repo_id, GEN + 1, Some(&cd)).is_none());
        assert!(MmapRestoreCache::open(&repo_id, GEN + 1, Some(&cd)).is_none());
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

    fn write_aged(path: &Path, data: &[u8], age: std::time::Duration) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(data).unwrap();
        f.set_modified(std::time::SystemTime::now() - age).unwrap();
    }

    fn write_fresh(path: &Path) {
        std::fs::write(path, b"fresh").unwrap();
    }

    const OLD: std::time::Duration = std::time::Duration::from_secs(25 * 3600);

    const SWEPT_TEMPS: [&str; 4] = [
        ".full_index_cache.AbC123.tmp",
        ".dedup_cache.xYz789.tmp",
        ".restore_cache.q1w2e3.tmp",
        ".index_blob.Zz9Zz9.tmp",
    ];

    fn sweep_cache_dir(dir: &Path) -> TempSweep {
        sweep_stale_temps_in(dir, &CACHE_FILE_NAMES, &LEGACY_TEMP_NAMES, STALE_TEMP_AGE)
    }

    #[test]
    fn sweep_removes_stale_temp_files_and_legacy_names() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = 0u64;
        for (i, name) in SWEPT_TEMPS.iter().chain(&LEGACY_TEMP_NAMES).enumerate() {
            let data = vec![7u8; 10 + i];
            bytes += data.len() as u64;
            write_aged(&dir.path().join(name), &data, OLD);
        }

        let swept = sweep_cache_dir(dir.path());

        assert_eq!(swept, TempSweep { files: 9, bytes });
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn sweep_keeps_fresh_and_future_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        for name in SWEPT_TEMPS.iter().chain(&LEGACY_TEMP_NAMES) {
            write_fresh(&dir.path().join(name));
        }
        let future = dir.path().join(".full_index_cache.Futur1.tmp");
        let f = std::fs::File::create(&future).unwrap();
        f.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(48 * 3600))
            .unwrap();
        drop(f);

        assert_eq!(sweep_cache_dir(dir.path()), TempSweep::default());
        for name in SWEPT_TEMPS.iter().chain(&LEGACY_TEMP_NAMES) {
            assert!(dir.path().join(name).exists(), "{name} was swept");
        }
        assert!(future.exists());
    }

    #[test]
    fn sweep_never_touches_caches_or_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        let names = [
            "full_index_cache",
            "dedup_cache",
            "restore_cache",
            "index_blob",
            "filecache",
            "snapshot_list",
            ".tmpAbC123",
            ".full_index_cache.tmp",
            ".full_index_cache.a.b.tmp",
            ".full_index_cache..tmp",
            ".other_cache.AbC123.tmp",
            "foo.tmp",
            ".cache.AbC123.tmp",
        ];
        for name in names {
            write_aged(&dir.path().join(name), b"keep", OLD);
        }
        let subdir = dir.path().join(".full_index_cache.Dir123.tmp");
        std::fs::create_dir(&subdir).unwrap();
        std::fs::File::open(&subdir)
            .and_then(|f| f.set_modified(std::time::SystemTime::now() - OLD))
            .ok();

        assert_eq!(sweep_cache_dir(dir.path()), TempSweep::default());
        for name in names {
            assert!(dir.path().join(name).exists(), "{name} was swept");
        }
        assert!(subdir.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn sweep_does_not_follow_symlinks() {
        let outside = tempfile::tempdir().unwrap();
        let target = outside.path().join("target");
        write_aged(&target, b"target", OLD);
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join(".full_index_cache.Lnk123.tmp");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        assert_eq!(sweep_cache_dir(dir.path()), TempSweep::default());
        assert!(std::fs::symlink_metadata(&link).is_ok());
        assert!(target.exists());
    }

    #[test]
    fn sweep_of_a_missing_dir_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            sweep_cache_dir(&dir.path().join("absent")),
            TempSweep::default()
        );
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::print_stderr, reason = "reports a skipped test")]
    fn sweep_survives_a_failed_removal() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).unwrap();
        let stale = locked.join(".full_index_cache.AbC123.tmp");
        write_aged(&stale, b"stuck", OLD);
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();

        // Root bypasses directory mode bits; probe instead of checking the uid.
        if std::fs::File::create(locked.join("probe")).is_ok() {
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
            eprintln!("skipping: directory permissions are not enforced for this user");
            return;
        }

        let swept = sweep_cache_dir(&locked);

        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(swept.files, 0);
        assert!(stale.exists());
    }

    #[test]
    fn sweep_of_the_temp_dir_matches_only_the_anchor() {
        let cache = tempfile::tempdir().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let anchor = crate::cache_paths::TEMP_ANCHOR_FILE;
        let stale = tmp.path().join(format!(".{anchor}.AbC123.tmp"));
        let fresh = tmp.path().join(format!(".{anchor}.Fresh1.tmp"));
        let other = tmp.path().join(format!(".{anchor}-other.AbC123.tmp"));
        let cache_temp = tmp.path().join(".full_index_cache.AbC123.tmp");
        write_aged(&stale, b"stale", OLD);
        write_fresh(&fresh);
        write_aged(&other, b"other", OLD);
        write_aged(&cache_temp, b"cache", OLD);

        let swept = sweep_stale_cache_temps_in(Some(cache.path()), Some(tmp.path()));

        assert_eq!(swept, TempSweep { files: 1, bytes: 5 });
        assert!(!stale.exists());
        assert!(fresh.exists());
        assert!(other.exists());
        assert!(
            cache_temp.exists(),
            "cache-dir names are not swept in the temp dir"
        );
    }

    #[test]
    fn temp_dir_sweep_is_rate_limited() {
        let interval = std::time::Duration::from_secs(3600);
        let t0 = std::time::Instant::now();
        let mut last = None;
        assert!(temp_dir_sweep_due(&mut last, t0, interval));
        assert_eq!(last, Some(t0));
        let soon = t0 + std::time::Duration::from_secs(3599);
        assert!(!temp_dir_sweep_due(&mut last, soon, interval));
        assert_eq!(last, Some(t0), "a skipped sweep does not reset the clock");
        let later = t0 + interval;
        assert!(temp_dir_sweep_due(&mut last, later, interval));
        assert_eq!(last, Some(later));
    }

    #[test]
    fn temp_file_beside_names_match_the_sweep() {
        let dir = tempfile::tempdir().unwrap();
        let all: Vec<&str> = CACHE_FILE_NAMES
            .iter()
            .copied()
            .chain([crate::cache_paths::TEMP_ANCHOR_FILE])
            .collect();
        for name in &all {
            let tmp = temp_file_beside(&dir.path().join(name)).unwrap();
            let file_name = tmp.path().file_name().unwrap().to_str().unwrap().to_owned();
            assert!(is_cache_temp_name(&file_name, &[name]), "{file_name}");
            for other in all.iter().filter(|o| *o != name) {
                assert!(
                    !is_cache_temp_name(&file_name, &[other]),
                    "{file_name} matches {other}"
                );
            }
        }
    }

    #[test]
    fn sweep_keeps_a_fresh_writer_temp_that_still_persists() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join(FULL_INDEX_CACHE_FILE);
        let mut w = FullCacheWriter::new_beside(&target).unwrap();
        w.push(&FullCacheEntry {
            chunk_id: ChunkId::from_bytes([1; 32]),
            refcount: 1,
            stored_size: 100,
            pack_id: PackId::from_bytes([2; 32]),
            pack_offset: 0,
        })
        .unwrap();

        assert_eq!(sweep_cache_dir(dir.path()), TempSweep::default());

        let (tmp, _, _) = w.finish(None).unwrap();
        persist_temp(tmp, &target).unwrap();
        assert!(MmapFullIndexCache::open_verified(&target).is_some());
    }
}
