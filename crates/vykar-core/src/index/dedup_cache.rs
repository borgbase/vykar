// memmap2::Mmap::map for read-only cache files; SAFETY documented per block.
#![allow(unsafe_code)]
// Wire-format reads against header-validated cache files. `open_validated`
// checks `mmap.len() >= header_size` and requires the file length to equal
// the (overflow-checked) size the header's `entry_count` implies, so entry
// slicing is in bounds for *any* input that reaches a reader, corrupt or
// not — a header that doesn't add up is a cache miss (`None`) instead.
#![allow(clippy::indexing_slicing)]

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use tempfile::{NamedTempFile, TempPath};
use tracing::{debug, info, warn};
use xorf::Xor8;

use crate::cache_paths::repo_cache_dir;
use crate::index::{ChunkIndex, ChunkIndexEntry};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;
use vykar_types::pack_id::PackId;

fn read_u32_le(bytes: [u8; 4]) -> u32 {
    u32::from_le_bytes(bytes)
}

fn read_u64_le(bytes: [u8; 8]) -> u64 {
    u64::from_le_bytes(bytes)
}

/// Fixed names of the local index caches that binaries before the
/// state-named chunk table kept in the per-repo cache dir. Nothing reads
/// them any more; they are deleted at open (see
/// [`sweep_stale_cache_temps`]).
pub(crate) const DEDUP_CACHE_FILE: &str = "dedup_cache";
pub(crate) const RESTORE_CACHE_FILE: &str = "restore_cache";
pub(crate) const FULL_INDEX_CACHE_FILE: &str = "full_index_cache";
/// The raw index blob copy vykar 0.24 kept; only the upgrade removes it.
pub(crate) const INDEX_BLOB_CACHE_FILE: &str = "index_blob";

/// Legacy cache files deleted at open whatever their age.
const LEGACY_CACHE_FILES: [&str; 3] = [DEDUP_CACHE_FILE, RESTORE_CACHE_FILE, FULL_INDEX_CACHE_FILE];

/// Targets `temp_file_beside` writes beside in the per-repo cache dir (the
/// legacy names too, for temps an older binary left behind).
const CACHE_FILE_NAMES: [&str; 7] = [
    DEDUP_CACHE_FILE,
    RESTORE_CACHE_FILE,
    FULL_INDEX_CACHE_FILE,
    INDEX_BLOB_CACHE_FILE,
    crate::index::table::TABLE_TEMP_TARGET,
    crate::index::table::BASE_TEMP_TARGET,
    crate::index::table::SEGMENT_TEMP_TARGET,
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
pub(crate) fn temp_file_beside(target: &Path) -> Result<NamedTempFile> {
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

/// Delete the legacy fixed-name caches in `dir` (see [`LEGACY_CACHE_FILES`]).
fn delete_legacy_caches_in(dir: &Path) -> TempSweep {
    let mut swept = TempSweep::default();
    for name in LEGACY_CACHE_FILES {
        let path = dir.join(name);
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.file_type().is_file() {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                swept.files += 1;
                swept.bytes += meta.len();
                debug!(path = %path.display(), "removed a legacy local index cache");
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => debug!(path = %path.display(), error = %e, "failed to remove a legacy cache"),
        }
    }
    swept
}

/// Sweep the leftovers of interrupted local cache writes in a repository's
/// cache dir and, when given, the system temp dir, and delete the legacy
/// fixed-name caches of the cache dir. The temp dir holds the temp-anchor
/// files of every sync that bypasses the local cache (`check`,
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
        swept.add(delete_legacy_caches_in(dir));
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
    /// Prefix used in the `debug!` miss messages, e.g. `"full index cache"`.
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
        // exception: each one builds a sibling temp file and renames or links
        // it into place, which never touches an inode a mapping holds. Any
        // writer added here must do the same. Today that is `FullCacheWriter`
        // (every full index cache writer, including the segment apply and
        // diff in `index::segment` and the table diff in `index::table`),
        // published by `ChunkTable::install` with a no-clobber link under a
        // name no file has (a damaged table under the name is unlinked
        // first, never written through). A table file is therefore never
        // modified while any process maps it. A finished temp file may also be
        // mapped before it is linked (`ChunkTable::install`); linking does not
        // modify it.
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

    /// Binary search the sorted entry array for `chunk_id`: the entry's index
    /// and its full byte slice (the 32-byte `ChunkId` prefix included).
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
    #[allow(dead_code, reason = "v1 index code, deleted in 05 step 10")]
    pub(crate) fn chunk_id_bytes_at(&self, i: usize) -> &[u8] {
        let offset = FULL_HEADER_SIZE + i * FULL_ENTRY_SIZE;
        &self.mmap[offset..offset + 32]
    }
}

/// Identity of an index state: BLAKE3 over its canonical sorted records,
/// i.e. the full index cache's checksum trailer.
pub type StateHash = [u8; 32];

/// The u64 generation that the full cache header carries for an index
/// state: the first 8 bytes of its
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

/// Build a full index cache from a ChunkIndex HashMap at an explicit path
/// (tests and the fuzz corpus generator).
#[doc(hidden)]
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
#[allow(dead_code, reason = "v1 index code, deleted in 05 step 10")]
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
/// Build a `ChunkIndex` from an opened (header- and checksum-validated) full
/// index cache (tests). A refcount-0 entry loads as refcount 1.
#[cfg(test)]
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
    /// 32-bit host a 36-byte entry size wraps to 0 at `1 << 30` entries,
    /// which would validate a header-only file and leave every entry slice
    /// out of bounds. Reproduced here with an oversized entry size so the
    /// overflow is exercised on any word size.
    #[test]
    fn framed_size_rejects_overflowing_entry_counts() {
        let spec = CacheSpec {
            entry_size: usize::MAX / 4,
            ..FULL_SPEC
        };
        let framing = FULL_HEADER_SIZE + FULL_CHECKSUM_SIZE;
        assert_eq!(spec.framed_size(0), Some(framing));
        assert_eq!(spec.framed_size(1), Some(framing + usize::MAX / 4));
        assert_eq!(spec.framed_size(5), None, "entries alone overflow");
        assert_eq!(
            FULL_SPEC.framed_size(3),
            Some(framing + 3 * FULL_ENTRY_SIZE)
        );
    }

    #[test]
    fn chunk_id_to_u64_extracts_first_8_bytes() {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&42u64.to_le_bytes());
        let id = ChunkId::from_bytes(bytes);
        assert_eq!(chunk_id_to_u64(&id), 42);
    }

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

    const SWEPT_TEMPS: [&str; 5] = [
        ".full_index_cache.AbC123.tmp",
        ".dedup_cache.xYz789.tmp",
        ".restore_cache.q1w2e3.tmp",
        ".index_blob.Zz9Zz9.tmp",
        ".chunk_table.Ab12Cd.tmp",
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

        assert_eq!(swept, TempSweep { files: 10, bytes });
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
        let table = format!("chunk_table.{}", "ab".repeat(32));
        let names = [
            "full_index_cache",
            "dedup_cache",
            "restore_cache",
            "index_blob",
            "filecache",
            "snapshot_list",
            table.as_str(),
            ".tmpAbC123",
            ".full_index_cache.tmp",
            ".full_index_cache.a.b.tmp",
            ".full_index_cache..tmp",
            ".chunk_table.tmp",
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

    /// The open-time sweep deletes the legacy fixed-name caches at any age,
    /// and never a published chunk table or the upgrade's index blob.
    #[test]
    fn open_sweep_deletes_legacy_caches_but_not_tables() {
        let dir = tempfile::tempdir().unwrap();
        for name in LEGACY_CACHE_FILES {
            write_fresh(&dir.path().join(name));
        }
        let table = dir.path().join(format!("chunk_table.{}", "cd".repeat(32)));
        write_aged(&table, b"table", OLD);
        write_aged(&dir.path().join(INDEX_BLOB_CACHE_FILE), b"blob", OLD);

        let swept = sweep_stale_cache_temps_in(Some(dir.path()), None);

        assert_eq!(swept.files, 3);
        for name in LEGACY_CACHE_FILES {
            assert!(!dir.path().join(name).exists(), "{name} was kept");
        }
        assert!(table.exists());
        assert!(dir.path().join(INDEX_BLOB_CACHE_FILE).exists());
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
