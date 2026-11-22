//! The repository's chunk index: one write-once table file per index state,
//! memory-mapped, plus this process's edits.
//!
//! A [`ChunkTable`] is a verified full index cache (VGFULL, sorted 80-byte
//! canonical records, BLAKE3 trailer = state hash) named after its state,
//! `chunk_table.<64 hex state hash>`, in the per-repo cache dir. It is written
//! once through a unique temp name and published with a no-clobber link, so a
//! table file is never modified, renamed over or replaced, and any number of
//! processes can map it. A table that is not in the cache dir (no cache dir,
//! `check`'s verifying sync, repair states no head names) lives in a temp file
//! that is deleted with it.
//!
//! Rules for the files (two processes sharing one cache dir):
//!
//! - a table is only ever created under a new inode and never written again;
//! - a name that is already taken is adopted when its content verifies, and
//!   replaced only when it does not;
//! - a table is deleted only when it is not the state just installed, no
//!   [`ChunkTable`] in this process maps it, and its mtime is more than
//!   `TABLE_KEEP_AGE` old (`prune_tables`). On Unix a deleted file stays
//!   readable through every mapping; a process that later looks for it finds
//!   it gone and syncs again;
//! - every open verifies the content against the state the name says.
//!
//! [`LiveIndex`] is the in-memory index of a repository: one mapped table and
//! an overlay of edits (positional refcounts, removed positions,
//! relocations, added entries), with the API of [`ChunkIndex`].

// Overlay columns have exactly the table's entry count; every position used
// to index them comes from a lookup in that table or a range over its length.
#![allow(clippy::indexing_slicing)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use tempfile::TempPath;
use tracing::debug;
use xorf::{Filter, Xor8};

use super::dedup_cache::{
    self, FullCacheEntry, FullCacheWriter, MmapFullIndexCache, StateHash, chunk_id_to_u64,
};
use super::hasher::ChunkIdHashMap;
use super::segment::{Diff, RefcountDelta, Relocation, Segment};
use super::{ChunkIndex, ChunkIndexEntry};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// File name prefix of a table: `chunk_table.<64 hex state_hash>`.
pub(crate) const TABLE_PREFIX: &str = "chunk_table.";

/// Target name temp files of a table are created beside
/// (`.chunk_table.<random>.tmp`); see [`table_anchor`].
pub(crate) const TABLE_TEMP_TARGET: &str = "chunk_table";

/// A table file another process may still be about to open is not deleted
/// until it is this old (mtime).
pub(crate) const TABLE_KEEP_AGE: Duration = Duration::from_secs(60 * 60);

/// Path of the table for `state` in `dir`.
pub(crate) fn table_path(dir: &Path, state: &StateHash) -> PathBuf {
    dir.join(format!("{TABLE_PREFIX}{}", hex::encode(state)))
}

/// The path temp files of tables in `dir` are created beside.
pub(crate) fn table_anchor(dir: &Path) -> PathBuf {
    dir.join(TABLE_TEMP_TARGET)
}

/// The state a table file name says, if `name` is one.
fn state_of_name(name: &str) -> Option<StateHash> {
    let hex_part = name.strip_prefix(TABLE_PREFIX)?;
    if hex_part.len() != 64 {
        return None;
    }
    hex::decode(hex_part).ok()?.try_into().ok()
}

/// State hashes and paths of the table files in `dir` (names only,
/// nothing opened). Empty when `dir` cannot be listed.
pub(crate) fn list_tables(dir: &Path) -> Vec<(StateHash, PathBuf)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let state = state_of_name(name.to_str()?)?;
            Some((state, entry.path()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Process-wide registry of mapped table files
// ---------------------------------------------------------------------------

/// Table files mapped by a [`ChunkTable`] of this process, with the number
/// of tables mapping each. [`prune_tables`] leaves them alone, so two
/// repository handles in one process (a daemon, the tests) never delete each
/// other's table, on Windows as on Unix.
static MAPPED: Mutex<Option<HashMap<PathBuf, usize>>> = Mutex::new(None);

fn is_mapped(path: &Path) -> bool {
    MAPPED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .is_some_and(|m| m.contains_key(path))
}

/// A registry entry for one mapped table file; removed on drop.
struct MappedGuard(Option<PathBuf>);

impl MappedGuard {
    fn none() -> Self {
        Self(None)
    }

    fn new(path: &Path) -> Self {
        let mut mapped = MAPPED.lock().unwrap_or_else(PoisonError::into_inner);
        *mapped
            .get_or_insert_with(HashMap::new)
            .entry(path.to_path_buf())
            .or_insert(0) += 1;
        Self(Some(path.to_path_buf()))
    }
}

impl Drop for MappedGuard {
    fn drop(&mut self) {
        let Some(path) = self.0.take() else {
            return;
        };
        let mut mapped = MAPPED.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(map) = mapped.as_mut()
            && let Some(count) = map.get_mut(&path)
        {
            *count -= 1;
            if *count == 0 {
                map.remove(&path);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ChunkTable
// ---------------------------------------------------------------------------

/// Where a table's bytes live.
enum TableBacking {
    /// `chunk_table.<hex>` in the cache dir; never modified, never renamed over.
    File(PathBuf),
    /// A temp file (no cache dir, a sync that ignores local tables, repair
    /// states, or a name that could not be published); deleted when the
    /// table is dropped.
    Scratch(#[allow(dead_code, reason = "held for its Drop, which deletes the file")] TempPath),
}

/// One index state, memory-mapped: a verified full index cache file.
pub struct ChunkTable {
    /// Declared before `backing`: unmapped before a scratch file is deleted.
    cache: MmapFullIndexCache,
    backing: TableBacking,
    filter: OnceLock<Option<Arc<Xor8>>>,
    distinct_packs: OnceLock<usize>,
    _mapped: MappedGuard,
}

impl std::fmt::Debug for ChunkTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkTable")
            .field("entries", &self.cache.entry_count())
            .field("state", &hex::encode(self.cache.state_hash()))
            .field("file", &self.file_path())
            .finish()
    }
}

fn invalid(msg: &str) -> VykarError {
    VykarError::InvalidFormat(msg.into())
}

impl ChunkTable {
    /// Map and verify the table file at `path`; `None` when it is absent,
    /// damaged, or its content is not the state its name says.
    pub(crate) fn open_file(path: &Path) -> Option<Self> {
        let named = path.file_name()?.to_str().and_then(state_of_name)?;
        let cache = MmapFullIndexCache::open_verified(path)?;
        if cache.state_hash() != named {
            debug!(path = %path.display(), "chunk table: content is not the state its name says");
            return None;
        }
        Some(Self::new(cache, TableBacking::File(path.to_path_buf())))
    }

    fn new(cache: MmapFullIndexCache, backing: TableBacking) -> Self {
        let _mapped = match &backing {
            TableBacking::File(path) => MappedGuard::new(path),
            TableBacking::Scratch(_) => MappedGuard::none(),
        };
        Self {
            cache,
            backing,
            filter: OnceLock::new(),
            distinct_packs: OnceLock::new(),
            _mapped,
        }
    }

    /// `tmp` as a scratch table, never published.
    pub(crate) fn scratch(tmp: TempPath) -> Result<Self> {
        let cache = MmapFullIndexCache::open_verified(&tmp)
            .ok_or_else(|| invalid("index state failed verification"))?;
        Ok(Self::new(cache, TableBacking::Scratch(tmp)))
    }

    /// The empty state as a scratch table in a temp file beside `anchor`.
    pub(crate) fn empty(anchor: &Path) -> Result<Self> {
        let tmp = FullCacheWriter::new_beside(anchor)?
            .finish(None)?
            .0
            .into_temp_path();
        Self::scratch(tmp)
    }

    /// Verify `tmp` and, when `dir` is given, publish it as
    /// `table_path(dir, state)` without replacing anything. A table already
    /// under that name is adopted when it verifies (`tmp` is dropped), and
    /// replaced once when it does not. A name that cannot be published keeps
    /// the table as scratch. The mapping is taken from `tmp` before it is
    /// linked, so it never depends on what is at the name.
    pub(crate) fn install(tmp: TempPath, dir: Option<&Path>) -> Result<Self> {
        let cache = MmapFullIndexCache::open_verified(&tmp)
            .ok_or_else(|| invalid("index state failed verification"))?;
        Ok(Self::publish_parts(cache, tmp, dir))
    }

    /// A scratch table published as [`Self::install`] would publish its
    /// temp file, keeping its mapping; a published table is returned as is.
    pub(crate) fn publish(self, dir: Option<&Path>) -> Self {
        let Self {
            cache,
            backing,
            filter,
            distinct_packs,
            _mapped,
        } = self;
        match backing {
            TableBacking::Scratch(tmp) => Self::publish_parts(cache, tmp, dir),
            backing => Self {
                cache,
                backing,
                filter,
                distinct_packs,
                _mapped,
            },
        }
    }

    fn publish_parts(cache: MmapFullIndexCache, tmp: TempPath, dir: Option<&Path>) -> Self {
        let Some(dir) = dir else {
            return Self::new(cache, TableBacking::Scratch(tmp));
        };
        let path = table_path(dir, &cache.state_hash());
        let tmp = match tmp.persist_noclobber(&path) {
            Ok(()) => return Self::new(cache, TableBacking::File(path)),
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Some(existing) = Self::open_file(&path)
                    && existing.cache.entry_count() == cache.entry_count()
                {
                    // Another process (or handle) published this state first.
                    return existing;
                }
                debug!(path = %path.display(), "chunk table: replacing a damaged table");
                if let Err(err) = std::fs::remove_file(&path) {
                    debug!(error = %err, "chunk table: cannot remove the damaged table, keeping a scratch copy");
                    return Self::new(cache, TableBacking::Scratch(e.path));
                }
                e.path.persist_noclobber(&path)
            }
            // No hard links on this filesystem: rename when the name is free.
            Err(e) if !path.exists() => e.path.persist(&path),
            Err(e) => Err(e),
        };
        match tmp {
            Ok(()) => Self::new(cache, TableBacking::File(path)),
            Err(e) => {
                debug!(error = %e.error, "chunk table: keeping the table in its temp file");
                Self::new(cache, TableBacking::Scratch(e.path))
            }
        }
    }

    /// State hash and entry count.
    pub fn state(&self) -> (StateHash, u64) {
        (self.cache.state_hash(), u64::from(self.cache.entry_count()))
    }

    pub fn len(&self) -> usize {
        self.cache.entry_count() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The verified full index cache.
    pub fn cache(&self) -> &MmapFullIndexCache {
        &self.cache
    }

    /// Binary search: position and record.
    pub fn find(&self, id: &ChunkId) -> Option<(u32, FullCacheEntry)> {
        let position = self.cache.find_position(id)?;
        Some((position, self.cache.entry_at(position as usize)))
    }

    /// The record at `position`. Panics when `position >= len()`.
    pub fn entry_at(&self, position: u32) -> FullCacheEntry {
        assert!(
            (position as usize) < self.len(),
            "table position out of range"
        );
        self.cache.entry_at(position as usize)
    }

    /// The 32 chunk ID bytes at `position`. Panics when `position >= len()`.
    pub(crate) fn chunk_id_bytes_at(&self, position: u32) -> &[u8] {
        assert!(
            (position as usize) < self.len(),
            "table position out of range"
        );
        self.cache.chunk_id_bytes_at(position as usize)
    }

    /// The chunk ID at `position`. Panics when `position >= len()`.
    pub(crate) fn chunk_id_at(&self, position: u32) -> ChunkId {
        ChunkId::from_bytes(
            self.chunk_id_bytes_at(position)
                .try_into()
                .expect("32-byte ChunkId slice"),
        )
    }

    /// Xor filter over the first 8 bytes of every chunk ID, built once on
    /// first call; `None` for an empty table or a failed construction.
    pub(crate) fn filter(&self) -> Option<Arc<Xor8>> {
        self.filter
            .get_or_init(|| {
                let keys: Vec<u64> = (0..self.cache.entry_count())
                    .map(|p| {
                        let bytes = self.cache.chunk_id_bytes_at(p as usize);
                        u64::from_le_bytes(bytes[..8].try_into().expect("8-byte prefix"))
                    })
                    .collect();
                dedup_cache::build_xor_filter_from_keys(&keys).map(Arc::new)
            })
            .clone()
    }

    /// Distinct pack IDs over all records, counted once.
    pub(crate) fn distinct_packs(&self) -> usize {
        *self.distinct_packs.get_or_init(|| {
            let mut packs = std::collections::HashSet::new();
            for entry in self.cache.iter() {
                packs.insert(entry.pack_id);
            }
            packs.len()
        })
    }

    /// The table file, when the table is published in a cache dir.
    pub(crate) fn file_path(&self) -> Option<&Path> {
        match &self.backing {
            TableBacking::File(path) => Some(path),
            TableBacking::Scratch(_) => None,
        }
    }
}

/// Best-effort: delete every table file in `dir` whose state is not `keep`,
/// that no table of this process maps, and whose mtime is more than
/// `keep_age` old. Errors (a sharing violation on a file another process
/// maps on Windows, a race with another pruner) are logged at debug and
/// skipped; the next install retries. Returns how many were deleted.
pub(crate) fn prune_tables(dir: &Path, keep: &StateHash, keep_age: Duration) -> usize {
    let mut deleted = 0;
    for (state, path) in list_tables(dir) {
        if state == *keep || is_mapped(&path) {
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.file_type().is_file() {
            continue;
        }
        // A future mtime (clock skew) makes `elapsed` fail: young.
        let old = meta
            .modified()
            .ok()
            .and_then(|m| m.elapsed().ok())
            .is_some_and(|age| age > keep_age);
        if !old {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                deleted += 1;
                debug!(path = %path.display(), "deleted an old chunk table");
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                debug!(path = %path.display(), error = %e, "cannot delete an old chunk table")
            }
        }
    }
    deleted
}

// ---------------------------------------------------------------------------
// Dedup prefilter
// ---------------------------------------------------------------------------

/// What a backup worker learned about a chunk from the [`DedupPrefilter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrefilterVerdict {
    /// Not in the session's table: a filter miss (xor filters have no false
    /// negatives), or a filter false positive the table ruled out. It may
    /// still be a chunk added earlier in this session.
    Absent,
    /// In the session's table at `position` (what the consumer records the
    /// refcount bump by).
    Present { stored_size: u32, position: u32 },
}

/// Read-only dedup state that parallel backup workers can query without the
/// repository: the session's table and its xor filter. Cheap to clone.
///
/// Confirming a filter hit in the worker lets it drop the raw chunk bytes
/// immediately instead of holding them until the consumer resolves the hit.
/// Sound because the table is immutable: a chunk present here is still
/// present when the consumer bumps its refcount (see
/// `Repository::bump_ref_presession_hit`).
#[derive(Clone)]
pub(crate) struct DedupPrefilter {
    filter: Arc<Xor8>,
    table: Arc<ChunkTable>,
}

impl DedupPrefilter {
    /// The prefilter over `table`; `None` when it has no filter.
    pub(crate) fn new(table: &Arc<ChunkTable>) -> Option<Self> {
        Some(Self {
            filter: table.filter()?,
            table: Arc::clone(table),
        })
    }

    /// Classify `id` against the table.
    pub(crate) fn lookup(&self, id: &ChunkId) -> PrefilterVerdict {
        if !self.filter.contains(&chunk_id_to_u64(id)) {
            return PrefilterVerdict::Absent;
        }
        match self.table.find(id) {
            Some((position, entry)) => PrefilterVerdict::Present {
                stored_size: entry.stored_size,
                position,
            },
            None => PrefilterVerdict::Absent,
        }
    }

    /// The table `Present` verdicts come from.
    pub(crate) fn table(&self) -> &Arc<ChunkTable> {
        &self.table
    }
}

// ---------------------------------------------------------------------------
// LiveIndex
// ---------------------------------------------------------------------------

/// This process's edits over a table.
#[derive(Clone, Default)]
struct Overlay {
    /// Refcount per table position, copied from the table on the first
    /// refcount edit (4 B per entry).
    refcounts: Option<Vec<u32>>,
    /// Bitset over table positions; set = removed.
    removed: Option<Vec<u64>>,
    removed_count: usize,
    /// New location (pack, offset, stored size) of a table position.
    relocated: HashMap<u32, (PackId, u64, u32)>,
    /// Entries for chunks absent from the table.
    added: ChunkIdHashMap<ChunkIndexEntry>,
}

impl Overlay {
    fn is_modified(&self) -> bool {
        self.refcounts.is_some()
            || self.removed_count > 0
            || !self.relocated.is_empty()
            || !self.added.is_empty()
    }

    fn is_removed(&self, position: u32) -> bool {
        self.removed
            .as_ref()
            .is_some_and(|bits| bits[position as usize / 64] & (1 << (position % 64)) != 0)
    }
}

/// The repository's chunk index: one mapped table plus this process's edits.
/// No table means "not loaded" and reads as the empty index. Cloning is
/// cheap for an unedited index (an `Arc`), and copies the edits otherwise.
#[derive(Clone, Default)]
pub struct LiveIndex {
    table: Option<Arc<ChunkTable>>,
    overlay: Overlay,
}

impl std::fmt::Debug for LiveIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveIndex")
            .field("table", &self.table)
            .field("modified", &self.overlay.is_modified())
            .field("len", &self.len())
            .finish()
    }
}

/// Where an entry of a [`LiveIndex`] lives.
enum Slot {
    /// A live table position and its record.
    Table(u32, FullCacheEntry),
    /// An entry of `Overlay::added`.
    Added,
}

impl LiveIndex {
    /// The empty, unloaded index.
    pub fn empty() -> Self {
        Self::default()
    }

    /// `table` with no edits.
    pub(crate) fn from_table(table: Arc<ChunkTable>) -> Self {
        Self {
            table: Some(table),
            overlay: Overlay::default(),
        }
    }

    /// The mapped table, when the index is loaded.
    pub fn table(&self) -> Option<&Arc<ChunkTable>> {
        self.table.as_ref()
    }

    pub fn is_loaded(&self) -> bool {
        self.table.is_some()
    }

    /// Whether anything was edited since the table was loaded.
    pub fn is_modified(&self) -> bool {
        self.overlay.is_modified()
    }

    /// Whether a table position was removed since the table was loaded.
    pub(crate) fn has_removed_entries(&self) -> bool {
        self.overlay.removed_count > 0
    }

    /// Drop every edit: the index is its table again.
    pub(crate) fn discard_edits(&mut self) {
        self.overlay = Overlay::default();
    }

    fn slot(&self, id: &ChunkId) -> Option<Slot> {
        if let Some(table) = &self.table
            && let Some((position, record)) = table.find(id)
        {
            return (!self.overlay.is_removed(position)).then_some(Slot::Table(position, record));
        }
        self.overlay.added.contains_key(id).then_some(Slot::Added)
    }

    /// The entry of a live table position.
    fn table_entry(&self, position: u32, record: &FullCacheEntry) -> ChunkIndexEntry {
        let refcount = self
            .overlay
            .refcounts
            .as_ref()
            .map_or(record.refcount, |rc| rc[position as usize]);
        let (pack_id, pack_offset, stored_size) = self
            .overlay
            .relocated
            .get(&position)
            .copied()
            .unwrap_or((record.pack_id, record.pack_offset, record.stored_size));
        ChunkIndexEntry {
            refcount,
            stored_size,
            pack_id,
            pack_offset,
        }
    }

    pub fn contains(&self, id: &ChunkId) -> bool {
        self.slot(id).is_some()
    }

    pub fn get(&self, id: &ChunkId) -> Option<ChunkIndexEntry> {
        match self.slot(id)? {
            Slot::Table(position, record) => Some(self.table_entry(position, &record)),
            Slot::Added => self.overlay.added.get(id).copied(),
        }
    }

    pub fn len(&self) -> usize {
        self.table.as_ref().map_or(0, |t| t.len()) - self.overlay.removed_count
            + self.overlay.added.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every entry, in chunk-ID order.
    pub fn iter(&self) -> LiveIter<'_> {
        let mut added: Vec<(ChunkId, ChunkIndexEntry)> =
            self.overlay.added.iter().map(|(id, e)| (*id, *e)).collect();
        added.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        LiveIter {
            index: self,
            position: 0,
            added: added.into_iter().peekable(),
        }
    }

    /// Distinct pack IDs over all entries; the table's cached count when
    /// nothing was edited.
    pub fn count_distinct_packs(&self) -> usize {
        if !self.is_modified() {
            return self.table.as_ref().map_or(0, |t| t.distinct_packs());
        }
        let mut packs = std::collections::HashSet::new();
        for (_, entry) in self.iter() {
            packs.insert(entry.pack_id);
        }
        packs.len()
    }

    fn refcounts_mut(&mut self) -> &mut Vec<u32> {
        let table = self.table.as_ref();
        self.overlay.refcounts.get_or_insert_with(|| {
            table.map_or_else(Vec::new, |t| t.cache().iter().map(|e| e.refcount).collect())
        })
    }

    fn set_removed(&mut self, position: u32, removed: bool) {
        let len = self.table.as_ref().map_or(0, |t| t.len());
        let bits = self
            .overlay
            .removed
            .get_or_insert_with(|| vec![0u64; len.div_ceil(64)]);
        let word = &mut bits[position as usize / 64];
        let mask = 1u64 << (position % 64);
        let was = *word & mask != 0;
        if removed && !was {
            *word |= mask;
            self.overlay.removed_count += 1;
            self.overlay.relocated.remove(&position);
        } else if !removed && was {
            *word &= !mask;
            self.overlay.removed_count -= 1;
        }
    }

    /// Set the location of a table position, recording a relocation only
    /// when it differs from the record.
    fn set_location(&mut self, position: u32, record: &FullCacheEntry, loc: (PackId, u64, u32)) {
        if loc == (record.pack_id, record.pack_offset, record.stored_size) {
            self.overlay.relocated.remove(&position);
        } else {
            self.overlay.relocated.insert(position, loc);
        }
    }

    /// Add a chunk: a present chunk gets one more reference (location
    /// unchanged), an absent one is inserted with refcount 1.
    pub fn add(&mut self, id: ChunkId, stored_size: u32, pack_id: PackId, pack_offset: u64) {
        if let Some(table) = &self.table
            && let Some((position, record)) = table.find(&id)
        {
            if self.overlay.is_removed(position) {
                self.set_removed(position, false);
                self.refcounts_mut()[position as usize] = 1;
                self.set_location(position, &record, (pack_id, pack_offset, stored_size));
            } else {
                let rc = &mut self.refcounts_mut()[position as usize];
                *rc += 1;
            }
            return;
        }
        self.overlay
            .added
            .entry(id)
            .and_modify(|e| e.refcount += 1)
            .or_insert(ChunkIndexEntry {
                refcount: 1,
                stored_size,
                pack_id,
                pack_offset,
            });
    }

    /// Increment the refcount of a present chunk.
    pub fn increment_refcount(&mut self, id: &ChunkId) {
        self.increment_refcount_by(id, 1);
    }

    /// Increment the refcount of a present chunk by `amount`.
    pub fn increment_refcount_by(&mut self, id: &ChunkId, amount: u32) {
        match self.slot(id) {
            Some(Slot::Table(position, _)) => self.refcounts_mut()[position as usize] += amount,
            Some(Slot::Added) => {
                if let Some(e) = self.overlay.added.get_mut(id) {
                    e.refcount += amount;
                }
            }
            None => {}
        }
    }

    /// Decrement a chunk's refcount; at 0 the chunk is removed. Returns the
    /// new refcount and the stored size, `None` for an absent chunk.
    pub fn decrement(&mut self, id: &ChunkId) -> Option<(u32, u32)> {
        match self.slot(id)? {
            Slot::Table(position, record) => {
                let size = self.table_entry(position, &record).stored_size;
                let rc = {
                    let rc = &mut self.refcounts_mut()[position as usize];
                    *rc = rc.saturating_sub(1);
                    *rc
                };
                if rc == 0 {
                    self.set_removed(position, true);
                }
                Some((rc, size))
            }
            Slot::Added => {
                let e = self.overlay.added.get_mut(id)?;
                e.refcount = e.refcount.saturating_sub(1);
                let (rc, size) = (e.refcount, e.stored_size);
                if rc == 0 {
                    self.overlay.added.remove(id);
                }
                Some((rc, size))
            }
        }
    }

    /// Update the location of a present chunk (compact). `false` when absent.
    pub fn update_location(
        &mut self,
        id: &ChunkId,
        pack_id: PackId,
        pack_offset: u64,
        stored_size: u32,
    ) -> bool {
        match self.slot(id) {
            Some(Slot::Table(position, record)) => {
                self.set_location(position, &record, (pack_id, pack_offset, stored_size));
                true
            }
            Some(Slot::Added) => {
                if let Some(e) = self.overlay.added.get_mut(id) {
                    e.pack_id = pack_id;
                    e.pack_offset = pack_offset;
                    e.stored_size = stored_size;
                }
                true
            }
            None => false,
        }
    }

    /// Remove a chunk; returns its entry when it was present.
    pub fn remove(&mut self, id: &ChunkId) -> Option<ChunkIndexEntry> {
        match self.slot(id)? {
            Slot::Table(position, record) => {
                let entry = self.table_entry(position, &record);
                self.set_removed(position, true);
                Some(entry)
            }
            Slot::Added => self.overlay.added.remove(id),
        }
    }

    /// Live table positions, in order.
    fn live_positions(&self) -> Vec<u32> {
        let len = self.table.as_ref().map_or(0, |t| t.len()) as u32;
        (0..len).filter(|&p| !self.overlay.is_removed(p)).collect()
    }

    /// Remove every entry in `pack_id`; returns how many were removed.
    pub fn remove_by_pack(&mut self, pack_id: &PackId) -> usize {
        let mut removed = 0;
        if let Some(table) = self.table.clone() {
            for position in self.live_positions() {
                let record = table.entry_at(position);
                if self.table_entry(position, &record).pack_id == *pack_id {
                    self.set_removed(position, true);
                    removed += 1;
                }
            }
        }
        let before = self.overlay.added.len();
        self.overlay.added.retain(|_, e| e.pack_id != *pack_id);
        removed + before - self.overlay.added.len()
    }

    /// Replace every refcount from a snapshot-derived map; entries not in
    /// `new_refcounts` are removed.
    pub fn rebuild_refcounts(&mut self, new_refcounts: &std::collections::HashMap<ChunkId, u32>) {
        if let Some(table) = self.table.clone() {
            for position in self.live_positions() {
                match new_refcounts.get(&table.chunk_id_at(position)) {
                    Some(&rc) => self.refcounts_mut()[position as usize] = rc,
                    None => self.set_removed(position, true),
                }
            }
        }
        self.overlay
            .added
            .retain(|id, e| match new_refcounts.get(id) {
                Some(&rc) => {
                    e.refcount = rc;
                    true
                }
                None => false,
            });
    }

    /// A `ChunkIndex` holding the same entries (tests, oracles).
    pub fn to_chunk_index(&self) -> ChunkIndex {
        let mut index = ChunkIndex::with_capacity(self.len());
        for (id, e) in self.iter() {
            index.insert_entry(id, e);
        }
        index
    }
}

impl<'a> IntoIterator for &'a LiveIndex {
    type Item = (ChunkId, ChunkIndexEntry);
    type IntoIter = LiveIter<'a>;

    fn into_iter(self) -> LiveIter<'a> {
        self.iter()
    }
}

/// [`LiveIndex::iter`]: table positions merged with the sorted added entries.
pub struct LiveIter<'a> {
    index: &'a LiveIndex,
    position: u32,
    added: std::iter::Peekable<std::vec::IntoIter<(ChunkId, ChunkIndexEntry)>>,
}

impl Iterator for LiveIter<'_> {
    type Item = (ChunkId, ChunkIndexEntry);

    fn next(&mut self) -> Option<Self::Item> {
        let table = self.index.table.as_deref();
        let len = table.map_or(0, ChunkTable::len) as u32;
        while self.position < len && self.index.overlay.is_removed(self.position) {
            self.position += 1;
        }
        let from_table = (self.position < len).then_some(self.position);
        match (from_table, self.added.peek()) {
            (None, None) => None,
            (Some(p), next_added) => {
                let table = table.expect("position below the table length");
                if let Some((id, _)) = next_added
                    && id.as_bytes().as_slice() < table.chunk_id_bytes_at(p)
                {
                    return self.added.next();
                }
                self.position += 1;
                let record = table.entry_at(p);
                Some((record.chunk_id, self.index.table_entry(p, &record)))
            }
            (None, Some(_)) => self.added.next(),
        }
    }
}

// ---------------------------------------------------------------------------
// Dense positions
// ---------------------------------------------------------------------------

/// Dense positions over the live entries of a [`LiveIndex`], for marks and
/// counts kept in flat arrays. Table entries keep their table position;
/// entries of the overlay's additions follow at `table_len + k`, `k` their
/// rank in chunk-ID order. Valid for the index it borrows (no edit can
/// happen while it is borrowed).
pub(crate) struct IndexPositions<'a> {
    index: &'a LiveIndex,
    table_len: u32,
    /// Chunk ID of an added entry -> its position (`table_len + k`).
    added: ChunkIdHashMap<u32>,
    /// The added entries' chunk IDs, sorted.
    added_ids: Vec<ChunkId>,
}

impl LiveIndex {
    /// Dense positions over this index's live entries.
    pub(crate) fn positions(&self) -> IndexPositions<'_> {
        let table_len = self.table.as_ref().map_or(0, |t| t.len()) as u32;
        let mut added_ids: Vec<ChunkId> = self.overlay.added.keys().copied().collect();
        added_ids.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let mut added = ChunkIdHashMap::default();
        added.reserve(added_ids.len());
        for (k, id) in added_ids.iter().enumerate() {
            added.insert(*id, table_len + k as u32);
        }
        IndexPositions {
            index: self,
            table_len,
            added,
            added_ids,
        }
    }
}

impl IndexPositions<'_> {
    /// Size of the position space (removed table positions included; they are
    /// never returned by `find` or `for_each_live`).
    pub(crate) fn len(&self) -> usize {
        self.table_len as usize + self.added_ids.len()
    }

    /// Position of a live entry; `None` when `id` is absent or removed. A
    /// table hit at a removed position is `None`, as for
    /// [`LiveIndex::contains`]: re-adding a removed chunk clears its removal.
    pub(crate) fn find(&self, id: &ChunkId) -> Option<u32> {
        if let Some(table) = &self.index.table
            && let Some((position, _)) = table.find(id)
        {
            return (!self.index.overlay.is_removed(position)).then_some(position);
        }
        self.added.get(id).copied()
    }

    /// Every live entry with its position, in position order (table first,
    /// then additions in chunk-ID order).
    pub(crate) fn for_each_live(&self, mut f: impl FnMut(u32, ChunkId, ChunkIndexEntry)) {
        if let Some(table) = &self.index.table {
            for position in 0..self.table_len {
                if self.index.overlay.is_removed(position) {
                    continue;
                }
                let record = table.entry_at(position);
                f(
                    position,
                    record.chunk_id,
                    self.index.table_entry(position, &record),
                );
            }
        }
        for (k, id) in self.added_ids.iter().enumerate() {
            if let Some(e) = self.index.overlay.added.get(id) {
                f(self.table_len + k as u32, *id, *e);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Diff against the table
// ---------------------------------------------------------------------------

/// The segment that turns `prev` into `next`, and `next` written as a full
/// index cache into a temp file beside `target`. `next`'s table must be at
/// `prev`'s state (checked, else `InvalidFormat`), so edits are read by
/// position without sorting; an unloaded `next` pairs with an empty `prev`.
///
/// The output (records, deltas, relocations, new entries, state hash) is the
/// one [`super::segment::diff_to_temp`] produces for the same entries,
/// including its refcount-0 rules: entries of `next` at refcount 0 are
/// treated as absent unless `prev` holds them at 0 too, and a `prev` entry at
/// refcount 0 that `next` lacks is an error.
pub fn diff_live_to_temp(
    prev: &MmapFullIndexCache,
    next: &LiveIndex,
    target: &Path,
) -> Result<(Diff, TempPath)> {
    let prev_state = (prev.state_hash(), u64::from(prev.entry_count()));
    match next.table() {
        Some(t) if t.state() == prev_state => {}
        None if prev.entry_count() == 0 => {}
        _ => {
            return Err(invalid(
                "the index is not at the state it is diffed against",
            ));
        }
    }

    let mut added: Vec<(ChunkId, ChunkIndexEntry)> =
        next.overlay.added.iter().map(|(id, e)| (*id, *e)).collect();
    added.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

    let mut deltas = Vec::new();
    let mut relocations = Vec::new();
    let mut new_entries = Vec::new();
    let mut w = FullCacheWriter::new_beside(target)?;
    let mut last: Option<ChunkId> = None;
    let mut j = 0usize;

    let record = |id: &ChunkId, e: &ChunkIndexEntry| FullCacheEntry {
        chunk_id: *id,
        refcount: e.refcount,
        stored_size: e.stored_size,
        pack_id: e.pack_id,
        pack_offset: e.pack_offset,
    };

    for (i, old) in prev.iter().enumerate() {
        if last.is_some_and(|l| l.as_bytes() >= old.chunk_id.as_bytes()) {
            return Err(invalid("predecessor not strictly sorted"));
        }
        last = Some(old.chunk_id);
        while let Some((id, e)) = added.get(j) {
            if id.as_bytes() >= old.chunk_id.as_bytes() {
                break;
            }
            j += 1;
            if e.refcount > 0 {
                let entry = record(id, e);
                w.push(&entry)?;
                new_entries.push(entry);
            }
        }
        let position = i as u32;
        let cur = (!next.overlay.is_removed(position)).then(|| next.table_entry(position, &old));
        match cur.filter(|e| e.refcount > 0 || old.refcount == 0) {
            None => {
                if old.refcount == 0 {
                    return Err(invalid(
                        "predecessor entry with refcount 0 cannot be removed",
                    ));
                }
                deltas.push(RefcountDelta {
                    position,
                    delta: -i64::from(old.refcount),
                });
            }
            Some(e) => {
                if e.refcount != old.refcount {
                    deltas.push(RefcountDelta {
                        position,
                        delta: i64::from(e.refcount) - i64::from(old.refcount),
                    });
                }
                if e.pack_id != old.pack_id
                    || e.pack_offset != old.pack_offset
                    || e.stored_size != old.stored_size
                {
                    relocations.push(Relocation {
                        position,
                        pack_id: e.pack_id,
                        pack_offset: e.pack_offset,
                        stored_size: e.stored_size,
                    });
                }
                w.push(&record(&old.chunk_id, &e))?;
            }
        }
    }
    for (id, e) in &added[j..] {
        if e.refcount > 0 {
            let entry = record(id, e);
            w.push(&entry)?;
            new_entries.push(entry);
        }
    }

    let (tmp, state_hash_after, entries_after) = w.finish(None)?;
    let diff = Diff {
        segment: Segment {
            prev_state_hash: prev.state_hash(),
            prev_entries: u64::from(prev.entry_count()),
            deltas,
            relocations,
            new_entries,
        },
        state_hash_after,
        entries_after,
    };
    Ok((diff, tmp.into_temp_path()))
}

#[cfg(test)]
mod tests;
