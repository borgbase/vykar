//! The repository's chunk index: one write-once table file per base, memory-
//! mapped, the head's segments applied over it in memory, and this
//! process's edits on top.
//!
//! A [`ChunkTable`] is the decoded body of one base object (index base v2,
//! [`super::base_v2`]) in a local file named after that object,
//! `index_base.<64 hex object name>` in the per-repo cache dir:
//!
//! ```text
//! magic "VYKTBL02" | body_len u64 LE | body | BLAKE3(body) [32]
//! ```
//!
//! It is written once through a unique temp name and published with a
//! no-clobber link, so a table file is never modified, renamed over or
//! replaced, and any number of processes can map it. A table that is not in
//! the cache dir (no cache dir, `check`'s verifying sync, a rebuilt or
//! upgraded index no head names yet) lives in a temp file deleted with it.
//! The raw segment objects of the head are cached beside it as
//! `index_segment.<64 hex object name>`, exactly as storage returned them,
//! and are read back only after the name check every `index.d/` read makes.
//!
//! Rules for the files (two processes sharing one cache dir):
//!
//! - a file is only ever created under a new inode and never written again;
//! - a base name that is already taken is adopted when its content
//!   verifies, and replaced only when it does not;
//! - a file is deleted only when the current head does not name it, no
//!   [`ChunkTable`] in this process maps it, and its mtime is more than
//!   `TABLE_KEEP_AGE` old (`prune_local_index_files`). On Unix a deleted
//!   file stays readable through every mapping; a process that later looks
//!   for it finds it gone and fetches the object again;
//! - every open verifies the content (checksum and canonical body), and a
//!   cached segment is checked against its name.
//!
//! [`LiveIndex`] is the in-memory index of a repository: one mapped table,
//! the `applied` overlay (what the head's segments did to it, shared with
//! write sessions and never edited in place) and the copy-on-write `edits`
//! of this process, with the API of [`ChunkIndex`].

// Overlay columns have exactly the table's entry count; every position used
// to index them comes from a lookup in that table or a range over its length.
#![allow(clippy::indexing_slicing)]
// memmap2::Mmap::map of the verified, write-once table files.
#![allow(unsafe_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use memmap2::Mmap;
use tempfile::{NamedTempFile, TempPath};
use tracing::{debug, warn};
use xorf::{Filter, Xor8};

use super::base_v2::{BaseView, BaseWriter, validate_base_body};
use super::dedup_cache::{self, chunk_id_to_u64};
use super::hasher::ChunkIdHashMap;
use super::head::{ObjectName, SegmentKind, SegmentRef};
use super::segment_v2::{Location, SegmentBody};
use super::{ChunkIndex, ChunkIndexEntry, LegacyChunkIndex, LegacyIndexEntry};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// File name prefix of a table: `index_base.<64 hex base object name>`.
pub(crate) const BASE_PREFIX: &str = "index_base.";

/// File name prefix of a cached segment object.
pub(crate) const SEGMENT_PREFIX: &str = "index_segment.";

/// File name prefix of the 80-byte tables of 05 steps 3-8, which no code
/// reads any more; [`prune_local_index_files`] deletes them by age.
pub(crate) const LEGACY_TABLE_PREFIX: &str = "chunk_table.";

/// Target name temp files of a table are created beside
/// (`.index_base.<random>.tmp`); see [`table_anchor`].
pub(crate) const BASE_TEMP_TARGET: &str = "index_base";

/// Target name temp files of a cached segment are created beside.
pub(crate) const SEGMENT_TEMP_TARGET: &str = "index_segment";

/// The temp target of the 80-byte tables (swept like the others).
pub(crate) const TABLE_TEMP_TARGET: &str = "chunk_table";

/// A local index file another process may still be about to open is not
/// deleted until it is this old (mtime).
pub(crate) const TABLE_KEEP_AGE: Duration = Duration::from_secs(60 * 60);

const TABLE_MAGIC: &[u8; 8] = b"VYKTBL02";
const TABLE_HEADER_LEN: usize = 16;
const TABLE_CHECKSUM_LEN: usize = 32;

/// The body of the empty index: a base v2 header with no packs and no records.
static EMPTY_BODY: [u8; super::base_v2::BASE_V2_HEADER_LEN] = [
    super::base_v2::BASE_V2_VERSION,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
];

/// Path of the table of base `name` in `dir`.
pub(crate) fn base_table_path(dir: &Path, name: &ObjectName) -> PathBuf {
    dir.join(format!("{BASE_PREFIX}{}", name.to_hex()))
}

/// Path of the cached segment object `name` in `dir`.
pub(crate) fn segment_cache_path(dir: &Path, name: &ObjectName) -> PathBuf {
    dir.join(format!("{SEGMENT_PREFIX}{}", name.to_hex()))
}

/// The path temp files of tables in `dir` are created beside.
pub(crate) fn table_anchor(dir: &Path) -> PathBuf {
    dir.join(BASE_TEMP_TARGET)
}

/// The object name a file name with `prefix` says, if it is one.
fn name_with_prefix(name: &str, prefix: &str) -> Option<ObjectName> {
    let hex_part = name.strip_prefix(prefix)?;
    if hex_part.len() != 64 {
        return None;
    }
    Some(ObjectName(hex::decode(hex_part).ok()?.try_into().ok()?))
}

/// Base names and paths of the table files in `dir` (names only, nothing
/// opened). Empty when `dir` cannot be listed.
pub(crate) fn list_base_tables(dir: &Path) -> Vec<(ObjectName, PathBuf)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let base = name_with_prefix(name.to_str()?, BASE_PREFIX)?;
            Some((base, entry.path()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Process-wide registry of mapped table files
// ---------------------------------------------------------------------------

/// Table files mapped by a [`ChunkTable`] of this process, with the number
/// of tables mapping each. [`prune_local_index_files`] leaves them alone, so
/// two repository handles in one process (a daemon, the tests) never delete
/// each other's table, on Windows as on Unix.
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
// Table files
// ---------------------------------------------------------------------------

/// Writes a table file (magic, length, body, checksum) into a unique temp
/// file; the body is written through [`Write`] (a [`BaseWriter`], or the
/// decompressed bytes of a base object).
pub(crate) struct TableFileWriter {
    w: BufWriter<NamedTempFile>,
    hasher: blake3::Hasher,
    len: u64,
    /// Refuse bodies longer than this (a decompression bound).
    limit: u64,
}

impl TableFileWriter {
    /// A writer into a temp file beside `anchor`.
    pub(crate) fn new_beside(anchor: &Path) -> Result<Self> {
        Self::with_limit(anchor, u64::MAX)
    }

    /// [`Self::new_beside`], refusing a body longer than `limit` bytes.
    pub(crate) fn with_limit(anchor: &Path, limit: u64) -> Result<Self> {
        let mut w = BufWriter::new(dedup_cache::temp_file_beside(anchor)?);
        w.write_all(&[0u8; TABLE_HEADER_LEN])?;
        Ok(Self {
            w,
            hasher: blake3::Hasher::new(),
            len: 0,
            limit,
        })
    }

    /// Append the checksum and the header; the temp file holds a complete
    /// table file (not yet verified).
    pub(crate) fn finish(mut self) -> Result<TempPath> {
        let checksum = self.hasher.finalize();
        self.w.write_all(checksum.as_bytes())?;
        self.w.seek(SeekFrom::Start(0))?;
        self.w.write_all(TABLE_MAGIC)?;
        self.w.write_all(&self.len.to_le_bytes())?;
        let tmp = self
            .w
            .into_inner()
            .map_err(|e| VykarError::from(e.into_error()))?;
        Ok(tmp.into_temp_path())
    }
}

impl Write for TableFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.len + buf.len() as u64 > self.limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "index base body longer than its head allows",
            ));
        }
        let n = self.w.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.len += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

/// Map and verify a table file: magic, length, checksum and a canonical
/// body. `None` when any of it fails, or the file cannot be opened or
/// mapped.
fn map_verified(path: &Path) -> Option<(Mmap, u32, u32)> {
    map_checked(path).ok().flatten()
}

/// [`map_verified`], telling the two failures apart: an error when the file
/// cannot be opened or mapped (a local I/O error), `Ok(None)` when its
/// bytes fail verification.
fn map_checked(path: &Path) -> std::io::Result<Option<(Mmap, u32, u32)>> {
    let file = std::fs::File::open(path)?;
    // SAFETY: table files are written once under a fresh inode and never
    // modified (module docs); a concurrent truncation would need another
    // writer to violate that rule.
    let mmap = unsafe { Mmap::map(&file) }?;
    Ok(verify_mapped(path, mmap))
}

fn verify_mapped(path: &Path, mmap: Mmap) -> Option<(Mmap, u32, u32)> {
    if mmap.len() < TABLE_HEADER_LEN + TABLE_CHECKSUM_LEN || &mmap[..8] != TABLE_MAGIC {
        debug!(path = %path.display(), "chunk table: bad magic or short file");
        return None;
    }
    let body_len = u64::from_le_bytes(mmap[8..16].try_into().ok()?);
    if body_len != (mmap.len() - TABLE_HEADER_LEN - TABLE_CHECKSUM_LEN) as u64 {
        debug!(path = %path.display(), "chunk table: length mismatch");
        return None;
    }
    let body_end = mmap.len() - TABLE_CHECKSUM_LEN;
    let body = &mmap[TABLE_HEADER_LEN..body_end];
    if blake3::hash(body).as_bytes()[..] != mmap[body_end..] {
        debug!(path = %path.display(), "chunk table: checksum mismatch");
        return None;
    }
    match validate_base_body(body) {
        Ok((packs, entries)) => Some((mmap, packs, entries)),
        Err(e) => {
            debug!(path = %path.display(), "chunk table: {e}");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// ChunkTable
// ---------------------------------------------------------------------------

/// Where a table's bytes live.
enum TableBacking {
    /// `index_base.<hex>` in the cache dir; never modified, never renamed over.
    File(PathBuf),
    /// A temp file (no cache dir, a sync that ignores local files, a state
    /// no head names, or a name that could not be published); deleted when
    /// the table is dropped.
    Scratch(#[allow(dead_code, reason = "held for its Drop, which deletes the file")] TempPath),
    /// The empty table: no file.
    None,
}

/// One entry of a table, its location resolved through the pack table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableEntry {
    pub chunk_id: ChunkId,
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
}

/// One base, memory-mapped: a verified table file.
pub struct ChunkTable {
    /// Declared before `backing`: unmapped before a scratch file is deleted.
    mmap: Option<Mmap>,
    pack_count: u32,
    entry_count: u32,
    /// The base object this is the body of; `None` for the empty table and
    /// for unnamed scratch tables.
    name: Option<ObjectName>,
    backing: TableBacking,
    filter: OnceLock<Option<Arc<Xor8>>>,
    _mapped: MappedGuard,
}

impl std::fmt::Debug for ChunkTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkTable")
            .field("entries", &self.entry_count)
            .field("packs", &self.pack_count)
            .field("base", &self.name.map(ObjectName::to_hex))
            .field("file", &self.file_path())
            .finish()
    }
}

fn invalid(msg: impl Into<String>) -> VykarError {
    VykarError::InvalidFormat(msg.into())
}

impl ChunkTable {
    fn from_parts(
        mmap: Mmap,
        pack_count: u32,
        entry_count: u32,
        name: Option<ObjectName>,
        backing: TableBacking,
    ) -> Self {
        let _mapped = match &backing {
            TableBacking::File(path) => MappedGuard::new(path),
            _ => MappedGuard::none(),
        };
        Self {
            mmap: Some(mmap),
            pack_count,
            entry_count,
            name,
            backing,
            filter: OnceLock::new(),
            _mapped,
        }
    }

    /// Map and verify the table file at `path`; `None` when it is absent,
    /// damaged, or not named `index_base.<hex>`.
    pub(crate) fn open_file(path: &Path) -> Option<Self> {
        let name = path
            .file_name()?
            .to_str()
            .and_then(|n| name_with_prefix(n, BASE_PREFIX))?;
        let (mmap, packs, entries) = map_verified(path)?;
        Some(Self::from_parts(
            mmap,
            packs,
            entries,
            Some(name),
            TableBacking::File(path.to_path_buf()),
        ))
    }

    /// The empty table: no file, no mapping.
    pub(crate) fn empty() -> Self {
        Self {
            mmap: None,
            pack_count: 0,
            entry_count: 0,
            name: None,
            backing: TableBacking::None,
            filter: OnceLock::new(),
            _mapped: MappedGuard::none(),
        }
    }

    /// Verify the table file in `tmp` (a decoded base, a fold, a rebuild)
    /// and, with both a `name` and a `dir`, publish it as
    /// `index_base.<name>` without replacing anything. A table already under
    /// that name is adopted when it verifies and holds as many records (`tmp`
    /// is dropped), and replaced once when it does not. A name that cannot be
    /// published keeps the table as scratch. The mapping is taken from `tmp`
    /// before it is linked, so it never depends on what is at the name.
    /// Fails with `InvalidFormat` when `tmp` fails verification, and with
    /// an I/O error when it cannot be opened or mapped.
    pub(crate) fn install(
        tmp: TempPath,
        name: Option<ObjectName>,
        dir: Option<&Path>,
    ) -> Result<Self> {
        let (mmap, packs, entries) =
            map_checked(&tmp)?.ok_or_else(|| invalid("index state failed verification"))?;
        Ok(match name {
            Some(name) => Self::publish_parts(mmap, packs, entries, name, tmp, dir),
            None => Self::from_parts(mmap, packs, entries, None, TableBacking::Scratch(tmp)),
        })
    }

    /// A scratch table published as base `name` in `dir`, as [`Self::install`]
    /// would publish its temp file, keeping its mapping. A published or empty
    /// table is returned as is, named `name`.
    pub(crate) fn publish(self, name: ObjectName, dir: Option<&Path>) -> Self {
        let Self {
            mmap,
            pack_count,
            entry_count,
            backing,
            filter,
            _mapped,
            ..
        } = self;
        match (backing, mmap) {
            (TableBacking::Scratch(tmp), Some(mmap)) => {
                drop(_mapped);
                Self::publish_parts(mmap, pack_count, entry_count, name, tmp, dir)
            }
            (backing, mmap) => Self {
                mmap,
                pack_count,
                entry_count,
                name: Some(name),
                backing,
                filter,
                _mapped,
            },
        }
    }

    fn publish_parts(
        mmap: Mmap,
        packs: u32,
        entries: u32,
        name: ObjectName,
        tmp: TempPath,
        dir: Option<&Path>,
    ) -> Self {
        let Some(dir) = dir else {
            return Self::from_parts(mmap, packs, entries, Some(name), TableBacking::Scratch(tmp));
        };
        let scratch = |mmap, tmp| {
            Self::from_parts(mmap, packs, entries, Some(name), TableBacking::Scratch(tmp))
        };
        let path = base_table_path(dir, &name);
        let tmp = match tmp.persist_noclobber(&path) {
            Ok(()) => {
                return Self::from_parts(
                    mmap,
                    packs,
                    entries,
                    Some(name),
                    TableBacking::File(path),
                );
            }
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Some(existing) = Self::open_file(&path)
                    && existing.entry_count == entries
                    && existing.pack_count == packs
                {
                    // Another process (or handle) published this base first.
                    return existing;
                }
                debug!(path = %path.display(), "chunk table: replacing a damaged table");
                if let Err(err) = std::fs::remove_file(&path) {
                    debug!(error = %err, "chunk table: cannot remove the damaged table, keeping a scratch copy");
                    return scratch(mmap, e.path);
                }
                e.path.persist_noclobber(&path)
            }
            // No hard links on this filesystem: rename when the name is free.
            Err(e) if !path.exists() => e.path.persist(&path),
            Err(e) => Err(e),
        };
        match tmp {
            Ok(()) => Self::from_parts(mmap, packs, entries, Some(name), TableBacking::File(path)),
            Err(e) => {
                debug!(error = %e.error, "chunk table: keeping the table in its temp file");
                scratch(mmap, e.path)
            }
        }
    }

    /// The decoded base body.
    pub(crate) fn body(&self) -> &[u8] {
        match &self.mmap {
            Some(m) => &m[TABLE_HEADER_LEN..m.len() - TABLE_CHECKSUM_LEN],
            None => &EMPTY_BODY,
        }
    }

    fn view(&self) -> BaseView<'_> {
        BaseView::trusted(self.body(), self.pack_count, self.entry_count)
    }

    /// The base object this table is the body of; `None` for the empty
    /// table and unnamed scratch tables.
    pub fn base_name(&self) -> Option<ObjectName> {
        self.name
    }

    pub fn len(&self) -> usize {
        self.entry_count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    fn entry_of(&self, view: &BaseView<'_>, position: u32) -> TableEntry {
        let (chunk_id, slot, offset, stored_size) = view.record_at(position);
        TableEntry {
            chunk_id,
            stored_size,
            pack_id: view.pack_at(slot).0,
            pack_offset: u64::from(offset),
        }
    }

    /// Binary search: position and record.
    pub fn find(&self, id: &ChunkId) -> Option<(u32, TableEntry)> {
        let view = self.view();
        let position = view.find(id)?;
        Some((position, self.entry_of(&view, position)))
    }

    /// The record at `position`. Panics when `position >= len()`.
    pub fn entry_at(&self, position: u32) -> TableEntry {
        self.entry_of(&self.view(), position)
    }

    /// The 32 chunk ID bytes at `position`. Panics when `position >= len()`.
    pub(crate) fn chunk_id_bytes_at(&self, position: u32) -> &[u8] {
        let view = BaseView::trusted(self.body(), self.pack_count, self.entry_count);
        view.chunk_id_bytes_at(position)
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
                let keys: Vec<u64> = (0..self.entry_count)
                    .map(|p| {
                        let bytes = self.chunk_id_bytes_at(p);
                        u64::from_le_bytes(bytes[..8].try_into().expect("8-byte prefix"))
                    })
                    .collect();
                dedup_cache::build_xor_filter_from_keys(&keys).map(Arc::new)
            })
            .clone()
    }

    /// The pack table's length: no walk over the records.
    pub(crate) fn distinct_packs(&self) -> usize {
        self.pack_count as usize
    }

    /// `trailer_len` of `pack` by binary search in the pack table.
    pub(crate) fn pack_trailer_len(&self, pack: &PackId) -> Option<u32> {
        let view = self.view();
        view.find_pack(pack).map(|slot| view.pack_at(slot).1)
    }

    /// The table file, when the table is published in a cache dir.
    pub(crate) fn file_path(&self) -> Option<&Path> {
        match &self.backing {
            TableBacking::File(path) => Some(path),
            _ => None,
        }
    }
}

/// Best-effort, debug-logged. Deletes from `dir`, when older than
/// `keep_age` and not mapped by this process: every `index_base.*` other
/// than `keep_base`; every `index_segment.*` not in `keep_segments`; every
/// `chunk_table.*` (the 80-byte tables of 05 steps 3-8). Errors (a sharing
/// violation on a file another process maps on Windows, a race with another
/// pruner) are skipped; the next prune retries. Returns how many were
/// deleted.
pub(crate) fn prune_local_index_files(
    dir: &Path,
    keep_base: Option<&ObjectName>,
    keep_segments: &HashSet<ObjectName>,
    keep_age: Duration,
) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut deleted = 0;
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let candidate = if let Some(base) = name_with_prefix(name, BASE_PREFIX) {
            keep_base != Some(&base)
        } else if let Some(seg) = name_with_prefix(name, SEGMENT_PREFIX) {
            !keep_segments.contains(&seg)
        } else {
            name.strip_prefix(LEGACY_TABLE_PREFIX)
                .is_some_and(|h| h.len() == 64)
        };
        let path = entry.path();
        if !candidate || is_mapped(&path) {
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
                debug!(path = %path.display(), "deleted an old local index file");
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                debug!(path = %path.display(), error = %e, "cannot delete an old local index file")
            }
        }
    }
    deleted
}

/// `index` (a test index) as a base body in a table file beside `anchor`,
/// every pack at `trailer_len` 0. Returns the temp file and the record
/// count.
#[cfg(test)]
pub(crate) fn base_body_from_chunk_index(
    index: &ChunkIndex,
    anchor: &Path,
) -> Result<(TempPath, u64)> {
    base_body_from_entries(index.iter().collect(), |e| *e, anchor)
}

/// The live entries of `legacy` (refcount above 0, see
/// [`LegacyChunkIndex::live`]) as a base body, like
/// [`base_body_from_chunk_index`]: what the upgrade writes.
pub(crate) fn base_body_from_legacy(
    legacy: &LegacyChunkIndex,
    anchor: &Path,
) -> Result<(TempPath, u64)> {
    let entries = legacy.iter().filter(|(_, e)| e.refcount > 0).collect();
    base_body_from_entries(
        entries,
        |e: &LegacyIndexEntry| ChunkIndexEntry {
            stored_size: e.stored_size,
            pack_id: e.pack_id,
            pack_offset: e.pack_offset,
        },
        anchor,
    )
}

/// `entries` (borrowed, so the caller's map is not copied) as a base body.
fn base_body_from_entries<E>(
    mut entries: Vec<(&ChunkId, &E)>,
    entry: impl Fn(&E) -> ChunkIndexEntry,
    anchor: &Path,
) -> Result<(TempPath, u64)> {
    entries.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut packs: Vec<PackId> = entries.iter().map(|(_, e)| entry(e).pack_id).collect();
    packs.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    packs.dedup();
    let pack_table: Vec<(PackId, u32)> = packs.iter().map(|p| (*p, 0)).collect();
    let count = u32::try_from(entries.len())
        .map_err(|_| invalid("index has more entries than a base can hold"))?;
    let mut w = BaseWriter::new(TableFileWriter::new_beside(anchor)?, &pack_table, count)?;
    for (id, e) in entries {
        let e = entry(e);
        let slot = packs
            .binary_search_by(|p| p.as_bytes().cmp(e.pack_id.as_bytes()))
            .map_err(|_| invalid("BUG: pack missing from the pack table"))?
            as u32;
        w.push(id, slot, e.pack_offset, e.stored_size)?;
    }
    Ok((w.finish()?.finish()?, u64::from(count)))
}

// ---------------------------------------------------------------------------
// Segment cache
// ---------------------------------------------------------------------------

/// The cached bytes of segment `r`, when `dir` holds them and they match
/// its name and size. A file that does not match is deleted (best-effort).
pub(crate) fn read_cached_segment(dir: &Path, r: &SegmentRef) -> Option<Vec<u8>> {
    let path = segment_cache_path(dir, &r.name);
    let bytes = std::fs::read(&path).ok()?;
    if bytes.len() as u64 == r.size && ObjectName::of(&bytes) == r.name {
        return Some(bytes);
    }
    debug!(path = %path.display(), "cached index segment does not match its name; deleting it");
    let _ = std::fs::remove_file(&path);
    None
}

/// Cache the bytes of segment `name` in `dir` (written once through a temp
/// file, never over an existing file). Best-effort.
pub(crate) fn write_cached_segment(dir: &Path, name: &ObjectName, bytes: &[u8]) {
    let path = segment_cache_path(dir, name);
    if path.exists() {
        return;
    }
    let result = (|| -> Result<()> {
        std::fs::create_dir_all(dir)?;
        let mut tmp = dedup_cache::temp_file_beside(&dir.join(SEGMENT_TEMP_TARGET))?;
        tmp.write_all(bytes)?;
        match tmp.into_temp_path().persist_noclobber(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(e) if !path.exists() => e.path.persist(&path).map_err(|e| e.error.into()),
            Err(e) => Err(e.error.into()),
        }
    })();
    if let Err(e) = result {
        debug!(path = %path.display(), "cannot cache an index segment: {e}");
    }
}

// ---------------------------------------------------------------------------
// Overlay and index state
// ---------------------------------------------------------------------------

/// What the head's segments did to the base table, or this process's edits
/// on top: removed and relocated table positions, entries absent from the
/// table, and trailer lengths of packs the table does not list.
#[derive(Clone, Default)]
struct Overlay {
    /// Bitset over table positions; set = removed.
    removed: Option<Vec<u64>>,
    removed_count: usize,
    /// New location (pack, offset, stored size) of a table position.
    relocated: HashMap<u32, (PackId, u64, u32)>,
    /// Entries for chunks absent from the table.
    added: ChunkIdHashMap<ChunkIndexEntry>,
    /// `trailer_len` of packs the table's pack table does not list (or
    /// lists with a smaller value).
    pack_trailers: HashMap<PackId, u32>,
}

/// Where an entry lives in one layer.
enum Slot {
    /// A live table position and its record.
    Table(u32, TableEntry),
    /// An entry of `Overlay::added`.
    Added,
}

impl Overlay {
    fn is_removed(&self, position: u32) -> bool {
        self.removed
            .as_ref()
            .is_some_and(|bits| bits[position as usize / 64] & (1 << (position % 64)) != 0)
    }

    fn removed_word(&self, word: usize) -> u64 {
        self.removed.as_ref().map_or(0, |bits| bits[word])
    }

    fn slot(&self, table: Option<&ChunkTable>, id: &ChunkId) -> Option<Slot> {
        if let Some(table) = table
            && let Some((position, record)) = table.find(id)
        {
            return (!self.is_removed(position)).then_some(Slot::Table(position, record));
        }
        self.added.contains_key(id).then_some(Slot::Added)
    }

    /// The location of a live table position.
    fn location(&self, position: u32, record: &TableEntry) -> (PackId, u64, u32) {
        self.relocated.get(&position).copied().unwrap_or((
            record.pack_id,
            record.pack_offset,
            record.stored_size,
        ))
    }

    /// The entry of a live table position.
    fn table_entry(&self, position: u32, record: &TableEntry) -> ChunkIndexEntry {
        let (pack_id, pack_offset, stored_size) = self.location(position, record);
        ChunkIndexEntry {
            stored_size,
            pack_id,
            pack_offset,
        }
    }

    fn get(&self, table: Option<&ChunkTable>, id: &ChunkId) -> Option<ChunkIndexEntry> {
        match self.slot(table, id)? {
            Slot::Table(position, record) => Some(self.table_entry(position, &record)),
            Slot::Added => self.added.get(id).copied(),
        }
    }

    fn pack_trailer_len(&self, table: Option<&ChunkTable>, pack: &PackId) -> Option<u32> {
        if let Some(&t) = self.pack_trailers.get(pack) {
            return Some(t);
        }
        table.and_then(|t| t.pack_trailer_len(pack))
    }

    fn set_removed(&mut self, table_len: usize, position: u32, removed: bool) {
        let bits = self
            .removed
            .get_or_insert_with(|| vec![0u64; table_len.div_ceil(64)]);
        let word = &mut bits[position as usize / 64];
        let mask = 1u64 << (position % 64);
        let was = *word & mask != 0;
        if removed && !was {
            *word |= mask;
            self.removed_count += 1;
            self.relocated.remove(&position);
        } else if !removed && was {
            *word &= !mask;
            self.removed_count -= 1;
        }
    }

    /// Set the location of a table position, recording a relocation only
    /// when it differs from the record.
    fn set_location(&mut self, position: u32, record: &TableEntry, loc: (PackId, u64, u32)) {
        if loc == (record.pack_id, record.pack_offset, record.stored_size) {
            self.relocated.remove(&position);
        } else {
            self.relocated.insert(position, loc);
        }
    }

    /// Insert an absent chunk (a removed table position comes back with the
    /// new location).
    fn insert_absent(&mut self, table: Option<&ChunkTable>, id: ChunkId, loc: Location) {
        let tuple = (loc.pack_id, loc.pack_offset, loc.stored_size);
        if let Some(table) = table
            && let Some((position, record)) = table.find(&id)
        {
            self.set_removed(table.len(), position, false);
            self.set_location(position, &record, tuple);
            return;
        }
        self.added.insert(
            id,
            ChunkIndexEntry {
                stored_size: loc.stored_size,
                pack_id: loc.pack_id,
                pack_offset: loc.pack_offset,
            },
        );
    }

    /// Remove a present chunk; its entry, `None` when absent.
    fn remove(&mut self, table: Option<&ChunkTable>, id: &ChunkId) -> Option<ChunkIndexEntry> {
        match self.slot(table, id)? {
            Slot::Table(position, record) => {
                let entry = self.table_entry(position, &record);
                let len = table.map_or(0, ChunkTable::len);
                self.set_removed(len, position, true);
                Some(entry)
            }
            Slot::Added => self.added.remove(id),
        }
    }

    /// Move a present chunk; `false` when absent.
    fn relocate(
        &mut self,
        table: Option<&ChunkTable>,
        id: &ChunkId,
        loc: (PackId, u64, u32),
    ) -> bool {
        match self.slot(table, id) {
            Some(Slot::Table(position, record)) => {
                self.set_location(position, &record, loc);
                true
            }
            Some(Slot::Added) => {
                if let Some(e) = self.added.get_mut(id) {
                    (e.pack_id, e.pack_offset, e.stored_size) = loc;
                }
                true
            }
            None => false,
        }
    }

    /// Record `trailer_len` for `pack` unless the layer already knows a
    /// value at least as large.
    fn note_pack_trailer(&mut self, table: Option<&ChunkTable>, pack: PackId, trailer_len: u32) {
        match self.pack_trailer_len(table, &pack) {
            Some(known) if known >= trailer_len => {}
            _ => {
                self.pack_trailers.insert(pack, trailer_len);
            }
        }
    }
}

/// The objects an index state was built from: what a head names.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct IndexState {
    pub base: Option<ObjectName>,
    pub segments: Vec<ObjectName>,
}

impl IndexState {
    /// The state `head` names.
    pub(crate) fn of_head(head: &super::head::IndexHead) -> Self {
        Self {
            base: head.base.as_ref().map(|b| b.name),
            segments: head.segments.iter().map(|s| s.name).collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// LiveIndex
// ---------------------------------------------------------------------------

/// The repository's chunk index: one mapped table, the head's segments
/// applied over it, and this process's edits. No table means "not loaded"
/// and reads as the empty index. Cloning is cheap for an unedited index
/// (`Arc`s), and copies the edits otherwise.
#[derive(Clone, Default)]
pub struct LiveIndex {
    table: Option<Arc<ChunkTable>>,
    /// The head's state over the table; shared with write sessions, never
    /// edited in place except by [`Self::apply_segment`] (copy-on-write).
    applied: Arc<Overlay>,
    /// Copy-on-write edits since install or the last write; `None` until
    /// the first mutation, which clones `applied`.
    edits: Option<Overlay>,
    state: IndexState,
}

impl std::fmt::Debug for LiveIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveIndex")
            .field("table", &self.table)
            .field("segments", &self.state.segments.len())
            .field("modified", &self.edits.is_some())
            .field("len", &self.len())
            .finish()
    }
}

/// An immutable index state for dedup: what a write session and its
/// workers resolve against.
#[derive(Clone)]
pub(crate) struct IndexView {
    table: Arc<ChunkTable>,
    applied: Arc<Overlay>,
}

impl IndexView {
    /// The table that positional reuse refers to.
    pub(crate) fn table(&self) -> &Arc<ChunkTable> {
        &self.table
    }

    /// Whether `self` and `other` are the same state (by identity).
    pub(crate) fn same_as(&self, other: &IndexView) -> bool {
        Arc::ptr_eq(&self.table, &other.table) && Arc::ptr_eq(&self.applied, &other.applied)
    }

    /// An addition of the view's segments, by key.
    pub(crate) fn find_added(&self, id: &ChunkId) -> Option<u32> {
        self.applied.added.get(id).map(|e| e.stored_size)
    }

    /// A table hit already found at `position`, unless the view's segments
    /// removed it; the current stored size.
    pub(crate) fn table_hit(&self, position: u32, record: &TableEntry) -> Option<u32> {
        (!self.applied.is_removed(position)).then(|| self.applied.location(position, record).2)
    }
}

impl LiveIndex {
    /// The empty, unloaded index.
    pub fn empty() -> Self {
        Self::default()
    }

    /// `table` with nothing applied and no edits; its state is the table's
    /// base.
    pub(crate) fn from_table(table: Arc<ChunkTable>) -> Self {
        let state = IndexState {
            base: table.base_name(),
            segments: Vec::new(),
        };
        Self {
            table: Some(table),
            applied: Arc::default(),
            edits: None,
            state,
        }
    }

    /// Make an unloaded index the loaded empty index (keeping its edits):
    /// what an index over a head with no objects is.
    pub(crate) fn load_empty(&mut self) {
        if self.table.is_none() {
            self.table = Some(Arc::new(ChunkTable::empty()));
        }
    }

    /// The mapped table, when the index is loaded.
    pub fn table(&self) -> Option<&Arc<ChunkTable>> {
        self.table.as_ref()
    }

    pub fn is_loaded(&self) -> bool {
        self.table.is_some()
    }

    /// Whether anything was edited since the index was loaded or written.
    pub fn is_modified(&self) -> bool {
        self.edits.is_some()
    }

    /// Drop every edit: the index is its loaded state again.
    pub(crate) fn discard_edits(&mut self) {
        self.edits = None;
    }

    /// The loaded state without this process's edits (shares the table
    /// and the applied layer).
    pub(crate) fn loaded_state(&self) -> LiveIndex {
        Self {
            table: self.table.clone(),
            applied: Arc::clone(&self.applied),
            edits: None,
            state: self.state.clone(),
        }
    }

    /// The objects this index state was built from.
    pub(crate) fn state(&self) -> &IndexState {
        &self.state
    }

    /// The immutable state for a write session; `None` when not loaded or
    /// edited.
    pub(crate) fn view(&self) -> Option<IndexView> {
        if self.edits.is_some() {
            return None;
        }
        Some(IndexView {
            table: Arc::clone(self.table.as_ref()?),
            applied: Arc::clone(&self.applied),
        })
    }

    fn layer(&self) -> &Overlay {
        self.edits.as_ref().unwrap_or(&self.applied)
    }

    /// The edits layer, created from `applied` on first use.
    fn edit_layer(&mut self) -> (Option<&ChunkTable>, &mut Overlay) {
        let applied = &self.applied;
        let edits = self.edits.get_or_insert_with(|| Overlay::clone(applied));
        (self.table.as_deref(), edits)
    }

    pub fn contains(&self, id: &ChunkId) -> bool {
        self.layer().slot(self.table.as_deref(), id).is_some()
    }

    pub fn get(&self, id: &ChunkId) -> Option<ChunkIndexEntry> {
        self.layer().get(self.table.as_deref(), id)
    }

    pub fn len(&self) -> usize {
        let layer = self.layer();
        self.table.as_ref().map_or(0, |t| t.len()) - layer.removed_count + layer.added.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every entry, in chunk-ID order.
    pub fn iter(&self) -> LiveIter<'_> {
        let mut added: Vec<(ChunkId, ChunkIndexEntry)> =
            self.layer().added.iter().map(|(id, e)| (*id, *e)).collect();
        added.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        LiveIter {
            index: self,
            position: 0,
            added: added.into_iter().peekable(),
        }
    }

    /// Distinct pack IDs, as a cost hint (pack sizing): the table's pack
    /// count plus the packs the current layer's added and relocated entries
    /// use that the table's pack table does not list. Exact when nothing
    /// was removed or moved; otherwise an upper bound (a pack whose entries
    /// were all removed or moved still counts). No walk over the table's
    /// records.
    pub fn count_distinct_packs(&self) -> usize {
        let layer = self.layer();
        let table = self.table.as_deref();
        let in_table = table.map_or(0, ChunkTable::distinct_packs);
        let outside: HashSet<PackId> = layer
            .added
            .values()
            .map(|e| e.pack_id)
            .chain(layer.relocated.values().map(|l| l.0))
            .filter(|p| table.is_none_or(|t| t.pack_trailer_len(p).is_none()))
            .collect();
        in_table + outside.len()
    }

    /// `trailer_len` of `pack`: the current layer, then the table's pack
    /// table.
    pub(crate) fn pack_trailer_len(&self, pack: &PackId) -> Option<u32> {
        self.layer().pack_trailer_len(self.table.as_deref(), pack)
    }

    /// Record the trailer length of a pack this process introduces (an
    /// edit). A smaller value than one already known is ignored.
    pub(crate) fn set_pack_trailer(&mut self, pack: PackId, trailer_len: u32) {
        let (table, layer) = self.edit_layer();
        layer.note_pack_trailer(table, pack, trailer_len);
    }

    /// Add a chunk when it is absent; a present chunk is left as it is
    /// (location unchanged). Either way the edit layer exists afterwards.
    pub fn add(&mut self, id: ChunkId, stored_size: u32, pack_id: PackId, pack_offset: u64) {
        let (table, layer) = self.edit_layer();
        if layer.slot(table, &id).is_some() {
            return;
        }
        layer.insert_absent(
            table,
            id,
            Location {
                pack_id,
                pack_offset,
                stored_size,
            },
        );
    }

    /// Update the location of a present chunk (compact). `false` when absent.
    pub fn update_location(
        &mut self,
        id: &ChunkId,
        pack_id: PackId,
        pack_offset: u64,
        stored_size: u32,
    ) -> bool {
        if !self.contains(id) {
            return false;
        }
        let (table, layer) = self.edit_layer();
        layer.relocate(table, id, (pack_id, pack_offset, stored_size))
    }

    /// Remove a chunk; returns its entry when it was present.
    pub fn remove(&mut self, id: &ChunkId) -> Option<ChunkIndexEntry> {
        if !self.contains(id) {
            return None;
        }
        let (table, layer) = self.edit_layer();
        layer.remove(table, id)
    }

    /// Live table positions of the current layer, in order.
    fn live_positions(&self) -> Vec<u32> {
        let layer = self.layer();
        let len = self.table.as_ref().map_or(0, |t| t.len()) as u32;
        (0..len).filter(|&p| !layer.is_removed(p)).collect()
    }

    /// Remove every entry in `pack_id`; returns how many were removed.
    pub fn remove_by_pack(&mut self, pack_id: &PackId) -> usize {
        let positions = self.live_positions();
        let (table, layer) = self.edit_layer();
        let mut removed = 0;
        if let Some(table) = table {
            for position in positions {
                let record = table.entry_at(position);
                if layer.location(position, &record).0 == *pack_id {
                    layer.set_removed(table.len(), position, true);
                    removed += 1;
                }
            }
        }
        let before = layer.added.len();
        layer.added.retain(|_, e| e.pack_id != *pack_id);
        removed + before - layer.added.len()
    }

    /// A `ChunkIndex` holding the same entries (tests, oracles).
    pub fn to_chunk_index(&self) -> ChunkIndex {
        let mut index = ChunkIndex::with_capacity(self.len());
        for (id, e) in self.iter() {
            index.insert_entry(id, e);
        }
        index
    }

    /// Apply segment `seg` (object `name`) to the applied layer, as a sync
    /// does with the head's segments in order and a backup commit with its
    /// own. `Err("BUG ...")` when there are edits. Removals, then
    /// relocations, then additions:
    /// - removing or relocating an absent chunk is `InvalidFormat`;
    /// - an absent chunk is added;
    /// - adding a present chunk keeps the lower of the two locations by
    ///   (pack ID bytes, offset); the other copy's bytes are dead;
    /// - a pack known with another `trailer_len` takes the larger (logged).
    ///
    /// On error the layer is partly applied: the caller drops the index.
    pub(crate) fn apply_segment(&mut self, name: ObjectName, seg: &SegmentBody) -> Result<()> {
        if self.edits.is_some() {
            return Err(VykarError::Other(
                "BUG: a segment applied over unsaved index edits".into(),
            ));
        }
        let table = self.table.as_deref();
        let layer = Arc::make_mut(&mut self.applied);
        let damaged = |what: &str, id: &ChunkId| {
            invalid(format!(
                "index segment {}: {what} absent chunk {id}",
                name.to_hex()
            ))
        };
        for id in &seg.removals {
            if layer.remove(table, id).is_none() {
                return Err(damaged("removes", id));
            }
        }
        for (id, loc) in &seg.relocations {
            if !layer.relocate(table, id, (loc.pack_id, loc.pack_offset, loc.stored_size)) {
                return Err(damaged("relocates", id));
            }
        }
        for (id, loc) in &seg.additions {
            match layer.get(table, id) {
                None => layer.insert_absent(table, *id, *loc),
                Some(e) => {
                    let lower = (loc.pack_id.as_bytes(), loc.pack_offset)
                        < (e.pack_id.as_bytes(), e.pack_offset);
                    if lower {
                        layer.relocate(table, id, (loc.pack_id, loc.pack_offset, loc.stored_size));
                    }
                }
            }
        }
        for (pack, trailer_len) in &seg.pack_trailers {
            if let Some(known) = layer.pack_trailer_len(table, pack)
                && known != *trailer_len
            {
                warn!(
                    pack = %pack,
                    known,
                    segment = trailer_len,
                    "index segment {} records another trailer length for a pack; using the larger",
                    name.to_hex()
                );
            }
            layer.note_pack_trailer(table, *pack, *trailer_len);
        }
        self.state.segments.push(name);
        Ok(())
    }

    /// The keyed change from the applied layer to the edits (empty without
    /// edits): removals = present in applied, absent now; additions = absent
    /// in applied, present now; relocations = present in both, moved.
    /// `pack_trailers` names every pack of an
    /// addition or relocation; a pack whose trailer length this process does
    /// not know is a bug (an error), so a stored 0 always means "no
    /// trailer". Walks only the overlay structures (the removed bitsets word
    /// by word, the relocated and added maps), never the whole table.
    pub(crate) fn pending_segment(&self, kind: SegmentKind) -> Result<SegmentBody> {
        let mut seg = SegmentBody::empty(kind);
        let Some(edits) = self.edits.as_ref() else {
            return Ok(seg);
        };
        let applied = &*self.applied;
        let table = self.table.as_deref();
        let loc_of = |(pack_id, pack_offset, stored_size): (PackId, u64, u32)| Location {
            pack_id,
            pack_offset,
            stored_size,
        };
        if let Some(table) = table {
            let words = table.len().div_ceil(64);
            for word in 0..words {
                let a = applied.removed_word(word);
                let e = edits.removed_word(word);
                let mut changed = a ^ e;
                while changed != 0 {
                    let bit = changed.trailing_zeros();
                    changed &= changed - 1;
                    let position = (word * 64) as u32 + bit;
                    let record = table.entry_at(position);
                    if e & (1 << bit) != 0 {
                        seg.removals.push(record.chunk_id);
                    } else {
                        seg.additions
                            .push((record.chunk_id, loc_of(edits.location(position, &record))));
                    }
                }
            }
            let moved: std::collections::BTreeSet<u32> = applied
                .relocated
                .keys()
                .chain(edits.relocated.keys())
                .copied()
                .collect();
            for position in moved {
                if applied.is_removed(position) || edits.is_removed(position) {
                    continue;
                }
                let record = table.entry_at(position);
                let now = edits.location(position, &record);
                if applied.location(position, &record) != now {
                    seg.relocations.push((record.chunk_id, loc_of(now)));
                }
            }
        }
        for (id, e) in &edits.added {
            let now = (e.pack_id, e.pack_offset, e.stored_size);
            match applied.added.get(id) {
                None => seg.additions.push((*id, loc_of(now))),
                Some(a) if (a.pack_id, a.pack_offset, a.stored_size) != now => {
                    seg.relocations.push((*id, loc_of(now)));
                }
                Some(_) => {}
            }
        }
        for id in applied.added.keys() {
            if !edits.added.contains_key(id) {
                seg.removals.push(*id);
            }
        }
        seg.additions
            .sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        seg.removals
            .sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        seg.relocations
            .sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let packs: std::collections::BTreeSet<[u8; 32]> = seg
            .additions
            .iter()
            .chain(seg.relocations.iter())
            .map(|(_, l)| *l.pack_id.as_bytes())
            .collect();
        for pack in packs {
            let pack = PackId::from_bytes(pack);
            let trailer_len = edits
                .pack_trailer_len(table, &pack)
                .ok_or_else(|| unknown_trailer(&pack))?;
            seg.pack_trailers.push((pack, trailer_len));
        }
        Ok(seg)
    }

    /// After the head naming `segment` was written: the edits become the
    /// applied layer, so the index equals one loaded from the head.
    pub(crate) fn mark_written(&mut self, segment: ObjectName) {
        if let Some(edits) = self.edits.take() {
            self.applied = Arc::new(edits);
        }
        self.state.segments.push(segment);
    }

    /// Stream the current state as a base body into `w` (a fold): the pack
    /// table first (with every pack's `trailer_len`; an unknown one is a
    /// bug), then the records in chunk-ID order. Returns `w` and the record
    /// count.
    pub(crate) fn write_base_body<W: Write>(&self, w: W) -> Result<(W, u64)> {
        let layer = self.layer();
        let table = self.table.as_deref();
        let mut packs: BTreeMap<[u8; 32], u32> = BTreeMap::new();
        let mut count = 0u32;
        for (_, e) in self.iter() {
            count = count
                .checked_add(1)
                .ok_or_else(|| invalid("index has more entries than a base can hold"))?;
            if let std::collections::btree_map::Entry::Vacant(v) =
                packs.entry(*e.pack_id.as_bytes())
            {
                let trailer_len = layer
                    .pack_trailer_len(table, &e.pack_id)
                    .ok_or_else(|| unknown_trailer(&e.pack_id))?;
                v.insert(trailer_len);
            }
        }
        let pack_table: Vec<(PackId, u32)> = packs
            .into_iter()
            .map(|(p, t)| (PackId::from_bytes(p), t))
            .collect();
        let mut writer = BaseWriter::new(w, &pack_table, count)?;
        for (id, e) in self.iter() {
            let slot = pack_table
                .binary_search_by(|(p, _)| p.as_bytes().cmp(e.pack_id.as_bytes()))
                .map_err(|_| unknown_trailer(&e.pack_id))? as u32;
            writer.push(&id, slot, e.pack_offset, e.stored_size)?;
        }
        Ok((writer.finish()?, u64::from(count)))
    }
}

/// The error for a pack whose `trailer_len` an index write does not know.
fn unknown_trailer(pack: &PackId) -> VykarError {
    VykarError::Other(format!(
        "BUG: the trailer length of pack {pack} is not known to the index write"
    ))
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
        let layer = self.index.layer();
        let len = table.map_or(0, ChunkTable::len) as u32;
        while self.position < len && layer.is_removed(self.position) {
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
                Some((record.chunk_id, layer.table_entry(p, &record)))
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
/// entries outside the table (added by segments or edits) follow at
/// `table_len + k`, `k` their rank in chunk-ID order. Valid for the index
/// it borrows (no edit can happen while it is borrowed).
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
        let mut added_ids: Vec<ChunkId> = self.layer().added.keys().copied().collect();
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

    /// Position of a live entry; `None` when `id` is absent or removed.
    pub(crate) fn find(&self, id: &ChunkId) -> Option<u32> {
        if let Some(table) = &self.index.table
            && let Some((position, _)) = table.find(id)
        {
            return (!self.index.layer().is_removed(position)).then_some(position);
        }
        self.added.get(id).copied()
    }

    /// Every live entry with its position, in position order (table first,
    /// then additions in chunk-ID order).
    pub(crate) fn for_each_live(&self, mut f: impl FnMut(u32, ChunkId, ChunkIndexEntry)) {
        let layer = self.index.layer();
        if let Some(table) = &self.index.table {
            for position in 0..self.table_len {
                if layer.is_removed(position) {
                    continue;
                }
                let record = table.entry_at(position);
                f(
                    position,
                    record.chunk_id,
                    layer.table_entry(position, &record),
                );
            }
        }
        for (k, id) in self.added_ids.iter().enumerate() {
            if let Some(e) = layer.added.get(id) {
                f(self.table_len + k as u32, *id, *e);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Dedup prefilter
// ---------------------------------------------------------------------------

/// What a backup worker learned about a chunk from the [`DedupPrefilter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrefilterVerdict {
    /// Not in the session's view: neither added by its segments nor in its
    /// table (xor filters have no false negatives; a false positive is ruled
    /// out in the table). It may still be a chunk added earlier in this
    /// session.
    Absent,
    /// In the view's table at `position` and not removed by its segments
    /// (what the consumer records the reuse by).
    Present { stored_size: u32, position: u32 },
    /// Added by a segment of the view (not in the table): reuse is recorded
    /// by key.
    PresentAdded { stored_size: u32 },
}

/// Read-only dedup state that parallel backup workers can query without the
/// repository: the session's view and its table's xor filter. Cheap to
/// clone.
///
/// Confirming a hit in the worker lets it drop the raw chunk bytes
/// immediately instead of holding them until the consumer resolves the hit.
/// Sound because the view is immutable: a chunk present here is still
/// present when the consumer records its reuse (see
/// `Repository::bump_ref_presession_hit`).
#[derive(Clone)]
pub(crate) struct DedupPrefilter {
    filter: Option<Arc<Xor8>>,
    view: IndexView,
}

impl DedupPrefilter {
    /// The prefilter over `view` (an empty table has no filter; its lookups
    /// go through the additions only).
    pub(crate) fn new(view: IndexView) -> Self {
        Self {
            filter: view.table.filter(),
            view,
        }
    }

    /// Classify `id` against the view: its additions, then the filter, then
    /// the table.
    pub(crate) fn lookup(&self, id: &ChunkId) -> PrefilterVerdict {
        if let Some(stored_size) = self.view.find_added(id) {
            return PrefilterVerdict::PresentAdded { stored_size };
        }
        if let Some(filter) = &self.filter
            && !filter.contains(&chunk_id_to_u64(id))
        {
            return PrefilterVerdict::Absent;
        }
        match self.view.table.find(id) {
            Some((position, record)) => match self.view.table_hit(position, &record) {
                Some(stored_size) => PrefilterVerdict::Present {
                    stored_size,
                    position,
                },
                None => PrefilterVerdict::Absent,
            },
            None => PrefilterVerdict::Absent,
        }
    }

    /// The view the verdicts come from.
    pub(crate) fn view(&self) -> &IndexView {
        &self.view
    }
}

#[cfg(test)]
mod tests;
