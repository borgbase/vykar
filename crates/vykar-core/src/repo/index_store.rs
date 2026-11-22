//! Storage side of the index (formats v6/v7).
//!
//! ```text
//! index.h/<gen>        # write-once: the IndexHead of generation <gen>; the highest wins
//! index.d/<64 hex>     # write-once: base, segment or sweep record, name = BLAKE3 of the stored bytes
//! ```
//!
//! Each index write puts a new head one generation above the one it read.
//! Old heads and the objects no kept head names are deleted by age
//! ([`collect_index_garbage`]), never by name alone, so a concurrent
//! writer's fresh object survives.
//!
//! Every read of an `index.d/` object checks `BLAKE3(bytes) == name` and the
//! size the head records before anything else. The AEAD contexts are static,
//! because the name is the hash of the ciphertext and cannot be in its own
//! AAD; the binding to the head is that check.
//!
//! [`sync_full_cache`] brings a local chunk table (`index::table`) to a
//! head's state: nothing to do when one already matches, the missing
//! segments applied in one streaming pass when one matches a state in the
//! head's chain, the base downloaded otherwise.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempPath;
use tracing::debug;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};

use super::format::{
    ObjectType, pack_object_streaming_with_context, pack_object_with_context,
    unpack_object_expect_with_context, unpack_object_expect_with_context_in_place,
};
use crate::cache_paths::TEMP_ANCHOR_FILE;
use crate::compress;
use crate::index::base;
use crate::index::dedup_cache::{FullCacheWriter, MmapFullIndexCache, StateHash};
use crate::index::head::{
    self, BaseRef, IndexHead, ObjectName, ObjectRef, SegmentKind, SegmentRef,
};
use crate::index::segment::{self, ChainStep, Segment};
use crate::index::sweep_record::SweepRecord;
use crate::index::table::{self, ChunkTable};

/// Directory of the write-once base and segment objects.
pub(crate) const INDEX_OBJECT_DIR: &str = "index.d/";

pub(crate) const BASE_CONTEXT: &[u8] = b"index.d/base";
pub(crate) const SEGMENT_CONTEXT: &[u8] = b"index.d/segment";

/// Directory of the heads, one per generation.
pub(crate) const HEAD_PREFIX: &str = "index.h/";
pub(crate) const HEAD_CONTEXT: &[u8] = b"index.h";
pub(crate) const SWEEP_RECORD_CONTEXT: &[u8] = b"index.d/sweep";

/// How long an unreferenced index object or an old head is kept, by the
/// storage's clock (04's T_idx).
pub(crate) const INDEX_GC_GRACE: std::time::Duration = std::time::Duration::from_secs(3600);

/// Heads kept below the latest regardless of age (04: latest − 16).
pub(crate) const HEADS_KEPT: u64 = 16;

/// How often a reader retries on a re-read head after a named object
/// vanished and the re-read head no longer names it (a fold moved it),
/// before giving up.
const MAX_HEAD_RETRIES: usize = 3;

/// Storage key of an `index.d/` object.
pub(crate) fn object_key(name: &ObjectName) -> String {
    format!("{INDEX_OBJECT_DIR}{}", name.to_hex())
}

fn invalid(msg: impl Into<String>) -> VykarError {
    VykarError::InvalidFormat(msg.into())
}

// ---------------------------------------------------------------------------
// Head
// ---------------------------------------------------------------------------

/// Storage key of the head of generation `generation`.
pub(crate) fn head_key(generation: u64) -> String {
    vykar_protocol::head_key(generation)
}

/// Generations under `index.h/`, ascending. Keys that are not canonical
/// generations (foreign files) are skipped.
pub(crate) fn list_head_generations(storage: &dyn StorageBackend) -> Result<Vec<u64>> {
    let mut gens: Vec<u64> = storage
        .list(HEAD_PREFIX)?
        .iter()
        .filter_map(|k| vykar_protocol::head_generation(k))
        .collect();
    gens.sort_unstable();
    gens.dedup();
    Ok(gens)
}

/// LIST `index.h/`, then GET the highest generation. A 404 on that GET
/// (the head was deleted between the two) re-lists and retries, at most
/// [`MAX_HEAD_RETRIES`] times, then [`VykarError::IndexHeadUnstable`].
/// `Ok(None)` when no head is listed.
pub(crate) fn fetch_latest_head_bytes(
    storage: &dyn StorageBackend,
) -> Result<Option<(u64, Vec<u8>)>> {
    let mut retries = 0;
    loop {
        let Some(&generation) = list_head_generations(storage)?.last() else {
            return Ok(None);
        };
        if let Some(bytes) = storage.get(&head_key(generation))? {
            return Ok(Some((generation, bytes)));
        }
        if retries == MAX_HEAD_RETRIES {
            return Err(VykarError::IndexHeadUnstable);
        }
        retries += 1;
        debug!(
            generation,
            attempt = retries,
            "listed index head vanished; re-listing"
        );
    }
}

/// Serialize and encrypt a head for its `index.h/<gen>` key.
pub(crate) fn pack_head(head: &IndexHead, crypto: &dyn CryptoEngine) -> Result<Vec<u8>> {
    pack_object_with_context(ObjectType::IndexHead, HEAD_CONTEXT, &head.encode()?, crypto)
}

/// PUT the head at `head_key(head.generation)`: a plain put under the lock (04
/// step 8 makes it put-if-absent).
pub(crate) fn put_head(
    storage: &dyn StorageBackend,
    head: &IndexHead,
    crypto: &dyn CryptoEngine,
) -> Result<()> {
    storage.put(&head_key(head.generation), &pack_head(head, crypto)?)
}

/// What the object at `index.h/<gen>` is.
#[derive(Debug)]
pub(crate) enum StoredHead {
    Head(IndexHead),
    /// Decrypts, and names a head layout this binary does not read.
    UnknownLayout(u32),
    /// Layout 2, but names features this binary does not know.
    UnknownFeatures(Vec<String>),
    /// An object type tag this binary does not know (a newer writer).
    UnknownType(u8),
    /// Fails to decrypt, decode or validate; the original error.
    Unreadable(VykarError),
}

/// Classify the bytes stored at `head_key(generation)`. A head whose `generation` field
/// differs from `generation` is unreadable. No storage access.
pub(crate) fn classify_head(generation: u64, data: &[u8], crypto: &dyn CryptoEngine) -> StoredHead {
    let plain = match unpack_object_expect_with_context(
        data,
        ObjectType::IndexHead,
        HEAD_CONTEXT,
        crypto,
    ) {
        Ok(plain) => plain,
        Err(VykarError::UnknownObjectType(tag)) => return StoredHead::UnknownType(tag),
        Err(e) => return StoredHead::Unreadable(e),
    };
    let decoded = match IndexHead::decode_unchecked(&plain) {
        Ok(h) => h,
        Err(e) => {
            return match head::peek_layout(&plain) {
                Some(layout) if layout != head::HEAD_LAYOUT_VERSION => {
                    StoredHead::UnknownLayout(layout)
                }
                _ => StoredHead::Unreadable(e),
            };
        }
    };
    if decoded.layout != head::HEAD_LAYOUT_VERSION {
        return StoredHead::UnknownLayout(decoded.layout);
    }
    let unknown = decoded.unknown_features();
    if !unknown.is_empty() {
        return StoredHead::UnknownFeatures(unknown);
    }
    if let Err(e) = decoded.validate() {
        return StoredHead::Unreadable(e);
    }
    if decoded.generation != generation {
        return StoredHead::Unreadable(invalid(format!(
            "index head at {} names generation {}",
            head_key(generation),
            decoded.generation
        )));
    }
    StoredHead::Head(decoded)
}

impl StoredHead {
    /// The head, or the error a reader returns for this object.
    pub(crate) fn into_head(self) -> Result<IndexHead> {
        match self {
            Self::Head(head) => Ok(head),
            Self::UnknownLayout(layout) => Err(head::unsupported_layout(layout)),
            Self::UnknownFeatures(names) => Err(head::unknown_features(&names)),
            Self::UnknownType(tag) => Err(VykarError::UnknownObjectType(tag)),
            Self::Unreadable(e) => Err(e),
        }
    }
}

/// LIST and GET the latest head and decode it. A v6 repository always has
/// one (`init` writes `index.h/1`).
pub(crate) fn fetch_head(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
) -> Result<IndexHead> {
    let (generation, data) =
        fetch_latest_head_bytes(storage)?.ok_or_else(|| invalid("index head missing"))?;
    classify_head(generation, &data, crypto).into_head()
}

// ---------------------------------------------------------------------------
// Base and segment objects
// ---------------------------------------------------------------------------

/// A packed write-once object and its name.
pub(crate) struct PackedObject {
    pub name: ObjectName,
    pub bytes: Vec<u8>,
}

impl PackedObject {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            name: ObjectName::of(&bytes),
            bytes,
        }
    }

    pub(crate) fn key(&self) -> String {
        object_key(&self.name)
    }

    pub(crate) fn size(&self) -> u64 {
        self.bytes.len() as u64
    }
}

/// Encode, compress and encrypt a segment.
pub(crate) fn pack_segment(seg: &Segment, crypto: &dyn CryptoEngine) -> Result<PackedObject> {
    let body = seg.encode();
    let packed = pack_object_streaming_with_context(
        ObjectType::IndexSegment,
        SEGMENT_CONTEXT,
        body.len() / 2 + 64,
        crypto,
        |buf| {
            compress::compress_stream_zstd(buf, 3, |w| {
                std::io::Write::write_all(w, &body)?;
                Ok(())
            })
        },
    )?;
    Ok(PackedObject::new(packed))
}

/// Encode, compress and encrypt the base of `cache`'s state.
pub(crate) fn pack_base(
    cache: &MmapFullIndexCache,
    crypto: &dyn CryptoEngine,
) -> Result<PackedObject> {
    let estimated = cache.entry_count() as usize * 48 + 64;
    let packed = pack_object_streaming_with_context(
        ObjectType::IndexBase,
        BASE_CONTEXT,
        estimated,
        crypto,
        |buf| compress::compress_stream_zstd(buf, 3, |w| base::encode_base(cache, w)),
    )?;
    Ok(PackedObject::new(packed))
}

/// The name check every `index.d/` read starts with.
fn verify_object(name: &ObjectName, size: u64, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 != size || ObjectName::of(bytes) != *name {
        return Err(invalid(format!(
            "index object {} does not match its name",
            name.to_hex()
        )));
    }
    Ok(())
}

/// Verify, decrypt and decode a fetched segment.
pub(crate) fn decode_segment(
    seg: &SegmentRef,
    bytes: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<Segment> {
    verify_object(&seg.name, seg.size, bytes)?;
    let compressed = unpack_object_expect_with_context(
        bytes,
        ObjectType::IndexSegment,
        SEGMENT_CONTEXT,
        crypto,
    )?;
    let segment = Segment::decode(&compress::decompress_metadata(&compressed)?)?;
    if seg.kind == SegmentKind::Backup
        && (!segment.deltas.is_empty() || !segment.relocations.is_empty())
    {
        return Err(invalid(format!(
            "index segment {}: a backup segment changes existing entries",
            seg.name.to_hex()
        )));
    }
    Ok(segment)
}

/// Encode, compress and encrypt a sweep record (zstd level 3).
pub(crate) fn pack_sweep_record(
    rec: &SweepRecord,
    crypto: &dyn CryptoEngine,
) -> Result<PackedObject> {
    let body = rec.encode()?;
    let packed = pack_object_streaming_with_context(
        ObjectType::SweepRecord,
        SWEEP_RECORD_CONTEXT,
        body.len() / 2 + 64,
        crypto,
        |buf| {
            compress::compress_stream_zstd(buf, 3, |w| {
                std::io::Write::write_all(w, &body)?;
                Ok(())
            })
        },
    )?;
    Ok(PackedObject::new(packed))
}

/// Verify, decrypt and decode a fetched sweep record.
pub(crate) fn decode_sweep_record(
    r: &ObjectRef,
    bytes: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<SweepRecord> {
    verify_object(&r.name, r.size, bytes)?;
    let compressed = unpack_object_expect_with_context(
        bytes,
        ObjectType::SweepRecord,
        SWEEP_RECORD_CONTEXT,
        crypto,
    )?;
    SweepRecord::decode(&compress::decompress_metadata(&compressed)?)
}

/// GET and decode the sweep record `head` names; `Ok(None)` when it names
/// none.
pub(crate) fn fetch_sweep_record(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    head: &IndexHead,
) -> Result<Option<SweepRecord>> {
    let Some(r) = head.sweep.record else {
        return Ok(None);
    };
    let bytes =
        storage
            .get(&object_key(&r.name))?
            .ok_or_else(|| VykarError::IndexObjectMissing {
                kind: "sweep record",
                key: object_key(&r.name),
            })?;
    decode_sweep_record(&r, &bytes, crypto).map(Some)
}

/// Verify, decrypt and decode a fetched base into a full index cache in a
/// temp file beside `target`, checked against the state the head records.
pub(crate) fn decode_base_to_temp(
    base_ref: &BaseRef,
    mut bytes: Vec<u8>,
    crypto: &dyn CryptoEngine,
    target: &Path,
) -> Result<TempPath> {
    verify_object(&base_ref.name, base_ref.size, &bytes)?;
    let compressed = unpack_object_expect_with_context_in_place(
        &mut bytes,
        ObjectType::IndexBase,
        BASE_CONTEXT,
        crypto,
    )?;
    let reader = compress::zstd_metadata_reader(compressed)?;
    let (tmp, hash, entries) = base::decode_base_to_temp(reader, base_ref.entries, target)?;
    if hash != base_ref.state_hash || entries != base_ref.entries {
        return Err(invalid(format!(
            "index base {} does not hold the state its head names",
            base_ref.name.to_hex()
        )));
    }
    Ok(tmp.into_temp_path())
}

/// What [`fetch_all`] got.
#[derive(Debug, PartialEq, Eq)]
enum Fetched {
    /// Every object, in key order.
    All(Vec<Vec<u8>>),
    /// The position in `keys` of the first absent object.
    Absent(usize),
}

/// GET `keys` with up to `width` requests in flight. Absence and errors
/// are reported for the first failing key in key order.
fn fetch_all(storage: &dyn StorageBackend, keys: &[String], width: usize) -> Result<Fetched> {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let width = width.clamp(1, keys.len().max(1));
    if width == 1 {
        let mut out = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            match storage.get(key)? {
                Some(bytes) => out.push(bytes),
                None => return Ok(Fetched::Absent(i)),
            }
        }
        return Ok(Fetched::All(out));
    }
    /// `None`: skipped after another key failed or was absent.
    type Slot = Option<Result<Option<Vec<u8>>>>;
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<Slot>> = Mutex::new((0..keys.len()).map(|_| None).collect());
    std::thread::scope(|scope| {
        for _ in 0..width {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(key) = keys.get(i) else { break };
                    let r = storage.get(key);
                    let failed = !matches!(r, Ok(Some(_)));
                    if let Some(slot) = results.lock().unwrap_or_else(|e| e.into_inner()).get_mut(i)
                    {
                        *slot = Some(r);
                    }
                    if failed {
                        // Let the other workers drain; nothing more is needed.
                        next.store(keys.len(), Ordering::Relaxed);
                        break;
                    }
                }
            });
        }
    });
    let mut out = Vec::with_capacity(keys.len());
    let results = results.into_inner().unwrap_or_else(|e| e.into_inner());
    for (i, r) in results.into_iter().enumerate() {
        match r {
            Some(Ok(Some(bytes))) => out.push(bytes),
            Some(Ok(None)) => return Ok(Fetched::Absent(i)),
            Some(Err(e)) => return Err(e),
            // Dispatch is in key order, so a skipped key lies after one that
            // failed or was absent, which returned above.
            None => {
                let key = keys.get(i).map_or("an index object", String::as_str);
                return Err(VykarError::Other(format!(
                    "index object fetch stopped before {key}"
                )));
            }
        }
    }
    Ok(Fetched::All(out))
}

// ---------------------------------------------------------------------------
// Bringing the local chunk table to a head's state
// ---------------------------------------------------------------------------

/// What [`sync_full_cache`] had to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncSource {
    /// A local table already held the head's state.
    UpToDate,
    /// A local table held an earlier state of the chain; `segments` were
    /// fetched and applied.
    CaughtUp { segments: usize },
    /// No usable local table: the base and `segments` were downloaded.
    Downloaded { segments: usize },
}

/// Where the chunk tables live for one sync.
pub(crate) struct CacheLocation<'a> {
    /// The per-repo cache dir holding `chunk_table.<state>` files, when the
    /// repository has one and local tables may be used. A table the sync
    /// produces is published there; without a dir it stays a scratch file.
    pub dir: Option<&'a Path>,
    /// The repository's table, when one is loaded: checked first, and
    /// accepted without re-verification (it was verified when mapped).
    pub live: Option<Arc<ChunkTable>>,
    /// Whether existing local tables may be used at all (`false` for
    /// `check`, which verifies every object in the chain).
    pub use_local: bool,
}

/// A chunk table at a head's state.
pub(crate) struct SyncedIndex {
    /// The head synced to; a re-read one if a named object vanished.
    pub head: IndexHead,
    pub table: Arc<ChunkTable>,
    pub source: SyncSource,
}

/// Path of the anchor named by [`TEMP_ANCHOR_FILE`].
fn temp_anchor() -> PathBuf {
    std::env::temp_dir().join(TEMP_ANCHOR_FILE)
}

/// Where temp files of a chunk table go: in `dir` when it exists or can be
/// created, otherwise in the system temp dir.
pub(crate) fn cache_anchor(dir: Option<&Path>) -> PathBuf {
    match dir {
        Some(d) if std::fs::create_dir_all(d).is_ok() => table::table_anchor(d),
        _ => temp_anchor(),
    }
}

/// A scratch table published in `dir` (see [`ChunkTable::publish`]), after
/// which the old tables there are deleted.
pub(crate) fn publish_table(table: ChunkTable, dir: Option<&Path>) -> ChunkTable {
    pruned_after(table.publish(dir), dir)
}

/// `table`, after deleting the old tables in `dir` when it is published there.
fn pruned_after(table: ChunkTable, dir: Option<&Path>) -> ChunkTable {
    prune_old_tables(&table, dir);
    table
}

/// Delete the old tables in `dir` when `table` is published there. A table
/// this process still maps is kept, so a caller that replaces its own table
/// drops the old one first.
pub(crate) fn prune_old_tables(table: &ChunkTable, dir: Option<&Path>) {
    if let Some(dir) = dir
        && table.file_path().is_some()
    {
        let deleted = table::prune_tables(dir, &table.state().0, table::TABLE_KEEP_AGE);
        if deleted > 0 {
            debug!(deleted, "deleted old chunk tables");
        }
    }
}

/// Bring a local chunk table to `head`'s state.
///
/// Readers take no lock, so a fold may delete the objects of the head they
/// read. A missing object is answered by re-reading the head: a head that
/// still names it as live means it is lost (no writer deletes an object the
/// current head names), and the read fails with
/// [`VykarError::IndexObjectMissing`]; otherwise a fold moved the head and
/// the read is retried on the new one, at most [`MAX_HEAD_RETRIES`] times.
pub(crate) fn sync_full_cache(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    mut head: IndexHead,
    location: CacheLocation<'_>,
    width: usize,
) -> Result<SyncedIndex> {
    let mut retries = 0;
    loop {
        let missing = match sync_once(storage, crypto, &head, &location, width)? {
            Attempt::Synced(table, source) => {
                return Ok(SyncedIndex {
                    head,
                    table,
                    source,
                });
            }
            Attempt::Missing(missing) => missing,
        };
        let fresh = fetch_head(storage, crypto)?;
        if fresh.live_objects().any(|n| n == missing.name) {
            return Err(VykarError::IndexObjectMissing {
                kind: missing.kind,
                key: object_key(&missing.name),
            });
        }
        if retries == MAX_HEAD_RETRIES {
            return Err(VykarError::IndexHeadUnstable);
        }
        retries += 1;
        debug!(
            attempt = retries,
            object = %missing.name.to_hex(),
            "index object vanished, the head moved; retrying on the new head"
        );
        head = fresh;
    }
}

/// An object a head names that storage does not have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MissingObject {
    name: ObjectName,
    /// `"base"` or `"segment"`.
    kind: &'static str,
}

/// One attempt of [`sync_full_cache`].
enum Attempt {
    Synced(Arc<ChunkTable>, SyncSource),
    /// An object named live by the attempted head is absent.
    Missing(MissingObject),
}

fn sync_once(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    head: &IndexHead,
    location: &CacheLocation<'_>,
    width: usize,
) -> Result<Attempt> {
    let start = match local_start(head, location) {
        LocalStart::AtHead(table) => return Ok(Attempt::Synced(table, SyncSource::UpToDate)),
        LocalStart::InChain(table, k) => Some((table, k)),
        LocalStart::Absent => None,
    };
    sync_from(storage, crypto, head, location, start, width)
}

/// The local table a sync can start from.
enum LocalStart {
    /// A local table already at the head's state.
    AtHead(Arc<ChunkTable>),
    /// A local table at `chain_states()[k]`, the latest such position found.
    InChain(Arc<ChunkTable>, usize),
    /// None usable (or `use_local` is false).
    Absent,
}

/// The repository's live table, then the table files in `location.dir`
/// named after a state of `head`'s chain, latest chain position first. A
/// file whose content is not the state its name says is skipped.
fn local_start(head: &IndexHead, location: &CacheLocation<'_>) -> LocalStart {
    if !location.use_local {
        return LocalStart::Absent;
    }
    let target_state = (head.state_hash, head.entries);
    let chain = head.chain_states();
    let position = |state: &(StateHash, u64)| chain.iter().rposition(|s| s == state);
    let live = location
        .live
        .as_ref()
        .and_then(|t| Some((position(&t.state())?, Arc::clone(t))));
    if let Some((_, t)) = &live
        && t.state() == target_state
    {
        return LocalStart::AtHead(Arc::clone(t));
    }
    let mut files: Vec<(usize, PathBuf)> = location
        .dir
        .map(table::list_tables)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(state, path)| {
            let k = chain.iter().rposition(|s| s.0 == state)?;
            Some((k, path))
        })
        .filter(|(k, _)| live.as_ref().is_none_or(|(best, _)| k > best))
        .collect();
    files.sort_by_key(|a| std::cmp::Reverse(a.0));
    for (_, path) in files {
        let Some(t) = ChunkTable::open_file(&path) else {
            continue;
        };
        let Some(k) = position(&t.state()) else {
            continue;
        };
        let t = Arc::new(t);
        if t.state() == target_state {
            return LocalStart::AtHead(t);
        }
        return LocalStart::InChain(t, k);
    }
    match live {
        Some((k, t)) => LocalStart::InChain(t, k),
        None => LocalStart::Absent,
    }
}

/// Bring `start` (or, without one, a downloaded base) to `head`'s state and
/// install the result. [`Attempt::Missing`] when an object `head` names is
/// absent.
fn sync_from(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    head: &IndexHead,
    location: &CacheLocation<'_>,
    start: Option<(Arc<ChunkTable>, usize)>,
    width: usize,
) -> Result<Attempt> {
    let target_state = (head.state_hash, head.entries);
    let dir = location.dir.filter(|_| location.use_local);
    let anchor = cache_anchor(dir);

    let from = start.as_ref().map_or(0, |(_, k)| *k);
    let pending = head
        .segments
        .get(from..)
        .ok_or_else(|| invalid("index chain position out of range"))?;
    // Segments before the base: the fold limits keep them small next to it,
    // so a missing one costs no base download.
    let keys: Vec<String> = pending.iter().map(|s| object_key(&s.name)).collect();
    let fetched = match fetch_all(storage, &keys, width)? {
        Fetched::All(f) => f,
        Fetched::Absent(i) => {
            let seg = pending.get(i).ok_or_else(|| {
                VykarError::Other(format!(
                    "index object fetch reported position {i} out of range"
                ))
            })?;
            return Ok(Attempt::Missing(MissingObject {
                name: seg.name,
                kind: "segment",
            }));
        }
    };

    let downloaded = start.is_none();
    let prev = match start {
        Some((table, _)) => table,
        None => {
            let tmp = match &head.base {
                Some(base_ref) => {
                    let Some(bytes) = storage.get(&object_key(&base_ref.name))? else {
                        return Ok(Attempt::Missing(MissingObject {
                            name: base_ref.name,
                            kind: "base",
                        }));
                    };
                    decode_base_to_temp(base_ref, bytes, crypto, &anchor)?
                }
                None => FullCacheWriter::new_beside(&anchor)?
                    .finish(None)?
                    .0
                    .into_temp_path(),
            };
            let table = ChunkTable::scratch(tmp)
                .map_err(|_| invalid("decoded index base failed verification"))?;
            Arc::new(table)
        }
    };

    let segments = pending
        .iter()
        .zip(&fetched)
        .map(|(r, bytes)| decode_segment(r, bytes, crypto))
        .collect::<Result<Vec<_>>>()?;
    drop(fetched);
    let steps: Vec<ChainStep<'_>> = pending
        .iter()
        .zip(&segments)
        .map(|(r, segment)| ChainStep {
            segment,
            state_hash_after: r.state_hash_after,
            entries_after: r.entries_after,
        })
        .collect();

    let source = if downloaded {
        SyncSource::Downloaded {
            segments: steps.len(),
        }
    } else {
        SyncSource::CaughtUp {
            segments: steps.len(),
        }
    };
    let table = if steps.is_empty() {
        // A downloaded base already at the head's state.
        Arc::into_inner(prev)
            .filter(|_| downloaded)
            .ok_or_else(|| invalid("index chain has no segments to apply"))?
    } else {
        let (tmp, _, _) = segment::apply_to_temp(prev.cache(), &steps, &anchor)?;
        drop(prev);
        ChunkTable::scratch(tmp)
            .map_err(|_| invalid("index cache failed verification after sync"))?
    };
    // Checked before anything is published.
    if table.state() != target_state {
        return Err(invalid("index state after sync differs from the head"));
    }
    Ok(Attempt::Synced(Arc::new(publish_table(table, dir)), source))
}

// ---------------------------------------------------------------------------
// Garbage collection and damage classification
// ---------------------------------------------------------------------------

/// Object name of an `index.d/` key, when it is one.
fn object_name_of_key(key: &str) -> Option<ObjectName> {
    if !vykar_protocol::is_index_object_key(key) {
        return None;
    }
    let hex = key.strip_prefix(INDEX_OBJECT_DIR)?;
    let bytes: [u8; 32] = hex::decode(hex).ok()?.try_into().ok()?;
    Some(ObjectName(bytes))
}

/// What [`collect_index_garbage`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct IndexGcStats {
    pub heads_deleted: usize,
    pub objects_deleted: usize,
    /// Why (part of) the run was skipped.
    pub skipped: Option<&'static str>,
}

/// Log a failed best-effort delete: at debug for a write refusal
/// (append-only), at warn otherwise.
fn log_failed_delete(what: &str, key: &str, e: &VykarError) {
    if e.is_write_refusal() {
        debug!("{what} {key} not deleted (refused): {e}");
    } else {
        tracing::warn!("failed to delete {what} {key}: {e}");
    }
}

/// Delete old heads and old unreferenced `index.d/` objects, by 04's rule
/// and the storage's clock:
/// - heads: keep `latest`, every generation >= `latest.generation - HEADS_KEPT`,
///   and every head younger than `grace`; delete the others;
/// - objects: keep every object a kept head names
///   ([`IndexHead::named_objects`]) and every object younger than `grace`;
///   delete the others.
///
/// "Now" is the newest mtime in the two listings (the storage's clock, not
/// the client's). A listed entry with an mtime in the future moves "now"
/// forward and over-estimates every other age. That damage is bounded: the
/// latest head and the [`HEADS_KEPT`] below it are kept whatever their age,
/// and so is every object a kept head names; only an object no recent head
/// names can go early (a concurrent writer's object not yet named by its
/// head among them). An entry without an mtime counts as age 0; with no mtime
/// at all nothing is deleted (`skipped = "no mtimes"`). A kept head other
/// than `latest` that cannot be read stops the object part (`skipped =
/// "unreadable retained head"`); heads already deleted stay deleted. A head
/// whose delete failed is still treated as not kept, so its objects may go:
/// it is never read, because readers take the latest. Deletes are
/// best-effort and logged (at debug when refused, as on an append-only
/// server). Storage errors on the two listings are returned.
pub(crate) fn collect_index_garbage(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    latest: &IndexHead,
    grace: std::time::Duration,
) -> Result<IndexGcStats> {
    let mut stats = IndexGcStats::default();
    let heads: Vec<(u64, Option<std::time::SystemTime>)> = storage
        .list_meta(HEAD_PREFIX)?
        .into_iter()
        .filter_map(|o| Some((vykar_protocol::head_generation(&o.key)?, o.mtime)))
        .collect();
    let objects: Vec<(ObjectName, Option<std::time::SystemTime>)> = storage
        .list_meta(INDEX_OBJECT_DIR)?
        .into_iter()
        .filter_map(|o| Some((object_name_of_key(&o.key)?, o.mtime)))
        .collect();
    let Some(now) = heads
        .iter()
        .map(|h| h.1)
        .chain(objects.iter().map(|o| o.1))
        .flatten()
        .max()
    else {
        stats.skipped = Some("no mtimes");
        return Ok(stats);
    };
    // Age 0 when the mtime is missing or newer than "now".
    let old = |mtime: Option<std::time::SystemTime>| {
        mtime.is_some_and(|m| now.duration_since(m).unwrap_or_default() >= grace)
    };

    let floor = latest.generation.saturating_sub(HEADS_KEPT);
    let (doomed, kept): (Vec<_>, Vec<_>) =
        heads.iter().copied().partition(|&(generation, mtime)| {
            generation != latest.generation && generation < floor && old(mtime)
        });

    // Read the kept heads before deleting anything.
    let mut keep: std::collections::HashSet<ObjectName> = latest.named_objects().collect();
    let mut readable = true;
    for &(generation, _) in &kept {
        if generation == latest.generation {
            continue;
        }
        let head = storage
            .get(&head_key(generation))
            .and_then(|d| d.ok_or_else(|| invalid("index head vanished")))
            .and_then(|d| classify_head(generation, &d, crypto).into_head());
        match head {
            Ok(h) => keep.extend(h.named_objects()),
            Err(e) => {
                debug!(
                    generation,
                    "retained index head unreadable ({e}); keeping every index object"
                );
                readable = false;
                break;
            }
        }
    }

    for &(generation, _) in &doomed {
        let key = head_key(generation);
        match storage.delete(&key) {
            Ok(()) => stats.heads_deleted += 1,
            Err(e) => log_failed_delete("index head", &key, &e),
        }
    }
    if !readable {
        stats.skipped = Some("unreadable retained head");
        return Ok(stats);
    }

    let garbage: Vec<String> = objects
        .iter()
        .filter(|(name, mtime)| !keep.contains(name) && old(*mtime))
        .map(|(name, _)| object_key(name))
        .collect();
    if garbage.is_empty() {
        return Ok(stats);
    }
    if let Some(batch) = storage.batch_delete() {
        match batch.delete_keys(&garbage, vykar_storage::BatchDeleteScope::KeysOnly) {
            Ok(()) => {
                stats.objects_deleted = garbage.len();
                return Ok(stats);
            }
            Err(e) => tracing::warn!(
                "batch delete of {} index object(s) failed ({e}); deleting one by one",
                garbage.len()
            ),
        }
    }
    for key in &garbage {
        match storage.delete(key) {
            Ok(()) => stats.objects_deleted += 1,
            Err(e) => log_failed_delete("index object", key, &e),
        }
    }
    Ok(stats)
}

/// Whether `e`, raised while verifying, decrypting, decoding or applying a
/// fetched `index.d/` object, means the object is damaged, or (for
/// [`VykarError::IndexObjectMissing`]) lost. Storage and local I/O errors,
/// `UnsupportedVersion` and [`VykarError::IndexHeadUnstable`] are not:
/// repair propagates them instead of dropping index state.
pub(crate) fn is_object_damage(e: &VykarError) -> bool {
    matches!(
        e,
        VykarError::InvalidFormat(_)
            | VykarError::IndexObjectMissing { .. }
            | VykarError::DecryptionFailed
            | VykarError::Decompression(_)
            | VykarError::Deserialization(_)
            | VykarError::UnknownObjectType(_)
            | VykarError::UnknownCompressionTag(_)
    )
}

#[cfg(test)]
pub(crate) mod tests;
