//! Storage side of the segmented index (formats v4/v5).
//!
//! ```text
//! index                # mutable, small: IndexHead
//! index.d/<64 hex>     # write-once: base or segment, name = BLAKE3 of the stored bytes
//! ```
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

use super::INDEX_OBJECT_CONTEXT;
use super::format::{
    ObjectType, pack_object_streaming_with_context, pack_object_with_context,
    unpack_object_expect_with_context, unpack_object_expect_with_context_in_place,
};
use crate::cache_paths::TEMP_ANCHOR_FILE;
use crate::compress;
use crate::index::base;
use crate::index::dedup_cache::{FullCacheWriter, MmapFullIndexCache, StateHash};
use crate::index::head::{self, BaseRef, IndexHead, ObjectName, SegmentRef};
use crate::index::segment::{self, ChainStep, Segment};
use crate::index::table::{self, ChunkTable};

/// Directory of the write-once base and segment objects.
pub(crate) const INDEX_OBJECT_DIR: &str = "index.d/";

pub(crate) const BASE_CONTEXT: &[u8] = b"index.d/base";
pub(crate) const SEGMENT_CONTEXT: &[u8] = b"index.d/segment";

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

/// Serialize and encrypt a head for the `index` key.
pub(crate) fn pack_head(head: &IndexHead, crypto: &dyn CryptoEngine) -> Result<Vec<u8>> {
    pack_object_with_context(
        ObjectType::IndexHead,
        INDEX_OBJECT_CONTEXT,
        &head.encode()?,
        crypto,
    )
}

/// Whether `data` (the object at `index`) is a monolithic index blob.
pub(crate) fn is_legacy_index(data: &[u8]) -> bool {
    data.first() == Some(&(ObjectType::ChunkIndex as u8))
}

/// What the object at `index` of a segmented repository is.
#[derive(Debug)]
pub(crate) enum StoredHead {
    Head(IndexHead),
    /// A monolithic index blob (an older binary overwrote the head).
    Legacy,
    /// Decrypts, and names a head layout this binary does not read.
    UnknownLayout(u32),
    /// An object type tag this binary does not know (a newer writer).
    UnknownType(u8),
    /// Fails to decrypt, decode or validate; the original error.
    Unreadable(VykarError),
}

/// Classify the bytes at `index`. No storage access.
pub(crate) fn classify_head(data: &[u8], crypto: &dyn CryptoEngine) -> StoredHead {
    if is_legacy_index(data) {
        return StoredHead::Legacy;
    }
    let plain = match unpack_object_expect_with_context(
        data,
        ObjectType::IndexHead,
        INDEX_OBJECT_CONTEXT,
        crypto,
    ) {
        Ok(plain) => plain,
        Err(VykarError::UnknownObjectType(tag)) => return StoredHead::UnknownType(tag),
        Err(e) => return StoredHead::Unreadable(e),
    };
    match IndexHead::decode(&plain) {
        Ok(head) => StoredHead::Head(head),
        Err(e) => match head::peek_layout(&plain) {
            Some(layout) if layout != head::HEAD_LAYOUT_VERSION => {
                StoredHead::UnknownLayout(layout)
            }
            _ => StoredHead::Unreadable(e),
        },
    }
}

impl StoredHead {
    /// The head, or the error [`decode_head`] returns for this object.
    pub(crate) fn into_head(self) -> Result<IndexHead> {
        match self {
            Self::Head(head) => Ok(head),
            Self::Legacy => Err(invalid(
                "the repository is in a segmented index format, but `index` holds a \
                 monolithic index (written by an older vykar); run `vykar check --repair` \
                 to rebuild it",
            )),
            Self::UnknownLayout(layout) => Err(head::unsupported_layout(layout)),
            Self::UnknownType(tag) => Err(VykarError::UnknownObjectType(tag)),
            Self::Unreadable(e) => Err(e),
        }
    }
}

/// Decrypt and decode the object stored at `index`.
pub(crate) fn decode_head(data: &[u8], crypto: &dyn CryptoEngine) -> Result<IndexHead> {
    classify_head(data, crypto).into_head()
}

/// GET and decode the head. A segmented repository always has one (`init`
/// writes the empty head).
pub(crate) fn fetch_head(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
) -> Result<IndexHead> {
    let data = storage
        .get("index")?
        .ok_or_else(|| invalid("index head missing"))?;
    decode_head(&data, crypto)
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
    Segment::decode(&compress::decompress_metadata(&compressed)?)
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

/// Names of every object under `index.d/`. Keys that are not object names
/// (foreign files) are left out, and so never deleted.
pub(crate) fn list_objects(storage: &dyn StorageBackend) -> Result<Vec<ObjectName>> {
    let keys = storage.list(INDEX_OBJECT_DIR)?;
    Ok(keys
        .iter()
        .filter(|k| vykar_protocol::is_index_object_key(k))
        .filter_map(|k| {
            let hex = k.strip_prefix(INDEX_OBJECT_DIR)?;
            let bytes: [u8; 32] = hex::decode(hex).ok()?.try_into().ok()?;
            Some(ObjectName(bytes))
        })
        .collect())
}

/// Delete every `index.d/` object that `head` names neither as live nor
/// (unless `drop_retired`) as retired: one batch request where the backend
/// has one, else (or if the batch fails) one `delete` per object.
/// Best-effort per object: a failed delete leaves debris for the next run.
/// Returns how many were deleted (for a batch, how many were requested).
pub(crate) fn collect_garbage(
    storage: &dyn StorageBackend,
    head: &IndexHead,
    drop_retired: bool,
) -> Result<usize> {
    let retired = head.retired.iter().copied().filter(|_| !drop_retired);
    let keep: std::collections::HashSet<ObjectName> = head.live_objects().chain(retired).collect();
    let garbage: Vec<ObjectName> = list_objects(storage)?
        .into_iter()
        .filter(|name| !keep.contains(name))
        .collect();
    if garbage.is_empty() {
        return Ok(0);
    }
    let keys: Vec<String> = garbage.iter().map(object_key).collect();
    if let Some(batch) = storage.batch_delete() {
        match batch.delete_keys(&keys, vykar_storage::BatchDeleteScope::KeysOnly) {
            Ok(()) => return Ok(keys.len()),
            Err(e) => tracing::warn!(
                "batch delete of {} index object(s) failed ({e}); deleting one by one",
                keys.len()
            ),
        }
    }
    let mut deleted = 0;
    for (name, key) in garbage.iter().zip(&keys) {
        match storage.delete(key) {
            Ok(()) => deleted += 1,
            Err(e) => tracing::warn!("failed to delete index object {}: {e}", name.to_hex()),
        }
    }
    Ok(deleted)
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
