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
//! [`sync_full_cache`] brings a local full index cache to a head's state:
//! nothing to do when it already matches, the missing segments applied in
//! one streaming pass when it matches a state in the head's chain, the base
//! downloaded otherwise.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use tempfile::TempPath;
use tracing::{debug, warn};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};

use super::format::{
    pack_object_streaming_with_context, pack_object_with_context,
    unpack_object_expect_with_context, unpack_object_expect_with_context_in_place, ObjectType,
};
use super::INDEX_OBJECT_CONTEXT;
use crate::compress;
use crate::index::base;
use crate::index::dedup_cache::{FullCacheWriter, MmapFullIndexCache, StateHash};
use crate::index::head::{self, BaseRef, IndexHead, ObjectName, SegmentRef};
use crate::index::segment::{self, ChainStep, Segment};

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

/// Decrypt and decode a base no head names; the caller checked its name.
fn decode_unnamed_base(
    mut bytes: Vec<u8>,
    crypto: &dyn CryptoEngine,
    target: &Path,
) -> Result<(TempPath, StateHash, u64)> {
    let compressed = unpack_object_expect_with_context_in_place(
        &mut bytes,
        ObjectType::IndexBase,
        BASE_CONTEXT,
        crypto,
    )?;
    let reader = compress::zstd_metadata_reader(compressed)?;
    let (tmp, hash, entries) = base::decode_unnamed_base_to_temp(reader, target)?;
    Ok((tmp.into_temp_path(), hash, entries))
}

/// [`decode_unnamed_base`] for a segment.
fn decode_unnamed_segment(bytes: &[u8], crypto: &dyn CryptoEngine) -> Result<Segment> {
    let compressed = unpack_object_expect_with_context(
        bytes,
        ObjectType::IndexSegment,
        SEGMENT_CONTEXT,
        crypto,
    )?;
    Segment::decode(&compress::decompress_metadata(&compressed)?)
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

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
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(key) = keys.get(i) else { break };
                let r = storage.get(key);
                let failed = !matches!(r, Ok(Some(_)));
                if let Some(slot) = results.lock().unwrap_or_else(|e| e.into_inner()).get_mut(i) {
                    *slot = Some(r);
                }
                if failed {
                    // Let the other workers drain; nothing more is needed.
                    next.store(keys.len(), Ordering::Relaxed);
                    break;
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
// Bringing a local full cache to a head's state
// ---------------------------------------------------------------------------

/// What [`sync_full_cache`] had to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncSource {
    /// The local cache already held the head's state.
    UpToDate,
    /// The local cache held an earlier state of the chain; `segments` were
    /// fetched and applied.
    CaughtUp { segments: usize },
    /// No usable local cache: the base and `segments` were downloaded.
    Downloaded { segments: usize },
}

/// Where the full index cache lives for one sync.
pub(crate) struct CacheLocation<'a> {
    /// The persistent cache file, when the repository has a cache directory.
    pub path: Option<&'a Path>,
    /// A temp-file cache kept from earlier in this run (see
    /// `Repository::index_scratch`). Checked before `path`.
    pub scratch: Option<TempPath>,
    /// Whether existing local caches may be used at all (`false` for
    /// `check`, which verifies every object in the chain).
    pub use_local: bool,
}

/// A full index cache at a head's state.
pub(crate) struct SyncedIndex {
    /// The head synced to; a re-read one if a named object vanished.
    pub head: IndexHead,
    pub cache: MmapFullIndexCache,
    /// Set when the cache lives in a temp file rather than at `path`.
    pub scratch: Option<TempPath>,
    pub source: SyncSource,
}

/// File name of the temp-file anchor, used for syncs that bypass the local
/// cache and when there is no cache directory.
pub(crate) const TEMP_ANCHOR_FILE: &str = "vykar-full-index-cache";

/// Path of the anchor named by [`TEMP_ANCHOR_FILE`].
fn temp_anchor() -> PathBuf {
    std::env::temp_dir().join(TEMP_ANCHOR_FILE)
}

/// Where temp files of a full index cache go: beside `path` when its
/// directory exists or can be created, otherwise in the system temp dir.
pub(crate) fn cache_anchor(path: Option<&Path>) -> PathBuf {
    match path {
        Some(p)
            if p.parent()
                .is_some_and(|d| std::fs::create_dir_all(d).is_ok()) =>
        {
            p.to_path_buf()
        }
        _ => temp_anchor(),
    }
}

/// Bring a local full index cache to `head`'s state.
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
    mut location: CacheLocation<'_>,
    width: usize,
) -> Result<SyncedIndex> {
    let mut retries = 0;
    loop {
        let missing = match sync_once(storage, crypto, &head, &mut location, width)? {
            Attempt::Synced(cache, source) => {
                return Ok(SyncedIndex {
                    head,
                    cache,
                    scratch: location.scratch,
                    source,
                })
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
    Synced(MmapFullIndexCache, SyncSource),
    /// An object named live by the attempted head is absent.
    Missing(MissingObject),
}

fn sync_once(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    head: &IndexHead,
    location: &mut CacheLocation<'_>,
    width: usize,
) -> Result<Attempt> {
    let start = match local_start(head, location) {
        LocalStart::AtHead(cache) => return Ok(Attempt::Synced(cache, SyncSource::UpToDate)),
        LocalStart::InChain(cache, k) => Some((cache, k)),
        LocalStart::Absent => None,
    };
    sync_from(storage, crypto, head, location, start, width)
}

/// Bring a local cache in `head`'s chain to `head` and install it;
/// `Ok(None)` when there is none. For writers under the lock: never
/// downloads a base or re-reads the head, and since no fold runs under the
/// lock, an absent object is lost ([`VykarError::IndexObjectMissing`]).
pub(crate) fn catch_up_local_cache(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    head: &IndexHead,
    location: &mut CacheLocation<'_>,
    width: usize,
) -> Result<Option<(MmapFullIndexCache, SyncSource)>> {
    let attempt = match local_start(head, location) {
        LocalStart::AtHead(cache) => return Ok(Some((cache, SyncSource::UpToDate))),
        LocalStart::InChain(cache, k) => {
            sync_from(storage, crypto, head, location, Some((cache, k)), width)?
        }
        LocalStart::Absent => return Ok(None),
    };
    match attempt {
        Attempt::Synced(cache, source) => Ok(Some((cache, source))),
        Attempt::Missing(m) => Err(VykarError::IndexObjectMissing {
            kind: m.kind,
            key: object_key(&m.name),
        }),
    }
}

/// The local cache a sync can start from.
enum LocalStart {
    /// A local cache already at the head's state.
    AtHead(MmapFullIndexCache),
    /// A local cache at `chain_states()[k]`, the latest such position found.
    InChain(MmapFullIndexCache, usize),
    /// None usable (or `use_local` is false).
    Absent,
}

/// Scan `location.scratch`, then `location.path`, for a cache at a state of
/// `head`'s chain.
fn local_start(head: &IndexHead, location: &CacheLocation<'_>) -> LocalStart {
    if !location.use_local {
        return LocalStart::Absent;
    }
    let target_state = (head.state_hash, head.entries);
    let chain = head.chain_states();
    // A local cache at some state of the chain, latest first.
    let mut start: Option<(MmapFullIndexCache, usize)> = None;
    let candidates = location.scratch.as_deref().into_iter().chain(location.path);
    for path in candidates {
        let Some(cache) = MmapFullIndexCache::open_verified(path) else {
            continue;
        };
        let state = (cache.state_hash(), u64::from(cache.entry_count()));
        if state == target_state {
            return LocalStart::AtHead(cache);
        }
        if let Some(k) = chain.iter().rposition(|s| *s == state) {
            if start.as_ref().is_none_or(|(_, best)| k > *best) {
                start = Some((cache, k));
            }
        }
    }
    match start {
        Some((cache, k)) => LocalStart::InChain(cache, k),
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
    location: &mut CacheLocation<'_>,
    start: Option<(MmapFullIndexCache, usize)>,
    width: usize,
) -> Result<Attempt> {
    let target_state = (head.state_hash, head.entries);
    let anchor = cache_anchor(location.path);

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
    // `prev_tmp` keeps a downloaded base alive while it is mapped; bound
    // first so the mapping drops before its file on an early return.
    let (prev_tmp, prev) = match start {
        Some((cache, _)) => (None, cache),
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
            let cache = MmapFullIndexCache::open_verified(&tmp)
                .ok_or_else(|| invalid("decoded index base failed verification"))?;
            (Some(tmp), cache)
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
    // Map our own temp file, not the path (see [`install`]).
    let (result, cache) = if steps.is_empty() {
        // A downloaded base already at the head's state.
        let tmp = prev_tmp.ok_or_else(|| invalid("index chain has no segments to apply"))?;
        (tmp, prev)
    } else {
        let (tmp, _, _) = segment::apply_to_temp(&prev, &steps, &anchor)?;
        // Unmap the predecessor before its file is replaced (Windows).
        drop(prev);
        drop(prev_tmp);
        let cache = MmapFullIndexCache::open_verified(&tmp)
            .ok_or_else(|| invalid("index cache failed verification after sync"))?;
        (tmp, cache)
    };
    if (cache.state_hash(), u64::from(cache.entry_count())) != target_state {
        return Err(invalid("index state after sync differs from the head"));
    }
    install(result, location);
    Ok(Attempt::Synced(cache, source))
}

/// Move a finished cache to `location.path`, or keep it as the scratch file
/// (no path, local caches unused, or a failed rename). A caller that needs
/// the cache maps `tmp` first: another process may rename its file over the
/// path at any time.
fn install(tmp: TempPath, location: &mut CacheLocation<'_>) {
    match location.path {
        Some(path) if location.use_local => match tmp.persist(path) {
            Ok(()) => location.scratch = None,
            Err(e) => {
                debug!(error = %e.error, "keeping the index cache in its temp file");
                location.scratch = Some(e.path);
            }
        },
        _ => location.scratch = Some(tmp),
    }
}

/// [`install`] for a cache written by an index write: persisted over
/// `path`, or kept as a temp file. Returns the temp file when it stays one.
pub(crate) fn install_cache(tmp: TempPath, path: Option<&Path>) -> Option<TempPath> {
    let mut location = CacheLocation {
        path,
        scratch: None,
        use_local: true,
    };
    install(tmp, &mut location);
    location.scratch
}

// ---------------------------------------------------------------------------
// Garbage collection and salvage
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
    match storage.batch_delete_keys(&keys, vykar_storage::BatchDeleteScope::KeysOnly) {
        Ok(()) => return Ok(keys.len()),
        Err(VykarError::UnsupportedBackend(_)) => {}
        Err(e) => tracing::warn!(
            "batch delete of {} index object(s) failed ({e}); deleting one by one",
            keys.len()
        ),
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

/// What [`salvage_chain`] could read.
pub(crate) struct Salvaged {
    /// The latest readable state, as a full index cache in a temp file.
    pub cache: TempPath,
    /// `false` when the head names a base that is missing or damaged; the
    /// state is then the empty index and no segment was applied.
    pub base_read: bool,
    /// How many of the head's segments were applied, counted from the first.
    pub applied: usize,
}

/// GET an `index.d/` object for salvage; a storage error names the object.
fn salvage_get(storage: &dyn StorageBackend, name: &ObjectName) -> Result<Option<Vec<u8>>> {
    let key = object_key(name);
    storage.get(&key).map_err(|e| match e {
        VykarError::Interrupted => e,
        e => VykarError::Other(format!("reading index object {key}: {e}")),
    })
}

/// The latest state of `head`'s chain that can still be read: the base (or
/// the empty index when it is missing or damaged), then segments up to the
/// first that is missing or fails to verify, decode or apply. For
/// `check --repair`, which rebuilds the index from there. A storage or local
/// I/O error, or an object from a newer vykar, is returned as an error.
pub(crate) fn salvage_chain(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    head: &IndexHead,
    anchor: &Path,
) -> Result<Salvaged> {
    let base = match &head.base {
        None => None,
        Some(base_ref) => match salvage_get(storage, &base_ref.name)? {
            None => {
                tracing::warn!("index base {} is missing", base_ref.name.to_hex());
                None
            }
            Some(bytes) => match decode_base_to_temp(base_ref, bytes, crypto, anchor) {
                Ok(tmp) => Some(tmp),
                Err(e) if is_object_damage(&e) => {
                    tracing::warn!("index base {}: {e}", base_ref.name.to_hex());
                    None
                }
                Err(e) => return Err(e),
            },
        },
    };
    let base_read = head.base.is_none() || base.is_some();
    let mut current = match base {
        Some(tmp) => tmp,
        None => FullCacheWriter::new_beside(anchor)?
            .finish(None)?
            .0
            .into_temp_path(),
    };
    if !base_read {
        // Later segments do not apply to the empty index.
        return Ok(Salvaged {
            cache: current,
            base_read,
            applied: 0,
        });
    }
    let mut applied = 0;
    for seg_ref in &head.segments {
        let hex = seg_ref.name.to_hex();
        let Some(bytes) = salvage_get(storage, &seg_ref.name)? else {
            tracing::warn!("index segment {hex} is missing");
            break;
        };
        let segment = match decode_segment(seg_ref, &bytes, crypto) {
            Ok(segment) => segment,
            Err(e) if is_object_damage(&e) => {
                tracing::warn!("index segment {hex}: {e}");
                break;
            }
            Err(e) => return Err(e),
        };
        let prev = MmapFullIndexCache::open_verified(&current)
            .ok_or_else(|| VykarError::Other("salvaged index state failed verification".into()))?;
        let step = ChainStep {
            segment: &segment,
            state_hash_after: seg_ref.state_hash_after,
            entries_after: seg_ref.entries_after,
        };
        let next = segment::apply_to_temp(&prev, &[step], anchor);
        // Unmap before `current` is replaced (and its file deleted).
        drop(prev);
        match next {
            Ok((tmp, _, _)) => {
                current = tmp;
                applied += 1;
            }
            Err(e) if is_object_damage(&e) => {
                tracing::warn!("index segment {hex}: {e}");
                break;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(Salvaged {
        cache: current,
        base_read: true,
        applied,
    })
}

/// State hash and entry count of an index state.
type State = (StateHash, u64);

/// An index state the walk holds, as a full cache in a temp file.
struct Walked {
    cache: TempPath,
    state: State,
}

/// An end state of the walk and the objects on its own path.
struct End {
    /// The base the walk started from; `None` = the empty index.
    origin: Option<ObjectName>,
    segments: Vec<ObjectName>,
    at: Walked,
}

/// The index [`rebuild_without_head`] rebuilt.
pub(crate) struct Headless {
    pub cache: TempPath,
    /// Bases that decoded.
    pub bases: usize,
    /// Segments applied, over all walks.
    pub applied: usize,
    /// Segments that decoded but follow no state the walk reached.
    pub unplaced: usize,
    /// Listed objects skipped as damage: absent, not matching their name,
    /// failing to decode, or not applying to the state they name.
    pub damaged: usize,
}

/// What [`rebuild_without_head`] found.
pub(crate) enum HeadlessOutcome {
    Rebuilt(Headless),
    /// The objects do not determine one index state; nothing may be
    /// written. The message lists them and says how to proceed.
    Refused(String),
}

/// Rebuild the index from the `index.d/` objects `names` (as listed) when
/// `index` cannot be read, for `check --repair`.
///
/// Starts are every base, plus the empty index when a segment follows it;
/// from each, segments are followed by the state they record. Two segments
/// following one state, segments returning to a state on the same path, or
/// several end states are refused. A single end state is used, but nothing
/// confirms it is the latest: a segment left by a failed write that follows
/// the latest state is applied, since nothing tells it from a committed one,
/// and when the head's state recurs inside the retired generation (a fold
/// back to an earlier state) the walk follows the retired segments past it.
/// After a fold that changed the state, the previous generation is a second
/// end state until a fold that leaves the state unchanged (the one at the
/// end of `compact`). Storage and local I/O errors, and objects of a newer
/// object version, are returned as errors; an object that matches its name
/// but has an unknown type or compression tag (a newer writer) is refused.
pub(crate) fn rebuild_without_head(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    mut names: Vec<ObjectName>,
    anchor: &Path,
) -> Result<HeadlessOutcome> {
    names.sort_unstable_by_key(|n| n.0);
    names.dedup();
    let mut damaged = 0;
    let mut bases: Vec<(ObjectName, Walked)> = Vec::new();
    let mut segments: Vec<(ObjectName, Segment)> = Vec::new();
    for name in names {
        let hex = name.to_hex();
        let Some(bytes) = salvage_get(storage, &name)? else {
            damaged += 1;
            warn!("index object {hex} is listed but missing");
            continue;
        };
        if ObjectName::of(&bytes) != name {
            damaged += 1;
            warn!("index object {hex} does not match its name");
            continue;
        }
        let decoded = match bytes.first().copied() {
            Some(t) if t == ObjectType::IndexBase as u8 => {
                decode_unnamed_base(bytes, crypto, anchor).map(|(cache, hash, entries)| {
                    let state = (hash, entries);
                    bases.push((name, Walked { cache, state }));
                })
            }
            Some(t) if t == ObjectType::IndexSegment as u8 => {
                decode_unnamed_segment(&bytes, crypto).map(|seg| segments.push((name, seg)))
            }
            Some(t) => return Ok(HeadlessOutcome::Refused(unknown_object_refusal(&name, t))),
            None => Err(invalid("empty object")),
        };
        match decoded {
            Ok(()) => {}
            // The bytes match their name, so a writer stored this codec.
            Err(VykarError::UnknownCompressionTag(tag)) => {
                return Ok(HeadlessOutcome::Refused(unknown_compression_refusal(
                    &name, tag,
                )));
            }
            Err(e) if is_object_damage(&e) => {
                damaged += 1;
                warn!("index object {hex}: {e}");
            }
            Err(e) => return Err(e),
        }
    }

    let empty: State = (head::empty_state_hash(), 0);
    let mut by_prev: HashMap<State, Vec<usize>> = HashMap::new();
    for (i, (_, seg)) in segments.iter().enumerate() {
        by_prev
            .entry((seg.prev_state_hash, seg.prev_entries))
            .or_default()
            .push(i);
    }
    let n_bases = bases.len();
    let mut starts: Vec<(Option<ObjectName>, Walked)> =
        bases.into_iter().map(|(n, w)| (Some(n), w)).collect();
    let follows_empty = by_prev.contains_key(&empty);
    if follows_empty || starts.is_empty() {
        let (tmp, hash, entries) = FullCacheWriter::new_beside(anchor)?.finish(None)?;
        let cache = tmp.into_temp_path();
        if !follows_empty {
            // Nothing usable: the empty index.
            return Ok(HeadlessOutcome::Rebuilt(Headless {
                cache,
                bases: 0,
                applied: 0,
                unplaced: segments.len(),
                damaged,
            }));
        }
        let state = (hash, entries);
        starts.push((None, Walked { cache, state }));
    }

    let mut visited: HashSet<State> = HashSet::new();
    let mut used = vec![false; segments.len()];
    let mut ends: Vec<End> = Vec::new();
    let mut applied = 0;
    for (origin, start) in starts {
        if !visited.insert(start.state) {
            continue;
        }
        let mut path: HashSet<State> = HashSet::from([start.state]);
        let mut on_path: Vec<ObjectName> = Vec::new();
        let mut current = start;
        loop {
            let i = match by_prev
                .get(&current.state)
                .map(Vec::as_slice)
                .unwrap_or_default()
            {
                [] => {
                    ends.push(End {
                        origin,
                        segments: on_path,
                        at: current,
                    });
                    break;
                }
                [i] => *i,
                cands => {
                    let names = cands
                        .iter()
                        .filter_map(|&c| segments.get(c).map(|(n, _)| n.to_hex()))
                        .collect();
                    return Ok(HeadlessOutcome::Refused(fork_refusal(names, current.state)));
                }
            };
            let (seg_name, segment) = segments
                .get(i)
                .ok_or_else(|| VykarError::Other("index segment position out of range".into()))?;
            if let Some(u) = used.get_mut(i) {
                *u = true;
            }
            let prev = MmapFullIndexCache::open_verified(&current.cache).ok_or_else(|| {
                VykarError::Other("rebuilt index state failed verification".into())
            })?;
            let next = segment::apply_unnamed_to_temp(&prev, segment, anchor);
            // Unmap before `current` may be replaced (and its file deleted).
            drop(prev);
            match next {
                Ok((cache, hash, entries)) => {
                    applied += 1;
                    let state = (hash, entries);
                    if path.contains(&state) {
                        return Ok(HeadlessOutcome::Refused(cycle_refusal(state)));
                    }
                    if !visited.insert(state) {
                        // Joined an earlier walk.
                        break;
                    }
                    path.insert(state);
                    on_path.push(*seg_name);
                    current = Walked { cache, state };
                }
                Err(e) if is_object_damage(&e) => {
                    damaged += 1;
                    warn!("index segment {}: {e}", seg_name.to_hex());
                    ends.push(End {
                        origin,
                        segments: on_path,
                        at: current,
                    });
                    break;
                }
                Err(e) => return Err(e),
            }
        }
    }

    let unplaced = used.iter().filter(|u| !**u).count();
    if ends.len() > 1 {
        return Ok(HeadlessOutcome::Refused(several_ends_refusal(&ends)));
    }
    let Some(end) = ends.pop() else {
        return Err(VykarError::Other(
            "rebuilding the index without a head found no end state".into(),
        ));
    };
    if unplaced > 0 {
        warn!("{unplaced} index segment(s) follow no index state that can be rebuilt");
    }
    Ok(HeadlessOutcome::Rebuilt(Headless {
        cache: end.at.cache,
        bases: n_bases,
        applied,
        unplaced,
        damaged,
    }))
}

const HEADLESS_PREFIX: &str =
    "the index head cannot be read, and the objects in index.d/ do not determine one index \
     state: ";
const HEADLESS_CACHE_WAY: &str =
    "Nothing was changed. Run `vykar check --repair` on the machine that wrote to the \
     repository last (its local index cache holds the latest state)";

fn fork_refusal(names: Vec<String>, state: State) -> String {
    format!(
        "{HEADLESS_PREFIX}{} segments follow the same state ({} entries): {}. Repair cannot \
         tell which one the head named. {HEADLESS_CACHE_WAY}, or restore `index` from a copy \
         of the repository",
        names.len(),
        state.1,
        names.join(", ")
    )
}

fn cycle_refusal(state: State) -> String {
    format!(
        "{HEADLESS_PREFIX}the segments return to an earlier state ({} entries), so the order \
         of the changes cannot be told. {HEADLESS_CACHE_WAY}, or restore `index` from a copy \
         of the repository",
        state.1
    )
}

fn several_ends_refusal(ends: &[End]) -> String {
    let mut lines = String::new();
    for end in ends {
        let origin = match end.origin {
            Some(name) => format!("base {}", name.to_hex()),
            None => "the empty index".to_string(),
        };
        let objects: Vec<String> = end
            .origin
            .iter()
            .chain(&end.segments)
            .map(|n| n.to_hex())
            .collect();
        let objects = if objects.is_empty() {
            "none".to_string()
        } else {
            objects.join(", ")
        };
        lines.push_str(&format!(
            "\n  - from {origin}: {} segment(s) applied, {} entries; objects: {objects}",
            end.segments.len(),
            end.at.state.1
        ));
    }
    format!(
        "{HEADLESS_PREFIX}they lead to {} different index states:{lines}\nOne of them can be \
         the previous generation, which stays in index.d/ until a fold that leaves the \
         index state unchanged (the one at the end of `vykar compact`), or an object \
         left by an interrupted index write. {HEADLESS_CACHE_WAY}, restore `index` from a \
         copy of the repository, or remove the objects that do not belong from index.d/ and \
         run `vykar check --repair` again",
        ends.len()
    )
}

fn unknown_object_refusal(name: &ObjectName, tag: u8) -> String {
    format!(
        "index object {} in index.d/ is neither an index base nor an index segment (type tag \
         {tag}); it was probably written by a newer vykar. Upgrade vykar to repair this \
         repository. Nothing was changed",
        name.to_hex()
    )
}

fn unknown_compression_refusal(name: &ObjectName, tag: u8) -> String {
    format!(
        "index object {} in index.d/ uses a compression this vykar does not know (tag \
         {tag}); it was probably written by a newer vykar. Upgrade vykar to repair this \
         repository. Nothing was changed",
        name.to_hex()
    )
}

#[cfg(test)]
pub(crate) mod tests;
