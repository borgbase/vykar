//! Storage side of the index (format v6).
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
//! [`sync_index`] brings an in-memory index (`index::table`) to a head:
//! the base's local table file (downloaded once per base), then the head's
//! segments applied in memory, from the local segment cache or fetched.

use std::collections::HashSet;
use std::io::{Read, Write};
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
use crate::index::LiveIndex;
use crate::index::base_v2::BASE_V2_VERSION;
use crate::index::head::{self, BaseRef, IndexHead, ObjectName, ObjectRef, SegmentRef};
use crate::index::segment_v2::SegmentBody;
use crate::index::sweep_record::SweepRecord;
use crate::index::table::{self, ChunkTable, IndexState, TableFileWriter};

/// Directory of the write-once base and segment objects.
pub(crate) const INDEX_OBJECT_DIR: &str = "index.d/";

pub(crate) const BASE_CONTEXT: &[u8] = b"index.d/base";
pub(crate) const SEGMENT_CONTEXT: &[u8] = b"index.d/segment";

/// Directory of the heads, one per generation.
pub(crate) const HEAD_PREFIX: &str = "index.h/";
pub(crate) const HEAD_CONTEXT: &[u8] = b"index.h";
pub(crate) const SWEEP_RECORD_CONTEXT: &[u8] = b"index.d/sweep";

/// How long an old head (below the kept generations) is kept, by the
/// storage's clock (04's T_idx).
pub(crate) const INDEX_GC_GRACE: std::time::Duration = super::ages::T_IDX;

/// How long an unreferenced `index.d/` object is kept, by the storage's
/// clock: T_pack (12 h), not T_idx. A collector deletes an object O that no
/// listed retained head names only once now − mtime(O) reaches this grace,
/// "now" being the mtime of the head the collector created. A writer whose
/// head names O creates it above that head's generation, so after the
/// collector's listing: at least this grace after mtime(O), and mtime(O) is
/// at or after the writer's stamp taken before O's PUT started. A writer
/// refuses to create a head when its first object PUT started more than
/// [`super::ages::INDEX_WRITE_LIMIT`] (this grace − M) minus the landing
/// allowance before (about 11 h 20 min), and fails before its snapshot
/// after a create that returned more than `INDEX_WRITE_LIMIT` after it, so
/// no head it publishes names an object a collector judged old (M absorbs
/// the rate difference of the two clocks).
/// Objects a write did not PUT (kept segments, the predecessor's base) are
/// named by the predecessor head, which the collector retains. Without the
/// lock this is what keeps a slow writer's objects; under it, it only makes
/// garbage stay longer: every fold in the last 12 h leaves its predecessor's
/// base and folded segments, and each lost fold attempt its own base.
pub(crate) const INDEX_OBJECT_GRACE: std::time::Duration = super::ages::T_PACK;

/// The two graces of [`collect_index_garbage`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IndexGcGrace {
    /// Old heads below the kept generations ([`INDEX_GC_GRACE`]).
    pub heads: std::time::Duration,
    /// Unreferenced `index.d/` objects ([`INDEX_OBJECT_GRACE`]).
    pub objects: std::time::Duration,
}

impl IndexGcGrace {
    pub(crate) const DEFAULT: Self = Self {
        heads: INDEX_GC_GRACE,
        objects: INDEX_OBJECT_GRACE,
    };

    /// Both graces `grace` (tests).
    #[cfg(test)]
    pub(crate) const fn both(grace: std::time::Duration) -> Self {
        Self {
            heads: grace,
            objects: grace,
        }
    }
}

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

/// PUT the head at `head_key(head.generation)` with a plain put: init's
/// first head (nothing can contend before the repository exists) and tests.
/// Every index write creates its head with [`create_head`].
pub(crate) fn put_head(
    storage: &dyn StorageBackend,
    head: &IndexHead,
    crypto: &dyn CryptoEngine,
) -> Result<()> {
    storage.put(&head_key(head.generation), &pack_head(head, crypto)?)
}

/// How a head create ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeadPut {
    /// The head at the key is the one this write sent (this call created
    /// it, or an earlier attempt of the same bytes did).
    Created,
    /// Another head holds the generation (or held it and was collected).
    Taken,
}

/// The notice a repository handle logged when its heads fell back to plain
/// PUTs under the lock (04 "Fallback"). Kept on the handle
/// (`Repository::head_fallback`), so every opened repository logs once: the
/// backend's verdict holds for the backend instance, which the handle owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeadFallback {
    /// Logged at info: the backend does not offer put-if-absent, or its
    /// storage refuses the mechanism.
    NotOffered,
    /// Logged at warn: the storage accepted the creates but ignored the
    /// condition, or an S3 endpoint with conditional writes enabled
    /// (`s3_conditional_writes: on`, or `auto` on AWS) refused the header.
    Refused,
}

/// Create the head bytes `packed` at `head_key(generation)` (04 step 8):
/// put-if-absent where the backend offers it, else a plain PUT (the lock
/// protocol of 04 "Fallback").
///
/// - `Ok(Yes)` → `Created`.
/// - `Ok(AlreadyExists)` → GET the key: the same bytes → `Created` (the
///   backend's own retry after a lost reply, or a plaintext head of the same
///   state); other bytes or none (collected since, or a soft-deleted key on
///   S3) → `Taken`.
/// - `Err(UnsupportedBackend)` (a check concluded the storage refuses or
///   ignores the condition; normally seen at the fence first) → noted in
///   `fallback` ([`note_fallback`]), then a plain PUT → `Created`. Never
///   silently unconditional. A `lock_free` writer (04 step 9, O4: it holds
///   no lock, so a plain PUT could replace another writer's head) fails
///   instead, as it does when the backend offers no put-if-absent.
/// - Any other `Err` (outcome unknown) → GET the key: the same bytes →
///   `Created`; other bytes → `Taken`; absent, or the GET fails → the
///   original error (the create may still land, and only at a generation
///   nobody created).
///
/// A head over [`vykar_storage::MAX_PUT_IF_ABSENT_BYTES`] fails with
/// `InvalidFormat` (about 200 000 segments; unreachable) rather than being
/// written unconditionally.
pub(crate) fn create_head(
    storage: &dyn StorageBackend,
    generation: u64,
    packed: &[u8],
    fallback: &mut Option<HeadFallback>,
    lock_free: bool,
) -> Result<HeadPut> {
    let key = head_key(generation);
    let Some(cond) = storage.conditional() else {
        if lock_free {
            return Err(lock_free_without_condition(None));
        }
        storage.put(&key, packed)?;
        return Ok(HeadPut::Created);
    };
    let compare = |storage: &dyn StorageBackend| -> Result<Option<HeadPut>> {
        Ok(storage.get(&key)?.map(|stored| {
            if stored == packed {
                HeadPut::Created
            } else {
                HeadPut::Taken
            }
        }))
    };
    match cond.put_if_absent(&key, packed) {
        Ok(vykar_storage::Created::Yes) => Ok(HeadPut::Created),
        Ok(vykar_storage::Created::AlreadyExists) => {
            Ok(compare(storage)?.unwrap_or(HeadPut::Taken))
        }
        Err(VykarError::UnsupportedBackend(reason)) if lock_free => {
            Err(lock_free_without_condition(Some(&reason)))
        }
        Err(VykarError::UnsupportedBackend(reason)) => {
            note_fallback(fallback, Some(&reason));
            storage.put(&key, packed)?;
            Ok(HeadPut::Created)
        }
        Err(VykarError::InvalidFormat(m)) => Err(VykarError::InvalidFormat(m)),
        Err(e) => match compare(storage) {
            Ok(Some(outcome)) => Ok(outcome),
            Ok(None) | Err(_) => Err(e),
        },
    }
}

/// The error of a lock-free writer (04 step 9) whose storage no longer
/// offers put-if-absent (`reason`: the check's or the refusal's verdict):
/// it holds no lock, so it never writes a head with a plain PUT (O4).
pub(crate) fn lock_free_without_condition(reason: Option<&str>) -> VykarError {
    VykarError::Other(format!(
        "the storage no longer offers put-if-absent ({}); a lock-free commit never writes \
         an index head with a plain PUT. Retry the backup",
        reason.unwrap_or("not offered")
    ))
}

/// Log that heads fall back to plain PUTs under the lock, once per handle:
/// nothing when `noted` is already set. `reason`: the check's verdict, or
/// `None` for a backend that does not offer put-if-absent at all. At warn
/// when the storage ignored the condition or an S3 endpoint with
/// conditional writes enabled refused the header (the reason names
/// `s3_conditional_writes`), at info otherwise. Returns whether it logged.
pub(crate) fn note_fallback(noted: &mut Option<HeadFallback>, reason: Option<&str>) -> bool {
    if noted.is_some() {
        return false;
    }
    let refused = reason.is_some_and(|r| {
        r.contains(vykar_storage::IGNORES_CONDITION) || r.contains("s3_conditional_writes")
    });
    match reason {
        Some(reason) if refused => tracing::warn!(
            "the storage did not pass the put-if-absent check ({reason}); index heads are \
             written with plain PUTs under the repository lock"
        ),
        Some(reason) => tracing::info!(
            "put-if-absent not offered ({reason}); index heads are written with plain PUTs \
             under the repository lock"
        ),
        None => tracing::info!(
            "the storage backend offers no put-if-absent; index heads are written with plain \
             PUTs under the repository lock"
        ),
    }
    *noted = Some(if refused {
        HeadFallback::Refused
    } else {
        HeadFallback::NotOffered
    });
    true
}

/// What the object at `index.h/<gen>` is.
#[derive(Debug)]
pub(crate) enum StoredHead {
    Head(IndexHead),
    /// Decrypts, and names a head layout this binary does not read.
    UnknownLayout(u32),
    /// Layout 3, but names features this binary does not know.
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

/// Compress and encrypt a base body (the bytes of a table's body).
pub(crate) fn pack_base_body(body: &[u8], crypto: &dyn CryptoEngine) -> Result<PackedObject> {
    let packed = pack_object_streaming_with_context(
        ObjectType::IndexBase,
        BASE_CONTEXT,
        body.len() / 2 + 64,
        crypto,
        |buf| {
            compress::compress_stream_zstd(buf, 3, |w| {
                std::io::Write::write_all(w, body)?;
                Ok(())
            })
        },
    )?;
    Ok(PackedObject::new(packed))
}

/// Encode, compress and encrypt a keyed segment.
pub(crate) fn pack_segment_v2(
    seg: &SegmentBody,
    crypto: &dyn CryptoEngine,
) -> Result<PackedObject> {
    let body = seg.encode()?;
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

/// Verify, decrypt and decode a fetched segment (body version 2): the
/// output is bounded by the entry count the head records, and the decoded
/// kind and entry count must be the head's.
pub(crate) fn decode_segment_v2(
    r: &SegmentRef,
    bytes: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<SegmentBody> {
    verify_object(&r.name, r.size, bytes)?;
    let compressed = unpack_object_expect_with_context(
        bytes,
        ObjectType::IndexSegment,
        SEGMENT_CONTEXT,
        crypto,
    )?;
    let bound = 20u64.saturating_add(80u64.saturating_mul(r.entries));
    let mut body = Vec::new();
    compress::zstd_metadata_reader(&compressed)?
        .take(bound.saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|e| VykarError::Decompression(format!("zstd: {e}")))?;
    if body.len() as u64 > bound {
        return Err(invalid(format!(
            "index segment {} is larger than its head allows",
            r.name.to_hex()
        )));
    }
    let seg = SegmentBody::decode(&body, r.entries)?;
    if seg.kind != r.kind || seg.entries() != r.entries {
        return Err(invalid(format!(
            "index segment {} is not the segment its head names",
            r.name.to_hex()
        )));
    }
    Ok(seg)
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

/// Verify, decrypt and decode a fetched base into a table file in a temp
/// file beside `anchor`. The output is bounded by the entry count the head
/// records, and the body's entry count must be the head's; the rest of the
/// body is checked when the table is installed.
pub(crate) fn decode_base_v2_to_temp(
    base_ref: &BaseRef,
    mut bytes: Vec<u8>,
    crypto: &dyn CryptoEngine,
    anchor: &Path,
) -> Result<TempPath> {
    verify_object(&base_ref.name, base_ref.size, &bytes)?;
    let compressed = unpack_object_expect_with_context_in_place(
        &mut bytes,
        ObjectType::IndexBase,
        BASE_CONTEXT,
        crypto,
    )?;
    let bound = 12u64.saturating_add(80u64.saturating_mul(base_ref.entries));
    let mut reader = compress::zstd_metadata_reader(compressed)?;
    let mut w = TableFileWriter::with_limit(anchor, bound)?;
    let mut header = [0u8; 12];
    let mut header_len = 0usize;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(VykarError::Decompression(format!("zstd: {e}"))),
        };
        let chunk = buf.get(..n).unwrap_or_default();
        if header_len < header.len() {
            let take = (header.len() - header_len).min(n);
            if let (Some(dst), Some(src)) = (
                header.get_mut(header_len..header_len + take),
                chunk.get(..take),
            ) {
                dst.copy_from_slice(src);
            }
            header_len += take;
        }
        w.write_all(chunk).map_err(|e| {
            if e.kind() == std::io::ErrorKind::InvalidData {
                invalid(format!(
                    "index base {} is larger than its head allows",
                    base_ref.name.to_hex()
                ))
            } else {
                VykarError::from(e)
            }
        })?;
    }
    let entries = header
        .get(8..12)
        .filter(|_| header_len == header.len())
        .and_then(|b| b.try_into().ok())
        .map(u32::from_le_bytes);
    if header.first() == Some(&BASE_V2_VERSION) && entries.map(u64::from) != Some(base_ref.entries)
    {
        return Err(invalid(format!(
            "index base {} does not hold the entries its head names",
            base_ref.name.to_hex()
        )));
    }
    if let Some(&v) = header.first()
        && header_len > 0
        && v != BASE_V2_VERSION
    {
        return Err(VykarError::UnsupportedVersion(u32::from(v)));
    }
    w.finish()
}

/// What [`fetch_all`] got.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Fetched {
    /// Every object, in key order.
    All(Vec<Vec<u8>>),
    /// The position in `keys` of the first absent object.
    Absent(usize),
}

/// GET `keys` with up to `width` requests in flight. Absence and errors
/// are reported for the first failing key in key order.
pub(crate) fn fetch_all(
    storage: &dyn StorageBackend,
    keys: &[String],
    width: usize,
) -> Result<Fetched> {
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
// Bringing the in-memory index to a head
// ---------------------------------------------------------------------------

/// What [`sync_index`] had to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncSource {
    /// The live index or a local table already held the head's state.
    UpToDate,
    /// The base was local; `segments` were applied (from the local cache or
    /// fetched).
    CaughtUp { segments: usize },
    /// The base was downloaded, and `segments` applied over it.
    Downloaded { segments: usize },
}

/// Where the local index files live for one sync.
pub(crate) struct CacheLocation<'a> {
    /// The per-repo cache dir holding `index_base.*` and `index_segment.*`,
    /// when the repository has one and local files may be used. A base the
    /// sync downloads is published there.
    pub dir: Option<&'a Path>,
    /// The repository's index, when one is loaded: its table and applied
    /// segments are used without re-verification (verified when loaded).
    pub live: Option<&'a LiveIndex>,
    /// Whether local files may be used at all (`false` for `check`, which
    /// verifies every object the head names; nothing local is read or
    /// written).
    pub use_local: bool,
}

/// An index at a head's state, with no edits.
pub(crate) struct SyncedIndex {
    /// The head synced to; a re-read one if a named object vanished.
    pub head: IndexHead,
    pub index: LiveIndex,
    pub source: SyncSource,
}

/// Path of the anchor named by [`TEMP_ANCHOR_FILE`].
fn temp_anchor() -> PathBuf {
    std::env::temp_dir().join(TEMP_ANCHOR_FILE)
}

/// Where temp files of a table go: in `dir` when it exists or can be
/// created, otherwise in the system temp dir.
pub(crate) fn cache_anchor(dir: Option<&Path>) -> PathBuf {
    match dir {
        Some(d) if std::fs::create_dir_all(d).is_ok() => table::table_anchor(d),
        _ => temp_anchor(),
    }
}

/// Delete the local index files in `dir` that `head` does not name, by the
/// rules of [`table::prune_local_index_files`].
pub(crate) fn prune_local_files(dir: Option<&Path>, head: &IndexHead) {
    let Some(dir) = dir else {
        return;
    };
    let base = head.base.as_ref().map(|b| b.name);
    let segments: HashSet<ObjectName> = head.segments.iter().map(|s| s.name).collect();
    let deleted =
        table::prune_local_index_files(dir, base.as_ref(), &segments, table::TABLE_KEEP_AGE);
    if deleted > 0 {
        debug!(deleted, "deleted old local index files");
    }
}

/// Bring an index to `head`:
/// 1. the table: the live index's when its base is `head`'s, else
///    `index_base.<name>` in `location.dir`, else the base object (fetched,
///    decoded, published in the dir); the empty table without a base;
/// 2. the applied segments: the live index's when it has the same base and
///    its segments are a prefix of `head`'s (only the rest is applied),
///    else every segment of `head`;
/// 3. each needed segment: from the local cache, else fetched (all at
///    once, up to `width` requests in flight) and cached; decoded and
///    applied in the head's order.
///
/// Readers take no lock, so a fold may delete the objects of the head they
/// read. A missing object is answered by re-reading the head: a head that
/// still names it as live means it is lost (no writer deletes an object the
/// current head names), and the read fails with
/// [`VykarError::IndexObjectMissing`]; otherwise a fold moved the head and
/// the read is retried on the new one, at most [`MAX_HEAD_RETRIES`] times.
pub(crate) fn sync_index(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    mut head: IndexHead,
    location: CacheLocation<'_>,
    width: usize,
) -> Result<SyncedIndex> {
    let mut retries = 0;
    loop {
        let missing = match sync_once(storage, crypto, &head, &location, width)? {
            Attempt::Synced(index, source) => {
                if location.use_local {
                    prune_local_files(location.dir, &head);
                }
                return Ok(SyncedIndex {
                    head,
                    index: *index,
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

/// One attempt of [`sync_index`].
enum Attempt {
    Synced(Box<LiveIndex>, SyncSource),
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
    let dir = location.dir.filter(|_| location.use_local);
    let live = location
        .live
        .filter(|l| location.use_local && l.is_loaded());
    let base_name = head.base.as_ref().map(|b| b.name);
    let base_entries = head.base.as_ref().map_or(0, |b| b.entries);

    // 1-2. The starting index and the segments still to apply; `None`: the
    // base must be downloaded (after the segments, so that a missing segment
    // costs no base download).
    let start: Option<(LiveIndex, usize)> = match live.filter(|l| l.state().base == base_name) {
        Some(l)
            if l.state().segments.len() <= head.segments.len()
                && head
                    .segments
                    .iter()
                    .map(|s| s.name)
                    .zip(l.state().segments.iter())
                    .all(|(a, b)| a == *b) =>
        {
            Some((l.loaded_state(), l.state().segments.len()))
        }
        Some(l) => l.table().map(|t| (LiveIndex::from_table(Arc::clone(t)), 0)),
        None => match &head.base {
            None => Some((LiveIndex::from_table(Arc::new(ChunkTable::empty())), 0)),
            Some(base_ref) => dir
                .and_then(|d| ChunkTable::open_file(&table::base_table_path(d, &base_ref.name)))
                .filter(|t| t.len() as u64 == base_ref.entries)
                .map(|t| (LiveIndex::from_table(Arc::new(t)), 0)),
        },
    };
    let from = start.as_ref().map_or(0, |(_, k)| *k);
    let pending = head
        .segments
        .get(from..)
        .ok_or_else(|| invalid("index segment position out of range"))?;

    // 3. The segments: local copies, then one fetch for the rest.
    let mut bytes: Vec<Option<Vec<u8>>> = pending
        .iter()
        .map(|r| dir.and_then(|d| table::read_cached_segment(d, r)))
        .collect();
    let wanted: Vec<usize> = bytes
        .iter()
        .enumerate()
        .filter(|(_, b)| b.is_none())
        .map(|(i, _)| i)
        .collect();
    if !wanted.is_empty() {
        let keys: Vec<String> = wanted
            .iter()
            .filter_map(|&i| pending.get(i))
            .map(|r| object_key(&r.name))
            .collect();
        match fetch_all(storage, &keys, width)? {
            Fetched::All(fetched) => {
                for (&i, b) in wanted.iter().zip(fetched) {
                    if let Some(slot) = bytes.get_mut(i) {
                        *slot = Some(b);
                    }
                }
            }
            Fetched::Absent(k) => {
                let seg = wanted.get(k).and_then(|&i| pending.get(i)).ok_or_else(|| {
                    VykarError::Other(format!(
                        "index object fetch reported position {k} out of range"
                    ))
                })?;
                return Ok(Attempt::Missing(MissingObject {
                    name: seg.name,
                    kind: "segment",
                }));
            }
        }
    }

    let downloaded = start.is_none();
    let mut index = match start {
        Some((index, _)) => index,
        None => {
            let base_ref = head
                .base
                .as_ref()
                .ok_or_else(|| VykarError::Other("BUG: no base to download".into()))?;
            let Some(object) = storage.get(&object_key(&base_ref.name))? else {
                return Ok(Attempt::Missing(MissingObject {
                    name: base_ref.name,
                    kind: "base",
                }));
            };
            let tmp = decode_base_v2_to_temp(base_ref, object, crypto, &cache_anchor(dir))?;
            // Only a failed verification is damage; a local I/O error
            // (opening or mapping the decoded file) stays one.
            let table =
                ChunkTable::install(tmp, Some(base_ref.name), dir).map_err(|e| match e {
                    VykarError::InvalidFormat(_) => {
                        invalid("decoded index base failed verification")
                    }
                    e => e,
                })?;
            LiveIndex::from_table(Arc::new(table))
        }
    };
    if index.table().map_or(0, |t| t.len() as u64) != base_entries {
        return Err(invalid(
            "index base holds another entry count than its head names",
        ));
    }
    for (r, b) in pending.iter().zip(bytes) {
        let b =
            b.ok_or_else(|| VykarError::Other("BUG: an index segment was not fetched".into()))?;
        let seg = decode_segment_v2(r, &b, crypto)?;
        if let Some(d) = dir {
            table::write_cached_segment(d, &r.name, &b);
        }
        drop(b);
        index.apply_segment(r.name, &seg)?;
    }
    if *index.state() != IndexState::of_head(head) {
        return Err(invalid("index state after sync differs from the head"));
    }
    let source = if downloaded {
        SyncSource::Downloaded {
            segments: pending.len(),
        }
    } else if pending.is_empty() {
        SyncSource::UpToDate
    } else {
        SyncSource::CaughtUp {
            segments: pending.len(),
        }
    };
    Ok(Attempt::Synced(Box::new(index), source))
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

/// The mtime of every `index.d/` object one LIST_META returns, by name
/// (keys that are not object names are skipped). `None` = listed without
/// one.
pub(crate) fn list_index_object_mtimes(
    storage: &dyn StorageBackend,
) -> Result<std::collections::HashMap<ObjectName, Option<std::time::SystemTime>>> {
    Ok(storage
        .list_meta(INDEX_OBJECT_DIR)?
        .into_iter()
        .filter_map(|o| Some((object_name_of_key(&o.key)?, o.mtime)))
        .collect())
}

/// What [`collect_index_garbage`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct IndexGcStats {
    pub heads_deleted: usize,
    pub objects_deleted: usize,
    /// Why (part of) the run was skipped.
    pub skipped: Option<&'static str>,
}

/// Log a failed best-effort delete: at debug for a write refusal (an
/// append-only backend that does not advertise it, a read-only mount), at
/// warn otherwise.
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
///   and every head younger than `grace.heads`; delete the others;
/// - objects: keep every object a kept head names
///   ([`IndexHead::named_objects`]) and every object younger than
///   `grace.objects`; delete the others.
///
/// "Now" is the listed mtime of `latest`, the head the caller created (the
/// storage's clock, not the client's: 04 "Constants", the mtime of an object
/// this client has just written). Not the newest listed mtime: one entry
/// with a future mtime (a stepped server clock) would move "now" forward
/// and make every young unnamed object look old. When the heads listing
/// lacks `latest` (a stale listing) nothing is deleted (`skipped = "own
/// head not listed"`); when it lists `latest` without an mtime (a store
/// without mtimes), neither (`skipped = "no mtimes"`). Another entry
/// without an mtime counts as age 0. A kept head other
/// than `latest` that cannot be read stops the object part (`skipped =
/// "unreadable retained head"`); heads already deleted stay deleted. A head
/// whose delete failed is still treated as not kept, so its objects may go:
/// it is never read, because readers take the latest. Deletes are
/// best-effort and logged (at debug when refused, as on an append-only
/// server). Storage errors on the two listings are returned.
///
/// On a server that advertises append-only mode
/// ([`vykar_storage::ServerOps::append_only`]) nothing is listed or deleted
/// (`skipped = "append-only server"`): old heads and objects accumulate until
/// a compact in a maintenance window.
///
/// Precondition: the caller **created** `latest` (04 step 8), so a writer
/// that read an older head creates a generation at or below `latest` and
/// finds it taken; it never publishes a head naming what this run deletes.
/// `index.d/` is listed before `index.h/` (04's order): a writer that read
/// `latest` or later PUTs its new objects after that read, so an object the
/// heads listing does not cover was PUT after the object listing (not
/// listed, not deleted) or is younger than `grace.objects` at "now" (the
/// heads listing includes `latest`, so "now" is its mtime) — unless the
/// writer's head lands more than `grace.objects` after its first object
/// PUT started, which the lock excludes and the writer itself refuses
/// ([`INDEX_OBJECT_GRACE`]). On a plaintext repository an index object's name is a
/// function of its content, so a writer can re-create an old unreferenced
/// name the listing saw as old; harmless while the lock keeps collectors away
/// from writers (recorded for the owner with 04 step 9: plaintext
/// repositories may keep the lock for index writes).
pub(crate) fn collect_index_garbage(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    latest: &IndexHead,
    grace: IndexGcGrace,
) -> Result<IndexGcStats> {
    let mut stats = IndexGcStats::default();
    // An append-only server refuses every delete below: skip the listings too.
    if storage.server_ops().is_some_and(|s| s.append_only()) {
        stats.skipped = Some("append-only server");
        return Ok(stats);
    }
    // Objects before heads (04 "Compact" 1 and 7): see the doc.
    let objects: Vec<(ObjectName, Option<std::time::SystemTime>)> = storage
        .list_meta(INDEX_OBJECT_DIR)?
        .into_iter()
        .filter_map(|o| Some((object_name_of_key(&o.key)?, o.mtime)))
        .collect();
    let heads: Vec<(u64, Option<std::time::SystemTime>)> = storage
        .list_meta(HEAD_PREFIX)?
        .into_iter()
        .filter_map(|o| Some((vykar_protocol::head_generation(&o.key)?, o.mtime)))
        .collect();
    // "Now" on the store's clock: the mtime of the head the caller created,
    // listed after the objects. Not the newest listed mtime: one entry with
    // a future mtime would make every young unnamed object look old.
    let Some(own) = heads.iter().find(|h| h.0 == latest.generation) else {
        stats.skipped = Some("own head not listed");
        return Ok(stats);
    };
    let Some(now) = own.1 else {
        stats.skipped = Some("no mtimes");
        return Ok(stats);
    };
    // Age 0 when the mtime is missing or newer than "now".
    let old = |mtime: Option<std::time::SystemTime>, grace: std::time::Duration| {
        mtime.is_some_and(|m| now.duration_since(m).unwrap_or_default() >= grace)
    };

    let (doomed, kept) = partition_heads(&heads, latest.generation, now, grace.heads);

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
        .filter(|(name, mtime)| !keep.contains(name) && old(*mtime, grace.objects))
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

/// `heads` split into those a collection deletes (not `latest`, more than
/// [`HEADS_KEPT`] below it, and at least `grace` old at `now`; a missing
/// or future mtime is age 0) and those it keeps.
#[allow(clippy::type_complexity)]
fn partition_heads(
    heads: &[(u64, Option<std::time::SystemTime>)],
    latest: u64,
    now: std::time::SystemTime,
    grace: std::time::Duration,
) -> (
    Vec<(u64, Option<std::time::SystemTime>)>,
    Vec<(u64, Option<std::time::SystemTime>)>,
) {
    let floor = latest.saturating_sub(HEADS_KEPT);
    heads.iter().copied().partition(|&(generation, mtime)| {
        generation != latest
            && generation < floor
            && mtime.is_some_and(|m| now.duration_since(m).unwrap_or_default() >= grace)
    })
}

/// The head part of [`collect_index_garbage`] alone: one listing of
/// `index.h/`, then the deletes of the heads it would delete; no object is
/// listed, read or deleted. For the head a lock-free backup that added no
/// chunk creates (04 step 9, U9): such heads add no segment and trigger no
/// fold, so without this `index.h/` would grow by one key per unchanged
/// backup until the next fold or compact. Same precondition, "now" and
/// skips as [`collect_index_garbage`]; the objects old heads named are
/// collected by the next full collection, after their 12 h grace.
pub(crate) fn collect_old_heads(
    storage: &dyn StorageBackend,
    latest: &IndexHead,
    grace: std::time::Duration,
) -> Result<IndexGcStats> {
    let mut stats = IndexGcStats::default();
    if storage.server_ops().is_some_and(|s| s.append_only()) {
        stats.skipped = Some("append-only server");
        return Ok(stats);
    }
    let heads: Vec<(u64, Option<std::time::SystemTime>)> = storage
        .list_meta(HEAD_PREFIX)?
        .into_iter()
        .filter_map(|o| Some((vykar_protocol::head_generation(&o.key)?, o.mtime)))
        .collect();
    let Some(own) = heads.iter().find(|h| h.0 == latest.generation) else {
        stats.skipped = Some("own head not listed");
        return Ok(stats);
    };
    let Some(now) = own.1 else {
        stats.skipped = Some("no mtimes");
        return Ok(stats);
    };
    let (doomed, _) = partition_heads(&heads, latest.generation, now, grace);
    for &(generation, _) in &doomed {
        let key = head_key(generation);
        match storage.delete(&key) {
            Ok(()) => stats.heads_deleted += 1,
            Err(e) => log_failed_delete("index head", &key, &e),
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
