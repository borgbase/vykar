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

use std::path::{Path, PathBuf};

use tempfile::TempPath;
use tracing::debug;
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
use crate::index::dedup_cache::{FullCacheWriter, MmapFullIndexCache};
use crate::index::head::{BaseRef, IndexHead, ObjectName, SegmentRef};
use crate::index::segment::{self, ChainStep, Segment};

/// Directory of the write-once base and segment objects.
pub(crate) const INDEX_OBJECT_DIR: &str = "index.d/";

const BASE_CONTEXT: &[u8] = b"index.d/base";
const SEGMENT_CONTEXT: &[u8] = b"index.d/segment";

/// How often a reader re-reads the head after a named object vanished (a
/// fold started after the head was read) before giving up.
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

/// Decrypt and decode the object stored at `index`.
pub(crate) fn decode_head(data: &[u8], crypto: &dyn CryptoEngine) -> Result<IndexHead> {
    if data.first() == Some(&(ObjectType::ChunkIndex as u8)) {
        return Err(invalid(
            "the repository is in a segmented index format, but `index` holds a \
             monolithic index (written by an older vykar)",
        ));
    }
    let plain = unpack_object_expect_with_context(
        data,
        ObjectType::IndexHead,
        INDEX_OBJECT_CONTEXT,
        crypto,
    )?;
    IndexHead::decode(&plain)
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
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "used by the segmented index write path, not yet implemented"
    )
)]
pub(crate) struct PackedObject {
    pub name: ObjectName,
    pub bytes: Vec<u8>,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "used by the segmented index write path, not yet implemented"
    )
)]
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
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "used by the segmented index write path, not yet implemented"
    )
)]
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
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "used by the segmented index write path, not yet implemented"
    )
)]
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

/// GET `keys` with up to `width` requests in flight. `None` if any is absent.
fn fetch_all(
    storage: &dyn StorageBackend,
    keys: &[String],
    width: usize,
) -> Result<Option<Vec<Vec<u8>>>> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    let width = width.clamp(1, keys.len().max(1));
    if width == 1 {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            match storage.get(key)? {
                Some(bytes) => out.push(bytes),
                None => return Ok(None),
            }
        }
        return Ok(Some(out));
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
    for r in results.into_inner().unwrap_or_else(|e| e.into_inner()) {
        match r {
            Some(Ok(Some(bytes))) => out.push(bytes),
            Some(Ok(None)) => return Ok(None),
            Some(Err(e)) => return Err(e),
            // Skipped after another key failed or was absent.
            None => {}
        }
    }
    if out.len() != keys.len() {
        return Ok(None);
    }
    Ok(Some(out))
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

/// Anchor for temp files when there is no cache directory.
fn temp_anchor() -> PathBuf {
    std::env::temp_dir().join("vykar-full-index-cache")
}

/// Bring a local full index cache to `head`'s state.
///
/// Readers take no lock, so a fold may delete the objects of the head they
/// read. A missing object is answered by re-reading the head, at most
/// [`MAX_HEAD_RETRIES`] times.
pub(crate) fn sync_full_cache(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    mut head: IndexHead,
    mut location: CacheLocation<'_>,
    width: usize,
) -> Result<SyncedIndex> {
    let mut attempt = 0;
    loop {
        match sync_once(storage, crypto, &head, &mut location, width)? {
            Some((cache, source)) => {
                return Ok(SyncedIndex {
                    head,
                    cache,
                    scratch: location.scratch,
                    source,
                })
            }
            None if attempt < MAX_HEAD_RETRIES => {
                attempt += 1;
                debug!(attempt, "index object vanished, re-reading the head");
                head = fetch_head(storage, crypto)?;
            }
            None => {
                return Err(invalid(
                    "index objects keep vanishing while reading the index; \
                     another client is rewriting it, retry later",
                ))
            }
        }
    }
}

/// One attempt of [`sync_full_cache`]. `Ok(None)` when an object named by
/// `head` is absent.
fn sync_once(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    head: &IndexHead,
    location: &mut CacheLocation<'_>,
    width: usize,
) -> Result<Option<(MmapFullIndexCache, SyncSource)>> {
    let target_state = (head.state_hash, head.entries);
    let chain = head.chain_states();

    // A local cache at some state of the chain, latest first.
    let mut start: Option<(MmapFullIndexCache, usize)> = None;
    if location.use_local {
        let candidates = location.scratch.as_deref().into_iter().chain(location.path);
        for path in candidates {
            let Some(cache) = MmapFullIndexCache::open_verified(path) else {
                continue;
            };
            let state = (cache.state_hash(), u64::from(cache.entry_count()));
            if state == target_state {
                return Ok(Some((cache, SyncSource::UpToDate)));
            }
            if let Some(k) = chain.iter().rposition(|s| *s == state) {
                if start.as_ref().is_none_or(|(_, best)| k > *best) {
                    start = Some((cache, k));
                }
            }
        }
    }

    let anchor = match location.path {
        Some(p)
            if p.parent()
                .is_some_and(|d| std::fs::create_dir_all(d).is_ok()) =>
        {
            p.to_path_buf()
        }
        _ => temp_anchor(),
    };

    let downloaded = start.is_none();
    // `prev_tmp` keeps a downloaded base alive while it is mapped.
    let (prev, from, prev_tmp) = match start {
        Some((cache, k)) => (cache, k, None),
        None => {
            let tmp = match &head.base {
                Some(base_ref) => {
                    let Some(bytes) = storage.get(&object_key(&base_ref.name))? else {
                        return Ok(None);
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
            (cache, 0, Some(tmp))
        }
    };

    let pending = head
        .segments
        .get(from..)
        .ok_or_else(|| invalid("index chain position out of range"))?;
    let keys: Vec<String> = pending.iter().map(|s| object_key(&s.name)).collect();
    let Some(fetched) = fetch_all(storage, &keys, width)? else {
        return Ok(None);
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

    let result = if steps.is_empty() {
        // A local cache at the head's state returned above, so this is a
        // downloaded base that already is the head's state.
        drop(prev);
        prev_tmp.ok_or_else(|| invalid("index chain has no segments to apply"))?
    } else {
        let (tmp, _, _) = segment::apply_to_temp(&prev, &steps, &anchor)?;
        drop(prev);
        drop(prev_tmp);
        tmp
    };

    let source = if downloaded {
        SyncSource::Downloaded {
            segments: steps.len(),
        }
    } else {
        SyncSource::CaughtUp {
            segments: steps.len(),
        }
    };
    let cache = install(result, location)?;
    if (cache.state_hash(), u64::from(cache.entry_count())) != target_state {
        return Err(invalid("index state after sync differs from the head"));
    }
    Ok(Some((cache, source)))
}

/// Move a finished cache to `location.path`, or keep it as the scratch file
/// when there is no path or the rename fails (Windows: the target is mapped
/// by another process). Returns it opened.
fn install(tmp: TempPath, location: &mut CacheLocation<'_>) -> Result<MmapFullIndexCache> {
    let path = match location.path {
        Some(path) if location.use_local => match tmp.persist(path) {
            Ok(()) => {
                location.scratch = None;
                path.to_path_buf()
            }
            Err(e) => {
                debug!(error = %e.error, "keeping the index cache in its temp file");
                let p = e.path.to_path_buf();
                location.scratch = Some(e.path);
                p
            }
        },
        _ => {
            let p = tmp.to_path_buf();
            location.scratch = Some(tmp);
            p
        }
    };
    MmapFullIndexCache::open_verified(&path)
        .ok_or_else(|| invalid("index cache failed verification after sync"))
}

#[cfg(test)]
pub(crate) mod tests;
