//! Entry points for the `fuzz/` crate, compiled only with the `fuzzing`
//! feature. Not part of the API.

use std::io::Read;
use std::path::Path;

use tempfile::TempPath;
use vykar_crypto::CryptoEngine;
use vykar_types::error::{Result, VykarError};

use crate::index::dedup_cache::MmapFullIndexCache;
use crate::index::head::{BaseRef, ObjectName, SegmentRef};
use crate::index::segment::Segment;
use crate::repo::index_store;

/// AEAD context of a stored index base.
pub const BASE_CONTEXT: &[u8] = index_store::BASE_CONTEXT;
/// AEAD context of a stored index segment.
pub const SEGMENT_CONTEXT: &[u8] = index_store::SEGMENT_CONTEXT;

/// `repo::index_store::decode_segment`.
pub fn decode_segment(
    seg: &SegmentRef,
    bytes: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<Segment> {
    index_store::decode_segment(seg, bytes, crypto)
}

/// `repo::index_store::decode_base_to_temp`.
pub fn decode_base_to_temp(
    base_ref: &BaseRef,
    bytes: Vec<u8>,
    crypto: &dyn CryptoEngine,
    target: &Path,
) -> Result<TempPath> {
    index_store::decode_base_to_temp(base_ref, bytes, crypto, target)
}

/// `repo::index_store::pack_segment`: the object's name and stored bytes.
pub fn pack_segment(seg: &Segment, crypto: &dyn CryptoEngine) -> Result<(ObjectName, Vec<u8>)> {
    let packed = index_store::pack_segment(seg, crypto)?;
    Ok((packed.name, packed.bytes))
}

/// `repo::index_store::pack_base`: the object's name and stored bytes.
pub fn pack_base(
    cache: &MmapFullIndexCache,
    crypto: &dyn CryptoEngine,
) -> Result<(ObjectName, Vec<u8>)> {
    let packed = index_store::pack_base(cache, crypto)?;
    Ok((packed.name, packed.bytes))
}

/// `repo::index_store::is_object_damage`.
pub fn is_object_damage(e: &VykarError) -> bool {
    index_store::is_object_damage(e)
}

/// Read `compress::zstd_metadata_reader(data)` to its end.
pub fn read_zstd_metadata(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    crate::compress::zstd_metadata_reader(data)?
        .read_to_end(&mut out)
        .map_err(|e| VykarError::Decompression(format!("zstd: {e}")))?;
    Ok(out)
}
