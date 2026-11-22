//! Entry points for the `fuzz/` crate, compiled only with the `fuzzing`
//! feature. Not part of the API.

use std::io::Read;
use std::path::{Path, PathBuf};

use vykar_crypto::CryptoEngine;
use vykar_types::error::{Result, VykarError};

use crate::index::head::{BaseRef, ObjectName, SegmentRef};
use crate::index::segment_v2::SegmentBody;
use crate::repo::ChunkerParams;
use crate::repo::index_store;
use crate::snapshot::item::Item;
use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;

/// The byte-string type of `SweepRecord::snapshots`, for seeds.
pub use serde_bytes::ByteBuf;

/// AEAD context of a stored index base.
pub const BASE_CONTEXT: &[u8] = index_store::BASE_CONTEXT;
/// AEAD context of a stored index segment.
pub const SEGMENT_CONTEXT: &[u8] = index_store::SEGMENT_CONTEXT;

/// `repo::index_store::decode_segment_v2`.
pub fn decode_segment_v2(
    seg: &SegmentRef,
    bytes: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<SegmentBody> {
    index_store::decode_segment_v2(seg, bytes, crypto)
}

/// `repo::index_store::decode_base_v2_to_temp`, then the verified install
/// of the table file as a sync does it; the table's record count.
pub fn decode_base_v2(
    base_ref: &BaseRef,
    bytes: Vec<u8>,
    crypto: &dyn CryptoEngine,
    anchor: &Path,
) -> Result<u64> {
    let tmp = index_store::decode_base_v2_to_temp(base_ref, bytes, crypto, anchor)?;
    Ok(crate::index::ChunkTable::install(tmp, None, None)?.len() as u64)
}

/// `repo::index_store::pack_segment_v2`: the object's name and stored bytes.
pub fn pack_segment_v2(
    seg: &SegmentBody,
    crypto: &dyn CryptoEngine,
) -> Result<(ObjectName, Vec<u8>)> {
    let packed = index_store::pack_segment_v2(seg, crypto)?;
    Ok((packed.name, packed.bytes))
}

/// `repo::index_store::pack_base_body`: the object's name and stored bytes.
pub fn pack_base_body(body: &[u8], crypto: &dyn CryptoEngine) -> Result<(ObjectName, Vec<u8>)> {
    let packed = index_store::pack_base_body(body, crypto)?;
    Ok((packed.name, packed.bytes))
}

/// The pack table and the records (chunk ID, pack slot, offset, stored
/// size) of a valid base v2 body.
#[allow(clippy::type_complexity)]
pub fn base_v2_contents(
    body: &[u8],
) -> Result<(Vec<(PackId, u32)>, Vec<(ChunkId, u32, u32, u32)>)> {
    let view = crate::index::base_v2::BaseView::new(body)?;
    let packs = (0..view.pack_count()).map(|s| view.pack_at(s)).collect();
    let records = (0..view.entry_count()).map(|p| view.record_at(p)).collect();
    Ok((packs, records))
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

/// `repo::open::derive_plaintext_chunk_id_key`.
pub fn plaintext_chunk_id_key(repo_id: &[u8], format: crate::repo::RepoFormat) -> [u8; 32] {
    crate::repo::derive_plaintext_chunk_id_key(repo_id, format)
}

/// Chunk lengths backup cuts `data` into (`chunker::ChunkReader`, bounded
/// by the input length as backup bounds it by the file size).
pub fn chunk_lengths(data: &[u8], config: &ChunkerParams) -> std::io::Result<Vec<usize>> {
    let mut reader = crate::chunker::ChunkReader::new(data.take(data.len() as u64), config);
    let mut lengths = Vec::new();
    while let Some(chunk) = reader.next_chunk()? {
        lengths.push(chunk.len());
    }
    Ok(lengths)
}

/// Chunk lengths of `chunker::chunk_stream` (command dumps) over `data`.
pub fn stream_chunk_lengths(data: &[u8], config: &ChunkerParams) -> Result<Vec<usize>> {
    crate::chunker::chunk_stream(data, config)
        .map(|chunk| {
            chunk
                .map(|c| c.length)
                .map_err(|e| VykarError::Other(format!("chunker: {e}")))
        })
        .collect()
}

/// The path restore writes `item` to, relative to the destination
/// (`sanitize_item_into`), or its refusal.
pub fn restore_rel_path(item: &Item) -> Result<PathBuf> {
    let mut out = PathBuf::new();
    crate::commands::restore::sanitize_item_into(item, &mut out)?;
    Ok(out)
}

/// A symlink's target as restore builds it, and whether restore flags it
/// as absolute or escaping its parent.
pub fn symlink_target(item: &Item) -> (PathBuf, bool) {
    use crate::commands::restore::{SymlinkSafety, classify_symlink_target_path};
    let target = crate::commands::restore::symlink_target_path(item);
    let flagged = classify_symlink_target_path(&target) != SymlinkSafety::Safe;
    (target, flagged)
}
