use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use vykar_protocol::PACK_MAGIC;

/// Build a minimal pack file: PACK_MAGIC + version(1) + [u32_le_len | blob]...
/// Returns (pack bytes, vec of (offset, length) for each blob - offset points
/// past the length prefix, matching what repack's keep_blobs expects).
pub(crate) fn build_pack(blobs: &[&[u8]]) -> (Vec<u8>, Vec<(u64, u64)>) {
    let mut buf = Vec::new();
    buf.extend_from_slice(PACK_MAGIC);
    buf.push(0x01); // version

    let mut refs = Vec::new();
    for blob in blobs {
        let offset = buf.len() as u64;
        let len = blob.len() as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(blob);
        refs.push((offset + 4, blob.len() as u64));
    }
    (buf, refs)
}

/// Write a pack file to disk and return its storage key (packs/<shard>/<hex>).
pub(crate) fn write_pack(tmp: &std::path::Path, pack_bytes: &[u8]) -> String {
    // Hash the content to get the pack name (matches how repack hashes).
    let pack_id = blake2b_256_hex(pack_bytes);
    let shard = &pack_id[..2];
    let key = format!("packs/{shard}/{pack_id}");
    let path = tmp.join("packs").join(shard).join(&pack_id);
    std::fs::write(&path, pack_bytes).expect("write pack file");
    key
}

pub(crate) fn blake2b_256_hex(data: &[u8]) -> String {
    let mut hasher = Blake2bVar::new(32).expect("valid output size");
    hasher.update(data);
    let mut out = [0u8; 32];
    hasher
        .finalize_variable(&mut out)
        .expect("valid output buffer length");
    hex::encode(out)
}

pub(crate) fn repack_body(ops: &[serde_json::Value]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "operations": ops })).unwrap()
}

pub(crate) fn repack_op(
    source_pack: &str,
    keep_blobs: &[(u64, u64)],
    delete_after: bool,
) -> serde_json::Value {
    let blobs: Vec<serde_json::Value> = keep_blobs
        .iter()
        .map(|(offset, length)| serde_json::json!({ "offset": offset, "length": length }))
        .collect();
    serde_json::json!({
        "source_pack": source_pack,
        "keep_blobs": blobs,
        "delete_after": delete_after,
    })
}
