//! Deciding which packs named in a recovered `pending_index` journal are safe
//! to adopt.
//!
//! A journal entry claims "pack P holds these chunks at these offsets". A
//! journal lists a pack only after its atomic upload returned `Ok`, so the
//! object at P's key is a complete upload of P. The length check here guards
//! against a rename lost to a server power failure and anything foreign at
//! the key.

use std::collections::{HashMap, HashSet};

use tracing::warn;

use crate::index::PendingPackEntry;
use vykar_storage::StorageBackend;
use vykar_types::pack_id::PackId;

/// Exact on-disk length of a pack: `header | (u32 len | blob)*` with no
/// trailer, so the last blob's end. `None` on overflow or no chunks.
pub(crate) fn expected_pack_len(entry: &PendingPackEntry) -> Option<u64> {
    let mut max = None;
    for c in &entry.chunks {
        let end = c.pack_offset.checked_add(u64::from(c.stored_size))?;
        max = Some(max.map_or(end, |m: u64| m.max(end)));
    }
    max
}

/// Return the subset of `entries` whose packs are safe to adopt. Every
/// rejected pack is logged here.
pub(crate) fn verify_journal_packs(
    storage: &dyn StorageBackend,
    entries: &[PendingPackEntry],
) -> HashSet<PackId> {
    let mut sized: Vec<(&PendingPackEntry, u64)> = Vec::with_capacity(entries.len());
    for entry in entries {
        match expected_pack_len(entry) {
            Some(len) => sized.push((entry, len)),
            None => warn!(
                pack_id = %entry.pack_id,
                "pending_index: pack has no chunks or offsets overflow, skipping"
            ),
        }
    }

    let actual = lookup_sizes(storage, sized.iter().map(|(e, _)| &e.pack_id));
    let mut accepted = HashSet::with_capacity(sized.len());
    for (entry, expected) in sized {
        match actual.get(&entry.pack_id).copied().flatten() {
            Some(size) if size == expected => {
                accepted.insert(entry.pack_id);
            }
            Some(size) => warn!(
                pack_id = %entry.pack_id,
                expected,
                actual = size,
                "pending_index: pack length mismatch, skipping"
            ),
            None => warn!(
                pack_id = %entry.pack_id,
                "pending_index: pack missing from storage, skipping"
            ),
        }
    }
    accepted
}

/// Object sizes for `packs`, from one sized listing per shard. Falls back to
/// `size()` when the listing fails or does not report a size. `None` means
/// missing (or the size lookup itself failed).
fn lookup_sizes<'a>(
    storage: &dyn StorageBackend,
    packs: impl Iterator<Item = &'a PackId>,
) -> HashMap<PackId, Option<u64>> {
    let mut by_shard: HashMap<String, Vec<PackId>> = HashMap::new();
    for pack_id in packs {
        by_shard
            .entry(format!("packs/{}", pack_id.shard_prefix()))
            .or_default()
            .push(*pack_id);
    }

    let size_of = |pack_id: &PackId| match storage.size(&pack_id.storage_key()) {
        Ok(size) => size,
        Err(e) => {
            warn!(pack_id = %pack_id, "pending_index: size lookup failed: {e}");
            None
        }
    };

    let mut out = HashMap::new();
    for (shard, pack_ids) in by_shard {
        let listed: Option<HashMap<String, Option<u64>>> = match storage.list_sized(&shard) {
            Ok(objs) => Some(objs.into_iter().map(|o| (o.key, o.size)).collect()),
            Err(e) => {
                warn!(
                    "pending_index: failed to list {shard}: {e}, falling back to per-pack checks"
                );
                None
            }
        };
        for pack_id in pack_ids {
            let size = match &listed {
                Some(map) => match map.get(&pack_id.storage_key()) {
                    Some(Some(size)) => Some(*size),
                    Some(None) => size_of(&pack_id),
                    None => None,
                },
                None => size_of(&pack_id),
            };
            out.insert(pack_id, size);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::PendingChunkEntry;
    use vykar_types::chunk_id::ChunkId;

    #[test]
    fn expected_len_is_last_blob_end() {
        let e = PendingPackEntry {
            pack_id: PackId::from_bytes([0; 32]),
            chunks: vec![
                PendingChunkEntry {
                    chunk_id: ChunkId::from_bytes([1; 32]),
                    stored_size: 10,
                    pack_offset: 13,
                },
                PendingChunkEntry {
                    chunk_id: ChunkId::from_bytes([2; 32]),
                    stored_size: 5,
                    pack_offset: 27,
                },
            ],
        };
        assert_eq!(expected_pack_len(&e), Some(32));
        let empty = PendingPackEntry {
            pack_id: e.pack_id,
            chunks: Vec::new(),
        };
        assert_eq!(expected_pack_len(&empty), None);
    }
}
