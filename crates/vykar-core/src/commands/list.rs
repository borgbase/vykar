use serde::Deserialize;

use crate::config::VykarConfig;
use crate::repo::format::{unpack_object_expect_with_context, ObjectType};
use crate::repo::manifest::SnapshotEntry;
use crate::repo::Repository;
use crate::snapshot::item::Item;
use crate::snapshot::SnapshotMeta;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::util::open_repo;
use crate::repo::OpenOptions;

/// List all snapshots in the repository.
pub fn list_snapshots(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<Vec<SnapshotEntry>> {
    let repo = open_repo(config, passphrase, OpenOptions::new())?;
    Ok(repo.manifest().snapshots.clone())
}

/// List all items in a specific snapshot.
/// Tries the local restore cache first to avoid downloading the full index.
/// Falls back to the full index (with blob cache) on cache miss.
pub fn list_snapshot_items(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<Vec<Item>> {
    let (mut repo, _session_guard) =
        super::util::open_repo_with_read_session(config, passphrase, OpenOptions::new())?;

    // Resolve "latest" or exact snapshot name
    let resolved_name = repo
        .manifest()
        .resolve_snapshot(snapshot_name)?
        .name
        .clone();

    // Try restore cache first (avoids loading the full index entirely)
    if let Some(ref cache) = repo.open_restore_cache() {
        match load_snapshot_items_via_lookup(&mut repo, &resolved_name, |id| cache.lookup(id)) {
            Ok(items) => return Ok(items),
            Err(VykarError::ChunkNotInIndex(_)) => {
                // Restore cache incomplete — fall through to full index
            }
            Err(_) => {
                // Decryption/read error (stale cache) — fall through to full index
            }
        }
    }

    // Fall back to full index load (benefits from blob cache)
    repo.load_chunk_index()?;
    load_snapshot_items(&mut repo, &resolved_name)
}

/// List all snapshots with their stats (loaded from snapshot metadata).
pub fn list_snapshots_with_stats(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<Vec<(SnapshotEntry, crate::snapshot::SnapshotStats)>> {
    let repo = open_repo(config, passphrase, OpenOptions::new())?;
    let entries = repo.manifest().snapshots.clone();
    let mut result = Vec::with_capacity(entries.len());
    for entry in entries {
        let stats = match load_snapshot_meta(&repo, &entry.name) {
            Ok(meta) => meta.stats,
            Err(_) => crate::snapshot::SnapshotStats::default(),
        };
        result.push((entry, stats));
    }
    Ok(result)
}

/// Get metadata for a specific snapshot.
pub fn get_snapshot_meta(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<SnapshotMeta> {
    let repo = open_repo(config, passphrase, OpenOptions::new())?;
    let resolved_name = repo
        .manifest()
        .resolve_snapshot(snapshot_name)?
        .name
        .clone();
    load_snapshot_meta(&repo, &resolved_name)
}

/// Load the SnapshotMeta for a snapshot by name.
pub fn load_snapshot_meta(repo: &Repository, snapshot_name: &str) -> Result<SnapshotMeta> {
    let entry = repo
        .manifest()
        .find_snapshot(snapshot_name)
        .ok_or_else(|| VykarError::SnapshotNotFound(snapshot_name.into()))?;

    let meta_data = repo
        .storage
        .get(&entry.id.storage_key())?
        .ok_or_else(|| VykarError::SnapshotNotFound(snapshot_name.into()))?;

    let meta_bytes = unpack_object_expect_with_context(
        &meta_data,
        ObjectType::SnapshotMeta,
        entry.id.as_bytes(),
        repo.crypto.as_ref(),
    )?;
    Ok(rmp_serde::from_slice(&meta_bytes)?)
}

/// Load and deserialize all items from a snapshot.
pub fn load_snapshot_items(repo: &mut Repository, snapshot_name: &str) -> Result<Vec<Item>> {
    let items_stream = load_snapshot_item_stream(repo, snapshot_name)?;
    decode_items_stream(&items_stream)
}

/// Load the raw concatenated item stream bytes for a snapshot.
pub fn load_snapshot_item_stream(repo: &mut Repository, snapshot_name: &str) -> Result<Vec<u8>> {
    let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
    load_item_stream_from_ptrs(repo, &snapshot_meta.item_ptrs)
}

/// Load item stream using a lookup closure instead of the chunk index.
/// Returns ChunkNotInIndex if any tree-pack chunk is missing from the lookup.
pub fn load_snapshot_item_stream_via_lookup<L>(
    repo: &mut Repository,
    snapshot_name: &str,
    lookup: L,
) -> Result<Vec<u8>>
where
    L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
{
    let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
    resolve_and_read(repo, &snapshot_meta.item_ptrs, |chunk_id, _repo| {
        lookup(chunk_id).ok_or(VykarError::ChunkNotInIndex(*chunk_id))
    })
}

/// Load the raw concatenated item stream bytes from chunk pointers using the chunk index.
pub fn load_item_stream_from_ptrs(repo: &mut Repository, item_ptrs: &[ChunkId]) -> Result<Vec<u8>> {
    resolve_and_read(repo, item_ptrs, |chunk_id, repo| {
        let entry = *repo
            .chunk_index()
            .get(chunk_id)
            .ok_or_else(|| VykarError::Other(format!("chunk not found: {chunk_id}")))?;
        Ok((entry.pack_id, entry.pack_offset, entry.stored_size))
    })
}

/// Resolve chunk locations and read them via coalesced range reads.
fn resolve_and_read<R>(repo: &mut Repository, item_ptrs: &[ChunkId], resolve: R) -> Result<Vec<u8>>
where
    R: Fn(&ChunkId, &Repository) -> Result<(PackId, u64, u32)>,
{
    let chunks: Vec<(ChunkId, PackId, u64, u32)> = item_ptrs
        .iter()
        .map(|chunk_id| {
            let (pack_id, pack_offset, stored_size) = resolve(chunk_id, repo)?;
            Ok((*chunk_id, pack_id, pack_offset, stored_size))
        })
        .collect::<Result<_>>()?;

    let mut items_stream = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut items_stream)?;
    Ok(items_stream)
}

/// Load and deserialize all items using a lookup closure.
pub fn load_snapshot_items_via_lookup<L>(
    repo: &mut Repository,
    snapshot_name: &str,
    lookup: L,
) -> Result<Vec<Item>>
where
    L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
{
    let items_stream = load_snapshot_item_stream_via_lookup(repo, snapshot_name, lookup)?;
    decode_items_stream(&items_stream)
}

/// Decode item stream bytes and call `visit` for each item in stream order.
pub fn for_each_decoded_item(
    items_stream: &[u8],
    mut visit: impl FnMut(Item) -> Result<()>,
) -> Result<()> {
    if items_stream.is_empty() {
        return Ok(());
    }

    // Items are encoded as concatenated MsgPack Item objects.
    let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(items_stream));
    while (de.position() as usize) < items_stream.len() {
        let item = Item::deserialize(&mut de)?;
        visit(item)?;
    }
    Ok(())
}

/// Stream items from a snapshot without materializing `Vec<Item>`.
pub fn for_each_snapshot_item(
    repo: &mut Repository,
    snapshot_name: &str,
    visit: impl FnMut(Item) -> Result<()>,
) -> Result<()> {
    let items_stream = load_snapshot_item_stream(repo, snapshot_name)?;
    for_each_decoded_item(&items_stream, visit)
}

fn decode_items_stream(items_stream: &[u8]) -> Result<Vec<Item>> {
    let mut items = Vec::new();
    for_each_decoded_item(items_stream, |item| {
        items.push(item);
        Ok(())
    })?;
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::decode_items_stream;
    use crate::snapshot::item::Item;

    #[test]
    fn decode_streamed_item_sequence() {
        let item = Item {
            path: "b.txt".into(),
            entry_type: crate::snapshot::item::ItemType::RegularFile,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: 0,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
        };

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&rmp_serde::to_vec(&item).unwrap());
        bytes.extend_from_slice(&rmp_serde::to_vec(&item).unwrap());

        let decoded = decode_items_stream(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].path, "b.txt");
        assert_eq!(decoded[1].path, "b.txt");
    }
}
