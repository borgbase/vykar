//! Pack maps: encrypted lists of the blobs of packs that carry no trailer.
//!
//! Layout (architecture.md, "Pack maps"):
//!
//! ~~~text
//! key    = packmaps/<hex(BLAKE3(stored))>
//! stored = envelope(ObjectType::PackMap, context b"packmap", zstd(body))
//! body   = [version u8 = 1][pack_count u32 LE] then pack_count x pack
//! pack   = [pack_id 32][entry_count u32 LE] then entry_count x entry
//! entry  = [chunk_id 32][offset u32 LE][stored_size u32 LE]
//! ~~~
//!
//! Packs are in increasing pack-ID order, each pack's entries in increasing
//! offset order without overlap. A map is write-once: its name is the hash of
//! its stored bytes, checked before anything else, as for `index.d/`. The
//! rebuild of the index from the packs (`index_rebuild`) prefers a map over
//! the pack's own trailer. Compact deletes a map once none of its packs is
//! left (`delete_dead_pack_maps`).

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, PoisonError};

use tracing::{debug, info, warn};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::Repo;
use super::bounded::try_for_each_bounded;
use super::control::CancelToken;
use super::format::{
    ObjectType, pack_object_streaming_with_context, unpack_object_expect_with_context,
};
use super::index_store::is_object_damage;
use crate::compress;
use crate::index::head::ObjectName;

/// AEAD object context of a pack map.
// Wire-format constant — DO NOT rename.
pub const PACK_MAP_CONTEXT: &[u8] = b"packmap";
/// The body version this binary writes and reads.
pub const PACK_MAP_BODY_VERSION: u8 = 1;
/// Bytes per entry in the body.
pub const PACK_MAP_ENTRY_LEN: usize = 40;
/// Reader bound on the entries of one map object.
pub const MAX_PACK_MAP_ENTRIES: usize = 400_000;
/// Writer: entries per map before a new one is started (whole packs only).
pub const PACK_MAP_TARGET_ENTRIES: usize = 65_536;
/// Directory of the maps.
pub(crate) const PACK_MAP_PREFIX: &str = "packmaps/";

/// `[version][pack_count u32]`.
const BODY_HEADER_LEN: usize = 5;
/// `[pack_id 32][entry_count u32]`.
const PACK_HEADER_LEN: usize = 36;
/// Lowest valid blob offset: the pack header plus one length prefix.
const FIRST_BLOB_OFFSET: u64 = vykar_protocol::PACK_HEADER_SIZE as u64 + 4;

/// One blob of a mapped pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackMapEntry {
    pub chunk_id: ChunkId,
    /// Offset of the blob's first byte (after its length prefix).
    pub offset: u32,
    pub stored_size: u32,
}

/// One map object: packs in ID order, each with its entries in offset order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackMap {
    pub packs: Vec<(PackId, Vec<PackMapEntry>)>,
}

fn invalid(msg: impl Into<String>) -> VykarError {
    VykarError::InvalidFormat(format!("pack map: {}", msg.into()))
}

fn take_u32(bytes: &[u8]) -> Result<(u32, &[u8])> {
    let (head, rest) = bytes
        .split_at_checked(4)
        .ok_or_else(|| invalid("short body"))?;
    let arr: [u8; 4] = head.try_into().map_err(|_| invalid("short body"))?;
    Ok((u32::from_le_bytes(arr), rest))
}

/// Why `entries` (sorted by offset) break the layout rules of a map, if
/// they do: an offset inside the pack header, overlapping blobs.
fn layout_violation(entries: &[PackMapEntry]) -> Option<String> {
    let mut min_offset = FIRST_BLOB_OFFSET;
    for e in entries {
        if u64::from(e.offset) < min_offset {
            return Some(format!(
                "blob at offset {} overlaps the header or the previous blob",
                e.offset
            ));
        }
        min_offset = u64::from(e.offset) + u64::from(e.stored_size) + 4;
    }
    None
}

impl PackMap {
    /// The body, uncompressed. Canonical: [`Self::decode_body`] accepts
    /// exactly one encoding of each map.
    pub fn encode_body(&self) -> Vec<u8> {
        let entries: usize = self.packs.iter().map(|(_, e)| e.len()).sum();
        let mut out = Vec::with_capacity(
            BODY_HEADER_LEN + PACK_HEADER_LEN * self.packs.len() + PACK_MAP_ENTRY_LEN * entries,
        );
        out.push(PACK_MAP_BODY_VERSION);
        // Counts come from the writer or from `decode_body`, both bounded by
        // MAX_PACK_MAP_ENTRIES.
        let count = |n: usize| u32::try_from(n).unwrap_or(u32::MAX).to_le_bytes();
        out.extend_from_slice(&count(self.packs.len()));
        for (pack_id, entries) in &self.packs {
            out.extend_from_slice(pack_id.as_bytes());
            out.extend_from_slice(&count(entries.len()));
            for e in entries {
                out.extend_from_slice(e.chunk_id.as_bytes());
                out.extend_from_slice(&e.offset.to_le_bytes());
                out.extend_from_slice(&e.stored_size.to_le_bytes());
            }
        }
        out
    }

    /// Strict decode. A body version other than [`PACK_MAP_BODY_VERSION`] is
    /// `UnsupportedVersion`; every other violation (no packs, a pack without
    /// entries, packs not in increasing ID order, entries not in increasing
    /// offset order or overlapping, a blob inside the pack header, more than
    /// [`MAX_PACK_MAP_ENTRIES`] entries, a length that differs from the
    /// counts) is `InvalidFormat`. For every accepted body `b`,
    /// `encode_body(decode_body(b)) == b`.
    pub fn decode_body(body: &[u8]) -> Result<Self> {
        let (&version, rest) = body.split_first().ok_or_else(|| invalid("empty body"))?;
        if version != PACK_MAP_BODY_VERSION {
            return Err(VykarError::UnsupportedVersion(u32::from(version)));
        }
        let (pack_count, mut rest) = take_u32(rest)?;
        let pack_count = pack_count as usize;
        if pack_count == 0 || pack_count > MAX_PACK_MAP_ENTRIES {
            return Err(invalid(format!(
                "pack count {pack_count} outside 1..={MAX_PACK_MAP_ENTRIES}"
            )));
        }
        let min_pack_len = PACK_HEADER_LEN + PACK_MAP_ENTRY_LEN;
        let mut packs = Vec::with_capacity(pack_count.min(rest.len() / min_pack_len));
        let mut total = 0usize;
        let mut prev: Option<[u8; 32]> = None;
        for _ in 0..pack_count {
            let (id, r) = rest
                .split_at_checked(32)
                .ok_or_else(|| invalid("short body"))?;
            let id: [u8; 32] = id.try_into().map_err(|_| invalid("short body"))?;
            if prev.is_some_and(|p| p >= id) {
                return Err(invalid("packs are not in increasing pack ID order"));
            }
            prev = Some(id);
            let (count, r) = take_u32(r)?;
            let count = count as usize;
            total = total.saturating_add(count);
            if count == 0 || total > MAX_PACK_MAP_ENTRIES {
                return Err(invalid(format!(
                    "entry count {count} of a pack is 0, or the map has more than \
                     {MAX_PACK_MAP_ENTRIES} entries"
                )));
            }
            let (raw, r) = r
                .split_at_checked(count * PACK_MAP_ENTRY_LEN)
                .ok_or_else(|| invalid("short body"))?;
            rest = r;
            let (raw_entries, _) = raw.as_chunks::<PACK_MAP_ENTRY_LEN>();
            let mut entries = Vec::with_capacity(count);
            for raw in raw_entries {
                let (chunk_id, raw) = raw
                    .split_at_checked(32)
                    .ok_or_else(|| invalid("short entry"))?;
                let chunk_id: [u8; 32] = chunk_id.try_into().map_err(|_| invalid("short entry"))?;
                let (offset, raw) = take_u32(raw)?;
                let (stored_size, _) = take_u32(raw)?;
                entries.push(PackMapEntry {
                    chunk_id: ChunkId::from_bytes(chunk_id),
                    offset,
                    stored_size,
                });
            }
            if let Some(why) = layout_violation(&entries) {
                return Err(invalid(why));
            }
            packs.push((PackId::from_bytes(id), entries));
        }
        if !rest.is_empty() {
            return Err(invalid(format!("{} bytes after the last pack", rest.len())));
        }
        Ok(Self { packs })
    }

    /// Encode, compress, encrypt; returns the name and the stored bytes.
    pub(crate) fn seal(&self, crypto: &dyn CryptoEngine) -> Result<(ObjectName, Vec<u8>)> {
        let body = self.encode_body();
        let stored = pack_object_streaming_with_context(
            ObjectType::PackMap,
            PACK_MAP_CONTEXT,
            body.len() / 2 + 64,
            crypto,
            |buf| {
                compress::compress_stream_zstd(buf, 3, |w| {
                    std::io::Write::write_all(w, &body)?;
                    Ok(())
                })
            },
        )?;
        Ok((ObjectName::of(&stored), stored))
    }

    /// Inverse of [`Self::seal`]. Checks `BLAKE3(stored) == name` first. A
    /// body version or a codec this binary does not know is
    /// `UnsupportedVersion`, an unknown envelope tag `UnknownObjectType`:
    /// both come from a newer vykar (see [`is_newer_object`]).
    pub(crate) fn open(
        name: &ObjectName,
        stored: &[u8],
        crypto: &dyn CryptoEngine,
    ) -> Result<Self> {
        if ObjectName::of(stored) != *name {
            return Err(invalid(format!(
                "pack map {} does not match its name",
                name.to_hex()
            )));
        }
        let compressed = unpack_object_expect_with_context(
            stored,
            ObjectType::PackMap,
            PACK_MAP_CONTEXT,
            crypto,
        )?;
        let body = compress::decompress_metadata(&compressed).map_err(|e| match e {
            VykarError::UnknownCompressionTag(tag) => {
                VykarError::UnsupportedVersion(u32::from(tag))
            }
            e => e,
        })?;
        Self::decode_body(&body)
    }
}

/// Whether an error from reading a pack map (or a trailer, for
/// `UnsupportedVersion`) means the object was written by a newer vykar:
/// refused, never treated as damage.
pub(crate) fn is_newer_object(e: &VykarError) -> bool {
    matches!(
        e,
        VykarError::UnsupportedVersion(_) | VykarError::UnknownObjectType(_)
    )
}

/// The error for the map at `key` that reads as written by a newer vykar
/// (`e`). A map is opened only when its bytes hash to its name, so rot
/// cannot read as newer; an object some other writer put there can, hence
/// the way out.
fn newer_pack_map(key: &str, e: &VykarError) -> VykarError {
    VykarError::Other(format!(
        "pack map {key} was written by a newer vykar ({e}); upgrade vykar. If no newer vykar \
         has written to this repository, the object does not belong there and may be removed \
         by hand; packs only it described are then taken from their trailers or become \
         uncovered"
    ))
}

/// `packmaps/<hex>`.
pub(crate) fn pack_map_key(name: &ObjectName) -> String {
    format!("{PACK_MAP_PREFIX}{}", name.to_hex())
}

/// The names of the maps in storage; keys that are not map names (foreign
/// files) are left out.
fn list_pack_maps(storage: &dyn StorageBackend) -> Result<Vec<ObjectName>> {
    Ok(storage
        .list(PACK_MAP_PREFIX)?
        .iter()
        .filter(|k| vykar_protocol::is_pack_map_key(k))
        .filter_map(|k| {
            let hex = k.strip_prefix(PACK_MAP_PREFIX)?;
            let bytes: [u8; 32] = hex::decode(hex).ok()?.try_into().ok()?;
            Some(ObjectName(bytes))
        })
        .collect())
}

/// What [`read_pack_maps`] found.
#[derive(Debug, Default)]
pub(crate) struct PackMapScan {
    /// Pack -> its entries in offset order (empty when `keep_entries` was
    /// false). A pack named by several maps gets the union of their entries.
    pub by_pack: HashMap<PackId, Vec<PackMapEntry>>,
    /// Maps read.
    pub maps: usize,
    /// Maps skipped: gone after the listing, not matching their name, or
    /// failing to decrypt or decode.
    pub damaged: usize,
}

/// Whether this storage can hold pack maps: always without a vykar-server;
/// with one, whether its `/health` lists `packmaps` in `key_dirs` (an older
/// server answers 400 to those keys). A failed `/health` is an error.
pub(crate) fn pack_maps_supported(storage: &dyn StorageBackend) -> Result<bool> {
    match storage.server_ops() {
        None => Ok(true),
        Some(server) => Ok(server
            .capabilities()?
            .accepts_key_dir(vykar_protocol::PACK_MAP_DIR)),
    }
}

/// Every pack map in storage, decoded. `keep_entries = false` keeps only the
/// pack IDs. A damaged map is skipped with a warning and counted; storage
/// errors are returned, and so is a map from a newer vykar
/// ([`is_newer_object`]), as an error naming its key. Up
/// to `width` GETs in flight; `cancel` is polled before each.
pub(crate) fn read_pack_maps(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    width: usize,
    keep_entries: bool,
    cancel: &CancelToken,
) -> Result<PackMapScan> {
    if !pack_maps_supported(storage)? {
        debug!("the server does not accept pack maps; none read");
        return Ok(PackMapScan::default());
    }
    let names = list_pack_maps(storage)?;
    let scan = Mutex::new(PackMapScan::default());
    try_for_each_bounded(width, &names, |name| {
        cancel.check()?;
        let key = pack_map_key(name);
        let opened = match storage.get(&key)? {
            None => Err(invalid(format!(
                "pack map {} is gone after the listing",
                name.to_hex()
            ))),
            Some(stored) => PackMap::open(name, &stored, crypto),
        };
        let map = match opened {
            Ok(map) => map,
            Err(e) if is_newer_object(&e) => {
                warn!("pack map {key} was written by a newer vykar ({e})");
                return Err(newer_pack_map(&key, &e));
            }
            Err(e) if is_object_damage(&e) => {
                warn!("pack map {key} cannot be read ({e}); left out");
                scan.lock().unwrap_or_else(PoisonError::into_inner).damaged += 1;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let mut scan = scan.lock().unwrap_or_else(PoisonError::into_inner);
        scan.maps += 1;
        for (pack_id, entries) in map.packs {
            let slot = scan.by_pack.entry(pack_id).or_default();
            if keep_entries {
                slot.extend(entries);
            }
        }
        Ok(())
    })?;
    cancel.check()?;
    let mut scan = scan.into_inner().unwrap_or_else(PoisonError::into_inner);
    for entries in scan.by_pack.values_mut() {
        entries.sort_by_key(|e| (e.offset, *e.chunk_id.as_bytes(), e.stored_size));
        entries.dedup();
    }
    Ok(scan)
}

/// Delete every pack map none of whose packs is `alive`. Best-effort: a
/// damaged map is kept, a map from a newer vykar stops the pass (an error
/// naming it), and a failed or refused delete is logged. Returns how many
/// maps were deleted. Does nothing when the server does not accept
/// `packmaps/` or none exist. Up to `width` GETs in flight.
pub(crate) fn delete_dead_pack_maps(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    width: usize,
    alive: &(dyn Fn(&PackId) -> bool + Sync),
    cancel: &CancelToken,
) -> Result<usize> {
    if !pack_maps_supported(storage)? {
        return Ok(0);
    }
    let names = list_pack_maps(storage)?;
    if names.is_empty() {
        return Ok(0);
    }
    let dead = Mutex::new(Vec::new());
    try_for_each_bounded(width, &names, |name| {
        cancel.check()?;
        let key = pack_map_key(name);
        let opened = match storage.get(&key)? {
            None => return Ok(()),
            Some(stored) => PackMap::open(name, &stored, crypto),
        };
        match opened {
            Ok(map) => {
                if !map.packs.iter().any(|(p, _)| alive(p)) {
                    dead.lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push(key);
                }
                Ok(())
            }
            Err(e) if is_newer_object(&e) => Err(newer_pack_map(&key, &e)),
            Err(e) if is_object_damage(&e) => {
                debug!("pack map {key} cannot be read ({e}); kept");
                Ok(())
            }
            Err(e) => Err(e),
        }
    })?;
    let mut deleted = 0;
    for key in dead.into_inner().unwrap_or_else(PoisonError::into_inner) {
        cancel.check()?;
        match storage.delete(&key) {
            Ok(()) => deleted += 1,
            Err(e) if e.is_write_refusal() => debug!("pack map {key} not deleted (refused): {e}"),
            Err(e) => warn!("failed to delete pack map {key}: {e}"),
        }
    }
    Ok(deleted)
}

/// What `Repo::write_pack_maps` did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackMapStats {
    pub maps_written: usize,
    pub packs_mapped: usize,
    pub entries_mapped: u64,
    /// Packs a readable map already covered; not rewritten.
    pub packs_already_mapped: usize,
    /// Packs whose index entries break the map rules (overlap, an offset
    /// over u32 or inside the pack header, too many entries); logged, not
    /// mapped.
    pub packs_skipped: usize,
    /// Of `packs_skipped`: packs with more entries than a map may hold.
    pub packs_skipped_too_many: usize,
    /// Of `packs_skipped`: packs whose entries the index describes
    /// inconsistently (overlap, an offset over u32 or inside the header),
    /// with the reason, in pack ID order.
    pub layout_violations: Vec<(PackId, String)>,
    pub bytes_written: u64,
}

/// The most entries a map may hold for one pack: [`MAX_PACK_MAP_ENTRIES`],
/// or the value a test set.
fn max_pack_map_entries() -> usize {
    #[cfg(test)]
    if let Some(n) = MAX_ENTRIES_FOR_TEST.with(std::cell::Cell::get) {
        return n;
    }
    MAX_PACK_MAP_ENTRIES
}

#[cfg(test)]
thread_local! {
    /// Lowers the per-pack entry limit of map writes on this thread (tests).
    pub(crate) static MAX_ENTRIES_FOR_TEST: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// Whether `mapped` (a pack's map entries) lists every entry of `entries`.
fn lists_all(mapped: &[PackMapEntry], entries: &[PackMapEntry]) -> bool {
    let key = |e: &PackMapEntry| (e.chunk_id, e.offset, e.stored_size);
    let mapped: HashSet<(ChunkId, u32, u32)> = mapped.iter().map(key).collect();
    entries.iter().all(|e| mapped.contains(&key(e)))
}

/// Cut packs (sorted by ID) into maps of at most `target` entries each;
/// a pack with more forms a map of its own.
fn group_into_maps(packs: Vec<(PackId, Vec<PackMapEntry>)>, target: usize) -> Vec<PackMap> {
    let mut maps = Vec::new();
    let mut current = Vec::new();
    let mut entries = 0usize;
    for (pack_id, pack_entries) in packs {
        if !current.is_empty() && entries + pack_entries.len() > target {
            maps.push(PackMap {
                packs: std::mem::take(&mut current),
            });
            entries = 0;
        }
        entries += pack_entries.len();
        current.push((pack_id, pack_entries));
    }
    if !current.is_empty() {
        maps.push(PackMap { packs: current });
    }
    maps
}

impl Repo {
    /// Write pack maps for the packs the loaded index names (all of them, or
    /// only those in `only`), skipping packs a readable map covers: for all
    /// packs, a pack any map names; for `only` (repair), a pack whose index
    /// entries the maps together list in full. A map written for a pack
    /// holds all of its index entries. Errors
    /// when the index is not loaded, when the server does not accept
    /// `packmaps/`, on any storage error and on a map from a newer vykar.
    /// Writes nothing else. The caller holds the maintenance lock. A
    /// cancelled run leaves the maps written so far, which a rerun counts as
    /// covering their packs.
    pub(crate) fn write_pack_maps(
        &mut self,
        only: Option<&HashSet<PackId>>,
        cancel: &CancelToken,
    ) -> Result<PackMapStats> {
        self.write_pack_maps_grouped(only, PACK_MAP_TARGET_ENTRIES, cancel)
    }

    /// [`Self::write_pack_maps`] with `target` entries per map.
    pub(crate) fn write_pack_maps_grouped(
        &mut self,
        only: Option<&HashSet<PackId>>,
        target: usize,
        cancel: &CancelToken,
    ) -> Result<PackMapStats> {
        if !self.chunk_index.is_loaded() && self.index_head().is_some_and(|h| !h.is_empty()) {
            return Err(VykarError::Other(
                "BUG: the chunk index was not loaded before pack maps were written".into(),
            ));
        }
        let storage = self.storage.as_ref();
        let crypto = self.crypto.as_ref();
        if !pack_maps_supported(storage)? {
            return Err(VykarError::Other(
                "the server does not accept pack maps (packmaps/); upgrade vykar-server".into(),
            ));
        }
        // Repair names packs whose maps may be partial, so it compares
        // entries; a full run takes any map naming a pack as covering it.
        let compare = only.is_some();
        let covered =
            read_pack_maps(storage, crypto, self.request_width(), compare, cancel)?.by_pack;

        let mut by_pack: HashMap<PackId, Vec<PackMapEntry>> = HashMap::new();
        let mut unmappable: HashMap<PackId, String> = HashMap::new();
        let mut already: HashSet<PackId> = HashSet::new();
        for (chunk_id, entry) in self.chunk_index.iter() {
            let pack_id = entry.pack_id;
            if only.is_some_and(|o| !o.contains(&pack_id)) {
                continue;
            }
            if !compare && covered.contains_key(&pack_id) {
                already.insert(pack_id);
                continue;
            }
            let Ok(offset) = u32::try_from(entry.pack_offset) else {
                unmappable
                    .entry(pack_id)
                    .or_insert_with(|| format!("offset {} does not fit u32", entry.pack_offset));
                continue;
            };
            by_pack.entry(pack_id).or_default().push(PackMapEntry {
                chunk_id,
                offset,
                stored_size: entry.stored_size,
            });
        }

        if compare {
            by_pack.retain(|pack_id, entries| {
                let listed = !unmappable.contains_key(pack_id)
                    && covered
                        .get(pack_id)
                        .is_some_and(|mapped| lists_all(mapped, entries));
                if listed {
                    already.insert(*pack_id);
                }
                !listed
            });
        }
        let mut stats = PackMapStats {
            packs_already_mapped: already.len(),
            ..PackMapStats::default()
        };
        let mut packs: Vec<(PackId, Vec<PackMapEntry>)> = Vec::with_capacity(by_pack.len());
        let max_entries = max_pack_map_entries();
        for (pack_id, mut entries) in by_pack {
            entries.sort_by_key(|e| e.offset);
            if let Some(why) = unmappable
                .remove(&pack_id)
                .or_else(|| layout_violation(&entries))
            {
                warn!(
                    "pack map: pack {pack_id} has index entries a map cannot hold ({why}); not mapped"
                );
                stats.packs_skipped += 1;
                stats.layout_violations.push((pack_id, why));
            } else if entries.len() > max_entries {
                warn!(
                    "pack map: pack {pack_id} has index entries a map cannot hold ({} entries, \
                     max {max_entries}); not mapped",
                    entries.len()
                );
                stats.packs_skipped += 1;
                stats.packs_skipped_too_many += 1;
            } else {
                packs.push((pack_id, entries));
            }
        }
        for (pack_id, why) in unmappable {
            warn!(
                "pack map: pack {pack_id} has index entries a map cannot hold ({why}); not mapped"
            );
            stats.packs_skipped += 1;
            stats.layout_violations.push((pack_id, why));
        }
        stats
            .layout_violations
            .sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        packs.sort_by_key(|(id, _)| *id.as_bytes());

        let maps = group_into_maps(packs, target.max(1));
        if !maps.is_empty() {
            storage.create_dir(PACK_MAP_PREFIX)?;
        }
        for map in maps {
            cancel.check()?;
            let (name, stored) = map.seal(crypto)?;
            storage.put(&pack_map_key(&name), &stored)?;
            stats.maps_written += 1;
            stats.packs_mapped += map.packs.len();
            stats.entries_mapped += map.packs.iter().map(|(_, e)| e.len() as u64).sum::<u64>();
            stats.bytes_written += stored.len() as u64;
        }
        info!(
            maps = stats.maps_written,
            packs = stats.packs_mapped,
            entries = stats.entries_mapped,
            already_mapped = stats.packs_already_mapped,
            skipped = stats.packs_skipped,
            "pack maps written"
        );
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{MemoryBackend, chunk_hasher_for, test_repo};
    use std::sync::Arc;
    use vykar_crypto::aes_gcm::Aes256GcmEngine;
    use vykar_crypto::chacha20_poly1305::ChaCha20Poly1305Engine;
    use vykar_crypto::testing::DeterministicEngine;
    use vykar_storage::{InnerBackend, delegate_storage_backend};

    fn plain() -> DeterministicEngine {
        DeterministicEngine::new(chunk_hasher_for([0xAA; 32]))
    }

    fn chunk(i: usize) -> ChunkId {
        let mut b = [0u8; 32];
        b[..8].copy_from_slice(&(i as u64).to_le_bytes());
        ChunkId::from_bytes(b)
    }

    fn pack(i: u8) -> PackId {
        PackId::from_bytes([i; 32])
    }

    /// `n` framed blobs of `size` bytes from the first offset on.
    fn entries(n: usize, first_chunk: usize, size: u32) -> Vec<PackMapEntry> {
        (0..n)
            .map(|i| PackMapEntry {
                chunk_id: chunk(first_chunk + i),
                offset: FIRST_BLOB_OFFSET as u32 + i as u32 * (size + 4),
                stored_size: size,
            })
            .collect()
    }

    fn map(packs: &[(u8, usize)]) -> PackMap {
        let mut next = 0;
        PackMap {
            packs: packs
                .iter()
                .map(|&(p, n)| {
                    let e = entries(n, next, 100);
                    next += n;
                    (pack(p), e)
                })
                .collect(),
        }
    }

    #[test]
    fn roundtrip_all_engines() {
        let aes = Aes256GcmEngine::new(&[0x11; 32], chunk_hasher_for([0x22; 32]));
        let chacha = ChaCha20Poly1305Engine::new(&[0x33; 32], chunk_hasher_for([0x44; 32]));
        let plain = plain();
        let engines: [&dyn CryptoEngine; 3] = [&plain, &aes, &chacha];
        let big = map(&[(1, PACK_MAP_TARGET_ENTRIES)]);
        for engine in engines {
            for m in [map(&[(1, 1)]), map(&[(1, 2), (2, 3), (9, 1)]), big.clone()] {
                let (name, stored) = m.seal(engine).unwrap();
                assert!(vykar_protocol::is_pack_map_key(&pack_map_key(&name)));
                assert_eq!(PackMap::open(&name, &stored, engine).unwrap(), m);
            }
        }
    }

    #[test]
    fn body_is_canonical() {
        for m in [map(&[(1, 1)]), map(&[(3, 5), (4, 1), (200, 7)])] {
            let body = m.encode_body();
            let decoded = PackMap::decode_body(&body).unwrap();
            assert_eq!(decoded, m);
            assert_eq!(decoded.encode_body(), body);
        }
    }

    fn rejected(body: &[u8], what: &str) {
        match PackMap::decode_body(body) {
            Err(VykarError::InvalidFormat(msg)) => assert!(msg.contains(what), "{msg}"),
            other => panic!("expected InvalidFormat({what}), got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects() {
        let good = map(&[(1, 2), (2, 2)]);
        let body = good.encode_body();

        let mut v2 = body.clone();
        v2[0] = 2;
        assert!(matches!(
            PackMap::decode_body(&v2),
            Err(VykarError::UnsupportedVersion(2))
        ));

        rejected(&[1, 0, 0, 0, 0], "pack count 0");
        let mut no_entries = vec![1, 1, 0, 0, 0];
        no_entries.extend_from_slice(&[7; 32]);
        no_entries.extend_from_slice(&0u32.to_le_bytes());
        rejected(&no_entries, "entry count 0");

        let swapped = PackMap {
            packs: vec![good.packs[1].clone(), good.packs[0].clone()],
        };
        rejected(&swapped.encode_body(), "increasing pack ID");
        let dup = PackMap {
            packs: vec![good.packs[0].clone(), good.packs[0].clone()],
        };
        rejected(&dup.encode_body(), "increasing pack ID");

        let mut unsorted = good.clone();
        unsorted.packs[0].1.swap(0, 1);
        rejected(&unsorted.encode_body(), "overlaps");
        let mut overlap = good.clone();
        overlap.packs[0].1[1].offset = overlap.packs[0].1[0].offset + 100 + 3;
        rejected(&overlap.encode_body(), "overlaps");
        let mut in_header = good.clone();
        in_header.packs[0].1[0].offset = 12;
        rejected(&in_header.encode_body(), "overlaps");

        rejected(&body[..body.len() - 1], "short");
        let mut longer = body.clone();
        longer.push(0);
        rejected(&longer, "after the last pack");

        let too_many = map(&[(1, MAX_PACK_MAP_ENTRIES), (2, 1)]);
        rejected(&too_many.encode_body(), "more than");
    }

    #[test]
    fn open_rejects() {
        let crypto = Aes256GcmEngine::new(&[0x11; 32], chunk_hasher_for([0x22; 32]));
        let m = map(&[(1, 3)]);
        let (name, stored) = m.seal(&crypto).unwrap();

        let mut flipped = stored.clone();
        *flipped.last_mut().unwrap() ^= 1;
        let err = PackMap::open(&name, &flipped, &crypto).unwrap_err();
        assert!(err.to_string().contains("does not match its name"), "{err}");

        let (other_name, other) = map(&[(2, 1)]).seal(&crypto).unwrap();
        assert!(PackMap::open(&name, &other, &crypto).is_err());
        assert!(PackMap::open(&other_name, &other, &crypto).is_ok());

        let body = m.encode_body();
        let seal_as = |tag: ObjectType, context: &[u8]| {
            let stored = pack_object_streaming_with_context(tag, context, 64, &crypto, |buf| {
                compress::compress_stream_zstd(buf, 3, |w| {
                    std::io::Write::write_all(w, &body)?;
                    Ok(())
                })
            })
            .unwrap();
            (ObjectName::of(&stored), stored)
        };
        let (n, s) = seal_as(ObjectType::PackMap, b"other");
        assert!(matches!(
            PackMap::open(&n, &s, &crypto),
            Err(VykarError::DecryptionFailed)
        ));
        let (n, s) = seal_as(ObjectType::PackTrailer, PACK_MAP_CONTEXT);
        let err = PackMap::open(&n, &s, &crypto).unwrap_err();
        assert!(is_object_damage(&err) && !is_newer_object(&err), "{err}");

        let mut newer = vec![15u8];
        newer.extend_from_slice(&s[1..]);
        let err = PackMap::open(&ObjectName::of(&newer), &newer, &crypto).unwrap_err();
        assert!(is_newer_object(&err), "{err}");
    }

    /// A repository whose index holds `packs` packs of `per_pack`
    /// entries each.
    fn repo_with_packs(packs: u8, per_pack: usize) -> Repo {
        let mut repo = test_repo();
        for p in 0..packs {
            for e in entries(per_pack, usize::from(p) * per_pack, 100) {
                repo.unchecked_writer().chunk_index_mut().add(
                    e.chunk_id,
                    e.stored_size,
                    pack(p + 1),
                    u64::from(e.offset),
                );
            }
        }
        repo
    }

    fn index_by_pack(repo: &Repo) -> HashMap<PackId, Vec<PackMapEntry>> {
        let mut out: HashMap<PackId, Vec<PackMapEntry>> = HashMap::new();
        for (chunk_id, e) in repo.chunk_index().iter() {
            out.entry(e.pack_id).or_default().push(PackMapEntry {
                chunk_id,
                offset: u32::try_from(e.pack_offset).unwrap(),
                stored_size: e.stored_size,
            });
        }
        for v in out.values_mut() {
            v.sort_by_key(|e| e.offset);
        }
        out
    }

    #[test]
    fn write_pack_maps_groups_and_is_idempotent() {
        let mut repo = repo_with_packs(5, 3);
        let cancel = CancelToken::new();
        let stats = repo.write_pack_maps_grouped(None, 6, &cancel).unwrap();
        assert_eq!(stats.maps_written, 3);
        assert_eq!(stats.packs_mapped, 5);
        assert_eq!(stats.entries_mapped, 15);
        assert_eq!(stats.packs_already_mapped, 0);

        let scan = read_pack_maps(
            repo.storage.as_ref(),
            repo.crypto.as_ref(),
            2,
            true,
            &cancel,
        )
        .unwrap();
        assert_eq!((scan.maps, scan.damaged), (3, 0));
        assert_eq!(scan.by_pack, index_by_pack(&repo));

        let again = repo.write_pack_maps_grouped(None, 6, &cancel).unwrap();
        assert_eq!(again.maps_written, 0);
        assert_eq!(again.packs_already_mapped, 5);
    }

    #[test]
    fn write_pack_maps_skips_packs_a_map_cannot_hold() {
        let mut repo = repo_with_packs(1, 2);
        // Pack 7: an offset past u32. Pack 8: two overlapping blobs.
        repo.unchecked_writer().chunk_index_mut().add(
            chunk(100),
            10,
            pack(7),
            u64::from(u32::MAX) + 1,
        );
        repo.unchecked_writer()
            .chunk_index_mut()
            .add(chunk(101), 10, pack(8), 13);
        repo.unchecked_writer()
            .chunk_index_mut()
            .add(chunk(102), 10, pack(8), 20);
        let only: HashSet<PackId> = [pack(1), pack(7), pack(8)].into_iter().collect();
        let stats = repo
            .write_pack_maps(Some(&only), &CancelToken::new())
            .unwrap();
        assert_eq!(stats.packs_skipped, 2);
        assert_eq!(stats.packs_mapped, 1);
        let scan = read_pack_maps(
            repo.storage.as_ref(),
            repo.crypto.as_ref(),
            2,
            false,
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(scan.by_pack.keys().copied().collect::<Vec<_>>(), [pack(1)]);
    }

    /// A vykar-server whose `/health` does not list `packmaps`.
    struct OldServer {
        inner: MemoryBackend,
    }

    impl InnerBackend for OldServer {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for OldServer;
        except [server_ops];

        fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
            Some(self)
        }
    }

    impl vykar_storage::ServerOps for OldServer {
        fn offers(&self, _op: vykar_storage::ServerOp) -> bool {
            false
        }

        fn capabilities(&self) -> Result<vykar_protocol::ServerCapabilities> {
            let mut caps = crate::testutil::fake_server_caps();
            caps.key_dirs.retain(|d| d != vykar_protocol::PACK_MAP_DIR);
            Ok(caps)
        }

        fn init(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn write_pack_maps_refuses_an_old_server() {
        let mut repo = repo_with_packs(1, 1);
        let old = Arc::new(OldServer {
            inner: MemoryBackend::new(),
        });
        repo.storage = Arc::clone(&old) as Arc<dyn StorageBackend>;
        let err = repo
            .write_pack_maps(None, &CancelToken::new())
            .unwrap_err()
            .to_string();
        assert!(err.contains("upgrade vykar-server"), "{err}");
        assert!(old.inner.list(PACK_MAP_PREFIX).unwrap().is_empty());
        // Reading lists nothing on such a server.
        let scan = read_pack_maps(
            old.as_ref(),
            repo.crypto.as_ref(),
            1,
            true,
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(scan.maps, 0);
    }

    #[test]
    fn read_pack_maps_skips_damage_and_refuses_newer() {
        let storage = MemoryBackend::new();
        let crypto = plain();
        let cancel = CancelToken::new();
        let (name, stored) = map(&[(1, 2)]).seal(&crypto).unwrap();
        storage.put(&pack_map_key(&name), &stored).unwrap();
        // Garbage of the right type under a valid name, and a foreign file.
        // (envelope tag, zstd codec tag, then no zstd frame).
        let mut garbage = vec![ObjectType::PackMap as u8, 0x02];
        garbage.extend_from_slice(b"not a pack map");
        storage
            .put(&pack_map_key(&ObjectName::of(&garbage)), &garbage)
            .unwrap();
        storage.put("packmaps/README", b"hello").unwrap();
        let scan = read_pack_maps(&storage, &crypto, 2, true, &cancel).unwrap();
        assert_eq!((scan.maps, scan.damaged), (1, 1));
        assert_eq!(scan.by_pack.len(), 1);

        let mut body = map(&[(2, 1)]).encode_body();
        body[0] = 2;
        let newer = pack_object_streaming_with_context(
            ObjectType::PackMap,
            PACK_MAP_CONTEXT,
            64,
            &crypto,
            |buf| {
                compress::compress_stream_zstd(buf, 3, |w| {
                    std::io::Write::write_all(w, &body)?;
                    Ok(())
                })
            },
        )
        .unwrap();
        storage
            .put(&pack_map_key(&ObjectName::of(&newer)), &newer)
            .unwrap();
        let err = read_pack_maps(&storage, &crypto, 2, true, &cancel)
            .unwrap_err()
            .to_string();
        assert!(err.contains("written by a newer vykar"), "{err}");
        assert!(
            err.contains(&pack_map_key(&ObjectName::of(&newer))),
            "{err}"
        );
    }
}
