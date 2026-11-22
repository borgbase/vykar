//! Rebuilding the chunk index from what the packs say about themselves, for
//! `check --repair` when the index cannot be read.
//!
//! Every listed pack is described by, in this order of precedence: a pack
//! map naming it (no request to the pack), its own trailer (one or two tail
//! reads), or the verified chunk tables of this machine (only for packs with
//! neither). A pack none describes is reported as uncovered. A chunk stored
//! in several packs resolves to the lowest (pack ID, offset). Storage errors
//! and objects written by a newer vykar abort the rebuild; only damage makes
//! a pack uncovered. A trailer of an unknown version is newer only when the
//! pack still matches its ID: without encryption one flipped byte also
//! produces it.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use tempfile::TempPath;
use tracing::{info, warn};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::hash::HashAlgorithm;
use vykar_types::pack_id::PackId;

use super::bounded::try_for_each_bounded;
use super::control::CancelToken;
use super::pack_listing::list_packs;
use super::pack_map::read_pack_maps;
use super::pack_trailer::{TrailerRead, read_pack_trailer};
use crate::index::base_v2::BaseWriter;
use crate::index::table::{ChunkTable, TableFileWriter};
use crate::repo::pack_trailer::trailer_frame_len;

/// Lowest valid blob offset: the pack header plus one length prefix.
const FIRST_BLOB_OFFSET: u64 = vykar_protocol::PACK_HEADER_SIZE as u64 + 4;

/// The reason of a pack with neither a usable map nor a trailer.
const NO_DESCRIPTION: &str = "no trailer and no pack map";

/// Where the parallel pass took the entries of a listed pack from (local
/// tables are consulted after it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PackSource {
    Map,
    Trailer,
}

/// A listed pack the rebuild found no entries for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UncoveredPack {
    pub pack_id: PackId,
    /// "no trailer and no pack map", or why its trailer or map entries were
    /// rejected.
    pub reason: String,
}

/// Counts of one rebuild. `Copy + Eq`: part of `IndexRecovery`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RebuildCounts {
    /// Listed packs that still existed when they were read.
    pub packs: usize,
    pub from_maps: usize,
    pub from_trailers: usize,
    pub from_local_tables: usize,
    pub uncovered: usize,
    /// Map objects skipped as damaged.
    pub damaged_maps: usize,
    /// Entries of the rebuilt index.
    pub entries: u64,
}

/// The result of [`rebuild_index_from_packs`].
pub(crate) struct Rebuilt {
    /// The rebuilt state as a base body in a table file (a temp file). A
    /// pack described by its trailer records that trailer's frame length,
    /// one described by a map 0, one from a local table that table's value.
    pub body: TempPath,
    pub counts: RebuildCounts,
    /// In pack ID order.
    pub uncovered: Vec<UncoveredPack>,
    /// Packs whose entries came only from a local table, in pack ID order.
    pub from_local_tables: Vec<PackId>,
}

/// One entry found for a pack: (chunk ID, pack slot, offset, stored size).
/// Slots are positions in the pack list sorted by ID, so comparing slots
/// compares pack IDs.
type Found = (ChunkId, u32, u32, u32);

/// What the parallel pass learned about one pack.
enum Outcome {
    Described(PackSource),
    /// Neither a usable map nor a trailer; the reason, and the pack length
    /// for the local-table pass.
    Undescribed(String, u64),
    /// The pack was gone when it was read.
    Vanished,
}

/// Whether a blob at `offset` of `size` bytes lies inside a pack of `len`
/// bytes, after its header.
fn inside(offset: u64, size: u32, len: u64) -> bool {
    offset >= FIRST_BLOB_OFFSET && offset.saturating_add(u64::from(size)) <= len
}

/// Rebuild the chunk index from the packs. `local_tables` is called at most
/// once, only when some pack has neither a usable map nor a usable trailer,
/// and returns verified chunk tables of this machine. `hash` is the
/// repository's pack ID hash.
pub(crate) fn rebuild_index_from_packs(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    hash: HashAlgorithm,
    local_tables: impl FnOnce() -> Vec<Arc<ChunkTable>>,
    anchor: &Path,
    width: usize,
    cancel: &CancelToken,
) -> Result<Rebuilt> {
    let maps = read_pack_maps(storage, crypto, width, true, cancel)?;
    let mut packs = list_packs(storage, width)?;
    cancel.check()?;
    packs.sort_by_key(|p| *p.pack_id.as_bytes());
    packs.dedup_by_key(|p| p.pack_id);
    let slots: Vec<(u32, &super::pack_listing::ListedPack)> = packs
        .iter()
        .enumerate()
        .map(|(i, p)| {
            u32::try_from(i)
                .map(|slot| (slot, p))
                .map_err(|_| VykarError::Other("more than u32::MAX packs".into()))
        })
        .collect::<Result<_>>()?;

    let found: Mutex<Vec<Found>> = Mutex::new(Vec::new());
    // trailer_len by pack slot, for packs described by their trailer.
    let trailers: Mutex<HashMap<u32, u32>> = Mutex::new(HashMap::new());
    let encrypting = crypto.is_encrypting();
    let outcomes: Mutex<Vec<(u32, Outcome)>> = Mutex::new(Vec::with_capacity(slots.len()));
    let record = |slot: u32, outcome: Outcome| {
        outcomes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((slot, outcome));
    };
    try_for_each_bounded(width, &slots, |&(slot, pack)| -> Result<()> {
        cancel.check()?;
        let len = match pack.size {
            Some(len) => len,
            None => match storage.size(&pack.key)? {
                Some(len) => len,
                None => {
                    record(slot, Outcome::Vanished);
                    return Ok(());
                }
            },
        };
        let mut reason = None;
        if let Some(entries) = maps.by_pack.get(&pack.pack_id) {
            match entries
                .iter()
                .find(|e| !inside(u64::from(e.offset), e.stored_size, len))
            {
                None => {
                    let mut found = found.lock().unwrap_or_else(PoisonError::into_inner);
                    found.extend(
                        entries
                            .iter()
                            .map(|e| (e.chunk_id, slot, e.offset, e.stored_size)),
                    );
                    drop(found);
                    record(slot, Outcome::Described(PackSource::Map));
                    return Ok(());
                }
                Some(e) => {
                    warn!(
                        "pack map entry at offset {} ({} bytes) lies outside pack {} of {len} \
                         bytes; the map's entries for it are not used",
                        e.offset, e.stored_size, pack.pack_id
                    );
                    reason = Some(format!(
                        "pack map: blob at offset {} ({} bytes) lies outside the pack",
                        e.offset, e.stored_size
                    ));
                }
            }
        }
        match read_pack_trailer(storage, &pack.key, len, crypto)? {
            TrailerRead::Found(t) => {
                trailers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(slot, trailer_frame_len(t.entries.len(), encrypting));
                let mut found = found.lock().unwrap_or_else(PoisonError::into_inner);
                found.extend(
                    t.entries
                        .iter()
                        .map(|e| (e.chunk_id, slot, e.offset, e.stored_size)),
                );
                drop(found);
                record(slot, Outcome::Described(PackSource::Trailer));
            }
            TrailerRead::PackMissing => record(slot, Outcome::Vanished),
            TrailerRead::Damaged(e @ VykarError::UnsupportedVersion(_)) => {
                // Under an encrypting engine only a newer writer produces
                // this, so the pack is not read. Without encryption one
                // flipped byte can; the pack ID names the pack's bytes,
                // trailer included, and tells the two apart.
                let bytes = if crypto.is_encrypting() {
                    None
                } else {
                    let Some(bytes) = storage.get(&pack.key)? else {
                        record(slot, Outcome::Vanished);
                        return Ok(());
                    };
                    Some(bytes)
                };
                if bytes
                    .as_ref()
                    .is_none_or(|b| PackId::compute(b, hash) == pack.pack_id)
                {
                    warn!(
                        "the trailer of pack {} was written by a newer vykar ({e})",
                        pack.pack_id
                    );
                    return Err(VykarError::Other(format!(
                        "the trailer of pack {} was written by a newer vykar ({e}); upgrade \
                         vykar to repair this repository",
                        pack.key
                    )));
                }
                drop(bytes);
                warn!(
                    "the trailer of pack {} reads as a newer version ({e}), but the pack does \
                     not match its ID: damaged",
                    pack.pack_id
                );
                let why = format!("trailer: {e}, and the pack does not match its ID");
                record(
                    slot,
                    Outcome::Undescribed(
                        reason.map_or(why.clone(), |r| format!("{r}; {why}")),
                        len,
                    ),
                );
            }
            TrailerRead::Damaged(e) => {
                warn!("the trailer of pack {} cannot be read: {e}", pack.pack_id);
                let why = format!("trailer: {e}");
                record(
                    slot,
                    Outcome::Undescribed(
                        reason.map_or(why.clone(), |r| format!("{r}; {why}")),
                        len,
                    ),
                );
            }
            TrailerRead::Absent => record(
                slot,
                Outcome::Undescribed(
                    reason.map_or_else(
                        || NO_DESCRIPTION.to_string(),
                        |r| format!("{r}; no trailer"),
                    ),
                    len,
                ),
            ),
        }
        Ok(())
    })?;
    cancel.check()?;

    let mut found = found.into_inner().unwrap_or_else(PoisonError::into_inner);
    let mut trailers = trailers
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner);
    let mut outcomes = outcomes
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner);
    outcomes.sort_by_key(|(slot, _)| *slot);

    let mut counts = RebuildCounts {
        damaged_maps: maps.damaged,
        ..RebuildCounts::default()
    };
    drop(maps);
    // Undescribed packs: slot -> (length, reason).
    let mut undescribed: HashMap<u32, (u64, String)> = HashMap::new();
    for (slot, outcome) in outcomes {
        match outcome {
            Outcome::Described(PackSource::Map) => counts.from_maps += 1,
            Outcome::Described(PackSource::Trailer) => counts.from_trailers += 1,
            Outcome::Undescribed(reason, len) => {
                undescribed.insert(slot, (len, reason));
            }
            Outcome::Vanished => continue,
        }
        counts.packs += 1;
    }

    // Local tables back the packs nothing else describes. Sound because a
    // pack ID names the pack's bytes: an entry that was ever true for an
    // existing pack is still true.
    let mut from_local_tables = Vec::new();
    if !undescribed.is_empty() {
        let by_id: HashMap<PackId, u32> = undescribed
            .keys()
            .filter_map(|&slot| Some((slots.get(slot as usize)?.1.pack_id, slot)))
            .collect();
        let mut covered: Vec<u32> = Vec::new();
        for table in local_tables() {
            cancel.check()?;
            for position in 0..table.len() as u32 {
                let e = table.entry_at(position);
                let Some(&slot) = by_id.get(&e.pack_id) else {
                    continue;
                };
                if let Some(t) = table.pack_trailer_len(&e.pack_id) {
                    trailers.insert(slot, t);
                }
                let Some((len, _)) = undescribed.get(&slot) else {
                    continue;
                };
                let Ok(offset) = u32::try_from(e.pack_offset) else {
                    continue;
                };
                if !inside(e.pack_offset, e.stored_size, *len) {
                    continue;
                }
                found.push((e.chunk_id, slot, offset, e.stored_size));
                covered.push(slot);
            }
        }
        covered.sort_unstable();
        covered.dedup();
        for slot in covered {
            if undescribed.remove(&slot).is_some()
                && let Some((_, pack)) = slots.get(slot as usize)
            {
                from_local_tables.push(pack.pack_id);
                counts.from_local_tables += 1;
            }
        }
    }
    let mut uncovered: Vec<(u32, String)> = undescribed
        .into_iter()
        .map(|(slot, (_, reason))| (slot, reason))
        .collect();
    uncovered.sort_by_key(|(slot, _)| *slot);
    let uncovered: Vec<UncoveredPack> = uncovered
        .into_iter()
        .filter_map(|(slot, reason)| {
            Some(UncoveredPack {
                pack_id: slots.get(slot as usize)?.1.pack_id,
                reason,
            })
        })
        .collect();
    counts.uncovered = uncovered.len();

    // The lowest (pack ID, offset) of each chunk.
    found.sort_unstable_by_key(|&(id, slot, offset, _)| (*id.as_bytes(), slot, offset));
    found.dedup_by_key(|e| e.0);
    // The packs the entries use, in slot (= pack ID) order.
    let mut used: Vec<u32> = found.iter().map(|f| f.1).collect();
    used.sort_unstable();
    used.dedup();
    let pack_table: Vec<(PackId, u32)> = used
        .iter()
        .map(|slot| {
            let pack_id = slots
                .get(*slot as usize)
                .map(|(_, p)| p.pack_id)
                .ok_or_else(|| {
                    VykarError::Other("BUG: rebuilt entry names no listed pack".into())
                })?;
            Ok((pack_id, trailers.get(slot).copied().unwrap_or(0)))
        })
        .collect::<Result<_>>()?;
    let count = u32::try_from(found.len())
        .map_err(|_| VykarError::Other("rebuilt index has more than u32::MAX entries".into()))?;
    let mut writer = BaseWriter::new(TableFileWriter::new_beside(anchor)?, &pack_table, count)?;
    for &(chunk_id, slot, offset, stored_size) in &found {
        let base_slot = used
            .binary_search(&slot)
            .map_err(|_| VykarError::Other("BUG: rebuilt entry names no used pack".into()))?
            as u32;
        writer.push(&chunk_id, base_slot, u64::from(offset), stored_size)?;
    }
    drop(found);
    let tmp = writer.finish()?.finish()?;
    counts.entries = u64::from(count);
    info!(
        packs = counts.packs,
        from_maps = counts.from_maps,
        from_trailers = counts.from_trailers,
        from_local_tables = counts.from_local_tables,
        uncovered = counts.uncovered,
        damaged_maps = counts.damaged_maps,
        entries = counts.entries,
        "rebuilt the chunk index from the packs"
    );
    Ok(Rebuilt {
        body: tmp,
        counts,
        uncovered,
        from_local_tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::pack::{PackType, PackWriter};
    use crate::repo::pack_map::{PackMap, PackMapEntry, pack_map_key};
    use crate::repo::pack_trailer::{parse_footer, trailer_frame_start};
    use crate::testutil::{MemoryBackend, RecordingBackend, chunk_hasher_for};
    use vykar_crypto::PlaintextEngine;
    use vykar_storage::{InnerBackend, ListedObject, delegate_storage_backend};
    use vykar_types::hash::HashAlgorithm;

    fn plain() -> PlaintextEngine {
        PlaintextEngine::new(chunk_hasher_for([0xAA; 32]))
    }

    fn chunk(i: usize) -> ChunkId {
        let mut b = [0u8; 32];
        b[..8].copy_from_slice(&(i as u64).to_le_bytes());
        ChunkId::from_bytes(b)
    }

    /// (chunk ID, pack ID, offset, stored size)
    type Loc = (ChunkId, PackId, u64, u32);

    /// Seal a pack of `chunks` (blob `i` is 20 + i bytes), store it, and
    /// return its ID and the locations of its blobs.
    fn put_pack(storage: &dyn StorageBackend, chunks: &[usize]) -> (PackId, Vec<Loc>) {
        let mut w = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
        for &c in chunks {
            w.add_blob(chunk(c), vec![c as u8; 20 + c % 7]).unwrap();
        }
        let sealed = w.seal(&plain()).unwrap();
        let id = sealed.pack_id;
        storage
            .put(&id.storage_key(), sealed.data.as_bytes().unwrap())
            .unwrap();
        let locs = sealed.entries.iter().map(|e| (e.0, id, e.2, e.1)).collect();
        (id, locs)
    }

    /// Rewrite a stored pack with `f` applied to its bytes and the start of
    /// its trailer frame.
    fn rewrite(storage: &dyn StorageBackend, id: PackId, f: impl FnOnce(&mut Vec<u8>, usize)) {
        let key = id.storage_key();
        let mut bytes = storage.get(&key).unwrap().unwrap();
        let len = parse_footer(&bytes).unwrap();
        let start = trailer_frame_start(bytes.len() as u64, len).unwrap() as usize;
        f(&mut bytes, start);
        storage.put(&key, &bytes).unwrap();
    }

    fn cut_trailer(storage: &dyn StorageBackend, id: PackId) {
        rewrite(storage, id, |b, start| b.truncate(start));
    }

    /// A rebuild's report, without its cache.
    struct Summary {
        counts: RebuildCounts,
        uncovered: Vec<UncoveredPack>,
        from_local_tables: Vec<PackId>,
    }

    fn rebuild_with(
        storage: &dyn StorageBackend,
        tables: Vec<Arc<ChunkTable>>,
    ) -> Result<(Summary, Vec<Loc>)> {
        let dir = tempfile::tempdir().unwrap();
        rebuild_index_from_packs(
            storage,
            &plain(),
            HashAlgorithm::Blake2b,
            || tables,
            &dir.path().join("anchor"),
            2,
            &CancelToken::new(),
        )
        .map(summarize)
    }

    /// The report of `rebuilt` and its entries.
    fn summarize(rebuilt: Rebuilt) -> (Summary, Vec<Loc>) {
        let Rebuilt {
            body,
            counts,
            uncovered,
            from_local_tables,
        } = rebuilt;
        let table = ChunkTable::install(body, None, None).unwrap();
        let mut locs: Vec<Loc> = (0..table.len() as u32)
            .map(|p| {
                let e = table.entry_at(p);
                (e.chunk_id, e.pack_id, e.pack_offset, e.stored_size)
            })
            .collect();
        locs.sort_by_key(|l| *l.0.as_bytes());
        let summary = Summary {
            counts,
            uncovered,
            from_local_tables,
        };
        (summary, locs)
    }

    fn rebuild(storage: &dyn StorageBackend) -> (Summary, Vec<Loc>) {
        rebuild_with(storage, Vec::new()).unwrap()
    }

    fn sorted(mut locs: Vec<Loc>) -> Vec<Loc> {
        locs.sort_by_key(|l| *l.0.as_bytes());
        locs
    }

    #[test]
    fn rebuild_from_trailers_equals_the_index() {
        let storage = MemoryBackend::new();
        let (_, mut expected) = put_pack(&storage, &[1, 2, 3]);
        expected.extend(put_pack(&storage, &[4, 5]).1);
        expected.extend(put_pack(&storage, &[6]).1);
        let (rebuilt, locs) = rebuild(&storage);
        assert_eq!(locs, sorted(expected));
        let c = rebuilt.counts;
        assert_eq!(
            (c.packs, c.from_trailers, c.from_maps, c.uncovered),
            (3, 3, 0, 0)
        );
        assert_eq!(c.entries, 6);
        assert!(rebuilt.uncovered.is_empty() && rebuilt.from_local_tables.is_empty());
    }

    fn map_of(locs: &[Loc]) -> PackMap {
        let mut by_pack: std::collections::BTreeMap<[u8; 32], Vec<PackMapEntry>> =
            std::collections::BTreeMap::new();
        for &(chunk_id, pack_id, offset, stored_size) in locs {
            by_pack
                .entry(*pack_id.as_bytes())
                .or_default()
                .push(PackMapEntry {
                    chunk_id,
                    offset: offset as u32,
                    stored_size,
                });
        }
        PackMap {
            packs: by_pack
                .into_iter()
                .map(|(id, mut e)| {
                    e.sort_by_key(|e| e.offset);
                    (PackId::from_bytes(id), e)
                })
                .collect(),
        }
    }

    fn put_map(storage: &dyn StorageBackend, map: &PackMap) {
        let (name, stored) = map.seal(&plain()).unwrap();
        storage.put(&pack_map_key(&name), &stored).unwrap();
    }

    #[test]
    fn rebuild_prefers_maps_and_reads_no_pack() {
        let (storage, log) = RecordingBackend::new();
        let (_, mut expected) = put_pack(&storage, &[1, 2, 3]);
        expected.extend(put_pack(&storage, &[4, 5]).1);
        put_map(&storage, &map_of(&expected));
        log.reset_get_range_count();
        let (rebuilt, locs) = rebuild(&storage);
        assert_eq!(log.get_range_count(), 0);
        assert_eq!(locs, sorted(expected));
        assert_eq!(
            (rebuilt.counts.from_maps, rebuilt.counts.from_trailers),
            (2, 0)
        );
    }

    #[test]
    fn rebuild_falls_back_from_a_bad_map_entry_to_the_trailer() {
        let storage = MemoryBackend::new();
        let (_, expected) = put_pack(&storage, &[1, 2]);
        let mut bad = expected.clone();
        bad[1].2 = 1 << 20;
        put_map(&storage, &map_of(&bad));
        let (rebuilt, locs) = rebuild(&storage);
        assert_eq!(locs, sorted(expected));
        assert_eq!(
            (rebuilt.counts.from_maps, rebuilt.counts.from_trailers),
            (0, 1)
        );
    }

    #[test]
    fn rebuild_resolves_duplicates_to_the_lowest_pack_and_offset() {
        let storage = MemoryBackend::new();
        let (a, la) = put_pack(&storage, &[1, 2]);
        let (b, lb) = put_pack(&storage, &[3, 1]);
        let (_, locs) = rebuild(&storage);
        let (low, high) = if a.as_bytes() < b.as_bytes() {
            (la, lb)
        } else {
            (lb, la)
        };
        let one = locs.iter().find(|l| l.0 == chunk(1)).unwrap();
        assert_eq!(*one, *low.iter().find(|l| l.0 == chunk(1)).unwrap());
        assert_ne!(one.1, high[0].1);
        assert_eq!(locs.len(), 3);
    }

    /// A verified table of `locs` (sorted by chunk ID), as a scratch file.
    fn table_of(locs: &[Loc], _dir: &Path) -> Arc<ChunkTable> {
        let mut index = crate::index::ChunkIndex::new();
        for &(chunk_id, pack_id, pack_offset, stored_size) in locs {
            index.add(chunk_id, stored_size, pack_id, pack_offset);
        }
        crate::testutil::base_table_of(&index)
    }

    /// REB-1: a rebuilt index records the trailer frame length of packs its
    /// trailer described, 0 for packs a map described.
    #[test]
    fn rebuild_records_trailer_lengths() {
        let storage = MemoryBackend::new();
        let (by_trailer, _) = put_pack(&storage, &[1, 2, 3]);
        let (by_map, mapped) = put_pack(&storage, &[4, 5]);
        put_map(&storage, &map_of(&mapped));
        let dir = tempfile::tempdir().unwrap();
        let rebuilt = rebuild_index_from_packs(
            &storage,
            &plain(),
            HashAlgorithm::Blake2b,
            Vec::new,
            &dir.path().join("anchor"),
            2,
            &CancelToken::new(),
        )
        .unwrap();
        let table = ChunkTable::install(rebuilt.body, None, None).unwrap();
        assert_eq!(
            table.pack_trailer_len(&by_trailer),
            Some(crate::repo::pack_trailer::trailer_frame_len(3, false))
        );
        assert_eq!(table.pack_trailer_len(&by_map), Some(0));
        let bytes = storage.get(&by_trailer.storage_key()).unwrap().unwrap();
        let blob_len = parse_footer(&bytes).unwrap();
        assert_eq!(table.pack_trailer_len(&by_trailer), Some(4 + blob_len));
    }

    #[test]
    fn rebuild_uses_local_tables_for_packs_without_trailer_or_map() {
        let storage = MemoryBackend::new();
        let (old, old_locs) = put_pack(&storage, &[1, 2]);
        let (_, new_locs) = put_pack(&storage, &[3]);
        cut_trailer(&storage, old);

        let (rebuilt, locs) = rebuild(&storage);
        assert_eq!(locs, new_locs);
        assert_eq!(
            rebuilt.uncovered,
            [UncoveredPack {
                pack_id: old,
                reason: NO_DESCRIPTION.into(),
            }]
        );
        assert_eq!(rebuilt.counts.uncovered, 1);

        let dir = tempfile::tempdir().unwrap();
        let mut table_locs = old_locs.clone();
        // An entry for a pack that is not listed is ignored.
        table_locs.push((chunk(9), PackId::from_bytes([0xEE; 32]), 13, 5));
        let table = table_of(&table_locs, dir.path());
        let (rebuilt, locs) = rebuild_with(&storage, vec![table]).unwrap();
        let mut expected = old_locs;
        expected.extend(new_locs);
        assert_eq!(locs, sorted(expected));
        assert_eq!(rebuilt.counts.from_local_tables, 1);
        assert_eq!(rebuilt.from_local_tables, [old]);
        assert!(rebuilt.uncovered.is_empty());
    }

    #[test]
    fn rebuild_reports_a_damaged_trailer_as_uncovered() {
        let storage = MemoryBackend::new();
        let (id, _) = put_pack(&storage, &[1, 2]);
        // The pack type byte: [u32 L][object tag][codec tag][version][type].
        rewrite(&storage, id, |b, start| b[start + 7] = 0xFF);
        let (rebuilt, locs) = rebuild(&storage);
        assert!(locs.is_empty());
        assert_eq!(rebuilt.uncovered.len(), 1);
        assert!(
            rebuilt.uncovered[0].reason.starts_with("trailer: "),
            "{:?}",
            rebuilt.uncovered
        );
    }

    /// A trailer of a newer body version in a pack that matches its ID is
    /// refused, naming the pack.
    #[test]
    fn rebuild_refuses_a_trailer_from_a_newer_vykar() {
        let storage = MemoryBackend::new();
        let (id, _) = put_pack(&storage, &[1, 2]);
        rewrite(&storage, id, |b, start| b[start + 6] = 2);
        // Store it under the ID of its new bytes, as a newer writer would.
        let bytes = storage.get(&id.storage_key()).unwrap().unwrap();
        storage.delete(&id.storage_key()).unwrap();
        let newer = PackId::compute(&bytes, HashAlgorithm::Blake2b).storage_key();
        storage.put(&newer, &bytes).unwrap();
        let err = rebuild_with(&storage, Vec::new())
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("written by a newer vykar"), "{err}");
        assert!(err.contains(&newer), "{err}");
    }

    /// The same bytes under the old ID: the version byte rotted, so the pack
    /// is uncovered and the rebuild goes on.
    #[test]
    fn rebuild_reports_a_rotted_version_byte_as_uncovered() {
        let storage = MemoryBackend::new();
        let (id, _) = put_pack(&storage, &[1, 2]);
        let (_, other) = put_pack(&storage, &[3]);
        rewrite(&storage, id, |b, start| b[start + 6] = 2);
        let (rebuilt, locs) = rebuild(&storage);
        assert_eq!(locs, other);
        assert_eq!(rebuilt.uncovered.len(), 1);
        assert_eq!(rebuilt.uncovered[0].pack_id, id);
        assert!(
            rebuilt.uncovered[0]
                .reason
                .contains("does not match its ID"),
            "{:?}",
            rebuilt.uncovered
        );
    }

    /// Fails range reads of one key, and lists a pack that does not exist.
    struct Faulty {
        inner: MemoryBackend,
        fail_range: Option<String>,
        phantom: Option<PackId>,
    }

    impl InnerBackend for Faulty {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for Faulty;
        except [get_range, list_sized, list_meta, server_ops];

        fn list_meta(&self, prefix: &str) -> Result<Vec<ListedObject>> {
            self.list_sized(prefix)
        }

        fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
            None
        }

        fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
            if self.fail_range.as_deref() == Some(key) {
                return Err(VykarError::Other("injected range failure".into()));
            }
            self.inner.get_range(key, offset, length)
        }

        fn list_sized(&self, prefix: &str) -> Result<Vec<ListedObject>> {
            let mut listed = self.inner.list_sized(prefix)?;
            if let Some(id) = self.phantom
                && id.storage_key().starts_with(prefix)
            {
                listed.push(ListedObject {
                    key: id.storage_key(),
                    size: Some(1000),
                    mtime: None,
                });
            }
            Ok(listed)
        }
    }

    #[test]
    fn rebuild_aborts_on_a_storage_error() {
        let inner = MemoryBackend::new();
        let (id, _) = put_pack(&inner, &[1, 2]);
        put_pack(&inner, &[3]);
        let storage = Faulty {
            inner,
            fail_range: Some(id.storage_key()),
            phantom: None,
        };
        let err = rebuild_with(&storage, Vec::new()).err().unwrap();
        assert!(err.to_string().contains("injected range failure"), "{err}");
    }

    #[test]
    fn rebuild_skips_a_pack_that_vanished_after_the_listing() {
        let inner = MemoryBackend::new();
        let (_, expected) = put_pack(&inner, &[1, 2]);
        let storage = Faulty {
            inner,
            fail_range: None,
            phantom: Some(PackId::from_bytes([0x42; 32])),
        };
        let (rebuilt, locs) = rebuild(&storage);
        assert_eq!(locs, sorted(expected));
        assert_eq!(rebuilt.counts.packs, 1);
        assert!(rebuilt.uncovered.is_empty());
    }
}
