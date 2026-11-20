//! Index base body: the full sorted records of one index state, with pack
//! IDs moved into a table.
//!
//! Layout (before compression):
//!
//! ```text
//! version u8
//! entries uvarint
//! packs   uvarint
//! pack table: packs × 32 bytes, strictly increasing
//! records: entries × { chunk_id 32 bytes, refcount uvarint,
//!                      stored_size uvarint, pack index uvarint,
//!                      pack_offset uvarint }, chunk IDs strictly increasing
//! ```
//!
//! Pack IDs are 32 of the 80 canonical bytes per entry and are incompressible
//! where they repeat beyond the compressor's window; the table stores each
//! once. Every table entry must be referenced, so the encoding of a state is
//! unique. Encoding and decoding stream: neither holds the records in memory.

// The varint buffer holds at most 10 bytes for a u64; pack slots are checked
// against the table length before use, and every pack is in `slot`.
#![allow(clippy::indexing_slicing)]

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;

use tempfile::NamedTempFile;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::dedup_cache::{FullCacheEntry, FullCacheWriter, MmapFullIndexCache, StateHash};

const BASE_VERSION: u8 = 1;

fn invalid(msg: impl Into<String>) -> VykarError {
    VykarError::InvalidFormat(format!("index base: {}", msg.into()))
}

fn put_uvarint<W: Write>(w: &mut W, mut v: u64) -> Result<()> {
    let mut buf = [0u8; 10];
    let mut n = 0;
    while v >= 0x80 {
        buf[n] = (v as u8) | 0x80;
        v >>= 7;
        n += 1;
    }
    buf[n] = v as u8;
    w.write_all(&buf[..=n])?;
    Ok(())
}

fn get_u8<R: BufRead>(r: &mut R) -> Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b).map_err(|_| invalid("truncated"))?;
    Ok(b[0])
}

fn get_uvarint<R: BufRead>(r: &mut R) -> Result<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let b = get_u8(r)?;
        let bits = u64::from(b & 0x7f);
        if shift == 63 && bits > 1 {
            return Err(invalid("varint overflow"));
        }
        v |= bits << shift;
        if b & 0x80 == 0 {
            // Canonical: no redundant trailing zero groups.
            if b == 0 && shift > 0 {
                return Err(invalid("non-canonical varint"));
            }
            return Ok(v);
        }
    }
    Err(invalid("varint overflow"))
}

fn get_u32<R: BufRead>(r: &mut R, what: &str) -> Result<u32> {
    u32::try_from(get_uvarint(r)?).map_err(|_| invalid(format!("{what} out of range")))
}

fn get_32<R: BufRead>(r: &mut R) -> Result<[u8; 32]> {
    let mut b = [0u8; 32];
    r.read_exact(&mut b).map_err(|_| invalid("truncated"))?;
    Ok(b)
}

/// Write the base body of `cache` to `w`. Two passes over the cache: one for
/// the pack table, one for the records.
pub fn encode_base<W: Write>(cache: &MmapFullIndexCache, w: &mut W) -> Result<()> {
    let mut packs: Vec<PackId> = {
        let mut seen = std::collections::HashSet::new();
        cache
            .iter()
            .map(|e| e.pack_id)
            .filter(|p| seen.insert(*p))
            .collect()
    };
    packs.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    let slot: HashMap<PackId, u64> = packs
        .iter()
        .enumerate()
        .map(|(i, p)| (*p, i as u64))
        .collect();

    w.write_all(&[BASE_VERSION])?;
    put_uvarint(w, u64::from(cache.entry_count()))?;
    put_uvarint(w, packs.len() as u64)?;
    for p in &packs {
        w.write_all(p.as_bytes())?;
    }
    for e in cache.iter() {
        w.write_all(e.chunk_id.as_bytes())?;
        put_uvarint(w, u64::from(e.refcount))?;
        put_uvarint(w, u64::from(e.stored_size))?;
        put_uvarint(w, slot[&e.pack_id])?;
        put_uvarint(w, e.pack_offset)?;
    }
    Ok(())
}

/// Decode a base body from `r` into a full index cache written to a temp
/// file beside `target`. `expected_entries` (from the head) bounds the
/// header before anything is allocated. Returns the temp file, the state
/// hash of the decoded records, and their count.
///
/// Rejects anything [`encode_base`] would not produce: unsorted or
/// duplicate pack IDs or chunk IDs, unreferenced pack table entries,
/// out-of-range values, and trailing bytes.
pub fn decode_base_to_temp<R: Read>(
    r: R,
    expected_entries: u64,
    target: &Path,
) -> Result<(NamedTempFile, StateHash, u64)> {
    decode_base_inner(r, Some(expected_entries), target)
}

/// [`decode_base_to_temp`] for a base no head names (`check --repair`
/// without a head): the entry count is the base's own, at most `u32::MAX`.
pub fn decode_unnamed_base_to_temp<R: Read>(
    r: R,
    target: &Path,
) -> Result<(NamedTempFile, StateHash, u64)> {
    decode_base_inner(r, None, target)
}

fn decode_base_inner<R: Read>(
    r: R,
    expected_entries: Option<u64>,
    target: &Path,
) -> Result<(NamedTempFile, StateHash, u64)> {
    let mut r = BufReader::with_capacity(256 << 10, r);
    let version = get_u8(&mut r)?;
    if version != BASE_VERSION {
        return Err(VykarError::UnsupportedVersion(u32::from(version)));
    }
    let entries = get_uvarint(&mut r)?;
    match expected_entries {
        Some(expected) if entries != expected => {
            return Err(invalid(format!(
                "holds {entries} entries, head names {expected}"
            )));
        }
        None if entries > u64::from(u32::MAX) => {
            return Err(invalid("entry count out of range"));
        }
        _ => {}
    }
    let n_packs = get_uvarint(&mut r)?;
    if n_packs > entries || (entries > 0 && n_packs == 0) {
        return Err(invalid("pack count out of range"));
    }

    // Grown as read, so a lying count fails at the end of the input.
    let mut packs: Vec<PackId> = Vec::new();
    for _ in 0..n_packs {
        let p = PackId::from_bytes(get_32(&mut r)?);
        if packs
            .last()
            .is_some_and(|last| last.as_bytes() >= p.as_bytes())
        {
            return Err(invalid("pack table not strictly sorted"));
        }
        packs.push(p);
    }
    let mut referenced = vec![false; packs.len()];

    let mut w = FullCacheWriter::new_beside(target)?;
    let mut last: Option<ChunkId> = None;
    for _ in 0..entries {
        let chunk_id = ChunkId::from_bytes(get_32(&mut r)?);
        if last.is_some_and(|l| l.as_bytes() >= chunk_id.as_bytes()) {
            return Err(invalid("records not strictly sorted"));
        }
        last = Some(chunk_id);
        let refcount = get_u32(&mut r, "refcount")?;
        let stored_size = get_u32(&mut r, "stored size")?;
        let slot = usize::try_from(get_uvarint(&mut r)?)
            .ok()
            .filter(|&s| s < packs.len())
            .ok_or_else(|| invalid("pack index out of range"))?;
        referenced[slot] = true;
        let pack_offset = get_uvarint(&mut r)?;
        w.push(&FullCacheEntry {
            chunk_id,
            refcount,
            stored_size,
            pack_id: packs[slot],
            pack_offset,
        })?;
    }
    if referenced.iter().any(|r| !r) {
        return Err(invalid("unreferenced pack table entry"));
    }
    let rest = r
        .fill_buf()
        .map_err(|e| invalid(format!("after the records: {e}")))?;
    if !rest.is_empty() {
        return Err(invalid("trailing bytes"));
    }
    w.finish(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::dedup_cache::build_full_index_cache_to_path;
    use crate::index::ChunkIndex;

    fn cache_of(index: &ChunkIndex, dir: &Path) -> MmapFullIndexCache {
        let path = dir.join("full");
        build_full_index_cache_to_path(index, 1, &path).unwrap();
        MmapFullIndexCache::open_verified(&path).unwrap()
    }

    fn sample_index(n: u32) -> ChunkIndex {
        let mut index = ChunkIndex::new();
        for i in 0..n {
            let mut id = [0u8; 32];
            id[..4].copy_from_slice(&i.wrapping_mul(2_654_435_761).to_be_bytes());
            let pack = PackId::from_bytes([(i % 7) as u8 + 1; 32]);
            index.add(ChunkId::from_bytes(id), 100 + i, pack, u64::from(i) * 1000);
            if i % 3 == 0 {
                index.increment_refcount_by(&ChunkId::from_bytes(id), i);
            }
        }
        index
    }

    fn encode(cache: &MmapFullIndexCache) -> Vec<u8> {
        let mut out = Vec::new();
        encode_base(cache, &mut out).unwrap();
        out
    }

    #[test]
    fn round_trips_with_equal_state_hash() {
        let dir = tempfile::tempdir().unwrap();
        for n in [0u32, 1, 500] {
            let cache = cache_of(&sample_index(n), dir.path());
            let body = encode(&cache);
            let (tmp, hash, count) =
                decode_base_to_temp(body.as_slice(), u64::from(n), &dir.path().join("x")).unwrap();
            assert_eq!(hash, cache.state_hash());
            assert_eq!(count, u64::from(n));
            let decoded = MmapFullIndexCache::open_verified(tmp.path()).unwrap();
            assert!(decoded.iter().eq(cache.iter()));
        }
    }

    #[test]
    fn pack_table_is_smaller_than_canonical_records() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_of(&sample_index(1000), dir.path());
        assert!(encode(&cache).len() < 1000 * 50);
    }

    #[test]
    fn rejects_malformed_bodies() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("x");
        let cache = cache_of(&sample_index(20), dir.path());
        let body = encode(&cache);
        let decode = |b: &[u8], n: u64| decode_base_to_temp(b, n, &target).map(|_| ());

        assert!(decode(&body, 20).is_ok());
        assert!(
            decode(&body, 21).is_err(),
            "entry count disagrees with head"
        );
        assert!(decode(&body[..body.len() - 1], 20).is_err(), "truncated");
        let mut trailing = body.clone();
        trailing.push(0);
        assert!(decode(&trailing, 20).is_err(), "trailing bytes");
        let mut version = body.clone();
        version[0] = 2;
        assert!(
            matches!(decode(&version, 20), Err(VykarError::UnsupportedVersion(2))),
            "unknown version"
        );

        // Header: version, entries=20 (1 byte), packs=7 (1 byte), table.
        let table = 3;
        let mut unsorted = body.clone();
        unsorted.swap(table, table + 32);
        assert!(decode(&unsorted, 20).is_err(), "pack table order");

        let records = table + 7 * 32;
        let mut swapped = body.clone();
        swapped[records] ^= 0xff;
        assert!(decode(&swapped, 20).is_err(), "record order");
    }

    #[test]
    fn rejects_unreferenced_pack_and_bad_slot() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("x");
        let mut body = vec![BASE_VERSION];
        put_uvarint(&mut body, 1).unwrap();
        put_uvarint(&mut body, 1).unwrap();
        body.extend_from_slice(&[1; 32]);
        body.extend_from_slice(&[5; 32]);
        for v in [1u64, 10] {
            put_uvarint(&mut body, v).unwrap();
        }
        let good_tail = {
            let mut t = Vec::new();
            put_uvarint(&mut t, 0).unwrap();
            put_uvarint(&mut t, 7).unwrap();
            t
        };
        let mut good = body.clone();
        good.extend_from_slice(&good_tail);
        assert!(decode_base_to_temp(good.as_slice(), 1, &target).is_ok());

        let mut bad_slot = body.clone();
        put_uvarint(&mut bad_slot, 1).unwrap();
        put_uvarint(&mut bad_slot, 7).unwrap();
        assert!(decode_base_to_temp(bad_slot.as_slice(), 1, &target).is_err());

        // Two table entries, both records in the first pack.
        let mut extra = vec![BASE_VERSION];
        put_uvarint(&mut extra, 2).unwrap();
        put_uvarint(&mut extra, 2).unwrap();
        extra.extend_from_slice(&[1; 32]);
        extra.extend_from_slice(&[2; 32]);
        for id in [5u8, 6] {
            extra.extend_from_slice(&[id; 32]);
            for v in [1u64, 10, 0, 7] {
                put_uvarint(&mut extra, v).unwrap();
            }
        }
        assert!(decode_base_to_temp(extra.as_slice(), 2, &target).is_err());
    }

    #[test]
    fn unnamed_decode_matches_named() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("x");
        for n in [0u32, 1, 200] {
            let cache = cache_of(&sample_index(n), dir.path());
            let body = encode(&cache);
            let (named, named_hash, named_count) =
                decode_base_to_temp(body.as_slice(), u64::from(n), &target).unwrap();
            let (unnamed, hash, count) =
                decode_unnamed_base_to_temp(body.as_slice(), &target).unwrap();
            assert_eq!((hash, count), (named_hash, named_count));
            let a = MmapFullIndexCache::open_verified(named.path()).unwrap();
            let b = MmapFullIndexCache::open_verified(unnamed.path()).unwrap();
            assert!(a.iter().eq(b.iter()));
        }
    }

    #[test]
    fn unnamed_decode_bounds_the_entry_count() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("x");
        let mut body = vec![BASE_VERSION];
        put_uvarint(&mut body, u64::from(u32::MAX) + 1).unwrap();
        let err = decode_unnamed_base_to_temp(body.as_slice(), &target).unwrap_err();
        assert!(
            err.to_string().contains("entry count out of range"),
            "{err}"
        );
        body[0] = 2;
        assert!(matches!(
            decode_unnamed_base_to_temp(body.as_slice(), &target),
            Err(VykarError::UnsupportedVersion(2))
        ));
    }
}
