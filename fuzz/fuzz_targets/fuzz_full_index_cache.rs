#![no_main]
//! A local full index cache file with a correct BLAKE3 trailer, opened with
//! `open_verified`, then encoded as a base and applied over.
//!
//! Input: byte 0 bit 0 selects the header.
//! - 0: the rest, cut to whole 80-byte records, is the entry region, under a
//!   well-formed header for that count. The file must open.
//! - 1: bytes 1..29 are the raw 28-byte header, the rest is the entry region.
use libfuzzer_sys::fuzz_target;
use vykar_core::index::base::{decode_unnamed_base_to_temp, encode_base};
use vykar_core::index::dedup_cache::MmapFullIndexCache;
use vykar_core::index::segment::{apply_to_temp, ChainStep, Segment};
use vykar_types::error::VykarError;

// `index/dedup_cache.rs` `FULL_MAGIC`, `FULL_VERSION`, `FULL_ENTRY_SIZE`
// and the layout `write_cache_header` writes.
const MAGIC: &[u8; 8] = b"VGFULL\0\0";
const VERSION: u32 = 3;
const ENTRY_SIZE: usize = 80;
const HEADER_SIZE: usize = 28;

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    let synthesized = mode & 1 == 0;
    let (header, region) = if synthesized {
        let count = rest.len() / ENTRY_SIZE;
        let Ok(count32) = u32::try_from(count) else {
            return;
        };
        let mut header = Vec::with_capacity(HEADER_SIZE);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.extend_from_slice(&1u64.to_le_bytes());
        header.extend_from_slice(&count32.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        (header, &rest[..count * ENTRY_SIZE])
    } else {
        let Some((header, region)) = rest.split_at_checked(HEADER_SIZE) else {
            return;
        };
        (header.to_vec(), region)
    };

    let tmp = std::env::temp_dir();
    let path = tmp.join(format!("vykar-fuzz-full-cache-{}", std::process::id()));
    let anchor = tmp.join("vykar-fuzz-full-cache-out");
    let mut file = header;
    file.extend_from_slice(region);
    file.extend_from_slice(blake3::hash(region).as_bytes());
    std::fs::write(&path, &file).expect("write the input cache");

    let opened = MmapFullIndexCache::open_verified(&path);
    if let Some(cache) = opened {
        check(&cache, &anchor);
    } else {
        assert!(
            !synthesized,
            "a well-framed cache with a correct trailer opens"
        );
    }
    let _ = std::fs::remove_file(&path);
});

fn check(cache: &MmapFullIndexCache, anchor: &std::path::Path) {
    let state = cache.state_hash();
    let count = u64::from(cache.entry_count());
    let entries: Vec<_> = cache.iter().collect();
    let sorted = entries
        .windows(2)
        .all(|w| w[0].chunk_id.as_bytes() < w[1].chunk_id.as_bytes());

    let mut body = Vec::new();
    encode_base(cache, &mut body).expect("any opened cache encodes");
    match decode_unnamed_base_to_temp(body.as_slice(), anchor) {
        Ok((_, hash, n)) => {
            assert!(sorted, "an unsorted cache's base decodes");
            assert_eq!(hash, state);
            assert_eq!(n, count);
        }
        Err(e) => assert!(!sorted, "a sorted cache's base fails to decode: {e:?}"),
    }

    // The empty chain copies the cache without checking its order.
    let (_, hash, n) = apply_to_temp(cache, &[], anchor).expect("the empty chain applies");
    assert_eq!(hash, state);
    assert_eq!(n, count);

    let seg = Segment::empty(state, count);
    let step = ChainStep {
        segment: &seg,
        state_hash_after: state,
        entries_after: count,
    };
    match apply_to_temp(cache, &[step], anchor) {
        Ok((_, hash, n)) => {
            assert!(sorted, "an empty layer accepts an unsorted cache");
            assert_eq!(hash, state);
            assert_eq!(n, count);
        }
        Err(e) => {
            assert!(!sorted, "an empty layer rejects a sorted cache: {e:?}");
            assert!(matches!(e, VykarError::InvalidFormat(_)), "{e:?}");
        }
    }
}
