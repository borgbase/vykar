#![no_main]
//! Input: the stored bytes of an `index.d/` object, named by their own
//! BLAKE3, through the store-level segment and base decoders of a
//! plaintext repository. The base decoder is checked against a buffered
//! reference built from public functions.
use libfuzzer_sys::fuzz_target;
use vykar_core::compress::decompress_metadata;
use vykar_core::fuzzing;
use vykar_core::index::base;
use vykar_core::index::dedup_cache::MmapFullIndexCache;
use vykar_core::index::head::{BaseRef, ObjectName, SegmentRef};
use vykar_core::index::segment::Segment;
use vykar_core::repo::format::{ObjectType, unpack_object_expect_with_context};
use vykar_crypto::PlaintextEngine;
use vykar_types::chunk_id::ChunkHasher;
use vykar_types::error::VykarError;
use vykar_types::hash::HashAlgorithm;

/// `compress/mod.rs` `TAG_ZSTD`.
const TAG_ZSTD: u8 = 0x02;

fn acceptable(e: &VykarError) -> bool {
    fuzzing::is_object_damage(e) || matches!(e, VykarError::UnsupportedVersion(_))
}

fuzz_target!(|data: &[u8]| {
    let engine = PlaintextEngine::new(ChunkHasher::new(HashAlgorithm::Blake3, [7; 32]));
    let anchor = std::env::temp_dir().join("vykar-fuzz-index-object");
    let name = ObjectName::of(data);
    let size = data.len() as u64;

    // Segment.
    let mut seg_ref = SegmentRef {
        name,
        size,
        state_hash_after: [0; 32],
        entries_after: 0,
    };
    match fuzzing::decode_segment(&seg_ref, data, &engine) {
        Ok(seg) => {
            assert!(seg.validate().is_ok(), "a decoded segment is valid");
            assert_eq!(Segment::decode(&seg.encode()).unwrap(), seg);
        }
        Err(e) => assert!(acceptable(&e), "{e:?}"),
    }
    seg_ref.name.0[0] ^= 1;
    match fuzzing::decode_segment(&seg_ref, data, &engine) {
        Err(VykarError::InvalidFormat(msg)) => {
            assert!(msg.contains("does not match its name"), "{msg}");
        }
        other => panic!("a wrong name must fail the name check: {other:?}"),
    }

    // Base, against the buffered reference.
    let reference = unpack_object_expect_with_context(
        data,
        ObjectType::IndexBase,
        fuzzing::BASE_CONTEXT,
        &engine,
    )
    .ok()
    .filter(|plain| plain.first() == Some(&TAG_ZSTD))
    .and_then(|plain| decompress_metadata(&plain).ok())
    .and_then(|body| base::decode_unnamed_base_to_temp(body.as_slice(), &anchor).ok())
    .map(|(_, hash, n)| (hash, n));
    let (state_hash, entries) = reference.unwrap_or(([0; 32], 0));
    let base_ref = BaseRef {
        name,
        size,
        state_hash,
        entries,
    };
    let store = fuzzing::decode_base_to_temp(&base_ref, data.to_vec(), &engine, &anchor);
    match (reference, store) {
        (Some((hash, n)), Ok(tmp)) => {
            let c = MmapFullIndexCache::open_verified(&tmp).expect("decoded base verifies");
            assert_eq!(c.state_hash(), hash);
            assert_eq!(u64::from(c.entry_count()), n);
        }
        (None, Err(e)) => assert!(acceptable(&e), "{e:?}"),
        (Some(_), Err(e)) => panic!("reference decoded, store failed: {e:?}"),
        // Not reachable today: the zeroed ref matches no decodable base.
        (None, Ok(_)) => panic!("reference failed, store decoded"),
    }
});
