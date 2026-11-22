#![no_main]
//! Input: the stored bytes of an `index.d/` object, named by their own
//! BLAKE3, through the store-level segment and base decoders (body version
//! 2) of a plaintext repository. A decoded segment re-encodes to the same
//! body; a decoded base's body is checked against a buffered reference built
//! from public functions.
use libfuzzer_sys::fuzz_target;
use vykar_core::compress::decompress_metadata;
use vykar_core::fuzzing;
use vykar_core::index::base_v2::validate_base_body;
use vykar_core::index::head::{BaseRef, ObjectName, SegmentKind, SegmentRef};
use vykar_core::index::segment_v2::SegmentBody;
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

    // Segment, under both kinds and a generous entry bound.
    for kind in [SegmentKind::Backup, SegmentKind::Maintenance] {
        let mut seg_ref = SegmentRef {
            name,
            size,
            kind,
            entries: 1 << 20,
        };
        match fuzzing::decode_segment_v2(&seg_ref, data, &engine) {
            Ok(seg) => {
                assert_eq!(seg.kind, kind);
                let body = seg.encode().expect("a decoded segment encodes");
                assert_eq!(SegmentBody::decode(&body, seg.entries()).unwrap(), seg);
            }
            Err(e) => assert!(acceptable(&e), "{e:?}"),
        }
        seg_ref.name.0[0] ^= 1;
        match fuzzing::decode_segment_v2(&seg_ref, data, &engine) {
            Err(VykarError::InvalidFormat(msg)) => {
                assert!(msg.contains("does not match its name"), "{msg}");
            }
            other => panic!("a wrong name must fail the name check: {other:?}"),
        }
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
    .and_then(|body| validate_base_body(&body).ok())
    .map(|(_, entries)| u64::from(entries));
    let base_ref = BaseRef {
        name,
        size,
        entries: reference.unwrap_or(0),
    };
    let store = fuzzing::decode_base_v2(&base_ref, data.to_vec(), &engine, &anchor);
    match (reference, store) {
        (Some(n), Ok(entries)) => assert_eq!(entries, n),
        (None, Err(e)) => assert!(acceptable(&e), "{e:?}"),
        (Some(_), Err(e)) => panic!("reference decoded, store failed: {e:?}"),
        (None, Ok(entries)) => {
            // Only the empty base (no records) matches the zeroed ref.
            assert_eq!(entries, 0);
        }
    }
});
