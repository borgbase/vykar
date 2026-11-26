#![no_main]
//! Input: the plaintext of an `index.d/` object (what
//! `unpack_object_expect_with_context` returns), sealed with a deterministic
//! AEAD engine under the segment's and the base's object type and context,
//! named by the BLAKE3 of the sealed bytes, through the store-level segment
//! and base decoders (body version 2). A decoded segment re-encodes to the
//! same body; a decoded base's body is checked against a buffered reference
//! built from public functions. The raw input also goes once through the
//! segment decoder as stored bytes, which rarely authenticate.
use libfuzzer_sys::fuzz_target;
use vykar_core::compress::decompress_metadata;
use vykar_core::fuzzing;
use vykar_core::index::base_v2::validate_base_body;
use vykar_core::index::head::{BaseRef, ObjectName, SegmentKind, SegmentRef};
use vykar_core::index::segment_v2::SegmentBody;
use vykar_core::repo::format::{
    ObjectType, pack_object_with_context, unpack_object_expect_with_context,
};
use vykar_crypto::testing::DeterministicEngine;
use vykar_types::chunk_id::ChunkHasher;
use vykar_types::error::VykarError;
use vykar_types::hash::HashAlgorithm;

/// `compress/mod.rs` `TAG_ZSTD`.
const TAG_ZSTD: u8 = 0x02;

fn acceptable(e: &VykarError) -> bool {
    fuzzing::is_object_damage(e) || matches!(e, VykarError::UnsupportedVersion(_))
}

fn engine() -> DeterministicEngine {
    DeterministicEngine::new(ChunkHasher::new(HashAlgorithm::Blake3, [7; 32]))
}

fuzz_target!(|data: &[u8]| {
    let engine = engine();
    let anchor = std::env::temp_dir().join("vykar-fuzz-index-object");

    // The raw input as stored bytes.
    let raw_ref = SegmentRef {
        name: ObjectName::of(data),
        size: data.len() as u64,
        kind: SegmentKind::Backup,
        entries: 1 << 20,
    };
    if let Err(e) = fuzzing::decode_segment_v2(&raw_ref, data, &engine) {
        assert!(acceptable(&e), "{e:?}");
    }

    // Segment, under both kinds and a generous entry bound.
    let Ok(sealed) = pack_object_with_context(
        ObjectType::IndexSegment,
        fuzzing::SEGMENT_CONTEXT,
        data,
        &engine,
    ) else {
        return;
    };
    let name = ObjectName::of(&sealed);
    let size = sealed.len() as u64;
    for kind in [SegmentKind::Backup, SegmentKind::Maintenance] {
        let mut seg_ref = SegmentRef {
            name,
            size,
            kind,
            entries: 1 << 20,
        };
        match fuzzing::decode_segment_v2(&seg_ref, &sealed, &engine) {
            Ok(seg) => {
                assert_eq!(seg.kind, kind);
                let body = seg.encode().expect("a decoded segment encodes");
                assert_eq!(SegmentBody::decode(&body, seg.entries()).unwrap(), seg);
            }
            Err(e) => assert!(acceptable(&e), "{e:?}"),
        }
        seg_ref.name.0[0] ^= 1;
        match fuzzing::decode_segment_v2(&seg_ref, &sealed, &engine) {
            Err(VykarError::InvalidFormat(msg)) => {
                assert!(msg.contains("does not match its name"), "{msg}");
            }
            other => panic!("a wrong name must fail the name check: {other:?}"),
        }
    }

    // Base, against the buffered reference.
    let Ok(sealed) =
        pack_object_with_context(ObjectType::IndexBase, fuzzing::BASE_CONTEXT, data, &engine)
    else {
        return;
    };
    let reference = unpack_object_expect_with_context(
        &sealed,
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
        name: ObjectName::of(&sealed),
        size: sealed.len() as u64,
        entries: reference.unwrap_or(0),
    };
    let store = fuzzing::decode_base_v2(&base_ref, sealed, &engine, &anchor);
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
