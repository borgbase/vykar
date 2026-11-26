use crate::compress::{self, Compression};
use crate::repo::format::{
    BlobLocation, ChunkDecodeError, ChunkDecodeStage, ObjectType, decode_chunk_into, pack_object,
    pack_object_streaming, pack_object_with_context, unpack_object, unpack_object_expect,
    unpack_object_expect_with_context, unpack_object_with_context,
};
use vykar_crypto::CryptoEngine;
use vykar_crypto::aes_gcm::Aes256GcmEngine;
use vykar_crypto::testing::DeterministicEngine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::VykarError;
use vykar_types::pack_id::PackId;

// Plaintext-only: 10-single-protocol step 3 removes this.
#[test]
fn roundtrip_plaintext() {
    let engine = vykar_crypto::PlaintextEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
    let data = b"manifest data here";
    let packed = pack_object(ObjectType::Manifest, data, &engine).unwrap();
    let (obj_type, unpacked) = unpack_object(&packed, &engine).unwrap();
    assert_eq!(obj_type, ObjectType::Manifest);
    assert_eq!(unpacked, data);
}

#[test]
fn roundtrip_encrypted() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], crate::testutil::chunk_hasher_for([0x22; 32]));
    let data = b"secret chunk data";
    let packed = pack_object(ObjectType::ChunkData, data, &engine).unwrap();
    let (obj_type, unpacked) = unpack_object(&packed, &engine).unwrap();
    assert_eq!(obj_type, ObjectType::ChunkData);
    assert_eq!(unpacked, data);
}

#[test]
fn type_tag_is_first_byte() {
    let engine = DeterministicEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
    let packed = pack_object(ObjectType::Manifest, b"data", &engine).unwrap();
    assert_eq!(packed[0], ObjectType::Manifest as u8);

    let packed2 = pack_object(ObjectType::ChunkData, b"data", &engine).unwrap();
    assert_eq!(packed2[0], ObjectType::ChunkData as u8);
}

#[test]
fn wrong_type_tag_encrypted_fails_aad() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], crate::testutil::chunk_hasher_for([0x22; 32]));
    let data = b"secret";
    let mut packed = pack_object(ObjectType::Manifest, data, &engine).unwrap();
    // Change the type tag byte — AAD mismatch should cause decryption failure
    packed[0] = ObjectType::ChunkData as u8;
    let result = unpack_object(&packed, &engine);
    assert!(result.is_err());
}

#[test]
fn empty_data_fails() {
    let engine = DeterministicEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
    let result = unpack_object(b"", &engine);
    assert!(result.is_err());
    match result.unwrap_err() {
        VykarError::InvalidFormat(msg) => assert_eq!(msg, "empty object"),
        other => panic!("expected InvalidFormat, got: {other}"),
    }
}

#[test]
fn unknown_type_tag_fails() {
    let engine = DeterministicEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
    let result = unpack_object(&[0xFF, 0x01, 0x02], &engine);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        VykarError::UnknownObjectType(0xFF)
    ));
}

#[test]
fn object_type_from_u8_valid() {
    assert_eq!(ObjectType::from_u8(0).unwrap(), ObjectType::Config);
    assert_eq!(ObjectType::from_u8(1).unwrap(), ObjectType::Manifest);
    assert_eq!(ObjectType::from_u8(2).unwrap(), ObjectType::SnapshotMeta);
    assert_eq!(ObjectType::from_u8(3).unwrap(), ObjectType::ChunkData);
    assert_eq!(ObjectType::from_u8(4).unwrap(), ObjectType::ChunkIndex);
    assert_eq!(ObjectType::from_u8(5).unwrap(), ObjectType::PackHeader);
    assert_eq!(ObjectType::from_u8(6).unwrap(), ObjectType::FileCache);
    assert_eq!(ObjectType::from_u8(7).unwrap(), ObjectType::PendingIndex);
    assert_eq!(ObjectType::from_u8(8).unwrap(), ObjectType::SnapshotCache);
    assert_eq!(ObjectType::from_u8(9).unwrap(), ObjectType::IndexHead);
    assert_eq!(ObjectType::from_u8(10).unwrap(), ObjectType::IndexBase);
    assert_eq!(ObjectType::from_u8(11).unwrap(), ObjectType::IndexSegment);
    assert_eq!(ObjectType::from_u8(12).unwrap(), ObjectType::PackTrailer);
    assert_eq!(ObjectType::from_u8(13).unwrap(), ObjectType::PackMap);
    assert_eq!(ObjectType::from_u8(14).unwrap(), ObjectType::SweepRecord);
}

#[test]
fn object_type_from_u8_invalid() {
    assert!(ObjectType::from_u8(15).is_err());
    assert!(ObjectType::from_u8(255).is_err());
}

#[test]
fn unpack_expect_rejects_wrong_object_type() {
    let engine = DeterministicEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
    let packed = pack_object(ObjectType::Manifest, b"data", &engine).unwrap();

    let err = unpack_object_expect(&packed, ObjectType::ChunkData, &engine).unwrap_err();
    assert!(matches!(err, VykarError::InvalidFormat(_)));
    assert!(err.to_string().contains("unexpected object type"));
}

// Plaintext-only: 10-single-protocol step 3 removes this.
#[test]
fn pack_object_streaming_roundtrip_plaintext() {
    let engine = vykar_crypto::PlaintextEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
    let data = b"streaming plaintext data";
    let packed = pack_object_streaming(ObjectType::Manifest, data.len(), &engine, |buf| {
        buf.extend_from_slice(data);
        Ok(())
    })
    .unwrap();
    let unpacked = unpack_object_expect(&packed, ObjectType::Manifest, &engine).unwrap();
    assert_eq!(unpacked, data);
}

#[test]
fn pack_object_streaming_roundtrip_encrypted() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], crate::testutil::chunk_hasher_for([0x22; 32]));
    let data = b"streaming encrypted data";
    let packed = pack_object_streaming(ObjectType::ChunkIndex, data.len(), &engine, |buf| {
        buf.extend_from_slice(data);
        Ok(())
    })
    .unwrap();
    let unpacked = unpack_object_expect(&packed, ObjectType::ChunkIndex, &engine).unwrap();
    assert_eq!(unpacked, data);
}

#[test]
fn pack_object_streaming_matches_pack_object() {
    // Under the same nonce (a fresh deterministic engine each), streaming
    // output is byte-identical to pack_object.
    let engine = || DeterministicEngine::new(crate::testutil::chunk_hasher_for([0xBB; 32]));
    let data = b"verify identical output";
    let packed_normal = pack_object(ObjectType::ChunkData, data, &engine()).unwrap();
    let packed_streaming =
        pack_object_streaming(ObjectType::ChunkData, data.len(), &engine(), |buf| {
            buf.extend_from_slice(data);
            Ok(())
        })
        .unwrap();
    assert_eq!(packed_normal, packed_streaming);
}

#[test]
fn context_bound_roundtrip_encrypted() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], crate::testutil::chunk_hasher_for([0x22; 32]));
    let data = b"context-bound payload";
    let context = b"chunk-identity";
    let packed = pack_object_with_context(ObjectType::ChunkData, context, data, &engine).unwrap();
    let unpacked =
        unpack_object_expect_with_context(&packed, ObjectType::ChunkData, context, &engine)
            .unwrap();
    assert_eq!(unpacked, data);
}

#[test]
fn context_bound_wrong_context_fails() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], crate::testutil::chunk_hasher_for([0x22; 32]));
    let packed =
        pack_object_with_context(ObjectType::ChunkData, b"chunk-a", b"ciphertext", &engine)
            .unwrap();
    let err = unpack_object_with_context(&packed, b"chunk-b", &engine).unwrap_err();
    assert!(matches!(err, VykarError::DecryptionFailed));
}

#[test]
fn context_bound_unpack_rejects_legacy_object() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], crate::testutil::chunk_hasher_for([0x22; 32]));
    let packed = pack_object(ObjectType::Manifest, b"legacy-manifest", &engine).unwrap();
    let err =
        unpack_object_expect_with_context(&packed, ObjectType::Manifest, b"manifest", &engine)
            .unwrap_err();
    assert!(matches!(err, VykarError::DecryptionFailed));
}

fn deterministic_engine() -> DeterministicEngine {
    DeterministicEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]))
}

fn aes_engine() -> Aes256GcmEngine {
    Aes256GcmEngine::new(&[0x11; 32], crate::testutil::chunk_hasher_for([0x22; 32]))
}

/// A stored chunk blob of `payload` (already compressed) under `id`.
fn chunk_blob(id: &ChunkId, payload: &[u8], engine: &dyn CryptoEngine) -> Vec<u8> {
    pack_object_with_context(ObjectType::ChunkData, id.as_bytes(), payload, engine).unwrap()
}

fn location(chunk_id: ChunkId, raw: &[u8]) -> BlobLocation {
    BlobLocation {
        chunk_id,
        pack_id: PackId::from_bytes([0x07; 32]),
        pack_offset: 7,
        stored_size: raw.len() as u32,
    }
}

fn decode(
    raw: &[u8],
    at: BlobLocation,
    engine: &dyn CryptoEngine,
    hint: Option<usize>,
    verify: bool,
) -> Result<Vec<u8>, ChunkDecodeError> {
    let mut out = vec![0xEE; 3];
    decode_chunk_into(&mut raw.to_vec(), at, engine, hint, verify, &mut out).map(|()| out)
}

#[test]
fn decode_chunk_round_trips_every_codec() {
    let data = b"chunk payload chunk payload chunk payload".repeat(20);
    let engines: [Box<dyn CryptoEngine>; 2] =
        [Box::new(aes_engine()), Box::new(deterministic_engine())];
    for engine in &engines {
        let id = ChunkId::compute(engine.chunk_hasher(), &data);
        for codec in [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ] {
            let raw = chunk_blob(
                &id,
                &compress::compress(codec, &data).unwrap(),
                engine.as_ref(),
            );
            for hint in [None, Some(data.len())] {
                for verify in [false, true] {
                    let out = decode(&raw, location(id, &raw), engine.as_ref(), hint, verify)
                        .unwrap_or_else(|e| panic!("{codec:?} hint {hint:?}: {e:?}"));
                    assert_eq!(out, data, "{codec:?} hint {hint:?}");
                }
            }
        }
    }
}

#[test]
fn decode_chunk_decrypt_stage() {
    let engine = aes_engine();
    let data = b"decrypt stage";
    let id = ChunkId::compute(engine.chunk_hasher(), data);
    let payload = compress::compress(Compression::None, data).unwrap();

    let raw = chunk_blob(&id, &payload, &engine);
    let other = ChunkId::from_bytes([0x33; 32]);
    let at = location(other, &raw);
    let e = decode(&raw, at, &engine, None, false).unwrap_err();
    assert!(
        matches!(&e.stage, ChunkDecodeStage::Decrypt(inner) if matches!(**inner, VykarError::DecryptionFailed)),
        "{e:?}"
    );
    assert_eq!(e.at, at);

    let manifest =
        pack_object_with_context(ObjectType::Manifest, id.as_bytes(), &payload, &engine).unwrap();
    let e = decode(&manifest, location(id, &manifest), &engine, None, false).unwrap_err();
    assert!(
        matches!(&e.stage, ChunkDecodeStage::Decrypt(inner)
            if matches!(&**inner, VykarError::InvalidFormat(m) if m.starts_with("unexpected object type"))),
        "{e:?}"
    );

    let e = decode(&[], location(id, &[]), &engine, None, false).unwrap_err();
    assert!(
        matches!(&e.stage, ChunkDecodeStage::Decrypt(inner)
            if matches!(&**inner, VykarError::InvalidFormat(m) if m == "empty object")),
        "{e:?}"
    );
}

#[test]
fn decode_chunk_decompress_stage() {
    let engine = deterministic_engine();
    let id = ChunkId::from_bytes([0x44; 32]);

    let raw = chunk_blob(&id, &[0xFF, 1, 2, 3], &engine);
    let e = decode(&raw, location(id, &raw), &engine, None, false).unwrap_err();
    assert!(
        matches!(&e.stage, ChunkDecodeStage::Decompress(inner)
            if matches!(**inner, VykarError::UnknownCompressionTag(0xFF))),
        "{e:?}"
    );

    let raw = chunk_blob(&id, compress::tests::ZSTD_EMPTY_COMPRESSED_BLOCK, &engine);
    let e = decode(&raw, location(id, &raw), &engine, None, false).unwrap_err();
    assert!(
        matches!(&e.stage, ChunkDecodeStage::Decompress(inner)
            if matches!(**inner, VykarError::Decompression(_))),
        "{e:?}"
    );
}

#[test]
fn decode_chunk_size_mismatch_only_with_hint() {
    let engine = deterministic_engine();
    let data = b"sized payload";
    let id = ChunkId::compute(engine.chunk_hasher(), data);
    let raw = chunk_blob(
        &id,
        &compress::compress(Compression::None, data).unwrap(),
        &engine,
    );
    let at = location(id, &raw);

    let e = decode(&raw, at, &engine, Some(data.len() + 1), false).unwrap_err();
    assert!(
        matches!(
            e.stage,
            ChunkDecodeStage::SizeMismatch { expected, actual }
                if expected == data.len() + 1 && actual == data.len()
        ),
        "{e:?}"
    );
    assert_eq!(decode(&raw, at, &engine, None, false).unwrap(), data);
}

/// Without an AEAD the chunk ID is always checked.
// Plaintext-only: 10-single-protocol step 3 removes this.
#[test]
fn decode_chunk_id_check_plaintext() {
    let data = b"payload stored under the wrong id";
    let wrong = ChunkId::from_bytes([0x55; 32]);
    let payload = compress::compress(Compression::None, data).unwrap();

    let engine = vykar_crypto::PlaintextEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
    let raw = chunk_blob(&wrong, &payload, &engine);
    let e = decode(&raw, location(wrong, &raw), &engine, None, false).unwrap_err();
    let actual_id = ChunkId::compute(engine.chunk_hasher(), data);
    assert!(
        matches!(e.stage, ChunkDecodeStage::IdMismatch { actual } if actual == actual_id),
        "{e:?}"
    );
}

/// Under an AEAD a blob sealed under the wrong ID authenticates: only the
/// verifying decode checks the ID.
#[test]
fn decode_chunk_id_check() {
    let data = b"payload stored under the wrong id";
    let wrong = ChunkId::from_bytes([0x55; 32]);
    let payload = compress::compress(Compression::None, data).unwrap();

    let engine = aes_engine();
    let raw = chunk_blob(&wrong, &payload, &engine);
    let at = location(wrong, &raw);
    assert_eq!(decode(&raw, at, &engine, None, false).unwrap(), data);
    let e = decode(&raw, at, &engine, None, true).unwrap_err();
    let actual_id = ChunkId::compute(engine.chunk_hasher(), data);
    assert!(
        matches!(e.stage, ChunkDecodeStage::IdMismatch { actual } if actual == actual_id),
        "{e:?}"
    );
}

#[test]
fn chunk_decode_error_conversions() {
    let id = ChunkId::from_bytes([0x66; 32]);
    let pack = PackId::from_bytes([0x07; 32]);
    let at = BlobLocation {
        chunk_id: id,
        pack_id: pack,
        pack_offset: 7,
        stored_size: 42,
    };
    let decrypt = || ChunkDecodeError {
        stage: ChunkDecodeStage::Decrypt(Box::new(VykarError::DecryptionFailed)),
        at,
    };

    assert!(matches!(
        VykarError::from(decrypt()),
        VykarError::DecryptionFailed
    ));
    match decrypt().into_located() {
        e @ VykarError::ChunkDecode(_) => assert_eq!(
            e.to_string(),
            format!(
                "chunk {id} in pack {pack} (offset 7, size 42): \
                 decryption failed: wrong passphrase or corrupted data"
            )
        ),
        other => panic!("{other:?}"),
    }
    let located_id_mismatch = ChunkDecodeError {
        stage: ChunkDecodeStage::IdMismatch {
            actual: ChunkId::from_bytes([0x77; 32]),
        },
        at,
    }
    .into_located();
    match located_id_mismatch {
        e @ VykarError::ChunkDecode(_) => assert_eq!(
            e.to_string(),
            format!(
                "invalid repository format: chunk {id} in pack {pack} (offset 7, size 42) \
                 hash mismatch after decode: expected {id}, got {}",
                ChunkId::from_bytes([0x77; 32])
            )
        ),
        other => panic!("{other:?}"),
    }

    let actual = ChunkId::from_bytes([0x77; 32]);
    let id_mismatch = ChunkDecodeError {
        stage: ChunkDecodeStage::IdMismatch { actual },
        at,
    };
    match VykarError::from(id_mismatch) {
        VykarError::InvalidFormat(m) => assert_eq!(
            m,
            format!(
                "chunk {id} in pack {pack} (offset 7, size 42) \
                 hash mismatch after decode: expected {id}, got {actual}"
            )
        ),
        other => panic!("{other:?}"),
    }

    let size_mismatch = ChunkDecodeError {
        stage: ChunkDecodeStage::SizeMismatch {
            expected: 10,
            actual: 9,
        },
        at,
    };
    match VykarError::from(size_mismatch) {
        VykarError::InvalidFormat(m) => {
            assert!(m.contains("size mismatch after restore decode"), "{m}")
        }
        other => panic!("{other:?}"),
    }
}
