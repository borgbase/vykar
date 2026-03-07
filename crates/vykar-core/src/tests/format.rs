use crate::repo::format::{
    pack_object, pack_object_streaming, pack_object_with_context, unpack_object,
    unpack_object_expect, unpack_object_expect_with_context, unpack_object_with_context,
    ObjectType,
};
use vykar_crypto::aes_gcm::Aes256GcmEngine;
use vykar_crypto::PlaintextEngine;
use vykar_types::error::VykarError;

#[test]
fn roundtrip_plaintext() {
    let engine = PlaintextEngine::new(&[0xAA; 32]);
    let data = b"manifest data here";
    let packed = pack_object(ObjectType::Manifest, data, &engine).unwrap();
    let (obj_type, unpacked) = unpack_object(&packed, &engine).unwrap();
    assert_eq!(obj_type, ObjectType::Manifest);
    assert_eq!(unpacked, data);
}

#[test]
fn roundtrip_encrypted() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], &[0x22; 32]);
    let data = b"secret chunk data";
    let packed = pack_object(ObjectType::ChunkData, data, &engine).unwrap();
    let (obj_type, unpacked) = unpack_object(&packed, &engine).unwrap();
    assert_eq!(obj_type, ObjectType::ChunkData);
    assert_eq!(unpacked, data);
}

#[test]
fn type_tag_is_first_byte() {
    let engine = PlaintextEngine::new(&[0xAA; 32]);
    let packed = pack_object(ObjectType::Manifest, b"data", &engine).unwrap();
    assert_eq!(packed[0], ObjectType::Manifest as u8);

    let packed2 = pack_object(ObjectType::ChunkData, b"data", &engine).unwrap();
    assert_eq!(packed2[0], ObjectType::ChunkData as u8);
}

#[test]
fn wrong_type_tag_encrypted_fails_aad() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], &[0x22; 32]);
    let data = b"secret";
    let mut packed = pack_object(ObjectType::Manifest, data, &engine).unwrap();
    // Change the type tag byte — AAD mismatch should cause decryption failure
    packed[0] = ObjectType::ChunkData as u8;
    let result = unpack_object(&packed, &engine);
    assert!(result.is_err());
}

#[test]
fn empty_data_fails() {
    let engine = PlaintextEngine::new(&[0xAA; 32]);
    let result = unpack_object(b"", &engine);
    assert!(result.is_err());
    match result.unwrap_err() {
        VykarError::InvalidFormat(msg) => assert_eq!(msg, "empty object"),
        other => panic!("expected InvalidFormat, got: {other}"),
    }
}

#[test]
fn unknown_type_tag_fails() {
    let engine = PlaintextEngine::new(&[0xAA; 32]);
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
}

#[test]
fn object_type_from_u8_invalid() {
    assert!(ObjectType::from_u8(9).is_err());
    assert!(ObjectType::from_u8(255).is_err());
}

#[test]
fn unpack_expect_rejects_wrong_object_type() {
    let engine = PlaintextEngine::new(&[0xAA; 32]);
    let packed = pack_object(ObjectType::Manifest, b"data", &engine).unwrap();

    let err = unpack_object_expect(&packed, ObjectType::ChunkData, &engine).unwrap_err();
    assert!(matches!(err, VykarError::InvalidFormat(_)));
    assert!(err.to_string().contains("unexpected object type"));
}

#[test]
fn pack_object_streaming_roundtrip_plaintext() {
    let engine = PlaintextEngine::new(&[0xAA; 32]);
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
    let engine = Aes256GcmEngine::new(&[0x11; 32], &[0x22; 32]);
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
    // For plaintext, streaming output should be byte-identical to pack_object
    let engine = PlaintextEngine::new(&[0xBB; 32]);
    let data = b"verify identical output";
    let packed_normal = pack_object(ObjectType::ChunkData, data, &engine).unwrap();
    let packed_streaming =
        pack_object_streaming(ObjectType::ChunkData, data.len(), &engine, |buf| {
            buf.extend_from_slice(data);
            Ok(())
        })
        .unwrap();
    assert_eq!(packed_normal, packed_streaming);
}

#[test]
fn context_bound_roundtrip_encrypted() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], &[0x22; 32]);
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
    let engine = Aes256GcmEngine::new(&[0x11; 32], &[0x22; 32]);
    let packed =
        pack_object_with_context(ObjectType::ChunkData, b"chunk-a", b"ciphertext", &engine)
            .unwrap();
    let err = unpack_object_with_context(&packed, b"chunk-b", &engine).unwrap_err();
    assert!(matches!(err, VykarError::DecryptionFailed));
}

#[test]
fn context_bound_unpack_rejects_legacy_object() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], &[0x22; 32]);
    let packed = pack_object(ObjectType::Manifest, b"legacy-manifest", &engine).unwrap();
    let err =
        unpack_object_expect_with_context(&packed, ObjectType::Manifest, b"manifest", &engine)
            .unwrap_err();
    assert!(matches!(err, VykarError::DecryptionFailed));
}
