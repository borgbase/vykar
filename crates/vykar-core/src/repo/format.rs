use vykar_crypto::CryptoEngine;
use vykar_types::error::{Result, VykarError};

/// Domain-separation marker for object identity binding in AEAD AAD.
// Wire-format constant — DO NOT rename (backward compatibility)
const OBJECT_CONTEXT_AAD_PREFIX: &[u8] = b"vger:object-context:v1\0";

/// Object type tags for the repo envelope format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjectType {
    Config = 0,
    Manifest = 1,
    SnapshotMeta = 2,
    ChunkData = 3,
    ChunkIndex = 4,
    PackHeader = 5,
    FileCache = 6,
    PendingIndex = 7,
    SnapshotCache = 8,
}

impl ObjectType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Self::Config),
            1 => Ok(Self::Manifest),
            2 => Ok(Self::SnapshotMeta),
            3 => Ok(Self::ChunkData),
            4 => Ok(Self::ChunkIndex),
            5 => Ok(Self::PackHeader),
            6 => Ok(Self::FileCache),
            7 => Ok(Self::PendingIndex),
            8 => Ok(Self::SnapshotCache),
            _ => Err(VykarError::UnknownObjectType(v)),
        }
    }
}

fn legacy_aad(tag: u8) -> [u8; 1] {
    [tag]
}

fn contextual_aad(tag: u8, context: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(1 + OBJECT_CONTEXT_AAD_PREFIX.len() + context.len());
    aad.push(tag);
    aad.extend_from_slice(OBJECT_CONTEXT_AAD_PREFIX);
    aad.extend_from_slice(context);
    aad
}

fn parse_object_envelope(data: &[u8]) -> Result<(u8, ObjectType, &[u8])> {
    if data.is_empty() {
        return Err(VykarError::InvalidFormat("empty object".into()));
    }
    let tag = data[0];
    let obj_type = ObjectType::from_u8(tag)?;
    Ok((tag, obj_type, &data[1..]))
}

/// Serialize a typed payload into an encrypted repo object.
///
/// Wire format (encrypted): `[1-byte type_tag][encrypted_blob]`
///   where encrypted_blob = `[12-byte nonce][ciphertext + 16-byte GCM tag]`
///
/// Wire format (plaintext): `[1-byte type_tag][plaintext]`
pub fn pack_object(
    obj_type: ObjectType,
    plaintext: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<Vec<u8>> {
    let tag = obj_type as u8;
    let aad = legacy_aad(tag); // authenticate the type tag
    let encrypted = crypto.encrypt(plaintext, &aad)?;

    let mut out = Vec::with_capacity(1 + encrypted.len());
    out.push(tag);
    out.extend_from_slice(&encrypted);
    Ok(out)
}

/// Serialize a typed payload into an encrypted repo object and bind it to an
/// object identity context.
///
/// Decryption should use `unpack_object_with_context` /
/// `unpack_object_expect_with_context`.
pub fn pack_object_with_context(
    obj_type: ObjectType,
    context: &[u8],
    plaintext: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<Vec<u8>> {
    let tag = obj_type as u8;
    let aad = contextual_aad(tag, context);
    let encrypted = crypto.encrypt(plaintext, &aad)?;

    let mut out = Vec::with_capacity(1 + encrypted.len());
    out.push(tag);
    out.extend_from_slice(&encrypted);
    Ok(out)
}

/// Serialize a typed payload via streaming into an encrypted repo object.
///
/// Like `pack_object`, but avoids allocating separate plaintext + ciphertext
/// buffers.  The `write_plaintext` callback writes serialized data directly
/// into the output buffer, which is then encrypted in-place (for encrypting
/// engines) or left as-is (for `PlaintextEngine`).
///
/// Wire format is identical to `pack_object` — `unpack_object` / `unpack_object_expect`
/// can read the result.
pub fn pack_object_streaming<F>(
    obj_type: ObjectType,
    estimated_plaintext_size: usize,
    crypto: &dyn CryptoEngine,
    write_plaintext: F,
) -> Result<Vec<u8>>
where
    F: FnOnce(&mut Vec<u8>) -> Result<()>,
{
    let tag = obj_type as u8;
    let aad = legacy_aad(tag);
    pack_object_streaming_inner(tag, &aad, estimated_plaintext_size, crypto, write_plaintext)
}

/// Streaming variant of `pack_object_with_context`.
pub fn pack_object_streaming_with_context<F>(
    obj_type: ObjectType,
    context: &[u8],
    estimated_plaintext_size: usize,
    crypto: &dyn CryptoEngine,
    write_plaintext: F,
) -> Result<Vec<u8>>
where
    F: FnOnce(&mut Vec<u8>) -> Result<()>,
{
    let tag = obj_type as u8;
    let aad = contextual_aad(tag, context);
    pack_object_streaming_inner(tag, &aad, estimated_plaintext_size, crypto, write_plaintext)
}

fn pack_object_streaming_inner<F>(
    tag: u8,
    aad: &[u8],
    estimated_plaintext_size: usize,
    crypto: &dyn CryptoEngine,
    write_plaintext: F,
) -> Result<Vec<u8>>
where
    F: FnOnce(&mut Vec<u8>) -> Result<()>,
{
    if crypto.is_encrypting() {
        // Layout: [tag][nonce 12][plaintext → ciphertext][tag 16]
        let mut buf = Vec::with_capacity(1 + 12 + estimated_plaintext_size + 16);
        buf.push(tag);
        // Reserve 12 bytes for the nonce (filled after encryption)
        buf.extend_from_slice(&[0u8; 12]);
        // Let the caller write plaintext starting at offset 13
        write_plaintext(&mut buf)?;
        // Encrypt the plaintext region in-place
        let plaintext_start = 1 + 12; // after tag + nonce placeholder
        let (nonce, gcm_tag) =
            crypto.encrypt_in_place_detached(&mut buf[plaintext_start..], aad)?;
        // Fill in the nonce
        buf[1..13].copy_from_slice(&nonce);
        // Append the authentication tag
        buf.extend_from_slice(&gcm_tag);
        Ok(buf)
    } else {
        // Plaintext engine: [tag][plaintext]
        let mut buf = Vec::with_capacity(1 + estimated_plaintext_size);
        buf.push(tag);
        write_plaintext(&mut buf)?;
        Ok(buf)
    }
}

/// Deserialize and decrypt a repo object.
/// Returns `(object_type, plaintext)`.
pub fn unpack_object(data: &[u8], crypto: &dyn CryptoEngine) -> Result<(ObjectType, Vec<u8>)> {
    let (tag, obj_type, encrypted) = parse_object_envelope(data)?;
    let aad = legacy_aad(tag);
    let plaintext = crypto.decrypt(encrypted, &aad)?;
    Ok((obj_type, plaintext))
}

/// Deserialize and decrypt a repo object bound to `context`.
pub fn unpack_object_with_context(
    data: &[u8],
    context: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<(ObjectType, Vec<u8>)> {
    let (tag, obj_type, encrypted) = parse_object_envelope(data)?;
    let aad = contextual_aad(tag, context);
    let plaintext = crypto.decrypt(encrypted, &aad)?;
    Ok((obj_type, plaintext))
}

/// Deserialize and decrypt a repo object, ensuring its type tag matches.
pub fn unpack_object_expect(
    data: &[u8],
    expected_type: ObjectType,
    crypto: &dyn CryptoEngine,
) -> Result<Vec<u8>> {
    let (obj_type, plaintext) = unpack_object(data, crypto)?;
    if obj_type != expected_type {
        return Err(VykarError::InvalidFormat(format!(
            "unexpected object type: expected {:?}, got {:?}",
            expected_type, obj_type
        )));
    }
    Ok(plaintext)
}

/// Context-bound variant of `unpack_object_expect`.
pub fn unpack_object_expect_with_context(
    data: &[u8],
    expected_type: ObjectType,
    context: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<Vec<u8>> {
    let (obj_type, plaintext) = unpack_object_with_context(data, context, crypto)?;
    if obj_type != expected_type {
        return Err(VykarError::InvalidFormat(format!(
            "unexpected object type: expected {:?}, got {:?}",
            expected_type, obj_type
        )));
    }
    Ok(plaintext)
}

/// Context-bound variant that decrypts into a caller-provided buffer.
/// Reduces allocation churn when called repeatedly in a hot loop.
pub fn unpack_object_expect_with_context_into(
    data: &[u8],
    expected_type: ObjectType,
    context: &[u8],
    crypto: &dyn CryptoEngine,
    output: &mut Vec<u8>,
) -> Result<()> {
    let (tag, obj_type, encrypted) = parse_object_envelope(data)?;
    let aad = contextual_aad(tag, context);
    // Always run AEAD before checking type (must decrypt to authenticate).
    crypto.decrypt_into(encrypted, &aad, output)?;
    if obj_type != expected_type {
        output.clear();
        return Err(VykarError::InvalidFormat(format!(
            "unexpected object type: expected {:?}, got {:?}",
            expected_type, obj_type
        )));
    }
    Ok(())
}
