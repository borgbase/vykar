use crate::compress;
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

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
    /// Segmented index head at `index` (formats v4/v5).
    IndexHead = 9,
    /// Segmented index base under `index.d/`.
    IndexBase = 10,
    /// Segmented index segment under `index.d/`.
    IndexSegment = 11,
    /// Pack trailer: the last framed blob of a pack, listing its blobs.
    PackTrailer = 12,
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
            9 => Ok(Self::IndexHead),
            10 => Ok(Self::IndexBase),
            11 => Ok(Self::IndexSegment),
            12 => Ok(Self::PackTrailer),
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
    let (tag, payload) = data
        .split_first()
        .ok_or_else(|| VykarError::InvalidFormat("empty object".into()))?;
    let obj_type = ObjectType::from_u8(*tag)?;
    Ok((*tag, obj_type, payload))
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
        // Encrypt the plaintext region in-place. The buffer was primed
        // with `push(tag)` + `extend_from_slice(&[0u8; 12])`, so
        // `buf.len() >= 13` and `buf[plaintext_start..]` / `buf[1..13]`
        // are in bounds.
        let plaintext_start = 1 + 12;
        #[allow(clippy::indexing_slicing)]
        let (nonce, gcm_tag) =
            crypto.encrypt_in_place_detached(&mut buf[plaintext_start..], aad)?;
        // Fill in the nonce
        #[allow(clippy::indexing_slicing)]
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

/// Context-bound variant that decrypts in place, returning the plaintext as a
/// subslice of `data`. On error `data` holds unspecified bytes.
pub fn unpack_object_expect_with_context_in_place<'a>(
    data: &'a mut [u8],
    expected_type: ObjectType,
    context: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<&'a mut [u8]> {
    let (tag, payload) = data
        .split_first_mut()
        .ok_or_else(|| VykarError::InvalidFormat("empty object".into()))?;
    let obj_type = ObjectType::from_u8(*tag)?;
    let aad = contextual_aad(*tag, context);
    // Always run AEAD before checking type (must decrypt to authenticate).
    let plaintext = crypto.decrypt_in_place(payload, &aad)?;
    if obj_type != expected_type {
        return Err(VykarError::InvalidFormat(format!(
            "unexpected object type: expected {:?}, got {:?}",
            expected_type, obj_type
        )));
    }
    Ok(plaintext)
}

/// Where a stored chunk blob sits, and the ID it is stored under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobLocation {
    pub chunk_id: ChunkId,
    pub pack_id: PackId,
    pub pack_offset: u64,
    pub stored_size: u32,
}

/// The decode step that failed, with its cause.
#[derive(Debug)]
pub enum ChunkDecodeStage {
    /// Envelope, AEAD or object-type check. Boxed to keep the error under
    /// `clippy::result_large_err`.
    Decrypt(Box<VykarError>),
    Decompress(Box<VykarError>),
    /// Only with a size hint: the decoded length differs from it.
    SizeMismatch {
        expected: usize,
        actual: usize,
    },
    /// The recomputed chunk ID differs from `BlobLocation::chunk_id`.
    IdMismatch {
        actual: ChunkId,
    },
}

#[derive(Debug)]
pub struct ChunkDecodeError {
    pub stage: ChunkDecodeStage,
    pub at: BlobLocation,
}

/// Decode one stored chunk blob into `out`: decrypt `raw` in place, one-shot
/// decompress, check the length against `hint` when one is given, and check
/// the chunk ID when `verify` is set or the engine does not encrypt (without
/// an AEAD tag the keyed chunk ID is the only checksum).
///
/// `raw` is overwritten whether this succeeds or fails, so it must not be
/// decoded again: callers that decode several ranges of one shared buffer
/// copy each blob out first (check, the lazy item stream); the coalesced
/// readers in `repo::blob_read` read each blob once and decode it in place.
/// `out` is cleared and filled; on error its contents are unspecified.
/// `hint` is also the ZSTD output size, see
/// [`compress::decompress_into_with_hint`].
pub fn decode_chunk_into(
    raw: &mut [u8],
    at: BlobLocation,
    crypto: &dyn CryptoEngine,
    hint: Option<usize>,
    verify: bool,
    out: &mut Vec<u8>,
) -> std::result::Result<(), ChunkDecodeError> {
    let fail = move |stage| ChunkDecodeError { stage, at };
    let compressed = unpack_object_expect_with_context_in_place(
        raw,
        ObjectType::ChunkData,
        at.chunk_id.as_bytes(),
        crypto,
    )
    .map_err(|e| fail(ChunkDecodeStage::Decrypt(Box::new(e))))?;
    compress::decompress_into_with_hint(compressed, hint, out)
        .map_err(|e| fail(ChunkDecodeStage::Decompress(Box::new(e))))?;
    if let Some(expected) = hint
        && out.len() != expected
    {
        return Err(fail(ChunkDecodeStage::SizeMismatch {
            expected,
            actual: out.len(),
        }));
    }
    if verify || !crypto.is_encrypting() {
        let actual = ChunkId::compute(crypto.chunk_hasher(), out);
        if actual != at.chunk_id {
            return Err(fail(ChunkDecodeStage::IdMismatch { actual }));
        }
    }
    Ok(())
}

impl ChunkDecodeError {
    /// The error with the blob's location in every message (restore's form).
    pub fn into_located(self) -> VykarError {
        let at = self.at;
        match self.stage {
            ChunkDecodeStage::Decrypt(e) | ChunkDecodeStage::Decompress(e) => {
                VykarError::Other(format!(
                    "chunk {} in pack {} (offset {}, size {}): {e}",
                    at.chunk_id, at.pack_id, at.pack_offset, at.stored_size
                ))
            }
            ChunkDecodeStage::SizeMismatch { expected, actual } => {
                VykarError::InvalidFormat(format!(
                    "chunk {} in pack {} (offset {}, size {}) size mismatch after restore decode: \
                     expected {} bytes, got {} bytes",
                    at.chunk_id, at.pack_id, at.pack_offset, at.stored_size, expected, actual
                ))
            }
            ChunkDecodeStage::IdMismatch { actual } => {
                let expected = at.chunk_id;
                VykarError::InvalidFormat(format!(
                    "chunk {expected} in pack {} (offset {}, size {}) \
                     hash mismatch after decode: expected {expected}, got {actual}",
                    at.pack_id, at.pack_offset, at.stored_size
                ))
            }
        }
    }
}

/// Decrypt and decompress failures keep their original error, as the read
/// paths have always returned them; size and ID mismatches carry the location.
impl From<ChunkDecodeError> for VykarError {
    fn from(e: ChunkDecodeError) -> Self {
        match e.stage {
            ChunkDecodeStage::Decrypt(inner) | ChunkDecodeStage::Decompress(inner) => *inner,
            stage => ChunkDecodeError { stage, at: e.at }.into_located(),
        }
    }
}
