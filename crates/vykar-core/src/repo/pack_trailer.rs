//! Pack trailers: the last framed blob of a pack, listing the pack's blobs.
//!
//! Layout (architecture.md, "Pack File Format"):
//!
//! ~~~text
//! pack         = [magic "VGERPACK"][ver=1] { [u32 LE len][blob] }* [u32 LE L][trailer blob]
//! trailer blob = object ++ footer                                    (L bytes in total)
//! object       = envelope(ObjectType::PackTrailer, context b"pack-trailer", tagged_body)
//! tagged_body  = [codec tag u8][body]   (written uncompressed)
//! body         = [version u8 = 1][pack_type u8][count u32 LE] then count x entry
//! entry        = [chunk_id 32][offset u32 LE][stored_size u32 LE][kind u8]
//! footer       = [L u32 LE][b"VYKTRLR1"]
//! ~~~
//!
//! The trailer is an ordinary length-framed blob, so every reader that scans
//! frames or reads indexed ranges ignores it; the pack ID covers it. Its
//! length is a function of the entry count and of whether the repository
//! encrypts ([`trailer_blob_len`]), so writers can size it before building it.

use vykar_crypto::CryptoEngine;
use vykar_protocol::{PACK_HEADER_SIZE, validate_pack_header};
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use crate::compress::{self, Compression};
use crate::repo::format::{
    ObjectType, pack_object_with_context, unpack_object_expect_with_context,
};

/// Last 8 bytes of a pack that ends in a trailer.
pub const TRAILER_MAGIC: &[u8; 8] = b"VYKTRLR1";
/// `[L u32 LE][TRAILER_MAGIC]`.
pub const TRAILER_FOOTER_LEN: usize = 12;
/// AEAD object context of a trailer.
// Wire-format constant — DO NOT rename.
pub const TRAILER_CONTEXT: &[u8] = b"pack-trailer";
/// The body version this binary writes and reads.
pub const TRAILER_BODY_VERSION: u8 = 1;
/// Bytes per entry in the body.
pub const TRAILER_ENTRY_LEN: usize = 41;
/// `[version][pack_type][count u32]`.
const BODY_HEADER_LEN: usize = 6;
/// Reader bound: ~16 MiB of entries, the largest trailer a server repack
/// carries ([`vykar_protocol::MAX_REPACK_TRAILER_BYTES`]). Writers stay at
/// [`crate::repo::pack::MAX_BLOBS_PER_PACK`] per backup pack.
pub const MAX_TRAILER_ENTRIES: usize = 400_000;
/// One tail read finds the trailer of a pack with up to ~1 500 blobs.
pub const TRAILER_TAIL_READ: u64 = 64 * 1024;

/// Smallest `L` a footer may claim: the footer plus the object's tag.
const MIN_TRAILER_BLOB_LEN: u32 = 13;
/// AEAD overhead of an encrypting engine: 12-byte nonce + 16-byte tag.
const AEAD_OVERHEAD: usize = 28;
/// Envelope tag byte plus codec tag byte.
const TAG_BYTES: usize = 2;
/// Lowest valid blob offset: the pack header plus one length prefix.
const FIRST_BLOB_OFFSET: u64 = PACK_HEADER_SIZE as u64 + 4;

/// What the trailer says the pack holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TrailerPackType {
    /// Backup data pack (file content).
    Data = 0,
    /// Backup tree pack (item-stream metadata).
    Tree = 1,
    /// Compact output; the source packs' types are not known.
    Repacked = 2,
}

impl TrailerPackType {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Self::Data),
            1 => Ok(Self::Tree),
            2 => Ok(Self::Repacked),
            _ => Err(VykarError::InvalidFormat(format!(
                "pack trailer: unknown pack type {v}"
            ))),
        }
    }
}

/// What a listed blob is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TrailerBlobKind {
    /// An `ObjectType::ChunkData` blob.
    Chunk = 0,
}

impl TrailerBlobKind {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Self::Chunk),
            _ => Err(VykarError::InvalidFormat(format!(
                "pack trailer: unknown blob kind {v}"
            ))),
        }
    }
}

/// One blob of the pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrailerEntry {
    pub chunk_id: ChunkId,
    /// Offset of the blob's first byte (after its length prefix).
    pub offset: u32,
    pub stored_size: u32,
    pub kind: TrailerBlobKind,
}

/// A decoded pack trailer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackTrailer {
    pub pack_type: TrailerPackType,
    pub entries: Vec<TrailerEntry>,
}

/// Offsets of blobs written in order after the pack header, each framed:
/// `PACK_HEADER_SIZE + 4` for the first, then `+ 4 + len` per blob. The
/// layout `PackWriter::seal` and the server's repack both produce.
pub fn framed_offsets(lengths: impl IntoIterator<Item = u32>) -> impl Iterator<Item = u64> {
    let mut next = FIRST_BLOB_OFFSET;
    lengths.into_iter().map(move |len| {
        let offset = next;
        next = next.saturating_add(4).saturating_add(u64::from(len));
        offset
    })
}

/// Exact length `L` of a sealed trailer blob with this many entries.
pub const fn trailer_blob_len(entries: usize, encrypting: bool) -> usize {
    let aead = if encrypting { AEAD_OVERHEAD } else { 0 };
    TAG_BYTES + aead + BODY_HEADER_LEN + TRAILER_ENTRY_LEN * entries + TRAILER_FOOTER_LEN
}

/// `4 + trailer_blob_len(entries, true)`: the frame a heap pack buffer
/// reserves before the engine is known.
pub const fn trailer_frame_len_upper(entries: usize) -> usize {
    4 + trailer_blob_len(entries, true)
}

fn invalid(msg: impl Into<String>) -> VykarError {
    VykarError::InvalidFormat(format!("pack trailer: {}", msg.into()))
}

fn u32_le(bytes: &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?))
}

impl PackTrailer {
    /// The trailer of a pack written as the header followed by the given
    /// blobs in order (offsets from [`framed_offsets`]). Err
    /// (`InvalidFormat`) when there are no blobs, more than
    /// [`MAX_TRAILER_ENTRIES`], or an offset does not fit u32.
    pub fn for_blobs(
        pack_type: TrailerPackType,
        blobs: impl IntoIterator<Item = (ChunkId, u32)>,
    ) -> Result<Self> {
        let mut entries = Vec::new();
        let mut next = FIRST_BLOB_OFFSET;
        for (chunk_id, stored_size) in blobs {
            let offset = u32::try_from(next)
                .map_err(|_| invalid(format!("blob offset {next} does not fit u32")))?;
            entries.push(TrailerEntry {
                chunk_id,
                offset,
                stored_size,
                kind: TrailerBlobKind::Chunk,
            });
            next = next
                .saturating_add(4)
                .saturating_add(u64::from(stored_size));
        }
        if entries.is_empty() {
            return Err(invalid("no blobs"));
        }
        if entries.len() > MAX_TRAILER_ENTRIES {
            return Err(invalid(format!(
                "{} blobs (max {MAX_TRAILER_ENTRIES})",
                entries.len()
            )));
        }
        Ok(Self { pack_type, entries })
    }

    /// Offsets of the entries, widened (what the index stores).
    pub fn offsets(&self) -> impl Iterator<Item = u64> + '_ {
        self.entries.iter().map(|e| u64::from(e.offset))
    }

    /// The body, uncompressed. Canonical: [`Self::decode_body`] accepts
    /// exactly one encoding of each trailer.
    pub fn encode_body(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(BODY_HEADER_LEN + TRAILER_ENTRY_LEN * self.entries.len());
        out.push(TRAILER_BODY_VERSION);
        out.push(self.pack_type as u8);
        // Entry counts come from `for_blobs` or `decode_body`, both bounded
        // by MAX_TRAILER_ENTRIES.
        let count = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&count.to_le_bytes());
        for e in &self.entries {
            out.extend_from_slice(e.chunk_id.as_bytes());
            out.extend_from_slice(&e.offset.to_le_bytes());
            out.extend_from_slice(&e.stored_size.to_le_bytes());
            out.push(e.kind as u8);
        }
        out
    }

    /// Strict decode. A body version other than [`TRAILER_BODY_VERSION`] is
    /// `UnsupportedVersion`; every other violation (unknown pack type or
    /// kind, a count of 0 or over [`MAX_TRAILER_ENTRIES`], a length that
    /// differs from the count, offsets that are not increasing, overlapping
    /// blobs, a blob inside the pack header) is `InvalidFormat`. For every
    /// accepted body `b`, `encode_body(decode_body(b)) == b`.
    pub fn decode_body(body: &[u8]) -> Result<Self> {
        let (&version, rest) = body.split_first().ok_or_else(|| invalid("empty body"))?;
        if version != TRAILER_BODY_VERSION {
            return Err(VykarError::UnsupportedVersion(u32::from(version)));
        }
        let (&pack_type, rest) = rest.split_first().ok_or_else(|| invalid("short body"))?;
        let pack_type = TrailerPackType::from_u8(pack_type)?;
        let (count, rest) = rest
            .split_at_checked(4)
            .ok_or_else(|| invalid("short body"))?;
        let count = u32_le(count).ok_or_else(|| invalid("short body"))? as usize;
        if count == 0 || count > MAX_TRAILER_ENTRIES {
            return Err(invalid(format!(
                "entry count {count} outside 1..={MAX_TRAILER_ENTRIES}"
            )));
        }
        if rest.len() != count * TRAILER_ENTRY_LEN {
            return Err(invalid(format!(
                "body of {} bytes for {count} entries",
                body.len()
            )));
        }
        let mut entries = Vec::with_capacity(count);
        // End of the previous blob, plus its successor's length prefix.
        let mut min_offset = FIRST_BLOB_OFFSET;
        // `rest` is exactly `count` entries long (checked above).
        let (raw_entries, _) = rest.as_chunks::<TRAILER_ENTRY_LEN>();
        for raw in raw_entries {
            let (id, raw) = raw
                .split_at_checked(32)
                .ok_or_else(|| invalid("short entry"))?;
            let (offset, raw) = raw
                .split_at_checked(4)
                .ok_or_else(|| invalid("short entry"))?;
            let (size, raw) = raw
                .split_at_checked(4)
                .ok_or_else(|| invalid("short entry"))?;
            let kind = raw.first().ok_or_else(|| invalid("short entry"))?;
            let chunk_id = ChunkId::from_bytes(id.try_into().map_err(|_| invalid("short entry"))?);
            let offset = u32_le(offset).ok_or_else(|| invalid("short entry"))?;
            let stored_size = u32_le(size).ok_or_else(|| invalid("short entry"))?;
            let kind = TrailerBlobKind::from_u8(*kind)?;
            if u64::from(offset) < min_offset {
                return Err(invalid(format!(
                    "blob at offset {offset} overlaps the header or the previous blob"
                )));
            }
            min_offset = u64::from(offset) + u64::from(stored_size) + 4;
            entries.push(TrailerEntry {
                chunk_id,
                offset,
                stored_size,
                kind,
            });
        }
        Ok(Self { pack_type, entries })
    }

    /// The trailer blob (object ++ footer): what follows the frame's u32.
    pub fn seal_blob(&self, crypto: &dyn CryptoEngine) -> Result<Vec<u8>> {
        let tagged = compress::compress(Compression::None, &self.encode_body())?;
        let mut blob =
            pack_object_with_context(ObjectType::PackTrailer, TRAILER_CONTEXT, &tagged, crypto)?;
        let len = u32::try_from(blob.len() + TRAILER_FOOTER_LEN)
            .map_err(|_| invalid("trailer does not fit u32"))?;
        blob.extend_from_slice(&len.to_le_bytes());
        blob.extend_from_slice(TRAILER_MAGIC);
        debug_assert_eq!(
            blob.len(),
            trailer_blob_len(self.entries.len(), crypto.is_encrypting())
        );
        Ok(blob)
    }

    /// Inverse of [`Self::seal_blob`]: footer check (magic, `L ==
    /// blob.len()`), decrypt under (tag 12, [`TRAILER_CONTEXT`]),
    /// decompress, [`Self::decode_body`]. Decryption errors pass through as
    /// the engine returns them.
    pub fn open_blob(blob: &[u8], crypto: &dyn CryptoEngine) -> Result<Self> {
        let split = blob
            .len()
            .checked_sub(TRAILER_FOOTER_LEN)
            .ok_or_else(|| invalid("shorter than its footer"))?;
        let (object, footer) = blob
            .split_at_checked(split)
            .ok_or_else(|| invalid("shorter than its footer"))?;
        let len = parse_footer(footer).ok_or_else(|| invalid("no footer"))?;
        if len as usize != blob.len() {
            return Err(invalid(format!(
                "footer says {len} bytes, blob has {}",
                blob.len()
            )));
        }
        let tagged = unpack_object_expect_with_context(
            object,
            ObjectType::PackTrailer,
            TRAILER_CONTEXT,
            crypto,
        )?;
        // Only a newer binary writes a codec this one does not know, so it is
        // refused like an unknown body version, not reported as damage.
        let body = compress::decompress(&tagged).map_err(|e| match e {
            VykarError::UnknownCompressionTag(tag) => {
                VykarError::UnsupportedVersion(u32::from(tag))
            }
            e => e,
        })?;
        Self::decode_body(&body)
    }

    /// Every entry lies inside `[PACK_HEADER_SIZE + 4, frame_start)`.
    pub fn check_within(&self, frame_start: u64) -> Result<()> {
        for e in &self.entries {
            let end = u64::from(e.offset) + u64::from(e.stored_size);
            if u64::from(e.offset) < FIRST_BLOB_OFFSET || end > frame_start {
                return Err(invalid(format!(
                    "blob at offset {} ({} bytes) lies outside the pack's blobs (end {frame_start})",
                    e.offset, e.stored_size
                )));
            }
        }
        Ok(())
    }
}

/// `L` from the last 12 bytes of a pack, when they are a trailer footer.
/// `None` when `last.len() < 12` or the magic differs.
pub fn parse_footer(last: &[u8]) -> Option<u32> {
    let start = last.len().checked_sub(TRAILER_FOOTER_LEN)?;
    let footer = last.get(start..)?;
    let (len, magic) = footer.split_at_checked(4)?;
    if magic != TRAILER_MAGIC {
        return None;
    }
    u32_le(len)
}

/// Start of the trailer frame of a `pack_len`-byte pack whose footer says
/// `L`: `pack_len - 4 - L`. `None` unless `L >= 13` and the frame starts
/// at or after `PACK_HEADER_SIZE`.
pub fn trailer_frame_start(pack_len: u64, trailer_blob_len: u32) -> Option<u64> {
    if trailer_blob_len < MIN_TRAILER_BLOB_LEN {
        return None;
    }
    let start = pack_len
        .checked_sub(4)?
        .checked_sub(u64::from(trailer_blob_len))?;
    (start >= PACK_HEADER_SIZE as u64).then_some(start)
}

/// Open a whole frame (`[u32 LE L][trailer blob]`): the prefix must equal
/// `frame.len() - 4` and the footer's `L`; then [`PackTrailer::open_blob`].
pub fn open_trailer_frame(frame: &[u8], crypto: &dyn CryptoEngine) -> Result<PackTrailer> {
    let (prefix, blob) = frame
        .split_at_checked(4)
        .ok_or_else(|| invalid("frame shorter than its length prefix"))?;
    let prefix = u32_le(prefix).ok_or_else(|| invalid("frame shorter than its length prefix"))?;
    if prefix as usize != blob.len() {
        return Err(invalid(format!(
            "frame prefix says {prefix} bytes, frame holds {}",
            blob.len()
        )));
    }
    PackTrailer::open_blob(blob, crypto)
}

/// The trailer of a whole pack in memory. `Ok(None)` when the last 12 bytes
/// are not a footer (a pack from before this format); `Err` when the pack
/// header is invalid, or a footer is present but the frame, the AEAD, the
/// body or the layout ([`PackTrailer::check_within`]) is wrong.
pub fn read_trailer_from_pack_bytes(
    pack: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<Option<PackTrailer>> {
    validate_pack_header(pack).map_err(VykarError::InvalidFormat)?;
    let Some(len) = parse_footer(pack) else {
        return Ok(None);
    };
    let pack_len = pack.len() as u64;
    let start = trailer_frame_start(pack_len, len)
        .ok_or_else(|| invalid(format!("footer length {len} does not fit the pack")))?;
    let frame = usize::try_from(start)
        .ok()
        .and_then(|s| pack.get(s..))
        .ok_or_else(|| invalid("frame start outside the pack"))?;
    let trailer = open_trailer_frame(frame, crypto)?;
    trailer.check_within(start)?;
    Ok(Some(trailer))
}

/// Outcome of [`read_pack_trailer`]. Each case a caller must tell apart has
/// its own variant, so none depends on an error message.
#[derive(Debug)]
pub enum TrailerRead {
    /// The pack ends in a valid trailer.
    Found(PackTrailer),
    /// The pack exists but its last 12 bytes are not a trailer footer (a pack
    /// from before trailers).
    Absent,
    /// The storage has no object at the key.
    PackMissing,
    /// A footer is present, but the frame, the AEAD, the body or the layout
    /// is wrong. A body of an unknown version or codec is
    /// `Damaged(VykarError::UnsupportedVersion(_))`: written by a newer
    /// binary, not broken; callers must refuse rather than repair it. (In a
    /// plaintext repository rot can produce it too: the index rebuild tells
    /// the two apart by hashing the pack against its ID.)
    ///
    /// Only the last 12 bytes decide between this and `Absent`. In a
    /// plaintext repository a pack from before trailers whose last blob ends
    /// in bytes shaped like a footer reads as `Damaged`, so this variant
    /// alone is not grounds for repairing or distrusting a pack.
    Damaged(VykarError),
}

/// The trailer of a pack in storage: one range read of the last
/// `min(TRAILER_TAIL_READ, pack_len)` bytes, and a second read of
/// `[frame_start, pack_len)` when the frame is longer. `pack_len` comes
/// from the caller's listing. The pack header is not read.
///
/// `Err` only for a failed request or a read shorter than asked (the pack
/// is not `pack_len` bytes long); every answer about the pack itself is a
/// [`TrailerRead`] variant.
pub fn read_pack_trailer(
    storage: &dyn StorageBackend,
    pack_key: &str,
    pack_len: u64,
    crypto: &dyn CryptoEngine,
) -> Result<TrailerRead> {
    // Too short for a footer: no request needed. The caller has the length
    // from a listing, so the pack exists.
    if pack_len < TRAILER_FOOTER_LEN as u64 {
        return Ok(TrailerRead::Absent);
    }
    let tail_len = TRAILER_TAIL_READ.min(pack_len);
    let tail_start = pack_len - tail_len;
    let Some(tail) = read_exact(storage, pack_key, tail_start, tail_len)? else {
        return Ok(TrailerRead::PackMissing);
    };
    let Some(len) = parse_footer(&tail) else {
        return Ok(TrailerRead::Absent);
    };
    let Some(start) = trailer_frame_start(pack_len, len) else {
        return Ok(TrailerRead::Damaged(invalid(format!(
            "footer length {len} does not fit a pack of {pack_len} bytes"
        ))));
    };
    let opened = if start >= tail_start {
        let skip = usize::try_from(start - tail_start).unwrap_or(usize::MAX);
        let frame = tail.get(skip..).unwrap_or_default();
        open_trailer_frame(frame, crypto)
    } else {
        let Some(frame) = read_exact(storage, pack_key, start, pack_len - start)? else {
            return Ok(TrailerRead::PackMissing);
        };
        open_trailer_frame(&frame, crypto)
    };
    Ok(
        match opened.and_then(|t| t.check_within(start).map(|()| t)) {
            Ok(t) => TrailerRead::Found(t),
            Err(e) => TrailerRead::Damaged(e),
        },
    )
}

/// A range read that must return exactly `len` bytes; `None` when the key
/// does not exist.
fn read_exact(
    storage: &dyn StorageBackend,
    key: &str,
    offset: u64,
    len: u64,
) -> Result<Option<Vec<u8>>> {
    let Some(data) = storage.get_range(key, offset, len)? else {
        return Ok(None);
    };
    if data.len() as u64 != len {
        return Err(VykarError::Other(format!(
            "short read from {key} at offset {offset}: expected {len} bytes, got {}",
            data.len()
        )));
    }
    Ok(Some(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::pack::{PackType, PackWriter};
    use crate::testutil::{RecordingBackend, chunk_hasher_for};
    use vykar_crypto::PlaintextEngine;
    use vykar_crypto::aes_gcm::Aes256GcmEngine;
    use vykar_crypto::chacha20_poly1305::ChaCha20Poly1305Engine;
    use vykar_types::hash::HashAlgorithm;

    fn plain() -> PlaintextEngine {
        PlaintextEngine::new(chunk_hasher_for([0xAA; 32]))
    }

    fn aes() -> Aes256GcmEngine {
        Aes256GcmEngine::new(&[0x11; 32], chunk_hasher_for([0x22; 32]))
    }

    fn id(i: usize) -> ChunkId {
        let mut b = [0u8; 32];
        b[..8].copy_from_slice(&(i as u64).to_le_bytes());
        ChunkId::from_bytes(b)
    }

    fn trailer(n: usize, pack_type: TrailerPackType) -> PackTrailer {
        PackTrailer::for_blobs(pack_type, (0..n).map(|i| (id(i), (i % 300) as u32 + 1))).unwrap()
    }

    /// A pack sealed by `PackWriter` with `n` blobs, and its entries.
    fn sealed_pack(n: usize, crypto: &dyn CryptoEngine) -> (Vec<u8>, Vec<(ChunkId, u32, u64)>) {
        let mut w = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
        for i in 0..n {
            w.add_blob(id(i), vec![i as u8; 10 + i % 5]).unwrap();
        }
        let sealed = w.seal(crypto).unwrap();
        let entries = sealed.entries.iter().map(|e| (e.0, e.1, e.2)).collect();
        (sealed.data.as_bytes().unwrap().to_vec(), entries)
    }

    fn entries_of(t: &PackTrailer) -> Vec<(ChunkId, u32, u64)> {
        t.entries
            .iter()
            .map(|e| (e.chunk_id, e.stored_size, u64::from(e.offset)))
            .collect()
    }

    #[test]
    fn roundtrip_all_engines() {
        let chacha = ChaCha20Poly1305Engine::new(&[0x33; 32], chunk_hasher_for([0x44; 32]));
        let (plain, aes) = (plain(), aes());
        let engines: [&dyn CryptoEngine; 3] = [&plain, &aes, &chacha];
        for engine in engines {
            for n in [1, 2, 10_000] {
                let t = trailer(n, TrailerPackType::Tree);
                let blob = t.seal_blob(engine).unwrap();
                assert_eq!(blob.len(), trailer_blob_len(n, engine.is_encrypting()));
                assert_eq!(PackTrailer::open_blob(&blob, engine).unwrap(), t);
            }
        }
        // The fixed overhead per pack: 52 bytes encrypted, 24 plaintext.
        assert_eq!(4 + trailer_blob_len(0, true), 52);
        assert_eq!(4 + trailer_blob_len(0, false), 24);
        assert_eq!(trailer_frame_len_upper(3), 52 + 3 * 41);
    }

    #[test]
    fn body_is_canonical() {
        for n in [1, 7, 500] {
            for pack_type in [
                TrailerPackType::Data,
                TrailerPackType::Tree,
                TrailerPackType::Repacked,
            ] {
                let body = trailer(n, pack_type).encode_body();
                assert_eq!(body.len(), BODY_HEADER_LEN + TRAILER_ENTRY_LEN * n);
                assert_eq!(PackTrailer::decode_body(&body).unwrap().encode_body(), body);
            }
        }
    }

    #[test]
    fn framed_offsets_match_the_pack_layout() {
        let offsets: Vec<u64> = framed_offsets([2, 3, 0]).collect();
        assert_eq!(offsets, vec![13, 19, 26]);
        let t = PackTrailer::for_blobs(TrailerPackType::Data, [(id(0), 2), (id(1), 3), (id(2), 0)])
            .unwrap();
        assert!(t.offsets().eq(offsets));
        assert!(PackTrailer::for_blobs(TrailerPackType::Data, []).is_err());
        // The fourth blob would start past u32.
        let huge = [(id(0), u32::MAX), (id(1), u32::MAX)];
        assert!(PackTrailer::for_blobs(TrailerPackType::Data, huge).is_err());
    }

    fn assert_invalid(body: &[u8], what: &str) {
        match PackTrailer::decode_body(body) {
            Err(VykarError::InvalidFormat(_)) => {}
            other => panic!("{what}: expected InvalidFormat, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects() {
        let good = trailer(3, TrailerPackType::Data).encode_body();
        let with = |i: usize, v: u8| {
            let mut b = good.clone();
            b[i] = v;
            b
        };
        let entry = |k: usize| BODY_HEADER_LEN + k * TRAILER_ENTRY_LEN;
        let with_u32 = |at: usize, v: u32| {
            let mut b = good.clone();
            b[at..at + 4].copy_from_slice(&v.to_le_bytes());
            b
        };

        // A newer body is refused by version, not as damage.
        assert!(matches!(
            PackTrailer::decode_body(&with(0, 2)),
            Err(VykarError::UnsupportedVersion(2))
        ));
        assert_invalid(&[], "empty");
        assert_invalid(&with(1, 3), "pack type");
        assert_invalid(&with(entry(1) + 40, 1), "kind");
        assert_invalid(&with_u32(2, 0), "count 0");
        let mut zero = good[..BODY_HEADER_LEN].to_vec();
        zero[2..6].copy_from_slice(&0u32.to_le_bytes());
        assert_invalid(&zero, "count 0, no entries");
        assert_invalid(
            &with_u32(2, MAX_TRAILER_ENTRIES as u32 + 1),
            "count over the bound",
        );
        assert_invalid(&good[..good.len() - 1], "one byte short");
        assert_invalid(&[good.clone(), vec![0]].concat(), "one byte long");
        assert_invalid(&with_u32(2, 4), "count above the entries");
        // Offsets: 13, 18, 24 for sizes 1, 2, 3.
        assert_invalid(&with_u32(entry(1) + 32, 13), "not increasing");
        assert_invalid(&with_u32(entry(1) + 32, 17), "overlaps the previous blob");
        assert_invalid(&with_u32(entry(0) + 32, 12), "inside the header");
        // The boundary itself is fine.
        assert!(PackTrailer::decode_body(&with_u32(entry(1) + 32, 18)).is_ok());
    }

    #[test]
    fn open_blob_rejects() {
        let t = trailer(2, TrailerPackType::Data);
        let aes = aes();
        let blob = t.seal_blob(&aes).unwrap();

        let mut flipped = blob.clone();
        flipped[20] ^= 1;
        assert!(
            PackTrailer::open_blob(&flipped, &aes).is_err(),
            "flipped ciphertext"
        );

        let tagged = compress::compress(Compression::None, &t.encode_body()).unwrap();
        let reseal = |obj: Vec<u8>| {
            let mut b = obj;
            let len = (b.len() + TRAILER_FOOTER_LEN) as u32;
            b.extend_from_slice(&len.to_le_bytes());
            b.extend_from_slice(TRAILER_MAGIC);
            b
        };
        let wrong_context = reseal(
            pack_object_with_context(ObjectType::PackTrailer, b"pack-trailes", &tagged, &aes)
                .unwrap(),
        );
        assert!(
            PackTrailer::open_blob(&wrong_context, &aes).is_err(),
            "context"
        );
        let wrong_tag = reseal(
            pack_object_with_context(ObjectType::ChunkData, TRAILER_CONTEXT, &tagged, &aes)
                .unwrap(),
        );
        assert!(PackTrailer::open_blob(&wrong_tag, &aes).is_err(), "tag");
        assert!(
            PackTrailer::open_blob(&blob, &plain()).is_err(),
            "other engine"
        );

        let n = blob.len();
        let mut wrong_len = blob.clone();
        wrong_len[n - 12..n - 8].copy_from_slice(&((n - 1) as u32).to_le_bytes());
        assert!(
            PackTrailer::open_blob(&wrong_len, &aes).is_err(),
            "footer length"
        );
        let mut wrong_magic = blob.clone();
        wrong_magic[n - 1] ^= 1;
        assert!(PackTrailer::open_blob(&wrong_magic, &aes).is_err(), "magic");
        assert!(PackTrailer::open_blob(&blob[..5], &aes).is_err(), "short");
    }

    #[test]
    fn footer_and_frame() {
        assert_eq!(parse_footer(b"VYKTRLR1"), None);
        let mut footer = 77u32.to_le_bytes().to_vec();
        footer.extend_from_slice(TRAILER_MAGIC);
        assert_eq!(parse_footer(&footer), Some(77));
        assert_eq!(parse_footer(&[&[0u8; 5][..], &footer].concat()), Some(77));
        footer[11] ^= 1;
        assert_eq!(parse_footer(&footer), None);

        assert_eq!(trailer_frame_start(100, 12), None, "L below 13");
        assert_eq!(trailer_frame_start(100, 13), Some(83));
        assert_eq!(trailer_frame_start(100, 87), Some(9));
        assert_eq!(
            trailer_frame_start(100, 88),
            None,
            "reaches into the header"
        );
        assert_eq!(trailer_frame_start(10, 200), None);

        let blob = trailer(1, TrailerPackType::Data)
            .seal_blob(&plain())
            .unwrap();
        let mut frame = (blob.len() as u32).to_le_bytes().to_vec();
        frame.extend_from_slice(&blob);
        assert!(open_trailer_frame(&frame, &plain()).is_ok());
        frame[0] ^= 1;
        assert!(open_trailer_frame(&frame, &plain()).is_err(), "prefix != L");
        assert!(open_trailer_frame(&frame[..3], &plain()).is_err());
    }

    #[test]
    fn read_from_pack_bytes() {
        for crypto in [&plain() as &dyn CryptoEngine, &aes()] {
            let (pack, entries) = sealed_pack(5, crypto);
            let t = read_trailer_from_pack_bytes(&pack, crypto)
                .unwrap()
                .unwrap();
            assert_eq!(entries_of(&t), entries);
            assert_eq!(t.pack_type, TrailerPackType::Data);
        }

        // A pack from before trailers.
        let mut old = vykar_protocol::PACK_MAGIC.to_vec();
        old.push(1);
        old.extend_from_slice(&[2, 0, 0, 0, 0xde, 0xad, 3, 0, 0, 0, 0xbe, 0xef, 0x42]);
        assert_eq!(read_trailer_from_pack_bytes(&old, &plain()).unwrap(), None);
        assert!(read_trailer_from_pack_bytes(b"garbage", &plain()).is_err());

        // A trailer whose last blob ends past the frame start.
        let t = PackTrailer::for_blobs(TrailerPackType::Data, [(id(0), 2), (id(1), 4)]).unwrap();
        let blob = t.seal_blob(&plain()).unwrap();
        let mut bad = old.clone();
        bad.extend_from_slice(&(blob.len() as u32).to_le_bytes());
        bad.extend_from_slice(&blob);
        assert!(read_trailer_from_pack_bytes(&bad, &plain()).is_err());
    }

    #[test]
    fn read_pack_trailer_tail_and_second_read() {
        let crypto = plain();
        let (backend, log) = RecordingBackend::new();
        // Small: one tail read. 2 000 blobs: the trailer (~82 KB) is longer
        // than the tail read, so a second read fetches the frame.
        for (n, reads) in [(3, 1), (2_000, 2)] {
            let (pack, entries) = sealed_pack(n, &crypto);
            let key = format!("packs/00/{n}");
            backend.put(&key, &pack).unwrap();
            log.reset_get_range_count();
            match read_pack_trailer(&backend, &key, pack.len() as u64, &crypto).unwrap() {
                TrailerRead::Found(t) => assert_eq!(entries_of(&t), entries),
                other => panic!("{n}: {other:?}"),
            }
            assert_eq!(log.get_range_count(), reads, "{n}");
        }

        let mut old = vykar_protocol::PACK_MAGIC.to_vec();
        old.push(1);
        old.extend_from_slice(&[2, 0, 0, 0, 0xde, 0xad]);
        backend.put("packs/00/old", &old).unwrap();
        assert!(matches!(
            read_pack_trailer(&backend, "packs/00/old", old.len() as u64, &crypto).unwrap(),
            TrailerRead::Absent
        ));

        assert!(matches!(
            read_pack_trailer(&backend, "packs/00/gone", 100, &crypto).unwrap(),
            TrailerRead::PackMissing
        ));

        let (mut pack, _) = sealed_pack(3, &crypto);
        let n = pack.len();
        let len = u32::from_le_bytes(pack[n - 12..n - 8].try_into().unwrap());
        let start = n - 4 - len as usize;
        // The frame: [len][tag][codec][version][pack type]...
        pack[start + 6] = 9; // body version
        backend.put("packs/00/newer", &pack).unwrap();
        assert!(matches!(
            read_pack_trailer(&backend, "packs/00/newer", n as u64, &crypto).unwrap(),
            TrailerRead::Damaged(VykarError::UnsupportedVersion(9))
        ));
        pack[start + 6] = TRAILER_BODY_VERSION;
        pack[start + 7] = 7; // pack type
        backend.put("packs/00/bad", &pack).unwrap();
        assert!(matches!(
            read_pack_trailer(&backend, "packs/00/bad", n as u64, &crypto).unwrap(),
            TrailerRead::Damaged(VykarError::InvalidFormat(_))
        ));
    }
}
