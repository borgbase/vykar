// LZ4 in-place compression uses raw pointer writes; SAFETY documented per block.
#![allow(unsafe_code)]
// Wire-format reads after explicit length checks (`data.is_empty()`,
// `payload.len() < 4`); the indexing and slicing into `data`/`payload` is
// unreachable for inputs that pass those guards.
#![allow(clippy::indexing_slicing)]

pub(crate) mod presize;

use std::io::Read;

use serde::{Deserialize, Serialize};

use crate::config::CompressionAlgorithm;
use vykar_types::error::{Result, VykarError};

fn read_u32_le(bytes: [u8; 4]) -> u32 {
    u32::from_le_bytes(bytes)
}

const TAG_NONE: u8 = 0x00;
const TAG_LZ4: u8 = 0x01;
const TAG_ZSTD: u8 = 0x02;

/// Maximum decompressed output size (32 MiB = 4× max chunk size).
/// Prevents decompression bombs from consuming unbounded memory.
const MAX_DECOMPRESS_SIZE: u64 = 32 * 1024 * 1024;

/// Maximum decompressed output size for metadata objects (4 GiB).
/// The chunk index scales ~80 bytes/chunk, so 10M chunks ≈ 800 MB.
/// This limit accommodates very large repos while still bounding memory.
const MAX_METADATA_DECOMPRESS_SIZE: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Compression {
    None,
    #[default]
    Lz4,
    Zstd {
        level: i32,
    },
}

impl Compression {
    /// Parse from config string like "lz4", "zstd", "none".
    pub fn from_config(algorithm: &str, zstd_level: i32) -> Result<Self> {
        match algorithm {
            "none" => Ok(Compression::None),
            "lz4" => Ok(Compression::Lz4),
            "zstd" => Ok(Compression::Zstd { level: zstd_level }),
            other => Err(VykarError::Config(format!(
                "unknown compression algorithm: {other}"
            ))),
        }
    }

    pub fn from_algorithm(algorithm: CompressionAlgorithm, zstd_level: i32) -> Self {
        match algorithm {
            CompressionAlgorithm::None => Compression::None,
            CompressionAlgorithm::Lz4 => Compression::Lz4,
            CompressionAlgorithm::Zstd => Compression::Zstd { level: zstd_level },
        }
    }
}

/// Returns the worst-case output size of `compress_append` (including the
/// 1-byte codec tag). Useful for pre-allocating a buffer that will never
/// need to grow.
pub fn compressed_size_bound(compression: Compression, data_len: usize) -> usize {
    match compression {
        Compression::None => 1 + data_len,
        // TAG + 4-byte LE uncompressed-size + worst-case LZ4 output
        Compression::Lz4 => 1 + 4 + lz4_flex::block::get_maximum_output_size(data_len),
        Compression::Zstd { .. } => 1 + zstd::zstd_safe::compress_bound(data_len),
    }
}

/// Append compressed data (including the 1-byte codec tag) directly into
/// `buf`. The caller should ensure `buf` has enough spare capacity (via
/// `compressed_size_bound`) to avoid reallocation.
pub fn compress_append(compression: Compression, data: &[u8], buf: &mut Vec<u8>) -> Result<()> {
    match compression {
        Compression::None => {
            buf.push(TAG_NONE);
            buf.extend_from_slice(data);
            Ok(())
        }
        Compression::Lz4 => {
            buf.push(TAG_LZ4);
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            let start = buf.len();
            let max_out = lz4_flex::block::get_maximum_output_size(data.len());
            buf.reserve(max_out);
            // SAFETY: `reserve` guarantees `buf.capacity() >= start + max_out`.
            // `compress_into` writes exactly `actual` bytes to the output slice
            // and never reads from uninitialized memory. `actual <= max_out` is
            // guaranteed by the LZ4 specification.
            let actual = unsafe {
                let dst = std::slice::from_raw_parts_mut(buf.as_mut_ptr().add(start), max_out);
                lz4_flex::block::compress_into(data, dst)
                    .map_err(|e| VykarError::Other(format!("lz4: {e}")))?
            };
            // SAFETY: `compress_into` initialized exactly `actual` bytes at
            // `start`, all prior bytes were already initialized.
            unsafe { buf.set_len(start + actual) };
            Ok(())
        }
        Compression::Zstd { level } => {
            use std::cell::RefCell;
            thread_local! {
                static ZSTD_CX: RefCell<Option<(i32, zstd::bulk::Compressor<'static>)>> =
                    const { RefCell::new(None) };
            }

            ZSTD_CX.with(|cell| {
                let mut slot = cell.borrow_mut();

                // Lazily init or reinit if the compression level changed.
                if !matches!(slot.as_ref(), Some((l, _)) if *l == level) {
                    let cx = zstd::bulk::Compressor::new(level)
                        .map_err(|e| VykarError::Other(format!("zstd init: {e}")))?;
                    *slot = Some((level, cx));
                }
                let (_, cx) = slot.as_mut().expect("zstd compressor initialized");

                buf.push(TAG_ZSTD);
                // Write compressed data directly into buf's spare capacity
                // via a Cursor, eliminating the intermediate Vec from
                // cx.compress(). The Cursor offsets the write pointer by its
                // position so ZSTD writes after the tag byte.
                let bound = zstd::zstd_safe::compress_bound(data.len());
                buf.reserve(bound);
                let pos = buf.len() as u64;
                let mut cursor = std::io::Cursor::new(&mut *buf);
                cursor.set_position(pos);
                cx.compress_to_buffer(data, &mut cursor)
                    .map_err(|e| VykarError::Other(format!("zstd compress: {e}")))?;
                Ok(())
            })
        }
    }
}

/// Compress data and prepend a 1-byte tag identifying the codec.
pub fn compress(compression: Compression, data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(compressed_size_bound(compression, data.len()));
    compress_append(compression, data, &mut out)?;
    // compress_append (LZ4 path) resizes to worst-case then truncates length,
    // leaving excess capacity. Shrink so callers that hold the Vec (e.g.
    // store_chunk → pack_object_with_context) don't carry oversized buffers.
    out.shrink_to_fit();
    Ok(out)
}

/// Decompress data by reading the 1-byte tag prefix and dispatching.
pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    decompress_with_hint(data, None)
}

/// Core decompression dispatch behind every `decompress*` entry point.
///
/// - `expected_size`: ZSTD hint for bulk decompression. `Some(n)` uses the
///   thread-local bulk decompressor; `None` uses streaming.
/// - `max_size`: Upper bound on decompressed output (bomb protection).
/// - `output`: cleared, then filled with the plaintext. Callers that hold the
///   buffer across calls keep their allocation.
fn decompress_into_impl(
    data: &[u8],
    expected_size: Option<usize>,
    max_size: u64,
    output: &mut Vec<u8>,
) -> Result<()> {
    if data.is_empty() {
        return Err(VykarError::Decompression("empty data".into()));
    }
    let tag = data[0];
    let payload = &data[1..];
    match tag {
        TAG_NONE => {
            output.clear();
            output.extend_from_slice(payload);
            Ok(())
        }
        TAG_LZ4 => {
            if payload.len() < 4 {
                return Err(VykarError::Decompression("lz4: payload too short".into()));
            }
            let uncompressed_size = read_u32_le(
                payload[..4]
                    .try_into()
                    .expect("payload.len() >= 4 (checked above)"),
            ) as usize;
            if uncompressed_size as u64 > max_size {
                return Err(VykarError::Decompression(format!(
                    "lz4: decompressed size ({uncompressed_size}) exceeds limit of {max_size} bytes"
                )));
            }
            // LZ4 expands each compressed byte to at most 255 output bytes
            // (match-length extension bytes each add 255), so a declared size
            // beyond that is corrupt. Rejecting it before the `resize` bounds
            // the eager zero-fill by the payload we actually hold, instead of
            // committing up to `max_size` (4 GiB on the metadata path) of
            // resident memory from a forged 4-byte length header.
            let max_possible = ((payload.len() - 4) as u64)
                .saturating_mul(255)
                .saturating_add(64);
            if uncompressed_size as u64 > max_possible {
                return Err(VykarError::Decompression(format!(
                    "lz4: declared size ({uncompressed_size}) impossible for a {}-byte payload",
                    payload.len() - 4
                )));
            }
            output.clear();
            output.resize(uncompressed_size, 0);
            let written = lz4_flex::block::decompress_into(&payload[4..], output)
                .map_err(|e| VykarError::Decompression(format!("lz4: {e}")))?;
            if written != uncompressed_size {
                return Err(VykarError::Decompression(format!(
                    "lz4: declared size {uncompressed_size} but decompressed {written} bytes"
                )));
            }
            output.truncate(written);
            Ok(())
        }
        TAG_ZSTD => {
            if let Some(hint) = expected_size {
                // Hot path (restore): reuse thread-local bulk decompressor
                use std::cell::RefCell;
                thread_local! {
                    static ZSTD_DX: RefCell<Option<zstd::bulk::Decompressor<'static>>> =
                        const { RefCell::new(None) };
                }
                ZSTD_DX.with(|cell| {
                    let mut slot = cell.borrow_mut();
                    if slot.is_none() {
                        *slot =
                            Some(zstd::bulk::Decompressor::new().map_err(|e| {
                                VykarError::Decompression(format!("zstd init: {e}"))
                            })?);
                    }
                    let dx = slot.as_mut().expect("zstd decompressor initialized");
                    // A zero hint means "unknown" — clamp to 1 so the bulk
                    // decompressor gets a minimal buffer rather than an empty
                    // one for what might be a valid non-empty frame.
                    let cap = hint
                        .max(1)
                        .min(usize::try_from(max_size).unwrap_or(usize::MAX));
                    output.clear();
                    output.resize(cap, 0);
                    let written = dx
                        .decompress_to_buffer(payload, output)
                        .map_err(|e| VykarError::Decompression(format!("zstd: {e}")))?;
                    output.truncate(written);
                    if output.len() as u64 > max_size {
                        return Err(VykarError::Decompression(format!(
                            "zstd: decompressed size exceeds limit of {max_size} bytes",
                        )));
                    }
                    Ok(())
                })
            } else {
                // Cold path: streaming decoder (handles unknown sizes efficiently)
                output.clear();
                let mut decoder = zstd::stream::Decoder::new(std::io::Cursor::new(payload))
                    .map_err(|e| VykarError::Decompression(format!("zstd init: {e}")))?;
                decoder
                    .by_ref()
                    .take(max_size + 1)
                    .read_to_end(output)
                    .map_err(|e| VykarError::Decompression(format!("zstd: {e}")))?;
                if output.len() as u64 > max_size {
                    return Err(VykarError::Decompression(format!(
                        "zstd: decompressed size exceeds limit of {max_size} bytes",
                    )));
                }
                Ok(())
            }
        }
        _ => Err(VykarError::UnknownCompressionTag(tag)),
    }
}

/// Allocating wrapper over [`decompress_into_impl`].
fn decompress_impl(data: &[u8], expected_size: Option<usize>, max_size: u64) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    decompress_into_impl(data, expected_size, max_size, &mut output)?;
    Ok(output)
}

/// Decompress data by reading the 1-byte tag prefix and dispatching.
///
/// `expected_size` controls ZSTD decode strategy:
/// - `Some(n)`: uses a bulk decompressor with `n` as the output buffer capacity
///   (capped by `MAX_DECOMPRESS_SIZE`). The value must be >= the actual
///   decompressed size or the call will error. Best for restore paths where
///   the exact size is known from snapshot metadata.
/// - `None`: uses a streaming decoder that handles unknown sizes. Slightly
///   slower due to per-call decoder initialization.
///
/// For LZ4 and None codecs the parameter is unused.
pub fn decompress_with_hint(data: &[u8], expected_size: Option<usize>) -> Result<Vec<u8>> {
    decompress_impl(data, expected_size, MAX_DECOMPRESS_SIZE)
}

/// Decompress metadata objects (e.g. chunk index) with a higher size limit.
///
/// Same dispatch logic as `decompress()` but allows up to 4 GiB of output,
/// sized for metadata objects that scale with the number of chunks in the repo.
pub fn decompress_metadata(data: &[u8]) -> Result<Vec<u8>> {
    decompress_impl(data, None, MAX_METADATA_DECOMPRESS_SIZE)
}

/// Decode a msgpack value directly from a compressed metadata payload.
///
/// Equivalent to `rmp_serde::from_slice(&decompress_metadata(data)?)`, but
/// for ZSTD the plaintext is never materialized: msgpack is decoded straight
/// out of the streaming decoder. For a large chunk index this avoids holding
/// the full decompressed msgpack (tens of MiB) alongside the structure being
/// built from it. The frame is still read to its end after decoding, so a
/// malformed or truncated frame and the size limit are rejected exactly as in
/// [`decompress_metadata`]. LZ4 and uncompressed payloads fall back to the
/// buffered path.
///
/// Maps that opt into `presize` are pre-sized from their length prefix,
/// within a budget proportional to `data.len()`.
///
/// Returns the value and the decompressed size in bytes.
pub fn decode_metadata_msgpack<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<(T, u64)> {
    presize::with_input_budget(data.len(), || decode_metadata_msgpack_inner(data))
}

fn decode_metadata_msgpack_inner<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<(T, u64)> {
    if data.first() != Some(&TAG_ZSTD) {
        let raw = decompress_metadata(data)?;
        let value = rmp_serde::from_slice(&raw)?;
        return Ok((value, raw.len() as u64));
    }
    let decoder = zstd::stream::Decoder::new(std::io::Cursor::new(&data[1..]))
        .map_err(|e| VykarError::Decompression(format!("zstd init: {e}")))?;
    let mut reader = MsgpackStreamReader::new(decoder.take(MAX_METADATA_DECOMPRESS_SIZE + 1));
    let value = rmp_serde::from_read(&mut reader)?;
    let total = reader
        .drain()
        .map_err(|e| VykarError::Decompression(format!("zstd: {e}")))?;
    if total > MAX_METADATA_DECOMPRESS_SIZE {
        return Err(VykarError::Decompression(format!(
            "zstd: decompressed size exceeds limit of {MAX_METADATA_DECOMPRESS_SIZE} bytes",
        )));
    }
    Ok((value, total))
}

/// A streaming reader over a ZSTD-tagged metadata payload, limited to the
/// metadata size bound. Empty input and the other known codec tags are
/// `Decompression` errors; a tag no codec has is `UnknownCompressionTag`.
/// The caller must read to the end to detect a truncated frame.
pub(crate) fn zstd_metadata_reader(data: &[u8]) -> Result<impl Read + '_> {
    let frame = match data.split_first() {
        Some((&TAG_ZSTD, frame)) => frame,
        Some((&tag, _)) if tag != TAG_NONE && tag != TAG_LZ4 => {
            return Err(VykarError::UnknownCompressionTag(tag));
        }
        _ => {
            return Err(VykarError::Decompression(
                "expected a zstd-compressed payload".into(),
            ));
        }
    };
    let decoder = zstd::stream::Decoder::with_buffer(frame)
        .map_err(|e| VykarError::Decompression(format!("zstd init: {e}")))?;
    Ok(LimitedRead {
        inner: decoder,
        left: MAX_METADATA_DECOMPRESS_SIZE,
    })
}

/// Fails, instead of ending early as `Read::take` would, once more than
/// `left` bytes were produced.
struct LimitedRead<R> {
    inner: R,
    left: u64,
}

impl<R: Read> Read for LimitedRead<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(out)?;
        self.left = self.left.checked_sub(n as u64).ok_or_else(|| {
            std::io::Error::other(format!(
                "zstd: decompressed size exceeds limit of {MAX_METADATA_DECOMPRESS_SIZE} bytes"
            ))
        })?;
        Ok(n)
    }
}

/// Buffered reader tuned for rmp-serde, which issues a `read_exact` for every
/// marker byte and every short field: millions of 1-32 byte reads for a large
/// index or file cache. `std::io::BufReader` (plus a counting wrapper) costs
/// enough per call that streaming decode ran ~1.6x slower than decoding a
/// buffered slice; here the in-buffer case is a bounds check and a copy.
/// Counts the bytes pulled from `inner` (at refill), so after [`Self::drain`]
/// the total is the full decompressed size.
struct MsgpackStreamReader<R> {
    inner: R,
    buf: Box<[u8]>,
    pos: usize,
    filled: usize,
    total: u64,
}

impl<R: Read> MsgpackStreamReader<R> {
    const CAPACITY: usize = 64 * 1024;

    fn new(inner: R) -> Self {
        Self {
            inner,
            buf: vec![0u8; Self::CAPACITY].into_boxed_slice(),
            pos: 0,
            filled: 0,
            total: 0,
        }
    }

    /// Refill the (fully consumed) buffer; returns the bytes read, 0 at EOF.
    fn refill(&mut self) -> std::io::Result<usize> {
        let n = loop {
            match self.inner.read(&mut self.buf) {
                Ok(n) => break n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        };
        self.pos = 0;
        self.filled = n;
        self.total += n as u64;
        Ok(n)
    }

    /// Read `inner` to its end (so a truncated or malformed frame tail is
    /// still detected) and return the total number of bytes it produced.
    fn drain(mut self) -> std::io::Result<u64> {
        while self.refill()? != 0 {}
        Ok(self.total)
    }
}

impl<R: Read> Read for MsgpackStreamReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos == self.filled && self.refill()? == 0 {
            return Ok(0);
        }
        let n = out.len().min(self.filled - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }

    #[inline]
    fn read_exact(&mut self, out: &mut [u8]) -> std::io::Result<()> {
        if self.filled - self.pos >= out.len() {
            let end = self.pos + out.len();
            out.copy_from_slice(&self.buf[self.pos..end]);
            self.pos = end;
            return Ok(());
        }
        let mut rest = out;
        while !rest.is_empty() {
            match self.read(rest)? {
                0 => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                n => rest = &mut rest[n..],
            }
        }
        Ok(())
    }
}

/// Staging buffer in front of the streaming ZSTD encoder (about
/// `ZSTD_CStreamInSize`). rmp-serde emits one `write_all` per marker byte and
/// short field; without this, each of those is a separate
/// `ZSTD_compressStream` FFI call.
const ZSTD_STREAM_INPUT_BUFFER: usize = 128 << 10;

/// Writer handed to the [`compress_stream_zstd`] callback: a buffered ZSTD
/// encoder appending to the output `Vec`.
pub type ZstdStreamWriter<'a> =
    std::io::BufWriter<zstd::stream::write::Encoder<'static, &'a mut Vec<u8>>>;

/// Stream-compress data into `buf` using ZSTD. Prepends the codec tag byte.
/// The `write_data` callback should write uncompressed data into the writer;
/// small writes are batched before they reach the encoder.
pub fn compress_stream_zstd<F>(buf: &mut Vec<u8>, level: i32, write_data: F) -> Result<()>
where
    F: FnOnce(&mut ZstdStreamWriter<'_>) -> Result<()>,
{
    buf.push(TAG_ZSTD);
    let encoder = zstd::stream::write::Encoder::new(&mut *buf, level)
        .map_err(|e| VykarError::Other(format!("zstd init: {e}")))?;
    let mut writer = std::io::BufWriter::with_capacity(ZSTD_STREAM_INPUT_BUFFER, encoder);
    write_data(&mut writer)?;
    // `into_inner` flushes the staged bytes into the encoder.
    let encoder = writer
        .into_inner()
        .map_err(|e| VykarError::Other(format!("zstd write: {}", e.error())))?;
    encoder
        .finish()
        .map_err(|e| VykarError::Other(format!("zstd finish: {e}")))?;
    Ok(())
}

/// Decompress data into a caller-provided buffer, reusing its allocation.
///
/// `expected_size` controls ZSTD decode strategy (see [`decompress_with_hint`]).
/// When `Some(n)`, ZSTD uses a bulk decompressor and `n` must be >= the actual
/// decompressed size. All three codec paths reuse the existing allocation in
/// `output` from prior calls.
pub fn decompress_into_with_hint(
    data: &[u8],
    expected_size: Option<usize>,
    output: &mut Vec<u8>,
) -> Result<()> {
    decompress_into_impl(data, expected_size, MAX_DECOMPRESS_SIZE, output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decompress_rejects_lz4_bomb() {
        // Craft a payload with a huge size prefix (1 GiB) but tiny compressed data
        let mut bomb = (1u32 << 30).to_le_bytes().to_vec();
        bomb.extend_from_slice(&[0u8; 10]);
        // Prepend LZ4 tag
        let mut data = vec![TAG_LZ4];
        data.extend_from_slice(&bomb);
        assert!(decompress(&data).is_err());
    }

    #[test]
    fn decompress_rejects_lz4_impossible_declared_size() {
        // A declared size within the bomb limit but far beyond what the tiny
        // payload could expand to (max 255x) must be rejected BEFORE the
        // output buffer is zero-filled to the declared size.
        let mut forged = (16u32 << 20).to_le_bytes().to_vec(); // declares 16 MiB
        forged.extend_from_slice(&[0u8; 8]); // 8-byte body: max ~2 KiB output
        let mut data = vec![TAG_LZ4];
        data.extend_from_slice(&forged);
        let err = decompress(&data).unwrap_err().to_string();
        assert!(err.contains("impossible"), "got {err:?}");
    }

    #[test]
    fn decompress_rejects_lz4_short_payload() {
        // Only tag + 2 bytes (need at least 4 for size prefix)
        let data = vec![TAG_LZ4, 0x00, 0x00];
        assert!(decompress(&data).is_err());
    }

    #[test]
    fn compress_decompress_lz4_roundtrip() {
        let original = b"hello world, this is a test of lz4 compression";
        let compressed = compress(Compression::Lz4, original).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn decompress_with_hint_matches_decompress() {
        let payloads: &[&[u8]] = &[b"", b"short", b"this payload is long enough to compress"];
        let codecs = [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ];

        for codec in codecs {
            for payload in payloads {
                let encoded = compress(codec, payload).unwrap();
                let plain_a = decompress(&encoded).unwrap();
                let plain_b = decompress_with_hint(&encoded, Some(payload.len())).unwrap();
                assert_eq!(plain_a, plain_b);
            }
        }
    }

    #[test]
    fn decompress_with_hint_caps_large_hint() {
        let payload = vec![0xAB; 1024];
        let encoded = compress(Compression::Zstd { level: 3 }, &payload).unwrap();
        let decoded = decompress_with_hint(&encoded, Some(usize::MAX)).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn compressed_size_bound_is_upper_bound() {
        let codecs = [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ];
        // Test a range of sizes including edge cases
        let sizes: &[usize] = &[0, 1, 15, 256, 4096, 65536];
        for codec in codecs {
            for &size in sizes {
                let data = vec![0xAB; size];
                let bound = compressed_size_bound(codec, size);
                let compressed = compress(codec, &data).unwrap();
                assert!(
                    compressed.len() <= bound,
                    "{codec:?} size={size}: compressed len {} > bound {bound}",
                    compressed.len(),
                );
            }
        }
    }

    #[test]
    fn compress_append_matches_compress() {
        let codecs = [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ];
        let payloads: &[&[u8]] = &[
            b"",
            b"short",
            b"this payload is long enough to actually compress well with lz4",
        ];
        for codec in codecs {
            for payload in payloads {
                let standalone = compress(codec, payload).unwrap();
                let mut appended = Vec::new();
                compress_append(codec, payload, &mut appended).unwrap();
                assert_eq!(
                    standalone,
                    appended,
                    "{codec:?} payload len {}: standalone vs append mismatch",
                    payload.len(),
                );
                // Both must decompress to the original
                let recovered = decompress(&appended).unwrap();
                assert_eq!(recovered, *payload);
            }
        }
    }

    #[test]
    fn compress_append_into_prefilled_buffer() {
        // Verify compress_append works correctly when the buffer already has data
        let prefix = b"existing-data";
        let payload = b"hello world, this is a test of appending into a non-empty buffer";
        let codecs = [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ];
        for codec in codecs {
            let mut buf = prefix.to_vec();
            compress_append(codec, payload, &mut buf).unwrap();
            assert_eq!(&buf[..prefix.len()], prefix);
            let recovered = decompress(&buf[prefix.len()..]).unwrap();
            assert_eq!(recovered, payload);
        }
    }

    #[test]
    fn compress_does_not_leave_oversized_buffer() {
        // Verify compress() shrinks after LZ4 compress_append (which resizes
        // to worst-case then truncates). shrink_to_fit is best-effort, so we
        // check that capacity isn't wildly above len rather than exact equality.
        let payload = vec![0x42; 8192];
        let compressed = compress(Compression::Lz4, &payload).unwrap();
        let excess = compressed.capacity() - compressed.len();
        assert!(
            excess <= compressed.len() / 4,
            "excess capacity {excess} is too large relative to len {}",
            compressed.len(),
        );
    }

    #[test]
    fn decompress_metadata_roundtrip_all_codecs() {
        let payload = b"metadata payload for roundtrip test".to_vec();
        let codecs = [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ];
        for codec in codecs {
            let compressed = compress(codec, &payload).unwrap();
            let decompressed = decompress_metadata(&compressed).unwrap();
            assert_eq!(decompressed, payload, "roundtrip failed for {codec:?}");
        }
    }

    #[test]
    fn decompress_metadata_succeeds_above_chunk_limit() {
        // Data >32 MiB should fail with decompress() but succeed with decompress_metadata()
        let size = (MAX_DECOMPRESS_SIZE as usize) + 1024;
        let payload = vec![0x42; size];
        let compressed = compress(Compression::Zstd { level: 1 }, &payload).unwrap();

        // decompress() should fail (exceeds 32 MiB limit)
        assert!(decompress(&compressed).is_err());

        // decompress_metadata() should succeed (4 GiB limit)
        let decompressed = decompress_metadata(&compressed).unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn decode_metadata_msgpack_matches_buffered_decode_all_codecs() {
        let value: std::collections::BTreeMap<String, Vec<u64>> = (0..2000)
            .map(|i| (format!("key-{i}"), (0..i % 17).collect()))
            .collect();
        let encoded = rmp_serde::to_vec(&value).unwrap();
        for codec in [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ] {
            let compressed = compress(codec, &encoded).unwrap();
            let (decoded, size): (std::collections::BTreeMap<String, Vec<u64>>, u64) =
                decode_metadata_msgpack(&compressed).unwrap();
            assert_eq!(decoded, value, "{codec:?}");
            assert_eq!(size, encoded.len() as u64, "{codec:?}");
        }
    }

    #[test]
    fn msgpack_stream_reader_reads_across_refills_and_counts() {
        let data: Vec<u8> = (0..400_000u32).map(|i| (i * 7 + i / 3) as u8).collect();
        let mut r = MsgpackStreamReader::new(&data[..]);
        let mut out = Vec::new();
        // Mixed exact reads straddling the 64 KiB buffer boundary.
        let mut sizes = [1usize, 3, 32, 7, 65_535, 2, 100_000].iter().cycle();
        while out.len() < 250_000 {
            let n = *sizes.next().unwrap();
            let mut chunk = vec![0u8; n];
            r.read_exact(&mut chunk).unwrap();
            out.extend_from_slice(&chunk);
        }
        let mut tail = vec![0u8; 10];
        let n = r.read(&mut tail).unwrap();
        out.extend_from_slice(&tail[..n]);
        assert_eq!(out[..], data[..out.len()]);
        assert_eq!(r.drain().unwrap(), data.len() as u64);

        let mut short = MsgpackStreamReader::new(&data[..10]);
        let mut buf = [0u8; 11];
        assert_eq!(
            short.read_exact(&mut buf).unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn decode_metadata_msgpack_rejects_truncated_zstd_tail() {
        // A frame truncated after the msgpack value must still be rejected,
        // as the buffered path would: the frame is read to its end.
        let mut encoded = rmp_serde::to_vec(&vec![7u64; 1000]).unwrap();
        encoded.extend((0..200_000u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8));
        let mut compressed = compress(Compression::Zstd { level: 3 }, &encoded).unwrap();
        compressed.truncate(compressed.len() - 16);
        assert!(decompress_metadata(&compressed).is_err());
        assert!(decode_metadata_msgpack::<Vec<u64>>(&compressed).is_err());
    }

    #[test]
    fn decompress_metadata_rejects_empty_input() {
        assert!(decompress_metadata(&[]).is_err());
    }

    #[test]
    fn decompress_metadata_rejects_unknown_tag() {
        assert!(decompress_metadata(&[0xFF]).is_err());
    }

    #[test]
    fn roundtrip_none() {
        let data = b"hello world, no compression";
        let compressed = compress(Compression::None, data).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn roundtrip_zstd() {
        let data = b"hello world, zstd compression test data here";
        let compressed = compress(Compression::Zstd { level: 3 }, data).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn lz4_actually_compresses() {
        let data = vec![0x42u8; 10_000];
        let compressed = compress(Compression::Lz4, &data).unwrap();
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn decompress_empty_data_fails() {
        let result = decompress(b"");
        assert!(result.is_err());
        match result.unwrap_err() {
            VykarError::Decompression(msg) => assert_eq!(msg, "empty data"),
            other => panic!("expected Decompression error, got: {other}"),
        }
    }

    #[test]
    fn decompress_unknown_tag_fails() {
        let result = decompress(&[0xFF, 0x00, 0x01]);
        assert!(result.is_err());
        match result.unwrap_err() {
            VykarError::UnknownCompressionTag(0xFF) => {}
            other => panic!("expected UnknownCompressionTag(0xFF), got: {other}"),
        }
    }

    #[test]
    fn roundtrip_empty_payload_none() {
        let compressed = compress(Compression::None, b"").unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, b"");
    }

    #[test]
    fn roundtrip_empty_payload_lz4() {
        let compressed = compress(Compression::Lz4, b"").unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, b"");
    }

    #[test]
    fn roundtrip_empty_payload_zstd() {
        let compressed = compress(Compression::Zstd { level: 3 }, b"").unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, b"");
    }

    #[test]
    fn from_config_valid() {
        assert_eq!(
            Compression::from_config("none", 3).unwrap(),
            Compression::None
        );
        assert_eq!(
            Compression::from_config("lz4", 3).unwrap(),
            Compression::Lz4
        );
        assert_eq!(
            Compression::from_config("zstd", 5).unwrap(),
            Compression::Zstd { level: 5 }
        );
    }

    #[test]
    fn from_config_invalid() {
        let result = Compression::from_config("brotli", 3);
        assert!(result.is_err());
    }

    #[test]
    fn zstd_level_change_reinit() {
        let data = b"hello world, test zstd level change reinit path";

        let compressed_1 = compress(Compression::Zstd { level: 1 }, data).unwrap();
        let decompressed_1 = decompress(&compressed_1).unwrap();
        assert_eq!(decompressed_1, data);

        let compressed_9 = compress(Compression::Zstd { level: 9 }, data).unwrap();
        let decompressed_9 = decompress(&compressed_9).unwrap();
        assert_eq!(decompressed_9, data);
    }

    #[test]
    fn compress_stream_zstd_roundtrip() {
        let payload = b"hello world, this is a stream compression test payload";
        let mut buf = Vec::new();
        compress_stream_zstd(&mut buf, 3, |encoder| {
            use std::io::Write;
            encoder
                .write_all(payload)
                .map_err(|e| VykarError::Other(format!("write: {e}")))?;
            Ok(())
        })
        .unwrap();

        // First byte should be the ZSTD tag
        assert_eq!(buf[0], TAG_ZSTD);

        // Should decompress to the original payload
        let decompressed = decompress(&buf).unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn compress_stream_zstd_into_prefilled_buffer() {
        let prefix = b"existing-data";
        let payload = b"stream compressed payload";
        let mut buf = prefix.to_vec();
        compress_stream_zstd(&mut buf, 3, |encoder| {
            use std::io::Write;
            encoder
                .write_all(payload)
                .map_err(|e| VykarError::Other(format!("write: {e}")))?;
            Ok(())
        })
        .unwrap();

        assert_eq!(&buf[..prefix.len()], prefix);
        let decompressed = decompress(&buf[prefix.len()..]).unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn compress_stream_zstd_mixed_write_sizes_roundtrip() {
        // Tiny writes (staged), a write larger than the staging buffer
        // (passed through), and tiny writes again: the stream must decode to
        // exactly the concatenation, in order.
        use std::io::Write;
        let mut expected = Vec::new();
        let mut buf = Vec::new();
        compress_stream_zstd(&mut buf, 3, |w| {
            for i in 0..100_000u32 {
                let b = [(i % 251) as u8];
                w.write_all(&b)?;
                expected.extend_from_slice(&b);
            }
            let big: Vec<u8> = (0..(ZSTD_STREAM_INPUT_BUFFER * 3 + 17))
                .map(|i| (i * 7 % 256) as u8)
                .collect();
            w.write_all(&big)?;
            expected.extend_from_slice(&big);
            for i in 0..1000u32 {
                let b = i.to_le_bytes();
                w.write_all(&b)?;
                expected.extend_from_slice(&b);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(buf[0], TAG_ZSTD);
        assert_eq!(decompress(&buf).unwrap(), expected);
    }

    #[test]
    fn decompress_into_with_hint_matches_decompress() {
        let payloads: &[&[u8]] = &[
            b"",
            b"tiny",
            b"larger test payload for decompress_into hint checks",
        ];
        let codecs = [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ];

        let mut output = Vec::new();
        for codec in codecs {
            for payload in payloads {
                let encoded = compress(codec, payload).unwrap();
                let expected = decompress(&encoded).unwrap();
                // With exact hint
                decompress_into_with_hint(&encoded, Some(payload.len()), &mut output).unwrap();
                assert_eq!(output, expected, "{codec:?} hint=Some(exact)");
                // Without hint
                output.clear();
                decompress_into_with_hint(&encoded, None, &mut output).unwrap();
                assert_eq!(output, expected, "{codec:?} hint=None");
            }
        }
    }

    #[test]
    fn decompress_into_with_hint_reuses_buffer() {
        let mut output = Vec::with_capacity(1024);
        let ptr_before = output.as_ptr();

        let data1 = b"first payload for buffer reuse test";
        let encoded1 = compress(Compression::Lz4, data1).unwrap();
        decompress_into_with_hint(&encoded1, Some(data1.len()), &mut output).unwrap();
        assert_eq!(output, data1);

        let data2 = b"second payload";
        let encoded2 = compress(Compression::Lz4, data2).unwrap();
        decompress_into_with_hint(&encoded2, Some(data2.len()), &mut output).unwrap();
        assert_eq!(output, data2);

        // Buffer should still be using the same allocation (data2 fits in original capacity)
        assert_eq!(output.as_ptr(), ptr_before);
    }

    #[test]
    fn decompress_into_with_hint_caps_large_hint() {
        let payload = vec![0xAB; 1024];
        let encoded = compress(Compression::Zstd { level: 3 }, &payload).unwrap();
        let mut output = Vec::new();
        decompress_into_with_hint(&encoded, Some(usize::MAX), &mut output).unwrap();
        assert_eq!(output, payload);
    }

    #[test]
    fn decompress_into_with_hint_empty_data_fails() {
        let mut output = Vec::new();
        let result = decompress_into_with_hint(b"", None, &mut output);
        assert!(result.is_err());
        match result.unwrap_err() {
            VykarError::Decompression(msg) => assert_eq!(msg, "empty data"),
            other => panic!("expected Decompression error, got: {other}"),
        }
    }

    #[test]
    fn zstd_metadata_reader_reads_a_frame_and_rejects_it_truncated() {
        let payload: Vec<u8> = (0..200_000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
            .collect();
        let mut compressed = Vec::new();
        compress_stream_zstd(&mut compressed, 3, |w| {
            std::io::Write::write_all(w, &payload)?;
            Ok(())
        })
        .unwrap();

        let mut out = Vec::new();
        zstd_metadata_reader(&compressed)
            .unwrap()
            .read_to_end(&mut out)
            .unwrap();
        assert_eq!(out, payload);

        for cut in [compressed.len() / 2, compressed.len() - 1] {
            let mut out = Vec::new();
            let read = zstd_metadata_reader(&compressed[..cut])
                .unwrap()
                .read_to_end(&mut out);
            assert!(read.is_err(), "cut at {cut} must fail");
            assert!(decompress_metadata(&compressed[..cut]).is_err());
        }
    }

    #[test]
    fn zstd_metadata_reader_rejects_other_codecs() {
        let none = compress(Compression::None, b"x").unwrap();
        let lz4 = compress(Compression::Lz4, b"x").unwrap();
        for data in [&[][..], &none, &lz4] {
            assert!(matches!(
                zstd_metadata_reader(data),
                Err(VykarError::Decompression(_))
            ));
        }
        assert!(matches!(
            zstd_metadata_reader(&[0xEE, 1, 2, 3]),
            Err(VykarError::UnknownCompressionTag(0xEE))
        ));
    }

    #[test]
    fn limited_read_fails_once_past_its_limit() {
        let mut out = Vec::new();
        let at_limit = LimitedRead {
            inner: &[7u8; 10][..],
            left: 10,
        }
        .read_to_end(&mut out);
        assert_eq!(at_limit.unwrap(), 10);

        let err = LimitedRead {
            inner: &[7u8; 10][..],
            left: 9,
        }
        .read_to_end(&mut Vec::new())
        .unwrap_err();
        assert!(err.to_string().contains("exceeds limit"), "got {err}");
    }
}
