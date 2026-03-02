use std::io::Read;

use serde::{Deserialize, Serialize};

use crate::config::CompressionAlgorithm;
use vykar_types::error::{Result, VykarError};

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
                let (_, cx) = slot.as_mut().unwrap();

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
    if data.is_empty() {
        return Err(VykarError::Decompression("empty data".into()));
    }
    let tag = data[0];
    let payload = &data[1..];
    match tag {
        TAG_NONE => Ok(payload.to_vec()),
        TAG_LZ4 => {
            if payload.len() < 4 {
                return Err(VykarError::Decompression("lz4: payload too short".into()));
            }
            let uncompressed_size = u32::from_le_bytes(payload[..4].try_into().unwrap()) as u64;
            if uncompressed_size > MAX_DECOMPRESS_SIZE {
                return Err(VykarError::Decompression(format!(
                    "lz4: decompressed size ({uncompressed_size}) exceeds limit of {MAX_DECOMPRESS_SIZE} bytes"
                )));
            }
            lz4_flex::decompress_size_prepended(payload)
                .map_err(|e| VykarError::Decompression(format!("lz4: {e}")))
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
                    let dx = slot.as_mut().unwrap();
                    // A zero hint means "unknown" — clamp to 1 so bulk::decompress
                    // allocates a minimal buffer rather than returning an empty Vec
                    // for what might be a valid non-empty frame.
                    let cap = hint.max(1).min(MAX_DECOMPRESS_SIZE as usize);
                    let output = dx
                        .decompress(payload, cap)
                        .map_err(|e| VykarError::Decompression(format!("zstd: {e}")))?;
                    if output.len() as u64 > MAX_DECOMPRESS_SIZE {
                        return Err(VykarError::Decompression(format!(
                            "zstd: decompressed size exceeds limit of {} bytes",
                            MAX_DECOMPRESS_SIZE
                        )));
                    }
                    Ok(output)
                })
            } else {
                // Cold path: streaming decoder (handles unknown sizes efficiently)
                let mut decoder = zstd::stream::Decoder::new(std::io::Cursor::new(payload))
                    .map_err(|e| VykarError::Decompression(format!("zstd init: {e}")))?;
                let mut output = Vec::new();
                decoder
                    .by_ref()
                    .take(MAX_DECOMPRESS_SIZE + 1)
                    .read_to_end(&mut output)
                    .map_err(|e| VykarError::Decompression(format!("zstd: {e}")))?;
                if output.len() as u64 > MAX_DECOMPRESS_SIZE {
                    return Err(VykarError::Decompression(format!(
                        "zstd: decompressed size exceeds limit of {} bytes",
                        MAX_DECOMPRESS_SIZE
                    )));
                }
                Ok(output)
            }
        }
        _ => Err(VykarError::UnknownCompressionTag(tag)),
    }
}

/// Decompress metadata objects (e.g. chunk index) with a higher size limit.
///
/// Same dispatch logic as `decompress()` but allows up to 4 GiB of output,
/// sized for metadata objects that scale with the number of chunks in the repo.
pub fn decompress_metadata(data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Err(VykarError::Decompression("empty data".into()));
    }
    let tag = data[0];
    let payload = &data[1..];
    match tag {
        TAG_NONE => Ok(payload.to_vec()),
        TAG_LZ4 => {
            if payload.len() < 4 {
                return Err(VykarError::Decompression("lz4: payload too short".into()));
            }
            let uncompressed_size = u32::from_le_bytes(payload[..4].try_into().unwrap()) as u64;
            if uncompressed_size > MAX_METADATA_DECOMPRESS_SIZE {
                return Err(VykarError::Decompression(format!(
                    "lz4: decompressed size ({uncompressed_size}) exceeds metadata limit of {MAX_METADATA_DECOMPRESS_SIZE} bytes"
                )));
            }
            lz4_flex::decompress_size_prepended(payload)
                .map_err(|e| VykarError::Decompression(format!("lz4: {e}")))
        }
        TAG_ZSTD => {
            let mut decoder = zstd::stream::Decoder::new(std::io::Cursor::new(payload))
                .map_err(|e| VykarError::Decompression(format!("zstd init: {e}")))?;
            let mut output = Vec::new();
            decoder
                .by_ref()
                .take(MAX_METADATA_DECOMPRESS_SIZE + 1)
                .read_to_end(&mut output)
                .map_err(|e| VykarError::Decompression(format!("zstd: {e}")))?;
            if output.len() as u64 > MAX_METADATA_DECOMPRESS_SIZE {
                return Err(VykarError::Decompression(format!(
                    "zstd: decompressed size exceeds metadata limit of {} bytes",
                    MAX_METADATA_DECOMPRESS_SIZE
                )));
            }
            Ok(output)
        }
        _ => Err(VykarError::UnknownCompressionTag(tag)),
    }
}

/// Stream-compress data into `buf` using ZSTD. Prepends the codec tag byte.
/// The `write_data` callback should write uncompressed data into the encoder.
pub fn compress_stream_zstd<F>(buf: &mut Vec<u8>, level: i32, write_data: F) -> Result<()>
where
    F: FnOnce(&mut zstd::stream::write::Encoder<&mut Vec<u8>>) -> Result<()>,
{
    buf.push(TAG_ZSTD);
    let mut encoder = zstd::stream::write::Encoder::new(&mut *buf, level)
        .map_err(|e| VykarError::Other(format!("zstd init: {e}")))?;
    write_data(&mut encoder)?;
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
            let uncompressed_size = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
            if uncompressed_size as u64 > MAX_DECOMPRESS_SIZE {
                return Err(VykarError::Decompression(format!(
                    "lz4: decompressed size ({uncompressed_size}) exceeds limit of {MAX_DECOMPRESS_SIZE} bytes"
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
                    static ZSTD_DX_INTO: RefCell<Option<zstd::bulk::Decompressor<'static>>> =
                        const { RefCell::new(None) };
                }
                ZSTD_DX_INTO.with(|cell| {
                    let mut slot = cell.borrow_mut();
                    if slot.is_none() {
                        *slot =
                            Some(zstd::bulk::Decompressor::new().map_err(|e| {
                                VykarError::Decompression(format!("zstd init: {e}"))
                            })?);
                    }
                    let dx = slot.as_mut().unwrap();
                    let cap = hint.max(1).min(MAX_DECOMPRESS_SIZE as usize);
                    output.clear();
                    output.resize(cap, 0);
                    let written = dx
                        .decompress_to_buffer(payload, output)
                        .map_err(|e| VykarError::Decompression(format!("zstd: {e}")))?;
                    output.truncate(written);
                    if output.len() as u64 > MAX_DECOMPRESS_SIZE {
                        return Err(VykarError::Decompression(format!(
                            "zstd: decompressed size exceeds limit of {} bytes",
                            MAX_DECOMPRESS_SIZE
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
                    .take(MAX_DECOMPRESS_SIZE + 1)
                    .read_to_end(output)
                    .map_err(|e| VykarError::Decompression(format!("zstd: {e}")))?;
                if output.len() as u64 > MAX_DECOMPRESS_SIZE {
                    return Err(VykarError::Decompression(format!(
                        "zstd: decompressed size exceeds limit of {} bytes",
                        MAX_DECOMPRESS_SIZE
                    )));
                }
                Ok(())
            }
        }
        _ => Err(VykarError::UnknownCompressionTag(tag)),
    }
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
}
