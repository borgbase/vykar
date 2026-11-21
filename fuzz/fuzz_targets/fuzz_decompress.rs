#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::compress::{
    decompress, decompress_into_with_hint, decompress_metadata, decompress_with_hint,
};

/// `compress/mod.rs` `TAG_ZSTD`.
const TAG_ZSTD: u8 = 0x02;

fuzz_target!(|data: &[u8]| {
    let unhinted = decompress(data);
    let buffered = decompress_metadata(data);

    // Chunk decoding must not depend on the size hint: `check` decodes
    // without one, restore with the size from the item.
    if let Ok(plain) = &unhinted {
        assert_eq!(
            decompress_with_hint(data, Some(plain.len())).as_ref().ok(),
            Some(plain)
        );
        let mut out = vec![0xEE; 64];
        decompress_into_with_hint(data, Some(plain.len()), &mut out).unwrap();
        assert_eq!(&out, plain);
        decompress_into_with_hint(data, None, &mut out).unwrap();
        assert_eq!(&out, plain);
    }
    // No implication between chunk and metadata decoding: libzstd's one-shot
    // and streaming decoders each accept frames the other rejects (seeds
    // `zstd_empty_compressed_block`, `zstd_rle_block_over_window`).

    // The streaming reader bases are decoded through agrees with the
    // buffered path on zstd, and takes nothing else.
    let streamed = vykar_core::fuzzing::read_zstd_metadata(data);
    if data.first() == Some(&TAG_ZSTD) {
        assert_eq!(streamed.ok(), buffered.ok());
    } else {
        assert!(streamed.is_err());
    }
});
