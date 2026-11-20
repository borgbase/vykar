#![no_main]
use libfuzzer_sys::fuzz_target;

/// `compress/mod.rs` `TAG_ZSTD`.
const TAG_ZSTD: u8 = 0x02;

fuzz_target!(|data: &[u8]| {
    let _ = vykar_core::compress::decompress(data);
    let buffered = vykar_core::compress::decompress_metadata(data);

    // The streaming reader bases are decoded through agrees with the
    // buffered path on zstd, and takes nothing else.
    let streamed = vykar_core::fuzzing::read_zstd_metadata(data);
    if data.first() == Some(&TAG_ZSTD) {
        assert_eq!(streamed.ok(), buffered.ok());
    } else {
        assert!(streamed.is_err());
    }
});
