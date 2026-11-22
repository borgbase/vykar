#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::repo::pack_map::PackMap;

fuzz_target!(|data: &[u8]| {
    // The body decoder is canonical.
    if let Ok(m) = PackMap::decode_body(data) {
        assert_eq!(
            m.encode_body(),
            data,
            "decoded pack map body must re-encode identically"
        );
    }
});
