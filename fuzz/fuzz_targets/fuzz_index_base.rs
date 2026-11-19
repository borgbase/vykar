#![no_main]
//! Input: one byte of expected entry count (as the head would name it),
//! then the base body.
use libfuzzer_sys::fuzz_target;
use vykar_core::index::base::{decode_base_to_temp, encode_base};
use vykar_core::index::dedup_cache::MmapFullIndexCache;

fuzz_target!(|data: &[u8]| {
    let Some((&expected, body)) = data.split_first() else {
        return;
    };
    let target = std::env::temp_dir().join("vykar-fuzz-index-base");
    if let Ok((tmp, _, _)) = decode_base_to_temp(body, u64::from(expected), &target) {
        let cache = MmapFullIndexCache::open_verified(tmp.path()).expect("decoded cache verifies");
        let mut encoded = Vec::new();
        encode_base(&cache, &mut encoded).expect("a decoded base encodes");
        assert_eq!(encoded, body, "decoded base must re-encode identically");
    }
});
