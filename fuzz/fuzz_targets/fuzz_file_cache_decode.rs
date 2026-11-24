#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::repo::file_cache::FileCache;

fuzz_target!(|data: &[u8]| {
    let _ = FileCache::decode_from_plaintext(data);
    // The merged save's decode, which skips the sections it replaces.
    let skip: std::collections::BTreeSet<String> = ["/".to_string(), "/home".to_string()].into();
    let _ = FileCache::decode_from_plaintext_skipping(data, &skip);
});
