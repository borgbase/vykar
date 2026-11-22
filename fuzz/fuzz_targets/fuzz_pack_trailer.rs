#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::repo::pack_trailer::{
    PackTrailer, open_trailer_frame, parse_footer, read_trailer_from_pack_bytes,
};
use vykar_crypto::PlaintextEngine;
use vykar_types::chunk_id::ChunkHasher;
use vykar_types::hash::HashAlgorithm;

fuzz_target!(|data: &[u8]| {
    // The body decoder is canonical.
    if let Ok(t) = PackTrailer::decode_body(data) {
        assert_eq!(
            t.encode_body(),
            data,
            "decoded trailer body must re-encode identically"
        );
    }
    let engine = PlaintextEngine::new(ChunkHasher::new(HashAlgorithm::Blake3, [7; 32]));
    let _ = parse_footer(data);
    let _ = open_trailer_frame(data, &engine);
    let _ = read_trailer_from_pack_bytes(data, &engine);
});
