#![no_main]
//! `compress::decode_metadata_msgpack`, the streaming decode of a v2/v3
//! `IndexBlob`, against decompressing it whole and decoding the buffer.

use libfuzzer_sys::fuzz_target;
use vykar_core::index::IndexBlob;

/// The blob's entries in a canonical order.
fn canonical(blob: &IndexBlob) -> (u64, Vec<Vec<u8>>) {
    let mut entries: Vec<Vec<u8>> = blob
        .chunks
        .iter()
        .map(|(id, entry)| rmp_serde::to_vec(&(id, entry)).unwrap())
        .collect();
    entries.sort_unstable();
    (blob.generation, entries)
}

fuzz_target!(|data: &[u8]| {
    let streamed = vykar_core::compress::decode_metadata_msgpack::<IndexBlob>(data);
    let buffered = vykar_core::compress::decompress_metadata(data).and_then(|raw| {
        let blob: IndexBlob = rmp_serde::from_slice(&raw)?;
        Ok((blob, raw.len() as u64))
    });
    match (streamed, buffered) {
        (Ok((s, s_len)), Ok((b, b_len))) => {
            assert_eq!(s_len, b_len);
            assert_eq!(canonical(&s), canonical(&b));
        }
        (Err(_), Err(_)) => {}
        (s, b) => panic!(
            "streaming and buffered decode disagree: {:?} vs {:?}",
            s.map(|(_, n)| n),
            b.map(|(_, n)| n)
        ),
    }
});
