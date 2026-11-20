#![no_main]
//! The repository `config`: unencrypted and unauthenticated even in an
//! encrypted repository, so anyone with write access to storage controls it.
//! Decoded and checked as open does, then used as backup uses it.

use libfuzzer_sys::fuzz_target;
use vykar_core::repo::pack::{compute_data_pack_target, compute_tree_pack_target};
use vykar_core::repo::RepoConfig;

/// Fixed input for the chunkers: varied enough to produce cuts.
fn chunker_input() -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    (0..96 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fuzz_target!(|data: &[u8]| {
    let Ok(config) = rmp_serde::from_slice::<RepoConfig>(data) else {
        return;
    };
    let Ok(format) = config.validate() else {
        return;
    };
    let _ = vykar_core::fuzzing::plaintext_chunk_id_key(&config.id, format);

    for packs in [0, 1, 50, 10_000, usize::MAX] {
        let target = compute_data_pack_target(packs, config.min_pack_size, config.max_pack_size);
        if config.min_pack_size <= config.max_pack_size {
            assert!(target >= config.min_pack_size as usize);
            assert!(target <= config.max_pack_size as usize);
        }
    }
    let _ = compute_tree_pack_target(config.min_pack_size);

    // Backup's gate for the stored chunker parameters.
    let params = &config.chunker_params;
    if params.validate_stored().is_err() {
        return;
    }
    let input = chunker_input();
    let lengths = vykar_core::fuzzing::chunk_lengths(&input, params).unwrap();
    assert_eq!(lengths.iter().sum::<usize>(), input.len());
    if let Some((_, rest)) = lengths.split_last() {
        for &len in rest {
            assert!(
                len >= params.min_size as usize,
                "{len} < {}",
                params.min_size
            );
        }
    }
    for &len in &lengths {
        assert!(len > 0 && len <= params.max_size as usize);
    }
    // The resumable reader cuts exactly where fastcdc's own streaming
    // chunker (used for command dumps) does.
    let streamed = vykar_core::fuzzing::stream_chunk_lengths(&input, params).unwrap();
    assert_eq!(lengths, streamed);
});
