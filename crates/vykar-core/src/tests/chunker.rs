use std::io::{Cursor, Read};

use crate::chunker::{chunk_data, chunk_stream, chunk_stream_bounded};
use crate::config::ChunkerConfig;

fn test_config() -> ChunkerConfig {
    ChunkerConfig {
        min_size: 256,
        avg_size: 1024,
        max_size: 4096,
    }
}

#[test]
fn chunks_cover_entire_input() {
    let data = vec![0x42u8; 10_000];
    let config = test_config();
    let chunks = chunk_data(&data, &config);

    // Verify no gaps or overlaps
    let mut expected_offset = 0;
    for (offset, length) in &chunks {
        assert_eq!(
            *offset, expected_offset,
            "gap or overlap at offset {offset}"
        );
        expected_offset = offset + length;
    }
    assert_eq!(
        expected_offset,
        data.len(),
        "chunks don't cover entire input"
    );
}

#[test]
fn deterministic_chunking() {
    let data = vec![0x42u8; 10_000];
    let config = test_config();
    let chunks1 = chunk_data(&data, &config);
    let chunks2 = chunk_data(&data, &config);
    assert_eq!(chunks1, chunks2);
}

#[test]
fn respects_max_size() {
    let data = vec![0x42u8; 20_000];
    let config = test_config();
    let chunks = chunk_data(&data, &config);
    for (_, length) in &chunks {
        assert!(
            *length <= config.max_size as usize,
            "chunk size {} exceeds max_size {}",
            length,
            config.max_size
        );
    }
}

#[test]
fn small_data_single_chunk() {
    let data = vec![0x42u8; 100]; // Smaller than min_size
    let config = test_config();
    let chunks = chunk_data(&data, &config);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], (0, 100));
}

#[test]
fn empty_data_no_chunks() {
    let config = test_config();
    let chunks = chunk_data(b"", &config);
    assert!(chunks.is_empty());
}

#[test]
fn stream_chunking_matches_slice_chunking() {
    let data = vec![0x42u8; 50_000];
    let config = test_config();
    let expected = chunk_data(&data, &config);

    let actual: Vec<(usize, usize)> = chunk_stream(Cursor::new(data), &config)
        .map(|result| {
            let chunk = result.expect("stream chunking should succeed");
            (chunk.offset as usize, chunk.length)
        })
        .collect();

    assert_eq!(actual, expected);
}

#[test]
fn bounded_stream_preserves_chunks_and_read_limit() {
    use rand::{RngExt, SeedableRng};

    let config = test_config();
    let mut rng = rand::rngs::StdRng::seed_from_u64(1909);
    let random: Vec<u8> = (0..20_000).map(|_| rng.random()).collect();
    for data in [random, vec![0; 20_000]] {
        for limit in [
            0, 1, 255, 256, 257, 1023, 1024, 1025, 3001, 4095, 4096, 4097, 19_999,
        ] {
            let mut source = Cursor::new(&data);
            let actual: Vec<_> = chunk_stream_bounded((&mut source).take(limit), &config)
                .map(|result| {
                    let chunk = result.expect("bounded chunking should succeed");
                    assert_eq!(
                        chunk.data,
                        data[chunk.offset as usize..chunk.offset as usize + chunk.length]
                    );
                    (chunk.offset as usize, chunk.length)
                })
                .collect();
            assert_eq!(actual, chunk_data(&data[..limit as usize], &config));
            assert_eq!(
                source.position(),
                limit,
                "must not read into the next segment"
            );
        }
        // A short underlying reader still has the original chunk boundaries.
        let actual: Vec<_> = chunk_stream_bounded(Cursor::new(&data).take(30_000), &config)
            .map(|result| {
                let chunk = result.expect("short bounded reader should succeed");
                (chunk.offset as usize, chunk.length)
            })
            .collect();
        assert_eq!(actual, chunk_data(&data, &config));
    }
}
