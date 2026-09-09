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

/// Deterministic pseudo-random bytes, reproduced verbatim by the generator
/// that produced the pinned cut points below so the vectors can be
/// regenerated if the chunker parameters ever legitimately change.
fn seeded_data(seed: u64, len: usize) -> Vec<u8> {
    let mut s = seed;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        out.extend_from_slice(&s.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Cut points captured under fastcdc 4.0.1 at the shipped default parameters
/// (512 KiB / 2 MiB / 8 MiB).
const GOLDEN_DEFAULT_PARAMS: &[(usize, usize)] = &[
    (0, 2116973),
    (2116973, 3203000),
    (5319973, 2206160),
    (7526133, 4161992),
    (11688125, 2204428),
    (13892553, 1322709),
    (15215262, 4257310),
    (19472572, 3084359),
    (22556931, 5199617),
    (27756548, 777729),
    (28534277, 2224983),
    (30759260, 6651058),
    (37410318, 2422009),
    (39832327, 853460),
    (40685787, 893025),
    (41578812, 364228),
];

/// Cut points captured under fastcdc 4.0.1 at the small parameters used
/// throughout this module (256 / 1024 / 4096).
const GOLDEN_SMALL_PARAMS: &[(usize, usize)] = &[
    (0, 2070),
    (2070, 2260),
    (4330, 2412),
    (6742, 1243),
    (7985, 1306),
    (9291, 505),
    (9796, 1572),
    (11368, 1458),
    (12826, 1885),
    (14711, 1105),
    (15816, 1063),
    (16879, 357),
    (17236, 1550),
    (18786, 1299),
    (20085, 1678),
    (21763, 586),
    (22349, 1220),
    (23569, 3043),
    (26612, 1411),
    (28023, 1193),
    (29216, 1784),
    (31000, 1927),
    (32927, 1246),
    (34173, 1085),
    (35258, 3615),
    (38873, 969),
    (39842, 1090),
    (40932, 696),
    (41628, 655),
    (42283, 343),
    (42626, 2311),
    (44937, 1088),
    (46025, 1940),
    (47965, 2352),
    (50317, 795),
    (51112, 1155),
    (52267, 1222),
    (53489, 1153),
    (54642, 1475),
    (56117, 1431),
    (57548, 1268),
    (58816, 1816),
    (60632, 2475),
    (63107, 2897),
    (66004, 1152),
    (67156, 1030),
    (68186, 1051),
    (69237, 1143),
    (70380, 570),
    (70950, 851),
    (71801, 1200),
    (73001, 889),
    (73890, 290),
    (74180, 1162),
    (75342, 1108),
    (76450, 666),
    (77116, 1059),
    (78175, 260),
    (78435, 1030),
    (79465, 1554),
    (81019, 1926),
    (82945, 1470),
    (84415, 682),
    (85097, 1042),
    (86139, 2234),
    (88373, 1985),
    (90358, 1086),
    (91444, 1189),
    (92633, 1002),
    (93635, 3366),
    (97001, 1378),
    (98379, 395),
    (98774, 1512),
    (100286, 324),
    (100610, 849),
    (101459, 276),
    (101735, 665),
];

/// Chunk boundaries are repository format, not an implementation detail: they
/// determine chunk identity, so a shift would silently stop existing
/// repositories deduplicating and rewrite every backup from scratch.
///
/// The other tests in this module only compare fastcdc against itself, so they
/// cannot catch a cut-point change. These vectors were generated under fastcdc
/// 4.0.1 and are the guard for every future chunker bump. A failure here means
/// dedup compatibility with existing repositories is broken — do not
/// regenerate the vectors to make it pass.
#[test]
fn cut_points_match_pinned_vectors_at_default_params() {
    let data = seeded_data(0x5645_4b41_5f43_4443, 40 * 1024 * 1024);
    let chunks = chunk_data(&data, &ChunkerConfig::default());
    assert_eq!(chunks, GOLDEN_DEFAULT_PARAMS);
}

#[test]
fn cut_points_match_pinned_vectors_at_small_params() {
    let data = seeded_data(0x0123_4567_89ab_cdef, 100 * 1024);
    let chunks = chunk_data(&data, &test_config());
    assert_eq!(chunks, GOLDEN_SMALL_PARAMS);
}
