//! `LazyItemStream`: the item stream decoded chunk by chunk, with items
//! straddling chunk and range-read boundaries.

use std::io::Read;

use vykar_types::chunk_id::ChunkId;
use vykar_types::error::VykarError;
use vykar_types::pack_id::PackId;

use crate::commands::list::{LazyItemStream, for_each_decoded_item};
use crate::compress::Compression;
use crate::repo::control::CancelToken;
use crate::repo::pack::PackType;
use crate::repo::{Repository, WriteSessionOptions};
use crate::snapshot::item::{Item, ItemType};
use crate::testutil::{PutLog, RecordingBackend, init_test_environment};

type Loc = (ChunkId, PackId, u64, u32);

fn repo_on_counting_backend() -> (Repository, PutLog) {
    init_test_environment();
    let (backend, log) = RecordingBackend::new();
    let mut repo = Repository::init(
        Box::new(backend),
        crate::repo::EncryptionMode::None,
        crate::repo::ChunkerParams::default(),
        None,
        None,
        None,
    )
    .expect("init");
    repo.begin_write_session(&WriteSessionOptions::default())
        .expect("begin write session");
    (repo, log)
}

fn aes_repo_on_counting_backend() -> (Repository, PutLog) {
    init_test_environment();
    let (backend, log) = RecordingBackend::new();
    let mut repo = Repository::init(
        Box::new(backend),
        crate::repo::EncryptionMode::Aes256Gcm,
        crate::repo::ChunkerParams::default(),
        Some("test-pass"),
        None,
        None,
    )
    .expect("init");
    repo.begin_write_session(&WriteSessionOptions::default())
        .expect("begin write session");
    (repo, log)
}

fn item(i: usize) -> Item {
    Item {
        path: format!("dir/file-{i:05}-{}", "x".repeat(i % 37)),
        entry_type: ItemType::RegularFile,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        user: None,
        group: None,
        mtime: i as i64,
        atime: None,
        ctime: None,
        size: 0,
        chunks: Vec::new(),
        link_target: None,
        xattrs: None,
        raw_names: None,
        hardlink: None,
    }
}

/// Encode `n` items, cut the stream every `cut` bytes (so items straddle the
/// cuts), store each piece as a tree chunk and return the stream plus the
/// chunk locations in stream order.
fn stored_stream(repo: &mut Repository, n: usize, cut: usize) -> (Vec<u8>, Vec<Loc>) {
    let mut stream = Vec::new();
    for i in 0..n {
        stream.extend_from_slice(&rmp_serde::to_vec(&item(i)).unwrap());
    }
    let ids: Vec<ChunkId> = stream
        .chunks(cut)
        .map(|piece| {
            repo.store_chunk(piece, Compression::None, PackType::Tree)
                .unwrap()
                .0
        })
        .collect();
    // Chunks stored in a session are in the index once it is saved.
    repo.save_state().unwrap();
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    let locs = ids
        .iter()
        .map(|id| {
            let e = repo.chunk_index().get(id).unwrap();
            (*id, e.pack_id, e.pack_offset, e.stored_size)
        })
        .collect();
    (stream, locs)
}

fn paths_of(stream: &mut LazyItemStream<'_>) -> vykar_types::error::Result<Vec<String>> {
    let mut paths = Vec::new();
    stream.for_each_item(|item| {
        paths.push(item.path);
        Ok(())
    })?;
    Ok(paths)
}

fn eager_paths(stream: &[u8]) -> Vec<String> {
    let mut paths = Vec::new();
    for_each_decoded_item(stream, |item| {
        paths.push(item.path);
        Ok(())
    })
    .unwrap();
    paths
}

#[test]
fn lazy_stream_matches_eager_decode_across_windows() {
    let (mut repo, log) = repo_on_counting_backend();
    // 33-byte cuts: almost every item straddles a chunk boundary.
    let (stream, locs) = stored_stream(&mut repo, 500, 33);
    assert!(locs.len() > 100);
    let expected = eager_paths(&stream);
    assert_eq!(expected.len(), 500);

    // Uncapped windows: one range read per run of chunks that sit forward in
    // the same pack (dedup'd pieces point back to earlier blobs).
    let pack_runs = 1 + locs
        .windows(2)
        .filter(|w| w[0].1 != w[1].1 || w[1].2 < w[0].2 + u64::from(w[0].3))
        .count();
    log.reset_get_range_count();
    let never = CancelToken::new();
    let mut lazy = LazyItemStream::for_test(&repo, locs.clone(), false, u64::MAX, &never);
    assert_eq!(paths_of(&mut lazy).unwrap(), expected);
    assert_eq!(log.get_range_count() as usize, pack_runs);

    // A window cap of ~4 chunks: many range reads, same items.
    let cap = u64::from(locs[0].3) * 4;
    log.reset_get_range_count();
    let mut lazy = LazyItemStream::for_test(&repo, locs.clone(), false, cap, &never);
    assert_eq!(paths_of(&mut lazy).unwrap(), expected);
    let reads = log.get_range_count() as usize;
    assert!(
        reads >= locs.len() / 5 && reads > pack_runs,
        "expected one read per ~4 chunks, got {reads} for {} chunks",
        locs.len()
    );
}

#[test]
fn lazy_stream_empty_is_empty() {
    let (repo, log) = repo_on_counting_backend();
    let never = CancelToken::new();
    let mut lazy = LazyItemStream::for_test(&repo, Vec::new(), false, u64::MAX, &never);
    assert!(paths_of(&mut lazy).unwrap().is_empty());
    assert_eq!(log.get_range_count(), 0);
}

#[test]
fn lazy_stream_surfaces_chunk_id_mismatch_not_a_decode_error() {
    let (mut repo, _log) = repo_on_counting_backend();
    let (_stream, mut locs) = stored_stream(&mut repo, 200, 64);
    // Point chunk 3's location at chunk 4's bytes: on a plaintext repo only
    // the chunk-ID check can notice.
    let (_, p, o, s) = locs[4];
    locs[3] = (locs[3].0, p, o, s);
    let never = CancelToken::new();
    let mut lazy = LazyItemStream::for_test(&repo, locs, false, u64::MAX, &never);
    let err = paths_of(&mut lazy).unwrap_err();
    assert!(
        matches!(err, VykarError::InvalidFormat(_)),
        "expected the chunk-ID mismatch, got {err:?}"
    );
}

#[test]
fn lazy_stream_polls_shutdown_between_range_reads() {
    let (mut repo, log) = repo_on_counting_backend();
    let (_stream, locs) = stored_stream(&mut repo, 300, 64);
    let cap = u64::from(locs[0].3) * 2;
    let shutdown = CancelToken::new();
    let mut lazy = LazyItemStream::for_test(&repo, locs, false, cap, &shutdown);
    log.reset_get_range_count();
    let mut seen = 0usize;
    let err = lazy
        .for_each_item(|_| {
            seen += 1;
            shutdown.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, VykarError::Interrupted), "got {err:?}");
    assert!(seen >= 1);
    assert_eq!(
        log.get_range_count(),
        1,
        "no read after the flag was raised"
    );
}

/// A location that no longer holds its chunk (a compaction moved it after
/// the index was loaded) is re-resolved once against the index at a fresh
/// head, and the read goes on.
#[test]
fn lazy_stream_retries_a_bad_location_against_a_fresh_index() {
    let (mut repo, _log) = repo_on_counting_backend();
    let (stream, mut locs) = stored_stream(&mut repo, 200, 64);
    // A location that decodes to the wrong chunk.
    let (_, p, o, s) = locs[2];
    locs[5] = (locs[5].0, p, o, s);

    let never = CancelToken::new();
    let mut lazy = LazyItemStream::for_test(&repo, locs.clone(), true, u64::MAX, &never);
    assert!(!lazy.retried());
    assert_eq!(paths_of(&mut lazy).unwrap(), eager_paths(&stream));
    assert!(lazy.retried());

    // Without the retry the error surfaces.
    let mut lazy = LazyItemStream::for_test(&repo, locs, false, u64::MAX, &never);
    assert!(paths_of(&mut lazy).is_err());
}

#[test]
fn lazy_stream_reads_an_aead_repo() {
    let (mut repo, _log) = aes_repo_on_counting_backend();
    let cut = 64;
    let (stream, mut locs) = stored_stream(&mut repo, 200, cut);
    let mut pieces: Vec<&[u8]> = stream.chunks(cut).collect();
    locs.insert(4, locs[3]);
    pieces.insert(4, pieces[3]);

    let cap = u64::from(locs[0].3) * 4;
    let never = CancelToken::new();
    let mut lazy = LazyItemStream::for_test(&repo, locs, false, cap, &never);
    let mut bytes = Vec::new();
    lazy.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, pieces.concat());
}
