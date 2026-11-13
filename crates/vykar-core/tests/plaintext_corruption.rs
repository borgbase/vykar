//! Plaintext (`encryption: none`) repos have no AEAD tag, so chunk IDs are
//! recomputed on every read. A same-length bit flip in a stored chunk must be
//! rejected by restore (file data) and list (item streams) without `--verify`.

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
#![allow(clippy::indexing_slicing)]

mod common;

use std::path::Path;

use vykar_core::commands;
use vykar_core::config::VykarConfig;
use vykar_types::chunk_id::ChunkId;

use crate::common::{backup_source, make_test_config, open_local_repo};

/// Deterministic incompressible bytes (xorshift64).
fn noise(len: usize) -> Vec<u8> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// Init a plaintext repo and back up one file under `snap`.
fn setup(tmp: &Path) -> (VykarConfig, std::path::PathBuf) {
    let repo_dir = tmp.join("repo");
    let src_dir = tmp.join("src");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::write(src_dir.join("data.bin"), noise(64 * 1024)).unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();
    backup_source(&config, &src_dir, "src", "snap", None, false);
    (config, repo_dir)
}

/// Flip the middle byte of `id`'s stored blob in its on-disk pack. Backups use
/// `Compression::None`, so the blob still decodes at the same length.
fn flip_byte_in_chunk(repo_dir: &Path, id: &ChunkId) {
    let repo = open_local_repo(repo_dir, None);
    let entry = *repo.chunk_index().get(id).unwrap();
    let path = repo_dir.join(entry.pack_id.storage_key());
    let mut pack = std::fs::read(&path).unwrap();
    let pos = entry.pack_offset as usize + entry.stored_size as usize / 2;
    pack[pos] ^= 0x01;
    std::fs::write(&path, pack).unwrap();
}

#[test]
fn plaintext_restore_rejects_corrupt_file_chunk_without_verify() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, repo_dir) = setup(tmp.path());

    let items = commands::list::list_snapshot_items(&config, None, "snap").unwrap();
    let file = items.iter().find(|i| i.path.ends_with("data.bin")).unwrap();
    assert_eq!(file.chunks.len(), 1, "fixture should be a single chunk");
    flip_byte_in_chunk(&repo_dir, &file.chunks[0].id);

    let dest = tmp.path().join("restore");
    let err = commands::restore::run(
        &config,
        None,
        "snap",
        dest.to_str().unwrap(),
        None,
        false,
        false,
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("hash mismatch"), "got: {err}");

    // The mismatch is raised in Phase 4, after the staging dir was created
    // inside `dest`; so `dest` must exist and be empty, i.e. cleaned up rather
    // than never staged.
    assert!(dest.is_dir(), "restore should have staged under {dest:?}");
    let leftovers: Vec<_> = std::fs::read_dir(&dest)
        .unwrap()
        .flatten()
        .map(|e| e.file_name())
        .collect();
    assert!(
        leftovers.is_empty(),
        "no restored file or staging dir may remain: {leftovers:?}"
    );
}

#[test]
fn plaintext_list_rejects_corrupt_item_stream_chunk() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, repo_dir) = setup(tmp.path());

    let item_ptr = {
        let repo = open_local_repo(&repo_dir, None);
        let meta = commands::list::load_snapshot_meta(&repo, "snap").unwrap();
        meta.item_ptrs[0]
    };
    flip_byte_in_chunk(&repo_dir, &item_ptr);

    let err = commands::list::list_snapshot_items(&config, None, "snap")
        .unwrap_err()
        .to_string();
    assert!(err.contains("hash mismatch"), "got: {err}");
}
