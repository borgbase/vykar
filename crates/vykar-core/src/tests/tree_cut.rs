//! Content-defined cut points of the item stream, end to end through backup.
//!
//! The cut key derives from the repository's chunk-ID key, which is fresh per
//! `init`, so cut positions differ between runs. The bounds have margin; a
//! failure should be investigated, not fixed by loosening a bound.

use std::collections::HashSet;
use std::path::Path;

use crate::commands::list::{for_each_decoded_item, load_snapshot_item_stream, load_snapshot_meta};
use crate::repo::control::CancelToken;
use crate::tests::helpers::{backup_single_source, init_repo, open_local_repo};
use vykar_types::chunk_id::ChunkId;

/// `ITEM_CUT_MIN` in `commands/backup/item_cut.rs`.
const MIN_CHUNK: usize = 8 * 1024;

/// 40 dirs × 600 small files with unique content: about 100 tree chunks, so
/// the per-run variation in chunk count stays well inside the test bounds.
pub(crate) fn make_tree_cut_source(source_dir: &Path) {
    for d in 0..40 {
        let dir = source_dir.join(format!("dir-{d:02}"));
        std::fs::create_dir_all(&dir).unwrap();
        for f in 0..600 {
            std::fs::write(dir.join(format!("file-{f:04}.txt")), format!("{d}/{f}")).unwrap();
        }
    }
}

fn item_ptrs(repo_dir: &Path, name: &str) -> Vec<ChunkId> {
    let repo = open_local_repo(repo_dir);
    load_snapshot_meta(&repo, name).unwrap().item_ptrs
}

#[test]
fn every_tree_chunk_holds_whole_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = init_repo(&repo_dir);
    config.xattrs.enabled = false;
    make_tree_cut_source(&source_dir);
    backup_single_source(&config, &source_dir, "src", "snap1");

    let mut repo = open_local_repo(&repo_dir);
    let ptrs = load_snapshot_meta(&repo, "snap1").unwrap().item_ptrs;
    assert!(ptrs.len() >= 8, "only {} tree chunks", ptrs.len());

    let mut concat = Vec::new();
    for (i, id) in ptrs.iter().enumerate() {
        let chunk = repo.read_chunk(id).unwrap();
        for_each_decoded_item(&chunk, |_| Ok(()))
            .unwrap_or_else(|e| panic!("chunk {i} does not hold whole entries: {e}"));
        if i + 1 < ptrs.len() {
            assert!(
                chunk.len() >= MIN_CHUNK,
                "chunk {i} is {} bytes",
                chunk.len()
            );
        }
        concat.extend_from_slice(&chunk);
    }
    let stream = load_snapshot_item_stream(&mut repo, "snap1", &CancelToken::new()).unwrap();
    assert!(
        concat == stream,
        "tree chunks do not concatenate to the stream"
    );

    let mean = stream.len() / ptrs.len();
    assert!(
        (16 * 1024..=55 * 1024).contains(&mean),
        "mean tree chunk size {mean} over {} chunks",
        ptrs.len()
    );
}

#[test]
fn backup_after_insert_rewrites_only_nearby_tree_chunks() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = init_repo(&repo_dir);
    config.xattrs.enabled = false;
    make_tree_cut_source(&source_dir);

    let check = |prev: &[ChunkId], next: &[ChunkId], step: &str| {
        let prev: HashSet<&ChunkId> = prev.iter().collect();
        let new = next.iter().filter(|id| !prev.contains(id)).count();
        assert!(next.len() >= 8, "{step}: only {} tree chunks", next.len());
        assert!(new >= 1, "{step}: no new tree chunk");
        assert!(
            new <= 4 && new * 3 <= next.len(),
            "{step}: {new} of {} tree chunks new",
            next.len()
        );
    };

    backup_single_source(&config, &source_dir, "src", "snap1");
    let snap1 = item_ptrs(&repo_dir, "snap1");

    // Also changes dir-05's mtime, so two nearby entries change.
    std::fs::write(source_dir.join("dir-05").join("file-0100a.txt"), b"new").unwrap();
    backup_single_source(&config, &source_dir, "src", "snap2");
    let snap2 = item_ptrs(&repo_dir, "snap2");
    check(&snap1, &snap2, "insert");

    std::fs::remove_file(source_dir.join("dir-30").join("file-0050.txt")).unwrap();
    backup_single_source(&config, &source_dir, "src", "snap3");
    let snap3 = item_ptrs(&repo_dir, "snap3");
    check(&snap2, &snap3, "remove");
}
