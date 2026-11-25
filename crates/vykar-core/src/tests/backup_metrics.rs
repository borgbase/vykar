//! The backup's diagnostic counters (`BackupOutcome::instrumentation`)
//! against what reached the repository.

use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::commands::backup::BackupOutcome;
use crate::tests::helpers::{make_test_config, try_backup_single_source};

/// Six 8 KiB files of pseudo-random content (no chunk repeats within or
/// across files), all above the test chunker's `min_size`, so none takes the
/// whole-file small read.
fn write_distinct_files(src: &Path) {
    for i in 0..6u64 {
        let mut x = 0x9e37_79b9_7f4a_7c15 ^ (i + 1);
        let data: Vec<u8> = (0..8 * 1024)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.to_le_bytes()[0]
            })
            .collect();
        fs::write(src.join(format!("f{i}.bin")), data).unwrap();
    }
}

fn backup_with_threads(dir: &Path, threads: usize, small_files: bool) -> BackupOutcome {
    let repo_dir = dir.join("repo");
    let src = dir.join("src");
    fs::create_dir_all(&src).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.limits.threads = threads;
    config.chunker.min_size = 1024;
    config.chunker.avg_size = 2048;
    config.chunker.max_size = 4096;
    crate::commands::init::run(&config, None).unwrap();

    write_distinct_files(&src);
    if small_files {
        for i in 0..3u8 {
            fs::write(src.join(format!("s{i}.txt")), vec![i; 100]).unwrap();
        }
    }
    try_backup_single_source(&config, &src, "test", "snap").unwrap()
}

/// (count, total bytes) of the pack objects under `<repo>/packs/<shard>/`.
fn packs_on_disk(repo_dir: &Path) -> (u64, u64) {
    let mut count = 0;
    let mut bytes = 0;
    for shard in fs::read_dir(repo_dir.join("packs")).unwrap() {
        let shard = shard.unwrap();
        if !shard.file_type().unwrap().is_dir() {
            continue;
        }
        for pack in fs::read_dir(shard.path()).unwrap() {
            let pack = pack.unwrap();
            count += 1;
            bytes += pack.metadata().unwrap().len();
        }
    }
    (count, bytes)
}

#[test]
fn pack_counters_match_storage() {
    for threads in [1, 2] {
        let dir = tempfile::tempdir().unwrap();
        let outcome = backup_with_threads(dir.path(), threads, true);
        let m = &outcome.instrumentation;
        let w = &m.write;

        let (count, bytes) = packs_on_disk(&dir.path().join("repo"));
        assert_eq!(w.data_packs + w.tree_packs, count, "threads {threads}");
        assert_eq!(
            w.data_pack_bytes + w.tree_pack_bytes,
            bytes,
            "threads {threads}"
        );
        assert_eq!(m.threads, threads);
        assert!(m.pipeline.is_some());

        let p = m.pipeline.unwrap();
        assert_eq!(p.workers, threads);
        assert!(p.consumer_recv_wait <= p.wall);
        assert!(p.consumer_busy <= p.wall);
        assert!(p.consumer_cap_wait <= w.upload_cap_wait);
        assert!(p.worker_busy > Duration::ZERO);
        assert!(p.worker_transform <= p.worker_busy);
    }
}

/// Issue 1 of plan 02: at threads = 1 the old sequential path force-sealed a
/// pack per new file above `min_size`. The pipeline seals the same packs at
/// every thread count.
#[test]
fn pack_layout_does_not_depend_on_threads() {
    let mut tree_packs = Vec::new();
    for threads in [1, 2, 4] {
        let dir = tempfile::tempdir().unwrap();
        let w = backup_with_threads(dir.path(), threads, false)
            .instrumentation
            .write;
        assert_eq!(w.data_packs, 1, "threads {threads}");
        tree_packs.push(w.tree_packs);
    }
    assert!(
        tree_packs.windows(2).all(|w| w[0] == w[1]),
        "tree packs differ across thread counts: {tree_packs:?}"
    );
}
