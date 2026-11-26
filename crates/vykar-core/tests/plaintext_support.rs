//! The plaintext (`encryption: none`) product path end to end, which the
//! rest of the suite stopped exercising when its helpers moved to encrypted
//! repositories (10-single-protocol step 2): init, two backups, list,
//! restore, delete, a compact that repacks, a verifying check and a restore
//! after it.

// Plaintext-only: 10-single-protocol step 3 removes this.

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]

mod common;

use std::path::Path;
use std::time::Duration;

use vykar_core::commands;
use vykar_core::config::VykarConfig;
use vykar_core::repo::control::CancelToken;

use crate::common::{backup_source, make_plaintext_config};

/// Deterministic incompressible bytes (xorshift64) from `seed`.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// Every file under `dir`, by path relative to it, with its bytes.
fn tree(dir: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let rel = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                out.push((rel, std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// Move the mtime of every file under `dir` back by `by` (ages on the
/// local store's clock are mtimes).
fn age(dir: &Path, by: Duration) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            age(&path, by);
        } else {
            let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
            std::fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(mtime - by)
                .unwrap();
        }
    }
}

fn restore(config: &VykarConfig, snapshot: &str, dest: &Path) {
    commands::restore::run(
        config,
        None,
        snapshot,
        dest.to_str().unwrap(),
        None,
        false,
        false,
        &CancelToken::new(),
    )
    .unwrap();
}

/// The restored snapshot of `src` under `dest`: restore recreates the
/// source's absolute path below the destination.
fn restored_tree(dest: &Path, src: &Path) -> Vec<(String, Vec<u8>)> {
    let rel = src.strip_prefix("/").unwrap_or(src);
    let under = dest.join(rel);
    if under.is_dir() {
        tree(&under)
    } else {
        tree(dest)
    }
}

#[test]
fn a_plaintext_repository_round_trips() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let src1 = tmp.path().join("src1");
    let src2 = tmp.path().join("src2");
    for dir in [&repo_dir, &src1, &src2] {
        std::fs::create_dir_all(dir).unwrap();
    }
    std::fs::write(src1.join("a.bin"), noise(1, 200_000)).unwrap();
    std::fs::write(src1.join("b.bin"), noise(2, 50_000)).unwrap();
    std::fs::write(src2.join("c.bin"), noise(3, 60_000)).unwrap();
    std::fs::write(src2.join("b.bin"), noise(2, 50_000)).unwrap();

    let mut config = make_plaintext_config(&repo_dir);
    config.cache_dir = Some(tmp.path().join("cache").to_string_lossy().into_owned());
    commands::init::run(&config, None).unwrap();
    backup_source(&config, &src1, "src1", "s1", None, false);
    backup_source(&config, &src2, "src2", "s2", None, false);

    let listed: Vec<String> = commands::list::list_snapshots(&config, None)
        .unwrap()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(listed, ["s1", "s2"]);

    for (name, src) in [("s1", &src1), ("s2", &src2)] {
        let dest = tmp.path().join(format!("restore-{name}"));
        restore(&config, name, &dest);
        assert_eq!(restored_tree(&dest, src), tree(src), "{name}");
    }

    commands::delete::run(&config, None, &["s1"], false, &CancelToken::new()).unwrap();
    // Past T_pack (12 h): s1's segment and packs are old, so the sweep
    // takes s1's chunks and the compact repacks the half-dead pack.
    let past_t_pack = Duration::from_secs(13 * 3600);
    age(&repo_dir.join("index.d"), past_t_pack);
    age(&repo_dir.join("packs"), past_t_pack);
    let stats =
        commands::compact::run(&config, None, 0.0, None, false, &CancelToken::new()).unwrap();
    assert!(stats.packs_repacked > 0, "{stats:?}");

    let check = commands::check::run(&config, None, true, false).unwrap();
    assert!(check.errors.is_empty(), "{:?}", check.errors);

    let dest = tmp.path().join("restore-s2-after");
    restore(&config, "s2", &dest);
    assert_eq!(restored_tree(&dest, &src2), tree(&src2));
}
