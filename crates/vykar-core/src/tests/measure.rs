//! `measure::listing::analyze` end to end against a local repository.

use std::path::Path;

use crate::measure::listing::{ListingOptions, ListingReport, analyze};
use crate::tests::helpers::{backup_single_source, init_repo, open_local_repo};
use crate::tests::tree_cut::make_tree_cut_source;

/// `snap1` of the tree-cut source, then `snap2` with 50 files added to `dir-07`.
fn two_snapshot_repo(root: &Path) -> std::path::PathBuf {
    let repo_dir = root.join("repo");
    let source_dir = root.join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = init_repo(&repo_dir);
    config.xattrs.enabled = false;
    make_tree_cut_source(&source_dir);
    backup_single_source(&config, &source_dir, "src", "snap1");
    for f in 0..50 {
        std::fs::write(
            source_dir.join(format!("dir-07/added-{f:02}.txt")),
            format!("added {f}"),
        )
        .unwrap();
    }
    backup_single_source(&config, &source_dir, "src", "snap2");
    repo_dir
}

fn run(repo_dir: &Path, opts: ListingOptions) -> ListingReport {
    let mut repo = open_local_repo(repo_dir);
    analyze(&mut repo, &opts).unwrap()
}

#[test]
fn listing_stats_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = two_snapshot_repo(tmp.path());
    let r = run(
        &repo_dir,
        ListingOptions {
            label: Some("src".into()),
            ..ListingOptions::default()
        },
    );

    assert_eq!(r.snapshots.len(), 2);
    assert_eq!(r.shape_snapshot.as_deref(), Some("snap2"));
    let shape = r.shape.as_ref().unwrap();
    assert_eq!(shape.entries, 24_090);
    assert_eq!(shape.directories, 41);
    assert_eq!(shape.entries_per_dir.max, 650);
    assert!(shape.bytes_per_dir.is_some());

    assert_eq!(r.pairs.len(), 1);
    let pair = &r.pairs[0];
    let pointers = r.snapshots[1].pointers;
    assert!(
        pair.new_chunks > 0 && pair.new_chunks < pointers,
        "{} new of {pointers}",
        pair.new_chunks
    );
    assert!(pair.positions.iter().all(|p| u64::from(*p) < pointers));
    assert!(pair.new_stored_bytes > 0);
    assert_eq!(pair.leaves.len(), 3);
    for l in &pair.leaves {
        assert!(l.new_leaves <= l.leaves, "target {}", l.target);
        assert_eq!(l.upload_bytes, l.root_bytes + l.new_leaf_bytes);
    }
    assert_eq!(r.decoded_chunks, r.distinct_tree_chunks);
    assert!(r.key_lengths.is_some());
}

#[test]
fn listing_stats_assume_key_len_reads_no_chunks() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = two_snapshot_repo(tmp.path());
    let decoded = run(
        &repo_dir,
        ListingOptions {
            label: Some("src".into()),
            ..ListingOptions::default()
        },
    );
    let assumed = run(
        &repo_dir,
        ListingOptions {
            label: Some("src".into()),
            shape: false,
            assume_key_len: Some(40),
            ..ListingOptions::default()
        },
    );
    assert_eq!(assumed.decoded_chunks, 0);
    assert!(assumed.shape.is_none());
    assert!(assumed.key_lengths.is_none());
    assert_eq!(assumed.pairs[0].positions, decoded.pairs[0].positions);
}

#[test]
fn listing_stats_pairs_stay_within_label() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = init_repo(&repo_dir);
    config.xattrs.enabled = false;
    let (src_a, src_b) = (tmp.path().join("a"), tmp.path().join("b"));
    for dir in [&src_a, &src_b] {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("f"), "x").unwrap();
    }
    backup_single_source(&config, &src_a, "a", "a1");
    backup_single_source(&config, &src_b, "b", "b1");
    std::fs::write(src_a.join("g"), "y").unwrap();
    backup_single_source(&config, &src_a, "a", "a2");
    backup_single_source(&config, &src_b, "b", "b2");

    let r = run(&repo_dir, ListingOptions::default());
    assert_eq!(r.snapshots.len(), 4);
    let mut pairs: Vec<(&str, &str, &str)> = r
        .pairs
        .iter()
        .map(|p| (p.source_label.as_str(), p.prev.as_str(), p.cur.as_str()))
        .collect();
    pairs.sort_unstable();
    assert_eq!(pairs, vec![("a", "a1", "a2"), ("b", "b1", "b2")]);
}
