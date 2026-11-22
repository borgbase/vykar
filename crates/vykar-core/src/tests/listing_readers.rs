//! The listing readers of 06 step 2 against today's whole-stream decode:
//! `list` reading a batch of tree chunks at a time, the chunk-aware diff and
//! find decoding each distinct tree chunk once.

use std::collections::{BTreeMap, HashSet};

use vykar_types::chunk_id::ChunkId;
use vykar_types::error::VykarError;

use crate::commands;
use crate::commands::diff::{DiffChangeKind, DiffEntry, DiffStats, FullDiffReason, diff_on_repo};
use crate::commands::find::{
    FileStatus, FindFilter, FindHit, FindStats, PathTimeline, annotate_hits, find_on_repo,
    matches_filter,
};
use crate::commands::list::{
    ChunkedListingStats, LISTING_BATCH_CHUNKS, LISTING_BATCH_STORED_BYTES, for_each_item_by_chunk,
    for_each_listed_item, list_snapshot_items, load_snapshot_items, load_snapshot_meta,
};
use crate::repo::control::CancelToken;
use crate::repo::manifest::SnapshotEntry;
use crate::repo::{Repository, TreeChunkSource};
use crate::snapshot::item::{Item, ItemType};
use crate::snapshot::listing::{ListingIndex, decode_ext, encode_ext};

use serde_bytes::ByteBuf;

use super::mark::{
    Fx, decode_items, edit_item_ptrs, edit_meta, encode_items, replace_tree_chunk,
    with_four_tree_chunks, write_tree,
};

// ---------------------------------------------------------------------------
// Oracles: today's algorithms over the whole-stream decode
// ---------------------------------------------------------------------------

fn items_of(fx: &Fx, name: &str) -> Vec<Item> {
    load_snapshot_items(&mut fx.open(), name, &CancelToken::new()).unwrap()
}

fn entry(repo: &Repository, name: &str) -> SnapshotEntry {
    repo.manifest().find_snapshot(name).unwrap().clone()
}

/// The diff before 06 step 2, verbatim: regular files keyed by display
/// path, the last one winning.
fn oracle_diff(base: Vec<Item>, target: Vec<Item>) -> Vec<DiffEntry> {
    let map = |items: Vec<Item>| -> BTreeMap<String, (u64, Vec<ChunkId>)> {
        let mut map = BTreeMap::new();
        for item in items {
            if item.entry_type == ItemType::RegularFile {
                let ids = item.chunks.iter().map(|c| c.id).collect();
                map.insert(item.path, (item.size, ids));
            }
        }
        map
    };
    let (base, target) = (map(base), map(target));
    let delta = |old: u64, new: u64| (i128::from(new) - i128::from(old)) as i64;
    let mut entries = Vec::new();
    for (path, (size, ids)) in &base {
        match target.get(path) {
            None => entries.push(DiffEntry {
                path: path.clone(),
                change: DiffChangeKind::Removed,
                old_size: Some(*size),
                new_size: None,
                size_delta: delta(*size, 0),
            }),
            Some((new_size, new_ids)) if size != new_size || ids != new_ids => {
                entries.push(DiffEntry {
                    path: path.clone(),
                    change: DiffChangeKind::Modified,
                    old_size: Some(*size),
                    new_size: Some(*new_size),
                    size_delta: delta(*size, *new_size),
                });
            }
            Some(_) => {}
        }
    }
    for (path, (size, _)) in &target {
        if !base.contains_key(path) {
            entries.push(DiffEntry {
                path: path.clone(),
                change: DiffChangeKind::Added,
                old_size: None,
                new_size: Some(*size),
                size_delta: delta(0, *size),
            });
        }
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    entries
}

fn diff(fx: &Fx, base: &str, target: &str) -> (Vec<DiffEntry>, DiffStats) {
    let mut repo = fx.open();
    let (b, t) = (entry(&repo, base), entry(&repo, target));
    diff_on_repo(&mut repo, &b, &t, &CancelToken::new()).unwrap()
}

fn diff_err(fx: &Fx, base: &str, target: &str) -> VykarError {
    let mut repo = fx.open();
    let (b, t) = (entry(&repo, base), entry(&repo, target));
    diff_on_repo(&mut repo, &b, &t, &CancelToken::new()).unwrap_err()
}

/// A find result in comparable form.
type Found = Vec<(
    String,
    Vec<(String, u64, i64, ItemType, Vec<ChunkId>, FileStatus)>,
)>;

fn found(timelines: Vec<PathTimeline>) -> Found {
    timelines
        .into_iter()
        .map(|t| {
            let hits = t
                .hits
                .into_iter()
                .map(|a| {
                    let h = a.hit;
                    (
                        h.snapshot_name,
                        h.size,
                        h.mtime,
                        h.entry_type,
                        h.chunk_ids,
                        a.status,
                    )
                })
                .collect();
            (t.path, hits)
        })
        .collect()
}

/// The find before 06 step 2: each snapshot's whole stream in order.
fn oracle_find(fx: &Fx, names: &[&str], filter: &FindFilter) -> Found {
    let mut repo = fx.open();
    let mut hits_by_path: BTreeMap<String, Vec<FindHit>> = BTreeMap::new();
    for name in names {
        let e = entry(&repo, name);
        for item in load_snapshot_items(&mut repo, name, &CancelToken::new()).unwrap() {
            if matches_filter(&item, filter) {
                hits_by_path
                    .entry(item.path.clone())
                    .or_default()
                    .push(FindHit {
                        snapshot_name: e.name.clone(),
                        snapshot_time: e.time,
                        size: item.size,
                        mtime: item.mtime,
                        entry_type: item.entry_type,
                        chunk_ids: item.chunks.iter().map(|c| c.id).collect(),
                    });
            }
        }
    }
    found(
        hits_by_path
            .into_iter()
            .map(|(path, hits)| PathTimeline {
                path,
                hits: annotate_hits(hits),
            })
            .collect(),
    )
}

fn find(fx: &Fx, names: &[&str], filter: &FindFilter) -> (Found, FindStats) {
    let mut repo = fx.open();
    let entries: Vec<SnapshotEntry> = names.iter().map(|n| entry(&repo, n)).collect();
    let (timelines, stats) =
        find_on_repo(&mut repo, &entries, filter, &CancelToken::new()).unwrap();
    (found(timelines), stats)
}

fn find_err(fx: &Fx, names: &[&str]) -> VykarError {
    let mut repo = fx.open();
    let entries: Vec<SnapshotEntry> = names.iter().map(|n| entry(&repo, n)).collect();
    find_on_repo(&mut repo, &entries, &no_filter(), &CancelToken::new())
        .err()
        .unwrap()
}

fn no_filter() -> FindFilter {
    FindFilter::build(None, None, None, None, None, None, None).unwrap()
}

fn listed(fx: &Fx, name: &str) -> Vec<Item> {
    list_snapshot_items(&fx.config, fx.pass, name).unwrap()
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A tree of at least 4 tree chunks in snapshot "s1".
fn four_chunks(encrypted: bool) -> Fx {
    with_four_tree_chunks(encrypted, "s1", |fx, dirs| {
        write_tree(&fx.src("src"), "t", dirs);
        fx.backup(&fx.src("src"), "s1");
    })
}

/// [`four_chunks`], then a modified file in `t-00`, a new one in `t-01`
/// and a deleted one in `t-02`, backed up as "s2".
fn changed(encrypted: bool) -> Fx {
    let fx = four_chunks(encrypted);
    let src = fx.src("src");
    std::fs::write(src.join("t-00/f-0005.txt"), "changed content").unwrap();
    std::fs::write(src.join("t-01/new.txt"), "a new file").unwrap();
    std::fs::remove_file(src.join("t-02/f-0007.txt")).unwrap();
    fx.backup(&src, "s2");
    fx
}

fn set(ids: &[ChunkId]) -> HashSet<ChunkId> {
    ids.iter().copied().collect()
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

#[test]
fn list_by_chunk_matches_the_whole_stream_decode() {
    let fx = four_chunks(true);
    let ptrs = fx.item_ptrs("s1");
    let oracle = items_of(&fx, "s1");
    assert!(!oracle.is_empty());
    let repo = fx.open();
    for (max_chunks, max_stored) in [
        (1, u64::MAX),
        (2, u64::MAX),
        (LISTING_BATCH_CHUNKS, LISTING_BATCH_STORED_BYTES),
    ] {
        let mut items = Vec::new();
        let stats: ChunkedListingStats = for_each_item_by_chunk(
            &repo,
            &ptrs,
            TreeChunkSource::CacheFirst,
            max_chunks,
            max_stored,
            &CancelToken::new(),
            |item| {
                items.push(item);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(items, oracle, "caps ({max_chunks}, {max_stored})");
        assert_eq!(stats.chunks, ptrs.len());
        assert_eq!(stats.items, oracle.len() as u64);
        if max_chunks == 1 {
            assert_eq!(stats.batches, ptrs.len());
        }
    }
    drop(repo);

    assert_eq!(listed(&fx, "s1"), oracle);
    let mut visited = Vec::new();
    for_each_listed_item(&fx.config, fx.pass, "s1", |item| {
        visited.push(item);
        Ok(())
    })
    .unwrap();
    assert_eq!(visited, oracle);
}

#[test]
fn list_by_chunk_repeats_a_repeated_pointer() {
    let fx = four_chunks(false);
    edit_item_ptrs(&fx.open(), "s1", |p| p.push(p[0]));
    let oracle = items_of(&fx, "s1");
    let first = decode_items(&fx.open().read_chunk(&fx.item_ptrs("s1")[0]).unwrap());
    assert!(oracle.ends_with(&first), "the repeated run comes last");
    assert_eq!(listed(&fx, "s1"), oracle);
}

#[test]
fn readers_reject_a_chunk_that_ends_inside_an_entry() {
    let fx = four_chunks(false);
    fx.backup(&fx.src("src"), "s2");
    assert_eq!(fx.item_ptrs("s1"), fx.item_ptrs("s2"));
    let oracle = items_of(&fx, "s1");
    // Move the last 5 bytes of chunk 1 to the front of chunk 2: the
    // concatenated stream is unchanged, the chunks no longer hold whole
    // entries.
    let mut tail = Vec::new();
    replace_tree_chunk(&fx, "s2", 1, |mut plain| {
        tail = plain.split_off(plain.len() - 5);
        plain
    });
    replace_tree_chunk(&fx, "s2", 2, |plain| [tail, plain].concat());

    assert_eq!(
        items_of(&fx, "s2"),
        oracle,
        "the tolerant reader is unchanged"
    );
    let needle = "does not decode on its own";
    let err = list_snapshot_items(&fx.config, fx.pass, "s2").unwrap_err();
    assert!(err.to_string().contains(needle), "list: {err}");
    let err = diff_err(&fx, "s1", "s2");
    assert!(err.to_string().contains(needle), "diff: {err}");
    let err = find_err(&fx, &["s1", "s2"]);
    assert!(err.to_string().contains(needle), "find: {err}");
}

#[test]
fn list_refuses_a_dangling_pointer_even_when_cached() {
    let fx = four_chunks(false);
    let id = fx.item_ptrs("s1")[0];
    fx.edit_index(|repo| {
        repo.chunk_index_mut().remove(&id).unwrap();
    });
    assert!(fx.cached().contains(&id), "the chunk stays cached");
    let err = list_snapshot_items(&fx.config, fx.pass, "s1").unwrap_err();
    assert!(
        matches!(err, VykarError::ChunkNotInIndex(missing) if missing == id),
        "got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// diff
// ---------------------------------------------------------------------------

#[test]
fn diff_decodes_only_non_shared_chunks() {
    for encrypted in [false, true] {
        let fx = changed(encrypted);
        let (p1, p2) = (set(&fx.item_ptrs("s1")), set(&fx.item_ptrs("s2")));
        let (entries, stats) = diff(&fx, "s1", "s2");
        assert_eq!(
            entries,
            oracle_diff(items_of(&fx, "s1"), items_of(&fx, "s2"))
        );
        let kinds: Vec<(&str, DiffChangeKind)> = entries
            .iter()
            .map(|e| (e.path.as_str(), e.change))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("t-00/f-0005.txt", DiffChangeKind::Modified),
                ("t-01/new.txt", DiffChangeKind::Added),
                ("t-02/f-0007.txt", DiffChangeKind::Removed),
            ]
        );
        assert_eq!(stats.full, None);
        assert_eq!(stats.decoded_chunks, p1.symmetric_difference(&p2).count());
        assert!(stats.decoded_chunks < p1.union(&p2).count());
        assert!(stats.shared > 0);
        assert_eq!(stats.shared, p1.intersection(&p2).count());
    }
}

#[test]
fn diff_refuses_a_dangling_pointer_in_a_shared_chunk() {
    let fx = changed(false);
    let p2 = set(&fx.item_ptrs("s2"));
    let id = *fx
        .item_ptrs("s1")
        .iter()
        .find(|id| p2.contains(id))
        .expect("a shared tree chunk");
    fx.edit_index(|repo| {
        repo.chunk_index_mut().remove(&id).unwrap();
    });
    let err = diff_err(&fx, "s1", "s2");
    assert!(
        matches!(err, VykarError::ChunkNotInIndex(missing) if missing == id),
        "got {err:?}"
    );
}

#[test]
fn diff_of_identical_listings_decodes_nothing() {
    let fx = four_chunks(false);
    fx.backup(&fx.src("src"), "s2");
    assert_eq!(fx.item_ptrs("s1"), fx.item_ptrs("s2"));
    let (entries, stats) = diff(&fx, "s1", "s2");
    assert!(entries.is_empty(), "{entries:?}");
    assert_eq!(stats.decoded_chunks, 0);
    assert_eq!(stats.full, None);
}

#[test]
fn diff_ignores_where_shared_chunks_sit() {
    let fx = changed(false);
    let (before, before_stats) = diff(&fx, "s1", "s2");
    edit_item_ptrs(&fx.open(), "s2", |p| p.reverse());
    let (entries, stats) = diff(&fx, "s1", "s2");
    assert_eq!(entries, before);
    assert_eq!(
        entries,
        oracle_diff(items_of(&fx, "s1"), items_of(&fx, "s2"))
    );
    assert_eq!(stats.decoded_chunks, before_stats.decoded_chunks);
    assert_eq!(stats.full, None);
}

#[test]
fn diff_falls_back_on_a_repeated_pointer() {
    let fx = changed(false);
    edit_item_ptrs(&fx.open(), "s2", |p| p.push(p[0]));
    let (entries, stats) = diff(&fx, "s1", "s2");
    // The fast path gives the same rows here; the reason is the real check.
    assert_eq!(stats.full, Some(FullDiffReason::RepeatedPointer));
    assert_eq!(
        entries,
        oracle_diff(items_of(&fx, "s1"), items_of(&fx, "s2"))
    );
}

#[test]
fn diff_falls_back_on_a_duplicate_path_in_decoded_chunks() {
    let fx = changed(false);
    let p1 = set(&fx.item_ptrs("s1"));
    let p2 = fx.item_ptrs("s2");
    // A non-shared chunk k of s2 with a shared chunk j after it.
    let k = p2.iter().position(|id| !p1.contains(id)).unwrap();
    let j = (k + 1..p2.len())
        .find(|&j| p1.contains(&p2[j]))
        .expect("a shared chunk after the first non-shared one");
    let shared_file = decode_items(&fx.open().read_chunk(&p2[j]).unwrap())
        .into_iter()
        .find(|i| i.entry_type == ItemType::RegularFile && !i.chunks.is_empty())
        .unwrap();
    // Two modified copies of that file in chunk k: the last copy, in the
    // shared chunk, decides, so a whole-stream diff reports no row for it,
    // while a fast path that trusted the decoded chunks would report it
    // Added.
    replace_tree_chunk(&fx, "s2", k, |plain| {
        let mut items = decode_items(&plain);
        for n in 1..=2 {
            let mut copy = shared_file.clone();
            copy.size += n;
            copy.chunks[0].id = ChunkId::from_bytes([n as u8; 32]);
            items.push(copy);
        }
        encode_items(&items)
    });
    let (entries, stats) = diff(&fx, "s1", "s2");
    assert_eq!(stats.full, Some(FullDiffReason::DuplicatePath));
    assert_eq!(
        entries,
        oracle_diff(items_of(&fx, "s1"), items_of(&fx, "s2"))
    );
    assert!(
        !entries.iter().any(|e| e.path == shared_file.path),
        "{entries:?}"
    );
}

/// Two snapshots of a single-directory source with a command dump whose
/// source directory holds `vykar-dumps/dump.txt` on disk, so the walk
/// would find the dump's path if backup did not exclude it.
#[cfg(unix)]
fn dumps_next_to_a_directory_source() -> Fx {
    let fx = Fx::new(false);
    let src = fx.src("src");
    write_tree(&src, "t", 2);
    std::fs::create_dir(src.join("vykar-dumps")).unwrap();
    std::fs::write(src.join("vykar-dumps/dump.txt"), "on disk").unwrap();
    let dumps = vec![crate::config::CommandDump {
        name: "dump.txt".to_string(),
        command: "echo dump".to_string(),
    }];
    let backup = |name: &str| {
        let source_paths = vec![src.to_string_lossy().to_string()];
        commands::backup::run(
            &fx.config,
            commands::backup::BackupRequest {
                snapshot_name: name,
                passphrase: fx.pass,
                source_paths: &source_paths,
                source_label: "src",
                exclude_patterns: &[],
                exclude_if_present: &[],
                one_file_system: true,
                git_ignore: false,
                xattrs_enabled: false,
                compression: crate::compress::Compression::None,
                command_dumps: &dumps,
                verbose: false,
            },
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
    };
    backup("s1");
    std::fs::write(src.join("t-00/f-0003.txt"), "changed").unwrap();
    backup("s2");
    fx
}

/// The key index of snapshot `name`.
fn index_of(fx: &Fx, name: &str) -> ListingIndex {
    let meta = load_snapshot_meta(&fx.open(), name).unwrap();
    decode_ext(meta.ext.as_deref(), meta.item_ptrs.len()).unwrap()
}

#[cfg(unix)]
#[test]
fn diff_is_chunk_aware_with_dumps_next_to_a_directory_source() {
    // A single-path source (root "") with command dumps ("vykar-dumps"):
    // backup excludes the source's own vykar-dumps/, so the pair is not
    // nested.
    let fx = dumps_next_to_a_directory_source();
    for name in ["s1", "s2"] {
        let index = index_of(&fx, name);
        let roots: Vec<Vec<u8>> = index.segments.iter().map(|s| s.root.to_vec()).collect();
        assert_eq!(roots, vec![b"vykar-dumps".to_vec(), Vec::new()]);
        let items = items_of(&fx, name);
        assert_eq!(
            items
                .iter()
                .filter(|i| i.path == "vykar-dumps/dump.txt")
                .count(),
            1
        );
    }
    let (entries, stats) = diff(&fx, "s1", "s2");
    assert_eq!(stats.full, None);
    assert_eq!(
        entries,
        oracle_diff(items_of(&fx, "s1"), items_of(&fx, "s2"))
    );
    assert!(
        entries
            .iter()
            .any(|e| e.path == "t-00/f-0003.txt" && e.change == DiffChangeKind::Modified),
        "{entries:?}"
    );
}

#[cfg(unix)]
#[test]
fn diff_falls_back_on_nested_segments() {
    // s2's index edited to two equal roots "" and "".
    let fx = dumps_next_to_a_directory_source();
    let mut index = index_of(&fx, "s2");
    index.segments[0].root = ByteBuf::from(Vec::new());
    let ext = encode_ext(&index).unwrap();
    edit_meta(&fx.open(), "s2", |meta| meta.ext = Some(ext));
    let (entries, stats) = diff(&fx, "s1", "s2");
    assert_eq!(stats.full, Some(FullDiffReason::NestedSegments));
    assert_eq!(
        entries,
        oracle_diff(items_of(&fx, "s1"), items_of(&fx, "s2"))
    );
    assert!(
        entries
            .iter()
            .any(|e| e.path == "t-00/f-0003.txt" && e.change == DiffChangeKind::Modified),
        "{entries:?}"
    );
}

#[cfg(unix)]
#[test]
fn diff_with_hard_links_matches_today() {
    let fx = Fx::new(false);
    let src = fx.src("src");
    std::fs::write(src.join("a"), "original").unwrap();
    std::fs::hard_link(src.join("a"), src.join("b")).unwrap();
    std::fs::write(src.join("c"), "other").unwrap();
    fx.backup(&src, "s1");
    // In place: both names see the new content.
    std::fs::write(src.join("a"), "rewritten content").unwrap();
    fx.backup(&src, "s2");
    let (entries, _) = diff(&fx, "s1", "s2");
    assert_eq!(
        entries,
        oracle_diff(items_of(&fx, "s1"), items_of(&fx, "s2"))
    );
    let modified: Vec<&str> = entries
        .iter()
        .filter(|e| e.change == DiffChangeKind::Modified)
        .map(|e| e.path.as_str())
        .collect();
    assert_eq!(modified, vec!["a", "b"]);
}

#[cfg(unix)]
#[test]
fn diff_compares_non_utf8_names_by_raw_bytes() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let fx = Fx::new(false);
    let src = fx.src("src");
    let (n80, n81) = (OsStr::from_bytes(b"n\x80"), OsStr::from_bytes(b"n\x81"));
    if std::fs::write(src.join(n80), "same").is_err()
        || std::fs::write(src.join(n81), "same").is_err()
    {
        return; // filesystem rejects non-UTF8 names — skip.
    }
    std::fs::write(src.join("plain.txt"), "plain").unwrap();
    fx.backup(&src, "s1");
    std::fs::remove_file(src.join(n81)).unwrap();
    fx.backup(&src, "s2");

    let (entries, stats) = diff(&fx, "s1", "s2");
    assert_eq!(stats.full, None);
    let rows: Vec<(&str, DiffChangeKind)> = entries
        .iter()
        .map(|e| (e.path.as_str(), e.change))
        .collect();
    assert_eq!(rows, vec![("n\u{FFFD}", DiffChangeKind::Removed)]);
    // The intended change: keyed by the lossy display path, the two names
    // shadowed each other and the removal went unreported.
    assert!(oracle_diff(items_of(&fx, "s1"), items_of(&fx, "s2")).is_empty());
}

// ---------------------------------------------------------------------------
// find
// ---------------------------------------------------------------------------

#[test]
fn find_decodes_each_distinct_tree_chunk_once() {
    let fx = four_chunks(false);
    let src = fx.src("src");
    std::fs::write(src.join("t-01/f-0002.txt"), "changed content").unwrap();
    fx.backup(&src, "s2");
    fx.backup(&src, "s3");
    let names = ["s1", "s2", "s3"];
    let ptrs: Vec<Vec<ChunkId>> = names.iter().map(|n| fx.item_ptrs(n)).collect();
    let distinct: HashSet<ChunkId> = ptrs.iter().flatten().copied().collect();
    assert!(distinct.len() < ptrs.iter().map(Vec::len).sum::<usize>());

    let filters = [
        no_filter(),
        FindFilter::build(None, Some("f-000*.txt"), None, None, None, None, None).unwrap(),
        FindFilter::build(Some("t-01".into()), None, None, None, None, None, None).unwrap(),
    ];
    for (i, filter) in filters.iter().enumerate() {
        let (results, stats) = find(&fx, &names, filter);
        assert!(!results.is_empty(), "filter {i}");
        assert_eq!(results, oracle_find(&fx, &names, filter), "filter {i}");
        assert_eq!(stats.snapshots, 3);
        assert_eq!(stats.distinct, distinct.len());
        assert_eq!(stats.walk.chunks, distinct.len());
        assert_eq!(stats.pointers, ptrs.iter().map(Vec::len).sum::<usize>());
    }
}

#[test]
fn find_counts_a_repeated_pointer_per_occurrence() {
    let fx = four_chunks(false);
    fx.backup(&fx.src("src"), "s2");
    edit_item_ptrs(&fx.open(), "s1", |p| p.push(p[0]));
    for names in [&["s1"][..], &["s1", "s2"]] {
        let (results, stats) = find(&fx, names, &no_filter());
        assert_eq!(results, oracle_find(&fx, names, &no_filter()), "{names:?}");
        assert_eq!(stats.distinct, fx.item_ptrs("s2").len());
    }
    // A path of the repeated chunk has two hits in s1.
    let (results, _) = find(&fx, &["s1"], &no_filter());
    assert!(results.iter().any(|(_, hits)| hits.len() == 2));
}
