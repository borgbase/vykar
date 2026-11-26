//! The listing key index end to end (06 step 4): backup writes it, the read
//! API answers through it exactly as a scan does, a wrong index falls back,
//! `check` validates it, and the repair rewriter replaces it.

use std::path::Path;

use crate::commands;
use crate::commands::check::scan_for_test;
use crate::commands::list::{
    ItemStreamReader, ListUnderRead, display_path_under, list_under_on_repo, load_snapshot_meta,
};
use crate::commands::listing::{ListingRead, ListingView, Query, matches};
use crate::compress::Compression;
use crate::config::{CommandDump, VykarConfig};
use crate::repo::Repo;
use crate::repo::control::CancelToken;
use crate::repo::format::{ObjectType, pack_object_with_context};
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::ItemType;
use crate::snapshot::listing::{
    ListingIndex, ORDER_DFS_NAME, ORDER_UNKNOWN, STRIDE, decode_ext, encode_ext,
};
use crate::tests::helpers::{backup_single_source, init_repo, open_local_repo};
use crate::tests::tree_cut::make_tree_cut_source;
use serde_bytes::ByteBuf;
use vykar_types::error::VykarError;

pub(crate) fn backup(
    config: &VykarConfig,
    source_paths: &[String],
    snapshot: &str,
    dumps: &[CommandDump],
) {
    commands::backup::run(
        config,
        commands::backup::BackupRequest {
            snapshot_name: snapshot,
            passphrase: None,
            source_paths,
            source_label: "src",
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: dumps,
            verbose: false,
        },
    )
    .unwrap();
}

/// A repository with one backup of `source` (created by `make`).
pub(crate) fn repo_with<R>(make: impl FnOnce(&Path) -> R) -> (tempfile::TempDir, VykarConfig) {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source).unwrap();
    let mut config = init_repo(&repo_dir);
    config.xattrs.enabled = false;
    make(&source);
    backup_single_source(&config, &source, "src", "snap");
    (tmp, config)
}

/// Names whose dfs-name order differs from the byte order of the joined
/// path, non-UTF-8 names, a backslash name and a directory of 3,000 files.
/// Returns whether the non-UTF-8 names were created (not where the
/// filesystem refuses them, as APFS does).
pub(crate) fn make_awkward(root: &Path) -> bool {
    for top in [
        "z", "é", "b", "B", "C", "ab", "a b", "a-b", "a.b", "a", "Zeta", "0", "_x",
    ] {
        let d = root.join(top);
        if std::fs::create_dir(&d).is_err() {
            continue; // case-insensitive filesystem
        }
        for mid in 0..4 {
            let m = d.join(format!("m{mid}"));
            std::fs::create_dir(&m).unwrap();
            for f in 0..3 {
                std::fs::write(m.join(format!("f{f}")), "x").unwrap();
            }
        }
        std::fs::write(root.join(format!("{top}.txt")), "x").unwrap();
    }
    let big = root.join("big");
    std::fs::create_dir(&big).unwrap();
    for f in 0..3000 {
        std::fs::write(big.join(format!("file-{f:05}")), format!("{f}")).unwrap();
    }
    #[cfg(unix)]
    let raw = {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let raw_dir = root.join(OsStr::from_bytes(b"n\x80"));
        let raw = crate::testutil::try_raw_name(&raw_dir, |p| std::fs::create_dir(p));
        if raw {
            std::fs::write(raw_dir.join("child"), "x").unwrap();
            std::fs::write(root.join(OsStr::from_bytes(b"\xff")), "x").unwrap();
        }
        std::fs::write(root.join("né"), "x").unwrap();
        std::fs::write(root.join("\u{10000}"), "x").unwrap();
        std::fs::write(root.join("a\\b"), "x").unwrap();
        raw
    };
    #[cfg(not(unix))]
    let raw = false;
    raw
}

pub(crate) fn make_big(root: &Path) -> bool {
    make_tree_cut_source(root);
    make_awkward(root)
}

/// Every entry of the snapshot, by a scan: (path, is a directory).
fn scan_all(repo: &mut Repo, meta: &SnapshotMeta) -> Vec<(Vec<u8>, bool)> {
    ItemStreamReader::new(repo).ensure_index(repo).unwrap();
    let mut all = Vec::new();
    let stats = ListingView::ignore_index(meta)
        .subtree(repo, b"", &CancelToken::new(), |i| {
            all.push((i.path_bytes().to_vec(), i.entry_type == ItemType::Directory));
            Ok(())
        })
        .unwrap();
    assert!(!stats.via_index);
    all
}

fn read(
    repo: &mut Repo,
    view: &ListingView<'_>,
    dir: &[u8],
    subtree: bool,
) -> (Vec<Vec<u8>>, ListingRead) {
    ItemStreamReader::new(repo).ensure_index(repo).unwrap();
    let mut got = Vec::new();
    let visit = |i: crate::snapshot::item::Item| {
        got.push(i.path_bytes().to_vec());
        Ok(())
    };
    let shutdown = CancelToken::new();
    let stats = if subtree {
        view.subtree(repo, dir, &shutdown, visit)
    } else {
        view.dir_entries(repo, dir, &shutdown, visit)
    }
    .unwrap();
    (got, stats)
}

/// One query's result through the index.
struct Checked {
    dir: Vec<u8>,
    subtree: bool,
    stats: ListingRead,
}

/// For every directory of the snapshot and `extra`, `dir_entries` and
/// `subtree` through the index equal the scan, without a fallback. The
/// first `direct` queries are also run through `ListingView::ignore_index`.
fn assert_reads_match_scan(
    repo_dir: &Path,
    snap: &str,
    extra: &[Vec<u8>],
    direct: usize,
) -> Vec<Checked> {
    let mut repo = open_local_repo(repo_dir);
    let meta = load_snapshot_meta(&repo, snap).unwrap();
    let all = scan_all(&mut repo, &meta);
    let mut dirs: Vec<Vec<u8>> = extra.to_vec();
    dirs.extend(all.iter().filter(|(_, d)| *d).map(|(p, _)| p.clone()));
    let file = all.iter().find(|(_, d)| !*d).unwrap().0.clone();
    dirs.extend([Vec::new(), file, b"no/such/dir".to_vec()]);

    let view = ListingView::new(&meta);
    let scan_view = ListingView::ignore_index(&meta);
    assert!(view.index().is_ok(), "{:?}", view.index());
    let mut out = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        for subtree in [false, true] {
            let q = if subtree {
                Query::Subtree(dir)
            } else {
                Query::DirEntries(dir)
            };
            let want: Vec<Vec<u8>> = all
                .iter()
                .filter(|(p, _)| matches(q, p))
                .map(|(p, _)| p.clone())
                .collect();
            let (got, stats) = read(&mut repo, &view, dir, subtree);
            let name = String::from_utf8_lossy(dir);
            assert_eq!(got, want, "{name:?} subtree={subtree}");
            if !(subtree && dir.is_empty()) {
                assert!(
                    stats.via_index,
                    "{name:?} subtree={subtree}: {:?}",
                    stats.fallback
                );
            }
            if i < direct {
                let (scanned, s) = read(&mut repo, &scan_view, dir, subtree);
                assert_eq!(scanned, want, "{name:?} subtree={subtree}");
                assert!(!s.via_index);
            }
            out.push(Checked {
                dir: dir.clone(),
                subtree,
                stats,
            });
        }
    }
    out
}

/// The paths of each tree chunk of `snap`.
pub(crate) fn chunk_paths(repo_dir: &Path, snap: &str) -> Vec<Vec<Vec<u8>>> {
    let repo = open_local_repo(repo_dir);
    let meta = load_snapshot_meta(&repo, snap).unwrap();
    meta.item_ptrs
        .iter()
        .map(|id| {
            let chunk = repo.read_chunk(id).unwrap();
            let mut paths = Vec::new();
            commands::list::for_each_decoded_item(&chunk, |i| {
                paths.push(i.path_bytes().to_vec());
                Ok(())
            })
            .unwrap();
            paths
        })
        .collect()
}

/// Re-seal snapshot `name`'s object with its meta edited by `f`.
pub(crate) fn reseal(repo_dir: &Path, name: &str, f: impl FnOnce(&mut SnapshotMeta)) {
    let repo = open_local_repo(repo_dir);
    let entry = repo.manifest().find_snapshot(name).unwrap().clone();
    let mut meta = load_snapshot_meta(&repo, name).unwrap();
    f(&mut meta);
    let packed = pack_object_with_context(
        ObjectType::SnapshotMeta,
        entry.id.as_bytes(),
        &rmp_serde::to_vec(&meta).unwrap(),
        repo.crypto().as_ref(),
    )
    .unwrap();
    repo.storage()
        .put(&entry.id.storage_key(), &packed)
        .unwrap();
}

pub(crate) fn index_of(repo_dir: &Path, snap: &str) -> (SnapshotMeta, ListingIndex) {
    let repo = open_local_repo(repo_dir);
    let meta = load_snapshot_meta(&repo, snap).unwrap();
    let index = decode_ext(meta.ext.as_deref(), meta.item_ptrs.len()).unwrap();
    (meta, index)
}

fn listing_warnings(repo_dir: &Path, config: &VykarConfig) -> Vec<(String, String)> {
    let mut repo = open_local_repo(repo_dir);
    let scan = scan_for_test(&mut repo, config, false).unwrap();
    assert!(scan.issues.is_empty(), "{:?}", scan.issues);
    scan.listing_index_warnings
}

#[test]
fn backup_writes_an_index_and_check_accepts_it() {
    let (tmp, config) = repo_with(make_tree_cut_source);
    let repo_dir = tmp.path().join("repo");
    let (meta, index) = index_of(&repo_dir, "snap");
    let n = meta.item_ptrs.len();
    assert!(n >= 8, "only {n} tree chunks");
    assert_eq!(index.order, ORDER_DFS_NAME);
    assert_eq!(index.stride, STRIDE);
    assert_eq!(index.segments.len(), 1);
    assert!(index.segments[0].root.is_empty());
    assert_eq!(index.keys.len(), n.div_ceil(STRIDE as usize));
    assert!(listing_warnings(&repo_dir, &config).is_empty());
}

#[test]
fn reads_through_the_index_equal_the_scan() {
    let (tmp, _config) = repo_with(make_big);
    let repo_dir = tmp.path().join("repo");
    let checked = assert_reads_match_scan(&repo_dir, "snap", &[], 8);
    let mut bounded = 0;
    for c in &checked {
        if !c.subtree && c.stats.matches < 100 && c.stats.match_chunks <= 1 {
            bounded += 1;
            assert!(
                c.stats.chunks_read <= 34,
                "{:?}: {} chunks read",
                String::from_utf8_lossy(&c.dir),
                c.stats.chunks_read
            );
        }
    }
    assert!(bounded > 50, "only {bounded} small directories");
    // The 3,000-entry directory spans several chunks.
    let big = checked
        .iter()
        .find(|c| c.dir == b"big" && !c.subtree)
        .unwrap();
    assert_eq!(big.stats.matches, 3000);
    assert!(big.stats.match_chunks > 1);
}

#[cfg(unix)]
#[test]
fn non_utf8_and_backslash_names() {
    let mut raw = false;
    let (tmp, _config) = repo_with(|r| raw = make_awkward(r));
    let repo_dir = tmp.path().join("repo");
    let mut repo = open_local_repo(&repo_dir);
    let meta = load_snapshot_meta(&repo, "snap").unwrap();
    let view = ListingView::new(&meta);
    if raw {
        let (got, stats) = read(&mut repo, &view, b"n\x80", false);
        assert!(stats.via_index);
        assert_eq!(got, vec![b"n\x80/child".to_vec()]);
    }
    let (top, _) = read(&mut repo, &view, b"", false);
    assert!(top.contains(&b"a\\b".to_vec()));
    let (in_a, stats) = read(&mut repo, &view, b"a", false);
    assert!(stats.via_index);
    assert!(!in_a.is_empty());
    assert!(in_a.iter().all(|p| p.starts_with(b"a/")), "{in_a:?}");
}

#[test]
fn a_wrong_index_falls_back_and_check_warns() {
    let (tmp, config) = repo_with(make_big);
    let repo_dir = tmp.path().join("repo");
    let chunks = chunk_paths(&repo_dir, "snap");
    assert!(chunks.len() > 64, "only {} tree chunks", chunks.len());
    let (meta, index) = index_of(&repo_dir, "snap");
    let mut repo = open_local_repo(&repo_dir);
    let all = scan_all(&mut repo, &meta);

    let run = |repo: &mut Repo, ext: Option<Vec<u8>>, dir: &[u8]| {
        let mut m = meta.clone();
        m.ext = ext;
        let (got, stats) = read(repo, &ListingView::new(&m), dir, true);
        let want: Vec<Vec<u8>> = all
            .iter()
            .filter(|(p, _)| matches(Query::Subtree(dir), p))
            .map(|(p, _)| p.clone())
            .collect();
        assert_eq!(got, want, "{:?}", String::from_utf8_lossy(dir));
        assert!(!want.is_empty());
        stats
    };
    let with_key = |key: &[u8]| {
        let mut ix = index.clone();
        ix.keys[1] = ByteBuf::from(key.to_vec());
        encode_ext(&ix).unwrap()
    };
    // Too large: chunk 40's first path as the key of chunk 32, queried at
    // an entry of chunk 40 that the key does not exceed.
    let too_large = with_key(&chunks[40][0]);
    let stats = run(&mut repo, Some(too_large.clone()), &chunks[40][1]);
    assert!(stats.fallback.is_some() && !stats.via_index);
    // Too small: noticed through the predecessor of chunk 32, for a query
    // whose results lie before it and not in the first chunk.
    let stats = run(&mut repo, Some(with_key(b"")), &chunks[5][1]);
    assert!(stats.fallback.is_some() && !stats.via_index);
    // No usable index at all.
    let order_0 = encode_ext(&ListingIndex {
        order: ORDER_UNKNOWN,
        segments: Vec::new(),
        stride: STRIDE,
        keys: Vec::new(),
    })
    .unwrap();
    let unknown_version = rmp_serde::to_vec(&(2u32, &index)).unwrap();
    for ext in [
        None,
        Some(order_0),
        Some(unknown_version),
        Some(b"garbage".to_vec()),
    ] {
        let stats = run(&mut repo, ext.clone(), &chunks[5][1]);
        assert!(stats.fallback.is_some() && !stats.via_index, "{ext:?}");
    }
    drop(repo);

    // check warns about the stored too-large key, and nothing else.
    reseal(&repo_dir, "snap", |m| m.ext = Some(too_large));
    let warnings = listing_warnings(&repo_dir, &config);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0].0, "snap");
    let plan = commands::check::run_with_repair(
        &config,
        None,
        false,
        commands::check::RepairMode::PlanOnly,
        None,
        &CancelToken::new(),
    )
    .unwrap()
    .plan;
    assert!(
        matches!(
            plan.actions.as_slice(),
            [commands::check::RepairAction::RemoveUnreferencedEntries]
        ),
        "{:?}",
        plan.actions
    );
    // Legitimate kinds are silent.
    reseal(&repo_dir, "snap", |m| m.ext = None);
    assert!(listing_warnings(&repo_dir, &config).is_empty());
    reseal(&repo_dir, "snap", |m| {
        m.ext = Some(rmp_serde::to_vec(&(9u32, "future")).unwrap());
    });
    assert!(listing_warnings(&repo_dir, &config).is_empty());
}

/// `list_under_on_repo` of `dir`: the display paths, in order, and how it read.
fn list_under(repo: &Repo, meta: &SnapshotMeta, dir: &str) -> (Vec<String>, ListUnderRead) {
    let mut got = Vec::new();
    let read = list_under_on_repo(repo, meta, dir, &CancelToken::new(), |i| {
        got.push(i.path);
        Ok(())
    })
    .unwrap();
    (got, read)
}

/// For every directory display path and a few odd filters, the listing
/// under it equals the whole listing filtered by display path. With
/// `indexed`, every clean filter without U+FFFD reads through the index.
/// With `raw`, the non-UTF-8 names of `make_awkward` exist.
fn assert_list_under_equals_the_filter(repo_dir: &Path, indexed: bool, raw: bool) {
    let repo = open_local_repo(repo_dir);
    let meta = load_snapshot_meta(&repo, "snap").unwrap();
    let mut all: Vec<(String, bool)> = Vec::new();
    commands::list::for_each_item_by_chunk(
        &repo,
        &meta.item_ptrs,
        crate::repo::TreeChunkSource::CacheFirst,
        commands::list::LISTING_BATCH_CHUNKS,
        commands::list::LISTING_BATCH_STORED_BYTES,
        &CancelToken::new(),
        |i| {
            all.push((i.path.clone(), i.entry_type == ItemType::Directory));
            Ok(())
        },
    )
    .unwrap();
    let mut dirs: Vec<String> = all
        .iter()
        .filter(|(_, d)| *d)
        .map(|(p, _)| p.clone())
        .collect();
    dirs.push(all.iter().find(|(_, d)| !*d).unwrap().0.clone());
    dirs.extend(
        ["no/such", "n\u{FFFD}", "a//b", "/b", "big/"]
            .iter()
            .map(|s| s.to_string()),
    );
    for dir in &dirs {
        let want: Vec<String> = all
            .iter()
            .filter(|(p, _)| display_path_under(p, dir))
            .map(|(p, _)| p.clone())
            .collect();
        let (got, read) = list_under(&repo, &meta, dir);
        assert_eq!(got, want, "{dir:?}");
        let narrowable =
            !dir.contains('\u{FFFD}') && crate::snapshot::listing::is_clean(dir.as_bytes());
        match read {
            ListUnderRead::Index(s) => {
                assert!(indexed && narrowable, "{dir:?}");
                assert!(s.via_index, "{dir:?}: {:?}", s.fallback);
            }
            ListUnderRead::Whole(s) => {
                assert!(!(indexed && narrowable), "{dir:?}");
                assert_eq!(s.chunks, meta.item_ptrs.len(), "{dir:?}");
            }
        }
    }
    #[cfg(unix)]
    if raw {
        let (got, _) = list_under(&repo, &meta, "n\u{FFFD}");
        assert_eq!(
            got,
            vec!["n\u{FFFD}".to_string(), "n\u{FFFD}/child".to_string()]
        );
    }
    #[cfg(not(unix))]
    let _ = raw;
}

#[test]
fn list_under_equals_the_display_filter() {
    let mut raw = false;
    let (tmp, _config) = repo_with(|r| raw = make_awkward(r));
    assert_list_under_equals_the_filter(&tmp.path().join("repo"), true, raw);
}

#[test]
fn list_under_without_an_index() {
    let mut raw = false;
    let (tmp, _config) = repo_with(|r| raw = make_awkward(r));
    let repo_dir = tmp.path().join("repo");
    reseal(&repo_dir, "snap", |m| m.ext = None);
    assert_list_under_equals_the_filter(&repo_dir, false, raw);
}

#[test]
fn list_under_refuses_a_dangling_pointer_outside_the_subtree() {
    let (tmp, _config) = repo_with(make_big);
    let repo_dir = tmp.path().join("repo");
    let mut repo = open_local_repo(&repo_dir);
    let meta = load_snapshot_meta(&repo, "snap").unwrap();
    let id = *meta.item_ptrs.last().unwrap();
    repo.unchecked_writer()
        .chunk_index_mut()
        .remove(&id)
        .unwrap();
    let err = list_under_on_repo(&repo, &meta, "0", &CancelToken::new(), |_| Ok(())).unwrap_err();
    assert!(
        matches!(err, VykarError::ChunkNotInIndex(missing) if missing == id),
        "{err:?}"
    );
}

/// Every ancestor of `root` ("a/b/c": "a", "a/b").
fn ancestors(root: &[u8]) -> Vec<Vec<u8>> {
    root.iter()
        .enumerate()
        .filter(|(_, b)| **b == b'/')
        .map(|(i, _)| root[..i].to_vec())
        .collect()
}

pub(crate) fn dumps() -> Vec<CommandDump> {
    vec![
        CommandDump {
            name: "zeta".to_string(),
            command: "echo zeta".to_string(),
        },
        CommandDump {
            name: "alpha".to_string(),
            command: "echo alpha".to_string(),
        },
    ]
}

/// A repository with snapshot `name` of two roots (in non-name order),
/// each filled by `make`, plus two command dumps. Returns the roots.
pub(crate) fn multi_root_repo<R>(
    tmp: &Path,
    name: &str,
    make: impl Fn(&Path) -> R,
) -> (VykarConfig, Vec<std::path::PathBuf>) {
    let repo_dir = tmp.join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);
    let roots = vec![tmp.join("two"), tmp.join("one")];
    for r in &roots {
        std::fs::create_dir_all(r).unwrap();
        make(r);
    }
    let paths: Vec<String> = roots
        .iter()
        .map(|r| r.to_string_lossy().to_string())
        .collect();
    backup(&config, &paths, name, &dumps());
    (config, roots)
}

#[cfg(unix)]
#[test]
fn dumps_and_multi_path_roots_are_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    // Config order is not name order.
    let (config, roots) = multi_root_repo(tmp.path(), "multi", make_awkward);

    let (_, index) = index_of(&repo_dir, "multi");
    assert_eq!(index.order, ORDER_DFS_NAME);
    assert_eq!(index.segments.len(), 3);
    assert_eq!(index.segments[0].root.as_slice(), b"vykar-dumps");
    assert!(
        index
            .segments
            .windows(2)
            .all(|w| w[0].first_ptr < w[1].first_ptr)
    );
    let mut extra = vec![b"vykar-dumps".to_vec()];
    for (seg, root) in index.segments[1..].iter().zip(&roots) {
        let name = root.file_name().unwrap().as_encoded_bytes();
        assert!(
            seg.root.ends_with(name),
            "{:?}",
            String::from_utf8_lossy(&seg.root)
        );
        extra.push(seg.root.to_vec());
        extra.extend(ancestors(&seg.root));
    }
    let checked = assert_reads_match_scan(&repo_dir, "multi", &extra, 6);
    // The root entries are the children of their parent.
    let parent = ancestors(&index.segments[1].root).pop().unwrap();
    let c = checked
        .iter()
        .find(|c| c.dir == parent && !c.subtree)
        .unwrap();
    assert_eq!(c.stats.matches, 2);
    assert!(listing_warnings(&repo_dir, &config).is_empty());
}

/// A source directory named `vykar-dumps` next to command dumps is
/// excluded by the backup, so no two segments share that root in a real
/// snapshot (the reader handles it anyway, see the unit tests of
/// `commands::listing`).
#[cfg(unix)]
#[test]
fn a_source_vykar_dumps_directory_next_to_the_dumps() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source = tmp.path().join("source");
    std::fs::create_dir_all(source.join("vykar-dumps")).unwrap();
    std::fs::write(source.join("vykar-dumps/x"), "x").unwrap();
    std::fs::write(source.join("f"), "x").unwrap();
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);
    backup(
        &config,
        &[source.to_string_lossy().to_string()],
        "dup",
        &dumps(),
    );

    let (_, index) = index_of(&repo_dir, "dup");
    assert_eq!(index.segments.len(), 2);
    assert_eq!(index.segments[0].root.as_slice(), b"vykar-dumps");
    assert!(index.segments[1].root.is_empty());
    let checked = assert_reads_match_scan(&repo_dir, "dup", &[b"vykar-dumps".to_vec()], 4);
    let matches_of = |subtree: bool| {
        checked
            .iter()
            .find(|c| c.dir == b"vykar-dumps" && c.subtree == subtree)
            .unwrap()
            .stats
            .matches
    };
    // alpha and zeta; the source's vykar-dumps/x is excluded.
    assert_eq!(matches_of(false), 2);
    assert_eq!(matches_of(true), 3);
}
