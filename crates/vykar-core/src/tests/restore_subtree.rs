//! Restore of selected paths and of a glob's literal leading components
//! through the listing key index: the restored tree equals the one the
//! whole-listing read restores.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use crate::commands;
use crate::commands::listing::ListingRead;
use crate::commands::restore::{
    RestoreStats, TestSelection, force_subtree_reads, restore_on_repo_for_test,
};
use crate::repo::control::CancelToken;
use crate::snapshot::listing::{STRIDE, encode_ext};
use crate::tests::helpers::open_local_repo;
use crate::tests::listing_index::{index_of, make_big, multi_root_repo, repo_with, reseal};
use serde_bytes::ByteBuf;
use vykar_types::error::VykarError;

/// One restored entry: a directory, a file's bytes or a link's target.
#[derive(Debug, PartialEq, Eq)]
enum Entry {
    Dir,
    File(Vec<u8>),
    Link(PathBuf),
}

fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Entry>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries {
        let path = e.unwrap().path();
        let rel = path.strip_prefix(root).unwrap().to_path_buf();
        let ft = std::fs::symlink_metadata(&path).unwrap().file_type();
        if ft.is_symlink() {
            out.insert(rel, Entry::Link(std::fs::read_link(&path).unwrap()));
        } else if ft.is_dir() {
            out.insert(rel, Entry::Dir);
            walk(root, &path, out);
        } else {
            out.insert(rel, Entry::File(std::fs::read(&path).unwrap()));
        }
    }
}

fn tree(dest: &Path) -> BTreeMap<PathBuf, Entry> {
    let mut out = BTreeMap::new();
    walk(dest, dest, &mut out);
    out
}

fn counts(s: &RestoreStats) -> (u64, u64, u64, u64, u64) {
    (s.files, s.dirs, s.symlinks, s.hardlinks, s.total_bytes)
}

/// Restores of `snapshot` into fresh destinations under `tmp`.
struct Restorer<'t> {
    tmp: &'t Path,
    repo_dir: PathBuf,
    snapshot: &'t str,
    n: std::cell::Cell<usize>,
}

impl Restorer<'_> {
    fn dest(&self) -> PathBuf {
        let n = self.n.get();
        self.n.set(n + 1);
        self.tmp.join(format!("dest-{n}"))
    }

    fn run(
        &self,
        selection: TestSelection<'_>,
        whole: bool,
    ) -> (BTreeMap<PathBuf, Entry>, RestoreStats, Option<ListingRead>) {
        let dest = self.dest();
        let mut repo = open_local_repo(&self.repo_dir);
        let (stats, read) = restore_on_repo_for_test(
            &mut repo,
            self.snapshot,
            dest.to_str().unwrap(),
            selection,
            whole,
            &CancelToken::new(),
        )
        .unwrap();
        (tree(&dest), stats, read)
    }

    /// The forced scoped restore of `selected`, checked against the whole
    /// read; returns the scoped read's cost.
    fn same_as_whole(&self, selected: &[&str]) -> Option<ListingRead> {
        let selected: HashSet<String> = selected.iter().map(|s| s.to_string()).collect();
        let (want, want_stats, read) = self.run(TestSelection::Paths(&selected), true);
        assert!(read.is_none());
        let _force = force_subtree_reads();
        let (got, stats, read) = self.run(TestSelection::Paths(&selected), false);
        assert_eq!(got, want, "{selected:?}");
        assert_eq!(counts(&stats), counts(&want_stats), "{selected:?}");
        read
    }
}

/// Small directories, non-UTF-8 names and a symlink.
fn make_small(root: &Path) {
    for top in ["a", "b", "a-b", "é"] {
        for mid in 0..2 {
            let m = root.join(top).join(format!("m{mid}"));
            std::fs::create_dir_all(&m).unwrap();
            for f in 0..2 {
                std::fs::write(m.join(format!("f{f}")), format!("{top} {mid} {f}")).unwrap();
            }
        }
        std::fs::write(root.join(format!("{top}.txt")), top).unwrap();
    }
    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let raw = root.join(OsStr::from_bytes(b"n\x80"));
        std::fs::create_dir(&raw).unwrap();
        std::fs::write(raw.join("child"), "c").unwrap();
        std::fs::write(root.join(OsStr::from_bytes(b"\xff")), "ff").unwrap();
        std::os::unix::fs::symlink("a/m0/f0", root.join("ln")).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn selected_subtrees_restore_like_the_whole_read() {
    let tmp = tempfile::tempdir().unwrap();
    multi_root_repo(tmp.path(), "multi", make_small);
    let repo_dir = tmp.path().join("repo");
    let (_, index) = index_of(&repo_dir, "multi");
    let r1 = String::from_utf8(index.segments[1].root.to_vec()).unwrap();
    let parent = r1.rsplit_once('/').unwrap().0.to_string();
    let r = Restorer {
        tmp: tmp.path(),
        repo_dir,
        snapshot: "multi",
        n: Default::default(),
    };
    let p = |s: &str| format!("{r1}/{s}");
    for selected in [
        vec![p("a/m0/f0")],
        vec![p("a")],
        vec![p("a"), p("a/m0/f1")],
        vec![p("a"), p("b")],
        vec![p("ln"), p("é/m1")],
        vec!["vykar-dumps".to_string()],
        vec![parent.clone()],
        vec![p("nosuch")],
    ] {
        let refs: Vec<&str> = selected.iter().map(String::as_str).collect();
        let read = r.same_as_whole(&refs).expect("a subtree read");
        assert!(
            read.via_index && read.fallback.is_none(),
            "{selected:?}: {read:?}"
        );
    }
    // An empty selection reads no listing and restores nothing.
    let read = r.same_as_whole(&[]).expect("a subtree read");
    assert_eq!((read.queries, read.chunks_read), (0, 0));
    // A display path with U+FFFD is read whole.
    let lossy = p("n\u{FFFD}");
    assert!(r.same_as_whole(&[&lossy]).is_none());
    let lossy_child = p("n\u{FFFD}/child");
    assert!(r.same_as_whole(&[&lossy_child, &p("a")]).is_none());
}

#[test]
fn a_small_subtree_of_a_big_listing_reads_few_chunks() {
    let (tmp, _config) = repo_with(make_big);
    let repo_dir = tmp.path().join("repo");
    let (meta, _) = index_of(&repo_dir, "snap");
    let r = Restorer {
        tmp: tmp.path(),
        repo_dir,
        snapshot: "snap",
        n: Default::default(),
    };
    let read = r.same_as_whole(&["a/m0"]).unwrap();
    assert!(read.via_index && read.fallback.is_none(), "{read:?}");
    // Two ranges (the directory and its descendants), each at most the
    // predecessor plus one grid span plus one chunk.
    let bound = 2 * (STRIDE as usize + 2);
    assert!(
        read.chunks_read <= bound && bound < meta.item_ptrs.len(),
        "{} of {} tree chunks",
        read.chunks_read,
        meta.item_ptrs.len()
    );
}

#[cfg(unix)]
#[test]
fn glob_prefix_restore_like_the_whole_read() {
    let tmp = tempfile::tempdir().unwrap();
    multi_root_repo(tmp.path(), "multi", make_small);
    let repo_dir = tmp.path().join("repo");
    let (_, index) = index_of(&repo_dir, "multi");
    let r1 = String::from_utf8(index.segments[1].root.to_vec()).unwrap();
    let r = Restorer {
        tmp: tmp.path(),
        repo_dir,
        snapshot: "multi",
        n: Default::default(),
    };
    for (pattern, narrowed) in [
        (format!("{r1}/a/m1/*"), true),
        (format!("{r1}/a/m1/f0"), true),
        (format!("{r1}/[ab]/m0"), true),
        ("*".to_string(), false),
    ] {
        // Unforced, this listing is too small for the subtree reads to pay.
        let (want, want_stats, read) = r.run(TestSelection::Pattern(Some(&pattern)), false);
        assert!(read.is_none(), "{pattern}");
        assert!(!want.is_empty(), "{pattern}");
        let _force = force_subtree_reads();
        let (got, stats, read) = r.run(TestSelection::Pattern(Some(&pattern)), false);
        assert_eq!(got, want, "{pattern}");
        assert_eq!(counts(&stats), counts(&want_stats), "{pattern}");
        assert_eq!(read.is_some(), narrowed, "{pattern}");
        assert!(read.is_none_or(|r| r.via_index), "{pattern}");
    }
}

#[test]
fn selected_restore_without_an_index() {
    let (tmp, _config) = repo_with(make_small);
    let repo_dir = tmp.path().join("repo");
    reseal(&repo_dir, "snap", |m| m.ext = None);
    let r = Restorer {
        tmp: tmp.path(),
        repo_dir,
        snapshot: "snap",
        n: Default::default(),
    };
    assert!(r.same_as_whole(&["a", "b/m0/f1"]).is_none());
}

#[cfg(unix)]
#[test]
fn hard_link_across_two_selected_subtrees() {
    use std::os::unix::fs::MetadataExt;
    let (tmp, _config) = repo_with(|root| {
        std::fs::create_dir_all(root.join("a")).unwrap();
        std::fs::create_dir_all(root.join("b")).unwrap();
        std::fs::write(root.join("a/x"), "shared").unwrap();
        std::fs::hard_link(root.join("a/x"), root.join("b/y")).unwrap();
    });
    let r = Restorer {
        tmp: tmp.path(),
        repo_dir: tmp.path().join("repo"),
        snapshot: "snap",
        n: Default::default(),
    };
    let selected: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
    let _force = force_subtree_reads();
    let dest = r.tmp.join("dest-hl");
    let mut repo = open_local_repo(&r.repo_dir);
    let (stats, read) = restore_on_repo_for_test(
        &mut repo,
        "snap",
        dest.to_str().unwrap(),
        TestSelection::Paths(&selected),
        false,
        &CancelToken::new(),
    )
    .unwrap();
    assert!(read.is_some_and(|r| r.via_index));
    assert_eq!(stats.hardlinks, 1);
    let ino = |p: &str| std::fs::metadata(dest.join(p)).unwrap().ino();
    assert_eq!(ino("a/x"), ino("b/y"));
    assert_eq!(std::fs::read(dest.join("b/y")).unwrap(), b"shared");
}

/// After one directory's subtree read falls back to a scan, the remaining
/// directories are read by one more scan, not one each.
#[test]
fn a_fallback_reads_the_remaining_directories_in_one_scan() {
    let (tmp, _config) = repo_with(make_big);
    let repo_dir = tmp.path().join("repo");
    let (meta, mut index) = index_of(&repo_dir, "snap");
    let n = meta.item_ptrs.len();
    // Every key but the first empty: the search lands on the last grid
    // chunk, whose predecessor reaches the range of any early directory.
    for k in index.keys.iter_mut().skip(1) {
        *k = ByteBuf::new();
    }
    let ext = encode_ext(&index).unwrap();
    reseal(&repo_dir, "snap", |m| m.ext = Some(ext));
    let r = Restorer {
        tmp: tmp.path(),
        repo_dir,
        snapshot: "snap",
        n: Default::default(),
    };
    let read = r.same_as_whole(&["0", "B", "C"]).unwrap();
    assert!(!read.via_index && read.fallback.is_some(), "{read:?}");
    assert_eq!(read.queries, 3);
    assert!(
        read.chunks_read >= 2 * n && read.chunks_read < 3 * n,
        "{} tree chunks read of {n}",
        read.chunks_read
    );
}

/// A pointer the chunk index lacks fails the restore before the
/// destination is created, even where the subtree read would not reach it.
#[test]
fn a_dangling_pointer_fails_before_the_destination_is_created() {
    let (tmp, _config) = repo_with(make_big);
    let repo_dir = tmp.path().join("repo");
    let (meta, _) = index_of(&repo_dir, "snap");
    let id = *meta.item_ptrs.last().unwrap();
    let mut repo = open_local_repo(&repo_dir);
    commands::util::with_maintenance_lock(&mut repo, |repo| {
        repo.load_chunk_index_under_lock()?;
        repo.chunk_index_mut().remove(&id).unwrap();
        repo.save_state()
    })
    .unwrap();
    drop(repo);

    let selected: HashSet<String> = ["0".to_string()].into();
    let dest = tmp.path().join("dest-dangling");
    let _force = force_subtree_reads();
    let mut repo = open_local_repo(&repo_dir);
    let err = restore_on_repo_for_test(
        &mut repo,
        "snap",
        dest.to_str().unwrap(),
        TestSelection::Paths(&selected),
        false,
        &CancelToken::new(),
    )
    .unwrap_err();
    assert!(
        matches!(err, VykarError::ChunkNotInIndex(missing) if missing == id),
        "{err:?}"
    );
    assert!(!dest.exists());
}
