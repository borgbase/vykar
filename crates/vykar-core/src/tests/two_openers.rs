//! Two openers of one repository sharing one cache directory: the rules of
//! the write-once base tables and cached segments (05 "One chunk table").
//! Both handles live in this process, which is what a daemon running a
//! scheduler beside CLI-like operations does.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use vykar_storage::StorageBackend;
use vykar_storage::local_backend::LocalBackend;
use vykar_types::chunk_id::ChunkId;

use crate::compress::Compression;
use crate::index::head::ObjectName;
use crate::index::table::{self, ChunkTable};
use crate::repo::pack::PackType;
use crate::repo::{ChunkerParams, EncryptionMode, OpenOptions, Repository, WriteSessionOptions};
use crate::tests::helpers::FailGet;
use crate::testutil::init_test_environment;

struct Env {
    _tmp: tempfile::TempDir,
    repo_dir: PathBuf,
    cache: PathBuf,
}

impl Env {
    /// A repository holding `n` chunks in a base (S0), and an empty cache dir.
    fn new(n: u8) -> (Self, Vec<ChunkId>) {
        init_test_environment();
        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        let cache = tmp.path().join("cache");
        std::fs::create_dir_all(&repo_dir).unwrap();
        let storage = LocalBackend::new(repo_dir.to_str().unwrap()).unwrap();
        let setup_cache = tmp.path().join("setup-cache");
        let mut repo = Repository::init(
            Box::new(storage),
            EncryptionMode::None,
            ChunkerParams::default(),
            None,
            None,
            Some(setup_cache),
        )
        .unwrap();
        repo.request_index_fold();
        let ids = store(&mut repo, 0, n);
        drop(repo);
        let env = Self {
            _tmp: tmp,
            repo_dir,
            cache,
        };
        (env, ids)
    }

    /// A handle opened with the index, and the count of its `index.d/` GETs.
    fn open(&self) -> (Repository, Arc<AtomicUsize>) {
        let gets = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&gets);
        let local: Arc<dyn StorageBackend> =
            Arc::new(LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap());
        let storage = FailGet::new(
            local,
            move |k| {
                if k.starts_with("index.d/") {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                false
            },
            usize::MAX,
        );
        let repo = Repository::open(
            Box::new(storage),
            None,
            Some(self.cache.clone()),
            OpenOptions::new().with_index(),
        )
        .unwrap();
        (repo, gets)
    }

    fn dir(&self, repo: &Repository) -> PathBuf {
        crate::cache_paths::repo_cache_dir(&repo.config.id, Some(&self.cache)).unwrap()
    }
}

/// Store chunks `from..from + n` in one session and save; returns their IDs.
fn store(repo: &mut Repository, from: u8, n: u8) -> Vec<ChunkId> {
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    let ids = (from..from + n)
        .map(|b| {
            repo.store_chunk(&payload(b), Compression::None, PackType::Data)
                .unwrap()
                .0
        })
        .collect();
    repo.save_state().unwrap();
    ids
}

fn payload(b: u8) -> Vec<u8> {
    format!("two openers chunk {b}").into_bytes()
}

/// Store chunks as a fold: a new base.
fn store_fold(repo: &mut Repository, from: u8, n: u8) -> Vec<ChunkId> {
    repo.request_index_fold();
    store(repo, from, n)
}

fn base(repo: &Repository) -> ObjectName {
    repo.index_head().unwrap().base.as_ref().unwrap().name
}

fn bases_in(dir: &Path) -> Vec<ObjectName> {
    sorted(
        table::list_base_tables(dir)
            .into_iter()
            .map(|(s, _)| s)
            .collect(),
    )
}

fn sorted(mut v: Vec<ObjectName>) -> Vec<ObjectName> {
    v.sort_unstable_by_key(|n| n.0);
    v
}

fn age_file(path: &Path, secs: u64) {
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(secs))
        .unwrap();
}

/// TWO-1. A and B open at S0 (one base file). A writes a segment; B still
/// reads S0's chunks, and B's load under the lock uses the base file and
/// A's cached segment, downloading nothing. B writes a maintenance segment,
/// and A's next load catches up from the cached files too. Every file each
/// open used was verified.
#[test]
fn two_openers_share_one_cache_dir() {
    let (env, ids) = Env::new(8);
    let (mut a, a_gets) = env.open();
    let (mut b, b_gets) = env.open();
    let dir = env.dir(&a);
    let s0 = base(&a);
    assert_eq!(bases_in(&dir), vec![s0]);
    assert_eq!(b_gets.load(Ordering::SeqCst), 0, "B found A's base table");

    store(&mut a, 100, 4);
    assert_eq!(a.index_head().unwrap().segments.len(), 1);
    assert_eq!(bases_in(&dir), vec![s0], "a segment rewrites no table");
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(b.read_chunk(id).unwrap(), payload(i as u8));
    }

    b.load_chunk_index_under_lock().unwrap();
    assert_eq!(b.index_head(), a.index_head());
    assert_eq!(
        b_gets.load(Ordering::SeqCst),
        0,
        "B used the cached segment"
    );
    let first = ids[0];
    b.chunk_index_mut().remove(&first);
    b.save_state().unwrap();
    assert_eq!(b.index_head().unwrap().segments.len(), 2);

    let before = a_gets.load(Ordering::SeqCst);
    a.load_chunk_index().unwrap();
    assert_eq!(a.index_head(), b.index_head());
    assert_eq!(
        a_gets.load(Ordering::SeqCst),
        before,
        "A used the cached files"
    );
    assert_eq!(
        a.chunk_index().table().unwrap().file_path(),
        Some(table::base_table_path(&dir, &s0).as_path())
    );
    assert!(!a.chunk_index().contains(&first));
}

/// A base table deleted while B maps it keeps serving B; once B drops it,
/// the next load downloads the base again and publishes the same name.
#[cfg(unix)]
#[test]
fn a_table_deleted_under_a_reader_is_rebuilt() {
    let (env, ids) = Env::new(6);
    let (mut b, gets) = env.open();
    let dir = env.dir(&b);
    let path = table::base_table_path(&dir, &base(&b));
    std::fs::remove_file(&path).unwrap();
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(b.read_chunk(id).unwrap(), payload(i as u8));
    }

    b.clear_chunk_index();
    let before = gets.load(Ordering::SeqCst);
    b.load_chunk_index().unwrap();
    assert!(gets.load(Ordering::SeqCst) > before, "downloaded the index");
    assert!(ChunkTable::open_file(&path).is_some(), "published again");
    assert_eq!(b.chunk_index().len(), ids.len());
}

/// A fold deletes old base tables, but never one this process maps and
/// never a young one another process may be about to open.
#[test]
fn prune_leaves_tables_of_other_openers() {
    let (env, _) = Env::new(4);
    let (a, _) = env.open();
    let (mut b, _) = env.open();
    let dir = env.dir(&a);
    let s0 = base(&a);
    let s0_path = table::base_table_path(&dir, &s0);
    age_file(&s0_path, 7200);

    // B's fold to S1: S0 is old, but A maps it.
    store_fold(&mut b, 50, 2);
    let s1 = base(&b);
    assert_eq!(bases_in(&dir), sorted(vec![s0, s1]));

    // A is gone and B moved on: the old, unmapped S0 is deleted with B's
    // next fold; the young S1 stays.
    drop(a);
    store_fold(&mut b, 60, 2);
    let s2 = base(&b);
    assert!(!s0_path.exists());
    assert_eq!(bases_in(&dir), sorted(vec![s1, s2]));
}

/// Two openers syncing the same head into one empty cache dir: both
/// succeed, one file is published, and both read the same entries.
#[test]
fn concurrent_installs_of_one_state_succeed() {
    let (env, ids) = Env::new(16);
    let env = &env;
    let entries: Vec<Vec<_>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(move || {
                    let (repo, _) = env.open();
                    repo.chunk_index().iter().collect::<Vec<_>>()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(entries[0], entries[1]);
    assert_eq!(entries[0].len(), ids.len());
    let (repo, _) = env.open();
    assert_eq!(bases_in(&env.dir(&repo)), vec![base(&repo)]);
}
