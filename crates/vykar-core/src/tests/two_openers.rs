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
use crate::repo::control::CancelToken;
use crate::repo::file_cache::{
    CachedChunkRef, CachedChunks, FileCache, FileCacheLock, FileCacheLockError,
};
use crate::repo::pack::PackType;
use crate::repo::{
    ChunkerParams, EncryptionMode, FileCacheSave, HostEnv, OpenOptions, Repo, SystemClock,
    WriteSessionOptions,
};
use crate::sim::env::{SimTime, TimeMode};
use crate::tests::helpers::FailGet;
use crate::testutil::init_test_environment;
use vykar_types::snapshot_id::SnapshotId;

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
        let mut repo = Repo::init(
            Box::new(storage),
            EncryptionMode::Aes256Gcm,
            ChunkerParams::default(),
            Some(crate::testing::TEST_PASSPHRASE),
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
    fn open(&self) -> (Repo, Arc<AtomicUsize>) {
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
        let repo = Repo::open(
            Box::new(storage),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(self.cache.clone()),
            OpenOptions::new().with_index(),
        )
        .unwrap();
        (repo, gets)
    }

    /// A handle opened with the file cache (no index).
    fn open_with_file_cache(&self) -> Repo {
        let storage = LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap();
        Repo::open(
            Box::new(storage),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(self.cache.clone()),
            OpenOptions::new().with_file_cache(),
        )
        .unwrap()
    }

    fn dir(&self, repo: &Repo) -> PathBuf {
        crate::cache_paths::repo_cache_dir(&repo.config().id, Some(&self.cache)).unwrap()
    }
}

/// Store chunks `from..from + n` in one session and save; returns their IDs.
fn store(repo: &mut Repo, from: u8, n: u8) -> Vec<ChunkId> {
    let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
    let ids = (from..from + n)
        .map(|b| {
            ws.store_chunk(repo, &payload(b), Compression::None, PackType::Data)
                .unwrap()
                .0
        })
        .collect();
    repo.unchecked_writer().save_session(ws).unwrap();
    ids
}

fn payload(b: u8) -> Vec<u8> {
    format!("two openers chunk {b}").into_bytes()
}

/// Store chunks as a fold: a new base.
fn store_fold(repo: &mut Repo, from: u8, n: u8) -> Vec<ChunkId> {
    repo.request_index_fold();
    store(repo, from, n)
}

fn base(repo: &Repo) -> ObjectName {
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
    b.unchecked_writer().chunk_index_mut().remove(&first);
    b.unchecked_writer().save_state().unwrap();
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

// ---------------------------------------------------------------------------
// The file cache: two openers merge their sections (04 step 9 "Local caches")
// ---------------------------------------------------------------------------

fn anchor(b: u8) -> SnapshotId {
    SnapshotId::from_bytes([b; 32])
}

/// What a backup of `root` anchored at `snapshot` leaves in the handle
/// (`commit_finalize`'s merge), then the save.
fn back_up_file_cache(repo: &mut Repo, root: &str, snapshot: SnapshotId) -> FileCacheSave {
    let mut write = FileCache::new();
    write.begin_sections(&[root.to_string()], &[0]);
    let chunks = CachedChunks::Single(CachedChunkRef {
        id: ChunkId::from_bytes([0xAA; 32]),
        size: 1,
    });
    write.insert(&format!("{root}/f"), 1, 1, 1, 1, 1, chunks);
    write.finalize_sections(snapshot);
    for (key, section) in write.take_active_sections() {
        repo.file_cache_mut().merge_section(&key, section);
        repo.note_file_cache_written(&key);
    }
    repo.mark_file_cache_dirty();
    repo.save_file_cache_cancellable(&CancelToken::new())
        .unwrap()
        .expect("a cache dir")
}

/// What a backup's start does with sections anchored at a deleted snapshot.
fn invalidate_file_cache(repo: &mut Repo, gone: SnapshotId) {
    let dropped = repo
        .file_cache_mut()
        .invalidate_missing_snapshots_collect(&|id| *id != gone);
    assert!(!dropped.is_empty());
    repo.note_file_cache_dropped(dropped);
    repo.mark_file_cache_dirty();
}

/// The sections on disk, as a fresh opener loads them: (root, anchor).
fn file_cache_on_disk(env: &Env) -> Vec<(String, SnapshotId)> {
    let repo = env.open_with_file_cache();
    let cache = repo.file_cache();
    cache
        .section_keys()
        .into_iter()
        .map(|k| {
            let a = cache.section_anchor(&k).unwrap();
            (k, a)
        })
        .collect()
}

#[test]
fn file_cache_merge_keeps_the_other_openers_section() {
    let (env, _) = Env::new(1);
    let mut a = env.open_with_file_cache();
    let mut b = env.open_with_file_cache();
    assert_eq!(back_up_file_cache(&mut a, "/a", anchor(1)).merged, None);
    assert_eq!(back_up_file_cache(&mut b, "/b", anchor(2)).merged, Some(1));
    assert_eq!(
        file_cache_on_disk(&env),
        vec![("/a".into(), anchor(1)), ("/b".into(), anchor(2))]
    );
}

/// A file absent when the handle loaded and present when it saves was
/// written by another process: the merge path is taken.
#[test]
fn file_cache_absent_at_load_present_at_save_merges() {
    let (env, _) = Env::new(1);
    let mut late = env.open_with_file_cache();
    let mut other = env.open_with_file_cache();
    back_up_file_cache(&mut other, "/other", anchor(1));
    let save = back_up_file_cache(&mut late, "/late", anchor(2));
    assert_eq!(save.merged, Some(1));
    assert_eq!(file_cache_on_disk(&env).len(), 2);
}

/// Without another process the save is today's single write: no re-read of
/// the file, one uncontended lock.
#[test]
fn file_cache_unchanged_file_saves_without_reading() {
    let (env, _) = Env::new(1);
    let mut repo = env.open_with_file_cache();
    let first = back_up_file_cache(&mut repo, "/a", anchor(1));
    assert_eq!(
        first,
        FileCacheSave {
            merged: None,
            locked: true
        }
    );
    let again = back_up_file_cache(&mut repo, "/a", anchor(2));
    assert_eq!(
        again,
        FileCacheSave {
            merged: None,
            locked: true
        }
    );
    // A handle that loaded the file saves it without re-reading it too.
    drop(repo);
    let mut reopened = env.open_with_file_cache();
    let third = back_up_file_cache(&mut reopened, "/b", anchor(3));
    assert_eq!(third.merged, None);
    assert_eq!(
        file_cache_on_disk(&env),
        vec![("/a".into(), anchor(2)), ("/b".into(), anchor(3))]
    );
}

/// A section this handle invalidated is not taken back from another
/// process's copy when that copy has the same anchor; a newer section of the
/// same root (another anchor) is kept.
#[test]
fn file_cache_drop_applies_only_to_the_same_anchor() {
    let (env, _) = Env::new(1);
    back_up_file_cache(&mut env.open_with_file_cache(), "/r", anchor(1));
    let mut a = env.open_with_file_cache();
    let mut b = env.open_with_file_cache();
    back_up_file_cache(&mut b, "/r", anchor(2));
    invalidate_file_cache(&mut a, anchor(1));
    back_up_file_cache(&mut a, "/a", anchor(3));
    assert_eq!(
        file_cache_on_disk(&env),
        vec![("/a".into(), anchor(3)), ("/r".into(), anchor(2))]
    );
}

/// A and B load {x, y}; B invalidates y and saves; A backs up x and saves:
/// A's stale in-memory copy of y is not written back.
#[test]
fn file_cache_stale_copies_are_not_written_back() {
    let (env, _) = Env::new(1);
    let mut setup = env.open_with_file_cache();
    back_up_file_cache(&mut setup, "/x", anchor(1));
    back_up_file_cache(&mut setup, "/y", anchor(2));
    drop(setup);
    let mut a = env.open_with_file_cache();
    let mut b = env.open_with_file_cache();
    invalidate_file_cache(&mut b, anchor(2));
    b.save_file_cache_cancellable(&CancelToken::new()).unwrap();
    back_up_file_cache(&mut a, "/x", anchor(3));
    assert_eq!(file_cache_on_disk(&env), vec![("/x".into(), anchor(3))]);
}

/// Two handles saving disjoint sections at once both end present.
#[test]
fn file_cache_lock_serialises_saves() {
    let (env, _) = Env::new(1);
    for i in 0..20 {
        let a = env.open_with_file_cache();
        let b = env.open_with_file_cache();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let run = |mut repo: Repo, root: String, barrier: Arc<std::sync::Barrier>| {
            std::thread::spawn(move || {
                barrier.wait();
                back_up_file_cache(&mut repo, &root, anchor(i));
            })
        };
        let ta = run(a, format!("/a{i}"), Arc::clone(&barrier));
        let tb = run(b, format!("/b{i}"), barrier);
        ta.join().unwrap();
        tb.join().unwrap();
        let roots: Vec<String> = file_cache_on_disk(&env).into_iter().map(|s| s.0).collect();
        for want in [format!("/a{i}"), format!("/b{i}")] {
            assert!(
                roots.contains(&want),
                "round {i}: {want} missing from {roots:?}"
            );
        }
    }
}

/// Loads {x, y}, then another binary replaces the file with what `bytes`
/// makes and the handle backs up x: y survives the save.
fn file_cache_unmergeable_file_keeps_untouched_sections(bytes: impl FnOnce(&Repo) -> Vec<u8>) {
    let (env, _) = Env::new(1);
    let mut setup = env.open_with_file_cache();
    back_up_file_cache(&mut setup, "/x", anchor(1));
    back_up_file_cache(&mut setup, "/y", anchor(2));
    drop(setup);
    let mut repo = env.open_with_file_cache();
    let path = env.dir(&repo).join("filecache");
    std::fs::remove_file(&path).unwrap();
    std::fs::write(&path, bytes(&repo)).unwrap();
    let save = back_up_file_cache(&mut repo, "/x", anchor(3));
    assert_eq!(save.merged, None);
    assert_eq!(
        file_cache_on_disk(&env),
        vec![("/x".into(), anchor(3)), ("/y".into(), anchor(2))]
    );
}

#[test]
fn file_cache_garbage_file_keeps_untouched_sections() {
    file_cache_unmergeable_file_keeps_untouched_sections(|_| b"not a file cache".to_vec());
}

#[test]
fn file_cache_other_format_keeps_untouched_sections() {
    // A well-formed object whose plaintext starts with another format byte.
    file_cache_unmergeable_file_keeps_untouched_sections(|repo| {
        crate::repo::format::pack_object_with_context(
            crate::repo::format::ObjectType::FileCache,
            b"filecache",
            &[0x7F, 1, 2, 3],
            repo.crypto().as_ref(),
        )
        .unwrap()
    });
}

/// The save never blocks on another process's lock: after
/// `FILE_CACHE_LOCK_WAIT` on the caller's clock it saves without it.
#[test]
fn file_cache_save_without_the_lock_after_the_wait() {
    let (env, _) = Env::new(1);
    let mut repo = env.open_with_file_cache();
    let time = SimTime::new(chrono::Utc::now(), TimeMode::Auto);
    repo.set_host_env(HostEnv::system().with_clock(Arc::new(time.clock(chrono::Duration::zero()))));
    let dir = env.dir(&repo);
    std::fs::create_dir_all(&dir).unwrap();
    let held = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dir.join(FileCacheLock::FILE_NAME))
        .unwrap();
    held.lock().unwrap();
    let start = time.now_utc();
    let save = back_up_file_cache(&mut repo, "/a", anchor(1));
    assert!(!save.locked);
    assert!(time.now_utc() - start >= chrono::Duration::seconds(30));
    drop(held);
    assert_eq!(file_cache_on_disk(&env), vec![("/a".into(), anchor(1))]);
    // Without the lock the file may be another process's by now: the
    // handle kept no stamp, so its next save re-reads and merges.
    let next = back_up_file_cache(&mut repo, "/b", anchor(2));
    assert_eq!(
        next,
        FileCacheSave {
            merged: Some(1),
            locked: true
        }
    );
    assert_eq!(
        file_cache_on_disk(&env),
        vec![("/a".into(), anchor(1)), ("/b".into(), anchor(2))]
    );

    // A cancelled caller does not wait at all.
    let held = std::fs::File::open(dir.join(FileCacheLock::FILE_NAME)).unwrap();
    held.lock().unwrap();
    let cancel = CancelToken::new();
    cancel.cancel();
    let clock = SystemClock;
    let begun = std::time::Instant::now();
    let got = FileCacheLock::acquire(&dir, &clock, &cancel, std::time::Duration::from_secs(30));
    assert!(matches!(got, Err(FileCacheLockError::Cancelled)), "{got:?}");
    assert!(begun.elapsed() < std::time::Duration::from_secs(5));
}
