//! Maintenance operations load the chunk index under the repository lock,
//! and refresh this machine's existing local index caches after writing it.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use vykar_storage::{delegate_storage_backend, ConditionalGet, InnerBackend, StorageBackend};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use crate::commands;
use crate::commands::backup::{self, BackupProgressEvent};
use crate::commands::util::with_maintenance_lock;
use crate::config::{RetentionConfig, SourceEntry, VykarConfig};
use crate::index::dedup_cache::{self, MmapDedupCache, MmapFullIndexCache, MmapRestoreCache};
use crate::index::ChunkIndexEntry;
use crate::repo::{OpenOptions, Repository};

use super::helpers::{backup_single_source, init_repo_v3, source_entry};

const FAST: &str = "index unchanged, fast path";
const SLOW: &str = "decode index";

/// Counts reads of `index`, conditional or not.
struct CountIndexReads {
    inner: Box<dyn StorageBackend>,
    reads: Arc<AtomicUsize>,
}

impl InnerBackend for CountIndexReads {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for CountIndexReads;
    except [get, get_if_changed];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if key == "index" {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }

    fn get_if_changed(&self, key: &str, known_blake3: &[u8; 32]) -> Result<ConditionalGet> {
        if key == "index" {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_if_changed(key, known_blake3)
    }
}

/// Fails every `index` PUT.
struct FailIndexPut {
    inner: Box<dyn StorageBackend>,
}

impl InnerBackend for FailIndexPut {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for FailIndexPut;
    except [put, put_pack, begin_staged_put, profile];

    fn profile(&self) -> vykar_storage::BackendProfile {
        self.inner.profile()
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if key == "index" {
            return Err(VykarError::Other("injected index PUT failure".into()));
        }
        self.inner.put(key, data)
    }

    // Packs never take the `index` key.
    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        self.inner.put_pack(key, data)
    }

    fn begin_staged_put(&self, prefix: &str) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        self.inner.begin_staged_put(prefix)
    }
}

struct Env {
    dir: tempfile::TempDir,
    repo_dir: PathBuf,
    cache_dir: PathBuf,
    config: VykarConfig,
}

impl Env {
    /// An empty repository; this client's caches live in a private dir.
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("repo");
        let cache_dir = dir.path().join("cache");
        let mut config = init_repo_v3(&repo_dir);
        config.cache_dir = Some(cache_dir.to_string_lossy().into_owned());
        config.retention = RetentionConfig {
            keep_last: Some(1),
            ..RetentionConfig::default()
        };
        Self {
            dir,
            repo_dir,
            cache_dir,
            config,
        }
    }

    /// A source directory holding `files` (name, fill byte), 64 KiB each.
    fn source(&self, name: &str, files: &[(&str, u8)]) -> PathBuf {
        let src = self.dir.path().join(name);
        fs::create_dir_all(&src).unwrap();
        for (file, byte) in files {
            fs::write(src.join(file), vec![*byte; 64 * 1024]).unwrap();
        }
        src
    }

    /// Back up `src` with this client's config, returning the commit stages.
    fn backup(&self, src: &Path, label: &str, name: &str) -> Vec<&'static str> {
        // Distinct snapshot times for retention ordering.
        std::thread::sleep(Duration::from_millis(2));
        let source_paths = vec![src.to_string_lossy().to_string()];
        let mut stages = Vec::new();
        let mut on_event = |e: BackupProgressEvent| {
            if let BackupProgressEvent::CommitStage { stage } = e {
                stages.push(stage);
            }
        };
        backup::run_impl(
            &self.config,
            backup::BackupRequest {
                snapshot_name: name,
                passphrase: None,
                source_paths: &source_paths,
                source_label: label,
                exclude_patterns: &[],
                exclude_if_present: &[],
                one_file_system: true,
                git_ignore: false,
                xattrs_enabled: false,
                compression: crate::compress::Compression::None,
                command_dumps: &[],
                verbose: false,
            },
            Some(&mut on_event),
            None,
            None,
        )
        .unwrap();
        stages
    }

    /// Another client, with its own cache dir, backs up `src`.
    fn backup_elsewhere(&self, src: &Path, label: &str, name: &str) {
        std::thread::sleep(Duration::from_millis(2));
        let mut other = self.config.clone();
        other.cache_dir = Some(
            self.dir
                .path()
                .join("other-cache")
                .to_string_lossy()
                .into_owned(),
        );
        backup_single_source(&other, src, label, name);
    }

    fn local(&self) -> Box<dyn StorageBackend> {
        Box::new(
            vykar_storage::local_backend::LocalBackend::new(self.repo_dir.to_str().unwrap())
                .unwrap(),
        )
    }

    /// Open over `storage` with this client's cache dir.
    fn open(&self, storage: Box<dyn StorageBackend>, opts: OpenOptions) -> Repository {
        Repository::open(storage, None, Some(self.cache_dir.clone()), opts).unwrap()
    }

    /// Open with the index over plain local storage and a throwaway cache
    /// dir, so the index is decoded from storage.
    fn open_fresh(&self) -> Repository {
        let cache = tempfile::tempdir_in(self.dir.path()).unwrap();
        Repository::open(
            self.local(),
            None,
            Some(cache.path().to_path_buf()),
            OpenOptions::new().with_index(),
        )
        .unwrap()
    }

    fn sources(&self, labels: &[&str]) -> Vec<SourceEntry> {
        labels
            .iter()
            .map(|l| source_entry(&self.dir.path().join(l), l))
            .collect()
    }

    fn assert_check_clean(&self) {
        let result = commands::check::run(&self.config, None, true, false).unwrap();
        assert!(
            result.errors.is_empty(),
            "check errors: {:?}",
            result.errors
        );
    }
}

fn entries(repo: &Repository) -> HashMap<ChunkId, ChunkIndexEntry> {
    repo.chunk_index().iter().map(|(id, e)| (*id, *e)).collect()
}

/// Whether the full, dedup and restore caches in `cache_dir` are valid at
/// `generation`.
fn caches_at(repo_id: &[u8], cache_dir: &Path, generation: u64) -> [bool; 3] {
    let cd = Some(cache_dir);
    [
        MmapFullIndexCache::open(repo_id, generation, cd).is_some(),
        MmapDedupCache::open(repo_id, generation, cd).is_some(),
        MmapRestoreCache::open(repo_id, generation, cd).is_some(),
    ]
}

fn find_file(dir: &Path, name: &str) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()? {
        let path = entry.ok()?.path();
        if path.is_dir() {
            if let Some(found) = find_file(&path, name) {
                return Some(found);
            }
        } else if path.file_name().is_some_and(|n| n == name) {
            return Some(path);
        }
    }
    None
}

// --- (A) local caches refreshed after a maintenance index write ---

#[test]
fn prune_refreshes_local_caches_so_next_backup_is_fast() {
    let env = Env::new();
    let src = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src, "a", "s1");
    fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
    env.backup(&src, "a", "s2");

    let (stats, _) = commands::prune::run(
        &env.config,
        None,
        false,
        false,
        &env.sources(&["a"]),
        &[],
        None,
    )
    .unwrap();
    assert_eq!(stats.pruned, 1);
    assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);

    let remote = env.open_fresh();
    let id = remote.config.id.clone();
    assert_eq!(
        caches_at(&id, &env.cache_dir, remote.index_generation()),
        [true; 3]
    );

    fs::write(src.join("three.bin"), vec![0x33u8; 64 * 1024]).unwrap();
    let stages = env.backup(&src, "a", "s3");
    assert!(stages.contains(&FAST), "expected fast path: {stages:?}");
    assert!(!stages.contains(&SLOW), "unexpected slow path: {stages:?}");
    env.assert_check_clean();
}

#[test]
fn compact_refreshes_local_caches() {
    let env = Env::new();
    let src = env.source("a", &[("one.bin", 0x11), ("dead.bin", 0x44)]);
    env.backup(&src, "a", "s1");
    fs::remove_file(src.join("dead.bin")).unwrap();
    fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
    env.backup(&src, "a", "s2");
    // s1's data pack now holds a live chunk (one.bin) and a dead one.
    commands::delete::run(&env.config, None, &["s1"], false, None).unwrap();
    let before = env.open_fresh().index_generation();

    let stats = commands::compact::run(&env.config, None, 0.0, None, false, None).unwrap();
    assert!(stats.packs_repacked > 0, "{stats:?}");

    let remote = env.open_fresh();
    assert_ne!(remote.index_generation(), before, "compact wrote the index");
    let id = remote.config.id.clone();
    assert_eq!(
        caches_at(&id, &env.cache_dir, remote.index_generation()),
        [true; 3]
    );

    fs::write(src.join("three.bin"), vec![0x33u8; 64 * 1024]).unwrap();
    let stages = env.backup(&src, "a", "s3");
    assert!(stages.contains(&FAST), "expected fast path: {stages:?}");
    env.assert_check_clean();
}

#[test]
fn maintenance_does_not_create_caches_on_a_host_without_them() {
    let env = Env::new();
    let src = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src, "a", "s1");
    fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
    env.backup(&src, "a", "s2");

    let other_cache = env.dir.path().join("maintenance-only");
    fs::create_dir_all(&other_cache).unwrap();
    let mut config = env.config.clone();
    config.cache_dir = Some(other_cache.to_string_lossy().into_owned());
    let (stats, _) =
        commands::prune::run(&config, None, false, false, &env.sources(&["a"]), &[], None).unwrap();
    assert_eq!(stats.pruned, 1);

    let id = env.open_fresh().config.id.clone();
    let cd = Some(other_cache.as_path());
    for path in [
        dedup_cache::full_index_cache_path(&id, cd),
        dedup_cache::dedup_cache_path(&id, cd),
        dedup_cache::restore_cache_path(&id, cd),
    ] {
        let path = path.unwrap();
        assert!(!path.exists(), "{} was created", path.display());
    }
}

#[test]
fn failed_index_write_in_prune_leaves_caches_untouched() {
    let env = Env::new();
    let src = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src, "a", "s1");
    fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
    env.backup(&src, "a", "s2");
    let remote = env.open_fresh();
    let (id, generation) = (remote.config.id.clone(), remote.index_generation());
    assert_eq!(caches_at(&id, &env.cache_dir, generation), [true; 3]);

    let mut repo = env.open(
        Box::new(FailIndexPut { inner: env.local() }),
        OpenOptions::new(),
    );
    let (stats, _) = commands::prune::run_on_repo(
        &mut repo,
        &env.config,
        false,
        false,
        &env.sources(&["a"]),
        &[],
        None,
    )
    .unwrap();
    assert_eq!(stats.pruned, 1);
    assert_eq!(stats.warnings.len(), 1, "{:?}", stats.warnings);
    assert!(stats.warnings[0].contains("persisting refcount changes failed"));
    drop(repo);

    // The caches still describe the remote index, which was not replaced.
    assert_eq!(caches_at(&id, &env.cache_dir, generation), [true; 3]);
    let remote = env.open_fresh();
    assert_eq!(remote.index_generation(), generation);
    let own = env.open(env.local(), OpenOptions::new().with_index());
    assert_eq!(own.index_generation(), generation);
    assert_eq!(entries(&own), entries(&remote));
}

#[test]
fn lock_lost_before_save_leaves_caches_untouched() {
    let env = Env::new();
    let src = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src, "a", "s1");
    let remote = env.open_fresh();
    let (id, generation) = (remote.config.id.clone(), remote.index_generation());
    assert_eq!(caches_at(&id, &env.cache_dir, generation), [true; 3]);

    let mut repo = env.open(env.local(), OpenOptions::new());
    let result = with_maintenance_lock(&mut repo, |repo| {
        repo.load_chunk_index_under_lock()?;
        let first = *repo.chunk_index().iter().next().unwrap().0;
        repo.chunk_index_mut().increment_refcount(&first);
        for key in repo.storage.list("locks/").unwrap() {
            if key.ends_with(".json") {
                repo.storage.delete(&key).unwrap();
            }
        }
        repo.save_state()
    });
    let err = result.unwrap_err();
    assert!(
        matches!(err, VykarError::LockExpired(_)),
        "expected LockExpired, got: {err}"
    );
    drop(repo);

    assert_eq!(caches_at(&id, &env.cache_dir, generation), [true; 3]);
    assert_eq!(env.open_fresh().index_generation(), generation);
}

// --- (C) index loaded under the maintenance lock ---

/// Chunks in the remote index after `f` that were not there before it.
fn new_chunks(env: &Env, f: impl FnOnce()) -> HashMap<ChunkId, ChunkIndexEntry> {
    let before = entries(&env.open_fresh());
    f();
    entries(&env.open_fresh())
        .into_iter()
        .filter(|(id, _)| !before.contains_key(id))
        .collect()
}

fn assert_present(env: &Env, chunks: &HashMap<ChunkId, ChunkIndexEntry>) {
    assert!(!chunks.is_empty());
    let after = entries(&env.open_fresh());
    for (id, entry) in chunks {
        let found = after.get(id).unwrap_or_else(|| panic!("chunk {id} lost"));
        assert_eq!(found.pack_id, entry.pack_id, "chunk {id}");
        assert_eq!(found.refcount, entry.refcount, "chunk {id}");
    }
}

#[test]
fn prune_uses_index_current_at_lock_time() {
    let env = Env::new();
    let src_a = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src_a, "a", "s1");
    fs::write(src_a.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
    env.backup(&src_a, "a", "s2");

    // Opened with the index before the other client commits.
    let mut repo = env.open(env.local(), OpenOptions::new().with_index());
    let src_b = env.source("b", &[("other.bin", 0x55)]);
    let s3_chunks = new_chunks(&env, || env.backup_elsewhere(&src_b, "b", "s3"));

    let (stats, _) = commands::prune::run_on_repo(
        &mut repo,
        &env.config,
        false,
        false,
        &env.sources(&["a", "b"]),
        &[],
        None,
    )
    .unwrap();
    drop(repo);
    assert_eq!(stats.pruned, 1);
    assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);

    let names: Vec<String> = env
        .open_fresh()
        .manifest()
        .snapshots
        .iter()
        .map(|s| s.name.clone())
        .collect();
    assert_eq!(names.len(), 2, "{names:?}");
    assert!(names.contains(&"s2".to_string()) && names.contains(&"s3".to_string()));
    assert_present(&env, &s3_chunks);
    env.assert_check_clean();
}

#[test]
fn delete_uses_index_current_at_lock_time() {
    let env = Env::new();
    let src_a = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src_a, "a", "s1");

    let mut repo = env.open(env.local(), OpenOptions::new().with_index());
    let src_b = env.source("b", &[("other.bin", 0x55)]);
    let s2_chunks = new_chunks(&env, || env.backup_elsewhere(&src_b, "b", "s2"));

    let result = commands::delete::run_on_repo(&mut repo, &["s1"], false, None).unwrap();
    drop(repo);
    assert_eq!(result.stats.len(), 1);
    assert!(result.warnings.is_empty(), "{:?}", result.warnings);

    assert_present(&env, &s2_chunks);
    env.assert_check_clean();
}

#[test]
fn compact_does_not_delete_packs_committed_after_open() {
    let env = Env::new();
    let src_a = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src_a, "a", "s1");

    let mut repo = env.open(env.local(), OpenOptions::new().with_index());
    let src_b = env.source("b", &[("other.bin", 0x55)]);
    let s2_chunks = new_chunks(&env, || env.backup_elsewhere(&src_b, "b", "s2"));

    let stats = commands::compact::run_on_repo(&mut repo, 0.0, None, false, None).unwrap();
    drop(repo);
    assert_eq!(stats.packs_orphan, 0, "{stats:?}");

    assert_present(&env, &s2_chunks);
    let storage = env.local();
    for entry in s2_chunks.values() {
        assert!(
            storage.exists(&entry.pack_id.storage_key()).unwrap(),
            "pack {} deleted",
            entry.pack_id
        );
    }
    env.assert_check_clean();

    let dest = env.dir.path().join("restored");
    fs::create_dir_all(&dest).unwrap();
    commands::restore::run(
        &env.config,
        None,
        "s2",
        dest.to_str().unwrap(),
        None,
        false,
        false,
        None,
    )
    .unwrap();
    let restored = find_file(&dest, "other.bin").expect("other.bin restored");
    assert_eq!(
        fs::read(restored).unwrap(),
        fs::read(src_b.join("other.bin")).unwrap()
    );
}

#[test]
fn prune_with_nothing_to_prune_does_not_read_index() {
    let env = Env::new();
    let src = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src, "a", "s1");
    fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
    env.backup(&src, "a", "s2");

    let reads = Arc::new(AtomicUsize::new(0));
    let counting = || {
        Box::new(CountIndexReads {
            inner: env.local(),
            reads: Arc::clone(&reads),
        })
    };

    let mut keep_all = env.config.clone();
    keep_all.retention.keep_last = Some(10);
    let mut repo = env.open(counting(), OpenOptions::new());
    let (stats, _) = commands::prune::run_on_repo(
        &mut repo,
        &keep_all,
        false,
        false,
        &env.sources(&["a"]),
        &[],
        None,
    )
    .unwrap();
    assert_eq!(stats.pruned, 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);

    // A dry run with a candidate does not read it either.
    let mut repo = env.open(counting(), OpenOptions::new());
    let (stats, _) = commands::prune::run_on_repo(
        &mut repo,
        &env.config,
        true,
        false,
        &env.sources(&["a"]),
        &[],
        None,
    )
    .unwrap();
    assert_eq!(stats.pruned, 1);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(env.open_fresh().manifest().snapshots.len(), 2);
}

#[test]
fn prune_loads_index_after_session_check() {
    let env = Env::new();
    let src = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src, "a", "s1");
    fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
    env.backup(&src, "a", "s2");

    crate::repo::lock::register_session(env.local().as_ref(), "live-session").unwrap();

    let reads = Arc::new(AtomicUsize::new(0));
    let mut repo = env.open(
        Box::new(CountIndexReads {
            inner: env.local(),
            reads: Arc::clone(&reads),
        }),
        OpenOptions::new(),
    );
    let err = commands::prune::run_on_repo(
        &mut repo,
        &env.config,
        false,
        false,
        &env.sources(&["a"]),
        &[],
        None,
    )
    .err()
    .unwrap();
    assert!(
        matches!(err, VykarError::ActiveSessions(_)),
        "expected ActiveSessions, got: {err}"
    );
    assert_eq!(reads.load(Ordering::SeqCst), 0);
}
