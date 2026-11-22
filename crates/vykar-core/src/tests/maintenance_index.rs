//! Maintenance operations load the chunk index under the repository lock,
//! and leave this machine's chunk table at the state they write.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use crate::commands;
use crate::commands::backup::{self, BackupProgressEvent};
use crate::commands::util::with_maintenance_lock;
use crate::config::{RetentionConfig, SourceEntry, VykarConfig};
use crate::index::ChunkIndexEntry;
use crate::index::dedup_cache;
use crate::index::table::{self, ChunkTable};
use crate::repo::control::CancelToken;
use crate::repo::format::ObjectType;
use crate::repo::{OpenOptions, RepoFormat, Repository};

use super::helpers::{backup_single_source, make_test_config, source_entry};

/// GETs of `index` (the head) and of `index.d/` objects.
#[derive(Default)]
struct IndexReads {
    head: AtomicUsize,
    objects: AtomicUsize,
}

impl IndexReads {
    /// `(head GETs, index.d/ GETs)` so far.
    fn counts(&self) -> (usize, usize) {
        (
            self.head.load(Ordering::SeqCst),
            self.objects.load(Ordering::SeqCst),
        )
    }

    fn count(&self, key: &str) {
        if key == "index" {
            self.head.fetch_add(1, Ordering::SeqCst);
        } else if key.starts_with(crate::repo::index_store::INDEX_OBJECT_DIR) {
            self.objects.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Counts reads of `index`, conditional or not, and of `index.d/` objects.
struct CountIndexReads {
    inner: Box<dyn StorageBackend>,
    reads: Arc<IndexReads>,
}

impl InnerBackend for CountIndexReads {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for CountIndexReads;
    except [get];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.reads.count(key);
        self.inner.get(key)
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
    except [put, put_pack, begin_staged_put, conditional, profile];

    // Test wrapper: keeps every write on `put`.
    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

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
    /// An empty repository at `format`; this client's caches live in a private dir.
    fn new(format: RepoFormat) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("repo");
        let cache_dir = dir.path().join("cache");
        let mut config = make_test_config(&repo_dir);
        commands::init::run_at_format(&config, None, format).unwrap();
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
        self.backup_with(src, label, name, None)
    }

    /// Back up with every GET counted in `reads`.
    fn backup_counting(
        &self,
        src: &Path,
        label: &str,
        name: &str,
        reads: &Arc<IndexReads>,
    ) -> Vec<&'static str> {
        let wrap = |inner: Box<dyn StorageBackend>| -> Box<dyn StorageBackend> {
            Box::new(CountIndexReads {
                inner,
                reads: Arc::clone(reads),
            })
        };
        self.backup_with(src, label, name, Some(&wrap))
    }

    fn backup_with(
        &self,
        src: &Path,
        label: &str,
        name: &str,
        wrap: Option<backup::BackendWrapper<'_>>,
    ) -> Vec<&'static str> {
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
            &CancelToken::new(),
            wrap,
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

    /// Plain local storage wrapped to count index reads into `reads`.
    fn counting(&self, reads: &Arc<IndexReads>) -> Box<dyn StorageBackend> {
        Box::new(CountIndexReads {
            inner: self.local(),
            reads: Arc::clone(reads),
        })
    }

    /// The bytes at `index`.
    fn index_bytes(&self) -> Vec<u8> {
        fs::read(self.repo_dir.join("index")).unwrap()
    }

    /// Files under `index.d/` (0 when the directory does not exist).
    fn index_objects(&self) -> usize {
        self.index_object_names().len()
    }

    /// Names of the files under `index.d/`.
    fn index_object_names(&self) -> BTreeSet<String> {
        let Ok(dir) = fs::read_dir(self.repo_dir.join("index.d")) else {
            return BTreeSet::new();
        };
        dir.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
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
    repo.chunk_index().iter().collect()
}

/// Whether `cache_dir` holds a valid chunk table of the state whose
/// generation is `generation`.
fn table_at(repo_id: &[u8], cache_dir: &Path, generation: u64) -> bool {
    let Some(dir) = crate::cache_paths::repo_cache_dir(repo_id, Some(cache_dir)) else {
        return false;
    };
    table::list_tables(&dir).into_iter().any(|(state, path)| {
        dedup_cache::generation_for_state(&state) == generation
            && ChunkTable::open_file(&path).is_some()
    })
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

// --- (A) the local table after a maintenance index write ---

const FORMATS: [RepoFormat; 1] = [RepoFormat::V5];

/// The next backup used the local table: no `index.d/` download.
fn assert_backup_uses_caches(env: &Env, src: &Path, name: &str) {
    let reads = Arc::new(IndexReads::default());
    env.backup_counting(src, "a", name, &reads);
    let (head, objects) = reads.counts();
    assert!(head > 0, "the counter saw no head GET");
    assert_eq!(objects, 0, "backup downloaded index objects");
}

#[test]
fn prune_leaves_the_table_at_the_new_state_so_next_backup_is_fast() {
    for format in FORMATS {
        let env = Env::new(format);
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
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(stats.pruned, 1);
        assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);

        let remote = env.open_fresh();
        let id = remote.config.id.clone();
        assert!(
            table_at(&id, &env.cache_dir, remote.index_generation()),
            "{format:?}"
        );

        fs::write(src.join("three.bin"), vec![0x33u8; 64 * 1024]).unwrap();
        assert_backup_uses_caches(&env, &src, "s3");
        env.assert_check_clean();
    }
}

#[test]
fn compact_leaves_the_table_at_the_new_state() {
    for format in FORMATS {
        let env = Env::new(format);
        let src = env.source("a", &[("one.bin", 0x11), ("dead.bin", 0x44)]);
        env.backup(&src, "a", "s1");
        fs::remove_file(src.join("dead.bin")).unwrap();
        fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
        env.backup(&src, "a", "s2");
        // s1's data pack now holds a live chunk (one.bin) and a dead one.
        commands::delete::run(&env.config, None, &["s1"], false, &CancelToken::new()).unwrap();
        let before = env.open_fresh().index_generation();

        let stats =
            commands::compact::run(&env.config, None, 0.0, None, false, &CancelToken::new())
                .unwrap();
        assert!(stats.packs_repacked > 0, "{stats:?}");

        let remote = env.open_fresh();
        assert_ne!(remote.index_generation(), before, "compact wrote the index");
        let id = remote.config.id.clone();
        assert!(
            table_at(&id, &env.cache_dir, remote.index_generation()),
            "{format:?}"
        );

        fs::write(src.join("three.bin"), vec![0x33u8; 64 * 1024]).unwrap();
        assert_backup_uses_caches(&env, &src, "s3");
        env.assert_check_clean();
    }
}

/// Maintenance on a host without a table of the repository writes a
/// segment and leaves that host's cache dir with the table of the new state.
#[test]
fn maintenance_on_a_host_without_caches_segmented() {
    let env = Env::new(RepoFormat::V5);
    let src = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src, "a", "s1");
    fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
    env.backup(&src, "a", "s2");
    let head_before = env.open_fresh().index_head().unwrap().clone();

    let other_cache = env.dir.path().join("maintenance-only");
    fs::create_dir_all(&other_cache).unwrap();
    let mut config = env.config.clone();
    config.cache_dir = Some(other_cache.to_string_lossy().into_owned());
    let (stats, _) = commands::prune::run(
        &config,
        None,
        false,
        false,
        &env.sources(&["a"]),
        &[],
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(stats.pruned, 1);

    let remote = env.open_fresh();
    let head_after = remote.index_head().unwrap().clone();
    assert_eq!(head_after.base, head_before.base);
    assert_eq!(head_after.segments.len(), head_before.segments.len() + 1);

    let id = remote.config.id.clone();
    let generation = dedup_cache::generation_for_state(&head_after.state_hash);
    assert!(table_at(&id, &other_cache, generation));
}

#[test]
fn failed_index_write_in_prune_leaves_caches_untouched() {
    for format in FORMATS {
        let env = Env::new(format);
        let src = env.source("a", &[("one.bin", 0x11)]);
        env.backup(&src, "a", "s1");
        fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
        env.backup(&src, "a", "s2");
        let remote = env.open_fresh();
        let (id, generation) = (remote.config.id.clone(), remote.index_generation());
        assert!(table_at(&id, &env.cache_dir, generation));
        let index_before = env.index_bytes();
        let objects_before = env.index_object_names();

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
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(stats.pruned, 1);
        assert_eq!(stats.warnings.len(), 1, "{:?}", stats.warnings);
        assert!(stats.warnings[0].contains("persisting refcount changes failed"));
        drop(repo);

        // The table still describes the remote index, which was not replaced.
        assert!(table_at(&id, &env.cache_dir, generation));
        assert_eq!(env.index_bytes(), index_before, "{format:?}");
        let remote = env.open_fresh();
        assert_eq!(remote.index_generation(), generation);
        let own = env.open(env.local(), OpenOptions::new().with_index());
        assert_eq!(own.index_generation(), generation);
        assert_eq!(entries(&own), entries(&remote));
        drop(own);

        // The segment PUT went through before the failed head PUT.
        let objects = env.index_object_names();
        let debris: Vec<&String> = objects.difference(&objects_before).collect();
        assert_eq!(debris.len(), 1, "{debris:?}");
        assert_eq!(objects.len(), objects_before.len() + 1);
        let bytes = fs::read(env.repo_dir.join("index.d").join(debris[0])).unwrap();
        assert_eq!(bytes[0], ObjectType::IndexSegment as u8);
        let head = remote.index_head().unwrap();
        let named = head
            .base
            .iter()
            .map(|b| b.name)
            .chain(head.segments.iter().map(|s| s.name))
            .chain(head.retired.iter().copied())
            .any(|n| n.to_hex() == *debris[0]);
        assert!(!named, "debris is named by the head");
        drop(remote);

        commands::compact::run(&env.config, None, 100.0, None, false, &CancelToken::new()).unwrap();
        assert_eq!(env.index_object_names(), objects_before);
    }
}

#[test]
fn lock_lost_before_save_leaves_caches_untouched() {
    for format in FORMATS {
        let env = Env::new(format);
        let src = env.source("a", &[("one.bin", 0x11)]);
        env.backup(&src, "a", "s1");
        let remote = env.open_fresh();
        let (id, generation) = (remote.config.id.clone(), remote.index_generation());
        assert!(table_at(&id, &env.cache_dir, generation));
        let index_before = env.index_bytes();
        let objects_before = env.index_objects();

        let mut repo = env.open(env.local(), OpenOptions::new());
        let result = with_maintenance_lock(&mut repo, |repo| {
            repo.load_chunk_index_under_lock()?;
            let first = repo.chunk_index().iter().next().unwrap().0;
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

        assert!(table_at(&id, &env.cache_dir, generation));
        assert_eq!(env.open_fresh().index_generation(), generation);
        assert_eq!(env.index_bytes(), index_before, "{format:?}");
        // The segment is PUT, then the lock checked before the head PUT.
        assert_eq!(env.index_objects(), objects_before + 1, "{format:?}");
    }
}

/// Changes a refcount and persists the index; call under the maintenance lock.
fn bump_and_save(repo: &mut Repository) -> Result<()> {
    repo.load_chunk_index_under_lock()?;
    let first = repo.chunk_index().iter().next().unwrap().0;
    repo.chunk_index_mut().increment_refcount(&first);
    repo.save_state()
}

fn delete_locks(repo: &Repository) {
    for key in repo.storage.list("locks/").unwrap() {
        if key.ends_with(".json") {
            repo.storage.delete(&key).unwrap();
        }
    }
}

/// Persists a refcount change under the maintenance lock, then loses the
/// lock before the action returns. Returns the generation before and after.
fn save_then_lose_lock(env: &Env) -> (u64, u64) {
    let before = env.open_fresh().index_generation();
    let mut repo = env.open(env.local(), OpenOptions::new());
    with_maintenance_lock(&mut repo, |repo| {
        bump_and_save(repo)?;
        delete_locks(repo);
        Ok(())
    })
    .unwrap();
    drop(repo);
    (before, env.open_fresh().index_generation())
}

#[test]
fn lock_lost_after_save_still_installs_the_table_segmented() {
    let env = Env::new(RepoFormat::V5);
    let src = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src, "a", "s1");
    let id = env.open_fresh().config.id.clone();

    let (before, after) = save_then_lose_lock(&env);
    assert_ne!(before, after);
    assert!(table_at(&id, &env.cache_dir, after));
}

/// Saves under the maintenance lock, loses it, and lets another writer on
/// this host persist a newer index. Returns our generation.
fn save_then_get_taken_over(env: &Env) -> u64 {
    let mut repo = env.open(env.local(), OpenOptions::new());
    let ours = with_maintenance_lock(&mut repo, |repo| {
        bump_and_save(repo)?;
        let ours = repo.index_generation();
        delete_locks(repo);
        let mut other = env.open(env.local(), OpenOptions::new());
        with_maintenance_lock(&mut other, bump_and_save)?;
        Ok(ours)
    })
    .unwrap();
    drop(repo);
    ours
}

/// After a takeover both states' tables are in the cache dir (ours is
/// younger than `TABLE_KEEP_AGE`, and no table is replaced); the next open
/// and backup use the newer one.
#[test]
fn takeover_keeps_the_newer_table_segmented() {
    let env = Env::new(RepoFormat::V5);
    let src = env.source("a", &[("one.bin", 0x11)]);
    env.backup(&src, "a", "s1");

    let ours = save_then_get_taken_over(&env);

    let remote = env.open_fresh();
    let id = remote.config.id.clone();
    assert_ne!(ours, remote.index_generation());
    assert!(table_at(&id, &env.cache_dir, ours));
    assert!(table_at(&id, &env.cache_dir, remote.index_generation()));

    let own = env.open(env.local(), OpenOptions::new().with_index());
    assert_eq!(own.index_generation(), remote.index_generation());
    assert_eq!(entries(&own), entries(&remote));
    drop(own);

    fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
    assert_backup_uses_caches(&env, &src, "s2");
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
    for format in FORMATS {
        let env = Env::new(format);
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
            &CancelToken::new(),
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
}

#[test]
fn delete_uses_index_current_at_lock_time() {
    for format in FORMATS {
        let env = Env::new(format);
        let src_a = env.source("a", &[("one.bin", 0x11)]);
        env.backup(&src_a, "a", "s1");

        let mut repo = env.open(env.local(), OpenOptions::new().with_index());
        let src_b = env.source("b", &[("other.bin", 0x55)]);
        let s2_chunks = new_chunks(&env, || env.backup_elsewhere(&src_b, "b", "s2"));

        let result =
            commands::delete::run_on_repo(&mut repo, &["s1"], false, &CancelToken::new()).unwrap();
        drop(repo);
        assert_eq!(result.stats.len(), 1);
        assert!(result.warnings.is_empty(), "{:?}", result.warnings);

        assert_present(&env, &s2_chunks);
        env.assert_check_clean();
    }
}

#[test]
fn compact_does_not_delete_packs_committed_after_open() {
    for format in FORMATS {
        let env = Env::new(format);
        let src_a = env.source("a", &[("one.bin", 0x11)]);
        env.backup(&src_a, "a", "s1");

        let mut repo = env.open(env.local(), OpenOptions::new().with_index());
        let src_b = env.source("b", &[("other.bin", 0x55)]);
        let s2_chunks = new_chunks(&env, || env.backup_elsewhere(&src_b, "b", "s2"));

        let stats =
            commands::compact::run_on_repo(&mut repo, 0.0, None, false, &CancelToken::new())
                .unwrap();
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
            &CancelToken::new(),
        )
        .unwrap();
        let restored = find_file(&dest, "other.bin").expect("other.bin restored");
        assert_eq!(
            fs::read(restored).unwrap(),
            fs::read(src_b.join("other.bin")).unwrap()
        );
    }
}

/// Index reads an open without the index makes: the single head GET.
const OPEN_READS: (usize, usize) = (1, 0);

#[test]
fn prune_with_nothing_to_prune_does_not_read_index() {
    for format in FORMATS {
        let env = Env::new(format);
        let src = env.source("a", &[("one.bin", 0x11)]);
        env.backup(&src, "a", "s1");
        fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
        env.backup(&src, "a", "s2");

        let mut keep_all = env.config.clone();
        keep_all.retention.keep_last = Some(10);
        let reads = Arc::new(IndexReads::default());
        let mut repo = env.open(env.counting(&reads), OpenOptions::new());
        assert_eq!(reads.counts(), OPEN_READS, "{format:?}: open");
        let (stats, _) = commands::prune::run_on_repo(
            &mut repo,
            &keep_all,
            false,
            false,
            &env.sources(&["a"]),
            &[],
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(stats.pruned, 0);
        assert_eq!(reads.counts(), OPEN_READS, "{format:?}: prune");

        // A dry run with a candidate does not read it either.
        let reads = Arc::new(IndexReads::default());
        let mut repo = env.open(env.counting(&reads), OpenOptions::new());
        assert_eq!(reads.counts(), OPEN_READS, "{format:?}: open");
        let (stats, _) = commands::prune::run_on_repo(
            &mut repo,
            &env.config,
            true,
            false,
            &env.sources(&["a"]),
            &[],
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(stats.pruned, 1);
        assert_eq!(reads.counts(), OPEN_READS, "{format:?}: dry run");
        assert_eq!(env.open_fresh().manifest().snapshots.len(), 2);
    }
}

#[test]
fn prune_loads_index_after_session_check() {
    for format in FORMATS {
        let env = Env::new(format);
        let src = env.source("a", &[("one.bin", 0x11)]);
        env.backup(&src, "a", "s1");
        fs::write(src.join("two.bin"), vec![0x22u8; 64 * 1024]).unwrap();
        env.backup(&src, "a", "s2");

        crate::repo::lock::register_session(env.local().as_ref(), "live-session").unwrap();

        let reads = Arc::new(IndexReads::default());
        let mut repo = env.open(env.counting(&reads), OpenOptions::new());
        assert_eq!(reads.counts(), OPEN_READS, "{format:?}: open");
        let err = commands::prune::run_on_repo(
            &mut repo,
            &env.config,
            false,
            false,
            &env.sources(&["a"]),
            &[],
            &CancelToken::new(),
        )
        .err()
        .unwrap();
        assert!(
            matches!(err, VykarError::ActiveSessions(_)),
            "expected ActiveSessions, got: {err}"
        );
        assert_eq!(reads.counts(), OPEN_READS, "{format:?}: prune");
    }
}
