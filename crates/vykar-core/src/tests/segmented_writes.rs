//! Writing repositories in the segmented index formats (v4/v5): backup
//! commits, maintenance writes, folds, garbage collection and repair.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::helpers::{backup_single_source, make_test_config};
use crate::commands;
use crate::commands::check::RepairMode;
use crate::config::VykarConfig;
use crate::index::head::IndexHead;
use crate::index::{ChunkIndex, IndexBlob};
use crate::repo::format::{pack_object_streaming_with_context, ObjectType};
use crate::repo::index_store::INDEX_OBJECT_DIR;
use crate::repo::{EncryptionMode, OpenOptions, RepoFormat, Repository};
use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{delegate_storage_backend, InnerBackend, StorageBackend};
use vykar_types::error::{Result, VykarError};

struct Fixture {
    tmp: tempfile::TempDir,
    repo_dir: PathBuf,
    source: PathBuf,
    config: VykarConfig,
}

fn fixture(format: RepoFormat) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source = tmp.path().join("src");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source).unwrap();
    let mut config = make_test_config(&repo_dir);
    let cache = tmp.path().join("cache");
    config.cache_dir = Some(cache.to_string_lossy().into_owned());
    Repository::init_at_format(
        Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap()),
        EncryptionMode::None,
        config.chunker.clone(),
        None,
        Some(&config.repository),
        Some(cache),
        format,
    )
    .unwrap();
    Fixture {
        tmp,
        repo_dir,
        source,
        config,
    }
}

impl Fixture {
    fn cache(&self) -> PathBuf {
        PathBuf::from(self.config.cache_dir.as_ref().unwrap())
    }

    /// The same repository seen from another machine (own cache directory).
    fn other_client(&self, name: &str) -> VykarConfig {
        let mut config = self.config.clone();
        config.cache_dir = Some(self.tmp.path().join(name).to_string_lossy().into_owned());
        config
    }

    fn storage(&self) -> Box<dyn StorageBackend> {
        Box::new(LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap())
    }

    fn open_with(
        &self,
        storage: Box<dyn StorageBackend>,
        cache: &Path,
        opts: OpenOptions,
    ) -> Result<Repository> {
        Repository::open(storage, None, Some(cache.to_path_buf()), opts)
    }

    fn open(&self, cache: &Path, with_index: bool) -> Result<Repository> {
        let opts = if with_index {
            OpenOptions::new().with_index()
        } else {
            OpenOptions::new()
        };
        self.open_with(self.storage(), cache, opts)
    }

    fn head(&self) -> IndexHead {
        self.open(&self.cache(), false)
            .unwrap()
            .index_head()
            .cloned()
            .unwrap()
    }

    /// Objects under `index.d/` on disk.
    fn index_objects(&self) -> usize {
        std::fs::read_dir(self.repo_dir.join(INDEX_OBJECT_DIR))
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_type().unwrap().is_file())
            .count()
    }

    fn write(&self, name: &str, content: &str) {
        std::fs::write(self.source.join(name), content).unwrap();
    }

    fn backup(&self, config: &VykarConfig, name: &str) {
        backup_single_source(config, &self.source, "src", name);
    }

    fn check_clean(&self, config: &VykarConfig) {
        let result = commands::check::run(config, None, true, false).unwrap();
        assert!(result.errors.is_empty(), "{:?}", result.errors);
    }

    fn restore(&self, config: &VykarConfig, snapshot: &str) -> PathBuf {
        let dest = self.tmp.path().join(format!("restore-{snapshot}"));
        commands::restore::run(
            config,
            None,
            snapshot,
            dest.to_str().unwrap(),
            None,
            false,
            true,
            None,
        )
        .unwrap();
        dest
    }
}

/// The file called `name` somewhere below `dir`.
fn find(dir: &Path, name: &str) -> PathBuf {
    fn walk(dir: &Path, name: &str) -> Option<PathBuf> {
        std::fs::read_dir(dir).ok()?.find_map(|entry| {
            let path = entry.ok()?.path();
            if path.is_dir() {
                walk(&path, name)
            } else {
                path.file_name().is_some_and(|n| n == name).then_some(path)
            }
        })
    }
    walk(dir, name).unwrap_or_else(|| panic!("{name} not found below {}", dir.display()))
}

fn sorted_entries(index: &ChunkIndex) -> Vec<String> {
    let mut v: Vec<String> = index
        .iter()
        .map(|(id, e)| format!("{id:?} {e:?}"))
        .collect();
    v.sort();
    v
}

/// The index as a client with no local cache builds it.
fn cold_index(fx: &Fixture, name: &str) -> Vec<String> {
    let cold = fx.tmp.path().join(name);
    sorted_entries(fx.open(&cold, true).unwrap().chunk_index())
}

/// Every backup appends one segment and uploads nothing else to `index.d/`
/// (a small index stays below the fold limits). Clients with and without a
/// local cache read the same index, and the snapshots restore.
#[test]
fn backups_append_one_segment_each() {
    for format in [RepoFormat::V4, RepoFormat::V5] {
        let fx = fixture(format);
        fx.write("a", "alpha");
        fx.backup(&fx.config, "s1");
        let head = fx.head();
        assert!(head.base.is_none());
        assert_eq!(head.segments.len(), 1);
        assert_eq!(fx.index_objects(), 1);

        fx.write("b", "bravo");
        fx.backup(&fx.config, "s2");
        assert_eq!(fx.head().segments.len(), 2);
        assert_eq!(fx.index_objects(), 2);

        // Unchanged files: refcount bumps only.
        fx.backup(&fx.config, "s3");
        let head = fx.head();
        assert_eq!(head.segments.len(), 3);
        assert_eq!(fx.index_objects(), 3);
        assert!(head.segments[2].size < head.segments[1].size);
        let info = commands::info::run(&fx.config, None)
            .unwrap()
            .index
            .unwrap();
        assert_eq!(info.segments, 3);
        assert_eq!(info.base_bytes, 0);
        assert_eq!(
            info.segment_bytes,
            head.segments.iter().map(|s| s.size).sum::<u64>()
        );

        let warm = sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index());
        assert_eq!(warm, cold_index(&fx, "cold"));
        fx.check_clean(&fx.config);
        let dest = fx.restore(&fx.config, "s3");
        assert_eq!(std::fs::read_to_string(find(&dest, "b")).unwrap(), "bravo");
    }
}

/// Delete writes a segment with negative deltas; compact past the soft
/// limit folds into a new base and retires the old objects; a later
/// compact removes debris but keeps the retired generation.
#[test]
fn delete_and_compact_fold_and_collect_garbage() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    fx.write("c", "charlie");
    fx.backup(&fx.config, "s3");
    let before = fx.head();
    let chunks_before = fx.open(&fx.cache(), true).unwrap().chunk_index().len();

    commands::delete::run(&fx.config, None, &["s1"], false, None).unwrap();
    let after_delete = fx.head();
    assert_eq!(after_delete.segments.len(), before.segments.len() + 1);
    assert!(fx.open(&fx.cache(), true).unwrap().chunk_index().len() < chunks_before);
    assert_eq!(
        cold_index(&fx, "cold1"),
        sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index())
    );
    fx.check_clean(&fx.config);

    compact(&fx, 4);
    let folded = fx.head();
    assert!(folded.segments.is_empty(), "past the soft limit");
    assert_ne!(folded.base, after_delete.base);
    // Everything live before the compact is retired (with the compact's
    // own repack segment, if it wrote one).
    assert!(
        after_delete
            .live_objects()
            .all(|n| folded.retired.contains(&n)),
        "{folded:?} vs {after_delete:?}"
    );
    assert_eq!(fx.index_objects(), 1 + folded.retired.len());
    assert_eq!(
        cold_index(&fx, "cold2"),
        sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index())
    );
    fx.check_clean(&fx.config);

    // Debris of an interrupted write is removed; the retired generation
    // stays for readers still on the previous head.
    std::fs::write(
        fx.repo_dir.join(INDEX_OBJECT_DIR).join("ab".repeat(32)),
        b"debris",
    )
    .unwrap();
    compact(&fx, 4);
    assert_eq!(fx.head(), folded);
    assert_eq!(fx.index_objects(), 1 + folded.retired.len());
    fx.restore(&fx.config, "s3");
}

/// Compact with the soft limit at `segments` segments.
fn compact(fx: &Fixture, segments: usize) {
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    let mut policy = crate::repo::index_write::FoldPolicy::default();
    policy.soft.segments = segments;
    repo.set_fold_policy(policy);
    commands::compact::run_on_repo(&mut repo, 0.0, None, false, None).unwrap();
}

/// Every writer folds at the hard limit, so a repository that is never
/// compacted stays bounded.
#[test]
fn maintenance_writes_fold_at_the_hard_limit() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s1");
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    let mut policy = crate::repo::index_write::FoldPolicy::default();
    policy.hard.segments = 3;
    policy.hard.percent = 100_000;
    repo.set_fold_policy(policy);

    // The backup left one segment over the empty base.
    let mut counts = Vec::new();
    let mut last = None;
    for round in 0..5u32 {
        repo.load_chunk_index_under_lock().unwrap();
        let id = *repo.chunk_index().iter().next().unwrap().0;
        repo.chunk_index_mut().increment_refcount_by(&id, round + 1);
        let live: Vec<_> = repo.index_head().unwrap().live_objects().collect();
        repo.save_state().unwrap();
        let head = repo.index_head().unwrap().clone();
        assert_eq!(head, fx.head());
        if head.segments.is_empty() {
            assert_eq!(head.retired, live, "a fold retires the previous live set");
        }
        counts.push(head.segments.len());
        last = Some(head);
    }
    assert_eq!(counts, vec![2, 0, 1, 2, 0]);
    // The new base and the retired generation; the one before is gone.
    assert_eq!(fx.index_objects(), 1 + last.unwrap().retired.len());
    assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
}

/// On a plaintext repository a change repeated from a state the chain
/// returned to packs to an object name the head already holds; the write
/// folds instead of naming it twice.
#[test]
fn repeated_plaintext_segment_folds() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    repo.load_chunk_index_under_lock().unwrap();
    let id = *repo.chunk_index().iter().next().unwrap().0;
    for change in [1i64, -1, 1] {
        if change > 0 {
            repo.chunk_index_mut().increment_refcount(&id);
        } else {
            repo.chunk_index_mut().decrement(&id);
        }
        repo.save_state().unwrap();
    }
    let head = fx.head();
    assert!(head.segments.is_empty(), "{head:?}");
    assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
}

/// A lost lock between the segment and the head PUT publishes nothing: the
/// head is unchanged and the segment is unreferenced debris.
#[test]
fn fence_failure_publishes_nothing() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    let head_bytes = std::fs::read(fx.repo_dir.join("index")).unwrap();
    let objects = fx.index_objects();

    let mut repo = fx.open(&fx.cache(), false).unwrap();
    repo.load_chunk_index_under_lock().unwrap();
    let before = sorted_entries(repo.chunk_index());
    let id = *repo.chunk_index().iter().next().unwrap().0;
    repo.chunk_index_mut().increment_refcount(&id);
    repo.set_lock_fence(Arc::new(|| Err(VykarError::Other("lock lost".into()))));
    let err = repo.save_state().unwrap_err().to_string();
    assert!(err.contains("lock lost"), "{err}");

    assert_eq!(
        std::fs::read(fx.repo_dir.join("index")).unwrap(),
        head_bytes
    );
    assert_eq!(fx.index_objects(), objects + 1);
    assert_eq!(
        sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index()),
        before
    );
    assert_eq!(cold_index(&fx, "cold"), before);
}

/// Two clients with their own caches alternate commits; each catches up on
/// the other's segments.
#[test]
fn two_clients_alternate_commits() {
    let fx = fixture(RepoFormat::V4);
    let other = fx.other_client("cache-b");
    fx.write("a", "alpha");
    fx.backup(&fx.config, "a1");
    fx.write("b", "bravo");
    fx.backup(&other, "b1");
    fx.write("c", "charlie");
    fx.backup(&fx.config, "a2");
    fx.backup(&other, "b2");
    assert_eq!(fx.head().segments.len(), 4);

    let a = sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index());
    let b_cache = PathBuf::from(other.cache_dir.as_ref().unwrap());
    let b = sorted_entries(fx.open(&b_cache, true).unwrap().chunk_index());
    assert_eq!(a, b);
    assert_eq!(a, cold_index(&fx, "cold"));
    fx.check_clean(&fx.config);
    fx.check_clean(&other);
}

/// The monolithic blob an older binary would write.
fn legacy_blob(index: &ChunkIndex) -> Vec<u8> {
    let blob = IndexBlob {
        generation: 7,
        chunks: index.clone(),
    };
    let crypto = vykar_crypto::PlaintextEngine::new(crate::testutil::test_chunk_hasher());
    pack_object_streaming_with_context(ObjectType::ChunkIndex, b"index", 64, &crypto, |buf| {
        crate::compress::compress_stream_zstd(buf, 3, |encoder| {
            rmp_serde::encode::write(encoder, &blob)?;
            Ok(())
        })
    })
    .unwrap()
}

/// An older binary overwrote the head with a monolithic blob. Every command
/// refuses and names `check --repair`, which rebuilds the segmented index
/// from the blob with one base write and removes the stale objects.
#[test]
fn repair_rebuilds_an_overwritten_head() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    let index = fx.open(&fx.cache(), true).unwrap().chunk_index().clone();
    std::fs::write(fx.repo_dir.join("index"), legacy_blob(&index)).unwrap();

    let err = fx.open(&fx.cache(), false).err().unwrap().to_string();
    assert!(err.contains("check --repair"), "{err}");

    let result =
        commands::check::run_with_repair(&fx.config, None, false, RepairMode::Apply, None, None)
            .unwrap();
    assert!(!result.plan.has_data_loss, "{:?}", result.plan.actions);
    let head = fx.head();
    assert!(head.base.is_some());
    assert!(head.segments.is_empty());
    assert!(head.retired.is_empty());
    assert_eq!(fx.index_objects(), 1);
    assert_eq!(sorted_entries(&index), cold_index(&fx, "cold"));
    fx.check_clean(&fx.config);

    fx.write("c", "charlie");
    fx.backup(&fx.config, "s3");
    assert_eq!(fx.head().segments.len(), 1);
}

/// A segment object went missing. Loading the index fails and names
/// `check --repair`; repair rebuilds from the readable part of the chain.
#[test]
fn repair_salvages_a_broken_chain() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    fx.write("c", "charlie");
    fx.backup(&fx.config, "s3");
    let head = fx.head();
    let lost = head.segments.last().unwrap().name;
    std::fs::remove_file(fx.repo_dir.join(INDEX_OBJECT_DIR).join(lost.to_hex())).unwrap();

    let other = fx.other_client("cache-b");
    let b_cache = PathBuf::from(other.cache_dir.as_ref().unwrap());
    let err = fx.open(&b_cache, true).err().unwrap().to_string();
    assert!(err.contains("check --repair"), "{err}");

    commands::check::run_with_repair(&other, None, false, RepairMode::Apply, None, None).unwrap();
    let repaired = fx.head();
    assert!(repaired.segments.is_empty());
    assert_eq!(fx.index_objects(), 1);
    fx.check_clean(&other);
    fx.restore(&other, "s1");
}

/// Logs `put` and `delete` keys in call order.
struct MutLog {
    inner: Arc<dyn StorageBackend>,
    ops: Arc<Mutex<Vec<String>>>,
}

impl InnerBackend for MutLog {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for MutLog;
    except [put, delete, put_pack, begin_staged_put, batch_delete_keys, profile];

    fn profile(&self) -> vykar_storage::BackendProfile {
        self.inner.profile()
    }

    // Packs and staged writes are not logged.
    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        self.inner.put_pack(key, data)
    }

    fn begin_staged_put(&self, prefix: &str) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        self.inner.begin_staged_put(prefix)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.ops.lock().unwrap().push(format!("put {key}"));
        self.inner.put(key, data)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.ops.lock().unwrap().push(format!("delete {key}"));
        self.inner.delete(key)
    }

    fn batch_delete_keys(&self, keys: &[String], scope: vykar_storage::BatchDeleteScope) -> Result<()> {
        self.ops.lock().unwrap().extend(keys.iter().map(|k| format!("delete {k}")));
        self.inner.batch_delete_keys(keys, scope)
    }
}

/// Client-side compact persists the index once for the whole batch, before
/// deleting any pack, including empty ones whose entries were removed from
/// the in-memory index without a save. Both layouts.
#[test]
fn compact_deletes_packs_only_after_persisting_the_index() {
    for format in [RepoFormat::V3, RepoFormat::V5] {
        let fx = fixture(format);
        fx.write("a", "alpha");
        fx.write("b", "bravo");
        fx.backup(&fx.config, "s1");
        std::fs::remove_file(fx.source.join("b")).unwrap();
        fx.write("c", "charlie");
        fx.backup(&fx.config, "s2");
        fx.write("d", "delta");
        fx.backup(&fx.config, "s3");
        commands::delete::run(&fx.config, None, &["s1"], false, None).unwrap();

        let ops = Arc::new(Mutex::new(Vec::new()));
        let storage = Box::new(MutLog {
            inner: Arc::from(fx.storage()),
            ops: Arc::clone(&ops),
        });
        let mut repo = fx
            .open_with(storage, &fx.cache(), OpenOptions::new())
            .unwrap();
        commands::util::with_maintenance_lock(&mut repo, |repo| {
            repo.load_chunk_index_under_lock()?;
            // A pack whose entries leave only the in-memory index.
            let (_, entry) = repo
                .chunk_index()
                .iter()
                .max_by_key(|(_, e)| e.pack_offset)
                .map(|(id, e)| (*id, *e))
                .unwrap();
            repo.chunk_index_mut().remove_by_pack(&entry.pack_id);
            ops.lock().unwrap().clear();
            commands::compact::compact_repo(repo, 0.0, None, false, 2, None)
        })
        .unwrap();

        let ops = ops.lock().unwrap().clone();
        let first_pack_delete = ops
            .iter()
            .position(|o| o.starts_with("delete packs/"))
            .unwrap_or_else(|| panic!("{format:?}: no pack deleted: {ops:?}"));
        let head_puts: Vec<usize> = ops
            .iter()
            .enumerate()
            .filter(|(_, o)| *o == "put index")
            .map(|(i, _)| i)
            .collect();
        assert!(
            head_puts.first().is_some_and(|&i| i < first_pack_delete),
            "{format:?}: {ops:?}"
        );
        assert_eq!(
            head_puts.iter().filter(|&&i| i < first_pack_delete).count(),
            1,
            "{format:?}: one index write for the batch: {ops:?}"
        );
    }
}
