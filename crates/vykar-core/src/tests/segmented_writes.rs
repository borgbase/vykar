//! Writing repositories in the segmented index formats (v4/v5): backup
//! commits, maintenance writes, folds, garbage collection and repair.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::helpers::{
    FailGet, FaultyPut, backup_single_source, is_base_put, is_head_put, make_test_config,
};
use crate::commands;
use crate::commands::check::{IndexRecovery, RepairAction, RepairMode};
use crate::config::VykarConfig;
use crate::index::ChunkIndex;
use crate::index::head::{IndexHead, ObjectName};
use crate::index::table::{self, ChunkTable};
use crate::repo::control::CancelToken;
use crate::repo::format::{ObjectType, pack_object_streaming_with_context};
use crate::repo::index_store::{
    INDEX_OBJECT_DIR, SEGMENT_CONTEXT, decode_segment, object_key, pack_head,
};
use crate::repo::{EncryptionMode, OpenOptions, RepoFormat, Repository, WriteSessionOptions};
use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
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
        config.chunker.params(),
        None,
        Some(&config.repository.init_options()),
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
            &CancelToken::new(),
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

fn sorted_entries(index: &impl crate::testutil::EntryIter) -> Vec<String> {
    let mut v: Vec<String> = index
        .entry_vec()
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

/// A backup commit leaves the chunk table at the new state, and it agrees
/// with the index a cold client builds.
#[test]
fn backup_leaves_the_table_at_the_new_state() {
    for format in [RepoFormat::V4, RepoFormat::V5] {
        let fx = fixture(format);
        fx.write("a", "alpha");
        fx.backup(&fx.config, "s1");
        fx.write("b", "bravo");
        fx.backup(&fx.config, "s2");

        let repo = fx.open(&fx.cache(), false).unwrap();
        let head = fx.head();
        let dir = crate::cache_paths::repo_cache_dir(&repo.config.id, Some(&fx.cache())).unwrap();
        let table = ChunkTable::open_file(&table::table_path(&dir, &head.state_hash))
            .expect("the table of the new state");
        assert_eq!(table.state(), (head.state_hash, head.entries));
        assert_eq!(sorted_entries(&table), cold_index(&fx, "cold"));
    }
}

/// `save_state` of a session with no changes writes nothing and reads no
/// index object: the loaded table stays the index, and its file as it was.
#[test]
fn an_empty_session_leaves_the_table_as_is() {
    for format in [RepoFormat::V4, RepoFormat::V5] {
        let fx = fixture(format);
        fx.write("a", "alpha");
        fx.backup(&fx.config, "s1");
        let cd = fx.cache();

        // Index reads fail once the repository is open.
        let fail = Arc::new(AtomicBool::new(false));
        let armed = Arc::clone(&fail);
        let storage = FailGet::new(
            Arc::from(fx.storage()),
            move |k| {
                armed.load(Ordering::SeqCst) && (k == "index" || k.starts_with(INDEX_OBJECT_DIR))
            },
            usize::MAX,
        );
        let mut repo = fx
            .open_with(Box::new(storage), &cd, OpenOptions::new().with_index())
            .unwrap();
        let generation = repo.index_generation();
        let file = repo
            .chunk_index()
            .table()
            .unwrap()
            .file_path()
            .unwrap()
            .to_path_buf();
        let before = std::fs::read(&file).unwrap();
        fail.store(true, Ordering::SeqCst);

        repo.begin_write_session(&WriteSessionOptions::default())
            .unwrap();
        repo.save_state().unwrap();

        assert_eq!(repo.index_generation(), generation);
        assert_eq!(std::fs::read(&file).unwrap(), before);
        assert_eq!(sorted_entries(repo.chunk_index()), cold_index(&fx, "cold"));
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

    commands::delete::run(&fx.config, None, &["s1"], false, &CancelToken::new()).unwrap();
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
    commands::compact::run_on_repo(&mut repo, 0.0, None, false, &CancelToken::new()).unwrap();
}

/// A reader holding a head from before two production folds: after the
/// first its objects are retired and it completes on that head; after the
/// second they are deleted, and it retries on the re-read head.
#[test]
fn reader_on_a_pre_fold_head_recovers_through_production_folds() {
    use crate::repo::index_store::{self, CacheLocation};

    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    let h0 = fx.head();
    assert!(h0.base.is_none());
    assert!(!h0.segments.is_empty());
    compact(&fx, 1);
    assert!(fx.head().segments.is_empty(), "folded");

    let repo = fx.open(&fx.cache(), false).unwrap();
    let storage = fx.storage();
    let sync = |head: IndexHead| {
        index_store::sync_full_cache(
            &*storage,
            repo.crypto.as_ref(),
            head,
            CacheLocation {
                dir: None,
                live: None,
                use_local: false,
            },
            2,
        )
        .unwrap()
    };
    assert_eq!(sync(h0.clone()).head, h0, "retired, not yet deleted");

    fx.write("c", "charlie");
    fx.backup(&fx.config, "s3");
    compact(&fx, 1);
    for name in h0.live_objects() {
        assert!(
            !fx.repo_dir
                .join(INDEX_OBJECT_DIR)
                .join(name.to_hex())
                .exists(),
            "{}",
            name.to_hex()
        );
    }
    let synced = sync(h0);
    assert_eq!(synced.head, fx.head(), "retried on the re-read head");
    assert_eq!(sorted_entries(&*synced.table), cold_index(&fx, "cold"));
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
        let id = repo.chunk_index().iter().next().unwrap().0;
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
    let id = repo.chunk_index().iter().next().unwrap().0;
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
    let id = repo.chunk_index().iter().next().unwrap().0;
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
    let crypto = vykar_crypto::PlaintextEngine::new(crate::testutil::test_chunk_hasher());
    crate::tests::helpers::legacy_index_blob(index, 7, &crypto)
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
    let index = fx
        .open(&fx.cache(), true)
        .unwrap()
        .chunk_index()
        .to_chunk_index();
    std::fs::write(fx.repo_dir.join("index"), legacy_blob(&index)).unwrap();

    let err = fx.open(&fx.cache(), false).err().unwrap().to_string();
    assert!(err.contains("check --repair"), "{err}");

    let result = commands::check::run_with_repair(
        &fx.config,
        None,
        false,
        RepairMode::Apply,
        None,
        &CancelToken::new(),
    )
    .unwrap();
    assert!(!result.plan.has_data_loss, "{:?}", result.plan.actions);
    assert_eq!(result.plan.index_recovery, IndexRecovery::LegacyBlob);
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

/// Three backups; the files hold `alpha`, `bravo`, `charlie`.
fn three_backups() -> Fixture {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    fx.write("c", "charlie");
    fx.backup(&fx.config, "s3");
    fx
}

/// What a refused repair must leave alone: the head's bytes and the
/// numbers of `index.d/` objects and snapshots.
fn repo_state(fx: &Fixture) -> (Vec<u8>, usize, usize) {
    let snapshots = std::fs::read_dir(fx.repo_dir.join("snapshots"))
        .unwrap()
        .count();
    (
        std::fs::read(fx.repo_dir.join("index")).unwrap(),
        fx.index_objects(),
        snapshots,
    )
}

fn read_restored(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(find(dir, name)).unwrap()
}

/// A segment object went missing. Loading the index fails and names
/// `check --repair`. Repair rebuilds the chain up to the lost segment, so
/// `s3`, which needs it, is removed; the plan says so, and an apply that
/// was shown a plan without data loss, or with another index reading,
/// refuses and changes nothing.
#[test]
fn repair_salvages_a_broken_chain() {
    let fx = three_backups();
    let head = fx.head();
    let lost = head.segments.last().unwrap().name;
    std::fs::remove_file(fx.repo_dir.join(INDEX_OBJECT_DIR).join(lost.to_hex())).unwrap();

    let other = fx.other_client("cache-b");
    let b_cache = PathBuf::from(other.cache_dir.as_ref().unwrap());
    let err = fx.open(&b_cache, true).err().unwrap().to_string();
    assert!(err.contains("check --repair"), "{err}");
    assert!(err.contains("is missing from storage"), "{err}");
    assert!(err.contains(&lost.to_hex()), "{err}");
    assert!(!err.contains("retry later"), "{err}");

    let repair = |mode| {
        commands::check::run_with_repair(&other, None, false, mode, None, &CancelToken::new())
    };
    let shown = repair(RepairMode::PlanOnly).unwrap().plan;
    assert_eq!(
        shown.index_recovery,
        IndexRecovery::PartialChain {
            base_lost: false,
            applied: 2,
            total: 3
        }
    );
    assert!(shown.has_data_loss, "{:?}", shown.actions);
    let removes_s3 = |a: &RepairAction| matches!(a, RepairAction::RemoveDanglingSnapshot { snapshot_name, .. } if snapshot_name == "s3");
    assert!(shown.actions.iter().any(removes_s3), "{:?}", shown.actions);
    let names = format!("{:?}", shown.actions);
    assert!(
        !names.contains("\"s1\"") && !names.contains("\"s2\""),
        "{names}"
    );

    let before = repo_state(&fx);
    for (has_data_loss, index_recovery, reason) in [
        (false, IndexRecovery::Complete, "aborting repair"),
        (false, shown.index_recovery, "removes data"),
        (true, IndexRecovery::Complete, "read differently"),
    ] {
        let err = repair(RepairMode::ApplyShown {
            has_data_loss,
            index_recovery,
        })
        .err()
        .unwrap()
        .to_string();
        assert!(err.contains("aborting repair"), "{err}");
        assert!(err.contains(reason), "{err}");
        assert_eq!(repo_state(&fx), before);
    }

    let result = repair(RepairMode::ApplyShown {
        has_data_loss: shown.has_data_loss,
        index_recovery: shown.index_recovery,
    })
    .unwrap();
    assert!(
        result.applied.iter().any(removes_s3),
        "{:?}",
        result.applied
    );
    let repaired = fx.head();
    assert!(repaired.segments.is_empty());
    assert_eq!(fx.index_objects(), 1);
    fx.check_clean(&other);

    assert_eq!(read_restored(&fx.restore(&other, "s1"), "a"), "alpha");
    assert_eq!(read_restored(&fx.restore(&other, "s2"), "b"), "bravo");
    let dest = fx.tmp.path().join("restore-s3");
    let s3 = commands::restore::run(
        &other,
        None,
        "s3",
        dest.to_str().unwrap(),
        None,
        false,
        true,
        &CancelToken::new(),
    );
    assert!(s3.is_err());

    let clean = repair(RepairMode::ApplyShown {
        has_data_loss: false,
        index_recovery: IndexRecovery::Complete,
    })
    .unwrap();
    assert!(!clean.plan.has_data_loss, "{:?}", clean.plan.actions);
    assert_eq!(clean.plan.index_recovery, IndexRecovery::Complete);
}

/// A storage error while reading the chain is not damage: a client without
/// a cache at the head refuses to repair, whether a retry would succeed or
/// not, and nothing is changed.
#[test]
fn repair_load_refuses_on_a_storage_error() {
    let fx = three_backups();
    let key = format!("{INDEX_OBJECT_DIR}{}", fx.head().segments[2].name.to_hex());
    let before = repo_state(&fx);
    for (i, times) in [1, usize::MAX].into_iter().enumerate() {
        let failing = key.clone();
        let storage = FailGet::new(Arc::from(fx.storage()), move |k| k == failing, times);
        let cache = fx.tmp.path().join(format!("no-cache-{i}"));
        let mut repo = fx
            .open_with(
                Box::new(storage),
                &cache,
                OpenOptions::new().tolerate_broken_index(),
            )
            .unwrap();
        let err = repo
            .load_chunk_index_for_repair()
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("injected GET failure"), "{err}");
        assert!(err.contains("changed nothing"), "{err}");
    }
    assert_eq!(repo_state(&fx), before);
    let warm = sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index());
    assert_eq!(cold_index(&fx, "cold"), warm);
    assert_eq!(read_restored(&fx.restore(&fx.config, "s3"), "c"), "charlie");
}

/// A segment written by a newer vykar is not damage: a client without a
/// cache at the head refuses to repair, says why, and changes nothing.
#[test]
fn repair_load_refuses_a_segment_from_a_newer_vykar() {
    let fx = three_backups();
    let crypto = Arc::clone(&fx.open(&fx.cache(), false).unwrap().crypto);
    let storage = fx.storage();
    let mut head = fx.head();
    let seg = head.segments.last_mut().unwrap();
    let bytes = storage.get(&object_key(&seg.name)).unwrap().unwrap();
    let mut body = decode_segment(seg, &bytes, crypto.as_ref())
        .unwrap()
        .encode();
    body[0] = 99;
    let packed = pack_object_streaming_with_context(
        ObjectType::IndexSegment,
        SEGMENT_CONTEXT,
        body.len() + 64,
        crypto.as_ref(),
        |buf| {
            crate::compress::compress_stream_zstd(buf, 3, |w| {
                std::io::Write::write_all(w, &body)?;
                Ok(())
            })
        },
    )
    .unwrap();
    seg.name = ObjectName::of(&packed);
    seg.size = packed.len() as u64;
    storage.put(&object_key(&seg.name), &packed).unwrap();
    storage
        .put("index", &pack_head(&head, crypto.as_ref()).unwrap())
        .unwrap();

    let before = repo_state(&fx);
    let mut repo = fx
        .open_with(
            fx.storage(),
            &fx.tmp.path().join("cold"),
            OpenOptions::new().tolerate_broken_index(),
        )
        .unwrap();
    let err = repo
        .load_chunk_index_for_repair()
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("written by a newer vykar"), "{err}");
    assert!(err.contains("Nothing was changed"), "{err}");
    assert_eq!(repo_state(&fx), before);
}

/// A client whose cache holds the head's state repairs from it when the
/// chain cannot be read for any reason.
#[test]
fn repair_load_uses_the_local_cache_on_a_storage_error() {
    let fx = three_backups();
    let key = format!("{INDEX_OBJECT_DIR}{}", fx.head().segments[2].name.to_hex());
    let warm = sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index());
    let storage = FailGet::new(Arc::from(fx.storage()), move |k| k == key, usize::MAX);
    let mut repo = fx
        .open_with(
            Box::new(storage),
            &fx.cache(),
            OpenOptions::new().tolerate_broken_index(),
        )
        .unwrap();
    assert_eq!(
        repo.load_chunk_index_for_repair().unwrap(),
        IndexRecovery::Complete
    );
    assert_eq!(sorted_entries(repo.chunk_index()), warm);
}

/// Without a head, the plan says the index is rebuilt from nothing.
#[test]
fn repair_reports_a_missing_head() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    std::fs::remove_file(fx.repo_dir.join("index")).unwrap();
    let plan = commands::check::run_with_repair(
        &fx.config,
        None,
        false,
        RepairMode::PlanOnly,
        None,
        &CancelToken::new(),
    )
    .unwrap()
    .plan;
    assert_eq!(plan.index_recovery, IndexRecovery::HeadMissing);
    assert!(plan.has_data_loss, "{:?}", plan.actions);
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
    except [put, delete, put_pack, begin_staged_put, conditional, batch_delete, profile];

    // Test wrapper: keeps every write on `put`.
    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

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

    // Every delete goes through the logged `delete` (the inner is local).
    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        None
    }
}

/// Client-side compact persists the index once for the whole batch, before
/// deleting any pack, including empty ones whose entries were removed from
/// the in-memory index without a save.
#[test]
fn compact_deletes_packs_only_after_persisting_the_index() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s1");
    std::fs::remove_file(fx.source.join("b")).unwrap();
    fx.write("c", "charlie");
    fx.backup(&fx.config, "s2");
    fx.write("d", "delta");
    fx.backup(&fx.config, "s3");
    commands::delete::run(&fx.config, None, &["s1"], false, &CancelToken::new()).unwrap();

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
            .unwrap();
        repo.chunk_index_mut().remove_by_pack(&entry.pack_id);
        ops.lock().unwrap().clear();
        commands::compact::compact_repo(repo, 0.0, None, false, 2, &CancelToken::new())
    })
    .unwrap();

    let ops = ops.lock().unwrap().clone();
    let first_pack_delete = ops
        .iter()
        .position(|o| o.starts_with("delete packs/"))
        .unwrap_or_else(|| panic!("no pack deleted: {ops:?}"));
    let head_puts: Vec<usize> = ops
        .iter()
        .enumerate()
        .filter(|(_, o)| *o == "put index")
        .map(|(i, _)| i)
        .collect();
    assert!(
        head_puts.first().is_some_and(|&i| i < first_pack_delete),
        "{ops:?}"
    );
    assert_eq!(
        head_puts.iter().filter(|&&i| i < first_pack_delete).count(),
        1,
        "one index write for the batch: {ops:?}"
    );
}

#[derive(Debug, Clone, Copy)]
enum Damage {
    /// An older binary overwrote the head with a monolithic blob.
    LegacyBlob,
    /// The last segment object is gone.
    LostSegment,
}

/// Three backups, then `damage`. Returns the keys under `index.d/` and the
/// bytes at `index` after the damage.
fn damaged(damage: Damage) -> (Fixture, std::collections::BTreeSet<String>, Vec<u8>) {
    let fx = three_backups();
    match damage {
        Damage::LegacyBlob => {
            let index = fx
                .open(&fx.cache(), true)
                .unwrap()
                .chunk_index()
                .to_chunk_index();
            std::fs::write(fx.repo_dir.join("index"), legacy_blob(&index)).unwrap();
        }
        Damage::LostSegment => {
            let lost = fx.head().segments.last().unwrap().name;
            std::fs::remove_file(fx.repo_dir.join(INDEX_OBJECT_DIR).join(lost.to_hex())).unwrap();
        }
    }
    let before = fx
        .storage()
        .list(INDEX_OBJECT_DIR)
        .unwrap()
        .into_iter()
        .collect();
    let index = std::fs::read(fx.repo_dir.join("index")).unwrap();
    (fx, before, index)
}

/// Opens the damaged repository through `storage` with a fresh cache
/// directory and loads the index as repair does.
fn open_for_repair(fx: &Fixture, storage: Box<dyn StorageBackend>, cache: &str) -> Repository {
    let mut repo = fx
        .open_with(
            storage,
            &fx.tmp.path().join(cache),
            OpenOptions::new().tolerate_broken_index(),
        )
        .unwrap();
    repo.load_chunk_index_for_repair().unwrap();
    repo
}

fn faulty(
    fx: &Fixture,
    fail: impl Fn(&str, &[u8]) -> bool + Send + Sync + 'static,
) -> (Box<dyn StorageBackend>, Arc<Mutex<Vec<String>>>) {
    let ops = Arc::new(Mutex::new(Vec::new()));
    let storage = FaultyPut {
        inner: Arc::from(fx.storage()),
        fail: Box::new(fail),
        ops: Arc::clone(&ops),
    };
    (Box::new(storage), ops)
}

/// A fold without a previous head (repair) that fails at the base PUT, the
/// head PUT or the fence between them leaves `index` and every object in
/// `index.d/` as they were, so the next repair reads the same index.
#[test]
fn fold_without_a_head_keeps_old_objects_until_the_head_is_written() {
    #[derive(Debug, Clone, Copy)]
    enum Failure {
        BasePut,
        HeadPut,
        SecondFence,
    }
    for damage in [Damage::LegacyBlob, Damage::LostSegment] {
        for failure in [Failure::BasePut, Failure::HeadPut, Failure::SecondFence] {
            let case = format!("{damage:?}-{failure:?}");
            let (fx, before, index_before) = damaged(damage);
            let (storage, _ops) = match failure {
                Failure::BasePut => faulty(&fx, is_base_put),
                Failure::HeadPut => faulty(&fx, is_head_put),
                Failure::SecondFence => faulty(&fx, |_, _| false),
            };
            let mut repo = open_for_repair(&fx, storage, &format!("{case}-cold"));
            let loaded = sorted_entries(repo.chunk_index());
            assert!(!loaded.is_empty(), "{case}");
            if let Failure::SecondFence = failure {
                let probe: Arc<dyn StorageBackend> = Arc::from(fx.storage());
                let known = before.clone();
                repo.set_lock_fence(Arc::new(move || {
                    if probe
                        .list(INDEX_OBJECT_DIR)?
                        .iter()
                        .any(|k| !known.contains(k))
                    {
                        Err(VykarError::LockExpired("injected".into()))
                    } else {
                        Ok(())
                    }
                }));
            }
            repo.request_index_fold();
            assert!(repo.save_state().is_err(), "{case}");
            drop(repo);

            assert_eq!(
                std::fs::read(fx.repo_dir.join("index")).unwrap(),
                index_before,
                "{case}"
            );
            let after: std::collections::BTreeSet<String> = fx
                .storage()
                .list(INDEX_OBJECT_DIR)
                .unwrap()
                .into_iter()
                .collect();
            assert!(before.is_subset(&after), "{case}: {before:?} vs {after:?}");
            assert!(after.difference(&before).count() <= 1, "{case}: {after:?}");

            let mut repo = open_for_repair(&fx, fx.storage(), &format!("{case}-retry"));
            assert_eq!(sorted_entries(repo.chunk_index()), loaded, "{case}");
            repo.request_index_fold();
            repo.save_state().unwrap();
            drop(repo);
            let head = fx.head();
            assert!(head.base.is_some(), "{case}");
            assert!(
                head.segments.is_empty() && head.retired.is_empty(),
                "{case}"
            );
            assert_eq!(fx.index_objects(), 1, "{case}");
            assert_eq!(cold_index(&fx, &format!("{case}-after")), loaded, "{case}");
        }
    }
}

/// A fold without a previous head PUTs the base, then the head, and only
/// then deletes the objects the new head does not name.
#[test]
fn fold_without_a_head_deletes_old_objects_after_the_head_put() {
    for damage in [Damage::LegacyBlob, Damage::LostSegment] {
        let (fx, _, _) = damaged(damage);
        let (storage, ops) = faulty(&fx, |_, _| false);
        let mut repo = open_for_repair(&fx, storage, "cold");
        ops.lock().unwrap().clear();
        repo.request_index_fold();
        repo.save_state().unwrap();
        drop(repo);

        let new = fx.head();
        let base_key = format!("put {INDEX_OBJECT_DIR}{}", new.base.unwrap().name.to_hex());
        let ops = ops.lock().unwrap().clone();
        let base_put = ops.iter().position(|o| *o == base_key).unwrap();
        let first_head_put = ops.iter().position(|o| o == "put index").unwrap();
        let last_head_put = ops.iter().rposition(|o| o == "put index").unwrap();
        assert!(base_put < first_head_put, "{damage:?}: {ops:?}");
        let deletes: Vec<usize> = ops
            .iter()
            .enumerate()
            .filter(|(_, o)| o.starts_with(&format!("delete {INDEX_OBJECT_DIR}")))
            .map(|(i, _)| i)
            .collect();
        assert!(!deletes.is_empty(), "{damage:?}: {ops:?}");
        assert!(
            deletes.iter().all(|&i| i > last_head_put),
            "{damage:?}: {ops:?}"
        );
        assert_eq!(fx.index_objects(), 1, "{damage:?}");
    }
}

/// A lock lost after the head PUT of a fold without a previous head is
/// returned, and nothing is deleted; the new head is in place and valid.
#[test]
fn lost_lock_after_the_head_put_stops_the_fold_without_a_head() {
    for damage in [Damage::LegacyBlob, Damage::LostSegment] {
        let (fx, before, index_before) = damaged(damage);
        let mut repo = open_for_repair(&fx, fx.storage(), "cold");
        let loaded = sorted_entries(repo.chunk_index());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        repo.set_lock_fence(Arc::new(move || {
            if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 2 {
                Err(VykarError::LockExpired("injected".into()))
            } else {
                Ok(())
            }
        }));
        repo.request_index_fold();
        let err = repo.save_state().unwrap_err();
        assert!(
            matches!(err, VykarError::LockExpired(_)),
            "{damage:?}: {err}"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        drop(repo);

        assert_ne!(
            std::fs::read(fx.repo_dir.join("index")).unwrap(),
            index_before
        );
        let head = fx.head();
        let base = head.base.as_ref().expect("the new head names a base");
        assert!(head.segments.is_empty() && head.retired.is_empty());
        let after: std::collections::BTreeSet<String> = fx
            .storage()
            .list(INDEX_OBJECT_DIR)
            .unwrap()
            .into_iter()
            .collect();
        assert!(before.is_subset(&after), "{damage:?}: {after:?}");
        assert!(after.contains(&format!("{INDEX_OBJECT_DIR}{}", base.name.to_hex())));
        assert_eq!(cold_index(&fx, "cold-after"), loaded, "{damage:?}");
    }
}

/// Fails listings of `index.d/` once `index` no longer holds `head_before`,
/// that is, once a new head has been PUT.
struct FailListAfterHeadPut {
    inner: Arc<dyn StorageBackend>,
    head_before: Vec<u8>,
}

impl FailListAfterHeadPut {
    fn check(&self, prefix: &str) -> Result<()> {
        if prefix.starts_with(INDEX_OBJECT_DIR)
            && self.inner.get("index")?.as_ref() != Some(&self.head_before)
        {
            return Err(VykarError::Other("injected list failure".into()));
        }
        Ok(())
    }
}

impl InnerBackend for FailListAfterHeadPut {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for FailListAfterHeadPut;
    except [list, list_sized, list_meta, server_ops];

    fn list_meta(
        &self,
        prefix: &str,
    ) -> vykar_types::error::Result<Vec<vykar_storage::ListedObject>> {
        self.list_sized(prefix)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.check(prefix)?;
        self.inner.list(prefix)
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.check(prefix)?;
        self.inner.list_sized(prefix)
    }

    // No server behind the inner backend: listings stay on `list_sized`.
    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        None
    }
}

/// After the head PUT of a fold without a previous head, a failed listing
/// is only logged: the save succeeds, the new head is in place and the old
/// objects stay as debris.
#[test]
fn failed_listing_after_the_head_put_of_a_fold_without_a_head_is_logged() {
    for damage in [Damage::LegacyBlob, Damage::LostSegment] {
        let (fx, before, index_before) = damaged(damage);
        let storage = FailListAfterHeadPut {
            inner: Arc::from(fx.storage()),
            head_before: index_before.clone(),
        };
        let mut repo = open_for_repair(&fx, Box::new(storage), "cold");
        let loaded = sorted_entries(repo.chunk_index());
        repo.request_index_fold();
        repo.save_state().unwrap();
        drop(repo);

        assert_ne!(
            std::fs::read(fx.repo_dir.join("index")).unwrap(),
            index_before,
            "{damage:?}"
        );
        let head = fx.head();
        assert!(head.base.is_some(), "{damage:?}");
        assert!(head.segments.is_empty() && head.retired.is_empty());
        let after: std::collections::BTreeSet<String> = fx
            .storage()
            .list(INDEX_OBJECT_DIR)
            .unwrap()
            .into_iter()
            .collect();
        assert!(before.is_subset(&after), "{damage:?}: {after:?}");
        assert_eq!(cold_index(&fx, "cold-after"), loaded, "{damage:?}");
    }
}

/// An index that became empty folds to a head without a base, as `init`
/// writes it; later writes append to it and fold into a base again.
#[test]
fn empty_index_folds_to_a_head_without_base() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    commands::delete::run(&fx.config, None, &["s1", "s2"], false, &CancelToken::new()).unwrap();
    assert!(fx.open(&fx.cache(), true).unwrap().chunk_index().is_empty());

    let live: Vec<_> = fx.head().live_objects().collect();
    compact(&fx, 1);
    let folded = fx.head();
    assert!(folded.base.is_none(), "{folded:?}");
    assert!(folded.segments.is_empty(), "{folded:?}");
    assert_eq!(folded.entries, 0);
    assert_eq!(folded.state_hash, crate::index::head::empty_state_hash());
    assert!(
        live.iter().all(|n| folded.retired.contains(n)),
        "{folded:?} vs {live:?}"
    );
    assert_eq!(fx.index_objects(), folded.retired.len());
    let info = commands::info::run(&fx.config, None).unwrap();
    assert_eq!(info.index.unwrap().base_bytes, 0);
    assert!(cold_index(&fx, "cold1").is_empty());
    fx.check_clean(&fx.config);

    compact(&fx, 1);
    assert_eq!(fx.head(), folded);
    assert_eq!(fx.index_objects(), folded.retired.len());

    fx.write("c", "charlie");
    fx.backup(&fx.config, "s3");
    let head = fx.head();
    assert!(head.base.is_none());
    assert_eq!(head.segments.len(), 1);
    let s3_segment = head.segments[0].name;
    let warm = sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index());
    assert_eq!(cold_index(&fx, "cold2"), warm);

    compact(&fx, 1);
    let head = fx.head();
    assert!(head.base.is_some());
    assert_eq!(head.retired, vec![s3_segment]);
    assert_eq!(fx.index_objects(), 2);
    assert_eq!(cold_index(&fx, "cold3"), warm);
    fx.check_clean(&fx.config);
    assert_eq!(read_restored(&fx.restore(&fx.config, "s3"), "c"), "charlie");
}

/// A fold with a previous head that fails at the base or head PUT leaves
/// the head and every object it names in place; the next fold removes what
/// the failed one left.
#[test]
fn put_base_failures_with_a_previous_head_leave_it_intact() {
    type Fail = fn(&str, &[u8]) -> bool;
    for (name, fail) in [("base", is_base_put as Fail), ("head", is_head_put as Fail)] {
        let fx = three_backups();
        let head_before = std::fs::read(fx.repo_dir.join("index")).unwrap();
        let live: Vec<_> = fx.head().live_objects().collect();
        let warm = sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index());

        let (storage, _ops) = faulty(&fx, fail);
        let mut repo = fx
            .open_with(storage, &fx.cache(), OpenOptions::new())
            .unwrap();
        let mut policy = crate::repo::index_write::FoldPolicy::default();
        policy.soft.segments = 1;
        repo.set_fold_policy(policy);
        let err = commands::compact::run_on_repo(&mut repo, 0.0, None, false, &CancelToken::new())
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("injected failure"), "{name}: {err}");
        drop(repo);

        assert_eq!(
            std::fs::read(fx.repo_dir.join("index")).unwrap(),
            head_before,
            "{name}"
        );
        for n in &live {
            assert!(
                fx.repo_dir.join(INDEX_OBJECT_DIR).join(n.to_hex()).exists(),
                "{name}"
            );
        }
        assert_eq!(cold_index(&fx, &format!("cold-{name}")), warm, "{name}");

        // A new state, so the next base is not named like the failed one's.
        fx.write("d", "delta");
        fx.backup(&fx.config, "s4");
        compact(&fx, 1);
        assert_eq!(fx.index_objects(), 1 + fx.head().retired.len(), "{name}");
    }
}

/// Repair of a repository with no head and no snapshots writes the empty
/// head, as `init` does, and removes the debris in `index.d/`.
#[test]
fn repair_of_an_empty_index_writes_the_empty_head() {
    let fx = fixture(RepoFormat::V5);
    std::fs::write(
        fx.repo_dir.join(INDEX_OBJECT_DIR).join("ab".repeat(32)),
        b"debris",
    )
    .unwrap();
    std::fs::remove_file(fx.repo_dir.join("index")).unwrap();
    commands::check::run_with_repair(
        &fx.config,
        None,
        false,
        RepairMode::Apply,
        None,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(fx.head(), IndexHead::empty());
    assert_eq!(fx.index_objects(), 0);
    fx.check_clean(&fx.config);
}

/// Two backups over the same cache, then a repository at the second head
/// loaded under the lock. Returns the fixture, the repo cache dir, the
/// repository and the head after the first backup.
fn loaded_after_two_backups() -> (Fixture, PathBuf, Repository, IndexHead) {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    let first = fx.head();
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    repo.load_chunk_index_under_lock().unwrap();
    let dir = crate::cache_paths::repo_cache_dir(&repo.config.id, Some(&fx.cache())).unwrap();
    (fx, dir, repo, first)
}

fn bump_one_refcount(repo: &mut Repository) {
    let id = repo.chunk_index().iter().next().unwrap().0;
    repo.chunk_index_mut().increment_refcount(&id);
}

/// A lockless reader on an older head published that state's table in the
/// same cache dir. It coexists with the writer's table and does not displace
/// it: the write appends a segment to the state it loaded.
#[test]
fn a_readers_earlier_state_does_not_displace_the_writers_table() {
    let (fx, dir, mut repo, first) = loaded_after_two_backups();
    let before = repo.index_head().cloned().unwrap();
    let objects = fx.index_objects();
    let older = table::table_path(&dir, &first.state_hash);
    assert!(
        ChunkTable::open_file(&older).is_some(),
        "the earlier state's table"
    );
    assert!(ChunkTable::open_file(&table::table_path(&dir, &before.state_hash)).is_some());

    bump_one_refcount(&mut repo);
    repo.save_state().unwrap();

    let head = repo.index_head().cloned().unwrap();
    let table = ChunkTable::open_file(&table::table_path(&dir, &head.state_hash)).unwrap();
    assert_eq!(table.state(), (head.state_hash, head.entries));
    drop(table);
    assert!(older.exists(), "a young table of another state is kept");
    assert_eq!(head, fx.head());
    assert_eq!(head.segments.len(), before.segments.len() + 1);
    assert_eq!(head.base, before.base);
    assert_eq!(head.retired, before.retired);
    assert_eq!(fx.index_objects(), objects + 1, "one segment, no base");
    assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
}

/// A write diffs against the table it loaded, mapped in memory: removing
/// the table files, or putting another state's content under the loaded
/// state's name, changes nothing about it.
#[test]
fn a_write_does_not_need_the_table_file() {
    for unrelated in [false, true] {
        let (fx, dir, mut repo, _) = loaded_after_two_backups();
        let before = repo.index_head().cloned().unwrap();
        let path = table::table_path(&dir, &before.state_hash);
        std::fs::remove_file(&path).unwrap();
        if unrelated {
            crate::index::dedup_cache::build_full_index_cache_to_path(
                &crate::repo::index_store::tests::index_with(500..510, 0),
                9,
                &path,
            )
            .unwrap();
        }

        bump_one_refcount(&mut repo);
        repo.save_state().unwrap();

        let head = fx.head();
        assert_eq!(head.base, before.base, "{unrelated}");
        assert_eq!(
            head.segments.len(),
            before.segments.len() + 1,
            "{unrelated}"
        );
        assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
    }
}

/// A write against a head another client has moved since the load is
/// refused and leaves that client's head alone (tests plan item 1.6,
/// second assertion).
#[test]
fn a_write_after_another_writer_moved_the_head_is_refused() {
    let (fx, _, mut repo, before) = loaded_after_two_backups();
    fx.write("c", "charlie");
    fx.backup(&fx.other_client("other"), "s3");
    let moved = fx.head();
    assert_ne!(moved, before);

    bump_one_refcount(&mut repo);
    let err = repo.save_state().unwrap_err().to_string();
    assert!(err.contains("index changed on storage"), "{err}");
    assert_eq!(fx.head(), moved);
}

/// Damage the head: keep its first half.
fn truncate_head(fx: &Fixture) {
    let path = fx.repo_dir.join("index");
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
}

fn plan_only(config: &VykarConfig) -> Result<commands::check::RepairResult> {
    commands::check::run_with_repair(
        config,
        None,
        false,
        RepairMode::PlanOnly,
        None,
        &CancelToken::new(),
    )
}

fn removes(snapshot: &str) -> impl Fn(&RepairAction) -> bool + '_ {
    move |a| matches!(a, RepairAction::RemoveDanglingSnapshot { snapshot_name, .. } if snapshot_name == snapshot)
}

/// An unreadable head is rebuilt from this machine's cache, or on another
/// machine from the chain in `index.d/`.
#[test]
fn repair_rebuilds_from_a_corrupt_head() {
    let fx = three_backups();
    let before = cold_index(&fx, "cold-before");
    truncate_head(&fx);
    assert!(fx.open(&fx.cache(), false).is_err());

    let warm = plan_only(&fx.config).unwrap().plan;
    assert_eq!(
        warm.index_recovery,
        IndexRecovery::HeadUnreadable {
            from_local_cache: true,
            bases: 0,
            applied: 0,
            unplaced: 0,
            damaged: 0
        }
    );

    let other = fx.other_client("cache-b");
    let shown = plan_only(&other).unwrap().plan;
    assert_eq!(
        shown.index_recovery,
        IndexRecovery::HeadUnreadable {
            from_local_cache: false,
            bases: 0,
            applied: 3,
            unplaced: 0,
            damaged: 0
        }
    );
    assert!(!shown.has_data_loss, "{:?}", shown.actions);
    commands::check::run_with_repair(
        &other,
        None,
        false,
        RepairMode::ApplyShown {
            has_data_loss: false,
            index_recovery: shown.index_recovery,
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();

    let head = fx.head();
    assert!(head.base.is_some());
    assert!(head.segments.is_empty() && head.retired.is_empty());
    assert_eq!(fx.index_objects(), 1);
    assert_eq!(cold_index(&fx, "cold-after"), before);
    fx.check_clean(&other);
    assert_eq!(read_restored(&fx.restore(&other, "s3"), "c"), "charlie");
    fx.write("d", "delta");
    fx.backup(&other, "s4");
    assert_eq!(fx.head().segments.len(), 1);
}

/// Fails every listing of `index.d/`.
struct FailIndexList {
    inner: Arc<dyn StorageBackend>,
}

impl FailIndexList {
    fn check(&self, prefix: &str) -> Result<()> {
        if prefix.starts_with(INDEX_OBJECT_DIR) {
            return Err(VykarError::Other("injected list failure".into()));
        }
        Ok(())
    }
}

impl InnerBackend for FailIndexList {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for FailIndexList;
    except [list, list_sized, list_meta, server_ops];

    fn list_meta(
        &self,
        prefix: &str,
    ) -> vykar_types::error::Result<Vec<vykar_storage::ListedObject>> {
        self.list_sized(prefix)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.check(prefix)?;
        self.inner.list(prefix)
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.check(prefix)?;
        self.inner.list_sized(prefix)
    }

    // No server behind the inner backend: listings stay on `list_sized`.
    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        None
    }
}

/// A failed `index.d/` listing on an unreadable head is named as such, without
/// the advice to remove an index object.
#[test]
fn repair_without_a_head_names_a_failed_listing() {
    let fx = three_backups();
    truncate_head(&fx);
    let before = repo_state(&fx);
    let storage = FailIndexList {
        inner: Arc::from(fx.storage()),
    };
    let mut repo = fx
        .open_with(
            Box::new(storage),
            &fx.tmp.path().join("cold"),
            OpenOptions::new().tolerate_broken_index(),
        )
        .unwrap();
    let err = repo
        .load_chunk_index_for_repair()
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("listing index.d/"), "{err}");
    assert!(err.contains("injected list failure"), "{err}");
    assert!(!err.contains("removing that object"), "{err}");
    assert_eq!(repo_state(&fx), before);
}

/// After a fold that changed the state, `index.d/` holds two chains that
/// nothing links: a client without a current cache is refused and changes
/// nothing; the client whose cache holds the latest state repairs.
#[test]
fn repair_after_a_state_changing_fold_needs_a_current_cache() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s1");
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    let mut policy = crate::repo::index_write::FoldPolicy::default();
    policy.hard.segments = 3;
    policy.hard.percent = 100_000;
    repo.set_fold_policy(policy);
    let mut counts = Vec::new();
    for round in 0..3u32 {
        repo.load_chunk_index_under_lock().unwrap();
        let id = repo.chunk_index().iter().next().unwrap().0;
        repo.chunk_index_mut().increment_refcount_by(&id, round + 1);
        repo.save_state().unwrap();
        counts.push(repo.index_head().unwrap().segments.len());
    }
    assert_eq!(counts, vec![2, 0, 1]);
    let head = fx.head();
    assert_eq!(head.retired.len(), 2);
    let live_base = head.base.as_ref().unwrap().name.to_hex();
    drop(repo);
    truncate_head(&fx);

    let cold = fx.other_client("cold");
    let before = repo_state(&fx);
    for mode in [RepairMode::PlanOnly, RepairMode::Apply] {
        let err =
            commands::check::run_with_repair(&cold, None, false, mode, None, &CancelToken::new())
                .err()
                .unwrap()
                .to_string();
        assert!(err.contains("2 different index states"), "{err}");
        assert!(err.contains("the empty index"), "{err}");
        assert!(err.contains(&live_base), "{err}");
        assert_eq!(repo_state(&fx), before);
    }

    let shown = plan_only(&fx.config).unwrap().plan;
    assert!(matches!(
        shown.index_recovery,
        IndexRecovery::HeadUnreadable {
            from_local_cache: true,
            ..
        }
    ));
    assert!(!shown.has_data_loss, "{:?}", shown.actions);
    commands::check::run_with_repair(
        &fx.config,
        None,
        false,
        RepairMode::ApplyShown {
            has_data_loss: false,
            index_recovery: shown.index_recovery,
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();
    let head = fx.head();
    assert!(head.base.is_some() && head.segments.is_empty());
    fx.check_clean(&fx.config);
    assert_eq!(read_restored(&fx.restore(&fx.config, "s1"), "a"), "alpha");
}

/// A cache older than the latest state is used as it is: the plan removes
/// the snapshot committed after it.
#[test]
fn repair_from_a_stale_local_cache_removes_later_snapshots() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    let b = fx.other_client("cache-b");
    let b_cache = PathBuf::from(b.cache_dir.as_ref().unwrap());
    drop(fx.open(&b_cache, true).unwrap());
    fx.write("c", "charlie");
    fx.backup(&fx.config, "s3");
    truncate_head(&fx);

    let plan = plan_only(&b).unwrap().plan;
    assert_eq!(
        plan.index_recovery,
        IndexRecovery::HeadUnreadable {
            from_local_cache: true,
            bases: 0,
            applied: 0,
            unplaced: 0,
            damaged: 0
        }
    );
    assert!(plan.has_data_loss, "{:?}", plan.actions);
    assert!(plan.actions.iter().any(removes("s3")), "{:?}", plan.actions);
    let names = format!("{:?}", plan.actions);
    assert!(
        !names.contains("\"s1\"") && !names.contains("\"s2\""),
        "{names}"
    );
}

/// A headless repair whose head PUT fails leaves its base behind; the next
/// cold repair lists it as a second end state, and succeeds once it is gone.
#[test]
fn rerun_after_a_failed_headless_repair_write_is_refused() {
    let fx = three_backups();
    truncate_head(&fx);
    let index_bytes = std::fs::read(fx.repo_dir.join("index")).unwrap();
    let listing = |fx: &Fixture| -> std::collections::BTreeSet<String> {
        fx.storage()
            .list(INDEX_OBJECT_DIR)
            .unwrap()
            .into_iter()
            .collect()
    };
    let before = listing(&fx);

    let (storage, _) = faulty(&fx, is_head_put);
    let mut repo = fx
        .open_with(
            storage,
            &fx.tmp.path().join("cold-a"),
            OpenOptions::new().tolerate_broken_index(),
        )
        .unwrap();
    assert!(matches!(
        repo.load_chunk_index_for_repair().unwrap(),
        IndexRecovery::HeadUnreadable {
            from_local_cache: false,
            ..
        }
    ));
    bump_one_refcount(&mut repo);
    repo.request_index_fold();
    assert!(repo.save_state().is_err());
    drop(repo);

    assert_eq!(
        std::fs::read(fx.repo_dir.join("index")).unwrap(),
        index_bytes
    );
    let after = listing(&fx);
    let added: Vec<&String> = after.difference(&before).collect();
    assert_eq!(added.len(), 1, "{added:?}");
    assert!(before.is_subset(&after));
    let debris = added[0].strip_prefix(INDEX_OBJECT_DIR).unwrap().to_string();

    let cold = fx.other_client("cold-b");
    let err = plan_only(&cold).err().unwrap().to_string();
    assert!(err.contains("different index states"), "{err}");
    assert!(err.contains(&debris), "{err}");

    std::fs::remove_file(fx.repo_dir.join(INDEX_OBJECT_DIR).join(&debris)).unwrap();
    let plan = plan_only(&cold).unwrap().plan;
    assert!(
        matches!(
            plan.index_recovery,
            IndexRecovery::HeadUnreadable {
                from_local_cache: false,
                bases: 0,
                applied: 3,
                ..
            }
        ),
        "{:?}",
        plan.index_recovery
    );
}

/// A segment whose head PUT failed follows the latest state; without a head
/// it is applied like a committed one, and the repair is clean.
#[test]
fn repair_without_a_head_applies_a_failed_write_s_segment() {
    let fx = three_backups();
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    repo.load_chunk_index_under_lock().unwrap();
    bump_one_refcount(&mut repo);
    repo.set_lock_fence(Arc::new(|| Err(VykarError::Other("lock lost".into()))));
    assert!(repo.save_state().is_err());
    drop(repo);
    truncate_head(&fx);

    let cold = fx.other_client("cold");
    let shown = plan_only(&cold).unwrap().plan;
    assert_eq!(
        shown.index_recovery,
        IndexRecovery::HeadUnreadable {
            from_local_cache: false,
            bases: 0,
            applied: 4,
            unplaced: 0,
            damaged: 0
        }
    );
    assert!(!shown.has_data_loss, "{:?}", shown.actions);
    commands::check::run_with_repair(
        &cold,
        None,
        false,
        RepairMode::ApplyShown {
            has_data_loss: false,
            index_recovery: shown.index_recovery,
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();
    fx.check_clean(&cold);
}

/// An `index` of a newer head layout or of an unknown type is refused, not
/// rebuilt over.
#[test]
fn repair_refuses_a_newer_index() {
    let fx = fixture(RepoFormat::V5);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    let crypto = vykar_crypto::PlaintextEngine::new(crate::testutil::test_chunk_hasher());
    let newer = pack_head(
        &IndexHead {
            layout: 2,
            ..fx.head()
        },
        &crypto,
    )
    .unwrap();
    for (bytes, expected) in [
        (newer, ["unsupported index head layout 2", "newer vykar"]),
        (vec![200u8; 16], ["type tag 200", "newer vykar"]),
    ] {
        std::fs::write(fx.repo_dir.join("index"), &bytes).unwrap();
        let before = repo_state(&fx);
        for mode in [RepairMode::PlanOnly, RepairMode::Apply] {
            let err = commands::check::run_with_repair(
                &fx.config,
                None,
                false,
                mode,
                None,
                &CancelToken::new(),
            )
            .err()
            .unwrap()
            .to_string();
            for text in expected {
                assert!(err.contains(text), "{err}");
            }
            assert!(err.contains("restore it from a copy"), "{err}");
            assert_eq!(repo_state(&fx), before);
        }
    }
}
