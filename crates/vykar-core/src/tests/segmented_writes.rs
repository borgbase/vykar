//! Writing repositories in the index format v6: backup commits,
//! maintenance writes, folds, garbage collection and repair.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::helpers::{
    FailGet, FaultyPut, PAST_T_PACK, age_repo_objects, backup_single_source, is_base_put,
    is_head_put, latest_head_key, make_test_config, set_store_now,
};
use crate::commands;
use crate::commands::check::{IndexLoss, IndexRecovery, RebuildCounts, RepairAction, RepairMode};
use crate::config::VykarConfig;
use crate::index::head::{IndexHead, ObjectName, SegmentKind};
use crate::index::table;
use crate::repo::control::CancelToken;
use crate::repo::format::{ObjectType, pack_object_streaming_with_context};
use crate::repo::index_store::{
    HEAD_PREFIX, INDEX_OBJECT_DIR, SEGMENT_CONTEXT, decode_segment_v2, object_key, pack_head,
};
use crate::repo::{EncryptionMode, Fence, OpenOptions, Repo, RepoFormat, WriteSessionOptions};
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
    Repo::init_at_format(
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
    ) -> Result<Repo> {
        Repo::open(storage, None, Some(cache.to_path_buf()), opts)
    }

    fn open(&self, cache: &Path, with_index: bool) -> Result<Repo> {
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

    /// The file of the latest head.
    fn head_path(&self) -> PathBuf {
        self.repo_dir.join(latest_head_key(&*self.storage()))
    }

    /// Remove every head.
    fn remove_heads(&self) {
        std::fs::remove_dir_all(self.repo_dir.join(HEAD_PREFIX)).unwrap();
        std::fs::create_dir_all(self.repo_dir.join(HEAD_PREFIX)).unwrap();
    }

    /// Whether every object `head` names is on disk.
    fn named_objects_exist(&self, head: &IndexHead) -> bool {
        head.named_objects().all(|n| {
            self.repo_dir
                .join(INDEX_OBJECT_DIR)
                .join(n.to_hex())
                .exists()
        })
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

/// The entries, sorted, as strings.
fn sorted_entries(index: &impl crate::testutil::EntryIter) -> Vec<String> {
    let mut v: Vec<String> = index
        .entry_vec()
        .iter()
        .map(|(id, e)| format!("{id:?} {} {} {}", e.pack_id, e.pack_offset, e.stored_size))
        .collect();
    v.sort();
    v
}

/// The index as a client with no local cache builds it.
fn cold_index(fx: &Fixture, name: &str) -> Vec<String> {
    let cold = fx.tmp.path().join(name);
    sorted_entries(fx.open(&cold, true).unwrap().chunk_index())
}

/// Every backup that stores new chunks appends one backup segment of new
/// entries and uploads nothing else to `index.d/` (a small
/// index stays below the fold limits); a backup that stores nothing new
/// writes no index object and no head (BK-1). Clients with and without a
/// local cache read the same index, and the snapshots restore.
#[test]
fn backups_append_one_segment_each() {
    for format in [
        RepoFormat::V6(vykar_types::hash::HashAlgorithm::Blake2b),
        RepoFormat::CURRENT,
    ] {
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

        // Unchanged files: reuse only, which is not persisted.
        let before = fx.head();
        fx.backup(&fx.config, "s3");
        let head = fx.head();
        assert_eq!(head, before, "no head written");
        assert_eq!(fx.index_objects(), 2);
        let repo = fx.open(&fx.cache(), false).unwrap();
        for seg_ref in &head.segments {
            assert_eq!(seg_ref.kind, SegmentKind::Backup);
            let bytes = fx
                .storage()
                .get(&object_key(&seg_ref.name))
                .unwrap()
                .unwrap();
            let seg = decode_segment_v2(seg_ref, &bytes, repo.crypto().as_ref()).unwrap();
            assert!(seg.removals.is_empty() && seg.relocations.is_empty());
            assert!(!seg.additions.is_empty());
            // BK-6: the recorded trailer length of each new pack is its
            // real frame.
            for (pack, trailer_len) in &seg.pack_trailers {
                let bytes = fx.storage().get(&pack.storage_key()).unwrap().unwrap();
                let blob_len = crate::repo::pack_trailer::parse_footer(&bytes).unwrap();
                assert_eq!(*trailer_len, 4 + blob_len);
            }
        }
        drop(repo);
        let info = commands::info::run(&fx.config, None)
            .unwrap()
            .index
            .unwrap();
        assert_eq!(info.segments, 2);
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

/// A backup commit leaves this host's cache dir with every local file the
/// new head needs (its segments, cached as written), and the index loaded
/// from them agrees with the one a cold client builds.
#[test]
fn backup_leaves_the_local_files_of_the_new_head() {
    for format in [
        RepoFormat::V6(vykar_types::hash::HashAlgorithm::Blake2b),
        RepoFormat::CURRENT,
    ] {
        let fx = fixture(format);
        fx.write("a", "alpha");
        fx.backup(&fx.config, "s1");
        fx.write("b", "bravo");
        fx.backup(&fx.config, "s2");

        let repo = fx.open(&fx.cache(), false).unwrap();
        let head = fx.head();
        assert!(head.base.is_none());
        assert_eq!(head.segments.len(), 2);
        let dir = crate::cache_paths::repo_cache_dir(&repo.config().id, Some(&fx.cache())).unwrap();
        for s in &head.segments {
            assert!(table::read_cached_segment(&dir, s).is_some(), "{s:?}");
        }
        let warm = fx.open(&fx.cache(), true).unwrap();
        assert_eq!(sorted_entries(warm.chunk_index()), cold_index(&fx, "cold"));
    }
}

/// `save_state` of a session with no changes writes nothing and reads no
/// index object: the loaded index stays, and the local files as they were.
#[test]
fn an_empty_session_leaves_the_table_as_is() {
    for format in [
        RepoFormat::V6(vykar_types::hash::HashAlgorithm::Blake2b),
        RepoFormat::CURRENT,
    ] {
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
                armed.load(Ordering::SeqCst)
                    && (k.starts_with(HEAD_PREFIX) || k.starts_with(INDEX_OBJECT_DIR))
            },
            usize::MAX,
        );
        let mut repo = fx
            .open_with(Box::new(storage), &cd, OpenOptions::new().with_index())
            .unwrap();
        let generation = repo.index_generation();
        let dir = crate::cache_paths::repo_cache_dir(&repo.config().id, Some(&cd)).unwrap();
        let before = local_files(&dir);
        assert!(!before.is_empty());
        fail.store(true, Ordering::SeqCst);

        let ws = repo.begin_write_session(&WriteSessionOptions::default());
        repo.unchecked_writer().save_session(ws).unwrap();

        assert_eq!(repo.index_generation(), generation);
        assert_eq!(local_files(&dir), before);
        assert_eq!(sorted_entries(repo.chunk_index()), cold_index(&fx, "cold"));
    }
}

/// Delete writes nothing to the index; compact sweeps the deleted
/// snapshot's chunks in a maintenance segment and, past the soft limit,
/// folds into a new base that retires the old objects. A later compact
/// removes aged debris but keeps every object a kept head names.
#[test]
fn delete_and_compact_sweep_fold_and_collect_garbage() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    fx.write("c", "charlie");
    fx.backup(&fx.config, "s3");
    let before = fx.head();
    let chunks_before = fx.open(&fx.cache(), true).unwrap().chunk_index().len();

    commands::delete::run(&fx.config, None, &["s1"], false, &CancelToken::new()).unwrap();
    assert_eq!(fx.head(), before, "delete writes no index object");
    assert_eq!(
        fx.open(&fx.cache(), true).unwrap().chunk_index().len(),
        chunks_before
    );
    fx.check_clean(&fx.config);

    compact(&fx, 4);
    let folded = fx.head();
    assert!(folded.segments.is_empty(), "past the soft limit");
    assert!(folded.base.is_some());
    assert!(folded.sweep.generation > before.generation);
    assert!(
        folded.sweep.generation < folded.generation,
        "the fold carries it"
    );
    assert!(folded.sweep.record.is_some());
    // Everything live before the compact is retired, with the sweep's
    // maintenance segment.
    assert!(
        before.live_objects().all(|n| folded.retired.contains(&n)),
        "{folded:?} vs {before:?}"
    );
    assert!(folded.retired.len() > before.segments.len());
    assert!(fx.open(&fx.cache(), true).unwrap().chunk_index().len() < chunks_before);
    assert!(fx.named_objects_exist(&folded));
    assert_eq!(
        cold_index(&fx, "cold2"),
        sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index())
    );
    fx.check_clean(&fx.config);

    // Debris of an interrupted write is removed; what kept heads name stays.
    let debris = fx.repo_dir.join(INDEX_OBJECT_DIR).join("ab".repeat(32));
    std::fs::write(&debris, b"debris").unwrap();
    compact(&fx, 4);
    let after = fx.head();
    assert_eq!(after.base, folded.base);
    assert!(after.segments.is_empty());
    assert!(!debris.exists());
    assert!(fx.named_objects_exist(&after));
    assert!(fx.named_objects_exist(&folded));
    fx.restore(&fx.config, "s3");
}

/// Compact with the soft limit at `segments` segments and no grace for
/// index garbage, every object aged past T_pack first: the segments are
/// foldable and the deleted snapshots' entries sweepable (04 step 7 is not
/// these tests' subject).
fn compact(fx: &Fixture, segments: usize) {
    age_repo_objects(&fx.repo_dir, &["index.d", "packs"], PAST_T_PACK);
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    let mut policy = crate::repo::index_write::FoldPolicy::default();
    policy.soft.segments = segments;
    repo.set_fold_policy(policy);
    repo.set_index_gc_grace(std::time::Duration::ZERO);
    commands::compact::run_on_repo(&mut repo, 0.0, None, false, &CancelToken::new()).unwrap();
}

/// A reader holding a head from before two production folds: after the
/// first its objects are retired and it completes on that head; once they
/// are deleted (the GC does so when they aged out), it retries on the
/// re-read head.
#[test]
fn reader_on_a_pre_fold_head_recovers_through_production_folds() {
    use crate::repo::index_store::{self, CacheLocation};

    let fx = fixture(RepoFormat::CURRENT);
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
        index_store::sync_index(
            &*storage,
            repo.crypto().as_ref(),
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
        std::fs::remove_file(fx.repo_dir.join(INDEX_OBJECT_DIR).join(name.to_hex())).unwrap();
    }
    let synced = sync(h0);
    assert_eq!(synced.head, fx.head(), "retried on the re-read head");
    assert_eq!(sorted_entries(&synced.index), cold_index(&fx, "cold"));
}

/// An index edit that writes a segment: removes the first entry, or puts
/// back the one `toggled` holds.
pub(crate) fn toggle_an_entry(
    repo: &mut Repo,
    toggled: &mut Option<(
        vykar_types::chunk_id::ChunkId,
        crate::index::ChunkIndexEntry,
        u32,
    )>,
) {
    match toggled.take() {
        Some((id, e, trailer_len)) => {
            let mut writer = repo.unchecked_writer();
            let index = writer.chunk_index_mut();
            index.set_pack_trailer(e.pack_id, trailer_len);
            index.add(id, e.stored_size, e.pack_id, e.pack_offset);
        }
        None => {
            let (id, e) = repo.chunk_index().iter().next().unwrap();
            let trailer_len = repo.chunk_index().pack_trailer_len(&e.pack_id).unwrap();
            repo.unchecked_writer().chunk_index_mut().remove(&id);
            *toggled = Some((id, e, trailer_len));
        }
    }
}

/// Every writer folds at the hard limit, so a repository that is never
/// compacted stays bounded.
#[test]
fn maintenance_writes_fold_at_the_hard_limit() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s1");
    // s1's segment is old, so the chain is foldable (04 step 7).
    age_repo_objects(&fx.repo_dir, &["index.d"], PAST_T_PACK);
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    set_store_now(&mut repo);
    let mut policy = crate::repo::index_write::FoldPolicy::default();
    policy.hard.segments = 3;
    policy.hard.percent = 100_000;
    repo.set_fold_policy(policy);

    // The backup left one segment over the empty base.
    let mut counts = Vec::new();
    let mut last = None;
    let mut toggled = None;
    for _ in 0..5u32 {
        repo.load_chunk_index_under_lock().unwrap();
        toggle_an_entry(&mut repo, &mut toggled);
        let live: Vec<_> = repo.index_head().unwrap().live_objects().collect();
        repo.unchecked_writer().save_state().unwrap();
        let head = repo.index_head().unwrap().clone();
        assert_eq!(head, fx.head());
        if head.segments.is_empty() {
            assert_eq!(head.retired, live, "a fold retires the previous live set");
        }
        counts.push(head.segments.len());
        last = Some(head);
    }
    // Round 2 repeats round 0's removal after the fold retired it: on a
    // plaintext repository the segment's name is already named (retired),
    // so that write folds too.
    assert_eq!(counts, vec![2, 0, 0, 1, 2]);
    assert!(fx.named_objects_exist(&last.unwrap()));
    assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
}

/// On a plaintext repository a change repeated from a state the chain
/// returned to packs to an object name the head already holds; the write
/// folds instead of naming it twice.
#[test]
fn repeated_plaintext_segment_folds() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    repo.load_chunk_index_under_lock().unwrap();
    // Remove, re-add, remove: the third write's segment is the first's.
    let mut toggled = None;
    for _ in 0..3 {
        toggle_an_entry(&mut repo, &mut toggled);
        repo.unchecked_writer().save_state().unwrap();
    }
    let head = fx.head();
    assert!(head.segments.is_empty(), "{head:?}");
    assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
}

/// A lost lock between the segment and the head PUT publishes nothing: the
/// head is unchanged and the segment is unreferenced debris.
#[test]
fn fence_failure_publishes_nothing() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    let head_path = fx.head_path();
    let head_bytes = std::fs::read(&head_path).unwrap();
    let objects = fx.index_objects();

    let mut repo = fx.open(&fx.cache(), false).unwrap();
    repo.load_chunk_index_under_lock().unwrap();
    let before = sorted_entries(repo.chunk_index());
    toggle_an_entry(&mut repo, &mut None);
    let err = repo
        .writer(Fence::lock(Arc::new(|| {
            Err(VykarError::Other("lock lost".into()))
        })))
        .save_state()
        .unwrap_err()
        .to_string();
    assert!(err.contains("lock lost"), "{err}");

    assert_eq!(fx.head_path(), head_path, "no new head");
    assert_eq!(std::fs::read(&head_path).unwrap(), head_bytes);
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
    let fx = fixture(RepoFormat::V6(vykar_types::hash::HashAlgorithm::Blake2b));
    let other = fx.other_client("cache-b");
    fx.write("a", "alpha");
    fx.backup(&fx.config, "a1");
    fx.write("b", "bravo");
    fx.backup(&other, "b1");
    fx.write("c", "charlie");
    fx.backup(&fx.config, "a2");
    fx.backup(&other, "b2");
    assert_eq!(fx.head().segments.len(), 3, "b2 stored nothing new");

    let a = sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index());
    let b_cache = PathBuf::from(other.cache_dir.as_ref().unwrap());
    let b = sorted_entries(fx.open(&b_cache, true).unwrap().chunk_index());
    assert_eq!(a, b);
    assert_eq!(a, cold_index(&fx, "cold"));
    fx.check_clean(&fx.config);
    fx.check_clean(&other);
}

/// Three backups; the files hold `alpha`, `bravo`, `charlie`.
fn three_backups() -> Fixture {
    let fx = fixture(RepoFormat::CURRENT);
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
        std::fs::read(fx.head_path()).unwrap(),
        fx.index_objects(),
        snapshots,
    )
}

fn read_restored(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(find(dir, name)).unwrap()
}

/// A segment object went missing. Loading the index fails and names
/// `check --repair`. Repair rebuilds the index from the packs, whose
/// trailers describe every chunk, so nothing is lost: every snapshot is kept
/// and restores. An apply that was shown another index reading refuses and
/// changes nothing.
#[test]
fn repair_rebuilds_a_broken_chain_from_the_packs() {
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
    let IndexRecovery::RebuiltFromPacks { cause, counts } = shown.index_recovery else {
        panic!("{:?}", shown.index_recovery);
    };
    assert_eq!(cause, IndexLoss::ChainDamaged);
    assert_eq!(counts.packs, pack_count(&fx));
    assert_eq!(counts.from_trailers, counts.packs);
    assert_eq!(counts.uncovered, 0);
    assert!(shown.uncovered_packs.is_empty());
    assert!(!shown.has_data_loss, "{:?}", shown.actions);
    assert!(
        matches!(
            shown.actions.as_slice(),
            [RepairAction::RemoveUnreferencedEntries]
        ),
        "{:?}",
        shown.actions
    );

    let before = repo_state(&fx);
    let err = repair(RepairMode::ApplyShown {
        has_data_loss: false,
        index_recovery: IndexRecovery::Complete,
    })
    .err()
    .unwrap()
    .to_string();
    assert!(err.contains("aborting repair"), "{err}");
    assert!(err.contains("read differently"), "{err}");
    assert_eq!(repo_state(&fx), before);

    repair(RepairMode::ApplyShown {
        has_data_loss: shown.has_data_loss,
        index_recovery: shown.index_recovery,
    })
    .unwrap();
    let repaired = fx.head();
    assert!(repaired.segments.is_empty() && repaired.retired.is_empty());
    assert!(repaired.base.is_some());
    assert_eq!(repaired.generation, head.generation + 1);
    assert_eq!(
        repaired.sweep.generation, repaired.generation,
        "a repair sets sweep.gen"
    );
    fx.check_clean(&other);

    assert_eq!(read_restored(&fx.restore(&other, "s1"), "a"), "alpha");
    assert_eq!(read_restored(&fx.restore(&other, "s2"), "b"), "bravo");
    assert_eq!(read_restored(&fx.restore(&other, "s3"), "c"), "charlie");

    let clean = repair(RepairMode::ApplyShown {
        has_data_loss: false,
        index_recovery: IndexRecovery::Complete,
    })
    .unwrap();
    assert!(!clean.plan.has_data_loss, "{:?}", clean.plan.actions);
    assert_eq!(clean.plan.index_recovery, IndexRecovery::Complete);
}

/// Packs on disk.
fn pack_count(fx: &Fixture) -> usize {
    fn walk(dir: &Path) -> usize {
        std::fs::read_dir(dir).map_or(0, |entries| {
            entries
                .flatten()
                .map(|e| {
                    let path = e.path();
                    if path.is_dir() { walk(&path) } else { 1 }
                })
                .sum()
        })
    }
    walk(&fx.repo_dir.join("packs"))
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
            .load_chunk_index_for_repair(&CancelToken::new())
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
    let crypto = Arc::clone(fx.open(&fx.cache(), false).unwrap().crypto());
    let storage = fx.storage();
    let mut head = fx.head();
    let seg = head.segments.last_mut().unwrap();
    let bytes = storage.get(&object_key(&seg.name)).unwrap().unwrap();
    let mut body = decode_segment_v2(seg, &bytes, crypto.as_ref())
        .unwrap()
        .encode()
        .unwrap();
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
        .put(
            &latest_head_key(&*storage),
            &pack_head(&head, crypto.as_ref()).unwrap(),
        )
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
        .load_chunk_index_for_repair(&CancelToken::new())
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("written by a newer vykar"), "{err}");
    assert!(err.contains("Nothing was changed"), "{err}");
    assert_eq!(repo_state(&fx), before);
}

/// A storage error while reading the chain is not damage, also for a
/// client whose cache holds the head's state: repair refuses, says why and
/// changes nothing.
#[test]
fn repair_load_refuses_a_storage_error_even_with_local_copies() {
    let fx = three_backups();
    let key = format!("{INDEX_OBJECT_DIR}{}", fx.head().segments[2].name.to_hex());
    let before = repo_state(&fx);
    let storage = FailGet::new(Arc::from(fx.storage()), move |k| k == key, usize::MAX);
    let mut repo = fx
        .open_with(
            Box::new(storage),
            &fx.cache(),
            OpenOptions::new().tolerate_broken_index(),
        )
        .unwrap();
    let err = repo
        .load_chunk_index_for_repair(&CancelToken::new())
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("injected GET failure"), "{err}");
    assert!(err.contains("changed nothing"), "{err}");
    drop(repo);
    assert_eq!(repo_state(&fx), before);
}

/// A segment missing or damaged on storage, on a client whose cache holds
/// the head's state: repair loads that state from the local copies and
/// writes it as a fresh base under a new head, so a client with an empty
/// cache opens the repository, restores every snapshot and checks clean. A
/// client without the copies rebuilds the index from the packs instead.
#[test]
fn repair_heals_a_damaged_chain_from_local_copies() {
    for remove in [true, false] {
        let case = if remove { "removed" } else { "corrupted" };
        let fx = three_backups();
        let head = fx.head();
        let warm = sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index());
        let lost = head.segments[1].name;
        let path = fx.repo_dir.join(INDEX_OBJECT_DIR).join(lost.to_hex());
        if remove {
            std::fs::remove_file(&path).unwrap();
        } else {
            let mut bytes = std::fs::read(&path).unwrap();
            let mid = bytes.len() / 2;
            bytes[mid] ^= 0xff;
            std::fs::write(&path, bytes).unwrap();
        }

        let repair = |config: &VykarConfig, mode| {
            commands::check::run_with_repair(config, None, false, mode, None, &CancelToken::new())
        };
        let cold = fx.other_client("cold-plan");
        let plan = repair(&cold, RepairMode::PlanOnly).unwrap().plan;
        assert!(
            matches!(
                plan.index_recovery,
                IndexRecovery::RebuiltFromPacks {
                    cause: IndexLoss::ChainDamaged,
                    ..
                }
            ),
            "{case}: {:?}",
            plan.index_recovery
        );
        let plan = repair(&fx.config, RepairMode::PlanOnly).unwrap().plan;
        assert_eq!(plan.index_recovery, IndexRecovery::Complete, "{case}");
        assert!(!plan.has_data_loss, "{case}: {:?}", plan.actions);

        repair(&fx.config, RepairMode::Apply).unwrap();
        let repaired = fx.head();
        assert!(repaired.generation > head.generation, "{case}");
        assert!(
            repaired.base.is_some() && repaired.segments.is_empty(),
            "{case}: {repaired:?}"
        );
        assert!(repaired.retired.contains(&lost), "{case}: {repaired:?}");
        for n in repaired.live_objects() {
            let key = object_key(&n);
            assert!(fx.storage().exists(&key).unwrap(), "{case}: {key}");
        }

        let after = fx.other_client("cold-after");
        assert_eq!(cold_index(&fx, "cold-index"), warm, "{case}");
        fx.check_clean(&after);
        assert_eq!(read_restored(&fx.restore(&after, "s1"), "a"), "alpha");
        assert_eq!(read_restored(&fx.restore(&after, "s2"), "b"), "bravo");
        assert_eq!(read_restored(&fx.restore(&after, "s3"), "c"), "charlie");
    }
}

/// Without a head, the index is rebuilt from the packs and nothing is lost.
#[test]
fn repair_rebuilds_a_missing_head_from_the_packs() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.remove_heads();
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
    assert!(
        matches!(
            plan.index_recovery,
            IndexRecovery::RebuiltFromPacks {
                cause: IndexLoss::HeadMissing,
                counts: RebuildCounts { uncovered: 0, .. },
            }
        ),
        "{:?}",
        plan.index_recovery
    );
    assert!(!plan.has_data_loss, "{:?}", plan.actions);
    commands::check::run_with_repair(
        &fx.config,
        None,
        false,
        RepairMode::Apply,
        None,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(read_restored(&fx.restore(&fx.config, "s1"), "a"), "alpha");
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

/// Client-side compact persists the swept index before deleting any pack,
/// including the packs the sweep left without entries.
#[test]
fn compact_deletes_packs_only_after_persisting_the_index() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s1");
    std::fs::remove_file(fx.source.join("b")).unwrap();
    fx.write("c", "charlie");
    fx.backup(&fx.config, "s2");
    fx.write("d", "delta");
    fx.backup(&fx.config, "s3");
    commands::delete::run(&fx.config, None, &["s1"], false, &CancelToken::new()).unwrap();
    age_repo_objects(&fx.repo_dir, &["index.d", "packs"], PAST_T_PACK);

    let ops = Arc::new(Mutex::new(Vec::new()));
    let storage = Box::new(MutLog {
        inner: Arc::from(fx.storage()),
        ops: Arc::clone(&ops),
    });
    let mut repo = fx
        .open_with(storage, &fx.cache(), OpenOptions::new())
        .unwrap();
    let stats = commands::util::with_maintenance_lock(&mut repo, |repo| {
        repo.load_chunk_index_under_lock()?;
        ops.lock().unwrap().clear();
        commands::compact::compact_repo(repo, 0.0, None, false, 2, &CancelToken::new())
    })
    .unwrap();
    assert!(stats.chunks_unreferenced > 0, "{stats:?}");

    let ops = ops.lock().unwrap().clone();
    let first_pack_delete = ops
        .iter()
        .position(|o| o.starts_with("delete packs/"))
        .unwrap_or_else(|| panic!("no pack deleted: {ops:?}"));
    let head_puts: Vec<usize> = ops
        .iter()
        .enumerate()
        .filter(|(_, o)| o.starts_with("put index.h/"))
        .map(|(i, _)| i)
        .collect();
    assert!(
        head_puts.first().is_some_and(|&i| i < first_pack_delete),
        "{ops:?}"
    );
    // The sweep's head, and at most the batch's one.
    let before_deletes = head_puts.iter().filter(|&&i| i < first_pack_delete).count();
    assert!((1..=2).contains(&before_deletes), "{ops:?}");
}

#[derive(Debug, Clone, Copy)]
enum Damage {
    /// The latest head is truncated.
    TruncatedHead,
    /// The last segment object is gone.
    LostSegment,
}

/// Three backups, then `damage`. Returns the keys under `index.d/` and the
/// bytes of the latest head after the damage.
fn damaged(damage: Damage) -> (Fixture, std::collections::BTreeSet<String>, Vec<u8>) {
    let fx = three_backups();
    match damage {
        Damage::TruncatedHead => truncate_head(&fx),
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
    let index = std::fs::read(fx.head_path()).unwrap();
    (fx, before, index)
}

/// Opens the damaged repository through `storage` with a fresh cache
/// directory and loads the index as repair does.
fn open_for_repair(fx: &Fixture, storage: Box<dyn StorageBackend>, cache: &str) -> Repo {
    let mut repo = fx
        .open_with(
            storage,
            &fx.tmp.path().join(cache),
            OpenOptions::new().tolerate_broken_index(),
        )
        .unwrap();
    repo.load_chunk_index_for_repair(&CancelToken::new())
        .unwrap();
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
/// head PUT or the fence between them leaves the heads and every object in
/// `index.d/` as they were, so the next repair reads the same index.
#[test]
fn fold_without_a_head_keeps_old_objects_until_the_head_is_written() {
    #[derive(Debug, Clone, Copy)]
    enum Failure {
        BasePut,
        HeadPut,
        SecondFence,
    }
    for damage in [Damage::TruncatedHead, Damage::LostSegment] {
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
            let mut fence = Fence::unchecked();
            if let Failure::SecondFence = failure {
                let probe: Arc<dyn StorageBackend> = Arc::from(fx.storage());
                let known = before.clone();
                fence = Fence::lock(Arc::new(move || {
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
            let heads_before = fx.storage().list(HEAD_PREFIX).unwrap().len();
            assert!(repo.writer(fence).save_state().is_err(), "{case}");
            drop(repo);

            assert_eq!(fx.storage().list(HEAD_PREFIX).unwrap().len(), heads_before);
            assert_eq!(
                std::fs::read(fx.head_path()).unwrap(),
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
            repo.unchecked_writer().save_state().unwrap();
            drop(repo);
            let head = fx.head();
            assert!(head.base.is_some(), "{case}");
            assert!(
                head.segments.is_empty() && head.retired.is_empty(),
                "{case}"
            );
            assert!(fx.named_objects_exist(&head), "{case}");
            assert_eq!(cold_index(&fx, &format!("{case}-after")), loaded, "{case}");
        }
    }
}

/// A fold without a previous head PUTs the base, then the head, and
/// deletes nothing in `index.d/` before the head PUT (the young objects of
/// the damaged index are not deleted at all).
#[test]
fn fold_without_a_head_deletes_nothing_before_the_head_put() {
    for damage in [Damage::TruncatedHead, Damage::LostSegment] {
        let (fx, before, _) = damaged(damage);
        let (storage, ops) = faulty(&fx, |_, _| false);
        let mut repo = open_for_repair(&fx, storage, "cold");
        ops.lock().unwrap().clear();
        repo.request_index_fold();
        repo.unchecked_writer().save_state().unwrap();
        drop(repo);

        let new = fx.head();
        let base_key = format!("put {INDEX_OBJECT_DIR}{}", new.base.unwrap().name.to_hex());
        let ops = ops.lock().unwrap().clone();
        let base_put = ops.iter().position(|o| *o == base_key).unwrap();
        let head_put = ops
            .iter()
            .position(|o| o.starts_with("put index.h/"))
            .unwrap();
        assert!(base_put < head_put, "{damage:?}: {ops:?}");
        assert!(
            !ops.iter()
                .any(|o| o.starts_with(&format!("delete {INDEX_OBJECT_DIR}"))),
            "{damage:?}: {ops:?}"
        );
        let after: std::collections::BTreeSet<String> = fx
            .storage()
            .list(INDEX_OBJECT_DIR)
            .unwrap()
            .into_iter()
            .collect();
        assert!(before.is_subset(&after), "{damage:?}");
    }
}

/// Fails listings of `index.d/` once there are more than `heads_before`
/// heads, that is, once a new head has been PUT.
struct FailListAfterHeadPut {
    inner: Arc<dyn StorageBackend>,
    heads_before: usize,
}

impl FailListAfterHeadPut {
    fn check(&self, prefix: &str) -> Result<()> {
        if prefix.starts_with(INDEX_OBJECT_DIR)
            && self.inner.list(HEAD_PREFIX)?.len() != self.heads_before
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
    for damage in [Damage::TruncatedHead, Damage::LostSegment] {
        let (fx, before, index_before) = damaged(damage);
        let storage = FailListAfterHeadPut {
            inner: Arc::from(fx.storage()),
            heads_before: fx.storage().list(HEAD_PREFIX).unwrap().len(),
        };
        let mut repo = open_for_repair(&fx, Box::new(storage), "cold");
        let loaded = sorted_entries(repo.chunk_index());
        repo.request_index_fold();
        repo.unchecked_writer().save_state().unwrap();
        drop(repo);

        assert_ne!(
            std::fs::read(fx.head_path()).unwrap(),
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

/// With every snapshot deleted, compact refuses to sweep the index to
/// empty (R1a) and changes nothing; `check --repair` removes the entries no
/// snapshot references and folds to a head without a base, as `init`
/// writes it. Later writes append to it and fold into a base again.
#[test]
fn empty_index_folds_to_a_head_without_base() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    commands::delete::run(&fx.config, None, &["s1", "s2"], false, &CancelToken::new()).unwrap();
    let entries = fx.open(&fx.cache(), true).unwrap().chunk_index().len();
    assert!(entries > 0);

    let head = fx.head();
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    let err = commands::compact::run_on_repo(&mut repo, 0.0, None, false, &CancelToken::new())
        .unwrap_err()
        .to_string();
    drop(repo);
    assert!(err.starts_with(commands::compact::MARK_REFUSED), "{err}");
    assert!(err.contains("no snapshot was listed"), "{err}");
    assert!(err.contains(&format!("holds {entries} chunks")), "{err}");
    assert_eq!(fx.head(), head, "nothing written");

    let live: Vec<_> = head.live_objects().collect();
    commands::check::run_with_repair(
        &fx.config,
        None,
        false,
        RepairMode::Apply,
        None,
        &CancelToken::new(),
    )
    .unwrap();
    let folded = fx.head();
    assert!(folded.base.is_none(), "{folded:?}");
    assert!(folded.segments.is_empty(), "{folded:?}");
    assert!(folded.is_empty());
    assert!(
        live.iter().all(|n| folded.retired.contains(n)),
        "{folded:?} vs {live:?}"
    );
    let info = commands::info::run(&fx.config, None).unwrap();
    assert_eq!(info.index.unwrap().base_bytes, 0);
    assert!(cold_index(&fx, "cold1").is_empty());
    fx.check_clean(&fx.config);

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
    assert!(fx.named_objects_exist(&head));
    assert_eq!(cold_index(&fx, "cold3"), warm);
    fx.check_clean(&fx.config);
    assert_eq!(read_restored(&fx.restore(&fx.config, "s3"), "c"), "charlie");
}

/// A compact whose sweep head PUT fails, or whose fold fails at the base
/// PUT, leaves the latest head naming the same live objects, all in place;
/// the next compact folds.
#[test]
fn put_base_failures_with_a_previous_head_leave_it_intact() {
    type Fail = fn(&str, &[u8]) -> bool;
    for (name, fail) in [("base", is_base_put as Fail), ("head", is_head_put as Fail)] {
        let fx = three_backups();
        age_repo_objects(&fx.repo_dir, &["index.d", "packs"], PAST_T_PACK);
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

        assert_eq!(fx.head().live_objects().collect::<Vec<_>>(), live, "{name}");
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
        let head = fx.head();
        assert!(head.base.is_some() && head.segments.is_empty(), "{name}");
        assert!(fx.named_objects_exist(&head), "{name}");
    }
}

/// A partial fold over [old b0, young b1] whose maintenance write fails at
/// the base PUT, its own segment's PUT or the head PUT leaves the previous
/// head and index in place; a retry on the same handle then succeeds, and
/// the handle's index, a warm reopen and a cold one all hold the edited
/// state.
#[test]
fn a_partial_fold_failing_before_its_head_retries_on_the_same_handle() {
    type Fail = fn(&str, &[u8]) -> bool;
    fn is_segment_put(key: &str, data: &[u8]) -> bool {
        key.starts_with(INDEX_OBJECT_DIR) && data.first() == Some(&(ObjectType::IndexSegment as u8))
    }
    for (name, fail) in [
        ("base", is_base_put as Fail),
        ("segment", is_segment_put as Fail),
        ("head", is_head_put as Fail),
    ] {
        let fx = fixture(RepoFormat::CURRENT);
        fx.write("a", "alpha");
        fx.write("b", "bravo");
        fx.backup(&fx.config, "s0");
        age_segment(&fx, &fx.head().segments[0].name);
        fx.write("c", "charlie");
        fx.backup(&fx.config, "s1");
        let before = fx.head();
        let unedited = sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index());

        let armed = Arc::new(AtomicBool::new(true));
        let on = Arc::clone(&armed);
        let (storage, _ops) = faulty(&fx, move |k, d| on.load(Ordering::SeqCst) && fail(k, d));
        let mut repo = fx
            .open_with(storage, &fx.cache(), OpenOptions::new())
            .unwrap();
        set_store_now(&mut repo);
        repo.load_chunk_index_under_lock().unwrap();
        let mut toggled = None;
        toggle_an_entry(&mut repo, &mut toggled);
        repo.request_index_fold();
        let want = sorted_entries(repo.chunk_index());
        assert_ne!(want, unedited);
        let err = repo
            .unchecked_writer()
            .save_state()
            .unwrap_err()
            .to_string();
        assert!(err.contains("injected failure"), "{name}: {err}");
        assert_eq!(fx.head(), before, "{name}");
        assert_eq!(
            cold_index(&fx, &format!("cold-failed-{name}")),
            unedited,
            "{name}"
        );

        armed.store(false, Ordering::SeqCst);
        repo.unchecked_writer().save_state().unwrap();
        assert_eq!(sorted_entries(repo.chunk_index()), want, "{name}");
        let head = fx.head();
        head.validate().unwrap();
        assert!(head.generation > before.generation, "{name}: {head:?}");
        // A requested fold stays requested over a failed attempt: the retry
        // folds the old segment into a new base and keeps the young one.
        assert_ne!(head.base, before.base, "{name}: {head:?}");
        assert_eq!(
            head.segments.first(),
            before.segments.get(1),
            "{name}: {head:?}"
        );
        assert!(fx.named_objects_exist(&head), "{name}");
        assert_eq!(cold_index(&fx, &format!("cold-{name}")), want, "{name}");
        drop(repo);
        assert_eq!(
            sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index()),
            want,
            "{name}"
        );
    }
}

/// Repair of a repository with no head and no snapshots writes the empty
/// head at generation 1, recorded as a repair; the young debris in
/// `index.d/` stays until it ages out.
#[test]
fn repair_of_an_empty_index_writes_the_empty_head() {
    let fx = fixture(RepoFormat::CURRENT);
    std::fs::write(
        fx.repo_dir.join(INDEX_OBJECT_DIR).join("ab".repeat(32)),
        b"debris",
    )
    .unwrap();
    fx.remove_heads();
    commands::check::run_with_repair(
        &fx.config,
        None,
        false,
        RepairMode::Apply,
        None,
        &CancelToken::new(),
    )
    .unwrap();
    let head = fx.head();
    let mut expected = IndexHead::initial();
    expected.sweep.generation = 1;
    assert_eq!(head, expected);
    fx.check_clean(&fx.config);
}

/// Two backups over the same cache, then a repository at the second head
/// loaded under the lock. Returns the fixture, the repo cache dir, the
/// repository and the head after the first backup.
fn loaded_after_two_backups() -> (Fixture, PathBuf, Repo, IndexHead) {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    let first = fx.head();
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    repo.load_chunk_index_under_lock().unwrap();
    let dir = crate::cache_paths::repo_cache_dir(&repo.config().id, Some(&fx.cache())).unwrap();
    (fx, dir, repo, first)
}

/// An index edit that writes a segment: removes one entry.
fn remove_one_entry(repo: &mut Repo) {
    let id = repo.chunk_index().iter().next().unwrap().0;
    repo.unchecked_writer().chunk_index_mut().remove(&id);
}

/// The local index files of `dir` with their bytes, sorted by name.
fn local_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut v: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_str()?.to_string();
            (name.starts_with(table::BASE_PREFIX) || name.starts_with(table::SEGMENT_PREFIX))
                .then(|| (name, std::fs::read(e.path()).unwrap()))
        })
        .collect();
    v.sort();
    v
}

/// A write appends one segment and rewrites no local file: the cached
/// segments stay as they were and the new one is cached beside them.
#[test]
fn a_write_appends_a_segment_and_rewrites_no_local_file() {
    let (fx, dir, mut repo, first) = loaded_after_two_backups();
    let before = repo.index_head().cloned().unwrap();
    assert!(first.segments.iter().all(|s| before.segments.contains(s)));
    let objects = fx.index_objects();
    let files = local_files(&dir);

    remove_one_entry(&mut repo);
    repo.unchecked_writer().save_state().unwrap();

    let head = repo.index_head().cloned().unwrap();
    assert_eq!(head, fx.head());
    assert_eq!(head.segments.len(), before.segments.len() + 1);
    assert_eq!(head.base, before.base);
    assert_eq!(head.retired, before.retired);
    assert_eq!(fx.index_objects(), objects + 1, "one segment, no base");
    let now = local_files(&dir);
    assert!(files.iter().all(|f| now.contains(f)), "nothing rewritten");
    assert_eq!(now.len(), files.len() + 1, "the new segment is cached");
    assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
}

/// A write needs no local file: removing them, or putting garbage under a
/// cached segment's name, changes nothing about it.
#[test]
fn a_write_does_not_need_the_local_files() {
    for garbage in [false, true] {
        let (fx, dir, mut repo, _) = loaded_after_two_backups();
        let before = repo.index_head().cloned().unwrap();
        for (name, _) in local_files(&dir) {
            std::fs::remove_file(dir.join(&name)).unwrap();
            if garbage {
                std::fs::write(dir.join(&name), b"garbage").unwrap();
            }
        }

        remove_one_entry(&mut repo);
        repo.unchecked_writer().save_state().unwrap();

        let head = fx.head();
        assert_eq!(head.base, before.base, "{garbage}");
        assert_eq!(head.segments.len(), before.segments.len() + 1, "{garbage}");
        assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
        let warm = fx.open(&fx.cache(), true).unwrap();
        assert_eq!(sorted_entries(warm.chunk_index()), cold_index(&fx, "cold2"));
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

    remove_one_entry(&mut repo);
    let err = repo
        .unchecked_writer()
        .save_state()
        .unwrap_err()
        .to_string();
    assert!(err.contains("index changed on storage"), "{err}");
    assert_eq!(fx.head(), moved);
}

/// Damage the head: keep its first half.
fn truncate_head(fx: &Fixture) {
    let path = fx.head_path();
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

/// An unreadable head is rebuilt from the packs, on this machine as on
/// another one; the repaired index equals the one before the damage.
#[test]
fn repair_rebuilds_from_a_corrupt_head() {
    let fx = three_backups();
    let before = cold_index(&fx, "cold-before");
    truncate_head(&fx);
    assert!(fx.open(&fx.cache(), false).is_err());
    let packs = pack_count(&fx);
    let from_trailers = |recovery: IndexRecovery| match recovery {
        IndexRecovery::RebuiltFromPacks {
            cause: IndexLoss::HeadUnreadable,
            counts,
        } => {
            assert_eq!(counts.packs, packs, "{counts:?}");
            assert_eq!(counts.uncovered, 0, "{counts:?}");
            counts.from_trailers
        }
        other => panic!("{other:?}"),
    };

    let warm = plan_only(&fx.config).unwrap().plan;
    assert_eq!(from_trailers(warm.index_recovery), packs);
    assert!(!warm.has_data_loss, "{:?}", warm.actions);

    let other = fx.other_client("cache-b");
    let shown = plan_only(&other).unwrap().plan;
    assert_eq!(from_trailers(shown.index_recovery), packs);
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
    assert!(fx.named_objects_exist(&head));
    assert_eq!(cold_index(&fx, "cold-after"), before);
    fx.check_clean(&other);
    assert_eq!(read_restored(&fx.restore(&other, "s3"), "c"), "charlie");
    fx.write("d", "delta");
    fx.backup(&other, "s4");
    assert_eq!(fx.head().segments.len(), 1);
}

/// Fails every listing below `prefix`.
struct FailList {
    inner: Arc<dyn StorageBackend>,
    prefix: &'static str,
}

impl FailList {
    fn check(&self, prefix: &str) -> Result<()> {
        if prefix.starts_with(self.prefix) {
            return Err(VykarError::Other("injected list failure".into()));
        }
        Ok(())
    }
}

impl InnerBackend for FailList {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for FailList;
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

/// A failed listing of `packs/` or `packmaps/` on an unreadable head stops
/// the rebuild; the error says so, without the advice to remove an index
/// object, and nothing is changed.
#[test]
fn repair_without_a_head_names_a_failed_listing() {
    let fx = three_backups();
    truncate_head(&fx);
    let before = repo_state(&fx);
    for prefix in ["packs/", "packmaps/"] {
        let storage = FailList {
            inner: Arc::from(fx.storage()),
            prefix,
        };
        let mut repo = fx
            .open_with(
                Box::new(storage),
                &fx.tmp.path().join("cold"),
                OpenOptions::new().tolerate_broken_index(),
            )
            .unwrap();
        let err = repo
            .load_chunk_index_for_repair(&CancelToken::new())
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("rebuilding the index from the packs"),
            "{prefix}: {err}"
        );
        assert!(err.contains("injected list failure"), "{prefix}: {err}");
        assert!(err.contains("Nothing was changed"), "{prefix}: {err}");
        assert!(!err.contains("removing that object"), "{prefix}: {err}");
        assert_eq!(repo_state(&fx), before);
    }
}

/// A cache older than the latest state does not matter: the index is
/// rebuilt from the packs, and the snapshot committed after the cache's
/// state is kept.
#[test]
fn repair_with_a_stale_local_cache_keeps_later_snapshots() {
    let fx = fixture(RepoFormat::CURRENT);
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
    assert!(
        matches!(
            plan.index_recovery,
            IndexRecovery::RebuiltFromPacks {
                cause: IndexLoss::HeadUnreadable,
                counts: RebuildCounts {
                    uncovered: 0,
                    from_local_tables: 0,
                    ..
                },
            }
        ),
        "{:?}",
        plan.index_recovery
    );
    assert!(!plan.has_data_loss, "{:?}", plan.actions);
    assert!(
        !plan.actions.iter().any(removes("s3")),
        "{:?}",
        plan.actions
    );
    commands::check::run_with_repair(
        &b,
        None,
        false,
        RepairMode::Apply,
        None,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(read_restored(&fx.restore(&b, "s3"), "c"), "charlie");
}

/// A repair whose head PUT fails leaves its base behind in `index.d/`; the
/// next cold repair rebuilds from the packs anyway and is clean.
#[test]
fn rerun_after_a_failed_rebuild_write_succeeds() {
    let fx = three_backups();
    truncate_head(&fx);
    let head_path = fx.head_path();
    let index_bytes = std::fs::read(&head_path).unwrap();
    let before = fx.index_objects();

    let (storage, _) = faulty(&fx, is_head_put);
    let mut repo = fx
        .open_with(
            storage,
            &fx.tmp.path().join("cold-a"),
            OpenOptions::new().tolerate_broken_index(),
        )
        .unwrap();
    assert!(matches!(
        repo.load_chunk_index_for_repair(&CancelToken::new())
            .unwrap()
            .recovery,
        IndexRecovery::RebuiltFromPacks {
            cause: IndexLoss::HeadUnreadable,
            ..
        }
    ));
    remove_one_entry(&mut repo);
    repo.request_index_fold();
    assert!(repo.unchecked_writer().save_state().is_err());
    drop(repo);
    assert_eq!(fx.head_path(), head_path);
    assert_eq!(std::fs::read(&head_path).unwrap(), index_bytes);
    assert_eq!(fx.index_objects(), before + 1);

    let cold = fx.other_client("cold-b");
    let plan = plan_only(&cold).unwrap().plan;
    assert!(!plan.has_data_loss, "{:?}", plan.actions);
    commands::check::run_with_repair(
        &cold,
        None,
        false,
        RepairMode::ApplyShown {
            has_data_loss: false,
            index_recovery: plan.index_recovery,
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();
    assert!(fx.named_objects_exist(&fx.head()));
    fx.check_clean(&cold);
    assert_eq!(read_restored(&fx.restore(&cold, "s3"), "c"), "charlie");
}

/// A latest head of a newer layout, with unknown features, or of an unknown
/// type is refused, not rebuilt over; so is a head of an older development
/// layout, with its own advice.
#[test]
fn repair_refuses_a_newer_index() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    let crypto = vykar_crypto::PlaintextEngine::new(crate::testutil::test_chunk_hasher());
    let newer = pack_head(
        &IndexHead {
            layout: 4,
            ..fx.head()
        },
        &crypto,
    )
    .unwrap();
    let featured = pack_head(
        &IndexHead {
            features: vec!["later".into()],
            ..fx.head()
        },
        &crypto,
    )
    .unwrap();
    let older = pack_head(
        &IndexHead {
            layout: 2,
            ..fx.head()
        },
        &crypto,
    )
    .unwrap();
    let head_path = fx.head_path();
    let newer_vykar = ["newer vykar", "restore it from a copy"];
    for (bytes, expected) in [
        (
            newer,
            [
                "unsupported index head layout 4",
                newer_vykar[0],
                newer_vykar[1],
            ],
        ),
        (
            featured,
            ["feature(s) later", newer_vykar[0], newer_vykar[1]],
        ),
        (
            vec![200u8; 16],
            ["type tag 200", newer_vykar[0], newer_vykar[1]],
        ),
        (
            older,
            [
                "unsupported index head layout 2",
                "older development build",
                "Recreate the repository",
            ],
        ),
    ] {
        std::fs::write(&head_path, &bytes).unwrap();
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
            assert!(err.contains("Nothing was changed"), "{err}");
            assert_eq!(repo_state(&fx), before);
        }
    }
}

// ── Young segments and the partial fold (04 step 7) ─────────────────────

/// Move segment `name`'s mtime back past T_pack: it is old on the store's
/// clock.
fn age_segment(fx: &Fixture, name: &ObjectName) {
    let path = fx.repo_dir.join(INDEX_OBJECT_DIR).join(name.to_hex());
    let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(mtime - PAST_T_PACK)
        .unwrap();
}

/// Back up the fixture's source as `name` through the steppable commit,
/// with `wrap` around its storage and `tweak` applied to its handle before
/// the commit (a fold request, fold limits).
fn backup_with(
    fx: &Fixture,
    name: &str,
    wrap: Option<commands::backup::BackendWrapper<'_>>,
    tweak: impl FnOnce(&mut Repo),
) {
    fn no_events(_: crate::repo::control::CommitEvent) {}
    let source_paths = vec![fx.source.to_string_lossy().into_owned()];
    let req = commands::backup::BackupRequest {
        snapshot_name: name,
        passphrase: None,
        source_paths: &source_paths,
        source_label: "src",
        exclude_patterns: &[],
        exclude_if_present: &[],
        one_file_system: true,
        git_ignore: false,
        xattrs_enabled: false,
        compression: crate::compress::Compression::None,
        command_dumps: &[],
        verbose: false,
    };
    let mut pending = commands::backup::upload(
        &fx.config,
        &req,
        &mut None,
        &CancelToken::new(),
        wrap,
        crate::repo::HostEnv::system(),
    )
    .map_err(|(_, e)| e)
    .unwrap();
    tweak(pending.repo_mut());
    let guard = pending
        .wait_for_lock(&mut None, &CancelToken::new())
        .unwrap();
    let mut commit = pending.begin_commit(commands::backup::CommitLock::Held(guard));
    let result = commit.drive(&no_events);
    commit
        .finish(result, &mut None, &crate::repo::control::CancelToken::new())
        .unwrap();
}

/// `begin_commit` runs a commit under the commit lock's fence, and a
/// lock-free one under `Fence::lock_free_backup` (which passes: the commit
/// machine checks its marker itself).
#[test]
fn begin_commit_picks_the_fence_of_its_exclusion() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    let source_paths = vec![fx.source.to_string_lossy().into_owned()];
    let upload = |name: &str| {
        let req = commands::backup::BackupRequest {
            snapshot_name: name,
            passphrase: None,
            source_paths: &source_paths,
            source_label: "src",
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: crate::compress::Compression::None,
            command_dumps: &[],
            verbose: false,
        };
        commands::backup::upload(
            &fx.config,
            &req,
            &mut None,
            &CancelToken::new(),
            None,
            crate::repo::HostEnv::system(),
        )
        .map_err(|(_, e)| e)
        .unwrap()
    };

    let free = upload("free").begin_commit(commands::backup::CommitLock::Free);
    assert!(free.fence().is_lock_free_backup());
    let err = free
        .finish(Err(VykarError::Interrupted), &mut None, &CancelToken::new())
        .unwrap_err();
    assert!(matches!(err, VykarError::Interrupted));

    let pending = upload("held");
    let guard = pending
        .wait_for_lock(&mut None, &CancelToken::new())
        .unwrap();
    let mut held = pending.begin_commit(commands::backup::CommitLock::Held(guard));
    assert!(!held.fence().is_lock_free_backup());
    let result = held.drive(&|_: crate::repo::control::CommitEvent| {});
    held.finish(result, &mut None, &CancelToken::new()).unwrap();
}

/// A backup whose commit folds (requested: the hard limit's stand-in)
/// writes into the new base only the old segment s1; s2, young, stays a
/// segment, and so does the backup's own segment, after it. The head
/// retires only what it folded; warm and cold indexes agree; every
/// snapshot restores.
#[test]
fn a_backup_fold_keeps_young_segments_and_its_own() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    let before = fx.head();
    let (s1, s2) = (before.segments[0].name, before.segments[1].name);
    age_segment(&fx, &s1);

    fx.write("c", "charlie");
    backup_with(&fx, "s3", None, Repo::request_index_fold);
    let head = fx.head();
    head.validate().unwrap();
    assert!(head.base.is_some(), "{head:?}");
    assert_eq!(head.segments.len(), 2, "{head:?}");
    assert_eq!(head.segments[0].name, s2);
    let own = &head.segments[1];
    assert_eq!(own.kind, SegmentKind::Backup);
    assert!(!before.named_objects().any(|n| n == own.name));
    assert_eq!(head.retired, vec![s1]);
    assert!(fx.named_objects_exist(&head));
    let warm = sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index());
    assert_eq!(cold_index(&fx, "cold"), warm);
    fx.check_clean(&fx.config);
    for (snapshot, file, content) in [
        ("s1", "a", "alpha"),
        ("s2", "b", "bravo"),
        ("s3", "c", "charlie"),
    ] {
        assert_eq!(
            read_restored(&fx.restore(&fx.config, snapshot), file),
            content
        );
    }
}

/// A requested fold over young backup segments only has nothing to fold:
/// the commit writes its segment, no base, and collects no garbage.
#[test]
fn a_backup_fold_with_nothing_old_writes_a_segment() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    let before = fx.head();
    fx.write("c", "charlie");
    backup_with(&fx, "s3", None, Repo::request_index_fold);
    let head = fx.head();
    assert!(head.base.is_none(), "{head:?}");
    assert_eq!(head.segments.len(), 3);
    assert_eq!(head.segments[..2], before.segments[..]);
    assert!(head.retired.is_empty());
    assert_eq!(fx.index_objects(), 3, "no base");
    assert_eq!(
        cold_index(&fx, "cold"),
        sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index())
    );
}

/// The hard limit counts only the foldable prefix: three young backup
/// segments at a limit of two stay segments; once the first two are old,
/// the next backup folds them and keeps s3 and its own segment.
#[test]
fn the_hard_limit_counts_only_old_segments() {
    let fx = fixture(RepoFormat::CURRENT);
    let limits = |repo: &mut Repo| {
        let mut policy = crate::repo::index_write::FoldPolicy::default();
        policy.hard.segments = 2;
        policy.hard.percent = 100_000;
        repo.set_fold_policy(policy);
    };
    for (n, name) in ["s1", "s2", "s3"].iter().enumerate() {
        fx.write(&format!("f{n}"), name);
        backup_with(&fx, name, None, limits);
    }
    let three = fx.head();
    assert!(three.base.is_none(), "{three:?}");
    assert_eq!(three.segments.len(), 3);

    let names: Vec<ObjectName> = three.segments.iter().map(|s| s.name).collect();
    age_segment(&fx, &names[0]);
    age_segment(&fx, &names[1]);
    fx.write("f3", "s4");
    backup_with(&fx, "s4", None, limits);
    let head = fx.head();
    assert!(head.base.is_some(), "{head:?}");
    assert_eq!(head.segments.len(), 2, "{head:?}");
    assert_eq!(head.segments[0].name, names[2]);
    assert_eq!(head.retired, names[..2].to_vec());
    assert_eq!(
        cold_index(&fx, "cold"),
        sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index())
    );
    fx.check_clean(&fx.config);
}

/// A maintenance write at the hard limit behind a young backup segment has
/// nothing to fold and writes its segment; once the backup segment is old
/// the whole chain and the edits fold. Behind an old backup segment and a
/// young one, a requested fold writes the old one into the base and
/// appends the write's own segment after the kept ones. The handle's
/// rebuilt index equals a cold one each time.
#[test]
fn a_maintenance_fold_behind_a_young_segment_appends_its_edits() {
    let policy = {
        let mut p = crate::repo::index_write::FoldPolicy::default();
        p.hard.segments = 3;
        p.hard.percent = 100_000;
        p
    };
    let open = |fx: &Fixture| {
        let mut repo = fx.open(&fx.cache(), false).unwrap();
        set_store_now(&mut repo);
        repo.set_fold_policy(policy);
        repo
    };
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s1");
    let b1 = fx.head().segments[0].name;
    let mut toggled = None;
    let mut repo = open(&fx);
    for _ in 0..2 {
        repo.load_chunk_index_under_lock().unwrap();
        toggle_an_entry(&mut repo, &mut toggled);
        repo.unchecked_writer().save_state().unwrap();
    }
    let head = fx.head();
    assert!(head.base.is_none(), "nothing old to fold: {head:?}");
    assert_eq!(head.segments.len(), 3);
    assert_eq!(cold_index(&fx, "cold1"), sorted_entries(repo.chunk_index()));
    drop(repo);

    age_segment(&fx, &b1);
    let mut repo = open(&fx);
    repo.load_chunk_index_under_lock().unwrap();
    toggle_an_entry(&mut repo, &mut toggled);
    repo.unchecked_writer().save_state().unwrap();
    let head = fx.head();
    assert!(head.base.is_some() && head.segments.is_empty(), "{head:?}");
    assert_eq!(cold_index(&fx, "cold2"), sorted_entries(repo.chunk_index()));
    drop(repo);

    // [old b0, young b1, m1] and a requested fold.
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s0");
    let b0 = fx.head().segments[0].name;
    age_segment(&fx, &b0);
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s1");
    let b1 = fx.head().segments[1].name;
    let mut toggled = None;
    let mut repo = open(&fx);
    repo.load_chunk_index_under_lock().unwrap();
    toggle_an_entry(&mut repo, &mut toggled);
    repo.unchecked_writer().save_state().unwrap();
    let m1 = fx.head().segments[2].name;
    repo.load_chunk_index_under_lock().unwrap();
    toggle_an_entry(&mut repo, &mut toggled);
    repo.request_index_fold();
    repo.unchecked_writer().save_state().unwrap();
    let head = fx.head();
    head.validate().unwrap();
    assert!(head.base.is_some(), "{head:?}");
    let names: Vec<ObjectName> = head.segments.iter().map(|s| s.name).collect();
    assert_eq!(names.len(), 3, "{head:?}");
    assert_eq!(names[..2], [b1, m1]);
    assert_eq!(head.segments[2].kind, SegmentKind::Maintenance);
    assert_eq!(head.retired, vec![b0]);
    assert!(fx.named_objects_exist(&head));
    assert_eq!(cold_index(&fx, "cold3"), sorted_entries(repo.chunk_index()));
    assert_eq!(
        sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index()),
        sorted_entries(repo.chunk_index())
    );
    // The second toggle put the removed entry back.
    assert!(toggled.is_none());
    drop(repo);
    fx.check_clean(&fx.config);
}

/// `check --repair`'s fold is full: young backup segments go into the base
/// too.
#[test]
fn a_full_fold_folds_young_segments() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    let before = fx.head();
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    commands::util::with_maintenance_lock(&mut repo, |repo| {
        repo.load_chunk_index_under_lock()?;
        repo.request_full_index_fold();
        repo.mark_index_dirty();
        repo.save_state()
    })
    .unwrap();
    let head = fx.head();
    assert!(head.base.is_some() && head.segments.is_empty(), "{head:?}");
    assert_eq!(head.retired, before.live_objects().collect::<Vec<_>>());
    assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
}

/// A partial fold reads the segments the local cache lacks: here every
/// cached segment file is removed after the index load; the fold fetches
/// them, writes the base from the first one, re-applies the kept one after
/// the head PUT and caches it again.
#[test]
fn a_partial_fold_fetches_what_the_cache_lacks() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    let before = fx.head();
    age_segment(&fx, &before.segments[0].name);
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    commands::util::with_maintenance_lock(&mut repo, |repo| {
        repo.load_chunk_index_under_lock()?;
        let dir = repo.cache_dir().unwrap();
        for e in std::fs::read_dir(&dir).unwrap() {
            let path = e.unwrap().path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(table::SEGMENT_PREFIX))
            {
                std::fs::remove_file(path).unwrap();
            }
        }
        repo.request_index_fold();
        repo.mark_index_dirty();
        repo.save_state()
    })
    .unwrap();
    let head = fx.head();
    assert!(head.base.is_some(), "{head:?}");
    assert_eq!(head.segments, before.segments[1..]);
    assert_eq!(head.retired, vec![before.segments[0].name]);
    let dir = repo.cache_dir().unwrap();
    assert!(table::read_cached_segment(&dir, &before.segments[1]).is_some());
    assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
}

/// A cached segment file of the right size whose bytes do not verify is
/// taken as present by the fold's presence check (metadata only); the fold
/// then reads it once, finds it bad and GETs the segment instead.
#[test]
fn a_partial_fold_refetches_a_damaged_cached_segment() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    let before = fx.head();
    age_segment(&fx, &before.segments[0].name);
    let mut repo = fx.open(&fx.cache(), false).unwrap();
    commands::util::with_maintenance_lock(&mut repo, |repo| {
        repo.load_chunk_index_under_lock()?;
        let dir = repo.cache_dir().unwrap();
        for r in &before.segments {
            let path = dir.join(format!("{}{}", table::SEGMENT_PREFIX, r.name.to_hex()));
            let len = std::fs::metadata(&path).unwrap().len();
            assert_eq!(len, r.size);
            std::fs::write(&path, vec![0u8; len as usize]).unwrap();
        }
        repo.request_index_fold();
        repo.mark_index_dirty();
        repo.save_state()
    })
    .unwrap();
    let head = fx.head();
    assert!(head.base.is_some(), "{head:?}");
    assert_eq!(head.segments, before.segments[1..]);
    assert_eq!(cold_index(&fx, "cold"), sorted_entries(repo.chunk_index()));
    assert_eq!(
        sorted_entries(repo.chunk_index()),
        sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index())
    );
}

/// Compact's soft fold counts only the foldable prefix: two young backup
/// segments at a soft limit of two stay; once they are old, compact folds
/// them and keeps the young third one after the new base.
#[test]
fn compact_soft_fold_keeps_young_segments() {
    let fx = fixture(RepoFormat::CURRENT);
    let compact = |fx: &Fixture| {
        let mut repo = fx.open(&fx.cache(), false).unwrap();
        let mut policy = crate::repo::index_write::FoldPolicy::default();
        policy.soft.segments = 2;
        repo.set_fold_policy(policy);
        commands::compact::run_on_repo(&mut repo, 0.0, None, false, &CancelToken::new()).unwrap();
    };
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    fx.write("b", "bravo");
    fx.backup(&fx.config, "s2");
    compact(&fx);
    let head = fx.head();
    assert!(head.base.is_none(), "{head:?}");
    assert_eq!(head.segments.len(), 2);

    age_segment(&fx, &head.segments[0].name);
    age_segment(&fx, &head.segments[1].name);
    fx.write("c", "charlie");
    fx.backup(&fx.config, "s3");
    let before = fx.head();
    compact(&fx);
    let head = fx.head();
    assert!(head.base.is_some(), "{head:?}");
    assert_eq!(head.segments[0], before.segments[2]);
    assert_eq!(
        head.retired,
        before.live_objects().take(2).collect::<Vec<_>>()
    );
    assert_eq!(
        cold_index(&fx, "cold"),
        sorted_entries(fx.open(&fx.cache(), true).unwrap().chunk_index())
    );
    fx.check_clean(&fx.config);
}

/// Fails every LIST of `index.d/` (the segments' ages).
struct FailIndexListing {
    inner: Box<dyn StorageBackend>,
}

impl InnerBackend for FailIndexListing {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for FailIndexListing;
    except [list_meta];

    fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        if prefix == INDEX_OBJECT_DIR {
            return Err(VykarError::Other("injected index.d/ listing failure".into()));
        }
        self.inner.list_meta(prefix)
    }
}

/// A failed `index.d/` listing never fails a backup's commit: the ages are
/// unknown, so young, and the folding commit writes a plain segment.
/// Compact's sweep, which deletes by those ages, refuses instead.
#[test]
fn a_failed_segment_listing_never_fails_a_backup() {
    let fx = fixture(RepoFormat::CURRENT);
    fx.write("a", "alpha");
    fx.backup(&fx.config, "s1");
    age_segment(&fx, &fx.head().segments[0].name);
    fx.write("b", "bravo");
    let wrap = |inner: Box<dyn StorageBackend>| -> Box<dyn StorageBackend> {
        Box::new(FailIndexListing { inner })
    };
    backup_with(&fx, "s2", Some(&wrap), Repo::request_index_fold);
    let head = fx.head();
    assert!(head.base.is_none(), "{head:?}");
    assert_eq!(head.segments.len(), 2);
    fx.check_clean(&fx.config);

    let mut repo = fx
        .open_with(wrap(fx.storage()), &fx.cache(), OpenOptions::new())
        .unwrap();
    let err = commands::compact::run_on_repo(&mut repo, 0.0, None, false, &CancelToken::new())
        .unwrap_err()
        .to_string();
    assert!(err.contains("injected index.d/ listing failure"), "{err}");
    assert_eq!(fx.head(), head, "nothing written");
}
