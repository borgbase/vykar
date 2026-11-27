//! `check --repair` re-indexes the chunks the index lost from the packs
//! that still describe them, and a compact refused for them says so and is
//! recorded until a compact marks again or a repair applies.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::helpers::{backup_single_source, latest_head_key, make_test_config, pass};
use crate::commands;
use crate::commands::check::{IndexRecovery, RepairAction, RepairMode, RepairResult};
use crate::commands::compact::CompactStats;
use crate::config::VykarConfig;
use crate::repo::control::CancelToken;
use crate::repo::{OpenOptions, Repo};
use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

struct Fixture {
    tmp: tempfile::TempDir,
    repo_dir: PathBuf,
    source: PathBuf,
    config: VykarConfig,
}

/// A repository on a local directory, with its own cache dir.
/// `small_packs`: every chunk in a pack of its own.
fn fixture(small_packs: bool) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source = tmp.path().join("src");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source).unwrap();
    let mut config = make_test_config(&repo_dir);
    if small_packs {
        config.repository.min_pack_size = 1;
        config.repository.max_pack_size = 4 * 1024;
    }
    config.cache_dir = Some(tmp.path().join("cache").to_string_lossy().into_owned());
    commands::init::run(&config, pass(&config)).unwrap();
    Fixture {
        tmp,
        repo_dir,
        source,
        config,
    }
}

impl Fixture {
    /// The same repository seen from another machine (own cache dir).
    fn client(&self, name: &str) -> VykarConfig {
        let mut config = self.config.clone();
        config.cache_dir = Some(self.tmp.path().join(name).to_string_lossy().into_owned());
        config
    }

    fn storage(&self) -> Box<dyn StorageBackend> {
        Box::new(LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap())
    }

    fn open(&self, config: &VykarConfig) -> Repo {
        let cache = PathBuf::from(config.cache_dir.as_ref().unwrap());
        Repo::open(
            self.storage(),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(cache),
            OpenOptions::new(),
        )
        .unwrap()
    }

    /// The packs the index of `config`'s client names.
    fn indexed_packs(&self, config: &VykarConfig) -> HashSet<PackId> {
        let mut repo = self.open(config);
        repo.load_chunk_index_uncached().unwrap();
        repo.chunk_index().iter().map(|(_, e)| e.pack_id).collect()
    }

    fn write(&self, name: &str, content: &[u8]) {
        std::fs::write(self.source.join(name), content).unwrap();
    }

    /// Back up as `name`; returns the packs the backup added.
    fn backup(&self, name: &str) -> HashSet<PackId> {
        let before = self.indexed_packs(&self.config);
        backup_single_source(&self.config, &self.source, "src", name);
        let after = self.indexed_packs(&self.config);
        after.difference(&before).copied().collect()
    }

    /// A lost index update: remove the latest head, which the last backup
    /// wrote. Its packs and snapshot stay; no head names them.
    fn lose_latest_head(&self) {
        std::fs::remove_file(self.repo_dir.join(latest_head_key(&*self.storage()))).unwrap();
    }

    /// Drop every entry of `packs` from the index with a maintenance
    /// segment (the index keeps knowing their trailer lengths).
    fn drop_entries_of(&self, packs: &HashSet<PackId>) {
        let mut repo = self.open(&self.config);
        commands::util::with_exclusive_session(&mut repo, |repo| {
            repo.load_latest_chunk_index()?;
            for p in packs {
                assert!(repo.chunk_index_mut().remove_by_pack(p) > 0);
            }
            repo.save_state()
        })
        .unwrap();
    }

    fn compact(&self, config: &VykarConfig) -> Result<CompactStats> {
        commands::compact::run(
            config,
            pass(config),
            config.compact.threshold,
            None,
            false,
            &CancelToken::new(),
        )
    }

    fn repair(&self, config: &VykarConfig, mode: RepairMode) -> Result<RepairResult> {
        commands::check::run_with_repair(
            config,
            pass(config),
            false,
            mode,
            None,
            &CancelToken::new(),
        )
    }

    fn check_clean(&self, config: &VykarConfig) {
        let result = commands::check::run(config, pass(config), true, false).unwrap();
        assert!(result.errors.is_empty(), "{:?}", result.errors);
    }

    /// Restore `snapshot` with `config`; returns the restored files by name.
    fn restore(&self, config: &VykarConfig, snapshot: &str) -> BTreeMap<String, Vec<u8>> {
        let dest = self
            .tmp
            .path()
            .join(format!("restore-{snapshot}-{}", rand::random::<u32>()));
        commands::restore::run(
            config,
            pass(config),
            snapshot,
            dest.to_str().unwrap(),
            None,
            false,
            true,
            &CancelToken::new(),
        )
        .unwrap();
        let mut files = BTreeMap::new();
        collect(&dest, &mut |path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            files.insert(name, std::fs::read(path).unwrap());
        });
        files
    }

    /// Every file of the repository and its bytes (lock files aside).
    fn repo_files(&self) -> BTreeMap<String, Vec<u8>> {
        let mut files = BTreeMap::new();
        collect(&self.repo_dir, &mut |path| {
            let rel = path.strip_prefix(&self.repo_dir).unwrap().to_string_lossy();
            if !rel.starts_with("locks") {
                files.insert(rel.into_owned(), std::fs::read(path).unwrap());
            }
        });
        files
    }

    fn info(&self, config: &VykarConfig) -> commands::info::InfoStats {
        commands::info::run(config, pass(config)).unwrap()
    }
}

fn collect(dir: &Path, f: &mut dyn FnMut(&Path)) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect(&path, f);
        } else {
            f(&path);
        }
    }
}

/// Distinct, incompressible content.
fn noise(seed: u8, len: usize) -> Vec<u8> {
    let mut state = u64::from(seed).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn reindexed(actions: &[RepairAction]) -> Option<(Vec<PackId>, usize)> {
    actions.iter().find_map(|a| match a {
        RepairAction::ReindexChunks {
            pack_ids,
            chunk_count,
        } => Some((pack_ids.clone(), *chunk_count)),
        _ => None,
    })
}

fn removes_a_snapshot(actions: &[RepairAction]) -> bool {
    actions
        .iter()
        .any(|a| matches!(a, RepairAction::RemoveDanglingSnapshot { .. }))
}

fn sorted(packs: &HashSet<PackId>) -> Vec<PackId> {
    let mut v: Vec<PackId> = packs.iter().copied().collect();
    v.sort_unstable_by_key(|p| *p.as_bytes());
    v
}

/// A lost update (the head naming s2's packs is gone) is re-indexed from
/// the packs' trailers, which no index on this machine knew: the plan says
/// so and loses nothing, and after the repair s2 restores and compact runs.
#[test]
fn a_lost_update_is_reindexed() {
    let fx = fixture(false);
    fx.write("a", b"alpha");
    fx.backup("s1");
    fx.write("b", &noise(1, 5000));
    let s2_packs = fx.backup("s2");
    assert!(!s2_packs.is_empty());
    fx.lose_latest_head();
    let cold = fx.client("cold");

    let err = fx.compact(&cold).unwrap_err();
    let msg = err.to_string();
    assert!(matches!(err, VykarError::MarkRefused(_)), "{msg}");
    assert!(msg.contains("names tree chunk"), "{msg}");
    assert!(msg.contains("vykar check --repair"), "{msg}");

    let before = fx.repo_files();
    let shown = fx.repair(&cold, RepairMode::PlanOnly).unwrap();
    assert_eq!(
        fx.repo_files(),
        before,
        "a repair plan changed the repository"
    );
    let (packs, chunks) = reindexed(&shown.plan.actions).expect("a re-index");
    assert!(chunks > 0);
    assert_eq!(packs, sorted(&s2_packs));
    assert!(
        matches!(
            shown.plan.actions.as_slice(),
            [
                RepairAction::ReindexChunks { .. },
                RepairAction::RemoveUnreferencedEntries
            ]
        ),
        "{:?}",
        shown.plan.actions
    );
    assert!(!shown.plan.has_data_loss);
    assert_eq!(shown.plan.index_recovery, IndexRecovery::Complete);
    assert!(
        !shown.check_result.errors.is_empty(),
        "the result shows what was wrong"
    );

    let applied = fx.repair(&cold, RepairMode::Apply).unwrap();
    assert!(
        reindexed(&applied.applied).is_some(),
        "{:?}",
        applied.applied
    );
    assert!(!removes_a_snapshot(&applied.applied));
    assert!(
        applied.repair_errors.is_empty(),
        "{:?}",
        applied.repair_errors
    );
    let head = fx.open(&fx.client("look")).index_head().cloned().unwrap();
    assert!(head.base.is_some() && head.segments.is_empty(), "{head:?}");
    assert_eq!(head.sweep.generation, head.generation, "{head:?}");

    fx.compact(&cold).unwrap();
    let other = fx.client("other");
    assert_eq!(fx.restore(&other, "s2")["b"], noise(1, 5000));
    assert_eq!(fx.restore(&other, "s1")["a"], b"alpha");
    fx.check_clean(&cold);
    fx.check_clean(&fx.client("third"));
}

/// The lost update's packs are described by pack maps only (their trailers
/// are not read), and the loaded index never knew them: the repair writes
/// the fold and a cold client restores s2.
#[test]
fn a_lost_update_is_reindexed_from_pack_maps() {
    let fx = fixture(false);
    fx.write("a", b"alpha");
    fx.backup("s1");
    fx.write("b", &noise(2, 5000));
    let s2_packs = fx.backup("s2");
    let stats =
        commands::pack_maps::run(&fx.config, pass(&fx.config), &CancelToken::new()).unwrap();
    assert!(stats.packs_mapped >= s2_packs.len(), "{stats:?}");
    fx.lose_latest_head();
    let cold = fx.client("cold");

    let applied = fx.repair(&cold, RepairMode::Apply).unwrap();
    let (packs, _) = reindexed(&applied.applied).expect("a re-index");
    assert_eq!(packs, sorted(&s2_packs));
    assert!(!removes_a_snapshot(&applied.applied));
    assert!(
        applied.repair_errors.is_empty(),
        "{:?}",
        applied.repair_errors
    );

    let head = fx.open(&fx.client("look")).index_head().cloned().unwrap();
    assert!(head.base.is_some() && head.segments.is_empty(), "{head:?}");
    assert_eq!(fx.restore(&fx.client("restore"), "s2")["b"], noise(2, 5000));
    fx.check_clean(&fx.client("check"));
}

/// s2 adds b and c, each in packs of their own; the update is lost and c's
/// pack is gone too: the repair re-indexes the trees and b, and drops only
/// c from s2.
#[test]
fn a_partly_recoverable_snapshot_keeps_what_the_packs_still_hold() {
    let fx = fixture(true);
    fx.write("a", &noise(3, 3000));
    fx.backup("s1");
    fx.write("b", &noise(4, 3000));
    fx.write("c", &noise(5, 3000));
    let s2_packs = fx.backup("s2");

    // c's data pack, while the index still names it.
    let c_pack = {
        let mut repo = fx.open(&fx.config);
        repo.load_chunk_index_uncached().unwrap();
        let items =
            commands::list::load_snapshot_items(&mut repo, "s2", &CancelToken::new()).unwrap();
        let c = items.iter().find(|i| i.path.ends_with('c')).unwrap();
        let packs: HashSet<PackId> = c
            .chunks
            .iter()
            .map(|r| repo.chunk_index().get(&r.id).unwrap().pack_id)
            .collect();
        assert_eq!(packs.len(), 1, "c in one pack");
        *packs.iter().next().unwrap()
    };
    fx.lose_latest_head();
    std::fs::remove_file(fx.repo_dir.join(c_pack.storage_key())).unwrap();
    let cold = fx.client("cold");

    let shown = fx.repair(&cold, RepairMode::PlanOnly).unwrap();
    let (packs, _) = reindexed(&shown.plan.actions).expect("a re-index");
    let mut expected = s2_packs.clone();
    expected.remove(&c_pack);
    assert_eq!(packs, sorted(&expected));
    let dropped = shown
        .plan
        .actions
        .iter()
        .find_map(|a| match a {
            RepairAction::DropItemsFromSnapshot {
                snapshot_name,
                dropped_paths,
                ..
            } => Some((snapshot_name.clone(), dropped_paths.clone())),
            _ => None,
        })
        .expect("an item drop");
    assert_eq!(dropped.0, "s2");
    assert_eq!(dropped.1.len(), 1);
    assert!(dropped.1[0].ends_with('c'), "{dropped:?}");
    assert!(shown.plan.has_data_loss);
    assert!(!removes_a_snapshot(&shown.plan.actions));

    fx.repair(&cold, RepairMode::Apply).unwrap();
    let other = fx.client("other");
    let files = fx.restore(&other, "s2");
    assert_eq!(files.get("b"), Some(&noise(4, 3000)));
    assert!(!files.contains_key("c"), "{:?}", files.keys());
    fx.check_clean(&other);
}

/// A compact refused for lost entries shows in `info` on this machine until
/// a repair applies; a second refusal keeps the first time.
#[test]
fn a_refused_compact_is_reported_until_the_repair() {
    let fx = fixture(false);
    fx.write("a", b"alpha");
    fx.backup("s1");
    fx.write("b", &noise(6, 5000));
    let s2_packs = fx.backup("s2");
    fx.drop_entries_of(&s2_packs);
    assert_eq!(fx.info(&fx.config).compact_refused, None);

    assert!(matches!(
        fx.compact(&fx.config),
        Err(VykarError::MarkRefused(_))
    ));
    let first = fx.info(&fx.config).compact_refused.expect("recorded");
    assert!(first.detail.contains("check --repair"), "{}", first.detail);
    assert_eq!(fx.info(&fx.client("elsewhere")).compact_refused, None);

    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(fx.compact(&fx.config).is_err());
    let second = fx.info(&fx.config).compact_refused.expect("recorded");
    assert_eq!(second.first, first.first);
    assert!(second.last > first.last);

    // A plan clears nothing; the applied repair does.
    fx.repair(&fx.config, RepairMode::PlanOnly).unwrap();
    assert!(fx.info(&fx.config).compact_refused.is_some());
    let applied = fx.repair(&fx.config, RepairMode::Apply).unwrap();
    assert!(
        reindexed(&applied.applied).is_some(),
        "{:?}",
        applied.applied
    );
    assert_eq!(fx.info(&fx.config).compact_refused, None);
    fx.compact(&fx.config).unwrap();
    assert_eq!(fx.restore(&fx.client("other"), "s2")["b"], noise(6, 5000));
}

/// A compact that marks clears the record; a refusal for another cause is
/// not recorded and keeps its advice.
#[test]
fn a_compact_that_marks_clears_the_record() {
    let fx = fixture(false);
    fx.write("a", b"alpha");
    fx.backup("s1");
    let dir = fx.open(&fx.config).cache_dir();
    commands::compact_refusal::record(dir.as_deref(), "an earlier refusal");
    assert!(fx.info(&fx.config).compact_refused.is_some());
    let stats = fx.compact(&fx.config).unwrap();
    assert!(stats.sweep_skipped.is_none());
    assert_eq!(fx.info(&fx.config).compact_refused, None);

    std::fs::write(fx.repo_dir.join("snapshots/not-an-id"), b"x").unwrap();
    let err = fx.compact(&fx.config).unwrap_err();
    let msg = err.to_string();
    assert!(matches!(err, VykarError::MarkRefused(_)), "{msg}");
    assert!(msg.ends_with("Run `vykar check` for details"), "{msg}");
    assert_eq!(fx.info(&fx.config).compact_refused, None);
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

    fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
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

    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        None
    }
}

/// A lookup that cannot read storage fails the repair before anything is
/// written; it never falls back to removing the snapshot.
#[test]
fn a_failed_lookup_changes_nothing() {
    let fx = fixture(false);
    fx.write("a", b"alpha");
    fx.backup("s1");
    fx.write("b", &noise(7, 5000));
    fx.backup("s2");
    fx.lose_latest_head();
    let before = fx.repo_files();
    for mode in [RepairMode::PlanOnly, RepairMode::Apply] {
        let storage = FailList {
            inner: Arc::from(fx.storage()),
            prefix: "packmaps/",
        };
        let cold = fx.client("cold");
        let cache = PathBuf::from(cold.cache_dir.as_ref().unwrap());
        let mut repo = Repo::open(
            Box::new(storage),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(cache),
            commands::check::repair_open_options(mode),
        )
        .unwrap();
        let err = commands::check::run_with_repair_on_repo(
            &mut repo,
            &cold,
            false,
            mode,
            None,
            &CancelToken::new(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("re-indexing"), "{err}");
        assert!(err.contains("injected list failure"), "{err}");
        assert!(err.contains("Nothing was changed"), "{err}");
        assert_eq!(fx.repo_files(), before, "{mode:?}");
    }
}

/// Fails every read of the packs in `keys` with an I/O error.
struct FailPackReads {
    inner: Arc<dyn StorageBackend>,
    keys: HashSet<String>,
}

impl FailPackReads {
    fn check(&self, key: &str) -> Result<()> {
        if self.keys.contains(key) {
            return Err(VykarError::Io(std::io::Error::other(format!(
                "injected read failure: {key}"
            ))));
        }
        Ok(())
    }
}

impl InnerBackend for FailPackReads {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for FailPackReads;
    except [get, get_range, get_range_into, open_range, server_ops];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.check(key)?;
        self.inner.get(key)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.check(key)?;
        self.inner.get_range(key, offset, length)
    }

    fn get_range_into(&self, key: &str, offset: u64, length: u64, buf: &mut Vec<u8>) -> Result<bool> {
        self.check(key)?;
        self.inner.get_range_into(key, offset, length, buf)
    }

    fn open_range<'a>(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &'a mut Vec<u8>,
    ) -> Result<Option<Box<dyn vykar_storage::RangeReader + 'a>>> {
        vykar_storage::buffered_range(self, key, offset, length, buf)
    }

    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        None
    }
}

/// The trees just re-indexed (from pack maps, so the lookup reads no pack)
/// cannot be read: the repair fails before anything is written instead of
/// reporting their data missing, and a later repair re-indexes them.
#[test]
fn a_failed_read_of_a_reindexed_tree_changes_nothing() {
    let fx = fixture(false);
    fx.write("a", b"alpha");
    fx.backup("s1");
    fx.write("b", &noise(9, 5000));
    let s2_packs = fx.backup("s2");
    commands::pack_maps::run(&fx.config, pass(&fx.config), &CancelToken::new()).unwrap();
    fx.lose_latest_head();
    let before = fx.repo_files();
    for mode in [RepairMode::PlanOnly, RepairMode::Apply] {
        let storage = FailPackReads {
            inner: Arc::from(fx.storage()),
            keys: s2_packs.iter().map(PackId::storage_key).collect(),
        };
        let cold = fx.client("cold");
        let cache = PathBuf::from(cold.cache_dir.as_ref().unwrap());
        let mut repo = Repo::open(
            Box::new(storage),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(cache),
            commands::check::repair_open_options(mode),
        )
        .unwrap();
        let err = commands::check::run_with_repair_on_repo(
            &mut repo,
            &cold,
            false,
            mode,
            None,
            &CancelToken::new(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("re-indexing"), "{err}");
        assert!(err.contains("injected read failure"), "{err}");
        assert!(err.contains("Nothing was changed"), "{err}");
        assert_eq!(fx.repo_files(), before, "{mode:?}");
    }

    let cold = fx.client("cold");
    let applied = fx.repair(&cold, RepairMode::Apply).unwrap();
    assert!(
        reindexed(&applied.applied).is_some(),
        "{:?}",
        applied.applied
    );
    assert!(!removes_a_snapshot(&applied.applied));
    assert_eq!(fx.restore(&fx.client("other"), "s2")["b"], noise(9, 5000));
}

/// The shown plan re-indexed and lost nothing; the packs are gone by the
/// time it is applied: the plan rebuilt in the repair's exclusive session removes s2, which
/// the shown one did not, so the repair refuses.
#[test]
fn apply_shown_refuses_when_the_packs_vanished() {
    let fx = fixture(false);
    fx.write("a", b"alpha");
    fx.backup("s1");
    fx.write("b", &noise(8, 5000));
    let s2_packs = fx.backup("s2");
    fx.lose_latest_head();
    let cold = fx.client("cold");
    let shown = fx.repair(&cold, RepairMode::PlanOnly).unwrap();
    assert!(reindexed(&shown.plan.actions).is_some());
    assert!(!shown.plan.has_data_loss);

    for p in &s2_packs {
        std::fs::remove_file(fx.repo_dir.join(p.storage_key())).unwrap();
    }
    let before = fx.repo_files();
    let err = fx
        .repair(
            &cold,
            RepairMode::ApplyShown {
                has_data_loss: false,
                index_recovery: shown.plan.index_recovery,
            },
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("removes data"), "{err}");
    assert_eq!(fx.repo_files(), before);
}
