//! `vykar upgrade`: monolithic index (v2/v3) to segmented (v4/v5) in place.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::helpers::{backup_single_source, is_base_put, is_head_put, make_test_config, FaultyPut};
use crate::commands;
use crate::commands::check::{IndexRecovery, RepairAction, RepairMode};
use crate::config::VykarConfig;
use crate::index::ChunkIndex;
use crate::repo::index_store::{self, INDEX_OBJECT_DIR};
use crate::repo::{
    EncryptionMode, OpenOptions, RepoConfig, RepoFormat, Repository, UpgradeOutcome,
};
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

    fn storage(&self) -> Box<dyn StorageBackend> {
        Box::new(LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap())
    }

    fn open(&self, cache: &Path, with_index: bool) -> Result<Repository> {
        let opts = if with_index {
            OpenOptions::new().with_index()
        } else {
            OpenOptions::new()
        };
        Repository::open(self.storage(), None, Some(cache.to_path_buf()), opts)
    }

    fn version(&self) -> u32 {
        let config: RepoConfig =
            rmp_serde::from_slice(&std::fs::read(self.repo_dir.join("config")).unwrap()).unwrap();
        config.version
    }

    fn set_version(&self, version: u32) {
        let path = self.repo_dir.join("config");
        let mut config: RepoConfig = rmp_serde::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config.version = version;
        std::fs::write(path, rmp_serde::to_vec(&config).unwrap()).unwrap();
    }

    fn index_objects(&self) -> usize {
        std::fs::read_dir(self.repo_dir.join(INDEX_OBJECT_DIR))
            .map(|d| d.count())
            .unwrap_or(0)
    }

    fn write(&self, name: &str, content: &str) {
        std::fs::write(self.source.join(name), content).unwrap();
    }

    fn backup(&self, name: &str) {
        backup_single_source(&self.config, &self.source, "src", name);
    }

    fn check_clean(&self) {
        let result = commands::check::run(&self.config, None, true, false).unwrap();
        assert!(result.errors.is_empty(), "{:?}", result.errors);
    }

    /// The configuration of a client with its own cache directory.
    fn cold(&self, name: &str) -> VykarConfig {
        let mut config = self.config.clone();
        config.cache_dir = Some(self.tmp.path().join(name).to_string_lossy().into_owned());
        config
    }

    fn snapshot_names(&self) -> Vec<String> {
        let repo = self.open(&self.cache(), false).unwrap();
        let mut names: Vec<String> = repo
            .manifest()
            .snapshots
            .iter()
            .map(|s| s.name.clone())
            .collect();
        names.sort();
        names
    }

    fn truncate_head(&self) {
        let path = self.repo_dir.join("index");
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
    }

    /// The index as a client with no local cache builds it.
    fn cold_index(&self, name: &str) -> Vec<String> {
        sorted_entries(
            self.open(&self.tmp.path().join(name), true)
                .unwrap()
                .chunk_index(),
        )
    }
}

fn sorted_entries(index: &ChunkIndex) -> Vec<String> {
    let mut v: Vec<String> = index
        .iter()
        .map(|(id, e)| format!("{id:?} {e:?}"))
        .collect();
    v.sort();
    v
}

/// v2 → v4 and v3 → v5: one base, no segments, the same index, `index.gen`
/// and the local blob copy gone; later backups append segments, and a
/// second upgrade does nothing.
#[test]
fn upgrade_moves_the_index_to_a_base() {
    for (from, to) in [
        (RepoFormat::V2, RepoFormat::V4),
        (RepoFormat::V3, RepoFormat::V5),
    ] {
        let fx = fixture(from);
        fx.write("a", "alpha");
        fx.backup("s1");
        fx.write("b", "bravo");
        fx.backup("s2");
        let before = fx.cold_index("cold-before");
        let blob = crate::index::dedup_cache::index_blob_cache_path(
            &fx.open(&fx.cache(), false).unwrap().config.id,
            Some(&fx.cache()),
        )
        .unwrap();
        assert!(blob.exists(), "{from:?}: the backup left a local blob copy");

        let outcome = commands::upgrade::run(&fx.config, None).unwrap();
        let UpgradeOutcome::Upgraded {
            from: f,
            to: t,
            entries,
            base_bytes,
            resumed,
        } = outcome
        else {
            panic!("{from:?}: {outcome:?}");
        };
        assert_eq!((f, t), (from.version(), to.version()));
        assert_eq!(entries as usize, before.len());
        assert!(base_bytes > 0 && !resumed);
        assert_eq!(fx.version(), to.version());
        assert!(!fx.repo_dir.join("index.gen").exists());
        assert!(!blob.exists());
        assert_eq!(fx.index_objects(), 1);

        let repo = fx.open(&fx.cache(), true).unwrap();
        let head = repo.index_head().unwrap().clone();
        assert!(head.base.is_some() && head.segments.is_empty() && head.retired.is_empty());
        assert_eq!(sorted_entries(repo.chunk_index()), before);
        drop(repo);
        assert_eq!(fx.cold_index("cold-after"), before);
        fx.check_clean();

        fx.write("c", "charlie");
        fx.backup("s3");
        let repo = fx.open(&fx.cache(), false).unwrap();
        assert_eq!(repo.index_head().unwrap().segments.len(), 1);
        drop(repo);
        fx.check_clean();

        assert_eq!(
            commands::upgrade::run(&fx.config, None).unwrap(),
            UpgradeOutcome::AlreadyCurrent {
                version: to.version()
            }
        );
    }
}

/// An empty repository gets the empty head, as `init` writes it.
#[test]
fn upgrade_of_an_empty_repository() {
    let fx = fixture(RepoFormat::V3);
    let outcome = commands::upgrade::run(&fx.config, None).unwrap();
    assert!(
        matches!(
            outcome,
            UpgradeOutcome::Upgraded {
                entries: 0,
                base_bytes: 0,
                ..
            }
        ),
        "{outcome:?}"
    );
    let repo = fx.open(&fx.cache(), false).unwrap();
    assert!(repo.index_head().unwrap().base.is_none());
    drop(repo);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.check_clean();
}

/// Interrupted after the head PUT: `config` still says v3. Monolithic index
/// reads name `vykar upgrade`, which verifies the chain and completes.
#[test]
fn interrupted_upgrade_resumes() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    let before = fx.cold_index("cold-before");
    commands::upgrade::run(&fx.config, None).unwrap();
    fx.set_version(3);
    std::fs::write(fx.repo_dir.join("index.gen"), 9u64.to_le_bytes()).unwrap();

    let err = fx
        .open(&fx.tmp.path().join("other"), true)
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("vykar upgrade"), "{err}");

    // A stray object from a crashed write is not named by the head.
    std::fs::write(
        fx.repo_dir.join(INDEX_OBJECT_DIR).join("a".repeat(64)),
        b"x",
    )
    .unwrap();
    let outcome = commands::upgrade::run(&fx.config, None).unwrap();
    assert!(
        matches!(
            outcome,
            UpgradeOutcome::Upgraded {
                from: 3,
                to: 5,
                resumed: true,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(fx.version(), 5);
    assert!(!fx.repo_dir.join("index.gen").exists());
    assert_eq!(fx.cold_index("cold-after"), before);
    fx.check_clean();
}

/// Refuses to overwrite `config`, as an append-only server does.
struct NoConfigOverwrite {
    inner: Arc<dyn StorageBackend>,
}

impl InnerBackend for NoConfigOverwrite {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for NoConfigOverwrite;
    except [put, put_pack, begin_staged_put, profile];

    fn profile(&self) -> vykar_storage::BackendProfile {
        self.inner.profile()
    }

    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        self.inner.put_pack(key, data)
    }

    fn begin_staged_put(&self, prefix: &str) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        self.inner.begin_staged_put(prefix)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if key == "config" {
            return Err(VykarError::Other("append-only: cannot overwrite immutable object".into()));
        }
        self.inner.put(key, data)
    }
}

/// The pre-flight fails before anything is written.
#[test]
fn upgrade_without_config_overwrite_changes_nothing() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    let index = std::fs::read(fx.repo_dir.join("index")).unwrap();
    let storage = Box::new(NoConfigOverwrite {
        inner: Arc::from(fx.storage()),
    });
    let mut repo = Repository::open(storage, None, Some(fx.cache()), OpenOptions::new()).unwrap();
    let err = commands::util::with_maintenance_lock(&mut repo, |repo| repo.upgrade_index_layout())
        .unwrap_err()
        .to_string();
    assert!(err.contains("append-only"), "{err}");
    assert_eq!(fx.version(), 3);
    assert_eq!(std::fs::read(fx.repo_dir.join("index")).unwrap(), index);
    assert_eq!(fx.index_objects(), 0);
    fx.check_clean();
}

/// Debris an earlier attempt left in `index.d/`.
fn write_debris(fx: &Fixture) -> PathBuf {
    let dir = fx.repo_dir.join(INDEX_OBJECT_DIR);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("a".repeat(64));
    std::fs::write(&path, b"debris").unwrap();
    path
}

/// Upgrades through a `FaultyPut` that fails the PUTs `fail` selects.
fn upgrade_through(
    fx: &Fixture,
    fail: impl Fn(&str, &[u8]) -> bool + Send + Sync + 'static,
) -> (Result<UpgradeOutcome>, Vec<String>) {
    let ops = Arc::new(Mutex::new(Vec::new()));
    let storage = Box::new(FaultyPut {
        inner: Arc::from(fx.storage()),
        fail: Box::new(fail),
        ops: Arc::clone(&ops),
    });
    let mut repo = Repository::open(storage, None, Some(fx.cache()), OpenOptions::new()).unwrap();
    let result =
        commands::util::with_maintenance_lock(&mut repo, |repo| repo.upgrade_index_layout());
    drop(repo);
    let ops = ops.lock().unwrap().clone();
    (result, ops)
}

/// Debris of an earlier attempt is deleted only after the head PUT, for an
/// empty and a populated index.
#[test]
fn upgrade_deletes_debris_only_after_the_head() {
    for populated in [false, true] {
        let fx = fixture(RepoFormat::V3);
        if populated {
            fx.write("a", "alpha");
            fx.backup("s1");
        }
        let debris = write_debris(&fx);
        let (result, ops) = upgrade_through(&fx, |_, _| false);
        result.unwrap();
        let delete = format!("delete {INDEX_OBJECT_DIR}{}", "a".repeat(64));
        let deleted = ops.iter().position(|o| *o == delete);
        let head_put = ops.iter().rposition(|o| o == "put index");
        assert!(
            deleted.is_some() && head_put.is_some() && deleted > head_put,
            "{populated}: {ops:?}"
        );
        assert!(!debris.exists(), "{populated}");
        assert_eq!(fx.index_objects(), usize::from(populated));
        assert_eq!(fx.version(), 5);
    }
}

/// An upgrade that fails at the base or head PUT leaves the legacy index,
/// the version and the debris as they were; a retry completes.
#[test]
fn upgrade_failing_before_the_head_keeps_the_legacy_index() {
    type Fail = fn(&str, &[u8]) -> bool;
    for (name, fail) in [("base", is_base_put as Fail), ("head", is_head_put as Fail)] {
        let fx = fixture(RepoFormat::V3);
        fx.write("a", "alpha");
        fx.backup("s1");
        let before = fx.cold_index(&format!("cold-before-{name}"));
        let debris = write_debris(&fx);
        let index_before = std::fs::read(fx.repo_dir.join("index")).unwrap();

        let (result, ops) = upgrade_through(&fx, fail);
        let err = result.err().unwrap().to_string();
        assert!(err.contains("injected failure"), "{name}: {err} {ops:?}");
        assert_eq!(
            std::fs::read(fx.repo_dir.join("index")).unwrap(),
            index_before,
            "{name}"
        );
        assert_eq!(fx.version(), 3, "{name}");
        assert!(debris.exists(), "{name}");
        fx.open(&fx.tmp.path().join(format!("v3-open-{name}")), true)
            .unwrap();
        fx.check_clean();

        commands::upgrade::run(&fx.config, None).unwrap();
        assert_eq!(fx.index_objects(), 1, "{name}");
        assert_eq!(fx.cold_index(&format!("after-{name}")), before, "{name}");
    }
}

fn repair(config: &VykarConfig, mode: RepairMode) -> Result<commands::check::RepairResult> {
    commands::check::run_with_repair(config, None, false, mode, None, None)
}

fn apply_shown(config: &VykarConfig, plan: &commands::check::RepairPlan) -> Result<()> {
    repair(
        config,
        RepairMode::ApplyShown {
            has_data_loss: plan.has_data_loss,
            index_recovery: plan.index_recovery,
        },
    )
    .map(|_| ())
}

fn resumed(outcome: &UpgradeOutcome) -> bool {
    matches!(outcome, UpgradeOutcome::Upgraded { resumed: true, .. })
}

/// An interrupted upgrade whose chain lost a segment: `vykar upgrade` names
/// `check --repair`, which repairs the segmented index (dropping the snapshot
/// that needs the segment) and leaves `config` at v3; `vykar upgrade` then
/// completes.
#[test]
fn interrupted_upgrade_with_a_broken_chain_is_repaired_then_completed() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.write("b", "bravo");
    fx.backup("s2");
    commands::upgrade::run(&fx.config, None).unwrap();
    fx.write("c", "charlie");
    fx.backup("s3");
    let head = fx
        .open(&fx.cache(), false)
        .unwrap()
        .index_head()
        .cloned()
        .unwrap();
    fx.set_version(3);
    let lost = head.segments.last().unwrap().name;
    std::fs::remove_file(fx.repo_dir.join(INDEX_OBJECT_DIR).join(lost.to_hex())).unwrap();

    let err = commands::upgrade::run(&fx.config, None)
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("check --repair"), "{err}");
    assert!(err.contains("`vykar upgrade` again"), "{err}");
    assert_eq!(fx.version(), 3);

    let cold = fx.cold("cold");
    let plan = repair(&cold, RepairMode::PlanOnly).unwrap().plan;
    assert_eq!(
        plan.index_recovery,
        IndexRecovery::PartialChain {
            base_lost: false,
            applied: 0,
            total: 1
        }
    );
    assert!(plan.has_data_loss, "{:?}", plan.actions);
    let removes_s3 = |a: &RepairAction| matches!(a, RepairAction::RemoveDanglingSnapshot { snapshot_name, .. } if snapshot_name == "s3");
    assert!(plan.actions.iter().any(removes_s3), "{:?}", plan.actions);
    let names = format!("{:?}", plan.actions);
    assert!(
        !names.contains("\"s1\"") && !names.contains("\"s2\""),
        "{names}"
    );
    apply_shown(&cold, &plan).unwrap();

    assert_eq!(fx.version(), 3);
    let bytes = std::fs::read(fx.repo_dir.join("index")).unwrap();
    assert_eq!(bytes[0], 9);
    let crypto = vykar_crypto::PlaintextEngine::new(crate::testutil::test_chunk_hasher());
    let head = index_store::decode_head(&bytes, &crypto).unwrap();
    assert!(head.base.is_some() && head.segments.is_empty());
    assert_eq!(fx.snapshot_names(), ["s1", "s2"]);

    let outcome = commands::upgrade::run(&fx.config, None).unwrap();
    assert!(
        matches!(
            outcome,
            UpgradeOutcome::Upgraded {
                from: 3,
                to: 5,
                resumed: true,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(fx.version(), 5);
    assert!(!fx.repo_dir.join("index.gen").exists());
    fx.check_clean();
    assert_eq!(fx.snapshot_names(), ["s1", "s2"]);
}

/// An interrupted upgrade whose head is damaged: rebuilt from this
/// machine's cache, or on another machine from the upgrade's base.
#[test]
fn interrupted_upgrade_with_a_corrupt_head_is_repaired_then_completed() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    let before = fx.cold_index("cold-before");
    commands::upgrade::run(&fx.config, None).unwrap();
    fx.set_version(3);
    fx.truncate_head();

    let err = commands::upgrade::run(&fx.config, None)
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("cannot be read"), "{err}");
    assert!(err.contains("check --repair"), "{err}");
    assert!(err.contains("`vykar upgrade` again"), "{err}");

    let warm = repair(&fx.config, RepairMode::PlanOnly).unwrap().plan;
    assert!(
        matches!(
            warm.index_recovery,
            IndexRecovery::HeadUnreadable {
                from_local_cache: true,
                ..
            }
        ),
        "{:?}",
        warm.index_recovery
    );

    let cold = fx.cold("cold");
    let plan = repair(&cold, RepairMode::PlanOnly).unwrap().plan;
    assert_eq!(
        plan.index_recovery,
        IndexRecovery::HeadUnreadable {
            from_local_cache: false,
            bases: 1,
            applied: 0,
            unplaced: 0,
            damaged: 0
        }
    );
    assert!(!plan.has_data_loss, "{:?}", plan.actions);
    apply_shown(&cold, &plan).unwrap();
    assert_eq!(fx.version(), 3);

    assert!(resumed(&commands::upgrade::run(&fx.config, None).unwrap()));
    assert_eq!(fx.cold_index("cold-after"), before);
    fx.check_clean();
}

/// Repair of a healthy interrupted upgrade runs on the segmented index and
/// leaves the upgrade to `vykar upgrade`.
#[test]
fn repair_of_a_healthy_interrupted_upgrade_keeps_it_resumable() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    let before = fx.cold_index("cold-before");
    commands::upgrade::run(&fx.config, None).unwrap();
    fx.set_version(3);

    let result = repair(&fx.config, RepairMode::Apply).unwrap();
    assert_eq!(result.plan.index_recovery, IndexRecovery::Complete);
    assert!(!result.plan.has_data_loss, "{:?}", result.plan.actions);
    assert_eq!(fx.version(), 3);

    assert!(resumed(&commands::upgrade::run(&fx.config, None).unwrap()));
    assert_eq!(fx.cold_index("cold-after"), before);
}

/// A monolithic index whose first byte was damaged into the head's tag, with
/// nothing in `index.d/`: refused, not rebuilt as a segmented index.
#[test]
fn damaged_monolithic_index_that_looks_like_a_head_is_refused() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    let path = fx.repo_dir.join("index");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] = 9;
    std::fs::write(&path, &bytes).unwrap();

    for mode in [RepairMode::PlanOnly, RepairMode::Apply] {
        let err = repair(&fx.config, mode).err().unwrap().to_string();
        assert!(
            err.contains("more likely a damaged monolithic index"),
            "{err}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(fx.version(), 3);
        assert_eq!(fx.index_objects(), 0);
    }
    let err = commands::upgrade::run(&fx.config, None)
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("cannot be read"), "{err}");
}
