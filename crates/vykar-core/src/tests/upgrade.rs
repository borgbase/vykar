//! `vykar upgrade`: monolithic index (v2/v3) to segmented (v4/v5) in place.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::helpers::{
    FaultyPut, backup_single_source, downgrade_to_legacy, is_base_put, is_head_put,
    make_test_config, try_backup_single_source,
};
use crate::commands;
use crate::commands::check::{IndexRecovery, RepairAction, RepairMode};
use crate::config::VykarConfig;
use crate::index::dedup_cache;
use crate::index::table::{self, ChunkTable};
use crate::repo::control::CancelToken;
use crate::repo::index_store::{self, INDEX_OBJECT_DIR};
use crate::repo::{
    EncryptionMode, OpenOptions, RepoConfig, RepoFormat, Repository, UpgradeOutcome,
};
use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::error::{Result, VykarError};

struct Fixture {
    tmp: tempfile::TempDir,
    repo_dir: PathBuf,
    source: PathBuf,
    config: VykarConfig,
    /// The v2/v3 format [`Fixture::downgrade`] produces.
    legacy: RepoFormat,
}

/// A repository at the segmented successor of `legacy` (v2 → v4, v3 → v5),
/// which a test fills and then turns into a `legacy` one with
/// [`Fixture::downgrade`]: no binary writes v2/v3 any more.
fn fixture(legacy: RepoFormat) -> Fixture {
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
        legacy.segmented_successor().unwrap(),
    )
    .unwrap();
    Fixture {
        tmp,
        repo_dir,
        source,
        config,
        legacy,
    }
}

impl Fixture {
    fn cache(&self) -> PathBuf {
        PathBuf::from(self.config.cache_dir.as_ref().unwrap())
    }

    fn storage(&self) -> Box<dyn StorageBackend> {
        Box::new(LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap())
    }

    /// Turn the repository into the `legacy` one 0.24 writes for its data.
    fn downgrade(&self) {
        let format = downgrade_to_legacy(&self.repo_dir, None, &self.cache());
        assert_eq!(format, self.legacy);
    }

    fn open(&self, cache: &Path, with_index: bool) -> Result<Repository> {
        let opts = if with_index {
            OpenOptions::new().with_index()
        } else {
            OpenOptions::new()
        };
        Repository::open(self.storage(), None, Some(cache.to_path_buf()), opts)
    }

    /// The open `vykar upgrade` does: any format, without the index.
    fn open_legacy(&self, cache: &Path) -> Result<Repository> {
        Repository::open(
            self.storage(),
            None,
            Some(cache.to_path_buf()),
            OpenOptions::new().legacy_format(),
        )
    }

    fn repo_id(&self) -> Vec<u8> {
        self.open_legacy(&self.cache()).unwrap().config.id.clone()
    }

    /// This machine's copy of the legacy index blob, planted as 0.24 leaves it.
    fn plant_blob_copy(&self) -> PathBuf {
        let blob =
            dedup_cache::index_blob_cache_path(&self.repo_id(), Some(&self.cache())).unwrap();
        std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
        std::fs::write(&blob, b"0.24 blob copy").unwrap();
        blob
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

    /// Every file under the repository, by its path relative to it.
    fn tree(&self) -> std::collections::BTreeMap<String, Vec<u8>> {
        let mut out = std::collections::BTreeMap::new();
        let mut stack = vec![self.repo_dir.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let rel = path.strip_prefix(&self.repo_dir).unwrap();
                    let rel = rel.to_string_lossy().replace('\\', "/");
                    out.insert(rel, std::fs::read(&path).unwrap());
                }
            }
        }
        out
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
        let repo = self.open_legacy(&self.cache()).unwrap();
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

    /// The index as a client with no local cache builds it; the legacy
    /// index while `config` is at v2/v3.
    fn cold_index(&self, name: &str) -> Vec<String> {
        let cache = self.tmp.path().join(name);
        if RepoFormat::from_version(self.version()).is_ok_and(RepoFormat::is_upgrade_only) {
            let repo = self.open_legacy(&cache).unwrap();
            return sorted_entries(&repo.legacy_index().unwrap());
        }
        sorted_entries(self.open(&cache, true).unwrap().chunk_index())
    }
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
        fx.downgrade();
        let before = fx.cold_index("cold-before");
        let blob = fx.plant_blob_copy();

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
    fx.downgrade();
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

/// Interrupted after the head PUT: `config` still says v3. Every open but
/// the upgrade's names `vykar upgrade`, which verifies the chain and
/// completes.
#[test]
fn interrupted_upgrade_resumes() {
    for (from, to) in [
        (RepoFormat::V2, RepoFormat::V4),
        (RepoFormat::V3, RepoFormat::V5),
    ] {
        let fx = fixture(from);
        fx.write("a", "alpha");
        fx.backup("s1");
        fx.downgrade();
        let before = fx.cold_index("cold-before");
        commands::upgrade::run(&fx.config, None).unwrap();
        fx.set_version(from.version());
        std::fs::write(fx.repo_dir.join("index.gen"), 9u64.to_le_bytes()).unwrap();

        let err = fx
            .open(&fx.tmp.path().join("other"), true)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("vykar upgrade"), "{from:?}: {err}");

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
                UpgradeOutcome::Upgraded { from: f, to: t, resumed: true, .. }
                    if (f, t) == (from.version(), to.version())
            ),
            "{from:?}: {outcome:?}"
        );
        assert_eq!(fx.version(), to.version(), "{from:?}");
        assert!(!fx.repo_dir.join("index.gen").exists(), "{from:?}");
        assert_eq!(fx.cold_index("cold-after"), before, "{from:?}");
        fx.check_clean();
    }
}

/// Both branches leave the handle's index at the upgraded state and its
/// table in the cache dir. On resume the table can only come from the sync.
#[test]
fn upgrade_leaves_the_index_and_the_table_at_the_new_state() {
    for resumed in [false, true] {
        let fx = fixture(RepoFormat::V3);
        fx.write("a", "alpha");
        fx.backup("s1");
        fx.downgrade();
        if resumed {
            commands::upgrade::run(&fx.config, None).unwrap();
            fx.set_version(3);
        }
        let id = fx.repo_id();
        let dir = crate::cache_paths::repo_cache_dir(&id, Some(&fx.cache())).unwrap();
        for (_, path) in table::list_tables(&dir) {
            std::fs::remove_file(path).unwrap();
        }

        let mut repo = fx.open_legacy(&fx.cache()).unwrap();
        let outcome =
            commands::util::with_maintenance_lock(&mut repo, |r| r.upgrade_index_layout()).unwrap();
        assert!(
            matches!(outcome, UpgradeOutcome::Upgraded { resumed: r, .. } if r == resumed),
            "{outcome:?}"
        );
        assert_eq!(repo.format(), RepoFormat::V5);
        let head = repo.index_head().unwrap().clone();
        assert_eq!(
            repo.chunk_index().table().map(|t| t.state()),
            Some((head.state_hash, head.entries)),
            "resumed: {resumed}"
        );
        let generation = dedup_cache::generation_for_state(&head.state_hash);
        assert_eq!(repo.index_generation(), generation);
        let bytes = std::fs::read(fx.repo_dir.join("index")).unwrap();
        let crypto = vykar_crypto::PlaintextEngine::new(crate::testutil::test_chunk_hasher());
        assert_eq!(index_store::decode_head(&bytes, &crypto).unwrap(), head);
        drop(repo);

        assert!(ChunkTable::open_file(&table::table_path(&dir, &head.state_hash)).is_some());
        fx.check_clean();
    }
}

/// The pre-flight fails before anything is written.
#[test]
fn upgrade_without_config_overwrite_changes_nothing() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let index = std::fs::read(fx.repo_dir.join("index")).unwrap();
    let (result, ops) = upgrade_through(&fx, |k, _| k == "config");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("append-only"), "{err}");
    assert!(err.contains("Nothing was changed"), "{err}");
    assert!(
        ops.iter().all(|o| o == "put config"
            || o.starts_with("put locks/")
            || o.starts_with("delete locks/")),
        "{ops:?}"
    );
    assert_eq!(
        ops.iter().filter(|o| *o == "put config").count(),
        1,
        "{ops:?}"
    );
    assert_eq!(fx.version(), 3);
    assert_eq!(std::fs::read(fx.repo_dir.join("index")).unwrap(), index);
    assert_eq!(fx.index_objects(), 0);
    commands::upgrade::run(&fx.config, None).unwrap();
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
    let mut repo = Repository::open(
        storage,
        None,
        Some(fx.cache()),
        OpenOptions::new().legacy_format(),
    )
    .unwrap();
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
        fx.downgrade();
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
    for (from, to) in [
        (RepoFormat::V2, RepoFormat::V4),
        (RepoFormat::V3, RepoFormat::V5),
    ] {
        for (name, fail) in [("base", is_base_put as Fail), ("head", is_head_put as Fail)] {
            let name = format!("{from:?}-{name}");
            let fx = fixture(from);
            fx.write("a", "alpha");
            fx.backup("s1");
            fx.downgrade();
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
            assert_eq!(fx.version(), from.version(), "{name}");
            assert!(debris.exists(), "{name}");
            assert_eq!(fx.cold_index(&format!("legacy-{name}")), before, "{name}");

            commands::upgrade::run(&fx.config, None).unwrap();
            assert_eq!(fx.version(), to.version(), "{name}");
            assert_eq!(fx.index_objects(), 1, "{name}");
            assert_eq!(fx.cold_index(&format!("after-{name}")), before, "{name}");
        }
    }
}

/// Upgrades under the maintenance lock with `fence` as the lock fence
/// (installed inside the lock, replacing the real one).
fn upgrade_with_fence(
    fx: &Fixture,
    fence: Arc<dyn Fn() -> Result<()> + Send + Sync>,
) -> Result<UpgradeOutcome> {
    let mut repo = fx.open_legacy(&fx.cache()).unwrap();
    commands::util::with_maintenance_lock(&mut repo, |repo| {
        repo.set_lock_fence(fence);
        repo.upgrade_index_layout()
    })
}

fn is_index_head(data: &[u8]) -> bool {
    data.first() == Some(&(crate::repo::format::ObjectType::IndexHead as u8))
}

fn holds_head(fx: &Fixture) -> bool {
    is_index_head(&std::fs::read(fx.repo_dir.join("index")).unwrap())
}

/// The refusal every open but the upgrade's returns for v2/v3.
fn assert_legacy_refusal(err: &VykarError) {
    let msg = err.to_string();
    assert!(
        msg.contains("which this vykar opens only to upgrade it") && msg.contains("vykar upgrade"),
        "{err:?}"
    );
}

/// Answers the capability probe as `answer` says; everything else forwards.
struct ProbedServer {
    inner: Arc<dyn StorageBackend>,
    answer: fn() -> Result<vykar_protocol::ServerCapabilities>,
}

impl InnerBackend for ProbedServer {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for ProbedServer;
    except [server_ops];

    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        Some(self)
    }
}

impl vykar_storage::ServerOps for ProbedServer {
    fn offers(&self, _op: vykar_storage::ServerOp) -> bool {
        false
    }

    fn capabilities(&self) -> Result<vykar_protocol::ServerCapabilities> {
        (self.answer)()
    }

    fn init(&self) -> Result<()> {
        Err(VykarError::Other("ProbedServer: init not expected".into()))
    }
}

fn server(segmented_index: bool) -> vykar_protocol::ServerCapabilities {
    vykar_protocol::ServerCapabilities {
        status: "ok".into(),
        version: "0.19.0".into(),
        protocol_version: vykar_protocol::PROTOCOL_VERSION,
        hashes: vec!["blake3".into()],
        pack_parts: false,
        segmented_index,
        ..Default::default()
    }
}

/// The version-switch `config` PUT fails after the head is in place: every
/// command but the upgrade is refused with the legacy-format error, nothing
/// is written, and a rerun resumes.
#[test]
fn upgrade_failing_at_the_version_switch_is_resumed() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let before = fx.cold_index("cold-before");
    let config_path = fx.repo_dir.join("config");
    let config_before = std::fs::read(&config_path).unwrap();
    assert!(fx.repo_dir.join("index.gen").exists());

    // The pre-flight PUT writes the stored bytes unchanged; the version
    // switch writes new ones.
    let unchanged = config_before.clone();
    let (result, ops) =
        upgrade_through(&fx, move |k, d| k == "config" && d != unchanged.as_slice());
    let err = result.err().unwrap().to_string();
    assert!(err.contains("injected failure: put config"), "{err}");
    let last_config = ops.iter().rposition(|o| o == "put config");
    let last_head = ops.iter().rposition(|o| o == "put index");
    assert!(last_head.is_some() && last_config > last_head, "{ops:?}");
    assert_eq!(std::fs::read(&config_path).unwrap(), config_before);
    assert_eq!(fx.version(), 3);
    assert!(holds_head(&fx));
    assert!(fx.repo_dir.join("index.gen").exists());
    let index = std::fs::read(fx.repo_dir.join("index")).unwrap();
    let packs = || {
        let mut keys = fx.storage().list("packs/").unwrap();
        keys.sort();
        keys
    };
    let packs_before = packs();

    assert_legacy_refusal(&fx.open(&fx.tmp.path().join("reader"), true).err().unwrap());
    assert_legacy_refusal(&fx.open(&fx.cache(), true).err().unwrap());
    assert_legacy_refusal(
        &commands::check::run(&fx.config, None, false, false)
            .err()
            .unwrap(),
    );
    assert_legacy_refusal(
        &commands::compact::run(&fx.config, None, 0.0, None, false, &CancelToken::new())
            .err()
            .unwrap(),
    );
    fx.write("b", "bravo");
    assert_legacy_refusal(
        &try_backup_single_source(&fx.config, &fx.source, "src", "s2")
            .err()
            .unwrap(),
    );
    assert_eq!(std::fs::read(fx.repo_dir.join("index")).unwrap(), index);
    assert_eq!(fx.snapshot_names(), ["s1"]);
    assert_eq!(packs(), packs_before);

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

/// The lock is lost just before the version switch, on a fresh upgrade and
/// on a resumed one: `config` is not written, and a rerun resumes.
#[test]
fn lost_lock_before_the_version_switch_leaves_config_unchanged() {
    for resume in [false, true] {
        let fx = fixture(RepoFormat::V3);
        fx.write("a", "alpha");
        fx.backup("s1");
        fx.downgrade();
        let before = fx.cold_index("cold-before");
        let debris = if resume {
            commands::upgrade::run(&fx.config, None).unwrap();
            fx.set_version(3);
            None
        } else {
            Some(write_debris(&fx))
        };
        let config_path = fx.repo_dir.join("config");
        let config_before = std::fs::read(&config_path).unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let probe: Arc<dyn StorageBackend> = Arc::from(fx.storage());
        let fence: Arc<dyn Fn() -> Result<()> + Send + Sync> = {
            let calls = Arc::clone(&calls);
            let debris = debris.clone();
            Arc::new(move || {
                calls.fetch_add(1, Ordering::SeqCst);
                // Fresh: only the fence after GC removed the debris, i.e. the
                // version switch's. Resume: that is the only fence call.
                let lost = match &debris {
                    None => true,
                    Some(d) => {
                        probe.get("index")?.is_some_and(|b| is_index_head(&b)) && !d.exists()
                    }
                };
                if lost {
                    Err(VykarError::LockExpired("injected".into()))
                } else {
                    Ok(())
                }
            })
        };
        let err = upgrade_with_fence(&fx, fence).err().unwrap();
        assert!(
            matches!(err, VykarError::LockExpired(_)),
            "{resume}: {err:?}"
        );
        let expected_calls = if resume { 1 } else { 4 };
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls, "{resume}");
        if let Some(d) = &debris {
            assert!(!d.exists());
        }
        assert_eq!(
            std::fs::read(&config_path).unwrap(),
            config_before,
            "{resume}"
        );
        assert_eq!(fx.version(), 3);
        assert!(holds_head(&fx), "{resume}");
        assert_legacy_refusal(&fx.open(&fx.tmp.path().join("cold"), true).err().unwrap());

        let outcome = commands::upgrade::run(&fx.config, None).unwrap();
        assert!(resumed(&outcome), "{resume}: {outcome:?}");
        assert_eq!(fx.version(), 5);
        assert_eq!(fx.cold_index("cold-after"), before, "{resume}");
        fx.check_clean();
    }
}

/// A server without the segmented index, or a failed probe, refuses the
/// upgrade before anything is written.
#[test]
fn upgrade_probes_the_server_before_writing() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let read = |name: &str| std::fs::read(fx.repo_dir.join(name)).unwrap();
    let state = || {
        (
            read("config"),
            read("index"),
            read("index.gen"),
            fx.index_objects(),
        )
    };
    let before = state();

    let upgrade = |answer: fn() -> Result<vykar_protocol::ServerCapabilities>| {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let logged = FaultyPut {
            inner: Arc::from(fx.storage()),
            fail: Box::new(|_, _| false),
            ops: Arc::clone(&ops),
        };
        let storage = Box::new(ProbedServer {
            inner: Arc::new(logged),
            answer,
        });
        let mut repo = Repository::open(
            storage,
            None,
            Some(fx.cache()),
            OpenOptions::new().legacy_format(),
        )
        .unwrap();
        let result = commands::util::with_maintenance_lock(&mut repo, |r| r.upgrade_index_layout());
        drop(repo);
        let ops = ops.lock().unwrap().clone();
        (result, ops)
    };
    let only_lock_ops = |ops: &[String]| {
        assert!(
            ops.iter()
                .all(|o| o.starts_with("put locks/") || o.starts_with("delete locks/")),
            "{ops:?}"
        );
    };

    let (result, ops) = upgrade(|| Ok(server(false)));
    let err = result.err().unwrap();
    assert!(matches!(err, VykarError::Config(_)), "{err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("does not support the segmented index of format v5")
            && msg.contains("Upgrade the server first"),
        "{msg}"
    );
    only_lock_ops(&ops);
    assert_eq!(state(), before);

    let (result, ops) = upgrade(|| Err(VykarError::Other("injected probe failure".into())));
    let msg = result.err().unwrap().to_string();
    assert!(msg.contains("injected probe failure"), "{msg}");
    only_lock_ops(&ops);
    assert_eq!(state(), before);

    let (result, ops) = upgrade(|| Ok(server(true)));
    for op in ["put index", "put config"] {
        assert!(ops.iter().any(|o| o == op), "{ops:?}");
    }
    let outcome = result.unwrap();
    assert!(
        matches!(
            outcome,
            UpgradeOutcome::Upgraded {
                from: 3,
                to: 5,
                resumed: false,
                ..
            }
        ),
        "{outcome:?}"
    );
    fx.check_clean();
}

fn repair(config: &VykarConfig, mode: RepairMode) -> Result<commands::check::RepairResult> {
    commands::check::run_with_repair(config, None, false, mode, None, &CancelToken::new())
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

/// An interrupted upgrade whose chain lost a segment: `vykar upgrade`
/// completes the version switch, deletes nothing and names `check --repair`,
/// which repairs the index as v5 (dropping the snapshot that needs the
/// segment).
#[test]
fn interrupted_upgrade_with_a_broken_chain_is_completed_then_repaired() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.write("b", "bravo");
    fx.backup("s2");
    fx.downgrade();
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
    let index_gen = fx.repo_dir.join("index.gen");
    std::fs::write(&index_gen, 9u64.to_le_bytes()).unwrap();
    let blob = fx.plant_blob_copy();

    let err = commands::upgrade::run(&fx.config, None)
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("completed an interrupted upgrade"), "{err}");
    assert!(err.contains("check --repair"), "{err}");
    assert_eq!(fx.version(), 5);
    assert!(index_gen.exists() && blob.exists());

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

    let bytes = std::fs::read(fx.repo_dir.join("index")).unwrap();
    let crypto = vykar_crypto::PlaintextEngine::new(crate::testutil::test_chunk_hasher());
    let head = index_store::decode_head(&bytes, &crypto).unwrap();
    assert!(head.base.is_some() && head.segments.is_empty());
    fx.check_clean();
    assert_eq!(fx.snapshot_names(), ["s1", "s2"]);
    assert_eq!(
        commands::upgrade::run(&fx.config, None).unwrap(),
        UpgradeOutcome::AlreadyCurrent { version: 5 }
    );
}

/// An interrupted upgrade whose head is damaged: `vykar upgrade` completes
/// the version switch and names `check --repair`, which rebuilds the index
/// from this machine's cache, or on another machine from the upgrade's base.
#[test]
fn interrupted_upgrade_with_a_corrupt_head_is_completed_then_repaired() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let before = fx.cold_index("cold-before");
    commands::upgrade::run(&fx.config, None).unwrap();
    fx.set_version(3);
    fx.truncate_head();
    let index_gen = fx.repo_dir.join("index.gen");
    std::fs::write(&index_gen, 9u64.to_le_bytes()).unwrap();
    let blob = fx.plant_blob_copy();

    let err = commands::upgrade::run(&fx.config, None)
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("completed an interrupted upgrade"), "{err}");
    assert!(err.contains("check --repair"), "{err}");
    assert_eq!(fx.version(), 5);
    assert!(index_gen.exists() && blob.exists());

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

    assert_eq!(fx.cold_index("cold-after"), before);
    fx.check_clean();
    assert_eq!(
        commands::upgrade::run(&fx.config, None).unwrap(),
        UpgradeOutcome::AlreadyCurrent { version: 5 }
    );
}

/// `check --repair` refuses a healthy interrupted upgrade like any v3
/// repository and changes nothing; `vykar upgrade` then resumes it.
#[test]
fn check_refuses_a_legacy_repository() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let before = fx.cold_index("cold-before");
    commands::upgrade::run(&fx.config, None).unwrap();
    fx.set_version(3);
    let state = fx.tree();

    for mode in [RepairMode::PlanOnly, RepairMode::Apply] {
        assert_legacy_refusal(&repair(&fx.config, mode).err().unwrap());
        assert_eq!(fx.tree(), state);
    }

    assert!(resumed(&commands::upgrade::run(&fx.config, None).unwrap()));
    assert_eq!(fx.cold_index("cold-after"), before);
}

/// A monolithic index whose first byte was damaged into the head's tag, with
/// nothing in `index.d/`: the upgrade refuses it and changes nothing, and
/// `check --repair` refuses the v3 repository.
#[test]
fn damaged_monolithic_index_that_looks_like_a_head_is_refused() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let path = fx.repo_dir.join("index");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] = 9;
    std::fs::write(&path, &bytes).unwrap();

    let err = commands::upgrade::run(&fx.config, None)
        .err()
        .unwrap()
        .to_string();
    assert!(
        err.contains("more likely a damaged monolithic index"),
        "{err}"
    );
    assert_eq!(fx.version(), 3);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(fx.index_objects(), 0);

    for mode in [RepairMode::PlanOnly, RepairMode::Apply] {
        assert_legacy_refusal(&repair(&fx.config, mode).err().unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(fx.version(), 3);
    }
}

/// A legacy index that cannot be decoded fails the upgrade before anything
/// is written, and nothing falls back to a local copy.
#[test]
fn unreadable_legacy_index_fails_the_upgrade_and_changes_nothing() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let path = fx.repo_dir.join("index");
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
    // A local copy 0.24 left must not be used in place of the unreadable index.
    fx.plant_blob_copy();
    let state = fx.tree();

    let err = commands::upgrade::run(&fx.config, None)
        .err()
        .unwrap()
        .to_string();
    assert!(
        err.contains("the v3 index cannot be read") && err.contains("nothing was changed"),
        "{err}"
    );
    assert_eq!(fx.version(), 3);
    assert_eq!(fx.tree(), state);
}

/// Every command but `vykar upgrade` refuses a v3 repository, naming the
/// upgrade, and none of them writes to it.
#[test]
fn legacy_open_is_refused_by_every_command_and_writes_nothing() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let state = fx.tree();
    let config = &fx.config;

    let errors: Vec<(&str, VykarError)> = vec![
        ("open", fx.open(&fx.cache(), false).err().unwrap()),
        ("open with index", fx.open(&fx.cache(), true).err().unwrap()),
        (
            "check",
            commands::check::run(config, None, false, false)
                .err()
                .unwrap(),
        ),
        (
            "check --repair --dry-run",
            repair(config, RepairMode::PlanOnly).err().unwrap(),
        ),
        (
            "list",
            commands::list::list_snapshots(config, None).err().unwrap(),
        ),
        ("info", commands::info::run(config, None).err().unwrap()),
        (
            "compact",
            commands::compact::run(config, None, 0.0, None, false, &CancelToken::new())
                .err()
                .unwrap(),
        ),
        (
            "prune",
            commands::prune::run(config, None, false, false, &[], &[], &CancelToken::new())
                .err()
                .unwrap(),
        ),
        (
            "delete",
            commands::delete::run(config, None, &["s1"], false, &CancelToken::new())
                .err()
                .unwrap(),
        ),
        (
            "backup",
            try_backup_single_source(config, &fx.source, "src", "s2")
                .err()
                .unwrap(),
        ),
    ];
    for (command, err) in &errors {
        assert!(
            err.to_string().contains("vykar upgrade"),
            "{command}: {err}"
        );
    }

    let transient = |p: &String| p.starts_with("locks/") || p.starts_with("sessions/");
    let after = fx.tree();
    assert!(after.keys().all(|p| !transient(p)), "{:?}", after.keys());
    let mut state = state;
    state.retain(|p, _| !transient(p));
    assert_eq!(after, state);
}

#[test]
fn legacy_open_rejects_load_index() {
    let fx = fixture(RepoFormat::V3);
    fx.downgrade();
    let err = Repository::open(
        fx.storage(),
        None,
        Some(fx.cache()),
        OpenOptions::new().legacy_format().with_index(),
    )
    .err()
    .unwrap()
    .to_string();
    assert!(err.contains("read only by `vykar upgrade`"), "{err}");
}

#[test]
fn init_refuses_legacy_formats() {
    for format in [RepoFormat::V2, RepoFormat::V3] {
        let tmp = tempfile::tempdir().unwrap();
        let err = Repository::init_at_format(
            Box::new(LocalBackend::new(tmp.path().to_str().unwrap()).unwrap()),
            EncryptionMode::None,
            crate::repo::ChunkerParams::default(),
            None,
            None,
            None,
            format,
        )
        .err()
        .unwrap();
        assert!(
            matches!(&err, VykarError::Config(m) if m.contains("can no longer be created")),
            "{format:?}: {err:?}"
        );
        assert!(!tmp.path().join("config").exists(), "{format:?}");
    }
}

/// Logs every `get` key.
struct GetLog {
    inner: Arc<dyn StorageBackend>,
    gets: Arc<Mutex<Vec<String>>>,
}

impl InnerBackend for GetLog {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for GetLog;
    except [get];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.gets.lock().unwrap().push(key.to_string());
        self.inner.get(key)
    }
}

/// The upgrade reads `index` once with a plain GET and never reads
/// `index.gen`, from an empty cache directory.
#[test]
fn upgrade_reads_the_index_with_a_plain_get() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let before = fx.cold_index("cold-before");

    let gets = Arc::new(Mutex::new(Vec::new()));
    let storage = Box::new(GetLog {
        inner: Arc::from(fx.storage()),
        gets: Arc::clone(&gets),
    });
    let cache = fx.tmp.path().join("empty-cache");
    let mut repo = Repository::open(
        storage,
        None,
        Some(cache),
        OpenOptions::new().legacy_format(),
    )
    .unwrap();
    let outcome =
        commands::util::with_maintenance_lock(&mut repo, |r| r.upgrade_index_layout()).unwrap();
    drop(repo);
    assert!(
        matches!(outcome, UpgradeOutcome::Upgraded { resumed: false, .. }),
        "{outcome:?}"
    );
    let gets = gets.lock().unwrap().clone();
    assert_eq!(gets.iter().filter(|k| *k == "index").count(), 1, "{gets:?}");
    assert!(!gets.iter().any(|k| k == "index.gen"), "{gets:?}");
    assert_eq!(fx.cold_index("cold-after"), before);
}
