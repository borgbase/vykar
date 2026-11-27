//! `vykar upgrade`: monolithic index (v2/v3) to format v6 in place.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::helpers::{
    FaultyPut, backup_single_source, downgrade_to_legacy, is_base_put, is_head_put,
    make_test_config, pass, try_backup_single_source,
};
use super::vendored_v0_24_lock as v024;
use crate::commands;
use crate::commands::check::RepairMode;
use crate::config::VykarConfig;
use crate::index::dedup_cache;
use crate::index::table::{self, ChunkTable};
use crate::repo::control::CancelToken;
use crate::repo::index_store::{self, INDEX_OBJECT_DIR};
use crate::repo::{
    DecodedConfig, EncryptionMode, HostEnv, OpenOptions, Repo, RepoConfig, RepoFormat,
    UpgradeOutcome, WriterMode,
};
use crate::sim::env::{SimTime, TimeMode};
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

/// A repository at the upgrade target of `legacy` (v6 with its hash),
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
    Repo::init_at_format(
        Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap()),
        EncryptionMode::Aes256Gcm,
        config.chunker.params(),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(&config.repository.init_options()),
        Some(cache),
        legacy.upgrade_target().unwrap(),
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
        let format = downgrade_to_legacy(&self.repo_dir, pass(&self.config), &self.cache());
        assert_eq!(format, self.legacy);
    }

    fn open(&self, cache: &Path, with_index: bool) -> Result<Repo> {
        let opts = if with_index {
            OpenOptions::new().with_index()
        } else {
            OpenOptions::new()
        };
        Repo::open(
            self.storage(),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(cache.to_path_buf()),
            opts,
        )
    }

    /// The open `vykar upgrade` does: any format, without the index.
    fn open_legacy(&self, cache: &Path) -> Result<Repo> {
        Repo::open(
            self.storage(),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(cache.to_path_buf()),
            OpenOptions::new().legacy_format(),
        )
    }

    fn repo_id(&self) -> Vec<u8> {
        self.open_legacy(&self.cache()).unwrap().config().id.clone()
    }

    /// This machine's copy of the legacy index blob, planted as 0.24 leaves it.
    fn plant_blob_copy(&self) -> PathBuf {
        let blob =
            dedup_cache::index_blob_cache_path(&self.repo_id(), Some(&self.cache())).unwrap();
        std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
        std::fs::write(&blob, b"0.24 blob copy").unwrap();
        blob
    }

    fn decoded_config(&self) -> DecodedConfig {
        RepoConfig::decode(&std::fs::read(self.repo_dir.join("config")).unwrap()).unwrap()
    }

    fn version(&self) -> u32 {
        match self.decoded_config() {
            DecodedConfig::Map(c) => c.version,
            DecodedConfig::Positional(c) => c.version,
        }
    }

    /// Rewrite `config` as the positional config of `version` (what the
    /// development formats 4 and 5 wrote).
    fn set_version(&self, version: u32) {
        let path = self.repo_dir.join("config");
        let c = self.decoded_config().into_repo_config();
        let legacy = crate::repo::LegacyRepoConfig {
            version,
            id: c.id,
            chunker_params: c.chunker_params,
            encryption: c.encryption,
            created: c.created,
            min_pack_size: c.min_pack_size,
            max_pack_size: c.max_pack_size,
        };
        std::fs::write(path, rmp_serde::to_vec(&legacy).unwrap()).unwrap();
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
        let result = commands::check::run(&self.config, pass(&self.config), true, false).unwrap();
        assert!(result.errors.is_empty(), "{:?}", result.errors);
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

    /// Generations under `index.h/`.
    fn heads(&self) -> Vec<u64> {
        index_store::list_head_generations(&*self.storage()).unwrap()
    }

    fn legacy_bytes(&self) -> Option<Vec<u8>> {
        std::fs::read(self.repo_dir.join("index")).ok()
    }

    /// The index as a client with no local cache builds it; the legacy
    /// index while `config` is at v2/v3.
    fn cold_index(&self, name: &str) -> Vec<String> {
        let cache = self.tmp.path().join(name);
        if self
            .decoded_config()
            .validate()
            .is_ok_and(RepoFormat::is_upgrade_only)
        {
            let repo = self.open_legacy(&cache).unwrap();
            return sorted_entries(&repo.legacy_index().unwrap().to_live_index());
        }
        sorted_entries(self.open(&cache, true).unwrap().chunk_index())
    }
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

/// B7 (04 step 7): upgrade rewrites the index and does not run beside a
/// backup. Through the production path the refusal runs under the legacy
/// lock key, after the stale-session cleanup; nothing
/// is upgraded and the session's marker is kept.
#[test]
fn upgrade_refuses_with_a_live_session() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let before = fx.decoded_config();
    let sessions = fx.repo_dir.join("sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let marker = sessions.join("deadbeef-1234-5678-9abc-def012345678.json");
    std::fs::write(
        &marker,
        format!(
            r#"{{"hostname":"other-host","pid":99999,"registered_at":"{now}","last_refresh":"{now}","elapsed_secs":0}}"#
        ),
    )
    .unwrap();
    let err = commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap_err();
    assert!(
        matches!(err, vykar_types::error::VykarError::ActiveSessions(_)),
        "got {err}"
    );
    assert!(marker.exists());
    assert_eq!(format!("{:?}", fx.decoded_config()), format!("{before:?}"));
    assert!(
        std::fs::read_dir(fx.repo_dir.join("locks"))
            .map(|d| d.count())
            .unwrap_or(0)
            == 0,
        "the lock is released"
    );
}

/// v2 and v3 → v6 (UPG-1): one base, no segments, a head at
/// `index.h/1`, the same index; `index`, `index.gen` and the local blob
/// copy gone; later backups append segments, and a second upgrade does
/// nothing.
#[test]
fn upgrade_moves_the_index_to_a_base() {
    for (from, to) in [
        (
            RepoFormat::V2,
            RepoFormat::V6(vykar_types::hash::HashAlgorithm::Blake2b),
        ),
        (RepoFormat::V3, RepoFormat::CURRENT),
    ] {
        let fx = fixture(from);
        fx.write("a", "alpha");
        fx.backup("s1");
        fx.write("b", "bravo");
        fx.backup("s2");
        fx.downgrade();
        let before = fx.cold_index("cold-before");
        let blob = fx.plant_blob_copy();

        let outcome = commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
        let UpgradeOutcome::Upgraded {
            from: f,
            to: t,
            chunk_hash,
            entries,
            base_bytes,
            packs_mapped,
            packs_unmapped,
            resumed,
            ..
        } = outcome
        else {
            panic!("{from:?}: {outcome:?}");
        };
        assert_eq!((f, t), (from.version(), to.version()));
        assert_eq!(chunk_hash, to.chunk_hash());
        assert!(packs_mapped > 0 && packs_unmapped == 0, "{from:?}");
        assert_eq!(entries as usize, before.len());
        assert!(base_bytes > 0 && !resumed);
        assert_eq!(fx.version(), to.version());
        assert!(!fx.repo_dir.join("index.gen").exists());
        assert!(!fx.repo_dir.join("index").exists());
        assert!(!blob.exists());
        assert_eq!(fx.heads(), vec![1]);
        assert_eq!(fx.index_objects(), 1);

        let repo = fx.open(&fx.cache(), true).unwrap();
        let head = repo.index_head().unwrap().clone();
        assert!(head.base.is_some() && head.segments.is_empty() && head.retired.is_empty());
        assert_eq!(head.sweep, crate::index::head::SweepState::default());
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

        let again = commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
        assert!(
            matches!(again, UpgradeOutcome::AlreadyCurrent { version, .. } if version == to.version()),
            "{again:?}"
        );
    }
}

/// An empty repository gets the empty head at `index.h/1`.
#[test]
fn upgrade_of_an_empty_repository() {
    let fx = fixture(RepoFormat::V3);
    fx.downgrade();
    let outcome = commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
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

/// The `config` PUT of the version switch fails, so the upgrade stops
/// after its head PUT.
fn interrupt_at_the_switch(fx: &Fixture) -> (VykarError, Vec<String>) {
    let unchanged = std::fs::read(fx.repo_dir.join("config")).unwrap();
    let (result, ops) = upgrade_through(fx, move |k, d| k == "config" && d != unchanged.as_slice());
    (result.err().unwrap(), ops)
}

/// UPG-2: interrupted after the head PUT, `config` still says v2/v3 and
/// `index` is intact, so a released-format reader still reads it. Every
/// open but the upgrade's names `vykar upgrade`, which rebuilds the new
/// index from the legacy one at the next generation and completes.
#[test]
fn interrupted_upgrade_resumes_from_the_legacy_index() {
    for (from, to) in [
        (
            RepoFormat::V2,
            RepoFormat::V6(vykar_types::hash::HashAlgorithm::Blake2b),
        ),
        (RepoFormat::V3, RepoFormat::CURRENT),
    ] {
        let fx = fixture(from);
        fx.write("a", "alpha");
        fx.backup("s1");
        fx.downgrade();
        let before = fx.cold_index("cold-before");
        let legacy = fx.legacy_bytes().unwrap();
        let (err, _) = interrupt_at_the_switch(&fx);
        assert!(
            err.to_string().contains("injected failure: put config"),
            "{err}"
        );
        assert_eq!(fx.version(), from.version());
        assert_eq!(fx.legacy_bytes().unwrap(), legacy, "{from:?}");
        assert_eq!(fx.heads(), vec![1]);
        assert_eq!(fx.cold_index("legacy-reader"), before, "{from:?}");

        let err = fx
            .open(&fx.tmp.path().join("other"), true)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("vykar upgrade"), "{from:?}: {err}");

        let outcome = commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
        assert!(
            matches!(
                outcome,
                UpgradeOutcome::Upgraded { from: f, to: t, resumed: true, .. }
                    if (f, t) == (from.version(), to.version())
            ),
            "{from:?}: {outcome:?}"
        );
        assert_eq!(fx.version(), to.version(), "{from:?}");
        assert_eq!(fx.heads(), vec![1, 2]);
        assert!(!fx.repo_dir.join("index.gen").exists(), "{from:?}");
        assert!(fx.legacy_bytes().is_none(), "{from:?}");
        assert_eq!(fx.cold_index("cold-after"), before, "{from:?}");
        fx.check_clean();
    }
}

/// Writes an old client makes between an interrupted attempt and the
/// resume land in the legacy index, and the resume keeps them.
#[test]
fn a_resume_keeps_what_an_old_client_wrote_in_between() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    interrupt_at_the_switch(&fx);
    // An old binary's write: the legacy index changes (here: one entry
    // fewer, as after its prune).
    let repo = fx.open_legacy(&fx.cache()).unwrap();
    let mut index = repo.legacy_index().unwrap();
    let crypto = Arc::clone(repo.crypto());
    drop(repo);
    let gone = *index.iter().next().unwrap().0;
    index.remove(&gone);
    let blob = super::helpers::legacy_index_blob(&index, 0x25, crypto.as_ref());
    std::fs::write(fx.repo_dir.join("index"), blob).unwrap();
    let expected = sorted_entries(&index.to_live_index());

    assert!(resumed(
        &commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap()
    ));
    let repo = fx.open(&fx.tmp.path().join("cold"), true).unwrap();
    assert_eq!(sorted_entries(repo.chunk_index()), expected);
    assert!(!repo.chunk_index().contains(&gone));
}

/// UPG-same: a legacy entry at refcount 0 (garbage the old release had not
/// removed yet) is not carried over; every other entry is, at its location.
#[test]
fn upgrade_drops_legacy_entries_at_refcount_zero() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let repo = fx.open_legacy(&fx.cache()).unwrap();
    let mut index = repo.legacy_index().unwrap();
    let crypto = Arc::clone(repo.crypto());
    drop(repo);
    let expected = sorted_entries(&index.to_live_index());
    let (_, e) = index.iter().map(|(id, e)| (*id, *e)).next().unwrap();
    let zero = vykar_types::chunk_id::ChunkId::from_bytes([0xEF; 32]);
    index.insert(
        zero,
        crate::index::LegacyIndexEntry {
            refcount: 0,
            pack_offset: e.pack_offset + 1,
            ..e
        },
    );
    let blob = super::helpers::legacy_index_blob(&index, 0x26, crypto.as_ref());
    std::fs::write(fx.repo_dir.join("index"), blob).unwrap();

    commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    let repo = fx.open(&fx.tmp.path().join("cold"), true).unwrap();
    assert!(!repo.chunk_index().contains(&zero));
    assert_eq!(sorted_entries(repo.chunk_index()), expected);
    drop(repo);
    fx.check_clean();
}

/// Both branches leave the handle's index at the upgraded state and its
/// base table in the cache dir.
#[test]
fn upgrade_leaves_the_index_and_the_table_at_the_new_state() {
    for resume in [false, true] {
        let fx = fixture(RepoFormat::V3);
        fx.write("a", "alpha");
        fx.backup("s1");
        fx.downgrade();
        if resume {
            interrupt_at_the_switch(&fx);
        }
        let id = fx.repo_id();
        let dir = crate::cache_paths::repo_cache_dir(&id, Some(&fx.cache())).unwrap();
        for (_, path) in table::list_base_tables(&dir) {
            std::fs::remove_file(path).unwrap();
        }

        let mut repo = fx.open_legacy(&fx.cache()).unwrap();
        let outcome =
            commands::util::with_exclusive_session(&mut repo, |r| r.upgrade_index_layout())
                .unwrap();
        assert!(
            matches!(outcome, UpgradeOutcome::Upgraded { resumed: r, .. } if r == resume),
            "{outcome:?}"
        );
        assert_eq!(repo.format(), RepoFormat::CURRENT);
        let head = repo.index_head().unwrap().clone();
        assert_eq!(
            *repo.chunk_index().state(),
            crate::index::table::IndexState::of_head(&head),
            "resumed: {resume}"
        );
        assert_eq!(repo.index_generation(), head.generation);
        assert_eq!(
            index_store::fetch_head(&*fx.storage(), repo.crypto().as_ref()).unwrap(),
            head
        );
        drop(repo);

        assert!(
            ChunkTable::open_file(&table::base_table_path(
                &dir,
                &head.base.as_ref().unwrap().name
            ))
            .is_some()
        );
        fx.check_clean();
    }
}

/// UPG-5: the pre-flight fails before anything is written.
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
        ops.iter().all(|o| o == "put config" || is_lock_op(o)),
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
    assert!(fx.heads().is_empty());
    commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
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
    let mut repo = Repo::open(
        storage,
        Some(crate::testing::TEST_PASSPHRASE),
        Some(fx.cache()),
        OpenOptions::new().legacy_format(),
    )
    .unwrap();
    let result = commands::upgrade::upgrade_repo(&mut repo, None);
    drop(repo);
    let ops = ops.lock().unwrap().clone();
    (result, ops)
}

/// Debris of an earlier attempt is young, so it is not deleted, and nothing
/// under `index.d/` is deleted before the head PUT.
#[test]
fn upgrade_keeps_young_debris() {
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
        assert!(
            !ops.iter()
                .any(|o| o.starts_with(&format!("delete {INDEX_OBJECT_DIR}"))),
            "{populated}: {ops:?}"
        );
        assert!(ops.iter().any(|o| o == "put index.h/1"), "{ops:?}");
        assert!(debris.exists(), "{populated}");
        assert_eq!(fx.index_objects(), 1 + usize::from(populated));
        assert_eq!(fx.version(), 6);
    }
}

/// An upgrade that fails at the base or head PUT leaves the legacy index,
/// the version and the debris as they were; a retry completes.
#[test]
fn upgrade_failing_before_the_head_keeps_the_legacy_index() {
    type Fail = fn(&str, &[u8]) -> bool;
    for (from, to) in [
        (
            RepoFormat::V2,
            RepoFormat::V6(vykar_types::hash::HashAlgorithm::Blake2b),
        ),
        (RepoFormat::V3, RepoFormat::CURRENT),
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
            assert!(fx.heads().is_empty(), "{name}");
            assert!(debris.exists(), "{name}");
            assert_eq!(fx.cold_index(&format!("legacy-{name}")), before, "{name}");

            commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
            assert_eq!(fx.version(), to.version(), "{name}");
            assert_eq!(fx.heads(), vec![1], "{name}");
            assert_eq!(fx.cold_index(&format!("after-{name}")), before, "{name}");
        }
    }
}

/// Upgrades in an exclusive session with `fence` as the writer's fence
/// (installed inside the session, replacing the real one).
fn upgrade_with_fence(
    fx: &Fixture,
    fence: Arc<dyn Fn() -> Result<()> + Send + Sync>,
) -> Result<UpgradeOutcome> {
    let mut repo = fx.open_legacy(&fx.cache()).unwrap();
    commands::util::with_exclusive_session(&mut repo, |repo| {
        repo.replace_fence_for_test(crate::repo::Fence::marker(fence));
        repo.upgrade_index_layout()
    })
}

/// The refusal every open but the upgrade's returns for v2/v3.
fn assert_legacy_refusal(err: &VykarError) {
    let msg = err.to_string();
    assert!(
        msg.contains("which this vykar opens only to upgrade it") && msg.contains("vykar upgrade"),
        "{err:?}"
    );
}

/// The version switch's `config` PUT fails after the head is in place:
/// every command but the upgrade is refused with the legacy-format error,
/// nothing is written, and a rerun resumes.
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
    let index = fx.legacy_bytes().unwrap();

    let (err, ops) = interrupt_at_the_switch(&fx);
    assert!(
        err.to_string().contains("injected failure: put config"),
        "{err}"
    );
    let last_config = ops.iter().rposition(|o| o == "put config");
    let last_head = ops.iter().rposition(|o| o.starts_with("put index.h/"));
    assert!(last_head.is_some() && last_config > last_head, "{ops:?}");
    assert_eq!(std::fs::read(&config_path).unwrap(), config_before);
    assert_eq!(fx.version(), 3);
    assert!(fx.repo_dir.join("index.gen").exists());
    let packs = || {
        let mut keys = fx.storage().list("packs/").unwrap();
        keys.sort();
        keys
    };
    let packs_before = packs();

    assert_legacy_refusal(&fx.open(&fx.tmp.path().join("reader"), true).err().unwrap());
    assert_legacy_refusal(&fx.open(&fx.cache(), true).err().unwrap());
    assert_legacy_refusal(
        &commands::check::run(&fx.config, pass(&fx.config), false, false)
            .err()
            .unwrap(),
    );
    assert_legacy_refusal(
        &commands::compact::run(
            &fx.config,
            pass(&fx.config),
            0.0,
            None,
            false,
            &CancelToken::new(),
        )
        .err()
        .unwrap(),
    );
    fx.write("b", "bravo");
    assert_legacy_refusal(
        &try_backup_single_source(&fx.config, &fx.source, "src", "s2")
            .err()
            .unwrap(),
    );
    assert_eq!(fx.legacy_bytes().unwrap(), index);
    assert_eq!(fx.snapshot_names(), ["s1"]);
    assert_eq!(packs(), packs_before);

    let outcome = commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    assert!(
        matches!(
            outcome,
            UpgradeOutcome::Upgraded {
                from: 3,
                to: 6,
                resumed: true,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(fx.version(), 6);
    assert!(!fx.repo_dir.join("index.gen").exists());
    assert_eq!(fx.cold_index("cold-after"), before);
    fx.check_clean();
}

/// The writer's fence fails just before the version switch, on a fresh upgrade and
/// on a resumed one: `config` is not written, the legacy index stays, and a
/// rerun resumes.
#[test]
fn a_lost_fence_before_the_version_switch_leaves_config_unchanged() {
    for resume in [false, true] {
        let fx = fixture(RepoFormat::V3);
        fx.write("a", "alpha");
        fx.backup("s1");
        fx.downgrade();
        let before = fx.cold_index("cold-before");
        if resume {
            interrupt_at_the_switch(&fx);
        }
        let config_path = fx.repo_dir.join("config");
        let config_before = std::fs::read(&config_path).unwrap();
        let legacy = fx.legacy_bytes().unwrap();

        // The fences, three on a fresh and on a resumed upgrade: before the
        // head PUT, then (a marker fence, since 10-single-protocol step 8)
        // before the index garbage collection, which a failed check skips,
        // then before the version switch, which a failed check refuses.
        // Every check after the first fails.
        let calls = Arc::new(AtomicUsize::new(0));
        let fence: Arc<dyn Fn() -> Result<()> + Send + Sync> = {
            let calls = Arc::clone(&calls);
            Arc::new(move || {
                if calls.fetch_add(1, Ordering::SeqCst) >= 1 {
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
        assert_eq!(calls.load(Ordering::SeqCst), 3, "{resume}");
        assert_eq!(
            std::fs::read(&config_path).unwrap(),
            config_before,
            "{resume}"
        );
        assert_eq!(fx.version(), 3);
        assert_eq!(fx.legacy_bytes().unwrap(), legacy, "{resume}");
        assert!(!fx.heads().is_empty(), "{resume}");
        assert_legacy_refusal(&fx.open(&fx.tmp.path().join("cold"), true).err().unwrap());

        let outcome = commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
        assert!(resumed(&outcome), "{resume}: {outcome:?}");
        assert_eq!(fx.version(), 6);
        assert_eq!(fx.cold_index("cold-after"), before, "{resume}");
        fx.check_clean();
    }
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

fn server(segmented_index: bool, v6_key_dirs: bool) -> vykar_protocol::ServerCapabilities {
    vykar_protocol::ServerCapabilities {
        status: "ok".into(),
        version: "0.19.0".into(),
        protocol_version: vykar_protocol::PROTOCOL_VERSION,
        hashes: vec!["blake3".into()],
        pack_parts: false,
        segmented_index,
        key_dirs: if v6_key_dirs {
            vykar_protocol::V6_KEY_DIRS
                .iter()
                .map(|d| d.to_string())
                .collect()
        } else {
            Vec::new()
        },
        ..Default::default()
    }
}

/// A server without the segmented index or the v6 key directories, or a
/// failed probe, refuses the upgrade before anything is written.
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
            fx.heads(),
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
        let mut repo = Repo::open(
            storage,
            Some(crate::testing::TEST_PASSPHRASE),
            Some(fx.cache()),
            OpenOptions::new().legacy_format(),
        )
        .unwrap();
        let result = commands::upgrade::upgrade_repo(&mut repo, None);
        drop(repo);
        let ops = ops.lock().unwrap().clone();
        (result, ops)
    };
    let only_lock_ops = |ops: &[String]| {
        assert!(ops.iter().all(|o| is_lock_op(o)), "{ops:?}");
        // D10: the legacy key is written before the exclusive marker.
        let key = ops.iter().position(|o| o.starts_with("put locks/"));
        let marker = ops.iter().position(|o| o.starts_with("put sessions/x-"));
        assert!(key.is_some() && marker.is_some() && key < marker, "{ops:?}");
    };

    for answer in [
        (|| Ok(server(false, true))) as fn() -> Result<vykar_protocol::ServerCapabilities>,
        || Ok(server(true, false)),
    ] {
        let (result, ops) = upgrade(answer);
        let err = result.err().unwrap();
        assert!(matches!(err, VykarError::Config(_)), "{err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains("does not support the index of format v6")
                && msg.contains("Upgrade the server first"),
            "{msg}"
        );
        only_lock_ops(&ops);
        assert_eq!(state(), before);
    }

    let (result, ops) = upgrade(|| Err(VykarError::Other("injected probe failure".into())));
    let msg = result.err().unwrap().to_string();
    assert!(msg.contains("injected probe failure"), "{msg}");
    only_lock_ops(&ops);
    assert_eq!(state(), before);

    let (result, ops) = upgrade(|| Ok(server(true, true)));
    for op in ["put index.h/1", "put config", "delete index"] {
        assert!(ops.iter().any(|o| o == op), "{ops:?}");
    }
    let outcome = result.unwrap();
    assert!(
        matches!(
            outcome,
            UpgradeOutcome::Upgraded {
                from: 3,
                to: 6,
                resumed: false,
                ..
            }
        ),
        "{outcome:?}"
    );
    fx.check_clean();
}

fn repair(config: &VykarConfig, mode: RepairMode) -> Result<commands::check::RepairResult> {
    commands::check::run_with_repair(config, pass(config), false, mode, None, &CancelToken::new())
}

fn resumed(outcome: &UpgradeOutcome) -> bool {
    matches!(outcome, UpgradeOutcome::Upgraded { resumed: true, .. })
}

/// UPG-3: interrupted after the version switch (the deletion of `index`
/// failed). The repository opens as v6, which ignores `index`; `vykar
/// upgrade` reports it current and deletes `index` and `index.gen`.
#[test]
fn upgrade_interrupted_after_the_switch_finishes_the_cleanup() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let legacy = fx.legacy_bytes().unwrap();
    commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    std::fs::write(fx.repo_dir.join("index"), &legacy).unwrap();
    std::fs::write(fx.repo_dir.join("index.gen"), 9u64.to_le_bytes()).unwrap();

    fx.check_clean();
    fx.write("b", "bravo");
    fx.backup("s2");
    let again = commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    assert!(
        matches!(again, UpgradeOutcome::AlreadyCurrent { version: 6, .. }),
        "{again:?}"
    );
    assert!(fx.legacy_bytes().is_none());
    assert!(!fx.repo_dir.join("index.gen").exists());
    fx.check_clean();
}

/// `check --repair` refuses an interrupted upgrade like any v3 repository
/// and changes nothing; `vykar upgrade` then resumes it.
#[test]
fn check_refuses_a_legacy_repository() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let before = fx.cold_index("cold-before");
    interrupt_at_the_switch(&fx);
    let state = fx.tree();

    for mode in [RepairMode::PlanOnly, RepairMode::Apply] {
        assert_legacy_refusal(&repair(&fx.config, mode).err().unwrap());
        assert_eq!(fx.tree(), state);
    }

    assert!(resumed(
        &commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap()
    ));
    assert_eq!(fx.cold_index("cold-after"), before);
}

/// UPG-4: an `index` holding an IndexHead-tagged object under a v3 config
/// (a development build's interrupted upgrade, or damage) is refused by the
/// upgrade, which changes nothing; `check --repair` refuses the v3
/// repository.
#[test]
fn an_index_holding_a_head_is_refused() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let path = fx.repo_dir.join("index");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] = crate::repo::format::ObjectType::IndexHead as u8;
    std::fs::write(&path, &bytes).unwrap();
    let state = fx.tree();

    let err = commands::upgrade::run(&fx.config, pass(&fx.config))
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("development build"), "{err}");
    assert!(err.contains("restore `index` from a copy"), "{err}");
    assert_eq!(fx.version(), 3);
    let transient = |p: &String| p.starts_with("locks/") || p.starts_with("sessions/");
    let mut after = fx.tree();
    after.retain(|p, _| !transient(p));
    let mut state = state;
    state.retain(|p, _| !transient(p));
    assert_eq!(after, state);
    assert!(fx.heads().is_empty());

    for mode in [RepairMode::PlanOnly, RepairMode::Apply] {
        assert_legacy_refusal(&repair(&fx.config, mode).err().unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(fx.version(), 3);
    }
}

/// FMT-2: a config rewritten to the development versions 4 and 5 is
/// refused by every open, the upgrade's included, by version, and nothing
/// is written.
#[test]
fn development_versions_4_and_5_are_refused() {
    for version in [4u32, 5] {
        let fx = fixture(RepoFormat::V3);
        fx.write("a", "alpha");
        fx.backup("s1");
        fx.set_version(version);
        let state = fx.tree();
        let unsupported = |e: VykarError| {
            assert!(
                matches!(e, VykarError::UnsupportedVersion(v) if v == version),
                "{version}: {e:?}"
            );
        };
        unsupported(fx.open(&fx.cache(), false).err().unwrap());
        unsupported(fx.open(&fx.cache(), true).err().unwrap());
        unsupported(fx.open_legacy(&fx.cache()).err().unwrap());
        unsupported(
            commands::upgrade::run(&fx.config, pass(&fx.config))
                .err()
                .unwrap(),
        );
        assert_eq!(fx.tree(), state, "{version}");
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

    let err = commands::upgrade::run(&fx.config, pass(&fx.config))
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
            commands::check::run(config, pass(config), false, false)
                .err()
                .unwrap(),
        ),
        (
            "check --repair --dry-run",
            repair(config, RepairMode::PlanOnly).err().unwrap(),
        ),
        (
            "list",
            commands::list::list_snapshots(config, pass(config))
                .err()
                .unwrap(),
        ),
        (
            "info",
            commands::info::run(config, pass(config)).err().unwrap(),
        ),
        (
            "compact",
            commands::compact::run(config, pass(config), 0.0, None, false, &CancelToken::new())
                .err()
                .unwrap(),
        ),
        (
            "prune",
            commands::prune::run(
                config,
                pass(config),
                false,
                false,
                &[],
                &[],
                &CancelToken::new(),
            )
            .err()
            .unwrap(),
        ),
        (
            "delete",
            commands::delete::run(config, pass(config), &["s1"], false, &CancelToken::new())
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
    let err = Repo::open(
        fx.storage(),
        Some(crate::testing::TEST_PASSPHRASE),
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
        let err = Repo::init_at_format(
            Box::new(LocalBackend::new(tmp.path().to_str().unwrap()).unwrap()),
            EncryptionMode::Aes256Gcm,
            crate::repo::ChunkerParams::default(),
            Some(crate::testing::TEST_PASSPHRASE),
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
    let mut repo = Repo::open(
        storage,
        Some(crate::testing::TEST_PASSPHRASE),
        Some(cache),
        OpenOptions::new().legacy_format(),
    )
    .unwrap();
    let outcome =
        commands::util::with_exclusive_session(&mut repo, |r| r.upgrade_index_layout()).unwrap();
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

/// R1: packs with more entries than a pack map may hold are skipped and
/// reported, and the upgrade completes; those packs are described only by
/// the index.
#[test]
fn upgrade_reports_packs_no_map_can_describe() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.write("b", "bravo");
    fx.backup("s1");
    fx.downgrade();
    crate::repo::pack_map::MAX_ENTRIES_FOR_TEST.with(|c| c.set(Some(1)));
    let outcome = commands::upgrade::run(&fx.config, pass(&fx.config));
    crate::repo::pack_map::MAX_ENTRIES_FOR_TEST.with(|c| c.set(None));
    let UpgradeOutcome::Upgraded {
        packs_mapped,
        packs_unmapped,
        ..
    } = outcome.unwrap()
    else {
        panic!("not upgraded");
    };
    assert!(packs_unmapped > 0, "{packs_unmapped}");
    assert!(packs_mapped + packs_unmapped > 0);
    assert_eq!(fx.version(), 6);
    fx.check_clean();
}

/// R1: a legacy index that describes a pack inconsistently (two chunks
/// overlapping) stops the upgrade before the switch: the repository keeps
/// its version and its legacy index.
#[test]
fn upgrade_stops_on_a_pack_the_legacy_index_describes_inconsistently() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.write("b", "bravo");
    fx.backup("s1");
    fx.downgrade();
    let repo = fx.open_legacy(&fx.cache()).unwrap();
    let mut index = repo.legacy_index().unwrap();
    let (first, e) = index
        .iter()
        .map(|(id, e)| (*id, *e))
        .find(|(_, e)| index.iter().filter(|(_, o)| o.pack_id == e.pack_id).count() > 1)
        .expect("a pack with two chunks");
    let overlapping = vykar_types::chunk_id::ChunkId::from_bytes([0xEE; 32]);
    index.insert(
        overlapping,
        crate::index::LegacyIndexEntry {
            pack_offset: e.pack_offset + 1,
            ..e
        },
    );
    let _ = first;
    let blob = super::helpers::legacy_index_blob(&index, 0x24, repo.crypto().as_ref());
    drop(repo);
    std::fs::write(fx.repo_dir.join("index"), &blob).unwrap();

    let err = commands::upgrade::run(&fx.config, pass(&fx.config))
        .unwrap_err()
        .to_string();
    assert!(err.contains("inconsistently"), "{err}");
    assert!(err.contains("still v3"), "{err}");
    assert_eq!(fx.version(), 3);
    assert_eq!(std::fs::read(fx.repo_dir.join("index")).unwrap(), blob);
    assert!(fx.heads().is_empty(), "no head written");
}

/// UPG-7: after an interrupted attempt and its resume, the index garbage
/// collection (no grace) keeps every object the latest head names and
/// deletes objects no kept head names; the attempt's head stays within the
/// window of recent heads.
#[test]
fn garbage_collection_after_a_resumed_upgrade_keeps_the_latest_objects() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    interrupt_at_the_switch(&fx);
    commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    assert_eq!(fx.heads(), vec![1, 2]);
    let debris = write_debris(&fx);
    let repo = fx.open(&fx.cache(), false).unwrap();
    let latest = repo.index_head().unwrap().clone();
    let stats = index_store::collect_index_garbage(
        repo.storage().as_ref(),
        repo.crypto().as_ref(),
        &latest,
        index_store::IndexGcGrace::both(std::time::Duration::ZERO),
    )
    .unwrap();
    assert!(stats.objects_deleted >= 1, "{stats:?}");
    assert!(!debris.exists(), "unnamed debris is collected");
    for name in latest.named_objects() {
        assert!(
            fx.repo_dir
                .join(INDEX_OBJECT_DIR)
                .join(name.to_hex())
                .exists(),
            "{name:?}"
        );
    }
    assert_eq!(fx.heads(), vec![1, 2], "within latest - 16");
    drop(repo);
    fx.check_clean();
}

/// TRL-3: packs of an upgraded repository are recorded without a trailer
/// (`trailer_len` 0; v2/v3 writers wrote none), but these hold one (the
/// fixture was written by this build): compact counts the trailer as dead,
/// repacks such a pack once, and leaves the output alone afterwards.
#[test]
fn a_trailer_recorded_as_absent_is_repacked_once() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    let repo = fx.open(&fx.cache(), true).unwrap();
    let packs: Vec<_> = repo.chunk_index().iter().map(|(_, e)| e.pack_id).collect();
    assert!(
        packs
            .iter()
            .all(|p| repo.chunk_index().pack_trailer_len(p) == Some(0))
    );
    drop(repo);
    // Compact repacks only packs older than T_pack.
    super::helpers::age_repo_objects(&fx.repo_dir, &["packs"], super::helpers::PAST_T_PACK);
    let first = commands::compact::run(
        &fx.config,
        pass(&fx.config),
        1.0,
        None,
        false,
        &CancelToken::new(),
    )
    .unwrap();
    assert!(first.packs_repacked > 0, "{first:?}");
    super::helpers::age_repo_objects(&fx.repo_dir, &["packs"], super::helpers::PAST_T_PACK);
    let second = commands::compact::run(
        &fx.config,
        pass(&fx.config),
        1.0,
        None,
        false,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(second.packs_repacked, 0, "{second:?}");
    let repo = fx.open(&fx.cache(), true).unwrap();
    for (_, e) in repo.chunk_index().iter() {
        let recorded = repo.chunk_index().pack_trailer_len(&e.pack_id).unwrap();
        let bytes = repo
            .storage()
            .get(&e.pack_id.storage_key())
            .unwrap()
            .unwrap();
        assert_eq!(
            recorded,
            4 + crate::repo::pack_trailer::parse_footer(&bytes).unwrap()
        );
    }
    drop(repo);
    fx.check_clean();
}

/// The frozen unencrypted v2/v3 fixtures, opened as `vykar upgrade` opens
/// them (`open_repo_on` with `legacy_format`): refused by their config
/// with the storage op log holding no PUT and no DELETE.
#[test]
fn legacy_none_fixtures_are_refused_without_a_write() {
    for (prefix, version) in [("v2", 2u64), ("v3", 3)] {
        crate::testutil::init_test_environment();
        let archive = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(format!("{prefix}-none.tar.gz"));
        let tmp = tempfile::tempdir().unwrap();
        tar::Archive::new(flate2::read::GzDecoder::new(
            std::fs::File::open(&archive).unwrap(),
        ))
        .unpack(tmp.path())
        .unwrap();
        let meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(tmp.path().join("meta.json")).unwrap()).unwrap();
        assert_eq!(meta["format_version"].as_u64(), Some(version));
        let repo_dir = tmp.path().join("repo");
        let config = make_test_config(&repo_dir);
        let local =
            vykar_storage::local_backend::LocalBackend::new(repo_dir.to_str().unwrap()).unwrap();
        let (storage, calls) = crate::testutil::CallLog::new(Box::new(local));
        let err = crate::commands::util::open_repo_on(
            Box::new(storage),
            &crate::commands::util::open_request(&config),
            pass(&config),
            OpenOptions::new().legacy_format(),
        )
        .err()
        .expect("an unencrypted repository is refused")
        .to_string();
        assert!(
            err.contains("no longer supports unencrypted repositories")
                && err.contains("new repository"),
            "{prefix}: {err}"
        );
        let calls = calls.lock().unwrap().clone();
        assert_eq!(calls, ["get config"], "{prefix}");
    }
}

// ── The writer mode (10-single-protocol step 5) ─────────────────────────

/// A backend that offers no put-if-absent, over another one.
struct NoConditional {
    inner: Arc<dyn StorageBackend>,
}

impl InnerBackend for NoConditional {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for NoConditional;
    except [conditional];

    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }
}

impl Fixture {
    fn writer_mode(&self) -> WriterMode {
        match self.decoded_config() {
            DecodedConfig::Map(c) => c.writer_mode(),
            DecodedConfig::Positional(_) => panic!("a positional config"),
        }
    }

    fn config_bytes(&self) -> Vec<u8> {
        std::fs::read(self.repo_dir.join("config")).unwrap()
    }

    /// A handle (any format) whose storage offers no put-if-absent.
    fn open_without_conditional(&self) -> Repo {
        Repo::open(
            Box::new(NoConditional {
                inner: Arc::from(self.storage()),
            }),
            Some(crate::testing::TEST_PASSPHRASE),
            Some(self.cache()),
            OpenOptions::new().legacy_format(),
        )
        .unwrap()
    }

    /// The upgrade's production body (exclusive session, live sessions
    /// refused) on a handle without put-if-absent.
    fn upgrade_without_conditional(&self, mode: Option<WriterMode>) -> Result<UpgradeOutcome> {
        let mut repo = self.open_without_conditional();
        commands::util::with_exclusive_session(&mut repo, |repo| {
            repo.refuse_live_sessions()?;
            repo.upgrade_index_layout_with(mode)
        })
    }

    /// A backup whose storage offers no put-if-absent.
    fn backup_without_conditional(&self, name: &str) -> Result<()> {
        let source_paths = vec![self.source.to_string_lossy().to_string()];
        let wrap = |b: Box<dyn StorageBackend>| -> Box<dyn StorageBackend> {
            Box::new(NoConditional {
                inner: Arc::from(b),
            })
        };
        commands::backup::run_impl(
            &self.config,
            commands::backup::BackupRequest {
                snapshot_name: name,
                passphrase: pass(&self.config),
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
            },
            None,
            &CancelToken::new(),
            Some(&wrap),
            crate::repo::HostEnv::system(),
        )
        .map(drop)
    }

    /// A live session marker of another host under `sessions/`.
    fn plant_live_session(&self) -> PathBuf {
        let sessions = self.repo_dir.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let marker = sessions.join("deadbeef-1234-5678-9abc-def012345678.json");
        std::fs::write(
            &marker,
            format!(
                r#"{{"hostname":"other-host","pid":99999,"registered_at":"{now}","last_refresh":"{now}","elapsed_secs":0}}"#
            ),
        )
        .unwrap();
        marker
    }
}

/// A v3 repository on storage that passes the init-time check is upgraded
/// as multi-writer.
#[test]
fn upgrade_records_the_mode_the_check_decides() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let outcome = commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    let UpgradeOutcome::Upgraded { writer_mode, .. } = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(writer_mode.mode, WriterMode::Multi);
    assert!(writer_mode.changed && writer_mode.single_reason.is_none());
    assert_eq!(fx.writer_mode(), WriterMode::Multi);
    let DecodedConfig::Map(c) = fx.decoded_config() else {
        unreachable!()
    };
    assert_eq!(
        c.features,
        vec![crate::repo::MULTI_WRITER_FEATURE.to_string()]
    );
    fx.check_clean();
}

/// `--commit-mode single` on a v3 repository records single-writer, with
/// no reason (it was asked for).
#[test]
fn upgrade_with_commit_mode_single_records_single() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let outcome =
        commands::upgrade::run_with_mode(&fx.config, pass(&fx.config), Some(WriterMode::Single))
            .unwrap();
    let UpgradeOutcome::Upgraded { writer_mode, .. } = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(writer_mode.mode, WriterMode::Single);
    assert_eq!(writer_mode.single_reason, None);
    assert_eq!(fx.writer_mode(), WriterMode::Single);
    fx.backup("s2");
    fx.check_clean();
}

/// `--commit-mode multi` on storage without put-if-absent refuses before
/// anything is written: the config is still the positional v3 one, and no
/// head or pack map exists.
#[test]
fn upgrade_with_commit_mode_multi_refuses_on_storage_without_the_condition() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    let config = fx.config_bytes();
    let err = fx
        .upgrade_without_conditional(Some(WriterMode::Multi))
        .unwrap_err();
    assert!(err.to_string().contains("nothing was changed"), "{err}");
    assert_eq!(fx.config_bytes(), config);
    assert!(matches!(fx.decoded_config(), DecodedConfig::Positional(c) if c.version == 3));
    assert!(fx.heads().is_empty());
    assert_eq!(
        std::fs::read_dir(fx.repo_dir.join("packmaps"))
            .map(|d| d.count())
            .unwrap_or(0),
        0
    );
}

/// On a v6 repository a plain upgrade reports the mode and changes
/// nothing; `--commit-mode` switches it, changing only the feature, and a
/// switch to the current mode is a no-op.
#[test]
fn a_v6_upgrade_reports_the_mode_and_switches_on_request() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    let run = |mode| commands::upgrade::run_with_mode(&fx.config, pass(&fx.config), mode).unwrap();
    let report = |outcome: UpgradeOutcome| match outcome {
        UpgradeOutcome::AlreadyCurrent {
            version: 6,
            writer_mode,
        } => writer_mode,
        other => panic!("{other:?}"),
    };
    let before = fx.decoded_config().into_repo_config();
    let r = report(run(None));
    assert_eq!((r.mode, r.changed), (WriterMode::Multi, false));
    assert!(!r.put_if_absent_offered);

    let r = report(run(Some(WriterMode::Single)));
    assert_eq!((r.mode, r.changed), (WriterMode::Single, true));
    let after = fx.decoded_config().into_repo_config();
    assert!(after.features.is_empty());
    let mut expected = before.clone();
    expected.features.clear();
    assert_eq!(after, expected, "only the feature changed");
    // A plain upgrade now says the storage could be multi-writer.
    let r = report(run(None));
    assert_eq!((r.mode, r.changed), (WriterMode::Single, false));
    assert!(r.put_if_absent_offered);
    let config = fx.config_bytes();
    let r = report(run(Some(WriterMode::Single)));
    assert_eq!((r.mode, r.changed), (WriterMode::Single, false));
    assert_eq!(fx.config_bytes(), config, "a no-op writes nothing");
    fx.backup("s2");

    let r = report(run(Some(WriterMode::Multi)));
    assert_eq!((r.mode, r.changed), (WriterMode::Multi, true));
    assert_eq!(fx.decoded_config().into_repo_config(), before);
    fx.backup("s3");
    fx.check_clean();
}

/// A client whose storage lost put-if-absent: its backup refuses on the
/// multi-writer repository; it switches the repository to single-writer
/// through the upgrade's production body and then backs up. Its switch
/// back to multi-writer refuses with nothing changed.
#[test]
fn a_client_without_the_condition_switches_to_single_and_backs_up() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    assert_eq!(fx.writer_mode(), WriterMode::Multi);
    let err = fx.backup_without_conditional("refused").unwrap_err();
    assert!(
        matches!(err, VykarError::MultiWriterUnsupported { .. }),
        "{err:?}"
    );
    let outcome = fx
        .upgrade_without_conditional(Some(WriterMode::Single))
        .unwrap();
    assert!(
        matches!(&outcome, UpgradeOutcome::AlreadyCurrent { writer_mode, .. }
            if writer_mode.mode == WriterMode::Single && writer_mode.changed),
        "{outcome:?}"
    );
    assert_eq!(fx.writer_mode(), WriterMode::Single);
    fx.write("b", "bravo");
    fx.backup_without_conditional("s2").unwrap();

    let config = fx.config_bytes();
    let err = fx
        .upgrade_without_conditional(Some(WriterMode::Multi))
        .unwrap_err();
    assert!(err.to_string().contains("stays single-writer"), "{err}");
    assert_eq!(fx.config_bytes(), config);
    assert_eq!(fx.snapshot_names(), vec!["s1", "s2"]);
    fx.check_clean();
}

/// A v6 `upgrade --commit-mode` beside a live backup's session refuses
/// `ActiveSessions` naming it (its exclusive registration's listing sees
/// the marker, 10-single-protocol step 8): the config bytes and every other
/// object are unchanged, and no exclusive marker stays.
#[test]
fn the_switch_refuses_beside_a_live_session() {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    let config = fx.config_bytes();
    let marker = fx.plant_live_session();
    let tree = fx.tree();
    let err =
        commands::upgrade::run_with_mode(&fx.config, pass(&fx.config), Some(WriterMode::Single))
            .unwrap_err();
    assert!(matches!(err, VykarError::ActiveSessions(_)), "{err:?}");
    assert!(
        err.to_string()
            .contains("deadbeef-1234-5678-9abc-def012345678"),
        "{err}"
    );
    assert!(marker.exists());
    assert_eq!(fx.config_bytes(), config);
    assert_eq!(fx.tree(), tree, "the refusal wrote nothing that stayed");
    assert!(exclusive_marker_keys(&*fx.storage()).is_empty());
}

// ── The legacy lock key (10-single-protocol step 6) ─────────────────────

/// What a [`Hooked`] hook is called with.
#[derive(Clone, Copy, Debug)]
enum Call<'a> {
    /// A read (`get`, `exists`) of the key.
    Read(&'a str),
    /// A write (`put k`, `delete k`, `create_dir k`), before it is
    /// forwarded; an `Err` fails it unforwarded.
    Before(&'a str),
    /// The same write after it was forwarded and succeeded.
    After(&'a str),
}

type Hook = Box<dyn Fn(Call<'_>, &dyn StorageBackend) -> Result<()> + Send + Sync>;

/// Logs every write (`put`, `delete`, `create_dir`, packs and staged
/// writes included) and calls `hook` around each write and before each
/// `get`/`exists`, with the raw backend.
struct Hooked {
    inner: Arc<dyn StorageBackend>,
    hook: Hook,
    ops: Mutex<Vec<String>>,
}

impl Hooked {
    fn write(&self, op: String, f: impl FnOnce() -> Result<()>) -> Result<()> {
        self.ops.lock().unwrap().push(op.clone());
        (self.hook)(Call::Before(&op), &*self.inner)?;
        f()?;
        (self.hook)(Call::After(&op), &*self.inner)
    }

    fn ops(&self) -> Vec<String> {
        self.ops.lock().unwrap().clone()
    }
}

impl InnerBackend for Hooked {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for Hooked;
    except [get, put, delete, exists, create_dir, put_pack, begin_staged_put, conditional,
            batch_delete, profile];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        (self.hook)(Call::Read(key), &*self.inner)?;
        self.inner.get(key)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.write(format!("put {key}"), || self.inner.put(key, data))
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.write(format!("delete {key}"), || self.inner.delete(key))
    }

    fn exists(&self, key: &str) -> Result<bool> {
        (self.hook)(Call::Read(key), &*self.inner)?;
        self.inner.exists(key)
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        self.write(format!("create_dir {key}"), || self.inner.create_dir(key))
    }

    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        self.write(format!("put {key}"), || self.inner.put(key, data.as_slice()))
    }

    // Keeps every write on the hooked `put`.
    fn begin_staged_put(&self, _prefix: &str) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        Ok(None)
    }

    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        None
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            staged_put: false,
            ..self.inner.profile()
        }
    }
}

/// The 0.24 lock keys (`locks/*.json`) on `raw`.
fn legacy_lock_keys(raw: &dyn StorageBackend) -> Vec<String> {
    let mut keys = v024::list_lock_keys(raw).unwrap();
    keys.sort();
    keys
}

/// The one lock key on `raw`: the upgrade's.
fn the_lock_key(raw: &dyn StorageBackend) -> String {
    let keys = legacy_lock_keys(raw);
    assert_eq!(keys.len(), 1, "{keys:?}");
    keys.into_iter().next().unwrap()
}

/// Whether `op` is a write of a lock key.
/// An op of the upgrade's exclusion: on its legacy lock key (`locks/`) or
/// on its exclusive session's marker (`sessions/x-`, 10-single-protocol
/// step 8: registration, refresh, deregistration).
fn is_lock_op(op: &str) -> bool {
    op.split_once(' ')
        .is_some_and(|(_, k)| k.starts_with("locks/") || k.starts_with("sessions/x-"))
}

/// `fx` opened as `vykar upgrade` opens it, on `env` when given, then
/// switched to a [`Hooked`] over its storage (so the open's reads are not
/// hooked).
fn hooked_repo(fx: &Fixture, env: Option<HostEnv>, hook: Hook) -> (Repo, Arc<Hooked>) {
    let mut repo = fx.open_legacy(&fx.cache()).unwrap();
    if let Some(env) = env {
        repo.set_host_env(env);
    }
    let hooked = Arc::new(Hooked {
        inner: Arc::from(fx.storage()),
        hook,
        ops: Mutex::new(Vec::new()),
    });
    repo.set_storage_for_test(Arc::clone(&hooked) as Arc<dyn StorageBackend>);
    (repo, hooked)
}

/// A v3 fixture with one backup.
fn legacy_fixture() -> Fixture {
    let fx = fixture(RepoFormat::V3);
    fx.write("a", "alpha");
    fx.backup("s1");
    fx.downgrade();
    fx
}

/// A sim clock starting now and this machine's identity on it.
fn sim_env(host: Option<crate::repo::HostIdentity>) -> (Arc<SimTime>, HostEnv) {
    let time = SimTime::new(chrono::Utc::now(), TimeMode::Auto);
    let mut env = HostEnv::system().with_clock(Arc::new(time.clock(chrono::Duration::zero())));
    if let Some(host) = host {
        env = env.with_host(host);
    }
    (time, env)
}

/// Done-when 1: while the upgrade runs, a 0.24 backup's (or read's)
/// session registration and a 0.24 maintenance lock both refuse, naming the
/// upgrade's key.
///
/// The vendored 0.24 code reads this machine's real hostname, PID liveness
/// and boot ID. The upgrade runs on the real host environment
/// (`HostEnv::system()`: this hostname, this live PID, this boot), so 0.24's
/// same-host rule finds the PID alive and the boot unchanged and leaves the
/// key to the age rule, under which it is young.
#[test]
fn a_0_24_backup_and_compact_refuse_during_an_upgrade() {
    let fx = legacy_fixture();
    let probed = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&probed);
    let hook: Hook = Box::new(move |call, raw| {
        let Call::Before(op) = call else {
            return Ok(());
        };
        if !op.starts_with("put packmaps/") || seen.fetch_add(1, Ordering::SeqCst) > 0 {
            return Ok(());
        }
        let key = the_lock_key(raw);
        match v024::register_session_inner(raw, "v024", 1, 100) {
            Err(VykarError::Locked(holder)) => assert_eq!(holder.key, key),
            other => panic!("0.24 registration: {other:?}"),
        }
        assert!(!raw.exists("sessions/v024.json").unwrap());
        match v024::acquire_lock(raw) {
            Err(VykarError::Locked(holder)) => assert_eq!(holder.key, key),
            other => panic!("0.24 lock: {other:?}"),
        }
        assert_eq!(legacy_lock_keys(raw), vec![key]);
        Ok(())
    });
    let (mut repo, _hooked) = hooked_repo(&fx, None, hook);
    let outcome = commands::upgrade::upgrade_repo(&mut repo, None).unwrap();
    drop(repo);
    assert!(
        matches!(outcome, UpgradeOutcome::Upgraded { from: 3, to: 6, .. }),
        "{outcome:?}"
    );
    assert!(probed.load(Ordering::SeqCst) > 0, "no pack map was written");
    assert!(legacy_lock_keys(&*fx.storage()).is_empty());
    fx.check_clean();
}

/// Done-when 2: beside a live 0.24 lock the upgrade refuses, writing
/// nothing but its own key (and deleting it again).
#[test]
fn the_upgrade_refuses_beside_a_0_24_lock() {
    let fx = legacy_fixture();
    let raw = fx.storage();
    let guard = v024::acquire_lock(&*raw).unwrap();
    let foreign = guard.key().to_string();
    let config = std::fs::read(fx.repo_dir.join("config")).unwrap();
    let index = fx.legacy_bytes();

    let (mut repo, hooked) = hooked_repo(&fx, None, Box::new(|_, _| Ok(())));
    let err = commands::upgrade::upgrade_repo(&mut repo, None).unwrap_err();
    drop(repo);
    match &err {
        VykarError::Locked(holder) => assert_eq!(holder.key, foreign),
        other => panic!("{other:?}"),
    }
    let ops = hooked.ops();
    assert_eq!(ops.len(), 2, "{ops:?}");
    assert!(ops[0].starts_with("put locks/"), "{ops:?}");
    assert_eq!(ops[1], ops[0].replacen("put", "delete", 1), "{ops:?}");
    assert_eq!(std::fs::read(fx.repo_dir.join("config")).unwrap(), config);
    assert_eq!(fx.legacy_bytes(), index);
    assert!(!fx.repo_dir.join("packmaps").exists());
    assert!(fx.heads().is_empty());
    assert_eq!(legacy_lock_keys(&*raw), vec![foreign]);

    v024::release_lock(&*raw, guard).unwrap();
    commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    assert_eq!(fx.version(), 6);
}

/// Done-when 3: an upgrade that takes 7 h keeps its key's `time` under an
/// hour old at every write, so 0.24 never takes it for stale.
///
/// Each call not under `locks/` advances the clock 10 min, each such write
/// another 40 min, in the order assert (before forwarding), forward,
/// advance. So between two writes 50 min or more pass: an implementation
/// that rewrites only at the lock fence (the head and the `config` switch)
/// or only every 3 h would show a key older than an hour at the second or
/// third write, and fail the assertion.
#[test]
fn the_legacy_key_stays_young_across_a_7h_upgrade() {
    let fx = legacy_fixture();
    // Manual time: the exclusive session's heartbeat sleeps until the hook
    // advances the clock, so it keeps up with every minute.
    let time = SimTime::new(chrono::Utc::now(), TimeMode::Manual);
    let env = HostEnv::system().with_clock(Arc::new(time.clock(chrono::Duration::zero())));
    let checked = Arc::new(AtomicUsize::new(0));
    let hook: Hook = {
        let time = Arc::clone(&time);
        let checked = Arc::clone(&checked);
        Box::new(move |call, raw| {
            // One heartbeat poll (30 s) at a time, each time letting the
            // exclusive session's heartbeat (10-single-protocol step 8) catch
            // up: it counts polls, not elapsed time, and a stretch over its
            // gap limit (30 min) reads as a suspended machine and stops the
            // upgrade.
            let step = |minutes: u64| {
                for _ in 0..minutes * 2 {
                    time.advance(crate::repo::sessions::HEARTBEAT_POLL);
                    time.wait_for_waiters(1, std::time::Duration::from_secs(5));
                }
            };
            match call {
                Call::Read(key)
                    if !key.starts_with("locks/") && !key.starts_with("sessions/x-") =>
                {
                    step(10)
                }
                Call::Before(op) if !is_lock_op(op) => {
                    let key = the_lock_key(raw);
                    let entry: v024::LockEntry =
                        serde_json::from_slice(&raw.get(&key).unwrap().unwrap()).unwrap();
                    let written = chrono::DateTime::parse_from_rfc3339(&entry.time)
                        .unwrap()
                        .with_timezone(&chrono::Utc);
                    let now = time.now_utc();
                    assert!(
                        now - written < chrono::Duration::minutes(60),
                        "{op}: the key is {} old",
                        now - written
                    );
                    assert!(!v024::should_cleanup_lock(
                        &key,
                        Some(&entry),
                        now,
                        chrono::Duration::hours(6),
                        "other-host",
                        None,
                        &|_| true,
                    ));
                    checked.fetch_add(1, Ordering::SeqCst);
                }
                Call::After(op) if !is_lock_op(op) => step(50),
                _ => {}
            }
            Ok(())
        })
    };
    let (mut repo, hooked) = hooked_repo(&fx, Some(env), hook);
    let outcome = commands::upgrade::upgrade_repo(&mut repo, None).unwrap();
    drop(repo);
    assert!(
        matches!(outcome, UpgradeOutcome::Upgraded { from: 3, to: 6, .. }),
        "{outcome:?}"
    );
    assert!(
        time.elapsed() >= std::time::Duration::from_secs(7 * 3600),
        "{:?}",
        time.elapsed()
    );
    let ops = hooked.ops();
    let own = ops
        .iter()
        .find(|o| o.starts_with("put locks/") && o.ends_with(".json"))
        .unwrap()
        .clone();
    let rewrites = ops.iter().filter(|o| **o == own).count() - 1;
    assert!(rewrites >= 6, "{rewrites} rewrites: {ops:?}");
    assert!(checked.load(Ordering::SeqCst) >= 7);
    assert!(legacy_lock_keys(&*fx.storage()).is_empty());
    fx.check_clean();
}

/// Done-when 4a: a key removed during the upgrade (`break-lock`, as 0.24
/// runs it) stops the upgrade at its next write; nothing more is written,
/// not even the release. At the pre-flight `config` re-PUT the error is the
/// lost key's, not the append-only text.
#[test]
fn an_upgrade_whose_key_was_removed_writes_nothing_more() {
    // (what triggers the removal, where the upgrade stops)
    for (trigger, at_config) in [("get config", true), ("put packmaps/", false)] {
        let fx = legacy_fixture();
        let index = fx.legacy_bytes();
        let removed = Arc::new(Mutex::new(None::<usize>));
        let (mut repo, hooked) = {
            let removed = Arc::clone(&removed);
            let ops_seen = Arc::new(AtomicUsize::new(0));
            hooked_repo(
                &fx,
                None,
                Box::new(move |call, raw| {
                    let hit = match call {
                        Call::Read(key) => trigger == format!("get {key}") && at_config,
                        Call::Before(op) => {
                            ops_seen.fetch_add(1, Ordering::SeqCst);
                            !at_config && op.starts_with(trigger)
                        }
                        Call::After(_) => false,
                    };
                    let mut removed = removed.lock().unwrap();
                    if hit && removed.is_none() {
                        assert_eq!(v024::break_lock(raw).unwrap(), 1);
                        // Ops logged so far; the write being hooked is in flight.
                        *removed = Some(ops_seen.load(Ordering::SeqCst));
                    }
                    Ok(())
                }),
            )
        };
        let err = commands::upgrade::upgrade_repo(&mut repo, None).unwrap_err();
        drop(repo);
        let msg = err.to_string();
        assert!(
            matches!(err, VykarError::LockExpired(_)),
            "{trigger}: {err:?}"
        );
        assert!(msg.contains("removed"), "{msg}");
        let ops = hooked.ops();
        let at = removed.lock().unwrap().expect("the key was removed");
        // The in-flight write (packmaps) completes; nothing after it but the
        // exclusive session's deregistration (10-single-protocol step 8).
        assert!(ops.len() >= at, "{trigger}: {ops:?}");
        assert!(
            ops[at..]
                .iter()
                .all(|o| o.starts_with("delete sessions/x-")),
            "{trigger}: {ops:?}"
        );
        if at_config {
            assert!(!ops.iter().any(|o| o == "put config"), "{ops:?}");
        }
        assert_eq!(fx.version(), 3, "{trigger}");
        assert_eq!(fx.legacy_bytes(), index, "{trigger}");
        assert!(fx.heads().is_empty(), "{trigger}");

        commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
        assert_eq!(fx.version(), 6, "{trigger}");
        fx.check_clean();
    }
}

/// Done-when 4b: an upgrade that cannot rewrite its key and stalls 3 h
/// between writes stops before switching, deleting its keys and writing
/// nothing else. Since 10-single-protocol step 8 the exclusive session's
/// check runs first, and the 3 h gap breaks its marker record's continuity
/// (over 30 min): the upgrade stops `MarkerLostDuringCommit` at its second
/// write, before the legacy key's 5.5 h rule (`tests::legacy_lock` covers
/// that rule on the key itself).
#[test]
fn an_upgrade_that_cannot_rewrite_its_key_stops() {
    let fx = legacy_fixture();
    let (time, env) = sim_env(None);
    let hook: Hook = {
        let time = Arc::clone(&time);
        // The key the acquisition wrote; every later PUT of it fails.
        let own = Mutex::new(None::<String>);
        Box::new(move |call, _raw| {
            match call {
                Call::Before(op) if op.starts_with("put locks/") && op.ends_with(".json") => {
                    let mut own = own.lock().unwrap();
                    match own.as_deref() {
                        None => *own = Some(op.to_string()),
                        Some(first) if first == op => {
                            return Err(VykarError::Other(format!("injected failure: {op}")));
                        }
                        Some(_) => {}
                    }
                }
                // 3 h per write: the second write's check finds the rewrite
                // due and fails it, the third finds 6 h since the last
                // successful one. Only a failed rewrite counted as a
                // success would get past 5.5 h.
                Call::After(op) if !is_lock_op(op) => {
                    time.advance(std::time::Duration::from_secs(3 * 3600));
                }
                _ => {}
            }
            Ok(())
        })
    };
    let (mut repo, hooked) = hooked_repo(&fx, Some(env), hook);
    let err = commands::upgrade::upgrade_repo(&mut repo, None).unwrap_err();
    drop(repo);
    assert!(matches!(err, VykarError::MarkerLostDuringCommit), "{err:?}");
    let ops = hooked.ops();
    let own_put = ops[0].clone();
    assert!(own_put.starts_with("put locks/"), "{ops:?}");
    let own_key = own_put.strip_prefix("put ").unwrap();
    let last_write = ops.iter().rposition(|o| !is_lock_op(o)).unwrap();
    // After the last data write: only the exclusion's own requests (marker
    // refreshes, failed key rewrites), ending with the deletes of the
    // marker keys and of the legacy key.
    let after = &ops[last_write + 1..];
    assert!(after.iter().all(|o| is_lock_op(o)), "{ops:?}");
    assert_eq!(after.last(), Some(&format!("delete {own_key}")), "{ops:?}");
    assert!(
        after.iter().any(|o| o.starts_with("delete sessions/x-")),
        "{ops:?}"
    );
    assert!(matches!(fx.decoded_config(), DecodedConfig::Positional(c) if c.version == 3));
    assert!(fx.heads().is_empty());
    assert!(legacy_lock_keys(&*fx.storage()).is_empty());

    commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    assert_eq!(fx.version(), 6);
}

/// A key removed between acquisition and the stale-session reap's delete:
/// since 10-single-protocol step 8 the reap is the exclusive registration's
/// and runs on the raw storage (the legacy key's check does not guard it),
/// so the stale session is reaped; the upgrade then stops `LockExpired` at
/// its first write, the variant kept, with no data written.
#[test]
fn a_key_removed_before_the_session_reap_is_lock_expired() {
    let fx = legacy_fixture();
    let host = crate::repo::HostIdentity {
        hostname: "upgrader-host".into(),
        pid: 4242,
        boot_id: Some("boot-x".into()),
        installation_id: Some("inst-x".into()),
        pid_ns: Some(1),
    };
    // A session this machine left behind: its PID is dead, so it is reaped.
    let now = chrono::Utc::now().to_rfc3339();
    let entry = crate::repo::sessions::SessionEntry {
        hostname: host.hostname.clone(),
        pid: 7,
        registered_at: now.clone(),
        last_refresh: now,
        boot_id: host.boot_id.clone(),
        installation_id: host.installation_id.clone(),
        pid_ns: host.pid_ns,
        elapsed_secs: None,
        committing: false,
        writes_heads: false,
    };
    let marker = "sessions/deadbeef-1234-5678-9abc-def012345678.json";
    fx.storage()
        .put(marker, &serde_json::to_vec(&entry).unwrap())
        .unwrap();
    let env = HostEnv::system()
        .with_host(host)
        .with_pid_alive(Arc::new(|_, pid| pid == 4242));
    let hook: Hook = Box::new(move |call, raw| {
        // The reap's GET of the marker, before its delete: `vykar clear-sessions`.
        if let Call::Read(key) = call
            && key == marker
        {
            raw.delete(&the_lock_key(raw))?;
        }
        Ok(())
    });
    let (mut repo, hooked) = hooked_repo(&fx, Some(env), hook);
    let err = commands::upgrade::upgrade_repo(&mut repo, None).unwrap_err();
    drop(repo);
    assert!(matches!(err, VykarError::LockExpired(_)), "{err:?}");
    let ops = hooked.ops();
    assert!(
        ops.iter()
            .all(|o| is_lock_op(o) || *o == format!("delete {marker}")),
        "{ops:?}"
    );
    assert!(!fx.storage().exists(marker).unwrap(), "the reap ran");
    assert!(matches!(fx.decoded_config(), DecodedConfig::Positional(c) if c.version == 3));
    assert!(legacy_lock_keys(&*fx.storage()).is_empty());
}

/// The documented residual: a 0.24 client on a machine with the
/// upgrader's hostname whose PID check fails there (another machine with
/// that name, this one after a reboot, another PID namespace) reaps the
/// key at once; the upgrade then stops at its next write, before the switch.
#[test]
fn a_0_24_client_with_this_hostname_reaps_the_key() {
    let fx = legacy_fixture();
    let host = crate::repo::HostIdentity {
        hostname: "upgrader-host".into(),
        pid: 999_999,
        boot_id: Some("boot-x".into()),
        installation_id: None,
        pid_ns: None,
    };
    let env = HostEnv::system().with_host(host);
    let reaped = Arc::new(AtomicUsize::new(0));
    let hook: Hook = {
        let reaped = Arc::clone(&reaped);
        Box::new(move |call, raw| {
            if let Call::Before(op) = call
                && op.starts_with("put packmaps/")
                && reaped.fetch_add(1, Ordering::SeqCst) == 0
            {
                // 0.24's reaper as it runs on a machine named upgrader-host,
                // where the upgrader's PID is not alive.
                let surviving = v024::cleanup_stale_locks_inner(
                    raw,
                    chrono::Duration::hours(6),
                    "upgrader-host",
                    Some("boot-x"),
                    |_| false,
                )
                .unwrap();
                assert!(surviving.is_empty(), "{surviving:?}");
            }
            Ok(())
        })
    };
    let (mut repo, _hooked) = hooked_repo(&fx, Some(env), hook);
    let err = commands::upgrade::upgrade_repo(&mut repo, None).unwrap_err();
    drop(repo);
    assert!(matches!(err, VykarError::LockExpired(_)), "{err:?}");
    assert!(reaped.load(Ordering::SeqCst) > 0);
    assert_eq!(fx.version(), 3);
    assert!(fx.heads().is_empty());
}

/// The legacy key's own `LockExpired` through a whole v2/v3 upgrade (the
/// legacy arm of the combined check): the exclusive session's heartbeat
/// keeps its marker fresh (the clock stepped one heartbeat poll at a time,
/// as in [`the_legacy_key_stays_young_across_a_7h_upgrade`]) while every
/// rewrite of the legacy key fails; once its last successful write is more
/// than 5.5 h old the next check stops the upgrade before the switch,
/// nothing more is written but the exclusion's own deletes, and a rerun
/// completes.
#[test]
fn a_legacy_key_that_cannot_be_rewritten_expires_beside_a_live_marker() {
    let fx = legacy_fixture();
    let time = SimTime::new(chrono::Utc::now(), TimeMode::Manual);
    let env = HostEnv::system().with_clock(Arc::new(time.clock(chrono::Duration::zero())));
    let hook: Hook = {
        let time = Arc::clone(&time);
        let own = Mutex::new(None::<String>);
        Box::new(move |call, _raw| {
            let step = |minutes: u64| {
                for _ in 0..minutes * 2 {
                    time.advance(crate::repo::sessions::HEARTBEAT_POLL);
                    time.wait_for_waiters(1, std::time::Duration::from_secs(5));
                }
            };
            match call {
                Call::Before(op) if op.starts_with("put locks/") && op.ends_with(".json") => {
                    let mut own = own.lock().unwrap();
                    match own.as_deref() {
                        None => *own = Some(op.to_string()),
                        Some(first) if first == op => {
                            return Err(VykarError::Other(format!("injected failure: {op}")));
                        }
                        Some(_) => {}
                    }
                }
                Call::Read(key)
                    if !key.starts_with("locks/") && !key.starts_with("sessions/x-") =>
                {
                    step(10)
                }
                Call::After(op) if !is_lock_op(op) => step(50),
                _ => {}
            }
            Ok(())
        })
    };
    let (mut repo, hooked) = hooked_repo(&fx, Some(env), hook);
    let err = commands::upgrade::upgrade_repo(&mut repo, None).unwrap_err();
    drop(repo);
    assert!(
        matches!(&err, VykarError::LockExpired(m) if m.contains("last rewritten")),
        "{err:?}"
    );
    let ops = hooked.ops();
    let own_put = ops[0].clone();
    assert!(own_put.starts_with("put locks/"), "{ops:?}");
    let own_key = own_put.strip_prefix("put ").unwrap();
    // The heartbeat refreshed the marker (a later generation was PUT): the
    // stop is the legacy key's, not a lapse of the marker.
    assert!(
        ops.iter().any(|o| {
            o.strip_prefix("put ").is_some_and(|k| {
                crate::repo::sessions::parse_session_marker_key(k)
                    .is_some_and(|(id, n)| crate::repo::sessions::is_exclusive_session(id) && n > 0)
            })
        }),
        "{ops:?}"
    );
    assert!(
        ops.iter().filter(|o| **o == own_put).count() > 1,
        "the key's rewrites were tried: {ops:?}"
    );
    let last_write = ops.iter().rposition(|o| !is_lock_op(o)).unwrap();
    let after = &ops[last_write + 1..];
    assert!(after.iter().all(|o| is_lock_op(o)), "{ops:?}");
    assert_eq!(after.last(), Some(&format!("delete {own_key}")), "{ops:?}");
    assert!(matches!(fx.decoded_config(), DecodedConfig::Positional(c) if c.version == 3));
    assert!(legacy_lock_keys(&*fx.storage()).is_empty());
    assert!(
        exclusive_marker_keys(&*fx.storage()).is_empty(),
        "the marker was deregistered"
    );

    commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    assert_eq!(fx.version(), 6);
    fx.check_clean();
}

/// The `sessions/x-*.json` keys on `raw`.
fn exclusive_marker_keys(raw: &dyn StorageBackend) -> Vec<String> {
    raw.list("sessions/")
        .unwrap()
        .into_iter()
        .filter(|k| k.starts_with("sessions/x-") && k.ends_with(".json"))
        .collect()
}

/// A v2/v3 upgrade whose exclusive marker is deleted mid-run
/// (`vykar clear-sessions`, reuse 0 so the next write checks): the write
/// in flight lands; the next check stops the upgrade
/// `MarkerLostDuringCommit`, nothing more is written, the legacy key is
/// released, the version is not switched, and a rerun completes.
#[test]
fn an_upgrade_whose_marker_was_removed_writes_nothing_more() {
    let fx = legacy_fixture();
    let index = fx.legacy_bytes();
    let removed = Arc::new(Mutex::new(None::<usize>));
    let (mut repo, hooked) = {
        let removed = Arc::clone(&removed);
        let ops_seen = Arc::new(AtomicUsize::new(0));
        hooked_repo(
            &fx,
            None,
            Box::new(move |call, raw| {
                let Call::Before(op) = call else {
                    return Ok(());
                };
                ops_seen.fetch_add(1, Ordering::SeqCst);
                let mut removed = removed.lock().unwrap();
                if op.starts_with("put packmaps/") && removed.is_none() {
                    let keys = exclusive_marker_keys(raw);
                    assert!(!keys.is_empty(), "the upgrade's marker");
                    for key in keys {
                        raw.delete(&key).unwrap();
                    }
                    // Ops logged so far; the write being hooked is in flight.
                    *removed = Some(ops_seen.load(Ordering::SeqCst));
                }
                Ok(())
            }),
        )
    };
    repo.set_exclusive_check_reuse(std::time::Duration::ZERO);
    let err = commands::upgrade::upgrade_repo(&mut repo, None).unwrap_err();
    drop(repo);
    assert!(matches!(err, VykarError::MarkerLostDuringCommit), "{err:?}");
    let ops = hooked.ops();
    let at = removed.lock().unwrap().expect("the marker was removed");
    let own_key = ops[0].strip_prefix("put ").unwrap().to_string();
    assert!(own_key.starts_with("locks/"), "{ops:?}");
    // After the in-flight write: only the exclusion's own deletes (the
    // deregistration and the legacy key's release, last).
    assert!(ops.len() > at, "{ops:?}");
    assert!(
        ops[at..]
            .iter()
            .all(|o| o.starts_with("delete sessions/x-") || o.starts_with("delete locks/")),
        "{ops:?}"
    );
    assert_eq!(ops.last(), Some(&format!("delete {own_key}")), "{ops:?}");
    assert!(legacy_lock_keys(&*fx.storage()).is_empty());
    assert_eq!(fx.version(), 3);
    assert_eq!(fx.legacy_bytes(), index);
    assert!(fx.heads().is_empty());

    commands::upgrade::run(&fx.config, pass(&fx.config)).unwrap();
    assert_eq!(fx.version(), 6);
    fx.check_clean();
}
