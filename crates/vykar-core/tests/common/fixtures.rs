//! Frozen repository fixtures under `tests/fixtures/`: extraction and the
//! helpers every fixture suite uses.

// `panic!` on a malformed fixture, and `meta["key"]` indexing a
// `serde_json::Value`, are the right failure modes here.
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use vykar_core::config::{EncryptionModeConfig, VykarConfig};
use vykar_core::repo::{EncryptionMode, OpenOptions, Repository};
use vykar_storage::local_backend::LocalBackend;

use super::{init_test_environment, make_test_config};

/// One extracted fixture archive.
pub struct Fixture {
    /// Kept alive for the duration of the test.
    pub _tmp: tempfile::TempDir,
    pub root: PathBuf,
    /// The archive name prefix, e.g. `v2`.
    pub prefix: String,
    pub mode: String,
    pub passphrase: Option<String>,
    pub snapshots: Vec<String>,
    /// The archive's `meta.json`.
    pub meta: serde_json::Value,
}

impl Fixture {
    pub fn repo_dir(&self) -> PathBuf {
        self.root.join("repo")
    }
    pub fn source_dir(&self) -> PathBuf {
        self.root.join("source")
    }
    pub fn cache_dir(&self) -> PathBuf {
        self.root.join("cache")
    }
    pub fn passphrase(&self) -> Option<&str> {
        self.passphrase.as_deref()
    }
    /// The snapshot the fixture's `source/` tree corresponds to (the last one).
    pub fn latest_snapshot(&self) -> &str {
        self.snapshots.last().unwrap()
    }

    pub fn config(&self) -> VykarConfig {
        let mut config = make_test_config(&self.repo_dir());
        config.cache_dir = Some(self.cache_dir().to_string_lossy().into_owned());
        config.encryption.mode = match self.mode.as_str() {
            "none" => EncryptionModeConfig::None,
            "aes256gcm" => EncryptionModeConfig::Aes256Gcm,
            "chacha20poly1305" => EncryptionModeConfig::Chacha20Poly1305,
            other => panic!("unknown fixture encryption mode {other}"),
        };
        config
    }

    pub fn expected_encryption(&self) -> EncryptionMode {
        match self.mode.as_str() {
            "none" => EncryptionMode::None,
            "aes256gcm" => EncryptionMode::Aes256Gcm,
            "chacha20poly1305" => EncryptionMode::Chacha20Poly1305,
            other => panic!("unknown fixture encryption mode {other}"),
        }
    }

    /// Open with the index and the fixture's cache dir.
    pub fn open(&self) -> Repository {
        self.open_with_cache(self.cache_dir())
    }

    /// Open with the index and a fresh cache dir `root/name`.
    pub fn open_cold(&self, name: &str) -> Repository {
        self.open_with_cache(self.root.join(name))
    }

    /// Open a v2/v3 fixture as `vykar upgrade` does: without the index, with
    /// the fixture's cache dir.
    pub fn open_legacy(&self) -> Repository {
        let storage = Box::new(LocalBackend::new(self.repo_dir().to_str().unwrap()).unwrap());
        Repository::open(
            storage,
            self.passphrase(),
            Some(self.cache_dir()),
            OpenOptions::new().legacy_format(),
        )
        .unwrap()
    }

    fn open_with_cache(&self, cache: PathBuf) -> Repository {
        let storage = Box::new(LocalBackend::new(self.repo_dir().to_str().unwrap()).unwrap());
        Repository::open(
            storage,
            self.passphrase(),
            Some(cache),
            OpenOptions::new().with_index(),
        )
        .unwrap()
    }
}

/// `tests/fixtures/{prefix}-{mode}.tar.gz`.
pub fn archive_path(prefix: &str, mode: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(format!("{prefix}-{mode}.tar.gz"))
}

/// Extract `tests/fixtures/{prefix}-{mode}.tar.gz`, asserting
/// `meta.format_version == format_version`.
pub fn extract(prefix: &str, mode: &str, format_version: u64) -> Fixture {
    init_test_environment();

    let archive = archive_path(prefix, mode);
    let tmp = tempfile::tempdir().unwrap();
    let file = std::fs::File::open(&archive)
        .unwrap_or_else(|e| panic!("open fixture {}: {e}", archive.display()));
    tar::Archive::new(flate2::read::GzDecoder::new(file))
        .unpack(tmp.path())
        .unwrap();

    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(tmp.path().join("meta.json")).unwrap()).unwrap();
    assert_eq!(meta["format_version"].as_u64(), Some(format_version));

    let root = tmp.path().to_path_buf();
    Fixture {
        _tmp: tmp,
        root,
        prefix: prefix.to_string(),
        mode: mode.to_string(),
        passphrase: meta["passphrase"].as_str().map(str::to_string),
        snapshots: meta["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect(),
        meta,
    }
}

pub fn digest_hex(data: &[u8]) -> String {
    use blake2::{Blake2b256, Digest};
    let mut hasher = Blake2b256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Every regular file under `root`, keyed by its path relative to `root` and
/// summarised as `(length, BLAKE2b-256 hex)` so a mismatch prints a diff a
/// human can read rather than megabytes of bytes.
pub fn read_tree(root: &Path) -> BTreeMap<PathBuf, (u64, String)> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                stack.push(path);
            } else {
                let rel = path.strip_prefix(root).unwrap().to_path_buf();
                let data = std::fs::read(&path).unwrap();
                out.insert(rel, (data.len() as u64, digest_hex(&data)));
            }
        }
    }
    out
}

/// Pack files of the fixture's repository.
fn pack_count(fx: &Fixture) -> usize {
    read_tree(&fx.repo_dir())
        .into_keys()
        .filter(|p| p.starts_with("packs"))
        .count()
}

/// Delete `index`, every object in `index.d/` and the fixture's cache dir.
fn lose_index(fx: &Fixture) {
    std::fs::remove_file(fx.repo_dir().join("index")).unwrap();
    for entry in std::fs::read_dir(fx.repo_dir().join("index.d")).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            std::fs::remove_file(path).unwrap();
        }
    }
    std::fs::remove_dir_all(fx.cache_dir()).unwrap();
}

/// A config for the fixture's repository with a fresh cache dir `root/name`.
fn cold_config(fx: &Fixture, name: &str) -> VykarConfig {
    let mut config = fx.config();
    config.cache_dir = Some(fx.root.join(name).to_string_lossy().into_owned());
    config
}

/// The fixture's packs were written before pack trailers. After `vykar
/// upgrade` and `commands::pack_maps::run`, losing the whole index and the
/// cache loses nothing: `check --repair` on a cold machine takes every pack
/// from a pack map and the latest snapshot restores byte-identical. Without
/// the maps, every pack is uncovered and the plan removes every snapshot.
pub fn assert_index_loss_is_recovered_from_pack_maps(prefix: &str, mode: &str, version: u64) {
    use vykar_core::commands;
    use vykar_core::commands::check::{IndexRecovery, RepairAction, RepairMode};
    use vykar_core::repo::control::CancelToken;

    let repair = |config: &VykarConfig, fx: &Fixture, mode: RepairMode| {
        commands::check::run_with_repair(
            config,
            fx.passphrase(),
            false,
            mode,
            None,
            &CancelToken::new(),
        )
        .unwrap()
    };

    let fx = extract(prefix, mode, version);
    let label = format!("{prefix}-{mode}");
    commands::upgrade::run(&fx.config(), fx.passphrase()).unwrap();
    let packs = pack_count(&fx);
    let stats =
        commands::pack_maps::run(&fx.config(), fx.passphrase(), &CancelToken::new()).unwrap();
    assert_eq!(stats.packs_mapped, packs, "{label}: {stats:?}");
    lose_index(&fx);
    let cold = cold_config(&fx, "cold");
    let result = repair(&cold, &fx, RepairMode::Apply);
    let IndexRecovery::RebuiltFromPacks { counts, .. } = result.plan.index_recovery else {
        panic!("{label}: {:?}", result.plan.index_recovery);
    };
    assert_eq!(
        (counts.packs, counts.from_maps),
        (packs, packs),
        "{label}: {counts:?}"
    );
    assert!(
        !result.plan.has_data_loss,
        "{label}: {:?}",
        result.plan.actions
    );
    assert!(
        result.repair_errors.is_empty(),
        "{label}: {:?}",
        result.repair_errors
    );
    let check = commands::check::run(&cold, fx.passphrase(), true, false).unwrap();
    assert!(check.errors.is_empty(), "{label}: {:?}", check.errors);
    let dest = fx.root.join("restore-rebuilt");
    commands::restore::run(
        &cold,
        fx.passphrase(),
        fx.latest_snapshot(),
        dest.to_str().unwrap(),
        None,
        cold.xattrs.enabled,
        true,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(read_tree(&dest), read_tree(&fx.source_dir()), "{label}");

    let fx = extract(prefix, mode, version);
    commands::upgrade::run(&fx.config(), fx.passphrase()).unwrap();
    lose_index(&fx);
    let cold = cold_config(&fx, "cold");
    let plan = repair(&cold, &fx, RepairMode::PlanOnly).plan;
    let IndexRecovery::RebuiltFromPacks { counts, .. } = plan.index_recovery else {
        panic!("{label}: {:?}", plan.index_recovery);
    };
    assert_eq!(
        (counts.packs, counts.uncovered),
        (packs, packs),
        "{label}: {counts:?}"
    );
    assert!(plan.has_data_loss, "{label}");
    for snapshot in &fx.snapshots {
        assert!(
            plan.actions.iter().any(|a| matches!(
                a,
                RepairAction::RemoveDanglingSnapshot { snapshot_name, .. } if snapshot_name == snapshot
            )),
            "{label}: {snapshot} is not removed: {:?}",
            plan.actions
        );
    }
}
