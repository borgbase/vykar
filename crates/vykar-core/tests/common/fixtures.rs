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
