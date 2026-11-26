//! An unencrypted repository is refused by its own config, whatever the
//! client's config says (#208), and nothing is written.

#![allow(clippy::unwrap_used, clippy::panic)]
#![allow(clippy::pedantic)]

mod common;

use std::path::Path;
use std::sync::{Arc, Mutex};

use vykar_core::commands;
use vykar_core::commands::util::open_repo;
use vykar_core::compress::Compression;
use vykar_core::config::{EncryptionModeConfig, VykarConfig};
use vykar_core::repo::control::CancelToken;
use vykar_core::repo::{EncryptionMode, OpenOptions, Repo, RepoConfig};
use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::error::{Result, VykarError};

use crate::common::fixtures::{self, read_tree};
use crate::common::make_test_config;

const PASS: &str = "correct horse battery staple";

/// Records every call on the storage as "op key" and forwards it.
struct CallLog {
    inner: LocalBackend,
    calls: Arc<Mutex<Vec<String>>>,
}

impl CallLog {
    fn note(&self, op: &str, key: &str) {
        self.calls.lock().unwrap().push(format!("{op} {key}"));
    }
}

impl InnerBackend for CallLog {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for CallLog;
    except [
        get, put, delete, exists, list, get_range, get_range_into, open_range, create_dir,
        put_owned, put_pack, begin_staged_put, size, list_sized, list_meta, remove_stale_temp_files,
        conditional, batch_delete, server_ops, profile
    ];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.note("get", key);
        self.inner.get(key)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.note("put", key);
        self.inner.put(key, data)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.note("delete", key);
        self.inner.delete(key)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        self.note("exists", key);
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.note("list", prefix);
        self.inner.list(prefix)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.note("get_range", key);
        self.inner.get_range(key, offset, length)
    }

    fn get_range_into(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &mut Vec<u8>,
    ) -> Result<bool> {
        self.note("get_range", key);
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

    fn create_dir(&self, key: &str) -> Result<()> {
        self.note("create_dir", key);
        self.inner.create_dir(key)
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.note("put", key);
        self.inner.put_owned(key, data)
    }

    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        self.note("put", key);
        self.inner.put_pack(key, data)
    }

    fn begin_staged_put(
        &self,
        prefix: &str,
    ) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        self.note("begin_staged_put", prefix);
        Ok(None)
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        self.note("size", key);
        self.inner.size(key)
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.note("list", prefix);
        self.inner.list_sized(prefix)
    }

    fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.note("list", prefix);
        self.inner.list_meta(prefix)
    }

    fn remove_stale_temp_files(&self, older_than: std::time::Duration) -> Result<usize> {
        self.note("remove_stale_temp_files", "");
        self.inner.remove_stale_temp_files(older_than)
    }

    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        None
    }

    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        None
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            staged_put: false,
            ..self.inner.profile()
        }
    }
}

fn assert_refused(err: &VykarError, what: &str) {
    let text = err.to_string();
    assert!(
        text.contains("no longer supports unencrypted repositories")
            && text.contains("new repository"),
        "{what}: {text}"
    );
}

fn count_files(dir: &Path) -> usize {
    walk(dir).len()
}

fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    rd.flat_map(|e| {
        let p = e.unwrap().path();
        if p.is_dir() { walk(&p) } else { vec![p] }
    })
    .collect()
}

fn open_err(config: &VykarConfig) -> VykarError {
    match open_repo(config, Some(PASS), OpenOptions::new(), &CancelToken::new()) {
        Ok(_) => panic!("open unexpectedly succeeded"),
        Err(e) => e,
    }
}

fn try_backup(config: &VykarConfig, src: &Path) -> std::result::Result<(), VykarError> {
    let source_paths = vec![src.to_string_lossy().to_string()];
    commands::backup::run(
        config,
        commands::backup::BackupRequest {
            snapshot_name: "snap",
            passphrase: Some(PASS),
            source_paths: &source_paths,
            source_label: "src",
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
    .map(|_| ())
}

/// The frozen unencrypted v2/v3 fixtures: open reads only `config`, and
/// open, upgrade and key export are refused; the repository is unchanged
/// (no `locks/` or `sessions/` file appears).
#[test]
fn none_fixtures_are_refused_and_nothing_is_written() {
    for (prefix, version) in [("v2", 2u64), ("v3", 3)] {
        let fx = fixtures::extract(prefix, "none", version);
        let repo_dir = fx.repo_dir();
        let before = read_tree(&repo_dir);

        let calls: Arc<Mutex<Vec<String>>> = Arc::default();
        let storage = CallLog {
            inner: LocalBackend::new(repo_dir.to_str().unwrap()).unwrap(),
            calls: calls.clone(),
        };
        let err = Repo::open(
            Box::new(storage),
            None,
            Some(fx.cache_dir()),
            OpenOptions::new(),
        )
        .err()
        .expect("open is refused");
        assert_refused(&err, &format!("{prefix} open"));
        assert_eq!(*calls.lock().unwrap(), vec!["get config".to_string()]);

        let err = commands::upgrade::run(&fx.config(), None).unwrap_err();
        assert_refused(&err, &format!("{prefix} upgrade"));
        let err = commands::key::export(&fx.config(), None)
            .err()
            .expect("key export is refused");
        assert_refused(&err, &format!("{prefix} key export"));

        assert_eq!(read_tree(&repo_dir), before, "{prefix}");
    }
}

/// A v6 map config saying mode none is refused by every open, `--trust-repo`
/// too. A backup registers its session marker before it opens and removes it
/// when the open is refused: no pack, snapshot or marker remains.
#[test]
fn a_v6_none_config_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.passphrase = Some(PASS.into());
    config.cache_dir = Some(tmp.path().join("cache").to_string_lossy().into_owned());
    common::init_repo(&config);

    let path = repo_dir.join("config");
    let mut stored = RepoConfig::decode(&std::fs::read(&path).unwrap())
        .unwrap()
        .into_repo_config();
    stored.encryption = EncryptionMode::None;
    std::fs::write(&path, stored.encode().unwrap()).unwrap();

    assert_refused(&open_err(&config), "open");
    let mut trusting = config.clone();
    trusting.trust_repo = true;
    assert_refused(&open_err(&trusting), "open with trust_repo");

    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("secret.txt"), b"DOWNGRADE-REPRO-MARKER").unwrap();
    let err = try_backup(&config, &src).unwrap_err();
    assert_refused(&err, "backup");
    assert_eq!(count_files(&repo_dir.join("packs")), 0);
    assert_eq!(count_files(&repo_dir.join("snapshots")), 0);
    assert_eq!(count_files(&repo_dir.join("sessions")), 0);
}

#[test]
fn encrypting_config_opens_encrypted_repo() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Auto;
    config.cache_dir = Some(tmp.path().join("cache").to_string_lossy().into_owned());
    vykar_core::testing::with_cheap_kdf(|| commands::init::run(&config, Some(PASS))).unwrap();
    open_repo(&config, Some(PASS), OpenOptions::new(), &CancelToken::new()).unwrap();
}
