//! The repository `config` is unauthenticated, so whoever controls storage can
//! swap in an `encryption: none` repository. A client whose own config asks for
//! encryption must refuse it rather than silently upload plaintext (#208).

#![allow(clippy::unwrap_used, clippy::panic)]
#![allow(clippy::pedantic)]

mod common;

use std::path::Path;

use vykar_core::commands;
use vykar_core::commands::util::open_repo;
use vykar_core::compress::Compression;
use vykar_core::config::{EncryptionModeConfig, VykarConfig};
use vykar_core::repo::OpenOptions;
use vykar_types::error::VykarError;

use crate::common::make_test_config;

const PASS: &str = "correct horse battery staple";

/// Init a plaintext repo the way a storage operator would, and return a client
/// config for the same URL with `mode`.
fn plaintext_repo(tmp: &Path, mode: EncryptionModeConfig) -> VykarConfig {
    let repo_dir = tmp.join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    commands::init::run(&make_test_config(&repo_dir), None).unwrap();

    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = mode;
    // Fresh cache dir: no identity pin for this URL.
    config.cache_dir = Some(tmp.join("client-cache").to_string_lossy().into_owned());
    config
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
        if p.is_dir() {
            walk(&p)
        } else {
            vec![p]
        }
    })
    .collect()
}

fn open_err(config: &VykarConfig) -> VykarError {
    match open_repo(config, Some(PASS), OpenOptions::new()) {
        Ok(_) => panic!("open unexpectedly succeeded"),
        Err(e) => e,
    }
}

fn try_backup(config: &VykarConfig, src: &Path) -> Result<(), VykarError> {
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

#[test]
fn encrypting_config_refuses_plaintext_repo_on_open() {
    for mode in [
        EncryptionModeConfig::Auto,
        EncryptionModeConfig::Aes256Gcm,
        EncryptionModeConfig::Chacha20Poly1305,
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let config = plaintext_repo(tmp.path(), mode);
        let err = open_err(&config);
        assert!(
            matches!(err, VykarError::PlaintextRepository(_)),
            "{mode:?}: {err}"
        );
    }
}

#[test]
fn encrypting_config_refuses_plaintext_backup() {
    let tmp = tempfile::tempdir().unwrap();
    let config = plaintext_repo(tmp.path(), EncryptionModeConfig::Auto);
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("secret.txt"), b"DOWNGRADE-REPRO-MARKER").unwrap();

    let err = try_backup(&config, &src).unwrap_err();
    assert!(matches!(err, VykarError::PlaintextRepository(_)), "{err}");

    let repo_dir = tmp.path().join("repo");
    assert_eq!(count_files(&repo_dir.join("packs")), 0);
    assert_eq!(count_files(&repo_dir.join("snapshots")), 0);
}

#[test]
fn trust_repo_does_not_waive_encryption_requirement() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = plaintext_repo(tmp.path(), EncryptionModeConfig::Auto);
    config.trust_repo = true;
    let err = open_err(&config);
    assert!(matches!(err, VykarError::PlaintextRepository(_)), "{err}");
}

#[test]
fn plaintext_config_opens_plaintext_repo() {
    let tmp = tempfile::tempdir().unwrap();
    let config = plaintext_repo(tmp.path(), EncryptionModeConfig::None);
    open_repo(&config, None, OpenOptions::new()).unwrap();
}

#[test]
fn encrypting_config_opens_encrypted_repo() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Auto;
    config.cache_dir = Some(tmp.path().join("cache").to_string_lossy().into_owned());
    commands::init::run(&config, Some(PASS)).unwrap();
    open_repo(&config, Some(PASS), OpenOptions::new()).unwrap();
}
