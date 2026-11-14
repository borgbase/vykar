//! `restore` with `continue_on_error`: files whose data cannot be read are
//! omitted and reported, every other file is restored byte-identical, and a
//! damaged path never appears in `dest` (not even as an empty placeholder).
//! Strict mode on the same repository aborts with `RestoreSourceUnreadable`.

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
#![allow(clippy::indexing_slicing, clippy::panic)]

mod common;

use std::path::{Path, PathBuf};

use vykar_core::commands;
use vykar_core::commands::restore::{RestoreOptions, RestoreStats};
use vykar_core::config::{EncryptionModeConfig, VykarConfig};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use crate::common::{backup_source, make_test_config, open_local_repo};

const PASS: &str = "continue-on-error-pass";

/// Deterministic incompressible bytes (xorshift64).
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ seed.wrapping_mul(0xA24B_AED4_963E_E407);
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

struct Fixture {
    _tmp: tempfile::TempDir,
    config: VykarConfig,
    repo_dir: PathBuf,
    src_dir: PathBuf,
    dest: PathBuf,
    passphrase: Option<&'static str>,
}

impl Fixture {
    /// Back up `files` (name, seed) from a fresh source dir. `extra` runs on
    /// the source dir before the backup (e.g. to add hard links).
    fn new(encrypted: bool, files: &[(&str, u64)], extra: impl FnOnce(&Path)) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        let src_dir = tmp.path().join("src");
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&src_dir).unwrap();
        for (name, seed) in files {
            std::fs::write(src_dir.join(name), noise(*seed, 64 * 1024)).unwrap();
        }
        extra(&src_dir);

        let mut config = make_test_config(&repo_dir);
        let passphrase = if encrypted {
            config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
            Some(PASS)
        } else {
            None
        };
        commands::init::run(&config, passphrase).unwrap();
        backup_source(&config, &src_dir, "src", "snap", passphrase, false);
        let dest = tmp.path().join("restore");
        Self {
            _tmp: tmp,
            config,
            repo_dir,
            src_dir,
            dest,
            passphrase,
        }
    }

    /// Snapshot-relative path and single chunk of the file named `name`.
    fn file_chunk(&self, name: &str) -> (PathBuf, ChunkId) {
        let items =
            commands::list::list_snapshot_items(&self.config, self.passphrase, "snap").unwrap();
        let item = items.iter().find(|i| i.path.ends_with(name)).unwrap();
        assert_eq!(item.chunks.len(), 1, "fixture files are a single chunk");
        (PathBuf::from(&item.path), item.chunks[0].id)
    }

    fn flip_byte_in_chunk(&self, id: &ChunkId) {
        let repo = open_local_repo(&self.repo_dir, self.passphrase);
        let entry = *repo.chunk_index().get(id).unwrap();
        let path = self.repo_dir.join(entry.pack_id.storage_key());
        let mut pack = std::fs::read(&path).unwrap();
        pack[entry.pack_offset as usize + entry.stored_size as usize / 2] ^= 0x01;
        std::fs::write(&path, pack).unwrap();
    }

    fn restore(&self, continue_on_error: bool) -> Result<RestoreStats> {
        commands::restore::run_with_options(
            &self.config,
            self.passphrase,
            "snap",
            self.dest.to_str().unwrap(),
            None,
            RestoreOptions {
                continue_on_error,
                ..Default::default()
            },
            None,
        )
    }

    fn assert_restored(&self, rel: &Path, name: &str) {
        assert_eq!(
            std::fs::read(self.dest.join(rel)).unwrap(),
            std::fs::read(self.src_dir.join(name)).unwrap(),
            "{name} must be restored byte-identical"
        );
    }

    fn assert_absent(&self, rel: &Path) {
        assert!(
            std::fs::symlink_metadata(self.dest.join(rel)).is_err(),
            "{} must not exist in dest (no placeholder either)",
            rel.display()
        );
    }

    /// No regular file anywhere under `dest`, and no staging dir left.
    fn assert_no_files_published(&self) {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            for e in std::fs::read_dir(dir).unwrap().flatten() {
                let ft = e.file_type().unwrap();
                if ft.is_dir() {
                    walk(&e.path(), out);
                } else {
                    out.push(e.path());
                }
            }
        }
        let mut found = Vec::new();
        if self.dest.exists() {
            walk(&self.dest, &mut found);
        }
        assert!(found.is_empty(), "nothing may be published: {found:?}");
        self.assert_no_staging();
    }

    fn assert_no_staging(&self) {
        let leftovers: Vec<_> = std::fs::read_dir(&self.dest)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".vykar-restore-"))
            .collect();
        assert!(leftovers.is_empty(), "staging dir left: {leftovers:?}");
    }
}

fn damaged_paths(stats: &RestoreStats) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = stats.damaged.iter().map(|d| d.path.clone()).collect();
    v.sort();
    v
}

const FILES: &[(&str, u64)] = &[("a.bin", 1), ("b.bin", 2), ("c.bin", 3)];

fn corrupt_chunk_restores_the_rest(encrypted: bool) {
    let fx = Fixture::new(encrypted, FILES, |_| {});
    let (b_rel, b_chunk) = fx.file_chunk("b.bin");
    fx.flip_byte_in_chunk(&b_chunk);

    let stats = fx.restore(true).unwrap();
    assert_eq!(damaged_paths(&stats), vec![b_rel.clone()]);
    assert_eq!(stats.damaged_total, 1);
    assert_eq!(stats.files, 2);
    assert_eq!(stats.total_bytes, 2 * 64 * 1024);
    fx.assert_absent(&b_rel);
    for name in ["a.bin", "c.bin"] {
        fx.assert_restored(&fx.file_chunk(name).0, name);
    }
    fx.assert_no_staging();
}

#[test]
fn plaintext_corrupt_chunk_restores_the_rest() {
    corrupt_chunk_restores_the_rest(false);
}

#[test]
fn encrypted_corrupt_chunk_restores_the_rest() {
    corrupt_chunk_restores_the_rest(true);
}

#[test]
fn deleted_pack_damages_every_file_in_it() {
    let fx = Fixture::new(false, FILES, |_| {});
    let (a_rel, a_chunk) = fx.file_chunk("a.bin");
    let pack = {
        let repo = open_local_repo(&fx.repo_dir, None);
        repo.chunk_index().get(&a_chunk).unwrap().pack_id
    };
    std::fs::remove_file(fx.repo_dir.join(pack.storage_key())).unwrap();

    let stats = fx.restore(true).unwrap();
    assert!(damaged_paths(&stats).contains(&a_rel));
    for d in &stats.damaged {
        assert!(d.reason.contains("pack not found"), "got: {}", d.reason);
        fx.assert_absent(&d.path);
    }
    assert_eq!(stats.files + stats.damaged_total, FILES.len() as u64);
    fx.assert_no_staging();
}

#[test]
fn chunk_missing_from_index_is_damage() {
    let fx = Fixture::new(false, FILES, |_| {});
    let (b_rel, b_chunk) = fx.file_chunk("b.bin");
    {
        let mut repo = open_local_repo(&fx.repo_dir, None);
        repo.chunk_index_mut().remove(&b_chunk).unwrap();
        repo.save_state().unwrap();
    }

    // Strict: aborts, keeping the original variant inside the wrapper.
    let err = fx.restore(false).unwrap_err();
    assert!(
        matches!(&err, VykarError::RestoreSourceUnreadable(inner)
            if matches!(**inner, VykarError::ChunkNotInIndex(id) if id == b_chunk)),
        "got: {err:?}"
    );
    fx.assert_no_files_published();

    let stats = fx.restore(true).unwrap();
    assert_eq!(damaged_paths(&stats), vec![b_rel.clone()]);
    assert!(stats.damaged[0].reason.contains("chunk not found in index"));
    fx.assert_absent(&b_rel);
    fx.assert_restored(&fx.file_chunk("a.bin").0, "a.bin");
    fx.assert_restored(&fx.file_chunk("c.bin").0, "c.bin");
}

#[cfg(unix)]
#[test]
fn damaged_hardlink_representative_drops_the_whole_group() {
    let fx = Fixture::new(false, &[("h1.bin", 7), ("ok.bin", 8)], |src| {
        std::fs::hard_link(src.join("h1.bin"), src.join("h2.bin")).unwrap();
        std::fs::hard_link(src.join("h1.bin"), src.join("h3.bin")).unwrap();
    });
    let intact = commands::restore::run(
        &fx.config,
        None,
        "snap",
        fx.dest.with_extension("intact").to_str().unwrap(),
        None,
        false,
        false,
        None,
    )
    .unwrap();
    assert_eq!(intact.hardlinks, 2, "fixture must restore as a link group");

    let (_, chunk) = fx.file_chunk("h1.bin");
    fx.flip_byte_in_chunk(&chunk);

    let stats = fx.restore(true).unwrap();
    let damaged = damaged_paths(&stats);
    assert_eq!(
        damaged.len(),
        3,
        "representative and both links: {damaged:?}"
    );
    for name in ["h1.bin", "h2.bin", "h3.bin"] {
        let rel = fx.file_chunk(name).0;
        assert!(damaged.contains(&rel), "{name} not reported: {damaged:?}");
        fx.assert_absent(&rel);
    }
    assert_eq!(stats.hardlinks, 0);
    fx.assert_restored(&fx.file_chunk("ok.bin").0, "ok.bin");
    fx.assert_no_staging();
}

#[test]
fn strict_mode_wraps_source_failure_and_publishes_nothing() {
    let fx = Fixture::new(false, FILES, |_| {});
    let (_, b_chunk) = fx.file_chunk("b.bin");
    fx.flip_byte_in_chunk(&b_chunk);

    let err = fx.restore(false).unwrap_err();
    let VykarError::RestoreSourceUnreadable(inner) = &err else {
        panic!("expected RestoreSourceUnreadable, got: {err:?}");
    };
    assert!(
        matches!(&**inner, VykarError::InvalidFormat(m) if m.contains("hash mismatch")),
        "inner variant must be preserved, got: {inner:?}"
    );
    let source = std::error::Error::source(&err).expect("inner error is the source");
    assert_eq!(source.to_string(), inner.to_string());
    let msg = err.to_string();
    assert!(msg.contains("hash mismatch"), "got: {msg}");
    assert!(
        msg.contains("no restored files were published"),
        "got: {msg}"
    );
    fx.assert_no_files_published();
}

#[test]
/// Only failures reading repository data are wrapped. A destination rejected
/// before any read stays its own error; the worker-phase mapping is unit-tested
/// as `restore_error_wraps_only_source_failures`.
fn strict_mode_does_not_wrap_destination_preflight_errors() {
    let fx = Fixture::new(false, FILES, |_| {});
    std::fs::create_dir_all(&fx.dest).unwrap();
    std::fs::write(fx.dest.join("occupied"), b"x").unwrap();
    let err = fx.restore(false).unwrap_err();
    assert!(matches!(err, VykarError::Config(_)), "got: {err:?}");
}
