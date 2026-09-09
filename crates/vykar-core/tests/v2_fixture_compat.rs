//! Compatibility tests against **frozen** repository-format-v2 fixtures.
//!
//! The archives under `tests/fixtures/` were written by a binary that predates
//! BLAKE3 (see `scripts/gen-v2-fixtures.sh` for the exact commit). Creating a
//! v2 repository with today's code and reading it back would prove very
//! little — a regression shared by the writer and the reader is invisible to
//! that. These fixtures are the actual compatibility evidence, so **do not
//! regenerate them**: a failure here means today's binary can no longer read
//! repositories that exist in the wild.

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
// Test-only: `panic!` in a fixture-loading helper is the right failure mode,
// and `meta["key"]` indexing a `serde_json::Value` panics on a malformed
// fixture, which is exactly what should happen.
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use vykar_core::commands;
use vykar_core::config::{EncryptionModeConfig, VykarConfig};
use vykar_core::repo::file_cache::FileCache;
use vykar_core::repo::{identity, EncryptionMode, OpenOptions, Repository};
use vykar_storage::local_backend::LocalBackend;
use vykar_types::error::VykarError;

use crate::common::{backup_source, exercise_pack_naming, make_test_config};
use vykar_types::hash::HashAlgorithm;

/// One fixture per encryption mode.
const MODES: &[&str] = &["none", "aes256gcm", "chacha20poly1305"];

struct Fixture {
    /// Kept alive for the duration of the test.
    _tmp: tempfile::TempDir,
    root: PathBuf,
    mode: String,
    passphrase: Option<String>,
    snapshots: Vec<String>,
    /// The identity fingerprint the pre-change binary computed for this repo.
    pin_fingerprint: String,
}

impl Fixture {
    fn repo_dir(&self) -> PathBuf {
        self.root.join("repo")
    }
    fn source_dir(&self) -> PathBuf {
        self.root.join("source")
    }
    fn cache_dir(&self) -> PathBuf {
        self.root.join("cache")
    }
    fn passphrase(&self) -> Option<&str> {
        self.passphrase.as_deref()
    }
    /// The snapshot the fixture's `source/` tree corresponds to (the last one).
    fn latest_snapshot(&self) -> &str {
        self.snapshots.last().unwrap()
    }

    fn config(&self) -> VykarConfig {
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

    fn expected_encryption(&self) -> EncryptionMode {
        match self.mode.as_str() {
            "none" => EncryptionMode::None,
            "aes256gcm" => EncryptionMode::Aes256Gcm,
            "chacha20poly1305" => EncryptionMode::Chacha20Poly1305,
            other => panic!("unknown fixture encryption mode {other}"),
        }
    }

    fn open(&self) -> Repository {
        let storage = Box::new(LocalBackend::new(self.repo_dir().to_str().unwrap()).unwrap());
        Repository::open(
            storage,
            self.passphrase(),
            Some(self.cache_dir()),
            OpenOptions::new().with_index(),
        )
        .unwrap()
    }
}

fn extract(mode: &str) -> Fixture {
    common::init_test_environment();

    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(format!("v2-{mode}.tar.gz"));
    let tmp = tempfile::tempdir().unwrap();
    let file = std::fs::File::open(&archive)
        .unwrap_or_else(|e| panic!("open fixture {}: {e}", archive.display()));
    tar::Archive::new(flate2::read::GzDecoder::new(file))
        .unpack(tmp.path())
        .unwrap();

    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(tmp.path().join("meta.json")).unwrap()).unwrap();
    assert_eq!(meta["format_version"].as_u64(), Some(2));

    let root = tmp.path().to_path_buf();
    Fixture {
        _tmp: tmp,
        root,
        mode: mode.to_string(),
        passphrase: meta["passphrase"].as_str().map(str::to_string),
        snapshots: meta["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect(),
        pin_fingerprint: meta["pin_fingerprint"].as_str().unwrap().to_string(),
    }
}

fn digest_hex(data: &[u8]) -> String {
    use blake2::digest::{Update, VariableOutput};
    let mut hasher = blake2::Blake2bVar::new(32).unwrap();
    hasher.update(data);
    let mut out = [0u8; 32];
    hasher.finalize_variable(&mut out).unwrap();
    hex::encode(out)
}

/// Every regular file under `root`, keyed by its path relative to `root` and
/// summarised as `(length, BLAKE2b-256 hex)` so a mismatch prints a diff a
/// human can read rather than megabytes of bytes.
fn read_tree(root: &Path) -> BTreeMap<PathBuf, (u64, String)> {
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

// ── Reading ────────────────────────────────────────────────────────────────

#[test]
fn v2_fixtures_open_as_format_v2() {
    for mode in MODES {
        let fx = extract(mode);
        let repo = fx.open();
        assert_eq!(
            repo.config.version, 2,
            "{mode}: fixture must stay at format v2"
        );
        assert_eq!(repo.config.encryption, fx.expected_encryption(), "{mode}");
        assert_eq!(
            repo.manifest().snapshots.len(),
            fx.snapshots.len(),
            "{mode}: all fixture snapshots must be listable"
        );
        assert!(
            repo.skipped_snapshots().is_empty(),
            "{mode}: no snapshot may be unreadable"
        );
    }
}

#[test]
fn v2_fixtures_restore_byte_identical() {
    for mode in MODES {
        let fx = extract(mode);
        let config = fx.config();
        let dest = fx.root.join("restore-original");

        commands::restore::run(
            &config,
            fx.passphrase(),
            fx.latest_snapshot(),
            dest.to_str().unwrap(),
            None,
            config.xattrs.enabled,
            true, // verify chunk IDs: the point of the exercise
        )
        .unwrap();

        assert_eq!(
            read_tree(&dest),
            read_tree(&fx.source_dir()),
            "{mode}: restored tree differs from the tree that was backed up"
        );
    }
}

#[test]
fn v2_fixtures_verify_data_is_clean() {
    for mode in MODES {
        let fx = extract(mode);
        let result = commands::check::run(&fx.config(), fx.passphrase(), true, false).unwrap();
        assert!(
            result.errors.is_empty(),
            "{mode}: check --verify-data reported {:?}",
            result.errors
        );
    }
}

// ── Writing ────────────────────────────────────────────────────────────────

#[test]
fn v2_fixtures_accept_a_new_backup_and_still_dedup() {
    for mode in MODES {
        let fx = extract(mode);
        let config = fx.config();

        let before = fx.open().chunk_index().len();

        // Back up the exact tree the last fixture snapshot already contains.
        // File data must dedup completely against chunks written by the
        // pre-change binary; only snapshot metadata is genuinely new.
        let stats = backup_source(
            &config,
            &fx.source_dir(),
            "fixture-src",
            "post-change-snap",
            fx.passphrase(),
            config.xattrs.enabled,
        );

        assert!(
            stats.original_size > 1024 * 1024,
            "{mode}: expected a non-trivial source, got {} bytes",
            stats.original_size
        );
        assert!(
            stats.deduplicated_size < stats.original_size / 8,
            "{mode}: {} of {} bytes were newly stored — dedup against the \
             pre-change chunk IDs is broken",
            stats.deduplicated_size,
            stats.original_size
        );

        let after = fx.open().chunk_index().len();
        assert!(
            after >= before,
            "{mode}: chunk index shrank ({before} -> {after})"
        );
        assert!(
            after - before < before,
            "{mode}: chunk index nearly doubled ({before} -> {after}) — the \
             new backup re-stored content instead of deduplicating"
        );
    }
}

/// Packs written into a v2 repository today — by backup and by repack — must
/// still be named by BLAKE2b, or a v2-era binary could not locate them.
#[test]
fn v2_fixtures_name_new_packs_by_blake2b() {
    for mode in MODES {
        let fx = extract(mode);
        let config = fx.config();
        exercise_pack_naming(
            &config,
            fx.passphrase(),
            &fx.source_dir(),
            HashAlgorithm::Blake2b,
        );
    }
}

#[test]
fn v2_fixtures_survive_delete_prune_compact_and_restore() {
    for mode in MODES {
        let fx = extract(mode);
        let config = fx.config();

        // Add a snapshot so deleting one still leaves something to restore.
        backup_source(
            &config,
            &fx.source_dir(),
            "fixture-src",
            "post-change-snap",
            fx.passphrase(),
            config.xattrs.enabled,
        );

        let oldest = fx.snapshots.first().unwrap().clone();
        let deleted =
            commands::delete::run(&config, fx.passphrase(), &[&oldest], false, None).unwrap();
        assert!(
            deleted.warnings.is_empty(),
            "{mode}: {:?}",
            deleted.warnings
        );

        // Prune refuses to run without a retention rule; keep everything that
        // is left so this step exercises the code path without deleting more.
        let mut prune_config = config.clone();
        prune_config.retention.keep_last = Some(10);
        commands::prune::run(&prune_config, fx.passphrase(), false, false, &[], &[], None).unwrap();

        // Threshold 0.0 so compaction actually repacks the holes delete left.
        commands::compact::run(&config, fx.passphrase(), 0.0, None, false, None).unwrap();

        let dest = fx.root.join("restore-after-maintenance");
        commands::restore::run(
            &config,
            fx.passphrase(),
            "post-change-snap",
            dest.to_str().unwrap(),
            None,
            config.xattrs.enabled,
            true,
        )
        .unwrap();
        assert_eq!(
            read_tree(&dest),
            read_tree(&fx.source_dir()),
            "{mode}: restore after delete/prune/compact is not byte-identical"
        );

        let result = commands::check::run(&config, fx.passphrase(), true, false).unwrap();
        assert!(
            result.errors.is_empty(),
            "{mode}: check --verify-data after maintenance reported {:?}",
            result.errors
        );
    }
}

/// The v2 half of the `{none, aes256gcm} x {v2, v3}` lifecycle matrix that
/// `lifecycle_integration.rs` covers for v3 (every repo it creates is v3 now).
/// The encrypted fixtures must still reject a wrong passphrase the same way.
#[test]
fn v2_encrypted_fixtures_reject_a_wrong_passphrase() {
    for mode in MODES {
        let fx = extract(mode);
        if fx.passphrase().is_none() {
            continue; // plaintext fixture: no passphrase to get wrong
        }
        let config = fx.config();
        let wrong = Some("definitely-not-the-fixture-passphrase");

        let storage = Box::new(LocalBackend::new(fx.repo_dir().to_str().unwrap()).unwrap());
        assert!(
            matches!(
                Repository::open(storage, wrong, None, OpenOptions::new()),
                Err(VykarError::DecryptionFailed)
            ),
            "{mode}: opening with a wrong passphrase must fail"
        );

        assert!(
            matches!(
                commands::restore::run(
                    &config,
                    wrong,
                    fx.latest_snapshot(),
                    fx.root.join("bad-restore").to_str().unwrap(),
                    None,
                    config.xattrs.enabled,
                    false,
                ),
                Err(VykarError::DecryptionFailed)
            ),
            "{mode}: restoring with a wrong passphrase must fail"
        );

        assert!(
            matches!(
                commands::check::run(&config, wrong, true, false),
                Err(VykarError::DecryptionFailed)
            ),
            "{mode}: check with a wrong passphrase must fail"
        );
    }
}

// ── Local state written by the pre-change binary ───────────────────────────

/// The pin file name is a cache key over the repository URL, which necessarily
/// differs between fixture-generation time and test time. Reproduced here from
/// `identity::pin_file_path`; the test below proves the derivation still names
/// the file the implementation reads.
fn pin_file_name(url: &str) -> String {
    use blake2::digest::{Update, VariableOutput};
    let mut hasher = blake2::Blake2bVar::new(32).unwrap();
    hasher.update(url.as_bytes());
    let mut hash = [0u8; 32];
    hasher.finalize_variable(&mut hash).unwrap();
    format!("pin.{}", hex::encode(hash))
}

#[test]
fn v2_fixture_pin_files_still_validate() {
    for mode in MODES {
        let fx = extract(mode);
        let repo = fx.open();
        let url = fx.repo_dir().to_string_lossy().into_owned();

        // The fingerprint is BLAKE2b(repo_id || chunk_id_key) and stays
        // BLAKE2b for every repository format. For the plaintext fixture it
        // also pins the derived chunk-ID key, which format v3 changes.
        let fingerprint =
            identity::compute_fingerprint(&repo.config.id, repo.crypto.chunk_hasher().key());
        assert_eq!(
            hex::encode(fingerprint),
            fx.pin_fingerprint,
            "{mode}: identity fingerprint drifted from the pre-change binary"
        );

        // Move the pre-change pin file to the name this repo URL hashes to,
        // then let the real code path validate it.
        let cache = fx.cache_dir();
        let original = std::fs::read_dir(&cache)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("pin."))
            })
            .unwrap_or_else(|| panic!("{mode}: fixture has no pin file"));
        let target = cache.join(pin_file_name(&url));
        std::fs::rename(&original, &target).unwrap();

        identity::verify_or_pin(
            &url,
            &repo.config.id,
            repo.crypto.chunk_hasher().key(),
            Some(&cache),
            false, // no --trust-repo
        )
        .unwrap_or_else(|e| panic!("{mode}: pre-change pin file rejected: {e}"));

        // Closing the loop: corrupt the file we just placed and confirm the
        // same call now fails. Without this, a wrong `pin_file_name` would
        // silently make the assertion above a TOFU no-op.
        let mut pin: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&target).unwrap()).unwrap();
        pin["fingerprint"] = serde_json::Value::String("0".repeat(64));
        std::fs::write(&target, serde_json::to_vec(&pin).unwrap()).unwrap();
        assert!(
            identity::verify_or_pin(
                &url,
                &repo.config.id,
                repo.crypto.chunk_hasher().key(),
                Some(&cache),
                false,
            )
            .is_err(),
            "{mode}: verify_or_pin does not read the pin file this test writes"
        );
    }
}

#[test]
fn v2_fixture_file_caches_still_load() {
    for mode in MODES {
        let fx = extract(mode);
        let repo = fx.open();
        let cache = fx.cache_dir();

        // The file cache is keyed by repo_id, so its path is portable as-is.
        assert!(
            cache
                .join(hex::encode(&repo.config.id))
                .join("filecache")
                .exists(),
            "{mode}: fixture is missing the pre-change file cache"
        );

        let loaded = FileCache::load(&repo.config.id, repo.crypto.as_ref(), Some(&cache));
        assert!(
            !loaded.is_empty(),
            "{mode}: pre-change file cache loaded as empty — the on-disk \
             format or its AEAD context changed"
        );
    }
}
