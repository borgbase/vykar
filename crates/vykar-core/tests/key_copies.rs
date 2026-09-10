//! Repository-key redundancy: two copies, their diagnosis at open, what
//! `check --repair` heals, and what `prove_key` refuses.

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
#![allow(clippy::panic, clippy::indexing_slicing)]

mod common;

use std::path::{Path, PathBuf};

use vykar_core::commands;
use vykar_core::commands::check::{RepairAction, RepairMode};
use vykar_core::commands::key::KeyExport;
use vykar_core::config::{EncryptionModeConfig, VykarConfig};
use vykar_core::repo::identity::{self, KeyProof};
use vykar_core::repo::{
    EncryptionMode, OpenOptions, RepoConfig, RepoFormat, Repository, KEY_PRIMARY, KEY_SECONDARY,
};
use vykar_crypto::key::MasterKey;
use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend};
use vykar_types::error::VykarError;

use crate::common::{backup_source, make_test_config};

const PASS: &str = "key-copies-test-passphrase";

struct Fixture {
    _tmp: tempfile::TempDir,
    repo_dir: PathBuf,
    source_dir: PathBuf,
    cache_dir: PathBuf,
    config: VykarConfig,
}

impl Fixture {
    /// An encrypted repository with one snapshot, its own cache directory (so
    /// the identity pin is isolated from other tests).
    fn new() -> Self {
        Self::build(true)
    }

    /// Same, but with no snapshot — `index` is still written by `init`.
    fn empty() -> Self {
        Self::build(false)
    }

    fn build(with_snapshot: bool) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        let source_dir = tmp.path().join("source");
        let cache_dir = tmp.path().join("cache");
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::write(source_dir.join("a.txt"), b"hello key redundancy").unwrap();

        let mut config = make_test_config(&repo_dir);
        config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
        config.cache_dir = Some(cache_dir.to_string_lossy().into_owned());

        commands::init::run(&config, Some(PASS)).unwrap();
        if with_snapshot {
            backup_source(&config, &source_dir, "src", "snap1", Some(PASS), false);
        }

        Self {
            _tmp: tmp,
            repo_dir,
            source_dir,
            cache_dir,
            config,
        }
    }

    fn primary(&self) -> PathBuf {
        self.repo_dir.join(KEY_PRIMARY)
    }
    fn secondary(&self) -> PathBuf {
        self.repo_dir.join(KEY_SECONDARY)
    }
    fn read_primary(&self) -> Vec<u8> {
        std::fs::read(self.primary()).unwrap()
    }

    fn open(&self) -> vykar_types::error::Result<Repository> {
        let storage = Box::new(LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap());
        Repository::open(
            storage,
            Some(PASS),
            Some(self.cache_dir.clone()),
            OpenOptions::new(),
        )
    }

    /// Open through an arbitrary backend — used to inject storage failures
    /// deterministically, without depending on permission bits (which root
    /// bypasses).
    fn open_on(&self, storage: Box<dyn StorageBackend>) -> vykar_types::error::Result<Repository> {
        Repository::open(
            storage,
            Some(PASS),
            Some(self.cache_dir.clone()),
            OpenOptions::new(),
        )
    }

    /// A local backend that fails `get` on `unreadable` keys and `put` on
    /// `unwritable` keys with an I/O-style error.
    fn faulty(&self, unreadable: &[&str], unwritable: &[&str]) -> Box<dyn StorageBackend> {
        Box::new(FaultyBackend {
            inner: self.storage(),
            unreadable: unreadable.iter().map(|k| (*k).to_string()).collect(),
            unwritable: unwritable.iter().map(|k| (*k).to_string()).collect(),
        })
    }

    /// Open the way the CLI does: with the URL, so the identity pin is
    /// consulted during key selection.
    fn open_pinned(&self) -> vykar_types::error::Result<Repository> {
        let storage = Box::new(LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap());
        Repository::open(
            storage,
            Some(PASS),
            Some(self.cache_dir.clone()),
            OpenOptions::new().with_repo_url(self.config.repository.url.clone()),
        )
    }

    /// Point the local pin at an identity this repository does not have.
    fn stale_pin(&self) {
        let pins = self.pin_files();
        assert_eq!(pins.len(), 1, "init must have pinned the identity");
        let mut pin: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&pins[0]).unwrap()).unwrap();
        pin["fingerprint"] = serde_json::Value::String("00".repeat(32));
        std::fs::write(&pins[0], serde_json::to_string_pretty(&pin).unwrap()).unwrap();
    }

    fn open_with(&self, passphrase: &str) -> vykar_types::error::Result<Repository> {
        let storage = Box::new(LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap());
        Repository::open(
            storage,
            Some(passphrase),
            Some(self.cache_dir.clone()),
            OpenOptions::new(),
        )
    }

    fn repo_config(&self) -> RepoConfig {
        let data = std::fs::read(self.repo_dir.join("config")).unwrap();
        rmp_serde::from_slice(&data).unwrap()
    }

    fn format(&self) -> RepoFormat {
        RepoFormat::from_version(self.repo_config().version).unwrap()
    }

    fn storage(&self) -> LocalBackend {
        LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap()
    }

    /// Drop the local identity pin so `prove_key` has only data to go on.
    fn forget_pin(&self) {
        for entry in std::fs::read_dir(&self.cache_dir).unwrap().flatten() {
            if entry.file_name().to_string_lossy().starts_with("pin.") {
                std::fs::remove_file(entry.path()).unwrap();
            }
        }
    }

    fn pin_files(&self) -> Vec<PathBuf> {
        std::fs::read_dir(&self.cache_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .map(|n| n.to_string_lossy().starts_with("pin."))
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Remove every object a candidate key could be proved against.
    fn strip_provable_data(&self) {
        std::fs::remove_file(self.repo_dir.join("index")).unwrap();
        let snapshots = self.repo_dir.join("snapshots");
        if snapshots.is_dir() {
            for entry in std::fs::read_dir(&snapshots).unwrap().flatten() {
                std::fs::remove_file(entry.path()).unwrap();
            }
        }
    }
}

/// Simulates EIO on specific objects. Deterministic, unlike `chmod 000`,
/// which root ignores.
struct FaultyBackend {
    inner: LocalBackend,
    unreadable: Vec<String>,
    unwritable: Vec<String>,
}

impl InnerBackend for FaultyBackend {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for FaultyBackend;
    except [get, put];

    fn get(&self, key: &str) -> vykar_types::error::Result<Option<Vec<u8>>> {
        if self.unreadable.iter().any(|k| k == key) {
            return Err(VykarError::Io(std::io::Error::other(format!(
                "simulated I/O error reading {key}"
            ))));
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, data: &[u8]) -> vykar_types::error::Result<()> {
        if self.unwritable.iter().any(|k| k == key) {
            return Err(VykarError::Io(std::io::Error::other(format!(
                "simulated I/O error writing {key}"
            ))));
        }
        self.inner.put(key, data)
    }
}

/// Replace a repository object with a directory of the same name. Reading it
/// then fails with EISDIR for every user, root included — a deterministic
/// stand-in for an unreadable object on paths that go through the config
/// rather than an injectable backend.
fn make_unreadable_via_fs(path: &Path) {
    std::fs::remove_file(path).unwrap();
    std::fs::create_dir(path).unwrap();
}

/// A key blob from an unrelated repository, wrapped under the *same*
/// passphrase — the adversarial case that unwraps perfectly and is still not
/// this repository's key.
fn foreign_key_blob() -> Vec<u8> {
    let key = MasterKey::generate().unwrap();
    rmp_serde::to_vec(&key.to_encrypted(PASS).unwrap()).unwrap()
}

/// A foreign key wrapped under a *different* passphrase — the shape that
/// makes only one of two divergent copies unwrap.
fn foreign_key_blob_with(passphrase: &str) -> Vec<u8> {
    let key = MasterKey::generate().unwrap();
    rmp_serde::to_vec(&key.to_encrypted(passphrase).unwrap()).unwrap()
}

fn wrap_same_key_freshly(blob: &[u8]) -> Vec<u8> {
    let encrypted = vykar_crypto::key::inspect_key_blob(blob).unwrap();
    let key = MasterKey::from_encrypted(&encrypted, PASS).unwrap();
    // A fresh wrap picks a new salt and nonce, so the bytes differ while the
    // key material does not.
    rmp_serde::to_vec(&key.to_encrypted(PASS).unwrap()).unwrap()
}

fn key_dir_snapshot(repo_dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut entries: Vec<(String, Vec<u8>)> = std::fs::read_dir(repo_dir.join("keys"))
        .unwrap()
        .flatten()
        .map(|e| {
            (
                e.file_name().to_string_lossy().into_owned(),
                std::fs::read(e.path()).unwrap(),
            )
        })
        .collect();
    entries.sort();
    entries
}

// ---------------------------------------------------------------------------
// Redundancy at init, and self-heal on open
// ---------------------------------------------------------------------------

#[test]
fn init_writes_both_key_copies() {
    let fx = Fixture::empty();
    assert!(fx.primary().exists(), "{KEY_PRIMARY} must exist");
    assert!(fx.secondary().exists(), "{KEY_SECONDARY} must exist");
    assert_eq!(
        std::fs::read(fx.primary()).unwrap(),
        std::fs::read(fx.secondary()).unwrap(),
        "the two copies must be byte-identical, not independent wraps"
    );
}

#[test]
fn open_falls_back_to_the_secondary_and_restores_the_primary() {
    let fx = Fixture::new();
    let original = fx.read_primary();
    std::fs::remove_file(fx.primary()).unwrap();

    fx.open().expect("open must succeed off the redundant copy");

    assert!(fx.primary().exists(), "the primary must have been restored");
    assert_eq!(std::fs::read(fx.primary()).unwrap(), original);
}

#[test]
fn legacy_single_copy_repo_backfills_the_secondary() {
    let fx = Fixture::new();
    std::fs::remove_file(fx.secondary()).unwrap();

    fx.open().unwrap();

    assert!(fx.secondary().exists(), "the secondary must be backfilled");
    assert_eq!(std::fs::read(fx.secondary()).unwrap(), fx.read_primary());
}

#[test]
fn backfill_failure_never_fails_the_open() {
    let fx = Fixture::new();
    std::fs::remove_file(fx.secondary()).unwrap();

    // The backfill PUT cannot land.
    fx.open_on(fx.faulty(&[], &[KEY_SECONDARY]))
        .expect("a backend that refuses the write must not fail the open");
    assert!(
        !fx.secondary().exists(),
        "the backfill must have failed silently"
    );
}

#[test]
fn both_copies_missing_names_key_import() {
    let fx = Fixture::new();
    std::fs::remove_file(fx.primary()).unwrap();
    std::fs::remove_file(fx.secondary()).unwrap();

    let err = fx.open().err().expect("open must fail");
    let msg = err.to_string();
    assert!(msg.contains("repository key is missing"), "{msg}");
    assert!(msg.contains("vykar key import"), "{msg}");
}

#[test]
fn importing_the_key_back_makes_the_repository_readable_again() {
    let fx = Fixture::new();
    let blob = fx.read_primary();
    std::fs::remove_file(fx.primary()).unwrap();
    std::fs::remove_file(fx.secondary()).unwrap();
    assert!(fx.open().is_err());

    // What `vykar key import` does, at the storage level.
    std::fs::write(fx.primary(), &blob).unwrap();
    std::fs::write(fx.secondary(), &blob).unwrap();

    let repo = fx.open().expect("open must succeed once the key is back");
    assert_eq!(repo.manifest().snapshots.len(), 1);

    // And a fresh backup still round-trips through the restored key.
    backup_source(
        &fx.config,
        &fx.source_dir,
        "src",
        "snap2",
        Some(PASS),
        false,
    );
    let repo = fx.open().unwrap();
    assert_eq!(repo.manifest().snapshots.len(), 2);
}

// ---------------------------------------------------------------------------
// The headline diagnosis
// ---------------------------------------------------------------------------

/// Two intact, byte-identical copies plus a failed unwrap is *weighted*
/// evidence for a wrong passphrase — and nothing stronger. The assertion that
/// the message does **not** claim proof is the guard against that overclaim
/// creeping back in: on a CoW or deduplicating filesystem the two objects can
/// share physical extents, so one bad block corrupts both identically.
#[test]
fn matching_copies_and_a_wrong_passphrase_are_reported_as_likely_not_proven() {
    let fx = Fixture::new();
    let err = fx.open_with("not-the-passphrase").err().expect("must fail");
    let msg = err.to_string();

    assert!(
        msg.contains("likely incorrect passphrase; both key copies match"),
        "unexpected message: {msg}"
    );
    for overclaim in ["proves", "proven", "guaranteed", "certainly"] {
        assert!(
            !msg.contains(overclaim),
            "message must not claim proof (found {overclaim:?}): {msg}"
        );
    }
    assert!(
        msg.contains("strong evidence, not certainty"),
        "the hedge must be explicit: {msg}"
    );
}

#[test]
fn unambiguous_damage_is_reported_as_corruption_not_a_passphrase_problem() {
    let fx = Fixture::new();
    // Damage the msgpack framing of both copies identically.
    for path in [fx.primary(), fx.secondary()] {
        std::fs::write(path, b"this is not msgpack").unwrap();
    }
    let msg = fx.open().err().expect("must fail").to_string();
    assert!(msg.contains("corrupt"), "{msg}");
    assert!(
        !msg.contains("likely incorrect passphrase"),
        "unambiguous damage must not be blamed on the passphrase: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Divergence
// ---------------------------------------------------------------------------

#[test]
fn a_corrupt_copy_does_not_stop_the_open_and_check_reports_it() {
    let fx = Fixture::new();
    let good = fx.read_primary();
    std::fs::write(fx.secondary(), b"garbage").unwrap();

    fx.open().expect("the good copy must carry the open");

    let result = commands::check::run(&fx.config, Some(PASS), false, false).unwrap();
    assert_eq!(result.key_files_checked, Some(2));
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.context == "repository key" && e.message.contains(KEY_SECONDARY)),
        "check must report the damaged copy: {:?}",
        result.errors
    );

    // Repair heals it, and never touches the good copy.
    let repaired = commands::check::run_with_repair(
        &fx.config,
        Some(PASS),
        false,
        RepairMode::Apply,
        None,
        None,
    )
    .unwrap();
    assert!(
        repaired.applied.iter().any(|a| matches!(
            a,
            RepairAction::RestoreKeyCopy { to, .. } if to == KEY_SECONDARY
        )),
        "expected a RestoreKeyCopy action, got {:?}",
        repaired.applied
    );
    assert_eq!(std::fs::read(fx.secondary()).unwrap(), good);
    assert_eq!(fx.read_primary(), good);
}

/// A foreign key planted as the secondary unwraps under the same passphrase,
/// so "first success wins" would be a coin flip. With `index` intact the
/// genuine key is provable against repository data, so this must resolve —
/// not land in the ambiguous branch.
#[test]
fn divergent_but_resolvable_opens_off_the_real_key_and_repairs_the_other() {
    let fx = Fixture::new();
    let genuine = fx.read_primary();
    std::fs::write(fx.secondary(), foreign_key_blob()).unwrap();

    let repo = fx
        .open()
        .expect("the genuine key is provable, so this opens");
    assert_eq!(repo.manifest().snapshots.len(), 1);

    let result = commands::check::run(&fx.config, Some(PASS), false, false).unwrap();
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.message.contains(KEY_SECONDARY) && e.message.contains(KEY_PRIMARY)),
        "check must name both copies: {:?}",
        result.errors
    );

    commands::check::run_with_repair(&fx.config, Some(PASS), false, RepairMode::Apply, None, None)
        .unwrap();

    assert_eq!(
        std::fs::read(fx.secondary()).unwrap(),
        genuine,
        "the bad copy must be rewritten from the proven one"
    );
    assert_eq!(
        fx.read_primary(),
        genuine,
        "the genuine primary must never be the copy overwritten"
    );
}

/// The same planted foreign key, but with nothing left to prove either
/// candidate against. The failure belongs at **open**, not in the scan: no key
/// is selected, so there is nothing for a scan issue or a repair action to act
/// on — and writing the wrong key over the right one cannot be undone.
#[test]
fn divergent_and_unresolvable_fails_at_open_and_changes_nothing() {
    let fx = Fixture::new();
    std::fs::write(fx.secondary(), foreign_key_blob()).unwrap();
    fx.strip_provable_data();
    fx.forget_pin();

    let before = key_dir_snapshot(&fx.repo_dir);

    let msg = fx.open().err().expect("open must refuse").to_string();
    assert!(msg.contains(KEY_PRIMARY), "must name the primary: {msg}");
    assert!(
        msg.contains(KEY_SECONDARY),
        "must name the secondary: {msg}"
    );
    assert!(msg.contains("Refusing to guess"), "{msg}");

    // `check` and `check --repair` cannot get past the same open.
    assert!(commands::check::run(&fx.config, Some(PASS), false, false).is_err());
    assert!(commands::check::run_with_repair(
        &fx.config,
        Some(PASS),
        false,
        RepairMode::Apply,
        None,
        None
    )
    .is_err());

    assert_eq!(
        key_dir_snapshot(&fx.repo_dir),
        before,
        "no key file may be touched when neither candidate can be established"
    );
}

/// Two independent wraps of the *same* key: different salt and nonce, so the
/// bytes differ, but the material does not. Either copy is valid.
#[test]
fn same_key_wrapped_twice_resolves_without_complaint() {
    let fx = Fixture::new();
    let genuine = fx.read_primary();
    let rewrapped = wrap_same_key_freshly(&genuine);
    assert_ne!(rewrapped, genuine, "a fresh wrap must differ byte-wise");
    std::fs::write(fx.secondary(), &rewrapped).unwrap();

    let repo = fx.open().expect("identical key material must resolve");
    assert_eq!(repo.manifest().snapshots.len(), 1);

    commands::check::run_with_repair(&fx.config, Some(PASS), false, RepairMode::Apply, None, None)
        .unwrap();
    assert_eq!(
        std::fs::read(fx.secondary()).unwrap(),
        genuine,
        "repair canonicalizes on the primary"
    );
}

/// The same two wraps, with nothing left to prove either against: no `index`,
/// no snapshots, no pin. There is no rival candidate — both copies hold the
/// same key — so no proof may be demanded.
#[test]
fn same_key_wrapped_twice_needs_no_proof() {
    let fx = Fixture::new();
    let rewrapped = wrap_same_key_freshly(&fx.read_primary());
    std::fs::write(fx.secondary(), &rewrapped).unwrap();
    fx.strip_provable_data();
    fx.forget_pin();

    fx.open_pinned()
        .expect("identical key material must resolve without evidence");
}

/// **A stale pin is not an unresolvable repository.** Both copies unwrap to
/// different keys and the pin contradicts both: the diagnosis must name
/// `--trust-repo`, the same as it does when only one copy unwraps.
#[test]
fn stale_pin_with_divergent_copies_reports_a_mismatch_not_a_guess() {
    let fx = Fixture::new();
    std::fs::write(fx.secondary(), foreign_key_blob()).unwrap();
    fx.stale_pin();

    let err = fx
        .open_pinned()
        .err()
        .expect("a stale pin must refuse the open");
    assert!(matches!(err, VykarError::RepositoryMismatch(_)), "{err}");
    assert!(err.to_string().contains("--trust-repo"), "{err}");
    assert!(
        !err.to_string().contains("Refusing to guess"),
        "a stale pin is an identity problem, not an unresolvable one: {err}"
    );
}

// ---------------------------------------------------------------------------
// Dry run
// ---------------------------------------------------------------------------

/// `check --repair --dry-run` prints "no changes applied"; a key backfill
/// firing during the open underneath would make that false.
#[test]
fn dry_run_repair_does_not_backfill_the_missing_copy() {
    let fx = Fixture::new();
    std::fs::remove_file(fx.secondary()).unwrap();
    let before = key_dir_snapshot(&fx.repo_dir);

    commands::check::run_with_repair(
        &fx.config,
        Some(PASS),
        false,
        RepairMode::PlanOnly,
        None,
        None,
    )
    .unwrap();

    assert_eq!(
        key_dir_snapshot(&fx.repo_dir),
        before,
        "dry run must leave keys/ byte-for-byte unchanged"
    );
    assert!(!fx.secondary().exists());

    // The suppression is scoped to dry-run: an ordinary read path still heals.
    commands::info::run(&fx.config, Some(PASS)).unwrap();
    assert!(
        fx.secondary().exists(),
        "an ordinary open must still backfill"
    );
}

#[test]
fn unencrypted_repositories_skip_the_key_phase() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("a.txt"), b"plaintext").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();
    backup_source(&config, &source_dir, "src", "snap1", None, false);

    let result = commands::check::run(&config, None, false, false).unwrap();
    assert_eq!(result.key_files_checked, None);
    assert!(!repo_dir.join("keys").join("repokey").exists());
    assert!(result.errors.is_empty(), "{:?}", result.errors);
}

// ---------------------------------------------------------------------------
// prove_key: the adversarial cases
// ---------------------------------------------------------------------------

fn prove(fx: &Fixture, candidate: &MasterKey, with_pin: bool) -> KeyProof {
    let url = fx.config.repository.url.clone();
    identity::prove_key(
        &fx.storage(),
        &fx.repo_config(),
        fx.format(),
        if with_pin { Some(url.as_str()) } else { None },
        Some(&fx.cache_dir),
        candidate,
    )
    .unwrap()
}

fn master_key_of(fx: &Fixture) -> MasterKey {
    let encrypted = vykar_crypto::key::inspect_key_blob(&fx.read_primary()).unwrap();
    MasterKey::from_encrypted(&encrypted, PASS).unwrap()
}

#[test]
fn the_genuine_key_proves_against_repository_data_even_when_empty() {
    let fx = Fixture::empty();
    fx.forget_pin();
    assert_eq!(
        prove(&fx, &master_key_of(&fx), true),
        KeyProof::AuthenticatedData,
        "`init` writes an encrypted `index`, so even an empty repo is provable"
    );
}

/// A key from a *different* repository, wrapped under the same passphrase,
/// unwraps perfectly. Only `prove_key` catches it.
#[test]
fn a_foreign_key_under_the_same_passphrase_is_refused() {
    let fx = Fixture::new();
    let foreign = {
        let encrypted = vykar_crypto::key::inspect_key_blob(&foreign_key_blob()).unwrap();
        MasterKey::from_encrypted(&encrypted, PASS).unwrap()
    };

    // With this client's pin present: hard disproof.
    assert_eq!(prove(&fx, &foreign, true), KeyProof::Mismatch);

    // With no pin, but the repository's own `index` present: still refused,
    // because it cannot decrypt repository-authored ciphertext.
    fx.forget_pin();
    assert_eq!(prove(&fx, &foreign, true), KeyProof::Unproven);
}

/// **Pin precedence.** A candidate that *does* decrypt this repository's
/// `index` but contradicts an existing pin must come back `Mismatch`, not
/// `AuthenticatedData`. A data-check-first implementation passes every other
/// test here and fails only this one.
#[test]
fn a_contradicting_pin_beats_self_consistent_repository_data() {
    let fx = Fixture::new();
    let pinned_before = fx.pin_files();
    assert_eq!(pinned_before.len(), 1, "init must have pinned the identity");

    // Replace the repository at the same URL, leaving the old pin in place.
    // The new repository is internally consistent — its own key decrypts its
    // own index — and is still not the one this client trusted.
    std::fs::remove_dir_all(&fx.repo_dir).unwrap();
    std::fs::create_dir_all(&fx.repo_dir).unwrap();
    let mut fresh_config = fx.config.clone();
    fresh_config.trust_repo = false;
    // `init` re-pins unconditionally, so build the replacement without it.
    let storage = Box::new(LocalBackend::new(fx.repo_dir.to_str().unwrap()).unwrap());
    Repository::init(
        storage,
        EncryptionMode::Aes256Gcm,
        fresh_config.chunker.clone(),
        Some(PASS),
        Some(&fresh_config.repository),
        Some(fx.cache_dir.clone()),
    )
    .unwrap();

    let replacement_key = master_key_of(&fx);
    assert_eq!(
        prove(&fx, &replacement_key, false),
        KeyProof::AuthenticatedData,
        "without the pin, the replacement is self-consistent"
    );
    assert_eq!(
        prove(&fx, &replacement_key, true),
        KeyProof::Mismatch,
        "the pin must win over self-consistent repository data"
    );
}

/// Evaluating a candidate must never TOFU-pin it: that would convert a
/// rejected import into a trusted identity on any client without a pin.
#[test]
fn proving_a_key_never_writes_a_pin() {
    let fx = Fixture::new();
    fx.forget_pin();
    assert!(fx.pin_files().is_empty());

    let foreign = {
        let encrypted = vykar_crypto::key::inspect_key_blob(&foreign_key_blob()).unwrap();
        MasterKey::from_encrypted(&encrypted, PASS).unwrap()
    };
    let _ = prove(&fx, &foreign, true);
    let _ = prove(&fx, &master_key_of(&fx), true);

    assert!(
        fx.pin_files().is_empty(),
        "prove_key must use a read-only pin accessor"
    );
}

/// No pin *and* no decryptable repository object: absence of evidence, which
/// is neither proof nor disproof.
#[test]
fn a_key_with_nothing_to_prove_it_against_is_unproven() {
    let fx = Fixture::new();
    let genuine = master_key_of(&fx);
    fx.strip_provable_data();
    fx.forget_pin();

    assert_eq!(prove(&fx, &genuine, true), KeyProof::Unproven);
}

#[test]
fn a_matching_pin_alone_is_enough_when_no_data_is_readable() {
    let fx = Fixture::new();
    let genuine = master_key_of(&fx);
    fx.strip_provable_data();

    assert_eq!(prove(&fx, &genuine, true), KeyProof::PinMatch);
}

/// Counts every read `prove_key` makes against storage.
struct CountingBackend {
    inner: LocalBackend,
    reads: std::sync::atomic::AtomicUsize,
}

impl InnerBackend for CountingBackend {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for CountingBackend;
    except [get, list];

    fn get(&self, key: &str) -> vykar_types::error::Result<Option<Vec<u8>>> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.get(key)
    }

    fn list(&self, prefix: &str) -> vykar_types::error::Result<Vec<String>> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.list(prefix)
    }
}

/// A matching pin is decisive on its own: no `index` fetch, no snapshot
/// listing. A pinned candidate must not pay for repository round trips.
#[test]
fn a_matching_pin_short_circuits_before_any_storage_read() {
    let fx = Fixture::new();
    let genuine = master_key_of(&fx);
    let storage = CountingBackend {
        inner: fx.storage(),
        reads: std::sync::atomic::AtomicUsize::new(0),
    };

    let proof = identity::prove_key(
        &storage,
        &fx.repo_config(),
        fx.format(),
        Some(&fx.config.repository.url),
        Some(&fx.cache_dir),
        &genuine,
    )
    .unwrap();

    assert_eq!(proof, KeyProof::PinMatch);
    assert_eq!(
        storage.reads.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a matching pin must not consult repository data"
    );
}

// ---------------------------------------------------------------------------
// key export / key import on raw storage
// ---------------------------------------------------------------------------

/// With a matching pin, an import needs no repository data and no `--force`.
#[test]
fn a_matching_pin_authorizes_an_import_without_data_or_force() {
    let fx = Fixture::new();
    let exported = commands::key::export(&fx.config, Some(PASS)).unwrap();
    fx.strip_provable_data();
    std::fs::remove_file(fx.primary()).unwrap();
    std::fs::remove_file(fx.secondary()).unwrap();

    let report = commands::key::import(&fx.config, Some(PASS), &exported.export, false)
        .expect("a matching pin is sufficient");
    assert_eq!(report.proof, KeyProof::PinMatch);
    assert!(report.restored());
    assert!(!report.partial());
    assert_eq!(fx.read_primary(), exported.export.blob);
    assert_eq!(std::fs::read(fx.secondary()).unwrap(), exported.export.blob);
}

#[test]
fn import_refuses_an_export_for_another_repository() {
    let fx = Fixture::new();
    let other = Fixture::new();
    let foreign = commands::key::export(&other.config, Some(PASS)).unwrap();

    let err = commands::key::import(&fx.config, Some(PASS), &foreign.export, true)
        .err()
        .expect("a mismatched repository id must be refused before anything else");
    assert!(err.to_string().contains("different repository"), "{err}");
}

#[test]
fn import_rejects_a_malformed_blob_before_touching_storage() {
    let fx = Fixture::new();
    let before = key_dir_snapshot(&fx.repo_dir);
    let export = KeyExport {
        repo_id: fx.repo_config().id,
        blob: b"not an EncryptedKey".to_vec(),
    };
    let err = commands::key::import(&fx.config, Some(PASS), &export, true)
        .err()
        .expect("must fail");
    assert!(err.to_string().contains("malformed key export"), "{err}");
    assert_eq!(key_dir_snapshot(&fx.repo_dir), before);
}

/// `export` must not go through `Repository::open`: a recovery tool for the
/// key cannot fail on an unreadable advisory sidecar, and must write nothing.
///
/// The sidecar is made unreadable by replacing it with a directory, which
/// fails with EISDIR for root as well; this path is reached through the
/// config, so a failure cannot be injected into the backend.
#[test]
fn export_works_on_a_repository_open_would_choke_on_and_writes_nothing() {
    let fx = Fixture::new();
    std::fs::remove_file(fx.secondary()).unwrap();
    let before = key_dir_snapshot(&fx.repo_dir);
    make_unreadable_via_fs(&fx.repo_dir.join("index.gen"));

    // Export first: `open` backfills the missing copy before it trips over
    // the sidecar, which would mask whether *export* wrote anything.
    let exported = commands::key::export(&fx.config, Some(PASS))
        .expect("export must not depend on opening the repository");
    assert_eq!(
        key_dir_snapshot(&fx.repo_dir),
        before,
        "export must never backfill or otherwise write"
    );
    assert!(fx.open().is_err(), "the fixture must be one `open` refuses");

    assert_eq!(exported.export.blob, fx.read_primary());
    assert!(matches!(
        exported.copies,
        vykar_core::repo::KeyCopyState::OneMissing { missing } if missing == KEY_SECONDARY
    ));
}

// ---------------------------------------------------------------------------
// Regressions
// ---------------------------------------------------------------------------

/// **An unproven candidate must never win a divergence.**
///
/// The setup: no local pin, no `index` to decrypt, a genuine primary wrapped
/// under passphrase A and a planted secondary wrapped under passphrase B.
/// Opening with B unwraps exactly one copy — the planted one — and nothing
/// corroborates it. Accepting it would hand `check --repair` a licence to
/// overwrite the genuine primary from a key an attacker chose, which cannot be
/// undone.
#[test]
fn an_unprovable_divergent_candidate_cannot_win_and_cannot_drive_repair() {
    const ATTACKER_PASS: &str = "attacker-chosen-passphrase";

    let fx = Fixture::new();
    let genuine = fx.read_primary();
    std::fs::write(fx.secondary(), foreign_key_blob_with(ATTACKER_PASS)).unwrap();
    fx.strip_provable_data();
    fx.forget_pin();

    let before = key_dir_snapshot(&fx.repo_dir);

    // Only the planted copy unwraps under the attacker's passphrase.
    let msg = fx
        .open_with(ATTACKER_PASS)
        .err()
        .expect("an unprovable candidate must not open the repository")
        .to_string();
    assert!(msg.contains(KEY_SECONDARY), "must name the copy: {msg}");
    assert!(msg.contains("Refusing to guess"), "{msg}");

    // And `check --repair` under that passphrase cannot get past the open, so
    // the genuine primary is never rewritten.
    let mut attacker_config = fx.config.clone();
    attacker_config.encryption.passphrase = Some(ATTACKER_PASS.to_string());
    assert!(commands::check::run_with_repair(
        &attacker_config,
        Some(ATTACKER_PASS),
        false,
        RepairMode::Apply,
        None,
        None,
    )
    .is_err());

    assert_eq!(
        key_dir_snapshot(&fx.repo_dir),
        before,
        "no key file may be rewritten from an unproven candidate"
    );
    assert_eq!(fx.read_primary(), genuine);
}

/// The same planted copy, but with `index` left intact. The genuine key is now
/// provable and the planted one is not, so the open must succeed off the
/// genuine key — and, critically, an open under the attacker's passphrase must
/// still fail rather than establishing their key as this repository's.
#[test]
fn a_provable_genuine_key_still_wins_against_a_planted_copy() {
    const ATTACKER_PASS: &str = "attacker-chosen-passphrase";

    let fx = Fixture::new();
    let genuine = fx.read_primary();
    std::fs::write(fx.secondary(), foreign_key_blob_with(ATTACKER_PASS)).unwrap();
    fx.forget_pin();

    fx.open().expect("the genuine key is provable");

    let err = fx
        .open_with(ATTACKER_PASS)
        .err()
        .expect("the planted key is not provable, so it must not open the repository");
    assert!(!err.to_string().is_empty());

    // No pin may have been created for the planted key.
    assert!(
        fx.pin_files().is_empty(),
        "a refused candidate must not become a trusted identity"
    );
    assert_eq!(fx.read_primary(), genuine);
}

/// **Redundancy must survive an I/O failure on one copy.** An unreadable
/// object is not the same as a missing one: the open proceeds off the
/// survivor, and the unreadable copy is left strictly alone.
#[test]
fn an_unreadable_copy_does_not_defeat_the_intact_one() {
    let fx = Fixture::new();
    let genuine = fx.read_primary();

    fx.open_on(fx.faulty(&[KEY_PRIMARY], &[]))
        .expect("an unreadable copy must not defeat the intact one");
    assert_eq!(
        fx.read_primary(),
        genuine,
        "an unreadable copy must never be overwritten — it may be intact"
    );
}

/// The readable copy is corrupt and the other merely could not be read: the
/// diagnosis must say so, not claim the other copy does not exist — fixing
/// access may recover the key without a restore.
#[test]
fn a_corrupt_copy_beside_an_unreadable_one_points_at_access_not_absence() {
    let fx = Fixture::new();
    std::fs::write(fx.primary(), b"not msgpack").unwrap();

    let msg = fx
        .open_on(fx.faulty(&[KEY_SECONDARY], &[]))
        .err()
        .expect("must fail")
        .to_string();
    assert!(msg.contains("corrupt"), "{msg}");
    assert!(
        msg.contains(&format!("{KEY_SECONDARY}, could not be read")),
        "{msg}"
    );
    assert!(msg.contains("Fix access"), "{msg}");
    assert!(
        !msg.contains("no other copy exists"),
        "an unreadable copy is not a nonexistent one: {msg}"
    );

    // Whereas a genuinely absent other copy is described as such.
    std::fs::remove_file(fx.secondary()).unwrap();
    let msg = fx.open().err().expect("must fail").to_string();
    assert!(msg.contains("no other copy exists"), "{msg}");
    assert!(msg.contains("vykar key import"), "{msg}");
}

/// `check` must not promise to rewrite a copy it will not touch: an unreadable
/// copy is not proven corrupt, so repair leaves it alone and the message says
/// to fix access instead. Reached through the config, so the object is made
/// unreadable on the filesystem (a directory in its place) rather than by
/// injection.
#[test]
fn check_reports_an_unreadable_copy_without_promising_a_rewrite() {
    let fx = Fixture::new();
    make_unreadable_via_fs(&fx.secondary());

    let result = commands::check::run(&fx.config, Some(PASS), false, false).unwrap();
    assert_eq!(result.key_files_checked, Some(1));
    let issue = result
        .errors
        .iter()
        .find(|e| e.context == "repository key" && e.message.contains(KEY_SECONDARY))
        .expect("the unreadable copy must be reported");
    assert!(issue.message.contains("fix access"), "{}", issue.message);
    assert!(
        !issue.message.contains("check --repair"),
        "must not promise a rewrite it will not perform: {}",
        issue.message
    );
    assert!(
        fx.secondary().is_dir(),
        "the unreadable object must be left alone"
    );
}

/// Argon2 parameters that pass every ceiling but violate Argon2's own
/// `memory >= 8 * parallelism` floor can never derive a key, so the blob is
/// corrupt — not a passphrase problem, and not something to re-prompt for.
#[test]
fn impossible_argon2_params_are_corruption_not_a_passphrase_failure() {
    let fx = Fixture::new();
    let mut mangled = vykar_crypto::key::inspect_key_blob(&fx.read_primary()).unwrap();
    mangled.kdf.memory_cost = 1;
    mangled.kdf.parallelism = 16;
    let bytes = rmp_serde::to_vec(&mangled).unwrap();
    for path in [fx.primary(), fx.secondary()] {
        std::fs::write(path, &bytes).unwrap();
    }

    let err = fx.open().err().expect("must fail");
    assert!(!err.is_passphrase_failure(), "{err}");
    assert!(err.to_string().contains("corrupt"), "{err}");
}

/// Both copies unreadable is not "the key is missing": say what actually
/// happened, so the operator fixes access instead of hunting for a backup.
#[test]
fn both_copies_unreadable_reports_the_read_failure_not_a_missing_key() {
    let fx = Fixture::new();

    let msg = fx
        .open_on(fx.faulty(&[KEY_PRIMARY, KEY_SECONDARY], &[]))
        .err()
        .expect("open must fail")
        .to_string();
    assert!(msg.contains("could not read the repository key"), "{msg}");
    assert!(msg.contains("simulated I/O error"), "{msg}");
    assert!(
        !msg.contains("vykar key import"),
        "an access failure must not send the operator to a restore: {msg}"
    );
}

/// Ambiguous unwrap failures must stay a *typed* authentication failure, or
/// every passphrase re-prompt and cache-eviction path downstream silently
/// stops working.
#[test]
fn ambiguous_unwrap_failures_are_typed_as_passphrase_failures() {
    let fx = Fixture::new();

    // Two matching copies, wrong passphrase.
    let err = fx.open_with("not-the-passphrase").err().expect("must fail");
    assert!(err.is_passphrase_failure(), "{err}");

    // Single copy, wrong passphrase.
    std::fs::remove_file(fx.secondary()).unwrap();
    let err = fx.open_with("not-the-passphrase").err().expect("must fail");
    assert!(err.is_passphrase_failure(), "{err}");
}

/// Proven corruption is *not* a passphrase failure — reporting it as one would
/// send the operator round a retry loop that cannot succeed.
#[test]
fn proven_corruption_is_not_typed_as_a_passphrase_failure() {
    let fx = Fixture::new();
    for path in [fx.primary(), fx.secondary()] {
        std::fs::write(path, b"not msgpack").unwrap();
    }
    let err = fx.open().err().expect("must fail");
    assert!(!err.is_passphrase_failure(), "{err}");
}

/// Two copies that are corrupt in *different* ways are still proven
/// corruption. Typing it as a passphrase failure would send the GUI round a
/// retry loop that cannot succeed, and blame the operator for bit rot.
#[test]
fn two_differently_corrupted_copies_are_not_a_passphrase_failure() {
    let fx = Fixture::new();
    // Different bytes, and different defects: bad msgpack framing vs. a
    // well-framed blob with an impossible nonce.
    std::fs::write(fx.primary(), b"not msgpack at all").unwrap();
    let mut mangled = vykar_crypto::key::inspect_key_blob(&std::fs::read(fx.secondary()).unwrap())
        .expect("the secondary starts out well-formed");
    mangled.nonce = vec![0u8; 8];
    std::fs::write(fx.secondary(), rmp_serde::to_vec(&mangled).unwrap()).unwrap();

    let err = fx.open().err().expect("both copies are unusable");
    assert!(
        !err.is_passphrase_failure(),
        "proven corruption in both copies must not be typed as a passphrase failure: {err}"
    );
    let msg = err.to_string();
    assert!(msg.contains("corrupt"), "{msg}");
    assert!(
        !msg.contains("Either the passphrase is wrong"),
        "must not blame the passphrase: {msg}"
    );
    assert!(
        msg.contains(KEY_PRIMARY) && msg.contains(KEY_SECONDARY),
        "{msg}"
    );
}

/// One corrupt copy and one that merely fails to authenticate keeps the
/// ambiguity: a wrong passphrase still explains the second copy.
#[test]
fn one_corrupt_and_one_unauthenticated_copy_stays_a_passphrase_failure() {
    let fx = Fixture::new();
    std::fs::write(fx.primary(), b"not msgpack at all").unwrap();

    let err = fx
        .open_with("not-the-passphrase")
        .err()
        .expect("neither copy unwraps");
    assert!(err.is_passphrase_failure(), "{err}");
}
