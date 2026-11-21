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

use vykar_core::commands;
use vykar_core::index::ChunkIndex;
use vykar_core::repo::file_cache::FileCache;
use vykar_core::repo::{OpenOptions, Repository, identity};
use vykar_storage::local_backend::LocalBackend;

use crate::common::fixtures::{self, Fixture, read_tree};
use crate::common::{backup_source, exercise_pack_naming};
use vykar_types::hash::HashAlgorithm;

/// One fixture per encryption mode.
const MODES: &[&str] = &["none", "aes256gcm", "chacha20poly1305"];

fn extract(mode: &str) -> Fixture {
    fixtures::extract("v2", mode, 2)
}

/// [`extract`], then `vykar upgrade` to v4: every command but the upgrade
/// refuses v2.
fn extract_upgraded(mode: &str) -> Fixture {
    let fx = extract(mode);
    commands::upgrade::run(&fx.config(), fx.passphrase()).unwrap();
    fx
}

fn sorted_entries(
    index: &ChunkIndex,
) -> Vec<(
    vykar_types::chunk_id::ChunkId,
    vykar_core::index::ChunkIndexEntry,
)> {
    let mut out: Vec<_> = index.iter().map(|(id, e)| (*id, *e)).collect();
    out.sort_by_key(|(id, _)| *id.as_bytes());
    out
}

/// The identity fingerprint the pre-change binary computed for this repo.
fn pin_fingerprint(fx: &Fixture) -> String {
    fx.meta["pin_fingerprint"].as_str().unwrap().to_string()
}

// ── Reading ────────────────────────────────────────────────────────────────

/// Every open but the upgrade's refuses a v2 fixture and writes nothing.
#[test]
fn v2_fixtures_open_only_for_upgrade() {
    for mode in MODES {
        let fx = extract(mode);
        let tree = read_tree(&fx.repo_dir());
        for opts in [OpenOptions::new(), OpenOptions::new().with_index()] {
            let storage = Box::new(LocalBackend::new(fx.repo_dir().to_str().unwrap()).unwrap());
            let err = Repository::open(storage, fx.passphrase(), Some(fx.cache_dir()), opts)
                .err()
                .unwrap()
                .to_string();
            assert!(err.contains("vykar upgrade"), "{mode}: {err}");
        }
        assert_eq!(
            read_tree(&fx.repo_dir()),
            tree,
            "{mode}: a refused open wrote"
        );

        let repo = fx.open_legacy();
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
        assert!(!repo.legacy_index().unwrap().is_empty(), "{mode}");
    }
}

#[test]
fn v2_fixtures_restore_byte_identical() {
    for mode in MODES {
        let fx = extract_upgraded(mode);
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
            None,
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
        let fx = extract_upgraded(mode);
        let result = commands::check::run(&fx.config(), fx.passphrase(), true, false).unwrap();
        assert!(
            result.errors.is_empty(),
            "{mode}: check --verify-data reported {:?}",
            result.errors
        );
    }
}

// ── Writing ────────────────────────────────────────────────────────────────

/// `vykar upgrade` takes each v2 fixture to v4 in place: same chunk IDs (the
/// plaintext key follows the chunk hash), the same index, every snapshot
/// still restores byte-identical, and a new backup dedups against the chunks
/// the pre-change binary wrote.
#[test]
fn v2_fixtures_upgrade_to_v4() {
    use vykar_core::repo::UpgradeOutcome;
    for mode in MODES {
        let fx = extract(mode);
        let config = fx.config();
        let before = sorted_entries(&fx.open_legacy().legacy_index().unwrap());

        let outcome = commands::upgrade::run(&config, fx.passphrase()).unwrap();
        assert!(
            matches!(
                outcome,
                UpgradeOutcome::Upgraded {
                    from: 2,
                    to: 4,
                    resumed: false,
                    ..
                }
            ),
            "{mode}: {outcome:?}"
        );
        assert!(!fx.repo_dir().join("index.gen").exists(), "{mode}");

        let repo = fx.open();
        assert_eq!(repo.config.version, 4, "{mode}");
        assert_eq!(
            before,
            sorted_entries(repo.chunk_index()),
            "{mode}: the upgrade changed the index"
        );
        drop(repo);

        let result = commands::check::run(&config, fx.passphrase(), true, false).unwrap();
        assert!(result.errors.is_empty(), "{mode}: {:?}", result.errors);

        let dest = fx.root.join("restore-upgraded");
        commands::restore::run(
            &config,
            fx.passphrase(),
            fx.latest_snapshot(),
            dest.to_str().unwrap(),
            None,
            config.xattrs.enabled,
            true,
            None,
        )
        .unwrap();
        assert_eq!(read_tree(&dest), read_tree(&fx.source_dir()), "{mode}");

        let stats = backup_source(
            &config,
            &fx.source_dir(),
            "fixture-src",
            "post-upgrade-snap",
            fx.passphrase(),
            config.xattrs.enabled,
        );
        // File data must dedup completely against chunks written by the
        // pre-change binary; only snapshot metadata is genuinely new.
        assert!(
            stats.original_size > 1024 * 1024,
            "{mode}: expected a non-trivial source, got {} bytes",
            stats.original_size
        );
        assert!(
            stats.deduplicated_size < stats.original_size / 8,
            "{mode}: {} of {} bytes were newly stored after the upgrade — dedup \
             against the pre-change chunk IDs is broken",
            stats.deduplicated_size,
            stats.original_size
        );
        let after = fx.open().chunk_index().len();
        assert!(
            after >= before.len() && after - before.len() < before.len(),
            "{mode}: chunk index {} -> {after}: the new backup re-stored content \
             instead of deduplicating",
            before.len()
        );
        assert!(
            matches!(
                commands::upgrade::run(&config, fx.passphrase()).unwrap(),
                UpgradeOutcome::AlreadyCurrent { version: 4 }
            ),
            "{mode}"
        );
    }
}

/// Packs written into an upgraded v2 repository — by backup and by repack —
/// must still be named by BLAKE2b: v4 keeps v2's chunk and pack hash.
#[test]
fn v2_fixtures_name_new_packs_by_blake2b() {
    for mode in MODES {
        let fx = extract_upgraded(mode);
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
        let fx = extract_upgraded(mode);
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
            None,
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
/// `lifecycle_integration.rs` covers for v5 (every repo it creates is v5 now).
/// The encrypted fixtures must still reject a wrong passphrase the same way
/// on the upgrade's open. Every other open refuses v2 before the key check.
#[test]
fn v2_encrypted_fixtures_reject_a_wrong_passphrase() {
    for mode in MODES {
        let fx = extract(mode);
        if fx.passphrase().is_none() {
            continue; // plaintext fixture: no passphrase to get wrong
        }
        let config = fx.config();
        let wrong = Some("definitely-not-the-fixture-passphrase");

        // A frozen v2 fixture predates key redundancy, so it carries a single
        // copy: the diagnosis says so rather than claiming corroboration.
        let open = |opts| {
            let storage = Box::new(LocalBackend::new(fx.repo_dir().to_str().unwrap()).unwrap());
            Repository::open(storage, wrong, None, opts)
                .err()
                .unwrap_or_else(|| panic!("{mode}: opening with a wrong passphrase must fail"))
                .to_string()
        };
        let msg = open(OpenOptions::new().legacy_format());
        assert!(
            msg.contains("wrong passphrase, or the stored key is damaged"),
            "{mode}: unexpected wrong-passphrase message: {msg}"
        );
        let msg = open(OpenOptions::new());
        assert!(msg.contains("vykar upgrade"), "{mode}: {msg}");

        let err = commands::upgrade::run(&config, wrong)
            .err()
            .unwrap_or_else(|| panic!("{mode}: upgrading with a wrong passphrase must fail"));
        assert!(
            err.to_string().contains("wrong passphrase"),
            "{mode}: unexpected upgrade message: {err}"
        );

        let err = commands::restore::run(
            &config,
            wrong,
            fx.latest_snapshot(),
            fx.root.join("bad-restore").to_str().unwrap(),
            None,
            config.xattrs.enabled,
            false,
            None,
        )
        .err()
        .unwrap_or_else(|| panic!("{mode}: restoring with a wrong passphrase must fail"));
        assert!(
            err.to_string().contains("vykar upgrade"),
            "{mode}: unexpected restore message: {err}"
        );

        let err = commands::check::run(&config, wrong, true, false)
            .err()
            .unwrap_or_else(|| panic!("{mode}: check with a wrong passphrase must fail"));
        assert!(
            err.to_string().contains("vykar upgrade"),
            "{mode}: unexpected check message: {err}"
        );
    }
}

// ── Local state written by the pre-change binary ───────────────────────────

/// `identity::pin_file_path`, reproduced so the test can name the file the
/// implementation reads.
fn pin_file_name(url: &str) -> String {
    format!("pin3.{}", blake3::hash(url.as_bytes()).to_hex())
}

/// The pre-BLAKE3 fingerprint, `BLAKE2b-256(repo_id || chunk_id_key)`.
fn legacy_fingerprint(repo_id: &[u8], chunk_id_key: &[u8; 32]) -> String {
    use blake2::{Blake2b256, Digest};
    let mut hasher = Blake2b256::new();
    hasher.update(repo_id);
    hasher.update(chunk_id_key);
    hex::encode(hasher.finalize())
}

/// Pins written before the BLAKE3 switch are left alone and ignored: the first
/// open re-pins under the new name (the accepted one-time TOFU gap), and from
/// then on the new pin is enforced.
#[test]
fn v2_fixture_pin_files_are_superseded() {
    for mode in MODES {
        let fx = extract(mode);
        let repo = fx.open_legacy();
        let url = fx.repo_dir().to_string_lossy().into_owned();
        let repo_id = &repo.config.id;
        let key = repo.crypto.chunk_hasher().key();

        // Guards the fixture reading: the pin really is the old derivation.
        assert_eq!(
            legacy_fingerprint(repo_id, key),
            pin_fingerprint(&fx),
            "{mode}: fixture pin is not the BLAKE2b fingerprint"
        );
        let fingerprint = hex::encode(identity::compute_fingerprint(repo_id, key));
        assert_ne!(fingerprint, pin_fingerprint(&fx), "{mode}");

        let cache = fx.cache_dir();
        let legacy_pin = std::fs::read_dir(&cache)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("pin."))
            })
            .unwrap_or_else(|| panic!("{mode}: fixture has no pin file"));
        let legacy_bytes = std::fs::read(&legacy_pin).unwrap();

        // Rewrite the old pin to contradict this repository, as it would after
        // a re-init. It must not be consulted.
        let mut pin: serde_json::Value = serde_json::from_slice(&legacy_bytes).unwrap();
        pin["fingerprint"] = serde_json::Value::String("0".repeat(64));
        std::fs::write(&legacy_pin, serde_json::to_vec(&pin).unwrap()).unwrap();
        let legacy_bytes = std::fs::read(&legacy_pin).unwrap();

        identity::verify_or_pin(&url, repo_id, key, Some(&cache), false)
            .unwrap_or_else(|e| panic!("{mode}: old pin was consulted: {e}"));
        assert_eq!(std::fs::read(&legacy_pin).unwrap(), legacy_bytes, "{mode}");

        let target = cache.join(pin_file_name(&url));
        let new_pin: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&target).unwrap()).unwrap();
        assert_eq!(new_pin["fingerprint"], fingerprint.as_str(), "{mode}");

        // Closing the loop: the new pin is enforced.
        let mut pin = new_pin;
        pin["fingerprint"] = serde_json::Value::String("0".repeat(64));
        std::fs::write(&target, serde_json::to_vec(&pin).unwrap()).unwrap();
        assert!(
            identity::verify_or_pin(&url, repo_id, key, Some(&cache), false).is_err(),
            "{mode}: verify_or_pin does not read the pin file this test writes"
        );
    }
}

/// The pre-change file cache keys paths by BLAKE2b-128, so it is dropped on
/// load rather than misread: the next backup re-reads every file once.
#[test]
fn v2_fixture_file_caches_are_dropped() {
    for mode in MODES {
        let fx = extract(mode);
        let repo = fx.open_legacy();
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
            loaded.is_empty(),
            "{mode}: BLAKE2b-keyed file cache was loaded"
        );
    }
}
