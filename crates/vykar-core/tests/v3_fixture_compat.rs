//! Compatibility tests against **frozen** repository-format-v3 fixtures.
//!
//! The archives under `tests/fixtures/` were written by the vykar v0.24.0
//! binary (`scripts/gen-v3-fixtures.sh v0.24.0`): format v3 is what 0.20 to
//! 0.24 write, so every upgrading user has one. Each repository holds three
//! snapshots, a deleted one whose pack 0.24's `compact` repacked, and the
//! local caches 0.24 left behind. A failure here means today's binary can no
//! longer read or upgrade such a repository, so **do not regenerate them**
//! (see `tests/fixtures/README.md`).

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
// Test-only: `panic!` on a malformed fixture and `meta["key"]` indexing a
// `serde_json::Value` are the right failure modes.
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]

mod common;

use vykar_core::commands;
use vykar_core::repo::{IndexLayout, Repository, UpgradeOutcome};

use crate::common::backup_source;
use crate::common::fixtures::{self, Fixture, archive_path, read_tree};

/// One fixture per encryption mode.
const MODES: &[&str] = &["none", "aes256gcm", "chacha20poly1305"];

/// BLAKE3 of each archive as committed. A mismatch means the archive was
/// regenerated or replaced.
const FIXTURE_DIGESTS: &[(&str, &str)] = &[
    (
        "v3-none.tar.gz",
        "f68e7bf2ffe61dfa6c9307d512fc9ddc9ea951065b01bbbb07af797888e98b7c",
    ),
    (
        "v3-aes256gcm.tar.gz",
        "c61e13c87b9a3bb9bb9efc36d263584ed795be35265dbabce9dd7924935e50fc",
    ),
    (
        "v3-chacha20poly1305.tar.gz",
        "abcf1826fc6b38bb09b2a5abb19926ed7a70f3757e4c9264b2ce124744a5ca05",
    ),
];

fn extract(mode: &str) -> Fixture {
    fixtures::extract("v3", mode, 3)
}

fn sorted_entries(
    repo: &Repository,
) -> Vec<(
    vykar_types::chunk_id::ChunkId,
    vykar_core::index::ChunkIndexEntry,
)> {
    let mut out: Vec<_> = repo.chunk_index().iter().map(|(id, e)| (*id, *e)).collect();
    out.sort_by_key(|(id, _)| *id.as_bytes());
    out
}

fn assert_check_clean(fx: &Fixture, label: &str) {
    let result = commands::check::run(&fx.config(), fx.passphrase(), true, false).unwrap();
    assert!(
        result.errors.is_empty(),
        "{label}: check --verify-data reported {:?}",
        result.errors
    );
}

fn assert_restores_source(fx: &Fixture, dest_name: &str, label: &str) {
    let config = fx.config();
    let dest = fx.root.join(dest_name);
    commands::restore::run(
        &config,
        fx.passphrase(),
        fx.latest_snapshot(),
        dest.to_str().unwrap(),
        None,
        config.xattrs.enabled,
        true, // verify chunk IDs
        None,
    )
    .unwrap();
    assert_eq!(
        read_tree(&dest),
        read_tree(&fx.source_dir()),
        "{label}: restored tree differs from the tree that was backed up"
    );
}

#[test]
fn v3_fixture_archives_are_frozen() {
    let mut mismatches = Vec::new();
    for mode in MODES {
        let path = archive_path("v3", mode);
        let file = path.file_name().unwrap().to_str().unwrap().to_string();
        let actual = blake3::hash(&std::fs::read(&path).unwrap())
            .to_hex()
            .to_string();
        let pinned = FIXTURE_DIGESTS
            .iter()
            .find(|(name, _)| *name == file)
            .map(|(_, d)| *d);
        if pinned != Some(actual.as_str()) {
            mismatches.push(format!("(\"{file}\", \"{actual}\"),"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "these archives are frozen (tests/fixtures/README.md); restore them with \
         `git checkout` instead of regenerating. Actual digests:\n{}",
        mismatches.join("\n")
    );
}

#[test]
fn v3_fixtures_open_as_format_v3() {
    for mode in MODES {
        let fx = extract(mode);
        assert_eq!(
            fx.meta["generated_from_tag"].as_str(),
            Some("v0.24.0"),
            "{mode}"
        );
        assert!(fx.repo_dir().join("index").is_file(), "{mode}");
        assert!(fx.repo_dir().join("index.gen").is_file(), "{mode}");
        assert!(!fx.repo_dir().join("index.d").exists(), "{mode}");

        let repo = fx.open();
        assert_eq!(
            repo.config.version, 3,
            "{mode}: fixture must stay at format v3"
        );
        assert_eq!(repo.format().index_layout(), IndexLayout::Monolithic);
        assert!(repo.index_head().is_none(), "{mode}");
        assert_eq!(repo.config.encryption, fx.expected_encryption(), "{mode}");

        let mut listed: Vec<String> = repo
            .manifest()
            .snapshots
            .iter()
            .map(|s| s.name.clone())
            .collect();
        listed.sort();
        let mut expected = fx.snapshots.clone();
        expected.sort();
        assert_eq!(listed, expected, "{mode}");
        let deleted = fx.meta["deleted_snapshot"].as_str().unwrap();
        assert!(!listed.iter().any(|s| s == deleted), "{mode}");
        assert!(
            repo.skipped_snapshots().is_empty(),
            "{mode}: no snapshot may be unreadable"
        );
    }
}

#[test]
fn v3_fixtures_restore_byte_identical() {
    for mode in MODES {
        let fx = extract(mode);
        assert_restores_source(&fx, "restore-original", mode);
    }
}

#[test]
fn v3_fixtures_verify_data_is_clean() {
    for mode in MODES {
        let fx = extract(mode);
        assert_check_clean(&fx, mode);
    }
}

/// `vykar upgrade` takes each v3 fixture to v5 in place: the same index, every
/// check clean, the latest snapshot restores byte-identical, and a new backup
/// dedups against the chunk IDs 0.24 wrote.
#[test]
fn v3_fixtures_upgrade_to_v5() {
    for mode in MODES {
        let fx = extract(mode);
        let config = fx.config();
        let before = sorted_entries(&fx.open());

        let outcome = commands::upgrade::run(&config, fx.passphrase()).unwrap();
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
            "{mode}: {outcome:?}"
        );
        assert!(!fx.repo_dir().join("index.gen").exists(), "{mode}");

        let repo = fx.open();
        assert_eq!(repo.config.version, 5, "{mode}");
        assert_eq!(
            before,
            sorted_entries(&repo),
            "{mode}: the upgrade changed the index"
        );
        drop(repo);

        assert_check_clean(&fx, mode);
        assert_restores_source(&fx, "restore-upgraded", mode);

        let stats = backup_source(
            &config,
            &fx.source_dir(),
            "fixture-src",
            "post-upgrade-snap",
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
            "{mode}: {} of {} bytes were newly stored after the upgrade",
            stats.deduplicated_size,
            stats.original_size
        );
        assert!(
            matches!(
                commands::upgrade::run(&config, fx.passphrase()).unwrap(),
                UpgradeOutcome::AlreadyCurrent { version: 5 }
            ),
            "{mode}"
        );
    }
}
