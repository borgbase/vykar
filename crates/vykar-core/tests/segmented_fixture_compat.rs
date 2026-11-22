//! Compatibility tests against **frozen** segmented-index (v4/v5) fixtures.
//!
//! The archives under `tests/fixtures/` were written by the binary named in
//! `scripts/gen-segmented-fixtures.sh`: each holds a head with a base, two
//! segments over it and a non-empty `retired` generation. v4 is the v2
//! fixture upgraded in place. A failure here means today's binary can no
//! longer read or extend segmented repositories that exist in the wild, so
//! **do not regenerate them** (see `tests/fixtures/README.md`).

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
// Test-only: `panic!` on a malformed fixture and `meta["key"]` indexing a
// `serde_json::Value` are the right failure modes.
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]

mod common;

use std::collections::BTreeSet;
use std::path::Path;

use vykar_core::commands;
use vykar_core::index::head::IndexHead;
use vykar_core::repo::Repository;
use vykar_core::repo::control::CancelToken;
use vykar_types::hash::HashAlgorithm;

use crate::common::fixtures::{self, Fixture, archive_path, read_tree};
use crate::common::{backup_source, exercise_pack_naming};

/// `(prefix, mode)` of every segmented fixture.
const FIXTURES: &[(&str, &str)] = &[
    ("v4", "none"),
    ("v4", "aes256gcm"),
    ("v4", "chacha20poly1305"),
    ("v5", "none"),
    ("v5", "aes256gcm"),
    ("v5", "chacha20poly1305"),
];

/// BLAKE3 of each archive as committed. A mismatch means the archive was
/// regenerated or replaced.
const FIXTURE_DIGESTS: &[(&str, &str)] = &[
    (
        "v4-none.tar.gz",
        "7d160c026d3bffbae7f154da505edb46671584106332bf9b21083236cfe69e68",
    ),
    (
        "v4-aes256gcm.tar.gz",
        "ded796b3d6661e5fa8554867071225ba4dfa1bc58875719fcb62f72a4d02c8bd",
    ),
    (
        "v4-chacha20poly1305.tar.gz",
        "5febc15903189a2549e5f531c6a182a0c4d61483c4655d6e825bca5aceb4f537",
    ),
    (
        "v5-none.tar.gz",
        "d18270ce8d3dff7181803fe3e4d50ad87dd5a640c83b656c16fe45645588bfc0",
    ),
    (
        "v5-aes256gcm.tar.gz",
        "1674197c70e320579bf5cd619991e26ad8086f3c018888c0665d984579c4ac2b",
    ),
    (
        "v5-chacha20poly1305.tar.gz",
        "ad4b552a667f7b781c4e5815034f73303cd8e65aaff04bd73138331a7ca16310",
    ),
];

fn format_version(prefix: &str) -> u32 {
    match prefix {
        "v4" => 4,
        "v5" => 5,
        other => panic!("unknown fixture prefix {other}"),
    }
}

fn extract(prefix: &str, mode: &str) -> Fixture {
    fixtures::extract(prefix, mode, u64::from(format_version(prefix)))
}

fn head(repo: &Repository) -> IndexHead {
    repo.index_head()
        .expect("a segmented repository has a head")
        .clone()
}

/// Hex names of the head's base, segments and retired objects.
fn named_objects(head: &IndexHead) -> BTreeSet<String> {
    head.live_objects()
        .chain(head.retired.iter().copied())
        .map(|n| n.to_hex())
        .collect()
}

/// File names under `repo/index.d/`.
fn index_files(repo_dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(repo_dir.join("index.d"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect()
}

fn sorted_entries(
    repo: &Repository,
) -> Vec<(
    vykar_types::chunk_id::ChunkId,
    vykar_core::index::ChunkIndexEntry,
)> {
    let mut out: Vec<_> = repo.chunk_index().iter().collect();
    out.sort_by_key(|(id, _)| *id.as_bytes());
    out
}

fn assert_cold_equals_warm(fx: &Fixture, cold_name: &str, label: &str) {
    let warm = fx.open();
    let cold = fx.open_cold(cold_name);
    let entries = sorted_entries(&warm);
    assert_eq!(
        entries,
        sorted_entries(&cold),
        "{label}: a cold open differs from the warm one"
    );
    assert_eq!(entries.len() as u64, head(&cold).entries, "{label}");
}

fn assert_check_clean(fx: &Fixture, label: &str) {
    let result = commands::check::run(&fx.config(), fx.passphrase(), true, false).unwrap();
    assert!(
        result.errors.is_empty(),
        "{label}: check --verify-data reported {:?}",
        result.errors
    );
}

#[test]
fn segmented_fixture_archives_are_frozen() {
    let mut mismatches = Vec::new();
    for (prefix, mode) in FIXTURES {
        let path = archive_path(prefix, mode);
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
fn segmented_fixtures_open_at_their_format() {
    for (prefix, mode) in FIXTURES {
        let label = format!("{prefix}-{mode}");
        let fx = extract(prefix, mode);
        let repo = fx.open();
        assert_eq!(repo.config.version, format_version(prefix), "{label}");
        assert_eq!(repo.config.encryption, fx.expected_encryption(), "{label}");
        let mut listed: Vec<_> = repo
            .manifest()
            .snapshots
            .iter()
            .map(|s| s.name.clone())
            .collect();
        listed.sort();
        let mut expected = fx.snapshots.clone();
        expected.sort();
        assert_eq!(listed, expected, "{label}: every snapshot must be listable");
        assert!(repo.skipped_snapshots().is_empty(), "{label}");

        let head = head(&repo);
        assert!(head.base.is_some(), "{label}: head has no base");
        assert_eq!(
            Some(head.segments.len() as u64),
            fx.meta["expected_segments"].as_u64(),
            "{label}"
        );
        assert!(!head.retired.is_empty(), "{label}: head has no retired set");

        let meta: BTreeSet<String> = fx.meta["index_objects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(named_objects(&head), meta, "{label}: head names");
        assert_eq!(index_files(&fx.repo_dir()), meta, "{label}: index.d/");
    }
}

#[test]
fn segmented_fixtures_cold_open_equals_warm() {
    for (prefix, mode) in FIXTURES {
        let fx = extract(prefix, mode);
        assert_cold_equals_warm(&fx, "cold", &format!("{prefix}-{mode}"));
    }
}

#[test]
fn segmented_fixtures_restore_byte_identical() {
    for (prefix, mode) in FIXTURES {
        let fx = extract(prefix, mode);
        let config = fx.config();
        let dest = fx.root.join("restore-original");
        commands::restore::run(
            &config,
            fx.passphrase(),
            fx.latest_snapshot(),
            dest.to_str().unwrap(),
            None,
            config.xattrs.enabled,
            true, // verify chunk IDs
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(
            read_tree(&dest),
            read_tree(&fx.source_dir()),
            "{prefix}-{mode}: restored tree differs from the tree that was backed up"
        );
    }
}

#[test]
fn segmented_fixtures_verify_data_is_clean() {
    for (prefix, mode) in FIXTURES {
        let fx = extract(prefix, mode);
        assert_check_clean(&fx, &format!("{prefix}-{mode}"));
    }
}

/// A backup extends the frozen chain by exactly one segment: no fold, and
/// every object the old binary wrote stays named.
#[test]
fn segmented_fixtures_accept_a_backup_as_one_segment() {
    for (prefix, mode) in FIXTURES {
        let label = format!("{prefix}-{mode}");
        let fx = extract(prefix, mode);
        let config = fx.config();
        let before = head(&fx.open());
        let files_before = index_files(&fx.repo_dir());

        let stats = backup_source(
            &config,
            &fx.source_dir(),
            "fixture-src",
            "post-fixture-snap",
            fx.passphrase(),
            config.xattrs.enabled,
        );
        assert!(
            stats.deduplicated_size < stats.original_size / 8,
            "{label}: {} of {} bytes were newly stored",
            stats.deduplicated_size,
            stats.original_size
        );

        let after = head(&fx.open());
        let n = before.segments.len();
        assert_eq!(after.base, before.base, "{label}: base changed");
        assert_eq!(after.retired, before.retired, "{label}: retired changed");
        assert_eq!(after.segments.len(), n + 1, "{label}");
        assert_eq!(after.segments[..n], before.segments[..], "{label}");

        let files_after = index_files(&fx.repo_dir());
        assert!(files_after.is_superset(&files_before), "{label}");
        assert_eq!(files_after.len(), files_before.len() + 1, "{label}");

        assert_cold_equals_warm(&fx, "cold-after-backup", &label);
        assert_check_clean(&fx, &label);
    }
}

/// Compact's garbage collection keeps the retired generation of a head an
/// older binary wrote.
#[test]
fn segmented_fixtures_compact_keeps_the_retired_generation() {
    for (prefix, mode) in FIXTURES {
        let label = format!("{prefix}-{mode}");
        let fx = extract(prefix, mode);
        let before = head(&fx.open());

        commands::compact::run(
            &fx.config(),
            fx.passphrase(),
            100.0,
            None,
            false,
            &CancelToken::new(),
        )
        .unwrap();

        let files = index_files(&fx.repo_dir());
        for name in named_objects(&before) {
            assert!(files.contains(&name), "{label}: {name} was deleted");
        }
        assert_eq!(head(&fx.open()).retired, before.retired, "{label}");
    }
}

#[test]
fn segmented_fixtures_name_new_packs_by_their_hash() {
    for (prefix, mode) in FIXTURES {
        let fx = extract(prefix, mode);
        let algo = match *prefix {
            "v4" => HashAlgorithm::Blake2b,
            _ => HashAlgorithm::Blake3,
        };
        exercise_pack_naming(&fx.config(), fx.passphrase(), &fx.source_dir(), algo);
    }
}
