//! The **frozen** segmented-index (v4/v5) fixtures.
//!
//! The archives under `tests/fixtures/` were written by the binary named in
//! `scripts/gen-segmented-fixtures.sh` (development builds of formats 4 and
//! 5). Since 05 step 8 no binary opens v4/v5: these tests check that every
//! open, the upgrade's included, refuses them by version and leaves them
//! byte-identical. **Do not regenerate them** (see `tests/fixtures/README.md`);
//! they are deleted in 05 step 10.

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
// Test-only: `panic!` on a malformed fixture and `meta["key"]` indexing a
// `serde_json::Value` are the right failure modes.
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]

mod common;

use vykar_core::commands;
use vykar_core::repo::{OpenOptions, Repository};
use vykar_storage::local_backend::LocalBackend;
use vykar_types::error::VykarError;

use crate::common::fixtures::{self, Fixture, archive_path, read_tree};

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

/// FIX-1: every v4 and v5 fixture is refused by version, by a normal open,
/// by the upgrade's open and by `vykar upgrade`, and its files are unchanged.
#[test]
fn segmented_fixtures_are_refused_by_version() {
    for (prefix, mode) in FIXTURES {
        let label = format!("{prefix}-{mode}");
        let fx = extract(prefix, mode);
        let version = format_version(prefix);
        let before = read_tree(&fx.repo_dir());
        let refused = |e: VykarError, what: &str| {
            assert!(
                matches!(e, VykarError::UnsupportedVersion(v) if v == version),
                "{label} {what}: {e:?}"
            );
        };
        for opts in [
            OpenOptions::new().with_index(),
            OpenOptions::new().legacy_format(),
        ] {
            let storage = Box::new(LocalBackend::new(fx.repo_dir().to_str().unwrap()).unwrap());
            let err = Repository::open(storage, fx.passphrase(), Some(fx.cache_dir()), opts)
                .err()
                .expect("refused");
            refused(err, "open");
        }
        refused(
            commands::upgrade::run(&fx.config(), fx.passphrase()).expect_err("refused"),
            "upgrade",
        );
        assert_eq!(read_tree(&fx.repo_dir()), before, "{label}: files changed");
    }
}
