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
use vykar_core::index::ChunkIndex;
use vykar_core::repo::control::CancelToken;
use vykar_core::repo::{OpenOptions, Repository, UpgradeOutcome};
use vykar_core::snapshot::item::ItemType;
use vykar_storage::local_backend::LocalBackend;

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
    index: &ChunkIndex,
) -> Vec<(
    vykar_types::chunk_id::ChunkId,
    vykar_core::index::ChunkIndexEntry,
)> {
    let mut out: Vec<_> = index.iter().map(|(id, e)| (*id, *e)).collect();
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
        &CancelToken::new(),
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

/// Every open but the upgrade's refuses a v3 fixture and writes nothing; the
/// upgrade's open reads its config, snapshots and legacy index.
#[test]
fn v3_fixtures_open_only_for_upgrade() {
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
            repo.config.version, 3,
            "{mode}: fixture must stay at format v3"
        );
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
        assert!(!repo.legacy_index().unwrap().is_empty(), "{mode}");
    }
}

/// A backup into a v3 fixture is refused and leaves no session marker and no
/// new pack.
#[test]
fn v3_fixture_backup_is_refused_and_leaves_no_session() {
    let fx = extract("none");
    let config = fx.config();
    let packs_before = pack_files(&fx);
    let source_paths = vec![fx.source_dir().to_string_lossy().to_string()];
    let err = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "refused",
            passphrase: fx.passphrase(),
            source_paths: &source_paths,
            source_label: "fixture-src",
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: vykar_core::compress::Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
    .err()
    .unwrap()
    .to_string();
    assert!(err.contains("vykar upgrade"), "{err}");
    let sessions = fx.repo_dir().join("sessions");
    let markers: Vec<_> = std::fs::read_dir(&sessions)
        .map(|d| d.map(|e| e.unwrap().file_name()).collect())
        .unwrap_or_default();
    assert!(markers.is_empty(), "{markers:?}");
    assert_eq!(pack_files(&fx), packs_before);
}

fn pack_files(fx: &Fixture) -> Vec<std::path::PathBuf> {
    let mut packs: Vec<_> = read_tree(&fx.repo_dir())
        .into_keys()
        .filter(|p| p.starts_with("packs"))
        .collect();
    packs.sort();
    packs
}

/// `vykar upgrade` takes each v3 fixture to v6 in place (UPG-1): the same
/// index, a head at `index.h/1` and no `index` or `index.gen`, every
/// check clean, the latest snapshot restores byte-identical, and a new backup
/// dedups against the chunk IDs 0.24 wrote.
#[test]
fn v3_fixtures_upgrade_to_v6() {
    for mode in MODES {
        let fx = extract(mode);
        let config = fx.config();
        // The upgrade carries over the live legacy entries (refcount above
        // 0) at their locations.
        let before = sorted_entries(&fx.open_legacy().legacy_index().unwrap().to_live_index());

        let outcome = commands::upgrade::run(&config, fx.passphrase()).unwrap();
        assert!(
            matches!(
                outcome,
                UpgradeOutcome::Upgraded {
                    from: 3,
                    to: 6,
                    chunk_hash: vykar_types::hash::HashAlgorithm::Blake3,
                    packs_unmapped: 0,
                    resumed: false,
                    ..
                }
            ),
            "{mode}: {outcome:?}"
        );
        assert!(!fx.repo_dir().join("index.gen").exists(), "{mode}");
        assert!(!fx.repo_dir().join("index").exists(), "{mode}");
        assert!(fx.repo_dir().join("index.h/1").is_file(), "{mode}");

        let repo = fx.open();
        assert_eq!(repo.config.version, 6, "{mode}");
        assert_eq!(repo.config.chunk_hash, "blake3", "{mode}");
        let head = repo.index_head().unwrap();
        assert_eq!(head.layout, 3, "{mode}");
        assert!(head.base.is_some() && head.segments.is_empty(), "{mode}");
        let maps = commands::pack_maps::run(&config, fx.passphrase(), &CancelToken::new()).unwrap();
        assert_eq!(maps.packs_mapped, 0, "{mode}: every pack already mapped");
        assert_eq!(
            before,
            sorted_entries(&repo.chunk_index().to_chunk_index()),
            "{mode}: the upgrade changed the index"
        );
        drop(repo);

        assert_check_clean(&fx, mode);
        assert_restores_source(&fx, "restore-upgraded", mode);
        assert_readers_match_whole_stream(&fx, mode);

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
        assert_formats_list_identically(&fx, "post-upgrade-snap", mode);
        assert!(
            matches!(
                commands::upgrade::run(&config, fx.passphrase()).unwrap(),
                UpgradeOutcome::AlreadyCurrent { version: 6 }
            ),
            "{mode}"
        );
    }
}

/// The fixture's latest snapshot (snapshot format 1, written by 0.24) and
/// `fresh`, a backup of the same tree by this binary (format 2), list the
/// same entries. Times, ownership, mode, csize, xattrs and directory sizes
/// are left out: tar extraction, the extracting user and the filesystem
/// change them (a directory's size is its filesystem's st_size), and csize is
/// a stored size, not content.
fn assert_formats_list_identically(fx: &Fixture, fresh: &str, label: &str) {
    type Entry = (
        String,
        ItemType,
        u64,
        Option<String>,
        Vec<(vykar_types::chunk_id::ChunkId, u32)>,
    );
    let mut repo = fx.open();
    let mut listing = |name: &str| -> Vec<Entry> {
        let mut out: Vec<Entry> =
            commands::list::load_snapshot_items(&mut repo, name, &CancelToken::new())
                .unwrap()
                .into_iter()
                .map(|i| {
                    let chunks = i.chunks.iter().map(|c| (c.id, c.size)).collect();
                    let size = if i.entry_type == ItemType::Directory {
                        0
                    } else {
                        i.size
                    };
                    (i.path, i.entry_type, size, i.link_target, chunks)
                })
                .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    };
    let old = listing(fx.latest_snapshot());
    let new = listing(fresh);
    assert!(!old.is_empty(), "{label}: empty fixture listing");
    assert_eq!(old, new, "{label}: format 1 and format 2 list differently");
    drop(listing);
    let version = |name: &str| {
        commands::list::load_snapshot_meta(&repo, name)
            .unwrap()
            .format_version
    };
    assert_eq!(version(fx.latest_snapshot()), 1, "{label}");
    assert_eq!(version(fresh), 2, "{label}");
}

/// The chunk-aware diff and the find that decodes each distinct tree chunk
/// once agree with whole-stream decodes on the fixture's format-1 snapshots.
fn assert_readers_match_whole_stream(fx: &Fixture, label: &str) {
    let config = fx.config();
    let mut repo = fx.open();
    let mut names: Vec<_> = repo.manifest().snapshots.clone();
    names.sort_by_key(|e| e.time);
    let mut items = |name: &str| {
        commands::list::load_snapshot_items(&mut repo, name, &CancelToken::new()).unwrap()
    };

    // Today's diff: regular files by display path, the last one winning.
    type Sig = (u64, Vec<vykar_types::chunk_id::ChunkId>);
    let files = |items: Vec<vykar_core::snapshot::item::Item>| {
        let mut map: std::collections::BTreeMap<String, Sig> = Default::default();
        for i in items {
            if i.entry_type == ItemType::RegularFile {
                map.insert(i.path, (i.size, i.chunks.iter().map(|c| c.id).collect()));
            }
        }
        map
    };
    let n = fx.snapshots.len();
    let (a, b) = (&fx.snapshots[n - 2], &fx.snapshots[n - 1]);
    let result = commands::diff::run(&config, fx.passphrase(), a, b).unwrap();
    let base = files(items(&result.base_snapshot));
    let target = files(items(&result.target_snapshot));
    let mut expected: Vec<(String, commands::diff::DiffChangeKind)> = Vec::new();
    for (path, sig) in &base {
        match target.get(path) {
            None => expected.push((path.clone(), commands::diff::DiffChangeKind::Removed)),
            Some(new) if new != sig => {
                expected.push((path.clone(), commands::diff::DiffChangeKind::Modified));
            }
            Some(_) => {}
        }
    }
    for path in target.keys() {
        if !base.contains_key(path) {
            expected.push((path.clone(), commands::diff::DiffChangeKind::Added));
        }
    }
    expected.sort_by(|x, y| x.0.cmp(&y.0));
    let got: Vec<_> = result
        .entries
        .iter()
        .map(|e| (e.path.clone(), e.change))
        .collect();
    assert_eq!(got, expected, "{label}: diff {a} {b}");

    // Today's find with an empty filter: every item of every snapshot, by
    // path, in snapshot time order.
    let mut expected: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for e in &names {
        for i in items(&e.name) {
            expected.entry(i.path).or_default().push(e.name.clone());
        }
    }
    let filter =
        commands::find::FindFilter::build(None, None, None, None, None, None, None).unwrap();
    let scope = commands::find::FindScope {
        source_label: None,
        last_n: None,
    };
    let got: std::collections::BTreeMap<String, Vec<String>> =
        commands::find::run(&config, fx.passphrase(), &scope, &filter)
            .unwrap()
            .into_iter()
            .map(|t| {
                let snaps = t.hits.into_iter().map(|h| h.hit.snapshot_name).collect();
                (t.path, snaps)
            })
            .collect();
    assert!(!got.is_empty(), "{label}");
    assert_eq!(got, expected, "{label}: find");
}

/// After the upgrade, pack maps make the repository recoverable without its
/// index: the fixture's packs carry no trailer.
#[test]
fn v3_fixtures_survive_index_loss_with_pack_maps() {
    for mode in MODES {
        fixtures::assert_index_loss_is_recovered_from_pack_maps("v3", mode, 3);
    }
}
