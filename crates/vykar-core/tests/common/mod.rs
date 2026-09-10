//! Fixtures shared by the `tests/*.rs` integration binaries.
//!
//! Each binary compiles this module independently and uses only part of it,
//! hence the blanket `dead_code` allow.

#![allow(dead_code)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
// Test-only env mutation; SAFETY per block.
#![allow(unsafe_code)]

use std::path::Path;
use std::sync::Once;

use vykar_core::commands;
use vykar_core::compress::Compression;
use vykar_core::config::{
    EncryptionConfig, EncryptionModeConfig, RepositoryConfig, SourceEntry, SourceHooksConfig,
    VykarConfig,
};
use vykar_core::repo::{OpenOptions, Repository};
use vykar_core::snapshot::SnapshotStats;
use vykar_storage::local_backend::LocalBackend;
use vykar_types::hash::HashAlgorithm;
use vykar_types::pack_id::PackId;

static TEST_ENV_INIT: Once = Once::new();

/// Point `HOME`/`XDG_CACHE_HOME` at a per-process temp directory so tests never
/// touch the developer's real cache. Idempotent.
pub fn init_test_environment() {
    TEST_ENV_INIT.call_once(|| {
        let base = std::env::temp_dir().join(format!("vykar-tests-{}", std::process::id()));
        let home = base.join("home");
        let cache = base.join("cache");
        let _ = std::fs::create_dir_all(&home);
        let _ = std::fs::create_dir_all(&cache);
        // SAFETY: Once::call_once runs this single-threaded at test-process
        // startup before any threads are spawned.
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("XDG_CACHE_HOME", &cache);
            // Windows reads these instead; without them the cache lands in the
            // real user profile.
            std::env::set_var("USERPROFILE", &home);
            std::env::set_var("LOCALAPPDATA", &cache);
        }
    });
}

/// Plaintext, local-backend config rooted at `repo_dir`.
pub fn make_test_config(repo_dir: &Path) -> VykarConfig {
    init_test_environment();

    VykarConfig {
        repository: RepositoryConfig {
            url: repo_dir.to_string_lossy().to_string(),
            min_pack_size: 32 * 1024 * 1024,
            max_pack_size: 512 * 1024 * 1024,
            ..Default::default()
        },
        encryption: EncryptionConfig {
            mode: EncryptionModeConfig::None,
            passphrase: None,
            passcommand: None,
        },
        one_file_system: true,
        ..Default::default()
    }
}

/// Open the repository at `repo_dir` with the chunk index loaded.
pub fn open_local_repo(repo_dir: &Path, passphrase: Option<&str>) -> Repository {
    init_test_environment();
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    Repository::open(storage, passphrase, None, OpenOptions::new().with_index()).unwrap()
}

/// Like [`open_local_repo`], but also enables the local file cache. Tests that
/// exercise snapshot-cache behaviour must use the uncached variant.
pub fn open_local_repo_cached(repo_dir: &Path, passphrase: Option<&str>) -> Repository {
    init_test_environment();
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    Repository::open(
        storage,
        passphrase,
        None,
        OpenOptions::new().with_index().with_file_cache(),
    )
    .unwrap()
}

/// Back up `source_dir` under `snapshot_name` with the standard test request.
pub fn backup_source(
    config: &VykarConfig,
    source_dir: &Path,
    source_label: &str,
    snapshot_name: &str,
    passphrase: Option<&str>,
    xattrs_enabled: bool,
) -> SnapshotStats {
    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    commands::backup::run(
        config,
        commands::backup::BackupRequest {
            snapshot_name,
            passphrase,
            source_paths: &source_paths,
            source_label,
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap()
    .stats
}

/// Single-path source entry with default hooks and no excludes.
pub fn source_entry(path: &Path, label: &str) -> SourceEntry {
    SourceEntry {
        paths: vec![path.to_string_lossy().to_string()],
        label: label.to_string(),
        exclude: Vec::new(),
        exclude_if_present: Vec::new(),
        one_file_system: true,
        git_ignore: false,
        xattrs_enabled: false,
        hooks: SourceHooksConfig::default(),
        retention: None,
        repos: Vec::new(),
        command_dumps: Vec::new(),
    }
}

/// Assert every `packs/<xx>/<id>` file in `repo_dir` is named by the `algo`
/// digest of its bytes and lives in the shard for its first byte.
///
/// Pack IDs follow the repository format (BLAKE2b for v2, BLAKE3 for v3);
/// nothing else in the suite checks the on-disk names, so a writer using the
/// wrong algorithm would otherwise pass unnoticed.
pub fn assert_pack_names(repo_dir: &Path, algo: HashAlgorithm) {
    let mut visited = 0usize;
    for shard in std::fs::read_dir(repo_dir.join("packs")).unwrap() {
        let shard = shard.unwrap().path();
        if !shard.is_dir() {
            continue;
        }
        let shard_name = shard.file_name().unwrap().to_string_lossy().into_owned();
        for pack in std::fs::read_dir(&shard).unwrap() {
            let pack = pack.unwrap().path();
            let name = pack.file_name().unwrap().to_string_lossy().into_owned();
            let expected = PackId::compute(&std::fs::read(&pack).unwrap(), algo).to_hex();
            assert_eq!(
                name,
                expected,
                "{} is not named by its {algo:?} digest",
                pack.display()
            );
            assert_eq!(
                shard_name,
                &name[..2],
                "{} is in the wrong shard directory",
                pack.display()
            );
            visited += 1;
        }
    }
    assert!(
        visited > 0,
        "no pack files found under {}",
        repo_dir.display()
    );
}

/// Deterministic, non-repeating filler (splitmix64) so nothing dedups against
/// itself.
fn pseudo_random_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        out.extend_from_slice(&z.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Write, delete, compact and restore so that packs produced by both a
/// backup and a repack are checked against `algo` via [`assert_pack_names`].
///
/// Two incompressible ~2 MiB files land in one data pack (well under the
/// test config's `min_pack_size`). Backing up, dropping one file, deleting the
/// first snapshot and compacting at threshold 0 leaves that pack partially
/// live, which forces a real repack rather than a whole-pack delete.
pub fn exercise_pack_naming(
    config: &VykarConfig,
    passphrase: Option<&str>,
    source_dir: &Path,
    algo: HashAlgorithm,
) {
    let repo_dir = Path::new(&config.repository.url);
    let xattrs = config.xattrs.enabled;

    let keep = pseudo_random_bytes(0x9E37_79B9_7F4A_7C15, 2 * 1024 * 1024);
    let drop = pseudo_random_bytes(0xD1B5_4A32_D192_ED03, 2 * 1024 * 1024);
    std::fs::write(source_dir.join("pn-keep.bin"), &keep).unwrap();
    std::fs::write(source_dir.join("pn-drop.bin"), &drop).unwrap();

    let stats = backup_source(
        config,
        source_dir,
        "pack-naming",
        "pn-a",
        passphrase,
        xattrs,
    );
    assert!(
        stats.deduplicated_size >= (keep.len() + drop.len()) as u64,
        "expected both files to be newly stored, got {} bytes",
        stats.deduplicated_size
    );
    assert_pack_names(repo_dir, algo);

    std::fs::remove_file(source_dir.join("pn-drop.bin")).unwrap();
    backup_source(
        config,
        source_dir,
        "pack-naming",
        "pn-b",
        passphrase,
        xattrs,
    );

    let deleted = commands::delete::run(config, passphrase, &["pn-a"], false, None).unwrap();
    assert!(deleted.warnings.is_empty(), "{:?}", deleted.warnings);

    let compact = commands::compact::run(config, passphrase, 0.0, None, false, None).unwrap();
    assert!(
        compact.packs_repacked > 0,
        "compaction repacked nothing, so repack pack names went unchecked: {compact:?}"
    );
    assert_pack_names(repo_dir, algo);

    let dest = repo_dir.parent().unwrap().join("pn-restore");
    commands::restore::run(
        config,
        passphrase,
        "pn-b",
        dest.to_str().unwrap(),
        None,
        xattrs,
        true,
        None,
    )
    .unwrap();
    assert_eq!(std::fs::read(dest.join("pn-keep.bin")).unwrap(), keep);
    assert!(!dest.join("pn-drop.bin").exists());
}
