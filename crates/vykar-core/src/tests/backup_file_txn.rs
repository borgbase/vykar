//! A segmented large file committed in pieces (`FileTxn`): a skip after
//! its first segment drops the file's item, reuse and stats but keeps its
//! chunks, which later files dedup against and compact sweeps; a failed
//! command dump leaves its packs in the session journal for the next run.
//!
//! A file is segmented only above the 64 MiB segment size, so these tests
//! back up one of 64 MiB + 64 KiB (two segments).

use std::fs;
use std::path::Path;

use crate::commands;
use crate::commands::backup;
use crate::config::{CommandDump, VykarConfig};
use crate::repo::control::CancelToken;
use crate::snapshot::item::Item;
use crate::tests::helpers::{
    PAST_T_PACK, age_repo_objects, make_test_config, open_local_repo, try_backup_single_source,
};

/// 64 MiB + 64 KiB: two segments.
const LARGE: usize = 64 * 1024 * 1024 + 64 * 1024;

/// `len` bytes of word-wise xorshift output (fast in a debug build, no
/// chunk repeats).
fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut x = 0x9e37_79b9_7f4a_7c15 ^ seed;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// One worker (segments are read in order), the test chunker of
/// `backup_metrics`, an initialised repository at `<dir>/repo`.
fn config_in(dir: &Path) -> VykarConfig {
    let mut config = make_test_config(&dir.join("repo"));
    config.limits.threads = 1;
    config.chunker.min_size = 1024;
    config.chunker.avg_size = 2048;
    config.chunker.max_size = 4096;
    config.xattrs.enabled = false;
    commands::init::run(&config, None).unwrap();
    config
}

fn items_of(config: &VykarConfig, snapshot: &str) -> Vec<Item> {
    let mut repo = open_local_repo(Path::new(&config.repository.url));
    commands::list::load_snapshot_items(&mut repo, snapshot, &CancelToken::new()).unwrap()
}

fn item<'a>(items: &'a [Item], name: &str) -> Option<&'a Item> {
    items
        .iter()
        .find(|i| i.path.rsplit('/').next() == Some(name))
}

fn unreferenced(config: &VykarConfig) -> commands::compact::UnreferencedReport {
    let mut repo = open_local_repo(Path::new(&config.repository.url));
    commands::compact::report_unreferenced_on_repo(&mut repo, &CancelToken::new()).unwrap()
}

fn check_clean(config: &VykarConfig) {
    let result = commands::check::run(config, None, true, false).unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
}

fn restore(config: &VykarConfig, snapshot: &str, dest: &Path) {
    commands::restore::run(
        config,
        None,
        snapshot,
        dest.to_str().unwrap(),
        None,
        false,
        true,
        &CancelToken::new(),
    )
    .unwrap();
}

/// The restored file `name` under `dest` (restore keeps the source's
/// absolute path below the destination).
fn restored(dest: &Path, name: &str) -> Vec<u8> {
    fn find(dir: &Path, name: &str) -> Option<std::path::PathBuf> {
        for entry in fs::read_dir(dir).ok()? {
            let path = entry.ok()?.path();
            if path.is_dir() {
                if let Some(p) = find(&path, name) {
                    return Some(p);
                }
            } else if path.file_name().is_some_and(|n| n == name) {
                return Some(path);
            }
        }
        None
    }
    let path = find(dest, name).unwrap_or_else(|| panic!("{name} not restored"));
    fs::read(path).unwrap()
}

/// A large file's first chunks go into the open pack after the files before
/// it: nothing seals the pack when the file starts.
#[test]
fn large_file_shares_a_pack_with_the_files_before_it() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    let config = config_in(dir.path());
    fs::write(src.join("a.bin"), pseudo_random(8 * 1024, 1)).unwrap();
    fs::write(src.join("z.bin"), pseudo_random(LARGE, 2)).unwrap();

    try_backup_single_source(&config, &src, "src", "s1").unwrap();

    let items = items_of(&config, "s1");
    let a = item(&items, "a.bin").unwrap().chunks[0].id;
    let z = item(&items, "z.bin").unwrap().chunks[0].id;
    let repo = open_local_repo(Path::new(&config.repository.url));
    let index = repo.chunk_index();
    assert_eq!(
        index.get(&a).unwrap().pack_id,
        index.get(&z).unwrap().pack_id
    );
}

/// Drift in the second segment skips `z.bin` after its first segment was
/// committed. The file is not in the snapshot and its share of the stats is
/// gone; its chunks stay as unreferenced entries, which `zz.bin` (a prefix
/// of `z.bin`, read later) dedups against and a compact sweeps once they
/// are old.
#[cfg(unix)]
#[test]
fn drift_in_a_later_segment_skips_the_file_and_keeps_its_chunks() {
    use std::io::Write;

    crate::testutil::init_test_environment();
    let _guard = crate::testutil::CWD_LOCK.lock().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    let config = config_in(dir.path());
    let z_data = pseudo_random(LARGE, 3);
    fs::write(src.join("a.txt"), b"a small file").unwrap();
    fs::write(src.join("z.bin"), &z_data).unwrap();
    fs::write(src.join("zz.bin"), &z_data[..1024 * 1024]).unwrap();

    // Fires once segment 1 is being read: segment 0 is committed.
    let z = src.join("z.bin");
    let append_to = z.clone();
    backup::read_source::test_hooks::install_hook(z, 64 * 1024 * 1024 + 1, move || {
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(&append_to)
            .unwrap();
        f.write_all(&[0xaa; 4096]).unwrap();
        f.sync_all().unwrap();
    });
    let outcome = try_backup_single_source(&config, &src, "src", "s1");
    backup::read_source::test_hooks::clear_hook();
    let outcome = outcome.unwrap();

    assert!(outcome.is_partial);
    assert_eq!(outcome.stats.errors, 1);
    assert_eq!(outcome.stats.nfiles, 2);
    let items = items_of(&config, "s1");
    assert!(
        item(&items, "z.bin").is_none(),
        "the skipped file is absent"
    );
    let small = item(&items, "a.txt").unwrap();
    let prefix = item(&items, "zz.bin").unwrap();
    // New bytes: a.txt and zz.bin's last chunk (cut at its end, so not one
    // of z.bin's). Every other chunk of zz.bin is one segment 0 of the
    // skipped z.bin stored, and adds nothing.
    let small_bytes: u64 = small.chunks.iter().map(|c| u64::from(c.csize)).sum();
    let last = prefix.chunks.last().unwrap();
    assert!(prefix.chunks.len() > 1);
    assert_eq!(
        outcome.stats.deduplicated_size,
        small_bytes + u64::from(last.csize),
        "zz.bin dedups against the skipped file's chunks"
    );

    let report = unreferenced(&config);
    assert!(
        !report.unreferenced.is_empty(),
        "segment 0's chunks are indexed, unreferenced"
    );
    check_clean(&config);

    let dest = dir.path().join("restore-1");
    restore(&config, "s1", &dest);
    assert_eq!(restored(&dest, "a.txt"), b"a small file");
    assert_eq!(restored(&dest, "zz.bin"), &z_data[..1024 * 1024]);

    // Once old, compact sweeps them.
    let repo_dir = Path::new(&config.repository.url);
    age_repo_objects(repo_dir, &["index.d", "packs"], PAST_T_PACK);
    let compacted =
        commands::compact::run(&config, None, 0.0, None, false, &CancelToken::new()).unwrap();
    assert!(compacted.chunks_unreferenced > 0, "{compacted:?}");
    assert!(unreferenced(&config).unreferenced.is_empty());
    check_clean(&config);
    let dest = dir.path().join("restore-2");
    restore(&config, "s1", &dest);
    assert_eq!(restored(&dest, "a.txt"), b"a small file");
    assert_eq!(restored(&dest, "zz.bin"), &z_data[..1024 * 1024]);
}

fn backup_with_dump(
    config: &VykarConfig,
    src: &Path,
    snapshot: &str,
    dump: &CommandDump,
) -> vykar_types::error::Result<backup::BackupOutcome> {
    let source_paths = vec![src.to_string_lossy().to_string()];
    backup::run(
        config,
        backup::BackupRequest {
            snapshot_name: snapshot,
            passphrase: None,
            source_paths: &source_paths,
            source_label: "src",
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: crate::compress::Compression::None,
            command_dumps: std::slice::from_ref(dump),
            verbose: false,
        },
    )
}

/// A dump that fails after its output fails the backup and leaves its packs
/// in the session journal; the next run recovers them and, the output being
/// the same, stores none of it again.
#[cfg(unix)]
#[test]
fn failed_dump_leaves_its_packs_in_the_journal() {
    const DUMP_LEN: usize = 2 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    let config = config_in(dir.path());
    let output = dir.path().join("dump-output");
    fs::write(&output, pseudo_random(DUMP_LEN, 4)).unwrap();
    let dump = |exit: u8| CommandDump {
        name: "db.dump".into(),
        command: format!("cat '{}'; exit {exit}", output.display()),
    };

    let err = backup_with_dump(&config, &src, "s1", &dump(1)).unwrap_err();
    assert!(err.to_string().contains("db.dump"), "{err}");
    let sessions = dir.path().join("repo").join("sessions");
    let journals: Vec<_> = fs::read_dir(&sessions)
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().into_string().unwrap();
            name.ends_with(".index").then_some(name)
        })
        .collect();
    assert_eq!(journals.len(), 1, "{journals:?}");

    let outcome = backup_with_dump(&config, &src, "s2", &dump(0)).unwrap();
    let items = items_of(&config, "s2");
    let dumped = item(&items, "db.dump").expect("the dump is in the snapshot");
    assert_eq!(dumped.size, DUMP_LEN as u64);
    assert!(
        outcome.stats.deduplicated_size < (DUMP_LEN / 4) as u64,
        "the dump's chunks were recovered, not stored again: {}",
        outcome.stats.deduplicated_size
    );
    check_clean(&config);
}

/// An unchanged dump stores nothing on the next backup: every dump chunk is
/// found again through the one chunk-store entry.
#[cfg(unix)]
#[test]
fn unchanged_dump_stores_nothing_on_the_second_backup() {
    const DUMP_LEN: usize = 2 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    let config = config_in(dir.path());
    let output = dir.path().join("dump-output");
    fs::write(&output, pseudo_random(DUMP_LEN, 5)).unwrap();
    let dump = CommandDump {
        name: "db.dump".into(),
        command: format!("cat '{}'", output.display()),
    };

    let first = backup_with_dump(&config, &src, "s1", &dump).unwrap();
    assert!(
        first.stats.deduplicated_size >= DUMP_LEN as u64,
        "{}",
        first.stats.deduplicated_size
    );
    let second = backup_with_dump(&config, &src, "s2", &dump).unwrap();
    assert_eq!(second.stats.deduplicated_size, 0);

    let chunks_of = |snapshot: &str| {
        let items = items_of(&config, snapshot);
        let dumped = item(&items, "db.dump").expect("the dump is in the snapshot");
        assert_eq!(dumped.size, DUMP_LEN as u64);
        dumped.chunks.clone()
    };
    assert_eq!(chunks_of("s1"), chunks_of("s2"));
    check_clean(&config);
}
