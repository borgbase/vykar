//! End-to-end tests for the backup TOCTOU (walk-to-open / intra-read) drift
//! detection paths exercised through the sequential and parallel backup code.
//!
//! Two deterministic mechanisms are used here:
//!
//! - **Mechanism A**: soft-error the open itself (e.g. chmod 000) so the
//!   sequential path takes the pre-checkpoint skip branch.
//! - **Mechanism C**: install a read hook via
//!   [`backup::read_source::test_hooks`] so the test thread can mutate the
//!   file after a deterministic byte threshold of the worker's read has
//!   elapsed, forcing the intra-read post-fstat check to fire.
//!
//! Synthetic pipeline / rollback tests (Mechanism B) live next to the code
//! they exercise in `commands/backup/pipeline.rs`.

// libc::geteuid for root detection; SAFETY documented per block.
#![allow(unsafe_code)]

use std::fs;
use std::io::Write;
use std::path::Path;

use crate::commands::backup;

fn init() {
    crate::testutil::init_test_environment();
}

fn write_file(path: &Path, bytes: &[u8]) {
    let mut f = fs::File::create(path).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
}

fn backup_to(
    config: &crate::config::VykarConfig,
    source: &Path,
    snapshot_name: &str,
) -> crate::commands::backup::BackupOutcome {
    let source_paths = vec![source.to_string_lossy().to_string()];
    backup::run(
        config,
        backup::BackupRequest {
            snapshot_name,
            passphrase: None,
            source_paths: &source_paths,
            source_label: "test",
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: crate::compress::Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    )
    .unwrap()
}

/// Mechanism C: truncation mid-read is caught by the intra-read post-fstat
/// check, and the whole file is skipped with a soft-error warning.
#[cfg(unix)]
#[test]
fn intra_read_truncation_is_skipped_with_warning() {
    init();
    let _guard = crate::testutil::CWD_LOCK.lock().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src_dir = dir.path().join("src");
    fs::create_dir_all(&src_dir).unwrap();

    let mut config = crate::tests::helpers::make_test_config(&repo_dir);
    // Force the sequential backup path (1 worker).
    config.limits.threads = 1;
    // Small chunker min_size so the file is a "large" file (hits the sequential
    // chunk-loop path rather than the cross-file batch).
    config.chunker.min_size = 1024;
    config.chunker.avg_size = 2048;
    config.chunker.max_size = 4096;
    crate::commands::init::run(&config, None).unwrap();

    // Large-enough file that the chunk loop will do at least one read before
    // the hook fires.
    let file = src_dir.join("big.bin");
    let data = vec![0x5au8; 64 * 1024];
    write_file(&file, &data);

    // Install a hook that truncates the file to 1 byte after the first read
    // event. The truncation changes size + mtime + ctime, so the post-fstat
    // intra-read check fails with VykarError::FileChangedDuringRead.
    let truncate_path = file.clone();
    backup::read_source::test_hooks::install_hook(file.clone(), 1, move || {
        let _ = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&truncate_path);
    });

    let outcome = backup_to(&config, &src_dir, "snap");
    backup::read_source::test_hooks::clear_hook();

    assert!(outcome.is_partial, "outcome should be partial");
    assert_eq!(outcome.stats.errors, 1, "one file should have been skipped");
    assert_eq!(
        outcome.stats.nfiles, 0,
        "nothing should have committed for the mutated file"
    );
}

/// Mechanism C (append): a mid-read append grows the file between `pre_meta`
/// and `read_to_end` completion. The read is hard-capped at `pre_meta.size + 1`
/// so the chunker/classifier cannot consume unbounded appended bytes, and the
/// `total_bytes != pre_meta.size` post-check then skips the file with a warning.
/// Exercises the sequential large-file path (`sequential.rs` chunker stream).
#[cfg(unix)]
#[test]
fn intra_read_append_sequential_large_is_skipped_with_warning() {
    init();
    let _guard = crate::testutil::CWD_LOCK.lock().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src_dir = dir.path().join("src");
    fs::create_dir_all(&src_dir).unwrap();

    let mut config = crate::tests::helpers::make_test_config(&repo_dir);
    config.limits.threads = 1;
    config.chunker.min_size = 1024;
    config.chunker.avg_size = 2048;
    config.chunker.max_size = 4096;
    crate::commands::init::run(&config, None).unwrap();

    let file = src_dir.join("big.bin");
    let data = vec![0x5au8; 64 * 1024];
    write_file(&file, &data);

    // After the first read, append 1 MiB. Without the `.take()` cap the
    // chunker would happily feed those appended bytes through compression /
    // encryption before the post-fstat check ran; with the cap, at most one
    // extra byte is read and the drift check skips the file.
    let append_path = file.clone();
    backup::read_source::test_hooks::install_hook(file.clone(), 1, move || {
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(&append_path)
            .unwrap();
        f.write_all(&vec![0xaau8; 1024 * 1024]).unwrap();
        f.sync_all().unwrap();
    });

    let outcome = backup_to(&config, &src_dir, "snap");
    backup::read_source::test_hooks::clear_hook();

    assert!(outcome.is_partial, "outcome should be partial");
    assert_eq!(outcome.stats.errors, 1, "one file should have been skipped");
    assert_eq!(
        outcome.stats.nfiles, 0,
        "nothing should have committed for the mutated file"
    );
}

/// Parallel-path variant of the append test covering `pipeline.rs` medium-file
/// chunker stream (site 2). Same regression coverage as the sequential large
/// test, but routed through the pipeline worker path (`threads > 1`).
#[cfg(unix)]
#[test]
fn intra_read_append_parallel_large_is_skipped_with_warning() {
    init();
    let _guard = crate::testutil::CWD_LOCK.lock().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src_dir = dir.path().join("src");
    fs::create_dir_all(&src_dir).unwrap();

    let mut config = crate::tests::helpers::make_test_config(&repo_dir);
    // Force the parallel pipeline path (>1 worker).
    config.limits.threads = 4;
    config.chunker.min_size = 1024;
    config.chunker.avg_size = 2048;
    config.chunker.max_size = 4096;
    crate::commands::init::run(&config, None).unwrap();

    let file = src_dir.join("big.bin");
    let data = vec![0x5au8; 64 * 1024];
    write_file(&file, &data);

    let append_path = file.clone();
    backup::read_source::test_hooks::install_hook(file.clone(), 1, move || {
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(&append_path)
            .unwrap();
        f.write_all(&vec![0xaau8; 1024 * 1024]).unwrap();
        f.sync_all().unwrap();
    });

    let outcome = backup_to(&config, &src_dir, "snap");
    backup::read_source::test_hooks::clear_hook();

    assert!(outcome.is_partial, "outcome should be partial");
    assert_eq!(outcome.stats.errors, 1, "one file should have been skipped");
    assert_eq!(
        outcome.stats.nfiles, 0,
        "nothing should have committed for the mutated file"
    );
}

/// Parallel-path small-file variant covering `pipeline.rs` small-file
/// single-chunk read (site 1). The file starts below `min_chunk_size` so it
/// takes the `read_to_end`-into-Vec branch; the append grows it past the
/// pre-sized capacity, and the `.take(size + 1)` cap bounds the allocation.
#[cfg(unix)]
#[test]
fn intra_read_append_parallel_small_is_skipped_with_warning() {
    init();
    let _guard = crate::testutil::CWD_LOCK.lock().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src_dir = dir.path().join("src");
    fs::create_dir_all(&src_dir).unwrap();

    let mut config = crate::tests::helpers::make_test_config(&repo_dir);
    config.limits.threads = 4;
    // Small file must be < min_size to hit the single-chunk Vec path.
    config.chunker.min_size = 32 * 1024;
    config.chunker.avg_size = 64 * 1024;
    config.chunker.max_size = 128 * 1024;
    crate::commands::init::run(&config, None).unwrap();

    let file = src_dir.join("small.bin");
    let data = vec![0x5au8; 4 * 1024];
    write_file(&file, &data);

    let append_path = file.clone();
    backup::read_source::test_hooks::install_hook(file.clone(), 1, move || {
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(&append_path)
            .unwrap();
        f.write_all(&vec![0xaau8; 1024 * 1024]).unwrap();
        f.sync_all().unwrap();
    });

    let outcome = backup_to(&config, &src_dir, "snap");
    backup::read_source::test_hooks::clear_hook();

    assert!(outcome.is_partial, "outcome should be partial");
    assert_eq!(outcome.stats.errors, 1, "one file should have been skipped");
    assert_eq!(
        outcome.stats.nfiles, 0,
        "nothing should have committed for the mutated file"
    );
}

/// Mechanism A: a chmod-000 file causes a soft open error in the sequential
/// path. The pre-checkpoint open-skip branch must count the file and still
/// commit the sibling — if a rollback tracker were leaked, the next call to
/// `begin_rollback_checkpoint` for the readable file would panic and no
/// snapshot would land.
#[cfg(unix)]
#[test]
fn sequential_soft_open_error_skips_bad_commits_good() {
    use std::os::unix::fs::PermissionsExt;
    init();
    let _guard = crate::testutil::CWD_LOCK.lock().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src_dir = dir.path().join("src");
    fs::create_dir_all(&src_dir).unwrap();

    let mut config = crate::tests::helpers::make_test_config(&repo_dir);
    config.limits.threads = 1;
    config.chunker.min_size = 1024;
    config.chunker.avg_size = 2048;
    config.chunker.max_size = 4096;
    crate::commands::init::run(&config, None).unwrap();

    // Two files: one good, one unreadable.
    let good = src_dir.join("good.bin");
    write_file(&good, &vec![0x11u8; 10 * 1024]);
    let bad = src_dir.join("bad.bin");
    write_file(&bad, &vec![0x22u8; 10 * 1024]);
    fs::set_permissions(&bad, fs::Permissions::from_mode(0o000)).unwrap();

    let outcome = backup_to(&config, &src_dir, "snap");

    // Cleanup permissions before tempdir drop.
    fs::set_permissions(&bad, fs::Permissions::from_mode(0o644)).unwrap();

    // When running as root the chmod-000 has no effect; in that case the
    // test degenerates to a happy-path check (the readable-file assertion
    // still holds).
    // SAFETY: geteuid takes no arguments and is always sound; the result is
    // a plain u32.
    let is_root = unsafe { libc::geteuid() == 0 };
    if !is_root {
        assert_eq!(outcome.stats.errors, 1, "permission denied should skip 1");
    }
    assert!(
        outcome.stats.nfiles >= 1,
        "the readable file should have committed"
    );
}
