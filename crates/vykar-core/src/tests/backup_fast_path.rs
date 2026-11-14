//! Backup commit fast path: when the remote `index` blob is byte-identical to
//! the locally cached copy, the commit merges the local full index cache
//! instead of decoding the remote index. Any difference must fall back to the
//! slow path, and both paths must leave a consistent repository.

use std::fs;
use std::path::Path;

use crate::commands;
use crate::commands::backup::{self, BackupProgressEvent};
use crate::index::dedup_cache;

fn backup_with_stages(
    config: &crate::config::VykarConfig,
    src: &Path,
    name: &str,
) -> Vec<&'static str> {
    let source_paths = vec![src.to_string_lossy().to_string()];
    let mut stages = Vec::new();
    let mut on_event = |e: BackupProgressEvent| {
        if let BackupProgressEvent::CommitStage { stage } = e {
            stages.push(stage);
        }
    };
    backup::run_with_progress(
        config,
        backup::BackupRequest {
            snapshot_name: name,
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
        Some(&mut on_event),
        None,
    )
    .unwrap();
    stages
}

const FAST: &str = "index unchanged, fast path";
const SLOW: &str = "decode index";

#[test]
fn fast_path_taken_only_when_cached_blob_matches() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("a.bin"), vec![0x11u8; 64 * 1024]).unwrap();
    let config = crate::tests::helpers::init_repo(&repo_dir);

    backup_with_stages(&config, &src, "s1");

    // Unchanged remote index + valid local caches: fast path.
    fs::write(src.join("b.bin"), vec![0x22u8; 64 * 1024]).unwrap();
    let stages = backup_with_stages(&config, &src, "s2");
    assert!(stages.contains(&FAST), "expected fast path: {stages:?}");
    assert!(!stages.contains(&SLOW), "unexpected slow path: {stages:?}");

    // A cached blob that differs from the remote one in its last byte (same
    // length) must not be trusted.
    let repo_id = crate::tests::helpers::open_local_repo(&repo_dir)
        .config
        .id
        .clone();
    let blob_path = dedup_cache::index_blob_cache_path(&repo_id, None).unwrap();
    let mut cached = fs::read(&blob_path).unwrap();
    *cached.last_mut().unwrap() ^= 0x01;
    fs::write(&blob_path, &cached).unwrap();

    fs::write(src.join("c.bin"), vec![0x33u8; 64 * 1024]).unwrap();
    let stages = backup_with_stages(&config, &src, "s3");
    assert!(stages.contains(&SLOW), "expected slow path: {stages:?}");
    assert!(!stages.contains(&FAST), "unexpected fast path: {stages:?}");

    // The slow path re-caches the blob it wrote, so the next run is fast again.
    fs::write(src.join("d.bin"), vec![0x44u8; 64 * 1024]).unwrap();
    let stages = backup_with_stages(&config, &src, "s4");
    assert!(stages.contains(&FAST), "expected fast path: {stages:?}");

    let result = commands::check::run(&config, None, true, false).unwrap();
    assert!(
        result.errors.is_empty(),
        "check errors: {:?}",
        result.errors
    );
    assert_eq!(result.snapshots_checked, 4);
}
