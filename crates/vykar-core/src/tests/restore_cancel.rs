//! Cancellation at the public restore entry points.
//!
//! The fine-grained cases (mid-stream, and the window between staging and the
//! rename) live in `commands::restore`'s own test module, where the private
//! `restore_with_filter` and the pre-publish injection hook are reachable.
//! What is pinned here is the public contract: `run` / `run_selected` honour
//! the flag, report `Interrupted`, and leave nothing behind at the destination.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use vykar_types::error::VykarError;

use crate::commands;
use crate::config::VykarConfig;

use super::helpers::{backup_single_source, init_repo};

fn fixture(tmp: &Path) -> (VykarConfig, std::path::PathBuf) {
    let repo_dir = tmp.join("repo");
    let source_dir = tmp.join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("a.txt"), b"alpha").unwrap();
    std::fs::write(source_dir.join("b.txt"), b"bravo").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src", "snap-1");
    (config, tmp.join("dest"))
}

/// A destination that either does not exist or holds nothing — in particular no
/// `.vykar-restore-*` staging directory.
fn assert_dest_clean(dest: &Path) {
    if !dest.exists() {
        return;
    }
    let entries: Vec<String> = std::fs::read_dir(dest)
        .unwrap()
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    assert!(
        entries.is_empty(),
        "destination must be empty, found: {entries:?}"
    );
}

#[test]
fn pre_cancelled_restore_run_writes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, dest) = fixture(tmp.path());

    let flag = AtomicBool::new(true);
    let err = commands::restore::run(
        &config,
        None,
        "snap-1",
        dest.to_str().unwrap(),
        None,
        false,
        false,
        Some(&flag),
    )
    .unwrap_err();

    assert!(
        matches!(err, VykarError::Interrupted),
        "expected Interrupted, got: {err}"
    );
    assert_dest_clean(&dest);
}

#[test]
fn pre_cancelled_restore_run_selected_writes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, dest) = fixture(tmp.path());

    let mut selected = HashSet::new();
    selected.insert("a.txt".to_string());

    let flag = AtomicBool::new(true);
    let err = commands::restore::run_selected(
        &config,
        None,
        "snap-1",
        dest.to_str().unwrap(),
        &selected,
        false,
        false,
        Some(&flag),
    )
    .unwrap_err();

    assert!(
        matches!(err, VykarError::Interrupted),
        "expected Interrupted, got: {err}"
    );
    assert_dest_clean(&dest);
}

/// An unset flag must not change behaviour: the same call restores normally.
/// Without this the two assertions above could pass for the wrong reason.
#[test]
fn restore_with_unset_flag_completes_normally() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, dest) = fixture(tmp.path());

    let flag = AtomicBool::new(false);
    let stats = commands::restore::run(
        &config,
        None,
        "snap-1",
        dest.to_str().unwrap(),
        None,
        false,
        false,
        Some(&flag),
    )
    .unwrap();

    assert_eq!(stats.files, 2);
    assert!(dest.exists() && std::fs::read_dir(&dest).unwrap().count() > 0);
}
