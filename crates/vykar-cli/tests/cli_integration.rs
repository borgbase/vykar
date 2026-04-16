use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use tempfile::TempDir;
use vykar_core::repo::{OpenOptions, Repository};
use vykar_storage::local_backend::LocalBackend;

struct CliFixture {
    _tmp: TempDir,
    home_dir: PathBuf,
    cache_dir: PathBuf,
    config_home: PathBuf,
    repo_dir: PathBuf,
    source_a: PathBuf,
    source_b: PathBuf,
    config_path: PathBuf,
}

impl CliFixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let home_dir = tmp.path().join("home");
        let cache_dir = tmp.path().join("cache");
        let config_home = tmp.path().join("config-home");
        let repo_dir = tmp.path().join("repo");
        let source_a = tmp.path().join("source-a");
        let source_b = tmp.path().join("source-b");
        let config_path = tmp.path().join("vykar.yaml");

        std::fs::create_dir_all(&home_dir).unwrap();
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&source_a).unwrap();
        std::fs::create_dir_all(&source_b).unwrap();

        Self {
            _tmp: tmp,
            home_dir,
            cache_dir,
            config_home,
            repo_dir,
            source_a,
            source_b,
            config_path,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut cmd = Command::new(vykar_binary_path());
        cmd.args(args);
        cmd.env("HOME", &self.home_dir);
        cmd.env("XDG_CACHE_HOME", &self.cache_dir);
        cmd.env("XDG_CONFIG_HOME", &self.config_home);
        cmd.env("NO_COLOR", "1");
        cmd.output().unwrap()
    }

    fn run_ok(&self, args: &[&str]) -> String {
        let output = self.run(args);
        if !output.status.success() {
            panic!(
                "command failed: {:?}\nstdout:\n{}\nstderr:\n{}",
                args,
                stdout(&output),
                stderr(&output)
            );
        }
        stdout(&output)
    }

    fn run_err(&self, args: &[&str]) -> (String, String) {
        let output = self.run(args);
        assert!(
            !output.status.success(),
            "command unexpectedly succeeded: {:?}\nstdout:\n{}\nstderr:\n{}",
            args,
            stdout(&output),
            stderr(&output)
        );
        (stdout(&output), stderr(&output))
    }

    fn run_with_stdin(&self, args: &[&str], stdin_data: &str) -> Output {
        use std::io::Write;
        use std::process::Stdio;

        let mut child = Command::new(vykar_binary_path());
        child
            .args(args)
            .env("HOME", &self.home_dir)
            .env("XDG_CACHE_HOME", &self.cache_dir)
            .env("XDG_CONFIG_HOME", &self.config_home)
            .env("NO_COLOR", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = child.spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(stdin_data.as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn vykar_binary_path() -> PathBuf {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_vykar") {
        return PathBuf::from(path);
    }

    let current_exe = std::env::current_exe().expect("failed to resolve current test binary path");
    let debug_dir = current_exe
        .parent()
        .and_then(|p| p.parent())
        .expect("unexpected test binary path layout");

    #[cfg(windows)]
    let candidate = debug_dir.join("vykar.exe");
    #[cfg(not(windows))]
    let candidate = debug_dir.join("vykar");

    assert!(
        candidate.exists(),
        "unable to locate vykar binary at {:?}",
        candidate
    );
    candidate
}

fn yaml_quote_path(path: &Path) -> String {
    let raw = path.to_string_lossy();
    format!("\"{}\"", raw.replace('\\', "\\\\").replace('"', "\\\""))
}

fn write_plain_config(config_path: &Path, repo_dir: &Path) {
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\n",
        yaml_quote_path(repo_dir)
    );
    std::fs::write(config_path, config).unwrap();
}

fn write_sources_config(
    config_path: &Path,
    repo_dir: &Path,
    source_a: &Path,
    source_b: &Path,
    keep_last: usize,
) {
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nretention:\n  keep_last: {}\nsources:\n  - path: {}\n    label: src-a\n  - path: {}\n    label: src-b\n",
        yaml_quote_path(repo_dir),
        keep_last,
        yaml_quote_path(source_a),
        yaml_quote_path(source_b)
    );
    std::fs::write(config_path, config).unwrap();
}

fn parse_snapshot_name(output: &str) -> String {
    output
        .lines()
        .find_map(|line| line.strip_prefix("Snapshot created: "))
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| panic!("missing snapshot name in output:\n{output}"))
}

fn delete_pack_for_first_chunk(repo_dir: &Path) {
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    let repo = Repository::open(storage, None, None, OpenOptions::new().with_index()).unwrap();
    let (_chunk_id, entry) = repo
        .chunk_index()
        .iter()
        .next()
        .expect("repo must contain at least one chunk");
    let pack_path = repo_dir.join(entry.pack_id.storage_key());
    assert!(
        pack_path.exists(),
        "expected pack file to exist: {pack_path:?}"
    );
    std::fs::remove_file(pack_path).unwrap();
}

fn corrupt_first_snapshot(repo_dir: &Path) {
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    let repo = Repository::open(storage, None, None, OpenOptions::new().with_index()).unwrap();
    let entry = repo
        .manifest()
        .snapshots
        .first()
        .expect("must have snapshot");
    let snap_path = repo_dir.join(entry.id.storage_key());
    std::fs::write(snap_path, b"garbage-snapshot-data").unwrap();
}

#[test]
fn cli_init_backup_list_restore_info_roundtrip() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("alpha.txt"), b"alpha file\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();
    let restore = fx._tmp.path().join("restore");
    let restore_str = restore.to_string_lossy().to_string();

    let init_out = fx.run_ok(&["--config", &cfg, "init"]);
    assert!(init_out.contains("Repository initialized at:"));

    let backup_out = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snapshot = parse_snapshot_name(&backup_out);

    let list_out = fx.run_ok(&["--config", &cfg, "list"]);
    assert!(list_out.contains(&snapshot));

    let restore_out = fx.run_ok(&["--config", &cfg, "restore", &snapshot, &restore_str]);
    assert!(restore_out.contains("Restored:"));

    assert_eq!(
        std::fs::read_to_string(restore.join("alpha.txt")).unwrap(),
        "alpha file\n"
    );

    let info_out = fx.run_ok(&["--config", &cfg, "info"]);
    assert!(info_out.contains("Snapshots"));
    assert!(info_out.contains("Encryption"));
}

#[test]
fn cli_delete_dry_run_then_real_delete() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("delete.txt"), b"delete me\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    let backup_out = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snapshot = parse_snapshot_name(&backup_out);

    let dry_run_out = fx.run_ok(&[
        "--config",
        &cfg,
        "snapshot",
        "delete",
        &snapshot,
        "--dry-run",
    ]);
    assert!(dry_run_out.contains("Dry run: would delete snapshot"));
    assert!(dry_run_out.contains(&snapshot));

    let list_before = fx.run_ok(&["--config", &cfg, "list"]);
    assert!(list_before.contains(&snapshot));

    let delete_out = fx.run_ok(&["--config", &cfg, "snapshot", "delete", &snapshot]);
    assert!(delete_out.contains("Deleted snapshot"));
    assert!(delete_out.contains(&snapshot));

    let list_after = fx.run_ok(&["--config", &cfg, "list"]);
    assert!(list_after.contains("No snapshots found."));
}

#[test]
fn cli_prune_list_source_filter_and_mutation() {
    let fx = CliFixture::new();
    write_sources_config(&fx.config_path, &fx.repo_dir, &fx.source_a, &fx.source_b, 1);
    std::fs::write(fx.source_a.join("a.txt"), b"a-v1").unwrap();
    std::fs::write(fx.source_b.join("b.txt"), b"b-v1").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.run_ok(&["--config", &cfg, "init"]);

    let a1_out = fx.run_ok(&["--config", &cfg, "backup", "--source", "src-a"]);
    let snap_a1 = parse_snapshot_name(&a1_out);
    std::thread::sleep(Duration::from_millis(2));

    std::fs::write(fx.source_a.join("a.txt"), b"a-v2").unwrap();
    let a2_out = fx.run_ok(&["--config", &cfg, "backup", "--source", "src-a"]);
    let snap_a2 = parse_snapshot_name(&a2_out);
    std::thread::sleep(Duration::from_millis(2));

    let b1_out = fx.run_ok(&["--config", &cfg, "backup", "--source", "src-b"]);
    let snap_b1 = parse_snapshot_name(&b1_out);

    let dry_out = fx.run_ok(&[
        "--config",
        &cfg,
        "prune",
        "--source",
        "src-a",
        "--list",
        "--dry-run",
    ]);
    assert!(dry_out.contains("keep"));
    assert!(dry_out.contains("prune"));

    let prune_out = fx.run_ok(&["--config", &cfg, "prune", "--source", "src-a", "--list"]);
    assert!(prune_out.contains("Pruned 1 snapshots"));

    let list_after = fx.run_ok(&["--config", &cfg, "list"]);
    assert!(!list_after.contains(&snap_a1));
    assert!(list_after.contains(&snap_a2));
    assert!(list_after.contains(&snap_b1));
}

#[test]
fn cli_snapshot_find_timeline_and_filters() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    // Create initial files
    std::fs::create_dir_all(fx.source_a.join("sub")).unwrap();
    std::fs::write(fx.source_a.join("hello.txt"), b"hello v1").unwrap();
    std::fs::write(fx.source_a.join("sub/deep.rs"), b"fn main() {}").unwrap();
    std::fs::write(fx.source_a.join("big.bin"), vec![0u8; 2048]).unwrap();

    fx.run_ok(&["--config", &cfg, "init"]);

    // First backup
    let out1 = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snap1 = parse_snapshot_name(&out1);
    std::thread::sleep(Duration::from_millis(10));

    // Modify hello.txt, leave others unchanged
    std::fs::write(fx.source_a.join("hello.txt"), b"hello v2").unwrap();

    // Second backup
    let out2 = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snap2 = parse_snapshot_name(&out2);

    // --- Basic find (no filters) returns all files across both snapshots ---
    let find_all = fx.run_ok(&["--config", &cfg, "snapshot", "find"]);
    assert!(find_all.contains("hello.txt"), "should list hello.txt");
    assert!(find_all.contains("big.bin"), "should list big.bin");
    assert!(find_all.contains(&snap1), "should reference first snapshot");
    assert!(
        find_all.contains(&snap2),
        "should reference second snapshot"
    );

    // --- Status annotations ---
    // hello.txt was added in snap1, modified in snap2
    assert!(find_all.contains("added"), "should show 'added' status");
    assert!(
        find_all.contains("modified"),
        "should show 'modified' status"
    );
    // big.bin and sub/deep.rs are unchanged in snap2
    assert!(
        find_all.contains("unchanged"),
        "should show 'unchanged' status"
    );

    // --- Filter by --name glob ---
    let find_rs = fx.run_ok(&["--config", &cfg, "snapshot", "find", "--name", "*.rs"]);
    assert!(find_rs.contains("deep.rs"), "glob should match .rs files");
    assert!(
        !find_rs.contains("hello.txt"),
        "glob should exclude .txt files"
    );
    assert!(
        !find_rs.contains("big.bin"),
        "glob should exclude .bin files"
    );

    // --- Filter by --type ---
    let find_dirs = fx.run_ok(&["--config", &cfg, "snapshot", "find", "--type", "d"]);
    assert!(find_dirs.contains("sub"), "should find 'sub' directory");
    assert!(
        !find_dirs.contains("hello.txt"),
        "should exclude regular files"
    );

    // --- Filter by --path subtree ---
    let find_sub = fx.run_ok(&["--config", &cfg, "snapshot", "find", "sub"]);
    assert!(
        find_sub.contains("deep.rs"),
        "path filter should include sub/deep.rs"
    );
    assert!(
        !find_sub.contains("hello.txt"),
        "path filter should exclude hello.txt"
    );

    // --- Filter by --larger ---
    let find_large = fx.run_ok(&["--config", &cfg, "snapshot", "find", "--larger", "1K"]);
    assert!(
        find_large.contains("big.bin"),
        "should include 2 KiB file when --larger 1K"
    );
    assert!(
        !find_large.contains("hello.txt"),
        "should exclude small file when --larger 1K"
    );

    // --- Filter by --smaller ---
    let find_small = fx.run_ok(&["--config", &cfg, "snapshot", "find", "--smaller", "100"]);
    assert!(
        find_small.contains("hello.txt"),
        "should include small file when --smaller 100"
    );
    assert!(
        !find_small.contains("big.bin"),
        "should exclude large file when --smaller 100"
    );

    // --- Filter by --last ---
    let find_last1 = fx.run_ok(&["--config", &cfg, "snapshot", "find", "--last", "1"]);
    assert!(
        !find_last1.contains(&snap1),
        "--last 1 should exclude first snapshot"
    );
    assert!(
        find_last1.contains(&snap2),
        "--last 1 should include second snapshot"
    );
    // With only one snapshot, all files are "added"
    assert!(
        !find_last1.contains("unchanged"),
        "--last 1 should not show 'unchanged'"
    );

    // --- --last 0 is rejected ---
    let (_stdout, stderr) = fx.run_err(&["--config", &cfg, "snapshot", "find", "--last", "0"]);
    assert!(
        stderr.contains("0") || stderr.contains("invalid"),
        "--last 0 should be rejected"
    );

    // --- --since with invalid value is rejected ---
    let (_stdout, stderr) = fx.run_err(&["--config", &cfg, "snapshot", "find", "--since", "0h"]);
    assert!(
        stderr.contains("positive") || stderr.contains("must be"),
        "--since 0h should be rejected"
    );
}

#[test]
fn cli_delete_repo_with_flag() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("repo-del.txt"), b"delete repo\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    assert!(fx.repo_dir.join("config").exists());

    let delete_out = fx.run_ok(&["--config", &cfg, "delete", "--yes-delete-this-repo"]);
    assert!(delete_out.contains("deleted"));
    assert!(!fx.repo_dir.exists());
}

#[test]
fn cli_check_verify_data_detects_tampered_pack() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("check.txt"), b"check data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    let check_ok = fx.run_ok(&["--config", &cfg, "check", "--verify-data"]);
    assert!(check_ok.contains("0 errors"));

    delete_pack_for_first_chunk(&fx.repo_dir);
    let (stdout_err, _stderr_err) = fx.run_err(&["--config", &cfg, "check", "--verify-data"]);
    assert!(stdout_err.contains("Errors found:"));
    assert!(stdout_err.contains("missing from storage"));
}

#[test]
fn cli_restore_latest_alias() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("latest.txt"), b"latest test\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();
    let restore = fx._tmp.path().join("restore-latest");
    let restore_str = restore.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    let restore_out = fx.run_ok(&["--config", &cfg, "restore", "latest", &restore_str]);
    assert!(restore_out.contains("Restored:"));

    assert_eq!(
        std::fs::read_to_string(restore.join("latest.txt")).unwrap(),
        "latest test\n"
    );
}

#[test]
fn cli_restore_missing_dest_fails() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();

    // "restore latest" without a dest should fail with a usage error
    let (_stdout, stderr) = fx.run_err(&["--config", &cfg, "restore", "latest"]);
    assert!(
        stderr.contains("DEST") || stderr.contains("required"),
        "expected usage error about missing dest, got:\n{stderr}"
    );
}

#[test]
fn cli_snapshot_list_latest_alias() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    // Snapshot 1: only old.txt
    std::fs::write(fx.source_a.join("old.txt"), b"old\n").unwrap();
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    // Snapshot 2: add unique.txt (old.txt still present)
    std::fs::write(fx.source_a.join("unique.txt"), b"unique\n").unwrap();
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    let out = fx.run_ok(&["--config", &cfg, "snapshot", "list", "latest"]);
    assert!(
        out.contains("unique.txt"),
        "expected snapshot list latest to show content from newest snapshot, got:\n{out}"
    );
}

#[test]
fn cli_snapshot_info_latest_alias() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("info.txt"), b"info\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    // Snapshot 1
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    // Snapshot 2 — this should be the one "latest" resolves to
    std::fs::write(fx.source_a.join("extra.txt"), b"extra\n").unwrap();
    let backup2_out = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snap2_name = parse_snapshot_name(&backup2_out);

    let out = fx.run_ok(&["--config", &cfg, "snapshot", "info", "latest"]);
    assert!(
        out.contains(&snap2_name),
        "expected snapshot info latest to report second snapshot '{snap2_name}', got:\n{out}"
    );
}

#[test]
fn cli_snapshot_delete_latest_rejects_alias() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("del.txt"), b"del\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    let (stdout, stderr) = fx.run_err(&["--config", &cfg, "snapshot", "delete", "latest"]);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("not found") || combined.contains("Not found"),
        "expected snapshot delete latest to fail with not-found, got:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
fn cli_bulk_snapshot_delete() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    std::fs::write(fx.source_a.join("a.txt"), b"aaa").unwrap();
    let out1 = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snap1 = parse_snapshot_name(&out1);

    std::fs::write(fx.source_a.join("b.txt"), b"bbb").unwrap();
    let out2 = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snap2 = parse_snapshot_name(&out2);

    std::fs::write(fx.source_a.join("c.txt"), b"ccc").unwrap();
    let out3 = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snap3 = parse_snapshot_name(&out3);

    // Delete two snapshots in one command.
    let delete_out = fx.run_ok(&["--config", &cfg, "snapshot", "delete", &snap1, &snap2]);
    assert!(delete_out.contains(&format!("Deleted snapshot '{snap1}'")));
    assert!(delete_out.contains(&format!("Deleted snapshot '{snap2}'")));
    assert!(delete_out.contains("Total:"));

    // Third snapshot survives.
    let list_after = fx.run_ok(&["--config", &cfg, "list"]);
    assert!(!list_after.contains(&snap1));
    assert!(!list_after.contains(&snap2));
    assert!(list_after.contains(&snap3));
}

#[test]
fn cli_bulk_delete_requires_repo_flag_with_multiple_repos() {
    let fx = CliFixture::new();
    let repo_b = fx._tmp.path().join("repo_b");
    std::fs::create_dir_all(&repo_b).unwrap();

    // Config with two repositories.
    let config = format!(
        "repositories:\n  - url: {}\n    label: repo-a\n  - url: {}\n    label: repo-b\nencryption:\n  mode: none\nsources: []\n",
        yaml_quote_path(&fx.repo_dir),
        yaml_quote_path(&repo_b),
    );
    std::fs::write(&fx.config_path, config).unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.run_ok(&["--config", &cfg, "init", "-R", "repo-a"]);
    fx.run_ok(&["--config", &cfg, "init", "-R", "repo-b"]);

    let source = fx.source_a.to_string_lossy().to_string();
    std::fs::write(fx.source_a.join("file.txt"), b"data").unwrap();

    let out1 = fx.run_ok(&["--config", &cfg, "backup", "-R", "repo-a", &source]);
    let snap1 = parse_snapshot_name(&out1);

    std::fs::write(fx.source_a.join("file2.txt"), b"data2").unwrap();
    let out2 = fx.run_ok(&["--config", &cfg, "backup", "-R", "repo-a", &source]);
    let snap2 = parse_snapshot_name(&out2);

    // Bulk delete without -R should fail.
    let (_stdout, stderr) = fx.run_err(&["--config", &cfg, "snapshot", "delete", &snap1, &snap2]);
    assert!(
        stderr.contains("requires -R") || stderr.contains("requires --repo"),
        "expected -R requirement error, got:\n{stderr}"
    );

    // Same command with -R should succeed.
    let delete_out = fx.run_ok(&[
        "--config", &cfg, "snapshot", "delete", "-R", "repo-a", &snap1, &snap2,
    ]);
    assert!(delete_out.contains(&format!("Deleted snapshot '{snap1}'")));
}

// ── Backup --threads tests ──────────────────────────────────────────────────

#[test]
fn cli_backup_threads_out_of_range_rejected() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    let (_stdout, stderr) = fx.run_err(&["--config", &cfg, "backup", "--threads", "200", &source]);
    assert!(
        stderr.contains("200") || stderr.contains("invalid"),
        "--threads 200 should be rejected, got:\n{stderr}"
    );
}

// ── Daemon tests ────────────────────────────────────────────────────────────

#[test]
fn cli_daemon_rejects_disabled_schedule() {
    let fx = CliFixture::new();
    // Default config has schedule.enabled=false
    write_plain_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();

    let (_stdout, stderr) = fx.run_err(&["--config", &cfg, "daemon"]);
    assert!(
        stderr.contains("schedule.enabled is false"),
        "expected schedule.enabled error, got:\n{stderr}"
    );
}

#[test]
fn cli_daemon_encrypted_without_passphrase_fails() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    // Ensure no env passphrase leaks in from the test runner
    let output = Command::new(vykar_binary_path())
        .args(["--config", &cfg, "daemon"])
        .env("HOME", &fx.home_dir)
        .env("XDG_CACHE_HOME", &fx.cache_dir)
        .env("XDG_CONFIG_HOME", &fx.config_home)
        .env("NO_COLOR", "1")
        .env_remove("VYKAR_PASSPHRASE")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr_str = stderr(&output);
    assert!(
        stderr_str.contains("no non-interactive passphrase source"),
        "expected passphrase error, got:\n{stderr_str}"
    );
}

#[test]
#[cfg(unix)]
fn cli_daemon_on_startup_and_shutdown() {
    use std::process::Stdio;

    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    // Init repo first
    fx.run_ok(&["--config", &cfg, "init"]);

    // Spawn daemon as a background process
    let mut child = Command::new(vykar_binary_path())
        .args(["--config", &cfg, "daemon"])
        .env("HOME", &fx.home_dir)
        .env("XDG_CACHE_HOME", &fx.cache_dir)
        .env("XDG_CONFIG_HOME", &fx.config_home)
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Wait for the on_startup backup cycle to complete (empty sources = fast)
    std::thread::sleep(Duration::from_secs(3));

    // Send SIGTERM for graceful shutdown
    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();

    // Wait for clean exit with a deadline to avoid hanging
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match child.try_wait().unwrap() {
            Some(_) => break,
            None => {
                if std::time::Instant::now() > deadline {
                    child.kill().unwrap();
                    panic!("daemon did not exit within 10 seconds after SIGTERM");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    let output = child.wait_with_output().unwrap();
    let stderr_str = String::from_utf8_lossy(&output.stderr).to_string();

    // Verify backup cycle ran
    assert!(
        stderr_str.contains("Summary"),
        "daemon should have run a backup cycle, got stderr:\n{stderr_str}"
    );

    // Verify clean exit
    assert!(
        output.status.success(),
        "daemon should exit cleanly after SIGTERM, got status: {}, stderr:\n{stderr_str}",
        output.status
    );
}

#[test]
#[cfg(unix)]
fn cli_daemon_second_instance_rejected_by_scheduler_lock() {
    use std::process::Stdio;

    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    // Spawn the first daemon — it should acquire the scheduler lock.
    let mut first = Command::new(vykar_binary_path())
        .args(["--config", &cfg, "daemon"])
        .env("HOME", &fx.home_dir)
        .env("XDG_CACHE_HOME", &fx.cache_dir)
        .env("XDG_CONFIG_HOME", &fx.config_home)
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Give the first daemon time to acquire the lock.
    std::thread::sleep(Duration::from_secs(2));

    // Verify the first daemon is still running (didn't crash).
    assert!(
        first.try_wait().unwrap().is_none(),
        "first daemon should still be running"
    );

    // Spawn the second daemon — it should fail immediately with the lock error.
    let second_output = Command::new(vykar_binary_path())
        .args(["--config", &cfg, "daemon"])
        .env("HOME", &fx.home_dir)
        .env("XDG_CACHE_HOME", &fx.cache_dir)
        .env("XDG_CONFIG_HOME", &fx.config_home)
        .env("NO_COLOR", "1")
        .output()
        .unwrap();

    assert!(
        !second_output.status.success(),
        "second daemon should exit with non-zero status"
    );
    let second_stderr = stderr(&second_output);
    assert!(
        second_stderr.contains("another vykar scheduler is already running"),
        "expected scheduler lock error, got:\n{second_stderr}"
    );

    // Clean up the first daemon.
    Command::new("kill")
        .args(["-TERM", &first.id().to_string()])
        .status()
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match first.try_wait().unwrap() {
            Some(_) => break,
            None => {
                if std::time::Instant::now() > deadline {
                    first.kill().unwrap();
                    panic!("first daemon did not exit within 10 seconds after SIGTERM");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

#[test]
fn cli_empty_config_exits_with_error() {
    let fx = CliFixture::new();
    let yaml = "encryption:\n  mode: none\n";
    std::fs::write(&fx.config_path, yaml).unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let (_stdout, stderr) = fx.run_err(&["--config", &cfg, "list"]);
    assert!(
        stderr.contains("no repositories configured"),
        "expected 'no repositories configured' error, got:\n{stderr}"
    );
}

// ── check --repair tests ────────────────────────────────────────────────────

#[test]
fn cli_check_repair_dry_run_prints_plan() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    corrupt_first_snapshot(&fx.repo_dir);

    let (out, err) = fx.run_err(&["--config", &cfg, "check", "--repair", "--dry-run"]);
    assert!(
        out.contains("Repair plan:"),
        "expected 'Repair plan:' in stdout, got:\n{out}"
    );
    assert!(
        out.contains("Remove corrupted snapshot blob"),
        "expected 'Remove corrupted snapshot blob' in stdout, got:\n{out}"
    );
    assert!(
        err.contains("Dry run: no changes applied"),
        "expected 'Dry run: no changes applied' in stderr, got:\n{err}"
    );
}

#[test]
fn cli_check_repair_yes_repairs_then_clean() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    corrupt_first_snapshot(&fx.repo_dir);

    let out = fx.run_ok(&["--config", &cfg, "check", "--repair", "--yes"]);
    assert!(
        out.contains("Repairs applied:"),
        "expected 'Repairs applied:' in stdout, got:\n{out}"
    );

    let check_out = fx.run_ok(&["--config", &cfg, "check"]);
    assert!(
        check_out.contains("0 errors"),
        "expected '0 errors' after repair, got:\n{check_out}"
    );
}

#[test]
fn cli_check_repair_noninteractive_aborts_tier2() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    corrupt_first_snapshot(&fx.repo_dir);

    // stdin is closed by Command::output() → read_line gets EOF → empty input
    // → input.trim() != "repair" → prints "Aborted." → exits 0
    let output = fx.run(&["--config", &cfg, "check", "--repair"]);
    assert!(
        output.status.success(),
        "abort should exit 0, got status: {}, stderr:\n{}",
        output.status,
        stderr(&output)
    );
    let err = stderr(&output);
    assert!(
        err.contains("Aborted"),
        "expected 'Aborted' in stderr, got:\n{err}"
    );
}

#[test]
fn cli_check_repair_positive_prompt_applies() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    corrupt_first_snapshot(&fx.repo_dir);

    let output = fx.run_with_stdin(&["--config", &cfg, "check", "--repair"], "repair\n");
    assert!(
        output.status.success(),
        "repair with positive prompt should succeed, got status: {}, stderr:\n{}",
        output.status,
        stderr(&output)
    );
    let out = stdout(&output);
    assert!(
        out.contains("Repairs applied:"),
        "expected 'Repairs applied:' in stdout, got:\n{out}"
    );
}

#[test]
fn cli_check_repair_safe_auto_apply() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    // No corruption — safe plan (refcount rebuild only)
    let output = fx.run(&["--config", &cfg, "check", "--repair"]);
    assert!(
        output.status.success(),
        "safe auto-apply should succeed, got status: {}, stderr:\n{}",
        output.status,
        stderr(&output)
    );
    let err = stderr(&output);
    assert!(
        err.contains("No data-loss actions; applying safe repairs"),
        "expected safe-auto-apply message in stderr, got:\n{err}"
    );
    let out = stdout(&output);
    assert!(
        out.contains("Repair plan:"),
        "expected 'Repair plan:' in stdout, got:\n{out}"
    );
    assert!(
        out.contains("Rebuild chunk refcounts"),
        "expected 'Rebuild chunk refcounts' in stdout, got:\n{out}"
    );
}

#[test]
fn cli_check_repair_yes_clean_repo() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    // No corruption — --yes should still work on a clean repo
    let out = fx.run_ok(&["--config", &cfg, "check", "--repair", "--yes"]);
    assert!(
        out.contains("Repair plan:"),
        "expected 'Repair plan:' in stdout, got:\n{out}"
    );
    assert!(
        out.contains("Rebuild chunk refcounts"),
        "expected 'Rebuild chunk refcounts' in stdout, got:\n{out}"
    );
}

// ── SIGHUP daemon reload tests ──────────────────────────────────────────────

/// Shared log collector: a background thread reads stderr and appends to a
/// shared buffer. The collector lives for the lifetime of the child process
/// (reads until EOF), so it never blocks the test and never consumes the
/// ChildStderr out from under wait_with_output.
#[cfg(unix)]
struct LogCollector {
    buf: std::sync::Arc<std::sync::Mutex<String>>,
    _handle: std::thread::JoinHandle<()>,
}

#[cfg(unix)]
impl LogCollector {
    fn spawn(mut stderr: std::process::ChildStderr) -> Self {
        use std::io::Read;

        let buf = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let buf_clone = buf.clone();
        let handle = std::thread::spawn(move || {
            let mut tmp = [0u8; 1024];
            loop {
                match stderr.read(&mut tmp) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let chunk = String::from_utf8_lossy(&tmp[..n]);
                        buf_clone.lock().unwrap().push_str(&chunk);
                    }
                }
            }
        });
        Self {
            buf,
            _handle: handle,
        }
    }

    /// Block until the collected stderr contains `marker` or `timeout` elapses.
    /// Returns true if the marker was found.
    fn wait_for(&self, marker: &str, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.snapshot().contains(marker) {
                return true;
            }
            if std::time::Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn snapshot(&self) -> String {
        self.buf.lock().unwrap().clone()
    }
}

/// Helper: spawn a daemon process with piped stderr/stdout.
#[cfg(unix)]
fn spawn_daemon(fx: &CliFixture, cfg: &str) -> (std::process::Child, LogCollector) {
    use std::process::Stdio;

    let mut child = Command::new(vykar_binary_path())
        .args(["--config", cfg, "daemon"])
        .env("HOME", &fx.home_dir)
        .env("XDG_CACHE_HOME", &fx.cache_dir)
        .env("XDG_CONFIG_HOME", &fx.config_home)
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let collector = LogCollector::spawn(child.stderr.take().unwrap());
    (child, collector)
}

/// Helper: wait for daemon to exit with a deadline, killing if needed.
#[cfg(unix)]
fn wait_for_exit(child: &mut std::process::Child, timeout_secs: u64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match child.try_wait().unwrap() {
            Some(_) => return,
            None => {
                if std::time::Instant::now() > deadline {
                    child.kill().unwrap();
                    panic!("daemon did not exit within {timeout_secs} seconds");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Helper: send a signal to a child process.
#[cfg(unix)]
fn send_signal(child: &std::process::Child, sig: &str) {
    Command::new("kill")
        .args([sig, &child.id().to_string()])
        .status()
        .unwrap();
}

#[test]
#[cfg(unix)]
fn cli_daemon_sighup_reloads_config() {
    let fx = CliFixture::new();

    // Start with empty sources and a 2-second interval so the second cycle
    // fires quickly after SIGHUP reload reschedules.
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: true\n  every: \"2s\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    // Wait for first backup cycle (empty sources)
    assert!(
        logs.wait_for("Summary", Duration::from_secs(15)),
        "first cycle should complete, got stderr:\n{}",
        logs.snapshot()
    );

    // Write a file and update config to include the source directory
    std::fs::write(fx.source_a.join("reload-test.txt"), b"reload data\n").unwrap();
    let new_config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources:\n  - {}\nschedule:\n  enabled: true\n  every: \"2s\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir),
        yaml_quote_path(&fx.source_a),
    );
    std::fs::write(&fx.config_path, new_config).unwrap();

    // Send SIGHUP to reload config
    send_signal(&child, "-HUP");

    // Wait for the reload + a second cycle that uses the new source.
    // "1 files" in the Summary proves the daemon picked up the new source.
    assert!(
        logs.wait_for("1 files", Duration::from_secs(15)),
        "second cycle should back up the new source, got stderr:\n{}",
        logs.snapshot()
    );

    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);

    let stderr_str = logs.snapshot();
    assert!(
        stderr_str.contains("configuration reloaded successfully"),
        "should see reload success log, got stderr:\n{stderr_str}"
    );
}

#[test]
#[cfg(unix)]
fn cli_daemon_sighup_invalid_config_keeps_old() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    assert!(
        logs.wait_for("Summary", Duration::from_secs(15)),
        "first cycle should complete"
    );

    // Overwrite config with invalid YAML
    std::fs::write(&fx.config_path, "{{{{invalid yaml!!!!").unwrap();

    send_signal(&child, "-HUP");

    assert!(
        logs.wait_for("configuration reload rejected", Duration::from_secs(5)),
        "should see rejection log, got stderr:\n{}",
        logs.snapshot()
    );

    // Daemon should still be alive
    assert!(
        child.try_wait().unwrap().is_none(),
        "daemon should still be running after invalid config reload"
    );

    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);

    assert!(
        child.wait().unwrap().success(),
        "daemon should exit cleanly after SIGTERM"
    );
}

#[test]
#[cfg(unix)]
fn cli_daemon_sighup_empty_repos_rejected() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    assert!(
        logs.wait_for("Summary", Duration::from_secs(15)),
        "first cycle should complete"
    );

    // Overwrite config with empty repositories
    std::fs::write(
        &fx.config_path,
        "encryption:\n  mode: none\nschedule:\n  enabled: true\n  every: \"1h\"\n",
    )
    .unwrap();

    send_signal(&child, "-HUP");

    assert!(
        logs.wait_for("no repositories configured", Duration::from_secs(5)),
        "should see 'no repositories configured' rejection, got stderr:\n{}",
        logs.snapshot()
    );

    assert!(
        child.try_wait().unwrap().is_none(),
        "daemon should still be running"
    );

    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);
}

#[test]
#[cfg(unix)]
fn cli_daemon_sighup_schedule_disabled_rejected() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    assert!(
        logs.wait_for("Summary", Duration::from_secs(15)),
        "first cycle should complete"
    );

    // Overwrite config with schedule.enabled: false
    let disabled_config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: false\n  every: \"1h\"\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, disabled_config).unwrap();

    send_signal(&child, "-HUP");

    assert!(
        logs.wait_for("schedule.enabled", Duration::from_secs(5)),
        "should see schedule.enabled rejection, got stderr:\n{}",
        logs.snapshot()
    );

    assert!(
        child.try_wait().unwrap().is_none(),
        "daemon should still be running"
    );

    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);
}

#[test]
#[cfg(unix)]
fn cli_daemon_on_startup_not_retriggered_on_reload() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    // Wait for the initial on_startup cycle
    assert!(
        logs.wait_for("Summary", Duration::from_secs(15)),
        "first cycle should complete"
    );

    // Send SIGHUP — should NOT trigger another immediate cycle
    send_signal(&child, "-HUP");

    // Wait for reload to process, then a few extra seconds
    assert!(
        logs.wait_for("reloaded", Duration::from_secs(5)),
        "reload should succeed"
    );
    std::thread::sleep(Duration::from_secs(3));

    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);

    // Count "Summary" occurrences — should be exactly 1 (the initial cycle).
    // With every: "1h", the next cycle is ~1h away, so no second cycle runs.
    let stderr_str = logs.snapshot();
    let summary_count = stderr_str.matches("Summary").count();
    assert_eq!(
        summary_count, 1,
        "on_startup should not retrigger on reload; expected 1 Summary, got {summary_count}.\nstderr:\n{stderr_str}"
    );
}

#[test]
#[cfg(unix)]
fn cli_daemon_sigusr1_triggers_immediate_backup() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: false\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    // Wait for the daemon to enter its main loop (signal handlers installed)
    assert!(
        logs.wait_for("next backup scheduled", Duration::from_secs(10)),
        "daemon should log next backup scheduled, got stderr:\n{}",
        logs.snapshot()
    );

    // Send SIGUSR1 to trigger an immediate backup
    send_signal(&child, "-USR1");

    assert!(
        logs.wait_for("SIGUSR1 received", Duration::from_secs(5)),
        "should see SIGUSR1 log, got stderr:\n{}",
        logs.snapshot()
    );

    assert!(
        logs.wait_for("Summary", Duration::from_secs(15)),
        "SIGUSR1 should trigger a backup cycle, got stderr:\n{}",
        logs.snapshot()
    );

    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);
}

#[test]
#[cfg(unix)]
fn cli_daemon_sigusr1_preserves_schedule() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    // Wait for the on_startup cycle to complete and next_run to be scheduled
    assert!(
        logs.wait_for("next backup scheduled", Duration::from_secs(15)),
        "daemon should schedule next backup after startup cycle, got stderr:\n{}",
        logs.snapshot()
    );

    // Snapshot the log position before SIGUSR1
    let pre_signal_log = logs.snapshot();
    let pre_signal_len = pre_signal_log.len();

    // Send SIGUSR1 to trigger an ad-hoc backup
    send_signal(&child, "-USR1");

    assert!(
        logs.wait_for("SIGUSR1 received", Duration::from_secs(5)),
        "should see SIGUSR1 log, got stderr:\n{}",
        logs.snapshot()
    );

    // Give it a moment to finish the cycle
    for _ in 0..50 {
        std::thread::sleep(Duration::from_millis(100));
        let current = logs.snapshot();
        if current[pre_signal_len..].contains("backup cycle finished") {
            break;
        }
    }

    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);

    // Check that "next backup scheduled" does NOT appear after the SIGUSR1 line —
    // this confirms next_run was not recalculated (the ~1h schedule is preserved).
    let final_log = logs.snapshot();
    let post_signal_final = &final_log[pre_signal_len..];
    let sigusr1_pos = post_signal_final.find("SIGUSR1 received").unwrap();
    let after_sigusr1 = &post_signal_final[sigusr1_pos..];

    assert!(
        !after_sigusr1.contains("next backup scheduled"),
        "next_run should NOT be recalculated when ad-hoc cycle finishes before scheduled slot.\nPost-SIGUSR1 log:\n{after_sigusr1}"
    );
}

#[test]
#[cfg(unix)]
fn cli_daemon_trust_repo_rejected() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    let output = Command::new(vykar_binary_path())
        .args(["--trust-repo", "--config", &cfg, "daemon"])
        .env("HOME", &fx.home_dir)
        .env("XDG_CACHE_HOME", &fx.cache_dir)
        .env("XDG_CONFIG_HOME", &fx.config_home)
        .env("NO_COLOR", "1")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr_str = stderr(&output);
    assert!(
        stderr_str.contains("--trust-repo"),
        "expected --trust-repo error, got:\n{stderr_str}"
    );
}

#[test]
fn cli_daemon_empty_config_startup_rejected() {
    let fx = CliFixture::new();
    let config = "encryption:\n  mode: none\nschedule:\n  enabled: true\n  every: \"1h\"\n";
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    let output = Command::new(vykar_binary_path())
        .args(["--config", &cfg, "daemon"])
        .env("HOME", &fx.home_dir)
        .env("XDG_CACHE_HOME", &fx.cache_dir)
        .env("XDG_CONFIG_HOME", &fx.config_home)
        .env("NO_COLOR", "1")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr_str = stderr(&output);
    assert!(
        stderr_str.contains("no repositories configured"),
        "expected 'no repositories configured' error, got:\n{stderr_str}"
    );
}
