use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use tempfile::TempDir;
use vykar_core::repo::Repository;
use vykar_storage::local_backend::LocalBackend;

struct CliFixture {
    _tmp: TempDir,
    home_dir: PathBuf,
    cache_dir: PathBuf,
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
        let repo_dir = tmp.path().join("repo");
        let source_a = tmp.path().join("source-a");
        let source_b = tmp.path().join("source-b");
        let config_path = tmp.path().join("vykar.yaml");

        std::fs::create_dir_all(&home_dir).unwrap();
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&source_a).unwrap();
        std::fs::create_dir_all(&source_b).unwrap();

        Self {
            _tmp: tmp,
            home_dir,
            cache_dir,
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
    let repo = Repository::open(storage, None, None).unwrap();
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
