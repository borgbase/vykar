use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

const MIB: u64 = 1024 * 1024;
const BACKUP_RSS_CAP_MIB: u64 = 512;
const RESTORE_RSS_CAP_MIB: u64 = 384;

struct CliFixture {
    _tmp: TempDir,
    home_dir: PathBuf,
    cache_dir: PathBuf,
    repo_dir: PathBuf,
    source_dir: PathBuf,
    config_path: PathBuf,
}

struct MonitoredOutput {
    output: Output,
    peak_rss_bytes: u64,
    saw_rss_sample: bool,
}

impl CliFixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let home_dir = tmp.path().join("home");
        let cache_dir = tmp.path().join("cache");
        let repo_dir = tmp.path().join("repo");
        let source_dir = tmp.path().join("source");
        let config_path = tmp.path().join("vykar.yaml");

        std::fs::create_dir_all(&home_dir).unwrap();
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&source_dir).unwrap();

        Self {
            _tmp: tmp,
            home_dir,
            cache_dir,
            repo_dir,
            source_dir,
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

    fn run_monitored(&self, args: &[&str]) -> MonitoredOutput {
        let mut cmd = Command::new(vykar_binary_path());
        cmd.args(args);
        cmd.env("HOME", &self.home_dir);
        cmd.env("XDG_CACHE_HOME", &self.cache_dir);
        cmd.env("NO_COLOR", "1");
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().unwrap();
        let pid = child.id();
        let mut peak_rss_bytes = 0u64;
        let mut saw_rss_sample = false;

        loop {
            if let Some(rss_bytes) = read_vm_rss_bytes(pid) {
                saw_rss_sample = true;
                peak_rss_bytes = peak_rss_bytes.max(rss_bytes);
            }

            match child.try_wait().unwrap() {
                Some(_) => break,
                None => thread::sleep(Duration::from_millis(20)),
            }
        }

        let output = child.wait_with_output().unwrap();

        MonitoredOutput {
            output,
            peak_rss_bytes,
            saw_rss_sample,
        }
    }

    fn run_monitored_ok(&self, args: &[&str]) -> (String, u64, bool) {
        let monitored = self.run_monitored(args);
        if !monitored.output.status.success() {
            panic!(
                "command failed: {:?}\nstdout:\n{}\nstderr:\n{}",
                args,
                stdout(&monitored.output),
                stderr(&monitored.output)
            );
        }
        (
            stdout(&monitored.output),
            monitored.peak_rss_bytes,
            monitored.saw_rss_sample,
        )
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
        "repositories:\n  - url: {}\nencryption:\n  mode: none\ncompression:\n  algorithm: none\nsources: []\n",
        yaml_quote_path(repo_dir)
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

fn read_vm_rss_bytes(pid: u32) -> Option<u64> {
    if !cfg!(target_os = "linux") {
        return None;
    }

    let status_path = PathBuf::from(format!("/proc/{pid}/status"));
    let status = std::fs::read_to_string(status_path).ok()?;

    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok())?;
            return Some(kb * 1024);
        }
    }

    None
}

fn write_deterministic_file(path: &Path, size_bytes: usize, seed: u8) {
    let mut f = File::create(path).unwrap();
    let mut buf = [0u8; 8192];
    let mut written = 0usize;
    let mut ctr = 0u64;

    while written < size_bytes {
        for b in &mut buf {
            *b = seed.wrapping_add((ctr as u8).wrapping_mul(31));
            ctr = ctr.wrapping_add(1);
        }
        let n = (size_bytes - written).min(buf.len());
        f.write_all(&buf[..n]).unwrap();
        written += n;
    }
}

fn mib_to_bytes(mib: u64) -> u64 {
    mib * MIB
}

fn bytes_to_mib(bytes: u64) -> f64 {
    (bytes as f64) / (MIB as f64)
}

fn count_regular_files(root: &Path) -> usize {
    let mut count = 0usize;
    let entries = std::fs::read_dir(root).unwrap();
    for entry in entries {
        let path = entry.unwrap().path();
        let meta = std::fs::symlink_metadata(&path).unwrap();
        if meta.is_file() {
            count += 1;
        } else if meta.is_dir() {
            count += count_regular_files(&path);
        }
    }
    count
}

fn skip_if_not_linux() -> bool {
    if cfg!(target_os = "linux") {
        return false;
    }
    eprintln!("skipping memory integration test: RSS sampler is Linux-only");
    true
}

fn assert_peak_under_cap(test_name: &str, peak_rss_bytes: u64, saw_rss_sample: bool, cap_mib: u64) {
    assert!(
        saw_rss_sample,
        "{test_name}: no RSS samples were captured while monitoring process"
    );

    let cap_bytes = mib_to_bytes(cap_mib);
    assert!(
        peak_rss_bytes <= cap_bytes,
        "{test_name}: peak RSS {:.1} MiB exceeded cap {} MiB",
        bytes_to_mib(peak_rss_bytes),
        cap_mib
    );
}

#[test]
fn memory_backup_large_file_peak_rss_under_cap() {
    if skip_if_not_linux() {
        return;
    }

    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);

    let file_path = fx.source_dir.join("large.bin");
    write_deterministic_file(&file_path, (256 * MIB) as usize, 17);

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_dir.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    let (_backup_out, peak_rss_bytes, saw_rss_sample) =
        fx.run_monitored_ok(&["--config", &cfg, "backup", &source]);

    assert_peak_under_cap(
        "memory_backup_large_file_peak_rss_under_cap",
        peak_rss_bytes,
        saw_rss_sample,
        BACKUP_RSS_CAP_MIB,
    );
}

#[test]
fn memory_backup_many_files_peak_rss_under_cap() {
    if skip_if_not_linux() {
        return;
    }

    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);

    for i in 0..64u8 {
        let file_path = fx.source_dir.join(format!("file-{i:02}.bin"));
        write_deterministic_file(&file_path, (4 * MIB) as usize, i.wrapping_mul(13));
    }

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_dir.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    let (_backup_out, peak_rss_bytes, saw_rss_sample) =
        fx.run_monitored_ok(&["--config", &cfg, "backup", &source]);

    assert_peak_under_cap(
        "memory_backup_many_files_peak_rss_under_cap",
        peak_rss_bytes,
        saw_rss_sample,
        BACKUP_RSS_CAP_MIB,
    );
}

#[test]
fn memory_restore_peak_rss_under_cap() {
    if skip_if_not_linux() {
        return;
    }

    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);

    for i in 0..64u8 {
        let file_path = fx.source_dir.join(format!("file-{i:02}.bin"));
        write_deterministic_file(&file_path, (4 * MIB) as usize, i.wrapping_mul(7));
    }

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_dir.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    let backup_out = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snapshot = parse_snapshot_name(&backup_out);

    let restore = fx._tmp.path().join("restore");
    let restore_str = restore.to_string_lossy().to_string();
    let (_restore_out, peak_rss_bytes, saw_rss_sample) =
        fx.run_monitored_ok(&["--config", &cfg, "restore", &snapshot, &restore_str]);

    assert_peak_under_cap(
        "memory_restore_peak_rss_under_cap",
        peak_rss_bytes,
        saw_rss_sample,
        RESTORE_RSS_CAP_MIB,
    );

    assert_eq!(count_regular_files(&restore), 64);
}
