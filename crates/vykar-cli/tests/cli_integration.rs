#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
#![allow(clippy::panic, clippy::indexing_slicing)]

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use tempfile::TempDir;
use vykar_core::repo::{OpenOptions, Repo};
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

/// Point this test process's own per-user directories at a per-process temp
/// directory: some tests open repositories in-process (`Repo::open`,
/// `Repo::init`), which read the installation ID from the config
/// directory and write identity pins into the cache directory. The child
/// processes get the fixture's own dirs through `CliFixture::test_env`.
#[allow(unsafe_code)]
fn isolate_process_env() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let base = std::env::temp_dir().join(format!("vykar-cli-tests-{}", std::process::id()));
        let (home, config, cache) = (base.join("home"), base.join("config"), base.join("cache"));
        for dir in [&home, &config, &cache] {
            std::fs::create_dir_all(dir).unwrap();
        }
        // SAFETY: Rust 2024 marks env mutation as unsafe due to process-global
        // races. This runs once, from the first fixture, before any test of
        // this process opens a repository in-process.
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("USERPROFILE", &home);
            std::env::set_var("XDG_CONFIG_HOME", &config);
            std::env::set_var("APPDATA", &config);
            std::env::set_var("XDG_CACHE_HOME", &cache);
            std::env::set_var("LOCALAPPDATA", &cache);
        }
    });
}

impl CliFixture {
    fn new() -> Self {
        isolate_process_env();
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

    /// Environment that keeps a run inside this fixture's temp dirs. `HOME`
    /// and the XDG vars cover Linux and macOS; Windows resolves the same
    /// lookups through `USERPROFILE`/`LOCALAPPDATA`/`APPDATA` (see
    /// `vykar_common::paths`), so both sets are always set — without them a
    /// Windows run reads and writes the developer's real profile.
    fn test_env(&self) -> [(&str, &OsStr); 7] {
        [
            ("HOME", self.home_dir.as_os_str()),
            ("USERPROFILE", self.home_dir.as_os_str()),
            ("XDG_CACHE_HOME", self.cache_dir.as_os_str()),
            ("LOCALAPPDATA", self.cache_dir.as_os_str()),
            ("XDG_CONFIG_HOME", self.config_home.as_os_str()),
            ("APPDATA", self.config_home.as_os_str()),
            ("NO_COLOR", OsStr::new("1")),
        ]
    }

    /// Create every repository `cfg` names, as `vykar init` would, through
    /// the library with the cheap test KDF; their identity pins and caches go
    /// where this fixture's vykar children look for them.
    fn init_repos(&self, cfg: &str) {
        self.init_where(cfg, |_| true);
    }

    /// [`Self::init_repos`] for the repository labelled `label` only
    /// (`vykar init -R <label>`).
    fn init_repo_labelled(&self, cfg: &str, label: &str) {
        self.init_where(cfg, |r| r.label.as_deref() == Some(label));
    }

    fn init_where(&self, cfg: &str, pick: impl Fn(&vykar_core::config::ResolvedRepo) -> bool) {
        let repos = vykar_core::config::load_and_resolve(Path::new(cfg)).unwrap();
        let mut initialized = 0;
        for repo in repos.into_iter().filter(|r| pick(r)) {
            let mut config = repo.config;
            if config.cache_dir.is_none() {
                config.cache_dir = Some(self.child_cache_root().to_string_lossy().into_owned());
            }
            // A configured passcommand runs here as it would in the child.
            // Only a config that names no passphrase source falls back to the
            // test passphrase; a named source that yields nothing is a broken
            // fixture.
            let named =
                config.encryption.passphrase.is_some() || config.encryption.passcommand.is_some();
            let resolved = vykar_core::app::passphrase::resolve_passphrase(
                &config,
                repo.label.as_deref(),
                |_| Ok(None),
            )
            .unwrap();
            let pass = match resolved {
                Some(p) => p.to_string(),
                None if named => panic!(
                    "{cfg}: repository {:?} names a passphrase source that resolves to nothing",
                    repo.label
                ),
                None => TEST_PASS.to_string(),
            };
            vykar_core::testing::with_cheap_kdf(|| {
                vykar_core::commands::init::run(&config, Some(&pass))
            })
            .unwrap();
            initialized += 1;
        }
        assert!(initialized > 0, "{cfg} names no repository to initialize");
    }

    /// Flip the middle byte of the stored blob of the chunk of the file
    /// `name` in the latest snapshot of the single repository `cfg` names.
    fn flip_byte_in_chunk_of(&self, cfg: &str, name: &str) {
        let repo = vykar_core::config::load_and_resolve(Path::new(cfg))
            .unwrap()
            .remove(0);
        let mut config = repo.config;
        config.cache_dir = Some(self.child_cache_root().to_string_lossy().into_owned());
        let snapshot = vykar_core::commands::list::list_snapshots(&config, Some(TEST_PASS))
            .unwrap()
            .last()
            .unwrap()
            .name
            .clone();
        let items =
            vykar_core::commands::list::list_snapshot_items(&config, Some(TEST_PASS), &snapshot)
                .unwrap();
        let item = items.iter().find(|i| i.path.ends_with(name)).unwrap();
        let storage = Box::new(LocalBackend::new(&config.repository.url).unwrap());
        let opened = Repo::open(
            storage,
            Some(TEST_PASS),
            None,
            OpenOptions::new().with_index(),
        )
        .unwrap();
        let entry = opened.chunk_index().get(&item.chunks[0].id).unwrap();
        let path = Path::new(&config.repository.url).join(entry.pack_id.storage_key());
        let mut pack = std::fs::read(&path).unwrap();
        pack[entry.pack_offset as usize + entry.stored_size as usize / 2] ^= 0x01;
        std::fs::write(&path, pack).unwrap();
    }

    /// The vykar cache root of this fixture's children: `paths::cache_dir()`
    /// (`XDG_CACHE_HOME` and `LOCALAPPDATA` are `cache_dir`; macOS uses
    /// `$HOME/Library/Caches`) joined with `vykar`.
    fn child_cache_root(&self) -> PathBuf {
        if cfg!(target_os = "macos") {
            self.home_dir.join("Library").join("Caches").join("vykar")
        } else {
            self.cache_dir.join("vykar")
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut cmd = Command::new(vykar_binary_path());
        cmd.args(args);
        cmd.envs(self.test_env());
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

    /// Like [`Self::run`], but with `VYKAR_PASSPHRASE` set for encrypted
    /// repositories.
    fn run_encrypted(&self, args: &[&str], passphrase: &str) -> Output {
        let mut cmd = Command::new(vykar_binary_path());
        cmd.args(args);
        cmd.envs(self.test_env());
        cmd.env("VYKAR_PASSPHRASE", passphrase);
        cmd.output().unwrap()
    }

    fn run_encrypted_ok(&self, args: &[&str], passphrase: &str) -> Output {
        let output = self.run_encrypted(args, passphrase);
        if !output.status.success() {
            panic!(
                "command failed: {:?}\nstdout:\n{}\nstderr:\n{}",
                args,
                stdout(&output),
                stderr(&output)
            );
        }
        output
    }

    fn run_with_stdin(&self, args: &[&str], stdin_data: &str) -> Output {
        use std::io::Write;
        use std::process::Stdio;

        let mut child = Command::new(vykar_binary_path());
        child
            .args(args)
            .envs(self.test_env())
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

/// The passphrase of every repository these tests create through the library.
const TEST_PASS: &str = vykar_core::testing::TEST_PASSPHRASE;

/// The `encryption:` section of a test config: AES-256-GCM with
/// [`TEST_PASS`].
fn enc_yaml() -> String {
    format!("encryption:\n  mode: aes256gcm\n  passphrase: \"{TEST_PASS}\"\n")
}

fn write_test_config(config_path: &Path, repo_dir: &Path) {
    let config = format!(
        "repositories:\n  - url: {}\n{}sources: []\n",
        yaml_quote_path(repo_dir),
        enc_yaml()
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
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nretention:\n  keep_last: {}\nsources:\n  - path: {}\n    label: src-a\n  - path: {}\n    label: src-b\n",
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
    let repo = Repo::open(
        storage,
        Some(TEST_PASS),
        None,
        OpenOptions::new().with_index(),
    )
    .unwrap();
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
    let repo = Repo::open(
        storage,
        Some(TEST_PASS),
        None,
        OpenOptions::new().with_index(),
    )
    .unwrap();
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
    write_test_config(&fx.config_path, &fx.repo_dir);
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
    // Users hold a mix of repository formats; `info` has to say which.
    let has_row = |label: &str, value: &str| {
        info_out
            .lines()
            .any(|l| l.contains(label) && l.contains(value))
    };
    assert!(
        has_row("Format", "v6"),
        "expected a Format row reading v6, got:\n{info_out}"
    );
    assert!(
        has_row("Hash", "blake3"),
        "expected a Hash row reading blake3, got:\n{info_out}"
    );
}

/// `vykar init` must write format v6 to the repository `config`, read straight
/// off disk rather than through the library that produced it.
#[test]
fn cli_init_writes_repository_format_v6() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);

    let config_bytes = std::fs::read(fx.repo_dir.join("config")).expect("repo config exists");
    // From v6 on `config` is a msgpack map (0x89 = fixmap of 9: the seven
    // old fields, chunk_hash and features).
    assert_eq!(
        config_bytes.first(),
        Some(&0x89),
        "RepoConfig must be a nine-entry msgpack map"
    );
    let config = vykar_core::repo::RepoConfig::decode(&config_bytes)
        .unwrap()
        .into_repo_config();
    assert_eq!(config.version, 6, "init must write version 6");
    assert_eq!(config.chunk_hash, "blake3");
    // A local filesystem passes the put-if-absent check: multi-writer.
    assert_eq!(config.features, ["multi_writer"]);
    assert!(fx.repo_dir.join("index.h").join("1").is_file());
    assert!(!fx.repo_dir.join("index").exists());
}

/// The empty v3 repository 0.24 creates, at `repo_dir`: no binary writes
/// v2/v3 any more, so a v6 one is rewritten into it. `index` becomes an empty
/// `IndexBlob` (zstd msgpack, packed as `ChunkIndex` with context `index`),
/// `index.gen` 0, `index.d/` and `index.h/` go and `config` says version 3.
fn init_empty_v3(repo_dir: &Path) {
    use vykar_core::repo::format::{ObjectType, pack_object_streaming_with_context};

    std::fs::create_dir_all(repo_dir).unwrap();
    let storage = || Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    vykar_core::testing::with_cheap_kdf(|| {
        Repo::init(
            storage(),
            vykar_core::repo::EncryptionMode::Aes256Gcm,
            vykar_core::repo::ChunkerParams::default(),
            Some(TEST_PASS),
            None,
            None,
        )
    })
    .unwrap();
    let cache = TempDir::new().unwrap();
    let repo = Repo::open(
        storage(),
        Some(TEST_PASS),
        Some(cache.path().to_path_buf()),
        OpenOptions::new(),
    )
    .unwrap();
    let blob = vykar_core::index::IndexBlob {
        generation: 0,
        chunks: vykar_core::index::LegacyChunkIndex::new(),
    };
    let index = pack_object_streaming_with_context(
        ObjectType::ChunkIndex,
        b"index",
        64,
        repo.crypto().as_ref(),
        |buf| {
            vykar_core::compress::compress_stream_zstd(buf, 3, |encoder| {
                rmp_serde::encode::write(encoder, &blob)?;
                Ok(())
            })
        },
    )
    .unwrap();
    drop(repo);

    std::fs::write(repo_dir.join("index"), index).unwrap();
    std::fs::write(repo_dir.join("index.gen"), 0u64.to_le_bytes()).unwrap();
    std::fs::remove_dir_all(repo_dir.join("index.d")).unwrap();
    std::fs::remove_dir_all(repo_dir.join("index.h")).unwrap();
    let path = repo_dir.join("config");
    let config = vykar_core::repo::RepoConfig::decode(&std::fs::read(&path).unwrap())
        .unwrap()
        .into_repo_config();
    let legacy = vykar_core::repo::LegacyRepoConfig {
        version: 3,
        id: config.id,
        chunker_params: config.chunker_params,
        encryption: config.encryption,
        created: config.created,
        min_pack_size: config.min_pack_size,
        max_pack_size: config.max_pack_size,
    };
    std::fs::write(path, rmp_serde::to_vec(&legacy).unwrap()).unwrap();
}

/// `vykar upgrade` takes a v3 repository to v6. It refuses to run without a
/// terminal unless `--yes` is given, and a second run does nothing.
#[test]
fn cli_upgrade_moves_a_v3_repository_to_v6() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();
    init_empty_v3(&fx.repo_dir);

    let (_, stderr) = fx.run_err(&["--config", &cfg, "list"]);
    assert!(stderr.contains("vykar upgrade"), "{stderr}");

    let (_, stderr) = fx.run_err(&["--config", &cfg, "upgrade"]);
    assert!(stderr.contains("--yes"), "{stderr}");
    assert_eq!(
        std::fs::read(fx.repo_dir.join("config")).unwrap().get(1),
        Some(&3)
    );

    let out = fx.run_ok(&["--config", &cfg, "upgrade", "--yes"]);
    assert!(out.contains("from format v3 to v6 (blake3)"), "{out}");
    assert!(out.contains("packs mapped"), "{out}");
    let config =
        vykar_core::repo::RepoConfig::decode(&std::fs::read(fx.repo_dir.join("config")).unwrap())
            .unwrap()
            .into_repo_config();
    assert_eq!((config.version, config.chunk_hash.as_str()), (6, "blake3"));
    assert!(!fx.repo_dir.join("index").exists());
    assert!(!fx.repo_dir.join("index.gen").exists());

    let out = fx.run_ok(&["--config", &cfg, "upgrade", "--yes"]);
    assert!(out.contains("already at format v6"), "{out}");
}

/// `vykar info` shows the commit mode; `vykar upgrade --commit-mode`
/// switches it either way, and a plain upgrade then changes nothing.
#[test]
fn cli_upgrade_switches_the_commit_mode() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();
    let init_out = fx.run_ok(&["--config", &cfg, "init"]);
    assert!(!init_out.contains("Commit mode"), "{init_out}");
    let mode_row = |want: &str| {
        let info = fx.run_ok(&["--config", &cfg, "info"]);
        assert!(
            info.lines()
                .any(|l| l.contains("Commit mode") && l.contains(want)),
            "expected a Commit mode row reading {want}, got:\n{info}"
        );
        info
    };
    let info = mode_row("multi-writer");
    assert!(!info.contains("--commit-mode multi"), "{info}");

    let out = fx.run_ok(&[
        "--config",
        &cfg,
        "upgrade",
        "--yes",
        "--commit-mode",
        "single",
    ]);
    assert!(out.contains("single-writer"), "{out}");
    let info = mode_row("single-writer");
    // The storage offers put-if-absent: info says how to switch back.
    assert!(info.contains("--commit-mode multi"), "{info}");
    let out = fx.run_ok(&["--config", &cfg, "upgrade", "--yes"]);
    assert!(out.contains("already at format v6"), "{out}");
    assert!(out.contains("--commit-mode multi"), "{out}");

    let out = fx.run_ok(&[
        "--config",
        &cfg,
        "upgrade",
        "--yes",
        "--commit-mode",
        "multi",
    ]);
    assert!(out.contains("multi-writer"), "{out}");
    mode_row("multi-writer");
    let out = fx.run_ok(&["--config", &cfg, "upgrade", "--yes"]);
    assert!(out.contains("already at format v6"), "{out}");
    assert!(!out.contains("--commit-mode multi"), "{out}");
}

#[test]
fn cli_delete_dry_run_then_real_delete() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("delete.txt"), b"delete me\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
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
    fx.init_repos(&cfg);

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

/// `write_sources_config` with a global `hooks:` block of `(key, command)`.
#[cfg(unix)]
fn write_hooked_sources_config(fx: &CliFixture, hooks: &[(&str, String)]) {
    write_sources_config(&fx.config_path, &fx.repo_dir, &fx.source_a, &fx.source_b, 1);
    let mut config = std::fs::read_to_string(&fx.config_path).unwrap();
    config.push_str("hooks:\n");
    for (key, cmd) in hooks {
        let quoted = format!("\"{}\"", cmd.replace('\\', "\\\\").replace('"', "\\\""));
        config.push_str(&format!("  {key}: {quoted}\n"));
    }
    std::fs::write(&fx.config_path, config).unwrap();
}

#[test]
#[cfg(unix)]
fn cli_prune_compact_runs_compact_hooks() {
    let fx = CliFixture::new();
    write_sources_config(&fx.config_path, &fx.repo_dir, &fx.source_a, &fx.source_b, 1);
    std::fs::write(fx.source_a.join("a.txt"), b"a-v1").unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.init_repos(&cfg);
    fx.run_ok(&["--config", &cfg, "backup", "--source", "src-a"]);
    std::fs::write(fx.source_a.join("a.txt"), b"a-v2").unwrap();
    fx.run_ok(&["--config", &cfg, "backup", "--source", "src-a"]);

    // Hooks are added only now, so the setup backups do not log.
    let log = fx.home_dir.join("hooks.log");
    let append = |word: &str| format!("echo {word} >> '{}'", log.display());
    write_hooked_sources_config(
        &fx,
        &[
            ("before", append("before:$VYKAR_COMMAND")),
            ("after", append("after:$VYKAR_COMMAND")),
            ("finally", append("finally:$VYKAR_COMMAND")),
            ("before_compact", append("before_compact")),
        ],
    );

    let out = fx.run_ok(&["--config", &cfg, "prune", "--compact"]);
    assert!(out.contains("Pruned 1 snapshots"), "{out}");

    let lines: Vec<String> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(
        lines,
        [
            "before:prune",
            "after:prune",
            "finally:prune",
            "before:compact",
            "before_compact",
            "after:compact",
            "finally:compact",
        ]
    );
}

#[test]
#[cfg(unix)]
fn cli_prune_compact_skipped_prune_does_not_compact() {
    let fx = CliFixture::new();
    let marker = fx.home_dir.join("compacted");
    write_hooked_sources_config(
        &fx,
        &[
            ("before_prune", "exit 75".to_string()),
            ("before_compact", format!("touch '{}'", marker.display())),
        ],
    );
    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.init_repos(&cfg);

    let output = fx.run(&["--config", &cfg, "prune", "--compact"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(stderr(&output).contains("Skipped:"), "{}", stderr(&output));
    assert!(!marker.exists());
}

#[test]
#[cfg(unix)]
fn cli_prune_compact_failed_prune_does_not_compact() {
    let fx = CliFixture::new();
    let marker = fx.home_dir.join("compacted");
    write_hooked_sources_config(
        &fx,
        &[
            ("before_prune", "exit 3".to_string()),
            ("before_compact", format!("touch '{}'", marker.display())),
        ],
    );
    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.init_repos(&cfg);

    let output = fx.run(&["--config", &cfg, "prune", "--compact"]);
    assert!(!output.status.success(), "{}", stderr(&output));
    assert!(!marker.exists());
}

#[test]
fn cli_snapshot_find_timeline_and_filters() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    // Create initial files
    std::fs::create_dir_all(fx.source_a.join("sub")).unwrap();
    std::fs::write(fx.source_a.join("hello.txt"), b"hello v1").unwrap();
    std::fs::write(fx.source_a.join("sub/deep.rs"), b"fn main() {}").unwrap();
    std::fs::write(fx.source_a.join("big.bin"), vec![0u8; 2048]).unwrap();

    fx.init_repos(&cfg);

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
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("repo-del.txt"), b"delete repo\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    assert!(fx.repo_dir.join("config").exists());

    let delete_out = fx.run_ok(&["--config", &cfg, "delete", "--yes-delete-this-repo"]);
    assert!(delete_out.contains("deleted"));
    assert!(!fx.repo_dir.exists());
}

/// `break-lock` (10-single-protocol step 9): on a fresh repository it
/// finds nothing; with leftover lock files and session markers it removes
/// everything under `locks/` without asking, lists every marker on stderr
/// (the exclusive one labelled), and without a terminal refuses to remove
/// them, naming the lock files it removed; `--sessions` is accepted and
/// does the same.
#[test]
fn cli_break_lock_lists_sessions_and_refuses_without_a_terminal() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.init_repos(&cfg);

    let quiet = fx.run(&["--config", &cfg, "break-lock"]);
    assert!(quiet.status.success(), "{}", stderr(&quiet));
    assert!(
        stdout(&quiet).contains("No active sessions found."),
        "{}",
        stdout(&quiet)
    );

    let locks = fx.repo_dir.join("locks");
    std::fs::create_dir_all(&locks).unwrap();
    std::fs::write(locks.join("old.json"), br#"{"hostname":"h","pid":1}"#).unwrap();
    std::fs::write(locks.join("vykar-clock-x"), b"vykar clock probe").unwrap();
    let sessions = fx.repo_dir.join("sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let marker = |host: &str, pid: u32| {
        format!(
            r#"{{"hostname":"{host}","pid":{pid},"registered_at":"{now}","last_refresh":"{now}","elapsed_secs":0}}"#
        )
    };
    let exclusive = "x-0123456789abcdef0123456789abcdef";
    let shared = "fedcba9876543210fedcba9876543210";
    let exclusive_key = sessions.join(format!("{exclusive}.json"));
    let shared_key = sessions.join(format!("{shared}.json"));
    std::fs::write(&exclusive_key, marker("repair-host", 4242)).unwrap();
    std::fs::write(&shared_key, marker("backup-host", 77)).unwrap();

    let output = fx.run(&["--config", &cfg, "break-lock"]);
    assert!(!output.status.success(), "{}", stdout(&output));
    assert!(
        stdout(&output).contains("Removed 1 leftover lock file(s)"),
        "{}",
        stdout(&output)
    );
    assert_eq!(
        std::fs::read_dir(&locks).unwrap().count(),
        0,
        "locks/ emptied"
    );
    let err = stderr(&output);
    let exclusive_line = err
        .lines()
        .find(|l| l.contains(&format!("{exclusive}: host=repair-host, pid=4242")))
        .unwrap_or_else(|| panic!("no line for the exclusive session: {err}"));
    assert!(
        exclusive_line.ends_with("(exclusive: check --repair, upgrade or a pack-map write)"),
        "{exclusive_line}"
    );
    assert!(
        err.contains(&format!("{shared}: host=backup-host, pid=77")),
        "{err}"
    );
    assert!(
        err.contains("refusing to remove sessions without confirmation"),
        "{err}"
    );
    assert!(
        err.contains("1 leftover lock file(s) were removed"),
        "{err}"
    );
    assert!(exclusive_key.exists() && shared_key.exists());

    let flagged = fx.run(&["--config", &cfg, "break-lock", "--sessions"]);
    assert!(!flagged.status.success(), "{}", stdout(&flagged));
    let err = stderr(&flagged);
    assert!(
        err.contains("refusing to remove sessions without confirmation"),
        "{err}"
    );
    assert!(
        err.contains("0 leftover lock file(s) were removed"),
        "{err}"
    );
    assert!(exclusive_key.exists() && shared_key.exists());
}

#[test]
fn cli_check_verify_data_detects_tampered_pack() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("check.txt"), b"check data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    let check_ok = fx.run_ok(&["--config", &cfg, "check", "--verify-data"]);
    assert!(check_ok.contains("0 errors"));

    delete_pack_for_first_chunk(&fx.repo_dir);
    let (stdout_err, _stderr_err) = fx.run_err(&["--config", &cfg, "check", "--verify-data"]);
    assert!(stdout_err.contains("Errors found:"));
    assert!(stdout_err.contains("missing from storage"));
}

/// Strict restore of a damaged repo exits 1 with a hint; `--continue-on-error`
/// restores the intact file, lists the damaged one and exits 3.
#[test]
fn cli_restore_continue_on_error_skips_damaged_file() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\ncompression:\n  algorithm: none\nsources: []\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let content = b"continue-on-error-corruption-marker-0123456789abcdef\n";
    std::fs::write(fx.source_a.join("bad.txt"), content).unwrap();
    std::fs::write(fx.source_a.join("good.txt"), b"intact content\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();
    fx.init_repos(&cfg);
    fx.run_ok(&["--config", &cfg, "backup", &source]);
    // The stored blob of bad.txt's chunk, flipped inside its ciphertext.
    fx.flip_byte_in_chunk_of(&cfg, "bad.txt");

    let strict = fx._tmp.path().join("restore-strict");
    let strict_str = strict.to_string_lossy().to_string();
    let out = fx.run(&["--config", &cfg, "restore", "latest", &strict_str]);
    let err = stderr(&out);
    assert_eq!(out.status.code(), Some(1), "stderr:\n{err}");
    assert!(err.contains("no restored files were published"), "{err}");
    assert!(err.contains("hint: --continue-on-error"), "{err}");

    let lenient = fx._tmp.path().join("restore-continue");
    let lenient_str = lenient.to_string_lossy().to_string();
    let out = fx.run(&[
        "--config",
        &cfg,
        "restore",
        "latest",
        &lenient_str,
        "--continue-on-error",
    ]);
    let err = stderr(&out);
    assert_eq!(out.status.code(), Some(3), "stderr:\n{err}");
    let damaged: Vec<&str> = err.lines().filter(|l| l.starts_with("damaged: ")).collect();
    assert_eq!(damaged.len(), 1, "{err}");
    assert!(damaged[0].contains("bad.txt") && damaged[0].contains("decryption failed"));
    assert!(
        !lenient.join("bad.txt").exists(),
        "damaged file must be absent"
    );
    assert_eq!(
        std::fs::read(lenient.join("good.txt")).unwrap(),
        b"intact content\n"
    );
}

#[test]
fn cli_restore_latest_alias() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("latest.txt"), b"latest test\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();
    let restore = fx._tmp.path().join("restore-latest");
    let restore_str = restore.to_string_lossy().to_string();

    fx.init_repos(&cfg);
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
    write_test_config(&fx.config_path, &fx.repo_dir);
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
    write_test_config(&fx.config_path, &fx.repo_dir);

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);

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
fn cli_snapshot_list_path_filter() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    std::fs::create_dir_all(fx.source_a.join("sub/b")).unwrap();
    std::fs::create_dir_all(fx.source_a.join("subx")).unwrap();
    std::fs::write(fx.source_a.join("sub/a"), b"a\n").unwrap();
    std::fs::write(fx.source_a.join("sub/b/c"), b"c\n").unwrap();
    std::fs::write(fx.source_a.join("subx/d"), b"d\n").unwrap();
    std::fs::write(fx.source_a.join("top"), b"top\n").unwrap();

    fx.init_repos(&cfg);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    for filter in ["sub", "./sub/"] {
        let out = fx.run_ok(&[
            "--config", &cfg, "snapshot", "list", "latest", "--path", filter,
        ]);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(
            lines,
            vec!["sub", "sub/a", "sub/b", "sub/b/c"],
            "--path {filter}"
        );
    }

    let out = fx.run_ok(&[
        "--config", &cfg, "snapshot", "list", "latest", "--path", "nosuch",
    ]);
    assert_eq!(out, "", "--path nosuch");

    // --long under --path: the --long lines of those paths.
    let all = fx.run_ok(&["--config", &cfg, "snapshot", "list", "latest", "--long"]);
    let want: Vec<&str> = all
        .lines()
        .filter(|l| {
            let path = l.rsplit(' ').next().unwrap();
            path == "sub" || path.starts_with("sub/")
        })
        .collect();
    let out = fx.run_ok(&[
        "--config", &cfg, "snapshot", "list", "latest", "--path", "sub", "--long",
    ]);
    assert_eq!(out.lines().collect::<Vec<_>>(), want);
    assert_eq!(want.len(), 4);
}

#[test]
fn cli_snapshot_info_latest_alias() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("info.txt"), b"info\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);

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
fn cli_snapshot_diff_reports_file_content_changes() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);

    std::fs::write(fx.source_a.join("modified.txt"), b"before").unwrap();
    std::fs::write(fx.source_a.join("removed.txt"), b"removed").unwrap();
    std::fs::write(fx.source_a.join("unchanged.txt"), b"same").unwrap();
    let out1 = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snap1 = parse_snapshot_name(&out1);
    std::thread::sleep(Duration::from_millis(10));

    std::fs::write(fx.source_a.join("modified.txt"), b"after-content").unwrap();
    std::fs::remove_file(fx.source_a.join("removed.txt")).unwrap();
    std::fs::write(fx.source_a.join("added.txt"), b"added").unwrap();
    let out2 = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snap2 = parse_snapshot_name(&out2);

    let diff = fx.run_ok(&["--config", &cfg, "snapshot", "diff", &snap1, &snap2]);
    assert!(diff.contains("Added"));
    assert!(diff.contains("added.txt"));
    assert!(diff.contains("Removed"));
    assert!(diff.contains("removed.txt"));
    assert!(diff.contains("Modified"));
    assert!(diff.contains("modified.txt"));
    assert!(!diff.contains("unchanged.txt"));

    let reversed = fx.run_ok(&["--config", &cfg, "snapshot", "diff", &snap2, &snap1]);
    assert!(reversed.contains("Added"));
    assert!(reversed.contains("added.txt"));
    assert!(reversed.contains("Removed"));
    assert!(reversed.contains("removed.txt"));
}

#[test]
fn cli_snapshot_diff_resolves_in_multi_repo_config() {
    let fx = CliFixture::new();
    let repo_b = fx._tmp.path().join("repo_b");
    std::fs::create_dir_all(&repo_b).unwrap();

    let config = format!(
        "repositories:\n  - url: {}\n    label: repo-a\n  - url: {}\n    label: repo-b\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\n",
        yaml_quote_path(&fx.repo_dir),
        yaml_quote_path(&repo_b),
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);

    // Two snapshots in repo-b only.
    std::fs::write(fx.source_a.join("modified.txt"), b"before").unwrap();
    let out1 = fx.run_ok(&["--config", &cfg, "backup", "-R", "repo-b", &source]);
    let snap1 = parse_snapshot_name(&out1);
    std::thread::sleep(Duration::from_millis(10));
    std::fs::write(fx.source_a.join("modified.txt"), b"after").unwrap();
    std::fs::write(fx.source_a.join("added.txt"), b"added").unwrap();
    let out2 = fx.run_ok(&["--config", &cfg, "backup", "-R", "repo-b", &source]);
    let snap2 = parse_snapshot_name(&out2);

    // No -R; auto-probe should find both in repo-b.
    let diff = fx.run_ok(&["--config", &cfg, "snapshot", "diff", &snap1, &snap2]);
    assert!(
        diff.contains("Modified") && diff.contains("modified.txt"),
        "expected modified row, got:\n{diff}"
    );
    assert!(
        diff.contains("Added") && diff.contains("added.txt"),
        "expected added row, got:\n{diff}"
    );
}

#[test]
fn cli_snapshot_diff_errors_when_snapshots_in_different_repos() {
    let fx = CliFixture::new();
    let repo_b = fx._tmp.path().join("repo_b");
    std::fs::create_dir_all(&repo_b).unwrap();

    let config = format!(
        "repositories:\n  - url: {}\n    label: repo-a\n  - url: {}\n    label: repo-b\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\n",
        yaml_quote_path(&fx.repo_dir),
        yaml_quote_path(&repo_b),
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);

    std::fs::write(fx.source_a.join("a.txt"), b"a").unwrap();
    let out_a = fx.run_ok(&["--config", &cfg, "backup", "-R", "repo-a", &source]);
    let snap_a = parse_snapshot_name(&out_a);

    std::fs::write(fx.source_a.join("b.txt"), b"b").unwrap();
    let out_b = fx.run_ok(&["--config", &cfg, "backup", "-R", "repo-b", &source]);
    let snap_b = parse_snapshot_name(&out_b);

    let (_stdout, stderr) = fx.run_err(&["--config", &cfg, "snapshot", "diff", &snap_a, &snap_b]);
    assert!(
        stderr.contains("repo-a") && stderr.contains("repo-b"),
        "expected error naming both repos, got:\n{stderr}"
    );
    assert!(
        stderr.contains("same repository"),
        "expected same-repository error, got:\n{stderr}"
    );
}

#[test]
fn cli_snapshot_diff_empty_diff_reports_no_changes() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);

    std::fs::write(fx.source_a.join("only.txt"), b"unchanged").unwrap();
    let out1 = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snap1 = parse_snapshot_name(&out1);
    std::thread::sleep(Duration::from_millis(10));
    let out2 = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snap2 = parse_snapshot_name(&out2);

    let diff = fx.run_ok(&["--config", &cfg, "snapshot", "diff", &snap1, &snap2]);
    assert!(
        diff.contains("No file changes found."),
        "expected 'No file changes found.', got:\n{diff}"
    );
}

#[test]
fn cli_snapshot_delete_latest_rejects_alias() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("del.txt"), b"del\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
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
    write_test_config(&fx.config_path, &fx.repo_dir);

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);

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
    assert!(delete_out.contains("Run `vykar compact` to reclaim their space."));

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
        "repositories:\n  - url: {}\n    label: repo-a\n  - url: {}\n    label: repo-b\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\n",
        yaml_quote_path(&fx.repo_dir),
        yaml_quote_path(&repo_b),
    );
    std::fs::write(&fx.config_path, config).unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.init_repos(&cfg);

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
    write_test_config(&fx.config_path, &fx.repo_dir);
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
    write_test_config(&fx.config_path, &fx.repo_dir);
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
        .envs(fx.test_env())
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
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    // Init repo first
    fx.init_repos(&cfg);

    // Spawn daemon as a background process
    let mut child = Command::new(vykar_binary_path())
        .args(["--config", &cfg, "daemon"])
        .envs(fx.test_env())
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
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.init_repos(&cfg);

    // Spawn the first daemon — it should acquire the scheduler lock.
    let mut first = Command::new(vykar_binary_path())
        .args(["--config", &cfg, "daemon"])
        .envs(fx.test_env())
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
        .envs(fx.test_env())
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
    let yaml = "encryption:\n  mode: aes256gcm\n";
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
fn cli_compact_report_unreferenced_prints_counts() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();
    fx.init_repos(&cfg);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    let out = fx.run_ok(&["--config", &cfg, "compact", "--report-unreferenced"]);
    let lines: Vec<&str> = out.lines().collect();
    for prefix in [
        "Snapshots: 1 (",
        "Index entries: ",
        "Packs with unreferenced entries: 0 (",
    ] {
        assert!(
            lines.iter().any(|l| l.starts_with(prefix)),
            "no line starting with {prefix:?}:\n{out}"
        );
    }
    assert!(out.contains(" 0 unreferenced,"), "{out}");
}

#[test]
fn cli_compact_report_unreferenced_conflicts_with_dry_run() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();
    let (_, err) = fx.run_err(&[
        "--config",
        &cfg,
        "compact",
        "--report-unreferenced",
        "--dry-run",
    ]);
    assert!(err.contains("cannot be used with"), "{err}");
}

#[test]
fn cli_check_repair_dry_run_prints_plan() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
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

/// A lost index segment: the dry run says the index is rebuilt from the
/// packs, whose trailers describe every chunk, so nothing is removed.
#[test]
fn cli_check_repair_dry_run_reports_a_rebuild_of_a_damaged_chain() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
    std::fs::write(fx.source_a.join("a.txt"), b"alpha\n").unwrap();
    fx.run_ok(&["--config", &cfg, "backup", &source]);
    std::fs::write(fx.source_a.join("b.txt"), b"bravo\n").unwrap();
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    // No local cache of the index's state, on any platform.
    for dir in [
        fx.cache_dir.clone(),
        fx.home_dir.join("Library").join("Caches"),
    ] {
        let _ = std::fs::remove_dir_all(dir);
    }
    let segment = std::fs::read_dir(fx.repo_dir.join("index.d"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(segment).unwrap();

    let out = stdout(&fx.run(&["--config", &cfg, "check", "--repair", "--dry-run"]));
    assert!(
        out.contains("the index chain is damaged; the index is rebuilt from the packs"),
        "{out}"
    );
    assert!(out.contains("from pack trailers"), "{out}");
    assert!(
        !out.contains("Snapshots whose data the rebuilt index cannot locate"),
        "{out}"
    );
}

/// An unreadable index head without a local cache: the dry run says the
/// index is rebuilt from the packs.
#[test]
fn cli_check_repair_dry_run_reports_an_unreadable_head() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
    std::fs::write(fx.source_a.join("a.txt"), b"alpha\n").unwrap();
    fx.run_ok(&["--config", &cfg, "backup", &source]);
    std::fs::write(fx.source_a.join("b.txt"), b"bravo\n").unwrap();
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    for dir in [
        fx.cache_dir.clone(),
        fx.home_dir.join("Library").join("Caches"),
    ] {
        let _ = std::fs::remove_dir_all(dir);
    }
    let index = latest_head(&fx.repo_dir);
    let bytes = std::fs::read(&index).unwrap();
    std::fs::write(&index, &bytes[..bytes.len() / 2]).unwrap();

    let out = stdout(&fx.run(&["--config", &cfg, "check", "--repair", "--dry-run"]));
    assert!(
        out.contains("the index head cannot be read; the index is rebuilt from the packs"),
        "{out}"
    );
    assert!(!out.contains("describe none of their contents"), "{out}");
}

#[test]
fn cli_check_repair_yes_repairs_then_clean() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
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
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
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
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
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
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    // No corruption — safe plan (the index sweep only)
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
        out.contains("Remove index entries that no surviving snapshot references"),
        "expected 'Remove index entries that no surviving snapshot references' in stdout, got:\n{out}"
    );
}

#[test]
fn cli_check_repair_yes_clean_repo() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("some.txt"), b"some data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.init_repos(&cfg);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    // No corruption — --yes should still work on a clean repo
    let out = fx.run_ok(&["--config", &cfg, "check", "--repair", "--yes"]);
    assert!(
        out.contains("Repair plan:"),
        "expected 'Repair plan:' in stdout, got:\n{out}"
    );
    assert!(
        out.contains("Remove index entries that no surviving snapshot references"),
        "expected 'Remove index entries that no surviving snapshot references' in stdout, got:\n{out}"
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
        .envs(fx.test_env())
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
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: true\n  every: \"2s\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.init_repos(&cfg);

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
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources:\n  - {}\nschedule:\n  enabled: true\n  every: \"2s\"\n  on_startup: true\n",
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
fn cli_daemon_sighup_picks_up_changed_passphrase() {
    let fx = CliFixture::new();
    std::fs::write(fx.source_a.join("a.txt"), b"passphrase reload\n").unwrap();
    let write_config = |passphrase: &str| {
        let config = format!(
            "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: {passphrase}\nsources:\n  - {}\nschedule:\n  enabled: true\n  every: \"2s\"\n  on_startup: true\n",
            yaml_quote_path(&fx.repo_dir),
            yaml_quote_path(&fx.source_a),
        );
        std::fs::write(&fx.config_path, config).unwrap();
    };
    let cfg = fx.config_path.to_string_lossy().to_string();

    write_config("right");
    fx.init_repos(&cfg);

    write_config("wrong");
    let (mut child, logs) = spawn_daemon(&fx, &cfg);
    assert!(
        logs.wait_for("backup cycle finished with errors", Duration::from_secs(20)),
        "first cycle should fail with the wrong passphrase, got stderr:\n{}",
        logs.snapshot()
    );

    write_config("right");
    send_signal(&child, "-HUP");
    let reload_marker = "configuration reloaded successfully";
    assert!(
        logs.wait_for(reload_marker, Duration::from_secs(20)),
        "reload should succeed, got stderr:\n{}",
        logs.snapshot()
    );

    // Only a cycle after the reload counts; without clearing the cache the
    // daemon keeps the wrong passphrase it resolved first.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let succeeded_after_reload = loop {
        let current = logs.snapshot();
        let pos = current.find(reload_marker).unwrap();
        if current[pos..].contains("backup cycle finished successfully") {
            break true;
        }
        if std::time::Instant::now() > deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(100));
    };

    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);
    assert!(
        succeeded_after_reload,
        "a cycle after the reload should succeed, got stderr:\n{}",
        logs.snapshot()
    );
}

#[test]
#[cfg(unix)]
fn cli_daemon_sighup_invalid_config_keeps_old() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.init_repos(&cfg);

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
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.init_repos(&cfg);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    assert!(
        logs.wait_for("Summary", Duration::from_secs(15)),
        "first cycle should complete"
    );

    // Overwrite config with empty repositories
    std::fs::write(
        &fx.config_path,
        "encryption:\n  mode: aes256gcm\nschedule:\n  enabled: true\n  every: \"1h\"\n",
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
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.init_repos(&cfg);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    assert!(
        logs.wait_for("Summary", Duration::from_secs(15)),
        "first cycle should complete"
    );

    // Overwrite config with schedule.enabled: false
    let disabled_config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: false\n  every: \"1h\"\n",
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
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.init_repos(&cfg);

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
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: false\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.init_repos(&cfg);

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
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.init_repos(&cfg);

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
fn cli_daemon_per_repo_schedule_runs_only_due_repo() {
    let fx = CliFixture::new();
    let repo_b = fx._tmp.path().join("repo_b");
    std::fs::create_dir_all(&repo_b).unwrap();
    std::fs::write(fx.source_a.join("file.txt"), b"content").unwrap();

    // repo-a overrides the global hourly cadence with a 2s one that also fires
    // at startup; repo-b stays on the global hourly schedule.
    let config = format!(
        "repositories:\n  \
           - url: {}\n    label: repo-a\n    schedule:\n      enabled: true\n      every: \"2s\"\n      on_startup: true\n  \
           - url: {}\n    label: repo-b\n\
         encryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\n\
         schedule:\n  enabled: true\n  every: \"1h\"\n\
         sources:\n  - {}\n",
        yaml_quote_path(&fx.repo_dir),
        yaml_quote_path(&repo_b),
        yaml_quote_path(&fx.source_a),
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.init_repos(&cfg);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    assert!(
        logs.wait_for("next backup scheduled", Duration::from_secs(15)),
        "daemon should schedule the next per-repo run, got stderr:\n{}",
        logs.snapshot()
    );
    assert!(
        logs.wait_for("backup cycle finished", Duration::from_secs(15)),
        "repo-a's on_startup cycle should run, got stderr:\n{}",
        logs.snapshot()
    );

    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);

    let list_a = fx.run_ok(&["--config", &cfg, "list", "-R", "repo-a"]);
    assert!(
        list_a.contains("source-a"),
        "repo-a should have a snapshot from its own 2s schedule, got:\n{list_a}"
    );

    let list_b = fx.run_ok(&["--config", &cfg, "list", "-R", "repo-b"]);
    assert!(
        !list_b.contains("source-a"),
        "repo-b is on the 1h global schedule and must not have run, got:\n{list_b}"
    );
}

#[test]
#[cfg(unix)]
fn cli_daemon_starts_when_only_one_repo_enables_schedule() {
    let fx = CliFixture::new();
    let repo_b = fx._tmp.path().join("repo_b");
    std::fs::create_dir_all(&repo_b).unwrap();

    // Global schedule disabled; a single repo opts in. The daemon must start.
    let config = format!(
        "repositories:\n  \
           - url: {}\n    label: repo-a\n  \
           - url: {}\n    label: repo-b\n    schedule:\n      enabled: true\n      every: \"1h\"\n\
         encryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\n\
         schedule:\n  enabled: false\n\
         sources: []\n",
        yaml_quote_path(&fx.repo_dir),
        yaml_quote_path(&repo_b),
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    fx.init_repos(&cfg);

    let (mut child, logs) = spawn_daemon(&fx, &cfg);

    assert!(
        logs.wait_for("next backup scheduled", Duration::from_secs(15)),
        "daemon should start when any repo enables its schedule, got stderr:\n{}",
        logs.snapshot()
    );

    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);
}

#[test]
#[cfg(unix)]
fn cli_daemon_trust_repo_rejected() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources: []\nschedule:\n  enabled: true\n  every: \"1h\"\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    let output = Command::new(vykar_binary_path())
        .args(["--trust-repo", "--config", &cfg, "daemon"])
        .envs(fx.test_env())
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
    let config = "encryption:\n  mode: aes256gcm\nschedule:\n  enabled: true\n  every: \"1h\"\n";
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    let output = Command::new(vykar_binary_path())
        .args(["--config", &cfg, "daemon"])
        .envs(fx.test_env())
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr_str = stderr(&output);
    assert!(
        stderr_str.contains("no repositories configured"),
        "expected 'no repositories configured' error, got:\n{stderr_str}"
    );
}

// ── Top-level --repo/--source and skip-unavailable tests ───────────────────

/// Helper: write a two-repo config with labels and per-source retention.
fn write_two_repo_sources_config(
    config_path: &Path,
    repo_a: &Path,
    repo_b: &Path,
    source_a: &Path,
    source_b: &Path,
) {
    let config = format!(
        "repositories:\n\
         \x20 - url: {}\n\
         \x20   label: repo-a\n\
         \x20 - url: {}\n\
         \x20   label: repo-b\n\
         encryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\n\
         retention:\n  keep_last: 5\n\
         sources:\n\
         \x20 - path: {}\n\
         \x20   label: src-a\n\
         \x20 - path: {}\n\
         \x20   label: src-b\n",
        yaml_quote_path(repo_a),
        yaml_quote_path(repo_b),
        yaml_quote_path(source_a),
        yaml_quote_path(source_b),
    );
    std::fs::write(config_path, config).unwrap();
}

#[test]
fn cli_bare_source_without_repo_rejected() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();

    let (_stdout, stderr) = fx.run_err(&["--config", &cfg, "-S", "src-a"]);
    assert!(
        stderr.contains("required") || stderr.contains("--repo"),
        "expected --source requires --repo error, got:\n{stderr}"
    );
}

#[test]
fn cli_bare_repo_source_with_subcommand_rejected() {
    let fx = CliFixture::new();
    write_test_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();

    let (_stdout, stderr) = fx.run_err(&["--config", &cfg, "-R", "repo-a", "backup"]);
    assert!(
        stderr.contains("cannot be combined with a subcommand"),
        "expected subcommand conflict error, got:\n{stderr}"
    );
}

#[test]
fn cli_bare_repo_source_filters_cycle() {
    let fx = CliFixture::new();
    write_sources_config(&fx.config_path, &fx.repo_dir, &fx.source_a, &fx.source_b, 5);
    std::fs::write(fx.source_a.join("a.txt"), b"aaa").unwrap();
    std::fs::write(fx.source_b.join("b.txt"), b"bbb").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.init_repos(&cfg);

    // Run full cycle filtered to src-a only
    let out = fx.run_ok(&[
        "--config",
        &cfg,
        "-R",
        &fx.repo_dir.to_string_lossy(),
        "-S",
        "src-a",
    ]);
    assert!(out.contains("src-a"), "should back up src-a, got:\n{out}");
    assert!(
        !out.contains("src-b"),
        "should NOT back up src-b, got:\n{out}"
    );

    // Verify only one snapshot was created (src-a only)
    let list_out = fx.run_ok(&["--config", &cfg, "list"]);
    assert!(
        list_out.contains("src-a"),
        "list should show src-a snapshot, got:\n{list_out}"
    );
    assert!(
        !list_out.contains("src-b"),
        "list should NOT show src-b snapshot, got:\n{list_out}"
    );
}

#[test]
fn cli_bare_invalid_source_fails_backup_check_still_runs() {
    let fx = CliFixture::new();
    write_sources_config(&fx.config_path, &fx.repo_dir, &fx.source_a, &fx.source_b, 5);
    std::fs::write(fx.source_a.join("a.txt"), b"aaa").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.init_repos(&cfg);

    // Run with invalid source label — should fail overall
    let (stdout, stderr) = fx.run_err(&[
        "--config",
        &cfg,
        "-R",
        &fx.repo_dir.to_string_lossy(),
        "-S",
        "nonexistent",
    ]);
    let combined = format!("{stdout}{stderr}");

    // Backup should fail with unknown source
    assert!(
        combined.contains("FAILED") || combined.contains("nonexistent"),
        "backup should fail for invalid source, got:\n{combined}"
    );

    // Check should still run (it's source-agnostic)
    assert!(
        combined.contains("check"),
        "check step should still appear in output, got:\n{combined}"
    );
}

#[test]
fn cli_bare_multi_repo_skips_missing_local() {
    let fx = CliFixture::new();
    let repo_b = fx._tmp.path().join("repo_b");
    // Note: repo_b directory is NOT created — simulates unmounted disk

    write_two_repo_sources_config(
        &fx.config_path,
        &fx.repo_dir,
        &repo_b,
        &fx.source_a,
        &fx.source_b,
    );
    std::fs::write(fx.source_a.join("a.txt"), b"aaa").unwrap();
    std::fs::write(fx.source_b.join("b.txt"), b"bbb").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.init_repo_labelled(&cfg, "repo-a");

    // Bare command across both repos — repo-b is missing, should be skipped
    let output = fx.run(&["--config", &cfg]);
    let err = stderr(&output);

    assert!(
        err.contains("skipping 'repo-b'"),
        "should warn about skipping repo-b, got:\n{err}"
    );
    assert!(
        output.status.success(),
        "should exit successfully when missing repo is skipped, got status: {}, stderr:\n{err}",
        output.status
    );
}

#[test]
#[cfg(unix)]
fn cli_bare_multi_repo_missing_local_runs_hooks_then_skips() {
    let fx = CliFixture::new();
    let repo_b = fx._tmp.path().join("repo_b");
    write_two_repo_sources_config(
        &fx.config_path,
        &fx.repo_dir,
        &repo_b,
        &fx.source_a,
        &fx.source_b,
    );
    std::fs::write(fx.source_a.join("a.txt"), b"aaa").unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.init_repo_labelled(&cfg, "repo-a");

    let log = fx._tmp.path().join("hooks.log");
    let mut config = std::fs::read_to_string(&fx.config_path).unwrap();
    config.push_str("hooks:\n");
    for name in FIXTURE_HOOKS {
        let cmd = format!("echo {name}:$VYKAR_LABEL >> '{}'", log.display());
        config.push_str(&format!("  {name}: {}\n", yaml_single(&cmd)));
    }
    std::fs::write(&fx.config_path, config).unwrap();

    let output = fx.run(&["--config", &cfg]);
    let err = stderr(&output);
    assert!(output.status.success(), "{err}");
    assert!(err.contains("Warning: skipping 'repo-b'"), "{err}");
    let repo_b_hooks: Vec<String> = hook_log(&log)
        .into_iter()
        .filter(|l| l.ends_with(":repo-b"))
        .collect();
    assert_eq!(
        repo_b_hooks,
        ["before_backup:repo-b", "finally_backup:repo-b"]
    );
}

#[test]
fn cli_bare_single_repo_missing_is_skipped() {
    let fx = CliFixture::new();
    let missing_repo = fx._tmp.path().join("nonexistent_repo");
    write_test_config(&fx.config_path, &missing_repo);
    let cfg = fx.config_path.to_string_lossy().to_string();

    let output = fx.run(&["--config", &cfg]);
    let err = stderr(&output);
    assert!(
        output.status.success(),
        "a missing single repo is skipped, got status: {}, stderr:\n{err}",
        output.status
    );
    assert!(err.contains("skipping"), "{err}");
    assert!(err.contains("repository not found at"), "{err}");
    assert!(err.contains("vykar init"), "{err}");
}

#[test]
fn cli_bare_single_repo_missing_with_repo_flag_errors() {
    let fx = CliFixture::new();
    let missing_repo = fx._tmp.path().join("nonexistent_repo");
    std::fs::write(fx.source_a.join("a.txt"), b"aaa").unwrap();
    let config = format!(
        "repositories:\n  - url: {}\n    label: nas\nencryption:\n  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\nsources:\n  - path: {}\n    label: src\n",
        yaml_quote_path(&missing_repo),
        yaml_quote_path(&fx.source_a)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    let (_, err) = fx.run_err(&["--config", &cfg, "-R", "nas"]);
    assert!(!err.contains("skipping"), "{err}");
    assert!(
        err.lines()
            .any(|l| l.contains("backup") && l.contains("FAILED: repository not found at")),
        "{err}"
    );
}

/// YAML single-quoted scalar.
#[cfg(unix)]
fn yaml_single(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(unix)]
const FIXTURE_HOOKS: [&str; 4] = [
    "before_backup",
    "after_backup",
    "failed_backup",
    "finally_backup",
];

/// The shared unavailable-repository fixture: `nas` at `<tmp>/volume/repo`
/// (not created), one source with one file, and every backup hook appending its
/// name to the returned `<tmp>/hooks.log`. `before_backup` replaces that hook's
/// command; `encryption` and `extra` are YAML blocks spliced in as given.
#[cfg(unix)]
fn write_unavailable_config(
    fx: &CliFixture,
    before_backup: Option<&str>,
    encryption: &str,
    extra: &str,
) -> PathBuf {
    let tmp = fx._tmp.path();
    let log = tmp.join("hooks.log");
    std::fs::write(fx.source_a.join("a.txt"), b"aaa").unwrap();
    let hooks: String = FIXTURE_HOOKS
        .iter()
        .map(|name| {
            let cmd = match before_backup {
                Some(cmd) if *name == "before_backup" => cmd.to_string(),
                _ => format!("echo {name} >> '{}'", log.display()),
            };
            format!("  {name}: {}\n", yaml_single(&cmd))
        })
        .collect();
    let config = format!(
        "repositories:\n  - url: {}\n    label: nas\nencryption:\n{encryption}hooks:\n{hooks}sources:\n  - path: {}\n    label: src\n{extra}",
        yaml_single(&tmp.join("volume").join("repo").to_string_lossy()),
        yaml_single(&fx.source_a.to_string_lossy()),
    );
    std::fs::write(&fx.config_path, config).unwrap();
    log
}

#[cfg(unix)]
fn hook_log(log: &Path) -> Vec<String> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim().to_string())
        .collect()
}

#[cfg(unix)]
const PLAIN: &str = "  mode: aes256gcm\n  passphrase: \"vykar-test-passphrase\"\n";

/// Initializes `<tmp>/volume/repo` and moves `volume` to `store`, returning
/// a `before_backup` command that moves it back, as a mount would, and logs
/// its name.
#[cfg(unix)]
fn init_then_unmount(fx: &CliFixture, cfg: &str) -> String {
    let tmp = fx._tmp.path();
    let volume = tmp.join("volume");
    let store = tmp.join("store");
    std::fs::create_dir_all(volume.join("repo")).unwrap();
    fx.init_repos(cfg);
    std::fs::rename(&volume, &store).unwrap();
    format!(
        "mv '{}' '{}' && echo before_backup >> '{}'",
        store.display(),
        volume.display(),
        tmp.join("hooks.log").display()
    )
}

#[test]
#[cfg(unix)]
fn cli_bare_unavailable_repo_runs_hooks_then_skips() {
    let fx = CliFixture::new();
    let log = write_unavailable_config(&fx, None, PLAIN, "");
    let cfg = fx.config_path.to_string_lossy().to_string();

    let output = fx.run(&["--config", &cfg]);
    let err = stderr(&output);
    assert!(output.status.success(), "{err}");
    assert!(err.contains("Warning: skipping 'nas'"), "{err}");
    assert!(err.contains("vykar init"), "{err}");
    assert_eq!(hook_log(&log), vec!["before_backup", "finally_backup"]);
}

#[test]
#[cfg(unix)]
fn cli_bare_before_hook_that_mounts_the_repository_runs_the_backup() {
    let fx = CliFixture::new();
    let log = write_unavailable_config(&fx, None, PLAIN, "");
    let cfg = fx.config_path.to_string_lossy().to_string();
    let mount = init_then_unmount(&fx, &cfg);
    write_unavailable_config(&fx, Some(&mount), PLAIN, "");

    let output = fx.run(&["--config", &cfg]);
    let (out, err) = (stdout(&output), stderr(&output));
    assert!(output.status.success(), "{out}\n{err}");
    assert!(out.contains("Snapshot created"), "{out}");
    assert!(!err.contains("skipping"), "{err}");
    assert!(hook_log(&log).iter().any(|l| l == "after_backup"));
}

#[test]
#[cfg(unix)]
fn cli_bare_unavailable_encrypted_repo_resolves_no_passphrase() {
    let fx = CliFixture::new();
    let marker = fx._tmp.path().join("pass-ran");
    let encryption = format!(
        "  mode: aes256gcm\n  passcommand: {}\n",
        yaml_single(&format!("touch '{}' && echo secret", marker.display()))
    );
    let log = write_unavailable_config(&fx, None, &encryption, "");
    let cfg = fx.config_path.to_string_lossy().to_string();

    let output = fx.run(&["--config", &cfg]);
    let err = stderr(&output);
    assert!(output.status.success(), "{err}");
    assert!(!marker.exists(), "passcommand ran for a missing repository");
    assert_eq!(hook_log(&log), vec!["before_backup", "finally_backup"]);
}

/// `aes256gcm` with a passcommand that logs `pass` to the hook log.
#[cfg(unix)]
fn logging_passcommand(fx: &CliFixture) -> String {
    let log = fx._tmp.path().join("hooks.log");
    format!(
        "  mode: aes256gcm\n  passcommand: {}\n",
        yaml_single(&format!("echo pass >> '{}' && echo secret", log.display()))
    )
}

#[test]
#[cfg(unix)]
fn cli_bare_mounted_encrypted_repo_resolves_passphrase_after_before_hooks() {
    let fx = CliFixture::new();
    let encryption = logging_passcommand(&fx);
    let log = write_unavailable_config(&fx, None, &encryption, "");
    let cfg = fx.config_path.to_string_lossy().to_string();
    let mount = init_then_unmount(&fx, &cfg);
    write_unavailable_config(&fx, Some(&mount), &encryption, "");
    let _ = std::fs::remove_file(&log);

    let output = fx.run(&["--config", &cfg]);
    let (out, err) = (stdout(&output), stderr(&output));
    assert!(output.status.success(), "{out}\n{err}");
    assert!(out.contains("Snapshot created"), "{out}");
    let lines = hook_log(&log);
    assert_eq!(
        lines.get(..3),
        Some(&["before_backup", "pass", "after_backup"].map(String::from)[..]),
        "{lines:?}"
    );
}

#[test]
#[cfg(unix)]
fn cli_bare_present_encrypted_repo_resolves_passphrase_before_hooks() {
    let fx = CliFixture::new();
    let encryption = logging_passcommand(&fx);
    let log = write_unavailable_config(&fx, None, &encryption, "");
    let cfg = fx.config_path.to_string_lossy().to_string();
    std::fs::create_dir_all(fx._tmp.path().join("volume").join("repo")).unwrap();
    fx.init_repos(&cfg);
    let _ = std::fs::remove_file(&log);

    let output = fx.run(&["--config", &cfg]);
    let err = stderr(&output);
    assert!(output.status.success(), "{err}");
    let lines = hook_log(&log);
    assert_eq!(
        lines.get(..2),
        Some(&["pass", "before_backup"].map(String::from)[..]),
        "{lines:?}"
    );
}

#[test]
#[cfg(unix)]
fn cli_bare_mounted_encrypted_repo_passphrase_failure_fails_the_backup() {
    let fx = CliFixture::new();
    let encryption = logging_passcommand(&fx);
    let log = write_unavailable_config(&fx, None, &encryption, "");
    let cfg = fx.config_path.to_string_lossy().to_string();
    let mount = init_then_unmount(&fx, &cfg);
    let failing = format!(
        "  mode: aes256gcm\n  passcommand: {}\n",
        yaml_single(&format!("echo pass >> '{}' && exit 1", log.display()))
    );
    write_unavailable_config(&fx, Some(&mount), &failing, "");
    let _ = std::fs::remove_file(&log);

    let output = fx.run(&["--config", &cfg]);
    let err = stderr(&output);
    assert_eq!(output.status.code(), Some(1), "{err}");
    assert!(
        err.lines().any(|l| l.contains("backup")
            && l.contains("FAILED: configuration error: passcommand failed")),
        "{err}"
    );
    assert_eq!(
        hook_log(&log),
        ["before_backup", "pass", "failed_backup", "finally_backup"]
    );
}

#[test]
#[cfg(unix)]
fn cli_daemon_skips_unavailable_single_repo() {
    let fx = CliFixture::new();
    let log = write_unavailable_config(
        &fx,
        None,
        PLAIN,
        "schedule:\n  enabled: true\n  every: \"1h\"\n  on_startup: true\n",
    );
    let cfg = fx.config_path.to_string_lossy().to_string();

    let (mut child, logs) = spawn_daemon(&fx, &cfg);
    let found = logs.wait_for("every repository was skipped", Duration::from_secs(15));
    send_signal(&child, "-TERM");
    wait_for_exit(&mut child, 10);
    let out = logs.snapshot();

    assert!(found, "{out}");
    assert!(
        out.lines().any(|l| l.contains("WARN")
            && l.contains("skipping unavailable repository")
            && l.contains("vykar init")),
        "{out}"
    );
    assert_eq!(hook_log(&log), vec!["before_backup", "finally_backup"]);
}

#[test]
fn cli_multi_repo_init_not_skipped() {
    let fx = CliFixture::new();
    let repo_b = fx._tmp.path().join("repo_b");
    std::fs::create_dir_all(&repo_b).unwrap();

    write_two_repo_sources_config(
        &fx.config_path,
        &fx.repo_dir,
        &repo_b,
        &fx.source_a,
        &fx.source_b,
    );

    let cfg = fx.config_path.to_string_lossy().to_string();

    // Init without -R across both repos — neither should be skipped
    let out = fx.run_ok(&["--config", &cfg, "init"]);
    // Both repos should show initialization messages
    let init_count = out.matches("Repository initialized at:").count();
    assert_eq!(
        init_count, 2,
        "both repos should be initialized, got {init_count} init messages:\n{out}"
    );

    // Verify both have config files
    assert!(
        fx.repo_dir.join("config").exists(),
        "repo-a should be initialized"
    );
    assert!(
        repo_b.join("config").exists(),
        "repo-b should be initialized"
    );
}

// ---------------------------------------------------------------------------
// `vykar key export` / `vykar key import`
// ---------------------------------------------------------------------------

const KEY_PASS: &str = "cli-key-command-passphrase";

fn write_encrypted_config(config_path: &Path, repo_dir: &Path) {
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: aes256gcm\nsources: []\n",
        yaml_quote_path(repo_dir)
    );
    std::fs::write(config_path, config).unwrap();
}

/// An initialized, encrypted repository plus its config path.
fn encrypted_fixture() -> (CliFixture, String) {
    let fx = CliFixture::new();
    write_encrypted_config(&fx.config_path, &fx.repo_dir);
    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.run_encrypted_ok(&["--config", &cfg, "init"], KEY_PASS);
    (fx, cfg)
}

/// Every identity pin this fixture's client wrote. The cache root is
/// platform-dependent, so look in both places.
fn pin_paths(fx: &CliFixture) -> Vec<PathBuf> {
    let candidates = [
        fx.cache_dir.join("vykar"),
        fx.home_dir.join("Library").join("Caches").join("vykar"),
    ];
    let mut out = Vec::new();
    for dir in candidates {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with("pin3.") {
                out.push(entry.path());
            }
        }
    }
    out
}

/// Decode the base64 body of an armored export back to the stored key blob.
fn armor_body(armor: &str) -> Vec<u8> {
    use base64::Engine;
    let body: String = armor
        .lines()
        .map(str::trim)
        .filter(|l| !l.starts_with("-----") && !l.starts_with("repository:") && !l.is_empty())
        .collect();
    base64::engine::general_purpose::STANDARD
        .decode(body.as_bytes())
        .unwrap()
}

/// Delete every identity pin this fixture's client wrote. The cache root is
/// platform-dependent (`$XDG_CACHE_HOME/vykar` on Linux,
/// `$HOME/Library/Caches/vykar` on macOS), so try both. Returns how many were
/// removed.
fn forget_pins(fx: &CliFixture) -> usize {
    let candidates = [
        fx.cache_dir.join("vykar"),
        fx.home_dir.join("Library").join("Caches").join("vykar"),
    ];
    let mut removed = 0;
    for dir in candidates {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with("pin3.") {
                std::fs::remove_file(entry.path()).unwrap();
                removed += 1;
            }
        }
    }
    removed
}

fn repo_id_hex(repo_dir: &Path) -> String {
    let data = std::fs::read(repo_dir.join("config")).unwrap();
    let config = vykar_core::repo::RepoConfig::decode(&data)
        .unwrap()
        .into_repo_config();
    hex::encode(&config.id)
}

#[test]
fn cli_key_export_writes_the_armor_to_stdout() {
    let (fx, cfg) = encrypted_fixture();
    let output = fx.run_encrypted_ok(&["--config", &cfg, "key", "export"], KEY_PASS);

    // The armor is the *result*, so it must survive `2>/dev/null` — and it
    // must be the only thing on stdout, so it can be piped straight into a
    // password manager.
    let out = stdout(&output);
    assert!(
        out.starts_with("-----BEGIN VYKAR REPOSITORY KEY-----\n"),
        "{out}"
    );
    assert!(
        out.ends_with("-----END VYKAR REPOSITORY KEY-----\n"),
        "{out}"
    );
    assert!(
        out.contains(&format!("repository: {}", repo_id_hex(&fx.repo_dir))),
        "{out}"
    );

    // Status goes to stderr, and never the armor.
    let err = stderr(&output);
    assert!(err.contains("Verified"), "{err}");
    assert!(!err.contains("BEGIN VYKAR REPOSITORY KEY"), "{err}");
}

/// `encryption.mode: none` is refused while the config is parsed: `init`
/// fails with the targeted message and writes no repository.
#[test]
fn cli_init_refuses_encryption_mode_none() {
    let fx = CliFixture::new();
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\n",
        yaml_quote_path(&fx.repo_dir)
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();

    let (_out, err) = fx.run_err(&["--config", &cfg, "init"]);
    assert!(err.contains("no longer supported"), "{err}");
    assert!(!fx.repo_dir.join("config").exists());
}

#[test]
fn cli_key_import_restores_a_repository_that_can_no_longer_be_opened() {
    let (fx, cfg) = encrypted_fixture();
    let export_path = fx.repo_dir.parent().unwrap().join("repokey.txt");
    fx.run_encrypted_ok(
        &[
            "--config",
            &cfg,
            "key",
            "export",
            "-o",
            export_path.to_str().unwrap(),
        ],
        KEY_PASS,
    );

    for name in ["repokey", "repokey.2"] {
        std::fs::remove_file(fx.repo_dir.join("keys").join(name)).unwrap();
    }
    let opened = fx.run_encrypted(&["--config", &cfg, "list"], KEY_PASS);
    assert!(!opened.status.success(), "the repo must be unopenable now");

    let out = fx.run_encrypted_ok(
        &[
            "--config",
            &cfg,
            "key",
            "import",
            export_path.to_str().unwrap(),
        ],
        KEY_PASS,
    );
    let out = stdout(&out);
    assert!(out.contains("keys/repokey: created"), "{out}");
    assert!(out.contains("keys/repokey.2: created"), "{out}");

    fx.run_encrypted_ok(&["--config", &cfg, "list"], KEY_PASS);
}

#[test]
fn cli_key_import_leaves_a_matching_primary_untouched() {
    let (fx, cfg) = encrypted_fixture();
    let export_path = fx.repo_dir.parent().unwrap().join("repokey.txt");
    fx.run_encrypted_ok(
        &[
            "--config",
            &cfg,
            "key",
            "export",
            "-o",
            export_path.to_str().unwrap(),
        ],
        KEY_PASS,
    );
    std::fs::remove_file(fx.repo_dir.join("keys").join("repokey.2")).unwrap();

    let out = stdout(&fx.run_encrypted_ok(
        &[
            "--config",
            &cfg,
            "key",
            "import",
            export_path.to_str().unwrap(),
        ],
        KEY_PASS,
    ));
    assert!(
        out.contains("keys/repokey: already correct, left untouched"),
        "{out}"
    );
    assert!(out.contains("keys/repokey.2: created"), "{out}");
}

/// A key from a *different* repository, wrapped under the same passphrase,
/// unwraps perfectly — and with the `repository:` header rewritten, the paste
/// check passes too. Only `prove_key` catches it, and `--force` must not
/// push it through.
#[test]
fn cli_key_import_refuses_a_foreign_key_even_with_force() {
    let (victim, victim_cfg) = encrypted_fixture();
    let (attacker, attacker_cfg) = encrypted_fixture();

    let foreign =
        stdout(&attacker.run_encrypted_ok(&["--config", &attacker_cfg, "key", "export"], KEY_PASS));
    // Rewrite the header so the repository-id check cannot be what refuses it.
    let disguised: String = foreign
        .lines()
        .map(|line| {
            if line.starts_with("repository:") {
                format!("repository: {}", repo_id_hex(&victim.repo_dir))
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let path = victim.repo_dir.parent().unwrap().join("foreign.txt");
    std::fs::write(&path, format!("{disguised}\n")).unwrap();

    let refused = victim.run_encrypted(
        &[
            "--config",
            &victim_cfg,
            "key",
            "import",
            path.to_str().unwrap(),
        ],
        KEY_PASS,
    );
    assert!(!refused.status.success(), "the import must be refused");
    let err = stderr(&refused);
    assert!(err.contains("refusing to import"), "{err}");

    // --force does not override a pin contradiction.
    let forced = victim.run_encrypted(
        &[
            "--config",
            &victim_cfg,
            "key",
            "import",
            path.to_str().unwrap(),
            "--force",
        ],
        KEY_PASS,
    );
    assert!(!forced.status.success(), "--force must not override this");
    assert!(
        stderr(&forced).contains("--force does not override"),
        "{}",
        stderr(&forced)
    );

    // And the victim's own key is untouched.
    victim.run_encrypted_ok(&["--config", &victim_cfg, "list"], KEY_PASS);
}

/// Restoring a key copy is tier-1: it applies without the `type 'repair'`
/// prompt, because `check` only runs once a key has been established.
#[test]
fn cli_check_repair_heals_a_key_copy_without_prompting() {
    let (fx, cfg) = encrypted_fixture();
    let good = std::fs::read(fx.repo_dir.join("keys").join("repokey")).unwrap();
    std::fs::write(fx.repo_dir.join("keys").join("repokey.2"), b"garbage").unwrap();

    let output = fx.run_encrypted_ok(&["--config", &cfg, "check", "--repair"], KEY_PASS);
    let out = stdout(&output);
    assert!(
        out.contains("Restore repository key copy keys/repokey.2"),
        "{out}"
    );
    assert!(
        stderr(&output).contains("No data-loss actions; applying safe repairs"),
        "{}",
        stderr(&output)
    );
    assert_eq!(
        std::fs::read(fx.repo_dir.join("keys").join("repokey.2")).unwrap(),
        good
    );
}

#[test]
fn cli_check_summary_reports_the_key_copies() {
    let (fx, cfg) = encrypted_fixture();
    let out = stdout(&fx.run_encrypted_ok(&["--config", &cfg, "check"], KEY_PASS));
    assert!(out.contains("Check complete: 2 key copies"), "{out}");
}

/// Nothing left to prove the key against — no pin on this client, and no
/// decryptable repository object. Refused by default; `--force` is the
/// override for *this case only*.
#[test]
fn cli_key_import_unproven_key_needs_force() {
    let (fx, cfg) = encrypted_fixture();
    let export_path = fx.repo_dir.parent().unwrap().join("repokey.txt");
    fx.run_encrypted_ok(
        &[
            "--config",
            &cfg,
            "key",
            "export",
            "-o",
            export_path.to_str().unwrap(),
        ],
        KEY_PASS,
    );

    // Remove every piece of evidence: the repository's own ciphertext...
    for entry in std::fs::read_dir(fx.repo_dir.join("index.h"))
        .unwrap()
        .flatten()
    {
        std::fs::remove_file(entry.path()).unwrap();
    }
    for entry in std::fs::read_dir(fx.repo_dir.join("snapshots"))
        .unwrap()
        .flatten()
    {
        std::fs::remove_file(entry.path()).unwrap();
    }
    // ...and this client's identity pin.
    let removed = forget_pins(&fx);
    assert!(removed > 0, "expected `init` to have pinned the identity");
    for name in ["repokey", "repokey.2"] {
        std::fs::remove_file(fx.repo_dir.join("keys").join(name)).unwrap();
    }

    let refused = fx.run_encrypted(
        &[
            "--config",
            &cfg,
            "key",
            "import",
            export_path.to_str().unwrap(),
        ],
        KEY_PASS,
    );
    assert!(!refused.status.success(), "an unproven key must be refused");
    assert!(
        stderr(&refused).contains("could not be verified"),
        "{}",
        stderr(&refused)
    );

    let forced = fx.run_encrypted_ok(
        &[
            "--config",
            &cfg,
            "key",
            "import",
            export_path.to_str().unwrap(),
            "--force",
        ],
        KEY_PASS,
    );
    assert!(
        stderr(&forced).contains("importing an unverified key"),
        "{}",
        stderr(&forced)
    );
    assert!(fx.repo_dir.join("keys").join("repokey").exists());
    assert!(fx.repo_dir.join("keys").join("repokey.2").exists());
}

/// **Export must not pick "the first copy that unwraps".**
///
/// A planted primary that shares the passphrase unwraps perfectly. Ordinary
/// commands resolve the divergence and open off the genuine secondary; an
/// export that skipped that resolution would write out the planted key and
/// print success, producing a recovery backup that restores nothing.
#[test]
fn cli_key_export_exports_the_genuine_copy_not_a_planted_one() {
    let (fx, cfg) = encrypted_fixture();
    let keys = fx.repo_dir.join("keys");
    let genuine = std::fs::read(keys.join("repokey")).unwrap();

    // Plant a foreign key, wrapped under the same passphrase, as the primary.
    let (attacker, attacker_cfg) = encrypted_fixture();
    let foreign = armor_body(&stdout(
        &attacker.run_encrypted_ok(&["--config", &attacker_cfg, "key", "export"], KEY_PASS),
    ));
    assert_ne!(foreign, genuine);
    std::fs::write(keys.join("repokey"), &foreign).unwrap();

    // Ordinary commands still work — they open off the genuine secondary.
    fx.run_encrypted_ok(&["--config", &cfg, "list"], KEY_PASS);

    let exported = armor_body(&stdout(
        &fx.run_encrypted_ok(&["--config", &cfg, "key", "export"], KEY_PASS),
    ));
    assert_eq!(
        exported, genuine,
        "export must resolve the divergence, not take the first copy that unwraps"
    );
}

/// `.mode()` only applies at creation, so an export over an existing
/// world-readable file has to tighten it explicitly.
#[cfg(unix)]
#[test]
fn cli_key_export_tightens_permissions_on_an_existing_file() {
    use std::os::unix::fs::PermissionsExt;

    let (fx, cfg) = encrypted_fixture();
    let dest = fx.repo_dir.parent().unwrap().join("preexisting.txt");
    std::fs::write(&dest, "stale contents").unwrap();
    std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o644)).unwrap();

    fx.run_encrypted_ok(
        &[
            "--config",
            &cfg,
            "key",
            "export",
            "-o",
            dest.to_str().unwrap(),
        ],
        KEY_PASS,
    );

    let mode = std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "export must not leave a 0644 key file behind");
    assert!(
        std::fs::read_to_string(&dest)
            .unwrap()
            .contains("BEGIN VYKAR REPOSITORY KEY")
    );
}

/// **`--trust-repo` must re-pin.** It waives the pin to authorize the import;
/// leaving the stale pin behind makes the very next ordinary command fail, so
/// the import looks like it did nothing.
#[test]
fn cli_key_import_with_trust_repo_updates_the_pin() {
    let (fx, cfg) = encrypted_fixture();
    let export_path = fx.repo_dir.parent().unwrap().join("repokey.txt");
    fx.run_encrypted_ok(
        &[
            "--config",
            &cfg,
            "key",
            "export",
            "-o",
            export_path.to_str().unwrap(),
        ],
        KEY_PASS,
    );

    // Point the pin at an identity this repository does not have.
    let pins = pin_paths(&fx);
    assert_eq!(pins.len(), 1, "init must have pinned the identity");
    let mut pin: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&pins[0]).unwrap()).unwrap();
    pin["fingerprint"] = serde_json::Value::String("00".repeat(32));
    std::fs::write(&pins[0], serde_json::to_string_pretty(&pin).unwrap()).unwrap();

    // Without --trust-repo the import is refused, and the pin is untouched.
    let refused = fx.run_encrypted(
        &[
            "--config",
            &cfg,
            "key",
            "import",
            export_path.to_str().unwrap(),
        ],
        KEY_PASS,
    );
    assert!(!refused.status.success());
    assert!(
        stderr(&refused).contains("--trust-repo"),
        "{}",
        stderr(&refused)
    );
    let after_refusal: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&pins[0]).unwrap()).unwrap();
    assert_eq!(
        after_refusal["fingerprint"], pin["fingerprint"],
        "a refused import must leave the pin untouched"
    );

    // With --trust-repo it succeeds — and the next ordinary command must work.
    fx.run_encrypted_ok(
        &[
            "--config",
            &cfg,
            "--trust-repo",
            "key",
            "import",
            export_path.to_str().unwrap(),
        ],
        KEY_PASS,
    );
    fx.run_encrypted_ok(&["--config", &cfg, "list"], KEY_PASS);
}

/// **A key belongs to exactly one repository.** Without `-R`, `key export`
/// would concatenate armor blocks (which `import` cannot read back) or
/// overwrite one `-o` file; `key import` carries one repository's key. Refused
/// before any repository is opened — no passphrase is even asked for.
#[test]
fn cli_key_commands_refuse_multiple_repositories_without_repo_flag() {
    let fx = CliFixture::new();
    let repo_b = fx.repo_dir.parent().unwrap().join("repo-b");
    std::fs::create_dir_all(&repo_b).unwrap();
    let config = format!(
        "repositories:\n  - url: {}\n  - url: {}\nencryption:\n  mode: aes256gcm\nsources: []\n",
        yaml_quote_path(&fx.repo_dir),
        yaml_quote_path(&repo_b),
    );
    std::fs::write(&fx.config_path, config).unwrap();
    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.run_encrypted_ok(&["--config", &cfg, "init"], KEY_PASS);

    let dest = fx.repo_dir.parent().unwrap().join("all-keys.txt");
    // No passphrase in the environment: were a repository opened, the failure
    // would be about the passphrase, not about `-R`.
    for args in [
        vec!["--config", cfg.as_str(), "key", "export"],
        vec![
            "--config",
            cfg.as_str(),
            "key",
            "export",
            "-o",
            dest.to_str().unwrap(),
        ],
        vec![
            "--config",
            cfg.as_str(),
            "key",
            "import",
            dest.to_str().unwrap(),
        ],
    ] {
        let (out, err) = fx.run_err(&args);
        assert!(err.contains("requires -R"), "{args:?}: {err}");
        assert!(!err.contains("passphrase"), "refused after opening: {err}");
        assert!(!out.contains("BEGIN VYKAR"), "{args:?}: {out}");
    }
    assert!(
        !dest.exists(),
        "a refused export must not touch the destination"
    );

    // Scoped to one repository each, the keys round-trip into the right repo.
    let a = fx.repo_dir.to_str().unwrap().to_string();
    let b = repo_b.to_str().unwrap().to_string();
    let dest_a = fx.repo_dir.parent().unwrap().join("key-a.txt");
    let dest_b = fx.repo_dir.parent().unwrap().join("key-b.txt");
    for (repo, dest) in [(&a, &dest_a), (&b, &dest_b)] {
        fx.run_encrypted_ok(
            &[
                "--config",
                &cfg,
                "key",
                "export",
                "-R",
                repo,
                "-o",
                dest.to_str().unwrap(),
            ],
            KEY_PASS,
        );
        for name in ["repokey", "repokey.2"] {
            std::fs::remove_file(Path::new(repo).join("keys").join(name)).unwrap();
        }
    }
    assert_ne!(
        armor_body(&std::fs::read_to_string(&dest_a).unwrap()),
        armor_body(&std::fs::read_to_string(&dest_b).unwrap()),
    );
    for (repo, dest) in [(&a, &dest_a), (&b, &dest_b)] {
        fx.run_encrypted_ok(
            &[
                "--config",
                &cfg,
                "key",
                "import",
                "-R",
                repo,
                dest.to_str().unwrap(),
            ],
            KEY_PASS,
        );
        fx.run_encrypted_ok(&["--config", &cfg, "list", "-R", repo], KEY_PASS);
    }
}

/// Degraded key copies are reported on stderr for *every* command, not only
/// by `key export`: the default log filter is `warn`, so an ordinary `list`
/// tells the operator to run `check --repair`. Export additionally names
/// the damaged copy in its status line.
#[test]
fn cli_degraded_key_copies_warn_on_stderr() {
    let (fx, cfg) = encrypted_fixture();
    let keys = fx.repo_dir.join("keys");
    std::fs::write(keys.join("repokey.2"), b"garbage").unwrap();

    let listed = fx.run_encrypted_ok(&["--config", &cfg, "list"], KEY_PASS);
    let err = stderr(&listed);
    assert!(
        err.contains("keys/repokey.2 disagrees with keys/repokey"),
        "{err}"
    );
    assert!(err.contains("check --repair"), "{err}");

    let exported = fx.run_encrypted_ok(&["--config", &cfg, "key", "export"], KEY_PASS);
    let err = stderr(&exported);
    assert!(err.contains("keys/repokey.2 disagrees"), "{err}");
    assert!(stdout(&exported).starts_with("-----BEGIN"));

    // A missing copy is named too.
    std::fs::remove_file(keys.join("repokey.2")).unwrap();
    let exported = fx.run_encrypted_ok(&["--config", &cfg, "key", "export"], KEY_PASS);
    let err = stderr(&exported);
    assert!(err.contains("keys/repokey.2 is missing"), "{err}");
}

/// **`--trust-repo` must reach export's divergence resolution.** With a stale
/// pin and divergent copies, consulting the pin during selection rejects both
/// candidates before `verify_or_pin` ever gets to accept the new identity —
/// so `key export` would fail on a repository `list` opens fine.
#[test]
fn cli_key_export_honours_trust_repo_when_copies_diverge() {
    let (fx, cfg) = encrypted_fixture();
    let keys = fx.repo_dir.join("keys");
    let genuine = std::fs::read(keys.join("repokey")).unwrap();

    // Divergent copies: a planted foreign key as the secondary.
    let (attacker, attacker_cfg) = encrypted_fixture();
    let foreign = armor_body(&stdout(
        &attacker.run_encrypted_ok(&["--config", &attacker_cfg, "key", "export"], KEY_PASS),
    ));
    std::fs::write(keys.join("repokey.2"), &foreign).unwrap();

    // ...plus a pin that contradicts this repository's identity.
    let pins = pin_paths(&fx);
    assert_eq!(pins.len(), 1);
    let mut pin: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&pins[0]).unwrap()).unwrap();
    pin["fingerprint"] = serde_json::Value::String("00".repeat(32));
    std::fs::write(&pins[0], serde_json::to_string_pretty(&pin).unwrap()).unwrap();

    // Baseline: the stale pin blocks both, with and without divergence.
    let refused = fx.run_encrypted(&["--config", &cfg, "key", "export"], KEY_PASS);
    assert!(
        !refused.status.success(),
        "a stale pin must block the export"
    );

    // `--trust-repo` gets through — the same way `list` does.
    fx.run_encrypted_ok(&["--config", &cfg, "--trust-repo", "list"], KEY_PASS);
    let exported = armor_body(&stdout(&fx.run_encrypted_ok(
        &["--config", &cfg, "--trust-repo", "key", "export"],
        KEY_PASS,
    )));
    assert_eq!(
        exported, genuine,
        "--trust-repo must resolve the divergence, not export the planted copy"
    );
}

/// The file of the latest index head of the repository at `repo_dir`.
fn latest_head(repo_dir: &Path) -> std::path::PathBuf {
    std::fs::read_dir(repo_dir.join("index.h"))
        .unwrap()
        .flatten()
        .filter_map(|e| Some((e.file_name().to_str()?.parse::<u64>().ok()?, e.path())))
        .max_by_key(|(g, _)| *g)
        .map(|(_, p)| p)
        .expect("an index head")
}
