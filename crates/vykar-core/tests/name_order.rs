//! End-to-end: snapshot listings come out in name order (06 order 1,
//! "dfs-name") whatever filesystem the source lives on. On Linux, one source
//! sits on the system temp dir (often tmpfs) and one under the cargo target
//! dir (often ext4, where the walker stats in inode order).

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
#![allow(clippy::panic, clippy::indexing_slicing)]
// Logs the filesystem type and skipped fixture parts.
#![allow(clippy::print_stderr)]

mod common;

use std::path::Path;

use vykar_core::commands;
use vykar_core::compress::Compression;
use vykar_core::config::{CommandDump, VykarConfig};
use vykar_core::repo::control::CancelToken;

use crate::common::make_test_config;

/// 06 order 1: (parent components, name), components compared as bytes.
fn dfs_name_key(p: &[u8]) -> (Vec<&[u8]>, &[u8]) {
    let mut c: Vec<&[u8]> = p.split(|b| *b == b'/').collect();
    let name = c.pop().unwrap();
    (c, name)
}

fn assert_dfs_name_order(paths: &[Vec<u8>]) {
    for w in paths.windows(2) {
        assert!(
            dfs_name_key(&w[0]) < dfs_name_key(&w[1]),
            "not in dfs-name order: {:?} then {:?}",
            String::from_utf8_lossy(&w[0]),
            String::from_utf8_lossy(&w[1]),
        );
    }
}

/// Log the filesystem type so the test output shows whether the inode-order
/// case actually ran (ext4 is 0xef53, tmpfs 0x01021994).
fn log_fs_type(dir: &Path) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let c = std::ffi::CString::new(dir.as_os_str().as_bytes()).unwrap();
        // SAFETY: `c` is a valid NUL-terminated path and `buf` a zeroed
        // statfs that the call fills in.
        #[allow(unsafe_code)]
        let f_type = unsafe {
            let mut buf: libc::statfs = std::mem::zeroed();
            if libc::statfs(c.as_ptr(), &mut buf) != 0 {
                return;
            }
            buf.f_type
        };
        eprintln!("{}: f_type {:#x}", dir.display(), f_type);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = dir;
}

/// About 250 entries over three levels, created in reverse name order so
/// that inode order differs from name order, with names whose dfs-name order
/// differs from the byte order of the joined path (`a.b` vs `a/x`).
///
/// The mixed-case pair `b`/`B` is only created where the filesystem
/// distinguishes case (not on default APFS or NTFS); returns whether it was.
/// `C` and `Zeta` keep an uppercase-before-lowercase case everywhere.
fn make_tree(root: &Path) -> bool {
    std::fs::create_dir_all(root).unwrap();
    let names = [
        "z", "é", "b", "B", "C", "ab", "a b", "a-b", "a.b", "a", "Zeta", "0", "_x",
    ];
    let mut mixed_case = true;
    for top in names {
        let d = root.join(top);
        if top == "B" && d.symlink_metadata().is_ok() {
            // `b` already exists under this name: case-insensitive.
            eprintln!("{}: case-insensitive; skipping `B`", root.display());
            mixed_case = false;
            continue;
        }
        std::fs::create_dir(&d).unwrap();
        for mid in (0..4).rev() {
            let m = d.join(format!("m{mid}"));
            std::fs::create_dir(&m).unwrap();
            for f in (0..3).rev() {
                std::fs::write(m.join(format!("f{f}")), "x").unwrap();
            }
        }
        std::fs::write(root.join(format!("{top}.txt")), "x").unwrap();
    }
    for f in (0..40).rev() {
        std::fs::write(root.join(format!("file{f:02}")), "x").unwrap();
    }
    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let raw_dir = root.join(OsStr::from_bytes(b"n\x80"));
        match std::fs::create_dir(&raw_dir) {
            Ok(()) => {
                std::fs::write(raw_dir.join("child"), "x").unwrap();
                std::fs::write(root.join(OsStr::from_bytes(b"\xff")), "x").unwrap();
            }
            // Linux filesystems take any name; only elsewhere (APFS) is a
            // refusal a reason to skip. Same rule as vykar-core's
            // testutil::try_raw_name.
            Err(e) if !cfg!(target_os = "linux") && e.raw_os_error() == Some(libc::EILSEQ) => {
                eprintln!("filesystem refuses non-UTF-8 names; skipping those entries");
            }
            Err(e) => panic!("creating {}: {e}", raw_dir.display()),
        }
        std::fs::write(root.join("né"), "x").unwrap();
        std::fs::write(root.join("\u{10000}"), "x").unwrap();
    }
    mixed_case
}

fn backup(
    config: &VykarConfig,
    source_paths: &[String],
    label: &str,
    snapshot: &str,
    dumps: &[CommandDump],
) {
    commands::backup::run(
        config,
        commands::backup::BackupRequest {
            snapshot_name: snapshot,
            passphrase: Some(common::TEST_PASSPHRASE),
            source_paths,
            source_label: label,
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: dumps,
            verbose: false,
        },
    )
    .unwrap();
}

fn listed_paths(config: &VykarConfig, snapshot: &str) -> Vec<Vec<u8>> {
    commands::list::list_snapshot_items(config, common::pass(config), snapshot)
        .unwrap()
        .iter()
        .map(|i| i.path_bytes().to_vec())
        .collect()
}

fn init_repo(repo_dir: &Path) -> VykarConfig {
    let config = make_test_config(repo_dir);
    common::init_repo(&config);
    config
}

fn path_string(p: &Path) -> String {
    p.to_string_lossy().to_string()
}

#[test]
fn ext4_and_tmpfs_sources_list_in_the_same_order() {
    let repo = tempfile::tempdir().unwrap();
    let config = init_repo(repo.path());

    let tmp = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    // Same basename: a single-path directory source stores paths relative
    // to the root, so the two listings are directly comparable.
    let tmp_src = tmp.path().join("src");
    let target_src = target.path().join("src");
    let tmp_mixed = make_tree(&tmp_src);
    let target_mixed = make_tree(&target_src);
    // Linux filesystems distinguish case, so the inode-order case on ext4
    // also covers the `B` < `a` < `b` siblings.
    #[cfg(target_os = "linux")]
    assert!(tmp_mixed && target_mixed, "mixed-case pair not created");
    let _ = (tmp_mixed, target_mixed);
    log_fs_type(&tmp_src);
    log_fs_type(&target_src);

    backup(&config, &[path_string(&tmp_src)], "tmp", "s-tmp", &[]);
    backup(
        &config,
        &[path_string(&target_src)],
        "target",
        "s-target",
        &[],
    );

    let a = listed_paths(&config, "s-tmp");
    let b = listed_paths(&config, "s-target");
    assert!(a.len() > 200, "fixture too small: {}", a.len());
    assert_dfs_name_order(&a);
    assert_dfs_name_order(&b);
    assert_eq!(a, b);
}

#[test]
fn multi_path_source_lists_each_root_in_name_order() {
    let repo = tempfile::tempdir().unwrap();
    let config = init_repo(repo.path());

    let tmp = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let roots = [tmp.path().join("one"), target.path().join("two")];
    for r in &roots {
        make_tree(r);
        log_fs_type(r);
    }
    let paths: Vec<String> = roots.iter().map(|r| path_string(r)).collect();
    backup(&config, &paths, "multi", "s-multi", &[]);

    let listed = listed_paths(&config, "s-multi");
    let mut relative: Vec<Vec<Vec<u8>>> = Vec::new();
    let mut rest = listed.as_slice();
    for r in &roots {
        // The EmitRoot prefix is platform-specific (`C/Users/...` on
        // Windows), so take it from the root entry, which comes first.
        let prefix = rest[0].clone();
        let mut root_suffix = b"/".to_vec();
        root_suffix.extend_from_slice(r.file_name().unwrap().as_encoded_bytes());
        assert!(
            prefix.ends_with(&root_suffix),
            "root entry must come first for its root: {:?}",
            String::from_utf8_lossy(&prefix),
        );
        let mut with_slash = prefix.clone();
        with_slash.push(b'/');
        let n = rest[1..]
            .iter()
            .take_while(|p| p.starts_with(&with_slash))
            .count();
        let root_paths = &rest[..=n];
        assert_dfs_name_order(root_paths);
        relative.push(
            root_paths[1..]
                .iter()
                .map(|p| p[with_slash.len()..].to_vec())
                .collect(),
        );
        rest = &rest[n + 1..];
    }
    assert!(rest.is_empty(), "items outside the two roots");
    assert!(relative[0].len() > 200);
    assert_eq!(relative[0], relative[1]);
}

#[cfg(unix)]
#[test]
fn dumps_list_in_name_order_and_run_in_config_order() {
    let repo = tempfile::tempdir().unwrap();
    let config = init_repo(repo.path());

    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("f"), "x").unwrap();
    let marker = path_string(&tmp.path().join("marker"));

    let dumps = vec![
        CommandDump {
            name: "zeta".to_string(),
            command: format!("echo z > '{marker}'; echo zeta"),
        },
        CommandDump {
            name: "alpha".to_string(),
            command: format!("cat '{marker}'"),
        },
    ];
    backup(&config, &[path_string(&src)], "dumps", "s-dumps", &dumps);

    let listed = listed_paths(&config, "s-dumps");
    assert_eq!(
        &listed[..3],
        &[
            b"vykar-dumps".to_vec(),
            b"vykar-dumps/alpha".to_vec(),
            b"vykar-dumps/zeta".to_vec(),
        ]
    );

    let dest = tempfile::tempdir().unwrap();
    commands::restore::run(
        &config,
        common::pass(&config),
        "s-dumps",
        dest.path().to_str().unwrap(),
        None,
        false,
        false,
        &CancelToken::new(),
    )
    .unwrap();
    // zeta ran first: alpha read the marker it wrote.
    let alpha = std::fs::read_to_string(dest.path().join("vykar-dumps/alpha")).unwrap();
    assert_eq!(alpha, "z\n");
}
