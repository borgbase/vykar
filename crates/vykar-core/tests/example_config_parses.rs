#![allow(clippy::unwrap_used, clippy::panic)]
//! The shipped `vykar.example.yaml` must actually parse.
//!
//! Users copy this file to start from, so a config that no longer loads is a
//! first-run failure. It also guards the YAML scalar-resolution rules in
//! `config::yaml` against a parser swap: the example exercises real-world
//! shapes (nested source/repository entries, durations, retention) that the
//! focused unit tests do not.

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

#[test]
fn shipped_example_config_parses() {
    let source = repo_root().join("vykar.example.yaml");
    assert!(source.is_file(), "missing {}", source.display());
    let body = std::fs::read_to_string(&source).unwrap();

    // The example carries `${DB_USER}`-style placeholders inside commented-out
    // blocks. Env expansion runs before YAML parsing, so those are still
    // expanded and the file does not load with them unset. Supply them via an
    // `env_file` overlay so this test covers what it means to — that the
    // example's actual YAML parses and resolves.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join(".env"),
        "DB_USER=u\nDB_PASSWORD=p\nDB_DATABASE=d\n",
    )
    .unwrap();
    let path = dir.path().join("vykar.yaml");
    std::fs::write(&path, format!("env_file: .env\n{body}")).unwrap();

    let repos = vykar_core::config::load_and_resolve(&path)
        .unwrap_or_else(|e| panic!("vykar.example.yaml failed to parse: {e}"));
    assert!(
        !repos.is_empty(),
        "example config should resolve at least one repository"
    );
}

/// YAML 1.1 boolean words must stay strings, and `env_file` must be consumed
/// by the pre-parse pass before environment expansion. Both are properties of
/// `config::yaml`, exercised here through the real file-loading entry point
/// rather than a hand-built deserializer.
#[test]
fn yaml11_bool_words_and_env_file_survive_a_real_load() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(".env"), "VYKAR_TEST_LABEL=from-env\n").unwrap();

    let config = dir.path().join("vykar.yaml");
    std::fs::write(
        &config,
        r#"
env_file: .env
sources:
  - /home/user
repositories:
  - label: no
    url: /backups/one
  - label: "off"
    url: /backups/two
"#,
    )
    .unwrap();

    let repos = vykar_core::config::load_and_resolve(&config)
        .unwrap_or_else(|e| panic!("config with YAML 1.1 bool words failed to parse: {e}"));

    let labels: Vec<_> = repos.iter().filter_map(|r| r.label.as_deref()).collect();
    assert_eq!(
        labels,
        vec!["no", "off"],
        "`no`/`off` must stay strings, not become booleans"
    );
}
