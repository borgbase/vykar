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

/// Loads the file exactly as shipped, with no environment prepared. Users copy
/// it verbatim, so that is the property worth testing.
///
/// This also guards a trap the file documents but once fell into itself: env
/// expansion rewrites the whole file, comments included, and there is no escape
/// syntax, so a literal `${VAR}` in a commented-out example makes the config
/// fail to load whenever that variable is unset.
#[test]
fn shipped_example_config_parses() {
    let path = repo_root().join("vykar.example.yaml");
    assert!(path.is_file(), "missing {}", path.display());

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

#[test]
fn yaml_errors_do_not_print_neighboring_secrets() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("vykar.yaml");
    let secret = "SYNTHETIC_SECRET_FOR_YAML_REGRESSION";
    std::fs::write(
        dir.path().join(".env"),
        format!("VYKAR_YAML_REVIEW_SECRET={secret}\n"),
    )
    .unwrap();

    for (secret_value, invalid_setting) in [
        // Syntax error in the first pass, next to a literal secret.
        (secret, "s3_soft_delete: ["),
        // Type and unknown-field errors after environment expansion.
        ("${VYKAR_YAML_REVIEW_SECRET}", "s3_soft_delete: typo"),
        ("${VYKAR_YAML_REVIEW_SECRET}", "unknown_setting: true"),
    ] {
        std::fs::write(&config, format!(
            "env_file: .env\nsources: [/tmp/source]\nrepositories:\n  - url: /tmp/repo\n    secret_access_key: {secret_value}\n    {invalid_setting}\n"
        )).unwrap();
        let error = vykar_core::config::load_and_resolve(&config)
            .unwrap_err()
            .to_string();
        assert!(
            !error.contains(secret),
            "error leaked a credential: {error}"
        );
        assert!(
            error.contains(config.to_str().unwrap()),
            "missing config path: {error}"
        );
        assert!(
            error.contains("line") && error.contains("column"),
            "missing error location: {error}"
        );
    }
}

#[test]
fn config_load_preserves_legacy_odd_chunker_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("vykar.yaml");
    std::fs::write(&config,
        "sources: [/tmp/source]\nrepositories: [{url: /tmp/repo}]\nchunker: {min_size: 257, avg_size: 1025, max_size: 4095}\n"
    ).unwrap();
    let repos = vykar_core::config::load_and_resolve(&config).unwrap();
    let chunker = &repos.first().unwrap().config.chunker;
    assert_eq!(
        (chunker.min_size, chunker.avg_size, chunker.max_size),
        (257, 1025, 4095)
    );
}
