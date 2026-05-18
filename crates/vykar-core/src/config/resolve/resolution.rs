use std::path::{Path, PathBuf};

use super::super::hooks::HooksConfig;
use super::super::sources::{normalize_sources, SourceEntry};
use super::super::types::*;
use super::super::util::expand_tilde;
use super::document::{ConfigDocument, EnvFilePre};
use super::env::{expand_env_placeholders, parse_env_files};

/// A fully resolved repository with its merged config.
#[derive(Debug, Clone)]
pub struct ResolvedRepo {
    pub label: Option<String>,
    pub config: VykarConfig,
    pub global_hooks: HooksConfig,
    pub repo_hooks: HooksConfig,
    pub sources: Vec<SourceEntry>,
}

/// Load and resolve a config file into one `ResolvedRepo` per repository entry.
pub fn load_and_resolve(path: &Path) -> vykar_types::error::Result<Vec<ResolvedRepo>> {
    let contents = std::fs::read_to_string(path).map_err(|e| {
        vykar_types::error::VykarError::Config(format!("cannot read '{}': {e}", path.display()))
    })?;

    // Pre-parse to extract env_file paths before environment variable expansion.
    let pre: EnvFilePre = serde_yaml::from_str(&contents).map_err(|e| {
        vykar_types::error::VykarError::Config(format!("invalid config '{}': {e}", path.display()))
    })?;

    // Parse env files into an overlay map (does not mutate process env).
    let base_dir = path.parent().unwrap_or(Path::new("."));
    let env_paths: Vec<PathBuf> = pre
        .env_file
        .iter()
        .map(|s| base_dir.join(expand_tilde(s)))
        .collect();
    let env_overlay = parse_env_files(path, &env_paths)?;

    let expanded = expand_env_placeholders(&contents, path, &env_overlay)?;
    let raw: ConfigDocument = serde_yaml::from_str(&expanded).map_err(|e| {
        vykar_types::error::VykarError::Config(format!("invalid config '{}': {e}", path.display()))
    })?;

    resolve_document(raw)
}

pub(super) fn resolve_document(
    mut raw: ConfigDocument,
) -> vykar_types::error::Result<Vec<ResolvedRepo>> {
    if raw.repositories.is_empty() {
        return Ok(Vec::new());
    }

    // Check for duplicate repo labels
    let mut seen = std::collections::HashSet::new();
    for label in raw.repositories.iter().filter_map(|e| e.label.as_deref()) {
        if !seen.insert(label) {
            return Err(vykar_types::error::VykarError::Config(format!(
                "duplicate repository label: '{label}'"
            )));
        }
    }

    // Validate global hooks, schedule, and chunker params
    raw.hooks.validate()?;
    raw.schedule.validate()?;
    raw.limits.validate()?;
    raw.chunker.validate()?;
    raw.compact.validate();
    raw.check.validate()?;

    // Validate per-repo hooks
    for entry in &raw.repositories {
        if let Some(ref h) = entry.hooks {
            h.validate()?;
        }
        if let Some(ref limits) = entry.limits {
            limits.validate()?;
        }
    }

    // Normalize sources
    let all_sources: Vec<SourceEntry> = normalize_sources(
        raw.sources,
        &raw.exclude_if_present,
        raw.one_file_system,
        raw.git_ignore,
        raw.xattrs.enabled,
    )?;

    // Check for duplicate source labels
    let mut source_labels = std::collections::HashSet::new();
    for src in &all_sources {
        if !source_labels.insert(&src.label) {
            return Err(vykar_types::error::VykarError::Config(format!(
                "duplicate source label: '{}'",
                src.label
            )));
        }
    }

    // Validate that source `repos` references exist
    let repo_labels: std::collections::HashSet<&str> = raw
        .repositories
        .iter()
        .filter_map(|e| e.label.as_deref())
        .collect();
    for src in &all_sources {
        for repo_ref in &src.repos {
            if !repo_labels.contains(repo_ref.as_str()) {
                return Err(vykar_types::error::VykarError::Config(format!(
                    "source '{}' references unknown repository '{repo_ref}'",
                    src.label
                )));
            }
        }
    }

    let repos = raw
        .repositories
        .into_iter()
        .map(|entry| {
            let entry_label = entry.label.clone();
            let repo_hooks = entry.hooks.clone().unwrap_or_default();

            // Filter sources for this repo: include sources whose `repos` is empty
            // (meaning all repos) or whose `repos` list contains this repo's label.
            let sources_for_repo: Vec<SourceEntry> = all_sources
                .iter()
                .filter(|src| {
                    src.repos.is_empty()
                        || entry_label
                            .as_deref()
                            .is_some_and(|l| src.repos.iter().any(|r| r == l))
                })
                .map(|src| {
                    // Merge exclude patterns: global + per-source
                    let mut merged_exclude = raw.exclude_patterns.clone();
                    merged_exclude.extend(src.exclude.clone());
                    SourceEntry {
                        paths: src.paths.clone(),
                        label: src.label.clone(),
                        exclude: merged_exclude,
                        exclude_if_present: src.exclude_if_present.clone(),
                        one_file_system: src.one_file_system,
                        git_ignore: src.git_ignore,
                        xattrs_enabled: src.xattrs_enabled,
                        hooks: src.hooks.clone(),
                        retention: src.retention.clone(),
                        repos: src.repos.clone(),
                        command_dumps: src.command_dumps.clone(),
                    }
                })
                .collect();

            ResolvedRepo {
                label: entry_label,
                config: VykarConfig {
                    repository: entry.to_repo_config(),
                    encryption: entry.encryption.unwrap_or_else(|| raw.encryption.clone()),
                    exclude_patterns: raw.exclude_patterns.clone(),
                    exclude_if_present: raw.exclude_if_present.clone(),
                    one_file_system: raw.one_file_system,
                    git_ignore: raw.git_ignore,
                    chunker: raw.chunker.clone(),
                    compression: entry.compression.unwrap_or_else(|| raw.compression.clone()),
                    retention: entry.retention.unwrap_or_else(|| raw.retention.clone()),
                    xattrs: raw.xattrs.clone(),
                    schedule: raw.schedule.clone(),
                    limits: entry.limits.unwrap_or_else(|| raw.limits.clone()),
                    compact: raw.compact.clone(),
                    check: raw.check.clone(),
                    cache_dir: raw
                        .cache_dir
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(expand_tilde),
                    trust_repo: false,
                    hostname_override: raw
                        .hostname
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(String::from),
                },
                global_hooks: raw.hooks.clone(),
                repo_hooks,
                sources: sources_for_repo,
            }
        })
        .collect();

    Ok(repos)
}

/// Select a repository by label or URL from a list of resolved repos.
pub fn select_repo<'a>(repos: &'a [ResolvedRepo], selector: &str) -> Option<&'a ResolvedRepo> {
    // Try label match first
    repos
        .iter()
        .find(|r| r.label.as_deref() == Some(selector))
        .or_else(|| {
            // Fall back to URL match
            repos.iter().find(|r| r.config.repository.url == selector)
        })
}

/// Select sources by label from a list of source entries.
///
/// Returns an error with available source labels if any selector doesn't match.
pub fn select_sources<'a>(
    sources: &'a [SourceEntry],
    selectors: &[String],
) -> std::result::Result<Vec<&'a SourceEntry>, String> {
    let mut result = Vec::new();
    for sel in selectors {
        match sources.iter().find(|s| s.label == *sel) {
            Some(s) => result.push(s),
            None => {
                let available: Vec<&str> = sources.iter().map(|s| s.label.as_str()).collect();
                return Err(format!(
                    "no source matching '{sel}'\nAvailable sources: {}",
                    available.join(", ")
                ));
            }
        }
    }
    Ok(result)
}

/// Load and parse a config file. Returns the first repository's config.
#[deprecated(
    note = "prefer load_and_resolve() and explicit repository selection for multi-repo configs"
)]
pub fn load_config(path: &Path) -> vykar_types::error::Result<VykarConfig> {
    let repos = load_and_resolve(path)?;
    repos
        .into_iter()
        .next()
        .map(|r| r.config)
        .ok_or_else(|| vykar_types::error::VykarError::Config("no repositories configured".into()))
}

#[cfg(test)]
mod tests {
    use super::super::super::defaults::*;
    use super::super::test_support::{make_test_repo, make_test_source};
    use super::*;
    use std::fs;
    use vykar_common::paths;

    #[test]
    #[allow(deprecated)]
    fn test_load_config_missing_file() {
        let result = load_config(Path::new("/nonexistent/path/config.yaml"));
        assert!(result.is_err());
    }

    #[test]
    #[allow(deprecated)]
    fn test_load_config_empty_repos() {
        let yaml = "repositories: []\nencryption:\n  mode: none\n";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_config(&path).unwrap_err();
        assert!(
            err.to_string().contains("no repositories configured"),
            "expected 'no repositories configured' error, got: {err}"
        );
    }

    #[test]
    fn test_single_repo() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    label: main
encryption:
  mode: none
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].label.as_deref(), Some("main"));
        assert_eq!(repos[0].config.repository.url, "/tmp/repo");
        assert_eq!(repos[0].config.encryption.mode, "none");
        assert_eq!(repos[0].sources.len(), 1);
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user"]);
        assert_eq!(repos[0].sources[0].label, "user");
    }

    #[test]
    fn test_multi_repo_basic() {
        let yaml = r#"
encryption:
  mode: aes256gcm
sources:
  - /home/user
compression:
  algorithm: lz4
retention:
  keep_daily: 7

repositories:
  - url: /backups/local
    label: local
  - url: /backups/remote
    label: remote
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 2);

        assert_eq!(repos[0].label.as_deref(), Some("local"));
        assert_eq!(repos[0].config.repository.url, "/backups/local");
        // Inherits top-level defaults
        assert_eq!(repos[0].config.encryption.mode, "aes256gcm");
        assert_eq!(repos[0].config.compression.algorithm, "lz4");
        assert_eq!(repos[0].config.retention.keep_daily, Some(7));
        assert_eq!(repos[0].sources.len(), 1);
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user"]);

        assert_eq!(repos[1].label.as_deref(), Some("remote"));
        assert_eq!(repos[1].config.repository.url, "/backups/remote");
        assert_eq!(repos[1].sources.len(), 1);
    }

    #[test]
    fn test_cache_dir_empty_string_resolves_to_none() {
        let yaml = r#"
cache_dir: ""
repositories:
  - url: /tmp/repo
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.cache_dir, None);
    }

    #[test]
    fn test_cache_dir_whitespace_resolves_to_none() {
        let yaml = r#"
cache_dir: "   "
repositories:
  - url: /tmp/repo
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.cache_dir, None);
    }

    #[test]
    fn test_cache_dir_tilde_expands_when_nonempty() {
        let yaml = r#"
cache_dir: "~/vykar-cache"
repositories:
  - url: /tmp/repo
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let home = paths::home_dir().expect("home dir should be available in test env");
        let expected = home.join("vykar-cache").to_string_lossy().to_string();
        assert_eq!(
            repos[0].config.cache_dir.as_deref(),
            Some(expected.as_str())
        );
    }

    #[test]
    fn test_sftp_repository_options_parse() {
        let yaml = r#"
repositories:
  - url: sftp://backup@nas.local/backups/vykar
    sftp_key: /tmp/id_ed25519
    sftp_known_hosts: /tmp/known_hosts
    sftp_timeout: 60
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 1);
        let repo = &repos[0].config.repository;
        assert_eq!(
            repo.sftp_key.as_deref(),
            Some("/tmp/id_ed25519"),
            "sftp_key should be parsed"
        );
        assert_eq!(
            repo.sftp_known_hosts.as_deref(),
            Some("/tmp/known_hosts"),
            "sftp_known_hosts should be parsed"
        );
        assert_eq!(repo.sftp_timeout, Some(60), "sftp_timeout should be parsed");
    }

    #[test]
    fn test_encryption_mode_defaults_to_auto_when_omitted() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.encryption.mode, "auto");
    }

    #[test]
    fn test_encryption_mode_chacha20poly1305_parses() {
        let yaml = r#"
encryption:
  mode: chacha20poly1305
repositories:
  - url: /tmp/repo
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.encryption.mode, "chacha20poly1305");
    }

    #[test]
    fn test_multi_repo_overrides() {
        let yaml = r#"
encryption:
  mode: aes256gcm
sources:
  - /home/user
compression:
  algorithm: lz4
retention:
  keep_daily: 7

repositories:
  - url: /backups/local
    label: local
  - url: /backups/remote
    label: remote
    encryption:
      mode: aes256gcm
      passcommand: "pass show vykar-remote"
    compression:
      algorithm: zstd
    retention:
      keep_daily: 30
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 2);

        // First repo uses defaults
        let local = &repos[0];
        assert_eq!(local.config.compression.algorithm, "lz4");
        assert_eq!(local.config.retention.keep_daily, Some(7));
        assert_eq!(local.sources.len(), 1);
        assert_eq!(local.sources[0].paths, vec!["/home/user"]);

        // Second repo uses overrides
        let remote = &repos[1];
        assert_eq!(remote.config.compression.algorithm, "zstd");
        assert_eq!(remote.config.retention.keep_daily, Some(30));
        assert_eq!(
            remote.config.encryption.passcommand.as_deref(),
            Some("pass show vykar-remote")
        );
        assert_eq!(remote.sources.len(), 1);
    }

    #[test]
    fn test_limits_inherit_and_repo_override() {
        let yaml = r#"
limits:
  threads: 4
  nice: 5
  connections: 3
  upload_mib_per_sec: 50
  download_mib_per_sec: 80

repositories:
  - url: /backups/local
    label: local
  - url: /backups/remote
    label: remote
    limits:
      threads: 2
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 2);

        let local = &repos[0].config.limits;
        assert_eq!(local.threads, 4);
        assert_eq!(local.nice, 5);
        assert_eq!(local.connections, 3);
        assert_eq!(local.upload_mib_per_sec, 50);
        assert_eq!(local.download_mib_per_sec, 80);

        let remote = &repos[1].config.limits;
        assert_eq!(remote.threads, 2);
        assert_eq!(remote.nice, 0);
        assert_eq!(remote.connections, 2); // default
        assert_eq!(remote.upload_mib_per_sec, 0);
        assert_eq!(remote.download_mib_per_sec, 0);
    }

    #[test]
    fn test_limits_invalid_nice_rejected() {
        let yaml = r#"
limits:
  nice: 25
repositories:
  - url: /backups/local
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        assert!(
            err.to_string().contains("limits.nice"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_multi_repo_pack_size_defaults() {
        let yaml = r#"
repositories:
  - url: /backups/a
    min_pack_size: 1048576
  - url: /backups/b
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.min_pack_size, 1048576);
        assert_eq!(
            repos[1].config.repository.min_pack_size,
            default_min_pack_size()
        );
        assert_eq!(
            repos[1].config.repository.max_pack_size,
            default_max_pack_size()
        );
    }

    #[test]
    fn test_missing_repositories_returns_empty_vec() {
        let yaml = r#"
encryption:
  mode: none
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let result = load_and_resolve(&path).unwrap();
        assert!(
            result.is_empty(),
            "expected empty vec when repositories key is missing"
        );
    }

    #[test]
    fn test_empty_repositories_returns_empty_vec() {
        let yaml = r#"
repositories: []
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let result = load_and_resolve(&path).unwrap();
        assert!(result.is_empty(), "expected empty vec for repositories: []");
    }

    #[test]
    fn test_reject_duplicate_labels() {
        let yaml = r#"
repositories:
  - url: /backups/a
    label: same
  - url: /backups/b
    label: same
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate repository label"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_select_repo_by_label() {
        let repos = vec![
            make_test_repo("/backups/local", Some("local")),
            make_test_repo("/backups/remote", Some("remote")),
        ];

        let found = select_repo(&repos, "remote").unwrap();
        assert_eq!(found.config.repository.url, "/backups/remote");
    }

    #[test]
    fn test_select_repo_by_url() {
        let repos = vec![
            make_test_repo("/backups/local", Some("local")),
            make_test_repo("/backups/unlabeled", None),
        ];

        let found = select_repo(&repos, "/backups/unlabeled").unwrap();
        assert!(found.label.is_none());
        assert_eq!(found.config.repository.url, "/backups/unlabeled");
    }

    #[test]
    fn test_select_repo_no_match() {
        let repos = vec![make_test_repo("/backups/local", Some("local"))];

        assert!(select_repo(&repos, "nonexistent").is_none());
    }

    #[test]
    #[allow(deprecated)]
    fn test_load_config_returns_first_repo() {
        let yaml = r#"
repositories:
  - url: /tmp/first
  - url: /tmp/second
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let config = load_config(&path).unwrap();
        assert_eq!(config.repository.url, "/tmp/first");
    }

    #[test]
    fn test_select_sources_by_label() {
        let sources = vec![
            make_test_source("docs"),
            make_test_source("photos"),
            make_test_source("music"),
        ];

        let result = select_sources(&sources, &["photos".into()]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].label, "photos");
    }

    #[test]
    fn test_select_sources_multiple() {
        let sources = vec![
            make_test_source("docs"),
            make_test_source("photos"),
            make_test_source("music"),
        ];

        let result = select_sources(&sources, &["docs".into(), "music".into()]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].label, "docs");
        assert_eq!(result[1].label, "music");
    }

    #[test]
    fn test_select_sources_no_match() {
        let sources = vec![make_test_source("docs"), make_test_source("photos")];

        let err = select_sources(&sources, &["nonexistent".into()]).unwrap_err();
        assert!(
            err.contains("no source matching 'nonexistent'"),
            "unexpected: {err}"
        );
        assert!(err.contains("docs"), "should list available sources: {err}");
        assert!(
            err.contains("photos"),
            "should list available sources: {err}"
        );
    }

    #[test]
    fn test_select_sources_empty_selectors() {
        let sources = vec![make_test_source("docs")];

        let result = select_sources(&sources, &[]).unwrap();
        assert!(result.is_empty());
    }
}

#[cfg(test)]
mod merge_behavior_tests {
    use super::super::super::deserialize::STRICT_STRING_ERROR;
    use super::*;
    use std::fs;
    use vykar_common::paths;

    #[test]
    fn test_hooks_deserialize() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
hooks:
  before_backup:
    - "pg_dump mydb > /tmp/db.sql"
  finally_backup:
    - "rm -f /tmp/db.sql"
  after:
    - "echo done"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].global_hooks.get_hooks("before_backup").len(), 1);
        assert_eq!(repos[0].global_hooks.get_hooks("finally_backup").len(), 1);
        assert_eq!(repos[0].global_hooks.get_hooks("after").len(), 1);
        assert!(repos[0].global_hooks.get_hooks("before").is_empty());
    }

    #[test]
    fn test_hooks_default_empty() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert!(repos[0].global_hooks.is_empty());
        assert!(repos[0].repo_hooks.is_empty());
    }

    #[test]
    fn test_hooks_per_repo() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    label: main
    hooks:
      before:
        - "mount /mnt/nas"
      finally:
        - "umount /mnt/nas"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert!(repos[0].global_hooks.is_empty());
        assert_eq!(repos[0].repo_hooks.get_hooks("before").len(), 1);
        assert_eq!(repos[0].repo_hooks.get_hooks("finally").len(), 1);
    }

    #[test]
    fn test_hooks_validation_rejects_bad_keys() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
hooks:
  before_invalid_command:
    - "echo nope"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid hook key"), "unexpected error: {msg}");
    }

    #[test]
    fn test_hooks_rejects_bool_in_command_list() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
hooks:
  before_backup:
    - true
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        assert!(
            err.to_string().contains(STRICT_STRING_ERROR),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_hooks_validation_rejects_non_automation_command_keys() {
        let dir = tempfile::tempdir().unwrap();

        for key in ["before_info", "after_init", "failed_run"] {
            let yaml = format!(
                r#"
repositories:
  - url: /tmp/repo
hooks:
  {key}:
    - "echo nope"
"#
            );
            let path = dir.path().join(format!("{key}.yaml"));
            fs::write(&path, yaml).unwrap();

            let err = load_and_resolve(&path).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("invalid hook key"), "unexpected error: {msg}");
            assert!(msg.contains(key), "error should mention key {key}: {msg}");
        }
    }

    #[test]
    fn test_hooks_validation_rejects_bad_repo_keys() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    hooks:
      on_start:
        - "echo nope"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid hook key"), "unexpected error: {msg}");
    }

    #[test]
    fn test_hooks_string_and_list_global_and_repo() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    hooks:
      before: "echo repo-before"
      after:
        - "echo repo-after-1"
        - "echo repo-after-2"
hooks:
  before_backup: "pg_dump mydb > /tmp/db.sql"
  after:
    - "echo done"
  failed: "curl -fsS -m 10 https://hc-ping.com/uuid/fail"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();

        // Global hooks: string and list both work
        assert_eq!(
            repos[0].global_hooks.get_hooks("before_backup"),
            &["pg_dump mydb > /tmp/db.sql"]
        );
        assert_eq!(repos[0].global_hooks.get_hooks("after"), &["echo done"]);
        assert_eq!(
            repos[0].global_hooks.get_hooks("failed"),
            &["curl -fsS -m 10 https://hc-ping.com/uuid/fail"]
        );

        // Per-repo hooks: string and list both work
        assert_eq!(
            repos[0].repo_hooks.get_hooks("before"),
            &["echo repo-before"]
        );
        assert_eq!(
            repos[0].repo_hooks.get_hooks("after"),
            &["echo repo-after-1", "echo repo-after-2"]
        );
    }

    #[test]
    fn test_hooks_rejects_bool_as_string_value() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
hooks:
  before_backup: true
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(STRICT_STRING_ERROR), "unexpected error: {msg}");
    }

    #[test]
    fn test_sources_simple_single() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user/documents
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources.len(), 1);
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user/documents"]);
        assert_eq!(repos[0].sources[0].label, "documents");
    }

    #[test]
    fn test_sources_simple_multiple_grouped() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user/documents
  - /home/user/photos
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        // Multiple simple sources are grouped into a single entry
        assert_eq!(repos[0].sources.len(), 1);
        assert_eq!(
            repos[0].sources[0].paths,
            vec!["/home/user/documents", "/home/user/photos"]
        );
        assert_eq!(repos[0].sources[0].label, "default");
    }

    #[test]
    fn test_sources_rich() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    label: main
sources:
  - path: /home/user/documents
    label: docs
    exclude:
      - "*.tmp"
    retention:
      keep_daily: 14
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources.len(), 1);
        let src = &repos[0].sources[0];
        assert_eq!(src.paths, vec!["/home/user/documents"]);
        assert_eq!(src.label, "docs");
        assert_eq!(src.exclude, vec!["*.tmp"]);
        assert_eq!(src.retention.as_ref().unwrap().keep_daily, Some(14));
    }

    #[test]
    fn test_sources_mixed_simple_and_rich() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user/photos
  - path: /home/user/documents
    label: docs
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources.len(), 2);
        // Simple entries come first (grouped), then rich entries
        assert_eq!(repos[0].sources[0].label, "photos");
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user/photos"]);
        assert_eq!(repos[0].sources[1].label, "docs");
        assert_eq!(repos[0].sources[1].paths, vec!["/home/user/documents"]);
    }

    #[test]
    fn test_sources_repo_targeting() {
        let yaml = r#"
repositories:
  - url: /backups/local
    label: local
  - url: /backups/remote
    label: remote

sources:
  - path: /home/user/documents
    label: docs
    repos:
      - local
  - path: /home/user/photos
    label: photos
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();

        // local repo should have both (docs targets local, photos targets all)
        let local = &repos[0];
        assert_eq!(local.sources.len(), 2);

        // remote repo should only have photos (docs targets local only)
        let remote = &repos[1];
        assert_eq!(remote.sources.len(), 1);
        assert_eq!(remote.sources[0].label, "photos");
    }

    #[test]
    fn test_sources_exclude_merge() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
exclude_patterns:
  - "*.cache"
sources:
  - path: /home/user/documents
    exclude:
      - "*.tmp"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let src = &repos[0].sources[0];
        // Global + per-source excludes should be merged
        assert_eq!(src.exclude, vec!["*.cache", "*.tmp"]);
    }

    #[test]
    fn test_exclusion_feature_defaults() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user/documents
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert!(repos[0].config.exclude_if_present.is_empty());
        assert!(!repos[0].config.one_file_system);
        assert!(!repos[0].config.git_ignore);
        assert!(repos[0].config.xattrs.enabled);

        let src = &repos[0].sources[0];
        assert!(src.exclude_if_present.is_empty());
        assert!(!src.one_file_system);
        assert!(!src.git_ignore);
        assert!(src.xattrs_enabled);
    }

    #[test]
    fn test_source_exclusion_feature_overrides() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
exclude_if_present:
  - .nobackup
  - CACHEDIR.TAG
one_file_system: true
git_ignore: false
xattrs:
  enabled: false
sources:
  - path: /home/user/documents
    label: docs
    exclude_if_present:
      - .skip
    one_file_system: false
    git_ignore: true
    xattrs:
      enabled: true
  - path: /home/user/photos
    label: photos
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let docs = repos[0].sources.iter().find(|s| s.label == "docs").unwrap();
        let photos = repos[0]
            .sources
            .iter()
            .find(|s| s.label == "photos")
            .unwrap();

        // Per-source marker list replaces global markers when set.
        assert_eq!(docs.exclude_if_present, vec![".skip"]);
        assert!(!docs.one_file_system);
        assert!(docs.git_ignore);
        assert!(docs.xattrs_enabled);

        // Sources without overrides inherit global defaults.
        assert_eq!(photos.exclude_if_present, vec![".nobackup", "CACHEDIR.TAG"]);
        assert!(photos.one_file_system);
        assert!(!photos.git_ignore);
        assert!(!photos.xattrs_enabled);
    }

    #[test]
    fn test_sources_reject_duplicate_labels() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /a
    label: same
  - path: /b
    label: same
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("duplicate source label"), "unexpected: {msg}");
    }

    #[test]
    fn test_sources_reject_unknown_repo_ref() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    label: main
sources:
  - path: /home/user
    repos:
      - nonexistent
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown repository"), "unexpected: {msg}");
    }

    #[test]
    fn test_sources_hooks_string_and_list() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user
    hooks:
      before: "echo single"
      after:
        - "echo first"
        - "echo second"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let hooks = &repos[0].sources[0].hooks;
        assert_eq!(hooks.before, vec!["echo single"]);
        assert_eq!(hooks.after, vec!["echo first", "echo second"]);
        assert!(hooks.failed.is_empty());
        assert!(hooks.finally.is_empty());
    }

    #[test]
    fn test_source_hooks_rejects_unknown_keys() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user
    hooks:
      before_backup:
        - "echo wrong"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        // deny_unknown_fields on SourceHooksConfig triggers a deser error;
        // serde wraps it in the untagged-enum "did not match" message.
        assert!(
            msg.contains("before_backup")
                || msg.contains("unknown field")
                || msg.contains("did not match"),
            "expected deserialization error for unknown hook key, got: {msg}"
        );
    }

    #[test]
    fn test_empty_sources_allowed() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert!(repos[0].sources.is_empty());
    }

    #[test]
    fn test_tilde_expanded_in_repo_url_and_sources() {
        let yaml = r#"
repositories:
  - url: ~/backups/repo
sources:
  - ~/documents
  - path: ~/photos
    label: pics
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let home = paths::home_dir().unwrap().to_string_lossy().to_string();

        // Repository URL should be expanded
        assert!(
            repos[0].config.repository.url.starts_with(&home),
            "repo url not expanded: {}",
            repos[0].config.repository.url
        );
        // Path::join inserts OS separator at the join point but preserves
        // internal separators from the YAML value ("backups/repo").
        let expected_suffix = format!("{}backups/repo", std::path::MAIN_SEPARATOR);
        assert!(
            repos[0].config.repository.url.ends_with(&expected_suffix),
            "repo url suffix wrong: {}",
            repos[0].config.repository.url
        );

        // Simple source path should be expanded
        assert!(
            repos[0].sources[0].paths[0].starts_with(&home),
            "source path not expanded: {}",
            repos[0].sources[0].paths[0]
        );

        // Rich source path should be expanded
        assert!(
            repos[0].sources[1].paths[0].starts_with(&home),
            "rich source path not expanded: {}",
            repos[0].sources[1].paths[0]
        );
    }
}

#[cfg(test)]
mod source_shape_tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_sources_rich_paths_plural() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - paths:
      - /home/user/documents
      - /home/user/photos
    label: multi
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources.len(), 1);
        let src = &repos[0].sources[0];
        assert_eq!(src.paths, vec!["/home/user/documents", "/home/user/photos"]);
        assert_eq!(src.label, "multi");
    }

    #[test]
    fn test_sources_rich_path_singular_still_works() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user/documents
    label: docs
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user/documents"]);
        assert_eq!(repos[0].sources[0].label, "docs");
    }

    #[test]
    fn test_sources_reject_both_path_and_paths() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user/documents
    paths:
      - /home/user/photos
    label: bad
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("cannot have both 'path' and 'paths'"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_sources_reject_neither_path_nor_paths() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - label: bad
    exclude:
      - "*.tmp"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("must have 'path', 'paths', or 'command_dumps'"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_sources_multi_path_requires_label() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - paths:
      - /home/user/documents
      - /home/user/photos
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("require an explicit 'label'"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_sources_accept_duplicate_basenames_in_multi_path() {
        // Multi-path sources with duplicate basenames are accepted at load
        // time — disambiguation now happens at ResolvedSource::resolve_all
        // via the full-path snapshot prefix. (Issue #143.)
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a").join("docs");
        let b = dir.path().join("b").join("docs");
        fs::create_dir_all(&a).unwrap();
        fs::create_dir_all(&b).unwrap();
        let yaml = format!(
            r#"
repositories:
  - url: /tmp/repo
sources:
  - paths:
      - {}
      - {}
    label: multi
"#,
            a.display(),
            b.display()
        );
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources[0].paths.len(), 2);
    }

    #[test]
    fn test_sources_reject_empty_paths_list() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - paths: []
    label: empty
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must not be empty"), "unexpected: {msg}");
    }

    #[test]
    fn test_command_dumps_parse() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user
    command_dumps:
      - name: mydb.sql
        command: pg_dump mydb
      - name: other.sql
        command: pg_dumpall
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let source = &repos[0].sources[0];
        assert_eq!(source.command_dumps.len(), 2);
        assert_eq!(source.command_dumps[0].name, "mydb.sql");
        assert_eq!(source.command_dumps[0].command, "pg_dump mydb");
        assert_eq!(source.command_dumps[1].name, "other.sql");
    }

    #[test]
    fn test_command_dumps_empty_name_rejected() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user
    command_dumps:
      - name: ""
        command: echo hi
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        assert!(
            err.to_string().contains("'name' must not be empty"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_command_dumps_slash_in_name_rejected() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user
    command_dumps:
      - name: sub/dir.sql
        command: echo hi
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        assert!(
            err.to_string().contains("must not contain '/' or '\\'"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_command_dumps_duplicate_names_rejected() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user
    command_dumps:
      - name: dump.sql
        command: echo a
      - name: dump.sql
        command: echo b
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        assert!(
            err.to_string().contains("duplicate name 'dump.sql'"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_command_dumps_only_source_no_paths() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - label: databases
    command_dumps:
      - name: all.sql
        command: pg_dumpall
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let source = &repos[0].sources[0];
        assert!(source.paths.is_empty());
        assert_eq!(source.label, "databases");
        assert_eq!(source.command_dumps.len(), 1);
    }

    #[test]
    fn test_command_dumps_only_source_requires_label() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - command_dumps:
      - name: all.sql
        command: pg_dumpall
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        assert!(
            err.to_string()
                .contains("dump-only source entries require an explicit 'label'"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn hostname_override_parsed_from_yaml() {
        let yaml =
            "repositories:\n  - url: /tmp/repo\nsources:\n  - /tmp/src\nhostname: MyOverride\n";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();
        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(
            repos[0].config.hostname_override.as_deref(),
            Some("MyOverride")
        );
    }

    #[test]
    fn hostname_override_trims_whitespace() {
        let yaml = "repositories:\n  - url: /tmp/repo\nsources:\n  - /tmp/src\nhostname: \"  \"\n";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();
        let repos = load_and_resolve(&path).unwrap();
        assert!(repos[0].config.hostname_override.is_none());
    }
}
