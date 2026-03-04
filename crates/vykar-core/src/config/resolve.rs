use std::fmt;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::platform::paths;

use super::defaults::*;
use super::deserialize::*;
use super::hooks::HooksConfig;
use super::limits::ResourceLimitsConfig;
use super::sources::{normalize_sources, SourceEntry, SourceInput};
use super::types::*;
use super::util::expand_tilde;

/// A single entry in the `repositories:` list.
/// Contains all `RepositoryConfig` fields plus optional per-repo overrides.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryEntry {
    /// Repository URL: bare path, `file://`, `s3://`, `s3+http://`, `sftp://`, or `http(s)://`.
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub url: String,
    #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
    pub region: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
    pub access_key_id: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
    pub secret_access_key: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
    pub sftp_key: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
    pub sftp_known_hosts: Option<String>,
    #[serde(default)]
    pub sftp_timeout: Option<u64>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_strict_string",
        alias = "rest_token"
    )]
    pub access_token: Option<String>,
    #[serde(default = "default_allow_insecure_http")]
    pub allow_insecure_http: bool,
    pub min_pack_size: Option<u32>,
    pub max_pack_size: Option<u32>,

    /// Optional label for `--repo` selection.
    #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
    pub label: Option<String>,

    // Per-repo overrides (None = use top-level defaults)
    pub encryption: Option<EncryptionConfig>,
    pub compression: Option<CompressionConfig>,
    pub retention: Option<RetentionConfig>,

    /// Retry settings for remote backends.
    pub retry: Option<RetryConfig>,

    /// Per-repository resource limits (full override of top-level `limits`).
    pub limits: Option<ResourceLimitsConfig>,

    /// Per-repo hooks (optional).
    #[serde(default)]
    pub hooks: Option<HooksConfig>,
}

impl RepositoryEntry {
    fn to_repo_config(&self) -> RepositoryConfig {
        RepositoryConfig {
            url: expand_tilde(&self.url),
            region: self.region.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
            sftp_key: self.sftp_key.clone(),
            sftp_known_hosts: self.sftp_known_hosts.clone(),
            sftp_timeout: self.sftp_timeout,
            access_token: self.access_token.clone(),
            allow_insecure_http: self.allow_insecure_http,
            min_pack_size: self.min_pack_size.unwrap_or_else(default_min_pack_size),
            max_pack_size: self.max_pack_size.unwrap_or_else(default_max_pack_size),
            retry: self.retry.clone().unwrap_or_default(),
        }
    }
}

/// Intermediate deserialization struct for the YAML config file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigDocument {
    #[serde(default)]
    repositories: Vec<RepositoryEntry>,
    #[serde(default)]
    encryption: EncryptionConfig,
    #[serde(default)]
    sources: Vec<SourceInput>,
    #[serde(default, deserialize_with = "deserialize_vec_strict_string")]
    exclude_patterns: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_vec_strict_string")]
    exclude_if_present: Vec<String>,
    #[serde(default = "default_one_file_system")]
    one_file_system: bool,
    #[serde(default)]
    git_ignore: bool,
    #[serde(default)]
    xattrs: XattrsConfig,
    #[serde(default)]
    chunker: ChunkerConfig,
    #[serde(default)]
    compression: CompressionConfig,
    #[serde(default)]
    retention: RetentionConfig,
    #[serde(default)]
    schedule: ScheduleConfig,
    #[serde(default)]
    limits: ResourceLimitsConfig,
    #[serde(default)]
    compact: CompactConfig,
    /// Global hooks — apply to all repositories.
    #[serde(default)]
    hooks: HooksConfig,
    /// Root directory for all local caches and pack temp files.
    #[serde(default)]
    cache_dir: Option<String>,
}

// Backward-compatible alias used by internal tests.
#[cfg(test)]
type RawConfig = ConfigDocument;

/// Expand `${VAR}` and `${VAR:-default}` placeholders in raw config text.
fn expand_env_placeholders(input: &str, path: &Path) -> vykar_types::error::Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut cursor = 0usize;

    while let Some(offset) = input[cursor..].find("${") {
        let start = cursor + offset;
        out.push_str(&input[cursor..start]);

        let token_start = start + 2;
        let Some(token_end_rel) = input[token_start..].find('}') else {
            return Err(config_expand_error(
                path,
                input,
                start,
                "unterminated environment placeholder",
            ));
        };
        let token_end = token_start + token_end_rel;
        let token = &input[token_start..token_end];
        let replacement = resolve_env_token(token, path, input, start)?;
        out.push_str(&replacement);
        cursor = token_end + 1;
    }

    out.push_str(&input[cursor..]);
    Ok(out)
}

fn resolve_env_token(
    token: &str,
    path: &Path,
    input: &str,
    start: usize,
) -> vykar_types::error::Result<String> {
    if token.is_empty() {
        return Err(config_expand_error(
            path,
            input,
            start,
            "empty environment placeholder",
        ));
    }

    if let Some(split_at) = token.find(":-") {
        let name = &token[..split_at];
        let default = &token[split_at + 2..];
        if !is_valid_env_var_name(name) {
            return Err(config_expand_error(
                path,
                input,
                start,
                format!("invalid environment variable name '{name}'"),
            ));
        }

        return match std::env::var(name) {
            Ok(value) if !value.is_empty() => Ok(value),
            Ok(_) => Ok(default.to_string()),
            Err(std::env::VarError::NotPresent) => Ok(default.to_string()),
            Err(std::env::VarError::NotUnicode(_)) => Err(config_expand_error(
                path,
                input,
                start,
                format!("environment variable '{name}' is not valid UTF-8"),
            )),
        };
    }

    if !is_valid_env_var_name(token) {
        return Err(config_expand_error(
            path,
            input,
            start,
            format!("invalid environment placeholder '{token}'"),
        ));
    }

    match std::env::var(token) {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Err(config_expand_error(
            path,
            input,
            start,
            format!("environment variable '{token}' is not set"),
        )),
        Err(std::env::VarError::NotUnicode(_)) => Err(config_expand_error(
            path,
            input,
            start,
            format!("environment variable '{token}' is not valid UTF-8"),
        )),
    }
}

fn is_valid_env_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

fn config_expand_error(
    path: &Path,
    input: &str,
    start: usize,
    message: impl fmt::Display,
) -> vykar_types::error::VykarError {
    let (line, column) = byte_offset_to_line_col(input, start);
    vykar_types::error::VykarError::Config(format!(
        "invalid config '{}': {message} at line {line}, column {column}",
        path.display()
    ))
}

fn byte_offset_to_line_col(input: &str, byte_offset: usize) -> (usize, usize) {
    let mut line = 1usize;
    let mut column = 1usize;
    for ch in input[..byte_offset].chars() {
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line, column)
}

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
    let expanded = expand_env_placeholders(&contents, path)?;
    let raw: ConfigDocument = serde_yaml::from_str(&expanded).map_err(|e| {
        vykar_types::error::VykarError::Config(format!("invalid config '{}': {e}", path.display()))
    })?;

    resolve_document(raw)
}

fn resolve_document(mut raw: ConfigDocument) -> vykar_types::error::Result<Vec<ResolvedRepo>> {
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

    // Validate global hooks and chunker params
    raw.hooks.validate()?;
    raw.limits.validate()?;
    raw.chunker.validate();
    raw.compact.validate();

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
                    cache_dir: raw
                        .cache_dir
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(expand_tilde),
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

// --- Config resolution ---

/// Tracks where the config file was found.
#[derive(Debug, Clone)]
pub enum ConfigSource {
    /// Explicitly passed via `--config`.
    CliArg(PathBuf),
    /// Set via the `VYKAR_CONFIG` env var.
    EnvVar(PathBuf),
    /// Found by searching standard locations.
    SearchOrder { path: PathBuf, level: &'static str },
}

impl ConfigSource {
    pub fn path(&self) -> &Path {
        match self {
            ConfigSource::CliArg(p) => p,
            ConfigSource::EnvVar(p) => p,
            ConfigSource::SearchOrder { path, .. } => path,
        }
    }
}

impl fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigSource::CliArg(p) => write!(f, "{} (--config)", p.display()),
            ConfigSource::EnvVar(p) => write!(f, "{} (VYKAR_CONFIG)", p.display()),
            ConfigSource::SearchOrder { path, level } => {
                write!(f, "{} ({})", path.display(), level)
            }
        }
    }
}

/// Returns search locations in priority order: project, user, system.
pub fn default_config_search_paths() -> Vec<(PathBuf, &'static str)> {
    let mut paths = vec![(PathBuf::from("vykar.yaml"), "project")];

    #[cfg(windows)]
    let user_config = paths::config_dir().map(|base| base.join("vykar").join("config.yaml"));

    #[cfg(not(windows))]
    let user_config = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| paths::home_dir().map(|h| h.join(".config")))
        .map(|base| base.join("vykar").join("config.yaml"));

    if let Some(p) = user_config {
        paths.push((p, "user"));
    }

    #[cfg(windows)]
    {
        let program_data = std::env::var_os("PROGRAMDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"));
        paths.push((program_data.join("vykar").join("config.yaml"), "system"));
    }

    #[cfg(not(windows))]
    {
        // System config
        paths.push((PathBuf::from("/etc/vykar/config.yaml"), "system"));
    }

    paths
}

/// Resolve which config file to use.
///
/// Priority: CLI arg > `VYKAR_CONFIG` env var > first existing file from search paths.
/// Returns `None` if nothing is found.
pub fn resolve_config_path(cli_config: Option<&str>) -> Option<ConfigSource> {
    // 1. Explicit --config
    if let Some(path) = cli_config {
        return Some(ConfigSource::CliArg(PathBuf::from(path)));
    }

    // 2. VYKAR_CONFIG env var
    if let Ok(val) = std::env::var("VYKAR_CONFIG") {
        if !val.is_empty() {
            return Some(ConfigSource::EnvVar(PathBuf::from(val)));
        }
    }

    // 3. Search standard locations
    for (path, level) in default_config_search_paths() {
        if path.exists() {
            return Some(ConfigSource::SearchOrder { path, level });
        }
    }

    None
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

/// Returns a minimal YAML config template suitable for bootstrapping.
pub fn minimal_config_template() -> &'static str {
    r#"# vykar configuration file
# Minimal required configuration.
# Full reference: https://vykar.borgbase.com/configuration

# repositories:
#   - url: /path/to/repo

# sources:
#   - /path/to/source

# --- Common optional settings (uncomment as needed) ---

# encryption:
#   passphrase: "secret"
#
# retention:
#   keep_daily: 7
#   keep_weekly: 4
#
# compression:
#   algorithm: zstd
#   zstd_level: 3
#
# exclude_patterns:
#   - "*.tmp"
#   - ".cache/**"
#
# schedule:
#   enabled: true
#   every: "24h"
"#
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Mutex;

    use super::super::deserialize::STRICT_STRING_ERROR;
    use crate::config::SourceHooksConfig;

    // Tests that mutate process-global state (env vars, CWD) must be serialized.
    static GLOBAL_STATE: Mutex<()> = Mutex::new(());

    #[test]
    fn test_search_paths_order() {
        let paths = default_config_search_paths();
        assert!(paths.len() >= 2);
        assert_eq!(paths[0].1, "project");
        // Last entry should be system
        assert_eq!(paths.last().unwrap().1, "system");
        // If there are 3 entries, middle is user
        if paths.len() == 3 {
            assert_eq!(paths[1].1, "user");
        }
    }

    #[test]
    fn test_resolve_cli_arg_wins() {
        let result = resolve_config_path(Some("/tmp/override.yaml"));
        let source = result.unwrap();
        assert!(matches!(source, ConfigSource::CliArg(_)));
        assert_eq!(source.path(), Path::new("/tmp/override.yaml"));
    }

    #[test]
    fn test_resolve_env_var() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_CONFIG", "/tmp/env-config.yaml");
        let result = resolve_config_path(None);
        let source = result.unwrap();
        assert!(matches!(source, ConfigSource::EnvVar(_)));
        assert_eq!(source.path(), Path::new("/tmp/env-config.yaml"));
    }

    #[test]
    fn test_resolve_search_finds_project() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("vykar.yaml");
        fs::write(&config_path, "repositories:\n  - url: /tmp/repo\n").unwrap();

        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let _env_guard = EnvGuard::set("VYKAR_CONFIG", "");

        let result = resolve_config_path(None);
        std::env::set_current_dir(original).unwrap();

        let source = result.unwrap();
        assert!(matches!(
            source,
            ConfigSource::SearchOrder {
                level: "project",
                ..
            }
        ));
    }

    #[test]
    fn test_resolve_nothing_found() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let _env_guard = EnvGuard::set("VYKAR_CONFIG", "");
        let _xdg_guard = EnvGuard::set("XDG_CONFIG_HOME", dir.path().to_str().unwrap());

        let result = resolve_config_path(None);
        std::env::set_current_dir(original).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_minimal_template_is_valid_yaml() {
        let template = minimal_config_template();
        // Template is valid YAML (everything uncommented is still parseable).
        let parsed: Result<RawConfig, _> = serde_yaml::from_str(template);
        assert!(
            parsed.is_ok(),
            "template should parse as valid YAML: {:?}",
            parsed.err()
        );
        // With repositories commented out, resolve_document should return an empty vec.
        let raw = parsed.unwrap();
        let result = resolve_document(raw).unwrap();
        assert!(result.is_empty(), "expected empty vec for template config");
    }

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

    /// RAII guard to set an env var and restore its previous value on drop.
    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, val: &str) -> Self {
            let prev = std::env::var(key).ok();
            std::env::set_var(key, val);
            Self { key, prev }
        }

        fn unset(key: &'static str) -> Self {
            let prev = std::env::var(key).ok();
            std::env::remove_var(key);
            Self { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }

    // --- Multi-repo tests ---

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
  cpu:
    max_threads: 4
    nice: 5
  io:
    read_mib_per_sec: 100
    write_mib_per_sec: 50
  network:
    read_mib_per_sec: 80
    write_mib_per_sec: 40

repositories:
  - url: /backups/local
    label: local
  - url: /backups/remote
    label: remote
    limits:
      cpu:
        max_threads: 2
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 2);

        let local = &repos[0].config.limits;
        assert_eq!(local.cpu.max_threads, 4);
        assert_eq!(local.cpu.nice, 5);
        assert_eq!(local.io.read_mib_per_sec, 100);
        assert_eq!(local.io.write_mib_per_sec, 50);
        assert_eq!(local.network.read_mib_per_sec, 80);
        assert_eq!(local.network.write_mib_per_sec, 40);

        let remote = &repos[1].config.limits;
        assert_eq!(remote.cpu.max_threads, 2);
        assert_eq!(remote.cpu.nice, 0);
        assert_eq!(remote.io.read_mib_per_sec, 0);
        assert_eq!(remote.io.write_mib_per_sec, 0);
        assert_eq!(remote.network.read_mib_per_sec, 0);
        assert_eq!(remote.network.write_mib_per_sec, 0);
    }

    #[test]
    fn test_limits_invalid_nice_rejected() {
        let yaml = r#"
limits:
  cpu:
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
            err.to_string().contains("limits.cpu.nice"),
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

    fn make_test_repo(url: &str, label: Option<&str>) -> ResolvedRepo {
        ResolvedRepo {
            label: label.map(|s| s.to_string()),
            config: VykarConfig {
                repository: RepositoryConfig {
                    url: url.to_string(),
                    region: None,
                    access_key_id: None,
                    secret_access_key: None,
                    sftp_key: None,
                    sftp_known_hosts: None,
                    sftp_timeout: None,
                    access_token: None,
                    allow_insecure_http: false,
                    min_pack_size: default_min_pack_size(),
                    max_pack_size: default_max_pack_size(),
                    retry: RetryConfig::default(),
                },
                encryption: EncryptionConfig::default(),
                exclude_patterns: vec![],
                exclude_if_present: vec![],
                one_file_system: default_one_file_system(),
                git_ignore: false,
                chunker: ChunkerConfig::default(),
                compression: CompressionConfig::default(),
                retention: RetentionConfig::default(),
                xattrs: XattrsConfig::default(),
                schedule: ScheduleConfig::default(),
                limits: ResourceLimitsConfig::default(),
                compact: CompactConfig::default(),
                cache_dir: None,
            },
            global_hooks: HooksConfig::default(),
            repo_hooks: HooksConfig::default(),
            sources: vec![],
        }
    }

    // --- select_sources tests ---

    fn make_test_source(label: &str) -> SourceEntry {
        SourceEntry {
            paths: vec![format!("/home/{label}")],
            label: label.to_string(),
            exclude: Vec::new(),
            exclude_if_present: Vec::new(),
            one_file_system: default_one_file_system(),
            git_ignore: false,
            xattrs_enabled: default_xattrs_enabled(),
            hooks: SourceHooksConfig::default(),
            retention: None,
            repos: Vec::new(),
            command_dumps: Vec::new(),
        }
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

    // --- Hooks config tests ---

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

    // --- Sources tests ---

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
        assert!(repos[0].config.repository.url.ends_with("/backups/repo"));

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

    #[test]
    fn test_env_expand_bare_var_in_config() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let _repo_guard = EnvGuard::set("VYKAR_TEST_REPO_URL", "/tmp/vykar-env-repo");

        let yaml = r#"
repositories:
  - url: ${VYKAR_TEST_REPO_URL}
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "/tmp/vykar-env-repo");
    }

    #[test]
    fn test_env_expand_default_used_when_unset() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let _repo_guard = EnvGuard::unset("VYKAR_TEST_REPO_URL");

        let yaml = r#"
repositories:
  - url: ${VYKAR_TEST_REPO_URL:-/tmp/vykar-default-repo}
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "/tmp/vykar-default-repo");
    }

    #[test]
    fn test_env_expand_default_used_when_empty() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let _repo_guard = EnvGuard::set("VYKAR_TEST_REPO_URL", "");

        let yaml = r#"
repositories:
  - url: ${VYKAR_TEST_REPO_URL:-/tmp/vykar-default-repo}
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "/tmp/vykar-default-repo");
    }

    #[test]
    fn test_env_expand_default_not_used_when_non_empty() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let _repo_guard = EnvGuard::set("VYKAR_TEST_REPO_URL", "/tmp/vykar-non-empty-repo");

        let yaml = r#"
repositories:
  - url: ${VYKAR_TEST_REPO_URL:-/tmp/vykar-default-repo}
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "/tmp/vykar-non-empty-repo");
    }

    #[test]
    fn test_env_expand_bare_var_missing_is_error() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let _repo_guard = EnvGuard::unset("VYKAR_TEST_REPO_URL");

        let yaml = r#"
repositories:
  - url: ${VYKAR_TEST_REPO_URL}
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("VYKAR_TEST_REPO_URL"), "unexpected: {msg}");
        assert!(msg.contains("line"), "unexpected: {msg}");
        assert!(msg.contains("column"), "unexpected: {msg}");
    }

    #[test]
    fn test_env_expand_bare_var_can_be_empty() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_EMPTY", "");

        let expanded =
            expand_env_placeholders("repo=${VYKAR_TEST_EMPTY}", Path::new("test-config.yaml"))
                .unwrap();
        assert_eq!(expanded, "repo=");
    }

    #[test]
    fn test_env_expand_rejects_unterminated_placeholder() {
        let err = expand_env_placeholders("repo=${VYKAR_TEST", Path::new("test-config.yaml"))
            .unwrap_err();
        assert!(
            err.to_string().contains("unterminated"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_env_expand_rejects_empty_placeholder() {
        let err = expand_env_placeholders("repo=${}", Path::new("test-config.yaml")).unwrap_err();
        assert!(err.to_string().contains("empty"), "unexpected: {err}");
    }

    #[test]
    fn test_env_expand_rejects_invalid_variable_name() {
        let err =
            expand_env_placeholders("repo=${1BAD}", Path::new("test-config.yaml")).unwrap_err();
        assert!(
            err.to_string().contains("invalid environment"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_env_expand_rejects_invalid_placeholder_syntax() {
        let err =
            expand_env_placeholders("repo=${VYKAR_TEST-default}", Path::new("test-config.yaml"))
                .unwrap_err();
        assert!(
            err.to_string().contains("invalid environment placeholder"),
            "unexpected: {err}"
        );
    }

    // --- Multi-path source tests ---

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
    fn test_sources_reject_duplicate_basenames_in_multi_path() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - paths:
      - /home/user/a/docs
      - /home/user/b/docs
    label: multi
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate basename 'docs'"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_sources_reject_duplicate_basenames_in_simple_group() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user/a/docs
  - /home/user/b/docs
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate basename 'docs'"),
            "unexpected: {msg}"
        );
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
    fn test_keep_within_accepts_integer_scalar() {
        let yaml = r#"
retention:
  keep_within: 7
repositories:
  - url: /tmp/repo
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.retention.keep_within.as_deref(), Some("7"));
    }

    #[test]
    fn test_schedule_every_accepts_integer_scalar() {
        let yaml = r#"
schedule:
  every: 12
repositories:
  - url: /tmp/repo
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.schedule.every, "12");
        assert_eq!(
            repos[0].config.schedule.every_duration().unwrap().as_secs(),
            12 * 24 * 60 * 60
        );
    }

    #[test]
    fn test_strict_string_rejects_unquoted_numeric_label() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    label: 7
sources:
  - /home/user
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
    fn test_strict_string_rejects_unquoted_bool_url() {
        let yaml = r#"
repositories:
  - url: false
sources:
  - /home/user
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
    fn test_strict_string_accepts_quoted_literals() {
        let yaml = r#"
repositories:
  - url: "false"
    label: "7"
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "false");
        assert_eq!(repos[0].label.as_deref(), Some("7"));
    }

    /// serde_yaml 0.9 only treats `true`/`false`/`True`/`False`/`TRUE`/`FALSE`
    /// as booleans. The extended YAML 1.1 boolean literals (`yes`/`no`/`on`/`off`)
    /// are deserialized as plain strings, so they pass through `StrictString`
    /// without issue. These tests document that guarantee.
    #[test]
    fn test_strict_string_allows_yaml11_bool_words_as_strings() {
        for word in ["no", "yes", "on", "off"] {
            let yaml = format!(
                r#"
repositories:
  - url: /tmp/repo
    label: {word}
sources:
  - /home/user
"#
            );
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("config.yaml");
            fs::write(&path, yaml).unwrap();

            let repos = load_and_resolve(&path).unwrap();
            assert_eq!(
                repos[0].label.as_deref(),
                Some(word),
                "bare `{word}` should be treated as a string, not a boolean"
            );
        }
    }

    // --- command_dumps tests ---

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
}
