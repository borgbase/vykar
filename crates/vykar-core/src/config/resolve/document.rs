use serde::Deserialize;

use super::super::defaults::*;
use super::super::deserialize::*;
use super::super::hooks::HooksConfig;
use super::super::limits::ResourceLimitsConfig;
use super::super::sources::SourceInput;
use super::super::types::*;
use super::super::util::expand_tilde;

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

    /// Use soft-delete for S3 Object Lock compatibility.
    #[serde(default)]
    pub s3_soft_delete: bool,

    /// Per-repository resource limits (full override of top-level `limits`).
    pub limits: Option<ResourceLimitsConfig>,

    /// Per-repo hooks (optional).
    #[serde(default)]
    pub hooks: Option<HooksConfig>,
}

impl RepositoryEntry {
    pub(super) fn to_repo_config(&self) -> RepositoryConfig {
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
            retry: self.retry.unwrap_or_default(),
            s3_soft_delete: self.s3_soft_delete,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictChunkerConfig {
    #[serde(default = "default_min_size")]
    min_size: u32,
    #[serde(default = "default_avg_size")]
    avg_size: u32,
    #[serde(default = "default_max_size")]
    max_size: u32,
}

impl From<StrictChunkerConfig> for ChunkerConfig {
    fn from(value: StrictChunkerConfig) -> Self {
        Self {
            min_size: value.min_size,
            avg_size: value.avg_size,
            max_size: value.max_size,
        }
    }
}

// ChunkerConfig is serialized in snapshot metadata, so we keep the wire type
// forward-compatible and enforce strict YAML config parsing here instead.
fn deserialize_strict_chunker_config<'de, D>(deserializer: D) -> Result<ChunkerConfig, D::Error>
where
    D: serde::Deserializer<'de>,
{
    StrictChunkerConfig::deserialize(deserializer).map(Into::into)
}

/// Intermediate deserialization struct for the YAML config file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConfigDocument {
    #[serde(default)]
    pub(super) repositories: Vec<RepositoryEntry>,
    #[serde(default)]
    pub(super) encryption: EncryptionConfig,
    #[serde(default)]
    pub(super) sources: Vec<SourceInput>,
    #[serde(default, deserialize_with = "deserialize_vec_strict_string")]
    pub(super) exclude_patterns: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_vec_strict_string")]
    pub(super) exclude_if_present: Vec<String>,
    #[serde(default = "default_one_file_system")]
    pub(super) one_file_system: bool,
    #[serde(default)]
    pub(super) git_ignore: bool,
    #[serde(default)]
    pub(super) xattrs: XattrsConfig,
    #[serde(default, deserialize_with = "deserialize_strict_chunker_config")]
    pub(super) chunker: ChunkerConfig,
    #[serde(default)]
    pub(super) compression: CompressionConfig,
    #[serde(default)]
    pub(super) retention: RetentionConfig,
    #[serde(default)]
    pub(super) schedule: ScheduleConfig,
    #[serde(default)]
    pub(super) limits: ResourceLimitsConfig,
    #[serde(default)]
    pub(super) compact: CompactConfig,
    #[serde(default)]
    pub(super) check: CheckConfig,
    /// Global hooks — apply to all repositories.
    #[serde(default)]
    pub(super) hooks: HooksConfig,
    /// Root directory for all local caches and pack temp files.
    #[serde(default)]
    pub(super) cache_dir: Option<String>,
    /// Override hostname recorded in snapshot metadata.
    #[serde(default)]
    pub(super) hostname: Option<String>,
    /// Paths to .env files loaded before environment variable expansion.
    /// Accepts a single path or a list of paths. Consumed during pre-parse in
    /// `load_and_resolve`; present here so `deny_unknown_fields` accepts it.
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    #[allow(dead_code)]
    pub(super) env_file: Vec<String>,
}

// Backward-compatible alias used by internal tests.
#[cfg(test)]
pub(super) type RawConfig = ConfigDocument;

/// Pre-parse struct to extract `env_file` before environment variable expansion.
/// Uses `flatten` + `Value` to ignore all other fields.
#[derive(Debug, Deserialize)]
pub(super) struct EnvFilePre {
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub(super) env_file: Vec<String>,
    #[serde(flatten)]
    _rest: serde_yaml::Value,
}

#[cfg(test)]
mod tests {
    use super::super::super::deserialize::STRICT_STRING_ERROR;
    use super::super::resolution::load_and_resolve;
    use std::fs;

    #[test]
    fn test_chunker_rejects_unknown_keys() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
chunker:
  avg_size: 1024
  unexpected: 1
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unexpected") || msg.contains("unknown field"),
            "expected unknown-field error, got: {msg}"
        );
    }

    #[test]
    fn test_compression_rejects_unknown_keys() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
compression:
  algorithm: zstd
  levelz: 3
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("levelz") || msg.contains("unknown field"),
            "expected unknown-field error, got: {msg}"
        );
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
        assert_eq!(repos[0].config.schedule.every, Some("12".to_string()));
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
}
