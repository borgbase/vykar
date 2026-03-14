use std::time::Duration;

use fastcdc::v2020::{AVERAGE_MAX, AVERAGE_MIN, MAXIMUM_MIN, MINIMUM_MAX, MINIMUM_MIN};
use serde::{Deserialize, Serialize};

use super::defaults::*;
use super::deserialize::*;
use super::limits::ResourceLimitsConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VykarConfig {
    pub repository: RepositoryConfig,
    #[serde(default)]
    pub encryption: EncryptionConfig,
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    #[serde(default)]
    pub exclude_if_present: Vec<String>,
    #[serde(default = "default_one_file_system")]
    pub one_file_system: bool,
    #[serde(default)]
    pub git_ignore: bool,
    #[serde(default)]
    pub chunker: ChunkerConfig,
    #[serde(default)]
    pub compression: CompressionConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    #[serde(default)]
    pub xattrs: XattrsConfig,
    #[serde(default)]
    pub schedule: ScheduleConfig,
    #[serde(default)]
    pub limits: ResourceLimitsConfig,
    #[serde(default)]
    pub compact: CompactConfig,
    /// Root directory for all local caches and pack temp files.
    /// Default: platform cache dir + "vykar" (e.g. ~/.cache/vykar/).
    #[serde(default)]
    pub cache_dir: Option<String>,
    /// Runtime-only: accept a changed repository identity and re-pin.
    #[serde(skip)]
    pub trust_repo: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    /// Keep all snapshots within this time interval (e.g. "2d", "48h", "1w")
    #[serde(default, deserialize_with = "deserialize_optional_duration_string")]
    pub keep_within: Option<String>,
    /// Keep the N most recent snapshots
    pub keep_last: Option<usize>,
    pub keep_hourly: Option<usize>,
    pub keep_daily: Option<usize>,
    pub keep_weekly: Option<usize>,
    pub keep_monthly: Option<usize>,
    pub keep_yearly: Option<usize>,
}

impl RetentionConfig {
    /// Returns true if at least one keep_* option is set.
    pub fn has_any_rule(&self) -> bool {
        self.keep_within.is_some()
            || self.keep_last.is_some()
            || self.keep_hourly.is_some()
            || self.keep_daily.is_some()
            || self.keep_weekly.is_some()
            || self.keep_monthly.is_some()
            || self.keep_yearly.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct XattrsConfig {
    #[serde(default = "default_xattrs_enabled")]
    pub enabled: bool,
}

impl Default for XattrsConfig {
    fn default() -> Self {
        Self {
            enabled: default_xattrs_enabled(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, deserialize_with = "deserialize_optional_duration_string")]
    pub every: Option<String>,
    #[serde(default)]
    pub cron: Option<String>,
    #[serde(default)]
    pub on_startup: bool,
    #[serde(default)]
    pub jitter_seconds: u64,
    #[serde(default = "default_passphrase_prompt_timeout_seconds")]
    pub passphrase_prompt_timeout_seconds: u64,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            every: None,
            cron: None,
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: default_passphrase_prompt_timeout_seconds(),
        }
    }
}

impl ScheduleConfig {
    pub fn every_duration(&self) -> vykar_types::error::Result<Duration> {
        let raw = self.every.as_deref().unwrap_or("24h");
        super::defaults::parse_human_duration(raw)
    }

    pub fn is_cron(&self) -> bool {
        self.cron.is_some()
    }

    pub fn validate(&self) -> vykar_types::error::Result<()> {
        use vykar_types::error::VykarError;

        if self.every.is_some() && self.cron.is_some() {
            return Err(VykarError::Config(
                "schedule: 'every' and 'cron' are mutually exclusive".into(),
            ));
        }

        if let Some(ref expr) = self.cron {
            use croner::Cron;
            expr.parse::<Cron>().map_err(|e| {
                VykarError::Config(format!("schedule.cron: invalid expression '{expr}': {e}"))
            })?;
            // Reject 6-field (with seconds) and 7-field expressions
            let field_count = expr.split_whitespace().count();
            if field_count != 5 {
                return Err(VykarError::Config(format!(
                    "schedule.cron: expected 5 fields (minute hour dom month dow), got {field_count}"
                )));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryConfig {
    /// Repository URL: bare path, `file://`, `s3://`, `s3+http://`, `sftp://`, or `http(s)://`.
    pub url: String,
    /// S3 region (default: us-east-1).
    pub region: Option<String>,
    /// S3 access key ID.
    pub access_key_id: Option<String>,
    /// S3 secret access key.
    pub secret_access_key: Option<String>,
    /// Path to SSH private key for SFTP backend.
    pub sftp_key: Option<String>,
    /// Path to OpenSSH known_hosts file for SFTP host key verification.
    pub sftp_known_hosts: Option<String>,
    /// Per-request SFTP timeout in seconds (default: 30).
    pub sftp_timeout: Option<u64>,
    /// Bearer token for server backend authentication.
    pub access_token: Option<String>,
    /// Allow plaintext HTTP transport for remote endpoints (unsafe; defaults to false).
    #[serde(default = "default_allow_insecure_http")]
    pub allow_insecure_http: bool,
    #[serde(default = "default_min_pack_size")]
    pub min_pack_size: u32,
    #[serde(default = "default_max_pack_size")]
    pub max_pack_size: u32,
    /// Retry settings for remote backends (S3, SFTP, REST).
    #[serde(default)]
    pub retry: RetryConfig,
    /// Use soft-delete for S3 Object Lock compatibility.
    ///
    /// When enabled, `delete()` overwrites the object with a zero-byte tombstone
    /// instead of issuing a real DELETE. Combined with S3 Object Lock, this
    /// preserves previous versions for the configured retention period.
    #[serde(default)]
    pub s3_soft_delete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptionConfig {
    #[serde(default = "default_encryption_mode")]
    pub mode: EncryptionModeConfig,
    #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
    pub passphrase: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
    pub passcommand: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionModeConfig {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "aes256gcm")]
    Aes256Gcm,
    #[serde(rename = "chacha20poly1305")]
    Chacha20Poly1305,
}

impl EncryptionModeConfig {
    pub fn as_str(self) -> &'static str {
        match self {
            EncryptionModeConfig::None => "none",
            EncryptionModeConfig::Auto => "auto",
            EncryptionModeConfig::Aes256Gcm => "aes256gcm",
            EncryptionModeConfig::Chacha20Poly1305 => "chacha20poly1305",
        }
    }
}

impl PartialEq<&str> for EncryptionModeConfig {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            mode: default_encryption_mode(),
            passphrase: None,
            passcommand: None,
        }
    }
}

/// Hard cap on `ChunkerConfig::max_size`. Any user-configured value above
/// this is clamped down during validation. This bounds the maximum encrypted
/// blob size to `CHUNK_MAX_SIZE_HARD_CAP + 1024` (compression tag + encryption
/// envelope).
pub const CHUNK_MAX_SIZE_HARD_CAP: u32 = 16 * 1024 * 1024; // 16 MiB

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkerConfig {
    #[serde(default = "default_min_size")]
    pub min_size: u32,
    #[serde(default = "default_avg_size")]
    pub avg_size: u32,
    #[serde(default = "default_max_size")]
    pub max_size: u32,
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        Self {
            min_size: default_min_size(),
            avg_size: default_avg_size(),
            max_size: default_max_size(),
        }
    }
}

impl ChunkerConfig {
    /// Clamp `max_size` to `CHUNK_MAX_SIZE_HARD_CAP` with a warning if it
    /// was configured above the cap.
    pub fn validate(&mut self) -> vykar_types::error::Result<()> {
        use vykar_types::error::VykarError;

        if self.max_size > CHUNK_MAX_SIZE_HARD_CAP {
            tracing::warn!(
                configured = self.max_size,
                cap = CHUNK_MAX_SIZE_HARD_CAP,
                "chunker.max_size exceeds hard cap, clamping"
            );
            self.max_size = CHUNK_MAX_SIZE_HARD_CAP;
        }

        if !(MINIMUM_MIN..=MINIMUM_MAX).contains(&self.min_size) {
            return Err(VykarError::Config(format!(
                "chunker.min_size must be in [{MINIMUM_MIN}, {MINIMUM_MAX}], got {}",
                self.min_size
            )));
        }

        if !(AVERAGE_MIN..=AVERAGE_MAX).contains(&self.avg_size) {
            return Err(VykarError::Config(format!(
                "chunker.avg_size must be in [{AVERAGE_MIN}, {AVERAGE_MAX}], got {}",
                self.avg_size
            )));
        }

        if !(MAXIMUM_MIN..=CHUNK_MAX_SIZE_HARD_CAP).contains(&self.max_size) {
            return Err(VykarError::Config(format!(
                "chunker.max_size must be in [{MAXIMUM_MIN}, {CHUNK_MAX_SIZE_HARD_CAP}], got {}",
                self.max_size
            )));
        }

        if self.min_size > self.avg_size {
            return Err(VykarError::Config(format!(
                "chunker must satisfy min_size <= avg_size, got {} > {}",
                self.min_size, self.avg_size
            )));
        }

        if self.avg_size > self.max_size {
            return Err(VykarError::Config(format!(
                "chunker must satisfy avg_size <= max_size, got {} > {}",
                self.avg_size, self.max_size
            )));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompressionConfig {
    #[serde(default = "default_algorithm")]
    pub algorithm: CompressionAlgorithm,
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionAlgorithm {
    None,
    Lz4,
    Zstd,
}

impl CompressionAlgorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            CompressionAlgorithm::None => "none",
            CompressionAlgorithm::Lz4 => "lz4",
            CompressionAlgorithm::Zstd => "zstd",
        }
    }
}

impl PartialEq<&str> for CompressionAlgorithm {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: default_algorithm(),
            zstd_level: default_zstd_level(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactConfig {
    #[serde(default = "default_compact_threshold")]
    pub threshold: f64,
}

impl Default for CompactConfig {
    fn default() -> Self {
        Self {
            threshold: default_compact_threshold(),
        }
    }
}

impl CompactConfig {
    pub fn validate(&mut self) {
        if !self.threshold.is_finite() || self.threshold < 0.0 || self.threshold > 100.0 {
            tracing::warn!(
                configured = self.threshold,
                "compact.threshold out of range (0–100), resetting to default"
            );
            self.threshold = default_compact_threshold();
        }
    }
}

// RetryConfig is defined in vykar-storage; re-exported here for ergonomic config construction.
pub use vykar_storage::RetryConfig;

#[cfg(test)]
mod tests {
    use super::*;

    fn schedule(every: Option<&str>, cron: Option<&str>) -> ScheduleConfig {
        ScheduleConfig {
            every: every.map(String::from),
            cron: cron.map(String::from),
            ..Default::default()
        }
    }

    #[test]
    fn validate_accepts_every_only() {
        assert!(schedule(Some("6h"), None).validate().is_ok());
    }

    #[test]
    fn validate_accepts_cron_only() {
        assert!(schedule(None, Some("0 3 * * *")).validate().is_ok());
    }

    #[test]
    fn validate_accepts_neither() {
        assert!(schedule(None, None).validate().is_ok());
    }

    #[test]
    fn validate_rejects_both_every_and_cron() {
        let err = schedule(Some("6h"), Some("0 3 * * *"))
            .validate()
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("mutually exclusive"), "got: {msg}");
    }

    #[test]
    fn validate_rejects_invalid_cron() {
        let err = schedule(None, Some("bad")).validate().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid expression"), "got: {msg}");
    }

    #[test]
    fn validate_rejects_six_field_cron() {
        let err = schedule(None, Some("0 0 3 * * *")).validate().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("expected 5 fields"), "got: {msg}");
    }

    #[test]
    fn validate_rejects_seven_field_cron() {
        let err = schedule(None, Some("0 0 3 * * * 2025"))
            .validate()
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("expected 5 fields"), "got: {msg}");
    }

    #[test]
    fn chunker_validate_accepts_defaults() {
        let mut config = ChunkerConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn chunker_validate_clamps_hard_cap() {
        let mut config = ChunkerConfig {
            min_size: 256,
            avg_size: 1024,
            max_size: CHUNK_MAX_SIZE_HARD_CAP + 1,
        };

        config.validate().unwrap();
        assert_eq!(config.max_size, CHUNK_MAX_SIZE_HARD_CAP);
    }

    #[test]
    fn chunker_validate_rejects_fastcdc_bounds() {
        let mut config = ChunkerConfig {
            min_size: MINIMUM_MIN - 1,
            avg_size: 1024,
            max_size: 4096,
        };

        let err = config.validate().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("chunker.min_size"), "got: {msg}");
    }

    #[test]
    fn chunker_validate_rejects_invalid_ordering() {
        let mut config = ChunkerConfig {
            min_size: 4096,
            avg_size: 1024,
            max_size: 8192,
        };

        let err = config.validate().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("min_size <= avg_size"), "got: {msg}");
    }
}
