use std::time::Duration;

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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
pub struct ScheduleConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(
        default = "default_schedule_every",
        deserialize_with = "deserialize_duration_string"
    )]
    pub every: String,
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
            every: default_schedule_every(),
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: default_passphrase_prompt_timeout_seconds(),
        }
    }
}

impl ScheduleConfig {
    pub fn every_duration(&self) -> vykar_types::error::Result<Duration> {
        super::defaults::parse_human_duration(&self.every)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Maximum concurrent SFTP connections (default: 4, clamped to 1..=32).
    pub sftp_max_connections: Option<usize>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub fn validate(&mut self) {
        if self.max_size > CHUNK_MAX_SIZE_HARD_CAP {
            tracing::warn!(
                configured = self.max_size,
                cap = CHUNK_MAX_SIZE_HARD_CAP,
                "chunker.max_size exceeds hard cap, clamping"
            );
            self.max_size = CHUNK_MAX_SIZE_HARD_CAP;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
