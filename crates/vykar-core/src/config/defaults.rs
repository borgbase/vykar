use std::time::Duration;

use super::types::{CompressionAlgorithm, EncryptionModeConfig};

pub(super) fn default_encryption_mode() -> EncryptionModeConfig {
    EncryptionModeConfig::Auto
}

pub(super) fn default_min_size() -> u32 {
    512 * 1024 // 512 KiB
}

pub(super) fn default_avg_size() -> u32 {
    2 * 1024 * 1024 // 2 MiB
}

pub(super) fn default_max_size() -> u32 {
    8 * 1024 * 1024 // 8 MiB
}

pub(super) fn default_algorithm() -> CompressionAlgorithm {
    CompressionAlgorithm::Lz4
}

pub(super) fn default_zstd_level() -> i32 {
    3
}

pub(crate) fn default_min_pack_size() -> u32 {
    32 * 1024 * 1024 // 32 MiB
}

pub(crate) fn default_max_pack_size() -> u32 {
    192 * 1024 * 1024 // 192 MiB
}

pub(super) fn default_allow_insecure_http() -> bool {
    false
}

pub(super) fn default_one_file_system() -> bool {
    false
}

pub(super) fn default_xattrs_enabled() -> bool {
    true
}

pub(super) fn default_compact_threshold() -> f64 {
    20.0
}

pub(super) fn default_passphrase_prompt_timeout_seconds() -> u64 {
    300
}

/// Parse a simple duration string like "30m", "4h", or "2d".
pub fn parse_interval(raw: &str) -> vykar_types::error::Result<Duration> {
    let input = raw.trim();
    if input.is_empty() {
        return Err(vykar_types::error::VykarError::Config(
            "duration must not be empty".into(),
        ));
    }

    let (num_part, unit) = match input.chars().last() {
        Some(c) if c.is_ascii_alphabetic() => (&input[..input.len() - 1], Some(c)),
        Some(_) => (input, None),
        None => {
            return Err(vykar_types::error::VykarError::Config(
                "duration must not be empty".into(),
            ));
        }
    };

    let value: u64 = num_part.parse().map_err(|_| {
        vykar_types::error::VykarError::Config(format!("invalid duration value: '{raw}'"))
    })?;

    let secs = match unit {
        Some('s') | Some('S') => value,
        Some('m') | Some('M') => value.saturating_mul(60),
        Some('h') | Some('H') => value.saturating_mul(60 * 60),
        Some('d') | Some('D') => value.saturating_mul(60 * 60 * 24),
        Some(other) => {
            return Err(vykar_types::error::VykarError::Config(format!(
                "unsupported duration suffix '{other}' in '{raw}' (use s/m/h/d)"
            )));
        }
        None => value.saturating_mul(60 * 60 * 24),
    };

    if secs == 0 {
        return Err(vykar_types::error::VykarError::Config(
            "duration must be greater than zero".into(),
        ));
    }

    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval_units() {
        assert_eq!(parse_interval("10s").unwrap().as_secs(), 10);
        assert_eq!(parse_interval("30m").unwrap().as_secs(), 30 * 60);
        assert_eq!(parse_interval("4h").unwrap().as_secs(), 4 * 60 * 60);
        assert_eq!(parse_interval("2d").unwrap().as_secs(), 2 * 24 * 60 * 60);
    }

    #[test]
    fn test_parse_interval_plain_number_is_days() {
        assert_eq!(parse_interval("3").unwrap().as_secs(), 3 * 24 * 60 * 60);
    }

    #[test]
    fn test_parse_interval_rejects_invalid_values() {
        assert!(parse_interval("").is_err());
        assert!(parse_interval("0h").is_err());
        assert!(parse_interval("5w").is_err());
    }
}
