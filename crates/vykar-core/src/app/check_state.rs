use std::path::{Path, PathBuf};
use std::time::Duration;

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use vykar_common::paths;

#[derive(Debug, Serialize, Deserialize)]
struct CheckStateFile {
    fingerprint: String,
    last_full_check: DateTime<Utc>,
}

/// Per-repo check state file path.
/// `<cache_base>/vykar/check.<BLAKE2b-256(url) hex>`
///
/// Deliberately BLAKE2b for every repository format, including v3. A pure
/// URL-to-filename cache key; changing it would orphan local check state.
fn check_state_path(url: &str, cache_dir: Option<&Path>) -> Option<PathBuf> {
    let base = match cache_dir {
        Some(dir) => Some(dir.to_path_buf()),
        None => paths::cache_dir().map(|d| d.join("vykar")),
    };
    base.map(|b| {
        let mut hasher = Blake2bVar::new(32).expect("valid output size");
        hasher.update(url.as_bytes());
        let mut hash = [0u8; 32];
        hasher.finalize_variable(&mut hash).expect("correct length");
        b.join(format!("check.{}", hex::encode(hash)))
    })
}

/// Read the last full check timestamp, validating fingerprint.
/// Returns None if missing, corrupt, or fingerprint mismatch.
pub fn last_full_check(
    url: &str,
    fingerprint: &str,
    cache_dir: Option<&Path>,
) -> Option<DateTime<Utc>> {
    let path = check_state_path(url, cache_dir)?;
    let contents = std::fs::read_to_string(&path).ok()?;
    let state: CheckStateFile = serde_json::from_str(&contents).ok()?;
    if state.fingerprint != fingerprint {
        tracing::debug!("check state fingerprint mismatch for {url}, will run full check");
        return None;
    }
    Some(state.last_full_check)
}

/// Record a successful full check with the repo fingerprint.
/// Creates parent dirs if needed. Non-fatal on write failure.
pub fn record_full_check(url: &str, fingerprint: &str, cache_dir: Option<&Path>) {
    let Some(path) = check_state_path(url, cache_dir) else {
        tracing::warn!("cache directory unavailable; cannot record check timestamp");
        return;
    };

    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::warn!("could not create cache directory {}: {e}", parent.display());
            return;
        }
    }

    let state = CheckStateFile {
        fingerprint: fingerprint.to_string(),
        last_full_check: Utc::now(),
    };

    let json = match serde_json::to_string_pretty(&state) {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!("could not serialize check state: {e}");
            return;
        }
    };

    if let Err(e) = crate::repo::file_cache::atomic_write(&path, json.as_bytes()) {
        tracing::warn!("could not write check state to {}: {e}", path.display());
    }
}

/// Check if a full check is due based on the configured interval.
/// Returns true (fail-open) if state is missing, corrupt, or fingerprint mismatches.
pub fn full_check_is_due(
    url: &str,
    fingerprint: &str,
    cache_dir: Option<&Path>,
    interval: Duration,
) -> bool {
    let Some(last) = last_full_check(url, fingerprint, cache_dir) else {
        return true;
    };
    let elapsed = Utc::now().signed_duration_since(last);
    let interval_chrono = chrono::Duration::from_std(interval).unwrap_or(chrono::TimeDelta::MAX);
    elapsed >= interval_chrono
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_state_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(last_full_check("file:///test", "abc123", Some(dir.path())).is_none());
    }

    #[test]
    fn round_trip_read_write() {
        let dir = tempfile::tempdir().unwrap();
        let url = "file:///test";
        let fp = "abc123";

        record_full_check(url, fp, Some(dir.path()));
        let ts = last_full_check(url, fp, Some(dir.path()));
        assert!(ts.is_some());
        // Should have been written just now
        let elapsed = Utc::now().signed_duration_since(ts.unwrap());
        assert!(elapsed.num_seconds() < 5);
    }

    #[test]
    fn fingerprint_mismatch_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let url = "s3://bucket/repo";

        record_full_check(url, "fp-old", Some(dir.path()));
        assert!(last_full_check(url, "fp-new", Some(dir.path())).is_none());
    }

    #[test]
    fn full_check_is_due_when_no_state() {
        let dir = tempfile::tempdir().unwrap();
        assert!(full_check_is_due(
            "file:///test",
            "abc",
            Some(dir.path()),
            Duration::from_secs(3600),
        ));
    }

    #[test]
    fn full_check_not_due_when_recent() {
        let dir = tempfile::tempdir().unwrap();
        let url = "file:///test";
        let fp = "abc";

        record_full_check(url, fp, Some(dir.path()));
        assert!(!full_check_is_due(
            url,
            fp,
            Some(dir.path()),
            Duration::from_secs(3600),
        ));
    }

    #[test]
    fn full_check_due_after_fingerprint_change() {
        let dir = tempfile::tempdir().unwrap();
        let url = "file:///test";

        record_full_check(url, "old-fp", Some(dir.path()));
        assert!(full_check_is_due(
            url,
            "new-fp",
            Some(dir.path()),
            Duration::from_secs(86400 * 365),
        ));
    }

    #[test]
    fn different_urls_independent() {
        let dir = tempfile::tempdir().unwrap();
        record_full_check("s3://a", "fp-a", Some(dir.path()));
        record_full_check("s3://b", "fp-b", Some(dir.path()));

        assert!(last_full_check("s3://a", "fp-a", Some(dir.path())).is_some());
        assert!(last_full_check("s3://b", "fp-b", Some(dir.path())).is_some());
        // Cross-check fingerprints should fail
        assert!(last_full_check("s3://a", "fp-b", Some(dir.path())).is_none());
    }
}
