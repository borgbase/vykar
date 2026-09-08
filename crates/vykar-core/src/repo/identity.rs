use std::io::Write;
use std::path::{Path, PathBuf};

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use serde::{Deserialize, Serialize};

use vykar_common::paths;
use vykar_types::error::{Result, VykarError};

/// Pin file format (one per repository URL).
#[derive(Debug, Serialize, Deserialize)]
struct PinFile {
    version: u32,
    repo_id: String,
    fingerprint: String,
    url: String,
}

/// Compute a 32-byte fingerprint from repo identity and key material.
/// `BLAKE2b-256(repo_id || chunk_id_key)`
///
/// **Deliberately BLAKE2b for every repository format, including v3.** Keeping it fixed
/// preserves every existing TOFU pin with zero branching, and this hashes key
/// material rather than content, so the BLAKE3 throughput argument does not
/// apply. Do not "finish the migration" here.
pub fn compute_fingerprint(repo_id: &[u8], chunk_id_key: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Blake2bVar::new(32).expect("valid output size");
    hasher.update(repo_id);
    hasher.update(chunk_id_key);
    let mut out = [0u8; 32];
    hasher.finalize_variable(&mut out).expect("correct length");
    out
}

/// Derive the pin file path for a given URL.
/// `<cache_base>/vykar/pin.<BLAKE2b-256(url), all 32 bytes hex-encoded>`
///
/// Deliberately BLAKE2b for every repository format, including v3. This is a pure
/// URL-to-filename cache key; changing it would orphan every local pin for no
/// gain.
fn pin_file_path(url: &str, cache_dir: Option<&Path>) -> Option<PathBuf> {
    let base = match cache_dir {
        Some(dir) => Some(dir.to_path_buf()),
        None => paths::cache_dir().map(|d| d.join("vykar")),
    };
    base.map(|b| {
        let mut hasher = Blake2bVar::new(32).expect("valid output size");
        hasher.update(url.as_bytes());
        let mut hash = [0u8; 32];
        hasher.finalize_variable(&mut hash).expect("correct length");
        b.join(format!("pin.{}", hex::encode(hash)))
    })
}

/// Verify the repository identity against a locally pinned fingerprint,
/// or create a new pin on first use (TOFU).
///
/// - If no pin file exists: write one (trust on first use).
/// - If pin file exists and fingerprint matches: OK.
/// - If pin file exists and fingerprint differs: error (unless `trust` is true).
/// - If `trust` is true: overwrite any existing pin.
/// - If cache dir is unavailable: warn and skip (don't block the operation).
pub fn verify_or_pin(
    url: &str,
    repo_id: &[u8],
    chunk_id_key: &[u8; 32],
    cache_dir: Option<&Path>,
    trust: bool,
) -> Result<()> {
    let Some(path) = pin_file_path(url, cache_dir) else {
        tracing::warn!("cache directory unavailable; skipping repository identity verification");
        return Ok(());
    };

    let fingerprint = compute_fingerprint(repo_id, chunk_id_key);
    let fp_hex = hex::encode(fingerprint);

    if trust {
        return write_pin(&path, url, repo_id, &fp_hex);
    }

    match std::fs::read_to_string(&path) {
        Ok(contents) => {
            let pin: PinFile = serde_json::from_str(&contents).map_err(|e| {
                VykarError::Other(format!(
                    "corrupt identity pin file at {}: {e}",
                    path.display()
                ))
            })?;
            if pin.fingerprint == fp_hex {
                Ok(())
            } else {
                Err(VykarError::RepositoryMismatch(format!(
                    "expected fingerprint {}… but got {}… for repository at '{}'. \
                     If this is expected (e.g. after re-init), re-run with --trust-repo to re-pin.",
                    &pin.fingerprint[..16],
                    &fp_hex[..16],
                    url,
                )))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // TOFU: first use — pin the current identity.
            write_pin(&path, url, repo_id, &fp_hex)
        }
        Err(e) => Err(VykarError::Other(format!(
            "could not read identity pin file at {}: {e}. \
             Fix the file permissions or remove the pin file to re-establish trust.",
            path.display()
        ))),
    }
}

/// Atomically write a pin file via temp-file + rename.
fn write_pin(path: &Path, url: &str, repo_id: &[u8], fp_hex: &str) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if let Err(e) = std::fs::create_dir_all(parent) {
        tracing::warn!("could not create cache directory {}: {e}", parent.display());
        return Ok(());
    }

    let pin = PinFile {
        version: 1,
        repo_id: hex::encode(repo_id),
        fingerprint: fp_hex.to_string(),
        url: url.to_string(),
    };
    let json = serde_json::to_string_pretty(&pin)
        .map_err(|e| VykarError::Other(format!("failed to serialize pin: {e}")))?;

    let mut tmp = match tempfile::NamedTempFile::new_in(parent) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("could not create temp file for identity pin: {e}");
            return Ok(());
        }
    };
    if let Err(e) = tmp.write_all(json.as_bytes()) {
        tracing::warn!("could not write identity pin: {e}");
        return Ok(());
    }
    if let Err(e) = tmp.persist(path) {
        tracing::warn!("could not persist identity pin to {}: {e}", path.display());
        return Ok(());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_repo_id() -> Vec<u8> {
        vec![0xAA; 32]
    }

    fn test_chunk_id_key() -> [u8; 32] {
        [0xBB; 32]
    }

    #[test]
    fn fingerprint_deterministic() {
        let a = compute_fingerprint(&test_repo_id(), &test_chunk_id_key());
        let b = compute_fingerprint(&test_repo_id(), &test_chunk_id_key());
        assert_eq!(a, b);
    }

    #[test]
    fn fingerprint_differs_on_repo_id() {
        let a = compute_fingerprint(&[0xAA; 32], &test_chunk_id_key());
        let b = compute_fingerprint(&[0xCC; 32], &test_chunk_id_key());
        assert_ne!(a, b);
    }

    #[test]
    fn fingerprint_differs_on_key() {
        let a = compute_fingerprint(&test_repo_id(), &[0xBB; 32]);
        let b = compute_fingerprint(&test_repo_id(), &[0xDD; 32]);
        assert_ne!(a, b);
    }

    #[test]
    fn first_call_creates_pin() {
        let dir = tempfile::tempdir().unwrap();
        let result = verify_or_pin(
            "file:///tmp/test-repo",
            &test_repo_id(),
            &test_chunk_id_key(),
            Some(dir.path()),
            false,
        );
        assert!(result.is_ok());
        // Verify pin file was created
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("pin."))
            .collect();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn same_fingerprint_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let url = "file:///tmp/test-repo";
        verify_or_pin(
            url,
            &test_repo_id(),
            &test_chunk_id_key(),
            Some(dir.path()),
            false,
        )
        .unwrap();
        let result = verify_or_pin(
            url,
            &test_repo_id(),
            &test_chunk_id_key(),
            Some(dir.path()),
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn different_fingerprint_fails() {
        let dir = tempfile::tempdir().unwrap();
        let url = "file:///tmp/test-repo";
        verify_or_pin(
            url,
            &test_repo_id(),
            &test_chunk_id_key(),
            Some(dir.path()),
            false,
        )
        .unwrap();
        let other_key = [0xDD; 32];
        let result = verify_or_pin(url, &test_repo_id(), &other_key, Some(dir.path()), false);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("identity mismatch"), "got: {err}");
        assert!(err.contains("--trust-repo"), "got: {err}");
    }

    #[test]
    fn trust_overwrites_pin() {
        let dir = tempfile::tempdir().unwrap();
        let url = "file:///tmp/test-repo";
        verify_or_pin(
            url,
            &test_repo_id(),
            &test_chunk_id_key(),
            Some(dir.path()),
            false,
        )
        .unwrap();
        let other_key = [0xDD; 32];
        // With trust=true, the mismatch is accepted and re-pinned
        let result = verify_or_pin(url, &test_repo_id(), &other_key, Some(dir.path()), true);
        assert!(result.is_ok());
        // Subsequent call with new key succeeds
        let result = verify_or_pin(url, &test_repo_id(), &other_key, Some(dir.path()), false);
        assert!(result.is_ok());
    }

    #[test]
    fn missing_cache_dir_degrades_gracefully() {
        let result = verify_or_pin(
            "file:///tmp/test-repo",
            &test_repo_id(),
            &test_chunk_id_key(),
            None::<&Path>,
            false,
        );
        // Should succeed (skip verification) when no cache dir — but only if
        // the platform has no default cache dir. On most dev machines it does,
        // so we just check it doesn't error.
        assert!(result.is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_pin_fails_closed() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let url = "file:///tmp/test-repo";
        // Create a valid pin first
        verify_or_pin(
            url,
            &test_repo_id(),
            &test_chunk_id_key(),
            Some(dir.path()),
            false,
        )
        .unwrap();
        // Find the pin file and make it unreadable
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_string_lossy().starts_with("pin.") {
                std::fs::set_permissions(entry.path(), std::fs::Permissions::from_mode(0o000))
                    .unwrap();
            }
        }
        // Verification must fail, not silently skip
        let result = verify_or_pin(
            url,
            &test_repo_id(),
            &test_chunk_id_key(),
            Some(dir.path()),
            false,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("could not read"), "got: {err}");
        // Restore permissions so tempdir cleanup works
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_string_lossy().starts_with("pin.") {
                std::fs::set_permissions(entry.path(), std::fs::Permissions::from_mode(0o644))
                    .unwrap();
            }
        }
    }

    #[test]
    fn different_urls_independent() {
        let dir = tempfile::tempdir().unwrap();
        let key_a = [0xAA; 32];
        let key_b = [0xBB; 32];
        verify_or_pin(
            "s3://bucket-a",
            &test_repo_id(),
            &key_a,
            Some(dir.path()),
            false,
        )
        .unwrap();
        verify_or_pin(
            "s3://bucket-b",
            &test_repo_id(),
            &key_b,
            Some(dir.path()),
            false,
        )
        .unwrap();
        // Both survive independently
        assert!(verify_or_pin(
            "s3://bucket-a",
            &test_repo_id(),
            &key_a,
            Some(dir.path()),
            false
        )
        .is_ok());
        assert!(verify_or_pin(
            "s3://bucket-b",
            &test_repo_id(),
            &key_b,
            Some(dir.path()),
            false
        )
        .is_ok());
    }
}
