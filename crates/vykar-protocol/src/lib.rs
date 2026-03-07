//! Shared wire-format types and constants for vykar client ↔ server communication.
//!
//! This crate is intentionally minimal: DTOs, pack format constants, protocol
//! versioning, and transport-level validation. No storage I/O, no crypto.

use serde::{Deserialize, Serialize};

// ── Pack format constants ──────────────────────────────────────────────────

/// Magic bytes at the start of every pack file.
pub const PACK_MAGIC: &[u8; 8] = b"VGERPACK";

/// Size of the pack header (magic + version byte).
pub const PACK_HEADER_SIZE: usize = 9;

/// Version byte written into new packs by this binary.
pub const PACK_VERSION_CURRENT: u8 = 1;

/// Oldest pack version this binary can read.
///
/// A repo contains packs from many backup runs — bumping `PACK_VERSION_CURRENT`
/// to 2 must not break reading existing v1 packs. Bump MIN only when a version
/// is truly retired (requires a migration).
pub const PACK_VERSION_MIN: u8 = 1;

/// Newest pack version this binary understands.
///
/// Always == `PACK_VERSION_CURRENT` (we can read anything we can write).
pub const PACK_VERSION_MAX: u8 = PACK_VERSION_CURRENT;

// ── Protocol version ───────────────────────────────────────────────────────

/// Current protocol version. Sent by clients in requests.
pub const PROTOCOL_VERSION: u32 = 1;

/// Minimum protocol version the server accepts.
///
/// Bump this when a new version introduces breaking semantic changes
/// that make older request formats unsafe to process.
pub const MIN_PROTOCOL_VERSION: u32 = 1;

/// Validate a request's protocol version. Returns `Err(message)` if incompatible.
///
/// Compatibility contract:
/// - `version == 0` → legacy client (pre-versioning). Accepted while
///   `MIN_PROTOCOL_VERSION == 1`. When a future breaking change bumps MIN to 2,
///   legacy clients are rejected.
/// - `version < MIN_PROTOCOL_VERSION` (and != 0) → client too old, reject
/// - `version > PROTOCOL_VERSION` → client too new, reject
/// - `MIN_PROTOCOL_VERSION <= version <= PROTOCOL_VERSION` → accepted
pub fn check_protocol_version(version: u32) -> Result<(), String> {
    if version == 0 {
        // Legacy client (pre-versioning). Accept while MIN == 1.
        if MIN_PROTOCOL_VERSION > 1 {
            return Err(format!(
                "legacy client (no protocol version); server requires >= {MIN_PROTOCOL_VERSION}"
            ));
        }
        return Ok(());
    }
    if version < MIN_PROTOCOL_VERSION {
        return Err(format!(
            "protocol version {version} too old; server requires >= {MIN_PROTOCOL_VERSION}"
        ));
    }
    if version > PROTOCOL_VERSION {
        return Err(format!(
            "protocol version {version} not supported; server supports <= {PROTOCOL_VERSION}"
        ));
    }
    Ok(())
}

// ── Repack wire types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackBlobRef {
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackOperationRequest {
    pub source_pack: String,
    pub keep_blobs: Vec<RepackBlobRef>,
    pub delete_after: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackPlanRequest {
    pub operations: Vec<RepackOperationRequest>,
    #[serde(default)]
    pub protocol_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackOperationResult {
    pub source_pack: String,
    pub new_pack: Option<String>,
    pub new_offsets: Vec<u64>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackResultResponse {
    pub completed: Vec<RepackOperationResult>,
}

// ── Verify-packs wire types ────────────────────────────────────────────────

/// A single blob expected at a given offset+length in a pack.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyBlobRef {
    pub offset: u64,
    pub length: u64,
}

/// Request to verify a single pack file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyPackRequest {
    pub pack_key: String,
    /// Estimated on-disk size of the pack (used for server-side rate limiting).
    #[serde(default)]
    pub expected_size: u64,
    pub expected_blobs: Vec<VerifyBlobRef>,
}

/// Batch request to verify multiple packs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyPacksPlanRequest {
    pub packs: Vec<VerifyPackRequest>,
    #[serde(default)]
    pub protocol_version: u32,
}

/// Result of verifying a single pack file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyPackResult {
    pub pack_key: String,
    pub hash_valid: bool,
    pub header_valid: bool,
    pub blobs_valid: bool,
    pub error: Option<String>,
}

/// Batch response from verify-packs endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyPacksResponse {
    pub results: Vec<VerifyPackResult>,
    /// True when the server stopped early (e.g. byte-volume cap reached).
    /// The client should re-queue unprocessed packs in a subsequent batch.
    #[serde(default)]
    pub truncated: bool,
}

// ── Repository layout ─────────────────────────────────────────────────────

/// Top-level file entries that can appear in a vykar repository root.
/// "manifest" is a v1 legacy artifact — remove once v1 clients are retired.
pub const KNOWN_ROOT_FILES: &[&str] = &["config", "index", "index.gen", "manifest"];

/// Top-level directory entries that can appear in a vykar repository root.
pub const KNOWN_ROOT_DIRS: &[&str] = &[
    "keys",
    "snapshots",
    "packs",
    "locks",
    "sessions",
    "pending_index",
];

/// Returns true if `name` matches the server temp-file naming convention
/// (`.tmp.{target}.{unique_id}`), used for atomic writes.
pub fn is_temp_file(name: &str) -> bool {
    let basename = name.rsplit('/').next().unwrap_or(name);
    basename.starts_with(".tmp.")
}

/// Returns true if `key` is a known vykar repository storage key.
///
/// Matches root files, directory-prefixed paths, and `.tmp.*` temp files.
pub fn is_known_repo_key(key: &str) -> bool {
    KNOWN_ROOT_FILES.contains(&key)
        || KNOWN_ROOT_DIRS
            .iter()
            .any(|d| key.starts_with(d) && key.as_bytes().get(d.len()) == Some(&b'/'))
        || is_temp_file(key)
}

// ── Transport-level validation ─────────────────────────────────────────────

/// Validate a pack storage key: must be `packs/<2-hex-shard>/<64-hex-id>`.
pub fn is_valid_pack_key(key: &str) -> bool {
    let parts: Vec<&str> = key.trim_matches('/').split('/').collect();
    if parts.len() != 3 || parts[0] != "packs" {
        return false;
    }
    parts[1].len() == 2
        && parts[1].chars().all(|c| c.is_ascii_hexdigit())
        && parts[2].len() == 64
        && parts[2].chars().all(|c| c.is_ascii_hexdigit())
}

/// Validate a single blob reference from a wire-format request.
///
/// Returns `Ok(())` on success, `Err(message)` on failure.
/// `context` appears in error messages (e.g. "operation 3 blob 5").
pub fn validate_blob_ref(offset: u64, length: u64, context: &str) -> Result<(), String> {
    if length == 0 {
        return Err(format!("blob length must be > 0 at {context}"));
    }
    if length > u32::MAX as u64 {
        return Err(format!("blob length exceeds pack format max at {context}"));
    }
    if offset.checked_add(length).is_none() {
        return Err(format!("blob range overflow at {context}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Serde default round-trip ───────────────────────────────────────

    #[test]
    fn verify_plan_defaults_without_optional_fields() {
        let json = r#"{"packs":[]}"#;
        let plan: VerifyPacksPlanRequest = serde_json::from_str(json).unwrap();
        assert_eq!(plan.protocol_version, 0);
        assert!(plan.packs.is_empty());
    }

    #[test]
    fn verify_plan_round_trip_with_all_fields() {
        let plan = VerifyPacksPlanRequest {
            packs: vec![VerifyPackRequest {
                pack_key: "packs/ab/abcd".repeat(5),
                expected_size: 1024,
                expected_blobs: vec![VerifyBlobRef {
                    offset: 13,
                    length: 100,
                }],
            }],
            protocol_version: 1,
        };
        let json = serde_json::to_string(&plan).unwrap();
        let deser: VerifyPacksPlanRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.protocol_version, 1);
        assert_eq!(deser.packs[0].expected_size, 1024);
    }

    #[test]
    fn repack_plan_defaults_without_protocol_version() {
        let json = r#"{"operations":[]}"#;
        let plan: RepackPlanRequest = serde_json::from_str(json).unwrap();
        assert_eq!(plan.protocol_version, 0);
    }

    #[test]
    fn verify_response_defaults_without_truncated() {
        let json = r#"{"results":[]}"#;
        let resp: VerifyPacksResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.truncated);
    }

    #[test]
    fn verify_pack_request_defaults_without_expected_size() {
        let json = r#"{"pack_key":"packs/ab/cc","expected_blobs":[]}"#;
        let req: VerifyPackRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.expected_size, 0);
    }

    // ── validate_blob_ref ──────────────────────────────────────────────

    #[test]
    fn validate_blob_ref_rejects_zero_length() {
        let err = validate_blob_ref(0, 0, "test").unwrap_err();
        assert!(err.contains("length must be > 0"));
    }

    #[test]
    fn validate_blob_ref_rejects_too_large_length() {
        let err = validate_blob_ref(0, u32::MAX as u64 + 1, "test").unwrap_err();
        assert!(err.contains("exceeds pack format max"));
    }

    #[test]
    fn validate_blob_ref_rejects_overflow() {
        let err = validate_blob_ref(u64::MAX, 1, "test").unwrap_err();
        assert!(err.contains("overflow"));
    }

    #[test]
    fn validate_blob_ref_accepts_valid() {
        assert!(validate_blob_ref(13, 100, "test").is_ok());
    }

    // ── is_valid_pack_key ──────────────────────────────────────────────

    #[test]
    fn valid_pack_key_accepted() {
        let key = format!("packs/ab/{}", "a1".repeat(32));
        assert!(is_valid_pack_key(&key));
    }

    #[test]
    fn pack_key_wrong_segments_rejected() {
        assert!(!is_valid_pack_key("packs/ab"));
        assert!(!is_valid_pack_key("packs/ab/cd/ef"));
        assert!(!is_valid_pack_key("notpacks/ab/abcd"));
    }

    #[test]
    fn pack_key_wrong_shard_length_rejected() {
        let key = format!("packs/abc/{}", "a1".repeat(32));
        assert!(!is_valid_pack_key(&key));
    }

    #[test]
    fn pack_key_non_hex_rejected() {
        let key = format!("packs/ab/{}", "zz".repeat(32));
        assert!(!is_valid_pack_key(&key));
    }

    // ── check_protocol_version ─────────────────────────────────────────

    #[test]
    fn protocol_version_0_legacy_accepted() {
        assert!(check_protocol_version(0).is_ok());
    }

    #[test]
    fn protocol_version_current_accepted() {
        assert!(check_protocol_version(PROTOCOL_VERSION).is_ok());
    }

    #[test]
    fn protocol_version_too_new_rejected() {
        let err = check_protocol_version(PROTOCOL_VERSION + 1).unwrap_err();
        assert!(err.contains("not supported"));
    }

    #[test]
    fn protocol_version_max_rejected() {
        let err = check_protocol_version(u32::MAX).unwrap_err();
        assert!(err.contains("not supported"));
    }

    // ── is_known_repo_key ─────────────────────────────────────────────

    #[test]
    fn known_root_files_accepted() {
        for f in KNOWN_ROOT_FILES {
            assert!(is_known_repo_key(f), "{f} should be known");
        }
    }

    #[test]
    fn known_dir_prefixed_keys_accepted() {
        assert!(is_known_repo_key("keys/repokey"));
        assert!(is_known_repo_key("snapshots/abc123"));
        assert!(is_known_repo_key("packs/ab/deadbeef"));
        assert!(is_known_repo_key("locks/lock.json"));
        assert!(is_known_repo_key("sessions/abc123.json"));
        assert!(is_known_repo_key("pending_index/session123"));
    }

    #[test]
    fn bare_dir_names_rejected() {
        for d in KNOWN_ROOT_DIRS {
            assert!(!is_known_repo_key(d), "bare dir '{d}' should not match");
        }
    }

    #[test]
    fn unknown_keys_rejected() {
        assert!(!is_known_repo_key("random_file"));
        assert!(!is_known_repo_key("data/something"));
    }

    // ── is_temp_file ──────────────────────────────────────────────────

    #[test]
    fn temp_files_detected() {
        assert!(is_temp_file(".tmp.config.0"));
        assert!(is_temp_file("packs/ab/.tmp.deadbeef.0"));
    }

    #[test]
    fn non_temp_files_not_detected() {
        assert!(!is_temp_file("config"));
        assert!(!is_temp_file("tmp.config"));
    }
}
