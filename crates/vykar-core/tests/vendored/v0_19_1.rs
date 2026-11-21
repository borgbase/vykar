//! `config` types of vykar v0.19.1 (commit
//! 2cc94ebc3d35a00da7679e34758c11841b346bf5), the last release that writes
//! format v2.
//!
//! Copied verbatim; do not edit. Each `BEGIN`/`END` block must diff against
//! `git show v0.19.1:<path> | sed -n '<range>p'` with only visibility and `use`
//! lines differing. `open_gate` reproduces the release's decode-and-gate
//! sequence and is the only code written for the test.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// BEGIN v0.19.1 crates/vykar-core/src/repo/mod.rs:35-47
/// Persisted (unencrypted) at the `config` key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    pub version: u32,
    pub id: Vec<u8>, // 32 bytes
    pub chunker_params: ChunkerConfig,
    pub encryption: EncryptionMode,
    pub created: DateTime<Utc>,
    #[serde(default = "default_min_pack_size")]
    pub min_pack_size: u32,
    #[serde(default = "default_max_pack_size")]
    pub max_pack_size: u32,
}
// END

// BEGIN v0.19.1 crates/vykar-core/src/repo/mod.rs:99-104
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionMode {
    None,
    Aes256Gcm,
    Chacha20Poly1305,
}
// END

// BEGIN v0.19.1 crates/vykar-core/src/config/types.rs:311-319
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkerConfig {
    #[serde(default = "default_min_size")]
    pub min_size: u32,
    #[serde(default = "default_avg_size")]
    pub avg_size: u32,
    #[serde(default = "default_max_size")]
    pub max_size: u32,
}
// END

// BEGIN v0.19.1 crates/vykar-core/src/config/defaults.rs:9-19
fn default_min_size() -> u32 {
    512 * 1024 // 512 KiB
}

fn default_avg_size() -> u32 {
    2 * 1024 * 1024 // 2 MiB
}

fn default_max_size() -> u32 {
    8 * 1024 * 1024 // 8 MiB
}
// END

// BEGIN v0.19.1 crates/vykar-core/src/config/defaults.rs:29-35
fn default_min_pack_size() -> u32 {
    32 * 1024 * 1024 // 32 MiB
}

fn default_max_pack_size() -> u32 {
    192 * 1024 * 1024 // 192 MiB
}
// END

/// `Repository::open` (open.rs:217-225) at v0.19.1: decode, then
/// `version != 2`.
pub fn open_gate(bytes: &[u8]) -> Result<RepoConfig, super::Refusal> {
    let config: RepoConfig =
        rmp_serde::from_slice(bytes).map_err(super::Refusal::Deserialization)?;
    if config.version != 2 {
        return Err(super::Refusal::UnsupportedVersion(config.version));
    }
    Ok(config)
}
