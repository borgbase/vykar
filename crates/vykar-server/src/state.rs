use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use chrono::{DateTime, Utc};

use crate::config::ServerSection;
use crate::quota::{self, QuotaState};

/// Shared application state, wrapped in Arc for axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<AppStateInner>,
}

pub struct AppStateInner {
    pub config: ServerSection,
    pub data_dir: PathBuf,
    /// Quota usage in bytes.
    pub quota_usage: AtomicU64,
    /// Auto-detected or explicit quota state.
    pub quota_state: Arc<QuotaState>,

    /// Last backup timestamp (updated on manifest PUT).
    pub last_backup_at: RwLock<Option<DateTime<Utc>>>,

    /// Active locks: lock_id -> LockInfo
    pub locks: RwLock<HashMap<String, LockInfo>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LockInfo {
    pub hostname: String,
    pub pid: u64,
    pub acquired_at: DateTime<Utc>,
    pub ttl_seconds: u64,
}

impl LockInfo {
    pub fn is_expired(&self) -> bool {
        let elapsed = Utc::now()
            .signed_duration_since(self.acquired_at)
            .num_seconds();
        let ttl_i64 = i64::try_from(self.ttl_seconds).unwrap_or(i64::MAX);
        elapsed > ttl_i64
    }
}

pub(crate) fn read_unpoisoned<'a, T>(
    lock: &'a RwLock<T>,
    lock_name: &'static str,
) -> RwLockReadGuard<'a, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::error!(
                lock = lock_name,
                "rwlock poisoned; continuing with inner state"
            );
            poisoned.into_inner()
        }
    }
}

pub(crate) fn write_unpoisoned<'a, T>(
    lock: &'a RwLock<T>,
    lock_name: &'static str,
) -> RwLockWriteGuard<'a, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::error!(
                lock = lock_name,
                "rwlock poisoned; continuing with inner state"
            );
            poisoned.into_inner()
        }
    }
}

/// Return the names of any top-level entries in `data_dir` that are not part
/// of a valid vykar repository layout. An empty or non-existent directory is fine.
/// Server-created temp files (`.tmp.*`) are tolerated — they come from interrupted PUTs.
pub(crate) fn unexpected_entries(data_dir: &Path) -> Vec<String> {
    let mut bad = Vec::new();
    if let Ok(entries) = std::fs::read_dir(data_dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                let known = vykar_protocol::KNOWN_ROOT_FILES.contains(&name)
                    || vykar_protocol::KNOWN_ROOT_DIRS.contains(&name)
                    || vykar_protocol::is_temp_file(name);
                if !known {
                    bad.push(name.to_string());
                }
            }
        }
    }
    bad.sort();
    bad
}

impl AppState {
    pub fn new(config: ServerSection, explicit_quota: Option<u64>) -> Self {
        let configured_data_dir = PathBuf::from(&config.data_dir);
        let data_dir = configured_data_dir
            .canonicalize()
            .unwrap_or(configured_data_dir);

        let bad = unexpected_entries(&data_dir);
        if !bad.is_empty() {
            eprintln!(
                "Error: data directory '{}' contains unexpected entries: {}",
                data_dir.display(),
                bad.join(", ")
            );
            eprintln!(
                "The data directory must contain only repository files. \
                 Remove unrelated files or choose a different --data-dir."
            );
            std::process::exit(1);
        }

        // Initialize quota usage by scanning data_dir
        let quota_usage = dir_size(&data_dir);

        // Detect quota
        let (source, limit) = quota::detect_quota(&data_dir, explicit_quota, quota_usage);
        quota::log_quota(source, limit);
        let is_explicit = matches!(explicit_quota, Some(v) if v > 0);
        let quota_state = QuotaState::new(source, limit, is_explicit, data_dir.clone());

        Self {
            inner: Arc::new(AppStateInner {
                config,
                data_dir,
                quota_usage: AtomicU64::new(quota_usage),
                quota_state,
                last_backup_at: RwLock::new(None),
                locks: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Build state with a pre-constructed `QuotaState`, skipping auto-detection.
    /// Used by tests to get deterministic behavior.
    #[cfg(test)]
    pub fn new_with_quota(config: ServerSection, quota_state: Arc<QuotaState>) -> Self {
        let configured_data_dir = PathBuf::from(&config.data_dir);
        let data_dir = configured_data_dir
            .canonicalize()
            .unwrap_or(configured_data_dir);

        let bad = unexpected_entries(&data_dir);
        if !bad.is_empty() {
            eprintln!(
                "Error: data directory '{}' contains unexpected entries: {}",
                data_dir.display(),
                bad.join(", ")
            );
            eprintln!(
                "The data directory must contain only repository files. \
                 Remove unrelated files or choose a different --data-dir."
            );
            std::process::exit(1);
        }

        let quota_usage = dir_size(&data_dir);

        Self {
            inner: Arc::new(AppStateInner {
                config,
                data_dir,
                quota_usage: AtomicU64::new(quota_usage),
                quota_state,
                last_backup_at: RwLock::new(None),
                locks: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Resolve a full file path within the repo, ensuring it stays within data_dir.
    pub fn file_path(&self, key: &str) -> Option<PathBuf> {
        if !is_valid_storage_key(key) {
            return None;
        }
        let trimmed = key.trim_matches('/');
        let path = if trimmed.is_empty() {
            self.inner.data_dir.clone()
        } else {
            self.inner.data_dir.join(trimmed)
        };
        if !path.starts_with(&self.inner.data_dir) {
            return None;
        }
        if !existing_ancestor_within(&path, &self.inner.data_dir) {
            return None;
        }
        Some(path)
    }

    /// Lenient path resolution for cleanup/deletion. Performs path-traversal safety
    /// checks but does NOT enforce the strict repo key schema, allowing deletion of
    /// `.tmp.*` leftover files from interrupted PUTs.
    pub fn file_path_for_cleanup(&self, key: &str) -> Option<PathBuf> {
        if !is_safe_relative_path(key) {
            return None;
        }
        let trimmed = key.trim_matches('/');
        if trimmed.is_empty() {
            return None;
        }
        let path = self.inner.data_dir.join(trimmed);
        if !path.starts_with(&self.inner.data_dir) {
            return None;
        }
        if !existing_ancestor_within(&path, &self.inner.data_dir) {
            return None;
        }
        Some(path)
    }

    /// Get current effective quota limit in bytes. 0 = unlimited.
    pub fn quota_limit(&self) -> u64 {
        self.inner.quota_state.limit()
    }

    /// Get current quota usage.
    pub fn quota_used(&self) -> u64 {
        self.inner.quota_usage.load(Ordering::Relaxed)
    }

    /// Update quota usage after a write.
    pub fn add_quota_usage(&self, bytes: u64) {
        self.inner.quota_usage.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Update quota usage after a delete.
    pub fn sub_quota_usage(&self, bytes: u64) {
        // Use fetch_update for saturating subtraction
        let _ =
            self.inner
                .quota_usage
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(bytes))
                });
    }

    /// Record that a manifest was written (backup completed).
    pub fn record_backup(&self) {
        let mut ts = write_unpoisoned(&self.inner.last_backup_at, "last_backup_at");
        *ts = Some(Utc::now());

        // Refresh quota in the background (fire-and-forget).
        let qs = self.inner.quota_state.clone();
        let usage = self.quota_used();
        tokio::task::spawn_blocking(move || qs.refresh(usage));
    }
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else if let Ok(meta) = p.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

/// Path-traversal safety check without schema enforcement. Rejects null bytes,
/// backslashes, empty segments, `.` and `..` — but allows any filename.
fn is_safe_relative_path(key: &str) -> bool {
    if key.contains('\0') || key.contains('\\') {
        return false;
    }
    let trimmed = key.trim_matches('/');
    if trimmed.is_empty() {
        return true;
    }
    !trimmed
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
}

fn is_valid_storage_key(key: &str) -> bool {
    if key.contains('\0') || key.contains('\\') {
        return false;
    }
    let trimmed = key.trim_matches('/');
    if trimmed.is_empty() {
        return true;
    }
    let parts: Vec<&str> = trimmed.split('/').collect();
    if parts
        .iter()
        .any(|part| part.is_empty() || *part == "." || *part == "..")
    {
        return false;
    }
    // Check directory prefixes first.
    match parts[0] {
        "keys" | "snapshots" | "locks" | "sessions" | "pending_index" => {
            return (1..=2).contains(&parts.len());
        }
        "packs" => return is_valid_packs_key(&parts),
        _ => {}
    }
    if vykar_protocol::KNOWN_ROOT_FILES.contains(&parts[0]) {
        return parts.len() == 1;
    }
    false
}

fn is_valid_packs_key(parts: &[&str]) -> bool {
    if parts.is_empty() || parts[0] != "packs" {
        return false;
    }
    if parts.len() == 1 {
        return true;
    }
    let shard = parts[1];
    if shard.len() != 2 || !shard.chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    if parts.len() == 2 {
        return true;
    }
    if parts.len() == 3 {
        let pack = parts[2];
        return pack.len() == 64 && pack.chars().all(|c| c.is_ascii_hexdigit());
    }
    false
}

fn existing_ancestor_within(path: &Path, base: &Path) -> bool {
    let mut cursor = Some(path);
    while let Some(candidate) = cursor {
        if candidate.exists() {
            return candidate
                .canonicalize()
                .map(|canon| canon.starts_with(base))
                .unwrap_or(false);
        }
        cursor = candidate.parent();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_single_file_keys() {
        assert!(is_valid_storage_key("config"));
        assert!(is_valid_storage_key("manifest"));
        assert!(is_valid_storage_key("index"));
    }

    #[test]
    fn valid_directory_keys() {
        assert!(is_valid_storage_key("sessions/abc123.json"));
        // Per-session pending index journal (co-located with session marker)
        assert!(is_valid_storage_key("sessions/abc123.index"));
        // Legacy pending_index directory (backward compat)
        assert!(is_valid_storage_key("pending_index/session123"));
    }

    #[test]
    fn rejects_unknown_top_level_keys() {
        assert!(!is_valid_storage_key("unknown"));
    }
}
