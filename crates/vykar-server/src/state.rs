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
    /// Committed quota usage in bytes (bytes actually on disk).
    pub quota_usage: AtomicU64,
    /// In-flight reserved bytes for uploads/repacks not yet committed. Kept
    /// separate from `quota_usage` so a background rescan of committed usage
    /// (`rescan_usage`) never clobbers concurrent reservations.
    pub quota_reserved: AtomicU64,
    /// Auto-detected or explicit quota state.
    pub quota_state: Arc<QuotaState>,

    /// Last backup timestamp (updated on new snapshot write).
    pub last_backup_at: RwLock<Option<DateTime<Utc>>>,
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
                quota_reserved: AtomicU64::new(0),
                quota_state,
                last_backup_at: RwLock::new(None),
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
                quota_reserved: AtomicU64::new(0),
                quota_state,
                last_backup_at: RwLock::new(None),
            }),
        }
    }

    /// Resolve a full file path within the repo, ensuring it stays within `data_dir`.
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

    /// Get current committed quota usage.
    pub fn quota_used(&self) -> u64 {
        self.inner.quota_usage.load(Ordering::Relaxed)
    }

    /// Get current in-flight reserved bytes (uploads/repacks not yet committed).
    #[cfg(test)]
    pub fn quota_reserved(&self) -> u64 {
        self.inner.quota_reserved.load(Ordering::Relaxed)
    }

    /// Reserve `bytes` of quota headroom for an in-flight write.
    ///
    /// The predicate is `limit == 0 || used + reserved_after <= limit`, where
    /// `reserved_after` is the running total of all outstanding reservations
    /// plus `bytes`. On success returns an RAII [`QuotaReservation`] whose
    /// `Drop` releases any bytes not committed — so every failure path (write
    /// error, Content-Length mismatch, checksum mismatch, rename failure,
    /// panic) releases automatically without per-site cleanup.
    ///
    /// # Errors
    ///
    /// Returns `Err((used, limit))` when the reservation would exceed the quota,
    /// for composing a 413 response.
    ///
    /// Known residual: two concurrent same-key overwrites both reserve their
    /// full net size, so committed usage can transiently over-count until the
    /// next `rescan_usage()`. Over-counting is the safe direction (rejects
    /// rather than admits).
    pub fn try_reserve_quota(&self, bytes: u64) -> Result<QuotaReservation, (u64, u64)> {
        let limit = self.quota_limit();
        if limit == 0 {
            // Unlimited: track reserved bytes for accounting symmetry.
            self.inner
                .quota_reserved
                .fetch_add(bytes, Ordering::Relaxed);
            return Ok(QuotaReservation {
                state: self.clone(),
                remaining: bytes,
            });
        }
        let used = self.quota_used();
        let result = self.inner.quota_reserved.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |reserved| {
                let after = reserved.checked_add(bytes)?;
                (used.saturating_add(after) <= limit).then_some(after)
            },
        );
        match result {
            Ok(_) => Ok(QuotaReservation {
                state: self.clone(),
                remaining: bytes,
            }),
            Err(_) => Err((used, limit)),
        }
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

        // Rescan committed usage and refresh quota in the background
        // (fire-and-forget). Corrects any drift accumulated from the
        // reservation fast-path (e.g. concurrent same-key overwrites).
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || rescan_usage(&inner));
    }
}

/// RAII quota reservation: holds bytes reserved in `quota_reserved` and, on
/// `Drop`, releases whatever has not been committed. Grown or committed as an
/// upload streams; see [`AppState::try_reserve_quota`].
pub struct QuotaReservation {
    state: AppState,
    /// Reserved bytes not yet committed to `quota_usage`.
    remaining: u64,
}

impl QuotaReservation {
    /// Reserved bytes not yet committed.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Reserve `bytes` more headroom (streaming uploads with no Content-Length).
    ///
    /// # Errors
    ///
    /// Returns `Err((used, limit))` when growing would exceed the quota.
    pub fn grow(&mut self, bytes: u64) -> Result<(), (u64, u64)> {
        let limit = self.state.quota_limit();
        if limit == 0 {
            self.state
                .inner
                .quota_reserved
                .fetch_add(bytes, Ordering::Relaxed);
            self.remaining = self.remaining.saturating_add(bytes);
            return Ok(());
        }
        let used = self.state.quota_used();
        let result = self.state.inner.quota_reserved.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |reserved| {
                let after = reserved.checked_add(bytes)?;
                (used.saturating_add(after) <= limit).then_some(after)
            },
        );
        match result {
            Ok(_) => {
                self.remaining = self.remaining.saturating_add(bytes);
                Ok(())
            }
            Err(_) => Err((used, limit)),
        }
    }

    /// Commit `bytes` from the reservation into committed usage. Used for
    /// per-operation commits (repack) where the reservation outlives one unit.
    ///
    /// Ordering invariant: add to `quota_usage` *before* releasing from
    /// `quota_reserved`. The in-between state double-counts the bytes, so a
    /// concurrent reservation can only be falsely rejected (safe direction).
    /// The reverse order would leave the bytes invisible to both counters,
    /// letting a concurrent reservation overrun the limit.
    pub fn commit_partial(&mut self, bytes: u64) {
        let bytes = bytes.min(self.remaining);
        self.state.add_quota_usage(bytes);
        self.release_reserved(bytes);
        self.remaining -= bytes;
    }

    /// Commit all remaining reserved bytes into committed usage.
    /// Same ordering invariant as [`Self::commit_partial`].
    pub fn commit(mut self) {
        let bytes = self.remaining;
        self.state.add_quota_usage(bytes);
        self.release_reserved(bytes);
        self.remaining = 0;
    }

    /// Release `bytes` from `quota_reserved`, saturating at zero.
    fn release_reserved(&self, bytes: u64) {
        let _ = self.state.inner.quota_reserved.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |reserved| Some(reserved.saturating_sub(bytes)),
        );
    }
}

impl Drop for QuotaReservation {
    fn drop(&mut self) {
        if self.remaining > 0 {
            self.release_reserved(self.remaining);
            self.remaining = 0;
        }
    }
}

/// Rescan committed usage from disk and refresh the quota limit. **Blocking** —
/// call via `spawn_blocking`. In-flight reservations live in `quota_reserved`,
/// so overwriting `quota_usage` here cannot clobber them.
pub(crate) fn rescan_usage(inner: &AppStateInner) {
    let usage = dir_size(&inner.data_dir);
    inner.quota_usage.store(usage, Ordering::Relaxed);
    inner.quota_state.refresh(usage);
}

/// Fsync a directory so renames/creations inside it survive power loss.
/// No-op on non-Unix (`std::fs::File` cannot open directories on Windows).
pub(crate) fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
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
    // Temp-file names are never valid committed keys. Append-only mode allows
    // deleting temp-named files (uncommitted upload debris); if PUT could
    // commit an object under a temp-looking name (e.g. `snapshots/.tmp.x.1` —
    // the namespaces below accept any basename), that object would be
    // deletable in append-only mode, breaking its guarantee.
    if vykar_protocol::is_temp_file(key) {
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
    let Some(first) = parts.first() else {
        return false;
    };
    match *first {
        "keys" | "snapshots" | "locks" | "sessions" | "pending_index" => {
            return (1..=2).contains(&parts.len());
        }
        "packs" => return is_valid_packs_key(&parts),
        _ => {}
    }
    if vykar_protocol::KNOWN_ROOT_FILES.contains(first) {
        return parts.len() == 1;
    }
    false
}

fn is_valid_packs_key(parts: &[&str]) -> bool {
    let Some(first) = parts.first() else {
        return false;
    };
    if *first != "packs" {
        return false;
    }
    if parts.len() == 1 {
        return true;
    }
    let Some(shard) = parts.get(1) else {
        return false;
    };
    if shard.len() != 2 || !shard.chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    if parts.len() == 2 {
        return true;
    }
    if parts.len() == 3 {
        let Some(pack) = parts.get(2) else {
            return false;
        };
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
                .is_ok_and(|canon| canon.starts_with(base));
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
        assert!(is_valid_storage_key("index"));
        assert!(is_valid_storage_key("index.gen"));
        assert!(is_valid_storage_key("manifest"));
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

    #[test]
    fn rejects_temp_named_keys() {
        // Temp names must never be valid committed keys: append-only mode
        // allows deleting them, so a PUT-able temp-named object would be
        // deletable committed data.
        assert!(!is_valid_storage_key("snapshots/.tmp.evil.1"));
        assert!(!is_valid_storage_key("keys/.tmp.repokey.0"));
        assert!(!is_valid_storage_key("snapshots/.repack_tmp.3"));
        assert!(!is_valid_storage_key(".tmp.config.0"));
        // Trailing slashes are trimmed during path resolution, so they must
        // not smuggle a temp name past this check.
        assert!(!is_valid_storage_key("snapshots/.tmp.evil.1/"));
    }

    fn test_state(quota: u64) -> (AppState, tempfile::TempDir) {
        use crate::config::ServerSection;
        use crate::quota::{QuotaSource, QuotaState};

        let tmp = tempfile::tempdir().expect("create tempdir");
        let data_dir = tmp.path().to_path_buf();
        let (source, limit) = if quota > 0 {
            (QuotaSource::Explicit, quota)
        } else {
            (QuotaSource::Unlimited, 0)
        };
        let quota_state = QuotaState::new(source, limit, true, data_dir.clone());
        let config = ServerSection {
            data_dir: data_dir.to_string_lossy().into_owned(),
            token: "t".to_string(),
            ..Default::default()
        };
        (AppState::new_with_quota(config, quota_state), tmp)
    }

    #[cfg(unix)]
    #[test]
    fn fsync_dir_ok_on_existing_err_on_missing() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        assert!(
            fsync_dir(tmp.path()).is_ok(),
            "fsync of existing dir succeeds"
        );
        assert!(
            fsync_dir(&tmp.path().join("nope")).is_err(),
            "fsync of missing dir errors"
        );
    }

    #[test]
    fn rescan_usage_corrects_drift() {
        let (state, tmp) = test_state(0);
        // Put a file of known size on disk (bypassing the write path).
        std::fs::write(tmp.path().join("config"), vec![0u8; 4096]).unwrap();

        // Seed drift: usage reads far higher than what is actually on disk.
        state.add_quota_usage(9999);
        assert_eq!(state.quota_used(), 9999);

        rescan_usage(&state.inner);
        assert_eq!(state.quota_used(), 4096, "usage should match on-disk size");
    }

    #[test]
    fn reservation_release_on_drop() {
        let (state, _tmp) = test_state(1000);
        {
            let _res = state.try_reserve_quota(600).expect("reserve 600");
            assert_eq!(state.quota_reserved(), 600);
        }
        assert_eq!(state.quota_reserved(), 0, "drop releases reservation");
        assert_eq!(state.quota_used(), 0, "drop does not commit");
    }

    #[test]
    fn reservation_commit_moves_to_usage() {
        let (state, _tmp) = test_state(1000);
        let res = state.try_reserve_quota(600).expect("reserve 600");
        res.commit();
        assert_eq!(state.quota_reserved(), 0);
        assert_eq!(state.quota_used(), 600);
    }

    #[test]
    fn reservation_rejects_over_limit() {
        let (state, _tmp) = test_state(1000);
        let _res = state.try_reserve_quota(700).expect("reserve 700");
        let Err(err) = state.try_reserve_quota(700) else {
            panic!("second reservation should exceed limit");
        };
        assert_eq!(err, (0, 1000), "second reservation exceeds limit");
        assert_eq!(state.quota_reserved(), 700, "failed reserve adds nothing");
    }
}
