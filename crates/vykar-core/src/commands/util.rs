use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::info;

use std::sync::Arc;

use crate::config::VykarConfig;
use crate::limits;
use crate::repo::lock;
use crate::repo::lock::SessionEntry;
use crate::repo::{identity, OpenOptions, Repository};
use crate::storage;
use vykar_types::error::{
    ActiveSessionDetails, ActiveSessionInfo, ActiveSessionList, Result, VykarError,
};

/// Build an [`ActiveSessionList`] from raw `list_session_entries` output.
///
/// Parseable entries are rendered with host/pid/age; entries with `None`
/// (malformed JSON) are passed through as `ActiveSessionInfo { details: None }`
/// so [`ActiveSessionList::Display`] surfaces them to the operator instead
/// of silently dropping them — the fail-closed policy required by
/// [`with_maintenance_lock`].
pub(crate) fn build_active_session_list(
    entries: Vec<(String, Option<SessionEntry>)>,
    now: &chrono::DateTime<chrono::Utc>,
) -> ActiveSessionList {
    let infos = entries
        .into_iter()
        .map(|(id, entry)| ActiveSessionInfo {
            id,
            details: entry.map(|e| ActiveSessionDetails {
                hostname: e.hostname,
                pid: e.pid,
                age: lock::format_age(now, &e.last_refresh),
            }),
        })
        .collect();
    ActiveSessionList(infos)
}

/// Extract the cache_dir override from config as a PathBuf.
pub(crate) fn cache_dir_from_config(config: &VykarConfig) -> Option<PathBuf> {
    config.cache_dir.as_deref().map(PathBuf::from)
}

/// Replace the inner detail of a `RepoNotFound` error with the actual repo URL.
pub(crate) fn enrich_repo_not_found(err: VykarError, url: &str) -> VykarError {
    match err {
        VykarError::RepoNotFound(_) => VykarError::RepoNotFound(url.to_string()),
        other => other,
    }
}

/// Verify the repository identity against the locally pinned fingerprint.
pub(crate) fn verify_repo_identity(config: &VykarConfig, repo: &Repository) -> Result<()> {
    identity::verify_or_pin(
        &config.repository.url,
        &repo.config.id,
        repo.crypto.chunk_id_key(),
        cache_dir_from_config(config).as_deref(),
        config.trust_repo,
    )
}

/// Open a repository from config using the standard backend resolver.
pub fn open_repo(
    config: &VykarConfig,
    passphrase: Option<&str>,
    opts: OpenOptions,
) -> Result<Repository> {
    let connections = config.limits.connections;
    let backend = storage::backend_from_config(&config.repository, connections)?;
    let backend = limits::wrap_storage_backend(backend, &config.limits);
    let repo = Repository::open(backend, passphrase, cache_dir_from_config(config), opts)
        .map_err(|e| enrich_repo_not_found(e, &config.repository.url))?;
    verify_repo_identity(config, &repo)?;
    Ok(repo)
}

/// Open a repo for a read-only operation, registering a session marker
/// BEFORE opening so that concurrent maintenance sees us.
/// Returns (repo, session_guard). The guard deregisters on drop.
pub fn open_repo_with_read_session(
    config: &VykarConfig,
    passphrase: Option<&str>,
    opts: OpenOptions,
) -> Result<(Repository, lock::SessionGuard)> {
    let connections = config.limits.connections;
    let backend = storage::backend_from_config(&config.repository, connections)?;
    let backend = limits::wrap_storage_backend(backend, &config.limits);

    let session_id = format!("{:032x}", rand::random::<u128>());
    lock::register_session(backend.as_ref(), &session_id)?;

    let open_result = Repository::open(backend, passphrase, cache_dir_from_config(config), opts);

    let deregister_fresh = |sid: &str| {
        if let Ok(cleanup) = storage::backend_from_config(&config.repository, 1) {
            lock::deregister_session(cleanup.as_ref(), sid);
        }
    };

    match open_result {
        Ok(repo) => {
            // Verify identity before adopting the session into a guard.
            if let Err(e) = verify_repo_identity(config, &repo) {
                deregister_fresh(&session_id);
                return Err(e);
            }
            match lock::SessionGuard::adopt(Arc::clone(&repo.storage), session_id.clone()) {
                Ok(guard) => Ok((repo, guard)),
                Err(e) => {
                    lock::deregister_session(repo.storage.as_ref(), &session_id);
                    Err(e)
                }
            }
        }
        Err(e) => {
            // Backend was consumed by open — build a fresh one just for cleanup.
            deregister_fresh(&session_id);
            Err(enrich_repo_not_found(e, &config.repository.url))
        }
    }
}

/// Open a repository and execute a mutation while holding an advisory lock.
pub fn with_open_repo_lock<T>(
    config: &VykarConfig,
    passphrase: Option<&str>,
    opts: OpenOptions,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let mut repo = open_repo(config, passphrase, opts)?;
    with_repo_lock(&mut repo, action)
}

/// Return `Err(VykarError::Interrupted)` if the shutdown flag is set.
pub fn check_interrupted(shutdown: Option<&AtomicBool>) -> Result<()> {
    if shutdown.is_some_and(|f| f.load(Ordering::Relaxed)) {
        return Err(VykarError::Interrupted);
    }
    Ok(())
}

/// Execute a repository mutation while holding an advisory lock.
/// Ensures the lock release is always attempted even when the action fails.
/// On error, performs best-effort cleanup (seals partial packs, waits for
/// in-flight uploads, writes pending_index) before releasing the lock.
pub fn with_repo_lock<T>(
    repo: &mut Repository,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let guard = lock::acquire_lock(repo.storage.as_ref())?;
    run_under_fence(repo, guard, action)
}

/// Shared epilogue for lock-guarded operations: installs a lock fence, runs
/// the action, performs best-effort cleanup on error, then releases the lock.
///
/// # Failure policy
///
/// - **Action errors are fatal.** Propagated to the caller as-is; any
///   subsequent release failure is logged via `tracing::warn!` but does not
///   replace the original error.
/// - **Release errors after a successful action are warning-only.** The
///   action has already committed to storage (e.g. the snapshot blob was
///   written), so reporting a failure would misrepresent the outcome. A
///   `tracing::warn!` fires referencing `vykar break-lock` and the 6-hour
///   stale-lock TTL; the caller receives the action's `Ok` value.
///
/// There is no progress sink here, so the release warning is tracing-only —
/// GUI consumers do not see it. Acceptable trade-off: leaked advisory locks
/// self-heal in 6 hours and `vykar break-lock` is available for immediate
/// recovery. Callers that do have a progress sink (backup's commit path)
/// surface the same warning via `BackupProgressEvent::Warning` instead.
fn run_under_fence<T>(
    repo: &mut Repository,
    guard: lock::LockGuard,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let fence = lock::build_lock_fence(&guard, Arc::clone(&repo.storage));
    repo.set_lock_fence(fence);

    let result = action(repo);

    if result.is_err() {
        repo.flush_on_abort();
    }

    repo.clear_lock_fence();
    let lock_key = guard.key().to_string();
    match lock::release_lock(repo.storage.as_ref(), guard) {
        Ok(()) => result,
        Err(release_err) => {
            if result.is_err() {
                tracing::warn!("failed to release repository lock: {release_err}");
            } else {
                tracing::warn!(
                    "operation completed successfully, but releasing the repository lock \
                     failed: {release_err}. The advisory lock at `{lock_key}` may persist; \
                     future operations on this repository may be blocked for up to 6 hours \
                     until automatic stale-lock cleanup, or run `vykar break-lock` to clear \
                     it manually."
                );
            }
            result
        }
    }
}

/// Open a repository and execute a maintenance operation while holding the lock.
///
/// Unlike `with_open_repo_lock`, this first cleans up stale sessions and
/// refuses to proceed if active (non-stale) backup sessions exist.
pub fn with_open_repo_maintenance_lock<T>(
    config: &VykarConfig,
    passphrase: Option<&str>,
    opts: OpenOptions,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let mut repo = open_repo(config, passphrase, opts)?;
    with_maintenance_lock(&mut repo, action)
}

/// Execute a maintenance operation while holding an advisory lock.
///
/// Acquires the lock, cleans up stale sessions (>45 min since last refresh),
/// then refuses to run if any non-stale sessions remain. This prevents
/// maintenance from deleting packs that active backups depend on. Malformed
/// markers (unparseable JSON or bad timestamps) are preserved and surfaced
/// in the blocking list — maintenance will not proceed past them without
/// operator intervention (`break-lock --sessions`).
pub fn with_maintenance_lock<T>(
    repo: &mut Repository,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let guard = lock::acquire_lock(repo.storage.as_ref())?;

    // Clean up stale sessions before checking for active ones.
    let stale_threshold = lock::default_stale_session_duration();
    let local_hostname = crate::platform::hostname();
    match lock::cleanup_stale_sessions(
        repo.storage.as_ref(),
        stale_threshold,
        &local_hostname,
        crate::platform::is_pid_alive,
    ) {
        Ok(cleaned) => {
            if !cleaned.is_empty() {
                info!(
                    count = cleaned.len(),
                    sessions = ?cleaned,
                    "cleaned up stale backup sessions"
                );
            }
        }
        Err(e) => {
            let _ = lock::release_lock(repo.storage.as_ref(), guard);
            return Err(VykarError::Other(format!(
                "cannot clean up stale backup sessions (storage error: {e}); \
                 refusing maintenance to avoid data loss"
            )));
        }
    }

    // Check for active (non-stale) sessions. Fail-closed: if we can't
    // list sessions, refuse to run rather than risk deleting active packs.
    // Malformed markers are surfaced in the blocking list via
    // `build_active_session_list` — they are NOT silently filtered.
    match lock::list_session_entries(repo.storage.as_ref()) {
        Ok(entries) if !entries.is_empty() => {
            let list = build_active_session_list(entries, &chrono::Utc::now());
            let _ = lock::release_lock(repo.storage.as_ref(), guard);
            return Err(VykarError::ActiveSessions(list));
        }
        Err(e) => {
            let _ = lock::release_lock(repo.storage.as_ref(), guard);
            return Err(VykarError::Other(format!(
                "cannot verify no active backup sessions (storage error: {e}); \
                 refusing maintenance to avoid data loss"
            )));
        }
        _ => {}
    }

    // Session checks passed — hand off to the shared fence/flush/release epilogue.
    run_under_fence(repo, guard, action)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(hostname: &str, pid: u32, last_refresh: &str) -> SessionEntry {
        SessionEntry {
            hostname: hostname.to_string(),
            pid,
            registered_at: last_refresh.to_string(),
            last_refresh: last_refresh.to_string(),
        }
    }

    #[test]
    fn build_list_formats_parseable_entry() {
        let now = chrono::Utc::now();
        let fifteen_min_ago = (now - chrono::Duration::minutes(15)).to_rfc3339();
        let entries = vec![(
            "sess1".to_string(),
            Some(entry("host-a", 7, &fifteen_min_ago)),
        )];

        let list = build_active_session_list(entries, &now);

        assert_eq!(list.0.len(), 1);
        let d = list.0[0].details.as_ref().expect("should be parseable");
        assert_eq!(list.0[0].id, "sess1");
        assert_eq!(d.hostname, "host-a");
        assert_eq!(d.pid, 7);
        assert!(!d.age.is_empty() && d.age != "unknown");
        assert!(!list.has_malformed());
    }

    #[test]
    fn build_list_preserves_malformed_entry() {
        let now = chrono::Utc::now();
        let entries = vec![("corrupt".to_string(), None)];

        let list = build_active_session_list(entries, &now);

        assert_eq!(list.0.len(), 1);
        assert_eq!(list.0[0].id, "corrupt");
        assert!(list.0[0].details.is_none(), "None must be preserved");
        assert!(list.has_malformed());
    }

    #[test]
    fn build_list_mixed_entries() {
        let now = chrono::Utc::now();
        let recent = (now - chrono::Duration::minutes(5)).to_rfc3339();
        let entries = vec![
            ("live".to_string(), Some(entry("host-b", 99, &recent))),
            ("corrupt".to_string(), None),
        ];

        let list = build_active_session_list(entries, &now);

        assert_eq!(list.0.len(), 2);
        assert!(list.0[0].details.is_some());
        assert!(list.0[1].details.is_none());
        assert!(list.has_malformed());
    }

    #[test]
    fn display_includes_remediation_hint() {
        let now = chrono::Utc::now();
        let recent = (now - chrono::Duration::minutes(5)).to_rfc3339();
        let list =
            build_active_session_list(vec![("s".to_string(), Some(entry("h", 1, &recent)))], &now);

        let rendered = format!("{list}");
        assert!(
            rendered.contains("break-lock --sessions"),
            "display must mention the remediation command, got: {rendered}"
        );
        assert!(rendered.contains("host=h"));
        assert!(rendered.contains("pid=1"));
    }

    #[test]
    fn display_malformed_entry_is_not_awkward() {
        let now = chrono::Utc::now();
        let list = build_active_session_list(vec![("corrupt".to_string(), None)], &now);

        let rendered = format!("{list}");
        // Must not produce phrases like "last refresh unknown (malformed marker) ago".
        assert!(
            !rendered.contains("last refresh"),
            "malformed entries should use their own format, got: {rendered}"
        );
        assert!(rendered.contains("malformed marker"));
    }
}
