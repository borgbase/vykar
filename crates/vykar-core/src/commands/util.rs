use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::info;

use crate::config::VykarConfig;
use crate::limits;
use crate::repo::lock;
use crate::repo::Repository;
use crate::storage;
use vykar_types::error::{Result, VykarError};

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

/// Open a repository from config using the standard backend resolver.
pub fn open_repo(config: &VykarConfig, passphrase: Option<&str>) -> Result<Repository> {
    let connections = config.limits.connections;
    let backend = storage::backend_from_config(&config.repository, connections)?;
    let backend = limits::wrap_storage_backend(backend, &config.limits);
    Repository::open(backend, passphrase, cache_dir_from_config(config))
        .map_err(|e| enrich_repo_not_found(e, &config.repository.url))
}

/// Open a repository without loading the chunk index.
/// Suitable for read-only operations that load or filter the index lazily.
pub fn open_repo_without_index(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<Repository> {
    let connections = config.limits.connections;
    let backend = storage::backend_from_config(&config.repository, connections)?;
    let backend = limits::wrap_storage_backend(backend, &config.limits);
    Repository::open_without_index(backend, passphrase, cache_dir_from_config(config))
        .map_err(|e| enrich_repo_not_found(e, &config.repository.url))
}

/// Open a repository without loading the chunk index or file cache.
/// Suitable for operations (e.g. restore) that need neither.
pub fn open_repo_without_index_or_cache(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<Repository> {
    let connections = config.limits.connections;
    let backend = storage::backend_from_config(&config.repository, connections)?;
    let backend = limits::wrap_storage_backend(backend, &config.limits);
    Repository::open_without_index_or_cache(backend, passphrase, cache_dir_from_config(config))
        .map_err(|e| enrich_repo_not_found(e, &config.repository.url))
}

/// Open a repository and execute a mutation while holding an advisory lock.
pub fn with_open_repo_lock<T>(
    config: &VykarConfig,
    passphrase: Option<&str>,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let mut repo = open_repo(config, passphrase)?;
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
    let result = action(repo);

    if result.is_err() {
        repo.flush_on_abort();
    }

    match lock::release_lock(repo.storage.as_ref(), guard) {
        Ok(()) => result,
        Err(release_err) => {
            if result.is_err() {
                tracing::warn!("failed to release repository lock: {release_err}");
                result
            } else {
                Err(release_err)
            }
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
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let mut repo = open_repo(config, passphrase)?;
    with_maintenance_lock(&mut repo, action)
}

/// Execute a maintenance operation while holding an advisory lock.
///
/// Acquires the lock, cleans up stale sessions (>72h), then refuses to run
/// if any non-stale sessions remain. This prevents maintenance from deleting
/// packs that active backups depend on.
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
            tracing::warn!("failed to clean up stale sessions: {e}");
        }
    }

    // Check for active (non-stale) sessions. Fail-closed: if we can't
    // list sessions, refuse to run rather than risk deleting active packs.
    match lock::list_sessions(repo.storage.as_ref()) {
        Ok(sessions) if !sessions.is_empty() => {
            let _ = lock::release_lock(repo.storage.as_ref(), guard);
            return Err(VykarError::ActiveSessions(sessions));
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

    let result = action(repo);

    if result.is_err() {
        repo.flush_on_abort();
    }

    match lock::release_lock(repo.storage.as_ref(), guard) {
        Ok(()) => result,
        Err(release_err) => {
            if result.is_err() {
                tracing::warn!("failed to release repository lock: {release_err}");
                result
            } else {
                Err(release_err)
            }
        }
    }
}
