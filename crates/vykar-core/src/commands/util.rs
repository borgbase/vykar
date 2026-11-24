use std::path::PathBuf;

use tracing::info;

use std::sync::Arc;

use crate::config::{EncryptionModeConfig, VykarConfig};
use crate::limits;
use crate::repo::control::CancelToken;
use crate::repo::lock;
use crate::repo::{OpenOptions, OpenRequest, Repository, identity};
use crate::storage;
use vykar_storage::StorageBackend;
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

/// The open request `config` describes.
pub fn open_request(config: &VykarConfig) -> OpenRequest {
    OpenRequest {
        url: config.repository.url.clone(),
        cache_dir: cache_dir_from_config(config),
        trust_repo: config.trust_repo,
        require_encryption: config.encryption.mode != EncryptionModeConfig::None,
    }
}

/// Verify the repository identity against the locally pinned fingerprint.
pub(crate) fn verify_repo_identity(req: &OpenRequest, repo: &Repository) -> Result<()> {
    identity::verify_or_pin(
        &req.url,
        &repo.config.id,
        repo.crypto.chunk_hasher().key(),
        req.cache_dir.as_deref(),
        req.trust_repo,
    )
}

/// Attach the identity context `Repository::open` cannot derive on its own.
///
/// The URL is what lets the key loader consult the local identity pin, and
/// `trust_repo` is the deliberate waiver of it. The configured encryption mode
/// decides whether a plaintext repository is acceptable: the repository's own
/// config is unauthenticated and must not be able to downgrade the client.
/// Applied in one place so every core open path gets all three; caller-set
/// flags such as `skip_key_backfill` are preserved.
pub(crate) fn with_identity_context(opts: OpenOptions, req: &OpenRequest) -> OpenOptions {
    opts.with_repo_url(req.url.clone())
        .trust_repo(req.trust_repo)
        .require_encryption(req.require_encryption)
}

/// Open a repository from config using the standard backend resolver.
///
/// The backend observes `cancel` in its retry waits.
pub fn open_repo(
    config: &VykarConfig,
    passphrase: Option<&str>,
    opts: OpenOptions,
    cancel: &CancelToken,
) -> Result<Repository> {
    let backend = storage::backend_from_config(
        &config
            .repository
            .storage_config(config.limits.connections)
            .with_cancel(cancel.clone()),
    )?;
    let backend = limits::wrap_storage_backend(backend, &config.limits);
    open_repo_on(backend, &open_request(config), passphrase, opts)
}

/// [`open_repo`] over a backend the caller built. Applies the same identity
/// context and pin check; no limits wrapper (the caller decides).
pub(crate) fn open_repo_on(
    backend: Box<dyn StorageBackend>,
    req: &OpenRequest,
    passphrase: Option<&str>,
    opts: OpenOptions,
) -> Result<Repository> {
    let opts = with_identity_context(opts, req);
    let repo = Repository::open(backend, passphrase, req.cache_dir.clone(), opts)
        .map_err(|e| enrich_repo_not_found(e, &req.url))?;
    verify_repo_identity(req, &repo)?;
    Ok(repo)
}

/// Execute a repository mutation while holding an advisory lock.
/// Ensures the lock release is always attempted even when the action fails.
/// On error, performs best-effort cleanup (seals partial packs, waits for
/// in-flight uploads, writes pending_index) before releasing the lock.
pub fn with_repo_lock<T>(
    repo: &mut Repository,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let env = repo.host_env().clone();
    let guard = lock::acquire_lock(repo.storage.as_ref(), &env)?;
    run_under_fence(repo, guard, action)
}

/// Clear the lock fence and release the advisory lock, reporting a failed
/// release without changing the operation's outcome.
///
/// # Failure policy
///
/// - **After a failed action**, a release failure is `tracing::warn!`-only:
///   the caller already has a more meaningful error to propagate.
/// - **After a successful action**, the work is already committed to storage
///   (e.g. the snapshot blob was written), so a release failure must not be
///   reported as a failure of the operation. It goes to `on_warning`, which
///   decides where the user sees it.
///
/// `on_warning` is what lets the two callers differ: `run_under_fence` has no
/// progress sink and passes `tracing::warn!`, while backup's commit path
/// passes its `BackupProgressEvent::Warning` emitter so GUI consumers see it
/// too. Leaked advisory locks self-heal after 6 hours, and `vykar break-lock`
/// clears them immediately.
pub(crate) fn clear_fence_and_release(
    repo: &mut Repository,
    guard: lock::LockGuard,
    action_succeeded: bool,
    on_warning: &mut dyn FnMut(String),
) {
    repo.clear_lock_fence();
    let lock_key = guard.key().to_string();
    let Err(release_err) = lock::release_lock(repo.storage.as_ref(), guard) else {
        return;
    };
    if !action_succeeded {
        tracing::warn!("failed to release repository lock: {release_err}");
        return;
    }
    on_warning(format!(
        "the operation completed successfully, but releasing the repository lock \
         failed: {release_err}. The advisory lock at `{lock_key}` may persist; \
         future operations on this repository may be blocked for up to 6 hours \
         until automatic stale-lock cleanup, or run `vykar break-lock` to clear \
         it manually."
    ));
}

/// Shared epilogue for lock-guarded operations: installs a lock fence, runs
/// the action, performs best-effort cleanup on error, then releases the lock
/// via [`clear_fence_and_release`]. Action errors are propagated as-is.
fn run_under_fence<T>(
    repo: &mut Repository,
    guard: lock::LockGuard,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let fence = lock::build_lock_fence(&guard, Arc::clone(&repo.storage), repo.host_env().clone());
    repo.set_lock_fence(fence);
    repo.set_lock_store_time(guard.store_time());

    let result = action(repo);

    if result.is_err() {
        repo.flush_on_abort();
    }

    clear_fence_and_release(repo, guard, result.is_ok(), &mut |msg| {
        tracing::warn!("{msg}");
    });
    result
}

/// Open a repository and execute a maintenance operation while holding the lock.
///
/// Unlike `with_repo_lock`, this first cleans up stale sessions, and the
/// action refuses (`ActiveSessions`, under the lock) while backup sessions
/// are live: upgrade, its one caller, rewrites the index and must not run
/// beside a backup.
pub fn with_open_repo_maintenance_lock<T>(
    config: &VykarConfig,
    passphrase: Option<&str>,
    opts: OpenOptions,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    // Upgrade, the only caller, has no token.
    let mut repo = open_repo(config, passphrase, opts, &CancelToken::new())?;
    with_maintenance_lock(&mut repo, |repo| {
        repo.refuse_live_sessions()?;
        action(repo)
    })
}

/// `Err(AppendOnlyServer { command })` when the repository's server
/// advertises append-only mode ([`vykar_storage::ServerOps::append_only`]).
/// Call before a command's first delete; it sends at most one `GET /health`.
pub(crate) fn refuse_on_append_only_server(
    storage: &dyn StorageBackend,
    command: &'static str,
) -> Result<()> {
    if storage.server_ops().is_some_and(|s| s.append_only()) {
        return Err(VykarError::AppendOnlyServer { command });
    }
    Ok(())
}

/// Execute a maintenance operation while holding an advisory lock.
///
/// Acquires the lock, cleans up stale sessions (no marker written for 45 min
/// by the store's clock), lists the sessions that remain live (for compact's
/// horizon, `Repository::live_sessions`), installs the lock fence, runs the
/// action and releases the lock. It does not refuse on live sessions:
/// compact, prune and delete run beside backups (04 step 7), compact keeping
/// their uploads by age and inside their markers' horizons. Actions that
/// must not run beside a backup refuse themselves with
/// `Repository::refuse_live_sessions` (`check --repair`, upgrade).
/// Malformed markers (unparseable JSON or bad timestamps) are judged by
/// their mtime: while younger than 45 minutes they are preserved and give
/// compact no horizon (no orphan pack is deleted), after that they are
/// reaped like any stale marker (`break-lock --sessions` clears them at
/// once). A failed listing refuses.
///
/// The session listing runs inside the fence (it is
/// `list_live_sessions_under_lock` called first in the action): a failed
/// listing returns through the fence's epilogue, which releases the lock.
pub fn with_maintenance_lock<T>(
    repo: &mut Repository,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    with_maintenance_lock_unlisted(repo, |repo| {
        list_live_sessions_under_lock(repo)?;
        action(repo)
    })
}

/// [`with_maintenance_lock`] without the session listing: acquire, remove
/// probe debris, fence, `lock_store_time`, run, release. The action lists
/// sessions itself ([`list_live_sessions_under_lock`]) where the order
/// matters: compact, after its pack listing (04 "Compact" 1).
pub(crate) fn with_maintenance_lock_unlisted<T>(
    repo: &mut Repository,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let env = repo.host_env().clone();
    let guard = lock::acquire_lock(repo.storage.as_ref(), &env)?;
    // An uncontended acquirer never probes the store's clock, so it removes
    // the old probes its acquisition listed (best effort).
    guard.remove_probe_debris(repo.storage.as_ref());
    run_under_fence(repo, guard, action)
}

/// The stale-session reap and the live listing of [`with_maintenance_lock`],
/// for an action already under the maintenance lock; sets
/// `Repository::live_sessions`. "Now" is `Repository::lock_store_time`.
/// Fails closed: without the listing compact would not know whose uploads
/// to keep, so a failed listing refuses the maintenance.
pub(crate) fn list_live_sessions_under_lock(repo: &mut Repository) -> Result<()> {
    // Clean up stale sessions; the ones kept are live. Backups may run
    // beside maintenance (04 step 7): compact keeps their uploads by age and
    // by their marker's horizon.
    let stale_threshold = lock::default_stale_session_duration();
    let live = match lock::cleanup_stale_sessions_keeping(
        repo.storage.as_ref(),
        stale_threshold,
        repo.host_env(),
        repo.lock_store_time(),
    ) {
        Ok((cleaned, live)) => {
            if !cleaned.is_empty() {
                info!(
                    count = cleaned.len(),
                    sessions = ?cleaned,
                    "cleaned up stale backup sessions"
                );
            }
            live
        }
        Err(e) => {
            return Err(VykarError::Other(format!(
                "cannot clean up stale backup sessions (storage error: {e}); \
                 refusing maintenance to avoid data loss"
            )));
        }
    };
    if !live.is_empty() {
        info!(
            count = live.len(),
            "backup sessions are live; maintenance runs beside them"
        );
    }
    repo.set_live_sessions(Some(live));
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::repo::lock::{SessionEntry, build_active_session_list};

    fn entry(hostname: &str, pid: u32, last_refresh: &str) -> SessionEntry {
        SessionEntry {
            hostname: hostname.to_string(),
            pid,
            registered_at: last_refresh.to_string(),
            last_refresh: last_refresh.to_string(),
            boot_id: None,
            installation_id: None,
            pid_ns: None,
            elapsed_secs: None,
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
