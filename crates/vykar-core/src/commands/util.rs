use std::path::PathBuf;

use tracing::info;

use std::sync::Arc;

use crate::config::VykarConfig;
use crate::limits;
use crate::repo::control::CancelToken;
use crate::repo::lock;
use crate::repo::{Fence, OpenOptions, OpenRequest, Repo, RepoWriter, identity};
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
    }
}

/// Verify the repository identity against the locally pinned fingerprint.
pub(crate) fn verify_repo_identity(req: &OpenRequest, repo: &Repo) -> Result<()> {
    identity::verify_or_pin(
        &req.url,
        &repo.config().id,
        repo.crypto().chunk_hasher().key(),
        req.cache_dir.as_deref(),
        req.trust_repo,
    )
}

/// Attach the identity context `Repo::open` cannot derive on its own.
///
/// The URL is what lets the key loader consult the local identity pin, and
/// `trust_repo` is the deliberate waiver of it. Applied in one place so
/// every core open path gets both; caller-set flags such as
/// `skip_key_backfill` are preserved.
pub(crate) fn with_identity_context(opts: OpenOptions, req: &OpenRequest) -> OpenOptions {
    opts.with_repo_url(req.url.clone())
        .trust_repo(req.trust_repo)
}

/// Open a repository from config using the standard backend resolver.
///
/// The backend observes `cancel` in its retry waits.
pub fn open_repo(
    config: &VykarConfig,
    passphrase: Option<&str>,
    opts: OpenOptions,
    cancel: &CancelToken,
) -> Result<Repo> {
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
) -> Result<Repo> {
    let opts = with_identity_context(opts, req);
    let repo = Repo::open(backend, passphrase, req.cache_dir.clone(), opts)
        .map_err(|e| enrich_repo_not_found(e, &req.url))?;
    verify_repo_identity(req, &repo)?;
    Ok(repo)
}

/// Execute a repository mutation while holding an advisory lock.
/// Ensures the lock release is always attempted even when the action fails.
/// A write session the action begins is the action's to save or flush
/// (`WriteSession::flush_on_abort`) before it returns.
pub fn with_repo_lock<T>(
    repo: &mut Repo,
    action: impl FnOnce(&mut RepoWriter<'_>) -> Result<T>,
) -> Result<T> {
    let env = repo.host_env().clone();
    let guard = lock::acquire_lock(repo.storage().as_ref(), &env)?;
    run_under_fence(repo, guard, action)
}

/// Clear the lock-hold state and release the advisory lock, reporting a
/// failed release without changing the operation's outcome.
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
    repo: &mut Repo,
    guard: lock::LockGuard,
    action_succeeded: bool,
    on_warning: &mut dyn FnMut(String),
) {
    repo.clear_lock_state();
    let lock_key = guard.key().to_string();
    let Err(release_err) = lock::release_lock(repo.storage().as_ref(), guard) else {
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

/// Shared epilogue for lock-guarded operations: runs the action on a
/// writer under the lock's fence, then releases the lock via
/// [`clear_fence_and_release`]. Action errors are propagated as-is.
fn run_under_fence<T>(
    repo: &mut Repo,
    guard: lock::LockGuard,
    action: impl FnOnce(&mut RepoWriter<'_>) -> Result<T>,
) -> Result<T> {
    let fence = Fence::lock(lock::build_lock_fence(
        &guard,
        Arc::clone(repo.storage()),
        repo.host_env().clone(),
    ));
    repo.set_lock_store_time(guard.store_time());

    let result = action(&mut repo.writer(fence));

    clear_fence_and_release(repo, guard, result.is_ok(), &mut |msg| {
        tracing::warn!("{msg}");
    });
    result
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

/// Execute a maintenance operation while holding an advisory lock (lock
/// mode; in lock-free mode compact, prune and delete register a session
/// instead, `with_maintenance_session`).
///
/// Acquires the lock, cleans up stale sessions (no marker written for 45 min
/// by the store's clock), lists the sessions that remain live (for compact's
/// horizon, `Repo::live_sessions`), runs the action on a writer under
/// the lock's fence and releases the lock. It does not refuse on live sessions:
/// compact, prune and delete run beside backups (04 step 7), compact keeping
/// their uploads by age and inside their markers' horizons. Actions that
/// must not run beside a backup refuse themselves with
/// `Repo::refuse_live_sessions` (`check --repair`, upgrade).
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
    repo: &mut Repo,
    action: impl FnOnce(&mut RepoWriter<'_>) -> Result<T>,
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
    repo: &mut Repo,
    action: impl FnOnce(&mut RepoWriter<'_>) -> Result<T>,
) -> Result<T> {
    let env = repo.host_env().clone();
    let guard = lock::acquire_lock(repo.storage().as_ref(), &env)?;
    // An uncontended acquirer never probes the store's clock, so it removes
    // the old probes its acquisition listed (best effort).
    guard.remove_probe_debris(repo.storage().as_ref());
    run_under_fence(repo, guard, action)
}

/// The stale-session reap and the live listing of [`with_maintenance_lock`],
/// for an action already under the maintenance lock; sets
/// `Repo::live_sessions`. "Now" is `Repo::lock_store_time`.
/// Fails closed: without the listing compact would not know whose uploads
/// to keep, so a failed listing refuses the maintenance.
pub(crate) fn list_live_sessions_under_lock(repo: &mut Repo) -> Result<()> {
    // Clean up stale sessions; the ones kept are live. Backups may run
    // beside maintenance (04 step 7): compact keeps their uploads by age and
    // by their marker's horizon.
    //
    // The same listing is the lock holder's half of the marker handshake
    // (04 step 9): a live session that may write heads without the lock (a
    // lock-free backup committing, or a lock-free compact, whose marker
    // carries `writes_heads` for its whole run) makes this holder yield with
    // `Locked` naming it, before any write; the user retries.
    let listed = lock::yield_to_lock_free_writers(
        repo.storage().as_ref(),
        repo.host_env(),
        repo.lock_store_time(),
        None,
    );
    let live = match listed {
        // `LockExpired`: the deletes of a v2/v3 upgrade's reap run its
        // lock key's check first.
        Err(e @ (VykarError::Locked(_) | VykarError::LockExpired(_))) => return Err(e),
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

/// The registered session of a lock-free maintenance run (04 step 9): its
/// heartbeat runs until the run ends, then it deregisters.
pub(crate) struct MaintenanceSession {
    guard: lock::SessionGuard,
}

impl MaintenanceSession {
    pub(crate) fn id(&self) -> &str {
        self.guard.session_id()
    }

    /// The session's marker record (its heartbeat's view of the marker).
    pub(crate) fn marker(&self) -> Arc<lock::SessionMarker> {
        self.guard.marker()
    }
}

/// The fence of a lock-free maintenance run (04 step 9), installed as the
/// handle's lock fence for the whole run: the run's marker exists now and
/// its record is [continuous](lock::MarkerReport::continuous), the
/// predicate a lock-free backup checks before its snapshot PUT. It runs
/// before every head create of the run (the publish machine's fence step:
/// the sweep head, each repack batch's create and each create after a
/// rebase, the fold), before each batch of pack deletes, before the index
/// garbage collection and before each snapshot delete. A failed check ends
/// the run with `MarkerLostDuringCommit`; what it wrote so far is
/// unreferenced debris (repack outputs are orphans for a later compact).
///
/// Why: once its marker is reaped (a stall over the session staleness, a
/// suspended laptop) nothing protects what the run decided before. Another
/// compact takes its repack outputs for old orphans and deletes them, and a
/// lock holder no longer yields to it, so a lock-path backup commits a
/// reuse-only snapshot (no head) that this run's sweep, planned from an
/// older mark, would then strip. The check narrows both to a stall between
/// the check and the write it guards. A reap needs the marker unrefreshed
/// for the session staleness (45 min), and such a gap breaks the record's
/// continuity (`MARKER_GAP_LIMIT`, 30 min) whether or not the key is
/// still there: the check also catches a reaper that listed the key stale
/// and deletes it only after a late refresh.
fn maintenance_marker_fence(
    marker: Arc<lock::SessionMarker>,
    storage: Arc<dyn StorageBackend>,
    clock: Arc<dyn crate::repo::host_env::Clock>,
) -> Arc<dyn Fn() -> Result<()> + Send + Sync> {
    Arc::new(move || {
        let present = marker.probe(storage.as_ref())?;
        let report = marker.report(clock.as_ref());
        if present && report.continuous() {
            return Ok(());
        }
        if !present {
            marker.note_missing();
        }
        tracing::warn!(
            session_id = marker.session_id(),
            present,
            ?report,
            "this maintenance run's session marker was reaped, or went unrefreshed for a \
             while (machine suspended?): another client may have taken the run for ended; \
             nothing more is written"
        );
        Err(VykarError::MarkerLostDuringCommit)
    })
}

/// Lock-free maintenance (04 step 9): register a session marker carrying
/// `flags`, adopt it with a heartbeat, run `action`, deregister. No lock is
/// taken; the action's writer is fenced by the marker check
/// ([`maintenance_marker_fence`]) for the whole run.
///
/// The registration probe keeps the lock holders out: the marker is written
/// before the probe lists `locks/` (a live lock fails `Locked` after the
/// probe's retries, as a backup's registration does), and every lock holder
/// lists `sessions/` after its lock is confirmed, so `check --repair` and
/// `upgrade` refuse beside this session (`refuse_live_sessions`) and a
/// lock-mode writer yields to a marker that may write heads
/// (`writes_heads`: a compact). Clears the store time, live sessions and
/// segment ages at the end (`Repo::clear_lock_state`).
pub(crate) fn with_maintenance_session<T>(
    repo: &mut Repo,
    flags: lock::MarkerFlags,
    action: impl FnOnce(&mut RepoWriter<'_>, &MaintenanceSession) -> Result<T>,
) -> Result<T> {
    let env = repo.host_env().clone();
    let id = format!("{:032x}", rand::random::<u128>());
    let registration = lock::register_session_with(repo.storage().as_ref(), &id, &env, flags)?;
    let guard =
        match lock::SessionGuard::adopt(Arc::clone(repo.storage()), registration, env.clone()) {
            Ok(g) => g,
            Err(e) => {
                lock::deregister_session(repo.storage().as_ref(), &id, &env);
                return Err(e);
            }
        };
    tracing::debug!(session_id = id, ?flags, "maintenance session registered");
    let session = MaintenanceSession { guard };
    let fence = Fence::marker(maintenance_marker_fence(
        session.marker(),
        Arc::clone(repo.storage()),
        Arc::clone(&repo.host_env().clock),
    ));
    let result = action(&mut repo.writer(fence), &session);
    repo.clear_lock_state();
    drop(session);
    result
}

/// The exclusion of a command that deletes only snapshot objects and
/// writes no index (prune, delete): the maintenance lock in lock mode; in
/// lock-free mode (04 step 9) a registered session without flags
/// ([`with_maintenance_session`]), which keeps `check --repair` and
/// `upgrade` out and lists no sessions (these commands write no head and
/// judge no age). Snapshot deletes need no exclusion from writers: a
/// concurrent compact's mark skips a snapshot deleted after its listing
/// (`mark::load_snapshots_strict`), and a backup that reused chunks of a
/// deleted snapshot is protected by its commit (the young-segment rule and
/// compact's handling of committing sessions) or fails
/// `StaleChunksDuringCommit`.
pub(crate) fn with_snapshot_removal_exclusion<T>(
    repo: &mut Repo,
    action: impl FnOnce(&mut RepoWriter<'_>) -> Result<T>,
) -> Result<T> {
    match repo.commit_mode()? {
        crate::repo::CommitMode::Lock => with_maintenance_lock(repo, action),
        crate::repo::CommitMode::LockFree => {
            with_maintenance_session(repo, lock::MarkerFlags::default(), |repo, _| action(repo))
        }
    }
}

/// Sessions #1 of a lock-free maintenance run (04 step 9): one listing of
/// `sessions/`; "now" on the store's clock is the mtime of `own`'s newest
/// key in it (written after registration and before this listing, so it
/// precedes compact's head read, as 8b's I8 argument needs); stale sessions
/// are reaped with that "now"; `Repo::live_sessions` are the others
/// and `Repo::lock_store_time` is that "now". Fails closed as
/// [`list_live_sessions_under_lock`].
pub(crate) fn list_live_sessions_registered(
    repo: &mut Repo,
    own: &MaintenanceSession,
) -> Result<()> {
    let (cleaned, live, now) =
        match lock::reap_sessions_registered(repo.storage().as_ref(), repo.host_env(), own.id()) {
            Ok(listed) => listed,
            Err(e) => {
                return Err(VykarError::Other(format!(
                    "cannot clean up stale backup sessions (storage error: {e}); \
                     refusing maintenance to avoid data loss"
                )));
            }
        };
    if !cleaned.is_empty() {
        info!(
            count = cleaned.len(),
            sessions = ?cleaned,
            "cleaned up stale backup sessions"
        );
    }
    if !live.is_empty() {
        info!(
            count = live.len(),
            "backup sessions are live; maintenance runs beside them"
        );
    }
    repo.set_lock_store_time(now);
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
            committing: false,
            writes_heads: false,
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
