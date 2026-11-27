use std::path::PathBuf;

use tracing::info;

use std::sync::Arc;

use crate::config::VykarConfig;
use crate::limits;
use crate::repo::control::CancelToken;
use crate::repo::sessions;
use crate::repo::write_check::WriteChecked;
use crate::repo::{Fence, FenceFn, OpenOptions, OpenRequest, Repo, RepoWriter, identity};
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

/// The registered session of a maintenance run (04 step 9): its
/// heartbeat runs until the run ends, then it deregisters.
pub(crate) struct MaintenanceSession {
    guard: sessions::SessionGuard,
}

impl MaintenanceSession {
    pub(crate) fn id(&self) -> &str {
        self.guard.session_id()
    }

    /// The session's marker record (its heartbeat's view of the marker).
    pub(crate) fn marker(&self) -> Arc<sessions::SessionMarker> {
        self.guard.marker()
    }
}

/// The fence of a maintenance run in a session (04 step 9), installed as the
/// handle's fence for the whole run: the run's marker exists now and
/// its record is [continuous](sessions::MarkerReport::continuous), the
/// predicate a backup checks before its snapshot PUT. It runs
/// before every head create of the run (the publish machine's fence step:
/// the sweep head, each repack batch's create and each create after a
/// rebase, the fold), before each batch of pack deletes, before the index
/// garbage collection and before each snapshot delete. A failed check ends
/// the run with `MarkerLostDuringCommit`; what it wrote so far is
/// unreferenced debris (repack outputs are orphans for a later compact).
///
/// Why: once its marker is reaped (a stall over the session staleness, a
/// suspended laptop) nothing protects what the run decided before. Another
/// compact takes its repack outputs for old orphans and deletes them, and
/// its decisions (the sweep planned from its mark, the repack sources it
/// chose) are no longer guarded against another compact that took it for
/// ended. The check narrows both to a stall between
/// the check and the write it guards. A reap needs the marker unrefreshed
/// for the session staleness (45 min), and such a gap breaks the record's
/// continuity (`MARKER_GAP_LIMIT`, 30 min) whether or not the key is
/// still there: the check also catches a reaper that listed the key stale
/// and deletes it only after a late refresh.
fn maintenance_marker_fence(
    marker: Arc<sessions::SessionMarker>,
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

/// The record shows a lapse no later probe can undo: the marker was found
/// missing, it went unrefreshed longer than [`sessions::MARKER_GAP_LIMIT`], or a
/// marker PUT was slow. A superseded key whose check failed (`unchecked`)
/// is not one: the next probe checks it again.
fn marker_lapsed(report: &sessions::MarkerReport) -> bool {
    report.missing || report.longest_gap > sessions::MARKER_GAP_LIMIT || report.slow_puts > 0
}

/// The check before every write of an exclusive run, as a fence function
/// shared by the run's `WriteChecked` storage and its writer's
/// `Fence::marker`. It passes while
/// - the marker record is [continuous](sessions::MarkerReport::continuous) at
///   this call (no request), and
/// - an EXISTS of the marker's current key ([`sessions::SessionMarker::probe`])
///   found it, sent at most `reuse` ago measured with
///   [`sessions::MarkerStamp::until`] (suspend included). An older one is
///   repeated, and so is one whose record is not continuous at this call
///   (a superseded key whose check failed is checked again by the probe);
///   one call at a time (a mutex), so concurrent uploads wait for one
///   EXISTS.
///
/// The first lapse is sticky (no further request): the probe found the
/// current key gone (recorded with `note_missing`), or the record shows the
/// marker missing, a gap over [`sessions::MARKER_GAP_LIMIT`] or a slow marker
/// PUT; it is warned once, then every call fails `MarkerLostDuringCommit`.
/// A failed EXISTS (transport) returns its error and is not sticky.
///
/// `storage` is the raw backend (the session guard's), never the wrapper.
pub(crate) fn exclusive_write_check(
    marker: Arc<sessions::SessionMarker>,
    storage: Arc<dyn StorageBackend>,
    clock: Arc<dyn crate::repo::host_env::Clock>,
    reuse: std::time::Duration,
) -> FenceFn {
    // (when the last EXISTS that found the marker was sent, lapsed)
    let state: std::sync::Mutex<(Option<sessions::MarkerStamp>, bool)> =
        std::sync::Mutex::new((None, false));
    Arc::new(move || {
        let mut st = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if st.1 {
            return Err(VykarError::MarkerLostDuringCommit);
        }
        if let Some(stamp) = st.0
            && stamp.until(&sessions::MarkerStamp::now(clock.as_ref())) < reuse
            && marker.report(clock.as_ref()).continuous()
        {
            return Ok(());
        }
        // A heartbeat refresh may leave a superseded key unchecked between
        // our probe and our report: probe again, a few times at most.
        for _ in 0..3 {
            let sent = sessions::MarkerStamp::now(clock.as_ref());
            let present = marker.probe(storage.as_ref())?;
            let report = marker.report(clock.as_ref());
            if present && report.continuous() {
                st.0 = Some(sent);
                return Ok(());
            }
            if !present || marker_lapsed(&report) {
                if !present {
                    marker.note_missing();
                }
                st.1 = true;
                tracing::warn!(
                    session_id = marker.session_id(),
                    present,
                    ?report,
                    "this exclusive run's session marker was removed, or went unrefreshed for \
                     a while (machine suspended or storage unreachable?): another client may \
                     have taken the run for ended; nothing more is written"
                );
                return Err(VykarError::MarkerLostDuringCommit);
            }
        }
        Err(VykarError::Other(format!(
            "session {} marker check undecided: a superseded key stayed unchecked",
            marker.session_id()
        )))
    })
}

/// `first`, then `second` when `first` passed.
fn both(first: FenceFn, second: FenceFn) -> FenceFn {
    Arc::new(move || {
        first()?;
        second()
    })
}

/// The registered session of an exclusive run (10-single-protocol step 8):
/// its heartbeat runs until the run ends, then it deregisters.
pub(crate) struct ExclusiveSession {
    guard: sessions::SessionGuard,
}

impl ExclusiveSession {
    pub(crate) fn id(&self) -> &str {
        self.guard.session_id()
    }
}

/// Run `action` as an exclusive session: `check --repair`, `vykar upgrade`
/// (`--commit-mode` included) and pack maps (10-single-protocol step 8).
///
/// Registers `sessions/x-<32 hex>.json` and lists `sessions/` once
/// (`sessions::register_exclusive`: stale sessions reaped, orphan journals
/// deleted); refuses `ActiveSessions` when any other session is live
/// (malformed or vanished markers included: fail closed), its own marker
/// deleted. Then adopts the marker with a heartbeat, reads `config` again on
/// a v6 handle (`Repo::reread_writer_mode`; no `commit_mode()`: a mode
/// switch to single-writer must run without put-if-absent), wraps the
/// handle's storage for the whole action in `WriteChecked` with the
/// exclusive write check (`exclusive_write_check`) and runs `action` on a
/// writer fenced by the same check. Every shared registration and a
/// backup's pre-head listing refuse `Locked` while the marker is live
/// (`sessions::exclusive_listing`). At the end, whatever the result: the
/// session state is cleared, the raw storage restored and the session deregistered
/// (through the raw storage; a failed deregistration is warned only).
/// "Now" on the store's clock (`Repo::store_time`) is the marker's
/// mtime in the registration's listing, and `Repo::live_sessions` is empty.
pub fn with_exclusive_session<T>(
    repo: &mut Repo,
    action: impl FnOnce(&mut RepoWriter<'_>) -> Result<T>,
) -> Result<T> {
    with_exclusive_session_as(repo, None, |w, _| action(w))
}

/// [`with_exclusive_session`] with the session, and `also` (the v2/v3
/// upgrade's legacy key check) run after the exclusive check before every
/// write.
pub(crate) fn with_exclusive_session_as<T>(
    repo: &mut Repo,
    also: Option<FenceFn>,
    action: impl FnOnce(&mut RepoWriter<'_>, &ExclusiveSession) -> Result<T>,
) -> Result<T> {
    let env = repo.host_env().clone();
    let raw = Arc::clone(repo.storage());
    // On a v2/v3 upgrade the registration's reap runs on the raw storage:
    // the legacy key is held already but its check does not run for these
    // deletes; a reap deletes only stale session objects.
    let reg = sessions::register_exclusive(raw.as_ref(), &env)?;
    if !reg.cleaned.is_empty() {
        info!(
            count = reg.cleaned.len(),
            sessions = ?reg.cleaned,
            "cleaned up stale backup sessions"
        );
    }
    let id = reg.registration.session_id().to_string();
    repo.set_live_sessions(Some(reg.live));
    if let Err(e) = repo.refuse_live_sessions() {
        repo.clear_session_state();
        sessions::deregister_session(raw.as_ref(), &id, &env);
        return Err(e);
    }
    let guard = match sessions::SessionGuard::adopt(Arc::clone(&raw), reg.registration, env.clone())
    {
        Ok(g) => g,
        Err(e) => {
            repo.clear_session_state();
            sessions::deregister_session(raw.as_ref(), &id, &env);
            return Err(e);
        }
    };
    tracing::debug!(session_id = id, "exclusive session registered");
    let session = ExclusiveSession { guard };
    if !repo.format().is_upgrade_only()
        && let Err(e) = repo.reread_writer_mode()
    {
        repo.clear_session_state();
        drop(session);
        return Err(e);
    }
    let check = exclusive_write_check(
        session.guard.marker(),
        Arc::clone(&raw),
        Arc::clone(&env.clock),
        repo.exclusive_check_reuse(),
    );
    let check = match also {
        Some(also) => both(check, also),
        None => check,
    };
    let prev = repo.swap_storage(Arc::new(WriteChecked::new(
        Arc::clone(&raw),
        Arc::clone(&check),
    )));
    repo.set_store_time(reg.now);
    let result = action(&mut repo.writer(Fence::marker(check)), &session);
    repo.clear_session_state();
    repo.swap_storage(prev);
    drop(session);
    result
}

/// Maintenance in a session (04 step 9; compact, prune and delete in both
/// modes since 10-single-protocol step 7): register a session marker
/// carrying `flags`, adopt it with a heartbeat, read `config` again and
/// take the repository's mode from that read
/// ([`Repo::reread_writer_mode`], then [`Repo::commit_mode`]), run
/// `action`, deregister. The action's writer is fenced by
/// the marker check ([`maintenance_marker_fence`]) for the whole run.
///
/// The registration keeps the exclusive sessions out (10-single-protocol
/// step 8): the marker is written before the registration lists
/// `sessions/` (a live exclusive session fails `Locked` after the
/// registration's retries, as a backup's registration does), and every
/// exclusive session lists `sessions/` after its own marker PUT, so
/// `check --repair`, `upgrade` and pack maps refuse beside this session
/// (`ActiveSessions`). The re-read after the registration closes the window
/// between open and registration: a mode switch (an exclusive session,
/// refusing beside any live session) that ran in it is seen by the read,
/// one that runs after it refuses beside this session. A changed identity or format
/// refuses. Clears the store time, live sessions and segment ages at the
/// end (`Repo::clear_session_state`).
pub(crate) fn with_maintenance_session<T>(
    repo: &mut Repo,
    flags: sessions::MarkerFlags,
    action: impl FnOnce(&mut RepoWriter<'_>, &MaintenanceSession) -> Result<T>,
) -> Result<T> {
    let env = repo.host_env().clone();
    let id = format!("{:032x}", rand::random::<u128>());
    let registration = sessions::register_session_with(repo.storage().as_ref(), &id, &env, flags)?;
    let guard = match sessions::SessionGuard::adopt(
        Arc::clone(repo.storage()),
        registration,
        env.clone(),
    ) {
        Ok(g) => g,
        Err(e) => {
            sessions::deregister_session(repo.storage().as_ref(), &id, &env);
            return Err(e);
        }
    };
    tracing::debug!(session_id = id, ?flags, "maintenance session registered");
    let session = MaintenanceSession { guard };
    if let Err(e) = repo.reread_writer_mode().and_then(|_| repo.commit_mode()) {
        drop(session);
        return Err(e);
    }
    let fence = Fence::marker(maintenance_marker_fence(
        session.marker(),
        Arc::clone(repo.storage()),
        Arc::clone(&repo.host_env().clock),
    ));
    let result = action(&mut repo.writer(fence), &session);
    repo.clear_session_state();
    drop(session);
    result
}

/// The exclusion of a command that deletes only snapshot objects and
/// writes no index (prune, delete), in both modes: a registered session
/// without flags ([`with_maintenance_session`]), which keeps `check --repair` and
/// `upgrade` out and lists no sessions (these commands write no head and
/// judge no age, and make no single-writer check). Snapshot deletes need
/// no exclusion from writers: a
/// concurrent compact's mark skips a snapshot deleted after its listing
/// (`mark::load_snapshots_strict`), and a backup that reused chunks of a
/// deleted snapshot is protected by its commit (the young-segment rule and
/// compact's handling of committing sessions) or fails
/// `StaleChunksDuringCommit`.
pub(crate) fn with_snapshot_removal_session<T>(
    repo: &mut Repo,
    action: impl FnOnce(&mut RepoWriter<'_>) -> Result<T>,
) -> Result<T> {
    // Refuses `MultiWriterUnsupported` before anything is written.
    repo.commit_mode()?;
    with_maintenance_session(repo, sessions::MarkerFlags::default(), |repo, _| {
        action(repo)
    })
}

/// Sessions #1 of a maintenance run in a session (04 step 9): one listing of
/// `sessions/`; "now" on the store's clock is the mtime of `own`'s newest
/// key in it (written after registration and before this listing, so it
/// precedes compact's head read, as 8b's I8 argument needs); stale sessions
/// are reaped with that "now"; `Repo::live_sessions` are the others
/// and `Repo::store_time` is that "now". Fails closed: without the listing
/// compact would not know whose uploads to keep, so a failed listing
/// refuses the maintenance.
pub(crate) fn list_live_sessions_registered(
    repo: &mut Repo,
    own: &MaintenanceSession,
) -> Result<()> {
    let (cleaned, live, now) = match sessions::reap_sessions_registered(
        repo.storage().as_ref(),
        repo.host_env(),
        own.id(),
    ) {
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
    repo.set_store_time(now);
    repo.set_live_sessions(Some(live));
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::repo::sessions::{SessionEntry, build_active_session_list};

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
            rendered.contains("vykar break-lock"),
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
