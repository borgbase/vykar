//! Session markers: shared sessions (`sessions/<id>.json`) and exclusive
//! ones (`sessions/x-<id>.json`), their registration, heartbeat and
//! reaping, and the listings of the single-writer check and of the
//! exclusive refusal.
//!
//! The advisory lock of releases up to 0.24 and of development builds is
//! gone (10-single-protocol step 9): `locks/` is read only by the v2/v3
//! upgrade's legacy key (`upgrade::LegacyUpgradeLock`) and cleared by
//! [`clear_legacy_locks`].

use std::sync::Arc;
use std::thread::JoinHandle;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::ages::{
    CLOCK_PROBE_DEBRIS_AGE, HEAD_LANDING_ALLOWANCE, MARGIN, PACK_AGE_LIMIT, is_probe_key,
};
use super::control::CancelToken;
use super::host_env::{Clock, HostEnv, StopSignal};
use vykar_storage::{StorageBackend, StoreNow};
use vykar_types::error::{
    ActiveSessionDetails, ActiveSessionInfo, ActiveSessionList, LockHolder, LockHolderDetails,
    Result, VykarError,
};

// ── Same-host and age judgement ──────────────────────────────────────────

/// A live session as a [`LockHolder`]: its marker key `key` and, when its
/// newest key was read, its host, PID and age.
fn session_holder(key: String, entry: Option<&SessionEntry>, env: &HostEnv) -> LockHolder {
    LockHolder {
        key,
        details: entry.map(|e| LockHolderDetails {
            hostname: e.hostname.clone(),
            pid: e.pid,
            age: format_age(&env.now_utc(), &e.registered_at),
        }),
    }
}

/// Another live session that may write heads on a single-writer
/// repository (10-single-protocol step 7), from one listing of
/// `sessions/`.
#[derive(Debug)]
pub(crate) enum HeadWriter {
    /// Carries `writes_heads` (a compact), or its newest key is
    /// unreadable (fail closed).
    Compact(LockHolder),
    /// Carries `committing` (a backup), none carries `writes_heads`.
    Committing(LockHolder),
}

impl HeadWriter {
    pub(crate) fn into_holder(self) -> LockHolder {
        match self {
            HeadWriter::Compact(h) | HeadWriter::Committing(h) => h,
        }
    }
}

/// The single-writer check's listing (10-single-protocol step 7): one reap
/// and listing of `sessions/` ([`reap_sessions`] with
/// [`ReapNow::OwnSession`]`(own)`, orphan journals kept: they are
/// maintenance's to delete), then the first other live session that may
/// write heads: `Compact` before `Committing`. A live session whose newest
/// key is malformed, or gone on GET (a rotation the listing half-saw, or a
/// deregistration), is listed and its newest key read once more; still
/// unreadable it counts as `Compact` (fail closed), and one with no key
/// left has deregistered. `Ok(None)`:
/// no other head writer is live. A failed listing is an error.
///
/// Why it is enough (invariant I1 of the step): every head writer writes
/// its flag before the listing it decides on (a backup its commit mark, a
/// compact its registration), so of two writers whose listings both
/// passed, one listing started after the other's flag was written and, by
/// the LIST contract, saw it.
pub(crate) fn single_writer_listing(
    storage: &dyn StorageBackend,
    env: &HostEnv,
    own: &str,
) -> Result<Option<HeadWriter>> {
    let (cleaned, live, _) = reap_sessions(
        storage,
        default_stale_session_duration(),
        env,
        ReapNow::OwnSession(own),
        false,
    )?;
    if !cleaned.is_empty() {
        info!(count = cleaned.len(), sessions = ?cleaned, "cleaned up stale backup sessions");
    }
    let mut committing: Option<LockHolder> = None;
    let mut unread: Vec<&str> = Vec::new();
    for s in live.iter().filter(|s| s.session_id != own) {
        let key = || session_marker_key(&s.session_id);
        match &s.entry {
            Some(entry) if entry.writes_heads => {
                return Ok(Some(HeadWriter::Compact(session_holder(
                    key(),
                    Some(entry),
                    env,
                ))));
            }
            Some(entry) if entry.committing => {
                committing.get_or_insert_with(|| session_holder(key(), Some(entry), env));
            }
            Some(_) => {}
            None => unread.push(&s.session_id),
        }
    }
    if !unread.is_empty() {
        // Malformed, or gone since the LIST (a rotation the LIST half-saw,
        // or a deregistration): list again and read the newest key again.
        let keys = storage.list(SESSIONS_PREFIX)?;
        let sessions = group_session_markers(&keys);
        for id in unread {
            let Some(&(_, newest)) = sessions.get(id).and_then(|m| m.last()) else {
                debug!(
                    session_id = id,
                    "session deregistered during the single-writer listing"
                );
                continue;
            };
            let entry = storage
                .get(newest)?
                .and_then(|data| serde_json::from_slice::<SessionEntry>(&data).ok());
            match entry {
                Some(entry) if entry.writes_heads => {
                    return Ok(Some(HeadWriter::Compact(session_holder(
                        newest.to_string(),
                        Some(&entry),
                        env,
                    ))));
                }
                Some(entry) if entry.committing => {
                    committing.get_or_insert_with(|| {
                        session_holder(newest.to_string(), Some(&entry), env)
                    });
                }
                Some(_) => {}
                // Unreadable twice: fail closed, as a compact.
                None => {
                    return Ok(Some(HeadWriter::Compact(session_holder(
                        newest.to_string(),
                        None,
                        env,
                    ))));
                }
            }
        }
    }
    Ok(committing.map(HeadWriter::Committing))
}

/// An entry's age as a reaper judges it. Also judges foreign locks for
/// the v2/v3 upgrade's legacy lock key (`upgrade::LegacyUpgradeLock`).
#[derive(Clone, Copy, Debug)]
pub(crate) enum EntryAge {
    /// On the store's clock: (store now − the entry's mtime).
    Store(std::time::Duration),
    /// The listing reported no mtime (test doubles; a vykar-server older
    /// than `?list&sized&mtime`; an S3 `LastModified` that does not
    /// parse): the entry's own timestamp against `env`'s clock, as before
    /// 04 step 5.
    Timestamp(Option<Duration>),
    /// The listing reported an mtime but "now" could not be read: never
    /// stale by age (fail closed).
    Unknown,
}

impl EntryAge {
    /// The age of an entry listed with `mtime`, judged at `now`;
    /// `timestamp` gives the fallback when there is no mtime (warned once
    /// per run of `env`).
    pub(crate) fn of(
        mtime: Option<std::time::SystemTime>,
        now: Option<StoreNow>,
        env: &HostEnv,
        timestamp: impl FnOnce() -> Option<Duration>,
    ) -> Self {
        match (mtime, now) {
            (Some(mtime), Some(now)) => Self::Store(now.age_of(mtime)),
            (Some(_), None) => Self::Unknown,
            (None, _) => {
                warn_timestamp_fallback(env);
                Self::Timestamp(timestamp())
            }
        }
    }

    pub(crate) fn exceeds(self, max_age: Duration) -> bool {
        match self {
            Self::Store(age) => max_age.to_std().is_ok_and(|max| age > max),
            Self::Timestamp(age) => age.is_some_and(|age| age > max_age),
            Self::Unknown => false,
        }
    }

    /// The age as the same-host rule compares it with local uptime.
    pub(crate) fn as_std(self) -> Option<std::time::Duration> {
        match self {
            Self::Store(age) => Some(age),
            Self::Timestamp(age) => age.and_then(|a| a.to_std().ok()),
            Self::Unknown => None,
        }
    }
}

/// One warning per opened repository (`HostEnv::for_new_run`), and one
/// more for a backup's session registration, which runs before the open,
/// when a lock or marker is aged by the timestamp inside it because the
/// storage listed it without an mtime: those ages depend on the writer's
/// and this machine's clocks.
fn warn_timestamp_fallback(env: &HostEnv) {
    if env.first_timestamp_fallback() {
        warn!(
            "the storage listed locks or session markers without modification times; \
             judging their age by the timestamps inside them (client clocks)"
        );
    }
}

/// Who wrote a lock or session entry, as the same-host rule reads it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EntryHost<'a> {
    pub hostname: &'a str,
    pub pid: u32,
    pub boot_id: Option<&'a str>,
    pub installation_id: Option<&'a str>,
    pub pid_ns: Option<u64>,
    /// How long ago the entry was last written: on the store's clock (its
    /// mtime), or by its own timestamp when the listing reports no mtime.
    pub age: Option<std::time::Duration>,
}

/// What the same-host rule says about an entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SameHost {
    /// Not provably this machine: judged by age alone.
    Unknown,
    /// This machine and boot, and the process is alive.
    Alive,
    /// This machine and boot, and the process is gone: stale.
    DeadProcess,
    /// This installation, an earlier boot: older than our uptime, stale.
    EarlierBoot,
}

/// The same-host verdict on `entry` for `env`.
///
/// "This machine" needs the same hostname **and** the same installation ID
/// (both known), so namesakes (other installations) and old clients (no
/// installation ID) are judged by age alone. This narrows the old
/// same-hostname rules with one widening, for session markers: the old
/// marker rule needed a dead PID, while `EarlierBoot` reaps a marker of
/// this installation's earlier boot even when a local process now has that
/// PID number (04 step 1). With another boot ID the entry is stale only once
/// it is older than our uptime: a reboot left it behind, while a cloned
/// machine that shares the installation wrote it after both booted. With
/// the same boot (or no boot ID on either side, as on macOS) and the same
/// PID namespace, a dead PID makes it stale.
///
/// Residual: a clone that booted after its sibling's last write reaps the
/// sibling's live entry (for markers about one refresh interval after the
/// clone's boot, for the v2/v3 upgrade's legacy key up to its 5-minute
/// rewrite).
pub(crate) fn same_host(entry: &EntryHost<'_>, env: &HostEnv) -> SameHost {
    let local = &env.host;
    if entry.hostname != local.hostname {
        return SameHost::Unknown;
    }
    let (Some(mine), Some(theirs)) = (local.installation_id.as_deref(), entry.installation_id)
    else {
        return SameHost::Unknown;
    };
    if mine != theirs {
        return SameHost::Unknown;
    }
    match (local.boot_id.as_deref(), entry.boot_id) {
        (Some(a), Some(b)) if a != b => {
            // Our earlier boot, or a clone of this installation that wrote
            // it after both machines booted.
            let older = entry
                .age
                .zip(env.clock.uptime())
                .is_some_and(|(age, up)| age > up);
            return if older {
                SameHost::EarlierBoot
            } else {
                SameHost::Unknown
            };
        }
        (Some(_), Some(_)) | (None, None) => {}
        // One side does not know its boot: no claim.
        _ => return SameHost::Unknown,
    }
    if local.pid_ns != entry.pid_ns {
        return SameHost::Unknown;
    }
    if env.is_pid_alive(entry.pid) {
        SameHost::Alive
    } else {
        SameHost::DeadProcess
    }
}

// ── Legacy lock directory ────────────────────────────────────────────────

/// 0.24's lock directory, read only by the v2/v3 upgrade's legacy key.
pub(crate) const LEGACY_LOCKS_PREFIX: &str = "locks/";

/// Delete every object under `locks/`: vykar 0.24 locks (or the legacy key of
/// an interrupted v2/v3 upgrade, `locks/*.json`, counted) and the clock and
/// put-if-absent probes development builds left there (not counted). No
/// passphrase needed. Returns the number of `.json` keys removed; the first
/// failed DELETE is returned as the error. A missing directory removes nothing.
pub fn clear_legacy_locks(storage: &dyn StorageBackend) -> Result<usize> {
    let mut removed = 0usize;
    for key in storage.list(LEGACY_LOCKS_PREFIX)? {
        if !key.starts_with(LEGACY_LOCKS_PREFIX) {
            continue;
        }
        storage.delete(&key)?;
        if key.ends_with(".json") {
            removed += 1;
        }
    }
    Ok(removed)
}

// ── Session markers ──────────────────────────────────────────────────────

/// Prefix of every session object: the markers `sessions/<id>.json`
/// (generation 0, written at registration, the shape every released binary
/// writes) and `sessions/<id>.<n>.json` (the `n`-th refresh attempt), and the
/// journal `sessions/<id>.index`. Of a session's markers the newest
/// generation is the live one.
pub(crate) const SESSIONS_PREFIX: &str = "sessions/";

/// Prefix of an exclusive session's ID (10-single-protocol step 8): `x-`
/// followed by 32 hex. A shared session's ID is 32 hex and never starts with
/// it, so a listing alone tells an exclusive session's markers apart.
pub(crate) const EXCLUSIVE_ID_PREFIX: &str = "x-";

/// A fresh exclusive session ID: `x-` + 32 lowercase hex.
pub(crate) fn exclusive_session_id() -> String {
    format!("{EXCLUSIVE_ID_PREFIX}{:032x}", rand::random::<u128>())
}

/// Whether `session_id` (as `parse_session_marker_key` returns it) is an
/// exclusive session's.
pub fn is_exclusive_session(session_id: &str) -> bool {
    session_id.starts_with(EXCLUSIVE_ID_PREFIX)
}
/// Sessions whose newest marker was last written strictly longer ago than
/// this (by its mtime on the store's clock; by its `last_refresh` when the
/// listing reports no mtime) are treated as stale. Shared between maintenance cleanup
/// ([`cleanup_stale_sessions`]) and pending-index recovery.
///
/// Set to 3× the 15-minute session heartbeat interval — two missed refreshes
/// still keep a session live, a third marks it dead.
pub(crate) const SESSION_STALE_SECS: i64 = 45 * 60;

/// Storage key of generation `generation` of a session's marker:
/// `sessions/<id>.json` for 0 (written at registration, the shape every
/// released binary writes), `sessions/<id>.<n>.json` for the `n`-th refresh
/// attempt. Session IDs contain no '.' (callers generate 32 hex digits, an
/// exclusive session `x-` + 32 hex).
pub(crate) fn session_marker_key_at(session_id: &str, generation: u64) -> String {
    if generation == 0 {
        format!("{SESSIONS_PREFIX}{session_id}.json")
    } else {
        format!("{SESSIONS_PREFIX}{session_id}.{generation}.json")
    }
}

/// [`session_marker_key_at`] for generation 0: `sessions/<id>.json`.
pub(crate) fn session_marker_key(session_id: &str) -> String {
    session_marker_key_at(session_id, 0)
}

/// Storage key for a session's pending index journal: `sessions/<id>.index`.
pub(crate) fn session_index_key(session_id: &str) -> String {
    format!("{SESSIONS_PREFIX}{session_id}.index")
}

/// The session and generation a `.json` key under `sessions/` names.
///
/// Total over such keys, so every marker counts (fail closed): a stem
/// `<x>.<n>` with `n` canonical decimal ≥ 1 (no leading zero) and `x`
/// non-empty is (`x`, n); any other stem is (stem, 0). `None` for keys
/// outside `sessions/`, without the `.json` suffix, or with an empty stem.
pub(crate) fn parse_session_marker_key(key: &str) -> Option<(&str, u64)> {
    let stem = key.strip_prefix(SESSIONS_PREFIX)?.strip_suffix(".json")?;
    if stem.is_empty() {
        return None;
    }
    if let Some((id, n)) = stem.rsplit_once('.')
        && !id.is_empty()
        && !n.is_empty()
        && !n.starts_with('0')
        && n.bytes().all(|b| b.is_ascii_digit())
        && let Ok(generation) = n.parse::<u64>()
    {
        return Some((id, generation));
    }
    Some((stem, 0))
}

/// The `.json` markers among `keys`, grouped by session ID (ascending), each
/// session's keys in ascending generation: the last is the live marker.
fn group_session_markers(keys: &[String]) -> std::collections::BTreeMap<&str, Vec<(u64, &str)>> {
    let mut sessions: std::collections::BTreeMap<&str, Vec<(u64, &str)>> =
        std::collections::BTreeMap::new();
    for key in keys {
        if let Some((id, generation)) = parse_session_marker_key(key) {
            sessions
                .entry(id)
                .or_default()
                .push((generation, key.as_str()));
        }
    }
    for markers in sessions.values_mut() {
        markers.sort_unstable();
    }
    sessions
}

/// A session marker stored at `sessions/<session_id>[.<n>].json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionEntry {
    pub hostname: String,
    pub pid: u32,
    pub registered_at: String,
    pub last_refresh: String,
    #[serde(default)]
    pub boot_id: Option<String>,
    #[serde(default)]
    pub installation_id: Option<String>,
    #[serde(default)]
    pub pid_ns: Option<u64>,
    /// Time since the session's start (t0: its registration, pulled back by
    /// the store age of the oldest journal pack it recovered), suspend
    /// included, rounded up to a second, when this body was built. Compact
    /// reads it as the horizon mtime − elapsed_secs − M (04 "Compact" 3).
    /// `None` from writers before 04 step 7: their horizon is unknown (−∞).
    #[serde(default)]
    pub elapsed_secs: Option<u64>,
    /// A backup from its commit mark until deregistration: it may create
    /// index heads, and its snapshot may not be listed yet (04 step 9).
    /// Absent when false, so an unflagged marker is what it was before.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub committing: bool,
    /// A maintenance session that may create index heads, for its whole run
    /// (a compact that is not a dry run). Absent when false.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub writes_heads: bool,
}

impl SessionEntry {
    /// The marker of `env`'s process, registered at `registered_at`, last
    /// written at `last_refresh`. The one place that fills the identity
    /// fields. `elapsed_secs` is 0: registration's key is written before
    /// the session's start is stamped.
    pub(crate) fn new(
        env: &HostEnv,
        registered_at: DateTime<Utc>,
        last_refresh: DateTime<Utc>,
    ) -> Self {
        Self {
            hostname: env.host.hostname.clone(),
            pid: env.host.pid,
            registered_at: registered_at.to_rfc3339(),
            last_refresh: last_refresh.to_rfc3339(),
            boot_id: env.host.boot_id.clone(),
            installation_id: env.host.installation_id.clone(),
            pid_ns: env.host.pid_ns,
            elapsed_secs: Some(0),
            committing: false,
            writes_heads: false,
        }
    }
}

/// The flags a session marker carries (04 step 9): every key the session
/// writes after a flag is set carries it, the heartbeat's included.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MarkerFlags {
    pub committing: bool,
    pub writes_heads: bool,
}

/// `d` in whole seconds, rounded up.
fn ceil_secs(d: std::time::Duration) -> u64 {
    d.as_secs().saturating_add(u64::from(d.subsec_nanos() > 0))
}

/// When a marker write completed, on every clock a gap is measured with.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MarkerStamp {
    mono: std::time::Instant,
    uptime: Option<std::time::Duration>,
    wall: DateTime<Utc>,
}

impl MarkerStamp {
    pub(crate) fn now(clock: &dyn Clock) -> Self {
        Self {
            mono: clock.monotonic(),
            uptime: clock.uptime(),
            wall: clock.now_utc(),
        }
    }

    /// Time from `self` to `later`, suspend included: the uptime difference
    /// (`CLOCK_BOOTTIME`) when both have one, else the larger of the
    /// monotonic difference and the wall-clock difference (clamped at
    /// zero). Never less than the monotonic difference.
    pub(crate) fn until(&self, later: &MarkerStamp) -> std::time::Duration {
        let mono = later.mono.saturating_duration_since(self.mono);
        match (self.uptime, later.uptime) {
            (Some(a), Some(b)) => b.saturating_sub(a).max(mono),
            _ => {
                let wall = (later.wall - self.wall)
                    .to_std()
                    .unwrap_or(std::time::Duration::ZERO);
                wall.max(mono)
            }
        }
    }
}

/// A registered session: what [`SessionGuard::adopt`] needs.
#[derive(Debug)]
pub struct Registration {
    session_id: String,
    entry: SessionEntry,
    written: MarkerStamp,
}

impl Registration {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

/// Register a shared session (a backup). Writes a session marker, then
/// lists `sessions/` (`exclusive_listing`) for a live exclusive session
/// (`check --repair`, `upgrade`, pack maps; 10-single-protocol step 8).
///
/// While one is live, retries briefly (3 attempts, jittered backoff). If
/// still refused after retries, deletes the marker and returns
/// `Err(Locked)` naming the exclusive session's key.
///
/// Blocks the calling thread on `env.clock` between attempts. Callers running
/// inside an async runtime must invoke this via `tokio::task::spawn_blocking`
/// (see `commands/mount.rs` for the established pattern).
pub fn register_session(
    storage: &dyn StorageBackend,
    session_id: &str,
    env: &HostEnv,
) -> Result<Registration> {
    register_session_inner(storage, session_id, 3, 2000, env, MarkerFlags::default())
}

/// [`register_session`] whose registration key already carries `flags`
/// (a maintenance session, 04 step 9: a compact that may create index heads
/// registers with `writes_heads`, so the flag is written before its
/// registration lists `sessions/`). Every later key of the session carries
/// the same flags.
pub(crate) fn register_session_with(
    storage: &dyn StorageBackend,
    session_id: &str,
    env: &HostEnv,
    flags: MarkerFlags,
) -> Result<Registration> {
    register_session_inner(storage, session_id, 3, 2000, env, flags)
}

/// [`register_session`] with its retry policy: PUT the marker (deleted again
/// on a failed PUT), then [`exclusive_listing`] under [`retry_while_contended`]
/// (`max_attempts`, `base_delay_ms`); the marker is deleted on the final
/// error.
pub(crate) fn register_session_inner(
    storage: &dyn StorageBackend,
    session_id: &str,
    max_attempts: usize,
    base_delay_ms: u64,
    env: &HostEnv,
    flags: MarkerFlags,
) -> Result<Registration> {
    let key = session_marker_key(session_id);
    let now = env.now_utc();
    let entry = SessionEntry {
        committing: flags.committing,
        writes_heads: flags.writes_heads,
        ..SessionEntry::new(env, now, now)
    };
    let data = serde_json::to_vec(&entry)
        .map_err(|e| VykarError::Other(format!("session serialize: {e}")))?;
    if let Err(e) = storage.put(&key, &data) {
        // The PUT may have committed although it failed (a lost reply, or a
        // retry wait cut by a cancel): do not leave the marker behind.
        let _ = storage.delete(&key);
        return Err(e);
    }
    let written = MarkerStamp::now(env.clock.as_ref());

    match retry_while_contended(max_attempts, base_delay_ms, env.clock.as_ref(), || {
        exclusive_listing(storage, env, &key).map(|_| ())
    }) {
        Ok(()) => {
            debug!(session_id, "session registered, no exclusive session live");
            Ok(Registration {
                session_id: session_id.to_string(),
                entry,
                written,
            })
        }
        Err(e) => {
            // An exclusive session live, or an unexpected error: clean up
            // and propagate.
            let _ = storage.delete(&key);
            Err(e)
        }
    }
}

/// The shared sessions' check for a live exclusive session (10-single-protocol
/// step 8): one `list_meta` of `sessions/`; "now" on the store's clock is the
/// mtime of `own_key` (the caller's marker key it has just written, or its
/// current key) in it. Every other session whose ID is exclusive is judged
/// as [`reap_sessions`] judges a session ([`judge_session`]); a stale one is
/// reaped (its marker keys deleted, best effort, a failed delete logged and
/// the session still not blocking; its journal kept); a live one, including
/// one whose newest key is unreadable while young or gone since the LIST
/// (fail closed), fails `Locked` naming its newest key. Shared sessions are
/// not judged, not reaped and not read. `Ok(now)` when no exclusive session
/// is live. A failed listing or GET is an error.
///
/// Why it is enough: an exclusive session writes its marker before the
/// listing it decides on ([`register_exclusive`]), and the caller wrote
/// `own_key` before this listing; of an exclusive and a shared registration
/// that both passed, one listing started after the other's PUT completed
/// and, by the LIST contract, saw it (invariant X1).
pub(crate) fn exclusive_listing(
    storage: &dyn StorageBackend,
    env: &HostEnv,
    own_key: &str,
) -> Result<Option<StoreNow>> {
    let local_now = env.now_utc();
    let listing = storage.list_meta(SESSIONS_PREFIX)?;
    let now = StoreNow::of_written(&listing, own_key);
    let mtimes: std::collections::HashMap<&str, Option<std::time::SystemTime>> =
        listing.iter().map(|o| (o.key.as_str(), o.mtime)).collect();
    let keys: Vec<String> = listing.iter().map(|o| o.key.clone()).collect();
    let max_age = default_stale_session_duration();
    for (session_id, markers) in group_session_markers(&keys) {
        if !is_exclusive_session(session_id) {
            continue;
        }
        let Some(&(_, newest)) = markers.last() else {
            continue;
        };
        match judge_session(
            storage, session_id, newest, &mtimes, now, env, local_now, max_age,
        )? {
            Judged::Stale => {
                for &(_, key) in &markers {
                    if let Err(e) = storage.delete(key) {
                        warn!(session_id, key, error = %e, "could not delete a stale exclusive session's marker");
                    }
                }
                info!(session_id, "reaped a stale exclusive session");
            }
            Judged::Live(live) => {
                return Err(VykarError::Locked(session_holder(
                    newest.to_string(),
                    live.entry.as_ref(),
                    env,
                )));
            }
        }
    }
    Ok(now)
}

/// What an exclusive registration found: the registration, the sessions it
/// reaped, the other sessions still live (an exclusive run refuses unless
/// empty), and "now" on the store's clock (its own key's mtime in the
/// listing).
pub(crate) struct ExclusiveRegistration {
    pub(crate) registration: Registration,
    pub(crate) cleaned: Vec<String>,
    pub(crate) live: Vec<LiveSession>,
    pub(crate) now: Option<StoreNow>,
}

/// Register an exclusive session (10-single-protocol step 8): PUT
/// `sessions/x-<hex>.json` (a registration body, no flags; a failed PUT
/// deletes it again and returns the error), then one reap and listing of
/// `sessions/` ([`reap_sessions`] with [`ReapNow::OwnSession`], orphan
/// journals deleted as maintenance always did). A failed listing deletes the
/// marker (best effort, [`deregister_session`]) and returns
/// `Other("cannot clean up stale backup sessions (storage error: …);
/// refusing maintenance to avoid data loss")`. No retry: two exclusive
/// registrations that see each other both refuse. The caller refuses on a
/// non-empty `live` and deregisters.
pub(crate) fn register_exclusive(
    storage: &dyn StorageBackend,
    env: &HostEnv,
) -> Result<ExclusiveRegistration> {
    let session_id = exclusive_session_id();
    let key = session_marker_key(&session_id);
    let now = env.now_utc();
    let entry = SessionEntry::new(env, now, now);
    let data = serde_json::to_vec(&entry)
        .map_err(|e| VykarError::Other(format!("session serialize: {e}")))?;
    if let Err(e) = storage.put(&key, &data) {
        // The PUT may have committed although it failed: do not leave the
        // marker behind.
        let _ = storage.delete(&key);
        return Err(e);
    }
    let written = MarkerStamp::now(env.clock.as_ref());
    let (cleaned, live, now) = match reap_sessions(
        storage,
        default_stale_session_duration(),
        env,
        ReapNow::OwnSession(&session_id),
        true,
    ) {
        Ok(listed) => listed,
        Err(e) => {
            deregister_session(storage, &session_id, env);
            return Err(VykarError::Other(format!(
                "cannot clean up stale backup sessions (storage error: {e}); \
                 refusing maintenance to avoid data loss"
            )));
        }
    };
    debug!(
        session_id,
        live = live.len(),
        "exclusive session registered"
    );
    Ok(ExclusiveRegistration {
        registration: Registration {
            session_id,
            entry,
            written,
        },
        cleaned,
        live,
        now,
    })
}

/// Deregister a session before a [`SessionGuard`] exists: delete its
/// registration marker. Best-effort: retries twice on failure.
///
/// Blocks the calling thread on `env.clock` between retries. Callers running
/// inside an async runtime must invoke this via `tokio::task::spawn_blocking`
/// (see `commands/mount.rs` for the established pattern).
pub fn deregister_session(storage: &dyn StorageBackend, session_id: &str, env: &HostEnv) {
    deregister_keys(storage, session_id, &[session_marker_key(session_id)], env);
}

/// Delete `keys` of session `session_id`, each with 3 attempts 500 ms apart
/// on `env.clock`, warning after the last.
fn deregister_keys(storage: &dyn StorageBackend, session_id: &str, keys: &[String], env: &HostEnv) {
    for key in keys {
        for attempt in 0..3 {
            match storage.delete(key) {
                Ok(()) => {
                    debug!(session_id, key = %key, "session marker deleted");
                    break;
                }
                Err(e) => {
                    if attempt < 2 {
                        debug!(session_id, key = %key, error = %e, "retrying session deregister");
                        env.clock.sleep(std::time::Duration::from_millis(500));
                    } else {
                        warn!(session_id, key = %key, error = %e, "failed to deregister session after retries");
                    }
                }
            }
        }
    }
}

/// Longest stretch without a successful marker write that 04's commit rule
/// tolerates ("not refreshed for more than 30 minutes"): two refresh
/// intervals, below the 45-minute staleness.
pub(crate) const MARKER_GAP_LIMIT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// How long one EXISTS of an exclusive session's marker vouches for it
/// (10-single-protocol step 8, `commands::util::exclusive_write_check`): a
/// repair or an upgrade writes thousands of objects, and an EXISTS per write
/// would double their requests.
pub(crate) const EXCLUSIVE_CHECK_REUSE: std::time::Duration = std::time::Duration::from_secs(5);

/// What a session knows about its marker; shared by the heartbeat and the
/// commit, whose writes (the heartbeat's refreshes and rechecks, a
/// commit's [`Self::mark`]) are serialised by `write`.
///
/// The `n`-th refresh attempt PUTs a new key, generation `n`, checks with an
/// EXISTS that the key it replaces is still there, and deletes the key
/// before that one. No key is ever written twice, so a key a reaper deleted
/// cannot come back through this client's requests (a late copy of an
/// earlier PUT, delayed or duplicated by the transport, still can), and a
/// refresh that finds its previous key gone knows the marker was missing. A
/// live session keeps two keys for a whole refresh interval: a LIST that is
/// not atomic can pass the new key's position before its PUT, but then
/// reaches the old key, which still exists. The one assumption left is that a LIST does not miss a key that
/// existed for its whole duration.
///
/// Every key carries its own `elapsed_secs`: the time since the session's
/// start ([`Self::elapsed`] when its body was built), so compact can read a
/// live session's horizon (mtime − elapsed − M) from its newest key alone.
/// Generation 0's body is registration's, with `"elapsed_secs":0`.
pub(crate) struct SessionMarker {
    session_id: String,
    /// Identity and `registered_at` of every key this session writes.
    entry: SessionEntry,
    /// The session's start (t0): when its registration key was written.
    started: MarkerStamp,
    state: std::sync::Mutex<MarkerState>,
    /// Held for the whole of a refresh, a recheck or a mark: one marker
    /// write at a time, so no two writers take one generation's turn.
    write: std::sync::Mutex<()>,
}

struct MarkerState {
    /// The flags every key written from now on carries.
    flags: MarkerFlags,
    /// Generation of the key that is the marker now: the newest one whose
    /// PUT succeeded.
    current: u64,
    /// When the current key's PUT completed.
    current_written: MarkerStamp,
    /// Highest generation attempted; a key is never attempted twice.
    attempted: u64,
    /// Generations whose key may exist: the current one, earlier ones not
    /// yet deleted, keys whose PUT failed (it may have landed) and keys
    /// whose DELETE failed. Deregistration deletes all of them.
    owned: std::collections::BTreeSet<u64>,
    /// Superseded keys whose presence check failed: checked again at
    /// every heartbeat poll, at the next refresh and by the commit's probe, never deleted before an
    /// EXISTS found them.
    unchecked: std::collections::BTreeSet<u64>,
    /// Superseded keys found present, deleted by the next refresh.
    retired: std::collections::BTreeSet<u64>,
    /// When the last successful marker write completed: registration, or a
    /// write whose predecessors were all found present.
    last_ok: MarkerStamp,
    longest_gap: std::time::Duration,
    missing: bool,
    refreshes: u32,
    failed_refreshes: u32,
    /// Marker PUTs that took longer than `MARGIN` to return: a key that
    /// lands that late may carry a horizon later than the session's start,
    /// so the record no longer counts as continuous.
    slow_puts: u32,
    /// How far the session's start is pulled back: the store age of the
    /// oldest journal pack it recovered or stored pack it accepted
    /// (`existing_pack`; `Duration::MAX` for one of unknown age).
    pullback: std::time::Duration,
    /// The record was restarted at the commit mark after a gap
    /// ([`SessionMarker::restart_record`]).
    restarted: bool,
}

impl MarkerState {
    /// Apply one presence check of the superseded key `generation`: `true`
    /// if it settled the key (found or found gone). A result for a key that
    /// is no longer waiting for a check (the other checker settled it) is
    /// ignored, so a session's own delete of a settled key never reads as a
    /// reap.
    fn settle(&mut self, generation: u64, present: bool) {
        if !self.unchecked.remove(&generation) {
            return;
        }
        if present {
            self.retired.insert(generation);
        } else {
            self.owned.remove(&generation);
            self.missing = true;
        }
    }

    /// The writes up to the current key are all proven: close the stretch.
    fn close_gap(&mut self) -> std::time::Duration {
        let gap = self.last_ok.until(&self.current_written);
        self.longest_gap = self.longest_gap.max(gap);
        self.last_ok = self.current_written;
        gap
    }
}

/// A snapshot of the record, with the open stretch since the last
/// successful write included in `longest_gap`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MarkerReport {
    pub missing: bool,
    pub longest_gap: std::time::Duration,
    /// Superseded keys whose presence check has not succeeded yet.
    pub unchecked: usize,
    pub refreshes: u32,
    pub failed_refreshes: u32,
    /// Marker PUTs that took longer than `MARGIN` to return.
    pub slow_puts: u32,
    /// The record was restarted at the commit mark after a gap (the suspend
    /// rule): it judges only the time since the mark, and the pack-age term
    /// stays binding at the fences around the head PUT.
    pub restarted: bool,
}

impl MarkerReport {
    /// Never missing, every superseded key checked, never unrefreshed
    /// longer than [`MARKER_GAP_LIMIT`], and no marker PUT slower than
    /// `MARGIN`.
    pub(crate) fn continuous(&self) -> bool {
        !self.missing
            && self.unchecked == 0
            && self.longest_gap <= MARKER_GAP_LIMIT
            && self.slow_puts == 0
    }
}

/// What one refresh attempt found.
#[derive(Debug)]
pub(crate) enum RefreshOutcome {
    /// New key written, every superseded key confirmed present. `gap`: time
    /// since the last successful write.
    Refreshed { gap: std::time::Duration },
    /// New key written; a superseded key was gone: a reaper deleted it.
    Missing,
    /// The new key was written; a presence check failed, so a superseded key
    /// stays unchecked. The gap stays open; the heartbeat checks it again at
    /// its next poll.
    Unchecked(VykarError),
    /// The new key's PUT failed (it may exist). Nothing about the past is
    /// learned; the gap stays open.
    Failed(VykarError),
    /// New key written and every superseded key confirmed present, but the
    /// PUT took `took`, longer than `MARGIN`: the record is no longer
    /// continuous, and the heartbeat refreshes again at its next poll so
    /// the newest key's horizon is right again.
    Slow { took: std::time::Duration },
}

impl SessionMarker {
    pub(crate) fn new(registration: &Registration) -> Self {
        Self {
            session_id: registration.session_id.clone(),
            entry: registration.entry.clone(),
            started: registration.written,
            state: std::sync::Mutex::new(MarkerState {
                flags: MarkerFlags {
                    committing: registration.entry.committing,
                    writes_heads: registration.entry.writes_heads,
                },
                current: 0,
                current_written: registration.written,
                attempted: 0,
                owned: std::collections::BTreeSet::from([0]),
                unchecked: std::collections::BTreeSet::new(),
                retired: std::collections::BTreeSet::new(),
                last_ok: registration.written,
                longest_gap: std::time::Duration::ZERO,
                missing: false,
                refreshes: 0,
                failed_refreshes: 0,
                slow_puts: 0,
                pullback: std::time::Duration::ZERO,
                restarted: false,
            }),
            write: std::sync::Mutex::new(()),
        }
    }

    /// Pull the session's start back by `age` (the store age of a
    /// recovered journal pack, or of a pack an append-only server already
    /// held, `existing_pack`); keeps the largest. The next refresh writes
    /// it.
    pub(crate) fn pull_back(&self, age: std::time::Duration) {
        let mut st = self.lock();
        st.pullback = st.pullback.max(age);
    }

    /// Time since the session's start: from registration's write to now,
    /// suspend included ([`MarkerStamp::until`]), plus the pull-back;
    /// saturating (a pull-back by an unknown age is `Duration::MAX`).
    pub(crate) fn elapsed(&self, clock: &dyn Clock) -> std::time::Duration {
        let now = MarkerStamp::now(clock);
        self.started
            .until(&now)
            .saturating_add(self.lock().pullback)
    }

    /// The commit's probe found the current key gone: record the marker as
    /// missing, so every later check of the record (the fence before and
    /// after the head PUT) sees it.
    pub(crate) fn note_missing(&self) {
        self.lock().missing = true;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MarkerState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn key(&self, generation: u64) -> String {
        session_marker_key_at(&self.session_id, generation)
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The key that is the marker now, with its generation.
    pub(crate) fn current(&self) -> (u64, String) {
        let generation = self.lock().current;
        (generation, self.key(generation))
    }

    /// One refresh attempt (the heartbeat), serialised with every other
    /// marker write by `write`.
    ///
    /// PUTs the next generation's key; then checks the key it replaces and
    /// every superseded key whose earlier check failed; then deletes the
    /// keys an earlier refresh found present. Nothing is written over an
    /// existing key and nothing is written when a write fails, so a reaped
    /// key stays deleted and a failed read cannot overwrite a live marker.
    pub(crate) fn refresh(
        &self,
        storage: &dyn StorageBackend,
        clock: &dyn Clock,
    ) -> RefreshOutcome {
        let _write = self.write_turn();
        self.refresh_locked(storage, clock)
    }

    /// Set `flags` and write a new key carrying them (04 step 9, the
    /// writer's half of the marker handshake): one refresh
    /// attempt, serialised with the heartbeat. `Ok(key)`: the key whose PUT
    /// succeeded (it carries the flags); what the attempt found about
    /// superseded keys (gone, unchecked) and a slow PUT is recorded as a
    /// heartbeat refresh records it. `Err`: the PUT failed; the flags stay
    /// set for later refreshes.
    pub(crate) fn mark(
        &self,
        storage: &dyn StorageBackend,
        clock: &dyn Clock,
        flags: MarkerFlags,
    ) -> Result<String> {
        let _write = self.write_turn();
        self.lock().flags = flags;
        match self.refresh_locked(storage, clock) {
            RefreshOutcome::Failed(e) => Err(e),
            outcome => {
                debug!(
                    session_id = self.session_id,
                    ?flags,
                    ?outcome,
                    "session marker flagged"
                );
                Ok(self.current().1)
            }
        }
    }

    /// The flags every key written from now on carries.
    #[cfg(test)]
    pub(crate) fn flags(&self) -> MarkerFlags {
        self.lock().flags
    }

    fn write_turn(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// [`Self::refresh`] with `write` held.
    fn refresh_locked(&self, storage: &dyn StorageBackend, clock: &dyn Clock) -> RefreshOutcome {
        let (generation, to_check, flags) = {
            let mut st = self.lock();
            st.attempted += 1;
            let generation = st.attempted;
            st.owned.insert(generation);
            let mut to_check: Vec<u64> = st.unchecked.iter().copied().collect();
            to_check.push(st.current);
            (generation, to_check, st.flags)
        };
        let entry = SessionEntry {
            last_refresh: clock.now_utc().to_rfc3339(),
            elapsed_secs: Some(ceil_secs(self.elapsed(clock))),
            committing: flags.committing,
            writes_heads: flags.writes_heads,
            ..self.entry.clone()
        };
        // On the suspend-aware clock, as `elapsed_secs`: a suspend during
        // the PUT delays the key's mtime past the instant its body was built.
        let put_start = MarkerStamp::now(clock);
        let put = serde_json::to_vec(&entry)
            .map_err(|e| VykarError::Other(format!("session serialize: {e}")))
            .and_then(|data| storage.put(&self.key(generation), &data));
        if let Err(e) = put {
            self.lock().failed_refreshes += 1;
            return RefreshOutcome::Failed(e);
        }
        let written = MarkerStamp::now(clock);
        let took = put_start.until(&written);
        let slow = took > MARGIN;
        let checks: Vec<(u64, Result<bool>)> = to_check
            .into_iter()
            .map(|g| (g, storage.exists(&self.key(g))))
            .collect();

        let (outcome, to_delete) = {
            let mut st = self.lock();
            // Before any delete: the probe follows the marker to the new key.
            let previous = st.current;
            st.current = generation;
            st.current_written = written;
            st.unchecked.insert(previous);
            // Found present by an earlier refresh: two generations back now.
            let to_delete: Vec<u64> = std::mem::take(&mut st.retired).into_iter().collect();
            let missing_before = st.missing;
            let mut error: Option<VykarError> = None;
            for (g, result) in checks {
                match result {
                    Ok(present) => st.settle(g, present),
                    Err(e) => {
                        error.get_or_insert(e);
                    }
                }
            }
            let outcome = if st.missing && !missing_before {
                st.last_ok = written;
                RefreshOutcome::Missing
            } else if let Some(e) = error {
                st.failed_refreshes += 1;
                RefreshOutcome::Unchecked(e)
            } else {
                // Every check answered, so every superseded key is settled.
                st.refreshes += 1;
                let gap = st.close_gap();
                if slow {
                    RefreshOutcome::Slow { took }
                } else {
                    RefreshOutcome::Refreshed { gap }
                }
            };
            if slow {
                st.slow_puts = st.slow_puts.saturating_add(1);
            }
            (outcome, to_delete)
        };

        for g in to_delete {
            match storage.delete(&self.key(g)) {
                Ok(()) => {
                    self.lock().owned.remove(&g);
                }
                Err(e) => {
                    debug!(session_id = self.session_id, generation = g, error = %e, "failed to delete a superseded session marker; deregistration retries");
                }
            }
        }
        outcome
    }

    /// Check every superseded key whose earlier check failed again (one
    /// found gone records the marker as missing). `Ok(Some(gap))` when that
    /// settled them all with none gone, which closes the open stretch at the
    /// current key; `Ok(None)` when there was nothing to settle or a key was
    /// gone; the first error when a check failed and a key is still
    /// unchecked.
    fn check_unchecked(&self, storage: &dyn StorageBackend) -> Result<Option<std::time::Duration>> {
        let unchecked: Vec<u64> = self.lock().unchecked.iter().copied().collect();
        if unchecked.is_empty() {
            return Ok(None);
        }
        let mut error: Option<VykarError> = None;
        for g in unchecked {
            match storage.exists(&self.key(g)) {
                Ok(present) => self.lock().settle(g, present),
                Err(e) => {
                    error.get_or_insert(e);
                }
            }
        }
        let mut st = self.lock();
        if st.unchecked.is_empty() {
            return Ok((!st.missing).then(|| st.close_gap()));
        }
        match error {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }

    /// The heartbeat's poll between refreshes while a superseded key is
    /// unchecked: check those keys again and write nothing. `Refreshed`
    /// when all are settled present (`gap`: the stretch this closed),
    /// `Missing` when one was gone, `Unchecked` when a check failed again.
    pub(crate) fn recheck(&self, storage: &dyn StorageBackend) -> RefreshOutcome {
        let _write = self.write_turn();
        let missing_before = self.lock().missing;
        let result = self.check_unchecked(storage);
        if self.lock().missing && !missing_before {
            return RefreshOutcome::Missing;
        }
        match result {
            Ok(gap) => RefreshOutcome::Refreshed {
                gap: gap.unwrap_or_default(),
            },
            Err(e) => RefreshOutcome::Unchecked(e),
        }
    }

    /// Whether the marker exists now: first every superseded key whose
    /// check failed is checked again (one found gone records the marker as
    /// missing; one whose check fails again fails the probe with that
    /// error, since a reap of it cannot be ruled out), then EXISTS of the
    /// current key; when that finds nothing and a refresh moved the marker
    /// meanwhile, again with the new key (at most 3 rounds, then `Err`).
    pub(crate) fn probe(&self, storage: &dyn StorageBackend) -> Result<bool> {
        self.check_unchecked(storage)?;
        for _ in 0..3 {
            let (generation, key) = self.current();
            if storage.exists(&key)? {
                return Ok(true);
            }
            if self.lock().current == generation {
                return Ok(false);
            }
        }
        Err(VykarError::Other(format!(
            "session {} marker moved during each of three probes",
            self.session_id
        )))
    }

    pub(crate) fn report(&self, clock: &dyn Clock) -> MarkerReport {
        let now = MarkerStamp::now(clock);
        let st = self.lock();
        MarkerReport {
            missing: st.missing,
            longest_gap: st.longest_gap.max(st.last_ok.until(&now)),
            unchecked: st.unchecked.len(),
            refreshes: st.refreshes,
            failed_refreshes: st.failed_refreshes,
            slow_puts: st.slow_puts,
            restarted: st.restarted,
        }
    }

    /// The suspend rule (10-single-protocol step 7, open decision 3): forget
    /// what the record learned before the commit mark (gaps, a miss, slow
    /// PUTs, superseded keys whose check failed), so that from here on it
    /// judges only the time since the mark. Called once, right after a
    /// successful mark and probe of a backup whose record was not
    /// continuous, and only after the pack-age term passed at the mark. The
    /// record stays marked `restarted`, which keeps the pack-age term
    /// binding at the fences around the head PUT ([`head_landing_check`],
    /// [`commit_marker_check`]). `elapsed` (time since the session's
    /// start) is not reset.
    pub(crate) fn restart_record(&self) {
        let _write = self.write_turn();
        let mut st = self.lock();
        st.missing = false;
        st.longest_gap = std::time::Duration::ZERO;
        st.slow_puts = 0;
        st.last_ok = st.current_written;
        // Keys whose presence was never settled are deleted by the next
        // refresh, as settled ones are; their outcome no longer counts.
        let unchecked = std::mem::take(&mut st.unchecked);
        st.retired.extend(unchecked);
        st.restarted = true;
    }

    /// Every key that may exist, for deregistration.
    pub(crate) fn owned_keys(&self) -> Vec<String> {
        let owned: Vec<u64> = self.lock().owned.iter().copied().collect();
        owned.into_iter().map(|g| self.key(g)).collect()
    }
}

/// 04 "Why markers carry weight" and I8. The marker record is sound when
/// the probe found the current key and the record is
/// [continuous](MarkerReport::continuous), or the probe failed and the
/// record is continuous. A backup whose record is not sound (a marker
/// recorded missing, a current key found gone, a superseded key unchecked,
/// a gap over [`MARKER_GAP_LIMIT`] or a slow marker write) commits only
/// while `elapsed` + [`HEAD_LANDING_ALLOWANCE`] <= [`PACK_AGE_LIMIT`]: none
/// of its packs (its uploads, and recovered packs through the pull-back)
/// can be old enough for compact yet, and compact's age guard protects
/// them where the marker cannot. Otherwise it refuses:
/// `StaleChunksDuringCommit` for a missing marker, a current key found gone
/// or a gap; `Other` for an unchecked superseded key; the probe's error for
/// a failed probe.
pub(crate) fn commit_marker_check(
    session_id: &str,
    probe: Result<bool>,
    report: &MarkerReport,
    elapsed: std::time::Duration,
) -> Result<()> {
    let (refusal, reason) = if report.missing {
        (
            VykarError::StaleChunksDuringCommit,
            "the session marker was missing at a refresh",
        )
    } else {
        match probe {
            Ok(false) => (
                VykarError::StaleChunksDuringCommit,
                "the session marker was reaped before the commit",
            ),
            Ok(true) if report.unchecked > 0 => (
                VykarError::Other(format!(
                    "session {session_id}: {} superseded marker key(s) could not be checked for a reap",
                    report.unchecked
                )),
                "a superseded session marker key could not be checked for a reap",
            ),
            Ok(true) if !report.continuous() => (
                VykarError::StaleChunksDuringCommit,
                "the session marker went unrefreshed for a while (machine suspended?) or a marker write was slow",
            ),
            Ok(true) if report.restarted => (
                VykarError::StaleChunksDuringCommit,
                "the session marker's record was restarted at the commit mark after a gap",
            ),
            Ok(true) => return Ok(()),
            Err(e) if report.continuous() => {
                warn!(
                    session_id,
                    error = %e,
                    "could not probe the session marker; refreshed without a gap, proceeding"
                );
                return Ok(());
            }
            Err(e) => (
                e,
                "could not probe the session marker and its refreshes are not continuous",
            ),
        }
    };
    pack_age_term(
        session_id,
        reason,
        refusal,
        report,
        elapsed,
        HEAD_LANDING_ALLOWANCE,
    )
}

/// The commit rule's pack-age term at the fences around the head PUT, from
/// the record alone (no probe): `Err(StaleChunksDuringCommit)` when the
/// record is not continuous and `elapsed` + `allowance` >
/// [`PACK_AGE_LIMIT`]. Before the head PUT `allowance` is
/// [`HEAD_LANDING_ALLOWANCE`] (an early refusal); after it returned, zero
/// (the binding check: the head has landed by then). A record restarted at
/// the commit mark ([`SessionMarker::restart_record`]) is judged as one
/// that is not continuous, however continuous it is since.
pub(crate) fn head_landing_check(
    session_id: &str,
    report: &MarkerReport,
    elapsed: std::time::Duration,
    allowance: std::time::Duration,
) -> Result<()> {
    if (report.continuous() && !report.restarted)
        || elapsed.saturating_add(allowance) <= PACK_AGE_LIMIT
    {
        return Ok(());
    }
    let reason = if report.restarted {
        "the session marker's record was restarted at the commit mark after a gap"
    } else {
        "the session marker record is not continuous"
    };
    pack_age_term(
        session_id,
        reason,
        VykarError::StaleChunksDuringCommit,
        report,
        elapsed,
        allowance,
    )
}

/// Commit (with a warning) while `elapsed` + `allowance` <=
/// [`PACK_AGE_LIMIT`], else `Err(refusal)`; `reason`: why the marker
/// record is not sound.
fn pack_age_term(
    session_id: &str,
    reason: &str,
    refusal: VykarError,
    report: &MarkerReport,
    elapsed: std::time::Duration,
    allowance: std::time::Duration,
) -> Result<()> {
    if elapsed.saturating_add(allowance) <= PACK_AGE_LIMIT {
        warn!(
            session_id,
            reason,
            elapsed_secs = elapsed.as_secs(),
            "the session marker cannot protect this backup's uploads, but they are young enough to be kept by age; committing"
        );
        return Ok(());
    }
    warn!(
        session_id,
        reason,
        elapsed_secs = elapsed.as_secs(),
        allowance_secs = allowance.as_secs(),
        gap_secs = report.longest_gap.as_secs(),
        unchecked = report.unchecked,
        "the session marker cannot protect this backup's uploads and they may be old enough for compact to delete; refusing to commit, retry the backup"
    );
    Err(refusal)
}

/// How often the heartbeat refreshes the session marker.
pub(crate) const REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15 * 60);
/// The heartbeat's poll on `env.clock`.
pub(crate) const HEARTBEAT_POLL: std::time::Duration = std::time::Duration::from_secs(30);
/// The longest wait before a failed marker PUT is tried again.
pub(crate) const PUT_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(2 * 60);

/// What the heartbeat does at one poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeartbeatAction {
    Wait,
    /// [`SessionMarker::recheck`]: check the unchecked superseded keys,
    /// write nothing.
    Recheck,
    /// [`SessionMarker::refresh`]: write the next generation's key.
    Refresh,
}

/// When the heartbeat acts, counted in polls of [`HEARTBEAT_POLL`], not
/// elapsed time, so a single long jump of the clock (a suspend) counts as
/// one poll.
///
/// A refresh is due [`REFRESH_INTERVAL`] after the last attempt; after
/// `k` failed PUTs in a row, `30 s · 2^(k−1)` after it (the next poll,
/// then 1 min), capped at [`PUT_RETRY_MAX`]: the cap is small against the
/// slack between the refresh interval and the marker staleness (30 min),
/// so a session whose store comes back writes its marker again before a
/// reaper can take it for stale. While a
/// superseded key is unchecked (a presence check failed, the PUT
/// succeeded), every poll before the next refresh rechecks it and writes
/// nothing, so an outage of presence checks adds one key per refresh
/// interval and its checks grow linearly. The gap bookkeeping is the
/// marker's: a stretch stays open until a write is proven.
#[derive(Debug)]
pub(crate) struct HeartbeatSchedule {
    since_attempt: std::time::Duration,
    due: std::time::Duration,
    failed_puts: u32,
    recheck: bool,
}

impl HeartbeatSchedule {
    pub(crate) fn new() -> Self {
        Self {
            since_attempt: std::time::Duration::ZERO,
            due: REFRESH_INTERVAL,
            failed_puts: 0,
            recheck: false,
        }
    }

    /// One poll has passed: what to do now.
    pub(crate) fn poll(&mut self) -> HeartbeatAction {
        self.since_attempt += HEARTBEAT_POLL;
        if self.since_attempt >= self.due {
            HeartbeatAction::Refresh
        } else if self.recheck {
            HeartbeatAction::Recheck
        } else {
            HeartbeatAction::Wait
        }
    }

    /// What `action` (a [`Recheck`](HeartbeatAction::Recheck) or a
    /// [`Refresh`](HeartbeatAction::Refresh)) found.
    pub(crate) fn record(&mut self, action: HeartbeatAction, outcome: &RefreshOutcome) {
        if action == HeartbeatAction::Refresh {
            self.since_attempt = std::time::Duration::ZERO;
            if matches!(outcome, RefreshOutcome::Failed(_)) {
                self.failed_puts = self.failed_puts.saturating_add(1);
                let doublings = (self.failed_puts - 1).min(16);
                self.due = (HEARTBEAT_POLL * (1u32 << doublings)).min(PUT_RETRY_MAX);
                return;
            }
            self.failed_puts = 0;
            // A slow PUT: write a key with a right horizon at the next poll.
            self.due = if matches!(outcome, RefreshOutcome::Slow { .. }) {
                HEARTBEAT_POLL
            } else {
                REFRESH_INTERVAL
            };
        }
        self.recheck = matches!(outcome, RefreshOutcome::Unchecked(_));
    }
}

/// The name of every session's heartbeat thread.
pub(crate) const HEARTBEAT_THREAD_NAME: &str = "session-refresh";

/// RAII guard that deregisters a session on drop and periodically refreshes
/// the session marker so maintenance doesn't treat it as stale.
pub struct SessionGuard {
    storage: Arc<dyn StorageBackend>,
    marker: Arc<SessionMarker>,
    refresh_handle: Option<JoinHandle<()>>,
    stop: Arc<StopSignal>,
    env: HostEnv,
}

impl SessionGuard {
    /// Adopt an already-registered session. Starts the refresh thread but
    /// does NOT call `register_session()` — the caller must have done that
    /// before opening the repo (mirrors backup's ordering).
    pub fn adopt(
        storage: Arc<dyn StorageBackend>,
        registration: Registration,
        env: HostEnv,
    ) -> Result<Self> {
        let marker = Arc::new(SessionMarker::new(&registration));
        let stop = Arc::new(StopSignal::new());
        let handle = Self::spawn_refresher(
            Arc::clone(&storage),
            Arc::clone(&marker),
            Arc::clone(&stop),
            env.clone(),
        )?;
        Ok(Self {
            storage,
            marker,
            refresh_handle: Some(handle),
            stop,
            env,
        })
    }

    pub fn session_id(&self) -> &str {
        self.marker.session_id()
    }

    /// The session's marker record, for the commit.
    pub(crate) fn marker(&self) -> Arc<SessionMarker> {
        Arc::clone(&self.marker)
    }

    /// The heartbeat polls every 30 s on `env.clock` and acts as
    /// [`HeartbeatSchedule`] says.
    fn spawn_refresher(
        storage: Arc<dyn StorageBackend>,
        marker: Arc<SessionMarker>,
        stop: Arc<StopSignal>,
        env: HostEnv,
    ) -> Result<JoinHandle<()>> {
        std::thread::Builder::new()
            .name(HEARTBEAT_THREAD_NAME.into())
            .spawn(move || {
                let session_id = marker.session_id();
                let mut schedule = HeartbeatSchedule::new();
                loop {
                    if env.clock.wait_timeout(&stop, HEARTBEAT_POLL) {
                        break;
                    }
                    let action = schedule.poll();
                    let outcome = match action {
                        HeartbeatAction::Wait => continue,
                        HeartbeatAction::Recheck => marker.recheck(storage.as_ref()),
                        HeartbeatAction::Refresh => {
                            marker.refresh(storage.as_ref(), env.clock.as_ref())
                        }
                    };
                    match &outcome {
                        RefreshOutcome::Refreshed { gap } => {
                            if *gap > MARKER_GAP_LIMIT {
                                warn!(
                                    session_id,
                                    gap_secs = gap.as_secs(),
                                    "session marker went unrefreshed for a while (machine suspended?)"
                                );
                            } else {
                                debug!(session_id, ?action, "session marker refreshed");
                            }
                        }
                        RefreshOutcome::Missing => warn!(
                            session_id,
                            "session marker was removed by another client while this session ran; \
                             a backup in this session will not commit"
                        ),
                        RefreshOutcome::Unchecked(e) if action == HeartbeatAction::Recheck => {
                            debug!(session_id, error = %e, "superseded session marker key still unchecked");
                        }
                        RefreshOutcome::Unchecked(e) => warn!(
                            session_id,
                            error = %e,
                            "could not check a superseded session marker key; checking again at the next poll"
                        ),
                        RefreshOutcome::Failed(e) => {
                            warn!(session_id, error = %e, "failed to refresh session marker");
                        }
                        RefreshOutcome::Slow { took } => warn!(
                            session_id,
                            took_secs = took.as_secs(),
                            "a session marker write took longer than the margin; \
                             refreshing again, and a commit of this session must pass the pack-age rule"
                        ),
                    }
                    schedule.record(action, &outcome);
                }
            })
            .map_err(|e| VykarError::Other(format!("failed to spawn session refresh thread: {e}")))
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.stop.signal();
        if let Some(handle) = self.refresh_handle.take() {
            let _ = handle.join();
        }
        deregister_keys(
            self.storage.as_ref(),
            self.marker.session_id(),
            &self.marker.owned_keys(),
            &self.env,
        );
    }
}

/// List the IDs of all sessions with a marker, sorted, one per session.
pub fn list_sessions(storage: &dyn StorageBackend) -> Result<Vec<String>> {
    let keys = storage.list(SESSIONS_PREFIX)?;
    Ok(group_session_markers(&keys)
        .into_keys()
        .map(String::from)
        .collect())
}

/// Remove sessions whose marker is older than `max_age`, or left on this
/// machine. Returns the IDs of cleaned sessions.
///
/// A session is judged by its newest listed marker key; when that one is
/// stale, every listed key of the session is deleted, and an older key of a
/// session whose newest key is fresh is kept.
///
/// Ages are on the store's clock: (`now` − the marker key's mtime), `now`
/// being the mtime of an object the caller has just written. The timestamps inside a marker are used only
/// when the listing reports no mtime; with an mtime but no `now` nothing is
/// stale by age (fail closed). A malformed marker (unparseable JSON or bad
/// timestamps) is judged by its mtime alone: it is preserved, and keeps
/// compact from deleting orphan packs, while younger than `max_age`, and is reaped like any stale
/// marker after that; without an mtime it is preserved.
///
/// `.index` journals for cleaned sessions are **preserved** so the next backup
/// can recover uploaded-but-uncommitted chunks via `recover_pending_index()`.
/// Orphaned `.index` files from a *prior* cleanup run (no companion `.json`
/// and not cleaned this invocation) are deleted — they already had their
/// grace period. With `now` known, probes under `sessions/` (the
/// put-if-absent check's, the existing-pack clock's) older than an hour
/// (debris of a failed delete) are removed too, best effort.
///
/// `env` gives the fallback time and enables same-host detection: if
/// `same_host` finds the session was left by a dead process or an earlier
/// boot of this machine, it is treated as stale regardless of age. Sessions
/// from other hosts, other installations and old clients are only cleaned
/// by the `max_age` timeout.
pub fn cleanup_stale_sessions(
    storage: &dyn StorageBackend,
    max_age: Duration,
    env: &HostEnv,
    now: Option<StoreNow>,
) -> Result<Vec<String>> {
    cleanup_stale_sessions_keeping(storage, max_age, env, now).map(|(cleaned, _)| cleaned)
}

/// [`cleanup_stale_sessions`], also returning the sessions it kept, in
/// ascending session ID order, from the same listing and GETs of each
/// session's newest key: one whose newest key is malformed, or gone since
/// the LIST, has `entry: None` (no horizon). Maintenance uses it so
/// `sessions/` is listed once. Removes old probe debris as that does.
pub(crate) fn cleanup_stale_sessions_keeping(
    storage: &dyn StorageBackend,
    max_age: Duration,
    env: &HostEnv,
    now: Option<StoreNow>,
) -> Result<(Vec<String>, Vec<LiveSession>)> {
    reap_sessions(storage, max_age, env, ReapNow::Known(now), true)
        .map(|(cleaned, kept, _)| (cleaned, kept))
}

/// Where [`reap_sessions`] reads "now" on the store's clock.
pub(crate) enum ReapNow<'a> {
    /// Read already by the caller (the mtime of a key it wrote; `None`:
    /// unknown).
    Known(Option<StoreNow>),
    /// The caller's own registered session (a maintenance run, 04 step
    /// 9): "now" is the mtime of its newest key in this listing,
    /// written before the listing; the session itself is neither reaped
    /// nor returned as live.
    OwnSession(&'a str),
}

/// The stale-session reap and live listing of a maintenance run
/// (04 step 9, compact's sessions #1): [`cleanup_stale_sessions_keeping`]
/// from one listing of `sessions/`, "now" being the mtime of `own`'s
/// newest key in it ([`ReapNow::OwnSession`]). Returns the cleaned
/// sessions, the live ones other than `own`, and that "now" (`None`
/// when the listing reports no mtime for it).
pub(crate) fn reap_sessions_registered(
    storage: &dyn StorageBackend,
    env: &HostEnv,
    own: &str,
) -> Result<(Vec<String>, Vec<LiveSession>, Option<StoreNow>)> {
    reap_sessions(
        storage,
        default_stale_session_duration(),
        env,
        ReapNow::OwnSession(own),
        true,
    )
}

/// How one session's newest listed marker key is judged (the body of
/// [`reap_sessions`]' first pass).
enum Judged {
    /// Stale by age, or left on this machine: every listed key of it may be
    /// deleted.
    Stale,
    /// Kept: live, or unreadable while young or without an mtime, or gone
    /// since the LIST.
    Live(LiveSession),
}

/// Judge session `session_id` by its newest listed marker key `newest`, the
/// listing's `mtimes`, "now" on the store's clock and `local_now`. GETs
/// `newest` once; a GET that finds nothing is live with no entry (the
/// session may still be live: a rotation or a deregistration). A malformed
/// body is judged by its mtime alone; otherwise the session is stale when
/// older than `max_age` or left on this machine ([`same_host`]).
#[allow(clippy::too_many_arguments)]
fn judge_session(
    storage: &dyn StorageBackend,
    session_id: &str,
    newest: &str,
    mtimes: &std::collections::HashMap<&str, Option<std::time::SystemTime>>,
    now: Option<StoreNow>,
    env: &HostEnv,
    local_now: DateTime<Utc>,
    max_age: Duration,
) -> Result<Judged> {
    let mtime = mtimes.get(newest).copied().flatten();
    let keep = |entry: Option<SessionEntry>| {
        Judged::Live(LiveSession {
            session_id: session_id.to_string(),
            entry,
            mtime,
        })
    };
    let Some(data) = storage.get(newest)? else {
        // Gone since the LIST (a rotation or a deregistration): the
        // session may still be live, so its journal is kept.
        return Ok(keep(None));
    };
    let entry = serde_json::from_slice::<SessionEntry>(&data).ok();
    let ts = entry.as_ref().and_then(|entry| {
        chrono::DateTime::parse_from_rfc3339(&entry.last_refresh)
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(&entry.registered_at))
            .ok()
            .map(|ts| ts.with_timezone(&Utc))
    });
    let age = EntryAge::of(mtime, now, env, || {
        ts.map(|ts| local_now.signed_duration_since(ts))
    });
    let (Some(entry), Some(_)) = (entry, ts) else {
        // Unparseable .json or a bad timestamp: we cannot read who wrote
        // it. Judged by its mtime alone; while young (or without an
        // mtime) it is preserved, so compact's horizon is −∞ while it is
        // young (fail-closed), together with its `.index`.
        if matches!(age, EntryAge::Store(_)) && age.exceeds(max_age) {
            debug!(
                session_id,
                ?age,
                "cleaning malformed session marker, stale by its mtime"
            );
            return Ok(Judged::Stale);
        }
        return Ok(keep(None));
    };

    let is_stale_by_age = age.exceeds(max_age);
    let verdict = same_host(
        &EntryHost {
            hostname: &entry.hostname,
            pid: entry.pid,
            boot_id: entry.boot_id.as_deref(),
            installation_id: entry.installation_id.as_deref(),
            pid_ns: entry.pid_ns,
            age: age.as_std(),
        },
        env,
    );
    let is_left_local = matches!(verdict, SameHost::DeadProcess | SameHost::EarlierBoot);
    if is_stale_by_age {
        debug!(session_id, ?age, "cleaning stale session");
        Ok(Judged::Stale)
    } else if is_left_local {
        debug!(
            session_id,
            pid = entry.pid,
            ?verdict,
            "cleaning session left on this machine"
        );
        Ok(Judged::Stale)
    } else {
        Ok(keep(Some(entry)))
    }
}

/// [`cleanup_stale_sessions_keeping`]; `delete_orphan_journals`: whether its
/// second pass runs (a backup's single-writer listing skips it, so a
/// crashed session's journal keeps its grace until the next maintenance, as
/// before backups listed sessions at their commit). The second pass also removes
/// probe debris older than [`CLOCK_PROBE_DEBRIS_AGE`] when "now" is known.
/// Also returns the "now" it judged ages by.
fn reap_sessions(
    storage: &dyn StorageBackend,
    max_age: Duration,
    env: &HostEnv,
    now: ReapNow<'_>,
    delete_orphan_journals: bool,
) -> Result<(Vec<String>, Vec<LiveSession>, Option<StoreNow>)> {
    let local_now = env.now_utc();
    let listing = storage.list_meta(SESSIONS_PREFIX)?;
    let mtimes: std::collections::HashMap<&str, Option<std::time::SystemTime>> =
        listing.iter().map(|o| (o.key.as_str(), o.mtime)).collect();
    let keys: Vec<String> = listing.iter().map(|o| o.key.clone()).collect();
    let (now, own) = match now {
        ReapNow::Known(now) => (now, None),
        ReapNow::OwnSession(own) => {
            let newest = group_session_markers(&keys)
                .get(own)
                .and_then(|m| m.last())
                .map(|&(_, key)| key.to_string());
            let now = newest.and_then(|key| StoreNow::of_written(&listing, &key));
            (now, Some(own))
        }
    };
    let mut cleaned = Vec::new();
    let mut kept = Vec::new();
    // Track which session IDs survived the first pass (not deleted).
    let mut surviving_markers: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Track session IDs cleaned in this invocation — their .index files get
    // a one-run grace period so the next backup can recover the journal.
    let mut cleaned_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

    // First pass: the .json session markers, per session.
    for (session_id, markers) in group_session_markers(&keys) {
        if Some(session_id) == own {
            surviving_markers.insert(session_id.to_string());
            continue;
        }
        let Some(&(_, newest)) = markers.last() else {
            continue;
        };
        match judge_session(
            storage, session_id, newest, &mtimes, now, env, local_now, max_age,
        )? {
            Judged::Stale => {
                for &(_, key) in &markers {
                    storage.delete(key)?;
                }
                cleaned_ids.insert(session_id.to_string());
                cleaned.push(session_id.to_string());
            }
            Judged::Live(live) => {
                surviving_markers.insert(session_id.to_string());
                kept.push(live);
            }
        }
    }

    // Second pass: clean orphaned .index files whose .json marker no longer exists
    // AND that were not cleaned in *this* invocation. Files cleaned this run get
    // a one-run grace period so the next backup can recover their journal via
    // `recover_pending_index()`.
    for key in keys.iter().filter(|_| delete_orphan_journals) {
        if let Some(id) = key
            .strip_prefix(SESSIONS_PREFIX)
            .and_then(|s| s.strip_suffix(".index"))
            && !surviving_markers.contains(id)
            && !cleaned_ids.contains(id)
        {
            storage.delete(key)?;
        }
        if let Some(now) = now
            && is_probe_key(key)
            && !key.ends_with(".json")
            && !key.ends_with(".index")
            && mtimes
                .get(key.as_str())
                .copied()
                .flatten()
                .is_some_and(|m| now.age_of(m) > CLOCK_PROBE_DEBRIS_AGE)
            && let Err(e) = storage.delete(key)
        {
            debug!(key, error = %e, "could not delete an old probe");
        }
    }

    Ok((cleaned, kept, now))
}

/// Sessions #2 of a compact (04 step 9, O3): one listing of
/// `sessions/` and a GET of each other session's newest key. Returns the
/// IDs of the sessions that are committing: their newest key carries
/// `committing`, or is malformed, or is gone on GET twice (fail closed: a
/// gone newest key can be a rotation the listing half-saw). A key gone on
/// GET is listed again once; a session with no key left then has
/// deregistered, which a backup does only after its snapshot PUT. Reaps
/// nothing. `own` is the caller's session.
pub(crate) fn committing_sessions(storage: &dyn StorageBackend, own: &str) -> Result<Vec<String>> {
    let keys = storage.list(SESSIONS_PREFIX)?;
    let mut committing = Vec::new();
    let mut unread: Vec<String> = Vec::new();
    for (session_id, markers) in group_session_markers(&keys) {
        if session_id == own {
            continue;
        }
        let Some(&(_, newest)) = markers.last() else {
            continue;
        };
        match storage.get(newest)? {
            None => unread.push(session_id.to_string()),
            Some(data) => match serde_json::from_slice::<SessionEntry>(&data) {
                Ok(entry) if !entry.committing => {}
                _ => committing.push(session_id.to_string()),
            },
        }
    }
    if !unread.is_empty() {
        let keys = storage.list(SESSIONS_PREFIX)?;
        let sessions = group_session_markers(&keys);
        for id in unread {
            let Some(&(_, newest)) = sessions.get(id.as_str()).and_then(|m| m.last()) else {
                debug!(
                    session_id = id,
                    "session deregistered during compact's second session listing"
                );
                continue;
            };
            let entry = storage
                .get(newest)?
                .and_then(|data| serde_json::from_slice::<SessionEntry>(&data).ok());
            if entry.is_none_or(|e| e.committing) {
                committing.push(id);
            }
        }
    }
    committing.sort();
    Ok(committing)
}

/// List all sessions with their parsed marker, one per session, in
/// ascending session ID order.
///
/// Returns `(session_id, Option<SessionEntry>)` pairs for the newest listed
/// marker key of each session. The entry is `None` if that marker could not
/// be parsed (malformed JSON) or vanished since the LIST; a session with any
/// listed key is reported.
pub fn list_session_entries(
    storage: &dyn StorageBackend,
) -> Result<Vec<(String, Option<SessionEntry>)>> {
    let keys = storage.list(SESSIONS_PREFIX)?;
    let mut entries = Vec::new();
    for (session_id, markers) in group_session_markers(&keys) {
        let Some(&(_, newest)) = markers.last() else {
            continue;
        };
        let entry = storage
            .get(newest)?
            .and_then(|data| serde_json::from_slice::<SessionEntry>(&data).ok());
        entries.push((session_id.to_string(), entry));
    }
    Ok(entries)
}

/// A session whose marker survived the stale-session cleanup, as one
/// listing of `sessions/` and a GET of its newest key saw it.
#[derive(Clone, Debug)]
pub struct LiveSession {
    pub session_id: String,
    /// The newest key's body; `None` when it does not parse or vanished
    /// since the listing.
    pub entry: Option<SessionEntry>,
    /// The newest key's mtime on the store's clock; `None` when the listing
    /// reports none.
    pub mtime: Option<std::time::SystemTime>,
}

impl LiveSession {
    /// 04 "Compact" 3: mtime − elapsed_secs − `MARGIN`. `None` (−∞: no
    /// orphan pack may go) when the body, its `elapsed_secs` or the mtime
    /// is unknown, or the subtraction underflows.
    pub(crate) fn horizon(&self) -> Option<std::time::SystemTime> {
        let elapsed = self.entry.as_ref()?.elapsed_secs?;
        let back = std::time::Duration::from_secs(elapsed).checked_add(MARGIN)?;
        self.mtime?.checked_sub(back)
    }
}

/// Build an [`ActiveSessionList`] from raw `list_session_entries` output.
///
/// Parseable entries are rendered with host/pid/age; entries with `None`
/// (malformed JSON) are passed through as `ActiveSessionInfo { details: None }`
/// so `ActiveSessionList`'s `Display` surfaces them to the operator instead
/// of silently dropping them — the fail-closed policy of
/// `Repo::refuse_live_sessions`.
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
                age: format_age(now, &e.last_refresh),
            }),
        })
        .collect();
    ActiveSessionList(infos)
}

/// Delete all files under `sessions/` regardless of parse result.
///
/// Returns the number of files removed. This is a recovery mechanism that
/// cleans `.json` markers, `.index` journals, and any other stray files.
pub fn clear_all_sessions(storage: &dyn StorageBackend) -> Result<usize> {
    let keys = storage.list(SESSIONS_PREFIX)?;
    let mut removed = 0usize;
    for key in &keys {
        if key.starts_with(SESSIONS_PREFIX) {
            storage.delete(key)?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// Exponential backoff (cap 2^5 × base) with ±25 % jitter, minimum 100 ms.
fn backoff_delay(attempt: usize, base_delay_ms: u64) -> u64 {
    let delay = base_delay_ms * (1 << attempt.min(5));
    let jitter = (rand::random::<u64>() % (delay / 2).max(1)).wrapping_sub(delay / 4);
    delay.wrapping_add(jitter).max(100)
}

/// Compact holder description for log fields — `Display for LockHolder` is
/// the user-facing sentence, too verbose for a log field.
fn describe_holder(holder: &LockHolder) -> String {
    holder.details.as_ref().map_or_else(
        || holder.key.clone(),
        |d| format!("{} (PID {}, held {})", d.hostname, d.pid, d.age),
    )
}

/// Run `op` until it returns something other than `Locked`, with exponential
/// backoff + jitter between attempts. Returns the last `Locked` when attempts
/// run out. Blocks the calling thread on `clock`.
pub(crate) fn retry_while_contended<T>(
    max_attempts: usize,
    base_delay_ms: u64,
    clock: &dyn Clock,
    mut op: impl FnMut() -> Result<T>,
) -> Result<T> {
    for attempt in 0..max_attempts {
        match op() {
            Ok(v) => return Ok(v),
            Err(VykarError::Locked(holder)) => {
                if attempt + 1 < max_attempts {
                    let delay = backoff_delay(attempt, base_delay_ms);
                    debug!(
                        attempt = attempt + 1,
                        max_attempts,
                        holder = %describe_holder(&holder),
                        delay_ms = delay,
                        "another session holds the repository, retrying"
                    );
                    clock.sleep(std::time::Duration::from_millis(delay));
                } else {
                    return Err(VykarError::Locked(holder));
                }
            }
            Err(e) => return Err(e),
        }
    }
    // Loop only falls through when `max_attempts == 0`; every other path
    // returns inside the match.
    Err(VykarError::Other(
        "retry_while_contended called with max_attempts == 0".to_string(),
    ))
}

/// How long a single-writer backup's commit protection waits for a compact
/// (and an exclusive session) before refusing.
///
/// A compact runs for hours on a large repository; a backup that finishes
/// uploading meanwhile waits for it rather than fail after all its uploads.
/// The cost: a backup waiting on a crashed session on another host waits
/// until the session's marker is stale (same-host dead sessions are reaped
/// at once) or this long, whichever comes first.
pub const COMMIT_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(2 * 3600);

/// Longest single sleep between `cancel` checks.
const WAIT_SLICE: std::time::Duration = std::time::Duration::from_millis(500);

/// Run `attempt_once` until it returns something other than `Locked`, or
/// `max_wait` has elapsed (one final attempt after the deadline, so this
/// never gives up early). `cancel` is checked before every attempt
/// (including the first) and every 500 ms of a sleep (`Interrupted`);
/// `on_contended` is called once, on the first `Locked`, with the holder it
/// named.
///
/// The deadline bounds waiting, not the attempts: a slow attempt can
/// overrun it. No fairness: waiters do not queue.
///
/// Blocks the calling thread on `env.clock`. Callers running inside an async
/// runtime must invoke this via `tokio::task::spawn_blocking`.
pub(crate) fn wait_while_contended<T>(
    env: &HostEnv,
    max_wait: std::time::Duration,
    base_delay_ms: u64,
    cancel: &CancelToken,
    mut on_contended: impl FnMut(&LockHolder),
    mut attempt_once: impl FnMut() -> Result<T>,
) -> Result<T> {
    let start = env.clock.monotonic();
    let deadline = start + max_wait;
    let mut attempt: usize = 0;
    loop {
        if cancel.is_cancelled() {
            return Err(VykarError::Interrupted);
        }
        let holder = match attempt_once() {
            Ok(v) => return Ok(v),
            Err(VykarError::Locked(holder)) => holder,
            Err(e) => return Err(e),
        };
        let now = env.clock.monotonic();
        if now >= deadline {
            return Err(VykarError::Locked(holder));
        }
        let delay = std::time::Duration::from_millis(backoff_delay(attempt, base_delay_ms))
            .min(deadline - now);
        if attempt == 0 {
            info!(
                oldest_key_from = %describe_holder(&holder),
                max_wait_secs = max_wait.as_secs(),
                "another writer holds the repository; waiting"
            );
            on_contended(&holder);
        } else {
            debug!(
                attempt = attempt + 1,
                oldest_key_from = %describe_holder(&holder),
                delay_ms = delay.as_millis() as u64,
                waited_ms = now.duration_since(start).as_millis() as u64,
                "repository still held, retrying"
            );
        }
        let wake = now + delay;
        loop {
            let now = env.clock.monotonic();
            if now >= wake {
                break;
            }
            if cancel.is_cancelled() {
                return Err(VykarError::Interrupted);
            }
            env.clock.sleep((wake - now).min(WAIT_SLICE));
        }
        attempt += 1;
    }
}

/// Default stale session threshold.
pub fn default_stale_session_duration() -> Duration {
    Duration::seconds(SESSION_STALE_SECS)
}

/// Format the age between `now` and an RFC3339 `timestamp` as a compact
/// human-readable string (e.g. `"2h"`, `"1d 3h"`, `"42m"`, `"12s"`).
///
/// Returns `"unknown"` if the timestamp fails to parse.
pub fn format_age(now: &chrono::DateTime<Utc>, timestamp: &str) -> String {
    let Ok(ts) = chrono::DateTime::parse_from_rfc3339(timestamp) else {
        return "unknown".to_string();
    };
    format_age_since(now, &ts.with_timezone(&Utc))
}

/// Same as [`format_age`] but for an already-parsed timestamp.
///
/// Sub-minute ages render in seconds: for session contention the difference
/// between a few-second commit and a minutes-long compact is the whole signal,
/// and flooring both to `"0m"` would erase it.
pub fn format_age_since(now: &chrono::DateTime<Utc>, then: &chrono::DateTime<Utc>) -> String {
    let dur = now.signed_duration_since(*then);
    let hours = dur.num_hours();
    if hours >= 24 {
        format!("{}d {}h", hours / 24, hours % 24)
    } else if hours > 0 {
        format!("{hours}h")
    } else {
        let mins = dur.num_minutes();
        if mins > 0 {
            format!("{mins}m")
        } else {
            format!("{}s", dur.num_seconds().max(0))
        }
    }
}
