//! A pack upload an append-only vykar-server refused because the key
//! exists already (04 "Crash-recovery journals").
//!
//! A pack's key is the hash of its bytes, so an object stored under it is
//! the same pack. A pack PUT whose reply was lost (the server stored it) is
//! retried by the backend; an append-only vykar-server refuses the retry
//! (403, the key exists) before reading the body, and without this the
//! backup would fail on a pack that is stored.
//!
//! The refused pack counts as uploaded when the server advertises
//! append-only mode, the error reads as a write refusal, the key class
//! forbids an overwrite there, and the stored object has the pack's size.
//! It is an old object the session now depends on, so it is treated like an
//! adopted journal pack: its store age pulls the session's start back
//! (`SessionMarker::pull_back`), which the marker's horizon and the commit's
//! pack-age term (I8) both read. An unknown age counts as the oldest
//! possible ([`UNKNOWN_PACK_AGE`]): compact then gets no horizon from the
//! session, and the commit refuses if the marker record is not sound.
//!
//! What carries the safety is the advertised mode, the key class and the
//! equal size under a key that is the hash of the bytes; the server writes
//! a pack to a temporary file and renames it, so no torn object sits under
//! a final key. The reading of the error as a refusal is a coarse filter (a
//! substring match): where it matches some other failure, the stored object
//! is still this pack and counting it is right.
//!
//! Known costs and limits:
//! - Each accepted pack costs one listing of its shard, and a clock probe
//!   under `sessions/` when the shared reading is older than five minutes
//!   or failed.
//! - A 403 on a part upload turns the REST backend's parts path off for the
//!   rest of the process, so later packs of that backup go as whole PUTs.
//! - The stored marker carries the pull-back only from its next refresh. A
//!   compact with other access to the storage (the server refuses it) that
//!   deletes an accepted pack older than T_pack before the commit makes the
//!   commit's pack check fail ("missing from storage"); a re-run succeeds.
//!   With a head PUT that lands after the session ended it is the
//!   `LateHeadThenCompact` shape, which 04 step 8 closes.

use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use tracing::{debug, warn};
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};

use super::ages::StoreClock;
use super::host_env::HostEnv;
use super::sessions::SessionMarker;

/// The age a refused pack of unknown age counts as: the oldest possible.
/// As a pull-back it saturates the session's `elapsed`, so the marker's
/// horizon is `None` (−∞) and the pack-age term refuses an unsound record.
pub(crate) const UNKNOWN_PACK_AGE: Duration = Duration::MAX;

/// What a write session needs to accept a refused pack upload: the client's
/// environment, a shared reading of the store's clock, and the session's
/// marker once the backup attaches it.
pub(crate) struct ExistingPacks {
    env: HostEnv,
    marker: OnceLock<Arc<SessionMarker>>,
    clock: Mutex<StoreClock>,
}

impl ExistingPacks {
    pub(crate) fn new(env: HostEnv) -> Self {
        Self {
            env,
            marker: OnceLock::new(),
            clock: Mutex::new(StoreClock::in_sessions()),
        }
    }

    /// The session's marker, pulled back by every refused pack accepted
    /// from now on. Only the first call has an effect.
    pub(crate) fn attach_marker(&self, marker: Arc<SessionMarker>) {
        let _ = self.marker.set(marker);
    }

    /// The outcome of a pack upload of `len` bytes to `key` that failed
    /// with `err`: `Ok` when the refusal is an append-only server's answer
    /// to an existing pack of the same size (the session's start is then
    /// pulled back by its age), else `err` unchanged.
    pub(crate) fn settle_refused(
        &self,
        storage: &dyn StorageBackend,
        key: &str,
        len: u64,
        err: VykarError,
    ) -> Result<()> {
        if matches!(err, VykarError::Interrupted)
            || !err.is_write_refusal()
            || !storage.server_ops().is_some_and(|s| s.append_only())
            || vykar_protocol::append_only_allows(key, vykar_protocol::Verb::Overwrite)
        {
            return Err(err);
        }
        let Some((size, mtime)) = stored_meta(storage, key) else {
            return Err(err);
        };
        if size != len {
            warn!(
                key,
                stored = size,
                sealed = len,
                "the append-only server refused a pack whose key exists with another size"
            );
            return Err(err);
        }
        let age = mtime.and_then(|mtime| {
            let (now, _) = self
                .clock
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .now(storage, &self.env);
            now.map(|now| now.age_of(mtime))
        });
        if let Some(marker) = self.marker.get() {
            marker.pull_back(age.unwrap_or(UNKNOWN_PACK_AGE));
        }
        warn!(
            key,
            age_secs = age.map(|a| a.as_secs()),
            "the append-only server already holds this pack (an earlier attempt whose reply \
             was lost); using the stored copy"
        );
        Ok(())
    }
}

/// `key`'s size and mtime from a listing of its directory (`size()` when
/// the listing reports no size). `None` when it is not listed or the
/// lookup failed.
fn stored_meta(
    storage: &dyn StorageBackend,
    key: &str,
) -> Option<(u64, Option<std::time::SystemTime>)> {
    let (dir, _) = key.rsplit_once('/')?;
    let listed = match storage.list_meta(&format!("{dir}/")) {
        Ok(listed) => listed.into_iter().find(|o| o.key == key)?,
        Err(e) => {
            debug!(key, error = %e, "could not list a refused pack's directory");
            return None;
        }
    };
    let size = match listed.size {
        Some(size) => size,
        None => storage.size(key).ok().flatten()?,
    };
    Some((size, listed.mtime))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::sessions::{self, MarkerReport, register_session};
    use crate::testutil::{MemoryBackend, init_test_environment};
    use vykar_storage::{InnerBackend, delegate_storage_backend};

    const HOUR: Duration = Duration::from_secs(3600);
    const KEY: &str = "packs/ab/abababababababababababababababababababababababababababababab";

    /// A vykar-server stand-in that advertises append-only mode (or not)
    /// over a `MemoryBackend` that refuses what such a server refuses.
    struct Advertised {
        inner: MemoryBackend,
        append_only: bool,
    }

    impl InnerBackend for Advertised {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for Advertised;
        except [server_ops];

        fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
            Some(self)
        }
    }

    impl vykar_storage::ServerOps for Advertised {
        fn offers(&self, _op: vykar_storage::ServerOp) -> bool {
            false
        }

        fn capabilities(&self) -> Result<vykar_protocol::ServerCapabilities> {
            let mut caps = crate::testutil::fake_server_caps();
            caps.append_only = self.append_only;
            Ok(caps)
        }

        fn init(&self) -> Result<()> {
            Ok(())
        }

        fn append_only(&self) -> bool {
            self.append_only
        }
    }

    /// A store holding a 100-byte pack under `KEY`, `age` old by its
    /// mtime (`None`: a store that reports no mtimes).
    fn store_with_pack(age: Option<Duration>, append_only: bool) -> Advertised {
        let inner = match age {
            Some(_) => MemoryBackend::append_only_with_mtimes(),
            None => MemoryBackend::append_only(),
        };
        inner.put(KEY, &[7; 100]).unwrap();
        if let Some(age) = age {
            inner.set_mtime(KEY, std::time::SystemTime::now() - age);
        }
        Advertised { inner, append_only }
    }

    /// The error a second PUT of `KEY` gets from the store.
    fn refusal(storage: &dyn StorageBackend) -> VykarError {
        storage.put(KEY, &[7; 100]).unwrap_err()
    }

    fn with_marker(storage: &dyn StorageBackend) -> (ExistingPacks, Arc<SessionMarker>) {
        init_test_environment();
        let env = HostEnv::system();
        let registration = register_session(storage, "existing", &env).unwrap();
        let marker = Arc::new(SessionMarker::new(&registration));
        let existing = ExistingPacks::new(env);
        existing.attach_marker(Arc::clone(&marker));
        (existing, marker)
    }

    /// A refused PUT of a pack the append-only server holds with the same
    /// size counts as uploaded, and its age pulls the session back.
    #[test]
    fn a_stored_pack_of_the_same_size_is_used_and_pulls_the_session_back() {
        let storage = store_with_pack(Some(7 * HOUR), true);
        let (existing, marker) = with_marker(&storage);
        let err = refusal(&storage);
        existing.settle_refused(&storage, KEY, 100, err).unwrap();
        let elapsed = marker.elapsed(&crate::repo::host_env::SystemClock);
        assert!(elapsed >= 7 * HOUR, "{elapsed:?}");
        assert!(elapsed < 8 * HOUR, "{elapsed:?}");
    }

    /// The clock probe that reads the stored pack's age is written under
    /// `sessions/` (not `locks/`): its reading comes from a `sessions/`
    /// listing, and the probe is deleted.
    #[test]
    fn the_clock_probe_lives_under_sessions() {
        let storage = store_with_pack(Some(7 * HOUR), true);
        let (existing, marker) = with_marker(&storage);
        let prefix = existing.clock.lock().unwrap().prefix;
        assert_eq!(prefix, crate::repo::ages::CLOCK_PROBE_PREFIX);
        assert!(prefix.starts_with("sessions/"));
        let err = refusal(&storage);
        existing.settle_refused(&storage, KEY, 100, err).unwrap();
        // The age was read on the store's clock, so the probe was listed.
        let elapsed = marker.elapsed(&crate::repo::host_env::SystemClock);
        assert!(elapsed >= 7 * HOUR && elapsed < 8 * HOUR, "{elapsed:?}");
        let left: Vec<String> = ["sessions/", "locks/"]
            .iter()
            .flat_map(|d| storage.list(d).unwrap())
            .filter(|k| k.contains("vykar-clock-"))
            .collect();
        assert!(left.is_empty(), "{left:?}");
    }

    /// Another size, a server that does not advertise append-only mode, a
    /// key the server lets a client overwrite, or an error that is no
    /// refusal: the upload's error stands, and nothing is pulled back.
    #[test]
    fn anything_else_keeps_the_upload_error() {
        let storage = store_with_pack(Some(7 * HOUR), true);
        let (existing, marker) = with_marker(&storage);
        let err = refusal(&storage);
        assert!(existing.settle_refused(&storage, KEY, 101, err).is_err());

        let plain = store_with_pack(Some(7 * HOUR), false);
        let err = refusal(&plain);
        assert!(existing.settle_refused(&plain, KEY, 100, err).is_err());

        let other = VykarError::Other("REST PUT: HTTP 500".into());
        assert!(existing.settle_refused(&storage, KEY, 100, other).is_err());
        let err = VykarError::Other("403 Forbidden".into());
        assert!(
            existing
                .settle_refused(&storage, "sessions/x.json", 100, err)
                .is_err()
        );
        let missing = "packs/cd/cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        let err = VykarError::Other("403 Forbidden".into());
        assert!(
            existing
                .settle_refused(&storage, missing, 100, err)
                .is_err()
        );

        let elapsed = marker.elapsed(&crate::repo::host_env::SystemClock);
        assert!(elapsed < HOUR, "{elapsed:?}");
    }

    /// Without an mtime the pack counts as the oldest possible: the marker
    /// gives compact no horizon, and the commit's pack-age term refuses
    /// exactly when the marker record is not sound.
    #[test]
    fn a_stored_pack_of_unknown_age_counts_as_the_oldest_possible() {
        let storage = store_with_pack(None, true);
        let (existing, marker) = with_marker(&storage);
        let err = refusal(&storage);
        existing.settle_refused(&storage, KEY, 100, err).unwrap();
        let elapsed = marker.elapsed(&crate::repo::host_env::SystemClock);
        assert_eq!(elapsed, UNKNOWN_PACK_AGE);

        let sound = MarkerReport {
            missing: false,
            longest_gap: Duration::ZERO,
            unchecked: 0,
            refreshes: 1,
            failed_refreshes: 0,
            slow_puts: 0,
            restarted: false,
        };
        let unsound = MarkerReport {
            slow_puts: 1,
            ..sound.clone()
        };
        sessions::commit_marker_check("existing", Ok(true), &sound, elapsed).unwrap();
        sessions::head_landing_check("existing", &sound, elapsed, Duration::ZERO).unwrap();
        assert!(matches!(
            sessions::commit_marker_check("existing", Ok(true), &unsound, elapsed),
            Err(VykarError::StaleChunksDuringCommit)
        ));
        assert!(matches!(
            sessions::head_landing_check("existing", &unsound, elapsed, Duration::ZERO),
            Err(VykarError::StaleChunksDuringCommit)
        ));
    }
}
