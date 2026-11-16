//! Registry of in-progress part uploads (`PUT packs/..?upload=..`).
//!
//! A part upload spans several requests: parts are positioned writes into one
//! temp file, and a final `?complete` verifies and renames it. The registry
//! is what owns that temp file and its quota reservation *between* requests:
//!
//! - The temp file lives in the pack's shard directory under the regular
//!   [`vykar_protocol::TEMP_FILE_PREFIX`], so listings hide it and the
//!   delete-repo purge and the aged-debris sweep cover it like any other.
//!   Its name is derived from the pack id and the client's token, so every
//!   part request of one upload resolves the same file.
//! - The entry holds a `tempfile::TempPath` (unlinks on drop) and a
//!   [`QuotaReservation`] for the declared total (releases on drop), so
//!   expiring an abandoned upload is just dropping its entry.
//! - Completion takes both out through a [`Completion`] guard, which owns
//!   the file (first at the temp path, then at its sealed name) and the
//!   reservation until the file is published, and cleans up whatever it
//!   still owns when dropped: any failed check, or the completion future
//!   dropped at any await. For the guard's whole life the registry holds a
//!   *completing* marker under the temp path. While it is present a part
//!   request for that temp path is refused with a retryable 503 instead of
//!   starting a fresh upload, and a second completion gets a retryable 503
//!   instead of taking one: nothing can create a file at the temp path, and
//!   so nothing can seal one over the file being verified.
//! - An upload no request has touched for [`PART_UPLOAD_IDLE_TTL`] is
//!   abandoned and expired. Expiry runs whenever a new upload starts and from
//!   the server's periodic sweep, so a client that vanished cannot pin quota.
//!
//! The registry is in-memory. After a server restart, an upload's temp file
//! is orphaned (its completion answers 404 and the client falls back to a
//! whole PUT); the aged-debris sweep removes it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::ServerError;
use crate::state::{AppState, QuotaReservation};

/// An upload whose last request finished this long ago is abandoned.
///
/// Far above any gap between the parts of one upload (they are sent
/// back-to-back), and short enough that a crashed client's reservation does
/// not hold quota for long.
pub(crate) const PART_UPLOAD_IDLE_TTL: Duration = Duration::from_secs(60 * 60);

/// Upper bound on concurrently registered uploads. A client keeps a handful
/// in flight; the cap only bounds what a misbehaving one can pin. Completing
/// markers count too, but each stands in for the upload it took and lives
/// only as long as one completion request.
pub(crate) const MAX_PART_UPLOADS: usize = 1024;

/// One registered upload.
pub(crate) struct PartUpload {
    /// Declared size of the finished object; every part must agree.
    pub total: u64,
    temp: tempfile::TempPath,
    reservation: QuotaReservation,
    last_active: Mutex<Instant>,
}

impl PartUpload {
    pub(crate) fn path(&self) -> &Path {
        &self.temp
    }

    pub(crate) fn touch(&self) {
        *self
            .last_active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();
    }

    fn idle_for(&self, now: Instant) -> Duration {
        now.saturating_duration_since(
            *self
                .last_active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Split into the temp file owner and the reservation, for completion.
    pub(crate) fn into_parts(self) -> (tempfile::TempPath, QuotaReservation) {
        (self.temp, self.reservation)
    }
}

/// Result of [`PartUploads::take`].
pub(crate) enum Take<'a> {
    /// Exclusive ownership: no part request is running, and none can start
    /// on this temp path until the guard is dropped.
    Owned(Completion<'a>),
    /// A part request still holds the upload; nothing was removed.
    Busy,
    /// Another completion for this temp path is still running.
    Completing,
    /// No such upload (never started, completed, failed or expired).
    Missing,
}

/// A registry slot, keyed by temp path.
enum Entry {
    /// Parts may join; completion may take it once no part holds it.
    Active(Arc<PartUpload>),
    /// Taken by a [`Completion`] that has not been dropped yet.
    Completing,
}

type Map = HashMap<PathBuf, Entry>;

#[derive(Default)]
pub struct PartUploads {
    map: Mutex<Map>,
}

impl PartUploads {
    fn lock(&self) -> std::sync::MutexGuard<'_, Map> {
        self.map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Join the upload at `temp`, or start it: reserve quota for `total` and
    /// create the temp file. The returned handle keeps the upload alive (and
    /// un-completable) until dropped.
    ///
    /// The file is created synchronously under the registry lock, so the file
    /// and its owner come into existence together (see `create_temp_in`).
    /// It is opened without truncation: parts are positioned writes of
    /// content-addressed bytes, so any bytes already present are either
    /// rewritten or caught by the completion digest.
    ///
    /// While a completion for this temp path is running, from `take` until
    /// its guard is dropped, this answers a retryable 503 rather than
    /// registering a new upload. Once the completion has ended the temp path
    /// is free again: a stale part arriving later (one whose client attempt
    /// timed out and was retried, delivered late) starts a fresh upload on a
    /// new file there, which nothing completes; it pins its reservation until
    /// idle expiry, like any abandoned upload.
    pub(crate) fn begin_or_join(
        &self,
        state: &AppState,
        temp: &Path,
        total: u64,
    ) -> Result<Arc<PartUpload>, ServerError> {
        let mut map = self.lock();
        match map.get(temp) {
            Some(Entry::Active(upload)) => {
                if upload.total != total {
                    return Err(ServerError::BadRequest(format!(
                        "part declares total {total}, upload was started with {}",
                        upload.total
                    )));
                }
                upload.touch();
                return Ok(Arc::clone(upload));
            }
            Some(Entry::Completing) => {
                return Err(ServerError::ServiceUnavailable(
                    "upload is being completed".into(),
                ));
            }
            None => {}
        }

        expire_idle(&mut map, Instant::now(), PART_UPLOAD_IDLE_TTL);
        if map.len() >= MAX_PART_UPLOADS {
            return Err(ServerError::ServiceUnavailable(
                "too many part uploads in progress".into(),
            ));
        }

        let reservation = state.try_reserve_quota(total).map_err(|(used, limit)| {
            ServerError::PayloadTooLarge(format!(
                "quota exceeded: used {used}, limit {limit}, request {total}"
            ))
        })?;
        // Owner first: if the open fails, nothing is left to unlink and
        // dropping the owner of a missing path is a no-op.
        let owner = tempfile::TempPath::try_from_path(temp).map_err(ServerError::from)?;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&owner)
            .map_err(ServerError::from)?;
        let upload = Arc::new(PartUpload {
            total,
            temp: owner,
            reservation,
            last_active: Mutex::new(Instant::now()),
        });
        map.insert(temp.to_path_buf(), Entry::Active(Arc::clone(&upload)));
        Ok(upload)
    }

    /// Take the upload at `temp` for completion, leaving a completing marker
    /// in its place until the returned guard is dropped.
    /// Fails with [`Take::Busy`] (and leaves it registered) while a part
    /// request holds it, and with [`Take::Completing`] while another
    /// completion owns it.
    pub(crate) fn take(&self, temp: &Path) -> Take<'_> {
        let mut map = self.lock();
        let Some(entry) = map.get_mut(temp) else {
            return Take::Missing;
        };
        let Entry::Active(upload) = std::mem::replace(entry, Entry::Completing) else {
            return Take::Completing; // it was already a marker
        };
        // Handles are cloned only under this lock, so the count cannot grow.
        match Arc::try_unwrap(upload) {
            Ok(upload) => {
                let total = upload.total;
                let (file, reservation) = upload.into_parts();
                Take::Owned(Completion {
                    registry: self,
                    temp: temp.to_path_buf(),
                    total,
                    file: Some(file),
                    reservation: Some(reservation),
                })
            }
            Err(upload) => {
                *entry = Entry::Active(upload);
                Take::Busy
            }
        }
    }

    /// Drop every upload idle for longer than `ttl`, unlinking its temp file
    /// and releasing its reservation. Returns how many were expired.
    pub(crate) fn expire(&self, ttl: Duration) -> usize {
        let expired = {
            let mut map = self.lock();
            expire_idle(&mut map, Instant::now(), ttl)
        };
        let n = expired.len();
        if n > 0 {
            tracing::info!(uploads = n, "expired abandoned part uploads");
        }
        // Dropped outside the lock: each drop unlinks a file.
        drop(expired);
        n
    }

    /// Registered entries, completing markers included.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }
}

/// Exclusive ownership of an upload being completed, from
/// [`PartUploads::take`] until dropped.
///
/// While it exists the registry holds a completing marker for the temp
/// path: one part request that already held the upload made `take` answer
/// [`Take::Busy`], and `begin_or_join` and `take` refuse every later request
/// for the temp path. So no other request can create a file at the temp
/// path, and none can rename one to the sealed name: `<temp>.sealed` is
/// written only by this guard's [`Completion::seal`], and the file found
/// there is the one this guard sealed until it publishes or drops it.
///
/// The guard owns the file (at the temp path, then at the sealed path) and
/// the reservation until [`Completion::publish`] renames the file into
/// place and commits the reservation. Dropping it unlinks whatever file it
/// still owns, releases the reservation and only then clears the marker, so
/// the marker never outlives the guard and a request arriving between the
/// cleanup and the clear is refused rather than racing it.
pub(crate) struct Completion<'a> {
    registry: &'a PartUploads,
    temp: PathBuf,
    total: u64,
    /// The upload's file: at `temp` until sealed, then at the sealed path.
    /// `None` once published, after a failed rename, or forgotten in a test.
    file: Option<tempfile::TempPath>,
    /// `None` once committed.
    reservation: Option<QuotaReservation>,
}

impl Completion<'_> {
    pub(crate) fn temp_path(&self) -> &Path {
        &self.temp
    }

    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    /// Rename the temp file to `sealed`, away from the name part requests
    /// open. The guard keeps owning the file under its new name; the marker
    /// stays. On failure the temp file is unlinked (the reservation is
    /// released when the guard drops).
    pub(crate) fn seal(&mut self, sealed: &Path) -> std::io::Result<()> {
        let temp = self.file.take().expect("an unsealed guard owns its file");
        if let Err(e) = temp.persist(sealed) {
            drop(e.path); // unlink before the guard clears the marker
            return Err(e.error);
        }
        self.file = Some(tempfile::TempPath::try_from_path(sealed)?);
        Ok(())
    }

    /// Rename the owned file to `dest` and commit the reservation. On a
    /// failed rename the file is unlinked and the reservation is released
    /// when the guard drops. The marker stays until the guard is dropped.
    pub(crate) fn publish(&mut self, dest: &Path) -> std::io::Result<()> {
        let file = self.file.take().expect("the guard owns its file");
        if let Err(e) = file.persist(dest) {
            drop(e.path);
            return Err(e.error);
        }
        if let Some(reservation) = self.reservation.take() {
            reservation.commit();
        }
        Ok(())
    }

    /// Drop the upload's ownership without unlinking its file, as a server
    /// restart would.
    #[cfg(test)]
    pub(crate) fn forget_file(mut self) {
        if let Some(file) = self.file.take() {
            file.keep().expect("keep temp file");
        }
    }
}

impl Drop for Completion<'_> {
    fn drop(&mut self) {
        // Unlink and release before clearing the marker, outside the lock: a
        // request arriving in between is refused rather than handed a new
        // file at the temp path that this cleanup would then race.
        drop(self.file.take());
        drop(self.reservation.take());
        let mut map = self.registry.lock();
        if matches!(map.get(&self.temp), Some(Entry::Completing)) {
            map.remove(&self.temp);
        }
    }
}

/// Remove idle uploads no request holds. Returns them so the caller decides
/// where the unlinks happen. Completing markers belong to a running
/// completion, however long its hash and fdatasync take, and are never
/// expired: they carry no idle time, and their guard removes them.
fn expire_idle(map: &mut Map, now: Instant, ttl: Duration) -> Vec<Arc<PartUpload>> {
    let stale: Vec<PathBuf> = map
        .iter()
        .filter(|(_, e)| match e {
            Entry::Active(u) => Arc::strong_count(u) == 1 && u.idle_for(now) >= ttl,
            Entry::Completing => false,
        })
        .map(|(k, _)| k.clone())
        .collect();
    stale
        .iter()
        .filter_map(|k| match map.remove(k) {
            Some(Entry::Active(u)) => Some(u),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::test_helpers::setup_app;

    fn begin(state: &AppState, temp: &Path) -> Result<Arc<PartUpload>, ServerError> {
        state.inner.part_uploads.begin_or_join(state, temp, 100)
    }

    /// For a completion's whole life, sealed or not, a part for the same temp
    /// path gets no handle on its file and registers no upload, and no second
    /// completion can take it.
    #[test]
    fn taken_upload_refuses_parts_until_the_completion_ends() {
        let (_router, state, tmp) = setup_app(1000);
        let temp = tmp.path().join(".tmp.taken");
        let uploads = &state.inner.part_uploads;
        drop(begin(&state, &temp).unwrap());

        let Take::Owned(mut completion) = uploads.take(&temp) else {
            panic!("idle upload should be owned");
        };
        let refused = |uploads: &PartUploads| {
            assert!(matches!(
                begin(&state, &temp),
                Err(ServerError::ServiceUnavailable(_))
            ));
            assert!(matches!(uploads.take(&temp), Take::Completing));
            assert_eq!(uploads.expire(Duration::ZERO), 0, "markers never expire");
            assert_eq!(uploads.len(), 1, "only the marker");
            assert_eq!(state.quota_reserved(), 100, "no second reservation");
        };
        refused(uploads);

        let sealed = tmp.path().join(".tmp.taken.sealed");
        completion.seal(&sealed).unwrap();
        assert!(!temp.exists() && sealed.exists());
        refused(uploads);
        assert!(!temp.exists(), "no fresh file at the temp path");

        drop(completion);
        assert!(!sealed.exists());
        assert_eq!(uploads.len(), 0, "dropping the guard clears the marker");
        assert!(matches!(uploads.take(&temp), Take::Missing));
        // Only now may a late part start a fresh upload on a new file.
        drop(begin(&state, &temp).unwrap());
        assert!(temp.exists());
    }

    /// The reviewer's race: completion A seals and opens its file to verify
    /// it; a late part with the same token then starts a fresh upload and a
    /// second completion B seals it over A's sealed name, so A's path-based
    /// publish would commit B's unverified bytes. Both requests must be
    /// refused while A runs, and A must publish exactly what it verified.
    #[test]
    fn running_completion_owns_its_sealed_name() {
        let (_router, state, tmp) = setup_app(1000);
        let temp = tmp.path().join(".tmp.c");
        let sealed = tmp.path().join(".tmp.c.sealed");
        let final_path = tmp.path().join("pack");
        let uploads = &state.inner.part_uploads;

        let a = begin(&state, &temp).unwrap();
        std::fs::write(a.path(), b"VERIFIED-A").unwrap();
        drop(a);
        let Take::Owned(mut ca) = uploads.take(&temp) else {
            panic!("idle upload should be owned");
        };
        ca.seal(&sealed).unwrap();
        let mut verified = Vec::new();
        std::io::Read::read_to_end(&mut std::fs::File::open(&sealed).unwrap(), &mut verified)
            .unwrap();

        // A late part with the same token, then a completion B.
        assert!(matches!(
            begin(&state, &temp),
            Err(ServerError::ServiceUnavailable(_))
        ));
        assert!(!temp.exists());
        assert!(matches!(uploads.take(&temp), Take::Completing));

        ca.publish(&final_path).unwrap();
        assert_eq!(std::fs::read(&final_path).unwrap(), verified);
        assert_eq!(verified, b"VERIFIED-A");
        assert_eq!(state.quota_reserved(), 0, "reservation committed");
        assert_eq!(state.quota_used(), 100);
        assert_eq!(uploads.len(), 1, "marker held until the guard drops");
        drop(ca);
        assert_eq!(uploads.len(), 0);
        assert_eq!(
            state.quota_used(),
            100,
            "drop after publish commits nothing twice"
        );
        assert!(final_path.exists() && !sealed.exists());
    }

    /// A completion future dropped after the seal (client gone mid-hash)
    /// unlinks the sealed file, releases the reservation and clears the
    /// marker, so the same token can upload again.
    #[test]
    fn completion_dropped_after_seal_cleans_up_and_clears_the_marker() {
        let (_router, state, tmp) = setup_app(1000);
        let temp = tmp.path().join(".tmp.mid");
        let sealed = tmp.path().join(".tmp.mid.sealed");
        let uploads = &state.inner.part_uploads;
        drop(begin(&state, &temp).unwrap());

        let Take::Owned(mut completion) = uploads.take(&temp) else {
            panic!("idle upload should be owned");
        };
        completion.seal(&sealed).unwrap();
        drop(completion);
        assert!(!temp.exists() && !sealed.exists(), "sealed file unlinked");
        assert_eq!(state.quota_reserved(), 0, "reservation released");
        assert_eq!(state.quota_used(), 0);
        assert_eq!(uploads.len(), 0, "marker cleared");
        assert!(matches!(uploads.take(&temp), Take::Missing));
        drop(begin(&state, &temp).unwrap());
        assert!(temp.exists());
        assert_eq!(state.quota_reserved(), 100);
    }

    /// A completion dropped before sealing (early return, client disconnect)
    /// cleans up exactly like before and leaves no marker behind.
    #[test]
    fn dropped_completion_cleans_up_and_clears_the_marker() {
        let (_router, state, tmp) = setup_app(1000);
        let temp = tmp.path().join(".tmp.dropped");
        let uploads = &state.inner.part_uploads;
        drop(begin(&state, &temp).unwrap());

        let Take::Owned(completion) = uploads.take(&temp) else {
            panic!("idle upload should be owned");
        };
        drop(completion);
        assert!(!temp.exists(), "temp file unlinked");
        assert_eq!(state.quota_reserved(), 0, "reservation released");
        assert_eq!(uploads.len(), 0, "marker cleared");
        assert!(matches!(uploads.take(&temp), Take::Missing));
        drop(begin(&state, &temp).unwrap());
        assert_eq!(uploads.len(), 1);
    }

    /// A failed seal (the rename) unlinks at once; dropping the guard then
    /// releases and clears.
    #[test]
    fn failed_seal_cleans_up_and_clears_the_marker() {
        let (_router, state, tmp) = setup_app(1000);
        let temp = tmp.path().join(".tmp.failed");
        let uploads = &state.inner.part_uploads;
        drop(begin(&state, &temp).unwrap());

        let Take::Owned(mut completion) = uploads.take(&temp) else {
            panic!("idle upload should be owned");
        };
        let sealed = tmp.path().join("no-such-dir").join("sealed");
        assert!(completion.seal(&sealed).is_err());
        assert!(!temp.exists());
        drop(completion);
        assert_eq!(state.quota_reserved(), 0);
        assert_eq!(uploads.len(), 0);
    }

    /// A part request holding the upload still makes completion `Busy`
    /// and leaves the upload joinable.
    #[test]
    fn held_upload_is_busy_and_stays_active() {
        let (_router, state, tmp) = setup_app(1000);
        let temp = tmp.path().join(".tmp.busy");
        let uploads = &state.inner.part_uploads;
        let held = begin(&state, &temp).unwrap();
        assert!(matches!(uploads.take(&temp), Take::Busy));
        drop(begin(&state, &temp).unwrap());
        drop(held);
        assert!(matches!(uploads.take(&temp), Take::Owned(_)));
    }
}
