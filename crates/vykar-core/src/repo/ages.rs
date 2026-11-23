//! Ages on the storage's clock (04-commit-protocol.md "Constants"): every
//! age a protocol decision uses is (store now − object mtime), where "now"
//! is the mtime of an object this client has just written; client durations
//! use the local monotonic clock; no step compares two machines' clocks.
//!
//! Who reads which constant: compact keeps an orphan pack younger than
//! [`T_PACK`] and one inside a live backup's marker horizon (its marker's
//! mtime − `elapsed_secs` − [`MARGIN`]); a backup whose marker record is
//! not sound commits only while its packs stay younger than
//! [`PACK_AGE_LIMIT`] at its head's landing ([`HEAD_LANDING_ALLOWANCE`]
//! before the head PUT, none after it); journal recovery adopts packs
//! younger than [`RECOVERY_MAX_PACK_AGE`].

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tracing::debug;
use vykar_storage::{ListedObject, StorageBackend, StoreNow};
use vykar_types::error::Result;

use super::host_env::HostEnv;
use super::lock::SESSION_STALE_SECS;

/// Grace for packs that no index entry names, on the store's clock.
pub(crate) const T_PACK: Duration = Duration::from_secs(12 * 3600);
/// Grace for unreferenced `index.d/` objects and old heads.
pub(crate) const T_IDX: Duration = Duration::from_secs(3600);
/// Margin M against request timeouts and clock resolution.
pub(crate) const MARGIN: Duration = Duration::from_secs(10 * 60);

/// Time this client allows from the pack-age check before a commit's head
/// PUT (at the fence immediately before it) to the head landing: the early
/// refusal (04 "Backup commit" 6). It does not bound a head PUT: at the
/// default retry policy one PUT can take 6 attempts × 300 s response wait
/// plus back-off, about 31.5 min, and longer when every phase stalls (about
/// 100 min). The binding check is the one after the head PUT returned,
/// with no allowance: a commit whose packs may be [`PACK_AGE_LIMIT`] old
/// by then does not write its snapshot.
pub(crate) const HEAD_LANDING_ALLOWANCE: Duration = Duration::from_secs(30 * 60);

/// Oldest journal pack a backup recovers, on the store's clock (04
/// "Crash-recovery journals": T_pack/2).
pub(crate) const RECOVERY_MAX_PACK_AGE: Duration = Duration::from_secs(T_PACK.as_secs() / 2);

/// What the commit rule lets a backup with a broken marker record publish:
/// packs younger than T_pack − M at its head's landing.
pub(crate) const PACK_AGE_LIMIT: Duration =
    Duration::from_secs(T_PACK.as_secs() - MARGIN.as_secs());

// 04's relations: T_pack − M (the commit refusal, I8) and T_idx − M (segment
// renewal) are positive and leave room; the journal window T_pack/2 outlasts
// a marker's staleness by more than M.
const _: () = assert!(T_PACK.as_secs() > 2 * MARGIN.as_secs());
const _: () = assert!(T_IDX.as_secs() > MARGIN.as_secs());
const _: () = assert!(T_PACK.as_secs() / 2 > SESSION_STALE_SECS as u64 + MARGIN.as_secs());
const _: () = assert!(
    PACK_AGE_LIMIT.as_secs() > HEAD_LANDING_ALLOWANCE.as_secs() + RECOVERY_MAX_PACK_AGE.as_secs()
);

/// Key prefix of a clock probe. Not `.json`, so no lock reader (this
/// binary's `is_lock_key`, or 0.24's `list_lock_keys`) takes it for a lock.
pub(crate) const CLOCK_PROBE_PREFIX: &str = "locks/vykar-clock-";
/// A probe older than this is debris of a failed delete.
pub(crate) const CLOCK_PROBE_DEBRIS_AGE: Duration = Duration::from_secs(3600);

/// A reading of the store's clock older than this (on the local monotonic
/// clock) is not extrapolated: the next lock attempt probes again. Bounds
/// how far a store clock stepped back after the reading can carry an
/// extrapolated "now" ahead of it.
pub(crate) const STORE_READING_MAX_AGE: Duration = Duration::from_secs(5 * 60);

/// The clock probe key of a process: one random suffix per process, ending
/// in `pid` (32 hex digits in all). A re-probe lands on the same key, so on
/// a store that keeps a tombstone per deleted key (`s3_soft_delete`)
/// probes leave one per process, not one per attempt. Two threads of one
/// process that probe at once can delete each other's probe; the loser
/// reads "now unknown" and its attempt ends `Locked` (fail closed).
fn clock_probe_key(pid: u32) -> String {
    static SUFFIX: OnceLock<u128> = OnceLock::new();
    let suffix = *SUFFIX.get_or_init(rand::random::<u128>) >> 32;
    format!("{CLOCK_PROBE_PREFIX}{suffix:024x}{pid:08x}")
}

/// "Now" on the store's clock from a probe: PUT
/// `locks/vykar-clock-<32 hex>` (`clock_probe_key`), LIST `locks/` with
/// mtimes, DELETE the probe (best effort, debug log). Returns the reading
/// (`None` if the probe is not listed or has no mtime) and the listing
/// without the probe.
///
/// # Errors
///
/// The probe's PUT or LIST failed. Callers that must not fail on it read
/// an error as "now unknown".
pub(crate) fn probe_store_now(
    storage: &dyn StorageBackend,
    env: &HostEnv,
) -> Result<(Option<StoreNow>, Vec<ListedObject>)> {
    let key = clock_probe_key(env.host.pid);
    let listed = storage
        .put(&key, b"vykar clock probe")
        .and_then(|()| storage.list_meta("locks/"));
    if let Err(e) = storage.delete(&key) {
        debug!(key = %key, error = %e, "could not delete the clock probe");
    }
    let mut listing = listed?;
    let now = StoreNow::of_written(&listing, &key);
    listing.retain(|o| o.key != key);
    Ok((now, listing))
}

/// One wait's reading of the store's clock: the lock attempts of one wait
/// (`acquire_lock_with_deadline`, the simulator's lock phase) share it, so
/// the wait probes once rather than once per attempt.
#[derive(Debug, Default)]
pub(crate) struct StoreClock {
    /// The last reading and the local monotonic time just after it.
    reading: Option<(StoreNow, Instant)>,
}

impl StoreClock {
    /// "Now" on the store's clock, and the listing of `locks/` when this
    /// call probed. Within [`STORE_READING_MAX_AGE`] of the last reading it
    /// extrapolates: reading + monotonic time elapsed since. Otherwise, or
    /// when there is no reading (none yet, or the last probe failed), it
    /// probes; a failed probe is "now unknown" (`None`).
    ///
    /// The extrapolation errs young, never old: the reading is the mtime of
    /// a probe written before the LIST that returned it, and the monotonic
    /// mark is taken after that LIST returned, so the elapsed time counts
    /// from a moment later than the one the store stamped; a monotonic clock
    /// that stops while the machine is suspended only makes it smaller.
    /// This holds up to the rate difference between the local monotonic
    /// clock and the store's over `STORE_READING_MAX_AGE`, well under a
    /// second and nothing against the ages judged with it (hours). (A
    /// store clock stepped back after the reading is not seen until the
    /// next probe, at most `STORE_READING_MAX_AGE` later: until then a lock
    /// written after the step looks older by the step's size.)
    pub(crate) fn now(
        &mut self,
        storage: &dyn StorageBackend,
        env: &HostEnv,
    ) -> (Option<StoreNow>, Option<Vec<ListedObject>>) {
        if let Some((at, mark)) = self.reading {
            let elapsed = env.clock.elapsed(mark);
            if elapsed < STORE_READING_MAX_AGE {
                return (Some(StoreNow::at(at.time() + elapsed)), None);
            }
        }
        match probe_store_now(storage, env) {
            Ok((now, listing)) => {
                let mark = env.clock.monotonic();
                self.reading = now.map(|n| (n, mark));
                (now, Some(listing))
            }
            Err(e) => {
                debug!(error = %e, "clock probe failed; reaping nothing by age");
                self.reading = None;
                (None, None)
            }
        }
    }
}
