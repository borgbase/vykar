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
//! younger than [`RECOVERY_MAX_PACK_AGE`]; an index write whose head
//! create returns more than [`HEAD_FRESHNESS_LIMIT`] after the listing that
//! chose its generation checks that generation again (04 step 8); index
//! garbage collection deletes old heads older than [`T_IDX`] and
//! unreferenced `index.d/` objects older than [`T_PACK`]
//! (`index_store::INDEX_OBJECT_GRACE`), and an index write slower than
//! [`INDEX_WRITE_LIMIT`] from its first object PUT is refused.
//!
//! Segment age (04 step 7): a backup segment's age is the store's "now"
//! minus the mtime of its `index.d/` key in one LIST_META ([`SegmentAges`]).
//! "Now" is the mtime of a key this client wrote: the maintenance lock's
//! for a lock holder, a session's own marker key for a backup's or a
//! compact's index write (since 10-single-protocol step 7 on every
//! repository). While a
//! backup segment is younger than [`T_PACK`], compact's sweep leaves the
//! entries it adds and no fold writes it into a base. Unknown is young: an
//! object the listing lacks, one listed without an mtime, or no "now".
//! Every segment gets a fresh nonce, so a new segment always has a new name
//! and a new mtime.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime};

use tracing::debug;
use vykar_storage::{ListedObject, StorageBackend, StoreNow};
use vykar_types::error::Result;

use crate::index::head::{ObjectName, SegmentKind, SegmentRef};

use super::host_env::HostEnv;
use super::sessions::SESSION_STALE_SECS;

/// Grace for packs that no index entry names, on the store's clock.
pub(crate) const T_PACK: Duration = Duration::from_secs(12 * 3600);
/// Grace for old heads below the kept generations (unreferenced
/// `index.d/` objects keep [`T_PACK`]: `index_store::INDEX_OBJECT_GRACE`).
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

// 04's relations: T_pack − M (the commit refusal, I8) and T_idx − M (the
// head create's re-listing of index.h/) are positive and leave room; the journal window T_pack/2 outlasts
// a marker's staleness by more than M.
const _: () = assert!(T_PACK.as_secs() > 2 * MARGIN.as_secs());
const _: () = assert!(T_IDX.as_secs() > MARGIN.as_secs());
const _: () = assert!(T_PACK.as_secs() / 2 > SESSION_STALE_SECS as u64 + MARGIN.as_secs());
const _: () = assert!(
    PACK_AGE_LIMIT.as_secs() > HEAD_LANDING_ALLOWANCE.as_secs() + RECOVERY_MAX_PACK_AGE.as_secs()
);

/// Longest time from the start of an index write's first `index.d/` PUT to
/// the return of its head create (`index_store::INDEX_OBJECT_GRACE` − M):
/// the write refuses to create its head when this minus
/// [`HEAD_LANDING_ALLOWANCE`] (about 11 h 20 min) has passed, and fails
/// before its snapshot when the create returned later than this. Measured
/// suspend included, as markers measure elapsed time.
pub(crate) const INDEX_WRITE_LIMIT: Duration =
    Duration::from_secs(super::index_store::INDEX_OBJECT_GRACE.as_secs() - MARGIN.as_secs());

const _: () = assert!(
    super::index_store::INDEX_OBJECT_GRACE.as_secs()
        > MARGIN.as_secs() + HEAD_LANDING_ALLOWANCE.as_secs()
);

/// The start of [`vykar_types::error::VykarError::SlowIndexWrite`]'s text, the error an index
/// write refused by [`INDEX_WRITE_LIMIT`] fails with (the simulator's
/// fallback for wrapped errors matches it).
#[cfg(any(test, feature = "sim"))]
pub(crate) const SLOW_INDEX_WRITE_TEXT: &str = "the index write took longer than";

/// Longest time from the head listing that chose a write's generation to
/// the return of its head create after which the generation is checked
/// against a fresh listing (04 step 8). Old heads are collected only below
/// `latest − HEADS_KEPT` and once older than [`T_IDX`], so within
/// `T_IDX − MARGIN` of the listing no generation created after it can have
/// been collected and re-made by this create (`MARGIN` absorbs the rate
/// difference of the two clocks). Measured suspend included, as markers
/// measure elapsed time.
pub(crate) const HEAD_FRESHNESS_LIMIT: Duration =
    Duration::from_secs(T_IDX.as_secs() - MARGIN.as_secs());

/// Clock probe of the existing-pack check (`existing_pack`): neither
/// `.json` nor `.index`, so no marker or journal reader (this binary's
/// or 0.24's) takes it for one.
pub(crate) const CLOCK_PROBE_PREFIX: &str = "sessions/vykar-clock-";
/// The put-if-absent check's probe
/// (`vykar_storage::PUT_IF_ABSENT_PROBE_KEY_PREFIX`); one left by a failed
/// delete is removed by age like a clock probe.
pub(crate) const PUT_IF_ABSENT_PROBE_PREFIX: &str = vykar_storage::PUT_IF_ABSENT_PROBE_KEY_PREFIX;

/// Whether `key` is a probe (clock or put-if-absent, both under
/// `sessions/`): skipped by session readers, debris once older than
/// [`CLOCK_PROBE_DEBRIS_AGE`].
pub(crate) fn is_probe_key(key: &str) -> bool {
    [CLOCK_PROBE_PREFIX, PUT_IF_ABSENT_PROBE_PREFIX]
        .iter()
        .any(|p| key.starts_with(p))
}
/// A probe older than this is debris of a failed delete.
pub(crate) const CLOCK_PROBE_DEBRIS_AGE: Duration = Duration::from_secs(3600);

/// A reading of the store's clock older than this (on the local monotonic
/// clock) is not extrapolated: the next call probes again. Bounds
/// how far a store clock stepped back after the reading can carry an
/// extrapolated "now" ahead of it.
pub(crate) const STORE_READING_MAX_AGE: Duration = Duration::from_secs(5 * 60);

/// The clock probe key of a process under `prefix`: one random suffix per
/// process, ending in `pid` (32 hex digits in all). A re-probe lands on the same key, so on
/// a store that keeps a tombstone per deleted key (`s3_soft_delete`)
/// probes leave one per process, not one per attempt. Two threads of one
/// process that probe at once can delete each other's probe; the loser
/// reads "now unknown" (fail closed).
fn clock_probe_key(prefix: &str, pid: u32) -> String {
    static SUFFIX: OnceLock<u128> = OnceLock::new();
    let suffix = *SUFFIX.get_or_init(rand::random::<u128>) >> 32;
    format!("{prefix}{suffix:024x}{pid:08x}")
}

/// "Now" on the store's clock from a probe: PUT
/// `<prefix><32 hex>` (`clock_probe_key`), LIST the prefix's directory
/// with mtimes, DELETE the probe (best effort, debug log). Returns the
/// reading (`None` if the probe is not listed or has no mtime) and the
/// listing without the probe.
///
/// # Errors
///
/// The probe's PUT or LIST failed. Callers that must not fail on it read
/// an error as "now unknown".
pub(crate) fn probe_store_now(
    storage: &dyn StorageBackend,
    env: &HostEnv,
    prefix: &'static str,
) -> Result<(Option<StoreNow>, Vec<ListedObject>)> {
    let key = clock_probe_key(prefix, env.host.pid);
    let dir = prefix.rfind('/').map_or("", |i| &prefix[..=i]);
    let listed = storage
        .put(&key, b"vykar clock probe")
        .and_then(|()| storage.list_meta(dir));
    if let Err(e) = storage.delete(&key) {
        debug!(key = %key, error = %e, "could not delete the clock probe");
    }
    let mut listing = listed?;
    let now = StoreNow::of_written(&listing, &key);
    listing.retain(|o| o.key != key);
    Ok((now, listing))
}

/// A reading of the store's clock that the calls of one run share
/// (the existing-pack check's, [`Self::in_sessions`]), so the run probes
/// once per [`STORE_READING_MAX_AGE`] rather than once per call.
#[derive(Debug)]
pub(crate) struct StoreClock {
    /// The last reading and the local monotonic time just after it.
    reading: Option<(StoreNow, Instant)>,
    /// Key prefix of this clock's probe; its directory is what the probe lists.
    pub(super) prefix: &'static str,
}

impl StoreClock {
    /// A clock whose probe lives under `sessions/` (the existing-pack check).
    pub(crate) fn in_sessions() -> Self {
        Self {
            reading: None,
            prefix: CLOCK_PROBE_PREFIX,
        }
    }

    /// "Now" on the store's clock, and the listing of the probe's directory
    /// when this call probed. Within [`STORE_READING_MAX_AGE`] of the last reading it
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
    /// next probe, at most `STORE_READING_MAX_AGE` later: until then an
    /// object written after the step looks older by the step's size.)
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
        match probe_store_now(storage, env, self.prefix) {
            Ok((now, listing)) => {
                let mark = env.clock.monotonic();
                self.reading = now.map(|n| (n, mark));
                (now, Some(listing))
            }
            Err(e) => {
                debug!(error = %e, "clock probe failed; now unknown");
                self.reading = None;
                (None, None)
            }
        }
    }
}

/// Ages of `index.d/` objects on the store's clock: one listing and one
/// reading of "now" (the run's session store time, `Repo::store_time`). An
/// object not listed, listed without an mtime, or judged without a "now"
/// is young (04 step 7: fail closed means keep).
#[derive(Clone, Debug, Default)]
pub(crate) struct SegmentAges {
    now: Option<StoreNow>,
    mtimes: HashMap<ObjectName, Option<SystemTime>>,
}

impl SegmentAges {
    /// LIST_META `index.d/` and keep every object's mtime, judged against
    /// `now`.
    ///
    /// # Errors
    ///
    /// The listing failed.
    pub(crate) fn list(storage: &dyn StorageBackend, now: Option<StoreNow>) -> Result<Self> {
        let mtimes = super::index_store::list_index_object_mtimes(storage)?;
        Ok(Self { now, mtimes })
    }

    /// Ages from parts (tests).
    #[cfg(test)]
    pub(crate) fn from_parts(
        now: Option<StoreNow>,
        mtimes: HashMap<ObjectName, Option<SystemTime>>,
    ) -> Self {
        Self { now, mtimes }
    }

    /// Whether the listing showed `name`.
    pub(crate) fn listed(&self, name: &ObjectName) -> bool {
        self.mtimes.contains_key(name)
    }

    /// Whether `name` is at least `grace` old: always at zero grace;
    /// otherwise it needs a listed mtime and a "now" (a future mtime is age
    /// 0).
    pub(crate) fn is_old(&self, name: &ObjectName, grace: Duration) -> bool {
        if grace.is_zero() {
            return true;
        }
        match (self.now, self.mtimes.get(name).copied().flatten()) {
            (Some(now), Some(mtime)) => now.age_of(mtime) >= grace,
            _ => false,
        }
    }
}

/// Whether a fold may write `seg` into a base and a sweep may remove the
/// entries it adds: a maintenance segment always; a backup segment once it
/// is `grace` old by `ages` (never without ages, unless `grace` is zero).
///
/// Maintenance segments are foldable at any age because they add nothing a
/// backup depends on: a sweep removes, a repack relocates by chunk ID, and
/// the only maintenance segments with additions come from `check
/// --repair`, which refuses to run beside live sessions. (Part 9 note: once
/// the lock goes, repair's own sweep needs the young-segment rule too.)
pub(crate) fn foldable(seg: &SegmentRef, ages: Option<&SegmentAges>, grace: Duration) -> bool {
    match seg.kind {
        SegmentKind::Maintenance => true,
        SegmentKind::Backup => grace.is_zero() || ages.is_some_and(|a| a.is_old(&seg.name, grace)),
    }
}

/// The length of the longest prefix of `segments` that is foldable
/// (segments apply in order, so only a prefix can go into a base).
pub(crate) fn foldable_prefix(
    segments: &[SegmentRef],
    ages: Option<&SegmentAges>,
    grace: Duration,
) -> usize {
    segments
        .iter()
        .take_while(|s| foldable(s, ages, grace))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(byte: u8, kind: SegmentKind) -> SegmentRef {
        SegmentRef {
            name: ObjectName([byte; 32]),
            size: 10,
            kind,
            entries: 1,
        }
    }

    #[test]
    fn segment_ages_fail_closed() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100 * 3600);
        let at = |hours_ago: u64| Some(now - Duration::from_secs(hours_ago * 3600));
        let mtimes = HashMap::from([
            (ObjectName([1; 32]), at(13)),
            (ObjectName([2; 32]), at(1)),
            (ObjectName([3; 32]), None),
            (ObjectName([4; 32]), at(12)),
            (ObjectName([5; 32]), Some(now + Duration::from_secs(3600))),
        ]);
        let ages = SegmentAges::from_parts(Some(StoreNow::at(now)), mtimes.clone());
        assert!(ages.is_old(&ObjectName([1; 32]), T_PACK));
        assert!(!ages.is_old(&ObjectName([2; 32]), T_PACK));
        assert!(!ages.is_old(&ObjectName([3; 32]), T_PACK), "no mtime");
        assert!(!ages.is_old(&ObjectName([9; 32]), T_PACK), "not listed");
        assert!(ages.is_old(&ObjectName([4; 32]), T_PACK), "age = grace");
        assert!(!ages.is_old(&ObjectName([5; 32]), T_PACK), "future mtime");
        // Zero grace: everything is old, even unlisted.
        assert!(ages.is_old(&ObjectName([9; 32]), Duration::ZERO));
        // No "now": young.
        let no_now = SegmentAges::from_parts(None, mtimes);
        assert!(!no_now.is_old(&ObjectName([1; 32]), T_PACK));
        // Without ages a backup segment is young, a maintenance one not.
        assert!(!foldable(&seg(1, SegmentKind::Backup), None, T_PACK));
        assert!(foldable(&seg(1, SegmentKind::Backup), None, Duration::ZERO));
        assert!(foldable(&seg(2, SegmentKind::Maintenance), None, T_PACK));
    }

    #[test]
    fn foldable_prefix_stops_at_the_first_young_backup_segment() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100 * 3600);
        let mtimes = HashMap::from([
            (
                ObjectName([1; 32]),
                Some(now - Duration::from_secs(13 * 3600)),
            ),
            (ObjectName([2; 32]), Some(now - Duration::from_secs(3600))),
            (
                ObjectName([4; 32]),
                Some(now - Duration::from_secs(13 * 3600)),
            ),
        ]);
        let ages = SegmentAges::from_parts(Some(StoreNow::at(now)), mtimes);
        let segments = [
            seg(0, SegmentKind::Maintenance),
            seg(1, SegmentKind::Backup),
            seg(2, SegmentKind::Backup),
            seg(3, SegmentKind::Maintenance),
            seg(4, SegmentKind::Backup),
        ];
        // The old backup segment after the young one is outside the prefix.
        assert_eq!(foldable_prefix(&segments, Some(&ages), T_PACK), 2);
        assert_eq!(foldable_prefix(&segments, None, T_PACK), 1);
        assert_eq!(foldable_prefix(&segments, None, Duration::ZERO), 5);
        assert_eq!(foldable_prefix(&segments[3..], Some(&ages), T_PACK), 2);
        assert_eq!(foldable_prefix(&[], Some(&ages), T_PACK), 0);
    }
}
