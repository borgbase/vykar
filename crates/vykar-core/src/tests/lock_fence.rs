//! The exclusive write check (10-single-protocol step 8,
//! `commands::util::exclusive_write_check`), at unit level on a memory
//! backend and virtual time: the check before every write of `check
//! --repair`, `vykar upgrade` and pack maps. It replaced the lock's fence,
//! which these tests covered before (10-single-protocol step 9 deleted it).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::Utc;

use crate::commands::util::exclusive_write_check;
use crate::repo::sessions::{self, EXCLUSIVE_CHECK_REUSE, SessionMarker};
use crate::repo::{FenceFn, HostEnv};
use crate::sim::env::{SimTime, TimeMode};
use crate::testutil::MemoryBackend;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::error::{Result, VykarError};

/// A memory backend that counts EXISTS requests and fails the next
/// `fail_exists` of them.
struct Counting {
    inner: MemoryBackend,
    exists: AtomicUsize,
    fail_exists: AtomicUsize,
}

impl InnerBackend for Counting {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for Counting;
    except [exists];

    fn exists(&self, key: &str) -> Result<bool> {
        self.exists.fetch_add(1, Ordering::SeqCst);
        if self
            .fail_exists
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(VykarError::Other("injected EXISTS failure".into()));
        }
        self.inner.exists(key)
    }
}

impl Counting {
    fn exists_count(&self) -> usize {
        self.exists.load(Ordering::SeqCst)
    }
}

struct Rig {
    time: Arc<SimTime>,
    env: HostEnv,
    storage: Arc<Counting>,
    marker: Arc<SessionMarker>,
    key: String,
}

/// A registered exclusive session on virtual time (no heartbeat: its
/// refreshes are the test's).
fn rig() -> Rig {
    let time = SimTime::new(Utc::now(), TimeMode::Auto);
    let env = HostEnv::system().with_clock(Arc::new(time.clock(chrono::Duration::zero())));
    let storage = Arc::new(Counting {
        inner: MemoryBackend::with_mtimes(),
        exists: AtomicUsize::new(0),
        fail_exists: AtomicUsize::new(0),
    });
    let reg = sessions::register_exclusive(storage.as_ref(), &env).unwrap();
    assert!(reg.live.is_empty());
    let key = sessions::session_marker_key(reg.registration.session_id());
    let marker = Arc::new(SessionMarker::new(&reg.registration));
    Rig {
        time,
        env,
        storage,
        marker,
        key,
    }
}

impl Rig {
    fn check(&self) -> FenceFn {
        exclusive_write_check(
            Arc::clone(&self.marker),
            Arc::clone(&self.storage) as Arc<dyn StorageBackend>,
            Arc::clone(&self.env.clock),
            EXCLUSIVE_CHECK_REUSE,
        )
    }
}

fn is_lost(r: &Result<()>) -> bool {
    matches!(r, Err(VykarError::MarkerLostDuringCommit))
}

#[test]
fn a_fresh_session_passes_with_one_exists_per_reuse_interval() {
    let r = rig();
    let check = r.check();
    check().unwrap();
    assert_eq!(r.storage.exists_count(), 1);
    check().unwrap();
    assert_eq!(r.storage.exists_count(), 1, "reused within the interval");
    r.time
        .advance(EXCLUSIVE_CHECK_REUSE + std::time::Duration::from_secs(1));
    check().unwrap();
    assert_eq!(r.storage.exists_count(), 2);
}

/// Within the reuse interval a removed marker still passes (the documented
/// residual RX1, asserted so it cannot widen silently); after it the check
/// fails `MarkerLostDuringCommit` and stays failed without a request.
#[test]
fn a_removed_marker_fails_after_the_reuse_interval_and_stays_failed() {
    let r = rig();
    let check = r.check();
    check().unwrap();
    r.storage.inner.delete(&r.key).unwrap();
    check().unwrap();
    r.time
        .advance(EXCLUSIVE_CHECK_REUSE + std::time::Duration::from_secs(1));
    assert!(is_lost(&check()));
    let sent = r.storage.exists_count();
    assert!(is_lost(&check()));
    assert!(is_lost(&check()));
    assert_eq!(r.storage.exists_count(), sent, "sticky: no further EXISTS");
}

/// A stretch over the gap limit (30 min without a successful marker write)
/// fails the check with the marker still present: a reaper may have taken
/// it for stale.
#[test]
fn a_gap_over_the_limit_fails_with_the_marker_present() {
    let r = rig();
    let check = r.check();
    check().unwrap();
    r.time
        .advance(sessions::MARKER_GAP_LIMIT + std::time::Duration::from_secs(60));
    assert!(is_lost(&check()));
    assert!(r.storage.inner.exists(&r.key).unwrap());
}

#[test]
fn a_suspend_shorter_than_the_gap_limit_passes() {
    let r = rig();
    let check = r.check();
    check().unwrap();
    r.time.advance(std::time::Duration::from_secs(20 * 60));
    check().unwrap();
}

#[test]
fn concurrent_checks_send_one_exists() {
    let r = rig();
    let check = r.check();
    std::thread::scope(|s| {
        for _ in 0..8 {
            let check = Arc::clone(&check);
            s.spawn(move || check().unwrap());
        }
    });
    assert_eq!(r.storage.exists_count(), 1);
}

#[test]
fn a_transport_error_is_returned_and_not_sticky() {
    let r = rig();
    let check = r.check();
    r.storage.fail_exists.store(1, Ordering::SeqCst);
    let err = check().unwrap_err();
    assert!(
        matches!(&err, VykarError::Other(m) if m.contains("injected")),
        "{err}"
    );
    check().unwrap();
}

/// Gate 1, A2: inside the reuse interval a record that is not continuous
/// only because a superseded key's EXISTS failed at a refresh (`unchecked`)
/// runs the full probe, which checks that key again: the write passes.
#[test]
fn a_failed_check_of_a_superseded_key_does_not_end_the_run() {
    let r = rig();
    let check = r.check();
    check().unwrap();
    r.storage.fail_exists.store(1, Ordering::SeqCst);
    let outcome = r.marker.refresh(r.storage.as_ref(), r.env.clock.as_ref());
    assert!(
        matches!(outcome, sessions::RefreshOutcome::Unchecked(_)),
        "{outcome:?}"
    );
    assert_eq!(r.marker.report(r.env.clock.as_ref()).unchecked, 1);
    check().unwrap();
    assert!(r.marker.report(r.env.clock.as_ref()).continuous());
}
