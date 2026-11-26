//! Atomic create-if-absent ("put-if-absent") and the one check that decides
//! whether a backend's storage honours it.
//!
//! Each backend that can do it has a raw primitive (no-clobber rename on a
//! local filesystem, `If-None-Match: *` on vykar-server and S3,
//! `hardlink@openssh.com` on SFTP). None of them is trusted blindly: the first
//! [`ConditionalWrite::verify`] or [`ConditionalWrite::put_if_absent`] on a
//! backend instance runs [`sequential_check`] with that primitive and caches a
//! conclusive answer in the backend's [`Verifier`]. The stricter
//! [`ConditionalWrite::verify_at_init`] adds a concurrent race on top of it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Barrier, Mutex};

use vykar_types::error::{Result, VykarError};

use crate::StorageBackend;

/// Directory the put-if-absent check writes its probe keys under. A probe
/// key is neither `.json` nor `.index`, so no session-marker or journal
/// parser (this binary's or 0.24's) takes it for one, and vykar-server
/// classes it as ephemeral (create, overwrite, delete allowed in
/// append-only mode).
pub const PROBE_DIR: &str = "sessions/";

/// Prefix of every put-if-absent probe key: [`PROBE_DIR`] +
/// [`probe::PROBE_PREFIX`](crate::probe::PROBE_PREFIX).
pub const PUT_IF_ABSENT_PROBE_KEY_PREFIX: &str = "sessions/vykar-probe-";

/// Concurrent creators per round of [`Verifier::verify_at_init`]'s race.
pub const INIT_RACE_WRITERS: usize = 8;

/// Rounds of that race, each on a fresh key.
pub const INIT_RACE_ROUNDS: usize = 3;

/// Largest body `put_if_absent` accepts, on every backend: one request, never
/// a multipart upload (S3 multipart completion is not reliably conditional).
pub const MAX_PUT_IF_ABSENT_BYTES: usize = 16 * 1024 * 1024;

/// What a [`ConditionalWrite::put_if_absent`] call found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Created {
    /// This call created the object.
    Yes,
    /// An object was already there. May be this caller's own earlier attempt
    /// (a retry after a lost reply): compare the stored bytes to tell.
    AlreadyExists,
}

/// The answer of [`ConditionalWrite::verify`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionalSupport {
    /// The sequential check passed.
    Verified,
    /// The storage refuses the mechanism or ignores the condition. Final for
    /// this backend instance: `conditional()` returns `None` from then on.
    Unsupported { reason: String },
}

/// Atomic create-if-absent. Obtained from
/// [`StorageBackend::conditional`](crate::StorageBackend::conditional).
pub trait ConditionalWrite: Send + Sync {
    /// Create `key` with `data` only if no object is stored under `key`.
    /// Visibility and durability as `StorageBackend::put`. `Ok(Yes)`: this
    /// call created it. `Ok(AlreadyExists)`: an object was there (possibly
    /// this caller's own retried write). `Err`: unknown, the object may or may
    /// not have been created.
    ///
    /// Runs [`Self::verify`] first (once per backend instance; see there).
    /// After `Unsupported` it returns `Err(VykarError::UnsupportedBackend)`.
    /// `data` must not exceed [`MAX_PUT_IF_ABSENT_BYTES`] and `key` must not
    /// be a temp-file name (`Err(InvalidFormat)` otherwise, without a
    /// request). On S3 with `s3_soft_delete`, a deleted (tombstoned) key still
    /// exists.
    ///
    /// # Errors
    ///
    /// See above.
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<Created>;

    /// Check once, by writing, that the storage honours the condition: create
    /// a fresh key under `sessions/` ([`PROBE_DIR`]) twice, expect the second create refused and
    /// the first bytes kept, delete the key. A conclusive answer is cached for
    /// the life of the backend. `Err` means undecided (no write permission,
    /// transport failure, cancel): nothing is cached and the next call checks
    /// again. Writes only when called; nothing calls it on open.
    ///
    /// The check is sequential: it catches a storage that ignores the
    /// condition, not one that lets two concurrent creates both win; see
    /// [`Self::verify_at_init`] for the race.
    ///
    /// # Errors
    ///
    /// Undecided, as above.
    fn verify(&self) -> Result<ConditionalSupport>;

    /// [`Verifier::verify_at_init`] with this backend's primitive: the
    /// sequential check plus the concurrent race, for a verdict that is
    /// recorded (init, the commit-mode switch). About
    /// 4 + [`INIT_RACE_ROUNDS`] × ([`INIT_RACE_WRITERS`] + 2) requests, once.
    ///
    /// # Errors
    ///
    /// Undecided, as [`Self::verify`].
    fn verify_at_init(&self) -> Result<ConditionalSupport>;
}

/// A backend's unverified primitive. `Err(VykarError::UnsupportedBackend(_))`
/// means "the storage refuses the mechanism itself". Any other `Err` is
/// undecided.
/// `Sync`, so [`Verifier::verify_at_init`]'s race can share it across threads.
pub type RawPut<'a> = &'a (dyn Fn(&str, &[u8]) -> Result<Created> + Sync);

/// Start of the reason of an `Unsupported` answer when the storage accepted
/// the creates but ignored the condition (a second create succeeded, or
/// replaced the object), as opposed to refusing the mechanism.
pub const IGNORES_CONDITION: &str = "ignores the condition";

/// The per-backend-instance cache of [`ConditionalWrite::verify`]. Public
/// for backends outside this crate (test doubles), which must run the same
/// check as the backends here.
pub struct Verifier {
    state: Mutex<Checked>,
    /// Set together with a cached `Unsupported`; read lock-free.
    refused: AtomicBool,
}

/// What a [`Verifier`] has concluded so far.
struct Checked {
    /// Conclusive results only.
    answer: Option<ConditionalSupport>,
    /// The init-time race passed on this instance (implies
    /// `answer == Some(Verified)`).
    raced: bool,
}

impl Default for Verifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Verifier {
    pub const fn new() -> Self {
        Self {
            state: Mutex::new(Checked {
                answer: None,
                raced: false,
            }),
            refused: AtomicBool::new(false),
        }
    }

    /// `conditional()` returns `None` once this is true.
    pub fn refused(&self) -> bool {
        self.refused.load(Ordering::Acquire)
    }

    /// The cached answer, or the sequential check run under the mutex
    /// (concurrent callers wait for the one check). Caches only `Ok(..)`.
    ///
    /// # Errors
    ///
    /// Undecided, as [`ConditionalWrite::verify`].
    pub fn verify(
        &self,
        store: &dyn StorageBackend,
        raw: RawPut<'_>,
    ) -> Result<ConditionalSupport> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.checked(&mut state, store, raw)
    }

    /// The cached answer, or the sequential check, cached when conclusive.
    fn checked(
        &self,
        state: &mut Checked,
        store: &dyn StorageBackend,
        raw: RawPut<'_>,
    ) -> Result<ConditionalSupport> {
        if let Some(cached) = state.answer.as_ref() {
            return Ok(cached.clone());
        }
        let answer = sequential_check(store, raw, PROBE_DIR)?;
        self.conclude(state, answer.clone());
        Ok(answer)
    }

    /// Cache a conclusive `answer`; `Unsupported` also sets `refused`.
    fn conclude(&self, state: &mut Checked, answer: ConditionalSupport) {
        if matches!(answer, ConditionalSupport::Unsupported { .. }) {
            self.refused.store(true, Ordering::Release);
            state.raced = false;
        }
        state.answer = Some(answer);
    }

    /// Conclude `Unsupported { reason }` without running any check (a
    /// backend that knows, without a request, that its storage cannot be
    /// trusted), as a failed check would: cached, `refused` set. Returns the
    /// cached answer, which is an earlier `Unsupported` if there was one.
    pub fn refuse(&self, reason: impl Into<String>) -> ConditionalSupport {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cached @ ConditionalSupport::Unsupported { .. }) = state.answer.as_ref() {
            return cached.clone();
        }
        let answer = unsupported(reason);
        self.conclude(&mut state, answer.clone());
        answer
    }

    /// The check init and the commit-mode switch run (10-single-protocol
    /// step 5): the sequential check (cached, as [`Self::verify`]), then
    /// [`INIT_RACE_ROUNDS`] rounds of [`INIT_RACE_WRITERS`] concurrent raw
    /// creates of one fresh key under [`PROBE_DIR`], each round's key deleted
    /// after it. `Verified` only when every round had exactly one winner
    /// whose bytes are stored. Two creates that both succeeded give
    /// `Unsupported` (reason starting with [`IGNORES_CONDITION`]), cached
    /// and final for this instance as a sequential `Unsupported` is, even
    /// if an earlier [`Self::verify`] had cached `Verified`; callers run it
    /// before any create on the instance. A pass is cached too. `Err` is
    /// undecided and caches nothing about the race. Runs under the state
    /// mutex, so concurrent `verify` callers wait for it.
    ///
    /// # Errors
    ///
    /// Undecided, as [`ConditionalWrite::verify`].
    pub fn verify_at_init(
        &self,
        store: &dyn StorageBackend,
        raw: RawPut<'_>,
    ) -> Result<ConditionalSupport> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.raced {
            return Ok(ConditionalSupport::Verified);
        }
        if let ConditionalSupport::Unsupported { reason } = self.checked(&mut state, store, raw)? {
            return Ok(ConditionalSupport::Unsupported { reason });
        }
        let answer = race_check(store, raw, PROBE_DIR, INIT_RACE_WRITERS, INIT_RACE_ROUNDS)?;
        match &answer {
            ConditionalSupport::Verified => state.raced = true,
            ConditionalSupport::Unsupported { .. } => self.conclude(&mut state, answer.clone()),
        }
        Ok(answer)
    }

    /// [`Self::verify`], then `Verified` gives `Ok(())` and `Unsupported`
    /// gives `Err(UnsupportedBackend)` naming `label` and the reason.
    pub(crate) fn require(
        &self,
        label: &str,
        store: &dyn StorageBackend,
        raw: RawPut<'_>,
    ) -> Result<()> {
        match self.verify(store, raw)? {
            ConditionalSupport::Verified => Ok(()),
            ConditionalSupport::Unsupported { reason } => {
                Err(put_if_absent_unavailable(format_args!("{label}: {reason}")))
            }
        }
    }

    /// The whole [`ConditionalWrite::put_if_absent`] of a backend whose raw
    /// primitive is `raw`: the argument checks, the check (once), the raw
    /// create, and a refusal after a passed check read as a failed write.
    /// `label` names the backend in the "unavailable" error.
    ///
    /// # Errors
    ///
    /// As [`ConditionalWrite::put_if_absent`].
    pub fn put_if_absent(
        &self,
        label: &str,
        store: &dyn StorageBackend,
        raw: RawPut<'_>,
        key: &str,
        data: &[u8],
    ) -> Result<Created> {
        check_args(key, data)?;
        self.require(label, store, raw)?;
        after_verified(raw(key, data))
    }
}

/// The error [`ConditionalWrite::put_if_absent`] returns once put-if-absent is
/// no longer offered (its check concluded `Unsupported`). `detail` names the
/// backend and the reason.
pub fn put_if_absent_unavailable(detail: impl std::fmt::Display) -> VykarError {
    VykarError::UnsupportedBackend(format!("put-if-absent unavailable: {detail}"))
}

/// Argument checks shared by every backend's `put_if_absent`.
/// The outcome of a raw create made after the check passed. The storage is
/// known to honour the condition, so a refusal of the mechanism at this point
/// (a transient link failure, say) is an ordinary failed write with an
/// unknown outcome, not "this backend cannot do it".
pub(crate) fn after_verified(outcome: Result<Created>) -> Result<Created> {
    outcome.map_err(after_verified_err)
}

/// The error half of [`after_verified`].
fn after_verified_err(e: VykarError) -> VykarError {
    match e {
        VykarError::UnsupportedBackend(m) => VykarError::Other(format!(
            "conditional write failed on a verified backend: {m}"
        )),
        other => other,
    }
}

pub(crate) fn check_args(key: &str, data: &[u8]) -> Result<()> {
    if vykar_protocol::is_temp_file(key) {
        return Err(VykarError::InvalidFormat(format!(
            "put-if-absent: '{key}' is a temp-file name"
        )));
    }
    if data.len() > MAX_PUT_IF_ABSENT_BYTES {
        return Err(VykarError::InvalidFormat(format!(
            "put-if-absent: {} bytes exceed the {MAX_PUT_IF_ABSENT_BYTES}-byte limit",
            data.len()
        )));
    }
    Ok(())
}

fn unsupported(reason: impl Into<String>) -> ConditionalSupport {
    ConditionalSupport::Unsupported {
        reason: reason.into(),
    }
}

/// Create a fresh key under `dir` twice and expect the second create refused
/// with the first bytes kept. Deletes the key after the first create attempt.
fn sequential_check(
    store: &dyn StorageBackend,
    raw: RawPut<'_>,
    dir: &str,
) -> Result<ConditionalSupport> {
    let key = format!("{dir}{}", crate::probe::new_prefix());
    let a = crate::probe::payload("A", &key, 64);
    let b = crate::probe::payload("B", &key, 64);
    let result = check_steps(store, raw, &key, &a, &b);
    if let Err(e) = store.delete(&key) {
        tracing::warn!(key = %key, error = %e, "put-if-absent check: could not delete its probe object");
    }
    result
}

/// `rounds` rounds of `writers` concurrent raw creates of one fresh key
/// under `dir` (`<probe prefix>-race<r>`), each round's key deleted after
/// it. Judged per round: two or more `Yes` is `Unsupported` (even beside
/// errors); any writer error is undecided; then the stored bytes must be a
/// writer's, and a sole `Yes` must be that writer (zero `Yes` where the
/// stored bytes are an `AlreadyExists` writer's is a lost reply retried by
/// the primitive, a pass).
fn race_check(
    store: &dyn StorageBackend,
    raw: RawPut<'_>,
    dir: &str,
    writers: usize,
    rounds: usize,
) -> Result<ConditionalSupport> {
    let base = format!("{dir}{}", crate::probe::new_prefix());
    for round in 0..rounds {
        let key = format!("{base}-race{round}");
        let result = race_round(store, raw, &key, writers);
        if let Err(e) = store.delete(&key) {
            tracing::warn!(key = %key, error = %e, "put-if-absent race: could not delete its probe object");
        }
        match result? {
            ConditionalSupport::Verified => {}
            unsupported => return Ok(unsupported),
        }
    }
    Ok(ConditionalSupport::Verified)
}

/// One round of [`race_check`] on `key`; does not delete it.
fn race_round(
    store: &dyn StorageBackend,
    raw: RawPut<'_>,
    key: &str,
    writers: usize,
) -> Result<ConditionalSupport> {
    let payloads: Vec<Vec<u8>> = (0..writers)
        .map(|i| crate::probe::payload(&format!("w{i}"), key, 64))
        .collect();
    let barrier = Barrier::new(writers);
    let outcomes: Vec<Result<Created>> = std::thread::scope(|s| {
        let handles: Vec<_> = payloads
            .iter()
            .map(|p| {
                let barrier = &barrier;
                s.spawn(move || {
                    barrier.wait();
                    raw(key, p)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|_| Err(VykarError::Other("writer thread panicked".into())))
            })
            .collect()
    });
    let winners: Vec<usize> = outcomes
        .iter()
        .enumerate()
        .filter(|(_, o)| matches!(o, Ok(Created::Yes)))
        .map(|(i, _)| i)
        .collect();
    if winners.len() >= 2 {
        return Ok(unsupported(format!(
            "{IGNORES_CONDITION}: {} of {writers} concurrent creates of one key succeeded",
            winners.len()
        )));
    }
    if let Some(e) = outcomes.into_iter().find_map(|o| o.err()) {
        return Err(after_verified_err(e));
    }
    let Some(stored) = store.get(key)? else {
        return Err(VykarError::Other(
            "put-if-absent race: the key vanished".into(),
        ));
    };
    let Some(owner) = payloads.iter().position(|p| *p == stored) else {
        return Err(VykarError::Other(
            "put-if-absent race: the stored bytes are no writer's".into(),
        ));
    };
    if winners.first().is_some_and(|&w| w != owner) {
        return Ok(unsupported(format!(
            "{IGNORES_CONDITION}: a concurrent create replaced the winner's object"
        )));
    }
    Ok(ConditionalSupport::Verified)
}

fn check_steps(
    store: &dyn StorageBackend,
    raw: RawPut<'_>,
    key: &str,
    a: &[u8],
    b: &[u8],
) -> Result<ConditionalSupport> {
    match raw(key, a) {
        Ok(Created::Yes) => {}
        // A remote primitive retries internally: a retry after a lost reply
        // finds this call's own object. Only the stored bytes can tell.
        Ok(Created::AlreadyExists) => match store.get(key)? {
            Some(stored) if stored == a => {}
            _ => {
                return Err(VykarError::Other(
                    "put-if-absent check: a fresh random key was reported as existing \
                     with other content"
                        .into(),
                ));
            }
        },
        Err(VykarError::UnsupportedBackend(m)) => return Ok(unsupported(m)),
        Err(e) => return Err(e),
    }
    match raw(key, b) {
        Ok(Created::AlreadyExists) => {}
        Ok(Created::Yes) => {
            return Ok(unsupported(format!(
                "{IGNORES_CONDITION}: a second create of the same key succeeded"
            )));
        }
        Err(VykarError::UnsupportedBackend(m)) => return Ok(unsupported(m)),
        Err(e) => return Err(e),
    }
    match store.get(key)? {
        Some(stored) if stored == a => Ok(ConditionalSupport::Verified),
        Some(_) => Ok(unsupported(format!(
            "{IGNORES_CONDITION}: the second create replaced the object"
        ))),
        None => Err(VykarError::Other(
            "put-if-absent check: probe object vanished".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BackendProfile, unsupported_server_ops};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    /// A map store; the raw primitive under test is a separate closure.
    #[derive(Default)]
    struct Map {
        objects: Mutex<HashMap<String, Vec<u8>>>,
        deletes: AtomicUsize,
    }

    impl Map {
        fn set(&self, key: &str, data: &[u8]) {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), data.to_vec());
        }
        fn len(&self) -> usize {
            self.objects.lock().unwrap().len()
        }
    }

    impl StorageBackend for Map {
        fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }
        fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.set(key, data);
            Ok(())
        }
        fn delete(&self, key: &str) -> Result<()> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }
        fn exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
        fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn get_range(&self, _key: &str, _offset: u64, _length: u64) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn create_dir(&self, _key: &str) -> Result<()> {
            Ok(())
        }
        fn profile(&self) -> BackendProfile {
            BackendProfile {
                local: true,
                staged_put: false,
                request_width: 1,
                transfer_width: 1,
            }
        }
        unsupported_server_ops!();
    }

    /// A correct primitive over `map`.
    fn honest(map: &Map, key: &str, data: &[u8]) -> Result<Created> {
        let mut objects = map.objects.lock().unwrap();
        if objects.contains_key(key) {
            return Ok(Created::AlreadyExists);
        }
        objects.insert(key.to_string(), data.to_vec());
        Ok(Created::Yes)
    }

    #[test]
    fn check_verified() {
        let map = Map::default();
        let v = Verifier::new();
        let raw = |k: &str, d: &[u8]| honest(&map, k, d);
        assert_eq!(v.verify(&map, &raw).unwrap(), ConditionalSupport::Verified);
        assert!(!v.refused());
        assert_eq!(map.len(), 0, "the probe key is deleted");
        assert_eq!(map.deletes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn check_ignored_second_create() {
        let map = Map::default();
        let v = Verifier::new();
        let calls = AtomicUsize::new(0);
        let raw = |k: &str, d: &[u8]| {
            calls.fetch_add(1, Ordering::SeqCst);
            map.set(k, d);
            Ok(Created::Yes)
        };
        let ConditionalSupport::Unsupported { reason } = v.verify(&map, &raw).unwrap() else {
            panic!("a second create that succeeds must be Unsupported");
        };
        assert!(reason.contains("ignores the condition"), "{reason}");
        assert!(v.refused());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        // Cached: no further raw call.
        assert!(matches!(
            v.verify(&map, &raw).unwrap(),
            ConditionalSupport::Unsupported { .. }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn check_overwritten_bytes() {
        let map = Map::default();
        let v = Verifier::new();
        let n = AtomicUsize::new(0);
        // Claims "exists" on the second create but replaced the object anyway.
        let raw = |k: &str, d: &[u8]| {
            map.set(k, d);
            if n.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(Created::Yes)
            } else {
                Ok(Created::AlreadyExists)
            }
        };
        let ConditionalSupport::Unsupported { reason } = v.verify(&map, &raw).unwrap() else {
            panic!("replaced bytes must be Unsupported");
        };
        assert!(reason.contains("replaced"), "{reason}");
        assert!(v.refused());
    }

    #[test]
    fn check_mechanism_refused() {
        let map = Map::default();
        let v = Verifier::new();
        let raw = |_: &str, _: &[u8]| -> Result<Created> {
            Err(VykarError::UnsupportedBackend("no hard links".into()))
        };
        assert_eq!(
            v.verify(&map, &raw).unwrap(),
            ConditionalSupport::Unsupported {
                reason: "no hard links".into()
            }
        );
        assert!(v.refused());
    }

    #[test]
    fn check_inconclusive_not_cached() {
        let map = Map::default();
        let v = Verifier::new();
        let calls = AtomicUsize::new(0);
        let raw = |_: &str, _: &[u8]| -> Result<Created> {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(VykarError::Other("403".into()))
        };
        assert!(v.verify(&map, &raw).is_err());
        assert!(!v.refused());
        assert_eq!(
            map.deletes.load(Ordering::SeqCst),
            1,
            "a lost reply may have created it"
        );
        assert!(v.verify(&map, &raw).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2, "an Err is not cached");
    }

    #[test]
    fn check_first_create_retried() {
        // The primitive's own retry after a lost reply: the object is there,
        // with this check's bytes, and the primitive reports AlreadyExists.
        let map = Map::default();
        let v = Verifier::new();
        let n = AtomicUsize::new(0);
        let raw = |k: &str, d: &[u8]| {
            if n.fetch_add(1, Ordering::SeqCst) == 0 {
                map.set(k, d);
                Ok(Created::AlreadyExists)
            } else {
                honest(&map, k, d)
            }
        };
        assert_eq!(v.verify(&map, &raw).unwrap(), ConditionalSupport::Verified);
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn check_first_create_exists_with_other_bytes() {
        let map = Map::default();
        let v = Verifier::new();
        let raw = |k: &str, _: &[u8]| {
            map.set(k, b"someone else");
            Ok(Created::AlreadyExists)
        };
        assert!(v.verify(&map, &raw).is_err());
        assert!(!v.refused(), "inconclusive: nothing cached");
        assert!(v.state.lock().unwrap().answer.is_none());
    }

    #[test]
    fn after_verified_reports_a_refusal_as_a_failed_write() {
        let refused = after_verified(Err(VykarError::UnsupportedBackend("no link".into())));
        assert!(matches!(refused, Err(VykarError::Other(m)) if m.contains("no link")));
        assert_eq!(after_verified(Ok(Created::Yes)).unwrap(), Created::Yes);
        assert!(matches!(
            after_verified(Err(VykarError::Interrupted)),
            Err(VykarError::Interrupted)
        ));
    }

    #[test]
    fn require_maps_unsupported() {
        let map = Map::default();
        let v = Verifier::new();
        let raw = |_: &str, _: &[u8]| -> Result<Created> {
            Err(VykarError::UnsupportedBackend(
                "refuses If-None-Match".into(),
            ))
        };
        match v.require("S3", &map, &raw) {
            Err(VykarError::UnsupportedBackend(m)) => {
                assert!(m.contains("S3"), "{m}");
                assert!(m.contains("refuses If-None-Match"), "{m}");
            }
            other => panic!("expected UnsupportedBackend, got {other:?}"),
        }
    }

    #[test]
    fn verifier_put_if_absent_checks_then_creates() {
        let map = Map::default();
        let v = Verifier::new();
        let raw = |k: &str, d: &[u8]| honest(&map, k, d);
        assert_eq!(
            v.put_if_absent("test", &map, &raw, "index.h/1", b"a")
                .unwrap(),
            Created::Yes
        );
        assert_eq!(
            v.put_if_absent("test", &map, &raw, "index.h/1", b"b")
                .unwrap(),
            Created::AlreadyExists
        );
        assert_eq!(map.objects.lock().unwrap().get("index.h/1").unwrap(), b"a");

        let ignoring = Map::default();
        let v = Verifier::new();
        let raw = |k: &str, d: &[u8]| {
            ignoring.set(k, d);
            Ok(Created::Yes)
        };
        match v.put_if_absent("test", &ignoring, &raw, "index.h/1", b"a") {
            Err(VykarError::UnsupportedBackend(m)) => {
                assert!(m.contains(IGNORES_CONDITION), "{m}");
            }
            other => panic!("expected UnsupportedBackend, got {other:?}"),
        }
        assert!(v.refused());
        assert!(!ignoring.objects.lock().unwrap().contains_key("index.h/1"));
    }

    #[test]
    fn check_args_refuses_temp_names_and_large_bodies() {
        assert!(check_args("locks/a", b"x").is_ok());
        assert!(matches!(
            check_args("locks/.tmp.a.1", b"x"),
            Err(VykarError::InvalidFormat(_))
        ));
        let big = vec![0u8; MAX_PUT_IF_ABSENT_BYTES + 1];
        assert!(matches!(
            check_args("locks/a", &big),
            Err(VykarError::InvalidFormat(_))
        ));
        assert!(check_args("locks/a", &big[..MAX_PUT_IF_ABSENT_BYTES]).is_ok());
    }

    #[test]
    fn concurrent_first_verify_runs_one_check() {
        let map = Arc::new(Map::default());
        let v = Arc::new(Verifier::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = std::sync::Barrier::new(4);
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    let raw = |k: &str, d: &[u8]| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        honest(&map, k, d)
                    };
                    barrier.wait();
                    assert_eq!(v.verify(&*map, &raw).unwrap(), ConditionalSupport::Verified);
                });
            }
        });
        assert_eq!(calls.load(Ordering::SeqCst), 2, "one check, two creates");
    }

    #[test]
    fn probe_key_prefix_is_dir_plus_probe_prefix() {
        assert_eq!(
            PUT_IF_ABSENT_PROBE_KEY_PREFIX,
            format!("{PROBE_DIR}{}", crate::probe::PROBE_PREFIX)
        );
    }

    /// A primitive honest on the sequential check's keys and `racy` on the
    /// race's (`-race` in the key), with the raw calls counted.
    struct Scripted<'a, F> {
        map: &'a Map,
        calls: AtomicUsize,
        keys: Mutex<Vec<String>>,
        racy: F,
    }

    impl<'a, F> Scripted<'a, F>
    where
        F: Fn(&Map, &str, &[u8]) -> Result<Created> + Sync,
    {
        fn new(map: &'a Map, racy: F) -> Self {
            Self {
                map,
                calls: AtomicUsize::new(0),
                keys: Mutex::new(Vec::new()),
                racy,
            }
        }
        fn put(&self, k: &str, d: &[u8]) -> Result<Created> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.keys.lock().unwrap().push(k.to_string());
            if k.contains("-race") {
                (self.racy)(self.map, k, d)
            } else {
                honest(self.map, k, d)
            }
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn race_verified_on_an_honest_primitive() {
        let map = Map::default();
        let v = Verifier::new();
        let p = Scripted::new(&map, honest);
        let raw = |k: &str, d: &[u8]| p.put(k, d);
        assert_eq!(
            v.verify_at_init(&map, &raw).unwrap(),
            ConditionalSupport::Verified
        );
        assert_eq!(p.calls(), 2 + INIT_RACE_ROUNDS * INIT_RACE_WRITERS);
        assert_eq!(map.len(), 0, "every probe key is deleted");
        assert_eq!(
            v.verify_at_init(&map, &raw).unwrap(),
            ConditionalSupport::Verified
        );
        assert_eq!(v.verify(&map, &raw).unwrap(), ConditionalSupport::Verified);
        assert_eq!(
            p.calls(),
            2 + INIT_RACE_ROUNDS * INIT_RACE_WRITERS,
            "cached"
        );
        assert!(!v.refused());
    }

    /// Lets the first two creates of each race key both win.
    fn two_winners(
        calls: &Mutex<HashMap<String, usize>>,
        map: &Map,
        k: &str,
        d: &[u8],
    ) -> Result<Created> {
        let mut calls = calls.lock().unwrap();
        let n = calls.entry(k.to_string()).or_default();
        *n += 1;
        if *n <= 2 {
            map.set(k, d);
            Ok(Created::Yes)
        } else {
            Ok(Created::AlreadyExists)
        }
    }

    #[test]
    fn race_catches_two_winners() {
        let map = Map::default();
        let v = Verifier::new();
        let per_key = Mutex::new(HashMap::new());
        let p = Scripted::new(&map, |m: &Map, k: &str, d: &[u8]| {
            two_winners(&per_key, m, k, d)
        });
        let raw = |k: &str, d: &[u8]| p.put(k, d);
        let ConditionalSupport::Unsupported { reason } = v.verify_at_init(&map, &raw).unwrap()
        else {
            panic!("two winners must be Unsupported");
        };
        assert!(reason.starts_with(IGNORES_CONDITION), "{reason}");
        assert!(reason.contains("2 of 8"), "{reason}");
        assert!(v.refused());
        assert_eq!(map.len(), 0, "the race key is deleted");
        let calls = p.calls();
        assert!(matches!(
            v.verify(&map, &raw).unwrap(),
            ConditionalSupport::Unsupported { reason: r } if r == reason
        ));
        assert!(matches!(
            v.verify_at_init(&map, &raw).unwrap(),
            ConditionalSupport::Unsupported { .. }
        ));
        assert_eq!(p.calls(), calls, "cached");
    }

    #[test]
    fn race_after_cached_verified_flips_to_unsupported() {
        let map = Map::default();
        let v = Verifier::new();
        let per_key = Mutex::new(HashMap::new());
        let p = Scripted::new(&map, |m: &Map, k: &str, d: &[u8]| {
            two_winners(&per_key, m, k, d)
        });
        let raw = |k: &str, d: &[u8]| p.put(k, d);
        assert_eq!(v.verify(&map, &raw).unwrap(), ConditionalSupport::Verified);
        assert!(matches!(
            v.verify_at_init(&map, &raw).unwrap(),
            ConditionalSupport::Unsupported { .. }
        ));
        assert!(v.refused());
        assert!(matches!(
            v.verify(&map, &raw).unwrap(),
            ConditionalSupport::Unsupported { .. }
        ));
    }

    #[test]
    fn race_writer_error_is_undecided() {
        let map = Map::default();
        let v = Verifier::new();
        let failed = AtomicBool::new(false);
        let p = Scripted::new(&map, |m: &Map, k: &str, d: &[u8]| {
            if !failed.swap(true, Ordering::SeqCst) {
                return Err(VykarError::Other("503".into()));
            }
            honest(m, k, d)
        });
        let raw = |k: &str, d: &[u8]| p.put(k, d);
        assert!(v.verify_at_init(&map, &raw).is_err());
        assert!(!v.refused());
        {
            let state = v.state.lock().unwrap();
            assert!(!state.raced);
            assert_eq!(state.answer, Some(ConditionalSupport::Verified));
        }
        assert_eq!(map.len(), 0, "the race key is deleted");
    }

    #[test]
    fn race_writer_refusal_is_undecided() {
        let map = Map::default();
        let v = Verifier::new();
        let failed = AtomicBool::new(false);
        let p = Scripted::new(&map, |m: &Map, k: &str, d: &[u8]| {
            if !failed.swap(true, Ordering::SeqCst) {
                return Err(VykarError::UnsupportedBackend("link failed".into()));
            }
            honest(m, k, d)
        });
        let raw = |k: &str, d: &[u8]| p.put(k, d);
        assert!(matches!(
            v.verify_at_init(&map, &raw),
            Err(VykarError::Other(m)) if m.contains("link failed")
        ));
        assert!(!v.refused());
    }

    #[test]
    fn race_lost_reply_counts_as_one_winner() {
        let map = Map::default();
        let v = Verifier::new();
        let p = Scripted::new(&map, |m: &Map, k: &str, d: &[u8]| {
            let mut objects = m.objects.lock().unwrap();
            if !objects.contains_key(k) {
                objects.insert(k.to_string(), d.to_vec());
            }
            // The winner's reply was lost; its retry finds its own object.
            Ok(Created::AlreadyExists)
        });
        let raw = |k: &str, d: &[u8]| p.put(k, d);
        assert_eq!(
            v.verify_at_init(&map, &raw).unwrap(),
            ConditionalSupport::Verified
        );
    }

    #[test]
    fn race_replaced_winner_is_unsupported() {
        let map = Map::default();
        let v = Verifier::new();
        let per_key = Mutex::new(HashMap::<String, usize>::new());
        let p = Scripted::new(&map, |m: &Map, k: &str, d: &[u8]| {
            let mut calls = per_key.lock().unwrap();
            let n = calls.entry(k.to_string()).or_default();
            *n += 1;
            // The first create wins; the second replaces it but says it existed.
            m.set(k, d);
            Ok(if *n == 1 {
                Created::Yes
            } else {
                Created::AlreadyExists
            })
        });
        // Only the first two creates of a key write; later ones must not
        // replace the second's bytes again, so stop writing after two.
        let raw = |k: &str, d: &[u8]| {
            let n = per_key.lock().unwrap().get(k).copied().unwrap_or(0);
            if k.contains("-race") && n >= 2 {
                p.calls.fetch_add(1, Ordering::SeqCst);
                return Ok(Created::AlreadyExists);
            }
            p.put(k, d)
        };
        let ConditionalSupport::Unsupported { reason } = v.verify_at_init(&map, &raw).unwrap()
        else {
            panic!("a replaced winner must be Unsupported");
        };
        assert!(reason.contains("replaced the winner"), "{reason}");
        assert!(v.refused());
    }

    #[test]
    fn probe_keys_live_under_sessions() {
        let map = Map::default();
        let v = Verifier::new();
        let p = Scripted::new(&map, honest);
        let raw = |k: &str, d: &[u8]| p.put(k, d);
        v.verify_at_init(&map, &raw).unwrap();
        let keys = p.keys.lock().unwrap();
        assert!(!keys.is_empty());
        for k in keys.iter() {
            assert!(k.starts_with(PUT_IF_ABSENT_PROBE_KEY_PREFIX), "{k}");
            assert!(!k.contains('.'), "{k}");
        }
    }

    #[test]
    fn refuse_caches_unsupported_without_a_request() {
        let map = Map::default();
        let v = Verifier::new();
        let calls = AtomicUsize::new(0);
        let raw = |k: &str, d: &[u8]| {
            calls.fetch_add(1, Ordering::SeqCst);
            honest(&map, k, d)
        };
        assert!(matches!(
            v.refuse("known bad"),
            ConditionalSupport::Unsupported { reason } if reason == "known bad"
        ));
        assert!(v.refused());
        assert!(matches!(
            v.verify_at_init(&map, &raw).unwrap(),
            ConditionalSupport::Unsupported { reason } if reason == "known bad"
        ));
        assert!(v.put_if_absent("t", &map, &raw, "k", b"x").is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
