//! Atomic create-if-absent ("put-if-absent") and the one check that decides
//! whether a backend's storage honours it.
//!
//! Each backend that can do it has a raw primitive (no-clobber rename on a
//! local filesystem, `If-None-Match: *` on vykar-server and S3,
//! `hardlink@openssh.com` on SFTP). None of them is trusted blindly: the first
//! [`ConditionalWrite::verify`] or [`ConditionalWrite::put_if_absent`] on a
//! backend instance runs [`sequential_check`] with that primitive and caches a
//! conclusive answer in the backend's [`Verifier`].

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use vykar_types::error::{Result, VykarError};

use crate::StorageBackend;

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
    /// a fresh key under `locks/` twice, expect the second create refused and
    /// the first bytes kept, delete the key. A conclusive answer is cached for
    /// the life of the backend. `Err` means undecided (no write permission,
    /// transport failure, cancel): nothing is cached and the next call checks
    /// again. Writes only when called; nothing calls it on open.
    ///
    /// The check is sequential: it catches a storage that ignores the
    /// condition, not one that lets two concurrent creates both win.
    ///
    /// # Errors
    ///
    /// Undecided, as above.
    fn verify(&self) -> Result<ConditionalSupport>;
}

/// A backend's unverified primitive. `Err(VykarError::UnsupportedBackend(_))`
/// means "the storage refuses the mechanism itself" (a convention internal to
/// this module). Any other `Err` is undecided.
pub(crate) type RawPut<'a> = &'a dyn Fn(&str, &[u8]) -> Result<Created>;

/// The per-backend cache of [`ConditionalWrite::verify`].
pub(crate) struct Verifier {
    /// Conclusive results only.
    state: Mutex<Option<ConditionalSupport>>,
    /// Set together with a cached `Unsupported`; read lock-free.
    refused: AtomicBool,
}

impl Verifier {
    pub(crate) const fn new() -> Self {
        Self {
            state: Mutex::new(None),
            refused: AtomicBool::new(false),
        }
    }

    /// `conditional()` returns `None` once this is true.
    pub(crate) fn refused(&self) -> bool {
        self.refused.load(Ordering::Acquire)
    }

    /// The cached answer, or the sequential check run under the mutex
    /// (concurrent callers wait for the one check). Caches only `Ok(..)`.
    pub(crate) fn verify(
        &self,
        store: &dyn StorageBackend,
        raw: RawPut<'_>,
    ) -> Result<ConditionalSupport> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cached) = state.as_ref() {
            return Ok(cached.clone());
        }
        let answer = sequential_check(store, raw)?;
        if matches!(answer, ConditionalSupport::Unsupported { .. }) {
            self.refused.store(true, Ordering::Release);
        }
        *state = Some(answer.clone());
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
    match outcome {
        Err(VykarError::UnsupportedBackend(m)) => Err(VykarError::Other(format!(
            "conditional write failed on a verified backend: {m}"
        ))),
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

/// Create a fresh key twice and expect the second create refused with the
/// first bytes kept. Deletes the key after the first create attempt.
fn sequential_check(store: &dyn StorageBackend, raw: RawPut<'_>) -> Result<ConditionalSupport> {
    let key = format!("locks/{}", crate::probe::new_prefix());
    let a = crate::probe::payload("A", &key, 64);
    let b = crate::probe::payload("B", &key, 64);
    let result = check_steps(store, raw, &key, &a, &b);
    if let Err(e) = store.delete(&key) {
        tracing::warn!(key = %key, error = %e, "put-if-absent check: could not delete its probe object");
    }
    result
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
            return Ok(unsupported(
                "ignores the condition: a second create of the same key succeeded",
            ));
        }
        Err(VykarError::UnsupportedBackend(m)) => return Ok(unsupported(m)),
        Err(e) => return Err(e),
    }
    match store.get(key)? {
        Some(stored) if stored == a => Ok(ConditionalSupport::Verified),
        Some(_) => Ok(unsupported(
            "ignores the condition: the second create replaced the object",
        )),
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
        assert!(v.state.lock().unwrap().is_none());
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
}
