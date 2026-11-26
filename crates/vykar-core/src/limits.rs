// libc syscalls for niceness control (getpriority/setpriority/SYS_gettid).
// Each `unsafe { }` has a SAFETY comment (enforced by undocumented_unsafe_blocks).
#![allow(unsafe_code)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(unix)]
use nix::errno::Errno;

use crate::config::ResourceLimitsConfig;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::error::Result;

// ── Rate limiting runtime ────────────────────────────────────────────────────

const BYTES_PER_MIB: u64 = 1024 * 1024;

fn mib_per_sec_to_bytes_per_sec(mib_per_sec: u64) -> u64 {
    mib_per_sec.saturating_mul(BYTES_PER_MIB)
}

#[derive(Debug)]
struct LimiterState {
    start: Instant,
    bytes_consumed: u128,
}

/// Simple process-local byte-rate limiter shared by multiple call sites.
#[derive(Debug)]
pub struct ByteRateLimiter {
    bytes_per_sec: u64,
    state: Mutex<LimiterState>,
}

impl ByteRateLimiter {
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            state: Mutex::new(LimiterState {
                start: Instant::now(),
                bytes_consumed: 0,
            }),
        }
    }

    pub fn consume(&self, bytes: usize) {
        if bytes == 0 || self.bytes_per_sec == 0 {
            return;
        }

        let sleep_duration = {
            let mut state = match self.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.bytes_consumed = state.bytes_consumed.saturating_add(bytes as u128);

            let elapsed_secs = state.start.elapsed().as_secs_f64();
            #[allow(
                clippy::cast_precision_loss,
                reason = "rate-limit time math; precision loss bounded by u128/u64 magnitudes"
            )]
            let expected_secs = state.bytes_consumed as f64 / self.bytes_per_sec as f64;
            if expected_secs > elapsed_secs {
                Some(Duration::from_secs_f64(expected_secs - elapsed_secs))
            } else {
                None
            }
        }; // lock released

        if let Some(d) = sleep_duration {
            std::thread::sleep(d);
        }
    }
}

/// Wrap a storage backend with rate limiting based on config bandwidth caps.
pub fn wrap_storage_backend(
    inner: Box<dyn StorageBackend>,
    limits: &ResourceLimitsConfig,
) -> Box<dyn StorageBackend> {
    let read_bps = mib_per_sec_to_bytes_per_sec(limits.download_mib_per_sec);
    let write_bps = mib_per_sec_to_bytes_per_sec(limits.upload_mib_per_sec);
    if read_bps == 0 && write_bps == 0 {
        return inner;
    }

    let read_limiter = (read_bps > 0).then(|| Arc::new(ByteRateLimiter::new(read_bps)));
    let write_limiter = (write_bps > 0).then(|| Arc::new(ByteRateLimiter::new(write_bps)));

    Box::new(ThrottledStorageBackend {
        inner,
        read_limiter,
        write_limiter,
    })
}

struct ThrottledStorageBackend {
    inner: Box<dyn StorageBackend>,
    read_limiter: Option<Arc<ByteRateLimiter>>,
    write_limiter: Option<Arc<ByteRateLimiter>>,
}

impl InnerBackend for ThrottledStorageBackend {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

// Only the byte-moving methods are throttled. Everything else is forwarded by
// the macro. `server_ops()` and `batch_delete()` are forwarded, so the
// server-side operations, whose bytes never cross the client's link, reach the
// inner backend; the accessors have no default, so this impl cannot silently
// answer None on behalf of a backend that has them, which is how server-side
// verify and init once came to be disabled on every throttled REST repo.
//
// Tokens are charged per object (before an upload is sent, after a read
// completes), so the caps bound the average rate, not the peak: each object
// still moves at link speed. Shaping the peak would need the limiter inside
// the HTTP backends. Conditional writes are charged like `put`: the wrapper
// offers put-if-absent exactly when the inner backend does, and answers it
// itself so its bytes pass the write limiter.
delegate_storage_backend! {
    for ThrottledStorageBackend;
    except [
        get, put, get_range, get_range_into, open_range, put_owned, put_pack, begin_staged_put,
        conditional, profile
    ];

    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        self.inner
            .conditional()
            .map(|_| self as &dyn vykar_storage::ConditionalWrite)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let out = self.inner.get(key)?;
        if let (Some(limiter), Some(data)) = (self.read_limiter.as_ref(), out.as_ref()) {
            limiter.consume(data.len());
        }
        Ok(out)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if let Some(limiter) = self.write_limiter.as_ref() {
            limiter.consume(data.len());
        }
        self.inner.put(key, data)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let out = self.inner.get_range(key, offset, length)?;
        if let (Some(limiter), Some(data)) = (self.read_limiter.as_ref(), out.as_ref()) {
            limiter.consume(data.len());
        }
        Ok(out)
    }

    fn get_range_into(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &mut Vec<u8>,
    ) -> Result<bool> {
        let found = self.inner.get_range_into(key, offset, length, buf)?;
        if found
            && let Some(limiter) = self.read_limiter.as_ref() {
                limiter.consume(buf.len());
            }
        Ok(found)
    }

    // Buffer through the throttled `get_range_into` above, so a throttled
    // range costs the same tokens (gaps included) as before streaming reads.
    fn open_range<'a>(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &'a mut Vec<u8>,
    ) -> Result<Option<Box<dyn vykar_storage::RangeReader + 'a>>> {
        vykar_storage::buffered_range(self, key, offset, length, buf)
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        if let Some(limiter) = self.write_limiter.as_ref() {
            limiter.consume(data.len());
        }
        self.inner.put_owned(key, data)
    }

    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        if let Some(limiter) = self.write_limiter.as_ref() {
            limiter.consume(data.len());
        }
        self.inner.put_pack(key, data)
    }

    // A staged pack is written from the backup's consumer thread, where
    // throttling would stall chunk processing instead of one upload thread.
    // Keep throttled repositories on the buffered `put_pack` path above.
    fn begin_staged_put(&self, _prefix: &str) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        Ok(None)
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            staged_put: false,
            ..self.inner.profile()
        }
    }
}

/// Why the throttle answers put-if-absent itself when the inner backend has
/// stopped offering it (a check on the inner concluded `Unsupported`).
const WITHDRAWN: &str = "the backend no longer offers it";

impl vykar_storage::ConditionalWrite for ThrottledStorageBackend {
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<vykar_storage::Created> {
        // Checked before charging, so a refused call costs no tokens.
        let inner = self
            .inner
            .conditional()
            .ok_or_else(|| vykar_storage::put_if_absent_unavailable(WITHDRAWN))?;
        if let Some(limiter) = self.write_limiter.as_ref() {
            limiter.consume(data.len());
        }
        inner.put_if_absent(key, data)
    }

    fn verify(&self) -> Result<vykar_storage::ConditionalSupport> {
        match self.inner.conditional() {
            Some(inner) => inner.verify(),
            None => Ok(vykar_storage::ConditionalSupport::Unsupported {
                reason: format!("put-if-absent: {WITHDRAWN}"),
            }),
        }
    }

    fn verify_at_init(&self) -> Result<vykar_storage::ConditionalSupport> {
        match self.inner.conditional() {
            Some(inner) => inner.verify_at_init(),
            None => Ok(vykar_storage::ConditionalSupport::Unsupported {
                reason: format!("put-if-absent: {WITHDRAWN}"),
            }),
        }
    }
}

/// Set the nice value of every thread in this process. One-way: unprivileged
/// processes cannot lower nice again, so nothing is restored afterwards.
pub fn apply_process_nice(target_nice: i32) -> std::result::Result<(), String> {
    if target_nice == 0 {
        return Ok(());
    }

    #[cfg(unix)]
    {
        set_process_nice(target_nice)
    }

    #[cfg(not(unix))]
    {
        tracing::debug!("limits.nice={target_nice} ignored: not supported on this platform");
        Ok(())
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn get_process_nice() -> std::result::Result<i32, String> {
    Errno::clear();
    // SAFETY: getpriority with PRIO_PROCESS/0 reads the calling thread's nice value.
    // No pointer arguments; errno is cleared beforehand to distinguish -1 return from error.
    let value = unsafe { nix::libc::getpriority(nix::libc::PRIO_PROCESS, 0) };
    let errno = Errno::last_raw();
    if value == -1 && errno != 0 {
        return Err(format!(
            "getpriority failed: {}",
            std::io::Error::from_raw_os_error(errno)
        ));
    }
    Ok(value)
}

// Nice is a one-way ratchet here: a thread is only touched when its current
// nice is below the target. Lowering nice needs CAP_SYS_NICE or a raised
// RLIMIT_NICE on Linux (root on macOS), which an unprivileged backup process
// does not have, so the value is never restored — a long-lived daemon or GUI
// keeps the highest value any repository requested so far. Skipping threads
// that are already at or above the target keeps a second apply with a lower
// value (per-repository override, or a process started under `nice -n`) from
// failing with EACCES.
//
// On Linux, `setpriority(PRIO_PROCESS, 0, …)` only renices the calling thread —
// NPTL stores the nice value per task, not per process, despite POSIX wording.
// To match user expectations we walk /proc/self/task and renice every TID.
// Other Unix targets (macOS) honor POSIX per-process semantics, so a single
// syscall is enough there.
#[cfg(target_os = "linux")]
fn set_process_nice(value: i32) -> std::result::Result<(), String> {
    let entries = std::fs::read_dir("/proc/self/task")
        .map_err(|e| format!("cannot list /proc/self/task: {e}"))?;

    let mut last_err: Option<String> = None;
    let mut applied = 0usize;
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                last_err = Some(format!("read_dir entry failed: {e}"));
                continue;
            }
        };
        let tid: i32 = match entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<i32>().ok())
        {
            Some(t) => t,
            None => continue,
        };

        Errno::clear();
        // SAFETY: getpriority with PRIO_PROCESS and a TID reads that task's
        // nice value. No pointer arguments; errno is cleared beforehand to
        // distinguish a -1 return from an error.
        let current = unsafe { nix::libc::getpriority(nix::libc::PRIO_PROCESS, tid as u32) };
        if current == -1 {
            let errno = Errno::last_raw();
            // ESRCH: thread exited between readdir and getpriority — benign.
            if errno == nix::libc::ESRCH {
                continue;
            }
            if errno != 0 {
                let msg = format!(
                    "getpriority(tid={tid}) failed: {}",
                    std::io::Error::from_raw_os_error(errno)
                );
                tracing::warn!("{msg}");
                last_err = Some(msg);
                continue;
            }
        }
        if current >= value {
            applied += 1;
            continue;
        }

        Errno::clear();
        // SAFETY: setpriority with PRIO_PROCESS and a TID adjusts that task's
        // nice value. No pointer arguments; the value is range-checked by the
        // kernel.
        let rc = unsafe { nix::libc::setpriority(nix::libc::PRIO_PROCESS, tid as u32, value) };
        if rc != 0 {
            let errno = Errno::last_raw();
            // ESRCH: thread exited between readdir and setpriority — benign.
            if errno == nix::libc::ESRCH {
                continue;
            }
            let msg = if errno == 0 {
                format!("setpriority(tid={tid}) failed")
            } else {
                format!(
                    "setpriority(tid={tid}) failed: {}",
                    std::io::Error::from_raw_os_error(errno)
                )
            };
            tracing::warn!("{msg}");
            last_err = Some(msg);
            continue;
        }
        applied += 1;
    }

    if applied == 0 {
        if let Some(msg) = last_err {
            return Err(msg);
        }
        return Err("setpriority: no tasks found under /proc/self/task".to_string());
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "linux")))]
fn set_process_nice(value: i32) -> std::result::Result<(), String> {
    if get_process_nice()? >= value {
        return Ok(());
    }
    Errno::clear();
    // SAFETY: setpriority with PRIO_PROCESS/0 adjusts the calling process's nice value.
    // No pointer arguments; the value parameter is range-checked by the kernel.
    let rc = unsafe { nix::libc::setpriority(nix::libc::PRIO_PROCESS, 0, value) };
    if rc != 0 {
        let errno = Errno::last_raw();
        if errno == 0 {
            return Err("setpriority failed".to_string());
        }
        return Err(format!(
            "setpriority failed: {}",
            std::io::Error::from_raw_os_error(errno)
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use vykar_protocol::{ServerCapabilities, VerifyPacksPlanRequest, VerifyPacksResponse};
    use vykar_storage::{BatchDelete, BatchDeleteScope, ServerOp, ServerOps};

    #[test]
    fn mib_conversion() {
        assert_eq!(mib_per_sec_to_bytes_per_sec(0), 0);
        assert_eq!(mib_per_sec_to_bytes_per_sec(1), 1024 * 1024);
        assert_eq!(mib_per_sec_to_bytes_per_sec(8), 8 * 1024 * 1024);
    }

    /// Regression: `ThrottledStorageBackend` must forward the server-side ops.
    /// A wrapper answering None from the accessors would silently disable
    /// server-side pack verification, server-side init and batch deletes as
    /// soon as a bandwidth cap is configured on a REST repo.
    #[test]
    fn throttled_backend_forwards_server_side_ops() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct ServerOpRecorder {
            verify_calls: Arc<AtomicUsize>,
            init_calls: Arc<AtomicUsize>,
            sizes_calls: Arc<AtomicUsize>,
            batch_calls: Arc<AtomicUsize>,
        }

        impl ServerOps for ServerOpRecorder {
            fn offers(&self, op: ServerOp) -> bool {
                matches!(op, ServerOp::VerifyPacks | ServerOp::ObjectSizes)
            }
            fn capabilities(&self) -> Result<ServerCapabilities> {
                Ok(ServerCapabilities::default())
            }
            fn init(&self) -> Result<()> {
                self.init_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn verify_packs(&self, _plan: &VerifyPacksPlanRequest) -> Result<VerifyPacksResponse> {
                self.verify_calls.fetch_add(1, Ordering::SeqCst);
                Ok(VerifyPacksResponse {
                    results: Vec::new(),
                    truncated: false,
                })
            }
            fn object_sizes(&self, keys: &[String]) -> Result<Vec<Option<u64>>> {
                self.sizes_calls.fetch_add(1, Ordering::SeqCst);
                Ok(vec![Some(1); keys.len()])
            }
        }

        impl BatchDelete for ServerOpRecorder {
            fn delete_keys(&self, _keys: &[String], _scope: BatchDeleteScope) -> Result<()> {
                self.batch_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        impl StorageBackend for ServerOpRecorder {
            fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
                Ok(None)
            }
            fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
                Ok(())
            }
            fn delete(&self, _key: &str) -> Result<()> {
                Ok(())
            }
            fn exists(&self, _key: &str) -> Result<bool> {
                Ok(false)
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
            fn batch_delete(&self) -> Option<&dyn BatchDelete> {
                Some(self)
            }
            fn server_ops(&self) -> Option<&dyn ServerOps> {
                Some(self)
            }

            fn profile(&self) -> vykar_storage::BackendProfile {
                vykar_storage::BackendProfile {
                    local: false,
                    staged_put: false,
                    request_width: 1,
                    transfer_width: 1,
                }
            }

            vykar_storage::unsupported_server_ops!(except [batch_delete, server_ops]);
        }

        let verify_calls = Arc::new(AtomicUsize::new(0));
        let init_calls = Arc::new(AtomicUsize::new(0));
        let sizes_calls = Arc::new(AtomicUsize::new(0));
        let batch_calls = Arc::new(AtomicUsize::new(0));
        let inner = Box::new(ServerOpRecorder {
            verify_calls: Arc::clone(&verify_calls),
            init_calls: Arc::clone(&init_calls),
            sizes_calls: Arc::clone(&sizes_calls),
            batch_calls: Arc::clone(&batch_calls),
        });

        // Non-zero caps, so the wrapper is actually installed.
        let limits = ResourceLimitsConfig {
            upload_mib_per_sec: 1,
            download_mib_per_sec: 1,
            ..ResourceLimitsConfig::default()
        };
        let wrapped = wrap_storage_backend(inner, &limits);

        let plan =
            VerifyPacksPlanRequest::new(Vec::new(), vykar_types::hash::HashAlgorithm::Blake3);
        let server = wrapped.server_ops().expect("server ops forwarded");
        let response = server.verify_packs(&plan).expect("verify forwarded");
        assert!(response.results.is_empty());
        server.init().expect("init forwarded");
        let sizes = server
            .object_sizes(&["packs/00/x".to_string()])
            .expect("object_sizes forwarded");
        assert_eq!(sizes, vec![Some(1)]);
        wrapped
            .batch_delete()
            .expect("batch delete forwarded")
            .delete_keys(&[], BatchDeleteScope::KeysOnly)
            .expect("batch delete reaches the inner backend");

        assert_eq!(verify_calls.load(Ordering::SeqCst), 1);
        assert_eq!(init_calls.load(Ordering::SeqCst), 1);
        assert_eq!(sizes_calls.load(Ordering::SeqCst), 1);
        assert_eq!(batch_calls.load(Ordering::SeqCst), 1);
    }

    /// Put-if-absent is offered exactly when the inner backend offers it,
    /// reaches the inner backend, and is charged to the write limiter.
    #[test]
    fn throttled_backend_wraps_conditional_writes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use vykar_storage::{ConditionalSupport, ConditionalWrite, Created};

        struct CondLeaf {
            offers: bool,
            creates: AtomicUsize,
        }

        impl StorageBackend for CondLeaf {
            fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
                Ok(None)
            }
            fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
                Ok(())
            }
            fn delete(&self, _key: &str) -> Result<()> {
                Ok(())
            }
            fn exists(&self, _key: &str) -> Result<bool> {
                Ok(false)
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
            fn conditional(&self) -> Option<&dyn ConditionalWrite> {
                self.offers.then_some(self as &dyn ConditionalWrite)
            }
            fn profile(&self) -> vykar_storage::BackendProfile {
                vykar_storage::BackendProfile {
                    local: false,
                    staged_put: false,
                    request_width: 1,
                    transfer_width: 1,
                }
            }
            vykar_storage::unsupported_server_ops!(except[conditional]);
        }

        impl ConditionalWrite for CondLeaf {
            fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<Created> {
                self.creates.fetch_add(1, Ordering::SeqCst);
                Ok(Created::Yes)
            }
            fn verify(&self) -> Result<ConditionalSupport> {
                Ok(ConditionalSupport::Verified)
            }
            fn verify_at_init(&self) -> Result<ConditionalSupport> {
                self.verify()
            }
        }

        let limiter = Arc::new(ByteRateLimiter::new(u64::MAX));
        let wrapped = ThrottledStorageBackend {
            inner: Box::new(CondLeaf {
                offers: true,
                creates: AtomicUsize::new(0),
            }),
            read_limiter: None,
            write_limiter: Some(Arc::clone(&limiter)),
        };
        let cond = wrapped
            .conditional()
            .expect("offered like the inner backend");
        assert_eq!(cond.verify().unwrap(), ConditionalSupport::Verified);
        assert_eq!(cond.verify_at_init().unwrap(), ConditionalSupport::Verified);
        assert_eq!(
            cond.put_if_absent("locks/k", &[0u8; 100]).unwrap(),
            Created::Yes
        );
        assert_eq!(limiter.state.lock().unwrap().bytes_consumed, 100);

        let wrapped = ThrottledStorageBackend {
            inner: Box::new(CondLeaf {
                offers: false,
                creates: AtomicUsize::new(0),
            }),
            read_limiter: None,
            write_limiter: Some(Arc::new(ByteRateLimiter::new(u64::MAX))),
        };
        assert!(wrapped.conditional().is_none());
        // A handle obtained before the inner withdrew answers conclusively,
        // without charging the limiter.
        assert!(matches!(
            ConditionalWrite::verify(&wrapped).unwrap(),
            ConditionalSupport::Unsupported { .. }
        ));
        let refused = ConditionalWrite::put_if_absent(&wrapped, "locks/k", &[0u8; 10]).unwrap_err();
        assert!(
            matches!(&refused, vykar_types::error::VykarError::UnsupportedBackend(m) if m.contains("put-if-absent unavailable")),
            "{refused:?}"
        );
    }

    /// Packs reach the inner `put_pack` (not `put`/`put_owned`), release
    /// signal intact.
    #[test]
    fn throttled_backend_forwards_put_pack() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct PackRecorder {
            pack_calls: Arc<AtomicUsize>,
        }

        impl StorageBackend for PackRecorder {
            fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
                Ok(None)
            }
            fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
                Err(vykar_types::error::VykarError::Other(
                    "pack reached put".into(),
                ))
            }
            fn delete(&self, _key: &str) -> Result<()> {
                Ok(())
            }
            fn exists(&self, _key: &str) -> Result<bool> {
                Ok(false)
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
            fn put_owned(&self, _key: &str, _data: Vec<u8>) -> Result<()> {
                Err(vykar_types::error::VykarError::Other(
                    "pack reached put_owned".into(),
                ))
            }
            fn put_pack(&self, _key: &str, data: vykar_storage::PackData) -> Result<()> {
                self.pack_calls.fetch_add(1, Ordering::SeqCst);
                drop(data);
                Ok(())
            }
            fn profile(&self) -> vykar_storage::BackendProfile {
                vykar_storage::BackendProfile {
                    local: false,
                    staged_put: false,
                    request_width: 1,
                    transfer_width: 1,
                }
            }

            vykar_storage::unsupported_server_ops!();
        }

        let pack_calls = Arc::new(AtomicUsize::new(0));
        let inner = Box::new(PackRecorder {
            pack_calls: Arc::clone(&pack_calls),
        });
        // Non-zero cap, so the wrapper is actually installed.
        let limits = ResourceLimitsConfig {
            upload_mib_per_sec: 1,
            ..ResourceLimitsConfig::default()
        };
        let wrapped = wrap_storage_backend(inner, &limits);

        let released = Arc::new(AtomicUsize::new(0));
        let r = Arc::clone(&released);
        let data = vykar_storage::PackData::with_release(vec![0; 16], move || {
            r.fetch_add(1, Ordering::SeqCst);
        });
        wrapped.put_pack("packs/00/x", data).unwrap();
        assert_eq!(pack_calls.load(Ordering::SeqCst), 1);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }

    /// The throttle keeps packs on its `put` path, so its profile must not
    /// promise staged writes; everything else is the inner backend's.
    #[test]
    fn throttled_backend_disables_staged_put_in_profile() {
        let tmp = tempfile::tempdir().unwrap();
        let inner = vykar_storage::local_backend::LocalBackend::new(tmp.path().to_str().unwrap())
            .unwrap()
            .with_max_connections(Some(16));
        let limits = ResourceLimitsConfig {
            upload_mib_per_sec: 1,
            ..ResourceLimitsConfig::default()
        };
        let wrapped = wrap_storage_backend(Box::new(inner), &limits);
        assert_eq!(
            wrapped.profile(),
            vykar_storage::BackendProfile {
                local: true,
                staged_put: false,
                request_width: 8,
                transfer_width: 16,
            }
        );
        assert!(wrapped.begin_staged_put("packs").unwrap().is_none());
    }

    #[test]
    fn wrap_storage_noop_when_unlimited() {
        let limits = ResourceLimitsConfig::default();
        // With 0/0 bandwidth, wrap_storage_backend returns inner unchanged.
        // Just verify it doesn't panic.
        assert_eq!(limits.upload_mib_per_sec, 0);
        assert_eq!(limits.download_mib_per_sec, 0);
    }

    // Regression for issue #119: apply_process_nice must renice every thread
    // of the calling process on Linux, not just the calling task. Also covers
    // issue #187: re-applying the same or a lower value must succeed without
    // touching threads that are already at or above the target.
    //
    // Marked `#[ignore]` because:
    //   1. setpriority is process-wide — running this concurrently with the
    //      rest of the unit-test binary would renice unrelated test threads.
    //   2. Default RLIMIT_NICE on most Linux setups forbids unprivileged
    //      processes from lowering their own nice value, so the elevated nice
    //      leaks into later tests in the same binary.
    // Run manually with: cargo test -p vykar-core --lib -- --ignored apply_process_nice
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "process-wide setpriority; run manually with --ignored"]
    fn apply_process_nice_renices_all_threads() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        // Both targets are nonzero so neither call takes the `target == 0`
        // early return; the second is lower to exercise the raise-only path.
        let hi = 19;
        let lo = 10;

        // Raising nice never needs CAP_SYS_NICE, so this works unprivileged.
        // SAFETY: Errno::clear is a thread-local store; getpriority with
        // PRIO_PROCESS/0 reads the calling thread's nice value with no pointer
        // arguments.
        let start = unsafe {
            Errno::clear();
            nix::libc::getpriority(nix::libc::PRIO_PROCESS, 0)
        };
        if start >= hi {
            // Already at the maximum nice — nothing to test.
            return;
        }

        let n = 3usize;
        let park = Arc::new(Barrier::new(n + 1));
        let release = Arc::new(Barrier::new(n + 1));
        let tids: Arc<std::sync::Mutex<Vec<i32>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..n)
            .map(|_| {
                let park = Arc::clone(&park);
                let release = Arc::clone(&release);
                let tids = Arc::clone(&tids);
                thread::spawn(move || {
                    // SAFETY: SYS_gettid takes no arguments and returns the
                    // calling kernel thread id; always sound on Linux.
                    let tid = unsafe { nix::libc::syscall(nix::libc::SYS_gettid) } as i32;
                    tids.lock().unwrap().push(tid);
                    park.wait();
                    release.wait();
                })
            })
            .collect();

        park.wait(); // all worker threads have registered their TID

        let assert_all_at = |expected: i32, what: &str| {
            for tid in tids.lock().unwrap().iter().copied() {
                Errno::clear();
                // SAFETY: getpriority with PRIO_PROCESS and a TID reads that
                // task's nice value; no pointer arguments.
                let actual = unsafe { nix::libc::getpriority(nix::libc::PRIO_PROCESS, tid as u32) };
                let errno = Errno::last_raw();
                assert!(
                    !(actual == -1 && errno != 0),
                    "getpriority(tid={tid}) failed: errno={errno}"
                );
                assert_eq!(actual, expected, "thread {tid} wrong after {what}");
            }
            // Also verify the calling thread.
            // SAFETY: getpriority with PRIO_PROCESS/0 reads the calling
            // thread's nice value; no pointer arguments.
            let calling = unsafe { nix::libc::getpriority(nix::libc::PRIO_PROCESS, 0) };
            assert_eq!(calling, expected, "calling thread wrong after {what}");
        };

        apply_process_nice(hi).expect("apply hi");
        assert_all_at(hi, "apply");

        // Repeat and lower: both are no-ops that must not fail with EACCES.
        apply_process_nice(hi).expect("re-apply hi");
        assert_all_at(hi, "re-apply");
        apply_process_nice(lo).expect("apply lo");
        assert_all_at(hi, "apply lower");

        release.wait();
        for h in handles {
            h.join().unwrap();
        }
    }
}
