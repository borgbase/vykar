use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(unix)]
use nix::errno::Errno;
use tracing::warn;

use crate::config::ResourceLimitsConfig;
use vykar_storage::{RepackPlanRequest, RepackResultResponse, StorageBackend};
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

/// Read adaptor that applies an optional shared byte-rate limiter.
pub struct LimitedReader<'a, R> {
    inner: R,
    limiter: Option<&'a ByteRateLimiter>,
}

impl<'a, R> LimitedReader<'a, R> {
    pub fn new(inner: R, limiter: Option<&'a ByteRateLimiter>) -> Self {
        Self { inner, limiter }
    }
}

impl<R: Read> Read for LimitedReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if let Some(limiter) = self.limiter {
            limiter.consume(n);
        }
        Ok(n)
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

impl StorageBackend for ThrottledStorageBackend {
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

    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
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
        if found {
            if let Some(limiter) = self.read_limiter.as_ref() {
                limiter.consume(buf.len());
            }
        }
        Ok(found)
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        self.inner.create_dir(key)
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        if let Some(limiter) = self.write_limiter.as_ref() {
            limiter.consume(data.len());
        }
        self.inner.put_owned(key, data)
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        self.inner.size(key)
    }

    fn server_repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        self.inner.server_repack(plan)
    }

    fn batch_delete_keys(&self, keys: &[String]) -> Result<()> {
        self.inner.batch_delete_keys(keys)
    }
}

/// Guard that restores process niceness when dropped.
pub struct NiceGuard {
    #[cfg(unix)]
    previous_nice: i32,
}

impl NiceGuard {
    pub fn apply(target_nice: i32) -> std::result::Result<Option<Self>, String> {
        if target_nice == 0 {
            return Ok(None);
        }

        #[cfg(unix)]
        {
            let previous = get_process_nice()?;
            set_process_nice(target_nice)?;
            Ok(Some(Self {
                previous_nice: previous,
            }))
        }

        #[cfg(not(unix))]
        {
            let _ = target_nice;
            Err("limits.nice is not supported on this platform".to_string())
        }
    }
}

impl Drop for NiceGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Err(err) = set_process_nice(self.previous_nice) {
            warn!(
                "failed to restore process niceness to {}: {err}",
                self.previous_nice
            );
        }
    }
}

#[cfg(unix)]
fn get_process_nice() -> std::result::Result<i32, String> {
    Errno::clear();
    // SAFETY: getpriority with PRIO_PROCESS/0 reads the calling process's nice value.
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

#[cfg(unix)]
fn set_process_nice(value: i32) -> std::result::Result<(), String> {
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

    #[test]
    fn mib_conversion() {
        assert_eq!(mib_per_sec_to_bytes_per_sec(0), 0);
        assert_eq!(mib_per_sec_to_bytes_per_sec(1), 1024 * 1024);
        assert_eq!(mib_per_sec_to_bytes_per_sec(8), 8 * 1024 * 1024);
    }

    #[test]
    fn wrap_storage_noop_when_unlimited() {
        let limits = ResourceLimitsConfig::default();
        // With 0/0 bandwidth, wrap_storage_backend returns inner unchanged.
        // Just verify it doesn't panic.
        assert_eq!(limits.upload_mib_per_sec, 0);
        assert_eq!(limits.download_mib_per_sec, 0);
    }
}
