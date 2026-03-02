use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

use vykar_types::error::{Result, VykarError};

// ---------------------------------------------------------------------------
// ByteBudget — semaphore-style memory cap for in-flight pipeline data
// ---------------------------------------------------------------------------

/// Tracks available bytes for in-flight pipeline data.
///
/// Workers call [`acquire`] before buffering file data and the consumer calls
/// [`release`] after committing chunks. This caps the total materialized
/// `ProcessedFile` data to approximately `capacity` bytes.
///
/// **Approximation**: Workers acquire by walk-time `file_size` (from stat
/// metadata). If a file grows between stat and read, actual buffered bytes may
/// slightly exceed the cap. This is a pragmatic trade-off — re-statting at open
/// time is still racy, and chunk-level accounting adds significant complexity
/// for an edge case.
pub(super) struct ByteBudget {
    pub(super) state: Mutex<BudgetState>,
    freed: Condvar,
    peak_acquired: AtomicUsize,
}

pub(super) struct BudgetState {
    pub(super) available: usize,
    pub(super) capacity: usize,
    pub(super) poisoned: bool,
}

impl ByteBudget {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(BudgetState {
                available: capacity,
                capacity,
                poisoned: false,
            }),
            freed: Condvar::new(),
            peak_acquired: AtomicUsize::new(0),
        }
    }

    /// Block until `n` bytes are available, then subtract them.
    ///
    /// If `n > capacity`, it is clamped to `capacity` so a single file larger
    /// than the budget can still proceed (it just acquires the entire budget
    /// and runs alone). Returns `Err` if the budget has been poisoned.
    pub(super) fn acquire(&self, n: usize) -> Result<usize> {
        let mut st = self.state.lock().unwrap();
        let n = n.min(st.capacity);
        loop {
            if st.poisoned {
                return Err(VykarError::Other("pipeline budget poisoned".into()));
            }
            if st.available >= n {
                st.available -= n;
                let acquired = st.capacity - st.available;
                self.peak_acquired.fetch_max(acquired, Ordering::Relaxed);
                return Ok(n);
            }
            st = self.freed.wait(st).unwrap();
        }
    }

    /// Return `n` bytes to the budget and wake any blocked workers.
    pub(super) fn release(&self, n: usize) {
        let mut st = self.state.lock().unwrap();
        st.available = (st.available + n).min(st.capacity);
        self.freed.notify_all();
    }

    /// Poison the budget so all current and future `acquire` calls return `Err`.
    pub(super) fn poison(&self) {
        let mut st = self.state.lock().unwrap();
        st.poisoned = true;
        self.freed.notify_all();
    }

    /// Return the peak number of acquired (in-flight) bytes observed.
    pub(super) fn peak_acquired(&self) -> usize {
        self.peak_acquired.load(Ordering::Relaxed)
    }
}

/// RAII guard that releases budget bytes on drop (worker failure safety).
///
/// Call [`defuse`] to transfer ownership of the acquired bytes to the
/// `ProcessedEntry` — the consumer will then call `release` explicitly.
pub(super) struct BudgetGuard<'a> {
    budget: &'a ByteBudget,
    bytes: usize,
}

impl<'a> BudgetGuard<'a> {
    /// Acquire `n` bytes from the budget, returning a guard that will release
    /// them on drop if not defused.
    #[allow(dead_code)] // Used in tests; production code uses from_pre_acquired.
    pub(super) fn new(budget: &'a ByteBudget, n: usize) -> Result<Self> {
        let acquired = budget.acquire(n)?;
        Ok(Self {
            budget,
            bytes: acquired,
        })
    }

    /// Wrap already-acquired bytes in an RAII guard (no acquire call).
    ///
    /// Used when budget was acquired by the walk thread before dispatch to
    /// workers. The guard ensures bytes are released if the worker `?`-bails.
    pub(super) fn from_pre_acquired(budget: &'a ByteBudget, bytes: usize) -> Self {
        Self { budget, bytes }
    }

    /// Consume the guard without releasing the bytes. Returns the byte count
    /// so the caller can pass it to `ProcessedEntry::ProcessedFile.acquired_bytes`.
    pub(super) fn defuse(self) -> usize {
        let bytes = self.bytes;
        mem::forget(self);
        bytes
    }
}

impl Drop for BudgetGuard<'_> {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget.release(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    #[test]
    fn byte_budget_acquire_release() {
        let budget = ByteBudget::new(1024);
        budget.acquire(512).unwrap();
        budget.acquire(512).unwrap();
        // Budget is exhausted — release and re-acquire.
        budget.release(1024);
        budget.acquire(1024).unwrap();
        budget.release(1024);
    }

    #[test]
    fn byte_budget_blocks_and_unblocks() {
        use std::sync::atomic::AtomicBool;
        let budget = Arc::new(ByteBudget::new(100));
        budget.acquire(100).unwrap();

        let acquired = Arc::new(AtomicBool::new(false));
        let acquired2 = Arc::clone(&acquired);
        let budget2 = Arc::clone(&budget);

        let handle = std::thread::spawn(move || {
            budget2.acquire(50).unwrap();
            acquired2.store(true, Ordering::SeqCst);
            budget2.release(50);
        });

        // Give the thread time to block.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!acquired.load(Ordering::SeqCst), "should be blocked");

        // Release budget — thread should unblock.
        budget.release(100);
        handle.join().unwrap();
        assert!(acquired.load(Ordering::SeqCst), "should have acquired");
    }

    #[test]
    fn byte_budget_oversized_clamps() {
        // Request larger than capacity doesn't deadlock — clamped to capacity.
        let budget = ByteBudget::new(64);
        budget.acquire(128).unwrap();
        budget.release(64); // clamped to 64
    }

    #[test]
    fn byte_budget_poison_unblocks() {
        let budget = Arc::new(ByteBudget::new(100));
        budget.acquire(100).unwrap();

        let budget2 = Arc::clone(&budget);
        let handle = std::thread::spawn(move || {
            let result = budget2.acquire(50);
            assert!(result.is_err(), "should fail after poison");
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        budget.poison();
        handle.join().unwrap();

        // Subsequent acquire also fails.
        assert!(budget.acquire(1).is_err());
    }

    #[test]
    fn byte_budget_concurrent_stress() {
        let budget = Arc::new(ByteBudget::new(1000));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let b = Arc::clone(&budget);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    b.acquire(100).unwrap();
                    // Simulate some work.
                    std::thread::yield_now();
                    b.release(100);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All released — full budget should be available.
        budget.acquire(1000).unwrap();
        budget.release(1000);
    }

    #[test]
    fn byte_budget_enforces_cap() {
        use std::sync::atomic::AtomicUsize;

        let cap = 500usize;
        let budget = Arc::new(ByteBudget::new(cap));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let b = Arc::clone(&budget);
            let inf = Arc::clone(&in_flight);
            let pk = Arc::clone(&peak);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    let chunk = 100;
                    b.acquire(chunk).unwrap();
                    let current = inf.fetch_add(chunk, Ordering::SeqCst) + chunk;
                    pk.fetch_max(current, Ordering::Relaxed);
                    std::thread::yield_now();
                    inf.fetch_sub(chunk, Ordering::SeqCst);
                    b.release(chunk);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert!(
            peak.load(Ordering::SeqCst) <= cap,
            "peak {} exceeded cap {}",
            peak.load(Ordering::SeqCst),
            cap
        );
    }

    #[test]
    fn budget_guard_releases_on_drop() {
        let budget = ByteBudget::new(100);
        {
            let _guard = BudgetGuard::new(&budget, 100).unwrap();
            // Guard drops here — should release.
        }
        // Should succeed because guard released.
        budget.acquire(100).unwrap();
        budget.release(100);
    }

    #[test]
    fn budget_guard_defuse_transfers() {
        let budget = ByteBudget::new(100);
        let bytes = {
            let guard = BudgetGuard::new(&budget, 80).unwrap();
            guard.defuse()
        };
        assert_eq!(bytes, 80);
        // Budget should still be held — only 20 available.
        // Acquire the remaining 20 to prove exactly 80 is still held.
        budget.acquire(20).unwrap();
        // Now manually release both the defused amount and our 20.
        budget.release(80);
        budget.release(20);
        // Full budget available again.
        budget.acquire(100).unwrap();
        budget.release(100);
    }

    #[test]
    fn budget_guard_oversized_request_clamps_to_capacity() {
        let budget = ByteBudget::new(64);
        let bytes = {
            let guard = BudgetGuard::new(&budget, 128).unwrap();
            guard.defuse()
        };
        assert_eq!(bytes, 64, "defused bytes should match acquired budget");

        // Budget is fully held by the defused guard, so no bytes remain.
        budget.release(bytes);
        budget.acquire(64).unwrap();
        budget.release(64);
    }
}
