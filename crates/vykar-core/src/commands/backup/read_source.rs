//! Thin wrapper around `std::fs::File` used by the backup read paths.
//!
//! The wrapper is transparent in production — `BackupSource::read` just
//! delegates to `File::read`. In test builds, it also consults a global
//! hook registered via [`test_hooks::install_hook`] so tests can inject
//! deterministic intra-read mutations of the underlying file, exercising
//! the TOCTOU post-read fstat drift check.
//!
//! The hook is **path-scoped** and **generation-tagged**: it only fires
//! for reads of the specific file the test registered, and only while
//! the originally-matched hook is still installed. If a later test
//! installs a new hook before an earlier test's worker thread releases
//! its `BackupSource`, the stale source's reads are silently ignored.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

/// Wraps an open `File` so reads can be observed by a test hook.
///
/// Production builds compile `read` as a thin passthrough.
pub(crate) struct BackupSource {
    file: File,
    /// Generation of the hook that matched at `open()` time, or `None`
    /// if no hook matched. `fire_after_read` re-checks the currently
    /// installed hook's generation before firing, so a later test
    /// installing a new hook cannot trigger this source's reads.
    #[cfg(test)]
    hook_generation: Option<u64>,
}

impl BackupSource {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        #[cfg(test)]
        let hook_generation = test_hooks::is_instrumented_path(path);
        #[cfg(not(test))]
        let _ = path;
        Ok(Self {
            file,
            #[cfg(test)]
            hook_generation,
        })
    }

    /// Borrow the underlying file for fstat and similar metadata calls.
    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    /// Seek the underlying file (used by segmented large-file reads).
    pub(crate) fn seek_from_start(&mut self, pos: u64) -> io::Result<u64> {
        self.file.seek(SeekFrom::Start(pos))
    }
}

impl Read for BackupSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read(buf)?;
        #[cfg(test)]
        if let Some(gen) = self.hook_generation {
            test_hooks::fire_after_read(gen, n as u64);
        }
        Ok(n)
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    //! Test-only hook fired after every `BackupSource::read` call for the
    //! specific file path registered via [`install_hook`].
    //!
    //! Stored in a process-global `Mutex` so it works across the worker
    //! threads spawned by the pipeline. Path-scoped so concurrent tests
    //! running unrelated backups don't accidentally trigger the hook.
    //!
    //! Each installed hook carries a monotonic **generation**. A
    //! `BackupSource` records the generation it matched at `open()`, and
    //! `fire_after_read` only fires when the currently-installed hook
    //! still bears the same generation. This prevents a stale
    //! `BackupSource` from test A firing test B's hook action when tests
    //! run sequentially in the same process and A's worker thread
    //! outlives the cleanup boundary.

    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, OnceLock};

    type Action = Box<dyn FnMut() + Send>;

    struct Hook {
        generation: u64,
        target: PathBuf,
        threshold: u64,
        bytes_seen: u64,
        triggered: bool,
        action: Action,
    }

    static HOOK_GEN: AtomicU64 = AtomicU64::new(0);

    fn cell() -> &'static Mutex<Option<Hook>> {
        static CELL: OnceLock<Mutex<Option<Hook>>> = OnceLock::new();
        CELL.get_or_init(|| Mutex::new(None))
    }

    /// Register a hook for reads of `target`. The hook fires exactly once,
    /// the first time cumulative bytes read from that path reach `threshold`.
    pub(crate) fn install_hook<P, F>(target: P, threshold: u64, action: F)
    where
        P: Into<PathBuf>,
        F: FnMut() + Send + 'static,
    {
        let generation = HOOK_GEN.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
        *cell().lock().unwrap_or_else(|e| e.into_inner()) = Some(Hook {
            generation,
            target: target.into(),
            threshold,
            bytes_seen: 0,
            triggered: false,
            action: Box::new(action),
        });
    }

    /// Remove any installed hook (always call after a test that installed one).
    pub(crate) fn clear_hook() {
        *cell().lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// Query whether a freshly opened `BackupSource` should report reads
    /// to the installed hook. Returns the matching hook's generation iff
    /// canonicalized paths match. Compared via `canonicalize` so relative
    /// / symlinked variations match.
    pub(crate) fn is_instrumented_path(path: &Path) -> Option<u64> {
        let guard = cell().lock().unwrap_or_else(|e| e.into_inner());
        let hook = guard.as_ref()?;
        // Best-effort canonicalization; fall back to direct compare.
        let a = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let b = std::fs::canonicalize(&hook.target).unwrap_or_else(|_| hook.target.clone());
        if a == b {
            Some(hook.generation)
        } else {
            None
        }
    }

    /// Fire the hook iff the currently-installed hook's generation matches
    /// `source_generation` — the generation captured by the `BackupSource`
    /// at open time. Stale sources whose hook was replaced are no-ops.
    pub(crate) fn fire_after_read(source_generation: u64, n: u64) {
        let mut guard = cell().lock().unwrap_or_else(|e| e.into_inner());
        let Some(hook) = guard.as_mut() else {
            return;
        };
        if hook.generation != source_generation {
            return;
        }
        let before = hook.bytes_seen;
        hook.bytes_seen = hook.bytes_seen.saturating_add(n);
        if !hook.triggered && before < hook.threshold && hook.bytes_seen >= hook.threshold {
            hook.triggered = true;
            (hook.action)();
        }
    }
}
