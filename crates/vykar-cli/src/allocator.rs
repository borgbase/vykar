//! Process-wide glibc tuning, called only from main before starting workers.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

/// Maximum number of malloc arenas.
const ARENA_MAX: libc::c_int = 2;

/// Pinned mmap threshold, in bytes.
const MMAP_THRESHOLD_BYTES: libc::c_int = 1024 * 1024;

/// Reduce retained allocator memory without changing explicitly configured limits.
pub(super) fn configure() {
    configure_arena_max();
    configure_mmap_threshold();
}

/// Cap the number of per-thread arenas.
///
/// The 49 GiB local-backup benchmark used 16% less peak RSS with two arenas and
/// essentially unchanged CPU time. That measurement predates
/// [`configure_mmap_threshold`], which subsumes most of the benefit; the cap is
/// kept because it still applies when a user pins their own mmap threshold, and
/// because removing it measured as no change either way.
#[allow(unsafe_code)]
fn configure_arena_max() {
    if has_override("MALLOC_ARENA_MAX", b"glibc.malloc.arena_max=") {
        return;
    }

    // Set the limit directly: setting MALLOC_ARENA_MAX here would be too late
    // for glibc's startup environment processing.
    // SAFETY: this module is compiled only for Linux/glibc, where mallopt
    // accepts M_ARENA_MAX and a positive arena count. main calls this before
    // CLI initialization or application threads, as required by mallopt's
    // MT-Unsafe init/const:mallopt contract. No pointers are passed.
    // Best effort: allocation still works normally if the request is rejected.
    let _ = unsafe { libc::mallopt(libc::M_ARENA_MAX, ARENA_MAX) };
}

/// Pin the mmap threshold so glibc stops raising it as the backup runs.
///
/// Allocations at or above the threshold are served by `mmap` and released to
/// the kernel on free. Backup buffers — chunks, compressed blobs, pack buffers
/// — sit above 1 MiB, while the small per-item allocations that dominate the
/// walk stay on the arena path.
///
/// glibc's default threshold is dynamic: freeing an mmap'd block raises it to
/// that block's size, up to 32 MiB, and raises the trim threshold with it. A
/// backup frees a steady stream of multi-MiB buffers, so the threshold reaches
/// its ceiling early in the run. From then on those buffers are cut from an
/// arena that is never trimmed back — memory the process holds but does not
/// reuse. Pinning the threshold also switches the dynamic adjustment off
/// (glibc's `no_dyn_threshold`), which is the other half of the fix: the trim
/// threshold then stays at its 128 KiB default instead of ratcheting up too.
///
/// On the 49 GiB local-backup benchmark this cut mean peak RSS by 34%, from
/// 500 MiB to 329 MiB, for about 5% more CPU time and 3-5% more wall time. It
/// also made peak RSS far more repeatable: the sample standard deviation across
/// runs fell from tens of MiB to single digits.
#[allow(unsafe_code)]
fn configure_mmap_threshold() {
    // glibc spells the environment variable with a trailing underscore.
    if has_override("MALLOC_MMAP_THRESHOLD_", b"glibc.malloc.mmap_threshold=") {
        return;
    }

    // SAFETY: as in `configure_arena_max` — Linux/glibc only, called from main
    // before any application thread starts, and M_MMAP_THRESHOLD accepts a
    // positive byte count. No pointers are passed, and a rejected request just
    // leaves glibc's dynamic default in place.
    let _ = unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD_BYTES) };
}

/// Whether the user configured this knob, via either the `MALLOC_*` environment
/// variable or the matching `glibc.malloc.*` entry in `GLIBC_TUNABLES`.
fn has_override(env_var: &str, tunable_prefix: &[u8]) -> bool {
    has_override_in(
        std::env::var_os(env_var).as_deref(),
        std::env::var_os("GLIBC_TUNABLES").as_deref(),
        tunable_prefix,
    )
}

fn has_override_in(
    env_value: Option<&OsStr>,
    tunables: Option<&OsStr>,
    tunable_prefix: &[u8],
) -> bool {
    env_value.is_some()
        || tunables.is_some_and(|value| {
            value
                .as_bytes()
                .split(|&byte| byte == b':')
                .any(|entry| entry.starts_with(tunable_prefix))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARENA: &[u8] = b"glibc.malloc.arena_max=";
    const MMAP: &[u8] = b"glibc.malloc.mmap_threshold=";

    #[test]
    fn preserves_explicit_arena_limits() {
        // Presence is enough: let glibc interpret empty/invalid values too.
        for value in ["", "0", "8", "invalid"] {
            assert!(has_override_in(Some(OsStr::new(value)), None, ARENA));
        }
        for value in [
            "glibc.malloc.arena_max=0",
            "glibc.malloc.arena_max=8:glibc.malloc.trim_threshold=65536",
            "glibc.malloc.trim_threshold=65536:glibc.malloc.arena_max=4",
            "glibc.malloc.arena_max=",
        ] {
            assert!(has_override_in(None, Some(OsStr::new(value)), ARENA));
        }
        assert!(has_override_in(
            None,
            Some(OsStr::from_bytes(b"other=\xff:glibc.malloc.arena_max=4")),
            ARENA,
        ));
    }

    #[test]
    fn unrelated_tunables_keep_the_default_limit() {
        assert!(!has_override_in(None, None, ARENA));
        for value in [
            "",
            "glibc.malloc.arena_test=8",
            "glibc.malloc.trim_threshold=65536",
            "other.glibc.malloc.arena_max=8",
            "glibc.malloc.arena_max_extra=8",
        ] {
            assert!(!has_override_in(None, Some(OsStr::new(value)), ARENA));
        }
    }

    #[test]
    fn preserves_explicit_mmap_thresholds() {
        for value in ["", "0", "131072", "invalid"] {
            assert!(has_override_in(Some(OsStr::new(value)), None, MMAP));
        }
        for value in [
            "glibc.malloc.mmap_threshold=0",
            "glibc.malloc.mmap_threshold=131072:glibc.malloc.arena_max=4",
            "glibc.malloc.arena_max=4:glibc.malloc.mmap_threshold=131072",
            "glibc.malloc.mmap_threshold=",
        ] {
            assert!(has_override_in(None, Some(OsStr::new(value)), MMAP));
        }
        assert!(has_override_in(
            None,
            Some(OsStr::from_bytes(
                b"other=\xff:glibc.malloc.mmap_threshold=131072"
            )),
            MMAP,
        ));
    }

    #[test]
    fn unrelated_tunables_keep_the_pinned_threshold() {
        assert!(!has_override_in(None, None, MMAP));
        for value in [
            "",
            "glibc.malloc.arena_max=8",
            "glibc.malloc.trim_threshold=65536",
            "other.glibc.malloc.mmap_threshold=131072",
            "glibc.malloc.mmap_threshold_extra=131072",
        ] {
            assert!(!has_override_in(None, Some(OsStr::new(value)), MMAP));
        }
    }

    /// The two knobs are independent: overriding one must not suppress the other.
    #[test]
    fn each_knob_reads_only_its_own_override() {
        let arena_only = Some(OsStr::new("glibc.malloc.arena_max=4"));
        assert!(has_override_in(None, arena_only, ARENA));
        assert!(!has_override_in(None, arena_only, MMAP));

        let mmap_only = Some(OsStr::new("glibc.malloc.mmap_threshold=131072"));
        assert!(has_override_in(None, mmap_only, MMAP));
        assert!(!has_override_in(None, mmap_only, ARENA));
    }
}
