//! Process-wide glibc tuning, called only from main before starting workers.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

/// Reduce retained allocator memory without changing explicitly configured limits.
#[allow(unsafe_code)]
pub(super) fn configure() {
    let arena_max = std::env::var_os("MALLOC_ARENA_MAX");
    let tunables = std::env::var_os("GLIBC_TUNABLES");
    if has_arena_override(arena_max.as_deref(), tunables.as_deref()) {
        return;
    }

    // The 49 GiB local-backup benchmark used 16% less peak RSS with two
    // arenas and essentially unchanged CPU time. Set the limit directly:
    // setting MALLOC_ARENA_MAX here would be too late for glibc's startup
    // environment processing.
    // SAFETY: this module is compiled only for Linux/glibc, where mallopt
    // accepts M_ARENA_MAX and a positive arena count. main calls this before
    // CLI initialization or application threads, as required by mallopt's
    // MT-Unsafe init/const:mallopt contract. No pointers are passed.
    // Best effort: allocation still works normally if the request is rejected.
    let _ = unsafe { libc::mallopt(libc::M_ARENA_MAX, 2) };
}

fn has_arena_override(arena_max: Option<&OsStr>, tunables: Option<&OsStr>) -> bool {
    arena_max.is_some()
        || tunables.is_some_and(|value| {
            value
                .as_bytes()
                .split(|&byte| byte == b':')
                .any(|entry| entry.starts_with(b"glibc.malloc.arena_max="))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_explicit_arena_limits() {
        // Presence is enough: let glibc interpret empty/invalid values too.
        for value in ["", "0", "8", "invalid"] {
            assert!(has_arena_override(Some(OsStr::new(value)), None));
        }
        for value in [
            "glibc.malloc.arena_max=0",
            "glibc.malloc.arena_max=8:glibc.malloc.trim_threshold=65536",
            "glibc.malloc.trim_threshold=65536:glibc.malloc.arena_max=4",
            "glibc.malloc.arena_max=",
        ] {
            assert!(has_arena_override(None, Some(OsStr::new(value))));
        }
        assert!(has_arena_override(
            None,
            Some(OsStr::from_bytes(b"other=\xff:glibc.malloc.arena_max=4")),
        ));
    }

    #[test]
    fn unrelated_tunables_keep_the_default_limit() {
        assert!(!has_arena_override(None, None));
        for value in [
            "",
            "glibc.malloc.arena_test=8",
            "glibc.malloc.trim_threshold=65536",
            "other.glibc.malloc.arena_max=8",
            "glibc.malloc.arena_max_extra=8",
        ] {
            assert!(!has_arena_override(None, Some(OsStr::new(value))));
        }
    }
}
