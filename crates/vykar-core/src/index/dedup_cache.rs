//! Housekeeping of the per-repository cache dir: unique temp files beside a
//! cache target, the sweep of stale temps, the deletion of the fixed-name
//! caches that earlier binaries kept, and the xor-filter helpers of the dedup
//! prefilter. The module name is historical.

use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;
use tracing::{debug, info, warn};
use xorf::Xor8;

use crate::cache_paths::repo_cache_dir;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

/// Fixed names of the local index caches that binaries before the
/// state-named chunk table kept in the per-repo cache dir. Nothing reads
/// them any more; they are deleted at open (see
/// [`sweep_stale_cache_temps`]).
pub(crate) const DEDUP_CACHE_FILE: &str = "dedup_cache";
pub(crate) const RESTORE_CACHE_FILE: &str = "restore_cache";
pub(crate) const FULL_INDEX_CACHE_FILE: &str = "full_index_cache";
/// The raw index blob copy vykar 0.24 kept; only the upgrade removes it.
pub(crate) const INDEX_BLOB_CACHE_FILE: &str = "index_blob";

/// Legacy cache files deleted at open whatever their age.
const LEGACY_CACHE_FILES: [&str; 3] = [DEDUP_CACHE_FILE, RESTORE_CACHE_FILE, FULL_INDEX_CACHE_FILE];

/// Targets `temp_file_beside` writes beside in the per-repo cache dir (the
/// legacy names too, for temps an older binary left behind).
const CACHE_FILE_NAMES: [&str; 7] = [
    DEDUP_CACHE_FILE,
    RESTORE_CACHE_FILE,
    FULL_INDEX_CACHE_FILE,
    INDEX_BLOB_CACHE_FILE,
    crate::index::table::TABLE_TEMP_TARGET,
    crate::index::table::BASE_TEMP_TARGET,
    crate::index::table::SEGMENT_TEMP_TARGET,
];

/// Fixed temp names that binaries up to v0.24.0 wrote beside the caches
/// (`with_extension("tmp")`, and the fast path's `full_index_cache.merged`).
const LEGACY_TEMP_NAMES: [&str; 5] = [
    "dedup_cache.tmp",
    "restore_cache.tmp",
    "full_index_cache.tmp",
    "index_blob.tmp",
    "full_index_cache.merged",
];

const TEMP_SUFFIX: &str = ".tmp";

fn temp_prefix(target_name: &str) -> String {
    format!(".{target_name}.")
}

/// A uniquely named temp file in `target`'s directory, to be persisted over
/// `target`. Unique names let concurrent writers of the same cache (two
/// processes catching up one cache directory) never share a temp file; the
/// last `persist` wins and every mapping keeps its own inode.
pub(crate) fn temp_file_beside(target: &Path) -> Result<NamedTempFile> {
    let dir = match target.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let prefix = temp_prefix(
        target
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("cache"),
    );
    Ok(tempfile::Builder::new()
        .prefix(&prefix)
        .suffix(TEMP_SUFFIX)
        .tempfile_in(dir)?)
}

// ---------------------------------------------------------------------------
// Sweep of leftover temp files
// ---------------------------------------------------------------------------

/// How old (by mtime) a leftover cache temp file must be before it is deleted.
///
/// The gate is a heuristic, not ownership: a file is swept only when it has
/// not been written for 24 h. Every writer advances the mtime while it writes,
/// and the longest wait between finishing a temp file and renaming it (one
/// pack verification or one base upload) is far shorter. A process suspended
/// longer than that in between (a laptop asleep during a base upload, a
/// wall-clock jump) loses the file: the operation fails or continues without
/// the local cache, and the next run recovers. Nothing is corrupted, because
/// no cache file itself is ever unlinked or truncated.
pub(crate) const STALE_TEMP_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// Minimum time between two sweeps of the system temp dir in one process.
const TEMP_DIR_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// What a sweep removed.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TempSweep {
    pub files: u64,
    pub bytes: u64,
}

impl TempSweep {
    fn add(&mut self, other: TempSweep) {
        self.files += other.files;
        self.bytes += other.bytes;
    }
}

/// Whether `name` is a temp file `temp_file_beside` creates beside one of
/// `targets`: `.<target>.<alphanumerics>.tmp`.
fn is_cache_temp_name(name: &str, targets: &[&str]) -> bool {
    targets.iter().any(|t| {
        name.strip_prefix(temp_prefix(t).as_str())
            .and_then(|rest| rest.strip_suffix(TEMP_SUFFIX))
            .is_some_and(|mid| !mid.is_empty() && mid.chars().all(|c| c.is_ascii_alphanumeric()))
    })
}

/// Delete, in `dir` only (no recursion), the regular files whose name is a
/// temp name of one of `targets` or one of `legacy`, and whose mtime is more
/// than `max_age` old. Never fails: every error is logged and skipped.
pub(crate) fn sweep_stale_temps_in(
    dir: &Path,
    targets: &[&str],
    legacy: &[&str],
    max_age: std::time::Duration,
) -> TempSweep {
    let mut swept = TempSweep::default();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            debug!(dir = %dir.display(), error = %e, "cache temp sweep: cannot list dir");
            return swept;
        }
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if !is_cache_temp_name(name, targets) && !legacy.contains(&name) {
            continue;
        }
        let path = entry.path();
        // By path, not `entry.metadata()`: on Windows that returns the
        // directory listing's times, which can lag behind a live writer.
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.file_type().is_file() {
            continue;
        }
        // A future mtime (clock skew) makes `elapsed` fail: not stale.
        let stale = meta
            .modified()
            .ok()
            .and_then(|m| m.elapsed().ok())
            .is_some_and(|age| age > max_age);
        if !stale {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                swept.files += 1;
                swept.bytes += meta.len();
                debug!(path = %path.display(), bytes = meta.len(), "removed stale cache temp file");
            }
            // Another process swept or persisted it first.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                debug!(path = %path.display(), error = %e, "failed to remove stale cache temp file");
            }
        }
    }
    swept
}

/// Delete the legacy fixed-name caches in `dir` (see [`LEGACY_CACHE_FILES`]).
fn delete_legacy_caches_in(dir: &Path) -> TempSweep {
    let mut swept = TempSweep::default();
    for name in LEGACY_CACHE_FILES {
        let path = dir.join(name);
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.file_type().is_file() {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                swept.files += 1;
                swept.bytes += meta.len();
                debug!(path = %path.display(), "removed a legacy local index cache");
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => debug!(path = %path.display(), error = %e, "failed to remove a legacy cache"),
        }
    }
    swept
}

/// Sweep the leftovers of interrupted local cache writes in a repository's
/// cache dir and, when given, the system temp dir, and delete the legacy
/// fixed-name caches of the cache dir. The temp dir holds the temp-anchor
/// files of every sync that bypasses the local cache (`check`,
/// `check --repair`, upgrade resume) and of runs without a cache dir.
fn sweep_stale_cache_temps_in(repo_cache_dir: Option<&Path>, temp_dir: Option<&Path>) -> TempSweep {
    let mut swept = TempSweep::default();
    if let Some(dir) = repo_cache_dir {
        swept.add(sweep_stale_temps_in(
            dir,
            &CACHE_FILE_NAMES,
            &LEGACY_TEMP_NAMES,
            STALE_TEMP_AGE,
        ));
        swept.add(delete_legacy_caches_in(dir));
    }
    if let Some(dir) = temp_dir {
        swept.add(sweep_stale_temps_in(
            dir,
            &[crate::cache_paths::TEMP_ANCHOR_FILE],
            &[],
            STALE_TEMP_AGE,
        ));
    }
    if swept.files > 0 {
        info!(
            files = swept.files,
            bytes = swept.bytes,
            "removed stale local cache temp files"
        );
    }
    swept
}

/// Whether a temp-dir sweep is due at `now`, given the time of the last one;
/// records `now` as the last sweep when it is.
fn temp_dir_sweep_due(
    last: &mut Option<std::time::Instant>,
    now: std::time::Instant,
    interval: std::time::Duration,
) -> bool {
    let due = last.is_none_or(|prev| now.saturating_duration_since(prev) >= interval);
    if due {
        *last = Some(now);
    }
    due
}

/// Sweep the leftovers of interrupted local cache writes: the repository's
/// cache dir on every call, and the system temp dir at most once per
/// `TEMP_DIR_SWEEP_INTERVAL` per process.
pub(crate) fn sweep_stale_cache_temps(repo_id: &[u8], cache_dir: Option<&Path>) -> TempSweep {
    static LAST_TEMP_DIR_SWEEP: std::sync::Mutex<Option<std::time::Instant>> =
        std::sync::Mutex::new(None);
    let temp_dir_due = {
        let mut last = LAST_TEMP_DIR_SWEEP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        temp_dir_sweep_due(
            &mut last,
            std::time::Instant::now(),
            TEMP_DIR_SWEEP_INTERVAL,
        )
    };
    // Test binaries never list or delete in the real system temp dir.
    let temp_dir = temp_dir_due
        .then(std::env::temp_dir)
        .filter(|_| !cfg!(test));
    sweep_stale_cache_temps_in(
        repo_cache_dir(repo_id, cache_dir).as_deref(),
        temp_dir.as_deref(),
    )
}

// ---------------------------------------------------------------------------
// Xor filter helpers
// ---------------------------------------------------------------------------

/// Extract the first 8 bytes of a ChunkId as a little-endian u64.
/// Keyed-hash output has excellent entropy, so this is a high-quality hash key.
pub(crate) fn chunk_id_to_u64(id: &ChunkId) -> u64 {
    u64::from_le_bytes(id.as_bytes()[..8].try_into().expect("ChunkId is 32 bytes"))
}

/// Build an Xor8 filter from pre-computed u64 keys.
/// Returns `None` if the slice is empty or construction fails.
///
/// The `catch_unwind` is still warranted after xorf 0.13. `Xor8::from` keeps a
/// `debug_assert!` that every key is distinct, and these keys are 64-bit
/// truncations of chunk IDs, so two distinct chunks can collide. 0.13's
/// construction fix was to BinaryFuse segment-length boundaries, which this
/// code does not use. It only catches in debug builds — the release profile
/// sets `panic = "abort"` and compiles the assertion out anyway — but a
/// missing filter is harmless either way: it is a probabilistic pre-filter, so
/// falling back to `None` re-checks chunks against the index rather than
/// losing data.
pub(crate) fn build_xor_filter_from_keys(keys: &[u64]) -> Option<Xor8> {
    if keys.is_empty() {
        return None;
    }
    match std::panic::catch_unwind(|| Xor8::from(keys)) {
        Ok(filter) => {
            debug!(entries = keys.len(), "built xor filter");
            Some(filter)
        }
        Err(_) => {
            warn!("xor filter construction panicked");
            None
        }
    }
}

/// The local filesystem path of the raw index blob copy that vykar 0.24 kept
/// for its conditional GET. Only the upgrade removes it.
pub fn index_blob_cache_path(repo_id: &[u8], cache_dir: Option<&Path>) -> Option<PathBuf> {
    repo_cache_dir(repo_id, cache_dir).map(|d| d.join(INDEX_BLOB_CACHE_FILE))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_id_to_u64_extracts_first_8_bytes() {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&42u64.to_le_bytes());
        let id = ChunkId::from_bytes(bytes);
        assert_eq!(chunk_id_to_u64(&id), 42);
    }

    #[test]
    fn index_blob_cache_path_returns_some() {
        let repo_id = [0xEEu8; 32];
        let path = index_blob_cache_path(&repo_id, None);
        assert!(path.is_some());
        let p = path.unwrap();
        assert!(p.to_string_lossy().contains("index_blob"));
        assert!(p.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    fn write_aged(path: &Path, data: &[u8], age: std::time::Duration) {
        use std::io::Write;
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(data).unwrap();
        f.set_modified(std::time::SystemTime::now() - age).unwrap();
    }

    fn write_fresh(path: &Path) {
        std::fs::write(path, b"fresh").unwrap();
    }

    const OLD: std::time::Duration = std::time::Duration::from_secs(25 * 3600);

    const SWEPT_TEMPS: [&str; 5] = [
        ".full_index_cache.AbC123.tmp",
        ".dedup_cache.xYz789.tmp",
        ".restore_cache.q1w2e3.tmp",
        ".index_blob.Zz9Zz9.tmp",
        ".chunk_table.Ab12Cd.tmp",
    ];

    fn sweep_cache_dir(dir: &Path) -> TempSweep {
        sweep_stale_temps_in(dir, &CACHE_FILE_NAMES, &LEGACY_TEMP_NAMES, STALE_TEMP_AGE)
    }

    #[test]
    fn sweep_removes_stale_temp_files_and_legacy_names() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = 0u64;
        for (i, name) in SWEPT_TEMPS.iter().chain(&LEGACY_TEMP_NAMES).enumerate() {
            let data = vec![7u8; 10 + i];
            bytes += data.len() as u64;
            write_aged(&dir.path().join(name), &data, OLD);
        }
        let swept = sweep_cache_dir(dir.path());

        assert_eq!(swept, TempSweep { files: 10, bytes });
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn sweep_keeps_fresh_and_future_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        for name in SWEPT_TEMPS.iter().chain(&LEGACY_TEMP_NAMES) {
            write_fresh(&dir.path().join(name));
        }
        let future = dir.path().join(".full_index_cache.Futur1.tmp");
        let f = std::fs::File::create(&future).unwrap();
        f.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(48 * 3600))
            .unwrap();
        drop(f);

        assert_eq!(sweep_cache_dir(dir.path()), TempSweep::default());
        for name in SWEPT_TEMPS.iter().chain(&LEGACY_TEMP_NAMES) {
            assert!(dir.path().join(name).exists(), "{name} was swept");
        }
        assert!(future.exists());
    }

    #[test]
    fn sweep_never_touches_caches_or_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        let table = format!("chunk_table.{}", "ab".repeat(32));
        let names = [
            "full_index_cache",
            "dedup_cache",
            "restore_cache",
            "index_blob",
            "filecache",
            "snapshot_list",
            table.as_str(),
            ".tmpAbC123",
            ".full_index_cache.tmp",
            ".full_index_cache.a.b.tmp",
            ".full_index_cache..tmp",
            ".chunk_table.tmp",
            ".other_cache.AbC123.tmp",
            "foo.tmp",
            ".cache.AbC123.tmp",
        ];
        for name in names {
            write_aged(&dir.path().join(name), b"keep", OLD);
        }
        let subdir = dir.path().join(".full_index_cache.Dir123.tmp");
        std::fs::create_dir(&subdir).unwrap();
        std::fs::File::open(&subdir)
            .and_then(|f| f.set_modified(std::time::SystemTime::now() - OLD))
            .ok();

        assert_eq!(sweep_cache_dir(dir.path()), TempSweep::default());
        for name in names {
            assert!(dir.path().join(name).exists(), "{name} was swept");
        }
        assert!(subdir.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn sweep_does_not_follow_symlinks() {
        let outside = tempfile::tempdir().unwrap();
        let target = outside.path().join("target");
        write_aged(&target, b"target", OLD);
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join(".full_index_cache.Lnk123.tmp");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        assert_eq!(sweep_cache_dir(dir.path()), TempSweep::default());
        assert!(std::fs::symlink_metadata(&link).is_ok());
        assert!(target.exists());
    }

    #[test]
    fn sweep_of_a_missing_dir_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            sweep_cache_dir(&dir.path().join("absent")),
            TempSweep::default()
        );
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::print_stderr, reason = "reports a skipped test")]
    fn sweep_survives_a_failed_removal() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).unwrap();
        let stale = locked.join(".full_index_cache.AbC123.tmp");
        write_aged(&stale, b"stuck", OLD);
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();

        // Root bypasses directory mode bits; probe instead of checking the uid.
        if std::fs::File::create(locked.join("probe")).is_ok() {
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
            eprintln!("skipping: directory permissions are not enforced for this user");
            return;
        }

        let swept = sweep_cache_dir(&locked);

        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(swept.files, 0);
        assert!(stale.exists());
    }

    /// The open-time sweep deletes the legacy fixed-name caches at any age,
    /// and never a published chunk table or the upgrade's index blob.
    #[test]
    fn open_sweep_deletes_legacy_caches_but_not_tables() {
        let dir = tempfile::tempdir().unwrap();
        for name in LEGACY_CACHE_FILES {
            write_fresh(&dir.path().join(name));
        }
        let table = dir.path().join(format!("chunk_table.{}", "cd".repeat(32)));
        write_aged(&table, b"table", OLD);
        write_aged(&dir.path().join(INDEX_BLOB_CACHE_FILE), b"blob", OLD);

        let swept = sweep_stale_cache_temps_in(Some(dir.path()), None);

        assert_eq!(swept.files, 3);
        for name in LEGACY_CACHE_FILES {
            assert!(!dir.path().join(name).exists(), "{name} was kept");
        }
        assert!(table.exists());
        assert!(dir.path().join(INDEX_BLOB_CACHE_FILE).exists());
    }

    #[test]
    fn sweep_of_the_temp_dir_matches_only_the_anchor() {
        let cache = tempfile::tempdir().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let anchor = crate::cache_paths::TEMP_ANCHOR_FILE;
        let stale = tmp.path().join(format!(".{anchor}.AbC123.tmp"));
        let fresh = tmp.path().join(format!(".{anchor}.Fresh1.tmp"));
        let other = tmp.path().join(format!(".{anchor}-other.AbC123.tmp"));
        let cache_temp = tmp.path().join(".full_index_cache.AbC123.tmp");
        write_aged(&stale, b"stale", OLD);
        write_fresh(&fresh);
        write_aged(&other, b"other", OLD);
        write_aged(&cache_temp, b"cache", OLD);

        let swept = sweep_stale_cache_temps_in(Some(cache.path()), Some(tmp.path()));

        assert_eq!(swept, TempSweep { files: 1, bytes: 5 });
        assert!(!stale.exists());
        assert!(fresh.exists());
        assert!(other.exists());
        assert!(
            cache_temp.exists(),
            "cache-dir names are not swept in the temp dir"
        );
    }

    #[test]
    fn temp_dir_sweep_is_rate_limited() {
        let interval = std::time::Duration::from_secs(3600);
        let t0 = std::time::Instant::now();
        let mut last = None;
        assert!(temp_dir_sweep_due(&mut last, t0, interval));
        assert_eq!(last, Some(t0));
        let soon = t0 + std::time::Duration::from_secs(3599);
        assert!(!temp_dir_sweep_due(&mut last, soon, interval));
        assert_eq!(last, Some(t0), "a skipped sweep does not reset the clock");
        let later = t0 + interval;
        assert!(temp_dir_sweep_due(&mut last, later, interval));
        assert_eq!(last, Some(later));
    }

    #[test]
    fn temp_file_beside_names_match_the_sweep() {
        let dir = tempfile::tempdir().unwrap();
        let all: Vec<&str> = CACHE_FILE_NAMES
            .iter()
            .copied()
            .chain([crate::cache_paths::TEMP_ANCHOR_FILE])
            .collect();
        for name in &all {
            let tmp = temp_file_beside(&dir.path().join(name)).unwrap();
            let file_name = tmp.path().file_name().unwrap().to_str().unwrap().to_owned();
            assert!(is_cache_temp_name(&file_name, &[name]), "{file_name}");
            for other in all.iter().filter(|o| *o != name) {
                assert!(
                    !is_cache_temp_name(&file_name, &[other]),
                    "{file_name} matches {other}"
                );
            }
        }
    }

    #[test]
    fn sweep_keeps_a_fresh_writer_temp_that_still_persists() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join(crate::index::table::TABLE_TEMP_TARGET);
        let mut tmp = temp_file_beside(&target).unwrap();
        tmp.write_all(b"table bytes").unwrap();

        assert_eq!(sweep_cache_dir(dir.path()), TempSweep::default());

        tmp.persist(&target).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"table bytes");
    }
}
