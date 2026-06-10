//! Source-path resolution: classify each configured source as directory or
//! file, compute its canonical absolute path, choose the snapshot-root emission
//! policy, and reject prefix collisions / nested roots. Executed once per backup.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

use crate::repo::file_cache::{ParentReusePolicy, ParentReuseRoot};
use vykar_types::error::{Result, VykarError};

/// Classification of a configured source path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceKind {
    Directory,
    File,
}

/// Whether to emit a synthetic root entry for this source and, if so, the
/// prefix to use for snapshot paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RootEmission {
    /// Single-path directory backup — descendants' paths are relative to the
    /// walk root, no root-level `Item` is emitted.
    SkipRoot,
    /// Multi-path directory backup OR any file source. The walker emits the
    /// real root entry first, and descendants (if any) are prefixed with
    /// `prefix`. May be multi-component (e.g. `var/lib/machines/base/etc`).
    EmitRoot { prefix: String },
}

/// Resolved source-path state, computed once per backup.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedSource {
    /// Verbatim user-supplied path (used for progress events).
    pub configured: String,
    /// Canonicalized absolute source path. Symlinks at the root are followed.
    pub abs_source: PathBuf,
    /// Cached string form of `abs_source`.
    pub abs_source_str: String,
    pub kind: SourceKind,
    pub policy: RootEmission,
    /// Byte-faithful form of the `EmitRoot` prefix when it is not valid UTF-8
    /// (Unix only). `None` for `SkipRoot`, a UTF-8 prefix, or on non-Unix. The
    /// `RootEmission::EmitRoot.prefix` string stays the lossy display form.
    pub prefix_raw: Option<Vec<u8>>,
}

impl ResolvedSource {
    /// Resolve a single configured source path.
    ///
    /// Stats with [`std::fs::metadata`] (follows symlinks), canonicalizes, and
    /// derives the prefix + policy. No cached metadata is stored — the walker
    /// re-stats at emission time to avoid a resolve→emission TOCTOU window.
    pub fn resolve(configured: &str, multi_path: bool) -> Result<Self> {
        let configured_path = Path::new(configured);
        let meta = std::fs::metadata(configured_path)
            .map_err(|e| VykarError::Config(format!("source does not exist: {configured}: {e}")))?;

        let kind = if meta.is_dir() {
            SourceKind::Directory
        } else if meta.is_file() {
            SourceKind::File
        } else {
            return Err(VykarError::Config(format!(
                "unsupported source type: {configured}"
            )));
        };

        let abs_source = std::fs::canonicalize(configured_path).unwrap_or_else(|_| {
            if configured_path.is_absolute() {
                configured_path.to_path_buf()
            } else {
                std::env::current_dir()
                    .unwrap_or_default()
                    .join(configured_path)
            }
        });
        let abs_source_str = abs_source.to_string_lossy().to_string();

        // For single-path single-source files, keep the legacy basename so
        // `/tmp/hello.txt` continues to land at `hello.txt` (no gratuitous
        // layout change for the common single-file recipe). For everything
        // else that needs an EmitRoot (multi-path, or directory in multi
        // mode), derive the prefix from the absolute configured path so
        // paths with the same basename can coexist.
        let (policy, prefix_raw) = if multi_path {
            let mut prefix = derive_snapshot_prefix(configured_path, &abs_source);
            if prefix.is_empty() {
                return Err(VykarError::Config(format!(
                    "source has no safe prefix: {configured}"
                )));
            }
            let raw = finalize_raw_prefix(
                &mut prefix,
                derive_snapshot_prefix_raw(configured_path, &abs_source),
            );
            (RootEmission::EmitRoot { prefix }, raw)
        } else if kind == SourceKind::File {
            let mut prefix = derive_basename(configured_path, &abs_source);
            if prefix.is_empty() {
                return Err(VykarError::Config(format!(
                    "source has no safe basename: {configured}"
                )));
            }
            let raw = finalize_raw_prefix(
                &mut prefix,
                derive_basename_raw(configured_path, &abs_source),
            );
            (RootEmission::EmitRoot { prefix }, raw)
        } else {
            (RootEmission::SkipRoot, None)
        };

        Ok(Self {
            configured: configured.to_string(),
            abs_source,
            abs_source_str,
            kind,
            policy,
            prefix_raw,
        })
    }

    /// Build the `ParentReuseRoot` corresponding to this source. Keeps the
    /// `RootEmission`→`ParentReusePolicy` mapping in one place so the two
    /// enums can't drift.
    pub fn parent_reuse_root(&self) -> ParentReuseRoot {
        let policy = match &self.policy {
            RootEmission::SkipRoot => ParentReusePolicy::SkipRoot,
            RootEmission::EmitRoot { prefix } => ParentReusePolicy::EmitRoot {
                prefix: prefix.clone(),
            },
        };
        ParentReuseRoot {
            abs_root: self.abs_source_str.clone(),
            policy,
        }
    }

    /// Resolve a batch of configured paths. Rejects two failure modes that
    /// would produce snapshot-path collisions:
    ///
    /// 1. Two `EmitRoot` sources resolving to the same snapshot prefix.
    /// 2. One `EmitRoot` source's canonical root is a strict path-component
    ///    ancestor of another's — every file under the inner root would
    ///    appear twice in the snapshot under different prefixes.
    pub fn resolve_all(configured: &[String], multi_path: bool) -> Result<Vec<Self>> {
        let mut resolved: Vec<Self> = Vec::with_capacity(configured.len());
        // Keyed on the *canonical* prefix bytes (raw when non-UTF8, else the
        // display bytes) so two prefixes that differ only in non-UTF8 bytes are
        // not conflated by their shared lossy display string.
        let mut seen_prefixes: HashMap<Vec<u8>, String> = HashMap::new();

        for cfg in configured {
            let source = Self::resolve(cfg, multi_path)?;
            if let RootEmission::EmitRoot { prefix } = &source.policy {
                let prefix_key = source
                    .prefix_raw
                    .clone()
                    .unwrap_or_else(|| prefix.as_bytes().to_vec());
                if let Some(prev) = seen_prefixes.get(&prefix_key) {
                    return Err(VykarError::Config(format!(
                        "sources {prev} and {cfg} both resolve to snapshot prefix '{prefix}'"
                    )));
                }
                // Reject nested canonical roots in either direction. Today
                // /foo + /foo/bar accidentally avoided collision via differing
                // basenames but already double-stored every file under
                // /foo/bar — the new scheme produces real snapshot-path
                // collisions, so reject explicitly.
                if matches!(source.policy, RootEmission::EmitRoot { .. }) {
                    for other in &resolved {
                        if !matches!(other.policy, RootEmission::EmitRoot { .. }) {
                            continue;
                        }
                        if source.abs_source.strip_prefix(&other.abs_source).is_ok()
                            && source.abs_source != other.abs_source
                        {
                            return Err(VykarError::Config(format!(
                                "source {cfg} is nested under source {} — backing up both would produce duplicate snapshot entries",
                                other.configured
                            )));
                        }
                        if other.abs_source.strip_prefix(&source.abs_source).is_ok()
                            && other.abs_source != source.abs_source
                        {
                            return Err(VykarError::Config(format!(
                                "source {} is nested under source {cfg} — backing up both would produce duplicate snapshot entries",
                                other.configured
                            )));
                        }
                    }
                }
                seen_prefixes.insert(prefix_key, cfg.clone());
            }
            resolved.push(source);
        }

        Ok(resolved)
    }
}

/// Prefer the configured spelling for the basename, falling back to the
/// canonical path's last component when the configured path has no safe name
/// (e.g. `.`, `..`, or an empty string). Explicitly rejects `.` and `..` even
/// if `Path::file_name` returns them — the restore-time sanitizer rejects any
/// snapshot path containing those components, so a basename of `..` must never
/// reach the snapshot.
fn derive_basename(configured: &Path, abs_source: &Path) -> String {
    if let Some(name) = configured.file_name() {
        let s = name.to_string_lossy();
        if is_safe_basename(&s) {
            return s.to_string();
        }
    }
    if let Some(name) = abs_source.file_name() {
        let s = name.to_string_lossy();
        if is_safe_basename(&s) {
            return s.to_string();
        }
    }
    String::new()
}

fn is_safe_basename(s: &str) -> bool {
    !s.is_empty() && s != "." && s != ".."
}

/// Promote a derived raw byte prefix to `Some` only when it is genuinely
/// non-UTF8, and in that case rederive `display` *from* the raw bytes so the
/// [`crate::snapshot::item::Item::validate`] consistency invariant
/// (`from_utf8_lossy(raw) == display`) holds **by construction** — the display
/// string can never drift out of sync with the raw shadow even if the display
/// derivation later evolves without a matching change to the raw twin. Returns
/// `None` for UTF-8 prefixes (the common case) and for empty bytes (non-Unix),
/// leaving `display` untouched.
fn finalize_raw_prefix(display: &mut String, raw: Vec<u8>) -> Option<Vec<u8>> {
    if std::str::from_utf8(&raw).is_ok() {
        return None;
    }
    *display = String::from_utf8_lossy(&raw).into_owned();
    Some(raw)
}

#[cfg(unix)]
fn is_safe_basename_bytes(b: &[u8]) -> bool {
    !b.is_empty() && b != b"." && b != b".."
}

/// Byte-faithful counterpart to [`derive_basename`].
#[cfg(unix)]
fn derive_basename_raw(configured: &Path, abs_source: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    if let Some(name) = configured.file_name() {
        let b = name.as_bytes();
        if is_safe_basename_bytes(b) {
            return b.to_vec();
        }
    }
    if let Some(name) = abs_source.file_name() {
        let b = name.as_bytes();
        if is_safe_basename_bytes(b) {
            return b.to_vec();
        }
    }
    Vec::new()
}

#[cfg(not(unix))]
fn derive_basename_raw(_configured: &Path, _abs_source: &Path) -> Vec<u8> {
    Vec::new()
}

/// Byte-faithful counterpart to [`derive_snapshot_prefix`] (Unix). Mirrors the
/// display derivation exactly so `from_utf8_lossy` of the result equals it:
/// lexically clean the absolutized path, then strip the leading `/` and any
/// trailing `/` at the byte level (both ASCII, so byte- and char-stripping
/// agree).
#[cfg(unix)]
fn derive_snapshot_prefix_raw(configured: &Path, abs_source: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    let abs_clean = match absolutize_and_clean(configured) {
        Some(p) => p,
        None => return derive_basename_raw(configured, abs_source),
    };
    let bytes = abs_clean.as_os_str().as_bytes();
    let bytes = bytes.strip_prefix(b"/").unwrap_or(bytes);
    let trimmed_len = bytes.iter().rposition(|&b| b != b'/').map_or(0, |i| i + 1);
    let trimmed = bytes.get(..trimmed_len).unwrap_or(bytes);
    if trimmed.is_empty() {
        derive_basename_raw(configured, abs_source)
    } else {
        trimmed.to_vec()
    }
}

#[cfg(not(unix))]
fn derive_snapshot_prefix_raw(_configured: &Path, _abs_source: &Path) -> Vec<u8> {
    Vec::new()
}

/// Make a configured path absolute (no canonicalize / no symlink follow) and
/// lexically clean it. Returns None if cleaning would escape the filesystem
/// root (e.g. `/..`) or produce a path with no name components — caller falls
/// back to basename.
fn absolutize_and_clean(p: &Path) -> Option<PathBuf> {
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(p)
    };
    let mut out = PathBuf::new();
    for component in abs.components() {
        match component {
            Component::Prefix(pre) => out.push(pre.as_os_str()),
            Component::RootDir => out.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !pop_normal(&mut out) {
                    return None;
                }
            }
            Component::Normal(part) => out.push(part),
        }
    }
    Some(out)
}

/// Pop the trailing `Normal` component from `out`. Returns false if the
/// trailing component is not a `Normal` (i.e. only root/prefix remain), so the
/// caller can detect an attempt to escape the filesystem root via `..`.
fn pop_normal(out: &mut PathBuf) -> bool {
    let mut components: Vec<Component> = out.components().collect();
    match components.last() {
        Some(Component::Normal(_)) => {
            components.pop();
            let mut rebuilt = PathBuf::new();
            for c in components {
                match c {
                    Component::Prefix(pre) => rebuilt.push(pre.as_os_str()),
                    Component::RootDir => rebuilt.push(c.as_os_str()),
                    Component::Normal(part) => rebuilt.push(part),
                    Component::CurDir | Component::ParentDir => {}
                }
            }
            *out = rebuilt;
            true
        }
        _ => false,
    }
}

#[cfg(unix)]
fn derive_snapshot_prefix(configured: &Path, abs_source: &Path) -> String {
    let abs_clean = match absolutize_and_clean(configured) {
        Some(p) => p,
        None => return derive_basename(configured, abs_source),
    };
    let s = abs_clean.to_string_lossy();
    let trimmed = s.strip_prefix('/').unwrap_or(&s).trim_end_matches('/');
    if trimmed.is_empty() {
        derive_basename(configured, abs_source)
    } else {
        trimmed.to_string()
    }
}

#[cfg(windows)]
fn derive_snapshot_prefix(configured: &Path, abs_source: &Path) -> String {
    let abs_clean = match absolutize_and_clean(configured) {
        Some(p) => p,
        None => return derive_basename(configured, abs_source),
    };
    // dunce::simplified strips the \\?\ verbatim prefix if present.
    let simplified = dunce::simplified(&abs_clean);
    let s = simplified.to_string_lossy().replace('\\', "/");
    // Drive letter: "C:/Users/..." → "C/Users/..."
    // UNC root:    "//server/share/..." → "server/share/..."
    let s = s.replacen(':', "", 1);
    let trimmed = s.trim_start_matches('/').trim_end_matches('/');
    if trimmed.is_empty() {
        derive_basename(configured, abs_source)
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(path: &Path) -> String {
        path.to_string_lossy().to_string()
    }

    /// The expected snapshot prefix for a directory under `tmpdir` whose
    /// configured absolute form ends in `tail` (e.g. `/data`). Equivalent to
    /// the absolute path with leading `/` stripped.
    #[cfg(unix)]
    fn expected_prefix(abs_path: &Path) -> String {
        let s = abs_path.to_string_lossy().to_string();
        s.strip_prefix('/').unwrap_or(&s).to_string()
    }

    #[test]
    fn resolve_directory_single() {
        let tmp = tempfile::tempdir().unwrap();
        let src = cfg(tmp.path());
        let resolved = ResolvedSource::resolve(&src, false).unwrap();
        assert_eq!(resolved.kind, SourceKind::Directory);
        assert_eq!(resolved.policy, RootEmission::SkipRoot);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_directory_multi() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("data");
        std::fs::create_dir(&dir).unwrap();
        let src = cfg(&dir);
        let resolved = ResolvedSource::resolve(&src, true).unwrap();
        assert_eq!(resolved.kind, SourceKind::Directory);
        match resolved.policy {
            RootEmission::EmitRoot { prefix } => {
                assert!(prefix.ends_with("/data"), "got: {prefix}");
                assert_eq!(prefix, expected_prefix(&dir));
            }
            _ => panic!("expected EmitRoot"),
        }
    }

    #[test]
    fn resolve_file() {
        // Single-path single-source file: keeps `derive_basename` so
        // `/tmp/hello.txt` lands at `hello.txt`, not `tmp/hello.txt`.
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("hello.txt");
        std::fs::write(&file, b"x").unwrap();
        let resolved = ResolvedSource::resolve(&cfg(&file), false).unwrap();
        assert_eq!(resolved.kind, SourceKind::File);
        match resolved.policy {
            RootEmission::EmitRoot { prefix } => assert_eq!(prefix, "hello.txt"),
            _ => panic!("expected EmitRoot for file"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_file_multi() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("notes.md");
        std::fs::write(&file, b"y").unwrap();
        let resolved = ResolvedSource::resolve(&cfg(&file), true).unwrap();
        assert_eq!(resolved.kind, SourceKind::File);
        match resolved.policy {
            RootEmission::EmitRoot { prefix } => {
                assert!(prefix.ends_with("/notes.md"), "got: {prefix}");
            }
            _ => panic!("expected EmitRoot for file"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_symlink_to_directory() {
        use std::os::unix::fs as unix_fs;
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real-docs");
        std::fs::create_dir(&real).unwrap();
        let link = tmp.path().join("docs");
        unix_fs::symlink(&real, &link).unwrap();

        let resolved = ResolvedSource::resolve(&cfg(&link), true).unwrap();
        assert_eq!(resolved.kind, SourceKind::Directory);
        assert_eq!(resolved.abs_source, std::fs::canonicalize(&real).unwrap());
        // The prefix is derived from the configured path (not canonicalized),
        // so the configured name `docs` wins over the canonical `real-docs`.
        match resolved.policy {
            RootEmission::EmitRoot { prefix } => {
                assert!(prefix.ends_with("/docs"), "got: {prefix}");
                assert!(!prefix.contains("real-docs"), "got: {prefix}");
            }
            _ => panic!("expected EmitRoot"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_symlink_to_file() {
        use std::os::unix::fs as unix_fs;
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real.txt");
        std::fs::write(&real, b"x").unwrap();
        let link = tmp.path().join("alias");
        unix_fs::symlink(&real, &link).unwrap();

        // Single-source file: still keeps the configured basename.
        let resolved = ResolvedSource::resolve(&cfg(&link), false).unwrap();
        assert_eq!(resolved.kind, SourceKind::File);
        match resolved.policy {
            RootEmission::EmitRoot { prefix } => assert_eq!(prefix, "alias"),
            _ => panic!("expected EmitRoot"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_symlinks_distinct_configured_names_not_duplicated() {
        use std::os::unix::fs as unix_fs;
        let tmp = tempfile::tempdir().unwrap();
        let shared = tmp.path().join("shared");
        std::fs::create_dir(&shared).unwrap();
        let link_a = tmp.path().join("a");
        let link_b = tmp.path().join("b");
        unix_fs::symlink(&shared, &link_a).unwrap();
        unix_fs::symlink(&shared, &link_b).unwrap();

        // Two symlinks pointing at the same target with *distinct* configured
        // names produce distinct prefixes (ending in `/a` and `/b`), so the
        // equal-prefix check doesn't fire.
        let resolved = ResolvedSource::resolve_all(&[cfg(&link_a), cfg(&link_b)], true).unwrap();
        assert_eq!(resolved.len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_broken_symlink() {
        use std::os::unix::fs as unix_fs;
        let tmp = tempfile::tempdir().unwrap();
        let link = tmp.path().join("dangling");
        unix_fs::symlink(tmp.path().join("nonexistent"), &link).unwrap();
        let err = ResolvedSource::resolve(&cfg(&link), false).unwrap_err();
        assert!(matches!(err, VykarError::Config(_)), "got: {err:?}");
    }

    #[test]
    fn resolve_nonexistent() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("not-here");
        let err = ResolvedSource::resolve(&cfg(&missing), false).unwrap_err();
        assert!(matches!(err, VykarError::Config(_)), "got: {err:?}");
    }

    #[cfg(unix)]
    #[test]
    fn resolve_trailing_slash() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("foo");
        std::fs::create_dir(&dir).unwrap();
        let mut s = cfg(&dir);
        s.push('/');
        let resolved = ResolvedSource::resolve(&s, true).unwrap();
        match resolved.policy {
            RootEmission::EmitRoot { prefix } => {
                assert!(prefix.ends_with("/foo"), "got: {prefix}");
                assert!(!prefix.ends_with('/'), "got: {prefix}");
            }
            _ => panic!("expected EmitRoot"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_dot_source() {
        let _lock = crate::testutil::CWD_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let prev = std::env::current_dir().unwrap();
        std::env::set_current_dir(tmp.path()).unwrap();
        let result = ResolvedSource::resolve(".", true);
        std::env::set_current_dir(prev).unwrap();

        let resolved = result.unwrap();
        match resolved.policy {
            RootEmission::EmitRoot { prefix } => {
                assert!(!prefix.is_empty(), "prefix should not be empty");
                assert!(!prefix.contains("/./"), "got: {prefix}");
                assert!(!prefix.contains("/../"), "got: {prefix}");
                assert!(!prefix.starts_with('/'), "got: {prefix}");
            }
            _ => panic!("expected EmitRoot"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_dotdot_source() {
        let _lock = crate::testutil::CWD_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("child");
        std::fs::create_dir(&child).unwrap();
        let prev = std::env::current_dir().unwrap();
        std::env::set_current_dir(&child).unwrap();
        let result = ResolvedSource::resolve("..", true);
        std::env::set_current_dir(prev).unwrap();

        let resolved = result.unwrap();
        match resolved.policy {
            RootEmission::EmitRoot { prefix } => {
                assert!(!prefix.is_empty());
                assert!(!prefix.contains(".."), "got: {prefix}");
            }
            _ => panic!("expected EmitRoot"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_root_skiproot() {
        // Filesystem root with SkipRoot policy (single-path, directory) is
        // acceptable because the prefix is never consulted.
        let resolved = ResolvedSource::resolve("/", false).unwrap();
        assert_eq!(resolved.policy, RootEmission::SkipRoot);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_root_emitroot_rejected() {
        let err = ResolvedSource::resolve("/", true).unwrap_err();
        match err {
            VykarError::Config(msg) => {
                assert!(
                    msg.contains("no safe prefix") || msg.contains("no safe basename"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_all_accepts_duplicate_basenames_distinct_parents() {
        // `/tmp/a/etc` and `/tmp/b/etc` both have basename "etc" but distinct
        // absolute-path prefixes (`tmp/.../a/etc` vs `tmp/.../b/etc`), so the
        // new scheme accepts both. This is the issue #143 scenario.
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a").join("etc");
        let b = tmp.path().join("b").join("etc");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let resolved = ResolvedSource::resolve_all(&[cfg(&a), cfg(&b)], true).unwrap();
        assert_eq!(resolved.len(), 2);
        let prefixes: Vec<String> = resolved
            .iter()
            .filter_map(|s| match &s.policy {
                RootEmission::EmitRoot { prefix } => Some(prefix.clone()),
                _ => None,
            })
            .collect();
        assert_ne!(prefixes[0], prefixes[1]);
        assert!(prefixes[0].ends_with("/etc"));
        assert!(prefixes[1].ends_with("/etc"));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_all_rejects_equal_prefix() {
        // Two configured paths that absolutize to the same string (here,
        // duplicate config entries).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("data");
        std::fs::create_dir(&dir).unwrap();
        let err = ResolvedSource::resolve_all(&[cfg(&dir), cfg(&dir)], true).unwrap_err();
        match err {
            VykarError::Config(msg) => {
                assert!(
                    msg.contains("snapshot prefix") || msg.contains("nested"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_all_rejects_nested_roots() {
        // `/tmp/foo` + `/tmp/foo/bar` — bar's contents would be double-stored.
        let tmp = tempfile::tempdir().unwrap();
        let outer = tmp.path().join("foo");
        let inner = outer.join("bar");
        std::fs::create_dir_all(&inner).unwrap();
        let err = ResolvedSource::resolve_all(&[cfg(&outer), cfg(&inner)], true).unwrap_err();
        match err {
            VykarError::Config(msg) => assert!(msg.contains("nested"), "got: {msg}"),
            other => panic!("expected Config error, got {other:?}"),
        }
        // Symmetric: inner-then-outer also rejected.
        let err = ResolvedSource::resolve_all(&[cfg(&inner), cfg(&outer)], true).unwrap_err();
        match err {
            VykarError::Config(msg) => assert!(msg.contains("nested"), "got: {msg}"),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_all_distinct_basenames_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("data");
        let b = tmp.path().join("logs");
        std::fs::create_dir(&a).unwrap();
        std::fs::create_dir(&b).unwrap();
        let resolved = ResolvedSource::resolve_all(&[cfg(&a), cfg(&b)], true).unwrap();
        assert_eq!(resolved.len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn derive_snapshot_prefix_collapses_dotdot() {
        // Lexical normalization collapses `..` components before they become
        // part of the prefix — required because the restore-time sanitizer
        // rejects snapshot paths containing `..`.
        let tmp = tempfile::tempdir().unwrap();
        let foo = tmp.path().join("foo");
        let bar = tmp.path().join("bar");
        std::fs::create_dir(&foo).unwrap();
        std::fs::create_dir(&bar).unwrap();
        let cfg_path = foo.join("..").join("bar");
        let prefix = derive_snapshot_prefix(&cfg_path, &bar);
        assert!(!prefix.contains(".."), "got: {prefix}");
        assert!(prefix.ends_with("/bar"), "got: {prefix}");
    }

    #[cfg(unix)]
    #[test]
    fn derive_snapshot_prefix_falls_back_on_root_escape() {
        // `/..` would escape the root; fall back to derive_basename, which
        // must never produce `.`, `..`, or any multi-component path —
        // restore-time sanitization rejects all of these.
        let prefix = derive_snapshot_prefix(Path::new("/.."), Path::new("/"));
        assert!(!prefix.contains('/'), "got: {prefix}");
        assert_ne!(prefix, ".", "got: {prefix}");
        assert_ne!(prefix, "..", "got: {prefix}");
        // With the current sanitizing fallback, `/..` produces an empty
        // prefix so resolve() can surface a clean error.
        assert!(prefix.is_empty(), "got: {prefix}");
    }

    #[test]
    fn derive_basename_rejects_dot_and_dotdot() {
        // Defensive guard: even if Path::file_name ever returned `.` or `..`,
        // derive_basename must collapse those to empty so the caller errors
        // out instead of producing an unrestorable snapshot prefix.
        assert_eq!(derive_basename(Path::new(".."), Path::new("..")), "");
        assert_eq!(derive_basename(Path::new("."), Path::new(".")), "");
    }

    #[cfg(unix)]
    #[test]
    fn resolve_dotdot_root_emitroot_rejected() {
        // `/..` in EmitRoot mode must error — the cleaned prefix is empty
        // and the basename fallback also rejects it.
        let err = ResolvedSource::resolve("/..", true).unwrap_err();
        match err {
            VykarError::Config(msg) => {
                assert!(
                    msg.contains("no safe prefix") || msg.contains("no safe basename"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(windows)]
    #[test]
    fn derive_snapshot_prefix_windows_drive_letter() {
        let prefix = derive_snapshot_prefix(
            Path::new(r"C:\Users\me\docs"),
            Path::new(r"C:\Users\me\docs"),
        );
        assert_eq!(prefix, "C/Users/me/docs");
    }

    #[cfg(windows)]
    #[test]
    fn derive_snapshot_prefix_windows_unc() {
        let prefix = derive_snapshot_prefix(
            Path::new(r"\\server\share\dir"),
            Path::new(r"\\server\share\dir"),
        );
        assert_eq!(prefix, "server/share/dir");
    }

    #[cfg(windows)]
    #[test]
    fn derive_snapshot_prefix_windows_verbatim() {
        let prefix = derive_snapshot_prefix(Path::new(r"\\?\C:\foo"), Path::new(r"\\?\C:\foo"));
        assert_eq!(prefix, "C/foo");
    }
}
