//! Source-path resolution: classify each configured source as directory or
//! file, compute its canonical absolute path, choose the snapshot-root emission
//! policy, and reject unsafe / duplicate basenames. Executed once per backup.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::repo::file_cache::{ParentReusePolicy, ParentReuseRoot};
use vykar_types::error::{Result, VykarError};

/// Classification of a configured source path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceKind {
    Directory,
    File,
}

/// Whether to emit a synthetic root entry for this source and, if so, the
/// basename to use for the snapshot prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RootEmission {
    /// Single-path directory backup — descendants' paths are relative to the
    /// walk root, no root-level `Item` is emitted.
    SkipRoot,
    /// Multi-path directory backup OR any file source. The walker emits the
    /// real root entry first, and descendants (if any) are prefixed with
    /// `basename`.
    EmitRoot { basename: String },
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
}

impl ResolvedSource {
    /// Resolve a single configured source path.
    ///
    /// Stats with [`std::fs::metadata`] (follows symlinks), canonicalizes, and
    /// derives the basename + policy. No cached metadata is stored — the walker
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

        let emit_root = multi_path || kind != SourceKind::Directory;

        let policy = if emit_root {
            let basename = derive_basename(configured_path, &abs_source);
            if basename.is_empty() {
                return Err(VykarError::Config(format!(
                    "source has no safe basename: {configured}"
                )));
            }
            RootEmission::EmitRoot { basename }
        } else {
            RootEmission::SkipRoot
        };

        Ok(Self {
            configured: configured.to_string(),
            abs_source,
            abs_source_str,
            kind,
            policy,
        })
    }

    /// Build the `ParentReuseRoot` corresponding to this source. Keeps the
    /// `RootEmission`→`ParentReusePolicy` mapping in one place so the two
    /// enums can't drift.
    pub fn parent_reuse_root(&self) -> ParentReuseRoot {
        let policy = match &self.policy {
            RootEmission::SkipRoot => ParentReusePolicy::SkipRoot,
            RootEmission::EmitRoot { basename } => ParentReusePolicy::EmitRoot {
                basename: basename.clone(),
            },
        };
        ParentReuseRoot {
            abs_root: self.abs_source_str.clone(),
            policy,
        }
    }

    /// Resolve a batch of configured paths. Rejects duplicate basenames across
    /// `EmitRoot` sources with a single clear error naming both colliding paths.
    pub fn resolve_all(configured: &[String], multi_path: bool) -> Result<Vec<Self>> {
        let mut resolved = Vec::with_capacity(configured.len());
        let mut seen_basenames: HashMap<String, String> = HashMap::new();

        for cfg in configured {
            let source = Self::resolve(cfg, multi_path)?;
            if let RootEmission::EmitRoot { basename } = &source.policy {
                if let Some(prev) = seen_basenames.get(basename) {
                    return Err(VykarError::Config(format!(
                        "duplicate source basename '{basename}' — sources {prev} and {cfg} would collide in the snapshot"
                    )));
                }
                seen_basenames.insert(basename.clone(), cfg.clone());
            }
            resolved.push(source);
        }

        Ok(resolved)
    }
}

/// Prefer the configured spelling for the basename, falling back to the
/// canonical path's last component when the configured path has no safe name
/// (e.g. `.`, `..`, or an empty string).
fn derive_basename(configured: &Path, abs_source: &Path) -> String {
    if let Some(name) = configured.file_name() {
        return name.to_string_lossy().to_string();
    }
    if let Some(name) = abs_source.file_name() {
        return name.to_string_lossy().to_string();
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(path: &Path) -> String {
        path.to_string_lossy().to_string()
    }

    #[test]
    fn resolve_directory_single() {
        let tmp = tempfile::tempdir().unwrap();
        let src = cfg(tmp.path());
        let resolved = ResolvedSource::resolve(&src, false).unwrap();
        assert_eq!(resolved.kind, SourceKind::Directory);
        assert_eq!(resolved.policy, RootEmission::SkipRoot);
    }

    #[test]
    fn resolve_directory_multi() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("data");
        std::fs::create_dir(&dir).unwrap();
        let src = cfg(&dir);
        let resolved = ResolvedSource::resolve(&src, true).unwrap();
        assert_eq!(resolved.kind, SourceKind::Directory);
        match resolved.policy {
            RootEmission::EmitRoot { basename } => assert_eq!(basename, "data"),
            _ => panic!("expected EmitRoot"),
        }
    }

    #[test]
    fn resolve_file() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("hello.txt");
        std::fs::write(&file, b"x").unwrap();
        let resolved = ResolvedSource::resolve(&cfg(&file), false).unwrap();
        assert_eq!(resolved.kind, SourceKind::File);
        match resolved.policy {
            RootEmission::EmitRoot { basename } => assert_eq!(basename, "hello.txt"),
            _ => panic!("expected EmitRoot for file"),
        }
    }

    #[test]
    fn resolve_file_multi() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("notes.md");
        std::fs::write(&file, b"y").unwrap();
        let resolved = ResolvedSource::resolve(&cfg(&file), true).unwrap();
        assert_eq!(resolved.kind, SourceKind::File);
        match resolved.policy {
            RootEmission::EmitRoot { basename } => assert_eq!(basename, "notes.md"),
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
        // Preserves the configured name, not the target name.
        match resolved.policy {
            RootEmission::EmitRoot { basename } => assert_eq!(basename, "docs"),
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

        let resolved = ResolvedSource::resolve(&cfg(&link), false).unwrap();
        assert_eq!(resolved.kind, SourceKind::File);
        match resolved.policy {
            RootEmission::EmitRoot { basename } => assert_eq!(basename, "alias"),
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

    #[test]
    fn resolve_trailing_slash() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("foo");
        std::fs::create_dir(&dir).unwrap();
        let mut s = cfg(&dir);
        s.push('/');
        let resolved = ResolvedSource::resolve(&s, true).unwrap();
        match resolved.policy {
            RootEmission::EmitRoot { basename } => assert_eq!(basename, "foo"),
            _ => panic!("expected EmitRoot"),
        }
    }

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
            RootEmission::EmitRoot { basename } => {
                assert!(!basename.is_empty(), "basename should not be empty");
                assert_ne!(basename, ".");
                assert_ne!(basename, "..");
                assert_ne!(basename, "/");
            }
            _ => panic!("expected EmitRoot"),
        }
    }

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
            RootEmission::EmitRoot { basename } => {
                assert!(!basename.is_empty());
                assert_ne!(basename, "..");
            }
            _ => panic!("expected EmitRoot"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_root_skiproot() {
        // Filesystem root with SkipRoot policy (single-path, directory) is
        // acceptable because the basename is never consulted.
        let resolved = ResolvedSource::resolve("/", false).unwrap();
        assert_eq!(resolved.policy, RootEmission::SkipRoot);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_root_emitroot_rejected() {
        let err = ResolvedSource::resolve("/", true).unwrap_err();
        match err {
            VykarError::Config(msg) => assert!(msg.contains("no safe basename")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn resolve_all_rejects_duplicate_basenames() {
        let tmp1 = tempfile::tempdir().unwrap();
        let tmp2 = tempfile::tempdir().unwrap();
        let a = tmp1.path().join("data");
        let b = tmp2.path().join("data");
        std::fs::create_dir(&a).unwrap();
        std::fs::create_dir(&b).unwrap();
        let err = ResolvedSource::resolve_all(&[cfg(&a), cfg(&b)], true).unwrap_err();
        match err {
            VykarError::Config(msg) => {
                assert!(msg.contains("duplicate source basename"), "got: {msg}");
                assert!(msg.contains("data"), "got: {msg}");
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

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
}
