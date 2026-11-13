use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use vykar_types::error::{Result, VykarError};

use crate::{ListedObject, StorageBackend};

/// Storage backend for local filesystem using `std::fs` directly.
pub struct LocalBackend {
    root: PathBuf,
}

impl LocalBackend {
    /// Create a backend rooted at the given directory path.
    ///
    /// # Errors
    ///
    /// Returns an error if an existing root path cannot be canonicalized.
    pub fn new(root: &str) -> Result<Self> {
        let root_path = PathBuf::from(root);
        // Canonicalize if the path already exists for clearer errors and
        // correct strip_prefix behavior with symlinked roots.
        let root = if root_path.exists() {
            fs::canonicalize(&root_path)?
        } else {
            root_path
        };
        Ok(Self { root })
    }

    /// Reject storage keys that could escape the repository root.
    fn validate_key(key: &str) -> Result<()> {
        if key.is_empty() {
            return Err(VykarError::InvalidFormat(
                "unsafe storage key: empty".into(),
            ));
        }
        if key.starts_with('/') || key.starts_with('\\') {
            return Err(VykarError::InvalidFormat(format!(
                "unsafe storage key: absolute path '{key}'"
            )));
        }
        if key.contains('\\') {
            return Err(VykarError::InvalidFormat(format!(
                "unsafe storage key: contains backslash '{key}'"
            )));
        }
        let path = std::path::Path::new(key);
        for component in path.components() {
            if component == Component::ParentDir {
                return Err(VykarError::InvalidFormat(format!(
                    "unsafe storage key: parent traversal '{key}'"
                )));
            }
        }
        Ok(())
    }

    /// Resolve a `/`-separated storage key to a filesystem path under the root.
    fn resolve(&self, key: &str) -> Result<PathBuf> {
        Self::validate_key(key)?;
        Ok(self.root.join(key))
    }

    /// Write data to a temp file in the same directory, then atomically rename
    /// into place. This ensures readers never see a partial/corrupt file.
    fn atomic_write(&self, path: &Path, data: &[u8]) -> Result<()> {
        let dir = path.parent().unwrap_or(&self.root);
        // `.tmp.` prefix so SIGKILL debris is recognised by
        // `vykar_protocol::is_temp_file` (and thus cleaned by `delete-repo`).
        let mut tmp = tempfile::Builder::new()
            .prefix(vykar_protocol::TEMP_FILE_PREFIX)
            .rand_bytes(16)
            .tempfile_in(dir)?;
        tmp.write_all(data)?;
        vykar_common::fs::fsync_file(tmp.as_file())?;
        let persisted = tmp.persist(path).map_err(|e| e.error)?;
        drop(persisted);
        Self::sync_directory(dir)?;
        Ok(())
    }

    fn sync_directory(dir: &Path) -> Result<()> {
        vykar_common::fs::fsync_dir(dir)?;
        Ok(())
    }

    fn delete_path_with_sync(
        path: &Path,
        sync_directory: impl FnOnce(&Path) -> Result<()>,
    ) -> Result<()> {
        match fs::remove_file(path) {
            Ok(()) => {
                let dir = path.parent().unwrap_or(Path::new("."));
                sync_directory(dir)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

impl StorageBackend for LocalBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.resolve(key)?;
        match fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.resolve(key)?;
        match self.atomic_write(&path, data) {
            Err(VykarError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                self.atomic_write(&path, data)
            }
            other => other,
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.resolve(key)?;
        Self::delete_path_with_sync(&path, Self::sync_directory)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let path = self.resolve(key)?;
        match fs::metadata(&path) {
            Ok(meta) => Ok(meta.is_file()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        let path = self.resolve(key)?;
        match fs::metadata(&path) {
            Ok(meta) if meta.is_file() => Ok(Some(meta.len())),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .list_entries(prefix, false)?
            .into_iter()
            .map(|o| o.key)
            .collect())
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        self.list_entries(prefix, true)
    }

    fn remove_stale_temp_files(&self, older_than: std::time::Duration) -> Result<usize> {
        if !self.root.is_dir() {
            return Ok(0);
        }
        // On a network mount the mtime comes from the file server, so "now"
        // must too.
        let now = if older_than.is_zero() {
            None
        } else {
            match self.storage_now()? {
                Some(now) => Some(now),
                None => {
                    tracing::debug!("storage reported no mtime; skipping temp sweep");
                    return Ok(0);
                }
            }
        };
        let mut removed = 0;
        self.walk_repo_files(&mut |entry, key| {
            if !vykar_protocol::is_repo_temp_file(key) {
                return Ok(());
            }
            if let Some(now) = now {
                // A concurrent `put` may rename its temp file away mid-walk.
                let modified = match entry.metadata().and_then(|m| m.modified()) {
                    Ok(m) => m,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(e) => {
                        tracing::warn!(key, "cannot stat temp file, skipping: {e}");
                        return Ok(());
                    }
                };
                if !crate::is_stale(now, modified, older_than) {
                    return Ok(());
                }
            }
            // One undeletable file must not fail the sweep (and with it
            // delete-repo); it is logged and left for the next run.
            match fs::remove_file(entry.path()) {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!(key, "cannot remove temp file: {e}"),
            }
            Ok(())
        })?;
        Ok(removed)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let path = self.resolve(key)?;
        let mut file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        file.seek(SeekFrom::Start(offset))?;
        let length_usize = usize::try_from(length)
            .map_err(|_| VykarError::Other(format!("range length too large: {length}")))?;
        let mut buf = vec![0u8; length_usize];
        let mut filled = 0;
        while filled < buf.len() {
            match file.read(buf.get_mut(filled..).expect("filled <= buf.len()")) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => return Err(e.into()),
            }
        }
        buf.truncate(filled);
        if filled != length_usize {
            return Err(VykarError::Other(format!(
                "short read on {key} at offset {offset}: expected {length} bytes, got {filled}"
            )));
        }
        Ok(Some(buf))
    }

    fn get_range_into(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &mut Vec<u8>,
    ) -> Result<bool> {
        let path = self.resolve(key)?;
        let mut file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                buf.clear();
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        };
        file.seek(SeekFrom::Start(offset))?;
        buf.clear();
        let length_usize = usize::try_from(length)
            .map_err(|_| VykarError::Other(format!("range length too large: {length}")))?;
        buf.resize(length_usize, 0);
        let mut filled = 0;
        while filled < buf.len() {
            match file.read(buf.get_mut(filled..).expect("filled <= buf.len()")) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => return Err(e.into()),
            }
        }
        if filled != length_usize {
            return Err(VykarError::Other(format!(
                "short read on {key} at offset {offset}: expected {length} bytes, got {filled}"
            )));
        }
        Ok(true)
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let path = self.resolve(key.trim_end_matches('/'))?;
        fs::create_dir_all(&path)?;
        Ok(())
    }

    crate::unsupported_server_ops!(except[remove_stale_temp_files]);
}

impl LocalBackend {
    fn list_entries(&self, prefix: &str, with_size: bool) -> Result<Vec<ListedObject>> {
        let dir = if prefix.is_empty() {
            self.root.clone()
        } else {
            self.resolve(prefix)?
        };
        match fs::metadata(&dir) {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => return Ok(Vec::new()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        }
        let mut out = Vec::new();
        self.walk_files(&dir, &mut |entry, key| {
            if vykar_protocol::is_temp_file(key) {
                return Ok(());
            }
            let size = if with_size {
                match entry.metadata() {
                    Ok(meta) => Some(meta.len()),
                    // Deleted between `read_dir` and the stat.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(e) => return Err(e.into()),
                }
            } else {
                None
            };
            out.push(ListedObject {
                key: key.to_string(),
                size,
            });
            Ok(())
        })?;
        Ok(out)
    }

    /// The storage's current time: the mtime of a freshly created probe file
    /// in the root. The probe carries the temp prefix, so a later sweep
    /// removes it if the cleanup here fails.
    fn storage_now(&self) -> Result<Option<std::time::SystemTime>> {
        let probe = tempfile::Builder::new()
            .prefix(&format!("{}sweep-probe.", vykar_protocol::TEMP_FILE_PREFIX))
            .rand_bytes(16)
            .tempfile_in(&self.root)?;
        Ok(probe.as_file().metadata()?.modified().ok())
    }

    /// Visit regular files in the repository's own namespace: the root's
    /// files and everything under [`vykar_protocol::KNOWN_ROOT_DIRS`].
    /// Foreign subdirectories are not entered.
    fn walk_repo_files(
        &self,
        visit: &mut dyn FnMut(&fs::DirEntry, &str) -> Result<()>,
    ) -> Result<()> {
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if file_type.is_file() {
                visit(&entry, &name)?;
            } else if file_type.is_dir() && vykar_protocol::KNOWN_ROOT_DIRS.contains(&&*name) {
                self.walk_files(&entry.path(), visit)?;
            }
        }
        Ok(())
    }

    /// Recursively visit all regular files under `dir`, passing each entry and
    /// its `/`-separated key relative to `self.root`.
    fn walk_files(
        &self,
        dir: &Path,
        visit: &mut dyn FnMut(&fs::DirEntry, &str) -> Result<()>,
    ) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                self.walk_files(&entry.path(), visit)?;
            } else if file_type.is_file() {
                if let Ok(rel) = entry.path().strip_prefix(&self.root) {
                    let key = rel
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    visit(&entry, &key)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_key_rejects_unsafe_keys() {
        // Absolute paths
        assert!(LocalBackend::validate_key("/etc/passwd").is_err());
        assert!(LocalBackend::validate_key("\\Windows\\System32").is_err());

        // Parent traversal
        assert!(LocalBackend::validate_key("../../outside").is_err());
        assert!(LocalBackend::validate_key("foo/../../etc/passwd").is_err());

        // Backslash
        assert!(LocalBackend::validate_key("foo\\bar").is_err());

        // Empty
        assert!(LocalBackend::validate_key("").is_err());
    }

    #[test]
    fn validate_key_accepts_safe_keys() {
        assert!(LocalBackend::validate_key("config").is_ok());
        assert!(LocalBackend::validate_key("packs/ab/deadbeef").is_ok());
        assert!(LocalBackend::validate_key("snapshots/abc123").is_ok());
        assert!(LocalBackend::validate_key("index").is_ok());
        assert!(LocalBackend::validate_key("keys/repokey").is_ok());
    }

    #[test]
    fn exists_returns_false_for_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        assert!(!backend.exists("no_such_file").unwrap());
    }

    #[test]
    fn exists_returns_true_for_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        backend.put("test_file", b"hello").unwrap();
        assert!(backend.exists("test_file").unwrap());
    }

    #[test]
    fn list_returns_empty_for_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        let keys = backend.list("no_such_dir").unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn resolve_rejects_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        assert!(backend.get("../../etc/passwd").is_err());
        assert!(backend.put("../escape", b"bad").is_err());
        assert!(backend.delete("/absolute").is_err());
    }

    #[test]
    fn put_overwrites_existing_key() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        backend.put("index", b"version1").unwrap();
        assert_eq!(backend.get("index").unwrap().unwrap(), b"version1");
        backend.put("index", b"version2").unwrap();
        assert_eq!(backend.get("index").unwrap().unwrap(), b"version2");
    }

    #[test]
    fn put_creates_parent_dirs_on_demand() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        // Parent directory "locks" doesn't exist yet — put should create it
        backend.put("locks/abc.json", b"lock").unwrap();
        assert_eq!(backend.get("locks/abc.json").unwrap().unwrap(), b"lock");
    }

    #[test]
    fn delete_missing_file_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();

        backend.delete("missing").unwrap();
    }

    #[test]
    fn delete_removes_file_and_syncs_parent() {
        use std::cell::Cell;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("marker");
        fs::write(&path, b"data").unwrap();
        let synced = Cell::new(false);

        LocalBackend::delete_path_with_sync(&path, |synced_dir| {
            assert_eq!(synced_dir, dir.path());
            synced.set(true);
            Ok(())
        })
        .unwrap();

        assert!(synced.get(), "successful delete must sync parent directory");
        assert!(!path.exists());
    }

    #[test]
    fn delete_propagates_parent_sync_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("marker");
        fs::write(&path, b"data").unwrap();

        let err = LocalBackend::delete_path_with_sync(&path, |_dir| {
            Err(VykarError::Other("sync failed".to_string()))
        })
        .unwrap_err();

        assert!(matches!(err, VykarError::Other(msg) if msg == "sync failed"));
        assert!(!path.exists(), "file removal happened before sync failure");
    }

    #[test]
    fn list_empty_prefix_returns_all_files() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        backend.put("config", b"cfg").unwrap();
        backend.put("manifest", b"mfst").unwrap();
        backend.put("keys/repokey", b"key").unwrap();
        backend.put("packs/ab/pack1", b"p1").unwrap();
        backend.put("snapshots/snap1", b"s1").unwrap();

        let mut keys = backend.list("").unwrap();
        keys.sort();

        assert_eq!(
            keys,
            vec![
                "config",
                "keys/repokey",
                "manifest",
                "packs/ab/pack1",
                "snapshots/snap1",
            ]
        );
    }

    #[test]
    fn list_and_list_sized_hide_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        backend.put("index", b"12345").unwrap();
        backend.put("packs/ab/pack1", b"pp").unwrap();
        fs::write(dir.path().join(".tmp.index.0123"), b"partial").unwrap();
        fs::write(dir.path().join("packs/ab/.tmp.pack1.abcd"), b"x").unwrap();

        let mut keys = backend.list("").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["index", "packs/ab/pack1"]);

        let mut sized = backend.list_sized("").unwrap();
        sized.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(
            sized,
            vec![
                ListedObject {
                    key: "index".into(),
                    size: Some(5)
                },
                ListedObject {
                    key: "packs/ab/pack1".into(),
                    size: Some(2)
                },
            ]
        );
        assert_eq!(backend.list("packs/").unwrap(), vec!["packs/ab/pack1"]);
    }

    #[test]
    fn remove_stale_temp_files_respects_age_and_walks_subdirs() {
        use std::time::{Duration, SystemTime};

        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        backend.put("index", b"i").unwrap();
        backend.put("packs/ab/pack1", b"p").unwrap();

        let old = SystemTime::now() - Duration::from_secs(2 * 3600);
        let stale = [
            ".tmp.index.1",
            "keys/.tmp.repokey.2",
            "snapshots/.tmp.s.3",
            "packs/ab/.tmp.pack1.4",
        ];
        for rel in stale {
            let path = dir.path().join(rel);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, b"x").unwrap();
            fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        let fresh = dir.path().join("packs/ab/.tmp.pack1.5");
        fs::write(&fresh, b"x").unwrap();

        let removed = backend
            .remove_stale_temp_files(Duration::from_secs(3600))
            .unwrap();
        assert_eq!(removed, stale.len());
        for rel in stale {
            assert!(!dir.path().join(rel).exists(), "{rel} should be swept");
        }
        assert!(fresh.exists(), "fresh temp file must survive");
        assert!(dir.path().join("index").exists());
        assert!(dir.path().join("packs/ab/pack1").exists());
    }

    #[test]
    fn sweep_leaves_foreign_directories_alone() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        backend.put("index", b"i").unwrap();
        let foreign = dir.path().join("other-app/.tmp.cache");
        fs::create_dir_all(foreign.parent().unwrap()).unwrap();
        fs::write(&foreign, b"x").unwrap();
        fs::write(dir.path().join(".tmp.index.1"), b"x").unwrap();

        assert_eq!(
            backend
                .remove_stale_temp_files(std::time::Duration::ZERO)
                .unwrap(),
            1
        );
        assert!(foreign.exists(), "files outside the repo namespace survive");
        assert!(!dir.path().join(".tmp.index.1").exists());
        // The clock probe leaves nothing behind.
        assert_eq!(
            backend
                .remove_stale_temp_files(std::time::Duration::from_secs(3600))
                .unwrap(),
            0
        );
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn sweep_continues_past_undeletable_temp_file() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        let locked = dir.path().join("packs/aa");
        fs::create_dir_all(&locked).unwrap();
        fs::write(locked.join(".tmp.stuck"), b"x").unwrap();
        fs::create_dir_all(dir.path().join("packs/bb")).unwrap();
        fs::write(dir.path().join("packs/bb/.tmp.free"), b"x").unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o555)).unwrap();
        // Root ignores directory mode bits; nothing to test then.
        if fs::File::create(locked.join("probe")).is_ok() {
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }

        let removed = backend.remove_stale_temp_files(std::time::Duration::ZERO);
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(removed.unwrap(), 1);
        assert!(locked.join(".tmp.stuck").exists());
        assert!(!dir.path().join("packs/bb/.tmp.free").exists());
    }

    #[test]
    #[cfg(unix)] // Windows mandatory file locking prevents concurrent rename-to-same-target
    fn put_concurrent_writes_are_atomic() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalBackend::new(dir.path().to_str().unwrap()).unwrap());
        // Pre-create the parent directory so both threads can write immediately
        backend.put("contested", b"seed").unwrap();

        let payload_a = vec![0xAAu8; 1024 * 64];
        let payload_b = vec![0xBBu8; 1024 * 64];

        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = [payload_a.clone(), payload_b.clone()]
            .into_iter()
            .map(|payload| {
                let backend = Arc::clone(&backend);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    backend.put("contested", &payload).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let result = backend.get("contested").unwrap().unwrap();
        // Result must be exactly one of the two full payloads — never a mixture
        assert!(result == payload_a || result == payload_b);
    }
}
