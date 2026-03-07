use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use vykar_types::error::{Result, VykarError};

use crate::StorageBackend;

/// Storage backend for local filesystem using `std::fs` directly.
pub struct LocalBackend {
    root: PathBuf,
}

impl LocalBackend {
    /// Create a backend rooted at the given directory path.
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
        let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
        tmp.write_all(data)?;
        tmp.persist(path).map_err(|e| e.error)?;
        Ok(())
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

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.put(key, &data)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.resolve(key)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
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
        let dir = if prefix.is_empty() {
            self.root.clone()
        } else {
            self.resolve(prefix)?
        };
        match fs::metadata(&dir) {
            Ok(meta) if meta.is_dir() => {
                let mut keys = Vec::new();
                self.list_recursive(&dir, &mut keys)?;
                Ok(keys)
            }
            Ok(_) => Ok(Vec::new()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let path = self.resolve(key)?;
        let mut file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; length as usize];
        let mut filled = 0;
        while filled < buf.len() {
            match file.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => return Err(e.into()),
            }
        }
        buf.truncate(filled);
        if filled != length as usize {
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
        buf.resize(length as usize, 0);
        let mut filled = 0;
        while filled < buf.len() {
            match file.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => return Err(e.into()),
            }
        }
        if filled != length as usize {
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
}

impl LocalBackend {
    /// Recursively list all files under `dir`, adding their paths relative to
    /// `self.root` as `/`-separated keys.
    fn list_recursive(&self, dir: &std::path::Path, keys: &mut Vec<String>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                self.list_recursive(&entry.path(), keys)?;
            } else if file_type.is_file() {
                if let Ok(rel) = entry.path().strip_prefix(&self.root) {
                    // Convert to `/`-separated key to match repository storage keys.
                    let key = rel
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    keys.push(key);
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
