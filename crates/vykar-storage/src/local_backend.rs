use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use vykar_types::error::{Result, VykarError};

use crate::conditional::{Verifier, after_verified, check_args};
use crate::{
    BackendProfile, ConditionalSupport, ConditionalWrite, Created, ListedObject, RangeCursor,
    RangeReader, StagedPut, StorageBackend,
};

/// Write buffer of a staged object. Its appends are a 4-byte length prefix
/// followed by a blob of typically hundreds of KiB, which bypasses the buffer,
/// so this only coalesces the small writes.
const STAGED_WRITE_BUFFER: usize = 64 * 1024;

/// Storage backend for local filesystem using `std::fs` directly.
pub struct LocalBackend {
    root: PathBuf,
    request_width: usize,
    transfer_width: usize,
    /// Cache of the put-if-absent check.
    cond: Verifier,
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
        Ok(Self {
            root,
            request_width: crate::local_request_width(None),
            transfer_width: crate::transfer_width(None),
            cond: Verifier::new(),
        })
    }

    /// Size [`BackendProfile::request_width`] and
    /// [`BackendProfile::transfer_width`] from the repository's configured
    /// connection count.
    #[must_use]
    pub fn with_max_connections(mut self, max_connections: Option<usize>) -> Self {
        self.request_width = crate::local_request_width(max_connections);
        self.transfer_width = crate::transfer_width(max_connections);
        self
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

    /// Create a `.tmp.`-prefixed temp file in `dir`, creating `dir` if needed.
    fn temp_file_in(dir: &Path) -> std::io::Result<tempfile::NamedTempFile> {
        let create = || {
            tempfile::Builder::new()
                .prefix(vykar_protocol::TEMP_FILE_PREFIX)
                .rand_bytes(16)
                .tempfile_in(dir)
        };
        match create() {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(dir)?;
                create()
            }
            other => other,
        }
    }

    /// The unverified put-if-absent primitive: temp file, fsync, no-clobber
    /// rename (or link), fsync the directory.
    fn put_if_absent_raw(&self, key: &str, data: &[u8]) -> Result<Created> {
        let path = self.resolve(key)?;
        let dir = path.parent().unwrap_or(&self.root);
        let mut tmp = Self::temp_file_in(dir)?;
        tmp.write_all(data)?;
        vykar_common::fs::fsync_file(tmp.as_file())?;
        match tmp.persist_noclobber(&path) {
            Ok(persisted) => {
                drop(persisted);
                Self::sync_directory(dir)?;
                Ok(Created::Yes)
            }
            // Dropping the returned temp file removes it.
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
                Ok(Created::AlreadyExists)
            }
            Err(e) if vykar_common::fs::is_no_clobber_unsupported(&e.error) => {
                Err(VykarError::UnsupportedBackend(format!(
                    "local filesystem cannot create without replacing ({})",
                    e.error
                )))
            }
            Err(e) => Err(e.error.into()),
        }
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

    /// Stage into a temp file directly in `prefix`'s directory. For packs that
    /// is `packs/`, one level above the shard directory the final name lands
    /// in: the shard is only known once the content hash is, and `packs/` is
    /// on the same filesystem, so `commit` is a plain rename. `packs/` lies
    /// in `KNOWN_ROOT_DIRS`, so `remove_stale_temp_files` sweeps crash debris.
    fn begin_staged_put(&self, prefix: &str) -> Result<Option<Box<dyn StagedPut>>> {
        let prefix = prefix.trim_end_matches('/');
        let dir = self.resolve(prefix)?;
        let file = Self::temp_file_in(&dir)?;
        Ok(Some(Box::new(LocalStagedPut {
            root: self.root.clone(),
            prefix: format!("{prefix}/"),
            file: std::io::BufWriter::with_capacity(STAGED_WRITE_BUFFER, file),
        })))
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
            .list_entries(prefix, false, false)?
            .into_iter()
            .map(|o| o.key)
            .collect())
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        self.list_entries(prefix, true, false)
    }

    fn list_meta(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        self.list_entries(prefix, true, true)
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

    /// Read each piece straight from the file instead of buffering the range.
    /// The object size is checked here, so a truncated object fails before
    /// any piece is handed out, as it does with `get_range_into`.
    fn open_range<'a>(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &'a mut Vec<u8>,
    ) -> Result<Option<Box<dyn RangeReader + 'a>>> {
        let path = self.resolve(key)?;
        buf.clear();
        let file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let cursor = RangeCursor::new(offset, length)?;
        let size = file.metadata()?.len();
        if size < offset.saturating_add(length) {
            return Err(VykarError::Other(format!(
                "short read on {key} at offset {offset}: expected {length} bytes, got {}",
                size.saturating_sub(offset)
            )));
        }
        Ok(Some(Box::new(LocalRange {
            file,
            key: key.to_string(),
            cursor,
            buf,
        })))
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let path = self.resolve(key.trim_end_matches('/'))?;
        fs::create_dir_all(&path)?;
        Ok(())
    }

    fn profile(&self) -> BackendProfile {
        BackendProfile {
            local: true,
            staged_put: true,
            request_width: self.request_width,
            transfer_width: self.transfer_width,
        }
    }

    fn conditional(&self) -> Option<&dyn ConditionalWrite> {
        (!self.cond.refused()).then_some(self as &dyn ConditionalWrite)
    }

    crate::unsupported_server_ops!(except[remove_stale_temp_files, conditional]);
}

impl ConditionalWrite for LocalBackend {
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<Created> {
        check_args(key, data)?;
        self.cond
            .require("local", self, &|k, d| self.put_if_absent_raw(k, d))?;
        after_verified(self.put_if_absent_raw(key, data))
    }

    fn verify(&self) -> Result<ConditionalSupport> {
        self.cond.verify(self, &|k, d| self.put_if_absent_raw(k, d))
    }
}

/// A range opened by [`LocalBackend::open_range`]: each piece is one
/// positional read into the caller's buffer.
struct LocalRange<'a> {
    file: fs::File,
    key: String,
    cursor: RangeCursor,
    buf: &'a mut Vec<u8>,
}

impl RangeReader for LocalRange<'_> {
    fn read_at(&mut self, offset: u64, len: usize) -> Result<&mut [u8]> {
        self.cursor.advance(offset, len)?;
        self.buf.clear();
        self.buf.resize(len, 0);
        let filled = read_full_at(&mut self.file, self.buf, offset)?;
        if filled != len {
            return Err(VykarError::Other(format!(
                "short read on {} at offset {offset}: expected {len} bytes, got {filled}",
                self.key
            )));
        }
        Ok(self.buf.as_mut_slice())
    }
}

/// Fill `buf` from `file` at `offset`, stopping early only at end of file.
/// Returns the number of bytes read.
fn read_full_at(file: &mut fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        let mut filled = 0;
        while let Some(rest) = buf.get_mut(filled..).filter(|r| !r.is_empty()) {
            match file.read_at(rest, offset + filled as u64) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(filled)
    }
    #[cfg(not(unix))]
    {
        file.seek(SeekFrom::Start(offset))?;
        let mut filled = 0;
        while let Some(rest) = buf.get_mut(filled..).filter(|r| !r.is_empty()) {
            match file.read(rest) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(filled)
    }
}

/// A temp file being filled by [`LocalBackend::begin_staged_put`]. Dropping
/// it uncommitted deletes the file (`NamedTempFile`'s drop).
struct LocalStagedPut {
    root: PathBuf,
    /// The staging directory key with a trailing `/`; committed keys must
    /// start with it so the rename never leaves this directory's tree.
    prefix: String,
    file: std::io::BufWriter<tempfile::NamedTempFile>,
}

impl StagedPut for LocalStagedPut {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data)?;
        Ok(())
    }

    /// The same steps as `atomic_write` after its `write_all`: fsync the
    /// file, rename it to its final name, fsync the destination directory.
    /// The staging directory is not fsynced: should a crash lose the
    /// rename's removal of the temp name, the leftover is `.tmp.` debris for
    /// the sweep, never a visible object.
    fn commit(self: Box<Self>, key: &str) -> Result<()> {
        LocalBackend::validate_key(key)?;
        if !key.starts_with(&self.prefix) {
            return Err(VykarError::Other(format!(
                "staged object under '{}' cannot be committed as '{key}'",
                self.prefix
            )));
        }
        let path = self.root.join(key);
        let dir = path.parent().unwrap_or(&self.root).to_path_buf();
        let tmp = self.file.into_inner().map_err(|e| e.into_error())?;
        vykar_common::fs::fsync_file(tmp.as_file())?;
        let persisted = match tmp.persist(&path) {
            Ok(f) => f,
            Err(e) if e.error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&dir)?;
                e.file.persist(&path).map_err(|e| e.error)?
            }
            Err(e) => return Err(e.error.into()),
        };
        drop(persisted);
        LocalBackend::sync_directory(&dir)
    }
}

impl LocalBackend {
    fn list_entries(
        &self,
        prefix: &str,
        with_size: bool,
        with_mtime: bool,
    ) -> Result<Vec<ListedObject>> {
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
            let (size, mtime) = if with_size || with_mtime {
                match entry.metadata() {
                    Ok(meta) => (
                        with_size.then_some(meta.len()),
                        if with_mtime {
                            meta.modified().ok()
                        } else {
                            None
                        },
                    ),
                    // Deleted between `read_dir` and the stat.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(e) => return Err(e.into()),
                }
            } else {
                (None, None)
            };
            out.push(ListedObject {
                key: key.to_string(),
                size,
                mtime,
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
            } else if file_type.is_file()
                && let Ok(rel) = entry.path().strip_prefix(&self.root)
            {
                let key = rel
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                visit(&entry, &key)?;
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
    fn profile_reports_local_staging_and_width() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let backend = LocalBackend::new(root).unwrap();
        let profile = backend.profile();
        assert!(profile.local);
        assert!(profile.staged_put);
        assert_eq!(profile.request_width, 2);
        assert_eq!(profile.transfer_width, 2);
        assert!(backend.begin_staged_put("packs").unwrap().is_some());

        let wide = LocalBackend::new(root)
            .unwrap()
            .with_max_connections(Some(16));
        let wide = wide.profile();
        assert_eq!(wide.request_width, 8);
        assert_eq!(wide.transfer_width, 16);
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
                    size: Some(5),
                    mtime: None,
                },
                ListedObject {
                    key: "packs/ab/pack1".into(),
                    size: Some(2),
                    mtime: None,
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

    /// Files under `rel` (recursively), as `/`-joined keys.
    fn files_under(root: &Path, rel: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![root.join(rel)];
        while let Some(dir) = stack.pop() {
            let Ok(rd) = fs::read_dir(&dir) else { continue };
            for entry in rd {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let rel = path.strip_prefix(root).unwrap();
                    out.push(rel.to_string_lossy().replace('\\', "/"));
                }
            }
        }
        out.sort();
        out
    }

    #[test]
    fn staged_put_commits_under_final_key() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();

        let mut staged = backend.begin_staged_put("packs").unwrap().unwrap();
        staged.append(b"head").unwrap();
        // Larger than the write buffer, so it bypasses it.
        let big = vec![7u8; STAGED_WRITE_BUFFER + 3];
        staged.append(&big).unwrap();
        staged.append(b"tail").unwrap();

        // While open, only a hidden temp file directly in `packs/` exists.
        let staging = files_under(dir.path(), "packs");
        assert_eq!(staging.len(), 1);
        assert!(staging[0].starts_with("packs/.tmp."), "{staging:?}");
        assert!(vykar_protocol::is_repo_temp_file(&staging[0]));
        assert!(backend.list("packs/").unwrap().is_empty());

        // The shard directory does not exist yet; commit creates it.
        staged.commit("packs/ab/abcdef").unwrap();
        let mut expected = b"head".to_vec();
        expected.extend_from_slice(&big);
        expected.extend_from_slice(b"tail");
        assert_eq!(backend.get("packs/ab/abcdef").unwrap().unwrap(), expected);
        assert_eq!(files_under(dir.path(), "packs"), vec!["packs/ab/abcdef"]);
    }

    #[test]
    fn dropped_staged_put_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        let mut staged = backend.begin_staged_put("packs").unwrap().unwrap();
        staged.append(&[1u8; 100]).unwrap();
        drop(staged);
        assert!(files_under(dir.path(), "packs").is_empty());
    }

    #[test]
    fn staged_put_rejects_key_outside_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        for key in ["index", "packsx/ab/cd", "packs/../index", "/packs/ab/cd"] {
            let mut staged = backend.begin_staged_put("packs").unwrap().unwrap();
            staged.append(b"x").unwrap();
            assert!(staged.commit(key).is_err(), "{key} must be rejected");
        }
        assert!(files_under(dir.path(), "packs").is_empty());
        assert!(!dir.path().join("index").exists());
        assert!(backend.begin_staged_put("../escape").is_err());
    }

    #[test]
    #[cfg(unix)] // the forgotten handle stays open; Windows may refuse the delete
    fn stale_staged_temp_is_swept() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        let mut staged = backend.begin_staged_put("packs").unwrap().unwrap();
        staged.append(b"partial").unwrap();
        // Simulate a crash: the temp file survives the process.
        std::mem::forget(staged);
        assert_eq!(
            backend
                .remove_stale_temp_files(std::time::Duration::ZERO)
                .unwrap(),
            1
        );
        assert!(files_under(dir.path(), "packs").is_empty());
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

    // ── open_range: the streaming override and the buffered default ────────

    /// A 1000-byte object whose byte `i` is `i % 251`, in a fresh repo dir.
    fn range_fixture() -> (tempfile::TempDir, LocalBackend, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        let data: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        backend.put("packs/ab/obj", &data).unwrap();
        (dir, backend, data)
    }

    /// Read `pieces` from `[offset, offset+length)` with the local override or
    /// the buffered default, returning each piece's bytes.
    fn read_pieces(
        backend: &LocalBackend,
        buffered: bool,
        key: &str,
        offset: u64,
        length: u64,
        pieces: &[(u64, usize)],
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let mut buf = Vec::new();
        let reader = if buffered {
            crate::buffered_range(backend, key, offset, length, &mut buf)?
        } else {
            backend.open_range(key, offset, length, &mut buf)?
        };
        let Some(mut reader) = reader else {
            return Ok(None);
        };
        let mut out = Vec::new();
        for &(off, len) in pieces {
            out.push(reader.read_at(off, len)?.to_vec());
        }
        Ok(Some(out))
    }

    #[test]
    fn open_range_reads_pieces_and_skips_gaps() {
        let (_dir, backend, data) = range_fixture();
        // Range [100, 900); pieces with gaps before, between and after.
        let pieces = [(100, 10), (150, 50), (200, 1), (500, 300), (850, 50)];
        for buffered in [false, true] {
            let got = read_pieces(&backend, buffered, "packs/ab/obj", 100, 800, &pieces)
                .unwrap()
                .unwrap();
            for (&(off, len), bytes) in pieces.iter().zip(&got) {
                let off = off as usize;
                assert_eq!(
                    bytes,
                    &data[off..off + len],
                    "buffered={buffered} piece at {off}"
                );
            }
        }
    }

    #[test]
    fn open_range_missing_key_is_none() {
        let (_dir, backend, _) = range_fixture();
        for buffered in [false, true] {
            let got = read_pieces(&backend, buffered, "packs/ab/missing", 0, 10, &[(0, 10)]);
            assert!(got.unwrap().is_none(), "buffered={buffered}");
        }
    }

    #[test]
    fn open_range_past_end_of_object_fails_at_open() {
        let (_dir, backend, _) = range_fixture();
        for buffered in [false, true] {
            let mut buf = Vec::new();
            let err = if buffered {
                crate::buffered_range(&backend, "packs/ab/obj", 900, 200, &mut buf).err()
            } else {
                backend.open_range("packs/ab/obj", 900, 200, &mut buf).err()
            };
            let err = err
                .expect("short object must fail before any piece")
                .to_string();
            assert!(err.contains("short read"), "buffered={buffered}: {err}");
        }
    }

    #[test]
    fn open_range_rejects_pieces_outside_or_backwards() {
        let (_dir, backend, _) = range_fixture();
        for buffered in [false, true] {
            let key = "packs/ab/obj";
            // Before the range start.
            assert!(read_pieces(&backend, buffered, key, 100, 100, &[(50, 10)]).is_err());
            // Past the range end.
            assert!(read_pieces(&backend, buffered, key, 100, 100, &[(150, 51)]).is_err());
            // Overlapping the previous piece.
            assert!(
                read_pieces(&backend, buffered, key, 100, 100, &[(100, 20), (110, 5)]).is_err()
            );
            // Adjacent pieces and an empty piece are fine.
            let got = read_pieces(
                &backend,
                buffered,
                key,
                100,
                100,
                &[(100, 20), (120, 0), (120, 80)],
            )
            .unwrap()
            .unwrap();
            assert_eq!(got.iter().map(Vec::len).collect::<Vec<_>>(), [20, 0, 80]);
        }
    }

    #[test]
    fn open_range_object_truncated_after_open_is_short_read() {
        let (dir, backend, _) = range_fixture();
        let mut buf = Vec::new();
        let mut reader = backend
            .open_range("packs/ab/obj", 0, 1000, &mut buf)
            .unwrap()
            .unwrap();
        assert_eq!(reader.read_at(0, 100).unwrap().len(), 100);
        // Shrink the object under the open reader: the next piece is short.
        let f = fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join("packs/ab/obj"))
            .unwrap();
        f.set_len(600).unwrap();
        let err = reader.read_at(500, 200).unwrap_err().to_string();
        assert!(err.contains("short read"), "{err}");
        assert!(err.contains("expected 200 bytes, got 100"), "{err}");
    }
    #[test]
    fn conformance_suite_passes() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        assert!(backend.conditional().is_some());
        crate::conformance::run(&backend, &crate::conformance::ConformanceOptions::default())
            .unwrap();
    }

    #[test]
    fn put_if_absent_creates_missing_directories() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        let cond = backend.conditional().unwrap();
        assert_eq!(cond.put_if_absent("a/b/c/k", b"x").unwrap(), Created::Yes);
        assert_eq!(fs::read(dir.path().join("a/b/c/k")).unwrap(), b"x");
        assert_eq!(
            cond.put_if_absent("a/b/c/k", b"y").unwrap(),
            Created::AlreadyExists
        );
        assert_eq!(fs::read(dir.path().join("a/b/c/k")).unwrap(), b"x");
    }

    #[test]
    fn list_meta_reports_mtime_and_list_sized_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        backend.put("locks/a", b"abc").unwrap();
        let meta = backend.list_meta("locks/").unwrap();
        let [entry] = meta.as_slice() else {
            panic!("{meta:?}");
        };
        assert_eq!(entry.size, Some(3));
        let now = std::time::SystemTime::now();
        let mtime = entry.mtime.unwrap();
        let skew = now
            .duration_since(mtime)
            .or_else(|e| Ok::<_, ()>(e.duration()))
            .unwrap();
        assert!(skew < std::time::Duration::from_secs(5), "{skew:?}");
        assert_eq!(backend.list_sized("locks/").unwrap()[0].mtime, None);
    }
}
