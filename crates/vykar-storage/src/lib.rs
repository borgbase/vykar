#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::doc_markdown,
        clippy::expect_used,
        clippy::format_push_string,
        clippy::indexing_slicing,
        clippy::manual_let_else,
        clippy::panic,
        clippy::unwrap_used
    )
)]

pub mod local_backend;
#[doc(hidden)]
pub mod probe;
pub mod rest_backend;
pub mod s3_backend;
#[cfg(feature = "backend-sftp")]
pub mod sftp_backend;

mod conditional;
#[doc(hidden)]
pub mod conformance;
mod delegate;
mod http_util;
mod retry;

#[doc(hidden)]
pub use delegate::__macro_support;
pub use delegate::InnerBackend;

pub use conditional::{
    ConditionalSupport, ConditionalWrite, Created, MAX_PUT_IF_ABSENT_BYTES,
    put_if_absent_unavailable,
};
// `S3ConditionalWrites` is defined below, next to `StorageConfig`.

#[cfg(feature = "backend-sftp")]
pub(crate) mod runtime;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use url::Url;

use vykar_types::cancel::CancelToken;
use vykar_types::error::{Result, VykarError};

// Wire-format types used in the `StorageBackend` trait surface. Callers import
// them from `vykar_protocol` directly.
use vykar_protocol::{
    RepackPlanRequest, RepackResultResponse, ServerCapabilities, VerifyPacksPlanRequest,
    VerifyPacksResponse,
};
use vykar_types::hash::HashAlgorithm;

/// Abstract key-value storage for repository objects.
/// Keys are `/`-separated string paths (e.g. "packs/ab/ab01cd02...").
///
/// # Runtime invariant
///
/// The trait surface is synchronous, but implementors (notably the SFTP
/// backend) may drive async I/O internally via `tokio::runtime::Runtime::block_on`.
/// Callers must therefore **not** invoke these methods from inside a tokio
/// runtime context — doing so panics with "Cannot start a runtime from within a
/// runtime." If you need to call a `StorageBackend` from async code, dispatch
/// through `tokio::task::spawn_blocking`.
#[allow(clippy::missing_errors_doc)]
pub trait StorageBackend: Send + Sync {
    /// Read an object by key. Returns `None` if not found.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Write an object, replacing any existing one.
    ///
    /// # Visibility (all backends)
    ///
    /// Publication is atomic: a reader observes either the complete previous
    /// object (or absence, if the key was absent) or the complete new one —
    /// never a prefix or a mix. After `Ok` the new object is visible.
    ///
    /// After `Err` the outcome is **unknown**: the key may hold the complete
    /// old object, the complete new one (e.g. a lost acknowledgement), or
    /// nothing only if it was absent before. Callers must not treat `Err` as
    /// "not published"; retrying the same `put` is the correct response.
    ///
    /// Backends that stage writes in temp files name them with
    /// [`vykar_protocol::TEMP_FILE_PREFIX`] so [`Self::list`] hides them and
    /// [`Self::remove_stale_temp_files`] can sweep them.
    ///
    /// # Durability (best effort, per backend)
    ///
    /// Survival of a server power loss is separate from visibility:
    /// - Local and the REST server fsync the file and its directory.
    /// - SFTP fsyncs the file via `fsync@openssh.com` when the server supports
    ///   it. Directories cannot be fsynced over SFTP, so a power loss right
    ///   after the rename may lose the rename.
    /// - S3 is durable once acknowledged.
    fn put(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Delete an object.
    fn delete(&self, key: &str) -> Result<()>;

    /// Check if an object exists.
    fn exists(&self, key: &str) -> Result<bool>;

    /// List all keys under a prefix. Returns full key paths.
    ///
    /// Never returns temp files ([`vykar_protocol::is_temp_file`]).
    ///
    /// # Consistency
    ///
    /// A LIST must reflect every PUT and DELETE that completed before it
    /// started, **including those made by other clients**. Repository locking
    /// (PUT own key, then LIST for others) depends on this. What provides it:
    /// - S3: AWS guarantees strong read-after-write and list consistency.
    ///   S3-compatible services must provide the same; not all are verified.
    /// - vykar-server: a single process over a local directory.
    /// - Local and SFTP: only as consistent as the filesystem underneath. A
    ///   repository on NFS/SMB with directory or attribute caching can serve a
    ///   stale listing (RFC 8881 §10.8.2), which breaks locking.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Read a byte range from an object. Returns `None` if not found.
    ///
    /// When the key exists, the returned `Vec<u8>` **must** contain exactly
    /// `length` bytes. A short read is an error, not a silent truncation.
    /// `length` must be > 0 (callers must not request zero-length reads).
    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>>;

    /// Read a byte range from an object into a caller-provided buffer.
    ///
    /// Returns `Ok(true)` when the key exists (buf filled), `Ok(false)` when
    /// not found (buf cleared). `LocalBackend` overrides this to read directly
    /// into `buf`, achieving true buffer reuse. Other backends fall through to
    /// `get_range()` + move.
    fn get_range_into(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &mut Vec<u8>,
    ) -> Result<bool> {
        if let Some(data) = self.get_range(key, offset, length)? {
            *buf = data;
            Ok(true)
        } else {
            buf.clear();
            Ok(false)
        }
    }

    /// Open a byte range of an object for reading piece by piece with
    /// [`RangeReader::read_at`], as one logical range request.
    ///
    /// Returns `Ok(None)` when the key does not exist (`buf` cleared). `buf`
    /// is the caller's scratch buffer: the reader keeps the bytes it hands out
    /// there, so its capacity is reused across ranges.
    ///
    /// The default buffers the whole range with [`Self::get_range_into`]
    /// (see [`buffered_range`]), so backends that fetch a range in one request
    /// keep exactly their request pattern and error behaviour.
    /// `LocalBackend` overrides it to read each piece straight from the file,
    /// which bounds the buffer to the largest piece instead of the range.
    ///
    /// The short-read contract still holds: reading a piece that the object
    /// cannot fully supply is an error, never a truncated slice. Backends that
    /// know the object size should fail at open, before any piece is handed
    /// out, so a truncated object fails the same way it does on the buffered
    /// path.
    ///
    /// A wrapper that intercepts `get_range_into` (rate limiting, test
    /// recorders, fault injection) must also decide what to do here, or
    /// streamed reads bypass it; [`crate::delegate_storage_backend!`] refuses
    /// to compile a wrapper that overrides one without the other. Calling
    /// [`buffered_range`] with `self` routes every read through the wrapper's
    /// own `get_range_into`.
    fn open_range<'a>(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &'a mut Vec<u8>,
    ) -> Result<Option<Box<dyn RangeReader + 'a>>> {
        buffered_range(self, key, offset, length, buf)
    }

    /// Create a directory marker (no-op for flat object stores).
    fn create_dir(&self, key: &str) -> Result<()>;

    /// Write an object from an owned buffer. Backends can override to avoid
    /// an extra copy when the caller already owns the data.
    ///
    /// Same contract as [`Self::put`]. Sealed packs go through [`Self::put_pack`],
    /// which may release the buffer early.
    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.put(key, &data)
    }

    /// Upload a sealed pack. `key` is a content-addressed pack key
    /// (`packs/<shard>/<id>`, the id naming the bytes), which lets a backend
    /// prove an ambiguous upload committed from the object's size alone.
    ///
    /// Visibility and durability as [`Self::put`]. The difference: a backend may
    /// drop `data` before it returns, once it no longer needs the bytes (REST and
    /// S3: after every part is acknowledged, before the completion request, and
    /// only after an earlier completion succeeded on this backend instance).
    /// A completion that still fails after its in-place retries (and, on S3, is
    /// not reconciled as committed by size) is then returned as an error instead
    /// of being retried with the whole object.
    ///
    /// Dropping `data` fires its release signal (see [`PackData`]); callers use
    /// it to stop counting the bytes as held.
    ///
    /// The default uploads through [`Self::put_owned`] and releases on return.
    fn put_pack(&self, key: &str, data: PackData) -> Result<()> {
        let (bytes, _release) = data.into_parts();
        self.put_owned(key, bytes)
    }

    /// Start writing an object incrementally, for callers that only learn
    /// its key once every byte is written (a pack is named by the hash of
    /// its contents). The bytes are staged under the directory key `prefix`
    /// (e.g. `"packs"`), and [`StagedPut::commit`] must name a key below it.
    ///
    /// Returns `Ok(None)` when the backend cannot stage writes; the caller
    /// then buffers the object and uses [`Self::put_pack`]. That is the
    /// default, and the right answer for every backend whose upload needs the
    /// whole object up front (S3, SFTP, REST).
    ///
    /// A wrapper that intercepts `put`/`put_owned`/`put_pack` (rate limiting, test
    /// recorders) must also decide what to do here, or staged objects bypass
    /// it; [`crate::delegate_storage_backend!`] refuses to compile a wrapper
    /// that overrides one without the other. Returning `Ok(None)` keeps every
    /// object on the `put` path the wrapper already intercepts; such a wrapper
    /// also reports `staged_put: false` from [`Self::profile`], which
    /// `delegate_storage_backend!` enforces.
    fn begin_staged_put(&self, _prefix: &str) -> Result<Option<Box<dyn StagedPut>>> {
        Ok(None)
    }

    /// Return the size in bytes of an object. Returns `None` if not found.
    ///
    /// Backends should override this with a metadata-only operation (e.g.
    /// HTTP HEAD, `stat()`, `fs::metadata`) to avoid downloading the object.
    fn size(&self, key: &str) -> Result<Option<u64>> {
        self.get(key)?
            .map(|v| {
                u64::try_from(v.len())
                    .map_err(|_| VykarError::Other("object length does not fit u64".into()))
            })
            .transpose()
    }

    /// Like [`Self::list`], plus each object's size when the backend's listing
    /// reports it cheaply. `size: None` means "not reported", not "absent";
    /// callers that need it fall back to [`Self::size`].
    fn list_sized(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        Ok(self
            .list(prefix)?
            .into_iter()
            .map(|key| ListedObject {
                key,
                size: None,
                mtime: None,
            })
            .collect())
    }

    /// Like [`Self::list_sized`], plus each object's last-modified time by
    /// the storage's clock where the backend reports it (local: filesystem
    /// mtime; SFTP: readdir mtime, 1 s resolution; S3: `LastModified`;
    /// vykar-server: milliseconds). `mtime: None` means "not reported" (a
    /// vykar-server older than `?list&sized&mtime`, or a backend without it).
    /// `list_sized` itself always reports `mtime: None`.
    fn list_meta(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        self.list_sized(prefix)
    }

    // ── Capability methods: no defaults, deliberately ──────────────────────
    //
    // The capability methods are the accessors `conditional`, `batch_delete`
    // and `server_ops`, plus `remove_stale_temp_files`, `bind_content_hash`
    // and `profile`. Callers never infer a capability from an error: they ask
    // the accessor, and `None` means "take the client-side path". A default
    // body here would make a *wrapper* that forgets to forward silently answer
    // "not offered" on behalf of a backend that does offer the operation —
    // exactly the bug that disabled server-side pack verification and server
    // init whenever a bandwidth cap was configured on a REST repo.
    //
    // Leaving them required means a new capability method breaks every impl,
    // including `delegate_storage_backend!`'s inventory, until it is handled.
    // Backends with no server-side APIs get the bodies from
    // `unsupported_server_ops!`, except `profile`, which every backend states
    // itself.
    //
    // The methods above may keep defaults: each routes back through this
    // impl's own `get`/`put`/`get_range`/`list`, so inheriting one is correct (at
    // worst less efficient), never a silent loss of capability; `put_pack`
    // routes through this impl's own `put_owned`, and `list_meta` through
    // its own `list_sized` (losing only the mtime, which then reads "not
    // reported"). The one exception, `begin_staged_put`, defaults to
    // `Ok(None)` ("buffer and `put` instead"), which is likewise only ever
    // less efficient.

    /// Delete temp files ([`vykar_protocol::is_repo_temp_file`]) last
    /// modified at least `older_than` ago (`Duration::ZERO` = all), in the
    /// repository's own namespace: the root and the
    /// [`vykar_protocol::KNOWN_ROOT_DIRS`] trees, never foreign
    /// subdirectories. Age is measured against the storage's clock, not the
    /// client's. A file the storage refuses to delete is logged and skipped;
    /// returns how many were removed.
    ///
    /// Callers must hold the maintenance lock, except delete-repo with
    /// `Duration::ZERO`: it destroys the repository, so a concurrent backup
    /// fails regardless, and a lock write could block deleting a full or
    /// read-only repository.
    ///
    /// Required for the reason in the note above: a default `Ok(0)` would let
    /// a wrapper silently skip the sweep. Backends that never leave
    /// client-side temp files (S3) get `Ok(0)` from `unsupported_server_ops!`;
    /// `RestBackend` returns it explicitly, since the server sweeps its own
    /// (and purges every temp file when delete-repo sends
    /// `batch-delete&cleanup-dirs`).
    fn remove_stale_temp_files(&self, older_than: std::time::Duration) -> Result<usize>;

    /// Bind this backend to the repository's content-digest algorithm.
    ///
    /// The backend is built by `backend_from_config` before the repository
    /// `config` has been read, so it has to be told which upload-integrity
    /// header to send. Binding happens exactly once, immediately after the
    /// format is resolved.
    ///
    /// Idempotent for the same value; a second, *different* binding is a
    /// programming error and returns an error rather than silently switching
    /// algorithms mid-run.
    ///
    /// Required rather than defaulted for the reason in the note above: a
    /// default no-op would be inherited by every wrapper and could then
    /// silently never reach `RestBackend`. Backends with nothing to bind get
    /// the `Ok(())` body from `unsupported_server_ops!`.
    fn bind_content_hash(&self, algo: HashAlgorithm) -> Result<()>;

    /// Atomic create-if-absent, when this backend offers it. `None`: never
    /// (the backend has no mechanism, the server does not advertise
    /// `conditional_put`, `s3_conditional_writes` says off or auto on a
    /// non-AWS endpoint), or a `verify()` on this instance concluded
    /// `Unsupported`. `Some` does not mean verified: the first
    /// `verify()`/`put_if_absent()` checks by writing.
    ///
    /// Never writes. `RestBackend` may `GET /health` (read-only) on its
    /// first call, and a failed probe there caches "nothing supported" for
    /// the process: call [`ServerOps::capabilities`] first to see the error.
    /// Required for the reason in the note above: a wrapper inheriting a
    /// `None` default would silently disable the capability.
    fn conditional(&self) -> Option<&dyn ConditionalWrite>;

    /// The backend's native multi-key delete, when it has one. `None`: delete
    /// key by key with [`Self::delete`] (local, SFTP, S3 with `s3_soft_delete`,
    /// or an S3-compatible endpoint that answered DeleteObjects with
    /// NotImplemented earlier on this instance). Never does I/O. Required for
    /// the reason in the note above: a wrapper inheriting a `None` default
    /// would silently cost compact one request per pack.
    fn batch_delete(&self) -> Option<&dyn BatchDelete>;

    /// The operations a vykar-server runs on the repository's behalf. `Some`
    /// exactly when a vykar-server is behind this backend (only `RestBackend`);
    /// which optional operations it serves is [`ServerOps::offers`]. Never does
    /// I/O. Required for the reason in the note above: a wrapper inheriting a
    /// `None` default would silently disable server-side verify, init and
    /// repack, the bug the note describes.
    fn server_ops(&self) -> Option<&dyn ServerOps>;

    /// What kind of backend this is. Required for the reason in the note above:
    /// a wrapper inheriting a default would report a guess instead of the
    /// backend's answer. A wrapper that overrides `begin_staged_put` must
    /// report `staged_put` to match.
    fn profile(&self) -> BackendProfile;
}

/// An object being written incrementally; see
/// [`StorageBackend::begin_staged_put`].
///
/// Nothing is visible under any key until [`Self::commit`] returns `Ok`.
/// Dropping it uncommitted discards the staged bytes. A crash leaves at most a
/// [`vykar_protocol::TEMP_FILE_PREFIX`] file that
/// [`StorageBackend::remove_stale_temp_files`] sweeps.
///
/// `Sync` so a repository holding an open staged pack can be shared
/// (`Arc<Repository>`); the methods take `&mut self` or `self`, so
/// implementors need no locking.
pub trait StagedPut: Send + Sync {
    /// Append `data` to the object.
    fn append(&mut self, data: &[u8]) -> Result<()>;

    /// Publish everything appended under `key`, with the visibility and
    /// durability contract of [`StorageBackend::put`]: after `Ok` the object
    /// is complete and as durable as a `put` of the same bytes would be.
    fn commit(self: Box<Self>, key: &str) -> Result<()>;
}

/// Sequential access to a range opened by [`StorageBackend::open_range`].
pub trait RangeReader {
    /// Return the `len` bytes at absolute object offset `offset`.
    ///
    /// Pieces must lie inside the opened range and must not go backwards:
    /// `offset` is at or after the end of the previous piece. Bytes between
    /// two pieces are skipped. The slice stays valid until the next call; it
    /// is mutable so callers can decode in place.
    ///
    /// A piece that cannot be read in full is an error.
    fn read_at(&mut self, offset: u64, len: usize) -> Result<&mut [u8]>;
}

/// [`StorageBackend::open_range`] by buffering the whole range with one
/// [`StorageBackend::get_range_into`] call on `backend`, then serving pieces
/// out of `buf`. The trait default, and the implementation for wrappers that
/// intercept `get_range_into`.
///
/// # Errors
///
/// Whatever `get_range_into` returns.
pub fn buffered_range<'a, B: StorageBackend + ?Sized>(
    backend: &B,
    key: &str,
    offset: u64,
    length: u64,
    buf: &'a mut Vec<u8>,
) -> Result<Option<Box<dyn RangeReader + 'a>>> {
    if !backend.get_range_into(key, offset, length, buf)? {
        return Ok(None);
    }
    Ok(Some(Box::new(BufferedRange {
        buf,
        cursor: RangeCursor::new(offset, length)?,
    })))
}

/// Position of a [`RangeReader`] inside its opened range; enforces the
/// in-range, forward-only contract of [`RangeReader::read_at`].
#[derive(Debug, Clone, Copy)]
pub struct RangeCursor {
    start: u64,
    end: u64,
    pos: u64,
}

impl RangeCursor {
    /// A cursor at the start of `[offset, offset + length)`.
    ///
    /// # Errors
    ///
    /// When the range end overflows `u64`.
    pub fn new(offset: u64, length: u64) -> Result<Self> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| VykarError::Other(format!("range overflows: {offset}+{length}")))?;
        Ok(Self {
            start: offset,
            end,
            pos: offset,
        })
    }

    /// Validate the next piece and advance past it. Returns the piece's
    /// offset relative to the start of the range.
    ///
    /// # Errors
    ///
    /// When the piece goes backwards or leaves the range.
    pub fn advance(&mut self, offset: u64, len: usize) -> Result<u64> {
        let piece_end = u64::try_from(len)
            .ok()
            .and_then(|l| offset.checked_add(l))
            .filter(|&e| offset >= self.pos && e <= self.end)
            .ok_or_else(|| {
                VykarError::Other(format!(
                    "range read {offset}+{len} outside [{}, {}) or before {}",
                    self.start, self.end, self.pos
                ))
            })?;
        self.pos = piece_end;
        Ok(offset - self.start)
    }
}

struct BufferedRange<'a> {
    buf: &'a mut Vec<u8>,
    cursor: RangeCursor,
}

impl RangeReader for BufferedRange<'_> {
    fn read_at(&mut self, offset: u64, len: usize) -> Result<&mut [u8]> {
        let rel = self.cursor.advance(offset, len)?;
        let rel = usize::try_from(rel)
            .map_err(|_| VykarError::Other(format!("range offset too large: {rel}")))?;
        // `get_range_into` filled exactly `length` bytes, and the cursor keeps
        // every piece inside that length.
        self.buf
            .get_mut(rel..rel + len)
            .ok_or_else(|| VykarError::Other("range piece beyond buffered bytes".into()))
    }
}

/// A sealed pack's bytes, handed to [`StorageBackend::put_pack`].
///
/// Dropping it frees the bytes and then fires the release signal given to
/// [`Self::with_release`], exactly once. A backend that is done with the bytes
/// before the upload is (an HTTP part upload waiting for its completion) drops
/// it early; otherwise it drops when `put_pack` returns, or on unwind.
pub struct PackData {
    // Declared before `release`: fields drop in declaration order, so the
    // bytes are freed before the signal fires.
    bytes: Vec<u8>,
    release: PackRelease,
}

/// The release half of a [`PackData`]; fires its signal when dropped.
pub struct PackRelease(Option<Box<dyn FnOnce() + Send>>);

impl PackData {
    /// Bytes with no release signal.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            release: PackRelease(None),
        }
    }

    /// Bytes whose release calls `on_release`.
    pub fn with_release(bytes: Vec<u8>, on_release: impl FnOnce() + Send + 'static) -> Self {
        Self {
            bytes,
            release: PackRelease(Some(Box::new(on_release))),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Split into the bytes and the signal, for a backend that hands the
    /// bytes on by value. Keep the `PackRelease` alive until the bytes are
    /// gone: binding it to `_` fires the signal immediately.
    pub fn into_parts(self) -> (Vec<u8>, PackRelease) {
        (self.bytes, self.release)
    }
}

impl Drop for PackRelease {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

/// What a backend is, for callers that tune to it. See [`StorageBackend::profile`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendProfile {
    /// Repository on a local filesystem.
    pub local: bool,
    /// [`StorageBackend::begin_staged_put`] can return a staged writer.
    pub staged_put: bool,
    /// Independent read-only requests worth issuing at once (listing,
    /// existence checks, pack verification, item-stream range reads). At
    /// least 1. May exceed what the backend serves at once: SFTP reports
    /// three per pooled connection, and the extra requests wait for a
    /// connection.
    pub request_width: usize,
    /// Whole-object transfers worth running at once: pack uploads during a
    /// backup (a writer may queue [`UPLOAD_HEADROOM`] more), read groups
    /// during a restore. An HTTP backend may split each transfer into
    /// parallel parts. At least 1.
    pub transfer_width: usize,
}

/// One entry of [`StorageBackend::list_sized`] and
/// [`StorageBackend::list_meta`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    pub key: String,
    pub size: Option<u64>,
    /// Last modification by the storage's clock; `None` = not reported.
    /// Only [`StorageBackend::list_meta`] fills it.
    pub mtime: Option<std::time::SystemTime>,
}

/// What [`BatchDelete::delete_keys`] removes besides the named keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchDeleteScope {
    /// Exactly the named keys. For a repository that stays live (compact).
    KeysOnly,
    /// The repository itself is going away (delete-repo): a vykar-server
    /// also prunes now-empty directories and purges every temp file,
    /// including those of a backup still in flight (`cleanup-dirs`).
    WholeRepository,
}

/// A backend-native multi-key delete. Obtained from
/// [`StorageBackend::batch_delete`].
pub trait BatchDelete: Send + Sync {
    /// Delete `keys`. An absent key is not an error. After `Err` an unknown
    /// subset may already be deleted; deleting a key again is harmless.
    ///
    /// `scope` says whether the repository survives. Only
    /// [`BatchDeleteScope::WholeRepository`] may touch anything not named in
    /// `keys`; a caller in a live repository (compact) must pass
    /// [`BatchDeleteScope::KeysOnly`].
    ///
    /// Never reports `UnsupportedBackend`: a backend that learns during the call
    /// that its batch API is missing deletes the remaining keys one by one
    /// before returning, and offers no `batch_delete()` from then on.
    ///
    /// # Errors
    ///
    /// The first failed request.
    fn delete_keys(&self, keys: &[String], scope: BatchDeleteScope) -> Result<()>;
}

/// An optional operation of a [`ServerOps`], asked with [`ServerOps::offers`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerOp {
    /// [`ServerOps::verify_packs`].
    VerifyPacks,
    /// [`ServerOps::repack`].
    Repack,
    /// [`ServerOps::repack`] of a plan whose operations carry a
    /// [`vykar_protocol::RepackOperationRequest::trailer`]. Nothing in vykar
    /// sends one yet (05-index-gc-format.md step 4).
    RepackTrailer,
    /// [`ServerOps::object_sizes`].
    ObjectSizes,
    /// [`ServerOps::list_tree_sized`].
    ListTree,
}

/// What a vykar-server does on the repository's behalf. Obtained from
/// [`StorageBackend::server_ops`]; only `RestBackend` (and test fakes)
/// implement it. Wrappers forward the accessor and never implement this
/// trait, so the defaults below cannot hide a forgotten forward.
pub trait ServerOps: Send + Sync {
    /// Whether this server serves `op`. Callers ask before calling the
    /// matching method and never infer it from an error. May `GET /health`
    /// (read-only) on its first call; a failed probe offers only what every
    /// supported vykar-server serves.
    fn offers(&self, op: ServerOp) -> bool;

    /// `GET /health`, fetched now. Every vykar-server answers it.
    ///
    /// # Errors
    ///
    /// Transport or parse failure.
    fn capabilities(&self) -> Result<ServerCapabilities>;

    /// Server-side repository directory scaffolding (keys/, snapshots/,
    /// locks/, packs/*). Every vykar-server answers it.
    ///
    /// # Errors
    ///
    /// The request failed.
    fn init(&self) -> Result<()>;

    /// Server-side pack verification: hash check + header + blob boundary
    /// scan. Only after `offers(ServerOp::VerifyPacks)`.
    ///
    /// # Errors
    ///
    /// The request failed; `Other` when not offered.
    fn verify_packs(&self, plan: &VerifyPacksPlanRequest) -> Result<VerifyPacksResponse> {
        let _ = plan;
        Err(not_offered("verify-packs"))
    }

    /// Execute a server-side repack plan. Only after `offers(ServerOp::Repack)`;
    /// a plan that carries a trailer only after `offers(ServerOp::RepackTrailer)`
    /// (`RestBackend` refuses it locally otherwise).
    ///
    /// # Errors
    ///
    /// The request failed; `Other` when not offered.
    fn repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        let _ = plan;
        Err(not_offered("repack"))
    }

    /// The size of each of `keys` (`None` = absent), in order, from as few
    /// requests as the server allows (one per top-level directory). Every key
    /// must lie inside a directory. Only after `offers(ServerOp::ObjectSizes)`.
    /// `RestBackend` against a server that predates `?sizes` returns `Err` once
    /// and stops offering it.
    ///
    /// # Errors
    ///
    /// The request failed, or the server answered the wrong count.
    fn object_sizes(&self, keys: &[String]) -> Result<Vec<Option<u64>>> {
        let _ = keys;
        Err(not_offered("object sizes"))
    }

    /// [`StorageBackend::list_sized`] of the whole tree under `prefix` in one
    /// request (compact's 256 pack shards). Same result contract, including
    /// `size: None` from a server older than `?list&sized`. Only after
    /// `offers(ServerOp::ListTree)`.
    ///
    /// # Errors
    ///
    /// The request failed; `Other` when not offered.
    fn list_tree_sized(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        let _ = prefix;
        Err(not_offered("tree listing"))
    }
}

/// The error of a [`ServerOps`] method called without its `offers()`: a
/// programming error, deliberately not `UnsupportedBackend`.
fn not_offered(op: &str) -> VykarError {
    VykarError::Other(format!("server operation not offered: {op}"))
}

// Lets a `Box<Arc<dyn StorageBackend>>` coerce to `Box<dyn StorageBackend>`
// (used by tests that share one backend between a repository and assertions).
impl InnerBackend for Arc<dyn StorageBackend> {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &**self
    }
}

delegate_storage_backend! {
    for Arc<dyn StorageBackend>;
    except [];
}

/// Parsed repository URL.
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedUrl {
    /// Local filesystem path.
    Local { path: String },
    /// S3 or S3-compatible storage.
    S3 {
        bucket: String,
        root: String,
        /// Explicit endpoint URL derived from the repository URL.
        endpoint: String,
    },
    /// SFTP remote storage.
    Sftp {
        user: Option<String>,
        host: String,
        port: Option<u16>,
        path: String,
    },
    /// REST backend (HTTP/HTTPS).
    Rest { url: String },
}

impl ParsedUrl {
    /// Returns `true` for local filesystem repositories.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }
}

/// Parse a repository URL into its components.
///
/// Supported formats:
/// - Bare path (`/backups/repo`, `./relative`, `relative`) -> `Local`
/// - `file:///backups/repo` -> `Local`
/// - `s3://endpoint[:port]/bucket/prefix` -> `S3` over HTTPS
/// - `s3+http://endpoint[:port]/bucket/prefix` -> `S3` over HTTP (unsafe; blocked by default)
/// - `sftp://[user@]host[:port]/path` -> `Sftp`
/// - `http(s)://...` -> `Rest`
///
/// # Errors
///
/// Returns an error when the URL is empty, has an unsupported scheme, or is
/// missing required scheme-specific parts.
pub fn parse_repo_url(raw: &str) -> Result<ParsedUrl> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(VykarError::Config(
            "repository URL must not be empty".into(),
        ));
    }

    // Bare path: starts with `/`, `./`, or `../`
    if trimmed.starts_with('/')
        || trimmed.starts_with("./")
        || trimmed.starts_with("../")
        || trimmed == "."
        || trimmed == ".."
    {
        return Ok(ParsedUrl::Local {
            path: trimmed.to_string(),
        });
    }

    // Bare relative paths (without ./) are local too.
    if !trimmed.contains("://") {
        return Ok(ParsedUrl::Local {
            path: trimmed.to_string(),
        });
    }

    // Try to parse as URL
    let url = Url::parse(trimmed)
        .map_err(|e| VykarError::Config(format!("invalid repository URL '{trimmed}': {e}")))?;

    match url.scheme() {
        "file" => {
            let path = url.path().to_string();
            if path.is_empty() {
                return Err(VykarError::Config("file:// URL has empty path".into()));
            }
            Ok(ParsedUrl::Local { path })
        }
        "s3" | "s3+https" => parse_s3_url(&url, "https"),
        "s3+http" => parse_s3_url(&url, "http"),
        "sftp" => parse_sftp_url(&url),
        "http" | "https" => Ok(ParsedUrl::Rest {
            url: trimmed.to_string(),
        }),
        other => Err(VykarError::UnsupportedBackend(format!(
            "unsupported URL scheme: '{other}'"
        ))),
    }
}

/// Parse an S3 URL (`s3://` or `s3+http://`).
///
/// S3 URLs must always include an endpoint host and bucket path:
/// `s3://endpoint[:port]/bucket[/prefix]`.
fn parse_s3_url(url: &Url, endpoint_scheme: &str) -> Result<ParsedUrl> {
    let host = url
        .host_str()
        .ok_or_else(|| VykarError::Config("s3 URL is missing an endpoint host".into()))?;

    let port_suffix = url.port().map(|p| format!(":{p}")).unwrap_or_default();
    let endpoint = format!("{endpoint_scheme}://{host}{port_suffix}");

    let path = url.path().trim_start_matches('/');
    let (bucket, root) = path.split_once('/').unwrap_or((path, ""));
    if bucket.is_empty() {
        return Err(VykarError::Config(
            "s3 URL must include a bucket in the path (expected s3://endpoint/bucket[/prefix])"
                .into(),
        ));
    }
    Ok(ParsedUrl::S3 {
        bucket: bucket.to_string(),
        root: root.to_string(),
        endpoint,
    })
}

/// Parse an `sftp://` URL.
fn parse_sftp_url(url: &Url) -> Result<ParsedUrl> {
    let host = url
        .host_str()
        .ok_or_else(|| VykarError::Config("sftp:// URL is missing a host".into()))?;

    let user = if url.username().is_empty() {
        None
    } else {
        Some(url.username().to_string())
    };

    let path = url.path().to_string();

    Ok(ParsedUrl::Sftp {
        user,
        host: host.to_string(),
        port: url.port(),
        path,
    })
}

fn enforce_secure_http(url: &str, allow_insecure_http: bool, field_name: &str) -> Result<()> {
    let lowered = url.to_ascii_lowercase();
    if lowered.starts_with("http://") {
        if allow_insecure_http {
            tracing::warn!(
                "{field_name} uses plaintext HTTP; repository.allow_insecure_http=true enables this unsafe mode"
            );
            return Ok(());
        }
        return Err(VykarError::Config(format!(
            "{field_name} uses insecure HTTP and is blocked by default. \
Use HTTPS, or set repository.allow_insecure_http: true to permit plaintext HTTP (unsafe)."
        )));
    }
    Ok(())
}

/// Whether the S3 backend offers put-if-absent (`If-None-Match: *` on
/// PutObject), the `s3_conditional_writes` repository setting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum S3ConditionalWrites {
    /// Only for AWS endpoints (host under `amazonaws.com` or
    /// `amazonaws.com.cn`), and only once the sequential check passes. Every
    /// other endpoint: off.
    #[default]
    Auto,
    /// The user asserts the service honours the condition atomically. The
    /// sequential check still runs and refuses a service that ignores it.
    On,
    /// Never offered.
    Off,
}

/// Configuration for constructing a storage backend.
///
/// This is a decoupled subset of the full repository config, containing
/// only the fields needed by storage backends.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub url: String,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub sftp_key: Option<String>,
    pub sftp_agent: Option<String>,
    pub sftp_known_hosts: Option<String>,
    pub max_connections: Option<usize>,
    pub sftp_timeout: Option<u64>,
    pub access_token: Option<String>,
    pub allow_insecure_http: bool,
    pub retry: RetryConfig,
    /// Use soft-delete for S3 Object Lock compatibility.
    pub s3_soft_delete: bool,
    /// Whether S3 offers put-if-absent; see [`S3ConditionalWrites`].
    pub s3_conditional_writes: S3ConditionalWrites,
    /// Cancels retry waits and abandons SFTP reads in flight. Once it is
    /// cancelled, a remote backend starts no new attempt of an operation and
    /// returns Interrupted. The first attempt of an operation still runs, so
    /// cleanup after a cancel (lock release, session removal) is attempted.
    /// Requests that change storage are never abandoned in flight. Use
    /// CancelToken::new() for a backend that nothing cancels. The local
    /// backend ignores it.
    pub cancel: CancelToken,
}

impl StorageConfig {
    /// This config with [`StorageConfig::cancel`] replaced by `cancel`.
    #[must_use]
    pub fn with_cancel(mut self, cancel: CancelToken) -> Self {
        self.cancel = cancel;
        self
    }
}

/// [`BackendProfile::request_width`] of a remote backend: three requests per
/// configured connection, capped at 24.
pub(crate) fn remote_request_width(max_connections: Option<usize>) -> usize {
    (max_connections.unwrap_or(2) * 3).clamp(1, 24)
}

/// [`BackendProfile::request_width`] of a local backend: one per configured
/// connection, capped at 8.
pub(crate) fn local_request_width(max_connections: Option<usize>) -> usize {
    max_connections.unwrap_or(2).clamp(1, 8)
}

/// [`BackendProfile::transfer_width`] of the local, REST and S3 backends:
/// one transfer per configured connection (default 2).
pub(crate) fn transfer_width(max_connections: Option<usize>) -> usize {
    max_connections.unwrap_or(2).max(1)
}

/// Sealed packs a writer may hold beyond [`BackendProfile::transfer_width`],
/// each already on its own upload thread, so up to
/// `transfer_width + UPLOAD_HEADROOM` pack uploads run at once and a sealed
/// pack is ready the moment a transfer finishes. Consumed by the writer
/// only; the HTTP part-slot and pool sizes are independent formulas.
pub const UPLOAD_HEADROOM: usize = 2;

/// Idle-connection pool size for the HTTP backends, derived from the
/// repository's `max_connections`.
///
/// ureq's defaults (3 idle per host, 10 total) are far below the number of
/// threads vykar points at a single host: listing and existence checks run at
/// [`remote_request_width`], so anything smaller throws away connections and
/// pays a fresh TLS handshake per request. Idle connections are a reuse cache
/// rather than a concurrency cap, so sizing to the worst-case consumer only
/// costs a few idle sockets. A repository is a single host, so the per-host
/// and total limits are the same number.
pub(crate) fn http_idle_pool_size(max_connections: Option<usize>) -> usize {
    remote_request_width(max_connections).max(4)
}

/// How long an idle connection may sit in the pool before being discarded.
/// ureq's 15s default expires connections between phases of a backup.
pub(crate) const HTTP_IDLE_AGE: std::time::Duration = std::time::Duration::from_secs(60);

/// TLS settings for the REST and S3 agents.
///
/// Repositories are often self-hosted behind an internal CA, so verify against
/// the OS trust store rather than ureq's bundled Mozilla roots. There is no
/// fallback to those roots: a Linux host with no CA bundle fails with a clear
/// error instead of quietly trusting a different set of authorities.
pub(crate) fn http_tls_config() -> ureq::tls::TlsConfig {
    ureq::tls::TlsConfig::builder()
        .root_certs(ureq::tls::RootCerts::PlatformVerifier)
        .build()
}

// HTTP phase timeouts for REST and S3. ureq leaves request-header writes and
// the wait for response headers unbounded by default, so a pooled connection
// whose peer silently vanished would block forever. These values aim for
// "never infinite", not fast failure.

/// DNS resolution of a new connection.
pub(crate) const HTTP_RESOLVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Writing request headers (the body has its own budget).
pub(crate) const HTTP_SEND_REQUEST_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(30);
/// Waiting for response headers on plain data operations.
pub(crate) const HTTP_RECV_RESPONSE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);
/// Waiting for response headers on vykar-server admin actions. Coupled to the
/// server's `ADMIN_ACTION_TIMEOUT` (600s) plus margin; raise both together.
pub(crate) const HTTP_ADMIN_RECV_RESPONSE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(11 * 60);
/// Waiting for a server-side repack result. A finite deadline is only safe
/// because compact deletes source packs itself after persisting the index; a
/// timed-out repack leaves orphan new packs, never dangling index entries.
pub(crate) const HTTP_REPACK_RECV_RESPONSE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(60 * 60);

/// Retry configuration for remote storage backends (S3, SFTP, REST).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries).
    #[serde(default = "RetryConfig::default_max_retries")]
    pub max_retries: usize,
    /// Initial delay between retries in milliseconds.
    #[serde(default = "RetryConfig::default_retry_delay_ms")]
    pub retry_delay_ms: u64,
    /// Maximum delay between retries in milliseconds.
    #[serde(default = "RetryConfig::default_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,
}

impl RetryConfig {
    fn default_max_retries() -> usize {
        5
    }
    fn default_retry_delay_ms() -> u64 {
        1500
    }
    fn default_retry_max_delay_ms() -> u64 {
        60_000
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: Self::default_max_retries(),
            retry_delay_ms: Self::default_retry_delay_ms(),
            retry_max_delay_ms: Self::default_retry_max_delay_ms(),
        }
    }
}

/// Build a storage backend from a `StorageConfig`.
///
/// # Errors
///
/// Returns an error when the repository URL is invalid, insecure HTTP is not
/// explicitly allowed, or backend initialization fails.
pub fn backend_from_config(cfg: &StorageConfig) -> Result<Box<dyn StorageBackend>> {
    let parsed = parse_repo_url(&cfg.url)?;

    if let ParsedUrl::Rest { url } = &parsed {
        enforce_secure_http(url, cfg.allow_insecure_http, "repository.url")?;
    }

    if let ParsedUrl::S3 { endpoint, .. } = &parsed {
        enforce_secure_http(endpoint, cfg.allow_insecure_http, "repository.url")?;
    }

    match parsed {
        ParsedUrl::Local { path } => Ok(Box::new(
            local_backend::LocalBackend::new(&path)?.with_max_connections(cfg.max_connections),
        )),
        ParsedUrl::S3 {
            bucket,
            root,
            endpoint,
        } => {
            let region = cfg.region.as_deref().unwrap_or("us-east-1");

            let access_key_id = cfg.access_key_id.as_deref().ok_or_else(|| {
                VykarError::Config("S3 requires access_key_id in repository config".into())
            })?;
            let secret_access_key = cfg.secret_access_key.as_deref().ok_or_else(|| {
                VykarError::Config("S3 requires secret_access_key in repository config".into())
            })?;

            Ok(Box::new(
                s3_backend::S3Backend::new(
                    &bucket,
                    region,
                    &root,
                    &endpoint,
                    access_key_id,
                    secret_access_key,
                    cfg.retry,
                    cfg.s3_soft_delete,
                    cfg.max_connections,
                )?
                .with_cancel(cfg.cancel.clone())
                .with_conditional_writes(cfg.s3_conditional_writes),
            ))
        }
        #[cfg(feature = "backend-sftp")]
        ParsedUrl::Sftp {
            user,
            host,
            port,
            path,
        } => Ok(Box::new(
            sftp_backend::SftpBackend::new(
                &host,
                user.as_deref(),
                port,
                &path,
                cfg.sftp_key.as_deref(),
                cfg.sftp_agent.as_deref(),
                cfg.sftp_known_hosts.as_deref(),
                cfg.max_connections,
                cfg.sftp_timeout,
                cfg.retry,
            )?
            .with_cancel(cfg.cancel.clone()),
        )),
        #[cfg(not(feature = "backend-sftp"))]
        ParsedUrl::Sftp { .. } => Err(VykarError::UnsupportedBackend(
            "sftp (compile with feature 'backend-sftp')".into(),
        )),
        ParsedUrl::Rest { url } => {
            let token = cfg.access_token.as_deref();
            Ok(Box::new(
                rest_backend::RestBackend::new(&url, token, cfg.retry, cfg.max_connections)?
                    .with_cancel(cfg.cancel.clone()),
            ))
        }
    }
}

/// A temp file is stale once it is at least `older_than` old by the storage
/// server's clock. An mtime in the future is never stale.
pub(crate) fn is_stale(
    server_now: std::time::SystemTime,
    modified: std::time::SystemTime,
    older_than: std::time::Duration,
) -> bool {
    server_now
        .duration_since(modified)
        .is_ok_and(|age| age >= older_than)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn counting_release(bytes: Vec<u8>) -> (PackData, Arc<AtomicUsize>) {
        let fired = Arc::new(AtomicUsize::new(0));
        let f = Arc::clone(&fired);
        let data = PackData::with_release(bytes, move || {
            f.fetch_add(1, Ordering::SeqCst);
        });
        (data, fired)
    }

    #[test]
    fn pack_data_release_fires_once_on_drop() {
        let (data, fired) = counting_release(vec![1, 2, 3]);
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        drop(data);
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pack_data_into_parts_defers_release_to_the_guard() {
        let (data, fired) = counting_release(vec![1, 2, 3]);
        let (bytes, release) = data.into_parts();
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        drop(bytes);
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        drop(release);
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pack_data_new_has_no_signal() {
        let data = PackData::new(vec![1, 2]);
        assert_eq!(data.len(), 2);
        assert!(!data.is_empty());
        drop(data);
    }

    /// A leaf that keeps the trait's `put_pack`, recording what `put_owned`
    /// saw and whether the release had fired by then.
    struct OwnedRecorder {
        fired: Arc<AtomicUsize>,
        seen: Mutex<Option<(Vec<u8>, usize)>>,
    }

    impl StorageBackend for OwnedRecorder {
        fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
            Err(VykarError::Other("put_pack must reach put_owned".into()))
        }
        fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }
        fn exists(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }
        fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn get_range(&self, _key: &str, _offset: u64, _length: u64) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn create_dir(&self, _key: &str) -> Result<()> {
            Ok(())
        }
        fn put_owned(&self, _key: &str, data: Vec<u8>) -> Result<()> {
            *self.seen.lock().unwrap() = Some((data, self.fired.load(Ordering::SeqCst)));
            Ok(())
        }
        fn profile(&self) -> BackendProfile {
            BackendProfile {
                local: false,
                staged_put: false,
                request_width: 1,
                transfer_width: 1,
            }
        }
        unsupported_server_ops!();
    }

    #[test]
    fn default_put_pack_goes_through_put_owned_and_releases_on_return() {
        let (data, fired) = counting_release(vec![7; 64]);
        let backend = OwnedRecorder {
            fired: Arc::clone(&fired),
            seen: Mutex::new(None),
        };
        backend.put_pack("packs/00/x", data).unwrap();
        let (bytes, fired_inside) = backend.seen.lock().unwrap().take().unwrap();
        assert_eq!(bytes, vec![7; 64]);
        assert_eq!(fired_inside, 0, "released only after put_owned returned");
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn http_tls_uses_platform_verifier() {
        assert!(matches!(
            http_tls_config().root_certs(),
            ureq::tls::RootCerts::PlatformVerifier
        ));
    }

    #[test]
    fn request_width_and_idle_pool() {
        assert_eq!(http_idle_pool_size(None), 6);
        assert_eq!(http_idle_pool_size(Some(1)), 4); // floor, not 3
        assert_eq!(http_idle_pool_size(Some(4)), 12);
        assert_eq!(http_idle_pool_size(Some(8)), 24);
        assert_eq!(http_idle_pool_size(Some(64)), 24); // ceiling

        for (c, w) in [
            (None, 6),
            (Some(1), 3),
            (Some(2), 6),
            (Some(4), 12),
            (Some(8), 24),
            (Some(16), 24),
        ] {
            assert_eq!(remote_request_width(c), w, "remote {c:?}");
        }
        for (c, w) in [
            (None, 2),
            (Some(1), 1),
            (Some(2), 2),
            (Some(8), 8),
            (Some(16), 8),
        ] {
            assert_eq!(local_request_width(c), w, "local {c:?}");
        }
        for (c, w) in [
            (None, 2),
            (Some(0), 1),
            (Some(1), 1),
            (Some(8), 8),
            (Some(16), 16),
        ] {
            assert_eq!(transfer_width(c), w, "transfer {c:?}");
        }
    }

    /// Remote `(c * 3).min(24)`, local `c.min(8)`; transfer width `c` everywhere.
    #[test]
    fn profile_widths_by_backend() {
        let tmp = tempfile::tempdir().unwrap();
        let local_url = tmp.path().to_str().unwrap().to_string();
        for c in 1..=16usize {
            let mut local = test_config(&local_url);
            local.max_connections = Some(c);
            assert_eq!(
                backend_from_config(&local).unwrap().profile(),
                BackendProfile {
                    local: true,
                    staged_put: true,
                    request_width: c.min(8),
                    transfer_width: c,
                },
                "local c={c}"
            );

            let remote = BackendProfile {
                local: false,
                staged_put: false,
                request_width: (c * 3).min(24),
                transfer_width: c,
            };
            let mut rest = test_config("https://backup.example.com/repo");
            rest.max_connections = Some(c);
            assert_eq!(
                backend_from_config(&rest).unwrap().profile(),
                remote,
                "rest c={c}"
            );

            let mut s3 = test_config("s3://s3.example.com/my-bucket/vykar");
            s3.access_key_id = Some("test-key".into());
            s3.secret_access_key = Some("test-secret".into());
            s3.max_connections = Some(c);
            assert_eq!(
                backend_from_config(&s3).unwrap().profile(),
                remote,
                "s3 c={c}"
            );
        }
    }

    #[test]
    fn test_bare_absolute_path() {
        let parsed = parse_repo_url("/backups/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "/backups/repo".into()
            }
        );
    }

    #[test]
    fn test_bare_relative_path() {
        let parsed = parse_repo_url("./my-repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "./my-repo".into()
            }
        );
    }

    #[test]
    fn test_bare_relative_path_without_dot_prefix() {
        let parsed = parse_repo_url("my-repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "my-repo".into()
            }
        );
    }

    #[test]
    fn test_file_url() {
        let parsed = parse_repo_url("file:///backups/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "/backups/repo".into()
            }
        );
    }

    #[test]
    fn test_s3_bucket_only() {
        let err = parse_repo_url("s3://my-bucket").unwrap_err();
        assert!(err.to_string().contains("must include a bucket"));
    }

    #[test]
    fn test_s3_bucket_with_prefix() {
        let parsed = parse_repo_url("s3://s3.us-east-1.amazonaws.com/my-bucket/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vykar".into(),
                endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            }
        );
    }

    #[test]
    fn test_s3_bucket_with_nested_prefix() {
        let parsed =
            parse_repo_url("s3://s3.us-east-1.amazonaws.com/my-bucket/backups/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "backups/vykar".into(),
                endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            }
        );
    }

    #[test]
    fn test_s3_custom_endpoint_with_port() {
        let parsed = parse_repo_url("s3://minio.local:9000/my-bucket/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vykar".into(),
                endpoint: "https://minio.local:9000".into(),
            }
        );
    }

    #[test]
    fn test_s3_custom_endpoint_with_dot() {
        let parsed = parse_repo_url("s3://s3.example.com/my-bucket/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vykar".into(),
                endpoint: "https://s3.example.com".into(),
            }
        );
    }

    #[test]
    fn test_s3_custom_endpoint_missing_bucket() {
        let err = parse_repo_url("s3://minio.local:9000").unwrap_err();
        assert!(err.to_string().contains("must include a bucket"));
    }

    #[test]
    fn test_s3_http_endpoint_scheme() {
        let parsed = parse_repo_url("s3+http://minio.local:9000/my-bucket/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vykar".into(),
                endpoint: "http://minio.local:9000".into(),
            }
        );
    }

    #[test]
    fn test_sftp_basic() {
        let parsed = parse_repo_url("sftp://nas.local/backups/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Sftp {
                user: None,
                host: "nas.local".into(),
                port: None,
                path: "/backups/vykar".into(),
            }
        );
    }

    #[test]
    fn test_sftp_with_user() {
        let parsed = parse_repo_url("sftp://backup@nas.local/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Sftp {
                user: Some("backup".into()),
                host: "nas.local".into(),
                port: None,
                path: "/repo".into(),
            }
        );
    }

    #[test]
    fn test_sftp_with_user_and_port() {
        let parsed = parse_repo_url("sftp://backup@nas.local:2222/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Sftp {
                user: Some("backup".into()),
                host: "nas.local".into(),
                port: Some(2222),
                path: "/repo".into(),
            }
        );
    }

    /// A URL cannot carry a path relative to the login directory: the dot
    /// segment is dropped (why the probe has `--relative-root`).
    #[test]
    fn test_sftp_dot_segment_becomes_absolute() {
        let parsed = parse_repo_url("sftp://backup@nas.local/./repo").unwrap();
        assert!(
            matches!(&parsed, ParsedUrl::Sftp { path, .. } if path == "/repo"),
            "{parsed:?}"
        );
    }

    #[test]
    fn test_https_rest() {
        let parsed = parse_repo_url("https://backup.example.com/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Rest {
                url: "https://backup.example.com/repo".into(),
            }
        );
    }

    #[test]
    fn test_http_rest() {
        let parsed = parse_repo_url("http://localhost:8080/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Rest {
                url: "http://localhost:8080/repo".into(),
            }
        );
    }

    #[test]
    fn test_unsupported_scheme() {
        let err = parse_repo_url("ftp://host/path").unwrap_err();
        assert!(err.to_string().contains("unsupported URL scheme"));
    }

    #[test]
    fn test_invalid_url() {
        let err = parse_repo_url("http://[::1").unwrap_err();
        assert!(err.to_string().contains("invalid repository URL"));
    }

    #[test]
    fn test_empty_url_rejected() {
        let err = parse_repo_url("   ").unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    fn test_config(url: &str) -> StorageConfig {
        StorageConfig {
            url: url.to_string(),
            region: None,
            access_key_id: None,
            secret_access_key: None,
            sftp_key: None,
            sftp_agent: None,
            sftp_known_hosts: None,
            max_connections: None,
            sftp_timeout: None,
            access_token: None,
            allow_insecure_http: false,
            retry: RetryConfig::default(),
            s3_soft_delete: false,
            s3_conditional_writes: S3ConditionalWrites::Auto,
            cancel: CancelToken::new(),
        }
    }

    #[test]
    fn test_backend_rejects_http_rest_by_default() {
        let cfg = test_config("http://localhost:8080/repo");
        let err = match backend_from_config(&cfg) {
            Ok(_) => panic!("expected HTTP REST URL to be rejected"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("repository.url"));
        assert!(msg.contains("repository.allow_insecure_http: true"));
    }

    #[test]
    fn test_backend_rejects_http_s3_endpoint_by_default() {
        let mut cfg = test_config("s3+http://minio.local:9000/my-bucket/vykar");
        cfg.access_key_id = Some("test-key".into());
        cfg.secret_access_key = Some("test-secret".into());
        let err = match backend_from_config(&cfg) {
            Ok(_) => panic!("expected HTTP S3 endpoint to be rejected"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("repository.url"));
        assert!(msg.contains("repository.allow_insecure_http: true"));
    }

    #[test]
    fn test_backend_allows_http_rest_when_opted_in() {
        let mut cfg = test_config("http://localhost:8080/repo");
        cfg.allow_insecure_http = true;
        let backend = backend_from_config(&cfg);
        assert!(
            backend.is_ok(),
            "expected HTTP REST URL to be allowed when opted in"
        );
    }

    #[test]
    fn test_backend_allows_http_s3_endpoint_when_opted_in() {
        let mut cfg = test_config("s3+http://minio.local:9000/my-bucket/vykar");
        cfg.allow_insecure_http = true;
        cfg.access_key_id = Some("test-key".into());
        cfg.secret_access_key = Some("test-secret".into());
        let backend = backend_from_config(&cfg);
        assert!(
            backend.is_ok(),
            "expected HTTP S3 endpoint to be allowed when opted in"
        );
    }

    #[test]
    fn default_retry_reaches_one_minute_average_window() {
        let cfg = RetryConfig::default();
        assert_eq!(cfg.max_retries, 5);
        let mut delay = cfg.retry_delay_ms;
        let mut base: u64 = 0;
        let mut avg_x2: u64 = 0;
        for _ in 0..cfg.max_retries {
            base += delay;
            avg_x2 += delay * 3;
            delay = (delay * 2).min(cfg.retry_max_delay_ms);
        }
        let avg_ms = avg_x2 / 2;
        assert!(
            base >= 45_000,
            "default backoff base should span >=45s, got {base}ms"
        );
        assert!(
            (55_000..=85_000).contains(&avg_ms),
            "expected avg cumulative backoff near 1 minute, got {avg_ms}ms"
        );
    }
}
