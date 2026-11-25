//! WebDAV server for browsing snapshot contents.
//!
//! Starts a read-only WebDAV server exposing snapshot contents as a virtual
//! filesystem. macOS Finder can mount it natively via "Connect to Server".

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use dav_server::DavHandler;
use dav_server::davpath::DavPath;
use dav_server::fs::*;
use futures_util::stream;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use lru::LruCache;
use percent_encoding::percent_decode_str;
use tokio::net::TcpListener;

use crate::commands::list as list_cmd;
use crate::commands::listing::ListingView;
use crate::config::VykarConfig;
use crate::repo::control::CancelToken;
use crate::repo::{Repo, TreeChunkSource};
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use super::util::open_repo;

// ─── VFS tree ──────────────────────────────────────────────────────────────

type ChunkCache = Arc<Mutex<LruCache<ChunkId, Arc<Vec<u8>>>>>;

/// A node of the virtual filesystem. Directories are shared, so a node is
/// cheap to clone.
#[derive(Clone)]
enum VfsNode {
    Dir(Arc<VfsDir>),
    File {
        chunks: Arc<Vec<ChunkRef>>,
        meta: VfsMeta,
    },
    Symlink {
        _target: String,
        meta: VfsMeta,
    },
}

/// A directory's children by name.
type Children = Arc<HashMap<String, VfsNode>>;

/// A directory: its metadata and its children, known up front or read on
/// first use.
struct VfsDir {
    meta: VfsMeta,
    children: DirChildren,
}

enum DirChildren {
    /// Known up front (whole-built snapshots, the multi-snapshot root, tests).
    Ready(Children),
    /// Read on first use; a failed load is not cached.
    Lazy {
        loader: DirLoader,
        loaded: LoadOnce<Children>,
    },
}

/// What a lazy directory reads its children from.
enum DirLoader {
    /// The root of a snapshot not opened yet.
    Snapshot { repo: Arc<Repo>, name: String },
    /// A directory of a snapshot with a usable key index: its raw paths
    /// (several when non-UTF-8 names share a display name; an empty path is
    /// the snapshot root) and its display path from the snapshot root.
    Indexed {
        snap: Arc<IndexedSnapshot>,
        raws: Vec<Vec<u8>>,
        display: String,
    },
    /// Tests: blocks until the channel delivers, then has no children.
    #[cfg(test)]
    Gate(Mutex<std::sync::mpsc::Receiver<()>>),
}

/// A value loaded at most once. Readers of a loaded value take no lock; a
/// load holds only this value's own lock, so concurrent requests for the
/// same value wait while other values load in parallel. A failed load stores
/// nothing.
struct LoadOnce<T> {
    value: OnceLock<T>,
    loading: Mutex<()>,
}

impl<T> LoadOnce<T> {
    fn new() -> Self {
        Self {
            value: OnceLock::new(),
            loading: Mutex::new(()),
        }
    }

    fn get(&self) -> Option<&T> {
        self.value.get()
    }

    fn get_or_load(&self, load: impl FnOnce() -> Result<T>) -> Result<&T> {
        if let Some(v) = self.value.get() {
            return Ok(v);
        }
        let _loading = self.loading.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(v) = self.value.get() {
            return Ok(v);
        }
        let v = load()?;
        Ok(self.value.get_or_init(|| v))
    }
}

/// An opened snapshot whose key index is usable.
struct IndexedSnapshot {
    repo: Arc<Repo>,
    meta: SnapshotMeta,
    /// Non-empty segment roots of the index (implicit ancestors come from
    /// them).
    roots: Vec<Vec<u8>>,
    /// Set once a directory read fell back to a scan: further directories
    /// come from the snapshot's tree built whole.
    degraded: AtomicBool,
    whole: LoadOnce<VfsNode>,
}

impl VfsDir {
    fn ready(children: HashMap<String, VfsNode>, meta: VfsMeta) -> Self {
        Self {
            meta,
            children: DirChildren::Ready(Arc::new(children)),
        }
    }

    fn lazy(loader: DirLoader, meta: VfsMeta) -> Self {
        Self {
            meta,
            children: DirChildren::Lazy {
                loader,
                loaded: LoadOnce::new(),
            },
        }
    }

    /// The children, loading them on first use (blocking I/O).
    fn children(&self) -> Result<Children> {
        match &self.children {
            DirChildren::Ready(c) => Ok(c.clone()),
            DirChildren::Lazy { loader, loaded } => loaded.get_or_load(|| loader.load()).cloned(),
        }
    }

    /// The children if already known, without I/O or waiting.
    fn children_if_loaded(&self) -> Option<Children> {
        match &self.children {
            DirChildren::Ready(c) => Some(c.clone()),
            DirChildren::Lazy { loaded, .. } => loaded.get().cloned(),
        }
    }

    #[cfg(test)]
    fn is_loaded(&self) -> bool {
        self.children_if_loaded().is_some()
    }
}

impl DirLoader {
    fn load(&self) -> Result<Children> {
        match self {
            DirLoader::Snapshot { repo, name } => load_snapshot_root(repo, name),
            DirLoader::Indexed {
                snap,
                raws,
                display,
            } => load_indexed(snap, raws, display),
            #[cfg(test)]
            DirLoader::Gate(rx) => {
                let _ = rx.lock().unwrap_or_else(|e| e.into_inner()).recv();
                Ok(Arc::new(HashMap::new()))
            }
        }
    }
}

/// Metadata for a VFS node, compatible with WebDAV metadata requirements.
#[derive(Debug, Clone, PartialEq)]
struct VfsMeta {
    size: u64,
    mtime: SystemTime,
    is_dir: bool,
    is_symlink: bool,
}

impl VfsMeta {
    fn from_item(item: &Item) -> Self {
        let mtime = if item.mtime >= 0 {
            UNIX_EPOCH + std::time::Duration::from_nanos(item.mtime as u64)
        } else {
            UNIX_EPOCH
        };
        Self {
            size: item.size,
            mtime,
            is_dir: item.entry_type == ItemType::Directory,
            is_symlink: item.entry_type == ItemType::Symlink,
        }
    }

    fn dir_default() -> Self {
        Self {
            size: 0,
            mtime: UNIX_EPOCH,
            is_dir: true,
            is_symlink: false,
        }
    }
}

fn node_meta(node: &VfsNode) -> &VfsMeta {
    match node {
        VfsNode::Dir(d) => &d.meta,
        VfsNode::File { meta, .. } => meta,
        VfsNode::Symlink { meta, .. } => meta,
    }
}

/// The non-empty segment roots of `meta`'s key index, or `None` when it has
/// no usable index.
fn indexed_roots(meta: &SnapshotMeta) -> Option<Vec<Vec<u8>>> {
    let view = ListingView::new(meta);
    let index = view.index().ok()?;
    Some(
        index
            .segments
            .iter()
            .filter(|s| !s.root.is_empty())
            .map(|s| s.root.to_vec())
            .collect(),
    )
}

/// The root directory of snapshot `name`: read through its key index one
/// directory at a time when it has a usable one (the root's children are
/// read here), else its whole tree (built here); the item count is returned
/// for a whole build. A pointer the chunk index lacks is refused up front.
fn open_snapshot(repo: &Arc<Repo>, name: &str) -> Result<(VfsNode, Option<usize>)> {
    let meta = list_cmd::load_snapshot_meta(repo, name)?;
    list_cmd::resolve_from_index(repo.chunk_index(), &meta.item_ptrs)?;
    match indexed_roots(&meta) {
        Some(roots) => {
            let snap = Arc::new(IndexedSnapshot {
                repo: Arc::clone(repo),
                meta,
                roots,
                degraded: AtomicBool::new(false),
                whole: LoadOnce::new(),
            });
            let children = load_indexed(&snap, &[Vec::new()], "")?;
            Ok((
                VfsNode::Dir(Arc::new(VfsDir {
                    meta: VfsMeta::dir_default(),
                    children: DirChildren::Ready(children),
                })),
                None,
            ))
        }
        None => {
            let (root, count) = build_whole(repo, &meta)?;
            Ok((root, Some(count)))
        }
    }
}

/// The children of snapshot `name`'s root directory.
fn load_snapshot_root(repo: &Arc<Repo>, name: &str) -> Result<Children> {
    match open_snapshot(repo, name)?.0 {
        VfsNode::Dir(d) => Ok(d.children_if_loaded().unwrap_or_default()),
        _ => Ok(Children::default()),
    }
}

/// A child while a directory's children are collected.
enum ChildBuild {
    Dir {
        meta: VfsMeta,
        raws: Vec<Vec<u8>>,
    },
    File {
        chunks: Vec<ChunkRef>,
        meta: VfsMeta,
    },
    Symlink {
        target: String,
        meta: VfsMeta,
    },
}

/// `a/b` (`b` when `a` is empty).
fn join_path(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut p = a.to_vec();
    if !p.is_empty() {
        p.push(b'/');
    }
    p.extend_from_slice(b);
    p
}

/// Children of the directory whose raw paths are `raws` (display path
/// `display`): the implicit ancestors of segment roots under it, then its
/// entries through the key index (`dir_entries`), merged as
/// `insert_into_tree` merges.
fn load_indexed(
    snap: &Arc<IndexedSnapshot>,
    raws: &[Vec<u8>],
    display_path: &str,
) -> Result<Children> {
    if snap.degraded.load(AtomicOrdering::Relaxed) {
        return whole_children(snap, display_path);
    }
    let mut build: HashMap<String, ChildBuild> = HashMap::new();
    let view = ListingView::new(&snap.meta);
    let mut done: Vec<&[u8]> = Vec::new();
    for raw in raws {
        if done.contains(&raw.as_slice()) {
            continue;
        }
        done.push(raw);
        for root in &snap.roots {
            let rest = if raw.is_empty() {
                Some(root.as_slice())
            } else {
                root.strip_prefix(raw.as_slice())
                    .and_then(|r| r.strip_prefix(b"/"))
            };
            let Some(c) = rest
                .and_then(|r| r.split(|b| *b == b'/').next())
                .filter(|c| !c.is_empty())
            else {
                continue;
            };
            let child_raw = join_path(raw, c);
            let name = String::from_utf8_lossy(c).into_owned();
            match build.get_mut(&name) {
                Some(ChildBuild::Dir { raws, .. }) => {
                    if !raws.contains(&child_raw) {
                        raws.push(child_raw);
                    }
                }
                Some(_) => {}
                None => {
                    build.insert(
                        name,
                        ChildBuild::Dir {
                            meta: VfsMeta::dir_default(),
                            raws: vec![child_raw],
                        },
                    );
                }
            }
        }
        let mut entries = 0usize;
        let read = view.dir_entries(&snap.repo, raw, &CancelToken::new(), |item| {
            entries += 1;
            let Some(name) = item.path.rsplit('/').next().filter(|n| !n.is_empty()) else {
                return Ok(());
            };
            let meta = VfsMeta::from_item(&item);
            match item.entry_type {
                ItemType::Directory => {
                    if let Some(ChildBuild::Dir { meta: m, raws }) = build.get_mut(name) {
                        *m = meta;
                        if !raws.iter().any(|r| r.as_slice() == item.path_bytes()) {
                            raws.push(item.path_bytes().to_vec());
                        }
                    } else {
                        build.insert(
                            name.to_string(),
                            ChildBuild::Dir {
                                meta,
                                raws: vec![item.path_bytes().to_vec()],
                            },
                        );
                    }
                }
                ItemType::RegularFile => {
                    build.insert(
                        name.to_string(),
                        ChildBuild::File {
                            chunks: item.chunks,
                            meta,
                        },
                    );
                }
                ItemType::Symlink => {
                    build.insert(
                        name.to_string(),
                        ChildBuild::Symlink {
                            target: item.link_target.unwrap_or_default(),
                            meta,
                        },
                    );
                }
            }
            Ok(())
        })?;
        tracing::debug!(
            "mount: snapshot {} dir {:?}: {} entries, via index {}, {} tree chunks",
            snap.meta.name,
            display_path,
            entries,
            read.via_index,
            read.chunks_read
        );
        if !read.via_index {
            tracing::warn!(
                "mount: snapshot {}: a directory read fell back to a scan ({}); \
                 reading its further directories from the whole listing",
                snap.meta.name,
                read.fallback.as_deref().unwrap_or("unknown reason")
            );
            snap.degraded.store(true, AtomicOrdering::Relaxed);
        }
    }
    let children = build
        .into_iter()
        .map(|(name, child)| {
            let node = match child {
                ChildBuild::Dir { meta, raws } => {
                    let display = if display_path.is_empty() {
                        name.clone()
                    } else {
                        format!("{display_path}/{name}")
                    };
                    VfsNode::Dir(Arc::new(VfsDir::lazy(
                        DirLoader::Indexed {
                            snap: Arc::clone(snap),
                            raws,
                            display,
                        },
                        meta,
                    )))
                }
                ChildBuild::File { chunks, meta } => VfsNode::File {
                    chunks: Arc::new(chunks),
                    meta,
                },
                ChildBuild::Symlink { target, meta } => VfsNode::Symlink {
                    _target: target,
                    meta,
                },
            };
            (name, node)
        })
        .collect();
    Ok(Arc::new(children))
}

/// The children of display path `display` in the snapshot's tree built
/// whole (built on first use).
fn whole_children(snap: &IndexedSnapshot, display: &str) -> Result<Children> {
    let root = snap
        .whole
        .get_or_load(|| build_whole(&snap.repo, &snap.meta).map(|(root, _)| root))?;
    Ok(match lookup_loaded(root, display.as_bytes()) {
        Ok(Some(VfsNode::Dir(d))) => d.children_if_loaded().unwrap_or_default(),
        _ => Children::default(),
    })
}

/// A node of a tree under construction (the whole-built tree).
enum BuildNode {
    Dir {
        children: HashMap<String, BuildNode>,
        meta: VfsMeta,
    },
    File {
        chunks: Vec<ChunkRef>,
        meta: VfsMeta,
    },
    Symlink {
        target: String,
        meta: VfsMeta,
    },
}

impl BuildNode {
    fn finish(self) -> VfsNode {
        match self {
            BuildNode::Dir { children, meta } => VfsNode::Dir(Arc::new(VfsDir::ready(
                children
                    .into_iter()
                    .map(|(name, child)| (name, child.finish()))
                    .collect(),
                meta,
            ))),
            BuildNode::File { chunks, meta } => VfsNode::File {
                chunks: Arc::new(chunks),
                meta,
            },
            BuildNode::Symlink { target, meta } => VfsNode::Symlink {
                _target: target,
                meta,
            },
        }
    }
}

/// Insert `item` into a tree under construction.
fn insert_item(root: &mut HashMap<String, BuildNode>, item: &Item) {
    let path = item.path.trim_start_matches('/');
    if path.is_empty() {
        return;
    }
    let components: Vec<&str> = path.split('/').collect();
    insert_into_tree(root, &components, item);
}

/// Snapshot `meta`'s whole tree with every directory known, read a batch of
/// tree chunks at a time; also returns the item count.
fn build_whole(repo: &Repo, meta: &SnapshotMeta) -> Result<(VfsNode, usize)> {
    let mut root = HashMap::new();
    let mut count = 0usize;
    list_cmd::for_each_item_by_chunk(
        repo,
        &meta.item_ptrs,
        TreeChunkSource::CacheFirst,
        list_cmd::LISTING_BATCH_CHUNKS,
        list_cmd::LISTING_BATCH_STORED_BYTES,
        &CancelToken::new(),
        |item| {
            count += 1;
            insert_item(&mut root, &item);
            Ok(())
        },
    )?;
    tracing::debug!("mount: snapshot {} built whole: {count} items", meta.name);
    let tree = BuildNode::Dir {
        children: root,
        meta: VfsMeta::dir_default(),
    };
    Ok((tree.finish(), count))
}

/// Build a VFS tree from a list of snapshot items.
#[cfg(test)]
fn build_vfs_tree(items: &[Item]) -> VfsNode {
    let mut root = HashMap::new();
    for item in items {
        insert_item(&mut root, item);
    }
    BuildNode::Dir {
        children: root,
        meta: VfsMeta::dir_default(),
    }
    .finish()
}

fn insert_into_tree(children: &mut HashMap<String, BuildNode>, components: &[&str], item: &Item) {
    let (head, tail) = match components.split_first() {
        Some(parts) => parts,
        None => return,
    };

    if tail.is_empty() {
        let name = (*head).to_string();
        // If this is a directory and one already exists as an intermediate,
        // just update its metadata rather than replacing it (which would lose children).
        if item.entry_type == ItemType::Directory
            && let Some(BuildNode::Dir { meta, .. }) = children.get_mut(&name)
        {
            *meta = VfsMeta::from_item(item);
            return;
        }
        let node = match item.entry_type {
            ItemType::Directory => BuildNode::Dir {
                children: HashMap::new(),
                meta: VfsMeta::from_item(item),
            },
            ItemType::RegularFile => BuildNode::File {
                chunks: item.chunks.clone(),
                meta: VfsMeta::from_item(item),
            },
            ItemType::Symlink => BuildNode::Symlink {
                target: item.link_target.clone().unwrap_or_default(),
                meta: VfsMeta::from_item(item),
            },
        };
        children.insert(name, node);
    } else {
        let dir_name = (*head).to_string();
        let entry = children.entry(dir_name).or_insert_with(|| BuildNode::Dir {
            children: HashMap::new(),
            meta: VfsMeta::dir_default(),
        });
        if let BuildNode::Dir {
            children: dir_children,
            ..
        } = entry
        {
            insert_into_tree(dir_children, tail, item);
        }
    }
}

/// The components of a node path (DavPath bytes): UTF-8 required, empty
/// components skipped.
fn path_components(path: &[u8]) -> Option<impl Iterator<Item = &str>> {
    let s = std::str::from_utf8(path).ok()?;
    Some(s.split('/').filter(|c| !c.is_empty()))
}

/// The node at `path` (DavPath bytes, UTF-8, empty components skipped),
/// loading directories on the way (blocking I/O). The node itself is not
/// loaded.
fn lookup(root: &VfsNode, path: &[u8]) -> Result<Option<VfsNode>> {
    let Some(components) = path_components(path) else {
        return Ok(None);
    };
    let mut current = root.clone();
    for component in components {
        let next = match &current {
            VfsNode::Dir(d) => d.children()?.get(component).cloned(),
            _ => None,
        };
        match next {
            Some(n) => current = n,
            None => return Ok(None),
        }
    }
    Ok(Some(current))
}

/// A directory on the way of [`lookup_loaded`] is not loaded yet.
struct NotLoaded;

/// [`lookup`] without I/O: `Err(NotLoaded)` when a directory on the way is
/// not loaded yet.
fn lookup_loaded(root: &VfsNode, path: &[u8]) -> std::result::Result<Option<VfsNode>, NotLoaded> {
    let Some(components) = path_components(path) else {
        return Ok(None);
    };
    let mut current = root.clone();
    for component in components {
        let next = match &current {
            VfsNode::Dir(d) => d
                .children_if_loaded()
                .ok_or(NotLoaded)?
                .get(component)
                .cloned(),
            _ => None,
        };
        match next {
            Some(n) => current = n,
            None => return Ok(None),
        }
    }
    Ok(Some(current))
}

/// [`lookup_loaded`], else [`lookup`] inside `tokio::task::block_in_place`
/// (the server's multi-thread runtime; outside a runtime it just runs).
fn lookup_in_runtime(root: &VfsNode, path: &[u8]) -> Result<Option<VfsNode>> {
    if let Ok(found) = lookup_loaded(root, path) {
        return Ok(found);
    }
    tokio::task::block_in_place(|| lookup(root, path))
}

/// A directory's children, loading them inside `block_in_place` when they
/// are not known yet.
fn children_in_runtime(dir: &VfsDir) -> Result<Children> {
    match dir.children_if_loaded() {
        Some(c) => Ok(c),
        None => tokio::task::block_in_place(|| dir.children()),
    }
}

// ─── HTML Web UI ──────────────────────────────────────────────────────────

fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

fn percent_encode_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{:02X}", b));
            }
        }
    }
    out
}

fn format_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes == 0 {
        return "0 B".to_string();
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "size formatting for human-readable display"
    )]
    let mut size = bytes as f64;
    for unit in UNITS {
        if size < 1024.0 {
            return if *unit == "B" {
                format!("{size:.0} {unit}")
            } else {
                format!("{size:.1} {unit}")
            };
        }
        size /= 1024.0;
    }
    format!("{size:.1} PiB")
}

fn format_mtime(t: SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Local> = t.into();
    dt.format("%Y-%m-%d %H:%M").to_string()
}

/// Determine what to do with an incoming request: serve HTML, redirect, or
/// pass through to the WebDAV handler.
#[derive(Debug)]
enum BrowserAction {
    ServeHtml,
    Redirect(String),
    PassThrough,
}

fn classify_browser_request<B>(req: &hyper::Request<B>, root: &VfsNode) -> BrowserAction {
    if req.method() != hyper::Method::GET {
        return BrowserAction::PassThrough;
    }
    let is_browser = req
        .headers()
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.contains("text/html"));
    if !is_browser {
        return BrowserAction::PassThrough;
    }
    let raw_path = req.uri().path();
    let decoded = percent_decode_str(raw_path).decode_utf8_lossy();
    match lookup_in_runtime(root, decoded.as_bytes()) {
        Ok(Some(VfsNode::Dir(_))) => {
            if raw_path == "/" || raw_path.ends_with('/') {
                BrowserAction::ServeHtml
            } else {
                BrowserAction::Redirect(format!("{raw_path}/"))
            }
        }
        _ => BrowserAction::PassThrough,
    }
}

/// The HTML listing of directory `path`; `Err` when a directory on the way
/// cannot be read.
fn render_directory_html(path: &str, root: &VfsNode, is_snapshot_root: bool) -> Result<String> {
    let children = match lookup_in_runtime(root, path.as_bytes())? {
        Some(VfsNode::Dir(d)) => children_in_runtime(&d)?,
        _ => return Ok(String::from("<html><body><h1>Not Found</h1></body></html>")),
    };

    // Collect and sort entries: dirs first, then files, alphabetical within each
    let mut dirs: Vec<(&String, &VfsNode)> = Vec::new();
    let mut files: Vec<(&String, &VfsNode)> = Vec::new();
    for (name, child) in children.iter() {
        match child {
            VfsNode::Dir(_) => dirs.push((name, child)),
            _ => files.push((name, child)),
        }
    }
    if is_snapshot_root {
        // Snapshot root: sort by mtime descending (newest first), name tie-breaker
        dirs.sort_by(|a, b| {
            node_meta(b.1)
                .mtime
                .cmp(&node_meta(a.1).mtime)
                .then_with(|| a.0.to_lowercase().cmp(&b.0.to_lowercase()))
        });
    } else {
        dirs.sort_by_key(|a| a.0.to_lowercase());
    }
    files.sort_by_key(|a| a.0.to_lowercase());

    let display_path = if path.is_empty() || path == "/" {
        "/"
    } else {
        path
    };
    let title = html_escape(display_path);

    // Build breadcrumbs
    let mut breadcrumbs = String::from(r#"<a href="/">root</a>"#);
    if path != "/" && !path.is_empty() {
        let trimmed = path.trim_matches('/');
        let mut href = String::from("/");
        for part in trimmed.split('/') {
            if part.is_empty() {
                continue;
            }
            href.push_str(&percent_encode_path(part));
            href.push('/');
            breadcrumbs.push_str(&format!(
                r#" / <a href="{}">{}</a>"#,
                href,
                html_escape(part),
            ));
        }
    }

    let mut rows = String::new();

    // Parent directory link
    if path != "/" && !path.is_empty() {
        rows.push_str(
            r#"<tr data-parent="1"><td class="icon">📁</td><td data-sort="-1"><a href="../">../</a></td><td class="size" data-sort="-1">—</td><td class="mtime" data-sort="-1"></td></tr>
"#,
        );
    }

    for (name, child) in &dirs {
        let meta = node_meta(child);
        let mtime = format_mtime(meta.mtime);
        let mtime_ms = meta
            .mtime
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        let name_sort = html_escape(&name.to_lowercase());
        rows.push_str(&format!(
            r#"<tr><td class="icon">📁</td><td data-sort="{name_sort}"><a href="{}/"><strong>{}/</strong></a></td><td class="size" data-sort="0" data-dir="1">—</td><td class="mtime" data-sort="{mtime_ms}">{}</td></tr>
"#,
            percent_encode_path(name),
            html_escape(name),
            mtime,
        ));
    }

    for (name, child) in &files {
        let meta = node_meta(child);
        let mtime = format_mtime(meta.mtime);
        let mtime_ms = meta
            .mtime
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        let name_sort = html_escape(&name.to_lowercase());
        let (icon, display) = match child {
            VfsNode::Symlink { _target, .. } => (
                "🔗",
                format!("{} → {}", html_escape(name), html_escape(_target)),
            ),
            _ => ("📄", html_escape(name)),
        };
        rows.push_str(&format!(
            r#"<tr><td class="icon">{icon}</td><td data-sort="{name_sort}"><a href="{}">{display}</a></td><td class="size" data-sort="{}">{}</td><td class="mtime" data-sort="{mtime_ms}">{}</td></tr>
"#,
            percent_encode_path(name),
            meta.size,
            format_size(meta.size),
            mtime,
        ));
    }

    let (sort_col, sort_dir) = if is_snapshot_root {
        ("3", "desc")
    } else {
        ("1", "asc")
    };

    Ok(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>vykar — {title}</title>
<style>
:root {{ --bg: #fff; --fg: #1a1a1a; --link: #0066cc; --border: #e0e0e0; --hover: #f5f5f5; --muted: #666; }}
@media (prefers-color-scheme: dark) {{
  :root {{ --bg: #1a1a1a; --fg: #e0e0e0; --link: #6cb6ff; --border: #333; --hover: #252525; --muted: #999; }}
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; background: var(--bg); color: var(--fg); padding: 2rem; max-width: 960px; margin: 0 auto; }}
h1 {{ font-size: 1.1rem; font-weight: 600; margin-bottom: 0.5rem; word-break: break-all; }}
.breadcrumbs {{ font-size: 0.85rem; color: var(--muted); margin-bottom: 1.5rem; }}
.breadcrumbs a {{ color: var(--link); text-decoration: none; }}
.breadcrumbs a:hover {{ text-decoration: underline; }}
table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
th {{ text-align: left; padding: 0.5rem 0.75rem; border-bottom: 2px solid var(--border); font-weight: 600; color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; }}
td {{ padding: 0.4rem 0.75rem; border-bottom: 1px solid var(--border); }}
tr:hover {{ background: var(--hover); }}
a {{ color: var(--link); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
.icon {{ width: 1.5rem; text-align: center; }}
.size {{ text-align: right; color: var(--muted); white-space: nowrap; }}
.mtime {{ color: var(--muted); white-space: nowrap; }}
th.sortable {{ cursor: pointer; user-select: none; }}
th.sortable:hover {{ color: var(--fg); }}
.sort-arrow {{ font-size: 0.7em; margin-left: 0.3em; }}
th.sort-asc .sort-arrow::after {{ content: "\25B2"; }}
th.sort-desc .sort-arrow::after {{ content: "\25BC"; }}
footer {{ margin-top: 2rem; font-size: 0.75rem; color: var(--muted); }}
</style>
</head>
<body>
<h1>{title}</h1>
<div class="breadcrumbs">{breadcrumbs}</div>
<table data-sort-col="{sort_col}" data-sort-dir="{sort_dir}">
<thead><tr><th></th><th class="sortable" data-col="1" data-type="text">Name <span class="sort-arrow"></span></th><th class="sortable" data-col="2" data-type="num" style="text-align:right">Size <span class="sort-arrow"></span></th><th class="sortable" data-col="3" data-type="num">Modified <span class="sort-arrow"></span></th></tr></thead>
<tbody>
{rows}</tbody>
</table>
<footer>vykar backup — WebDAV + Web UI</footer>
<script>
(function() {{
  var table = document.querySelector("table");
  var thead = table.querySelector("thead tr");
  var tbody = table.querySelector("tbody");
  var col = parseInt(table.dataset.sortCol, 10);
  var dir = table.dataset.sortDir;

  function setArrow(c, d) {{
    thead.querySelectorAll("th.sortable").forEach(function(th) {{
      th.classList.remove("sort-asc", "sort-desc");
    }});
    var th = thead.querySelector('th[data-col="' + c + '"]');
    if (th) th.classList.add("sort-" + d);
  }}
  setArrow(col, dir);

  thead.addEventListener("click", function(e) {{
    var th = e.target.closest("th.sortable");
    if (!th) return;
    var c = parseInt(th.dataset.col, 10);
    var type = th.dataset.type;
    if (c === col) {{
      dir = dir === "asc" ? "desc" : "asc";
    }} else {{
      col = c;
      dir = type === "text" ? "asc" : "desc";
    }}
    setArrow(col, dir);
    sortRows(col, dir, type);
  }});

  function sortRows(c, d, type) {{
    var rows = Array.from(tbody.querySelectorAll("tr"));
    var parent = [];
    var rest = [];
    rows.forEach(function(r) {{
      if (r.dataset.parent === "1") parent.push(r);
      else rest.push(r);
    }});
    rest.sort(function(a, b) {{
      var ca = a.cells[c];
      var cb = b.cells[c];
      var va = ca.dataset.sort;
      var vb = cb.dataset.sort;
      var result;
      if (type === "num") {{
        var na = parseFloat(va) || 0;
        var nb = parseFloat(vb) || 0;
        result = na - nb;
        if (result === 0) {{
          result = (a.cells[1].dataset.sort || "").localeCompare(b.cells[1].dataset.sort || "");
        }}
      }} else {{
        var da = ca.dataset.dir === "1" ? 0 : 1;
        var db = cb.dataset.dir === "1" ? 0 : 1;
        if (da !== db) return da - db;
        result = (va || "").localeCompare(vb || "");
      }}
      return d === "asc" ? result : -result;
    }});
    parent.concat(rest).forEach(function(r) {{ tbody.appendChild(r); }});
  }}
}})();
</script>
</body>
</html>"#
    ))
}

// ─── DavMetaData ───────────────────────────────────────────────────────────

impl DavMetaData for VfsMeta {
    fn len(&self) -> u64 {
        self.size
    }

    fn modified(&self) -> FsResult<SystemTime> {
        Ok(self.mtime)
    }

    fn is_dir(&self) -> bool {
        self.is_dir
    }

    fn is_symlink(&self) -> bool {
        self.is_symlink
    }
}

// ─── DavDirEntry ───────────────────────────────────────────────────────────

struct VykarDirEntry {
    name: String,
    meta: VfsMeta,
}

impl DavDirEntry for VykarDirEntry {
    fn name(&self) -> Vec<u8> {
        self.name.as_bytes().to_vec()
    }

    fn metadata(&self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let meta = self.meta.clone();
        Box::pin(async move { Ok(Box::new(meta) as Box<dyn DavMetaData>) })
    }
}

// ─── DavFileSystem ─────────────────────────────────────────────────────────

/// Read-only WebDAV filesystem backed by vykar snapshot data.
#[derive(Clone)]
struct VykarDavFs {
    root: VfsNode,
    repo: Arc<Repo>,
    cache: Arc<Mutex<LruCache<ChunkId, Arc<Vec<u8>>>>>,
}

impl VykarDavFs {
    /// The node at `path`, loading directories on the way.
    fn lookup(&self, path: &DavPath) -> FsResult<VfsNode> {
        match lookup_in_runtime(&self.root, path.as_bytes()) {
            Ok(Some(node)) => Ok(node),
            Ok(None) => Err(FsError::NotFound),
            Err(e) => {
                tracing::warn!("mount: {path}: {e}");
                Err(FsError::GeneralFailure)
            }
        }
    }
}

impl DavFileSystem for VykarDavFs {
    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        Box::pin(async move {
            let node = self.lookup(path)?;
            Ok(Box::new(node_meta(&node).clone()) as Box<dyn DavMetaData>)
        })
    }

    // The snapshot VFS never follows links, so `metadata` already describes the link itself.
    fn symlink_metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        DavFileSystem::metadata(self, path)
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        Box::pin(async move {
            let node = self.lookup(path)?;
            match node {
                VfsNode::Dir(d) => {
                    let children = children_in_runtime(&d).map_err(|e| {
                        tracing::warn!("mount: {path}: {e}");
                        FsError::GeneralFailure
                    })?;
                    let entries: Vec<_> = children
                        .iter()
                        .map(|(name, child)| {
                            Ok(Box::new(VykarDirEntry {
                                name: name.clone(),
                                meta: node_meta(child).clone(),
                            }) as Box<dyn DavDirEntry>)
                        })
                        .collect();
                    Ok(Box::pin(stream::iter(entries)) as FsStream<Box<dyn DavDirEntry>>)
                }
                _ => Err(FsError::Forbidden),
            }
        })
    }

    fn open<'a>(
        &'a self,
        path: &'a DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn DavFile>> {
        Box::pin(async move {
            if options.write || options.append || options.create || options.create_new {
                return Err(FsError::Forbidden);
            }

            let node = self.lookup(path)?;
            match node {
                VfsNode::File { chunks, meta } => Ok(Box::new(VykarDavFile {
                    chunks,
                    meta,
                    pos: 0,
                    repo: self.repo.clone(),
                    cache: self.cache.clone(),
                }) as Box<dyn DavFile>),
                _ => Err(FsError::Forbidden),
            }
        })
    }
}

// ─── DavFile ───────────────────────────────────────────────────────────────

struct VykarDavFile {
    chunks: Arc<Vec<ChunkRef>>,
    meta: VfsMeta,
    pos: u64,
    repo: Arc<Repo>,
    cache: ChunkCache,
}

impl fmt::Debug for VykarDavFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VykarDavFile")
            .field("pos", &self.pos)
            .field("size", &self.meta.size)
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

/// Read a chunk via the LRU cache, falling back to the repository. Neither
/// lock is held across the storage read, so concurrent misses read in
/// parallel (and two misses on the same chunk both read it).
fn read_chunk_cached(
    repo: &Repo,
    cache: &ChunkCache,
    chunk_id: &ChunkId,
) -> FsResult<Arc<Vec<u8>>> {
    // Fast path: cache hit
    {
        let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(data) = guard.get(chunk_id) {
            return Ok(data.clone());
        }
    }

    // Slow path: read from repository
    let data = repo
        .read_chunk(chunk_id)
        .map_err(|_| FsError::GeneralFailure)?;

    let data = Arc::new(data);

    {
        let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
        guard.put(*chunk_id, data.clone());
    }

    Ok(data)
}

impl DavFile for VykarDavFile {
    fn metadata(&mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let meta = self.meta.clone();
        Box::pin(async move { Ok(Box::new(meta) as Box<dyn DavMetaData>) })
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<'_, Bytes> {
        let repo = self.repo.clone();
        let cache = self.cache.clone();
        let chunks = self.chunks.clone();
        let start_pos = self.pos;
        let file_size = self.meta.size;

        Box::pin(async move {
            if start_pos >= file_size {
                return Ok(Bytes::new());
            }

            let count = count.min((file_size - start_pos) as usize);

            let result = tokio::task::spawn_blocking(move || -> FsResult<Vec<u8>> {
                let mut buf = Vec::with_capacity(count);
                let mut remaining = count;
                let mut offset = start_pos;
                let mut chunk_start: u64 = 0;

                for chunk_ref in chunks.iter() {
                    let chunk_end = chunk_start + chunk_ref.size as u64;

                    if offset >= chunk_end {
                        chunk_start = chunk_end;
                        continue;
                    }
                    if remaining == 0 {
                        break;
                    }

                    let chunk_data = read_chunk_cached(&repo, &cache, &chunk_ref.id)?;

                    let start_in_chunk = (offset - chunk_start) as usize;
                    let available = chunk_data.len() - start_in_chunk;
                    let to_copy = remaining.min(available);

                    let slice = chunk_data
                        .get(start_in_chunk..start_in_chunk + to_copy)
                        .expect("start + to_copy <= chunk_data.len() (to_copy <= available)");
                    buf.extend_from_slice(slice);

                    remaining -= to_copy;
                    offset += to_copy as u64;
                    chunk_start = chunk_end;
                }

                Ok(buf)
            })
            .await
            .map_err(|_| FsError::GeneralFailure)??;

            let bytes_read = result.len() as u64;
            self.pos += bytes_read;
            Ok(Bytes::from(result))
        })
    }

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<'_, u64> {
        Box::pin(async move {
            let new_pos = match pos {
                SeekFrom::Start(p) => p,
                SeekFrom::Current(p) => {
                    if p >= 0 {
                        self.pos.saturating_add(p as u64)
                    } else {
                        self.pos
                            .checked_sub((-p) as u64)
                            .ok_or(FsError::GeneralFailure)?
                    }
                }
                SeekFrom::End(p) => {
                    if p >= 0 {
                        self.meta.size.saturating_add(p as u64)
                    } else {
                        self.meta
                            .size
                            .checked_sub((-p) as u64)
                            .ok_or(FsError::GeneralFailure)?
                    }
                }
            };
            self.pos = new_pos;
            Ok(new_pos)
        })
    }

    fn write_buf(&mut self, _buf: Box<dyn bytes::Buf + Send>) -> FsFuture<'_, ()> {
        Box::pin(async { Err(FsError::Forbidden) })
    }

    fn write_bytes(&mut self, _buf: Bytes) -> FsFuture<'_, ()> {
        Box::pin(async { Err(FsError::Forbidden) })
    }

    fn flush(&mut self) -> FsFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

// ─── Public API ────────────────────────────────────────────────────────────

/// Progress events emitted by the mount command.
#[derive(Debug)]
pub enum MountProgressEvent {
    LoadingSnapshots,
    /// Sent only for a single snapshot built whole before serving (one
    /// without a usable listing key index); other snapshots and directories
    /// are read on first access.
    SnapshotLoaded {
        name: String,
        item_count: usize,
    },
    Serving {
        address: String,
    },
    ShuttingDown,
}

/// Start a read-only WebDAV server exposing snapshot contents.
///
/// If `snapshot_name` is given, serves that single snapshot at the root.
/// Otherwise, serves all snapshots as top-level directories.
pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: Option<&str>,
    address: &str,
    cache_size: usize,
    source_filter: &[String],
) -> Result<()> {
    run_with_progress(
        config,
        passphrase,
        snapshot_name,
        address,
        cache_size,
        source_filter,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_with_progress(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: Option<&str>,
    address: &str,
    cache_size: usize,
    source_filter: &[String],
    mut progress: Option<&mut dyn FnMut(MountProgressEvent)>,
    shutdown: Option<Arc<tokio::sync::Notify>>,
) -> Result<()> {
    // Mount cancels through `shutdown` (a Notify), not a token.
    let repo = open_repo(
        config,
        passphrase,
        crate::repo::OpenOptions::new().with_index(),
        &CancelToken::new(),
    )?;

    if let Some(ref mut cb) = progress {
        cb(MountProgressEvent::LoadingSnapshots);
    }
    let repo = Arc::new(repo);
    let root = if let Some(name) = snapshot_name {
        // One snapshot: its root directory (or its whole tree, without a
        // usable key index) is read before serving, so a bad snapshot fails
        // the mount.
        let (root, item_count) = open_snapshot(&repo, name)?;
        if let (Some(item_count), Some(cb)) = (item_count, progress.as_mut()) {
            cb(MountProgressEvent::SnapshotLoaded {
                name: name.to_string(),
                item_count,
            });
        }
        root
    } else {
        let entries: Vec<_> = repo
            .manifest()
            .snapshots
            .iter()
            .filter(|e| source_filter.is_empty() || source_filter.contains(&e.source_label))
            .cloned()
            .collect();
        multi_snapshot_root(&repo, &entries)
    };

    let cache_size = NonZeroUsize::new(cache_size)
        .unwrap_or_else(|| NonZeroUsize::new(256).expect("literal non-zero cache size"));
    let cache = Arc::new(Mutex::new(LruCache::new(cache_size)));

    let fs = VykarDavFs {
        root: root.clone(),
        repo,
        cache,
    };

    let handler = DavHandler::builder()
        .filesystem(Box::new(fs))
        .build_handler();

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| VykarError::Other(format!("failed to create tokio runtime: {e}")))?;

    let is_multi_snapshot = snapshot_name.is_none();
    rt.block_on(async {
        serve(
            handler,
            root,
            address,
            is_multi_snapshot,
            &mut progress,
            shutdown,
        )
        .await
    })
}

/// The root of a multi-snapshot mount: one directory per snapshot of
/// `entries`, each read on first access. Reads nothing.
fn multi_snapshot_root(
    repo: &Arc<Repo>,
    entries: &[crate::repo::manifest::SnapshotEntry],
) -> VfsNode {
    let children = entries
        .iter()
        .map(|entry| {
            let dir = VfsDir::lazy(
                DirLoader::Snapshot {
                    repo: Arc::clone(repo),
                    name: entry.name.clone(),
                },
                VfsMeta {
                    mtime: SystemTime::from(entry.time),
                    ..VfsMeta::dir_default()
                },
            );
            (entry.name.clone(), VfsNode::Dir(Arc::new(dir)))
        })
        .collect();
    VfsNode::Dir(Arc::new(VfsDir::ready(children, VfsMeta::dir_default())))
}

async fn serve(
    handler: DavHandler,
    root: VfsNode,
    address: &str,
    is_multi_snapshot: bool,
    progress: &mut Option<&mut dyn FnMut(MountProgressEvent)>,
    shutdown: Option<Arc<tokio::sync::Notify>>,
) -> Result<()> {
    let addr: std::net::SocketAddr = address
        .parse()
        .map_err(|e| VykarError::Config(format!("invalid address '{address}': {e}")))?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| VykarError::Other(format!("failed to bind to {addr}: {e}")))?;
    let bound = listener
        .local_addr()
        .map_err(|e| VykarError::Other(format!("local_addr failed: {e}")))?;

    if let Some(cb) = progress.as_deref_mut() {
        cb(MountProgressEvent::Serving {
            address: format!("{bound}"),
        });
    }

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result
                    .map_err(|e| VykarError::Other(format!("accept error: {e}")))?;
                // WebDAV clients issue many small request/response pairs; Nagle's
                // algorithm delays each reply waiting for an ACK.
                if let Err(e) = stream.set_nodelay(true) {
                    tracing::debug!("failed to set TCP_NODELAY on WebDAV connection: {e}");
                }
                let io = TokioIo::new(stream);
                let handler = handler.clone();
                let root = root.clone();

                tokio::spawn(async move {
                    if let Err(e) = http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |req| {
                                let handler = handler.clone();
                                let root = root.clone();
                                async move {
                                    match classify_browser_request(&req, &root) {
                                        BrowserAction::ServeHtml => {
                                            let raw_path = req.uri().path().to_string();
                                            let decoded = percent_decode_str(&raw_path).decode_utf8_lossy();
                                            let is_snapshot_root = is_multi_snapshot
                                                && decoded.trim_matches('/').is_empty();
                                            let (status, html) = match render_directory_html(&decoded, &root, is_snapshot_root) {
                                                Ok(html) => (200, html),
                                                Err(e) => {
                                                    tracing::warn!("mount: {decoded}: {e}");
                                                    (500, format!(
                                                        "<html><body><h1>Error</h1><p>{}</p></body></html>",
                                                        html_escape(&e.to_string())
                                                    ))
                                                }
                                            };
                                            let resp = hyper::Response::builder()
                                                .status(status)
                                                .header("content-type", "text/html; charset=utf-8")
                                                .body(dav_server::body::Body::from(html))
                                                .expect("static HTML response is valid");
                                            Ok::<_, Infallible>(resp)
                                        }
                                        BrowserAction::Redirect(location) => {
                                            let resp = hyper::Response::builder()
                                                .status(301)
                                                .header("location", location)
                                                .body(dav_server::body::Body::from(""))
                                                .expect("redirect response is valid");
                                            Ok::<_, Infallible>(resp)
                                        }
                                        BrowserAction::PassThrough => {
                                            Ok::<_, Infallible>(handler.handle(req).await)
                                        }
                                    }
                                }
                            }),
                        )
                        .await
                    {
                        tracing::debug!("connection error: {e}");
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                if let Some(cb) = progress.as_deref_mut() {
                    cb(MountProgressEvent::ShuttingDown);
                }
                break;
            }
            _ = async {
                match shutdown.as_ref() {
                    Some(n) => n.notified().await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                if let Some(cb) = progress.as_deref_mut() {
                    cb(MountProgressEvent::ShuttingDown);
                }
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a simple VFS tree with a single directory entry.
    fn tree_with_dir(name: &str) -> VfsNode {
        let mut children = HashMap::new();
        children.insert(
            name.to_string(),
            VfsNode::Dir(Arc::new(VfsDir::ready(
                HashMap::new(),
                VfsMeta::dir_default(),
            ))),
        );
        VfsNode::Dir(Arc::new(VfsDir::ready(children, VfsMeta::dir_default())))
    }

    /// Helper: build a hyper GET request with the given URI and Accept header.
    fn browser_get(uri: &str) -> hyper::Request<()> {
        hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri(uri)
            .header("accept", "text/html")
            .body(())
            .unwrap()
    }

    #[test]
    fn test_classify_encoded_dir_as_html() {
        // VFS tree has "form c" (with a real space); request uses percent-encoded path.
        let tree = tree_with_dir("form c");
        let req = browser_get("/form%20c/");
        match classify_browser_request(&req, &tree) {
            BrowserAction::ServeHtml => {} // expected
            other => panic!("expected ServeHtml, got {other:?}"),
        }
    }

    #[test]
    fn test_classify_encoded_dir_redirect() {
        let tree = tree_with_dir("form c");
        // No trailing slash → should redirect, preserving the encoded path.
        let req = browser_get("/form%20c");
        match classify_browser_request(&req, &tree) {
            BrowserAction::Redirect(loc) => assert_eq!(loc, "/form%20c/"),
            other => panic!("expected Redirect, got {other:?}"),
        }
    }

    #[test]
    fn test_render_html_decoded_path() {
        let tree = tree_with_dir("form c");
        // After decoding, render_directory_html should find the "form c" dir.
        let decoded = percent_decode_str("/form%20c/").decode_utf8_lossy();
        let html = render_directory_html(&decoded, &tree, false).unwrap();
        assert!(
            !html.contains("Not Found"),
            "decoded path should resolve the directory"
        );
    }

    /// Helper: build a VFS tree with multiple snapshot dirs at root, each with a given mtime.
    fn tree_with_snapshots(snapshots: &[(&str, SystemTime)]) -> VfsNode {
        let mut children = HashMap::new();
        for (name, mtime) in snapshots {
            children.insert(
                name.to_string(),
                VfsNode::Dir(Arc::new(VfsDir::ready(
                    HashMap::new(),
                    VfsMeta {
                        size: 0,
                        mtime: *mtime,
                        is_dir: true,
                        is_symlink: false,
                    },
                ))),
            );
        }
        VfsNode::Dir(Arc::new(VfsDir::ready(children, VfsMeta::dir_default())))
    }

    /// Helper: build a VFS tree with files at root for testing data-sort attributes.
    fn tree_with_files(files: &[(&str, u64, SystemTime)]) -> VfsNode {
        let mut children = HashMap::new();
        for (name, size, mtime) in files {
            children.insert(
                name.to_string(),
                VfsNode::File {
                    chunks: Arc::new(vec![]),
                    meta: VfsMeta {
                        size: *size,
                        mtime: *mtime,
                        is_dir: false,
                        is_symlink: false,
                    },
                },
            );
        }
        VfsNode::Dir(Arc::new(VfsDir::ready(children, VfsMeta::dir_default())))
    }

    #[test]
    fn test_snapshot_root_sort_order() {
        let t1 = UNIX_EPOCH + std::time::Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + std::time::Duration::from_secs(2000);
        let t3 = UNIX_EPOCH + std::time::Duration::from_secs(3000);
        let tree = tree_with_snapshots(&[("snap-old", t1), ("snap-new", t3), ("snap-mid", t2)]);
        let html = render_directory_html("/", &tree, true).unwrap();
        // Newest first: snap-new before snap-mid before snap-old
        let pos_new = html.find("snap-new").expect("snap-new should appear");
        let pos_mid = html.find("snap-mid").expect("snap-mid should appear");
        let pos_old = html.find("snap-old").expect("snap-old should appear");
        assert!(
            pos_new < pos_mid && pos_mid < pos_old,
            "snapshot root should sort by mtime descending: new={pos_new}, mid={pos_mid}, old={pos_old}"
        );
    }

    #[test]
    fn test_data_sort_attributes() {
        let mtime = UNIX_EPOCH + std::time::Duration::from_secs(1_700_000);
        let tree = tree_with_files(&[("readme.txt", 4096, mtime)]);
        let html = render_directory_html("/", &tree, false).unwrap();
        // File size data-sort should be the raw byte count
        assert!(
            html.contains(r#"data-sort="4096""#),
            "file row should have data-sort with raw byte count"
        );
        // File mtime data-sort should be millis since epoch
        let expected_ms = format!("data-sort=\"{}\"", 1_700_000u128 * 1000);
        assert!(
            html.contains(&expected_ms),
            "file row should have data-sort with mtime in millis"
        );
    }

    #[test]
    fn test_sortable_headers_present() {
        let tree = tree_with_dir("test");
        let html = render_directory_html("/", &tree, false).unwrap();
        assert!(
            html.contains(r#"class="sortable" data-col="1" data-type="text""#),
            "Name header should be sortable"
        );
        assert!(
            html.contains(r#"class="sortable" data-col="2" data-type="num""#),
            "Size header should be sortable"
        );
        assert!(
            html.contains(r#"class="sortable" data-col="3" data-type="num""#),
            "Modified header should be sortable"
        );
    }

    #[test]
    fn test_initial_sort_attribute() {
        let tree = tree_with_dir("test");
        // Snapshot root: sort by modified descending
        let html_root = render_directory_html("/", &tree, true).unwrap();
        assert!(
            html_root.contains(r#"data-sort-col="3" data-sort-dir="desc""#),
            "snapshot root table should default to col 3 desc"
        );
        // Inner directory: sort by name ascending
        let html_inner = render_directory_html("/", &tree, false).unwrap();
        assert!(
            html_inner.contains(r#"data-sort-col="1" data-sort-dir="asc""#),
            "inner directory table should default to col 1 asc"
        );
    }

    // ─── Lazy tree ──────────────────────────────────────────────────────

    use crate::tests::helpers::open_local_repo;
    use crate::tests::listing_index::{backup, make_awkward, multi_root_repo, reseal};

    /// Every directory of `lazy` (loading it) has the children of `eager`
    /// with the same kind, metadata, chunks and link targets.
    fn assert_same_tree(lazy: &VfsNode, eager: &VfsNode, path: &str) {
        assert_eq!(node_meta(lazy), node_meta(eager), "{path:?}");
        match (lazy, eager) {
            (VfsNode::Dir(l), VfsNode::Dir(e)) => {
                let lc = l.children().unwrap();
                let ec = e.children_if_loaded().unwrap();
                let mut ln: Vec<&String> = lc.keys().collect();
                let mut en: Vec<&String> = ec.keys().collect();
                ln.sort();
                en.sort();
                assert_eq!(ln, en, "{path:?}");
                for (name, child) in lc.iter() {
                    assert_same_tree(child, &ec[name], &format!("{path}/{name}"));
                }
            }
            (VfsNode::File { chunks: l, .. }, VfsNode::File { chunks: e, .. }) => {
                assert_eq!(l, e, "{path:?}");
            }
            (VfsNode::Symlink { _target: l, .. }, VfsNode::Symlink { _target: e, .. }) => {
                assert_eq!(l, e, "{path:?}");
            }
            _ => panic!("{path:?}: kinds differ"),
        }
    }

    /// Directory `node`'s children (loading them) under a default root meta.
    fn contents(node: &VfsNode) -> VfsNode {
        VfsNode::Dir(Arc::new(VfsDir::ready(
            (*dir_of(node).children().unwrap()).clone(),
            VfsMeta::dir_default(),
        )))
    }

    /// The eager tree of snapshot `name`, the oracle.
    fn eager_tree(repo: &mut Repo, name: &str) -> VfsNode {
        build_vfs_tree(&list_cmd::load_snapshot_items(repo, name, &CancelToken::new()).unwrap())
    }

    fn dir_of(node: &VfsNode) -> &Arc<VfsDir> {
        match node {
            VfsNode::Dir(d) => d,
            _ => panic!("not a directory"),
        }
    }

    /// The child directory `name` of `dir`, which must be loaded.
    fn loaded_child(dir: &VfsDir, name: &str) -> VfsNode {
        dir.children_if_loaded().unwrap()[name].clone()
    }

    #[cfg(unix)]
    #[test]
    fn lazy_tree_equals_the_eager_tree() {
        let tmp = tempfile::tempdir().unwrap();
        multi_root_repo(tmp.path(), "multi", make_awkward);
        let mut repo = open_local_repo(&tmp.path().join("repo"));
        let eager = eager_tree(&mut repo, "multi");
        let repo = Arc::new(repo);
        let (lazy, count) = open_snapshot(&repo, "multi").unwrap();
        assert_eq!(count, None, "the snapshot has a key index");
        assert_same_tree(&lazy, &eager, "");
        // Through the multi-snapshot root too.
        let entries = repo.manifest().snapshots.clone();
        let root = multi_snapshot_root(&repo, &entries);
        let snap = lookup(&root, b"/multi").unwrap().unwrap();
        assert_same_tree(&contents(&snap), &eager, "");
    }

    /// Two snapshots of `a/b/c/f`, `a/x/g` and `top`.
    fn two_snapshots() -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        let source = tmp.path().join("src");
        std::fs::create_dir_all(source.join("a/b/c")).unwrap();
        std::fs::create_dir_all(source.join("a/x")).unwrap();
        std::fs::write(source.join("a/b/c/f"), "f").unwrap();
        std::fs::write(source.join("a/x/g"), "g").unwrap();
        std::fs::write(source.join("top"), "t").unwrap();
        std::fs::create_dir_all(&repo_dir).unwrap();
        let config = crate::tests::helpers::init_repo(&repo_dir);
        let paths = [source.to_string_lossy().to_string()];
        backup(&config, &paths, "s1", &[]);
        backup(&config, &paths, "s2", &[]);
        (tmp, repo_dir)
    }

    #[test]
    fn multi_snapshot_root_reads_no_listing() {
        let (_tmp, repo_dir) = two_snapshots();
        let repo = Arc::new(open_local_repo(&repo_dir));
        let entries = repo.manifest().snapshots.clone();
        let root = multi_snapshot_root(&repo, &entries);
        let rd = dir_of(&root);
        let mut names: Vec<String> = rd.children().unwrap().keys().cloned().collect();
        names.sort();
        assert_eq!(names, vec!["s1".to_string(), "s2".to_string()]);
        let snaps = rd.children().unwrap();
        assert!(snaps.values().all(|s| !dir_of(s).is_loaded()));
        // What WebDAV `metadata` does on the snapshot directory.
        assert!(lookup(&root, b"/s1").unwrap().is_some());
        assert!(!dir_of(&snaps["s1"]).is_loaded());

        // A lookup three levels deep loads exactly the directories on the way.
        let node = lookup(&root, b"/s1/a/b").unwrap().unwrap();
        assert!(!dir_of(&node).is_loaded());
        let s1 = dir_of(&snaps["s1"]);
        assert!(s1.is_loaded());
        let a = loaded_child(s1, "a");
        assert!(dir_of(&a).is_loaded());
        assert!(!dir_of(&loaded_child(dir_of(&a), "x")).is_loaded());
        assert!(!dir_of(&snaps["s2"]).is_loaded());
        assert!(lookup(&root, b"/s1/a/b/c/f").unwrap().is_some());
        assert!(lookup(&root, b"/s1/a/nosuch").unwrap().is_none());
    }

    #[test]
    fn snapshot_without_an_index_is_built_whole() {
        let (_tmp, repo_dir) = two_snapshots();
        reseal(&repo_dir, "s1", |m| m.ext = None);
        let mut repo = open_local_repo(&repo_dir);
        let eager = eager_tree(&mut repo, "s1");
        let repo = Arc::new(repo);
        let entries = repo.manifest().snapshots.clone();
        let root = multi_snapshot_root(&repo, &entries);
        let s1 = lookup(&root, b"/s1").unwrap().unwrap();
        assert_same_tree(&contents(&s1), &eager, "");
        // Every directory of the whole-built snapshot is known.
        let a = lookup(&root, b"/s1/a").unwrap().unwrap();
        assert!(dir_of(&a).is_loaded());
        // The other snapshot still reads one directory at a time.
        let a2 = lookup(&root, b"/s2/a").unwrap().unwrap();
        assert!(!dir_of(&a2).is_loaded());
        let (_, count) = open_snapshot(&repo, "s1").unwrap();
        assert_eq!(count, Some(7));
    }

    #[test]
    fn unreadable_snapshot_fails_only_its_directory() {
        let (_tmp, repo_dir) = two_snapshots();
        let repo = Arc::new(open_local_repo(&repo_dir));
        let entries = repo.manifest().snapshots.clone();
        let root = multi_snapshot_root(&repo, &entries);
        let bad = repo.manifest().find_snapshot("s1").unwrap().id;
        repo.storage().delete(&bad.storage_key()).unwrap();
        assert!(lookup(&root, b"/s1/a").is_err());
        assert!(lookup(&root, b"/s2/a/b/c/f").unwrap().is_some());
        let mut names: Vec<String> = dir_of(&root).children().unwrap().keys().cloned().collect();
        names.sort();
        assert_eq!(names, vec!["s1".to_string(), "s2".to_string()]);
        // A failed load is not cached.
        assert!(!dir_of(&lookup(&root, b"/s1").unwrap().unwrap()).is_loaded());
    }

    /// A directory read that falls back to a scan switches the snapshot to
    /// its whole tree for further directories; the results stay the same.
    #[test]
    fn a_fallback_switches_the_snapshot_to_the_whole_tree() {
        let (_tmp, repo_dir) = two_snapshots();
        let mut repo = open_local_repo(&repo_dir);
        let eager = eager_tree(&mut repo, "s1");
        let repo = Arc::new(repo);
        let mut meta = list_cmd::load_snapshot_meta(&repo, "s1").unwrap();
        let roots = indexed_roots(&meta).unwrap();
        // The key index says nothing is wrong; a view ignoring it scans.
        meta.ext = Some(b"garbage".to_vec());
        let snap = Arc::new(IndexedSnapshot {
            repo: Arc::clone(&repo),
            meta,
            roots,
            degraded: AtomicBool::new(false),
            whole: LoadOnce::new(),
        });
        let children = load_indexed(&snap, &[Vec::new()], "").unwrap();
        assert!(snap.degraded.load(AtomicOrdering::Relaxed));
        assert!(snap.whole.get().is_none());
        let lazy = VfsNode::Dir(Arc::new(VfsDir::ready(
            (*children).clone(),
            VfsMeta::dir_default(),
        )));
        assert_same_tree(&lazy, &eager, "");
        assert!(snap.whole.get().is_some());
    }

    /// Reading a directory's loaded children takes no lock: while another
    /// thread loads that directory, `children_if_loaded` answers at once.
    #[test]
    fn a_loading_directory_does_not_block_readers() {
        let (tx, rx) = std::sync::mpsc::channel();
        let dir = Arc::new(VfsDir::lazy(
            DirLoader::Gate(Mutex::new(rx)),
            VfsMeta::dir_default(),
        ));
        let loader = {
            let dir = Arc::clone(&dir);
            std::thread::spawn(move || dir.children().map(|c| c.len()))
        };
        // Wait until the loader holds the directory's loading lock.
        let DirChildren::Lazy { loaded, .. } = &dir.children else {
            panic!("not lazy")
        };
        let t0 = std::time::Instant::now();
        while loaded.loading.try_lock().is_ok() {
            assert!(t0.elapsed() < std::time::Duration::from_secs(30));
            std::thread::yield_now();
        }
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        {
            let dir = Arc::clone(&dir);
            std::thread::spawn(move || done_tx.send(dir.children_if_loaded().is_none()));
        }
        let answered = done_rx.recv_timeout(std::time::Duration::from_secs(10));
        tx.send(()).unwrap();
        assert_eq!(answered, Ok(true), "children_if_loaded waited for the load");
        assert_eq!(loader.join().unwrap().unwrap(), 0);
        assert!(dir.is_loaded());
    }

    /// Shared state of a [`RendezvousBackend`]: (reads in flight, most at once).
    #[derive(Default)]
    struct Rendezvous {
        armed: std::sync::atomic::AtomicBool,
        state: Mutex<(usize, usize)>,
        cond: std::sync::Condvar,
    }

    /// MemoryBackend whose armed range reads wait until two are in flight.
    struct RendezvousBackend {
        inner: crate::testutil::MemoryBackend,
        rv: Arc<Rendezvous>,
    }

    impl vykar_storage::InnerBackend for RendezvousBackend {
        fn inner_backend(&self) -> &dyn vykar_storage::StorageBackend {
            &self.inner
        }
    }

    vykar_storage::delegate_storage_backend! {
        for RendezvousBackend;
        except [get_range];

        fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
            use std::sync::atomic::Ordering;
            if !self.rv.armed.load(Ordering::SeqCst) {
                return self.inner.get_range(key, offset, length);
            }
            {
                let mut s = self.rv.state.lock().unwrap();
                s.0 += 1;
                s.1 = s.1.max(s.0);
                self.rv.cond.notify_all();
                let _ = self
                    .rv
                    .cond
                    .wait_timeout_while(s, std::time::Duration::from_secs(30), |s| s.1 < 2)
                    .unwrap();
            }
            let result = self.inner.get_range(key, offset, length);
            self.rv.state.lock().unwrap().0 -= 1;
            result
        }
    }

    /// Two uncached reads through one `Repo` reach storage at the same
    /// time. Guards against a lock held across the storage read in
    /// `read_chunk_cached`: with one, the second read cannot enter `get_range`
    /// while the first waits, the first times out and `max` stays 1.
    #[test]
    fn uncached_reads_run_concurrently() {
        use crate::compress::Compression;
        use crate::repo::pack::PackType;
        use crate::repo::{EncryptionMode, WriteSessionOptions};

        crate::testutil::init_test_environment();
        let rv = Arc::new(Rendezvous::default());
        let backend = RendezvousBackend {
            inner: crate::testutil::MemoryBackend::new(),
            rv: Arc::clone(&rv),
        };
        let mut repo = Repo::init(
            Box::new(backend),
            EncryptionMode::None,
            crate::repo::ChunkerParams::default(),
            None,
            None,
            None,
        )
        .unwrap();
        let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
        let data_a = b"mount chunk a".repeat(100);
        let data_b = b"mount chunk b".repeat(100);
        let (id_a, _, _) = ws
            .store_chunk(&repo, &data_a, Compression::None, PackType::Data)
            .unwrap();
        let (id_b, _, _) = ws
            .store_chunk(&repo, &data_b, Compression::None, PackType::Data)
            .unwrap();
        // A session's chunks are in the index once it is saved.
        repo.unchecked_writer().save_session(ws).unwrap();
        rv.armed.store(true, std::sync::atomic::Ordering::SeqCst);

        let repo = Arc::new(repo);
        let cache: ChunkCache = Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(16).unwrap())));
        let (a, b) = std::thread::scope(|s| {
            let a = s.spawn(|| read_chunk_cached(&repo, &cache, &id_a));
            let b = s.spawn(|| read_chunk_cached(&repo, &cache, &id_b));
            (a.join().unwrap(), b.join().unwrap())
        });

        assert_eq!(*a.ok().unwrap(), data_a);
        assert_eq!(*b.ok().unwrap(), data_b);
        assert_eq!(rv.state.lock().unwrap().1, 2);
    }
}
