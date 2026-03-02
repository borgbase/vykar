//! WebDAV server for browsing snapshot contents.
//!
//! Starts a read-only WebDAV server exposing snapshot contents as a virtual
//! filesystem. macOS Finder can mount it natively via "Connect to Server".

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use dav_server::davpath::DavPath;
use dav_server::fs::*;
use dav_server::DavHandler;
use futures_util::stream;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use lru::LruCache;
use tokio::net::TcpListener;

use crate::commands::list as list_cmd;
use crate::config::VykarConfig;
use crate::repo::Repository;
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use super::util::open_repo;

// ─── VFS tree ──────────────────────────────────────────────────────────────

type ChunkCache = Arc<Mutex<LruCache<ChunkId, Arc<Vec<u8>>>>>;

/// In-memory virtual filesystem node built from snapshot items.
enum VfsNode {
    Dir {
        children: HashMap<String, VfsNode>,
        meta: VfsMeta,
    },
    File {
        chunks: Vec<ChunkRef>,
        meta: VfsMeta,
    },
    Symlink {
        _target: String,
        meta: VfsMeta,
    },
}

/// Metadata for a VFS node, compatible with WebDAV metadata requirements.
#[derive(Debug, Clone)]
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
        VfsNode::Dir { meta, .. } => meta,
        VfsNode::File { meta, .. } => meta,
        VfsNode::Symlink { meta, .. } => meta,
    }
}

/// Build a VFS tree from a list of snapshot items.
fn build_vfs_tree(items: &[Item]) -> VfsNode {
    let mut root_children = HashMap::new();

    for item in items {
        let path = item.path.trim_start_matches('/');
        if path.is_empty() {
            continue;
        }
        let components: Vec<&str> = path.split('/').collect();
        insert_into_tree(&mut root_children, &components, item);
    }

    VfsNode::Dir {
        children: root_children,
        meta: VfsMeta::dir_default(),
    }
}

fn insert_into_tree(children: &mut HashMap<String, VfsNode>, components: &[&str], item: &Item) {
    if components.is_empty() {
        return;
    }

    if components.len() == 1 {
        let name = components[0].to_string();
        // If this is a directory and one already exists as an intermediate,
        // just update its metadata rather than replacing it (which would lose children).
        if item.entry_type == ItemType::Directory {
            if let Some(VfsNode::Dir { meta, .. }) = children.get_mut(&name) {
                *meta = VfsMeta::from_item(item);
                return;
            }
        }
        let node = match item.entry_type {
            ItemType::Directory => VfsNode::Dir {
                children: HashMap::new(),
                meta: VfsMeta::from_item(item),
            },
            ItemType::RegularFile => VfsNode::File {
                chunks: item.chunks.clone(),
                meta: VfsMeta::from_item(item),
            },
            ItemType::Symlink => VfsNode::Symlink {
                _target: item.link_target.clone().unwrap_or_default(),
                meta: VfsMeta::from_item(item),
            },
        };
        children.insert(name, node);
    } else {
        let dir_name = components[0].to_string();
        let entry = children.entry(dir_name).or_insert_with(|| VfsNode::Dir {
            children: HashMap::new(),
            meta: VfsMeta::dir_default(),
        });
        if let VfsNode::Dir {
            children: ref mut dir_children,
            ..
        } = entry
        {
            insert_into_tree(dir_children, &components[1..], item);
        }
    }
}

/// Lookup a node by its path bytes (as returned by `DavPath::as_bytes()`).
fn lookup<'a>(root: &'a VfsNode, path: &[u8]) -> Option<&'a VfsNode> {
    let path_str = std::str::from_utf8(path).ok()?;
    let path_str = path_str.trim_start_matches('/');

    if path_str.is_empty() {
        return Some(root);
    }

    let mut current = root;
    for component in path_str.split('/') {
        if component.is_empty() {
            continue;
        }
        match current {
            VfsNode::Dir { children, .. } => {
                current = children.get(component)?;
            }
            _ => return None,
        }
    }

    Some(current)
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
enum BrowserAction {
    ServeHtml,
    Redirect(String),
    PassThrough,
}

fn classify_browser_request<B>(req: &hyper::Request<B>, tree: &VfsNode) -> BrowserAction {
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
    let path = req.uri().path();
    match lookup(tree, path.as_bytes()) {
        Some(VfsNode::Dir { .. }) => {
            if path == "/" || path.ends_with('/') {
                BrowserAction::ServeHtml
            } else {
                BrowserAction::Redirect(format!("{path}/"))
            }
        }
        _ => BrowserAction::PassThrough,
    }
}

fn render_directory_html(path: &str, tree: &VfsNode) -> String {
    let node = lookup(tree, path.as_bytes());
    let children = match node {
        Some(VfsNode::Dir { children, .. }) => children,
        _ => return String::from("<html><body><h1>Not Found</h1></body></html>"),
    };

    // Collect and sort entries: dirs first, then files, alphabetical within each
    let mut dirs: Vec<(&String, &VfsNode)> = Vec::new();
    let mut files: Vec<(&String, &VfsNode)> = Vec::new();
    for (name, child) in children {
        match child {
            VfsNode::Dir { .. } => dirs.push((name, child)),
            _ => files.push((name, child)),
        }
    }
    dirs.sort_by(|a, b| a.0.to_lowercase().cmp(&b.0.to_lowercase()));
    files.sort_by(|a, b| a.0.to_lowercase().cmp(&b.0.to_lowercase()));

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
            r#"<tr><td class="icon">📁</td><td><a href="../">../</a></td><td class="size"></td><td class="mtime"></td></tr>
"#,
        );
    }

    for (name, child) in &dirs {
        let meta = node_meta(child);
        let mtime = format_mtime(meta.mtime);
        rows.push_str(&format!(
            r#"<tr><td class="icon">📁</td><td><a href="{}/"><strong>{}/</strong></a></td><td class="size">—</td><td class="mtime">{}</td></tr>
"#,
            percent_encode_path(name),
            html_escape(name),
            mtime,
        ));
    }

    for (name, child) in &files {
        let meta = node_meta(child);
        let mtime = format_mtime(meta.mtime);
        let (icon, display) = match child {
            VfsNode::Symlink { _target, .. } => (
                "🔗",
                format!("{} → {}", html_escape(name), html_escape(_target)),
            ),
            _ => ("📄", html_escape(name)),
        };
        rows.push_str(&format!(
            r#"<tr><td class="icon">{icon}</td><td><a href="{}">{display}</a></td><td class="size">{}</td><td class="mtime">{}</td></tr>
"#,
            percent_encode_path(name),
            format_size(meta.size),
            mtime,
        ));
    }

    format!(
        r##"<!DOCTYPE html>
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
footer {{ margin-top: 2rem; font-size: 0.75rem; color: var(--muted); }}
</style>
</head>
<body>
<h1>{title}</h1>
<div class="breadcrumbs">{breadcrumbs}</div>
<table>
<thead><tr><th></th><th>Name</th><th style="text-align:right">Size</th><th>Modified</th></tr></thead>
<tbody>
{rows}</tbody>
</table>
<footer>vykar backup — WebDAV + Web UI</footer>
</body>
</html>"##
    )
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
    tree: Arc<VfsNode>,
    repo: Arc<Mutex<Repository>>,
    cache: Arc<Mutex<LruCache<ChunkId, Arc<Vec<u8>>>>>,
}

impl DavFileSystem for VykarDavFs {
    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        Box::pin(async move {
            let node = lookup(&self.tree, path.as_bytes()).ok_or(FsError::NotFound)?;
            Ok(Box::new(node_meta(node).clone()) as Box<dyn DavMetaData>)
        })
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        Box::pin(async move {
            let node = lookup(&self.tree, path.as_bytes()).ok_or(FsError::NotFound)?;
            match node {
                VfsNode::Dir { children, .. } => {
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

            let node = lookup(&self.tree, path.as_bytes()).ok_or(FsError::NotFound)?;
            match node {
                VfsNode::File { chunks, meta } => Ok(Box::new(VykarDavFile {
                    chunks: chunks.clone(),
                    meta: meta.clone(),
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
    chunks: Vec<ChunkRef>,
    meta: VfsMeta,
    pos: u64,
    repo: Arc<Mutex<Repository>>,
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

/// Read a chunk via the LRU cache, falling back to the repository.
fn read_chunk_cached(
    repo: &Arc<Mutex<Repository>>,
    cache: &ChunkCache,
    chunk_id: &ChunkId,
) -> FsResult<Arc<Vec<u8>>> {
    // Fast path: cache hit
    {
        let mut guard = cache.lock().expect("mutex poisoned");
        if let Some(data) = guard.get(chunk_id) {
            return Ok(data.clone());
        }
    }

    // Slow path: read from repository
    let data = {
        let mut guard = repo.lock().expect("mutex poisoned");
        guard
            .read_chunk(chunk_id)
            .map_err(|_| FsError::GeneralFailure)?
    };

    let data = Arc::new(data);

    {
        let mut guard = cache.lock().expect("mutex poisoned");
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

                for chunk_ref in &chunks {
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

                    buf.extend_from_slice(&chunk_data[start_in_chunk..start_in_chunk + to_copy]);

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
    let mut repo = open_repo(config, passphrase)?;

    // Build the VFS tree from snapshot items
    eprintln!("Loading snapshot data...");
    let tree = if let Some(name) = snapshot_name {
        let items = list_cmd::load_snapshot_items(&mut repo, name)?;
        eprintln!("Loaded {} items from snapshot '{name}'", items.len());
        build_vfs_tree(&items)
    } else {
        let mut root_children = HashMap::new();
        let entries: Vec<_> = if source_filter.is_empty() {
            repo.manifest().snapshots.clone()
        } else {
            repo.manifest()
                .snapshots
                .iter()
                .filter(|e| source_filter.contains(&e.source_label))
                .cloned()
                .collect()
        };
        for entry in &entries {
            let items = list_cmd::load_snapshot_items(&mut repo, &entry.name)?;
            eprintln!(
                "Loaded {} items from snapshot '{}'",
                items.len(),
                entry.name
            );
            root_children.insert(entry.name.clone(), build_vfs_tree(&items));
        }
        VfsNode::Dir {
            children: root_children,
            meta: VfsMeta::dir_default(),
        }
    };

    let repo = Arc::new(Mutex::new(repo));
    let cache_size = NonZeroUsize::new(cache_size).unwrap_or(NonZeroUsize::new(256).unwrap());
    let cache = Arc::new(Mutex::new(LruCache::new(cache_size)));

    let tree = Arc::new(tree);

    let fs = VykarDavFs {
        tree: tree.clone(),
        repo,
        cache,
    };

    let handler = DavHandler::builder()
        .filesystem(Box::new(fs))
        .build_handler();

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| VykarError::Other(format!("failed to create tokio runtime: {e}")))?;

    rt.block_on(async { serve(handler, tree, address).await })
}

async fn serve(handler: DavHandler, tree: Arc<VfsNode>, address: &str) -> Result<()> {
    let addr: std::net::SocketAddr = address
        .parse()
        .map_err(|e| VykarError::Config(format!("invalid address '{address}': {e}")))?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| VykarError::Other(format!("failed to bind to {addr}: {e}")))?;

    eprintln!("Serving on http://{addr}");
    eprintln!("  Browse in browser:  http://{addr}");
    eprintln!("  WebDAV (Finder):    Go → Connect to Server → http://{addr}");
    eprintln!("Press Ctrl+C to stop.");

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result
                    .map_err(|e| VykarError::Other(format!("accept error: {e}")))?;
                let io = TokioIo::new(stream);
                let handler = handler.clone();
                let tree = tree.clone();

                tokio::spawn(async move {
                    if let Err(e) = http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |req| {
                                let handler = handler.clone();
                                let tree = tree.clone();
                                async move {
                                    match classify_browser_request(&req, &tree) {
                                        BrowserAction::ServeHtml => {
                                            let path = req.uri().path().to_string();
                                            let html = render_directory_html(&path, &tree);
                                            let resp = hyper::Response::builder()
                                                .status(200)
                                                .header("content-type", "text/html; charset=utf-8")
                                                .body(dav_server::body::Body::from(html))
                                                .unwrap();
                                            Ok::<_, Infallible>(resp)
                                        }
                                        BrowserAction::Redirect(location) => {
                                            let resp = hyper::Response::builder()
                                                .status(301)
                                                .header("location", location)
                                                .body(dav_server::body::Body::from(""))
                                                .unwrap();
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
                eprintln!("\nShutting down.");
                break;
            }
        }
    }

    Ok(())
}
