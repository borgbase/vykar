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
use percent_encoding::percent_decode_str;
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
#[derive(Debug)]
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
    let raw_path = req.uri().path();
    let decoded = percent_decode_str(raw_path).decode_utf8_lossy();
    match lookup(tree, decoded.as_bytes()) {
        Some(VfsNode::Dir { .. }) => {
            if raw_path == "/" || raw_path.ends_with('/') {
                BrowserAction::ServeHtml
            } else {
                BrowserAction::Redirect(format!("{raw_path}/"))
            }
        }
        _ => BrowserAction::PassThrough,
    }
}

fn render_directory_html(path: &str, tree: &VfsNode, is_snapshot_root: bool) -> String {
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
    if is_snapshot_root {
        // Snapshot root: sort by mtime descending (newest first), name tie-breaker
        dirs.sort_by(|a, b| {
            node_meta(b.1)
                .mtime
                .cmp(&node_meta(a.1).mtime)
                .then_with(|| a.0.to_lowercase().cmp(&b.0.to_lowercase()))
        });
    } else {
        dirs.sort_by(|a, b| a.0.to_lowercase().cmp(&b.0.to_lowercase()));
    }
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

/// Progress events emitted by the mount command.
#[derive(Debug)]
pub enum MountProgressEvent {
    LoadingSnapshots,
    SnapshotLoaded { name: String, item_count: usize },
    Serving { address: String },
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
    )
}

pub fn run_with_progress(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: Option<&str>,
    address: &str,
    cache_size: usize,
    source_filter: &[String],
    mut progress: Option<&mut dyn FnMut(MountProgressEvent)>,
) -> Result<()> {
    let mut repo = open_repo(
        config,
        passphrase,
        crate::repo::OpenOptions::new().with_index(),
    )?;

    // Build the VFS tree from snapshot items
    if let Some(ref mut cb) = progress {
        cb(MountProgressEvent::LoadingSnapshots);
    }
    let tree = if let Some(name) = snapshot_name {
        let items = list_cmd::load_snapshot_items(&mut repo, name)?;
        if let Some(ref mut cb) = progress {
            cb(MountProgressEvent::SnapshotLoaded {
                name: name.to_string(),
                item_count: items.len(),
            });
        }
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
            if let Some(ref mut cb) = progress {
                cb(MountProgressEvent::SnapshotLoaded {
                    name: entry.name.clone(),
                    item_count: items.len(),
                });
            }
            let mut snap_tree = build_vfs_tree(&items);
            if let VfsNode::Dir { ref mut meta, .. } = snap_tree {
                meta.mtime = SystemTime::from(entry.time);
            }
            root_children.insert(entry.name.clone(), snap_tree);
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

    let is_multi_snapshot = snapshot_name.is_none();
    rt.block_on(async { serve(handler, tree, address, is_multi_snapshot, &mut progress).await })
}

async fn serve(
    handler: DavHandler,
    tree: Arc<VfsNode>,
    address: &str,
    is_multi_snapshot: bool,
    progress: &mut Option<&mut dyn FnMut(MountProgressEvent)>,
) -> Result<()> {
    let addr: std::net::SocketAddr = address
        .parse()
        .map_err(|e| VykarError::Config(format!("invalid address '{address}': {e}")))?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| VykarError::Other(format!("failed to bind to {addr}: {e}")))?;

    if let Some(cb) = progress.as_deref_mut() {
        cb(MountProgressEvent::Serving {
            address: format!("{addr}"),
        });
    }

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
                                            let raw_path = req.uri().path().to_string();
                                            let decoded = percent_decode_str(&raw_path).decode_utf8_lossy();
                                            let is_snapshot_root = is_multi_snapshot
                                                && decoded.trim_matches('/').is_empty();
                                            let html = render_directory_html(&decoded, &tree, is_snapshot_root);
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
            VfsNode::Dir {
                children: HashMap::new(),
                meta: VfsMeta::dir_default(),
            },
        );
        VfsNode::Dir {
            children,
            meta: VfsMeta::dir_default(),
        }
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
        let html = render_directory_html(&decoded, &tree, false);
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
                VfsNode::Dir {
                    children: HashMap::new(),
                    meta: VfsMeta {
                        size: 0,
                        mtime: *mtime,
                        is_dir: true,
                        is_symlink: false,
                    },
                },
            );
        }
        VfsNode::Dir {
            children,
            meta: VfsMeta::dir_default(),
        }
    }

    /// Helper: build a VFS tree with files at root for testing data-sort attributes.
    fn tree_with_files(files: &[(&str, u64, SystemTime)]) -> VfsNode {
        let mut children = HashMap::new();
        for (name, size, mtime) in files {
            children.insert(
                name.to_string(),
                VfsNode::File {
                    chunks: vec![],
                    meta: VfsMeta {
                        size: *size,
                        mtime: *mtime,
                        is_dir: false,
                        is_symlink: false,
                    },
                },
            );
        }
        VfsNode::Dir {
            children,
            meta: VfsMeta::dir_default(),
        }
    }

    #[test]
    fn test_snapshot_root_sort_order() {
        let t1 = UNIX_EPOCH + std::time::Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + std::time::Duration::from_secs(2000);
        let t3 = UNIX_EPOCH + std::time::Duration::from_secs(3000);
        let tree = tree_with_snapshots(&[("snap-old", t1), ("snap-new", t3), ("snap-mid", t2)]);
        let html = render_directory_html("/", &tree, true);
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
        let html = render_directory_html("/", &tree, false);
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
        let html = render_directory_html("/", &tree, false);
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
        let html_root = render_directory_html("/", &tree, true);
        assert!(
            html_root.contains(r#"data-sort-col="3" data-sort-dir="desc""#),
            "snapshot root table should default to col 3 desc"
        );
        // Inner directory: sort by name ascending
        let html_inner = render_directory_html("/", &tree, false);
        assert!(
            html_inner.contains(r#"data-sort-col="1" data-sort-dir="asc""#),
            "inner directory table should default to col 1 asc"
        );
    }
}
