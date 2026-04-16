use std::collections::HashMap;
use std::path::Path;

use slint::SharedString;
use vykar_core::snapshot::item::{Item, ItemType};

use crate::TreeRowData;
use vykar_common::display::format_bytes;

// ── Tree view data structures ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckState {
    Unchecked = 0,
    Checked = 1,
    Partial = 2,
}

#[derive(Debug, Clone)]
pub(crate) struct TreeNode {
    pub name: String,
    /// Display path (used in tests and for node identification).
    #[cfg_attr(not(test), allow(dead_code))]
    pub full_path: String,
    pub entry_type: String,
    pub permissions: String,
    pub size_str: String,
    pub parent: Option<usize>,
    pub children: Vec<usize>,
    pub expanded: bool,
    pub check_state: CheckState,
    pub depth: usize,
    pub is_dir: bool,
    /// Original item paths for restore. Empty = synthetic node (never collapse).
    /// Multiple entries = merged from overlapping sources (emit all on collapse).
    pub item_paths: Vec<String>,
    /// True if this node or any descendant has merged origins (item_paths.len() > 1)
    /// or is synthetic. When true, a Checked node with a single item_path must
    /// recurse instead of collapsing, because descendants may have origins not
    /// covered by this node's item_path prefix.
    pub mixed_origins: bool,
}

pub(crate) struct FileTree {
    pub arena: Vec<TreeNode>,
    pub roots: Vec<usize>,
    pub visible_rows: Vec<usize>,
}

// ── Path transformation ──

struct TransformedItem {
    display_path: String,
    item_path: String,
    entry_type: ItemType,
    mode: u32,
    size: u64,
}

/// Compute the longest common directory-component prefix of all paths.
/// Returns empty string if paths is empty. Preserves the leading `/` for
/// absolute Unix paths; omits it for Windows-style paths (e.g. `C:/...`).
fn common_directory_prefix(paths: &[String]) -> String {
    if paths.is_empty() {
        return String::new();
    }

    fn split_components(p: &str) -> Vec<&str> {
        p.split('/').filter(|c| !c.is_empty()).collect()
    }

    let has_leading_slash = paths[0].starts_with('/');

    let first_components = split_components(&paths[0]);
    let mut common_len = first_components.len();

    for path in &paths[1..] {
        let components = split_components(path);
        common_len = common_len.min(components.len());
        for i in 0..common_len {
            if first_components[i] != components[i] {
                common_len = i;
                break;
            }
        }
    }

    if common_len == 0 {
        return if has_leading_slash {
            "/".to_string()
        } else {
            String::new()
        };
    }

    let joined = first_components[..common_len].join("/");
    if has_leading_slash {
        format!("/{joined}")
    } else {
        joined
    }
}

/// Transform items using source_paths to reconstruct absolute-ish display paths
/// with the common prefix stripped.
///
/// Returns `(transformed_items, common_prefix_for_display)`.
fn transform_items(items: &[Item], source_paths: &[String]) -> (Vec<TransformedItem>, String) {
    // Normalize source_paths: replace backslashes with forward slashes (Windows
    // config paths are stored as-is in snapshot metadata) so prefix computation
    // and splitting work uniformly.
    let source_paths: Vec<String> = source_paths.iter().map(|p| p.replace('\\', "/")).collect();
    let source_paths = source_paths.as_slice();

    if source_paths.is_empty() {
        // Backward compat: no transformation
        let transformed = items
            .iter()
            .map(|item| TransformedItem {
                display_path: item.path.clone(),
                item_path: item.path.clone(),
                entry_type: item.entry_type,
                mode: item.mode,
                size: item.size,
            })
            .collect();
        return (transformed, String::new());
    }

    let mut common_prefix = common_directory_prefix(source_paths);

    // If any source path equals the common prefix, go up one level so every
    // source contributes at least one directory level below the root.
    if source_paths.iter().any(|sp| sp == &common_prefix) {
        common_prefix = Path::new(&common_prefix)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "/".to_string());
    }

    let strip_prefix = if common_prefix == "/" {
        ""
    } else {
        common_prefix.as_str()
    };

    if source_paths.len() == 1 {
        // Single source: prepend the relative source path to each item.
        let source = &source_paths[0];
        let display_prefix = source
            .strip_prefix(strip_prefix)
            .unwrap_or(source)
            .trim_start_matches('/');

        let transformed = items
            .iter()
            .map(|item| {
                let display_path = if display_prefix.is_empty() {
                    item.path.clone()
                } else if item.path.is_empty() {
                    display_prefix.to_string()
                } else {
                    format!("{display_prefix}/{}", item.path)
                };
                TransformedItem {
                    display_path,
                    item_path: item.path.clone(),
                    entry_type: item.entry_type,
                    mode: item.mode,
                    size: item.size,
                }
            })
            .collect();
        return (transformed, common_prefix);
    }

    // Multi-source: build basename → display_prefix map.
    let mut basename_to_display: HashMap<String, String> = HashMap::new();
    for source in source_paths {
        let basename = Path::new(source)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| source.clone());
        let display_prefix = source
            .strip_prefix(strip_prefix)
            .unwrap_or(source)
            .trim_start_matches('/')
            .to_string();
        basename_to_display.insert(basename, display_prefix);
    }

    let transformed = items
        .iter()
        .map(|item| {
            let first_component = item.path.split('/').next().unwrap_or("");
            let display_path =
                if let Some(display_prefix) = basename_to_display.get(first_component) {
                    let rest = item
                        .path
                        .strip_prefix(first_component)
                        .unwrap_or("")
                        .trim_start_matches('/');
                    if rest.is_empty() {
                        display_prefix.clone()
                    } else {
                        format!("{display_prefix}/{rest}")
                    }
                } else {
                    // Unmatched (e.g. vykar-dumps/) — keep as-is
                    item.path.clone()
                };
            TransformedItem {
                display_path,
                item_path: item.path.clone(),
                entry_type: item.entry_type,
                mode: item.mode,
                size: item.size,
            }
        })
        .collect();

    (transformed, common_prefix)
}

impl FileTree {
    /// Build tree from snapshot items, transforming paths using source_paths.
    /// Returns `(tree, common_prefix_display)` for the UI header.
    pub fn build_from_items(items: &[Item], source_paths: &[String]) -> (Self, String) {
        let (transformed, common_prefix) = transform_items(items, source_paths);

        let mut arena = Vec::new();
        let mut roots = Vec::new();
        // Map from display directory path to node index
        let mut dir_map: HashMap<String, usize> = HashMap::new();

        // First pass: create directory nodes from paths.
        // For each item, ensure all parent directories exist as nodes.
        for ti in &transformed {
            let parts: Vec<&str> = ti.display_path.split('/').collect();
            let mut current_path = String::new();

            for (i, part) in parts.iter().enumerate() {
                if i > 0 {
                    current_path.push('/');
                }
                current_path.push_str(part);

                let is_last = i == parts.len() - 1;

                if is_last && ti.entry_type != ItemType::Directory {
                    // File/symlink — handled in second pass
                    continue;
                }

                if let Some(&existing_idx) = dir_map.get(&current_path) {
                    // Directory node already exists. If this is a real directory
                    // item (not just an intermediate parent), merge its item_path.
                    if is_last {
                        let node: &mut TreeNode = &mut arena[existing_idx];
                        if !node.item_paths.contains(&ti.item_path) {
                            node.item_paths.push(ti.item_path.clone());
                        }
                        // Update permissions from the real item if we had none
                        if node.permissions.is_empty() {
                            node.permissions = format!("{:o}", ti.mode & 0o7777);
                        }
                    }
                    continue;
                }

                let parent_path = if i > 0 {
                    let idx = current_path.rfind('/').unwrap();
                    Some(current_path[..idx].to_string())
                } else {
                    None
                };

                let parent_idx = parent_path.as_ref().and_then(|p| dir_map.get(p).copied());

                // A node is "real" (has item_paths) only when it's the last
                // component AND came from a directory item. Intermediate parent
                // components are synthetic (empty item_paths).
                let item_paths = if is_last {
                    vec![ti.item_path.clone()]
                } else {
                    vec![]
                };

                let permissions = if is_last {
                    format!("{:o}", ti.mode & 0o7777)
                } else {
                    String::new()
                };

                let node_idx = arena.len();
                arena.push(TreeNode {
                    name: part.to_string(),
                    full_path: current_path.clone(),
                    entry_type: "dir".to_string(),
                    permissions,
                    size_str: String::new(),
                    parent: parent_idx,
                    children: Vec::new(),
                    expanded: false,
                    check_state: CheckState::Unchecked,
                    depth: i,
                    is_dir: true,
                    item_paths,
                    mixed_origins: false,
                });

                if let Some(pidx) = parent_idx {
                    arena[pidx].children.push(node_idx);
                } else {
                    roots.push(node_idx);
                }

                dir_map.insert(current_path.clone(), node_idx);
            }
        }

        // Second pass: add files and symlinks, deduplicating by display path.
        let mut file_map: HashMap<String, usize> = HashMap::new();

        for ti in &transformed {
            if ti.entry_type == ItemType::Directory {
                continue;
            }

            // Check for duplicate display path (from overlapping sources)
            if let Some(&existing_idx) = file_map.get(&ti.display_path) {
                let node = &mut arena[existing_idx];
                if !node.item_paths.contains(&ti.item_path) {
                    node.item_paths.push(ti.item_path.clone());
                }
                continue;
            }

            let parts: Vec<&str> = ti.display_path.split('/').collect();
            let depth = parts.len() - 1;

            let parent_path = if parts.len() > 1 {
                Some(parts[..parts.len() - 1].join("/"))
            } else {
                None
            };

            let parent_idx = parent_path.as_ref().and_then(|p| dir_map.get(p).copied());
            let name = parts.last().unwrap().to_string();

            let type_str = match ti.entry_type {
                ItemType::RegularFile => "file",
                ItemType::Symlink => "link",
                ItemType::Directory => unreachable!(),
            }
            .to_string();

            let node_idx = arena.len();
            arena.push(TreeNode {
                name,
                full_path: ti.display_path.clone(),
                entry_type: type_str,
                permissions: format!("{:o}", ti.mode & 0o7777),
                size_str: format_bytes(ti.size),
                parent: parent_idx,
                children: Vec::new(),
                expanded: false,
                check_state: CheckState::Unchecked,
                depth,
                is_dir: false,
                item_paths: vec![ti.item_path.clone()],
                mixed_origins: false,
            });

            if let Some(pidx) = parent_idx {
                arena[pidx].children.push(node_idx);
            } else {
                roots.push(node_idx);
            }

            file_map.insert(ti.display_path.clone(), node_idx);
        }

        // Bottom-up pass: propagate mixed_origins. A node is mixed if it has
        // multiple item_paths, is synthetic (empty), or any child is mixed.
        // Process in reverse arena order (children before parents by construction).
        for i in (0..arena.len()).rev() {
            if arena[i].item_paths.len() != 1 {
                arena[i].mixed_origins = true;
            }
            if arena[i].mixed_origins {
                if let Some(pidx) = arena[i].parent {
                    arena[pidx].mixed_origins = true;
                }
            }
        }

        // Sort children of each node alphabetically (dirs first, then files)
        let sort_key: Vec<(bool, String)> = arena
            .iter()
            .map(|n| (n.is_dir, n.name.to_lowercase()))
            .collect();
        for node in &mut arena {
            node.children.sort_by(|a, b| {
                let (ad, ref an) = sort_key[*a];
                let (bd, ref bn) = sort_key[*b];
                bd.cmp(&ad).then_with(|| an.cmp(bn))
            });
        }
        roots.sort_by(|a, b| {
            let (ad, ref an) = sort_key[*a];
            let (bd, ref bn) = sort_key[*b];
            bd.cmp(&ad).then_with(|| an.cmp(bn))
        });

        let mut tree = FileTree {
            arena,
            roots,
            visible_rows: Vec::new(),
        };
        tree.rebuild_visible();
        (tree, common_prefix)
    }

    pub fn rebuild_visible(&mut self) {
        self.visible_rows.clear();
        let roots = self.roots.clone();
        for root in roots {
            Self::dfs_visible(&self.arena, &mut self.visible_rows, root);
        }
    }

    fn dfs_visible(arena: &[TreeNode], visible: &mut Vec<usize>, idx: usize) {
        visible.push(idx);
        if arena[idx].is_dir && arena[idx].expanded {
            let children = arena[idx].children.clone();
            for child in children {
                Self::dfs_visible(arena, visible, child);
            }
        }
    }

    pub fn toggle_expanded(&mut self, node_idx: usize) {
        if node_idx < self.arena.len() && self.arena[node_idx].is_dir {
            self.arena[node_idx].expanded = !self.arena[node_idx].expanded;
            self.rebuild_visible();
        }
    }

    pub fn expand_all(&mut self) {
        for node in &mut self.arena {
            if node.is_dir {
                node.expanded = true;
            }
        }
        self.rebuild_visible();
    }

    pub fn collapse_all(&mut self) {
        for node in &mut self.arena {
            if node.is_dir {
                node.expanded = false;
            }
        }
        self.rebuild_visible();
    }

    pub fn toggle_check(&mut self, node_idx: usize) {
        if node_idx >= self.arena.len() {
            return;
        }

        let new_state = match self.arena[node_idx].check_state {
            CheckState::Unchecked => CheckState::Checked,
            CheckState::Checked | CheckState::Partial => CheckState::Unchecked,
        };

        // Set this node and all descendants
        self.set_check_recursive(node_idx, new_state);

        // Walk up parents to recalculate
        let mut current = self.arena[node_idx].parent;
        while let Some(parent_idx) = current {
            self.recalc_parent_check(parent_idx);
            current = self.arena[parent_idx].parent;
        }
    }

    fn set_check_recursive(&mut self, idx: usize, state: CheckState) {
        self.arena[idx].check_state = state;
        let children: Vec<usize> = self.arena[idx].children.clone();
        for child in children {
            self.set_check_recursive(child, state);
        }
    }

    fn recalc_parent_check(&mut self, idx: usize) {
        let children = &self.arena[idx].children;
        if children.is_empty() {
            return;
        }
        let all_checked = children
            .iter()
            .all(|&c| self.arena[c].check_state == CheckState::Checked);
        let all_unchecked = children
            .iter()
            .all(|&c| self.arena[c].check_state == CheckState::Unchecked);

        self.arena[idx].check_state = if all_checked {
            CheckState::Checked
        } else if all_unchecked {
            CheckState::Unchecked
        } else {
            CheckState::Partial
        };
    }

    pub fn select_all(&mut self) {
        for node in &mut self.arena {
            node.check_state = CheckState::Checked;
        }
    }

    pub fn deselect_all(&mut self) {
        for node in &mut self.arena {
            node.check_state = CheckState::Unchecked;
        }
    }

    pub fn count_checked(&self) -> (usize, usize) {
        let total = self.arena.iter().filter(|n| !n.is_dir).count();
        let checked = self
            .arena
            .iter()
            .filter(|n| !n.is_dir && n.check_state == CheckState::Checked)
            .count();
        (checked, total)
    }

    pub fn collect_checked_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        for &root in &self.roots {
            self.collect_checked_node(root, &mut paths);
        }
        paths
    }

    fn collect_checked_node(&self, idx: usize, paths: &mut Vec<String>) {
        let node = &self.arena[idx];
        match node.check_state {
            CheckState::Checked => {
                if node.item_paths.is_empty() {
                    // Synthetic node — no item path, must recurse
                    for &child in &node.children {
                        self.collect_checked_node(child, paths);
                    }
                } else if !node.mixed_origins || node.item_paths.len() > 1 {
                    // Safe to collapse:
                    // - !mixed_origins: single-origin subtree, one prefix covers all
                    // - len > 1: merge point, multiple prefixes cover all origins
                    paths.extend(node.item_paths.iter().cloned());
                } else {
                    // mixed_origins + single item_path: this node's one prefix
                    // cannot cover merged descendants with different origins.
                    // Emit own path (so this directory's metadata is restored)
                    // AND recurse to reach the actual merge points.
                    paths.extend(node.item_paths.iter().cloned());
                    for &child in &node.children {
                        self.collect_checked_node(child, paths);
                    }
                }
            }
            CheckState::Partial => {
                for &child in &node.children {
                    self.collect_checked_node(child, paths);
                }
            }
            CheckState::Unchecked => {}
        }
    }

    pub fn to_slint_model(&self) -> Vec<TreeRowData> {
        self.visible_rows
            .iter()
            .map(|&idx| {
                let node = &self.arena[idx];
                TreeRowData {
                    name: SharedString::from(&node.name),
                    type_str: SharedString::from(&node.entry_type),
                    permissions: SharedString::from(&node.permissions),
                    size_str: SharedString::from(&node.size_str),
                    depth: node.depth as i32,
                    is_dir: node.is_dir,
                    expanded: node.expanded,
                    check_state: node.check_state as i32,
                    node_index: idx as i32,
                }
            })
            .collect()
    }

    pub fn selection_text(&self) -> String {
        let (checked, total) = self.count_checked();
        format!("{checked} / {total} files selected")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vykar_core::snapshot::item::{Item, ItemType};

    fn dir(path: &str) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::Directory,
            mode: 0o755,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: 0,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
        }
    }

    fn file(path: &str, size: u64) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
        }
    }

    /// Find a node by its display path (full_path).
    fn find_node<'a>(tree: &'a FileTree, display_path: &str) -> Option<&'a TreeNode> {
        tree.arena.iter().find(|n| n.full_path == display_path)
    }

    fn sorted(mut v: Vec<String>) -> Vec<String> {
        v.sort();
        v
    }

    // Test 1: Single source, production-shaped
    #[test]
    fn single_source_basic() {
        let items = vec![
            dir("Documents"),
            file("Documents/a.txt", 100),
            file("Documents/b.txt", 200),
            dir("Pictures"),
            file("Pictures/photo.jpg", 300),
        ];
        let source_paths = vec!["/home/adam".to_string()];

        let (mut tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "/home");

        // Root should be synthetic "adam"
        assert_eq!(tree.roots.len(), 1);
        let root = &tree.arena[tree.roots[0]];
        assert_eq!(root.name, "adam");
        assert!(root.item_paths.is_empty(), "root should be synthetic");

        // Real directory nodes should have item_paths
        let docs = find_node(&tree, "adam/Documents").unwrap();
        assert_eq!(docs.item_paths, vec!["Documents"]);

        // Select all → collect should recurse through synthetic root
        tree.select_all();
        let paths = sorted(tree.collect_checked_paths());
        assert_eq!(paths, vec!["Documents", "Pictures"]);
    }

    // Test 2: Multi-source, shared parent
    #[test]
    fn multi_source_shared_parent() {
        let items = vec![
            dir("alice"),
            file("alice/file.txt", 100),
            dir("bob"),
            file("bob/file.txt", 100),
        ];
        let source_paths = vec!["/home/alice".to_string(), "/home/bob".to_string()];

        let (mut tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "/home");

        // Roots should be alice/ and bob/
        assert_eq!(tree.roots.len(), 2);
        let root_names: Vec<&str> = tree
            .roots
            .iter()
            .map(|&i| tree.arena[i].name.as_str())
            .collect();
        assert!(root_names.contains(&"alice"));
        assert!(root_names.contains(&"bob"));

        // alice/ is a real dir (from dir item) so it should collapse
        let alice = find_node(&tree, "alice").unwrap();
        assert_eq!(alice.item_paths, vec!["alice"]);

        tree.select_all();
        let paths = sorted(tree.collect_checked_paths());
        assert_eq!(paths, vec!["alice", "bob"]);
    }

    // Test 3: Multi-source, nested/overlapping
    #[test]
    fn multi_source_nested_overlapping() {
        let items = vec![
            // From source /home/adam (basename "adam")
            dir("adam"),
            dir("adam/Documents"),
            file("adam/Documents/a.txt", 100),
            dir("adam/.thunderbird"),
            file("adam/.thunderbird/prefs.js", 50),
            // From source /home/adam/.thunderbird (basename ".thunderbird")
            dir(".thunderbird"),
            file(".thunderbird/prefs.js", 50),
        ];
        let source_paths = vec![
            "/home/adam".to_string(),
            "/home/adam/.thunderbird".to_string(),
        ];

        let (mut tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "/home");

        // Single root "adam" since .thunderbird maps into adam/.thunderbird
        assert_eq!(tree.roots.len(), 1);
        assert_eq!(tree.arena[tree.roots[0]].name, "adam");

        // Merged dir: adam/.thunderbird has item_paths from both sources
        let tb = find_node(&tree, "adam/.thunderbird").unwrap();
        assert_eq!(
            sorted(tb.item_paths.clone()),
            vec![".thunderbird", "adam/.thunderbird"]
        );

        // Deduped file: only one node for prefs.js
        let prefs_count = tree
            .arena
            .iter()
            .filter(|n| n.full_path == "adam/.thunderbird/prefs.js")
            .count();
        assert_eq!(prefs_count, 1);

        // The file should have both item_paths
        let prefs = find_node(&tree, "adam/.thunderbird/prefs.js").unwrap();
        assert_eq!(
            sorted(prefs.item_paths.clone()),
            vec![".thunderbird/prefs.js", "adam/.thunderbird/prefs.js"]
        );

        // Check adam/.thunderbird → emits both origins
        let tb_idx = tree
            .arena
            .iter()
            .position(|n| n.full_path == "adam/.thunderbird")
            .unwrap();
        tree.toggle_check(tb_idx);
        let paths = sorted(tree.collect_checked_paths());
        assert_eq!(paths, vec![".thunderbird", "adam/.thunderbird"]);

        // Check adam/Documents → single origin, collapses normally
        tree.deselect_all();
        let docs_idx = tree
            .arena
            .iter()
            .position(|n| n.full_path == "adam/Documents")
            .unwrap();
        tree.toggle_check(docs_idx);
        let paths = tree.collect_checked_paths();
        assert_eq!(paths, vec!["adam/Documents"]);

        // Select All — ancestor adam/ has mixed_origins, must recurse through
        // merged subtree to emit both origin prefixes for .thunderbird
        tree.deselect_all();
        tree.select_all();
        let paths = sorted(tree.collect_checked_paths());
        assert_eq!(
            paths,
            vec![
                ".thunderbird",
                "adam",
                "adam/.thunderbird",
                "adam/Documents",
            ]
        );
    }

    // Test 4a: Command dumps in multi-source — unmatched roots kept as-is
    #[test]
    fn command_dumps_multi_source() {
        let items = vec![
            dir("alice"),
            file("alice/file.txt", 100),
            dir("vykar-dumps"),
            file("vykar-dumps/db.sql", 5000),
        ];
        let source_paths = vec!["/home/alice".to_string(), "/home/bob".to_string()];

        let (mut tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "/home");

        // vykar-dumps doesn't match any source basename, kept as-is at root
        let dumps = find_node(&tree, "vykar-dumps").unwrap();
        assert_eq!(dumps.item_paths, vec!["vykar-dumps"]);

        // Check vykar-dumps → collapses to its item_path
        let dumps_idx = tree
            .arena
            .iter()
            .position(|n| n.full_path == "vykar-dumps")
            .unwrap();
        tree.toggle_check(dumps_idx);
        let paths = tree.collect_checked_paths();
        assert_eq!(paths, vec!["vykar-dumps"]);
    }

    // Test 4b: Command dumps in single-source — appear inside source prefix
    // (single-source items have no basename prefix so all items are treated
    // as belonging to the source)
    #[test]
    fn command_dumps_single_source() {
        let items = vec![
            dir("Documents"),
            file("Documents/a.txt", 100),
            dir("vykar-dumps"),
            file("vykar-dumps/db.sql", 5000),
        ];
        let source_paths = vec!["/home/user".to_string()];

        let (mut tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "/home");

        // Single root "user" containing both Documents and vykar-dumps
        assert_eq!(tree.roots.len(), 1);
        assert_eq!(tree.arena[tree.roots[0]].name, "user");

        // vykar-dumps appears under user/ but its item_path is the original
        let dumps = find_node(&tree, "user/vykar-dumps").unwrap();
        assert_eq!(dumps.item_paths, vec!["vykar-dumps"]);

        // Checking vykar-dumps restores correctly via original item_path
        let dumps_idx = tree
            .arena
            .iter()
            .position(|n| n.full_path == "user/vykar-dumps")
            .unwrap();
        tree.toggle_check(dumps_idx);
        let paths = tree.collect_checked_paths();
        assert_eq!(paths, vec!["vykar-dumps"]);
    }

    // Test 5: Disjoint paths
    #[test]
    fn disjoint_paths() {
        let items = vec![
            dir("user"),
            file("user/a.txt", 100),
            dir("data"),
            file("data/b.txt", 200),
        ];
        let source_paths = vec!["/home/user".to_string(), "/var/data".to_string()];

        let (mut tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "/");

        // "home" and "var" are synthetic roots
        let root_names: Vec<&str> = tree
            .roots
            .iter()
            .map(|&i| tree.arena[i].name.as_str())
            .collect();
        assert!(root_names.contains(&"home"));
        assert!(root_names.contains(&"var"));

        let home = find_node(&tree, "home").unwrap();
        assert!(home.item_paths.is_empty(), "home should be synthetic");

        let var = find_node(&tree, "var").unwrap();
        assert!(var.item_paths.is_empty(), "var should be synthetic");

        // Real dirs inside
        let user = find_node(&tree, "home/user").unwrap();
        assert_eq!(user.item_paths, vec!["user"]);

        let data = find_node(&tree, "var/data").unwrap();
        assert_eq!(data.item_paths, vec!["data"]);

        // Select all — synthetic roots recurse
        tree.select_all();
        let paths = sorted(tree.collect_checked_paths());
        assert_eq!(paths, vec!["data", "user"]);
    }

    // Test 6: Empty source_paths (backward compat)
    #[test]
    fn empty_source_paths_backward_compat() {
        let items = vec![dir("Documents"), file("Documents/a.txt", 100)];
        let source_paths: Vec<String> = vec![];

        let (mut tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "");

        // Root is Documents directly (no transformation)
        assert_eq!(tree.roots.len(), 1);
        assert_eq!(tree.arena[tree.roots[0]].name, "Documents");

        // item_paths should be set from the original path
        let docs = find_node(&tree, "Documents").unwrap();
        assert_eq!(docs.item_paths, vec!["Documents"]);

        tree.select_all();
        let paths = tree.collect_checked_paths();
        assert_eq!(paths, vec!["Documents"]);
    }

    // Test 7: Partial selection with real dirs
    #[test]
    fn partial_selection_real_dirs() {
        let items = vec![
            dir("Documents"),
            file("Documents/a.txt", 100),
            file("Documents/b.txt", 200),
            dir("Pictures"),
            file("Pictures/photo.jpg", 300),
        ];
        let source_paths = vec!["/home/adam".to_string()];

        let (mut tree, _) = FileTree::build_from_items(&items, &source_paths);

        // Check only Documents
        let docs_idx = tree
            .arena
            .iter()
            .position(|n| n.full_path == "adam/Documents")
            .unwrap();
        tree.toggle_check(docs_idx);

        let paths = tree.collect_checked_paths();
        assert_eq!(paths, vec!["Documents"]);

        // Pictures should remain unchecked
        let pics = find_node(&tree, "adam/Pictures").unwrap();
        assert_eq!(pics.check_state, CheckState::Unchecked);

        // Root adam should be Partial
        let root = &tree.arena[tree.roots[0]];
        assert_eq!(root.check_state, CheckState::Partial);
    }

    // Test 8: Return value — common prefix string
    #[test]
    fn common_prefix_return_values() {
        let items = vec![dir("Documents")];

        // Single source
        let (_, prefix) = FileTree::build_from_items(&items, &["/home/adam".to_string()]);
        assert_eq!(prefix, "/home");

        // Disjoint sources
        let items2 = vec![dir("user"), dir("data")];
        let (_, prefix) = FileTree::build_from_items(
            &items2,
            &["/home/user".to_string(), "/var/data".to_string()],
        );
        assert_eq!(prefix, "/");

        // Empty source_paths
        let (_, prefix) = FileTree::build_from_items(&items, &[]);
        assert_eq!(prefix, "");
    }

    // Test 9: Windows-style backslash paths are normalized
    #[test]
    fn windows_source_paths_normalized() {
        let items = vec![dir("Documents"), file("Documents/a.txt", 100)];
        let source_paths = vec![r"C:\Users\Alice".to_string()];

        let (tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "C:/Users");

        // Root should be "Alice" (the basename after normalization)
        assert_eq!(tree.roots.len(), 1);
        assert_eq!(tree.arena[tree.roots[0]].name, "Alice");

        // Items should appear under Alice/
        let docs = find_node(&tree, "Alice/Documents");
        assert!(docs.is_some());
    }
}
