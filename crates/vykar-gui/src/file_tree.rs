use std::collections::HashMap;

use slint::SharedString;
use vykar_core::snapshot::item::{Item, ItemType};

use crate::progress::format_bytes;
use crate::TreeRowData;

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
}

pub(crate) struct FileTree {
    pub arena: Vec<TreeNode>,
    pub roots: Vec<usize>,
    pub visible_rows: Vec<usize>,
}

impl FileTree {
    pub fn build_from_items(items: &[Item]) -> Self {
        let mut arena = Vec::new();
        let mut roots = Vec::new();
        // Map from directory path to node index
        let mut dir_map: HashMap<String, usize> = HashMap::new();

        // First pass: create all directory nodes from paths
        // We need to ensure parent directories exist for every item
        for item in items {
            let parts: Vec<&str> = item.path.split('/').collect();
            let mut current_path = String::new();

            // Create directory nodes for all parent components
            for (i, part) in parts.iter().enumerate() {
                if i > 0 {
                    current_path.push('/');
                }
                current_path.push_str(part);

                let is_last = i == parts.len() - 1;

                if is_last && item.entry_type != ItemType::Directory {
                    // This is a file/symlink — will be added in second pass
                    continue;
                }

                if dir_map.contains_key(&current_path) {
                    continue;
                }

                let parent_path = if i > 0 {
                    let idx = current_path.rfind('/').unwrap();
                    Some(current_path[..idx].to_string())
                } else {
                    None
                };

                let parent_idx = parent_path.as_ref().and_then(|p| dir_map.get(p).copied());

                let type_str = "dir".to_string();
                let permissions = if is_last {
                    format!("{:o}", item.mode & 0o7777)
                } else {
                    String::new()
                };
                let size_str = String::new();

                let node_idx = arena.len();
                arena.push(TreeNode {
                    name: part.to_string(),
                    full_path: current_path.clone(),
                    entry_type: type_str,
                    permissions,
                    size_str,
                    parent: parent_idx,
                    children: Vec::new(),
                    expanded: false,
                    check_state: CheckState::Unchecked,
                    depth: i,
                    is_dir: true,
                });

                if let Some(pidx) = parent_idx {
                    arena[pidx].children.push(node_idx);
                } else {
                    roots.push(node_idx);
                }

                dir_map.insert(current_path.clone(), node_idx);
            }
        }

        // Second pass: add files and symlinks
        for item in items {
            if item.entry_type == ItemType::Directory {
                continue;
            }

            let parts: Vec<&str> = item.path.split('/').collect();
            let depth = parts.len() - 1;

            let parent_path = if parts.len() > 1 {
                Some(parts[..parts.len() - 1].join("/"))
            } else {
                None
            };

            let parent_idx = parent_path.as_ref().and_then(|p| dir_map.get(p).copied());
            let name = parts.last().unwrap().to_string();

            let type_str = match item.entry_type {
                ItemType::RegularFile => "file",
                ItemType::Symlink => "link",
                ItemType::Directory => unreachable!(),
            }
            .to_string();

            let node_idx = arena.len();
            arena.push(TreeNode {
                name,
                full_path: item.path.clone(),
                entry_type: type_str,
                permissions: format!("{:o}", item.mode & 0o7777),
                size_str: format_bytes(item.size),
                parent: parent_idx,
                children: Vec::new(),
                expanded: false,
                check_state: CheckState::Unchecked,
                depth,
                is_dir: false,
            });

            if let Some(pidx) = parent_idx {
                arena[pidx].children.push(node_idx);
            } else {
                roots.push(node_idx);
            }
        }

        // Sort children of each node alphabetically (dirs first, then files)
        let arena_snapshot: Vec<(bool, String)> =
            arena.iter().map(|n| (n.is_dir, n.name.clone())).collect();
        for node in &mut arena {
            node.children.sort_by(|&a, &b| {
                let (a_dir, ref a_name) = arena_snapshot[a];
                let (b_dir, ref b_name) = arena_snapshot[b];
                b_dir
                    .cmp(&a_dir)
                    .then_with(|| a_name.to_lowercase().cmp(&b_name.to_lowercase()))
            });
        }
        // Also sort roots
        roots.sort_by(|&a, &b| {
            let (a_dir, ref a_name) = arena_snapshot[a];
            let (b_dir, ref b_name) = arena_snapshot[b];
            b_dir
                .cmp(&a_dir)
                .then_with(|| a_name.to_lowercase().cmp(&b_name.to_lowercase()))
        });

        let mut tree = FileTree {
            arena,
            roots,
            visible_rows: Vec::new(),
        };
        tree.rebuild_visible();
        tree
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
        // Collect minimal set: if a directory is fully checked, just include that dir
        // Otherwise include individual checked files
        let mut paths = Vec::new();
        self.collect_checked_from_roots(&mut paths);
        paths
    }

    fn collect_checked_from_roots(&self, paths: &mut Vec<String>) {
        for &root in &self.roots {
            self.collect_checked_node(root, paths);
        }
    }

    fn collect_checked_node(&self, idx: usize, paths: &mut Vec<String>) {
        let node = &self.arena[idx];
        match node.check_state {
            CheckState::Checked => {
                // Include this entire subtree
                paths.push(node.full_path.clone());
            }
            CheckState::Partial => {
                // Recurse into children
                for &child in &node.children {
                    self.collect_checked_node(child, paths);
                }
            }
            CheckState::Unchecked => {
                // Skip
            }
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
