// Tree-arena indexing — see file_tree/builder.rs for the invariant. All
// `arena[idx]` accesses use indices produced by `arena.push()` or stored in
// `TreeNode.parent` / `.children`, which are bounds-safe by construction.
#![allow(clippy::indexing_slicing)]

use super::{CheckState, FileTree};

impl FileTree {
    pub fn rebuild_visible(&mut self) {
        self.visible_rows.clear();
        let roots = self.roots.clone();
        for root in roots {
            Self::dfs_visible(&self.arena, &mut self.visible_rows, root);
        }
    }

    fn dfs_visible(arena: &[super::TreeNode], visible: &mut Vec<usize>, idx: usize) {
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

        self.set_check_recursive(node_idx, new_state);

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
                    for &child in &node.children {
                        self.collect_checked_node(child, paths);
                    }
                } else if !node.mixed_origins || node.item_paths.len() > 1 {
                    paths.extend(node.item_paths.iter().cloned());
                } else {
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

    pub fn selection_text(&self) -> String {
        let (checked, total) = self.count_checked();
        format!("{checked} / {total} files selected")
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{dir, file, find_node};
    use super::super::{CheckState, FileTree};

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

        let docs_idx = tree
            .arena
            .iter()
            .position(|n| n.full_path == "adam/Documents")
            .unwrap();
        tree.toggle_check(docs_idx);

        let paths = tree.collect_checked_paths();
        assert_eq!(paths, vec!["Documents"]);

        let pics = find_node(&tree, "adam/Pictures").unwrap();
        assert_eq!(pics.check_state, CheckState::Unchecked);

        let root = &tree.arena[tree.roots[0]];
        assert_eq!(root.check_state, CheckState::Partial);
    }
}
