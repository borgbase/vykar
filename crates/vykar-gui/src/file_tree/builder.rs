// Tree-arena indexing: every `arena[idx]` access uses an index returned by
// `arena.push()` or stored in a `TreeNode.parent` / `.children` field, both
// of which are populated only with valid in-bounds indices in this module.
#![allow(clippy::indexing_slicing)]

use std::collections::HashMap;

use vykar_common::display::format_bytes;
use vykar_core::snapshot::item::{Item, ItemType};

use super::path::transform_items;
use super::{CheckState, FileTree, TreeNode};

impl FileTree {
    /// Build tree from snapshot items, transforming paths using source_paths.
    /// Returns `(tree, common_prefix_display)` for the UI header.
    pub fn build_from_items(items: &[Item], source_paths: &[String]) -> (Self, String) {
        let (transformed, common_prefix) = transform_items(items, source_paths);

        let mut arena = Vec::new();
        let mut roots = Vec::new();
        let mut dir_map: HashMap<String, usize> = HashMap::new();

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
                    continue;
                }

                if let Some(&existing_idx) = dir_map.get(&current_path) {
                    if is_last {
                        let node: &mut TreeNode = &mut arena[existing_idx];
                        if !node.item_paths.contains(&ti.item_path) {
                            node.item_paths.push(ti.item_path.clone());
                        }
                        if node.permissions.is_empty() {
                            node.permissions = format!("{:o}", ti.mode & 0o7777);
                        }
                    }
                    continue;
                }

                let parent_path = if i > 0 {
                    let idx = current_path
                        .rfind('/')
                        .expect("nested path contains separator");
                    Some(current_path[..idx].to_string())
                } else {
                    None
                };

                let parent_idx = parent_path.as_ref().and_then(|p| dir_map.get(p).copied());
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

        let mut file_map: HashMap<String, usize> = HashMap::new();

        for ti in &transformed {
            if ti.entry_type == ItemType::Directory {
                continue;
            }

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
            let name = parts
                .last()
                .expect("split path has at least one part")
                .to_string();

            let type_str = match ti.entry_type {
                ItemType::Symlink => "link",
                ItemType::RegularFile | ItemType::Directory => "file",
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
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{dir, file, find_node, sorted};
    use super::super::FileTree;

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

        assert_eq!(tree.roots.len(), 1);
        let root = &tree.arena[tree.roots[0]];
        assert_eq!(root.name, "adam");
        assert!(root.item_paths.is_empty(), "root should be synthetic");

        let docs = find_node(&tree, "adam/Documents").unwrap();
        assert_eq!(docs.item_paths, vec!["Documents"]);

        tree.select_all();
        let paths = sorted(tree.collect_checked_paths());
        assert_eq!(paths, vec!["Documents", "Pictures"]);
    }

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

        assert_eq!(tree.roots.len(), 2);
        let root_names: Vec<&str> = tree
            .roots
            .iter()
            .map(|&i| tree.arena[i].name.as_str())
            .collect();
        assert!(root_names.contains(&"alice"));
        assert!(root_names.contains(&"bob"));

        let alice = find_node(&tree, "alice").unwrap();
        assert_eq!(alice.item_paths, vec!["alice"]);

        tree.select_all();
        let paths = sorted(tree.collect_checked_paths());
        assert_eq!(paths, vec!["alice", "bob"]);
    }

    #[test]
    fn multi_source_nested_overlapping() {
        let items = vec![
            dir("adam"),
            dir("adam/Documents"),
            file("adam/Documents/a.txt", 100),
            dir("adam/.thunderbird"),
            file("adam/.thunderbird/prefs.js", 50),
            dir(".thunderbird"),
            file(".thunderbird/prefs.js", 50),
        ];
        let source_paths = vec![
            "/home/adam".to_string(),
            "/home/adam/.thunderbird".to_string(),
        ];

        let (mut tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "/home");

        assert_eq!(tree.roots.len(), 1);
        assert_eq!(tree.arena[tree.roots[0]].name, "adam");

        let tb = find_node(&tree, "adam/.thunderbird").unwrap();
        assert_eq!(
            sorted(tb.item_paths.clone()),
            vec![".thunderbird", "adam/.thunderbird"]
        );

        let prefs_count = tree
            .arena
            .iter()
            .filter(|n| n.full_path == "adam/.thunderbird/prefs.js")
            .count();
        assert_eq!(prefs_count, 1);

        let prefs = find_node(&tree, "adam/.thunderbird/prefs.js").unwrap();
        assert_eq!(
            sorted(prefs.item_paths.clone()),
            vec![".thunderbird/prefs.js", "adam/.thunderbird/prefs.js"]
        );

        let tb_idx = tree
            .arena
            .iter()
            .position(|n| n.full_path == "adam/.thunderbird")
            .unwrap();
        tree.toggle_check(tb_idx);
        let paths = sorted(tree.collect_checked_paths());
        assert_eq!(paths, vec![".thunderbird", "adam/.thunderbird"]);

        tree.deselect_all();
        let docs_idx = tree
            .arena
            .iter()
            .position(|n| n.full_path == "adam/Documents")
            .unwrap();
        tree.toggle_check(docs_idx);
        let paths = tree.collect_checked_paths();
        assert_eq!(paths, vec!["adam/Documents"]);

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

        let dumps = find_node(&tree, "vykar-dumps").unwrap();
        assert_eq!(dumps.item_paths, vec!["vykar-dumps"]);

        let dumps_idx = tree
            .arena
            .iter()
            .position(|n| n.full_path == "vykar-dumps")
            .unwrap();
        tree.toggle_check(dumps_idx);
        let paths = tree.collect_checked_paths();
        assert_eq!(paths, vec!["vykar-dumps"]);
    }

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

        assert_eq!(tree.roots.len(), 1);
        assert_eq!(tree.arena[tree.roots[0]].name, "user");

        let dumps = find_node(&tree, "user/vykar-dumps").unwrap();
        assert_eq!(dumps.item_paths, vec!["vykar-dumps"]);

        let dumps_idx = tree
            .arena
            .iter()
            .position(|n| n.full_path == "user/vykar-dumps")
            .unwrap();
        tree.toggle_check(dumps_idx);
        let paths = tree.collect_checked_paths();
        assert_eq!(paths, vec!["vykar-dumps"]);
    }

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

        let user = find_node(&tree, "home/user").unwrap();
        assert_eq!(user.item_paths, vec!["user"]);

        let data = find_node(&tree, "var/data").unwrap();
        assert_eq!(data.item_paths, vec!["data"]);

        tree.select_all();
        let paths = sorted(tree.collect_checked_paths());
        assert_eq!(paths, vec!["data", "user"]);
    }

    #[test]
    fn empty_source_paths_backward_compat() {
        let items = vec![dir("Documents"), file("Documents/a.txt", 100)];
        let source_paths: Vec<String> = vec![];

        let (mut tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "");

        assert_eq!(tree.roots.len(), 1);
        assert_eq!(tree.arena[tree.roots[0]].name, "Documents");

        let docs = find_node(&tree, "Documents").unwrap();
        assert_eq!(docs.item_paths, vec!["Documents"]);

        tree.select_all();
        let paths = tree.collect_checked_paths();
        assert_eq!(paths, vec!["Documents"]);
    }

    #[test]
    fn windows_source_paths_normalized() {
        let items = vec![dir("Documents"), file("Documents/a.txt", 100)];
        let source_paths = vec![r"C:\Users\Alice".to_string()];

        let (tree, prefix) = FileTree::build_from_items(&items, &source_paths);
        assert_eq!(prefix, "C:/Users");

        assert_eq!(tree.roots.len(), 1);
        assert_eq!(tree.arena[tree.roots[0]].name, "Alice");

        let docs = find_node(&tree, "Alice/Documents");
        assert!(docs.is_some());
    }
}
