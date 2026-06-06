// Tree-arena indexing — `visible_rows` only contains indices stored when
// the corresponding `arena` entry was created. See file_tree/builder.rs.
#![allow(clippy::indexing_slicing)]

use slint::SharedString;

use crate::TreeRowData;

use super::FileTree;

impl FileTree {
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
}
