#![cfg_attr(
    all(target_os = "windows", not(debug_assertions)),
    windows_subsystem = "windows"
)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local};
use crossbeam_channel::{Receiver, Sender};
use slint::{Model, ModelRc, SharedString, StandardListViewItem, VecModel};
use tray_icon::menu::{Menu, MenuEvent, MenuId, MenuItem, Submenu};
use tray_icon::{Icon, TrayIconBuilder};
use vykar_core::app::{self, operations, passphrase};
use vykar_core::commands::find::{FileStatus, FindFilter, FindScope};
use vykar_core::commands::init;
use vykar_core::config::{self, ResolvedRepo, ScheduleConfig};
use vykar_core::snapshot::item::{Item, ItemType};
use vykar_types::error::VykarError;

mod progress;
mod state;
use progress::{format_bytes, format_check_status, format_count, BackupStatusTracker};

const APP_TITLE: &str = "Vykar Backup";

// ── Tree view data structures ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckState {
    Unchecked = 0,
    Checked = 1,
    Partial = 2,
}

#[derive(Debug, Clone)]
struct TreeNode {
    name: String,
    full_path: String,
    entry_type: String,
    permissions: String,
    size_str: String,
    parent: Option<usize>,
    children: Vec<usize>,
    expanded: bool,
    check_state: CheckState,
    depth: usize,
    is_dir: bool,
}

struct FileTree {
    arena: Vec<TreeNode>,
    roots: Vec<usize>,
    visible_rows: Vec<usize>,
}

impl FileTree {
    fn build_from_items(items: &[Item]) -> Self {
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

    fn rebuild_visible(&mut self) {
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

    fn toggle_expanded(&mut self, node_idx: usize) {
        if node_idx < self.arena.len() && self.arena[node_idx].is_dir {
            self.arena[node_idx].expanded = !self.arena[node_idx].expanded;
            self.rebuild_visible();
        }
    }

    fn expand_all(&mut self) {
        for node in &mut self.arena {
            if node.is_dir {
                node.expanded = true;
            }
        }
        self.rebuild_visible();
    }

    fn collapse_all(&mut self) {
        for node in &mut self.arena {
            if node.is_dir {
                node.expanded = false;
            }
        }
        self.rebuild_visible();
    }

    fn toggle_check(&mut self, node_idx: usize) {
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

    fn select_all(&mut self) {
        for node in &mut self.arena {
            node.check_state = CheckState::Checked;
        }
    }

    fn deselect_all(&mut self) {
        for node in &mut self.arena {
            node.check_state = CheckState::Unchecked;
        }
    }

    fn count_checked(&self) -> (usize, usize) {
        let total = self.arena.iter().filter(|n| !n.is_dir).count();
        let checked = self
            .arena
            .iter()
            .filter(|n| !n.is_dir && n.check_state == CheckState::Checked)
            .count();
        (checked, total)
    }

    fn collect_checked_paths(&self) -> Vec<String> {
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

    fn to_slint_model(&self) -> Vec<TreeRowData> {
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

    fn selection_text(&self) -> String {
        let (checked, total) = self.count_checked();
        format!("{checked} / {total} files selected")
    }
}

slint::slint! {
    import { VerticalBox, HorizontalBox, Button, LineEdit, ScrollView, TabWidget, ComboBox, StandardTableView, ListView, CheckBox, Palette, GroupBox, TextEdit } from "std-widgets.slint";

    struct RepoInfo {
        name: string,
        url: string,
        snapshots: string,
        last_snapshot: string,
        size: string,
    }

    struct SourceInfo {
        label: string,
        paths: string,
        excludes: string,
        target_repos: string,
        expanded: bool,
        detail_paths: string,
        detail_excludes: string,
        detail_exclude_if_present: string,
        detail_flags: string,
        detail_hooks: string,
        detail_retention: string,
        detail_command_dumps: string,
    }

    // check_state: 0=unchecked, 1=checked, 2=partial
    struct TreeRowData {
        name: string,
        type_str: string,
        permissions: string,
        size_str: string,
        depth: int,
        is_dir: bool,
        expanded: bool,
        check_state: int,
        node_index: int,
    }

    export component RestoreWindow inherits Window {
        in-out property <string> snapshot_name;
        in-out property <string> repo_name;
        in-out property <string> status_text: "Ready";
        in-out property <string> selection_text: "";
        in-out property <[TreeRowData]> tree_rows: [];

        callback toggle_expanded(/* node_index */ int);
        callback toggle_checked(/* node_index */ int);
        callback expand_all_clicked();
        callback collapse_all_clicked();
        callback select_all_clicked();
        callback deselect_all_clicked();
        callback restore_selected_clicked();
        callback cancel_clicked();

        title: "Restore Snapshot";
        preferred-width: 900px;
        preferred-height: 600px;

        VerticalBox {
            padding: 12px;
            spacing: 8px;

            HorizontalLayout {
                spacing: 24px;
                HorizontalLayout {
                    spacing: 4px;
                    Text { text: "Snapshot:"; vertical-alignment: center; font-weight: 700; }
                    Text { text: root.snapshot_name; vertical-alignment: center; }
                }
                HorizontalLayout {
                    spacing: 4px;
                    Text { text: "Repository:"; vertical-alignment: center; font-weight: 700; }
                    Text { text: root.repo_name; vertical-alignment: center; }
                }
                Rectangle { horizontal-stretch: 1; }
            }

            // Toolbar
            HorizontalBox {
                spacing: 8px;
                Button { text: "Expand All"; clicked => { root.expand_all_clicked(); } }
                Button { text: "Collapse All"; clicked => { root.collapse_all_clicked(); } }
                Button { text: "Select All"; clicked => { root.select_all_clicked(); } }
                Button { text: "Deselect All"; clicked => { root.deselect_all_clicked(); } }
                Rectangle { horizontal-stretch: 1; }
                Text { text: root.selection_text; vertical-alignment: center; color: Palette.foreground; }
            }

            // Tree view
            Rectangle {
                vertical-stretch: 1;
                border-width: 1px;
                border-color: Palette.border;
                border-radius: 4px;
                background: Palette.alternate-background;
                ListView {
                for row[row_idx] in root.tree_rows: Rectangle {
                    height: 28px;
                    // Hover highlight — declared first so it sits behind the interactive layout
                    Rectangle {
                        background: ta.has-hover ? Palette.background.darker(3%) : transparent;
                        opacity: 0.5;
                        ta := TouchArea { }
                    }
                    HorizontalLayout {
                        padding-left: 4px;
                        padding-right: 8px;
                        spacing: 0px;

                        // Indentation
                        Rectangle { width: row.depth * 20px; }

                        // Expand/collapse arrow (for directories)
                        if row.is_dir: TouchArea {
                            width: 20px;
                            mouse-cursor: pointer;
                            clicked => { root.toggle_expanded(row.node_index); }
                            Text {
                                text: row.expanded ? "v" : ">";
                                font-size: 10px;
                                color: Palette.foreground;
                                vertical-alignment: center;
                                horizontal-alignment: center;
                            }
                        }
                        if !row.is_dir: Rectangle { width: 20px; }

                        // Tri-state checkbox
                        TouchArea {
                            width: 22px;
                            mouse-cursor: pointer;
                            clicked => { root.toggle_checked(row.node_index); }
                            Rectangle {
                                x: 2px;
                                y: (parent.height - 16px) / 2;
                                width: 16px;
                                height: 16px;
                                border-width: 1px;
                                border-color: row.check_state == 0 ? Palette.border : Palette.accent-background;
                                border-radius: 3px;
                                background: row.check_state == 1 ? Palette.accent-background : row.check_state == 2 ? Palette.accent-background.transparentize(50%) : Palette.background;

                                // Checkmark for checked state
                                if row.check_state == 1: Text {
                                    text: "x";
                                    color: white;
                                    font-size: 12px;
                                    horizontal-alignment: center;
                                    vertical-alignment: center;
                                }
                                // Dash for partial state
                                if row.check_state == 2: Rectangle {
                                    x: 3px;
                                    y: (parent.height - 2px) / 2;
                                    width: parent.width - 6px;
                                    height: 2px;
                                    background: white;
                                }
                            }
                        }

                        // Name
                        Text {
                            text: row.name;
                            vertical-alignment: center;
                            horizontal-stretch: 1;
                            overflow: elide;
                            font-weight: row.is_dir ? 700 : 400;
                        }

                        // Type
                        Text {
                            text: row.type_str;
                            vertical-alignment: center;
                            width: 50px;
                            color: Palette.foreground.transparentize(40%);
                            font-size: 11px;
                        }

                        // Permissions
                        Text {
                            text: row.permissions;
                            vertical-alignment: center;
                            width: 70px;
                            color: Palette.foreground.transparentize(40%);
                            font-size: 11px;
                        }

                        // Size
                        Text {
                            text: row.size_str;
                            vertical-alignment: center;
                            width: 80px;
                            horizontal-alignment: right;
                            color: Palette.foreground.transparentize(40%);
                            font-size: 11px;
                        }
                    }
                }
            }
            }

            HorizontalBox {
                spacing: 8px;
                Text {
                    vertical-alignment: center;
                    text: root.status_text;
                    color: Palette.foreground;
                }
                Rectangle { horizontal-stretch: 1; }
                Button {
                    text: "Cancel";
                    clicked => { root.cancel_clicked(); }
                }
                Button {
                    text: "Restore Selected";
                    clicked => { root.restore_selected_clicked(); }
                }
            }
        }
    }

    export component FindWindow inherits Window {
        in-out property <[string]> repo_names: [];
        in-out property <string> repo_combo_value;
        in-out property <string> name_pattern;
        in-out property <string> status_text: "Enter a name pattern and click Search.";
        in-out property <[[StandardListViewItem]]> result_rows: [];

        callback search_clicked();
        callback close_clicked();

        title: "Find Files";
        preferred-width: 950px;
        preferred-height: 600px;

        VerticalBox {
            padding: 12px;
            spacing: 8px;

            // Search controls bar
            HorizontalBox {
                spacing: 8px;
                Text { text: "Repository:"; vertical-alignment: center; }
                ComboBox {
                    model: root.repo_names;
                    current-value <=> root.repo_combo_value;
                }
                Text { text: "Name pattern:"; vertical-alignment: center; }
                LineEdit {
                    horizontal-stretch: 1;
                    text <=> root.name_pattern;
                    placeholder-text: "e.g. *.rs, config*";
                    accepted => { root.search_clicked(); }
                }
                Button {
                    text: "Search";
                    clicked => { root.search_clicked(); }
                }
            }

            // Results table
            StandardTableView {
                vertical-stretch: 1;
                columns: [
                    { title: "Snapshot", width: 160px },
                    { title: "Path", horizontal-stretch: 1 },
                    { title: "Date", width: 150px },
                    { title: "Size", width: 90px },
                    { title: "Status", width: 90px },
                ];
                rows: root.result_rows;
            }

            // Footer
            HorizontalBox {
                spacing: 8px;
                Text {
                    vertical-alignment: center;
                    text: root.status_text;
                    color: #666666;
                    horizontal-stretch: 1;
                }
                Button {
                    text: "Close";
                    clicked => { root.close_clicked(); }
                }
            }
        }
    }

    component ToolTipArea {
        preferred-height: 100%;
        preferred-width: 100%;

        in property <string> text;
        in property <bool> show-left: false;

        ta := TouchArea {
            @children
        }
        Rectangle {
            states [
                visible when ta.has-hover: {
                    opacity: 0.8;
                    in {
                        animate opacity { duration: 175ms; delay: 700ms; }
                    }
                }
            ]
            x: root.show-left ? ta.mouse-x - self.width - 1rem : ta.mouse-x + 1rem;
            y: ta.mouse-y + 1rem;
            background: Palette.background;
            border-width: 1px;
            border-color: #888888;
            border-radius: 4px;
            opacity: 0;
            width: tt.preferred-width;
            height: tt.preferred-height;
            tt := HorizontalLayout {
                padding: 4px;
                Text { text <=> root.text; font-size: 11px; }
            }
        }
    }

    export component MainWindow inherits Window {
        in-out property <string> config_path;
        in-out property <string> schedule_text;
        in-out property <string> status_text;
        in-out property <[[StandardListViewItem]]> log_rows: [];

        // Operation busy state — disables all action buttons
        in-out property <bool> operation_busy: false;

        // Repo model (custom cards)
        in-out property <bool> repo_loading: true;
        in-out property <[RepoInfo]> repo_model: [];

        // Source model (custom cards)
        in-out property <[SourceInfo]> source_model: [];

        // Snapshot table
        in-out property <[string]> repo_names: [];
        in-out property <string> snapshots_repo_combo_value;
        in-out property <[[StandardListViewItem]]> snapshot_rows: [];

        // Editor tab state
        in-out property <string> editor_text: "";
        in-out property <string> editor_baseline: "";
        in-out property <string> editor_status: "";
        in-out property <bool> editor_dirty: false;
        in-out property <string> editor_font_family: "";

        callback open_config_clicked();
        callback switch_config_clicked();
        callback save_and_apply_clicked();
        callback discard_clicked();
        callback backup_all_clicked();
        callback find_files_clicked();
        callback reload_config_clicked();
        callback backup_repo_clicked(/* index */ int);
        callback backup_source_clicked(/* index */ int);
        callback toggle_source_expanded(/* index */ int);
        callback refresh_snapshots_clicked();
        callback restore_selected_snapshot_clicked(/* row */ int);
        callback delete_selected_snapshot_clicked(/* row */ int);
        callback snapshots_repo_changed(/* value */ string);
        callback snapshot_sort_ascending(/* column */ int);
        callback snapshot_sort_descending(/* column */ int);
        callback cancel_clicked();

        title: "Vykar Backup";
        preferred-width: 1100px;
        preferred-height: 760px;

        VerticalBox {
            padding: 0px;
            spacing: 0px;

            // ── Header ──
            VerticalBox {
                padding-left: 12px;
                padding-right: 12px;
                padding-top: 10px;
                padding-bottom: 6px;
                spacing: 4px;

                HorizontalBox {
                    spacing: 8px;
                    Text { text: "Config:"; vertical-alignment: center; width: 65px; font-weight: 700; }
                    TouchArea {
                        mouse-cursor: pointer;
                        clicked => { root.open_config_clicked(); }
                        Text { text: root.config_path; color: #4a90d9; vertical-alignment: center; }
                    }
                    TouchArea {
                        mouse-cursor: pointer;
                        clicked => { root.switch_config_clicked(); }
                        Text { text: "(Change)"; color: #4a90d9; vertical-alignment: center; font-size: 12px; }
                    }
                    Rectangle { horizontal-stretch: 1; }
                    ToolTipArea {
                        text: "Reload configuration from disk";
                        Button {
                            text: "Reload";
                            enabled: !root.operation_busy && !root.editor_dirty;
                            clicked => { root.reload_config_clicked(); }
                        }
                    }
                    ToolTipArea {
                        text: "Search files across snapshots";
                        Button {
                            text: "Find Files";
                            enabled: !root.operation_busy;
                            clicked => { root.find_files_clicked(); }
                        }
                    }
                    ToolTipArea {
                        text: "Backup, prune, compact, and check all repos";
                        show-left: true;
                        Button {
                            text: root.operation_busy ? "Cancel" : "Full Backup";
                            primary: !root.operation_busy;
                            clicked => {
                                if (root.operation_busy) {
                                    root.cancel_clicked();
                                } else {
                                    root.backup_all_clicked();
                                }
                            }
                        }
                    }
                }
                HorizontalBox {
                    spacing: 8px;
                    Text { text: "Schedule:"; vertical-alignment: center; width: 65px; font-weight: 700; }
                    Text { text: root.schedule_text; vertical-alignment: center; }
                }
            }

            // ── Tabs ──
            HorizontalLayout {
                vertical-stretch: 1;
                padding-left: 8px;
                padding-right: 8px;
            TabWidget {

                Tab {
                    title: "Repositories";
                    VerticalBox {
                        spacing: 8px;
                        padding: 8px;
                        if root.repo_loading: Text {
                            text: "Loading repository data\u{2026}";
                            horizontal-alignment: center;
                            vertical-alignment: center;
                            vertical-stretch: 1;
                            color: #888888;
                        }
                        if !root.repo_loading: ListView {
                            vertical-stretch: 1;
                            for repo[idx] in root.repo_model: GroupBox {
                                HorizontalLayout {
                                    padding: 8px;
                                    spacing: 16px;
                                    VerticalLayout {
                                        horizontal-stretch: 1;
                                        min-width: 350px;
                                        spacing: 4px;
                                        Text { text: repo.name; font-weight: 700; }
                                        Text { text: repo.url; color: #888888; font-size: 11px; }
                                    }
                                    VerticalLayout {
                                        horizontal-stretch: 0;
                                        min-width: 250px;
                                        spacing: 4px;
                                        Text { text: "Snapshots: " + repo.snapshots + "  ·  Size: " + repo.size; }
                                        Text { text: "Latest: " + repo.last_snapshot; font-size: 11px; color: #888888; }
                                    }
                                    VerticalLayout {
                                        alignment: center;
                                        HorizontalLayout {
                                            spacing: 8px;
                                            Button { text: "Backup"; primary: true; enabled: !root.operation_busy; clicked => { root.backup_repo_clicked(idx); } }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                Tab {
                    title: "Sources";
                    VerticalBox {
                        spacing: 8px;
                        padding: 8px;
                        ListView {
                            vertical-stretch: 1;
                            for source[idx] in root.source_model: GroupBox {
                                VerticalLayout {
                                    spacing: 0px;
                                    // Header row — always visible
                                    HorizontalLayout {
                                        padding: 8px;
                                        spacing: 16px;
                                        TouchArea {
                                            horizontal-stretch: 1;
                                            mouse-cursor: pointer;
                                            clicked => { root.toggle_source_expanded(idx); }
                                            HorizontalLayout {
                                                spacing: 12px;
                                                // Chevron
                                                Text {
                                                    text: source.expanded ? "v" : ">";
                                                    vertical-alignment: center;
                                                    font-size: 11px;
                                                    width: 14px;
                                                    color: #888888;
                                                }
                                                VerticalLayout {
                                                    horizontal-stretch: 1;
                                                    spacing: 4px;
                                                    Text { text: source.label; font-weight: 700; }
                                                    if !source.expanded: Text { text: source.paths; color: #888888; font-size: 11px; overflow: elide; }
                                                }
                                                if !source.expanded: VerticalLayout {
                                                    horizontal-stretch: 0;
                                                    min-width: 200px;
                                                    max-width: 350px;
                                                    spacing: 4px;
                                                    Text { text: "Excludes: " + source.excludes; font-size: 11px; overflow: elide; }
                                                    Text { text: "Repos: " + source.target_repos; font-size: 11px; color: #888888; }
                                                }
                                            }
                                        }
                                        VerticalLayout {
                                            alignment: center;
                                            Button { text: "Backup"; primary: true; enabled: !root.operation_busy; clicked => { root.backup_source_clicked(idx); } }
                                        }
                                    }
                                    // Expanded detail section
                                    if source.expanded: VerticalLayout {
                                        padding-left: 42px;
                                        padding-right: 8px;
                                        padding-bottom: 8px;
                                        spacing: 6px;
                                        // Folders
                                        VerticalLayout {
                                            spacing: 2px;
                                            Text { text: "Folders"; font-weight: 700; font-size: 11px; color: #888888; }
                                            Text { text: source.detail_paths; font-size: 11px; wrap: word-wrap; }
                                        }
                                        // Exclusions
                                        if source.detail_excludes != "": VerticalLayout {
                                            spacing: 2px;
                                            Text { text: "Exclusions"; font-weight: 700; font-size: 11px; color: #888888; }
                                            Text { text: source.detail_excludes; font-size: 11px; wrap: word-wrap; }
                                        }
                                        // Exclude-If-Present
                                        if source.detail_exclude_if_present != "": VerticalLayout {
                                            spacing: 2px;
                                            Text { text: "Exclude If Present"; font-weight: 700; font-size: 11px; color: #888888; }
                                            Text { text: source.detail_exclude_if_present; font-size: 11px; wrap: word-wrap; }
                                        }
                                        // Options (flags)
                                        if source.detail_flags != "": VerticalLayout {
                                            spacing: 2px;
                                            Text { text: "Options"; font-weight: 700; font-size: 11px; color: #888888; }
                                            Text { text: source.detail_flags; font-size: 11px; }
                                        }
                                        // Target Repositories
                                        VerticalLayout {
                                            spacing: 2px;
                                            Text { text: "Target Repositories"; font-weight: 700; font-size: 11px; color: #888888; }
                                            Text { text: source.target_repos; font-size: 11px; }
                                        }
                                        // Command Dumps
                                        if source.detail_command_dumps != "": VerticalLayout {
                                            spacing: 2px;
                                            Text { text: "Command Dumps"; font-weight: 700; font-size: 11px; color: #888888; }
                                            Text { text: source.detail_command_dumps; font-size: 11px; wrap: word-wrap; }
                                        }
                                        // Hooks
                                        if source.detail_hooks != "": VerticalLayout {
                                            spacing: 2px;
                                            Text { text: "Hooks"; font-weight: 700; font-size: 11px; color: #888888; }
                                            Text { text: source.detail_hooks; font-size: 11px; wrap: word-wrap; }
                                        }
                                        // Retention
                                        if source.detail_retention != "": VerticalLayout {
                                            spacing: 2px;
                                            Text { text: "Retention"; font-weight: 700; font-size: 11px; color: #888888; }
                                            Text { text: source.detail_retention; font-size: 11px; }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                Tab {
                    title: "Snapshots";
                    VerticalBox {
                        spacing: 8px;
                        padding: 8px;

                        HorizontalBox {
                            spacing: 8px;
                            Text { text: "Repository:"; vertical-alignment: center; }
                            ComboBox {
                                model: root.repo_names;
                                current-value <=> root.snapshots_repo_combo_value;
                                selected(value) => {
                                    root.snapshots_repo_changed(value);
                                }
                            }
                            Button {
                                text: "Refresh";
                                enabled: !root.operation_busy;
                                clicked => { root.refresh_snapshots_clicked(); }
                            }
                        }

                        snapshot-table := StandardTableView {
                            vertical-stretch: 1;
                            columns: [
                                { title: "ID" },
                                { title: "Host" },
                                { title: "Time" },
                                { title: "Source" },
                                { title: "Label" },
                                { title: "Files" },
                                { title: "Size" },
                            ];
                            rows: root.snapshot_rows;
                            sort-ascending(col-idx) => { root.snapshot_sort_ascending(col-idx); }
                            sort-descending(col-idx) => { root.snapshot_sort_descending(col-idx); }
                        }

                        HorizontalBox {
                            spacing: 8px;
                            Button {
                                text: "Restore Selected Snapshot";
                                enabled: !root.operation_busy;
                                clicked => {
                                    root.restore-selected-snapshot-clicked(snapshot-table.current-row);
                                }
                            }
                            Button {
                                text: "Delete Selected Snapshot";
                                enabled: !root.operation_busy;
                                clicked => {
                                    root.delete-selected-snapshot-clicked(snapshot-table.current-row);
                                }
                            }
                        }
                    }
                }

                Tab {
                    title: "Log";
                    VerticalBox {
                        padding: 8px;
                        StandardTableView {
                            vertical-stretch: 1;
                            columns: [
                                { title: "Time", width: 90px },
                                { title: "Event", horizontal-stretch: 1 },
                            ];
                            rows: root.log_rows;
                        }
                    }
                }

                Tab {
                    title: "Edit";
                    VerticalBox {
                        padding: 8px;
                        spacing: 8px;
                        Text {
                            text: "You can edit Vykar's YAML config below. This will be saved to disk and can also be used by the command line client. We plan on adding a prettier interface for editing the configuration in the future.";
                            wrap: word-wrap;
                            color: #888888;
                            font-size: 12px;
                        }
                        TextEdit {
                            vertical-stretch: 1;
                            text <=> root.editor_text;
                            font-size: 13px;
                            font-family: root.editor_font_family;
                            wrap: no-wrap;
                            edited(new_text) => {
                                root.editor_dirty = (new_text != root.editor_baseline);
                                root.editor_status = "";
                            }
                        }
                        if root.editor_status != "": Text {
                            text: root.editor_status;
                            color: #cc0000;
                            wrap: word-wrap;
                        }
                        HorizontalBox {
                            spacing: 8px;
                            Rectangle { horizontal-stretch: 1; }
                            Button {
                                text: "Discard";
                                enabled: root.editor_dirty;
                                clicked => { root.discard_clicked(); }
                            }
                            Button {
                                text: "Save and Apply";
                                primary: true;
                                enabled: root.editor_dirty && !root.operation_busy;
                                clicked => { root.save_and_apply_clicked(); }
                            }
                        }
                    }
                }
            }
            }

            // ── Footer ──
            Rectangle {
                height: 1px;
                background: #d5d5d5;
            }

            HorizontalBox {
                padding-left: 12px;
                padding-right: 12px;
                padding-top: 6px;
                padding-bottom: 6px;
                spacing: 8px;

                Text { text: "Status:"; vertical-alignment: center; }
                Text { text: root.status_text; vertical-alignment: center; horizontal-stretch: 1; }
            }
        }
    }
}

// ── Commands and Events ──

#[derive(Debug)]
enum AppCommand {
    RunBackupAll {
        scheduled: bool,
    },
    RunBackupRepo {
        repo_name: String,
    },
    RunBackupSource {
        source_label: String,
    },
    FetchAllRepoInfo,
    RefreshSnapshots {
        repo_selector: String,
    },
    FetchSnapshotContents {
        repo_name: String,
        snapshot_name: String,
    },
    RestoreSelected {
        repo_name: String,
        snapshot: String,
        dest: String,
        paths: Vec<String>,
    },
    DeleteSnapshot {
        repo_name: String,
        snapshot_name: String,
    },
    FindFiles {
        repo_name: String,
        name_pattern: String,
    },
    OpenConfigFile,
    ReloadConfig,
    SwitchConfig,
    SaveAndApplyConfig {
        yaml_text: String,
    },
    ShowWindow,
    Quit,
}

#[derive(Debug, Clone)]
struct RepoInfoData {
    name: String,
    url: String,
    snapshots: String,
    last_snapshot: String,
    size: String,
}

#[derive(Debug, Clone)]
struct SnapshotRowData {
    id: String,
    hostname: String,
    time_str: String,
    source: String,
    label: String,
    files: String,
    size: String,
    nfiles: u64,
    size_bytes: u64,
    time_epoch: i64,
    repo_name: String,
}

#[derive(Debug, Clone)]
struct SourceInfoData {
    label: String,
    paths: String,
    excludes: String,
    target_repos: String,
    detail_paths: String,
    detail_excludes: String,
    detail_exclude_if_present: String,
    detail_flags: String,
    detail_hooks: String,
    detail_retention: String,
    detail_command_dumps: String,
}

#[derive(Debug, Clone)]
struct FindResultRow {
    path: String,
    snapshot: String,
    date: String,
    size: String,
    status: String,
}

#[derive(Debug, Clone)]
enum UiEvent {
    Status(String),
    LogEntry {
        timestamp: String,
        message: String,
    },
    ConfigInfo {
        path: String,
        schedule: String,
    },
    RepoNames(Vec<String>),
    RepoModelData {
        items: Vec<RepoInfoData>,
        labels: Vec<String>,
    },
    SourceModelData {
        items: Vec<SourceInfoData>,
        labels: Vec<String>,
    },
    SnapshotTableData {
        data: Vec<SnapshotRowData>,
    },
    SnapshotContentsData {
        items: Vec<Item>,
    },
    RestoreStatus(String),
    FindResultsData {
        rows: Vec<FindResultRow>,
    },
    ConfigText(String),
    ConfigSaveError(String),
    OperationStarted,
    OperationFinished,
    Quit,
    ShowWindow,
    TriggerSnapshotRefresh,
}

// ── Scheduler ──

#[derive(Debug)]
struct SchedulerState {
    enabled: bool,
    paused: bool,
    every: Duration,
    cron: Option<String>,
    jitter_seconds: u64,
    next_run: Option<Instant>,
}

impl Default for SchedulerState {
    fn default() -> Self {
        Self {
            enabled: false,
            paused: false,
            every: Duration::from_secs(24 * 60 * 60),
            cron: None,
            jitter_seconds: 0,
            next_run: None,
        }
    }
}

fn schedule_description(schedule: &ScheduleConfig, paused: bool) -> String {
    let timing = if let Some(ref cron) = schedule.cron {
        format!("cron={cron}")
    } else {
        format!("every={}", schedule.every.as_deref().unwrap_or("24h"))
    };
    format!(
        "enabled={}, {timing}, on_startup={}, jitter_seconds={}, paused={}",
        schedule.enabled, schedule.on_startup, schedule.jitter_seconds, paused,
    )
}

// ── Tray icon ──

fn build_tray_icon() -> Result<
    (
        tray_icon::TrayIcon,
        MenuId,
        MenuId,
        MenuId,
        Submenu,
        MenuItem,
    ),
    String,
> {
    let menu = Menu::new();

    let open_item = MenuItem::new(format!("Open {APP_TITLE}"), true, None);
    let run_now_item = MenuItem::new("Full Backup", true, None);
    let source_submenu = Submenu::new("Backup Source", true);
    let cancel_item = MenuItem::new("Cancel Backup", false, None);
    let quit_item = MenuItem::new("Quit", true, None);

    menu.append(&open_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&run_now_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&source_submenu)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&cancel_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&quit_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;

    let logo_bytes = include_bytes!("../../../docs/src/images/logo_simple.png");
    let logo_img = image::load_from_memory(logo_bytes)
        .map_err(|e| format!("failed to decode logo: {e}"))?
        .resize(44, 44, image::imageops::FilterType::Lanczos3)
        .into_rgba8();
    let (w, h) = logo_img.dimensions();
    let icon =
        Icon::from_rgba(logo_img.into_raw(), w, h).map_err(|e| format!("tray icon error: {e}"))?;

    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip(APP_TITLE)
        .with_icon(icon)
        .with_icon_as_template(true)
        .build()
        .map_err(|e| format!("tray icon build failed: {e}"))?;

    Ok((
        tray,
        open_item.id().clone(),
        run_now_item.id().clone(),
        quit_item.id().clone(),
        source_submenu,
        cancel_item,
    ))
}

// ── Scheduler thread ──

fn spawn_scheduler(
    app_tx: Sender<AppCommand>,
    ui_tx: Sender<UiEvent>,
    scheduler: Arc<Mutex<SchedulerState>>,
    backup_running: Arc<AtomicBool>,
) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(1));

        let mut should_run = false;

        {
            let mut state = match scheduler.lock() {
                Ok(s) => s,
                Err(_) => break,
            };

            if !state.enabled || state.paused {
                continue;
            }

            if state.next_run.is_none() {
                match compute_scheduler_delay(&state) {
                    Ok(delay) => state.next_run = Some(Instant::now() + delay),
                    Err(e) => {
                        state.paused = true;
                        state.next_run = None;
                        let _ = ui_tx.send(UiEvent::LogEntry {
                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                            message: format!(
                                "Scheduler error: {e}. Scheduling paused — reload config to resume."
                            ),
                        });
                        continue;
                    }
                }
            }

            if let Some(next) = state.next_run {
                if Instant::now() >= next && !backup_running.load(Ordering::SeqCst) {
                    should_run = true;
                    match compute_scheduler_delay(&state) {
                        Ok(delay) => state.next_run = Some(Instant::now() + delay),
                        Err(e) => {
                            state.paused = true;
                            state.next_run = None;
                            let _ = ui_tx.send(UiEvent::LogEntry {
                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                message: format!(
                                    "Scheduler error: {e}. Scheduling paused — reload config to resume."
                                ),
                            });
                        }
                    }
                }
            }
        }

        if should_run
            && app_tx
                .send(AppCommand::RunBackupAll { scheduled: true })
                .is_err()
        {
            break;
        }
    });
}

fn compute_scheduler_delay(
    state: &SchedulerState,
) -> std::result::Result<Duration, vykar_types::error::VykarError> {
    if let Some(ref cron_expr) = state.cron {
        let tmp = ScheduleConfig {
            enabled: true,
            every: None,
            cron: Some(cron_expr.clone()),
            on_startup: false,
            jitter_seconds: state.jitter_seconds,
            passphrase_prompt_timeout_seconds: 300,
        };
        vykar_core::app::scheduler::next_run_delay(&tmp)
    } else {
        Ok(state.every + vykar_core::app::scheduler::random_jitter(state.jitter_seconds))
    }
}

// ── Helpers ──

fn format_repo_name(repo: &ResolvedRepo) -> String {
    repo.label
        .clone()
        .unwrap_or_else(|| repo.config.repository.url.clone())
}

fn to_table_model(rows: Vec<Vec<String>>) -> ModelRc<ModelRc<StandardListViewItem>> {
    let outer: Vec<ModelRc<StandardListViewItem>> = rows
        .into_iter()
        .map(|row| {
            let items: Vec<StandardListViewItem> = row
                .into_iter()
                .map(|cell| StandardListViewItem::from(SharedString::from(cell)))
                .collect();
            ModelRc::new(VecModel::from(items))
        })
        .collect();
    ModelRc::new(VecModel::from(outer))
}

fn sort_snapshot_table(
    sd: &Arc<Mutex<Vec<SnapshotRowData>>>,
    si: &Arc<Mutex<Vec<String>>>,
    sr: &Arc<Mutex<Vec<String>>>,
    ui_weak: &slint::Weak<MainWindow>,
    col_idx: i32,
    ascending: bool,
) {
    let Some(ui) = ui_weak.upgrade() else {
        return;
    };
    let Ok(mut data) = sd.lock() else {
        return;
    };

    // Columns: 0=ID, 1=Host, 2=Time, 3=Source, 4=Label, 5=Files, 6=Size
    match col_idx {
        0 => data.sort_by(|a, b| a.id.cmp(&b.id)),
        1 => data.sort_by(|a, b| a.hostname.cmp(&b.hostname)),
        2 => data.sort_by(|a, b| a.time_epoch.cmp(&b.time_epoch)),
        3 => data.sort_by(|a, b| a.source.cmp(&b.source)),
        4 => data.sort_by(|a, b| a.label.cmp(&b.label)),
        5 => data.sort_by(|a, b| a.nfiles.cmp(&b.nfiles)),
        6 => data.sort_by(|a, b| a.size_bytes.cmp(&b.size_bytes)),
        _ => return,
    }
    if !ascending {
        data.reverse();
    }

    if let Ok(mut ids) = si.lock() {
        *ids = data.iter().map(|d| d.id.clone()).collect();
    }
    if let Ok(mut rnames) = sr.lock() {
        *rnames = data.iter().map(|d| d.repo_name.clone()).collect();
    }

    let rows: Vec<Vec<String>> = data
        .iter()
        .map(|d| {
            vec![
                d.id.clone(),
                d.hostname.clone(),
                d.time_str.clone(),
                d.source.clone(),
                d.label.clone(),
                d.files.clone(),
                d.size.clone(),
            ]
        })
        .collect();
    ui.set_snapshot_rows(to_table_model(rows));
}

fn to_string_model(items: Vec<String>) -> ModelRc<SharedString> {
    let shared: Vec<SharedString> = items.into_iter().map(SharedString::from).collect();
    ModelRc::new(VecModel::from(shared))
}

fn collect_repo_names(repos: &[ResolvedRepo]) -> Vec<String> {
    repos.iter().map(format_repo_name).collect()
}

fn build_source_model_data(repos: &[ResolvedRepo]) -> (Vec<SourceInfoData>, Vec<String>) {
    let mut seen = std::collections::HashSet::new();
    let mut items = Vec::new();
    let mut labels = Vec::new();

    for repo in repos {
        for source in &repo.sources {
            if !seen.insert(source.label.clone()) {
                continue;
            }
            let target = if source.repos.is_empty() {
                "(all)".to_string()
            } else {
                source.repos.join(", ")
            };
            let mut flags = Vec::new();
            if source.one_file_system {
                flags.push("one_file_system");
            }
            if source.git_ignore {
                flags.push("git_ignore");
            }
            if source.xattrs_enabled {
                flags.push("xattrs");
            }

            let mut hooks_lines = Vec::new();
            for (phase, cmds) in [
                ("before", &source.hooks.before),
                ("after", &source.hooks.after),
                ("failed", &source.hooks.failed),
                ("finally", &source.hooks.finally),
            ] {
                if !cmds.is_empty() {
                    hooks_lines.push(format!("{}: {}", phase, cmds.join("; ")));
                }
            }

            let mut retention_parts = Vec::new();
            if let Some(ref ret) = source.retention {
                if let Some(ref v) = ret.keep_within {
                    retention_parts.push(format!("keep_within: {v}"));
                }
                if let Some(v) = ret.keep_last {
                    retention_parts.push(format!("keep_last: {v}"));
                }
                if let Some(v) = ret.keep_hourly {
                    retention_parts.push(format!("keep_hourly: {v}"));
                }
                if let Some(v) = ret.keep_daily {
                    retention_parts.push(format!("keep_daily: {v}"));
                }
                if let Some(v) = ret.keep_weekly {
                    retention_parts.push(format!("keep_weekly: {v}"));
                }
                if let Some(v) = ret.keep_monthly {
                    retention_parts.push(format!("keep_monthly: {v}"));
                }
                if let Some(v) = ret.keep_yearly {
                    retention_parts.push(format!("keep_yearly: {v}"));
                }
            }

            items.push(SourceInfoData {
                label: source.label.clone(),
                paths: source.paths.join(", "),
                excludes: source.exclude.join(", "),
                target_repos: target,
                detail_paths: source.paths.join("\n"),
                detail_excludes: source.exclude.join("\n"),
                detail_exclude_if_present: source.exclude_if_present.join("\n"),
                detail_flags: flags.join(", "),
                detail_hooks: hooks_lines.join("\n"),
                detail_retention: retention_parts.join(", "),
                detail_command_dumps: source
                    .command_dumps
                    .iter()
                    .map(|d| format!("{}: {}", d.name, d.command))
                    .collect::<Vec<_>>()
                    .join("\n"),
            });
            labels.push(source.label.clone());
        }
    }

    (items, labels)
}

fn send_structured_data(ui_tx: &Sender<UiEvent>, repos: &[ResolvedRepo]) {
    let _ = ui_tx.send(UiEvent::RepoNames(collect_repo_names(repos)));

    let (items, labels) = build_source_model_data(repos);
    let _ = ui_tx.send(UiEvent::SourceModelData { items, labels });
}

fn resolve_passphrase_for_repo(
    repo: &ResolvedRepo,
) -> Result<Option<zeroize::Zeroizing<String>>, VykarError> {
    let repo_name = format_repo_name(repo);
    let pass = passphrase::resolve_passphrase(&repo.config, repo.label.as_deref(), |prompt| {
        let title = format!("{APP_TITLE} — Passphrase ({repo_name})");
        let message = format!(
            "Enter passphrase for {}\nRepository: {}",
            prompt
                .repository_label
                .as_deref()
                .unwrap_or(prompt.repository_url.as_str()),
            prompt.repository_url,
        );
        let value = tinyfiledialogs::password_box(&title, &message);
        Ok(value.filter(|v| !v.is_empty()).map(zeroize::Zeroizing::new))
    })?;
    Ok(pass)
}

fn get_or_resolve_passphrase(
    repo: &ResolvedRepo,
    cache: &mut HashMap<String, zeroize::Zeroizing<String>>,
) -> Result<Option<zeroize::Zeroizing<String>>, VykarError> {
    let key = &repo.config.repository.url;
    if let Some(existing) = cache.get(key) {
        return Ok(Some(existing.clone()));
    }
    let pass = resolve_passphrase_for_repo(repo)?;
    if let Some(ref p) = pass {
        cache.insert(key.clone(), p.clone());
    }
    Ok(pass)
}

fn select_repos<'a>(
    repos: &'a [ResolvedRepo],
    selector: &str,
) -> Result<Vec<&'a ResolvedRepo>, VykarError> {
    let selector = selector.trim();
    if selector.is_empty() {
        return Ok(repos.iter().collect());
    }

    let repo = config::select_repo(repos, selector)
        .ok_or_else(|| VykarError::Config(format!("no repository matching '{selector}'")))?;
    Ok(vec![repo])
}

fn find_repo_for_snapshot<'a>(
    repos: &'a [ResolvedRepo],
    selector: &str,
    snapshot: &str,
    passphrases: &mut HashMap<String, zeroize::Zeroizing<String>>,
) -> Result<(&'a ResolvedRepo, Option<zeroize::Zeroizing<String>>), VykarError> {
    for repo in select_repos(repos, selector)? {
        let key = repo.config.repository.url.clone();
        let pass = if let Some(cached) = passphrases.get(&key) {
            Some(cached.clone())
        } else {
            let p = resolve_passphrase_for_repo(repo)?;
            if let Some(ref v) = p {
                passphrases.insert(key.clone(), v.clone());
            }
            p
        };

        match operations::list_snapshot_items(
            &repo.config,
            pass.as_deref().map(|s| s.as_str()),
            snapshot,
        ) {
            Ok(_) => return Ok((repo, pass)),
            Err(VykarError::SnapshotNotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }

    Err(VykarError::SnapshotNotFound(snapshot.to_string()))
}

fn send_log(ui_tx: &Sender<UiEvent>, message: impl Into<String>) {
    let timestamp = Local::now().format("%H:%M:%S").to_string();
    let _ = ui_tx.send(UiEvent::LogEntry {
        timestamp,
        message: message.into(),
    });
}

fn log_backup_report(
    ui_tx: &Sender<UiEvent>,
    repo_name: &str,
    report: &operations::BackupRunReport,
) {
    if report.created.is_empty() {
        send_log(ui_tx, format!("[{repo_name}] no snapshots created"));
        return;
    }
    for created in &report.created {
        send_log(
            ui_tx,
            format!(
                "[{repo_name}] snapshot {} source={} files={} original={} compressed={} deduplicated={}",
                created.snapshot_name,
                created.source_label,
                created.stats.nfiles,
                format_bytes(created.stats.original_size),
                format_bytes(created.stats.compressed_size),
                format_bytes(created.stats.deduplicated_size),
            ),
        );
    }
}

/// Load and fully validate a config file: parse YAML, check non-empty, validate schedule.
/// Returns the parsed repos or a human-readable error string.
fn validate_config(config_path: &std::path::Path) -> Result<Vec<config::ResolvedRepo>, String> {
    let repos = app::load_runtime_config_from_path(config_path).map_err(|e| format!("{e}"))?;
    if repos.is_empty() {
        return Err("Config is empty (no repositories defined).".into());
    }
    // Validate schedule is usable (parses interval or cron)
    vykar_core::app::scheduler::next_run_delay(&repos[0].config.schedule)
        .map_err(|e| format!("Invalid schedule: {e}"))?;
    Ok(repos)
}

/// Apply a (possibly new) config file: load, validate, update runtime state, and notify the UI.
/// When `update_source` is true the runtime source path is switched to `config_path`.
/// Returns `true` on success, `false` on failure.
#[allow(clippy::too_many_arguments)]
fn apply_config(
    config_path: PathBuf,
    update_source: bool,
    runtime: &mut app::RuntimeConfig,
    config_display_path: &mut PathBuf,
    passphrases: &mut HashMap<String, zeroize::Zeroizing<String>>,
    scheduler: &Arc<Mutex<SchedulerState>>,
    schedule_paused: bool,
    scheduler_lock_held: bool,
    ui_tx: &Sender<UiEvent>,
    app_tx: &Sender<AppCommand>,
) -> bool {
    let repos = match validate_config(&config_path) {
        Ok(v) => v,
        Err(msg) => {
            send_log(ui_tx, format!("{msg} Keeping previous config."));
            return false;
        }
    };
    let schedule = repos[0].config.schedule.clone();

    if update_source {
        use vykar_core::config::ConfigSource;
        runtime.source = ConfigSource::SearchOrder {
            path: config_path.clone(),
            level: "user",
        };
    }
    runtime.repos = repos;
    passphrases.clear();

    if let Ok(mut state) = scheduler.lock() {
        state.enabled = schedule.enabled && scheduler_lock_held;
        state.paused = schedule_paused || !scheduler_lock_held;
        state.every = schedule
            .every_duration()
            .unwrap_or(Duration::from_secs(24 * 60 * 60));
        state.cron = schedule.cron.clone();
        state.jitter_seconds = schedule.jitter_seconds;
        // Compute initial next_run via the scheduler delay (includes jitter)
        let delay = vykar_core::app::scheduler::next_run_delay(&schedule)
            .unwrap_or(Duration::from_secs(24 * 60 * 60));
        state.next_run = Some(Instant::now() + delay);
    }

    let canonical = dunce::canonicalize(&config_path).unwrap_or_else(|_| config_path.clone());
    *config_display_path = canonical.clone();

    let schedule_desc = if scheduler_lock_held {
        schedule_description(&schedule, schedule_paused)
    } else {
        "disabled (external scheduler)".to_string()
    };
    let _ = ui_tx.send(UiEvent::ConfigInfo {
        path: canonical.display().to_string(),
        schedule: schedule_desc,
    });
    send_structured_data(ui_tx, &runtime.repos);
    let _ = app_tx.send(AppCommand::FetchAllRepoInfo);
    send_log(ui_tx, "Configuration reloaded.");

    // Send raw config text to populate the editor tab
    match std::fs::read_to_string(&canonical) {
        Ok(text) => {
            let _ = ui_tx.send(UiEvent::ConfigText(text));
        }
        Err(e) => {
            send_log(ui_tx, format!("Could not read config file for editor: {e}"));
        }
    }

    true
}

// ── Worker thread ──

#[allow(clippy::too_many_arguments)]
fn run_worker(
    app_tx: Sender<AppCommand>,
    cmd_rx: Receiver<AppCommand>,
    ui_tx: Sender<UiEvent>,
    scheduler: Arc<Mutex<SchedulerState>>,
    backup_running: Arc<AtomicBool>,
    cancel_requested: Arc<AtomicBool>,
    mut runtime: app::RuntimeConfig,
    scheduler_lock_held: bool,
) {
    let mut passphrases: HashMap<String, zeroize::Zeroizing<String>> = HashMap::new();

    let mut config_display_path = dunce::canonicalize(runtime.source.path())
        .unwrap_or_else(|_| runtime.source.path().to_path_buf());

    let schedule = runtime.schedule();
    let schedule_paused = !scheduler_lock_held;
    let schedule_delay = vykar_core::app::scheduler::next_run_delay(&schedule)
        .unwrap_or_else(|_| Duration::from_secs(24 * 60 * 60));

    if let Ok(mut state) = scheduler.lock() {
        state.enabled = schedule.enabled && scheduler_lock_held;
        state.paused = !scheduler_lock_held;
        state.every = schedule
            .every_duration()
            .unwrap_or(Duration::from_secs(24 * 60 * 60));
        state.cron = schedule.cron.clone();
        state.jitter_seconds = schedule.jitter_seconds;
        state.next_run = Some(Instant::now() + schedule_delay);
    }

    let schedule_desc = if scheduler_lock_held {
        schedule_description(&schedule, false)
    } else {
        "disabled (external scheduler)".to_string()
    };
    let _ = ui_tx.send(UiEvent::ConfigInfo {
        path: config_display_path.display().to_string(),
        schedule: schedule_desc,
    });

    send_structured_data(&ui_tx, &runtime.repos);

    // Populate the editor tab with the current config file contents
    if let Ok(text) = std::fs::read_to_string(&config_display_path) {
        let _ = ui_tx.send(UiEvent::ConfigText(text));
    }

    // Auto-fetch repo info at startup
    let _ = app_tx.send(AppCommand::FetchAllRepoInfo);

    if scheduler_lock_held && schedule.enabled && schedule.on_startup {
        send_log(
            &ui_tx,
            "Scheduled on-startup backup requested by configuration.",
        );
        let _ = app_tx.send(AppCommand::RunBackupAll { scheduled: true });
    }

    while let Ok(cmd) = cmd_rx.recv() {
        match cmd {
            AppCommand::RunBackupAll { scheduled } => {
                cancel_requested.store(false, Ordering::SeqCst);
                backup_running.store(true, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted);
                let _ = ui_tx.send(UiEvent::Status(if scheduled {
                    "Running scheduled backup cycle...".to_string()
                } else {
                    "Running backup cycle...".to_string()
                }));

                let mut any_snapshots_created = false;
                let total = runtime.repos.len();
                for (i, repo) in runtime.repos.iter().enumerate() {
                    if cancel_requested.load(Ordering::SeqCst) {
                        send_log(&ui_tx, "Backup cancelled by user.");
                        break;
                    }

                    let repo_name = format_repo_name(repo);
                    let _ = ui_tx.send(UiEvent::Status(format!(
                        "[{}] ({}/{total})...",
                        repo_name,
                        i + 1
                    )));

                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(pass) => pass,
                        Err(e) => {
                            send_log(
                                &ui_tx,
                                format!("[{repo_name}] failed to resolve passphrase: {e}"),
                            );
                            continue;
                        }
                    };

                    if repo.config.encryption.mode != vykar_core::config::EncryptionModeConfig::None
                        && passphrase.is_none()
                    {
                        send_log(
                            &ui_tx,
                            format!(
                                "[{repo_name}] passphrase prompt canceled; skipping this repository"
                            ),
                        );
                        continue;
                    }

                    let mut tracker = BackupStatusTracker::new(repo_name.clone());
                    let ui_tx_progress = ui_tx.clone();
                    let rn = repo_name.clone();
                    let result = operations::run_full_cycle_for_repo(
                        &repo.config,
                        &repo.sources,
                        passphrase.as_deref().map(|s| s.as_str()),
                        Some(&cancel_requested),
                        &mut |event| match &event {
                            operations::CycleEvent::StepStarted(step) => {
                                let _ = ui_tx_progress.send(UiEvent::Status(format!(
                                    "[{rn}] {}...",
                                    step.command_name()
                                )));
                            }
                            operations::CycleEvent::Backup(evt) => {
                                if let Some(status) = tracker.format(evt) {
                                    let _ = ui_tx_progress.send(UiEvent::Status(status));
                                }
                            }
                            operations::CycleEvent::Check(evt) => {
                                let _ = ui_tx_progress
                                    .send(UiEvent::Status(format_check_status(&rn, evt)));
                            }
                            _ => {}
                        },
                        None,
                        None,
                    );

                    if let Some(ref report) = result.backup_report {
                        if !report.created.is_empty() {
                            any_snapshots_created = true;
                        }
                        log_backup_report(&ui_tx, &repo_name, report);
                    }
                    for (step, outcome) in &result.steps {
                        let msg = progress::format_step_outcome(&repo_name, *step, outcome);
                        if !msg.is_empty() {
                            send_log(&ui_tx, msg);
                        }
                    }
                }

                if any_snapshots_created {
                    let _ = ui_tx.send(UiEvent::TriggerSnapshotRefresh);
                }

                backup_running.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationFinished);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RunBackupRepo { repo_name } => {
                let repo_name_sel = repo_name.trim().to_string();
                if repo_name_sel.is_empty() {
                    send_log(&ui_tx, "Select a repository first.");
                    continue;
                }

                let repo = match config::select_repo(&runtime.repos, &repo_name_sel) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_name_sel}'."));
                        continue;
                    }
                };

                cancel_requested.store(false, Ordering::SeqCst);
                backup_running.store(true, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted);
                let rn = format_repo_name(repo);
                let _ = ui_tx.send(UiEvent::Status(format!("Running backup for [{rn}]...")));

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{rn}] passphrase error: {e}"));
                        backup_running.store(false, Ordering::SeqCst);
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                if repo.config.encryption.mode != vykar_core::config::EncryptionModeConfig::None
                    && passphrase.is_none()
                {
                    send_log(
                        &ui_tx,
                        format!("[{rn}] passphrase prompt canceled; skipping."),
                    );
                    backup_running.store(false, Ordering::SeqCst);
                    let _ = ui_tx.send(UiEvent::OperationFinished);
                    let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                    continue;
                }

                let mut tracker = BackupStatusTracker::new(rn.clone());
                let ui_tx_progress = ui_tx.clone();
                match operations::run_backup_for_repo_with_progress(
                    &repo.config,
                    &repo.sources,
                    passphrase.as_deref().map(|s| s.as_str()),
                    &mut |event| {
                        if let Some(status) = tracker.format(&event) {
                            let _ = ui_tx_progress.send(UiEvent::Status(status));
                        }
                    },
                    Some(&cancel_requested),
                ) {
                    Ok(report) => {
                        if !report.created.is_empty() {
                            let _ = app_tx.send(AppCommand::RefreshSnapshots {
                                repo_selector: rn.clone(),
                            });
                        }
                        log_backup_report(&ui_tx, &rn, &report);
                    }
                    Err(e) => send_log(&ui_tx, format!("[{rn}] backup failed: {e}")),
                }

                backup_running.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationFinished);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RunBackupSource { source_label } => {
                let source_label = source_label.trim().to_string();
                if source_label.is_empty() {
                    send_log(&ui_tx, "Select a source first.");
                    continue;
                }

                cancel_requested.store(false, Ordering::SeqCst);
                backup_running.store(true, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted);
                let _ = ui_tx.send(UiEvent::Status(format!(
                    "Running backup for source '{source_label}'..."
                )));

                let mut any_backed_up = false;
                let total = runtime.repos.len();
                for (i, repo) in runtime.repos.iter().enumerate() {
                    if cancel_requested.load(Ordering::SeqCst) {
                        send_log(&ui_tx, "Backup cancelled by user.");
                        break;
                    }

                    let matching_sources: Vec<config::SourceEntry> = repo
                        .sources
                        .iter()
                        .filter(|s| s.label == source_label)
                        .cloned()
                        .collect();

                    if matching_sources.is_empty() {
                        continue;
                    }

                    let repo_name = format_repo_name(repo);
                    let _ = ui_tx.send(UiEvent::Status(format!(
                        "Backing up [{}] ({}/{total})...",
                        repo_name,
                        i + 1
                    )));

                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(p) => p,
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                            continue;
                        }
                    };

                    if repo.config.encryption.mode != vykar_core::config::EncryptionModeConfig::None
                        && passphrase.is_none()
                    {
                        send_log(
                            &ui_tx,
                            format!("[{repo_name}] passphrase prompt canceled; skipping."),
                        );
                        continue;
                    }

                    let mut tracker = BackupStatusTracker::new(repo_name.clone());
                    let ui_tx_progress = ui_tx.clone();
                    match operations::run_backup_for_repo_with_progress(
                        &repo.config,
                        &matching_sources,
                        passphrase.as_deref().map(|s| s.as_str()),
                        &mut |event| {
                            if let Some(status) = tracker.format(&event) {
                                let _ = ui_tx_progress.send(UiEvent::Status(status));
                            }
                        },
                        Some(&cancel_requested),
                    ) {
                        Ok(report) => {
                            if !report.created.is_empty() {
                                any_backed_up = true;
                            }
                            log_backup_report(&ui_tx, &repo_name, &report);
                        }
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] backup failed: {e}"));
                        }
                    }
                }

                if !any_backed_up {
                    send_log(
                        &ui_tx,
                        format!("No repositories found with source '{source_label}'."),
                    );
                } else {
                    let _ = ui_tx.send(UiEvent::TriggerSnapshotRefresh);
                }

                backup_running.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationFinished);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::FetchAllRepoInfo => {
                let _ = ui_tx.send(UiEvent::Status("Fetching repository info...".to_string()));

                let mut items = Vec::new();
                let mut labels = Vec::new();

                let total = runtime.repos.len();
                for (i, repo) in runtime.repos.iter().enumerate() {
                    let repo_name = format_repo_name(repo);
                    let _ = ui_tx.send(UiEvent::Status(format!(
                        "Loading repo info: [{}] ({}/{total})...",
                        repo_name,
                        i + 1
                    )));
                    let url = repo.config.repository.url.clone();
                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(p) => p,
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                            continue;
                        }
                    };

                    match vykar_core::commands::info::run(
                        &repo.config,
                        passphrase.as_deref().map(|s| s.as_str()),
                    ) {
                        Ok(stats) => {
                            let last_snapshot = stats
                                .last_snapshot_time
                                .map(|t| {
                                    let local: DateTime<Local> = t.with_timezone(&Local);
                                    local.format("%Y-%m-%d %H:%M:%S").to_string()
                                })
                                .unwrap_or_else(|| "N/A".to_string());

                            items.push(RepoInfoData {
                                name: repo_name.clone(),
                                url,
                                snapshots: stats.snapshot_count.to_string(),
                                last_snapshot,
                                size: format_bytes(stats.deduplicated_size),
                            });
                            labels.push(repo_name);
                        }
                        Err(e) => {
                            if matches!(e, VykarError::RepoNotFound(_)) {
                                let confirmed = tinyfiledialogs::message_box_yes_no(
                                    &format!("{APP_TITLE} — Repository Not Initialized"),
                                    &format!(
                                        "Repository {repo_name} at {url} is not initialized.\n\
                                         Would you like to initialize it now?",
                                    ),
                                    tinyfiledialogs::MessageBoxIcon::Question,
                                    tinyfiledialogs::YesNo::Yes,
                                );
                                if confirmed == tinyfiledialogs::YesNo::Yes {
                                    // Resolve passphrase for init following the canonical rule:
                                    // 1. encryption: none → None
                                    // 2. Configured source (passphrase field / passcommand)
                                    //    → reuse already-resolved value (no re-execution)
                                    // 3. Interactive GUI prompt with enter + confirm
                                    //
                                    // We only reuse the outer `passphrase` when it provably
                                    // came from a configured source. If it came from a single
                                    // interactive password_box (no confirmation), we must NOT
                                    // reuse it — init needs enter+confirm to avoid typos.
                                    // Note: VYKAR_PASSPHRASE env var is not checked here
                                    // because take_env_passphrase() removes it on first read,
                                    // making the probe unreliable in a GUI context.
                                    let has_configured_source =
                                        repo.config.encryption.passphrase.is_some()
                                            || repo.config.encryption.passcommand.is_some();
                                    let init_pass: Option<zeroize::Zeroizing<String>> =
                                        if repo.config.encryption.mode
                                            == vykar_core::config::EncryptionModeConfig::None
                                        {
                                            None
                                        } else if has_configured_source && passphrase.is_some() {
                                            passphrase.clone()
                                        } else {
                                            let title = format!(
                                                "{APP_TITLE} — New Passphrase ({repo_name})"
                                            );
                                            let p1 = tinyfiledialogs::password_box(
                                                &title,
                                                "Enter new passphrase:",
                                            );
                                            match p1.filter(|v| !v.is_empty()) {
                                                None => {
                                                    send_log(
                                                        &ui_tx,
                                                        format!(
                                                            "[{repo_name}] Init cancelled \
                                                             (no passphrase)."
                                                        ),
                                                    );
                                                    continue;
                                                }
                                                Some(p1_val) => {
                                                    let p2 = tinyfiledialogs::password_box(
                                                        &format!(
                                                            "{APP_TITLE} — Confirm Passphrase \
                                                             ({repo_name})"
                                                        ),
                                                        "Confirm passphrase:",
                                                    );
                                                    match p2 {
                                                        Some(ref p2_val) if p2_val == &p1_val => {
                                                            Some(zeroize::Zeroizing::new(p1_val))
                                                        }
                                                        _ => {
                                                            send_log(
                                                                &ui_tx,
                                                                format!(
                                                                    "[{repo_name}] Passphrases \
                                                                     do not match."
                                                                ),
                                                            );
                                                            continue;
                                                        }
                                                    }
                                                }
                                            }
                                        };

                                    let retry_pass = init_pass.clone();
                                    match init::run(
                                        &repo.config,
                                        init_pass.as_deref().map(|s| s.as_str()),
                                    ) {
                                        Ok(_) => {
                                            send_log(
                                                &ui_tx,
                                                format!("[{repo_name}] Repository initialized."),
                                            );
                                            if let Some(p) = init_pass {
                                                passphrases
                                                    .insert(repo.config.repository.url.clone(), p);
                                            }
                                        }
                                        Err(VykarError::RepoAlreadyExists(_)) => {
                                            send_log(
                                                &ui_tx,
                                                format!(
                                                    "[{repo_name}] Repository was initialized \
                                                     concurrently."
                                                ),
                                            );
                                        }
                                        Err(init_err) => {
                                            send_log(
                                                &ui_tx,
                                                format!("[{repo_name}] init failed: {init_err}"),
                                            );
                                            continue;
                                        }
                                    }

                                    // Retry info with the init passphrase to populate the repo card
                                    if let Ok(stats) = vykar_core::commands::info::run(
                                        &repo.config,
                                        retry_pass.as_deref().map(|s| s.as_str()),
                                    ) {
                                        let last_snapshot = stats
                                            .last_snapshot_time
                                            .map(|t| {
                                                let local: DateTime<Local> =
                                                    t.with_timezone(&Local);
                                                local.format("%Y-%m-%d %H:%M:%S").to_string()
                                            })
                                            .unwrap_or_else(|| "N/A".to_string());
                                        items.push(RepoInfoData {
                                            name: repo_name.clone(),
                                            url: url.clone(),
                                            snapshots: stats.snapshot_count.to_string(),
                                            last_snapshot,
                                            size: format_bytes(stats.deduplicated_size),
                                        });
                                        labels.push(repo_name);
                                    }
                                } else {
                                    send_log(
                                        &ui_tx,
                                        format!("[{repo_name}] Repository initialization skipped."),
                                    );
                                }
                            } else {
                                send_log(&ui_tx, format!("[{repo_name}] info failed: {e}"));
                            }
                        }
                    }
                }

                let _ = ui_tx.send(UiEvent::RepoModelData { items, labels });
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RefreshSnapshots { repo_selector } => {
                let _ = ui_tx.send(UiEvent::Status("Loading snapshots...".to_string()));

                let repos_to_scan = match select_repos(&runtime.repos, &repo_selector) {
                    Ok(repos) => repos,
                    Err(e) => {
                        send_log(&ui_tx, format!("Failed to select repository: {e}"));
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let mut data = Vec::new();

                for repo in repos_to_scan {
                    let repo_name = format_repo_name(repo);
                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(pass) => pass,
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                            continue;
                        }
                    };

                    match operations::list_snapshots_with_stats(
                        &repo.config,
                        passphrase.as_deref().map(|s| s.as_str()),
                    ) {
                        Ok(mut snapshots) => {
                            snapshots.sort_by_key(|(s, _)| s.time);
                            for (s, stats) in snapshots {
                                let ts: DateTime<Local> = s.time.with_timezone(&Local);
                                let sources = if s.source_paths.is_empty() {
                                    if s.source_label.is_empty() {
                                        "-".to_string()
                                    } else {
                                        s.source_label.clone()
                                    }
                                } else {
                                    s.source_paths.join("\n")
                                };
                                let label = if s.source_label.is_empty() {
                                    "-".to_string()
                                } else {
                                    s.source_label.clone()
                                };
                                let hostname = if s.hostname.is_empty() {
                                    "-".to_string()
                                } else {
                                    s.hostname.clone()
                                };
                                data.push(SnapshotRowData {
                                    id: s.name.clone(),
                                    hostname,
                                    time_str: ts.format("%Y-%m-%d %H:%M:%S").to_string(),
                                    source: sources,
                                    label,
                                    files: format_count(stats.nfiles),
                                    size: format_bytes(stats.deduplicated_size),
                                    nfiles: stats.nfiles,
                                    size_bytes: stats.deduplicated_size,
                                    time_epoch: s.time.timestamp(),
                                    repo_name: repo_name.clone(),
                                });
                            }
                        }
                        Err(e) => {
                            send_log(
                                &ui_tx,
                                format!("[{repo_name}] snapshot listing failed: {e}"),
                            );
                        }
                    }
                }

                let _ = ui_tx.send(UiEvent::SnapshotTableData { data });
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::FetchSnapshotContents {
                repo_name,
                snapshot_name,
            } => {
                let _ = ui_tx.send(UiEvent::Status("Loading snapshot contents...".to_string()));

                match find_repo_for_snapshot(
                    &runtime.repos,
                    &repo_name,
                    &snapshot_name,
                    &mut passphrases,
                ) {
                    Ok((repo, passphrase)) => {
                        match operations::list_snapshot_items(
                            &repo.config,
                            passphrase.as_deref().map(|s| s.as_str()),
                            &snapshot_name,
                        ) {
                            Ok(items) => {
                                send_log(
                                    &ui_tx,
                                    format!(
                                        "Loaded {} item(s) from snapshot {} in [{}]",
                                        items.len(),
                                        snapshot_name,
                                        format_repo_name(repo)
                                    ),
                                );

                                let _ = ui_tx.send(UiEvent::SnapshotContentsData { items });
                            }
                            Err(e) => {
                                send_log(&ui_tx, format!("Failed to load snapshot items: {e}"));
                            }
                        }
                    }
                    Err(e) => {
                        send_log(&ui_tx, format!("Failed to resolve snapshot: {e}"));
                    }
                }

                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RestoreSelected {
                repo_name,
                snapshot,
                dest,
                paths,
            } => {
                cancel_requested.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted);
                let _ = ui_tx.send(UiEvent::Status("Restoring selected items...".to_string()));

                match find_repo_for_snapshot(
                    &runtime.repos,
                    &repo_name,
                    &snapshot,
                    &mut passphrases,
                ) {
                    Ok((repo, passphrase)) => {
                        let path_set: std::collections::HashSet<String> =
                            paths.into_iter().collect();
                        match operations::restore_selected(
                            &repo.config,
                            passphrase.as_deref().map(|s| s.as_str()),
                            &snapshot,
                            &dest,
                            &path_set,
                        ) {
                            Ok(stats) => {
                                send_log(
                                    &ui_tx,
                                    format!(
                                        "Restored selected items from {} -> {} (files={}, dirs={}, symlinks={}, bytes={})",
                                        snapshot,
                                        dest,
                                        stats.files,
                                        stats.dirs,
                                        stats.symlinks,
                                        format_bytes(stats.total_bytes),
                                    ),
                                );
                                let _ = ui_tx
                                    .send(UiEvent::RestoreStatus("Restore complete.".to_string()));
                            }
                            Err(e) => {
                                send_log(&ui_tx, format!("Restore failed: {e}"));
                                let _ = ui_tx
                                    .send(UiEvent::RestoreStatus("Restore failed.".to_string()));
                            }
                        }
                    }
                    Err(e) => {
                        send_log(&ui_tx, format!("Failed to resolve snapshot: {e}"));
                        let _ = ui_tx.send(UiEvent::RestoreStatus("Restore failed.".to_string()));
                    }
                }

                let _ = ui_tx.send(UiEvent::OperationFinished);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::DeleteSnapshot {
                repo_name,
                snapshot_name,
            } => {
                // Confirm with user
                let confirmed = tinyfiledialogs::message_box_yes_no(
                    "Delete Snapshot",
                    &format!(
                        "Are you sure you want to delete snapshot {snapshot_name} from {repo_name}?"
                    ),
                    tinyfiledialogs::MessageBoxIcon::Question,
                    tinyfiledialogs::YesNo::No,
                );

                if confirmed == tinyfiledialogs::YesNo::No {
                    send_log(&ui_tx, "Snapshot deletion cancelled.");
                    continue;
                }

                cancel_requested.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted);
                let _ = ui_tx.send(UiEvent::Status("Deleting snapshot...".to_string()));

                let repo = match config::select_repo(&runtime.repos, &repo_name) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_name}'."));
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                match operations::delete_snapshot(
                    &repo.config,
                    passphrase.as_deref().map(|s| s.as_str()),
                    &snapshot_name,
                ) {
                    Ok(stats) => {
                        send_log(
                            &ui_tx,
                            format!(
                                "[{repo_name}] Deleted snapshot '{}': {} chunks freed, {} reclaimed",
                                stats.snapshot_name,
                                stats.chunks_deleted,
                                format_bytes(stats.space_freed),
                            ),
                        );
                        // Auto-refresh snapshots
                        let _ = app_tx.send(AppCommand::RefreshSnapshots {
                            repo_selector: repo_name,
                        });
                    }
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] delete failed: {e}"));
                    }
                }
                let _ = ui_tx.send(UiEvent::OperationFinished);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::FindFiles {
                repo_name,
                name_pattern,
            } => {
                cancel_requested.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted);
                let _ = ui_tx.send(UiEvent::Status("Searching files...".to_string()));

                let repo = match config::select_repo(&runtime.repos, &repo_name) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_name}'."));
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let filter = match FindFilter::build(
                    None,
                    Some(&name_pattern),
                    None,
                    None,
                    None,
                    None,
                    None,
                ) {
                    Ok(f) => f,
                    Err(e) => {
                        send_log(&ui_tx, format!("Invalid name pattern: {e}"));
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let scope = FindScope {
                    source_label: None,
                    last_n: None,
                };

                match vykar_core::commands::find::run(
                    &repo.config,
                    passphrase.as_deref().map(|s| s.as_str()),
                    &scope,
                    &filter,
                ) {
                    Ok(timelines) => {
                        let mut rows = Vec::new();
                        for tl in &timelines {
                            for ah in &tl.hits {
                                let ts: DateTime<Local> =
                                    ah.hit.snapshot_time.with_timezone(&Local);
                                rows.push(FindResultRow {
                                    path: tl.path.clone(),
                                    snapshot: ah.hit.snapshot_name.clone(),
                                    date: ts.format("%Y-%m-%d %H:%M:%S").to_string(),
                                    size: format_bytes(ah.hit.size),
                                    status: match ah.status {
                                        FileStatus::Added => "Added".to_string(),
                                        FileStatus::Modified => "Modified".to_string(),
                                        FileStatus::Unchanged => "Unchanged".to_string(),
                                    },
                                });
                            }
                        }
                        send_log(
                            &ui_tx,
                            format!(
                                "[{repo_name}] Find '{}': {} paths, {} total hits",
                                name_pattern,
                                timelines.len(),
                                rows.len(),
                            ),
                        );
                        let _ = ui_tx.send(UiEvent::FindResultsData { rows });
                    }
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] find failed: {e}"));
                    }
                }

                let _ = ui_tx.send(UiEvent::OperationFinished);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::OpenConfigFile => {
                let path = runtime.source.path().display().to_string();
                send_log(&ui_tx, format!("Opening config file: {path}"));
                let _ = std::process::Command::new("open").arg(&path).spawn();
            }
            AppCommand::ReloadConfig => {
                let config_path = dunce::canonicalize(runtime.source.path())
                    .unwrap_or_else(|_| runtime.source.path().to_path_buf());
                apply_config(
                    config_path,
                    false,
                    &mut runtime,
                    &mut config_display_path,
                    &mut passphrases,
                    &scheduler,
                    schedule_paused,
                    scheduler_lock_held,
                    &ui_tx,
                    &app_tx,
                );
            }
            AppCommand::SwitchConfig => {
                let picked = tinyfiledialogs::open_file_dialog(
                    "Open vykar config",
                    "",
                    Some((&["*.yaml", "*.yml"], "YAML files")),
                );
                if let Some(path_str) = picked {
                    apply_config(
                        PathBuf::from(path_str),
                        true,
                        &mut runtime,
                        &mut config_display_path,
                        &mut passphrases,
                        &scheduler,
                        schedule_paused,
                        scheduler_lock_held,
                        &ui_tx,
                        &app_tx,
                    );
                }
            }
            AppCommand::SaveAndApplyConfig { yaml_text } => {
                let config_path = config_display_path.clone();
                let tmp_path = config_path.with_extension("yaml.tmp");
                if let Err(e) = std::fs::write(&tmp_path, &yaml_text) {
                    let _ = ui_tx.send(UiEvent::ConfigSaveError(format!("Write failed: {e}")));
                    continue;
                }

                if let Err(msg) = validate_config(&tmp_path) {
                    let _ = std::fs::remove_file(&tmp_path);
                    let _ = ui_tx.send(UiEvent::ConfigSaveError(msg));
                    continue;
                }

                if let Err(e) = std::fs::rename(&tmp_path, &config_path) {
                    let _ = std::fs::remove_file(&tmp_path);
                    let _ = ui_tx.send(UiEvent::ConfigSaveError(format!("Rename failed: {e}")));
                    continue;
                }

                // apply_config re-runs validate_config internally, which is
                // redundant but harmless — it keeps the function self-contained.
                if apply_config(
                    config_path,
                    false,
                    &mut runtime,
                    &mut config_display_path,
                    &mut passphrases,
                    &scheduler,
                    schedule_paused,
                    scheduler_lock_held,
                    &ui_tx,
                    &app_tx,
                ) {
                    send_log(&ui_tx, "Configuration saved and applied.");
                } else {
                    let _ = ui_tx.send(UiEvent::ConfigSaveError(
                        "Config saved to disk but failed to apply. Check log for details.".into(),
                    ));
                }
            }
            AppCommand::ShowWindow => {
                let _ = ui_tx.send(UiEvent::ShowWindow);
            }
            AppCommand::Quit => {
                let _ = ui_tx.send(UiEvent::Quit);
                break;
            }
        }
    }
}

// ── Main ──

thread_local! {
    static LOG_MODEL: RefCell<Option<Rc<VecModel<ModelRc<StandardListViewItem>>>>> = const { RefCell::new(None) };
    static FILE_TREE: RefCell<Option<FileTree>> = const { RefCell::new(None) };
}

fn ensure_log_model(ui: &MainWindow) -> Rc<VecModel<ModelRc<StandardListViewItem>>> {
    LOG_MODEL.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            let model = Rc::new(VecModel::<ModelRc<StandardListViewItem>>::default());
            ui.set_log_rows(ModelRc::from(model.clone()));
            *borrow = Some(model);
        }
        borrow.as_ref().unwrap().clone()
    })
}

const MAX_LOG_ROWS: usize = 10_000;
const TRIM_TARGET: usize = 9_000;

fn append_log_row(ui: &MainWindow, timestamp: &str, message: &str) {
    let model = ensure_log_model(ui);
    let row: Vec<StandardListViewItem> = vec![
        StandardListViewItem::from(SharedString::from(timestamp)),
        StandardListViewItem::from(SharedString::from(message)),
    ];
    model.push(ModelRc::new(VecModel::from(row)));
    if model.row_count() > MAX_LOG_ROWS {
        // Rebuild from the newest TRIM_TARGET rows in one shot to avoid
        // O(n)-per-row front-removal and repeated model-change notifications.
        let start = model.row_count() - TRIM_TARGET;
        let keep: Vec<_> = (start..model.row_count())
            .map(|i| model.row_data(i).unwrap())
            .collect();
        let fresh = Rc::new(VecModel::from(keep));
        ui.set_log_rows(ModelRc::from(fresh.clone()));
        LOG_MODEL.with(|cell| *cell.borrow_mut() = Some(fresh));
    }
}

fn refresh_tree_view(rw: &RestoreWindow) {
    FILE_TREE.with(|cell| {
        if let Some(ref tree) = *cell.borrow() {
            let rows = tree.to_slint_model();
            let selection = tree.selection_text();
            rw.set_tree_rows(ModelRc::new(VecModel::from(rows)));
            rw.set_selection_text(selection.into());
        }
    });
}

fn resolve_or_create_config(
    saved_config_path: Option<&str>,
) -> Result<app::RuntimeConfig, Box<dyn std::error::Error>> {
    use vykar_core::config::ConfigSource;

    // 0. Try saved config path from GUI state (if file still exists)
    if let Some(saved) = saved_config_path {
        let path = PathBuf::from(saved);
        if path.is_file() {
            if let Ok(repos) = config::load_and_resolve(&path) {
                let source = ConfigSource::SearchOrder {
                    path,
                    level: "user",
                };
                return Ok(app::RuntimeConfig { source, repos });
            }
        }
    }

    // 1. Check standard search paths (env var, project, user, system)
    if let Some(source) = config::resolve_config_path(None) {
        let repos = config::load_and_resolve(source.path())?;
        return Ok(app::RuntimeConfig { source, repos });
    }

    // 2. No config found — ask the user what to do
    let user_config_path = config::default_config_search_paths()
        .into_iter()
        .find(|(_, level)| *level == "user")
        .map(|(p, _)| p);

    let message = match &user_config_path {
        Some(p) => format!(
            "No vykar configuration file was found.\n\n\
             Create a starter config at\n{}?\n\n\
             Select No to open an existing file instead.",
            p.display()
        ),
        None => "No vykar configuration file was found.\n\n\
                 Select Yes to pick an existing config file."
            .to_string(),
    };

    let choice = tinyfiledialogs::message_box_yes_no(
        "No configuration found",
        &message,
        tinyfiledialogs::MessageBoxIcon::Question,
        tinyfiledialogs::YesNo::Yes,
    );

    let config_path = if choice == tinyfiledialogs::YesNo::Yes {
        if let Some(path) = user_config_path {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, config::minimal_config_template())?;
            path
        } else {
            // No user-level path available, fall through to file picker
            let picked = tinyfiledialogs::open_file_dialog(
                "Open vykar config",
                "",
                Some((&["*.yaml", "*.yml"], "YAML files")),
            );
            match picked {
                Some(p) => PathBuf::from(p),
                None => std::process::exit(0),
            }
        }
    } else {
        let picked = tinyfiledialogs::open_file_dialog(
            "Open vykar config",
            "",
            Some((&["*.yaml", "*.yml"], "YAML files")),
        );
        match picked {
            Some(p) => PathBuf::from(p),
            None => std::process::exit(0),
        }
    };

    let repos = config::load_and_resolve(&config_path)?;
    let source = ConfigSource::SearchOrder {
        path: config_path,
        level: "user",
    };
    Ok(app::RuntimeConfig { source, repos })
}

fn capture_gui_state(
    ui: &MainWindow,
    active_config_path: &Arc<Mutex<String>>,
) -> Option<state::GuiState> {
    let win_size = ui.window().size();
    let scale = ui.window().scale_factor();
    if win_size.width == 0 || win_size.height == 0 {
        return None;
    }
    if !scale.is_finite() || scale <= 0.0 {
        return None;
    }
    let w = win_size.width as f32 / scale;
    let h = win_size.height as f32 / scale;
    if !w.is_finite() || !h.is_finite() || w <= 0.0 || h <= 0.0 {
        return None;
    }
    let config_path = active_config_path.lock().ok().map(|cp| cp.clone());
    Some(state::GuiState {
        config_path,
        window_width: Some(w),
        window_height: Some(h),
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tray-icon uses GTK widgets internally on Linux; GTK must be
    // initialised before any Menu / MenuItem is created.
    #[cfg(target_os = "linux")]
    gtk::init().expect("Failed to initialize GTK");

    let gui_state = state::load();
    let runtime = resolve_or_create_config(gui_state.config_path.as_deref())?;

    // Track the active config path so we can persist it on quit.
    let active_config_path = Arc::new(Mutex::new(
        dunce::canonicalize(runtime.source.path())
            .unwrap_or_else(|_| runtime.source.path().to_path_buf())
            .display()
            .to_string(),
    ));
    // Last captured GUI state — updated on every window hide so we have a valid
    // snapshot even if the window is already destroyed when the process exits.
    let last_gui_state: Arc<Mutex<Option<state::GuiState>>> = Arc::new(Mutex::new(None));

    let (app_tx, app_rx) = crossbeam_channel::unbounded::<AppCommand>();
    let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();

    let scheduler = Arc::new(Mutex::new(SchedulerState::default()));
    let backup_running = Arc::new(AtomicBool::new(false));
    let cancel_requested = Arc::new(AtomicBool::new(false));

    // Attempt to acquire the process-wide scheduler lock.
    // If another scheduler (daemon or GUI) holds it, disable automatic scheduling
    // but keep the GUI fully functional for manual operations.
    let scheduler_lock = vykar_core::app::scheduler::SchedulerLock::try_acquire();
    let scheduler_lock_held = scheduler_lock.is_some();
    // Keep the lock alive for the entire process lifetime.
    let _scheduler_lock = scheduler_lock;

    if !scheduler_lock_held {
        let _ = ui_tx.send(UiEvent::LogEntry {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            message: "Scheduler disabled \u{2014} another vykar scheduler is already running (daemon or GUI).".to_string(),
        });
    }

    spawn_scheduler(
        app_tx.clone(),
        ui_tx.clone(),
        scheduler.clone(),
        backup_running.clone(),
    );

    let ui_tx_for_cancel = ui_tx.clone();

    thread::spawn({
        let app_tx = app_tx.clone();
        let scheduler = scheduler.clone();
        let backup_running = backup_running.clone();
        let cancel_requested = cancel_requested.clone();
        move || {
            run_worker(
                app_tx,
                app_rx,
                ui_tx,
                scheduler,
                backup_running,
                cancel_requested,
                runtime,
                scheduler_lock_held,
            )
        }
    });

    let ui = MainWindow::new()?;
    if let (Some(w), Some(h)) = (gui_state.window_width, gui_state.window_height) {
        ui.window().set_size(slint::LogicalSize::new(w, h));
    }
    ui.set_config_path("(loading...)".into());
    ui.set_schedule_text("(loading...)".into());
    ui.set_editor_font_family(
        if cfg!(target_os = "macos") {
            "Menlo"
        } else if cfg!(target_os = "windows") {
            "Consolas"
        } else {
            "DejaVu Sans Mono"
        }
        .into(),
    );
    ui.set_status_text("Idle".into());

    let restore_win = RestoreWindow::new()?;
    let find_win = FindWindow::new()?;

    // Parallel arrays for looking up names by table row index.
    // Wrapped in Arc<Mutex<>> so callbacks and the event loop can share them.
    let repo_labels: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let source_labels: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let snapshot_ids: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let snapshot_repo_names: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let snapshot_data: Arc<Mutex<Vec<SnapshotRowData>>> = Arc::new(Mutex::new(Vec::new()));

    // ── Event loop consumer ──

    let ui_weak_for_events = ui.as_weak();
    let restore_weak_for_events = restore_win.as_weak();
    let find_weak_for_events = find_win.as_weak();
    let repo_labels_for_events = repo_labels.clone();
    let source_labels_for_events = source_labels.clone();
    let snapshot_ids_for_events = snapshot_ids.clone();
    let snapshot_repo_names_for_events = snapshot_repo_names.clone();
    let snapshot_data_for_events = snapshot_data.clone();
    let app_tx_for_events = app_tx.clone();
    let active_config_path_for_events = active_config_path.clone();
    let last_gui_state_for_events = last_gui_state.clone();
    let tray_source_items: Arc<Mutex<Vec<(MenuId, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let (submenu_labels_tx, submenu_labels_rx) = crossbeam_channel::unbounded::<Vec<String>>();

    thread::spawn(move || {
        while let Ok(event) = ui_rx.recv() {
            let ui_weak = ui_weak_for_events.clone();
            let restore_weak = restore_weak_for_events.clone();
            let find_weak = find_weak_for_events.clone();
            let repo_labels = repo_labels_for_events.clone();
            let source_labels = source_labels_for_events.clone();
            let snapshot_ids = snapshot_ids_for_events.clone();
            let snapshot_repo_names = snapshot_repo_names_for_events.clone();
            let snapshot_data = snapshot_data_for_events.clone();
            let app_tx = app_tx_for_events.clone();
            let active_config_path = active_config_path_for_events.clone();
            let last_gui_state = last_gui_state_for_events.clone();
            let submenu_labels_tx = submenu_labels_tx.clone();

            let _ = slint::invoke_from_event_loop(move || {
                let Some(ui) = ui_weak.upgrade() else {
                    return;
                };

                match event {
                    UiEvent::Status(status) => ui.set_status_text(status.into()),
                    UiEvent::LogEntry { timestamp, message } => {
                        append_log_row(&ui, &timestamp, &message);
                    }
                    UiEvent::ConfigInfo { path, schedule } => {
                        if let Ok(mut cp) = active_config_path.lock() {
                            *cp = path.clone();
                        }
                        ui.set_config_path(path.into());
                        ui.set_schedule_text(schedule.into());
                        // Eagerly persist so Cmd-Q keeps the config path.
                        if let Some(s) = capture_gui_state(&ui, &active_config_path) {
                            state::save(&s);
                            if let Ok(mut last) = last_gui_state.lock() {
                                *last = Some(s);
                            }
                        }
                    }
                    UiEvent::RepoNames(names) => {
                        let first = names.first().cloned().unwrap_or_default();
                        ui.set_repo_names(to_string_model(names));
                        // Pre-select first repo in snapshots combo and auto-load
                        if ui.get_snapshots_repo_combo_value().is_empty() && !first.is_empty() {
                            ui.set_snapshots_repo_combo_value(first.clone().into());
                            let _ = app_tx.send(AppCommand::RefreshSnapshots {
                                repo_selector: first,
                            });
                        }
                    }
                    UiEvent::RepoModelData { items, labels } => {
                        ui.set_repo_loading(false);
                        if let Ok(mut rl) = repo_labels.lock() {
                            *rl = labels;
                        }
                        let model: Vec<RepoInfo> = items
                            .into_iter()
                            .map(|d| RepoInfo {
                                name: d.name.into(),
                                url: d.url.into(),
                                snapshots: d.snapshots.into(),
                                last_snapshot: d.last_snapshot.into(),
                                size: d.size.into(),
                            })
                            .collect();
                        ui.set_repo_model(ModelRc::new(VecModel::from(model)));
                    }
                    UiEvent::SourceModelData { items, labels } => {
                        // Signal the main thread to rebuild the tray submenu
                        let _ = submenu_labels_tx.send(labels.clone());

                        if let Ok(mut sl) = source_labels.lock() {
                            *sl = labels;
                        }
                        let model: Vec<SourceInfo> = items
                            .into_iter()
                            .map(|d| SourceInfo {
                                label: d.label.into(),
                                paths: d.paths.into(),
                                excludes: d.excludes.into(),
                                target_repos: d.target_repos.into(),
                                expanded: false,
                                detail_paths: d.detail_paths.into(),
                                detail_excludes: d.detail_excludes.into(),
                                detail_exclude_if_present: d.detail_exclude_if_present.into(),
                                detail_flags: d.detail_flags.into(),
                                detail_hooks: d.detail_hooks.into(),
                                detail_retention: d.detail_retention.into(),
                                detail_command_dumps: d.detail_command_dumps.into(),
                            })
                            .collect();
                        ui.set_source_model(ModelRc::new(VecModel::from(model)));
                    }
                    UiEvent::SnapshotTableData { data } => {
                        if let Ok(mut si) = snapshot_ids.lock() {
                            *si = data.iter().map(|d| d.id.clone()).collect();
                        }
                        if let Ok(mut sr) = snapshot_repo_names.lock() {
                            *sr = data.iter().map(|d| d.repo_name.clone()).collect();
                        }
                        let rows: Vec<Vec<String>> = data
                            .iter()
                            .map(|d| {
                                vec![
                                    d.id.clone(),
                                    d.hostname.clone(),
                                    d.time_str.clone(),
                                    d.source.clone(),
                                    d.label.clone(),
                                    d.files.clone(),
                                    d.size.clone(),
                                ]
                            })
                            .collect();
                        if let Ok(mut sd) = snapshot_data.lock() {
                            *sd = data;
                        }
                        ui.set_snapshot_rows(to_table_model(rows));
                    }
                    UiEvent::SnapshotContentsData { items } => {
                        if let Some(rw) = restore_weak.upgrade() {
                            let tree = FileTree::build_from_items(&items);
                            let selection = tree.selection_text();
                            let rows = tree.to_slint_model();
                            rw.set_tree_rows(ModelRc::new(VecModel::from(rows)));
                            rw.set_selection_text(selection.into());
                            rw.set_status_text("Ready".into());
                            FILE_TREE.with(|cell| {
                                *cell.borrow_mut() = Some(tree);
                            });
                        }
                    }
                    UiEvent::RestoreStatus(status) => {
                        if let Some(rw) = restore_weak.upgrade() {
                            rw.set_status_text(status.into());
                        }
                    }
                    UiEvent::FindResultsData { rows } => {
                        if let Some(fw) = find_weak.upgrade() {
                            let count = rows.len();
                            let table_rows: Vec<Vec<String>> = rows
                                .into_iter()
                                .map(|r| vec![r.snapshot, r.path, r.date, r.size, r.status])
                                .collect();
                            fw.set_result_rows(to_table_model(table_rows));
                            fw.set_status_text(format!("{count} results found.").into());
                        }
                    }
                    UiEvent::ConfigText(text) => {
                        ui.set_editor_baseline(text.clone().into());
                        ui.set_editor_text(text.into());
                        ui.set_editor_dirty(false);
                        ui.set_editor_status(SharedString::default());
                    }
                    UiEvent::ConfigSaveError(message) => {
                        ui.set_editor_status(message.into());
                    }
                    UiEvent::OperationStarted => {
                        ui.set_operation_busy(true);
                    }
                    UiEvent::OperationFinished => {
                        ui.set_operation_busy(false);
                    }
                    UiEvent::Quit => {
                        if let Some(s) = capture_gui_state(&ui, &active_config_path) {
                            if let Ok(mut last) = last_gui_state.lock() {
                                *last = Some(s);
                            }
                        }
                        let _ = slint::quit_event_loop();
                    }
                    UiEvent::ShowWindow => {
                        let _ = ui.show();
                        #[cfg(target_os = "macos")]
                        {
                            use objc2::MainThreadMarker;
                            use objc2_app_kit::NSApplication;
                            if let Some(mtm) = MainThreadMarker::new() {
                                NSApplication::sharedApplication(mtm).activate();
                            }
                        }
                    }
                    UiEvent::TriggerSnapshotRefresh => {
                        let sel = ui.get_snapshots_repo_combo_value().to_string();
                        let _ = app_tx.send(AppCommand::RefreshSnapshots { repo_selector: sel });
                    }
                }
            });
        }
    });

    // ── Callback wiring: MainWindow ──

    let tx = app_tx.clone();
    ui.on_open_config_clicked(move || {
        let _ = tx.send(AppCommand::OpenConfigFile);
    });

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_switch_config_clicked(move || {
        if let Some(u) = ui_weak.upgrade() {
            if u.get_editor_dirty() {
                let proceed = tinyfiledialogs::message_box_yes_no(
                    "Unsaved changes",
                    "You have unsaved changes in the editor. Discard them and switch config?",
                    tinyfiledialogs::MessageBoxIcon::Warning,
                    tinyfiledialogs::YesNo::No,
                );
                if proceed == tinyfiledialogs::YesNo::No {
                    return;
                }
            }
        }
        let _ = tx.send(AppCommand::SwitchConfig);
    });

    // Save and Apply — send editor text to worker for validation + save
    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_save_and_apply_clicked(move || {
        if let Some(u) = ui_weak.upgrade() {
            let yaml = u.get_editor_text().to_string();
            let _ = tx.send(AppCommand::SaveAndApplyConfig { yaml_text: yaml });
        }
    });

    // Discard — UI-local, no worker round-trip
    let ui_weak = ui.as_weak();
    ui.on_discard_clicked(move || {
        if let Some(u) = ui_weak.upgrade() {
            let baseline = u.get_editor_baseline();
            u.set_editor_text(baseline);
            u.set_editor_dirty(false);
            u.set_editor_status(SharedString::default());
        }
    });

    let tx = app_tx.clone();
    ui.on_backup_all_clicked(move || {
        let _ = tx.send(AppCommand::RunBackupAll { scheduled: false });
    });

    {
        let cancel = cancel_requested.clone();
        let log_tx = ui_tx_for_cancel.clone();
        ui.on_cancel_clicked(move || {
            cancel.store(true, Ordering::SeqCst);
            send_log(
                &log_tx,
                "Cancel requested; will stop after current step completes.",
            );
        });
    }

    // Find Files button — sync repo names and show FindWindow
    {
        let fw_weak = find_win.as_weak();
        let ui_weak = ui.as_weak();
        ui.on_find_files_clicked(move || {
            if let (Some(fw), Some(u)) = (fw_weak.upgrade(), ui_weak.upgrade()) {
                fw.set_repo_names(u.get_repo_names());
                if fw.get_repo_combo_value().is_empty() {
                    fw.set_repo_combo_value(u.get_snapshots_repo_combo_value());
                }
                let _ = fw.show();
            }
        });
    }

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_reload_config_clicked(move || {
        if let Some(u) = ui_weak.upgrade() {
            u.set_repo_loading(true);
        }
        let _ = tx.send(AppCommand::ReloadConfig);
    });

    let tx = app_tx.clone();
    let rl = repo_labels.clone();
    ui.on_backup_repo_clicked(move |idx| {
        let Some(i) = usize::try_from(idx).ok() else {
            return;
        };
        if let Ok(labels) = rl.lock() {
            if let Some(name) = labels.get(i) {
                let _ = tx.send(AppCommand::RunBackupRepo {
                    repo_name: name.clone(),
                });
            }
        }
    });

    let tx = app_tx.clone();
    let sl = source_labels.clone();
    ui.on_backup_source_clicked(move |idx| {
        let Some(i) = usize::try_from(idx).ok() else {
            return;
        };
        if let Ok(labels) = sl.lock() {
            if let Some(label) = labels.get(i) {
                let _ = tx.send(AppCommand::RunBackupSource {
                    source_label: label.clone(),
                });
            }
        }
    });

    {
        let ui_weak = ui.as_weak();
        ui.on_toggle_source_expanded(move |idx| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let model = ui.get_source_model();
            let mut items: Vec<SourceInfo> = (0..model.row_count())
                .filter_map(|i| model.row_data(i))
                .collect();
            if let Some(item) = items.get_mut(idx as usize) {
                item.expanded = !item.expanded;
            }
            ui.set_source_model(ModelRc::new(VecModel::from(items)));
        });
    }

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_refresh_snapshots_clicked(move || {
        let Some(ui) = ui_weak.upgrade() else {
            return;
        };
        let _ = tx.send(AppCommand::RefreshSnapshots {
            repo_selector: ui.get_snapshots_repo_combo_value().to_string(),
        });
    });

    let tx = app_tx.clone();
    ui.on_snapshots_repo_changed({
        let tx = tx.clone();
        move |value| {
            let _ = tx.send(AppCommand::RefreshSnapshots {
                repo_selector: value.to_string(),
            });
        }
    });

    // Snapshot sorting callbacks
    {
        let sd = snapshot_data.clone();
        let si = snapshot_ids.clone();
        let sr = snapshot_repo_names.clone();
        let ui_weak = ui.as_weak();
        ui.on_snapshot_sort_ascending(move |col_idx| {
            sort_snapshot_table(&sd, &si, &sr, &ui_weak, col_idx, true);
        });
    }
    {
        let sd = snapshot_data.clone();
        let si = snapshot_ids.clone();
        let sr = snapshot_repo_names.clone();
        let ui_weak = ui.as_weak();
        ui.on_snapshot_sort_descending(move |col_idx| {
            sort_snapshot_table(&sd, &si, &sr, &ui_weak, col_idx, false);
        });
    }

    let tx = app_tx.clone();
    let si = snapshot_ids.clone();
    let sr = snapshot_repo_names.clone();
    let rw_weak = restore_win.as_weak();
    ui.on_restore_selected_snapshot_clicked(move |row| {
        let Some(r) = usize::try_from(row).ok() else {
            return;
        };
        let (snap_name, rname) = {
            let ids = si.lock().unwrap_or_else(|e| e.into_inner());
            let rnames = sr.lock().unwrap_or_else(|e| e.into_inner());
            match (ids.get(r), rnames.get(r)) {
                (Some(id), Some(rn)) => (id.clone(), rn.clone()),
                _ => return,
            }
        };

        if let Some(rw) = rw_weak.upgrade() {
            rw.set_snapshot_name(snap_name.clone().into());
            rw.set_repo_name(rname.clone().into());
            rw.set_status_text("Loading contents...".into());
            rw.set_tree_rows(ModelRc::new(VecModel::<TreeRowData>::default()));
            rw.set_selection_text("".into());
            let _ = rw.show();
        }

        let _ = tx.send(AppCommand::FetchSnapshotContents {
            repo_name: rname,
            snapshot_name: snap_name,
        });
    });

    let tx = app_tx.clone();
    let si = snapshot_ids.clone();
    let sr = snapshot_repo_names.clone();
    ui.on_delete_selected_snapshot_clicked(move |row| {
        let Some(r) = usize::try_from(row).ok() else {
            return;
        };
        let (snap_name, rname) = {
            let ids = si.lock().unwrap_or_else(|e| e.into_inner());
            let rnames = sr.lock().unwrap_or_else(|e| e.into_inner());
            match (ids.get(r), rnames.get(r)) {
                (Some(id), Some(rn)) => (id.clone(), rn.clone()),
                _ => return,
            }
        };

        let _ = tx.send(AppCommand::DeleteSnapshot {
            repo_name: rname,
            snapshot_name: snap_name,
        });
    });

    // ── Callback wiring: RestoreWindow ──

    // Tree: toggle expanded
    {
        let rw_weak = restore_win.as_weak();
        restore_win.on_toggle_expanded(move |node_index| {
            let Some(rw) = rw_weak.upgrade() else {
                return;
            };
            let Some(ni) = usize::try_from(node_index).ok() else {
                return;
            };
            FILE_TREE.with(|cell| {
                if let Some(ref mut tree) = *cell.borrow_mut() {
                    tree.toggle_expanded(ni);
                }
            });
            refresh_tree_view(&rw);
        });
    }

    // Tree: toggle checked
    {
        let rw_weak = restore_win.as_weak();
        restore_win.on_toggle_checked(move |node_index| {
            let Some(rw) = rw_weak.upgrade() else {
                return;
            };
            let Some(ni) = usize::try_from(node_index).ok() else {
                return;
            };
            FILE_TREE.with(|cell| {
                if let Some(ref mut tree) = *cell.borrow_mut() {
                    tree.toggle_check(ni);
                }
            });
            refresh_tree_view(&rw);
        });
    }

    // Expand All
    {
        let rw_weak = restore_win.as_weak();
        restore_win.on_expand_all_clicked(move || {
            let Some(rw) = rw_weak.upgrade() else {
                return;
            };
            FILE_TREE.with(|cell| {
                if let Some(ref mut tree) = *cell.borrow_mut() {
                    tree.expand_all();
                }
            });
            refresh_tree_view(&rw);
        });
    }

    // Collapse All
    {
        let rw_weak = restore_win.as_weak();
        restore_win.on_collapse_all_clicked(move || {
            let Some(rw) = rw_weak.upgrade() else {
                return;
            };
            FILE_TREE.with(|cell| {
                if let Some(ref mut tree) = *cell.borrow_mut() {
                    tree.collapse_all();
                }
            });
            refresh_tree_view(&rw);
        });
    }

    // Select All
    {
        let rw_weak = restore_win.as_weak();
        restore_win.on_select_all_clicked(move || {
            let Some(rw) = rw_weak.upgrade() else {
                return;
            };
            FILE_TREE.with(|cell| {
                if let Some(ref mut tree) = *cell.borrow_mut() {
                    tree.select_all();
                }
            });
            refresh_tree_view(&rw);
        });
    }

    // Deselect All
    {
        let rw_weak = restore_win.as_weak();
        restore_win.on_deselect_all_clicked(move || {
            let Some(rw) = rw_weak.upgrade() else {
                return;
            };
            FILE_TREE.with(|cell| {
                if let Some(ref mut tree) = *cell.borrow_mut() {
                    tree.deselect_all();
                }
            });
            refresh_tree_view(&rw);
        });
    }

    // Restore Selected — opens folder picker, then sends command
    {
        let tx = app_tx.clone();
        let rw_weak = restore_win.as_weak();
        restore_win.on_restore_selected_clicked(move || {
            let Some(rw) = rw_weak.upgrade() else {
                return;
            };

            let paths = FILE_TREE.with(|cell| {
                cell.borrow()
                    .as_ref()
                    .map(|tree| tree.collect_checked_paths())
                    .unwrap_or_default()
            });

            if paths.is_empty() {
                rw.set_status_text("No items selected.".into());
                return;
            }

            let dest = tinyfiledialogs::select_folder_dialog("Select restore destination", ".");
            let Some(dest) = dest else {
                return;
            };

            rw.set_status_text("Restoring...".into());
            let _ = tx.send(AppCommand::RestoreSelected {
                repo_name: rw.get_repo_name().to_string(),
                snapshot: rw.get_snapshot_name().to_string(),
                dest,
                paths,
            });
        });
    }

    // Cancel — hides restore window
    {
        let rw_weak = restore_win.as_weak();
        restore_win.on_cancel_clicked(move || {
            if let Some(rw) = rw_weak.upgrade() {
                let _ = rw.hide();
            }
        });
    }

    // ── Callback wiring: FindWindow ──

    {
        let tx = app_tx.clone();
        let fw_weak = find_win.as_weak();
        find_win.on_search_clicked(move || {
            let Some(fw) = fw_weak.upgrade() else {
                return;
            };
            let repo = fw.get_repo_combo_value().to_string();
            let pattern = fw.get_name_pattern().to_string();
            if repo.is_empty() || pattern.is_empty() {
                fw.set_status_text("Please select a repository and enter a name pattern.".into());
                return;
            }
            fw.set_status_text("Searching...".into());
            fw.set_result_rows(to_table_model(vec![]));
            let _ = tx.send(AppCommand::FindFiles {
                repo_name: repo,
                name_pattern: pattern,
            });
        });
    }

    {
        let fw_weak = find_win.as_weak();
        find_win.on_close_clicked(move || {
            if let Some(fw) = fw_weak.upgrade() {
                let _ = fw.hide();
            }
        });
    }

    // ── Close-to-tray behavior ──

    ui.window().on_close_requested({
        let ui_weak = ui.as_weak();
        let active_config_path = active_config_path.clone();
        let last_gui_state = last_gui_state.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                if let Some(s) = capture_gui_state(&ui, &active_config_path) {
                    state::save(&s);
                    if let Ok(mut last) = last_gui_state.lock() {
                        *last = Some(s);
                    }
                }
                let _ = ui.hide();
            }
            slint::CloseRequestResponse::HideWindow
        }
    });

    // ── Periodic resize-save timer ──
    // Flush GUI state to disk when the window size changes so Cmd-Q (which
    // bypasses on_close_requested) doesn't lose the latest dimensions.
    let _resize_save_timer = {
        let ui_weak = ui.as_weak();
        let active_config_path = active_config_path.clone();
        let last_gui_state = last_gui_state.clone();
        let mut last_saved_size: Option<(u32, u32)> = None;
        let timer = slint::Timer::default();
        timer.start(
            slint::TimerMode::Repeated,
            Duration::from_secs(2),
            move || {
                let Some(ui) = ui_weak.upgrade() else {
                    return;
                };
                let sz = ui.window().size();
                let current = (sz.width, sz.height);
                if current.0 == 0 || current.1 == 0 {
                    return;
                }
                if last_saved_size == Some(current) {
                    return;
                }
                if let Some(s) = capture_gui_state(&ui, &active_config_path) {
                    state::save(&s);
                    if let Ok(mut last) = last_gui_state.lock() {
                        *last = Some(s);
                    }
                    last_saved_size = Some(current);
                }
            },
        );
        timer
    };

    // ── Tray icon ──

    let (_tray_icon, open_item_id, run_now_item_id, quit_item_id, source_submenu, cancel_item) =
        build_tray_icon().map_err(|e| format!("failed to initialize tray icon: {e}"))?;

    let cancel_item_id = cancel_item.id().clone();

    // Timer to keep tray menu state in sync with the app.
    // Submenu/MenuItem are !Send, so they must stay on the main thread; the event
    // consumer sends updated labels via a channel and this timer picks them up.
    // The timer also syncs the cancel item's enabled state with backup_running.
    let _tray_sync_timer = {
        let tray_source_items = tray_source_items.clone();
        let backup_running = backup_running.clone();
        let timer = slint::Timer::default();
        let mut was_running = false;
        timer.start(
            slint::TimerMode::Repeated,
            Duration::from_millis(200),
            move || {
                // Drain all pending submenu updates, keeping only the latest
                let mut latest = None;
                while let Ok(labels) = submenu_labels_rx.try_recv() {
                    latest = Some(labels);
                }
                if let Some(labels) = latest {
                    while source_submenu.remove_at(0).is_some() {}
                    let mut new_items = Vec::new();
                    for label in &labels {
                        let mi = MenuItem::new(label, true, None);
                        new_items.push((mi.id().clone(), label.clone()));
                        let _ = source_submenu.append(&mi);
                    }
                    if let Ok(mut tsi) = tray_source_items.lock() {
                        *tsi = new_items;
                    }
                }

                // Sync cancel item enabled state
                let running = backup_running.load(Ordering::SeqCst);
                if running != was_running {
                    cancel_item.set_enabled(running);
                    was_running = running;
                }
            },
        );
        timer
    };

    {
        let tx = app_tx.clone();
        let tray_source_items = tray_source_items.clone();
        let cancel = cancel_requested.clone();
        let log_tx = ui_tx_for_cancel.clone();
        thread::spawn(move || {
            let menu_rx = MenuEvent::receiver();
            while let Ok(event) = menu_rx.recv() {
                if event.id == open_item_id {
                    let _ = tx.send(AppCommand::ShowWindow);
                } else if event.id == run_now_item_id {
                    let _ = tx.send(AppCommand::RunBackupAll { scheduled: false });
                } else if event.id == cancel_item_id {
                    cancel.store(true, Ordering::SeqCst);
                    send_log(
                        &log_tx,
                        "Cancel requested; will stop after current step completes.",
                    );
                } else if event.id == quit_item_id {
                    let _ = tx.send(AppCommand::Quit);
                    break;
                } else if let Ok(items) = tray_source_items.lock() {
                    if let Some((_, label)) = items.iter().find(|(id, _)| *id == event.id) {
                        let _ = tx.send(AppCommand::RunBackupSource {
                            source_label: label.clone(),
                        });
                    }
                }
            }
        });
    }

    ui.show()?;
    slint::run_event_loop_until_quit()?;

    // Persist GUI state. Eager saves (config change, resize timer, window hide)
    // cover most paths; this final capture handles Cmd-Q on macOS where the
    // event loop exits without triggering on_close_requested.
    let final_state = capture_gui_state(&ui, &active_config_path)
        .or_else(|| last_gui_state.lock().ok().and_then(|g| g.clone()));
    if let Some(s) = final_state {
        state::save(&s);
    }

    Ok(())
}
