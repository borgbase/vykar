use std::cell::RefCell;

use crossbeam_channel::Sender;
use slint::{ComponentHandle, ModelRc, VecModel};
use vykar_core::snapshot::item::Item;

use crate::file_tree::FileTree;
use crate::messages::AppCommand;
use crate::RestoreWindow;

thread_local! {
    static FILE_TREE: RefCell<Option<FileTree>> = const { RefCell::new(None) };
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

pub(crate) fn handle_snapshot_contents(rw: &RestoreWindow, items: Vec<Item>) {
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

pub(crate) fn handle_restore_finished(rw: &RestoreWindow, success: bool, message: String) {
    rw.set_busy(false);
    if success {
        let _ = rw.hide();
        tinyfiledialogs::message_box_ok(
            "Restore",
            &format!("Restore complete.\n\n{message}"),
            tinyfiledialogs::MessageBoxIcon::Info,
        );
    } else {
        rw.set_status_text("Ready".into());
        tinyfiledialogs::message_box_ok(
            "Restore",
            "Restore failed. The destination folder must be empty.",
            tinyfiledialogs::MessageBoxIcon::Error,
        );
    }
}

pub(crate) fn wire_callbacks(restore_win: &RestoreWindow, app_tx: Sender<AppCommand>) {
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

            rw.set_busy(true);
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
}
