use std::cell::RefCell;

use crossbeam_channel::Sender;
use slint::{ComponentHandle, ModelRc, SharedString, VecModel};
use vykar_core::snapshot::item::Item;

use crate::file_tree::FileTree;
use crate::messages::AppCommand;
use crate::{RestoreWindow, TreeRowData};

thread_local! {
    static FILE_TREE: RefCell<Option<FileTree>> = const { RefCell::new(None) };
    /// Strong reference keeps the window alive even when hidden, so in-flight
    /// results (SnapshotContentsData, RestoreFinished) are never dropped.
    static RESTORE_HANDLE: RefCell<Option<RestoreWindow>> = const { RefCell::new(None) };
}

/// Return the existing RestoreWindow or lazily create and wire one.
/// Must be called on the main (UI) thread.
pub(crate) fn ensure_window(app_tx: &Sender<AppCommand>) -> Option<RestoreWindow> {
    RESTORE_HANDLE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if let Some(ref rw) = *borrow {
            return Some(rw.clone_strong());
        }
        let rw = RestoreWindow::new().ok()?;
        wire_callbacks(&rw, app_tx.clone());
        let handle = rw.clone_strong();
        *borrow = Some(rw);
        Some(handle)
    })
}

/// Access the RestoreWindow if it exists (runs closure on main thread).
pub(crate) fn with_window(f: impl FnOnce(&RestoreWindow)) {
    RESTORE_HANDLE.with(|cell| {
        if let Some(ref rw) = *cell.borrow() {
            f(rw);
        }
    });
}

/// Clear only the FILE_TREE backing data (not the UI).
/// Called when switching snapshots to prevent stale tree access.
pub(crate) fn clear_file_tree() {
    FILE_TREE.with(|cell| {
        *cell.borrow_mut() = None;
    });
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

pub(crate) fn handle_snapshot_contents(
    rw: &RestoreWindow,
    repo_name: &str,
    snapshot_name: &str,
    items: Vec<Item>,
) {
    // Discard stale results if the window has moved on to a different snapshot.
    if rw.get_repo_name() != repo_name || rw.get_snapshot_name() != snapshot_name {
        return;
    }
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

fn clear_tree(rw: &RestoreWindow) {
    FILE_TREE.with(|cell| {
        *cell.borrow_mut() = None;
    });
    rw.set_tree_rows(ModelRc::new(VecModel::<TreeRowData>::default()));
    rw.set_selection_text(SharedString::default());
}

pub(crate) fn handle_restore_finished(rw: &RestoreWindow, success: bool, message: String) {
    rw.set_busy(false);
    if success {
        clear_tree(rw);
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

    // Restore Selected — validate selection, spawn thread for folder picker
    // The folder dialog must NOT run on the Slint event loop thread because
    // on Windows the native dialog pumps its own Win32 message loop, which
    // deadlocks with Slint's event loop (borgbase/vykar#98).
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

            rw.set_status_text("Pick a destination folder...".into());

            let tx = tx.clone();
            let rw_weak = rw.as_weak();
            let repo_name = rw.get_repo_name().to_string();
            let snapshot = rw.get_snapshot_name().to_string();
            std::thread::spawn(move || {
                let dest = tinyfiledialogs::select_folder_dialog("Select restore destination", ".");
                let Some(dest) = dest else {
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(rw) = rw_weak.upgrade() {
                            rw.set_status_text("Ready".into());
                        }
                    });
                    return;
                };
                let _ = slint::invoke_from_event_loop({
                    let rw_weak = rw_weak.clone();
                    move || {
                        if let Some(rw) = rw_weak.upgrade() {
                            rw.set_busy(true);
                            rw.set_status_text("Restoring...".into());
                        }
                    }
                });
                let _ = tx.send(AppCommand::RestoreSelected {
                    repo_name,
                    snapshot,
                    dest,
                    paths,
                });
            });
        });
    }

    // Cancel — hides restore window
    {
        let rw_weak = restore_win.as_weak();
        restore_win.on_cancel_clicked(move || {
            if let Some(rw) = rw_weak.upgrade() {
                clear_tree(&rw);
                let _ = rw.hide();
            }
        });
    }

    // Titlebar close — clear tree so FILE_TREE doesn't keep snapshot data resident.
    {
        let rw_weak = restore_win.as_weak();
        restore_win.window().on_close_requested(move || {
            if let Some(rw) = rw_weak.upgrade() {
                clear_tree(&rw);
            }
            slint::CloseRequestResponse::HideWindow
        });
    }
}
