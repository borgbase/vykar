use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_channel::Sender;
use slint::{ComponentHandle, Model, ModelRc, SharedString, VecModel};

use crate::controllers;
use crate::messages::{AppCommand, SnapshotRowData, UiEvent};
use crate::repo_helpers::send_log;
use crate::view_models::sort_snapshot_table;
use crate::{AppData, MainWindow, SourceInfo, TreeRowData};

pub(crate) fn wire_callbacks(
    ui: &MainWindow,
    app_tx: Sender<AppCommand>,
    ui_tx: Sender<UiEvent>,
    cancel_requested: Arc<AtomicBool>,
    snapshot_data: Arc<Mutex<Vec<SnapshotRowData>>>,
) {
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
        let cancel = cancel_requested;
        let log_tx = ui_tx;
        ui.on_cancel_clicked(move || {
            cancel.store(true, Ordering::SeqCst);
            send_log(
                &log_tx,
                "Cancel requested; will stop after current step completes.",
            );
        });
    }

    // Find Files button — lazy-create FindWindow, sync repo names, and show
    {
        let tx = app_tx.clone();
        let ui_weak = ui.as_weak();
        ui.on_find_files_clicked(move || {
            if let (Some(fw), Some(u)) = (controllers::find::ensure_window(&tx), ui_weak.upgrade())
            {
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

    {
        let tx = app_tx.clone();
        let ui_weak = ui.as_weak();
        ui.on_backup_repo_clicked(move |idx| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let labels = ui.global::<AppData>().get_repo_labels();
            if let Some(name) = labels.row_data(idx as usize) {
                let _ = tx.send(AppCommand::RunBackupRepo {
                    repo_name: name.to_string(),
                });
            }
        });
    }

    {
        let tx = app_tx.clone();
        let ui_weak = ui.as_weak();
        ui.on_backup_source_clicked(move |idx| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let labels = ui.global::<AppData>().get_source_labels();
            if let Some(label) = labels.row_data(idx as usize) {
                let _ = tx.send(AppCommand::RunBackupSource {
                    source_label: label.to_string(),
                });
            }
        });
    }

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
        let ui_weak = ui.as_weak();
        ui.on_snapshot_sort_ascending(move |col_idx| {
            sort_snapshot_table(&sd, &ui_weak, col_idx, true);
        });
    }
    {
        let sd = snapshot_data;
        let ui_weak = ui.as_weak();
        ui.on_snapshot_sort_descending(move |col_idx| {
            sort_snapshot_table(&sd, &ui_weak, col_idx, false);
        });
    }

    {
        let tx = app_tx.clone();
        let ui_weak = ui.as_weak();
        ui.on_restore_selected_snapshot_clicked(move |row| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let r = row as usize;
            let ids = ui.global::<AppData>().get_snapshot_ids();
            let rnames = ui.global::<AppData>().get_snapshot_repo_names();
            let (snap_name, rname) = match (ids.row_data(r), rnames.row_data(r)) {
                (Some(id), Some(rn)) => (id.to_string(), rn.to_string()),
                _ => return,
            };

            // Clear stale tree data before showing the window for a new snapshot.
            controllers::restore::clear_file_tree();

            if let Some(rw) = controllers::restore::ensure_window(&tx) {
                rw.set_snapshot_name(snap_name.clone().into());
                rw.set_repo_name(rname.clone().into());
                rw.set_status_text("Loading contents...".into());
                rw.set_tree_rows(ModelRc::new(VecModel::<TreeRowData>::default()));
                rw.set_selection_text("".into());
                rw.set_source_root_text("".into());
                let _ = rw.show();
            }

            let _ = tx.send(AppCommand::FetchSnapshotContents {
                repo_name: rname,
                snapshot_name: snap_name,
            });
        });
    }

    {
        let tx = app_tx;
        let ui_weak = ui.as_weak();
        ui.on_delete_selected_snapshot_clicked(move |row| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let r = row as usize;
            let ids = ui.global::<AppData>().get_snapshot_ids();
            let rnames = ui.global::<AppData>().get_snapshot_repo_names();
            let (snap_name, rname) = match (ids.row_data(r), rnames.row_data(r)) {
                (Some(id), Some(rn)) => (id.to_string(), rn.to_string()),
                _ => return,
            };

            let _ = tx.send(AppCommand::DeleteSnapshot {
                repo_name: rname,
                snapshot_name: snap_name,
            });
        });
    }
}
