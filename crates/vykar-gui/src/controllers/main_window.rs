use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_channel::Sender;
use slint::{ComponentHandle, Model, ModelRc, SharedString, VecModel};

use crate::controllers;
use crate::messages::{AppCommand, SnapshotRowData, SnapshotSelection, SourceInfoData, UiEvent};
use crate::repo_helpers::send_log;
use crate::view_models::{
    build_repo_source_model, current_repo_name, publish_snapshot_table, sort_snapshot_table,
    to_find_groups_model,
};
use crate::{AppData, MainWindow, SourceInfo, TreeRowData};

/// Return the row index iff exactly one snapshot is selected. The
/// single-row buttons (Restore, Mount) are gated on this in the UI but we
/// re-check here to avoid acting on a stale or racy selection.
fn single_selected_index(sel: &Arc<Mutex<SnapshotSelection>>) -> Option<usize> {
    let guard = sel.lock().ok()?;
    let mut iter = guard
        .selected
        .iter()
        .enumerate()
        .filter_map(|(i, s)| s.then_some(i));
    let only = iter.next()?;
    if iter.next().is_some() {
        return None;
    }
    Some(only)
}

/// Toggle the `expanded` flag on a `SourceInfo` row in the given model, which
/// must be a `VecModel<SourceInfo>`. Rebuilds the model since Slint's
/// `ModelRc` doesn't expose mutable row access.
fn toggle_expanded(
    ui: &MainWindow,
    idx: i32,
    getter: fn(&MainWindow) -> ModelRc<SourceInfo>,
    setter: fn(&MainWindow, ModelRc<SourceInfo>),
) {
    let model = getter(ui);
    let mut items: Vec<SourceInfo> = (0..model.row_count())
        .filter_map(|i| model.row_data(i))
        .collect();
    if let Some(item) = items.get_mut(idx as usize) {
        item.expanded = !item.expanded;
    }
    setter(ui, ModelRc::new(VecModel::from(items)));
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn wire_callbacks(
    ui: &MainWindow,
    app_tx: Sender<AppCommand>,
    ui_tx: Sender<UiEvent>,
    snapshot_data: Arc<Mutex<Vec<SnapshotRowData>>>,
    snapshot_selection: Arc<Mutex<SnapshotSelection>>,
    source_cache: Arc<Mutex<Vec<SourceInfoData>>>,
    cancel_requested: Arc<AtomicBool>,
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
            if let Some(ui) = ui_weak.upgrade() {
                toggle_expanded(
                    &ui,
                    idx,
                    MainWindow::get_source_model,
                    MainWindow::set_source_model,
                );
            }
        });
    }

    {
        let ui_weak = ui.as_weak();
        ui.on_toggle_repo_source_expanded(move |idx| {
            if let Some(ui) = ui_weak.upgrade() {
                toggle_expanded(
                    &ui,
                    idx,
                    MainWindow::get_repo_source_model,
                    MainWindow::set_repo_source_model,
                );
            }
        });
    }

    {
        let tx = app_tx.clone();
        let ui_weak = ui.as_weak();
        ui.on_backup_repo_source_clicked(move |idx| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let model = ui.get_repo_source_model();
            if let Some(item) = model.row_data(idx as usize) {
                let _ = tx.send(AppCommand::RunBackupSource {
                    source_label: item.label.to_string(),
                });
            }
        });
    }

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_refresh_snapshots_clicked(move || {
        let Some(ui) = ui_weak.upgrade() else {
            return;
        };
        if let Some(name) = current_repo_name(&ui) {
            let _ = tx.send(AppCommand::RefreshSnapshots {
                repo_selector: name,
            });
        }
    });

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_prune_snapshots_clicked(move || {
        let Some(ui) = ui_weak.upgrade() else {
            return;
        };
        if let Some(name) = current_repo_name(&ui) {
            let confirmed = tinyfiledialogs::message_box_yes_no(
                "Prune Snapshots",
                &format!(
                    "Run prune on {name}? This will delete snapshots that fall outside the retention rules for this repository."
                ),
                tinyfiledialogs::MessageBoxIcon::Question,
                tinyfiledialogs::YesNo::No,
            );
            if confirmed == tinyfiledialogs::YesNo::No {
                return;
            }
            let _ = tx.send(AppCommand::PruneRepo { repo_name: name });
        }
    });

    // Sidebar navigation
    {
        let ui_weak = ui.as_weak();
        ui.on_select_page(move |page| {
            if let Some(ui) = ui_weak.upgrade() {
                ui.set_current_page(page);
            }
        });
    }

    {
        let tx = app_tx.clone();
        let ui_weak = ui.as_weak();
        let source_cache = source_cache.clone();
        ui.on_select_repo_and_page(move |repo_idx, page| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let prev_repo = ui.get_current_repo_index();
            ui.set_current_repo_index(repo_idx);
            ui.set_current_page(page);

            if repo_idx != prev_repo {
                let labels = ui.global::<AppData>().get_repo_labels();
                if let Some(name) = labels.row_data(repo_idx as usize) {
                    let repo_name = name.to_string();
                    if let Ok(cache) = source_cache.lock() {
                        let model = build_repo_source_model(&cache, Some(repo_name.as_str()));
                        ui.set_repo_source_model(ModelRc::new(VecModel::from(model)));
                    }
                    let _ = tx.send(AppCommand::RefreshSnapshots {
                        repo_selector: repo_name,
                    });
                }
            }
        });
    }

    // Cancel the current operation — mirrors the tray "Cancel" menu item.
    {
        let cancel = cancel_requested.clone();
        let ui_tx = ui_tx.clone();
        ui.on_cancel_operation_clicked(move || {
            cancel.store(true, Ordering::SeqCst);
            send_log(
                &ui_tx,
                "Cancel requested; will stop after current step completes.",
            );
        });
    }

    // Find page — search callback (formerly FindWindow::on_search_clicked)
    {
        let tx = app_tx.clone();
        let ui_weak = ui.as_weak();
        ui.on_find_search_clicked(move || {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let pattern = ui.get_find_name_pattern().to_string();
            let repo = match current_repo_name(&ui) {
                Some(n) => n,
                None => {
                    ui.set_find_status_text("Please select a repository.".into());
                    return;
                }
            };
            if pattern.is_empty() {
                ui.set_find_status_text("Please enter a name pattern.".into());
                return;
            }
            ui.set_find_status_text("Searching...".into());
            ui.set_find_groups(to_find_groups_model(vec![]));
            ui.set_find_has_searched(false);
            let _ = tx.send(AppCommand::FindFiles {
                repo_name: repo,
                name_pattern: pattern,
            });
        });
    }

    // Snapshot sorting callbacks
    {
        let sd = snapshot_data.clone();
        let sel = snapshot_selection.clone();
        let ui_weak = ui.as_weak();
        ui.on_snapshot_sort_ascending(move |col_idx| {
            sort_snapshot_table(&sd, &sel, &ui_weak, col_idx, true);
        });
    }
    {
        let sd = snapshot_data.clone();
        let sel = snapshot_selection.clone();
        let ui_weak = ui.as_weak();
        ui.on_snapshot_sort_descending(move |col_idx| {
            sort_snapshot_table(&sd, &sel, &ui_weak, col_idx, false);
        });
    }

    // Snapshot row click — drives multi-row selection.
    {
        let sd = snapshot_data.clone();
        let sel = snapshot_selection.clone();
        let ui_weak = ui.as_weak();
        ui.on_snapshot_row_clicked(move |row, toggle, range| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let Ok(data) = sd.lock() else {
                return;
            };
            let Ok(mut selection) = sel.lock() else {
                return;
            };
            let len = data.len();
            if row < 0 || (row as usize) >= len {
                return;
            }
            let row = row as usize;
            // Resize defensively if the table mutated under us.
            if selection.selected.len() != len {
                selection.reset(len);
            }

            if range && selection.anchor.is_some() {
                let anchor = selection.anchor.unwrap();
                let (lo, hi) = if anchor <= row {
                    (anchor, row)
                } else {
                    (row, anchor)
                };
                for v in selection.selected.iter_mut() {
                    *v = false;
                }
                for v in &mut selection.selected[lo..=hi] {
                    *v = true;
                }
            } else if toggle {
                if let Some(slot) = selection.selected.get_mut(row) {
                    *slot = !*slot;
                }
                selection.anchor = Some(row);
            } else {
                for v in selection.selected.iter_mut() {
                    *v = false;
                }
                if let Some(slot) = selection.selected.get_mut(row) {
                    *slot = true;
                }
                selection.anchor = Some(row);
            }

            publish_snapshot_table(&ui, &data, &selection);
        });
    }

    {
        let tx = app_tx.clone();
        let sel = snapshot_selection.clone();
        let ui_weak = ui.as_weak();
        ui.on_restore_selected_snapshot_clicked(move || {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let Some(r) = single_selected_index(&sel) else {
                return;
            };
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
        let tx = app_tx.clone();
        let sel = snapshot_selection.clone();
        let ui_weak = ui.as_weak();
        ui.on_mount_selected_snapshot_clicked(move || {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let Some(r) = single_selected_index(&sel) else {
                return;
            };
            let ids = ui.global::<AppData>().get_snapshot_ids();
            let rnames = ui.global::<AppData>().get_snapshot_repo_names();
            let (snap_name, rname) = match (ids.row_data(r), rnames.row_data(r)) {
                (Some(id), Some(rn)) => (id.to_string(), rn.to_string()),
                _ => return,
            };
            // Optimistically mark active so the Mount buttons disable immediately.
            // MountStarted will set the real URL; MountFailed will clear this.
            ui.set_is_mount_active(true);
            let _ = tx.send(AppCommand::StartMount {
                repo_name: rname,
                snapshot_name: Some(snap_name),
            });
        });
    }

    {
        let tx = app_tx.clone();
        ui.on_stop_mount_clicked(move || {
            let _ = tx.send(AppCommand::StopMount);
        });
    }

    {
        let ui_weak = ui.as_weak();
        ui.on_open_mount_url_clicked(move || {
            if let Some(ui) = ui_weak.upgrade() {
                let url = ui.get_mount_url().to_string();
                if !url.is_empty() {
                    let _ = opener::open_browser(&url);
                }
            }
        });
    }

    {
        let tx = app_tx;
        let sel = snapshot_selection;
        let ui_weak = ui.as_weak();
        ui.on_delete_selected_snapshots_clicked(move || {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let Ok(selection) = sel.lock() else {
                return;
            };
            let ids = ui.global::<AppData>().get_snapshot_ids();
            let rnames = ui.global::<AppData>().get_snapshot_repo_names();
            // Group by repo so each repo's delete runs as a single batch under
            // one maintenance lock (`commands::delete::run`).
            let mut by_repo: std::collections::BTreeMap<String, Vec<String>> =
                std::collections::BTreeMap::new();
            for (i, sel_flag) in selection.selected.iter().enumerate() {
                if !*sel_flag {
                    continue;
                }
                let (Some(id), Some(rn)) = (ids.row_data(i), rnames.row_data(i)) else {
                    continue;
                };
                by_repo
                    .entry(rn.to_string())
                    .or_default()
                    .push(id.to_string());
            }
            for (repo_name, snapshot_names) in by_repo {
                let _ = tx.send(AppCommand::DeleteSnapshots {
                    repo_name,
                    snapshot_names,
                });
            }
        });
    }
}
