use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_channel::Sender;
use slint::{ComponentHandle, Model, ModelRc, SharedString, VecModel};

use crate::controllers;
use crate::messages::{AppCommand, UiEvent};
use crate::repo_helpers::send_log;
use crate::ui_state::{self, SourceModelScope};
use crate::view_models::to_find_groups_model;
use crate::{MainWindow, TreeRowData};

fn wire_recovery_button(
    ui: &MainWindow,
    app_tx: &Sender<AppCommand>,
    register: impl FnOnce(&MainWindow, Box<dyn Fn() + 'static>),
    dialog_title: &'static str,
    prompt_template: &'static str,
    make_command: impl Fn(String) -> AppCommand + 'static,
) {
    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    let cb = Box::new(move || {
        let Some(ui) = ui_weak.upgrade() else {
            return;
        };
        let Some(repo_name) = ui_state::current_repo_name(&ui) else {
            return;
        };
        let prompt = prompt_template.replace("{repo}", &repo_name);
        let confirmed = tinyfiledialogs::message_box_yes_no(
            dialog_title,
            &prompt,
            tinyfiledialogs::MessageBoxIcon::Warning,
            tinyfiledialogs::YesNo::No,
        );
        if confirmed == tinyfiledialogs::YesNo::Yes {
            let _ = tx.send(make_command(repo_name));
        }
    });
    register(ui, cb);
}

pub(crate) fn wire_callbacks(
    ui: &MainWindow,
    app_tx: Sender<AppCommand>,
    ui_tx: Sender<UiEvent>,
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

    wire_recovery_button(
        ui,
        &app_tx,
        |ui, cb| ui.on_clear_locks_clicked(cb),
        "Clear Repository Lock",
        "Clear advisory locks for {repo}?\n\nOnly use this if no Vykar operation is currently writing to this repository.",
        |repo_name| AppCommand::ClearRepoLocks { repo_name },
    );

    wire_recovery_button(
        ui,
        &app_tx,
        |ui, cb| ui.on_clear_sessions_clicked(cb),
        "Clear Backup Sessions",
        "Clear all backup sessions for {repo}?\n\nThis can affect live backups from this or another machine. Only continue if you are sure no backups are running.",
        |repo_name| AppCommand::ClearRepoSessions { repo_name },
    );

    {
        let tx = app_tx.clone();
        ui.on_backup_repo_clicked(move |idx| {
            if let Some(name) = ui_state::repo_name_at(idx) {
                let _ = tx.send(AppCommand::RunBackupRepo { repo_name: name });
            }
        });
    }

    {
        let tx = app_tx.clone();
        ui.on_backup_source_clicked(move |idx| {
            if let Some(label) = ui_state::source_label_at(idx) {
                let _ = tx.send(AppCommand::RunBackupSource {
                    source_label: label,
                });
            }
        });
    }

    {
        ui.on_toggle_source_expanded(move |idx| {
            ui_state::toggle_source_expanded(SourceModelScope::All, idx);
        });
    }

    {
        ui.on_toggle_repo_source_expanded(move |idx| {
            ui_state::toggle_source_expanded(SourceModelScope::Repo, idx);
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
        if let Some(name) = ui_state::current_repo_name(&ui) {
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
        if let Some(name) = ui_state::current_repo_name(&ui) {
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
        ui.on_select_repo_and_page(move |repo_idx, page| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let prev_repo = ui.get_current_repo_index();
            ui.set_current_repo_index(repo_idx);
            ui.set_current_page(page);

            if repo_idx != prev_repo {
                if let Some(repo_name) = ui_state::repo_name_at(repo_idx) {
                    ui_state::refresh_repo_source_model(Some(repo_name.as_str()));
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
                "Cancel requested; Vykar will stop when the current file, upload, or storage operation returns.",
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
            let repo = match ui_state::current_repo_name(&ui) {
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
        let ui_weak = ui.as_weak();
        ui.on_snapshot_sort_ascending(move |col_idx| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            ui_state::sort_snapshots(&ui, col_idx, true);
        });
    }
    {
        let ui_weak = ui.as_weak();
        ui.on_snapshot_sort_descending(move |col_idx| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            ui_state::sort_snapshots(&ui, col_idx, false);
        });
    }

    // Snapshot row click — drives multi-row selection.
    {
        let ui_weak = ui.as_weak();
        ui.on_snapshot_row_clicked(move |row, toggle, range| {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            ui_state::click_snapshot_row(&ui, row, toggle, range);
        });
    }

    {
        let tx = app_tx.clone();
        ui.on_restore_selected_snapshot_clicked(move || {
            let Some(snapshot) = ui_state::single_selected_snapshot() else {
                return;
            };

            // Clear stale tree data before showing the window for a new snapshot.
            controllers::restore::clear_file_tree();

            if let Some(rw) = controllers::restore::ensure_window(&tx) {
                rw.set_snapshot_name(snapshot.snapshot_id.clone().into());
                rw.set_repo_name(snapshot.repo_name.clone().into());
                rw.set_status_text("Loading contents...".into());
                rw.set_tree_rows(ModelRc::new(VecModel::<TreeRowData>::default()));
                rw.set_selection_text("".into());
                rw.set_source_root_text("".into());
                let _ = rw.show();
            }

            let _ = tx.send(AppCommand::FetchSnapshotContents {
                repo_name: snapshot.repo_name,
                snapshot_name: snapshot.snapshot_id,
            });
        });
    }

    {
        let tx = app_tx.clone();
        let ui_weak = ui.as_weak();
        ui.on_mount_selected_snapshot_clicked(move || {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let Some(snapshot) = ui_state::single_selected_snapshot() else {
                return;
            };
            // Optimistically mark active so the Mount buttons disable immediately.
            // MountStarted will set the real URL; MountFailed will clear this.
            ui.set_is_mount_active(true);
            let _ = tx.send(AppCommand::StartMount {
                repo_name: snapshot.repo_name,
                snapshot_name: Some(snapshot.snapshot_id),
            });
        });
    }

    {
        let tx = app_tx.clone();
        let ui_tx = ui_tx.clone();
        ui.on_diff_selected_snapshots_clicked(move || {
            let Some(snapshots) = ui_state::selected_snapshots(2) else {
                return;
            };
            let [first, second] = match snapshots.as_slice() {
                [a, b] => [a, b],
                _ => return,
            };
            if first.repo_name != second.repo_name {
                send_log(&ui_tx, "Cannot diff snapshots from different repositories.");
                return;
            }

            if let Some(dw) = controllers::diff::ensure_window() {
                controllers::diff::prepare_loading(
                    &dw,
                    &first.repo_name,
                    &first.snapshot_id,
                    &second.snapshot_id,
                );
                let _ = dw.show();
            }

            let _ = tx.send(AppCommand::DiffSnapshots {
                repo_name: first.repo_name.clone(),
                snapshot_a: first.snapshot_id.clone(),
                snapshot_b: second.snapshot_id.clone(),
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
        ui.on_delete_selected_snapshots_clicked(move || {
            // Group by repo so each repo's delete runs as a single batch under
            // one maintenance lock (`commands::delete::run`).
            for (repo_name, snapshot_names) in ui_state::selected_snapshots_by_repo() {
                let _ = tx.send(AppCommand::DeleteSnapshots {
                    repo_name,
                    snapshot_names,
                });
            }
        });
    }
}
