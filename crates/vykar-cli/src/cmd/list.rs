use comfy_table::{Cell, CellAlignment};

use vykar_core::app::views::SnapshotRowView;
use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::error::CliResult;
use crate::passphrase::with_repo_passphrase;
use crate::table::CliTableTheme;

pub(crate) fn run_list(
    config: &VykarConfig,
    label: Option<&str>,
    source_filter: &[String],
    last: Option<usize>,
) -> CliResult<()> {
    let listing = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::list::list_snapshots_with_stats(
            config, passphrase,
        )?)
    })?;
    let mut snapshots = listing.snapshots;

    // Never present a truncated list as if it were complete. Reported before
    // the empty-list early return below: when the only snapshot in the repo is
    // one we cannot read, "No snapshots found." is actively misleading. The
    // filters below must not suppress it either — a hidden snapshot's label is
    // unknown, so it can never be filtered out safely.
    let hidden = vykar_core::repo::snapshot_cache::describe_skipped(&listing.hidden);
    if let Some(ref msg) = hidden {
        eprintln!("warning: {msg}");
    }

    // Filter by source label if requested
    if !source_filter.is_empty() {
        snapshots.retain(|(e, _)| source_filter.iter().any(|f| f == &e.source_label));
    }

    // Truncate to last N entries
    if let Some(n) = last {
        let len = snapshots.len();
        if n < len {
            snapshots.drain(..len - n);
        }
    }
    if snapshots.is_empty() {
        println!("No snapshots found.");
        return Ok(());
    }

    let theme = CliTableTheme::detect();
    let mut table = theme.new_data_table(&[
        "ID", "Date", "Host", "Label", "Source", "Files", "Size", "Added",
    ]);

    // Right-align Files, Size and Added columns (indices 5, 6 and 7)
    let col = table.column_mut(5).expect("Files column");
    col.set_cell_alignment(CellAlignment::Right);
    let col = table.column_mut(6).expect("Size column");
    col.set_cell_alignment(CellAlignment::Right);
    let col = table.column_mut(7).expect("Added column");
    col.set_cell_alignment(CellAlignment::Right);

    let mut prev_group: Option<(String, String)> = None;

    for (entry, stats) in &snapshots {
        let row = SnapshotRowView::new(entry, stats.as_ref());

        // Repeated host/label pairs are blanked so a run of snapshots from one
        // source reads as a group.
        let current_group = (row.hostname.clone(), row.label.clone());
        let (host_col, label_col) = if prev_group.as_ref() == Some(&current_group) {
            (String::new(), String::new())
        } else {
            current_group.clone()
        };
        prev_group = Some(current_group);

        let source_col = if row.source_paths.is_empty() {
            "-".to_string()
        } else {
            row.source_paths.join("\n")
        };

        table.add_row(vec![
            Cell::new(row.id),
            Cell::new(row.time),
            Cell::new(host_col),
            Cell::new(label_col),
            Cell::new(source_col),
            Cell::new(row.files),
            Cell::new(row.size),
            Cell::new(row.added),
        ]);
    }
    println!("{table}");

    // Repeat under the table: on a long listing the pre-table warning has
    // usually scrolled out of view by the time the user reads the last row.
    if let Some(ref msg) = hidden {
        eprintln!("warning: {msg}");
    }

    Ok(())
}
