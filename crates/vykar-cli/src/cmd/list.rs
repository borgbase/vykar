use chrono::Local;
use comfy_table::{Cell, CellAlignment};

use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::format::{format_bytes, format_count};
use crate::passphrase::with_repo_passphrase;
use crate::table::CliTableTheme;

pub(crate) fn run_list(
    config: &VykarConfig,
    label: Option<&str>,
    source_filter: &[String],
    last: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut snapshots = with_repo_passphrase(config, label, |passphrase| {
        commands::list::list_snapshots_with_stats(config, passphrase)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

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
    let mut table =
        theme.new_data_table(&["ID", "Date", "Host", "Label", "Source", "Files", "Size"]);

    // Right-align Files and Size columns (indices 5 and 6)
    let col = table.column_mut(5).expect("Files column");
    col.set_cell_alignment(CellAlignment::Right);
    let col = table.column_mut(6).expect("Size column");
    col.set_cell_alignment(CellAlignment::Right);

    let mut prev_group: Option<(String, String)> = None;

    for (entry, stats) in &snapshots {
        let effective_label = if !entry.label.is_empty() {
            entry.label.clone()
        } else if !entry.source_label.is_empty() {
            entry.source_label.clone()
        } else {
            String::new()
        };

        let current_group = (entry.hostname.clone(), effective_label.clone());

        let (host_col, label_col) = if prev_group.as_ref() == Some(&current_group) {
            (String::new(), String::new())
        } else {
            let host = if entry.hostname.is_empty() {
                "-".to_string()
            } else {
                entry.hostname.clone()
            };
            let label = if effective_label.is_empty() {
                "-".to_string()
            } else {
                effective_label.clone()
            };
            (host, label)
        };

        prev_group = Some(current_group);

        let source_col = if !entry.source_paths.is_empty() {
            entry.source_paths.join("\n")
        } else if !entry.source_label.is_empty() {
            entry.source_label.clone()
        } else {
            "-".to_string()
        };

        let date_col = entry
            .time
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M")
            .to_string();

        let (files_col, size_col) = match stats {
            Some(st) => (format_count(st.nfiles), format_bytes(st.deduplicated_size)),
            None => ("-".to_string(), "-".to_string()),
        };

        table.add_row(vec![
            Cell::new(&entry.name),
            Cell::new(date_col),
            Cell::new(host_col),
            Cell::new(label_col),
            Cell::new(source_col),
            Cell::new(files_col),
            Cell::new(size_col),
        ]);
    }
    println!("{table}");

    Ok(())
}
