use comfy_table::Cell;

use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::passphrase::with_repo_passphrase;
use crate::table::CliTableTheme;

pub(crate) fn run_list(
    config: &VykarConfig,
    label: Option<&str>,
    source_filter: &[String],
    last: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut snapshots = with_repo_passphrase(config, label, |passphrase| {
        commands::list::list_snapshots(config, passphrase)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    // Filter by source label if requested
    if !source_filter.is_empty() {
        snapshots.retain(|e| source_filter.iter().any(|f| f == &e.source_label));
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
    let mut table = theme.new_data_table(&["ID", "Host", "Source", "Label", "Date"]);

    for entry in &snapshots {
        let host_col = if entry.hostname.is_empty() {
            "-".to_string()
        } else {
            entry.hostname.clone()
        };
        let source_col = if !entry.source_paths.is_empty() {
            entry.source_paths.join("\n")
        } else if !entry.source_label.is_empty() {
            entry.source_label.clone()
        } else {
            "-".to_string()
        };
        let label_col = if !entry.label.is_empty() {
            entry.label.clone()
        } else if !entry.source_label.is_empty() {
            entry.source_label.clone()
        } else {
            "-".to_string()
        };
        table.add_row(vec![
            Cell::new(entry.name.clone()),
            Cell::new(host_col),
            Cell::new(source_col),
            Cell::new(label_col),
            Cell::new(entry.time.format("%Y-%m-%d %H:%M:%S").to_string()),
        ]);
    }
    println!("{table}");

    Ok(())
}
