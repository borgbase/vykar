use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::format::{format_size_with_bytes, format_size_with_savings};
use crate::passphrase::with_repo_passphrase;
use crate::table::{add_kv_row, CliTableTheme};

pub(crate) fn run_info(
    config: &VykarConfig,
    label: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let stats = with_repo_passphrase(config, label, |passphrase| {
        commands::info::run(config, passphrase)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    let theme = CliTableTheme::detect();

    // Group 1: repository metadata
    let mut t1 = theme.new_kv_table();
    let repo_name = label.unwrap_or(&config.repository.url);
    add_kv_row(&mut t1, theme, "Repository", repo_name);
    add_kv_row(&mut t1, theme, "URL", config.repository.url.clone());
    add_kv_row(&mut t1, theme, "Encryption", stats.encryption.as_str());
    add_kv_row(
        &mut t1,
        theme,
        "Created",
        stats.repo_created.format("%Y-%m-%d %H:%M:%S UTC"),
    );
    println!("{t1}");
    println!();

    // Group 2: statistics
    let mut t2 = theme.new_kv_table();
    add_kv_row(&mut t2, theme, "Snapshots", stats.snapshot_count);
    let last_snapshot = stats
        .last_snapshot_time
        .map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "-".to_string());
    add_kv_row(&mut t2, theme, "Last snapshot", last_snapshot);
    add_kv_row(
        &mut t2,
        theme,
        "Raw size",
        format_size_with_bytes(stats.raw_size),
    );
    add_kv_row(
        &mut t2,
        theme,
        "Compressed",
        format_size_with_savings(stats.compressed_size, stats.raw_size, "ratio"),
    );
    add_kv_row(
        &mut t2,
        theme,
        "Deduplicated",
        format_size_with_savings(stats.deduplicated_size, stats.raw_size, "savings"),
    );
    add_kv_row(
        &mut t2,
        theme,
        "Unique stored",
        format_size_with_bytes(stats.unique_stored_size),
    );
    add_kv_row(
        &mut t2,
        theme,
        "Referenced",
        format_size_with_bytes(stats.referenced_stored_size),
    );
    add_kv_row(&mut t2, theme, "Unique chunks", stats.unique_chunks);
    println!("{t2}");
    Ok(())
}
