use chrono::{DateTime, Local, Utc};
use serde::Serialize;

use vykar_core::commands;
use vykar_core::commands::info::InfoStats;
use vykar_core::config::VykarConfig;

use crate::error::CliResult;
use crate::format::{format_bytes, format_size_with_savings};
use crate::output::{Output, Report};
use crate::passphrase::with_repo_passphrase;
use crate::table::{CliTableTheme, add_kv_row};

pub(crate) fn run_info(
    config: &VykarConfig,
    label: Option<&str>,
    out: &mut Output,
) -> CliResult<()> {
    let stats = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::info::run(config, passphrase)?)
    })?;
    out.emit(&InfoReport {
        repo_name: label.unwrap_or(&config.repository.url),
        url: &config.repository.url,
        stats,
    })
}

struct InfoReport<'a> {
    repo_name: &'a str,
    url: &'a str,
    stats: InfoStats,
}

#[derive(Serialize)]
struct InfoJson<'a> {
    url: &'a str,
    encryption: &'static str,
    format_version: u32,
    chunk_hash: &'static str,
    index: Option<IndexJson>,
    created: DateTime<Utc>,
    snapshot_count: usize,
    last_snapshot_time: Option<DateTime<Utc>>,
    raw_size: u64,
    compressed_size: u64,
    deduplicated_size: u64,
    unique_stored_size: u64,
    referenced_stored_size: Option<u64>,
    last_sweep_time: Option<DateTime<Utc>>,
    unique_chunks: usize,
}

#[derive(Serialize)]
struct IndexJson {
    base_bytes: u64,
    segments: usize,
    segment_bytes: u64,
}

impl Report for InfoReport<'_> {
    const KEY: &'static str = "info";

    fn json(&self) -> impl Serialize {
        let stats = &self.stats;
        InfoJson {
            url: self.url,
            encryption: stats.encryption.as_str(),
            format_version: stats.format.version(),
            chunk_hash: stats.format.chunk_hash().as_str(),
            index: stats.index.map(|index| IndexJson {
                base_bytes: index.base_bytes,
                segments: index.segments,
                segment_bytes: index.segment_bytes,
            }),
            created: stats.repo_created,
            snapshot_count: stats.snapshot_count,
            last_snapshot_time: stats.last_snapshot_time,
            raw_size: stats.raw_size,
            compressed_size: stats.compressed_size,
            deduplicated_size: stats.deduplicated_size,
            unique_stored_size: stats.unique_stored_size,
            referenced_stored_size: stats.referenced_stored_size,
            last_sweep_time: stats.last_sweep_time,
            unique_chunks: stats.unique_chunks,
        }
    }

    fn print_human(&self) {
        print_info(self.repo_name, self.url, &self.stats);
    }
}

fn print_info(repo_name: &str, url: &str, stats: &InfoStats) {
    let theme = CliTableTheme::detect();

    // Group 1: repository metadata
    let mut t1 = theme.new_kv_table();
    add_kv_row(&mut t1, theme, "Repository", repo_name);
    add_kv_row(&mut t1, theme, "URL", url);
    add_kv_row(&mut t1, theme, "Encryption", stats.encryption.as_str());
    add_kv_row(
        &mut t1,
        theme,
        "Format",
        format!("v{}", stats.format.version()),
    );
    add_kv_row(&mut t1, theme, "Hash", stats.format.chunk_hash().as_str());
    if let Some(ref index) = stats.index {
        add_kv_row(
            &mut t1,
            theme,
            "Index",
            format!(
                "{} base, {} segment{} ({})",
                format_bytes(index.base_bytes),
                index.segments,
                if index.segments == 1 { "" } else { "s" },
                format_bytes(index.segment_bytes),
            ),
        );
    }
    add_kv_row(
        &mut t1,
        theme,
        "Created",
        stats
            .repo_created
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S"),
    );
    println!("{t1}");
    println!();

    // Group 2: statistics
    let mut t2 = theme.new_kv_table();
    add_kv_row(&mut t2, theme, "Snapshots", stats.snapshot_count);
    let last_snapshot = stats
        .last_snapshot_time
        .map(|t| {
            t.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "-".to_string());
    add_kv_row(&mut t2, theme, "Last snapshot", last_snapshot);
    add_kv_row(&mut t2, theme, "Raw size", format_bytes(stats.raw_size));
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
        format_bytes(stats.unique_stored_size),
    );
    add_kv_row(
        &mut t2,
        theme,
        "Referenced",
        stats
            .referenced_stored_size
            .map_or_else(|| "-".to_string(), format_bytes),
    );
    let last_sweep = stats
        .last_sweep_time
        .map(|t| {
            t.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "never".to_string());
    add_kv_row(&mut t2, theme, "Last sweep", last_sweep);
    add_kv_row(&mut t2, theme, "Unique chunks", stats.unique_chunks);
    println!("{t2}");
}
