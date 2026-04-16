use std::sync::atomic::AtomicBool;

use chrono::Local;

use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::cli::{SnapshotCommand, SortField};
use crate::format::{format_bytes, parse_size};
use crate::passphrase::with_repo_passphrase;
use crate::table::{add_kv_row, CliTableTheme};

fn normalize_path_filter(raw: &str) -> String {
    let s = raw.strip_prefix("./").unwrap_or(raw);
    s.trim_end_matches('/').to_string()
}

fn path_matches_filter(item_path: &str, filter: &str) -> bool {
    item_path == filter || item_path.starts_with(&format!("{filter}/"))
}

pub(crate) fn run_snapshot_command(
    command: &SnapshotCommand,
    config: &VykarConfig,
    label: Option<&str>,
    shutdown: Option<&AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        SnapshotCommand::List {
            snapshot,
            path,
            long,
            sort,
            ..
        } => {
            let mut items = with_repo_passphrase(config, label, |passphrase| {
                commands::list::list_snapshot_items(config, passphrase, snapshot)
                    .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
            })?;

            // Apply path filter (empty filter after normalization means "all items")
            if let Some(ref raw_path) = path {
                let filter = normalize_path_filter(raw_path);
                if !filter.is_empty() {
                    items.retain(|item| path_matches_filter(&item.path, &filter));
                }
            }

            // Apply sort
            match sort {
                SortField::Name => items.sort_by(|a, b| a.path.cmp(&b.path)),
                SortField::Size => items.sort_by(|a, b| b.size.cmp(&a.size)),
                SortField::Mtime => items.sort_by(|a, b| b.mtime.cmp(&a.mtime)),
            }

            if *long {
                for item in &items {
                    let type_char = match item.entry_type {
                        vykar_core::snapshot::item::ItemType::Directory => "d",
                        vykar_core::snapshot::item::ItemType::RegularFile => "-",
                        vykar_core::snapshot::item::ItemType::Symlink => "l",
                    };
                    let secs = item.mtime / 1_000_000_000;
                    let nsecs = (item.mtime % 1_000_000_000) as u32;
                    let mtime = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs)
                        .map(|dt| {
                            dt.with_timezone(&Local)
                                .format("%Y-%m-%d %H:%M:%S")
                                .to_string()
                        })
                        .unwrap_or_else(|| "-".to_string());
                    println!(
                        "{}{:04o} {:>10} {} {}",
                        type_char,
                        item.mode & 0o7777,
                        format_bytes(item.size),
                        mtime,
                        item.path,
                    );
                }
            } else {
                for item in &items {
                    println!("{}", item.path);
                }
            }
            Ok(())
        }
        SnapshotCommand::Delete {
            snapshots, dry_run, ..
        } => super::delete::run_delete(config, label, snapshots, *dry_run, shutdown),
        SnapshotCommand::Find {
            path,
            source,
            last,
            name,
            iname,
            entry_type,
            since,
            larger,
            smaller,
            ..
        } => run_snapshot_find(
            config, label, path, source, last, name, iname, entry_type, since, larger, smaller,
        ),
        SnapshotCommand::Info { snapshot, .. } => {
            let meta = with_repo_passphrase(config, label, |passphrase| {
                commands::list::get_snapshot_meta(config, passphrase, snapshot)
                    .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
            })?;

            let theme = CliTableTheme::detect();

            // Group 1: snapshot metadata
            let mut t1 = theme.new_kv_table();
            add_kv_row(&mut t1, theme, "Name", &meta.name);
            add_kv_row(&mut t1, theme, "Hostname", &meta.hostname);
            add_kv_row(&mut t1, theme, "Username", &meta.username);
            add_kv_row(
                &mut t1,
                theme,
                "Start time",
                meta.time.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S"),
            );
            add_kv_row(
                &mut t1,
                theme,
                "End time",
                meta.time_end
                    .with_timezone(&Local)
                    .format("%Y-%m-%d %H:%M:%S"),
            );
            let duration = meta.time_end.signed_duration_since(meta.time);
            let secs = duration.num_seconds();
            let duration_str = if secs >= 60 {
                format!("{}m {:02}s", secs / 60, secs % 60)
            } else {
                format!("{secs}s")
            };
            add_kv_row(&mut t1, theme, "Duration", duration_str);
            let effective_label = if meta.label.is_empty() {
                &meta.source_label
            } else {
                &meta.label
            };
            add_kv_row(&mut t1, theme, "Label", effective_label);
            add_kv_row(&mut t1, theme, "Source paths", meta.source_paths.join(", "));
            if !meta.comment.is_empty() {
                add_kv_row(&mut t1, theme, "Comment", &meta.comment);
            }
            println!("{t1}");
            println!();

            // Group 2: statistics
            let mut t2 = theme.new_kv_table();
            add_kv_row(&mut t2, theme, "Files", meta.stats.nfiles);
            add_kv_row(
                &mut t2,
                theme,
                "Original size",
                format_bytes(meta.stats.original_size),
            );
            add_kv_row(
                &mut t2,
                theme,
                "Compressed",
                format_bytes(meta.stats.compressed_size),
            );
            add_kv_row(
                &mut t2,
                theme,
                "Deduplicated",
                format_bytes(meta.stats.deduplicated_size),
            );
            println!("{t2}");
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_snapshot_find(
    config: &VykarConfig,
    label: Option<&str>,
    path: &Option<String>,
    source: &Option<String>,
    last: &Option<u64>,
    name: &Option<String>,
    iname: &Option<String>,
    entry_type: &Option<String>,
    since: &Option<String>,
    larger: &Option<String>,
    smaller: &Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    use commands::find::{FileStatus, FindFilter, FindScope};
    use vykar_core::snapshot::item::ItemType;

    let scope = FindScope {
        source_label: source.clone(),
        last_n: last.map(|n| n as usize),
    };

    let path_prefix = path.as_deref().map(normalize_path_filter);

    let item_type: Option<ItemType> = entry_type
        .as_deref()
        .map(|t| match t {
            "f" => Ok(ItemType::RegularFile),
            "d" => Ok(ItemType::Directory),
            "l" => Ok(ItemType::Symlink),
            _ => Err(format!("unknown --type '{t}': use f, d, or l")),
        })
        .transpose()
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let since_dt = since
        .as_deref()
        .map(|s| -> Result<_, Box<dyn std::error::Error>> {
            if !s.trim().ends_with(|c: char| c.is_ascii_alphabetic()) {
                return Err(
                    format!("--since requires a unit suffix (h, d, w, m, y), got '{s}'").into(),
                );
            }
            let dur = vykar_core::prune::parse_timespan(s)?;
            if dur <= chrono::Duration::zero() {
                return Err(format!("--since duration must be positive (got '{s}')").into());
            }
            Ok(chrono::Utc::now() - dur)
        })
        .transpose()?;

    let larger_than = larger.as_deref().map(|s| parse_size(s)).transpose()?;

    let smaller_than = smaller.as_deref().map(|s| parse_size(s)).transpose()?;

    let filter = FindFilter::build(
        path_prefix,
        name.as_deref(),
        iname.as_deref(),
        item_type,
        since_dt,
        larger_than,
        smaller_than,
    )
    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let timelines = with_repo_passphrase(config, label, |passphrase| {
        commands::find::run(config, passphrase, &scope, &filter)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    if timelines.is_empty() {
        println!("No matching files found.");
        return Ok(());
    }

    for timeline in &timelines {
        println!("{}", timeline.path);
        println!("  {:<12} {:<20} {:>10}  Status", "Snapshot", "Date", "Size");
        for ah in &timeline.hits {
            let date = ah
                .hit
                .snapshot_time
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();
            let size = format_bytes(ah.hit.size);
            let status = match ah.status {
                FileStatus::Added => "added",
                FileStatus::Modified => "modified",
                FileStatus::Unchanged => "unchanged",
            };
            println!(
                "  {:<12} {:<20} {:>10}  {}",
                ah.hit.snapshot_name, date, size, status
            );
        }
        println!();
    }

    Ok(())
}
