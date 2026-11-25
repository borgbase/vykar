use std::cmp::Reverse;

use chrono::{DateTime, Local, Utc};
use comfy_table::{Cell, CellAlignment};
use serde::Serialize;

use vykar_core::app::views::SnapshotRowView;
use vykar_core::commands;
use vykar_core::commands::diff::{DiffChangeKind, DiffResult};
use vykar_core::commands::find::{FileStatus, PathTimeline};
use vykar_core::config::VykarConfig;
use vykar_core::repo::control::CancelToken;
use vykar_core::snapshot::SnapshotMeta;
use vykar_core::snapshot::item::Item;

use crate::cli::{SnapshotCommand, SortField};
use crate::error::{CliError, CliResult};
use crate::format::{format_bytes, parse_size};
use crate::output::{Output, Report, SnapshotStatsJson, item_type_name, non_empty, utc_from_nanos};
use crate::passphrase::with_repo_passphrase;
use crate::table::{CliTableTheme, add_kv_row};
use vykar_common::display::{format_duration_seconds, format_optional_size, format_size_delta};

fn normalize_path_filter(raw: &str) -> String {
    let s = raw.trim_prefix("./");
    s.trim_end_matches('/').to_string()
}

pub(crate) fn run_snapshot_command(
    command: &SnapshotCommand,
    config: &VykarConfig,
    label: Option<&str>,
    shutdown: &CancelToken,
    out: &mut Output,
) -> CliResult<()> {
    match command {
        SnapshotCommand::List {
            snapshot,
            path,
            long,
            sort,
            ..
        } => {
            // Path filter (empty after normalization means "all items"):
            // only that subtree is read where the snapshot allows it, and
            // only matching items are kept.
            let filter = path
                .as_deref()
                .map(normalize_path_filter)
                .filter(|f| !f.is_empty());
            let mut items = Vec::new();
            with_repo_passphrase(config, label, |passphrase| {
                let push = |item| {
                    items.push(item);
                    Ok(())
                };
                Ok(match filter.as_deref() {
                    Some(f) => commands::list::for_each_listed_item_under(
                        config, passphrase, snapshot, f, push,
                    )?,
                    None => {
                        commands::list::for_each_listed_item(config, passphrase, snapshot, push)?
                    }
                })
            })?;

            // Apply sort
            match sort {
                SortField::Name => items.sort_by(|a, b| a.path.cmp(&b.path)),
                SortField::Size => items.sort_by_key(|a| Reverse(a.size)),
                SortField::Mtime => items.sort_by_key(|a| Reverse(a.mtime)),
            }

            out.emit(&ItemsReport { items, long: *long })
        }
        SnapshotCommand::Delete {
            snapshots, dry_run, ..
        } => super::delete::run_delete(config, label, snapshots, *dry_run, shutdown, out),
        SnapshotCommand::Diff {
            snapshot_a,
            snapshot_b,
        } => run_snapshot_diff(config, label, snapshot_a, snapshot_b, out),
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
            config, label, path, source, last, name, iname, entry_type, since, larger, smaller, out,
        ),
        SnapshotCommand::Info { snapshot, .. } => {
            let meta = with_repo_passphrase(config, label, |passphrase| {
                Ok(commands::list::get_snapshot_meta(
                    config, passphrase, snapshot,
                )?)
            })?;

            out.emit(&MetaReport(meta))
        }
    }
}

struct ItemsReport {
    items: Vec<Item>,
    long: bool,
}

#[derive(Serialize)]
struct ItemJson<'a> {
    path: &'a str,
    #[serde(rename = "type")]
    entry_type: &'static str,
    /// Permission bits.
    mode: u32,
    size: u64,
    mtime: Option<DateTime<Utc>>,
    link_target: Option<&'a str>,
}

impl Report for ItemsReport {
    const KEY: &'static str = "snapshot_list";

    fn json(&self) -> impl Serialize {
        self.items
            .iter()
            .map(|item| ItemJson {
                path: &item.path,
                entry_type: item_type_name(item.entry_type),
                mode: item.mode & 0o7777,
                size: item.size,
                mtime: utc_from_nanos(item.mtime),
                link_target: item.link_target.as_deref(),
            })
            .collect::<Vec<_>>()
    }

    fn print_human(&self) {
        if self.long {
            for item in &self.items {
                let type_char = match item.entry_type {
                    vykar_core::snapshot::item::ItemType::Directory => "d",
                    vykar_core::snapshot::item::ItemType::RegularFile => "-",
                    vykar_core::snapshot::item::ItemType::Symlink => "l",
                };
                let mtime = utc_from_nanos(item.mtime)
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
            for item in &self.items {
                println!("{}", item.path);
            }
        }
    }
}

struct MetaReport(SnapshotMeta);

#[derive(Serialize)]
struct MetaJson<'a> {
    name: &'a str,
    hostname: Option<&'a str>,
    username: Option<&'a str>,
    time: DateTime<Utc>,
    time_end: DateTime<Utc>,
    duration_secs: i64,
    label: Option<&'a str>,
    source_paths: &'a [String],
    comment: Option<&'a str>,
    stats: SnapshotStatsJson,
}

impl Report for MetaReport {
    const KEY: &'static str = "snapshot_info";

    fn json(&self) -> impl Serialize {
        let meta = &self.0;
        MetaJson {
            name: &meta.name,
            hostname: non_empty(&meta.hostname),
            username: non_empty(&meta.username),
            time: meta.time,
            time_end: meta.time_end,
            duration_secs: meta.time_end.signed_duration_since(meta.time).num_seconds(),
            label: non_empty(SnapshotRowView::effective_label_parts(
                &meta.source_label,
                &meta.label,
            )),
            source_paths: &meta.source_paths,
            comment: non_empty(&meta.comment),
            stats: SnapshotStatsJson::from(&meta.stats),
        }
    }

    fn print_human(&self) {
        let meta = &self.0;
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
        add_kv_row(
            &mut t1,
            theme,
            "Duration",
            format_duration_seconds(duration.num_seconds()),
        );
        let effective_label =
            SnapshotRowView::effective_label_parts(&meta.source_label, &meta.label);
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
    }
}

fn run_snapshot_diff(
    config: &VykarConfig,
    label: Option<&str>,
    snapshot_a: &str,
    snapshot_b: &str,
    out: &mut Output,
) -> CliResult<()> {
    let diff = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::diff::run(
            config, passphrase, snapshot_a, snapshot_b,
        )?)
    })?;
    out.emit(&DiffReport(diff))
}

struct DiffReport(DiffResult);

#[derive(Serialize)]
struct DiffJson<'a> {
    base_snapshot: &'a str,
    target_snapshot: &'a str,
    entries: Vec<DiffEntryJson<'a>>,
}

#[derive(Serialize)]
struct DiffEntryJson<'a> {
    path: &'a str,
    change: &'static str,
    old_size: Option<u64>,
    new_size: Option<u64>,
    size_delta: i64,
}

fn change_name(change: DiffChangeKind) -> &'static str {
    match change {
        DiffChangeKind::Added => "added",
        DiffChangeKind::Removed => "removed",
        DiffChangeKind::Modified => "modified",
    }
}

impl Report for DiffReport {
    const KEY: &'static str = "snapshot_diff";

    fn json(&self) -> impl Serialize {
        let diff = &self.0;
        DiffJson {
            base_snapshot: &diff.base_snapshot,
            target_snapshot: &diff.target_snapshot,
            entries: diff
                .entries
                .iter()
                .map(|entry| DiffEntryJson {
                    path: &entry.path,
                    change: change_name(entry.change),
                    old_size: entry.old_size,
                    new_size: entry.new_size,
                    size_delta: entry.size_delta,
                })
                .collect(),
        }
    }

    fn print_human(&self) {
        print_diff(&self.0);
    }
}

fn print_diff(diff: &DiffResult) {
    if diff.entries.is_empty() {
        println!("No file changes found.");
        return;
    }

    let theme = CliTableTheme::detect();
    let mut table = theme.new_data_table(&["Change", "Path", "Old Size", "New Size", "Delta"]);
    for col_idx in [2, 3, 4] {
        let col = table.column_mut(col_idx).expect("size column");
        col.set_cell_alignment(CellAlignment::Right);
    }

    for entry in &diff.entries {
        let change = match entry.change {
            DiffChangeKind::Added => "Added",
            DiffChangeKind::Removed => "Removed",
            DiffChangeKind::Modified => "Modified",
        };
        table.add_row(vec![
            Cell::new(change),
            Cell::new(&entry.path),
            Cell::new(format_optional_size(entry.old_size)),
            Cell::new(format_optional_size(entry.new_size)),
            Cell::new(format_size_delta(entry.size_delta)),
        ]);
    }

    println!("{table}");
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
    out: &mut Output,
) -> CliResult<()> {
    use commands::find::{FindFilter, FindScope};
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
            _ => Err(CliError::from(format!(
                "unknown --type '{t}': use f, d, or l"
            ))),
        })
        .transpose()?;

    let since_dt = since
        .as_deref()
        .map(|s| -> CliResult<_> {
            if !s.trim().ends_with(|c: char| c.is_ascii_alphabetic()) {
                return Err(CliError::from(format!(
                    "--since requires a unit suffix (h, d, w, m, y), got '{s}'"
                )));
            }
            let dur = vykar_core::prune::parse_timespan(s)?;
            if dur <= chrono::Duration::zero() {
                return Err(CliError::from(format!(
                    "--since duration must be positive (got '{s}')"
                )));
            }
            Ok(chrono::Utc::now() - dur)
        })
        .transpose()?;

    let larger_than = larger.as_deref().map(parse_size).transpose()?;

    let smaller_than = smaller.as_deref().map(parse_size).transpose()?;

    let filter = FindFilter::build(
        path_prefix,
        name.as_deref(),
        iname.as_deref(),
        item_type,
        since_dt,
        larger_than,
        smaller_than,
    )?;

    let timelines = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::find::run(config, passphrase, &scope, &filter)?)
    })?;

    out.emit(&FindReport(timelines))
}

struct FindReport(Vec<PathTimeline>);

#[derive(Serialize)]
struct TimelineJson<'a> {
    path: &'a str,
    hits: Vec<HitJson<'a>>,
}

#[derive(Serialize)]
struct HitJson<'a> {
    snapshot: &'a str,
    snapshot_time: DateTime<Utc>,
    #[serde(rename = "type")]
    entry_type: &'static str,
    size: u64,
    mtime: Option<DateTime<Utc>>,
    status: &'static str,
}

fn file_status_name(status: FileStatus) -> &'static str {
    match status {
        FileStatus::Added => "added",
        FileStatus::Modified => "modified",
        FileStatus::Unchanged => "unchanged",
    }
}

impl Report for FindReport {
    const KEY: &'static str = "snapshot_find";

    fn json(&self) -> impl Serialize {
        self.0
            .iter()
            .map(|timeline| TimelineJson {
                path: &timeline.path,
                hits: timeline
                    .hits
                    .iter()
                    .map(|ah| HitJson {
                        snapshot: &ah.hit.snapshot_name,
                        snapshot_time: ah.hit.snapshot_time,
                        entry_type: item_type_name(ah.hit.entry_type),
                        size: ah.hit.size,
                        mtime: utc_from_nanos(ah.hit.mtime),
                        status: file_status_name(ah.status),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>()
    }

    fn print_human(&self) {
        print_find(&self.0);
    }
}

fn print_find(timelines: &[PathTimeline]) {
    if timelines.is_empty() {
        println!("No matching files found.");
        return;
    }

    for timeline in timelines {
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
            let status = file_status_name(ah.status);
            println!(
                "  {:<12} {:<20} {:>10}  {}",
                ah.hit.snapshot_name, date, size, status
            );
        }
        println!();
    }
}
