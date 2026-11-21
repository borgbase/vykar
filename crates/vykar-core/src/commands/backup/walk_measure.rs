//! Walk one configured source as a backup would and time it, for
//! `examples/walk_order.rs` (06 step 0). Public only with the `measure`
//! feature; not API.
//!
//! The walk thread's per-entry syscalls (stat, readlink, xattrs) run as in a
//! backup. File content is read only for `read_changed_since_ns`, and on this
//! thread in emission order, whereas a backup reads it on worker threads.

use std::time::Instant;

use super::walk::{EmitOrder, InodeSortedWalk, Materialized, WalkEvent, materialize_item};
use super::{BackupRequest, prepare};
use crate::compress::Compression;
use crate::config::{SourceEntry, VykarConfig};
use crate::measure::shape::{DirShape, EntryKind, ShapeReport};
use vykar_types::error::{Result, VykarError, is_soft_backup_io_error};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkOrder {
    Stat,
    Name,
}

impl From<WalkOrder> for EmitOrder {
    fn from(o: WalkOrder) -> Self {
        match o {
            WalkOrder::Stat => EmitOrder::Stat,
            WalkOrder::Name => EmitOrder::Name,
        }
    }
}

/// `Auto` asks the filesystem, as a backup does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeSort {
    Auto,
    On,
    Off,
}

#[derive(Debug, Clone)]
pub struct WalkMeasureOptions {
    pub order: WalkOrder,
    pub inode_sort: InodeSort,
    /// readlink and xattrs per entry, as the walk thread does.
    pub materialize: bool,
    /// Read the content of regular files with `mtime_ns` or `ctime_ns` at or
    /// after this time.
    pub read_changed_since_ns: Option<i64>,
    pub shape: bool,
}

impl Default for WalkMeasureOptions {
    fn default() -> Self {
        Self {
            order: WalkOrder::Stat,
            inode_sort: InodeSort::Auto,
            materialize: true,
            read_changed_since_ns: None,
            shape: true,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct WalkReport {
    pub source_label: String,
    pub paths: Vec<String>,
    /// `"stat"` or `"name"`.
    pub order: String,
    /// Per resolved path.
    pub inode_sort: Vec<bool>,
    /// After `prepare`'s platform check.
    pub xattrs_enabled: bool,
    pub wall_ms: u64,
    pub entries: u64,
    pub dirs: u64,
    pub files: u64,
    pub symlinks: u64,
    pub unsupported: u64,
    pub skipped: u64,
    pub skipped_dataless: u64,
    pub read_files: u64,
    pub read_bytes: u64,
    /// Hex BLAKE3 over each emitted snapshot path's bytes followed by `0x00`.
    pub order_digest: String,
    /// Bytes per directory are not tracked.
    pub shape: Option<ShapeReport>,
}

pub fn measure_walk(
    config: &VykarConfig,
    source: &SourceEntry,
    opts: &WalkMeasureOptions,
) -> Result<WalkReport> {
    // `prepare` reads none of snapshot_name, passphrase, compression, verbose.
    let req = BackupRequest {
        snapshot_name: "",
        passphrase: None,
        source_paths: &source.paths,
        source_label: &source.label,
        exclude_patterns: &source.exclude,
        exclude_if_present: &source.exclude_if_present,
        one_file_system: source.one_file_system,
        git_ignore: source.git_ignore,
        xattrs_enabled: source.xattrs_enabled,
        compression: Compression::None,
        command_dumps: &source.command_dumps,
        verbose: false,
    };
    let prepared = prepare(config, &req)?;

    let inode_override = match opts.inode_sort {
        InodeSort::Auto => None,
        InodeSort::On => Some(true),
        InodeSort::Off => Some(false),
    };

    let mut report = WalkReport {
        source_label: source.label.clone(),
        paths: source.paths.clone(),
        order: match opts.order {
            WalkOrder::Stat => "stat",
            WalkOrder::Name => "name",
        }
        .to_string(),
        inode_sort: Vec::new(),
        xattrs_enabled: prepared.xattrs_enabled,
        wall_ms: 0,
        entries: 0,
        dirs: 0,
        files: 0,
        symlinks: 0,
        unsupported: 0,
        skipped: 0,
        skipped_dataless: 0,
        read_files: 0,
        read_bytes: 0,
        order_digest: String::new(),
        shape: None,
    };
    let mut digest = blake3::Hasher::new();
    let mut shape = opts.shape.then(|| DirShape::new(false));

    let start = Instant::now();
    for src in &prepared.resolved_sources {
        let walk = InodeSortedWalk::new_inner(
            src,
            &prepared.exclude_patterns,
            &source.exclude_if_present,
            source.one_file_system,
            source.git_ignore,
            opts.order.into(),
            inode_override,
        )?;
        report.inode_sort.push(walk.inode_sort());

        for event in walk {
            let walked = match event? {
                WalkEvent::Entry(walked) => walked,
                WalkEvent::Skipped { .. } => {
                    report.skipped += 1;
                    continue;
                }
                WalkEvent::SkippedDataless { .. } => {
                    report.skipped_dataless += 1;
                    continue;
                }
            };

            let path_bytes = walked
                .snapshot_path_raw
                .as_deref()
                .unwrap_or(walked.snapshot_path.as_bytes());
            digest.update(path_bytes);
            digest.update(&[0]);
            let kind = if walked.file_type.is_dir() {
                report.dirs += 1;
                EntryKind::Dir
            } else if walked.file_type.is_symlink() {
                report.symlinks += 1;
                EntryKind::Symlink
            } else {
                report.files += 1;
                EntryKind::File
            };
            report.entries += 1;
            if let Some(s) = shape.as_mut() {
                s.push(path_bytes, kind, 0);
            }

            let meta = &walked.metadata;
            let read_path = opts
                .read_changed_since_ns
                .filter(|t| {
                    walked.file_type.is_file()
                        && meta.size > 0
                        && !meta.is_dataless
                        && (meta.mtime_ns >= *t || meta.ctime_ns >= *t)
                })
                .map(|_| walked.abs_path.clone());

            if opts.materialize {
                match materialize_item(walked, prepared.xattrs_enabled)? {
                    Materialized::Entry { .. } => {}
                    Materialized::SoftError { .. } => report.skipped += 1,
                    Materialized::Unsupported { .. } => report.unsupported += 1,
                }
            }

            if let Some(path) = read_path {
                match read_whole(&path) {
                    Ok(n) => {
                        report.read_files += 1;
                        report.read_bytes += n;
                    }
                    Err(e) if is_soft_backup_io_error(&e) => report.skipped += 1,
                    Err(e) => {
                        return Err(VykarError::Other(format!(
                            "read error for {}: {e}",
                            path.display()
                        )));
                    }
                }
            }
        }
    }
    report.wall_ms = start.elapsed().as_millis() as u64;
    report.order_digest = digest.finalize().to_hex().to_string();
    report.shape = shape.map(DirShape::finish);
    Ok(report)
}

fn read_whole(path: &std::path::Path) -> std::io::Result<u64> {
    let mut file = std::fs::File::open(path)?;
    std::io::copy(&mut file, &mut std::io::sink())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::tests::helpers::source_entry;

    #[test]
    fn measure_walk_digest_matches_across_modes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let mut nonempty = 0;
        for d in ["c", "a", "b"] {
            std::fs::create_dir(root.join(d)).unwrap();
            for f in (0..10).rev() {
                let content = if f % 3 == 0 { "" } else { "data" };
                std::fs::write(root.join(format!("{d}/f{f}")), content).unwrap();
                if !content.is_empty() {
                    nonempty += 1;
                }
            }
        }
        std::os::unix::fs::symlink("a", root.join("link")).unwrap();

        let config = VykarConfig::default();
        let source = source_entry(root, "src");
        let run = |order, inode_sort, read| {
            measure_walk(
                &config,
                &source,
                &WalkMeasureOptions {
                    order,
                    inode_sort,
                    read_changed_since_ns: read,
                    ..WalkMeasureOptions::default()
                },
            )
            .unwrap()
        };

        let name_on = run(WalkOrder::Name, InodeSort::On, None);
        let stat_off = run(WalkOrder::Stat, InodeSort::Off, None);
        assert_eq!(name_on.order_digest, stat_off.order_digest);
        assert_eq!(name_on.inode_sort, vec![true]);
        assert_eq!(stat_off.inode_sort, vec![false]);
        assert_eq!(
            (
                name_on.entries,
                name_on.dirs,
                name_on.files,
                name_on.symlinks
            ),
            (
                stat_off.entries,
                stat_off.dirs,
                stat_off.files,
                stat_off.symlinks
            )
        );
        assert_eq!((name_on.entries, name_on.symlinks), (34, 1));
        assert_eq!(name_on.shape, stat_off.shape);

        let read = run(WalkOrder::Stat, InodeSort::Auto, Some(0));
        assert_eq!(read.read_files, nonempty);
        assert_eq!(read.read_bytes, nonempty * 4);
    }
}
