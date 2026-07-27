use crate::error::CliResult;

pub(crate) fn parse_size(s: &str) -> CliResult<u64> {
    Ok(vykar_common::display::parse_size(s)?)
}

pub(crate) use vykar_common::display::format_bytes;

pub(crate) fn print_backup_stats(stats: &vykar_core::snapshot::SnapshotStats) {
    if stats.errors > 0 {
        println!(
            "  Files: {}, Errors: {}, Original: {}, Compressed: {}, Deduplicated: {}",
            stats.nfiles,
            stats.errors,
            format_bytes(stats.original_size),
            format_bytes(stats.compressed_size),
            format_bytes(stats.deduplicated_size),
        );
    } else {
        println!(
            "  Files: {}, Original: {}, Compressed: {}, Deduplicated: {}",
            stats.nfiles,
            format_bytes(stats.original_size),
            format_bytes(stats.compressed_size),
            format_bytes(stats.deduplicated_size),
        );
    }
}

/// Report cloud-only (macOS `FileProvider`) content that could not be captured.
///
/// Printed after the stats line and deliberately kept out of the exit code:
/// on a machine with iCloud Drive this would otherwise make every run report
/// partial success forever. The directory line is blunt because each unlistable
/// directory omits an entire subtree, not a single file.
pub(crate) fn print_dataless_summary(files: u64, dirs: u64) {
    if files > 0 {
        println!("  Cloud-only files skipped (no prior backup): {files}");
    }
    if dirs > 0 {
        println!(
            "  Cloud-only directories that could not be listed: {dirs} \
             (their contents are absent from this snapshot)"
        );
    }
}

pub(crate) fn format_size_with_savings(bytes: u64, reference: u64, label: &str) -> String {
    if reference == 0 {
        return format_bytes(bytes);
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "human-readable savings percentage; exact byte precision is not required"
    )]
    let pct = (1.0 - bytes as f64 / reference as f64) * 100.0;
    format!("{}  ({:.1}% {label})", format_bytes(bytes), pct)
}
