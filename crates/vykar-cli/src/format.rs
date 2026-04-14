pub(crate) fn parse_size(s: &str) -> Result<u64, Box<dyn std::error::Error>> {
    vykar_common::display::parse_size(s).map_err(|e| e.into())
}

pub(crate) use vykar_common::display::{format_bytes, format_count};

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

pub(crate) fn format_size_with_savings(bytes: u64, reference: u64, label: &str) -> String {
    if reference == 0 {
        return format_bytes(bytes);
    }
    let pct = (1.0 - bytes as f64 / reference as f64) * 100.0;
    format!("{}  ({:.1}% {label})", format_bytes(bytes), pct)
}
