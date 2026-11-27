use vykar_core::commands;
use vykar_core::config::VykarConfig;
use vykar_core::repo::control::CancelToken;

use crate::error::CliResult;
use crate::format::{format_bytes, parse_size};
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_compact(
    config: &VykarConfig,
    label: Option<&str>,
    threshold: f64,
    max_repack_size: Option<String>,
    dry_run: bool,
    cancel: &CancelToken,
) -> CliResult<()> {
    let max_bytes = max_repack_size.map(|s| parse_size(&s)).transpose()?;

    let stats = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::compact::run(
            config, passphrase, threshold, max_bytes, dry_run, cancel,
        )?)
    })?;

    if dry_run {
        println!(
            "Dry run: {} packs total, {} would be repacked, {} would be deleted (empty)",
            stats.packs_total, stats.packs_repacked, stats.packs_deleted_empty,
        );
        println!(
            "  {} live blobs, {} would be freed",
            stats.blobs_live,
            format_bytes(stats.space_freed),
        );
        if stats.chunks_unreferenced > 0 {
            println!(
                "  {} unreferenced chunks ({}) would be removed from the index",
                stats.chunks_unreferenced,
                format_bytes(stats.bytes_unreferenced),
            );
        }
        print_young_unreferenced(&stats);
        print_repack_held(&stats);
        print_compact_warnings(&stats);
    } else {
        print_compact_summary(&stats);
    }

    Ok(())
}

/// `vykar compact --report-unreferenced`: the counts of a mark, on stdout.
pub(crate) fn run_report_unreferenced(
    config: &VykarConfig,
    label: Option<&str>,
    cancel: &CancelToken,
) -> CliResult<()> {
    let report = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::compact::report_unreferenced(
            config, passphrase, cancel,
        )?)
    })?;
    println!(
        "Snapshots: {} ({} distinct tree chunks, {} stored)",
        report.snapshots,
        report.tree_chunks,
        format_bytes(report.tree_stored_bytes),
    );
    println!(
        "Index entries: {} ({} referenced, {} unreferenced, {})",
        report.entries,
        report.referenced,
        report.unreferenced.len(),
        format_bytes(report.unreferenced_bytes),
    );
    println!(
        "Packs with unreferenced entries: {} ({} hold nothing referenced, {})",
        report.packs_with_unreferenced,
        report.packs_only_unreferenced,
        format_bytes(report.packs_only_unreferenced_bytes),
    );
    Ok(())
}

/// The completion line plus any pack warnings, shared with the full-cycle
/// summary in `dispatch`.
pub(crate) fn print_compact_summary(stats: &commands::compact::CompactStats) {
    println!(
        "Compaction complete: {} packs repacked, {} empty packs deleted, {} freed",
        stats.packs_repacked,
        stats.packs_deleted_empty,
        format_bytes(stats.space_freed),
    );
    if stats.chunks_unreferenced > 0 {
        println!(
            "  {} unreferenced chunks ({}) removed from the index",
            stats.chunks_unreferenced,
            format_bytes(stats.bytes_unreferenced),
        );
    }
    if stats.packs_abandoned > 0 {
        println!(
            "  {} repacked pack(s) abandoned: another compact running at the same time repacked \
             the same data first; they are deleted by a compact once {} h old",
            stats.packs_abandoned,
            commands::compact::ORPHAN_PACK_GRACE.as_secs() / 3600,
        );
    }
    print_young_unreferenced(stats);
    print_repack_held(stats);
    print_sweep_skipped(stats);
    print_compact_warnings(stats);
}

/// Packs a compact did not repack under its source rule (04 step
/// 9), on stdout: a later compact repacks them.
fn print_repack_held(stats: &commands::compact::CompactStats) {
    if stats.packs_repack_held > 0 {
        println!(
            "  {} pack(s) with unused space not repacked this run: younger than {} h, of unknown \
             age, or newer than the start of a backup that is still running; a later compact \
             repacks them",
            stats.packs_repack_held,
            commands::compact::ORPHAN_PACK_GRACE.as_secs() / 3600,
        );
    }
}

/// Why a compact that ran beside committing backups removed no index
/// entry (04 step 9), on stdout: the next compact sweeps.
fn print_sweep_skipped(stats: &commands::compact::CompactStats) {
    use commands::compact::SweepSkipped;
    match stats.sweep_skipped {
        Some(SweepSkipped::Committing) => println!(
            "  No unreferenced chunks removed this run: a backup was committing; a later \
             compact removes them"
        ),
        Some(SweepSkipped::Contention) => println!(
            "  No unreferenced chunks removed this run: backups committed during each of {} \
             marks; a later compact removes them",
            commands::compact::COMPACT_MARK_ATTEMPTS,
        ),
        None => {}
    }
}

/// Unreferenced chunks the sweep kept because recent backups added them
/// (04 step 7), on stdout.
fn print_young_unreferenced(stats: &commands::compact::CompactStats) {
    if stats.chunks_unreferenced_young > 0 {
        println!(
            "  {} unreferenced chunks ({}) kept: added by backups in the last {} h; a later \
             compact removes them",
            stats.chunks_unreferenced_young,
            format_bytes(stats.bytes_unreferenced_young),
            commands::compact::ORPHAN_PACK_GRACE.as_secs() / 3600,
        );
    }
}

/// Corrupt/orphan pack notices. Reported after a dry run too — the scan that
/// finds them happens either way.
fn print_compact_warnings(stats: &commands::compact::CompactStats) {
    if stats.packs_corrupt > 0 {
        eprintln!(
            "  Warning: {} corrupt pack(s) found; run `vykar check --verify-data` for details",
            stats.packs_corrupt,
        );
    }
    if stats.packs_orphan > 0 {
        eprintln!(
            "  {} orphan pack(s) (present on disk but not in index)",
            stats.packs_orphan,
        );
        if stats.packs_orphan_young > 0 {
            eprintln!(
                "  {} of them kept: younger than {} h (uploads not yet committed, or emptied by this run's sweep)",
                stats.packs_orphan_young,
                commands::compact::ORPHAN_PACK_GRACE.as_secs() / 3600,
            );
        }
        if stats.packs_orphan_unknown_age > 0 {
            eprintln!(
                "  {} of them kept: age unknown (the storage reports no modification times)",
                stats.packs_orphan_unknown_age,
            );
        }
        if stats.packs_orphan_live > 0 {
            eprintln!(
                "  {} of them kept: they may belong to a backup that is still running",
                stats.packs_orphan_live,
            );
        }
    }
    if stats.temp_files_removed > 0 {
        eprintln!(
            "  Removed {} stale temp file(s) from interrupted uploads",
            stats.temp_files_removed,
        );
    }
}
