use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::error::CliResult;
use crate::format::format_bytes;
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_restore(
    config: &VykarConfig,
    label: Option<&str>,
    snapshot_name: String,
    dest: String,
    pattern: Option<String>,
    verify: bool,
) -> CliResult<()> {
    let stats = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::restore::run(
            config,
            passphrase,
            &snapshot_name,
            &dest,
            pattern.as_deref(),
            config.xattrs.enabled,
            verify,
        )?)
    })?;

    // Hard links are only mentioned when present, keeping the common-case
    // summary unchanged.
    let hardlinks = if stats.hardlinks > 0 {
        format!(", {} hard links", stats.hardlinks)
    } else {
        String::new()
    };
    println!(
        "Restored: {} files, {} dirs, {} symlinks{} ({})",
        stats.files,
        stats.dirs,
        stats.symlinks,
        hardlinks,
        format_bytes(stats.total_bytes),
    );

    for w in &stats.warnings {
        eprintln!("warning: {w}");
    }
    if stats.warnings_suppressed > 0 {
        eprintln!(
            "warning: {} additional metadata warnings suppressed",
            stats.warnings_suppressed
        );
    }

    Ok(())
}
