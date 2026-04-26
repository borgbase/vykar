use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::format::format_bytes;
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_restore(
    config: &VykarConfig,
    label: Option<&str>,
    snapshot_name: String,
    dest: String,
    pattern: Option<String>,
    verify: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let stats = with_repo_passphrase(config, label, |passphrase| {
        commands::restore::run(
            config,
            passphrase,
            &snapshot_name,
            &dest,
            pattern.as_deref(),
            config.xattrs.enabled,
            verify,
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    println!(
        "Restored: {} files, {} dirs, {} symlinks ({})",
        stats.files,
        stats.dirs,
        stats.symlinks,
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
