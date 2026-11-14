use std::sync::atomic::AtomicBool;

use vykar_core::commands;
use vykar_core::commands::restore::RestoreOptions;
use vykar_core::config::VykarConfig;
use vykar_types::error::VykarError;

use crate::error::{CliError, CliResult};
use crate::format::format_bytes;
use crate::passphrase::with_repo_passphrase;

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_restore(
    config: &VykarConfig,
    label: Option<&str>,
    snapshot_name: String,
    dest: String,
    pattern: Option<String>,
    verify: bool,
    continue_on_error: bool,
    shutdown: Option<&AtomicBool>,
) -> CliResult<bool> {
    let opts = RestoreOptions {
        xattrs_enabled: config.xattrs.enabled,
        verify_chunks: verify,
        continue_on_error,
    };
    let stats = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::restore::run_with_options(
            config,
            passphrase,
            &snapshot_name,
            &dest,
            pattern.as_deref(),
            opts,
            shutdown,
        )?)
    })
    .map_err(|e| match e {
        // Repo-level hooks flatten the error to its message, so the hint has
        // to travel inside it.
        CliError::Vykar(e @ VykarError::RestoreSourceUnreadable(_)) => CliError::Msg(format!(
            "{e}\nhint: --continue-on-error restores the files that are intact"
        )),
        e => e,
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

    for d in &stats.damaged {
        eprintln!("damaged: {}: {}", d.path.display(), d.reason);
    }
    if stats.damaged_total > 0 {
        let unlisted = stats.damaged_total - stats.damaged.len() as u64;
        let more = if unlisted > 0 {
            format!(" ({unlisted} more not shown)")
        } else {
            String::new()
        };
        eprintln!(
            "{} damaged file(s) not restored{more}; run `vykar check --verify-data`",
            stats.damaged_total
        );
    }

    Ok(stats.damaged_total > 0)
}
