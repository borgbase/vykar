use serde::Serialize;

use vykar_core::commands;
use vykar_core::commands::restore::{RestoreOptions, RestoreStats};
use vykar_core::config::VykarConfig;
use vykar_core::repo::control::CancelToken;
use vykar_types::error::VykarError;

use crate::error::{CliError, CliResult};
use crate::format::format_bytes;
use crate::output::{Output, Report};
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
    shutdown: &CancelToken,
    out: &mut Output,
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
        // A chunk removed by a concurrent prune and compact is not damage:
        // the message already says to run the command again.
        CliError::Vykar(e @ VykarError::RestoreSourceUnreadable(_))
            if !e.is_removed_during_read() =>
        {
            CliError::Msg(format!(
                "{e}\nhint: --continue-on-error restores the files that are intact"
            ))
        }
        e => e,
    })?;

    let partial = stats.damaged_total > 0;
    out.emit(&RestoreReport(stats))?;
    Ok(partial)
}

struct RestoreReport(RestoreStats);

#[derive(Serialize)]
struct RestoreJson<'a> {
    files: u64,
    dirs: u64,
    symlinks: u64,
    hardlinks: u64,
    hardlink_copies: u64,
    total_bytes: u64,
    warnings: &'a [String],
    warnings_suppressed: u64,
    /// Files left out because their data could not be read; capped, see
    /// `damaged_total` for the exact count.
    damaged: Vec<DamagedJson<'a>>,
    damaged_total: u64,
}

#[derive(Serialize)]
struct DamagedJson<'a> {
    path: std::borrow::Cow<'a, str>,
    reason: &'a str,
}

impl Report for RestoreReport {
    const KEY: &'static str = "restore";

    fn json(&self) -> impl Serialize {
        let stats = &self.0;
        RestoreJson {
            files: stats.files,
            dirs: stats.dirs,
            symlinks: stats.symlinks,
            hardlinks: stats.hardlinks,
            hardlink_copies: stats.hardlink_copies,
            total_bytes: stats.total_bytes,
            warnings: &stats.warnings,
            warnings_suppressed: stats.warnings_suppressed,
            damaged: stats
                .damaged
                .iter()
                .map(|d| DamagedJson {
                    path: d.path.to_string_lossy(),
                    reason: &d.reason,
                })
                .collect(),
            damaged_total: stats.damaged_total,
        }
    }

    fn print_human(&self) {
        print_restore(&self.0);
    }
}

fn print_restore(stats: &RestoreStats) {
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
}
