use std::io::IsTerminal;

use vykar_core::commands;
use vykar_core::commands::restore::{RestoreEvent, RestoreOptions};
use vykar_core::config::VykarConfig;
use vykar_core::repo::control::{CancelToken, EventSink, FnMutSink};
use vykar_types::error::VykarError;

use crate::error::{CliError, CliResult};
use crate::format::format_bytes;
use crate::progress::RestoreProgressRenderer;
use vykar_core::app::passphrase::RepoPassphrase;

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_restore(
    config: &VykarConfig,
    snapshot_name: String,
    dest: String,
    pattern: Option<String>,
    verify: bool,
    continue_on_error: bool,
    cancel: &CancelToken,
    passphrase: &mut RepoPassphrase<'_>,
) -> CliResult<bool> {
    let opts = RestoreOptions {
        xattrs_enabled: config.xattrs.enabled,
        verify_chunks: verify,
        continue_on_error,
    };
    let stats = passphrase
        .with(|passphrase| -> CliResult<_> {
            // Built after any passphrase prompt; only a terminal gets progress.
            let mut renderer = std::io::stderr()
                .is_terminal()
                .then(RestoreProgressRenderer::new);
            let result = {
                let sink = renderer
                    .as_mut()
                    .map(|r| FnMutSink::new(move |e| r.on_event(e)));
                commands::restore::run_with_options(
                    config,
                    passphrase,
                    &snapshot_name,
                    &dest,
                    pattern.as_deref(),
                    opts,
                    sink.as_ref().map(|s| s as &dyn EventSink<RestoreEvent>),
                    cancel,
                )
            };
            // On errors too, so a message never lands on a half-drawn line.
            if let Some(r) = renderer.as_mut() {
                r.finish();
            }
            Ok(result?)
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
