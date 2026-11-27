use std::io::{IsTerminal, Write};

use chrono::{DateTime, Utc};
use vykar_core::config::VykarConfig;
use vykar_core::repo::HostEnv;
use vykar_core::repo::sessions;

use crate::error::{CliError, CliResult};

/// `vykar clear-sessions` (until 10-single-protocol: `break-lock`): first
/// remove everything under `locks/` without asking (vykar 0.24 locks, the
/// legacy key of an interrupted v2/v3 upgrade, probe debris), then list every
/// session marker and remove them all after confirmation on a terminal.
/// Without a terminal it refuses (an error) whenever a marker exists, after
/// the lock files were removed.
///
/// `stale` (`--stale`): after the lock files, remove only the markers the
/// reapers would remove (`sessions::clear_stale_sessions`), without asking
/// and without a terminal, and succeed whatever live markers remain.
pub(crate) fn run_clear_sessions(
    config: &VykarConfig,
    _label: Option<&str>,
    stale: bool,
) -> CliResult<()> {
    let storage = vykar_core::storage::backend_from_config(
        &config
            .repository
            .storage_config(config.limits.connections)
            .with_cancel(crate::signal::shutdown_token().clone()),
    )?;

    let removed = sessions::clear_legacy_locks(storage.as_ref())?;
    if removed > 0 {
        println!(
            "Removed {removed} leftover lock file(s) (vykar 0.24, or an interrupted upgrade)."
        );
    }

    if stale {
        return clear_stale(storage.as_ref());
    }
    clear_sessions(storage.as_ref(), removed)
}

/// Remove the stale markers and report what was removed and what was left.
fn clear_stale(storage: &dyn vykar_storage::StorageBackend) -> CliResult<()> {
    let cleared = sessions::clear_stale_sessions(storage, &HostEnv::system())?;
    if !cleared.store_now_known {
        eprintln!(
            "Warning: the storage's clock could not be read; markers were judged stale only \
             when their process is provably gone on this machine."
        );
    }
    for id in &cleared.removed {
        println!("Removed stale session {id}.");
    }
    println!(
        "Removed {} stale session(s); {} live session(s) left.",
        cleared.removed.len(),
        cleared.live.len()
    );
    if !cleared.live.is_empty() {
        let now = Utc::now();
        eprintln!("Live session(s) kept:");
        for live in &cleared.live {
            print_session(&now, &live.session_id, live.entry.as_ref());
        }
    }
    Ok(())
}

/// One line of a session listing on stderr.
fn print_session(now: &DateTime<Utc>, id: &str, entry: Option<&sessions::SessionEntry>) {
    let exclusive = if sessions::is_exclusive_session(id) {
        " (exclusive: check --repair, upgrade or a pack-map write)"
    } else {
        ""
    };
    if let Some(e) = entry {
        let age = sessions::format_age(now, &e.last_refresh);
        eprintln!(
            "  - {id}: host={}, pid={}, age={age}{exclusive}",
            e.hostname, e.pid
        );
    } else {
        eprintln!("  - {id}: (malformed marker){exclusive}");
    }
}

/// List the session markers and remove them all after confirmation.
/// `locks_removed`: the lock files removed before, named in the refusal.
fn clear_sessions(
    storage: &dyn vykar_storage::StorageBackend,
    locks_removed: usize,
) -> CliResult<()> {
    let entries = sessions::list_session_entries(storage)?;

    if entries.is_empty() {
        println!("No active sessions found.");
        return Ok(());
    }

    let now = Utc::now();
    eprintln!("Found {} active session(s):", entries.len());
    for (id, entry) in &entries {
        print_session(&now, id, entry.as_ref());
    }

    eprintln!();
    eprintln!(
        "WARNING: Removing sessions may interrupt live backups and maintenance on other machines."
    );

    if !std::io::stdin().is_terminal() {
        return Err(CliError::from(format!(
            "refusing to remove sessions without confirmation in non-interactive mode \
             ({locks_removed} leftover lock file(s) were removed; removing session markers \
             needs a terminal). `vykar clear-sessions --stale` removes only the stale markers \
             (45 minutes old, or of a process gone on this machine) without asking"
        )));
    }

    eprint!("Remove all sessions? [y/N]: ");
    std::io::stderr().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    if !input.trim().eq_ignore_ascii_case("y") {
        eprintln!("Aborted.");
        return Ok(());
    }

    let removed = sessions::clear_all_sessions(storage)?;
    println!("Removed {removed} session file(s).");
    Ok(())
}
