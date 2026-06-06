use std::io::{IsTerminal, Write};

use chrono::Utc;
use vykar_core::config::VykarConfig;
use vykar_core::repo::lock;

use crate::error::{CliError, CliResult};

pub(crate) fn run_break_lock(
    config: &VykarConfig,
    _label: Option<&str>,
    sessions: bool,
) -> CliResult<()> {
    let storage =
        vykar_core::storage::backend_from_config(&config.repository, config.limits.connections)?;

    let removed = lock::break_lock(storage.as_ref())?;

    if removed == 0 {
        println!("No locks found.");
    } else {
        println!("Removed {removed} lock(s).");
    }

    if sessions {
        clear_sessions(storage.as_ref())?;
    }

    Ok(())
}

fn clear_sessions(storage: &dyn vykar_storage::StorageBackend) -> CliResult<()> {
    let entries = lock::list_session_entries(storage)?;

    if entries.is_empty() {
        println!("No active sessions found.");
        return Ok(());
    }

    let now = Utc::now();
    eprintln!("Found {} active session(s):", entries.len());
    for (id, entry) in &entries {
        if let Some(e) = entry {
            let age = lock::format_age(&now, &e.last_refresh);
            eprintln!("  - {id}: host={}, pid={}, age={age}", e.hostname, e.pid);
        } else {
            eprintln!("  - {id}: (malformed marker)");
        }
    }

    eprintln!();
    eprintln!("WARNING: Removing sessions may interrupt live backups on other machines.");

    if !std::io::stdin().is_terminal() {
        return Err(CliError::from(
            "refusing to remove sessions without confirmation in non-interactive mode",
        ));
    }

    eprint!("Remove all sessions? [y/N]: ");
    std::io::stderr().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    if !input.trim().eq_ignore_ascii_case("y") {
        eprintln!("Aborted.");
        return Ok(());
    }

    let removed = lock::clear_all_sessions(storage)?;
    println!("Removed {removed} session file(s).");
    Ok(())
}
