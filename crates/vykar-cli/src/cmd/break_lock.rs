use std::io::Write;

use chrono::Utc;
use serde::Serialize;
use vykar_core::config::VykarConfig;
use vykar_core::repo::lock;

use crate::error::{CliError, CliResult};
use crate::output::{Output, Report};

pub(crate) fn run_break_lock(
    config: &VykarConfig,
    _label: Option<&str>,
    sessions: bool,
    out: &mut Output,
) -> CliResult<()> {
    let storage = vykar_core::storage::backend_from_config(
        &config
            .repository
            .storage_config(config.limits.connections)
            .with_cancel(crate::signal::shutdown_token().clone()),
    )?;

    let removed = lock::break_lock(storage.as_ref())?;
    out.emit(&LocksReport {
        locks_removed: removed,
    })?;

    if sessions {
        clear_sessions(storage.as_ref(), out)?;
    }

    Ok(())
}

#[derive(Serialize)]
struct LocksReport {
    locks_removed: usize,
}

impl Report for LocksReport {
    const KEY: &'static str = "break_lock";

    fn json(&self) -> impl Serialize {
        self
    }

    fn print_human(&self) {
        if self.locks_removed == 0 {
            println!("No locks found.");
        } else {
            println!("Removed {} lock(s).", self.locks_removed);
        }
    }
}

#[derive(Serialize)]
struct SessionsReport {
    sessions_found: usize,
    sessions_removed: usize,
}

impl Report for SessionsReport {
    const KEY: &'static str = "sessions";

    fn json(&self) -> impl Serialize {
        self
    }

    fn print_human(&self) {
        if self.sessions_found == 0 {
            println!("No active sessions found.");
        } else {
            println!("Removed {} session file(s).", self.sessions_removed);
        }
    }
}

fn clear_sessions(storage: &dyn vykar_storage::StorageBackend, out: &mut Output) -> CliResult<()> {
    let entries = lock::list_session_entries(storage)?;

    if entries.is_empty() {
        return out.emit(&SessionsReport {
            sessions_found: 0,
            sessions_removed: 0,
        });
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

    if !out.interactive() {
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
    out.emit(&SessionsReport {
        sessions_found: entries.len(),
        sessions_removed: removed,
    })
}
