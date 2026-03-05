use std::io::{IsTerminal, Write};

use chrono::Utc;
use vykar_core::config::VykarConfig;
use vykar_core::repo::lock;

pub(crate) fn run_break_lock(
    config: &VykarConfig,
    _label: Option<&str>,
    sessions: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage =
        vykar_core::storage::backend_from_config(&config.repository, config.limits.connections)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

    let removed = lock::break_lock(storage.as_ref())
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

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

fn clear_sessions(
    storage: &dyn vykar_storage::StorageBackend,
) -> Result<(), Box<dyn std::error::Error>> {
    let entries = lock::list_session_entries(storage)
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

    if entries.is_empty() {
        println!("No active sessions found.");
        return Ok(());
    }

    let now = Utc::now();
    eprintln!("Found {} active session(s):", entries.len());
    for (id, entry) in &entries {
        if let Some(e) = entry {
            let age = format_age(&now, &e.last_refresh);
            eprintln!("  - {id}: host={}, pid={}, age={age}", e.hostname, e.pid);
        } else {
            eprintln!("  - {id}: (malformed marker)");
        }
    }

    eprintln!();
    eprintln!("WARNING: Removing sessions may interrupt live backups on other machines.");

    if !std::io::stdin().is_terminal() {
        return Err(
            "refusing to remove sessions without confirmation in non-interactive mode".into(),
        );
    }

    eprint!("Remove all sessions? [y/N]: ");
    std::io::stderr().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    if !input.trim().eq_ignore_ascii_case("y") {
        eprintln!("Aborted.");
        return Ok(());
    }

    let removed = lock::clear_all_sessions(storage)
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
    println!("Removed {removed} session file(s).");
    Ok(())
}

fn format_age(now: &chrono::DateTime<Utc>, timestamp: &str) -> String {
    let Ok(ts) = chrono::DateTime::parse_from_rfc3339(timestamp) else {
        return "unknown".to_string();
    };
    let dur = now.signed_duration_since(ts.with_timezone(&Utc));
    let hours = dur.num_hours();
    if hours >= 24 {
        format!("{}d {}h", hours / 24, hours % 24)
    } else if hours > 0 {
        format!("{hours}h")
    } else {
        let mins = dur.num_minutes().max(0);
        format!("{mins}m")
    }
}
