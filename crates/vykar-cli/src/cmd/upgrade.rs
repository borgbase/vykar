use std::io::{self, BufRead, IsTerminal, Write};

use vykar_core::commands;
use vykar_core::config::VykarConfig;
use vykar_core::repo::UpgradeOutcome;

use crate::error::{CliError, CliResult};
use crate::format::format_bytes;
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_upgrade(config: &VykarConfig, label: Option<&str>, yes: bool) -> CliResult<()> {
    if !yes {
        if !io::stdin().is_terminal() {
            return Err(CliError::from(
                "upgrade is one-way; pass --yes to run it non-interactively",
            ));
        }
        eprintln!(
            "This upgrades the repository to the v6 repository format. It is one-way:\n\
             vykar releases before 0.25 can no longer read or write it. Stop every other\n\
             client of this repository first, and upgrade vykar on all of them."
        );
        eprint!("Type 'upgrade' to proceed: ");
        io::stderr().flush()?;
        let mut input = String::new();
        io::stdin().lock().read_line(&mut input)?;
        if input.trim() != "upgrade" {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    let outcome = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::upgrade::run(config, passphrase)?)
    })?;

    match outcome {
        UpgradeOutcome::AlreadyCurrent { version } => {
            println!("Repository is already at format v{version}; nothing to do.");
        }
        UpgradeOutcome::Upgraded {
            from,
            to,
            chunk_hash,
            entries,
            base_bytes,
            packs_mapped,
            packs_unmapped,
            resumed,
        } => {
            if resumed {
                println!("Completed an interrupted upgrade.");
            }
            println!(
                "Upgraded repository from format v{from} to v{to} ({}): {entries} index \
                 entries, base {}, {packs_mapped} packs mapped.",
                chunk_hash.as_str(),
                format_bytes(base_bytes)
            );
            if packs_unmapped > 0 {
                eprintln!(
                    "Warning: {packs_unmapped} pack(s) hold more entries than a pack map can \
                     describe; only the index describes them, so `vykar check --repair` \
                     cannot rebuild their entries if the index is lost."
                );
            }
        }
    }
    Ok(())
}
