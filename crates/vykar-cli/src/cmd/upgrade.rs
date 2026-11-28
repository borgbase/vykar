use std::io::{self, BufRead, IsTerminal, Write};

use vykar_core::commands;
use vykar_core::config::VykarConfig;
use vykar_core::repo::{UpgradeOutcome, WriterMode, WriterModeReport};

use crate::error::{CliError, CliResult};
use crate::format::format_bytes;
use vykar_core::app::passphrase::RepoPassphrase;

/// The line `vykar info` and a plain `vykar upgrade` add for a single-writer
/// repository whose storage offers put-if-absent.
pub(crate) const MULTI_WRITER_HINT: &str = "This storage offers put-if-absent: \
     `vykar upgrade --commit-mode multi` lets backups commit side by side (it checks the \
     storage first).";

pub(crate) fn run_upgrade(
    config: &VykarConfig,
    yes: bool,
    mode: Option<WriterMode>,
    passphrase: &mut RepoPassphrase<'_>,
) -> CliResult<()> {
    if !yes {
        if !io::stdin().is_terminal() {
            return Err(CliError::from(
                "upgrade is one-way; pass --yes to run it non-interactively",
            ));
        }
        match mode {
            Some(mode) => eprintln!(
                "This switches the repository's commit mode to {mode} (or records it when the\n\
                 repository is upgraded from v2/v3). Stop every other client of this repository\n\
                 first."
            ),
            None => eprintln!(
                "This upgrades the repository to the v6 repository format. It is one-way:\n\
                 vykar releases before 0.30 can no longer read or write it. Stop every other\n\
                 client of this repository first, and upgrade vykar on all of them."
            ),
        }
        eprint!("Type 'upgrade' to proceed: ");
        io::stderr().flush()?;
        let mut input = String::new();
        io::stdin().lock().read_line(&mut input)?;
        if input.trim() != "upgrade" {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    let outcome = passphrase.with(|passphrase| -> CliResult<_> {
        Ok(commands::upgrade::run_with_mode(config, passphrase, mode)?)
    })?;

    match outcome {
        UpgradeOutcome::AlreadyCurrent {
            version,
            writer_mode:
                WriterModeReport {
                    mode: current,
                    changed,
                    put_if_absent_offered,
                    ..
                },
        } => {
            if changed {
                println!("Switched the repository to {current}.");
            } else {
                println!("Repository is already at format v{version} ({current}); nothing to do.");
                if mode.is_none() && put_if_absent_offered {
                    println!("{MULTI_WRITER_HINT}");
                }
            }
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
            writer_mode,
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
            match writer_mode.single_reason {
                Some(reason) => println!("Commit mode: {} ({reason})", writer_mode.mode),
                None => println!("Commit mode: {}", writer_mode.mode),
            }
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
