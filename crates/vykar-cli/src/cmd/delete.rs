use serde::Serialize;

use vykar_core::commands;
use vykar_core::commands::delete::DeleteResult;
use vykar_core::commands::delete_repo::DeleteRepoStats;
use vykar_core::config::VykarConfig;
use vykar_core::repo::control::CancelToken;

use crate::error::{CliError, CliResult};
use crate::output::{Output, Report};
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_delete(
    config: &VykarConfig,
    label: Option<&str>,
    snapshot_names: &[String],
    dry_run: bool,
    shutdown: &CancelToken,
    out: &mut Output,
) -> CliResult<()> {
    let name_refs: Vec<&str> = snapshot_names.iter().map(|s| s.as_str()).collect();
    let result = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::delete::run(
            config, passphrase, &name_refs, dry_run, shutdown,
        )?)
    })?;

    out.emit(&SnapshotDeleteReport { result, dry_run })
}

struct SnapshotDeleteReport {
    result: DeleteResult,
    dry_run: bool,
}

#[derive(Serialize)]
struct SnapshotDeleteJson<'a> {
    dry_run: bool,
    /// Snapshots deleted, or that a dry run would delete.
    snapshots: Vec<&'a str>,
    warnings: &'a [String],
}

impl Report for SnapshotDeleteReport {
    const KEY: &'static str = "snapshot_delete";

    fn json(&self) -> impl Serialize {
        SnapshotDeleteJson {
            dry_run: self.dry_run,
            snapshots: self
                .result
                .stats
                .iter()
                .map(|s| s.snapshot_name.as_str())
                .collect(),
            warnings: &self.result.warnings,
        }
    }

    fn print_human(&self) {
        let (result, dry_run) = (&self.result, self.dry_run);
        for stats in &result.stats {
            if dry_run {
                println!("Dry run: would delete snapshot '{}'", stats.snapshot_name);
            } else {
                println!("Deleted snapshot '{}'", stats.snapshot_name);
            }
        }
        if !dry_run && !result.stats.is_empty() {
            println!("Run `vykar compact` to reclaim their space.");
        }

        for w in &result.warnings {
            eprintln!("warning: {w}");
        }
    }
}

pub(crate) fn run_delete_repo(
    config: &VykarConfig,
    label: Option<&str>,
    yes_delete_this_repo: bool,
    out: &mut Output,
) -> CliResult<()> {
    // Verify the repo exists before prompting — use list + is_known_repo_key
    // so we detect partially-deleted repos (e.g. config gone but packs remain).
    let backend = vykar_core::storage::backend_from_config(
        &config
            .repository
            .storage_config(config.limits.connections)
            .with_cancel(crate::signal::shutdown_token().clone()),
    )?;
    let all_keys = backend.list("")?;
    let has_repo_keys = all_keys
        .iter()
        .any(|k| commands::delete_repo::is_known_repo_key(k));
    if !has_repo_keys {
        return Err(CliError::from(format!(
            "no repository found at '{}'",
            config.repository.url
        )));
    }
    drop(backend);

    if !yes_delete_this_repo {
        if !out.interactive() {
            return Err(CliError::from(
                "refusing to delete repository without confirmation in non-interactive mode; \
                 use --yes-delete-this-repo to skip the prompt",
            ));
        }

        let repo_name = label.unwrap_or(&config.repository.url);
        eprintln!(
            "WARNING: This will permanently delete the entire repository '{repo_name}' \
             and ALL its snapshots."
        );
        eprintln!();
        eprint!("Type 'delete' to confirm: ");
        std::io::Write::flush(&mut std::io::stderr())?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if input.trim() != "delete" {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    let stats = commands::delete_repo::run(config)?;
    out.emit(&RepoDeleteReport {
        repo_name: label.unwrap_or(&config.repository.url),
        url: &config.repository.url,
        stats,
    })
}

struct RepoDeleteReport<'a> {
    repo_name: &'a str,
    url: &'a str,
    stats: DeleteRepoStats,
}

#[derive(Serialize)]
struct RepoDeleteJson<'a> {
    keys_deleted: u64,
    /// Entries vykar does not manage, left in place.
    unknown_entries: &'a [String],
    root_removed: bool,
}

impl Report for RepoDeleteReport<'_> {
    const KEY: &'static str = "delete";

    fn json(&self) -> impl Serialize {
        RepoDeleteJson {
            keys_deleted: self.stats.keys_deleted,
            unknown_entries: &self.stats.unknown_entries,
            root_removed: self.stats.root_removed,
        }
    }

    fn print_human(&self) {
        print_repo_deleted(self.repo_name, self.url, &self.stats);
    }
}

fn print_repo_deleted(repo_name: &str, url: &str, stats: &DeleteRepoStats) {
    if stats.unknown_entries.is_empty() {
        println!(
            "Repository '{repo_name}' deleted ({} keys removed).",
            stats.keys_deleted
        );
    } else {
        println!(
            "Repository '{repo_name}' deleted ({} keys removed, {} unknown entries left).",
            stats.keys_deleted,
            stats.unknown_entries.len()
        );
        eprintln!();
        eprintln!(
            "Warning: {} unknown entries were not removed:",
            stats.unknown_entries.len()
        );
        for entry in &stats.unknown_entries {
            eprintln!("  {entry}");
        }
    }

    if stats.is_local && !stats.root_removed && stats.unknown_entries.is_empty() {
        eprintln!(
            "Note: repository directory '{}' could not be fully removed; \
             it may contain empty directories, other non-file entries, or temp-named \
             files ('.tmp.*') in directories vykar does not manage.",
            url
        );
    }
}
