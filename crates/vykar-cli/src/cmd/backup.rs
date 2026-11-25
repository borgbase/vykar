use std::io::IsTerminal;

use serde::Serialize;

use vykar_core::app::operations::{self, BackupRunEvent, BackupRunReport};
use vykar_core::compress::Compression;
use vykar_core::config::{self, CompressionAlgorithm, ResolvedRepo, SourceEntry};
use vykar_core::repo::control::CancelToken;
use vykar_types::error::VykarError;

use crate::error::{CliError, CliResult};
use crate::format::{print_backup_stats, print_dataless_summary};
use crate::output::{Output, Report, SnapshotStatsJson};
use crate::passphrase::with_repo_passphrase;
use crate::progress::BackupProgressRenderer;

pub(crate) struct BackupRunOpts<'a> {
    pub user_label: Option<String>,
    pub compression_override: Option<String>,
    pub connections: Option<usize>,
    pub threads: Option<usize>,
    pub paths: Vec<String>,
    pub source_filter: &'a [String],
    pub shutdown: &'a CancelToken,
    pub verbose: u8,
}

/// Returns `Ok(true)` if the backup completed with partial success (some files skipped),
/// `Ok(false)` for full success.
pub(crate) fn run_backup(
    repo: &ResolvedRepo,
    opts: BackupRunOpts<'_>,
    out: &mut Output,
) -> CliResult<bool> {
    let BackupRunOpts {
        user_label,
        compression_override,
        connections,
        threads,
        paths,
        source_filter,
        shutdown,
        verbose,
    } = opts;

    // Clone repo and apply overrides
    let mut repo = repo.clone();
    if let Some(c) = connections {
        repo.config.limits.connections = c;
    }
    if let Some(t) = threads {
        repo.config.limits.threads = t;
    }
    if let Some(ref algo) = compression_override {
        // Validate the compression string by trying to parse it
        Compression::from_config(algo, repo.config.compression.zstd_level)?;
        // Set on config so run_backup_sources uses it
        repo.config.compression.algorithm = match algo.as_str() {
            "none" => CompressionAlgorithm::None,
            "lz4" => CompressionAlgorithm::Lz4,
            "zstd" => CompressionAlgorithm::Zstd,
            _ => return Err(CliError::from(format!("unsupported compression: {algo}"))),
        };
    }

    let label = repo.label.as_deref();

    with_repo_passphrase(&repo.config, label, |passphrase| {
        let is_tty = std::io::stderr().is_terminal();
        let show_progress = (is_tty && !out.is_json()) || verbose > 0;

        if !source_filter.is_empty() && !paths.is_empty() {
            return Err(CliError::from("cannot combine --source with ad-hoc paths"));
        }

        if user_label.is_some() && paths.is_empty() {
            return Err(CliError::from("--label can only be used with ad-hoc paths"));
        }

        // Resolve sources — configured, filtered, or synthesized from ad-hoc paths
        let sources: Vec<SourceEntry> = if !paths.is_empty() {
            let expanded: Vec<String> = paths.iter().map(|p| config::expand_tilde(p)).collect();
            let source_label = if let Some(ref lbl) = user_label {
                lbl.clone()
            } else if let [only] = expanded.as_slice() {
                config::label_from_path(only)
            } else {
                "adhoc".to_string()
            };
            vec![SourceEntry {
                paths: expanded,
                label: source_label,
                exclude: repo.config.exclude_patterns.clone(),
                exclude_if_present: repo.config.exclude_if_present.clone(),
                one_file_system: repo.config.one_file_system,
                git_ignore: repo.config.git_ignore,
                xattrs_enabled: repo.config.xattrs.enabled,
                hooks: Default::default(),
                retention: None,
                repos: Vec::new(),
                command_dumps: Vec::new(),
            }]
        } else if repo.sources.is_empty() {
            return Err(CliError::from(
                "no sources configured and no paths specified",
            ));
        } else if !source_filter.is_empty() {
            config::select_sources(&repo.sources, source_filter)?
                .into_iter()
                .cloned()
                .collect()
        } else {
            repo.sources.clone()
        };

        // Delegate to core's hook-aware backup
        let mut renderer = show_progress.then(|| BackupProgressRenderer::new(verbose, is_tty));

        let mut callback = |evt: BackupRunEvent| match evt {
            BackupRunEvent::Backup(bpe) => {
                if let Some(ref mut r) = renderer {
                    r.on_event(bpe);
                }
            }
            // HookWarning: no action — tracing::warn! already fired
            BackupRunEvent::HookWarning { .. } => {}
            // Always printed, even without progress: the run may still end in `Err`.
            BackupRunEvent::SourceFailed { label, error } => {
                let line = format!("Error: source '{label}': {error}");
                match renderer {
                    Some(ref mut r) => r.print_line(&line),
                    None => eprintln!("{line}"),
                }
            }
        };

        let result = operations::run_backup_selection(
            &repo,
            &sources,
            passphrase,
            shutdown,
            verbose >= 1,
            Some(&mut callback),
        );

        if let Some(ref mut r) = renderer {
            r.finish();
        }

        let report = result?;

        // Dataless (cloud-only) skips are reported, but deliberately not
        // counted as partial — see `print_dataless_summary`.
        let had_partial = report.created.iter().any(|c| c.stats.errors > 0);
        out.emit(&BackupReport(&report))?;
        if report.aborted.as_ref().is_some_and(|a| a.interrupted) {
            return Err(VykarError::Interrupted.into());
        }

        if report.failure_summary().is_some() {
            let failed = report.failed.len() + usize::from(report.aborted.is_some());
            return Err(CliError::from(format!(
                "{failed} of {} source(s) failed",
                sources.len()
            )));
        }

        Ok(had_partial)
    })
}

struct BackupReport<'a>(&'a BackupRunReport);

#[derive(Serialize)]
pub(crate) struct BackupJson<'a> {
    created: Vec<CreatedJson<'a>>,
    skipped: Vec<SourceReasonJson<'a>>,
    failed: Vec<SourceReasonJson<'a>>,
    aborted: Option<AbortedJson<'a>>,
}

#[derive(Serialize)]
struct CreatedJson<'a> {
    snapshot: &'a str,
    source_label: &'a str,
    source_paths: &'a [String],
    stats: SnapshotStatsJson,
    dataless_files_skipped: u64,
    dataless_dirs_skipped: u64,
}

#[derive(Serialize)]
struct SourceReasonJson<'a> {
    label: &'a str,
    reason: &'a str,
}

#[derive(Serialize)]
struct AbortedJson<'a> {
    label: &'a str,
    error: &'a str,
    interrupted: bool,
    not_attempted: &'a [String],
}

impl<'a> From<&'a BackupRunReport> for BackupJson<'a> {
    fn from(report: &'a BackupRunReport) -> Self {
        Self {
            created: report
                .created
                .iter()
                .map(|c| CreatedJson {
                    snapshot: &c.snapshot_name,
                    source_label: &c.source_label,
                    source_paths: &c.source_paths,
                    stats: SnapshotStatsJson::from(&c.stats),
                    dataless_files_skipped: c.dataless_files_skipped,
                    dataless_dirs_skipped: c.dataless_dirs_skipped,
                })
                .collect(),
            skipped: report
                .skipped
                .iter()
                .map(|s| SourceReasonJson {
                    label: &s.label,
                    reason: &s.reason,
                })
                .collect(),
            failed: report
                .failed
                .iter()
                .map(|f| SourceReasonJson {
                    label: &f.label,
                    reason: &f.error,
                })
                .collect(),
            aborted: report.aborted.as_ref().map(|a| AbortedJson {
                label: &a.label,
                error: &a.error,
                interrupted: a.interrupted,
                not_attempted: &a.not_attempted,
            }),
        }
    }
}

impl Report for BackupReport<'_> {
    const KEY: &'static str = "backup";

    fn json(&self) -> impl Serialize {
        BackupJson::from(self.0)
    }

    fn print_human(&self) {
        let report = self.0;
        for created in &report.created {
            let stats = &created.stats;
            if stats.errors > 0 {
                eprintln!(
                    "Warning: {} file(s) could not be read and were excluded from the snapshot",
                    stats.errors
                );
            }
            let paths_display = created.source_paths.join(", ");
            println!("Snapshot created: {}", created.snapshot_name);
            println!(
                "  Source: {paths_display} (label: {})",
                created.source_label
            );
            print_backup_stats(stats);
            print_dataless_summary(
                created.dataless_files_skipped,
                created.dataless_dirs_skipped,
            );
        }
        for skipped in &report.skipped {
            eprintln!("Skipped source '{}': {}", skipped.label, skipped.reason);
        }
        if let Some(aborted) = &report.aborted {
            crate::format::print_aborted_run(aborted);
        }
    }
}
