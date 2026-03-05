use rand::RngCore;

use crate::commands;
use crate::compress::Compression;
use crate::config::{ResolvedRepo, SourceEntry, VykarConfig};
use crate::repo::manifest::SnapshotEntry;
use crate::snapshot::item::Item;
use vykar_types::error::{Result, VykarError};

#[derive(Debug, Clone)]
pub struct BackupSourceResult {
    pub source_label: String,
    pub snapshot_name: String,
    pub source_paths: Vec<String>,
    pub stats: crate::snapshot::SnapshotStats,
}

#[derive(Debug, Clone, Default)]
pub struct BackupRunReport {
    pub created: Vec<BackupSourceResult>,
}

#[derive(Debug, Clone)]
pub struct RepoBackupRunReport {
    pub repo_label: Option<String>,
    pub repository_url: String,
    pub report: BackupRunReport,
}

#[derive(Debug, Clone)]
pub struct RestoreRequest {
    pub snapshot_name: String,
    pub destination: String,
    pub pattern: Option<String>,
}

fn generate_snapshot_name() -> String {
    let mut buf = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

pub fn run_backup_for_repo(
    config: &VykarConfig,
    sources: &[SourceEntry],
    passphrase: Option<&str>,
) -> Result<BackupRunReport> {
    run_backup_for_repo_with_progress(config, sources, passphrase, &mut |_| {})
}

pub fn run_backup_for_repo_with_progress(
    config: &VykarConfig,
    sources: &[SourceEntry],
    passphrase: Option<&str>,
    progress: &mut dyn FnMut(commands::backup::BackupProgressEvent),
) -> Result<BackupRunReport> {
    if sources.is_empty() {
        return Err(VykarError::Config(
            "no sources configured for this repository".into(),
        ));
    }

    let compression =
        Compression::from_algorithm(config.compression.algorithm, config.compression.zstd_level);

    let mut report = BackupRunReport::default();

    for source in sources {
        let snapshot_name = generate_snapshot_name();
        let outcome = commands::backup::run_with_progress(
            config,
            commands::backup::BackupRequest {
                snapshot_name: &snapshot_name,
                passphrase,
                source_paths: &source.paths,
                source_label: &source.label,
                exclude_patterns: &source.exclude,
                exclude_if_present: &source.exclude_if_present,
                one_file_system: source.one_file_system,
                git_ignore: source.git_ignore,
                xattrs_enabled: source.xattrs_enabled,
                compression,
                command_dumps: &source.command_dumps,
                verbose: false,
            },
            Some(progress),
            None,
        )?;

        report.created.push(BackupSourceResult {
            source_label: source.label.clone(),
            snapshot_name,
            source_paths: source.paths.clone(),
            stats: outcome.stats,
        });
    }

    Ok(report)
}

pub fn run_backup_for_all_repos(
    repos: &[ResolvedRepo],
    passphrase_lookup: &mut dyn FnMut(&ResolvedRepo) -> Result<Option<String>>,
) -> Result<Vec<RepoBackupRunReport>> {
    let mut reports = Vec::with_capacity(repos.len());
    for repo in repos {
        let passphrase = passphrase_lookup(repo)?;
        let report = run_backup_for_repo(&repo.config, &repo.sources, passphrase.as_deref())?;
        reports.push(RepoBackupRunReport {
            repo_label: repo.label.clone(),
            repository_url: repo.config.repository.url.clone(),
            report,
        });
    }
    Ok(reports)
}

pub fn list_snapshots(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<Vec<SnapshotEntry>> {
    commands::list::list_snapshots(config, passphrase)
}

pub fn list_snapshots_with_stats(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<Vec<(SnapshotEntry, crate::snapshot::SnapshotStats)>> {
    commands::list::list_snapshots_with_stats(config, passphrase)
}

pub fn list_snapshot_items(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<Vec<Item>> {
    commands::list::list_snapshot_items(config, passphrase, snapshot_name)
}

pub fn restore_snapshot(
    config: &VykarConfig,
    passphrase: Option<&str>,
    req: &RestoreRequest,
) -> Result<commands::restore::RestoreStats> {
    commands::restore::run(
        config,
        passphrase,
        &req.snapshot_name,
        &req.destination,
        req.pattern.as_deref(),
        config.xattrs.enabled,
    )
}

pub fn restore_selected(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    destination: &str,
    selected_paths: &std::collections::HashSet<String>,
) -> Result<commands::restore::RestoreStats> {
    commands::restore::run_selected(
        config,
        passphrase,
        snapshot_name,
        destination,
        selected_paths,
        config.xattrs.enabled,
    )
}

pub fn check_repo(
    config: &VykarConfig,
    passphrase: Option<&str>,
    verify_data: bool,
) -> Result<commands::check::CheckResult> {
    commands::check::run(config, passphrase, verify_data, false)
}

pub fn check_repo_with_progress(
    config: &VykarConfig,
    passphrase: Option<&str>,
    verify_data: bool,
    progress: &mut dyn FnMut(commands::check::CheckProgressEvent),
) -> Result<commands::check::CheckResult> {
    commands::check::run_with_progress(config, passphrase, verify_data, false, Some(progress))
}

pub fn delete_snapshot(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<commands::delete::DeleteStats> {
    commands::delete::run(config, passphrase, snapshot_name, false, None)
}
