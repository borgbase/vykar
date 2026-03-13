use std::collections::HashMap;

use chrono::Local;
use crossbeam_channel::Sender;
use vykar_core::app::{operations, passphrase};
use vykar_core::config::{self, ResolvedRepo};
use vykar_types::error::VykarError;

use crate::messages::UiEvent;
use crate::progress::format_bytes;
use crate::APP_TITLE;

pub(crate) fn format_repo_name(repo: &ResolvedRepo) -> String {
    repo.label
        .clone()
        .unwrap_or_else(|| repo.config.repository.url.clone())
}

pub(crate) fn resolve_passphrase_for_repo(
    repo: &ResolvedRepo,
) -> Result<Option<zeroize::Zeroizing<String>>, VykarError> {
    let repo_name = format_repo_name(repo);
    let pass = passphrase::resolve_passphrase(&repo.config, repo.label.as_deref(), |prompt| {
        let title = format!("{APP_TITLE} — Passphrase ({repo_name})");
        let message = format!(
            "Enter passphrase for {}\nRepository: {}",
            prompt
                .repository_label
                .as_deref()
                .unwrap_or(prompt.repository_url.as_str()),
            prompt.repository_url,
        );
        let value = tinyfiledialogs::password_box(&title, &message);
        Ok(value.filter(|v| !v.is_empty()).map(zeroize::Zeroizing::new))
    })?;
    Ok(pass)
}

pub(crate) fn get_or_resolve_passphrase(
    repo: &ResolvedRepo,
    cache: &mut HashMap<String, zeroize::Zeroizing<String>>,
) -> Result<Option<zeroize::Zeroizing<String>>, VykarError> {
    let key = &repo.config.repository.url;
    if let Some(existing) = cache.get(key) {
        return Ok(Some(existing.clone()));
    }
    let pass = resolve_passphrase_for_repo(repo)?;
    if let Some(ref p) = pass {
        cache.insert(key.clone(), p.clone());
    }
    Ok(pass)
}

pub(crate) fn select_repos<'a>(
    repos: &'a [ResolvedRepo],
    selector: &str,
) -> Result<Vec<&'a ResolvedRepo>, VykarError> {
    let selector = selector.trim();
    if selector.is_empty() {
        return Ok(repos.iter().collect());
    }

    let repo = config::select_repo(repos, selector)
        .ok_or_else(|| VykarError::Config(format!("no repository matching '{selector}'")))?;
    Ok(vec![repo])
}

pub(crate) fn find_repo_for_snapshot<'a>(
    repos: &'a [ResolvedRepo],
    selector: &str,
    snapshot: &str,
    passphrases: &mut HashMap<String, zeroize::Zeroizing<String>>,
) -> Result<(&'a ResolvedRepo, Option<zeroize::Zeroizing<String>>), VykarError> {
    for repo in select_repos(repos, selector)? {
        let key = repo.config.repository.url.clone();
        let pass = if let Some(cached) = passphrases.get(&key) {
            Some(cached.clone())
        } else {
            let p = resolve_passphrase_for_repo(repo)?;
            if let Some(ref v) = p {
                passphrases.insert(key.clone(), v.clone());
            }
            p
        };

        match operations::list_snapshot_items(
            &repo.config,
            pass.as_deref().map(|s| s.as_str()),
            snapshot,
        ) {
            Ok(_) => return Ok((repo, pass)),
            Err(VykarError::SnapshotNotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }

    Err(VykarError::SnapshotNotFound(snapshot.to_string()))
}

pub(crate) fn send_log(ui_tx: &Sender<UiEvent>, message: impl Into<String>) {
    let timestamp = Local::now().format("%H:%M:%S").to_string();
    let _ = ui_tx.send(UiEvent::LogEntry {
        timestamp,
        message: message.into(),
    });
}

pub(crate) fn log_backup_report(
    ui_tx: &Sender<UiEvent>,
    repo_name: &str,
    report: &operations::BackupRunReport,
) {
    if report.created.is_empty() {
        send_log(ui_tx, format!("[{repo_name}] no snapshots created"));
        return;
    }
    for created in &report.created {
        send_log(
            ui_tx,
            format!(
                "[{repo_name}] snapshot {} source={} files={} original={} compressed={} deduplicated={}",
                created.snapshot_name,
                created.source_label,
                created.stats.nfiles,
                format_bytes(created.stats.original_size),
                format_bytes(created.stats.compressed_size),
                format_bytes(created.stats.deduplicated_size),
            ),
        );
    }
}
