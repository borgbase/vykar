use crossbeam_channel::Sender;
use vykar_core::app::passphrase::{
    PASSPHRASE_PROMPT_CANCELED, PassphrasePrompt, PromptContext, PromptReply,
};
use vykar_core::app::{AppService, OpsError, operations};
use vykar_core::commands;
use vykar_core::config::{self, ResolvedRepo};
use vykar_types::error::VykarError;

use crate::APP_TITLE;
use crate::messages::{UiEvent, log_entry_now};
use vykar_common::display::format_bytes;

/// The passphrase dialog: the title names the repository, a retry shows the
/// reason in red above the field.
pub(crate) struct DialogPrompt;

impl PassphrasePrompt for DialogPrompt {
    fn ask(
        &mut self,
        ctx: &PromptContext,
        retry_reason: Option<&str>,
    ) -> vykar_types::error::Result<PromptReply> {
        let repo_name = ctx
            .repository_label
            .as_deref()
            .unwrap_or(&ctx.repository_url);
        let title = format!("{APP_TITLE} - Passphrase ({repo_name})");
        let message = match ctx.repository_label.as_deref() {
            Some(label) if label != ctx.repository_url.as_str() => format!(
                "Enter passphrase for {label}\nRepository: {}",
                ctx.repository_url,
            ),
            _ => format!("Enter passphrase for {}", ctx.repository_url),
        };
        let value = crate::controllers::password_dialog::show_password_dialog_with_error(
            &title,
            &message,
            retry_reason.unwrap_or(""),
        );
        // The dialog returns `Some(_)` on submit (including an empty field) and
        // `None` on cancel/close. An empty string is a valid passphrase — a repo
        // can be created with one via the CLI — so it must pass through as
        // `Some("")`; only `None` (dismissed) signals "no passphrase entered".
        Ok(match value {
            Some(v) => PromptReply::Value(zeroize::Zeroizing::new(v)),
            None => PromptReply::Cancel,
        })
    }
}

/// Outcome of running an operation under [`run_with_passphrase`].
pub(crate) enum PassphraseRun<T> {
    /// The operation ran and returned a value.
    Ran(T),
    /// The user dismissed the passphrase prompt.
    Canceled,
}

/// A repo located by [`find_repo_for_snapshot`] together with the passphrase
/// validated for it.
pub(crate) type FoundRepo<'a> = (&'a ResolvedRepo, Option<zeroize::Zeroizing<String>>);

/// A value and the passphrase it was obtained with.
pub(crate) type WithPassphrase<T> = (T, Option<zeroize::Zeroizing<String>>);

/// `f` under the service's passphrase policy with the dialog: a cached value
/// is used directly, a wrong dialog entry re-prompts, a value is cached only
/// once `f` succeeded with it.
pub(crate) fn run_with_passphrase<T>(
    service: &mut AppService,
    repo: &ResolvedRepo,
    f: impl FnMut(Option<&str>) -> Result<T, VykarError>,
) -> Result<PassphraseRun<T>, VykarError> {
    Ok(
        match run_with_prompt(service, repo, &mut DialogPrompt, f)? {
            PassphraseRun::Ran((v, _)) => PassphraseRun::Ran(v),
            PassphraseRun::Canceled => PassphraseRun::Canceled,
        },
    )
}

/// [`run_with_passphrase`] that also returns the accepted value.
pub(crate) fn run_with_passphrase_value<T>(
    service: &mut AppService,
    repo: &ResolvedRepo,
    f: impl FnMut(Option<&str>) -> Result<T, VykarError>,
) -> Result<PassphraseRun<WithPassphrase<T>>, VykarError> {
    run_with_prompt(service, repo, &mut DialogPrompt, f)
}

/// Testable core of both: any prompt.
fn run_with_prompt<T>(
    service: &mut AppService,
    repo: &ResolvedRepo,
    prompt: &mut dyn PassphrasePrompt,
    mut f: impl FnMut(Option<&str>) -> Result<T, VykarError>,
) -> Result<PassphraseRun<WithPassphrase<T>>, VykarError> {
    let mut pass = service.passphrase(&repo.config, repo.label.as_deref(), prompt);
    match pass.with(|p| f(p).map_err(OpsError::from)) {
        Ok(v) => Ok(PassphraseRun::Ran((v, pass.value()))),
        Err(OpsError::Skipped(r)) if r == PASSPHRASE_PROMPT_CANCELED => Ok(PassphraseRun::Canceled),
        Err(OpsError::Engine(e)) => Err(e),
        // Unreachable without hooks; kept as an error rather than a panic.
        Err(e) => Err(VykarError::Other(e.to_string())),
    }
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

/// Locate the repo holding `snapshot` and validate its passphrase, routing
/// through [`run_with_passphrase_value`] so a wrong dialog entry re-prompts, bad
/// values are never cached, and a stale cached value is evicted (self-healing).
///
/// Returns [`PassphraseRun::Canceled`] if the user dismisses the prompt for the
/// matching repo. Candidate repos whose metadata lookup reports
/// [`VykarError::SnapshotNotFound`] are skipped; other errors propagate.
pub(crate) fn find_repo_for_snapshot<'a>(
    repos: &'a [ResolvedRepo],
    selector: &str,
    snapshot: &str,
    service: &mut AppService,
) -> Result<PassphraseRun<FoundRepo<'a>>, VykarError> {
    for repo in select_repos(repos, selector)? {
        let outcome = run_with_passphrase_value(service, repo, |pass| {
            commands::list::get_snapshot_meta(&repo.config, pass, snapshot).map(|_| ())
        });

        match outcome {
            Ok(PassphraseRun::Ran(((), pass))) => return Ok(PassphraseRun::Ran((repo, pass))),
            // Callers pass a concrete repo name, so multiple candidates are
            // rare; a canceled prompt aborts the lookup rather than falling
            // through to the next candidate.
            Ok(PassphraseRun::Canceled) => return Ok(PassphraseRun::Canceled),
            Err(VykarError::SnapshotNotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }

    Err(VykarError::SnapshotNotFound(snapshot.to_string()))
}

pub(crate) fn send_log(ui_tx: &Sender<UiEvent>, message: impl Into<String>) {
    let _ = ui_tx.send(log_entry_now(message));
}

pub(crate) fn log_backup_report(
    ui_tx: &Sender<UiEvent>,
    repo_name: &str,
    report: &operations::BackupRunReport,
) {
    if report.created.is_empty()
        && report.skipped.is_empty()
        && report.failed.is_empty()
        && report.aborted.is_none()
    {
        send_log(ui_tx, format!("[{repo_name}] no snapshots created"));
        return;
    }
    for skipped in &report.skipped {
        send_log(
            ui_tx,
            format!(
                "[{repo_name}] source {} skipped: {}",
                skipped.label, skipped.reason
            ),
        );
    }
    for failed in &report.failed {
        send_log(
            ui_tx,
            format!(
                "[{repo_name}] source {} failed: {}",
                failed.label, failed.error
            ),
        );
    }
    if let Some(aborted) = &report.aborted {
        let what = if aborted.interrupted {
            "interrupted"
        } else {
            "stopped"
        };
        let mut line = format!(
            "[{repo_name}] backup {what} at source {}: {}",
            aborted.label, aborted.error
        );
        if !aborted.not_attempted.is_empty() {
            line.push_str(&format!(
                " (not attempted: {})",
                aborted.not_attempted.join(", ")
            ));
        }
        send_log(ui_tx, line);
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

/// Whether a repo that reported `RepoNotFound` is a sensible target for an
/// init offer. Only local paths are inspected; remote backends report an
/// unreachable host as a connection error, never as `RepoNotFound`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum InitTarget {
    Remote,
    Offerable,
    /// Root and its parent are missing — most likely an unplugged drive or an
    /// unmounted share.
    Unavailable,
    /// Root exists and holds files, but no vykar `config`.
    NotEmpty,
    Invalid(String),
}

/// OS-generated files that don't make a directory "non-empty" for init.
const IGNORED_DIR_ENTRIES: &[&str] = &[".DS_Store", "Thumbs.db", "desktop.ini"];

pub(crate) fn classify_init_target(url: &str) -> InitTarget {
    let path = match vykar_core::storage::parse_repo_url(url) {
        Ok(vykar_core::storage::ParsedUrl::Local { path }) => path,
        _ => return InitTarget::Remote,
    };
    let root = std::path::Path::new(&path);

    match std::fs::metadata(root) {
        Ok(meta) if meta.is_dir() => match std::fs::read_dir(root) {
            Ok(entries) => {
                let has_content = entries.filter_map(Result::ok).any(|e| {
                    !IGNORED_DIR_ENTRIES
                        .iter()
                        .any(|ignored| e.file_name() == *ignored)
                });
                if has_content {
                    InitTarget::NotEmpty
                } else {
                    InitTarget::Offerable
                }
            }
            Err(e) => InitTarget::Invalid(format!("cannot read {path}: {e}")),
        },
        Ok(_) => InitTarget::Invalid(format!("{path} is not a directory")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // A bare relative name ("repo") has an empty parent — the cwd.
            let parent_exists = match root.parent() {
                Some(p) if p.as_os_str().is_empty() => true,
                Some(p) => p.is_dir(),
                None => true,
            };
            if parent_exists {
                InitTarget::Offerable
            } else {
                InitTarget::Unavailable
            }
        }
        Err(e) => InitTarget::Invalid(format!("cannot access {path}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod init_target {
        use super::*;

        fn classify(path: &std::path::Path) -> InitTarget {
            classify_init_target(path.to_str().unwrap())
        }

        #[test]
        fn missing_root_and_parent_is_unavailable() {
            let tmp = tempfile::tempdir().unwrap();
            let root = tmp.path().join("volume").join("repo");
            assert_eq!(classify(&root), InitTarget::Unavailable);
        }

        #[test]
        fn missing_root_with_existing_parent_is_offerable() {
            let tmp = tempfile::tempdir().unwrap();
            assert_eq!(classify(&tmp.path().join("repo")), InitTarget::Offerable);
        }

        #[test]
        fn empty_dir_is_offerable() {
            let tmp = tempfile::tempdir().unwrap();
            assert_eq!(classify(tmp.path()), InitTarget::Offerable);
        }

        #[test]
        fn dir_with_only_os_metadata_is_offerable() {
            let tmp = tempfile::tempdir().unwrap();
            std::fs::write(tmp.path().join(".DS_Store"), b"x").unwrap();
            std::fs::write(tmp.path().join("Thumbs.db"), b"x").unwrap();
            assert_eq!(classify(tmp.path()), InitTarget::Offerable);
        }

        #[test]
        fn dir_with_unrelated_file_is_not_empty() {
            let tmp = tempfile::tempdir().unwrap();
            std::fs::write(tmp.path().join("notes.txt"), b"x").unwrap();
            assert_eq!(classify(tmp.path()), InitTarget::NotEmpty);
        }

        #[test]
        fn regular_file_root_is_invalid() {
            let tmp = tempfile::tempdir().unwrap();
            let file = tmp.path().join("repo");
            std::fs::write(&file, b"x").unwrap();
            assert!(matches!(classify(&file), InitTarget::Invalid(_)));
        }

        #[test]
        fn remote_urls_are_remote() {
            assert_eq!(
                classify_init_target("s3://s3.example.com/bucket/repo"),
                InitTarget::Remote
            );
            assert_eq!(
                classify_init_target("sftp://user@host/backups/repo"),
                InitTarget::Remote
            );
        }
    }
}
