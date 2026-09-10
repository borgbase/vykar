use std::collections::HashMap;

use crossbeam_channel::Sender;
use vykar_core::app::{operations, passphrase};
use vykar_core::commands;
use vykar_core::config::{self, ResolvedRepo};
use vykar_types::error::VykarError;

use crate::messages::{log_entry_now, UiEvent};
use crate::APP_TITLE;
use vykar_common::display::format_bytes;

/// Resolve a repo passphrase (configured source or interactive prompt),
/// recording in `from_dialog` whether the value came from the interactive
/// dialog (retryable) versus a configured source (not retryable). `error_line`,
/// when non-empty, is shown in red above the prompt on a re-attempt.
fn resolve_passphrase_tracked(
    repo: &ResolvedRepo,
    error_line: &str,
    from_dialog: &mut bool,
) -> Result<Option<zeroize::Zeroizing<String>>, VykarError> {
    let repo_name = repo.label_or_url().to_string();
    passphrase::resolve_passphrase(&repo.config, repo.label.as_deref(), |prompt| {
        *from_dialog = true;
        let title = format!("{APP_TITLE} - Passphrase ({repo_name})");
        let message = match prompt.repository_label.as_deref() {
            Some(label) if label != prompt.repository_url.as_str() => format!(
                "Enter passphrase for {label}\nRepository: {}",
                prompt.repository_url,
            ),
            _ => format!("Enter passphrase for {}", prompt.repository_url),
        };
        let value = crate::controllers::password_dialog::show_password_dialog_with_error(
            &title, &message, error_line,
        );
        // The dialog returns `Some(_)` on submit (including an empty field) and
        // `None` on cancel/close. An empty string is a valid passphrase — a repo
        // can be created with one via the CLI — so it must pass through as
        // `Some("")`; only `None` (dismissed) signals "no passphrase entered".
        Ok(value.map(zeroize::Zeroizing::new))
    })
}

/// Outcome of running an operation under [`with_passphrase_retry`].
pub(crate) enum PassphraseRun<T> {
    /// The operation ran and returned a value.
    Ran(T),
    /// The repo is encrypted and the user dismissed the passphrase prompt.
    Canceled,
}

/// A repo located by [`find_repo_for_snapshot`] together with the passphrase
/// (if any) validated for it — `None` for a plaintext repo.
pub(crate) type FoundRepo<'a> = (&'a ResolvedRepo, Option<zeroize::Zeroizing<String>>);

/// Run `f` with a resolved passphrase, caching it **only on success**.
///
/// A cached (already-validated) passphrase is used directly. Otherwise the
/// passphrase is resolved and `f` runs; if `f` fails in a way a wrong
/// passphrase could explain (see [`VykarError::is_passphrase_failure`]) and the
/// passphrase came from the interactive dialog, the cache stays clean and the
/// user is re-prompted (up to `attempts` total). A wrong *configured* passphrase fails immediately. If the
/// user dismisses the prompt for an encrypted repo, returns
/// [`PassphraseRun::Canceled`].
pub(crate) fn with_passphrase_retry<T>(
    repo: &ResolvedRepo,
    cache: &mut HashMap<String, zeroize::Zeroizing<String>>,
    attempts: usize,
    f: impl FnMut(Option<&str>) -> Result<T, VykarError>,
) -> Result<PassphraseRun<T>, VykarError> {
    let key = repo.config.repository.url.clone();
    let encrypted = repo.config.encryption.mode != config::EncryptionModeConfig::None;
    with_passphrase_retry_inner(
        &key,
        encrypted,
        cache,
        attempts,
        |error_line| {
            let mut from_dialog = false;
            let pass = resolve_passphrase_tracked(repo, error_line, &mut from_dialog)?;
            Ok((pass, from_dialog))
        },
        f,
    )
}

/// Testable core of [`with_passphrase_retry`]: the cache / retry / provenance
/// state machine, with passphrase resolution injected via `resolve` (returns
/// the passphrase and whether it came from an interactive dialog).
fn with_passphrase_retry_inner<T>(
    key: &str,
    encrypted: bool,
    cache: &mut HashMap<String, zeroize::Zeroizing<String>>,
    attempts: usize,
    mut resolve: impl FnMut(&str) -> Result<(Option<zeroize::Zeroizing<String>>, bool), VykarError>,
    mut f: impl FnMut(Option<&str>) -> Result<T, VykarError>,
) -> Result<PassphraseRun<T>, VykarError> {
    // A cached passphrase is only ever inserted after a successful op, so it is
    // trusted — use it directly without a re-prompt path. Defense in depth: if a
    // cached value nonetheless fails to decrypt (a misbehaving call site cached a
    // bad value), evict it and fall through into the prompt/retry loop so the
    // cache is self-healing rather than permanently poisoned.
    if let Some(cached) = cache.get(key) {
        match f(Some(cached.as_str())) {
            Err(ref e) if e.is_passphrase_failure() => {
                cache.remove(key);
            }
            other => return other.map(PassphraseRun::Ran),
        }
    }

    let attempts = attempts.max(1);
    let mut error_line = String::new();
    let mut last_failure: Option<VykarError> = None;
    for attempt in 0..attempts {
        let (pass, from_dialog) = resolve(&error_line)?;

        // Encrypted repo with no passphrase → the user dismissed the prompt.
        if encrypted && pass.is_none() {
            return Ok(PassphraseRun::Canceled);
        }

        match f(pass.as_deref().map(|s| s.as_str())) {
            Ok(v) => {
                if let Some(p) = pass {
                    cache.insert(key.to_string(), p);
                }
                return Ok(PassphraseRun::Ran(v));
            }
            Err(e) if e.is_passphrase_failure() && from_dialog && attempt + 1 < attempts => {
                error_line = "Incorrect passphrase. Please try again.".to_string();
                last_failure = Some(e);
            }
            Err(e) => return Err(e),
        }
    }

    // Attempts exhausted after repeated wrong dialog entries. Return the last
    // real failure so its diagnosis (which key copies were consulted, whether
    // they corroborated each other) is not flattened away.
    Err(last_failure.unwrap_or(VykarError::DecryptionFailed))
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
/// through [`with_passphrase_retry`] so a wrong dialog entry re-prompts, bad
/// values are never cached, and a stale cached value is evicted (self-healing).
///
/// Returns [`PassphraseRun::Canceled`] if the user dismisses the prompt for the
/// matching encrypted repo. Candidate repos whose listing reports
/// [`VykarError::SnapshotNotFound`] are skipped; other errors propagate.
pub(crate) fn find_repo_for_snapshot<'a>(
    repos: &'a [ResolvedRepo],
    selector: &str,
    snapshot: &str,
    passphrases: &mut HashMap<String, zeroize::Zeroizing<String>>,
) -> Result<PassphraseRun<FoundRepo<'a>>, VykarError> {
    for repo in select_repos(repos, selector)? {
        let key = repo.config.repository.url.clone();
        let outcome = with_passphrase_retry(repo, passphrases, 3, |pass| {
            commands::list::list_snapshot_items(&repo.config, pass, snapshot).map(|_| ())
        });

        match outcome {
            Ok(PassphraseRun::Ran(())) => {
                // The passphrase (if any) is now validated and cached; read it
                // back to hand to the caller. `None` for plaintext repos.
                let pass = passphrases.get(&key).cloned();
                return Ok(PassphraseRun::Ran((repo, pass)));
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "repo://test";

    /// Build a resolver returning `responses` in order (passphrase, from_dialog),
    /// counting how many times it was called.
    fn scripted_resolver<'a>(
        responses: &'a [(Option<&'a str>, bool)],
        calls: &'a std::cell::Cell<usize>,
    ) -> impl FnMut(&str) -> Result<(Option<zeroize::Zeroizing<String>>, bool), VykarError> + 'a
    {
        move |_error_line| {
            let i = calls.get();
            calls.set(i + 1);
            let (pass, from_dialog) = *responses.get(i).expect("scripted response in range");
            Ok((
                pass.map(|s| zeroize::Zeroizing::new(s.to_string())),
                from_dialog,
            ))
        }
    }

    #[test]
    fn dialog_wrong_then_right_retries_and_caches_only_on_success() {
        let mut cache = HashMap::new();
        let calls = std::cell::Cell::new(0);
        let responses = [(Some("wrong"), true), (Some("right"), true)];
        let result = with_passphrase_retry_inner(
            KEY,
            true,
            &mut cache,
            3,
            scripted_resolver(&responses, &calls),
            |pass| {
                if pass == Some("right") {
                    Ok("ok")
                } else {
                    Err(VykarError::DecryptionFailed)
                }
            },
        );
        assert!(matches!(result, Ok(PassphraseRun::Ran("ok"))));
        assert_eq!(calls.get(), 2, "should re-prompt exactly once");
        // Only the correct passphrase is ever cached; the wrong one never is.
        assert_eq!(cache.get(KEY).map(|z| z.as_str()), Some("right"));
    }

    #[test]
    fn configured_wrong_passphrase_fails_without_retry() {
        let mut cache = HashMap::new();
        let calls = std::cell::Cell::new(0);
        // from_dialog = false → not retryable.
        let responses = [(Some("cfg"), false)];
        let result: Result<PassphraseRun<()>, _> = with_passphrase_retry_inner(
            KEY,
            true,
            &mut cache,
            3,
            scripted_resolver(&responses, &calls),
            |_pass| Err(VykarError::DecryptionFailed),
        );
        assert!(matches!(result, Err(VykarError::DecryptionFailed)));
        assert_eq!(calls.get(), 1, "configured passphrase is tried once");
        assert!(cache.is_empty(), "failed passphrase must not be cached");
    }

    /// Regression guard for the retry loop's coupling to the error *type*.
    ///
    /// Driven by a real `Repository::open` against a real encrypted
    /// repository, because the failure this must catch was introduced by
    /// changing which variant that open returns — a hand-written
    /// `Err(DecryptionFailed)` stub would have kept passing throughout.
    fn open_with(repo_dir: &std::path::Path) -> impl FnMut(Option<&str>) -> Result<(), VykarError> {
        let repo_dir = repo_dir.to_path_buf();
        move |pass: Option<&str>| {
            let storage = Box::new(
                vykar_storage::local_backend::LocalBackend::new(
                    repo_dir.to_str().expect("temp path is UTF-8"),
                )
                .map_err(|e| VykarError::Other(e.to_string()))?,
            );
            vykar_core::repo::Repository::open(
                storage,
                pass,
                None,
                vykar_core::repo::OpenOptions::new(),
            )
            .map(|_| ())
        }
    }

    /// Build an encrypted repository whose passphrase is `pass`.
    fn encrypted_repo(tmp: &tempfile::TempDir, pass: &str) -> std::path::PathBuf {
        let repo_dir = tmp.path().join("repo");
        std::fs::create_dir_all(&repo_dir).expect("create repo dir");
        let storage = Box::new(
            vykar_storage::local_backend::LocalBackend::new(
                repo_dir.to_str().expect("temp path is UTF-8"),
            )
            .expect("local backend"),
        );
        vykar_core::repo::Repository::init(
            storage,
            vykar_core::repo::EncryptionMode::Aes256Gcm,
            vykar_core::config::ChunkerConfig::default(),
            Some(pass),
            None,
            None,
        )
        .expect("init encrypted repo");
        repo_dir
    }

    #[test]
    fn a_real_wrong_passphrase_open_still_re_prompts() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repo_dir = encrypted_repo(&tmp, "right-passphrase");

        let mut cache = HashMap::new();
        let calls = std::cell::Cell::new(0);
        let responses = [
            (Some("wrong-passphrase"), true),
            (Some("right-passphrase"), true),
        ];
        let result = with_passphrase_retry_inner(
            KEY,
            true,
            &mut cache,
            3,
            scripted_resolver(&responses, &calls),
            open_with(&repo_dir),
        );

        assert!(
            matches!(result, Ok(PassphraseRun::Ran(()))),
            "unexpected outcome"
        );
        assert_eq!(calls.get(), 2, "a typo must trigger exactly one re-prompt");
        assert_eq!(cache.get(KEY).map(|z| z.as_str()), Some("right-passphrase"));
    }

    #[test]
    fn a_real_wrong_passphrase_open_evicts_a_stale_cached_entry() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repo_dir = encrypted_repo(&tmp, "right-passphrase");

        let mut cache = HashMap::new();
        cache.insert(
            KEY.to_string(),
            zeroize::Zeroizing::new("stale-passphrase".to_string()),
        );

        let calls = std::cell::Cell::new(0);
        let responses = [(Some("right-passphrase"), true)];
        let result = with_passphrase_retry_inner(
            KEY,
            true,
            &mut cache,
            3,
            scripted_resolver(&responses, &calls),
            open_with(&repo_dir),
        );

        assert!(
            matches!(result, Ok(PassphraseRun::Ran(()))),
            "unexpected outcome"
        );
        assert_eq!(
            cache.get(KEY).map(|z| z.as_str()),
            Some("right-passphrase"),
            "the stale entry must be evicted and replaced, not kept"
        );
    }

    /// A failure a wrong passphrase *cannot* explain must not burn retries.
    ///
    /// The two copies are corrupted **differently** on purpose: identical bytes
    /// take the matched-copies path, so a same-bytes fixture would never reach
    /// the divergent branch where the classification is easiest to get wrong.
    #[test]
    fn a_corrupt_key_is_not_treated_as_a_passphrase_problem() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repo_dir = encrypted_repo(&tmp, "right-passphrase");
        let keys = repo_dir.join("keys");
        std::fs::write(keys.join("repokey"), b"not msgpack at all").expect("corrupt primary");
        let mut mangled = vykar_crypto::key::inspect_key_blob(
            &std::fs::read(keys.join("repokey.2")).expect("read secondary"),
        )
        .expect("the secondary starts out well-formed");
        mangled.nonce = vec![0u8; 8];
        std::fs::write(
            keys.join("repokey.2"),
            rmp_serde::to_vec(&mangled).expect("re-encode"),
        )
        .expect("corrupt secondary");

        let mut cache = HashMap::new();
        let calls = std::cell::Cell::new(0);
        let responses = [(Some("right-passphrase"), true), (Some("again"), true)];
        let result = with_passphrase_retry_inner(
            KEY,
            true,
            &mut cache,
            3,
            scripted_resolver(&responses, &calls),
            open_with(&repo_dir),
        );

        let err = result.err().expect("a corrupt key must fail the open");
        assert!(!err.is_passphrase_failure(), "{err}");
        assert_eq!(calls.get(), 1, "proven corruption must not re-prompt");
    }

    /// Argon2 parameters inside every ceiling but below Argon2's own
    /// `memory >= 8 * parallelism` floor cannot derive a key. That is
    /// corruption, and must not cost the operator a re-prompt.
    #[test]
    fn impossible_argon2_params_are_not_treated_as_a_passphrase_problem() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repo_dir = encrypted_repo(&tmp, "right-passphrase");
        let keys = repo_dir.join("keys");
        let mut mangled = vykar_crypto::key::inspect_key_blob(
            &std::fs::read(keys.join("repokey")).expect("read primary"),
        )
        .expect("the primary starts out well-formed");
        mangled.kdf.memory_cost = 1;
        mangled.kdf.parallelism = 16;
        let bytes = rmp_serde::to_vec(&mangled).expect("re-encode");
        for name in ["repokey", "repokey.2"] {
            std::fs::write(keys.join(name), &bytes).expect("write mangled copy");
        }

        let mut cache = HashMap::new();
        let calls = std::cell::Cell::new(0);
        let responses = [(Some("right-passphrase"), true), (Some("again"), true)];
        let result = with_passphrase_retry_inner(
            KEY,
            true,
            &mut cache,
            3,
            scripted_resolver(&responses, &calls),
            open_with(&repo_dir),
        );

        let err = result.err().expect("an underivable key must fail the open");
        assert!(!err.is_passphrase_failure(), "{err}");
        assert_eq!(calls.get(), 1, "proven corruption must not re-prompt");
    }

    #[test]
    fn dismissed_prompt_on_encrypted_repo_is_canceled() {
        let mut cache = HashMap::new();
        let calls = std::cell::Cell::new(0);
        let responses = [(None, true)];
        let result: Result<PassphraseRun<()>, _> = with_passphrase_retry_inner(
            KEY,
            true,
            &mut cache,
            3,
            scripted_resolver(&responses, &calls),
            |_pass| unreachable!("f must not run when the prompt is dismissed"),
        );
        assert!(matches!(result, Ok(PassphraseRun::Canceled)));
        assert!(cache.is_empty());
    }

    #[test]
    fn cached_passphrase_is_used_without_resolving() {
        let mut cache = HashMap::new();
        cache.insert(KEY.to_string(), zeroize::Zeroizing::new("good".to_string()));
        let result = with_passphrase_retry_inner(
            KEY,
            true,
            &mut cache,
            3,
            |_e| unreachable!("resolver must not run when cached"),
            |pass| {
                assert_eq!(pass, Some("good"));
                Ok(42)
            },
        );
        assert!(matches!(result, Ok(PassphraseRun::Ran(42))));
    }

    #[test]
    fn poisoned_cache_is_evicted_and_reprompted() {
        // A cached value that no longer decrypts (poisoned by a misbehaving call
        // site) must be evicted, the user re-prompted, and the fresh correct
        // value cached — the cache is self-healing.
        let mut cache = HashMap::new();
        cache.insert(
            KEY.to_string(),
            zeroize::Zeroizing::new("stale".to_string()),
        );
        let calls = std::cell::Cell::new(0);
        let responses = [(Some("right"), true)];
        let result = with_passphrase_retry_inner(
            KEY,
            true,
            &mut cache,
            3,
            scripted_resolver(&responses, &calls),
            |pass| {
                if pass == Some("right") {
                    Ok("ok")
                } else {
                    Err(VykarError::DecryptionFailed)
                }
            },
        );
        assert!(matches!(result, Ok(PassphraseRun::Ran("ok"))));
        assert_eq!(calls.get(), 1, "should re-prompt once after eviction");
        assert_eq!(cache.get(KEY).map(|z| z.as_str()), Some("right"));
    }

    #[test]
    fn exhausted_attempts_returns_decryption_failed() {
        let mut cache = HashMap::new();
        let calls = std::cell::Cell::new(0);
        let responses = [(Some("a"), true), (Some("b"), true), (Some("c"), true)];
        let result: Result<PassphraseRun<()>, _> = with_passphrase_retry_inner(
            KEY,
            true,
            &mut cache,
            3,
            scripted_resolver(&responses, &calls),
            |_pass| Err(VykarError::DecryptionFailed),
        );
        assert!(matches!(result, Err(VykarError::DecryptionFailed)));
        assert_eq!(calls.get(), 3, "should prompt exactly `attempts` times");
        assert!(cache.is_empty());
    }

    #[test]
    fn unencrypted_repo_runs_with_none_passphrase() {
        let mut cache = HashMap::new();
        let calls = std::cell::Cell::new(0);
        let responses = [(None, false)];
        let result = with_passphrase_retry_inner(
            KEY,
            false, // not encrypted
            &mut cache,
            3,
            scripted_resolver(&responses, &calls),
            |pass| {
                assert_eq!(pass, None);
                Ok("done")
            },
        );
        assert!(matches!(result, Ok(PassphraseRun::Ran("done"))));
        assert!(cache.is_empty(), "None passphrase is never cached");
    }
}
