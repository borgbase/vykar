// Test-only env mutation; SAFETY documented per block.
#![allow(unsafe_code)]

use std::sync::Mutex;

use zeroize::Zeroizing;

use crate::app::passphrase::{
    reset_env_passphrase_cache, resolve_init_passphrase, resolve_passphrase, InitPassphrase,
    InitPromptStage, PassphrasePrompt,
};
use crate::config::EncryptionModeConfig;
use vykar_types::error::VykarError;

use super::helpers::make_test_config;

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn set_vykar_passphrase(value: Option<&str>) {
    // SAFETY: callers serialize on `ENV_LOCK` so no other test thread is
    // reading/writing env concurrently for the duration of this mutation.
    unsafe {
        match value {
            Some(v) => std::env::set_var("VYKAR_PASSPHRASE", v),
            None => std::env::remove_var("VYKAR_PASSPHRASE"),
        }
    }
    reset_env_passphrase_cache();
}

#[cfg(not(windows))]
fn print_script(text: &str) -> String {
    format!("printf '{text}'")
}

#[cfg(windows)]
fn print_script(text: &str) -> String {
    format!("Write-Output \"{text}\"")
}

#[test]
fn resolve_passphrase_returns_none_when_encryption_mode_is_none() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::None;
    config.encryption.passcommand = Some(print_script("cmd-pass"));
    set_vykar_passphrase(Some("env-pass"));

    let mut prompted = false;
    let pass = resolve_passphrase(&config, Some("repo-a"), |_prompt| {
        prompted = true;
        Ok(Some(Zeroizing::new("prompt-pass".into())))
    })
    .unwrap();

    assert!(pass.is_none());
    assert!(!prompted);
    set_vykar_passphrase(None);
}

#[test]
fn resolve_passphrase_uses_expected_precedence() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;

    set_vykar_passphrase(Some("env-pass"));
    config.encryption.passphrase = Some("inline-pass".into());
    config.encryption.passcommand = Some(print_script("cmd-pass"));
    let pass = resolve_passphrase(&config, None, |_prompt| {
        Ok(Some(Zeroizing::new("prompt-pass".into())))
    })
    .unwrap();
    assert_eq!(pass.as_deref().map(String::as_str), Some("inline-pass"));

    config.encryption.passphrase = None;
    let pass = resolve_passphrase(&config, None, |_prompt| {
        Ok(Some(Zeroizing::new("prompt-pass".into())))
    })
    .unwrap();
    assert_eq!(pass.as_deref().map(String::as_str), Some("cmd-pass"));

    config.encryption.passcommand = None;
    let pass = resolve_passphrase(&config, None, |_prompt| {
        Ok(Some(Zeroizing::new("prompt-pass".into())))
    })
    .unwrap();
    assert_eq!(pass.as_deref().map(String::as_str), Some("env-pass"));

    set_vykar_passphrase(None);
    let pass = resolve_passphrase(&config, None, |_prompt| {
        Ok(Some(Zeroizing::new("prompt-pass".into())))
    })
    .unwrap();
    assert_eq!(pass.as_deref().map(String::as_str), Some("prompt-pass"));
}

#[test]
fn resolve_passphrase_surfaces_passcommand_failure() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = None;
    config.encryption.passcommand = Some("exit 7".into());
    set_vykar_passphrase(None);

    let err = resolve_passphrase(&config, None, |_prompt| {
        Ok(Some(Zeroizing::new("prompt-pass".into())))
    })
    .err()
    .unwrap();
    assert!(format!("{err}").contains("passcommand failed"));
}

#[cfg(not(windows))]
#[test]
fn resolve_passphrase_passcommand_handles_shell_quoting() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = None;
    // Single quotes inside the command — sh -c must handle them correctly.
    config.encryption.passcommand = Some("printf '%s' 'hello world'".into());
    set_vykar_passphrase(None);

    let pass = resolve_passphrase(&config, None, |_prompt| {
        Ok(Some(Zeroizing::new("prompt-pass".into())))
    })
    .unwrap();
    assert_eq!(pass.as_deref().map(String::as_str), Some("hello world"));
}

#[cfg(not(windows))]
#[test]
fn resolve_passphrase_passcommand_gets_null_stdin() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = None;
    // A passcommand that drains stdin must see EOF immediately rather than
    // block on an inherited descriptor until the 60s timeout (issue #166).
    config.encryption.passcommand = Some("cat >/dev/null; printf '%s' 'drained'".into());
    set_vykar_passphrase(None);

    let start = std::time::Instant::now();
    let pass = resolve_passphrase(&config, None, |_prompt| {
        Ok(Some(Zeroizing::new("prompt-pass".into())))
    })
    .unwrap();
    assert_eq!(pass.as_deref().map(String::as_str), Some("drained"));
    assert!(
        start.elapsed() < std::time::Duration::from_secs(10),
        "passcommand should not have waited on stdin"
    );
}

#[test]
fn resolve_passphrase_passes_prompt_context() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = None;
    config.encryption.passcommand = None;
    set_vykar_passphrase(None);

    let mut seen_prompt: Option<PassphrasePrompt> = None;
    let pass = resolve_passphrase(&config, Some("repo-1"), |prompt| {
        seen_prompt = Some(prompt);
        Ok(Some(Zeroizing::new("prompt-pass".into())))
    })
    .unwrap();

    assert_eq!(pass.as_deref().map(String::as_str), Some("prompt-pass"));
    let prompt = seen_prompt.expect("prompt should have been invoked");
    assert_eq!(prompt.repository_label.as_deref(), Some("repo-1"));
    assert_eq!(prompt.repository_url, config.repository.url);
}

/// A passcommand whose every execution appends a line to `marker`, so a test
/// can prove it ran exactly once across a probe + init pair.
#[cfg(not(windows))]
fn counting_passcommand(marker: &std::path::Path, value: &str) -> String {
    format!("echo x >> {}; printf '%s' '{value}'", marker.display())
}

#[cfg(not(windows))]
#[test]
fn init_reuses_the_probed_configured_passphrase_without_rerunning_passcommand() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let marker = tmp.path().join("runs");
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passcommand = Some(counting_passcommand(&marker, "cmd-pass"));
    set_vykar_passphrase(None);

    // Probe: resolves through the passcommand (execution #1).
    let probed = resolve_passphrase(&config, None, |_| unreachable!("no prompt expected"))
        .unwrap()
        .unwrap();
    assert_eq!(probed.as_str(), "cmd-pass");

    // Init: the caller hands the probed value back, so the passcommand must
    // not run a second time and no confirmation is asked for.
    let outcome = resolve_init_passphrase(&config, None, Some(probed), |_, _| {
        unreachable!("configured source must not prompt")
    })
    .unwrap();
    assert!(matches!(outcome, InitPassphrase::Provided(p) if p.as_str() == "cmd-pass"));

    let runs = std::fs::read_to_string(&marker)
        .unwrap_or_default()
        .lines()
        .count();
    assert_eq!(runs, 1, "passcommand ran {runs} times, expected exactly 1");
}

#[test]
fn init_uses_env_passphrase_without_prompting() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = None;
    config.encryption.passcommand = None;
    set_vykar_passphrase(Some("env-pass"));

    // Non-interactive init must succeed with no prompt and no confirmation.
    let outcome = resolve_init_passphrase(&config, None, None, |_, _| {
        unreachable!("VYKAR_PASSPHRASE must not prompt")
    })
    .unwrap();
    assert!(matches!(outcome, InitPassphrase::Provided(p) if p.as_str() == "env-pass"));
    set_vykar_passphrase(None);
}

#[test]
fn init_confirms_an_interactively_entered_passphrase() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = None;
    config.encryption.passcommand = None;
    set_vykar_passphrase(None);

    // A passphrase typed at the probe prompt is *not* passed as
    // `pre_resolved_configured`, so init still asks twice.
    let mut stages = Vec::new();
    let outcome = resolve_init_passphrase(&config, Some("repo-1"), None, |stage, ctx| {
        assert_eq!(ctx.repository_label.as_deref(), Some("repo-1"));
        stages.push(stage);
        Ok(Some(Zeroizing::new("typed".into())))
    })
    .unwrap();
    assert!(matches!(outcome, InitPassphrase::Provided(p) if p.as_str() == "typed"));
    assert_eq!(stages, [InitPromptStage::Enter, InitPromptStage::Confirm]);
}

#[test]
fn init_rejects_empty_and_mismatched_passphrases() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = None;
    config.encryption.passcommand = None;
    set_vykar_passphrase(None);

    let err = resolve_init_passphrase(&config, None, None, |_, _| {
        Ok(Some(Zeroizing::new(String::new())))
    })
    .unwrap_err();
    assert!(matches!(err, VykarError::EmptyPassphrase));

    let err = resolve_init_passphrase(&config, None, None, |stage, _| {
        Ok(Some(Zeroizing::new(match stage {
            InitPromptStage::Enter => "first".to_string(),
            InitPromptStage::Confirm => "second".to_string(),
        })))
    })
    .unwrap_err();
    assert!(matches!(err, VykarError::PassphraseMismatch));
}

#[test]
fn init_rejects_an_empty_configured_passphrase() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passcommand = None;
    set_vykar_passphrase(None);

    // `passphrase: ""` in YAML deserializes to `Some("")` — the strict-string
    // deserializer only rejects nulls — so this reaches resolution intact and
    // must not silently key the new repository on an empty passphrase.
    config.encryption.passphrase = Some(String::new());
    let err = resolve_init_passphrase(&config, None, None, |_, _| {
        unreachable!("a configured source must not prompt")
    })
    .unwrap_err();
    assert!(matches!(err, VykarError::EmptyPassphrase));
}

#[test]
fn init_rejects_an_empty_pre_resolved_passphrase() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = Some(String::new());
    set_vykar_passphrase(None);

    // The GUI hands back whatever the probe resolved from the configured
    // source; an empty value there is the same config bug and must not be
    // waved through just because the probe already saw it.
    let err = resolve_init_passphrase(
        &config,
        None,
        Some(Zeroizing::new(String::new())),
        |_, _| unreachable!("a pre-resolved value must not prompt"),
    )
    .unwrap_err();
    assert!(matches!(err, VykarError::EmptyPassphrase));
}

#[test]
fn init_cancellation_of_either_prompt_is_not_an_error() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = None;
    config.encryption.passcommand = None;
    set_vykar_passphrase(None);

    let first = resolve_init_passphrase(&config, None, None, |_, _| Ok(None)).unwrap();
    assert!(matches!(first, InitPassphrase::Cancelled));

    let second = resolve_init_passphrase(&config, None, None, |stage, _| match stage {
        InitPromptStage::Enter => Ok(Some(Zeroizing::new("typed".into()))),
        InitPromptStage::Confirm => Ok(None),
    })
    .unwrap();
    assert!(matches!(second, InitPassphrase::Cancelled));
}

#[test]
fn init_needs_no_passphrase_for_an_unencrypted_repo() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::None;
    config.encryption.passphrase = Some("ignored".into());
    set_vykar_passphrase(Some("env-pass"));

    let outcome = resolve_init_passphrase(&config, None, None, |_, _| {
        unreachable!("unencrypted repo must not prompt")
    })
    .unwrap();
    assert!(matches!(outcome, InitPassphrase::NotRequired));
    set_vykar_passphrase(None);
}
