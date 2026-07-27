use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use zeroize::Zeroizing;

use crate::error::{CliError, CliResult};
use crate::prompt::prompt_hidden;
use vykar_core::app::passphrase::{
    resolve_init_passphrase, resolve_passphrase, InitPassphrase, InitPromptStage,
};
use vykar_core::config::{EncryptionModeConfig, VykarConfig};

/// Process-level passphrase cache keyed by repository URL.
/// Avoids double interactive prompts when probe-then-dispatch opens the same
/// repo twice (once to check the manifest, once to run the command).
static PASSPHRASE_CACHE: LazyLock<Mutex<HashMap<String, Option<Zeroizing<String>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) fn with_repo_passphrase<T>(
    config: &VykarConfig,
    label: Option<&str>,
    action: impl FnOnce(Option<&str>) -> CliResult<T>,
) -> CliResult<T> {
    let passphrase = get_passphrase(config, label)?;
    action(passphrase.as_deref().map(|s| s.as_str()))
}

pub(crate) fn get_passphrase(
    config: &VykarConfig,
    label: Option<&str>,
) -> CliResult<Option<Zeroizing<String>>> {
    let cache_key = config.repository.url.clone();

    // Check cache first (avoids double interactive prompt during probe+dispatch)
    if let Some(cached) = PASSPHRASE_CACHE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&cache_key)
    {
        return Ok(cached.clone());
    }

    let pass = resolve_passphrase(config, label, |ctx| {
        let prompt = match ctx.repository_label {
            Some(ref l) => format!("Enter passphrase for '{l}': "),
            None => "Enter passphrase: ".to_string(),
        };
        Ok(Some(Zeroizing::new(prompt_hidden(&prompt)?)))
    })?;

    PASSPHRASE_CACHE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(cache_key, pass.clone());
    Ok(pass)
}

pub(crate) fn get_init_passphrase(
    config: &VykarConfig,
    label: Option<&str>,
) -> CliResult<Option<Zeroizing<String>>> {
    // `encryption.passphrase` is first in the resolution order, so its presence
    // on an encrypted repo means it is the value being used.
    if config.encryption.mode != EncryptionModeConfig::None
        && config.encryption.passphrase.is_some()
    {
        tracing::warn!(
            "using plaintext encryption.passphrase from config; prefer encryption.passcommand or VYKAR_PASSPHRASE"
        );
    }

    let suffix = label.map(|l| format!(" for '{l}'")).unwrap_or_default();
    let outcome = resolve_init_passphrase(config, label, None, |stage, _ctx| {
        let text = match stage {
            InitPromptStage::Enter => format!("Enter new passphrase{suffix}: "),
            InitPromptStage::Confirm => format!("Confirm passphrase{suffix}: "),
        };
        Ok(Some(Zeroizing::new(prompt_hidden(&text)?)))
    })?;

    match outcome {
        InitPassphrase::NotRequired => Ok(None),
        InitPassphrase::Provided(p) => Ok(Some(p)),
        // Unreachable from a terminal: `prompt_hidden` never reports a
        // cancellation (Ctrl-C terminates the process, EOF is an I/O error).
        InitPassphrase::Cancelled => Err(CliError::from("passphrase entry cancelled")),
    }
}
