use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use zeroize::Zeroizing;

use crate::prompt::prompt_hidden;
use vykar_core::config::{EncryptionModeConfig, VykarConfig};

/// Process-level passphrase cache keyed by repository URL.
/// Avoids double interactive prompts when probe-then-dispatch opens the same
/// repo twice (once to check the manifest, once to run the command).
static PASSPHRASE_CACHE: LazyLock<Mutex<HashMap<String, Option<Zeroizing<String>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) fn with_repo_passphrase<T>(
    config: &VykarConfig,
    label: Option<&str>,
    action: impl FnOnce(Option<&str>) -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config, label)?;
    action(passphrase.as_deref().map(|s| s.as_str()))
}

pub(crate) fn get_passphrase(
    config: &VykarConfig,
    label: Option<&str>,
) -> Result<Option<Zeroizing<String>>, Box<dyn std::error::Error>> {
    if config.encryption.mode == EncryptionModeConfig::None {
        return Ok(None);
    }

    let cache_key = config.repository.url.clone();

    // Check cache first (avoids double interactive prompt during probe+dispatch)
    if let Some(cached) = PASSPHRASE_CACHE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&cache_key)
    {
        return Ok(cached.clone());
    }

    if let Some(pass) = configured_passphrase(config)? {
        PASSPHRASE_CACHE
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(cache_key, Some(pass.clone()));
        return Ok(Some(pass));
    }

    // Interactive prompt
    let prompt = match label {
        Some(l) => format!("Enter passphrase for '{l}': "),
        None => "Enter passphrase: ".to_string(),
    };
    let pass = Zeroizing::new(prompt_hidden(&prompt)?);
    PASSPHRASE_CACHE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(cache_key, Some(pass.clone()));
    Ok(Some(pass))
}

fn configured_passphrase(
    config: &VykarConfig,
) -> Result<Option<Zeroizing<String>>, Box<dyn std::error::Error>> {
    vykar_core::app::passphrase::configured_passphrase(config)
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
}

pub(crate) fn get_init_passphrase(
    config: &VykarConfig,
    label: Option<&str>,
) -> Result<Option<Zeroizing<String>>, Box<dyn std::error::Error>> {
    if config.encryption.mode == EncryptionModeConfig::None {
        return Ok(None);
    }
    if let Some(pass) = configured_passphrase(config)? {
        tracing::warn!(
            "using plaintext encryption.passphrase from config; prefer encryption.passcommand or VYKAR_PASSPHRASE"
        );
        return Ok(Some(pass));
    }

    let suffix = label.map(|l| format!(" for '{l}'")).unwrap_or_default();
    let p1 = Zeroizing::new(prompt_hidden(&format!("Enter new passphrase{suffix}: "))?);
    let p2 = Zeroizing::new(prompt_hidden(&format!("Confirm passphrase{suffix}: "))?);
    if *p1 != *p2 {
        return Err("passphrases do not match".into());
    }
    Ok(Some(p1))
}
