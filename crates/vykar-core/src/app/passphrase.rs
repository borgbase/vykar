use std::sync::Mutex;
use std::time::Duration;

use zeroize::{Zeroize, Zeroizing};

use crate::config::{EncryptionModeConfig, VykarConfig};
use crate::platform::shell;
use vykar_types::error::{Result, VykarError};

/// Default timeout for passcommand execution (60 seconds).
const PASSCOMMAND_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct PassphrasePrompt {
    pub repository_label: Option<String>,
    pub repository_url: String,
    pub timeout_seconds: u64,
}

pub fn configured_passphrase(config: &VykarConfig) -> Result<Option<Zeroizing<String>>> {
    if let Some(ref p) = config.encryption.passphrase {
        tracing::debug!(
            "using plaintext encryption.passphrase from config; prefer encryption.passcommand or VYKAR_PASSPHRASE"
        );
        return Ok(Some(Zeroizing::new(p.clone())));
    }

    if let Some(ref cmd) = config.encryption.passcommand {
        let mut command = shell::command_for_script(cmd);
        command.env_remove("VYKAR_PASSPHRASE");
        let output = shell::run_command_with_timeout(&mut command, PASSCOMMAND_TIMEOUT)
            .map_err(VykarError::Io)?;

        if !output.status.success() {
            return Err(VykarError::Config(format!(
                "passcommand failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let mut raw = String::from_utf8(output.stdout)
            .map_err(|e| VykarError::Config(format!("passcommand output is not UTF-8: {e}")))?;
        let pass = Zeroizing::new(raw.trim().to_string());
        raw.zeroize();

        if pass.is_empty() {
            return Err(VykarError::Config(
                "passcommand returned an empty passphrase".into(),
            ));
        }

        return Ok(Some(pass));
    }

    if let Some(pass) = take_env_passphrase() {
        return Ok(Some(pass));
    }

    Ok(None)
}

/// Cache for `VYKAR_PASSPHRASE`: `None` = not yet read, `Some(v)` = already consumed.
static ENV_PASSPHRASE: Mutex<Option<Option<Zeroizing<String>>>> = Mutex::new(None);

/// Read `VYKAR_PASSPHRASE` from the process environment on first call,
/// remove it from the environment, and cache the value for subsequent calls.
fn take_env_passphrase() -> Option<Zeroizing<String>> {
    let mut cache = ENV_PASSPHRASE.lock().unwrap();
    if let Some(ref cached) = *cache {
        return cached.clone();
    }
    let val = std::env::var("VYKAR_PASSPHRASE")
        .ok()
        .filter(|s| !s.is_empty());
    if val.is_some() {
        // Remove from env to reduce exposure window.
        // Safety: called during single-threaded startup before any thread pool.
        #[allow(unused_unsafe)]
        unsafe {
            std::env::remove_var("VYKAR_PASSPHRASE");
        }
    }
    let result = val.map(Zeroizing::new);
    *cache = Some(result.clone());
    result
}

/// Reset the cached env passphrase. Only used by tests.
#[cfg(test)]
pub(crate) fn reset_env_passphrase_cache() {
    *ENV_PASSPHRASE.lock().unwrap() = None;
}

pub fn resolve_passphrase<F>(
    config: &VykarConfig,
    label: Option<&str>,
    mut prompt: F,
) -> Result<Option<Zeroizing<String>>>
where
    F: FnMut(PassphrasePrompt) -> Result<Option<Zeroizing<String>>>,
{
    if config.encryption.mode == EncryptionModeConfig::None {
        return Ok(None);
    }

    if let Some(pass) = configured_passphrase(config)? {
        return Ok(Some(pass));
    }

    prompt(PassphrasePrompt {
        repository_label: label.map(|s| s.to_string()),
        repository_url: config.repository.url.clone(),
        timeout_seconds: config.schedule.passphrase_prompt_timeout_seconds,
    })
}
