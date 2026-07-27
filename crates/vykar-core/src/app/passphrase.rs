// Env removal of VYKAR_PASSPHRASE during single-threaded startup; SAFETY per block.
#![allow(unsafe_code)]
#![allow(
    clippy::duration_suboptimal_units,
    clippy::missing_errors_doc,
    clippy::option_option,
    clippy::redundant_closure_for_method_calls
)]

use std::sync::Mutex;
use std::time::Duration;

use zeroize::{Zeroize, Zeroizing};

use crate::config::{EncryptionModeConfig, VykarConfig};
use crate::platform::shell;
use vykar_types::error::{Result, VykarError};

/// Default timeout for passcommand execution (60 seconds).
const PASSCOMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// Context handed to an interactive passphrase prompt callback.
///
/// Deliberately carries no timeout: neither the CLI nor the GUI prompt can be
/// timed out (`schedule.passphrase_prompt_timeout_seconds` is accepted for
/// config compatibility but ignored).
#[derive(Debug, Clone)]
pub struct PassphrasePrompt {
    pub repository_label: Option<String>,
    pub repository_url: String,
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
        // A passcommand has no meaningful stdin — its *output* is the
        // passphrase. Inheriting the parent's fd 0 hands a GUI-launched child
        // whatever launchd left there; redirecting from /dev/null gives any
        // script that reads stdin a clean EOF instead of a blocking read.
        command.stdin(std::process::Stdio::null());
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
    let mut cache = ENV_PASSPHRASE
        .lock()
        .expect("passphrase cache lock not poisoned");
    if let Some(ref cached) = *cache {
        return cached.clone();
    }
    let val = std::env::var("VYKAR_PASSPHRASE")
        .ok()
        .filter(|s| !s.is_empty());
    if val.is_some() {
        // SAFETY: called during single-threaded startup before any thread
        // pool spawns; no concurrent env reads/writes can race here.
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
    *ENV_PASSPHRASE
        .lock()
        .expect("passphrase cache lock not poisoned") = None;
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
    })
}

/// Which of the two `init` prompts the callback is being asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitPromptStage {
    /// "Enter new passphrase".
    Enter,
    /// "Confirm passphrase" — must match the [`InitPromptStage::Enter`] value.
    Confirm,
}

/// Outcome of resolving the passphrase for a repository that is about to be
/// initialized.
///
/// Cancellation is a distinct variant rather than an error: dismissing the
/// prompt means "don't create the repository", which no front end should
/// report as a failure.
#[derive(Debug)]
pub enum InitPassphrase {
    /// `encryption.mode: none` — the repository takes no passphrase.
    NotRequired,
    Provided(Zeroizing<String>),
    /// The user dismissed either prompt.
    Cancelled,
}

/// Resolve the passphrase to initialize a repository with.
///
/// Resolution order:
/// 1. `pre_resolved_configured` — a value the caller already obtained from a
///    **configured** source while probing the repository. Provenance matters:
///    only `encryption.passphrase` / `encryption.passcommand` qualify, never a
///    passphrase the user typed at the probe prompt. A probe typo reused here
///    would lock the new repository behind it, which is exactly what the
///    enter-and-confirm pair below exists to prevent.
/// 2. A configured source resolved now, including `VYKAR_PASSPHRASE`. Values
///    from these sources skip confirmation — there is nothing to mistype.
/// 3. Interactive enter-and-confirm.
///
/// The passphrase must be non-empty **whatever source it came from**, and the
/// two interactive entries must match; both are reported as typed errors
/// ([`VykarError::EmptyPassphrase`], [`VykarError::PassphraseMismatch`]) rather
/// than a generic string. Dismissing *either* prompt yields
/// [`InitPassphrase::Cancelled`].
///
/// The emptiness check covers the configured sources too, not just the prompt:
/// `encryption.passphrase: ""` deserializes to `Some("")` (the strict-string
/// deserializer rejects nulls, not empty strings), so without it a config typo
/// would silently create an encrypted repository whose key is derived from an
/// empty passphrase.
pub fn resolve_init_passphrase<F>(
    config: &VykarConfig,
    label: Option<&str>,
    pre_resolved_configured: Option<Zeroizing<String>>,
    mut prompt: F,
) -> Result<InitPassphrase>
where
    F: FnMut(InitPromptStage, &PassphrasePrompt) -> Result<Option<Zeroizing<String>>>,
{
    if config.encryption.mode == EncryptionModeConfig::None {
        return Ok(InitPassphrase::NotRequired);
    }

    let provided = |pass: Zeroizing<String>| {
        if pass.is_empty() {
            Err(VykarError::EmptyPassphrase)
        } else {
            Ok(InitPassphrase::Provided(pass))
        }
    };

    if let Some(pass) = pre_resolved_configured {
        return provided(pass);
    }

    if let Some(pass) = configured_passphrase(config)? {
        return provided(pass);
    }

    let ctx = PassphrasePrompt {
        repository_label: label.map(|s| s.to_string()),
        repository_url: config.repository.url.clone(),
    };

    let Some(first) = prompt(InitPromptStage::Enter, &ctx)? else {
        return Ok(InitPassphrase::Cancelled);
    };
    // Checked before the confirm prompt so a blank first entry does not make
    // the user type it out twice to learn it was rejected.
    if first.is_empty() {
        return Err(VykarError::EmptyPassphrase);
    }

    let Some(second) = prompt(InitPromptStage::Confirm, &ctx)? else {
        return Ok(InitPassphrase::Cancelled);
    };
    if *first != *second {
        return Err(VykarError::PassphraseMismatch);
    }

    provided(first)
}
