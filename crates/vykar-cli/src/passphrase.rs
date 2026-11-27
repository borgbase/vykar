use std::io::IsTerminal;

use zeroize::Zeroizing;

use crate::error::{CliError, CliResult};
use crate::prompt::prompt_hidden;
use vykar_core::app::passphrase::{
    InitPassphrase, InitPromptStage, PassphrasePrompt, PromptContext, PromptReply,
    resolve_init_passphrase,
};
use vykar_core::config::VykarConfig;

/// The terminal prompt. A retry is asked only on a terminal: a passphrase
/// piped on stdin is read once, so a wrong one fails instead of consuming
/// further input.
pub(crate) struct TtyPrompt;

impl PassphrasePrompt for TtyPrompt {
    fn ask(
        &mut self,
        ctx: &PromptContext,
        retry_reason: Option<&str>,
    ) -> vykar_types::error::Result<PromptReply> {
        if let Some(reason) = retry_reason {
            if !std::io::stdin().is_terminal() {
                return Ok(PromptReply::Unavailable);
            }
            eprintln!("{reason}");
        }
        let text = match &ctx.repository_label {
            Some(l) => format!("Enter passphrase for '{l}': "),
            None => "Enter passphrase: ".to_string(),
        };
        Ok(PromptReply::Value(Zeroizing::new(prompt_hidden(&text)?)))
    }
}

pub(crate) fn get_init_passphrase(
    config: &VykarConfig,
    label: Option<&str>,
) -> CliResult<Option<Zeroizing<String>>> {
    // `encryption.passphrase` is first in the resolution order, so its presence
    // means it is the value being used.
    if config.encryption.passphrase.is_some() {
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
        InitPassphrase::Provided(p) => Ok(Some(p)),
        // Unreachable from a terminal: `prompt_hidden` never reports a
        // cancellation (Ctrl-C terminates the process, EOF is an I/O error).
        InitPassphrase::Cancelled => Err(CliError::from("passphrase entry cancelled")),
    }
}
