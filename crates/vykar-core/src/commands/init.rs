use crate::config::{EncryptionModeConfig, VykarConfig};
use crate::repo::{identity, EncryptionMode, Repository};
use crate::storage;
use vykar_crypto::select::{self, AutoAeadMode};
use vykar_types::error::Result;

/// Run `vykar init`.
pub fn run(config: &VykarConfig, passphrase: Option<&str>) -> Result<Repository> {
    let backend = storage::backend_from_config(&config.repository, config.limits.connections)?;

    let encryption = match config.encryption.mode {
        EncryptionModeConfig::None => EncryptionMode::None,
        EncryptionModeConfig::Auto => match select::select_best_aead() {
            AutoAeadMode::Aes256Gcm => EncryptionMode::Aes256Gcm,
            AutoAeadMode::Chacha20Poly1305 => EncryptionMode::Chacha20Poly1305,
        },
        EncryptionModeConfig::Aes256Gcm => EncryptionMode::Aes256Gcm,
        EncryptionModeConfig::Chacha20Poly1305 => EncryptionMode::Chacha20Poly1305,
    };

    let repo = Repository::init(
        backend,
        encryption,
        config.chunker.clone(),
        passphrase,
        Some(&config.repository),
        super::util::cache_dir_from_config(config),
    )?;

    // Pin the repository identity immediately to close the TOFU window.
    // Failure is non-fatal: the repo already exists on storage.
    if let Err(e) = identity::verify_or_pin(
        &config.repository.url,
        &repo.config.id,
        repo.crypto.chunk_hasher().key(),
        super::util::cache_dir_from_config(config).as_deref(),
        true, // always pin at init (no pre-existing pin to conflict with)
    ) {
        tracing::warn!("could not pin repository identity: {e}");
    }

    Ok(repo)
}
