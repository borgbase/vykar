use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::passphrase::get_init_passphrase;

pub(crate) fn run_init(
    config: &VykarConfig,
    label: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_init_passphrase(config, label)?;

    let repo = commands::init::run(config, passphrase.as_deref().map(|s| s.as_str()))?;
    println!("Repository initialized at: {}", config.repository.url);
    println!("Encryption mode: {}", repo.config.encryption.as_str());
    Ok(())
}
