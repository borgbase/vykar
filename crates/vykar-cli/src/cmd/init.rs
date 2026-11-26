use vykar_core::commands;
use vykar_core::config::VykarConfig;
use vykar_core::repo::identity;

use crate::error::CliResult;
use crate::passphrase::get_init_passphrase;

pub(crate) fn run_init(config: &VykarConfig, label: Option<&str>) -> CliResult<()> {
    let passphrase = get_init_passphrase(config, label)?;

    let repo = commands::init::run(config, passphrase.as_deref().map(|s| s.as_str()))?;
    println!("Repository initialized at: {}", config.repository.url);
    println!("Encryption mode: {}", repo.config().encryption.as_str());
    if let Some(reason) = repo.single_writer_reason() {
        println!(
            "Commit mode: single-writer ({reason}); backups commit one at a time. \
             `vykar upgrade --commit-mode multi` switches once the storage passes the check."
        );
    }

    let fingerprint =
        identity::compute_fingerprint(&repo.config().id, repo.crypto().chunk_hasher().key());
    eprintln!("Repository fingerprint: {}", hex::encode(fingerprint));

    Ok(())
}
