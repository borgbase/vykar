use serde::Serialize;

use vykar_core::commands;
use vykar_core::config::VykarConfig;
use vykar_core::repo::identity;

use crate::error::CliResult;
use crate::output::{Output, Report};
use crate::passphrase::get_init_passphrase;

pub(crate) fn run_init(
    config: &VykarConfig,
    label: Option<&str>,
    out: &mut Output,
) -> CliResult<()> {
    let passphrase = get_init_passphrase(config, label)?;

    let repo = commands::init::run(config, passphrase.as_deref().map(|s| s.as_str()))?;
    let fingerprint =
        identity::compute_fingerprint(&repo.config.id, repo.crypto.chunk_hasher().key());
    out.emit(&InitReport {
        url: &config.repository.url,
        encryption: repo.config.encryption.as_str(),
        fingerprint: hex::encode(fingerprint),
    })
}

#[derive(Serialize)]
struct InitReport<'a> {
    url: &'a str,
    encryption: &'static str,
    fingerprint: String,
}

impl Report for InitReport<'_> {
    const KEY: &'static str = "init";

    fn json(&self) -> impl Serialize {
        self
    }

    fn print_human(&self) {
        println!("Repository initialized at: {}", self.url);
        println!("Encryption mode: {}", self.encryption);
        eprintln!("Repository fingerprint: {}", self.fingerprint);
    }
}
