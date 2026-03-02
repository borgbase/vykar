use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_mount(
    config: &VykarConfig,
    label: Option<&str>,
    snapshot_name: Option<String>,
    address: String,
    cache_size: usize,
    source_filter: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    with_repo_passphrase(config, label, |passphrase| {
        commands::mount::run(
            config,
            passphrase,
            snapshot_name.as_deref(),
            &address,
            cache_size,
            source_filter,
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    Ok(())
}
