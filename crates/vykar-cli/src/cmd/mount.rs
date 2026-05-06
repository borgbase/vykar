use vykar_core::commands;
use vykar_core::commands::mount::MountProgressEvent;
use vykar_core::config::VykarConfig;

use crate::error::CliResult;
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_mount(
    config: &VykarConfig,
    label: Option<&str>,
    snapshot_name: Option<String>,
    address: String,
    cache_size: usize,
    source_filter: &[String],
) -> CliResult<()> {
    with_repo_passphrase(config, label, |passphrase| {
        let mut on_progress = |event: MountProgressEvent| match event {
            MountProgressEvent::LoadingSnapshots => {
                eprintln!("Loading snapshot data...");
            }
            MountProgressEvent::SnapshotLoaded { name, item_count } => {
                eprintln!("Loaded {item_count} items from snapshot '{name}'");
            }
            MountProgressEvent::Serving { address } => {
                eprintln!("Serving on http://{address}");
                eprintln!("  Browse in browser:  http://{address}");
                eprintln!("  WebDAV (Finder):    Go → Connect to Server → http://{address}");
                eprintln!("Press Ctrl+C to stop.");
            }
            MountProgressEvent::ShuttingDown => {
                eprintln!("\nShutting down.");
            }
        };
        commands::mount::run_with_progress(
            config,
            passphrase,
            snapshot_name.as_deref(),
            &address,
            cache_size,
            source_filter,
            Some(&mut on_progress),
            None,
        )?;
        Ok(())
    })?;

    Ok(())
}
