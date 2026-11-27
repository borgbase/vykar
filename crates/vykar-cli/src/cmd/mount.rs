use vykar_core::commands::mount::{self, MountProgressEvent, MountRequest};
use vykar_core::repo::control::{CancelToken, FnMutSink};

use crate::error::{CliError, CliResult};
use vykar_core::app::passphrase::RepoPassphrase;

pub(crate) fn run_mount(
    snapshot_name: Option<&str>,
    address: &str,
    cache_size: usize,
    source_filter: &[String],
    cancel: &CancelToken,
    passphrase: &mut RepoPassphrase<'_>,
) -> CliResult<()> {
    let repo = passphrase.open::<CliError>(mount::open_options(), cancel)?;
    let on_progress = |event: MountProgressEvent| match event {
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
    let req = MountRequest {
        snapshot: snapshot_name,
        address,
        cache_size,
        source_filter,
    };
    mount::run_on_repo(repo, &req, Some(&FnMutSink::new(on_progress)), cancel)?;
    Ok(())
}
