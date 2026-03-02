use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_check(
    config: &VykarConfig,
    label: Option<&str>,
    verify_data: bool,
    distrust_server: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if distrust_server && !verify_data {
        return Err(
            "--distrust-server requires --verify-data (flag is meaningless without data verification)"
                .into(),
        );
    }

    let result = with_repo_passphrase(config, label, |passphrase| {
        let mut on_progress = |event: commands::check::CheckProgressEvent| match event {
            commands::check::CheckProgressEvent::SnapshotStarted {
                current,
                total,
                name,
            } => {
                eprintln!("[{current}/{total}] Checking snapshot '{name}'...");
            }
            commands::check::CheckProgressEvent::PacksExistencePhaseStarted { total_packs } => {
                eprintln!("Verifying existence of {total_packs} packs in storage...");
            }
            commands::check::CheckProgressEvent::PacksExistenceProgress {
                checked,
                total_packs,
            } => {
                eprintln!("  existence: {checked}/{total_packs} packs");
            }
            commands::check::CheckProgressEvent::ChunksDataPhaseStarted { total_chunks } => {
                eprintln!("Verifying data integrity of {total_chunks} chunks...");
            }
            commands::check::CheckProgressEvent::ChunksDataProgress {
                verified,
                total_chunks,
            } => {
                eprintln!("  verify-data: {verified}/{total_chunks}");
            }
            commands::check::CheckProgressEvent::ServerVerifyPhaseStarted { total_packs } => {
                eprintln!("Server-side verification of {total_packs} packs...");
            }
            commands::check::CheckProgressEvent::ServerVerifyProgress {
                verified,
                total_packs,
            } => {
                eprintln!("  server-verify: {verified}/{total_packs} packs");
            }
        };

        commands::check::run_with_progress(
            config,
            passphrase,
            verify_data,
            distrust_server,
            Some(&mut on_progress),
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    if !result.errors.is_empty() {
        println!("Errors found:");
        for err in &result.errors {
            println!("  [{}] {}", err.context, err.message);
        }
        println!();
    }

    println!(
        "Check complete: {} snapshots, {} items, {} packs existence-checked ({} chunks), {} chunks data-verified, {} errors",
        result.snapshots_checked,
        result.items_checked,
        result.packs_existence_checked,
        result.chunks_existence_checked,
        result.chunks_data_verified,
        result.errors.len(),
    );

    if !result.errors.is_empty() {
        return Err(format!("check found {} error(s)", result.errors.len()).into());
    }

    Ok(())
}
