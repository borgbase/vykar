use std::io::{self, BufRead, Write};

use vykar_core::commands;
use vykar_core::commands::check::{RepairAction, RepairMode, RepairPlan, RepairResult};
use vykar_core::config::VykarConfig;

use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_check(
    config: &VykarConfig,
    label: Option<&str>,
    verify_data: bool,
    distrust_server: bool,
    repair: bool,
    dry_run: bool,
    yes: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !repair {
        if distrust_server && !verify_data {
            return Err(
                "--distrust-server requires --verify-data (flag is meaningless without data verification)"
                    .into(),
            );
        }
        return run_check_readonly(config, label, verify_data, distrust_server);
    }

    // --repair mode
    if dry_run {
        // Plan only: show plan and exit.
        let result = with_repo_passphrase(config, label, |passphrase| {
            commands::check::run_with_repair(
                config,
                passphrase,
                verify_data,
                RepairMode::PlanOnly,
                Some(&mut make_progress_callback()),
            )
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
        })?;
        print_check_summary(&result.check_result);
        print_repair_plan(&result.plan);
        eprintln!("Dry run: no changes applied.");
        if !result.check_result.errors.is_empty() {
            return Err(
                format!("check found {} error(s)", result.check_result.errors.len()).into(),
            );
        }
        return Ok(());
    }

    if yes {
        // --yes: apply directly without confirmation.
        let result = with_repo_passphrase(config, label, |passphrase| {
            commands::check::run_with_repair(
                config,
                passphrase,
                verify_data,
                RepairMode::Apply,
                Some(&mut make_progress_callback()),
            )
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
        })?;
        print_check_summary(&result.check_result);
        print_repair_plan(&result.plan);
        print_repair_result(&result);
        return Ok(());
    }

    // Interactive: plan first, then confirm, then apply.
    let plan_result = with_repo_passphrase(config, label, |passphrase| {
        commands::check::run_with_repair(
            config,
            passphrase,
            verify_data,
            RepairMode::PlanOnly,
            Some(&mut make_progress_callback()),
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    print_check_summary(&plan_result.check_result);
    print_repair_plan(&plan_result.plan);

    if !plan_result.plan.has_data_loss
        && (plan_result.plan.actions.is_empty()
            || (plan_result.plan.actions.len() == 1
                && matches!(plan_result.plan.actions[0], RepairAction::RebuildRefcounts)))
    {
        // Tier 1 only — apply without prompt.
        eprintln!("No data-loss actions; applying safe repairs...");
        let result = with_repo_passphrase(config, label, |passphrase| {
            commands::check::run_with_repair(
                config,
                passphrase,
                verify_data,
                RepairMode::Apply,
                Some(&mut make_progress_callback()),
            )
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
        })?;
        print_repair_result(&result);
        return Ok(());
    }

    if plan_result.plan.has_data_loss {
        eprint!("Type 'repair' to proceed: ");
        io::stderr().flush()?;
        let mut input = String::new();
        io::stdin().lock().read_line(&mut input)?;
        if input.trim() != "repair" {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    // Re-scan and apply under maintenance lock.
    let result = with_repo_passphrase(config, label, |passphrase| {
        commands::check::run_with_repair(
            config,
            passphrase,
            verify_data,
            RepairMode::Apply,
            Some(&mut make_progress_callback()),
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;
    print_repair_result(&result);

    Ok(())
}

fn run_check_readonly(
    config: &VykarConfig,
    label: Option<&str>,
    verify_data: bool,
    distrust_server: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = with_repo_passphrase(config, label, |passphrase| {
        commands::check::run_with_progress(
            config,
            passphrase,
            verify_data,
            distrust_server,
            Some(&mut make_progress_callback()),
            100,   // standalone always 100%
            false, // don't update daemon's full_every timer
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    print_check_summary(&result);

    if !result.errors.is_empty() {
        return Err(format!("check found {} error(s)", result.errors.len()).into());
    }

    Ok(())
}

pub(crate) fn format_check_progress(event: &commands::check::CheckProgressEvent) {
    match event {
        commands::check::CheckProgressEvent::SnapshotStarted {
            current,
            total,
            name,
        } => eprintln!("[{current}/{total}] Checking snapshot '{name}'..."),
        commands::check::CheckProgressEvent::PacksExistencePhaseStarted { total_packs } => {
            eprintln!("Verifying existence of {total_packs} packs in storage...");
        }
        commands::check::CheckProgressEvent::PacksExistenceProgress {
            checked,
            total_packs,
        } => eprintln!("  existence: {checked}/{total_packs} packs"),
        commands::check::CheckProgressEvent::ChunksDataPhaseStarted { total_chunks } => {
            eprintln!("Verifying data integrity of {total_chunks} chunks...");
        }
        commands::check::CheckProgressEvent::ChunksDataProgress {
            verified,
            total_chunks,
        } => eprintln!("  verify-data: {verified}/{total_chunks}"),
        commands::check::CheckProgressEvent::ServerVerifyPhaseStarted { total_packs } => {
            eprintln!("Server-side verification of {total_packs} packs...");
        }
        commands::check::CheckProgressEvent::ServerVerifyProgress {
            verified,
            total_packs,
        } => eprintln!("  server-verify: {verified}/{total_packs} packs"),
    }
}

fn make_progress_callback() -> impl FnMut(commands::check::CheckProgressEvent) {
    |event| format_check_progress(&event)
}

pub(crate) fn print_check_summary(result: &commands::check::CheckResult) {
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
}

fn print_repair_plan(plan: &RepairPlan) {
    if plan.actions.is_empty() {
        println!("\nRepair plan: no actions needed.");
        return;
    }

    println!("\nRepair plan:");

    // Tier 1 (safe)
    let safe_actions: Vec<&RepairAction> = plan
        .actions
        .iter()
        .filter(|a| matches!(a, RepairAction::RebuildRefcounts))
        .collect();
    if !safe_actions.is_empty() {
        println!("  Safe:");
        for a in safe_actions {
            println!("    - {}", format_repair_action(a));
        }
    }

    // Tier 2 (data loss)
    let loss_actions: Vec<&RepairAction> = plan
        .actions
        .iter()
        .filter(|a| !matches!(a, RepairAction::RebuildRefcounts))
        .collect();
    if !loss_actions.is_empty() {
        println!("  Will remove (data loss):");
        for a in &loss_actions {
            println!("    - {}", format_repair_action(a));
        }
    }
}

fn format_repair_action(action: &RepairAction) -> String {
    match action {
        RepairAction::RemoveCorruptSnapshot {
            name, snapshot_id, ..
        } => match name {
            Some(name) => format!("Remove corrupted snapshot blob '{name}'"),
            None => format!("Remove corrupted snapshot blob {snapshot_id}"),
        },
        RepairAction::RemoveInvalidSnapshotKey { storage_key } => {
            format!("Remove invalid snapshot key: {storage_key}")
        }
        RepairAction::RemoveDanglingIndexEntries {
            pack_id,
            chunk_count,
        } => {
            format!("Remove {chunk_count} dangling index entries for missing pack {pack_id}")
        }
        RepairAction::RemoveCorruptPack {
            pack_id,
            chunk_count,
        } => {
            format!("Remove {chunk_count} index entries for corrupt pack {pack_id}")
        }
        RepairAction::RemoveCorruptChunks { pack_id, chunk_ids } => {
            format!(
                "Remove {} corrupt chunk(s) from pack {pack_id}",
                chunk_ids.len()
            )
        }
        RepairAction::RemoveDanglingSnapshot {
            snapshot_name,
            missing_chunks,
        } => {
            format!("Remove snapshot '{snapshot_name}' with {missing_chunks} unresolvable chunk(s)")
        }
        RepairAction::RebuildRefcounts => "Rebuild chunk refcounts from surviving snapshots".into(),
    }
}

fn print_repair_result(result: &RepairResult) {
    if result.applied.is_empty() {
        println!("\nNo repairs applied.");
        return;
    }

    println!("\nRepairs applied:");
    for action in &result.applied {
        println!("  - {}", format_repair_action(action));
    }

    if !result.repair_errors.is_empty() {
        eprintln!("\nRepair errors:");
        for err in &result.repair_errors {
            eprintln!("  {err}");
        }
    }
}
