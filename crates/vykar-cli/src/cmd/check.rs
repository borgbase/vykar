use std::io::{self, BufRead, Write};

use vykar_core::commands;
use vykar_core::commands::check::{
    IndexLoss, IndexRecovery, RepairAction, RepairMode, RepairPlan, RepairResult,
};
use vykar_core::config::VykarConfig;
use vykar_core::repo::control::CancelToken;

use crate::error::{CliError, CliResult};
use crate::passphrase::with_repo_passphrase;

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_check(
    config: &VykarConfig,
    label: Option<&str>,
    verify_data: bool,
    distrust_server: bool,
    repair: bool,
    dry_run: bool,
    yes: bool,
    shutdown: &CancelToken,
) -> CliResult<()> {
    if !repair {
        if distrust_server && !verify_data {
            return Err(CliError::from(
                "--distrust-server requires --verify-data (flag is meaningless without data verification)",
            ));
        }
        return run_check_readonly(config, label, verify_data, distrust_server, shutdown);
    }

    // --repair mode
    if dry_run {
        // Plan only: show plan and exit.
        let result = run_repair(config, label, verify_data, RepairMode::PlanOnly, shutdown)?;
        print_check_summary(&result.check_result);
        print_repair_plan(&result.plan);
        eprintln!("Dry run: no changes applied.");
        if !result.check_result.errors.is_empty() {
            return Err(CliError::from(format!(
                "check found {} error(s)",
                result.check_result.errors.len()
            )));
        }
        return Ok(());
    }

    if yes {
        // --yes: apply directly without confirmation.
        let result = run_repair(config, label, verify_data, RepairMode::Apply, shutdown)?;
        print_check_summary(&result.check_result);
        print_repair_plan(&result.plan);
        print_repair_result(&result);
        return Ok(());
    }

    // Interactive: plan first, then confirm, then apply.
    let plan_result = run_repair(config, label, verify_data, RepairMode::PlanOnly, shutdown)?;

    print_check_summary(&plan_result.check_result);
    print_repair_plan(&plan_result.plan);

    if !plan_result.plan.has_data_loss && plan_result.plan.actions.iter().all(is_safe_action) {
        // Tier 1 only — apply without prompt.
        eprintln!("No data-loss actions; applying safe repairs...");
        let mode = apply_mode_after(&plan_result.plan);
        let result = run_repair(config, label, verify_data, mode, shutdown)?;
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
    let mode = apply_mode_after(&plan_result.plan);
    let result = run_repair(config, label, verify_data, mode, shutdown)?;
    print_repair_result(&result);

    Ok(())
}

/// The Apply mode after the user saw `plan`: the plan rebuilt under the lock
/// may not add data loss or come from an index read differently.
fn apply_mode_after(plan: &RepairPlan) -> RepairMode {
    RepairMode::ApplyShown {
        has_data_loss: plan.has_data_loss,
        index_recovery: plan.index_recovery,
    }
}

/// One scan-and-repair pass. `RepairMode::Apply` re-scans from scratch under the
/// maintenance lock, so an earlier `PlanOnly` pass never carries state forward.
fn run_repair(
    config: &VykarConfig,
    label: Option<&str>,
    verify_data: bool,
    mode: RepairMode,
    shutdown: &CancelToken,
) -> CliResult<RepairResult> {
    with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::check::run_with_repair(
            config,
            passphrase,
            verify_data,
            mode,
            Some(&mut make_progress_callback()),
            shutdown,
        )?)
    })
}

fn run_check_readonly(
    config: &VykarConfig,
    label: Option<&str>,
    verify_data: bool,
    distrust_server: bool,
    shutdown: &CancelToken,
) -> CliResult<()> {
    let result = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::check::run_with_progress(
            config,
            passphrase,
            verify_data,
            distrust_server,
            Some(&mut make_progress_callback()),
            100,   // standalone always 100%
            false, // don't update daemon's full_every timer
            shutdown,
        )?)
    })?;

    print_check_summary(&result);

    if !result.errors.is_empty() {
        return Err(CliError::from(format!(
            "check found {} error(s)",
            result.errors.len()
        )));
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
            missing,
        } => {
            if *missing > 0 {
                eprintln!("  existence: {checked}/{total_packs} packs ({missing} missing)");
            } else {
                eprintln!("  existence: {checked}/{total_packs} packs");
            }
        }
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

    // Unencrypted repositories have no key file; "0 key copies" would read
    // like a finding.
    let key_copies = match result.key_files_checked {
        Some(n) => format!("{n} key copies, "),
        None => String::new(),
    };
    println!(
        "Check complete: {}{} snapshots, {} items, {} packs existence-checked ({} chunks), {} chunks data-verified, {} errors",
        key_copies,
        result.snapshots_checked,
        result.items_checked,
        result.packs_existence_checked,
        result.chunks_existence_checked,
        result.chunks_data_verified,
        result.errors.len(),
    );
}

/// Tier-1 actions: applied without the `type 'repair'` prompt because they
/// cannot lose data.
///
/// `RestoreKeyCopy` qualifies because `check` only runs after
/// `Repo::open` succeeded — a key was therefore established, and the
/// action copies it onto a copy that is missing or provably not it. The
/// unresolvable case, where two candidate keys cannot be told apart, fails at
/// open and never reaches a repair plan at all.
fn is_safe_action(action: &RepairAction) -> bool {
    matches!(
        action,
        RepairAction::RemoveUnreferencedEntries
            | RepairAction::RestoreKeyCopy { .. }
            | RepairAction::WritePackMaps { .. }
    )
}

fn print_repair_plan(plan: &RepairPlan) {
    print_index_recovery(plan);
    if plan.actions.is_empty() {
        println!("\nRepair plan: no actions needed.");
        return;
    }

    println!("\nRepair plan:");

    // Tier 1 (safe)
    let safe_actions: Vec<&RepairAction> =
        plan.actions.iter().filter(|a| is_safe_action(a)).collect();
    if !safe_actions.is_empty() {
        println!("  Safe:");
        for a in safe_actions {
            println!("    - {}", format_repair_action(a));
        }
    }

    // Tier 2 (data loss)
    let loss_actions: Vec<&RepairAction> =
        plan.actions.iter().filter(|a| !is_safe_action(a)).collect();
    if !loss_actions.is_empty() {
        println!("  Will remove (data loss):");
        for a in &loss_actions {
            println!("    - {}", format_repair_action(a));
        }
    }
}

/// How the index was read, when not simply in full.
fn print_index_recovery(plan: &RepairPlan) {
    for line in index_recovery_lines(plan) {
        println!("{line}");
    }
}

/// Uncovered packs listed one per line before the rest are counted.
const MAX_UNCOVERED_LINES: usize = 20;

/// The lines [`print_index_recovery`] prints.
fn index_recovery_lines(plan: &RepairPlan) -> Vec<String> {
    let mut lines = Vec::new();
    let (cause, counts) = match plan.index_recovery {
        IndexRecovery::Complete => return lines,
        IndexRecovery::RebuiltFromPacks { cause, counts } => (cause, counts),
    };
    let cause = match cause {
        IndexLoss::HeadMissing => "the index head is missing",
        IndexLoss::HeadUnreadable => "the index head cannot be read",
        IndexLoss::ChainDamaged => "the index chain is damaged",
    };
    lines.push(format!(
        "\nIndex: {cause}; the index is rebuilt from the packs: {} pack(s), {} from pack \
         maps, {} from pack trailers, {} from this machine's index cache.",
        counts.packs, counts.from_maps, counts.from_trailers, counts.from_local_tables
    ));
    if counts.damaged_maps > 0 {
        lines.push(format!(
            "  {} pack map(s) cannot be read and are left out.",
            counts.damaged_maps
        ));
    }
    if !plan.uncovered_packs.is_empty() {
        lines.push(format!(
            "  {} pack(s) describe none of their contents and are not in the rebuilt index:",
            plan.uncovered_packs.len()
        ));
        for pack in plan.uncovered_packs.iter().take(MAX_UNCOVERED_LINES) {
            lines.push(format!("    - {}: {}", pack.pack_id, pack.reason));
        }
        if plan.uncovered_packs.len() > MAX_UNCOVERED_LINES {
            lines.push(format!(
                "    ... and {} more",
                plan.uncovered_packs.len() - MAX_UNCOVERED_LINES
            ));
        }
    }
    if plan.has_data_loss {
        lines.push(
            "  Snapshots whose data the rebuilt index cannot locate are removed by this \
             repair and listed below; packs only they used are deleted by the next \
             `vykar compact`."
                .into(),
        );
    }
    lines
}

fn format_repair_action(action: &RepairAction) -> String {
    match action {
        RepairAction::RestoreKeyCopy { from, to } => {
            format!("Restore repository key copy {to} from {from}")
        }
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
        RepairAction::DropItemsFromSnapshot {
            snapshot_name,
            item_indices,
            dropped_paths,
            reasons,
            ..
        } => {
            const MAX_PATHS: usize = 10;
            let n = item_indices.len();
            let mut out = format!(
                "Drop {n} item(s) from snapshot '{snapshot_name}' (snapshot kept, new id):"
            );
            for (path, reason) in dropped_paths.iter().zip(reasons.iter()).take(MAX_PATHS) {
                out.push_str(&format!("\n      - {path}  ({reason})"));
            }
            if n > MAX_PATHS {
                out.push_str(&format!("\n      ... and {} more", n - MAX_PATHS));
            }
            out
        }
        RepairAction::WritePackMaps { pack_ids } => format!(
            "Write pack maps for {} pack(s) whose index entries are not described by the packs \
             themselves",
            pack_ids.len()
        ),
        RepairAction::RemoveUnreferencedEntries => {
            "Remove index entries that no surviving snapshot references".into()
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use vykar_core::commands::check::{RebuildCounts, UncoveredPack};
    use vykar_types::pack_id::PackId;

    fn rebuilt_plan(uncovered: usize) -> RepairPlan {
        let uncovered_packs: Vec<UncoveredPack> = (0..uncovered)
            .map(|i| UncoveredPack {
                pack_id: PackId::from_bytes([i as u8; 32]),
                reason: "no trailer and no pack map".into(),
            })
            .collect();
        RepairPlan {
            actions: vec![RepairAction::RemoveUnreferencedEntries],
            has_data_loss: uncovered > 0,
            index_recovery: IndexRecovery::RebuiltFromPacks {
                cause: IndexLoss::HeadMissing,
                counts: RebuildCounts {
                    packs: 30,
                    from_trailers: 30 - uncovered,
                    uncovered,
                    ..RebuildCounts::default()
                },
            },
            uncovered_packs,
        }
    }

    #[test]
    fn index_recovery_lines_for_a_rebuild() {
        let lines = index_recovery_lines(&rebuilt_plan(0));
        assert_eq!(
            lines,
            vec![
                "\nIndex: the index head is missing; the index is rebuilt from the packs: 30 \
                 pack(s), 0 from pack maps, 30 from pack trailers, 0 from this machine's index \
                 cache."
                    .to_string()
            ]
        );

        let lines = index_recovery_lines(&rebuilt_plan(25));
        assert!(
            lines[1].starts_with("  25 pack(s) describe none of their contents"),
            "{lines:?}"
        );
        let listed = lines.iter().filter(|l| l.starts_with("    - ")).count();
        assert_eq!(listed, MAX_UNCOVERED_LINES);
        assert!(
            lines[2].ends_with(": no trailer and no pack map"),
            "{lines:?}"
        );
        assert!(
            lines.contains(&"    ... and 5 more".to_string()),
            "{lines:?}"
        );
        assert!(
            lines.last().unwrap().contains("are removed by this repair"),
            "{lines:?}"
        );
    }

    #[test]
    fn apply_mode_after_carries_the_shown_plan() {
        for has_data_loss in [false, true] {
            let recovery = IndexRecovery::RebuiltFromPacks {
                cause: IndexLoss::ChainDamaged,
                counts: RebuildCounts {
                    packs: 3,
                    from_trailers: 3,
                    ..RebuildCounts::default()
                },
            };
            let plan = RepairPlan {
                actions: vec![RepairAction::RemoveUnreferencedEntries],
                has_data_loss,
                index_recovery: recovery,
                uncovered_packs: Vec::new(),
            };
            assert_eq!(
                apply_mode_after(&plan),
                RepairMode::ApplyShown {
                    has_data_loss,
                    index_recovery: recovery,
                }
            );
        }
    }
}
