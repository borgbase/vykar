//! The bare `vykar` command and the daemon's cycle output: rendering of the
//! app service's cycle events on stdout/stderr.

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::atomic::Ordering;

use vykar_common::display::format_duration_seconds;
use vykar_core::app::AppService;
use vykar_core::app::operations::{CycleEvent, CycleStep, FullCycleResult, StepOutcome};
use vykar_core::app::passphrase::PassphrasePrompt;
use vykar_core::app::service::{AppEvent, CycleReport, CycleRequest};
use vykar_core::config::ResolvedRepo;

use crate::cmd;
use crate::cmd::check::{format_check_progress, print_check_summary};
use crate::dispatch::warn_if_untrusted_rest;
use crate::format::print_backup_stats;
use crate::progress::BackupProgressRenderer;
use crate::{EXIT_ERROR, EXIT_INTERRUPTED, EXIT_PARTIAL, signal};

/// Renders one repository's cycle at a time: step headers, backup progress,
/// step details and the summary table.
pub(crate) struct CycleRenderer {
    verbose: u8,
    is_tty: bool,
    show_progress: bool,
    backup: Option<BackupProgressRenderer>,
    started: Option<std::time::Instant>,
}

impl CycleRenderer {
    pub(crate) fn new(verbose: u8) -> Self {
        let is_tty = std::io::stderr().is_terminal();
        Self {
            verbose,
            is_tty,
            show_progress: is_tty || verbose > 0,
            backup: None,
            started: None,
        }
    }

    pub(crate) fn repo_started(&mut self) {
        self.started = Some(std::time::Instant::now());
    }

    pub(crate) fn on_cycle_event(&mut self, event: &CycleEvent) {
        match event {
            CycleEvent::StepStarted(step) => {
                eprintln!("==> Starting {}", step.command_name());
                if matches!(step, CycleStep::Backup) && self.show_progress {
                    self.backup = Some(BackupProgressRenderer::new(self.verbose, self.is_tty));
                }
            }
            CycleEvent::StepFinished(step, outcome) => {
                if matches!(step, CycleStep::Backup) {
                    if let Some(ref mut r) = self.backup {
                        r.finish();
                    }
                    self.backup = None;
                }
                if let StepOutcome::Failed(e) = outcome {
                    eprintln!("Error: {e}");
                }
            }
            CycleEvent::Backup(evt) => {
                if let Some(ref mut r) = self.backup {
                    r.on_event(evt.clone());
                }
            }
            CycleEvent::Check(evt) => {
                format_check_progress(evt);
            }
            // HookWarning: tracing::warn! already fired inside log_hook_errors.
            // Events are for GUI consumers only.
            CycleEvent::HookWarning { .. } => {}
            CycleEvent::StepWarning { step, message } => {
                // tracing::warn! already fired inside the step;
                // duplicate to stderr so CLI users see the warning
                // even without the tracing subscriber.
                eprintln!("warning: [{}] {message}", step.command_name());
            }
        }
    }

    pub(crate) fn repo_finished(&mut self, result: &FullCycleResult) {
        // Ensure renderer is cleaned up if cycle ended abruptly (e.g. shutdown).
        if let Some(ref mut r) = self.backup {
            r.finish();
        }
        self.backup = None;
        print_step_details(result);
        print_summary(&result.steps, self.started.take());
    }
}

/// The bare `vykar` command: one cycle over the selection, rendered on
/// stdout/stderr. Returns the core report; [`exit_code`] maps it.
pub(crate) fn run_bare_cycle(
    service: &mut AppService,
    repos: &[ResolvedRepo],
    request: &CycleRequest,
    verbose: u8,
    multi: bool,
    prompt: &mut dyn PassphrasePrompt,
) -> CycleReport {
    let mut renderer = CycleRenderer::new(verbose);
    service.run_cycle(
        repos,
        request,
        prompt,
        signal::shutdown_token(),
        &mut |event| match event {
            AppEvent::RepoStarted { repo, .. } => {
                if multi {
                    eprintln!("--- Repository: {} ---", repo.label_or_url());
                }
                warn_if_untrusted_rest(&repo.config, repo.label.as_deref());
                renderer.repo_started();
            }
            AppEvent::Cycle { event, .. } => renderer.on_cycle_event(&event),
            AppEvent::RepoFinished { repo, report } => {
                renderer.repo_finished(&report.result);
                if report.result.repo_unavailable
                    && let Some(reason) = report.skipped()
                {
                    eprintln!("Warning: skipping '{}' — {reason}", repo.label_or_url());
                }
                // An interrupt is reported once, by `exit_code`.
                if report.result.has_failures() && !signal::SHUTDOWN.load(Ordering::SeqCst) {
                    eprintln!("Error: one or more steps failed");
                }
            }
        },
    )
}

/// Interrupt, then any failure, then a partial backup, then success.
pub(crate) fn exit_code(report: &CycleReport) -> ExitCode {
    if signal::SHUTDOWN.load(Ordering::SeqCst) {
        eprintln!("Interrupted");
        return ExitCode::from(EXIT_INTERRUPTED);
    }
    if report.has_failures() {
        return ExitCode::from(EXIT_ERROR);
    }
    if report.had_partial() {
        return ExitCode::from(EXIT_PARTIAL);
    }
    ExitCode::SUCCESS
}

/// Print the detailed per-step output (matching what individual CLI wrappers print).
fn print_step_details(result: &FullCycleResult) {
    // Backup summaries
    if let Some(ref report) = result.backup_report {
        for created in &report.created {
            let stats = &created.stats;
            let paths_display = created.source_paths.join(", ");
            println!("Snapshot created: {}", created.snapshot_name);
            println!(
                "  Source: {paths_display} (label: {})",
                created.source_label
            );
            if stats.errors > 0 {
                eprintln!(
                    "Warning: {} file(s) could not be read and were excluded from the snapshot",
                    stats.errors
                );
            }
            print_backup_stats(stats);
        }
        for skipped in &report.skipped {
            eprintln!("Skipped source '{}': {}", skipped.label, skipped.reason);
        }
        for failed in &report.failed {
            eprintln!("Failed source '{}': {}", failed.label, failed.error);
        }
        if let Some(aborted) = &report.aborted {
            crate::format::print_aborted_run(aborted);
        }
    }

    // Prune stats
    if let Some(ref stats) = result.prune_stats {
        cmd::prune::print_prune_summary(stats);
    }

    // Compact stats
    if let Some(ref stats) = result.compact_stats {
        cmd::compact::print_compact_summary(stats);
    }

    // Check results (skip summary if check was skipped — the step outcome table already shows it)
    if let Some(ref result) = result.check_result
        && !result.skipped
    {
        print_check_summary(result);
    }
}

/// Prints the summary table.
fn print_summary(steps: &[(CycleStep, StepOutcome)], start: Option<std::time::Instant>) {
    let elapsed = start.map(|s| s.elapsed()).unwrap_or_default();
    eprintln!();
    eprintln!("=== Summary ===");
    for (step, result) in steps {
        let name = step.command_name();
        match result {
            StepOutcome::Ok => eprintln!("  {name:<12} ok"),
            StepOutcome::Partial => eprintln!("  {name:<12} ok (partial)"),
            StepOutcome::Failed(e) => {
                eprintln!("  {name:<12} FAILED: {e}");
            }
            StepOutcome::Skipped(reason) => eprintln!("  {name:<12} skipped ({reason})"),
        }
    }

    eprintln!(
        "  Duration:    {}",
        format_duration_seconds(elapsed.as_secs() as i64)
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passphrase::TtyPrompt;
    use vykar_core::testing::conformance;

    /// The bare command's cycle over the conformance fixture reports what the
    /// core cycle does. No prompt is reached: the passphrase is configured.
    #[test]
    fn conformance_bare_cycle() {
        crate::init_test_environment();
        let tmp = tempfile::tempdir().unwrap();
        let repos = conformance::fixture(tmp.path());

        let report = run_bare_cycle(
            &mut AppService::new(),
            &repos,
            &CycleRequest::default(),
            0,
            true,
            &mut TtyPrompt,
        );

        assert_eq!(
            conformance::summary(&report, &repos),
            conformance::EXPECTED_SUMMARY
        );
        assert_eq!(
            conformance::hook_logs(tmp.path()),
            conformance::EXPECTED_HOOKS
        );
        assert_eq!(exit_code(&report), ExitCode::SUCCESS);
    }
}
