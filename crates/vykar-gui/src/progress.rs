use std::time::Instant;

use vykar_common::display::{format_bytes, format_count, truncate_middle};
use vykar_core::app::operations::{CycleStep, StepOutcome};
use vykar_core::commands::backup::BackupProgressEvent;
use vykar_core::commands::check::CheckProgressEvent;

const THROTTLE_MS: u128 = 250;

pub struct BackupStatusTracker {
    repo_name: String,
    last_update: Instant,
    current_file: Option<String>,
}

impl BackupStatusTracker {
    pub fn new(repo_name: String) -> Self {
        Self {
            repo_name,
            last_update: Instant::now() - std::time::Duration::from_millis(THROTTLE_MS as u64),
            current_file: None,
        }
    }

    /// Returns Some(status_string) if enough time has elapsed or the event is significant.
    /// Returns None if the update should be skipped (throttled).
    pub fn format(&mut self, event: &BackupProgressEvent) -> Option<String> {
        match event {
            BackupProgressEvent::SourceStarted { source_path } => {
                self.last_update = Instant::now();
                Some(format!(
                    "[{}] backing up {}...",
                    self.repo_name, source_path
                ))
            }
            BackupProgressEvent::FileStarted { path } => {
                self.current_file = Some(path.clone());
                None
            }
            BackupProgressEvent::StatsUpdated {
                nfiles,
                original_size,
                current_file,
                ..  // errors, compressed_size, deduplicated_size
            } => {
                if let Some(f) = current_file {
                    self.current_file = Some(f.clone());
                }
                let now = Instant::now();
                if now.duration_since(self.last_update).as_millis() < THROTTLE_MS {
                    return None;
                }
                self.last_update = now;
                let file_suffix = match &self.current_file {
                    Some(f) => format!(" - {}", truncate_middle(f, 60)),
                    None => String::new(),
                };
                Some(format!(
                    "[{}] {} files, {}{}",
                    self.repo_name,
                    format_count(*nfiles),
                    format_bytes(*original_size),
                    file_suffix,
                ))
            }
            BackupProgressEvent::CommitStage { stage } => {
                self.last_update = Instant::now();
                Some(format!("[{}] committing: {}...", self.repo_name, stage))
            }
            _ => None,
        }
    }
}

pub fn format_check_status(repo_name: &str, event: &CheckProgressEvent) -> String {
    match event {
        CheckProgressEvent::SnapshotStarted {
            current,
            total,
            name,
        } => format!("[{repo_name}] checking snapshot {current}/{total}: {name}..."),
        CheckProgressEvent::PacksExistencePhaseStarted { total_packs } => {
            format!("[{repo_name}] verifying packs (0/{total_packs})...")
        }
        CheckProgressEvent::PacksExistenceProgress {
            checked,
            total_packs,
            missing,
        } => {
            if *missing > 0 {
                format!(
                    "[{repo_name}] verifying packs ({checked}/{total_packs}, {missing} missing)..."
                )
            } else {
                format!("[{repo_name}] verifying packs ({checked}/{total_packs})...")
            }
        }
        CheckProgressEvent::ChunksDataPhaseStarted { total_chunks } => {
            format!("[{repo_name}] verifying data (0/{total_chunks})...")
        }
        CheckProgressEvent::ChunksDataProgress {
            verified,
            total_chunks,
        } => format!("[{repo_name}] verifying data ({verified}/{total_chunks})..."),
        CheckProgressEvent::ServerVerifyPhaseStarted { total_packs } => {
            format!("[{repo_name}] verifying server packs (0/{total_packs})...")
        }
        CheckProgressEvent::ServerVerifyProgress {
            verified,
            total_packs,
        } => format!("[{repo_name}] verifying server packs ({verified}/{total_packs})..."),
    }
}

/// Format a step outcome for the GUI log. Returns empty string for Ok steps
/// (backup reports are logged separately with more detail).
pub fn format_step_outcome(repo_name: &str, step: CycleStep, outcome: &StepOutcome) -> String {
    let name = step.command_name();
    match outcome {
        StepOutcome::Ok => {
            if matches!(step, CycleStep::Backup) {
                // Backup details are logged via log_backup_report
                String::new()
            } else {
                format!("[{repo_name}] {name}: ok")
            }
        }
        StepOutcome::Partial => format!("[{repo_name}] {name}: ok (partial — some files skipped)"),
        StepOutcome::Skipped(reason) => format!("[{repo_name}] {name}: skipped ({reason})"),
        StepOutcome::Failed(e) => format!("[{repo_name}] {name}: FAILED: {e}"),
    }
}
