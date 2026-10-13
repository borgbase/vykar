use crate::app::operations::{
    run_backup_selection, run_hooked_step, CycleEvent, CycleStep, StepOutcome,
};
use crate::config::HooksConfig;
use crate::testutil::cmd_append;
use vykar_types::error::VykarError;

use super::helpers::{init_repo, make_test_repo, resolved_repo, source_entry};

#[test]
fn run_backup_selection_rejects_empty_sources() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);
    let repo = resolved_repo(config, Vec::new());

    let err = run_backup_selection(&repo, &[], None, None, false, None)
        .err()
        .unwrap();
    assert!(matches!(err, VykarError::Config(msg) if msg.contains("no sources configured")));
}

#[test]
fn run_backup_selection_returns_created_source_report() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("a.txt"), b"backup-report").unwrap();

    let config = init_repo(&repo_dir);
    let sources = vec![source_entry(&source_dir, "src-a")];
    let repo = resolved_repo(config, sources.clone());

    let report = run_backup_selection(&repo, &sources, None, None, false, None).unwrap();
    assert_eq!(report.created.len(), 1);
    assert_eq!(report.created[0].source_label, "src-a");
    assert_eq!(report.created[0].source_paths.len(), 1);
    assert!(report.created[0].stats.nfiles > 0);
    assert_ne!(report.created[0].snapshot_name, "");
}

fn hooked_repo(log: &std::path::Path) -> crate::config::ResolvedRepo {
    let mut repo = make_test_repo("/tmp/repo", None);
    repo.repo_hooks = HooksConfig {
        hooks: [
            ("after", cmd_append(log, "after")),
            ("failed", cmd_append(log, "failed:{error}")),
            ("after_check", cmd_append(log, "after_check")),
            ("failed_check", cmd_append(log, "failed_check:{error}")),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), vec![v]))
        .collect(),
    };
    repo
}

fn log_lines(path: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim().to_string())
        .collect()
}

#[test]
fn hooked_step_classified_failure_fires_failed_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let repo = hooked_repo(&log);
    let mut steps = Vec::new();
    let mut events = Vec::new();

    let value = run_hooked_step(
        CycleStep::Check,
        &repo,
        &mut |e| events.push(e),
        &mut steps,
        |_| Ok(3usize),
        |n| StepOutcome::Failed(format!("check found {n} error(s)")),
    );

    assert_eq!(value, Some(3));
    assert!(matches!(
        &steps[..],
        [(CycleStep::Check, StepOutcome::Failed(_))]
    ));
    assert_eq!(
        log_lines(&log),
        vec![
            "failed_check:check found 3 error(s)",
            "failed:check found 3 error(s)"
        ]
    );
    assert!(matches!(
        events.first(),
        Some(CycleEvent::StepStarted(CycleStep::Check))
    ));
    assert!(matches!(
        events.last(),
        Some(CycleEvent::StepFinished(CycleStep::Check, _))
    ));
}

#[test]
fn hooked_step_skipped_fires_after_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let repo = hooked_repo(&log);
    let mut steps = Vec::new();

    run_hooked_step(
        CycleStep::Check,
        &repo,
        &mut |_| {},
        &mut steps,
        |_| Ok(()),
        |_| StepOutcome::Skipped("check not due".into()),
    );

    assert!(matches!(
        &steps[..],
        [(CycleStep::Check, StepOutcome::Skipped(_))]
    ));
    assert_eq!(log_lines(&log), vec!["after_check", "after"]);
}

#[cfg(unix)]
#[test]
fn hooked_step_before_failure_skips_action_and_warns_before_finish() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let mut repo = hooked_repo(&log);
    // A failing `failed` hook surfaces as a warning ahead of StepFinished.
    repo.global_hooks = HooksConfig {
        hooks: [
            ("before_prune".to_string(), vec!["false".to_string()]),
            ("failed".to_string(), vec!["false".to_string()]),
        ]
        .into(),
    };
    let mut steps = Vec::new();
    let mut events = Vec::new();
    let mut action_ran = false;

    let value: Option<()> = run_hooked_step(
        CycleStep::Prune,
        &repo,
        &mut |e| events.push(e),
        &mut steps,
        |_| {
            action_ran = true;
            Ok(())
        },
        |_| StepOutcome::Ok,
    );

    assert!(value.is_none());
    assert!(!action_ran);
    assert!(matches!(
        &steps[..],
        [(CycleStep::Prune, StepOutcome::Failed(_))]
    ));
    assert_eq!(log_lines(&log).len(), 1, "repo `failed` hook ran");
    assert!(matches!(
        &events[..],
        [
            CycleEvent::StepStarted(CycleStep::Prune),
            CycleEvent::HookWarning { .. },
            CycleEvent::StepFinished(CycleStep::Prune, StepOutcome::Failed(_)),
        ]
    ));
}
