use std::cell::Cell;
use std::sync::atomic::AtomicBool;

use zeroize::Zeroizing;

use crate::app::operations::{
    run_backup_selection, run_full_cycle_for_repo, run_full_cycle_with_passphrase_source,
    run_hooked_step, BackupRunEvent, CycleEvent, CycleStep, FullCycleResult, StepOutcome,
};
use crate::config::{HooksConfig, ResolvedRepo, SourceHooksConfig};
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
    assert!(!report.created[0].snapshot_name.is_empty());
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

// ── Quiet hook skip (exit 75) ───────────────────────────────────────────────

const CMD_SKIP: &str = "exit 75";

fn hooks(pairs: &[(&str, &str)]) -> HooksConfig {
    HooksConfig {
        hooks: pairs
            .iter()
            .map(|(k, v)| (k.to_string(), vec![v.to_string()]))
            .collect(),
    }
}

/// A real local repo with one small source directory per label.
fn local_repo(tmp: &tempfile::TempDir, labels: &[&str]) -> ResolvedRepo {
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);
    let sources = labels
        .iter()
        .map(|label| {
            let dir = tmp.path().join(label);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("a.txt"), label.as_bytes()).unwrap();
            source_entry(&dir, label)
        })
        .collect();
    resolved_repo(config, sources)
}

fn run_cycle(repo: &ResolvedRepo) -> FullCycleResult {
    run_full_cycle_for_repo(repo, None, None, false, &[], &mut |_| {})
}

fn outcome(result: &FullCycleResult, step: CycleStep) -> &StepOutcome {
    &result
        .steps
        .iter()
        .find(|(s, _)| *s == step)
        .unwrap_or_else(|| panic!("no outcome for {}", step.command_name()))
        .1
}

#[test]
fn cycle_before_backup_skip_skips_every_step() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let mut repo = local_repo(&tmp, &["src"]);
    let before_check = cmd_append(&log, "before_check");
    repo.repo_hooks = hooks(&[
        ("before_backup", CMD_SKIP),
        ("before_check", before_check.as_str()),
    ]);

    let result = run_cycle(&repo);

    assert_eq!(result.steps.len(), 4);
    assert!(result
        .steps
        .iter()
        .all(|(_, o)| matches!(o, StepOutcome::Skipped(_))));
    assert!(!result.has_failures());
    assert!(result.backup_skipped().is_some());
    assert!(result.backup_report.is_none());
    assert!(
        log_lines(&log).is_empty(),
        "skipped steps run no hooks either"
    );
}

#[test]
fn cycle_before_prune_skip_only_skips_prune() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let mut repo = local_repo(&tmp, &["src"]);
    repo.config.retention.keep_last = Some(5);
    let before_compact = cmd_append(&log, "before_compact");
    let before_check = cmd_append(&log, "before_check");
    repo.repo_hooks = hooks(&[
        ("before_prune", CMD_SKIP),
        ("before_compact", before_compact.as_str()),
        ("before_check", before_check.as_str()),
    ]);

    let result = run_cycle(&repo);

    assert!(matches!(
        outcome(&result, CycleStep::Backup),
        StepOutcome::Ok
    ));
    assert!(matches!(
        outcome(&result, CycleStep::Prune),
        StepOutcome::Skipped(reason) if reason.contains("requested skip")
    ));
    assert!(matches!(
        outcome(&result, CycleStep::Compact),
        StepOutcome::Ok
    ));
    assert!(!result.has_failures(), "{:?}", result.steps);
    assert!(result.backup_skipped().is_none());
    assert_eq!(log_lines(&log), vec!["before_compact", "before_check"]);
}

#[test]
fn one_skipped_source_still_backs_up_the_other() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let mut repo = local_repo(&tmp, &["kept", "gated"]);
    repo.sources[1].hooks = SourceHooksConfig {
        before: vec![CMD_SKIP.to_string()],
        ..Default::default()
    };
    let after = cmd_append(&log, "after");
    let failed = cmd_append(&log, "failed");
    repo.repo_hooks = hooks(&[("after", after.as_str()), ("failed", failed.as_str())]);

    let report = run_backup_selection(&repo, &repo.sources, None, None, false, None).unwrap();

    assert_eq!(report.created.len(), 1);
    assert_eq!(report.created[0].source_label, "kept");
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(report.skipped[0].label, "gated");
    assert_eq!(log_lines(&log), vec!["after"]);
}

#[test]
fn all_sources_skipped_is_a_repo_level_skip() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let mut repo = local_repo(&tmp, &["one", "two"]);
    for source in &mut repo.sources {
        source.hooks = SourceHooksConfig {
            before: vec![CMD_SKIP.to_string()],
            ..Default::default()
        };
    }
    let after = cmd_append(&log, "after");
    let failed = cmd_append(&log, "failed");
    let finally = cmd_append(&log, "finally");
    repo.repo_hooks = hooks(&[
        ("after", after.as_str()),
        ("failed", failed.as_str()),
        ("finally", finally.as_str()),
    ]);

    let err = run_backup_selection(&repo, &repo.sources, None, None, false, None)
        .err()
        .unwrap();

    match err {
        VykarError::Skipped(reason) => {
            assert!(
                reason.contains("'one'") && reason.contains("'two'"),
                "{reason}"
            );
        }
        other => panic!("expected a skip, got {other}"),
    }
    assert_eq!(log_lines(&log), vec!["finally"]);
}

#[test]
fn resolver_not_called_when_backup_is_skipped() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, &["src"]);
    repo.repo_hooks = hooks(&[("before_backup", CMD_SKIP)]);
    let calls = Cell::new(0);

    let result = run_full_cycle_with_passphrase_source(
        &repo,
        &mut || {
            calls.set(calls.get() + 1);
            Ok(None)
        },
        None,
        false,
        &[],
        &mut |_| {},
    );

    assert_eq!(calls.get(), 0);
    assert!(result.backup_skipped().is_some());
}

#[test]
fn resolver_runs_once_after_before_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    let marker = tmp.path().join("before_ran");
    let mut repo = local_repo(&tmp, &["src"]);
    let before_cmd = crate::testutil::cmd_touch(&marker);
    repo.repo_hooks = hooks(&[("before_backup", before_cmd.as_str())]);
    let calls = Cell::new(0);

    let result = run_full_cycle_with_passphrase_source(
        &repo,
        &mut || {
            assert!(marker.exists(), "before hooks run first");
            calls.set(calls.get() + 1);
            Ok(None::<Zeroizing<String>>)
        },
        None,
        false,
        &[],
        &mut |_| {},
    );

    assert!(!result.has_failures(), "{:?}", result.steps);
    assert_eq!(calls.get(), 1, "memoized across backup, compact and check");
}

#[test]
fn resolver_failure_is_reported_once_and_fires_failed_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let mut repo = local_repo(&tmp, &["src"]);
    let failed = cmd_append(&log, "failed");
    let finally = cmd_append(&log, "finally");
    repo.repo_hooks = hooks(&[("failed", failed.as_str()), ("finally", finally.as_str())]);
    let calls = Cell::new(0);

    let result = run_full_cycle_with_passphrase_source(
        &repo,
        &mut || {
            calls.set(calls.get() + 1);
            Err(VykarError::Other("no route to host".into()))
        },
        None,
        false,
        &[],
        &mut |_| {},
    );

    assert_eq!(calls.get(), 1);
    assert!(matches!(
        outcome(&result, CycleStep::Backup),
        StepOutcome::Failed(e) if e.contains("no route to host")
    ));
    assert!(matches!(
        outcome(&result, CycleStep::Check),
        StepOutcome::Skipped(_)
    ));
    assert!(result.has_failures());
    assert_eq!(log_lines(&log), vec!["failed", "finally"]);
}

#[test]
fn canceled_resolver_skips_the_cycle_without_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let mut repo = local_repo(&tmp, &["src"]);
    let after = cmd_append(&log, "after");
    let failed = cmd_append(&log, "failed");
    let finally = cmd_append(&log, "finally");
    repo.repo_hooks = hooks(&[
        ("after", after.as_str()),
        ("failed", failed.as_str()),
        ("finally", finally.as_str()),
    ]);

    let result = run_full_cycle_with_passphrase_source(
        &repo,
        &mut || Err(VykarError::Skipped("passphrase prompt canceled".into())),
        None,
        false,
        &[],
        &mut |_| {},
    );

    assert!(!result.has_failures());
    assert_eq!(result.backup_skipped(), Some("passphrase prompt canceled"));
    assert_eq!(log_lines(&log), vec!["finally"]);
}

// ── Continuing past a failing source ────────────────────────────────────────

/// Point source `idx` at a path that does not exist.
fn break_source(repo: &mut ResolvedRepo, idx: usize) {
    let missing = std::path::Path::new(&repo.sources[idx].paths[0]).join("missing");
    repo.sources[idx].paths = vec![missing.to_string_lossy().to_string()];
}

fn run_collecting(
    repo: &ResolvedRepo,
) -> (
    vykar_types::error::Result<crate::app::operations::BackupRunReport>,
    Vec<BackupRunEvent>,
) {
    let mut events = Vec::new();
    let result = run_backup_selection(
        repo,
        &repo.sources,
        None,
        None,
        false,
        Some(&mut |e| events.push(e)),
    );
    (result, events)
}

fn source_failed_labels(events: &[BackupRunEvent]) -> Vec<&str> {
    events
        .iter()
        .filter_map(|e| match e {
            BackupRunEvent::SourceFailed { label, .. } => Some(label.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn missing_source_does_not_stop_the_next_one() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, &["gone", "ok"]);
    break_source(&mut repo, 0);

    let (result, events) = run_collecting(&repo);
    let report = result.unwrap();

    assert_eq!(report.created.len(), 1);
    assert_eq!(report.created[0].source_label, "ok");
    assert_eq!(report.failed.len(), 1);
    assert_eq!(report.failed[0].label, "gone");
    assert!(report.aborted.is_none());
    assert!(report
        .failure_summary()
        .is_some_and(|s| s.starts_with("source 'gone': ")));
    assert_eq!(source_failed_labels(&events), vec!["gone"]);
}

#[test]
fn sole_failing_source_returns_its_original_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, &["gone"]);
    break_source(&mut repo, 0);

    let (result, _) = run_collecting(&repo);
    let err = result.err().unwrap();
    assert!(
        matches!(&err, VykarError::Config(msg) if msg.contains("does not exist")),
        "{err}"
    );
}

#[test]
fn all_failing_sources_name_each_one() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, &["one", "two"]);
    break_source(&mut repo, 0);
    break_source(&mut repo, 1);

    let (result, events) = run_collecting(&repo);
    let err = result.err().unwrap();
    assert!(
        matches!(&err, VykarError::Other(msg) if msg.contains("'one'") && msg.contains("'two'")),
        "{err}"
    );
    assert_eq!(source_failed_labels(&events), vec!["one", "two"]);
}

/// With nothing created the repository may never have opened, so the result
/// must be `Err` — an `Ok` would let the GUI cache an unchecked passphrase.
#[test]
fn failed_plus_skipped_with_nothing_created_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, &["gone", "gated"]);
    break_source(&mut repo, 0);
    repo.sources[1].hooks = SourceHooksConfig {
        before: vec![CMD_SKIP.to_string()],
        ..Default::default()
    };

    let (result, _) = run_collecting(&repo);
    match result {
        Err(VykarError::Other(msg)) => {
            assert!(msg.contains("'gone'") && msg.contains("'gated'"), "{msg}");
        }
        Err(other) => panic!("expected Other, got {other:?}"),
        Ok(report) => panic!("expected Err, got {report:?}"),
    }
}

#[test]
fn source_before_hook_failure_is_source_local() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, &["hooked", "ok"]);
    repo.sources[0].hooks = SourceHooksConfig {
        before: vec!["exit 1".to_string()],
        ..Default::default()
    };

    let (result, events) = run_collecting(&repo);
    let report = result.unwrap();
    assert_eq!(report.created.len(), 1);
    assert_eq!(report.created[0].source_label, "ok");
    assert_eq!(report.failed[0].label, "hooked");
    assert_eq!(source_failed_labels(&events), vec!["hooked"]);
}

#[cfg(unix)]
#[test]
fn repository_failure_aborts_but_keeps_created_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, &["first", "second", "third"]);
    let config_path = tmp.path().join("repo").join("config");
    // Corrupt the repo config just before the second source opens it.
    repo.sources[1].hooks = SourceHooksConfig {
        before: vec![format!("printf garbage > '{}'", config_path.display())],
        ..Default::default()
    };

    let (result, events) = run_collecting(&repo);
    let report = result.unwrap();

    assert_eq!(report.created.len(), 1);
    assert_eq!(report.created[0].source_label, "first");
    assert!(report.failed.is_empty());
    let aborted = report.aborted.as_ref().expect("aborted");
    assert_eq!(aborted.label, "second");
    assert!(!aborted.interrupted);
    assert_eq!(aborted.not_attempted, vec!["third"]);
    assert!(report.failure_summary().is_some());
    assert!(source_failed_labels(&events).is_empty());
}

#[test]
fn wrong_passphrase_on_first_source_stays_typed() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("src");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("a.txt"), b"x").unwrap();
    let mut config = super::helpers::make_test_config(&repo_dir);
    config.encryption.mode = crate::config::EncryptionModeConfig::Aes256Gcm;
    crate::commands::init::run(&config, Some("right")).unwrap();
    let sources = vec![
        source_entry(&source_dir, "a"),
        source_entry(&source_dir, "b"),
    ];
    let repo = resolved_repo(config, sources);

    let err = run_backup_selection(&repo, &repo.sources, Some("wrong"), None, false, None)
        .err()
        .unwrap();
    assert!(err.is_passphrase_failure(), "{err:?}");
}

#[test]
fn shutdown_before_first_source_is_interrupted() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = local_repo(&tmp, &["one", "two"]);
    let shutdown = AtomicBool::new(true);

    let err = run_backup_selection(&repo, &repo.sources, None, Some(&shutdown), false, None)
        .err()
        .unwrap();
    assert!(matches!(err, VykarError::Interrupted), "{err:?}");
}

#[test]
fn partial_failure_fires_repo_failed_hook_naming_the_source() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let mut repo = local_repo(&tmp, &["gone", "ok"]);
    break_source(&mut repo, 0);
    let after = cmd_append(&log, "after");
    let failed = cmd_append(&log, "failed:{error}");
    repo.repo_hooks = hooks(&[("after", after.as_str()), ("failed", failed.as_str())]);

    let report = run_backup_selection(&repo, &repo.sources, None, None, false, None).unwrap();

    assert_eq!(report.created.len(), 1);
    let lines = log_lines(&log);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].starts_with("failed:source 'gone':"), "{lines:?}");
}

#[cfg(unix)]
#[test]
fn repo_before_failure_still_delivers_finally_warning() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, &["src"]);
    repo.repo_hooks = hooks(&[("before", "false"), ("finally", "false")]);

    let (result, events) = run_collecting(&repo);

    assert!(result.is_err());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, BackupRunEvent::HookWarning { .. })),
        "{events:?}"
    );
}

#[test]
fn cycle_with_failed_source_reports_failure_and_skips_maintenance() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, &["gone", "ok"]);
    repo.config.retention.keep_last = Some(5);
    break_source(&mut repo, 0);
    let mut warnings = Vec::new();

    let result = run_full_cycle_for_repo(&repo, None, None, false, &[], &mut |e| {
        if let CycleEvent::StepWarning { step, message } = e {
            warnings.push((step, message));
        }
    });

    assert!(matches!(
        outcome(&result, CycleStep::Backup),
        StepOutcome::Failed(msg) if msg.contains("'gone'")
    ));
    let report = result.backup_report.as_ref().expect("report kept");
    assert_eq!(report.created.len(), 1);
    assert_eq!(report.created[0].source_label, "ok");
    for step in [CycleStep::Prune, CycleStep::Compact] {
        assert!(
            matches!(outcome(&result, step), StepOutcome::Skipped(r) if r == "backup failed"),
            "{:?}",
            result.steps
        );
    }
    assert!(!matches!(
        outcome(&result, CycleStep::Check),
        StepOutcome::Skipped(r) if r == "backup failed"
    ));
    assert!(result.has_failures());
    assert!(
        warnings
            .iter()
            .any(|(s, m)| *s == CycleStep::Backup && m.contains("'gone'")),
        "{warnings:?}"
    );
}
