use std::cell::Cell;

use crate::app::operations::{CycleStep, StepOutcome};
use crate::app::passphrase::NoPrompt;
use crate::app::service::{AppEvent, AppService, CycleRequest, HookedCommand, RepoSelection};
use crate::app::{OpsError, StepErrorKind};
use crate::config::{HooksConfig, ResolvedRepo};
use crate::repo::control::{CancelToken, FnMutSink};
use crate::testing::{TEST_PASSPHRASE, conformance};
use crate::testutil::{cmd_append, cmd_touch};
use vykar_types::error::VykarError;

use super::helpers::{
    Reply, ScriptedPrompt, init_repo, make_test_config, prompt_only, resolved_repo, source_entry,
};

fn hooks(pairs: &[(&str, &str)]) -> HooksConfig {
    HooksConfig {
        hooks: pairs
            .iter()
            .map(|(k, v)| (k.to_string(), vec![v.to_string()]))
            .collect(),
    }
}

fn log_lines(path: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim().to_string())
        .collect()
}

/// An initialized local repository at `<tmp>/<name>` with one source.
fn local_repo(tmp: &tempfile::TempDir, name: &str) -> ResolvedRepo {
    let repo_dir = tmp.path().join(name);
    std::fs::create_dir_all(&repo_dir).unwrap();
    let config = init_repo(&repo_dir);
    let src = tmp.path().join(format!("{name}-src"));
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("a.txt"), name.as_bytes()).unwrap();
    let mut repo = resolved_repo(config, vec![source_entry(&src, "src")]);
    repo.label = Some(name.to_string());
    repo
}

/// A repository at `<tmp>/volume/<name>` that does not exist.
fn missing_repo(tmp: &tempfile::TempDir, name: &str) -> ResolvedRepo {
    let config = make_test_config(&tmp.path().join("volume").join(name));
    let src = tmp.path().join(format!("{name}-src"));
    std::fs::create_dir_all(&src).unwrap();
    let mut repo = resolved_repo(config, vec![source_entry(&src, "src")]);
    repo.label = Some(name.to_string());
    repo
}

#[test]
fn run_hooked_dry_run_runs_no_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let mut repo = missing_repo(&tmp, "r");
    let before = cmd_append(&log, "before_prune");
    let finally = cmd_append(&log, "finally_prune");
    repo.repo_hooks = hooks(&[
        ("before_prune", before.as_str()),
        ("finally_prune", finally.as_str()),
    ]);
    let mut service = AppService::new();

    for (dry_run, expected) in [
        (true, Vec::<&str>::new()),
        (false, vec!["before_prune", "finally_prune"]),
    ] {
        let _ = std::fs::remove_file(&log);
        let ran = Cell::new(false);
        let result = service.run_hooked(
            &repo,
            HookedCommand::Prune,
            dry_run,
            &mut NoPrompt,
            &mut |_| {},
            |_| {
                ran.set(true);
                Ok::<_, OpsError>(())
            },
        );
        assert!(result.is_ok(), "{dry_run}");
        assert!(ran.get(), "{dry_run}");
        assert_eq!(log_lines(&log), expected, "dry_run = {dry_run}");
    }
}

#[test]
fn run_hooked_asks_after_before_hooks_and_not_on_a_hook_skip() {
    let tmp = tempfile::tempdir().unwrap();
    let marker = tmp.path().join("before_ran");
    let mut repo = missing_repo(&tmp, "r");
    let _env = prompt_only(&mut repo.config);
    let touch = cmd_touch(&marker);
    repo.repo_hooks = hooks(&[("before_prune", touch.as_str())]);
    let mut service = AppService::new();
    let mut prompt = ScriptedPrompt::new(&[Reply::Value("p")]);
    let seen = marker.clone();
    prompt.on_ask = Some(Box::new(move || {
        assert!(seen.exists(), "before hooks first")
    }));

    let result = service.run_hooked(
        &repo,
        HookedCommand::Prune,
        false,
        &mut prompt,
        &mut |_| {},
        |pass| pass.with(|p| Ok::<_, OpsError>(p.map(str::to_string))),
    );
    assert_eq!(result.unwrap().as_deref(), Some("p"));
    assert_eq!(prompt.asks(), 1);

    repo.repo_hooks = hooks(&[("before_prune", "exit 75")]);
    let mut prompt = ScriptedPrompt::new(&[]);
    let mut service = AppService::new();
    let result = service.run_hooked(
        &repo,
        HookedCommand::Prune,
        false,
        &mut prompt,
        &mut |_| {},
        |pass| pass.with(|_| Ok::<_, OpsError>(())),
    );
    assert!(matches!(result, Err(OpsError::Skipped(_))), "{result:?}");
    assert_eq!(prompt.asks(), 0);
}

#[test]
fn run_hooked_retry_does_not_rerun_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    let log = tmp.path().join("log");
    let mut repo = local_repo(&tmp, "r");
    let _env = prompt_only(&mut repo.config);
    let before = cmd_append(&log, "before_prune");
    repo.repo_hooks = hooks(&[("before_prune", before.as_str())]);
    let mut service = AppService::new();
    let mut prompt = ScriptedPrompt::new(&[Reply::Value("wrong"), Reply::Value(TEST_PASSPHRASE)]);

    let result = service.run_hooked(
        &repo,
        HookedCommand::Prune,
        false,
        &mut prompt,
        &mut |_| {},
        |pass| {
            pass.with(|p| {
                crate::commands::list::list_snapshots(&repo.config, p)
                    .map(|_| ())
                    .map_err(OpsError::from)
            })
        },
    );

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(prompt.asks(), 2);
    assert_eq!(log_lines(&log), vec!["before_prune"]);
    assert!(service.is_cached(&repo.config));
}

#[test]
fn backup_asks_after_before_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    let marker = tmp.path().join("before_ran");
    let mut repo = local_repo(&tmp, "r");
    let _env = prompt_only(&mut repo.config);
    let touch = cmd_touch(&marker);
    repo.repo_hooks = hooks(&[("before_backup", touch.as_str())]);
    let mut service = AppService::new();
    let mut prompt = ScriptedPrompt::new(&[Reply::Value(TEST_PASSPHRASE)]);
    let seen = marker.clone();
    prompt.on_ask = Some(Box::new(move || {
        assert!(seen.exists(), "before hooks first")
    }));

    let report = service
        .backup(
            &repo,
            &repo.sources,
            &mut prompt,
            &CancelToken::new(),
            false,
            None,
        )
        .unwrap();

    assert_eq!(report.created.len(), 1);
    assert_eq!(prompt.asks(), 1);
    assert!(service.is_cached(&repo.config));
}

fn started(
    repos: &[ResolvedRepo],
    request: &CycleRequest,
) -> (Vec<(usize, usize, usize)>, crate::app::service::CycleReport) {
    let mut seen = Vec::new();
    let report = AppService::new().run_cycle(
        repos,
        request,
        &mut NoPrompt,
        &CancelToken::new(),
        &FnMutSink::new(|e: AppEvent<'_>| {
            if let AppEvent::RepoStarted {
                index,
                position,
                total,
                ..
            } = e
            {
                seen.push((index, position, total));
            }
        }),
    );
    (seen, report)
}

#[test]
fn run_cycle_selection() {
    let tmp = tempfile::tempdir().unwrap();
    let repos = vec![
        local_repo(&tmp, "a"),
        missing_repo(&tmp, "b"),
        local_repo(&tmp, "c"),
    ];
    let indices = |report: &crate::app::service::CycleReport| {
        report.repos.iter().map(|r| r.index).collect::<Vec<_>>()
    };

    let (seen, report) = started(&repos, &CycleRequest::default());
    assert_eq!(indices(&report), [0, 1, 2]);
    assert_eq!(seen, [(0, 0, 3), (1, 1, 3), (2, 2, 3)]);

    let request = CycleRequest {
        selection: RepoSelection::Indices(vec![2, 0, 9]),
        ..Default::default()
    };
    let (seen, report) = started(&repos, &request);
    assert_eq!(indices(&report), [2, 0]);
    assert_eq!(seen, [(2, 0, 2), (0, 1, 2)]);

    let request = CycleRequest {
        selection: RepoSelection::Explicit(1),
        ..Default::default()
    };
    let (_, report) = started(&repos, &request);
    let [only] = report.repos.as_slice() else {
        panic!("one repository");
    };
    assert!(!only.result.repo_unavailable());
    assert!(
        matches!(
            only.result.steps.first(),
            Some((CycleStep::Backup, StepOutcome::Failed(e))) if e.kind == StepErrorKind::RepoNotFound
        ),
        "{:?}",
        only.result.steps
    );

    let request = CycleRequest {
        selection: RepoSelection::Indices(vec![1]),
        ..Default::default()
    };
    let (_, report) = started(&repos, &request);
    let [only] = report.repos.as_slice() else {
        panic!("one repository");
    };
    assert!(only.result.repo_unavailable());
    assert_eq!(
        only.skipped().map(|r| r.kind),
        Some(crate::app::SkipKind::RepoUnavailable)
    );
    assert!(report.all_skipped());
    assert!(!report.has_failures());
}

#[test]
fn run_cycle_cancelled_reports_not_reached() {
    let tmp = tempfile::tempdir().unwrap();
    let repos = vec![
        missing_repo(&tmp, "a"),
        missing_repo(&tmp, "b"),
        missing_repo(&tmp, "c"),
    ];
    let cancel = CancelToken::new();
    let report = AppService::new().run_cycle(
        &repos,
        &CycleRequest::default(),
        &mut NoPrompt,
        &cancel,
        &|e: AppEvent<'_>| {
            if let AppEvent::RepoFinished { .. } = e {
                cancel.cancel();
            }
        },
    );
    assert_eq!(report.repos.len(), 1);
    assert_eq!(report.not_reached, [1, 2]);
}

#[test]
fn run_cycle_reuses_an_accepted_value() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, "r");
    let _env = prompt_only(&mut repo.config);
    let mut repos = vec![repo];
    let mut service = AppService::new();
    let request = CycleRequest::default();
    let cycle = |service: &mut AppService, repos: &[ResolvedRepo], prompt: &mut ScriptedPrompt| {
        let report = service.run_cycle(
            repos,
            &request,
            prompt,
            &CancelToken::new(),
            &|_: AppEvent<'_>| {},
        );
        assert!(
            !report.has_failures(),
            "{:?}",
            conformance::summary(&report, repos)
        );
    };

    let mut prompt = ScriptedPrompt::new(&[Reply::Value(TEST_PASSPHRASE)]);
    cycle(&mut service, &repos, &mut prompt);
    assert_eq!(prompt.asks(), 1);

    let mut prompt = ScriptedPrompt::new(&[]);
    cycle(&mut service, &repos, &mut prompt);
    assert_eq!(prompt.asks(), 0, "the accepted value is reused");

    // A changed source without a reload: the cached value is stale.
    if let Some(r) = repos.first_mut() {
        r.config.encryption.passcommand = Some("exit 1".into());
    }
    let mut prompt = ScriptedPrompt::new(&[]);
    let report = service.run_cycle(
        &repos,
        &request,
        &mut prompt,
        &CancelToken::new(),
        &|_: AppEvent<'_>| {},
    );
    assert!(report.has_failures(), "the failing passcommand now runs");
    assert!(!service.is_cached(&repos[0].config));
}

#[test]
fn accepted_passphrase_follows_the_source() {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, "r");
    let _env = prompt_only(&mut repo.config);
    repo.config.encryption.passcommand = Some(format!("echo {TEST_PASSPHRASE}"));
    let repos = vec![repo];
    let mut service = AppService::new();
    assert!(
        service.accepted_passphrase(&repos[0].config).is_none(),
        "nothing accepted yet"
    );

    let mut prompt = ScriptedPrompt::new(&[]);
    let report = service.run_cycle(
        &repos,
        &CycleRequest::default(),
        &mut prompt,
        &CancelToken::new(),
        &|_: AppEvent<'_>| {},
    );
    assert!(
        !report.has_failures(),
        "{:?}",
        conformance::summary(&report, &repos)
    );
    assert_eq!(prompt.asks(), 0);
    assert_eq!(
        service
            .accepted_passphrase(&repos[0].config)
            .as_deref()
            .map(String::as_str),
        Some(TEST_PASSPHRASE)
    );

    let mut changed = repos[0].config.clone();
    changed.encryption.passcommand = Some("echo other".into());
    assert!(
        service.accepted_passphrase(&changed).is_none(),
        "not handed over under another source"
    );
}

#[test]
fn conformance_core_cycle() {
    let tmp = tempfile::tempdir().unwrap();
    super::helpers::init_test_environment();
    let repos = conformance::fixture(tmp.path());
    let report = AppService::new().run_cycle(
        &repos,
        &CycleRequest::default(),
        &mut NoPrompt,
        &CancelToken::new(),
        &|_: AppEvent<'_>| {},
    );
    assert_eq!(
        conformance::summary(&report, &repos),
        conformance::EXPECTED_SUMMARY
    );
    assert_eq!(
        conformance::hook_logs(tmp.path()),
        conformance::EXPECTED_HOOKS
    );
}

#[test]
fn passphrase_scope_unused_engine_error_is_returned() {
    // A non-passphrase engine error from a hooked action keeps its type.
    let tmp = tempfile::tempdir().unwrap();
    let repo = missing_repo(&tmp, "r");
    let result = AppService::new().run_hooked(
        &repo,
        HookedCommand::Check,
        true,
        &mut NoPrompt,
        &mut |_| {},
        |pass| pass.with(|_| Err::<(), _>(OpsError::from(VykarError::Interrupted))),
    );
    assert!(matches!(
        result,
        Err(OpsError::Engine(VykarError::Interrupted))
    ));
}

/// `RepoPassphrase::open` replaces a refused value at the open and accepts
/// the one that opened: a later decryption failure is the caller's error.
#[test]
fn open_accepts_right_after_the_open() {
    use crate::app::passphrase::{PassphraseCache, RepoPassphrase};
    use crate::repo::OpenOptions;

    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, "r");
    let _env = prompt_only(&mut repo.config);
    let mut prompt = ScriptedPrompt::new(&[Reply::Value("wrong"), Reply::Value(TEST_PASSPHRASE)]);
    let mut cache = PassphraseCache::default();
    let mut scope = RepoPassphrase::new(&mut cache, &mut prompt, &repo.config, None);

    let opened = scope.open::<OpsError>(OpenOptions::new(), &CancelToken::new());
    assert!(opened.is_ok(), "{:?}", opened.err());
    drop(opened);
    let after = scope.run(|_| Err::<(), _>(OpsError::from(VykarError::DecryptionFailed)));
    assert!(
        matches!(after, Err(OpsError::Engine(VykarError::DecryptionFailed))),
        "{after:?}"
    );
    drop(scope);

    assert_eq!(prompt.asks(), 2);
    assert_eq!(
        cache.get(&repo.config).as_deref().map(String::as_str),
        Some(TEST_PASSPHRASE)
    );
}

/// A hooked command that opened first: a decryption failure after the open
/// is damage, the command's error. It neither re-prompts nor re-runs, and the
/// value that opened stays cached.
#[test]
fn hooked_prune_damage_after_open_does_not_reprompt() {
    use crate::repo::OpenOptions;

    let tmp = tempfile::tempdir().unwrap();
    let mut repo = local_repo(&tmp, "r");
    let _env = prompt_only(&mut repo.config);
    let mut service = AppService::new();
    let mut prompt = ScriptedPrompt::new(&[Reply::Value(TEST_PASSPHRASE)]);
    let runs = Cell::new(0);

    let result = service.run_hooked(
        &repo,
        HookedCommand::Prune,
        false,
        &mut prompt,
        &mut |_| {},
        |pass| -> Result<(), OpsError> {
            let _repo = pass.open::<OpsError>(OpenOptions::new(), &CancelToken::new())?;
            runs.set(runs.get() + 1);
            // What a damaged object read after the open returns.
            Err(OpsError::from(VykarError::DecryptionFailed))
        },
    );

    assert!(
        matches!(result, Err(OpsError::Engine(VykarError::DecryptionFailed))),
        "{result:?}"
    );
    assert_eq!(prompt.asks(), 1);
    assert_eq!(runs.get(), 1);
    assert!(service.is_cached(&repo.config));
}
