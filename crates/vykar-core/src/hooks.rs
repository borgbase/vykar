use std::time::Duration;

use crate::config::{HooksConfig, SourceHooksConfig, HOOK_COMMANDS};
use crate::platform::shell;
use vykar_types::error::{Result, VykarError};

// ── Hook runner ──────────────────────────────────────────────────────────────

/// Default timeout for hook command execution (5 minutes).
const HOOK_TIMEOUT: Duration = Duration::from_secs(300);

/// Context passed to hook commands via environment variables and variable substitution.
pub(crate) struct HookContext {
    pub command: String,
    pub repository: String,
    pub label: Option<String>,
    pub error: Option<String>,
    pub source_label: Option<String>,
    pub source_paths: Option<Vec<String>>,
    /// Accumulated non-fatal hook warnings. Callers drain after each lifecycle
    /// to surface via events (GUI) or tracing (CLI). `log_hook_errors` always
    /// fires `tracing::warn!` AND pushes here so no warning is ever lost.
    pub(crate) warnings: Vec<String>,
}

/// Which hook lists a lifecycle runs.
#[derive(Clone, Copy)]
pub(crate) enum HookPhases<'a> {
    /// Global + repo `HooksConfig`, with bare and command-specific keys.
    Repo {
        global: &'a HooksConfig,
        repo: &'a HooksConfig,
    },
    /// Source-level hooks (bare lists only).
    Source(&'a SourceHooksConfig),
}

impl HookPhases<'_> {
    fn is_empty(&self) -> bool {
        match self {
            Self::Repo { global, repo } => global.is_empty() && repo.is_empty(),
            Self::Source(hooks) => !hooks.has_any(),
        }
    }

    /// Repo: global bare → repo bare → global specific → repo specific.
    fn before(&self, ctx: &HookContext) -> Result<()> {
        match self {
            Self::Repo { global, repo } => {
                let before_key = format!("before_{}", ctx.command);
                run_hook_list(global.get_hooks("before"), ctx)?;
                run_hook_list(repo.get_hooks("before"), ctx)?;
                run_hook_list(global.get_hooks(&before_key), ctx)?;
                run_hook_list(repo.get_hooks(&before_key), ctx)
            }
            Self::Source(hooks) => run_hook_list(&hooks.before, ctx),
        }
    }

    /// Repo: `<phase>_<cmd>` then `<phase>`, repo before global.
    fn after_or_failed(&self, ctx: &mut HookContext, success: bool) {
        let phase = if success { "after" } else { "failed" };
        match self {
            Self::Repo { global, repo } => run_repo_phase(global, repo, phase, ctx),
            Self::Source(hooks) => {
                let list = if success { &hooks.after } else { &hooks.failed };
                log_hook_errors(run_hook_list(list, ctx), ctx);
            }
        }
    }

    fn finally(&self, ctx: &mut HookContext) {
        match self {
            Self::Repo { global, repo } => run_repo_phase(global, repo, "finally", ctx),
            Self::Source(hooks) => log_hook_errors(run_hook_list(&hooks.finally, ctx), ctx),
        }
    }
}

/// Non-fatal phase: repo specific → global specific → repo bare → global bare.
fn run_repo_phase(global: &HooksConfig, repo: &HooksConfig, phase: &str, ctx: &mut HookContext) {
    let key = format!("{phase}_{}", ctx.command);
    log_hook_errors(run_hook_list(repo.get_hooks(&key), ctx), ctx);
    log_hook_errors(run_hook_list(global.get_hooks(&key), ctx), ctx);
    log_hook_errors(run_hook_list(repo.get_hooks(phase), ctx), ctx);
    log_hook_errors(run_hook_list(global.get_hooks(phase), ctx), ctx);
}

/// Run the full hook lifecycle around an action:
///
/// 1. `before` hooks — failure skips the action, sets `ctx.error`, runs
///    `failed` + `finally` and returns `Err`.
/// 2. The action.
/// 3. `failed` if `failure(&r)` returns `Some` (which also becomes `{error}`),
///    otherwise `after`.
/// 4. `finally`, always.
///
/// `after` / `failed` / `finally` failures are non-fatal: they are pushed to
/// `ctx.warnings` (before hooks never push), so callers drain once afterwards.
/// Non-hookable commands and empty configs run the action directly.
pub(crate) fn run_lifecycle<R>(
    phases: HookPhases<'_>,
    ctx: &mut HookContext,
    action: impl FnOnce() -> R,
    failure: impl FnOnce(&R) -> Option<String>,
) -> Result<R> {
    if !HOOK_COMMANDS.contains(&ctx.command.as_str()) || phases.is_empty() {
        return Ok(action());
    }

    if let Err(e) = phases.before(ctx) {
        ctx.error = Some(e.to_string());
        phases.after_or_failed(ctx, false);
        phases.finally(ctx);
        return Err(e);
    }

    let r = action();
    let error = failure(&r);
    let success = error.is_none();
    if let Some(msg) = error {
        ctx.error.get_or_insert(msg);
    }
    phases.after_or_failed(ctx, success);
    phases.finally(ctx);
    Ok(r)
}

/// [`run_lifecycle`] with repo-level hooks around a fallible action.
pub(crate) fn run_with_hooks<T>(
    global: &HooksConfig,
    repo: &HooksConfig,
    ctx: &mut HookContext,
    action: impl FnOnce() -> Result<T>,
) -> Result<T> {
    run_lifecycle(HookPhases::Repo { global, repo }, ctx, action, err_string)?
}

/// [`run_lifecycle`] with source-level hooks around a fallible action.
pub(crate) fn run_source_hooks<T>(
    hooks: &SourceHooksConfig,
    ctx: &mut HookContext,
    action: impl FnOnce() -> Result<T>,
) -> Result<T> {
    run_lifecycle(HookPhases::Source(hooks), ctx, action, err_string)?
}

fn err_string<T>(r: &Result<T>) -> Option<String> {
    r.as_ref().err().map(ToString::to_string)
}

fn run_hook_list(cmds: &[String], ctx: &HookContext) -> Result<()> {
    for cmd in cmds {
        execute_hook_command(cmd, ctx)?;
    }
    Ok(())
}

fn execute_hook_command(cmd: &str, ctx: &HookContext) -> Result<()> {
    let expanded = substitute_variables(cmd, ctx);
    tracing::info!("Running hook: {cmd}");

    let mut child = shell::command_for_script(&expanded);
    child.env_remove("VYKAR_PASSPHRASE");

    // Set environment variables
    child.env("VYKAR_COMMAND", &ctx.command);
    child.env("VYKAR_REPOSITORY", &ctx.repository);
    if let Some(ref label) = ctx.label {
        child.env("VYKAR_LABEL", label);
    }
    if let Some(ref error) = ctx.error {
        child.env("VYKAR_ERROR", error);
    }
    if let Some(ref source_label) = ctx.source_label {
        child.env("VYKAR_SOURCE_LABEL", source_label);
    }
    if let Some(ref source_paths) = ctx.source_paths {
        child.env(
            "VYKAR_SOURCE_PATH",
            source_paths.join(source_paths_separator()),
        );
    }

    // Use the non-capturing variant: hooks inherit stdout/stderr so output
    // goes directly to the user's terminal (better UX than capturing).
    let status = shell::run_command_status_with_timeout(&mut child, HOOK_TIMEOUT)
        .map_err(|e| VykarError::Hook(format!("failed to execute hook '{cmd}': {e}")))?;

    if !status.success() {
        let code = status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string());
        return Err(VykarError::Hook(format!("hook '{cmd}' exited with {code}")));
    }

    Ok(())
}

fn substitute_variables(cmd: &str, ctx: &HookContext) -> String {
    let mut result = cmd.replace("{command}", &shell_escape(&ctx.command));
    result = result.replace("{repository}", &shell_escape(&ctx.repository));
    result = result.replace("{label}", &shell_escape(ctx.label.as_deref().unwrap_or("")));
    result = result.replace("{error}", &shell_escape(ctx.error.as_deref().unwrap_or("")));
    result = result.replace(
        "{source_label}",
        &shell_escape(ctx.source_label.as_deref().unwrap_or("")),
    );
    let source_path_str = ctx
        .source_paths
        .as_ref()
        .map(|ps| ps.join(source_paths_separator()))
        .unwrap_or_default();
    result = result.replace("{source_path}", &shell_escape(&source_path_str));
    result
}

fn shell_escape(input: &str) -> String {
    #[cfg(windows)]
    {
        if input.is_empty() {
            return "''".to_string();
        }
        let escaped = input.replace('\'', "''");
        return format!("'{escaped}'");
    }

    #[cfg(not(windows))]
    {
        if input.is_empty() {
            return "''".to_string();
        }
        let escaped = input.replace('\'', "'\"'\"'");
        format!("'{escaped}'")
    }
}

fn source_paths_separator() -> &'static str {
    #[cfg(windows)]
    {
        ";"
    }

    #[cfg(not(windows))]
    {
        ":"
    }
}

fn log_hook_errors(result: Result<()>, ctx: &mut HookContext) {
    if let Err(e) = result {
        let msg = e.to_string();
        tracing::warn!("Hook warning: {msg}");
        ctx.warnings.push(msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{cmd_append, cmd_touch};

    fn make_ctx(command: &str) -> HookContext {
        HookContext {
            command: command.to_string(),
            repository: "/tmp/repo".to_string(),
            label: Some("test".to_string()),
            error: None,
            source_label: None,
            source_paths: None,
            warnings: Vec::new(),
        }
    }

    fn hooks_from(pairs: &[(&str, Vec<&str>)]) -> HooksConfig {
        let mut hooks = std::collections::HashMap::new();
        for (key, cmds) in pairs {
            hooks.insert(
                key.to_string(),
                cmds.iter().map(|s| s.to_string()).collect(),
            );
        }
        HooksConfig { hooks }
    }

    #[cfg(windows)]
    fn cmd_env_assert() -> &'static str {
        "if ($env:VYKAR_COMMAND -eq 'backup' -and $env:VYKAR_REPOSITORY -eq '/tmp/repo' -and $env:VYKAR_LABEL -eq 'test') { exit 0 } else { exit 1 }"
    }

    #[cfg(not(windows))]
    fn cmd_env_assert() -> &'static str {
        "test \"$VYKAR_COMMAND\" = backup && test \"$VYKAR_REPOSITORY\" = /tmp/repo && test \"$VYKAR_LABEL\" = test"
    }

    #[cfg(windows)]
    fn cmd_true() -> &'static str {
        "$true | Out-Null"
    }

    #[cfg(not(windows))]
    fn cmd_true() -> &'static str {
        "true"
    }

    #[cfg(windows)]
    fn cmd_false() -> &'static str {
        "exit 1"
    }

    #[cfg(not(windows))]
    fn cmd_false() -> &'static str {
        "false"
    }

    #[test]
    fn test_variable_substitution() {
        let ctx = HookContext {
            command: "backup".into(),
            repository: "/mnt/nas".into(),
            label: Some("nas".into()),
            error: Some("disk full".into()),
            source_label: Some("docs".into()),
            source_paths: Some(vec!["/home/user/docs".into()]),
            warnings: Vec::new(),
        };
        let result = substitute_variables(
            "echo {command} {repository} {label} {error} {source_label} {source_path}",
            &ctx,
        );
        assert_eq!(
            result,
            "echo 'backup' '/mnt/nas' 'nas' 'disk full' 'docs' '/home/user/docs'"
        );
    }

    #[test]
    fn test_variable_substitution_multi_paths() {
        let ctx = HookContext {
            command: "backup".into(),
            repository: "/mnt/nas".into(),
            label: Some("nas".into()),
            error: None,
            source_label: Some("default".into()),
            source_paths: Some(vec!["/home/user/docs".into(), "/home/user/photos".into()]),
            warnings: Vec::new(),
        };
        let result = substitute_variables("paths={source_path}", &ctx);
        let expected_sep = if cfg!(windows) { ";" } else { ":" };
        assert_eq!(
            result,
            format!("paths='/home/user/docs{expected_sep}/home/user/photos'")
        );
    }

    #[test]
    fn test_variable_substitution_missing_optionals() {
        let ctx = HookContext {
            command: "backup".into(),
            repository: "/tmp/repo".into(),
            label: None,
            error: None,
            source_label: None,
            source_paths: None,
            warnings: Vec::new(),
        };
        let result = substitute_variables("cmd={command} label={label} err={error}", &ctx);
        assert_eq!(result, "cmd='backup' label='' err=''");
    }

    #[test]
    fn test_hook_env_vars() {
        // Use env to print vars, verify they're set
        let global = HooksConfig::default();
        let repo = hooks_from(&[("before_backup", vec![cmd_env_assert()])]);
        let mut ctx = make_ctx("backup");

        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(()));
        assert!(result.is_ok(), "env vars should be set: {:?}", result.err());
    }

    #[test]
    fn test_before_hook_success() {
        let global = hooks_from(&[("before", vec![cmd_true()])]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(42));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_before_hook_failure_aborts() {
        let global = hooks_from(&[("before", vec![cmd_false()])]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");
        let mut action_ran = false;

        let result = run_with_hooks(&global, &repo, &mut ctx, || {
            action_ran = true;
            Ok(())
        });

        assert!(result.is_err());
        assert!(!action_ran, "action should not run when before hook fails");
    }

    #[test]
    fn test_non_hookable_commands_skip_hooks() {
        let global = hooks_from(&[
            ("before", vec![cmd_false()]),
            ("before_info", vec![cmd_false()]),
            ("before_run", vec![cmd_false()]),
        ]);
        let repo = HooksConfig::default();

        for command in ["info", "run"] {
            let mut ctx = make_ctx(command);
            let mut action_ran = false;

            let result = run_with_hooks(&global, &repo, &mut ctx, || {
                action_ran = true;
                Ok(())
            });

            assert!(
                result.is_ok(),
                "non-hookable command should skip hooks: {command}"
            );
            assert!(action_ran, "action should run for command: {command}");
        }
    }

    #[test]
    fn test_after_runs_on_success_only() {
        // after hook writes a marker file
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("after_ran");
        let cmd = cmd_touch(&marker);

        let global = hooks_from(&[("after", vec![&cmd])]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(()));
        assert!(result.is_ok());
        assert!(marker.exists(), "after hook should run on success");

        // Now test failure case
        let marker2 = dir.path().join("after_ran2");
        let cmd2 = cmd_touch(&marker2);
        let global2 = hooks_from(&[("after", vec![&cmd2])]);
        let mut ctx2 = make_ctx("backup");

        let _result: Result<()> = run_with_hooks(&global2, &repo, &mut ctx2, || {
            Err(VykarError::Other("action failed".into()))
        });
        assert!(!marker2.exists(), "after hook should NOT run on failure");
    }

    #[test]
    fn test_failed_runs_on_failure_only() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("failed_ran");
        let cmd = cmd_touch(&marker);

        let global = hooks_from(&[("failed", vec![&cmd])]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let _result: Result<()> = run_with_hooks(&global, &repo, &mut ctx, || {
            Err(VykarError::Other("something broke".into()))
        });
        assert!(marker.exists(), "failed hook should run on failure");

        // Now test success case
        let marker2 = dir.path().join("failed_ran2");
        let cmd2 = cmd_touch(&marker2);
        let global2 = hooks_from(&[("failed", vec![&cmd2])]);
        let mut ctx2 = make_ctx("backup");

        let _result = run_with_hooks(&global2, &repo, &mut ctx2, || Ok(()));
        assert!(!marker2.exists(), "failed hook should NOT run on success");
    }

    #[test]
    fn test_finally_runs_always() {
        let dir = tempfile::tempdir().unwrap();

        // Test on success
        let marker1 = dir.path().join("finally_success");
        let cmd1 = cmd_touch(&marker1);
        let global1 = hooks_from(&[("finally", vec![&cmd1])]);
        let repo = HooksConfig::default();
        let mut ctx1 = make_ctx("backup");

        let _result = run_with_hooks(&global1, &repo, &mut ctx1, || Ok(()));
        assert!(marker1.exists(), "finally should run on success");

        // Test on failure
        let marker2 = dir.path().join("finally_failure");
        let cmd2 = cmd_touch(&marker2);
        let global2 = hooks_from(&[("finally", vec![&cmd2])]);
        let mut ctx2 = make_ctx("backup");

        let _result: Result<()> = run_with_hooks(&global2, &repo, &mut ctx2, || {
            Err(VykarError::Other("error".into()))
        });
        assert!(marker2.exists(), "finally should run on failure");
    }

    #[test]
    fn test_command_specific_hooks() {
        let dir = tempfile::tempdir().unwrap();
        let marker_backup = dir.path().join("before_backup_ran");
        let marker_prune = dir.path().join("before_prune_ran");
        let cmd_backup = cmd_touch(&marker_backup);
        let cmd_prune = cmd_touch(&marker_prune);

        let global = hooks_from(&[
            ("before_backup", vec![&cmd_backup]),
            ("before_prune", vec![&cmd_prune]),
        ]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(()));
        assert!(result.is_ok());
        assert!(
            marker_backup.exists(),
            "before_backup should run for backup command"
        );
        assert!(
            !marker_prune.exists(),
            "before_prune should NOT run for backup command"
        );
    }

    #[test]
    fn test_before_failure_triggers_failed_and_finally() {
        let dir = tempfile::tempdir().unwrap();
        let failed_marker = dir.path().join("failed_ran");
        let finally_marker = dir.path().join("finally_ran");
        let failed_cmd = cmd_touch(&failed_marker);
        let finally_cmd = cmd_touch(&finally_marker);

        let global = hooks_from(&[
            ("before", vec![cmd_false()]),
            ("failed", vec![&failed_cmd]),
            ("finally", vec![&finally_cmd]),
        ]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(()));
        assert!(result.is_err());
        assert!(
            failed_marker.exists(),
            "failed hook should run when before hook fails"
        );
        assert!(
            finally_marker.exists(),
            "finally hook should run when before hook fails"
        );
    }

    fn read_lines(path: &std::path::Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect()
    }

    #[test]
    fn test_lifecycle_failure_judgment_fires_failed_with_error() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("log");
        let after_cmd = cmd_append(&log, "after");
        let failed_cmd = cmd_append(&log, "{error}");

        let global = hooks_from(&[
            ("after", vec![after_cmd.as_str()]),
            ("failed_check", vec![failed_cmd.as_str()]),
        ]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("check");

        let r = run_lifecycle(
            HookPhases::Repo {
                global: &global,
                repo: &repo,
            },
            &mut ctx,
            || 2usize,
            |n| (*n > 0).then(|| format!("check found {n} error(s)")),
        );

        assert_eq!(r.unwrap(), 2);
        assert_eq!(ctx.error.as_deref(), Some("check found 2 error(s)"));
        assert_eq!(read_lines(&log), vec!["check found 2 error(s)"]);
    }

    #[test]
    fn test_after_and_finally_ordering() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("log");
        let entry = |name: &str| cmd_append(&log, name);

        let global = hooks_from(&[
            ("after", vec![&entry("global_after")]),
            ("after_backup", vec![&entry("global_after_backup")]),
            ("finally", vec![&entry("global_finally")]),
            ("finally_backup", vec![&entry("global_finally_backup")]),
        ]);
        let repo = hooks_from(&[
            ("after", vec![&entry("repo_after")]),
            ("after_backup", vec![&entry("repo_after_backup")]),
            ("finally", vec![&entry("repo_finally")]),
            ("finally_backup", vec![&entry("repo_finally_backup")]),
        ]);
        let mut ctx = make_ctx("backup");

        run_with_hooks(&global, &repo, &mut ctx, || Ok(())).unwrap();
        assert_eq!(
            read_lines(&log),
            vec![
                "repo_after_backup",
                "global_after_backup",
                "repo_after",
                "global_after",
                "repo_finally_backup",
                "global_finally_backup",
                "repo_finally",
                "global_finally",
            ]
        );
    }

    #[test]
    fn test_before_ordering() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("log");
        let entry = |name: &str| cmd_append(&log, name);

        let global = hooks_from(&[
            ("before", vec![&entry("global_before")]),
            ("before_backup", vec![&entry("global_before_backup")]),
        ]);
        let repo = hooks_from(&[
            ("before", vec![&entry("repo_before")]),
            ("before_backup", vec![&entry("repo_before_backup")]),
        ]);
        let mut ctx = make_ctx("backup");

        run_with_hooks(&global, &repo, &mut ctx, || Ok(())).unwrap();
        assert_eq!(
            read_lines(&log),
            vec![
                "global_before",
                "repo_before",
                "global_before_backup",
                "repo_before_backup",
            ]
        );
    }

    #[test]
    fn test_warnings_only_from_non_fatal_phases() {
        let global = hooks_from(&[("after", vec![cmd_false()])]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");
        run_with_hooks(&global, &repo, &mut ctx, || Ok(())).unwrap();
        assert_eq!(ctx.warnings.len(), 1, "failing after hook is a warning");

        let global = hooks_from(&[("before", vec![cmd_false()])]);
        let mut ctx = make_ctx("backup");
        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(()));
        assert!(result.is_err());
        assert!(ctx.warnings.is_empty(), "before failure is an error only");
        assert!(ctx.error.is_some());
    }

    #[test]
    fn test_before_error_reaches_failed_hooks() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("log");
        let failed_cmd = cmd_append(&log, "{error}");

        let global = hooks_from(&[
            ("before", vec![cmd_false()]),
            ("failed", vec![failed_cmd.as_str()]),
        ]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let err = run_with_hooks(&global, &repo, &mut ctx, || Ok(()))
            .unwrap_err()
            .to_string();
        assert_eq!(ctx.error.as_deref(), Some(err.as_str()));
        assert_eq!(read_lines(&log), vec![err]);
    }

    #[test]
    fn test_source_before_failure_skips_action_runs_failed_and_finally() {
        let dir = tempfile::tempdir().unwrap();
        let after_marker = dir.path().join("after_ran");
        let failed_marker = dir.path().join("failed_ran");
        let finally_marker = dir.path().join("finally_ran");

        let hooks = SourceHooksConfig {
            before: vec![cmd_false().to_string()],
            after: vec![cmd_touch(&after_marker)],
            failed: vec![cmd_touch(&failed_marker)],
            finally: vec![cmd_touch(&finally_marker)],
        };
        let mut ctx = make_ctx("backup");
        let mut action_ran = false;

        let result = run_source_hooks(&hooks, &mut ctx, || {
            action_ran = true;
            Ok(())
        });

        assert!(result.is_err());
        assert!(!action_ran);
        assert!(!after_marker.exists());
        assert!(failed_marker.exists());
        assert!(finally_marker.exists());
    }
}
