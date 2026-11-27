//! Test support for crates/vykar-core/tests/, vykar-cli and vykar-gui tests.
//! Not API; never enable the testing feature in a product build.

pub use crate::repo::{count_repo_opens, with_cheap_kdf};

/// The passphrase of every repository the test helpers create.
pub const TEST_PASSPHRASE: &str = "vykar-test-passphrase";

/// One configuration that every frontend's backup cycle runs, with the
/// outcome they must all report (07's conformance check): `present`, an
/// initialized local repository, and `missing`, a local repository on a
/// volume that is not there.
pub mod conformance {
    #![allow(clippy::expect_used)]

    use std::path::Path;

    use crate::app::operations::{CycleStep, StepOutcome};
    use crate::app::service::CycleReport;
    use crate::config::ResolvedRepo;

    use super::{TEST_PASSPHRASE, with_cheap_kdf};

    const HOOKS: [&str; 4] = [
        "before_backup",
        "after_backup",
        "failed_backup",
        "finally_backup",
    ];

    /// What every frontend's cycle over the fixture reports, per step.
    pub const EXPECTED_SUMMARY: &[&str] = &[
        "present backup ok",
        "present prune ok",
        "present compact ok",
        "present check skipped",
        "missing backup skipped",
        "missing prune skipped",
        "missing compact skipped",
        "missing check skipped",
        "missing unavailable",
    ];

    /// The hooks the fixture's cycle runs, per repository in order.
    pub const EXPECTED_HOOKS: &[&str] = &[
        "present:before_backup",
        "present:after_backup",
        "present:finally_backup",
        "missing:before_backup",
        "missing:finally_backup",
    ];

    /// YAML single-quoted scalar.
    fn quoted(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    /// Hook command that appends `value` as a line to `path`.
    fn cmd_append(path: &Path, value: &str) -> String {
        #[cfg(windows)]
        {
            let escaped = path.display().to_string().replace('\'', "''");
            format!("Add-Content -Path '{escaped}' -Value {value}")
        }

        #[cfg(not(windows))]
        {
            format!("echo {value} >> '{}'", path.display())
        }
    }

    /// The fixture's configuration, its files under `tmp` (the source with
    /// one file is created here; the repositories are not).
    pub fn config_yaml(tmp: &Path) -> String {
        let source = tmp.join("source");
        std::fs::create_dir_all(&source).expect("create source");
        std::fs::write(source.join("a.txt"), b"conformance").expect("write source file");
        let repo = |label: &str, url: &Path, extra: &str| {
            let log = tmp.join(format!("{label}.log"));
            let hooks = HOOKS
                .iter()
                .map(|name| format!("      {name}: {}\n", quoted(&cmd_append(&log, name))))
                .collect::<Vec<_>>()
                .concat();
            format!(
                "  - label: {label}\n    url: {}\n{extra}    hooks:\n{hooks}",
                quoted(&url.display().to_string()),
            )
        };
        format!(
            "repositories:\n{}{}\
             encryption:\n  mode: aes256gcm\n  passphrase: {}\n\
             cache_dir: {}\n\
             sources:\n  - path: {}\n    label: src\n",
            repo(
                "present",
                &tmp.join("present"),
                "    retention:\n      keep_last: 1\n"
            ),
            repo("missing", &tmp.join("volume").join("repo"), ""),
            quoted(TEST_PASSPHRASE),
            quoted(&tmp.join("cache").display().to_string()),
            quoted(&source.display().to_string()),
        )
    }

    /// Initialize the fixture's `present` repository.
    pub fn init_present(repos: &[ResolvedRepo]) {
        let repo = repos
            .iter()
            .find(|r| r.label.as_deref() == Some("present"))
            .expect("the fixture has a 'present' repository");
        std::fs::create_dir_all(&repo.config.repository.url).expect("create repository dir");
        with_cheap_kdf(|| crate::commands::init::run(&repo.config, Some(TEST_PASSPHRASE)))
            .expect("init the present repository");
    }

    /// Write `<tmp>/config.yaml`, initialize `present` and return the
    /// resolved repositories.
    pub fn fixture(tmp: &Path) -> Vec<ResolvedRepo> {
        let path = tmp.join("config.yaml");
        std::fs::write(&path, config_yaml(tmp)).expect("write config");
        let repos = crate::config::load_and_resolve(&path).expect("load the fixture config");
        init_present(&repos);
        repos
    }

    /// One line per step, `"{label} {step} {ok|partial|skipped|failed:{kind:?}}"`,
    /// plus `"{label} unavailable"` for a repository skipped as unavailable.
    pub fn summary(report: &CycleReport, repos: &[ResolvedRepo]) -> Vec<String> {
        let mut lines = Vec::new();
        for r in &report.repos {
            let label = repos
                .get(r.index)
                .map_or("?", |repo| repo.label_or_url())
                .to_string();
            for (step, outcome) in &r.result.steps {
                let word = match outcome {
                    StepOutcome::Ok => "ok".to_string(),
                    StepOutcome::Partial => "partial".to_string(),
                    StepOutcome::Skipped(_) => "skipped".to_string(),
                    StepOutcome::Failed(e) => format!("failed:{:?}", e.kind),
                };
                lines.push(format!("{label} {} {word}", step_name(*step)));
            }
            if r.result.repo_unavailable() {
                lines.push(format!("{label} unavailable"));
            }
        }
        lines
    }

    fn step_name(step: CycleStep) -> &'static str {
        step.command_name()
    }

    /// The hooks that ran, as `"<label>:<hook>"`, `present` first.
    pub fn hook_logs(tmp: &Path) -> Vec<String> {
        ["present", "missing"]
            .iter()
            .flat_map(|label| {
                std::fs::read_to_string(tmp.join(format!("{label}.log")))
                    .unwrap_or_default()
                    .lines()
                    .map(|l| format!("{label}:{}", l.trim()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}
