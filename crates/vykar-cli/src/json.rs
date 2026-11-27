//! `--json`: one JSON document on stdout for list, info, backup, prune,
//! compact and check. Messages, progress and prompts stay on stderr.

use serde::Serialize;
use vykar_core::app::operations::{BackupRunReport, StepOutcome};
use vykar_core::app::views::SnapshotRecord;
use vykar_core::app::{OpsError, StepError, StepErrorKind};
use vykar_core::commands::check::CheckResult;
use vykar_core::commands::compact::CompactStats;
use vykar_core::commands::info::InfoStats;
use vykar_core::commands::prune::{PruneListEntry, PruneStats};
use vykar_core::repo::snapshot_cache::{self, SkippedSnapshot};

use crate::cli::Commands;
use crate::error::{CliError, CliResult};

/// Version of every JSON document vykar prints or serves (`--json`, the
/// daemon's status.json). Fields are only added within a version.
pub(crate) const SCHEMA: u32 = 1;

/// Where one repository's command puts its result.
pub(crate) enum Output<'a> {
    /// Print for a person.
    Human,
    /// Keep for the document; print no result on stdout.
    Json(&'a mut Option<JsonResult>),
}

impl<'a> Output<'a> {
    /// `Json` with the slot, `Human` without one.
    pub(crate) fn from_slot(slot: Option<&'a mut Option<JsonResult>>) -> Self {
        slot.map_or(Self::Human, Self::Json)
    }

    pub(crate) fn is_json(&self) -> bool {
        matches!(self, Self::Json(_))
    }

    /// Store the result in JSON mode; a no-op in human mode.
    pub(crate) fn keep(&mut self, result: JsonResult) {
        if let Self::Json(slot) = self {
            **slot = Some(result);
        }
    }
}

/// A command's result in the document; untagged, the document's `command`
/// says which.
#[derive(Serialize)]
#[serde(untagged)]
pub(crate) enum JsonResult {
    List(ListJson),
    Info(InfoStats),
    Backup(BackupRunReport),
    Prune(PruneJson),
    Compact(CompactJson),
    Check(CheckResult),
}

#[derive(Serialize)]
pub(crate) struct ListJson {
    /// After --source and --last, in the table's order (oldest first).
    pub snapshots: Vec<SnapshotRecord>,
    /// Snapshots this binary could not read: the list is incomplete when
    /// this is non-empty.
    pub hidden: Vec<HiddenSnapshot>,
}

#[derive(Serialize)]
pub(crate) struct HiddenSnapshot {
    /// `SkippedSnapshot::id_hex`.
    pub id: String,
    /// "incompatible_envelope" | "undecodable" | "undecryptable" | "unavailable"
    pub kind: &'static str,
    /// The reason's text.
    pub message: String,
}

impl HiddenSnapshot {
    pub(crate) fn new(skipped: &SkippedSnapshot) -> Self {
        use snapshot_cache::SkipReason as R;
        let kind = match &skipped.reason {
            R::IncompatibleEnvelope { .. } => "incompatible_envelope",
            R::Undecodable(_) => "undecodable",
            R::Undecryptable(_) => "undecryptable",
            R::Unavailable(_) => "unavailable",
        };
        Self {
            id: skipped.id_hex.clone(),
            kind,
            message: skipped.reason.to_string(),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct PruneJson {
    pub dry_run: bool,
    pub stats: PruneStats,
    /// Every keep/prune decision (always filled under --json).
    pub entries: Vec<PruneListEntry>,
    /// `prune --compact`: the compact's stats; null without --compact or when
    /// the compact produced none.
    pub compact_stats: Option<CompactStats>,
    /// `prune --compact`: the outcome of the compact half; null without
    /// --compact and when the prune did not succeed (compact did not run).
    pub compact_outcome: Option<StepOutcome>,
}

#[derive(Serialize)]
pub(crate) struct CompactJson {
    pub dry_run: bool,
    pub stats: CompactStats,
}

#[derive(Serialize)]
pub(crate) struct RepoRef {
    /// The label, else the url: status.json's `repos[].name`, the key that
    /// joins the two.
    pub name: String,
    pub label: Option<String>,
    pub url: String,
}

/// One repository's entry. `outcome` has the form of a cycle step's;
/// `result` is present whenever the command produced one, also with a
/// failed or skipped outcome (a backup with failed sources, a check that
/// found errors, a prune whose compact was skipped).
#[derive(Serialize)]
pub(crate) struct RepoEntry {
    pub repository: RepoRef,
    pub outcome: StepOutcome,
    pub result: Option<JsonResult>,
}

#[derive(Serialize)]
pub(crate) struct Document<'a> {
    pub schema: u32,
    /// "list" | "info" | "backup" | "prune" | "compact" | "check"
    pub command: &'a str,
    pub repos: &'a [RepoEntry],
}

/// The command name of a --json run, or the error text when --json is not
/// supported for it.
pub(crate) fn json_command(cmd: Option<&Commands>) -> Result<&'static str, String> {
    match cmd {
        Some(Commands::List { .. }) => Ok("list"),
        Some(Commands::Info { .. }) => Ok("info"),
        Some(Commands::Backup { .. }) => Ok("backup"),
        Some(Commands::Prune { .. }) => Ok("prune"),
        Some(Commands::Compact {
            report_unreferenced: true,
            ..
        }) => Err("--json cannot be combined with compact --report-unreferenced".into()),
        Some(Commands::Compact { .. }) => Ok("compact"),
        Some(Commands::Check { repair: true, .. }) => {
            Err("--json cannot be combined with check --repair".into())
        }
        Some(Commands::Check { .. }) => Ok("check"),
        _ => Err("--json is supported only by list, info, backup, prune, compact and check".into()),
    }
}

/// `StepError` of a CLI error: engine and ops errors as the core classifies
/// them; `Io`, `Json` and `Msg` are `other`.
pub(crate) fn step_error(e: &CliError) -> StepError {
    match e {
        CliError::Vykar(v) => StepError::from(v),
        CliError::Ops(o) => StepError::from(o),
        CliError::Io(_) | CliError::Json(_) | CliError::Msg(_) => {
            StepError::new(StepErrorKind::Other, e.to_string())
        }
    }
}

/// The entry outcome of a per-repository result, before its skip is
/// swallowed: `Ok(false)` is ok, `Ok(true)` partial, an ops skip skipped,
/// any other error failed.
pub(crate) fn outcome_of(result: &CliResult<bool>) -> StepOutcome {
    match result {
        Ok(false) => StepOutcome::Ok,
        Ok(true) => StepOutcome::Partial,
        Err(CliError::Ops(OpsError::Skipped(r))) => StepOutcome::Skipped(r.clone()),
        Err(e) => StepOutcome::Failed(step_error(e)),
    }
}

fn render_document(command: &str, repos: &[RepoEntry]) -> serde_json::Result<String> {
    serde_json::to_string_pretty(&Document {
        schema: SCHEMA,
        command,
        repos,
    })
}

/// Print the document on stdout, pretty, with one trailing newline. An
/// encoding error is reported on stderr and returns false.
pub(crate) fn print_document(command: &str, repos: &[RepoEntry]) -> bool {
    match render_document(command, repos) {
        Ok(text) => {
            println!("{text}");
            true
        }
        Err(e) => {
            eprintln!("Error: could not encode the JSON document: {e}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use vykar_core::app::{SkipKind, SkipReason as AppSkipReason};
    use vykar_types::error::VykarError;

    use super::*;
    use crate::cli::Cli;

    fn command_of(args: &[&str]) -> Result<&'static str, String> {
        let cli = Cli::try_parse_from(args).unwrap();
        json_command(cli.command.as_ref())
    }

    #[test]
    fn json_command_accepts_the_six_and_refuses_the_rest() {
        for (args, name) in [
            (&["vykar", "list"][..], "list"),
            (&["vykar", "info"], "info"),
            (&["vykar", "backup"], "backup"),
            (&["vykar", "prune"], "prune"),
            (&["vykar", "prune", "--compact"], "prune"),
            (&["vykar", "compact"], "compact"),
            (&["vykar", "check"], "check"),
        ] {
            assert_eq!(command_of(args), Ok(name), "{args:?}");
        }
        for args in [
            &["vykar"][..],
            &["vykar", "restore", "latest", "/tmp/x"],
            &["vykar", "daemon"],
            &["vykar", "check", "--repair"],
            &["vykar", "compact", "--report-unreferenced"],
        ] {
            assert!(command_of(args).is_err(), "{args:?}");
        }
    }

    #[test]
    fn json_is_a_global_flag() {
        for args in [["vykar", "list", "--json"], ["vykar", "--json", "list"]] {
            assert!(Cli::try_parse_from(args).unwrap().json, "{args:?}");
        }
        assert!(!Cli::try_parse_from(["vykar", "list"]).unwrap().json);
    }

    #[test]
    fn outcomes_of_results() {
        let json = |r: CliResult<bool>| serde_json::to_string(&outcome_of(&r)).unwrap();
        assert_eq!(json(Ok(false)), r#"{"status":"ok"}"#);
        assert_eq!(json(Ok(true)), r#"{"status":"partial"}"#);
        assert_eq!(
            json(Err(CliError::Ops(OpsError::Skipped(AppSkipReason::new(
                SkipKind::Hook,
                "r"
            ))))),
            r#"{"status":"skipped","kind":"hook","message":"r"}"#
        );
        let interrupted = VykarError::Interrupted.to_string();
        assert_eq!(
            json(Err(CliError::Vykar(VykarError::Interrupted))),
            format!(r#"{{"status":"failed","kind":"interrupted","message":"{interrupted}"}}"#)
        );
        assert!(matches!(
            outcome_of(&Err(CliError::Vykar(VykarError::DecryptionFailed))),
            StepOutcome::Failed(e) if e.kind == StepErrorKind::Passphrase
        ));
        assert_eq!(
            json(Err(CliError::Msg("check found 1 error(s)".into()))),
            r#"{"status":"failed","kind":"other","message":"check found 1 error(s)"}"#
        );
    }

    #[test]
    fn document_with_two_entries() {
        let repos = [
            RepoEntry {
                repository: RepoRef {
                    name: "main".into(),
                    label: Some("main".into()),
                    url: "/r/main".into(),
                },
                outcome: StepOutcome::Ok,
                result: Some(JsonResult::Compact(CompactJson {
                    dry_run: true,
                    stats: CompactStats::default(),
                })),
            },
            RepoEntry {
                repository: RepoRef {
                    name: "/r/other".into(),
                    label: None,
                    url: "/r/other".into(),
                },
                outcome: StepOutcome::Skipped(AppSkipReason::new(SkipKind::Hook, "r")),
                result: None,
            },
        ];
        let text = render_document("compact", &repos).unwrap();
        let v: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(v["schema"], 1);
        assert_eq!(v["command"], "compact");
        assert_eq!(v["repos"][0]["repository"]["name"], "main");
        assert_eq!(v["repos"][0]["repository"]["label"], "main");
        assert_eq!(v["repos"][0]["outcome"]["status"], "ok");
        assert_eq!(v["repos"][0]["result"]["dry_run"], true);
        assert_eq!(v["repos"][0]["result"]["stats"]["packs_total"], 0);
        assert_eq!(v["repos"][1]["repository"]["name"], "/r/other");
        assert!(v["repos"][1]["repository"]["label"].is_null());
        assert_eq!(v["repos"][1]["outcome"]["kind"], "hook");
        assert!(v["repos"][1]["result"].is_null());
        // Struct order, not sorted keys.
        assert!(
            text.starts_with("{\n  \"schema\": 1,\n  \"command\": \"compact\",\n  \"repos\": [")
        );
    }
}
