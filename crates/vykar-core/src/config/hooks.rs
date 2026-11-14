use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::deserialize::{deserialize_strict_hooks_map, deserialize_string_or_vec};
use vykar_types::error::{Result, VykarError};

/// Valid hook prefixes.
const HOOK_PREFIXES: &[&str] = &["before", "after", "failed", "finally"];

/// Valid command suffixes for command-specific hooks.
/// Also used by the hook runner to skip hooks for non-hookable commands.
pub const HOOK_COMMANDS: &[&str] = &["backup", "prune", "compact", "check"];

/// Hook configuration: flat map of hook keys to lists of shell commands.
///
/// Valid keys are bare prefixes (`before`, `after`, `failed`, `finally`) and
/// command-specific variants (`before_backup`, `finally_prune`, etc.).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HooksConfig {
    #[serde(flatten, deserialize_with = "deserialize_strict_hooks_map")]
    pub hooks: HashMap<String, Vec<String>>,
}

impl HooksConfig {
    /// Validate that all keys match valid hook patterns.
    pub fn validate(&self) -> Result<()> {
        for key in self.hooks.keys() {
            if HOOK_PREFIXES.contains(&key.as_str()) {
                continue;
            }
            // Check for prefix_command pattern
            let valid = HOOK_PREFIXES.iter().any(|prefix| {
                key.strip_prefix(prefix)
                    .and_then(|rest| rest.strip_prefix('_'))
                    .is_some_and(|cmd| HOOK_COMMANDS.contains(&cmd))
            });
            if !valid {
                return Err(VykarError::Config(format!("invalid hook key: '{key}'")));
            }
        }
        for (key, cmds) in &self.hooks {
            check_commands(cmds, &format!("hook '{key}'"))?;
        }
        Ok(())
    }

    /// Look up commands for a hook key, returning an empty slice if absent.
    pub fn get_hooks(&self, key: &str) -> &[String] {
        self.hooks.get(key).map_or(&[], |v| v.as_slice())
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }
}

/// Source-level hooks — simpler than `HooksConfig`, only bare prefixes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceHooksConfig {
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub before: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub after: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub failed: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub finally: Vec<String>,
}

impl SourceHooksConfig {
    /// Reject commands that still use a removed `{placeholder}`.
    pub fn validate(&self, source_label: &str) -> Result<()> {
        for (phase, cmds) in [
            ("before", &self.before),
            ("after", &self.after),
            ("failed", &self.failed),
            ("finally", &self.finally),
        ] {
            check_commands(cmds, &format!("source '{source_label}': hook '{phase}'"))?;
        }
        Ok(())
    }

    pub fn has_any(&self) -> bool {
        !self.before.is_empty()
            || !self.after.is_empty()
            || !self.failed.is_empty()
            || !self.finally.is_empty()
    }
}

/// `{name}` placeholders that hook commands used to have substituted, with the
/// environment variable that carries the same value.
const REMOVED_PLACEHOLDERS: &[(&str, &str)] = &[
    ("{command}", "VYKAR_COMMAND"),
    ("{repository}", "VYKAR_REPOSITORY"),
    ("{label}", "VYKAR_LABEL"),
    ("{error}", "VYKAR_ERROR"),
    ("{source_label}", "VYKAR_SOURCE_LABEL"),
    ("{source_path}", "VYKAR_SOURCE_PATH"),
];

/// Hook values reach the shell through the environment only. Splicing them
/// into the command text let a value such as an error message run as code, so
/// a command that still uses a placeholder is rejected rather than run with
/// the placeholder left as literal text.
fn check_commands(cmds: &[String], location: &str) -> Result<()> {
    for cmd in cmds {
        let first = REMOVED_PLACEHOLDERS
            .iter()
            .filter_map(|&(placeholder, var)| Some((cmd.find(placeholder)?, placeholder, var)))
            .min();
        if let Some((_, placeholder, var)) = first {
            let reference = if cfg!(windows) {
                format!("$env:{var}")
            } else {
                format!("${var}")
            };
            return Err(VykarError::Config(format!(
                "{location}: placeholder {placeholder} is no longer supported; \
                 use the environment variable {reference} inside double quotes instead"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hooks(cmd: &str) -> HooksConfig {
        HooksConfig {
            hooks: HashMap::from([("failed".to_string(), vec![cmd.to_string()])]),
        }
    }

    #[test]
    fn validate_rejects_every_placeholder_in_any_position() {
        for &(placeholder, var) in REMOVED_PLACEHOLDERS {
            for cmd in [
                format!("logger {placeholder}"),
                format!("logger \"failed: {placeholder}\""),
                format!("logger 'failed: {placeholder}'"),
            ] {
                let msg = hooks(&cmd).validate().unwrap_err().to_string();
                assert!(msg.contains("hook 'failed'"), "{msg}");
                assert!(msg.contains(placeholder), "{msg}");
                assert!(msg.contains(var), "{msg}");
            }
        }
    }

    #[test]
    fn validate_reports_the_first_placeholder() {
        let msg = hooks("echo {source_label} {command}")
            .validate()
            .unwrap_err()
            .to_string();
        assert!(msg.contains("{source_label}"), "{msg}");
        assert!(!msg.contains("{command}"), "{msg}");
    }

    #[test]
    fn validate_accepts_env_vars_and_other_braces() {
        for cmd in [
            "notify-send \"vykar $VYKAR_COMMAND failed: $VYKAR_ERROR\"",
            "echo '{\"execute\":\"guest-fsfreeze-freeze\"}' | socat - unix-connect:/tmp/qga.sock",
            "awk '{print $1}' /proc/loadavg",
            "echo {errors} {Error} { error }",
        ] {
            assert!(hooks(cmd).validate().is_ok(), "{cmd}");
        }
    }

    #[test]
    fn source_validate_reports_source_and_phase() {
        let hooks = SourceHooksConfig {
            after: vec!["echo done {source_label}".to_string()],
            ..Default::default()
        };
        let msg = hooks.validate("docs").unwrap_err().to_string();
        assert!(msg.contains("source 'docs'"), "{msg}");
        assert!(msg.contains("hook 'after'"), "{msg}");
        assert!(msg.contains("VYKAR_SOURCE_LABEL"), "{msg}");
    }
}
