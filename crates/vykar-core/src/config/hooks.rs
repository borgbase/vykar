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
        Ok(())
    }

    /// Look up commands for a hook key, returning an empty slice if absent.
    pub fn get_hooks(&self, key: &str) -> &[String] {
        self.hooks.get(key).map(|v| v.as_slice()).unwrap_or(&[])
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
    pub fn has_any(&self) -> bool {
        !self.before.is_empty()
            || !self.after.is_empty()
            || !self.failed.is_empty()
            || !self.finally.is_empty()
    }
}
