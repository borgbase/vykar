use serde::{Deserialize, Serialize};

use super::deserialize::*;
use super::hooks::SourceHooksConfig;
use super::types::{RetentionConfig, XattrsConfig};
use super::util::{expand_tilde, label_from_path};

/// A command whose stdout is captured and stored as a virtual file in the backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandDump {
    /// Virtual filename (e.g. "mydb.sql"). Must not contain `/` or `\`.
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub name: String,
    /// Shell command whose stdout is captured (run via `sh -c`).
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub command: String,
}

/// YAML input for a source entry — either a plain path or a rich object.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum SourceInput {
    Simple(#[serde(deserialize_with = "deserialize_strict_string")] String),
    Rich {
        #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
        path: Option<String>,
        #[serde(default, deserialize_with = "deserialize_optional_vec_strict_string")]
        paths: Option<Vec<String>>,
        #[serde(default, deserialize_with = "deserialize_optional_strict_string")]
        label: Option<String>,
        #[serde(default, deserialize_with = "deserialize_vec_strict_string")]
        exclude: Vec<String>,
        #[serde(default, deserialize_with = "deserialize_optional_vec_strict_string")]
        exclude_if_present: Option<Vec<String>>,
        one_file_system: Option<bool>,
        git_ignore: Option<bool>,
        xattrs: Option<XattrsConfig>,
        #[serde(default)]
        hooks: SourceHooksConfig,
        retention: Option<RetentionConfig>,
        #[serde(default, deserialize_with = "deserialize_vec_strict_string")]
        repos: Vec<String>,
        #[serde(default)]
        command_dumps: Vec<CommandDump>,
    },
}

/// Canonical resolved source entry.
#[derive(Debug, Clone)]
pub struct SourceEntry {
    pub paths: Vec<String>,
    pub label: String,
    pub exclude: Vec<String>,
    pub exclude_if_present: Vec<String>,
    pub one_file_system: bool,
    pub git_ignore: bool,
    pub xattrs_enabled: bool,
    pub hooks: SourceHooksConfig,
    pub retention: Option<RetentionConfig>,
    pub repos: Vec<String>,
    pub command_dumps: Vec<CommandDump>,
}

/// Normalize a list of `SourceInput` into resolved `SourceEntry` values.
///
/// - All `Simple(String)` entries are grouped into a single `SourceEntry`:
///   - If exactly 1 simple entry, label = `label_from_path()` (backward compat)
///   - If multiple, label = `"default"` and all paths are collected
/// - Each `Rich` entry is normalized individually.
///   - `path:` is sugar for `paths: [path]` — exactly one must be set.
///   - Multi-path rich entries require an explicit `label`.
pub(super) fn normalize_sources(
    inputs: Vec<SourceInput>,
    default_exclude_if_present: &[String],
    default_one_file_system: bool,
    default_git_ignore: bool,
    default_xattrs_enabled: bool,
) -> vykar_types::error::Result<Vec<SourceEntry>> {
    let mut simple_paths: Vec<String> = Vec::new();
    let mut rich_entries: Vec<SourceEntry> = Vec::new();

    for input in inputs {
        match input {
            SourceInput::Simple(path) => {
                simple_paths.push(expand_tilde(&path));
            }
            SourceInput::Rich {
                path,
                paths,
                label,
                exclude,
                exclude_if_present,
                one_file_system,
                git_ignore,
                xattrs,
                hooks,
                retention,
                repos,
                command_dumps,
            } => {
                // Validate command_dumps
                for dump in &command_dumps {
                    if dump.name.is_empty() {
                        return Err(vykar_types::error::VykarError::Config(
                            "command_dumps: 'name' must not be empty".into(),
                        ));
                    }
                    if dump.name.contains('/') || dump.name.contains('\\') {
                        return Err(vykar_types::error::VykarError::Config(format!(
                            "command_dumps: name '{}' must not contain '/' or '\\'",
                            dump.name
                        )));
                    }
                    if dump.command.is_empty() {
                        return Err(vykar_types::error::VykarError::Config(format!(
                            "command_dumps: command for '{}' must not be empty",
                            dump.name
                        )));
                    }
                }
                // Check for duplicate dump names
                {
                    let mut seen_names = std::collections::HashSet::new();
                    for dump in &command_dumps {
                        if !seen_names.insert(&dump.name) {
                            return Err(vykar_types::error::VykarError::Config(format!(
                                "command_dumps: duplicate name '{}'",
                                dump.name
                            )));
                        }
                    }
                }

                let resolved_paths = match (path, paths) {
                    (Some(p), None) => vec![expand_tilde(&p)],
                    (None, Some(ps)) => {
                        if ps.is_empty() {
                            return Err(vykar_types::error::VykarError::Config(
                                "source 'paths' must not be empty".into(),
                            ));
                        }
                        ps.iter().map(|p| expand_tilde(p)).collect()
                    }
                    (Some(_), Some(_)) => {
                        return Err(vykar_types::error::VykarError::Config(
                            "source entry cannot have both 'path' and 'paths'".into(),
                        ));
                    }
                    (None, None) => {
                        if command_dumps.is_empty() {
                            return Err(vykar_types::error::VykarError::Config(
                                "source entry must have 'path', 'paths', or 'command_dumps'".into(),
                            ));
                        }
                        Vec::new()
                    }
                };

                // Multi-path rich entries require an explicit label
                if resolved_paths.len() > 1 && label.is_none() {
                    return Err(vykar_types::error::VykarError::Config(
                        "multi-path source entries require an explicit 'label'".into(),
                    ));
                }

                // Dump-only sources (no paths) require an explicit label
                if resolved_paths.is_empty() && label.is_none() {
                    return Err(vykar_types::error::VykarError::Config(
                        "dump-only source entries require an explicit 'label'".into(),
                    ));
                }

                let label = if resolved_paths.is_empty() {
                    label.unwrap()
                } else {
                    label.unwrap_or_else(|| label_from_path(&resolved_paths[0]))
                };

                // Validate no duplicate basenames within a multi-path entry
                if resolved_paths.len() > 1 {
                    let mut basenames = std::collections::HashSet::new();
                    for p in &resolved_paths {
                        let base = label_from_path(p);
                        if !basenames.insert(base.clone()) {
                            return Err(vykar_types::error::VykarError::Config(format!(
                                "duplicate basename '{base}' in multi-path source '{label}'"
                            )));
                        }
                    }
                }

                rich_entries.push(SourceEntry {
                    paths: resolved_paths,
                    label,
                    exclude,
                    exclude_if_present: exclude_if_present
                        .unwrap_or_else(|| default_exclude_if_present.to_vec()),
                    one_file_system: one_file_system.unwrap_or(default_one_file_system),
                    git_ignore: git_ignore.unwrap_or(default_git_ignore),
                    xattrs_enabled: xattrs.map_or(default_xattrs_enabled, |cfg| cfg.enabled),
                    hooks,
                    retention,
                    repos,
                    command_dumps,
                });
            }
        }
    }

    let mut result = Vec::new();

    // Group all simple entries into one SourceEntry
    if !simple_paths.is_empty() {
        let label = if simple_paths.len() == 1 {
            label_from_path(&simple_paths[0])
        } else {
            // Validate no duplicate basenames
            let mut basenames = std::collections::HashSet::new();
            for p in &simple_paths {
                let base = label_from_path(p);
                if !basenames.insert(base.clone()) {
                    return Err(vykar_types::error::VykarError::Config(format!(
                        "duplicate basename '{base}' in simple sources (use rich entries with explicit labels to disambiguate)"
                    )));
                }
            }
            "default".to_string()
        };
        result.push(SourceEntry {
            paths: simple_paths,
            label,
            exclude: Vec::new(),
            exclude_if_present: default_exclude_if_present.to_vec(),
            one_file_system: default_one_file_system,
            git_ignore: default_git_ignore,
            xattrs_enabled: default_xattrs_enabled,
            hooks: SourceHooksConfig::default(),
            retention: None,
            repos: Vec::new(),
            command_dumps: Vec::new(),
        });
    }

    result.extend(rich_entries);
    Ok(result)
}
