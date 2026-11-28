use std::fmt;

use serde::de::{self, Deserializer, MapAccess, Visitor};
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
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum SourceInput {
    Simple(String),
    Rich {
        path: Option<String>,
        paths: Option<Vec<String>>,
        label: Option<String>,
        exclude: Vec<String>,
        exclude_if_present: Option<Vec<String>>,
        one_file_system: Option<bool>,
        git_ignore: Option<bool>,
        xattrs: Option<XattrsConfig>,
        hooks: SourceHooksConfig,
        retention: Option<RetentionConfig>,
        repos: Vec<String>,
        command_dumps: Vec<CommandDump>,
    },
}

/// The mapping form of a source entry. Deserialized directly from the YAML
/// mapping, not through serde's untagged buffering, so every setting inside
/// it reaches the config deserializer with its own type (environment
/// placeholders in number and boolean settings depend on that).
#[derive(Deserialize)]
struct RichSourceInput {
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
}

impl From<RichSourceInput> for SourceInput {
    fn from(r: RichSourceInput) -> Self {
        Self::Rich {
            path: r.path,
            paths: r.paths,
            label: r.label,
            exclude: r.exclude,
            exclude_if_present: r.exclude_if_present,
            one_file_system: r.one_file_system,
            git_ignore: r.git_ignore,
            xattrs: r.xattrs,
            hooks: r.hooks,
            retention: r.retention,
            repos: r.repos,
            command_dumps: r.command_dumps,
        }
    }
}

/// A string is a plain source path; a mapping is a rich entry. Any other
/// scalar is refused as the strict-string fields refuse it.
impl<'de> Deserialize<'de> for SourceInput {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SourceVisitor;

        impl<'de> Visitor<'de> for SourceVisitor {
            type Value = SourceInput;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a source path or a mapping of source settings")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<SourceInput, E> {
                Ok(SourceInput::Simple(v.to_owned()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<SourceInput, E> {
                Ok(SourceInput::Simple(v))
            }
            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<SourceInput, A::Error> {
                RichSourceInput::deserialize(de::value::MapAccessDeserializer::new(map))
                    .map(SourceInput::from)
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> Result<SourceInput, E> {
                Err(E::custom(STRICT_STRING_ERROR))
            }
            fn visit_i64<E: de::Error>(self, _: i64) -> Result<SourceInput, E> {
                Err(E::custom(STRICT_STRING_ERROR))
            }
            fn visit_u64<E: de::Error>(self, _: u64) -> Result<SourceInput, E> {
                Err(E::custom(STRICT_STRING_ERROR))
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<SourceInput, E> {
                Err(E::custom(STRICT_STRING_ERROR))
            }
            fn visit_unit<E: de::Error>(self) -> Result<SourceInput, E> {
                Err(E::custom(STRICT_STRING_ERROR))
            }
        }

        deserializer.deserialize_any(SourceVisitor)
    }
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

/// The error for a source path that is empty, written literally as `""` or
/// set from an empty variable. `index` is the entry's position in `sources`.
fn empty_source_path(index: usize, label: Option<&str>) -> vykar_types::error::VykarError {
    let which = label.map_or_else(|| format!("#{}", index + 1), |l| format!("'{l}'"));
    vykar_types::error::VykarError::Config(format!("source {which}: path is empty"))
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

    for (i, input) in inputs.into_iter().enumerate() {
        match input {
            SourceInput::Simple(path) => {
                if path.trim().is_empty() {
                    return Err(empty_source_path(i, None));
                }
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
                    (Some(p), None) => {
                        if p.trim().is_empty() {
                            return Err(empty_source_path(i, label.as_deref()));
                        }
                        vec![expand_tilde(&p)]
                    }
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

                let label = match resolved_paths.first() {
                    None => label.expect("dump-only source label was validated"),
                    Some(first) => label.unwrap_or_else(|| label_from_path(first)),
                };

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
        let label = if let [only] = simple_paths.as_slice() {
            label_from_path(only)
        } else {
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
