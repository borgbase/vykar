use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Parse `.env` files into a variable overlay map. Does not modify the
/// process environment — the returned map is passed to `expand_env_placeholders`
/// which checks it before falling back to `std::env::var`.
///
/// Supported `.env` format:
/// - `KEY=VALUE`
/// - `export KEY=VALUE`
/// - `KEY="VALUE"` or `KEY='VALUE'` (quotes stripped)
/// - Blank lines and `#` comment lines are skipped
///
/// Paths are resolved relative to the config file's parent directory.
pub(super) fn parse_env_files(
    config_path: &Path,
    env_paths: &[PathBuf],
) -> vykar_types::error::Result<HashMap<String, String>> {
    let mut vars = HashMap::new();

    for env_path in env_paths {
        let contents = std::fs::read_to_string(env_path).map_err(|e| {
            vykar_types::error::VykarError::Config(format!(
                "cannot read env_file '{}' (referenced in '{}'): {e}",
                env_path.display(),
                config_path.display()
            ))
        })?;

        for (line_no, line) in contents.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Strip optional `export ` prefix.
            let line = line.trim_prefix("export ");

            let Some(eq_pos) = line.find('=') else {
                return Err(vykar_types::error::VykarError::Config(format!(
                    "invalid env_file '{}': missing '=' on line {}",
                    env_path.display(),
                    line_no + 1
                )));
            };

            let key = line[..eq_pos].trim();
            if key.is_empty() || !is_valid_env_var_name(key) {
                return Err(vykar_types::error::VykarError::Config(format!(
                    "invalid env_file '{}': invalid variable name '{}' on line {}",
                    env_path.display(),
                    key,
                    line_no + 1
                )));
            }

            let mut value = line[eq_pos + 1..].trim();

            // Strip matched quotes.
            if value.len() >= 2
                && ((value.starts_with('"') && value.ends_with('"'))
                    || (value.starts_with('\'') && value.ends_with('\'')))
            {
                value = &value[1..value.len() - 1];
            }

            vars.insert(key.to_string(), value.to_string());
        }
    }

    Ok(vars)
}

/// The result of expanding the placeholders of one value.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Expanded {
    /// The value with every placeholder replaced.
    pub(super) text: String,
    /// The variable names the value referenced, each once, in order of appearance.
    pub(super) vars: Vec<String>,
}

/// Expand ${VAR} and ${VAR:-default} placeholders in one parsed value.
/// Variables are looked up first in overlay (from env_file), then in
/// std::env. The result is never parsed as YAML. Errors carry no location;
/// the caller adds the key path and the YAML deserializer the line and column.
pub(super) fn expand_env_placeholders(
    value: &str,
    overlay: &HashMap<String, String>,
) -> Result<Expanded, String> {
    let mut text = String::with_capacity(value.len());
    let mut vars: Vec<String> = Vec::new();
    let mut cursor = 0usize;

    while let Some(offset) = value[cursor..].find("${") {
        let start = cursor + offset;
        text.push_str(&value[cursor..start]);

        let token_start = start + 2;
        let Some(token_end_rel) = value[token_start..].find('}') else {
            return Err("unterminated environment placeholder".to_string());
        };
        let token_end = token_start + token_end_rel;
        let token = &value[token_start..token_end];
        let (name, replacement) = resolve_env_token(token, overlay)?;
        if !vars.iter().any(|v| v == name) {
            vars.push(name.to_string());
        }
        text.push_str(&replacement);
        cursor = token_end + 1;
    }

    text.push_str(&value[cursor..]);
    Ok(Expanded { text, vars })
}

/// The variable name and replacement text for one placeholder body.
fn resolve_env_token<'t>(
    token: &'t str,
    overlay: &HashMap<String, String>,
) -> Result<(&'t str, String), String> {
    if token.is_empty() {
        return Err("empty environment placeholder".to_string());
    }

    if let Some(split_at) = token.find(":-") {
        let name = &token[..split_at];
        let default = &token[split_at + 2..];
        if !is_valid_env_var_name(name) {
            return Err(format!("invalid environment variable name '{name}'"));
        }
        return match lookup_var(name, overlay) {
            Ok(value) if !value.is_empty() => Ok((name, value)),
            Ok(_) | Err(std::env::VarError::NotPresent) => Ok((name, default.to_string())),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(format!("environment variable '{name}' is not valid UTF-8"))
            }
        };
    }

    if !is_valid_env_var_name(token) {
        return Err(format!("invalid environment placeholder '{token}'"));
    }

    match lookup_var(token, overlay) {
        Ok(value) => Ok((token, value)),
        Err(std::env::VarError::NotPresent) => {
            Err(format!("environment variable '{token}' is not set"))
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(format!("environment variable '{token}' is not valid UTF-8"))
        }
    }
}

/// Look up a variable name: check the overlay first, then the process environment.
fn lookup_var(name: &str, overlay: &HashMap<String, String>) -> Result<String, std::env::VarError> {
    if let Some(val) = overlay.get(name) {
        return Ok(val.clone());
    }
    std::env::var(name)
}

fn is_valid_env_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::super::resolution::load_and_resolve;
    use super::*;
    use crate::tests::helpers::EnvGuard;
    use crate::testutil::CWD_LOCK;
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_env_expand_bare_var_in_config() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _repo_guard = EnvGuard::set("VYKAR_TEST_REPO_URL", "/tmp/vykar-env-repo");

        let yaml = r#"
repositories:
  - url: ${VYKAR_TEST_REPO_URL}
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "/tmp/vykar-env-repo");
    }

    #[test]
    fn test_env_expand_default_used_when_unset() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _repo_guard = EnvGuard::unset("VYKAR_TEST_REPO_URL");

        let yaml = r#"
repositories:
  - url: ${VYKAR_TEST_REPO_URL:-/tmp/vykar-default-repo}
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "/tmp/vykar-default-repo");
    }

    #[test]
    fn test_env_expand_default_used_when_empty() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _repo_guard = EnvGuard::set("VYKAR_TEST_REPO_URL", "");

        let yaml = r#"
repositories:
  - url: ${VYKAR_TEST_REPO_URL:-/tmp/vykar-default-repo}
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "/tmp/vykar-default-repo");
    }

    #[test]
    fn test_env_expand_default_not_used_when_non_empty() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _repo_guard = EnvGuard::set("VYKAR_TEST_REPO_URL", "/tmp/vykar-non-empty-repo");

        let yaml = r#"
repositories:
  - url: ${VYKAR_TEST_REPO_URL:-/tmp/vykar-default-repo}
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "/tmp/vykar-non-empty-repo");
    }

    #[test]
    fn test_env_expand_bare_var_missing_is_error() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _repo_guard = EnvGuard::unset("VYKAR_TEST_REPO_URL");

        let yaml = r#"
repositories:
  - url: ${VYKAR_TEST_REPO_URL}
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("VYKAR_TEST_REPO_URL"), "unexpected: {msg}");
        assert!(msg.contains("line"), "unexpected: {msg}");
        assert!(msg.contains("column"), "unexpected: {msg}");
    }

    #[test]
    fn test_env_expand_bare_var_can_be_empty() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_EMPTY", "");
        let no_overlay = HashMap::new();

        let expanded = expand_env_placeholders("repo=${VYKAR_TEST_EMPTY}", &no_overlay).unwrap();
        assert_eq!(expanded.text, "repo=");
    }

    #[test]
    fn test_env_expand_rejects_unterminated_placeholder() {
        let no_overlay = HashMap::new();
        let err = expand_env_placeholders("repo=${VYKAR_TEST", &no_overlay).unwrap_err();
        assert!(err.contains("unterminated"), "unexpected: {err}");
    }

    #[test]
    fn test_env_expand_rejects_empty_placeholder() {
        let no_overlay = HashMap::new();
        let err = expand_env_placeholders("repo=${}", &no_overlay).unwrap_err();
        assert!(err.contains("empty"), "unexpected: {err}");
    }

    #[test]
    fn test_env_expand_rejects_invalid_variable_name() {
        let no_overlay = HashMap::new();
        let err = expand_env_placeholders("repo=${1BAD}", &no_overlay).unwrap_err();
        assert!(err.contains("invalid environment"), "unexpected: {err}");
    }

    #[test]
    fn test_env_expand_rejects_invalid_placeholder_syntax() {
        let no_overlay = HashMap::new();
        let err = expand_env_placeholders("repo=${VYKAR_TEST-default}", &no_overlay).unwrap_err();
        assert!(
            err.contains("invalid environment placeholder"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_env_expand_overlay_takes_precedence() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_OVERLAY", "from_env");
        let overlay = HashMap::from([("VYKAR_TEST_OVERLAY".to_string(), "from_file".to_string())]);

        let expanded = expand_env_placeholders("val=${VYKAR_TEST_OVERLAY}", &overlay).unwrap();
        assert_eq!(expanded.text, "val=from_file");
        assert_eq!(expanded.vars, vec!["VYKAR_TEST_OVERLAY".to_string()]);
    }

    #[test]
    fn test_env_expand_falls_back_to_env() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_FALLBACK", "from_env");
        let empty_overlay = HashMap::new();

        let expanded =
            expand_env_placeholders("val=${VYKAR_TEST_FALLBACK}", &empty_overlay).unwrap();
        assert_eq!(expanded.text, "val=from_env");
    }

    fn load(yaml: &str) -> vykar_types::error::Result<Vec<super::super::ResolvedRepo>> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();
        load_and_resolve(&path)
    }

    fn load_err(yaml: &str) -> String {
        load(yaml).unwrap_err().to_string()
    }

    #[test]
    fn test_env_value_with_yaml_syntax_is_literal() {
        let _lock = CWD_LOCK.lock().unwrap();
        let value = "pa#ss: word\n- x 'q' \"d\"";
        let _guard = EnvGuard::set("VYKAR_TEST_YAMLISH", value);

        let repos = load(
            r#"
repositories:
  - url: /tmp/r
encryption:
  passphrase: ${VYKAR_TEST_YAMLISH}
sources:
  - label: db
    command_dumps:
      - name: d.sql
        command: "echo '${VYKAR_TEST_YAMLISH}'"
"#,
        )
        .unwrap();
        assert_eq!(
            repos[0].config.encryption.passphrase.as_deref(),
            Some(value)
        );
        assert_eq!(
            repos[0].sources[0].command_dumps[0].command,
            format!("echo '{value}'")
        );
    }

    #[test]
    fn test_env_placeholder_in_comment_is_ignored() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::unset("VYKAR_TEST_IN_COMMENT");

        load(
            r#"
# url: ${VYKAR_TEST_IN_COMMENT}
# broken ${
# ${}
repositories:
  - url: /tmp/r # ${VYKAR_TEST_IN_COMMENT}
sources:
  - /home/user
"#,
        )
        .unwrap();
    }

    #[test]
    fn test_env_placeholder_in_key_is_not_expanded() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_KEY", "hostname");

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
sources:
  - /home/user
${VYKAR_TEST_KEY}: box
"#,
        );
        assert!(msg.contains("${VYKAR_TEST_KEY}"), "unexpected: {msg}");
    }

    /// An expanded value is used as it is: a placeholder inside it is text.
    #[test]
    fn test_env_expanded_value_is_not_expanded_again() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _g1 = EnvGuard::set("VYKAR_TEST_OUTER", "${VYKAR_TEST_INNER}");
        let _g2 = EnvGuard::unset("VYKAR_TEST_INNER");

        let repos = load(
            r#"
repositories:
  - url: /tmp/r
encryption:
  passphrase: ${VYKAR_TEST_OUTER}
sources:
  - /home/user
"#,
        )
        .unwrap();
        assert_eq!(
            repos[0].config.encryption.passphrase.as_deref(),
            Some("${VYKAR_TEST_INNER}")
        );
    }

    #[test]
    fn test_env_value_cannot_inject_a_list() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_LIST", "[a, b]");

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
sources:
  - /home/user
exclude_patterns: ${VYKAR_TEST_LIST}
"#,
        );
        assert!(msg.contains("expected sequence start"), "unexpected: {msg}");
        assert!(msg.contains("line"), "unexpected: {msg}");
    }

    #[test]
    fn test_env_rejected_value_is_not_echoed() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_ALGO", "SECRETISH-zz");

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
sources:
  - /home/user
compression:
  algorithm: ${VYKAR_TEST_ALGO}
"#,
        );
        assert!(msg.contains("compression.algorithm"), "unexpected: {msg}");
        assert!(msg.contains("${VYKAR_TEST_ALGO}"), "unexpected: {msg}");
        assert!(msg.contains("line"), "unexpected: {msg}");
        assert!(msg.contains("column"), "unexpected: {msg}");
        assert!(!msg.contains("SECRETISH"), "value leaked: {msg}");
    }

    #[test]
    fn test_env_numbers_and_booleans_expand() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _g1 = EnvGuard::set("VYKAR_TEST_NUM", "1048576");
        let _g2 = EnvGuard::set("VYKAR_TEST_SECS", "30");
        let _g3 = EnvGuard::set("VYKAR_TEST_TRUE", "true");
        let _g4 = EnvGuard::set("VYKAR_TEST_FALSE", "FALSE");
        let _g5 = EnvGuard::set("VYKAR_TEST_LEVEL", "5");
        let _g6 = EnvGuard::set("VYKAR_TEST_THRESHOLD", "25");
        let _g7 = EnvGuard::set("VYKAR_TEST_KEEP", "7");

        let repos = load(
            r#"
repositories:
  - url: /tmp/r
    min_pack_size: ${VYKAR_TEST_NUM}
    sftp_timeout: "${VYKAR_TEST_SECS}"
    allow_insecure_http: ${VYKAR_TEST_TRUE}
sources:
  - /home/user
one_file_system: "${VYKAR_TEST_FALSE}"
compression:
  algorithm: zstd
  zstd_level: ${VYKAR_TEST_LEVEL}
compact:
  threshold: ${VYKAR_TEST_THRESHOLD}
retention:
  keep_daily: ${VYKAR_TEST_KEEP}
"#,
        )
        .unwrap();
        let config = &repos[0].config;
        assert_eq!(config.repository.min_pack_size, 1_048_576);
        assert_eq!(config.repository.sftp_timeout, Some(30));
        assert!(config.repository.allow_insecure_http);
        assert!(!config.one_file_system);
        assert_eq!(config.compression.zstd_level, 5);
        assert!((config.compact.threshold - 25.0).abs() < f64::EPSILON);
        assert_eq!(config.retention.keep_daily, Some(7));
    }

    #[test]
    fn test_env_numbers_and_booleans_in_a_source_entry() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _g1 = EnvGuard::set("VYKAR_TEST_SRC_TRUE", "true");
        let _g2 = EnvGuard::set("VYKAR_TEST_SRC_KEEP", "3");

        let repos = load(
            r#"
repositories:
  - url: /tmp/r
sources:
  - path: /data
    one_file_system: ${VYKAR_TEST_SRC_TRUE}
    retention:
      keep_daily: ${VYKAR_TEST_SRC_KEEP}
"#,
        )
        .unwrap();
        assert!(repos[0].sources[0].one_file_system);
        assert_eq!(
            repos[0].sources[0].retention.as_ref().unwrap().keep_daily,
            Some(3)
        );
    }

    #[test]
    fn test_env_plain_source_entry_expands() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_SRC_PLAIN", "/data/plain");

        let repos = load(
            r#"
repositories:
  - url: /tmp/r
sources:
  - ${VYKAR_TEST_SRC_PLAIN}
"#,
        )
        .unwrap();
        assert_eq!(repos[0].sources[0].paths, vec!["/data/plain".to_string()]);
    }

    #[test]
    fn test_env_unknown_field_in_source_entry_is_ignored() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::unset("VYKAR_TEST_SRC_TYPO");

        let repos = load(
            r#"
repositories:
  - url: /tmp/r
sources: [{path: /data, typo: 1, typo2: "${VYKAR_TEST_SRC_TYPO}"}]
"#,
        )
        .unwrap();
        assert_eq!(repos[0].sources[0].paths, vec!["/data".to_string()]);
    }

    #[test]
    fn test_env_error_names_the_key_path() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::unset("VYKAR_TEST_MISSING_PATH");

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
sources:
  - path: ${VYKAR_TEST_MISSING_PATH}
    label: s
"#,
        );
        assert!(
            msg.contains(
                "environment variable 'VYKAR_TEST_MISSING_PATH' is not set in 'sources[0].path'"
            ),
            "unexpected: {msg}"
        );
        assert!(msg.contains("line"), "unexpected: {msg}");

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
sources:
  - /home/user
hooks:
  before_backup: "echo ${VYKAR_TEST_MISSING_PATH}"
"#,
        );
        assert!(msg.contains("'hooks.before_backup'"), "unexpected: {msg}");

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
    sftp_timeout: ${VYKAR_TEST_MISSING_PATH}
sources:
  - /home/user
"#,
        );
        assert!(
            msg.contains("'repositories[0].sftp_timeout'"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_env_bad_number_is_redacted() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _g1 = EnvGuard::set("VYKAR_TEST_BADNUM", "SECRETISH 12");
        let _g2 = EnvGuard::set("VYKAR_TEST_BADBOOL", "7");
        let _g3 = EnvGuard::unset("VYKAR_TEST_UNSET_NUM");

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
    sftp_timeout: ${VYKAR_TEST_BADNUM}
sources:
  - /home/user
"#,
        );
        assert!(
            msg.contains(
                "the value of 'repositories[0].sftp_timeout' set from ${VYKAR_TEST_BADNUM} is invalid"
            ),
            "unexpected: {msg}"
        );
        assert!(msg.contains("line"), "unexpected: {msg}");
        assert!(msg.contains("column"), "unexpected: {msg}");
        assert!(!msg.contains("SECRETISH"), "value leaked: {msg}");

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
    allow_insecure_http: ${VYKAR_TEST_BADBOOL}
sources:
  - /home/user
"#,
        );
        assert!(msg.contains("expected true or false"), "unexpected: {msg}");

        // An empty expansion does not unset a number setting.
        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
    sftp_timeout: ${VYKAR_TEST_UNSET_NUM:-}
sources:
  - /home/user
"#,
        );
        assert!(
            msg.contains("set from ${VYKAR_TEST_UNSET_NUM} is invalid"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_env_empty_url_is_refused() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_EMPTY_URL", "");

        let msg = load_err(
            r#"
repositories:
  - url: ${VYKAR_TEST_EMPTY_URL}
sources:
  - /home/user
"#,
        );
        assert!(msg.contains("url is empty"), "unexpected: {msg}");

        let msg = load_err(
            r#"
repositories:
  - url: "${VYKAR_TEST_EMPTY_URL:-}"
    label: main
sources:
  - /home/user
"#,
        );
        assert!(
            msg.contains("repository 'main': url is empty"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_env_empty_source_path_is_refused() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_EMPTY_SRC", "");

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
sources:
  - ${VYKAR_TEST_EMPTY_SRC}
"#,
        );
        assert!(
            msg.contains("source #1: path is empty"),
            "unexpected: {msg}"
        );

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
sources:
  - /home/user
  - path: "${VYKAR_TEST_EMPTY_SRC}"
    label: s
"#,
        );
        assert!(
            msg.contains("source 's': path is empty"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_env_plaintext_probe_names_repo_from_env() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_OLD_LABEL", "old-env");

        let msg = load_err(
            r#"
repositories:
  - url: /b
    label: ${VYKAR_TEST_OLD_LABEL}
    encryption:
      mode: none
sources:
  - /home/user
"#,
        );
        assert!(msg.contains("no longer supported"), "unexpected: {msg}");
        assert!(msg.contains("'old-env'"), "unexpected: {msg}");
    }

    #[test]
    fn test_env_empty_value_is_empty_string() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_EMPTY_HOST", "");

        let repos = load(
            r#"
repositories:
  - url: /tmp/r
sources:
  - /home/user
hostname: ${VYKAR_TEST_EMPTY_HOST}
"#,
        )
        .unwrap();
        assert_eq!(repos[0].config.hostname_override, None);
    }

    #[test]
    fn test_env_ignored_value_is_not_expanded() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::unset("VYKAR_TEST_IGNORED");

        let msg = load_err(
            r#"
repositories:
  - url: /b
    label: old
    encryption:
      mode: none
    access_token: ${VYKAR_TEST_IGNORED}
sources:
  - /home/user
"#,
        );
        assert!(msg.contains("no longer supported"), "unexpected: {msg}");
        assert!(msg.contains("'old'"), "unexpected: {msg}");
    }

    #[test]
    fn test_env_number_text_in_a_string_field() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_SEVEN", "7");

        let repos = load(
            r#"
repositories:
  - url: /tmp/r
    label: ${VYKAR_TEST_SEVEN}
sources:
  - /home/user
"#,
        )
        .unwrap();
        assert_eq!(repos[0].label.as_deref(), Some("7"));
    }

    #[test]
    fn test_literal_scalars_keep_yaml_rules_next_to_placeholders() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_LIT_URL", "/tmp/lit");

        let repos = load(
            r#"
repositories:
  - url: ${VYKAR_TEST_LIT_URL}
    sftp_timeout: "0x10"
sources:
  - /home/user
"#,
        )
        .unwrap();
        assert_eq!(repos[0].config.repository.sftp_timeout, Some(16));

        let msg = load_err(
            r#"
repositories:
  - url: ${VYKAR_TEST_LIT_URL}
    allow_insecure_http: 7
sources:
  - /home/user
"#,
        );
        assert!(
            msg.contains("invalid boolean (strict mode expects true/false)"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_env_placeholder_through_a_merge_key() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_MERGE_SECS", "45");

        let repos = load(
            r#"
repositories:
  - &r1
    url: /tmp/a
    label: a
    sftp_timeout: ${VYKAR_TEST_MERGE_SECS}
  - <<: *r1
    label: b
sources:
  - /home/user
"#,
        )
        .unwrap();
        assert_eq!(repos.len(), 2);
        assert_eq!(repos[0].config.repository.sftp_timeout, Some(45));
        assert_eq!(repos[1].config.repository.sftp_timeout, Some(45));
    }

    // The scan behind typed placeholders cannot read a document with a
    // duplicate key; the env_file pre-parse refuses that document first, so
    // the typed placeholder before it never reports a misleading error.
    #[test]
    fn test_env_typed_placeholder_before_a_duplicate_key() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_DUP_SECS", "30");

        let msg = load_err(
            r#"
repositories:
  - url: /tmp/r
    sftp_timeout: ${VYKAR_TEST_DUP_SECS}
sources:
  - /home/user
compression:
  algorithm: zstd
  algorithm: lz4
"#,
        );
        assert!(msg.contains("duplicate mapping key"), "unexpected: {msg}");
    }

    // parse_env_files returns a HashMap without touching std::env, so these
    // tests are safe to run in parallel — no EnvGuard or CWD_LOCK needed.
    fn parse_single(
        env_path: &Path,
        config_path: &Path,
    ) -> vykar_types::error::Result<HashMap<String, String>> {
        parse_env_files(config_path, &[env_path.to_path_buf()])
    }

    #[test]
    fn test_parse_env_file_basic() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join(".env");
        let config_path = dir.path().join("config.yaml");
        fs::write(&env_path, "A=hello\nB=world\n").unwrap();

        let vars = parse_single(&env_path, &config_path).unwrap();
        assert_eq!(vars["A"], "hello");
        assert_eq!(vars["B"], "world");
    }

    #[test]
    fn test_parse_env_file_export_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join(".env");
        let config_path = dir.path().join("config.yaml");
        fs::write(&env_path, "export MY_VAR=exported\n").unwrap();

        let vars = parse_single(&env_path, &config_path).unwrap();
        assert_eq!(vars["MY_VAR"], "exported");
    }

    #[test]
    fn test_parse_env_file_quoted_values() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join(".env");
        let config_path = dir.path().join("config.yaml");
        fs::write(&env_path, "DQ=\"double quoted\"\nSQ='single quoted'\n").unwrap();

        let vars = parse_single(&env_path, &config_path).unwrap();
        assert_eq!(vars["DQ"], "double quoted");
        assert_eq!(vars["SQ"], "single quoted");
    }

    #[test]
    fn test_parse_env_file_comments_and_blanks() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join(".env");
        let config_path = dir.path().join("config.yaml");
        fs::write(&env_path, "# comment\n\nKEY=value\n  # indented comment\n").unwrap();

        let vars = parse_single(&env_path, &config_path).unwrap();
        assert_eq!(vars.len(), 1);
        assert_eq!(vars["KEY"], "value");
    }

    #[test]
    fn test_parse_env_file_missing_equals_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join(".env");
        let config_path = dir.path().join("config.yaml");
        fs::write(&env_path, "GOOD=value\nbad line\n").unwrap();

        let err = parse_single(&env_path, &config_path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing '='"), "unexpected: {msg}");
        assert!(msg.contains("line 2"), "unexpected: {msg}");
    }

    #[test]
    fn test_parse_env_file_nonexistent_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join("missing.env");
        let config_path = dir.path().join("config.yaml");

        let err = parse_single(&env_path, &config_path).unwrap_err();
        assert!(
            err.to_string().contains("cannot read env_file"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_parse_env_files_later_file_overrides_earlier() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");

        let env1 = dir.path().join("first.env");
        fs::write(&env1, "SHARED=alpha\nONLY_FIRST=one\n").unwrap();
        let env2 = dir.path().join("second.env");
        fs::write(&env2, "SHARED=beta\nONLY_SECOND=two\n").unwrap();

        let vars = parse_env_files(&config_path, &[env1, env2]).unwrap();
        assert_eq!(vars["SHARED"], "beta");
        assert_eq!(vars["ONLY_FIRST"], "one");
        assert_eq!(vars["ONLY_SECOND"], "two");
    }

    #[test]
    fn test_parse_env_files_error_returns_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");

        let good = dir.path().join("good.env");
        fs::write(&good, "KEY=value\n").unwrap();
        let missing = dir.path().join("missing.env");

        let result = parse_env_files(&config_path, &[good, missing]);
        assert!(result.is_err());
    }

    #[test]
    fn test_env_file_integration_single() {
        let _lock = CWD_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();

        let _guard = EnvGuard::unset("VYKAR_TEST_ENVFILE_INT");
        fs::write(
            dir.path().join(".env"),
            "VYKAR_TEST_ENVFILE_INT=/from/env\n",
        )
        .unwrap();

        let yaml = "env_file: .env\nrepositories:\n  - url: ${VYKAR_TEST_ENVFILE_INT}\nsources:\n  - /tmp/src\n";
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "/from/env");
    }

    #[test]
    fn test_env_file_integration_list() {
        let _lock = CWD_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();

        let _guard_a = EnvGuard::unset("VYKAR_TEST_ENVFILE_LA");
        let _guard_b = EnvGuard::unset("VYKAR_TEST_ENVFILE_LB");
        fs::write(dir.path().join("a.env"), "VYKAR_TEST_ENVFILE_LA=/repo/a\n").unwrap();
        fs::write(dir.path().join("b.env"), "VYKAR_TEST_ENVFILE_LB=label-b\n").unwrap();

        let yaml = "env_file:\n  - a.env\n  - b.env\nrepositories:\n  - url: ${VYKAR_TEST_ENVFILE_LA}\n    label: ${VYKAR_TEST_ENVFILE_LB}\nsources:\n  - /tmp/src\n";
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.url, "/repo/a");
        assert_eq!(repos[0].label.as_deref(), Some("label-b"));
    }

    #[test]
    fn test_env_file_missing_file_is_error() {
        let _lock = CWD_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();

        let yaml = "env_file: missing.env\nrepositories:\n  - url: /repo\nsources:\n  - /tmp/src\n";
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        assert!(
            err.to_string().contains("cannot read env_file"),
            "unexpected: {err}"
        );
    }
}
