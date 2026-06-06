use std::collections::HashMap;
use std::fmt;
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
            let line = line.strip_prefix("export ").unwrap_or(line);

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

/// Expand `${VAR}` and `${VAR:-default}` placeholders in raw config text.
/// Variables are looked up first in `overlay` (from `env_file`), then in
/// `std::env`. This avoids mutating global process state.
pub(super) fn expand_env_placeholders(
    input: &str,
    path: &Path,
    overlay: &HashMap<String, String>,
) -> vykar_types::error::Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut cursor = 0usize;

    while let Some(offset) = input[cursor..].find("${") {
        let start = cursor + offset;
        out.push_str(&input[cursor..start]);

        let token_start = start + 2;
        let Some(token_end_rel) = input[token_start..].find('}') else {
            return Err(config_expand_error(
                path,
                input,
                start,
                "unterminated environment placeholder",
            ));
        };
        let token_end = token_start + token_end_rel;
        let token = &input[token_start..token_end];
        let replacement = resolve_env_token(token, path, input, start, overlay)?;
        out.push_str(&replacement);
        cursor = token_end + 1;
    }

    out.push_str(&input[cursor..]);
    Ok(out)
}

/// Look up a variable name: check the overlay first, then the process environment.
fn lookup_var(name: &str, overlay: &HashMap<String, String>) -> Result<String, std::env::VarError> {
    if let Some(val) = overlay.get(name) {
        return Ok(val.clone());
    }
    std::env::var(name)
}

fn resolve_env_token(
    token: &str,
    path: &Path,
    input: &str,
    start: usize,
    overlay: &HashMap<String, String>,
) -> vykar_types::error::Result<String> {
    if token.is_empty() {
        return Err(config_expand_error(
            path,
            input,
            start,
            "empty environment placeholder",
        ));
    }

    if let Some(split_at) = token.find(":-") {
        let name = &token[..split_at];
        let default = &token[split_at + 2..];
        if !is_valid_env_var_name(name) {
            return Err(config_expand_error(
                path,
                input,
                start,
                format!("invalid environment variable name '{name}'"),
            ));
        }

        return match lookup_var(name, overlay) {
            Ok(value) if !value.is_empty() => Ok(value),
            Ok(_) => Ok(default.to_string()),
            Err(std::env::VarError::NotPresent) => Ok(default.to_string()),
            Err(std::env::VarError::NotUnicode(_)) => Err(config_expand_error(
                path,
                input,
                start,
                format!("environment variable '{name}' is not valid UTF-8"),
            )),
        };
    }

    if !is_valid_env_var_name(token) {
        return Err(config_expand_error(
            path,
            input,
            start,
            format!("invalid environment placeholder '{token}'"),
        ));
    }

    match lookup_var(token, overlay) {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Err(config_expand_error(
            path,
            input,
            start,
            format!("environment variable '{token}' is not set"),
        )),
        Err(std::env::VarError::NotUnicode(_)) => Err(config_expand_error(
            path,
            input,
            start,
            format!("environment variable '{token}' is not valid UTF-8"),
        )),
    }
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

fn config_expand_error(
    path: &Path,
    input: &str,
    start: usize,
    message: impl fmt::Display,
) -> vykar_types::error::VykarError {
    let (line, column) = byte_offset_to_line_col(input, start);
    vykar_types::error::VykarError::Config(format!(
        "invalid config '{}': {message} at line {line}, column {column}",
        path.display()
    ))
}

fn byte_offset_to_line_col(input: &str, byte_offset: usize) -> (usize, usize) {
    let mut line = 1usize;
    let mut column = 1usize;
    for ch in input[..byte_offset].chars() {
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line, column)
}

#[cfg(test)]
mod tests {
    use super::super::resolution::load_and_resolve;
    use super::super::test_support::EnvGuard;
    use super::*;
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

        let expanded = expand_env_placeholders(
            "repo=${VYKAR_TEST_EMPTY}",
            Path::new("test-config.yaml"),
            &no_overlay,
        )
        .unwrap();
        assert_eq!(expanded, "repo=");
    }

    #[test]
    fn test_env_expand_rejects_unterminated_placeholder() {
        let no_overlay = HashMap::new();
        let err = expand_env_placeholders(
            "repo=${VYKAR_TEST",
            Path::new("test-config.yaml"),
            &no_overlay,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("unterminated"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_env_expand_rejects_empty_placeholder() {
        let no_overlay = HashMap::new();
        let err = expand_env_placeholders("repo=${}", Path::new("test-config.yaml"), &no_overlay)
            .unwrap_err();
        assert!(err.to_string().contains("empty"), "unexpected: {err}");
    }

    #[test]
    fn test_env_expand_rejects_invalid_variable_name() {
        let no_overlay = HashMap::new();
        let err =
            expand_env_placeholders("repo=${1BAD}", Path::new("test-config.yaml"), &no_overlay)
                .unwrap_err();
        assert!(
            err.to_string().contains("invalid environment"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_env_expand_rejects_invalid_placeholder_syntax() {
        let no_overlay = HashMap::new();
        let err = expand_env_placeholders(
            "repo=${VYKAR_TEST-default}",
            Path::new("test-config.yaml"),
            &no_overlay,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("invalid environment placeholder"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_env_expand_overlay_takes_precedence() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_OVERLAY", "from_env");
        let overlay = HashMap::from([("VYKAR_TEST_OVERLAY".to_string(), "from_file".to_string())]);

        let expanded = expand_env_placeholders(
            "val=${VYKAR_TEST_OVERLAY}",
            Path::new("test-config.yaml"),
            &overlay,
        )
        .unwrap();
        assert_eq!(expanded, "val=from_file");
    }

    #[test]
    fn test_env_expand_falls_back_to_env() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_TEST_FALLBACK", "from_env");
        let empty_overlay = HashMap::new();

        let expanded = expand_env_placeholders(
            "val=${VYKAR_TEST_FALLBACK}",
            Path::new("test-config.yaml"),
            &empty_overlay,
        )
        .unwrap();
        assert_eq!(expanded, "val=from_env");
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
